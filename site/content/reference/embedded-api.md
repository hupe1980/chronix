+++
title = "Embedded API"
description = "The chronix facade: opening a database, writing, querying, deletes and their tombstone lifecycle, rollups, retention, and Parquet export."
weight = 50
+++

## Public API (`chronix` facade)

The `chronix` crate ties all sub-crates together into a single embedded database.

### Module Structure (`db/`)

The `Chronix` struct and its methods are organized into focused submodules:

| Sub-module        | Contents                                              |
|-------------------|-------------------------------------------------------|
| `mod.rs`          | `Chronix` struct definition, `open()`, `Drop` impl   |
| `accessors.rs`    | Getters/setters (schema, config, statistics, UDFs)    |
| `write.rs`        | `insert`, `insert_batch`, admission control, backpressure |
| `query.rs`        | `execute`, `execute_stream`, segment reading, projection |
| `stream.rs`       | `execute_iter` — time-disjoint bucketing for bounded-memory scans |
| `delete.rs`       | `drop_measurement`, tombstones, hard delete           |
| `lifecycle.rs`    | `flush`, `close`, `compact`, GC, retention enforcement |
| `rollup.rs`       | Rollup definitions, materialisation, the real-time view |
| `backup.rs`       | Online backup and restore                             |
| `analytics_api.rs`| Anomaly detection, forecast API surface               |

### Lifecycle

```text
Chronix::open(config)
    │
    ├── Create/validate directories
    ├── Acquire exclusive file lock (fs2)
    ├── Open segment catalog (manifest + snapshot)
    ├── load_catalog_state(): time indices, blooms, tag index and the
    │     series set — all from the .series sidecars, no segment decoded
    ├── Open WAL writer
    ├── Restore schemas from catalog, reconcile with segment column lists
    ├── Replay WAL records above the catalog's WAL floor → memtable
    │
    ▼
  ┌──────────────────────────────────────────────┐
  │  insert(&Point) / insert_batch() / backfill() │
  │    → admission: open, capacity, window,       │
  │      cardinality (per point, nothing durable) │
  │    → schema: whole batch or nothing,          │
  │      persisted to the manifest first          │
  │    → one WAL record for the batch             │
  │    → memtable insert, unconditionally         │
  │    → wake the maintenance thread if needed    │
  └──────────────────────────────────────────────┘
    │
    ▼
  flush()
    │  for each shard:
    │    freeze_and_swap active memtable
    │    write frozen → SegmentWriter → .csx (+ .series sidecar)
    │    register in catalog (including CatalogColumnStats)
    │    update time index, bloom, tag index from the writer's series keys
    │  after ALL shards flushed: raise the WAL floor, truncate WAL,
    │  retire idle shards below the window
    │
    ▼
  compact()
    │  flush() first
    │  filter catalog to Active-only segments
    │  CompactionPicker::pick() → CompactionTask list
    │  CompactionExecutor::execute() per task
    │  register compacted segment in catalog (+ .series sidecar)
    │  populate metadata cache, bloom, tag index
    │  soft-delete input segments (marked SoftDeleted in catalog)
    │  materialise_rollups(): every rollup up to its final bucket
    │
    ▼
  gc() / gc_with_grace(ms)
    │  find soft-deleted segments past grace period (default: 5 min)
    │  hard-delete segment files (.csx + .series)
    │  remove catalog entries
    │  remove from blooms, tag_index, and metadata_cache
    │
    ▼
  archive_cold_segments(config)          [feature = "object-store"]
    │  group Active segments by (measurement, shard), keep the cold ones
    │  skip a group that is incomplete: another segment or an unflushed
    │    row overlaps it, or its rollups are not materialised past it
    │  read the group through execute_iter (dedup + tombstones)
    │  encode Parquet → upload → verify → drop catalog entries → delete files
    │
    ▼
  scan(measurement, min_ts, max_ts)
    │  query memtable shards
    │  return Arrow RecordBatch(es)
    │
    ▼
  close()
    │  flush all shards
    │  force catalog snapshot
    │  sync WAL
```

### Full Query Execution

`db.execute(&plan)` runs a complete query pipeline:

```text
QueryPlan
  ├── 1. Scan memtable (all shards matching measurement + time range)
  ├── 2. Pre-filter segments via tag inverted index (if tag filters present)
  ├── 3. Prune segments by time index + bloom filter + column stats
  ├── 4. Read segments (projection pushdown via compute_scan_columns)
  ├── 5. Filter tombstoned rows from segment data
  ├── 6. Merge memtable + segment batches
  ├── 7. Sort-merge dedup (last-write-wins, with schema alignment)
  ├── 8. Time-range + tag-equality filtering
  ├── 9. Column projection (extract only requested fields)
  └── 10. Post-processing (aggregate / downsample)
```

`db.execute_with_stats(&plan)` returns `(RecordBatch, PruningStats)` —
same pipeline but exposes segment pruning statistics for observability.

**Soft-deleted segments** are automatically excluded from all query paths.
`execute()`, `execute_stream()`, and `last_value()` call
`active_segments_for_measurement()` which filters out segments in the
`SoftDeleted` state.

**Empty-result schema preservation:** When a query matches no data, `execute()`
returns an empty `RecordBatch` with the correct measurement schema (timestamp +
tag columns + projected field columns) rather than an empty schema. This ensures
downstream consumers always receive consistent column metadata.

**Projection pushdown:** `compute_scan_columns()` calculates the minimal set of
columns needed (timestamp + tag filter columns + group-by columns + projected
fields) and passes them to `SegmentReader::read_projected()`. Only those columns
are decoded and decompressed from disk — all other column data is skipped
entirely, reducing I/O proportionally to the fraction of columns queried.

**Unified pruning via `prune_entries()`:** During step 3, a shared helper
`build_series_key()` extracts the series key and tag filter keys from the query's
tag filters. These are passed to `prune_entries()`, which applies bloom filter
pruning (segments that definitely don't contain the series key) and column stats
pruning (segments where required tag columns are all-null) in a single pass.
The tag inverted index (step 2) provides an additional pre-filter that narrows
the candidate set before per-segment bloom/stats checks.

### `execute_stream()`

`db.execute_stream(&plan)` returns `Result<Vec<RecordBatch>>` with genuinely
streaming semantics:

- **Scan plans:** Each data source (memtable, individual segments) is processed
  independently and emits its own `RecordBatch`. Results are deduped via
  `sort_merge_dedup_chunked()` which emits **65,536-row output batches during
  the k-way merge itself** (volcano-model streaming). This bounds output-path
  memory to O(chunk_size × columns) regardless of total surviving rows. A memory
  guard enforces the `max_query_result_bytes` limit on each emitted chunk,
  preventing OOM on unbounded queries.
- **Aggregate / Downsample plans:** All data is collected and the aggregation is
  applied, returning a single `RecordBatch`.

Both `execute()` and `execute_stream()` share the same pruning pipeline via
`prune_entries()` and the same `build_series_key()` helper, eliminating code
duplication. The tombstone set is accessed through an `RwLockReadGuard` (no
clone) for the duration of the stream.

Segment data is read via `read_segment_filtered()`, an extracted helper that
applies projection pushdown, tombstone filtering, time-range filtering, tag
equality filtering, and zone-map field predicate pushdown per segment.

### `last_value()` Fast Path

`db.last_value(measurement, tags)` returns the most recent `Point` for a
specific series without building a full query plan:

1. **Last-value cache** — returned immediately on a hit, after checking the
   entry against the tombstone set. The cache is opt-in
   (`enable_last_value_cache`, default `false`); without it this step is
   skipped and every call pays for the two below
2. **Memtable scan** — scan all shards for the series key, taking the point
   with the highest timestamp that no tombstone masks *at that timestamp*.
   Masking is per timestamp, not per series, so a delete covering one hour of a
   series leaves its newest surviving point answerable
3. **Segment scan** — the memtable's newest point is a *candidate*, not the
   answer: writes are accepted up to ±2 shards out of order, so a late arrival
   can sit in the memtable behind an already-flushed newer point. Segments are
   walked newest-first by `max_timestamp` and the walk stops as soon as a
   segment cannot beat the best so far — when the memtable point really is
   newest, no segment is opened. Bloom filters skip segments that cannot hold
   the series without reading any data

A cache hit costs ~400 ns. Without the cache the call is a memtable scan of the
series, which is hundreds of microseconds at 10K points.

### Cardinality Enforcement

Chronix tracks distinct canonical series keys in a `DashSet<String>` — exactly,
with no sketch beside it. Before
every `insert()` or `insert_batch()`, the new series key is checked against the
configured `max_series_cardinality` limit (default: 1,000,000). If the limit
would be exceeded, the write is rejected with `CardinalityExceeded`.

Batch inserts are checked atomically — all new keys in the batch are collected
into a `HashSet<u64>` for O(1) per-key deduplication, then validated against the
cardinality limit before any are committed.

### Delete and Drop

Both delete entry points resolve to the same implementation:
`delete_series(measurement, tags)` builds a `DeleteRequest` with the series'
tags and calls `execute_delete`, so both flush first and both evict the
last-value cache.

**A delete produces tombstones, and a tombstone is `{ series_canonical,
time_range, segments }`:**

- **`time_range` is always present.** There is no open-ended "delete this
  series" tombstone. A delete with no explicit upper bound resolves one *per
  series*, to the newest timestamp that series actually holds, so the delete
  covers the data that exists.
- **`segments` is the tombstone's scope, on the read path as much as for
  reclaiming it**: the segments active when the delete was issued, plus
  every compaction output those were merged into since. A row is masked only
  if it came from one of them.

Two properties follow from the scope:

- **A point written after a delete is visible at once**, even inside the
  interval the delete covered, because it lands in a newer segment that no
  tombstone names. Backfilling into a deleted interval behaves the same way.
- **A delete issued while a compaction is running still applies.** Registering
  a compaction extends every tombstone that named an input to name the output
  too, as one durable manifest step.

Memtable rows are never masked, and that is a property rather than an
omission: a delete flushes before it scans, so everything in a memtable was
written after every tombstone that exists.

**A whole-series delete also rewrites the series sidecars** of the segments it
scanned: the cardinality budget is rebuilt from those sidecars at open, so a
deleted series must not come back into the count at the next restart.

**Durability.** Tombstones are appended to the **catalog manifest** and fsynced
before `execute_delete` returns. They are *not* kept in the data WAL for
durability: that WAL is truncated once the memtable it covers has been flushed,
which is sooner than a tombstone must live. The WAL
still logs the resolved tombstones so a point-in-time restore replays the
delete, and the catalog is what a plain restart reads.

**Tombstone lifecycle.** A tombstone is reclaimed only when **every segment in
its `segments` set has left the catalog** — which happens when compaction
rewrites the segment (applying the tombstone as it merges) or when the segment
is deleted outright. `gc_tombstones()` runs after each compaction pass. Any
weaker rule reclaims a live tombstone and the deleted rows come back.

**Read paths.** Exactly one predicate is used:
`TombstoneSet::is_tombstoned(canonical, timestamp)`. The memtable scans do not
filter at all, because a delete flushes before it scans — so a tombstone can
only ever refer to data already in a segment. For segment data,
`filter_tombstoned()` accepts an optional explicit list of tag column names
(from `MeasurementSchema::tag_names()`) so that string-typed field columns are
not misidentified as tags. Collision-proof canonical series keys
(`measurement\0tag1=v1\0tag2=v2`) are computed per row and matched against the
tombstone set. Supports both plain `Utf8` and dictionary-encoded
(`Dictionary<Int32, Utf8>`) tag columns.
- **`drop_measurement(measurement)`** — removes the measurement schema from the
  `SchemaRegistry`, deletes all catalog entries, segment files (`.csx`), and
  series sidecar files (`.series`) on disk, and clears time index and bloom filter
  entries. This is an atomic, irreversible operation.

### Segment Lifecycle

Segments progress through three states tracked by `SegmentState`:

| State          | Description                                      |
|----------------|--------------------------------------------------|
| `Active`       | Normal segment, readable by queries              |
| `Compacting`   | Being processed by the compaction engine          |
| `SoftDeleted`  | Marked for removal with a `deleted_at_ms` timestamp |

After compaction, input segments are **soft-deleted** rather than immediately
removed from disk. The soft-delete operation follows persist-first ordering:
the manifest entry is written to disk before the in-memory state is mutated,
ensuring that an I/O failure leaves the segment in its original `Active` state
rather than creating a transient inconsistency. This allows in-flight queries
to continue reading data from the old segments during a grace period. `gc()` (default: 5 minute grace) or
`gc_with_grace(ms)` hard-deletes the segment files, catalog entries, bloom
filters, tag index entries, and metadata cache entries once the grace period
expires. All query paths (`execute()`, `execute_stream()`,
`last_value()`) automatically filter out soft-deleted segments via
`active_segments_for_measurement()`.

### Cardinality-Based Sort Order

During segment finalization, `SegmentWriter` sorts tag columns by ascending
cardinality (fewest distinct values first). This groups highly similar rows
together, maximizing compression ratios for dictionary and run-length encodings.
The sort key is `(measurement, lowest-cardinality-tag, ..., highest-cardinality-
tag, timestamp)`.

### Write Backpressure

`insert()` and `insert_batch()` call `apply_backpressure()` at the top of each
write. This reads the current L0 segment count from the catalog and calls
`CompactionPicker::backpressure_delay_ms(l0_count)` to compute an incremental
sleep duration. When compaction catches up and the L0 count drops below the
trigger threshold, the delay drops to zero and writes resume at full speed.
No data is ever dropped — only write throughput is throttled.

### Per-Measurement Retention

`enforce_retention()` supports per-measurement retention overrides via the
`measurement_retention` config map. After applying the global shard-level
retention, a second pass checks each measurement's individual cutoff and drops
segments from non-expired shards when a measurement has a shorter retention
period than the global default.

### Concurrency Model

- `Chronix` is `Send + Sync` — safe to share across threads
- `parking_lot::RwLock` for catalog, time index, and bloom filter maps
- `parking_lot::RwLock` for `SchemaRegistry` (no poison errors)
- `AtomicBool` for the `closed` flag
- File-level exclusive locking (`fs2::FileExt`) prevents multiple processes from
  opening the same database directory

### Ergonomic Macros

```rust
let tags = tags! { "host" => "srv-1", "region" => "us-east" };
let fields = fields! { "usage" => 42.5, "count" => 10_i64 };
```

Both macros handle the empty case (`tags!{}` → `BTreeMap::new()`) without
triggering unused `mut` warnings.
## Parquet Export

`export_parquet()` executes a query plan, then writes the result via
`parquet::arrow::ArrowWriter` with configurable compression (Snappy, Zstd, LZ4,
None) and row group size. Round-trip compatible with any Parquet reader.
