+++
title = "Storage Engine"
description = "The write path end to end: write-ahead log, lock-free memtable, immutable .csx segments, the storage backend, compaction, caching and the data lifecycle."
weight = 20
+++

## Write-Ahead Log (WAL)

### Overview

Every write is durably persisted to the WAL before acknowledgment. On crash, the
WAL is replayed to reconstruct state.

### File Format

WAL files begin with a 6-byte header:

```text
[magic: "CXWL" (4 bytes)][version: u16 LE (2 bytes)]
```

### Record Format

Each record uses WAL format version 1:

```text
[crc32c: u32][length: u32][sequence_no: u64][record_type: u8][payload_version: u8][payload: [u8; length]]
```

- **CRC** covers `[length, sequence_no, record_type, payload_version, payload]`
- **Sequence numbers** are globally monotonic across files
- **Record type** discriminant from `WalRecordType`: `Data`(0), `Batch`(1),
  `Schema`(2), `Tombstone`(3) — enables future record kinds without format changes
- **Payload version** (currently 1) — enables future payload format evolution
- The version is checked for **equality**: there is one format, and a file
  that is not it is refused at open rather than guessed at

### Payload Codec

WAL payloads are serialized using a **binary v1 codec** (`wal_codec` module in
`chronix-core`) backed by `bincode`:

```text
[version: 0x01][discriminant][bincode payload]
```

`wal_encode()` / `wal_decode()` and the `WalCodecError` type are exported from
`chronix_core`. Only binary v1 records are accepted; legacy JSON is rejected.
`wal_encode_write_point(&Point)` encodes a write directly from a borrow rather
than cloning the `Point` into a `WalEntry::Write`, keeping `String` and `Vec`
allocations off the hot write path. Batch inserts benefit most.

### Write Path

1. LZ4 compression (when enabled) is performed **outside** the mutex,
   reducing critical section duration.
2. `WalWriter::append(payload)` acquires the lock, assigns a sequence number,
   and writes the pre-compressed record to the current file via `BufWriter`
   (64 KB buffer, reducing syscall overhead ~8× compared to the default 8 KB).
3. CRC32c (hardware-accelerated) is computed inside the lock over the
   already-compressed payload.
4. Fsync policy determines when data is flushed to disk:
   - `PerWrite` — fsync after every record
   - `PerBatch` — fsync after each `append_batch()` call
   - `Periodic` — fsync at a configurable interval
5. `start_periodic_sync()` enables background periodic fsync (interval set via
   `WalConfig.fsync_interval`), bounding data-loss window without per-write syncs

### Group Commit

`WalWriter::append_durable(payload)` and `WalWriter::append_batch(payloads)`
extend the write path with **group commit** semantics. With `PerBatch` fsync
policy, concurrent callers write their records independently and then coordinate
a single shared `fsync`:

1. The calling thread acquires the write lock, appends the record, and releases
   the lock so other writers may proceed.
2. It then checks if a sync is already in progress.
   - If yes, the thread waits on a bounded `Condvar` (5-second timeout) and
     piggy-backs on the leader's sync. If the timeout fires, the waiter
     promotes itself to sync leader to prevent indefinite hangs.
   - If no, it becomes the **sync leader**, acquires the write lock again,
     flushes/fsyncs, and wakes all waiters.

This amortises `fsync` cost across concurrent callers — the key bottleneck in
durable write workloads.

### CRC Computation

CRC32C is computed **incrementally** over `[length, sequence_no, payload]` using
`crc32c_append`, avoiding per-record heap allocations.

### Rotation

- Files are rotated when they exceed `max_file_size`
- Naming: `wal_{starting_sequence:020}.cxwl`
- Rotation is transparent to callers
- The `WalWriter` caches the current file count to avoid repeated `readdir()`
  calls on each rotation check, updating the cache only on file creation/deletion
- New WAL file creation fsyncs the parent directory to ensure the directory
  entry is durable; creation fails if the directory fsync fails

### Truncation

- `truncate_before(seq)` removes WAL files whose max sequence ≤ `seq`
- The active file is never deleted
- Idempotent and safe to call at any time
- **Safety invariant:** WAL truncation only proceeds after ALL shard flushes
  succeed. If any shard flush fails, WAL entries are preserved for retry on
  the next flush cycle, preventing data loss

### Crash Recovery

- `replay_all(dir)` reads all WAL files in sequence order
- CRC verification catches corruption
- Truncated tails (partial writes from crashes) are silently skipped
- Complete records are always returned
## Segment Format (`chronix-engine::segment`)

Segments are immutable columnar files (`.csx`) that store time-series data after
it is flushed from the memtable.

### File Structure

```text
┌─────────────────────────────────────────────┐
│ Header (48 bytes)                           │
│   magic: "CXSG", version, flags, timestamps│
│   row_count, column_count, series_count     │
├─────────────────────────────────────────────┤
│ Row Group 0                                 │
│   Column blocks (encoded, optionally LZ4)   │
│   Validity bitmap per block (only if nulls) │
│   Per-block CRC32c in ColumnBlockMeta        │
├─────────────────────────────────────────────┤
│ Row Group 1 … N                             │
├─────────────────────────────────────────────┤
│ Metadata (JSON column defs + binary blocks) │
├─────────────────────────────────────────────┤
│ Footer (20 bytes)                           │
│   metadata_offset, row_group_count,         │
│   CRC32c checksum, magic                    │
└─────────────────────────────────────────────┘
```

### Null Semantics

An absent field is a real SQL `NULL`, not a placeholder value.

Each `ColumnBlockMeta` carries `validity_offset` and `validity_length`. When a
block contains nulls, an Arrow-compatible **LSB-first packed validity bitmap**
of `value_count` bits follows the block payload — bit *i* set means row *i*
holds a real value. The bitmap is stored uncompressed and unencrypted: it
reveals only *which* rows have values, never what they are, and keeping it
outside the compressed/encrypted payload lets the reader build an Arrow
`NullBuffer` with a single copy.

Blocks with no nulls store no bitmap at all (`validity_length == 0`), so dense
time-series data — the common case — pays nothing for this.

The bitmap sits in the data region, so it is covered by the whole-file CRC32c
that `SegmentReader::open()` verifies eagerly. A flipped bit in a bitmap fails
the checksum rather than silently turning a value into a `NULL`. (The
per-block `block_crc` deliberately covers only the value payload, matching
what the block decode path reads.)

**Absent is not zero.** The bitmap is what makes `WHERE x IS NULL` and
`WHERE x = 0` different questions, keeps a missing value out of `AVG`, `MIN`,
`SUM` and `COUNT`, and makes the memtable and the segment answer a query the
same way either side of a flush. Nulls survive compaction.

There is **one `.csx` format**, and the header version is checked for
equality: a segment Chronix did not write is refused at `open()` rather than
read as though its columns meant what this reader assumes
(`segment::header::tests::header_version_is_refused_in_both_directions`).

### Column Order

The storage layer emits record batches in Chronix's **canonical** order —
timestamp first, then tags sorted by name, then fields sorted by name — and
`measurement_schema_to_arrow` produces the SQL table schema in that same order.

This is a deliberate guarantee, not an implementation detail. Following
schema-*registration* order instead would make a measurement's SQL column
order depend on which field happened to be written first, and would let the
table schema disagree with the batch layout whenever fields were registered
non-alphabetically. `ChronixExec` additionally resolves projections **by
column name** rather than by index, so a projected query returns the requested
column even if the two orders were ever to drift apart again.

### Write Path

1. `SegmentWriter::finalize_batch(&RecordBatch, ..)` is the flush and
   compaction entry point — the columns arrive as Arrow arrays.
   `write_rows(&[Point])` + `finalize()` remain for callers that hold points
2. `finalize()` performs all encoding in a **streaming** pass:
   a. Points are sorted by `(measurement, tags-in-cardinality-order, timestamp)`
      using `sort_unstable_by` with a zero-allocation borrow-based comparator —
      tag order is derived from the schema (already sorted by cardinality in
      `discover_schema()`), and all comparisons use `&str` references directly
      without per-row String allocations
   b. A `BufWriter<File>` is opened and the header is written immediately
   c. Row groups are encoded **one at a time** via `encode_row_groups_streaming()`,
      writing each group's column blocks directly to the buffered writer. This
      avoids materializing the entire file in memory.
   d. CRC32c is computed **incrementally** via `crc32c::crc32c_append()` as
      each chunk is written, tracking the file position for the footer
   e. Optional LZ4 or Zstd compression per column block (skipped if encoding
      already achieves ≥ 8×). When `zstd_dict_training` is enabled, the first
      row group's encoded blocks are collected as training samples; after the
      first row group, `zstd::dict::from_samples()` builds a compression
      dictionary that is used for all subsequent row groups. The trained
      dictionary is embedded in the segment metadata for transparent
      decompression.
   f. Metadata, CRC32c footer, and final flush/fsync are written to the
      temp file, which is then atomically renamed to the final path.
      Parent directory fsync errors are propagated (not silenced) to
      preserve the crash-safe guarantee
3. `finalize_batch()` performs the same streaming write from an Arrow
   `RecordBatch` via `encode_row_groups_from_batch_streaming()` — used by
   the compaction pipeline for zero-copy output
4. `buffered_point_count()` returns the number of accumulated points

### Column Statistics

Per-column `ColumnStats` track min, max, null count, sum (f64 + i128), and
distinct count. Integer columns accumulate `sum_i128: i128` alongside the
legacy `sum: f64` for full precision above 2^53. Binary format is 76 bytes per
column (`COLUMN_STATS_SIZE`).

Float statistics use a total-ordering scheme (`f64_to_ordered_i64`) that maps
IEEE 754 sign-magnitude representation to two's-complement for correct
comparison:

- Positive floats: `bits ^ i64::MIN` (flip sign bit)
- Negative floats: `bits ^ i64::MAX` (flip all bits except sign)
- NaN values are excluded from min/max (they would sort above +∞)

This ensures `-∞ < -max < -0 < +0 < +max < +∞` in the integer domain,
enabling correct predicate pushdown on float columns.

### Block Compression

`compress_block()` applies LZ4 or Zstd compression to encoded column data. It
includes a `MAX_BLOCK_SIZE` guard (4 GiB) that returns an error for oversized
blocks instead of silently overflowing the `u32` length prefix.

`decompress_block_with_optional_dict()` handles all three codec tags (LZ4,
Zstd, Zstd-with-dictionary) and caps the uncompressed size at 256 MiB
(`MAX_UNCOMPRESSED_SIZE`), preventing multi-gigabyte allocations from corrupt
segment files.

#### Zstd Dictionary Training

When `zstd_dict_training` is enabled in `SegmentWriterConfig`, the writer
collects encoded column blocks from the first row group as training samples.
If there are ≥ 8 samples totalling ≥ 16 KiB, a compression dictionary is
trained via `zstd::dict::from_samples()` (max dictionary size: 110 KiB).
Subsequent row groups use the trained dictionary for compression, typically
achieving 20–40% better compression on homogeneous schemas. The dictionary
is stored as a trailing section in the segment metadata (`[dict_len: u32]
[dict_bytes]`) and is backward-compatible — old segments without a
dictionary section are read normally.

### Safety Validations

The segment reader performs several integrity checks when opening files:

- **Magic bytes** — header and footer must start/end with `CXSG`
- **CRC32c checksum** — computed over all data except the footer
- **Per-block CRC32c** — each column block's `block_crc` field is verified on
  read; a value of `0` skips the check for backward compatibility with
  pre-checksum segments
- **Version check** — the header version must equal `segment::header::VERSION`
- **Metadata offset** — validated against footer start to prevent out-of-bounds
- **Row group count** — validated against remaining data size to prevent OOM

#### `to_array!` Macro

All segment deserialization (`from_bytes()` methods in header, footer, stats,
and metadata) uses the `to_array!` macro for safe byte-slice-to-fixed-size-array
conversion:

```rust
let offset = u64::from_le_bytes(to_array!(data[4..12], "block meta offset")?);
```

On slice-length mismatch, the macro returns `SegmentError::CorruptFile` with a
descriptive message including the field name. This eliminates all `unwrap()`
calls in deserialization code, upholding the "no panics in library code"
guarantee.

### Read Path

1. `SegmentReader::open(path)` validates magic bytes and CRC32c checksum
2. `read_columns(&[&str], row_group)` reads specific columns (column projection)
   — each column block's `block_crc` is verified before decoding (skipped when
   `block_crc == 0` for pre-checksum files)
3. `read_all()` returns all data as an Apache Arrow `RecordBatch`
4. `read_projected(&[&str])` reads a column subset across all row groups
5. `read_projected_filtered(&[&str], time_range)` reads a column subset with
   **row-group-level time-range predicate pushdown**. Each row group's
   timestamp min/max stats are checked against the query's `[start, end]`
   window (both inclusive). Row groups whose time range does not overlap are
   skipped entirely — no I/O or decompression is performed for them.
6. `read_row_group(idx)` materialises a single row group as a `RecordBatch`,
   used by `last_value()` for reverse-order scanning.
## Memtable (`chronix-engine::memtable`)

The memtable is the in-memory write buffer between the WAL and on-disk segments.

### Data Structure

Points are stored in a lock-free concurrent skip list
(`crossbeam_skiplist::SkipMap`) keyed by `(series_key_hash, timestamp)`. This
provides:

- **Lock-free concurrent writes** from multiple threads
- **Ordered iteration** for efficient scans and flush
- **Last-write-wins deduplication** on key collisions
- **Canonical form comparison** in scans — hash collisions are handled correctly
  by comparing full series key strings. `MemtableKey` uses `Arc<str>` for
  canonical series names (interned, zero-copy clone) and a secondary SipHash-2-4
  hash for O(1) `Ord` fast-path, falling back to string comparison only on collision

### Lifecycle

```text
Active Memtable ──freeze()──► Frozen Memtable ──flush()──► Segment File
       │                              │
       │ new writes                   │ reads (merged)
       ▼                              ▼
  Fresh Memtable          WAL truncation (after all shards flushed)
```

1. The `FlushController` manages active and frozen memtable slots
2. When `estimated_size > flush_threshold`, the active memtable is frozen
3. A fresh empty memtable is swapped in atomically
4. The frozen memtable is flushed on the maintenance thread: one Arrow
   batch per measurement is built straight off the skip list
   (`Memtable::to_record_batches()`) and handed to
   `SegmentWriter::finalize_batch()` — no `Point` is rebuilt. When a
   measurement batch would produce an oversized segment,
   `estimate_max_rows()` slices it into multiple segments based on
   `target_segment_size_bytes` (segment auto-splitting)
5. `insert_batch(&[Point])` accepts multiple points with a single frozen check
   and a single atomic size update — reduces per-point lock/atomic contention
   and batch-updates the measurement index under a single write lock
6. Reads scan both active and frozen memtables for consistency
7. If no frozen memtable is available when flush is requested,
   `MemtableError::NoFrozenMemtable` is returned (distinct from the
   `Frozen` error that rejects writes to a frozen memtable)

### Time-Shard Routing

The `ShardRouter` manages per-shard memtables:

- `ShardId = timestamp / shard_duration` (default: 1 hour)
- Writes within `±ooo_shard_tolerance` shards are accepted (using
  `saturating_sub` / `saturating_add` for overflow-safe boundary computation)
- Writes beyond tolerance are rejected with `ShardOutOfRange`
- Each shard has its own `Arc<FlushController>` with independent lifecycle — `scan()` and `scan_measurement()` snapshot `Arc` refs under the lock, drop it, then iterate outside the critical section

### Observability

- `Memtable`, `FlushController`, and `ShardRouter` implement `Debug` with
  full field enumeration for diagnostic logging
- `entry_to_point()` failures (point reconstruction from skip-list entries)
  are logged at `warn` level via `tracing`, preventing silent data loss
## Storage Backend (`chronix-engine::storage`)

The storage crate provides a pluggable, async I/O abstraction layer.

### `StorageBackend` Trait

```rust
pub trait StorageBackend: Send + Sync {
    async fn put(&self, path: &SegmentPath, data: &[u8]) -> Result<()>;
    async fn get(&self, path: &SegmentPath) -> Result<Vec<u8>>;
    async fn get_range(&self, path: &SegmentPath, offset: u64, length: u64) -> Result<Vec<u8>>;
    async fn delete(&self, path: &SegmentPath) -> Result<()>;
    async fn list_segments(&self, namespace: &NamespaceId) -> Result<Vec<SegmentPath>>;
    async fn exists(&self, path: &SegmentPath) -> Result<bool>;
}
```

### `LocalFsBackend`

The default implementation for local disk I/O:

- **Atomic writes** — data is written to a `.tmp` file and renamed to the final
  path, preventing partial reads
- **`pread`-based range reads** — `get_range()` uses OS-level `FileExt::read_at`
  for efficient partial reads without seeking
- **Case-sensitive extension validation** via `SegmentPath`

### `SegmentPath`

A typed path wrapper that enforces `.csx` extension and provides namespace-scoped
storage isolation (SEC-03). Each `SegmentPath` contains:

- **`namespace: NamespaceId`** — tenant namespace for cross-tenant isolation
- **`shard_id: ShardId`** — shard assignment
- **`name: String`** — segment filename (must end in `.csx`)

On-disk layout: `data_dir/ns_{namespace}/shard_{id}/{name}`
Object store layout: `ns_{namespace}/shard_{id}/{name}`

This ensures that different tenants' segments are physically separated at the
storage layer, providing defense-in-depth beyond policy-level access control.
## Compaction (`chronix-engine::compaction`)

Chronix uses **Time-Window Compaction Strategy (TWCS)** — segments are grouped
by time shard and never compacted across shard boundaries.

### Components

| Component | Purpose |
|-----------|---------|
| `CompactionPicker` | Scans shards and selects L0 segments exceeding `trigger_threshold` (default: 4) |
| `CompactionExecutor` | Merge-sort compaction with dedup, tombstone cleanup, schema unification |
| `CompactionLevel` | L0 (memtable flush), L1 (first pass), L2 (full shard optimization) |
| `SegmentState` | Active, Compacting, SoftDeleted — tracks segment lifecycle |

### Merge Algorithm

```text
Input Segments (L0)
  │
  ├─ Sort by segment_id ascending (newest = highest priority for dedup)
  ├─ Unify schemas (superset of all columns)
  ├─ Align batches to the unified schema (null-pad missing columns)
  ├─ Per-batch: compute FNV-1a hash + canonical series key
  ├─ Assign sort-preserving canonical IDs (u32) for O(1) comparison
  ├─ Per-segment sort by (hash, timestamp, canonical_id)
  ├─ K-way merge via BinaryHeap (min-heap) — O(N log K) where K = segment count
  │   ├─ Inline dedup: last-write-wins on (hash, ts, canonical_id)
  │   └─ Inline tombstone filter (skip deleted series by canonical form)
  ├─ Chunk output into row_group_size batches
  ├─ Interleave via arrow::compute::interleave() → sorted output chunks
  │   (gathers rows from original per-batch arrays without concatenation)
  └─ Write directly via SegmentWriter::finalize_batches() (streaming I/O)
```

The **streaming compaction** pipeline avoids the legacy `concat_batches` +
`take` round-trip. Instead, per-batch hash and canonical computation keeps
peak memory proportional to the largest single batch rather than the sum
of all inputs. `arrow::compute::interleave()` materializes only the selected
rows from the original arrays, and `finalize_batch()` writes the output
using streaming I/O with incremental CRC32c.

### Integration in `db.compact()`

`Chronix::compact()` orchestrates the full compaction lifecycle:

1. **Flush** — ensures all memtable data is in segments
2. **Filter** — only `Active` segments are considered (excludes `Compacting`/`SoftDeleted`)
3. **Pick** — `CompactionPicker::pick()` groups L0 segments by shard
4. **Execute** — `CompactionExecutor::execute()` merge-sorts each task,
   using the database's configured `compression_codec` and `float_encoding`
   (not hardcoded defaults)
5. **Register** — new compacted segment added to catalog with column stats,
   metadata cache, bloom filter, time index, and tag index entries — all
   from the series keys the writer recorded (`SegmentMeta::series_keys`),
   persisted beside the segment as its `.series` sidecar. The compacted
   segment is never read back.
6. **Cleanup** — input segments removed from catalog, disk, and all indexes
7. **Rollups** — `materialise_rollups()` runs after the pass, whether or
   not anything was compacted: a bucket becomes final by time passing, not
   by segment count. See [Rollup](#rollup) below.

### Backpressure

When L0 count exceeds `4 × trigger_threshold`, `backpressure_delay_ms()`
returns a linear sleep duration (capped at 500ms) to throttle writes without
dropping data.

### The maintenance thread

Every open `Chronix` runs one maintenance thread (named `chronix::maintenance`),
holding only a `Weak` reference so it never keeps the database alive:

- **Flush on demand** — writers wake it when a memtable crosses
  `memtable_flush_threshold`; it freezes and writes the segment off the
  write path.
- **Periodic passes** — every `maintenance_interval` (default 30 s, 60 s in
  the small preset, `Duration::ZERO` disables) it runs `compact()` — which
  materialises rollups — then `gc()`, then `enforce_retention` when a
  `retention` is configured.
- **Lifecycle** — `close()` stops and joins it. Its own handle never closes
  the database, so dropping the last *user* handle mid-pass still closes,
  on the thread.

There is nothing for a caller — embedded or `chronixd` — to start.
- **Graceful shutdown** — `stop_and_wait()` signals via `Notify` and awaits
  the task handle; `stop()` is fire-and-forget
- **Non-blocking** — compaction runs on a separate Tokio task, never blocking
  the write or query paths
## Caching Layer (`chronix-engine::cache`)

### Last Value Cache (LVC)

Lock-free `DashMap<SeriesKey, Point>` storing the most recent point per series.
Updated on every `insert()` — zero I/O overhead. `last_value()` checks LVC
first, falls back to segment scan on miss.

### Segment Cache (TinyLFU)

Memory-bounded, scan-resistant cache for decoded Arrow arrays, keyed by
`(SegmentId, RowGroupId, ColumnName)`. Uses TinyLFU admission control
(frequency sketch + window LRU + main LRU) to prevent sequential scan
thrashing. Shared via `Arc<dyn Array>` for zero-copy concurrent reads.
Probabilistic promotion ensures hot data survives large scans.

### Metadata Cache

In-memory cache of segment headers and column statistics (~1 KB per segment).
Populated on startup from existing segments and updated on flush, compaction,
and deletion. Used by the query planner for pruning without reading segment files.
## Data Lifecycle

### Delete Operations

`DeleteBuilder` fluent API: `.measurement()`, `.tag()`, `.range()`, `.build()`.

`execute_delete()`:

1. **Flushes** every memtable, so the delete only ever has to reason about
   segments — which is why no read path filters the memtable against tombstones.
2. **Scans** the segments overlapping the request's time range, filtering by tag
   predicates, and records for each matching series the newest timestamp inside
   that range.
3. **Resolves** one tombstone per matching series: `[start, end]` from the
   request, with the upper bound falling back to that series' newest stored
   timestamp rather than to "forever".
4. **Persists** — a `WalEntry::Delete` carrying the resolved tombstones (for
   point-in-time restore), then a fsynced catalog-manifest append (the durable
   record), then the in-memory set.
5. **Evicts** the last-value cache for every tombstoned series, and releases the
   series from the cardinality budget only when the delete covered all of it.

`delete_series(measurement, tags)` is a call to `execute_delete` with the
series' tags and no time bounds.

Tombstones are materialised during compaction and reclaimed by
`gc_tombstones()` once every segment a tombstone was issued against has left the
catalog.

### Retention

`enforce_retention(retention)` takes a `Duration`, as `ChronixConfig::retention`
does, and identifies shards whose `max_timestamp` falls
before the cutoff and drops all segments, catalog entries, bloom filters, tag
index entries, and metadata index entries for those shards.

The cutoff is `reference − retention`, where the reference is
`retention::retention_reference(now, newest_timestamp_held)` —
`min(wall clock, newest timestamp the database holds)`. The newest timestamp
comes from the same catalog snapshot the shard bounds do, plus the shard live
writes are landing in; that shard's *start* is used, which can only hold data
back.

Cold archiving uses the bare wall clock: it moves rows that stay queryable
through the archive table, so the cap that protects an irreversible delete
would only stop a quiet database from tiering.

The pass returns `RetentionResult { shards_dropped, segments_deleted,
segments_preserved, bytes_freed }`. `shards_dropped` counts shards actually
removed; `segments_preserved` counts segments past the cutoff that a rollup
still needs.

A pass that deleted anything then calls `repair_live_series`, which re-derives
the live series set from the segment sidecars plus the memtables — memtables
first, since data only moves memtable → segment — and retains `known_series`
(the cardinality budget) and the last-value cache against it. The namespace →
measurement index is outside this: it follows the measurement rather than its
rows, so an emptied measurement stays resolvable for its tenant and a dropped
one is removed by the drop path.

**Crash-safety ordering:** Catalog and index entries are removed *before* segment
files are deleted. This ensures that a crash between the two operations leaves
harmless orphan files rather than catalog entries pointing to missing files.
Per-measurement retention follows the same ordering principle.

### Rollup

`RollupConfig` defines source → target measurement downsampling. Its bucketing
is a **`TimeBucket`**: a fixed span for sub-day widths, the local calendar for
`d`/`w`/`mo`/`y`, so a daily tier runs local midnight to local midnight and a
monthly one is a calendar month. `RollupBuilder` takes `every("15m")` /
`every("1mo")` with an optional `timezone("Europe/Berlin")`, and refuses a
width or a zone it cannot parse at `build()`.
`RollupRegistry` manages CRUD via `add()`, `remove()`, `list()`, `get()` and
`rollups_for_source()`, refuses a definition that would make a measurement
feed itself (`would_cycle()`), and carries a
`RollupState { materialised_until, invalid }` per rollup. Both the
definitions and the state are persisted in the **catalog manifest**, which
is fsynced before the call that changed it returns and CRC-32C framed —
losing them silently disables every rollup, so they get the same durability as
the segment list.

`RollupAccumulator` folds time-ordered batches into buckets and emits a
bucket once a later one has started, so memory is one open bucket per tag
group — and it reports `saw_unordered_input()`, which fails the
materialisation rather than writing a partial bucket. `RollupAccumulator::unordered()`
holds everything until `finish()` for callers whose input is complete but
unsorted. `BucketAccumulator` holds the per-field running stats (Avg, Min,
Max, First, Sum, Count, Last) and skips NaN; every numeric column is
aggregated, `Int64` and `UInt64` included.

**Every boundary comes from `TimeBucket`.** `start_of(ts)` gives the bucket
holding an instant and `next(start)` the one after it — never `start + width`,
which is not the next bucket when the width is a month or the day is a
transition day. The materialisation watermark, the invalidation range a late
write produces, the live half of the rollup view and the boundary retention
waits for all go through it. A fixed width floors by Euclidean remainder, so a
negative timestamp snaps down rather than towards zero, and saturates at
`i64::MIN`.

**Materialisation** — `db.materialise_rollups()` does two things per rollup,
in order:

1. **Repair.** For each `[from, to)` range in the invalidation log: delete
   the target's aggregates over that range, recompute them from the source,
   sync the WAL, then clear the entry and invalidate the same range in every
   tier downstream. The delete-then-rewrite works because a tombstone is
   scoped to the segments it was issued against, so the recomputed points —
   which land in a new segment — are visible immediately.
2. **Advance.** From the watermark to the newest bucket whose input has gone
   final: for a raw source, every bucket ending before the oldest shard the
   out-of-order window still admits, where the window is anchored on the
   newest write *or the newest timestamp on disk*; for a source that is
   itself a rollup target, that rollup's watermark. The range is read
   through `execute_iter` (deduplicated, tombstones applied, one
   time-disjoint bucket of segments in memory at a time), folded through
   `RollupAccumulator`, and written through `backfill` in chunks of 8 192
   points, flushing every eighth chunk.

**The points are synced before the watermark that claims them is
persisted.** Under `FsyncPolicy::Periodic` — the gateway preset — a WAL
append is only in the operating system's buffers, and a crash between the
two would leave a watermark past rows that do not exist. Nothing recomputes
a bucket below the watermark.

A rollup whose pass fails keeps its own watermark and does not stop the
others. `compact()` and `enforce_retention()` both call materialisation;
retention drops data only once `rollups_rooted_at(measurement)` are all
materialised past the end of the bucket containing it, with no repair
pending.

**Invalidation producers:** `backfill()` (any point below a rollup's
watermark), `execute_delete()` (the deleted range), a repair of an upstream
tier, and `refresh_rollup(name, start, end)` explicitly. The log is
coalesced, kept disjoint and ascending, and capped at 64 ranges — past that
they merge into their hull, so a writer scattering single points cannot grow
the catalog without limit.

### Two tiers, not three

A warm tier — `.csx` re-compressed in place to Zstd on local disk — existed
and has been removed. It duplicated what the hot tier already does when
`compression = "zstd"` and `zstd_level` are set, nothing ever ran it (there
was no scheduler, and the only caller in the tree was an example), and its
migration was unsafe: it round-tripped every row through the `Point` path,
which materialises an absent tag as an empty string and silently nulls a
field whose type conflicts with the first one seen, and its catalog swap
accepted any file that happened to exist at the target path — including a
truncated one left by a failed migration — before deleting the original.

What remains is the pair that earns its keep: **hot** `.csx` for the data
being queried, and **cold** Parquet for the archive other tools have to be
able to read. To trade CPU for space inside the hot tier, set
`compression = "zstd"` with a higher `zstd_level`; to move data out, use the
cold tier.
