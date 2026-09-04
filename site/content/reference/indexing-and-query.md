+++
title = "Indexing & Query Execution"
description = "Layered segment pruning, bounded-memory scans, the SQL engine on DataFusion, the PromQL evaluator, and encoded-domain predicate pushdown."
weight = 40
+++

## Indexing (`chronix-engine::index`)

The index crate provides metadata structures for efficient segment pruning.

### Time Index

`TimeIndex` is a sorted vector of `TimeIndexEntry` records:

| Field        | Type        | Description                    |
|--------------|-------------|--------------------------------|
| `segment_id` | `u64`       | References a catalog segment   |
| `min_ts`     | `Timestamp` | Earliest timestamp in segment  |
| `max_ts`     | `Timestamp` | Latest timestamp in segment    |

- Binary search via `segments_in_range(min, max)` returns all segments whose
  time range overlaps the query window
- `add_segment()` inserts and re-sorts; `remove_segment()` returns a
  `#[must_use]` boolean indicating whether the segment was found

### Bloom Filters

`SeriesBloomFilter` uses **Kirsch-Mitzenmacker double hashing** (two base hashes
produce *k* derived positions) with FNV-1a as the hash function:

- `new(expected_items, false_positive_rate)` computes optimal `m` (bits) and
  `k` (hash functions), clamped to `[1, 30]`
- Bit array stored as `Vec<u64>` with `div_ceil(m, 64)` entries
- `insert(&str)` returns `bool` (true = genuinely new key, false = probable
  duplicate); the count is only incremented for new keys
- `might_contain(&str)` operates on UTF-8 string keys
- Target FP rate of 1% — tests verify ≤ 5× target rate
- **Persistence:** the bloom filter itself is not stored. Every segment has a
  `.series` sidecar — its distinct series keys, LZ4-compressed postcard with
  a CRC-32C (`chronix_engine::index::series_index`) — and on `open()`
  `load_catalog_state()` rebuilds the bloom, the tag-index entries and the
  exact series set from it, without decoding the segment. A missing or
  corrupt sidecar is rebuilt once from the segment's tag columns.

### Segment Catalog

`SegmentCatalog` is the persistent registry of all segments:

```text
catalog/
├── manifest.jsonl   # Append-only manifest WAL (JSON lines)
└── snapshot.json    # Periodic full snapshot
```

- **Manifest WAL** — Each `AddSegment` / `RemoveSegment` operation is appended
  as a JSON line and flushed with `sync_data()` (cheaper than `sync_all()` —
  skips file metadata flush since mtime/size are not critical for crash
  recovery). This is recovered on open by replaying all entries.
- **Periodic snapshots** — After every 1000 manifest entries, a full snapshot is
  written atomically (write to `.tmp` then rename). On recovery, the snapshot is
  loaded first, then any manifest entries written after it are replayed.
- **Binary format** — The manifest WAL uses length-prefixed bincode with CRC32c
  integrity checksums instead of JSON Lines. Each entry is framed as
  `[u32 length][bincode payload][u32 CRC32c]`. Snapshots use bincode format
  (`.bin`), with legacy JSON fallback for migration. Binary format provides
  ~10× faster serialization and ~3× smaller output vs JSON.
- **`SegmentMeta`** — Stored per segment: `segment_id`, `shard_id`,
  `measurement`, `min_ts`, `max_ts`, `row_count`, `size_bytes`, `created_at`,
  plus an optional `schema` (`MeasurementSchema`)
- **`CatalogColumnStats`** — Per-column statistics stored in each catalog entry:
  `name`, `data_type`, `role`, and `stats` (`ColumnStats` with min/max/null/sum/
  distinct). Populated at flush time from `SegmentReader::column_metadata()`.
  Used by the segment pruning pipeline for Level 3 stats-based elimination.
- **Query helpers** — `segments_for_shard(shard_id)`,
  `segments_for_measurement(name)`, `all_segments()`, `all_schemas()`
## Advanced Indexing (`chronix-engine::index`)

### Inverted Tag Index

`TagInvertedIndex` maps `"key=value"` → `HashSet<SegmentId>`. Updated on
segment creation/deletion. `segments_for_tags()` intersects per-tag results
for multi-tag queries. Thread-safe via `parking_lot::RwLock`.

### Zone Maps (Column Statistics)

`ZoneMapPredicate` evaluates predicates (`>`, `>=`, `<`, `<=`, `==`, `!=`)
against per-row-group `(min, max)` stats, returning `AllMatch`, `NoneMatch`, or
`MaybeMatch`. Equality (`==`/`!=`) uses exact bit comparison via `f64::to_bits()`
rather than epsilon-based tolerance, ensuring correctness across all magnitudes.
Used at both segment-level and row-group-level for pushdown.

#### Late Materialization (Zone-Map Pushdown)

Zone maps enable **late materialization** — evaluating numeric field predicates against row-group statistics *before* decompressing data. This eliminates I/O and CPU for row groups that provably cannot match.

**Pipeline:**
1. SQL filter → `PushdownPredicates` extracts `FieldPredicate` structs (column, operator, value)
2. `ChronixExec` passes field predicates through `QueryPlan::Scan`
3. Segment reader evaluates each predicate against per-row-group `ColumnStats`:
   - **F64 columns:** Convert stats via `ordered_i64_to_f64()`, evaluate `may_match_f64()`
   - **I64/Timestamp columns:** Evaluate `may_match_i64()` directly on raw stats
4. Row groups where *any* predicate rules out a match are **skipped entirely**
5. Metric: `chronix_segment_rg_pruned_by_zone_map_total` tracks pruned row groups

**Supported operators:** `=`, `>`, `>=`, `<`, `<=` on numeric columns (F64, I64, U64).

**Impact:** For selective queries (e.g., `WHERE temperature > 100`), zone-map pruning can eliminate 90%+ of row groups, delivering 10-100x speedup on large segments.

### Per-row-group tag blooms

Each tag column carries a bloom filter per row group, so a row group that
cannot contain the queried tag value is skipped without decoding it.

**Metric:** `chronix_segment_rg_pruned_by_bloom_total`.
## Query Engine (`chronix-query`)

The query crate provides a vectorized, Arrow-based query execution pipeline.

### Query Plan

`QueryBuilder` constructs a `QueryPlan` with a fluent API:

```rust
let plan = QueryBuilder::scan("cpu")
    .time_range(start, end)
    .filter_tag("host", "server-01")
    .aggregate(AggFn::Mean)
    .limit(1000)
    .build()?;
```

Three plan variants:

| Plan          | Description                                   |
|---------------|-----------------------------------------------|
| `Scan`        | Raw data retrieval with optional filtering     |
| `Aggregate`   | Reduce across time range (sum/min/max/mean/…) |
| `Downsample`  | Time-bucket aggregation at fixed intervals     |

### Bounded-memory execution

Segments are written **series-major** — tags in cardinality order, then
timestamp — because that is what makes the columnar encodings compress. Their
row groups are therefore *not* time-ordered, so a lazy row-group-level merge
is impossible without re-sorting.

What is available instead: a time-partitioned LSM produces segments whose
`[min_timestamp, max_timestamp]` ranges rarely overlap. `Chronix::execute_iter`
sweeps the surviving catalog entries into **time-disjoint buckets** and merges
one bucket at a time. Because the buckets are disjoint and processed in
ascending time order, concatenating their output is byte-identical to a single
k-way merge — while peak memory drops from `O(total surviving rows)` to
`O(rows in the largest bucket)`. Segments that genuinely all overlap degrade
to one bucket rather than to a wrong answer.

Every `Scan`-rooted plan runs on top of that iterator:

| Plan over a scan | Peak memory | Note |
|---|---|---|
| `Scan` | one bucket + one 64 Ki-row chunk | output is globally time-ordered and deduplicated |
| `Aggregate` | `O(groups × fields)` | `StreamingAggregator` folds each batch and drops it |
| `Downsample` | one open bucket | `StreamingDownsampler` closes a bucket only when a later one arrives, so an interval spanning a batch boundary is emitted once |
| `Limit` | the taken rows | breaking out of the iterator stops the segment reads |

`execute_stream()` is the collecting wrapper over the same machinery and still
enforces `max_query_result_bytes`; a consumer that processes and drops each
batch does not need that guard because it never accumulates. `export_parquet`
drives the iterator directly, which is what lets a gateway export a window
larger than its RAM.

### Segment Pruning

Multi-level pruning eliminates segments before reading:

1. **Tag index pruning** — `TagInvertedIndex::segments_for_tags()` pre-filters
   segments that contain the queried tag values, reducing the candidate set
   before any per-segment work is done
2. **Time pruning** — `TimeIndex::segments_in_range()` eliminates segments
   outside the query time window
3. **Bloom pruning** — `SeriesBloomFilter::might_contain()` eliminates segments
   that definitely don't contain the target series key
4. **Column stats pruning** — `CatalogColumnStats` for each candidate segment
   are checked against the query's tag filter keys. Segments where a required
   tag column has `distinct_count == 0` and `value_count == 0` (all-null) are
   eliminated, since they cannot contain the filtered tag values.
5. **Row-group time pruning** — Within each candidate segment,
   `read_projected_filtered()` inspects per-row-group timestamp min/max stats
   and skips row groups whose `[min_ts, max_ts]` does not overlap the query
   time window `[start, end]` (both boundaries inclusive). This avoids
   decoding and decompressing data that falls outside the query range.
6. **Zone-map field pushdown** — For numeric field predicates (e.g.,
   `temperature > 100`), per-row-group `ColumnStats` min/max values are
   evaluated via `ZoneMapPredicate` to skip row groups that provably cannot
   match. See [Zone Maps](#zone-maps-column-statistics) for details.

The core implementation is `prune_entries()`, which accepts pre-filtered catalog
entries and a bloom lookup closure. Both `execute()` and `execute_stream()` call
this single function, ensuring all pruning levels are always applied
consistently. `prune_segments()` (used by the standalone query API) delegates to
`prune_entries()` internally after time-index filtering.

`PruningStats` tracks `total`, `time_pruned`, `bloom_pruned`, `pruned_by_stats`,
and `remaining` counts for observability. `execute_with_stats()` returns
`(RecordBatch, PruningStats)` so callers can inspect pruning effectiveness.

### Vectorized Filtering

Arrow compute kernels provide zero-copy filtering:

- `time_range_mask()` — Boolean mask for timestamp range
- `tag_equality_mask()` — Boolean mask for tag value equality
- `combine_masks()` — AND-combine multiple masks
- `filter_batch()` — Apply mask to `RecordBatch`; if a tag filter references
  a column that does not exist in the batch (schema evolution), zero rows
  are returned instead of silently skipping the filter
- `project_batch()` — Column projection

### Aggregation

`AggFn` supports seven aggregation functions:

| Function | Description              |
|----------|--------------------------|
| `Sum`    | Arithmetic sum           |
| `Min`    | Minimum value            |
| `Max`    | Maximum value            |
| `Mean`   | Arithmetic mean          |
| `Count`  | Non-null count           |
| `First`  | First value by timestamp |
| `Last`   | Last value by timestamp  |

`aggregate_batch()` operates on `RecordBatch` with automatic type dispatch
(f64/i64/u64). The three typed aggregate functions (`aggregate_f64`,
`aggregate_i64`, `aggregate_u64`) are generated by the `impl_aggregate!` macro
to avoid code duplication. `COUNT` on an empty array returns `0` (SQL semantics),
while other aggregation functions return `None`. `aggregate_grouped()` partitions
by a tag column and computes per-group aggregates using a single-pass
`IncrementalAccumulator` per (group, field).

**Vectorization tiers:**

- **Non-grouped (single batch):** Arrow SIMD kernels via `arrow_agg::sum/min/max`
  for O(N) columnar computation. Count uses O(1) null metadata.
- **Ungrouped streaming (multi-batch):** Arrow SIMD kernels per-batch for
  sum/min/max, merged across batches. First/Last uses linear argmin/argmax scan.
- **Grouped (single & multi-batch):** `TypedColumn` pre-downcast enum resolves
  column type once per column (Float64/Int64/UInt64), eliminating per-row
  `as_any().downcast_ref()` dynamic dispatch. `IncrementalAccumulator` tracks
  count/sum/min/max/first/last per group in a single pass. FNV-1a hash with
  1024-chain cap for anti-DoS protection.

**Cardinality-aware strategy selection:**

`AggregationStrategy` (`Sort` / `Hash`) is chosen by `choose_strategy()`:
when estimated group-by cardinality is ≤ 1 024, the engine sorts on group-by
columns via Arrow `RowConverter` and performs a single linear scan over
contiguous group runs (`aggregate_sorted()`). For higher or unknown
cardinality, the hash-based `aggregate_grouped()` path is used. Cardinality
is estimated from `ColumnStats::distinct_count` in the segment catalog or
supplied explicitly via `QueryBuilder::with_estimated_cardinality()`. The
EXPLAIN endpoint exposes the selected strategy.

### Deduplication

`sort_merge_dedup(batches, measurement)` performs **series-aware** last-write-wins
deduplication using a **K-way merge** architecture:

1. Compute a **union schema** across all input batches via `build_union_schema()`
   — preserves field order from the first batch, appends new fields from later
   batches, and marks all fields nullable when multiple batches are present
2. **Align** each batch to the union schema via `align_batch()` — missing columns
   are filled with typed null arrays (`arrow::array::new_null_array`), ensuring
   graceful handling of schema evolution without panics
3. **Resolve tag columns** via `resolve_tag_columns()` — inspects Arrow field
   metadata for `role=tag` markers; falls back to all `Utf8` columns except
   `timestamp`
4. **Compute per-row series hashes** via `compute_series_hashes()` — produces
   FNV-1a hashes matching `SeriesKey::hash_fnv()` canonical form
   `measurement\0tag1=v1\0tag2=v2` (tags sorted lexicographically)
5. **Assign integer group IDs** — contiguous series hashes are mapped to
   compact `u32` group IDs for faster equality comparison during dedup
6. **Per-batch sort** — each batch's row indices are sorted independently
   by `(group_id, timestamp, write_order)`. Later batches (higher write
   order) win on duplicate keys.
7. **K-way merge** — a `BinaryHeap` (min-heap) merges K pre-sorted streams
   in O(N log K) time (vs the previous O(N log N) global sort). Inline
   dedup keeps only the **last** entry per `(group_id, timestamp)` pair,
   implementing last-write-wins semantics.
8. **Interleave** — `arrow::compute::interleave()` materializes the output by
   gathering selected rows from the original per-batch arrays without
   concatenating all inputs first. This reduces peak memory from
   O(total_rows) to O(output_rows) and avoids an intermediate copy.
9. Return a de-duplicated `RecordBatch`

This design ensures that distinct series sharing the same timestamp are never
conflated — only true duplicates (same series + same timestamp) are collapsed.

### Downsampling

`downsample()` aggregates data into fixed time buckets:

- Bucket assignment: `bucket = ts - ts.rem_euclid(interval)` —
  uses Euclidean remainder for correct alignment with negative timestamps,
  avoiding the `div_euclid * interval` multiplication overflow for large timestamps.
  All call sites validate that `interval > 0` to prevent `rem_euclid(0)` panics
- Groups by bucket, applies the chosen `AggFn` per bucket
- Returns a `RecordBatch` with `(timestamp, value)` columns
- **Multi-type support:** Supports Float64, Int64, and UInt64 value columns via
  `extract_f64()` helper; unsupported types return a descriptive error
## Encoded-Domain Pushdown (`chronix-query`)

`EncodedDomainEvaluator` evaluates column predicates against row-group stats
before decoding. Returns `AllMatch` (emit without decoding), `Skip` (zero
decoding), or `Decode` (must decode). Batch evaluation via `evaluate_batch()`
for vectorized processing of multiple row groups.
## SQL Query Engine (DataFusion Integration)

Chronix integrates [Apache DataFusion](https://datafusion.apache.org/) v55 as
its SQL query engine, exposing measurements as SQL tables via a dynamic
catalog scoped to the session's namespace.

**Runtime Environment:** `create_session_context()` configures DataFusion's
`RuntimeEnv` with:
- **`FairSpillPool`** — Bounded memory pool (`per_query_memory_limit`, default
  256 MiB). Large GROUP BY, ORDER BY, and hash join operations spill to disk
  instead of OOM-killing the process.
- **`DiskManager`** — Spill directory at `<data_dir>/spill/` for temporary
  intermediate results during query execution.
- **`target_partitions`** — Set to `available_parallelism()` (number of CPU
  cores), enabling DataFusion's multi-threaded operator execution.

**Streaming Scan:** `ChronixExec` yields chunked, deduped 64K-row batches via
`execute_stream()` + `sort_merge_dedup_chunked()`, enabling DataFusion's full
incremental pipeline processing. Downstream operators (filter, project,
aggregate, sort) process data chunk-by-chunk instead of waiting for the entire
result set.

**Epoch literals:** an `EpochLiteralRule` runs **before** DataFusion's
`TypeCoercion`, rewriting an integer literal compared against a
timestamp-typed expression into a timestamp literal in that column's unit.
Without it `WHERE _time >= 1700000000000000000` — the predicate every other
Chronix surface's values invite — is a planning error, and `BETWEEN` fails
with an internal DataFusion bug message. Ordering matters: appending the
rule with `with_analyzer_rule` places it *after* coercion, where the plan
has already failed, so the default rule list is rebuilt with this one in
front.

**Catalog scoping:** `table_names()` and `table_exist()` answer from a
namespace → measurements index derived from the canonical series keys the
engine already keeps, so the planner's per-reference lookup is a hash
lookup. Scoping the rows was not enough on its own: a measurement resolving
to an empty table is distinguishable from one that does not resolve, and
that difference enumerates other tenants' measurement names. With the
catalog scoped, `information_schema` is enabled — which is what makes
`SHOW TABLES` and `SHOW COLUMNS` work.

**Security**: The HTTP SQL endpoint (`POST /api/v1/chronix/sql`) enforces
**read-only access**. DDL (`CREATE`, `DROP`), DML (`INSERT`, `DELETE`),
`COPY` and `Statement` plans (`SET`, `PREPARE`) are rejected at the logical
plan level before execution. `EXPLAIN`, `EXPLAIN ANALYZE` and `DESCRIBE` are
permitted, and the plan they wrap is verified by the same rules — the
wrapper is not what gets checked, because an `Explain` around an `Insert` is
not a DML node.

### Architecture

```text
SQL string → DataFusion Parser → LogicalPlan → Optimizer → PhysicalPlan
                                                               │
                                            ┌──────────────────┘
                                            ▼
                               ChronixTableProvider.scan()
                                    │
                                    ▼
                            Chronix Segment Readers
                         (predicate pushdown, projection)
```

### Catalog & Schema Provider

- **`ChronixCatalogProvider`** — Implements `CatalogProvider`, reports a single
  `"public"` schema containing all Chronix measurements as tables.
- **`ChronixSchemaProvider`** — Implements `SchemaProvider`, dynamically
  discovers measurements via `db.schema_registry().measurement_names()` and wraps each in a
  `ChronixTableProvider`.
- **`ChronixTableProvider`** — Implements `TableProvider` with full predicate
  pushdown (time range + tag filters) and column projection.

### Custom SQL Functions

Registered as DataFusion UDFs/UDAFs:

| Function | Type | Description |
|----------|------|-------------|
| `time_bucket(interval, timestamp)` | UDF | Truncate timestamps to buckets |
| `first(field, timestamp)` | UDAF | First value ordered by timestamp |
| `last(field, timestamp)` | UDAF | Last value ordered by timestamp |
| `rate(field, timestamp)` | UDAF | Per-second rate of change |
| `irate(field, timestamp)` | UDAF | Instantaneous rate (last two points) |
| `forecast(field, timestamp, horizon)` | UDAF | `LIST(DOUBLE)` of predicted values |
| `multivariate_forecast(target, predictor, timestamp, horizon)` | UDAF | `LIST(DOUBLE)` from a regression |
| `diff`, `pct_change`, `rolling_mean`, `rolling_std`, `rolling_corr`, `zscore`, `ewm`, `anomaly_score`, `stl_trend`, `stl_seasonal`, `stl_residual`, `stl_decompose`, `correlation`, `cross_correlation`, `multivariate_anomaly` | UDWF | Window functions — require `OVER (PARTITION BY … ORDER BY …)` |

The analytics functions are **window functions**, not scalar ones: their value
for a row depends on the rows around it and on which rows belong to the same
series, and a scalar UDF is handed one `RecordBatch` with neither ordering nor
partitioning. See
[Analytics & Streaming](@/reference/analytics-and-streaming.md#sql-interface).

#### Runtime UDF Registration

Custom scalar and aggregate UDFs can be registered at runtime via the `Chronix`
API and are automatically available in all subsequent SQL queries:

```rust
db.register_udf(Arc::new(my_scalar_udf));
db.register_udaf(Arc::new(my_aggregate_udaf));
```

Runtime-registered UDFs are included when `create_session_context()` builds a
new DataFusion session, alongside the built-in functions above.

**Null handling:** every function respects Arrow's null bitmap. Inside a
window kernel a NULL becomes `NaN` so positional alignment survives — row `i`
of the output belongs to row `i` of the input, and dropping a NULL would shift
everything after it — and `NaN` becomes NULL again on the way out, so
`IS NULL` and `avg()` behave. The routines that are not defined on `NaN`
(STL, the anomaly detectors) run over the dense series and their results are
placed back on the rows they came from, so a row whose input was missing gets a
missing output. Constant arguments (`window`, `period`, `alpha`, `horizon`) are
validated with descriptive errors rather than panicking.

### Window Functions

All standard SQL window functions are supported via DataFusion's built-in engine:

- **Ranking:** `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `NTILE(n)`
- **Offset:** `LAG(expr, offset, default)`, `LEAD(expr, offset, default)`
- **Value:** `FIRST_VALUE()`, `LAST_VALUE()`, `NTH_VALUE(expr, n)`
- **Frame:** `ROWS BETWEEN`, `RANGE BETWEEN`, `GROUPS BETWEEN` with
  `UNBOUNDED`, `N PRECEDING`, `N FOLLOWING`, `CURRENT ROW`

### ASOF JOIN

Cross-series temporal alignment via a custom DataFusion `ExecutionPlan`.

> **A Rust API, not SQL syntax.** DataFusion's parser has no `ASOF JOIN` and
> chronix adds none. The entry point is `chronix::sql::execute_asof_join`,
> taking the two physical plans `DataFrame::create_physical_plan` gives you.

```rust
let left = ctx.table("cpu").await?.create_physical_plan().await?;
let right = ctx.table("mem").await?.create_physical_plan().await?;
let joined = chronix::sql::execute_asof_join(
    left, right, "_time", vec!["host".into()], 5_000_000_000, ctx.task_ctx(),
).await?;
```

- `AsofJoinExec` implements `ExecutionPlan` with sort-merge nearest-timestamp
  matching.
- For each left row, binary search + backwards scan finds the closest right
  row (≤ tolerance) matching tag columns — **O(L log R)** complexity.
- Tag column indices and array references are resolved once per batch, not per
  row —  avoiding repeated `schema.index_of()` and downcast overhead.
- Tie-breaking: earlier timestamp wins.
- Materializes the right side; streams the left side.
- Rejected at plan creation: a negative tolerance, and a join key that is
  absent from either side or is not a string.
- Both inputs require a single partition (`required_input_distribution`), so
  DataFusion inserts the coalesce.

## PromQL Engine

Chronix includes a built-in PromQL parser and evaluator for Prometheus
compatibility.

### Pipeline

```text
PromQL string → Lexer → Tokens → Parser → AST (Expr) → Evaluator → PromQLValue
```

### Supported Features

- **Selectors:** `metric{label="value"}`, `metric{label!="value"}`,
  `metric{label=~"regex"}`, `metric{label!~"regex"}`
- **Label matchers:** all four match operators fully implemented:
  - `=` — exact string equality
  - `!=` — string not-equal
  - `=~` — regex match (compiled via `regex` crate)
  - `!~` — regex not-match
  
  Matchers are compiled once into `CompiledMatcher` structs at evaluation
  time and applied as post-filters after label set construction.
- **Range vectors:** `metric[5m]`
- **Functions:** `rate()`, `irate()`, `increase()`, `delta()`, `deriv()`,
  `predict_linear()`, `abs()`, `ceil()`, `floor()`, `round()`, `clamp()`,
  `label_replace()`, `label_join()`, `histogram_quantile()`
- **Aggregations:** `sum`, `avg`, `min`, `max`, `count`, `topk`, `bottomk`
  with `by` / `without` grouping
- **Binary operators:** `+`, `-`, `*`, `/`, `%`, `^`, comparisons with `bool`
  modifier
- **Vector matching:** `on()`, `ignoring()`, `group_left()`, `group_right()`
  with include label lists
- **Subqueries:** `metric[5m:1m]`, and `metric[5m:]` at the engine's 1-minute
  default step
- **Modifiers:** `offset 5m`, `offset -5m` (forward), `@ 1609746000`,
  `@ start()`, `@ end()`

### Modifiers

`offset` and `@` attach to a **selector or a subquery** and to nothing else, so
`sum(m) offset 5m` is a parse error — as it is in Prometheus.

`@` replaces the evaluation instant and the offset is then subtracted, in that
order. `@ start()` and `@ end()` take a range query's own bounds; for an instant
query both are the evaluation time. A pinned selector reads one fixed window
whatever the step, so it is fetched directly rather than through the range
query's prefetch span.

### Subqueries

`expr[range:step]` evaluates `expr` as an instant query at each point of an
**absolute** step grid — aligned to multiples of the step rather than to the
evaluation time, so a subquery inside a range query samples the same points at
every outer step instead of jittering underneath it.

- Every sample carries its **step** timestamp, so the result is step-aligned
  and strictly increasing.
- The window is **left-open** on the grid, as range selectors are.
- A step-less `[5m:]` uses the engine's 1-minute interval, **not** the outer
  step, so a panel means the same thing at every zoom level.
- A subquery is a range vector, so `rate(x[5m:15s])` extrapolates over five
  minutes exactly as `rate(x[5m])` does.

Inner selectors prefetch over the **outer** query's window widened by the
subquery's reach, which is constant across steps — so a range query containing
a subquery reads storage once.

### Counter functions and extrapolation

`rate()`, `increase()` and `delta()` follow Prometheus's `extrapolatedRate`
exactly, because approximating it produces plausible-looking numbers that are
simply wrong:

1. Walk consecutive samples and sum the deltas. A negative delta is a counter
   reset, so the *new* value is added instead of the difference
   (`rate`/`increase` only — `delta` is for gauges and does not correct).
2. The observed increase covers `sampled_interval = last_ts - first_ts`, which
   is almost never the whole range: the first sample lands after the range
   start and the last before the evaluation time. Scale it up by
   `extrapolate_to / sampled_interval`, where `extrapolate_to` adds the gap at
   each end — each capped at half a sample interval when the gap exceeds
   ~1.1 sample intervals, since a larger gap suggests the series did not exist
   there.
3. Counter zero-clamping: a counter cannot have been negative, so the backward
   extrapolation never reaches further than the point at which the counter
   would have been zero.
4. `rate` then divides by the range. For an evenly scraped counter steps 2 and
   4 cancel, recovering the true per-second slope — which is the invariant to
   test against.

`histogram_quantile()` follows `bucketQuantile`: fewer than two buckets yields
`NaN`, a quantile landing in the top bucket reports the highest finite bound
(the top bound is `+Inf` and carries no information), a lowest bucket with a
non-positive bound is returned as-is, and everything else interpolates
linearly within the containing bucket. Bucket counts are made monotonic first
to absorb scrape artifacts.

### Conformance details that are easy to get subtly wrong

Each is pinned by a test naming the upstream rule it encodes:

- A comparison keeps the **vector element's** value whichever side the scalar
  is written on, so `2 < some_metric` is the metric's value, not `2`.
- A duplicate on the "one" side of `group_left` is an **error**, not a cross
  product.
- `last_over_time` keeps `__name__` (it returns an actual sample of the
  series); every other `_over_time` drops it, and so does `timestamp()`.
- `deriv` and `idelta` **drop** a series with fewer than two points rather than
  emitting `NaN`.
- `clamp(v, min, max)` with `max < min` is an empty vector.
- `label_replace` refuses a destination that is not a legal label name.
- `NaN` sorts **last** in `sort`, `sort_desc`, `topk` and `bottomk`.
- `quantile` outside `[0, 1]` yields ±Inf rather than erroring; `round(v, 0)`
  is `NaN`, which falls out of `Floor(v/0 + 0.5) * 0`.

All of the above are covered in `chronix/tests/promql_conformance.rs`, which
drives parse → select → evaluate against stored samples rather than calling the
compute helpers directly.

### API Endpoints (Prometheus-compatible)

| Endpoint | Description |
|----------|-------------|
| `GET/POST /api/v1/query` | Instant query |
| `GET/POST /api/v1/query_range` | Range query (capped at `max_range_query_points`, default 11,000) |
| `GET /api/v1/series` | Series metadata (defaults to `now - 1h → now` when no time range given) |
| `GET /api/v1/labels` | Label names |
| `GET /api/v1/label/{name}/values` | Label values |

### HTTP Safety Limits

The HTTP server (`chronixd`) enforces multiple safety limits to prevent abuse:

| Setting | Default | Description |
|---------|---------|-------------|
| `max_body_size` | 10 MB | Maximum HTTP request body size |
| `max_write_batch_size` | 50,000 | Maximum points per `POST /api/v1/write` batch |
| `max_range_query_points` | 11,000 | Maximum evaluation steps in `query_range` |
| `sql_max_rows` | 100,000 | Maximum rows returned from SQL query |
| `sql_query_timeout_secs` | 30 | SQL query execution timeout (covers planning, execution, and result streaming) |
| `prom_series_limit` | 10,000 | Maximum label-sets returned by `/api/v1/prom/series` |
| *(prom series default range)* | `now - 1h → now` | Default time range for `/api/v1/prom/series` when no `start`/`end` provided; prevents full-table scans |

The SQL endpoint additionally rejects all DDL/DML statements (only `SELECT` is
allowed). Error responses from the series endpoint now propagate query failures
instead of silently returning partial results.
