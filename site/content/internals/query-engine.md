+++
title = "Query Execution"
description = "Chronix implements a SQL-compatible query engine optimized for time-series workloads. The engine translates SQL statements into a physical plan of streaming operators that process columnar data…."
weight = 320
+++

Chronix implements a SQL-compatible query engine optimized for time-series
workloads. The engine translates SQL statements into a physical plan of
streaming operators that process columnar data through Apache Arrow.

## Architecture

```text
SQL Query
    │
    ▼
┌──────────┐
│  Parser  │  ← SQL → AST
└────┬─────┘
     │
     ▼
┌──────────┐
│ Analyzer │  ← Resolve names, types, semantic checks
└────┬─────┘
     │
     ▼
┌──────────┐
│ Logical  │  ← Selection pushdown, projection pruning,
│ Optimizer│     predicate simplification
└────┬─────┘
     │
     ▼
┌──────────┐
│ Physical │  ← Choose scan strategy, join algorithm,
│ Planner  │     parallelism degree
└────┬─────┘
     │
     ▼
┌──────────┐
│ Executor │  ← Streaming record-batch evaluation
└──────────┘
```

## Time-Series SQL Extensions

Chronix extends standard SQL with time-series-specific syntax:

| Extension | Purpose | Example |
|-----------|---------|---------|
| `FORECAST()` | Point forecasting | `SELECT FORECAST(value, 24)` |
| `ANOMALY_SCORE()` | Anomaly detection | `SELECT ANOMALY_SCORE(value)` |
| `FILL()` | Missing value imputation | `SELECT FILL(value, 'locf')` |
| `RESAMPLE()` | Time-grid alignment | `SELECT RESAMPLE(value, '1m', 'avg')` |
| `SMOOTH()` | Noise removal | `SELECT SMOOTH(value, 'ema', 0.3)` |
| `CORRELATE()` | Cross-correlation | `SELECT CORRELATE(a, b, 10)` |
| `COOLDOWN INTERVAL` | Alert suppression | `COOLDOWN INTERVAL '120s'` |
### Aggregation Semantics

Seven aggregation functions are supported: `Sum`, `Min`, `Max`, `Mean`,
`Count`, `First`, `Last`.

**Timestamp-aware `First` / `Last`:** These functions find the value at the
minimum (`First`) or maximum (`Last`) timestamp rather than relying on array
order. This guarantees correct results even when underlying data arrives
out of order or after sort-merge deduplication.

**Mutual exclusion:** Downsample and aggregate operations cannot be combined
in a single query plan — the planner rejects such plans with a validation error.
## Query Optimization

### Predicate Pushdown

Time-range predicates are pushed down to the storage layer, enabling
**segment pruning** — segments whose time range does not overlap the query
range are skipped entirely:

```text
Query: WHERE _time > '2024-01-15' AND _time < '2024-01-16'

Segments:
  [Jan 01 – Jan 07]  ← SKIPPED
  [Jan 08 – Jan 14]  ← SKIPPED
  [Jan 15 – Jan 21]  ← SCANNED (overlap)
  [Jan 22 – Jan 28]  ← SKIPPED
```

### Projection Pruning

Only the columns referenced in the query are read from the columnar
segment files. For wide tables (50+ metrics), this reduces I/O by 10–50×.

### Tag Predicate Pruning

Tag filters (e.g. `WHERE host = 'server-1'`) use the **inverted index**
to identify matching segments and row groups, avoiding full scans.

### Cost-Based Statistics

Chronix exposes per-column statistics to DataFusion's cost-based optimizer
via the `ExecutionPlan::statistics()` method on `ChronixExec`. The
optimizer uses these statistics for:

- **Join ordering** — smaller tables are placed on the build side of hash joins
- **Memory allocation** — pre-allocate hash tables based on estimated cardinality
- **Filter selectivity** — estimate output rows for range predicates

Statistics are aggregated across all active segments for the measurement.
Columns whose `data_type` differs across segments are skipped during
merging to prevent nonsensical min/max values from mixed types.

| Statistic | Source | Precision |
|-----------|--------|-----------|
| `num_rows` | Sum of `row_count` across segments | Inexact |
| `total_byte_size` | Sum of `byte_size` across segments | Inexact |
| `min_value` / `max_value` | Min/max of per-column `ColumnStats` | Inexact |
| `null_count` | Sum of per-column null counts | Inexact |
| `distinct_count` | Sum of per-column distinct counts (upper bound) | Inexact |
| `sum_value` | Aggregated from `sum` / `sum_i128` | Inexact |

**No per-segment sketches.** The statistics provider works from the catalog's
**exact** per-column `distinct_count` rather than from a HyperLogLog or a
histogram stored per segment: exact is better than an estimate, and sketches
cost 51 KiB a segment — on an hourly-sharded gateway a 1,600-row segment would
be 91 % sketch. Non-block metadata is ~760 bytes, bounded by
`writer::tests::segment_metadata_overhead_stays_proportional_to_data`.

The general rule this came from: fixed per-segment overhead is invisible in a
cloud deployment and is device lifetime on eMMC, so segment metadata must be
proportional to the data it describes.

## Execution Model

### Volcano / Pull-Based

The executor uses the **Volcano model** (Graefe 1990): each operator
implements a `next()` method that pulls a batch of rows from its child.
Chronix uses **vectorized execution** — each `next()` call returns an
Arrow `RecordBatch` (typically 8192 rows), not a single row.

### Streaming Aggregation

Time-series queries often aggregate by time buckets:

```sql
SELECT time_bucket('5m', _time) AS bucket, AVG(value)
FROM metrics GROUP BY bucket ORDER BY bucket;
```

Because data is stored in time-sorted order, the aggregation operator
can emit results **streamingly** — each bucket is finalized as soon as
the next bucket's data arrives, without buffering the entire result set.

**Group-by performance:** Group-by operations use FNV hashing for group key
lookup instead of sorted-map comparisons (`BTreeMap<Vec<Option<String>>>`),
reducing per-row grouping overhead from O(k log n) to O(1) amortized, where
k is the number of group-by columns.

### LIMIT / OFFSET

The query planner supports `LIMIT` and `OFFSET` via the `QueryBuilder`:

```rust
let plan = QueryBuilder::new()
    .measurement("cpu")
    .range(start, end)
    .limit(100)
    .offset(50)
    .build()?;
```

`Limit` wraps any inner plan (Scan, Aggregate, Downsample) and applies
batch-level slicing. Entire batches within the offset range are skipped without
conversion. Partial batches use `batch.slice(skip, take)` for zero-copy row
elimination. The offset skips the first N rows, then the limit caps the result
size.

Over a `Scan`, the limit is applied to a **lazy** iterator, so breaking out of
the loop stops the underlying segment reads rather than discarding rows that
were already read. A `LIMIT 100` over a month of data touches the first time
bucket and nothing else.

### Bounded-memory operators

Every operator that sits directly on a scan folds its input rather than
holding it:

| Operator | State held | Why it is bounded |
|---|---|---|
| `Aggregate` | one accumulator set per group | every supported function (count/sum/min/max/avg/first/last) has an incremental form, so batches can be dropped as they are folded — memory is `O(groups × fields)`, independent of row count |
| `Downsample` | one open time bucket | input arrives ascending by timestamp, so a bucket is complete as soon as a row from a later bucket appears |
| `Limit` | the rows taken so far | bounded by the limit itself |

The downsampling case has a subtlety worth stating: bucket boundaries do not
line up with batch boundaries. Downsampling each batch independently emits the
straddling interval **twice**, once per side, which a caller then sums or plots
as two points. Carrying one open bucket across batches is what makes the result
independent of how the scan happened to be chunked.

### Deduplication Optimization

Sort-merge deduplication reuses the pre-computed sort-index permutation to
derive row identity, avoiding redundant key hashing or comparison during the
dedup pass. When the sort phase has already computed a permutation array,
the dedup operator consults this index to identify duplicate rows in O(1)
per row rather than recomputing series keys.

#### Chunked Streaming Dedup

`sort_merge_dedup_chunked()` implements a volcano-model streaming dedup
that emits row-group-sized (64 Ki rows) output batches during the k-way
merge itself, rather than materializing the entire deduplicated result.
This bounds output-path memory to `O(chunk_size × columns)` regardless
of total result size. The `BinaryHeap`-backed k-way merge is inherently
a pull-based streaming operator — each `pop()` yields the next globally
sorted row, and duplicates are resolved by preferring the batch with the
highest batch index (last-write-wins). Chunks are materialised via
`arrow::compute::interleave` for zero-copy column assembly.

### Parallel Execution

Independent segment scans run on separate threads via **rayon** work-stealing.
Segment files are memory-mapped (`mmap` with `MADV_SEQUENTIAL`), so parallel
reads benefit from OS page-cache management without duplicating data in
user-space.

DataFusion's `target_partitions` is set to `available_parallelism()` (CPU core
count), enabling multi-threaded operator execution for downstream operations
(hash aggregate, merge sort, hash join). Combined with `FairSpillPool` for
bounded memory and `DiskManager` for spill-to-disk, queries over datasets
larger than `per_query_memory_limit` spill intermediate results instead of
OOM-killing the process.

The degree of parallelism is bounded by:

1. Number of CPU cores (rayon thread pool)
2. Number of segments matching the query
3. Configured `max_concurrent_scans` limit

Results are merged via a **merge-sort** operator that preserves time order.

## Arrow Integration

All intermediate results are represented as Apache Arrow arrays,
enabling:

- **Zero-copy** handoff between operators
- **SIMD-accelerated** filter, comparison, and arithmetic kernels
- **Interop** with external analytics tools (Python/Pandas, DataFusion)

## Query Metrics

Each query execution records:

| Metric | Description |
|--------|-------------|
| `segments_scanned` | Number of segments opened |
| `segments_pruned` | Number of segments skipped |
| `rows_scanned` | Total rows read |
| `rows_returned` | Rows in final result |
| `elapsed_ms` | Wall-clock time |
| `cpu_ms` | CPU time across all threads |
