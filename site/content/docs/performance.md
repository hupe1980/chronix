+++
title = "Performance Tuning"
description = "Tune Chronix for your workload: memory budgets for constrained gateways, flush and compaction settings, object-store tiering, and the trade-offs behind each knob."
weight = 50
+++

This guide covers tuning Chronix for various workload profiles, including
ingestion-heavy vs query-heavy configurations, memory budgets, and
cluster-level optimizations.

## Workload Profiles

Every engine knob lives under `[database]`. The defaults suit a workstation;
these are the two directions worth moving in.

### Ingestion-heavy

High write throughput with periodic batch queries — IoT, infrastructure
monitoring.

```toml
[database]
wal_fsync_policy = "periodic"    # Trade durability for throughput
memtable_flush_threshold = 200000  # Rows; fewer, larger flushes
max_memtable_memory = 268435456    # 256 MiB across all shards
compaction_concurrency = 4         # More parallel compaction
compression = "lz4"                # Cheapest to write
```

`wal_fsync_policy` is the one with a real trade: `per_write` survives a
power cut with no loss, `per_batch` loses at most one in-flight batch, and
`periodic` bounds the fsync *rate* — which on flash is the wear rate — at
the cost of a bounded window of unsynced writes.

### Query-heavy

Analytical queries over large ranges — dashboards, model training.

```toml
[database]
wal_fsync_policy = "per_batch"
segment_cache_size = 512           # Decoded segments held in memory
enable_last_value_cache = true     # Sub-100 µs "latest value" reads
compression = "zstd"               # Smaller segments, less I/O per scan
zstd_level = 6
float_encoding = "pco"             # Best ratio on real metric data
```

### Constrained gateway

`Chronix::open_small()` presets all of this for a 512 MB box; the
`gateway_footprint` example measures the result. See **Memory Tuning**
below for what the budget actually contains.

## Memory Tuning

### What the engine holds

`DatabaseStatistics::resident_memory_bytes()` is the sum of four terms, each
reported separately and each exported as a gauge:

| Term | Gauge | Grows with |
|---|---|---|
| Memtables | `chronix_memtable_memory_bytes` | Unflushed rows; bounded by `max_memtable_memory` |
| String interners | `chronix_interner_memory_bytes` | **Cardinality** — tag keys, tag values, measurement names |
| WAL writer buffer | `chronix_wal_buffer_bytes` | Fixed |
| Catalog, schemas, tombstones | `chronix_catalog_memory_bytes` | Segment count; reclaimed by compaction |

Two things are deliberately *not* in that sum, because they are not the
engine's to hold:

- **A query's working set.** DataFusion allocates against its own pool,
  bounded by `per_query_memory_limit` (256 MiB by default). On a small box
  this is the term that dominates, and it is the one to lower first.
- **The CDC event bus.** Its broadcast ring is allocated on the first
  subscription and sized by `cdc_capacity` — 65 536 events (~7 MiB) by
  default, 4 096 under `open_small`. A deployment that never reads the CDC
  stream pays nothing for it.

### A constrained gateway, measured

`examples/gateway_footprint.rs` runs the design partner's workload — 50 series
at 1 Hz for an hour, a 1 s → 1 min → 15 min rollup cascade, then the SQL and
PromQL a dashboard would issue:

```text
step                                     heap  memtable interner    WAL  catalog
open_small                                0.1       0.0     0.00   0.06     0.00
180 000 points written                   10.3       8.0     0.01   0.06     0.00
flushed                                   0.2       0.0     0.00   0.06     0.00
3300 rollup points materialised           1.4       0.8     0.02   0.06     0.00
SQL group-by, 15-minute view, PromQL      1.6       0.8     0.02   0.06     0.00

peak heap under 25 MiB
```

The peak is the flush's Arrow batch plus the encoder's scratch, both
transient. What settles is 1.6 MiB.

### Server-side budgets

```text
Write path  = memtable + WAL buffer + compaction buffers
  max_memtable_memory      64–256 MB, depending on cardinality
  compaction               ~128 MB per concurrent worker

Read path   = segment cache + per-query pool
  segment_cache_size       1–8 GB — larger means fewer disk reads
  per_query_memory_limit   64–256 MB per concurrent query
```

## Object Store Tiering

### Cache Configuration

Object storage is configured on the embedded API rather than in
`chronixd.toml` — `ObjectStoreConfig { url, cache, .. }`, with
`CacheConfig { cache_dir, max_size_bytes }` for the local read cache.
Objects larger than the cache bypass LRU tracking rather than evicting
everything else.

### Cold archiving

Archiving is an embedded API call, not a server setting — it uploads segments
older than a threshold and removes them from the hot database, so it costs one
re-encode and upload per segment and nothing on the read path afterwards. See
[Object Storage](/reference/object-storage/).

## Cluster-Level Tuning

### Region Sizing

```toml
[cluster.autoscale]
region_size_threshold = "10GB"     # Split when region exceeds this
region_series_threshold = 100000   # Split when series count exceeds this
```

**Guidelines:**

- Smaller regions → faster splits but more overhead (more Raft groups)
- Larger regions → fewer splits but longer split/migration times
- Recommended: 5–20 GB per region for balanced operation

### Segment Auto-Splitting

During memtable flush, oversized measurement batches are automatically split
into multiple segments. The flush layer calls `estimate_max_rows()` to compute
how many rows fit within `target_segment_size_bytes`, then writes one segment
per chunk. This keeps individual segment files at a predictable size, improving
compaction efficiency and query latency on large ingestion bursts.

### Replication Tuning

The distributed tier is **frozen** and not part of the default build, so
its tuning is out of scope here — see the
[cluster guide](@/docs/cluster.md) for what exists and its status.

## Numerical Precision

### Kahan Compensated Summation

All SIMD sum paths (scalar fallback, SSE2, AVX2, AVX-512, NEON) use Kahan
compensated summation. This reduces floating-point rounding error from O(nε)
(naive accumulation) to O(ε), ensuring accurate aggregation results even on
very large series (millions of points). No configuration is required — Kahan
summation is always active.

## Cardinality-Aware Aggregation Strategy

The query engine automatically selects the most efficient aggregation
algorithm based on the estimated group-by cardinality:

| Strategy | Condition | Algorithm | Best For |
|----------|-----------|-----------|----------|
| **Sort** | estimated cardinality ≤ 1 024 | Sort on group-by columns → linear scan over contiguous runs | Low-cardinality (e.g. `region`, `status`) — cache-friendly, produces sorted output |
| **Hash** | estimated cardinality > 1 024 or unknown | Hash-based grouping with `IncrementalAccumulator` | High-cardinality or unknown distributions |

### How Cardinality Is Estimated

1. **Explicit hint** — `QueryBuilder::with_estimated_cardinality(n)` lets
   callers supply a cardinality hint directly.
2. **Catalog stats** — When no explicit hint is provided, the engine inspects
   `ColumnStats::distinct_count` from the segment catalog for each group-by
   column and takes the maximum as the estimate.
3. **Fallback** — If neither source provides a value, the engine defaults to
   hash-based aggregation (safe for any cardinality).

### Sort-Based Aggregation

When the sort strategy is selected, group-by columns are sorted via Arrow
`RowConverter` + `SortField`, and then a single linear scan identifies
contiguous group runs. Each run is aggregated in-place using
`IncrementalAccumulator`. This avoids hash table overhead for low-cardinality
groupings and produces output that is already sorted by group keys —
eliminating a subsequent ORDER BY if the query requests sorted results.

### EXPLAIN Output

The `EXPLAIN` endpoint includes the chosen strategy:

```json
{
  "type": "Aggregate",
  "strategy": "Sort",
  "estimated_cardinality": 42,
  ...
}
```

## Runtime Optimizations

### LTTB Downsampling (Dashboard)

Dashboard chart rendering uses **Largest-Triangle-Three-Buckets (LTTB)**
downsampling to reduce render complexity without losing visual fidelity.
When a time-series exceeds the display resolution, LTTB selects the most
visually representative subset of points:

- 100K points → ~2K display points (50× reduction)
- Preserves peaks, valleys, and trend shape
- Applied automatically in the dashboard chart widget

This dramatically reduces browser/native rendering time for high-cardinality
dashboard panels.

### WebhookChannel Shared Runtime

The `WebhookChannel` (used for signal/alert webhook delivery) reuses a single
Tokio runtime instead of creating a new runtime per delivery. This eliminates
per-delivery thread pool overhead and reduces memory usage under high alert
volume.

### BTreeMap for ModelRegistry

The analytics `ModelRegistry` uses `BTreeMap` instead of `HashMap` for
deterministic iteration order. This ensures that model listing, serialization,
and re-training operations produce consistent, reproducible results regardless
of hash seed randomization.

### Chimp128 Encoder

The `Chimp128Encoder` provides improved float compression for periodic sensor
data by maintaining a 128-entry ring buffer of recent values. Instead of
XOR-ing only against the immediately preceding value (as standard Chimp does),
Chimp128 searches the ring buffer for the reference value that produces the
smallest XOR, exploiting repeating patterns in periodic signals.

- **5–15% better compression** on periodic sensor data (temperature, power,
  vibration sensors with regular cycles)
- **Auto-selected** via `FloatPattern::Periodic` in the adaptive encoding
  selector — no manual configuration required
- **Encoding type**: `EncodingType::Chimp128 = 16`
- **No regression** on non-periodic data (falls through to standard Chimp)

The improvement is most pronounced on data with periods that fit within the
128-value window (e.g., 1 Hz sensors with cycles ≤ 2 minutes).

### Zstd Dictionary Training

When `zstd_dict_training` is enabled in `SegmentWriterConfig`, the segment
writer collects encoded column blocks from the first row group as training
samples. If the samples meet the minimum threshold (≥ 8 samples, ≥ 16 KiB
total), a Zstd compression dictionary is trained via
`zstd::dict::from_samples()`. All subsequent row groups are compressed using
the trained dictionary, typically achieving **20–40% better compression** on
homogeneous schemas compared to standard Zstd.

```toml
[database]
compression = "zstd"
zstd_dict_training = true          # Train a dictionary from the first row group
```

Key implementation details:

- **Training threshold**: ≥ 8 samples totalling ≥ 16,384 bytes
- **Max dictionary size**: 110 KiB (balances compression ratio vs memory)
- **Storage**: Dictionary is embedded in the segment metadata as a trailing
  section (`[dict_len: u32][dict_bytes]`)
- **Backward compatible**: Old segments without a dictionary section are read
  normally; the reader detects the codec tag (0x03 for Zstd-dict) and
  applies the dictionary automatically
- **Best for**: IoT fleet data, infrastructure metrics, and other workloads
  where columns across row groups share similar value distributions

### Flight SQL Columnar Conversion

`arrow_batch_to_points()` uses pre-downcast `TypedField` enum to eliminate
per-row type dispatch, providing O(columns) vs O(rows × columns) type
resolution for Arrow RecordBatch ingestion.

### Catalog Manifest Binary Format

The catalog manifest WAL uses postcard serialization, which is far faster than
JSON Lines and produces ~3× smaller output on disk. Each entry is framed as
`[u32 length][postcard payload][u32 CRC32c]` for crash-safe integrity.

The catalog snapshot includes a `CATALOG_FORMAT_VERSION` envelope that enables
forward-compatible schema evolution. During load, the version is validated
before deserialization, producing a clear error when encountering snapshots
from a newer format.

## Monitoring Performance

### Key Metrics to Track

| Metric | Target | Alert Threshold |
|--------|--------|----------------|
| Write latency (p99) | < 10 ms | > 50 ms |
| Query latency (p99) | < 100 ms | > 500 ms |
| Replication lag | < 1 s | > 5 s |
| Cache hit ratio | > 90% | < 70% |
| Disk usage deviation | < 10% | > 20% |
| Namespace usage ratio | < 80% | > 90% |

### Benchmark Commands

```bash
# CPU dot-product benchmark
cargo bench -p chronix-analytics::compute -- dot_product


# Cluster write throughput
chronixd bench write --threads 8 --batch-size 10000 --duration 60s

# Query latency
chronixd bench query --concurrent 16 --queries queries.sql --duration 60s
```

## Troubleshooting Performance

### High Write Latency

1. Check WAL sync mode — `async` is faster but less durable
2. Increase `memtable_size` to reduce flush frequency
3. Flushes run on the database's own maintenance thread as soon as a
   memtable crosses `memtable_flush_threshold` — writes are never blocked
   by flush I/O, and nothing has to be started
4. Monitor compaction backlog — increase `max_concurrent` compaction workers
5. Check `chronix_cluster_write_latency_seconds` histogram for outliers
6. Zero-copy WAL encoding via `encode_write_point(&Point)` serialises
   points directly into the WAL buffer without cloning, reducing allocation
   overhead on the hot write path

### High Query Latency

1. Increase `block_cache_size` to reduce disk reads
3. Check `chronix_cluster_query_latency_seconds` breakdown
4. Optimize query selectivity — use tag filters to narrow scan scope
5. Use narrow time ranges — row-group predicate pushdown automatically skips
   row groups outside the query window, greatly reducing I/O for large segments

### Compaction Overhead

1. Streaming compaction (`finalize_batch`) writes deduped Arrow data directly
   to segments using `BufWriter` with incremental CRC32c — no intermediate
   `Point` allocations, no in-memory file materialization
2. The k-way merge phase uses a streaming `KWayMergeIter` iterator that yields
   one deduplicated index at a time, collecting into fixed-size chunks. Peak
   memory during the merge is $O(\text{chunk\_size} + K)$ rather than $O(N)$
   where $N$ is total points across all input segments
3. Compaction uses `arrow::compute::interleave()` instead of `concat_batches` +
   `take` — peak memory is proportional to the largest single batch, not the
   sum of all inputs
4. Monitor compaction throughput via `chronix_compaction_cycle_duration_seconds`

### Rebalancing Too Slow

1. Increase `max_concurrent_migrations` (trade stability for speed)
2. Lower `disk_deviation_threshold` for earlier trigger
3. Monitor `chronix_cluster_region_splits_total` for unexpected split storms
4. Ensure network bandwidth between DataNodes is not saturated

---

## Measured Results

Apple M4 Pro (12 cores, 48 GB), macOS 26.5.1, `rustc` 1.98.0, default
configuration — which means `FsyncPolicy::PerBatch`, so each `insert_batch`
call costs one fsync. Your hardware and durability settings will move these;
reproduce them with `cargo bench -p chronix --bench chronix_bench`.

### Ingestion

| Benchmark | Result |
|-----------|--------|
| `sustained_ingestion/1M_points_100_batches` | 1.65 s — 607K points/sec, one writer |
| `concurrent_ingestion/2` | 1.19M points/sec |
| `concurrent_ingestion/4` | 2.10M points/sec |
| `concurrent_ingestion/8` | 2.31M points/sec |
| `insert_batch/10000` | 16.7 ms — 599K points/sec |
| `flush_10K` | 62 ms — memtable to segment, encode and fsync |

Ingestion scales with concurrent writers up to the point where the WAL group
sync dominates, which on this hardware is around four. A single writer divides
its time roughly 40/37/14 between WAL append, memtable insert and WAL encode.

### Queries

Against 100K points across 1,000 series.

| Benchmark | Result |
|-----------|--------|
| `query_latency/single_series_1hr` | 377 µs — one series, 3,600 points |
| `query_latency/lvc_lookup` | 394 ns — requires `enable_last_value_cache` |
| `query_latency/wide_query_100_series_24hr` | 3.18 ms |
| `query_latency/wide_query_all_series` | 5.75 ms |

The last-value cache is **off by default**; enable it per measurement with
`enable_last_value_cache` (see [Operations](@/docs/operations.md)). Without it,
`last_value` falls back to a memtable scan and costs hundreds of microseconds.

### Analytics

| Benchmark | Result |
|-----------|--------|
| `zscore_detect_1m` | 180 ms — 5.5M points/sec |
| `iqr_detect_1m` | 217 ms — 4.6M points/sec |
| `ses_fit_10k` | 587 µs — 17M points/sec |
| `arima_111_fit_10k` | 573 µs |

## Benchmark Suites

Chronix includes comprehensive criterion benchmarks across all layers.

### Compute Engine Benchmarks

```sh
cargo bench -p chronix-analytics::compute --bench compute_bench
```

| Benchmark | Description |
|-----------|-------------|
| `cpu_exp_smooth` | Batch exponential smoothing (SES) |
| `cpu_z_score` | Batch Z-Score anomaly scoring |
| `scale_cpu_exp_smooth_10Kx10K` | 10K series × 10K points — SES on CPU |
| `hybrid_decision_overhead` | Auto-select backend decision time (<100 µs target) |

### E2E Server Benchmarks

```sh
cargo bench -p chronixd --bench server_throughput
```

| Benchmark | Description |
|-----------|-------------|
| `http_write` / `grpc_write` | Single-batch write throughput |
| `http_query` / `grpc_query` | Single query latency |
| `concurrent_writers_readers` | Mixed read/write concurrency |
| `scale_http_write_10k` | 10K-point batch write |
| `scale_multi_measurement` | 100 measurements × 10 points |
| `scale_targeted_query` | 100K pre-loaded, 1K range query |
| `tsbs_devops` | TSBS-style DevOps workload (100 hosts × 4 metrics) |
| `replicated_write` | Write with replication factor 3 |
| `objstore_tiered_read` | 1 MB segment round-trip via in-memory object store |
| `multi_tenant` | 100 namespaces × 10 points each |
| `large_scale_targeted` | 1M pre-loaded points, 1K range query |

## See Also

- [Operations Guide](@/docs/operations.md) — configuration reference, monitoring
- [Cluster Operations](@/docs/cluster.md) — region sizing, replication tuning
- [Analytics Guide](@/docs/analytics.md) — compute engine usage patterns
- [Architecture Reference](@/reference/_index.md) — engine internals, encoding details
- [Guide](@/docs/_index.md)
