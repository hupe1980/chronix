+++
title = "Tune performance"
description = "Make Chronix faster: tiering, cardinality, runtime settings, the metrics that show a bottleneck, and measured benchmark results."
weight = 100
aliases = ["/docs/performance/"]
+++

This guide covers tuning Chronix for various workload profiles, including
ingestion-heavy vs query-heavy configurations, memory budgets, and
cluster-level optimizations.

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
[Object Storage](@/explanation/object-storage.md).

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
| Cache hit ratio | > 90% | < 70% |
| Disk usage deviation | < 10% | > 20% |
| Namespace usage ratio | < 80% | > 90% |

### Benchmark Commands

Every number in this page is produced by a Criterion bench in the tree:

```bash
cargo bench -p chronix-encoding          # codec ratios and throughput
cargo bench -p chronix-analytics         # compute kernels
cargo bench -p chronix                   # write, query, PromQL, SQL
```

`chronixd` is a server and takes no subcommands; there is no `chronixd bench`.

## Troubleshooting Performance

### High Write Latency

1. Check `wal_fsync_policy` — `periodic_<ms>` coalesces syncs, at the cost
   of losing up to that window on power failure
2. Increase `memtable_flush_threshold` to flush less often
3. Flushes run on the database's own maintenance thread as soon as a
   memtable crosses that threshold — writes are never blocked by flush I/O,
   and nothing has to be started
4. Monitor compaction backlog — raise `compaction_concurrency`
5. Read `chronix_write_duration_seconds` for the latency distribution and
   `chronix_batch_size` for how much each write carries

### High Query Latency

1. Increase `segment_cache_size` to reduce disk reads
2. Read `chronix_query_duration_seconds` for the distribution, and
   `chronix_segments_pruned_by_time_and_index_total` for how much of the
   catalog each query is skipping
3. Narrow the selectivity — tag filters cut the segment set through the
   inverted index before anything is opened
4. Use narrow time ranges — row-group predicate pushdown skips row groups
   outside the query window, which is most of a large segment

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
4. Monitor compaction throughput via `chronix_compaction_duration_seconds`

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
| `latest_n/gateway_30d` | 0.60 ms (`ORDER BY _time ASC LIMIT 10`), 0.53 ms (`DESC`) over 30 days of 50 series — the scan stops after the newest bucket |
| `cardinality_stats/1m_series_limit_10` | 397 ms — `/api/v1/status/tsdb` over 1 M series |
| `inspect/inspect_10k_segments` | 442 ms — `chronixd inspect` on a 10 000-segment directory, sidecars included |

**Parallelism.** `parallelism/overlapping_8_segments` reads 400K points from
one time range covered by eight overlapping segments: **24.4 ms serial,
26.4 ms with one thread per core** (Apple M4 Pro, 14 cores). The merge that resolves
overlapping writes dominates, so reading the segments in parallel buys
nothing here — which is why `open_small` is serial and loses nothing by it.

The last-value cache is **off by default**; enable it per measurement with
`enable_last_value_cache` (see [Operations](@/guides/operate.md)). Without it,
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
| `replicated_write` | Write with replication factor 3 (frozen cluster tier) |
| `objstore_tiered_read` | 1 MB segment round-trip via in-memory object store |
| `multi_tenant` | 100 namespaces × 10 points each |
| `large_scale_targeted` | 1M pre-loaded points, 1K range query |
