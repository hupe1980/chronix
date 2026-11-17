+++
title = "Run on a gateway"
description = "Fit Chronix on a 512 MB gateway: the small preset, memory budgets, fsync on flash, serial reads and the numbers to expect."
weight = 90
+++

Chronix runs on a 512 MB gateway with `Chronix::open_small` (or `[database]` settings to match). This guide explains what that preset sets, how to size memory, and what to expect from flash storage.

## Workload Profiles

Every engine knob lives under `[database]`. The defaults suit a workstation;
these are the two directions worth moving in.

### Ingestion-heavy

High write throughput with periodic batch queries — IoT, infrastructure
monitoring.

```toml
[database]
wal_fsync_policy = "periodic_5000" # fsync every 5 s, not every batch
memtable_flush_threshold = 200000  # Rows; fewer, larger flushes
max_memtable_memory = 268435456    # 256 MiB across all shards
compaction_concurrency = 4         # More parallel compaction
compression = "lz4"                # Cheapest to write
```

`wal_fsync_policy` is the one with a real trade: `per_write` survives a
power cut with no loss, `per_batch` loses at most one in-flight batch, and
`periodic_<ms>` bounds the fsync *rate* — which on flash is the wear rate —
at the cost of a bounded window of unsynced writes.

**An encrypted column costs nothing measurable on the write path.** Its
values are sealed in the write-ahead log once per batch per key, and on
`cargo bench --bench chronix_bench -- insert_batch_sealed` a 10 000-point
batch takes 20.3 ms with or without an encrypted field (Apple M4 Pro,
`per_batch` fsync): the fsync dominates, not the AES-GCM.

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

`DatabaseStatistics::resident_memory_bytes()` is the sum of five terms, each
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

### A declared detector's pass

`detector_pass/50_series_x_60_points` in `chronix_bench` times one
maintenance pass at the same shape — a minute of 50 series at 1 Hz, 3 000
points scored and written as verdicts: **15.6 ms** on an Apple M4 Pro, most
of it the verdict write (eight fields a point, one fsync). At one pass a
minute that is a quarter of a thousandth of one core.

### Server-side budgets

```text
Write path  = memtable + WAL buffer + compaction buffers
  max_memtable_memory      64–256 MB, depending on cardinality
  compaction               ~128 MB per concurrent worker

Read path   = segment cache + per-query pool
  segment_cache_size       1–8 GB — larger means fewer disk reads
  per_query_memory_limit   64–256 MB per concurrent query
```

## Numerical Precision

### Kahan Compensated Summation

All SIMD sum paths (scalar fallback, SSE2, AVX2, AVX-512, NEON) use Kahan
compensated summation. This reduces floating-point rounding error from O(nε)
(naive accumulation) to O(ε), ensuring accurate aggregation results even on
very large series (millions of points). No configuration is required — Kahan
summation is always active.
