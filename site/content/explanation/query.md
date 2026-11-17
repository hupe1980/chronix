+++
title = "Query execution"
description = "How a Chronix query runs: layered pruning with blooms and zone maps, the inverted tag index, bounded-memory scans, caches and DataFusion."
weight = 90
aliases = ["/internals/query-engine/", "/internals/indexing/", "/reference/indexing-and-query/", "/internals/caching/"]
+++

A query reads as little as it can. It narrows the measurement's segments by
time, tag index, series bloom and column statistics; inside each surviving
segment it skips row groups by time, tag bloom and zone map; and only then
decodes the projected columns. Scans run in bounded memory, one time-disjoint
bucket at a time. SQL is DataFusion on top of the same scan; PromQL and the
native `QueryBuilder` use it directly.

## Pruning, level by level

| Level | Uses | Skips | Metric |
|---|---|---|---|
| Segment: time | the catalog entry's `[min, max]` timestamp | segments outside the range | `chronix_segments_pruned_by_time_and_index_total` |
| Segment: tags | the inverted tag index | segments it can prove lack a filtered `key=value` | (same) |
| Segment: series | a bloom filter of the segment's series keys | segments without the queried series | — |
| Segment: stats | per-column catalog statistics | segments whose filtered tag column is all null | — |
| Row group: time | per-row-group timestamp min/max | row groups outside the range | `chronix_segment_rg_pruned_by_time_range_total` |
| Row group: tags | a bloom per tag column per row group | row groups that cannot hold the value | `chronix_segment_rg_pruned_by_bloom_total` |
| Row group: fields | zone maps (min/max per column) | row groups a numeric predicate rules out | `chronix_segment_rg_pruned_by_zone_map_total` |

`Chronix::execute_with_stats` returns `PruningStats` beside the result —
`segments_total`, `pruned_by_time`, `pruned_by_bloom`, `pruned_by_stats`,
`segments_remaining` — so a slow query can be explained. `EXPLAIN` in SQL and
the PromQL explain route show the same plan from the outside.

**Pruning only ever fails towards more work.** The tag index is derived state,
updated after the catalog, so a segment can be queryable before it is indexed.
The index therefore answers the negative question — *can it prove this segment
lacks a pair?* — and a segment it has not seen is scanned, not skipped. Zone
maps follow the same rule: a decimal predicate's bounds are widened by one
mantissa unit, so a value binary floating point cannot represent exactly never
prunes the row group that holds it.

### The structures behind it

- **Catalog.** Every segment's measurement, time range, row count, size and
  per-column statistics. Crash-safe through an append-only manifest with
  CRC-32C framing and periodic snapshots; open replays the snapshot and the
  manifest after it.
- **Series blooms and the tag index.** Rebuilt at open from each segment's
  `.series` sidecar — the segment's distinct series keys — so nothing decodes a
  segment to start. A missing or corrupt sidecar is rebuilt once from the
  segment's tag columns. The blooms use double hashing (two hashes produce all
  *k* positions) at a 1 % target false-positive rate.
- **Row-group metadata.** Each column block carries min/max, null and distinct
  counts and, for tag columns, a bloom. A validity bitmap stored unencrypted
  answers "does this field have a value here?" without decoding or decrypting
  it, which is what keeps `/labels` exact.

## Bounded-memory scans

Segments are written series-major, so their row groups are not in time order
and cannot be merged lazily row group by row group. Time-partitioned segments
rarely overlap, though, so `execute_iter` groups the surviving segments into
**time-disjoint buckets** and merges one bucket at a time: memory is the
largest bucket, not the result. Segments that all overlap become one bucket —
slower, never wrong.

| Plan | Peak memory |
|---|---|
| Scan | one bucket plus one 64 Ki-row output chunk |
| Aggregate | one accumulator per group and field |
| Downsample | one open time bucket |
| Limit | the rows taken; stopping the iterator stops the reads |

Within a bucket a k-way merge resolves duplicates **last write wins** per
series and timestamp. Distinct series that share a timestamp are never merged.
`export_parquet` drives the same iterator, so a gateway can export a window
larger than its memory; `execute_stream` collects it and enforces
`max_query_result_bytes`.

**Order and limits.** The scan's output is ascending in `_time`, and SQL knows
it: `ORDER BY _time [DESC] LIMIT n` is answered by the scan itself — descending
reads walk the buckets newest first — so "the latest ten rows" reads one bucket,
not the table.

**Parallelism.** How many threads a scan may use is the database's
`parallelism` setting: serial in `ChronixConfig::small`, otherwise the scan reads
a bucket's segments in parallel. Segments are memory-mapped; the page cache is
shared by every reader.

## The native query API

`QueryBuilder` builds three plan shapes over a scan:

```rust
let plan = QueryBuilder::new()
    .measurement("cpu")
    .tag("host", "web-01")
    .range(start, end)
    .aggregate(AggFn::Mean)
    .group_by(&["region"])
    .build()?;
```

| Plan | Result |
|---|---|
| `Scan` | the rows, time-ordered and deduplicated |
| `Aggregate` | `Sum`, `Min`, `Max`, `Mean`, `Count`, `First`, `Last` over the range, optionally grouped by tags |
| `Downsample` | the same functions per time bucket, using the `TimeBucket` calendar SQL and rollups share |

`First` and `Last` pick by timestamp, not by arrival order. Grouping chooses a
sort-based path for low cardinality and a hash-based one otherwise; `EXPLAIN`
names the choice.

## SQL

SQL is [Apache DataFusion](https://datafusion.apache.org/) 55 with each
measurement as a table, scoped to the caller's namespace. Chronix adds:

- **The scan** (`ChronixExec`): time-range and tag filters, projection, field
  predicates for zone maps, the `_time` ordering above, and statistics for the
  optimizer.
- **Bounded memory:** a fair spill pool of `per_query_memory_limit` (256 MiB by
  default), spilling to a directory under the data directory, and
  `target_partitions` equal to the parallelism budget.
- **Epoch literals:** `WHERE _time >= 1700000000000000000` is rewritten to a
  timestamp literal before type coercion, so integer epochs work as they do on
  every other surface.
- **Read-only HTTP:** `/api/v1/chronix/sql` refuses DDL, DML, `COPY` and
  session statements at the logical plan, including inside `EXPLAIN`.
- **Discovery:** `SHOW TABLES`, `SHOW COLUMNS` and `information_schema` answer
  from the namespace's own measurements only.
- **Functions:** time bucketing, counter rates, rolling statistics, STL,
  forecasting and anomaly scores — see the [SQL reference](@/reference/sql.md).

## Caches

| Cache | Holds | Keyed by | Kept coherent by |
|---|---|---|---|
| Last-value cache (opt-in: `enable_last_value_cache`) | the newest point per series | series key | updated on insert; series removed on delete, retention, GC and archive |
| Segment cache | decoded column blocks | segment, row group, column | segments are immutable; entries go when a segment is compacted away or retired |

The segment cache is bounded by memory and uses W-TinyLFU admission: a
frequency sketch admits a block only if it is used more often than the block it
would evict, so one large scan cannot flush the hot set. Concurrent misses on
the same block load it once.

## Related

- [Storage](@/explanation/storage.md) — segments, compaction and the catalog
- [SQL reference](@/reference/sql.md)
- [Performance tuning](@/guides/tune-performance.md)
