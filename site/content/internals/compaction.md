+++
title = "Compaction Strategies"
description = "Compaction is the background process that merges overlapping segment files, reclaims space from deleted data, and bounds read amplification."
weight = 50
+++

Compaction is the background process that merges overlapping segment files,
reclaims space from deleted data, and bounds read amplification.

## Why Compaction?

As the database runs, the write path produces a growing set of L0 segment
files — one per memtable flush. Without compaction:

- **Read amplification grows** — a query must scan all segments
- **Space amplification grows** — deleted and overwritten data is never reclaimed
- **Bloom filter efficiency drops** — more segments means more false-positive probes

## Time-Window Compaction Strategy (TWCS)

Chronix implements **TWCS**, originally developed for Apache Cassandra's
time-series workloads. The key insight is:

> Time-series data is written in temporal order and queried by time range.
> Segments covering the same time window should be merged together, but
> segments from different windows should never be combined.

### Algorithm

```text
1. Group L0 segments by their time window (e.g. 1-hour buckets)
2. For each window with > 1 segment:
   a. Merge-sort the segments by (series_key, timestamp)
   b. Apply last-write-wins deduplication
   c. Remove tombstoned series
   d. Write the merged output as a new segment at L1
3. Soft-delete the input segments
4. Update indexes (bloom, time, tag, stats)
```

### Why TWCS is Optimal for Time Series

| Property | Benefit |
|----------|---------|
| Never rewrites cold data | Historical segments are compacted once and never touched again |
| Bounded write amplification | Each point is rewritten at most once per level |
| Time-aligned segments | Perfect for time-range queries — minimal segment overlap |
| Simple TTL enforcement | Drop all segments in a window that exceeds retention |

### Multi-Level Compaction

Chronix uses a three-level hierarchy:

```text
L0: Raw flush output (many small, overlapping segments)
    │  TWCS merge
    ▼
L1: Compacted per time window (non-overlapping within each window)
    │  Size-tiered merge (when L1 count grows)
    ▼
L2: Fully compacted (one segment per time window)
```

## Hybrid Compaction Strategy

Pure TWCS can leave many small segments within a time window under bursty
ingestion. The **hybrid strategy** adds a size-tiered pass within each
time window:

1. TWCS groups segments by time window (primary strategy)
2. Within each window, segments are classified by **size tier**:
   - `Tiny` (< 1 MB), `Small` (1–10 MB), `Medium` (10–100 MB), `Large` (> 100 MB)
3. When a tier accumulates ≥ *threshold* segments (default: 4), they are merged
4. Cross-shard merges are never permitted

### Configuration

```rust
use chronix_engine::compaction::{CompactionPolicy, CompactionStrategy, SizeTier};

let policy = CompactionPolicy::new(CompactionStrategy::Hybrid {
    time_window: Duration::from_secs(3600),
    size_tier_threshold: 4,
});
```

Call `policy.validated()` to clamp thresholds to safe ranges:
`count_trigger_threshold` and `size_tier_threshold` are clamped to
`[2, 1000]`, and `max_write_amplification` to `[1.0, 1000.0]`.

### Write-Amplification Tracking

`WriteAmpTracker` uses atomic counters to track cumulative bytes flushed
vs. bytes rewritten by compaction. When the ratio exceeds
`max_write_amplification_ratio` (default: 10×), low-priority merges are
deferred to the next cycle.

## Write Amplification Analysis

For TWCS with *T* segments per window before compaction:

| Operation | Writes |
|-----------|--------|
| Initial flush | 1× (L0) |
| TWCS merge (L0 → L1) | 1× (read T + write 1) |
| Final merge (L1 → L2) | 1× (if L1 accumulates) |
| **Total** | **≤ 3×** |

Compare this to **leveled compaction** (RocksDB-style), which can reach
10–30× write amplification. TWCS achieves much lower WA because time-series
data naturally partitions by time.

## Merge-Sort with Last-Write-Wins

During compaction, overlapping points (same series key + timestamp) are
resolved by keeping only the latest write. This is a stable sort-merge:

```text
Segment A: (key=cpu/host=web1, ts=100, val=72.5)
Segment B: (key=cpu/host=web1, ts=100, val=73.1)  ← later write
Result:    (key=cpu/host=web1, ts=100, val=73.1)  ← B wins
```

### Tombstones

Compaction is what **materialises** a delete: the merge drops every row a
tombstone masks as it writes the output segment, so the rows leave the disk
here and nowhere else.

Each tombstone records the segment ids that were live when the delete was
issued. After a compaction pass, a tombstone whose segments have all left the
catalog is reclaimed — a rewritten segment no longer holds the rows, and a
deleted one holds nothing at all. That is the only condition under which
dropping a tombstone is safe. A rule keyed on the *series* rather than on its
segments fires after any compaction pass, including one that never touched the
segments holding the deleted rows, and the data comes back.

## Backpressure

When L0 segment count exceeds a threshold, the write path applies
**backpressure** — inserts are artificially slowed to let compaction catch up.
This prevents unbounded growth of L0 segments under sustained high write load.

## References

- B. Jia, T. Tu, et al. "Apache Cassandra's Time-Window Compaction
  Strategy." *Cassandra Documentation*, 2016.
- L. Lu, T. S. Pillai, et al. "WiscKey: Separating Keys from Values
  in SSD-Conscious Storage." *FAST*, 2016.
