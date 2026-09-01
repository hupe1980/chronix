+++
title = "Caching"
description = "Chronix uses three complementary caches to minimise disk I/O and reduce query latency."
weight = 70
+++

Chronix uses three complementary caches to minimise disk I/O and
reduce query latency.

## Last-Value Cache (LVC)

The LVC provides **O(1) lookup** of the most recent point for any series.
It is implemented as a `DashMap<SeriesKey, Arc<Point>>` — a sharded
concurrent hash map that avoids global locking.

### Design Rationale

Many monitoring dashboards display "current value" gauges that only need the
latest observation. Without the LVC, this requires scanning the memtable or
the most recent segment — O(log n) at best. The LVC short-circuits this to
a direct hash lookup.

The cache is updated **synchronously on every insert**, ensuring it is always
consistent with the most recent write. There is no cache invalidation problem
because the cache is append-only (newer values always replace older ones).

### Memory Cost

Each entry stores a `SeriesKey` (~100 bytes for typical tag sets) and an
`Arc<Point>` (~64 bytes). For 1 million active series, the LVC uses
approximately 160 MB.

## Segment Cache (TinyLFU)

Decoded columnar arrays are cached in a **scan-resistant TinyLFU cache**
bounded by a configurable memory limit. TinyLFU (Einziger et al., 2017)
combines a frequency sketch with a segmented LRU to admit only items that
have been accessed frequently enough, preventing sequential scans from
evicting hot data.

### TinyLFU Admission Theory

Pure LRU is vulnerable to sequential scan thrashing: a single full-table scan
evicts all hot entries. TinyLFU addresses this with a **frequency sketch**
(Count-Min Sketch) that estimates access frequency for each key. New items
are only admitted if their estimated frequency exceeds the victim's frequency.

The architecture consists of three sections:

- **Window LRU** (~1% of capacity): absorbs new entries
- **Probationary LRU** (~20%): candidates for promotion
- **Protected LRU** (~79%): frequently accessed items

Items entering the cache pass through the window section. On eviction from
the window, the frequency sketch decides whether the item is admitted to
the probationary section or discarded.

This gives near-optimal hit rates for Zipfian workloads while providing
robust scan resistance — matching the behavior of Caffeine (Java) and
Ristretto (Go).

### Implementation

The cache is implemented as a `LinkedHashMap` guarded by a `RwLock`. Reads
acquire a read lock (allowing concurrent access), while inserts and evictions
acquire a write lock. Cache entries are keyed by `(segment_id, column_name)`
and hold `Arc<dyn arrow::Array>`, enabling zero-copy sharing across
concurrent readers.

## Metadata Cache

Segment header metadata (time range, row count, column stats, encoding info)
is cached in memory on startup. At approximately **1 KB per segment**,
even a database with 100 000 segments uses only ~100 MB for metadata.

This cache enables the query engine to perform multi-level pruning (time
index, stats-based column elimination) without any disk I/O.

## Cache Coherence

All three caches follow a simple coherence model:

| Event | LVC | Segment Cache | Metadata Cache |
|-------|-----|---------------|----------------|
| Insert | Update | No change | No change |
| Flush | No change | No change | Add segment |
| Compaction | No change | Invalidate old | Replace old |
| Delete series | Remove | No change | No change |
| Drop measurement | Clear | Invalidate | Remove |

Because segment files are immutable, the segment and metadata caches never
need invalidation except during compaction (when old segments are replaced)
or explicit deletion.
