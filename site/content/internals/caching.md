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

`SegmentCache` is a `HashMap` with a doubly-linked LRU list threaded through
it, so eviction is O(1), guarded by one `RwLock`; the TinyLFU frequency sketch
sits behind its own `Mutex` so a hit on the read-lock fast path can still
record frequency. Entries are keyed by `SegmentCacheKey { segment_id,
row_group, column }` and hold an Arrow `ArrayRef`, so a cached column is
shared zero-copy across concurrent readers.

**Singleflight:** when several threads miss on the same key at once, one loads
and the rest wait on a `tokio::sync::Notify` — chosen over a `Condvar` so a
waiter yields its tokio worker instead of blocking it.

## Cache Coherence

Both caches follow a simple coherence model:

| Event | LVC | Segment Cache |
|-------|-----|---------------|
| Insert | Update | No change |
| Flush | No change | No change |
| Compaction | No change | Invalidate old |
| Delete series | Remove | No change |
| Drop measurement | Clear | Invalidate |
| Retention / GC / cold archive | **Remove released series** | Invalidate |

Because segment files are immutable, the segment cache never needs
invalidation except when a segment stops existing — compaction replacing it,
or a delete, retention, GC or an archive removing it.

The LVC is a **copy** of each series' newest row, so retention, GC and cold
archiving prune it through the same repair that releases the cardinality
budget. That is also what bounds it on a long-lived deployment: its entries
are the live series, and the live series are bounded by retention.
