+++
title = "Storage Engine"
description = "Chronix uses a Log-Structured Merge-tree (LSM-tree) architecture adapted for time-series workloads. This design separates the fast write path from the optimised read path, achieving high ingestion…."
weight = 10
+++

Chronix uses a **Log-Structured Merge-tree (LSM-tree)** architecture adapted
for time-series workloads. This design separates the fast write path from the
optimised read path, achieving high ingestion throughput without sacrificing
query performance.

## LSM-Tree Overview

The LSM-tree was introduced by O'Neil et al. (1996) as a way to transform
random writes into sequential I/O. The core idea is simple:

1. **Buffer writes in memory** — incoming data is appended to an in-memory
   data structure (the *memtable*). Writes are immediately durable because
   they are first recorded in a *write-ahead log* (WAL).

2. **Flush to immutable files** — when the memtable reaches a size threshold,
   it is frozen and written to disk as an immutable *sorted run* (in Chronix:
   a columnar *segment file*).

3. **Merge in the background** — a compaction process periodically merges
   overlapping segments to bound read amplification and reclaim space from
   deleted or overwritten data.

```text
Write Path                          Read Path
──────────                          ─────────
  insert()                          query()
     │                                 │
     ▼                                 ▼
  ┌─────┐                      ┌─────────────┐
  │ WAL │  (durability)        │  Memtable    │ ← scan in-memory
  └──┬──┘                      └──────┬──────┘
     │                                │
     ▼                                ▼
  ┌──────────┐                 ┌─────────────┐
  │ Memtable │ (in-memory)     │  Segments   │ ← multi-level pruning
  └────┬─────┘                 │  (on disk)  │   bloom → time → stats
       │  freeze & flush       └─────────────┘
       ▼
  ┌──────────┐
  │ Segment  │ (immutable, columnar, compressed)
  └──────────┘
       │  background
       ▼
  ┌──────────┐
  │Compaction│ (merge-sort, dedup, tombstone cleanup)
  └──────────┘
```

## Write Amplification

A key metric for LSM-trees is **write amplification** (WA) — the ratio of
total bytes written to disk versus bytes of user data. In a tiered compaction
strategy with *T* size ratio and *L* levels:

```
WA = O(T · L)
```

Chronix uses **Time-Window Compaction Strategy (TWCS)**, which bounds WA by
ensuring segments are only compacted within the same time window. This avoids
rewriting cold historical data and is optimal for append-mostly time-series
workloads.

## Space Amplification

Space amplification is the ratio of disk space used to the logical data size.
LSM-trees can have temporary space amplification during compaction (old + new
segments coexist). Chronix mitigates this with:

- **Soft-delete with grace period** — old segments are marked for deletion but
  retained briefly for in-flight queries.
- **Per-measurement segments** — each measurement flushes independently,
  limiting the blast radius of compaction.
- **Columnar compression** — **6.2×** end to end on 2-decimal meter readings,
  measured by `chronix/tests/segment_compression.rs`. Individual columns do far
  better (timestamps 30–55×, decimal floats 4–9× under ALP); a segment's ratio
  is set by its worst column and its fixed metadata.

## Read Amplification

Without pruning, a query must scan all segments. Chronix minimises read
amplification through **multi-level pruning**:

| Level | Technique | Eliminates |
|-------|-----------|------------|
| 1 | Time-range index | Segments outside the query window |
| 2 | Bloom filter | Segments that don't contain the target series |
| 3 | Column stats | Segments where all values are NULL for a column |
| 4 | Tag inverted index | Segments without matching tag values |
| 5 | Zone maps | Row groups outside predicate bounds |

After pruning, typically only 1–3 segments need actual decoding, keeping read
latency low even with thousands of segments on disk.

## Data Flow

The complete lifecycle of a write:

```text
1. Client calls db.insert(point)
2. Point is serialised and appended to WAL (fsync per policy)
3. Point is inserted into the active memtable (lock-free skip list)
4. Last-value cache is updated
5. CDC event is published to the EventBus
   ├── Signal triggers evaluate conditions
   ├── Streaming anomaly engine scores the point
   └── Continuous forecast engine updates models
6. When memtable size > threshold:
   a. `FlushScheduler` is signalled via `Notify` (non-blocking)
   b. Background task debounces rapid signals
   c. Memtable is frozen (atomic swap)
   d. New empty memtable replaces it
   e. Frozen memtable is flushed to segment(s) via `spawn_blocking`
   f. Time index, bloom filter, tag index, zone maps are updated
   g. Segment catalog is persisted
   h. WAL is truncated
7. Background compaction merges L0 segments
```

## References

- P. O'Neil, E. Cheng, D. Gawlick, E. O'Neil. "The Log-Structured
  Merge-Tree (LSM-Tree)." *Acta Informatica*, 33(4), 1996.
- N. Dayan, M. Athanassoulis, S. Idreos. "Monkey: Optimal Navigable
  Key-Value Store." *SIGMOD*, 2017.
