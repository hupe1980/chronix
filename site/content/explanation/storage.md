+++
title = "Storage engine"
description = "The Chronix LSM write path and read path: WAL, memtable, time-sharded .csx segments, the catalog and the on-disk format generation."
weight = 20
aliases = ["/internals/storage-engine/", "/reference/storage-engine/"]
+++

Chronix is a log-structured merge engine shaped for time series. A write goes
to the write-ahead log and an in-memory table, and is acknowledged. The
memtable is flushed to an immutable, compressed, columnar `.csx` segment per
time shard; compaction merges a shard's segments; a catalog records which
segments exist. One maintenance thread does all the background work, so an
embedded database needs nothing started beside it.

```text
insert ──▸ WAL ──▸ memtable (per time shard) ──flush──▸ .csx segment ──compact──▸ .csx
                                                           │
query ◂── memtable + segments, merged, last write wins ◂───┘
```

| Part | What it is | Details |
|---|---|---|
| WAL | Append-only log, CRC-32C per record, group commit | [Write-ahead log](@/explanation/wal.md) |
| Memtable | Concurrent skip list per time shard | [Memtable and segments](@/explanation/memtable-and-segments.md) |
| Segment | Immutable columnar file: row groups, encoded and compressed column blocks, statistics | [Memtable and segments](@/explanation/memtable-and-segments.md), [Encoding](@/explanation/encoding.md) |
| Catalog | Every segment's time range, size and column statistics; manifest plus snapshots | [Query execution](@/explanation/query.md) |
| Compaction | Merges one shard's segments, deduplicating and applying deletes | [Compaction](@/explanation/compaction.md) |
| Cold tier | Hive-partitioned Parquet on object storage | [Object storage](@/explanation/object-storage.md) |

## Writing

1. The point is encoded and appended to the WAL. When it is durable depends
   on `fsync_policy`: every write, every batch (the default), or periodically
   (`ChronixConfig::small`: every 5 s).
2. It is inserted into the memtable of its **time shard** (one hour by
   default). A point older than the shards still accepting writes is refused;
   `backfill` is the door for history.
3. The change is published for [triggers and subscribers](@/explanation/streaming-and-triggers.md).
4. When the memtable passes `memtable_flush_threshold` (64 MiB; 8 MiB in
   `small`), writers swap in a fresh one and wake the maintenance thread, which
   writes the frozen memtable as segments. The WAL is truncated only after every
   shard has flushed.

The same point written twice — same series, same timestamp — keeps the last
write, in the memtable and across segments alike.

## Background work

The maintenance thread runs on its own OS thread, every `maintenance_interval`
(30 s; 60 s in `small`; zero disables it) and whenever a memtable fills:

| Step | Does |
|---|---|
| Flush | Writes frozen memtables as segments |
| Compact | Merges a shard's small segments; materialises rollups |
| Garbage collection | Removes retired files and spent tombstones |
| Retention | Drops shards past `retention`, once every rollup that needs them has caught up |
| Checkpoints | Takes and prunes scheduled backups, when configured |

`close()` stops and joins it.

## Deleting

`delete` and `delete_series` flush first, so a delete only reasons about
segments. Each matching series gets a **tombstone** for the time range — an
fsynced catalog entry — scoped to the segments that existed when it was issued.
Reads apply tombstones; compaction removes the covered rows; the tombstone is
discarded once those segments are gone. Data written after the delete is not
affected. A series that is deleted entirely leaves the cardinality budget and
the last-value cache.

## Retention

`retention` drops whole shards whose newest point is older than the cutoff.
The cutoff is measured from the **earlier** of the wall clock and the newest
timestamp the database holds, so a gateway whose clock jumps forward, or that
was offline for a month, does not delete the data it has not yet replaced.
Catalog entries are removed before files, so a crash in between leaves orphan
files, never dangling entries. After a pass, the live series set and the
cardinality budget are re-derived from what remains.

## Rollups

A rollup materialises a source measurement into a coarser target —
`avg`, `min`, `max`, `first`, `last`, `sum`, `count` per bucket — and can feed
another rollup. Buckets are `TimeBucket`s: a fixed width below a day, the local
calendar for days, weeks, months and years, so a daily tier runs midnight to
midnight in its time zone.

- **Final buckets only.** A bucket is written once nothing more can arrive for
  it; a persisted **watermark** records how far each rollup has got. Reading a
  rollup with `Chronix::rollup` adds the unfinished buckets, computed from the
  source on the fly.
- **Late data repairs.** A backfill or delete below the watermark records an
  invalidated range; the next pass recomputes it, and every tier downstream.
- **Crash-safe.** Rollup points are synced before the watermark that claims
  them is persisted.
- **Retention waits.** Raw data is dropped only after every rollup rooted at it
  has materialised past it, with no repair pending.

Definitions, watermarks and invalidations live in the catalog, as do
[declared detectors](@/guides/forecast-and-detect.md), which follow the same
rules.

## On disk

```text
<data_dir>/
├── LOCK                         holder: pid, host, start time, version
├── wal/                         wal_<first sequence>.cxwl
├── catalog/                     manifest + snapshot
└── segments/shard_<id>/
    ├── seg_<nanos>_<n>.csx      the segment
    └── seg_<nanos>_<n>.series   its series keys, rebuilt if missing
```

Paths in the catalog are relative to `segments/`, so a data directory can be
moved or restored anywhere. `chronixd inspect <dir>` reads all of this without
opening the database.

**Format generation 2.** The segment, WAL, catalog and series-sidecar formats
carry one generation, checked for equality: a directory written by another
generation is refused at open by name, never misread. The formats are frozen
from chronix 0.8.

**Compression.** Columns are encoded per block — pco for numbers and
timestamps, dictionaries for tags — and optionally LZ4- or Zstd-compressed.
On two-decimal meter readings a whole segment is **6.2×** smaller than the raw
values; individual columns reach far more. See [Encoding](@/explanation/encoding.md).

## Related

- [Architecture](@/explanation/architecture.md)
- [Rollups and retention](@/guides/rollups-and-retention.md)
- [Backup and restore](@/guides/backup-and-restore.md)
