+++
title = "Rollups and retention"
description = "Keep raw data for days and summaries for years: declare calendar rollup tiers, set retention per measurement, and delete data safely."
weight = 60
+++

Keep raw data for as long as you need detail, and summaries for as long as you need history. Declare rollup tiers on calendar buckets, set retention per measurement, and Chronix keeps raw data until every tier that needs it is materialised.

## Retention and rollups

Retention drops data older than its window, where age is measured from
`min(wall clock, newest timestamp the database holds)`
(see [Operations](@/guides/operate.md)). Rollups pre-aggregate raw data
into coarser tiers — 1&nbsp;s → 1&nbsp;min → 15&nbsp;min — and are
**materialised**: every bucket is aggregated over every row that reaches it,
once the out-of-order window has closed over it.
`db.materialise_rollups()` does the work; `compact()` and
`enforce_retention()` call it, and a persisted watermark per rollup records
how far it has got, so a restart neither repeats nor forgets. A tier fed by
another tier advances exactly as far as its source has, so a cascade is
consistent by construction.

**And rollups are repaired, because "the window has closed" is not a proof
of finality.** A backfill, a delete or an import can change a bucket's input
long after the watermark passed it. Each of those records the range it
touched, and the next materialisation pass recomputes exactly those buckets:

```rust
// A device was offline for a day; its backlog arrives now.
db.backfill(&points)?.into_complete()?;
// The next pass recomputes the buckets that backlog changed —
// or do it immediately:
db.refresh_rollup("energy_15m", start_ns, end_ns)?;
```

Deleting raw data reaches the derived tiers the same way: the aggregate of a
deleted series is removed on the next pass, so an erasure request does not
leave a summary of the erased rows behind.

Retention is **rollup-aware**: data is dropped only once every rollup it
feeds — the whole chain — has been materialised past it *and* has no repair
pending. A retention pass that ran ahead of the rollup chain would trade a
year of raw readings for an aggregate that was never built; a segment still
needed is preserved and counted, and the next pass tries again.

See [Operations](@/guides/operate.md) for configuration.

## Deletes

A delete names a **time interval of a series**. There is no form that masks a
series forever:

- An unbounded delete resolves its upper bound to the newest timestamp that
  series actually holds, so **writing to a deleted series re-creates it** —
  which is what re-provisioning a device under a recycled identifier looks
  like.
- Re-ingesting *into* an interval that is still tombstoned stays masked, until
  compaction materialises the delete. This is the same rule Prometheus and
  InfluxDB apply.

Deletes are durable as soon as they return, and the rows leave the disk when
compaction next rewrites the affected segments. A delete can also be
**partial** — a segment that cannot be read is skipped — so anything acting on
an erasure obligation must check the reported `segments_skipped` and retry.

## Retention and Rollups

Retention (`retention_secs`, and `measurement_retention` per measurement) is
enforced by the database's own maintenance thread every
`maintenance_interval` (30 s by default). It is opt-in: with no retention
configured, nothing is ever deleted.

**Age is measured from `min(wall clock, newest timestamp the database
holds)`.** Two consequences:

- A clock that reads far ahead — a dead RTC, a bad NTP server, a restored VM
  snapshot — cannot expire data the data itself does not justify expiring.
- A database that stops receiving writes keeps its history. One fresh write
  moves the reference to the present and the backlog expires on the next pass.

The newest data is therefore never expired, whatever the rule says, and a
measurement that stops being written stops freeing disk. Use `DELETE` to
reclaim that space.

**Retention releases what it deletes.** On any pass that deleted something,
the cardinality budget (`max_series_cardinality`, reported as
`chronix_series_count`) and the last-value cache are re-derived from what the
database still holds, so device churn does not climb towards the cardinality
limit and `last_value()` stops answering for a series whose data has aged out.
`chronix_series_released_total` counts the released series. An emptied
measurement is still a queryable table returning no rows; it leaves the
measurement list only when it is dropped.

Rollups are materialised, never computed as a by-product of compaction or
retention. A bucket is aggregated once, over a deduplicated scan of its
source, as soon as its input is final: for a raw measurement, once the
out-of-order window (±2 shards of the newest write) has closed over it; for
a tier fed by another rollup, as far as that rollup has been materialised.
`compact()` — which the maintenance thread runs every `maintenance_interval`
— materialises after each pass, `enforce_retention()` materialises before
it drops, and `db.materialise_rollups()` is the explicit call. Definitions
and watermarks live in the **catalog**, fsynced before the call that changed it
returns and carried forward by every snapshot, so a rollup cannot be lost to a
half-written file beside the data directory.

`db.rollup(name, start, end)` reads the tier as a view: the materialised
buckets below the watermark spliced onto the unfinished ones above it,
aggregated from the source with the same accumulator. Reading the target
measurement directly instead ends a chart short by however much has not been
materialised yet. `db.rollup_where(name, start, end, &[("host", "a")])` is the
same view restricted to matching series; every key must be one of the rollup's
`group_by_tags`, because the target measurement carries only those and a
filter on any other key would narrow the live half and match nothing in the
materialised one.

**Late writes are repaired, not ignored.** A `backfill` or a delete into a
range a rollup has already aggregated records an *invalidation*; the next
pass deletes the stale aggregates over that range, recomputes them, and
cascades the repair to any tier downstream. `refresh_rollup(name, start,
end)` does the same immediately, for a change the engine could not have
observed — an out-of-band restore, say. `chronix_rollup_invalidations_total`
counts what has been marked, `chronix_rollup_ranges_recomputed_total` what
has been repaired, and `chronix_rollup_failures_total` any rollup whose pass
failed — the others still run.

Rollup-aware retention trades raw data for its aggregates, so a segment is
only dropped once **every** tier of its rollup chain has been materialised
past it and has no repair pending — for the per-measurement rules as well as
the global one. A segment that is still needed is preserved and counted in
`chronix_retention_segments_awaiting_rollup_total` and in the pass's
`segments_preserved`; a segment that cannot be read fails the
materialisation that needs it, so it is preserved too. `shards_dropped`
counts the shards the pass **removed**, so a pass that dropped nothing logs
`Retention preserved every expired segment` rather than a count.
`chronix_rollup_points_written_total` counts what materialisation wrote.
Cold archiving removes a segment from the hot database, so it waits on the
same gate (`chronix_cold_archive_groups_awaiting_rollup_total`).

Retention runs whenever **any** rule is configured — global,
per-measurement, or a rollup's own `retention_secs`.

A rollup's `retention_secs` (`retention_ns` in the Rust API) is the
retention of its *target* measurement, applied by the same pass.

## Partial Deletes

**Metric:** `chronix_delete_segments_skipped_total` counter.

A predicate delete skips segments it cannot open or read rather than aborting,
so a non-zero value means matching data may still be on disk. Alert on any
increase if you rely on deletes for an erasure obligation:

```yaml
- alert: ChronixPartialDelete
  expr: increase(chronix_delete_segments_skipped_total[15m]) > 0
  annotations:
    summary: "A delete could not scan every segment — data may remain"
```

The same signal is returned inline as `segments_skipped` on the HTTP and gRPC
delete responses, so callers can retry without waiting for a scrape.

## Tombstone backlog

**Metric:** `chronix_tombstone_gc_total` counter.

A delete writes tombstones; compaction is what removes the rows from disk. A
tombstone is reclaimed only once every segment it was issued against has been
rewritten or deleted, so the gap between "the delete returned" and "the bytes
are gone" is one compaction cycle for the affected segments. Two operational
consequences:

- **An erasure obligation is not discharged by the delete alone.** If you must
  be able to say the bytes are gone, force a compaction of the affected
  measurement and confirm `chronix_tombstone_gc_total` advanced.
- **A tombstone set that never shrinks** means compaction is not reaching those
  segments — check `chronix_compaction_tasks_completed_total` and the L0 depth
  rather than the delete path.

Deletes themselves are durable as soon as they return: tombstones are appended
to the catalog manifest and fsynced, not left to the data WAL, which is
truncated on the next flush.
