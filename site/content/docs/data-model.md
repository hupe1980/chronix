+++
title = "Data Model"
description = "Measurements, series, tags and fields in Chronix — how data is identified, why tag cardinality is the number that matters, and how time sharding, retention and deletes behave."
weight = 15
+++

Five ideas cover almost everything you will do with Chronix. If you have used
InfluxDB or Prometheus, most of this will be familiar; the places it differs are
called out.

## Point, series, measurement

A **point** is the unit of writing: a timestamp, a set of **fields** (the
values), and a **series key** that says what the values describe.

```text
measurement:  power
tags:         { meter = "main", phase = "L1" }     ← identity, indexed
fields:       { watts = 231.45, volts = 229.8 }    ← the data
timestamp:    1700000000000000000                  ← i64, nanoseconds
```

- A **measurement** is a named collection of related series — the rough
  equivalent of a table.
- **Tags** identify the series. They are strings, they are indexed, and the set
  of distinct tag combinations is what determines how much memory the database
  needs.
- **Fields** are what you are recording. They are not indexed, and each may be
  `f64`, `i64`, `u64`, `bool` or `String`.
- A **series** is one measurement plus one exact tag set. `power{meter=main,
  phase=L1}` and `power{meter=main, phase=L2}` are two series.

Timestamps are always **nanoseconds since the Unix epoch**, as a signed 64-bit
integer. Negative values (before 1970) are valid and handled throughout.

### What the timestamp column is called

One column, and each surface spells it the way that surface's users expect:

| Surface | Name |
|---------|------|
| SQL (`/api/v1/chronix/sql`, Flight SQL, `db.sql`) | `_time`, typed `TIMESTAMP(nanosecond)` |
| `GET /api/v1/measurements/{name}/schema` | `_time` — the name a query accepts |
| A JSON query row, and an Arrow batch from `execute_iter` | `timestamp`, `Int64` |
| The embedded `MeasurementSchema` | `time` |

In SQL the column is `_time` and only `_time` — in `SELECT` and in `WHERE`
alike. `WHERE timestamp > …` is a planning error naming the columns that
exist, which is the right failure: it is loud, and it says what to type.
Writes reject `time`, `timestamp` and `_time` as *user* column names, so
nothing you send can collide with it.

### Tag or field?

The question is whether you will ever *filter or group by* it.

- Filter or group by it → **tag**. `host`, `region`, `device_id`, `phase`.
- Only read it → **field**. `watts`, `temperature`, `bytes_sent`.

Putting a high-cardinality value in a tag — a request id, a timestamp, a user
id — creates one series per distinct value. That is the single most common way
to make a time-series database unusable, and it is not specific to Chronix.

### How a field is named in PromQL

SQL and the query builder address a measurement and pick columns from it.
PromQL addresses a **metric**, which carries one value per sample, so each
field is its own metric:

```text
power,meter=main watts=231.45,volts=229.8
                 ↓
PromQL:  power_watts{meter="main"}   power_volts{meter="main"}
```

The rule is `<measurement>_<field>`, with one exception: a field named `value`
gives the measurement name alone. That is how the Prometheus remote-write and
OTLP ingestion paths store a sample, so a scraped `up{job="api"}` is queried
as `up`, not `up_value`.

A metric's name depends only on its own measurement and field, so writing a
new field to a measurement never renames the metrics already in it. Ask
`/api/v1/label/__name__/values` for the names that exist.

## Cardinality is the number that matters

Distinct series, not points, is what bounds memory. Writing a million points to
fifty series is cheap; writing a thousand points to a million series is not.

Chronix tracks the live series count **exactly** and enforces
`max_series_cardinality` (default 1,000,000) on the write path. Exceeding it
rejects the write with `CardinalityExceeded` rather than degrading quietly. A
delete that removes a whole series returns it to the budget.

```rust
let config = ChronixConfig::builder()
    .data_dir("/var/lib/chronix")
    .max_series_cardinality(50_000)   // fail fast, well below what the box can hold
    .build()?;
```

## Time sharding

Data is partitioned into **shards** by time — one hour by default. A shard is
the unit of retention, compaction and tiering, which has two consequences worth
knowing before you tune anything:

- **Out-of-order writes are accepted within ±2 shards** of the newest write
  (not of the clock — a device that resumes after days offline is still
  "now"). Beyond that they are rejected *before* anything is made durable,
  and the rejection is *reported* rather than swallowed: `insert_batch`
  returns an `InsertResult { accepted, rejected }` that is `#[must_use]`, and
  `into_complete()` turns a partial batch into an error if you want
  strictness.
- **Writing outside the window is `backfill`.** Importing history is a
  different operation with a different cost — the shards it touches are
  opened, flushed and compacted like any other — so it has its own name.
- **Shard duration is a memory parameter, not only a compaction one.** Scans
  merge one shard-sized bucket at a time, so peak query memory tracks the shard
  size rather than the size of the result.

## Retention and rollups

Retention drops data older than its window. Rollups pre-aggregate raw data
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

See [Operations](@/docs/operations.md) for configuration.

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

## Last write wins

Two points with the same series key and the same timestamp collapse to one: the
later write. This is resolved consistently across the memtable, the segments
and compaction, so a query returns the same answer before and after a flush.

## Where this is implemented

- [Storage engine](@/internals/storage-engine.md) — how shards, segments and
  the memtable fit together
- [Crate layout & data model](@/reference/crate-layout.md) — the types, the
  canonical series form, and why its separators are reserved
- [Query execution](@/internals/query-engine.md) — how a scan is bounded
