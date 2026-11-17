+++
title = "Data model"
description = "Points, series, measurements, tags and fields in Chronix; value types including exact decimals; time sharding, cardinality and last-write-wins."
weight = 80
aliases = ["/docs/data-model/"]
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
  `f64`, `i64`, `u64`, `bool`, `String`, an exact
  [`Decimal`](#exact-decimals-for-money-and-meters), or a
  [native `Histogram`](#native-histograms).
- A **series** is one measurement plus one exact tag set. `power{meter=main,
  phase=L1}` and `power{meter=main, phase=L2}` are two series.

Timestamps are always **nanoseconds since the Unix epoch**, as a signed 64-bit
integer. Negative values (before 1970) are valid and handled throughout.

### The time column is `_time`

One name everywhere: the embedded `MeasurementSchema`, an Arrow batch from
`execute_iter`, a `.csx` segment, SQL, PromQL and the schema endpoint. What
the schema reports is what a query accepts.

```sql
SELECT _time, usage FROM cpu WHERE _time >= 1700000000000000000;
```

Not `time` or `timestamp`: both are SQL type keywords, and the leading
underscore marks the column as engine-owned. All three are rejected as *user*
column names, so nothing you write can collide with it.

The **type** differs by layer: `Int64` nanoseconds in storage, which is what
it encodes, and `TIMESTAMP(nanosecond)` in SQL, so date functions and
interval arithmetic work.

A **point** is not a table, and its `timestamp` field keeps that name — in a
JSON write body, a JSON query row and the gRPC `Point` message, as in every
protocol that carries points.

### The start timestamp is `_start`

When the series that produced a sample started counting. A counter that
restarts — a process restart, a gateway reboot — begins again from zero, and
the start timestamp is what tells a reader that it *started* rather than
wrapped. Prometheus remote write 2.0 and OpenTelemetry both send one per
sample, and chronix stores it:

```sql
SELECT _time, _start, value FROM http_requests_total ORDER BY _time DESC LIMIT 5;
```

- It is a nullable `TIMESTAMP(nanosecond)` beside `_time`, and the column exists
  only once a point has carried one — a measurement that never receives one
  pays nothing. A constant start costs about 0.06 bytes per row.
- Remote write 2.0 sets it from each sample's `start_timestamp` (`0` is
  unset), OTLP from `start_time_unix_nano` on sums, histograms and summaries.
  Line protocol has none. A start later than its sample is refused and counted.
- Embedded, set it with `Point::with_start_timestamp(ns)` and read it with
  `Point::start_timestamp()`. It belongs to the row: every field of a point
  shares one start.
- It is not a metric, a label or a rollup value. `rate()` does not use it yet,
  because Prometheus itself ships that behind a feature flag; the data is
  already stored, so turning it on later changes a query, not your data.

### Tag or field?

The question is whether you will ever *filter or group by* it.

- Filter or group by it → **tag**. `host`, `region`, `device_id`, `phase`.
- Only read it → **field**. `watts`, `temperature`, `bytes_sent`.

Putting a high-cardinality value in a tag — a request id, a timestamp, a user
id — creates one series per distinct value. That is the single most common way
to make a time-series database unusable, and it is not specific to Chronix.

### Native histograms

Every field type above answers with **one number**. A native histogram answers
with a whole **distribution**, in one sample.

A classic Prometheus histogram is a family of series — `foo_bucket` once per
boundary, plus `foo_sum` and `foo_count` — with boundaries fixed by whoever
instrumented the code. A native histogram implies its buckets from a *schema*,
so the resolution is a number, the buckets are sparse, and nothing has to be
guessed in advance.

```rust
use chronix::chronix_core::histogram::Histogram;

let mut latency = Histogram::empty(3);   // 8 buckets per power of two
latency.zero_threshold = 1e-6;           // [-1µs, +1µs] counts as zero
for sample in observed_seconds {
    latency.observe(sample);
}

let mut fields = BTreeMap::new();
fields.insert("latency".into(), FieldValue::Histogram(Box::new(latency)));
db.insert(&Point::new(key, fields, now)?)?;
```

Reading one back gives you the distribution, not a summary of it:

```rust
h.quantile(0.99);            // interpolated inside the bucket
h.fraction(0.0, 0.250);      // what share was under 250 ms
h.count(); h.sum(); h.avg(); h.stddev();
h.classic_buckets();         // the `le`-bucketed view
```

Schemas −4 to 8 (exponential) and −53 (explicit boundaries); the zero bucket;
positive and negative observations; merging two histograms of one schema. A
histogram with unsorted buckets is refused when the point is built — an invalid
one interpolates to a quantile that is plausible and wrong.

#### Querying

```promql
histogram_quantile(0.99, http_latency)    # p99, from the native buckets
histogram_count(http_latency)             # observations
histogram_sum(http_latency)               # their sum
histogram_avg(http_latency)               # sum / count
histogram_stddev(http_latency)            # and stdvar
histogram_fraction(0, 0.25, http_latency) # share under 250 ms
```

`histogram_quantile` dispatches on what the series holds — native buckets, or a
classic `le`-bucketed series. The rest of the family **refuses a float series
by name**: `histogram_count` of a gauge is not the gauge.

A rollup tier over a histogram column **merges** its samples: `sum` is the
merge, `avg` scales it by 1/n, `min` and `max` are NULL because distributions
have no ordering, and two schemas in one bucket are refused rather than
silently re-bucketed.

#### Ingest

```yaml
scrape_configs:
  - job_name: api
    scrape_native_histograms: true

remote_write:
  - url: "http://chronix:8086/api/v1/prom/write"
    send_native_histograms: true
    # Optional. Omit for remote write 1.0, which also carries histograms.
    protobuf_message: io.prometheus.write.v2.Request
```

Both remote-write versions are accepted, chosen by the `Content-Type`'s
`proto=` parameter; an unrecognised one is a `415` naming the two that work. A
2.0 write answers `X-Prometheus-Remote-Write-Samples-Written` and
`-Histograms-Written`, so a sender can tell "stored" from "dropped" — both are
`204`. Remote read returns histograms too.

An OpenTelemetry Collector needs no special configuration:

| OTel instrument | Stored as |
|---|---|
| Exponential histogram (base-2) | a native histogram at the matching schema |
| Explicit-bucket histogram | a native histogram with custom boundaries (schema −53) |

The histogram goes in the metric's **own** column, so the name you query is the
name you exported.

#### The classic names

A stored histogram also answers `foo_bucket`, `foo_count` and `foo_sum`, so an
existing panel needs no change:

```promql
http_latency_bucket                             # every boundary, ending at le="+Inf"
http_latency_count
http_latency_sum
histogram_quantile(0.99, http_latency_bucket)   # same answer as the native form
```

This is a **view** — nothing is stored twice, and the cumulative counts are
computed from the buckets on read. The names are resolvable but not *listed*,
so `/api/v1/label/__name__/values` shows `http_latency` alone and
`{__name__=~".+"}` does not return the same observations three times. A
measurement with a real float field called `count` keeps it: the view applies
only to histogram columns.

The `le` values are the boundaries the histogram **has**:

| Schema | Boundaries | A hand-written `le="0.5"` |
|---|---|---|
| −53, custom buckets — OTLP explicit-bucket, or a classic histogram | the ones the instrumentation chose | **matches** |
| −4…8, exponential | powers of `2^(2^-schema)`: `…, 0.435, 0.475, 0.516, …` | matches nothing |

So a dashboard migrating off classic histograms keeps working — its data
arrives as custom buckets and its `le` literals are the boundaries. Against an
exponential histogram, `http_latency_bucket` and a heatmap over it both work,
but a query pinned to `le="0.5"` returns empty rather than the nearest bucket.

Chronix stores no **exemplars**, and tells a 2.0 sender so with
`X-Prometheus-Remote-Write-Exemplars-Written: 0`.

### Exact decimals, for money and meters

`f64` cannot represent `0.1`. For a temperature or a load average that does
not matter; for a meter register a bill is computed from, it is the whole
problem. Chronix has a sixth field type for those:

```rust
// A quarter-hour register in kWh to four decimal places.
db.declare_field("meter", "z1nb_q", ColumnType::Decimal { scale: 4 })?;

let reading: Decimal = "1234.5678".parse()?;
let key = SeriesKey::new("meter", tags! { "device" => "main" })?;
db.insert(&Point::new(key, fields! { "z1nb_q" => reading }, now)?)?;
```

The question is not "how precise does this need to be" but **what kind of
number is this**:

| | `f64` | `Decimal` |
|---|---|---|
| A sensor reading, a rate, a ratio | ✅ | |
| A quantity someone is billed for, or a regulator audits | | ✅ |
| `SUM` / `MIN` / `MAX` / `FIRST` / `LAST` | approximate | **exact** |
| `AVG` | approximate | exact to scale + 6, rounded half away from zero |
| Forecasting, anomaly detection, percentiles | native | converted, and says so |
| In SQL | `DOUBLE` | `DECIMAL(38, s)` — DataFusion aggregates it exactly |

A decimal never passes through binary floating point: not on the way in, not
in the write-ahead log, not in a segment, not in a query result, and not on
any wire format. Where an answer genuinely cannot be exact — a moving
average, a Holt-Winters term, a PromQL sample — the conversion happens in one
named place and the result is a `DOUBLE`, so you can see it happen.

**In SQL a decimal literal is a decimal**, as in PostgreSQL, so `WHERE
z1nb_q > 0.1` and `SELECT z1nb_q + 0.05` are both exact. A `DOUBLE` column is
unaffected; write `CAST(1.5 AS DOUBLE)` for a literal that must be a float
whatever it meets.

**Integer arithmetic in SQL wraps on overflow.** `SELECT v + 1` where `v` is
`9223372036854775807` answers `-9223372036854775808` rather than failing, and
there is no setting for it. Sum an `i64` column whose values approach the
type's range as `DECIMAL` or `DOUBLE`, or bound it in the query. Decimal
arithmetic is unaffected: exact, or rejected.

#### The scale belongs to the column

A decimal column stores a fixed number of fractional digits, and that number
is fixed when the column is created. Every later write is rescaled to it:

- Fewer digits than the column? Widened exactly — `1.5` into a scale-4
  column is stored as `1.5000`.
- More digits than the column? **Rejected**, naming both. Storing it would
  mean rounding it.

So declare the column before the first write — `db.declare_field(…)` above,
or `POST /api/v1/measurements/{name}/schema/fields`. Otherwise the first
point decides, and a device that reports `231.4` before `231.45` pins the
column at one digit.

The limits are 38 significant digits and a scale of 0–38, which is what
Arrow's `Decimal128` carries. A value outside them is refused at ingest
rather than truncated.

#### Writing one over the wire

A JSON number is parsed as a `double` by every client library there is, so a
decimal travels as **digits in a string**:

```json
{ "measurement": "meter",
  "tags":   { "device": "main" },
  "fields": { "z1nb_q": { "decimal": "1234.5678" } },
  "timestamp": 1700000000000000000 }
```

Line protocol uses a `d` suffix, beside the `i` and `u` you already know:

```text
meter,device=main z1nb_q=1234.5678d 1700000000000000000
```

Reading a point back gives you the same `{"decimal": "…"}` shape, so a point
you read is a point you can write. A SQL result cell is a plain string of
digits, with `decimal(38, 4)` in the column metadata beside it.

Arrow Flight `DoPut` carries a `Decimal128` column directly, which is how a
`pyarrow` table of registers bulk-loads.

If your own values are already `rust_decimal::Decimal`, the `rust_decimal`
feature gives `TryFrom` in both directions. Chronix does not depend on it
otherwise: the representation is the same `i128` mantissa plus scale that
`rust_decimal` and PostgreSQL `NUMERIC` use.

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
  Embedded, that is `db.backfill(&points)`; over the network it is
  `?backfill=true` on every write path and `WriteRequest.backfill` on gRPC
  ([API Reference](@/reference/http-api.md#backfilling-history)). It is an
  explicit opt-in on both, never an automatic fallback: routing anything late
  to the backfill path would let one client with a wrong clock open a shard
  per hour of history.
- **Shard duration is a memory parameter, not only a compaction one.** Scans
  merge one shard-sized bucket at a time, so peak query memory tracks the shard
  size rather than the size of the result.

## Last write wins

Two points with the same series key and the same timestamp collapse to one: the
later write. This is resolved consistently across the memtable, the segments
and compaction, so a query returns the same answer before and after a flush.

## Where this is implemented

- [Storage engine](@/explanation/storage.md) — how shards, segments and
  the memtable fit together
- [Crate layout & data model](@/explanation/architecture.md) — the types, the
  canonical series form, and why its separators are reserved
- [Query execution](@/explanation/query.md) — how a scan is bounded
