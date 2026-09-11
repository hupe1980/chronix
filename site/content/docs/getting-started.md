+++
title = "Getting Started"
description = "Install Chronix as a Rust dependency or run the chronixd server. Write your first points and query them with the builder, SQL and PromQL."
weight = 10
+++

Chronix runs two ways from the same engine and the same on-disk format:
**embedded**, as a crate inside your binary, and **as a server**, with
`chronixd`. Start with whichever matches how you want to deploy — nothing you
learn in one is wasted on the other.

## Embedded

Add the crate:

```bash
cargo add chronix
```

SQL is on by default. If you do not need it, `default-features = false` drops
DataFusion and takes **96 seconds off a clean build** (131 s against 227 s for
a small consumer); everything else — writes, the native query API, PromQL,
rollups, retention, analytics, triggers, the cold archive — is unaffected. It
is not a way to shrink the binary: the linker already discards DataFusion when
nothing calls it, so the difference there is 0.34 MiB.

Open a database, write a point, read it back — this is
[`examples/quickstart.rs`](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/quickstart.rs),
which CI compiles and runs on every change:

```rust
use chronix::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A data directory is the whole deployment. It is file-locked, so a
    // second process cannot open it by accident. `open_small` is the
    // gateway preset; `Chronix::open(ChronixConfig::builder()…)` is the
    // general form.
    let db = Chronix::open_small("/tmp/quickstart")?;

    // A point is a series key (measurement + tags), some fields, and a
    // timestamp in **nanoseconds**.
    let key = SeriesKey::new("cpu", tags! { "host" => "web-01" })?;
    let point = Point::new(
        key,
        fields! { "usage_idle" => 95.5 },
        1_700_000_000_000_000_000,
    )?;
    db.insert(&point)?;

    // Query with the builder — an Arrow RecordBatch back.
    let plan = db.query().measurement("cpu").build()?;
    let batch = db.execute(&plan)?;
    println!("{} rows", batch.num_rows());

    // …or with SQL. Every measurement is a table, `_time` is the timestamp.
    let batches = db.sql("SELECT _time, host, usage_idle FROM cpu")?;
    println!("{} batches", batches.len());

    // `close()` flushes and records the WAL floor, so the next open replays
    // nothing. Dropping the last handle does the same.
    db.close()?;
    Ok(())
}
```

`open_small` sets a ~48&nbsp;MB memory budget and a periodic WAL fsync
policy that is gentler on eMMC and SD cards — the preset for a gateway with
512&nbsp;MB of RAM. For anything else, build a `ChronixConfig`; see
[Performance](@/docs/performance.md) for what each knob trades away.

A `Chronix` maintains itself: one built-in thread flushes memtables as they
fill and, every `maintenance_interval` (30 s), compacts, materialises
rollups and enforces retention. There is nothing to start and nothing to
schedule — in the embedded case or in `chronixd`.

`Chronix` is a handle: `clone()` is cheap, every clone shares the database,
and the last one dropped closes it. Hand clones to threads freely. `db.sql`
blocks on a private runtime; inside an async runtime call `db.sql_async`.

## Server

```bash
cargo install chronixd
chronixd --data-dir /var/lib/chronix
```

Defaults: HTTP on **8086**, gRPC on **8087**, Arrow Flight SQL on **8817**.
The HTTP port matches InfluxDB's so existing agents need no reconfiguration.

Write a point and read it back:

```bash
# Native JSON
curl -X POST localhost:8086/api/v1/write \
  -H 'Content-Type: application/json' \
  -d '{"measurement":"cpu","tags":{"host":"web-01"},
       "fields":{"usage_idle":95.5},"timestamp":1700000000000000000}'

# …or InfluxDB line protocol, which Telegraf already speaks
curl -X POST localhost:8086/write \
  --data-binary 'cpu,host=web-01 usage_idle=95.5 1700000000000000000'

curl -X POST localhost:8086/api/v1/chronix/query \
  -H 'Content-Type: application/json' \
  -d '{"measurement":"cpu"}'
```

A successful write answers `204 No Content`; a query answers a JSON array of
rows.

## Querying

Three query surfaces read the same data.

**SQL**, through DataFusion, with time-series functions added:

```sql
SELECT time_bucket('5m', _time) AS bucket,
       avg(usage_idle)          AS avg_idle
FROM cpu
WHERE host = 'web-01'
GROUP BY bucket
ORDER BY bucket;
```

`time_bucket` takes an optional time zone, and then a day means a day:

```sql
SELECT time_bucket('1d', _time, 'Europe/Berlin') AS day,
       avg(usage_idle)                           AS avg_idle
FROM cpu
GROUP BY day
ORDER BY day;
```

The **unit decides**: `'30s'`, `'5m'` and `'1h'` are a fixed span that never
varies, while `'1d'`, `'1w'`, `'1mo'` and `'1y'` follow that zone's calendar —
so a transition day is 23 or 25 hours and February is February. The month is
`'mo'`; `'m'` is always the minute.

A fourth argument moves the boundary, for a period that does not start at
midnight on the 1st:

```sql
-- A billing month that runs from the 15th to the 15th.
SELECT time_bucket('1mo', _time, '', '2024-01-15') AS period,
       max(reading) - min(reading)                 AS kwh
FROM meter GROUP BY period ORDER BY period;

-- A shift day that starts at 06:00 in Berlin.
SELECT time_bucket('1d', _time, 'Europe/Berlin', '2024-01-01 06:00:00') AS shift,
       avg(usage_idle)                                                  AS avg_idle
FROM cpu GROUP BY shift ORDER BY shift;
```

What matters is where the origin falls in the cycle, not how far away it is.
For a width of one unit — `1d`, `1mo` — that is just its time of day or day
of month, so any date with the right shape will do. For a multi-unit width it
also picks which unit opens a bucket: `3mo` anchored on a January gives
calendar quarters, anchored on a February gives February–April.

A monthly bucket must start on day 1–28: a boundary on the 29th, 30th or 31st
does not exist in every month, and is refused rather than clamped.

The long spellings work too, with or without the space: `'1 hour'`,
`'15 minutes'`, `'3 months'`. A bare `'M'` is refused rather than guessed at.

The time column is `_time` — the same name the schema endpoint, an Arrow
batch and a `.csx` segment use, so what you read back is what you type. It
compares against an epoch-nanosecond integer, an RFC 3339 string and `now()`
alike, so the numbers the write API gave you work unchanged:

```sql
SELECT * FROM cpu WHERE _time >= 1700000000000000000 LIMIT 10;
```

`SHOW TABLES` lists your measurements, `SHOW COLUMNS FROM cpu` their
columns, and `EXPLAIN` tells you whether a time filter actually narrowed the
scan — the difference between reading three segments and reading all of
them:

```sql
EXPLAIN SELECT * FROM cpu WHERE _time >= 1700000000000000000;
```

```text
ChronixExec: measurement=cpu, time=[1700000000000000000..9223372036854775807], filters=0, limit=None
```

**PromQL**, tracking Prometheus 3.x semantics — this is the same request
Grafana sends a Prometheus data source:

```bash
curl 'localhost:8086/api/v1/query?query=rate(cpu_usage_idle[5m])'
```

The metric is `cpu_usage_idle`, not `cpu`: a measurement holds several fields
and a PromQL metric holds one value, so each field is its own metric named
`<measurement>_<field>`. A field called `value` — which is how Prometheus
remote write and OTLP store a sample — gives the measurement name alone.
`curl localhost:8086/api/v1/label/__name__/values` lists the names that exist.

**Analytics in the query**, rather than in a service beside it:

```sql
SELECT forecast(usage_idle, _time, 24) AS predicted
FROM cpu;

-- `anomaly_score` is a window function: it needs the ordering to score
-- against, so it is always used with OVER.
SELECT _time,
       anomaly_score(usage_idle, 3.0) OVER (PARTITION BY host ORDER BY _time)
         AS score
FROM cpu;
```

## Where to go next

- [Data model](@/docs/data-model.md) — measurements, series, tags versus
  fields, and why cardinality is the number that matters
- [API reference](@/docs/api-reference.md) — every REST, gRPC, Flight SQL and
  embedded entry point, with examples
- [Analytics](@/docs/analytics.md) — forecasting and anomaly detection in depth
- [Operations](@/docs/operations.md) — configuration, metrics, retention, backup
- [Grafana](@/docs/grafana.md) — dashboards and the data-source setup
- [Examples](@/docs/examples.md) — runnable programs covering every subsystem
- [Internals](@/internals/storage-engine.md) — how the engine actually works

## A note on maturity

Chronix is pre-1.0 and has no production deployments yet. The on-disk format
and the public API are **not stable**, and the distributed tier is deliberately
frozen — see [Cluster](@/docs/cluster.md). What *is* solid is the single-node
engine: every performance and compression number in this documentation names
the test that pins it, and the gaps are written down rather than implied away.
