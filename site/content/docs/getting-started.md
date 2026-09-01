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

Open a database, write a point, read it back — this is
[`examples/quickstart.rs`](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/quickstart.rs),
which CI compiles and runs on every change:

```rust
use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A data directory is the whole deployment. It is file-locked, so a
    // second process cannot open it by accident.
    let config = ChronixConfig::builder().data_dir("/tmp/quickstart").build()?;
    let db = Chronix::open(config)?;

    // A point is a series key (measurement + tags), some fields, and a
    // timestamp in **nanoseconds**.
    let key = SeriesKey::new("cpu", tags! { "host" => "web-01" })?;
    let point = Point::new(
        key,
        fields! { "usage_idle" => 95.5 },
        1_700_000_000_000_000_000,
    )?;
    db.insert(&point)?;

    // Query with the builder. Without `.range()` the plan covers all time.
    let plan = db.query().measurement("cpu").build()?;
    let batch = db.execute(&plan)?; // an Arrow RecordBatch
    println!("{} rows", batch.num_rows());

    // `close()` flushes and truncates the WAL. Dropping without it is safe —
    // recovery replays the log — but closing makes the next open faster.
    db.close()?;
    Ok(())
}
```

On a memory-constrained machine — a gateway with 512&nbsp;MB of RAM and flash
storage — use the preset instead of hand-tuning:

```rust
let db = Chronix::open_small("/var/lib/chronix")?;
```

It sets a ~48&nbsp;MB memory budget and a periodic WAL fsync policy that is
gentler on eMMC and SD cards. See [Performance](@/docs/performance.md) for what
each knob trades away.

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
curl -X POST localhost:8086/api/v1/write/influx \
  --data-binary 'cpu,host=web-01 usage_idle=95.5 1700000000000000000'

curl -X POST localhost:8086/api/v1/query \
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

**PromQL**, tracking Prometheus 3.x semantics — this is the same request
Grafana sends a Prometheus data source:

```bash
curl 'localhost:8086/api/v1/prom/query?query=rate(cpu[5m])'
```

**Analytics in the query**, rather than in a service beside it:

```sql
SELECT FORECAST(usage_idle, 24)   AS predicted,
       ANOMALY(usage_idle)        AS anomaly_score
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
