+++
title = "Quickstart: embedded"
description = "Add Chronix to a Rust project, write points and query them with the builder, SQL and PromQL — an embedded time-series database in five minutes."
weight = 10
aliases = ["/docs/getting-started/"]
+++

Chronix is a library: add the crate, open a directory, and the database runs
inside your process — no server, no daemon. This tutorial writes a point and
reads it back with the query builder, SQL, PromQL and a forecast.

## Add the crate

Use Rust 1.95 or newer.

```bash
cargo add chronix
```

The default build includes SQL, analytics and field encryption. Optional features add the rest:

| Feature | Adds | Default |
|---|---|---|
| `sql` | `db.sql()` on DataFusion, `EXPLAIN`, the SQL analytics functions; implies `analytics` | on |
| `analytics` | Forecasting, anomaly detection, declared detectors, preprocessing | on |
| `streaming` | The change-data-capture bus, `db.subscribe()`, triggers and webhook delivery | off |
| `security` | API keys, JWT/OIDC, mTLS, Cedar authorization, namespaces, the audit chain | off |
| `field-encryption` | Per-column encryption at rest, sealed in the WAL too | on |
| `pipeline` | CDC → triggers → delivery, streaming detection, continuous forecasting; implies `streaming` and `security` | off |
| `object-store` | The Parquet cold tier on S3, GCS or Azure; implies `sql` | off |

`default-features = false` gives a storage-and-query build without
DataFusion, which compiles about 96 s faster; writes, the native query API,
PromQL, rollups and retention are unaffected.

## Write and read

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

What the program relies on:

- **One handle.** `Chronix` is cheap to `clone()`; every clone shares the
  database and the last one dropped closes it. `db.sql` blocks on a private
  runtime; inside async code use `db.sql_async`.
- **It maintains itself.** A built-in thread flushes memtables as they fill
  and, every 30 s, compacts, materialises rollups and enforces retention.
- **`open_small` is the gateway preset**: ≈ 48 MB of memory budgets, a periodic
  WAL fsync that spares flash, serial reads. `Chronix::open(config)` takes a
  full [`ChronixConfig`](@/reference/configuration.md).

## Query three ways

**SQL** — every measurement is a table and `_time` is the timestamp:

```sql
SELECT time_bucket('5m', _time) AS bucket, avg(usage_idle) AS avg_idle
FROM cpu WHERE host = 'web-01'
GROUP BY bucket ORDER BY bucket;
```

**PromQL**, in-process, with Prometheus 3.x semantics. Each field is its own
metric named `<measurement>_<field>`:

```rust
let value = db.promql("rate(cpu_usage_idle[5m])", now_ns)?;
```

**Analytics in the query** — a forecast is an aggregate, an anomaly score a
window function:

```sql
SELECT host, unnest(forecast(usage_idle, _time, 24)) AS predicted
FROM cpu GROUP BY host;

SELECT _time, anomaly_score(usage_idle, 3.0) OVER (PARTITION BY host ORDER BY _time) AS score
FROM cpu;
```

`anomaly_score` is between 0 and 1, and above 0.5 past the threshold.

## Next

- [Quickstart: the server](@/learn/server.md) — the same engine behind HTTP, PromQL and Grafana
- [Your first gateway](@/learn/gateway.md) — a detector that survives restarts
- [Data model](@/reference/data-model.md) — measurements, tags, fields and cardinality
- [SQL](@/reference/sql.md) and [PromQL](@/reference/promql.md) — every function
- [Examples](@/guides/examples.md) — thirty-plus runnable programs
