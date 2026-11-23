# Chronix

**The embedded-first time-series database with analytics built in.**

A Cargo dependency inside a gateway binary, or `chronixd` behind Grafana — one engine, one
file format. It stores, queries with SQL and PromQL, forecasts and detects anomalies, in pure Rust.

<!-- example: crates/chronix/examples/readme.rs -->
```rust
use chronix::arrow::util::pretty::print_batches;
use chronix::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Chronix::open_small("./chronix-data")?; // the gateway preset
    let key = SeriesKey::new("solar", tags! { "panel" => "roof" })?;
    let t0 = 1_700_000_000_000_000_000_i64; // nanoseconds since the epoch
    for i in 0..60 {
        let watts = 100.0 + 20.0 * f64::from(i); // a morning ramp, one point a minute
        let at = t0 + i64::from(i) * 60_000_000_000;
        db.insert(&Point::new(key.clone(), fields! { "watts" => watts }, at)?)?;
    }
    let sql = "SELECT time_bucket('10m', _time) AS t, avg(watts) FROM solar GROUP BY t ORDER BY t";
    print_batches(&db.sql(sql)?)?;
    let next = db.auto_forecast("solar", "watts", &[], t0, t0 + 3_600_000_000_000, 10, None)?;
    let model = &next.selection.label; // picked by cross-validation
    println!("{model} forecasts {:?} …", &next.result.values[..3]);
    db.close()?;
    std::fs::remove_dir_all("./chronix-data")?;
    Ok(())
}
```

```sh
cargo add chronix                                      # the library
brew install hupe1980/tap/chronixd && chronixd --demo  # the server, with a generated week of data
```

`chronixd --demo` serves a generated gateway week — PV, battery, grid and heat pump, with two
planted anomalies — on `127.0.0.1:8086`, and prints queries to paste and the console's address.
`cargo install chronixd` builds it from source; prebuilt binaries are on the [releases page](https://github.com/hupe1980/chronix/releases).

## Why Chronix

- **It runs inside your binary.** A file-locked directory, a crash-safe WAL and one
  maintenance thread that flushes, compacts, rolls up and enforces retention. The gateway
  preset peaks **under 25 MiB** of heap for an hour of ingest and rollups.
- **Read a whole quarter with one timestamp.** Declare a tier at startup with
  `ensure_rollup`, then read a bucket by its start. Live and stored buckets have
  the same boundaries, typed empty results and integer sample counts.
  [Rollups and retention](https://hupe1980.github.io/chronix/guides/rollups-and-retention/).
- **Analytics are in the engine.** Forecasting with the model picked by cross-validation,
  anomaly detection, and detectors you declare once and the store keeps — callable from Rust,
  SQL and the server. Estimates are checked against statsmodels and scipy.
- **Exact where exactness is the point.** A `Decimal` field type keeps meter registers and
  prices exact through every write path, aggregate and wire format. Pcodec compresses
  realistic meter data **5–29×**, and decimal registers up to **475×**.
- **It speaks the protocols you run.** Prometheus remote write 1.0 and 2.0 and PromQL
  (conforming to Prometheus 3.16), OpenTelemetry OTLP, InfluxDB line protocol and Arrow
  Flight SQL — each tested with the bytes the real client sends.
- **Agents can use it.** `/mcp` gives Claude and other MCP clients read-only tools to
  query, explain, forecast and detect anomalies in the database.
- **You can look at it.** `/console` is a page in the binary: PromQL and SQL with
  completion, charts with the forecast band and anomalies drawn on, cardinality and query
  plans — [the console](https://hupe1980.github.io/chronix/docs/console/).

## The server

```sh
chronixd --data-dir /var/lib/chronix     # HTTP :8086 · gRPC :8087 · Flight SQL :8817
chronixd --config /etc/chronixd.toml
chronixd inspect /var/lib/chronix        # what a data directory holds, safe while it runs
chronixd mcp --data-dir ./gateway-copy   # MCP over stdio, from a directory opened read-only
```

```sh
# InfluxDB line protocol — Telegraf writes here unchanged
curl -X POST http://localhost:8086/write -d 'cpu,host=srv1 usage=72.5'

# PromQL — what a Grafana Prometheus data source sends
curl 'http://localhost:8086/api/v1/query?query=cpu_usage'

# SQL, with forecasting as an aggregate
curl http://localhost:8086/api/v1/chronix/sql -H 'content-type: application/json' \
  -d '{"query":"SELECT unnest(forecast(usage, _time, 12)) AS next FROM cpu"}'
```

Point Grafana at `http://localhost:8086` as a Prometheus data source; the bundled dashboards
come out of `chronixd --export-dashboards ./dashboards`.

## Feature flags

The embedded build carries only what it names.

| Crate | Flag | Default | Adds |
|-------|------|---------|------|
| `chronix` | `sql` | on | SQL through DataFusion; implies `analytics` |
| `chronix` | `analytics` | on | Forecasting, anomaly detection, declared detectors |
| `chronix`, `chronix-engine` | `field-encryption` | on | Per-column AES-256-GCM |
| `chronix` | `streaming` | off | Change feed, subscriptions, triggers and webhooks |
| `chronix` | `security` | off | API keys, JWT, mTLS, Cedar policies, audit, namespaces |
| `chronix` | `pipeline` | off | The real-time pipeline; implies `streaming` and `security` |
| `chronix`, `chronix-engine` | `object-store` | off | Parquet cold tier on S3, GCS or Azure |
| `chronixd` | `console` | on | The console at `/console` |
| `chronixd` | `mcp` | on | The MCP server: `/mcp` and `chronixd mcp` |
| `chronixd` | `kafka`, `mqtt` | off | Ingestion connectors, pure Rust |
| `chronixd` | `otlp` | off | OTLP trace export of the server's own spans |
| `chronixd` | `object-store` | off | Periodic cold archiving |

## Documentation

- **[hupe1980.github.io/chronix](https://hupe1980.github.io/chronix)** — guides, the API
  reference, operations, and how the engine works
- **[docs.rs/chronix](https://docs.rs/chronix)** — the Rust API
- **[Examples](crates/chronix/examples/)** — one runnable program per feature:
  `cargo run -p chronix --example quickstart`
- **[CHANGELOG](https://github.com/hupe1980/chronix/blob/main/CHANGELOG.md)** — read it before
  a minor-version upgrade
- **[CONTRIBUTING](CONTRIBUTING.md)** — building, testing and the release procedure

## Status

Pre-1.0: a minor release may change the API, and the changelog lists every break. The on-disk
format is **generation 2 and frozen** — a later release does nothing to a data directory. A
directory written by chronix 0.7 or earlier is refused at open with the remedy in the message.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
