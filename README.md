# Chronix

**The embedded-first time-series database with analytics built in.**

Chronix is an analytics-native TSDB written in pure Rust. Add it as a Cargo
dependency for an in-process database (`Chronix::open()`), or run **`chronixd`**
— a standalone server with REST, gRPC, Arrow Flight SQL, PromQL, and
Prometheus/OTLP wire compatibility. Same engine, same file format, no external
dependencies, no sidecars.

From a 512 MB IoT gateway logging sensor data at 1 s resolution to a server
backing Grafana dashboards with real-time anomaly alerts — it is the same
crate.

## Why Chronix

- **Embedded-first** — a library, not a deployment. File-locked data
  directory, crash-safe WAL, graceful `close()`, and a database that
  maintains itself: one built-in thread flushes, compacts, materialises
  rollups, enforces retention and, when configured, takes a **checkpoint** on
  a schedule and prunes the old ones — so there is nothing to start and no
  daemon to run. Runs where a database *server* cannot.
- **Exact where exactness is the point** — a `Decimal` field type (`i128`
  mantissa × 10⁻ˢᶜᵃˡᵉ), because one class of time series is not a measurement
  but a legal quantity: a quarter-hour meter register a settlement is computed
  from, a price, an invoice line. `f64` cannot represent `0.1`, and a
  settlement that went through a `double` is one nobody can reproduce. The
  scale belongs to the **column**, fixed when it is created; a value with more
  digits is refused rather than rounded; `SUM`, `MIN`, `MAX`, `FIRST` and
  `LAST` — a rollup's included — stay exact in `i128`; SQL sees
  `DECIMAL(38, s)`, and a decimal literal is a decimal, so arithmetic on a
  register is exact too. It costs no compression: the mantissa *is* the
  integer ALP and pco spend their first stage recovering from a double, worth
  **475×** against 85× on noisy connection-point power.
- **Analytics inside the engine** — statistical forecasting (SES, Holt,
  Holt-Winters, ARIMA/SARIMA), anomaly detection (Z-score, MAD, IQR,
  forecast-residual, CUSUM, seasonal thresholds), drift detection, and
  programmable triggers run against zero-copy Arrow buffers, with automatic
  cross-validated model selection and a SQL surface of window functions and
  aggregates. No Python sidecar.
- **State-of-the-art compression** — Pcodec (`pco`, 2025) is the primary codec
  for every numeric column, with ALP (SIGMOD 2024), Chimp/Chimp128, Gorilla,
  Patas, delta-of-delta, PFOR, dictionary and bitmap behind it; the winner is
  chosen per block by trial encoding. Most metric values started life as
  decimals — a meter reporting `231.45` W — and pco recovers the integer
  instead of XOR-ing bit patterns, then entropy-codes the deltas. On realistic
  meter and inverter data (noise, plateaus, dropouts) that is worth **5–29×**,
  against 3–8× for ALP and 1.0–2.5× for the XOR codecs; on clean
  low-precision counters it reaches 46–85×. Timestamps use pco too: a jittered
  1-second sampler compresses 2.7× where delta-of-delta manages 0.9×, worse
  than plain. Every number here is pinned by a test — including the *gap*
  between the realistic and the synthetic figures.
- **Wire-compatible, at the routes the clients actually use** — InfluxDB Line
  Protocol, Prometheus remote write/read + full PromQL, OpenTelemetry OTLP
  metrics, Arrow Flight SQL. Point a Grafana **Prometheus** data source at
  `http://chronixd:8086` and nothing else needs configuring; Telegraf's
  `outputs.influxdb` and `outputs.influxdb_v2` write to `/write` and
  `/api/v2/write` gzipped, as they do by default; the OTel Collector's
  `otlphttp` exporter posts to `/v1/metrics`, gzipped, as it does by
  default. Each is driven by a test that sends the client's own bytes: a
  conformance suite that builds its own requests never posts a form body,
  never gzips, and never uses a route derived from a base URL.
- **Security depth** — Cedar policies (a formally verified engine),
  credential-bound namespace isolation, mTLS, Argon2 API keys, JWT/OIDC, and a
  tamper-evident HMAC-chained audit trail. All feature-gated; embedded builds
  compile none of it by default.

  > **Per-column encryption.** `[database.field_encryption]` names a column
  > and an **environment variable** — never a key — so the key is not on the
  > disk it protects. AES-256-GCM per block, bound to its column and segment;
  > compaction re-encrypts; and an encrypted column is refused by the Parquet
  > export, the cold archive and any rollup over it. Fields only, because a
  > tag is part of the series key and is stored beside the segment in
  > plaintext. Whole-directory encryption is the filesystem's job.

  > **A namespace is a tag**, stamped by the one write function every
  > ingestion surface goes through and enforced on every read. Which
  > namespaces a request may touch comes from its **credential**, not its
  > `X-Namespace` header. Segments are not partitioned by tenant on disk, so
  > it bounds requests, not filesystem access — see the
  > [security guide](site/content/docs/security.md).

## Architecture

```text
┌────────────────────────── Chronix ──────────────────────────┐
│                                                             │
│  chronixd            server daemon — REST / gRPC / Flight   │
│    │                 SQL / PromQL / Prometheus / OTLP       │
│    ▼                                                        │
│  chronix             public embedded API — Chronix::open()  │
│    ├── chronix-core        types, schema, config, errors    │
│    ├── chronix-encoding    column codecs (standalone crate) │
│    ├── chronix-engine      WAL · memtable · .csx segments · │
│    │                       indexes · compaction · caches    │
│    ├── chronix-query       pruning, vectorized execution    │
│    ├── chronix-analytics   preprocess · forecast · anomaly ·│
│    │                       multivariate · lifecycle · SIMD  │
│    ├── chronix-streaming   CDC bus · subscriptions ·        │
│    │                       triggers · delivery              │
│    └── chronix-security    authn · Cedar authz · audit ·    │
│                            tenancy   (feature-gated)        │
└─────────────────────────────────────────────────────────────┘
```

Nine crates in the default build:

| Crate | Purpose |
|-------|---------|
| `chronix-core` | Fundamental types, schema registry, configuration, errors |
| `chronix-encoding` | Column codecs: pco, ALP, Chimp, Gorilla, Patas, delta-of-delta, PFOR, dictionary, bitmap, adaptive selection |
| `chronix-engine` | Storage engine: WAL, lock-free memtable, immutable columnar `.csx` segments, time/bloom/tag/zone-map indexes, TWCS compaction, caches; optional S3/GCS/Azure cold tier (`object-store` feature) |
| `chronix-query` | Query planner, layered segment pruning, vectorized Arrow filtering/aggregation, dedup, downsampling |
| `chronix-analytics` | Preprocessing, six forecast models, seven anomaly detectors, multivariate analysis (correlation, VAR, Mahalanobis/Isolation Forest/PCA), model lifecycle (versioning, A/B, drift), streaming analytics, 5-tier SIMD compute |
| `chronix-streaming` | CDC event bus, filtered/resumable subscriptions, trigger engine with webhook delivery |
| `chronix-security` | API keys, JWT/OIDC, mTLS, an AES-256-GCM `EncryptionService` with key rotation, Cedar policies, a durable hash-chained audit trail, namespaces & quotas |
| `chronix` | Public facade — embedded API, SQL (DataFusion), PromQL |
| `chronixd` | Server binary — HTTP, gRPC, Flight SQL, connectors, TLS |

Additional workspace crates outside the default build: the **frozen**
distributed tier (`chronix-meta`, `chronix-cluster`, `chronix-dsim` —
Multi-Raft replication, kept compiling behind `chronixd --features cluster`
but not under active development until the single-node engine ships v1.0).

## Quick Start

```rust,no_run
use chronix::prelude::*;

// Open (or create) a database. `open_small` is the gateway preset
// (≈48 MB of budgets, measured under 25 MiB peak heap for ingest and rollups;
// flash-friendly WAL); `Chronix::open(ChronixConfig::builder()…)`
// is the general form. The handle is cheap to clone and shares the database.
let db = Chronix::open_small("/var/lib/chronix")?;

// A point is a series key (measurement + tags), fields, and a nanosecond timestamp.
let point = Point::new(
    SeriesKey::new("cpu", tags! { "host" => "server-01", "region" => "us-east" })?,
    fields! { "usage_idle" => 95.5, "usage_system" => 2.3 },
    1_700_000_000_000_000_000,
)?;
db.insert(&point)?;

// SQL — every measurement is a table and `_time` is the time column, and it
// compares against the same epoch nanoseconds `insert` took.
for batch in db.sql(
    "SELECT time_bucket('5m', _time) AS t, host, avg(usage_idle) FROM cpu
     WHERE _time >= 1700000000000000000 GROUP BY t, host",
)? {
    println!("{batch:?}"); // Arrow RecordBatch
}

// PromQL, exactly as chronixd serves it to Grafana.
let now = 1_700_000_000_000_000_000;
let series = db.promql(r#"rate(cpu_usage_system{host="server-01"}[5m])"#, now)?;

// Or the typed builder, with bounded-memory streaming for large windows.
let plan = db.query().measurement("cpu").tag("host", "server-01").field("usage_idle").build()?;
for batch in db.execute_iter(&plan)? {
    let batch = batch?; // one time bucket at a time, never the whole result
}

// Forecast the next 24 hours — no extra services required.
// `auto_forecast` picks the model by cross-validation and reports why.
let chosen = db.auto_forecast(
    "cpu", "usage_idle", &[("host", "server-01")],
    1_699_000_000_000_000_000, 1_701_000_000_000_000_000,
    24 * 60, None,
)?;
println!("{} won on {}", chosen.selection.label, chosen.selection.metric);

db.close()?;
```

## Feature Highlights

Each claim below is pinned by a test; the depth is in the
[internals](https://hupe1980.github.io/chronix/internals/) and
[reference](https://hupe1980.github.io/chronix/reference/) sections.

**Storage engine** — [details](https://hupe1980.github.io/chronix/internals/storage-engine/)

- **Crash-safe WAL** with CRC32c records, group commit and a configurable
  fsync policy (`Periodic` is the flash-friendly one for eMMC/SD). The catalog
  records a **WAL floor**, so a clean `close()` restarts without replaying and
  a crash replays only what was never flushed.
- **Admission before durability.** The out-of-order window, the cardinality
  budget and the schema are decided *before* the WAL append, so a rejected
  write is free and cannot return through replay. `backfill` is the explicit
  operation for history outside the window — `db.backfill(&points)` embedded,
  `?backfill=true` on every `chronixd` write path, `WriteRequest.backfill` on
  gRPC — and a refused write names the timestamp, the window and the remedy.
- **Bounded, measured memory.** A lock-free skip-list memtable flushes
  straight into Arrow columns. `examples/gateway_footprint.rs` prints **under
  25 MiB peak heap** for an hour of the design partner's workload — rollups
  and dashboards included — settling at 1.6 MiB, and
  `DatabaseStatistics::resident_memory_bytes` breaks that down term by term:
  memtables, interners, the WAL buffer and the catalog.
- **Immutable `.csx` segments**: row groups, per-column stats, validity
  bitmaps, LZ4/Zstd with dictionary training, mmap reads, atomic
  temp→fsync→rename. Footer size is proportional to the data, not a fixed
  sketch.
- **Compaction never blocks writes** — hybrid TWCS + size-tiered, streaming
  K-way merge in `O(chunk + K)` memory, with write-amplification budgeting and
  backpressure.
- **Layered pruning**: catalog time range → inverted tag index → series blooms →
  column stats → zone maps. Each level can only keep a segment it could have
  dropped, never the reverse. Every segment carries a `.series` sidecar, so
  blooms, the tag index and the exact cardinality are rebuilt at open without
  decoding a segment.
- **Deletes are ranged tombstones persisted in the catalog manifest.** They
  survive a flush, a restart and a compaction, and are reclaimed only once
  every segment they were issued against has been rewritten — so writing to a
  series after deleting it re-creates it rather than being swallowed.
- **Rollups are materialised and repaired, never approximated.** A cascade
  (1 s → 1 min → 15 min) with a persisted watermark per tier. A backfill, a
  delete or an import into an already-aggregated range records an
  **invalidation** that the next pass recomputes, and retention drops raw data
  only once every tier it feeds has caught up.
- **Retention measures age from `min(wall clock, newest timestamp held)`**, so
  a wrong clock cannot empty the database and a gateway whose sensors go quiet
  keeps its history; one fresh write resumes normal expiry. What it deletes it
  also **releases** — the cardinality budget and the last-value cache are
  re-derived from what is left, so device churn does not end in permanent
  write refusal.
- **A tier can be a calendar tier.** `every("1d").timezone("Europe/Berlin")`
  runs local midnight to local midnight — 23 or 25 hours on a daylight-saving
  day — and `every("1mo")` is a calendar month. The **unit decides**: sub-day
  widths are a fixed span, super-day widths follow the calendar. One
  `TimeBucket` answers this for all three surfaces that bucket time — a rollup
  tier, `time_bucket()` in SQL, and the native `downsample()` — so they cannot
  disagree about where a day begins. An `origin` moves the boundary, for a
  billing month that starts on the 15th or a shift day that starts at 06:00.
- **Parquet cold archive** reads each cold `(measurement, shard)` group
  *through the read path* — deduplicated, tombstones applied — writes one
  Hive-partitioned object to S3/GCS/Azure, verifies it, and only then drops
  the source. DuckDB, Polars and Spark read it directly;
  `register_cold_tier()` brings it back as a SQL table.

**Query** — [details](https://hupe1980.github.io/chronix/internals/query-engine/)

- **One read path.** `execute_iter()` streams a scan in bounded memory —
  time-disjoint buckets merged one at a time — and everything else folds over
  it: aggregates into accumulators sized by group count, downsampling with one
  open bucket, `LIMIT` stopping the scan. No two entry points can disagree.
- **SQL** through DataFusion, one call away: `db.sql("…")` synchronously,
  `db.sql_async` from async, `db.session_context()` for DataFusion's own API.
  Predicate pushdown, spill-to-disk, read-only enforcement at plan level, and
  **23 analytics functions** — `time_bucket`, fifteen window functions
  (`rolling_*`, `stl_*`, `anomaly_score`, …) and seven aggregates (`rate`,
  `irate`, `forecast`, `auto_forecast`, …).
- **PromQL tracking Prometheus 3.x** — 50+ functions, all matcher operators,
  vector matching, subqueries, `@` and negative offsets, embedded via
  `db.promql(…)` or served at the paths a Prometheus client derives. A range
  query reads its window **once**, not once per step. A metric is one
  `(measurement, field)` pair, so a name a query returns is a selector that
  returns it. `limit` is honoured on every endpoint that takes it upstream,
  and an answer a limit cut short carries
  `warnings: ["results truncated due to limit"]` rather than looking
  complete. Pinned by an end-to-end conformance suite.

**Analytics** — [guide](https://hupe1980.github.io/chronix/docs/analytics/)

- **Forecasting**: SES, Holt (damped), Holt-Winters, ARIMA, SARIMA, linear
  regression, with auto-ARIMA by AIC, `O(1)` online updates and persistence.
  `auto_forecast` picks the model by cross-validation and reports why.
- **Quantile forecasting** builds prediction intervals from the empirical
  distribution of walk-forward residuals, bucketed per horizon step, so
  interval shape and growth are learned rather than assumed Gaussian —
  with optional split-conformal correction and explicit physical bounds.
- **Anomaly detection**: Z-score, modified Z-score (MAD), IQR,
  forecast-residual (strict walk-forward), moving-average residual, seasonal
  dynamic threshold, CUSUM; multivariate Mahalanobis, Isolation Forest and PCA
  with per-series contributions.
- **Model lifecycle**: versioned registry, champion/challenger A/B testing,
  drift detection (PSI, KS, ADWIN), retroactive error tracking.
- **Preprocessing and compute**: gap detection, six interpolators, clock-drift
  correction, STL decomposition; runtime-dispatched SIMD (AVX-512F → AVX2+FMA
  → SSE2 / NEON) with Kahan summation and rayon batch parallelism.

**Streaming and signals** — [details](https://hupe1980.github.io/chronix/reference/analytics-and-streaming/)

- A bounded CDC event bus with filtered, resumable and persistent
  subscriptions, feeding per-series anomaly scoring in under 10 ms from
  ingest to score.
- A **trigger engine** — anomaly score, forecast deviation, threshold,
  MA crossover, rate of change, composite — managed with
  `CREATE/SHOW/DROP/ALTER TRIGGER`, embedded or over HTTP, scoped per tenant.
  Webhook delivery is a [CloudEvents](https://cloudevents.io) envelope,
  [Standard Webhooks](https://www.standardwebhooks.com)-signed with SSRF
  protection at parse *and* connect time. Each channel has its own worker and
  bounded queue, so a webhook that is retrying delays nothing but itself.

**Server (`chronixd`)** — [API reference](https://hupe1980.github.io/chronix/docs/api-reference/)

| Protocol | Default port | Description |
|----------|-------------|---------------------------------|
| HTTP/REST | 8086 | JSON write/query, InfluxDB Line Protocol (full escape semantics, `=` allowed in tags), management, `/metrics` |
| gRPC | 8087 | Write/query/schema, client-streaming ingestion with dedup keys |
| Flight SQL | 8817 | Arrow columnar transfer, catalog browsing (DBeaver/JDBC) |
| Prometheus | 8086 | Remote write/read, `/api/v1/query[_range]`, series/labels, `status/buildinfo` — at the paths a client derives from a base URL |
| OTLP | 8086 | OpenTelemetry metrics ingestion |

Non-finite samples — Prometheus staleness markers, OTLP quantiles with nothing
observed yet — are skipped and counted rather than failing the batch, because
both are routine traffic and a `400` stalls a remote-write queue indefinitely.

Every error carries a machine-readable `code`, and conditions that describe the
deployment are shown rather than redacted: `503 BACKPRESSURE` with a
`Retry-After` for a full memtable, `503 OVERLOADED` for a WAL poisoned by a
failed `fsync`, `507 STORAGE_FULL`, `504 QUERY_TIMEOUT` naming the setting that
bound it. Only chronix's own machinery is a redacted `500`. Read deadlines are
enforced inside the scan, so a query that runs out of time stops.

Plus Kafka/MQTT ingestion connectors (feature-gated, hot-reload, credential
rotation, SASL + TLS), TLS with certificate hot-reload, graceful shutdown that
waits for every protocol surface before closing the database, per-request
timeouts and row limits, SSE annotations, and bundled Grafana dashboards in
[`dashboards/`](dashboards/).

**SQL is a default-on feature.** `chronix` with `default-features = false`
drops DataFusion, which is 96 seconds of every clean build (131 s against
227 s for a small consumer). Writes, the native query API, PromQL, rollups,
retention, analytics, triggers and the cold archive are unaffected; only
`db.sql()`, `EXPLAIN` and the cold tier's read half go. It is not a way to
shrink the binary — the linker already discards DataFusion when nothing calls
it, so that difference is 0.34 MiB.

**No C toolchain for the connectors.** `krafka` and `rumqttc` are pure Rust.
One dependency does compile C — `aws-lc-rs`, the single rustls crypto provider
the whole workspace shares rather than inheriting each dependency's default —
and it ships a pregenerated build configuration for `linux_aarch64` among
others, so it drives `cc` directly instead of invoking CMake. A C compiler for
the target is the whole requirement: no cmake, no Fortran, no system
libraries.


## Building

```bash
# Build & test the product (default members)
cargo build
cargo test

# Lint / format
cargo clippy --all-targets
cargo fmt --all -- --check

# Everything including the frozen cluster tier
cargo test --workspace

# Benchmarks
cargo bench -p chronix            # end-to-end insert/flush/query/compaction
cargo bench -p chronix-encoding   # codecs & compression ratios
cargo bench -p chronix-engine     # WAL, segment, memtable
cargo bench -p chronix-analytics  # forecast, anomaly, SIMD, multivariate
cargo bench -p chronixd           # server throughput, TSBS DevOps

# Fuzzing (requires cargo-fuzz)
cargo fuzz list

# Miri (requires nightly)
cargo +nightly miri test -p chronix-core -- --skip proptests --skip config_toml_roundtrip
```

### Feature flags

| Crate | Flag | Default | Purpose |
|-------|------|---------|---------|
| `chronix-engine`, `chronix` | `field-encryption` | on | Per-column AES-256-GCM in the `.csx` format, declared by `[database.field_encryption]` |
| `chronix-engine` | `object-store` | off | S3/GCS/Azure cold tier, Parquet archive writer |
| `chronix` | `object-store` | off | `register_cold_tier()` — SQL over the Parquet archive |
| `chronix-streaming` | `flight` | off | Arrow Flight CDC export |
| `chronix-security` | `webhook` | on | Audit webhook sink |
| `chronixd` | `kafka`, `mqtt` | off | Ingestion connectors (`krafka` / `rumqttc`, both pure Rust) |
| `chronixd` | `object-store` | off | Periodic cold archiving to S3/GCS/Azure (`[cold_archive]`) |
| `chronixd` | `cluster` | off | Frozen distributed tier (Meta/Data modes) |

## Server Mode

```bash
cargo build --release -p chronixd

# Start with defaults (HTTP :8086, gRPC :8087, Flight SQL :8817)
./target/release/chronixd --data-dir /var/lib/chronix

# Or with a TOML config file
./target/release/chronixd --config /etc/chronixd/config.toml
```

```bash
# Write a point
curl -X POST http://localhost:8086/api/v1/write \
  -H 'Content-Type: application/json' \
  -d '{"measurement":"cpu","tags":{"host":"srv1"},"fields":{"usage":72.5}}'

# InfluxDB Line Protocol (Telegraf-compatible)
curl -X POST http://localhost:8086/write \
  -d 'cpu,host=srv1 usage=72.5 1609459200000000000'

# SQL
curl -X POST http://localhost:8086/api/v1/chronix/sql \
  -d '{"query":"SELECT time_bucket('\''5m'\'', _time) AS t, avg(usage) FROM cpu GROUP BY t"}'

# PromQL, at the path a Prometheus client derives
curl 'http://localhost:8086/api/v1/query?query=rate(cpu_usage[5m])'

# Health check
curl http://localhost:8086/health
```

## Examples

Runnable examples live in [`crates/chronix/examples/`](crates/chronix/examples/) — one per major feature,
each against an in-process embedded database:

`quickstart`, `basic_usage`, `query_and_aggregation`, `sql_queries`,
`promql_queries`, `forecast`, `quantile_forecast`, `anomaly_detection`,
`alerting`, `multivariate_analysis`, `preprocessing`, `model_lifecycle`,
`continuous_forecast`, `encoding`, `schema_exploration`,
`storage_lifecycle`, `backup_and_restore`, `delete_operations`, `exact_decimals`,
`gateway_footprint`,
`signal_triggers`, `data_pipeline`, `cold_tier`, `authz`, `encryption`,
`audit_logging`, `tenant_isolation`, `compute_engine`, `field_encryption`.

```bash
cargo run -p chronix --example basic_usage
```

## Documentation

Full documentation: **<https://hupe1980.github.io/chronix>** — built from
[`site/`](site/) with [Zola](https://www.getzola.org/).

| Section | What is in it |
|---------|---------------|
| [Guide](https://hupe1980.github.io/chronix/docs/) | Getting started, API reference, analytics, operations, performance, security, client SDKs, Grafana |
| [Internals](https://hupe1980.github.io/chronix/internals/) | How it works — storage engine, encoding, forecasting and anomaly algorithms, query execution |
| [Reference](https://hupe1980.github.io/chronix/reference/) | The implementation, subsystem by subsystem |

API documentation for the published crates is on
[docs.rs/chronix](https://docs.rs/chronix).

## Project Status

**Pre-release.** The on-disk format and the public API are not yet stable;
the first tagged release will declare both. There are no production
deployments.

The engine is hardened against the failures that matter: a property test on
every codec, three fuzz targets run nightly, and crash-recovery tests that
really crash — a child process that `abort()`s mid-flush, mid-compaction and
mid-materialisation, with the parent checking that nothing acknowledged was
lost.

The tree tracks the current ecosystem: **Arrow 59, DataFusion 55, parquet 59,
arrow-flight 59, tonic 0.14**. `cargo clippy --all-targets` is clean on the
default build at the configured lint level; the frozen cluster crates
(`chronix-meta`, `chronix-cluster`, `chronix-dsim`, outside
`default-members`) are compile-checked in CI but not lint-clean. The release
procedure and version policy are in [CONTRIBUTING.md](CONTRIBUTING.md), and
notable changes are in [CHANGELOG.md](CHANGELOG.md).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
