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
  rollups and enforces retention, so there is nothing to start and no
  daemon to run. Runs where a database *server* cannot.
- **Analytics inside the engine** — statistical forecasting (SES, Holt,
  Holt-Winters, ARIMA/SARIMA), anomaly detection (Z-score, MAD, IQR,
  forecast-residual, CUSUM, seasonal thresholds), drift detection, and
  programmable triggers run against zero-copy Arrow buffers, with automatic
  cross-validated model selection and a SQL surface of window functions and
  aggregates. No Python sidecar.
- **State-of-the-art compression** — Pcodec (`pco`, 2025) as the primary
  codec for every numeric column, with ALP (SIGMOD 2024), Chimp/Chimp128,
  Gorilla and Patas behind it, plus delta-of-delta, PFOR, dictionary and
  bitmap codecs under adaptive per-block selection — the winner is chosen
  per block by trial encoding, and recorded, so a workload the leaders are
  bad at costs one sample encode rather than a bad ratio. Most metric values
  started life as decimals — a meter reporting `231.45` W — and both pco
  and ALP recover the integer instead of XOR-ing bit patterns; pco then
  entropy-codes the deltas. On realistic meter and inverter data — sensor
  noise, plateaus, dropouts — that is worth **5–29×**, against 3–9× for ALP
  and 1.0–2.5× for the XOR codecs; on clean low-precision counters it
  reaches 46–85×. Timestamps use pco too: a jittered 1-second sampler
  compresses 2.7× where delta-of-delta managed 0.9×, i.e. worse than plain.
  Every number is pinned by a test, and one of those tests pins the *gap*
  between the realistic and synthetic figures so the easy number cannot
  quietly become the headline again.
- **Wire-compatible, at the routes the clients actually use** — InfluxDB Line
  Protocol, Prometheus remote write/read + full PromQL, OpenTelemetry OTLP
  metrics, Arrow Flight SQL. Point a Grafana **Prometheus** data source at
  `http://chronixd:8086` and nothing else needs configuring; Telegraf's
  `outputs.influxdb` and `outputs.influxdb_v2` write to `/write` and
  `/api/v2/write` gzipped, as they do by default; the OTel Collector's
  `otlphttp` exporter posts to `/v1/metrics`, gzipped, as it does by
  default. Each of those is driven by a test that sends the client's own
  bytes — a conformance suite that builds its own requests never posts a
  form body, never gzips, and never uses a route derived from a base URL,
  which is how all four integrations were broken under a green suite.
- **Security depth** — Cedar policies (a formally verified engine),
  credential-bound namespace isolation, AES-256-GCM encryption at rest, mTLS, Argon2 API keys,
  JWT/OIDC, and a tamper-evident HMAC-chained audit trail. All feature-gated;
  embedded builds compile none of it by default.

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
| `chronix-security` | API keys, JWT/OIDC, mTLS, AES-256-GCM at-rest encryption, Cedar policies, a durable hash-chained audit trail, namespaces & quotas |
| `chronix` | Public facade — embedded API, SQL (DataFusion), PromQL |
| `chronixd` | Server binary — HTTP, gRPC, Flight SQL, connectors, TLS |

Additional workspace crates outside the default build: `chronix-chaos`
(dev-only fault injection) and the **frozen** distributed tier
(`chronix-meta`, `chronix-cluster`, `chronix-dsim` — Multi-Raft replication,
kept compiling behind `chronixd --features cluster` but not under active
development until the single-node engine ships v1.0).

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

// SQL — every measurement is a table, `_time` is the timestamp, and it
// compares against the same epoch nanoseconds `insert` took.
for batch in db.sql(
    "SELECT time_bucket('5m', _time) AS t, host, avg(usage_idle) FROM cpu
     WHERE _time >= 1700000000000000000 GROUP BY t, host",
)? {
    println!("{batch:?}"); // Arrow RecordBatch
}

// PromQL, exactly as chronixd serves it to Grafana.
let now = 1_700_000_000_000_000_000;
let series = db.promql(r#"rate(usage_system{host="server-01"}[5m])"#, now)?;

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

**Storage engine**

- Crash-safe WAL: CRC32c records, group commit, LZ4, configurable fsync
  (`PerWrite` / `PerBatch` / `Periodic` — the last is flash-friendly for
  eMMC/SD gateways), rotation + truncation, corrupted-tail tolerance. The
  catalog records the **WAL floor**, so a clean `close()` is a replay-free
  restart and a crash replays only what was never flushed
- Admission before durability: the out-of-order window, the cardinality
  budget and the schema are decided per point *before* the WAL append, so a
  rejected write is free and can never come back through replay.
  `insert_batch` reports `InsertResult { accepted, rejected }`; `backfill`
  is the explicit operation for writing history outside the window
- Lock-free skip-list memtable with freeze-and-swap flush and time-shard
  routing; out-of-order and late-arrival handling with last-write-wins
  dedup. Its memory accounting is calibrated against a counting allocator
  in the test suite, and a flush builds Arrow columns straight off the skip
  list — the small preset's budget is a measured number:
  `examples/gateway_footprint.rs` prints under 25 MiB peak heap for an hour of the
  design partner's workload, rollups and dashboards included, and 1.6 MiB
  live once it settles. `DatabaseStatistics::resident_memory_bytes` breaks
  that down into memtables, interners, WAL buffer and catalog
- Immutable columnar `.csx` segments: row groups, per-column stats and
  encodings, per-block validity bitmaps, LZ4/Zstd (+ dictionary training),
  mmap reads with `MADV` hints, atomic temp→fsync→rename writes. Segment
  metadata is proportional to the data — a segment holding a few KB of
  columns costs a few hundred bytes of footer, not a fixed sketch.
- Hybrid TWCS + size-tiered compaction: streaming K-way merge
  (O(N log K) time, O(chunk+K) memory), tombstone cleanup,
  write-amplification budgeting, backpressure — never blocks writes
- Layered pruning: time index → series blooms → inverted tag index →
  column stats → row-group zone maps / per-row-group tag blooms. Each level
  can only ever keep a segment it could have dropped, never the reverse — the
  series bloom holds complete series keys, so it is consulted only when the
  query's tag filters cover every tag, and skipped otherwise
- Every segment carries a `.series` sidecar — its distinct series keys — from
  which the blooms, the tag index and the exact cardinality count are rebuilt
  at open without decoding a single segment
- Caches: last-value cache (opt-in per measurement; ~400 ns on a hit),
  TinyLFU segment cache, metadata cache
- Deletes are **ranged tombstones persisted in the catalog manifest**: a
  delete names an interval (resolved to the series' newest stored timestamp
  when you do not name one), survives a flush and a restart, and is reclaimed
  only once compaction has rewritten every segment it was issued against — so
  writing to a series after deleting it re-creates it, rather than being
  swallowed
- **Rollups are materialised and repaired, never approximated**
  (`first`/`last`/`min`/`max`/`avg`/`sum`/`count`, grouped by tags): each
  bucket is aggregated over every row that reaches it as soon as the
  out-of-order window has closed over it, with a persisted watermark per
  rollup and a cascade (1 s → 1 min → 15 min) that is consistent by
  construction. Because "the window has closed" is not a proof of finality,
  a backfill, a delete or an import into an already-aggregated range records
  an **invalidation** that the next pass recomputes — so an offline device's
  backlog and a deletion both reach the derived tiers, instead of leaving
  them silently stale for ever. `refresh_rollup()` does the same on demand.
  Retention with per-measurement overrides drops raw data only once every
  tier it feeds is materialised past it *and* has no repair pending.
  Streaming Parquet export (explicit dictionary encoding for tags, a
  `max_bytes` budget that reports truncation rather than filling the device)
- **Parquet cold archive**: `archive_cold_segments()` reads each cold
  `(measurement, shard)` group **through the read path** — deduplicated, with
  tombstones applied — writes it as one Hive-partitioned Parquet object
  (`measurement=…/shard=…/…parquet`) to S3/GCS/Azure, verifies it, and only
  then drops the source segments. The object carries the hot tier's schema,
  `_time` included, so a query moved to the archive is the same query. DuckDB,
  Polars and Spark read it directly and prune on the partition columns;
  `register_cold_tier()` brings it back as a SQL table here, one table per
  measurement. `chronixd` runs a pass periodically behind `[cold_archive]`.
  The hot tier stays `.csx`: specialised core, standard edges

**Query**

- Fluent `QueryBuilder` → vectorized Arrow execution, streaming 64 Ki-row
  batches, cardinality-aware grouping, projection pushdown to disk reads
- **One read path.** `execute_iter()` streams a scan in bounded memory:
  surviving segments are swept into time-disjoint buckets and merged one
  bucket at a time, so exporting or scanning a window far larger than RAM
  costs the busiest bucket rather than the whole result set. Everything else
  folds over it — aggregates into accumulators (memory proportional to the
  group count, not the row count), downsampling with one open bucket across
  batches, `LIMIT` stopping the scan — and `execute()` is a convenience over
  `execute_stream()`, so no two entry points can disagree
- **SQL** via DataFusion, one call away — `db.sql("…")` from synchronous
  code, `db.sql_async` from async, `db.session_context()` for DataFusion's
  own API: predicate pushdown, cost statistics for the
  optimizer, spill-to-disk, read-only enforcement at plan level, and
  23 analytics functions — one scalar (`time_bucket`), fifteen **window**
  functions (`diff`, `zscore`, `rolling_*`, `stl_*`, `anomaly_score`, …) that
  take a `PARTITION BY … ORDER BY`, and seven **aggregates** (`first`,
  `last`, `rate`, `irate`, `forecast`, `auto_forecast`,
  `multivariate_forecast`)
- **PromQL** — `db.promql("…", at)` / `db.promql_range(…)` embedded, the
  `/api/v1/query[_range]` endpoints served: full parser/evaluator tracking **Prometheus 3.x** — 50+
  functions including the eight UTC date functions, all matcher operators, vector matching
  (`on`/`ignoring`/`group_left`/`group_right`), subqueries, the `@` modifier
  (`@ start()`/`@ end()`), negative offsets, instant + range queries.
  Left-open range and lookback windows; every range-vector function
  stamps the evaluation timestamp, so a range query returns exactly one point
  per step; `rate`/`increase`/`delta` implement `extrapolatedRate` including
  counter-reset correction and zero-clamping; `histogram_quantile` follows
  `bucketQuantile`; `double_exponential_smoothing`, `mad_over_time` and
  `sort_by_label`/`sort_by_label_desc` follow the 3.x definitions, natural
  label ordering included. A subquery evaluates on an absolute step grid,
  stamps step timestamps, and is a range vector — `rate(x[5m:15s])`
  extrapolates as `rate(x[5m])` does. A range query reads its window **once**,
  not once per step, subqueries included, and
  `chronix_promql_scan_cache_hits_total` says so at runtime. All
  of it is covered by an end-to-end conformance suite that drives the real
  query path

**Analytics**

- Preprocessing: gap detection, six interpolators, smoothing, resampling,
  clock-drift correction, STL decomposition (Cleveland et al., with the
  low-pass step that keeps the seasonal component from vanishing), auto
  feature generation
- Forecast: SES, Holt (damped), Holt-Winters (additive/multiplicative),
  ARIMA, SARIMA, linear regression — auto-ARIMA via AIC, O(1) online
  updates, model persistence
- Quantile forecasting: prediction intervals from the empirical
  distribution of walk-forward residuals, bucketed per horizon step, so
  interval shape and growth are learned from the data instead of assumed
  Gaussian. Optional split-conformal correction — applied outward on both
  tails — for finite-sample coverage, plus explicit physical bounds that
  hold for the point forecast as well as the quantiles
- Anomaly: Z-score, modified Z-score (MAD), IQR, forecast-residual
  (strict walk-forward), moving-average residual, seasonal dynamic
  threshold, CUSUM; multivariate: Mahalanobis, Isolation Forest, PCA with
  per-series contributions
- Lifecycle: versioned model registry, champion/challenger A/B testing,
  drift detection (PSI, KS, ADWIN), retroactive MAPE/RMSE/MAE tracking,
  anomaly-precision feedback loop
- Streaming: CDC-fed per-series anomaly scoring (< 10 ms ingest-to-score),
  continuous forecasts, materialized forecast views, alert engine
- Custom models: pure-Rust registry for user `ForecastModel` /
  `AnomalyDetector` implementations
- Compute: runtime-dispatched SIMD (AVX-512F → AVX2+FMA → SSE2 / NEON)
  with Kahan summation, rayon batch parallelism, buffer pools

**Streaming & signals**

- CDC event bus (bounded broadcast, lazy event construction), filtered and
  resumable subscriptions, persistent subscriptions with replay
- Trigger engine: anomaly-score, forecast-deviation, threshold, MA-crossover
  (golden/death cross), rate-of-change, composite conditions; SQL management
  (`CREATE/SHOW/DROP/ALTER TRIGGER`) embedded or over HTTP behind
  `[triggers]`, namespace-scoped per tenant; webhook delivery with HMAC-SHA256
  signing, SSRF protection at parse *and* connect time, retry + dead-letter
  queue

**Server (`chronixd`)**

| Protocol | Default port | Description |
|----------|-------------|---------------------------------|
| HTTP/REST | 8086 | JSON write/query, InfluxDB Line Protocol (full escape semantics, `=` allowed in tags), management, `/metrics` |
| gRPC | 8087 | Write/query/schema, client-streaming ingestion with dedup keys |
| Flight SQL | 8817 | Arrow columnar transfer, catalog browsing (DBeaver/JDBC) |
| Prometheus | 8086 | Remote write/read, `/api/v1/query[_range]`, series/labels, `status/buildinfo` — at the paths a client derives from a base URL |
| OTLP | 8086 | OpenTelemetry metrics ingestion |

Non-finite samples — Prometheus staleness markers, OTLP quantiles with
nothing observed yet — are skipped and counted rather than failing the batch
they arrived in, because both are routine traffic and a `400` stalls a
remote-write queue indefinitely.

Plus: Kafka/MQTT ingestion connectors (feature-gated, hot-reload, credential
rotation, SASL + TLS), TLS with certificate hot-reload, graceful shutdown,
per-request timeouts and row/batch limits, SSE annotations and bundled
Grafana dashboards in [`dashboards/`](dashboards/).

Both connectors are **pure Rust** — `krafka` and `rumqttc` — so turning them
on adds no C build step. One dependency does compile C: `aws-lc-rs`, the
single rustls crypto provider the whole workspace shares rather than
inheriting each dependency's default. Nothing is shipped precompiled — crates
carry source — but `aws-lc-sys` ships a *pregenerated build configuration* for
`linux_aarch64` among others, so it drives `cc` directly instead of invoking
CMake. A C compiler for the target architecture is therefore the whole
requirement: no cmake, no Fortran, no system libraries. That is why CI builds
the embedded target on a native arm64 runner rather than cross-compiling —
`cargo build` there needs nothing a stock runner lacks.

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
| `chronix-engine` | `field-encryption` | on | AES-256-GCM segment/WAL encryption support |
| `chronix-engine` | `object-store` | off | S3/GCS/Azure cold tier, Parquet archive writer |
| `chronix` | `object-store` | off | `register_cold_tier()` — SQL over the Parquet archive |
| `chronix-streaming` | `flight` | off | Arrow Flight CDC export |
| `chronix-security` | `webhook` | on | Audit webhook sink |
| `chronixd` | `kafka`, `mqtt` | off | Ingestion connectors (`krafka` / `rumqttc`, both pure Rust) |
| `chronixd` | `object-store` | off | Periodic cold archiving to S3/GCS/Azure (`[cold_archive]`) |
| `chronixd` | `cluster` | off | Frozen distributed tier (Meta/Data modes) |
| `chronixd` | `chaos` | off | Fault-injection admin endpoints (dev only) |

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
curl 'http://localhost:8086/api/v1/query?query=rate(cpu[5m])'

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
`storage_lifecycle`, `delete_operations`, `gateway_footprint`,
`signal_triggers`, `data_pipeline`, `cold_tier`, `authz`, `encryption`,
`audit_logging`, `tenant_isolation`, `compute_engine`, `chaos_testing`.

```bash
cargo run -p chronix --example basic_usage
```

## Documentation

Full documentation: **<https://hupe1980.github.io/chronix>** — built from
[`site/`](site/) with [Zola](https://www.getzola.org/).

| Section | What is in it |
|---------|---------------|
| [Guide](site/content/docs/) | Getting started, API reference, analytics, operations, performance, security, client SDKs, Grafana |
| [Internals](site/content/internals/) | How it works — storage engine, encoding, forecasting and anomaly algorithms, query execution |
| [Reference](site/content/reference/) | The implementation, subsystem by subsystem |

API documentation for the published crates is on
[docs.rs/chronix](https://docs.rs/chronix).

## Project Status

Pre-release, under active development. The engine is extensively hardened —
a green default-build suite (`cargo test` prints the count), property tests
on every codec, three fuzz targets run nightly, crash-recovery integration
tests that really crash (a child process that `abort()`s), and 37 deep
audit passes — but the on-disk format and public API are **not yet
stable**. The first tagged release will declare both.

The tree tracks the current ecosystem: **Arrow 59, DataFusion 55, parquet 59,
arrow-flight 59, tonic 0.14**. The release procedure and version policy are in
[CONTRIBUTING.md](CONTRIBUTING.md); what remains before a first release is a
Grafana walkthrough and TSBS results.

`cargo clippy --all-targets` is clean on the default build at the configured
lint level. The frozen cluster crates (`chronix-meta`, `chronix-cluster`,
`chronix-dsim`, excluded from `default-members`) are compile-checked in CI
but not lint-clean.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
