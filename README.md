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
  directory, crash-safe WAL, graceful `close()`, zero background daemons
  required. Runs where a database *server* cannot.
- **Analytics inside the engine** — statistical forecasting (SES, Holt,
  Holt-Winters, ARIMA/SARIMA), anomaly detection (Z-score, MAD, IQR,
  forecast-residual, CUSUM, seasonal thresholds), drift detection, and
  programmable triggers run against zero-copy Arrow buffers, with automatic
  cross-validated model selection and a SQL surface of window functions and
  aggregates. No Python sidecar.
- **State-of-the-art compression** — ALP (SIGMOD 2024) as the primary float
  codec, with Chimp/Chimp128, Gorilla and Patas behind it, plus
  delta-of-delta, PFOR, dictionary and bitmap codecs under adaptive
  per-block selection. Most metric values started life as decimals — a meter
  reporting `231.45` W — and ALP recovers the integer instead of XOR-ing bit
  patterns, which is worth 4–9× on that data where the XOR codecs manage
  1.0–1.2× — and it decodes 3.3× faster than Chimp while doing it.
- **Wire-compatible** — InfluxDB Line Protocol, Prometheus remote write/read
  + full PromQL, OpenTelemetry OTLP metrics, Arrow Flight SQL. Drop-in
  behind Telegraf, Prometheus, Grafana, and the OTel Collector.
- **Security depth** — Cedar RBAC/ABAC (formally verified policy engine),
  namespace isolation applied at every ingestion and read surface, AES-256-GCM
  encryption at rest, mTLS, Argon2 API keys, JWT/OIDC, and a tamper-evident
  HMAC-chained audit trail. All feature-gated; embedded builds compile none of
  it by default.

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
│    │                       continuous aggs · triggers       │
│    └── chronix-security    authn · Cedar authz · audit ·    │
│                            tenancy   (feature-gated)        │
└─────────────────────────────────────────────────────────────┘
```

Nine crates in the default build:

| Crate | Purpose |
|-------|---------|
| `chronix-core` | Fundamental types, schema registry, configuration, errors |
| `chronix-encoding` | Column codecs: ALP, Chimp, Gorilla, Patas, delta-of-delta, PFOR, dictionary, bitmap, adaptive selection |
| `chronix-engine` | Storage engine: WAL, lock-free memtable, immutable columnar `.csx` segments, time/bloom/tag/zone-map indexes, TWCS compaction, caches; optional S3/GCS/Azure cold tier (`object-store` feature) |
| `chronix-query` | Query planner, layered segment pruning, vectorized Arrow filtering/aggregation, dedup, downsampling |
| `chronix-analytics` | Preprocessing, six forecast models, seven anomaly detectors, multivariate analysis (correlation, VAR, Mahalanobis/Isolation Forest/PCA), model lifecycle (versioning, A/B, drift), streaming analytics, 5-tier SIMD compute |
| `chronix-streaming` | CDC event bus, filtered/resumable subscriptions, continuous aggregations, trigger engine with webhook delivery |
| `chronix-security` | API keys, JWT/OIDC, mTLS, AES-256-GCM at-rest encryption, Cedar RBAC/ABAC, audit trail, namespaces & quotas |
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
use std::path::Path;

// Open (or create) a database
let config = ChronixConfig::builder()
    .data_dir(Path::new("/tmp/my-tsdb"))
    .build()
    .unwrap();
let db = Chronix::open(config).unwrap();

// …or, on a memory-constrained gateway (≈48 MB budget, flash-friendly WAL):
// let db = Chronix::open_small("/var/lib/chronix").unwrap();

// Insert points using the tags!/fields! macros
let point = Point::new(
    SeriesKey::new("cpu", tags! {
        "host" => "server-01",
        "region" => "us-east",
    }).unwrap(),
    fields! {
        "usage_idle" => 95.5,
        "usage_system" => 2.3,
    },
    1_700_000_000_000_000_000, // nanoseconds
).unwrap();

db.insert(&point).unwrap();

// Query via the fluent builder
let plan = db
    .query()
    .measurement("cpu")
    .tag("host", "server-01")
    .range(1_699_000_000_000_000_000, 1_701_000_000_000_000_000)
    .field("usage_idle")
    .build()
    .unwrap();

let batch = db.execute(&plan).unwrap(); // Arrow RecordBatch

// Forecast the next 24 hours — no extra services required.
// `auto_forecast` picks the model by cross-validation and reports why.
let chosen = db.auto_forecast(
    "cpu", "usage_idle", &[("host", "server-01")],
    1_699_000_000_000_000_000, 1_701_000_000_000_000_000,
    24 * 60, None,
).unwrap();
println!("{} won on {}", chosen.selection.label, chosen.selection.metric);

db.close().unwrap();
```

## Feature Highlights

**Storage engine**

- Crash-safe WAL: CRC32c records, group commit, LZ4, configurable fsync
  (`PerWrite` / `PerBatch` / `Periodic` — the last is flash-friendly for
  eMMC/SD gateways), rotation + truncation, corrupted-tail tolerance
- Lock-free skip-list memtable with freeze-and-swap flush and time-shard
  routing; out-of-order and late-arrival handling with last-write-wins dedup
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
- Caches: sub-10 µs last-value cache, TinyLFU segment cache, metadata cache
- Deletes are **ranged tombstones persisted in the catalog manifest**: a
  delete names an interval (resolved to the series' newest stored timestamp
  when you do not name one), survives a flush and a restart, and is reclaimed
  only once compaction has rewritten every segment it was issued against — so
  writing to a series after deleting it re-creates it, rather than being
  swallowed
- Retention with per-measurement overrides, multi-tier rollups computed
  during compaction, warm-tier re-compression, and streaming Parquet export
  (explicit dictionary encoding for tags, `max_bytes` budget that reports
  truncation rather than filling the device)
- **Parquet cold archive**: `archive_cold_segments()` re-encodes segments to
  Hive-partitioned Parquet (`namespace=…/shard=…/…parquet`), uploads them to
  S3/GCS/Azure, verifies each object, and then drops them from the hot
  database. DuckDB, Polars and Spark read the archive directly and prune on
  the partition columns; `register_cold_tier()` brings it back as a SQL table
  here. The hot tier stays `.csx`: specialised core, standard edges

**Query**

- Fluent `QueryBuilder` → vectorized Arrow execution, streaming 64 Ki-row
  batches, cardinality-aware grouping, projection pushdown to disk reads
- `execute_iter()` streams a scan in bounded memory: surviving segments are
  swept into time-disjoint buckets and merged one bucket at a time, so
  exporting or scanning a window far larger than RAM costs the busiest
  bucket rather than the whole result set. Aggregates fold into accumulators
  (memory proportional to the group count, not the row count), downsampling
  carries one open bucket across batches, and `LIMIT` stops the scan instead
  of truncating a materialised result
- **SQL** via DataFusion: predicate pushdown, cost statistics for the
  optimizer, spill-to-disk, ASOF JOIN, read-only enforcement at plan level, and
  22 analytics functions — one scalar (`time_bucket`), fifteen **window**
  functions (`diff`, `zscore`, `rolling_*`, `stl_*`, `anomaly_score`, …) that
  take a `PARTITION BY … ORDER BY`, and six **aggregates** (`first`, `last`,
  `rate`, `irate`, `forecast`, `multivariate_forecast`)
- **PromQL**: full parser/evaluator tracking **Prometheus 3.x** — 50+
  functions including the eight UTC date functions, all matcher operators, vector matching
  (`on`/`ignoring`/`group_left`/`group_right`), subqueries, instant + range
  queries. Left-open range and lookback windows; every range-vector function
  stamps the evaluation timestamp, so a range query returns exactly one point
  per step; `rate`/`increase`/`delta` implement `extrapolatedRate` including
  counter-reset correction and zero-clamping; `histogram_quantile` follows
  `bucketQuantile`; `double_exponential_smoothing`, `mad_over_time` and
  `sort_by_label`/`sort_by_label_desc` follow the 3.x definitions, natural
  label ordering included. A range query reads its window **once**, not once
  per step, and `chronix_promql_scan_cache_hits_total` says so at runtime. All
  of it is covered by an end-to-end conformance suite that drives the real
  query path

**Analytics**

- Preprocessing: gap detection, six interpolators, smoothing, resampling,
  clock-drift correction, STL decomposition, auto feature generation
- Forecast: SES, Holt (damped), Holt-Winters (additive/multiplicative),
  ARIMA, SARIMA, linear regression — auto-ARIMA via AIC, O(1) online
  updates, model persistence
- Quantile forecasting: prediction intervals from the empirical
  distribution of walk-forward residuals, bucketed per horizon step, so
  interval shape and growth are learned from the data instead of assumed
  Gaussian. Optional split-conformal correction — applied outward on both
  tails — for finite-sample coverage, plus explicit physical bounds
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
- Continuous aggregations with late-data bucket reopening
- Trigger engine: anomaly-score, forecast-deviation, threshold, MA-crossover
  (golden/death cross), rate-of-change, composite conditions; SQL management
  (`CREATE/SHOW/DROP/ALTER TRIGGER`); webhook delivery with HMAC-SHA256
  signing, SSRF protection, retry + dead-letter queue

**Server (`chronixd`)**

| Protocol | Default port | Description |
|----------|-------------|---------------------------------|
| HTTP/REST | 8086 | JSON write/query, InfluxDB Line Protocol (full escape semantics, `=` allowed in tags), management, `/metrics` |
| gRPC | 8087 | Write/query/schema, client-streaming ingestion with dedup keys |
| Flight SQL | 8817 | Arrow columnar transfer, catalog browsing (DBeaver/JDBC) |
| Prometheus | 8086 | Remote write/read, `/api/v1/query[_range]`, series/labels |
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
on does not pull in a C toolchain. Neither does anything else: the release
dependency graph of every crate here, `chronixd` included, contains no C build
step, which is what makes the cross-compiled aarch64 target the embedded case
is built for a plain `cargo build`. That holds because rustls is pinned to a
single crypto provider (`ring`) rather than inheriting each dependency's
default — see D41, which is also why the server's TLS starts at all.

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
| `chronix-engine` | `object-store` | off | S3/GCS/Azure cold tier, `.csx` → Parquet re-encode |
| `chronix` | `object-store` | off | `register_cold_tier()` — SQL over the Parquet archive |
| `chronix-streaming` | `flight` | off | Arrow Flight CDC export |
| `chronix-security` | `webhook` | on | Audit webhook sink |
| `chronixd` | `kafka`, `mqtt` | off | Ingestion connectors (`krafka` / `rumqttc`, both pure Rust) |
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
curl -X POST http://localhost:8086/api/v1/write/influx \
  -d 'cpu,host=srv1 usage=72.5 1609459200000000000'

# SQL
curl -X POST http://localhost:8086/api/v1/sql \
  -d '{"query":"SELECT time_bucket('\''5m'\'', _time) AS t, avg(usage) FROM cpu GROUP BY t"}'

# Health check
curl http://localhost:8086/health
```

## Examples

Runnable examples live in [`crates/chronix/examples/`](crates/chronix/examples/) — one per major feature,
each against an in-process embedded database:

`basic_usage`, `query_and_aggregation`, `sql_queries`, `promql_queries`,
`forecast`, `anomaly_detection`, `alerting`, `multivariate_analysis`,
`preprocessing`, `model_lifecycle`, `continuous_forecast`, `analytics`,
`encoding`, `schema_exploration`, `storage_lifecycle`, `delete_operations`,
`stream_aggregation`, `signal_triggers`, `data_pipeline`, `authz`,
`encryption`, `audit_logging`, `tenant_isolation`, `compute_engine`,
`chaos_testing`.

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
3,030 tests in the workspace (2,602 in the default build), property tests
on every codec, three fuzz targets run nightly, crash-recovery integration
tests, and 32 deep audit passes — but the on-disk format and public API are **not yet
stable**. The first tagged release will declare both.

The tree tracks the current ecosystem: **Arrow 59, DataFusion 55, parquet 59,
arrow-flight 59, tonic 0.14**. What remains before a first release is release
engineering — crates.io metadata, a semver policy, an API audit of the facade,
and a benchmark regression gate.

`cargo clippy --all-targets` is clean on the default build at the configured
lint level. The frozen cluster crates (`chronix-meta`, `chronix-cluster`,
`chronix-dsim`, excluded from `default-members`) are compile-checked in CI
but not lint-clean.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
