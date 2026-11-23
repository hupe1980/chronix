# Changelog

Notable changes to Chronix, in [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
format. Before 1.0 a breaking change bumps the minor version and a fix the patch.

## [0.10.0] - 2026-11-23

- **Breaking:** declared rollups store and return `<field>_count`
  as `UInt64`. Use an unsigned integer Arrow array when reading counts. Start
  with fresh targets for this schema. Export any history held only by an old
  float-count tier before rebuilding from the raw observations.
- Embedded rollup ranges select bucket starts inclusively. A read ending at
  the bucket start includes every available observation in that bucket.
  Empty reads expose the rollup columns when the source schema is known.
- `Chronix::ensure_rollup` creates a missing tier and accepts an equivalent
  declaration on restart. A conflicting definition reports the differing
  properties and preserves the original tier and watermark.
- Corrective backfills and deletes coordinate with materialisation so a
  correction crossing the watermark is included in the next repair.
  A failed catalog fsync rolls back the unacknowledged declaration.
- Calendar buckets with a local origin inside a repeated clock hour keep
  the same bucket start on both occurrences after the boundary. Bucket
  arithmetic also handles the first and last representable nanoseconds.
- HTTP, gRPC and Flight SQL accept independent port-zero binds. The
  `chronixd started` log record includes each assigned address.
- Updated `url` for [RUSTSEC-2026-0592](https://rustsec.org/advisories/RUSTSEC-2026-0592.html).

## [0.9.0] - 2026-11-20

**The calendar under a detector.** A season's slot is the wall-clock position, so a
quarter hour is judged against the same quarter hour on both clock-change days; forgetting
counts calendar periods; and chronix runs on one maintained time library with the IANA
database compiled in. No on-disk format change.

### Breaking changes

- One time library: chronix uses `jiff` with its compiled-in IANA database; `chrono` and `chrono-tz` are gone from every chronix crate, and a `default-features = false` build has no `chrono-tz` (RUSTSEC-2026-0555).
- `TimeBucket` and `Season` are `Clone`, not `Copy`.
- `TimeBucket::in_zone(Tz) -> Self` → `in_zone(&str) -> Result<Self, BucketParseError>`; `timezone()` returns `Option<&str>`.
- `Season::slot()` / `period()` return `&TimeBucket`; `Season::slot_of` returns `(slot, period index)`, not `(slot, period start)`.
- `Season::new` refuses a slot with an origin, a slot and period in different zones, and a calendar slot the period's origin would cut.
- `DetectorInfo` gains `warming_slots`.
- `time_bucket()` refuses a zone that is not a constant string instead of bucketing in UTC.
- ⚠️ Silent: a season's slot is the wall-clock position — 14:00 is slot 56 on every Europe/Berlin day, including both clock-change days, where it was 52 and 60.
- ⚠️ Silent: forgetting counts calendar periods (`TimeBucket::index_of`); a monthly or yearly season skipped or repeated periods.
- ⚠️ Silent: a zoned fixed-width bucket wider than its zone's offset starts at the standard-time midnight grid — `6h` in Europe/Berlin is 00:00/06:00/12:00/18:00, where it was 02:00/08:00/14:00/20:00.
- ⚠️ Silent: CloudEvents `time` and rollup `origin` print RFC 3339 with `Z` rather than `+00:00`.
- ⚠️ Silent: zone names are matched case-insensitively and reported in their canonical spelling; `Etc/Unknown` is refused.

### Added

- `brew install hupe1980/tap/chronixd` — macOS (Apple silicon) and Linux.
- `TimeBucket::index_of` (the bucket ordinal), `TimeBucket::wall_clock_ns`, `Season::slots`.
- Verdicts carry `slot`; `detectors()` and `/api/v1/detectors` report `warming_slots`.

## [0.8.0] - 2026-11-17

**Hard cut.** The on-disk format moves to generation 2 and a 0.7 data directory
is refused at `open()`, with a message naming both generations. Re-create the
directory and re-ingest; export anything only a rollup tier holds *before*
upgrading. From 0.8 the formats are frozen: no minor or patch release changes a
data directory.

### Breaking changes

- `DbError::PromQl(msg)` → `DbError::PromQl { message, span, hint }` (build with `DbError::promql(msg)`).
- `promql::ParseError` gains `span` and `hint`; its message no longer starts with a token index.
- `promql::ast`: `Duration(f64)` → `DurationExpr` (`DurationExpr::Literal(secs)`); `MatrixSelector` gains `modifier`, `VectorSelector` gains `smoothed`.
- `DbError` gains `ReadOnly`, `DirectoryLive` and `NamespaceQuotaExceeded`; `ChronixConfig` gains `read_only` and `parallelism`; `ForecastConfig`, `AutoForecastOptions` and `AutoForecast` gain fields — add `..Default::default()` to literals.
- `CpuEngine::thread_pool` returns `Option<&ThreadPool>`.
- `chronix::sql::register_udfs` is private.
- `_start` is a reserved column name; `ColumnRole::StartTimestamp` is a new variant.
- `SegmentCatalog::set_rollup` / `set_rollup_state` / `remove_rollup` → `set_declared` / `set_declared_state` / `remove_declared` with `DeclaredKind::Rollup`; `rollups()` returns an iterator.
- Remote write 2.0 follows rc.5: the per-series `created_timestamp` (field 6) is ignored.
- Cluster mode is the separate `chronixd-cluster` binary. `chronixd` drops the `cluster` feature, `--mode`, `--node-id`, `--raft-bind`, `--cluster-peers`, `--meta-addrs` and their `CHRONIXD_*` variables, and refuses a `[cluster]` section.
- Namespace quotas: `max_storage_bytes` is removed and refused by name (bound disk with retention); `0` means unlimited for every field; over-quota series or measurements are `400 NAMESPACE_QUOTA_EXCEEDED`; usage responses drop `storage_bytes`.
- `chronix_security::tenant` usage counters (`QuotaEnforcer`, `UsageRatio`, `get_usage`, `increment_*`, `check_and_increment_*`, …) are removed; use `Chronix::namespace_usage`.
- ⚠️ Silent: `ChronixConfig::small` runs queries and analytics on the caller's thread; set `parallelism` to opt back in.
- ⚠️ Silent: the `default` namespace's quota is unlimited unless set.
- ⚠️ Silent: reopening after a crash needs the key of every encrypted column the WAL holds; a missing or wrong key fails the open by name.

### Added

- **Console** at `/console` (feature `console`, on by default): PromQL and SQL with completion, charts with forecast and anomaly overlays, explain, cardinality, shareable URLs and copy as code.
- **MCP server** at `/mcp` (feature `mcp`, `[mcp]`): eight read-only tools — `list_metrics`, `label_values`, `query_promql`, `query_sql`, `explain`, `cardinality`, `forecast`, `detect_anomalies` — under the same auth, namespaces and Cedar policy as the query routes.
- `chronixd mcp --data-dir DIR` serves the same tools over stdio from a read-only directory.
- `chronixd --demo`: a generated gateway week with two planted anomalies, on loopback, with copy-paste queries and the console URL.
- `chronixd inspect DIR` (`--json`): state, format generation, measurements, series and WAL replay, without writing or locking. Embedded: `chronix::inspect::inspect`.
- `cargo install chronixd` — the server is on crates.io.
- Declared detectors: `declare_detector`, `drop_detector`, `detectors()`, `advance_detectors()`. A seasonal baseline per calendar slot with z-score and CUSUM, advanced by the maintenance pass, state that survives a restart, verdicts in `<name>_verdicts`; `/api/v1/detectors` on the server. Example: `declared_detector`.
- Start timestamps in a nullable `_start` column, from remote write 2.0 and OTLP; `Point::with_start_timestamp`.
- `GET /api/v1/status/tsdb` in Prometheus's shape, plus the series budget and headroom; `Chronix::cardinality`, `SeriesKey::canonical_parts`.
- PromQL conforms to Prometheus 3.16: `first_over_time`, duration expressions (`m[5m * 2]`, `offset (1h / 2)`), `anchored` and `smoothed` modifiers.
- `POST /api/v1/chronix/promql/explain` and `Chronix::promql_explain`; `Display` for PromQL expressions.
- Errors name the closest table, column, metric, label or function, or list what exists; HTTP error bodies carry `hint` and `span`; PromQL parse errors are positioned (`1:11: …`).
- Empty PromQL results warn about likely typos; `explain_empty=true` names the matcher that removed the last series. Embedded: `Chronix::diagnose`, `chronix::promql::explain_empty`.
- `ChronixConfig::parallelism` (`Serial`, `Threads(n)`, `Auto`) for scans, SQL and analytics; `Chronix::open_with_pool`; `CpuEngine::with_pool`, `CpuEngine::serial`.
- `[server] max_concurrent_queries`: SQL, PromQL and Flight SQL wait for a slot, then get `503` with `Retry-After`.
- Forecasts: `step` (fit on bucket means; `Chronix::auto_forecast_at_step`) and a `lower_bound` floor, reported as `clamped_below`.
- `ChronixConfig::read_only`: open a directory for queries only.
- `QueryBuilder::present(field)` keeps rows where a field has a value, from the validity bitmap alone.
- Per-namespace admission for embedded hosts: `set_namespace_limits`, `clear_namespace_limits`, `namespace_usage`.
- `chronix::arrow` re-exports the `arrow` crate results are in.
- Python client: `ChronixError.hint` and `ChronixError.span`.

### Changed

- Documentation reorganised into Learn, Guides, Reference and How it works, rewritten against the code; every old URL redirects.
- `ORDER BY _time [ASC|DESC] LIMIT n` stops early: 109 ms → 0.6 ms on 30 days of 50 series.
- Encrypted column values are sealed in the WAL (AES-256-GCM per key).
- The `LOCK` file records its holder (pid, host, open time, version) and is emptied on a clean close.
- Rollup and detector state share one declared-object record in the catalog.

### Fixed

- Namespace quotas were never enforced on writes; every write route now admits against them, from the same series set as the cardinality budget.
- Concurrent writes could overshoot the cardinality budget.
- Namespace request limits were lost on restart and ignored by gRPC and Flight SQL.
- `max_ingestion_rate` is enforced (`429 NAMESPACE_RATE_LIMITED`, or `400 BATCH_EXCEEDS_NAMESPACE_RATE` for a batch that can never fit) and can be set on create and update.
- `POST /api/v1/namespaces` refuses unknown fields instead of ignoring a misspelt quota.
- A certificate given with `--tls-cert` reloads every 60 s like one in the config file; `--tls-reload-interval-secs` defaulted to `0` and turned reloading off.
- Python client: a `429` rate limit raises `BackpressureError` (retryable, with `.retry_after`) instead of `QueryError`.
- `/api/v1/labels` and `/api/v1/label/{name}/values` no longer offer labels of metrics with no sample in the window.
- A failed second `open()` no longer truncates the holder's `LOCK` file.
- Compound PromQL durations (`1h30m`) and a bare `offset 100` parse.
- `chronixd --export-dashboards` works from an installed binary.
- The bundled dashboards provision from files; gradient fills and duplicate legends are fixed.
- The `anomaly_score` docs describe its output: a score in (0, 1), 0.5 at the threshold.
- The `analytics` feature docs state what turning it off removes.
- The Python client's licence metadata is `MIT OR Apache-2.0`.

## [0.7.0] - 2026-09-14

The on-disk format is unchanged; 0.6 data directories open normally.

### Added

- Native histograms: a distribution as one sample, with the Prometheus bucket schema (−4 to 8 and custom boundaries), the `histogram_*` PromQL family, rollups that merge them, ingest over remote write 1.0/2.0 and OTLP, remote read, and the classic `foo_bucket` / `_count` / `_sum` view.
- `Histogram::bound(schema, index)`.
- `EncodingType::Bytes` for columns of composite values.
- `Chronix::segments()`, `segments_of()` and `segment_count()`, returning owned `SegmentInfo`.
- `chronix_engine::format`: one version check for all four on-disk formats; `WalError::UnsupportedVersion` and `IndexError::UnsupportedVersion`.
- `ServerError::UnsupportedMediaType` (`415`).
- The workspace is on Rust edition 2024 (MSRV 1.94).

### Changed

- BREAKING: `chronix-analytics` is behind the facade's `analytics` feature (on by default; `sql` implies it).
- BREAKING: `Chronix::catalog`, `bloom_filters`, `rollup_registry`, `wal`, `shards` and `tag_index` are removed; use `segments()`, `segments_of()` and `segment_count()`.
- BREAKING: `MultivariateForecastResult` gains `series` and `target`, plus `for_series()` and `target_forecast()`.
- BREAKING: `preprocess::DriftReport` → `ClockDriftReport`, `lifecycle::DriftReport` → `ModelDriftReport`, `lifecycle::registry::AccuracyMetrics` → `VersionAccuracy`.
- BREAKING: `chronix_analytics::multivariate::granger` is removed; use `VarModel::granger_causality`.
- BREAKING: an OTLP explicit-bucket histogram is stored as one histogram column, not one field per bucket.
- BREAKING: an unset OTLP `sum` is absent, not zero.

### Fixed

- Histogram bucket boundaries are identical on every platform.
- Granger causality p-values were wrong (a Lanczos offset).
- `HoltWintersModel::fit` panicked on an empty series.
- A version mismatch in the WAL or series index was reported as corruption.

## [0.6.0] - 2026-09-13

### Added

- `Chronix::rollup_where(name, start, end, tags)`: a rollup view for the series matching `tags`.
- Robust STL: `StlConfig::robust()`, and a `robust` flag on the SQL `stl_*` functions.

### Fixed

- STL lost 11–22 % of the seasonal amplitude and found a season in a straight line.
- A NULL nulled the rest of its partition in `rolling_mean`, `rolling_std`, `rolling_corr` and `ewm`.
- `ljung_box` p-values were 11 % off.
- ARIMA mis-estimated mixed models, and a fitted ARIMA could be non-stationary and diverge.
- `rolling_std(v, 1)` returned `0.0` instead of NULL.
- An unknown detector name silently ran a z-score; it is now an error.
- A duplicate or unknown rollup, a malformed series key and an unknown model or detector answered `500`; they are `400` / `404`.

### Removed

- Four `[analytics]` settings that nothing read.

## [0.5.0] - 2026-09-12

### Added

- The Cedar authorization model is compiled in as a schema, and every policy is validated against it.
- Granular administrative capabilities (`ManageKeys`, `ManageBackups`, `ManageConfig`, `ManageModels`, `ManageNamespaces`, `ManageNodes`, `ManageRegions`, `ManageCluster`, `ViewCluster`), and Cedar roles on API keys.
- `PUT /api/v1/namespaces/{name}/quota`.
- Webhook signing secrets rotate: `triggers.webhook_signing_secrets` is a list, newest first.
- Scheduled checkpoints: `[database.checkpoints]` (interval, directory, `keep`).
- Per-column encryption is configurable: `[database.field_encryption]` names a column and the environment variable holding its key.
- `backup()` is a checkpoint: catalog-driven, lease-protected, hard-linked on the same filesystem; restore hard-links segments too.
- `Chronix::verify_backup()` and `POST /api/v1/admin/backup/verify`.
- Backup metrics: `chronix_backups_total`, `chronix_backup_failures_total`, `chronix_backup_bytes_total`, `chronix_backup_duration_seconds`.
- `restore()` verifies before it copies.
- `time_bucket()` and rollups take an `origin`; `TimeBucket::fixed(Duration)`.
- Arrow Flight `DoPut` can backfill.

### Changed

- `chronix-streaming`'s `flight` feature is `arrow` and no longer pulls the gRPC stack.
- `chronix`'s `security` and `streaming` features are off by default.
- `chronix-core` no longer depends on `serde_json`; `chronix-engine`'s `object-store` no longer pulls `tempfile`.
- A data request's authorization action comes from the route, not the HTTP method.
- gRPC and Flight SQL are behind the data-plane authorization gate.
- An administrative request needs both the capability and the policy.
- Health, readiness, metrics, the OpenAPI document and administrative routes are outside the data gate.
- `AuthzEngine::authorize_namespace` and `authorize_system` replace `authorize` and `authorize_admin`.
- BREAKING: the catalog stores segment paths relative to `segments/`.
- BREAKING: `triggers.webhook_signing_secret` → `triggers.webhook_signing_secrets` (a list).
- BREAKING: `CompactionTask` carries `segments_dir` and a relative `output`.
- BREAKING: `QueryBuilder::downsample()` takes a `TimeBucket`; `TimeBucket` and `BucketWidth` moved to `chronix_core::timebucket` (in the prelude).
- BREAKING: native `EXPLAIN` reports a `Downsample` bucket as width, timezone and calendar flag instead of `interval_ms`.
- BREAKING (gRPC): `SqlValue` and `FieldValue` gain a `json` variant.

### Fixed

- `LogReturnExpr`, `RollingStatExpr` and `RollingStat` are exported.
- A `[kafka]` or `[mqtt]` section in a binary built without that feature is a startup error.
- A connector that could not reach its broker reported `Running` for ever.
- An API key's creation was audited against the new key instead of the caller.
- Concurrent audited requests made the tamper-evident log report tampering.
- A failed audit write advanced the hash chain; a half-written line swallowed the next event.
- The trigger catalog fsynced the directory but not the data.
- Revoking a configured API key was undone by a restart.
- Creating a namespace was acknowledged before it was durable, and every registry failure was a `400`.
- gRPC `GetSchema` leaked another tenant's schema.
- The Python SDK's default ports and documented calls were wrong.
- A restored backup came up empty, and a backup taken under load could be incomplete.
- `tag_keys()` and `tag_values()` missed unflushed series.
- `last_value()` missed a pre-1970 point.
- Histograms reached Prometheus as summaries; `dashboards/query-performance.json` queried cluster-only metrics.
- Write-path counters are published at zero from startup.
- A soft delete's grace period no longer depends on the raw wall clock.
- `RollupBuilder::bucket()` dropped the bucket's origin.
- Every Arrow type a query produces reaches the client as a value, including negative-scale decimals.
- A SQL mistake (`1/0`, a bad cast, an unknown time zone) answers `400` with the reason instead of `500`.
- Converting query results back into points dropped `Decimal128` and other unenumerated columns.

### Removed

- `Chronix::Measurement` as an authorization resource; the unit of authorization is the namespace.
- Policy versioning, rollback and dry-run.
- `load_cross_tenant_isolation_policy` and `warn_if_no_namespace_policies`.
- `ChronixAction::{Forecast, DetectAnomalies, Subscribe, CreateRollup}` and `Admin` as a requestable action.
- `chronix-engine`'s dependency on `chronix-security`.
- Point-in-time recovery: `Chronix::restore_pitr`, `POST /api/v1/admin/restore/pitr`, `WalWriter::archive_before`.
- `WalEntry::Delete`, `EncryptingBackend` and `LocalFsBackend`.

## [0.4.0]

### Changed

- BREAKING: webhooks send a [CloudEvents](https://cloudevents.io) 1.0 envelope signed per [Standard Webhooks](https://www.standardwebhooks.com) `v1`.

### Fixed

- A soft-dropped measurement is invisible to every query surface for its whole grace period, and the pending state survives a restart.
- Measurement listings and PromQL discovery agree on which measurements exist.

## [0.3.0] and earlier

Predate this file. See the `v0.1.0` / `v0.2.0` / `v0.3.0` git tags.
