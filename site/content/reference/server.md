+++
title = "Server Architecture"
description = "How chronixd is put together: the HTTP, gRPC and Arrow Flight SQL surfaces, wire-protocol compatibility, metrics and distributed tracing."
weight = 60
+++

## Server Architecture (`chronixd`)

The `chronixd` crate provides a standalone server daemon that wraps the
embedded `Chronix` database with multi-protocol network access.

### Server Components

```text
┌───────────────────────────── chronixd ──────────────────────────────┐
│                                                                     │
│  ┌──────────┐  ┌──────────┐  ┌─────────────┐                       │
│  │ HTTP/REST │  │   gRPC   │  │ Flight SQL  │                       │
│  │  (axum)   │  │ (tonic)  │  │(arrow-flight│                       │
│  │  :8086    │  │  :8087   │  │  :8817      │                       │
│  └─────┬─────┘  └─────┬────┘  └──────┬──────┘                      │
│        │               │              │                              │
│        └───────────────┼──────────────┘                              │
│                        │                                             │
│               ┌────────▼────────┐                                    │
│               │  Arc<Chronix>   │  (shared embedded database)        │
│               └────────┬────────┘                                    │
│                        │                                             │
│  ┌─────────────────────┼────────────────────────┐                    │
│  │ chronix maintenance │ Prometheus Metrics      │                   │
│  │ thread (built in)   │ (metrics-exporter-prom) │                   │
│  └─────────────────────┴────────────────────────┘                    │
│                                                                     │
│  ┌──────────────── ConnectorManager ────────────────┐               │
│  │ ┌──────────────┐  ┌───────────────┐              │               │
│  │ │ KafkaConsumer │  │ MqttSubscriber│  (feature-   │               │
│  │ │ (krafka)     │  │ (rumqttc)     │   gated)     │               │
│  │ └──────────────┘  └───────────────┘              │               │
│  └──────────────────────────────────────────────────┘               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

- **Shared `Arc<Chronix>`** — All three servers share a single database
  instance. Writes via any protocol are immediately visible from all others.
- **`spawn_blocking`** — All Chronix methods are synchronous. The server wraps
  them with `tokio::task::spawn_blocking()` to avoid blocking the async runtime.
- **Signal-based shutdown** — `SIGTERM`/`SIGINT` triggers graceful shutdown:
  stop accepting connections, drain in-flight requests, flush memtables, close
  WAL, and exit. Server errors (both I/O and task panics) are logged.
- **TLS optional** — When a TLS section is present in the TOML config, all
  three servers use `rustls` for encryption. Otherwise they run in plaintext.
- **Zero `unsafe` code** — Connectors use an `OnceLock<Weak<Self>>` pattern
  for safe self-referencing instead of raw pointer reconstruction. `new_arc()`
  creates the Arc and stores a Weak self-reference; `start()` upgrades
  through `Weak::upgrade()` with a clear error if called outside an Arc.
- **Panic-free production paths** — `now_nanos()` and `setup_prometheus()`
  return `Result` instead of panicking. All error paths propagate cleanly.

### Flight SQL DoPut Pipeline

`DoPut` enables high-throughput bulk writes via Arrow RecordBatches. The
implementation follows this pipeline:

1. **Schema extraction** — `Schema::try_from(&FlightData)` decodes the Arrow
   schema from the first `FlightData` message in the stream
2. **Batch decoding** — Subsequent messages are decoded via
   `arrow_flight::utils::flight_data_to_arrow_batch()` using the extracted
   schema and an empty dictionary map
3. **Column classification** — Utf8 and dictionary-encoded string columns
   (`Dictionary<Int32, Utf8>`) are classified as tags (unless column metadata
   contains `role=field`), numeric/boolean columns as fields
4. **Point conversion** — `arrow_batch_to_points()` uses pre-downcast columnar
   access via `TypedField` and `TagCol` enums, performing O(columns) type
   downcasts instead of O(rows × columns). Tags support both plain `StringArray`
   and `DictionaryArray<Int32, Utf8>`. Fields are extracted with full type
   support (F64, I64, U64, Bool, String, Decimal128). Null values are skipped
   gracefully; a `Decimal128` value outside what Chronix stores is an error
   rather than a skipped field
5. **Database write** — Converted points are written via `db.insert_batch()`
   inside `spawn_blocking` for non-blocking async execution

The measurement name is derived from `CommandStatementUpdate.query`.

### Ingestion Connectors

`chronixd` includes built-in ingestion connectors for consuming time-series
data from external streaming systems. Connectors are **feature-gated** —
the real consumer/subscriber implementations are compiled only when the
corresponding Cargo feature flag is enabled.

#### Feature Flags

| Feature | Crate | Description |
|---------|-------|-------------|
| `kafka` | `krafka` 0.21 | Kafka consumer — pure async Rust, no C toolchain |
| `mqtt`  | `rumqttc` 0.25 | MQTT subscriber with async client. Plaintext only — see below |
| `all-connectors` | — | Enables both `kafka` and `mqtt` |

`rumqttc` is taken with `default-features = false`. Its `use-rustls` feature
is not a TLS option the subscriber offers — it never builds a `Transport` — so
enabling it only added `rustls-webpki` 0.102 (two advisories, and `rumqttc`'s
own `^0.102.8` caps it below the fixed 0.103.10), the unmaintained
`rustls-pemfile`, and a second crypto provider. MQTT connections are plaintext;
TLS returns when `rumqttc` moves to a provider-agnostic rustls.

Without the feature flag, the connector struct is still available (for
configuration parsing and testing), but `start()` logs a notice and
returns immediately. The `#[cfg_attr(not(feature = "..."), allow(dead_code))]`
pattern suppresses unused-field warnings for feature-gated fields.

#### Connector Trait

```rust
#[async_trait]
pub trait IngestionConnector: Send + Sync {
    fn name(&self) -> &str;
    fn connector_type(&self) -> &str;
    async fn start(&self) -> Result<(), ServerError>;
    async fn stop(&self) -> Result<(), ServerError>;
    async fn status(&self) -> ConnectorStatus;
    async fn metrics(&self) -> ConnectorMetrics;
    async fn is_healthy(&self) -> bool;
}
```

#### Connector Format (RQ-05)

The payload format is configured via the `ConnectorFormat` enum:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorFormat {
    LineProtocol,
    Json,
}
```

Invalid format values are rejected at deserialization time (serde) rather
than at runtime. Match statements in the consumer/subscriber are exhaustive —
no fallthrough `"unsupported format"` branches.

#### Topic-to-Measurement Mapping (ENT-06)

Both Kafka and MQTT connectors support explicit `topic_measurement_map`
for controlling which topic maps to which measurement name:

```toml
[mqtt]
topic_measurement_map = { "sensors/floor1/temp" = "temperature", "sensors/floor2/temp" = "temperature" }
```

When a topic is not in the map, the default behavior applies:
- **Kafka** — uses the topic name directly
- **MQTT** — uses the last segment of the topic path (e.g., `sensors/room1/temperature` → `temperature`)

This prevents measurement collisions when topics with different paths share
the same last segment.

#### Kafka Consumer (`kafka` feature)

When compiled with `--features kafka`, `KafkaConsumer` spawns a `krafka`
`Consumer` loop:

1. Builds a `Consumer` with brokers, group ID, offset reset policy, and the
   authentication implied by `security_protocol` / `sasl_mechanism`
2. Subscribes to configured topics
3. Polls a batch per second in a Tokio task
4. Parses payloads (JSON via `util::parse_json_point()`, or InfluxDB Line
   Protocol via `influx::parse_line_protocol()`)
5. Writes the whole batch with one `insert_batch()`
6. Commits offsets only after the write succeeds — a failed write leaves the
   batch uncommitted, so it is redelivered rather than lost (at-least-once)

`krafka` rather than `rdkafka`: `rdkafka` builds librdkafka from C via cmake,
which imposes a C toolchain on every build — including the aarch64
cross-compile — and adds an FFI surface to a tree that denies `unsafe_code`.

##### Transport security

`security_protocol` accepts `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT` and
`SASL_SSL`; `sasl_mechanism` accepts `PLAIN`, `SCRAM-SHA-256` and
`SCRAM-SHA-512`. Credentials come from `credential_file` (preferred, for
rotation) or from inline `sasl_username`/`sasl_password`. `ca_cert` points at
the PEM bundle for a private CA; without it the bundled WebPKI roots apply,
which is what a managed broker needs. An unrecognised value for either field
is rejected by name rather than ignored.

```toml
[kafka]
brokers = "broker:9093"
group_id = "chronix-ingest"
topics = ["metrics"]
security_protocol = "SASL_SSL"
sasl_mechanism = "SCRAM-SHA-512"
credential_file = "/run/secrets/kafka.json"
ca_cert = "/etc/ssl/private-ca.pem"
```
7. On stop, sets the running flag and yields to allow the consumer task
   to drain

#### MQTT Subscriber (`mqtt` feature)

When compiled with `--features mqtt`, `MqttSubscriber` spawns a `rumqttc`
`AsyncClient` loop:

1. Creates `MqttOptions` with broker, port, client ID, and optional TLS
2. Subscribes to configured topics with the specified QoS level
3. Handles `Event::Incoming(Packet::Publish)` events
4. Extracts topic-level tags from the MQTT topic path
5. Parses payloads via `util::parse_json_point()` with extra tags
6. Writes to the database with `insert_batch()`
7. Reconnects automatically with 5-second backoff on connection errors
8. Tracks reconnection count in metrics

#### Connector Manager

`ConnectorManager` orchestrates connector lifecycle:

- **`register()`** / **`start_all()`** / **`stop_all()`** — basic lifecycle
- **`reload(config)`** — diff-and-reconcile: builds the desired connector set
  from the new config, stops removed connectors, registers + starts new ones,
  preserves unchanged running connectors
- **`list_connectors()`** — returns `ConnectorInfo` (name, type, status, metrics)
  for the REST status API at `GET /api/v1/connectors`
- **`all_healthy()`** — aggregated health check for the `/ready` endpoint

Connectors are wired into `server.rs`: on startup, `KafkaConsumer` and
`MqttSubscriber` instances are created from `ServerConfig.kafka` /
`ServerConfig.mqtt`, registered with the manager, and started. On shutdown,
`stop_all()` drains in-flight data before the database is closed.

### Shared Utilities (`util.rs`)

Common parsing and conversion utilities shared across HTTP, Kafka, and MQTT:

- **`parse_json_point(measurement, text, extra_tags)`** — Parses a JSON payload
  into a `Point`. Extracts `tags`, `fields`, and optional `timestamp` keys.
  Supports optional `extra_tags` for MQTT topic-derived tags.
- **`json_value_to_field(key, value)`** — Converts a `serde_json::Value` to a
  `FieldValue`. Integers are preserved as `I64` (not coerced to F64).
- **`now_nanos()`** — Current wall clock as nanosecond Unix epoch. Returns
  `Result` to gracefully handle pre-epoch clocks and i64 overflow (year 2262+).
- **`column_type_to_str()` / `column_type_to_arrow()`** — Schema introspection
  helpers for gRPC and Flight SQL responses.

### Result encoding (`wire::value`)

One Arrow → JSON encoding, shared by every surface that answers a query in
JSON or protobuf. Its `match` over `DataType` has no catch-all arm, so an
Arrow release that adds a variant fails the build.

- **`to_json(col, idx)`** — a single cell, per the table in
  [API reference](@/docs/api-reference.md).
- **gRPC** keeps native proto scalars where one exists — `double` carries
  `NaN`, which JSON cannot — and routes the rest through `to_json` into
  `SqlValue.json`, distinct from `SqlValue.string`.
- **Arrow Flight SQL** streams the `RecordBatch` unmodified, preserving the
  Arrow types themselves.

`chronix_query::arrow_cell_to_field_value` is the inverse, used by the
region-snapshot and distributed-read paths: total over the six storage column
types, and an error for any other Arrow type.

### Server Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `chronix_http_points_written_total` | Counter | Points written via REST JSON |
| `chronix_influx_points_written_total` | Counter | Points written via InfluxDB Line Protocol |
| `chronix_http_queries_total` | Counter | Queries executed via REST |
| `chronix_grpc_points_written_total` | Counter | Points written via gRPC |
| `chronix_grpc_queries_total` | Counter | Queries executed via gRPC |
| `chronix_kafka_messages_consumed_total` | Counter | Messages consumed from Kafka topics |
| `chronix_kafka_consumer_lag` | Gauge | Kafka consumer lag in messages, summed over the connector's assigned partitions and labelled `connector`. Read from the watermarks the last fetch response already carried, so it costs no broker round trip |
| `chronix_kafka_deserialization_errors_total` | Counter | Kafka payload deserialization failures |
| `chronix_mqtt_messages_received_total` | Counter | Messages received from MQTT topics |
| `chronix_mqtt_reconnections_total` | Counter | MQTT broker reconnection count |
| `chronix_mqtt_decode_errors_total` | Counter | MQTT payload decode failures |
## Wire Protocol Compatibility

### Prometheus Remote Write/Read

- `POST /api/v1/prom/write` — Accepts Prometheus remote write protocol
  (protobuf + snappy). Maps `__name__` → measurement, labels → tags,
  samples → fields.
- `POST /api/v1/prom/read` — Serves Prometheus remote read requests.
  Translates `ReadRequest` matchers to Chronix tag filters.
- `GET /metrics` — Prometheus text exposition format. The built-in parser
  supports all four metric types: **gauge**, **counter**, **histogram**
  (bucket aggregation with `le` bounds, `_sum`, `_count`), and **summary**
  (quantile values with `_sum`, `_count`). `# TYPE` directives route lines
  to type-specific handlers. Histogram buckets are auto-sorted by bound.

### OpenTelemetry OTLP

- `POST /v1/metrics` — the path the OpenTelemetry Collector's `otlphttp`
  exporter derives from its endpoint. Accepts OTLP metrics (protobuf and
  JSON), gzipped or not. `POST /api/v1/otlp/metrics` is an alias.
  Maps gauge, sum, histogram, summary → Chronix measurements + fields.
  Resource attributes → tags.
## Metrics / Observability

Chronix uses the [`metrics`](https://crates.io/crates/metrics) facade crate
(v0.24) to emit runtime telemetry. The library emits metrics via `counter!`,
`gauge!`, and `histogram!` macros — the embedding application is responsible
for installing a recorder (e.g., `metrics-exporter-prometheus`).

| Metric | Type | Emitted From | Description |
|--------|------|--------------|-------------|
| `chronix_write_errors_total` | Counter | every write path | Writes that failed, labelled `reason` = `storage_full` \| `timeout` \| `rejected` \| `panic`. `storage_full` is the data volume, and the one to alert on |
| `chronix_sql_results_truncated_total` | Counter | the HTTP, gRPC and Flight SQL query paths | Results the `sql_max_rows` ceiling cut short or refused. A non-zero rate means somebody is reading a prefix and calling it an answer |
| `chronix_backfill_points_total` | Counter | every write path with `?backfill=true` | Points written **outside** the out-of-order window — importing history rather than ingesting live |
| `chronix_backups_total` | Counter | `backup()` | Checkpoints taken. Published at zero, so an alert on a nightly backup that stopped running can fire from the first scrape |
| `chronix_backup_failures_total` | Counter | `backup()` | Checkpoints that failed. **The one to alert on** — a backup that silently stops working is what the whole subsystem exists to prevent |
| `chronix_backup_bytes_total` | Counter | `backup()` | Bytes the checkpoints placed. Hard-linked segments count their size, so this measures the database rather than the disk the backup consumed |
| `chronix_backup_duration_seconds` | Histogram | `backup()` | How long a checkpoint takes, flush included |
| `chronix_compaction_backpressure_active` | Gauge | `apply_backpressure()` | 1.0 when write throttling is active, 0.0 otherwise |
| `chronix_catalog_fsync_total` | Counter | the catalog manifest | Manifest fsyncs. Beside `chronix_wal_fsync_total`: on flash the fsync rate is the wear rate. A compaction is **one**, whatever it retires, and so is a delete however many tombstones it produces |
| `chronix_maintenance_running` | Gauge | the maintenance thread, refreshed each tick | `1` while it is alive, `0` once it has ended. Everything a database does for itself happens there, so a `0` — or an absence lasting longer than a tick — means the process will stop accepting writes and only a restart recovers. **The one to alert on** |
| `chronix_maintenance_panics_total` | Counter | the maintenance thread | Passes that panicked. The thread survives one and the other passes keep running, so this is a defect report rather than an outage |
| `chronix_memtable_memory_bytes` | Gauge | `statistics()` | Memtable rows and index entries |
| `chronix_interner_memory_bytes` | Gauge | `statistics()` | String interners — grows with cardinality, not row count |
| `chronix_wal_buffer_bytes` | Gauge | `statistics()` | WAL writer's buffer; fixed size |
| `chronix_catalog_memory_bytes` | Gauge | `statistics()` | Segment catalog, schema registry and tombstones |
| `chronix_retention_shards_dropped_total` | Counter | `enforce_retention()` | Shards retention **removed** — not the ones it considered. A pass that declines an expired shard because a rollup still needs it counts zero here |
| `chronix_series_released_total` | Counter | `enforce_retention()`, `gc()`, `archive_cold_segments()` | Series returned to the cardinality budget because the last of their data was deleted |
| `chronix_gc_segments_deleted_total` | Counter | `gc()` | Segment files unlinked by garbage collection — the ones a retirement pass had to leave because a running read still held them |
| `chronix_segments_retired_awaiting_readers_total` | Counter | every retirement pass | Segments taken out of the database whose files could not be unlinked yet, because a running scan had already been handed the path |
| `chronix_leased_segments` | Gauge | `statistics()` | Segment files a running read is holding. Near zero at rest; a number that only grows is a scan nobody dropped |
| `chronix_rollup_invalidations_total` | Counter | `backfill()`, `execute_delete()` | Rollups whose buckets were marked for recomputation by a late write or a delete |
| `chronix_rollup_ranges_recomputed_total` | Counter | `materialise_rollups()` | Invalidated ranges recomputed |
| `chronix_rollup_failures_total` | Counter | `materialise_rollups()` | Rollups whose materialisation failed this pass (the others still run) |
| `chronix_cold_archive_groups_awaiting_rollup_total` | Counter | `archive_cold_segments()` | `(measurement, shard)` groups kept hot because a rollup has not caught up |
| `chronix_cold_archive_groups_incomplete_total` | Counter | `archive_cold_segments()` | Groups kept hot because another segment or an unflushed row overlaps their range |
| `chronix_cold_archive_objects_total` | Counter | `archive_cold_segments()` | Archive objects uploaded and verified |
| `chronix_cold_archive_rows_total` | Counter | `archive_cold_segments()` | Rows written to the archive, after dedup and tombstones |
| `chronix_rollup_points_written_total` | Counter | `materialise_rollups()` | Rollup points written by materialisation |
| `chronix_retention_segments_awaiting_rollup_total` | Counter | `enforce_retention()` | Expired segments preserved because a rollup they feed is not yet materialised past them |
| `chronix_rollup_points_written_total` | Counter | `compact()` | Number of input rows processed for rollup aggregation |
| `chronix_forecast_fit_duration_seconds` | Histogram | `chronix-analytics::forecast` | Duration of model fit operations |
| `chronix_forecast_predict_duration_seconds` | Histogram | `chronix-analytics::forecast` | Duration of model predict operations |
| `chronix_anomaly_fit_duration_seconds` | Histogram | `chronix-analytics::anomaly` | Duration of anomaly detector fit |
| `chronix_anomaly_detect_duration_seconds` | Histogram | `chronix-analytics::anomaly` | Duration of anomaly detection |
| `chronix_anomaly_detected_total` | Counter | `chronix-analytics::anomaly` | Total anomalies detected |
| `chronix_preprocess_duration_seconds` | Histogram | `chronix-analytics::preprocess` | Duration of preprocessing pipeline |
| `chronix_clock_drift_corrections_total` | Counter | `chronix-analytics::preprocess` | Clock drift corrections applied |
| `chronix_compute_batch_operations_total` | Counter | `chronix-analytics::compute` | Batch compute operations executed |
| `chronix_compute_parallel_fit_duration_seconds` | Histogram | `chronix-analytics::compute` | Duration of parallel model fitting |
| `chronix_segment_prefetch_bytes_total` | Counter | `chronix-engine::segment` | Total bytes successfully prefetched via mmap madvise |
| `chronix_segment_prefetch_errors_total` | Counter | `chronix-engine::segment` | Prefetch madvise failures |
| `chronix_model_drift_detected_total` | Counter | `chronix-analytics::lifecycle` | Drift detection events |
| `chronix_model_drift_score` | Gauge | `chronix-analytics::lifecycle` | Current drift score per model |
| `chronix_model_accuracy_mape` | Gauge | `chronix-analytics::lifecycle` | Model accuracy (MAPE) |
| `chronix_model_accuracy_rmse` | Gauge | `chronix-analytics::lifecycle` | Model accuracy (RMSE) |
| `chronix_model_age_seconds` | Gauge | `chronix-analytics::lifecycle` | Time since last model fit |
| `chronix_model_refit_total` | Counter | `chronix-analytics::lifecycle` | Model re-fit events |
| `chronix_model_ab_test_champion_mape` | Gauge | `chronix-analytics::lifecycle` | Champion model MAPE in A/B test |
| `chronix_model_ab_test_challenger_mape` | Gauge | `chronix-analytics::lifecycle` | Challenger model MAPE in A/B test |
| `chronix_correlation_compute_duration_seconds` | Histogram | `chronix-analytics::multivariate` | Duration of correlation computation |
| `chronix_multivariate_anomaly_detect_duration_seconds` | Histogram | `chronix-analytics::multivariate` | Duration of MV anomaly detection |
| `chronix_derived_series_eval_duration_seconds` | Histogram | `chronix-analytics::multivariate` | Duration of derived series evaluation |
| `chronix_composite_signal_fired_total` | Counter | `chronix-analytics::multivariate` | Composite signals fired |
| `chronix_signal_delivery_queued_total` | Counter | `DeliveryRouter::deliver` | Signals handed to a channel's worker — queued, not yet sent |
| `chronix_signal_delivery_total` | Counter | delivery worker | Signals delivered, labelled `channel` |
| `chronix_signal_delivery_failed_total` | Counter | delivery worker | Failed delivery *attempts*, labelled `channel` — a retried signal counts once per attempt |
| `chronix_signal_delivery_duration_seconds` | Histogram | delivery worker | Queue to successful delivery, **including retries**, labelled `channel` |
| `chronix_signal_delivery_queue_depth` | Gauge | `DeliveryRouter::deliver` | Signals waiting on a channel's worker, labelled `channel` |
| `chronix_signal_delivery_dropped_total` | Counter | `DeliveryRouter::deliver` | Signals discarded on a full queue, labelled `channel`. **Non-zero means alerts are being lost** |
| `chronix_signal_delivery_unrouted_total` | Counter | `DeliveryRouter::deliver` | Signals naming an unregistered channel — fired, delivered nowhere |
| `chronix_signal_evicted_total` | Counter | `SignalStore::store` | Signals evicted from a namespace's own ring to make room |

**Tracing instrumentation** – All forecast `fit()`/`predict()` and anomaly `fit()`/`detect()` operations are annotated with `#[tracing::instrument]` spans at debug level, enabling timing analysis via any `tracing-subscriber` backend.
## Distributed Tracing (`chronixd::otel`)

### OpenTelemetry Integration

Feature-gated `otlp` (default) enables OTLP trace export:

```text
TracingConfig
  ├── service_name
  ├── log_format: Pretty | Json | Compact
  ├── log_filter: EnvFilter string
  └── otlp: Option<OtlpConfig>
        ├── endpoint (default: http://localhost:4317)
        ├── sampling: AlwaysOn | AlwaysOff | Ratio(f64)
        └── always_sample_errors
```

### W3C Trace Context Propagation

- `extract_trace_context()` — reads `traceparent`/`tracestate` from gRPC metadata
- `inject_trace_context()` — writes trace context into outgoing gRPC metadata
- `make_trace_interceptor()` — tonic interceptor that injects trace context into outgoing requests
  - With `otlp` feature: uses OTel span context (includes current span ID)
  - Without `otlp` feature: uses task-local traceparent (set via `with_trace_context()`)
- `with_trace_context(parent, state, future)` — scopes a task-local W3C trace context, enabling trace propagation in downstream gRPC calls even without the OTLP exporter

### TracingGuard

RAII guard stores `SdkTracerProvider`; on drop, calls `provider.shutdown()` for clean OTLP flush.

---
