+++
title = "API Reference"
description = "Every Chronix endpoint: REST write and query, PromQL, Prometheus remote write/read, OTLP metrics, gRPC, Arrow Flight SQL, and the embedded Rust API, each with a runnable example."
weight = 20
+++

> Comprehensive reference for all Chronix server endpoints: REST, gRPC,
> Flight SQL, and the embedded Rust API.

**Base URL:** `http://localhost:8086` (default)

> **OpenAPI Spec:** A machine-readable OpenAPI 3.1 specification is available at `GET /api/v1/openapi.json`. Use this with Swagger UI, Redoc, or code generators for the most up-to-date API documentation.

---

## Table of Contents

- [Health & Metrics](#health-metrics)
- [Write Endpoints](#write-endpoints)
- [Query Endpoints](#query-endpoints)
- [PromQL Endpoints](#promql-endpoints)
- [Wire Protocol Endpoints](#wire-protocol-endpoints)
- [Annotations & Dashboards](#annotations-dashboards)
- [Management Endpoints](#management-endpoints)
- [Namespace Endpoints (Multi-Tenancy)](#namespace-endpoints)
- [Admin — Cluster Management](#admin-cluster-management)
- [Admin — Analytics Model Management](#admin-analytics-model-management)
- [Admin — Chaos Injection](#admin-chaos-injection)
- [Authentication](#authentication)
- [gRPC API](#grpc-api)
- [Flight SQL API](#flight-sql-api)
- [Embedded Rust API](#embedded-rust-api)

---

## Health & Metrics

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness probe. Returns `200 OK` when the process is running. |
| `GET` | `/ready` | Readiness probe. Returns `200 OK` when the database is open and accepting queries. |
| `GET` | `/metrics` | Prometheus-format metrics scrape endpoint. Path is configurable. All four Prometheus metric types (counter, gauge, histogram, summary) are fully parsed, including per-bucket and per-quantile data. |

### Example

```bash
curl http://localhost:8086/health
# {"status":"ok"}

curl http://localhost:8086/ready
# {"status":"ready","uptime_secs":123}
```

---

## Write Endpoints

### `POST /api/v1/write`

Write one or more time-series points in native JSON format.

**Request Body:**

```json
[
  {
    "measurement": "cpu",
    "tags": {"host": "srv1", "region": "us-east"},
    "fields": {"usage": 72.5, "count": 42},
    "timestamp": 1700000000000000000
  }
]
```

- `measurement` (string, required): Target measurement name.
- `tags` (object, optional): String key-value pairs for series grouping.
- `fields` (object, required): Numeric/boolean/string field values.
- `timestamp` (i64, optional): Nanosecond Unix epoch. Defaults to server time.

**Response:** `204 No Content`

### `POST /api/v1/write/influx`

Write data in [InfluxDB Line Protocol](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/) format.

**Content-Type:** `text/plain`

```
cpu,host=srv1,region=us-east usage=72.5,count=42i 1700000000000000000
```

**Response:** `204 No Content`

#### Escape semantics

Chronix implements the Line Protocol escape rules in full:

| Section | Escapable | Notes |
|---|---|---|
| Measurement | `,` ` ` | |
| Tag key / tag value | `,` `=` ` ` | `"` is an **ordinary character** here |
| Field key | `,` `=` ` ` | |
| String field value | `"` `\` | |

Two consequences worth calling out, because both differ from the previous
implementation:

- **`\\` is a literal backslash.** `path="C:\\"` is a value of `C:\` followed
  by a *real* closing quote. Escape detection counts consecutive backslashes,
  so an escaped backslash before a delimiter no longer swallows the rest of
  the line.
- **`=` is allowed in tag keys and values** (escaped, per the spec). Chronix
  previously rejected `=` in tag values to keep its internal series identity
  unambiguous; identity uses reserved control-character separators
  instead, so lines InfluxDB accepts are accepted here. See
  [Series Identity](@/reference/crate-layout.md#series-identity-and-the-canonical-form).

`\0` and `\x01` are reserved as identity separators and are rejected in
measurement names, tag keys and tag values.

**Batch size limit:** Both the JSON `/write` and `/write/influx` endpoints
enforce the same `max_write_batch_size` limit (default: 100,000 points).
Requests exceeding this limit are rejected with `400 Bad Request`.

**Write timeout:** All write endpoints enforce a configurable `write_timeout_secs`
deadline (default: 30 s). If the write does not complete within the deadline,
the server responds with `504 Gateway Timeout`. Set to `0` to disable.
This applies uniformly to HTTP JSON, Line Protocol, gRPC Write/StreamWrite,
Flight SQL DoPut, Prometheus remote write, and OTLP metrics ingestion.

---

## Query Endpoints

### `POST /api/v1/query`

Query time-series data using the native query plan format.

**Request Body:**

```json
{
  "measurement": "cpu",
  "tags": {"host": "srv1"},
  "fields": ["usage"],
  "start": 1700000000000000000,
  "end": 1700100000000000000
}
```

**Response:**

```json
{
  "columns": ["timestamp", "usage"],
  "rows": [
    {"timestamp": 1700000000000000000, "fields": {"usage": 72.5}}
  ]
}
```

### `POST /api/v1/sql`

Execute a SQL query via DataFusion.

**Request Body:**

```json
{"sql": "SELECT * FROM cpu WHERE host = 'srv1' ORDER BY timestamp DESC LIMIT 10"}
```

**Response:** JSON array of row objects with column names as keys.

### SQL Trigger WHEN Clause Syntax

SQL-based signal triggers support compound conditions with `AND`, `OR`, and
parenthesized grouping in `WHEN` clauses:

```sql
-- Simple condition
CREATE TRIGGER high_cpu WHEN value > 90
  ON cpu_usage ACTION webhook 'https://alerts.example.com';

-- Compound AND/OR with parentheses
CREATE TRIGGER complex_alert
  WHEN (value > 90 AND host = 'prod-srv1') OR (value > 95)
  ON cpu_usage ACTION webhook 'https://alerts.example.com';
```

The parser supports arbitrary nesting of `AND`/`OR` operators with
explicit parentheses for precedence control. Trigger names are validated
against SQL injection patterns — only alphanumeric characters and
underscores are accepted.

---

## PromQL Endpoints

Prometheus-compatible query API. Configure Grafana with a Prometheus
data source pointing to `http://chronix.example.com:8086/api/v1/prom/`.

| Method | Path | Description |
|--------|------|-------------|
| `GET/POST` | `/api/v1/prom/query` | Instant query |
| `GET/POST` | `/api/v1/prom/query_range` | Range query |
| `GET` | `/api/v1/prom/labels` | List all label names |
| `GET` | `/api/v1/prom/label/{name}/values` | Values for a given label |
| `GET` | `/api/v1/prom/series` | Find series matching label matchers |
| `GET` | `/api/v1/prom/metadata` | Metric metadata (types, help text) |

`/query` and `/query_range` answer `{"status":"success","data":{"resultType":…,"result":…}}`.
`/labels`, `/label/{name}/values` and `/series` answer a **bare array** under
`data`, as Prometheus does, with label names and values **sorted**.

All three accept `start` and `end` (seconds since epoch, defaulting to the last
hour) and repeated **`match[]`** series selectors. Every matcher in a selector
is applied, not only `__name__`:

```
GET /api/v1/prom/label/dc/values?match[]={__name__="cpu",host="a"}
→ {"status":"success","data":["eu"]}
```

Repeated `match[]` parameters are a **union**, as in Prometheus. A selector
that names no metric is skipped rather than treated as "everything", since it
cannot be answered without scanning every measurement.

`/label/__name__/values` lists the measurements holding data in the window, and
`/metadata` describes each one's fields — the two calls Grafana makes to
populate a metric browser.

### Semantics

The evaluator targets **Prometheus 3.x**. Two of its differences from 2.x are
visible in returned data:

- **Range selectors and the lookback window are left-open, right-closed**:
  `foo[5m]` at time `t` selects samples in `(t − 5m, t]`. A sample landing
  exactly on the older boundary belongs to the previous window, so evenly
  spaced samples yield a constant count per range.
- **`.` in a matcher regex matches newline.** Matchers remain fully anchored,
  so `{job=~"api"}` matches only the exact value.

A range query returns exactly one point per step per series: every function
over a range vector stamps the step's evaluation timestamp, so the matrix is
step-aligned and strictly increasing. `NaN` ranks last in `topk`, `bottomk`,
`sort` and `sort_desc`.

The 3.x-only functions `double_exponential_smoothing`, `mad_over_time`,
`sort_by_label` and `sort_by_label_desc` are available **without** a feature
flag (upstream gates them behind `--enable-feature=promql-experimental-functions`).
Two notes on their behaviour:

- `double_exponential_smoothing(v, sf, tf)` requires `0 < sf < 1` and
  `0 < tf < 1` — a value on the boundary is an error, not a clamp — and returns
  nothing for a range holding fewer than two samples.
- `sort_by_label` orders **naturally**, so `pod-2` precedes `pod-10`, with the
  full label set as the tie-break.

The eight date functions — `year`, `month`, `day_of_month`, `day_of_week`,
`day_of_year`, `days_in_month`, `hour`, `minute` — read the sample **value** as
a Unix timestamp in seconds and answer in **UTC**, so `hour() < 9` means the
same thing wherever the server runs. Called with no argument they use
`vector(time())`, and all of them drop `__name__`.

Not implemented: native histograms (`histogram_quantile` handles classic
`le`-bucketed histograms only) and the experimental `limitk` / `limit_ratio`.
A query using one of these returns an error rather than a wrong answer.

### Instant Query

```bash
curl 'http://localhost:8086/api/v1/prom/query?query=cpu_usage{host="srv1"}&time=1700000000'
```

### Range Query

```bash
curl 'http://localhost:8086/api/v1/prom/query_range?query=cpu_usage&start=1700000000&end=1700100000&step=60'
```

### Label Values

```bash
# List all measurement names
curl http://localhost:8086/api/v1/prom/label/__name__/values

# List all values for the "host" tag
curl http://localhost:8086/api/v1/prom/label/host/values
```

**Response Format:**

All PromQL endpoints return the standard Prometheus response envelope:

```json
{
  "status": "success",
  "data": {
    "resultType": "vector|matrix|scalar|string|labels|series|metadata",
    "result": [...]
  }
}
```

---

## Wire Protocol Endpoints

### `POST /api/v1/prom/write`

Prometheus Remote Write endpoint. Accepts snappy-compressed protobuf.

```bash
# Used automatically by Prometheus with remote_write config:
# remote_write:
#   - url: http://chronix:8086/api/v1/prom/write
```

**Non-finite samples are skipped, not rejected.** Chronix refuses to store
`NaN` and `±Inf` — a `NaN` in storage poisons every aggregate that reads it —
but Prometheus sends a **staleness marker**, which on the wire is a `NaN`,
every time a scrape target disappears. Returning `400` for the batch would
stall the remote-write queue indefinitely, because Prometheus retries rather
than drops. Such samples are counted in
`chronix_wire_non_finite_samples_skipped_total` and the rest of the batch is
written; the endpoint still returns `204`.

### `POST /api/v1/prom/read`

Prometheus Remote Read endpoint. Accepts snappy-compressed protobuf.

### Prometheus Metric Type Support

`parse_prometheus_text()` fully parses all four Prometheus exposition format
metric types: **counter**, **gauge**, **histogram**, and **summary**.
Histogram metrics are returned as `HistogramMetric` structs containing
per-bucket (`HistogramBucket`) boundary/count pairs plus `_sum` and `_count`.
Summary metrics are returned as `SummaryMetric` structs containing
per-quantile (`SummaryQuantile`) φ/value pairs plus `_sum` and `_count`.
The `ServerMetrics` struct exposes `histograms` and `summaries` fields
alongside the existing counters and gauges.

### `POST /api/v1/otlp/metrics`

The same rule applies: an OTLP summary quantile with nothing observed behind
it is `NaN`, and a gauge may report `NaN` or `±Inf` at any time. Gauge and sum
data points carrying a non-finite value are skipped; a histogram or summary
keeps the fields that *are* finite rather than losing the whole point.


OpenTelemetry OTLP metrics ingestion. Accepts OTLP JSON format with
gauges, sums, histograms, and summaries.

```bash
curl -X POST http://localhost:8086/api/v1/otlp/metrics \
  -H 'Content-Type: application/json' \
  -d '{"resourceMetrics": [...]}'
```

---

## Annotations & Dashboards

### `GET /api/v1/annotations`

Query past signal/alert events as Grafana-compatible annotations.

**Response:**

```json
[
  {"text": "Signal event in _signals", "time": 1700000000000, "tags": ["_signals"]}
]
```

### `GET /api/v1/annotations/stream`

Server-Sent Events (SSE) stream of real-time signal events.
Driven by CDC `EventBus` subscription — wakes instantly on write/delete events
with a 30-second fallback timer. Connect from Grafana or any SSE client for
live annotation updates.

```bash
curl -N http://localhost:8086/api/v1/annotations/stream
# data: {"text":"Signal event in _signals","time":1700000000000,"tags":["_signals"]}
```

### `GET /api/v1/cdc/stream`

Real-time Change Data Capture (CDC) event stream via Server-Sent Events.
Subscribes to the in-process `EventBus` and delivers mutation events
(writes, deletes, drops) as JSON-encoded SSE frames. This is the primary
integration point for external consumers that need real-time change
notifications without polling.

**Query Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `measurement` | string | Comma-separated list of measurements to filter on |
| `event_type` | string | Comma-separated event types: `point_written`, `series_deleted`, `measurement_dropped` |

**SSE Event Format:**

Each SSE frame includes an `event:` field matching the CDC event type,
enabling client-side event dispatch:

```bash
# Stream all events
curl -N http://localhost:8086/api/v1/cdc/stream

# Filter by measurement
curl -N "http://localhost:8086/api/v1/cdc/stream?measurement=cpu,mem"

# Filter by event type
curl -N "http://localhost:8086/api/v1/cdc/stream?event_type=point_written"

# Combined filters
curl -N "http://localhost:8086/api/v1/cdc/stream?measurement=cpu&event_type=point_written"
```

**Example SSE output:**

```
event: point_written
data: {"PointWritten":{"measurement":"cpu","tags":{"host":"srv1"},"fields":{"usage":{"F64":0.85}},"timestamp":1700000000000000000,"seq":42}}

event: series_deleted
data: {"SeriesDeleted":{"measurement":"mem","tags":{"host":"srv2"},"series_hash":12345,"seq":100}}

event: measurement_dropped
data: {"MeasurementDropped":{"measurement":"old_metric","seq":200}}
```

**JavaScript client example:**

```javascript
const source = new EventSource('/api/v1/cdc/stream?measurement=cpu');
source.addEventListener('point_written', (e) => {
  const event = JSON.parse(e.data);
  console.log('New point:', event.PointWritten);
});
source.addEventListener('series_deleted', (e) => {
  console.log('Series deleted:', JSON.parse(e.data));
});
```

### `GET /api/v1/dashboards/export`

Export all bundled Grafana dashboard JSON definitions.

**Response:** JSON array of complete Grafana dashboard model objects.

**CLI equivalent:**

```bash
chronixd --export-dashboards ./grafana/
```

---

## Management Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/measurements` | List all measurements with schema info |
| `GET` | `/api/v1/measurements/{name}/schema` | Get schema for a measurement |
| `DELETE` | `/api/v1/measurements/{name}` | Drop an entire measurement |
| `POST` | `/api/v1/delete` | Delete points matching predicates |
| `GET` | `/api/v1/rollups` | List rollup configurations |
| `POST` | `/api/v1/export/parquet` | Export data as Apache Parquet |
| `GET` | `/api/v1/connectors` | List active ingestion connectors |

### Delete Points

```bash
curl -X POST http://localhost:8086/api/v1/delete \
  -H 'Content-Type: application/json' \
  -d '{"measurement": "cpu", "tags": {"host": "old-srv"}, "start": 0, "end": 1700000000000000000}'
```

Response:

```json
{ "deleted": 42, "segments_skipped": 0, "complete": true }
```

| Field | Meaning |
|---|---|
| `deleted` | Number of series tombstoned |
| `segments_skipped` | Segments that could not be scanned and may still hold matching data |
| `complete` | `true` when `segments_skipped == 0` |

**A delete names a time interval.** `start` and `end` bound what is removed;
points outside them are untouched. Omitting them does *not* mean "delete this
series forever" — the upper bound is resolved per series to the newest
timestamp that series currently holds, so the delete covers the data that
exists and **a point written afterwards re-creates the series**. Re-ingesting
*into* an interval that is still tombstoned stays masked until compaction
materialises the delete, which is the same rule Prometheus and InfluxDB apply.

**A delete survives a restart.** Tombstones are persisted to the catalog
manifest and fsynced before the response is returned.

**A delete can be partial.** A segment that cannot be opened or read — a
corrupt file, or a segment tiered to remote storage — is skipped rather than
aborting the whole operation, so `deleted` alone cannot distinguish "nothing
matched" from "some data was never scanned".

Callers acting on an erasure obligation (GDPR requests, § 14a evidence
records) **must** treat a non-zero `segments_skipped` as a failed delete and
retry; a `chronix_delete_segments_skipped_total` counter is exported for
alerting. The gRPC `DeleteResponse` carries the same `segments_skipped` field,
and `/api/v1/delete_batch` reports it per item.

### Export Parquet

```bash
curl -X POST http://localhost:8086/api/v1/export/parquet \
  -H 'Content-Type: application/json' \
  -d '{"measurement": "cpu"}'
```

Response:

```json
{ "rows_written": 120000, "bytes_written": 1048576, "truncated": false }
```

| Field | Meaning |
|---|---|
| `rows_written` | Rows written to the file |
| `bytes_written` | Size of the finished file |
| `truncated` | `true` when a configured size budget cut the export short |

**Tag columns are dictionary-encoded** (`ParquetExportConfig::dictionary_tags`,
on by default). Tags are low-cardinality by construction, so this is the
dominant factor in export size when a gateway uploads a window to a fleet
backend.

**Exports can be size-bounded.** `ParquetExportConfig::max_bytes` stops the
writer before the file exceeds a budget and reports `truncated: true`, rather
than filling a device's flash. The bound is checked at row-group boundaries,
so the file may exceed it by up to one row group; the resulting file is always
a valid, readable Parquet file. Treat `truncated: true` as "this export is
incomplete", not as an error.

---

## Namespace Endpoints

Namespace management. Namespace definitions are durably persisted to disk and
survive process restarts.

Data isolation is separate and off by default: set `multi_tenancy = true` to
have every write stamped with the request's namespace and every read confined
to it. `X-Namespace` selects the namespace for HTTP; gRPC and Flight SQL use
`x-namespace` metadata. Without `multi_tenancy`, the header still selects
the namespace for authorization and rate limiting, but every request sees all
data. See [Security](/docs/security/#namespace-isolation).

**Authorization:** When a Cedar authorization engine is configured, the
namespace middleware checks that the authenticated principal is permitted
to access the target namespace (`Chronix::Namespace` resource type).
Policies can restrict users to specific namespaces:

```cedar
permit(
  principal in Chronix::Role::"team_a",
  action,
  resource == Chronix::Namespace::"team_a_ns"
);
```

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/namespaces` | Create a namespace |
| `GET` | `/api/v1/namespaces` | List all namespaces |
| `GET` | `/api/v1/namespaces/{name}` | Get namespace details |
| `DELETE` | `/api/v1/namespaces/{name}` | Delete a namespace |
| `GET` | `/api/v1/namespaces/{name}/usage` | Get resource usage vs quotas |

### Create Namespace

```bash
curl -X POST http://localhost:8086/api/v1/namespaces \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "production",
    "max_series_count": 1000000,
    "max_ingestion_rate": 50000,
    "max_storage_bytes": 107374182400,
    "max_measurements": 500,
    "max_models": 100
  }'
```

### Check Usage

```bash
curl http://localhost:8086/api/v1/namespaces/production/usage
```

---

## Admin — Cluster Management

All admin endpoints require the `Chronix::Action::"Admin"` Cedar action
when an authorization engine is configured. The authorization middleware
checks against the synthetic `Chronix::Measurement::"__system__"` resource.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/admin/nodes` | Register a cluster node |
| `GET` | `/api/v1/admin/nodes` | List all cluster nodes with status |
| `DELETE` | `/api/v1/admin/nodes` | Deregister a cluster node |
| `POST` | `/api/v1/admin/heartbeat` | Node heartbeat (keep-alive) |
| `POST` | `/api/v1/admin/regions` | Create a data region |
| `PUT` | `/api/v1/admin/regions/{id}/state` | Update region state |
| `GET` | `/api/v1/admin/routing` | Get the cluster routing table |
| `GET` | `/api/v1/admin/health` | Cluster-wide health status |
| `POST` | `/api/v1/admin/rebalance` | Trigger cluster rebalancing |
| `POST` | `/api/v1/admin/nodes/{id}/decommission` | Gracefully decommission a node |
| `PUT`  | `/api/v1/admin/log-level` | Change server log level at runtime |

### Register Node

```bash
curl -X POST http://localhost:8086/api/v1/admin/nodes \
  -H 'Content-Type: application/json' \
  -d '{"node_id": 10, "addr": "10.0.0.10:8086", "mode": "data"}'
```

### Cluster Health

```bash
curl http://localhost:8086/api/v1/admin/health
# {"status":"healthy","total_nodes":3,"healthy_nodes":3,...}
```

### Runtime Log-Level

Change the server's tracing log level at runtime without restart. Uses a
`reload::Layer` so the change takes effect immediately.

```bash
# Set log level to debug
curl -X PUT http://localhost:8086/api/v1/admin/log-level \
  -H 'Content-Type: application/json' \
  -d '{"level": "debug"}'
# {"previous": "info", "current": "debug"}

# Restore to info
curl -X PUT http://localhost:8086/api/v1/admin/log-level \
  -d '{"level": "info"}'
```

| Field | Type | Description |
|-------|------|-------------|
| `level` | string | One of `trace`, `debug`, `info`, `warn`, `error` |

**Response:** `200 OK` with `{"previous": "<old>", "current": "<new>"}`
**Error:** `400 Bad Request` for invalid level strings.

### Backup & Restore

#### Create Backup

```
POST /api/v1/admin/backup
```

**Request Body:**
| Field | Type | Description |
|-------|------|-------------|
| `target_dir` | string | Absolute path to the backup target directory |

**Response:** `200 OK` — Returns `BackupManifest` with `version`, `created_at`, `wal_sequence`, `file_count`, `total_bytes`.

#### Restore from Backup

```
POST /api/v1/admin/restore
```

**Request Body:**
| Field | Type | Description |
|-------|------|-------------|
| `backup_dir` | string | Absolute path to the backup source directory |
| `target_dir` | string | Absolute path to the restore target directory |

**Response:** `200 OK` — Returns `BackupManifest`.

---

## Admin — Analytics Model Management

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/admin/analytics/models` | List all trained models |
| `GET` | `/api/v1/admin/analytics/models/{measurement}/{name}` | Get model details |
| `DELETE` | `/api/v1/admin/analytics/models/{measurement}/{name}` | Delete a model |
| `POST` | `/api/v1/admin/analytics/retrain` | Trigger model re-training |

### List Models

```bash
curl http://localhost:8086/api/v1/admin/analytics/models
# {"models": [{"name":"ses_model","measurement":"cpu","model_type":"Ses",...}]}
```

### Retrain

```bash
curl -X POST http://localhost:8086/api/v1/admin/analytics/retrain \
  -H 'Content-Type: application/json' \
  -d '{"measurement": "cpu"}'
# {"accepted": true, "models_queued": 2}
```

---

## Admin — Chaos Injection

Controlled fault injection for resilience testing. The chaos agent must
be enabled via configuration.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/admin/chaos/inject` | Inject a fault |
| `GET` | `/api/v1/admin/chaos` | List active injections |
| `DELETE` | `/api/v1/admin/chaos` | Clear all injections |
| `DELETE` | `/api/v1/admin/chaos/{id}` | Clear a specific injection |

### Available Fault Types

| Fault | Parameters | Description |
|-------|-----------|-------------|
| `DiskFull` | — | Simulate disk full on the node |
| `LatencySpike` | `delay` (Duration) | Add artificial latency to all operations |
| `SlowDisk` | `latency` (Duration) | Add latency to disk I/O |
| `WriteDropper` | `drop_ratio` (0.0–1.0) | Drop a percentage of writes |
| `NetworkPartition` | `isolated_nodes` (Vec) | Isolate specific nodes |
| `KillNode` | `delay` (Duration) | Kill the node after a delay |
| `ReadCorruption` | `corruption_ratio` (0.0–1.0) | Corrupt a percentage of reads |

### Inject Fault

```bash
curl -X POST http://localhost:8086/api/v1/admin/chaos/inject \
  -H 'Content-Type: application/json' \
  -d '{"fault": "DiskFull", "duration_secs": 30, "description": "Test disk full handling"}'
# {"injection_id": 1, "fault": "DiskFull", "duration_secs": 30}
```

### Inject Latency Spike

```bash
curl -X POST http://localhost:8086/api/v1/admin/chaos/inject \
  -H 'Content-Type: application/json' \
  -d '{
    "fault": {"LatencySpike": {"delay": {"secs": 1, "nanos": 0}}},
    "duration_secs": 60,
    "description": "Simulate slow network"
  }'
```

### List Active Faults

```bash
curl http://localhost:8086/api/v1/admin/chaos
# [{"id": 1, "fault": "DiskFull", "description": "Test", "remaining_secs": 25.3}]
```

### Clear Fault

```bash
curl -X DELETE http://localhost:8086/api/v1/admin/chaos/1
# 204 No Content
```

---

## Authentication

When authentication is enabled, API keys are managed via these endpoints.
All other endpoints require a valid `Authorization: Bearer <key>` header.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/auth/keys` | Create a new API key |
| `GET` | `/api/v1/auth/keys` | List all API keys |
| `DELETE` | `/api/v1/auth/keys/{name}` | Revoke an API key |

### Create Key

```bash
curl -X POST http://localhost:8086/api/v1/auth/keys \
  -H 'Content-Type: application/json' \
  -d '{"name": "grafana-reader", "expires_in_secs": 86400}'
```

---

## gRPC API

The gRPC service runs on a separate port (default `4243`). The proto
definition is in `crates/chronixd/proto/chronix.proto`.

### `ChronixService`

| Method | Type | Description |
|--------|------|-------------|
| `Write` | Unary | Write a batch of points |
| `StreamWrite` | Client-streaming | Stream points with server-side batching and dedup |
| `Query` | Server-streaming | Query data and stream Arrow-format results |
| `GetSchema` | Unary | Get measurement schema |
| `ListMeasurements` | Unary | List all measurements with schemas |
| `Delete` | Unary | Delete points matching predicates |
| `DropMeasurement` | Unary | Drop an entire measurement |
| `ServerInfo` | Unary | Server metadata (version, uptime, stats) |
| `ExecuteSql` | Unary | Execute SQL queries via DataFusion (streaming internally) |

**Write timeout:** gRPC `Write` and `StreamWrite` enforce the same
`write_timeout_secs` deadline. Timeouts surface as gRPC status
`DEADLINE_EXCEEDED`.

### Example (grpcurl)

```bash
grpcurl -plaintext -d '{"measurement":"cpu","points":[...]}' \
  localhost:4243 chronix.ChronixService/Write
```

---

## Flight SQL API

Arrow Flight SQL service runs on a separate port (default `4244`).
Compatible with any Flight SQL client (e.g., Grafana Flight SQL plugin,
JDBC/ODBC drivers, DBeaver).

### Query Resource Limits

Flight SQL queries enforce the same resource limits as the REST and gRPC
interfaces:

| Setting | Default | Description |
|---------|---------|-------------|
| `sql_query_timeout_secs` | `30` | Maximum seconds for SQL query execution including result streaming |
| `sql_max_rows` | `100000` | Maximum rows returned by a single SQL query |
| `prom_series_limit` | `10000` | Maximum label-sets returned by `/api/v1/prom/series` |

Queries exceeding the timeout are cancelled. Row limits are enforced at the
batch level — once the cumulative row count reaches the limit, the final batch
is truncated via `batch.slice()` and no further batches are sent. This avoids
materializing the entire result set in memory.

### Supported Commands

| Command | Description |
|---------|-------------|
| `CommandStatementQuery` | Execute a SQL query and retrieve results |
| `CommandStatementUpdate` | Bulk write via DoPut (Arrow RecordBatch) |
| `CommandGetCatalogs` | List catalogs (returns `"chronix"`) |
| `CommandGetTableTypes` | List table types (returns `"TABLE"`) |
| `CommandGetTables` | List measurements as tables |

### Example (Python)

```python
from adbc_driver_flightsql import dbapi

conn = dbapi.connect("grpc://localhost:4244")
cursor = conn.cursor()
cursor.execute("SELECT * FROM cpu WHERE host = 'srv1' LIMIT 10")
print(cursor.fetchall())
```

---

## Embedded Rust API

Use Chronix as an embedded library without running a server.

```rust
use chronix::prelude::*;

// Open database
let config = ChronixConfig::builder()
    .data_dir("/var/lib/chronix")
    .build()?;
let db = Chronix::open(config)?;

// Write
let key = SeriesKey::new("cpu", [("host", "srv1")])?;
let fields = [("usage", FieldValue::F64(72.5))].into();
db.insert(Point::new(key, fields, timestamp_ns())?)?;

// Batch write (single frozen check, single atomic size update)
let points = vec![point1, point2, point3];
db.insert_batch(points)?;

// Query
let plan = db.query()
    .measurement("cpu")
    .tag("host", "srv1")
    .field("usage")
    .time_range(start_ns, end_ns)
    .build()?;
let batch: RecordBatch = db.execute(&plan)?;

// SQL
let ctx = chronix::sql::create_session_context(Arc::new(db));
let df = ctx.sql("SELECT avg(usage) FROM cpu GROUP BY host").await?;

// Analytics
use chronix::chronix_analytics::forecast::{ForecastModel, SesModel};
let mut model = SesModel::new(Some(0.3));
model.fit(&timestamps, &values)?;
let forecast = model.predict(10)?;
```

### Key Types

| Type | Description |
|------|-------------|
| `Chronix` | Main database handle — thread-safe, `Arc`-shareable |
| `ChronixConfig` | Database configuration (builder pattern) |
| `Point` | A single write point with series key, fields, timestamp |
| `SeriesKey` | Measurement name + tag set — identifies a unique series |
| `FieldValue` | Enum: `F64`, `I64`, `U64`, `Bool`, `String` |
| `QueryBuilder` | Fluent query construction |
| `QueryPlan` | Compiled query plan for execution |

---

## Error Responses

All REST endpoints return errors in a consistent format:

```json
{
  "error": "NOT_FOUND",
  "message": "measurement 'nonexistent' not found"
}
```

| HTTP Status | Error Code | Description |
|-------------|-----------|-------------|
| `400` | `BAD_REQUEST` | Invalid request body or parameters |
| `401` | `UNAUTHORIZED` | Missing or invalid authentication |
| `403` | `FORBIDDEN` | Cedar authorization denied (admin or namespace) |
| `404` | `NOT_FOUND` | Resource not found |
| `429` | `RATE_LIMITED` | Namespace quota exceeded |
| `500` | `INTERNAL` | Server-side error |

---

## See Also

- [Architecture Guide](@/reference/_index.md) — System design and data flow
- [Operations Guide](@/docs/operations.md) — Deployment and monitoring
- [Analytics Guide](@/docs/analytics.md) — Forecasting, anomaly detection
- [Cluster Guide](@/docs/cluster.md) — Distributed setup and scaling
- [Security Guide](@/docs/security.md) — Authentication and authorization
- [Performance Guide](@/docs/performance.md) — Tuning and benchmarks
