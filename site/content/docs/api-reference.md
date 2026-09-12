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

- [Errors](#errors)
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
- [Authentication](#authentication)
- [gRPC API](#grpc-api)
- [Flight SQL API](#flight-sql-api)
- [Embedded Rust API](#embedded-rust-api)

---

## Errors

Every failing request answers the same JSON envelope, whatever produced it —
a handler, a body that will not deserialise, a path no route matches, a method
no route accepts:

```json
{"error": "measurement not found: nope", "code": "NOT_FOUND"}
```

`code` is the machine-readable half; branch on it rather than on the message.

| Status | `code` | Means |
|--------|--------|-------|
| `400` | `BAD_REQUEST` | Malformed request — bad JSON, an unparseable parameter, invalid SQL |
| `400` | `CARDINALITY_EXCEEDED` | The write would exceed `max_series_cardinality` |
| `400` | `SCHEMA_ERROR` | A field's type conflicts with the measurement's schema |
| `400` | `PARTIAL_WRITE` | Some points of the batch were rejected; the body names them |
| `400` | `FUTURE_TIMESTAMP` | A point's timestamp is beyond `future_write_tolerance` — usually a device clock |
| `400` | `INVALID_POINT` | A measurement, tag or field name the engine refuses |
| `400` | `SQL_ERROR`, `PROMQL_ERROR` | The query did not plan |
| `401` | `UNAUTHORIZED` | Missing or invalid credentials |
| `403` | `FORBIDDEN` | Authenticated, but not permitted |
| `404` | `NOT_FOUND` | No such measurement, trigger, namespace — or no such route |
| `405` | `METHOD_NOT_ALLOWED` | The path exists; this method does not |
| `409` | `CONFLICT` | The resource already exists |
| `413` | `PAYLOAD_TOO_LARGE` | Body over `max_body_size` |
| `415` | `UNSUPPORTED_MEDIA_TYPE` | A JSON endpoint without `Content-Type: application/json` |
| `422` | `INVALID_BODY` | Valid JSON, wrong shape — a required field is missing |
| `429` | `RATE_LIMITED` | Rate limit exceeded |
| `500` | `INTERNAL_ERROR`, `DATABASE_ERROR` | chronix's own fault. Logged in full server-side; the response says only that it happened |
| `503` | `BACKPRESSURE` | The memtable is at capacity; the flush that clears it is already running. Carries `Retry-After: 1` |
| `503` | `OVERLOADED` | A condition that needs an operator — a WAL poisoned by a failed `fsync`. Carries `Retry-After: 60` |
| `503` | `DATABASE_CLOSED` | The server is shutting down |
| `504` | `WRITE_TIMEOUT` | The write did not finish within `write_timeout`. **Its outcome is unknown** — see below |
| `504` | `QUERY_TIMEOUT` | The read exceeded `sql_query_timeout_secs` or `query_timeout_secs` |
| `507` | `STORAGE_FULL` | No space left on the data volume. Carries `Retry-After: 5` |

The **PromQL endpoints are the one exception**, deliberately: they answer
Prometheus's own error shape, `{"status":"error","errorType":…,"error":…}`,
because every Prometheus client branches on that instead.

### Which errors are redacted

A `5xx` caused by chronix's own machinery is redacted to
`"DATABASE_ERROR: an internal error occurred"`, with the detail in the server
log. A `5xx` that describes the **deployment** is shown in full — a full disk,
a full memtable, a poisoned WAL, a query that ran out of time — because each
has a remedy and none discloses anything.

### `504 WRITE_TIMEOUT` means *unknown*, not *no*

`write_timeout` bounds how long the server waits, not how long the write
takes: a durable write is not abandoned half-way, so when the deadline fires
the write is still running and will probably land.

**Retry it.** A point is identified by its series and its timestamp, so
writing it twice stores it once. Concurrent writes are bounded by admission
control, which answers `503 BACKPRESSURE`.

---

## Health & Metrics

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness probe. Returns `200 OK` when the process is running. |
| `GET` | `/ready` | Readiness probe. `200` when the database is open **and accepting writes**; `503` with `{"ready": false, "reason": …}` when it is not. Point a Kubernetes `readinessProbe` here. |
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
- `fields` (object, required): Numeric/boolean/string field values, or
  `{"decimal": "…"}` for an exact one — see below.
- `timestamp` (i64, optional): Nanosecond Unix epoch. Defaults to server time.

#### Exact decimal fields

A JSON number is parsed as a `double` by `serde_json` and by every client
library there is, so writing `1234.5678` loses the digits before the server
ever sees them. An exact field therefore travels as **digits in a string**:

```json
{
  "measurement": "meter",
  "tags": {"device": "main"},
  "fields": {"z1nb_q": {"decimal": "1234.5678"}},
  "timestamp": 1700000000000000000
}
```

A JSON *number* inside the wrapper is refused, not converted.

Reading the point back gives the same shape, so a point you read is a point
you can write. A SQL result cell is a plain string of digits, with
`decimal(38, 4)` in the column metadata beside it.

The column's **scale is fixed when the column is created**, and a value
needing more fractional digits is refused, naming both. Declare it first when
the first value might not carry the digits you mean to keep:

```bash
curl -XPOST 'http://localhost:8086/api/v1/measurements/meter/schema/fields' \
  -H 'Content-Type: application/json' \
  -d '{"name":"z1nb_q","type":"decimal","scale":4}'
```

See [Data Model](/docs/data-model/#exact-decimals-for-money-and-meters).

**Query parameters:** `backfill=true` writes points **outside** the
out-of-order window — see [Backfilling history](#backfilling-history).

**Response:** `204 No Content`

A malformed body answers `400` naming the field and its position in the
batch — `invalid write batch: points[1]: unknown field \`measurment\``.

### `POST /write`, `POST /api/v2/write`, `POST /api/v1/write/influx`

Write data in [InfluxDB Line Protocol](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/) format.

**Content-Type:** `text/plain`. Bodies may be **gzipped**
(`Content-Encoding: gzip`), which is what Telegraf sends by default.

```
cpu,host=srv1,region=us-east usage=72.5,count=42i 1700000000000000000
```

**Query parameters:** `precision=ns|us|ms|s` sets the unit of every
timestamp in the body (default `ns`); `backfill=true` writes outside the
out-of-order window. `db`, `bucket`, `org` and `rp` are accepted and ignored —
chronix is one database per process and scopes by the `X-Namespace` header.

### Backfilling history

Live ingestion is held to ±`ooo_shard_tolerance` shards of the newest admitted
write. A point older than that is refused, and the refusal names the timestamp,
the window and the remedy:

```json
{"error": "partial write: 0 accepted, 1 rejected: Memtable error: timestamp 1600000000000000000 is outside the out-of-order window [1788613200000000000, 1788631200000000000] (+/-2 shard(s) of 3600s around the newest admitted write); import history with a backfill write instead",
 "code": "PARTIAL_WRITE"}
```

It is an explicit opt-in, not an automatic fallback: the window is what bounds
the number of open memtables.

```bash
# JSON, line protocol, remote write and OTLP all take the same parameter.
curl -X POST 'localhost:8086/api/v1/write?backfill=true' \
  -H 'Content-Type: application/json' \
  -d '{"measurement":"cpu","fields":{"usage":1.0},"timestamp":1600000000000000000}'
```

| Surface | How to ask |
|---|---|
| `POST /api/v1/write` | `?backfill=true` |
| `POST /write`, `/api/v2/write`, `/api/v1/write/influx` | `?backfill=true` |
| `POST /api/v1/prom/write` | `?backfill=true` |
| `POST /v1/metrics`, `/api/v1/otlp/metrics` | `?backfill=true` |
| gRPC `Write` | `WriteRequest.backfill = true` |
| Arrow Flight `DoPut` | `FlightData.app_metadata` = `{"backfill": true}` |
| Python SDK | `client.write(points, backfill=True)` |

Admission, the cardinality budget, the schema and the future-timestamp bound
are unchanged.

Flight SQL has no field for a write mode, so the flag travels in
`app_metadata`. Put it on any message of the stream; the first non-empty one
decides. An unrecognised key is **refused**, not ignored.

A backfill below a rollup's watermark marks those buckets for
re-materialisation ([Data Model](/docs/data-model/)).

**Response:** `204 No Content` when every line was stored.

If some lines fail to parse, the good ones are **still stored** and the
response is `400` with the counts:

```json
{"error": "partial write: line 2: …", "written": 2, "rejected": 1}
```

That is InfluxDB's behaviour, and the wording matters: Telegraf only treats
a failure as permanent when it recognises the message, and retries anything
else for ever — so failing the whole batch means the good lines never land.
A body in which nothing parses is a plain `400` naming the first failure.

#### Escape semantics

Chronix implements the Line Protocol escape rules in full:

| Section | Escapable | Notes |
|---|---|---|
| Measurement | `,` ` ` | |
| Tag key / tag value | `,` `=` ` ` | `"` is an **ordinary character** here |
| Field key | `,` `=` ` ` | |
| String field value | `"` `\` | |

#### Field value suffixes

| Written | Type |
|---|---|
| `72.5` | float (`f64`) — the default for a bare number |
| `42i` | signed integer |
| `42u` | unsigned integer |
| `t` / `true` / `f` / `false` | boolean |
| `"text"` | string |
| `1234.5678d` | **exact decimal** — a Chronix extension |

`d` is a Chronix extension — Influx has no exact type, so no line written for
Influx changes meaning here. The digits are parsed as digits.

```text
meter,device=main z1nb_q=1234.5678d 1700000000000000000
```

Two consequences worth calling out:

- **`\\` is a literal backslash.** `path="C:\\"` is a value of `C:\` followed
  by a *real* closing quote. Escape detection counts consecutive backslashes,
  so an escaped backslash before a delimiter does not swallow the rest of
  the line.
- **`=` is allowed in tag keys and values** (escaped, per the spec). Series
  identity uses reserved control-character separators rather than `=`, so
  lines InfluxDB accepts are accepted here. See
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

### `POST /api/v1/chronix/query`

Query time-series data using the native query plan format.

> Moved from `/api/v1/query`, which now serves the **Prometheus** instant
> query — that is the path every Prometheus client derives from a base URL,
> and a server that answers something else there cannot be used by one.
> `/api/v1/chronix/query/explain` and `/api/v1/chronix/sql` moved with it.

**Request Body:**

```json
{
  "measurement": "cpu",
  "range": {"start": 1700000000000000000, "end": 1700100000000000000},
  "tags": {"host": "srv1"},
  "fields": ["usage"],
  "limit": 100,
  "offset": 0
}
```

The time bounds live under `range`, and every key is checked: an unknown one
is a `400` rather than a field quietly dropped, because a dropped `start`
answers the whole measurement as though it had understood.

`limit` is bounded by the server's `sql_max_rows`. A larger one is refused
before the scan; naming none is refused after it if the result would exceed the
ceiling. The response is a bare array with nowhere to carry a `truncated` flag,
so this endpoint refuses rather than returning a prefix.

**Response** — a JSON **array** of rows. Each row separates the tags that
identify the series from the fields that carry values, and a string *field*
stays a field:

```json
[
  {
    "timestamp": 1700000000000000000,
    "tags": {"host": "srv1"},
    "fields": {"usage": 72.5}
  }
]
```

### `POST /api/v1/chronix/sql`

Execute a SQL query via DataFusion.

**Request Body:**

```json
{"query": "SELECT * FROM cpu WHERE host = 'srv1' ORDER BY _time DESC LIMIT 10"}
```

**Response** — column metadata plus **positional** rows, so a client can build
a frame without re-deriving the schema:

```json
{
  "columns": [{"name": "_time", "data_type": "Timestamp(ns)"},
              {"name": "usage", "data_type": "Float64"}],
  "rows": [[1700000000000000000, 72.5]],
  "row_count": 1,
  "truncated": false
}
```

`truncated` is `true` when `sql_max_rows` cut the answer short, and is always
present — an aggregate over a truncated scan is a wrong number, not a partial
one. Add a `LIMIT`, narrow the range, or raise `server.sql_max_rows`.

Arrow Flight SQL has no field for the flag, so it **refuses** a result over the
ceiling with `RESOURCE_EXHAUSTED`.

**How a value is encoded.** Every Arrow type a query can produce has a JSON
form, and the column's `data_type` says how to read it back:

| Arrow type | JSON |
|---|---|
| `Int*`, `UInt*` | number — exact, including past 2⁵³ |
| `Float*` | number; `NaN` and the infinities become `null` (JSON has no literal) |
| `Decimal*` | **string of digits** (`"30.10"`) — exact |
| `Utf8`, `LargeUtf8`, `Utf8View` | string |
| `Binary`, `FixedSizeBinary` | base64 string |
| `Timestamp`, `Date32`, `Date64`, `Time32`, `Time64`, `Duration` | integer, in the unit `data_type` names — `Date32` is days, `Timestamp(ns)` is nanoseconds |
| `Interval` | object: `months`, `days`, `nanoseconds` |
| `List`, `FixedSizeList` | array |
| `Struct` | object keyed by field name |
| `Map` | array of `{"key": …, "value": …}` |
| `Null` | `null` |

Read over Arrow Flight SQL to keep the Arrow types themselves, `NaN` included.

**When a query fails**, `400` carries the reason for anything the caller can
fix — a division by zero, an impossible cast, an unknown time zone, an
unsupported `date_trunc` granularity. `500` means a fault in the server.

**Time predicates.** The timestamp column is `_time`, typed
`TIMESTAMP(nanosecond)`. Three forms compare against it:

```sql
SELECT * FROM cpu WHERE _time >= 1700000000000000000;
SELECT * FROM cpu WHERE _time >= timestamp '2023-11-14T22:13:20Z';
SELECT * FROM cpu WHERE _time > now() - INTERVAL '1 hour';
```

An integer literal is read **in the column's unit**, so the epoch
nanoseconds the write API took compare equal to themselves. `BETWEEN` and
`IN` accept them too.

All three forms **prune** — the bounds reach the scan, so only overlapping
segments are read. `EXPLAIN` shows what the scan will read:

```sql
EXPLAIN SELECT * FROM cpu WHERE _time >= 1700000000000000000;
```

```text
ChronixExec: measurement=cpu, time=[1700000000000000000..9223372036854775807], filters=0, limit=None
```

A full `[-9223372036854775808..9223372036854775807]` range means the filter
did not reach the scan and the whole measurement is being read.

**A decimal literal is a decimal.** `0.05` is `DECIMAL`, not `DOUBLE`, as in
PostgreSQL — so comparison, `BETWEEN`, `IN`, `GROUP BY` **and** arithmetic on
an exact column all stay exact.

A `DOUBLE` column is unaffected: `Float64 op Decimal128` yields `Float64`, so
`usage * 1.5` is a float. The exponent form is not an escape hatch (`1.5e0`
is a decimal too) — write `CAST(1.5 AS DOUBLE)` for a literal that must be a
float whatever it meets.

**Catalog.** `SHOW TABLES`, `SHOW COLUMNS FROM <measurement>`, `DESCRIBE
<measurement>` and the `information_schema` views list the measurements the
request's namespace holds data for.

**What is refused.** The endpoint is read-only: DDL, DML, `COPY` and
statements such as `SET` and `PREPARE` are rejected before execution, so a
refused statement has no side effect. `EXPLAIN` and `EXPLAIN ANALYZE` are
permitted; the plan they wrap is held to the same rules, so
`EXPLAIN ANALYZE INSERT …` is refused.

### SQL Trigger WHEN Clause Syntax

SQL-based signal triggers support compound conditions with `AND`, `OR`, and
parenthesized grouping in `WHEN` clauses:

```sql
-- Simple condition
CREATE TRIGGER high_cpu ON cpu_usage
  WHEN value > 90
  DELIVER webhook('https://alerts.example.com');

-- Compound AND/OR with parentheses
CREATE TRIGGER complex_alert ON cpu_usage
  WHEN (value > 90 AND host = 'prod-srv1') OR (value > 95)
  DELIVER webhook('https://alerts.example.com')
  COOLDOWN INTERVAL '5m';
```

The parser supports arbitrary nesting of `AND`/`OR` operators with
explicit parentheses for precedence control. Trigger names are validated
against SQL injection patterns — only alphanumeric characters and
underscores are accepted.

**A quoted right-hand side compares a tag**, and an unquoted one compares a
numeric field:

```sql
-- Only the production hosts, and never the canary.
CREATE TRIGGER prod_cpu ON cpu_usage
  WHEN value > 90 AND env = 'production' AND role <> 'canary'
  DELIVER webhook('https://alerts.example.com');
```

Without one, a trigger fires for **every** series of its measurement. Only
`=` and `<>` apply to a tag.

The full grammar:

```text
CREATE TRIGGER <name> ON <measurement> WHEN <condition>
  [DELIVER <channel>[, <channel>…]] [COOLDOWN INTERVAL '<duration>']
SHOW TRIGGERS
DROP TRIGGER <name>
ALTER TRIGGER <name> ENABLE | DISABLE
```

There is no `WHERE`, `SEVERITY` or `ACTION` clause.

### `DELIVER` channels

Two: `log` and `webhook('https://…')`. Anything else — `nats(…)`, `mqtt(…)`,
`DELIVER TO log` — is a parse error naming the channels that exist.

Each webhook URL is its own channel, so two triggers can deliver to two
endpoints. Every delivery is a CloudEvents envelope, signed per the
[Standard Webhooks](https://www.standardwebhooks.com) `v1` scheme with
`triggers.webhook_signing_secrets`; with none configured, `DELIVER webhook(…)`
is refused at creation. The URL must be `https`, carry no userinfo, and name
neither an internal host nor a non-routable address — see
[Webhook URL SSRF Protection](/docs/security/#webhook-url-ssrf-protection).

**Delivery does not block evaluation.** Each channel owns a bounded queue and
a worker, so an unreachable webhook delays only itself. A queue that fills
drops the oldest waiting signal and counts it in
`chronix_signal_delivery_dropped_total{channel}` — a non-zero rate there means
alerts are being lost. Shutdown waits up to five seconds for the queues to
drain.

**A trigger may watch a rollup tier.** A rollup's target measurement publishes
the same change events any write does, and the `WHEN` clause names an
aggregate column as readily as a raw field:

```sql
CREATE TRIGGER hot_avg ON cpu_1m WHEN usage_avg > 90 DELIVER log;
```

## Trigger Endpoints

Served only when the server configuration has a `[triggers]` section;
otherwise they answer `404`. Every statement is scoped to the caller's
namespace: a trigger sees only its own tenant's series, two tenants may use the
same trigger name, and neither can list, drop or disable the other's.

### `POST /api/v1/triggers`

Runs one trigger DSL statement.

```bash
curl -X POST localhost:8080/api/v1/triggers \
  -H 'Content-Type: application/json' \
  -d '{"query": "CREATE TRIGGER hot_cpu ON cpu WHEN value > 90 DELIVER log"}'
```

`400` with the parser's message when the statement is refused — including a
`DELIVER webhook(…)` with no signing secret configured.

### `GET /api/v1/triggers`

The caller's triggers, under the names they were created with.

```json
{
  "triggers": [
    { "name": "hot_cpu", "measurement": "cpu", "enabled": true, "delivery": ["log"] }
  ]
}
```

### `GET /api/v1/triggers/{name}`

One trigger, without listing them all.

```json
{ "name": "hot_cpu", "measurement": "cpu", "enabled": true, "delivery": ["log"] }
```

A name the caller does not own is a `404`, which is also the answer when
another tenant owns it — a tenant must not be able to learn that.

### `DELETE /api/v1/triggers/{name}`

Drops one of the caller's triggers. A name the caller does not own is a `400`,
which is also the answer when another tenant owns it.

### `GET /api/v1/signals`

Recently fired signals for the caller, newest last.

```json
[
  {
    "event_id": "9f1c…",
    "trigger_name": "hot_cpu",
    "measurement": "cpu",
    "tags": { "host": "h1" },
    "timestamp": 1609459200000000000,
    "severity": "warning",
    "value": 99.0
  }
]
```

---

## PromQL Endpoints

Prometheus-compatible query API, served at the paths a Prometheus client
derives from a base URL. Point a Grafana **Prometheus** data source at
`http://chronix.example.com:8086` and nothing else needs configuring.

| Method | Path | Description |
|--------|------|-------------|
| `GET/POST` | `/api/v1/query` | Instant query |
| `GET/POST` | `/api/v1/query_range` | Range query |
| `GET/POST` | `/api/v1/labels` | List all label names |
| `GET/POST` | `/api/v1/label/{name}/values` | Values for a given label |
| `GET/POST` | `/api/v1/series` | Find series matching label matchers |
| `GET` | `/api/v1/metadata` | Metric metadata (types, help text) |
| `GET` | `/api/v1/status/buildinfo` | Version, probed when a data source is saved |
| `GET` | `/api/v1/rules`, `/api/v1/alerts`, `/api/v1/query_exemplars` | Empty but well-formed — chronix has no rules or exemplars |

`/api/v1/prom/*` is an alias for every query and discovery path, for
deployments that name it explicitly. Chronix's own JSON query API lives
under `/api/v1/chronix/*`, because `/api/v1/query` is where every Prometheus
client looks.

`POST` bodies are `application/x-www-form-urlencoded`, which is what Grafana
sends by default. `time`, `start` and `end` accept **RFC 3339 or a Unix
timestamp**; `step` accepts a **duration** (`15s`, `1m30s`) or a bare number
of seconds. An unparseable value is a 400 rather than a silent default.

Errors carry Prometheus's status codes, because clients branch on them:
`400` for `bad_data`, `422` for `execution`, `503` for `timeout`. The body is
`{"status":"error","errorType":…,"error":…}` — not the envelope the rest of the
API uses, for the same reason. The discovery endpoints answer in the same
envelope, which is what Grafana branches on.

### `limit`, and truncation

`/query`, `/query_range`, `/series`, `/labels` and `/label/{name}/values`
accept **`limit`**: a non-negative integer bounding the number of results,
where `0` (or absent) means no limit. On the query endpoints it bounds the
number of **series**, as upstream does, and leaves a scalar or a string alone.

Two ceilings can cut an answer — the request's `limit` and the server's
`prom_series_limit` — and either way the response says so, in the field
Prometheus uses:

```json
{"status": "success",
 "data": [{"__name__": "cpu_usage", "host": "a"}],
 "warnings": ["results truncated due to limit"]}
```

`warnings` is **absent** when nothing was cut, so its presence carries
information. A `limit` above `prom_series_limit` does not raise it: an
operator's ceiling is not a client's to lift.

A **parse** error is `bad_data`, and that includes the two things upstream
also catches in its parser: an unknown function name, and a selector whose
every matcher would be satisfied by an absent label (`{}`, `{host=~".*"}`).
`{host="a"}` is legal and reaches every metric carrying that label.

`/query` and `/query_range` answer `{"status":"success","data":{"resultType":…,"result":…}}`.
`/labels`, `/label/{name}/values` and `/series` answer a **bare array** under
`data`, as Prometheus does, with label names and values **sorted**.

All three accept `start` and `end` (defaulting to the last hour) and
repeated **`match[]`** series selectors. Every matcher in a selector is
applied, not only `__name__`:

```
GET /api/v1/label/dc/values?match[]={__name__="cpu_usage",host="a"}
→ {"status":"success","data":["eu"]}
```

Repeated `match[]` parameters are a **union**, as in Prometheus. A selector
that constrains no metric name is skipped rather than treated as
"everything", since it cannot be answered without scanning every measurement —
but a `__name__` **regex** does constrain it, so `match[]={__name__=~".+"}`
is answered here exactly as it is by `/query`.

`/label/__name__/values` lists the **metric names** holding data in the
window, and `/metadata` describes each one — the two calls Grafana makes to
populate a metric browser. Every name they offer is a selector that answers.

### Metric names

Chronix stores a *measurement* with many *fields*; PromQL addresses a
*metric*, which carries one value per sample. A metric is one
`(measurement, field)` pair:

| Written as | PromQL metric |
|------------|---------------|
| `cpu,host=a usage=42,load=0.7` | `cpu_usage`, `cpu_load` |
| `temperature,room=hall value=21` | `temperature` |
| Prometheus remote write / OTLP `up{job="api"}` | `up` |

The rule is `<measurement>_<field>`, except that a field named `value` gives
the measurement name alone — which is how the Prometheus remote-write and
OTLP paths store a sample, so a scraped metric round-trips under its own
name.

Two consequences worth knowing:

- **A bare measurement name is not a metric.** `cpu` selects nothing when the
  measurement's fields are `usage` and `load`; `cpu_usage` selects the series.
  Ask `/api/v1/label/__name__/values` — or Grafana's metric browser — for the
  names that exist.
- **Adding a field never renames an existing metric.** A name depends only on
  its own `(measurement, field)` pair, so writing `cpu,host=a temp=61` for the
  first time leaves `cpu_usage` and `cpu_load` exactly where they were.

A vector may not hold two series with the same label set, and a query that
would produce one is an error — `vector cannot contain metrics with the same
labelset`, as in Prometheus. `rate({__name__=~"cpu.+"}[5m])` is the usual way
to hit it: `rate` drops `__name__`, and `cpu_usage` and `cpu_load` then have
identical labels. Aggregate or select one metric instead.

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

**Modifiers.** `offset 5m` shifts back, `offset -5m` shifts forward, and
`@ <unix seconds>` / `@ start()` / `@ end()` pin an evaluation instant — `@`
replaces the evaluation time and the offset is then subtracted. Both attach to
a **selector or a subquery** and to nothing else, so `sum(m) offset 5m` is a
parse error naming the rule.

**Subqueries.** `expr[5m:1m]` evaluates `expr` at each point of an absolute
step grid; `expr[5m:]` uses the engine's 1-minute interval rather than the
outer step. Every sample carries its **step** timestamp, the window is
left-open on the grid, and a subquery is a range vector — so `rate(x[5m:15s])`
extrapolates over five minutes exactly as `rate(x[5m])` does.

**A comparison keeps the vector element's value**, whichever side the scalar is
written on: `2 < some_metric` is the metric's value, not `2`. A duplicate on
the "one" side of a `group_left` is an error rather than a cross product.
`last_over_time` keeps `__name__` while every other `_over_time` and
`timestamp()` drop it. `deriv` and `idelta` drop a series with fewer than two
points rather than emitting `NaN`.

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
curl 'http://localhost:8086/api/v1/query?query=cpu_usage{host="srv1"}&time=1700000000'
```

### Range Query

```bash
curl 'http://localhost:8086/api/v1/query_range?query=cpu_usage&start=1700000000&end=1700100000&step=60'
```

### Label Values

```bash
# List all metric names
curl http://localhost:8086/api/v1/label/__name__/values

# List all values for the "host" tag
curl http://localhost:8086/api/v1/label/host/values
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

## Drop-in client compatibility

Every route below is one a stock client derives on its own. None of them
needs a proxy, a path rewrite, or a non-default setting.

### Grafana

Add a **Prometheus** data source pointing at the server:

```
URL: http://chronixd:8086
```

That is the whole configuration. Grafana appends `/api/v1/query`,
`/api/v1/query_range`, `/api/v1/labels`, `/api/v1/series` and
`/api/v1/label/<name>/values` itself, POSTs them as
`application/x-www-form-urlencoded` (its default `httpMethod`), and probes
`/api/v1/status/buildinfo` when you press **Save & test**. All of those are
served, on `GET` and `POST` alike. `time`, `start` and `end` accept RFC 3339
or a Unix timestamp; `step` accepts `15s`, `1m30s` or a bare number of
seconds.

Errors carry the status codes Prometheus uses — 400 for `bad_data`, 422 for
`execution`, 503 for `timeout` — because Grafana branches on them.

### Telegraf

```toml
[[outputs.influxdb]]
  urls = ["http://chronixd:8086"]
  database = "telegraf"

# or, for the v2 client
[[outputs.influxdb_v2]]
  urls = ["http://chronixd:8086"]
  bucket = "metrics"
  organization = "acme"
```

Both post gzipped bodies by default, to `/write` and `/api/v2/write`
respectively; both are served and decompressed. The `precision` parameter is
honoured (`ns`, `us`, `ms`, `s`), and `db`, `bucket`, `org` and `rp` are
accepted and ignored — chronix is one database per process and scopes by the
`X-Namespace` header.

A batch containing a malformed line stores the good lines and answers
`400 {"error": "partial write: …", "written": N, "rejected": M}`, which is
what Telegraf recognises as a permanent failure. Failing the whole batch
instead makes it retry for ever.

### OpenTelemetry Collector

```yaml
exporters:
  otlphttp:
    endpoint: http://chronixd:8086
```

The exporter appends `/v1/metrics` and gzips by default; both are served.

### Prometheus remote write and read

```yaml
remote_write:
  - url: http://chronixd:8086/api/v1/prom/write

remote_read:
  - url: http://chronixd:8086/api/v1/prom/read
```

Remote read applies all four matcher types — `=`, `!=`, `=~` and `!~` — with
regexes anchored the way Prometheus anchors its own, and a `__name__` regex
selecting measurements. An absent label reads as the empty string, so
`job!="a"` also selects series that have no `job`.

A write whose points are rejected — a timestamp outside the out-of-order
window, or a series over the cardinality limit — answers `400` naming the
counts, not `204`. Both rejections are deterministic, so `400` is correct:
Prometheus treats it as permanent and drops the batch, where a `503` would
have it retry the same doomed batch for ever.


## Management Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/measurements` | List all measurements with schema info |
| `GET` | `/api/v1/measurements/{name}/schema` | Get schema for a measurement |
| `DELETE` | `/api/v1/measurements/{name}` | Drop an entire measurement |
| `POST` | `/api/v1/measurements/{name}/restore` | Undo a pending drop, inside the `soft_delete_ttl_secs` grace period |
| `POST` | `/api/v1/delete` | Delete points matching predicates |
| `GET` | `/api/v1/rollups` | List rollup configurations, each with its watermark and any pending repairs |
| `POST` | `/api/v1/rollups` | Create a rollup configuration |
| `DELETE` | `/api/v1/rollups/{name}` | Delete a rollup configuration |
| `POST` | `/api/v1/rollups/{name}/refresh` | Recompute a range now (`?start=&end=`, nanoseconds) |
| `POST` | `/api/v1/export/parquet` | Export data as Apache Parquet |
| `GET` | `/api/v1/connectors` | List active ingestion connectors |

### Connectors

`GET /api/v1/connectors` reports each running connector's counters:

```json
[
  {
    "name": "kafka-main",
    "connector_type": "kafka",
    "status": "Running",
    "metrics": {
      "messages_total": 128034,
      "points_total": 512136,
      "decode_errors": 2,
      "lag": 417,
      "throughput": 842.6
    }
  }
]
```

`lag` is **messages behind the source's newest offset**, and it is `null` for a
source that has no such measure — MQTT, which pushes rather than being polled.
Kafka's figure comes from the watermarks the last fetch response already
carried, so reading it costs no broker round trip, and it is also exported as
the `chronix_kafka_consumer_lag` gauge. `throughput` is points per second since
the connector started; for a windowed rate, take `rate()` over the
`chronix_*_points_total` counters.

### Rollups

A rollup is declared by a **bucket width** and, optionally, the **time zone**
its calendar is read against and an **origin** that moves the boundary:

```bash
curl -X POST http://localhost:8086/api/v1/rollups \
  -H 'content-type: application/json' \
  -d '{
        "name": "energy_monthly",
        "source_measurement": "energy",
        "target_measurement": "energy_monthly",
        "every": "1mo",
        "timezone": "Europe/Berlin",
        "aggregations": ["first", "last"],
        "group_by_tags": ["meter"]
      }'
```

**The unit decides what a bucket means.** Sub-day units (`ns`, `us`, `ms`,
`s`, `m`/`min`, `h`) are a fixed span that never varies — an hour bucket is
always an hour, and in a zone they are aligned to that zone's *standard*
offset so the boundaries stay evenly spaced across a daylight-saving change.
Super-day units (`d`, `w`, `mo`, `y`) follow the **local calendar**: a day
runs from local midnight to local midnight, which is 23 or 25 hours on a
transition day, and a month is 28, 29, 30 or 31 days. The month is `mo`; `m`
is always the minute.

Without a `timezone` a `1d` tier buckets on **UTC** midnight — which is 02:00
local in Berlin in summer and 01:00 in winter, so a "daily" total is a day's
worth of somebody else's day, and the two halves of the year do not agree with
each other. Set the zone whenever the buckets are meant to line up with what a
person calls a day.

**`origin`** moves the boundary off midnight on the 1st, for a period that
does not start there — `"origin": "2024-01-15"` is a billing month that runs
from the 15th, `"origin": "2024-01-01 06:00:00"` a shift day that starts at
six. Write it as a date, a local date and time (read in `timezone`), or an
RFC 3339 instant. What matters is where it falls in the cycle, not how far
away it is: for a one-unit width that is its time of day or day of month, and
for a multi-unit width it also picks which unit opens a bucket — `3mo`
anchored on a January gives calendar quarters. A monthly tier must start on
day 1–28: a boundary on the 29th, 30th or 31st is missing from some months,
so it is refused rather than clamped.

The same `origin` is the fourth argument to `time_bucket()` in SQL, and
`TimeBucket::with_origin` on the native query API.

A rollup's listing reports where it has got to and whether it is behind:

```bash
curl http://localhost:8086/api/v1/rollups
```

```json
{
  "items": [
    {
      "name": "energy_15m",
      "source_measurement": "energy",
      "target_measurement": "energy_15m",
      "every": "15m",
      "timezone": null,
      "origin": null,
      "aggregations": ["Avg", "Max"],
      "materialised_until": 1700000000000000000,
      "pending_repairs": [[1699900000000000000, 1699903600000000000]]
    }
  ],
  "total": 1, "offset": 0, "limit": 100
}
```

`every` is the width, spelled the way it was written, so what this endpoint
reports can be typed straight back into a create request. `timezone` is the
calendar it is read against, or `null` for UTC, and `origin` the instant the
boundaries are aligned to, or `null` for midnight on the 1st. `materialised_until` is the
exclusive end of the newest bucket aggregated so far. `pending_repairs` are bucket ranges *below* that watermark whose input
changed afterwards — a `backfill`, an import, or a delete — and which the
next maintenance pass will recompute. **A non-empty list is why retention is
holding raw data**: the tier does not yet agree with the raw data, so the raw
data stays. It empties on its own; if it does not, check the logs for
`rollup materialisation failed` and the `chronix_rollup_failures_total`
metric.

To recompute a range immediately — after an out-of-band restore, say, which
the engine cannot observe:

```bash
curl -X POST 'http://localhost:8086/api/v1/rollups/energy_15m/refresh?start=1699900000000000000&end=1699903600000000000'
```

```json
{ "rollup": "energy_15m", "points_written": 96 }
```

The range is aligned outward to whole buckets. Recomputing is idempotent:
the target's existing aggregates over the range are replaced, not merged
with, and any tier fed by this one is marked for repair in turn.

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

**Authorization:** the namespace a request may act in comes from its
credential. An API key carries a `namespaces` list, a JWT carries a
`namespaces` claim, and naming a namespace outside that list is answered
`403` — the header alone is not authority. An empty list means unrestricted,
which is why a server with `multi_tenancy = true` refuses to start while a
key has one.

When a Cedar authorization engine is also configured, the namespace
middleware additionally checks that the principal is permitted to act on the
target namespace (`Chronix::Namespace` resource type). Policies can restrict
users to specific namespaces:

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
| `target_dir` | string | Backup target directory, resolved inside `backup_root` |

**Response:** `200 OK` — a `BackupManifest` with `version`, `created_at`,
`wal_sequence`, `segments`, `file_count` and `total_bytes`.

The backup is a checkpoint taken after a flush: every write acknowledged
before the request is in it, and one accepted while it runs is not. Backing
up into a directory that already holds a checkpoint replaces it.

#### Verify a Backup

```
POST /api/v1/admin/backup/verify
```

**Request Body:**
| Field | Type | Description |
|-------|------|-------------|
| `backup_dir` | string | Backup directory, resolved inside `backup_root` |

**Response:** `200 OK` — the backup's `BackupManifest`.

**Errors:** `400` if the directory is not a backup, the manifest is corrupt,
or a segment is missing or the wrong size, with the file named. Reads only.

#### Restore from Backup

```
POST /api/v1/admin/restore
```

**Request Body:**
| Field | Type | Description |
|-------|------|-------------|
| `backup_dir` | string | Backup source directory, resolved inside `backup_root` |
| `target_dir` | string | Restore target directory; must not exist |

**Response:** `200 OK` — the backup's `BackupManifest`.

**Errors:** `400` if the target exists, the manifest is missing or corrupt,
or the backup is incomplete — every segment its catalog names is checked for
presence and size before anything is copied. The restored directory is a
complete database that can be opened on any machine.

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

## When the disk fills

A write that cannot be made durable answers **`507 Insufficient Storage`**;
gRPC answers `RESOURCE_EXHAUSTED` with the same text.

```json
{"error": "no space left on the data volume: WAL error: WAL I/O error: No space left on device (os error 28)",
 "code": "STORAGE_FULL"}
```

`507` is a 5xx, so retrying clients keep retrying — the condition usually
clears when a log rotates or retention runs.

**Nothing is lost.** The WAL rewinds to its last durable offset, so writes
resume once space returns without a restart, and a record the caller was told
had failed is never written later. A flush that cannot write its segment
leaves the memtable frozen for the next attempt. Reads keep working and
`/ready` stays `200`: this is back-pressure, not a reason to leave the load
balancer.

---

## Authentication

When authentication is configured, every endpoint outside `auth.exempt_paths`
requires `Authorization: Bearer <token>`, which may be an API key or a JWT.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/admin/auth/keys` | Create a new API key |
| `GET` | `/api/v1/admin/auth/keys` | List all API keys |
| `DELETE` | `/api/v1/admin/auth/keys/{name}` | Revoke an API key |

These endpoints, like namespace management and backup/restore, require a
credential carrying the **admin** capability.

### Create Key

```bash
curl -X POST http://localhost:8086/api/v1/admin/auth/keys \
  -H 'Authorization: Bearer <admin-key>' \
  -H 'Content-Type: application/json' \
  -d '{
        "name": "grafana-reader",
        "expires_at": 1793491200,
        "namespaces": ["tenant-a"]
      }'
```

`expires_at` is a Unix timestamp in seconds and is optional. `namespaces` is
required when `multi_tenancy = true`, because a key naming none may act in
every tenant.

The raw key is returned **once**; only its Argon2 hash is stored.

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
| `prom_series_limit` | `10000` | Maximum label-sets returned by `/api/v1/series` |

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

### Undoing a drop

`DELETE /api/v1/measurements/{name}` is the most destructive single call in
this API, and by default it is irreversible.

Set `[database] soft_delete_ttl_secs` and it stops being: the drop marks the
measurement pending, a background pass hard-deletes it once the deadline
passes, and until then it can be brought back with its data.

A measurement pending drop is invisible everywhere a genuinely dropped one
would be — `SHOW TABLES`, `/api/v1/measurements`, PromQL discovery, and every
query answer nothing for it — while its data is untouched, so a restore
before the deadline is instant and loses nothing. The pending state survives
a restart, so a restart between the drop and the restore neither un-drops it
nor forgets the deadline.

The deadline is measured from the wall clock **capped by the newest timestamp
the database holds** — the same reference retention uses — so a clock that
jumps forward cannot close the window and destroy the data it was protecting.
The trade is the same one retention makes: a database that has stopped
receiving data stops reclaiming this disk, and a `DELETE` without
`soft_delete_ttl_secs` is how to reclaim it immediately.

```bash
curl -X DELETE http://localhost:8086/api/v1/measurements/power   # 204
curl -X POST http://localhost:8086/api/v1/measurements/power/restore   # 204
```

`404` means there is nothing to undo — the measurement was never dropped, the
grace period has passed, or `soft_delete_ttl_secs` is not configured, and the
message says which. Under multi-tenancy a drop removes *this namespace's
series* rather than the measurement (the schema is shared between tenants), so
there is no pending drop and the answer is also `404`.
