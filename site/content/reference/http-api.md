+++
title = "HTTP API"
description = "Every chronixd HTTP endpoint: writes, queries, triggers, annotations, namespaces, admin and MCP, with request and response shapes."
weight = 10
aliases = ["/docs/api-reference/"]
+++

`chronixd` serves everything below on one port (`8086` by default). The
machine-readable form is the OpenAPI 3.1 document at `GET /api/v1/openapi.json`.
PromQL endpoints are documented in the [PromQL reference](@/reference/promql.md);
gRPC and Flight SQL in [gRPC and Flight SQL](@/reference/grpc-and-flight.md).

| Area | Routes |
|---|---|
| Write | `POST /api/v1/write` (JSON) · `POST /write`, `/api/v2/write`, `/api/v1/write/influx` (line protocol) · `POST /api/v1/prom/write` (remote write) · `POST /v1/metrics`, `/api/v1/otlp/metrics` (OTLP) |
| Query | `POST /api/v1/chronix/sql` · `POST /api/v1/chronix/query`, `/api/v1/chronix/query/explain` · PromQL: `/api/v1/query`, `/query_range`, `/series`, `/labels`, `/label/{name}/values`, `/metadata`, `/status/tsdb`, `POST /api/v1/chronix/promql/explain` · `POST /api/v1/prom/read` |
| Data | `GET /api/v1/measurements`, `/measurements/{name}/schema` · `POST /measurements/{name}/schema/fields` · `DELETE /measurements/{name}`, `POST …/restore` · `POST /api/v1/delete`, `/api/v1/delete_batch` · `POST /api/v1/export/parquet` |
| Derived data | `/api/v1/rollups`, `/rollups/{name}`, `POST /rollups/{name}/refresh` · `/api/v1/detectors`, `/detectors/{name}` · `/api/v1/triggers`, `/triggers/{name}` · `GET /api/v1/signals` |
| Streams | `GET /api/v1/cdc/stream` · `GET /api/v1/annotations`, `/annotations/stream` |
| Tenancy | `/api/v1/namespaces`, `/namespaces/{name}`, `…/usage`, `PUT …/quota` |
| Admin | `/api/v1/admin/auth/keys` · `POST /api/v1/admin/backup`, `/backup/verify`, `/restore` · `PUT /api/v1/admin/log-level` · `/api/v1/admin/analytics/models`, `/retrain` · `GET /api/v1/dashboards/export` · `GET /api/v1/connectors` |
| Agents and UI | `/mcp` · `/console` |
| Operations | `/health` (`/healthz`) · `/ready` (`/readyz`) · `/metrics` · `/api/v1/openapi.json` |

`/api/v1/alerts`, `/api/v1/rules` and `/api/v1/query_exemplars` answer
Prometheus's empty shapes, so Grafana's Prometheus data source does not error on
them. Every `/api/v1/prom/…` path is an alias of the same route without `prom/`.

---

## Errors

Every failing request answers the same JSON envelope, whatever produced it —
a handler, a body that will not deserialise, a path no route matches, a method
no route accepts:

```json
{"error": "measurement not found: nope", "code": "NOT_FOUND"}
```

`code` is the machine-readable half; branch on it rather than on the message.

### Hints and spans

A query that names a table, column or function the database does not have
says what it probably meant. `hint` and `span` are present only when there is
something to say:

```json
{
  "error": "unknown table \"cpu_usge\" — did you mean \"cpu_usage\"?",
  "code": "BAD_REQUEST",
  "hint": "did you mean \"cpu_usage\"?",
  "span": {"start": 14, "end": 22}
}
```

- `error` is complete on its own and always contains the hint.
- `span` is half-open **byte** offsets into the query as sent:
  `query[start..end]` is the name to underline.
- With no close match, `error` lists what exists instead:
  `unknown column "temp"; the columns are "_time", "host" and "usage"`.
- Suggestions only name tables the caller's namespace holds.

| Status | `code` | Means |
|--------|--------|-------|
| `400` | `BAD_REQUEST` | Malformed request — bad JSON, an unparseable parameter, invalid SQL |
| `400` | `CARDINALITY_EXCEEDED` | The write would exceed `max_series_cardinality` |
| `400` | `NAMESPACE_QUOTA_EXCEEDED` | The write would add a series or a measurement past its namespace's `max_series_count` or `max_measurements`. Permanent — the batch is refused whole and retrying it unchanged fails the same way |
| `400` | `BATCH_EXCEEDS_NAMESPACE_RATE` | One batch holds more points than its namespace's `max_ingestion_rate` allows in a second, so it can never be admitted. Split it |
| `400` | `INVALID_QUOTA` | A namespace quota out of bounds, or one Chronix does not have, such as `max_storage_bytes` |
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
| `429` | `RATE_LIMITED` | Over the server's or the namespace's request rate. Carries `Retry-After` |
| `429` | `NAMESPACE_RATE_LIMITED` | Over the namespace's ingestion rate; nothing of the batch was written. Carries `Retry-After` |
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

See [Data Model](@/reference/data-model.md#exact-decimals-for-money-and-meters).

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
re-materialisation ([Data Model](@/reference/data-model.md)).

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
  [Series Identity](@/explanation/architecture.md#series-identity-and-the-canonical-form).

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

`/api/v1/query` is the Prometheus instant query; this is the native one.
`/api/v1/chronix/query/explain` takes the same body and returns the plan.

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
[Webhook URL SSRF Protection](@/reference/security.md#webhook-url-ssrf-protection).

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
curl -X POST localhost:8086/api/v1/triggers \
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

Change events as Server-Sent Events: every write, series delete and dropped
measurement, with a 15-second keep-alive. Filter with comma-separated
`measurement` and `event_type` (`point_written`, `series_deleted`,
`measurement_dropped`). Each frame's `event:` is the event type.

```bash
curl -N 'http://localhost:8086/api/v1/cdc/stream?measurement=cpu&event_type=point_written'
```

```text
event: point_written
data: {"PointWritten":{"measurement":"cpu","tags":{"host":"srv1"},"fields":{"usage":{"F64":0.85}},"timestamp":1700000000000000000,"seq":42}}
```

A client that falls far behind skips the oldest events; resume from the store
using `seq`. In a browser, `new EventSource('/api/v1/cdc/stream')` with one
listener per event type.

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
| `POST` | `/api/v1/measurements/{name}/restore` | Undo a pending drop, inside the `soft_delete_ttl_secs` grace period |
| `POST` | `/api/v1/delete` | Delete points matching predicates |
| `GET` | `/api/v1/rollups` | List rollup configurations, each with its watermark and any pending repairs |
| `POST` | `/api/v1/rollups` | Create a rollup configuration |
| `DELETE` | `/api/v1/rollups/{name}` | Delete a rollup configuration |
| `POST` | `/api/v1/rollups/{name}/refresh` | Recompute a range now (`?start=&end=`, nanoseconds) |
| `POST` | `/api/v1/export/parquet` | Export data as Apache Parquet |
| `GET` | `/api/v1/connectors` | List active ingestion connectors |

### Declared detectors

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/detectors` | List declared detectors with watermark, series, late points, warming slots and status |
| `POST` | `/api/v1/detectors` | Declare a detector — `201` when created or identical; `409` if the name holds a different one |
| `DELETE` | `/api/v1/detectors/{name}` | Drop a detector and its state; its verdicts stay |

```bash
curl -X POST http://localhost:8086/api/v1/detectors \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "pv_health",
    "source_measurement": "pv",
    "field": "power",
    "season": {"slot": "15m", "period": "1d"},
    "timezone": "Europe/Berlin",
    "zscore": 4.0,
    "cusum": {"k": 0.5, "h": 5.0}
  }'
```

Optional fields: `where` (a tag → value map: only matching rows are judged),
`group_by_tags`, `min_samples`, `forgetting` (half-life in periods),
`output_measurement` (default `<name>_verdicts`), `settle_seconds`,
`max_points_per_pass`. A season's slots are wall-clock positions in
`timezone` (14:00 is slot 56 of a quarter-hour day on every day of the year);
`warming_slots` in the listing counts slots that have data but not yet
`min_samples` of it. Verdicts are ordinary rows with a `slot` field: query
them with SQL or PromQL (`pv_health_verdicts_z`). With multi-tenancy on, a detector is scoped
to the caller's namespace — it judges that namespace's rows only, and other
namespaces never see it. Rollup and detector administration are the same
Cedar actions (`Read` to list, `Write` to declare or drop).

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

`status` reports what the connector's task is doing:

| Status | Meaning | `chronix_connector_up` |
|---|---|---|
| `Running` | Connected and receiving | `1` |
| `Idle` | Connected, nothing arriving | `1` |
| `Reconnecting` | Cannot reach the broker; retrying with backoff | `0` |
| `Failed` | Retrying, and naming the error — `{"failed": "<reason>"}`. Not terminal | `0` |
| `Stopped` | Not started, or stopped | `0` |

Alert on `chronix_connector_up`, not on `/ready`: a connector that cannot
reach its broker does not stop this node answering queries, so readiness does
not fail on it.

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

| `every` unit | Bucket |
|---|---|
| `ns`, `us`, `ms`, `s`, `m`/`min`, `h` | a fixed span; in a time zone, aligned to its standard offset so boundaries stay evenly spaced across a DST change |
| `d`, `w`, `mo`, `y` | the local calendar: a day is midnight to midnight (23 or 25 h on a transition day), a month 28–31 days. `mo` is the month; `m` is the minute |

- **`timezone`** — the calendar's zone. Without it a `1d` tier is a UTC day,
  which in Berlin starts at 01:00 or 02:00 local; set it whenever buckets
  should match what a person calls a day.
- **`origin`** — moves the boundary: `"2024-01-15"` is a billing month from
  the 15th, `"2024-01-01 06:00:00"` a shift day from six, `3mo` anchored in
  January gives calendar quarters. A date, a local date and time (read in
  `timezone`) or an RFC 3339 instant. A monthly boundary on the 29th–31st is
  refused, since some months lack it. The same origin is `time_bucket()`'s
  fourth argument in SQL.

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

`every` reads back exactly as written. `materialised_until` is the exclusive
end of the newest final bucket. `pending_repairs` are ranges below it whose
source changed — a backfill, an import, a delete — and that the next
maintenance pass recomputes; **while any are pending, retention keeps the raw
data**. If they do not clear, look for `rollup materialisation failed` in the
log and `chronix_rollup_failures_total`.

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
data. See [Security](@/guides/secure-a-server.md#namespace-isolation).

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
| `GET` | `/api/v1/namespaces/{name}/usage` | Usage beside its quota |
| `PUT` | `/api/v1/namespaces/{name}/quota` | Replace a namespace's quota |

### Quotas

Every quota is enforced on every write surface — line protocol, remote write
1.0 and 2.0, OTLP, the JSON API, gRPC, Flight SQL and the Kafka and MQTT
connectors — and **`0` means unlimited**, so a namespace created without a
limit is unlimited rather than unwritable.

| Quota | Enforced | Over it |
|---|---|---|
| `max_series` | When a write would add a series. Exact: counted from the same set as the database's cardinality budget, and released by a whole-series delete, a dropped measurement and retention | `400 NAMESPACE_QUOTA_EXCEEDED`; the batch is refused whole. Series the namespace already holds stay writable |
| `max_measurements` | When a write would add a measurement. A measurement counts until it is dropped | `400 NAMESPACE_QUOTA_EXCEEDED` |
| `max_ingestion_rate` | Points per second, per batch, before it is written. One second's allowance is also the largest batch that can ever be admitted | `429 NAMESPACE_RATE_LIMITED` with `Retry-After`; a batch larger than the allowance is `400 BATCH_EXCEEDS_NAMESPACE_RATE` |
| `max_request_rps`, `max_request_burst` | Requests per second, on HTTP, gRPC and Flight SQL | `429 RATE_LIMITED` with `Retry-After` |

On gRPC and Flight SQL each refusal is `RESOURCE_EXHAUSTED`; the retryable
ones carry a `grpc-retry-pushback-ms` trailer. There is no storage quota:
namespaces share segments, so the bytes one occupies cannot be measured
exactly. Retention bounds disk.

### Create Namespace

```bash
curl -X POST http://localhost:8086/api/v1/namespaces \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "production",
    "max_series": 1000000,
    "max_ingestion_rate": 50000,
    "max_measurements": 500
  }'
```

### Change a Quota

Every field is required — a quota is a complete statement, and an omitted
field is not "leave it alone".

```bash
curl -X PUT http://localhost:8086/api/v1/namespaces/production/quota \
  -H 'Content-Type: application/json' \
  -d '{"max_series": 2000000, "max_ingestion_rate": 50000, "max_measurements": 500,
       "max_request_rps": 0, "max_request_burst": 0}'
```

### Check Usage

```bash
curl http://localhost:8086/api/v1/namespaces/production/usage
```

```json
{
  "series": 81234,
  "measurements": 41,
  "ingestion_rate": 12000,
  "quota": {
    "max_series_count": 1000000,
    "max_ingestion_rate": 50000,
    "max_measurements": 500,
    "max_request_rps": 0,
    "max_request_burst": 0
  }
}
```

`series` and `measurements` are the database's own counts — the set new
series are admitted against — and `ingestion_rate` is the points admitted
over the last whole second.

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

## MCP

`/mcp` is a [Model Context Protocol](https://modelcontextprotocol.io) endpoint
(streamable HTTP) with eight read-only tools: `list_metrics`, `label_values`,
`query_promql`, `query_sql`, `explain`, `cardinality`, `forecast` and
`detect_anomalies`. It is authenticated, namespaced and authorised (`Read`) like
`/query`. `chronixd mcp --data-dir DIR` serves the same tools over stdio from a
directory opened read-only. See [Use Chronix from an AI agent](@/guides/ai-agents.md).

## Admin

Everything under `/api/v1/admin/` requires a credential with the admin
capability. Backup, restore and `backup/verify` are described in
[Backup and restore](@/guides/backup-and-restore.md); the cluster binary's own
admin routes in [Cluster](@/reference/cluster.md).

### Runtime Log-Level Adjustment

| Method | Path | Description |
|--------|------|-------------|
| `PUT` | `/api/v1/admin/log-level` | Change the server log level at runtime |

Adjust the tracing log level without restarting the server. Uses a
`tracing_subscriber::reload::Layer` so changes take effect immediately
across all spans.

```bash
curl -X PUT http://localhost:8086/api/v1/admin/log-level \
  -H 'Content-Type: application/json' \
  -d '{"level": "debug"}'
# {"previous": "info", "current": "debug"}
```

Accepted levels: `trace`, `debug`, `info`, `warn`, `error`. Invalid
levels return `400 Bad Request`.

## Overload and full disk

### When the data volume fills

1. A write that cannot reach the WAL answers **`507 STORAGE_FULL`**. Clients
   retry; the condition usually clears.
2. The WAL **rewinds to its last durable offset**, so writes resume the moment
   space returns — no restart — and a record reported as failed is never
   written later.
3. A flush that cannot write its segment leaves the memtable **frozen** for the
   next attempt, so nothing acknowledged is lost.
4. Reads keep working and `/ready` stays `200`: back-pressure, not a reason to
   leave the load balancer.

**Fix it by freeing space** — retention, WAL `max_file_size`, or the cold tier.
Alert on `chronix_write_errors_total{reason="storage_full"}`.

A failed `fsync` is different and **does** stop writes: the kernel may drop the
dirty pages and clear the error, so the file's contents are unknown. `/ready`
answers `503` with the reason, and so does every write — `503 OVERLOADED`,
unredacted, naming the condition. Recovery is to close and reopen the database.

### When ingestion outruns the storage

A memtable at capacity is back-pressure, not a fault: the flush that clears it
is already running. Writes answer `503 BACKPRESSURE` with `Retry-After: 1` and
a message naming the limit, so an agent backs off for a second and carries on.
It is the ordinary condition on a gateway whose flash is slower than its
ingest. Alert on `chronix_write_admission_rejected_total`.
