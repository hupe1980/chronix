+++
title = "Send data to Chronix"
description = "Point Prometheus remote write, Telegraf, the OpenTelemetry Collector, Kafka or MQTT at Chronix — the endpoints and settings each client needs."
weight = 10
+++

Chronix speaks the protocols your agents already send. Point Prometheus remote write, Telegraf, the OpenTelemetry Collector, Kafka or MQTT at it; no client changes beyond the URL.

## Wire Protocol Endpoints

### `POST /api/v1/prom/write`

Prometheus Remote Write endpoint. Accepts snappy-compressed protobuf, in
**either** remote-write version.

```bash
# Used automatically by Prometheus with remote_write config:
# remote_write:
#   - url: http://chronix:8086/api/v1/prom/write
#     send_native_histograms: true
#     # Optional; omit for 1.0.
#     protobuf_message: io.prometheus.write.v2.Request
```

| `Content-Type` | Version |
|---|---|
| absent, or `application/x-protobuf` | 1.0 |
| `application/x-protobuf;proto=prometheus.WriteRequest` | 1.0 |
| `application/x-protobuf;proto=io.prometheus.write.v2.Request` | 2.0 |

Anything else is **415 Unsupported Media Type**, with both supported message
sets named in the body so a sender can fall back on its own.

A 2.0 write answers with what it actually stored:

| Header | Meaning |
|---|---|
| `X-Prometheus-Remote-Write-Samples-Written` | float samples stored |
| `X-Prometheus-Remote-Write-Histograms-Written` | native histograms stored |
| `X-Prometheus-Remote-Write-Exemplars-Written` | always `0` — Chronix stores no exemplars |

A 1.0 write answers none of them, because 1.0 does not define them.

**Native histograms** are carried by both versions and stored in the metric's
own column, so the name you query is the name you exported. A float sample and
a histogram under one metric name is a type conflict and is refused by name: a
metric that is a gauge on Monday and a distribution on Tuesday is two metrics.
A histogram staleness marker is dropped like a float one.

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


OpenTelemetry OTLP metrics ingestion, in protobuf
(`application/x-protobuf`) or JSON. Accepts gauges, sums, summaries, and
**both** histogram shapes.

```bash
curl -X POST http://localhost:8086/api/v1/otlp/metrics \
  -H 'Content-Type: application/json' \
  -d '{"resourceMetrics": [...]}'
```

| Instrument | Stored as |
|---|---|
| Gauge | field `gauge` |
| Sum | field `value` |
| Exponential histogram | field `value`, a native histogram at the matching schema |
| Explicit-bucket histogram | field `value`, a native histogram with custom boundaries (schema −53) |
| Summary | fields `count`, `sum`, `quantile_<q>` |

Both histogram shapes answer `histogram_quantile` and the rest of the
`histogram_*` family, and both answer the classic `foo_bucket` / `foo_count` /
`foo_sum` names.

An OTLP `sum` the exporter left unset stays **absent** rather than being stored
as zero — an unset sum means the instrument recorded negative events, not that
the observations summed to nothing.

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
    send_native_histograms: true
    # Optional; omit for remote write 1.0, which also carries histograms.
    protobuf_message: io.prometheus.write.v2.Request

remote_read:
  - url: http://chronixd:8086/api/v1/prom/read
```

Remote read returns native histograms as well as float samples, so a
federating Prometheus reading Chronix as long-term storage gets the
distribution back rather than an empty series.

Remote read applies all four matcher types — `=`, `!=`, `=~` and `!~` — with
regexes anchored the way Prometheus anchors its own, and a `__name__` regex
selecting measurements. An absent label reads as the empty string, so
`job!="a"` also selects series that have no `job`.

A write whose points are rejected — a timestamp outside the out-of-order
window, or a series over the cardinality limit — answers `400` naming the
counts, not `204`. Both rejections are deterministic, so `400` is correct:
Prometheus treats it as permanent and drops the batch, where a `503` would
have it retry the same doomed batch for ever.

## Connector Management

### Startup Retry

Ingestion connectors are started at boot. By default a connector that cannot
reach its broker stops the server, rather than leaving it running with a
silently dead ingestion path.

| Setting | Default | Description |
|---------|---------|-------------|
| `connector_start_retries` | `0` | Start attempts per connector; `0` is fail-fast |
| `connector_start_retry_backoff_ms` | `1000` | Initial backoff; doubles each retry |

Raise the retry count when connectors and brokers start concurrently, as in a
container stack.

## Protocol-Level Compatibility

Chronix supports industry-standard protocols, enabling any client that speaks these protocols:

### InfluxDB Line Protocol

Any InfluxDB v1/v2 client library can write to Chronix:

```
POST /write
Content-Type: text/plain

cpu,host=a usage=42.5 1700000000000000000
```

### Prometheus Remote Write/Read

Configure Prometheus to remote-write to Chronix:

```yaml
# prometheus.yml
remote_write:
  - url: "http://chronix:8086/api/v1/prom/write"

remote_read:
  - url: "http://chronix:8086/api/v1/prom/read"
```

### OTLP Metrics

OpenTelemetry collectors can export metrics to Chronix:

```yaml
# otel-collector-config.yaml
exporters:
  otlphttp:
    endpoint: http://chronix:8086/api/v1/otlp/metrics
```

### Arrow Flight SQL

Any ADBC-compatible client or BI tool (DBeaver, DataGrip, Tableau) can connect via Flight SQL on port `8817`.

### gRPC

The full Chronix gRPC API is available at port `8087`, defined in `chronix.proto` with 9 RPCs: `Write`, `StreamWrite`, `Query`, `GetSchema`, `ListMeasurements`, `Delete`, `DropMeasurement`, `ServerInfo`, `ExecuteSql`.
