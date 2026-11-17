+++
title = "Chronix — the time-series database you add with Cargo"
description = "An embedded-first time-series database in pure Rust: forecasting and anomaly detection in the engine, exact decimals, and PromQL, InfluxDB, OTLP and Flight SQL."
template = "index.html"
insert_anchor_links = "none"

[extra]
bare_title = true
eyebrow = "Embedded time-series database · Rust"
tagline = "The time-series database you add with Cargo."
lede = """
Chronix runs **inside your binary** — on a 512 MB gateway or behind Grafana — and
forecasts, detects anomalies and keeps exact decimals **in the engine**, not in a
service beside it.
"""

# Zola highlights Markdown, not Tera, so code lives here and the template
# renders it with `| markdown`.
hero_code = """
```rust
use chronix::prelude::*;

// The gateway preset: small memory budgets, serial reads.
let db = Chronix::open_small("/var/lib/chronix")?;
let t = 1_700_000_000_000_000_000; // ns since the epoch

let key = SeriesKey::new("power", tags! { "meter" => "main" })?;
db.insert(&Point::new(key, fields! { "watts" => 231.45 }, t)?)?;

// PromQL, in-process. No server.
let rate = db.promql("rate(power_watts[5m])", t)?;

// 1 440 points ahead; the model is picked by cross-validation.
let day = 86_400_000_000_000;
let next = db.auto_forecast("power", "watts", &[("meter", "main")],
                            t - 30 * day, t, 1_440, None)?;
```
"""

server_code = """
```shellscript
# InfluxDB line protocol: Telegraf writes here unchanged
curl localhost:8086/write \\
  --data-binary 'power,meter=main watts=231.45'

# PromQL: what Grafana's Prometheus data source sends
curl localhost:8086/api/v1/query \\
  --data-urlencode 'query=rate(power_watts[5m])'

# SQL: forecasting is an aggregate
curl localhost:8086/api/v1/chronix/sql \\
  -H 'content-type: application/json' -d '{"query":
  "SELECT unnest(forecast(watts, _time, 24)) FROM power"}'
```
"""

# Every number below is stated, with the test or bench that pins it, in the
# README or on docs/performance.md. Change one there first.
[[extra.cards]]
title = "In your binary"
stat = "under 25 MiB"
stat_note = "peak heap for an hour of gateway ingest and rollups"
body = "A Cargo dependency, not a deployment: a file-locked directory, a crash-safe WAL and one maintenance thread that flushes, compacts, rolls up and enforces retention. No daemon to start."
href = "@/guides/run-on-a-gateway.md"
more = "Run it on a gateway"

[[extra.cards]]
title = "Analytics in the engine"
stat = "ARIMA · ETS · CUSUM"
stat_note = "matched against statsmodels and scipy"
body = "Forecasting with calibrated intervals, anomaly detection, and detectors you declare once — persisted and advanced by the store like a rollup. Callable from Rust, SQL and PromQL-backed dashboards."
href = "@/guides/forecast-and-detect.md"
more = "Forecast and detect"

[[extra.cards]]
title = "Exact where it matters"
stat = "475×"
stat_note = "compression on decimal meter data"
body = "A Decimal type with the scale on the column, exact through the WAL, every wire format and every aggregate. Pcodec recovers the integer behind a float: 5–29× on realistic sensor data."
href = "@/reference/data-model.md"
more = "The data model"

[[extra.cards]]
title = "Speaks your protocols"
stat = "PromQL · OTLP · Influx · Flight"
stat_note = "tested with the bytes real clients send"
body = "Prometheus remote write and read with native histograms, OpenTelemetry metrics, InfluxDB line protocol and Arrow Flight SQL. Point Grafana at it as a Prometheus data source."
href = "@/guides/ingest.md"
more = "Send data"
+++
