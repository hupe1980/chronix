+++
title = "Chronix — embedded time-series database for Rust"
description = "An embedded-first time-series database in pure Rust. Forecasting and anomaly detection run inside the engine; speaks PromQL, InfluxDB, OTLP and Flight SQL."
template = "index.html"
insert_anchor_links = "none"

[extra]
bare_title = true
tagline = "A time-series database you add with Cargo."
subtitle = """
Chronix runs **inside your binary** — no server, no sidecar, no daemon. The same
engine also runs as `chronixd` behind Grafana when you need one. Forecasting and
anomaly detection are part of the query engine, not a Python service beside it.
"""

# Code samples live here rather than in the template because Zola highlights
# Markdown, not Tera — a fenced block in a template renders as literal text.
embedded_code = """
```rust
use chronix::prelude::*;

// Gateway preset: under 25 MiB peak heap measured for ingest and rollups,
// flash-friendly WAL.
let db = Chronix::open_small("/var/lib/chronix")?;

db.insert(&Point::new(
    SeriesKey::new("power", tags! { "meter" => "main" })?,
    fields! { "watts" => 231.45 },
    1_700_000_000_000_000_000,
)?)?;

// Forecasting is a method on the database, not another service —
// and the model is chosen by cross-validation, not by you.
let chosen = db.auto_forecast(
    "power", "watts", &[("meter", "main")],
    start, end, 24 * 60, None,
)?;
println!("{}", chosen.selection.label);   // e.g. SARIMA(1,0,0)(0,1,1)[24]
```
"""

server_code = """
```bash
chronixd --data-dir /var/lib/chronix

# InfluxDB line protocol — Telegraf can write here unchanged
curl -X POST localhost:8086/write \\
  --data-binary 'power,meter=main watts=231.45'

# PromQL — the same request Grafana sends a Prometheus data source
curl 'localhost:8086/api/v1/query?query=rate(power_watts[5m])'
```
"""
+++
