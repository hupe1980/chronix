+++
title = "Quickstart: the server"
description = "Run chronixd with demo data, query it with PromQL and SQL, open the console and connect Grafana — a time-series server in five minutes."
weight = 20
+++

`chronixd` is the server: the same engine and file format as the crate, behind
HTTP, PromQL, gRPC and Arrow Flight SQL. One command starts it on a week of
generated data.

## Run the demo

```bash
cargo install chronixd
chronixd --demo
```

The demo serves a generated week of gateway data — a PV array, a battery, the
grid connection and a heat pump, every 10 seconds — from a temporary directory
deleted on exit. It binds `127.0.0.1:8086` only and has no authentication.
Prebuilt binaries for Linux and macOS are on every
[GitHub release](https://github.com/hupe1980/chronix/releases).

## Query it

The banner prints these commands with the right port:

```bash
# PromQL: the PV array's output now
curl -s 'http://127.0.0.1:8086/api/v1/query?query=pv_power_w'

# SQL: the next hour of PV output, forecast in the engine
curl -s http://127.0.0.1:8086/api/v1/chronix/sql \
  -H 'content-type: application/json' \
  -d '{"query": "SELECT unnest(forecast(power_w, _time, 360)) FROM pv"}'
```

Two anomalies are planted: the PV output drops to 0 W for twenty minutes at
noon on the third day, and the heat pump draws 2 700 W for fifteen minutes at
03:10 on the fifth.

## Open the console

Open [`http://127.0.0.1:8086/console`](@/guides/console.md) to chart a query,
switch on forecasts and anomalies, and see the cardinality of the database.

{{ <screenshot src="img/console-anomalies.webp" alt="The console charting a week of the demo’s heat pump power with the planted spike ringed as an anomaly" caption="The demo in the console: the planted heat-pump spike, found by the anomaly detector." /> }}

## Connect Grafana

Add a **Prometheus** data source with the URL `http://127.0.0.1:8086` — no
plugin. [Connect Grafana](@/guides/grafana.md) covers dashboards, Flight SQL
and annotations.

## Run it on your own data

```bash
chronixd --data-dir /var/lib/chronix
```

It listens on HTTP **8086** (InfluxDB's port, so existing agents need no
change), gRPC **8087** and Arrow Flight SQL **8817**. Write a point and read it
back:

```bash
# InfluxDB line protocol — what Telegraf sends
curl -X POST localhost:8086/write \
  --data-binary 'cpu,host=web-01 usage_idle=95.5 1700000000000000000'

# Native JSON
curl -X POST localhost:8086/api/v1/write -H 'Content-Type: application/json' \
  -d '{"measurement": "cpu", "tags": {"host": "web-01"},
       "fields": {"usage_idle": 95.5}, "timestamp": 1700000000000000000}'

curl -X POST localhost:8086/api/v1/chronix/query -H 'Content-Type: application/json' \
  -d '{"measurement":"cpu"}'
```

A write answers `204 No Content`; a query answers a JSON array of rows.

## Next

- [Send data to Chronix](@/guides/ingest.md) — Prometheus, Telegraf, OpenTelemetry, Kafka, MQTT
- [Secure a server](@/guides/secure-a-server.md) — before it leaves loopback
- [Operate a server](@/guides/operate.md) — deploy, monitor, inspect, upgrade
- [Configuration](@/reference/configuration.md) and [command line](@/reference/cli.md)
