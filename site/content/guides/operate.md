+++
title = "Operate a server"
description = "Deploy chronixd, monitor it, inspect a data directory, upgrade across format generations, handle a full disk and troubleshoot."
weight = 110
aliases = ["/docs/operations/"]
+++

Deploying, configuring and monitoring Chronix.

## Deployment

### Single-Node (Embedded)

```rust
use chronix::prelude::*;

// The gateway preset; `Chronix::open(ChronixConfig::builder()…)` is the general form.
let db = Chronix::open_small("/var/lib/chronix")?;
```

### Single-Node Server

```bash
# Install (or download a prebuilt binary from https://github.com/hupe1980/chronix/releases)
cargo install chronixd

# Try it with a generated week of data, on loopback, deleted on exit
chronixd --demo

# Run with defaults (data in ./chronix-data, listen on 0.0.0.0:8086)
chronixd

# Custom configuration
chronixd --config /etc/chronix/chronixd.toml
```

**Validate the file before it reaches a node.** `--check-config` applies the
file, the environment and the flags, runs every startup validation, prints
what it resolved to, and exits without opening the database or binding a
port — so a typo, a `[tracing]` section this build cannot honour, or an
`[auth]` section that authenticates nobody fails in CI rather than on the
node:

```bash
chronixd --config /etc/chronix/chronixd.toml --check-config
```

In a container, with the file mounted:

```bash
docker run --rm -v /etc/chronix:/etc/chronix:ro \
  ghcr.io/hupe1980/chronixd:latest \
  --config /etc/chronix/chronixd.toml --check-config
```

It exits non-zero on a bad file, so it works as a deploy gate. See
[Configuration](@/reference/configuration.md).

## Monitoring

### Cardinality

Every new series counts against `max_series_cardinality`, and a namespace's
against its `max_series`; past the limit, writes of new series are refused.
To see what is growing:

```bash
curl -s localhost:8086/api/v1/status/tsdb?limit=5 | jq '.data | {
  headroom: .chronix.headroom,
  measurements: .seriesCountByMetricName,
  labels: .labelValueCountByLabelName }'
```

A label whose value count climbs with the series count — a request ID, a
timestamp, a session — is the usual cause: move it into a field.

### Grafana Dashboards

Pre-built Grafana dashboards are in the `dashboards/` directory:

| Dashboard | File |
|-----------|------|
| Cluster Overview | `dashboards/cluster-overview.json` |
| Query Performance | `dashboards/query-performance.json` |
| Analytics | `dashboards/analytics.json` |
| Storage & Tiering | `dashboards/storage.json` |

Import via Grafana UI: Dashboards → Import → Upload JSON.

### Distributed Tracing

Enable OpenTelemetry trace export:

```toml
[server]
log_format = "json"

[tracing]
service_name = "chronixd"

[tracing.otlp]
endpoint = "https://otel-collector:4317"
sampling = { ratio = 0.01 }
always_sample_errors = true
```

Requires a build with the `otlp` feature (`cargo install chronixd --features otlp`);
without it the section is a startup error rather than a collector that never
receives anything.

**`always_sample_errors`** (default: `true`): When enabled, the sampler is
overridden to `AlwaysOn`, ensuring all traces — including error-bearing ones —
are captured. This is necessary because OpenTelemetry sampling decisions are
made at span creation time, before errors occur. Set to `false` and use
`sampling.ratio` for controlled sampling in high-throughput environments where
capturing every error trace is not required.

Every HTTP request gets a span, and an incoming W3C `traceparent` header
continues the caller's trace, so a query appears inside the trace of the
dashboard or service that sent it.

## Inspecting a data directory

`chronixd inspect` answers the first questions about a data directory — is it
in use, was it closed cleanly, which format generation, what does it hold —
without opening it. It writes, locks and replays nothing, so it is safe on a
directory a running server holds, on a read-only copy taken off a gateway, and
on a directory another chronix version wrote.

```console
$ chronixd inspect /var/lib/chronix
/var/lib/chronix  not closed cleanly · generation 2

holder    pid 75279 on gw-0042 · chronix 0.8.0 · opened 3 h ago
formats   catalog ✓  segment ✓  wal ✓  series ✓
series    5 · budget 1 000 000 (default)
wal       2 files · 1 912 records to replay (3.4 MiB) · seq 88 121–90 032

measurement  segments  series       size  oldest      newest
battery             4       1    3.7 KiB  2026-11-17  3 min ago
grid                4       2    3.7 KiB  2026-11-17  3 min ago
pv                 73       1   61.8 KiB  2026-11-14  15 min ago

problems  none
next      the next open replays 1 912 WAL records (3.4 MiB); nothing
          acknowledged is lost
```

The first line is the state:

| State | Meaning |
|---|---|
| `in use` | A running process on this host has it open (`holder` names it) |
| `closed cleanly` | The last process closed it; the next open replays nothing |
| `not closed cleanly` | The last process stopped without closing; the next open replays the WAL records shown |
| `state unknown` | The `LOCK` file names a process on another host — live there, or a copy taken while it ran |

A damaged file or one from another format generation is listed under
`problems` by name, and the report continues. `next` says what to do.

| Flag | |
|---|---|
| `--json` | One JSON document (`"schema": "chronix.inspect/1"`) instead of the report |
| `--fast` | Skip the `.series` sidecars: one read per segment fewer, no series counts |
| `--config PATH` | Take the cardinality budget from this server configuration |

Exit status is `0` when a report was printed, problems included, and `1` when
the path is not a chronix data directory. From Rust, `chronix::inspect::inspect`
returns the same report as a struct.

## Upgrading and the on-disk format

A data directory belongs to **one format generation**: segments, WAL,
catalog and `.series` sidecars all carry the same number, and a build reads
exactly the generation it writes.

| Generation | Written by |
|---|---|
| 1 | chronix 0.7 and earlier |
| 2 | chronix 0.8 and later — **frozen** |

**0.8 was the hard cut, and the last one.** Opening a generation-1 directory
with 0.8 fails at `open()` with a message that names both generations and
what to do; it is never reported as a checksum or corruption error. There is
no migration: run the chronix that wrote the directory, or create a new one
and re-ingest. Before upgrading, export anything a rollup tier is the only
copy of — the raw data a tier summarised may already have been retired.

**From 0.8 the format is frozen.** A minor or patch release does nothing to
a data directory: upgrading is replacing the binary. A change that needs a
new layout waits for the next major version, and that release will say so at
the top of its changelog, in bold.

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

## Troubleshooting

### Common Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| `400 NAMESPACE_QUOTA_EXCEEDED` | Tenant at its series or measurement quota | Read `/api/v1/namespaces/{ns}/usage`; raise the quota or delete unused series |
| `429 NAMESPACE_RATE_LIMITED` | Tenant over its ingestion rate | Clients retry after `Retry-After`; raise `max_ingestion_rate` if the rate is expected |
| Slow cold storage reads | Cache cold | Pre-warm cache or increase `cache.max_size_bytes` |
| TLS `failed to configure` | Invalid certs/keys | Check file paths and cert/key pairing; server starts without TLS on failure |
| `BACKPRESSURE` (503) | The memtable is at capacity and a flush is running | Slow the ingestion rate, raise `database.max_memtable_memory`, or move to faster storage. Watch `chronix_write_admission_rejected_total{reason="memtable_full"}` |
| `OVERLOADED` (503) | The WAL is poisoned by a failed `fsync` | Close and reopen the database; the file's contents are unknown after the kernel dropped its dirty pages |
| `QUERY_TIMEOUT` (504) | A read exceeded its deadline | Narrow the time range, or raise `server.sql_query_timeout_secs` / `database.query_timeout_secs` |
| `WRITE_TIMEOUT` (504) | The server stopped waiting for a write | The write may still have landed — retry it; a point is identified by its series and timestamp |
| Circuit breaker open | Unhealthy DataNode | Check target node health; after the breaker's `cooldown` it admits one probe write and closes if that succeeds |
| `retention: rollups not yet materialised` | A rollup fed by this data has not caught up, or has a repair pending | Expected and self-healing: the next maintenance pass materialises and the pass after it drops the data. Persisting means a rollup is failing — check for `rollup materialisation failed` |

### API Key Rate Limiting

API key validation failures are rate-limited to prevent brute-force attacks.

| Setting | Default | Description |
|---------|---------|-------------|
| `auth.api_key_rate_limit_max_failures` | `20` | Maximum failed validation attempts per window |
| `auth.api_key_rate_limit_window_secs` | `60` | Sliding window duration in seconds |

When the failure count for a source exceeds the threshold within the sliding
window, subsequent API key validation attempts are rejected with
`429 Too Many Requests` until the window expires.

---
