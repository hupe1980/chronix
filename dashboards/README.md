# Chronix Grafana Dashboards

Pre-built Grafana dashboards for monitoring Chronix clusters.

## Dashboards

| File | Description |
|------|-------------|
| `cluster-overview.json` | Node status, region count, replication health, leader changes |
| `query-performance.json` | Query and write latency percentiles, throughput, plan- and scan-cache hit ratios, segment pruning |
| `analytics.json` | Forecast/anomaly latency, GPU utilization, compute engine metrics |
| `storage.json` | Disk usage, object store tiering, cache hit ratios, WAL metrics |
| `ingestion.json` | Write rate, write latency percentiles, WAL size, memtable flushes, batch distribution |
| `signals.json` | Active signals, signal fire rate, delivery latency, anomaly detections, channel health |

## Setup

1. **Import into Grafana:**
   - Open Grafana → Dashboards → Import
   - Upload the JSON file or paste its contents
   - Select a Prometheus data source configured to scrape Chronix metrics

2. **Data Source:**
   All dashboards expect a Prometheus data source named `Prometheus`.
   Chronix exposes metrics at `http://<host>:8086/metrics` by default.

3. **Requirements:**
   - Grafana 10.0+
   - Prometheus data source scraping Chronix metrics endpoint

## Why a panel is empty

A metric that has never been recorded is absent from a scrape, and Grafana
draws "No data" for it.

- **Write-path counters read `0`** from startup, so an ingestion-error panel
  shows a number rather than "No data".
- **Feature-gated panels stay empty until that feature is configured and
  used**: the object-store panels need a cold tier, `signals.json` needs
  triggers, the analytics panels need a forecast or a detector to run.
- **`cluster-overview.json` needs a `--features cluster` build.** The
  distributed tier is excluded from the default build.

## Export bundled dashboards

```bash
chronixd --export-dashboards ./grafana/
```
