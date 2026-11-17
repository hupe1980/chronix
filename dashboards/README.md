# Chronix Grafana Dashboards

Grafana dashboards for monitoring a Chronix server through its own metrics.

## Dashboards

| File | Description |
|------|-------------|
| `cluster-overview.json` | Node status, region count, replication health, leader changes |
| `query-performance.json` | Query and write latency percentiles, throughput, plan- and scan-cache hit ratios, segment pruning |
| `analytics.json` | Forecast and anomaly-detection latency, compute engine metrics |
| `storage.json` | Disk usage, object store tiering, cache hit ratios, WAL metrics, checkpoint rate and duration |
| `ingestion.json` | Write rate, write latency percentiles, WAL size, memtable flushes, batch distribution |
| `signals.json` | Active signals, signal fire rate, delivery latency, anomaly detections, channel health |

## Setup

1. **Import into Grafana:**
   - Open Grafana → Dashboards → Import
   - Upload the JSON file or paste its contents, then pick the data source in the
     dashboard's **Data source** selector

2. **Data Source:**
   Each dashboard has a **Data source** selector; pick the Prometheus server that
   scrapes Chronix's metrics at `http://<host>:8086/metrics`. The same files work
   through Import and through file provisioning.

3. **Requirements:**
   - Grafana 10.0+
   - Prometheus data source scraping Chronix metrics endpoint

## Why a panel is empty

A metric that has never been recorded is absent from a scrape, and Grafana
draws "No data" for it.

- **Write-path counters read `0`** from startup, so an ingestion-error panel
  shows a number rather than "No data". The checkpoint counters
  (`chronix_backups_total`, `chronix_backup_failures_total`) likewise — a flat
  zero on *Checkpoints → taken* means the schedule has stopped running.
- **Feature-gated panels stay empty until that feature is configured and
  used**: the object-store panels need a cold tier, `signals.json` needs
  triggers, the analytics panels need a forecast or a detector to run.
- **`cluster-overview.json` needs the `chronixd-cluster` binary.** `chronixd`
  does not emit cluster metrics.

## Export bundled dashboards

```bash
chronixd --export-dashboards ./grafana/
```
