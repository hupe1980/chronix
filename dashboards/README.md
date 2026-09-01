# Chronix Grafana Dashboards

Pre-built Grafana dashboards for monitoring Chronix clusters.

## Dashboards

| File | Description |
|------|-------------|
| `cluster-overview.json` | Node status, region count, replication health, leader changes |
| `query-performance.json` | Query latency percentiles, scatter-gather breakdown, write throughput |
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

## Export bundled dashboards

```bash
chronixd --export-dashboards ./grafana/
```
