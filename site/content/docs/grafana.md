+++
title = "Grafana Integration"
description = "Point Grafana at Chronix as a Prometheus data source, import the bundled dashboards, and query with PromQL or Flight SQL."
weight = 80
+++

Chronix integrates natively with Grafana through multiple data source types. This guide covers setup, dashboard provisioning, and best practices.

## Data Source Options

| Data Source | Protocol | Best For |
|-------------|----------|----------|
| **Prometheus** | HTTP (PromQL) | Real-time metrics, alerting |
| **Flight SQL** | gRPC (Arrow) | High-throughput analytics, large result sets |
| **Infinity** | HTTP REST | Custom JSON queries, admin views |

## 1. Prometheus Data Source (Recommended)

Chronix exposes a fully compatible Prometheus HTTP API at `/api/v1/prom/`.

### Add Data Source

1. Navigate to **Grafana → Connections → Data Sources → Add data source**
2. Select **Prometheus**
3. Configure:

| Field | Value |
|-------|-------|
| **URL** | `http://chronix.example.com:8086/api/v1/prom` |
| **Access** | Server (default) |
| **Scrape interval** | `15s` |

4. Click **Save & Test** — should show "Data source is working"

### Custom Headers (Authentication)

If Chronix authentication is enabled:

- Under **Custom HTTP Headers**, add:
  - **Header:** `Authorization`
  - **Value:** `Bearer <your-api-key>`

For multi-tenant deployments, also add:

- **Header:** `X-Chronix-Namespace`
- **Value:** `<namespace>`

### PromQL Examples

```promql
# CPU usage rate per host
rate(cpu_usage{host=~"server-.*"}[5m])

# 95th percentile query latency
histogram_quantile(0.95, rate(query_duration_seconds_bucket[5m]))

# Active series count
chronix_active_series_total

# ingestion rate
rate(chronix_ingested_points_total[1m])
```

## 2. Flight SQL Data Source

For analytical queries returning large Arrow tables.

### Prerequisites

Install the [Grafana Flight SQL plugin](https://grafana.com/grafana/plugins/grafana-flightsql-datasource/):

```bash
grafana-cli plugins install grafana-flightsql-datasource
```

### Configuration

| Field | Value |
|-------|-------|
| **Host** | `chronix.example.com:8817` |
| **Secure** | `false` (or `true` with TLS) |
| **Auth Type** | `none` / `token` |

### SQL Examples

```sql
-- Top 10 hosts by CPU usage in the last hour
SELECT host, AVG(usage) as avg_usage
FROM cpu
WHERE time > now() - INTERVAL '1 hour'
GROUP BY host
ORDER BY avg_usage DESC
LIMIT 10

-- Storage segment count per measurement
SELECT measurement, COUNT(*) as segments
FROM _segments
GROUP BY measurement
```

## 3. Pre-Built Dashboards

Chronix ships with 6 production-ready Grafana dashboards in the `dashboards/` directory:

| Dashboard | File | Description |
|-----------|------|-------------|
| **Cluster Overview** | `cluster-overview.json` | Node health, Raft state, replication lag |
| **Ingestion** | `ingestion.json` | Write throughput, batch sizes, WAL depth |
| **Query Performance** | `query-performance.json` | Latency histograms, cache hit rates, scan stats |
| **Storage** | `storage.json` | Segment counts, compaction stats, disk usage |
| **Analytics** | `analytics.json` | Forecasting models, anomaly detections |
| **Signals** | `signals.json` | Signal triggers, alert firings |

### Manual Import

1. Navigate to **Grafana → Dashboards → Import**
2. Upload the JSON file from `dashboards/`
3. Select your Chronix Prometheus data source
4. Click **Import**

### Provisioning (Automated)

For automated deployments, use Grafana provisioning:

**`/etc/grafana/provisioning/datasources/chronix.yaml`:**
```yaml
apiVersion: 1

datasources:
  - name: Chronix
    type: prometheus
    access: proxy
    url: http://chronix:8086/api/v1/prom
    isDefault: true
    jsonData:
      httpMethod: POST
      timeInterval: "15s"
```

**`/etc/grafana/provisioning/dashboards/chronix.yaml`:**
```yaml
apiVersion: 1

providers:
  - name: chronix
    type: file
    disableDeletion: false
    updateIntervalSeconds: 60
    options:
      path: /var/lib/grafana/dashboards/chronix
      foldersFromFilesStructure: true
```

Copy the dashboard JSON files:
```bash
cp dashboards/*.json /var/lib/grafana/dashboards/chronix/
```

### Docker Compose Example

```yaml
services:
  chronix:
    image: chronix:latest
    ports:
      - "8086:8086"   # HTTP + Prometheus
      - "8087:8087"   # gRPC
      - "8817:8817"   # Flight SQL

  grafana:
    image: grafana/grafana:11
    ports:
      - "3000:3000"
    volumes:
      - ./dashboards:/var/lib/grafana/dashboards/chronix:ro
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
```

## 4. Annotations

Chronix supports Grafana annotations for overlaying events on time-series panels.

### REST Endpoint

```
GET /api/v1/annotations?from=<start_ns>&to=<end_ns>&measurement=<name>
```

### SSE Stream

For live annotations:

```
GET /api/v1/annotations/stream
```

Configure in Grafana using the **Annotations** panel settings.

## 5. Alerting

Use Grafana Alerting with the Prometheus data source:

1. Create an **Alert Rule** using PromQL
2. Set the data source to your Chronix Prometheus source
3. Configure notification channels (Slack, PagerDuty, etc.)

Example alert: *CPU usage above 90% for 5 minutes*

```promql
avg by (host) (rate(cpu_usage[5m])) > 0.90
```

## 6. CDC Live Dashboard (SSE)

Chronix supports Server-Sent Events for real-time change data capture:

```
GET /api/v1/cdc/stream
```

Use the Grafana **Live** feature or a streaming panel plugin to display real-time writes as they happen.
