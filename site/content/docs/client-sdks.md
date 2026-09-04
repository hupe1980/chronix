+++
title = "Client SDKs"
description = "Talk to Chronix from Python, from any Arrow Flight SQL client, and from Telegraf, Prometheus and the OpenTelemetry Collector."
weight = 70
+++

Chronix provides official client SDKs for popular languages, plus protocol-level compatibility for ecosystem tools.

## Python SDK

The official Python client (`chronix-client`) is an async-first library built on `httpx`.

### Installation

```bash
pip install chronix-client
```

With pandas support:

```bash
pip install "chronix-client[pandas]"
```

### Quick Start

```python
import asyncio
from chronix_client import ChronixClient, Point, TimeRange

async def main():
    async with ChronixClient("http://localhost:8086") as client:
        # Write data points
        await client.write([
            Point(
                measurement="cpu",
                fields={"usage": 42.5, "idle": 57.5},
                tags={"host": "server-1", "region": "us-east"},
            )
        ])

        # Structured query
        result = await client.query(
            "cpu",
            TimeRange(start=0, end=2**63 - 1),
            tag_filters={"host": "server-1"},
        )
        for row in result:
            print(row)

        # SQL query
        sql_result = await client.sql(
            "SELECT host, AVG(usage) FROM cpu GROUP BY host"
        )
        df = sql_result.to_dataframe()  # Requires pandas extra

asyncio.run(main())
```

### Write Formats

**JSON writes:**
```python
await client.write([
    Point("cpu", {"usage": 42.5}, tags={"host": "a"})
])
```

**InfluxDB line protocol:**
```python
await client.write_line_protocol([
    "cpu,host=a usage=42.5 1700000000000000000",
    "mem,host=a total=16384i 1700000000000000000",
])
```

**Idempotent writes:**
```python
await client.write(
    points,
    idempotency_key="batch-001",
)
```

### PromQL Queries

```python
# Instant query
result = await client.prom_query('cpu_usage{host="server-1"}')

# Range query
result = await client.prom_query_range(
    'rate(cpu_usage[5m])',
    start="2024-01-01T00:00:00Z",
    end="2024-01-02T00:00:00Z",
    step="60s",
)

# Label discovery
labels = await client.prom_labels()
values = await client.prom_label_values("host")
```

### Arrow Flight SQL (Zero-Copy)

For maximum throughput analytical queries:

```python
import adbc_driver_flightsql.dbapi

uri = ChronixClient.flight_sql_uri("localhost", 8817)
conn = adbc_driver_flightsql.dbapi.connect(uri)
cursor = conn.cursor()
cursor.execute("SELECT * FROM cpu WHERE _time > now() - INTERVAL '1 hour'")
table = cursor.fetch_arrow_table()
df = table.to_pandas()
```

### Authentication & Multi-Tenancy

```python
client = ChronixClient(
    "http://localhost:8086",
    api_key="your-api-key",
    namespace="production",
)
```

### Exception Handling

```python
from chronix_client import ChronixError, WriteError, QueryError, ConnectionError

try:
    await client.write(points, idempotency_key="dup")
except WriteError as e:
    print(f"Write failed (HTTP {e.status_code}): {e}")
except ConnectionError as e:
    print(f"Cannot reach server: {e}")
```

---

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
