# chronix-client (Python)

Async Python client for the [Chronix](https://github.com/hupe1980/chronix) time-series database.

## Features

- **Async-first** — built on `httpx` for non-blocking I/O
- **JSON + Line Protocol writes** — both formats supported
- **Structured queries + SQL** — first-class support for both query APIs
- **PromQL** — instant & range queries via Prometheus-compatible endpoints
- **Arrow Flight SQL** — zero-copy queries via ADBC driver
- **Schema introspection** — list measurements, get column schemas
- **Multi-tenant** — namespace header support
- **Typed** — full type annotations with `py.typed` marker

## Installation

```bash
pip install chronix-client
```

With pandas support:

```bash
pip install "chronix-client[pandas]"
```

## Quick Start

```python
import asyncio
from chronix_client import ChronixClient, Point, TimeRange

async def main():
    async with ChronixClient("http://localhost:5555") as client:
        # Write
        await client.write([
            Point("cpu", {"usage": 42.5}, tags={"host": "a"})
        ])

        # Query
        result = await client.query(
            "cpu",
            TimeRange(start=0, end=2**63 - 1),
            tag_filters={"host": "a"},
        )
        for row in result:
            print(row)

        # SQL
        result = await client.sql("SELECT * FROM cpu ORDER BY _time DESC LIMIT 10")
        df = result.to_dataframe()  # requires pandas extra

asyncio.run(main())
```

## Exact Decimals

`f64` cannot represent `0.1`. For a meter register a bill is computed from,
write a `decimal.Decimal` and the digits stay digits — in the request body,
in storage, and in the query result:

```python
from decimal import Decimal

await client.write([
    Point("meter", {"z1nb_q": Decimal("1234.5678")}, tags={"device": "main"})
])
```

The client sends `{"z1nb_q": {"decimal": "1234.5678"}}` rather than a JSON
number, because a JSON number is parsed as a `double` at the other end. Line
protocol uses the `d` suffix: `meter,device=main z1nb_q=1234.5678d`.

A decimal column stores a fixed number of fractional digits, set when the
column is created; a value needing more is refused rather than rounded.
Declare it first when the first value might not carry the digits you mean to
keep — see [Data Model](https://hupe1980.github.io/chronix/docs/data-model/#exact-decimals-for-money-and-meters).

## Line Protocol Writes

```python
await client.write_line_protocol([
    "cpu,host=a usage=42.5 1700000000000000000",
    "mem,host=a total=16384i,used=8192i 1700000000000000000",
])
```

## PromQL Queries

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

## Arrow Flight SQL (Zero-Copy)

For high-throughput analytical queries, use Arrow Flight SQL via ADBC:

```python
import adbc_driver_flightsql.dbapi

uri = ChronixClient.flight_sql_uri("localhost", 5557)
conn = adbc_driver_flightsql.dbapi.connect(uri)
cursor = conn.cursor()
cursor.execute("SELECT * FROM cpu WHERE _time > now() - INTERVAL '1 hour'")
table = cursor.fetch_arrow_table()
df = table.to_pandas()
```

## Authentication & Multi-Tenancy

```python
client = ChronixClient(
    "http://localhost:5555",
    api_key="your-api-key",
    namespace="production",
)
```

## Idempotent Writes

```python
await client.write(
    [Point("cpu", {"usage": 42.5})],
    idempotency_key="batch-2024-01-15-001",
)
# Second call with same key → WriteError (HTTP 409)
```

## API Reference

### `ChronixClient`

| Method | Description |
|--------|-------------|
| `health()` | Health check |
| `ready()` | Readiness check |
| `server_info()` | Server metadata |
| `write(points, *, idempotency_key)` | Write points (JSON) |
| `write_line_protocol(lines, *, idempotency_key)` | Write (line protocol) |
| `query(measurement, time_range, *, tag_filters, field_columns, limit)` | Structured query |
| `sql(query)` | SQL query |
| `explain(measurement, time_range)` | Explain query plan |
| `list_measurements()` | List measurements |
| `get_schema(measurement)` | Get column schema |
| `drop_measurement(measurement)` | Drop measurement |
| `delete(measurement, time_range=None, *, tags)` | Delete data; returns a `DeleteResult` — check `.complete` |
| `prom_query(query, *, time)` | PromQL instant query |
| `prom_query_range(query, start, end, step)` | PromQL range query |
| `prom_labels()` | List Prometheus labels |
| `prom_label_values(label)` | Label values |
| `prom_series(match)` | Find series |
| `list_rollups()` | List rollup rules |
| `list_connectors()` | List connectors |
| `openapi_spec()` | Fetch OpenAPI spec |
| `flight_sql_uri(host, port)` | Build Flight SQL URI |

### Models

| Type | Description |
|------|-------------|
| `Point` | Data point with measurement, tags, fields, timestamp. A `decimal.Decimal` field is written exactly |
| `TimeRange` | **Closed** `[start, end]` in nanoseconds — both ends inclusive |
| `QueryResult` | Query result with `.rows`, `.to_dataframe()` |
| `ColumnSchema` | Column metadata (name, role, data_type) |
| `MeasurementInfo` | Measurement summary (name) |
| `ServerInfo` | Server metadata (version, uptime, measurement_count) |

### Exceptions

| Exception | When |
|-----------|------|
| `ChronixError` | Base exception (server errors) |
| `ConnectionError` | Cannot reach server |
| `WriteError` | Write failure (including idempotency conflict) |
| `QueryError` | Query/client error (4xx) |

## License

Apache-2.0 — same as Chronix.
