+++
title = "gRPC and Flight SQL"
description = "The chronixd gRPC service and Arrow Flight SQL endpoint: methods, ports, authentication and how to connect ADBC and JDBC clients."
weight = 40
+++

`chronixd` serves a gRPC API on port 8087 and Arrow Flight SQL on port 8817. Both use the same authentication, authorization and namespace rules as HTTP; Flight SQL returns Arrow record batches, so ADBC, JDBC and Python clients read results without conversion.

## gRPC API

The gRPC service runs on a separate port (default `4243`). The proto
definition is in `crates/chronixd/proto/chronix.proto`.

### `ChronixService`

| Method | Type | Description |
|--------|------|-------------|
| `Write` | Unary | Write a batch of points |
| `StreamWrite` | Client-streaming | Stream points with server-side batching and dedup |
| `Query` | Server-streaming | Query data and stream Arrow-format results |
| `GetSchema` | Unary | Get measurement schema |
| `ListMeasurements` | Unary | List all measurements with schemas |
| `Delete` | Unary | Delete points matching predicates |
| `DropMeasurement` | Unary | Drop an entire measurement |
| `ServerInfo` | Unary | Server metadata (version, uptime, stats) |
| `ExecuteSql` | Unary | Execute SQL queries via DataFusion (streaming internally) |

**Write timeout:** gRPC `Write` and `StreamWrite` enforce the same
`write_timeout_secs` deadline. Timeouts surface as gRPC status
`DEADLINE_EXCEEDED`.

### Example (grpcurl)

```bash
grpcurl -plaintext -d '{"measurement":"cpu","points":[...]}' \
  localhost:4243 chronix.ChronixService/Write
```

---

## Flight SQL API

Arrow Flight SQL service runs on a separate port (default `4244`).
Compatible with any Flight SQL client (e.g., Grafana Flight SQL plugin,
JDBC/ODBC drivers, DBeaver).

### Query Resource Limits

Flight SQL queries enforce the same resource limits as the REST and gRPC
interfaces:

| Setting | Default | Description |
|---------|---------|-------------|
| `sql_query_timeout_secs` | `30` | Maximum seconds for SQL query execution including result streaming |
| `sql_max_rows` | `100000` | Maximum rows returned by a single SQL query |
| `prom_series_limit` | `10000` | Maximum label-sets returned by `/api/v1/series` |

Queries exceeding the timeout are cancelled. Row limits are enforced at the
batch level — once the cumulative row count reaches the limit, the final batch
is truncated via `batch.slice()` and no further batches are sent. This avoids
materializing the entire result set in memory.

### Supported Commands

| Command | Description |
|---------|-------------|
| `CommandStatementQuery` | Execute a SQL query and retrieve results |
| `CommandStatementUpdate` | Bulk write via DoPut (Arrow RecordBatch) |
| `CommandGetCatalogs` | List catalogs (returns `"chronix"`) |
| `CommandGetTableTypes` | List table types (returns `"TABLE"`) |
| `CommandGetTables` | List measurements as tables |

### Example (Python)

```python
from adbc_driver_flightsql import dbapi

conn = dbapi.connect("grpc://localhost:4244")
cursor = conn.cursor()
cursor.execute("SELECT * FROM cpu WHERE host = 'srv1' LIMIT 10")
print(cursor.fetchall())
```

---
