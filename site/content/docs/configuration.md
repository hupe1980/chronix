+++
title = "Configuration Reference"
description = "Every Chronix and chronixd configuration key: defaults, units, and what each one trades away."
weight = 90
+++

This page documents all configuration options for Chronix — both the
embedded library (`ChronixConfig`) and the standalone server (`chronixd`).

---

## Embedded Library — `ChronixConfig`

Build with `ChronixConfig::builder()`:

```rust
use chronix::ChronixConfig;
use std::time::Duration;

let config = ChronixConfig::builder()
    .data_dir("/var/lib/chronix")
    .retention(Some(Duration::from_secs(30 * 86400)))
    .compression(CompressionCodec::Lz4)
    .build()?;
```

### Core Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `data_dir` | `PathBuf` | *required* | Root directory for all data files |
| `shard_duration` | `Duration` | 1 hour | Time range per memtable shard |
| `ooo_shard_tolerance` | `u32` | 2 | Number of past shards that accept out-of-order writes |
| `retention` | `Option<Duration>` | 30 days | Global data retention period |
| `measurement_retention` | `HashMap<String, Duration>` | empty | Per-measurement retention overrides |
| `max_series_cardinality` | `usize` | 1,000,000 | Maximum number of unique series |
| `shutdown_timeout` | `Duration` | 30s | Graceful shutdown deadline |

### WAL Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `wal.fsync_policy` | `FsyncPolicy` | `PerBatch` | When to fsync: `None`, `PerBatch`, `PerEntry` |
| `wal.fsync_interval` | `Option<Duration>` | `None` | Periodic background fsync interval (e.g. `100ms`). Enables `start_periodic_sync()` |
| `wal.max_file_size` | `usize` | 32 MB | Maximum WAL file size before rotation |
| `wal.max_unflushed_wals` | `usize` | 4 | Backpressure: max WAL files before blocking writes |

### Memory & Storage

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `memtable_flush_threshold` | `usize` | 64 MB | Flush memtable when it exceeds this size |
| `max_memtable_memory` | `usize` | 256 MB | Total memory budget for all memtables |
| `segment_cache_size` | `usize` | 512 MB | LRU cache for decoded segment data |
| `enable_last_value_cache` | `bool` | `false` | Enable O(1) last-value lookups |
| `lvc_measurements` | `Option<HashSet<String>>` | `None` | Measurements to include in LVC (None = all) |

### Encoding & Compression

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `compression` | `CompressionCodec` | `Lz4` | Block compression: `None`, `Lz4`, `Zstd` |
| `float_encoding` | `FloatEncoding` | `Chimp` | Float encoding: `Gorilla`, `Chimp` |

### Compaction

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `compaction_concurrency` | `usize` | 2 | Max concurrent compaction workers |
| `storage_backend` | `StorageBackendConfig` | `LocalFs` | Storage backend: `LocalFs`, `ObjectStore` |

### Analytics

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `analytics.default_forecast_model` | `String` | `"ses"` | Default forecast model (`ses`, `holt`, `holtwinters`, `arima`) |
| `analytics.default_anomaly_method` | `String` | `"zscore"` | Default anomaly detector (`zscore`, `mad`, `iqr`) |
| `analytics.default_confidence_level` | `f64` | 0.95 | Forecast prediction interval confidence |
| `analytics.max_forecast_horizon` | `usize` | 8760 | Maximum forecast steps |
| `analytics.max_training_points` | `usize` | 1,000,000 | Maximum training data points |
| `analytics.default_anomaly_threshold` | `f64` | 3.0 | Default Z-score threshold |
| `analytics.compute_threads` | `usize` | 0 (auto) | Worker threads for analytics (0 = num CPUs) |

### Multivariate

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `multivariate.max_series_per_context` | `usize` | 100 | Max variables in a multivariate context |
| `multivariate.default_interpolation` | `String` | `"linear"` | Default interpolation for alignment |
| `multivariate.rolling_window_size` | `usize` | 1000 | Default rolling window length |
| `multivariate.pca_variance_threshold` | `f64` | 0.95 | PCA explained variance cutoff |
| `multivariate.var_max_lag` | `usize` | 10 | Maximum VAR lag order to test |

---

## Server — `chronixd` Configuration

The server is configured via a TOML file (default: `chronixd.toml`):

```toml
[server]
http_addr = "0.0.0.0:8086"
grpc_addr = "0.0.0.0:8087"
flight_addr = "0.0.0.0:8817"
log_format = "json"
log_level = "info"
max_body_size = 10485760

[database]
data_dir = "/var/lib/chronix"
compression = "lz4"
float_encoding = "chimp"
retention_secs = 2592000
segment_cache_size = 536870912
```

### Server Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `http_addr` | `SocketAddr` | `0.0.0.0:8086` | HTTP API listen address |
| `grpc_addr` | `SocketAddr` | `0.0.0.0:8087` | gRPC API listen address |
| `flight_addr` | `SocketAddr` | `0.0.0.0:8817` | Arrow Flight listen address |
| `metrics_path` | `String` | `"/metrics"` | Prometheus metrics endpoint |
| `max_body_size` | `usize` | 10 MB | Maximum HTTP request body |
| `log_format` | `String` | `"text"` | Log format: `text`, `json` |
| `log_level` | `String` | `"info"` | Log filter: `trace`, `debug`, `info`, `warn`, `error` |
| `sql_query_timeout_secs` | `u64` | 30 | SQL query timeout |
| `prom_query_timeout_secs` | `u64` | 30 | PromQL query timeout (0 = disabled) |
| `sql_max_rows` | `usize` | 100,000 | Maximum rows per SQL result |
| `per_query_memory_limit` | `usize` | 268,435,456 (256 MiB) | Per-query memory budget enforced by `MemoryTracker` |
| `max_write_batch_size` | `usize` | 50,000 | Maximum points per write request |
| `cors_allowed_origins` | `Vec<String>` | empty | CORS allowed origins |
| `authz_policy_dir` | `Option<PathBuf>` | `None` | Directory containing Cedar authorization policy files. When set, the authorization engine loads `.cedar` policies from this path on startup |

### gRPC Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `grpc_keepalive_secs` | `u64` | 60 | gRPC keepalive interval |
| `grpc_keepalive_timeout_secs` | `u64` | 20 | gRPC keepalive timeout |
| `stream_batch_size` | `usize` | 10,000 | Streaming batch size |
| `stream_batch_interval_ms` | `u64` | 100 | Streaming batch interval |
| `dedup_window_secs` | `u64` | 300 | Deduplication window |

### TLS

```toml
[tls]
cert = "/etc/chronix/server.crt"
key = "/etc/chronix/server.key"
# Optional: CA certificate for mutual TLS (client cert verification)
client_ca = "/etc/chronix/ca.crt"
# Optional: hot-reload interval in seconds (0 = disabled)
# When enabled, the server watches cert/key files and atomically
# reloads TLS configuration on change — zero-downtime cert rotation.
reload_interval_secs = 3600
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `cert` | `PathBuf` | — | Path to PEM-encoded server certificate |
| `key` | `PathBuf` | — | Path to PEM-encoded private key |
| `client_ca` | `PathBuf?` | None | CA cert for mutual TLS client verification |
| `reload_interval_secs` | `u64` | 0 | Cert/key file change check interval (0=off) |

**CLI equivalents:**

```bash
chronixd --tls-cert /path/to/cert.pem \
         --tls-key /path/to/key.pem \
         --tls-client-ca /path/to/ca.pem \
         --tls-reload-interval-secs 3600
```

### Authentication

```toml
[auth]
exempt_paths = ["/health", "/ready", "/metrics"]

[[auth.api_keys]]
name = "ingest"
key = "$CHRONIX_INGEST_KEY"      # env-var reference, Argon2 PHC string, or plain text

[auth.jwt]
secret = "your-jwt-secret"
algorithm = "HS256"
issuer = "https://auth.example.com"     # optional
audience = "chronix"                    # optional
role_claim = "realm_access.roles"       # optional; defaults to "roles"
```

An API key carries a principal name and nothing else. Roles come from JWT
claims, so Cedar policies written against roles apply to JWT principals; an
API-key principal matches only policies written against its name.

### Cluster

```toml
[cluster]
mode = "data"           # "meta" or "data"
node_id = 1
raft_bind_addr = "0.0.0.0:9000"
cluster_peers = ["node2:9000", "node3:9000"]
meta_addrs = ["meta1:9000", "meta2:9000", "meta3:9000"]
```

### Connectors

```toml
[kafka]
brokers = "kafka1:9092,kafka2:9092"
group_id = "chronix-ingest"
topics = ["metrics"]
format = "line_protocol"

[mqtt]
broker = "mqtt.example.com"
port = 1883
client_id = "chronix-1"
topics = ["sensors/#"]
qos = 1
format = "json"
```

### Observability

```toml
[tracing]
service_name = "chronix"
log_format = "json"
log_filter = "info"

[tracing.otlp]
endpoint = "http://jaeger:4317"
sampling = { ratio = 0.01 }
always_sample_errors = true
```

---

## Environment Variables

| Variable | Override |
|----------|---------|
| `CHRONIX_DATA_DIR` | `database.data_dir` |
| `CHRONIX_LOG_LEVEL` | `server.log_level` |
| `CHRONIX_HTTP_ADDR` | `server.http_addr` |
| `CHRONIX_JWT_SECRET` | `auth.jwt.secret` |
