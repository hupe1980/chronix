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
| `ooo_shard_tolerance` | `u32` | 2 | Number of past shards that accept out-of-order writes. Writes outside the window use `backfill()` instead |
| `future_write_tolerance` | `Duration` | 1 h | How far ahead of the wall clock a timestamp may be, on every path including `backfill()`. The out-of-order window is anchored on the newest *admitted* write, so without this bound one point from a device with a broken clock moved the anchor into the future and every real write after it was rejected until the next restart |
| `retention` | `Option<Duration>` | `None` (keep forever) | Global data retention period. The maintenance pass enforces **every** configured rule — this one, `measurement_retention`, and each rollup's own `retention_ns` — so per-measurement rules take effect whether or not a global one is set |
| `measurement_retention` | `HashMap<String, Duration>` | empty | Per-measurement retention overrides |
| `max_series_cardinality` | `usize` | 1,000,000 | Maximum number of unique series |
| `query_timeout` | `Duration` | 60 s | Deadline for one read. Checked before each time bucket's segment I/O, so a scan that runs out of time stops rather than finishing and then reporting. `Duration::ZERO` disables |
| `per_query_memory_limit` | `usize` | 256 MiB | Memory budget for one query's intermediate state |
| `max_query_result_bytes` | `usize` | 256 MiB | Ceiling on the bytes one collected result may hold |

### WAL Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `wal.fsync_policy` | `FsyncPolicy` | `PerBatch` | `FsyncPolicy::PerWrite`, `PerBatch`, or `Periodic(Duration)` |
| `wal.fsync_interval` | `Option<Duration>` | `None` | Periodic background fsync interval (e.g. `100ms`). Enables `start_periodic_sync()` |
| `wal.max_file_size` | `usize` | 32 MB | Maximum WAL file size before rotation |
| `wal.max_unflushed_wals` | `usize` | 4 | Backpressure: max WAL files before blocking writes |

### Memory & Storage

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `memtable_flush_threshold` | `usize` | 64 MB | Flush memtable when it exceeds this size |
| `max_memtable_memory` | `usize` | 256 MB | Total memory budget for all memtables |
| `segment_cache_size` | `usize` | 512 MB | LRU cache for decoded segment data |
| `cdc_capacity` | `usize` | 65 536 | CDC events buffered for a slow subscriber. The ring (~112 bytes/event) is allocated on the first subscription, so a deployment that never reads the CDC stream pays nothing. `open_small` sets 4 096 |
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
| `maintenance_interval` | `Duration` | 30 s (60 s in the small preset) | How often the built-in maintenance thread compacts, materialises rollups, collects garbage and enforces retention; `0` disables the periodic passes (flush-on-demand stays) |

### Analytics

Two bounds, both enforced by the SQL forecast aggregates. The model, the
detector and the confidence level are arguments to the analytics API rather
than settings, so a per-call choice stays a per-call choice.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `analytics.max_forecast_horizon` | `usize` | 8760 | Most points one `forecast()` may predict. A call over it is a planning error naming this setting — refused rather than clamped, because a silently shortened forecast is a wrong answer |
| `analytics.max_training_points` | `usize` | 1,000,000 | Most input points one series feeds a model; the **newest** are kept. `0` means no bound |

---

## Server — `chronixd` Configuration

The server is configured via a TOML file (default: `chronixd.toml`). It is a
set of named sections: `[server]` holds the process-level scalars,
`[database]` is the embedded database, and every other table switches a
subsystem on by being present.

**A key or a section the server does not know is a startup error naming it** —
not a warning, and not silence.

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
maintenance_interval_secs = 30
ooo_shard_tolerance = 2
future_write_tolerance_secs = 3600
soft_delete_ttl_secs = 86400
```

### Database Settings

`[database]` maps to the embedded `ChronixConfig`, so everything in the
**Embedded Library** tables above applies to a server deployment too. Each key
below is **optional; omitting it keeps the engine's default**, which is why no
second default is written here.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maintenance_interval_secs` | `u64` | engine default (30 s) | How often the maintenance thread compacts, materialises rollups, collects garbage and enforces retention. `0` disables the periodic passes; flush-on-demand stays. On flash-backed storage a longer interval is fewer writes |
| `ooo_shard_tolerance` | `u32` | engine default (2) | Past shards that still accept live out-of-order writes. Anything older needs `?backfill=true` |
| `future_write_tolerance_secs` | `u64` | engine default (1 h) | How far ahead of the wall clock a timestamp may be, on every write path including backfill |
| `cdc_capacity` | `usize` | engine default (65 536) | Change events buffered for a slow subscriber. The ring is allocated on the first subscription, so a deployment that never reads the change stream pays nothing |
| `wal_max_unflushed` | `usize` | engine default | Unflushed WAL files tolerated before writes are held back |
| `soft_delete_ttl_secs` | `u64` | unset — drops are immediate | Grace period before a dropped measurement is hard-deleted. With it set, `DELETE /api/v1/measurements/{name}` is recoverable until the deadline passes; without it the drop is irreversible. Measured from the wall clock capped by the newest timestamp held, as retention is, so a clock jump cannot close the window early |
| `lvc_measurements` | `[String]` | empty — all measurements | Measurements the last-value cache covers, when `enable_last_value_cache` is on |

### Server Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `http_addr` | `SocketAddr` | `0.0.0.0:8086` | HTTP API listen address |
| `grpc_addr` | `SocketAddr` | unset — port `8087` on `http_addr`'s host | gRPC API listen address. Leave it unset and gRPC listens wherever HTTP does, so `--bind 127.0.0.1:8086` keeps every listener on loopback; set it to put gRPC somewhere else |
| `flight_addr` | `SocketAddr` | unset — port `8817` on `http_addr`'s host | Arrow Flight SQL listen address, with the same rule as `grpc_addr` |
| `authz_policy_dir` | `Option<PathBuf>` | `None` | Directory of Cedar `.cedar` policies; absent = open mode |
| `backup_root` | `Option<PathBuf>` | `<data_dir>/backups` | Root every admin backup/restore path is resolved inside |
| `multi_tenancy` | `bool` | `false` | Namespace isolation on every read and write |
| `metrics_path` | `String` | `"/metrics"` | Prometheus metrics endpoint |
| `max_body_size` | `usize` | 10 MB | Maximum HTTP request body |
| `log_format` | enum | `"text"` | Log format: `text`, `json`, `compact`. A value that is none of these is a startup error, not plain text |
| `log_level` | `String` | `"info"` | Log filter: `trace`, `debug`, `info`, `warn`, `error` |
| `sql_query_timeout_secs` | `u64` | 30 | SQL query timeout |
| `prom_query_timeout_secs` | `u64` | 30 | PromQL query timeout (0 = disabled) |
| `sql_max_rows` | `usize` | 100,000 | Maximum rows per SQL result |
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
| `reload_interval_secs` | `u64` | 60 | Seconds between checks for a rotated cert/key; `0` disables. **On by default** — a certificate that expires without a reload is an outage with no warning |

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
namespaces = ["tenant-a"]        # namespaces this key may act in; empty = all
admin = false                    # restore, namespace and key management

[auth.jwt]
secret = "your-jwt-secret"       # HS256/HS384/HS512 only
algorithm = "HS256"
issuer = "https://auth.example.com"     # optional
audience = "chronix"                    # optional
role_claim = "realm_access.roles"       # optional; defaults to "roles"

# For the asymmetric families, set a key file instead of a secret:
# algorithm = "RS256"                   # or PS*, ES256, ES384, EdDSA
# public_key_pem_file = "/etc/chronix/jwt-public.pem"
# jwks_url = "https://auth.example.com/.well-known/jwks.json"
```

An API key carries a principal name, the namespaces it may act in, and
whether it is an administrator. Roles come from JWT claims, so Cedar policies
written against roles apply to JWT principals; an API-key principal matches
only policies written against its name. A JWT may carry `namespaces` (an
array, or a single string) and `admin` as claims.

**`namespaces` is not optional under multi-tenancy.** A key naming none is
unrestricted and reads every tenant, so `chronixd` refuses to start with
`multi_tenancy = true` while one exists, naming it in the error.

**`admin` gates the administrative endpoints** — restore, namespace
management and key management — on any deployment without a Cedar policy
directory.

The verification key follows the algorithm's family. `HS*` reads `secret`;
`RS*`, `PS*`, `ES*` and `EdDSA` read `public_key_pem_file`; `jwks_url`
replaces the file for providers that publish and rotate their own keys, and
is fetched once at startup. Pairing an algorithm with the other family's key
material is a startup error.

### Audit log

```toml
[audit]
path = "/var/log/chronix/audit.jsonl"
hmac_key_env = "CHRONIX_AUDIT_KEY"   # names the env var holding the key
sync_each = true                     # fsync after every event
```

Without this section the audit trail lives only in the process log, which
does not survive a restart and cannot be shown to be unedited.

The hash chain **continues across restarts**, anchoring on the last sealed
event in the file. Without the key it is a bare SHA-256, which detects
corruption but not tampering. The key comes from the environment rather than
the config, so reading the config does not confer the ability to forge
history.

### Backup root

```toml
[server]
backup_root = "/var/lib/chronix/backups"   # defaults to <data_dir>/backups
```

Every path the admin backup and restore endpoints accept is resolved inside
this root, following symlinks. An absolute path outside it is refused.

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
username = "chronix"                       # or credential_file, which rotates
password = "$CHRONIX_MQTT_PASSWORD"
```

A connector carries no request, so its `namespace` comes from its own
configuration. Under `multi_tenancy = true` its points are tagged with that
namespace, `default` included.

`credential_file` holds `{"username": "...", "password": "..."}` and takes
precedence over the inline pair, so credentials rotate without a config
change.

**MQTT TLS is not available in this build** — `rumqttc`'s rustls feature
pulls in an advisory-capped `rustls-webpki` and an unmaintained
`rustls-pemfile`. Setting `ca_cert`, `client_cert` or `client_key` is
refused at startup rather than ignored, so a connector never sends its
password over a link the operator believes is encrypted. Terminate TLS in
front of the broker.

### Cold archiving

Absent by default, and requires a binary built with `--features object-store`;
a `[cold_archive]` section without it is a startup error.

```toml
[cold_archive]
remote_url = "s3://bucket/chronix"   # or gs://, az://, file://
cold_after_secs = 2592000            # 30 days
interval_secs = 3600                 # one pass per hour
max_objects_per_run = 8
```

`ArchiveOutcome.more_pending` is `true` when the bound stopped a pass with work
still eligible. The server's periodic task picks it up on the next tick; a
caller driving `archive_cold_segments` itself should call again.

Each pass archives up to `max_objects_per_run` complete `(measurement, shard)`
groups and stops; the next pass continues. A failed pass is logged and counted
in `chronix_cold_archive_pass_failures_total` — not fatal, and the data stays
hot and queryable. See [Object Storage](/reference/object-storage/) for what a
group is and when it is eligible.

### Signal triggers

Absent by default; the trigger endpoints answer `404` until it is present.

```toml
[triggers]
catalog_path = "triggers.json"          # relative to data_dir; survives restart
webhook_signing_secret = "$CHRONIX_WEBHOOK_SIGNING_SECRET"
webhook_timeout_secs = 10
webhook_allow_private_targets = false   # see below
signal_store_capacity = 10000
```

`webhook_signing_secret` is configuration rather than part of the `DELIVER`
clause: a secret written in SQL lands in the trigger catalog on disk and in
every `SHOW TRIGGERS`. Without it, `DELIVER webhook(…)` is refused at creation.

`webhook_allow_private_targets` waives the *resolved-address* rule for an
alerting endpoint on your own network — `https://alertmanager.corp.example/`
resolving to 10.x. It does not waive the https, userinfo or literal-address
checks.

### Observability

Log format and level are `[server]` settings; `[tracing]` only says where
traces go.

```toml
[server]
log_format = "json"   # text | json | compact
log_level  = "info"

[tracing]
service_name = "chronix"

[tracing.otlp]
endpoint = "https://jaeger:4317"
sampling = { ratio = 0.01 }        # or "always_on" / "always_off"
always_sample_errors = true
```

OTLP export needs `--features otlp`. A `[tracing.otlp]` section in a build
without it is a startup error, as is a `[tracing]` section naming no target.

---

## Checking a file before a deploy

```bash
chronixd --config /etc/chronix/chronixd.toml --check-config
```

Loads the file, applies the environment and the flags, runs every startup
validation, prints what it resolved to, and exits without opening the database
or binding a port. An unknown key, a `[tracing]` section this build cannot
honour, an `[auth]` section that authenticates nobody and two listeners on one
address are all refused here.

```text
configuration is valid
  http_addr        127.0.0.1:8086
  grpc_addr        127.0.0.1:8087
  flight_addr      127.0.0.1:8817
  data_dir         /var/lib/chronix
  log_format       Json
  log_level        info
  sql_max_rows     100000
  max_body_size    10485760
  multi_tenancy    false
  sections         tls, auth, audit
```

---

## Environment Variables

Precedence is **file, then environment, then CLI flags**.

| Variable | Override |
|----------|---------|
| `CHRONIX_DATA_DIR` | `database.data_dir` |
| `CHRONIX_LOG_LEVEL` | `server.log_level` |
| `CHRONIX_HTTP_ADDR` | `server.http_addr` |
| `CHRONIX_JWT_SECRET` | `auth.jwt.secret` — overrides an existing `[auth.jwt]` section; with no such section the server refuses to start rather than silently ignoring it |
| `CHRONIX_WEBHOOK_SIGNING_SECRET` | `triggers.webhook_signing_secret` |

A value that cannot be parsed is a startup error, not a fall back to the
default.

### `$VAR` in secret-bearing settings

Separately from the table above, any field that holds a credential accepts a
`$VAR` or `${VAR}` reference resolved at startup — API keys
(`auth.api_keys[].key`), and connector credentials (`username`, `password`,
`sasl_username`, `sasl_password`). The variable name is yours to choose:

```toml
[mqtt]
broker = "mqtt://broker.example:1883"
topics = ["sensors/#"]
username = "chronix"
password = "$CHRONIX_MQTT_PASSWORD"
```

A referenced variable that is not set is a startup error for a connector, and
a skipped key with a warning for an API key.

