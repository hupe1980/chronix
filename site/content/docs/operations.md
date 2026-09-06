+++
title = "Operations"
description = "Deploy and run Chronix: embedded and server modes, the full configuration reference, Prometheus metrics, Grafana dashboards, retention and rollups, backup and restore."
weight = 40
+++

Deploying, configuring and monitoring Chronix.

## Deployment

### Single-Node (Embedded)

```rust
use chronix::Chronix;

let db = Chronix::builder()
    .data_dir("/var/lib/chronix")
    .build()
    .await?;
```

### Single-Node Server

```bash
# Install
cargo install chronixd

# Run with defaults (data in ./chronix-data, listen on 0.0.0.0:8086)
chronixd

# Custom configuration
chronixd --config /etc/chronix/chronixd.toml
```

### Multi-Node Cluster

The distributed tier is frozen and excluded from the default build; it needs
`--features cluster`. See [Cluster](/docs/cluster/).

## Configuration Reference

`chronixd --config /etc/chronix/chronixd.toml`. Unknown keys are a startup
error, not a warning: every configuration struct is `deny_unknown_fields`, so
a typo fails loudly instead of being ignored.

### Server — `[server]`

Every setting below lives under a `[server]` table. A key the server does not
know is a startup error naming it.

| Setting | Default | Description |
|---------|---------|-------------|
| `http_addr` | `0.0.0.0:8086` | REST, PromQL, Prometheus and OTLP endpoints |
| `grpc_addr` | `0.0.0.0:8087` | gRPC service |
| `flight_addr` | `0.0.0.0:8817` | Flight SQL |
| `metrics_path` | `/metrics` | Prometheus scrape path |
| `metrics_require_auth` | `true` | Set `false` to allow unauthenticated scraping |
| `max_body_size` | `10485760` | Maximum HTTP request body, bytes |
| `log_format` / `log_level` | `text` / `info` | Logging |
| `shutdown_timeout_secs` | `30` | Graceful drain deadline |
| `public_url` | — | Absolute base URL for the generated OpenAPI `servers` entry. Leave unset behind a reverse proxy |

### Storage — `[database]`

```toml
[database]
data_dir = "/var/lib/chronix"
wal_fsync_policy = "per_batch"
compression = "zstd"
zstd_level = 3
```

| Setting | Default | Description |
|---------|---------|-------------|
| `data_dir` | `./chronix-data` | WAL, segments, manifest |
| `wal_fsync_policy` | `per_batch` | `per_write`, `per_batch`, or `periodic_<ms>` |
| `wal_max_file_size` | `64MB` | WAL rotation threshold |
| `memtable_flush_threshold` | — | Rows before a memtable is flushed |
| `max_memtable_memory` | — | Memtable memory ceiling, bytes |
| `shard_duration_secs` | `86400` | Time-shard width |
| `retention_secs` | `0` | Global retention; `0` disables |
| `compression` | `lz4` | `lz4`, `zstd`, or `none` |
| `zstd_level` | `3` | Zstd level 1–22; only used with `compression = "zstd"` |
| `zstd_dict_training` | `false` | Train a per-segment Zstd dictionary from the first row group. Helps where blocks are small and repetitive; costs a training pass per segment |
| `float_encoding` | `chimp` | `chimp`, `gorilla`, or `plain` |
| `segment_cache_size` | `512MB` | Decoded-segment cache |
| `enable_last_value_cache` | `false` | Sub-µs last-value reads (~400 ns; otherwise a memtable scan) |
| `compaction_concurrency` | — | Parallel compaction tasks |
| `max_series_cardinality` | — | Cardinality budget |
| `query_timeout_secs` | `60` | Deadline for one **native** read — `/api/v1/chronix/query`, exports, every engine scan. Checked before each time bucket's segment I/O, so it stops the reading. `0` disables |
| `per_query_memory_limit` | `268435456` | Memory budget for one query's intermediate state (DataFusion's pool spills past it) |
| `max_query_result_bytes` | `268435456` | Ceiling on the bytes one collected native result may hold |

`wal_fsync_policy = "periodic_5000"` coalesces fsyncs onto a background thread
at that interval instead of syncing per write. On flash-backed storage the
fsync rate is the wear rate, which is what the policy is for; the cost is that
up to one interval of writes is lost on an unclean shutdown. Deletes are
synced regardless of policy. The `chronix_wal_fsync_total` counter is the
observable form of the setting.

### Query limits

| Setting | Default | Description |
|---------|---------|-------------|
| `sql_query_timeout_secs` | `30` | SQL execution deadline, including result streaming. `504 QUERY_TIMEOUT` |
| `prom_query_timeout_secs` | `30` | PromQL execution deadline. The budget travels **into** the evaluator and is checked between range-query steps and before each selector scan, so it stops the work rather than only the waiting for it |
| `sql_max_rows` | `100000` | Maximum rows a SQL query returns |
| `max_range_query_points` | `11000` | Point budget for a PromQL range query |
| `prom_series_limit` | `10000` | Maximum label-sets from `/api/v1/series` |
| `prom_max_result_bytes` | `268435456` | Byte budget for an accumulated range-query result; `0` disables |

**Three deadlines, three engines.** `sql_query_timeout_secs` bounds
DataFusion, `prom_query_timeout_secs` bounds the PromQL evaluator, and
`database.query_timeout_secs` bounds the engine's own scan — the one the
native query endpoint and every export use. A deployment that raises only the
first leaves the other two at their defaults. All three are enforced *inside*
the work, so a query that runs out of time stops rather than being abandoned
while it continues.

### Write limits

| Setting | Default | Description |
|---------|---------|-------------|
| `max_write_batch_size` | `100000` | Maximum points per write request |
| `write_timeout_secs` | `30` | Deadline for any write; `0` disables |
| `stream_batch_size` | `10000` | Server-side batch size for gRPC `StreamWrite` |
| `stream_batch_interval_ms` | `100` | Server-side batch interval |
| `dedup_window_secs` | `300` | `StreamWrite` deduplication window; `0` disables |
| `max_dedup_entries` | `1000000` | Dedup cache ceiling (~24 bytes/entry) |

The JSON and Line Protocol write routes all enforce `max_write_batch_size`
and reject an oversized request with `400`. Every write path — REST, Line
Protocol, OTLP, Prometheus remote write, gRPC, Flight SQL `DoPut`, and the
Kafka and MQTT connectors — is wrapped in `write_timeout_secs` and answers
`504` or `DEADLINE_EXCEEDED` when it expires.

### Rate limiting

| Setting | Default | Description |
|---------|---------|-------------|
| `rate_limit_rps` / `rate_limit_burst` | — | Global request rate |
| `per_user_rate_limit_rps` / `per_user_rate_limit_burst` | — | Per-principal request rate |

### Multi-tenancy

```toml
[server]
multi_tenancy = true
```

Off by default. When on, every ingested point is stamped with the namespace of
the request that wrote it — `X-Namespace` for HTTP, `x-namespace` metadata for
gRPC and Flight SQL — and every read is confined to that namespace: REST, SQL,
PromQL, the Prometheus label and series endpoints, Prometheus remote read,
Flight SQL and gRPC. A client-supplied `__namespace__` tag is overwritten, and
the tag never appears in a result or a schema.

**Decide before ingesting.** Points written with tenancy off carry no
namespace and are invisible to a scoped read; points written with it on are
not addressable with it off. Switching a populated server is a re-ingest.

Ingestion connectors carry no request, so each takes its namespace from its
own configuration:

```toml
[kafka]
namespace = "tenant-a"
```

Namespaces and their quotas are managed through `/api/v1/namespaces` and
persisted to `{data_dir}/namespaces.json` with atomic writes, so they survive
a restart.

Measurement *names* are still process-wide: a tenant can learn that another
tenant's measurement exists, and reads no rows from it.

### TLS — `[tls]`

```toml
[tls]
cert = "/etc/chronix/server.crt"
key = "/etc/chronix/server.key"
client_ca = "/etc/chronix/ca.crt"   # optional; enables mTLS
reload_interval_secs = 60           # default; polls the files and hot-reloads, 0 disables
```

### Authentication and authorization

```toml
[[auth.api_keys]]
name = "ingest"
key = "$CHRONIX_INGEST_KEY"   # env-var reference, Argon2 PHC string, or plain text
namespaces = ["tenant-a"]     # required when multi_tenancy = true
admin = false                 # required for restore, namespace and key management

[auth.jwt]
# issuer, audience, and either a secret (HS*), public_key_pem_file, or jwks_url

[server]
authz_policy_dir = "/etc/chronix/policies"   # Cedar policies; absent = open mode

[audit]
path = "/var/log/chronix/audit.jsonl"
hmac_key_env = "CHRONIX_AUDIT_KEY"
```

`auth.exempt_paths` defaults to the health endpoints. Cedar is default-deny
once `authz_policy_dir` is set.

Two settings fail **open** if left out, so check them before going live:

- A key with no `namespaces` under `multi_tenancy = true` reads every
  tenant. The server refuses to start rather than allow it.
- A key with no `admin` cannot reach restore, namespace management or key
  management. Grant it deliberately, to the one key that needs it.

Without an `[audit]` section the audit trail lives only in the process log.
With one it is `fsync`ed, and its hash chain continues across restarts —
a chain that restarts looks exactly like one that was truncated.

### Cluster — `[cluster]`

Requires `--features cluster`; the tier is frozen and not part of the default
build.

| Setting | Description |
|---------|-------------|
| `mode` | Node role |
| `node_id` | Unique node identifier |
| `raft_bind_addr` | Raft listener |
| `cluster_peers` | Peer addresses |
| `meta_addrs` | MetaNode addresses |

### Cold archive

Archiving is an embedded API call — `Chronix::archive_cold_segments` behind the
`object-store` feature — not a server setting. It reads each cold
`(measurement, shard)` group **through the read path** — deduplicated, with
tombstones applied — writes it to object storage as one Parquet object under
`measurement=<m>/shard=<n>/`, verifies it, and only then removes the source
segments from the hot database. The object carries the hot tier's schema,
`_time` included, so a query moved to the archive is the same query. It is
queried as its own SQL table via `register_cold_tier` (one table per
measurement), or directly with DuckDB, Polars or Spark. See
[Object Storage](/reference/object-storage/).

### Parquet export

Exports are restricted to `{data_dir}/exports/`. Only the filename component of
a requested path is accepted; `../` traversal is rejected.

### Partial Deletes

**Metric:** `chronix_delete_segments_skipped_total` counter.

A predicate delete skips segments it cannot open or read rather than aborting,
so a non-zero value means matching data may still be on disk. Alert on any
increase if you rely on deletes for an erasure obligation:

```yaml
- alert: ChronixPartialDelete
  expr: increase(chronix_delete_segments_skipped_total[15m]) > 0
  annotations:
    summary: "A delete could not scan every segment — data may remain"
```

The same signal is returned inline as `segments_skipped` on the HTTP and gRPC
delete responses, so callers can retry without waiting for a scrape.

### Tombstone backlog

**Metric:** `chronix_tombstone_gc_total` counter.

A delete writes tombstones; compaction is what removes the rows from disk. A
tombstone is reclaimed only once every segment it was issued against has been
rewritten or deleted, so the gap between "the delete returned" and "the bytes
are gone" is one compaction cycle for the affected segments. Two operational
consequences:

- **An erasure obligation is not discharged by the delete alone.** If you must
  be able to say the bytes are gone, force a compaction of the affected
  measurement and confirm `chronix_tombstone_gc_total` advanced.
- **A tombstone set that never shrinks** means compaction is not reaching those
  segments — check `chronix_compaction_tasks_completed_total` and the L0 depth
  rather than the delete path.

Deletes themselves are durable as soon as they return: tombstones are appended
to the catalog manifest and fsynced, not left to the data WAL, which is
truncated on the next flush.

### PromQL scan cache

**Metrics:** `chronix_promql_scans_total` and
`chronix_promql_scan_cache_hits_total`, both labelled `kind="instant"|"range"`.

A range query is meant to read its window **once** rather than once per step.
Whether it does depends on the shape of the query, so the ratio is the signal:

```yaml
- alert: ChronixPromQLScanCacheIneffective
  expr: |
    rate(chronix_promql_scan_cache_hits_total{kind="range"}[10m])
      / rate(chronix_promql_scans_total{kind="range"}[10m]) < 0.5
  annotations:
    summary: "Range queries are re-reading their window per step"
```

A low ratio on `kind="range"` points at a dashboard issuing many-step queries
whose selectors miss the cache; a low ratio on `kind="instant"` is expected and
carries no information, since an instant query has one step.

### Retention and Rollups

Retention (`retention_secs`, and `measurement_retention` per measurement) is
enforced by the database's own maintenance thread every
`maintenance_interval` (30 s by default). It is opt-in: with no retention
configured, nothing is ever deleted.

Rollups are materialised, never computed as a by-product of compaction or
retention. A bucket is aggregated once, over a deduplicated scan of its
source, as soon as its input is final: for a raw measurement, once the
out-of-order window (±2 shards of the newest write) has closed over it; for
a tier fed by another rollup, as far as that rollup has been materialised.
`compact()` — which the maintenance thread runs every `maintenance_interval`
— materialises after each pass, `enforce_retention()` materialises before
it drops, and `db.materialise_rollups()` is the explicit call. Definitions
and watermarks live in the **catalog**, fsynced before the call that changed it
returns and carried forward by every snapshot, so a rollup cannot be lost to a
half-written file beside the data directory.

**Late writes are repaired, not ignored.** A `backfill` or a delete into a
range a rollup has already aggregated records an *invalidation*; the next
pass deletes the stale aggregates over that range, recomputes them, and
cascades the repair to any tier downstream. `refresh_rollup(name, start,
end)` does the same immediately, for a change the engine could not have
observed — an out-of-band restore, say. `chronix_rollup_invalidations_total`
counts what has been marked, `chronix_rollup_ranges_recomputed_total` what
has been repaired, and `chronix_rollup_failures_total` any rollup whose pass
failed — the others still run.

Rollup-aware retention trades raw data for its aggregates, so a segment is
only dropped once **every** tier of its rollup chain has been materialised
past it and has no repair pending — for the per-measurement rules as well as
the global one. A segment that is still needed is preserved and counted in
`chronix_retention_segments_awaiting_rollup_total`; a segment that cannot be
read fails the materialisation that needs it, so it is preserved too.
`chronix_rollup_points_written_total` counts what materialisation wrote.
Cold archiving removes a segment from the hot database, so it waits on the
same gate (`chronix_cold_archive_groups_awaiting_rollup_total`).

Retention runs whenever **any** rule is configured — global,
per-measurement, or a rollup's own `retention_secs`.

A rollup's `retention_secs` (`retention_ns` in the Rust API) is the
retention of its *target* measurement, applied by the same pass.

### Region Auto-Split Process

Chronix uses a **two-phase split** to ensure zero downtime during region splits:

**Phase 1 — Prepare:**
1. Source region transitions to `Splitting` state (still accepts reads **and** writes)
2. Two child regions are created in `Replicating` state covering each half of the hash range
3. Data begins replicating to children

**Phase 2 — Commit:**
1. Routing table atomically updated: source removed, children promoted to `Active`
2. Reads and writes redirect to the new child regions
3. Old region metadata cleaned up

The split triggers when either `region_size_threshold` or `region_series_threshold` is exceeded.
Monitor splits via the `chronix_cluster_region_splits_total` counter metric.

## Monitoring

### Prometheus Metrics

Chronix exposes Prometheus metrics at `/metrics` (default port 8086).

#### Cluster Health

| Metric | Type | Description |
|--------|------|-------------|
| `chronix_cluster_nodes_total` | Gauge | Total registered nodes |
| `chronix_cluster_regions_total` | Gauge | Total regions |
| `chronix_cluster_under_replicated_regions` | Gauge | Regions below replication factor |
| `chronix_cluster_leader_changes_total` | Counter | Leader election events |

#### Performance

| Metric | Type | Description |
|--------|------|-------------|
| `chronix_cluster_query_latency_seconds` | Histogram | Query latency |
| `chronix_cluster_write_latency_seconds` | Histogram | Write latency |
| `chronix_cluster_heartbeat_latency_seconds` | Histogram | Heartbeat RTT |
| `chronix_raft_log_replication_lag` | Histogram | Replication lag |

#### Object Store

| Metric | Type | Description |
|--------|------|-------------|
| `chronix_objstore_put_duration_seconds` | Histogram | Object put latency |
| `chronix_objstore_get_duration_seconds` | Histogram | Object get latency |
| `chronix_objstore_cache_hits_total` | Counter | Local cache hits |
| `chronix_objstore_cache_misses_total` | Counter | Local cache misses |

#### Segment I/O

| Metric | Type | Description |
|--------|------|-------------|
| `chronix_segment_prefetch_bytes_total` | Counter | Bytes successfully prefetched via mmap madvise |
| `chronix_segment_prefetch_errors_total` | Counter | Prefetch madvise failures |

#### Streaming Aggregation

| Metric | Type | Description |
|--------|------|-------------|
| `chronix_wire_non_finite_samples_skipped_total` | Counter | NaN/Infinity values dropped from streaming aggregation |

#### Self-maintenance

Flushing, compaction, rollup materialisation and retention all run on one
thread per open database. **Alert on `chronix_maintenance_running == 0`**: a
process that has lost it looks healthy until the memtable fills, after which
only a restart recovers. `/ready` answers `503` naming the condition, and a
full memtable with no thread to flush it is `503 OVERLOADED` rather than
retryable back-pressure.

| Metric | Type | Description |
|--------|------|-------------|
| `chronix_maintenance_running` | Gauge | `1` while the maintenance thread is alive, `0` once it has ended. Refreshed every tick, so an absence lasting longer than that means the same as a zero |
| `chronix_maintenance_panics_total` | Counter | Passes that panicked. The thread survives one — the other passes keep running — but a non-zero rate is a defect to report |

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

Requires `--features otlp`; without it the section is a startup error rather
than a collector that never receives anything.

**`always_sample_errors`** (default: `true`): When enabled, the sampler is
overridden to `AlwaysOn`, ensuring all traces — including error-bearing ones —
are captured. This is necessary because OpenTelemetry sampling decisions are
made at span creation time, before errors occur. Set to `false` and use
`sampling.ratio` for controlled sampling in high-throughput environments where
capturing every error trace is not required.

Named spans propagated across cluster:

| Span | Location |
|------|----------|
| `client_request` | Query router entry point |
| `scatter` | Fan-out to region leaders |
| `gather` | Merge results from regions |
| `write_batch` | Write router entry point |
| `health_check` | Coordinator health scan |
| `forecast_fit` | Distributed forecast fitting |
| `anomaly_detect` | Distributed anomaly detection |

## Backup & Restore

### WAL Snapshot

```bash
# Create a WAL checkpoint
chronixd snapshot --output /backup/chronix-$(date +%Y%m%d).snap

# Restore from snapshot
chronixd restore --input /backup/chronix-20240101.snap
```

### Object Store Backup

Cold-tiered segments in object storage provide inherent durability.
For additional safety, use cloud-native cross-region replication on
your S3/GCS/Azure bucket.

#### REST API

Backup and restore operations are also available via the admin REST API (requires admin authorization):

**Create Backup:**
```
POST /api/v1/admin/backup
Content-Type: application/json

{
  "target_dir": "/path/to/backup/directory"
}
```

Response: `200 OK` with `BackupManifest` JSON containing `version`, `created_at`, `wal_sequence`, `file_count`, and `total_bytes`.

**Restore from Backup:**
```
POST /api/v1/admin/restore
Content-Type: application/json

{
  "backup_dir": "/path/to/backup/source",
  "target_dir": "/path/to/restore/target"
}
```

Response: `200 OK` with `BackupManifest` JSON.

> **Note:** Restore is a static operation — it copies backup contents to the target directory without requiring a running database instance connection. The server must be restarted to load the restored data.

#### Binary Snapshots

Meta-store and region snapshots use postcard binary serialization for compact, efficient snapshot transfer:
- **Format:** `[u32 length][postcard payload][u32 CRC32c]`
- **Benefits:** 2-5x smaller than JSON, faster serialization/deserialization
- **Scope:** Raft state machine snapshots, region catalog snapshots

## Admin REST API

The admin REST API under `/api/v1/admin/` provides cluster management
and analytics model administration.  All endpoints require a running
cluster (meta-node or data-node); standalone mode returns `503`.

### Cluster Management

| Method | Path | Description |
|--------|------|-------------|
| `POST`   | `/api/v1/admin/nodes` | Register a new node |
| `GET`    | `/api/v1/admin/nodes` | List all cluster nodes |
| `DELETE` | `/api/v1/admin/nodes` | Deregister a node |
| `POST`   | `/api/v1/admin/nodes/{id}/decommission` | Graceful node removal |
| `POST`   | `/api/v1/admin/heartbeat` | Send node heartbeat |
| `POST`   | `/api/v1/admin/regions` | Create a region |
| `PUT`    | `/api/v1/admin/regions/{id}/state` | Update region state |
| `GET`    | `/api/v1/admin/routing` | Get routing table |
| `GET`    | `/api/v1/admin/health` | Cluster health summary |
| `POST`   | `/api/v1/admin/rebalance` | Trigger manual rebalance |

### Runtime Log-Level Adjustment

| Method | Path | Description |
|--------|------|-------------|
| `PUT` | `/api/v1/admin/log-level` | Change the server log level at runtime |

Adjust the tracing log level without restarting the server. Uses a
`tracing_subscriber::reload::Layer` so changes take effect immediately
across all spans.

```bash
curl -X PUT http://localhost:8086/api/v1/admin/log-level \
  -H 'Content-Type: application/json' \
  -d '{"level": "debug"}'
# {"previous": "info", "current": "debug"}
```

Accepted levels: `trace`, `debug`, `info`, `warn`, `error`. Invalid
levels return `400 Bad Request`.

### Circuit Breaker (Write Router)

The write router includes a per-node circuit breaker to avoid sending
writes to unhealthy DataNodes. When consecutive write failures to a node
exceed the threshold, the circuit opens and subsequent writes to that
node are short-circuited with an immediate error until the recovery
timeout elapses. After the timeout, the breaker transitions to
**HalfOpen** and admits exactly **one probe request** — all other callers
are blocked until the probe resolves. A successful probe closes the
circuit; a failed probe re-opens it.

| Setting | Default | Description |
|---------|---------|-------------|
| `circuit_breaker.failure_threshold` | `5` | Consecutive failures before opening |
| `circuit_breaker.recovery_timeout_secs` | `30` | Seconds before half-open probe |

**Metric:** `chronix_circuit_breaker_open_total` counter (labels: `node_id`) — incremented each time a node's breaker opens. There is no gauge of the current state; `rate(...[5m]) > 0` is how you alert on a node that keeps tripping.

### Analytics Model Management

| Method | Path | Description |
|--------|------|-------------|
| `GET`    | `/api/v1/admin/analytics/models` | List all trained models |
| `GET`    | `/api/v1/admin/analytics/models?measurement=cpu` | List models for a measurement |
| `GET`    | `/api/v1/admin/analytics/models/{measurement}/{name}` | Get model metadata |
| `DELETE` | `/api/v1/admin/analytics/models/{measurement}/{name}` | Delete a model |
| `POST`   | `/api/v1/admin/analytics/retrain` | Trigger model re-training |

**Re-training** clears the specified models from the catalog. The
`ContinuousForecastEngine` will automatically re-fit them on the next
incoming data batch.

### Namespace Management

| Method | Path | Description |
|--------|------|-------------|
| `POST`   | `/api/v1/namespaces` | Create a namespace |
| `GET`    | `/api/v1/namespaces` | List all namespaces |
| `GET`    | `/api/v1/namespaces/{name}` | Get namespace details |
| `DELETE` | `/api/v1/namespaces/{name}` | Delete a namespace |
| `GET`    | `/api/v1/namespaces/{name}/usage` | Get usage vs. limits |

Namespace routing uses the `X-Namespace` header.  If omitted, the
`default` namespace is used.  Namespace definitions are durably persisted
to disk and survive process restarts.

### When the data volume fills

1. A write that cannot reach the WAL answers **`507 STORAGE_FULL`**. Clients
   retry; the condition usually clears.
2. The WAL **rewinds to its last durable offset**, so writes resume the moment
   space returns — no restart — and a record reported as failed is never
   written later.
3. A flush that cannot write its segment leaves the memtable **frozen** for the
   next attempt, so nothing acknowledged is lost.
4. Reads keep working and `/ready` stays `200`: back-pressure, not a reason to
   leave the load balancer.

**Fix it by freeing space** — retention, WAL `max_file_size`, or the cold tier.
Alert on `chronix_write_errors_total{reason="storage_full"}`.

A failed `fsync` is different and **does** stop writes: the kernel may drop the
dirty pages and clear the error, so the file's contents are unknown. `/ready`
answers `503` with the reason, and so does every write — `503 OVERLOADED`,
unredacted, naming the condition. Recovery is to close and reopen the database.

### When ingestion outruns the storage

A memtable at capacity is back-pressure, not a fault: the flush that clears it
is already running. Writes answer `503 BACKPRESSURE` with `Retry-After: 1` and
a message naming the limit, so an agent backs off for a second and carries on.
It is the ordinary condition on a gateway whose flash is slower than its
ingest. Alert on `chronix_write_admission_rejected_total`.

### OpenAPI Specification

The full REST API specification is available as an OpenAPI 3.1 JSON document:

```
GET /api/v1/openapi.json
```

This endpoint returns a machine-readable API description suitable for code generation, documentation tools (e.g., Swagger UI, Redoc), and API testing frameworks.

The `servers[0].url` field in the OpenAPI document is dynamically derived from
the server's configured listen address and TLS settings, so generated clients
automatically target the correct host and port.

## Connector Management

### Startup Retry

Ingestion connectors are started at boot. By default a connector that cannot
reach its broker stops the server, rather than leaving it running with a
silently dead ingestion path.

| Setting | Default | Description |
|---------|---------|-------------|
| `connector_start_retries` | `0` | Start attempts per connector; `0` is fail-fast |
| `connector_start_retry_backoff_ms` | `1000` | Initial backoff; doubles each retry |

Raise the retry count when connectors and brokers start concurrently, as in a
container stack.

## Troubleshooting

### Common Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| High replication lag | Network or I/O bottleneck | Check `chronix_raft_log_replication_lag` p99 |
| Quota exceeded (429) | Tenant over limit | Increase quota or add retention policies |
| Slow cold storage reads | Cache cold | Pre-warm cache or increase `cache.max_size_bytes` |
| TLS `failed to configure` | Invalid certs/keys | Check file paths and cert/key pairing; server starts without TLS on failure |
| `BACKPRESSURE` (503) | The memtable is at capacity and a flush is running | Slow the ingestion rate, raise `database.max_memtable_memory`, or move to faster storage. Watch `chronix_write_admission_rejected_total{reason="memtable_full"}` |
| `OVERLOADED` (503) | The WAL is poisoned by a failed `fsync` | Close and reopen the database; the file's contents are unknown after the kernel dropped its dirty pages |
| `QUERY_TIMEOUT` (504) | A read exceeded its deadline | Narrow the time range, or raise `server.sql_query_timeout_secs` / `database.query_timeout_secs` |
| `WRITE_TIMEOUT` (504) | The server stopped waiting for a write | The write may still have landed — retry it; a point is identified by its series and timestamp |
| Circuit breaker open | Unhealthy DataNode | Check target node health; breaker auto-recovers after `recovery_timeout_secs` |
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

## See Also

- [Cluster Operations](@/docs/cluster.md) — cluster setup, scaling, failover
- [API Reference](@/docs/api-reference.md) — comprehensive endpoint documentation
- [Security Guide](@/docs/security.md) — production security checklist
- [Performance Tuning](@/docs/performance.md) — workload-specific tuning
- [Analytics Guide](@/docs/analytics.md) — forecast and anomaly detection APIs
- [Architecture Reference](@/reference/_index.md) — component deep-dive
- [Guide](@/docs/_index.md)
