+++
title = "Cluster (frozen tier)"
description = "The distributed Meta/Data/Query tier — frozen, kept compiling as reference until a post-1.0 thaw. Not part of the supported single-node product."
weight = 110
+++

> **Status: frozen tier.** The distributed cluster (chronix-meta,
> chronix-cluster, chronix-dsim; `chronixd --features cluster`) is complete
> and tested but excluded from the default build and not under active
> development until the single-node engine ships v1.0. This guide is kept as
> reference.


This guide covers setting up, scaling, and operating a Chronix cluster, including
failover, rebalancing, and multi-tenancy.

## Architecture Overview

```text
┌─────────────────────────────────────────────────────┐
│                    Clients                          │
│    Flight SQL │ gRPC │ REST │ Embedded API          │
└───────┬───────┴──────┴──────┴───────────────────────┘
        │
┌───────▼──────────────────────────────────────────────┐
│                  MetaNode Cluster (Raft)             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐           │
│  │ MetaNode1│  │ MetaNode2│  │ MetaNode3│           │
│  │ (Leader) │  │(Follower)│  │(Follower)│           │
│  └──────────┘  └──────────┘  └──────────┘           │
│  routing table · schemas · region assignments       │
└───────┬──────────────────────────────────────────────┘
        │
┌───────▼──────────────────────────────────────────────┐
│               DataNode Fleet                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐           │
│  │DataNode 1│  │DataNode 2│  │DataNode 3│  ...      │
│  │ Region A │  │ Region B │  │ Region C │           │
│  │ Region D │  │ Region A'│  │ Region B'│           │
│  └──────────┘  └──────────┘  └──────────┘           │
│  time-series data · WAL · memtables · segments       │
└──────────────────────────────────────────────────────┘
```

## Cluster Setup

### Prerequisites

- 3 or 5 machines for MetaNodes (odd count for Raft quorum)
- N machines for DataNodes (minimum 3 for replication factor 3)
- Network: MetaNodes must reach each other on port 4240; DataNodes on port 8086

### MetaNode Configuration

The node files below are a **design sketch**: today's `chronixd` has no
`[node]`, `[meta]` or `[data]` table and will refuse a file containing one.
They describe the topology the frozen tier is built for.

```toml
# MetaNode (design sketch — not loadable today)
[node]
mode = "meta"
node_id = 1

[meta]
peers = ["meta1:4240", "meta2:4240", "meta3:4240"]

[meta.raft]
heartbeat_interval_ms = 200
election_timeout_ms = 1000
snapshot_threshold = 10000
```

### DataNode Configuration

```toml
# DataNode (design sketch — not loadable today)
[node]
mode = "data"
node_id = 101
data_dir = "/var/lib/chronix/data"

[data]
meta_endpoints = ["meta1:4240", "meta2:4240", "meta3:4240"]
replication_factor = 3

[data.storage]
wal_dir = "/var/lib/chronix/wal"
segment_max_size = "256MB"
```

### Bootstrap Sequence

1. Start all MetaNodes. The first to form quorum becomes leader.
2. Start DataNodes. Each registers with the MetaNode cluster via gRPC.
3. DataNodes receive region assignments and begin accepting writes.

```bash
# On meta1, meta2, meta3:
chronixd --config /etc/chronix/meta.toml

# On data1, data2, ...:
chronixd --config /etc/chronix/data.toml
```

## Scaling

### Adding DataNodes

1. Provision a new machine with `chronixd` installed.
2. Configure it with a unique `node_id` and MetaNode endpoints.
3. Start the node — it auto-registers and receives region assignments.
4. Optionally trigger a rebalance to redistribute load.

```bash
# Trigger manual rebalance
curl -X POST http://meta-leader:8086/api/v1/admin/rebalance
```

### Region Auto-Split

Chronix automatically splits regions when:

- **Size threshold** exceeded (default: 10 GB)
- **Series count** exceeded (default: 100,000 series)

The auto-scaler runs on a configurable interval (default: 60 s) and:

1. Snapshots the source region
2. Derives deterministic child region IDs and checks for collisions
3. Splits the source region's `KeyRange` at its midpoint — each child
   region receives half of the key space (`[start, mid)` and `[mid, end)`)
4. Atomically updates the routing table (old region removed, two new added)
5. Redirects writes to new regions while backfill completes
6. Removes the old region after data migration

#### Range-Based Partitioning (D-01)

Each region owns a `KeyRange` — a `[start, end)` half-open interval over
the FNV-1a hash space. `RouteEntry` and `RegionInfo` carry an optional
`key_range: Option<KeyRange>` field:

- **`route_series()`** routes writes using **binary search**
  (`partition_point()`) over sorted key ranges when all routes have ranges.
  A `debug_assert` validates sort invariant before binary search
- **Modulo fallback:** Legacy configurations without key ranges fall back to
  `hash % region_count` routing for backward compatibility
- **`rebuild()`** sorts routes by `key_range.start` for deterministic ordering
- **`KeyRange`** supports `contains()`, `midpoint()`, `split()` for
  region management operations. `midpoint()` and `split()` return `Option`
  — `None` for unsplittable ranges (single-element or empty)

**Safety:** Region ID collisions are detected in both `prepare_split` and
`execute_split` — on collision, the split is aborted and logged. Child IDs are
derived via `checked_mul(2)` / `checked_add(1)` — if the arithmetic would
overflow `u64` (after ~63 recursive splits), the split is safely aborted and the
source region is restored to `Active` state.

`AutoScaleConfig` (`chronix-cluster::autoscale`) — defaults:

```text
region_size_threshold     10 GB
region_series_threshold   100 000
scan_interval             60 s
```

**Metric:** `chronix_cluster_region_splits_total` counter (labeled by measurement)

### Disk-Aware Rebalancing

When disk usage deviation exceeds 20% from the cluster mean, the rebalancer
proposes migrations from overloaded to underloaded nodes.

```text
disk_deviation_threshold    0.20
max_concurrent_migrations   2
```

Migrations are rate-limited to avoid impacting foreground traffic.

### Region Migration Protocol

Region migrations use a **two-phase protocol** to ensure safety:

1. **Phase 1 — BeginMigration:**
   - Validates the source region is `Active` and the source node is the current leader
   - Validates the destination node is alive and different from the source
   - Adds the destination as a learner replica in the Raft group
   - Transitions region state from `Active` to `Migrating`

2. **Data Transfer:**
   - Reads data from the source region via an initial snapshot
   - Writes (replicates) data to the destination region
   - The `Migrating` state prevents concurrent migrations of the same region

3. **Catch-Up Phase:**
   - After the initial snapshot transfer, a timestamp-based convergence loop
     replays writes that arrived on the source during the transfer
   - Uses `read_source_since()` / `read_source_range()` with an
     `Option<i64>` watermark (advanced via `saturating_add(1)` each round)
   - Runs up to `max_catchup_rounds` convergence rounds (default: 10,
     configurable per `MigrationPlan`), stopping early when no new data is found

4. **Write-Freeze + Final Drain (D-02):**
   - After catch-up rounds converge, the source region is transitioned to
     `ReadOnly` via a Raft `UpdateRegionState` proposal — this freezes all
     new writes to the source
   - A **final drain pass** captures any in-flight writes that landed after the
     last catch-up watermark, ensuring zero data loss
   - If the freeze fails, the migration is cancelled and the region returns to
     `Active` state

5. **Phase 2 — MigrateRegion (Completion):**
   - Validates: region exists, is in `Migrating` state, source is current leader, destination is a member, destination differs from source
   - Promotes the destination to leader
   - Removes the source from the replica set
   - Transitions region state back to `Active`

**Safety guarantees:**
- 10 safety gates total across all phases prevent invalid transitions
- Double-migration is impossible: `BeginMigration` rejects non-`Active` regions
- Destination must be a replica member before leader promotion
- Source remains leader throughout data transfer
- **Write-freeze** ensures zero data loss — the source is made read-only before
  the final drain pass, guaranteeing no writes are missed during cutover
- **Catch-up convergence** replays writes that land on the source during
  snapshot transfer to the destination before the freeze
- **Rollback on failure:** If data transfer (read or write) fails during Phase 2,
  a `CancelMigration` command rolls the region back to `Active` state — removing
  the destination from the replica set and rebuilding the routing table. This
  prevents regions from becoming permanently stuck in `Migrating` state.
  If the write-freeze itself fails, the migration is cancelled and the region
  returns to `Active` state automatically.

## Durable Raft Storage

Chronix persists all Raft state to disk using [redb](https://docs.rs/redb), an
embedded ACID B+ tree database written in pure Rust. This ensures that both
MetaNode and DataNode Raft groups survive process restarts without data loss.

### Storage Backend

`DurableLogStore<C>` provides a generic, crash-safe Raft log store backed by two
redb tables:

| Table       | Key        | Value             | Purpose                        |
|-------------|------------|-------------------|---------------------------------|
| `raft_log`  | `u64`      | `Vec<u8>` (postcard)| Raft log entries              |
| `raft_meta` | `&str`     | `Vec<u8>` (postcard)| Vote state and committed index |

All writes use redb's ACID transactions — committed Raft entries are durable
before acknowledgment. On restart, the log store replays directly from the
on-disk B+ tree without WAL replay.

### MetaNode Log Store

`MetaStore` supports two storage modes:

| Mode     | Backend     | Use Case                        |
|----------|-------------|----------------------------------|
| Memory   | `BTreeMap`  | Testing, development            |
| Durable  | `redb`      | Production (default with `data_dir`) |

```rust
// Production: durable Raft storage
let store = MetaStore::open("/var/lib/chronix/meta").await?;

// Testing: in-memory
let store = MetaStore::new_in_memory();
```

### DataNode Region Log Store

`RegionRaftManager` uses `RegionLogStoreKind::Durable` when a `data_dir` is
configured. Each region gets its own redb database file:

```text
/var/lib/chronix/data/raft/
├── region_1.redb
├── region_2.redb
└── region_3.redb
```

### Region Snapshots with Data

Raft snapshots include the full region data, ensuring complete state transfer
during leader election or when a new replica joins:

1. **Snapshot build:** the `RegionStateMachine` calls
   `RegionStorage::snapshot_data(region_id)` to serialize all points in the region
2. **Snapshot install:** the receiving node calls
   `RegionStorage::restore_snapshot(region_id, data)` to rebuild the region
   from the transferred data
3. **Wire format:** region data is serialized as JSON for portability
4. **Routing checksum:** MetaNode snapshots include a routing checksum
   (hash of serialized regions + nodes). On restore, the checksum is verified
   and a warning is logged if the routing table doesn't match the snapshot.

This ensures that follower replicas receive the complete dataset — not just the
Raft log suffix — making catch-up fast and reliable. gRPC snapshot messages
support up to 256 MiB per message to accommodate large region snapshots.

### Membership Changes

Chronix uses **joint consensus** (Raft §6) for safe membership transitions.
When adding or removing MetaNodes, the cluster transitions through a joint
configuration `C_old,new` before committing the new configuration `C_new`.
This prevents split-brain during membership changes.

### Idempotent Log Application

The MetaNode state machine tracks `last_applied_index` and skips entries with
index ≤ last_applied. This ensures idempotent log application — replayed entries
(e.g., after snapshot restore) do not duplicate state.

## Failover

### MetaNode Leader Failure

- Raft detects missing heartbeats within `election_timeout_ms`
- Remaining MetaNodes elect a new leader (typically < 3 s)
- Routing table, schemas, and region assignments are fully replicated
- **Durable Raft storage (redb)** ensures committed state survives leader crashes
- Client requests briefly retry, then resume on the new leader

### DataNode Failure

- Coordinator health-check loop detects missing heartbeats
- Node transitions: `Active → Suspect → Dead`
- Under-replicated regions are re-replicated to available nodes
- Writes continue to remaining replicas (majority write quorum)

> **Note (D-09):** DataNode heartbeats use an out-of-band `HashMap` store
> (not replicated via Raft) for sub-millisecond latency. This means heartbeat
> timestamps are **lost on MetaNode leader failover**. The coordinator uses a
> grace period (`coordinator_failover_grace >= 2 × heartbeat_interval`) to
> prevent false-positive dead-node declarations after failover. During the
> grace period, all nodes are treated as healthy until fresh heartbeats arrive.

`ClusterConfig` (`chronix-meta::types`) carries `heartbeat_interval_secs` and
`heartbeat_suspect_threshold`; a node is suspect after that many missed
heartbeats.

### Node Restart Recovery

When a MetaNode or DataNode restarts, Raft state is fully recovered from disk:

1. **Vote state** — the node remembers which candidate it voted for, preventing
   double-voting violations
2. **Committed log entries** — all committed Raft entries are replayed from the
   redb database, restoring the state machine to its pre-crash state
3. **Snapshot restoration** — if a snapshot exists, the state machine is restored
   from the snapshot first, then any subsequent log entries are replayed

No external coordination is required — the restarted node rejoins the Raft group
automatically and catches up via normal Raft log replication or snapshot transfer.

### Network Partition

- Majority partition continues serving reads and writes
- Minority partition rejects writes (no quorum)
- On reunion: Raft log reconciliation restores consistency

### gRPC Transport & Connection Pooling

Inter-node communication uses `tonic` gRPC over HTTP/2 with persistent
connections via `connect_lazy()`. A single multiplexed `Channel` is
cached per `GrpcNetwork` instance and reused across all RPCs
(vote, append-entries, install-snapshot). Benefits:

- **No per-RPC TCP handshake**: Eliminates connection setup overhead
- **HTTP/2 multiplexing**: Concurrent RPCs over a single connection
- **Keepalive**: 10-second interval pings with 5-second timeout detect
  dead peers and trigger automatic reconnection
- **Lazy connect**: Channel is established on first use, not at
  construction time, avoiding startup ordering issues

## Replication

### Write Path

1. Client sends write to any DataNode
2. DataNode forwards to region leader
3. Leader appends to WAL, replicates to followers via Raft (durably persisted to redb)
4. Majority acknowledgment → write committed
5. Memtable updated; the database's maintenance thread flushes it to immutable segments

#### Admission Control

Before step 1, `check_admission()` verifies the system can accept the write.
When pending write bytes exceed the configured threshold, the write is
immediately rejected with `503 Service Unavailable` without entering the
WAL/replication pipeline. This back-pressure mechanism prevents OOM kills
and cascading failures during ingestion spikes.

**Metric:** `chronix_write_admission_rejected_total`

#### Circuit Breaker (Write Router)

In clustered mode, the write router maintains a per-node circuit breaker.
When a DataNode fails consecutively beyond the threshold, the circuit opens
and subsequent writes to that node fail fast without network round-trips:

| State | Behavior |
|-------|----------|
| **Closed** | Normal operation — writes forwarded to node |
| **Open** | Writes short-circuited immediately — no network I/O |
| **Half-Open** | Single probe write sent; success closes, failure re-opens |

This prevents slow or crashed nodes from adding latency to the entire
write path. Healthy nodes continue serving unaffected.

**Metric:** `chronix_circuit_breaker_open_total` counter (labels: `node_id`) — incremented each time a node's breaker opens

### Read Path

1. Query router identifies target regions from routing table
2. Scatter: parallel requests to DataNode leaders (or followers, depending on consistency)
3. Gather: merge results, apply sorting/aggregation
4. Return combined result to client

**Read Consistency Levels:**

| Level | Target | Guarantee |
|-------|--------|-----------|
| `Leader` | Region leader only | Linearizable — always sees latest committed writes |
| `Follower` | Any follower replica | Eventual consistency — may lag behind leader |
| `BoundedStale { max_version_lag }` | Follower if fresh, leader fallback | Bounded staleness — routes to follower when routing table version lag ≤ threshold, otherwise falls back to leader |

> **Note:** `BoundedStale` checks the routing cache's known version against the required freshness. When version information is unavailable, it conservatively falls back to `Leader` reads.

**Routing Cache Resilience:**

The routing cache uses **exponential backoff with jitter** when retrying failed metadata fetches (base: 50 ms, cap: 800 ms). This prevents thundering-herd effects during MetaNode failovers.

### Replication Factor

`ClusterConfig::default_replication_factor` — 1, 2 or 3:

- **RF=1:** No replication (development only)
- **RF=2:** Tolerates 1 node failure (reads continue, writes need manual recovery)
- **RF=3:** Tolerates 1 node failure with automatic recovery (recommended)

## Change Data Capture (CDC)

Every mutation (write, delete, measurement drop) generates a `CdcEvent` on
the node-local `EventBus`. CDC is designed for cluster-wide consumption:

### PersistentSubscription

`PersistentSubscription` provides durable, at-least-once CDC delivery:

1. **Durable log** — When a `DurableLog` is attached to the `EventBus`,
   every published event is appended with a monotonic sequence number.
2. **Replay** — A `PersistentSubscription` specifies a `replay_from`
   sequence. On creation, all events from that sequence onward are replayed
   from the durable log before the subscriber receives live events.
3. **Deduplication** — After replay completes, the subscription tracks
   `last_committed_seq` to skip live events already covered by the replay
   window, preventing duplicate processing.
4. **Gap detection** — `gap_count()` reports the number of gaps detected in
   the sequence stream, useful for monitoring log truncation or data loss.

This enables downstream consumers (replication agents, materialized views,
external sinks) to rejoin at their last checkpoint without missing events.

### Arrow Flight CDC Export

`CdcBatchConverter` + `CdcFlightExporter` (behind the `flight` feature flag)
convert CDC events to Arrow `RecordBatch` / `FlightData` frames for
zero-copy gRPC streaming to external systems (Spark, Flink, DataFusion).

## Multi-Tenancy

### Namespace Isolation

Each namespace provides complete data isolation:

- Separate measurements, schemas, models, rollups, and policies
- No cross-namespace data leakage
- Independent quota enforcement

### Creating Namespaces

```bash
# Create a namespace with custom quotas
curl -X POST http://chronix:8086/api/v1/admin/namespaces \
  -H "Content-Type: application/json" \
  -d '{
    "name": "team-platform",
    "description": "Platform team metrics",
    "owner": "admin",
    "quota": {
      "max_series_count": 500000,
      "max_ingestion_rate": 50000,
      "max_storage_bytes": 53687091200,
      "max_measurements": 200
    }
  }'
```

### Request Routing

Include the `X-Namespace` header in all requests:

```bash
curl -H "X-Namespace: team-platform" \
  http://chronix:8086/api/v1/write \
  -d 'cpu,host=server1 usage=42.5'
```

Or use the URL prefix:

```
/api/v1/ns/team-platform/write
/api/v1/ns/team-platform/query
```

### Quota Enforcement

Quotas are enforced in real-time on the write path:

| Resource           | Default Limit | HTTP Status on Breach |
|--------------------|---------------|----------------------|
| `max_series_count` | 1,000,000     | 429 Too Many Requests |
| `max_ingestion_rate` | 100,000 pts/s | 429 Too Many Requests |
| `max_storage_bytes` | 100 GB        | 429 Too Many Requests |
| `max_measurements` | 1,000         | 429 Too Many Requests |

**Metric:** `chronix_namespace_usage_ratio` gauge (labels: `namespace`, `resource`)

### Monitoring Namespace Usage

```bash
# Get usage summary for a namespace
curl http://chronix:8086/api/v1/ns/team-platform/usage
```

Response:

```json
{
  "namespace": "team-platform",
  "usage": {
    "series_count": 150000,
    "storage_bytes": 21474836480,
    "measurements": 45,
    "ingestion_rate": 12000.0
  },
  "quota": {
    "max_series_count": 500000,
    "max_storage_bytes": 53687091200,
    "max_measurements": 200,
    "max_ingestion_rate": 50000
  },
  "ratios": {
    "series_count": 0.30,
    "storage_bytes": 0.40,
    "measurements": 0.225
  }
}
```

## Monitoring

### Key Cluster Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `chronix_cluster_nodes_total` | gauge | Total nodes by mode/state |
| `chronix_cluster_regions_total` | gauge | Regions by measurement/state |
| `chronix_cluster_heartbeat_latency_seconds` | histogram | Heartbeat round-trip |
| `chronix_cluster_write_latency_seconds` | histogram | Write operation latency |
| `chronix_cluster_query_latency_seconds` | histogram | Query operation latency |
| `chronix_cluster_under_replicated_regions` | gauge | Under-replicated region count |
| `chronix_cluster_leader_changes_total` | counter | Leader election events |
| `chronix_cluster_region_splits_total` | counter | Region split events |
| `chronix_raft_log_replication_lag` | histogram | Raft log lag per region |
| `chronix_namespace_usage_ratio` | gauge | Per-namespace resource usage |

### Grafana Dashboards

Pre-built dashboards are available in `dashboards/`:

- **Cluster Overview** — node health, region distribution, replication state
- **Query Performance** — latency histograms, throughput, scatter-gather breakdown
- **Analytics** — forecast/anomaly latency, model freshness
- **Storage** — disk usage, object store tiering, cache hit rates

Import via Grafana UI or provisioning:

```bash
chronixd --export-dashboards ./grafana/
```

## Troubleshooting

### Common Issues

**Node won't join cluster:**
- Verify MetaNode endpoints are reachable
- Check `node_id` is unique across all nodes
- Ensure firewall allows ports 4240 (meta) and 8086 (data)

**High replication lag:**
- Check network latency between DataNodes
- Increase `max_concurrent_migrations` if rebalancing is bottlenecked
- Monitor `chronix_raft_log_replication_lag` per region

**Region split storm:**
- Lower `region_series_threshold` or `region_size_threshold` gradually
- Increase `scan_interval` to reduce split frequency
- Monitor `chronix_cluster_region_splits_total` for anomalies

**Quota exceeded unexpectedly:**
- Check `chronix_namespace_usage_ratio` for approaching-limit alerts
- Review series cardinality with `GET /api/v1/ns/{ns}/usage`
- Increase quota limits via admin API

---

## See Also

- [Operations Guide](@/docs/operations.md) — deployment modes, configuration reference
- [Security Guide](@/docs/security.md) — inter-node mTLS, authentication, authorization
- [Performance Tuning](@/docs/performance.md) — cluster-level tuning, region sizing
- [Architecture Reference](@/reference/_index.md) — Raft consensus, region replication internals
- [Guide](@/docs/_index.md)
