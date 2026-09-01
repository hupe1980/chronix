+++
title = "Cluster Architecture (frozen)"
description = "The distributed Meta/Data/Query design and auto-scaling — kept compiling as reference, not part of the supported single-node product."
weight = 100
+++

## Cluster Architecture (`chronix-meta`, `chronix-cluster`)

Chronix scales from a single-node embedded database to a distributed cluster.
The same `chronixd` binary runs in one of three modes: **MetaNode**, **DataNode**,
or **QueryNode**.

### Node Roles

```text
┌─────────────────────────────────────────────────────────────┐
│                     MetaNode Cluster                        │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐                │
│  │ MetaNode │◄──│ MetaNode │──►│ MetaNode │  (Raft)        │
│  │ (leader) │   │(follower)│   │(follower)│                │
│  └────┬─────┘   └──────────┘   └──────────┘                │
│       │                                                     │
│       ▼                                                     │
│  MetaStateMachine                                           │
│    ├── Schema registry (measurement schemas)                │
│    ├── Routing table (region → DataNode mapping)            │
│    ├── Node membership (DataNode lifecycle)                  │
│    └── Model catalog (analytics model metadata)             │
└─────────────────────────────────────────────────────────────┘
          │  register / heartbeat / propose
          ▼
┌─────────────────────────────────────────────────────────────┐
│                     DataNode Pool                           │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐                │
│  │ DataNode │   │ DataNode │   │ DataNode │                │
│  │  R1, R3  │   │  R1, R2  │   │  R2, R3  │                │
│  └──────────┘   └──────────┘   └──────────┘                │
│                                                             │
│  Each DataNode hosts one or more regions (Rn).              │
│  Regions partition measurements via hash-based routing.     │
└─────────────────────────────────────────────────────────────┘
```

### Raft Consensus (`chronix-meta`)

The `MetaNode` cluster uses [OpenRaft](https://docs.rs/openraft) v0.9 for
linearizable metadata replication:

| Component             | Role                                                    |
|-----------------------|---------------------------------------------------------|
| `MetaStateMachine`    | Deterministic FSM — applies `MetaCommand` log entries   |
| `MetaLogStore`        | In-memory Raft log (`BTreeMap<u64, Entry>`)             |
| `MetaSmStore`         | `RaftStateMachine` + `RaftSnapshotBuilder` adapter      |
| `MetaStore`           | Facade — constructs a ready-to-use `Raft` instance      |
| `MetaNetworkFactory`  | `RaftNetworkFactory` — in-process router for testing    |
| `MetaRouter`          | Thread-safe map of `NodeId → MetaRaft` references       |
| `RoutingTable`        | Versioned `(measurement, region_id) → Vec<NodeId>` map  |

#### `MetaCommand` Variants

| Command                | Effect                                                 |
|------------------------|--------------------------------------------------------|
| `CreateMeasurement`    | Registers a new `MeasurementSchema`                     |
| `DropMeasurement`      | Removes schema and associated routing entries           |
| `RegisterNode`         | Adds a `DataNodeInfo` to membership                     |
| `DeregisterNode`       | Removes a node and rebuilds routing                     |
| `Heartbeat`            | Updates node generation counter and timestamp           |
| `CreateRegion`         | Assigns a region to a DataNode                          |
| `RemoveRegion`         | Removes a region assignment                             |
| `UpdateClusterConfig`  | Changes cluster-wide settings (RF, region count, etc.)  |
| `SaveModel`            | Stores analytics model metadata                         |
| `DeleteModel`          | Removes a model from the catalog                        |

#### Snapshot & Recovery

State snapshots use `bincode` serialization. On restore, the routing table is
rebuilt from the deserialized state. The snapshot includes all schemas, nodes,
regions, models, and cluster configuration.

### DataNode Lifecycle (`chronix-cluster`)

| Component            | Role                                                     |
|----------------------|----------------------------------------------------------|
| `DataNodeManager`    | Registration, heartbeat loop, graceful deregistration    |
| `RegionManager`      | Local region bookkeeping (create, remove, query)         |
| `ClusterCoordinator` | Health checks, under-replication detection, rebalancing  |
| `MetaClient` trait   | Abstract interface to MetaNode cluster                   |
| `InProcessMetaClient`| In-process implementation using `MetaRaft` directly      |

#### Health Check & Rebalance

`ClusterCoordinator::check_health()` returns a `HealthCheckResult` with:
- Active/suspect/dead node counts
- Under-replicated region list (regions below configured replication factor)
- `plan_rebalance()` generates `RegionMigration` actions to restore RF

### Cluster Metrics

All cluster components emit metrics via the `metrics` facade:

| Metric                                    | Type      | Description                  |
|-------------------------------------------|-----------|------------------------------|
| `chronix_cluster_nodes_total`             | Gauge     | Total node count by state    |
| `chronix_cluster_regions_total`           | Gauge     | Total region count           |
| `chronix_raft_leader_changes_total`       | Counter   | Raft leader elections        |
| `chronix_cluster_under_replicated_regions`| Gauge     | Regions below target RF      |
| `chronix_cluster_heartbeat_latency_seconds`| Histogram | Heartbeat round-trip time   |
| `chronix_cluster_write_latency_seconds`   | Histogram | Distributed write latency    |
| `chronix_cluster_query_latency_seconds`   | Histogram | Distributed query latency    |
| `chronix_raft_log_replication_lag`        | Histogram | Raft log replication lag (per region) |

### Test Infrastructure

Integration tests exercise a full Raft cluster in a single process:

**Cluster integration tests** (`chronix-meta/tests/cluster_integration.rs`):

1. `build_cluster()` — creates 3 `MetaStore` instances with shared `MetaRouter`
2. `initialize()` — bootstraps Raft with all 3 nodes as voters
3. `wait_for_leader()` — polls until a stable leader emerges
4. `propose()` — submits `MetaCommand` via the leader's `Raft::client_write()`

Five tests cover leader election, schema replication, node registration with
region creation, cluster config updates, and multi-schema model management.

**Admin integration tests** (`chronix-meta/tests/admin_integration.rs`):

1. `setup_raft()` — creates a single-node Raft cluster
2. `start_admin_server()` — binds an ephemeral port, serves `MetaAdminServer`
3. `GrpcMetaClient::new()` — connects to the local server

Six tests cover register/list nodes, heartbeat, create region with routing table
query, propose measurement creation, deregister node, and cluster info.

### Admin gRPC API (`chronix-meta::admin`)

The `MetaAdminService` exposes 10 RPCs over gRPC for cluster administration:

| RPC                 | Description                                        |
|---------------------|----------------------------------------------------|
| `RegisterNode`      | Register a DataNode with capacity info             |
| `DeregisterNode`    | Remove a DataNode from the cluster                 |
| `Heartbeat`         | DataNode periodic heartbeat with generation counter|
| `CreateRegion`      | Assign a region to a DataNode                      |
| `GetRoutingTable`   | Fetch current routing snapshot with version         |
| `Propose`           | Submit arbitrary `MetaCommand` through Raft         |
| `AddRaftNode`       | Add a MetaNode to the Raft cluster (learner → voter)|
| `RemoveRaftNode`    | Remove a MetaNode from the Raft cluster             |
| `ListNodes`         | List all registered DataNodes                       |
| `GetClusterInfo`    | Get cluster config, node count, leader info         |

**Server** (`MetaAdminServer`): wraps `Arc<MetaRaft>` + `Arc<MetaSmStore>`.
Proposals are serialized to JSON and committed via `Raft::client_write()`.

**Client** (`GrpcMetaClient`): implements `MetaClient` trait via gRPC. Supports
multiple MetaNode addresses with automatic failover — on connection failure,
rotates to the next address. Configurable timeout (default: 5s).

### Routing Cache (`chronix-cluster::routing_cache`)

`RoutingCache` wraps a local `RoutingSnapshot` and auto-refreshes on cache miss:

```text
┌──────────────┐    miss     ┌────────────┐    gRPC     ┌──────────┐
│ routes_for   │───────────► │  refresh() │────────────►│ MetaNode │
│ _measurement │◄─────┐     │            │◄────────────│          │
└──────────────┘      │     └────────────┘  routing    └──────────┘
                      │ hit — serve local    snapshot
                      │
                ┌─────┴──────┐
                │  snapshot  │
                │ (RwLock)   │
                └────────────┘
```

- **Stale routing retry**: on `RegionNotFound`, refreshes from MetaNode and
  retries up to `max_retries` (default: 3) before returning an error.
- **Version tracking**: `is_stale()` compares local version to remote version.
- **Thread-safe**: inner snapshot protected by `Arc<RwLock<RoutingSnapshot>>`.
- **O(1) region lookup**: secondary `HashMap<RegionId, RouteEntry>` index is rebuilt
  on every `refresh()`, making `lookup_region()` O(1) instead of O(total routes).

### Coordinator Health Loop (`chronix-cluster::coordinator`)

`ClusterCoordinator::run_health_loop()` runs a periodic background task:

1. Every `heartbeat_interval_secs`, calls `run_health_tick()`
2. `check_health()` evaluates node liveness based on heartbeat timestamps
3. Suspect/dead nodes produce `MetaCommand::UpdateNodeState` proposals via `MetaClient`
4. Under-replicated regions are logged and tracked via `chronix_cluster_under_replicated_regions` gauge
5. Graceful shutdown via `CancellationToken`

### `chronixd` Cluster Mode Integration

The `chronixd` binary supports three operating modes via `--mode`:

| Mode         | Description                                           |
|--------------|-------------------------------------------------------|
| `standalone` | Default — single-node embedded database (no cluster)  |
| `meta`       | MetaNode — runs Raft consensus for metadata           |
| `data`       | DataNode — hosts regions, heartbeats to MetaNodes     |

**MetaNode mode** (`chronixd --mode meta --node-id 1 --raft-bind 0.0.0.0:9100`):

1. Creates `MetaStore` + Raft node with `GrpcNetworkFactory`
2. Starts `RaftGrpcServer` + `MetaAdminServer` on the Raft bind address
3. Bootstraps single-node cluster if no `--cluster-peers` given
4. Starts `ClusterCoordinator` health loop with `InProcessMetaClient`

**DataNode mode** (`chronixd --mode data --node-id 10 --meta-addrs host1:9100,host2:9100`):

1. Creates `GrpcMetaClient` → `GrpcMetaClientAdapter` (implements `MetaClient`)
2. Creates `RegionManager`, `ChronixRegionStorage` (implements `RegionStorage`)
3. Creates `RoutingCache`, `DataGrpcClient` (connection-pooled), `WriteRouter`, `QueryRouter`
4. Creates `RegionMigrator` and `FailoverManager` (auto-healing replication)
5. Starts `DataGrpcServer` (WriteRegion / QueryRegion / ReplicateWal RPCs) on the gRPC address
6. Creates `DataNodeManager` — registers with MetaNode, starts heartbeat loop
7. Runs the full embedded Chronix database alongside cluster participation

Both modes include graceful shutdown: MetaNode cancels coordinator + gRPC server,
DataNode deregisters from the cluster before stopping.

### Data Service Layer (`chronix-cluster::data_service`)

The `DataGrpcServer` exposes three region-level RPCs defined in `data.proto`:

| RPC              | Description                                            |
|------------------|--------------------------------------------------------|
| `WriteRegion`    | Write a batch of points to a specific region           |
| `QueryRegion`    | Query points from a region with time range and filters |
| `ReplicateWal`   | Replay WAL entries from the region leader to followers |

The `RegionStorage` trait provides the storage abstraction:

```rust
#[async_trait]
trait RegionStorage: Send + Sync + Debug {
    async fn write_points(&self, region_id: u64, points: Vec<Point>) -> Result<u64>;
    async fn query_region(&self, region_id: u64, query: RegionQuery) -> Result<Vec<Point>>;
    async fn replicate_wal(&self, region_id: u64, entries: Vec<(u64, Vec<u8>)>) -> Result<u64>;
}
```

`ChronixRegionStorage` (in `chronixd`) implements this trait using the embedded
Chronix engine, converting Arrow `RecordBatch` results back to `Point` values.

`RegionQuery` provides a convenience builder:

```rust
RegionQuery::new("cpu", start_ns, end_ns)
    .with_tag_filter("host", "server-1")
    .with_field_columns(vec!["usage_idle".into()])
    .with_limit(1000)
```

`DataGrpcClient` uses `DashMap::entry()` for connection caching, preventing
TOCTOU duplicate-connection races when concurrent callers connect to the same
node simultaneously.

### Distributed Write Router (`chronix-cluster::write_router`)

The `WriteRouter` distributes writes to the correct region leaders:

1. Groups incoming points by measurement
2. Looks up routes via `RoutingCache` (auto-refreshes on miss)
3. Hashes each point's `SeriesKey` (`hash_fnv() % region_count`) to select a region
4. Groups points by region
5. Local regions: writes directly via `RegionStorage::write_points`
6. Remote regions: forwards via `DataGrpcClient::write_region`

Route lookup in step 2 uses a pre-built `HashMap<RegionId, &RouteEntry>` for
O(1) region-to-route resolution instead of a linear scan per point.

When a `RegionRaftManager` is configured (see Multi-Raft below), local writes
go through Raft quorum instead of directly to storage, ensuring durability
before acknowledging the write to the client.

#### Write Retry on Leader Change

When a remote write fails with a retriable error (`NotLeader`, `Timeout`,
or gRPC `Unavailable`), the router transparently retries:

1. Refreshes the routing table from the `MetaNode` to discover the new leader
2. Re-routes the write to the updated leader address
3. Retries up to `max_retries` (default: 2) before propagating the error

This ensures writes resume automatically after leader re-election — clients
see at most a brief delay, never an error, during leader transitions.

Point batches are pre-serialised to both core (`Vec<Point>`) and proto
(`Vec<DataPoint>`) representations **once** before the retry loop. On retry,
the router `.clone()`s the pre-built representations rather than re-building
from scratch, avoiding redundant allocations.

### Distributed Query Router (`chronix-cluster::query_router`)

The `QueryRouter` implements scatter-gather query execution:

1. Looks up regions for the target measurement via `RoutingCache`
2. **Scatter:** spawns parallel queries to each region via `pick_target()`
3. **Gather:** collects partial results, sorts by timestamp, applies global limit
4. Configurable per-query timeout (default: 30s) with partial-result tolerance

#### Read Consistency (`ReadConsistency`)

| Mode       | Behaviour                                                   |
|------------|-------------------------------------------------------------|
| `Leader`   | Routes to the region leader — linearizable reads (default)  |
| `Follower` | Routes to a random non-leader replica — eventually consistent |
| `Nearest`  | Routes to any replica — lowest latency                      |

`pick_target()` selects the target node based on the consistency level:
- **Leader:** always returns the region leader address
- **Follower:** picks a random non-leader from `replica_addrs`; falls back to
  leader when no other replicas exist
- **Nearest:** picks a random node from all `replica_addrs` (including leader)

### Distributed Analytics (`chronix-cluster::distributed_analytics`)

`DistributedAnalytics` runs FORECAST and ANOMALY queries across clustered data:

1. Fetches training/detection data from `QueryRouter` using `ReadConsistency::Follower`
   (offloads reads from region leaders)
2. Extracts the target field's `f64` values from returned points
3. Fits a forecast model or anomaly detector locally on the QueryNode/DataNode

#### Forecast Flow

```text
Client → DistributedAnalytics::forecast(request)
  → QueryRouter::query(measurement, Follower)     ← scatter-gather
  → extract_series(points, field)                  ← f64 extraction
  → LinearRegressionModel / SesModel::fit + predict
  → DistributedForecastResult { predictions, model_type }
```

#### Anomaly Detection Flow

```text
Client → DistributedAnalytics::detect_anomalies(request)
  → QueryRouter::query(measurement, Follower)
  → extract_series(points, field)
  → ZScoreDetector / IqrDetector::fit + detect
  → DistributedAnomalyResult { anomalies, detector_type }
```

### Region Migration (`chronix-cluster::region_migration`)

The `RegionMigrator` coordinates zero-downtime region moves:

1. Reads all data from the source region (local or remote)
2. Writes data to the destination in configurable batch sizes (default: 10,000)
3. **Updates routing table first via Raft** (if `MetaClient` is configured):
   - `AddRegionReplica(dest)` → `UpdateRegionLeader(dest)` → `RemoveRegionReplica(source)`
   - Errors from the first two Raft proposals are **propagated** — the migration
     aborts rather than leaving the source deleted with stale routing
4. Only **then** removes the region from the source `RegionManager`

The optional `with_meta_client()` builder wires in a `MetaClient` for Raft-based
routing table updates. Without it, the migrator performs data movement only
(useful for testing or single-node scenarios).

### Failover & Self-Healing (`chronix-cluster::failover`)

The `FailoverManager` monitors region health:

- **Dead node tracking:** nodes are marked dead/alive by the coordinator;
  `dead_nodes` uses `HashSet<NodeId>` for O(1) membership checks
- **Under-replication detection:** identifies regions below the target replication factor
- **Leader change detection:** detects when a region leader is on a dead node
- **Self-healing repair:** uses `RegionMigrator` to add new replicas, rate-limited
  to `max_concurrent_repairs` (default: 2) to avoid overloading surviving nodes
- **Admin rebalance:** `trigger_rebalance()` runs `check_health()` →
  `repair_under_replicated()` as a single entry point for manual or scheduled
  cluster rebalancing

### Inter-Node mTLS (`ClusterTlsConfig`)

All inter-node gRPC communication optionally uses mutual TLS (mTLS):

| Component            | TLS Applied                                          |
|----------------------|------------------------------------------------------|
| `DataGrpcClient`     | `.with_tls(ClientTlsConfig)` on channel endpoints    |
| `GrpcNetworkFactory` | `.with_tls(ClientTlsConfig)` for Raft peer channels  |
| `GrpcMetaClient`     | `.with_tls(ClientTlsConfig)` for admin connections   |
| Raft gRPC server     | `.tls_config(ServerTlsConfig)` on tonic builder      |
| Data gRPC server     | `.tls_config(ServerTlsConfig)` on tonic builder      |

Configuration via `cluster.tls` in `chronixd.toml`:

```toml
[cluster.tls]
ca_cert = "/certs/ca.pem"       # CA to verify peer certificates
cert    = "/certs/node.pem"     # This node's certificate
key     = "/certs/node-key.pem" # This node's private key
```

When `cluster.tls` is absent, all inter-node traffic uses plaintext HTTP
(backward-compatible, suitable for development/testing).

### Multi-Raft Per-Region Replication (`chronix-cluster::region_raft`)

Each region maintains its own independent Raft group, enabling per-region
leader election, quorum writes, and independent failure domains:

```text
┌─────────────────── DataNode 1 ─────────────────┐
│  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
│  │ R1-Raft  │  │ R2-Raft  │  │ R3-Raft  │ …    │
│  │ (leader) │  │(follower)│  │ (leader) │      │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘      │
│       │              │              │            │
│  ┌────▼──────────────▼──────────────▼────┐      │
│  │         RegionRaftManager             │      │
│  │  DashMap<RegionId, RegionRaft>        │      │
│  └───────────────────────────────────────┘      │
└─────────────────────────────────────────────────┘
```

The module is split into focused sub-modules under `src/region_raft/`:

| Sub-module        | Contents                                           |
|-------------------|----------------------------------------------------|
| `log_store.rs`    | In-memory Raft log storage (`RegionLogStore`)      |
| `state_machine.rs`| State-machine store applying writes (`RegionSmStore`) |
| `network.rs`      | In-process router & Raft network transport         |
| `manager.rs`      | Raft group lifecycle & write proposals             |
| `mod.rs`          | Type configuration, commands/responses, re-exports |

| Component               | Role                                                    |
|-------------------------|---------------------------------------------------------|
| `RegionRaftManager`     | Creates / removes Raft groups per region, proposes writes; `remove_group()` deregisters from router |
| `RegionLogStore`        | In-memory Raft log (`BTreeMap<u64, Entry>`); `purge()` returns `StorageError` on out-of-order IDs (no panics) |
| `RegionSmStore`         | Applies `RegionWriteCommand` to `RegionStorage` (zero-copy — payloads are moved, not cloned). Dedup cache bounded at 100K entries with FIFO eviction. |
| `RegionRaftRouter`      | Routes Raft RPCs between co-located region replicas; `remove_region()` sweeps stale entries |
| `RegionRaftNetworkFactory` | Creates `RaftNetwork` instances for a region group   |
| `RegionWriteCommand`    | Raft log entry — `WritePoints { region_id, points }`    |
| `RegionWriteResponse`   | Raft response — `{ written: u64 }`                      |

#### Quorum Write Flow

```text
Client write → WriteRouter
  → hash(SeriesKey) → region_id
  → RegionRaftManager::propose_write(region_id, points)
    → Raft::client_write(RegionWriteCommand::WritePoints)
      → leader appends to RegionLogStore
      → replicates to followers via RegionRaftNetwork
      → on quorum ack → RegionSmStore::apply()
        → RegionStorage::write_points(region_id, points)
  → WriteBatchResult returned to client (quorum committed)
```

Writes are acknowledged only after a majority of region replicas have committed
the entry to their Raft logs. This guarantees durability even if `RF - 1`
replicas fail immediately after the write.

### Performance Benchmarks (`chronix-cluster/benches/cluster_bench.rs`)

Criterion benchmarks validate cluster-layer performance:

| Benchmark Group            | Scenarios                                |
|----------------------------|------------------------------------------|
| `cluster_write_throughput` | `WriteRouter` local write: 100, 1K, 10K points |
| `cluster_write_single_region` | Single-region write: 100, 1K, 10K points |
| `cluster_raft_write`       | Raft quorum propose: 1, 10, 100 points   |
| `cluster_routing_hash`     | FNV hash throughput: 100, 1K, 10K keys   |

Run with: `cargo bench -p chronix-cluster --bench cluster_bench`

### Shared Test Utilities (`chronix-cluster::test_util`)

`test_util.rs` provides shared mock implementations to eliminate duplication
across test modules:

| Utility | Purpose |
|---------|---------|
| `MockSnapshotMetaClient` | Configurable `MetaClient` returning a preset `RoutingSnapshot` |
| `MockWriteStorage` | Write-log `RegionStorage` — records writes, stubs query/replicate |
| `MockRegionStore` | Per-region `RegionStorage` — accumulates writes, returns on query |
| `make_routing_snapshot()` | Build snapshot with leader self-replica (common test pattern) |
| `make_routing_snapshot_with_replicas()` | Build snapshot with explicit replica lists |

---
## Cluster Auto-Scaling (`chronix-cluster/autoscale`)

The autoscale module is decomposed into five submodules:

| File | Responsibility |
|------|---------------|
| `mod.rs` | `AutoScaler` struct, `assess()` entry-point, re-exports |
| `config.rs` | `AutoScaleConfig` with thresholds and tuning knobs |
| `split.rs` | `plan_splits()` — region split planning logic |
| `rebalance.rs` | `plan_disk_rebalance()` — disk-usage rebalancing |
| `periodic.rs` | Background periodic assessment loop |

### Region Split

Regions are automatically split when:
- Region size exceeds `region_size_threshold` (default: 10 GB)
- Series count exceeds `region_series_threshold` (default: 100K)

`AutoScaler::plan_splits()` generates `SplitPlan` directives.

### Disk Rebalancing

`AutoScaler::plan_disk_rebalance()` migrates regions when disk usage deviation exceeds threshold:
- Classifies nodes as overloaded (above mean + threshold) or underloaded (below mean - threshold)
- Generates migration commands: `MetaCommand::MigrateRegion { region_id, from, to }`
- Respects `max_concurrent_migrations` (default: 2)

### Assessment

`AutoScaler::assess()` returns a `ScaleAssessment` combining split plans and rebalance migrations, with `is_stable()` indicating whether the cluster is balanced.

---
