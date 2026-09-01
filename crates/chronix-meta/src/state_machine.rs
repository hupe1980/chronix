//! Raft-replicated metadata state machine.
//!
//! Applies [`MetaCommand`]s deterministically so all Raft replicas converge
//! to the same cluster state.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::error::{MetaError, Result};
use crate::routing::RoutingTable;
use crate::types::{
    ClusterConfig, DataNodeInfo, MeasurementSchema, MetaCommand, MetaResponse, NodeId, NodeState,
    RegionId, RegionInfo, RegionState,
};

/// Serialisable snapshot of the full metadata state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaSnapshot {
    /// Applied log index.
    pub last_applied_log: u64,
    /// Measurement schemas.
    pub schemas: BTreeMap<String, MeasurementSchema>,
    /// Registered data nodes.
    pub nodes: BTreeMap<NodeId, DataNodeInfo>,
    /// All regions.
    pub regions: BTreeMap<RegionId, RegionInfo>,
    /// Cluster-wide configuration.
    pub cluster_config: ClusterConfig,
    /// Analytics model metadata.
    pub models: BTreeMap<String, Vec<u8>>,
    /// Next region ID to allocate.
    pub next_region_id: u64,
    /// Out-of-band heartbeat store snapshot.
    ///
    /// Included in snapshots so that a new leader restored from a
    /// snapshot starts with a warm heartbeat store rather than empty.
    #[serde(default)]
    pub heartbeat_store: BTreeMap<NodeId, (u64, u64)>,
    /// FNV-1a checksum of serialized (regions, nodes) data.
    ///
    /// Verified on snapshot restore to detect corruption in the
    /// routing-relevant portion of the snapshot before rebuilding
    /// the derived routing table.
    #[serde(default)]
    pub routing_checksum: u64,
    /// Wall-clock timestamp (Unix secs) when the snapshot was taken.
    /// Used on restore to adjust heartbeat ages so stale nodes aren't
    /// treated as recently-seen.
    #[serde(default)]
    pub snapshot_timestamp_secs: u64,
}

impl Default for MetaSnapshot {
    fn default() -> Self {
        Self {
            last_applied_log: 0,
            schemas: BTreeMap::new(),
            nodes: BTreeMap::new(),
            regions: BTreeMap::new(),
            cluster_config: ClusterConfig::default(),
            models: BTreeMap::new(),
            next_region_id: 1,
            heartbeat_store: BTreeMap::new(),
            routing_checksum: 0,
            snapshot_timestamp_secs: 0,
        }
    }
}

/// Compute FNV-1a checksum of serialized (regions, nodes) data.
fn compute_routing_checksum(
    regions: &BTreeMap<RegionId, RegionInfo>,
    nodes: &BTreeMap<NodeId, DataNodeInfo>,
) -> u64 {
    use std::hash::{Hash, Hasher};
    let bytes = postcard::to_stdvec(&(regions, nodes)).unwrap_or_default();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

/// Inner state protected by a single `RwLock`.
///
/// Consolidating all Raft-replicated fields behind one lock ensures
/// that [`MetaStateMachine::snapshot()`] captures an atomic, consistent
/// view and eliminates ABBA deadlock risk between independent locks.
#[derive(Debug, Clone)]
struct MetaState {
    schemas: BTreeMap<String, MeasurementSchema>,
    nodes: BTreeMap<NodeId, DataNodeInfo>,
    regions: BTreeMap<RegionId, RegionInfo>,
    cluster_config: ClusterConfig,
    models: BTreeMap<String, Vec<u8>>,
    next_region_id: u64,
}

impl Default for MetaState {
    fn default() -> Self {
        Self {
            schemas: BTreeMap::new(),
            nodes: BTreeMap::new(),
            regions: BTreeMap::new(),
            cluster_config: ClusterConfig::default(),
            models: BTreeMap::new(),
            next_region_id: 1,
        }
    }
}

/// The metadata state machine.
///
/// Thread-safe and deterministic — the same sequence of commands always
/// produces the same state regardless of which node applies them.
///
/// All Raft-replicated state is consolidated behind a single
/// `RwLock<MetaState>` to guarantee atomic snapshots and eliminate
/// ABBA deadlock risk.
///
/// # Heartbeat Architecture
///
/// Heartbeat timestamps use a **dual-path** design:
///
/// 1. **Out-of-band path** — [`record_heartbeat`](Self::record_heartbeat)
///    updates a local in-memory store directly, bypassing Raft. This
///    provides low-latency liveness detection without consensus overhead.
///
/// 2. **Raft-replicated path** — [`MetaCommand::Heartbeat`] is proposed
///    through Raft for state transitions that must be durable (e.g.,
///    auto-recovery of `Suspect`/`Dead` nodes back to `Active`).
///
/// **Trade-off:** Out-of-band heartbeats are **not replicated** and are
/// lost on leader failover. After a leader election, the new leader's
/// heartbeat store is initially empty — it re-populates as data nodes
/// send their next heartbeats.
///
/// **Mitigation:** The [`ClusterCoordinator`] implements a configurable
/// **grace period** (see `record_leader_change` / `in_leader_grace_period`)
/// during which health checks are suppressed after leader failover. This
/// prevents spurious suspect/dead declarations while heartbeats propagate
/// to the new leader. The grace duration is:
///
/// ```text
/// grace = heartbeat_suspect_threshold × heartbeat_interval_secs
/// ```
pub struct MetaStateMachine {
    /// Last applied Raft log index.
    last_applied: AtomicU64,
    /// Consolidated Raft-replicated state — single lock for atomic snapshots.
    state: RwLock<MetaState>,
    /// Derived routing table (rebuilt on each region/node mutation).
    routing_table: RoutingTable,
    /// Out-of-band heartbeat store: `node_id → (generation, timestamp_secs)`.
    ///
    /// Updated directly via [`record_heartbeat`](Self::record_heartbeat)
    /// without a Raft proposal. Does **not** mutate Raft-replicated state.
    /// Lost on leader failover — mitigated by the coordinator grace period.
    heartbeat_store: RwLock<BTreeMap<NodeId, (u64, u64)>>,
    /// Monotonic heartbeat tracker using `Instant`.
    ///
    /// `SystemTime` is subject to NTP jumps.  This parallel store
    /// records the local *monotonic* instant when each heartbeat was
    /// received, so `check_health()` can compute liveness age without
    /// being affected by wall-clock corrections.
    heartbeat_instants: RwLock<BTreeMap<NodeId, Instant>>,
}

impl MetaStateMachine {
    /// Create a new, empty state machine.
    #[must_use]
    pub fn new() -> Self {
        Self {
            last_applied: AtomicU64::new(0),
            state: RwLock::new(MetaState::default()),
            routing_table: RoutingTable::new(),
            heartbeat_store: RwLock::new(BTreeMap::new()),
            heartbeat_instants: RwLock::new(BTreeMap::new()),
        }
    }

    /// Apply a command to the state machine.
    ///
    /// This is the core Raft apply function — must be **deterministic**.
    /// Acquires a single write lock on the consolidated state to ensure
    /// atomic mutations.
    ///
    /// Idempotent — entries with `log_index <= last_applied`
    /// are skipped to prevent duplicate state on Raft replays.
    pub fn apply(&self, log_index: u64, cmd: &MetaCommand) -> MetaResponse {
        // Skip already-applied entries (idempotency guard).
        let prev = self.last_applied.load(Ordering::Acquire);
        if log_index <= prev {
            tracing::debug!(
                log_index,
                last_applied = prev,
                "skipping already-applied Raft entry"
            );
            return MetaResponse::Ok;
        }
        self.last_applied.store(log_index, Ordering::Release);

        let mut state = self.state.write();
        let resp = match cmd {
            MetaCommand::CreateMeasurement(schema) => {
                Self::apply_create_measurement(&mut state, schema)
            }
            MetaCommand::DropMeasurement { name } => Self::apply_drop_measurement(&mut state, name),
            MetaCommand::RegisterNode(info) => Self::apply_register_node(&mut state, info),
            MetaCommand::DeregisterNode { node_id } => {
                Self::apply_deregister_node(&mut state, *node_id)
            }
            MetaCommand::Heartbeat {
                node_id,
                generation,
                timestamp_secs,
            } => {
                // Also update the out-of-band heartbeat store for consistency
                self.heartbeat_store
                    .write()
                    .insert(*node_id, (*generation, *timestamp_secs));
                Self::apply_heartbeat(&mut state, *node_id, *generation, *timestamp_secs)
            }
            MetaCommand::UpdateNodeState { node_id, state: ns } => {
                Self::apply_update_node_state(&mut state, *node_id, *ns)
            }
            MetaCommand::CreateRegion(info) => Self::apply_create_region(&mut state, info),
            MetaCommand::UpdateRegionState {
                region_id,
                state: rs,
            } => Self::apply_update_region_state(&mut state, *region_id, *rs),
            MetaCommand::UpdateRegionLeader {
                region_id,
                leader_node_id,
            } => Self::apply_update_region_leader(&mut state, *region_id, *leader_node_id),
            MetaCommand::AddRegionReplica { region_id, node_id } => {
                Self::apply_add_region_replica(&mut state, *region_id, *node_id)
            }
            MetaCommand::RemoveRegionReplica { region_id, node_id } => {
                Self::apply_remove_region_replica(&mut state, *region_id, *node_id)
            }
            MetaCommand::MigrateRegion {
                region_id,
                source_node_id,
                dest_node_id,
            } => Self::apply_migrate_region(&mut state, *region_id, *source_node_id, *dest_node_id),
            MetaCommand::BeginMigration {
                region_id,
                source_node_id,
                dest_node_id,
            } => {
                Self::apply_begin_migration(&mut state, *region_id, *source_node_id, *dest_node_id)
            }
            MetaCommand::CancelMigration {
                region_id,
                dest_node_id,
            } => Self::apply_cancel_migration(&mut state, *region_id, *dest_node_id),
            MetaCommand::UpdateClusterConfig(config) => {
                state.cluster_config = config.clone();
                MetaResponse::Ok
            }
            MetaCommand::SaveModel {
                measurement,
                model_id,
                metadata,
            } => {
                let key = format!("{measurement}:{model_id}");
                state.models.insert(key.clone(), metadata.clone());
                debug!(key = %key, "Model metadata saved");
                MetaResponse::Ok
            }
            MetaCommand::BatchHeartbeat { entries } => {
                // Replicate out-of-band heartbeat timestamps through
                // Raft so that followers (and future leaders) have a recent
                // view of node liveness. Updates both the Raft-replicated
                // `last_heartbeat_secs` on each DataNodeInfo *and* the
                // local out-of-band heartbeat store.
                let mut hb_store = self.heartbeat_store.write();
                for (&node_id, &(generation, ts)) in entries {
                    if let Some(node) = state.nodes.get_mut(&node_id) {
                        node.last_heartbeat_secs = ts;
                        node.heartbeat_generation = generation;
                    }
                    hb_store.insert(node_id, (generation, ts));
                }
                debug!(count = entries.len(), "Batch heartbeats replicated");
                MetaResponse::Ok
            }
        };

        // Only rebuild routing table when regions or nodes are modified.
        // Commands like SaveModel, BatchHeartbeat, UpdateClusterConfig
        // don't affect routing and skip the rebuild for efficiency.
        let affects_routing = !matches!(
            cmd,
            MetaCommand::SaveModel { .. }
                | MetaCommand::BatchHeartbeat { .. }
                | MetaCommand::UpdateClusterConfig(_)
        );
        if affects_routing {
            self.routing_table.rebuild(&state.regions, &state.nodes);
        }
        resp
    }

    /// Create a full snapshot of the current state.
    ///
    /// A single read-lock acquisition guarantees the snapshot is
    /// point-in-time consistent.
    #[must_use]
    pub fn snapshot(&self) -> MetaSnapshot {
        let state = self.state.read();
        MetaSnapshot {
            last_applied_log: self.last_applied.load(Ordering::Acquire),
            schemas: state.schemas.clone(),
            nodes: state.nodes.clone(),
            regions: state.regions.clone(),
            cluster_config: state.cluster_config.clone(),
            models: state.models.clone(),
            next_region_id: state.next_region_id,
            heartbeat_store: self.heartbeat_store.read().clone(),
            routing_checksum: compute_routing_checksum(&state.regions, &state.nodes),
            snapshot_timestamp_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
        }
    }

    /// Restore state from a snapshot.
    pub fn restore(&self, snapshot: MetaSnapshot) {
        // Verify routing-relevant data integrity before rebuilding
        // the derived routing table.
        if snapshot.routing_checksum != 0 {
            let expected = compute_routing_checksum(&snapshot.regions, &snapshot.nodes);
            if expected != snapshot.routing_checksum {
                warn!(
                    expected = snapshot.routing_checksum,
                    actual = expected,
                    "snapshot routing checksum mismatch — data may be corrupted"
                );
                metrics::counter!("chronix_snapshot_checksum_failures_total").increment(1);
            }
        }

        self.last_applied
            .store(snapshot.last_applied_log, Ordering::Release);

        let mut state = self.state.write();
        state.schemas = snapshot.schemas;
        state.nodes = snapshot.nodes.clone();
        state.regions = snapshot.regions.clone();
        state.cluster_config = snapshot.cluster_config;
        state.models = snapshot.models;
        state.next_region_id = snapshot.next_region_id;

        // Restore heartbeat store from snapshot so a new leader
        // starts with a warm view of node liveness.
        // Adjust heartbeat timestamps on restore to account
        // for time elapsed since the snapshot was taken.
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let mut restored_hb = snapshot.heartbeat_store;
        if snapshot.snapshot_timestamp_secs > 0 && now_secs > snapshot.snapshot_timestamp_secs {
            let elapsed = now_secs - snapshot.snapshot_timestamp_secs;
            for (_node, (last_seen_secs, _gen)) in restored_hb.iter_mut() {
                // Age the heartbeat by the time elapsed since snapshot.
                *last_seen_secs = last_seen_secs.saturating_sub(elapsed);
            }
        }
        *self.heartbeat_store.write() = restored_hb;

        // Rebuild derived routing table
        self.routing_table
            .rebuild(&snapshot.regions, &snapshot.nodes);
    }

    /// Access the derived routing table.
    #[must_use]
    pub fn routing_table(&self) -> &RoutingTable {
        &self.routing_table
    }

    /// Last applied Raft log index.
    #[must_use]
    pub fn last_applied_log(&self) -> u64 {
        self.last_applied.load(Ordering::Acquire)
    }

    /// Look up a measurement schema.
    #[must_use]
    pub fn get_schema(&self, name: &str) -> Option<MeasurementSchema> {
        self.state.read().schemas.get(name).cloned()
    }

    /// Get all measurement schemas.
    #[must_use]
    pub fn schemas(&self) -> BTreeMap<String, MeasurementSchema> {
        self.state.read().schemas.clone()
    }

    /// Look up a data node.
    #[must_use]
    pub fn get_node(&self, node_id: NodeId) -> Option<DataNodeInfo> {
        self.state.read().nodes.get(&node_id).cloned()
    }

    /// Get all registered data nodes.
    #[must_use]
    pub fn nodes(&self) -> BTreeMap<NodeId, DataNodeInfo> {
        self.state.read().nodes.clone()
    }

    /// Record a heartbeat directly (outside Raft consensus).
    ///
    /// Updates only the out-of-band heartbeat store for liveness
    /// detection timestamps. Does **not** mutate Raft-replicated state
    ///. Auto-recovery of `Suspect`/`Dead` nodes must be
    /// proposed through Raft via `MetaCommand::Heartbeat`.
    ///
    /// This data is **not** replicated and will be lost on leader
    /// failover. The coordinator grace period prevents false
    /// health-check failures during the re-convergence window.
    pub fn record_heartbeat(&self, node_id: NodeId, generation: u64) -> bool {
        let timestamp_secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();

        self.heartbeat_store
            .write()
            .insert(node_id, (generation, timestamp_secs));

        // Also record the monotonic instant so health checks
        // are immune to NTP / clock jumps.
        self.heartbeat_instants
            .write()
            .insert(node_id, Instant::now());

        let state = self.state.read();
        if state.nodes.contains_key(&node_id) {
            true
        } else {
            warn!(node_id, "Out-of-band heartbeat from unknown node");
            false
        }
    }

    /// Get out-of-band heartbeat data for all nodes.
    ///
    /// Returns `(generation, timestamp_secs)` for each node that has
    /// sent at least one heartbeat.
    #[must_use]
    pub fn heartbeat_data(&self) -> BTreeMap<NodeId, (u64, u64)> {
        self.heartbeat_store.read().clone()
    }

    /// Get monotonic heartbeat instants for all nodes.
    ///
    /// Returns the `Instant` at which the last out-of-band heartbeat
    /// was recorded for each node.  Immune to NTP / wall-clock jumps.
    #[must_use]
    pub fn heartbeat_instants(&self) -> BTreeMap<NodeId, Instant> {
        self.heartbeat_instants.read().clone()
    }

    /// Get the last heartbeat timestamp for a specific node.
    ///
    /// Prefers the out-of-band heartbeat store, falling back to the
    /// Raft-replicated `DataNodeInfo.last_heartbeat_secs`.
    #[must_use]
    pub fn last_heartbeat_secs(&self, node_id: NodeId) -> u64 {
        if let Some(&(_, ts)) = self.heartbeat_store.read().get(&node_id) {
            ts
        } else {
            let state = self.state.read();
            state
                .nodes
                .get(&node_id)
                .map_or(0, |n| n.last_heartbeat_secs)
        }
    }

    /// Get all active (non-dead) data nodes.
    #[must_use]
    pub fn active_nodes(&self) -> Vec<DataNodeInfo> {
        self.state
            .read()
            .nodes
            .values()
            .filter(|n| n.state != NodeState::Dead)
            .cloned()
            .collect()
    }

    /// Look up a region.
    #[must_use]
    pub fn get_region(&self, region_id: RegionId) -> Option<RegionInfo> {
        self.state.read().regions.get(&region_id).cloned()
    }

    /// Get all regions.
    #[must_use]
    pub fn regions(&self) -> BTreeMap<RegionId, RegionInfo> {
        self.state.read().regions.clone()
    }

    /// Get regions for a specific measurement.
    #[must_use]
    pub fn regions_for_measurement(&self, measurement: &str) -> Vec<RegionInfo> {
        self.state
            .read()
            .regions
            .values()
            .filter(|r| r.measurement == measurement)
            .cloned()
            .collect()
    }

    /// Get under-replicated regions (fewer replicas than desired).
    #[must_use]
    pub fn under_replicated_regions(&self) -> Vec<RegionInfo> {
        let state = self.state.read();
        state
            .regions
            .values()
            .filter(|r| {
                let active_replicas = r
                    .replica_node_ids
                    .iter()
                    .filter(|nid| {
                        state
                            .nodes
                            .get(nid)
                            .is_some_and(|n| n.state == NodeState::Active)
                    })
                    .count();
                active_replicas < r.replication_factor as usize
            })
            .cloned()
            .collect()
    }

    /// Current cluster configuration.
    #[must_use]
    pub fn cluster_config(&self) -> ClusterConfig {
        self.state.read().cluster_config.clone()
    }

    /// Look up a saved model.
    #[must_use]
    pub fn get_model(&self, measurement: &str, model_id: &str) -> Option<Vec<u8>> {
        let key = format!("{measurement}:{model_id}");
        self.state.read().models.get(&key).cloned()
    }

    /// Auto-create regions for a new measurement based on cluster config
    /// and schema settings.
    ///
    /// Generates `MetaCommand::CreateRegion` commands that should
    /// be proposed through the Raft log. Returns the commands and region IDs.
    /// Call `apply()` for each command on the Raft-replicated path.
    ///
    /// # Errors
    ///
    /// Returns an error if there are no active data nodes to assign regions.
    #[allow(clippy::cast_possible_truncation)]
    pub fn auto_create_regions(&self, schema: &MeasurementSchema) -> Result<Vec<RegionId>> {
        let commands = self.prepare_create_regions(schema)?;
        let ids: Vec<RegionId> = commands
            .iter()
            .map(|cmd| {
                if let MetaCommand::CreateRegion(info) = cmd {
                    info.region_id
                } else {
                    0
                }
            })
            .collect();

        // Apply through the deterministic apply path so state changes
        // are consistent with what Raft replicas would see.
        let mut state = self.state.write();
        for cmd in &commands {
            if let MetaCommand::CreateRegion(info) = cmd {
                Self::apply_create_region(&mut state, info);
            }
        }
        self.routing_table.rebuild(&state.regions, &state.nodes);
        Ok(ids)
    }

    /// Prepare `MetaCommand::CreateRegion` commands without applying them.
    ///
    /// In a Raft cluster, propose these commands through the Raft log so
    /// all replicas apply them deterministically.
    ///
    /// # Errors
    ///
    /// Returns an error if there are no active data nodes to assign regions.
    #[allow(clippy::cast_possible_truncation)]
    pub fn prepare_create_regions(&self, schema: &MeasurementSchema) -> Result<Vec<MetaCommand>> {
        let mut state = self.state.write();

        let active: Vec<DataNodeInfo> = state
            .nodes
            .values()
            .filter(|n| n.state != NodeState::Dead)
            .cloned()
            .collect();

        if active.is_empty() {
            return Err(MetaError::InvalidConfig(
                "No active data nodes to assign regions".into(),
            ));
        }

        let region_count = schema.region_count as usize;
        let replication_factor = schema.replication_factor.min(active.len() as u32);
        let mut commands = Vec::with_capacity(region_count);

        for i in 0..region_count {
            let region_id = state.next_region_id;
            state.next_region_id += 1;

            let leader_idx = i % active.len();
            let leader_node_id = active[leader_idx].node_id;

            // Pick replicas round-robin from active nodes
            let mut replicas = Vec::with_capacity(replication_factor as usize);
            for j in 0..replication_factor as usize {
                let idx = (leader_idx + j) % active.len();
                replicas.push(active[idx].node_id);
            }

            let region = RegionInfo::new(region_id, &schema.name, leader_node_id, replicas);
            commands.push(MetaCommand::CreateRegion(region));
        }

        Ok(commands)
    }

    // ── Private apply methods (take &mut MetaState, no individual locks) ──

    fn apply_create_measurement(state: &mut MetaState, schema: &MeasurementSchema) -> MetaResponse {
        if state.schemas.contains_key(&schema.name) {
            return MetaResponse::Error {
                message: format!("measurement '{}' already exists", schema.name),
            };
        }
        state.schemas.insert(schema.name.clone(), schema.clone());
        debug!(measurement = %schema.name, "Measurement schema created");
        MetaResponse::Created {
            description: format!("measurement '{}'", schema.name),
        }
    }

    fn apply_drop_measurement(state: &mut MetaState, name: &str) -> MetaResponse {
        if state.schemas.remove(name).is_none() {
            return MetaResponse::Error {
                message: format!("measurement '{name}' not found"),
            };
        }
        // Remove all regions for this measurement
        let to_remove: Vec<RegionId> = state
            .regions
            .values()
            .filter(|r| r.measurement == name)
            .map(|r| r.region_id)
            .collect();
        for rid in &to_remove {
            state.regions.remove(rid);
        }

        // Remove region IDs from node tracking
        for node in state.nodes.values_mut() {
            node.region_ids.retain(|r| !to_remove.contains(r));
        }

        debug!(measurement = %name, regions = to_remove.len(), "Measurement dropped");
        MetaResponse::Ok
    }

    fn apply_register_node(state: &mut MetaState, info: &DataNodeInfo) -> MetaResponse {
        state.nodes.insert(info.node_id, info.clone());
        debug!(node_id = info.node_id, addr = %info.grpc_addr, "Node registered");
        MetaResponse::Created {
            description: format!("node {}", info.node_id),
        }
    }

    fn apply_deregister_node(state: &mut MetaState, node_id: NodeId) -> MetaResponse {
        if state.nodes.remove(&node_id).is_none() {
            return MetaResponse::Error {
                message: format!("node {node_id} not found"),
            };
        }
        // Remove node from all region replica lists
        for region in state.regions.values_mut() {
            region.replica_node_ids.retain(|&nid| nid != node_id);
            if region.leader_node_id == node_id {
                region.leader_node_id = region.replica_node_ids.first().copied().unwrap_or(0);
                if region.replica_node_ids.is_empty() {
                    region.state = RegionState::ReadOnly;
                    tracing::warn!(
                        region_id = region.region_id,
                        "region has no remaining replicas after node deregistration — marked read-only"
                    );
                }
            }
        }
        debug!(node_id, "Node deregistered");
        MetaResponse::Ok
    }

    fn apply_heartbeat(
        state: &mut MetaState,
        node_id: NodeId,
        generation: u64,
        timestamp_secs: u64,
    ) -> MetaResponse {
        if let Some(node) = state.nodes.get_mut(&node_id) {
            node.heartbeat_generation = generation;
            node.last_heartbeat_secs = timestamp_secs;
            if node.state == NodeState::Suspect || node.state == NodeState::Dead {
                let old = node.state;
                node.state = NodeState::Active;
                debug!(node_id, old = %old, "Node auto-recovered via heartbeat");
            }
            MetaResponse::Ok
        } else {
            warn!(node_id, "Heartbeat from unknown node");
            MetaResponse::Error {
                message: format!("node {node_id} not registered"),
            }
        }
    }

    fn apply_update_node_state(
        state: &mut MetaState,
        node_id: NodeId,
        new_state: NodeState,
    ) -> MetaResponse {
        if let Some(node) = state.nodes.get_mut(&node_id) {
            let old = node.state;
            node.state = new_state;
            debug!(node_id, old = %old, new = %new_state, "Node state updated");
            MetaResponse::Ok
        } else {
            MetaResponse::Error {
                message: format!("node {node_id} not found"),
            }
        }
    }

    fn apply_create_region(state: &mut MetaState, info: &RegionInfo) -> MetaResponse {
        if state.regions.contains_key(&info.region_id) {
            return MetaResponse::Error {
                message: format!("region {} already exists", info.region_id),
            };
        }
        state.regions.insert(info.region_id, info.clone());

        // Track on leader node
        if let Some(node) = state.nodes.get_mut(&info.leader_node_id) {
            if !node.region_ids.contains(&info.region_id) {
                node.region_ids.push(info.region_id);
            }
        }

        debug!(region_id = info.region_id, measurement = %info.measurement, "Region created");
        MetaResponse::Created {
            description: format!("region {}", info.region_id),
        }
    }

    fn apply_update_region_state(
        state: &mut MetaState,
        region_id: RegionId,
        new_state: RegionState,
    ) -> MetaResponse {
        if let Some(region) = state.regions.get_mut(&region_id) {
            region.state = new_state;
            debug!(region_id, state = %new_state, "Region state updated");
            MetaResponse::Ok
        } else {
            MetaResponse::Error {
                message: format!("region {region_id} not found"),
            }
        }
    }

    fn apply_update_region_leader(
        state: &mut MetaState,
        region_id: RegionId,
        leader_node_id: NodeId,
    ) -> MetaResponse {
        if let Some(region) = state.regions.get_mut(&region_id) {
            // Invariant 1: leader must be in the replica set.
            if !region.replica_node_ids.contains(&leader_node_id) {
                return MetaResponse::Error {
                    message: format!(
                        "cannot set leader {leader_node_id} for region {region_id}: \
                         node is not in replica set {:?}",
                        region.replica_node_ids
                    ),
                };
            }

            // Invariant 2: leader node must exist and not be dead.
            if let Some(node) = state.nodes.get(&leader_node_id) {
                if node.state == NodeState::Dead {
                    return MetaResponse::Error {
                        message: format!(
                            "cannot set dead node {leader_node_id} as leader for region {region_id}"
                        ),
                    };
                }
            } else {
                return MetaResponse::Error {
                    message: format!(
                        "cannot set unregistered node {leader_node_id} as leader for region {region_id}"
                    ),
                };
            }

            region.leader_node_id = leader_node_id;
            debug!(region_id, leader = leader_node_id, "Region leader updated");
            MetaResponse::Ok
        } else {
            MetaResponse::Error {
                message: format!("region {region_id} not found"),
            }
        }
    }

    fn apply_add_region_replica(
        state: &mut MetaState,
        region_id: RegionId,
        node_id: NodeId,
    ) -> MetaResponse {
        // Check region exists
        if !state.regions.contains_key(&region_id) {
            return MetaResponse::Error {
                message: format!("region {region_id} not found"),
            };
        }

        // Invariant: node must be registered.
        if !state.nodes.contains_key(&node_id) {
            return MetaResponse::Error {
                message: format!(
                    "cannot add unregistered node {node_id} as replica for region {region_id}"
                ),
            };
        }

        // Add node to replica set
        if let Some(region) = state.regions.get_mut(&region_id) {
            if !region.replica_node_ids.contains(&node_id) {
                region.replica_node_ids.push(node_id);
            }
        }

        // Cross-update: track region on the node
        if let Some(node) = state.nodes.get_mut(&node_id) {
            if !node.region_ids.contains(&region_id) {
                node.region_ids.push(region_id);
            }
        }

        MetaResponse::Ok
    }

    fn apply_remove_region_replica(
        state: &mut MetaState,
        region_id: RegionId,
        node_id: NodeId,
    ) -> MetaResponse {
        if let Some(region) = state.regions.get_mut(&region_id) {
            // Invariant 1: cannot remove the last replica.
            if region.replica_node_ids.len() <= 1 && region.replica_node_ids.contains(&node_id) {
                return MetaResponse::Error {
                    message: format!(
                        "cannot remove last replica {node_id} from region {region_id}"
                    ),
                };
            }

            // Invariant 2: if removing the current leader, auto-reassign.
            if region.leader_node_id == node_id {
                let new_leader = region
                    .replica_node_ids
                    .iter()
                    .copied()
                    .find(|&nid| nid != node_id);
                if let Some(leader) = new_leader {
                    region.leader_node_id = leader;
                    debug!(
                        region_id,
                        old_leader = node_id,
                        new_leader = leader,
                        "Auto-reassigned leader before replica removal"
                    );
                }
            }

            region.replica_node_ids.retain(|&nid| nid != node_id);
        } else {
            return MetaResponse::Error {
                message: format!("region {region_id} not found"),
            };
        }

        // Cross-update: remove region from node tracking
        if let Some(node) = state.nodes.get_mut(&node_id) {
            node.region_ids.retain(|&rid| rid != region_id);
        }

        MetaResponse::Ok
    }

    /// Atomically migrate a region from source to destination.
    ///
    /// Completion phase of the two-step migration protocol.
    fn apply_migrate_region(
        state: &mut MetaState,
        region_id: RegionId,
        source_node_id: NodeId,
        dest_node_id: NodeId,
    ) -> MetaResponse {
        let region = match state.regions.get_mut(&region_id) {
            Some(r) => r,
            None => {
                return MetaResponse::Error {
                    message: format!("region {region_id} not found"),
                }
            }
        };

        // Gate 1: region must be in Migrating state.
        if region.state != RegionState::Migrating {
            return MetaResponse::Error {
                message: format!(
                    "region {region_id} is in state {:?}, expected Migrating \
                     (call BeginMigration first)",
                    region.state
                ),
            };
        }

        // Gate 2: destination must already be a replica.
        if !region.replica_node_ids.contains(&dest_node_id) {
            return MetaResponse::Error {
                message: format!(
                    "destination node {dest_node_id} is not a replica of region {region_id} \
                     (call BeginMigration first)"
                ),
            };
        }

        // Gate 3: validate destination node is not dead.
        match state.nodes.get(&dest_node_id) {
            Some(node) if node.state == NodeState::Dead => {
                return MetaResponse::Error {
                    message: format!(
                        "cannot migrate region {region_id} to dead node {dest_node_id}"
                    ),
                };
            }
            None => {
                return MetaResponse::Error {
                    message: format!(
                        "cannot migrate region {region_id} to unregistered node {dest_node_id}"
                    ),
                };
            }
            _ => {}
        }

        // Gate 4: source must be in replica set.
        if !region.replica_node_ids.contains(&source_node_id) {
            return MetaResponse::Error {
                message: format!(
                    "source node {source_node_id} is not a replica of region {region_id}"
                ),
            };
        }

        // Gate 5: removing source must not drop below 1 replica.
        let post_removal_count = region
            .replica_node_ids
            .iter()
            .filter(|&&nid| nid != source_node_id)
            .count();
        if post_removal_count < 1 {
            return MetaResponse::Error {
                message: format!(
                    "cannot remove source {source_node_id}: would leave region {region_id} \
                     with 0 replicas"
                ),
            };
        }

        // All gates passed — apply the migration atomically.
        region.leader_node_id = dest_node_id;
        region.replica_node_ids.retain(|&nid| nid != source_node_id);
        region.state = RegionState::Active;

        // Cross-update node tracking
        if let Some(src) = state.nodes.get_mut(&source_node_id) {
            src.region_ids.retain(|&rid| rid != region_id);
        }

        debug!(
            region_id,
            source = source_node_id,
            dest = dest_node_id,
            "Region migrated atomically"
        );
        MetaResponse::Ok
    }

    /// Begin a region migration — phase 1 of the two-step protocol.
    fn apply_begin_migration(
        state: &mut MetaState,
        region_id: RegionId,
        source_node_id: NodeId,
        dest_node_id: NodeId,
    ) -> MetaResponse {
        let region = match state.regions.get_mut(&region_id) {
            Some(r) => r,
            None => {
                return MetaResponse::Error {
                    message: format!("region {region_id} not found"),
                }
            }
        };

        // Gate 1: only Active regions can begin migration.
        if region.state != RegionState::Active {
            return MetaResponse::Error {
                message: format!(
                    "region {region_id} is in state {:?}, only Active regions can be migrated",
                    region.state
                ),
            };
        }

        // Gate 2: source must be the current leader.
        if region.leader_node_id != source_node_id {
            return MetaResponse::Error {
                message: format!(
                    "source node {source_node_id} is not the leader of region {region_id} \
                     (leader is {})",
                    region.leader_node_id
                ),
            };
        }

        // Gate 3: destination node must be registered and not dead.
        match state.nodes.get(&dest_node_id) {
            Some(node) if node.state == NodeState::Dead => {
                return MetaResponse::Error {
                    message: format!(
                        "cannot migrate region {region_id} to dead node {dest_node_id}"
                    ),
                };
            }
            None => {
                return MetaResponse::Error {
                    message: format!(
                        "cannot migrate region {region_id} to unregistered node {dest_node_id}"
                    ),
                };
            }
            _ => {}
        }

        // Gate 4: source and destination must be different.
        if source_node_id == dest_node_id {
            return MetaResponse::Error {
                message: format!(
                    "source and destination are the same node {source_node_id} — nothing to migrate"
                ),
            };
        }

        // Add destination as replica (learner).
        if !region.replica_node_ids.contains(&dest_node_id) {
            region.replica_node_ids.push(dest_node_id);
        }

        // Transition to Migrating state.
        region.state = RegionState::Migrating;

        // Cross-update: track region on the destination node.
        if let Some(dest) = state.nodes.get_mut(&dest_node_id) {
            if !dest.region_ids.contains(&region_id) {
                dest.region_ids.push(region_id);
            }
        }

        debug!(
            region_id,
            source = source_node_id,
            dest = dest_node_id,
            "Region migration begun — state=Migrating, dest added as learner"
        );
        MetaResponse::Ok
    }

    /// Cancel a migration that was begun with [`MetaCommand::BeginMigration`].
    fn apply_cancel_migration(
        state: &mut MetaState,
        region_id: RegionId,
        dest_node_id: NodeId,
    ) -> MetaResponse {
        let region = match state.regions.get_mut(&region_id) {
            Some(r) => r,
            None => {
                return MetaResponse::Error {
                    message: format!("region {region_id} not found"),
                }
            }
        };

        // Only Migrating regions can be cancelled.
        if region.state != RegionState::Migrating {
            return MetaResponse::Error {
                message: format!(
                    "region {region_id} is in state {:?}, only Migrating regions can be cancelled",
                    region.state
                ),
            };
        }

        // Remove destination from the replica set.
        region.replica_node_ids.retain(|&id| id != dest_node_id);

        // Restore to Active state.
        region.state = RegionState::Active;

        // Remove region from the destination node's region list.
        if let Some(dest) = state.nodes.get_mut(&dest_node_id) {
            dest.region_ids.retain(|&id| id != region_id);
        }

        debug!(
            region_id,
            dest = dest_node_id,
            "Migration cancelled — region restored to Active, dest removed"
        );
        MetaResponse::Ok
    }
}

impl Default for MetaStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MetaStateMachine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.read();
        f.debug_struct("MetaStateMachine")
            .field("last_applied", &self.last_applied_log())
            .field("schemas", &state.schemas.len())
            .field("nodes", &state.nodes.len())
            .field("regions", &state.regions.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_drop_measurement() {
        let sm = MetaStateMachine::new();
        let cmd = MetaCommand::CreateMeasurement(MeasurementSchema::new("cpu"));
        let resp = sm.apply(1, &cmd);
        assert!(matches!(resp, MetaResponse::Created { .. }));
        assert!(sm.get_schema("cpu").is_some());

        // Duplicate
        let resp2 = sm.apply(2, &cmd);
        assert!(matches!(resp2, MetaResponse::Error { .. }));

        // Drop
        let resp3 = sm.apply(3, &MetaCommand::DropMeasurement { name: "cpu".into() });
        assert!(matches!(resp3, MetaResponse::Ok));
        assert!(sm.get_schema("cpu").is_none());

        // Drop non-existent
        let resp4 = sm.apply(4, &MetaCommand::DropMeasurement { name: "cpu".into() });
        assert!(matches!(resp4, MetaResponse::Error { .. }));
    }

    #[test]
    fn register_and_deregister_node() {
        let sm = MetaStateMachine::new();
        let node = DataNodeInfo::new(1, "127.0.0.1:9100");

        let resp = sm.apply(1, &MetaCommand::RegisterNode(node.clone()));
        assert!(matches!(resp, MetaResponse::Created { .. }));
        assert!(sm.get_node(1).is_some());
        assert_eq!(sm.active_nodes().len(), 1);

        let resp2 = sm.apply(2, &MetaCommand::DeregisterNode { node_id: 1 });
        assert!(matches!(resp2, MetaResponse::Ok));
        assert!(sm.get_node(1).is_none());
    }

    #[test]
    fn heartbeat_updates_generation() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "addr")));

        sm.apply(
            2,
            &MetaCommand::Heartbeat {
                node_id: 1,
                generation: 5,
                timestamp_secs: 1000,
            },
        );

        let node = sm.get_node(1).unwrap();
        assert_eq!(node.heartbeat_generation, 5);
        assert_eq!(node.last_heartbeat_secs, 1000);

        // Out-of-band heartbeat store should also be updated
        let hb = sm.heartbeat_data();
        assert_eq!(hb.get(&1), Some(&(5, 1000)));
    }

    #[test]
    fn record_heartbeat_out_of_band() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "addr")));

        // Direct (out-of-band) heartbeat — no Raft proposal
        let ok = sm.record_heartbeat(1, 10);
        assert!(ok);

        let hb = sm.heartbeat_data();
        let (gen, ts) = hb.get(&1).expect("heartbeat should be recorded");
        assert_eq!(*gen, 10);
        // Timestamp is receiver-side (SystemTime::now()), so just verify it's non-zero
        assert!(*ts > 0);

        // last_heartbeat_secs should prefer out-of-band store
        assert!(sm.last_heartbeat_secs(1) > 0);

        // Unknown node should return false
        assert!(!sm.record_heartbeat(999, 1));
    }

    #[test]
    fn heartbeat_recovers_suspect_node() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "addr")));
        sm.apply(
            2,
            &MetaCommand::UpdateNodeState {
                node_id: 1,
                state: NodeState::Suspect,
            },
        );
        assert_eq!(sm.get_node(1).unwrap().state, NodeState::Suspect);

        sm.apply(
            3,
            &MetaCommand::Heartbeat {
                node_id: 1,
                generation: 1,
                timestamp_secs: 2000,
            },
        );
        assert_eq!(sm.get_node(1).unwrap().state, NodeState::Active);
    }

    #[test]
    fn region_lifecycle() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(2, &MetaCommand::RegisterNode(DataNodeInfo::new(2, "a:2")));
        sm.apply(3, &MetaCommand::RegisterNode(DataNodeInfo::new(3, "a:3")));

        let region = RegionInfo::new(1, "cpu", 1, vec![1, 2, 3]);
        sm.apply(4, &MetaCommand::CreateRegion(region));

        assert!(sm.get_region(1).is_some());
        assert_eq!(sm.regions_for_measurement("cpu").len(), 1);
        assert_eq!(sm.routing_table().region_count(), 1);

        // Update leader
        sm.apply(
            5,
            &MetaCommand::UpdateRegionLeader {
                region_id: 1,
                leader_node_id: 2,
            },
        );
        assert_eq!(sm.get_region(1).unwrap().leader_node_id, 2);

        // Update state
        sm.apply(
            6,
            &MetaCommand::UpdateRegionState {
                region_id: 1,
                state: RegionState::Migrating,
            },
        );
        assert_eq!(sm.get_region(1).unwrap().state, RegionState::Migrating);
    }

    #[test]
    fn region_replica_management() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(2, &MetaCommand::RegisterNode(DataNodeInfo::new(2, "a:2")));

        let region = RegionInfo::new(1, "cpu", 1, vec![1]);
        sm.apply(3, &MetaCommand::CreateRegion(region));

        // Add replica
        sm.apply(
            4,
            &MetaCommand::AddRegionReplica {
                region_id: 1,
                node_id: 2,
            },
        );
        let r = sm.get_region(1).unwrap();
        assert_eq!(r.replica_node_ids, vec![1, 2]);

        // Remove replica
        sm.apply(
            5,
            &MetaCommand::RemoveRegionReplica {
                region_id: 1,
                node_id: 2,
            },
        );
        let r = sm.get_region(1).unwrap();
        assert_eq!(r.replica_node_ids, vec![1]);
    }

    #[test]
    fn deregister_node_updates_regions() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(2, &MetaCommand::RegisterNode(DataNodeInfo::new(2, "a:2")));

        let region = RegionInfo::new(1, "cpu", 1, vec![1, 2]);
        sm.apply(3, &MetaCommand::CreateRegion(region));

        // Deregister node 1 (was leader)
        sm.apply(4, &MetaCommand::DeregisterNode { node_id: 1 });

        let r = sm.get_region(1).unwrap();
        assert!(!r.replica_node_ids.contains(&1));
        assert_eq!(r.leader_node_id, 2); // leader promoted
    }

    #[test]
    fn drop_measurement_cleans_up_regions() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(
            2,
            &MetaCommand::CreateMeasurement(MeasurementSchema::new("cpu")),
        );

        let region = RegionInfo::new(1, "cpu", 1, vec![1]);
        sm.apply(3, &MetaCommand::CreateRegion(region));

        sm.apply(4, &MetaCommand::DropMeasurement { name: "cpu".into() });
        assert!(sm.regions_for_measurement("cpu").is_empty());
        assert_eq!(sm.routing_table().region_count(), 0);
    }

    #[test]
    fn snapshot_and_restore() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(2, &MetaCommand::RegisterNode(DataNodeInfo::new(2, "a:2")));
        sm.apply(
            3,
            &MetaCommand::CreateMeasurement(MeasurementSchema::new("cpu")),
        );
        let region = RegionInfo::new(1, "cpu", 1, vec![1, 2]);
        sm.apply(4, &MetaCommand::CreateRegion(region));
        sm.apply(
            5,
            &MetaCommand::SaveModel {
                measurement: "cpu".into(),
                model_id: "m1".into(),
                metadata: vec![1, 2, 3],
            },
        );

        let snap = sm.snapshot();
        assert_eq!(snap.last_applied_log, 5);

        // Bincode roundtrip
        let bytes = postcard::to_stdvec(&snap).unwrap();
        let restored_snap: MetaSnapshot = postcard::from_bytes(&bytes).unwrap();

        let sm2 = MetaStateMachine::new();
        sm2.restore(restored_snap);

        assert_eq!(sm2.last_applied_log(), 5);
        assert!(sm2.get_schema("cpu").is_some());
        assert_eq!(sm2.nodes().len(), 2);
        assert_eq!(sm2.regions().len(), 1);
        assert_eq!(sm2.get_model("cpu", "m1").unwrap(), vec![1, 2, 3]);
        assert_eq!(sm2.routing_table().region_count(), 1);
    }

    #[test]
    fn auto_create_regions() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(2, &MetaCommand::RegisterNode(DataNodeInfo::new(2, "a:2")));
        sm.apply(3, &MetaCommand::RegisterNode(DataNodeInfo::new(3, "a:3")));

        let schema = MeasurementSchema::new("cpu")
            .with_region_count(3)
            .with_replication_factor(3);

        let ids = sm.auto_create_regions(&schema).unwrap();
        assert_eq!(ids.len(), 3);
        assert_eq!(sm.regions().len(), 3);
        assert_eq!(sm.routing_table().region_count(), 3);

        // Each region should have 3 replicas
        for id in &ids {
            let r = sm.get_region(*id).unwrap();
            assert_eq!(r.replica_node_ids.len(), 3);
        }
    }

    #[test]
    fn auto_create_regions_no_nodes() {
        let sm = MetaStateMachine::new();
        let schema = MeasurementSchema::new("cpu");
        assert!(sm.auto_create_regions(&schema).is_err());
    }

    #[test]
    fn under_replicated_regions() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(2, &MetaCommand::RegisterNode(DataNodeInfo::new(2, "a:2")));
        sm.apply(3, &MetaCommand::RegisterNode(DataNodeInfo::new(3, "a:3")));

        let mut region = RegionInfo::new(1, "cpu", 1, vec![1, 2, 3]);
        region.replication_factor = 3;
        sm.apply(4, &MetaCommand::CreateRegion(region));

        // All active — not under-replicated
        assert!(sm.under_replicated_regions().is_empty());

        // Kill node 3
        sm.apply(
            5,
            &MetaCommand::UpdateNodeState {
                node_id: 3,
                state: NodeState::Dead,
            },
        );
        let under = sm.under_replicated_regions();
        assert_eq!(under.len(), 1);
        assert_eq!(under[0].region_id, 1);
    }

    #[test]
    fn cluster_config_update() {
        let sm = MetaStateMachine::new();
        let cfg = ClusterConfig {
            cluster_name: "test-cluster".into(),
            default_replication_factor: 5,
            ..Default::default()
        };

        sm.apply(1, &MetaCommand::UpdateClusterConfig(cfg));
        let retrieved = sm.cluster_config();
        assert_eq!(retrieved.cluster_name, "test-cluster");
        assert_eq!(retrieved.default_replication_factor, 5);
    }

    #[test]
    fn save_and_get_model() {
        let sm = MetaStateMachine::new();
        sm.apply(
            1,
            &MetaCommand::SaveModel {
                measurement: "cpu".into(),
                model_id: "arima_v1".into(),
                metadata: vec![42, 43, 44],
            },
        );
        let data = sm.get_model("cpu", "arima_v1").unwrap();
        assert_eq!(data, vec![42, 43, 44]);
        assert!(sm.get_model("cpu", "nonexistent").is_none());
    }

    #[test]
    fn determinism_same_commands_same_state() {
        let cmds = [
            MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")),
            MetaCommand::RegisterNode(DataNodeInfo::new(2, "a:2")),
            MetaCommand::CreateMeasurement(MeasurementSchema::new("cpu")),
            MetaCommand::CreateRegion(RegionInfo::new(1, "cpu", 1, vec![1, 2])),
            MetaCommand::Heartbeat {
                node_id: 1,
                generation: 1,
                timestamp_secs: 1000,
            },
            MetaCommand::UpdateRegionLeader {
                region_id: 1,
                leader_node_id: 2,
            },
        ];

        let sm1 = MetaStateMachine::new();
        let sm2 = MetaStateMachine::new();

        for (i, cmd) in cmds.iter().enumerate() {
            sm1.apply((i + 1) as u64, cmd);
            sm2.apply((i + 1) as u64, cmd);
        }

        // Both state machines should produce identical snapshots
        let s1 = sm1.snapshot();
        let s2 = sm2.snapshot();
        let b1 = postcard::to_stdvec(&s1).unwrap();
        let b2 = postcard::to_stdvec(&s2).unwrap();
        assert_eq!(b1, b2);
    }

    #[test]
    fn last_applied_tracks_log_index() {
        let sm = MetaStateMachine::new();
        assert_eq!(sm.last_applied_log(), 0);
        sm.apply(
            42,
            &MetaCommand::CreateMeasurement(MeasurementSchema::new("cpu")),
        );
        assert_eq!(sm.last_applied_log(), 42);
    }

    #[test]
    fn debug_output() {
        let sm = MetaStateMachine::new();
        let dbg = format!("{sm:?}");
        assert!(dbg.contains("MetaStateMachine"));
        assert!(dbg.contains("last_applied"));
    }

    // ── Invariant enforcement tests ──────────────────────────────────

    #[test]
    fn update_leader_rejects_non_replica() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(2, &MetaCommand::RegisterNode(DataNodeInfo::new(2, "a:2")));
        sm.apply(
            3,
            &MetaCommand::CreateRegion(RegionInfo::new(1, "cpu", 1, vec![1])),
        );

        // Node 2 is not in the replica set — must be rejected
        let resp = sm.apply(
            4,
            &MetaCommand::UpdateRegionLeader {
                region_id: 1,
                leader_node_id: 2,
            },
        );
        assert!(matches!(resp, MetaResponse::Error { .. }));

        // Leader should remain node 1
        let region = sm.get_region(1).unwrap();
        assert_eq!(region.leader_node_id, 1);
    }

    #[test]
    fn update_leader_rejects_dead_node() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(2, &MetaCommand::RegisterNode(DataNodeInfo::new(2, "a:2")));
        sm.apply(
            3,
            &MetaCommand::CreateRegion(RegionInfo::new(1, "cpu", 1, vec![1, 2])),
        );

        // Mark node 2 as dead
        sm.apply(
            4,
            &MetaCommand::UpdateNodeState {
                node_id: 2,
                state: NodeState::Dead,
            },
        );

        // Promoting dead node should fail
        let resp = sm.apply(
            5,
            &MetaCommand::UpdateRegionLeader {
                region_id: 1,
                leader_node_id: 2,
            },
        );
        assert!(matches!(resp, MetaResponse::Error { .. }));
    }

    #[test]
    fn add_replica_rejects_unregistered_node() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(
            2,
            &MetaCommand::CreateRegion(RegionInfo::new(1, "cpu", 1, vec![1])),
        );

        // Node 99 is not registered
        let resp = sm.apply(
            3,
            &MetaCommand::AddRegionReplica {
                region_id: 1,
                node_id: 99,
            },
        );
        assert!(matches!(resp, MetaResponse::Error { .. }));
    }

    #[test]
    fn add_replica_updates_node_region_ids() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(2, &MetaCommand::RegisterNode(DataNodeInfo::new(2, "a:2")));
        sm.apply(
            3,
            &MetaCommand::CreateRegion(RegionInfo::new(1, "cpu", 1, vec![1])),
        );

        sm.apply(
            4,
            &MetaCommand::AddRegionReplica {
                region_id: 1,
                node_id: 2,
            },
        );

        let node = sm.get_node(2).unwrap();
        assert!(node.region_ids.contains(&1));
    }

    #[test]
    fn remove_last_replica_rejected() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(
            2,
            &MetaCommand::CreateRegion(RegionInfo::new(1, "cpu", 1, vec![1])),
        );

        let resp = sm.apply(
            3,
            &MetaCommand::RemoveRegionReplica {
                region_id: 1,
                node_id: 1,
            },
        );
        assert!(matches!(resp, MetaResponse::Error { .. }));

        // Region should still have the replica
        let region = sm.get_region(1).unwrap();
        assert_eq!(region.replica_node_ids, vec![1]);
    }

    #[test]
    fn remove_leader_auto_reassigns() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(2, &MetaCommand::RegisterNode(DataNodeInfo::new(2, "a:2")));
        sm.apply(
            3,
            &MetaCommand::CreateRegion(RegionInfo::new(1, "cpu", 1, vec![1, 2])),
        );

        // Remove the leader (node 1)
        let resp = sm.apply(
            4,
            &MetaCommand::RemoveRegionReplica {
                region_id: 1,
                node_id: 1,
            },
        );
        assert!(matches!(resp, MetaResponse::Ok));

        let region = sm.get_region(1).unwrap();
        // Leader should have been reassigned to node 2
        assert_eq!(region.leader_node_id, 2);
        assert_eq!(region.replica_node_ids, vec![2]);
    }

    #[test]
    fn deregister_node_marks_orphan_regions_readonly() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(
            2,
            &MetaCommand::CreateRegion(RegionInfo::new(1, "cpu", 1, vec![1])),
        );

        // Deregister the only node
        sm.apply(3, &MetaCommand::DeregisterNode { node_id: 1 });

        let region = sm.get_region(1).unwrap();
        assert!(region.replica_node_ids.is_empty());
        assert_eq!(region.state, RegionState::ReadOnly);
    }

    // ── Two-phase migration tests ────────────────────────────────────

    #[test]
    fn begin_migration_success() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(2, &MetaCommand::RegisterNode(DataNodeInfo::new(2, "a:2")));
        sm.apply(
            3,
            &MetaCommand::CreateRegion(RegionInfo::new(1, "cpu", 1, vec![1])),
        );

        // Phase 1: BeginMigration
        let resp = sm.apply(
            4,
            &MetaCommand::BeginMigration {
                region_id: 1,
                source_node_id: 1,
                dest_node_id: 2,
            },
        );
        assert!(matches!(resp, MetaResponse::Ok));

        let region = sm.get_region(1).unwrap();
        assert_eq!(region.state, RegionState::Migrating);
        assert!(
            region.replica_node_ids.contains(&2),
            "dest should be added as learner"
        );
        assert!(
            region.replica_node_ids.contains(&1),
            "source should still be present"
        );
        assert_eq!(
            region.leader_node_id, 1,
            "leader unchanged until MigrateRegion"
        );

        // Dest node should track this region
        let dst = sm.get_node(2).unwrap();
        assert!(dst.region_ids.contains(&1));
    }

    #[test]
    fn migrate_region_atomic_success() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(2, &MetaCommand::RegisterNode(DataNodeInfo::new(2, "a:2")));
        sm.apply(
            3,
            &MetaCommand::CreateRegion(RegionInfo::new(1, "cpu", 1, vec![1])),
        );

        // Phase 1: BeginMigration
        sm.apply(
            4,
            &MetaCommand::BeginMigration {
                region_id: 1,
                source_node_id: 1,
                dest_node_id: 2,
            },
        );

        // Phase 2: MigrateRegion
        let resp = sm.apply(
            5,
            &MetaCommand::MigrateRegion {
                region_id: 1,
                source_node_id: 1,
                dest_node_id: 2,
            },
        );
        assert!(matches!(resp, MetaResponse::Ok));

        let region = sm.get_region(1).unwrap();
        assert_eq!(region.leader_node_id, 2);
        assert_eq!(region.replica_node_ids, vec![2]);
        assert_eq!(region.state, RegionState::Active);

        // Source node should no longer track this region
        let src = sm.get_node(1).unwrap();
        assert!(!src.region_ids.contains(&1));

        // Dest node should track this region
        let dst = sm.get_node(2).unwrap();
        assert!(dst.region_ids.contains(&1));
    }

    #[test]
    fn migrate_region_rejects_non_migrating_state() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(2, &MetaCommand::RegisterNode(DataNodeInfo::new(2, "a:2")));
        sm.apply(
            3,
            &MetaCommand::CreateRegion(RegionInfo::new(1, "cpu", 1, vec![1])),
        );

        // Skip BeginMigration — region is still Active
        let resp = sm.apply(
            4,
            &MetaCommand::MigrateRegion {
                region_id: 1,
                source_node_id: 1,
                dest_node_id: 2,
            },
        );
        assert!(matches!(resp, MetaResponse::Error { .. }));

        // Region should be unchanged
        let region = sm.get_region(1).unwrap();
        assert_eq!(region.leader_node_id, 1);
        assert_eq!(region.state, RegionState::Active);
    }

    #[test]
    fn migrate_region_rejects_dead_dest() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(2, &MetaCommand::RegisterNode(DataNodeInfo::new(2, "a:2")));
        sm.apply(
            3,
            &MetaCommand::CreateRegion(RegionInfo::new(1, "cpu", 1, vec![1])),
        );

        // Begin migration while dest is still alive
        sm.apply(
            4,
            &MetaCommand::BeginMigration {
                region_id: 1,
                source_node_id: 1,
                dest_node_id: 2,
            },
        );

        // Dest node dies after BeginMigration
        sm.apply(
            5,
            &MetaCommand::UpdateNodeState {
                node_id: 2,
                state: NodeState::Dead,
            },
        );

        // MigrateRegion should reject dead destination
        let resp = sm.apply(
            6,
            &MetaCommand::MigrateRegion {
                region_id: 1,
                source_node_id: 1,
                dest_node_id: 2,
            },
        );
        assert!(matches!(resp, MetaResponse::Error { .. }));

        // Region should still be in Migrating state (not corrupted)
        let region = sm.get_region(1).unwrap();
        assert_eq!(region.leader_node_id, 1);
    }

    #[test]
    fn migrate_region_rejects_invalid_source() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(2, &MetaCommand::RegisterNode(DataNodeInfo::new(2, "a:2")));
        sm.apply(3, &MetaCommand::RegisterNode(DataNodeInfo::new(3, "a:3")));
        sm.apply(
            4,
            &MetaCommand::CreateRegion(RegionInfo::new(1, "cpu", 1, vec![1])),
        );

        // Begin migration from 1 → 2
        sm.apply(
            5,
            &MetaCommand::BeginMigration {
                region_id: 1,
                source_node_id: 1,
                dest_node_id: 2,
            },
        );

        // Try to complete with wrong source (node 3 not in replica set)
        let resp = sm.apply(
            6,
            &MetaCommand::MigrateRegion {
                region_id: 1,
                source_node_id: 3,
                dest_node_id: 2,
            },
        );
        assert!(matches!(resp, MetaResponse::Error { .. }));
    }

    #[test]
    fn begin_migration_rejects_non_active() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(2, &MetaCommand::RegisterNode(DataNodeInfo::new(2, "a:2")));
        sm.apply(3, &MetaCommand::RegisterNode(DataNodeInfo::new(3, "a:3")));
        sm.apply(
            4,
            &MetaCommand::CreateRegion(RegionInfo::new(1, "cpu", 1, vec![1])),
        );

        // Begin migration 1 → 2
        sm.apply(
            5,
            &MetaCommand::BeginMigration {
                region_id: 1,
                source_node_id: 1,
                dest_node_id: 2,
            },
        );

        // Try to begin again (region is now Migrating, not Active)
        let resp = sm.apply(
            6,
            &MetaCommand::BeginMigration {
                region_id: 1,
                source_node_id: 1,
                dest_node_id: 3,
            },
        );
        assert!(matches!(resp, MetaResponse::Error { .. }));
    }

    #[test]
    fn begin_migration_rejects_non_leader_source() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(2, &MetaCommand::RegisterNode(DataNodeInfo::new(2, "a:2")));
        sm.apply(
            3,
            &MetaCommand::CreateRegion(RegionInfo::new(1, "cpu", 1, vec![1])),
        );

        // Node 2 is not the leader
        let resp = sm.apply(
            4,
            &MetaCommand::BeginMigration {
                region_id: 1,
                source_node_id: 2,
                dest_node_id: 1,
            },
        );
        assert!(matches!(resp, MetaResponse::Error { .. }));
    }

    #[test]
    fn begin_migration_rejects_dead_dest() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(2, &MetaCommand::RegisterNode(DataNodeInfo::new(2, "a:2")));
        sm.apply(
            3,
            &MetaCommand::CreateRegion(RegionInfo::new(1, "cpu", 1, vec![1])),
        );
        sm.apply(
            4,
            &MetaCommand::UpdateNodeState {
                node_id: 2,
                state: NodeState::Dead,
            },
        );

        let resp = sm.apply(
            5,
            &MetaCommand::BeginMigration {
                region_id: 1,
                source_node_id: 1,
                dest_node_id: 2,
            },
        );
        assert!(matches!(resp, MetaResponse::Error { .. }));
    }

    #[test]
    fn begin_migration_rejects_same_source_dest() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "a:1")));
        sm.apply(
            2,
            &MetaCommand::CreateRegion(RegionInfo::new(1, "cpu", 1, vec![1])),
        );

        let resp = sm.apply(
            3,
            &MetaCommand::BeginMigration {
                region_id: 1,
                source_node_id: 1,
                dest_node_id: 1,
            },
        );
        assert!(matches!(resp, MetaResponse::Error { .. }));
    }

    #[test]
    fn begin_migration_serde_roundtrip() {
        let cmd = MetaCommand::BeginMigration {
            region_id: 1,
            source_node_id: 10,
            dest_node_id: 20,
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let restored: MetaCommand = serde_json::from_str(&json).unwrap();
        match restored {
            MetaCommand::BeginMigration {
                region_id,
                source_node_id,
                dest_node_id,
            } => {
                assert_eq!(region_id, 1);
                assert_eq!(source_node_id, 10);
                assert_eq!(dest_node_id, 20);
            }
            _ => panic!("wrong variant"),
        }
    }

    // ── Batch heartbeat replication ────────────────────────

    #[test]
    fn batch_heartbeat_updates_node_info() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "addr1")));
        sm.apply(2, &MetaCommand::RegisterNode(DataNodeInfo::new(2, "addr2")));

        let mut entries = BTreeMap::new();
        entries.insert(1, (10_u64, 1000_u64));
        entries.insert(2, (20_u64, 2000_u64));

        let resp = sm.apply(3, &MetaCommand::BatchHeartbeat { entries });
        assert!(matches!(resp, MetaResponse::Ok));

        // Raft-replicated field updated
        let node1 = sm.get_node(1).unwrap();
        assert_eq!(node1.last_heartbeat_secs, 1000);
        assert_eq!(node1.heartbeat_generation, 10);

        let node2 = sm.get_node(2).unwrap();
        assert_eq!(node2.last_heartbeat_secs, 2000);

        // Out-of-band heartbeat store also updated
        let hb = sm.heartbeat_data();
        assert_eq!(hb.get(&1), Some(&(10, 1000)));
        assert_eq!(hb.get(&2), Some(&(20, 2000)));
    }

    #[test]
    fn batch_heartbeat_ignores_unknown_nodes() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "addr")));

        let mut entries = BTreeMap::new();
        entries.insert(1, (1_u64, 100_u64));
        entries.insert(999, (1_u64, 100_u64)); // unknown node

        sm.apply(2, &MetaCommand::BatchHeartbeat { entries });

        // Known node updated
        assert_eq!(sm.get_node(1).unwrap().last_heartbeat_secs, 100);
        // Unknown node: not in Raft state, but present in heartbeat store
        assert!(sm.get_node(999).is_none());
        assert_eq!(sm.heartbeat_data().get(&999), Some(&(1, 100)));
    }

    #[test]
    fn snapshot_includes_heartbeat_store() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "addr")));
        sm.record_heartbeat(1, 42);

        let snap = sm.snapshot();
        assert!(!snap.heartbeat_store.is_empty());
        let (gen, _ts) = snap.heartbeat_store[&1];
        assert_eq!(gen, 42);
    }

    #[test]
    fn restore_repopulates_heartbeat_store() {
        let sm = MetaStateMachine::new();
        sm.apply(1, &MetaCommand::RegisterNode(DataNodeInfo::new(1, "addr")));
        sm.record_heartbeat(1, 42);
        let snap = sm.snapshot();

        // New state machine (simulates leader failover)
        let sm2 = MetaStateMachine::new();
        assert!(sm2.heartbeat_data().is_empty());

        sm2.restore(snap);
        let hb = sm2.heartbeat_data();
        assert_eq!(hb.get(&1).unwrap().0, 42);
    }

    #[test]
    fn batch_heartbeat_serde_roundtrip() {
        let mut entries = BTreeMap::new();
        entries.insert(1_u64, (5_u64, 123_u64));
        entries.insert(2, (10, 456));
        let cmd = MetaCommand::BatchHeartbeat { entries };
        let json = serde_json::to_string(&cmd).unwrap();
        let restored: MetaCommand = serde_json::from_str(&json).unwrap();
        match restored {
            MetaCommand::BatchHeartbeat { entries } => {
                assert_eq!(entries.len(), 2);
                assert_eq!(entries[&1], (5, 123));
            }
            _ => panic!("wrong variant"),
        }
    }
}
