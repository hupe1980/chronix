//! Cluster coordinator — orchestrates health checks and rebalancing.
//!
//! Runs on the `MetaNode` leader and periodically inspects cluster state
//! to detect dead nodes and plan region migrations.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use parking_lot::Mutex;

use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn};

use chronix_meta::{
    ClusterConfig, DataNodeInfo, MetaCommand, MetaRouter, MetaStore, NodeId, NodeState, RegionId,
    RegionInfo,
};

use crate::client::MetaClient;

/// Result of a cluster health check.
///
/// Lists nodes that should be transitioned to [`NodeState::Suspect`] or
/// [`NodeState::Dead`] based on missed heartbeats.
#[derive(Debug, Clone, Default)]
pub struct HealthCheckResult {
    /// Nodes whose heartbeats are late but not yet dead.
    pub suspect_nodes: Vec<NodeId>,
    /// Nodes whose heartbeats exceeded the dead threshold.
    pub dead_nodes: Vec<NodeId>,
}

impl HealthCheckResult {
    /// Returns `true` if no state changes are needed.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.suspect_nodes.is_empty() && self.dead_nodes.is_empty()
    }

    /// Total number of nodes with health issues.
    #[must_use]
    pub fn unhealthy_count(&self) -> usize {
        self.suspect_nodes.len() + self.dead_nodes.len()
    }
}

/// A planned migration of a region from one node to another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionMigration {
    /// Region to move.
    pub region_id: RegionId,
    /// Source node (currently the leader).
    pub from_node: NodeId,
    /// Destination node.
    pub to_node: NodeId,
}

/// Cluster coordinator that runs on the `MetaNode` leader.
///
/// Provides read-only analysis of cluster state: health checking,
/// under-replication detection, and rebalance planning. The caller is
/// responsible for proposing the resulting changes through Raft.
#[derive(Clone)]
pub struct ClusterCoordinator {
    /// Metadata store for reading cluster state.
    meta_store: MetaStore,
    /// In-process Raft network router.
    router: MetaRouter,
    /// Cluster-wide configuration.
    config: ClusterConfig,
    /// Cancellation token for background tasks.
    shutdown: CancellationToken,
    /// Timestamp of the last observed leader change.
    ///
    /// When set, health checks suppress suspect/dead declarations for
    /// one full suspect-threshold period so that heartbeats have time
    /// to propagate to the new leader.
    leader_change_at: Arc<Mutex<Option<Instant>>>,
}

impl ClusterCoordinator {
    /// Create a new coordinator.
    #[must_use]
    pub fn new(meta_store: MetaStore, router: MetaRouter, config: ClusterConfig) -> Self {
        Self {
            meta_store,
            router,
            config,
            shutdown: CancellationToken::new(),
            leader_change_at: Arc::new(Mutex::new(None)),
        }
    }

    /// Record a leader change so that the next health checks honour the
    /// grace period.
    ///
    /// While the grace period is active (`suspect_threshold` seconds
    /// after this call) `check_health()` will treat all nodes as alive
    /// to avoid spurious suspect/dead declarations caused by divergent
    /// heartbeat views across replicas.
    pub fn record_leader_change(&self) {
        let mut ts = self.leader_change_at.lock();
        info!("coordinator: leader change recorded - starting grace period");
        *ts = Some(Instant::now());
    }

    /// Returns `true` while the leader-change grace period is active.
    ///
    /// Grace period uses `dead_threshold × heartbeat_interval`
    /// (not `suspect_threshold`) so that nodes have enough time to
    /// re-establish heartbeats with the new leader. Out-of-band heartbeat
    /// state from the old leader is lost on failover, so we need the full
    /// dead window before declaring any node dead.
    fn in_leader_grace_period(&self) -> bool {
        let grace_duration = Duration::from_secs(
            u64::from(self.config.heartbeat_dead_threshold) * self.config.heartbeat_interval_secs,
        );

        let mut guard = self.leader_change_at.lock();

        if let Some(ts) = *guard {
            if ts.elapsed() < grace_duration {
                return true;
            }
            // Grace period expired – clear the flag.
            *guard = None;
        }
        false
    }

    /// Scan all registered nodes and detect suspects / dead nodes.
    ///
    /// Uses the out-of-band heartbeat store for liveness timestamps,
    /// falling back to Raft-replicated `last_heartbeat_secs` when the
    /// heartbeat store has no entry.
    ///
    /// Nodes in [`NodeState::Decommissioning`] are skipped.
    ///
    /// During the leader-change grace period all nodes are treated as
    /// alive to prevent spurious declarations.
    #[must_use]
    #[instrument(name = "health_check", skip(self))]
    pub fn check_health(&self) -> HealthCheckResult {
        // Suppress suspect/dead declarations right after a
        // leader change so heartbeats can propagate to the new leader.
        if self.in_leader_grace_period() {
            debug!("coordinator: leader-change grace period active – skipping health check");
            return HealthCheckResult::default();
        }

        let nodes = self.meta_store.state_machine().nodes();

        // Prefer monotonic `Instant`-based ages for
        // liveness detection.  SystemTime is subject to NTP jumps
        // that cause false deaths or resurrections.  The Instant
        // tracker is populated by `record_heartbeat()` and is immune
        // to wall-clock corrections.  Fall back to epoch-seconds for
        // nodes that have only Raft-replicated heartbeats (remote
        // nodes that haven't sent out-of-band heartbeats to *this*
        // leader).
        let instants = self.meta_store.state_machine().heartbeat_instants();
        let heartbeats = self.meta_store.state_machine().heartbeat_data();
        let now_instant = Instant::now();
        let now_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();

        let suspect_secs = u64::from(self.config.heartbeat_suspect_threshold)
            * self.config.heartbeat_interval_secs;
        let dead_secs =
            u64::from(self.config.heartbeat_dead_threshold) * self.config.heartbeat_interval_secs;

        let mut result = HealthCheckResult::default();

        for (&node_id, info) in &nodes {
            if info.state == NodeState::Decommissioning {
                continue;
            }

            // Prefer monotonic Instant if available.
            let age_secs = if let Some(&inst) = instants.get(&node_id) {
                now_instant.duration_since(inst).as_secs()
            } else {
                // Fall back to epoch-seconds (Raft-replicated path).
                let last_hb = heartbeats
                    .get(&node_id)
                    .map_or(info.last_heartbeat_secs, |&(_, ts)| ts);
                now_epoch.saturating_sub(last_hb)
            };

            if age_secs >= dead_secs {
                result.dead_nodes.push(node_id);
            } else if age_secs >= suspect_secs {
                result.suspect_nodes.push(node_id);
            }
        }

        result
    }

    /// Return regions that have fewer active replicas than their target
    /// replication factor.
    #[must_use]
    pub fn under_replicated_regions(&self) -> Vec<RegionInfo> {
        self.meta_store.state_machine().under_replicated_regions()
    }

    /// Compute a rebalance plan based on leader distribution.
    ///
    /// The algorithm counts how many regions each active node leads and
    /// proposes migrations to even out the distribution.
    ///
    /// Returns an empty list if rebalancing is not possible (e.g. fewer
    /// than two active nodes).
    #[must_use]
    pub fn plan_rebalance(&self) -> Vec<RegionMigration> {
        let sm = self.meta_store.state_machine();
        let nodes = sm.nodes();
        let regions = sm.regions();

        // Only consider active nodes.
        let active_ids: Vec<NodeId> = nodes
            .values()
            .filter(|n| n.state == NodeState::Active)
            .map(|n| n.node_id)
            .collect();

        if active_ids.len() <= 1 {
            return Vec::new();
        }

        // Count leader regions per active node.
        let mut load: BTreeMap<NodeId, Vec<RegionId>> =
            active_ids.iter().map(|&nid| (nid, Vec::new())).collect();

        for (&rid, rinfo) in &regions {
            if let Some(bucket) = load.get_mut(&rinfo.leader_node_id) {
                bucket.push(rid);
            }
        }

        let total: usize = load.values().map(Vec::len).sum();
        let n = active_ids.len();
        let lo = total / n;
        let hi = lo + 1;
        let hi_count = total % n;

        // Sort descending by region count for deterministic donor ordering.
        let mut sorted: Vec<(NodeId, Vec<RegionId>)> = load.into_iter().collect();
        sorted.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then_with(|| a.0.cmp(&b.0)));

        let mut surplus: Vec<(NodeId, RegionId)> = Vec::new();
        let mut deficit: Vec<(NodeId, usize)> = Vec::new();

        for (i, (nid, rids)) in sorted.iter().enumerate() {
            let target = if i < hi_count { hi } else { lo };

            if rids.len() > target {
                for rid in &rids[target..] {
                    surplus.push((*nid, *rid));
                }
            } else if rids.len() < target {
                deficit.push((*nid, target - rids.len()));
            }
        }

        let mut migrations = Vec::new();
        let mut surplus_iter = surplus.into_iter();

        for (to_node, need) in deficit {
            for _ in 0..need {
                if let Some((from_node, region_id)) = surplus_iter.next() {
                    migrations.push(RegionMigration {
                        region_id,
                        from_node,
                        to_node,
                    });
                }
            }
        }

        migrations
    }

    /// Return all currently active (non-dead) data nodes.
    #[must_use]
    pub fn active_nodes(&self) -> Vec<DataNodeInfo> {
        self.meta_store.state_machine().active_nodes()
    }

    /// Plan topology-aware replica repair for under-replicated regions.
    ///
    /// For each region that has fewer replicas than its replication
    /// factor, select new target nodes using the given
    /// [`PlacementPolicy`](crate::placement::PlacementPolicy) to
    /// maximise failure-domain diversity.
    ///
    /// Returns a list of `(region_id, new_target_node)` pairs.
    #[must_use]
    pub fn plan_repair(
        &self,
        policy: &crate::placement::PlacementPolicy,
    ) -> Vec<(RegionId, NodeId)> {
        let sm = self.meta_store.state_machine();
        let nodes = sm.nodes();
        let regions = sm.regions();

        let mut repairs = Vec::new();

        for (&rid, rinfo) in &regions {
            let deficit = rinfo
                .replication_factor
                .saturating_sub(rinfo.replica_node_ids.len() as u32);
            if deficit == 0 {
                continue;
            }
            let exclude: std::collections::HashSet<NodeId> =
                rinfo.replica_node_ids.iter().copied().collect();
            let targets = policy.select_targets(&nodes, deficit as usize, &exclude);
            for nid in targets {
                repairs.push((rid, nid));
            }
        }

        repairs
    }

    /// Access the underlying router.
    #[must_use]
    pub fn router(&self) -> &MetaRouter {
        &self.router
    }

    /// Access the cluster configuration.
    #[must_use]
    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Returns the shutdown token.
    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Run the periodic health check loop.
    ///
    /// Checks cluster health at the heartbeat interval, proposes
    /// node state transitions through the given `MetaClient`, and logs
    /// under-replicated regions. Runs until the shutdown token is
    /// cancelled.
    ///
    /// # Errors
    ///
    /// Returns an error only if the `MetaClient` returns a permanent
    /// failure. Transient errors are logged and retried on the next tick.
    pub async fn run_health_loop(&self, client: Arc<dyn MetaClient>) -> crate::error::Result<()> {
        let interval = Duration::from_secs(self.config.heartbeat_interval_secs);
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!(
            interval_secs = self.config.heartbeat_interval_secs,
            "coordinator: health loop started"
        );

        loop {
            tokio::select! {
                () = self.shutdown.cancelled() => {
                    info!("coordinator: health loop shutting down");
                    return Ok(());
                }
                _ = tick.tick() => {
                    self.run_health_tick(&*client).await;
                }
            }
        }
    }

    /// Execute one health check tick: detect suspect/dead nodes and propose
    /// state changes through Raft.
    ///
    /// Also replicates out-of-band heartbeat timestamps through Raft
    /// via `BatchHeartbeat` so that followers have a warm view
    /// of node liveness for failover.
    async fn run_health_tick(&self, client: &dyn MetaClient) {
        // Batch-replicate all out-of-band heartbeats through Raft
        // so followers (and future leaders) stay warm.
        let heartbeats = self.meta_store.state_machine().heartbeat_data();
        if !heartbeats.is_empty() {
            if let Err(e) = client
                .propose(MetaCommand::BatchHeartbeat {
                    entries: heartbeats,
                })
                .await
            {
                warn!(error = %e, "coordinator: failed to replicate batch heartbeats");
            }
        }

        let result = self.check_health();

        if result.is_healthy() {
            debug!("coordinator: cluster healthy");
            return;
        }

        // Propose state transitions for suspect nodes.
        for &node_id in &result.suspect_nodes {
            info!(node_id, "coordinator: marking node suspect");
            if let Err(e) = client
                .propose(MetaCommand::UpdateNodeState {
                    node_id,
                    state: NodeState::Suspect,
                })
                .await
            {
                warn!(node_id, error = %e, "coordinator: failed to mark suspect");
            }
        }

        // Propose state transitions for dead nodes.
        for &node_id in &result.dead_nodes {
            info!(node_id, "coordinator: marking node dead");
            if let Err(e) = client
                .propose(MetaCommand::UpdateNodeState {
                    node_id,
                    state: NodeState::Dead,
                })
                .await
            {
                warn!(node_id, error = %e, "coordinator: failed to mark dead");
            }
        }

        // Log under-replicated regions.
        let under = self.under_replicated_regions();
        if !under.is_empty() {
            warn!(
                count = under.len(),
                "coordinator: under-replicated regions detected"
            );
            for r in &under {
                debug!(
                    region_id = r.region_id,
                    measurement = %r.measurement,
                    "coordinator: under-replicated region"
                );
            }
            crate::metrics::set_under_replicated(under.len().try_into().unwrap_or(u64::MAX));
        }
    }
}

impl std::fmt::Debug for ClusterCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterCoordinator")
            .field("config", &self.config)
            .field("router", &self.router)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronix_meta::{DataNodeInfo, MetaCommand, MetaRouter, MetaStore, RegionInfo};

    /// Create a `DataNodeInfo` with a specific `last_heartbeat_secs`.
    fn node_with_heartbeat(node_id: NodeId, last_hb: u64) -> DataNodeInfo {
        let mut info = DataNodeInfo::new(node_id, format!("127.0.0.1:{}", 9000 + node_id));
        info.last_heartbeat_secs = last_hb;
        info
    }

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn default_coord(store: MetaStore) -> ClusterCoordinator {
        ClusterCoordinator::new(store, MetaRouter::new(), ClusterConfig::default())
    }

    // ── Health checks ───────────────────────────────────────────

    #[test]
    fn healthy_when_no_nodes() {
        let store = MetaStore::new_in_memory();
        let coord = default_coord(store);

        let result = coord.check_health();
        assert!(result.is_healthy());
        assert_eq!(result.unhealthy_count(), 0);
    }

    #[test]
    fn healthy_when_heartbeats_recent() {
        let store = MetaStore::new_in_memory();
        let sm = store.state_machine();

        sm.apply(
            1,
            &MetaCommand::RegisterNode(node_with_heartbeat(1, now_secs())),
        );
        sm.apply(
            2,
            &MetaCommand::RegisterNode(node_with_heartbeat(2, now_secs())),
        );

        let coord = default_coord(store);
        let result = coord.check_health();
        assert!(result.is_healthy());
    }

    #[test]
    fn detects_suspect_nodes() {
        let store = MetaStore::new_in_memory();
        let sm = store.state_machine();
        let config = ClusterConfig::default();

        // Suspect threshold: heartbeat_suspect_threshold * heartbeat_interval_secs
        // Default: 2 * 5 = 10 seconds
        let suspect_age =
            u64::from(config.heartbeat_suspect_threshold) * config.heartbeat_interval_secs + 1;

        sm.apply(
            1,
            &MetaCommand::RegisterNode(node_with_heartbeat(1, now_secs())),
        );
        sm.apply(
            2,
            &MetaCommand::RegisterNode(node_with_heartbeat(
                2,
                now_secs().saturating_sub(suspect_age),
            )),
        );

        let coord = default_coord(store);
        let result = coord.check_health();
        assert!(result.suspect_nodes.contains(&2));
        assert!(!result.suspect_nodes.contains(&1));
    }

    #[test]
    fn detects_dead_nodes() {
        let store = MetaStore::new_in_memory();
        let sm = store.state_machine();
        let config = ClusterConfig::default();

        // Dead threshold: heartbeat_dead_threshold * heartbeat_interval_secs
        // Default: 6 * 5 = 30 seconds
        let dead_age =
            u64::from(config.heartbeat_dead_threshold) * config.heartbeat_interval_secs + 1;

        sm.apply(
            1,
            &MetaCommand::RegisterNode(node_with_heartbeat(1, now_secs())),
        );
        sm.apply(
            2,
            &MetaCommand::RegisterNode(node_with_heartbeat(2, now_secs().saturating_sub(dead_age))),
        );

        let coord = default_coord(store);
        let result = coord.check_health();
        assert!(result.dead_nodes.contains(&2));
        assert!(!result.dead_nodes.contains(&1));
    }

    #[test]
    fn skips_decommissioning_nodes() {
        let store = MetaStore::new_in_memory();
        let sm = store.state_machine();

        // Register with an old heartbeat but then mark as decommissioning.
        let mut info = node_with_heartbeat(1, 0); // very old
        info.state = NodeState::Decommissioning;
        sm.apply(1, &MetaCommand::RegisterNode(info));

        let coord = default_coord(store);
        let result = coord.check_health();
        assert!(
            result.is_healthy(),
            "decommissioning nodes should be skipped"
        );
    }

    // ── Leader-change grace period ───────────────────────

    #[test]
    fn grace_period_suppresses_suspect_and_dead() {
        let store = MetaStore::new_in_memory();
        let sm = store.state_machine();
        let config = ClusterConfig::default();

        let dead_age =
            u64::from(config.heartbeat_dead_threshold) * config.heartbeat_interval_secs + 1;

        // Register a node whose heartbeat is well past the dead threshold.
        sm.apply(
            1,
            &MetaCommand::RegisterNode(node_with_heartbeat(1, now_secs().saturating_sub(dead_age))),
        );

        let coord = default_coord(store);

        // Without grace period the node is detected as dead.
        let result = coord.check_health();
        assert!(result.dead_nodes.contains(&1));

        // Record a leader change – grace period starts.
        coord.record_leader_change();

        // Now check_health must suppress all declarations.
        let result = coord.check_health();
        assert!(
            result.is_healthy(),
            "declarations should be suppressed during leader-change grace period"
        );
    }

    #[test]
    fn grace_period_expires_and_resumes_detection() {
        let store = MetaStore::new_in_memory();
        let sm = store.state_machine();

        // Use a very short suspect threshold so the grace period is tiny.
        let config = ClusterConfig {
            heartbeat_interval_secs: 1,
            heartbeat_suspect_threshold: 1, // grace = 1 second
            heartbeat_dead_threshold: 2,
            ..Default::default()
        };

        // Register a node with a heartbeat clearly beyond dead_secs but
        // within the clock-skew tolerance window.
        let dead_secs = u64::from(config.heartbeat_dead_threshold) * config.heartbeat_interval_secs;
        let stale_hb = now_secs() - dead_secs - 1;
        sm.apply(
            1,
            &MetaCommand::RegisterNode(node_with_heartbeat(1, stale_hb)),
        );
        let coord = ClusterCoordinator::new(store, MetaRouter::new(), config);

        coord.record_leader_change();

        // Immediately after, the grace period is active.
        assert!(coord.check_health().is_healthy());

        // Manually expire the grace period by back-dating the timestamp.
        {
            let mut guard = coord.leader_change_at.lock();
            *guard = Some(Instant::now() - Duration::from_secs(2));
        }

        // Now health check should detect the dead node again.
        let result = coord.check_health();
        assert!(
            !result.is_healthy(),
            "declarations should resume after grace period expires"
        );
    }

    #[test]
    fn record_leader_change_is_idempotent() {
        let store = MetaStore::new_in_memory();
        let coord = default_coord(store);

        coord.record_leader_change();
        assert!(coord.in_leader_grace_period());

        // Calling again just resets the timer – still in grace period.
        coord.record_leader_change();
        assert!(coord.in_leader_grace_period());
    }

    // ── Under-replicated regions ────────────────────────────────

    #[test]
    fn no_under_replicated_when_empty() {
        let store = MetaStore::new_in_memory();
        let coord = default_coord(store);
        assert!(coord.under_replicated_regions().is_empty());
    }

    #[test]
    fn detects_under_replicated_regions() {
        let store = MetaStore::new_in_memory();
        let sm = store.state_machine();

        // Register one active node.
        sm.apply(
            1,
            &MetaCommand::RegisterNode(node_with_heartbeat(1, now_secs())),
        );

        // Create a region with replication_factor = 3 but only 1 replica.
        let mut region = RegionInfo::new(1, "cpu", 1, vec![1]);
        region.replication_factor = 3;
        sm.apply(2, &MetaCommand::CreateRegion(region));

        let coord = default_coord(store);
        let under = coord.under_replicated_regions();
        assert_eq!(under.len(), 1);
        assert_eq!(under[0].region_id, 1);
    }

    // ── Rebalance planning ──────────────────────────────────────

    #[test]
    fn rebalance_empty_cluster() {
        let store = MetaStore::new_in_memory();
        let coord = default_coord(store);
        assert!(coord.plan_rebalance().is_empty());
    }

    #[test]
    fn rebalance_single_node_noop() {
        let store = MetaStore::new_in_memory();
        let sm = store.state_machine();

        sm.apply(
            1,
            &MetaCommand::RegisterNode(node_with_heartbeat(1, now_secs())),
        );
        sm.apply(
            2,
            &MetaCommand::CreateRegion(RegionInfo::new(1, "cpu", 1, vec![1])),
        );

        let coord = default_coord(store);
        assert!(coord.plan_rebalance().is_empty());
    }

    #[test]
    fn rebalance_distributes_evenly() {
        let store = MetaStore::new_in_memory();
        let sm = store.state_machine();

        // 3 nodes, all active.
        for id in 1..=3 {
            sm.apply(
                id,
                &MetaCommand::RegisterNode(node_with_heartbeat(id, now_secs())),
            );
        }

        // 3 regions all on node 1.
        for rid in 1..=3 {
            sm.apply(
                10 + rid,
                &MetaCommand::CreateRegion(RegionInfo::new(rid, "cpu", 1, vec![1])),
            );
        }

        let coord = default_coord(store);
        let migrations = coord.plan_rebalance();

        // Ideal: 1 region per node → 2 migrations off node 1.
        assert_eq!(migrations.len(), 2);
        for m in &migrations {
            assert_eq!(m.from_node, 1);
            assert_ne!(m.to_node, 1);
        }
    }

    #[test]
    fn rebalance_already_balanced() {
        let store = MetaStore::new_in_memory();
        let sm = store.state_machine();

        // 3 nodes, 1 region each.
        for id in 1..=3 {
            sm.apply(
                id,
                &MetaCommand::RegisterNode(node_with_heartbeat(id, now_secs())),
            );
            sm.apply(
                10 + id,
                &MetaCommand::CreateRegion(RegionInfo::new(id, "cpu", id, vec![id])),
            );
        }

        let coord = default_coord(store);
        assert!(coord.plan_rebalance().is_empty());
    }

    // ── Active nodes ────────────────────────────────────────────

    #[test]
    fn active_nodes_excludes_dead() {
        let store = MetaStore::new_in_memory();
        let sm = store.state_machine();

        sm.apply(
            1,
            &MetaCommand::RegisterNode(node_with_heartbeat(1, now_secs())),
        );

        let mut dead = node_with_heartbeat(2, now_secs());
        dead.state = NodeState::Dead;
        sm.apply(2, &MetaCommand::RegisterNode(dead));

        let coord = default_coord(store);
        let active = coord.active_nodes();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].node_id, 1);
    }

    // ── Accessors ───────────────────────────────────────────────

    #[test]
    fn debug_and_accessors() {
        let store = MetaStore::new_in_memory();
        let config = ClusterConfig::default();
        let router = MetaRouter::new();

        let coord = ClusterCoordinator::new(store, router, config.clone());
        assert_eq!(coord.config().cluster_name, config.cluster_name);
        assert_eq!(coord.router().node_count(), 0);

        let debug = format!("{coord:?}");
        assert!(debug.contains("ClusterCoordinator"));

        // Shutdown token should be usable.
        let token = coord.shutdown_token();
        assert!(!token.is_cancelled());
    }

    // ── Health loop ─────────────────────────────────────────────

    #[tokio::test]
    async fn health_tick_marks_dead_node() {
        use crate::client::InProcessMetaClient;
        use chronix_meta::{MetaNetworkFactory, MetaTypeConfig};
        use openraft::BasicNode;

        // Build single-node Raft cluster.
        let router = MetaRouter::new();
        let store = MetaStore::new_in_memory();

        let raft_config = Arc::new(
            openraft::Config {
                heartbeat_interval: 100,
                election_timeout_min: 200,
                election_timeout_max: 400,
                ..Default::default()
            }
            .validate()
            .expect("valid raft config"),
        );

        let net = MetaNetworkFactory::new(router.clone());
        let raft = openraft::Raft::<MetaTypeConfig>::new(
            1,
            raft_config,
            net,
            store.log_store(),
            store.sm_store(),
        )
        .await
        .expect("raft init");

        router.add_node(1, raft.clone());

        let mut members = BTreeMap::new();
        members.insert(1u64, BasicNode::new("127.0.0.1:9001"));
        raft.initialize(members).await.expect("init");

        // Wait for leader.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let raft = Arc::new(raft);
        let client = Arc::new(InProcessMetaClient::new(raft.clone(), store.sm_store()));

        // Register a node with a heartbeat beyond dead_secs but within
        // the clock-skew tolerance.
        let default_config = ClusterConfig::default();
        let dead_secs = u64::from(default_config.heartbeat_dead_threshold)
            * default_config.heartbeat_interval_secs;
        let stale_hb = now_secs() - dead_secs - 1;
        client
            .propose(MetaCommand::RegisterNode(node_with_heartbeat(2, stale_hb)))
            .await
            .expect("register");

        // Confirm node 2 is initially Active.
        let info = store.state_machine().get_node(2).unwrap();
        assert_eq!(info.state, NodeState::Active);

        // Run one health tick.
        let coord = ClusterCoordinator::new(store, router, ClusterConfig::default());
        coord.run_health_tick(&*client).await;

        // Node 2 should now be Dead (heartbeat age >>> dead threshold).
        let info = coord.meta_store.state_machine().get_node(2).unwrap();
        assert_eq!(info.state, NodeState::Dead);
    }

    #[tokio::test]
    async fn health_loop_stops_on_shutdown() {
        use crate::client::InProcessMetaClient;
        use chronix_meta::{MetaNetworkFactory, MetaTypeConfig};
        use openraft::BasicNode;

        let router = MetaRouter::new();
        let store = MetaStore::new_in_memory();

        let raft_config = Arc::new(
            openraft::Config {
                heartbeat_interval: 100,
                election_timeout_min: 200,
                election_timeout_max: 400,
                ..Default::default()
            }
            .validate()
            .expect("valid raft config"),
        );

        let net = MetaNetworkFactory::new(router.clone());
        let raft = openraft::Raft::<MetaTypeConfig>::new(
            1,
            raft_config,
            net,
            store.log_store(),
            store.sm_store(),
        )
        .await
        .expect("raft init");

        router.add_node(1, raft.clone());

        let mut members = BTreeMap::new();
        members.insert(1u64, BasicNode::new("127.0.0.1:9001"));
        raft.initialize(members).await.expect("init");

        tokio::time::sleep(Duration::from_millis(500)).await;

        let raft = Arc::new(raft);
        let client: Arc<dyn MetaClient> =
            Arc::new(InProcessMetaClient::new(raft.clone(), store.sm_store()));

        let config = ClusterConfig {
            heartbeat_interval_secs: 1,
            ..Default::default()
        };
        let coord = ClusterCoordinator::new(store, router, config);
        let token = coord.shutdown_token();

        let handle = tokio::spawn(async move { coord.run_health_loop(client).await });

        // Let it run briefly then cancel.
        tokio::time::sleep(Duration::from_millis(300)).await;
        token.cancel();

        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("should finish within timeout")
            .expect("task should not panic");

        assert!(result.is_ok(), "health loop should exit cleanly");
    }
}
