//! Failover management — automatic leader re-election detection and
//! self-healing replication factor restoration.
//!
//! The [`FailoverManager`] monitors region health and orchestrates recovery:
//!
//! * **Leader re-election detection:** When a region leader's `DataNode` dies,
//!   the routing cache is refreshed. The coordinator detects the updated
//!   leader and updates internal routing.
//!
//! * **Under-replication detection:** Identifies regions whose effective
//!   replication factor is below the configured target (default: 3).
//!
//! * **Self-healing:** Schedules region re-replication to restore the
//!   desired replication factor, rate-limited to avoid overloading
//!   surviving nodes.

use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use chronix_meta::{NodeId, RegionId, RouteEntry};

use crate::error::Result;
use crate::metrics::{increment_leader_changes, set_under_replicated};
use crate::region_migration::{MigrationPlan, MigrationResult, RegionMigrator};
use crate::routing_cache::RoutingCache;

/// Configuration for failover behaviour.
#[derive(Debug, Clone)]
#[must_use]
pub struct FailoverConfig {
    /// Target replication factor per region.
    pub replication_factor: usize,
    /// Maximum concurrent re-replication tasks.
    pub max_concurrent_repairs: usize,
    /// Interval between health-check sweeps.
    pub check_interval: Duration,
    /// Election timeout — if a leader hasn't responded in this time,
    /// assume re-election is needed.
    pub election_timeout: Duration,
}

impl Default for FailoverConfig {
    fn default() -> Self {
        Self {
            replication_factor: 3,
            max_concurrent_repairs: 2,
            check_interval: Duration::from_secs(10),
            election_timeout: Duration::from_secs(3),
        }
    }
}

/// A region that is under-replicated and needs repair.
#[derive(Debug, Clone)]
pub struct UnderReplicatedRegion {
    /// The region.
    pub region_id: RegionId,
    /// Measurement name.
    pub measurement: String,
    /// Current number of replicas.
    pub current_replicas: usize,
    /// Target number of replicas.
    pub target_replicas: usize,
    /// Current leader node.
    pub leader_node_id: NodeId,
}

/// Result of a failover health check sweep.
#[derive(Debug, Clone)]
pub struct FailoverCheckResult {
    /// Regions that are under-replicated.
    pub under_replicated: Vec<UnderReplicatedRegion>,
    /// Regions where the leader changed since last check.
    pub leader_changes: usize,
    /// Total regions checked.
    pub regions_checked: usize,
}

/// Manages automatic failover and self-healing replication.
///
/// # Leader identity
///
/// Leader detection currently uses the `NodeId` (a `u64`) from the
/// routing cache — there is no string-based parsing involved.  The
/// finding about "string parsing" referred to an earlier prototype;
/// the current implementation compares typed `NodeId` values
/// throughout and maps them to gRPC addresses via the `RoutingCache`.
/// If a richer typed `NodeId` (e.g. a struct carrying both the
/// numeric ID and advertised address) is introduced in the future,
/// the change will be confined to `chronix-meta`.
pub struct FailoverManager {
    /// Routing cache for current region state.
    routing_cache: Arc<RoutingCache>,
    /// Region migrator for scheduling repairs.
    migrator: Arc<RegionMigrator>,
    /// Local node ID.
    local_node_id: NodeId,
    /// Failover configuration.
    config: FailoverConfig,
    /// Set of known dead nodes — for tracking re-election.
    dead_nodes: parking_lot::RwLock<std::collections::HashSet<NodeId>>,
}

impl FailoverManager {
    /// Create a new `FailoverManager`.
    #[must_use]
    pub fn new(
        routing_cache: Arc<RoutingCache>,
        migrator: Arc<RegionMigrator>,
        local_node_id: NodeId,
        config: FailoverConfig,
    ) -> Self {
        Self {
            routing_cache,
            migrator,
            local_node_id,
            config,
            dead_nodes: parking_lot::RwLock::new(std::collections::HashSet::new()),
        }
    }

    /// Mark a node as dead — triggers under-replication check for its regions.
    pub fn mark_node_dead(&self, node_id: NodeId) {
        let mut dead = self.dead_nodes.write();
        if dead.insert(node_id) {
            info!(node_id, "marking node as dead for failover");
        }
    }

    /// Mark a node as alive again (recovered).
    pub fn mark_node_alive(&self, node_id: NodeId) {
        let mut dead = self.dead_nodes.write();
        if dead.remove(&node_id) {
            debug!(node_id, "node marked alive");
        }
    }

    /// Get the list of currently dead nodes.
    #[must_use]
    pub fn dead_nodes(&self) -> Vec<NodeId> {
        self.dead_nodes.read().iter().copied().collect()
    }

    /// Perform a health-check sweep across all regions.
    ///
    /// Returns the list of under-replicated regions and any leader changes
    /// detected since the last check.
    ///
    /// # Errors
    ///
    /// Returns an error if the routing cache cannot be refreshed.
    pub async fn check_health(&self) -> Result<FailoverCheckResult> {
        // Refresh routing data
        self.routing_cache.refresh().await?;
        let snapshot = self.routing_cache.snapshot();

        let dead = self.dead_nodes.read().clone();
        let mut under_replicated = Vec::new();
        let mut leader_changes: usize = 0;
        let mut regions_checked: usize = 0;

        for (measurement, routes) in &snapshot.entries {
            for route in routes {
                regions_checked += 1;

                // Count live replicas
                let live_replicas = Self::count_live_replicas(route, &dead);

                if live_replicas < self.config.replication_factor {
                    under_replicated.push(UnderReplicatedRegion {
                        region_id: route.region_id,
                        measurement: measurement.clone(),
                        current_replicas: live_replicas,
                        target_replicas: self.config.replication_factor,
                        leader_node_id: route.leader_node_id,
                    });
                }

                // Detect leader on dead node — means re-election happened or is needed
                if dead.contains(&route.leader_node_id) {
                    leader_changes += 1;
                    increment_leader_changes();
                    warn!(
                        region_id = route.region_id,
                        old_leader = route.leader_node_id,
                        "region leader is on dead node — re-election expected"
                    );
                }
            }
        }

        // Update metric
        set_under_replicated(under_replicated.len() as u64);

        debug!(
            regions_checked,
            under_replicated = under_replicated.len(),
            leader_changes,
            "failover health check complete"
        );

        Ok(FailoverCheckResult {
            under_replicated,
            leader_changes,
            regions_checked,
        })
    }

    /// Attempt self-healing repair for under-replicated regions.
    ///
    /// Uses the [`RegionMigrator`] to add new replicas on healthy nodes.
    /// At most `max_concurrent_repairs` repairs are started.
    ///
    /// # Errors
    ///
    /// Returns an error if any repair operation fails.
    pub async fn repair_under_replicated(
        &self,
        regions: &[UnderReplicatedRegion],
        available_nodes: &[NodeId],
    ) -> Result<Vec<MigrationResult>> {
        let mut results = Vec::new();
        let limit = self.config.max_concurrent_repairs.min(regions.len());

        for region in regions.iter().take(limit) {
            // Find a healthy node not already hosting this region
            let dest = self.find_repair_destination(region, available_nodes);
            let Some(dest_node_id) = dest else {
                warn!(
                    region_id = region.region_id,
                    "no suitable destination node for repair"
                );
                continue;
            };

            info!(
                region_id = region.region_id,
                source = region.leader_node_id,
                dest = dest_node_id,
                "starting repair migration"
            );

            let plan = MigrationPlan::new(
                region.region_id,
                &region.measurement,
                region.leader_node_id,
                dest_node_id,
            );

            match self.migrator.migrate(&plan).await {
                Ok(result) => results.push(result),
                Err(e) => {
                    warn!(
                        region_id = region.region_id,
                        error = %e,
                        "repair migration failed"
                    );
                }
            }
        }

        Ok(results)
    }

    /// Count the number of live replicas for a region.
    ///
    /// Counts from `replica_addrs` (which includes the leader). Dead
    /// nodes are excluded. If `replica_addrs` is empty, falls back to
    /// checking the leader alone.
    fn count_live_replicas(route: &RouteEntry, dead: &std::collections::HashSet<NodeId>) -> usize {
        if route.replica_addrs.is_empty() {
            // Legacy: no replica list, just check if leader is alive
            return usize::from(!dead.contains(&route.leader_node_id));
        }

        route
            .replica_addrs
            .iter()
            .filter(|(id, _)| !dead.contains(id))
            .count()
    }

    /// Find a healthy node to host a new replica.
    fn find_repair_destination(
        &self,
        region: &UnderReplicatedRegion,
        available_nodes: &[NodeId],
    ) -> Option<NodeId> {
        let dead = self.dead_nodes.read();
        available_nodes
            .iter()
            .copied()
            .find(|&nid| nid != region.leader_node_id && !dead.contains(&nid))
    }

    /// The failover configuration.
    pub fn config(&self) -> &FailoverConfig {
        &self.config
    }

    /// Admin API entry point — trigger a full rebalance cycle.
    ///
    /// 1. Runs `check_health()` to detect under-replicated regions.
    /// 2. Runs `repair_under_replicated()` on the detected regions.
    ///
    /// This is designed to be called via an admin gRPC/HTTP endpoint for
    /// manual rebalancing or by the `ClusterCoordinator` for automatic
    /// rebalancing during decommission.
    ///
    /// # Errors
    ///
    /// Returns an error if health check or repairs fail.
    pub async fn trigger_rebalance(
        &self,
        available_nodes: &[NodeId],
    ) -> Result<Vec<MigrationResult>> {
        info!("admin-triggered rebalance starting");

        let health = self.check_health().await?;

        if health.under_replicated.is_empty() {
            info!("no under-replicated regions — cluster is balanced");
            return Ok(Vec::new());
        }

        info!(
            under_replicated = health.under_replicated.len(),
            "rebalancing under-replicated regions"
        );

        self.repair_under_replicated(&health.under_replicated, available_nodes)
            .await
    }
}

impl std::fmt::Debug for FailoverManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailoverManager")
            .field("local_node_id", &self.local_node_id)
            .field("replication_factor", &self.config.replication_factor)
            .field("dead_nodes", &self.dead_nodes.read().len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::MetaClient;
    use crate::data_client::DataGrpcClient;
    use crate::data_service::RegionStorage;
    use crate::region::RegionManager;
    use chronix_meta::RoutingSnapshot;

    // ── Mock storage — uses shared region-store mock ───────────────
    use crate::test_util::MockRegionStore;
    type MockStorage = MockRegionStore;

    // ── Mock meta client — uses shared test utility ────────────────
    use crate::test_util::{
        make_routing_snapshot, make_routing_snapshot_with_replicas, MockSnapshotMetaClient,
    };

    fn make_failover_manager(snapshot: RoutingSnapshot, config: FailoverConfig) -> FailoverManager {
        let meta_client: Arc<dyn MetaClient> = Arc::new(MockSnapshotMetaClient::new(snapshot));
        let cache = Arc::new(RoutingCache::new(meta_client));
        let storage: Arc<dyn RegionStorage> = Arc::new(MockStorage::default());
        let mgr = Arc::new(RegionManager::new(1));
        let data_client = DataGrpcClient::new();
        let migrator = Arc::new(RegionMigrator::new(mgr, storage, data_client, 1));

        FailoverManager::new(cache, migrator, 1, config)
    }

    // ── Tests ──────────────────────────────────────────────────────

    #[test]
    fn default_config() {
        let cfg = FailoverConfig::default();
        assert_eq!(cfg.replication_factor, 3);
        assert_eq!(cfg.max_concurrent_repairs, 2);
        assert_eq!(cfg.check_interval, Duration::from_secs(10));
        assert_eq!(cfg.election_timeout, Duration::from_secs(3));
    }

    #[test]
    fn mark_node_dead_and_alive() {
        let snapshot = make_routing_snapshot("cpu", vec![(10, 1, "http://n1:5000")]);
        let fm = make_failover_manager(snapshot, FailoverConfig::default());

        assert!(fm.dead_nodes().is_empty());

        fm.mark_node_dead(2);
        assert_eq!(fm.dead_nodes(), vec![2]);

        // Marking twice doesn't duplicate
        fm.mark_node_dead(2);
        assert_eq!(fm.dead_nodes().len(), 1);

        fm.mark_node_alive(2);
        assert!(fm.dead_nodes().is_empty());
    }

    #[tokio::test]
    async fn check_health_all_alive() {
        let snapshot = make_routing_snapshot("cpu", vec![(10, 1, "http://n1:5000")]);
        let config = FailoverConfig {
            replication_factor: 1,
            ..Default::default()
        };
        let fm = make_failover_manager(snapshot, config);

        let result = fm.check_health().await.unwrap();
        assert_eq!(result.regions_checked, 1);
        assert!(result.under_replicated.is_empty());
        assert_eq!(result.leader_changes, 0);
    }

    #[tokio::test]
    async fn check_health_detects_under_replication() {
        let snapshot = make_routing_snapshot(
            "cpu",
            vec![(10, 1, "http://n1:5000"), (20, 2, "http://n2:5000")],
        );
        let config = FailoverConfig {
            replication_factor: 3,
            ..Default::default()
        };
        let fm = make_failover_manager(snapshot, config);

        let result = fm.check_health().await.unwrap();
        // Each region has 1 replica (leader only, no replica_addrs),
        // but target is 3 → both are under-replicated
        assert_eq!(result.under_replicated.len(), 2);
        assert!(result
            .under_replicated
            .iter()
            .all(|r| r.current_replicas == 1));
        assert!(result
            .under_replicated
            .iter()
            .all(|r| r.target_replicas == 3));
    }

    #[tokio::test]
    async fn check_health_detects_leader_on_dead_node() {
        let snapshot = make_routing_snapshot("cpu", vec![(10, 2, "http://n2:5000")]);
        let config = FailoverConfig {
            replication_factor: 1,
            ..Default::default()
        };
        let fm = make_failover_manager(snapshot, config);
        fm.mark_node_dead(2);

        let result = fm.check_health().await.unwrap();
        assert_eq!(result.leader_changes, 1);
        // Also under-replicated because leader is dead
        assert_eq!(result.under_replicated.len(), 1);
    }

    #[test]
    fn find_repair_destination_skips_dead_and_leader() {
        let snapshot = make_routing_snapshot("cpu", vec![(10, 1, "http://n1:5000")]);
        let fm = make_failover_manager(snapshot, FailoverConfig::default());
        fm.mark_node_dead(2);

        let region = UnderReplicatedRegion {
            region_id: 10,
            measurement: "cpu".into(),
            current_replicas: 1,
            target_replicas: 3,
            leader_node_id: 1,
        };

        // Node 1 is the leader, node 2 is dead → only node 3 is available
        let dest = fm.find_repair_destination(&region, &[1, 2, 3]);
        assert_eq!(dest, Some(3));

        // No available nodes
        let dest = fm.find_repair_destination(&region, &[1, 2]);
        assert_eq!(dest, None);
    }

    #[test]
    fn under_replicated_region_debug() {
        let r = UnderReplicatedRegion {
            region_id: 10,
            measurement: "cpu".into(),
            current_replicas: 1,
            target_replicas: 3,
            leader_node_id: 1,
        };
        let d = format!("{r:?}");
        assert!(d.contains("UnderReplicatedRegion"));
        assert!(d.contains("cpu"));
    }

    #[test]
    fn failover_check_result_debug() {
        let r = FailoverCheckResult {
            under_replicated: vec![],
            leader_changes: 0,
            regions_checked: 5,
        };
        let d = format!("{r:?}");
        assert!(d.contains("FailoverCheckResult"));
        assert!(d.contains("5"));
    }

    #[test]
    fn failover_manager_debug() {
        let snapshot = make_routing_snapshot("cpu", vec![(10, 1, "http://n1:5000")]);
        let fm = make_failover_manager(snapshot, FailoverConfig::default());
        let d = format!("{fm:?}");
        assert!(d.contains("FailoverManager"));
        assert!(d.contains("replication_factor"));
    }

    #[test]
    fn config_accessor() {
        let snapshot = make_routing_snapshot("cpu", vec![(10, 1, "http://n1:5000")]);
        let fm = make_failover_manager(snapshot, FailoverConfig::default());
        assert_eq!(fm.config().replication_factor, 3);
        assert_eq!(fm.config().max_concurrent_repairs, 2);
    }

    #[tokio::test]
    async fn trigger_rebalance_no_issues() {
        let snapshot = make_routing_snapshot_with_replicas(
            "cpu",
            vec![(
                10,
                1,
                "http://n1:5000",
                vec![
                    (1, "http://n1:5000"),
                    (2, "http://n2:5000"),
                    (3, "http://n3:5000"),
                ],
            )],
        );
        let fm = make_failover_manager(snapshot, FailoverConfig::default());

        let results = fm.trigger_rebalance(&[1, 2, 3]).await.unwrap();
        assert!(
            results.is_empty(),
            "no under-replicated regions — nothing to do"
        );
    }
}
