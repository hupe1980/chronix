//! Distributed write router.
//!
//! Routes incoming writes to the correct region leader `DataNode`, grouping
//! points by region for efficient batching. Local regions are written
//! directly; remote regions are forwarded via gRPC.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tracing::{debug, info, instrument, warn};

use chronix_core::Point;
use chronix_meta::{NodeId, RegionId, RouteEntry};

use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerState};
use crate::data_client::DataGrpcClient;
use crate::data_service::{core_to_proto_point, RegionStorage};
use crate::dedup::DeduplicationCache;
use crate::error::{ClusterError, Result};
use crate::metrics::{
    increment_circuit_breaker_open, increment_replication_errors, increment_replication_requests,
    record_replication_latency, record_write_latency,
};
use crate::region_raft::RegionRaftManager;
use crate::routing_cache::RoutingCache;

/// Result of a distributed write batch operation.
#[derive(Debug, Clone)]
pub struct WriteBatchResult {
    /// Total points successfully written.
    pub total_written: u64,
    /// Points written to local regions.
    pub local_written: u64,
    /// Points written to remote regions.
    pub remote_written: u64,
    /// Number of distinct regions targeted.
    pub regions_hit: usize,
}

/// Routes writes to the correct region leader for each point's series key.
///
/// The router maintains a [`RoutingCache`] for region → leader mapping,
/// a [`DataGrpcClient`] for forwarding writes to remote leaders, and a
/// reference to the local [`RegionStorage`] for direct writes.
///
/// When a [`RegionRaftManager`] is configured (via [`with_raft_manager`](Self::with_raft_manager)),
/// local writes go through the Raft quorum so that acknowledgements are
/// only returned after a majority of replicas have committed the entry.
///
/// When a remote write fails with a retriable error (leader moved,
/// node unavailable), the router refreshes the routing table and retries
/// transparently — clients see writes resume after leader re-election.
///
/// # Circuit breaker
///
/// A per-node circuit breaker tracks consecutive failures for each
/// remote (or local) target node.  After
/// [`circuit_breaker_threshold`](Self) consecutive retriable errors
/// the circuit opens and subsequent writes to that region on that node
/// fail-fast without attempting the network call.  After
/// [`circuit_breaker_cooldown`](Self) the circuit transitions to
/// half-open and allows a single probe write; a success closes the
/// circuit, a failure re-opens it.
pub struct WriteRouter {
    /// Routing information from the `MetaNode`.
    routing_cache: Arc<RoutingCache>,
    /// Local storage backend (only used for regions hosted on this node).
    local_storage: Arc<dyn RegionStorage>,
    /// gRPC client for remote `DataNode` writes.
    data_client: DataGrpcClient,
    /// This node's ID — writes to regions whose leader is this node go local.
    local_node_id: NodeId,
    /// Maximum retries on stale-routing errors (region not found on leader).
    max_retries: u32,
    /// Optional per-region Raft manager for quorum-replicated writes.
    raft_manager: Option<Arc<RegionRaftManager>>,
    /// When true, writes fail with an error if no Raft manager is configured
    /// instead of falling back to unreplicated direct storage writes.
    /// Set this to `true` in cluster deployments where replication is mandatory.
    require_raft: bool,
    /// Timeout for individual region write operations.
    /// Applies to both local Raft proposals and remote gRPC writes.
    replication_timeout: Duration,
    /// Per-region circuit breakers keyed by `(NodeId, RegionId)`.
    /// This prevents a single slow region from cascading failures to other
    /// regions hosted on the same node.
    breakers: DashMap<(NodeId, RegionId), CircuitBreaker>,
    /// Consecutive failure count before opening the circuit.
    circuit_breaker_threshold: u32,
    /// Cooldown before a half-open probe after the circuit opens.
    circuit_breaker_cooldown: Duration,
    /// Dedup cache for write idempotency across retries.
    dedup_cache: Arc<DeduplicationCache>,
}

impl WriteRouter {
    /// Create a new `WriteRouter`.
    pub fn new(
        routing_cache: Arc<RoutingCache>,
        local_storage: Arc<dyn RegionStorage>,
        data_client: DataGrpcClient,
        local_node_id: NodeId,
    ) -> Self {
        Self {
            routing_cache,
            local_storage,
            data_client,
            local_node_id,
            max_retries: 2,
            raft_manager: None,
            require_raft: true,
            replication_timeout: Duration::from_secs(60),
            breakers: DashMap::new(),
            circuit_breaker_threshold: 5,
            circuit_breaker_cooldown: Duration::from_secs(30),
            dedup_cache: Arc::new(DeduplicationCache::new()),
        }
    }

    /// Override the maximum number of stale-routing retries.
    #[must_use]
    pub fn with_max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    /// Attach a [`RegionRaftManager`] for quorum-replicated writes.
    ///
    /// When set, local writes are proposed through the region's Raft
    /// group instead of writing directly to [`RegionStorage`]. The
    /// acknowledgement is returned only after a majority quorum commit.
    #[must_use]
    pub fn with_raft_manager(mut self, manager: Arc<RegionRaftManager>) -> Self {
        self.raft_manager = Some(manager);
        self
    }

    /// Require quorum-replicated writes via Raft.
    ///
    /// When enabled, writes fail with [`ClusterError::Internal`] if no
    /// [`RegionRaftManager`] is configured, instead of silently falling
    /// back to unreplicated local storage writes.
    ///
    /// Enable this in cluster deployments where the replication guarantee
    /// must not be violated.
    #[must_use]
    pub fn with_require_raft(mut self, require: bool) -> Self {
        self.require_raft = require;
        self
    }

    /// Override the per-region write timeout (default: 60 s).
    ///
    /// This timeout applies to each individual region write attempt
    /// (both local Raft proposals and remote gRPC forwards). If a
    /// write exceeds this duration, it is treated as a
    /// [`ClusterError::Timeout`] and may be retried if retries remain.
    #[must_use]
    pub fn with_replication_timeout(mut self, timeout: Duration) -> Self {
        self.replication_timeout = timeout;
        self
    }

    /// Override the circuit-breaker failure threshold (default: 5).
    #[must_use]
    pub fn with_circuit_breaker_threshold(mut self, threshold: u32) -> Self {
        self.circuit_breaker_threshold = threshold;
        self
    }

    /// Override the circuit-breaker cooldown duration (default: 30 s).
    #[must_use]
    pub fn with_circuit_breaker_cooldown(mut self, cooldown: Duration) -> Self {
        self.circuit_breaker_cooldown = cooldown;
        self
    }

    /// Access the dedup cache for receiver-side duplicate detection.
    #[must_use]
    pub fn dedup_cache(&self) -> &Arc<DeduplicationCache> {
        &self.dedup_cache
    }

    /// Route and write a batch of points to the correct region leaders.
    ///
    /// # Errors
    ///
    /// Returns an error if no routes exist for a measurement or if any
    /// storage / RPC operation fails.
    #[instrument(name = "write_batch", skip_all, fields(batch_size = points.len()))]
    pub async fn write_batch(&self, points: &[Point]) -> Result<WriteBatchResult> {
        let start = Instant::now();
        let result = self.write_batch_inner(points).await;
        record_write_latency(start.elapsed());
        result
    }

    async fn write_batch_inner(&self, points: &[Point]) -> Result<WriteBatchResult> {
        if points.is_empty() {
            return Ok(WriteBatchResult {
                total_written: 0,
                local_written: 0,
                remote_written: 0,
                regions_hit: 0,
            });
        }

        // Group points by measurement first
        let mut by_measurement: BTreeMap<&str, Vec<&Point>> = BTreeMap::new();
        for point in points {
            by_measurement
                .entry(point.series_key().measurement())
                .or_default()
                .push(point);
        }

        let mut total_written: u64 = 0;
        let mut local_written: u64 = 0;
        let mut remote_written: u64 = 0;
        let mut regions_hit: usize = 0;

        for (measurement, measurement_points) in &by_measurement {
            let routes = self.get_routes_with_retry(measurement).await?;

            if routes.is_empty() {
                return Err(ClusterError::Validation(format!(
                    "no routes for measurement '{measurement}'"
                )));
            }

            // Use range-based routing via RoutingTable::route_series()
            // instead of modulo hashing. When key_range is populated on routes,
            // this uses stable range partitioning that survives region splits
            // without rehashing. Falls back to modulo for legacy routes.
            let mut by_region: BTreeMap<RegionId, Vec<&Point>> = BTreeMap::new();
            let route_by_region: std::collections::HashMap<RegionId, &RouteEntry> =
                routes.iter().map(|r| (r.region_id, r)).collect();

            let all_have_ranges = routes.iter().all(|r| r.key_range.is_some());

            for point in measurement_points {
                let hash = point.series_key().hash_fnv();

                let target_region = if all_have_ranges {
                    // Range-based routing: find the region whose key_range contains this hash
                    let idx = routes
                        .partition_point(|r| {
                            r.key_range.as_ref().map_or(true, |kr| kr.start <= hash)
                        })
                        .saturating_sub(1);
                    &routes[idx]
                } else {
                    // Legacy modulo fallback for regions without key_range
                    #[allow(clippy::cast_possible_truncation)]
                    let region_idx = (hash as usize) % routes.len();
                    &routes[region_idx]
                };

                by_region
                    .entry(target_region.region_id)
                    .or_default()
                    .push(point);
            }

            for (region_id, region_points) in by_region {
                let route = route_by_region
                    .get(&region_id)
                    .ok_or(ClusterError::RegionNotFound(region_id))?;

                // Reject writes to non-Active regions (ReadOnly,
                // Migrating, Splitting, Replicating).
                if route.region_state != chronix_meta::RegionState::Active {
                    return Err(ClusterError::RegionNotWritable(region_id));
                }

                regions_hit += 1;

                let written = self
                    .write_to_region(route, region_id, &region_points)
                    .await?;

                if route.leader_node_id == self.local_node_id {
                    local_written += written;
                } else {
                    remote_written += written;
                }
                total_written += written;
            }
        }

        debug!(
            total_written,
            local_written, remote_written, regions_hit, "write batch complete"
        );

        Ok(WriteBatchResult {
            total_written,
            local_written,
            remote_written,
            regions_hit,
        })
    }

    /// Write points to a region, retrying on retriable errors.
    ///
    /// On failure with `NotLeader`, `Timeout`, or gRPC `Unavailable`, the
    /// router refreshes the routing cache and retries up to `max_retries`
    /// times. This handles transparent write resumption after leader
    /// re-election.
    ///
    /// Each write generates a unique request ID. On retry, the dedup
    /// cache detects duplicate request IDs and returns the cached response
    /// instead of re-applying the write.
    async fn write_to_region(
        &self,
        initial_route: &RouteEntry,
        region_id: RegionId,
        points: &[&Point],
    ) -> Result<u64> {
        // Generate a unique request ID for dedup across retries.
        let request_id = format!(
            "wr-{}-{region_id}-{}",
            self.local_node_id,
            uuid::Uuid::new_v4()
        );
        // Track replication requests per region.
        increment_replication_requests(region_id);
        let region_start = Instant::now();

        let mut last_err: Option<ClusterError> = None;
        let mut current_leader = initial_route.leader_node_id;

        // Pre-build both representations once outside the retry loop.
        // On retry the leader may switch between local/remote, so we
        // keep both available. Cloning on each attempt is cheap compared
        // to re-serialising from scratch.
        let owned_points: Vec<Point> = points.iter().map(|p| (*p).clone()).collect();
        let proto_points: Vec<_> = points.iter().map(|p| core_to_proto_point(p)).collect();

        for attempt in 0..=self.max_retries {
            // Check dedup cache on retry to avoid double-writes.
            if attempt > 0 {
                if let Some(written) = self.dedup_cache.check(&request_id) {
                    debug!(
                        region_id,
                        request_id = %request_id,
                        "dedup cache hit — returning cached result"
                    );
                    return Ok(written);
                }
            }

            if attempt > 0 {
                // Refresh routing table to discover new leader
                info!(region_id, attempt, "refreshing routes after write failure");
                self.routing_cache.refresh().await?;

                // Look up the fresh route for this region
                let measurement = &initial_route.measurement;
                let fresh_routes = self
                    .routing_cache
                    .routes_for_measurement(measurement)
                    .await?;
                if let Some(fresh) = fresh_routes.iter().find(|r| r.region_id == region_id) {
                    current_leader = fresh.leader_node_id;
                }
            }

            // Circuit breaker — fail-fast per (node, region).
            {
                let mut breaker = self
                    .breakers
                    .entry((current_leader, region_id))
                    .or_insert_with(|| {
                        CircuitBreaker::new(
                            self.circuit_breaker_threshold,
                            self.circuit_breaker_cooldown,
                        )
                    });
                if !breaker.should_allow() {
                    warn!(
                        region_id,
                        node_id = current_leader,
                        "circuit breaker open for region on node, skipping attempt"
                    );
                    last_err = Some(ClusterError::Transport(format!(
                        "circuit breaker open for region {region_id} on node {current_leader}"
                    )));
                    continue;
                }
            }

            // Wrap the write in a configurable timeout so that a
            // stalled Raft proposal or hung gRPC call doesn't block
            // the write batch forever.
            let write_fut = async {
                if current_leader == self.local_node_id {
                    // When a Raft manager is available, propose through quorum
                    // so the write is acknowledged only after majority commit.
                    if let Some(ref mgr) = self.raft_manager {
                        mgr.propose_write_with_id(
                            region_id,
                            owned_points.clone(),
                            Some(request_id.clone()),
                        )
                        .await
                    } else if self.require_raft {
                        return Err(ClusterError::Internal(
                            "Raft manager required but not configured — refusing unreplicated write".into(),
                        ));
                    } else {
                        warn!(
                            region_id,
                            "no Raft manager configured — writing directly to local storage without replication"
                        );
                        self.local_storage
                            .write_points(region_id, owned_points.clone())
                            .await
                    }
                } else {
                    match self
                        .data_client
                        .write_region(current_leader, region_id, proto_points.clone())
                        .await
                    {
                        Ok(resp) => Ok(resp.written),
                        Err(e) => Err(e),
                    }
                }
            };
            let result = match tokio::time::timeout(self.replication_timeout, write_fut).await {
                Ok(inner) => inner,
                Err(_elapsed) => {
                    warn!(
                        region_id,
                        timeout_secs = self.replication_timeout.as_secs(),
                        "region write timed out"
                    );
                    Err(ClusterError::Timeout)
                }
            };

            match result {
                Ok(written) => {
                    // Record in dedup cache before returning.
                    self.dedup_cache.record(request_id.clone(), written);

                    // Record circuit breaker success (per region).
                    {
                        let mut breaker = self
                            .breakers
                            .entry((current_leader, region_id))
                            .or_insert_with(|| {
                                CircuitBreaker::new(
                                    self.circuit_breaker_threshold,
                                    self.circuit_breaker_cooldown,
                                )
                            });
                        let prev_state = breaker.state();
                        breaker.record_success();
                        if prev_state != CircuitBreakerState::Closed {
                            info!(
                                region_id,
                                node_id = current_leader,
                                "circuit breaker closed for region after successful write"
                            );
                        }
                    }
                    // Record successful replication latency.
                    record_replication_latency(region_id, region_start.elapsed());
                    return Ok(written);
                }
                Err(e) => {
                    // Record circuit breaker failure for retriable errors.
                    if Self::is_retriable(&e) {
                        let circuit_opened = {
                            let mut breaker = self
                                .breakers
                                .entry((current_leader, region_id))
                                .or_insert_with(|| {
                                    CircuitBreaker::new(
                                        self.circuit_breaker_threshold,
                                        self.circuit_breaker_cooldown,
                                    )
                                });
                            breaker.record_failure();
                            breaker.state() == CircuitBreakerState::Open
                        };
                        if circuit_opened {
                            increment_circuit_breaker_open(current_leader);
                            info!(
                                region_id,
                                node_id = current_leader,
                                "circuit breaker opened for region on node"
                            );
                        }
                    }
                    if Self::is_retriable(&e) && attempt < self.max_retries {
                        // Track replication errors (retriable).
                        increment_replication_errors(region_id, "retriable");
                        warn!(
                            region_id,
                            attempt,
                            error = %e,
                            "retriable write error, will retry"
                        );
                        // Exponential backoff with jitter to avoid
                        // hammering a recovering leader.
                        let base_ms = 50u64 << attempt.min(6);
                        let jitter = base_ms / 4;
                        let delay = Duration::from_millis(base_ms + (base_ms % (jitter.max(1))));
                        tokio::time::sleep(delay).await;
                        last_err = Some(e);
                        continue;
                    }
                    // Track replication errors (permanent).
                    increment_replication_errors(region_id, "permanent");
                    record_replication_latency(region_id, region_start.elapsed());
                    return Err(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| {
            ClusterError::ReplicationFailed("write retries exhausted".to_string())
        }))
    }

    /// Check whether a write error is worth retrying (leader change,
    /// transient unavailability, etc.).
    fn is_retriable(err: &ClusterError) -> bool {
        matches!(
            err,
            ClusterError::NotLeader
                | ClusterError::Timeout
                | ClusterError::NodeNotFound(_)
                | ClusterError::Transport(_)
        )
    }

    /// Get routes for a measurement, refreshing on cache miss.
    async fn get_routes_with_retry(&self, measurement: &str) -> Result<Vec<RouteEntry>> {
        for attempt in 0..=self.max_retries {
            let routes = self
                .routing_cache
                .routes_for_measurement(measurement)
                .await?;
            if !routes.is_empty() {
                return Ok(routes);
            }

            if attempt < self.max_retries {
                debug!(measurement, attempt, "no routes found, refreshing cache");
                self.routing_cache.refresh().await?;
            }
        }

        warn!(measurement, "no routes after retries");
        Ok(Vec::new())
    }
}

impl std::fmt::Debug for WriteRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteRouter")
            .field("local_node_id", &self.local_node_id)
            .field("max_retries", &self.max_retries)
            .field("replication_timeout", &self.replication_timeout)
            .field("circuit_breaker_threshold", &self.circuit_breaker_threshold)
            .field("circuit_breaker_cooldown", &self.circuit_breaker_cooldown)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::MetaClient;
    use crate::data_service::RegionQuery;
    use async_trait::async_trait;
    use chronix_core::{FieldValue, SeriesKey};
    use chronix_meta::RoutingSnapshot;
    use parking_lot::Mutex;

    // ── Mock storage — uses shared write-log mock ──────────────────
    use crate::test_util::MockWriteStorage;
    type MockStorage = MockWriteStorage;

    // ── Mock meta client — uses shared test utility ──────────────
    use crate::test_util::MockSnapshotMetaClient;

    // ── Helpers ────────────────────────────────────────────────────

    fn make_point(measurement: &str, tag_val: &str, ts: i64) -> Point {
        let tags: BTreeMap<String, String> = [("host".to_string(), tag_val.to_string())]
            .into_iter()
            .collect();
        let series_key = SeriesKey::new(measurement, tags).unwrap();
        let mut fields = BTreeMap::new();
        fields.insert("value".to_string(), FieldValue::F64(1.0));
        Point::new(series_key, fields, ts).unwrap()
    }

    fn make_routing_snapshot(
        measurement: &str,
        routes: Vec<(RegionId, NodeId, &str)>,
    ) -> RoutingSnapshot {
        let entries: Vec<RouteEntry> = routes
            .into_iter()
            .map(|(rid, nid, addr)| RouteEntry {
                region_id: rid,
                measurement: measurement.to_string(),
                leader_node_id: nid,
                leader_addr: addr.to_string(),
                replica_addrs: vec![],
                key_range: None,
                region_state: chronix_meta::RegionState::Active,
            })
            .collect();

        RoutingSnapshot {
            version: 1,
            entries: [(measurement.to_string(), entries)].into_iter().collect(),
        }
    }

    fn make_router(
        snapshot: RoutingSnapshot,
        local_node_id: NodeId,
    ) -> (WriteRouter, Arc<MockStorage>) {
        let meta_client: Arc<dyn MetaClient> = Arc::new(MockSnapshotMetaClient::new(snapshot));
        let cache = Arc::new(RoutingCache::new(meta_client));

        let storage = Arc::new(MockStorage::default());
        let data_client = DataGrpcClient::new();

        let router = WriteRouter::new(cache, storage.clone(), data_client, local_node_id)
            .with_require_raft(false);

        (router, storage)
    }

    // ── Tests ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn empty_batch_succeeds() {
        let snapshot = make_routing_snapshot("cpu", vec![(1, 1, "http://n1:5000")]);
        let (router, _) = make_router(snapshot, 1);

        let result = router.write_batch(&[]).await.unwrap();
        assert_eq!(result.total_written, 0);
        assert_eq!(result.regions_hit, 0);
    }

    #[tokio::test]
    async fn local_write_routes_correctly() {
        let snapshot = make_routing_snapshot("cpu", vec![(10, 1, "http://n1:5000")]);
        let (router, storage) = make_router(snapshot, 1);

        let points = vec![make_point("cpu", "srv1", 100)];
        let result = router.write_batch(&points).await.unwrap();

        assert_eq!(result.total_written, 1);
        assert_eq!(result.local_written, 1);
        assert_eq!(result.remote_written, 0);
        assert_eq!(result.regions_hit, 1);

        let writes = storage.writes.lock();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, 10);
    }

    #[tokio::test]
    async fn multiple_points_same_region() {
        let snapshot = make_routing_snapshot("cpu", vec![(10, 1, "http://n1:5000")]);
        let (router, storage) = make_router(snapshot, 1);

        let points = vec![
            make_point("cpu", "srv1", 100),
            make_point("cpu", "srv2", 200),
            make_point("cpu", "srv3", 300),
        ];
        let result = router.write_batch(&points).await.unwrap();

        assert_eq!(result.total_written, 3);
        assert_eq!(result.local_written, 3);

        let writes = storage.writes.lock();
        // All 3 points hash to the same single region
        let total: usize = writes.iter().map(|(_, pts)| pts.len()).sum();
        assert_eq!(total, 3);
    }

    #[tokio::test]
    async fn no_routes_returns_error() {
        let snapshot = RoutingSnapshot {
            version: 1,
            entries: BTreeMap::new(),
        };
        let (router, _) = make_router(snapshot, 1);

        let points = vec![make_point("cpu", "srv1", 100)];
        let result = router.write_batch(&points).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn points_distributed_across_regions() {
        // Two regions for "cpu", both local
        let snapshot = make_routing_snapshot(
            "cpu",
            vec![(10, 1, "http://n1:5000"), (20, 1, "http://n1:5000")],
        );
        let (router, storage) = make_router(snapshot, 1);

        // Generate enough points to hit both regions (hash distribution)
        let mut points = Vec::new();
        for i in 0..100 {
            points.push(make_point("cpu", &format!("host{i}"), i));
        }

        let result = router.write_batch(&points).await.unwrap();
        assert_eq!(result.total_written, 100);
        assert_eq!(result.local_written, 100);

        let writes = storage.writes.lock();
        // Both regions should have received some points
        let region_ids: Vec<_> = writes.iter().map(|(rid, _)| *rid).collect();
        assert!(region_ids.contains(&10) || region_ids.contains(&20));
    }

    #[test]
    fn debug_format() {
        let snapshot = make_routing_snapshot("cpu", vec![(1, 1, "http://n1:5000")]);
        let (router, _) = make_router(snapshot, 1);
        let debug = format!("{router:?}");
        assert!(debug.contains("WriteRouter"));
        assert!(debug.contains("local_node_id"));
    }

    #[test]
    fn write_batch_result_fields() {
        let result = WriteBatchResult {
            total_written: 10,
            local_written: 7,
            remote_written: 3,
            regions_hit: 2,
        };
        assert_eq!(result.total_written, 10);
        assert_eq!(result.local_written, 7);
        assert_eq!(result.remote_written, 3);
        assert_eq!(result.regions_hit, 2);
    }

    // ── Retriable error tests ──────────────────────────────────────

    #[test]
    fn is_retriable_not_leader() {
        assert!(WriteRouter::is_retriable(&ClusterError::NotLeader));
    }

    #[test]
    fn is_retriable_timeout() {
        assert!(WriteRouter::is_retriable(&ClusterError::Timeout));
    }

    #[test]
    fn is_retriable_node_not_found() {
        assert!(WriteRouter::is_retriable(&ClusterError::NodeNotFound(42)));
    }

    #[test]
    fn is_retriable_unavailable_transport() {
        assert!(WriteRouter::is_retriable(&ClusterError::Transport(
            "node 5 unavailable: connection refused".to_string()
        )));
    }

    #[test]
    fn is_retriable_connect_failure() {
        assert!(WriteRouter::is_retriable(&ClusterError::Transport(
            "connect to node 3: connection reset".to_string()
        )));
    }

    #[test]
    fn is_not_retriable_replication_failed() {
        assert!(!WriteRouter::is_retriable(
            &ClusterError::ReplicationFailed("data corruption".to_string())
        ));
    }

    #[test]
    fn is_not_retriable_region_not_found() {
        assert!(!WriteRouter::is_retriable(&ClusterError::RegionNotFound(7)));
    }

    #[test]
    fn is_not_retriable_generic_internal() {
        assert!(!WriteRouter::is_retriable(&ClusterError::Internal(
            "unknown error".to_string()
        )));
    }

    // ── write_to_region retry with mock ─────────────────────────────

    /// Storage that fails the first N writes, then succeeds.
    #[derive(Debug)]
    struct FailThenSucceedStorage {
        failures_remaining: Mutex<u32>,
        error: ClusterError,
        writes: Mutex<Vec<(RegionId, Vec<Point>)>>,
    }

    #[async_trait]
    impl RegionStorage for FailThenSucceedStorage {
        async fn write_points(&self, region_id: RegionId, points: Vec<Point>) -> Result<u64> {
            let mut remaining = self.failures_remaining.lock();
            if *remaining > 0 {
                *remaining -= 1;
                return Err(match &self.error {
                    ClusterError::NotLeader => ClusterError::NotLeader,
                    ClusterError::Timeout => ClusterError::Timeout,
                    ClusterError::Transport(m) => ClusterError::Transport(m.clone()),
                    e => ClusterError::Internal(format!("{e}")),
                });
            }
            let count = points.len() as u64;
            self.writes.lock().push((region_id, points));
            Ok(count)
        }

        async fn query_region(
            &self,
            _region_id: RegionId,
            _query: RegionQuery,
        ) -> Result<Vec<Point>> {
            Ok(vec![])
        }

        async fn replicate_wal(
            &self,
            _region_id: RegionId,
            _entries: Vec<(u64, Vec<u8>)>,
        ) -> Result<u64> {
            Ok(0)
        }

        async fn snapshot_data(&self, _region_id: RegionId) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }

        async fn restore_snapshot(&self, _region_id: RegionId, _data: &[u8]) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn write_retries_on_not_leader() {
        let storage = Arc::new(FailThenSucceedStorage {
            failures_remaining: Mutex::new(1),
            error: ClusterError::NotLeader,
            writes: Mutex::new(Vec::new()),
        });
        let snapshot = make_routing_snapshot("cpu", vec![(10, 1, "http://n1:5000")]);
        let meta_client: Arc<dyn MetaClient> = Arc::new(MockSnapshotMetaClient::new(snapshot));
        let cache = Arc::new(RoutingCache::new(meta_client));
        let data_client = DataGrpcClient::new();
        let router = WriteRouter::new(cache, storage.clone(), data_client, 1)
            .with_max_retries(2)
            .with_require_raft(false);

        let points = vec![make_point("cpu", "srv1", 100)];
        let result = router.write_batch(&points).await.unwrap();
        assert_eq!(result.total_written, 1);
        assert_eq!(result.local_written, 1);

        let writes = storage.writes.lock();
        assert_eq!(writes.len(), 1);
    }

    #[tokio::test]
    async fn write_retries_on_timeout() {
        let storage = Arc::new(FailThenSucceedStorage {
            failures_remaining: Mutex::new(1),
            error: ClusterError::Timeout,
            writes: Mutex::new(Vec::new()),
        });
        let snapshot = make_routing_snapshot("cpu", vec![(10, 1, "http://n1:5000")]);
        let meta_client: Arc<dyn MetaClient> = Arc::new(MockSnapshotMetaClient::new(snapshot));
        let cache = Arc::new(RoutingCache::new(meta_client));
        let data_client = DataGrpcClient::new();
        let router = WriteRouter::new(cache, storage.clone(), data_client, 1)
            .with_max_retries(2)
            .with_require_raft(false);

        let points = vec![make_point("cpu", "srv1", 100)];
        let result = router.write_batch(&points).await.unwrap();
        assert_eq!(result.total_written, 1);
    }

    #[tokio::test]
    async fn write_fails_after_retries_exhausted() {
        let storage = Arc::new(FailThenSucceedStorage {
            failures_remaining: Mutex::new(10), // always fails
            error: ClusterError::NotLeader,
            writes: Mutex::new(Vec::new()),
        });
        let snapshot = make_routing_snapshot("cpu", vec![(10, 1, "http://n1:5000")]);
        let meta_client: Arc<dyn MetaClient> = Arc::new(MockSnapshotMetaClient::new(snapshot));
        let cache = Arc::new(RoutingCache::new(meta_client));
        let data_client = DataGrpcClient::new();
        let router = WriteRouter::new(cache, storage, data_client, 1)
            .with_max_retries(2)
            .with_require_raft(false);

        let points = vec![make_point("cpu", "srv1", 100)];
        let result = router.write_batch(&points).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn non_retriable_error_not_retried() {
        let storage = Arc::new(FailThenSucceedStorage {
            failures_remaining: Mutex::new(1),
            error: ClusterError::Internal("data corruption".to_string()),
            writes: Mutex::new(Vec::new()),
        });
        let snapshot = make_routing_snapshot("cpu", vec![(10, 1, "http://n1:5000")]);
        let meta_client: Arc<dyn MetaClient> = Arc::new(MockSnapshotMetaClient::new(snapshot));
        let cache = Arc::new(RoutingCache::new(meta_client));
        let data_client = DataGrpcClient::new();
        let router = WriteRouter::new(cache, storage, data_client, 1)
            .with_max_retries(2)
            .with_require_raft(false);

        let points = vec![make_point("cpu", "srv1", 100)];
        let result = router.write_batch(&points).await;
        // Not retriable: should fail immediately without retry
        assert!(result.is_err());
    }

    // ── Raft quorum write tests ────────────────────────────────────

    #[tokio::test]
    async fn raft_quorum_write_via_router() {
        use crate::region_raft::{RegionRaftManager, RegionRaftRouter};
        use openraft::BasicNode;
        use std::time::Duration;

        let region_id = 10;
        let node_id = 1;

        // Set up Raft for region 10 as single-node cluster
        let raft_router = RegionRaftRouter::new();
        let raft_manager = Arc::new(RegionRaftManager::new_in_memory(raft_router, node_id));
        let raft_storage = Arc::new(MockStorage::default());
        let config = Arc::new(openraft::Config {
            heartbeat_interval: 20,
            election_timeout_min: 50,
            election_timeout_max: 100,
            ..Default::default()
        });

        let raft = raft_manager
            .create_raft_group(region_id, node_id, raft_storage.clone(), config)
            .await
            .unwrap();

        let mut members = BTreeMap::new();
        members.insert(node_id, BasicNode::new("127.0.0.1:10001"));
        raft.initialize(members).await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Build WriteRouter with Raft manager
        let snapshot = make_routing_snapshot("cpu", vec![(region_id, node_id, "http://n1:5000")]);
        let meta_client: Arc<dyn MetaClient> = Arc::new(MockSnapshotMetaClient::new(snapshot));
        let cache = Arc::new(RoutingCache::new(meta_client));
        // Use a separate "base" storage that should NOT receive writes
        // (the Raft state machine has its own storage)
        let base_storage = Arc::new(MockStorage::default());
        let data_client = DataGrpcClient::new();
        let router = WriteRouter::new(cache, base_storage.clone(), data_client, node_id)
            .with_raft_manager(raft_manager);

        let points = vec![make_point("cpu", "srv1", 100)];
        let result = router.write_batch(&points).await.unwrap();
        assert_eq!(result.total_written, 1);
        assert_eq!(result.local_written, 1);

        // The base storage should NOT have writes — Raft SM has its own
        assert_eq!(base_storage.writes.lock().len(), 0);

        // The Raft SM storage should have the write
        assert_eq!(raft_storage.writes.lock().len(), 1);
        assert_eq!(raft_storage.writes.lock()[0].0, region_id);
    }

    #[tokio::test]
    async fn without_raft_manager_writes_direct() {
        // When no Raft manager is configured, writes go directly to storage
        let snapshot = make_routing_snapshot("cpu", vec![(10, 1, "http://n1:5000")]);
        let (router, storage) = make_router(snapshot, 1);

        let points = vec![make_point("cpu", "srv1", 100)];
        let result = router.write_batch(&points).await.unwrap();
        assert_eq!(result.total_written, 1);

        // Direct write to storage
        assert_eq!(storage.writes.lock().len(), 1);
    }
}
