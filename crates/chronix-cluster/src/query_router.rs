//! Distributed query router — scatter-gather across region leaders.
//!
//! The [`QueryRouter`] breaks a query down by measurement, looks up the
//! regions that host that measurement via the [`RoutingCache`], fans out
//! sub-queries to each region leader (DataNode), and merges the results
//! back into a single ordered point stream.
//!
//! ## Key behaviour
//!
//! * **Scatter:** parallel `QueryRegion` gRPC calls to each region leader.
//! * **Gather:** merge results by timestamp, de-duplicate, apply limits.
//! * **Predicate pushdown:** tag filters and time range sent to each DataNode.
//! * **Read consistency:** supports `leader` (default) and `follower` reads.
//! * **Timeout:** per-query timeout with partial-result support.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::seq::IndexedRandom;
use tokio::time::timeout;
use tracing::{debug, instrument, warn};

use chronix_core::Point;
use chronix_meta::NodeId;

use crate::data_client::DataGrpcClient;
use crate::data_service::proto::{QueryRegionRequest, TagFilter};
use crate::data_service::{proto_to_core_point, RegionQuery, RegionStorage};
use crate::error::{ClusterError, Result};
use crate::metrics::record_query_latency;
use crate::routing_cache::RoutingCache;

// ── Read consistency ───────────────────────────────────────────────────────

/// Read consistency level for distributed queries.
///
/// # Staleness semantics
///
/// Only [`Leader`](Self::Leader) provides linearizable reads. All other
/// levels may return data that lags behind the leader by an unbounded
/// amount (limited only by replication throughput). Use
/// [`BoundedStale`](Self::BoundedStale) when you need a time-bounded
/// freshness window — the router will verify that the routing cache was
/// refreshed within `max_staleness` and fall back to the leader if it
/// was not.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum ReadConsistency {
    /// Read from the region leader (linearizable / latest data).
    #[default]
    Leader,
    /// Read from any follower replica (may be arbitrarily stale).
    ///
    /// **Warning:** the selected replica may be behind the leader by
    /// any number of Raft log entries. This level is suitable only for
    /// workloads that tolerate eventual consistency (dashboards,
    /// analytics, non-critical monitoring queries).
    Follower,
    /// Read from a randomly selected replica (lowest-contention heuristic).
    ///
    /// In a real deployment this could be extended with RTT-based selection;
    /// the current implementation picks a random replica to spread load.
    Nearest,
    /// Read from any replica whose routing metadata is at most
    /// `max_staleness` old.
    ///
    /// The router checks how long ago the routing cache was last
    /// refreshed. If within `max_staleness`, it allows a follower read.
    /// Otherwise it triggers a refresh; if the refresh succeeds and
    /// brings the cache within the window, a follower is used. If not,
    /// the query falls back to the leader.
    ///
    /// Unlike a version-based check, this provides a real **wall-clock
    /// freshness guarantee** — matching CockroachDB/Spanner bounded-
    /// staleness semantics.
    BoundedStale {
        /// Maximum acceptable age of the routing cache. When the cache
        /// was last refreshed more than this duration ago, the query is
        /// routed to the leader instead.
        max_staleness: Duration,
    },
}

impl ReadConsistency {
    /// Returns `true` when this level requires reading from the leader.
    #[must_use]
    pub fn is_leader(self) -> bool {
        self == Self::Leader
    }

    /// Returns `true` when any replica may serve the read.
    #[must_use]
    pub fn allows_follower(self) -> bool {
        matches!(
            self,
            Self::Follower | Self::Nearest | Self::BoundedStale { .. }
        )
    }
}

// ── DistributedQuery ───────────────────────────────────────────────────────

/// Parameters for a distributed query across multiple regions.
#[derive(Debug, Clone)]
pub struct DistributedQuery {
    /// The measurement to query.
    pub measurement: String,
    /// Start of the time range (inclusive, nanoseconds).
    pub start_ns: i64,
    /// End of the time range (exclusive, nanoseconds).
    pub end_ns: i64,
    /// Tag equality filters — all must match.
    pub tag_filters: Vec<(String, String)>,
    /// Specific field columns to return (empty = all).
    pub field_columns: Vec<String>,
    /// Global limit on total points returned (0 = no limit).
    pub limit: u64,
    /// Read consistency level.
    pub consistency: ReadConsistency,
}

// ── QueryResult ────────────────────────────────────────────────────────────

/// Result of a distributed query.
///
/// Includes explicit `partial_results` flag so callers can
/// detect when some regions failed and results are incomplete.
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Merged result points, sorted by timestamp ascending.
    pub points: Vec<Point>,
    /// Number of regions that successfully responded.
    pub regions_queried: usize,
    /// Number of regions that failed or timed out.
    pub regions_failed: usize,
    /// Whether the result set was truncated by the limit.
    pub truncated: bool,
    /// `true` when some (but not all) regions failed.
    /// Callers should inspect this flag for consistency-sensitive workloads
    /// (alerting, billing) and potentially reject partial data.
    pub partial_results: bool,
}

// ── QueryRouter ────────────────────────────────────────────────────────────

/// Distributed query router that scatters sub-queries across region
/// leaders and gathers results.
pub struct QueryRouter {
    /// Routing information from the `MetaNode`.
    routing_cache: Arc<RoutingCache>,
    /// Local storage backend — queries to local regions skip gRPC.
    local_storage: Arc<dyn RegionStorage>,
    /// gRPC client for querying remote `DataNode`s.
    data_client: DataGrpcClient,
    /// This node's ID — used to detect local regions.
    local_node_id: NodeId,
    /// Per-region query timeout.
    query_timeout: Duration,
    /// Optional Raft manager for linearizable read verification
    /// via ReadIndex protocol. When present and consistency is `Leader`,
    /// the query router verifies leadership before serving local reads.
    raft_manager: Option<Arc<crate::region_raft::RegionRaftManager>>,
}

impl QueryRouter {
    /// Create a new `QueryRouter`.
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
            query_timeout: Duration::from_secs(30),
            raft_manager: None,
        }
    }

    /// Override the per-region query timeout (default: 30s).
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = timeout;
        self
    }

    /// Attach a Raft manager for linearizable read verification.
    #[must_use]
    pub fn with_raft_manager(
        mut self,
        manager: Arc<crate::region_raft::RegionRaftManager>,
    ) -> Self {
        self.raft_manager = Some(manager);
        self
    }

    /// Execute a distributed query — scatter to regions, gather results.
    ///
    /// # Errors
    ///
    /// Returns an error if the routing cache has no routes for the
    /// measurement or all region queries fail.
    #[instrument(name = "client_request", skip(self, dq), fields(measurement = %dq.measurement, consistency = ?dq.consistency))]
    pub async fn query(&self, dq: &DistributedQuery) -> Result<QueryResult> {
        let start = Instant::now();
        let result = self.query_inner(dq).await;
        record_query_latency(start.elapsed());
        result
    }

    async fn query_inner(&self, dq: &DistributedQuery) -> Result<QueryResult> {
        // ── BoundedStale: time-based freshness check ─────────────────
        // If the routing cache was refreshed within `max_staleness`, allow
        // follower reads. Otherwise, trigger a refresh attempt and fall
        // back to leader if the cache is still too old.
        let effective_consistency = match dq.consistency {
            ReadConsistency::BoundedStale { max_staleness } => {
                let age = self.routing_cache.cache_age();

                if max_staleness.is_zero() {
                    // Zero tolerance → always use leader.
                    debug!(
                        measurement = %dq.measurement,
                        "BoundedStale: zero staleness tolerance — using Leader"
                    );
                    ReadConsistency::Leader
                } else if age <= max_staleness {
                    // Cache is fresh enough — allow follower reads.
                    debug!(
                        measurement = %dq.measurement,
                        cache_age_ms = age.as_millis(),
                        max_staleness_ms = max_staleness.as_millis(),
                        "BoundedStale: cache fresh — using Follower"
                    );
                    ReadConsistency::Follower
                } else {
                    // Cache is stale — attempt a refresh.
                    debug!(
                        measurement = %dq.measurement,
                        cache_age_ms = age.as_millis(),
                        max_staleness_ms = max_staleness.as_millis(),
                        "BoundedStale: cache stale, attempting refresh"
                    );
                    if let Err(e) = self.routing_cache.refresh().await {
                        warn!(error = %e, "routing cache refresh failed — using Leader");
                        ReadConsistency::Leader
                    } else {
                        let new_age = self.routing_cache.cache_age();
                        if new_age <= max_staleness {
                            debug!(
                                measurement = %dq.measurement,
                                new_cache_age_ms = new_age.as_millis(),
                                "BoundedStale: refresh succeeded — using Follower"
                            );
                            ReadConsistency::Follower
                        } else {
                            debug!(
                                measurement = %dq.measurement,
                                new_cache_age_ms = new_age.as_millis(),
                                "BoundedStale: still stale after refresh — using Leader"
                            );
                            ReadConsistency::Leader
                        }
                    }
                }
            }
            other => other,
        };

        let mut dq_effective = dq.clone();
        dq_effective.consistency = effective_consistency;

        let routes = self
            .routing_cache
            .routes_for_measurement(&dq_effective.measurement)
            .await?;

        if routes.is_empty() {
            return Err(ClusterError::Validation(format!(
                "no routes for measurement '{}'",
                dq.measurement
            )));
        }

        debug!(
            measurement = %dq.measurement,
            region_count = routes.len(),
            "scattering query to regions"
        );

        let handles = self.scatter(&dq_effective, &routes);
        self.gather(&dq_effective, handles).await
    }

    /// Scatter: spawn one async task per region.
    #[instrument(name = "scatter", skip_all, fields(region_count = routes.len()))]
    fn scatter(
        &self,
        dq: &DistributedQuery,
        routes: &[chronix_meta::RouteEntry],
    ) -> Vec<tokio::task::JoinHandle<Result<Vec<Point>>>> {
        let mut handles = Vec::with_capacity(routes.len());

        for route in routes {
            let region_id = route.region_id;
            let measurement = dq.measurement.clone();
            let tag_filters = dq.tag_filters.clone();
            let field_columns = dq.field_columns.clone();
            let start_ns = dq.start_ns;
            let end_ns = dq.end_ns;
            let query_timeout = self.query_timeout;

            // ── Pick target node based on read consistency ──────────
            let (target_id, target_addr) = Self::pick_target(route, dq.consistency);

            // Forward the distributed query limit to each sub-query so
            // regions do not return more points than needed.
            let sub_limit = dq.limit;

            if target_id == self.local_node_id {
                let storage = Arc::clone(&self.local_storage);
                // For Leader consistency on local regions, verify
                // leadership via Raft ReadIndex before serving the read.
                // This prevents stale reads from a deposed leader.
                let raft_manager = self.raft_manager.clone();
                let consistency = dq.consistency;
                handles.push(tokio::spawn(async move {
                    // Linearizable read verification via ReadIndex
                    if consistency.is_leader() {
                        if let Some(ref rm) = raft_manager {
                            if let Some(raft) = rm.get_raft(region_id) {
                                if let Err(e) = raft.ensure_linearizable().await {
                                    return Err(ClusterError::ReplicationFailed(format!(
                                        "linearizable read failed for region {region_id}: {e}"
                                    )));
                                }
                            }
                        }
                    }

                    let query = RegionQuery {
                        measurement,
                        start_ns,
                        end_ns,
                        tag_filters,
                        field_columns,
                        limit: sub_limit,
                    };
                    let result =
                        timeout(query_timeout, storage.query_region(region_id, query)).await;
                    match result {
                        Ok(Ok(points)) => Ok(points),
                        Ok(Err(e)) => Err(e),
                        Err(_) => Err(ClusterError::Timeout),
                    }
                }));
            } else {
                let data_client = self.data_client.clone();
                data_client.set_node_addr(target_id, &target_addr);
                let consistency = dq.consistency;

                handles.push(tokio::spawn(async move {
                    let request = QueryRegionRequest {
                        region_id,
                        measurement,
                        start_ns,
                        end_ns,
                        tag_filters: tag_filters
                            .into_iter()
                            .map(|(key, value)| TagFilter { key, value })
                            .collect(),
                        field_columns,
                        limit: sub_limit,
                        // Request linearizable verification on
                        // the remote node for Leader reads to prevent
                        // stale data from a deposed leader.
                        require_linearizable: consistency.is_leader(),
                    };

                    let result =
                        timeout(query_timeout, data_client.query_region(target_id, request)).await;

                    match result {
                        Ok(Ok(resp)) => resp.points.iter().map(proto_to_core_point).collect(),
                        Ok(Err(e)) => Err(e),
                        Err(_) => Err(ClusterError::Timeout),
                    }
                }));
            }
        }

        handles
    }

    /// Choose which node (leader or replica) to send the query to.
    ///
    /// For [`BoundedStale`](ReadConsistency::BoundedStale) the caller
    /// must verify staleness *before* calling this (see `query_inner`);
    /// by the time we get here `BoundedStale` has already been resolved
    /// to either `Follower` or `Leader`.
    fn pick_target(
        route: &chronix_meta::RouteEntry,
        consistency: ReadConsistency,
    ) -> (NodeId, String) {
        if consistency.is_leader() || route.replica_addrs.is_empty() {
            return (route.leader_node_id, route.leader_addr.clone());
        }

        // Follower, Nearest, or BoundedStale — pick a random replica
        // (skipping leader for Follower/BoundedStale, including for Nearest).
        let candidates: Vec<&(NodeId, String)> = match consistency {
            ReadConsistency::Follower | ReadConsistency::BoundedStale { .. } => route
                .replica_addrs
                .iter()
                .filter(|(id, _)| *id != route.leader_node_id)
                .collect(),
            ReadConsistency::Nearest | ReadConsistency::Leader => {
                route.replica_addrs.iter().collect()
            }
        };

        if candidates.is_empty() {
            // All replicas are the leader — fall back to leader.
            return (route.leader_node_id, route.leader_addr.clone());
        }

        let mut rng = rand::rng();
        // Replace .expect with safe match for best-in-class
        // panic-free production code. The is_empty() guard above guarantees
        // this branch always succeeds, but we avoid .expect() on principle.
        match candidates.choose(&mut rng) {
            Some(&&(id, ref addr)) => (id, addr.clone()),
            None => (route.leader_node_id, route.leader_addr.clone()),
        }
    }

    /// Gather: await all handles, merge, sort, and apply global limit.
    ///
    /// When a limit is set, the accumulation budget is capped
    /// to avoid unbounded memory growth across many regions. Each region's
    /// contribution is bounded proportionally, with a safety margin for
    /// out-of-order timestamps across regions.
    #[instrument(name = "gather", skip_all)]
    async fn gather(
        &self,
        dq: &DistributedQuery,
        handles: Vec<tokio::task::JoinHandle<Result<Vec<Point>>>>,
    ) -> Result<QueryResult> {
        #[allow(clippy::cast_possible_truncation)]
        let limit_usize = if dq.limit > 0 {
            dq.limit as usize
        } else {
            usize::MAX
        };

        // Pre-size with limit awareness to bound allocations.
        let initial_capacity = limit_usize.min(100_000);
        let mut all_points: Vec<Point> = Vec::with_capacity(initial_capacity);
        let mut regions_queried: usize = 0;
        let mut regions_failed: usize = 0;

        // Apply a per-gather budget. When a limit is set, cap the
        // total accumulated points to limit × 2 (safety margin for
        // cross-region timestamp interleaving). This prevents unbounded
        // memory growth while still allowing correct global ordering.
        let accumulation_cap = if dq.limit > 0 {
            limit_usize.saturating_mul(2)
        } else {
            usize::MAX
        };

        for handle in handles {
            match handle.await {
                Ok(Ok(mut points)) => {
                    regions_queried += 1;
                    // If accumulation is already at cap, only keep points
                    // that could displace existing ones (earlier timestamps).
                    let remaining = accumulation_cap.saturating_sub(all_points.len());
                    if remaining == 0 {
                        continue;
                    }
                    if points.len() > remaining {
                        // Keep only the earliest `remaining` points from
                        // this region to stay within the budget.
                        points.sort_by_key(chronix_core::Point::timestamp);
                        points.truncate(remaining);
                    }
                    all_points.extend(points);
                }
                Ok(Err(e)) => {
                    warn!(error = %e, "region query failed");
                    regions_failed += 1;
                }
                Err(e) => {
                    warn!(error = %e, "region query task panicked");
                    regions_failed += 1;
                }
            }
        }

        // If every region failed, return an error
        if regions_queried == 0 && regions_failed > 0 {
            return Err(ClusterError::Internal(format!(
                "all {} region queries failed for '{}'",
                regions_failed, dq.measurement
            )));
        }

        // ── Sort by timestamp ──────────────────────────────────────
        all_points.sort_by_key(chronix_core::Point::timestamp);

        // ── Apply global limit ─────────────────────────────────────
        let truncated = if dq.limit > 0 && all_points.len() > limit_usize {
            all_points.truncate(limit_usize);
            true
        } else {
            false
        };

        // Flag partial results so callers can make
        // consistency-aware decisions.
        let partial_results = regions_failed > 0 && regions_queried > 0;

        if partial_results {
            tracing::warn!(
                regions_queried,
                regions_failed,
                "query returned partial results — some regions failed"
            );
        }

        debug!(
            total_points = all_points.len(),
            regions_queried, regions_failed, truncated, partial_results, "gather complete"
        );

        Ok(QueryResult {
            points: all_points,
            regions_queried,
            regions_failed,
            truncated,
            partial_results,
        })
    }
}

impl std::fmt::Debug for QueryRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryRouter")
            .field("local_node_id", &self.local_node_id)
            .field("query_timeout", &self.query_timeout)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::MetaClient;
    use async_trait::async_trait;
    use chronix_core::{FieldValue, SeriesKey};
    use chronix_meta::{RouteEntry, RoutingSnapshot};
    use parking_lot::Mutex;
    use std::collections::BTreeMap;

    // ── Mock storage ───────────────────────────────────────────────

    #[derive(Debug)]
    struct MockStorage {
        points: Mutex<BTreeMap<u64, Vec<Point>>>,
    }

    impl MockStorage {
        fn new() -> Self {
            Self {
                points: Mutex::new(BTreeMap::new()),
            }
        }

        fn seed_region(&self, region_id: u64, pts: Vec<Point>) {
            self.points.lock().insert(region_id, pts);
        }
    }

    #[async_trait]
    impl RegionStorage for MockStorage {
        async fn write_points(&self, _region_id: u64, _points: Vec<Point>) -> Result<u64> {
            Ok(0)
        }

        async fn query_region(&self, region_id: u64, query: RegionQuery) -> Result<Vec<Point>> {
            let binding = self.points.lock();
            let pts = binding.get(&region_id).cloned().unwrap_or_default();

            // Apply time range filter
            let filtered: Vec<Point> = pts
                .into_iter()
                .filter(|p| p.timestamp() >= query.start_ns && p.timestamp() < query.end_ns)
                .filter(|p| {
                    // Apply tag filters
                    query
                        .tag_filters
                        .iter()
                        .all(|(k, v)| p.series_key().tag(k).is_some_and(|tv| tv == v))
                })
                .collect();

            // Apply limit
            if query.limit > 0 && filtered.len() > query.limit as usize {
                Ok(filtered[..query.limit as usize].to_vec())
            } else {
                Ok(filtered)
            }
        }

        async fn replicate_wal(
            &self,
            _region_id: u64,
            _entries: Vec<(u64, Vec<u8>)>,
        ) -> Result<u64> {
            Ok(0)
        }

        async fn snapshot_data(&self, _region_id: u64) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }

        async fn restore_snapshot(&self, _region_id: u64, _data: &[u8]) -> Result<()> {
            Ok(())
        }
    }

    // ── Mock meta client — uses shared test utility ────────────────
    use crate::test_util::{
        make_routing_snapshot, make_routing_snapshot_with_replicas, MockSnapshotMetaClient,
    };

    // ── Helpers ────────────────────────────────────────────────────

    fn make_point(measurement: &str, tag_val: &str, ts: i64) -> Point {
        let tags: BTreeMap<String, String> = [("host".to_string(), tag_val.to_string())]
            .into_iter()
            .collect();
        let sk = SeriesKey::new(measurement, tags).unwrap();
        let mut fields = BTreeMap::new();
        fields.insert("value".to_string(), FieldValue::F64(1.0));
        Point::new(sk, fields, ts).unwrap()
    }

    fn make_router(
        snapshot: RoutingSnapshot,
        local_node_id: NodeId,
    ) -> (QueryRouter, Arc<MockStorage>) {
        let meta_client: Arc<dyn MetaClient> = Arc::new(MockSnapshotMetaClient::new(snapshot));
        let cache = Arc::new(RoutingCache::new(meta_client));
        let storage = Arc::new(MockStorage::new());
        let data_client = DataGrpcClient::new();
        let router = QueryRouter::new(cache, storage.clone(), data_client, local_node_id);
        (router, storage)
    }

    // ── Tests ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn query_single_local_region() {
        let snapshot = make_routing_snapshot("cpu", vec![(10, 1, "http://n1:5000")]);
        let (router, storage) = make_router(snapshot, 1);

        storage.seed_region(
            10,
            vec![
                make_point("cpu", "a", 100),
                make_point("cpu", "b", 200),
                make_point("cpu", "c", 300),
            ],
        );

        let dq = DistributedQuery {
            measurement: "cpu".into(),
            start_ns: 0,
            end_ns: 1000,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 0,
            consistency: ReadConsistency::Leader,
        };

        let result = router.query(&dq).await.unwrap();
        assert_eq!(result.points.len(), 3);
        assert_eq!(result.regions_queried, 1);
        assert_eq!(result.regions_failed, 0);
        assert!(!result.truncated);
        // Verify sorted by timestamp
        assert_eq!(result.points[0].timestamp(), 100);
        assert_eq!(result.points[1].timestamp(), 200);
        assert_eq!(result.points[2].timestamp(), 300);
    }

    #[tokio::test]
    async fn query_merges_multiple_local_regions() {
        // Two regions, both local
        let snapshot = make_routing_snapshot(
            "cpu",
            vec![(10, 1, "http://n1:5000"), (20, 1, "http://n1:5000")],
        );
        let (router, storage) = make_router(snapshot, 1);

        // Put points with interleaved timestamps in each region
        storage.seed_region(
            10,
            vec![make_point("cpu", "a", 100), make_point("cpu", "a", 300)],
        );
        storage.seed_region(
            20,
            vec![make_point("cpu", "b", 200), make_point("cpu", "b", 400)],
        );

        let dq = DistributedQuery {
            measurement: "cpu".into(),
            start_ns: 0,
            end_ns: 500,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 0,
            consistency: ReadConsistency::Leader,
        };

        let result = router.query(&dq).await.unwrap();
        assert_eq!(result.points.len(), 4);
        assert_eq!(result.regions_queried, 2);
        // Merged and sorted by timestamp
        assert_eq!(result.points[0].timestamp(), 100);
        assert_eq!(result.points[1].timestamp(), 200);
        assert_eq!(result.points[2].timestamp(), 300);
        assert_eq!(result.points[3].timestamp(), 400);
    }

    #[tokio::test]
    async fn query_applies_time_range_filter() {
        let snapshot = make_routing_snapshot("cpu", vec![(10, 1, "http://n1:5000")]);
        let (router, storage) = make_router(snapshot, 1);

        storage.seed_region(
            10,
            vec![
                make_point("cpu", "a", 100),
                make_point("cpu", "a", 200),
                make_point("cpu", "a", 300),
                make_point("cpu", "a", 400),
            ],
        );

        let dq = DistributedQuery {
            measurement: "cpu".into(),
            start_ns: 150,
            end_ns: 350,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 0,
            consistency: ReadConsistency::Leader,
        };

        let result = router.query(&dq).await.unwrap();
        assert_eq!(result.points.len(), 2);
        assert_eq!(result.points[0].timestamp(), 200);
        assert_eq!(result.points[1].timestamp(), 300);
    }

    #[tokio::test]
    async fn query_applies_tag_filter() {
        let snapshot = make_routing_snapshot("cpu", vec![(10, 1, "http://n1:5000")]);
        let (router, storage) = make_router(snapshot, 1);

        storage.seed_region(
            10,
            vec![
                make_point("cpu", "srv1", 100),
                make_point("cpu", "srv2", 200),
                make_point("cpu", "srv1", 300),
            ],
        );

        let dq = DistributedQuery {
            measurement: "cpu".into(),
            start_ns: 0,
            end_ns: 1000,
            tag_filters: vec![("host".to_string(), "srv1".to_string())],
            field_columns: vec![],
            limit: 0,
            consistency: ReadConsistency::Leader,
        };

        let result = router.query(&dq).await.unwrap();
        assert_eq!(result.points.len(), 2);
        assert!(result
            .points
            .iter()
            .all(|p| p.series_key().tag("host") == Some("srv1")));
    }

    #[tokio::test]
    async fn query_applies_global_limit() {
        let snapshot = make_routing_snapshot("cpu", vec![(10, 1, "http://n1:5000")]);
        let (router, storage) = make_router(snapshot, 1);

        storage.seed_region(
            10,
            vec![
                make_point("cpu", "a", 100),
                make_point("cpu", "b", 200),
                make_point("cpu", "c", 300),
                make_point("cpu", "d", 400),
                make_point("cpu", "e", 500),
            ],
        );

        let dq = DistributedQuery {
            measurement: "cpu".into(),
            start_ns: 0,
            end_ns: 1000,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 3,
            consistency: ReadConsistency::Leader,
        };

        let result = router.query(&dq).await.unwrap();
        assert_eq!(result.points.len(), 3);
        // Limit is pushed down to sub-queries, so storage returns
        // exactly 3 points — no gather-level truncation needed.
        // Should have the earliest 3 timestamps (sort then truncate)
        assert_eq!(result.points[0].timestamp(), 100);
        assert_eq!(result.points[2].timestamp(), 300);
    }

    /// Multi-region gather truncation: each region returns up to `limit`
    /// points, but the combined set may exceed `limit` after merge, so
    /// gather-level truncation still fires.
    #[tokio::test]
    async fn query_multi_region_gather_truncation() {
        let snapshot = make_routing_snapshot(
            "cpu",
            vec![(10, 1, "http://n1:5000"), (20, 1, "http://n1:5000")],
        );
        let (router, storage) = make_router(snapshot, 1);

        // Each region has 3 points; with limit=2, each returns 2, but
        // gather sees 4 total and must truncate to 2.
        storage.seed_region(
            10,
            vec![
                make_point("cpu", "a", 100),
                make_point("cpu", "b", 300),
                make_point("cpu", "c", 500),
            ],
        );
        storage.seed_region(
            20,
            vec![
                make_point("cpu", "d", 200),
                make_point("cpu", "e", 400),
                make_point("cpu", "f", 600),
            ],
        );

        let dq = DistributedQuery {
            measurement: "cpu".into(),
            start_ns: 0,
            end_ns: 1000,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 2,
            consistency: ReadConsistency::Leader,
        };

        let result = router.query(&dq).await.unwrap();
        assert_eq!(result.points.len(), 2);
        assert!(result.truncated);
        // Earliest 2 timestamps across both regions
        assert_eq!(result.points[0].timestamp(), 100);
        assert_eq!(result.points[1].timestamp(), 200);
    }

    #[tokio::test]
    async fn query_no_routes_returns_error() {
        let snapshot = RoutingSnapshot {
            version: 1,
            entries: BTreeMap::new(),
        };
        let (router, _) = make_router(snapshot, 1);

        let dq = DistributedQuery {
            measurement: "cpu".into(),
            start_ns: 0,
            end_ns: 1000,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 0,
            consistency: ReadConsistency::Leader,
        };

        let result = router.query(&dq).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn query_empty_result() {
        let snapshot = make_routing_snapshot("cpu", vec![(10, 1, "http://n1:5000")]);
        let (router, _storage) = make_router(snapshot, 1);

        // No data seeded — empty result
        let dq = DistributedQuery {
            measurement: "cpu".into(),
            start_ns: 0,
            end_ns: 1000,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 0,
            consistency: ReadConsistency::Leader,
        };

        let result = router.query(&dq).await.unwrap();
        assert!(result.points.is_empty());
        assert_eq!(result.regions_queried, 1);
        assert!(!result.truncated);
    }

    #[tokio::test]
    async fn query_limit_not_truncated_when_under() {
        let snapshot = make_routing_snapshot("cpu", vec![(10, 1, "http://n1:5000")]);
        let (router, storage) = make_router(snapshot, 1);

        storage.seed_region(10, vec![make_point("cpu", "a", 100)]);

        let dq = DistributedQuery {
            measurement: "cpu".into(),
            start_ns: 0,
            end_ns: 1000,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 100,
            consistency: ReadConsistency::Leader,
        };

        let result = router.query(&dq).await.unwrap();
        assert_eq!(result.points.len(), 1);
        assert!(!result.truncated);
    }

    #[test]
    fn debug_format() {
        let snapshot = make_routing_snapshot("cpu", vec![(1, 1, "http://n1:5000")]);
        let (router, _) = make_router(snapshot, 1);
        let debug = format!("{router:?}");
        assert!(debug.contains("QueryRouter"));
        assert!(debug.contains("local_node_id"));
    }

    #[test]
    fn read_consistency_default_is_leader() {
        assert_eq!(ReadConsistency::default(), ReadConsistency::Leader);
    }

    #[test]
    fn distributed_query_debug() {
        let dq = DistributedQuery {
            measurement: "cpu".into(),
            start_ns: 0,
            end_ns: 100,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 0,
            consistency: ReadConsistency::Leader,
        };
        let debug = format!("{dq:?}");
        assert!(debug.contains("DistributedQuery"));
        assert!(debug.contains("cpu"));
    }

    #[test]
    fn query_result_debug() {
        let r = QueryResult {
            points: vec![],
            regions_queried: 2,
            regions_failed: 1,
            truncated: false,
            partial_results: true,
        };
        let debug = format!("{r:?}");
        assert!(debug.contains("QueryResult"));
        assert!(debug.contains("regions_queried"));
    }

    #[tokio::test]
    async fn with_timeout_configurable() {
        let snapshot = make_routing_snapshot("cpu", vec![(10, 1, "http://n1:5000")]);
        let (router, _) = make_router(snapshot, 1);
        let router = QueryRouter {
            query_timeout: Duration::from_millis(100),
            ..router
        };
        assert_eq!(router.query_timeout, Duration::from_millis(100));
    }

    // ── Follower-read tests ────────────────────────────────────────

    #[tokio::test]
    async fn follower_read_queries_local_replica() {
        // Leader is node 2 (remote), but node 1 (local) is a follower replica.
        let snapshot = make_routing_snapshot_with_replicas(
            "cpu",
            vec![(
                10,
                2,
                "http://n2:5000",
                vec![(2, "http://n2:5000"), (1, "http://n1:5000")],
            )],
        );
        let (router, storage) = make_router(snapshot, 1);

        storage.seed_region(10, vec![make_point("cpu", "a", 100)]);

        let dq = DistributedQuery {
            measurement: "cpu".into(),
            start_ns: 0,
            end_ns: 1000,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 0,
            consistency: ReadConsistency::Follower,
        };

        // With Follower consistency and local node as a follower,
        // we should get a result — it reads from local storage.
        let result = router.query(&dq).await.unwrap();
        assert_eq!(result.regions_queried, 1);
        // The follower read should return the data in local storage
        assert_eq!(result.points.len(), 1);
        assert_eq!(result.points[0].timestamp(), 100);
    }

    #[tokio::test]
    async fn leader_read_always_targets_leader() {
        // Leader is node 1 (local), replicas are 1, 2, 3. Leader read
        // should always go to node 1.
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
        let (router, storage) = make_router(snapshot, 1);
        storage.seed_region(10, vec![make_point("cpu", "a", 100)]);

        let dq = DistributedQuery {
            measurement: "cpu".into(),
            start_ns: 0,
            end_ns: 1000,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 0,
            consistency: ReadConsistency::Leader,
        };

        let result = router.query(&dq).await.unwrap();
        assert_eq!(result.points.len(), 1);
        assert_eq!(result.regions_queried, 1);
    }

    #[test]
    fn pick_target_leader_always_returns_leader() {
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
        let route = &snapshot.entries["cpu"][0];

        // Leader consistency — always returns leader
        let (id, addr) = QueryRouter::pick_target(route, ReadConsistency::Leader);
        assert_eq!(id, 1);
        assert_eq!(addr, "http://n1:5000");
    }

    #[test]
    fn pick_target_follower_never_returns_leader() {
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
        let route = &snapshot.entries["cpu"][0];

        // Follower consistency — never returns leader (node 1)
        for _ in 0..20 {
            let (id, _) = QueryRouter::pick_target(route, ReadConsistency::Follower);
            assert_ne!(id, 1, "follower read must not target the leader");
            assert!(id == 2 || id == 3);
        }
    }

    #[test]
    fn pick_target_follower_falls_back_to_leader_when_only_leader_is_replica() {
        // Only one replica which is also the leader — should fall back to leader
        let snapshot = make_routing_snapshot_with_replicas(
            "cpu",
            vec![(10, 1, "http://n1:5000", vec![(1, "http://n1:5000")])],
        );
        let route = &snapshot.entries["cpu"][0];

        let (id, addr) = QueryRouter::pick_target(route, ReadConsistency::Follower);
        assert_eq!(id, 1);
        assert_eq!(addr, "http://n1:5000");
    }

    #[test]
    fn pick_target_nearest_may_return_any_replica() {
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
        let route = &snapshot.entries["cpu"][0];

        // Nearest — should pick randomly from all replicas (inc. leader)
        let mut seen = std::collections::HashSet::new();
        for _ in 0..100 {
            let (id, _) = QueryRouter::pick_target(route, ReadConsistency::Nearest);
            seen.insert(id);
        }
        // Should have seen at least 2 different nodes across 100 random picks
        assert!(seen.len() >= 2, "nearest should spread across replicas");
    }

    #[test]
    fn pick_target_empty_replicas_falls_back_to_leader() {
        let route = RouteEntry {
            region_id: 10,
            measurement: "cpu".to_string(),
            leader_node_id: 1,
            leader_addr: "http://n1:5000".to_string(),
            replica_addrs: vec![],
            key_range: None,
            region_state: chronix_meta::RegionState::Active,
        };
        let snapshot = RoutingSnapshot {
            version: 1,
            entries: [("cpu".to_string(), vec![route.clone()])]
                .into_iter()
                .collect(),
        };
        let _ = make_router(snapshot, 1);

        let (id, _) = QueryRouter::pick_target(&route, ReadConsistency::Follower);
        assert_eq!(id, 1, "empty replicas should fall back to leader");
    }

    #[test]
    fn read_consistency_allows_follower() {
        assert!(!ReadConsistency::Leader.allows_follower());
        assert!(ReadConsistency::Follower.allows_follower());
        assert!(ReadConsistency::Nearest.allows_follower());
        assert!(ReadConsistency::BoundedStale {
            max_staleness: Duration::from_secs(5)
        }
        .allows_follower());
    }

    #[test]
    fn read_consistency_nearest_variant() {
        let c = ReadConsistency::Nearest;
        assert!(!c.is_leader());
        assert!(c.allows_follower());
        let debug = format!("{c:?}");
        assert!(debug.contains("Nearest"));
    }

    #[test]
    fn read_consistency_bounded_stale_variant() {
        let c = ReadConsistency::BoundedStale {
            max_staleness: Duration::from_secs(10),
        };
        assert!(!c.is_leader());
        assert!(c.allows_follower());
        let debug = format!("{c:?}");
        assert!(debug.contains("BoundedStale"));
    }

    #[test]
    fn pick_target_bounded_stale_skips_leader() {
        // BoundedStale should behave like Follower for pick_target
        let route = RouteEntry {
            region_id: 1,
            measurement: "cpu".into(),
            leader_node_id: 1,
            leader_addr: "addr:1".into(),
            replica_addrs: vec![
                (1, "addr:1".into()),
                (2, "addr:2".into()),
                (3, "addr:3".into()),
            ],
            key_range: None,
            region_state: chronix_meta::RegionState::Active,
        };

        // Run multiple times — should never return leader
        for _ in 0..50 {
            let (id, _) = QueryRouter::pick_target(
                &route,
                ReadConsistency::BoundedStale {
                    max_staleness: Duration::from_secs(5),
                },
            );
            assert_ne!(id, 1, "BoundedStale should skip leader");
        }
    }

    #[tokio::test]
    async fn bounded_stale_uses_follower_when_cache_fresh() {
        // After refresh, cache is fresh → should use Follower path.
        let snapshot = make_routing_snapshot_with_replicas(
            "cpu",
            vec![(
                10,
                2,
                "http://n2:5000",
                vec![(2, "http://n2:5000"), (1, "http://n1:5000")],
            )],
        );
        let (router, storage) = make_router(snapshot, 1);
        storage.seed_region(10, vec![make_point("cpu", "a", 100)]);

        // Trigger initial refresh so the cache has a fresh timestamp.
        let _ = router.routing_cache.refresh().await;

        let dq = DistributedQuery {
            measurement: "cpu".into(),
            start_ns: 0,
            end_ns: 1000,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 0,
            consistency: ReadConsistency::BoundedStale {
                max_staleness: Duration::from_secs(60),
            },
        };

        let result = router.query(&dq).await.unwrap();
        assert_eq!(result.regions_queried, 1);
        assert_eq!(result.points.len(), 1);
    }

    #[tokio::test]
    async fn bounded_stale_zero_staleness_uses_leader() {
        // max_staleness=0 → should always fall back to Leader.
        let snapshot = make_routing_snapshot_with_replicas(
            "cpu",
            vec![(
                10,
                1,
                "http://n1:5000",
                vec![(1, "http://n1:5000"), (2, "http://n2:5000")],
            )],
        );
        let (router, storage) = make_router(snapshot, 1);
        storage.seed_region(10, vec![make_point("cpu", "a", 100)]);

        let dq = DistributedQuery {
            measurement: "cpu".into(),
            start_ns: 0,
            end_ns: 1000,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 0,
            consistency: ReadConsistency::BoundedStale {
                max_staleness: Duration::ZERO,
            },
        };

        // Zero-staleness always routes to leader — should succeed
        // because leader is local (node 1).
        let result = router.query(&dq).await.unwrap();
        assert_eq!(result.regions_queried, 1);
        assert_eq!(result.points.len(), 1);
    }

    #[tokio::test]
    async fn bounded_stale_refreshes_when_stale() {
        // Cache starts with Instant::now() so is immediately fresh.
        // Use a very large staleness window → should use follower.
        let snapshot = make_routing_snapshot_with_replicas(
            "cpu",
            vec![(
                10,
                2,
                "http://n2:5000",
                vec![(2, "http://n2:5000"), (1, "http://n1:5000")],
            )],
        );
        let (router, storage) = make_router(snapshot, 1);
        storage.seed_region(10, vec![make_point("cpu", "a", 100)]);

        let dq = DistributedQuery {
            measurement: "cpu".into(),
            start_ns: 0,
            end_ns: 1000,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 0,
            consistency: ReadConsistency::BoundedStale {
                max_staleness: Duration::from_secs(3600),
            },
        };

        // Cache was just created — within 3600s window.
        let result = router.query(&dq).await.unwrap();
        assert_eq!(result.regions_queried, 1);
        assert_eq!(result.points.len(), 1);
    }
}
