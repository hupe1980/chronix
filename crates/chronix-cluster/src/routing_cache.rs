//! Cached routing table with stale-detection and automatic refresh.
//!
//! [`RoutingCache`] wraps a local copy of the global routing table and
//! transparently refreshes it when a lookup fails or the version becomes
//! stale. This implements the "stale routing retry" pattern from
//! Story 1.3.
//!
//! ## Backoff (R4 hardening)
//!
//! Refresh retries use exponential backoff with jitter (base 50 ms,
//! capped at 800 ms) to avoid thundering-herd effects against the
//! `MetaNode` cluster during outages.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chronix_meta::{RegionId, RouteEntry, RoutingSnapshot};
use parking_lot::{Mutex, RwLock};
use rand::Rng;
use tracing::{debug, warn};

use crate::client::MetaClient;
use crate::error::{ClusterError, Result};

/// Circuit breaker state for meta-cluster communication.
///
/// Prevents thundering herd on sustained failures:
/// - **Closed** (healthy): requests pass through normally.
/// - **Open** (tripped): refreshes are rejected immediately for a cooldown.
/// - **Half-open**: one probe request is allowed; success resets, failure
///   re-opens the circuit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

/// Default number of consecutive failures before tripping the circuit.
const CIRCUIT_BREAKER_THRESHOLD: u32 = 5;
/// Default cooldown after the circuit trips open.
const CIRCUIT_BREAKER_COOLDOWN: Duration = Duration::from_secs(30);

/// Locally cached routing table with automatic refresh on miss.
///
/// `QueryNodes` and `DataNodes` use this to route writes and queries to
/// the correct region leader. When a lookup returns `RegionNotFound` or
/// the cached version is older than the cluster version, the cache is
/// refreshed from the `MetaNode` cluster via the provided `MetaClient`.
pub struct RoutingCache {
    /// Current local snapshot.
    snapshot: Arc<RwLock<RoutingSnapshot>>,
    /// Secondary index: `region_id` → `RouteEntry` for O(1) lookups.
    region_index: Arc<RwLock<HashMap<RegionId, RouteEntry>>>,
    /// Client for fetching fresh routing tables.
    client: Arc<dyn MetaClient>,
    /// Maximum number of refresh retries on stale routing.
    max_retries: u32,
    /// Base delay for exponential backoff (default: 50 ms).
    retry_base_delay: Duration,
    /// Maximum backoff delay cap (default: 800 ms).
    retry_max_delay: Duration,
    /// Highest routing-table version ever seen from the meta cluster.
    latest_seen_version: Arc<AtomicU64>,
    /// Wall-clock instant of the last successful cache refresh.
    last_refresh_at: Mutex<std::time::Instant>,
    /// Circuit breaker: consecutive refresh failures.
    cb_consecutive_failures: AtomicU32,
    /// Circuit breaker: instant when the open state expires and transitions to half-open.
    cb_open_until: Mutex<Option<std::time::Instant>>,
    /// Circuit breaker failure threshold.
    cb_threshold: u32,
    /// Circuit breaker cooldown duration.
    cb_cooldown: Duration,
}

impl RoutingCache {
    /// Create a new routing cache backed by the given meta client.
    ///
    /// The cache starts empty and is populated on the first lookup.
    #[must_use]
    pub fn new(client: Arc<dyn MetaClient>) -> Self {
        Self {
            snapshot: Arc::new(RwLock::new(RoutingSnapshot::empty())),
            region_index: Arc::new(RwLock::new(HashMap::new())),
            client,
            max_retries: 3,
            retry_base_delay: Duration::from_millis(50),
            retry_max_delay: Duration::from_millis(800),
            latest_seen_version: Arc::new(AtomicU64::new(0)),
            last_refresh_at: Mutex::new(std::time::Instant::now()),
            cb_consecutive_failures: AtomicU32::new(0),
            cb_open_until: Mutex::new(None),
            cb_threshold: CIRCUIT_BREAKER_THRESHOLD,
            cb_cooldown: CIRCUIT_BREAKER_COOLDOWN,
        }
    }

    /// Set the maximum number of refresh retries.
    #[must_use]
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Current cached version.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.snapshot.read().version
    }

    /// Highest routing-table version ever observed from the meta cluster.
    ///
    /// This is updated on every successful [`refresh`](Self::refresh) and
    /// represents the most recent cluster state we have evidence of.
    #[must_use]
    pub fn latest_seen_version(&self) -> u64 {
        self.latest_seen_version.load(Ordering::Acquire)
    }

    /// How long ago the routing cache was last successfully refreshed.
    ///
    /// Used by [`BoundedStale`](crate::query_router::ReadConsistency::BoundedStale)
    /// to decide whether the cache is fresh enough for follower reads.
    #[must_use]
    pub fn cache_age(&self) -> Duration {
        self.last_refresh_at.lock().elapsed()
    }

    /// Return the full cached snapshot.
    #[must_use]
    pub fn snapshot(&self) -> RoutingSnapshot {
        self.snapshot.read().clone()
    }

    /// Look up routes for a measurement.
    ///
    /// If not found in cache, refreshes from the `MetaNode` cluster and
    /// retries up to `max_retries` times.
    ///
    /// # Errors
    ///
    /// Returns `ClusterError::RegionNotFound` if the measurement has no
    /// regions even after refresh.
    pub async fn routes_for_measurement(&self, measurement: &str) -> Result<Vec<RouteEntry>> {
        // Try local cache first.
        if let Some(entries) = self.lookup_measurement(measurement) {
            return Ok(entries);
        }

        // Stale — refresh and retry with exponential backoff.
        for attempt in 0..self.max_retries {
            debug!(measurement, attempt, "routing cache miss, refreshing");
            self.refresh().await?;

            if let Some(entries) = self.lookup_measurement(measurement) {
                return Ok(entries);
            }

            // Exponential backoff with jitter before next retry.
            Self::backoff_sleep(attempt, self.retry_base_delay, self.retry_max_delay).await;
        }

        warn!(measurement, "routing: no routes after refresh retries");
        Err(ClusterError::RegionNotFound(0))
    }

    /// Look up the route for a specific region.
    ///
    /// If not found, refreshes and retries.
    ///
    /// # Errors
    ///
    /// Returns `ClusterError::RegionNotFound` if the region is unknown
    /// even after refreshing.
    pub async fn route_for_region(&self, region_id: RegionId) -> Result<RouteEntry> {
        // Try local cache first.
        if let Some(entry) = self.lookup_region(region_id) {
            return Ok(entry);
        }

        // Stale — refresh and retry with exponential backoff.
        for attempt in 0..self.max_retries {
            debug!(
                region_id,
                attempt, "routing cache miss for region, refreshing"
            );
            self.refresh().await?;

            if let Some(entry) = self.lookup_region(region_id) {
                return Ok(entry);
            }

            // Exponential backoff with jitter before next retry.
            Self::backoff_sleep(attempt, self.retry_base_delay, self.retry_max_delay).await;
        }

        warn!(region_id, "routing: region not found after refresh retries");
        Err(ClusterError::RegionNotFound(region_id))
    }

    /// Force a refresh of the cached routing table.
    ///
    /// # Errors
    ///
    /// Returns an error if the `MetaNode` cluster is unreachable.
    pub async fn refresh(&self) -> Result<()> {
        // ── Circuit breaker check ──
        let cb_state = self.circuit_state();
        match cb_state {
            CircuitState::Open => {
                debug!("routing cache refresh blocked by circuit breaker (open)");
                return Err(ClusterError::Unavailable(
                    "routing cache circuit breaker is open".into(),
                ));
            }
            CircuitState::HalfOpen => {
                debug!("routing cache: circuit breaker half-open, allowing probe");
            }
            CircuitState::Closed => {}
        }

        let result = self.client.get_routing_table().await;

        match result {
            Ok(new_snapshot) => {
                // Success → reset circuit breaker.
                self.cb_consecutive_failures.store(0, Ordering::Relaxed);
                *self.cb_open_until.lock() = None;

                let new_version = new_snapshot.version;
                let old_version = self.version();

                self.latest_seen_version
                    .fetch_max(new_version, Ordering::Release);
                *self.last_refresh_at.lock() = std::time::Instant::now();

                if new_version >= old_version {
                    let mut index =
                        HashMap::with_capacity(new_snapshot.entries.values().map(Vec::len).sum());
                    for entries in new_snapshot.entries.values() {
                        for entry in entries {
                            index.insert(entry.region_id, entry.clone());
                        }
                    }

                    *self.region_index.write() = index;
                    *self.snapshot.write() = new_snapshot;

                    debug!(old_version, new_version, "routing cache refreshed");
                } else {
                    debug!(
                        old_version,
                        new_version,
                        "routing cache: ignoring stale snapshot (version went backwards)"
                    );
                }

                Ok(())
            }
            Err(e) => {
                // Failure → increment counter, possibly trip circuit.
                let prev = self.cb_consecutive_failures.fetch_add(1, Ordering::Relaxed);
                if prev + 1 >= self.cb_threshold {
                    let mut open_until = self.cb_open_until.lock();
                    *open_until = Some(std::time::Instant::now() + self.cb_cooldown);
                    warn!(
                        consecutive_failures = prev + 1,
                        cooldown_secs = self.cb_cooldown.as_secs(),
                        "routing cache circuit breaker tripped open"
                    );
                }
                Err(e)
            }
        }
    }

    /// Determine the current circuit breaker state.
    fn circuit_state(&self) -> CircuitState {
        let failures = self.cb_consecutive_failures.load(Ordering::Relaxed);
        if failures < self.cb_threshold {
            return CircuitState::Closed;
        }
        let open_until = self.cb_open_until.lock();
        match *open_until {
            Some(deadline) if std::time::Instant::now() < deadline => CircuitState::Open,
            _ => CircuitState::HalfOpen,
        }
    }

    /// Check if a specific version is stale compared to a known cluster
    /// version.
    #[must_use]
    pub fn is_stale(&self, cluster_version: u64) -> bool {
        self.version() < cluster_version
    }

    /// Lookup measurement in local cache (no network).
    fn lookup_measurement(&self, measurement: &str) -> Option<Vec<RouteEntry>> {
        let snap = self.snapshot.read();
        snap.entries.get(measurement).cloned()
    }

    /// Lookup region in local cache via O(1) index (no network).
    fn lookup_region(&self, region_id: RegionId) -> Option<RouteEntry> {
        self.region_index.read().get(&region_id).cloned()
    }

    /// Exponential backoff with jitter.
    ///
    /// Delay = min(base * 2^attempt + jitter, max_delay).
    async fn backoff_sleep(attempt: u32, base: Duration, max: Duration) {
        let exp = base.saturating_mul(1u32 << attempt.min(6));
        let capped = exp.min(max);
        // Add up to 50% jitter.
        let jitter_ms = rand::rng()
            .random_range(0..=u64::try_from(capped.as_millis().max(1)).unwrap_or(u64::MAX) / 2);
        let delay = capped + Duration::from_millis(jitter_ms);
        tokio::time::sleep(delay).await;
    }
}

impl std::fmt::Debug for RoutingCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snap = self.snapshot.read();
        f.debug_struct("RoutingCache")
            .field("version", &snap.version)
            .field("measurements", &snap.entries.len())
            .field("max_retries", &self.max_retries)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use chronix_meta::{
        DataNodeInfo, MetaCommand, MetaNetworkFactory, MetaRaft, MetaRouter, MetaStore,
        MetaTypeConfig, RegionInfo,
    };
    use openraft::BasicNode;

    use crate::client::InProcessMetaClient;

    /// Build a single-node Raft cluster for testing.
    async fn setup_single_node() -> (Arc<MetaRaft>, MetaStore) {
        let router = MetaRouter::new();
        let store = MetaStore::new_in_memory();

        let config = Arc::new(
            openraft::Config {
                heartbeat_interval: 20,
                election_timeout_min: 50,
                election_timeout_max: 100,
                ..Default::default()
            }
            .validate()
            .expect("valid config"),
        );

        let net = MetaNetworkFactory::new(router.clone());
        let raft = openraft::Raft::<MetaTypeConfig>::new(
            1,
            config,
            net,
            store.log_store(),
            store.sm_store(),
        )
        .await
        .expect("raft");

        router.add_node(1, raft.clone());

        let mut members = BTreeMap::new();
        members.insert(1u64, BasicNode::new("127.0.0.1:9001"));
        raft.initialize(members).await.expect("init");
        tokio::time::sleep(Duration::from_millis(150)).await;

        (Arc::new(raft), store)
    }

    #[tokio::test]
    async fn cache_starts_empty() {
        let (raft, store) = setup_single_node().await;
        let client: Arc<dyn MetaClient> =
            Arc::new(InProcessMetaClient::new(raft, store.sm_store()));
        let cache = RoutingCache::new(client);

        assert_eq!(cache.version(), 0);
        assert!(cache.snapshot().entries.is_empty());
    }

    #[tokio::test]
    async fn cache_refreshes_on_measurement_miss() {
        let (raft, store) = setup_single_node().await;
        let client: Arc<dyn MetaClient> =
            Arc::new(InProcessMetaClient::new(raft.clone(), store.sm_store()));

        // Register a node and create a region with routing.
        let node = DataNodeInfo::new(1, "127.0.0.1:9001");
        client
            .propose(MetaCommand::RegisterNode(node))
            .await
            .expect("register");
        client
            .propose(MetaCommand::CreateRegion(RegionInfo::new(
                1,
                "cpu",
                1,
                vec![1],
            )))
            .await
            .expect("create region");

        let cache = RoutingCache::new(client);

        // First call triggers refresh.
        let routes = cache.routes_for_measurement("cpu").await;
        assert!(routes.is_ok());
        let routes = routes.unwrap();
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].region_id, 1);
        assert!(cache.version() > 0);
    }

    #[tokio::test]
    async fn cache_refreshes_on_region_miss() {
        let (raft, store) = setup_single_node().await;
        let client: Arc<dyn MetaClient> =
            Arc::new(InProcessMetaClient::new(raft.clone(), store.sm_store()));

        client
            .propose(MetaCommand::RegisterNode(DataNodeInfo::new(
                1,
                "127.0.0.1:9001",
            )))
            .await
            .expect("register");
        client
            .propose(MetaCommand::CreateRegion(RegionInfo::new(
                1,
                "cpu",
                1,
                vec![1],
            )))
            .await
            .expect("create region");

        let cache = RoutingCache::new(client);

        let route = cache.route_for_region(1).await;
        assert!(route.is_ok());
        assert_eq!(route.unwrap().region_id, 1);
    }

    #[tokio::test]
    async fn cache_returns_error_for_unknown_measurement() {
        let (raft, store) = setup_single_node().await;
        let client: Arc<dyn MetaClient> =
            Arc::new(InProcessMetaClient::new(raft, store.sm_store()));
        let cache = RoutingCache::new(client);

        let result = cache.routes_for_measurement("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn cache_returns_error_for_unknown_region() {
        let (raft, store) = setup_single_node().await;
        let client: Arc<dyn MetaClient> =
            Arc::new(InProcessMetaClient::new(raft, store.sm_store()));
        let cache = RoutingCache::new(client);

        let result = cache.route_for_region(999).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn cache_uses_local_on_hit() {
        let (raft, store) = setup_single_node().await;
        let client: Arc<dyn MetaClient> =
            Arc::new(InProcessMetaClient::new(raft.clone(), store.sm_store()));

        client
            .propose(MetaCommand::RegisterNode(DataNodeInfo::new(
                1,
                "127.0.0.1:9001",
            )))
            .await
            .expect("register");
        client
            .propose(MetaCommand::CreateRegion(RegionInfo::new(
                1,
                "cpu",
                1,
                vec![1],
            )))
            .await
            .expect("region");

        let cache = RoutingCache::new(client);
        cache.refresh().await.expect("refresh");

        // Second call should use cached data.
        let v1 = cache.version();
        let routes = cache.routes_for_measurement("cpu").await.expect("routes");
        assert_eq!(routes.len(), 1);
        assert_eq!(
            cache.version(),
            v1,
            "version should not change on cache hit"
        );
    }

    #[test]
    fn is_stale_detection() {
        let client: Arc<dyn MetaClient> =
            Arc::new(crate::test_util::MockSnapshotMetaClient::noop());
        let cache = RoutingCache::new(client);
        assert!(cache.is_stale(1));
        assert!(!cache.is_stale(0));
    }

    #[test]
    fn with_max_retries() {
        let client: Arc<dyn MetaClient> =
            Arc::new(crate::test_util::MockSnapshotMetaClient::noop());
        let cache = RoutingCache::new(client).with_max_retries(5);
        assert_eq!(cache.max_retries, 5);
    }

    #[test]
    fn debug_format() {
        let client: Arc<dyn MetaClient> =
            Arc::new(crate::test_util::MockSnapshotMetaClient::noop());
        let cache = RoutingCache::new(client);
        let debug = format!("{cache:?}");
        assert!(debug.contains("RoutingCache"));
        assert!(debug.contains("max_retries"));
    }
}
