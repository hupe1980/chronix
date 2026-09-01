//! `DataNode` lifecycle manager.
//!
//! Manages registering the local node with the `MetaNode` cluster,
//! periodic heartbeats, and graceful deregistration.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use chronix_meta::{ClusterConfig, DataNodeInfo, NodeId, NodeState};

use crate::client::MetaClient;
use crate::error::Result;

/// Manages the lifecycle of a single `DataNode` within the cluster.
///
/// Handles registration, periodic heartbeats, and graceful shutdown
/// against the `MetaNode` cluster via a [`MetaClient`].
pub struct DataNodeManager {
    /// This node's unique identifier.
    node_id: NodeId,
    /// gRPC address advertised to the cluster.
    grpc_addr: String,
    /// Client for communicating with the `MetaNode` cluster.
    meta_client: Arc<dyn MetaClient>,
    /// Interval between heartbeat messages.
    heartbeat_interval: Duration,
    /// Monotonically increasing heartbeat generation counter.
    ///
    /// # Memory ordering
    ///
    /// `Ordering::Relaxed` is correct here because:
    ///
    /// 1. The generation counter is only **observed** (loaded) on the
    ///    same node that **increments** it — there is no cross-node
    ///    atomic synchronisation.
    /// 2. The counter is not used to guard access to any other shared
    ///    state (no acquire/release pairing is needed).
    /// 3. `fetch_add(1, Relaxed)` guarantees atomicity of the
    ///    increment itself on all architectures.
    ///
    /// The heartbeat RPC transmits the generation value as a regular
    /// function argument, which is sequenced by the `await` point —
    /// so the remote MetaNode always sees a consistent value.
    generation: Arc<AtomicU64>,
    /// Current lifecycle state of this node.
    state: Arc<RwLock<NodeState>>,
    /// Token used to signal graceful shutdown.
    shutdown: CancellationToken,
}

impl DataNodeManager {
    /// Create a new `DataNodeManager`.
    ///
    /// The node starts in [`NodeState::Dead`] until [`start`](Self::start)
    /// is called.
    #[must_use]
    pub fn new(
        node_id: NodeId,
        grpc_addr: impl Into<String>,
        meta_client: Arc<dyn MetaClient>,
        config: &ClusterConfig,
    ) -> Self {
        Self {
            node_id,
            grpc_addr: grpc_addr.into(),
            meta_client,
            heartbeat_interval: Duration::from_secs(config.heartbeat_interval_secs),
            generation: Arc::new(AtomicU64::new(0)),
            state: Arc::new(RwLock::new(NodeState::Dead)),
            shutdown: CancellationToken::new(),
        }
    }

    /// Returns a reference to the underlying [`MetaClient`].
    #[must_use]
    pub fn meta_client(&self) -> &Arc<dyn MetaClient> {
        &self.meta_client
    }

    /// Register this node with the `MetaNode` cluster and start heartbeating.
    ///
    /// Returns a [`JoinHandle`](tokio::task::JoinHandle) for the background
    /// heartbeat task.
    ///
    /// # Errors
    ///
    /// Returns an error if the registration call fails.
    pub async fn start(&self) -> Result<tokio::task::JoinHandle<()>> {
        let info = DataNodeInfo::new(self.node_id, &self.grpc_addr);
        self.meta_client.register_node(info).await?;
        *self.state.write() = NodeState::Active;

        debug!(node_id = self.node_id, "data node registered and active");

        let meta_client = Arc::clone(&self.meta_client);
        let node_id = self.node_id;
        let interval = self.heartbeat_interval;
        let generation = Arc::clone(&self.generation);
        let state = Arc::clone(&self.state);
        let shutdown = self.shutdown.clone();

        let handle = tokio::spawn(async move {
            Self::heartbeat_loop(meta_client, node_id, interval, generation, state, shutdown).await;
        });

        Ok(handle)
    }

    /// Gracefully stop this node: cancel heartbeats and deregister.
    ///
    /// # Errors
    ///
    /// Returns an error if the deregistration call fails.
    pub async fn stop(&self) -> Result<()> {
        self.shutdown.cancel();
        *self.state.write() = NodeState::Decommissioning;

        debug!(node_id = self.node_id, "data node deregistering");
        self.meta_client.deregister_node(self.node_id).await
    }

    /// Background heartbeat loop.
    ///
    /// Sends a heartbeat to the `MetaNode` cluster at the configured interval,
    /// incrementing the generation counter each time.
    async fn heartbeat_loop(
        meta_client: Arc<dyn MetaClient>,
        node_id: NodeId,
        interval: Duration,
        generation: Arc<AtomicU64>,
        state: Arc<RwLock<NodeState>>,
        shutdown: CancellationToken,
    ) {
        // Exponential backoff on consecutive failures, capped at 4× interval.
        let mut consecutive_failures: u32 = 0;
        loop {
            let backoff = if consecutive_failures == 0 {
                interval
            } else {
                let multiplier = 1u32 << consecutive_failures.min(2); // 2×, 4×
                interval * multiplier
            };

            tokio::select! {
                () = shutdown.cancelled() => {
                    debug!(node_id, "heartbeat loop shutting down");
                    break;
                }
                () = tokio::time::sleep(backoff) => {
                    // Only heartbeat while active.
                    if *state.read() != NodeState::Active {
                        consecutive_failures = 0;
                        continue;
                    }

                    let gen = generation.fetch_add(1, Ordering::Relaxed) + 1;
                    if let Err(e) = meta_client.heartbeat(node_id, gen).await {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        warn!(
                            node_id,
                            generation = gen,
                            consecutive_failures,
                            backoff_ms = backoff.as_millis() as u64,
                            error = %e,
                            "heartbeat failed"
                        );
                    } else {
                        consecutive_failures = 0;
                    }
                }
            }
        }
    }

    /// Returns this node's ID.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Returns the current lifecycle state.
    #[must_use]
    pub fn state(&self) -> NodeState {
        *self.state.read()
    }

    /// Returns the current heartbeat generation counter.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }
}

impl std::fmt::Debug for DataNodeManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataNodeManager")
            .field("node_id", &self.node_id)
            .field("grpc_addr", &self.grpc_addr)
            .field("state", &self.state())
            .field("generation", &self.generation())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicUsize;

    use async_trait::async_trait;
    use chronix_meta::{MetaCommand, MetaResponse, RegionInfo, RoutingSnapshot};

    /// Mock [`MetaClient`] that records calls for verification.
    #[derive(Default)]
    struct MockMetaClient {
        registered: parking_lot::Mutex<bool>,
        deregistered: parking_lot::Mutex<bool>,
        heartbeat_count: AtomicUsize,
    }

    #[async_trait]
    impl MetaClient for MockMetaClient {
        async fn register_node(&self, _info: DataNodeInfo) -> Result<()> {
            *self.registered.lock() = true;
            Ok(())
        }

        async fn deregister_node(&self, _node_id: NodeId) -> Result<()> {
            *self.deregistered.lock() = true;
            Ok(())
        }

        async fn heartbeat(&self, _node_id: NodeId, _generation: u64) -> Result<()> {
            self.heartbeat_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn create_region(&self, _info: RegionInfo) -> Result<()> {
            Ok(())
        }

        async fn get_routing_table(&self) -> Result<RoutingSnapshot> {
            Ok(RoutingSnapshot {
                version: 0,
                entries: BTreeMap::new(),
            })
        }

        async fn propose(&self, _cmd: MetaCommand) -> Result<MetaResponse> {
            Ok(MetaResponse::Ok)
        }
    }

    fn test_config() -> ClusterConfig {
        ClusterConfig {
            heartbeat_interval_secs: 1,
            ..ClusterConfig::default()
        }
    }

    #[test]
    fn new_starts_in_dead_state() {
        let mock = Arc::new(MockMetaClient::default());
        let mgr = DataNodeManager::new(1, "127.0.0.1:9100", mock, &test_config());

        assert_eq!(mgr.node_id(), 1);
        assert_eq!(mgr.state(), NodeState::Dead);
        assert_eq!(mgr.generation(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn start_registers_and_sets_active() {
        let mock = Arc::new(MockMetaClient::default());
        let mgr = DataNodeManager::new(1, "127.0.0.1:9100", mock.clone(), &test_config());

        let handle = mgr.start().await.unwrap();
        assert_eq!(mgr.state(), NodeState::Active);
        assert!(*mock.registered.lock());

        mgr.stop().await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_sends_periodically() {
        let mock = Arc::new(MockMetaClient::default());
        let mgr = DataNodeManager::new(1, "127.0.0.1:9100", mock.clone(), &test_config());

        let handle = mgr.start().await.unwrap();

        // Advance past two heartbeat intervals.
        tokio::time::sleep(Duration::from_secs(2) + Duration::from_millis(50)).await;

        let count = mock.heartbeat_count.load(Ordering::Relaxed);
        assert!(count >= 2, "expected ≥2 heartbeats, got {count}");
        assert!(mgr.generation() >= 2);

        mgr.stop().await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn stop_deregisters_and_cancels() {
        let mock = Arc::new(MockMetaClient::default());
        let mgr = DataNodeManager::new(1, "127.0.0.1:9100", mock.clone(), &test_config());

        let handle = mgr.start().await.unwrap();
        mgr.stop().await.unwrap();

        assert_eq!(mgr.state(), NodeState::Decommissioning);
        assert!(*mock.deregistered.lock());

        // Heartbeat task should finish.
        handle.await.unwrap();
    }

    #[test]
    fn debug_format() {
        let mock = Arc::new(MockMetaClient::default());
        let mgr = DataNodeManager::new(42, "127.0.0.1:9100", mock, &test_config());

        let debug = format!("{mgr:?}");
        assert!(debug.contains("DataNodeManager"));
        assert!(debug.contains("42"));
    }
}
