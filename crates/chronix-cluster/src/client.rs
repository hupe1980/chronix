//! Abstraction for communicating with the `MetaNode` cluster.
//!
//! Defines the [`MetaClient`] trait and provides [`InProcessMetaClient`] for
//! single-process clusters and testing, and [`GrpcMetaClientAdapter`] for
//! remote gRPC access to `MetaNode` clusters.

use std::sync::Arc;

use async_trait::async_trait;
use chronix_meta::{
    DataNodeInfo, GrpcMetaClient, MetaCommand, MetaRaft, MetaResponse, MetaSmStore, NodeId,
    RegionInfo, RoutingSnapshot,
};

use crate::error::{ClusterError, Result};

/// Async client interface for `MetaNode` operations.
///
/// Implementations communicate with the metadata Raft group to register
/// nodes, send heartbeats, manage regions, and retrieve routing information.
#[async_trait]
pub trait MetaClient: Send + Sync {
    /// Register a data node with the cluster.
    async fn register_node(&self, info: DataNodeInfo) -> Result<()>;

    /// Deregister (remove) a data node from the cluster.
    async fn deregister_node(&self, node_id: NodeId) -> Result<()>;

    /// Send a heartbeat from a data node.
    async fn heartbeat(&self, node_id: NodeId, generation: u64) -> Result<()>;

    /// Create a new data region in the cluster metadata.
    async fn create_region(&self, info: RegionInfo) -> Result<()>;

    /// Retrieve the current routing table snapshot.
    async fn get_routing_table(&self) -> Result<RoutingSnapshot>;

    /// Propose an arbitrary metadata command through Raft consensus.
    async fn propose(&self, cmd: MetaCommand) -> Result<MetaResponse>;
}

/// In-process [`MetaClient`] that talks directly to a co-located Raft instance.
///
/// Useful for single-process embedded clusters and integration tests where
/// the `MetaNode` lives in the same process as the `DataNode`.
pub struct InProcessMetaClient {
    /// Raft handle for proposing writes.
    raft: Arc<MetaRaft>,
    /// State-machine store for read-only queries.
    sm_store: Arc<MetaSmStore>,
}

impl InProcessMetaClient {
    /// Create a new in-process client backed by the given Raft instance.
    #[must_use]
    pub fn new(raft: Arc<MetaRaft>, sm_store: Arc<MetaSmStore>) -> Self {
        Self { raft, sm_store }
    }

    /// Propose a command through Raft and return the response.
    async fn write_cmd(&self, cmd: MetaCommand) -> Result<MetaResponse> {
        let resp = self.raft.client_write(cmd).await.map_err(|e| {
            // Use structured pattern matching instead of
            // parsing the stringified error.
            if let Some(fwd) = e.forward_to_leader() {
                ClusterError::RaftForwardToLeader {
                    leader_id: fwd.leader_id,
                }
            } else {
                ClusterError::Raft(e.to_string())
            }
        })?;
        Ok(resp.data)
    }

    /// Map a [`MetaResponse`] into a [`Result`], treating `Error` as failure.
    fn check_response(resp: MetaResponse) -> Result<()> {
        match resp {
            MetaResponse::Ok | MetaResponse::Created { .. } => Ok(()),
            MetaResponse::Error { message } => Err(ClusterError::Internal(message)),
        }
    }
}

impl std::fmt::Debug for InProcessMetaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcessMetaClient")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl MetaClient for InProcessMetaClient {
    async fn register_node(&self, info: DataNodeInfo) -> Result<()> {
        let resp = self.write_cmd(MetaCommand::RegisterNode(info)).await?;
        Self::check_response(resp)
    }

    async fn deregister_node(&self, node_id: NodeId) -> Result<()> {
        let resp = self
            .write_cmd(MetaCommand::DeregisterNode { node_id })
            .await?;
        Self::check_response(resp)
    }

    async fn heartbeat(&self, node_id: NodeId, generation: u64) -> Result<()> {
        // Update heartbeat state directly without a Raft proposal.
        // This avoids generating N/heartbeat_interval Raft log entries
        // per second for pure liveness checks.
        // The receiver (this node) stamps the timestamp to avoid clock-skew
        // issues between sender and receiver.
        let known = self
            .sm_store
            .state_machine()
            .record_heartbeat(node_id, generation);

        if known {
            Ok(())
        } else {
            Err(ClusterError::Internal(format!(
                "node {node_id} not registered"
            )))
        }
    }

    async fn create_region(&self, info: RegionInfo) -> Result<()> {
        let resp = self.write_cmd(MetaCommand::CreateRegion(info)).await?;
        Self::check_response(resp)
    }

    async fn get_routing_table(&self) -> Result<RoutingSnapshot> {
        Ok(self.sm_store.state_machine().routing_table().snapshot())
    }

    async fn propose(&self, cmd: MetaCommand) -> Result<MetaResponse> {
        self.write_cmd(cmd).await
    }
}

// ── gRPC Remote Adapter ─────────────────────────────────────────────

/// Adapter that wraps [`GrpcMetaClient`] to implement the [`MetaClient`] trait.
///
/// Use this when the `MetaNode` cluster is remote and accessible via gRPC.
/// The underlying client handles address failover and reconnection
/// automatically.
pub struct GrpcMetaClientAdapter {
    inner: GrpcMetaClient,
}

impl GrpcMetaClientAdapter {
    /// Create an adapter wrapping the given [`GrpcMetaClient`].
    #[must_use]
    pub fn new(client: GrpcMetaClient) -> Self {
        Self { inner: client }
    }

    /// Access the underlying [`GrpcMetaClient`] for admin operations
    /// not covered by the [`MetaClient`] trait (e.g., `list_nodes`,
    /// `add_raft_node`, `get_cluster_info`).
    #[must_use]
    pub fn inner(&self) -> &GrpcMetaClient {
        &self.inner
    }
}

impl std::fmt::Debug for GrpcMetaClientAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcMetaClientAdapter")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl MetaClient for GrpcMetaClientAdapter {
    async fn register_node(&self, info: DataNodeInfo) -> Result<()> {
        self.inner
            .register_node(info)
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))
    }

    async fn deregister_node(&self, node_id: NodeId) -> Result<()> {
        self.inner
            .deregister_node(node_id)
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))
    }

    async fn heartbeat(&self, node_id: NodeId, generation: u64) -> Result<()> {
        self.inner
            .heartbeat(node_id, generation)
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))
    }

    async fn create_region(&self, info: RegionInfo) -> Result<()> {
        self.inner
            .create_region(info)
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))
    }

    async fn get_routing_table(&self) -> Result<RoutingSnapshot> {
        self.inner
            .get_routing_table()
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))
    }

    async fn propose(&self, cmd: MetaCommand) -> Result<MetaResponse> {
        self.inner
            .propose(cmd)
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::Duration;

    use chronix_meta::{MetaNetworkFactory, MetaRouter, MetaStore, MetaTypeConfig};
    use openraft::BasicNode;

    /// Build a single-node Raft cluster for testing.
    async fn setup_single_node() -> (InProcessMetaClient, MetaStore) {
        let router = MetaRouter::new();
        let store = MetaStore::new_in_memory();

        let config = Arc::new(
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
            config,
            net,
            store.log_store(),
            store.sm_store(),
        )
        .await
        .expect("raft node creation");

        router.add_node(1, raft.clone());

        // Initialize single-node cluster.
        let mut members = BTreeMap::new();
        members.insert(1, BasicNode::new("127.0.0.1:9001"));
        raft.initialize(members).await.expect("initialize");

        // Wait for leader election.
        for _ in 0..50 {
            if raft.current_leader().await == Some(1) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(
            raft.current_leader().await,
            Some(1),
            "leader must be elected"
        );

        let client = InProcessMetaClient::new(Arc::new(raft), store.sm_store());
        (client, store)
    }

    #[tokio::test]
    async fn register_node_and_verify() {
        let (client, store) = setup_single_node().await;

        let info = DataNodeInfo::new(42, "127.0.0.1:8080");
        client.register_node(info).await.unwrap();

        let node = store.state_machine().get_node(42);
        assert!(node.is_some());
        assert_eq!(node.unwrap().grpc_addr, "127.0.0.1:8080");
    }

    #[tokio::test]
    async fn heartbeat_updates_generation() {
        let (client, store) = setup_single_node().await;

        let info = DataNodeInfo::new(10, "127.0.0.1:8080");
        client.register_node(info).await.unwrap();

        client.heartbeat(10, 5).await.unwrap();

        // Heartbeats bypass Raft and go to the out-of-band heartbeat store.
        let hb_data = store.state_machine().heartbeat_data();
        let (gen, _ts) = hb_data.get(&10).expect("heartbeat should be recorded");
        assert_eq!(*gen, 5);
    }

    #[tokio::test]
    async fn deregister_removes_node() {
        let (client, store) = setup_single_node().await;

        let info = DataNodeInfo::new(99, "127.0.0.1:8080");
        client.register_node(info).await.unwrap();
        assert!(store.state_machine().get_node(99).is_some());

        client.deregister_node(99).await.unwrap();
        assert!(store.state_machine().get_node(99).is_none());
    }

    #[tokio::test]
    async fn create_region_and_get_routing_table() {
        let (client, store) = setup_single_node().await;

        // Register the node first (needed for routing table rebuild).
        let info = DataNodeInfo::new(1, "127.0.0.1:9001");
        client.register_node(info).await.unwrap();

        let region = RegionInfo::new(1, "cpu", 1, vec![1]);
        client.create_region(region).await.unwrap();

        assert!(store.state_machine().get_region(1).is_some());

        let snapshot = client.get_routing_table().await.unwrap();
        assert!(!snapshot.entries.is_empty());
    }

    #[tokio::test]
    async fn propose_arbitrary_command() {
        let (client, _store) = setup_single_node().await;

        let resp = client
            .propose(MetaCommand::RegisterNode(DataNodeInfo::new(
                77,
                "127.0.0.1:7777",
            )))
            .await
            .unwrap();

        assert!(matches!(
            resp,
            MetaResponse::Ok | MetaResponse::Created { .. }
        ));
    }

    #[tokio::test]
    async fn debug_impl() {
        let (client, _store) = setup_single_node().await;
        let debug = format!("{client:?}");
        assert!(debug.contains("InProcessMetaClient"));
    }
}
