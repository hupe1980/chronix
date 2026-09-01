//! In-process Raft network transport.
//!
//! Provides an in-process, channel-free transport that routes Raft RPCs
//! directly to target [`MetaRaft`] instances. Ideal for testing and
//! embedded single-process clusters. Production deployments should use
//! a gRPC-based transport (provided by `chronix-cluster`).

use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::error::{NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::storage::Snapshot;
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory, Vote};
use parking_lot::RwLock;

use crate::store::{MetaRaft, MetaTypeConfig};

type NodeId = u64;

/// Simple newtype error for network failures.
///
/// Wraps a string message and implements [`std::error::Error`] so it
/// can be passed to [`NetworkError::new`].
#[derive(Debug)]
pub(crate) struct NetErr(pub(crate) String);

impl std::fmt::Display for NetErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for NetErr {}

/// Shared router that maps node IDs to their Raft instances.
///
/// All nodes in the cluster register here so that the network
/// can route RPCs to the correct target.
#[derive(Clone, Default)]
pub struct MetaRouter {
    routes: Arc<RwLock<BTreeMap<NodeId, MetaRaft>>>,
}

impl MetaRouter {
    /// Create a new, empty router.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a Raft instance for the given node.
    pub fn add_node(&self, id: NodeId, raft: MetaRaft) {
        self.routes.write().insert(id, raft);
    }

    /// Remove a node from the router.
    pub fn remove_node(&self, id: NodeId) {
        self.routes.write().remove(&id);
    }

    /// Look up a Raft instance by node ID.
    fn get(&self, id: NodeId) -> Option<MetaRaft> {
        self.routes.read().get(&id).cloned()
    }

    /// Number of registered nodes.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.routes.read().len()
    }
}

impl std::fmt::Debug for MetaRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaRouter")
            .field("nodes", &self.node_count())
            .finish()
    }
}

/// Factory that creates [`MetaNetwork`] instances for each target node.
///
/// Holds a shared [`MetaRouter`] so all created connections can reach
/// any registered node.
#[derive(Clone, Debug)]
pub struct MetaNetworkFactory {
    router: MetaRouter,
}

impl MetaNetworkFactory {
    /// Create a factory backed by the given router.
    #[must_use]
    pub fn new(router: MetaRouter) -> Self {
        Self { router }
    }

    /// Access the underlying router (e.g. to add nodes).
    #[must_use]
    pub fn router(&self) -> &MetaRouter {
        &self.router
    }
}

impl RaftNetworkFactory<MetaTypeConfig> for MetaNetworkFactory {
    type Network = MetaNetwork;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        MetaNetwork {
            target,
            router: self.router.clone(),
        }
    }
}

/// In-process Raft network connection to a single target node.
///
/// Routes RPCs by looking up the target's [`MetaRaft`] instance in the
/// shared router and calling its server-side handler directly.
#[derive(Debug)]
pub struct MetaNetwork {
    target: NodeId,
    router: MetaRouter,
}

impl MetaNetwork {
    /// Get the target Raft instance, or return an `Unreachable` error.
    fn target_raft(
        &self,
    ) -> Result<MetaRaft, Box<RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>>>
    {
        self.router.get(self.target).ok_or_else(|| {
            let err = NetErr(format!("node {} not found in router", self.target));
            Box::new(RPCError::Unreachable(Unreachable::new(&NetworkError::new(
                &err,
            ))))
        })
    }
}

impl RaftNetwork<MetaTypeConfig> for MetaNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<MetaTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        let raft = self.target_raft().map_err(|e| *e)?;
        raft.append_entries(rpc)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>>
    {
        let raft = self.target_raft().map_err(|e| *e)?;
        raft.vote(rpc)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<NodeId>,
        snapshot: Snapshot<MetaTypeConfig>,
        _cancel: impl futures::Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<
        SnapshotResponse<NodeId>,
        StreamingError<MetaTypeConfig, openraft::error::Fatal<NodeId>>,
    > {
        let raft = self.router.get(self.target).ok_or_else(|| {
            let err = NetErr(format!("node {} not found in router", self.target));
            StreamingError::Unreachable(Unreachable::new(&NetworkError::new(&err)))
        })?;

        raft.install_full_snapshot(vote, snapshot)
            .await
            .map_err(|fatal| match fatal {
                openraft::error::Fatal::StorageError(se) => StreamingError::StorageError(se),
                other => {
                    let err = NetErr(format!("raft fatal: {other}"));
                    StreamingError::Unreachable(Unreachable::new(&NetworkError::new(&err)))
                }
            })
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<MetaTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<
            NodeId,
            BasicNode,
            openraft::error::RaftError<NodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        let raft = self.router.get(self.target).ok_or_else(|| {
            let err = NetErr(format!("node {} not found in router", self.target));
            RPCError::Unreachable(Unreachable::new(&NetworkError::new(&err)))
        })?;

        raft.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_add_remove() {
        let router = MetaRouter::new();
        assert_eq!(router.node_count(), 0);
        // We can't easily create a MetaRaft without full setup,
        // but we can test the basic structure
        assert!(router.get(1).is_none());
    }

    #[test]
    fn factory_creation() {
        let router = MetaRouter::new();
        let factory = MetaNetworkFactory::new(router.clone());
        assert_eq!(factory.router().node_count(), 0);
    }

    #[tokio::test]
    async fn network_target_not_found() {
        let router = MetaRouter::new();
        let net = MetaNetwork { target: 1, router };
        let result = net.target_raft();
        assert!(result.is_err());
    }
}
