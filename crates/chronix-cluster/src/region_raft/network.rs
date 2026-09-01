//! In-memory Raft network and router for region Raft groups.

use std::sync::Arc;

use openraft::error::{NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::storage::Snapshot;
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory, Vote};

use super::{NodeId, RegionId, RegionRaft, RegionTypeConfig};

// ── In-Memory Router ────────────────────────────────────────────────

/// Simple newtype error for network failures.
#[derive(Debug)]
struct NetErr(String);

impl std::fmt::Display for NetErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for NetErr {}

/// Shared router that maps `(region_id, node_id)` → Raft instance.
///
/// All nodes register their per-region Raft handles here so the
/// in-process network can route RPCs to the correct target.
#[derive(Clone, Default)]
pub struct RegionRaftRouter {
    routes: Arc<parking_lot::RwLock<std::collections::BTreeMap<(RegionId, NodeId), RegionRaft>>>,
}

impl RegionRaftRouter {
    /// Create a new, empty router.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a Raft instance for the given region and node.
    pub fn add_node(&self, region_id: RegionId, node_id: NodeId, raft: RegionRaft) {
        self.routes.write().insert((region_id, node_id), raft);
    }

    /// Remove a node for a specific region from the router.
    pub fn remove_node(&self, region_id: RegionId, node_id: NodeId) {
        self.routes.write().remove(&(region_id, node_id));
    }

    /// Remove **all** entries for a region from the router.
    ///
    /// Called during group teardown to prevent stale routing to a dead
    /// Raft instance.
    pub fn remove_region(&self, region_id: RegionId) {
        self.routes.write().retain(|&(rid, _), _| rid != region_id);
    }

    /// Look up a Raft instance by region and node ID.
    pub(crate) fn get(&self, region_id: RegionId, node_id: NodeId) -> Option<RegionRaft> {
        self.routes.read().get(&(region_id, node_id)).cloned()
    }

    /// Number of registered (region, node) pairs.
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.routes.read().len()
    }
}

impl std::fmt::Debug for RegionRaftRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionRaftRouter")
            .field("entries", &self.entry_count())
            .finish()
    }
}

// ── In-Memory Network ───────────────────────────────────────────────

/// Factory that creates [`RegionRaftNetwork`] instances for a specific
/// Raft group (region). Each created network targets a single peer node.
#[derive(Clone, Debug)]
pub struct RegionRaftNetworkFactory {
    router: RegionRaftRouter,
    region_id: RegionId,
}

impl RegionRaftNetworkFactory {
    /// Create a factory for the given region.
    #[must_use]
    pub fn new(router: RegionRaftRouter, region_id: RegionId) -> Self {
        Self { router, region_id }
    }
}

impl RaftNetworkFactory<RegionTypeConfig> for RegionRaftNetworkFactory {
    type Network = RegionRaftNetwork;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        RegionRaftNetwork {
            target,
            router: self.router.clone(),
            region_id: self.region_id,
        }
    }
}

/// In-process Raft network connection to a single target node within
/// a region's Raft group. Routes RPCs by looking up the target's
/// [`RegionRaft`] handle in the shared router.
#[derive(Debug)]
pub struct RegionRaftNetwork {
    target: NodeId,
    router: RegionRaftRouter,
    region_id: RegionId,
}

impl RegionRaftNetwork {
    /// Get the target Raft instance, or return an `Unreachable` error.
    fn target_raft(
        &self,
    ) -> std::result::Result<
        RegionRaft,
        Box<RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>>,
    > {
        self.router.get(self.region_id, self.target).ok_or_else(|| {
            let err = NetErr(format!(
                "region {} node {} not found in router",
                self.region_id, self.target
            ));
            Box::new(RPCError::Unreachable(Unreachable::new(&NetworkError::new(
                &err,
            ))))
        })
    }
}

impl RaftNetwork<RegionTypeConfig> for RegionRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<RegionTypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<
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
    ) -> std::result::Result<
        VoteResponse<NodeId>,
        RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        let raft = self.target_raft().map_err(|e| *e)?;
        raft.vote(rpc)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<NodeId>,
        snapshot: Snapshot<RegionTypeConfig>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> std::result::Result<
        SnapshotResponse<NodeId>,
        StreamingError<RegionTypeConfig, openraft::error::Fatal<NodeId>>,
    > {
        let raft = self
            .router
            .get(self.region_id, self.target)
            .ok_or_else(|| {
                let err = NetErr(format!(
                    "region {} node {} not found in router",
                    self.region_id, self.target
                ));
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
        rpc: InstallSnapshotRequest<RegionTypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<
            NodeId,
            BasicNode,
            openraft::error::RaftError<NodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        let raft = self
            .router
            .get(self.region_id, self.target)
            .ok_or_else(|| {
                let err = NetErr(format!(
                    "region {} node {} not found in router",
                    self.region_id, self.target
                ));
                RPCError::Unreachable(Unreachable::new(&NetworkError::new(&err)))
            })?;

        raft.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }
}
