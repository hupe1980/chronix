//! Per-region Raft group lifecycle management.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::Semaphore;
use tracing::{debug, warn};

use chronix_core::Point;
use chronix_meta::DurableLogStore;

use crate::data_service::RegionStorage;
use crate::error::{ClusterError, Result};

use super::grpc_transport::RegionGrpcNetworkFactory;
use super::log_store::RegionLogStore;
use super::network::{RegionRaftNetworkFactory, RegionRaftRouter};
use super::state_machine::RegionSmStore;
use super::{NodeId, RegionId, RegionRaft, RegionWriteCommand};

// ── Region Raft Manager ─────────────────────────────────────────────

/// Default maximum number of concurrent in-flight Raft proposals.
const DEFAULT_MAX_INFLIGHT_PROPOSALS: usize = 256;

/// Manages all per-region Raft groups on a single `DataNode`.
///
/// Provides high-level operations for creating Raft groups and
/// proposing quorum-replicated writes.
pub struct RegionRaftManager {
    groups: DashMap<RegionId, RegionRaft>,
    router: RegionRaftRouter,
    /// This node's ID (retained for future use; currently unused after
    /// removing the racy stale-leader pre-check).
    _local_node_id: NodeId,
    /// Optional data directory for durable Raft log storage.
    /// When set, `create_raft_group` uses redb; when None, in-memory.
    data_dir: Option<PathBuf>,
    /// Semaphore that caps the number of concurrent in-flight proposals
    /// across all region Raft groups on this node (backpressure).
    proposal_semaphore: Arc<Semaphore>,
}

impl RegionRaftManager {
    /// Create a new manager backed by the given router (in-memory storage).
    ///
    /// Suitable for tests only. Production code should use
    /// [`with_data_dir`](Self::with_data_dir) for durable Raft logs.
    #[must_use]
    pub fn new_in_memory(router: RegionRaftRouter, local_node_id: NodeId) -> Self {
        Self {
            groups: DashMap::new(),
            router,
            _local_node_id: local_node_id,
            data_dir: None,
            proposal_semaphore: Arc::new(Semaphore::new(DEFAULT_MAX_INFLIGHT_PROPOSALS)),
        }
    }

    /// Create a new manager with durable Raft log storage.
    ///
    /// Region Raft logs are stored in `<data_dir>/region_<id>_raft.redb`.
    #[must_use]
    pub fn with_data_dir(
        router: RegionRaftRouter,
        local_node_id: NodeId,
        data_dir: impl AsRef<Path>,
    ) -> Self {
        Self {
            groups: DashMap::new(),
            router,
            _local_node_id: local_node_id,
            data_dir: Some(data_dir.as_ref().to_path_buf()),
            proposal_semaphore: Arc::new(Semaphore::new(DEFAULT_MAX_INFLIGHT_PROPOSALS)),
        }
    }

    /// Create a log store — durable if `data_dir` is set, in-memory otherwise.
    fn make_log_store(&self, region_id: RegionId) -> Result<super::RegionLogStoreKind> {
        match &self.data_dir {
            Some(dir) => {
                let db_path = dir.join(format!("region_{region_id}_raft.redb"));
                let store = DurableLogStore::open(&db_path).map_err(|e| {
                    ClusterError::Raft(format!(
                        "open durable log store for region {region_id}: {e}"
                    ))
                })?;
                Ok(super::RegionLogStoreKind::Durable(store))
            }
            None => Ok(super::RegionLogStoreKind::Memory(RegionLogStore::default())),
        }
    }

    /// Create a new Raft group for a region on this node.
    ///
    /// The Raft instance is stored locally and registered in the
    /// router so that other nodes in the same cluster can reach it.
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance cannot be created.
    pub async fn create_raft_group(
        &self,
        region_id: RegionId,
        node_id: NodeId,
        storage: Arc<dyn RegionStorage>,
        config: Arc<openraft::Config>,
    ) -> Result<RegionRaft> {
        let log_store = self.make_log_store(region_id)?;
        let sm_store = Arc::new(RegionSmStore::new(region_id, storage));
        let network = RegionRaftNetworkFactory::new(self.router.clone(), region_id);

        let raft = openraft::Raft::new(node_id, config, network, log_store, sm_store)
            .await
            .map_err(|e| ClusterError::Raft(format!("region raft init: {e}")))?;

        self.router.add_node(region_id, node_id, raft.clone());
        self.groups.insert(region_id, raft.clone());

        let mode = if self.data_dir.is_some() {
            "durable"
        } else {
            "in-memory"
        };
        debug!(region_id, node_id, mode, "Created region Raft group");
        Ok(raft)
    }

    /// Create a new Raft group for a region using **gRPC transport**.
    ///
    /// Unlike [`create_raft_group`](Self::create_raft_group) which uses
    /// the in-process router, this variant creates a Raft instance
    /// backed by gRPC connections so the group can span multiple
    /// `DataNode` processes.
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance cannot be created.
    pub async fn create_raft_group_grpc(
        &self,
        region_id: RegionId,
        node_id: NodeId,
        storage: Arc<dyn RegionStorage>,
        config: Arc<openraft::Config>,
        network: RegionGrpcNetworkFactory,
    ) -> Result<RegionRaft> {
        let log_store = self.make_log_store(region_id)?;
        let sm_store = Arc::new(RegionSmStore::new(region_id, storage));

        let raft = openraft::Raft::new(node_id, config, network, log_store, sm_store)
            .await
            .map_err(|e| ClusterError::Raft(format!("region raft init (grpc): {e}")))?;

        self.groups.insert(region_id, raft.clone());

        let mode = if self.data_dir.is_some() {
            "durable"
        } else {
            "in-memory"
        };
        debug!(region_id, node_id, mode, "Created region Raft group (gRPC)");
        Ok(raft)
    }

    /// Propose a quorum-replicated write to a region.
    ///
    /// The write is serialized into a Raft log entry, replicated to a
    /// majority of the region's Raft group, and applied to each node's
    /// [`RegionStorage`] upon commit.
    ///
    /// # Backpressure
    ///
    /// A `tokio::sync::Semaphore` caps the number of concurrent in-flight
    /// proposals across all region groups on this node. When the limit is
    /// reached, callers wait until a permit becomes available, which
    /// naturally throttles upstream writers.
    ///
    /// # Errors
    ///
    /// Returns an error if the region has no local Raft group, this
    /// node is not the leader, or quorum cannot be reached.
    pub async fn propose_write(&self, region_id: RegionId, points: Vec<Point>) -> Result<u64> {
        self.propose_write_with_id(region_id, points, None).await
    }

    /// Propose a replicated write with an explicit request ID for
    /// cluster-wide deduplication.
    pub async fn propose_write_with_id(
        &self,
        region_id: RegionId,
        points: Vec<Point>,
        request_id: Option<String>,
    ) -> Result<u64> {
        let raft = self
            .groups
            .get(&region_id)
            .ok_or(ClusterError::RegionNotFound(region_id))?
            .clone();

        // Acquire a backpressure permit before proposing.
        let _permit = self
            .proposal_semaphore
            .acquire()
            .await
            .map_err(|_| ClusterError::Internal("proposal semaphore closed".into()))?;

        // Leadership is validated atomically by OpenRaft inside
        // `client_write()`.  A pre-check via `metrics()` is racy
        // (TOCTOU) and was removed.

        let cmd = RegionWriteCommand::WritePoints {
            region_id,
            points,
            request_id,
        };

        let resp = raft.client_write(cmd).await.map_err(|e| {
            warn!(region_id, error = %e, "Region Raft write failed");
            ClusterError::ReplicationFailed(format!("region {region_id}: {e}"))
        })?;

        Ok(resp.data.written)
    }

    /// Get the Raft handle for a region, if it exists on this node.
    #[must_use]
    pub fn get_raft(&self, region_id: RegionId) -> Option<RegionRaft> {
        self.groups.get(&region_id).map(|r| r.clone())
    }

    /// Remove a region's Raft group from this node.
    ///
    /// Also removes all router entries for this region so that no RPCs
    /// can be routed to the now-dead Raft instance.
    #[must_use]
    pub fn remove_group(&self, region_id: RegionId) -> Option<RegionRaft> {
        let removed = self.groups.remove(&region_id).map(|(_, r)| r);
        if removed.is_some() {
            self.router.remove_region(region_id);
        }
        removed
    }

    /// Number of active region Raft groups on this node.
    #[must_use]
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Add a learner replica to a region's Raft group.
    ///
    /// The learner will receive log entries from the leader but does
    /// not vote in elections.  Once the learner has caught up, call
    /// [`promote_learner`](Self::promote_learner) to make it a full
    /// voter and update the membership.
    ///
    /// # Errors
    ///
    /// Returns an error if the region has no local Raft group or if
    /// adding the learner fails.
    pub async fn add_learner(
        &self,
        region_id: RegionId,
        learner_node_id: NodeId,
        learner_addr: impl Into<String>,
    ) -> Result<()> {
        let raft = self
            .groups
            .get(&region_id)
            .ok_or(ClusterError::RegionNotFound(region_id))?
            .clone();

        let node = openraft::BasicNode {
            addr: learner_addr.into(),
        };

        raft.add_learner(learner_node_id, node, true)
            .await
            .map_err(|e| {
                ClusterError::Raft(format!(
                    "add_learner for region {region_id}, node {learner_node_id}: {e}"
                ))
            })?;

        debug!(
            region_id,
            learner_node_id, "added learner to region Raft group"
        );
        Ok(())
    }

    /// Promote a learner to voter and update the membership set.
    ///
    /// `voter_ids` should include *all* desired voters (existing voters
    /// plus the newly promoted learner).  This triggers a joint-consensus
    /// membership change in the Raft group.
    ///
    /// # Errors
    ///
    /// Returns an error if the region has no local Raft group or the
    /// membership change fails.
    pub async fn promote_learner(
        &self,
        region_id: RegionId,
        voter_ids: std::collections::BTreeSet<NodeId>,
    ) -> Result<()> {
        let raft = self
            .groups
            .get(&region_id)
            .ok_or(ClusterError::RegionNotFound(region_id))?
            .clone();

        raft.change_membership(voter_ids.clone(), false)
            .await
            .map_err(|e| {
                ClusterError::Raft(format!("change_membership for region {region_id}: {e}"))
            })?;

        debug!(
            region_id,
            voters = ?voter_ids,
            "updated region Raft membership"
        );
        Ok(())
    }

    /// Access the underlying router.
    #[must_use]
    pub fn router(&self) -> &RegionRaftRouter {
        &self.router
    }
}

impl std::fmt::Debug for RegionRaftManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionRaftManager")
            .field("groups", &self.groups.len())
            .finish_non_exhaustive()
    }
}
