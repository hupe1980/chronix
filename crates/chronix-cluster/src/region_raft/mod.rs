//! Multi-Raft per-region replication.
//!
//! Each data region gets its own Raft group so that writes are quorum-
//! replicated across data nodes. A [`RegionRaftManager`] holds all
//! region Raft groups on a single node and provides the high-level
//! [`propose_write`](RegionRaftManager::propose_write) entry point.
//!
//! **Architecture:**
//!
//! ```text
//! client write ─▶ WriteRouter ─▶ RegionRaftManager::propose_write(region_id, points)
//!                                   │
//!                                   ▼
//!                         Raft<RegionTypeConfig>::client_write(RegionWriteCommand)
//!                                   │
//!                          quorum replication
//!                                   │
//!                                   ▼
//!                    RegionSmStore::apply() on each node
//!                           writes to RegionStorage
//! ```
//!
//! # Module organisation
//!
//! | Sub-module          | Contents                                      |
//! |---------------------|-----------------------------------------------|
//! | `log_store`       | In-memory Raft log storage                    |
//! | `state_machine`   | State-machine store (applies writes)          |
//! | `network`         | In-process router & Raft network transport    |
//! | `manager`         | Raft group lifecycle & write proposals        |

/// gRPC-based cross-node Region Raft transport.
pub mod grpc_transport;
mod log_store;
mod manager;
mod network;
mod state_machine;

use std::fmt::Debug;
use std::ops::RangeBounds;

use chronix_core::Point;
use chronix_meta::DurableLogStore;
use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{BasicNode, Entry, LogId, RaftLogReader, StorageError};
use serde::{Deserialize, Serialize};
use std::io::Cursor;

pub use grpc_transport::{
    DataNodeAddressMap, RegionGrpcNetwork, RegionGrpcNetworkFactory, RegionRaftGrpcServer,
};
pub use log_store::RegionLogStore;
pub use manager::RegionRaftManager;
pub use network::{RegionRaftNetworkFactory, RegionRaftRouter};
pub use state_machine::RegionSmStore;

type NodeId = u64;
type RegionId = u64;

// ── Type Configuration ──────────────────────────────────────────────

openraft::declare_raft_types!(
    /// OpenRaft type configuration for per-region data Raft groups.
    pub RegionTypeConfig:
        D = RegionWriteCommand,
        R = RegionWriteResponse,
        NodeId = u64,
        Node = BasicNode,
);

/// Convenience alias for a region Raft instance.
pub type RegionRaft = openraft::Raft<RegionTypeConfig>;

/// Convenience alias for a region Raft log entry.
pub type RegionEntry = Entry<RegionTypeConfig>;

// ── Commands & Responses ────────────────────────────────────────────

/// Raft-replicated write command for a data region.
///
/// Serialized into the Raft log and applied by [`RegionSmStore`] on
/// every node in the region's Raft group.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RegionWriteCommand {
    /// Write a batch of points to the region.
    WritePoints {
        /// Target region.
        region_id: u64,
        /// Points to write — serialized via serde as part of the Raft log.
        points: Vec<Point>,
        /// Client request ID for cluster-wide deduplication.
        /// Persisted in the Raft log so that every replica can detect
        /// duplicate writes after leader failover.
        request_id: Option<String>,
    },
}

/// Response returned after a region write is committed.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RegionWriteResponse {
    /// Number of points written.
    pub written: u64,
}

// ── Region Log Store Kind ───────────────────────────────────────────

/// Region Raft log store — in-memory for tests, durable for production.
#[derive(Clone, Debug)]
pub enum RegionLogStoreKind {
    /// In-memory store — fast, suitable for tests.
    Memory(RegionLogStore),
    /// Durable store backed by redb — crash-safe, for production.
    Durable(DurableLogStore<RegionTypeConfig>),
}

impl RaftLogReader<RegionTypeConfig> for RegionLogStoreKind {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<RegionEntry>, StorageError<u64>> {
        match self {
            Self::Memory(s) => s.try_get_log_entries(range).await,
            Self::Durable(s) => s.try_get_log_entries(range).await,
        }
    }
}

impl RaftLogStorage<RegionTypeConfig> for RegionLogStoreKind {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<RegionTypeConfig>, StorageError<u64>> {
        match self {
            Self::Memory(s) => s.get_log_state().await,
            Self::Durable(s) => s.get_log_state().await,
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &openraft::Vote<u64>) -> Result<(), StorageError<u64>> {
        match self {
            Self::Memory(s) => s.save_vote(vote).await,
            Self::Durable(s) => s.save_vote(vote).await,
        }
    }

    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<u64>>, StorageError<u64>> {
        match self {
            Self::Memory(s) => s.read_vote().await,
            Self::Durable(s) => s.read_vote().await,
        }
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<RegionTypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = RegionEntry> + Send,
        I::IntoIter: Send,
    {
        match self {
            Self::Memory(s) => s.append(entries, callback).await,
            Self::Durable(s) => s.append(entries, callback).await,
        }
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        match self {
            Self::Memory(s) => s.truncate(log_id).await,
            Self::Durable(s) => s.truncate(log_id).await,
        }
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        match self {
            Self::Memory(s) => s.purge(log_id).await,
            Self::Durable(s) => s.purge(log_id).await,
        }
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        match self {
            Self::Memory(s) => s.save_committed(committed).await,
            Self::Durable(s) => s.save_committed(committed).await,
        }
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        match self {
            Self::Memory(s) => s.read_committed().await,
            Self::Durable(s) => s.read_committed().await,
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_service::RegionStorage;
    use crate::error::ClusterError;
    use openraft::entry::RaftEntry;
    use openraft::storage::{RaftLogStorage, RaftStateMachine};
    use openraft::{CommittedLeaderId, EntryPayload, LogId, RaftLogReader, RaftSnapshotBuilder};
    use std::collections::{BTreeMap, BTreeSet};
    use std::time::Duration;

    // ── Test storage — uses shared write-log mock ──────────────────
    use crate::test_util::MockWriteStorage;
    type MockRegionStorage = MockWriteStorage;

    // ── Helpers ────────────────────────────────────────────────────

    fn log_id(term: u64, index: u64) -> LogId<u64> {
        LogId::new(CommittedLeaderId::new(term, 0), index)
    }

    fn blank_entry(lid: LogId<u64>) -> RegionEntry {
        RegionEntry::new_blank(lid)
    }

    fn normal_entry(lid: LogId<u64>, cmd: RegionWriteCommand) -> RegionEntry {
        Entry {
            log_id: lid,
            payload: EntryPayload::Normal(cmd),
        }
    }

    fn make_test_config() -> std::sync::Arc<openraft::Config> {
        std::sync::Arc::new(openraft::Config {
            heartbeat_interval: 20,
            election_timeout_min: 50,
            election_timeout_max: 100,
            ..Default::default()
        })
    }

    fn make_test_point(measurement: &str, ts: i64) -> Point {
        use chronix_core::{FieldValue, SeriesKey};
        let sk = SeriesKey::new(
            measurement,
            [("host".to_string(), "srv1".to_string())]
                .into_iter()
                .collect(),
        )
        .unwrap();
        let fields = [("value".to_string(), FieldValue::F64(42.0))]
            .into_iter()
            .collect();
        Point::new(sk, fields, ts).unwrap()
    }

    /// Build a single-node Raft cluster for a region and return the
    /// Raft handle and storage.
    async fn build_single_node(
        region_id: u64,
        node_id: u64,
        router: &RegionRaftRouter,
        config: &std::sync::Arc<openraft::Config>,
    ) -> (RegionRaft, std::sync::Arc<MockRegionStorage>) {
        let storage = std::sync::Arc::new(MockRegionStorage::default());
        let log_store = RegionLogStore::default();
        let sm_store = std::sync::Arc::new(RegionSmStore::new(region_id, storage.clone()));
        let network = RegionRaftNetworkFactory::new(router.clone(), region_id);

        let raft = openraft::Raft::new(node_id, config.clone(), network, log_store, sm_store)
            .await
            .unwrap();

        router.add_node(region_id, node_id, raft.clone());
        (raft, storage)
    }

    /// Build a 3-node Raft cluster for a region. Returns the Raft
    /// handles and storage instances for all three nodes.
    async fn build_three_node_cluster(
        region_id: u64,
        router: &RegionRaftRouter,
        config: &std::sync::Arc<openraft::Config>,
    ) -> [(RegionRaft, std::sync::Arc<MockRegionStorage>); 3] {
        let (raft1, s1) = build_single_node(region_id, 1, router, config).await;
        let (raft2, s2) = build_single_node(region_id, 2, router, config).await;
        let (raft3, s3) = build_single_node(region_id, 3, router, config).await;

        // Initialize cluster: node 1 bootstraps
        let mut members = BTreeMap::new();
        members.insert(1u64, BasicNode::new("127.0.0.1:10001"));
        raft1.initialize(members).await.unwrap();

        // Wait for leader
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Add nodes 2 and 3 as learners, then promote to voters
        raft1
            .add_learner(2, BasicNode::new("127.0.0.1:10002"), true)
            .await
            .unwrap();
        raft1
            .add_learner(3, BasicNode::new("127.0.0.1:10003"), true)
            .await
            .unwrap();

        let mut voters: BTreeSet<u64> = BTreeSet::new();
        voters.insert(1);
        voters.insert(2);
        voters.insert(3);
        raft1.change_membership(voters, false).await.unwrap();

        // Allow membership to propagate
        tokio::time::sleep(Duration::from_millis(80)).await;

        [(raft1, s1), (raft2, s2), (raft3, s3)]
    }

    // ── Log store tests ────────────────────────────────────────────

    #[tokio::test]
    async fn log_store_append_and_read() {
        let mut store = RegionLogStore::default();
        store.test_append(vec![blank_entry(log_id(1, 1))]).await;

        let entries = store.try_get_log_entries(1..2).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id.index, 1);
    }

    #[tokio::test]
    async fn log_store_vote_roundtrip() {
        let mut store = RegionLogStore::default();
        assert!(store.read_vote().await.unwrap().is_none());

        let vote = openraft::Vote::new(1, 1);
        store.save_vote(&vote).await.unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(vote));
    }

    #[tokio::test]
    async fn log_store_truncate() {
        let mut store = RegionLogStore::default();
        let entries: Vec<_> = (1..=5).map(|i| blank_entry(log_id(1, i))).collect();
        store.test_append(entries).await;

        store.truncate(log_id(1, 3)).await.unwrap();
        let entries = store.try_get_log_entries(1..=5).await.unwrap();
        assert_eq!(entries.len(), 2); // entries 1, 2 remain
    }

    #[tokio::test]
    async fn log_store_purge() {
        let mut store = RegionLogStore::default();
        let entries: Vec<_> = (1..=5).map(|i| blank_entry(log_id(1, i))).collect();
        store.test_append(entries).await;

        store.purge(log_id(1, 3)).await.unwrap();
        let entries = store.try_get_log_entries(1..=5).await.unwrap();
        assert_eq!(entries.len(), 2); // entries 4, 5 remain
    }

    #[tokio::test]
    async fn log_store_state() {
        let mut store = RegionLogStore::default();

        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());

        store
            .test_append(vec![blank_entry(log_id(1, 1)), blank_entry(log_id(1, 2))])
            .await;
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 2);
    }

    // ── State machine tests ────────────────────────────────────────

    #[tokio::test]
    async fn sm_store_apply_write_points() {
        let storage = std::sync::Arc::new(MockRegionStorage::default());
        let mut sm: std::sync::Arc<RegionSmStore> =
            std::sync::Arc::new(RegionSmStore::new(1, storage.clone()));

        let cmd = RegionWriteCommand::WritePoints {
            region_id: 1,
            request_id: None,
            points: vec![make_test_point("cpu", 1000)],
        };
        let entry = normal_entry(log_id(1, 1), cmd);

        let responses = sm.apply([entry]).await.unwrap();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].written, 1);
        assert_eq!(storage.total_points_written(), 1);
    }

    #[tokio::test]
    async fn sm_store_apply_blank() {
        let storage = std::sync::Arc::new(MockRegionStorage::default());
        let mut sm: std::sync::Arc<RegionSmStore> =
            std::sync::Arc::new(RegionSmStore::new(1, storage));

        let entry = blank_entry(log_id(1, 1));
        let responses = sm.apply([entry]).await.unwrap();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].written, 0);
    }

    #[tokio::test]
    async fn sm_store_applied_state() {
        let storage = std::sync::Arc::new(MockRegionStorage::default());
        let mut sm: std::sync::Arc<RegionSmStore> =
            std::sync::Arc::new(RegionSmStore::new(1, storage));

        let (last, _mem) = sm.applied_state().await.unwrap();
        assert!(last.is_none());

        sm.apply([blank_entry(log_id(1, 5))]).await.unwrap();

        let (last, _mem) = sm.applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 5);
    }

    #[tokio::test]
    async fn sm_store_snapshot_roundtrip() {
        let storage = std::sync::Arc::new(MockRegionStorage::default());
        let mut sm: std::sync::Arc<RegionSmStore> =
            std::sync::Arc::new(RegionSmStore::new(1, storage));

        sm.apply([blank_entry(log_id(1, 1))]).await.unwrap();

        let snapshot = sm.build_snapshot().await.unwrap();
        assert_eq!(snapshot.meta.last_log_id.unwrap().index, 1);

        let current = sm.get_current_snapshot().await.unwrap();
        assert!(current.is_some());
    }

    // ── Router tests ───────────────────────────────────────────────

    #[test]
    fn router_add_remove() {
        let router = RegionRaftRouter::new();
        assert_eq!(router.entry_count(), 0);
        assert!(router.get(1, 1).is_none());
    }

    #[tokio::test]
    async fn router_remove_region_cleans_all_nodes() {
        let router = RegionRaftRouter::new();
        let config = make_test_config();

        // Register two nodes for region 1 and one for region 2.
        let (raft_r1_n1, _) = build_single_node(1, 1, &router, &config).await;
        router.add_node(1, 2, raft_r1_n1.clone());
        let (_raft_r2_n1, _) = build_single_node(2, 1, &router, &config).await;

        assert_eq!(router.entry_count(), 3);

        // Remove all entries for region 1.
        router.remove_region(1);
        assert_eq!(
            router.entry_count(),
            1,
            "remove_region should remove all nodes for the given region"
        );
        assert!(router.get(1, 1).is_none());
        assert!(router.get(1, 2).is_none());
        assert!(router.get(2, 1).is_some()); // region 2 unaffected
    }

    // ── Single-node Raft tests ─────────────────────────────────────

    #[tokio::test]
    async fn single_node_raft_write() {
        let router = RegionRaftRouter::new();
        let config = make_test_config();
        let region_id = 1;

        let (raft, storage) = build_single_node(region_id, 1, &router, &config).await;

        // Initialize as single-node cluster
        let mut members = BTreeMap::new();
        members.insert(1u64, BasicNode::new("127.0.0.1:10001"));
        raft.initialize(members).await.unwrap();

        // Wait for leader election
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Propose a write
        let cmd = RegionWriteCommand::WritePoints {
            region_id,
            request_id: None,
            points: vec![make_test_point("cpu", 1000)],
        };
        let resp = raft.client_write(cmd).await.unwrap();
        assert_eq!(resp.data.written, 1);

        // Verify storage received the write
        assert_eq!(storage.total_points_written(), 1);
        let writes = storage.written_points();
        assert_eq!(writes[0].0, region_id);
        assert_eq!(writes[0].1.len(), 1);
    }

    // ── Three-node Raft tests ──────────────────────────────────────

    #[tokio::test]
    async fn three_node_replication() {
        let router = RegionRaftRouter::new();
        let config = make_test_config();
        let region_id = 10;

        let [(raft1, s1), (_raft2, s2), (_raft3, s3)] =
            build_three_node_cluster(region_id, &router, &config).await;

        // Write through leader (node 1)
        let cmd = RegionWriteCommand::WritePoints {
            region_id,
            request_id: None,
            points: vec![make_test_point("cpu", 1000), make_test_point("cpu", 2000)],
        };
        raft1.client_write(cmd).await.unwrap();

        // Wait for replication
        tokio::time::sleep(Duration::from_millis(150)).await;

        // All three storage instances should have the data
        assert_eq!(s1.total_points_written(), 2, "leader storage");
        assert_eq!(s2.total_points_written(), 2, "follower 2 storage");
        assert_eq!(s3.total_points_written(), 2, "follower 3 storage");
    }

    #[tokio::test]
    async fn three_node_multiple_writes() {
        let router = RegionRaftRouter::new();
        let config = make_test_config();
        let region_id = 20;

        let [(raft1, s1), (_raft2, s2), (_raft3, s3)] =
            build_three_node_cluster(region_id, &router, &config).await;

        // Multiple writes
        for i in 0..5 {
            let cmd = RegionWriteCommand::WritePoints {
                region_id,
                request_id: None,
                points: vec![make_test_point("mem", i * 1000)],
            };
            raft1.client_write(cmd).await.unwrap();
        }

        // Wait for replication
        tokio::time::sleep(Duration::from_millis(150)).await;

        assert_eq!(s1.total_points_written(), 5);
        assert_eq!(s2.total_points_written(), 5);
        assert_eq!(s3.total_points_written(), 5);
    }

    #[tokio::test]
    async fn quorum_write_with_one_node_down() {
        let router = RegionRaftRouter::new();
        let config = make_test_config();
        let region_id = 30;

        let [(raft1, s1), (_raft2, s2), (_raft3, _s3)] =
            build_three_node_cluster(region_id, &router, &config).await;

        // Remove node 3 from router (simulates network partition)
        router.remove_node(region_id, 3);

        // Write should still succeed (2 of 3 = quorum)
        let cmd = RegionWriteCommand::WritePoints {
            region_id,
            request_id: None,
            points: vec![make_test_point("disk", 5000)],
        };
        let resp = raft1.client_write(cmd).await.unwrap();
        assert_eq!(resp.data.written, 1);

        // Wait for replication to surviving nodes
        tokio::time::sleep(Duration::from_millis(80)).await;

        // Leader and follower 2 should have the data
        assert_eq!(s1.total_points_written(), 1);
        assert_eq!(s2.total_points_written(), 1);
    }

    #[tokio::test]
    async fn no_quorum_write_fails() {
        let router = RegionRaftRouter::new();
        let config = make_test_config();
        let region_id = 40;

        let [(raft1, _s1), (_raft2, _s2), (_raft3, _s3)] =
            build_three_node_cluster(region_id, &router, &config).await;

        // Remove both followers from router (no quorum possible)
        router.remove_node(region_id, 2);
        router.remove_node(region_id, 3);

        // Wait for leader to detect loss of followers
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Write should fail — no quorum
        let cmd = RegionWriteCommand::WritePoints {
            region_id,
            request_id: None,
            points: vec![make_test_point("net", 9000)],
        };
        let result =
            tokio::time::timeout(Duration::from_millis(500), raft1.client_write(cmd)).await;

        // Either times out or returns an error — either way, no quorum
        match result {
            Ok(Ok(_)) => panic!("expected write to fail without quorum"),
            Ok(Err(_)) => {} // Raft correctly rejected the write
            Err(_) => {}     // Timed out — also acceptable
        }
    }

    // ── Manager tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn manager_create_and_propose() {
        let router = RegionRaftRouter::new();
        let manager = RegionRaftManager::new_in_memory(router.clone(), 1);
        let config = make_test_config();
        let storage = std::sync::Arc::new(MockRegionStorage::default());

        let raft = manager
            .create_raft_group(1, 1, storage.clone(), config)
            .await
            .unwrap();

        // Initialize as single-node cluster
        let mut members = BTreeMap::new();
        members.insert(1u64, BasicNode::new("127.0.0.1:10001"));
        raft.initialize(members).await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Propose write through manager
        let written = manager
            .propose_write(1, vec![make_test_point("cpu", 100)])
            .await
            .unwrap();
        assert_eq!(written, 1);
        assert_eq!(storage.total_points_written(), 1);
    }

    #[tokio::test]
    async fn manager_group_lifecycle() {
        let router = RegionRaftRouter::new();
        let manager = RegionRaftManager::new_in_memory(router, 1);
        let config = make_test_config();
        let storage: std::sync::Arc<dyn RegionStorage> =
            std::sync::Arc::new(MockRegionStorage::default());

        manager
            .create_raft_group(10, 1, storage.clone(), config.clone())
            .await
            .unwrap();
        assert_eq!(manager.group_count(), 1);
        assert!(manager.get_raft(10).is_some());
        assert_eq!(manager.router().entry_count(), 1);

        manager
            .create_raft_group(20, 1, storage, config)
            .await
            .unwrap();
        assert_eq!(manager.group_count(), 2);
        assert_eq!(manager.router().entry_count(), 2);

        let _ = manager.remove_group(10);
        assert_eq!(manager.group_count(), 1);
        assert!(manager.get_raft(10).is_none());
        assert!(manager.get_raft(20).is_some());
        // Verify router entries are cleaned up on remove_group
        assert_eq!(
            manager.router().entry_count(),
            1,
            "remove_group must also deregister from the router"
        );
    }

    #[tokio::test]
    async fn manager_propose_unknown_region() {
        let router = RegionRaftRouter::new();
        let manager = RegionRaftManager::new_in_memory(router, 1);

        let result = manager
            .propose_write(999, vec![make_test_point("cpu", 100)])
            .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            ClusterError::RegionNotFound(id) => assert_eq!(id, 999),
            other => panic!("expected RegionNotFound, got {other:?}"),
        }
    }

    // ── Log store edge-case tests ──────────────────────────────────

    #[tokio::test]
    async fn log_store_purge_returns_error_on_backward_id() {
        let mut store = RegionLogStore::default();

        // Append two entries and purge up to index 5
        store
            .test_append(vec![blank_entry(log_id(1, 1)), blank_entry(log_id(1, 5))])
            .await;
        store.purge(log_id(1, 5)).await.unwrap();

        // Trying to purge at an earlier index should return an error, not panic
        let result = store.purge(log_id(1, 3)).await;
        assert!(
            result.is_err(),
            "purge with backward log_id should return StorageError"
        );
    }

    #[tokio::test]
    async fn log_store_purge_same_id_is_ok() {
        let mut store = RegionLogStore::default();
        store.test_append(vec![blank_entry(log_id(1, 1))]).await;

        // Purging the same index twice should succeed (idempotent)
        store.purge(log_id(1, 1)).await.unwrap();
        store.purge(log_id(1, 1)).await.unwrap();
    }
}
