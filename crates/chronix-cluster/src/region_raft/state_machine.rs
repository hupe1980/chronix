//! Raft state-machine store for a single data region.

use std::collections::VecDeque;
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::BasicNode;
use openraft::{
    EntryPayload, LogId, RaftSnapshotBuilder, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::debug;

use crate::data_service::RegionStorage;

use super::{RegionEntry, RegionTypeConfig, RegionWriteCommand, RegionWriteResponse};

// ── State Machine Store ─────────────────────────────────────────────

/// Region snapshot payload — includes both Raft bookkeeping and
/// actual region data so followers can reconstruct the full state
/// after Raft log compaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegionSnapshot {
    /// Monotonic counter for snapshot ordering.
    generation: u64,
    /// Opaque serialized region data from `RegionStorage::snapshot_data()`.
    /// Empty on bootstrap; populated once data exists.
    region_data: Vec<u8>,
}

/// Stored snapshot with `OpenRaft` metadata.
#[derive(Debug)]
struct StoredSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    data: Vec<u8>,
}

/// `OpenRaft` state-machine store for a single data region.
///
/// On each committed log entry the state machine calls
/// [`RegionStorage::write_points`] to persist data locally. This
/// ensures every node in the region's Raft group has an identical
/// copy of the region's data.
pub struct RegionSmStore {
    /// Storage backend for persisting points.
    storage: Arc<dyn RegionStorage>,
    /// Region this state machine belongs to.
    region_id: u64,
    /// Last applied log ID (`OpenRaft` bookkeeping).
    last_applied_log: RwLock<Option<LogId<u64>>>,
    /// Current Raft membership configuration.
    last_membership: RwLock<StoredMembership<u64, BasicNode>>,
    /// Monotonic snapshot index.
    snapshot_idx: AtomicU64,
    /// Most recent snapshot.
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    /// Cluster-wide dedup cache. Maps request_id → written count.
    /// This cache is replicated through the Raft log: every node processes
    /// the same log entries, so the cache is identical across all replicas.
    /// Bounded to `DEDUP_CACHE_CAPACITY` entries; oldest are evicted first.
    applied_requests: DashMap<String, u64>,
    /// Insertion-order tracker for bounded eviction of `applied_requests`.
    applied_requests_order: RwLock<VecDeque<String>>,
}

/// Maximum number of request IDs cached for cluster-wide dedup.
/// 100K entries × ~64 bytes/key ≈ 6.4 MiB — bounded memory per region.
const DEDUP_CACHE_CAPACITY: usize = 100_000;

impl RegionSmStore {
    /// Create a new state-machine store for the given region.
    pub fn new(region_id: u64, storage: Arc<dyn RegionStorage>) -> Self {
        Self {
            storage,
            region_id,
            last_applied_log: RwLock::new(None),
            last_membership: RwLock::new(StoredMembership::default()),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(None),
            applied_requests: DashMap::new(),
            applied_requests_order: RwLock::new(VecDeque::new()),
        }
    }

    /// Access the underlying storage.
    #[must_use]
    pub fn storage(&self) -> &Arc<dyn RegionStorage> {
        &self.storage
    }
}

impl std::fmt::Debug for RegionSmStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionSmStore")
            .field("region_id", &self.region_id)
            .field("snapshot_idx", &self.snapshot_idx.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl RaftSnapshotBuilder<RegionTypeConfig> for Arc<RegionSmStore> {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<Snapshot<RegionTypeConfig>, StorageError<u64>> {
        let last_applied_log = *self.last_applied_log.read().await;
        let last_membership = self.last_membership.read().await.clone();

        // Capture actual region data from the storage backend.
        let region_data = self
            .storage
            .snapshot_data(self.region_id)
            .await
            .map_err(|e| StorageIOError::read_state_machine(&e))?;

        let snap = RegionSnapshot {
            generation: self.snapshot_idx.load(Ordering::Relaxed),
            region_data,
        };
        let data =
            postcard::to_stdvec(&snap).map_err(|e| StorageIOError::read_state_machine(&e))?;

        let mut current_snapshot = self.current_snapshot.write().await;

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{snapshot_idx}")
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        *current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });

        debug!(
            region_id = self.region_id,
            last_applied = ?last_applied_log,
            size = data.len(),
            "Built region snapshot"
        );

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<RegionTypeConfig> for Arc<RegionSmStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> std::result::Result<
        (Option<LogId<u64>>, StoredMembership<u64, BasicNode>),
        StorageError<u64>,
    > {
        let last_applied = *self.last_applied_log.read().await;
        let membership = self.last_membership.read().await.clone();
        Ok((last_applied, membership))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> std::result::Result<Vec<RegionWriteResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = RegionEntry> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();

        for entry in entries {
            let log_id = entry.log_id;

            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(RegionWriteResponse::default());
                }
                EntryPayload::Normal(cmd) => {
                    let resp = match cmd {
                        RegionWriteCommand::WritePoints {
                            region_id,
                            points,
                            request_id,
                        } => {
                            // Cluster-wide dedup — if this request_id
                            // was already applied (e.g. after leader failover
                            // and re-proposal), return the cached result without
                            // re-writing. This works because every replica
                            // processes the same Raft log deterministically.
                            if let Some(ref rid) = request_id {
                                if let Some(cached) = self.applied_requests.get(rid) {
                                    debug!(
                                        region_id,
                                        request_id = %rid,
                                        cached_written = *cached,
                                        "skipping duplicate write (cluster-wide dedup)"
                                    );
                                    RegionWriteResponse { written: *cached }
                                } else {
                                    let written = self
                                        .storage
                                        .write_points(region_id, points)
                                        .await
                                        .map_err(|e| StorageIOError::write(&e))?;
                                    self.applied_requests.insert(rid.clone(), written);
                                    // Bounded eviction: drop oldest entry when at capacity.
                                    let mut order = self.applied_requests_order.write().await;
                                    order.push_back(rid.clone());
                                    while order.len() > DEDUP_CACHE_CAPACITY {
                                        if let Some(old) = order.pop_front() {
                                            self.applied_requests.remove(&old);
                                        }
                                    }
                                    RegionWriteResponse { written }
                                }
                            } else {
                                let written = self
                                    .storage
                                    .write_points(region_id, points)
                                    .await
                                    .map_err(|e| StorageIOError::write(&e))?;
                                RegionWriteResponse { written }
                            }
                        }
                    };
                    responses.push(resp);
                }
                EntryPayload::Membership(mem) => {
                    *self.last_membership.write().await = StoredMembership::new(Some(log_id), mem);
                    responses.push(RegionWriteResponse::default());
                }
            }

            // Update last_applied_log AFTER the entry has been
            // successfully processed. Setting it before would cause
            // a failed entry to be permanently skipped on recovery,
            // because applied_state() would report it as already
            // applied even though write_points never completed.
            *self.last_applied_log.write().await = Some(log_id);
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> std::result::Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> std::result::Result<(), StorageError<u64>> {
        let data = snapshot.into_inner();

        // Deserialize and restore region data.
        let snap: RegionSnapshot = postcard::from_bytes(&data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;

        // Restore actual region data from the snapshot.
        if !snap.region_data.is_empty() {
            self.storage
                .restore_snapshot(self.region_id, &snap.region_data)
                .await
                .map_err(|e| StorageIOError::write(&e))?;
        }

        *self.last_applied_log.write().await = meta.last_log_id;
        *self.last_membership.write().await = meta.last_membership.clone();

        *self.current_snapshot.write().await = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });

        debug!(
            region_id = self.region_id,
            last_applied = ?meta.last_log_id,
            snapshot_data_bytes = snap.region_data.len(),
            "Installed region snapshot with data"
        );
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<Snapshot<RegionTypeConfig>>, StorageError<u64>> {
        let guard = self.current_snapshot.read().await;
        match &*guard {
            Some(stored) => Ok(Some(Snapshot {
                meta: stored.meta.clone(),
                snapshot: Box::new(Cursor::new(stored.data.clone())),
            })),
            None => Ok(None),
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::MockRegionStore;
    use chronix_core::types::{FieldValue, Point, SeriesKey};
    use std::collections::BTreeMap;

    use openraft::CommittedLeaderId;

    fn make_point(measurement: &str, tag_val: &str, value: f64, ts: i64) -> Point {
        let mut tags = BTreeMap::new();
        tags.insert("host".to_string(), tag_val.to_string());
        let key = SeriesKey::new(measurement, tags).expect("valid key");
        let mut fields = BTreeMap::new();
        fields.insert("value".to_string(), FieldValue::F64(value));
        Point::new(key, fields, ts).expect("valid point")
    }

    #[tokio::test]
    async fn snapshot_round_trip_preserves_data() {
        let store = Arc::new(MockRegionStore::default());
        let region_id = 42;

        // Write some points through the mock.
        let points = vec![
            make_point("cpu", "host1", 1.0, 1000),
            make_point("cpu", "host2", 2.0, 2000),
            make_point("cpu", "host1", 3.0, 3000),
        ];
        store
            .write_points(region_id, points.clone())
            .await
            .expect("write");

        // Build a snapshot from the source state machine.
        let sm_source = Arc::new(RegionSmStore::new(region_id, store.clone()));
        let mut builder: Arc<RegionSmStore> = sm_source.clone();
        let snapshot = RaftSnapshotBuilder::build_snapshot(&mut builder)
            .await
            .expect("build_snapshot");

        // Verify snapshot contains data.
        let snap_data = snapshot.snapshot.into_inner();
        let snap: RegionSnapshot = postcard::from_bytes(&snap_data).expect("deserialize snapshot");
        assert!(
            !snap.region_data.is_empty(),
            "snapshot must contain region data"
        );

        // Create a fresh store (simulating a new follower node) and install the snapshot.
        let dest_store = Arc::new(MockRegionStore::default());
        let mut sm_dest: Arc<RegionSmStore> =
            Arc::new(RegionSmStore::new(region_id, dest_store.clone()));

        let meta = SnapshotMeta {
            last_log_id: Some(LogId::new(CommittedLeaderId::new(1, 0), 10)),
            last_membership: StoredMembership::default(),
            snapshot_id: "test-snap-1".to_string(),
        };

        RaftStateMachine::install_snapshot(&mut sm_dest, &meta, Box::new(Cursor::new(snap_data)))
            .await
            .expect("install_snapshot");

        // Verify the destination store now has the same points.
        let restored = dest_store
            .query_region(region_id, Default::default())
            .await
            .expect("query");
        assert_eq!(restored.len(), 3, "all points must be restored");
        assert_eq!(restored[0].timestamp(), 1000);
        assert_eq!(restored[1].timestamp(), 2000);
        assert_eq!(restored[2].timestamp(), 3000);
    }

    #[tokio::test]
    async fn snapshot_empty_region_produces_empty_data() {
        let store = Arc::new(MockRegionStore::default());
        let sm = Arc::new(RegionSmStore::new(99, store));
        let mut builder: Arc<RegionSmStore> = sm.clone();
        let snapshot = RaftSnapshotBuilder::build_snapshot(&mut builder)
            .await
            .expect("build_snapshot");

        let snap: RegionSnapshot =
            postcard::from_bytes(&snapshot.snapshot.into_inner()).expect("deser");
        let restored_points: Vec<Point> =
            serde_json::from_slice(&snap.region_data).expect("deser points");
        assert!(
            restored_points.is_empty(),
            "empty region should produce empty snapshot data"
        );
    }
}
