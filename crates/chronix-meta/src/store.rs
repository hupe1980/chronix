//! OpenRaft-integrated log store and state-machine adapter.
//!
//! Contains the Raft type configuration, an in-memory log store, and a
//! state-machine store that wraps [`MetaStateMachine`] for Raft replication.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftLogId, RaftLogReader, RaftSnapshotBuilder,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tracing::debug;

use crate::durable_log_store::DurableLogStore;
use crate::error::MetaError;
use crate::state_machine::{MetaSnapshot, MetaStateMachine};
use crate::types::{MetaCommand, MetaResponse};

// ── Type Configuration ──────────────────────────────────────────────

openraft::declare_raft_types!(
    /// OpenRaft type configuration for the Chronix metadata Raft group.
    pub MetaTypeConfig:
        D = MetaCommand,
        R = MetaResponse,
        NodeId = u64,
        Node = BasicNode,
);

/// Convenience alias for the metadata Raft instance.
pub type MetaRaft = openraft::Raft<MetaTypeConfig>;

/// Convenience alias for a metadata Raft log entry.
pub type MetaEntry = Entry<MetaTypeConfig>;

// ── Log Store ───────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct LogStoreInner {
    /// Last purged log id.
    last_purged_log_id: Option<LogId<u64>>,
    /// Raft log entries.
    log: BTreeMap<u64, MetaEntry>,
    /// Committed log id (optional optimisation).
    committed: Option<LogId<u64>>,
    /// Current granted vote.
    vote: Option<openraft::Vote<u64>>,
}

/// In-memory Raft log store.
///
/// Thread-safe through `Arc<Mutex<…>>` — suitable for testing and
/// embedded single-node deployments. Production multi-node clusters may
/// use a persistent WAL implementation instead.
#[derive(Clone, Debug, Default)]
pub struct MetaLogStore {
    inner: Arc<Mutex<LogStoreInner>>,
}

impl RaftLogReader<MetaTypeConfig> for MetaLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<MetaEntry>, StorageError<u64>> {
        let inner = self.inner.lock().await;
        let entries = inner.log.range(range).map(|(_, ent)| ent.clone()).collect();
        Ok(entries)
    }
}

impl RaftLogStorage<MetaTypeConfig> for MetaLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<MetaTypeConfig>, StorageError<u64>> {
        let inner = self.inner.lock().await;
        let last = inner
            .log
            .iter()
            .next_back()
            .map(|(_, ent)| *ent.get_log_id());
        let last_purged = inner.last_purged_log_id;
        let last = match last {
            None => last_purged,
            Some(x) => Some(x),
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &openraft::Vote<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        inner.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<u64>>, StorageError<u64>> {
        let inner = self.inner.lock().await;
        Ok(inner.vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<MetaTypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = MetaEntry> + Send,
        I::IntoIter: Send,
    {
        let mut inner = self.inner.lock().await;
        for entry in entries {
            inner.log.insert(entry.get_log_id().index, entry);
        }
        // In-memory: immediately "flushed"
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in keys {
            inner.log.remove(&key);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        if inner.last_purged_log_id.as_ref() > Some(&log_id) {
            return Err(StorageError::IO {
                source: StorageIOError::write_logs(&std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "purge log_id {log_id:?} is older than last_purged {:?}",
                        inner.last_purged_log_id
                    ),
                )),
            });
        }
        inner.last_purged_log_id = Some(log_id);

        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for key in keys {
            inner.log.remove(&key);
        }
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        inner.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        let inner = self.inner.lock().await;
        Ok(inner.committed)
    }
}

// ── State Machine Store ─────────────────────────────────────────────

/// Composite snapshot: our application state + `OpenRaft` metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RaftMetaSnapshot {
    /// Application-level state.
    data: MetaSnapshot,
}

/// Stored snapshot with `OpenRaft` metadata.
#[derive(Debug)]
struct StoredSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    data: Vec<u8>,
}

/// `OpenRaft` state-machine store wrapping [`MetaStateMachine`].
///
/// Tracks `OpenRaft`-specific metadata (last applied log ID, membership,
/// snapshots) while delegating business logic to the inner state machine.
///
/// ## Atomic snapshot capture
///
/// `last_applied_log` and `last_membership` are held under a single
/// `RwLock` so that `build_snapshot` and `applied_state` always see a
/// consistent pair. Previous code used separate locks, which allowed a
/// race window where membership could advance between the two reads.
pub struct MetaSmStore {
    /// Inner deterministic state machine.
    sm: MetaStateMachine,
    /// Atomically captured Raft state (log id + membership).
    raft_state: RwLock<SmRaftState>,
    /// Monotonic snapshot index for unique IDs.
    snapshot_idx: AtomicU64,
    /// Most recent snapshot.
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}

/// Raft-level metadata that must be captured atomically for snapshots.
#[derive(Clone, Debug)]
struct SmRaftState {
    last_applied_log: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
}

impl Default for SmRaftState {
    fn default() -> Self {
        Self {
            last_applied_log: None,
            last_membership: StoredMembership::default(),
        }
    }
}

impl MetaSmStore {
    /// Create a new state-machine store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sm: MetaStateMachine::new(),
            raft_state: RwLock::new(SmRaftState::default()),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(None),
        }
    }

    /// Access the inner deterministic state machine.
    #[must_use]
    pub fn state_machine(&self) -> &MetaStateMachine {
        &self.sm
    }
}

impl Default for MetaSmStore {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MetaSmStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaSmStore")
            .field("sm", &self.sm)
            .field("snapshot_idx", &self.snapshot_idx.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl RaftSnapshotBuilder<MetaTypeConfig> for Arc<MetaSmStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<MetaTypeConfig>, StorageError<u64>> {
        // Read both fields under a single lock for atomic capture.
        let state = self.raft_state.read().await.clone();
        let last_applied_log = state.last_applied_log;
        let last_membership = state.last_membership;

        // Serialize application state
        let meta_snap = self.sm.snapshot();
        let raft_snap = RaftMetaSnapshot { data: meta_snap };
        let data =
            postcard::to_stdvec(&raft_snap).map_err(|e| StorageIOError::read_state_machine(&e))?;

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
            last_applied = ?last_applied_log,
            size = data.len(),
            "Built metadata snapshot"
        );

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<MetaTypeConfig> for Arc<MetaSmStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        // Atomic read of both fields.
        let state = self.raft_state.read().await;
        Ok((state.last_applied_log, state.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<MetaResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = MetaEntry> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();

        for entry in entries {
            let log_id = entry.log_id;

            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(MetaResponse::Ok);
                }
                EntryPayload::Normal(ref cmd) => {
                    let resp = self.sm.apply(log_id.index, cmd);
                    responses.push(resp);
                }
                EntryPayload::Membership(ref mem) => {
                    self.raft_state.write().await.last_membership =
                        StoredMembership::new(Some(log_id), mem.clone());
                    responses.push(MetaResponse::Ok);
                }
            }

            // Update last_applied_log AFTER the entry is successfully
            // applied so a panic during apply() does not advance the
            // cursor past an unapplied entry.
            self.raft_state.write().await.last_applied_log = Some(log_id);
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let data = snapshot.into_inner();

        let raft_snap: RaftMetaSnapshot = postcard::from_bytes(&data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;

        // Restore application state
        self.sm.restore(raft_snap.data);

        // Atomic update of both fields.
        {
            let mut state = self.raft_state.write().await;
            state.last_applied_log = meta.last_log_id;
            state.last_membership = meta.last_membership.clone();
        }

        // Store snapshot
        *self.current_snapshot.write().await = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });

        debug!(last_applied = ?meta.last_log_id, "Installed metadata snapshot");
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<MetaTypeConfig>>, StorageError<u64>> {
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

// ── MetaStore Facade ────────────────────────────────────────────────

/// Raft log store variant — in-memory for tests, durable for production.
#[derive(Clone, Debug)]
pub enum MetaLogStoreKind {
    /// In-memory store — fast, suitable for tests and single-node development.
    Memory(MetaLogStore),
    /// Durable store backed by redb — crash-safe, for production clusters.
    Durable(DurableLogStore<MetaTypeConfig>),
}

impl RaftLogReader<MetaTypeConfig> for MetaLogStoreKind {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<MetaEntry>, StorageError<u64>> {
        match self {
            Self::Memory(s) => s.try_get_log_entries(range).await,
            Self::Durable(s) => s.try_get_log_entries(range).await,
        }
    }
}

impl RaftLogStorage<MetaTypeConfig> for MetaLogStoreKind {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<MetaTypeConfig>, StorageError<u64>> {
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
        callback: LogFlushed<MetaTypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = MetaEntry> + Send,
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

/// Convenience wrapper for creating a metadata Raft node.
///
/// Holds the log store and state-machine store, and provides methods
/// to initialise the [`openraft::Raft`] instance.
///
/// # Example (durable multi-node cluster)
///
/// ```rust,ignore
/// use chronix_meta::store::{MetaStore, MetaTypeConfig};
/// use chronix_meta::network::MetaNetworkFactory;
///
/// let store = MetaStore::open("/var/lib/chronix/meta")?;
/// let config = Arc::new(openraft::Config::default().validate().unwrap());
/// let raft = store.build_raft(1, config, MetaNetworkFactory::new()).await?;
/// ```
pub struct MetaStore {
    log_store: MetaLogStoreKind,
    sm_store: Arc<MetaSmStore>,
}

impl MetaStore {
    /// Create a new in-memory `MetaStore` for tests and single-node embedded deployments.
    #[must_use]
    pub fn new_in_memory() -> Self {
        Self {
            log_store: MetaLogStoreKind::Memory(MetaLogStore::default()),
            sm_store: Arc::new(MetaSmStore::new()),
        }
    }

    /// Open a `MetaStore` with durable log storage at the given directory.
    ///
    /// Creates `<data_dir>/meta_raft_log.redb` for the Raft log.
    /// The state machine starts empty and is reconstructed by replaying
    /// the Raft log (openraft handles this automatically).
    ///
    /// # Errors
    ///
    /// Returns an error if the redb database cannot be opened or created.
    pub fn open(data_dir: impl AsRef<Path>) -> std::result::Result<Self, MetaError> {
        let dir = data_dir.as_ref();
        std::fs::create_dir_all(dir).map_err(|e| MetaError::Raft(e.to_string()))?;
        let db_path = dir.join("meta_raft_log.redb");
        let durable = DurableLogStore::open(&db_path)
            .map_err(|e| MetaError::Raft(format!("open durable log store: {e}")))?;
        Ok(Self {
            log_store: MetaLogStoreKind::Durable(durable),
            sm_store: Arc::new(MetaSmStore::new()),
        })
    }

    /// Access the inner deterministic state machine for queries.
    #[must_use]
    pub fn state_machine(&self) -> &MetaStateMachine {
        self.sm_store.state_machine()
    }

    /// Get a clone of the log store (for passing to `Raft::new`).
    #[must_use]
    pub fn log_store(&self) -> MetaLogStoreKind {
        self.log_store.clone()
    }

    /// Get a clone of the state-machine store (for passing to `Raft::new`).
    #[must_use]
    pub fn sm_store(&self) -> Arc<MetaSmStore> {
        self.sm_store.clone()
    }

    /// Build and return a Raft instance ready for use.
    ///
    /// The caller is responsible for providing a network factory that
    /// can communicate with other `MetaNodes`.
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance cannot be created.
    pub async fn build_raft(
        &self,
        node_id: u64,
        config: Arc<openraft::Config>,
        network: impl openraft::RaftNetworkFactory<MetaTypeConfig>,
    ) -> Result<MetaRaft, MetaError> {
        openraft::Raft::new(
            node_id,
            config,
            network,
            self.log_store.clone(),
            self.sm_store.clone(),
        )
        .await
        .map_err(|e| MetaError::Raft(e.to_string()))
    }
}

impl Default for MetaStore {
    fn default() -> Self {
        Self::new_in_memory()
    }
}

impl Clone for MetaStore {
    fn clone(&self) -> Self {
        Self {
            log_store: self.log_store.clone(),
            sm_store: Arc::clone(&self.sm_store),
        }
    }
}

impl std::fmt::Debug for MetaStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaStore")
            .field("sm", &self.sm_store)
            .finish_non_exhaustive()
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
impl MetaLogStore {
    /// Test helper: append entries directly (bypasses `LogFlushed` callback
    /// which is `pub(crate)` in openraft and thus inaccessible).
    async fn test_append(&mut self, entries: Vec<MetaEntry>) {
        let mut inner = self.inner.lock().await;
        for entry in entries {
            inner.log.insert(entry.get_log_id().index, entry);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::entry::RaftEntry;
    use openraft::{LogId, RaftLogId, Vote};

    fn log_id(term: u64, index: u64) -> LogId<u64> {
        LogId::new(openraft::CommittedLeaderId::new(term, 0), index)
    }

    fn blank_entry(log_id: LogId<u64>) -> MetaEntry {
        MetaEntry::new_blank(log_id)
    }

    fn normal_entry(log_id: LogId<u64>, cmd: MetaCommand) -> MetaEntry {
        Entry {
            log_id,
            payload: EntryPayload::Normal(cmd),
        }
    }

    // ── Log Store tests ─────────────────────────────────────────

    #[tokio::test]
    async fn log_store_append_and_read() {
        let mut store = MetaLogStore::default();

        let entries = vec![
            blank_entry(log_id(1, 1)),
            blank_entry(log_id(1, 2)),
            blank_entry(log_id(1, 3)),
        ];
        store.test_append(entries).await;

        let read = store.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(read.len(), 3);
        assert_eq!(read[0].get_log_id().index, 1);
        assert_eq!(read[2].get_log_id().index, 3);
    }

    #[tokio::test]
    async fn log_store_state() {
        let mut store = MetaLogStore::default();

        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());
        assert!(state.last_purged_log_id.is_none());

        let entries = vec![blank_entry(log_id(1, 1)), blank_entry(log_id(1, 2))];
        store.test_append(entries).await;

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 2);
    }

    #[tokio::test]
    async fn log_store_truncate() {
        let mut store = MetaLogStore::default();

        let entries = vec![
            blank_entry(log_id(1, 1)),
            blank_entry(log_id(1, 2)),
            blank_entry(log_id(1, 3)),
        ];
        store.test_append(entries).await;

        store.truncate(log_id(1, 2)).await.unwrap();
        let read = store.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(read.len(), 1); // only entry 1 remains
        assert_eq!(read[0].get_log_id().index, 1);
    }

    #[tokio::test]
    async fn log_store_purge() {
        let mut store = MetaLogStore::default();

        let entries = vec![
            blank_entry(log_id(1, 1)),
            blank_entry(log_id(1, 2)),
            blank_entry(log_id(1, 3)),
        ];
        store.test_append(entries).await;

        store.purge(log_id(1, 2)).await.unwrap();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 2);

        let read = store.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(read.len(), 1); // only entry 3 remains
    }

    #[tokio::test]
    async fn log_store_vote() {
        let mut store = MetaLogStore::default();

        assert!(store.read_vote().await.unwrap().is_none());

        let vote = Vote::new(1, 0);
        store.save_vote(&vote).await.unwrap();
        assert_eq!(store.read_vote().await.unwrap().unwrap(), vote);
    }

    #[tokio::test]
    async fn log_store_committed() {
        let mut store = MetaLogStore::default();

        assert!(store.read_committed().await.unwrap().is_none());

        let lid = log_id(1, 5);
        store.save_committed(Some(lid)).await.unwrap();
        assert_eq!(store.read_committed().await.unwrap().unwrap(), lid);
    }

    // ── State Machine Store tests ───────────────────────────────

    #[tokio::test]
    async fn sm_store_apply_normal() {
        let store = Arc::new(MetaSmStore::new());
        let mut sm: Arc<MetaSmStore> = store.clone();

        let entries = vec![normal_entry(
            log_id(1, 1),
            MetaCommand::CreateMeasurement(crate::types::MeasurementSchema::new("cpu")),
        )];

        let responses = sm.apply(entries).await.unwrap();
        assert_eq!(responses.len(), 1);
        assert!(matches!(responses[0], MetaResponse::Created { .. }));
        assert!(store.state_machine().get_schema("cpu").is_some());
    }

    #[tokio::test]
    async fn sm_store_apply_blank() {
        let store = Arc::new(MetaSmStore::new());
        let mut sm: Arc<MetaSmStore> = store.clone();

        let entries = vec![blank_entry(log_id(1, 1))];
        let responses = sm.apply(entries).await.unwrap();
        assert_eq!(responses.len(), 1);
        assert!(matches!(responses[0], MetaResponse::Ok));
    }

    #[tokio::test]
    async fn sm_store_applied_state() {
        let store = Arc::new(MetaSmStore::new());
        let mut sm: Arc<MetaSmStore> = store.clone();

        let (last, _membership) = sm.applied_state().await.unwrap();
        assert!(last.is_none());

        let entries = vec![blank_entry(log_id(1, 1)), blank_entry(log_id(1, 2))];
        sm.apply(entries).await.unwrap();

        let (last, _) = sm.applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 2);
    }

    #[tokio::test]
    async fn sm_store_snapshot_roundtrip() {
        let store = Arc::new(MetaSmStore::new());
        let mut sm: Arc<MetaSmStore> = store.clone();

        // Apply some data
        let entries = vec![
            normal_entry(
                log_id(1, 1),
                MetaCommand::RegisterNode(crate::types::DataNodeInfo::new(1, "127.0.0.1:9001")),
            ),
            normal_entry(
                log_id(1, 2),
                MetaCommand::CreateMeasurement(crate::types::MeasurementSchema::new("cpu")),
            ),
        ];
        sm.apply(entries).await.unwrap();

        // Build snapshot
        let snapshot = sm.build_snapshot().await.unwrap();
        assert_eq!(snapshot.meta.last_log_id.unwrap().index, 2);

        // Restore into a fresh store
        let store2 = Arc::new(MetaSmStore::new());
        let mut sm2: Arc<MetaSmStore> = store2.clone();
        sm2.install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();

        // Verify restored state
        assert!(store2.state_machine().get_schema("cpu").is_some());
        assert!(store2.state_machine().get_node(1).is_some());
        let (last, _) = sm2.applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 2);
    }

    #[tokio::test]
    async fn sm_store_get_current_snapshot() {
        let store = Arc::new(MetaSmStore::new());
        let mut sm: Arc<MetaSmStore> = store.clone();

        // No snapshot initially
        assert!(sm.get_current_snapshot().await.unwrap().is_none());

        // Build one
        sm.build_snapshot().await.unwrap();

        // Now it should exist
        let snap = sm.get_current_snapshot().await.unwrap();
        assert!(snap.is_some());
    }

    #[tokio::test]
    async fn meta_store_facade() {
        let store = MetaStore::new_in_memory();
        assert!(store.state_machine().schemas().is_empty());
        assert!(store.state_machine().nodes().is_empty());

        // Verify SM cloning produces shared state
        let sm1 = store.sm_store();
        let sm2 = store.sm_store();
        assert!(Arc::ptr_eq(&sm1, &sm2));
    }

    #[tokio::test]
    async fn meta_store_durable_facade() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = MetaStore::open(tmp.path()).unwrap();
        assert!(store.state_machine().schemas().is_empty());
        assert!(store.state_machine().nodes().is_empty());

        let sm1 = store.sm_store();
        let sm2 = store.sm_store();
        assert!(Arc::ptr_eq(&sm1, &sm2));
    }
}
