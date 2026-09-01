//! Durable Raft log store backed by [`redb`](https://docs.rs/redb).
//!
//! Provides ACID-compliant, crash-safe persistence for Raft log entries,
//! vote state, commit cursor, and purge cursor. Each mutation is wrapped
//! in a redb write transaction with implicit fsync, ensuring durability
//! across process crashes and power loss.
//!
//! # Design
//!
//! - **Durable writes**: Every `append()`, `save_vote()`, `purge()`, etc.
//!   commits a redb write transaction. The `LogFlushed` callback is invoked
//!   only *after* commit, so the consensus layer treats entries as durable.
//!
//! - **Efficient range reads**: `try_get_log_entries()` uses redb's B+ tree
//!   range scan — O(log N + K) where K is the result count. The OS page
//!   cache keeps hot pages in memory without explicit caching.
//!
//! - **Crash recovery**: On restart, state is reconstructed directly from
//!   redb, which handles crash recovery internally via its copy-on-write
//!   B+ tree (no WAL replay needed).
//!
//! - **Generic**: Works with any openraft `RaftTypeConfig` — used by both
//!   Meta and Region Raft groups.

use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{
    Entry, LogId, RaftLogId, RaftLogReader, RaftTypeConfig, StorageError, StorageIOError,
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use tracing::debug;

/// redb table for Raft log entries: index → postcard(Entry<C>).
const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");

/// redb table for Raft metadata: key → postcard(value).
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

const VOTE_KEY: &str = "vote";
const COMMITTED_KEY: &str = "committed";
const LAST_PURGED_KEY: &str = "last_purged";

/// Helper to convert any Display error into a `StorageError` for writes.
fn write_err(e: impl std::fmt::Display) -> StorageError<u64> {
    StorageError::IO {
        source: StorageIOError::write_logs(&std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        )),
    }
}

/// Helper to convert any Display error into a `StorageError` for reads.
fn read_err(e: impl std::fmt::Display) -> StorageError<u64> {
    StorageError::IO {
        source: StorageIOError::read_logs(&std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        )),
    }
}

/// Durable Raft log store backed by [`redb`](https://docs.rs/redb) — an
/// embedded, ACID-compliant, crash-safe B+ tree database in pure Rust.
///
/// # Storage Layout
///
/// | Table       | Key    | Value              | Purpose                  |
/// |-------------|--------|--------------------|--------------------------|
/// | `raft_log`  | `u64`  | postcard `Entry<C>` | Log entries by index     |
/// | `raft_meta` | `&str` | postcard blob       | vote, committed, purged  |
///
/// # Usage
///
/// ```rust,ignore
/// use chronix_meta::durable_log_store::DurableLogStore;
/// use chronix_meta::store::MetaTypeConfig;
///
/// let store = DurableLogStore::<MetaTypeConfig>::open("/tmp/raft-log.redb")?;
/// ```
pub struct DurableLogStore<C>
where
    C: RaftTypeConfig<NodeId = u64, Entry = Entry<C>>,
{
    db: Arc<Database>,
    _phantom: PhantomData<C>,
}

impl<C> Clone for DurableLogStore<C>
where
    C: RaftTypeConfig<NodeId = u64, Entry = Entry<C>>,
{
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            _phantom: PhantomData,
        }
    }
}

impl<C> Debug for DurableLogStore<C>
where
    C: RaftTypeConfig<NodeId = u64, Entry = Entry<C>>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableLogStore").finish_non_exhaustive()
    }
}

impl<C> DurableLogStore<C>
where
    C: RaftTypeConfig<NodeId = u64, Entry = Entry<C>>,
{
    /// Open or create a durable log store at the given filesystem path.
    ///
    /// Creates the redb database file and initialises tables on first use.
    /// Subsequent opens recover state from the existing database.
    ///
    /// # Errors
    ///
    /// Returns `StorageError` if the database file cannot be created or
    /// the initial table setup fails.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError<u64>> {
        let db = Database::create(path.as_ref()).map_err(write_err)?;

        // Ensure tables exist (no-op on subsequent opens).
        let txn = db.begin_write().map_err(write_err)?;
        {
            txn.open_table(LOG_TABLE).map_err(write_err)?;
            txn.open_table(META_TABLE).map_err(write_err)?;
        }
        txn.commit().map_err(write_err)?;

        debug!(path = %path.as_ref().display(), "Opened durable Raft log store");

        Ok(Self {
            db: Arc::new(db),
            _phantom: PhantomData,
        })
    }

    /// Read a typed value from the metadata table.
    fn read_meta<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, StorageError<u64>> {
        let txn = self.db.begin_read().map_err(read_err)?;
        let table = txn.open_table(META_TABLE).map_err(read_err)?;
        match table.get(key).map_err(read_err)? {
            Some(guard) => {
                let val: T = postcard::from_bytes(guard.value()).map_err(read_err)?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    /// Write a typed value to the metadata table (atomic, durable).
    fn write_meta<T: serde::Serialize>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<(), StorageError<u64>> {
        let bytes = postcard::to_stdvec(value).map_err(write_err)?;
        let txn = self.db.begin_write().map_err(write_err)?;
        {
            let mut table = txn.open_table(META_TABLE).map_err(write_err)?;
            table.insert(key, bytes.as_slice()).map_err(write_err)?;
        }
        txn.commit().map_err(write_err)?;
        Ok(())
    }
}

// ── RaftLogReader ───────────────────────────────────────────────────

impl<C> RaftLogReader<C> for DurableLogStore<C>
where
    C: RaftTypeConfig<NodeId = u64, Entry = Entry<C>>,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<C>>, StorageError<u64>> {
        let txn = self.db.begin_read().map_err(read_err)?;
        let table = txn.open_table(LOG_TABLE).map_err(read_err)?;

        let mut entries = Vec::new();
        for item in table.range(range).map_err(read_err)? {
            let (_, val_guard) = item.map_err(read_err)?;
            let entry: Entry<C> = postcard::from_bytes(val_guard.value()).map_err(read_err)?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

// ── RaftLogStorage ──────────────────────────────────────────────────

impl<C> RaftLogStorage<C> for DurableLogStore<C>
where
    C: RaftTypeConfig<NodeId = u64, Entry = Entry<C>>,
{
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<u64>> {
        let last_purged: Option<LogId<u64>> = self.read_meta(LAST_PURGED_KEY)?;

        let txn = self.db.begin_read().map_err(read_err)?;
        let table = txn.open_table(LOG_TABLE).map_err(read_err)?;

        // Get the last (highest index) entry.
        let last_log_id = if let Some(item) = table.iter().map_err(read_err)?.next_back() {
            let (_, val_guard) = item.map_err(read_err)?;
            let entry: Entry<C> = postcard::from_bytes(val_guard.value()).map_err(read_err)?;
            Some(*entry.get_log_id())
        } else {
            last_purged
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &openraft::Vote<u64>) -> Result<(), StorageError<u64>> {
        let bytes = postcard::to_stdvec(vote).map_err(write_err)?;
        let txn = self.db.begin_write().map_err(write_err)?;
        {
            let mut table = txn.open_table(META_TABLE).map_err(write_err)?;
            table
                .insert(VOTE_KEY, bytes.as_slice())
                .map_err(write_err)?;
        }
        txn.commit().map_err(write_err)?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<u64>>, StorageError<u64>> {
        self.read_meta(VOTE_KEY)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<C>> + Send,
        I::IntoIter: Send,
    {
        let txn = self.db.begin_write().map_err(write_err)?;
        {
            let mut table = txn.open_table(LOG_TABLE).map_err(write_err)?;
            for entry in entries {
                let idx = entry.get_log_id().index;
                let data = postcard::to_stdvec(&entry).map_err(write_err)?;
                table.insert(idx, data.as_slice()).map_err(write_err)?;
            }
        }
        // Commit with implicit fsync — entries are durable after this.
        txn.commit().map_err(write_err)?;

        // Signal durability to the consensus layer.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let txn = self.db.begin_write().map_err(write_err)?;
        {
            let mut table = txn.open_table(LOG_TABLE).map_err(write_err)?;
            // Collect keys to remove: all entries with index >= log_id.index
            let keys: Vec<u64> = {
                let mut keys = Vec::new();
                for item in table.range(log_id.index..).map_err(write_err)? {
                    let (key_guard, _) = item.map_err(write_err)?;
                    keys.push(key_guard.value());
                }
                keys
            };
            for key in keys {
                table.remove(key).map_err(write_err)?;
            }
        }
        txn.commit().map_err(write_err)?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let txn = self.db.begin_write().map_err(write_err)?;
        {
            let mut meta = txn.open_table(META_TABLE).map_err(write_err)?;

            // Validate purge is forward-only (inside the write txn to avoid TOCTOU).
            let current_purged: Option<LogId<u64>> =
                match meta.get(LAST_PURGED_KEY).map_err(write_err)? {
                    Some(guard) => Some(postcard::from_bytes(guard.value()).map_err(write_err)?),
                    None => None,
                };
            if current_purged.as_ref() > Some(&log_id) {
                return Err(StorageError::IO {
                    source: StorageIOError::write_logs(&std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "purge log_id {log_id:?} is older than last_purged {current_purged:?}"
                        ),
                    )),
                });
            }

            // Update last_purged metadata.
            let purge_bytes = postcard::to_stdvec(&log_id).map_err(write_err)?;
            meta.insert(LAST_PURGED_KEY, purge_bytes.as_slice())
                .map_err(write_err)?;

            // Remove purged entries from log table.
            let mut log = txn.open_table(LOG_TABLE).map_err(write_err)?;
            let keys: Vec<u64> = {
                let mut keys = Vec::new();
                for item in log.range(..=log_id.index).map_err(write_err)? {
                    let (key_guard, _) = item.map_err(write_err)?;
                    keys.push(key_guard.value());
                }
                keys
            };
            for key in keys {
                log.remove(key).map_err(write_err)?;
            }
        }
        txn.commit().map_err(write_err)?;

        debug!(index = log_id.index, "Purged Raft log entries");
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        match committed {
            Some(ref lid) => self.write_meta(COMMITTED_KEY, lid),
            None => {
                // Remove committed key if None.
                let txn = self.db.begin_write().map_err(write_err)?;
                {
                    let mut table = txn.open_table(META_TABLE).map_err(write_err)?;
                    table.remove(COMMITTED_KEY).map_err(write_err)?;
                }
                txn.commit().map_err(write_err)?;
                Ok(())
            }
        }
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        self.read_meta(COMMITTED_KEY)
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::entry::RaftEntry;
    use openraft::{LogId, Vote};
    use tempfile::NamedTempFile;

    // Use MetaTypeConfig for tests (defined in store.rs).
    use crate::store::MetaTypeConfig;
    type TestEntry = Entry<MetaTypeConfig>;

    fn log_id(term: u64, index: u64) -> LogId<u64> {
        LogId::new(openraft::CommittedLeaderId::new(term, 0), index)
    }

    fn blank_entry(lid: LogId<u64>) -> TestEntry {
        TestEntry::new_blank(lid)
    }

    fn open_test_store() -> DurableLogStore<MetaTypeConfig> {
        let tmp = NamedTempFile::new().expect("tmpfile");
        DurableLogStore::open(tmp.path()).expect("open")
    }

    #[tokio::test]
    async fn durable_append_and_read() {
        let mut store = open_test_store();

        // Append entries via test helper (bypasses LogFlushed).
        let txn = store.db.begin_write().unwrap();
        {
            let mut table = txn.open_table(LOG_TABLE).unwrap();
            for i in 1..=3 {
                let entry = blank_entry(log_id(1, i));
                let data = postcard::to_stdvec(&entry).unwrap();
                table.insert(i, data.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();

        let read = store.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(read.len(), 3);
        assert_eq!(read[0].get_log_id().index, 1);
        assert_eq!(read[2].get_log_id().index, 3);
    }

    #[tokio::test]
    async fn durable_log_state() {
        let mut store = open_test_store();

        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());
        assert!(state.last_purged_log_id.is_none());

        // Insert entries directly.
        let txn = store.db.begin_write().unwrap();
        {
            let mut table = txn.open_table(LOG_TABLE).unwrap();
            for i in 1..=2 {
                let entry = blank_entry(log_id(1, i));
                let data = postcard::to_stdvec(&entry).unwrap();
                table.insert(i, data.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 2);
    }

    #[tokio::test]
    async fn durable_truncate() {
        let mut store = open_test_store();

        let txn = store.db.begin_write().unwrap();
        {
            let mut table = txn.open_table(LOG_TABLE).unwrap();
            for i in 1..=3 {
                let entry = blank_entry(log_id(1, i));
                let data = postcard::to_stdvec(&entry).unwrap();
                table.insert(i, data.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();

        store.truncate(log_id(1, 2)).await.unwrap();

        let read = store.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(read.len(), 1); // only entry 1 remains
        assert_eq!(read[0].get_log_id().index, 1);
    }

    #[tokio::test]
    async fn durable_purge() {
        let mut store = open_test_store();

        let txn = store.db.begin_write().unwrap();
        {
            let mut table = txn.open_table(LOG_TABLE).unwrap();
            for i in 1..=3 {
                let entry = blank_entry(log_id(1, i));
                let data = postcard::to_stdvec(&entry).unwrap();
                table.insert(i, data.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();

        store.purge(log_id(1, 2)).await.unwrap();

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 2);

        let read = store.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(read.len(), 1); // only entry 3 remains
    }

    #[tokio::test]
    async fn durable_vote_roundtrip() {
        let mut store = open_test_store();

        assert!(store.read_vote().await.unwrap().is_none());

        let vote = Vote::new(1, 0);
        store.save_vote(&vote).await.unwrap();
        assert_eq!(store.read_vote().await.unwrap().unwrap(), vote);
    }

    #[tokio::test]
    async fn durable_committed_roundtrip() {
        let mut store = open_test_store();

        assert!(store.read_committed().await.unwrap().is_none());

        let lid = log_id(1, 5);
        store.save_committed(Some(lid)).await.unwrap();
        assert_eq!(store.read_committed().await.unwrap().unwrap(), lid);

        // Clear committed.
        store.save_committed(None).await.unwrap();
        assert!(store.read_committed().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn durable_persistence_across_reopen() {
        let tmp = NamedTempFile::new().expect("tmpfile");
        let path = tmp.path().to_path_buf();

        // Write data, then drop the store.
        {
            let mut store = DurableLogStore::<MetaTypeConfig>::open(&path).unwrap();
            let vote = Vote::new(2, 1);
            store.save_vote(&vote).await.unwrap();

            let txn = store.db.begin_write().unwrap();
            {
                let mut table = txn.open_table(LOG_TABLE).unwrap();
                let entry = blank_entry(log_id(2, 10));
                let data = postcard::to_stdvec(&entry).unwrap();
                table.insert(10u64, data.as_slice()).unwrap();
            }
            txn.commit().unwrap();

            store.save_committed(Some(log_id(2, 10))).await.unwrap();
        }

        // Reopen and verify data survived.
        {
            let mut store = DurableLogStore::<MetaTypeConfig>::open(&path).unwrap();

            let vote = store.read_vote().await.unwrap().unwrap();
            assert_eq!(vote, Vote::new(2, 1));

            let state = store.get_log_state().await.unwrap();
            assert_eq!(state.last_log_id.unwrap().index, 10);

            let committed = store.read_committed().await.unwrap().unwrap();
            assert_eq!(committed.index, 10);
        }
    }

    #[tokio::test]
    async fn durable_purge_rejects_backward() {
        let mut store = open_test_store();

        let txn = store.db.begin_write().unwrap();
        {
            let mut table = txn.open_table(LOG_TABLE).unwrap();
            for i in 1..=5 {
                let entry = blank_entry(log_id(1, i));
                let data = postcard::to_stdvec(&entry).unwrap();
                table.insert(i, data.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();

        store.purge(log_id(1, 3)).await.unwrap();

        // Purging to an older log_id should fail.
        let result = store.purge(log_id(1, 1)).await;
        assert!(result.is_err());
    }
}
