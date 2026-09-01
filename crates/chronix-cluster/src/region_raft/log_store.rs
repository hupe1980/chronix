//! In-memory Raft log storage for a single data region.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{LogId, RaftLogId, RaftLogReader, StorageError, StorageIOError};
use tokio::sync::Mutex;

use super::{RegionEntry, RegionTypeConfig};

// ── Log Store ───────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct RegionLogStoreInner {
    last_purged_log_id: Option<LogId<u64>>,
    log: BTreeMap<u64, RegionEntry>,
    committed: Option<LogId<u64>>,
    vote: Option<openraft::Vote<u64>>,
}

/// In-memory Raft log store for a single region.
///
/// Follows the same pattern as the metadata [`MetaLogStore`](chronix_meta::MetaLogStore)
/// but stores region write commands instead of metadata commands.
#[derive(Clone, Debug, Default)]
pub struct RegionLogStore {
    inner: Arc<Mutex<RegionLogStoreInner>>,
}

impl RaftLogReader<RegionTypeConfig> for RegionLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<RegionEntry>, StorageError<u64>> {
        let inner = self.inner.lock().await;
        let entries = inner.log.range(range).map(|(_, ent)| ent.clone()).collect();
        Ok(entries)
    }
}

impl RaftLogStorage<RegionTypeConfig> for RegionLogStore {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> std::result::Result<LogState<RegionTypeConfig>, StorageError<u64>> {
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

    async fn save_vote(
        &mut self,
        vote: &openraft::Vote<u64>,
    ) -> std::result::Result<(), StorageError<u64>> {
        self.inner.lock().await.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> std::result::Result<Option<openraft::Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().await.vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<RegionTypeConfig>,
    ) -> std::result::Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = RegionEntry> + Send,
        I::IntoIter: Send,
    {
        let mut inner = self.inner.lock().await;
        for entry in entries {
            inner.log.insert(entry.get_log_id().index, entry);
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> std::result::Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in keys {
            inner.log.remove(&key);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> std::result::Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        if inner.last_purged_log_id.as_ref() > Some(&log_id) {
            let msg = format!(
                "purge log_id {log_id:?} is behind last_purged {:?}",
                inner.last_purged_log_id
            );
            return Err(StorageError::IO {
                source: StorageIOError::write_logs(&std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    msg,
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
    ) -> std::result::Result<(), StorageError<u64>> {
        self.inner.lock().await.committed = committed;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> std::result::Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().await.committed)
    }
}

// ── Test helper ─────────────────────────────────────────────────────

#[cfg(test)]
impl RegionLogStore {
    /// Test helper: append entries directly (bypasses `LogFlushed`
    /// callback which is `pub(crate)` in openraft).
    pub(crate) async fn test_append(&mut self, entries: Vec<RegionEntry>) {
        let mut inner = self.inner.lock().await;
        for entry in entries {
            inner.log.insert(entry.get_log_id().index, entry);
        }
    }
}
