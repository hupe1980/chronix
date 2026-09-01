//! Durable CDC event log with append-only file storage.
//!
//! **** Provides at-least-once delivery for CDC events by persisting
//! them to an append-only log file before broadcasting. Subscribers can resume
//! from a specific sequence number after a crash via [`DurableEventLog::replay_from`].
//!
//! ## Design
//!
//! Events are written as length-prefixed JSON frames:
//!
//! ```text
//! [4 bytes: payload length (little-endian u32)][N bytes: JSON payload]
//! ```
//!
//! An in-memory index maps sequence numbers to file offsets for fast
//! random-access replay. The index is rebuilt on startup by scanning
//! the log file.
//!
//! ## Retention
//!
//! The log supports size-based truncation: when the file exceeds
//! `max_log_bytes`, the oldest entries are discarded by rewriting
//! a compacted log.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::cdc::event::{CdcEvent, SequenceNumber};

/// Default maximum log file size before compaction (256 MiB).
pub const DEFAULT_MAX_LOG_BYTES: u64 = 256 * 1024 * 1024;

/// Durable append-only CDC event log.
///
/// Events are persisted to disk before being broadcast, enabling
/// crash-recovery replay via [`replay_from`](Self::replay_from).
pub struct DurableEventLog {
    /// Path to the log file.
    path: PathBuf,
    /// Writer for appending events.
    writer: BufWriter<File>,
    /// Sequence-number → file-offset index for replay.
    index: BTreeMap<SequenceNumber, u64>,
    /// Current write offset in the file.
    write_offset: u64,
    /// Maximum log size before compaction.
    max_log_bytes: u64,
    /// Lowest sequence number currently retained.
    min_seq: SequenceNumber,
}

impl DurableEventLog {
    /// Opens or creates a durable event log at the given path.
    ///
    /// On startup, the log file is scanned to rebuild the in-memory index.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_limit(path, DEFAULT_MAX_LOG_BYTES)
    }

    /// Opens or creates a durable event log with a custom size limit.
    pub fn open_with_limit(path: impl AsRef<Path>, max_log_bytes: u64) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;

        let mut log = Self {
            path,
            writer: BufWriter::new(file),
            index: BTreeMap::new(),
            write_offset: 0,
            max_log_bytes,
            min_seq: 0,
        };

        log.rebuild_index()?;
        Ok(log)
    }

    /// Appends an event to the durable log.
    ///
    /// The event is serialized, written to disk, and flushed before returning.
    /// This ensures the event is durable before being broadcast.
    pub fn append(&mut self, event: &CdcEvent) -> io::Result<()> {
        let payload =
            serde_json::to_vec(event).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        // Checked cast prevents silent truncation on >4 GiB payloads.
        let len = u32::try_from(payload.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "event payload exceeds u32::MAX",
            )
        })?;

        let offset = self.write_offset;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&payload)?;
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;

        let seq = event.seq();
        self.index.insert(seq, offset);
        self.write_offset += 4 + payload.len() as u64;

        if self.min_seq == 0 {
            self.min_seq = seq;
        }

        // Trigger compaction if we exceed the size limit.
        if self.write_offset > self.max_log_bytes {
            self.compact()?;
        }

        Ok(())
    }

    /// Appends a batch of events with a single flush.
    ///
    /// All events are serialized and written to disk, then flushed once.
    /// This amortises the fsync cost across the entire batch instead of
    /// paying one syscall per event.
    pub fn append_batch(&mut self, events: &[CdcEvent]) -> io::Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        for event in events {
            let payload = serde_json::to_vec(event)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            // Checked cast prevents silent truncation.
            let len = u32::try_from(payload.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "event payload exceeds u32::MAX",
                )
            })?;

            let offset = self.write_offset;
            self.writer.write_all(&len.to_le_bytes())?;
            self.writer.write_all(&payload)?;

            let seq = event.seq();
            self.index.insert(seq, offset);
            self.write_offset += 4 + payload.len() as u64;

            if self.min_seq == 0 {
                self.min_seq = seq;
            }
        }

        // Single flush + fsync for the entire batch.
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;

        // Trigger compaction if we exceed the size limit.
        if self.write_offset > self.max_log_bytes {
            self.compact()?;
        }

        Ok(())
    }

    /// Replays all events from the given sequence number (inclusive).
    ///
    /// Returns events in sequence order. If `from_seq` is older than the
    /// oldest retained event, replay starts from the oldest available.
    pub fn replay_from(&self, from_seq: SequenceNumber) -> io::Result<Vec<CdcEvent>> {
        let mut events = Vec::new();

        let mut reader = BufReader::new(File::open(&self.path)?);

        for (&seq, &offset) in self.index.range(from_seq..) {
            reader.seek(SeekFrom::Start(offset))?;
            let mut len_buf = [0u8; 4];
            reader.read_exact(&mut len_buf)?;
            let len = u32::from_le_bytes(len_buf) as usize;

            let mut payload = vec![0u8; len];
            reader.read_exact(&mut payload)?;

            let event: CdcEvent = serde_json::from_slice(&payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            debug_assert_eq!(event.seq(), seq);
            events.push(event);
        }

        Ok(events)
    }

    /// Returns the sequence number range currently retained in the log.
    pub fn retained_range(&self) -> (SequenceNumber, SequenceNumber) {
        let min = self.index.keys().next().copied().unwrap_or(0);
        let max = self.index.keys().next_back().copied().unwrap_or(0);
        (min, max)
    }

    /// Returns the number of events currently in the log.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Returns true if the log contains no events.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Returns the current log file size in bytes.
    pub fn file_size(&self) -> u64 {
        self.write_offset
    }

    /// Rebuilds the in-memory index by scanning the log file.
    fn rebuild_index(&mut self) -> io::Result<()> {
        let file = File::open(&self.path)?;
        let file_len = file.metadata()?.len();
        let mut reader = BufReader::new(file);
        let mut offset: u64 = 0;

        self.index.clear();

        while offset < file_len {
            let record_start = offset;
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            offset += 4;

            if offset + len as u64 > file_len {
                // Truncated record at end of file — skip
                tracing::warn!(
                    offset = record_start,
                    expected_len = len,
                    "truncated record at end of CDC event log, skipping"
                );
                break;
            }

            let mut payload = vec![0u8; len];
            reader.read_exact(&mut payload)?;
            offset += len as u64;

            match serde_json::from_slice::<CdcEvent>(&payload) {
                Ok(event) => {
                    let seq = event.seq();
                    self.index.insert(seq, record_start);
                    if self.min_seq == 0 || seq < self.min_seq {
                        self.min_seq = seq;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        offset = record_start,
                        error = %e,
                        "corrupt CDC event log entry, skipping"
                    );
                }
            }
        }

        self.write_offset = offset;
        Ok(())
    }

    /// Compacts the log by retaining only the most recent half of events.
    ///
    /// Writes a new log file, atomically replaces the old one, then
    /// reopens for append.
    fn compact(&mut self) -> io::Result<()> {
        let total = self.index.len();
        if total < 2 {
            return Ok(());
        }

        // Keep the newest half of events
        let keep_from_idx = total / 2;
        let seqs: Vec<SequenceNumber> = self.index.keys().copied().collect();
        let keep_from_seq = seqs[keep_from_idx];

        let tmp_path = self.path.with_extension("tmp");
        {
            let reader_file = File::open(&self.path)?;
            let mut reader = BufReader::new(reader_file);
            let mut tmp_writer = BufWriter::new(
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&tmp_path)?,
            );

            let mut new_index = BTreeMap::new();
            let mut new_offset: u64 = 0;

            for (&seq, &offset) in self.index.range(keep_from_seq..) {
                reader.seek(SeekFrom::Start(offset))?;
                let mut len_buf = [0u8; 4];
                reader.read_exact(&mut len_buf)?;
                let len = u32::from_le_bytes(len_buf) as usize;

                let mut payload = vec![0u8; len];
                reader.read_exact(&mut payload)?;

                new_index.insert(seq, new_offset);
                tmp_writer.write_all(&len_buf)?;
                tmp_writer.write_all(&payload)?;
                new_offset += 4 + len as u64;
            }

            tmp_writer.flush()?;
            tmp_writer.get_ref().sync_all()?;
        }

        // Atomic replace
        fs::rename(&tmp_path, &self.path)?;

        // Rebuild writer and index
        let file = OpenOptions::new().append(true).open(&self.path)?;
        let file_len = file.metadata()?.len();
        self.writer = BufWriter::new(file);
        self.write_offset = file_len;

        // Rebuild index from scratch for correctness
        self.rebuild_index()?;

        let retained = self.index.len();
        tracing::info!(
            compacted_from = total,
            retained,
            file_size = self.write_offset,
            "CDC event log compacted"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chronix_core::types::FieldValue;

    use super::*;

    fn make_event(seq: u64) -> CdcEvent {
        let mut event = CdcEvent::PointWritten {
            measurement: "cpu".into(),
            tags: BTreeMap::from([("host".into(), "srv1".into())]),
            fields: BTreeMap::from([("value".into(), FieldValue::F64(42.0))]),
            timestamp: seq as i64 * 1_000_000_000,
            seq: 0,
        };
        event.set_seq(seq);
        event
    }

    #[test]
    fn append_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cdc.log");

        {
            let mut log = DurableEventLog::open(&path).unwrap();
            for i in 1..=10 {
                log.append(&make_event(i)).unwrap();
            }
            assert_eq!(log.len(), 10);
            assert_eq!(log.retained_range(), (1, 10));
        }

        // Reopen and verify index rebuild
        {
            let log = DurableEventLog::open(&path).unwrap();
            assert_eq!(log.len(), 10);

            let events = log.replay_from(5).unwrap();
            assert_eq!(events.len(), 6); // 5,6,7,8,9,10
            assert_eq!(events[0].seq(), 5);
            assert_eq!(events[5].seq(), 10);
        }
    }

    #[test]
    fn replay_from_start() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cdc.log");

        let mut log = DurableEventLog::open(&path).unwrap();
        for i in 1..=5 {
            log.append(&make_event(i)).unwrap();
        }

        let events = log.replay_from(1).unwrap();
        assert_eq!(events.len(), 5);
    }

    #[test]
    fn replay_future_seq_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cdc.log");

        let mut log = DurableEventLog::open(&path).unwrap();
        log.append(&make_event(1)).unwrap();

        let events = log.replay_from(100).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn empty_log() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cdc.log");

        let log = DurableEventLog::open(&path).unwrap();
        assert!(log.is_empty());
        assert_eq!(log.len(), 0);

        let events = log.replay_from(1).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn compaction_triggered_by_size_limit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cdc.log");

        // Use a tiny limit to trigger compaction
        let mut log = DurableEventLog::open_with_limit(&path, 512).unwrap();

        for i in 1..=100 {
            log.append(&make_event(i)).unwrap();
        }

        // After compaction, some events should be trimmed
        assert!(log.len() < 100, "expected compaction to trim events");
        assert!(!log.is_empty(), "should retain some events");

        // Remaining events should be replayable
        let (min, max) = log.retained_range();
        assert!(min > 1, "oldest events should be compacted away");
        assert_eq!(max, 100);

        let events = log.replay_from(min).unwrap();
        assert_eq!(events.len(), log.len());
    }

    #[test]
    fn crash_recovery_truncated_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cdc.log");

        // Write some valid events
        {
            let mut log = DurableEventLog::open(&path).unwrap();
            for i in 1..=5 {
                log.append(&make_event(i)).unwrap();
            }
        }

        // Append a partial/corrupt record (length header but no body)
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&100u32.to_le_bytes()).unwrap(); // says 100 bytes follow
            f.write_all(b"short").unwrap(); // but only 5 bytes
        }

        // Reopen — should recover the 5 good events
        let log = DurableEventLog::open(&path).unwrap();
        assert_eq!(log.len(), 5);
        let events = log.replay_from(1).unwrap();
        assert_eq!(events.len(), 5);
    }
}
