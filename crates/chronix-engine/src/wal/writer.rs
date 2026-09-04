//! WAL writer — append records, manage rotation, and enforce fsync policies.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

use chronix_core::{FsyncPolicy, WalConfig, WalError};

use crate::wal::{
    WalRecordType, WAL_HEADER_SIZE, WAL_MAGIC, WAL_PAYLOAD_VERSION, WAL_RECORD_HEADER_SIZE,
    WAL_VERSION,
};

/// Constructs the WAL filename for a given starting sequence number.
pub(crate) fn wal_filename(seq_start: u64) -> String {
    format!("wal_{seq_start:020}.cxwl")
}

/// Write-Ahead Log writer.
///
/// Appends records to WAL files, manages rotation, and enforces the configured
/// fsync policy. Thread-safe: internal state is protected by a mutex.
///
/// # Group commit
///
/// When using [`FsyncPolicy::PerBatch`], the [`append_durable`](Self::append_durable)
/// method enables group commit: concurrent callers write their records under the
/// write lock, then coordinate so that a single `fsync` covers all pending
/// writes. The first waiter becomes the *sync leader* and performs the flush;
/// subsequent waiters piggy-back on the leader's sync.
pub struct WalWriter {
    /// WAL data directory.
    dir: PathBuf,
    /// Configuration.
    config: WalConfig,
    /// Mutable writer state (protected by mutex for thread safety).
    inner: Mutex<WalWriterInner>,
    /// Global sequence counter (atomic for lock-free reads).
    next_sequence: AtomicU64,
    /// Group commit coordination state.
    group_sync: Mutex<GroupSyncState>,
    /// Condition variable for group commit waiters.
    group_sync_cvar: Condvar,
    /// Shutdown flag for the periodic sync thread.
    periodic_shutdown: Arc<AtomicBool>,
    /// Handle to the periodic sync thread (if running).
    periodic_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Number of `fsync` calls issued against the WAL data file.
    ///
    /// Exported as `chronix_wal_fsync_total`. On flash storage the fsync rate
    /// is the wear rate, which is what [`FsyncPolicy::Periodic`] bounds — and
    /// an unobservable bound is one nobody can check.
    fsync_count: AtomicU64,
}

/// Coordination state for group-commit syncs.
struct GroupSyncState {
    /// Highest sequence number that has been durably synced to disk.
    synced_up_to: u64,
    /// Whether a sync is currently in progress.
    sync_in_progress: bool,
}

struct WalWriterInner {
    /// Current WAL file writer.
    writer: BufWriter<File>,
    /// Path of the current WAL file.
    current_path: PathBuf,
    /// First sequence number in the current file.
    file_start_seq: u64,
    /// Current file size in bytes.
    file_size: u64,
    /// Highest sequence number that has been fully written to the buffer.
    /// Used by group_sync to avoid marking un-written records as durable.
    max_written_seq: u64,
    /// Poison flag — set after an unrecoverable I/O error.
    /// Once poisoned, all subsequent writes are rejected immediately.
    ///
    /// # Recovery
    ///
    /// The poison flag is **not** clearable at runtime — by design.  Once
    /// the underlying file is in an unknown state (e.g. a failed truncation
    /// after a partial write), any further writes risk silent corruption.
    /// The correct recovery procedure is:
    ///
    /// 1. Drop the poisoned `WalWriter`.
    /// 2. Optionally inspect / repair the WAL directory on disk.
    /// 3. Re-open via [`WalWriter::open`], which replays and validates
    ///    existing records before resuming writes.
    poisoned: bool,
    /// Cached count of WAL files in the directory. Maintained
    /// incrementally during rotation and truncation to avoid `readdir`
    /// on every rotation.
    cached_file_count: usize,
}

impl WalWriter {
    /// Open or create a WAL writer in the given directory.
    ///
    /// If existing WAL files are present, the writer resumes from the last
    /// sequence number. Otherwise, a new WAL file is created starting at
    /// sequence 1.
    ///
    /// # Errors
    ///
    /// Returns [`WalError`] if the directory cannot be created or files
    /// cannot be opened.
    #[allow(clippy::needless_pass_by_value)] // config is cheap and owned by the writer conceptually
    pub fn open(dir: impl Into<PathBuf>, config: WalConfig) -> Result<Self, WalError> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;

        // Scan existing WAL files to find the highest sequence number
        let (start_seq, max_seq) = Self::scan_existing_files(&dir)?;
        // Seed the cached file count from disk at startup
        let initial_file_count = list_wal_files(&dir)?.len();

        let next_seq = if max_seq > 0 { max_seq + 1 } else { 1 };
        let file_start_seq = if start_seq > 0 { next_seq } else { 1 };

        let path = dir.join(wal_filename(file_start_seq));
        let (writer, file_size) = Self::create_wal_file(&path)?;
        // The new file counts too
        let cached_file_count = if initial_file_count > 0 && start_seq > 0 {
            // Existing files + the new file being created now
            initial_file_count + 1
        } else {
            // Fresh start — just the initial file
            1
        };

        let periodic_shutdown = Arc::new(AtomicBool::new(false));

        let wal = Self {
            dir,
            config: config.clone(),
            inner: Mutex::new(WalWriterInner {
                writer,
                current_path: path,
                file_start_seq,
                file_size,
                max_written_seq: if max_seq > 0 { max_seq } else { 0 },
                poisoned: false,
                cached_file_count,
            }),
            next_sequence: AtomicU64::new(next_seq),
            group_sync: Mutex::new(GroupSyncState {
                synced_up_to: 0,
                sync_in_progress: false,
            }),
            group_sync_cvar: Condvar::new(),
            periodic_shutdown: periodic_shutdown.clone(),
            periodic_handle: Mutex::new(None),
            fsync_count: AtomicU64::new(0),
        };

        Ok(wal)
    }

    /// Returns `true` if the WAL writer has been poisoned due to an
    /// unrecoverable I/O error.
    pub fn is_poisoned(&self) -> bool {
        self.inner.lock().poisoned
    }

    /// Clear the poison flag after operator-verified recovery.
    ///
    /// Re-opens the current WAL file for appending to ensure the
    /// underlying file descriptor is in a known-good state. Returns
    /// an error if the WAL file cannot be reopened.
    ///
    /// # Safety (logical)
    ///
    /// Call this only after verifying the WAL directory is intact
    /// (e.g. via `WalReader` replay). Clearing poison on a
    /// genuinely corrupt WAL risks silent data loss.
    pub fn clear_poison(&self) -> Result<(), WalError> {
        let mut inner = self.inner.lock();
        if !inner.poisoned {
            return Ok(());
        }
        // Re-open the file to get a fresh fd in a known state
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&inner.current_path)?;
        let file_size = file.metadata()?.len();
        inner.writer = std::io::BufWriter::new(file);
        inner.file_size = file_size;
        inner.poisoned = false;
        tracing::warn!("WAL poison flag cleared — writer resumed");
        Ok(())
    }

    /// Append a single payload to the WAL.
    ///
    /// Returns the assigned sequence number.
    ///
    /// # Errors
    ///
    /// Returns [`WalError`] on I/O failure, if the WAL is full, or if the
    /// internal lock was poisoned.
    pub fn append(&self, payload: &[u8]) -> Result<u64, WalError> {
        // Pre-compress outside the lock to reduce critical section duration.
        // CRC32c stays under the lock (hardware-accelerated, negligible cost).
        let prepared: std::borrow::Cow<'_, [u8]> = if self.config.compress {
            std::borrow::Cow::Owned(crate::wal::compress_wal_payload(payload))
        } else {
            std::borrow::Cow::Borrowed(payload)
        };

        let mut inner = self.inner.lock();

        let seq = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        self.maybe_rotate(&mut inner, seq)?;
        Self::write_record(&mut inner, seq, &prepared, false, WalRecordType::Data)?;

        if self.config.fsync_policy == FsyncPolicy::PerWrite {
            inner.writer.flush()?;
            self.sync_locked(&mut inner)?;
        }

        // Emit WAL metrics after state change.
        metrics::gauge!("chronix_wal_sequence_number").set(seq as f64);
        metrics::gauge!("chronix_wal_file_count").set(inner.cached_file_count as f64);
        metrics::gauge!("chronix_wal_current_file_bytes").set(inner.file_size as f64);

        Ok(seq)
    }

    /// Append a batch of payloads as a single atomic WAL record, followed by
    /// a single fsync (when using `PerBatch` policy).
    ///
    /// All payloads are serialized into a batch-framed compound record whose
    /// CRC covers the entire batch. On crash recovery, either all
    /// sub-payloads survive (CRC valid) or none (CRC mismatch / truncation),
    /// guaranteeing all-or-nothing batch atomicity.
    ///
    /// Uses the same group-commit pattern as `append_durable`:
    /// the write lock is released before fsync, allowing concurrent writers to
    /// proceed while the sync leader fsyncs for all pending records.
    ///
    /// Returns the sequence number of the batch record.
    ///
    /// # Errors
    ///
    /// Returns [`WalError`] on I/O failure or if the internal lock was
    /// poisoned.
    pub fn append_batch(&self, payloads: &[&[u8]]) -> Result<u64, WalError> {
        if payloads.is_empty() {
            return Ok(self.next_sequence.load(Ordering::Relaxed).saturating_sub(1));
        }

        // Encode + compress outside the lock to reduce critical section
        // duration.  Only the sequential buffer write + CRC (hardware-accelerated)
        // remain under the mutex.
        let batch_payload = crate::wal::encode_batch_payload(payloads);
        let prepared = if self.config.compress {
            crate::wal::compress_wal_payload(&batch_payload)
        } else {
            batch_payload
        };

        // Phase 1: Write under lock, then release.
        let seq;
        {
            let mut inner = self.inner.lock();

            seq = self.next_sequence.fetch_add(1, Ordering::Relaxed);

            self.maybe_rotate(&mut inner, seq)?;
            Self::write_record(&mut inner, seq, &prepared, false, WalRecordType::Batch)?;

            // `PerWrite` syncs inline; `Periodic` deliberately does not —
            // its syncs are coalesced onto the background thread. Syncing
            // here made `Periodic` behave exactly like `PerWrite`, so the
            // flash-wear preset bought nothing, and this path disagreed with
            // `append` about what one policy meant.
            if self.config.fsync_policy == FsyncPolicy::PerWrite {
                inner.writer.flush()?;
                self.sync_locked(&mut inner)?;

                metrics::gauge!("chronix_wal_sequence_number").set(seq as f64);
                metrics::gauge!("chronix_wal_file_count").set(inner.cached_file_count as f64);
                metrics::gauge!("chronix_wal_current_file_bytes").set(inner.file_size as f64);

                return Ok(seq);
            }

            // Emit WAL metrics (lock still held, before release).
            metrics::gauge!("chronix_wal_sequence_number").set(seq as f64);
            metrics::gauge!("chronix_wal_file_count").set(inner.cached_file_count as f64);
            metrics::gauge!("chronix_wal_current_file_bytes").set(inner.file_size as f64);
        }
        // Write lock released — other writers can proceed.

        // Phase 2: Group sync (PerBatch only) — amortises fsync across
        // concurrent callers instead of serializing through the mutex.
        if self.config.fsync_policy == FsyncPolicy::PerBatch {
            self.group_sync(seq)?;
        }

        Ok(seq)
    }

    /// Append a single payload and ensure it is durably committed.
    ///
    /// With [`FsyncPolicy::PerBatch`] this participates in **group commit**:
    /// the calling thread writes its record and then either becomes the *sync
    /// leader* (flushing and calling `fsync` for all pending records) or waits
    /// for an in-progress sync to complete. This amortises the cost of `fsync`
    /// across concurrent callers.
    ///
    /// With [`FsyncPolicy::PerWrite`] this behaves identically to
    /// [`append`](Self::append).
    ///
    /// With [`FsyncPolicy::Periodic`] the record is written and then
    /// **unconditionally** flushed + synced, because "durable" means the
    /// caller requires the data to be on stable storage before returning
    ///.
    ///
    /// # Errors
    ///
    /// Returns [`WalError`] on I/O failure, if the WAL is full, or if the
    /// internal lock was poisoned.
    pub fn append_durable(&self, payload: &[u8]) -> Result<u64, WalError> {
        // Pre-compress outside the lock to reduce critical section duration.
        let prepared: std::borrow::Cow<'_, [u8]> = if self.config.compress {
            std::borrow::Cow::Owned(crate::wal::compress_wal_payload(payload))
        } else {
            std::borrow::Cow::Borrowed(payload)
        };

        // Phase 1: assign sequence AND write under the same lock to prevent
        // out-of-order writes that would violate group_sync durability.
        let seq;
        {
            let mut inner = self.inner.lock();

            seq = self.next_sequence.fetch_add(1, Ordering::Relaxed);
            self.maybe_rotate(&mut inner, seq)?;
            Self::write_record(&mut inner, seq, &prepared, false, WalRecordType::Data)?;

            // PerWrite and Periodic: sync immediately while we still hold
            // the lock. "Durable" must mean durable regardless of
            // the fsync policy — this is the path a delete takes, where the
            // caller has been told the record is on stable storage before it
            // returns. The ordinary batch path above honours `Periodic`.
            if matches!(
                self.config.fsync_policy,
                FsyncPolicy::PerWrite | FsyncPolicy::Periodic(_)
            ) {
                inner.writer.flush()?;
                self.sync_locked(&mut inner)?;
                return Ok(seq);
            }
        }
        // Write lock released — other writers can proceed.

        // Phase 2: group sync (PerBatch only).
        if self.config.fsync_policy == FsyncPolicy::PerBatch {
            self.group_sync(seq)?;
        }

        Ok(seq)
    }

    /// Coordinate a group sync for the given sequence number.
    ///
    /// If no sync is in progress, the caller becomes the sync leader and
    /// performs the flush + fsync. Concurrent callers wait on the condition
    /// variable and piggy-back on the leader's sync.
    ///
    /// Waiters use a bounded wait (5 s) to avoid indefinite hangs when the
    /// sync leader stalls on disk I/O.  On timeout the waiter promotes
    /// itself to sync leader and retries.
    fn group_sync(&self, need_seq: u64) -> Result<(), WalError> {
        use std::time::Duration;
        // Use configurable timeout instead of hardcoded constant.
        let group_sync_timeout = Duration::from_secs(self.config.group_sync_timeout_secs);

        let mut state = self.group_sync.lock();

        // Another leader already synced past our sequence: nothing to do.
        if state.synced_up_to >= need_seq {
            return Ok(());
        }

        // Wait for an in-progress sync — it might cover our sequence.
        while state.sync_in_progress {
            let timed_out = self
                .group_sync_cvar
                .wait_for(&mut state, group_sync_timeout);

            if state.synced_up_to >= need_seq {
                return Ok(());
            }

            // On timeout, break out and become the new sync leader. The
            // previous leader is presumed stalled.
            if timed_out.timed_out() {
                break;
            }
        }

        // We are the sync leader.
        state.sync_in_progress = true;
        drop(state);

        // Perform the actual flush + fsync under the write lock.
        let result = (|| -> Result<u64, WalError> {
            let mut inner = self.inner.lock();
            inner.writer.flush()?;
            self.sync_locked(&mut inner)?;
            // Return the max sequence that was actually written to the buffer,
            // NOT next_sequence which may include records not yet written.
            Ok(inner.max_written_seq)
        })();

        // Update state and wake all waiters.
        let mut state = self.group_sync.lock();
        state.sync_in_progress = false;
        if let Ok(synced_seq) = result {
            // Only mark sequences as synced that were actually written + fsynced.
            state.synced_up_to = state.synced_up_to.max(synced_seq);
        }

        // `notify_all` is intentional here — not a thundering-herd bug.
        // Every waiting writer needs to re-check whether its *own* sequence
        // number was covered by the sync that just completed.  Writers whose
        // sequence exceeds `synced_up_to` will loop back and become the next
        // sync leader.  `notify_one` would risk leaving satisfied waiters
        // unaware while unsatisfied ones remain blocked.
        self.group_sync_cvar.notify_all();

        result.map(|_| ())
    }

    /// Flush the WAL buffer and sync to disk.
    ///
    /// # Errors
    ///
    /// Returns [`WalError`] on I/O failure or if the internal lock was
    /// poisoned.
    pub fn sync(&self) -> Result<(), WalError> {
        let mut inner = self.inner.lock();
        inner.writer.flush()?;
        self.sync_locked(&mut inner)
    }

    /// `fsync` the data file and count it. Caller holds `inner`.
    ///
    /// A failed `fsync` poisons the writer. After one, the kernel may have
    /// discarded dirty pages, so the file's contents are unknown — and the
    /// records the failed call covered are still in the file, where a
    /// *later* successful sync (by a group-commit follower, or the periodic
    /// thread) would make them durable after the caller was told they were
    /// not. Refusing further writes is the only honest state.
    fn sync_locked(&self, inner: &mut WalWriterInner) -> Result<(), WalError> {
        let started = std::time::Instant::now();
        if let Err(e) = inner.writer.get_ref().sync_data() {
            inner.poisoned = true;
            tracing::error!(error = %e, "WAL fsync failed — writer poisoned");
            return Err(e.into());
        }
        self.fsync_count.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("chronix_wal_fsync_total").increment(1);
        metrics::histogram!("chronix_wal_write_duration_seconds")
            .record(started.elapsed().as_secs_f64());
        Ok(())
    }

    /// Number of `fsync` calls issued against the WAL data file.
    ///
    /// The observable form of the fsync policy: with
    /// [`FsyncPolicy::Periodic`] this grows with elapsed time, not with the
    /// number of appends.
    #[must_use]
    pub fn fsync_count(&self) -> u64 {
        self.fsync_count.load(Ordering::Relaxed)
    }

    /// Start the background periodic sync thread.
    ///
    /// When the WAL is configured with `FsyncPolicy::Periodic(interval)`,
    /// this spawns a dedicated thread that calls `flush` + `fsync` at the
    /// configured interval. The thread runs until `shutdown_periodic_sync`
    /// is called or the `WalWriter` is dropped.
    ///
    /// No-op if the policy is not `Periodic`.
    pub fn start_periodic_sync(self: &Arc<Self>) {
        if let FsyncPolicy::Periodic(interval) = self.config.fsync_policy {
            // FINDING-16 fix: reject zero-duration intervals which would
            // create a CPU-burning busy loop.
            let interval = if interval.is_zero() {
                tracing::warn!(
                    "FsyncPolicy::Periodic(Duration::ZERO) would cause a busy loop, \
                     defaulting to 100ms"
                );
                std::time::Duration::from_millis(100)
            } else {
                interval
            };

            let shutdown = Arc::clone(&self.periodic_shutdown);
            let writer = Arc::clone(self);
            let handle = std::thread::Builder::new()
                .name("wal-periodic-sync".into())
                .spawn(move || {
                    while !shutdown.load(Ordering::Relaxed) {
                        std::thread::sleep(interval);
                        if shutdown.load(Ordering::Relaxed) {
                            break;
                        }
                        if let Err(e) = writer.sync() {
                            tracing::warn!(error = %e, "periodic WAL fsync failed");
                        }
                    }
                });
            // Log error instead of panicking on thread spawn failure.
            match handle {
                Ok(h) => {
                    let mut guard = self.periodic_handle.lock();
                    *guard = Some(h);
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "Failed to spawn WAL periodic sync thread — \
                         periodic fsync disabled for this writer"
                    );
                }
            }
        }
    }

    /// Stop the periodic sync thread and wait for it to finish.
    pub fn shutdown_periodic_sync(&self) {
        self.periodic_shutdown.store(true, Ordering::Relaxed);
        let mut guard = self.periodic_handle.lock();
        if let Some(handle) = guard.take() {
            let _ = handle.join();
        }
    }

    /// Force rotation to a new WAL file.
    ///
    /// # Errors
    ///
    /// Returns [`WalError`] on I/O failure or if the internal lock was
    /// poisoned.
    pub fn rotate(&self) -> Result<(), WalError> {
        let mut inner = self.inner.lock();
        let next_seq = self.next_sequence.load(Ordering::Acquire);
        self.force_rotate(&mut inner, next_seq)?;
        Ok(())
    }

    /// Delete WAL files whose maximum sequence number is ≤ the threshold.
    ///
    /// Never deletes the active WAL file. Idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`WalError`] on I/O failure or if the internal lock was
    /// poisoned.
    pub fn truncate_before(&self, sequence_no: u64) -> Result<usize, WalError> {
        let inner = self.inner.lock();
        let current_path = inner.current_path.clone();
        drop(inner);

        let mut entries = list_wal_files(&self.dir)?;
        entries.sort();

        let mut deleted = 0;
        // Use windows(2) to get each file's max seq from the next file's start
        // sequence, avoiding the O(n²) position lookup.
        for pair in entries.windows(2) {
            let (file_start, path) = &pair[0];
            let (next_start, _) = &pair[1];

            // Never delete the current active file.
            if *path == current_path {
                continue;
            }

            let max_seq_in_file = next_start.saturating_sub(1);

            if max_seq_in_file <= sequence_no {
                tracing::info!(
                    path = %path.display(),
                    seq_range = %format!("{file_start}..={max_seq_in_file}"),
                    "truncating WAL file"
                );
                fs::remove_file(path)?;
                deleted += 1;
            }
        }

        // Fsync the WAL directory so that file removals are durable.
        // Without this, a crash could resurrect deleted WAL files.
        if deleted > 0 {
            let dir_file = fs::File::open(&self.dir)?;
            dir_file.sync_all()?;

            // Update cached file count
            let mut inner = self.inner.lock();
            inner.cached_file_count = inner.cached_file_count.saturating_sub(deleted);

            // Emit WAL metrics after truncation.
            metrics::gauge!("chronix_wal_file_count").set(inner.cached_file_count as f64);
        }

        Ok(deleted)
    }

    /// Delete WAL files older than `max_age` based on file modification time.
    ///
    /// This provides a time-based compaction strategy complementing the
    /// sequence-based [`truncate_before`](Self::truncate_before). Useful when
    /// callers want to bound WAL retention by wall-clock time rather than (or
    /// in addition to) sequence numbers.
    ///
    /// Never deletes the active WAL file. Idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`WalError`] on I/O failure.
    pub fn truncate_before_age(&self, max_age: Duration) -> Result<usize, WalError> {
        let inner = self.inner.lock();
        let current_path = inner.current_path.clone();
        drop(inner);

        let entries = list_wal_files(&self.dir)?;
        let now = std::time::SystemTime::now();

        let mut deleted = 0;
        for (_file_start, path) in &entries {
            // Never delete the current active file.
            if *path == current_path {
                continue;
            }

            let metadata = fs::metadata(path)?;
            let modified = metadata.modified().map_err(|e| {
                WalError::Io(io::Error::other(format!(
                    "cannot read mtime of {}: {e}",
                    path.display()
                )))
            })?;

            let age = now.duration_since(modified).unwrap_or(Duration::ZERO);
            if age >= max_age {
                tracing::info!(
                    path = %path.display(),
                    age_secs = age.as_secs(),
                    max_age_secs = max_age.as_secs(),
                    "truncating WAL file (age-based)"
                );
                fs::remove_file(path)?;
                deleted += 1;
            }
        }

        // Fsync the WAL directory so that file removals are durable.
        if deleted > 0 {
            let dir_file = fs::File::open(&self.dir)?;
            dir_file.sync_all()?;

            // Update cached file count
            let mut inner = self.inner.lock();
            inner.cached_file_count = inner.cached_file_count.saturating_sub(deleted);
        }

        Ok(deleted)
    }

    /// Archive WAL files whose maximum sequence ≤ `sequence_no` to `archive_dir`,
    /// then delete the originals.
    ///
    /// This is the foundation for **Point-in-Time Recovery (PITR)**: WAL files
    /// are safely copied to an archive before removal, enabling future replays
    /// from the archive. The archive directory is created if it doesn't exist.
    ///
    /// Never archives the active WAL file. Returns the number of files archived.
    ///
    /// # Errors
    ///
    /// Returns [`WalError`] on I/O failure. If the copy succeeds but the
    /// delete fails, the archive copy is retained (safe — duplicates are
    /// tolerated during replay thanks to sequence monotonicity checks).
    pub fn archive_before(&self, sequence_no: u64, archive_dir: &Path) -> Result<usize, WalError> {
        let inner = self.inner.lock();
        let current_path = inner.current_path.clone();
        drop(inner);

        fs::create_dir_all(archive_dir)?;

        let mut entries = list_wal_files(&self.dir)?;
        entries.sort();

        let mut archived = 0;

        for pair in entries.windows(2) {
            let (file_start, path) = &pair[0];
            let (next_start, _) = &pair[1];

            if *path == current_path {
                continue;
            }

            let max_seq_in_file = next_start.saturating_sub(1);

            if max_seq_in_file <= sequence_no {
                let file_name = path.file_name().ok_or_else(|| {
                    WalError::Io(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "WAL file has no filename",
                    ))
                })?;
                let archive_path = archive_dir.join(file_name);

                // Copy to archive first — if this fails, no data is lost.
                fs::copy(path, &archive_path)?;

                tracing::info!(
                    path = %path.display(),
                    archive = %archive_path.display(),
                    seq_range = %format!("{file_start}..={max_seq_in_file}"),
                    "archived WAL file"
                );

                // Delete original — safe because archive copy succeeded.
                fs::remove_file(path)?;
                archived += 1;
            }
        }

        if archived > 0 {
            // Fsync both directories for durability.
            let dir_file = fs::File::open(&self.dir)?;
            dir_file.sync_all()?;
            let archive_file = fs::File::open(archive_dir)?;
            archive_file.sync_all()?;

            let mut inner = self.inner.lock();
            inner.cached_file_count = inner.cached_file_count.saturating_sub(archived);
            metrics::gauge!("chronix_wal_file_count").set(inner.cached_file_count as f64);
            metrics::counter!("chronix_wal_archived_total").increment(archived as u64);
        }

        Ok(archived)
    }

    /// Approximate heap bytes held by the writer's own buffers.
    ///
    /// The `BufWriter`'s capacity, which is what stands between an append and
    /// an `fsync`. It is one of the three terms the memtable budget does not
    /// count, and unlike the other two it is fixed rather than a function of
    /// the workload — so it is worth having as a number precisely because it
    /// is easy to forget.
    #[must_use]
    pub fn memory_bytes(&self) -> usize {
        self.inner.lock().writer.capacity()
    }

    /// Returns the current sequence number (the last assigned).
    #[inline]
    #[must_use]
    pub fn current_sequence(&self) -> u64 {
        self.next_sequence.load(Ordering::Acquire).saturating_sub(1)
    }

    /// Returns the path to the WAL directory.
    #[inline]
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Returns the number of WAL files in the directory.
    ///
    /// # Errors
    ///
    /// Returns [`WalError`] if the directory cannot be read.
    pub fn file_count(&self) -> Result<usize, WalError> {
        // The writer maintains this count across rotation and truncation;
        // the directory is listed once, at open. Listing it here made every
        // `insert()` pay a `readdir` for its admission check.
        Ok(self.inner.lock().cached_file_count)
    }

    /// Returns the total size in bytes of all WAL files on disk.
    ///
    /// # Errors
    ///
    /// Returns [`WalError`] if the directory cannot be read.
    pub fn total_bytes(&self) -> Result<u64, WalError> {
        let files = list_wal_files(&self.dir)?;
        let mut total: u64 = 0;
        for (_, path) in &files {
            if let Ok(meta) = fs::metadata(path) {
                total += meta.len();
            }
        }
        Ok(total)
    }

    /// Emit WAL gauge metrics for monitoring lag and resource usage.
    ///
    /// Emits `chronix_wal_file_count`, `chronix_wal_total_bytes`, and
    /// `chronix_wal_sequence_number` gauges.
    pub fn emit_metrics(&self) {
        let seq = self.current_sequence();
        metrics::gauge!("chronix_wal_sequence_number").set(seq as f64);

        let inner = self.inner.lock();
        metrics::gauge!("chronix_wal_file_count").set(inner.cached_file_count as f64);
        metrics::gauge!("chronix_wal_current_file_bytes").set(inner.file_size as f64);
    }

    /// Explicitly close the WAL writer, flushing all data to stable storage.
    ///
    /// Unlike the implicit [`Drop`] cleanup, this method returns I/O errors
    /// so close failures are not silently ignored.
    ///
    /// The shutdown sequence is:
    /// 1. Stop the periodic sync thread (if running).
    /// 2. Flush all buffered data to the operating system.
    /// 3. Issue `sync_all` to commit data and metadata to stable storage.
    ///
    /// # Close-path efficiency
    ///
    /// This method takes ownership of `self` (`mut self`), which guarantees
    /// exclusive access.  The inner state is obtained via
    /// [`Mutex::get_mut`](parking_lot::Mutex::get_mut) (no-lock fast path),
    /// so flush and fsync execute **outside any lock scope**.  This prevents
    /// mutex contention with concurrent readers or writers during shutdown.
    ///
    /// # Errors
    ///
    /// Returns [`WalError`] if the flush or fsync fails.
    pub fn close(mut self) -> Result<(), WalError> {
        // Phase 1: stop background threads so no further syncs can race.
        self.shutdown_periodic_sync();
        // Phase 2: flush + sync without locking (get_mut requires &mut self,
        // which we have since close() took ownership).
        let inner = self.inner.get_mut();
        inner.writer.flush()?;
        inner.writer.get_ref().sync_all()?;
        Ok(())
    }

    // ── Internal ──────────────────────────────────────────────────────

    fn create_wal_file(path: &Path) -> Result<(BufWriter<File>, u64), WalError> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        // 64 KiB buffer reduces syscall overhead by ~8× vs the
        // default 8 KiB, which is significant for write-heavy workloads.
        let mut writer = BufWriter::with_capacity(64 * 1024, file);

        // Write header
        writer.write_all(WAL_MAGIC)?;
        writer.write_all(&WAL_VERSION.to_le_bytes())?;
        writer.flush()?;
        // Ensure the header is durable before any records are appended.
        writer.get_ref().sync_data()?;

        // Fsync parent directory so the new file's directory entry is durable.
        if let Some(parent) = path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                dir.sync_all().map_err(|e| {
                    WalError::Io(std::io::Error::new(
                        e.kind(),
                        format!(
                            "failed to fsync WAL parent directory {}: {e}",
                            parent.display()
                        ),
                    ))
                })?;
            }
        }

        Ok((writer, WAL_HEADER_SIZE as u64))
    }

    /// Maximum payload size per WAL record (256 MiB, matching reader limit).
    const MAX_PAYLOAD_SIZE: usize = 256 * 1024 * 1024;

    fn write_record(
        inner: &mut WalWriterInner,
        seq: u64,
        payload: &[u8],
        compress: bool,
        record_type: WalRecordType,
    ) -> Result<(), WalError> {
        // Reject writes after an unrecoverable I/O error.
        if inner.poisoned {
            return Err(WalError::Poisoned);
        }

        if payload.len() > Self::MAX_PAYLOAD_SIZE {
            return Err(WalError::PayloadTooLarge {
                size: payload.len(),
                limit: Self::MAX_PAYLOAD_SIZE,
            });
        }

        // Optionally LZ4-compress the payload before writing.
        let payload: std::borrow::Cow<'_, [u8]> = if compress {
            std::borrow::Cow::Owned(crate::wal::compress_wal_payload(payload))
        } else {
            std::borrow::Cow::Borrowed(payload)
        };

        #[allow(clippy::cast_possible_truncation)]
        let length = payload.len() as u32;

        // Compute CRC incrementally over [length, sequence_no, record_type,
        // payload_version, payload] without allocating a temporary Vec.
        let type_ver_bytes = [record_type as u8, WAL_PAYLOAD_VERSION];
        let crc = crc32c::crc32c(&length.to_le_bytes());
        let crc = crc32c::crc32c_append(crc, &seq.to_le_bytes());
        let crc = crc32c::crc32c_append(crc, &type_ver_bytes);
        let crc = crc32c::crc32c_append(crc, &payload);

        // Save pre-write position so we can truncate on partial failure.
        // This prevents corrupted half-records from poisoning subsequent writes.
        let saved_pos = inner.file_size;

        let write_result = (|| -> Result<(), io::Error> {
            inner.writer.write_all(&crc.to_le_bytes())?;
            inner.writer.write_all(&length.to_le_bytes())?;
            inner.writer.write_all(&seq.to_le_bytes())?;
            inner.writer.write_all(&type_ver_bytes)?;
            inner.writer.write_all(&payload)?;
            Ok(())
        })();

        if let Err(e) = write_result {
            // Attempt to truncate back to the pre-write position to prevent
            // a partial header from corrupting the remainder of the file.
            tracing::error!(
                seq = seq,
                offset = saved_pos,
                error = %e,
                "WAL write_record failed mid-write, truncating to pre-write position"
            );
            // Flush any partial data in the BufWriter before truncating.
            let flush_ok = inner.writer.flush().is_ok();
            // Truncate the underlying file back to the saved position.
            let trunc_ok = inner.writer.get_mut().set_len(saved_pos).is_ok();
            // Seek to the truncation point so subsequent writes are correct.
            let seek_ok = inner.writer.seek(io::SeekFrom::Start(saved_pos)).is_ok();
            inner.file_size = saved_pos;

            // If recovery truncation itself failed, the file is in
            // an unknown state — poison the writer to prevent further damage.
            if !flush_ok || !trunc_ok || !seek_ok {
                tracing::error!(
                    "WAL recovery truncation failed — writer poisoned to prevent corruption"
                );
                inner.poisoned = true;
            }

            return Err(WalError::Io(e));
        }

        inner.file_size += WAL_RECORD_HEADER_SIZE as u64 + payload.len() as u64;
        // Track the highest fully-written sequence for group_sync correctness.
        if seq > inner.max_written_seq {
            inner.max_written_seq = seq;
        }

        Ok(())
    }

    fn maybe_rotate(&self, inner: &mut WalWriterInner, next_seq: u64) -> Result<(), WalError> {
        if inner.file_size >= self.config.max_file_size as u64 {
            self.force_rotate(inner, next_seq)?;
        }
        Ok(())
    }

    fn force_rotate(&self, inner: &mut WalWriterInner, next_seq: u64) -> Result<(), WalError> {
        // Use cached file count instead of readdir on every rotation
        if inner.cached_file_count >= self.config.max_unflushed_wals {
            return Err(WalError::Full {
                count: inner.cached_file_count,
                limit: self.config.max_unflushed_wals,
            });
        }

        // Flush and sync current file
        inner.writer.flush()?;
        self.sync_locked(inner)?;

        // Create new file
        let new_path = self.dir.join(wal_filename(next_seq));
        let (writer, file_size) = Self::create_wal_file(&new_path)?;

        tracing::info!(
            old_path = %inner.current_path.display(),
            new_path = %new_path.display(),
            next_seq = next_seq,
            "WAL rotated"
        );

        inner.writer = writer;
        inner.current_path = new_path;
        inner.file_start_seq = next_seq;
        inner.file_size = file_size;
        inner.cached_file_count += 1;

        Ok(())
    }

    fn scan_existing_files(dir: &Path) -> Result<(u64, u64), WalError> {
        let files = list_wal_files(dir)?;
        if files.is_empty() {
            return Ok((0, 0));
        }

        let min_start = files.iter().map(|(s, _)| *s).min().unwrap_or(0);

        // Use the maximum file-start sequence as the lower bound for max_seq.
        // This prevents sequence reuse when the last WAL file is empty (e.g.
        // crash during rotation): even without readable records, we know
        // sequences up to at least max_file_start - 1 were issued.
        let max_file_start = files.iter().map(|(s, _)| *s).max().unwrap_or(0);
        let mut max_seq = max_file_start.max(min_start);

        // Scan backward through files to find the highest sequence number
        // from actual records (handles empty/corrupt last file gracefully).
        // Only track the last valid record per file since sequences
        // are monotonically increasing — no need to scan all records.
        for (_, path) in files.iter().rev() {
            let reader = match crate::wal::reader::WalReader::open(path) {
                Ok(r) => r,
                Err(_) => continue,
            };
            let mut found_any = false;
            let mut last_seq = 0u64;
            for record in reader {
                match record {
                    Ok(r) => {
                        last_seq = r.sequence_no;
                        found_any = true;
                    }
                    Err(_) => break, // truncated tail
                }
            }
            if found_any {
                max_seq = max_seq.max(last_seq);
                break; // highest records are in the last readable file
            }
        }

        Ok((min_start, max_seq))
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        // Flush buffered data and sync to disk on orderly
        // shutdown.  Uses `get_mut()` (not `lock()`) because `&mut self`
        // guarantees exclusive access — no mutex contention during Drop.
        // Without this, up to 64 KiB of buffered writes can be silently lost.
        //
        // Wrap in catch_unwind so a panic here (e.g. poisoned mutex,
        // unexpected I/O failure) does not trigger a double-panic abort if
        // Drop runs during stack unwinding.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let inner = self.inner.get_mut();
            let _ = inner.writer.flush();
            let _ = inner.writer.get_ref().sync_all();
            self.periodic_shutdown.store(true, Ordering::Relaxed);
            let mut guard = self.periodic_handle.lock();
            if let Some(handle) = guard.take() {
                let _ = handle.join();
            }
        }));
        if result.is_err() {
            eprintln!("chronix: panic during WalWriter::drop suppressed to avoid abort");
        }
    }
}

/// List all WAL files in a directory, sorted by starting sequence number.
///
/// Returns `(starting_sequence, path)` pairs.
pub(crate) fn list_wal_files(dir: &Path) -> Result<Vec<(u64, PathBuf)>, WalError> {
    let mut files = Vec::new();

    if !dir.exists() {
        return Ok(files);
    }

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with("wal_")
                && path
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("cxwl"))
            {
                if let Some(seq_str) = name
                    .strip_prefix("wal_")
                    .and_then(|s| s.strip_suffix(".cxwl"))
                {
                    if let Ok(seq) = seq_str.parse::<u64>() {
                        files.push((seq, path));
                    }
                }
            }
        }
    }

    files.sort_by_key(|(seq, _)| *seq);
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_config() -> WalConfig {
        WalConfig {
            fsync_policy: FsyncPolicy::PerBatch,
            max_file_size: 32 * 1024 * 1024,
            max_unflushed_wals: 10,
            compress: true,
            ..WalConfig::default()
        }
    }

    fn small_config() -> WalConfig {
        WalConfig {
            fsync_policy: FsyncPolicy::PerBatch,
            max_file_size: 200, // Very small to trigger rotation
            max_unflushed_wals: 10,
            compress: false, // Disable compression so file sizes are predictable
            ..WalConfig::default()
        }
    }

    #[test]
    fn writer_creates_directory() {
        let dir = TempDir::new().unwrap();
        let wal_dir = dir.path().join("subdir").join("wal");
        let _writer = WalWriter::open(&wal_dir, test_config()).unwrap();
        assert!(wal_dir.exists());
    }

    #[test]
    fn writer_creates_initial_file() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();
        assert_eq!(writer.file_count().unwrap(), 1);
    }

    #[test]
    fn append_single_record() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();

        let seq = writer.append(b"hello world").unwrap();
        assert_eq!(seq, 1);

        let seq = writer.append(b"second").unwrap();
        assert_eq!(seq, 2);

        assert_eq!(writer.current_sequence(), 2);
    }

    /// `Periodic` must coalesce syncs onto the background thread, not issue
    /// one per append.
    ///
    /// It did the opposite on the batch path — the path every write to the
    /// database takes — which made `FsyncPolicy::Periodic(5s)` identical to
    /// `PerWrite`. The preset exists for flash-backed embedded storage, where
    /// the fsync rate is the wear rate, so "identical to `PerWrite`" is the
    /// whole cost the setting was chosen to avoid. The single-record `append`
    /// honoured the policy, so the two paths disagreed about what one policy
    /// meant, and nothing compared them.
    #[test]
    fn periodic_fsync_does_not_sync_on_every_batch() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            // Long enough that the background thread cannot fire during the run.
            fsync_policy: FsyncPolicy::Periodic(Duration::from_secs(3600)),
            ..Default::default()
        };
        let writer = Arc::new(WalWriter::open(dir.path(), config).unwrap());

        let payload = b"periodic-batch";
        for _ in 0..64 {
            writer.append_batch(&[payload]).unwrap();
        }

        assert_eq!(
            writer.fsync_count(),
            0,
            "Periodic must not fsync per batch — that is PerWrite's contract"
        );
    }

    /// The other half: the background thread has to actually run, or
    /// `Periodic` means "never sync" rather than "sync on an interval".
    #[test]
    fn periodic_fsync_thread_syncs_on_its_interval() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            fsync_policy: FsyncPolicy::Periodic(Duration::from_millis(20)),
            ..Default::default()
        };
        let writer = Arc::new(WalWriter::open(dir.path(), config).unwrap());
        writer.start_periodic_sync();

        writer.append_batch(&[b"x".as_slice()]).unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while writer.fsync_count() == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        writer.shutdown_periodic_sync();

        assert!(
            writer.fsync_count() > 0,
            "the periodic sync thread never fsynced"
        );
    }

    /// `PerWrite` still syncs every append — the corroborating half, so that
    /// "Periodic does not sync" cannot pass by the counter being broken.
    #[test]
    fn per_write_fsyncs_every_batch() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            fsync_policy: FsyncPolicy::PerWrite,
            ..Default::default()
        };
        let writer = Arc::new(WalWriter::open(dir.path(), config).unwrap());
        for _ in 0..8 {
            writer.append_batch(&[b"y".as_slice()]).unwrap();
        }
        assert_eq!(writer.fsync_count(), 8, "PerWrite must fsync per append");
    }

    #[test]
    fn append_batch() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();

        let payloads: Vec<&[u8]> = vec![b"one", b"two", b"three"];
        let batch_seq = writer.append_batch(&payloads).unwrap();
        // Batch is written as a single atomic record.
        assert_eq!(batch_seq, 1);
        assert_eq!(writer.current_sequence(), 1);

        // Verify the batch record can be read back and decoded.
        let records: Vec<_> = crate::wal::WalReader::open(
            &crate::wal::writer::list_wal_files(dir.path()).unwrap()[0].1,
        )
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
        assert_eq!(records.len(), 1);
        let sub = crate::wal::decode_batch_payload(&records[0].payload).unwrap();
        assert_eq!(sub.len(), 3);
        assert_eq!(sub[0], b"one");
        assert_eq!(sub[1], b"two");
        assert_eq!(sub[2], b"three");
    }

    #[test]
    fn append_batch_empty() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();
        let last_seq = writer.append_batch(&[]).unwrap();
        assert_eq!(last_seq, 0);
    }

    #[test]
    fn rotation_on_size() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), small_config()).unwrap();

        // max_file_size=200; header=6, each record = 16 + 100 = 116 bytes.
        // After 1st append: 6 + 116 = 122 (< 200).
        // After 2nd append: 122 + 116 = 238 (>= 200 but rotation checks *before* write).
        // 3rd append sees file_size=238 >= 200 → triggers rotation.
        let payload = vec![0u8; 100];
        writer.append(&payload).unwrap();
        writer.append(&payload).unwrap();
        writer.append(&payload).unwrap(); // Triggers rotation

        assert!(writer.file_count().unwrap() >= 2);
    }

    #[test]
    fn manual_rotation() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();

        writer.append(b"before").unwrap();
        writer.rotate().unwrap();
        writer.append(b"after").unwrap();

        assert_eq!(writer.file_count().unwrap(), 2);
    }

    #[test]
    fn truncation_removes_old_files() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), small_config()).unwrap();

        // Write enough to create multiple files
        let payload = vec![0u8; 100];
        for _ in 0..5 {
            writer.append(&payload).unwrap();
        }

        let initial_count = writer.file_count().unwrap();
        assert!(
            initial_count >= 2,
            "Expected multiple files, got {initial_count}"
        );

        // Truncate up to sequence 2
        let deleted = writer.truncate_before(2).unwrap();
        assert!(deleted > 0, "Expected some files deleted");

        let final_count = writer.file_count().unwrap();
        assert!(final_count < initial_count);
    }

    #[test]
    fn truncation_never_removes_active_file() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();

        writer.append(b"data").unwrap();
        writer.truncate_before(u64::MAX).unwrap();

        // Active file must remain
        assert_eq!(writer.file_count().unwrap(), 1);
    }

    #[test]
    fn truncation_idempotent() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), small_config()).unwrap();

        let payload = vec![0u8; 100];
        for _ in 0..5 {
            writer.append(&payload).unwrap();
        }

        let count1 = writer.truncate_before(2).unwrap();
        let count2 = writer.truncate_before(2).unwrap();
        assert_eq!(count2, 0, "Second truncation should be a no-op");
        assert!(count1 > 0 || count2 == 0);
    }

    #[test]
    fn truncation_by_age_removes_old_files() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), small_config()).unwrap();

        let payload = vec![0u8; 100];
        for _ in 0..5 {
            writer.append(&payload).unwrap();
        }

        let initial_count = writer.file_count().unwrap();
        assert!(
            initial_count >= 2,
            "Expected multiple files, got {initial_count}"
        );

        // All files are brand-new, so max_age of 0 seconds should delete
        // all non-active files.
        let deleted = writer.truncate_before_age(Duration::from_secs(0)).unwrap();
        assert!(deleted > 0, "Expected some files deleted by age");

        let final_count = writer.file_count().unwrap();
        assert!(final_count < initial_count);
        // Active file must remain
        assert!(final_count >= 1);
    }

    #[test]
    fn truncation_by_age_preserves_recent_files() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), small_config()).unwrap();

        let payload = vec![0u8; 100];
        for _ in 0..5 {
            writer.append(&payload).unwrap();
        }

        let initial_count = writer.file_count().unwrap();
        assert!(initial_count >= 2);

        // max_age of 1 hour — no files should be old enough to delete.
        let deleted = writer
            .truncate_before_age(Duration::from_secs(3600))
            .unwrap();
        assert_eq!(deleted, 0, "No files should be old enough");

        assert_eq!(writer.file_count().unwrap(), initial_count);
    }

    #[test]
    fn resume_from_existing_files() {
        let dir = TempDir::new().unwrap();
        let dir_path = dir.path().to_path_buf();

        // Write some records
        {
            let writer = WalWriter::open(&dir_path, test_config()).unwrap();
            writer.append(b"one").unwrap();
            writer.append(b"two").unwrap();
            writer.append(b"three").unwrap();
            writer.sync().unwrap();
        }

        // Reopen — should resume from sequence 4
        {
            let writer = WalWriter::open(&dir_path, test_config()).unwrap();
            let seq = writer.append(b"four").unwrap();
            assert_eq!(seq, 4);
        }
    }

    #[test]
    fn per_write_fsync() {
        let dir = TempDir::new().unwrap();
        let config = WalConfig {
            fsync_policy: FsyncPolicy::PerWrite,
            max_file_size: 32 * 1024 * 1024,
            max_unflushed_wals: 10,
            compress: true,
            ..WalConfig::default()
        };
        let writer = WalWriter::open(dir.path(), config).unwrap();
        let seq = writer.append(b"important data").unwrap();
        assert_eq!(seq, 1);
    }

    #[test]
    fn wal_full_error() {
        let dir = TempDir::new().unwrap();
        let config = WalConfig {
            fsync_policy: FsyncPolicy::PerBatch,
            max_file_size: 50,     // Very small
            max_unflushed_wals: 2, // Very low limit
            compress: true,
            ..WalConfig::default()
        };
        let writer = WalWriter::open(dir.path(), config).unwrap();

        // Fill up WAL files
        let payload = vec![0u8; 30];
        let mut hit_full = false;
        for _ in 0..20 {
            match writer.append(&payload) {
                Err(WalError::Full { .. }) => {
                    hit_full = true;
                    break;
                }
                Ok(_) => {}
                Err(e) => panic!("Unexpected error: {e}"),
            }
        }
        assert!(hit_full, "Expected WalError::Full");
    }

    #[test]
    fn wal_filename_format() {
        assert_eq!(wal_filename(1), "wal_00000000000000000001.cxwl");
        assert_eq!(wal_filename(42), "wal_00000000000000000042.cxwl");
    }

    #[test]
    fn append_durable_single_thread() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();

        let seq1 = writer.append_durable(b"alpha").unwrap();
        let seq2 = writer.append_durable(b"beta").unwrap();
        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);

        // Records should be durable — readable immediately after replay.
        let records = crate::wal::replay_all(dir.path()).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].payload, b"alpha");
        assert_eq!(records[1].payload, b"beta");
    }

    #[test]
    fn append_durable_per_write() {
        let dir = TempDir::new().unwrap();
        let config = WalConfig {
            fsync_policy: FsyncPolicy::PerWrite,
            ..test_config()
        };
        let writer = WalWriter::open(dir.path(), config).unwrap();

        let seq = writer.append_durable(b"important").unwrap();
        assert_eq!(seq, 1);

        let records = crate::wal::replay_all(dir.path()).unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn group_commit_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let dir = TempDir::new().unwrap();
        let writer = Arc::new(WalWriter::open(dir.path(), test_config()).unwrap());

        let num_threads = 8;
        let writes_per_thread = 50;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let w = Arc::clone(&writer);
                thread::spawn(move || {
                    for i in 0..writes_per_thread {
                        let payload = format!("t{t}_w{i}");
                        w.append_durable(payload.as_bytes()).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Ensure all buffered data is flushed to disk before replay.
        writer.sync().unwrap();

        // All records should be present and durable.
        let records = crate::wal::replay_all(dir.path()).unwrap();
        assert_eq!(records.len(), num_threads * writes_per_thread);

        // Sequence numbers should be unique and contiguous.
        let mut seqs: Vec<u64> = records.iter().map(|r| r.sequence_no).collect();
        seqs.sort_unstable();
        seqs.dedup();
        assert_eq!(seqs.len(), num_threads * writes_per_thread);
    }

    #[test]
    fn periodic_sync_background_thread() {
        use std::sync::Arc;
        use std::time::Duration;

        let dir = TempDir::new().unwrap();
        let config = WalConfig {
            fsync_policy: FsyncPolicy::Periodic(Duration::from_millis(50)),
            ..test_config()
        };
        let writer = Arc::new(WalWriter::open(dir.path(), config).unwrap());
        writer.start_periodic_sync();

        // Write records without explicit sync
        writer.append(b"periodic-1").unwrap();
        writer.append(b"periodic-2").unwrap();

        // Poll rather than sleep for a fixed span. The property is that the
        // background thread *eventually* makes the records durable, not that
        // it does so within one interval: a 50 ms interval read after an 80 ms
        // sleep leaves 30 ms of margin, and a loaded runner does not provide
        // it. This failed on macOS with zero records replayed.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let records = loop {
            let records = crate::wal::replay_all(dir.path()).unwrap();
            if records.len() == 2 || std::time::Instant::now() >= deadline {
                break records;
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        assert_eq!(
            records.len(),
            2,
            "the periodic sync thread never made the records durable"
        );

        // Clean shutdown
        writer.shutdown_periodic_sync();
    }

    #[test]
    fn archive_before_copies_and_deletes() {
        let dir = TempDir::new().unwrap();
        let archive_dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), small_config()).unwrap();

        // Write enough to create multiple files
        let payload = vec![0u8; 100];
        for _ in 0..6 {
            writer.append(&payload).unwrap();
        }
        writer.sync().unwrap();

        let files_before = writer.file_count().unwrap();
        assert!(files_before >= 2, "Need multiple files, got {files_before}");

        // Archive all files with max_seq <= 3
        let archived = writer.archive_before(3, archive_dir.path()).unwrap();
        assert!(archived >= 1, "Should archive at least 1 file");

        // Verify archive dir has the files
        let archive_files = list_wal_files(archive_dir.path()).unwrap();
        assert_eq!(archive_files.len(), archived);

        // Verify original files were removed
        let files_after = writer.file_count().unwrap();
        assert_eq!(files_after, files_before - archived);

        // Verify archived records are replayable
        let archived_records = crate::wal::replay_all(archive_dir.path()).unwrap();
        assert!(!archived_records.is_empty());
        for r in &archived_records {
            assert!(r.sequence_no <= 3);
        }
    }

    #[test]
    fn archive_before_never_archives_active() {
        let dir = TempDir::new().unwrap();
        let archive_dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();

        // Only 1 file (the active one)
        writer.append(b"data").unwrap();
        writer.sync().unwrap();
        assert_eq!(writer.file_count().unwrap(), 1);

        let archived = writer.archive_before(u64::MAX, archive_dir.path()).unwrap();
        assert_eq!(archived, 0, "Should not archive the active file");
    }

    #[test]
    fn archive_creates_dir_if_missing() {
        let dir = TempDir::new().unwrap();
        let archive_base = TempDir::new().unwrap();
        let archive_dir = archive_base.path().join("nested").join("archive");

        let writer = WalWriter::open(dir.path(), small_config()).unwrap();
        let payload = vec![0u8; 100];
        for _ in 0..4 {
            writer.append(&payload).unwrap();
        }
        writer.sync().unwrap();

        // Should create the nested directory
        let _archived = writer.archive_before(2, &archive_dir).unwrap();
        assert!(archive_dir.exists());
    }

    #[test]
    fn archive_then_pitr_replay() {
        let dir = TempDir::new().unwrap();
        let archive_dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), small_config()).unwrap();

        let payload = vec![0u8; 100];
        for _ in 0..8 {
            writer.append(&payload).unwrap();
        }
        writer.sync().unwrap();

        // Archive files with seq <= 4
        let _archived = writer.archive_before(4, archive_dir.path()).unwrap();

        // Replay range from archive: records 2..=4
        let records = crate::wal::replay_range(archive_dir.path(), 1, 4).unwrap();
        assert!(!records.is_empty());
        for r in &records {
            assert!(r.sequence_no > 1 && r.sequence_no <= 4);
        }
    }

    // ── Poison flag tests ─────────────────────────────────

    #[test]
    fn is_poisoned_starts_false() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), small_config()).unwrap();
        assert!(!writer.is_poisoned());
    }

    #[test]
    fn clear_poison_noop_when_not_poisoned() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), small_config()).unwrap();
        writer.clear_poison().unwrap(); // should be no-op
        assert!(!writer.is_poisoned());
    }

    #[test]
    fn poisoned_writer_rejects_writes() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), small_config()).unwrap();
        // Write once to make sure writer is functional
        writer.append(b"hello").unwrap();

        // Manually set poison flag
        writer.inner.lock().poisoned = true;
        assert!(writer.is_poisoned());

        // All write paths should fail with Poisoned
        let err = writer.append(b"world").unwrap_err();
        assert!(matches!(err, WalError::Poisoned));

        let err = writer.append_batch(&[b"a" as &[u8]]).unwrap_err();
        assert!(matches!(err, WalError::Poisoned));
    }

    #[test]
    fn clear_poison_restores_writes() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), small_config()).unwrap();
        writer.append(b"before").unwrap();

        // Poison the writer
        writer.inner.lock().poisoned = true;
        assert!(writer.is_poisoned());
        assert!(writer.append(b"fail").is_err());

        // Clear poison
        writer.clear_poison().unwrap();
        assert!(!writer.is_poisoned());

        // Writes should work again
        writer.append(b"after").unwrap();
    }

    #[test]
    fn clear_poison_data_survives_replay() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), small_config()).unwrap();
        writer.append(b"record-1").unwrap();
        writer.sync().unwrap();

        // Poison + clear cycle
        writer.inner.lock().poisoned = true;
        writer.clear_poison().unwrap();
        writer.append(b"record-2").unwrap();
        writer.sync().unwrap();

        // Replay and verify both records are intact
        let records = crate::wal::replay_all(dir.path()).unwrap();
        assert!(records.len() >= 2);
        assert_eq!(&records[0].payload, b"record-1");
        assert_eq!(&records[1].payload, b"record-2");
    }
}
