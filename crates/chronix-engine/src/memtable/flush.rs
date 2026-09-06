//! Memtable freeze-and-flush mechanism.
//!
//! This module implements the lifecycle of a memtable: freeze the active
//! memtable (making it immutable), swap in a fresh one, flush the frozen
//! memtable to a segment file via [`SegmentWriter`], and optionally truncate
//! the WAL up to the flushed sequence number.
//!
//! # Flush triggers
//!
//! Flush is triggered when:
//! - `estimated_size > memtable_flush_threshold`
//! - A time-shard boundary is crossed
//!
//! # Memory bounds
//!
//! If the combined size of active + frozen memtables exceeds
//! `max_memtable_memory`, new inserts are rejected with
//! [`MemtableError::CapacityExceeded`].

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::segment::writer::{SegmentMeta, SegmentWriter, SegmentWriterConfig};
use chronix_core::types::Point;

use crate::memtable::error::{MemtableError, Result};
use crate::memtable::memtable::Memtable;

/// Configuration for the flush controller.
#[derive(Debug, Clone)]
pub struct FlushConfig {
    /// Memory threshold (bytes) that triggers a flush.
    pub flush_threshold: usize,
    /// Maximum total memory across active + frozen memtables.
    pub max_memory: usize,
    /// Soft limit as a fraction of `max_memory` (0.0–1.0) at which
    /// graduated write stall begins. Below this, writes proceed without delay.
    /// Between soft limit and hard limit, writes are delayed proportionally.
    /// Default: 0.75 (delays begin at 75% of max_memory).
    pub stall_soft_fraction: f64,
    /// Maximum write stall delay applied at the hard limit boundary.
    /// Default: 100ms.
    pub max_stall_delay: std::time::Duration,
    /// Maximum number of frozen memtables queued for flush.
    /// When the queue is full, `freeze_and_swap` rejects with `CapacityExceeded`.
    /// Default: 2.
    pub max_frozen_memtables: usize,
    /// Directory to write segment files.
    pub segment_dir: PathBuf,
    /// Configuration for the segment writer.
    pub segment_writer_config: SegmentWriterConfig,
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            flush_threshold: 64 * 1024 * 1024, // 64 MB
            max_memory: 256 * 1024 * 1024,     // 256 MB
            stall_soft_fraction: 0.75,
            max_stall_delay: std::time::Duration::from_millis(100),
            max_frozen_memtables: 2,
            segment_dir: PathBuf::from("/tmp/chronix/segments"),
            segment_writer_config: SegmentWriterConfig::default(),
        }
    }
}

/// Manages the active and frozen memtable lifecycle.
///
/// The controller holds an active memtable that accepts writes, and
/// optionally a frozen memtable that is being flushed. Writes go to the
/// active memtable; when the flush threshold is reached, the active
/// memtable is frozen and swapped for a fresh one.
pub struct FlushController {
    /// The currently active (writable) memtable.
    active: Arc<RwLock<Arc<Memtable>>>,
    /// Queue of frozen (immutable) memtables awaiting flush.
    frozen: Arc<RwLock<VecDeque<Arc<Memtable>>>>,
    /// Flush configuration.
    config: FlushConfig,
    /// Monotonically increasing segment counter for unique filenames.
    segment_counter: std::sync::atomic::AtomicU64,
    /// Serialises `flush_frozen_with`: the head of the frozen queue must not
    /// change between the peek that starts a flush and the pop that ends it.
    flush_lock: parking_lot::Mutex<()>,
}

impl std::fmt::Debug for FlushController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Read values into locals BEFORE the debug_struct chain to avoid
        // holding both `active` and `frozen` read locks simultaneously.
        // freeze_and_swap acquires locks in reverse order (frozen → active),
        // so holding both reads would create an ABBA deadlock risk with
        // parking_lot's write-preferring RwLock.
        let active_len = self.active.read().len();
        let has_frozen = !self.frozen.read().is_empty();
        let total_memory = self.total_memory();
        let counter = self
            .segment_counter
            .load(std::sync::atomic::Ordering::Relaxed);
        f.debug_struct("FlushController")
            .field("config", &self.config)
            .field("active_len", &active_len)
            .field("has_frozen", &has_frozen)
            .field("frozen_count", &self.frozen.read().len())
            .field("total_memory", &total_memory)
            .field("segment_counter", &counter)
            .finish()
    }
}

impl FlushController {
    /// Create a new flush controller with the given configuration.
    #[must_use]
    pub fn new(config: FlushConfig) -> Self {
        Self {
            active: Arc::new(RwLock::new(Arc::new(Memtable::new()))),
            frozen: Arc::new(RwLock::new(VecDeque::new())),
            config,
            segment_counter: std::sync::atomic::AtomicU64::new(0),
            flush_lock: parking_lot::Mutex::new(()),
        }
    }

    /// Get a reference to the active memtable.
    #[must_use]
    pub fn active_memtable(&self) -> Arc<Memtable> {
        Arc::clone(&self.active.read())
    }

    /// Get a reference to the frozen memtable, if any.
    #[must_use]
    pub fn frozen_memtable(&self) -> Option<Arc<Memtable>> {
        self.frozen.read().front().cloned()
    }

    /// Insert a point into the active memtable.
    ///
    /// # Errors
    ///
    /// Returns [`MemtableError::CapacityExceeded`] if the total memory
    /// (active + frozen) exceeds the configured maximum.
    pub fn insert(&self, point: &Point) -> Result<()> {
        self.check_capacity()?;
        self.active.read().insert(point)
    }

    /// Insert a point that is already durable in the WAL at `wal_seq`.
    ///
    /// **No capacity check.** Admission control runs *before* the WAL
    /// append; what the WAL holds is inserted unconditionally, because a
    /// record that is durable but in no memtable is a record the next
    /// flush's WAL floor may pass — and then it is lost. The same applies
    /// to replay at open: rejecting an acknowledged record for being over
    /// a memory cap is silent data loss, not backpressure.
    ///
    /// # Errors
    ///
    /// Returns [`MemtableError::Frozen`] if the active memtable was frozen
    /// concurrently — which the freeze/insert lock order makes impossible
    /// from this method.
    pub fn insert_with_wal_seq(&self, point: &Point, wal_seq: u64) -> Result<()> {
        self.active.read().insert_with_wal_seq(point, wal_seq)
    }

    /// How many memtables are frozen and waiting to be flushed.
    #[must_use]
    pub fn frozen_count(&self) -> usize {
        self.frozen.read().len()
    }

    /// `true` when neither the active memtable nor any frozen one holds a
    /// point.
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.active.read().is_empty() && self.frozen.read().is_empty()
    }

    /// Check whether the flush threshold has been reached.
    #[must_use]
    pub fn should_flush(&self) -> bool {
        self.active.read().estimated_size() >= self.config.flush_threshold
    }

    /// Freeze the active memtable and swap in a fresh one.
    ///
    /// The frozen memtable is stored for later flushing.
    ///
    /// # Errors
    ///
    /// Returns an error if there is already a frozen memtable that hasn't
    /// been flushed yet.
    pub fn freeze_and_swap(&self) -> Result<()> {
        let mut frozen_guard = self.frozen.write();
        // Nothing to freeze: an empty memtable in the queue would only take
        // a slot from a real one (after a failed flush, every retry used to
        // queue one until the queue was full and the retry itself failed).
        if self.active.read().is_empty() {
            return Ok(());
        }
        // Allow multiple frozen memtables up to the configured limit.
        if frozen_guard.len() >= self.config.max_frozen_memtables {
            let frozen_size: usize = frozen_guard.iter().map(|m| m.estimated_size()).sum();
            return Err(MemtableError::CapacityExceeded {
                current: frozen_size,
                limit: self.config.max_memory,
            });
        }

        let mut active_guard = self.active.write();

        // Freeze the current active memtable
        active_guard.freeze();
        let old_active = Arc::clone(&active_guard);

        // Replace with a fresh memtable
        *active_guard = Arc::new(Memtable::new());

        // Push to back of frozen queue.
        frozen_guard.push_back(old_active);

        Ok(())
    }

    /// Flush the oldest frozen memtable to segment files.
    ///
    /// Equivalent to [`flush_frozen_with`](Self::flush_frozen_with) with a
    /// registration step that does nothing.
    ///
    /// # Errors
    ///
    /// See [`flush_frozen_with`](Self::flush_frozen_with).
    pub fn flush_frozen(&self) -> Result<Vec<FlushResult>> {
        self.flush_frozen_with(|_| Ok(()))
    }

    /// Flush the oldest frozen memtable to segment files and hand the
    /// results to `register` before letting the memtable go.
    ///
    /// Groups points by measurement and writes one `.csx` segment per
    /// measurement, so each segment holds exactly one measurement.
    ///
    /// **The frozen memtable is released only after every segment is
    /// written and `register` has returned `Ok`.** If either fails, the
    /// files written so far are removed and the memtable stays at the head
    /// of the frozen queue for the next attempt — otherwise a failed write
    /// drops the only in-memory copy while the next flush raises the WAL
    /// floor past its records. Registration is inside the same window: a
    /// segment on disk but not in the catalog is an orphan the next open
    /// deletes.
    ///
    /// One flush per controller at a time; a concurrent caller waits.
    ///
    /// # Errors
    ///
    /// Returns [`MemtableError::NoFrozenMemtable`] if nothing is frozen or
    /// the frozen memtable is empty (it is discarded), the segment writer's
    /// error, or `register`'s error.
    pub fn flush_frozen_with<F>(&self, mut register: F) -> Result<Vec<FlushResult>>
    where
        F: FnMut(&[FlushResult]) -> Result<()>,
    {
        let _one_at_a_time = self.flush_lock.lock();

        let frozen_mt = self
            .frozen
            .read()
            .front()
            .cloned()
            .ok_or(MemtableError::NoFrozenMemtable)?;

        let batches = frozen_mt.to_record_batches();
        if batches.is_empty() {
            // An empty memtable has nothing to flush and nothing to lose.
            self.frozen.write().pop_front();
            return Err(MemtableError::NoFrozenMemtable);
        }

        let max_wal_seq = frozen_mt.max_wal_seq();
        let flush_started = std::time::Instant::now();

        let mut results = Vec::with_capacity(batches.len());
        let mut written_paths: Vec<std::path::PathBuf> = Vec::new();
        let outcome = (|| -> Result<()> {
            for mb in batches {
                // Estimate compressed bytes per row from the column count and
                // derive the number of rows that fit `target_segment_size_bytes`;
                // a measurement wider than that is written as several segments.
                let max_rows_per_segment = estimate_max_rows(
                    mb.batch.num_columns(),
                    self.config.segment_writer_config.target_segment_size_bytes,
                    self.config.segment_writer_config.max_row_group_size,
                );

                let total = mb.batch.num_rows();
                let mut offset = 0;
                while offset < total {
                    let len = max_rows_per_segment.min(total - offset);
                    let slice = mb.batch.slice(offset, len);
                    offset += len;

                    let segment_path = self.next_segment_path();
                    let mut writer = SegmentWriter::new(
                        &segment_path,
                        self.config.segment_writer_config.clone(),
                    )?;
                    let segment_meta =
                        writer.finalize_batch(&slice, &mb.measurement, &mb.tag_columns)?;
                    written_paths.push(segment_path);
                    results.push(FlushResult {
                        measurement: mb.measurement.clone(),
                        series_keys: segment_meta.series_keys.clone(),
                        segment_meta,
                        max_wal_seq,
                        points_flushed: len,
                    });
                }
            }
            register(&results)
        })();

        match outcome {
            Ok(()) => {
                let popped = self.frozen.write().pop_front();
                debug_assert!(
                    popped.is_some_and(|m| Arc::ptr_eq(&m, &frozen_mt)),
                    "the flush lock keeps the head of the frozen queue stable"
                );
                // How often memtables reach disk, and how long that takes,
                // is the first thing to look at when writes stall — and
                // only the *emergency* flush was counted, which by
                // definition is the case that has already gone wrong.
                metrics::counter!("chronix_memtable_flushes_total", "outcome" => "ok").increment(1);
                metrics::histogram!("chronix_memtable_flush_duration_seconds")
                    .record(flush_started.elapsed().as_secs_f64());
                Ok(results)
            }
            Err(e) => {
                metrics::counter!("chronix_memtable_flushes_total", "outcome" => "error")
                    .increment(1);
                tracing::error!(
                    error = %e,
                    segments_written = written_paths.len(),
                    "flush failed — the memtable stays frozen for the next attempt"
                );
                for path in written_paths {
                    let _ = std::fs::remove_file(&path);
                }
                Err(e)
            }
        }
    }

    /// Scan both active and frozen memtables for a specific series.
    ///
    /// Deduplicated by timestamp, **newest write wins** — the active memtable
    /// first, then the frozen queue newest to oldest.
    ///
    /// The result is a *set*, in no particular order.
    /// [`ShardRouter`](crate::memtable::ShardRouter) sorts by timestamp across
    /// shards, and every caller reaches this through it.
    ///
    /// # Consistency
    ///
    /// Both memtable references are obtained under a single scope
    /// following frozen→active lock order (matching `freeze_and_swap`).
    /// This prevents a race where `freeze_and_swap()` moves data
    /// between the two reads.
    #[must_use]
    pub fn scan(
        &self,
        series_key: &chronix_core::SeriesKey,
        min_ts: i64,
        max_ts: i64,
    ) -> Vec<Point> {
        let (active, frozen_list) = self.snapshot_memtables();

        // Newest first: the active memtable, then the frozen queue from its
        // back — `freeze_and_swap` pushes to the back, so the front is oldest.
        let mut sources = Vec::with_capacity(frozen_list.len() + 1);
        sources.push(active.scan(series_key, min_ts, max_ts));
        for frozen_mt in frozen_list.iter().rev() {
            sources.push(frozen_mt.scan(series_key, min_ts, max_ts));
        }
        merge_newest_first(sources)
    }

    /// Scan all points across both active and frozen memtables.
    ///
    /// Deduplicated by `(series, timestamp)`, newest write wins. Unordered —
    /// see [`scan_measurement`](Self::scan_measurement).
    #[must_use]
    pub fn scan_all(&self) -> Vec<Point> {
        let (active, frozen_list) = self.snapshot_memtables();

        let mut sources = Vec::with_capacity(frozen_list.len() + 1);
        sources.push(active.scan_all());
        for frozen_mt in frozen_list.iter().rev() {
            sources.push(frozen_mt.scan_all());
        }
        merge_newest_first(sources)
    }

    /// Scan points for a specific measurement across active and frozen
    /// memtables.
    ///
    /// Deduplicated by `(series, timestamp)`, **newest write wins** — the
    /// active memtable first, then the frozen queue newest to oldest.
    ///
    /// The result is a *set*, in no particular order.
    /// [`ShardRouter`](crate::memtable::ShardRouter) sorts by timestamp across
    /// shards, and every caller reaches this through it.
    #[must_use]
    pub fn scan_measurement(&self, measurement: &str, min_ts: i64, max_ts: i64) -> Vec<Point> {
        let (active, frozen_list) = self.snapshot_memtables();

        let mut sources = Vec::with_capacity(frozen_list.len() + 1);
        sources.push(active.scan_measurement(measurement, min_ts, max_ts));
        for frozen_mt in frozen_list.iter().rev() {
            sources.push(frozen_mt.scan_measurement(measurement, min_ts, max_ts));
        }
        merge_newest_first(sources)
    }

    /// Returns the total estimated memory across active and all frozen memtables.
    #[must_use]
    pub fn total_memory(&self) -> usize {
        let active_size = self.active.read().estimated_size();
        let frozen_size: usize = self.frozen.read().iter().map(|m| m.estimated_size()).sum();
        active_size + frozen_size
    }

    /// Heap bytes held by the interners of the active and frozen memtables.
    #[must_use]
    pub fn total_interner_memory(&self) -> usize {
        let active = self.active.read().interner_bytes();
        let frozen: usize = self.frozen.read().iter().map(|m| m.interner_bytes()).sum();
        active + frozen
    }

    /// The smallest WAL sequence number held by a memtable that is not yet
    /// in a segment — active **or** frozen.
    ///
    /// This is what the WAL floor is capped by. It used to consult the
    /// active memtable only, so a frozen memtable still being written — or
    /// left behind by a failed flush — was invisible, and a concurrent
    /// flush of another shard raised the floor past its records.
    #[must_use]
    pub fn min_unflushed_wal_seq(&self) -> Option<u64> {
        let frozen = self.frozen.read();
        let active = self.active.read();
        active
            .min_wal_seq()
            .into_iter()
            .chain(frozen.iter().filter_map(|m| m.min_wal_seq()))
            .min()
    }

    /// Check capacity and reject if over limit.
    fn check_capacity(&self) -> Result<()> {
        let total = self.total_memory();
        if total >= self.config.max_memory {
            return Err(MemtableError::CapacityExceeded {
                current: total,
                limit: self.config.max_memory,
            });
        }
        Ok(())
    }

    /// Obtain a consistent snapshot of both active and frozen
    /// memtables. Acquires locks in frozen→active order (matching
    /// `freeze_and_swap`) to avoid ABBA deadlock.
    fn snapshot_memtables(&self) -> (Arc<Memtable>, Vec<Arc<Memtable>>) {
        let frozen_guard = self.frozen.read();
        let active_guard = self.active.read();
        (
            Arc::clone(&active_guard),
            frozen_guard.iter().cloned().collect(),
        )
    }

    /// Compute graduated write stall delay based on memory pressure.
    ///
    /// Returns `Duration::ZERO` when memory usage is below the soft limit.
    /// Between the soft limit and the hard limit, returns a linearly
    /// increasing delay up to `max_stall_delay`. At or above the hard
    /// limit, callers should use `check_capacity` which returns an error.
    ///
    /// This implements RocksDB-style graduated backpressure: as the memtable
    /// fills up, writes slow down proportionally, giving the flush thread
    /// time to drain without triggering hard rejections.
    #[must_use]
    pub fn write_stall_delay(&self) -> std::time::Duration {
        let total = self.total_memory();
        let soft_limit = (self.config.max_memory as f64 * self.config.stall_soft_fraction) as usize;

        if total <= soft_limit {
            return std::time::Duration::ZERO;
        }

        let hard = self.config.max_memory;
        let range = hard.saturating_sub(soft_limit);
        if range == 0 {
            return self.config.max_stall_delay;
        }

        let pressure = total.saturating_sub(soft_limit) as f64 / range as f64;
        let pressure = pressure.min(1.0);
        self.config.max_stall_delay.mul_f64(pressure)
    }

    /// Generate the next unique segment file path.
    fn next_segment_path(&self) -> PathBuf {
        let counter = self
            .segment_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        self.config
            .segment_dir
            .join(format!("seg_{now_ns}_{counter:06}.csx"))
    }
}

/// Result of a flush operation for a single measurement.
#[derive(Debug)]
pub struct FlushResult {
    /// Measurement name this segment contains.
    pub measurement: String,
    /// Metadata about the written segment file.
    pub segment_meta: SegmentMeta,
    /// Maximum WAL sequence number of the flushed memtable
    /// (for WAL truncation).
    pub max_wal_seq: Option<u64>,
    /// Number of points flushed.
    pub points_flushed: usize,
    /// The distinct series in this segment, as the writer saw them.
    pub series_keys: Vec<chronix_core::SeriesKey>,
}

/// Merge memtable scans into one deduplicated result, **newest source first**.
///
/// `sources` must be ordered newest to oldest — the active memtable, then the
/// frozen queue from its back (newest frozen) to its front (oldest). The first
/// copy of a `(series, timestamp)` encountered wins, so the newest write does.
///
/// # One pass, not a fold
///
/// Folding a two-argument merge over the frozen queue needs the precedence
/// spelled out at each step, and a fold that took the accumulated result as
/// the winner let the *oldest* frozen memtable beat the newest — invisible,
/// because the active memtable was still right. One pass has a single
/// ordering rule in a single place. It is also linear rather than quadratic,
/// and keys on the borrowed canonical form, so no `String` is allocated.
///
/// The output is in source order, not key order: callers reach this through
/// `ShardRouter`, which sorts by timestamp across shards.
fn merge_newest_first(sources: Vec<Vec<Point>>) -> Vec<Point> {
    let total: usize = sources.iter().map(Vec::len).sum();
    let mut all = Vec::with_capacity(total);
    for src in sources {
        all.extend(src);
    }

    // The key is the canonical form itself, **borrowed**, not a hash of it:
    // exact, and with no per-point `String`. The old form allocated one
    // `String` per point *per merge round* and used a `BTreeMap`, so it also
    // compared those strings O(log n) times each.
    let mut seen = std::collections::HashSet::with_capacity(total);
    let mut keep = Vec::with_capacity(total);
    for p in &all {
        keep.push(seen.insert((p.series_key().canonical_form(), p.timestamp())));
    }
    drop(seen);

    let mut keep = keep.into_iter();
    all.retain(|_| keep.next().unwrap_or(false));
    all
}

/// Estimate the maximum number of rows that fit within a target segment size.
///
/// Uses a conservative heuristic: count the number of columns (timestamp +
/// tags + fields) and multiply by an estimated compressed bytes-per-column
/// value (4 bytes — typical for delta/Gorilla-encoded + LZ4 time-series
/// data). The result is clamped to at least `min_rows` to avoid producing
/// trivially small segments.
fn estimate_max_rows(
    num_columns: usize,
    target_segment_size_bytes: usize,
    min_rows: usize,
) -> usize {
    // Conservative estimate: ~4 compressed bytes per column per row.
    // Real-world compression typically achieves 2–6 bytes/column/row for
    // numeric columns, 4–10 for string tags.
    let est_bytes_per_row = (num_columns * 4).max(8);
    let max_rows = target_segment_size_bytes / est_bytes_per_row;
    // At minimum one row-group worth.
    max_rows.max(min_rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::path::Path;

    use chronix_core::types::{FieldValue, SeriesKey};

    fn make_point(host: &str, ts: i64, value: f64) -> Point {
        make_point_with_measurement("cpu", host, ts, value)
    }

    fn make_point_with_measurement(measurement: &str, host: &str, ts: i64, value: f64) -> Point {
        let tags: BTreeMap<String, String> = [("host".to_string(), host.to_string())]
            .into_iter()
            .collect();
        let series_key = SeriesKey::new(measurement.to_string(), tags).unwrap();
        let fields: BTreeMap<String, FieldValue> = [("value".to_string(), FieldValue::F64(value))]
            .into_iter()
            .collect();
        Point::new(series_key, fields, ts).unwrap()
    }

    fn test_config(dir: &Path) -> FlushConfig {
        FlushConfig {
            flush_threshold: 1024, // low threshold for testing
            max_memory: 1024 * 1024,
            segment_dir: dir.to_path_buf(),
            segment_writer_config: SegmentWriterConfig {
                compress: false,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn insert_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let fc = FlushController::new(test_config(dir.path()));

        fc.insert(&make_point("a", 100, 1.0)).unwrap();
        fc.insert(&make_point("a", 200, 2.0)).unwrap();

        let all = fc.scan_all();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn freeze_and_swap() {
        let dir = tempfile::tempdir().unwrap();
        let fc = FlushController::new(test_config(dir.path()));

        fc.insert(&make_point("a", 100, 1.0)).unwrap();
        fc.insert(&make_point("a", 200, 2.0)).unwrap();

        fc.freeze_and_swap().unwrap();

        // Active should be empty, frozen should have data
        assert!(fc.active_memtable().is_empty());
        assert!(fc.frozen_memtable().is_some());
        assert_eq!(fc.frozen_memtable().unwrap().len(), 2);

        // Can still insert into the new active
        fc.insert(&make_point("a", 300, 3.0)).unwrap();
        assert_eq!(fc.active_memtable().len(), 1);
    }

    #[test]
    fn scan_merges_active_and_frozen() {
        let dir = tempfile::tempdir().unwrap();
        let fc = FlushController::new(test_config(dir.path()));

        fc.insert(&make_point("a", 100, 1.0)).unwrap();
        fc.insert(&make_point("a", 200, 2.0)).unwrap();
        fc.freeze_and_swap().unwrap();
        fc.insert(&make_point("a", 300, 3.0)).unwrap();

        let all = fc.scan_all();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn active_wins_on_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let fc = FlushController::new(test_config(dir.path()));

        fc.insert(&make_point("a", 100, 1.0)).unwrap();
        fc.freeze_and_swap().unwrap();
        fc.insert(&make_point("a", 100, 99.0)).unwrap();

        let all = fc.scan_all();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].field("value"), Some(&FieldValue::F64(99.0)));
    }

    #[test]
    fn flush_frozen_writes_segment() {
        let dir = tempfile::tempdir().unwrap();
        let fc = FlushController::new(test_config(dir.path()));

        for i in 0..10 {
            fc.insert(&make_point("a", i * 100, i as f64)).unwrap();
        }

        fc.freeze_and_swap().unwrap();
        let results = fc.flush_frozen().unwrap();

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.measurement, "cpu");
        assert_eq!(result.points_flushed, 10);
        assert!(result.segment_meta.path.exists());
        assert_eq!(result.segment_meta.row_count, 10);

        // Frozen slot should be cleared
        assert!(fc.frozen_memtable().is_none());
    }

    #[test]
    fn should_flush_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.flush_threshold = 100; // very low
        let fc = FlushController::new(config);

        assert!(!fc.should_flush());

        // Insert enough data to exceed threshold
        for i in 0..20 {
            let _ = fc.insert(&make_point("a", i, i as f64));
        }
        assert!(fc.should_flush());
    }

    #[test]
    fn multi_freeze_respects_limit() {
        let dir = tempfile::tempdir().unwrap();
        let fc = FlushController::new(test_config(dir.path()));

        // With max_frozen_memtables=2, two freezes should succeed.
        fc.insert(&make_point("a", 100, 1.0)).unwrap();
        fc.freeze_and_swap().unwrap();

        fc.insert(&make_point("a", 200, 2.0)).unwrap();
        fc.freeze_and_swap()
            .expect("second freeze should succeed with multi-slot frozen queue");

        // Third freeze should fail: frozen queue is at capacity (2).
        fc.insert(&make_point("a", 300, 3.0)).unwrap();
        let result = fc.freeze_and_swap();
        assert!(result.is_err(), "should reject third freeze at capacity");
    }

    #[test]
    fn capacity_rejection() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_memory = 1; // impossibly low
        let fc = FlushController::new(config);

        // First insert might work (no data yet), but subsequent should fail
        // due to capacity check. The first insert sets estimated_size > 0.
        let _ = fc.insert(&make_point("a", 100, 1.0));
        let result = fc.insert(&make_point("a", 200, 2.0));
        assert!(result.is_err());
    }

    #[test]
    fn wal_seq_in_flush_result() {
        let dir = tempfile::tempdir().unwrap();
        let fc = FlushController::new(test_config(dir.path()));

        fc.insert_with_wal_seq(&make_point("a", 100, 1.0), 5)
            .unwrap();
        fc.insert_with_wal_seq(&make_point("a", 200, 2.0), 10)
            .unwrap();

        fc.freeze_and_swap().unwrap();
        let results = fc.flush_frozen().unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].max_wal_seq, Some(10));
    }

    #[test]
    fn merging_keeps_the_first_source_that_has_a_key() {
        // Sources are newest first, so the earlier source wins a duplicate —
        // and a *third* source cannot beat the second, which is where the
        // fold this replaced went wrong.
        let merged = merge_newest_first(vec![
            vec![make_point("a", 100, 10.0), make_point("a", 300, 30.0)],
            vec![make_point("a", 100, 1.0), make_point("a", 200, 2.0)],
            vec![make_point("a", 200, 99.0)],
        ]);
        assert_eq!(merged.len(), 3);

        let ts100 = merged.iter().find(|p| p.timestamp() == 100).unwrap();
        assert_eq!(ts100.field("value"), Some(&FieldValue::F64(10.0)));
        let ts200 = merged.iter().find(|p| p.timestamp() == 200).unwrap();
        assert_eq!(
            ts200.field("value"),
            Some(&FieldValue::F64(2.0)),
            "the second source wins over the third"
        );
    }

    /// The **newest** value of a `(series, timestamp)` wins, including when
    /// both copies are in frozen memtables.
    ///
    /// The three read paths merge the frozen queue oldest-first with the
    /// accumulated result as the *winner*, so the first frozen memtable beat
    /// every one after it — older data beating newer, which is the one thing
    /// last-write-wins may not do. `active` was always right, which is what
    /// the doc comment said and what every test checked.
    #[test]
    fn the_newest_frozen_memtable_wins_a_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let fc = FlushController::new(test_config(dir.path()));

        // Write, freeze, rewrite the same (series, timestamp), freeze again:
        // now two *frozen* memtables hold the same point.
        fc.insert(&make_point("a", 100, 1.0)).unwrap();
        fc.freeze_and_swap().unwrap();
        fc.insert(&make_point("a", 100, 2.0)).unwrap();
        fc.freeze_and_swap().unwrap();

        let key = make_point("a", 100, 0.0).series_key().clone();
        for (path, points) in [
            ("scan", fc.scan(&key, 0, i64::MAX)),
            ("scan_all", fc.scan_all()),
            (
                "scan_measurement",
                fc.scan_measurement(key.measurement(), 0, i64::MAX),
            ),
        ] {
            assert_eq!(points.len(), 1, "{path}: one point survives dedup");
            assert_eq!(
                points[0].field("value"),
                Some(&FieldValue::F64(2.0)),
                "{path}: the newer write wins"
            );
        }
    }

    /// …and the active memtable still beats every frozen one.
    #[test]
    fn the_active_memtable_wins_a_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let fc = FlushController::new(test_config(dir.path()));
        fc.insert(&make_point("a", 100, 1.0)).unwrap();
        fc.freeze_and_swap().unwrap();
        fc.insert(&make_point("a", 100, 9.0)).unwrap();

        let points = fc.scan_all();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].field("value"), Some(&FieldValue::F64(9.0)));
    }

    #[test]
    fn scan_measurement_filters_active() {
        let dir = tempfile::tempdir().unwrap();
        let fc = FlushController::new(test_config(dir.path()));

        fc.insert(&make_point_with_measurement("cpu", "a", 100, 1.0))
            .unwrap();
        fc.insert(&make_point_with_measurement("mem", "a", 200, 2.0))
            .unwrap();
        fc.insert(&make_point_with_measurement("cpu", "b", 300, 3.0))
            .unwrap();

        let cpu = fc.scan_measurement("cpu", 0, i64::MAX);
        assert_eq!(cpu.len(), 2);
        assert!(cpu.iter().all(|p| p.series_key().measurement() == "cpu"));

        let mem = fc.scan_measurement("mem", 0, i64::MAX);
        assert_eq!(mem.len(), 1);
        assert_eq!(mem[0].series_key().measurement(), "mem");

        let none = fc.scan_measurement("disk", 0, i64::MAX);
        assert!(none.is_empty());
    }

    #[test]
    fn scan_measurement_merges_active_and_frozen() {
        let dir = tempfile::tempdir().unwrap();
        let fc = FlushController::new(test_config(dir.path()));

        // Insert into active, then freeze
        fc.insert(&make_point_with_measurement("cpu", "a", 100, 1.0))
            .unwrap();
        fc.insert(&make_point_with_measurement("mem", "a", 150, 9.0))
            .unwrap();
        fc.freeze_and_swap().unwrap();

        // Insert more into new active
        fc.insert(&make_point_with_measurement("cpu", "a", 200, 2.0))
            .unwrap();
        fc.insert(&make_point_with_measurement("mem", "b", 250, 8.0))
            .unwrap();

        // A `FlushController` scan is a *set*: it is `ShardRouter` that orders
        // by timestamp, across shards, and every caller reaches it that way.
        let mut cpu: Vec<i64> = fc
            .scan_measurement("cpu", 0, i64::MAX)
            .iter()
            .map(Point::timestamp)
            .collect();
        cpu.sort_unstable();
        assert_eq!(cpu, vec![100, 200]);

        let mem = fc.scan_measurement("mem", 0, i64::MAX);
        assert_eq!(mem.len(), 2);
    }

    #[test]
    fn min_unflushed_wal_seq_empty() {
        let fc = FlushController::new(FlushConfig::default());
        assert!(fc.min_unflushed_wal_seq().is_none());
    }

    #[test]
    fn min_unflushed_wal_seq_after_insert() {
        let fc = FlushController::new(FlushConfig::default());
        let p = make_point("host", 100, 1.0);
        fc.insert_with_wal_seq(&p, 42).unwrap();
        fc.insert_with_wal_seq(&p, 50).unwrap();
        assert_eq!(fc.min_unflushed_wal_seq(), Some(42));
    }

    /// A frozen memtable is unflushed until its segment is registered, so
    /// its records must keep capping the WAL floor. This used to report
    /// `None` after a freeze, and a concurrent flush of another shard then
    /// truncated the WAL files holding the frozen records.
    #[test]
    fn a_frozen_memtable_still_counts_as_unflushed() {
        let fc = FlushController::new(FlushConfig::default());
        let p = make_point("host", 100, 1.0);
        fc.insert_with_wal_seq(&p, 10).unwrap();
        fc.freeze_and_swap().unwrap();
        assert_eq!(
            fc.min_unflushed_wal_seq(),
            Some(10),
            "the frozen memtable holds seq 10 and nothing flushed it"
        );
        fc.insert_with_wal_seq(&make_point("host", 200, 2.0), 20)
            .unwrap();
        assert_eq!(fc.min_unflushed_wal_seq(), Some(10));
    }

    /// A flush that fails leaves the memtable frozen for a retry and removes
    /// the files it managed to write; it used to pop the memtable first and
    /// drop the only copy of the data on any error.
    #[test]
    fn a_failed_flush_keeps_the_frozen_memtable() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        // The segment directory is a regular file: every write fails.
        let blocked = dir.path().join("blocked");
        std::fs::write(&blocked, b"not a directory").unwrap();
        config.segment_dir = blocked.clone();
        let fc = FlushController::new(config);

        fc.insert_with_wal_seq(&make_point("a", 100, 1.0), 7)
            .unwrap();
        fc.freeze_and_swap().unwrap();
        assert!(fc.flush_frozen().is_err());
        assert!(
            fc.frozen_memtable().is_some(),
            "the memtable must survive a failed flush"
        );
        assert_eq!(fc.min_unflushed_wal_seq(), Some(7));

        // A registration failure is the same: nothing is lost, and the
        // segment written for the attempt is removed.
        std::fs::remove_file(&blocked).unwrap();
        std::fs::create_dir_all(&blocked).unwrap();
        let err = fc.flush_frozen_with(|results| {
            assert_eq!(results.len(), 1);
            assert!(results[0].segment_meta.path.exists());
            Err(MemtableError::NoFrozenMemtable)
        });
        assert!(err.is_err());
        assert!(fc.frozen_memtable().is_some());
        assert_eq!(
            std::fs::read_dir(&blocked).unwrap().count(),
            0,
            "a segment nothing registered is removed, not left as an orphan"
        );

        // And the retry succeeds and releases it.
        let results = fc.flush_frozen().unwrap();
        assert_eq!(results[0].points_flushed, 1);
        assert!(fc.frozen_memtable().is_none());
        assert!(fc.min_unflushed_wal_seq().is_none());
    }

    #[test]
    fn write_stall_zero_below_soft_limit() {
        let dir = tempfile::tempdir().unwrap();
        let config = FlushConfig {
            max_memory: 1024 * 1024,
            stall_soft_fraction: 0.75,
            max_stall_delay: std::time::Duration::from_millis(100),
            segment_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let fc = FlushController::new(config);

        // Empty memtable — no stall
        assert_eq!(fc.write_stall_delay(), std::time::Duration::ZERO);
    }

    #[test]
    fn write_stall_increases_with_pressure() {
        let dir = tempfile::tempdir().unwrap();
        let config = FlushConfig {
            max_memory: 10_000,       // 10 KB
            stall_soft_fraction: 0.5, // stall starts at 5 KB
            max_stall_delay: std::time::Duration::from_millis(100),
            flush_threshold: 10_000, // won't auto-flush
            segment_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let fc = FlushController::new(config);

        // Insert data to push past soft limit
        for i in 0..100 {
            let _ = fc.insert(&make_point("a", i, i as f64));
        }

        let delay = fc.write_stall_delay();
        // Should have non-zero delay since we're over the soft limit
        let mem = fc.total_memory();
        if mem > 5000 {
            assert!(
                delay > std::time::Duration::ZERO,
                "should stall when over soft limit (mem={mem})"
            );
            assert!(
                delay <= std::time::Duration::from_millis(100),
                "should not exceed max stall delay"
            );
        }
    }
}
