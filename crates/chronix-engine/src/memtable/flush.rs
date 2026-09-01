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

    /// Insert a point with an associated WAL sequence number.
    ///
    /// # Errors
    ///
    /// Returns [`MemtableError::CapacityExceeded`] if capacity is exceeded,
    /// or [`MemtableError::Frozen`] if the active memtable was frozen
    /// concurrently.
    pub fn insert_with_wal_seq(&self, point: &Point, wal_seq: u64) -> Result<()> {
        self.check_capacity()?;
        self.active.read().insert_with_wal_seq(point, wal_seq)
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

    /// Flush the frozen memtable to segment files.
    ///
    /// Groups points by measurement and writes one `.csx` segment per
    /// measurement. This ensures each segment contains data for exactly
    /// one measurement, enabling efficient measurement-level queries.
    ///
    /// Returns one [`FlushResult`] per measurement written.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no frozen memtable, or if the segment
    /// write fails.
    pub fn flush_frozen(&self) -> Result<Vec<FlushResult>> {
        // FIX: Atomically pop the oldest frozen memtable to prevent a
        // TOCTOU race where two concurrent callers both read the same front
        // item and then each pop_front() removes a different memtable —
        // causing the second pop to discard an unflushed memtable.
        let frozen_mt = self
            .frozen
            .write()
            .pop_front()
            .ok_or(MemtableError::NoFrozenMemtable)?;

        let points = frozen_mt.to_points();
        if points.is_empty() {
            return Err(MemtableError::NoFrozenMemtable);
        }

        let max_wal_seq = frozen_mt.max_wal_seq();

        // Group points by measurement for per-measurement segments
        let mut by_measurement: std::collections::BTreeMap<String, Vec<Point>> =
            std::collections::BTreeMap::new();
        for p in points {
            by_measurement
                .entry(p.series_key().measurement().to_string())
                .or_default()
                .push(p);
        }

        let mut results = Vec::with_capacity(by_measurement.len());
        let mut written_paths: Vec<std::path::PathBuf> = Vec::new();
        for (measurement, measurement_points) in by_measurement {
            // ── Size-based splitting ────────────────────────────────
            //
            // Estimate compressed bytes per row and derive the maximum
            // number of rows that fit inside `target_segment_size_bytes`.
            // If the measurement's point count exceeds this limit, split
            // into multiple segments.
            let max_rows_per_segment = estimate_max_rows(
                &measurement_points,
                self.config.segment_writer_config.target_segment_size_bytes,
                self.config.segment_writer_config.max_row_group_size,
            );

            // Split into chunks that each fit within the segment size budget.
            let chunks: Vec<&[Point]> = measurement_points.chunks(max_rows_per_segment).collect();

            for chunk in &chunks {
                let count = chunk.len();

                // Collect unique series keys for bloom filter construction.
                // Uses canonical_form() for dedup instead of hash_fnv() to
                // prevent hash-collision false dedup that would exclude a
                // series from the bloom filter, causing queries to skip the
                // segment entirely for that series.
                let mut seen_canonicals = std::collections::HashSet::new();
                let unique_series_keys: Vec<chronix_core::SeriesKey> = chunk
                    .iter()
                    .filter(|p| seen_canonicals.insert(p.series_key().canonical_form().to_string()))
                    .map(|p| p.series_key().clone())
                    .collect();

                let segment_path = self.next_segment_path();
                let write_result = (|| -> Result<SegmentMeta> {
                    let mut writer = SegmentWriter::new(
                        &segment_path,
                        self.config.segment_writer_config.clone(),
                    )?;
                    writer.write_rows(chunk)?;
                    Ok(writer.finalize()?)
                })();

                match write_result {
                    Ok(segment_meta) => {
                        written_paths.push(segment_path);
                        results.push(FlushResult {
                            measurement: measurement.clone(),
                            segment_meta,
                            max_wal_seq,
                            points_flushed: count,
                            series_keys: unique_series_keys,
                        });
                    }
                    Err(e) => {
                        // Do NOT delete successfully-written
                        // segments — they contain valid data that WAL replay
                        // would otherwise lose. Return partial results along
                        // with the error so the caller can register what was
                        // written before the failure.
                        tracing::error!(
                            measurement = %measurement,
                            error = %e,
                            segments_written = written_paths.len(),
                            "partial flush failure — keeping written segments for recovery"
                        );
                        return Err(e);
                    }
                }
            } // end chunk loop
        }

        Ok(results)
    }

    /// Scan both active and frozen memtables for a specific series.
    ///
    /// Results are merged and deduplicated by timestamp (active wins if
    /// both contain the same timestamp, since active is newer).
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

        let mut points = active.scan(series_key, min_ts, max_ts);

        // Merge from all frozen memtables (oldest first).
        for frozen_mt in &frozen_list {
            let frozen_points = frozen_mt.scan(series_key, min_ts, max_ts);
            points = merge_points(points, frozen_points);
        }

        points
    }

    /// Scan all points across both active and frozen memtables.
    #[must_use]
    pub fn scan_all(&self) -> Vec<Point> {
        let (active, frozen_list) = self.snapshot_memtables();

        let mut all_points = active.scan_all();

        for frozen_mt in &frozen_list {
            let frozen_points = frozen_mt.scan_all();
            all_points = merge_points(all_points, frozen_points);
        }

        all_points
    }

    /// Scan points for a specific measurement across active and frozen memtables.
    ///
    /// Results are merged and deduplicated (active wins for same timestamp).
    #[must_use]
    pub fn scan_measurement(&self, measurement: &str, min_ts: i64, max_ts: i64) -> Vec<Point> {
        let (active, frozen_list) = self.snapshot_memtables();

        let mut points = active.scan_measurement(measurement, min_ts, max_ts);

        for frozen_mt in &frozen_list {
            let frozen_points = frozen_mt.scan_measurement(measurement, min_ts, max_ts);
            points = merge_points(points, frozen_points);
        }

        points
    }

    /// Returns the total estimated memory across active and all frozen memtables.
    #[must_use]
    pub fn total_memory(&self) -> usize {
        let active_size = self.active.read().estimated_size();
        let frozen_size: usize = self.frozen.read().iter().map(|m| m.estimated_size()).sum();
        active_size + frozen_size
    }

    /// Returns the minimum WAL sequence held in the active memtable.
    ///
    /// Returns `None` when the active memtable is empty (no WAL
    /// sequences have been recorded).
    #[must_use]
    pub fn active_min_wal_seq(&self) -> Option<u64> {
        self.active.read().min_wal_seq()
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
    /// Unique series keys in this segment (for bloom filter construction).
    pub series_keys: Vec<chronix_core::SeriesKey>,
}

/// Merge two sorted point vectors, deduplicating by
/// `(series_canonical, timestamp)`. When both contain the same key, the
/// point from `primary` (active memtable) wins.
///
/// Uses the canonical form (`measurement\0tag1=v1\0tag2=v2`) as part of
/// the key instead of hash-only identity, preventing hash-collision data
/// loss where two different series with the same FNV-1a hash and timestamp
/// would otherwise silently overwrite each other.
fn merge_points(primary: Vec<Point>, secondary: Vec<Point>) -> Vec<Point> {
    use std::collections::BTreeMap as Map;

    // Build a map keyed by (series_hash, timestamp, canonical_form).
    // The hash is kept as the first component for fast BTree ordering;
    // the canonical form is the collision-proof tiebreaker.
    let mut map: Map<(u64, i64, String), Point> = Map::new();

    for p in secondary {
        let key = (
            p.series_key().hash_fnv(),
            p.timestamp(),
            p.series_key().canonical_form().to_string(),
        );
        map.insert(key, p);
    }
    // Primary overwrites secondary
    for p in primary {
        let key = (
            p.series_key().hash_fnv(),
            p.timestamp(),
            p.series_key().canonical_form().to_string(),
        );
        map.insert(key, p);
    }

    map.into_values().collect()
}

/// Estimate the maximum number of rows that fit within a target segment size.
///
/// Uses a conservative heuristic: count the number of columns (timestamp +
/// tags + fields) and multiply by an estimated compressed bytes-per-column
/// value (4 bytes — typical for delta/Gorilla-encoded + LZ4 time-series
/// data). The result is clamped to at least `min_rows` to avoid producing
/// trivially small segments.
fn estimate_max_rows(points: &[Point], target_segment_size_bytes: usize, min_rows: usize) -> usize {
    if points.is_empty() {
        return min_rows;
    }

    // Sample the first point for schema width.
    let sample = &points[0];
    let n_fields = sample.fields().len();
    let n_tags = sample.tags().len();

    // timestamp (1) + tags + fields
    let num_columns = 1 + n_tags + n_fields;

    // Conservative estimate: ~4 compressed bytes per column per row.
    // Real-world compression typically achieves 2–6 bytes/column/row for
    // numeric columns (delta + Gorilla + LZ4), 4–10 for string tags (RLE).
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
    fn merge_points_dedup() {
        let primary = vec![make_point("a", 100, 10.0), make_point("a", 300, 30.0)];
        let secondary = vec![
            make_point("a", 100, 1.0), // dup — primary wins
            make_point("a", 200, 2.0),
        ];

        let merged = merge_points(primary, secondary);
        assert_eq!(merged.len(), 3);

        // The ts=100 entry should have primary's value (10.0)
        let ts100 = merged.iter().find(|p| p.timestamp() == 100).unwrap();
        assert_eq!(ts100.field("value"), Some(&FieldValue::F64(10.0)));
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

        let cpu = fc.scan_measurement("cpu", 0, i64::MAX);
        assert_eq!(cpu.len(), 2);
        assert_eq!(cpu[0].timestamp(), 100);
        assert_eq!(cpu[1].timestamp(), 200);

        let mem = fc.scan_measurement("mem", 0, i64::MAX);
        assert_eq!(mem.len(), 2);
    }

    #[test]
    fn test_active_min_wal_seq_empty() {
        let fc = FlushController::new(FlushConfig::default());
        assert!(
            fc.active_min_wal_seq().is_none(),
            "empty active memtable should have no WAL seq"
        );
    }

    #[test]
    fn test_active_min_wal_seq_after_insert() {
        let fc = FlushController::new(FlushConfig::default());
        let p = make_point("host", 100, 1.0);
        fc.insert_with_wal_seq(&p, 42).unwrap();
        fc.insert_with_wal_seq(&p, 50).unwrap();
        assert_eq!(fc.active_min_wal_seq(), Some(42));
    }

    #[test]
    fn test_active_min_wal_seq_after_freeze() {
        let fc = FlushController::new(FlushConfig::default());
        let p = make_point("host", 100, 1.0);
        fc.insert_with_wal_seq(&p, 10).unwrap();
        fc.freeze_and_swap().unwrap();
        // Active is now empty, frozen holds the old data
        assert!(
            fc.active_min_wal_seq().is_none(),
            "fresh active after freeze should have no WAL seq"
        );
        // Insert new data into active
        fc.insert_with_wal_seq(&p, 20).unwrap();
        assert_eq!(fc.active_min_wal_seq(), Some(20));
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
