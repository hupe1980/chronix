//! Time-shard routing for memtable writes.
//!
//! The [`ShardRouter`] maps each incoming point to the correct shard based
//! on its timestamp, and routes the write to that shard's
//! [`FlushController`]. Shards outside the configured out-of-order
//! tolerance window are rejected.
//!
//! # Shard lifecycle
//!
//! 1. A new shard is created lazily on first write.
//! 2. When a shard's memtable reaches the flush threshold, it is frozen and
//!    flushed to a segment file.
//! 3. Shards below the tolerance window that hold no data are retired from
//!    the router; a backfill recreates one lazily.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

use chronix_core::types::{Point, ShardId, Timestamp};

use crate::memtable::error::{MemtableError, Result};
use crate::memtable::flush::{FlushConfig, FlushController, FlushResult};

/// Configuration for the shard router.
#[derive(Debug, Clone)]
pub struct ShardRouterConfig {
    /// Duration of each shard (e.g. 1 hour).
    pub shard_duration: Duration,
    /// Number of shards before/after the active shard to still accept
    /// writes for.
    pub ooo_shard_tolerance: i64,
    /// Flush configuration applied to each shard's controller.
    pub flush_config: FlushConfig,
}

impl Default for ShardRouterConfig {
    fn default() -> Self {
        Self {
            shard_duration: Duration::from_secs(3600), // 1 hour
            ooo_shard_tolerance: 1,
            flush_config: FlushConfig::default(),
        }
    }
}

/// A single shard entry managed by the router.
struct ShardEntry {
    /// The flush controller for this shard's memtable.
    controller: Arc<FlushController>,
}

/// Routes writes to per-shard memtables based on point timestamps.
///
/// Each shard covers a duration of [`shard_duration`](ShardRouterConfig::shard_duration) (default: 1 hour).
/// Writes within `±ooo_shard_tolerance` shards of the current active
/// shard are accepted; writes outside this window are rejected.
pub struct ShardRouter {
    /// Per-shard flush controllers, keyed by `ShardId`.
    shards: Arc<RwLock<BTreeMap<ShardId, ShardEntry>>>,
    /// Configuration.
    config: ShardRouterConfig,
    /// The current active shard (the most recently written-to).
    active_shard: Arc<RwLock<Option<ShardId>>>,
}

impl std::fmt::Debug for ShardRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardRouter")
            .field("config", &self.config)
            .field("active_shard", &*self.active_shard.read())
            .field("shard_count", &self.shards.read().len())
            .finish()
    }
}

impl ShardRouter {
    /// Create a new shard router with the given configuration.
    #[must_use]
    pub fn new(config: ShardRouterConfig) -> Self {
        Self {
            shards: Arc::new(RwLock::new(BTreeMap::new())),
            config,
            active_shard: Arc::new(RwLock::new(None)),
        }
    }

    /// Insert a point, routing it to the appropriate shard.
    ///
    /// # Errors
    ///
    /// Returns [`MemtableError::ShardOutOfRange`] if the point's timestamp
    /// maps to a shard outside the tolerance window.
    pub fn insert(&self, point: &Point) -> Result<()> {
        let shard_id = ShardId::from_timestamp(point.timestamp(), self.config.shard_duration);

        self.ensure_shard_allowed(shard_id, point.timestamp())?;
        self.insert_into_shard(shard_id, point)
    }

    /// Insert a point with an associated WAL sequence number.
    ///
    /// # Errors
    ///
    /// Returns [`MemtableError::ShardOutOfRange`] if the shard is outside
    /// tolerance.
    pub fn insert_with_wal_seq(&self, point: &Point, wal_seq: u64) -> Result<()> {
        let shard_id = ShardId::from_timestamp(point.timestamp(), self.config.shard_duration);

        self.ensure_shard_allowed(shard_id, point.timestamp())?;
        self.insert_into_shard_with_wal_seq(shard_id, point, wal_seq)
    }

    /// Admit a timestamp: decide whether a write for it may be accepted.
    ///
    /// This is the out-of-order tolerance check, split from the insert so a
    /// caller can run it *before* making the write durable. A point that
    /// fails admission never reaches the WAL, so what the caller was told
    /// was rejected cannot come back through replay.
    ///
    /// Admitting a timestamp in a newer shard advances the active shard,
    /// exactly as an insert would.
    ///
    /// # Errors
    ///
    /// Returns [`MemtableError::ShardOutOfRange`] if the timestamp maps to a
    /// shard outside the tolerance window.
    pub fn admit(&self, timestamp: Timestamp) -> Result<ShardId> {
        let shard_id = ShardId::from_timestamp(timestamp, self.config.shard_duration);
        self.ensure_shard_allowed(shard_id, timestamp)?;
        Ok(shard_id)
    }

    /// Insert a point that has already passed [`admit`](Self::admit) and
    /// been made durable.
    ///
    /// No tolerance check: the decision was taken before the WAL append,
    /// and re-taking it here would let a concurrent writer that advanced
    /// the active shard in between turn an acknowledged, durable write
    /// into a rejected one. The WAL is the truth; what is in it is
    /// inserted.
    ///
    /// # Errors
    ///
    /// Returns an error if the shard's memtable insertion fails.
    pub fn insert_admitted(&self, point: &Point, wal_seq: u64) -> Result<()> {
        let shard_id = ShardId::from_timestamp(point.timestamp(), self.config.shard_duration);
        self.insert_into_shard_with_wal_seq(shard_id, point, wal_seq)
    }

    /// Insert a point during WAL replay, bypassing shard tolerance checks.
    ///
    /// During crash recovery the WAL may contain records in a different
    /// order than the original `ensure_shard_allowed` evaluations (because
    /// WAL append order can differ from shard-check order under concurrent
    /// writes). Replaying with tolerance checks would reject points that
    /// were validly accepted at write time. This method skips the tolerance
    /// gate and updates the active shard monotonically so that subsequent
    /// live writes see the correct baseline.
    ///
    /// # Errors
    ///
    /// Returns an error if the shard's memtable insertion fails.
    pub fn insert_replay(&self, point: &Point, wal_seq: u64) -> Result<()> {
        let shard_id = ShardId::from_timestamp(point.timestamp(), self.config.shard_duration);

        // Update active shard monotonically (no tolerance rejection)
        {
            let mut active_guard = self.active_shard.write();
            match *active_guard {
                Some(active) if shard_id.0 > active.0 => {
                    *active_guard = Some(shard_id);
                }
                None => {
                    *active_guard = Some(shard_id);
                }
                _ => {}
            }
        }

        self.insert_into_shard_with_wal_seq(shard_id, point, wal_seq)
    }

    /// Flush a specific shard's memtable to segments.
    ///
    /// Equivalent to [`flush_shard_with`](Self::flush_shard_with) with a
    /// registration step that does nothing.
    ///
    /// # Errors
    ///
    /// See [`flush_shard_with`](Self::flush_shard_with).
    pub fn flush_shard(&self, shard_id: ShardId) -> Result<Vec<FlushResult>> {
        self.flush_shard_with(shard_id, |_| Ok(()))
    }

    /// Freeze the shard's active memtable, write it to segments, hand the
    /// results to `register`, and release the memtable only once
    /// registration succeeded — see
    /// [`FlushController::flush_frozen_with`].
    ///
    /// The router lock is released before the write starts: it used to be
    /// held for the whole segment write, and parking_lot's writer-preferring
    /// `RwLock` then queued every insert behind the first write that needed
    /// a new shard.
    ///
    /// # Errors
    ///
    /// Returns an error if the shard doesn't exist or the flush fails.
    pub fn flush_shard_with<F>(
        &self,
        shard_id: ShardId,
        mut register: F,
    ) -> Result<Vec<FlushResult>>
    where
        F: FnMut(&[FlushResult]) -> Result<()>,
    {
        let controller = {
            let shards = self.shards.read();
            let entry = shards
                .get(&shard_id)
                .ok_or(MemtableError::NoSuchShard(shard_id.0))?;
            Arc::clone(&entry.controller)
        };

        // A full frozen queue is not a reason to fail: it means earlier
        // flushes failed, and draining it is exactly what this call is for.
        match controller.freeze_and_swap() {
            Ok(()) | Err(MemtableError::CapacityExceeded { .. }) => {}
            Err(e) => return Err(e),
        }
        // Drain every frozen memtable, oldest first.
        let mut all = Vec::new();
        while controller.frozen_count() > 0 {
            match controller.flush_frozen_with(&mut register) {
                Ok(results) => all.extend(results),
                Err(MemtableError::NoFrozenMemtable) => continue,
                Err(e) => {
                    if all.is_empty() {
                        return Err(e);
                    }
                    // Something landed; report it and let the caller retry
                    // the rest. The unflushed memtables still cap the floor.
                    tracing::warn!(shard = %shard_id, error = %e, "flush stopped early");
                    return Ok(all);
                }
            }
        }
        if all.is_empty() {
            return Err(MemtableError::NoFrozenMemtable);
        }
        Ok(all)
    }

    /// The newest shard a write has been admitted to, if any.
    ///
    /// The out-of-order window is anchored here rather than at the wall
    /// clock: a device that was offline for days and resumes with old
    /// timestamps is still writing "now" as far as the engine is concerned.
    #[must_use]
    pub fn active_shard(&self) -> Option<ShardId> {
        *self.active_shard.read()
    }

    /// Anchor the out-of-order window at `shard` if no write has anchored
    /// it yet.
    ///
    /// `open()` seeds it from the newest timestamp in the catalog, so the
    /// window a database enforces after a clean restart is the one it
    /// enforced before — and so the rollup materialiser knows which buckets
    /// are final without waiting for the first live write.
    pub fn seed_active_shard(&self, shard: ShardId) {
        let mut active = self.active_shard.write();
        if active.is_none_or(|a| shard.0 > a.0) {
            *active = Some(shard);
        }
    }

    /// Drop the router entries of shards that hold no data and can no
    /// longer receive live writes — everything below the out-of-order
    /// window. They are recreated lazily if a backfill reaches them.
    ///
    /// Without this a long-running database accumulated one empty
    /// controller per hour of uptime, forever.
    pub fn retire_idle_shards(&self) {
        let Some(active) = self.active_shard() else {
            return;
        };
        let oldest_live = active.0.saturating_sub(self.config.ooo_shard_tolerance);
        let mut shards = self.shards.write();
        shards.retain(|id, entry| id.0 >= oldest_live || !entry.controller.is_idle());
    }

    /// Every shard the router currently holds a memtable for.
    #[must_use]
    pub fn shard_ids(&self) -> Vec<ShardId> {
        self.shards.read().keys().copied().collect()
    }

    /// Returns the total memory usage across all shards.
    #[must_use]
    pub fn total_memory(&self) -> usize {
        self.shards
            .read()
            .values()
            .map(|e| e.controller.total_memory())
            .sum()
    }

    /// Heap bytes held by every memtable's string interner.
    ///
    /// Not included in [`total_memory`](Self::total_memory), which counts rows
    /// and index entries. The interner grows with cardinality rather than with
    /// row count, so it is worth its own number.
    #[must_use]
    pub fn total_interner_memory(&self) -> usize {
        self.shards
            .read()
            .values()
            .map(|e| e.controller.total_interner_memory())
            .sum()
    }

    /// The smallest WAL sequence number held by any memtable — active or
    /// frozen, in any shard — that is not yet in a registered segment.
    /// `None` when every memtable is empty.
    #[must_use]
    pub fn min_unflushed_wal_seq(&self) -> Option<u64> {
        self.shards
            .read()
            .values()
            .filter_map(|e| e.controller.min_unflushed_wal_seq())
            .min()
    }

    /// Scan a specific series across all shards within a time range.
    #[must_use]
    pub fn scan(
        &self,
        series_key: &chronix_core::SeriesKey,
        min_ts: Timestamp,
        max_ts: Timestamp,
    ) -> Vec<Point> {
        // Snapshot controller Arcs under the lock, then release before scanning.
        let controllers: Vec<Arc<FlushController>> = self
            .shards
            .read()
            .values()
            .map(|e| Arc::clone(&e.controller))
            .collect();

        let mut all_points = Vec::new();
        for ctrl in &controllers {
            all_points.extend(ctrl.scan(series_key, min_ts, max_ts));
        }

        // Sort by timestamp
        all_points.sort_by_key(Point::timestamp);
        all_points
    }

    /// Scan all points belonging to a specific measurement across all shards.
    ///
    /// Returns points sorted by timestamp for the given measurement,
    /// filtered to `[min_ts, max_ts]`.
    #[must_use]
    pub fn scan_measurement(
        &self,
        measurement: &str,
        min_ts: Timestamp,
        max_ts: Timestamp,
    ) -> Vec<Point> {
        // Snapshot controller Arcs under the lock, then release before scanning.
        let controllers: Vec<Arc<FlushController>> = self
            .shards
            .read()
            .values()
            .map(|e| Arc::clone(&e.controller))
            .collect();

        let mut all_points = Vec::new();
        for ctrl in &controllers {
            all_points.extend(ctrl.scan_measurement(measurement, min_ts, max_ts));
        }

        // Sort by timestamp
        all_points.sort_by_key(Point::timestamp);
        all_points
    }

    /// Map a timestamp to its shard ID.
    #[must_use]
    pub fn shard_for_timestamp(&self, timestamp: Timestamp) -> ShardId {
        ShardId::from_timestamp(timestamp, self.config.shard_duration)
    }

    /// Check if a shard is within the tolerance window.
    /// `timestamp` is carried through only so the rejection can name the
    /// value the caller actually wrote. Reporting the shard's *start* instead
    /// hands back a number the caller never sent.
    fn ensure_shard_allowed(&self, shard_id: ShardId, timestamp: Timestamp) -> Result<()> {
        let mut active_guard = self.active_shard.write();

        let active = if let Some(active) = *active_guard {
            // Update active shard if this is a newer shard
            if shard_id.0 > active.0 {
                *active_guard = Some(shard_id);
                shard_id
            } else {
                active
            }
        } else {
            *active_guard = Some(shard_id);
            shard_id
        };

        let min_allowed = active.0.saturating_sub(self.config.ooo_shard_tolerance);
        let max_allowed = active.0.saturating_add(self.config.ooo_shard_tolerance);

        if shard_id.0 < min_allowed || shard_id.0 > max_allowed {
            return Err(MemtableError::ShardOutOfRange {
                timestamp,
                earliest: ShardId(min_allowed).start_timestamp(self.config.shard_duration),
                latest: ShardId(max_allowed).end_timestamp(self.config.shard_duration),
                tolerance: self.config.ooo_shard_tolerance,
                shard_secs: self.config.shard_duration.as_secs(),
            });
        }

        Ok(())
    }

    /// Obtain a reference to the shard entry, creating it if needed.
    /// Calls `f` with the shard's `FlushController`.
    fn with_shard<F, R>(&self, shard_id: ShardId, f: F) -> Result<R>
    where
        F: FnOnce(&FlushController) -> Result<R>,
    {
        // Fast path: read lock
        {
            let shards = self.shards.read();
            if let Some(entry) = shards.get(&shard_id) {
                return f(&entry.controller);
            }
        }

        // Slow path: create shard under write lock
        let mut shards = self.shards.write();
        let entry = shards.entry(shard_id).or_insert_with(|| {
            let mut config = self.config.flush_config.clone();
            config.segment_dir = config.segment_dir.join(format!("shard_{}", shard_id.0));
            ShardEntry {
                controller: Arc::new(FlushController::new(config)),
            }
        });
        f(&entry.controller)
    }

    /// Internal: insert a point into the specified shard.
    fn insert_into_shard(&self, shard_id: ShardId, point: &Point) -> Result<()> {
        self.with_shard(shard_id, |ctrl| ctrl.insert(point))
    }

    /// Internal: insert a point with WAL seq into the specified shard.
    fn insert_into_shard_with_wal_seq(
        &self,
        shard_id: ShardId,
        point: &Point,
        wal_seq: u64,
    ) -> Result<()> {
        self.with_shard(shard_id, |ctrl| ctrl.insert_with_wal_seq(point, wal_seq))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap as StdBTreeMap;

    use crate::segment::writer::SegmentWriterConfig;
    use chronix_core::types::{FieldValue, SeriesKey};

    fn make_point(ts: Timestamp, value: f64) -> Point {
        make_point_with_measurement("cpu", ts, value)
    }

    fn make_point_with_measurement(measurement: &str, ts: Timestamp, value: f64) -> Point {
        let tags: StdBTreeMap<String, String> = [("host".to_string(), "a".to_string())]
            .into_iter()
            .collect();
        let series_key = SeriesKey::new(measurement.to_string(), tags).unwrap();
        let fields: StdBTreeMap<String, FieldValue> =
            [("value".to_string(), FieldValue::F64(value))]
                .into_iter()
                .collect();
        Point::new(series_key, fields, ts).unwrap()
    }

    fn test_router_config(dir: &std::path::Path) -> ShardRouterConfig {
        ShardRouterConfig {
            shard_duration: Duration::from_secs(3600),
            ooo_shard_tolerance: 1,
            flush_config: FlushConfig {
                flush_threshold: 64 * 1024 * 1024,
                max_memory: 256 * 1024 * 1024,
                segment_dir: dir.to_path_buf(),
                segment_writer_config: SegmentWriterConfig {
                    compress: false,
                    ..Default::default()
                },
                ..Default::default()
            },
        }
    }

    /// Nanoseconds per hour, matching `shard_duration`.
    const NS_PER_HOUR: i64 = 3_600_000_000_000;

    #[test]
    fn routes_to_correct_shard() {
        let dir = tempfile::tempdir().unwrap();
        let router = ShardRouter::new(test_router_config(dir.path()));

        // Shard 0: timestamps [0, NS_PER_HOUR)
        let p1 = make_point(100, 1.0);
        router.insert(&p1).unwrap();

        assert_eq!(router.shard_ids().len(), 1);
        assert_eq!(router.shard_ids()[0], ShardId(0));
    }

    #[test]
    fn routes_to_multiple_shards() {
        let dir = tempfile::tempdir().unwrap();
        let router = ShardRouter::new(test_router_config(dir.path()));

        // Shard 0
        router.insert(&make_point(100, 1.0)).unwrap();
        // Shard 1
        router.insert(&make_point(NS_PER_HOUR + 100, 2.0)).unwrap();

        let shard_ids = router.shard_ids();
        assert_eq!(shard_ids.len(), 2);
    }

    #[test]
    fn ooo_within_tolerance_accepted() {
        let dir = tempfile::tempdir().unwrap();
        let router = ShardRouter::new(test_router_config(dir.path()));

        // Write to shard 1 first (sets active shard)
        router.insert(&make_point(NS_PER_HOUR + 100, 1.0)).unwrap();

        // Write to shard 0 (previous — within tolerance of 1)
        router.insert(&make_point(100, 2.0)).unwrap();

        assert_eq!(router.shard_ids().len(), 2);
    }

    #[test]
    fn ooo_beyond_tolerance_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_router_config(dir.path());
        config.ooo_shard_tolerance = 0; // no tolerance
        let router = ShardRouter::new(config);

        // Write to shard 1 (sets active)
        router.insert(&make_point(NS_PER_HOUR + 100, 1.0)).unwrap();

        // Write to shard 0 (previous — beyond tolerance of 0)
        let result = router.insert(&make_point(100, 2.0));
        assert!(result.is_err());
    }

    #[test]
    fn flush_shard() {
        let dir = tempfile::tempdir().unwrap();
        let router = ShardRouter::new(test_router_config(dir.path()));

        for i in 0..10 {
            router.insert(&make_point(i * 100, i as f64)).unwrap();
        }

        let shard_id = ShardId(0);
        let results = router.flush_shard(shard_id).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].measurement, "cpu");
        assert_eq!(results[0].points_flushed, 10);
    }

    #[test]
    fn scan_across_shards() {
        let dir = tempfile::tempdir().unwrap();
        let router = ShardRouter::new(test_router_config(dir.path()));

        // Data in shard 0
        router.insert(&make_point(100, 1.0)).unwrap();
        // Data in shard 1
        router.insert(&make_point(NS_PER_HOUR + 100, 2.0)).unwrap();

        let key = SeriesKey::new(
            "cpu".to_string(),
            [("host".to_string(), "a".to_string())]
                .into_iter()
                .collect(),
        )
        .unwrap();

        let points = router.scan(&key, 0, NS_PER_HOUR * 2);
        assert_eq!(points.len(), 2);
        assert!(points[0].timestamp() < points[1].timestamp());
    }

    #[test]
    fn total_memory_across_shards() {
        let dir = tempfile::tempdir().unwrap();
        let router = ShardRouter::new(test_router_config(dir.path()));

        router.insert(&make_point(100, 1.0)).unwrap();
        router.insert(&make_point(NS_PER_HOUR + 100, 2.0)).unwrap();

        assert!(router.total_memory() > 0);
    }

    #[test]
    fn shard_for_timestamp_computation() {
        let dir = tempfile::tempdir().unwrap();
        let router = ShardRouter::new(test_router_config(dir.path()));

        assert_eq!(router.shard_for_timestamp(0), ShardId(0));
        assert_eq!(router.shard_for_timestamp(NS_PER_HOUR), ShardId(1));
        assert_eq!(router.shard_for_timestamp(NS_PER_HOUR - 1), ShardId(0));
    }

    #[test]
    fn scan_measurement_across_shards() {
        let dir = tempfile::tempdir().unwrap();
        let router = ShardRouter::new(test_router_config(dir.path()));

        // Shard 0: cpu and mem points
        router
            .insert(&make_point_with_measurement("cpu", 100, 1.0))
            .unwrap();
        router
            .insert(&make_point_with_measurement("mem", 200, 2.0))
            .unwrap();
        // Shard 1: cpu point
        router
            .insert(&make_point_with_measurement("cpu", NS_PER_HOUR + 100, 3.0))
            .unwrap();

        // Scan cpu across all shards
        let cpu = router.scan_measurement("cpu", 0, i64::MAX);
        assert_eq!(cpu.len(), 2);
        assert!(cpu.iter().all(|p| p.series_key().measurement() == "cpu"));
        assert!(cpu[0].timestamp() < cpu[1].timestamp()); // sorted

        // Scan mem
        let mem = router.scan_measurement("mem", 0, i64::MAX);
        assert_eq!(mem.len(), 1);

        // Scan non-existent
        let none = router.scan_measurement("disk", 0, i64::MAX);
        assert!(none.is_empty());
    }

    #[test]
    fn scan_measurement_respects_time_range() {
        let dir = tempfile::tempdir().unwrap();
        let router = ShardRouter::new(test_router_config(dir.path()));

        router
            .insert(&make_point_with_measurement("cpu", 100, 1.0))
            .unwrap();
        router
            .insert(&make_point_with_measurement("cpu", 200, 2.0))
            .unwrap();
        router
            .insert(&make_point_with_measurement("cpu", 300, 3.0))
            .unwrap();

        let result = router.scan_measurement("cpu", 150, 250);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].timestamp(), 200);
    }
}
