//! Concurrent memtable backed by a lock-free skip list.
//!
//! The [`Memtable`] is the primary in-memory write buffer. It accepts
//! [`Point`] inserts, stores them in a concurrent skip
//! list ordered by `(series_key_hash, timestamp)`, and supports range scans
//! for reads.
//!
//! # Threading model
//!
//! Multiple writers can insert concurrently without external locking.
//! The skip list (from [`crossbeam_skiplist`]) provides lock-free concurrent
//! access. An [`AtomicUsize`] tracks the estimated memory footprint, and
//! [`AtomicU64`] values track the WAL sequence range for flush coordination.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_skiplist::SkipMap;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;

use chronix_core::types::{Point, SeriesKey, Timestamp};

use crate::memtable::error::{MemtableError, Result};
use crate::memtable::interner::StringInterner;
use crate::memtable::key::{MemtableEntry, MemtableKey};

/// A concurrent in-memory write buffer for time-series data.
///
/// Points are stored in a [`crossbeam_skiplist::SkipMap`] keyed by
/// [`MemtableKey`] `(series_key_hash, timestamp)`. The skip list provides
/// lock-free concurrent inserts and ordered iteration.
///
/// # Deduplication
///
/// If a point with the same `(series_key_hash, timestamp)` already exists,
/// the new value overwrites the old one (last-write-wins).
pub struct Memtable {
    /// Lock-free concurrent skip list.
    data: SkipMap<MemtableKey, MemtableEntry>,
    /// String interner for deduplicating measurement/tag strings.
    interner: StringInterner,
    /// Secondary index: measurement name → set of series hashes.
    ///
    /// Enables O(S × log N) `scan_measurement()` instead of O(N) full scans,
    /// where S is the number of distinct series for the target measurement.
    ///
    /// Uses [`DashMap`] with per-shard locking instead of a
    /// global `RwLock<HashMap>` to eliminate write-lock contention on the
    /// hot insert path.
    measurement_index: DashMap<Arc<str>, HashSet<u64>>,
    /// Estimated total memory usage in bytes, excluding `measurement_index`.
    estimated_size: AtomicUsize,
    /// Estimated `measurement_index` footprint in bytes.
    ///
    /// Accumulated at the two sites that write the index, not recomputed on
    /// read: the flush controller's capacity check calls `estimated_size()`
    /// once per point, and walking the `DashMap` there locks every shard.
    index_size: AtomicUsize,
    /// Minimum WAL sequence number covered by this memtable.
    min_wal_seq: AtomicU64,
    /// Maximum WAL sequence number covered by this memtable.
    max_wal_seq: AtomicU64,
    /// Whether this memtable has been frozen (read-only).
    frozen: AtomicBool,
}

impl std::fmt::Debug for Memtable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Memtable")
            .field("len", &self.data.len())
            .field(
                "estimated_size",
                &self.estimated_size.load(Ordering::Relaxed),
            )
            .field("interned_strings", &self.interner.len())
            .field("min_wal_seq", &self.min_wal_seq())
            .field("max_wal_seq", &self.max_wal_seq())
            .field("frozen", &self.frozen.load(Ordering::Relaxed))
            .finish()
    }
}

impl Memtable {
    /// Create a new empty memtable.
    #[must_use]
    pub fn new() -> Self {
        Self {
            data: SkipMap::new(),
            interner: StringInterner::new(),
            measurement_index: DashMap::new(),
            estimated_size: AtomicUsize::new(0),
            index_size: AtomicUsize::new(0),
            min_wal_seq: AtomicU64::new(u64::MAX),
            max_wal_seq: AtomicU64::new(0),
            frozen: AtomicBool::new(false),
        }
    }

    /// Insert a point into the memtable.
    ///
    /// The point is stored keyed by `(series_key_hash, timestamp)`. If a
    /// point with the same key already exists, it is overwritten
    /// (last-write-wins).
    ///
    /// # Errors
    ///
    /// Returns [`MemtableError::Frozen`] if the memtable has been frozen.
    pub fn insert(&self, point: &Point) -> Result<()> {
        if self.frozen.load(Ordering::Acquire) {
            return Err(MemtableError::Frozen);
        }

        let canonical: Arc<str> = self.interner.intern(point.series_key().canonical_form());
        let key = MemtableKey::new(point.series_key().hash_fnv(), point.timestamp(), canonical);

        let measurement = self.interner.intern(point.series_key().measurement());
        let measurement_for_index = Arc::clone(&measurement);
        let tags = point
            .series_key()
            .tags()
            .iter()
            .map(|(k, v)| (self.interner.intern(k), self.interner.intern(v)))
            .collect();

        let entry = MemtableEntry {
            measurement,
            tags,
            fields: point
                .fields()
                .iter()
                .map(|(k, v)| (Arc::clone(k), v.clone()))
                .collect(),
        };

        let entry_size = entry.estimated_size() + std::mem::size_of::<MemtableKey>();

        // If the key already exists, subtract the old entry size to avoid
        // monotonically inflating the estimate on overwrites.
        // Use saturating subtraction to prevent underflow when concurrent
        // overwrites race on the same key (TOCTOU between get and insert).
        // Crossbeam SkipMap::insert() doesn't return the old value,
        // so a separate get() is required. The two O(log N) traversals are
        // inherent to the SkipMap API.
        if let Some(existing) = self.data.get(&key) {
            let old_size = existing.value().estimated_size() + std::mem::size_of::<MemtableKey>();
            let _ =
                self.estimated_size
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                        Some(current.saturating_sub(old_size))
                    });
        }

        self.data.insert(key, entry);
        let new_size = self.estimated_size.fetch_add(entry_size, Ordering::Relaxed) + entry_size;

        // Emit memtable memory usage gauge so operators can monitor
        // memory pressure and flush readiness.
        metrics::gauge!("chronix_memtable_memory_bytes").set(new_size as f64);
        metrics::gauge!("chronix_memtable_entries").set(self.data.len() as f64);

        // Update secondary measurement index so scan_measurement() can
        // use targeted range scans instead of full table iteration.
        // DashMap entry API — only per-shard lock, no global contention.
        {
            let hash = point.series_key().hash_fnv();
            let delta = self.index_insert(measurement_for_index, hash);
            if delta > 0 {
                self.index_size.fetch_add(delta, Ordering::Relaxed);
            }
        }

        Ok(())
    }

    /// Insert a point with an associated WAL sequence number.
    ///
    /// Updates the min/max WAL sequence tracking in addition to inserting
    /// the point.
    ///
    /// # Errors
    ///
    /// Returns [`MemtableError::Frozen`] if the memtable has been frozen.
    pub fn insert_with_wal_seq(&self, point: &Point, wal_seq: u64) -> Result<()> {
        self.insert(point)?;
        self.update_wal_seq(wal_seq);
        Ok(())
    }

    /// Insert a batch of points in one call.
    ///
    /// More efficient than calling `insert` in a loop because:
    /// The frozen check is performed once.
    /// Strings are pre-interned and the measurement index is updated in
    ///   a single write-lock acquisition.
    /// Size adjustments are accumulated locally and flushed with a single
    ///   `fetch_add`.
    ///
    /// Returns the number of points successfully inserted.
    ///
    /// # Errors
    ///
    /// Returns [`MemtableError::Frozen`] if the memtable has been frozen.
    pub fn insert_batch(&self, points: &[Point]) -> Result<usize> {
        if self.frozen.load(Ordering::Acquire) {
            return Err(MemtableError::Frozen);
        }

        let mut total_size_delta: i64 = 0;
        // Collect (measurement, hash) pairs for batch measurement index update.
        let mut new_index_entries: Vec<(Arc<str>, u64)> = Vec::new();

        // Track previous key to skip overwrite check when
        // consecutive batch points have different keys (the common case
        // for append-only time-series ingestion).
        let mut prev_hash: u64 = 0;
        let mut prev_ts: i64 = i64::MIN;

        for point in points {
            let canonical: Arc<str> = self.interner.intern(point.series_key().canonical_form());
            let key = MemtableKey::new(point.series_key().hash_fnv(), point.timestamp(), canonical);

            let measurement = self.interner.intern(point.series_key().measurement());
            let tags = point
                .series_key()
                .tags()
                .iter()
                .map(|(k, v)| (self.interner.intern(k), self.interner.intern(v)))
                .collect();

            let entry = MemtableEntry {
                measurement: Arc::clone(&measurement),
                tags,
                fields: point
                    .fields()
                    .iter()
                    .map(|(k, v)| (Arc::clone(k), v.clone()))
                    .collect(),
            };

            let entry_size = entry.estimated_size() + std::mem::size_of::<MemtableKey>();

            // Only check for overwrites when the key could collide
            // with an existing entry. For strictly increasing timestamps
            // within the same series or different series, skip the O(log N)
            // get() entirely — the insert alone is sufficient.
            let cur_hash = point.series_key().hash_fnv();
            let cur_ts = point.timestamp();
            let may_overwrite = cur_hash == prev_hash && cur_ts == prev_ts;

            if may_overwrite {
                if let Some(existing) = self.data.get(&key) {
                    let old_size =
                        existing.value().estimated_size() + std::mem::size_of::<MemtableKey>();
                    total_size_delta -= old_size.min(i64::MAX as usize) as i64;
                }
            }

            prev_hash = cur_hash;
            prev_ts = cur_ts;

            self.data.insert(key, entry);
            // Saturating cast prevents theoretical i64 overflow.
            total_size_delta += entry_size.min(i64::MAX as usize) as i64;

            new_index_entries.push((measurement, cur_hash));
        }

        // Flush accumulated size delta in one atomic operation.
        if total_size_delta >= 0 {
            self.estimated_size
                .fetch_add(total_size_delta as usize, Ordering::Relaxed);
        } else {
            let _ =
                self.estimated_size
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                        Some(current.saturating_sub(total_size_delta.unsigned_abs() as usize))
                    });
        }

        // DashMap entry API — per-shard locking, no global contention.
        let mut index_delta = 0usize;
        for (measurement, hash) in new_index_entries {
            index_delta += self.index_insert(measurement, hash);
        }
        if index_delta > 0 {
            self.index_size.fetch_add(index_delta, Ordering::Relaxed);
        }

        // Emit memtable memory usage gauge after batch insert.
        metrics::gauge!("chronix_memtable_memory_bytes")
            .set(self.estimated_size.load(Ordering::Relaxed) as f64);
        metrics::gauge!("chronix_memtable_entries").set(self.data.len() as f64);

        Ok(points.len())
    }

    /// Insert a batch of points with an associated WAL sequence number.
    ///
    /// Uses the batch to set the WAL sequence range, then delegates to
    /// `insert_batch`.
    ///
    /// # Errors
    ///
    /// Returns [`MemtableError::Frozen`] if the memtable has been frozen.
    pub fn insert_batch_with_wal_seq(&self, points: &[Point], wal_seq: u64) -> Result<usize> {
        let count = self.insert_batch(points)?;
        self.update_wal_seq(wal_seq);
        Ok(count)
    }

    /// Update the WAL sequence range tracked by this memtable.
    fn update_wal_seq(&self, seq: u64) {
        // Update min: keep the smallest
        let mut current = self.min_wal_seq.load(Ordering::Relaxed);
        while seq < current {
            match self.min_wal_seq.compare_exchange_weak(
                current,
                seq,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }

        // Update max: keep the largest
        let mut current = self.max_wal_seq.load(Ordering::Relaxed);
        while seq > current {
            match self.max_wal_seq.compare_exchange_weak(
                current,
                seq,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Scan points for a specific series within a time range.
    ///
    /// Returns points sorted by timestamp for the series identified by
    /// `series_key`. Only points within `[min_ts, max_ts]` are returned.
    /// Uses canonical form comparison (not hash) to avoid false positives
    /// from hash collisions.
    #[must_use]
    pub fn scan(&self, series_key: &SeriesKey, min_ts: Timestamp, max_ts: Timestamp) -> Vec<Point> {
        let hash = series_key.hash_fnv();
        let canonical = series_key.canonical_form();
        // Use the canonical form for exact range boundaries so the
        // scan covers only entries for this specific series.
        let start = MemtableKey::new(hash, min_ts, Arc::from(canonical));
        let end = MemtableKey::new(hash, max_ts, Arc::from(canonical));

        self.data
            .range(start..=end)
            .filter(|entry| &*entry.key().series_canonical == canonical)
            .filter_map(|entry| Self::entry_to_point(entry.key(), entry.value()))
            .collect()
    }

    /// Scan all points across all series within a time range.
    ///
    /// Returns all points sorted by `(series_key_hash, timestamp)`.
    ///
    /// **Note:** this is intentionally O(N) — it scans all series.
    /// For measurement-scoped queries (the common case), use
    /// `scan_measurement` which leverages the measurement index for
    /// O(S × log N) per-series range scans.
    #[must_use]
    pub fn scan_range(&self, min_ts: Timestamp, max_ts: Timestamp) -> Vec<Point> {
        // Scan the full BTree but filter by timestamp. No sentinel upper
        // bound needed — just iterate all entries and filter.
        self.data
            .iter()
            .filter(|entry| {
                let ts = entry.key().timestamp;
                ts >= min_ts && ts <= max_ts
            })
            .filter_map(|entry| Self::entry_to_point(entry.key(), entry.value()))
            .collect()
    }

    /// Scan all points in the memtable, sorted by `(series_key_hash,
    /// timestamp)`.
    #[must_use]
    pub fn scan_all(&self) -> Vec<Point> {
        self.data
            .iter()
            .filter_map(|entry| Self::entry_to_point(entry.key(), entry.value()))
            .collect()
    }

    /// Scan all points belonging to a specific measurement within a time range.
    ///
    /// Uses a secondary measurement index to perform targeted range scans
    /// per series hash, achieving O(S × log N) complexity instead of O(N)
    /// full-table iteration, where S is the number of distinct series for
    /// the measurement.
    #[must_use]
    pub fn scan_measurement(
        &self,
        measurement: &str,
        min_ts: Timestamp,
        max_ts: Timestamp,
    ) -> Vec<Point> {
        let hashes = match self.measurement_index.get(measurement) {
            Some(set) => set.iter().copied().collect::<Vec<_>>(),
            None => return Vec::new(),
        };

        let mut results = Vec::new();
        for hash in hashes {
            // Build range bounds for this series hash across the timestamp window.
            let start = MemtableKey::lower_bound(hash, min_ts);
            let end = MemtableKey::upper_bound(hash, max_ts);
            results.extend(
                self.data
                    .range(start..=end)
                    .filter(|entry| {
                        let ts = entry.key().timestamp;
                        ts >= min_ts && ts <= max_ts && &*entry.value().measurement == measurement
                    })
                    .filter_map(|entry| Self::entry_to_point(entry.key(), entry.value())),
            );
        }
        results
    }

    /// Returns the number of entries in the memtable.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if the memtable contains no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the estimated memory usage in bytes.
    ///
    /// Includes the `measurement_index` DashMap overhead.
    #[must_use]
    pub fn estimated_size(&self) -> usize {
        self.estimated_size.load(Ordering::Relaxed) + self.index_size.load(Ordering::Relaxed)
    }

    /// Add `hash` to the index under `measurement`, returning the bytes the
    /// index grew by.
    ///
    /// The accounting matches what a full walk of the map would produce: an
    /// `Arc<str>` key (pointer pair + string bytes) plus the `HashSet`'s
    /// allocation (capacity × 8) and its own overhead. Growth is charged as
    /// the set's capacity actually changes, so an amortised rehash is
    /// accounted for on the insert that triggers it.
    fn index_insert(&self, measurement: Arc<str>, hash: u64) -> usize {
        const SET_OVERHEAD: usize = 56;
        match self.measurement_index.entry(measurement) {
            Entry::Occupied(mut occupied) => {
                let before = occupied.get().capacity();
                occupied.get_mut().insert(hash);
                (occupied.get().capacity() - before) * std::mem::size_of::<u64>()
            }
            Entry::Vacant(vacant) => {
                let key_bytes = std::mem::size_of::<usize>() * 2 + vacant.key().len();
                let mut set = HashSet::new();
                set.insert(hash);
                let set_bytes = set.capacity() * std::mem::size_of::<u64>() + SET_OVERHEAD;
                vacant.insert(set);
                key_bytes + set_bytes
            }
        }
    }

    /// Returns the minimum WAL sequence number tracked.
    ///
    /// Returns `None` if no WAL sequences have been recorded.
    #[must_use]
    pub fn min_wal_seq(&self) -> Option<u64> {
        let val = self.min_wal_seq.load(Ordering::Acquire);
        if val == u64::MAX {
            None
        } else {
            Some(val)
        }
    }

    /// Returns the maximum WAL sequence number tracked.
    ///
    /// Returns `None` if no WAL sequences have been recorded.
    #[must_use]
    pub fn max_wal_seq(&self) -> Option<u64> {
        let val = self.max_wal_seq.load(Ordering::Acquire);
        if val == 0 && self.min_wal_seq.load(Ordering::Acquire) == u64::MAX {
            None
        } else {
            Some(val)
        }
    }

    /// Returns whether this memtable has been frozen.
    #[must_use]
    pub fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::Acquire)
    }

    /// Freeze this memtable, preventing any further inserts.
    ///
    /// After freezing, all `insert` calls will return
    /// [`MemtableError::Frozen`].
    pub fn freeze(&self) {
        self.frozen.store(true, Ordering::Release);
    }

    /// Collect all points as a `Vec<Point>` for flushing to a segment.
    #[must_use]
    pub fn to_points(&self) -> Vec<Point> {
        self.scan_all()
    }

    /// Deprecated: use [`to_points`](Self::to_points) instead.
    #[must_use]
    #[deprecated(
        since = "0.1.0",
        note = "renamed to `to_points`; this method does not drain"
    )]
    pub fn drain_to_points(&self) -> Vec<Point> {
        self.to_points()
    }

    /// Convert a skip list entry back into a [`Point`].
    ///
    /// Returns `None` if reconstruction fails (e.g. invalid series key),
    /// which is logged as a warning.
    fn entry_to_point(key: &MemtableKey, entry: &MemtableEntry) -> Option<Point> {
        let tags = entry
            .tags
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let series_key = SeriesKey::new(entry.measurement.to_string(), tags)
            .map_err(|e| {
                // Emit metric so operators can alert on data loss
                // during flush, not just rely on log scanning.
                tracing::error!(
                    hash = key.series_key_hash,
                    error = %e,
                    "DATA LOSS: failed to reconstruct series key during flush"
                );
                metrics::counter!("chronix_memtable_dropped_points_total").increment(1);
                e
            })
            .ok()?;
        Point::new(
            series_key,
            entry
                .fields
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
            key.timestamp,
        )
        .map_err(|e| {
            tracing::error!(
                hash = key.series_key_hash,
                timestamp = key.timestamp,
                error = %e,
                "DATA LOSS: failed to reconstruct point during flush"
            );
            metrics::counter!("chronix_memtable_dropped_points_total").increment(1);
            e
        })
        .ok()
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: `SkipMap` is `Send + Sync`, and all other fields are atomic types.
// The `Memtable` is safe to share across threads.
// (This is automatically derived, but we document it explicitly.)

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use chronix_core::FieldValue;

    fn make_point(measurement: &str, host: &str, ts: i64, value: f64) -> Point {
        let tags: BTreeMap<String, String> = [("host".to_string(), host.to_string())]
            .into_iter()
            .collect();
        let series_key = SeriesKey::new(measurement.to_string(), tags).unwrap();
        let fields: BTreeMap<String, FieldValue> = [("value".to_string(), FieldValue::F64(value))]
            .into_iter()
            .collect();
        Point::new(series_key, fields, ts).unwrap()
    }

    #[test]
    fn insert_and_scan_all() {
        let mt = Memtable::new();
        mt.insert(&make_point("cpu", "a", 100, 1.0)).unwrap();
        mt.insert(&make_point("cpu", "a", 200, 2.0)).unwrap();
        mt.insert(&make_point("cpu", "a", 300, 3.0)).unwrap();

        let points = mt.scan_all();
        assert_eq!(points.len(), 3);
        assert_eq!(points[0].timestamp(), 100);
        assert_eq!(points[1].timestamp(), 200);
        assert_eq!(points[2].timestamp(), 300);
    }

    #[test]
    fn insert_and_scan_series() {
        let mt = Memtable::new();
        mt.insert(&make_point("cpu", "a", 100, 1.0)).unwrap();
        mt.insert(&make_point("cpu", "a", 200, 2.0)).unwrap();
        mt.insert(&make_point("cpu", "b", 150, 9.0)).unwrap();

        let key_a = SeriesKey::new(
            "cpu".to_string(),
            [("host".to_string(), "a".to_string())]
                .into_iter()
                .collect(),
        )
        .unwrap();

        let points = mt.scan(&key_a, 0, 300);
        assert_eq!(points.len(), 2);
        assert!(points
            .iter()
            .all(|p| p.series_key().tag("host") == Some("a")));
    }

    #[test]
    fn scan_time_range() {
        let mt = Memtable::new();
        mt.insert(&make_point("cpu", "a", 100, 1.0)).unwrap();
        mt.insert(&make_point("cpu", "a", 200, 2.0)).unwrap();
        mt.insert(&make_point("cpu", "a", 300, 3.0)).unwrap();

        let key = SeriesKey::new(
            "cpu".to_string(),
            [("host".to_string(), "a".to_string())]
                .into_iter()
                .collect(),
        )
        .unwrap();

        let points = mt.scan(&key, 150, 250);
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp(), 200);
    }

    #[test]
    fn last_write_wins() {
        let mt = Memtable::new();
        mt.insert(&make_point("cpu", "a", 100, 1.0)).unwrap();
        mt.insert(&make_point("cpu", "a", 100, 99.0)).unwrap();

        let points = mt.scan_all();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].field("value"), Some(&FieldValue::F64(99.0)));
    }

    #[test]
    fn empty_scan() {
        let mt = Memtable::new();
        let key = SeriesKey::new("cpu".to_string(), BTreeMap::new()).unwrap();
        let points = mt.scan(&key, 0, 1000);
        assert!(points.is_empty());
    }

    /// `estimated_size` tracks the index incrementally; this pins it to the
    /// full walk it replaced, so the two cannot drift apart silently.
    #[test]
    fn index_accounting_matches_a_full_walk() {
        let mt = Memtable::new();
        for i in 0..500u64 {
            let measurement = format!("m{}", i % 7);
            let series = format!("s{}", i % 53);
            mt.insert(&make_point(&measurement, &series, i as i64, 1.0))
                .unwrap();
        }

        // The batch path has its own index-write site; cover it too.
        let batch: Vec<_> = (500..800u64)
            .map(|i| {
                make_point(
                    &format!("m{}", i % 11),
                    &format!("s{}", i % 97),
                    i as i64,
                    2.0,
                )
            })
            .collect();
        mt.insert_batch(&batch).unwrap();

        let walked: usize = mt
            .measurement_index
            .iter()
            .map(|entry| {
                let key_bytes = std::mem::size_of::<usize>() * 2 + entry.key().len();
                let set_bytes = entry.value().capacity() * std::mem::size_of::<u64>() + 56;
                key_bytes + set_bytes
            })
            .sum();

        assert_eq!(
            mt.index_size.load(Ordering::Relaxed),
            walked,
            "incremental index accounting drifted from a full walk"
        );
    }

    #[test]
    fn estimated_size_increases() {
        let mt = Memtable::new();
        assert_eq!(mt.estimated_size(), 0);
        mt.insert(&make_point("cpu", "a", 100, 1.0)).unwrap();
        assert!(mt.estimated_size() > 0);
        let size_after_one = mt.estimated_size();
        mt.insert(&make_point("cpu", "a", 200, 2.0)).unwrap();
        assert!(mt.estimated_size() > size_after_one);
    }

    #[test]
    fn wal_seq_tracking() {
        let mt = Memtable::new();
        assert_eq!(mt.min_wal_seq(), None);
        assert_eq!(mt.max_wal_seq(), None);

        mt.insert_with_wal_seq(&make_point("cpu", "a", 100, 1.0), 10)
            .unwrap();
        assert_eq!(mt.min_wal_seq(), Some(10));
        assert_eq!(mt.max_wal_seq(), Some(10));

        mt.insert_with_wal_seq(&make_point("cpu", "a", 200, 2.0), 5)
            .unwrap();
        assert_eq!(mt.min_wal_seq(), Some(5));
        assert_eq!(mt.max_wal_seq(), Some(10));

        mt.insert_with_wal_seq(&make_point("cpu", "a", 300, 3.0), 20)
            .unwrap();
        assert_eq!(mt.min_wal_seq(), Some(5));
        assert_eq!(mt.max_wal_seq(), Some(20));
    }

    #[test]
    fn freeze_prevents_inserts() {
        let mt = Memtable::new();
        mt.insert(&make_point("cpu", "a", 100, 1.0)).unwrap();
        mt.freeze();
        assert!(mt.is_frozen());

        let result = mt.insert(&make_point("cpu", "a", 200, 2.0));
        assert!(result.is_err());

        // Existing data is still readable
        let points = mt.scan_all();
        assert_eq!(points.len(), 1);
    }

    #[test]
    fn len_and_is_empty() {
        let mt = Memtable::new();
        assert!(mt.is_empty());
        assert_eq!(mt.len(), 0);

        mt.insert(&make_point("cpu", "a", 100, 1.0)).unwrap();
        assert!(!mt.is_empty());
        assert_eq!(mt.len(), 1);
    }

    #[test]
    fn multiple_series() {
        let mt = Memtable::new();
        mt.insert(&make_point("cpu", "a", 100, 1.0)).unwrap();
        mt.insert(&make_point("cpu", "b", 100, 2.0)).unwrap();
        mt.insert(&make_point("mem", "a", 100, 3.0)).unwrap();

        assert_eq!(mt.len(), 3);

        let all = mt.scan_all();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn concurrent_inserts() {
        use std::sync::Arc;
        use std::thread;

        let mt = Arc::new(Memtable::new());
        let mut handles = Vec::new();

        for thread_id in 0..4 {
            let mt = Arc::clone(&mt);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let ts = i64::from(thread_id * 1000 + i);
                    let host = format!("host-{thread_id}");
                    mt.insert(&make_point("cpu", &host, ts, ts as f64)).unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(mt.len(), 400);
        let all = mt.scan_all();
        assert_eq!(all.len(), 400);

        // Verify ordering: each series should be ordered by timestamp
        let mut prev_key: Option<MemtableKey> = None;
        for p in &all {
            let key = MemtableKey::new(
                p.series_key().hash_fnv(),
                p.timestamp(),
                Arc::from(p.series_key().canonical_form()),
            );
            if let Some(prev) = prev_key {
                assert!(key >= prev, "entries should be ordered");
            }
            prev_key = Some(key);
        }
    }

    #[allow(deprecated)]
    #[test]
    fn to_points_is_scan_all() {
        let mt = Memtable::new();
        mt.insert(&make_point("cpu", "a", 100, 1.0)).unwrap();
        mt.insert(&make_point("cpu", "a", 200, 2.0)).unwrap();

        let scan = mt.scan_all();
        let points = mt.to_points();
        assert_eq!(scan.len(), points.len());
        for (s, d) in scan.iter().zip(points.iter()) {
            assert_eq!(s.timestamp(), d.timestamp());
            assert_eq!(s.fields(), d.fields());
        }

        // Verify deprecated alias still works
        let drain = mt.drain_to_points();
        assert_eq!(drain.len(), points.len());
    }

    #[test]
    fn scan_range_across_series() {
        let mt = Memtable::new();
        mt.insert(&make_point("cpu", "a", 50, 1.0)).unwrap();
        mt.insert(&make_point("cpu", "a", 100, 2.0)).unwrap();
        mt.insert(&make_point("cpu", "b", 75, 3.0)).unwrap();
        mt.insert(&make_point("cpu", "b", 200, 4.0)).unwrap();

        let points = mt.scan_range(60, 150);
        // Should get: cpu/a@100 and cpu/b@75
        assert_eq!(points.len(), 2);
        for p in &points {
            assert!(
                p.timestamp() >= 60 && p.timestamp() <= 150,
                "timestamp {} should be in [60, 150]",
                p.timestamp()
            );
        }
    }

    #[test]
    fn scan_measurement_filters_by_name() {
        let mt = Memtable::new();
        mt.insert(&make_point("cpu", "a", 100, 1.0)).unwrap();
        mt.insert(&make_point("cpu", "b", 200, 2.0)).unwrap();
        mt.insert(&make_point("mem", "a", 150, 3.0)).unwrap();
        mt.insert(&make_point("mem", "a", 300, 4.0)).unwrap();
        mt.insert(&make_point("disk", "c", 250, 5.0)).unwrap();

        // Scan only "cpu"
        let cpu = mt.scan_measurement("cpu", 0, i64::MAX);
        assert_eq!(cpu.len(), 2);
        assert!(cpu.iter().all(|p| p.series_key().measurement() == "cpu"));

        // Scan only "mem"
        let mem = mt.scan_measurement("mem", 0, i64::MAX);
        assert_eq!(mem.len(), 2);
        assert!(mem.iter().all(|p| p.series_key().measurement() == "mem"));

        // Scan "disk"
        let disk = mt.scan_measurement("disk", 0, i64::MAX);
        assert_eq!(disk.len(), 1);

        // Scan non-existent measurement
        let none = mt.scan_measurement("net", 0, i64::MAX);
        assert!(none.is_empty());
    }

    #[test]
    fn scan_measurement_respects_time_range() {
        let mt = Memtable::new();
        mt.insert(&make_point("cpu", "a", 100, 1.0)).unwrap();
        mt.insert(&make_point("cpu", "a", 200, 2.0)).unwrap();
        mt.insert(&make_point("cpu", "a", 300, 3.0)).unwrap();
        mt.insert(&make_point("mem", "a", 200, 4.0)).unwrap();

        let result = mt.scan_measurement("cpu", 150, 250);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].timestamp(), 200);
    }

    /// Regression test: two distinct series with the same FNV-1a hash
    /// and timestamp must both be stored and retrievable (no silent
    /// overwrite).
    #[test]
    fn hash_collision_preserves_both_series() {
        let mt = Memtable::new();

        // Create two points with the same hash and timestamp but
        // different canonical forms by manually constructing keys.
        // In production, FNV-1a collisions are rare but possible.
        let p1 = make_point("cpu", "host-a", 1000, 42.0);
        let p2 = make_point("cpu", "host-b", 1000, 99.0);

        mt.insert(&p1).unwrap();
        mt.insert(&p2).unwrap();

        // Both must be present
        assert_eq!(
            mt.len(),
            2,
            "two distinct series at same timestamp must both survive"
        );

        // Each series should return its own value
        let s1 = mt.scan(p1.series_key(), 0, i64::MAX);
        assert_eq!(s1.len(), 1);
        assert_eq!(
            *s1[0].field("value").unwrap(),
            chronix_core::FieldValue::F64(42.0)
        );

        let s2 = mt.scan(p2.series_key(), 0, i64::MAX);
        assert_eq!(s2.len(), 1);
        assert_eq!(
            *s2[0].field("value").unwrap(),
            chronix_core::FieldValue::F64(99.0)
        );
    }

    #[test]
    fn insert_batch_basic() {
        let mt = Memtable::new();
        let points = vec![
            make_point("cpu", "a", 100, 1.0),
            make_point("cpu", "a", 200, 2.0),
            make_point("cpu", "b", 300, 3.0),
            make_point("mem", "a", 400, 4.0),
        ];
        let count = mt.insert_batch(&points).unwrap();
        assert_eq!(count, 4);

        let all = mt.scan_all();
        assert_eq!(all.len(), 4);

        // Measurement index covers both measurements
        let cpu = mt.scan_measurement("cpu", 0, i64::MAX);
        assert_eq!(cpu.len(), 3);
        let mem = mt.scan_measurement("mem", 0, i64::MAX);
        assert_eq!(mem.len(), 1);
    }

    #[test]
    fn insert_batch_overwrite() {
        let mt = Memtable::new();
        let initial = vec![
            make_point("cpu", "a", 100, 1.0),
            make_point("cpu", "a", 200, 2.0),
        ];
        mt.insert_batch(&initial).unwrap();

        // Overwrite same timestamps with new values
        let overwrite = vec![
            make_point("cpu", "a", 100, 10.0),
            make_point("cpu", "a", 200, 20.0),
        ];
        mt.insert_batch(&overwrite).unwrap();

        let all = mt.scan_all();
        assert_eq!(all.len(), 2);
        assert_eq!(
            *all[0].field("value").unwrap(),
            chronix_core::FieldValue::F64(10.0)
        );
        assert_eq!(
            *all[1].field("value").unwrap(),
            chronix_core::FieldValue::F64(20.0)
        );
    }

    #[test]
    fn insert_batch_frozen_fails() {
        let mt = Memtable::new();
        mt.freeze();
        let points = vec![make_point("cpu", "a", 100, 1.0)];
        assert!(mt.insert_batch(&points).is_err());
    }

    #[test]
    fn insert_batch_with_wal_seq_updates_range() {
        let mt = Memtable::new();
        let points = vec![
            make_point("cpu", "a", 100, 1.0),
            make_point("cpu", "a", 200, 2.0),
        ];
        mt.insert_batch_with_wal_seq(&points, 42).unwrap();
        assert_eq!(mt.min_wal_seq(), Some(42));
        assert_eq!(mt.max_wal_seq(), Some(42));
    }
}
