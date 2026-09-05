//! Time-range index for segment pruning.
//!
//! The [`TimeIndex`] maintains a sorted collection of segment time ranges,
//! enabling O(log N) lookup to find segments that overlap a query's time
//! window. This is the first level of segment pruning.
//!
//! Internally backed by a `BTreeSet` keyed on `(min_ts, segment_id)` for
//! O(log N) insert and remove, replacing the former `Vec`-based approach
//! that required O(N) element shifting.

use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::Arc;

use parking_lot::RwLock;

use chronix_core::{SegmentId, Timestamp};

/// An entry in the time index representing a segment's time range.
#[derive(Debug, Clone, Eq)]
pub struct TimeIndexEntry {
    /// Unique segment identifier.
    pub segment_id: SegmentId,
    /// Minimum timestamp in the segment (inclusive).
    pub min_ts: Timestamp,
    /// Maximum timestamp in the segment (inclusive).
    pub max_ts: Timestamp,
}

impl PartialEq for TimeIndexEntry {
    fn eq(&self, other: &Self) -> bool {
        self.min_ts == other.min_ts && self.segment_id == other.segment_id
    }
}

impl PartialOrd for TimeIndexEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimeIndexEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.min_ts
            .cmp(&other.min_ts)
            .then_with(|| self.segment_id.cmp(&other.segment_id))
    }
}

impl TimeIndexEntry {
    /// Check whether this entry's time range overlaps with `[start, end]`.
    #[inline]
    #[must_use]
    pub fn overlaps(&self, start: Timestamp, end: Timestamp) -> bool {
        self.min_ts <= end && self.max_ts >= start
    }
}

/// Sorted time-range index for fast segment pruning.
///
/// Entries are kept in a `BTreeSet` ordered by `(min_ts, segment_id)` for
/// O(log N) insert and remove. Range queries use a BTree range scan.
/// Thread-safe via `Arc<RwLock<>>`.
///
/// # Complexity
///
/// - `segments_for_range`: O(log N + K) where K is the number of matching segments
/// - `add_segment`: O(log N) insert
/// - `remove_segment`: O(log N) remove
#[derive(Debug, Clone)]
pub struct TimeIndex {
    entries: Arc<RwLock<BTreeSet<TimeIndexEntry>>>,
    /// O(1) segment ID → (min_ts, max_ts) lookup for fast removal.
    segment_meta: Arc<RwLock<HashMap<SegmentId, (Timestamp, Timestamp)>>>,
}

impl TimeIndex {
    /// Create an empty time index.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(BTreeSet::new())),
            segment_meta: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Find all segments whose time range overlaps `[start_ts, end_ts]`.
    ///
    /// Uses a BTree range scan: iterates entries with `min_ts <= end_ts`
    /// and filters by `max_ts >= start_ts`. Entries with `min_ts > end_ts`
    /// are skipped entirely via the range upper bound.
    /// There is deliberately no bounded variant. One existed —
    /// `segments_for_range_limited`, documented for "pagination or preview
    /// queries" — whose only caller passed `usize::MAX`. Dropping segments
    /// from a scan is not pagination: it returns an arbitrary subset of the
    /// rows with no error and no way for the caller to tell.
    #[must_use]
    pub fn segments_for_range(&self, start_ts: Timestamp, end_ts: Timestamp) -> Vec<SegmentId> {
        let entries = self.entries.read();
        if entries.is_empty() {
            return Vec::new();
        }
        // Every entry with `min_ts <= end_ts`; `max_ts >= start_ts` filters
        // the rest.
        let upper = TimeIndexEntry {
            segment_id: SegmentId(u64::MAX),
            min_ts: end_ts,
            max_ts: Timestamp::MAX,
        };
        entries
            .range((Bound::Unbounded, Bound::Included(&upper)))
            .filter(|e| e.max_ts >= start_ts)
            .map(|e| e.segment_id)
            .collect()
    }

    /// Add a segment to the index.
    ///
    /// Acquires locks in canonical order (entries → meta)
    /// to prevent deadlocks with `remove_segment()`.
    pub fn add_segment(&self, entry: TimeIndexEntry) {
        let seg_id = entry.segment_id;
        let meta_val = (entry.min_ts, entry.max_ts);
        let mut entries = self.entries.write();
        let mut meta = self.segment_meta.write();
        entries.insert(entry);
        meta.insert(seg_id, meta_val);
    }

    /// Remove a segment from the index by its ID.
    ///
    /// Acquires locks in canonical order (entries → meta),
    /// matching `add_segment()`, to prevent deadlocks. Both structures
    /// are updated atomically under their respective write locks.
    /// Returns `true` if the segment was found and removed.
    #[must_use]
    pub fn remove_segment(&self, segment_id: SegmentId) -> bool {
        let mut entries = self.entries.write();
        let mut meta = self.segment_meta.write();
        if let Some(&(min_ts, max_ts)) = meta.get(&segment_id) {
            let key = TimeIndexEntry {
                segment_id,
                min_ts,
                max_ts,
            };
            let removed = entries.remove(&key);
            if removed {
                meta.remove(&segment_id);
            }
            removed
        } else {
            false
        }
    }

    /// Returns the number of entries in the index.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Returns `true` if the index is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Returns all entries sorted by `(min_ts, segment_id)` (for serialization/persistence).
    #[must_use]
    pub fn all_entries(&self) -> Vec<TimeIndexEntry> {
        self.entries.read().iter().cloned().collect()
    }
}

impl Default for TimeIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: u64, min: i64, max: i64) -> TimeIndexEntry {
        TimeIndexEntry {
            segment_id: SegmentId(id),
            min_ts: min,
            max_ts: max,
        }
    }

    #[test]
    fn empty_index_returns_nothing() {
        let idx = TimeIndex::new();
        let result = idx.segments_for_range(0, 1000);
        assert!(result.is_empty());
    }

    #[test]
    fn single_segment_match() {
        let idx = TimeIndex::new();
        idx.add_segment(entry(1, 100, 200));

        // Exact overlap
        assert_eq!(idx.segments_for_range(100, 200), vec![SegmentId(1)]);
        // Partial overlap
        assert_eq!(idx.segments_for_range(150, 300), vec![SegmentId(1)]);
        // No overlap (before)
        assert!(idx.segments_for_range(0, 99).is_empty());
        // No overlap (after)
        assert!(idx.segments_for_range(201, 300).is_empty());
        // Touch at boundary
        assert_eq!(idx.segments_for_range(200, 300), vec![SegmentId(1)]);
        assert_eq!(idx.segments_for_range(0, 100), vec![SegmentId(1)]);
    }

    #[test]
    fn multiple_segments_pruning() {
        let idx = TimeIndex::new();
        // Segments: [100,200], [300,400], [500,600], [700,800]
        idx.add_segment(entry(1, 100, 200));
        idx.add_segment(entry(2, 300, 400));
        idx.add_segment(entry(3, 500, 600));
        idx.add_segment(entry(4, 700, 800));

        // Query [250,550] should match segments 2 and 3
        let result = idx.segments_for_range(250, 550);
        assert_eq!(result, vec![SegmentId(2), SegmentId(3)]);

        // Query spanning all
        let result = idx.segments_for_range(0, 1000);
        assert_eq!(
            result,
            vec![SegmentId(1), SegmentId(2), SegmentId(3), SegmentId(4)]
        );

        // Query matching none
        assert!(idx.segments_for_range(900, 1000).is_empty());
    }

    #[test]
    fn overlapping_segments() {
        let idx = TimeIndex::new();
        // Segments overlap: [100,300], [200,400], [350,500]
        idx.add_segment(entry(1, 100, 300));
        idx.add_segment(entry(2, 200, 400));
        idx.add_segment(entry(3, 350, 500));

        let result = idx.segments_for_range(250, 350);
        assert_eq!(result, vec![SegmentId(1), SegmentId(2), SegmentId(3)]);
    }

    #[test]
    fn add_maintains_sort_order() {
        let idx = TimeIndex::new();
        idx.add_segment(entry(3, 500, 600));
        idx.add_segment(entry(1, 100, 200));
        idx.add_segment(entry(2, 300, 400));

        let entries = idx.all_entries();
        assert_eq!(entries[0].min_ts, 100);
        assert_eq!(entries[1].min_ts, 300);
        assert_eq!(entries[2].min_ts, 500);
    }

    #[test]
    fn remove_segment() {
        let idx = TimeIndex::new();
        idx.add_segment(entry(1, 100, 200));
        idx.add_segment(entry(2, 300, 400));

        assert!(idx.remove_segment(SegmentId(1)));
        assert_eq!(idx.len(), 1);
        assert!(!idx.remove_segment(SegmentId(99)));
    }

    #[test]
    fn hundred_segments_pruning() {
        let idx = TimeIndex::new();

        // Add 100 segments: [i*1000, i*1000+500] for i in 0..100
        for i in 0u64..100 {
            #[allow(clippy::cast_possible_wrap)]
            let start = (i as i64) * 1000;
            idx.add_segment(entry(i, start, start + 500));
        }
        assert_eq!(idx.len(), 100);

        // Query [5000, 7500] should match segments 5,6,7
        // seg 5: [5000,5500], seg 6: [6000,6500], seg 7: [7000,7500]
        let result = idx.segments_for_range(5000, 7500);
        assert_eq!(result, vec![SegmentId(5), SegmentId(6), SegmentId(7)]);

        // Query for a single point
        let result = idx.segments_for_range(50250, 50250);
        assert_eq!(result, vec![SegmentId(50)]);
    }

    #[test]
    fn entry_overlaps() {
        let e = entry(1, 100, 200);
        assert!(e.overlaps(100, 200)); // exact
        assert!(e.overlaps(50, 150)); // left overlap
        assert!(e.overlaps(150, 250)); // right overlap
        assert!(e.overlaps(120, 180)); // contained
        assert!(e.overlaps(50, 250)); // spanning
        assert!(!e.overlaps(0, 99)); // before
        assert!(!e.overlaps(201, 300)); // after
    }
}
