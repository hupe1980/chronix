//! Metadata Cache — in-memory cache for segment metadata.
//!
//! Caches segment headers, bloom filters, and column statistics permanently
//! in memory. Updated on segment creation, compaction, and deletion.
//! Memory cost is approximately 1 KB per segment — no eviction needed.
//!
//! # Eviction policy
//!
//! This cache intentionally has **no LRU eviction**.  Each entry is ~1 KB
//! (header + column stats), so even 100 000 segments consume only ~100 MB.
//! Segments are explicitly removed from the cache when they are deleted or
//! compacted away (see `MetadataCache::remove`).  If segment counts ever
//! grow to the point where this becomes a concern, an LRU bound or
//! generation-based eviction can be layered on top.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::segment::{ColumnMeta, SegmentHeader};
use chronix_core::SegmentId;

/// Cached metadata for a single segment.
#[derive(Debug, Clone)]
pub struct CachedSegmentMeta {
    /// Segment ID.
    pub segment_id: SegmentId,
    /// Parsed segment header.
    pub header: SegmentHeader,
    /// Per-column metadata (name, type, role, stats).
    pub columns: Vec<ColumnMeta>,
}

/// In-memory metadata cache for all segments.
///
/// Populated on startup from the segment catalog and updated as
/// segments are created, compacted, or deleted.
///
/// # Thread Safety
///
/// Uses `RwLock<HashMap>` for concurrent read access.
///
/// # Capacity bound
///
/// An optional `max_entries` limit caps the number of cached segments.
/// When the limit is reached, new inserts are still accepted (to keep
/// hot metadata available) but a warning is emitted so operators can
/// investigate.  Each entry is ~1 KB, so even the default cap of
/// 500 000 entries consumes at most ~500 MB — a safe upper bound for
/// most deployments.  Set `max_entries = 0` to disable the cap.
pub struct MetadataCache {
    /// Map from segment ID → cached metadata, `Arc`-wrapped to avoid a deep clone on read.
    entries: Arc<RwLock<HashMap<SegmentId, Arc<CachedSegmentMeta>>>>,
    /// Maximum number of entries before warnings are emitted (0 = unlimited).
    max_entries: usize,
}

/// Default maximum number of cached segment metadata entries.
///
/// At ~1 KB per entry this allows up to ~500 MB of metadata, which
/// is a safe upper bound for the vast majority of deployments.
const DEFAULT_MAX_ENTRIES: usize = 500_000;

impl MetadataCache {
    /// Create an empty metadata cache with the default capacity bound.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            max_entries: DEFAULT_MAX_ENTRIES,
        }
    }

    /// Create an empty metadata cache with a custom capacity bound.
    ///
    /// Pass `0` to disable the capacity warning.
    #[must_use]
    pub fn with_max_entries(max_entries: usize) -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            max_entries,
        }
    }

    /// Insert or update metadata for a segment.
    ///
    /// If the cache exceeds [`max_entries`](Self::with_max_entries) a
    /// warning is logged but the insert still succeeds so that hot
    /// metadata is never silently dropped.
    pub fn insert(&self, meta: CachedSegmentMeta) {
        let mut map = self.entries.write();
        map.insert(meta.segment_id, Arc::new(meta));
        if self.max_entries > 0 && map.len() > self.max_entries {
            tracing::warn!(
                cache_len = map.len(),
                max_entries = self.max_entries,
                "MetadataCache exceeded max_entries — consider raising the limit or investigating segment count",
            );
        }
    }

    /// Look up metadata for a segment.
    ///
    /// Returns an `Arc` for cheap cloning.
    #[must_use]
    pub fn get(&self, segment_id: SegmentId) -> Option<Arc<CachedSegmentMeta>> {
        self.entries.read().get(&segment_id).cloned()
    }

    /// Remove metadata for a segment.
    pub fn remove(&self, segment_id: SegmentId) {
        self.entries.write().remove(&segment_id);
    }

    /// Returns the number of cached segments.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Returns `true` if the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Clear all cached metadata.
    pub fn clear(&self) {
        self.entries.write().clear();
    }

    /// Returns column metadata for a segment, if cached.
    #[must_use]
    pub fn columns_for(&self, segment_id: SegmentId) -> Option<Vec<ColumnMeta>> {
        self.entries
            .read()
            .get(&segment_id)
            .map(|m| m.columns.clone())
    }
}

impl Default for MetadataCache {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MetadataCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataCache")
            .field("entries", &self.entries.read().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::ColumnStats;

    fn make_header() -> SegmentHeader {
        SegmentHeader {
            version: 1,
            flags: 0,
            created_at: 0,
            min_timestamp: 100,
            max_timestamp: 200,
            row_count: 1000,
            column_count: 3,
            series_count: 5,
            compression: 0,
            sort_order: 1,
        }
    }

    fn make_column_meta(name: &str) -> ColumnMeta {
        ColumnMeta {
            name: name.to_string(),
            data_type: 2,
            role: 2,
            default_encoding: 1,
            stats: ColumnStats::empty(),
            bloom_filter: None,
            row_group_blooms: None,
            encrypted: false,
            key_id: None,
        }
    }

    #[test]
    fn insert_and_get() {
        let cache = MetadataCache::new();
        cache.insert(CachedSegmentMeta {
            segment_id: SegmentId(1),
            header: make_header(),
            columns: vec![make_column_meta("cpu")],
        });

        let result = cache.get(SegmentId(1));
        assert!(result.is_some());
        assert_eq!(result.unwrap().columns.len(), 1);
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let cache = MetadataCache::new();
        assert!(cache.get(SegmentId(99)).is_none());
    }

    #[test]
    fn remove_entry() {
        let cache = MetadataCache::new();
        cache.insert(CachedSegmentMeta {
            segment_id: SegmentId(1),
            header: make_header(),
            columns: Vec::new(),
        });
        assert_eq!(cache.len(), 1);

        cache.remove(SegmentId(1));
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn columns_for_segment() {
        let cache = MetadataCache::new();
        cache.insert(CachedSegmentMeta {
            segment_id: SegmentId(1),
            header: make_header(),
            columns: vec![make_column_meta("cpu"), make_column_meta("mem")],
        });

        let cols = cache.columns_for(SegmentId(1)).unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "cpu");
    }
}
