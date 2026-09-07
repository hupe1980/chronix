//! Segment metadata index — one entry per live segment.
//!
//! Holds each segment's header, column statistics and per-tag bloom filters
//! so the query planner can prune without opening a file. Populated at open
//! from the catalog and maintained by flush, compaction, retention, delete
//! and GC.
//!
//! # This is an index, not a cache
//!
//! There is no eviction and no hit rate: the entry set *is* the live segment
//! set, bounded by the segments on disk exactly as the catalog is. It used to
//! advertise a `max_entries` cap that only logged — inserts succeeded
//! regardless — beside a comment putting an entry at "~1 KB", which no
//! per-tag bloom filter was going to honour.
//!
//! [`MetadataCache::memory_bytes`] replaces both: maintained as entries come
//! and go, summed into the resident-memory total, exported as
//! `chronix_metadata_cache_bytes`.

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
/// # Size
///
/// One entry per live segment, and [`memory_bytes`](Self::memory_bytes)
/// reports what they cost. The figure is maintained as entries are inserted
/// and removed rather than recomputed, because `statistics()` runs on every
/// metrics scrape and walking the map there would make the exporter the most
/// expensive thing in the process.
pub struct MetadataCache {
    /// Map from segment ID → cached metadata, `Arc`-wrapped to avoid a deep clone on read.
    entries: Arc<RwLock<HashMap<SegmentId, Arc<CachedSegmentMeta>>>>,
    /// Running total of [`entry_bytes`] over `entries`, maintained under the
    /// same lock so it cannot drift from the map it describes.
    bytes: Arc<RwLock<usize>>,
}

/// What one entry costs: the struct, its header, and every column's name,
/// statistics and bloom filter.
///
/// The bloom filter is the term that matters and the one the old "~1 KB per
/// entry" estimate omitted — it is a `Vec<u8>` sized by the segment's tag
/// cardinality, so a wide segment's entry is orders of magnitude larger than
/// a narrow one's.
fn entry_bytes(meta: &CachedSegmentMeta) -> usize {
    std::mem::size_of::<CachedSegmentMeta>()
        + meta.columns.capacity() * std::mem::size_of::<ColumnMeta>()
        + meta
            .columns
            .iter()
            .map(|c| c.name.capacity() + c.bloom_filter.as_ref().map_or(0, Vec::capacity))
            .sum::<usize>()
}

impl MetadataCache {
    /// Create an empty metadata index.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            bytes: Arc::new(RwLock::new(0)),
        }
    }

    /// Insert or update metadata for a segment.
    pub fn insert(&self, meta: CachedSegmentMeta) {
        let added = entry_bytes(&meta);
        let mut map = self.entries.write();
        let mut bytes = self.bytes.write();
        let replaced = map.insert(meta.segment_id, Arc::new(meta));
        *bytes = bytes.saturating_sub(replaced.as_deref().map_or(0, entry_bytes)) + added;
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
        let mut map = self.entries.write();
        let mut bytes = self.bytes.write();
        if let Some(removed) = map.remove(&segment_id) {
            *bytes = bytes.saturating_sub(entry_bytes(&removed));
        }
    }

    /// Bytes the index holds, counted as entries come and go.
    ///
    /// Summed into the database's resident-memory total and exported as
    /// `chronix_metadata_cache_bytes`.
    #[must_use]
    pub fn memory_bytes(&self) -> usize {
        *self.bytes.read()
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
        let mut map = self.entries.write();
        let mut bytes = self.bytes.write();
        map.clear();
        *bytes = 0;
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
            .field("bytes", &*self.bytes.read())
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
            decimal_scale: None,
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
