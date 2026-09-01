//! Inverted tag index — maps `"tag_key=tag_value"` → `Vec<SegmentId>`.
//!
//! Used to quickly prune segments that cannot contain a queried tag value.
//! Rebuilt from segment metadata on startup; updated on segment creation
//! and deletion.

use std::collections::{HashMap, HashSet};

use parking_lot::RwLock;

use chronix_core::SegmentId;

/// Build a tag index key without `format!` overhead.
///
/// Uses [`chronix_core::KV_SEPARATOR`] rather than `=` because `=` is legal in
/// both tag keys and tag values: `("a=b", "c")` and `("a", "b=c")` would
/// otherwise produce the same index key. That only ever caused false
/// positives (extra segments scanned, then filtered), but the separator is
/// reserved and free, so the ambiguity does not need to exist.
#[inline]
fn make_tag_key(key: &str, value: &str) -> String {
    let mut s = String::with_capacity(key.len() + 1 + value.len());
    s.push_str(key);
    s.push(chronix_core::KV_SEPARATOR);
    s.push_str(value);
    s
}

/// Thread-safe inverted tag index.
///
/// Maps `"tag_key=tag_value"` → set of segment IDs that contain that
/// tag value. This enables O(1) lookup to determine which segments
/// could possibly match a tag filter, avoiding full segment scans.
///
/// A reverse index (`segment_id` → set of tag keys) enables O(k) segment
/// removal where k is the number of tag values for that segment, instead
/// of scanning all entries.
///
/// # Thread Safety
///
/// A single `RwLock<InvertedState>` is used intentionally —
/// `remove_segment` must atomically update all three maps (index, reverse,
/// key_values) to prevent partial state on crash/panic. `parking_lot::RwLock`
/// allows concurrent readers; writers only hold the lock during short
/// segment catalog mutations (add/remove), which are infrequent relative
/// to the query read-path.
#[derive(Debug)]
pub struct TagInvertedIndex {
    state: RwLock<InvertedState>,
    /// Maximum number of distinct tag values tracked per tag key.
    ///
    /// When a key exceeds this cardinality, new values for that key are
    /// silently dropped and a warning is emitted. This prevents unbounded
    /// memory growth on high-cardinality workloads.
    max_tag_cardinality: usize,
}

/// Inner state for the inverted tag index, guarded by a single lock.
#[derive(Debug, Default)]
struct InvertedState {
    /// Map from "key=value" → set of segment IDs.
    index: HashMap<String, HashSet<SegmentId>>,
    /// Reverse map: segment ID → set of "key=value" strings.
    reverse: HashMap<SegmentId, HashSet<String>>,
    /// Secondary index mapping tag key → set of distinct values.
    key_values: HashMap<String, HashSet<String>>,
}

impl TagInvertedIndex {
    /// Create an empty inverted tag index.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: RwLock::new(InvertedState::default()),
            max_tag_cardinality: 1_000_000,
        }
    }

    /// Create an empty inverted tag index with a custom cardinality limit.
    ///
    /// Limits the number of distinct values tracked per tag key.
    #[must_use]
    pub fn with_max_cardinality(max_tag_cardinality: usize) -> Self {
        Self {
            state: RwLock::new(InvertedState::default()),
            max_tag_cardinality,
        }
    }

    /// Register a segment's tag values in the index.
    ///
    /// For each tag column in the segment's catalog entry, the distinct
    /// values (if available via column stats or direct metadata) are
    /// mapped to the segment ID.
    pub fn add_segment(&self, segment_id: SegmentId, tag_values: &[(&str, &str)]) {
        let mut s = self.state.write();
        let mut rev_keys = Vec::with_capacity(tag_values.len());
        for &(key, value) in tag_values {
            let entry_key = make_tag_key(key, value);
            s.index
                .entry(entry_key.clone())
                .or_default()
                .insert(segment_id);
            rev_keys.push(entry_key);
            // Maintain key→values secondary index
            // Enforce cardinality limit per tag key.
            let kv_entry = s.key_values.entry(key.to_string()).or_default();
            if kv_entry.len() < self.max_tag_cardinality {
                kv_entry.insert(value.to_string());
            } else if !kv_entry.contains(value) {
                tracing::warn!(
                    key,
                    cardinality = kv_entry.len(),
                    limit = self.max_tag_cardinality,
                    "tag cardinality limit reached — new values dropped"
                );
                metrics::counter!("chronix_tag_cardinality_limit_reached_total", "key" => key.to_string())
                    .increment(1);
            }
        }
        s.reverse.entry(segment_id).or_default().extend(rev_keys);
    }

    /// Remove a segment from the index in O(k) time where k is the
    /// number of tag values for that segment.
    pub fn remove_segment(&self, segment_id: SegmentId) {
        let mut s = self.state.write();

        if let Some(keys) = s.reverse.remove(&segment_id) {
            for key in &keys {
                if let Some(segments) = s.index.get_mut(key) {
                    segments.remove(&segment_id);
                    if segments.is_empty() {
                        s.index.remove(key);
                        // Remove from key_values when no segments
                        // reference this key=value pair any more.
                        if let Some(pos) = key.find(chronix_core::KV_SEPARATOR) {
                            let tag_key = &key[..pos];
                            let tag_val = &key[pos + 1..];
                            if let Some(vals) = s.key_values.get_mut(tag_key) {
                                vals.remove(tag_val);
                                if vals.is_empty() {
                                    s.key_values.remove(tag_key);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Find segments that contain a specific tag value.
    #[must_use]
    pub fn segments_for_tag(&self, key: &str, value: &str) -> Vec<SegmentId> {
        let entry_key = make_tag_key(key, value);
        let s = self.state.read();
        let mut result: Vec<SegmentId> = s
            .index
            .get(&entry_key)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        result.sort();
        result
    }

    /// Find segments matching ALL tag filters (intersection).
    ///
    /// Returns the intersection of segments across all tag filters.
    /// If filters is empty, returns an empty Vec.
    #[must_use]
    pub fn segments_for_tags(&self, filters: &[(&str, &str)]) -> Vec<SegmentId> {
        if filters.is_empty() {
            return Vec::new();
        }

        let s = self.state.read();
        let mut result: Option<HashSet<SegmentId>> = None;

        for &(key, value) in filters {
            let entry_key = make_tag_key(key, value);
            let matching = s.index.get(&entry_key).cloned().unwrap_or_default();

            result = Some(match result {
                Some(existing) => existing.intersection(&matching).copied().collect(),
                None => matching,
            });
        }

        let mut out: Vec<SegmentId> = result.map_or_else(Vec::new, |s| s.into_iter().collect());
        out.sort();
        out
    }

    /// Atomically replace multiple old segments with a single new segment.
    ///
    /// Used during compaction to avoid a window where the old segments have
    /// been removed but the new one hasn't been added yet, which would cause
    /// concurrent tag-filtered queries to miss data.
    pub fn replace_segments(
        &self,
        old_segment_ids: &[SegmentId],
        new_segment_id: SegmentId,
        new_tag_values: &[(&str, &str)],
    ) {
        let mut s = self.state.write();

        // Remove old segments
        for &old_id in old_segment_ids {
            if let Some(keys) = s.reverse.remove(&old_id) {
                for key in &keys {
                    if let Some(segments) = s.index.get_mut(key) {
                        segments.remove(&old_id);
                        if segments.is_empty() {
                            s.index.remove(key);
                            if let Some(pos) = key.find(chronix_core::KV_SEPARATOR) {
                                let tag_key = &key[..pos];
                                let tag_val = &key[pos + 1..];
                                if let Some(vals) = s.key_values.get_mut(tag_key) {
                                    vals.remove(tag_val);
                                    if vals.is_empty() {
                                        s.key_values.remove(tag_key);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Add new segment
        let mut rev_keys = Vec::with_capacity(new_tag_values.len());
        for &(key, value) in new_tag_values {
            let entry_key = make_tag_key(key, value);
            s.index
                .entry(entry_key.clone())
                .or_default()
                .insert(new_segment_id);
            rev_keys.push(entry_key);
            s.key_values
                .entry(key.to_string())
                .or_default()
                .insert(value.to_string());
        }
        s.reverse
            .entry(new_segment_id)
            .or_default()
            .extend(rev_keys);
    }

    /// Clear the entire index (e.g., for a full rebuild).
    pub fn clear(&self) {
        let mut s = self.state.write();
        s.index.clear();
        s.reverse.clear();
        s.key_values.clear();
    }

    /// Returns the total number of indexed tag-value pairs.
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.state.read().index.len()
    }

    /// Returns all distinct values for a given tag key.
    ///
    /// O(1) lookup via the secondary `key_values` index
    /// instead of scanning all entries with `strip_prefix`.
    #[must_use]
    pub fn values_for_key(&self, key: &str) -> Vec<String> {
        let s = self.state.read();
        let mut values: Vec<String> = s
            .key_values
            .get(key)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default();
        values.sort();
        values
    }

    /// Returns all distinct tag keys present in the index.
    ///
    /// O(1) lookup via the secondary `key_values` index
    /// instead of scanning all entries.
    #[must_use]
    pub fn all_keys(&self) -> Vec<String> {
        let s = self.state.read();
        let mut keys: Vec<String> = s.key_values.keys().cloned().collect();
        keys.sort();
        keys
    }
}

impl Default for TagInvertedIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_query_tag() {
        let idx = TagInvertedIndex::new();
        idx.add_segment(SegmentId(1), &[("host", "srv1"), ("region", "us-east")]);
        idx.add_segment(SegmentId(2), &[("host", "srv2"), ("region", "us-east")]);
        idx.add_segment(SegmentId(3), &[("host", "srv1"), ("region", "eu-west")]);

        let segs = idx.segments_for_tag("host", "srv1");
        assert_eq!(segs.len(), 2);
        assert!(segs.contains(&SegmentId(1)));
        assert!(segs.contains(&SegmentId(3)));
    }

    #[test]
    fn intersection_multi_tag() {
        let idx = TagInvertedIndex::new();
        idx.add_segment(SegmentId(1), &[("host", "srv1"), ("region", "us-east")]);
        idx.add_segment(SegmentId(2), &[("host", "srv1"), ("region", "eu-west")]);
        idx.add_segment(SegmentId(3), &[("host", "srv2"), ("region", "us-east")]);

        let segs = idx.segments_for_tags(&[("host", "srv1"), ("region", "us-east")]);
        assert_eq!(segs.len(), 1);
        assert!(segs.contains(&SegmentId(1)));
    }

    #[test]
    fn remove_segment() {
        let idx = TagInvertedIndex::new();
        idx.add_segment(SegmentId(1), &[("host", "srv1")]);
        idx.add_segment(SegmentId(2), &[("host", "srv1")]);

        idx.remove_segment(SegmentId(1));
        let segs = idx.segments_for_tag("host", "srv1");
        assert_eq!(segs.len(), 1);
        assert!(segs.contains(&SegmentId(2)));
    }

    #[test]
    fn missing_tag_returns_empty() {
        let idx = TagInvertedIndex::new();
        idx.add_segment(SegmentId(1), &[("host", "srv1")]);

        assert!(idx.segments_for_tag("datacenter", "dc1").is_empty());
    }

    #[test]
    fn clear_empties_index() {
        let idx = TagInvertedIndex::new();
        idx.add_segment(SegmentId(1), &[("host", "srv1")]);
        idx.clear();
        assert_eq!(idx.entry_count(), 0);
    }

    #[test]
    fn empty_filters_returns_empty() {
        let idx = TagInvertedIndex::new();
        idx.add_segment(SegmentId(1), &[("host", "srv1")]);
        assert!(idx.segments_for_tags(&[]).is_empty());
    }

    #[test]
    fn values_for_key_returns_sorted_distinct() {
        let idx = TagInvertedIndex::new();
        idx.add_segment(SegmentId(1), &[("host", "srv1"), ("region", "us-east")]);
        idx.add_segment(SegmentId(2), &[("host", "srv2"), ("region", "us-east")]);
        idx.add_segment(SegmentId(3), &[("host", "srv1"), ("region", "eu-west")]);

        let hosts = idx.values_for_key("host");
        assert_eq!(hosts, vec!["srv1", "srv2"]);

        let regions = idx.values_for_key("region");
        assert_eq!(regions, vec!["eu-west", "us-east"]);

        // Non-existent key returns empty
        assert!(idx.values_for_key("datacenter").is_empty());
    }

    #[test]
    fn all_keys_returns_sorted_distinct() {
        let idx = TagInvertedIndex::new();
        idx.add_segment(SegmentId(1), &[("host", "srv1"), ("region", "us-east")]);
        idx.add_segment(SegmentId(2), &[("env", "prod"), ("host", "srv2")]);

        let keys = idx.all_keys();
        assert_eq!(keys, vec!["env", "host", "region"]);
    }

    #[test]
    fn all_keys_empty_index() {
        let idx = TagInvertedIndex::new();
        assert!(idx.all_keys().is_empty());
        assert!(idx.values_for_key("any").is_empty());
    }

    #[test]
    fn remove_segment_cleans_up_reverse_index() {
        let idx = TagInvertedIndex::new();
        idx.add_segment(SegmentId(1), &[("host", "srv1"), ("region", "us-east")]);
        idx.add_segment(SegmentId(2), &[("host", "srv2"), ("region", "eu-west")]);

        // Verify reverse index populated
        assert_eq!(idx.state.read().reverse.len(), 2);

        // Remove segment 1 — only its entries should be affected.
        idx.remove_segment(SegmentId(1));
        assert_eq!(idx.state.read().reverse.len(), 1);
        assert!(idx.segments_for_tag("host", "srv1").is_empty());
        assert!(idx.segments_for_tag("region", "us-east").is_empty());
        assert_eq!(idx.segments_for_tag("host", "srv2").len(), 1);
        assert_eq!(idx.segments_for_tag("region", "eu-west").len(), 1);
    }

    #[test]
    fn remove_nonexistent_segment_is_noop() {
        let idx = TagInvertedIndex::new();
        idx.add_segment(SegmentId(1), &[("host", "srv1")]);
        idx.remove_segment(SegmentId(999)); // should not panic or corrupt
        assert_eq!(idx.segments_for_tag("host", "srv1").len(), 1);
    }

    /// `=` is legal in both tag keys and tag values, so an index key built
    /// with `=` could not tell `("a=b", "c")` from `("a", "b=c")`. Both the
    /// lookup and the `key_values` cleanup would then act on the wrong tag.
    #[test]
    fn equals_in_tag_key_or_value_is_unambiguous() {
        let idx = TagInvertedIndex::new();
        idx.add_segment(SegmentId(1), &[("a=b", "c")]);
        idx.add_segment(SegmentId(2), &[("a", "b=c")]);

        assert_eq!(idx.segments_for_tag("a=b", "c"), vec![SegmentId(1)]);
        assert_eq!(idx.segments_for_tag("a", "b=c"), vec![SegmentId(2)]);

        // Cleanup must target the right tag key.
        idx.remove_segment(SegmentId(1));
        assert!(idx.segments_for_tag("a=b", "c").is_empty());
        assert_eq!(
            idx.segments_for_tag("a", "b=c"),
            vec![SegmentId(2)],
            "removing one segment corrupted the other tag's entry"
        );
    }

    #[test]
    fn key_values_index_cleanup_on_remove() {
        let idx = TagInvertedIndex::new();
        idx.add_segment(SegmentId(1), &[("host", "srv1"), ("region", "us-east")]);
        idx.add_segment(SegmentId(2), &[("host", "srv2"), ("region", "us-east")]);

        // Both host values present
        assert_eq!(idx.values_for_key("host"), vec!["srv1", "srv2"]);

        // Remove segment 1 — "srv1" should disappear since no other segment uses it
        idx.remove_segment(SegmentId(1));
        assert_eq!(idx.values_for_key("host"), vec!["srv2"]);
        // "us-east" still referenced by segment 2
        assert_eq!(idx.values_for_key("region"), vec!["us-east"]);

        // Remove segment 2 — all values should be gone
        idx.remove_segment(SegmentId(2));
        assert!(idx.values_for_key("host").is_empty());
        assert!(idx.values_for_key("region").is_empty());
        assert!(idx.all_keys().is_empty());
    }

    #[test]
    fn replace_segments_atomic_swap() {
        let idx = TagInvertedIndex::new();
        idx.add_segment(SegmentId(1), &[("host", "srv1"), ("region", "us-east")]);
        idx.add_segment(SegmentId(2), &[("host", "srv2"), ("region", "us-east")]);
        idx.add_segment(SegmentId(3), &[("host", "srv3"), ("region", "eu-west")]);

        // Replace segments 1 and 2 with a new compacted segment 10
        idx.replace_segments(
            &[SegmentId(1), SegmentId(2)],
            SegmentId(10),
            &[("host", "srv1"), ("host", "srv2"), ("region", "us-east")],
        );

        // Old segments should be gone
        assert!(idx
            .segments_for_tag("host", "srv1")
            .contains(&SegmentId(10)));
        assert!(!idx.segments_for_tag("host", "srv1").contains(&SegmentId(1)));
        assert!(idx
            .segments_for_tag("host", "srv2")
            .contains(&SegmentId(10)));
        assert!(!idx.segments_for_tag("host", "srv2").contains(&SegmentId(2)));

        // Segment 3 should be unaffected
        assert_eq!(idx.segments_for_tag("host", "srv3"), vec![SegmentId(3)]);
        assert!(idx
            .segments_for_tag("region", "eu-west")
            .contains(&SegmentId(3)));

        // New segment should appear in us-east
        assert!(idx
            .segments_for_tag("region", "us-east")
            .contains(&SegmentId(10)));

        // key_values should be consistent
        let mut hosts = idx.values_for_key("host");
        hosts.sort();
        assert_eq!(hosts, vec!["srv1", "srv2", "srv3"]);
    }

    #[test]
    fn replace_segments_empty_old_list() {
        let idx = TagInvertedIndex::new();
        idx.add_segment(SegmentId(1), &[("host", "srv1")]);

        // Replace with no old segments — effectively just an add
        idx.replace_segments(&[], SegmentId(2), &[("host", "srv2")]);

        assert_eq!(idx.segments_for_tag("host", "srv1"), vec![SegmentId(1)]);
        assert_eq!(idx.segments_for_tag("host", "srv2"), vec![SegmentId(2)]);
    }
}
