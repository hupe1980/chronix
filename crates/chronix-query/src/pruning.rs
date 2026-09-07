//! Multi-level segment pruning pipeline.
//!
//! Eliminates segments from query processing before any data is decoded:
//! 1. **Time pruning** — Exclude segments whose time range doesn't overlap
//! 2. **Bloom pruning** — Exclude segments that don't contain the series key
//! 3. **Stats pruning** — Exclude segments where column statistics exclude the predicate

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::path::PathBuf;

use chronix_engine::index::{SegmentCatalog, SegmentCatalogEntry, SeriesBloomFilter, TimeIndex};

/// Statistics about segment pruning during query execution.
#[derive(Debug, Clone, Default)]
pub struct PruningStats {
    /// Total segments considered.
    pub segments_total: usize,
    /// Segments pruned by time range.
    pub pruned_by_time: usize,
    /// Segments pruned by bloom filter.
    pub pruned_by_bloom: usize,
    /// Segments pruned by column statistics.
    pub pruned_by_stats: usize,
    /// Segments that passed all pruning levels.
    pub segments_remaining: usize,
}

impl PruningStats {
    /// Total segments pruned across all levels.
    #[must_use]
    pub fn total_pruned(&self) -> usize {
        self.pruned_by_time + self.pruned_by_bloom + self.pruned_by_stats
    }
}

/// Result of the pruning pipeline: surviving segment entries and statistics.
#[derive(Debug)]
pub struct PruningResult {
    /// Segments that passed all pruning levels.
    pub segments: Vec<PrunedSegment>,
    /// Pruning statistics for metrics.
    pub stats: PruningStats,
}

/// A segment that survived pruning, with its catalog entry and file path.
#[derive(Debug, Clone)]
pub struct PrunedSegment {
    /// Catalog entry for this segment.
    pub entry: SegmentCatalogEntry,
    /// Filesystem path to the segment file.
    pub path: PathBuf,
}

/// Run the multi-level pruning pipeline.
///
/// # Levels
///
/// 1. **Time pruning**: Uses `TimeIndex` to find segments overlapping `[start, end]`
/// 2. **Bloom pruning**: Uses bloom filters to exclude segments missing the series key
/// 3. **Stats pruning**: Uses column statistics to exclude segments where
///    required tag columns have zero `distinct_count` (column exists but is
///    all-null — cannot possibly match a tag equality filter).
///
/// # Arguments
///
/// - `time_index` — Time range index
/// - `catalog` — Segment catalog for metadata lookup
/// - `bloom_filters` — Per-segment bloom filters (`segment_id` → filter)
/// - `start_ts`, `end_ts` — Query time range
/// - `series_key` — Canonical series key (for bloom), or `None`
/// - `tag_filter_keys` — Tag column names used in equality filters (for stats pruning)
#[must_use]
pub fn prune_segments<S: BuildHasher>(
    time_index: &TimeIndex,
    catalog: &SegmentCatalog,
    bloom_filters: &HashMap<u64, SeriesBloomFilter, S>,
    start_ts: i64,
    end_ts: i64,
    series_key: Option<&chronix_core::SeriesKey>,
    tag_filter_keys: &[&str],
) -> PruningResult {
    let segments_total = catalog.segment_count();

    // Level 1: Time pruning
    let time_matching_ids = time_index.segments_for_range(start_ts, end_ts);
    let pruned_by_time = segments_total - time_matching_ids.len();

    // Resolve IDs to catalog entries via HashMap for O(1) lookup per ID
    let entries: Vec<&SegmentCatalogEntry> = {
        let all = catalog.all_segments();
        let id_map: std::collections::HashMap<u64, &SegmentCatalogEntry> =
            all.iter().map(|e| (e.segment_id.0, *e)).collect();
        time_matching_ids
            .iter()
            .filter_map(|seg_id| id_map.get(&seg_id.0).copied())
            .collect()
    };

    let bloom_lookup = |seg_id: u64| bloom_filters.get(&seg_id);

    let mut result = prune_entries(entries, bloom_lookup, series_key, tag_filter_keys);
    result.stats.segments_total = segments_total;
    result.stats.pruned_by_time = pruned_by_time;
    result
}

/// Prune a pre-filtered set of segment entries through bloom and stats levels.
///
/// This is the workhorse used by both `prune_segments()` (after time-index
/// filtering) and the `Chronix::execute()` / `execute_stream()` pipelines
/// (which filter by measurement + time range inline before calling this).
///
/// # Arguments
///
/// - `entries` — Segment entries that already passed time + measurement filtering
/// - `bloom_lookup` — Closure that returns the bloom filter for a given segment id
/// - `series_key` — Canonical series key (for bloom), or `None` if no tag filters
/// - `tag_filter_keys` — Tag column names used in equality filters (for stats pruning)
#[must_use]
pub fn prune_entries<'a, F>(
    entries: Vec<&'a SegmentCatalogEntry>,
    bloom_lookup: F,
    series_key: Option<&chronix_core::SeriesKey>,
    tag_filter_keys: &[&str],
) -> PruningResult
where
    F: Fn(u64) -> Option<&'a SeriesBloomFilter>,
{
    let input_count = entries.len();
    let mut stats = PruningStats::default();

    // Level 2: Bloom filter pruning
    let mut surviving: Vec<PrunedSegment> = Vec::with_capacity(input_count);

    for entry in entries {
        if let Some(series_key) = series_key {
            if let Some(bloom) = bloom_lookup(entry.segment_id.0) {
                if !bloom.may_contain(series_key) {
                    stats.pruned_by_bloom += 1;
                    continue;
                }
            }
        }

        surviving.push(PrunedSegment {
            path: entry.path.clone(),
            entry: entry.clone(),
        });
    }

    // Level 3: Column stats pruning
    // Exclude segments where a required tag column is present but has
    // zero non-null values (all-null column cannot match equality filter).
    if !tag_filter_keys.is_empty() {
        surviving.retain(|ps| {
            for tag_key in tag_filter_keys {
                if let Some(cs) = ps.entry.column_stats.iter().find(|c| c.name == *tag_key) {
                    if cs.stats.distinct_count == 0 && cs.stats.value_count == 0 {
                        stats.pruned_by_stats += 1;
                        return false;
                    }
                }
            }
            true
        });
    }

    stats.segments_remaining = surviving.len();

    PruningResult {
        segments: surviving,
        stats,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronix_core::{SegmentId, SegmentState, ShardId};
    use chronix_engine::index::time_index::TimeIndexEntry;
    use std::collections::BTreeMap;

    fn make_entry(id: u64, shard: i64, min_ts: i64, max_ts: i64) -> SegmentCatalogEntry {
        SegmentCatalogEntry {
            segment_id: SegmentId(id),
            shard_id: ShardId(shard),
            measurement: "cpu".to_string(),
            path: PathBuf::from(format!("shard_{shard}/seg_{id}.csx")),
            min_timestamp: min_ts,
            max_timestamp: max_ts,
            row_count: 1000,
            series_count: 10,
            byte_size: 4096,
            row_group_count: 1,
            column_count: 5,
            column_stats: Vec::new(),
            state: SegmentState::default(),
        }
    }

    fn setup() -> (TimeIndex, SegmentCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let time_index = TimeIndex::new();
        let mut catalog = SegmentCatalog::new(dir.path()).unwrap();

        // Add 4 segments with different time ranges
        let entries = vec![
            make_entry(1, 0, 100, 200),
            make_entry(2, 0, 300, 400),
            make_entry(3, 0, 500, 600),
            make_entry(4, 0, 700, 800),
        ];

        for entry in &entries {
            time_index.add_segment(TimeIndexEntry {
                segment_id: entry.segment_id,
                min_ts: entry.min_timestamp,
                max_ts: entry.max_timestamp,
            });
            catalog.add_segment(entry.clone()).unwrap();
        }

        (time_index, catalog)
    }

    #[test]
    fn time_pruning() {
        let (time_index, catalog) = setup();
        let blooms = HashMap::new();

        let result = prune_segments(
            &time_index,
            &catalog,
            &blooms,
            250,
            550, // should match segments 2 and 3
            None,
            &[],
        );

        assert_eq!(result.stats.segments_total, 4);
        assert_eq!(result.stats.pruned_by_time, 2);
        assert_eq!(result.segments.len(), 2);
        assert_eq!(result.segments[0].entry.segment_id, SegmentId(2));
        assert_eq!(result.segments[1].entry.segment_id, SegmentId(3));
    }

    #[test]
    fn bloom_pruning() {
        let (time_index, catalog) = setup();

        let key1 = chronix_core::SeriesKey::new(
            "cpu",
            BTreeMap::from([("host".to_string(), "srv1".to_string())]),
        )
        .unwrap();

        let key2 = chronix_core::SeriesKey::new(
            "cpu",
            BTreeMap::from([("host".to_string(), "srv2".to_string())]),
        )
        .unwrap();

        // Create bloom filters: only segment 2 contains key1
        let mut bloom2 = SeriesBloomFilter::new(100, 0.01);
        bloom2.insert(&key1);

        let mut bloom3 = SeriesBloomFilter::new(100, 0.01);
        bloom3.insert(&key2); // doesn't contain key1

        let mut bloom_map = HashMap::new();
        bloom_map.insert(2, bloom2);
        bloom_map.insert(3, bloom3);

        let result = prune_segments(
            &time_index,
            &catalog,
            &bloom_map,
            250,
            550,
            Some(&key1),
            &["host"],
        );

        // Segment 3 should be pruned by bloom
        assert_eq!(result.stats.pruned_by_bloom, 1);
        assert_eq!(result.segments.len(), 1);
        assert_eq!(result.segments[0].entry.segment_id, SegmentId(2));
    }

    #[test]
    fn no_pruning_when_all_match() {
        let (time_index, catalog) = setup();
        let blooms = HashMap::new();

        let result = prune_segments(&time_index, &catalog, &blooms, 0, 1000, None, &[]);

        assert_eq!(result.stats.pruned_by_time, 0);
        assert_eq!(result.stats.pruned_by_bloom, 0);
        assert_eq!(result.segments.len(), 4);
    }

    #[test]
    fn all_pruned() {
        let (time_index, catalog) = setup();
        let blooms = HashMap::new();

        let result = prune_segments(
            &time_index,
            &catalog,
            &blooms,
            900,
            1000, // no segments in this range
            None,
            &[],
        );

        assert_eq!(result.segments.len(), 0);
        assert_eq!(result.stats.pruned_by_time, 4);
    }

    #[test]
    fn stats_pruning_by_all_null_tag() {
        use chronix_engine::index::CatalogColumnStats;
        use chronix_engine::segment::stats::ColumnStats;

        let dir = tempfile::tempdir().unwrap();
        let time_index = TimeIndex::new();
        let mut catalog = SegmentCatalog::new(dir.path()).unwrap();

        // Segment 1: has "host" tag with real data (distinct_count > 0)
        let mut entry1 = make_entry(1, 0, 100, 200);
        entry1.column_stats = vec![CatalogColumnStats {
            name: "host".to_string(),
            data_type: 1, // STRING
            role: 1,      // TAG
            decimal_scale: None,
            stats: ColumnStats {
                min_value: 0,
                max_value: 0,
                min_value_u64: 0,
                max_value_u64: 0,
                null_count: 0,
                value_count: 10,
                sum: 0.0,
                sum_i128: 0,
                distinct_count: 2, // has real values
            },
        }];

        // Segment 2: has "host" tag but all null (distinct_count == 0)
        let mut entry2 = make_entry(2, 0, 300, 400);
        entry2.column_stats = vec![CatalogColumnStats {
            name: "host".to_string(),
            data_type: 1,
            role: 1,
            decimal_scale: None,
            stats: ColumnStats {
                min_value: 0,
                max_value: 0,
                min_value_u64: 0,
                max_value_u64: 0,
                null_count: 10,
                value_count: 0,
                sum: 0.0,
                sum_i128: 0,
                distinct_count: 0, // all null
            },
        }];

        for entry in [&entry1, &entry2] {
            time_index.add_segment(TimeIndexEntry {
                segment_id: entry.segment_id,
                min_ts: entry.min_timestamp,
                max_ts: entry.max_timestamp,
            });
            catalog.add_segment(entry.clone()).unwrap();
        }

        let blooms = HashMap::new();
        let result = prune_segments(
            &time_index,
            &catalog,
            &blooms,
            0,
            500,
            None,
            &["host"], // filtering on "host" tag
        );

        // Segment 2 should be pruned by stats (all-null host column)
        assert_eq!(result.stats.pruned_by_stats, 1);
        assert_eq!(result.segments.len(), 1);
        assert_eq!(result.segments[0].entry.segment_id, SegmentId(1));
    }
}
