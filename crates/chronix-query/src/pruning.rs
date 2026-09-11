//! Multi-level segment pruning pipeline.
//!
//! Eliminates segments from query processing before any data is decoded:
//! 1. **Bloom pruning** — Exclude segments that cannot contain the series key
//! 2. **Stats pruning** — Exclude segments where column statistics exclude the predicate
//!
//! Time and tag-index pruning happen one level up, in `Chronix::prune_segments`,
//! where the candidate set is already scoped to one measurement: the catalog
//! entry carries `min_timestamp`/`max_timestamp`, so a separate time index
//! answered nothing the catalog could not.
//!
//! **Every level here may only ever fail towards more work.** A segment with
//! no bloom, or with no statistics for the filtered tag, is kept — both
//! structures are derived from a segment's sidecar and can lag the catalog,
//! and pruning what has not been seen loses rows with no error anywhere.

use std::path::PathBuf;

use chronix_engine::index::{SegmentCatalogEntry, SeriesBloomFilter};

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

    fn tag_stats(
        name: &str,
        distinct: u32,
        values: u64,
    ) -> chronix_engine::index::CatalogColumnStats {
        chronix_engine::index::CatalogColumnStats {
            name: name.to_string(),
            data_type: 1, // STRING
            role: 1,      // TAG
            decimal_scale: None,
            stats: chronix_engine::segment::stats::ColumnStats {
                min_value: 0,
                max_value: 0,
                min_value_u64: 0,
                max_value_u64: 0,
                null_count: 0,
                value_count: values,
                sum: 0.0,
                sum_i128: 0,
                distinct_count: distinct,
            },
        }
    }

    #[test]
    fn nothing_is_pruned_without_a_series_key_or_a_tag_filter() {
        let entries = [make_entry(1, 0, 100, 200), make_entry(2, 0, 300, 400)];
        let refs: Vec<&SegmentCatalogEntry> = entries.iter().collect();
        let result = prune_entries(refs, |_| None, None, &[]);
        assert_eq!(result.segments.len(), 2);
        assert_eq!(result.stats.total_pruned(), 0);
    }

    /// A missing bloom keeps its segment.
    ///
    /// A pruning structure may only ever fail towards more work: the blooms
    /// are rebuilt from sidecars and a segment can be in the catalog before
    /// its bloom is, so "no bloom" has to mean "may match".
    #[test]
    fn a_segment_without_a_bloom_is_kept() {
        use chronix_engine::index::SeriesBloomFilter;
        let entries = [make_entry(1, 0, 100, 200), make_entry(2, 0, 300, 400)];
        let refs: Vec<&SegmentCatalogEntry> = entries.iter().collect();

        let key = chronix_core::SeriesKey::new(
            "cpu",
            [("host".to_string(), "a".to_string())]
                .into_iter()
                .collect::<std::collections::BTreeMap<_, _>>(),
        )
        .unwrap();

        // Segment 1 has a bloom that does not hold the key; segment 2 has none.
        let mut bloom = SeriesBloomFilter::new(8, 0.01);
        bloom.insert(
            &chronix_core::SeriesKey::new(
                "cpu",
                [("host".to_string(), "z".to_string())]
                    .into_iter()
                    .collect::<std::collections::BTreeMap<_, _>>(),
            )
            .unwrap(),
        );

        let result = prune_entries(refs, |id| (id == 1).then_some(&bloom), Some(&key), &[]);
        assert_eq!(result.stats.pruned_by_bloom, 1);
        assert_eq!(result.segments.len(), 1);
        assert_eq!(result.segments[0].entry.segment_id, SegmentId(2));
    }

    /// A tag column that exists and is entirely null cannot match an equality
    /// filter on it.
    #[test]
    fn an_all_null_tag_column_is_pruned_by_its_statistics() {
        let mut with_values = make_entry(1, 0, 100, 200);
        with_values.column_stats = vec![tag_stats("host", 2, 10)];
        let mut all_null = make_entry(2, 0, 300, 400);
        all_null.column_stats = vec![tag_stats("host", 0, 0)];

        let entries = [with_values, all_null];
        let refs: Vec<&SegmentCatalogEntry> = entries.iter().collect();
        let result = prune_entries(refs, |_| None, None, &["host"]);

        assert_eq!(result.stats.pruned_by_stats, 1);
        assert_eq!(result.segments.len(), 1);
        assert_eq!(result.segments[0].entry.segment_id, SegmentId(1));
    }

    /// A segment carrying no statistics for the filtered tag is kept.
    #[test]
    fn a_segment_with_no_statistics_for_the_tag_is_kept() {
        let entries = [make_entry(1, 0, 100, 200)];
        let refs: Vec<&SegmentCatalogEntry> = entries.iter().collect();
        let result = prune_entries(refs, |_| None, None, &["host"]);
        assert_eq!(result.segments.len(), 1);
        assert_eq!(result.stats.pruned_by_stats, 0);
    }
}
