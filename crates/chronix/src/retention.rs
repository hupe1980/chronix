//! Retention enforcer — automatically drops shards older than the retention period.
//!
//! The `RetentionEnforcer` identifies shards whose `max_timestamp` is older
//! than the configured retention period and drops all their segments.

use std::collections::BTreeMap;

use chronix_core::ShardId;

/// Result of a retention enforcement pass.
#[derive(Debug, Clone)]
pub struct RetentionResult {
    /// Number of shards dropped.
    pub shards_dropped: usize,
    /// Number of segments deleted.
    pub segments_deleted: usize,
    /// Total bytes freed.
    pub bytes_freed: u64,
}

/// Identifies shards eligible for retention-based deletion.
///
/// A shard is eligible if its `max_timestamp` is before `cutoff_ts`.
///
/// # Arguments
///
/// * `shard_bounds` — A map of shard ID → `(min_timestamp, max_timestamp)`.
/// * `cutoff_ts` — Shards entirely before this timestamp are dropped.
///
/// # Returns
///
/// List of shard IDs that should be dropped.
#[must_use]
pub fn shards_to_drop(
    shard_bounds: &BTreeMap<ShardId, (i64, i64)>,
    cutoff_ts: i64,
) -> Vec<ShardId> {
    shard_bounds
        .iter()
        .filter(|&(_, &(_, max_ts))| max_ts < cutoff_ts)
        .map(|(&shard_id, _)| shard_id)
        .collect()
}

/// Compute the retention cutoff timestamp.
///
/// `now_ms` is the current time in the same unit as timestamps (typically
/// epoch nanoseconds). `retention_ns` is the retention duration in the
/// same unit.
#[must_use]
pub fn retention_cutoff(now_ns: i64, retention_ns: i64) -> i64 {
    now_ns.saturating_sub(retention_ns).max(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_shards_to_drop_when_all_recent() {
        let mut bounds = BTreeMap::new();
        bounds.insert(ShardId(1), (100, 200));
        bounds.insert(ShardId(2), (200, 300));

        let cutoff = 50; // Everything is after cutoff
        assert!(shards_to_drop(&bounds, cutoff).is_empty());
    }

    #[test]
    fn drops_old_shards() {
        let mut bounds = BTreeMap::new();
        bounds.insert(ShardId(1), (100, 200));
        bounds.insert(ShardId(2), (300, 400));
        bounds.insert(ShardId(3), (500, 600));

        let cutoff = 450; // Shard 1 and 2 should be dropped (max_ts < 450)
        let dropped = shards_to_drop(&bounds, cutoff);
        assert_eq!(dropped.len(), 2);
        assert!(dropped.contains(&ShardId(1)));
        assert!(dropped.contains(&ShardId(2)));
    }

    #[test]
    fn boundary_shard_not_dropped() {
        let mut bounds = BTreeMap::new();
        bounds.insert(ShardId(1), (100, 200));

        // max_ts == cutoff → NOT dropped (only strict <)
        assert!(shards_to_drop(&bounds, 200).is_empty());
    }

    #[test]
    fn retention_cutoff_basic() {
        assert_eq!(retention_cutoff(1_000_000, 500_000), 500_000);
    }

    #[test]
    fn retention_cutoff_no_underflow() {
        assert_eq!(retention_cutoff(100, 1_000_000), 0);
    }
}
