//! Retention enforcer — automatically drops shards older than the retention period.
//!
//! The `RetentionEnforcer` identifies shards whose `max_timestamp` is older
//! than the configured retention period and drops all their segments.

use std::collections::BTreeMap;

use chronix_core::ShardId;

/// Result of a retention enforcement pass.
#[derive(Debug, Clone)]
pub struct RetentionResult {
    /// Number of shards the pass removed entirely.
    ///
    /// Not the number it *considered*: a shard past the cutoff whose
    /// segments are all still needed by a rollup is not counted here, which
    /// is the difference between "the disk is not shrinking because there is
    /// nothing to drop" and "…because something is holding it back".
    pub shards_dropped: usize,
    /// Number of segments deleted.
    pub segments_deleted: usize,
    /// Segments past the cutoff that the pass declined to delete, because a
    /// rollup fed by them has not been materialised that far yet.
    ///
    /// Self-healing — the next pass tries again — but it is the reason a
    /// retention rule can appear not to work, so it is reported rather than
    /// left to a log line.
    pub segments_preserved: usize,
    /// Segments whose rows this pass removed but whose files it could not
    /// unlink yet, because a running scan had already been handed the path.
    ///
    /// Reported for the same reason `segments_preserved` is: it is the
    /// difference between "the disk is not shrinking because there was
    /// nothing to drop" and "…because somebody is reading it". The next
    /// garbage collection removes them.
    pub segments_awaiting_readers: usize,
    /// Bytes actually reclaimed from the disk.
    ///
    /// Not the size of what was dropped: a segment counted in
    /// `segments_awaiting_readers` is out of the database and still on the
    /// disk, so its bytes are reported by the garbage collection that
    /// unlinks it.
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

/// The instant retention measures age from: the wall clock, **capped by the
/// newest timestamp the database holds**.
///
/// Retention is the only irreversible thing a background thread does, and a
/// cutoff derived from `SystemTime::now()` alone makes one reading of the
/// clock enough to delete everything — a gateway with no battery-backed RTC,
/// an NTP server handing out a date in the next century, a restored VM
/// snapshot. It also empties a database that has simply *stopped writing*:
/// three days of readings that ended a month ago are three days old to each
/// other, and a seven-day rule has nothing to say about them.
///
/// Capping answers both, and is the rule the rollup materialiser already
/// used: finality is anchored on the newest write, not on the clock.
///
/// The cap **delays** retention rather than disabling it — one fresh write
/// moves the reference to the present. Two consequences, stated rather than
/// implied: the newest data can never be expired, and a database that stops
/// receiving data stops freeing disk. `delete` is the way to reclaim it.
///
/// `newest_data_ns` is `None` for an empty database, where nothing can expire
/// anyway.
#[must_use]
pub fn retention_reference(now_ns: i64, newest_data_ns: Option<i64>) -> i64 {
    match newest_data_ns {
        Some(newest) => now_ns.min(newest),
        None => now_ns,
    }
}

/// Compute the retention cutoff timestamp.
///
/// `reference_ns` comes from [`retention_reference`] — never from the wall
/// clock directly. `retention_ns` is the retention duration in the same unit
/// (epoch nanoseconds).
#[must_use]
pub fn retention_cutoff(reference_ns: i64, retention_ns: i64) -> i64 {
    reference_ns.saturating_sub(retention_ns).max(0)
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

    #[test]
    fn the_reference_is_the_clock_while_the_data_keeps_up() {
        // Live ingest: the newest point is a moment old, so the clock wins
        // and retention behaves exactly as a wall-clock rule.
        assert_eq!(retention_reference(1_000, Some(999)), 999);
        assert_eq!(retention_reference(1_000, Some(1_000)), 1_000);
    }

    #[test]
    fn a_clock_that_jumps_forward_cannot_expire_more_than_the_data_allows() {
        // The clock reads a century ahead for one pass. Without the cap the
        // cutoff is 100 years past every shard and the database is emptied.
        let newest = 1_000_i64;
        let clock_in_the_next_century = 3_000_000_000_000_i64;
        assert_eq!(
            retention_reference(clock_in_the_next_century, Some(newest)),
            newest,
        );
    }

    #[test]
    fn a_database_that_stopped_writing_stops_expiring() {
        // Writes ended a month ago; a seven-day rule must not empty it.
        let month = 30 * 86_400_000_000_000_i64;
        let week = 7 * 86_400_000_000_000_i64;
        let now = 100 * month;
        let newest = now - month;
        let cutoff = retention_cutoff(retention_reference(now, Some(newest)), week);
        assert!(
            cutoff < newest,
            "the newest data must survive its own retention window",
        );
    }

    #[test]
    fn an_empty_database_falls_back_to_the_clock() {
        assert_eq!(retention_reference(1_000, None), 1_000);
    }
}
