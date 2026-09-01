//! Region split planning and execution.

use std::collections::BTreeMap;

use tracing::{debug, info, warn};

use chronix_meta::{KeyRange, RegionId, RegionInfo, RegionState};

use super::{AutoScaler, RegionMetrics};

// ── Split Planning ─────────────────────────────────────────────

/// A planned region split.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitPlan {
    /// Region that should be split.
    pub source_region_id: RegionId,
    /// Pre-allocated globally-unique ID for the first child region.
    ///
    /// IDs must be allocated by the caller (e.g. via
    /// `MetaStateMachine::next_region_id`) instead of being derived
    /// from `source_region_id * 2`, which overflows after ~63 splits
    /// and can collide with existing regions.
    pub new_region_a: RegionId,
    /// Pre-allocated globally-unique ID for the second child region.
    pub new_region_b: RegionId,
    /// Measurement the region belongs to.
    pub measurement: String,
    /// Why the split was triggered.
    pub reason: SplitReason,
}

/// Reason for a region split.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SplitReason {
    /// Region size exceeds threshold.
    SizeExceeded {
        /// Current region size.
        current_bytes: u64,
        /// Threshold that was exceeded.
        threshold_bytes: u64,
    },
    /// Series count exceeds threshold.
    SeriesCountExceeded {
        /// Current number of series.
        current: u64,
        /// Threshold that was exceeded.
        threshold: u64,
    },
}

impl std::fmt::Display for SplitReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SizeExceeded {
                current_bytes,
                threshold_bytes,
            } => write!(
                f,
                "size {current_bytes} bytes exceeds threshold {threshold_bytes} bytes"
            ),
            Self::SeriesCountExceeded { current, threshold } => {
                write!(f, "series count {current} exceeds threshold {threshold}")
            }
        }
    }
}

// ── Split Phases ──────────────────────────────────────────────

/// Phase of a no-downtime region split.
///
/// Splits proceed through phases to ensure reads and writes continue
/// uninterrupted throughout the entire process:
///
/// 1. **Prepare** — source region is marked `Splitting` (still serves
///    reads *and* writes). Child regions are created in the routing table.
/// 2. **Commit** — routing table atomically swaps: the source region is
///    removed and both child regions are promoted to `Active`.  Writes
///    that arrived during the split window are forwarded.
///
/// Because the source remains writable throughout the prepare phase,
/// clients never observe "no region available" errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SplitPhase {
    /// Phase 1: mark source as `Splitting`, create child regions.
    Prepare,
    /// Phase 2: remove source, promote children to `Active`.
    Commit,
}

/// Result of a region split execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitResult {
    /// The original region that was split.
    pub source_region_id: RegionId,
    /// First half region (lower hash range).
    pub new_region_a: RegionId,
    /// Second half region (upper hash range).
    pub new_region_b: RegionId,
    /// Measurement of the split region.
    pub measurement: String,
    /// Phase completed by this execution.
    pub phase: SplitPhase,
}

// ── Split Detection (methods on AutoScaler) ───────────────────

impl AutoScaler {
    /// Check if a region should be split based on its metrics.
    ///
    /// Returns `None` for stale metrics (older than 2× scan interval).
    #[must_use]
    pub fn should_split(&self, metrics: &RegionMetrics) -> Option<SplitReason> {
        // Validate metric freshness — skip stale metrics to
        // prevent autoscale decisions based on outdated data (e.g. from
        // a node that recovered 10 minutes ago).
        if metrics.collected_at_secs > 0 {
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let max_age_secs = self.config().scan_interval.as_secs().saturating_mul(2);
            if now_secs.saturating_sub(metrics.collected_at_secs) > max_age_secs {
                tracing::warn!(
                    collected_at = metrics.collected_at_secs,
                    max_age_secs,
                    "skipping stale region metrics for autoscale decision"
                );
                return None;
            }
        }

        if metrics.size_bytes >= self.config().region_size_threshold {
            return Some(SplitReason::SizeExceeded {
                current_bytes: metrics.size_bytes,
                threshold_bytes: self.config().region_size_threshold,
            });
        }

        if metrics.series_count >= self.config().region_series_threshold {
            return Some(SplitReason::SeriesCountExceeded {
                current: metrics.series_count,
                threshold: self.config().region_series_threshold,
            });
        }

        None
    }

    /// Scan all regions and return split plans for those exceeding thresholds.
    ///
    /// `region_metrics` maps `RegionId` → observed metrics.
    /// `regions` provides the metadata needed to build split plans.
    ///
    /// `next_id` is called to allocate globally-unique child region
    /// IDs for each split plan. The caller should source IDs from
    /// `MetaStateMachine::next_region_id` (or equivalent) to guarantee
    /// uniqueness across the cluster.
    #[must_use]
    pub fn plan_splits(
        &self,
        regions: &BTreeMap<RegionId, RegionInfo>,
        region_metrics: &BTreeMap<RegionId, RegionMetrics>,
        next_id: &mut dyn FnMut() -> RegionId,
    ) -> Vec<SplitPlan> {
        let mut plans = Vec::new();

        for (&region_id, info) in regions {
            if let Some(metrics) = region_metrics.get(&region_id) {
                if let Some(reason) = self.should_split(metrics) {
                    info!(
                        region_id,
                        measurement = %info.measurement,
                        %reason,
                        "Region eligible for split"
                    );
                    let new_region_a = next_id();
                    let new_region_b = next_id();
                    plans.push(SplitPlan {
                        source_region_id: region_id,
                        new_region_a,
                        new_region_b,
                        measurement: info.measurement.clone(),
                        reason,
                    });
                }
            }
        }

        plans
    }

    // ── Split Execution ───────────────────────────────────────

    /// Begin phase 1 of a no-downtime split.
    ///
    /// The source region is marked `Splitting` so it continues to serve
    /// both reads and writes.  Two child regions are created in the
    /// routing table (initial state `Replicating`) so that new writes
    /// can begin flowing to them for backfill.
    ///
    /// Returns `None` if the source region is not found or is not `Active`.
    pub fn prepare_split(
        &self,
        plan: &SplitPlan,
        regions: &mut BTreeMap<RegionId, RegionInfo>,
    ) -> Option<SplitResult> {
        let source_info = regions.get_mut(&plan.source_region_id)?;

        // Only active regions can be split.
        if source_info.state != RegionState::Active {
            debug!(
                region = plan.source_region_id,
                state = %source_info.state,
                "Skipping split — region not active"
            );
            return None;
        }

        // Phase 1: mark source as Splitting (still accepts reads + writes).
        source_info.state = RegionState::Splitting;

        let source_snapshot = source_info.clone();

        // Use pre-allocated globally-unique IDs from the plan
        // instead of deriving from source_region_id * 2.
        let new_id_a = plan.new_region_a;
        let new_id_b = plan.new_region_b;

        if new_id_a == new_id_b {
            warn!(
                source = plan.source_region_id,
                "Split aborted — new_region_a and new_region_b are identical"
            );
            if let Some(src) = regions.get_mut(&plan.source_region_id) {
                src.state = RegionState::Active;
            }
            return None;
        }

        if regions.contains_key(&new_id_a) || regions.contains_key(&new_id_b) {
            warn!(
                source = plan.source_region_id,
                new_a = new_id_a,
                new_b = new_id_b,
                "Split aborted \u{2014} derived region IDs collide with existing regions"
            );
            // Revert source back to Active since split was aborted.
            if let Some(src) = regions.get_mut(&plan.source_region_id) {
                src.state = RegionState::Active;
            }
            return None;
        }

        // Create child regions in Replicating state (receiving backfill).
        let mut region_a = RegionInfo::new(
            new_id_a,
            plan.measurement.clone(),
            source_snapshot.leader_node_id,
            source_snapshot.replica_node_ids.clone(),
        );
        region_a.state = RegionState::Replicating;

        let mut region_b = RegionInfo::new(
            new_id_b,
            plan.measurement.clone(),
            source_snapshot.leader_node_id,
            source_snapshot.replica_node_ids,
        );
        region_b.state = RegionState::Replicating;

        // Assign key ranges to child regions so that range-based
        // routing survives splits without catastrophic rehashing. If the source
        // has a key_range, split it at the midpoint. Otherwise, assign the full
        // key space (bootstrap case for first split of legacy region).
        let source_range = source_snapshot.key_range.unwrap_or_else(KeyRange::full);
        if let Some((range_a, range_b)) = source_range.split() {
            region_a.key_range = Some(range_a);
            region_b.key_range = Some(range_b);
            info!(
                source = plan.source_region_id,
                range_a = %range_a, range_b = %range_b,
                "assigned key ranges to child regions"
            );
        } else {
            warn!(
                source = plan.source_region_id,
                range = %source_range,
                "source key range too small to split — children inherit full range"
            );
        }

        regions.insert(new_id_a, region_a);
        regions.insert(new_id_b, region_b);

        info!(
            source = plan.source_region_id,
            new_a = new_id_a,
            new_b = new_id_b,
            measurement = %plan.measurement,
            %plan.reason,
            "Split phase 1 (prepare): source marked Splitting, children created"
        );

        Some(SplitResult {
            source_region_id: plan.source_region_id,
            new_region_a: new_id_a,
            new_region_b: new_id_b,
            measurement: plan.measurement.clone(),
            phase: SplitPhase::Prepare,
        })
    }

    /// Complete phase 2 of a no-downtime split.
    ///
    /// The source region is removed and both child regions are promoted
    /// to `Active`.  This is the atomic cutover point — after this call,
    /// all reads and writes are served by the two child regions.
    ///
    /// Returns `None` if the source region is not in `Splitting` state.
    pub fn commit_split(
        &self,
        plan: &SplitPlan,
        regions: &mut BTreeMap<RegionId, RegionInfo>,
    ) -> Option<SplitResult> {
        let new_id_a = plan.new_region_a;
        let new_id_b = plan.new_region_b;

        // Validate source is still Splitting.
        let source_info = regions.get(&plan.source_region_id)?;
        if source_info.state != RegionState::Splitting {
            debug!(
                region = plan.source_region_id,
                state = %source_info.state,
                "Cannot commit split — source not in Splitting state"
            );
            return None;
        }

        // Validate children exist.
        if !regions.contains_key(&new_id_a) || !regions.contains_key(&new_id_b) {
            debug!(
                region = plan.source_region_id,
                "Cannot commit split — child regions missing"
            );
            return None;
        }

        // Phase 2: atomic cutover — remove source, activate children.
        regions.remove(&plan.source_region_id);

        if let Some(a) = regions.get_mut(&new_id_a) {
            a.state = RegionState::Active;
        }
        if let Some(b) = regions.get_mut(&new_id_b) {
            b.state = RegionState::Active;
        }

        info!(
            source = plan.source_region_id,
            new_a = new_id_a,
            new_b = new_id_b,
            measurement = %plan.measurement,
            %plan.reason,
            "Split phase 2 (commit): source removed, children active"
        );

        crate::metrics::increment_region_splits(&plan.measurement);

        Some(SplitResult {
            source_region_id: plan.source_region_id,
            new_region_a: new_id_a,
            new_region_b: new_id_b,
            measurement: plan.measurement.clone(),
            phase: SplitPhase::Commit,
        })
    }

    /// Execute a region split plan by creating two new regions and
    /// atomically updating the routing table.
    ///
    /// This is the single-step split for backward compatibility.
    /// For no-downtime behaviour, use `prepare_split` + `commit_split`.
    ///
    /// This method:
    /// 1. Generates two new region IDs by deriving from the source ID.
    /// 2. Creates new `RegionInfo` entries with the same leader/replicas.
    /// 3. Removes the old region and inserts two new ones atomically.
    /// 4. Emits the `chronix_cluster_region_splits_total` metric.
    ///
    /// The caller is responsible for data backfill (copying data from
    /// the old region to the new ones) and finalising via Raft.
    pub fn execute_split(
        &self,
        plan: &SplitPlan,
        regions: &mut BTreeMap<RegionId, RegionInfo>,
    ) -> Option<SplitResult> {
        let source_info = regions.get(&plan.source_region_id)?.clone();

        // Use pre-allocated globally-unique IDs from the plan.
        let new_id_a = plan.new_region_a;
        let new_id_b = plan.new_region_b;

        if regions.contains_key(&new_id_a) || regions.contains_key(&new_id_b) {
            warn!(
                source = plan.source_region_id,
                new_a = new_id_a,
                new_b = new_id_b,
                "Execute-split aborted \u{2014} derived region IDs collide with existing regions"
            );
            return None;
        }
        let region_a = RegionInfo::new(
            new_id_a,
            plan.measurement.clone(),
            source_info.leader_node_id,
            source_info.replica_node_ids.clone(),
        );
        let region_b = RegionInfo::new(
            new_id_b,
            plan.measurement.clone(),
            source_info.leader_node_id,
            source_info.replica_node_ids,
        );

        // Atomic routing table update: remove old, insert two new.
        regions.remove(&plan.source_region_id);
        regions.insert(new_id_a, region_a);
        regions.insert(new_id_b, region_b);

        info!(
            source = plan.source_region_id,
            new_a = new_id_a,
            new_b = new_id_b,
            measurement = %plan.measurement,
            %plan.reason,
            "Region split executed (single-step)"
        );

        // Record metric.
        crate::metrics::increment_region_splits(&plan.measurement);

        Some(SplitResult {
            source_region_id: plan.source_region_id,
            new_region_a: new_id_a,
            new_region_b: new_id_b,
            measurement: plan.measurement.clone(),
            phase: SplitPhase::Commit,
        })
    }

    /// Execute all split plans in order, returning results for each.
    pub fn execute_all_splits(
        &self,
        plans: &[SplitPlan],
        regions: &mut BTreeMap<RegionId, RegionInfo>,
    ) -> Vec<SplitResult> {
        plans
            .iter()
            .filter_map(|plan| self.execute_split(plan, regions))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::autoscale::tests_util::{make_config, make_id_alloc, make_region};

    // ── should_split ──────────────────────────────────────────

    #[test]
    fn split_not_needed_below_thresholds() {
        let scaler = AutoScaler::new(make_config());
        let metrics = RegionMetrics {
            size_bytes: 500_000,
            series_count: 500,
            ..Default::default()
        };
        assert!(scaler.should_split(&metrics).is_none());
    }

    #[test]
    fn split_triggered_by_size() {
        let scaler = AutoScaler::new(make_config());
        let metrics = RegionMetrics {
            size_bytes: 2_000_000,
            series_count: 100,
            ..Default::default()
        };
        let reason = scaler.should_split(&metrics).unwrap();
        assert!(matches!(reason, SplitReason::SizeExceeded { .. }));
    }

    #[test]
    fn split_triggered_by_series_count() {
        let scaler = AutoScaler::new(make_config());
        let metrics = RegionMetrics {
            size_bytes: 100,
            series_count: 2_000,
            ..Default::default()
        };
        let reason = scaler.should_split(&metrics).unwrap();
        assert!(matches!(reason, SplitReason::SeriesCountExceeded { .. }));
    }

    #[test]
    fn size_takes_priority_over_series() {
        let scaler = AutoScaler::new(make_config());
        let metrics = RegionMetrics {
            size_bytes: 2_000_000,
            series_count: 2_000,
            ..Default::default()
        };
        // Size is checked first.
        let reason = scaler.should_split(&metrics).unwrap();
        assert!(matches!(reason, SplitReason::SizeExceeded { .. }));
    }

    // ── plan_splits ───────────────────────────────────────────

    #[test]
    fn plan_splits_finds_eligible_regions() {
        let scaler = AutoScaler::new(make_config());

        let mut regions = BTreeMap::new();
        regions.insert(1, make_region(1, "cpu", 10));
        regions.insert(2, make_region(2, "mem", 10));
        regions.insert(3, make_region(3, "disk", 10));

        let mut metrics = BTreeMap::new();
        metrics.insert(
            1,
            RegionMetrics {
                size_bytes: 2_000_000,
                series_count: 100,
                ..Default::default()
            },
        );
        metrics.insert(
            2,
            RegionMetrics {
                size_bytes: 100,
                series_count: 100,
                ..Default::default()
            },
        );
        metrics.insert(
            3,
            RegionMetrics {
                size_bytes: 100,
                series_count: 5_000,
                ..Default::default()
            },
        );

        let plans = scaler.plan_splits(&regions, &metrics, &mut make_id_alloc());

        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].source_region_id, 1);
        assert_eq!(plans[0].measurement, "cpu");
        assert_eq!(plans[1].source_region_id, 3);
        assert_eq!(plans[1].measurement, "disk");
    }

    #[test]
    fn plan_splits_skips_regions_without_metrics() {
        let scaler = AutoScaler::new(make_config());

        let mut regions = BTreeMap::new();
        regions.insert(1, make_region(1, "cpu", 10));

        let metrics = BTreeMap::new(); // Empty

        let plans = scaler.plan_splits(&regions, &metrics, &mut make_id_alloc());
        assert!(plans.is_empty());
    }

    // ── Split execution ───────────────────────────────────────

    #[test]
    fn execute_split_creates_two_regions() {
        let scaler = AutoScaler::new(make_config());
        let mut regions = BTreeMap::new();
        regions.insert(10, make_region(10, "cpu", 1));

        let plan = SplitPlan {
            source_region_id: 10,
            new_region_a: 20,
            new_region_b: 21,
            measurement: "cpu".to_string(),
            reason: SplitReason::SizeExceeded {
                current_bytes: 2_000_000,
                threshold_bytes: 1_000_000,
            },
        };

        let result = scaler.execute_split(&plan, &mut regions).unwrap();

        // Source removed, two new regions added.
        assert!(!regions.contains_key(&10));
        assert_eq!(result.new_region_a, 20);
        assert_eq!(result.new_region_b, 21);
        assert!(regions.contains_key(&20));
        assert!(regions.contains_key(&21));
        assert_eq!(regions.len(), 2);

        // New regions preserve leader and measurement.
        assert_eq!(regions[&20].leader_node_id, 1);
        assert_eq!(regions[&20].measurement, "cpu");
        assert_eq!(regions[&21].leader_node_id, 1);
    }

    #[test]
    fn execute_split_missing_region_returns_none() {
        let scaler = AutoScaler::new(make_config());
        let mut regions = BTreeMap::new();

        let plan = SplitPlan {
            source_region_id: 99,
            new_region_a: 198,
            new_region_b: 199,
            measurement: "cpu".to_string(),
            reason: SplitReason::SizeExceeded {
                current_bytes: 2_000_000,
                threshold_bytes: 1_000_000,
            },
        };

        assert!(scaler.execute_split(&plan, &mut regions).is_none());
    }

    #[test]
    fn execute_all_splits_processes_batch() {
        let scaler = AutoScaler::new(make_config());
        let mut regions = BTreeMap::new();
        // Use IDs that won't collide under 2*id derivation.
        regions.insert(1000, make_region(1000, "cpu", 10));
        regions.insert(3000, make_region(3000, "mem", 20));

        let plans = vec![
            SplitPlan {
                source_region_id: 1000,
                new_region_a: 2000,
                new_region_b: 2001,
                measurement: "cpu".to_string(),
                reason: SplitReason::SizeExceeded {
                    current_bytes: 2_000_000,
                    threshold_bytes: 1_000_000,
                },
            },
            SplitPlan {
                source_region_id: 3000,
                new_region_a: 6000,
                new_region_b: 6001,
                measurement: "mem".to_string(),
                reason: SplitReason::SeriesCountExceeded {
                    current: 5000,
                    threshold: 1000,
                },
            },
        ];

        let results = scaler.execute_all_splits(&plans, &mut regions);
        assert_eq!(results.len(), 2);

        // Original regions gone, 4 new regions.
        assert!(!regions.contains_key(&1000));
        assert!(!regions.contains_key(&3000));
        assert_eq!(regions.len(), 4);
        // 1000 → 2000, 2001; 3000 → 6000, 6001
        assert!(regions.contains_key(&2000));
        assert!(regions.contains_key(&2001));
        assert!(regions.contains_key(&6000));
        assert!(regions.contains_key(&6001));
    }

    #[test]
    fn split_reason_display() {
        let size = SplitReason::SizeExceeded {
            current_bytes: 2_000_000,
            threshold_bytes: 1_000_000,
        };
        assert!(size.to_string().contains("2000000"));

        let series = SplitReason::SeriesCountExceeded {
            current: 5000,
            threshold: 1000,
        };
        assert!(series.to_string().contains("5000"));
    }

    // ── No-Downtime Split ────────────────────────────────────

    fn make_size_plan(region_id: RegionId) -> SplitPlan {
        SplitPlan {
            source_region_id: region_id,
            new_region_a: region_id * 2,
            new_region_b: region_id * 2 + 1,
            measurement: "cpu".to_string(),
            reason: SplitReason::SizeExceeded {
                current_bytes: 2_000_000,
                threshold_bytes: 1_000_000,
            },
        }
    }

    #[test]
    fn prepare_split_marks_source_splitting() {
        let scaler = AutoScaler::new(make_config());
        let mut regions = BTreeMap::new();
        regions.insert(10, make_region(10, "cpu", 1));

        let plan = make_size_plan(10);
        let result = scaler.prepare_split(&plan, &mut regions).unwrap();

        assert_eq!(result.phase, SplitPhase::Prepare);
        assert_eq!(result.source_region_id, 10);
        assert_eq!(result.new_region_a, 20);
        assert_eq!(result.new_region_b, 21);

        // Source still exists and is Splitting (reads + writes OK).
        let source = &regions[&10];
        assert_eq!(source.state, RegionState::Splitting);
        assert!(source.state.accepts_reads());
        assert!(source.state.accepts_writes());

        // Children created in Replicating state.
        assert_eq!(regions[&20].state, RegionState::Replicating);
        assert_eq!(regions[&21].state, RegionState::Replicating);

        // 3 regions total during split.
        assert_eq!(regions.len(), 3);
    }

    #[test]
    fn commit_split_activates_children_removes_source() {
        let scaler = AutoScaler::new(make_config());
        let mut regions = BTreeMap::new();
        regions.insert(10, make_region(10, "cpu", 1));

        let plan = make_size_plan(10);

        // Phase 1.
        scaler.prepare_split(&plan, &mut regions).unwrap();
        assert_eq!(regions.len(), 3);

        // Phase 2.
        let result = scaler.commit_split(&plan, &mut regions).unwrap();
        assert_eq!(result.phase, SplitPhase::Commit);

        // Source removed.
        assert!(!regions.contains_key(&10));

        // Children active.
        assert_eq!(regions[&20].state, RegionState::Active);
        assert_eq!(regions[&21].state, RegionState::Active);
        assert_eq!(regions.len(), 2);
    }

    #[test]
    fn prepare_split_rejects_non_active_region() {
        let scaler = AutoScaler::new(make_config());
        let mut regions = BTreeMap::new();
        let mut r = make_region(10, "cpu", 1);
        r.state = RegionState::ReadOnly;
        regions.insert(10, r);

        let plan = make_size_plan(10);
        assert!(scaler.prepare_split(&plan, &mut regions).is_none());
    }

    #[test]
    fn commit_split_rejects_without_prepare() {
        let scaler = AutoScaler::new(make_config());
        let mut regions = BTreeMap::new();
        regions.insert(10, make_region(10, "cpu", 1));

        let plan = make_size_plan(10);
        // Skip prepare — source is Active, not Splitting.
        assert!(scaler.commit_split(&plan, &mut regions).is_none());
    }

    #[test]
    fn no_downtime_split_data_accessible_throughout() {
        let scaler = AutoScaler::new(make_config());
        let mut regions = BTreeMap::new();
        regions.insert(10, make_region(10, "cpu", 1));

        let plan = make_size_plan(10);

        // Before split: data accessible via region 10.
        assert!(regions[&10].state.accepts_reads());
        assert!(regions[&10].state.accepts_writes());

        // Phase 1: source still serves traffic.
        scaler.prepare_split(&plan, &mut regions).unwrap();
        assert!(regions[&10].state.accepts_reads());
        assert!(regions[&10].state.accepts_writes());

        // Phase 2: source removed, but children serve traffic.
        scaler.commit_split(&plan, &mut regions).unwrap();
        assert!(regions[&20].state.accepts_reads());
        assert!(regions[&20].state.accepts_writes());
        assert!(regions[&21].state.accepts_reads());
        assert!(regions[&21].state.accepts_writes());
    }
}
