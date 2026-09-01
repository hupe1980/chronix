//! Periodic automatic rebalancing.

use std::collections::BTreeMap;
use std::time::Duration;

use tracing::{debug, info};

use chronix_meta::{RegionId, RegionInfo};

use super::rebalance::{NodeDiskUsage, ScaleAssessment};
use super::{AutoScaleConfig, AutoScaler, RegionMetrics};

/// Snapshot of cluster state for rebalancing assessment.
///
/// Used by [`PeriodicRebalancer`] to decouple state collection from
/// rebalancing policy decisions.
#[derive(Debug, Clone)]
pub struct ClusterSnapshot {
    /// All regions and their metadata.
    pub regions: BTreeMap<RegionId, RegionInfo>,
    /// Per-region observed metrics.
    pub region_metrics: BTreeMap<RegionId, RegionMetrics>,
    /// Per-node disk usage.
    pub disk_usages: Vec<NodeDiskUsage>,
    /// Region-to-leader mapping.
    pub leader_map: BTreeMap<RegionId, NodeId>,
}

use chronix_meta::NodeId;

/// Result of a periodic rebalancing check.
#[derive(Debug, Clone)]
pub struct RebalanceResult {
    /// The assessment produced by the auto-scaler.
    pub assessment: ScaleAssessment,
    /// How long ago the last rebalance check ran.
    pub time_since_last: Duration,
    /// Whether rebalancing was actually triggered (vs. just checked).
    pub triggered: bool,
}

/// Periodic automatic rebalancer.
///
/// Wraps an [`AutoScaler`] with timing logic to enforce rebalancing at
/// configurable intervals. The rebalancer is stateless beyond tracking
/// the last check time — all cluster state is provided per invocation.
///
/// # Usage
///
/// ```no_run
/// use chronix_cluster::autoscale::{PeriodicRebalancer, AutoScaleConfig};
///
/// let rebalancer = PeriodicRebalancer::new(AutoScaleConfig {
///     enable_periodic_rebalancing: true,
///     ..Default::default()
/// });
/// assert!(rebalancer.is_enabled());
/// ```
pub struct PeriodicRebalancer {
    scaler: AutoScaler,
    last_check: parking_lot::Mutex<Option<std::time::Instant>>,
}

impl PeriodicRebalancer {
    /// Create a new periodic rebalancer.
    #[must_use]
    pub fn new(config: AutoScaleConfig) -> Self {
        Self {
            scaler: AutoScaler::new(config),
            last_check: parking_lot::Mutex::new(None),
        }
    }

    /// Whether periodic rebalancing is enabled.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.scaler.config().enable_periodic_rebalancing
    }

    /// Access the underlying auto-scaler.
    #[must_use]
    pub fn scaler(&self) -> &AutoScaler {
        &self.scaler
    }

    /// Check if enough time has elapsed since the last rebalancing check.
    #[must_use]
    pub fn is_due(&self) -> bool {
        if !self.is_enabled() {
            return false;
        }
        let guard = self.last_check.lock();
        match *guard {
            None => true,
            Some(last) => last.elapsed() >= self.scaler.config().rebalance_interval,
        }
    }

    /// Duration since the last check, or `None` if never checked.
    #[must_use]
    pub fn time_since_last_check(&self) -> Option<Duration> {
        let guard = self.last_check.lock();
        guard.map(|last| last.elapsed())
    }

    /// Run a rebalancing check if the interval has elapsed.
    ///
    /// If periodic rebalancing is disabled or the interval hasn't elapsed,
    /// returns `None`. Otherwise, runs the assessment and returns the result
    /// with any planned migrations.
    ///
    /// The caller is responsible for executing returned migrations via Raft.
    pub fn check_and_rebalance(
        &self,
        snapshot: &ClusterSnapshot,
        next_id: &mut dyn FnMut() -> RegionId,
    ) -> Option<RebalanceResult> {
        if !self.is_due() {
            return None;
        }

        let time_since_last = self.time_since_last_check().unwrap_or(Duration::ZERO);

        // Update last check time.
        {
            let mut guard = self.last_check.lock();
            *guard = Some(std::time::Instant::now());
        }

        let assessment = self.scaler.assess(
            &snapshot.regions,
            &snapshot.region_metrics,
            &snapshot.disk_usages,
            &snapshot.leader_map,
            next_id,
        );

        let triggered = !assessment.is_stable();

        if triggered {
            info!(
                splits = assessment.split_plans.len(),
                migrations = assessment.rebalance_migrations.len(),
                "Periodic rebalancing triggered"
            );
            metrics::counter!("chronix_cluster_periodic_rebalance_total").increment(1);
        } else {
            debug!("Periodic rebalancing check — cluster stable");
        }

        Some(RebalanceResult {
            assessment,
            time_since_last,
            triggered,
        })
    }

    /// Force a rebalancing check regardless of interval timing.
    ///
    /// Useful for manual trigger (admin API) or initial startup.
    pub fn force_rebalance(
        &self,
        snapshot: &ClusterSnapshot,
        next_id: &mut dyn FnMut() -> RegionId,
    ) -> RebalanceResult {
        let time_since_last = self.time_since_last_check().unwrap_or(Duration::ZERO);

        {
            let mut guard = self.last_check.lock();
            *guard = Some(std::time::Instant::now());
        }

        let assessment = self.scaler.assess(
            &snapshot.regions,
            &snapshot.region_metrics,
            &snapshot.disk_usages,
            &snapshot.leader_map,
            next_id,
        );

        let triggered = !assessment.is_stable();

        info!(
            splits = assessment.split_plans.len(),
            migrations = assessment.rebalance_migrations.len(),
            triggered,
            "Manual/forced rebalancing check"
        );
        metrics::counter!("chronix_cluster_forced_rebalance_total").increment(1);

        RebalanceResult {
            assessment,
            time_since_last,
            triggered,
        }
    }

    /// Reset the last-check timer (e.g. after node joins/departures).
    pub fn reset_timer(&self) {
        let mut guard = self.last_check.lock();
        *guard = None;
    }
}

impl std::fmt::Debug for PeriodicRebalancer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeriodicRebalancer")
            .field("enabled", &self.is_enabled())
            .field(
                "rebalance_interval",
                &self.scaler.config().rebalance_interval,
            )
            .field("is_due", &self.is_due())
            .field("last_check", &*self.last_check.lock())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::super::rebalance::build_leader_map;
    use super::*;

    use crate::autoscale::tests_util::{make_config, make_id_alloc, make_region};

    fn make_snapshot_stable() -> ClusterSnapshot {
        let mut regions = BTreeMap::new();
        regions.insert(1, make_region(1, "cpu", 10));

        let mut region_metrics = BTreeMap::new();
        region_metrics.insert(
            1,
            RegionMetrics {
                size_bytes: 100,
                series_count: 10,
                ..Default::default()
            },
        );

        let disk_usages = vec![NodeDiskUsage {
            node_id: 10,
            used_bytes: 500,
            capacity_bytes: 1000,
            region_ids: vec![1],
        }];

        ClusterSnapshot {
            leader_map: build_leader_map(&regions),
            regions,
            region_metrics,
            disk_usages,
        }
    }

    fn make_snapshot_unbalanced() -> ClusterSnapshot {
        let mut regions = BTreeMap::new();
        regions.insert(1, make_region(1, "cpu", 10));
        regions.insert(2, make_region(2, "mem", 10));
        regions.insert(3, make_region(3, "disk", 20));

        let mut region_metrics = BTreeMap::new();
        region_metrics.insert(
            1,
            RegionMetrics {
                size_bytes: 2_000_000,
                ..Default::default()
            },
        );
        region_metrics.insert(
            2,
            RegionMetrics {
                size_bytes: 100,
                ..Default::default()
            },
        );
        region_metrics.insert(
            3,
            RegionMetrics {
                size_bytes: 100,
                ..Default::default()
            },
        );

        let disk_usages = vec![
            NodeDiskUsage {
                node_id: 10,
                used_bytes: 900,
                capacity_bytes: 1000,
                region_ids: vec![1, 2],
            },
            NodeDiskUsage {
                node_id: 20,
                used_bytes: 100,
                capacity_bytes: 1000,
                region_ids: vec![3],
            },
        ];

        ClusterSnapshot {
            leader_map: build_leader_map(&regions),
            regions,
            region_metrics,
            disk_usages,
        }
    }

    #[test]
    fn periodic_rebalancer_disabled_by_default() {
        let rebalancer = PeriodicRebalancer::new(AutoScaleConfig::default());
        assert!(!rebalancer.is_enabled());
        assert!(!rebalancer.is_due());
    }

    #[test]
    fn periodic_rebalancer_enabled() {
        let config = AutoScaleConfig {
            enable_periodic_rebalancing: true,
            rebalance_interval: Duration::from_millis(10),
            ..make_config()
        };
        let rebalancer = PeriodicRebalancer::new(config);
        assert!(rebalancer.is_enabled());
        // First check is always due (no previous check).
        assert!(rebalancer.is_due());
    }

    #[test]
    fn periodic_rebalancer_check_stable() {
        let config = AutoScaleConfig {
            enable_periodic_rebalancing: true,
            rebalance_interval: Duration::from_millis(1),
            ..make_config()
        };
        let rebalancer = PeriodicRebalancer::new(config);
        let snapshot = make_snapshot_stable();

        let result = rebalancer
            .check_and_rebalance(&snapshot, &mut make_id_alloc())
            .unwrap();
        assert!(!result.triggered);
        assert!(result.assessment.is_stable());
    }

    #[test]
    fn periodic_rebalancer_check_triggered() {
        let config = AutoScaleConfig {
            enable_periodic_rebalancing: true,
            rebalance_interval: Duration::from_millis(1),
            ..make_config()
        };
        let rebalancer = PeriodicRebalancer::new(config);
        let snapshot = make_snapshot_unbalanced();

        let result = rebalancer
            .check_and_rebalance(&snapshot, &mut make_id_alloc())
            .unwrap();
        assert!(result.triggered);
        assert!(!result.assessment.split_plans.is_empty());
    }

    #[test]
    fn periodic_rebalancer_respects_interval() {
        let config = AutoScaleConfig {
            enable_periodic_rebalancing: true,
            rebalance_interval: Duration::from_secs(3600), // 1 hour
            ..make_config()
        };
        let rebalancer = PeriodicRebalancer::new(config);
        let snapshot = make_snapshot_stable();

        // First check should succeed (never checked before).
        assert!(rebalancer
            .check_and_rebalance(&snapshot, &mut make_id_alloc())
            .is_some());
        // Immediately after, should return None (not due yet).
        assert!(rebalancer
            .check_and_rebalance(&snapshot, &mut make_id_alloc())
            .is_none());
    }

    #[test]
    fn periodic_rebalancer_force_ignores_interval() {
        let config = AutoScaleConfig {
            enable_periodic_rebalancing: false, // Even when disabled
            rebalance_interval: Duration::from_secs(3600),
            ..make_config()
        };
        let rebalancer = PeriodicRebalancer::new(config);
        let snapshot = make_snapshot_unbalanced();

        // check_and_rebalance returns None when disabled.
        assert!(rebalancer
            .check_and_rebalance(&snapshot, &mut make_id_alloc())
            .is_none());
        // force always works.
        let result = rebalancer.force_rebalance(&snapshot, &mut make_id_alloc());
        assert!(result.triggered);
    }

    #[test]
    fn periodic_rebalancer_reset_timer() {
        let config = AutoScaleConfig {
            enable_periodic_rebalancing: true,
            rebalance_interval: Duration::from_secs(3600),
            ..make_config()
        };
        let rebalancer = PeriodicRebalancer::new(config);
        let snapshot = make_snapshot_stable();

        // First check.
        assert!(rebalancer
            .check_and_rebalance(&snapshot, &mut make_id_alloc())
            .is_some());
        // Not due.
        assert!(rebalancer
            .check_and_rebalance(&snapshot, &mut make_id_alloc())
            .is_none());
        // Reset timer.
        rebalancer.reset_timer();
        // Now due again.
        assert!(rebalancer
            .check_and_rebalance(&snapshot, &mut make_id_alloc())
            .is_some());
    }

    #[test]
    fn periodic_rebalancer_debug_format() {
        let rebalancer = PeriodicRebalancer::new(AutoScaleConfig::default());
        let dbg = format!("{rebalancer:?}");
        assert!(dbg.contains("PeriodicRebalancer"));
        assert!(dbg.contains("enabled"));
    }

    #[test]
    fn periodic_rebalancer_time_since_last() {
        let config = AutoScaleConfig {
            enable_periodic_rebalancing: true,
            rebalance_interval: Duration::from_millis(1),
            ..make_config()
        };
        let rebalancer = PeriodicRebalancer::new(config);

        // Before any check, time_since_last is None.
        assert!(rebalancer.time_since_last_check().is_none());

        let snapshot = make_snapshot_stable();
        rebalancer.check_and_rebalance(&snapshot, &mut make_id_alloc());

        // After check, time_since_last is Some.
        let elapsed = rebalancer.time_since_last_check().unwrap();
        assert!(elapsed < Duration::from_secs(1));
    }
}
