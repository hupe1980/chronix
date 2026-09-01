//! Automatic cluster scaling — region splitting and disk-aware rebalancing.
//!
//! Provides [`AutoScaler`] which monitors region sizes and series counts,
//! triggers split when thresholds are exceeded, and generates rebalance
//! plans based on disk usage rather than just leader counts.

mod config;
mod periodic;
mod rebalance;
mod split;

pub use config::{AutoScaleConfig, RegionMetrics};
pub use periodic::{ClusterSnapshot, PeriodicRebalancer, RebalanceResult};
pub use rebalance::{build_leader_map, estimate_disk_usage, NodeDiskUsage, ScaleAssessment};
pub use split::{SplitPhase, SplitPlan, SplitReason, SplitResult};

/// Automatic region scaling engine.
///
/// The `AutoScaler` is a stateless planner: given current cluster state
/// and region metrics, it produces split plans and disk-aware rebalance
/// migrations. The caller is responsible for executing the plans via
/// Raft commands.
///
/// # Example
///
/// ```no_run
/// use chronix_cluster::autoscale::{AutoScaler, AutoScaleConfig, RegionMetrics};
///
/// let scaler = AutoScaler::new(AutoScaleConfig::default());
/// ```
#[derive(Debug, Clone)]
pub struct AutoScaler {
    config: AutoScaleConfig,
}

impl AutoScaler {
    /// Create a new auto-scaler with the given configuration.
    #[must_use]
    pub fn new(config: AutoScaleConfig) -> Self {
        Self { config }
    }

    /// Access the configuration.
    #[must_use]
    pub fn config(&self) -> &AutoScaleConfig {
        &self.config
    }
}

/// Shared test helpers used across submodule tests.
#[cfg(test)]
pub(crate) mod tests_util {
    use std::time::Duration;

    use chronix_meta::{NodeId, RegionId, RegionInfo};

    use super::AutoScaleConfig;

    pub fn make_config() -> AutoScaleConfig {
        AutoScaleConfig {
            region_size_threshold: 1_000_000, // 1 MB for test
            region_series_threshold: 1_000,
            scan_interval: Duration::from_secs(10),
            disk_deviation_threshold: 0.20,
            max_concurrent_migrations: 2,
            enable_periodic_rebalancing: false,
            rebalance_interval: Duration::from_secs(300),
        }
    }

    pub fn make_region(id: RegionId, measurement: &str, leader: NodeId) -> RegionInfo {
        RegionInfo::new(id, measurement.to_string(), leader, vec![leader])
    }

    /// Returns a closure that allocates monotonically increasing region IDs.
    pub fn make_id_alloc() -> impl FnMut() -> RegionId {
        let mut counter = 10_000u64;
        move || {
            let id = counter;
            counter += 1;
            id
        }
    }
}
