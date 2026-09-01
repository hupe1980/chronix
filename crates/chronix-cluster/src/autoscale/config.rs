//! Auto-scale configuration and region metrics types.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Configuration for automatic region scaling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoScaleConfig {
    /// Maximum region size in bytes before triggering a split (default: 10 GB).
    pub region_size_threshold: u64,

    /// Maximum series count per region before triggering a split (default: 100,000).
    pub region_series_threshold: u64,

    /// Interval between auto-scale scans (default: 60 seconds).
    pub scan_interval: Duration,

    /// Maximum disk usage deviation (fraction) before rebalancing (default: 0.20 = 20%).
    pub disk_deviation_threshold: f64,

    /// Maximum concurrent migrations during rebalancing (default: 2).
    pub max_concurrent_migrations: usize,

    /// Enable periodic automatic rebalancing (default: false — manual only).
    pub enable_periodic_rebalancing: bool,

    /// Interval between automatic rebalancing checks (default: 300 seconds).
    /// Only used when `enable_periodic_rebalancing` is true.
    pub rebalance_interval: Duration,
}

impl Default for AutoScaleConfig {
    fn default() -> Self {
        Self {
            region_size_threshold: 10 * 1024 * 1024 * 1024, // 10 GB
            region_series_threshold: 100_000,
            scan_interval: Duration::from_secs(60),
            disk_deviation_threshold: 0.20,
            max_concurrent_migrations: 2,
            enable_periodic_rebalancing: false,
            rebalance_interval: Duration::from_secs(300),
        }
    }
}

/// Observed metrics for a single region.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RegionMetrics {
    /// Data size in bytes.
    pub size_bytes: u64,
    /// Number of distinct series.
    pub series_count: u64,
    /// Write throughput (points per second, scaled × 1000 to avoid float).
    pub write_rate_milli: u64,
    /// Timestamp (seconds since UNIX epoch) when these metrics were collected.
    ///
    /// Autoscale evaluations skip metrics older than 2× the scan interval to
    /// prevent stale data from triggering spurious scale-up/down decisions.
    pub collected_at_secs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let cfg = AutoScaleConfig::default();
        assert_eq!(cfg.region_size_threshold, 10 * 1024 * 1024 * 1024);
        assert_eq!(cfg.region_series_threshold, 100_000);
        assert_eq!(cfg.max_concurrent_migrations, 2);
        assert!((cfg.disk_deviation_threshold - 0.20).abs() < f64::EPSILON);
    }

    #[test]
    fn config_serde_roundtrip() {
        let cfg = AutoScaleConfig::default();
        let json = serde_json::to_string(&cfg).unwrap();
        let deserialized: AutoScaleConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(
            deserialized.region_size_threshold,
            cfg.region_size_threshold
        );
        assert_eq!(
            deserialized.region_series_threshold,
            cfg.region_series_threshold
        );
    }

    #[test]
    fn default_config_includes_periodic_fields() {
        let cfg = AutoScaleConfig::default();
        assert!(!cfg.enable_periodic_rebalancing);
        assert_eq!(cfg.rebalance_interval, Duration::from_secs(300));
    }
}
