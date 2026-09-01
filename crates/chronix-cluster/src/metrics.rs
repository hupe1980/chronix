//! Cluster metrics constants and recording helpers.
//!
//! Provides named constants for metric keys and thin wrappers around
//! the [`metrics`] crate macros for ergonomic recording.

use std::time::Duration;

/// Total number of nodes in the cluster, labelled by mode and state.
pub const NODES_TOTAL: &str = "chronix_cluster_nodes_total";

/// Total number of regions, labelled by measurement and state.
pub const REGIONS_TOTAL: &str = "chronix_cluster_regions_total";

/// Heartbeat round-trip latency in seconds (histogram).
pub const HEARTBEAT_LATENCY: &str = "chronix_cluster_heartbeat_latency_seconds";

/// Write operation latency in seconds (histogram).
pub const WRITE_LATENCY: &str = "chronix_cluster_write_latency_seconds";

/// Query operation latency in seconds (histogram).
pub const QUERY_LATENCY: &str = "chronix_cluster_query_latency_seconds";

/// Number of under-replicated regions (gauge).
pub const UNDER_REPLICATED: &str = "chronix_cluster_under_replicated_regions";

/// Total number of leader changes (counter).
pub const LEADER_CHANGES: &str = "chronix_cluster_leader_changes_total";

/// Total number of region splits executed (counter).
pub const REGION_SPLITS: &str = "chronix_cluster_region_splits_total";

/// Raft log replication lag in seconds per region (histogram).
pub const REPLICATION_LAG: &str = "chronix_raft_log_replication_lag";

/// Total number of replication requests (counter per region).
pub const REPLICATION_REQUESTS_TOTAL: &str = "chronix_cluster_replication_requests_total";

/// Total number of replication errors (counter per region + error type).
pub const REPLICATION_ERRORS_TOTAL: &str = "chronix_cluster_replication_errors_total";

/// Replication write latency per-region (histogram in seconds).
pub const REPLICATION_LATENCY_SECONDS: &str = "chronix_cluster_replication_latency_seconds";

/// Total number of circuit breaker open events (counter per node).
pub const CIRCUIT_BREAKER_OPEN_TOTAL: &str = "chronix_circuit_breaker_open_total";

/// Record a heartbeat round-trip latency sample.
pub fn record_heartbeat_latency(duration: Duration) {
    metrics::histogram!(HEARTBEAT_LATENCY).record(duration.as_secs_f64());
}

/// Record a write operation latency sample.
pub fn record_write_latency(duration: Duration) {
    metrics::histogram!(WRITE_LATENCY).record(duration.as_secs_f64());
}

/// Record a query operation latency sample.
pub fn record_query_latency(duration: Duration) {
    metrics::histogram!(QUERY_LATENCY).record(duration.as_secs_f64());
}

/// Set the total node count for a given mode and state label pair.
#[allow(clippy::cast_precision_loss)]
pub fn set_nodes_total(mode: &str, state: &str, count: u64) {
    metrics::gauge!(NODES_TOTAL, "mode" => mode.to_owned(), "state" => state.to_owned())
        .set(count as f64);
}

/// Set the total region count for a given measurement and state label pair.
#[allow(clippy::cast_precision_loss)]
pub fn set_regions_total(measurement: &str, state: &str, count: u64) {
    metrics::gauge!(REGIONS_TOTAL, "measurement" => measurement.to_owned(), "state" => state.to_owned())
        .set(count as f64);
}

/// Increment the leader-change counter.
pub fn increment_leader_changes() {
    metrics::counter!(LEADER_CHANGES).increment(1);
}

/// Increment the region-splits counter.
///
/// Called after a region split completes successfully.
pub fn increment_region_splits(measurement: &str) {
    metrics::counter!(REGION_SPLITS, "measurement" => measurement.to_owned()).increment(1);
}

/// Set the number of under-replicated regions.
#[allow(clippy::cast_precision_loss)]
pub fn set_under_replicated(count: u64) {
    metrics::gauge!(UNDER_REPLICATED).set(count as f64);
}

/// Record a replication lag sample for a specific region.
pub fn record_replication_lag(region_id: u64, lag_seconds: f64) {
    metrics::histogram!(REPLICATION_LAG, "region" => region_id.to_string()).record(lag_seconds);
}

/// Increment the replication requests counter for a region.
pub fn increment_replication_requests(region_id: u64) {
    metrics::counter!(
        REPLICATION_REQUESTS_TOTAL,
        "region" => region_id.to_string()
    )
    .increment(1);
}

/// Increment the replication errors counter for a region.
pub fn increment_replication_errors(region_id: u64, error_type: &str) {
    metrics::counter!(
        REPLICATION_ERRORS_TOTAL,
        "region" => region_id.to_string(),
        "error" => error_type.to_owned()
    )
    .increment(1);
}

/// Increment the circuit-breaker-open counter for a node.
pub fn increment_circuit_breaker_open(node_id: u64) {
    metrics::counter!(
        CIRCUIT_BREAKER_OPEN_TOTAL,
        "node_id" => node_id.to_string()
    )
    .increment(1);
}

/// Record a replication write latency sample for a region.
pub fn record_replication_latency(region_id: u64, duration: Duration) {
    metrics::histogram!(
        REPLICATION_LATENCY_SECONDS,
        "region" => region_id.to_string()
    )
    .record(duration.as_secs_f64());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constants_are_prefixed() {
        assert!(NODES_TOTAL.starts_with("chronix_cluster_"));
        assert!(REGIONS_TOTAL.starts_with("chronix_cluster_"));
        assert!(HEARTBEAT_LATENCY.starts_with("chronix_cluster_"));
        assert!(WRITE_LATENCY.starts_with("chronix_cluster_"));
        assert!(QUERY_LATENCY.starts_with("chronix_cluster_"));
        assert!(UNDER_REPLICATED.starts_with("chronix_cluster_"));
        assert!(LEADER_CHANGES.starts_with("chronix_cluster_"));
        assert!(REGION_SPLITS.starts_with("chronix_cluster_"));
        assert!(REPLICATION_LAG.starts_with("chronix_raft_"));
    }

    #[test]
    fn record_heartbeat_latency_does_not_panic() {
        record_heartbeat_latency(Duration::from_millis(5));
    }

    #[test]
    fn record_write_latency_does_not_panic() {
        record_write_latency(Duration::from_millis(10));
    }

    #[test]
    fn record_query_latency_does_not_panic() {
        record_query_latency(Duration::from_millis(20));
    }

    #[test]
    fn set_nodes_total_does_not_panic() {
        set_nodes_total("data", "active", 5);
        set_nodes_total("meta", "dead", 0);
    }

    #[test]
    fn set_regions_total_does_not_panic() {
        set_regions_total("cpu", "active", 10);
        set_regions_total("mem", "migrating", 1);
    }

    #[test]
    fn increment_leader_changes_does_not_panic() {
        increment_leader_changes();
        increment_leader_changes();
    }

    #[test]
    fn increment_region_splits_does_not_panic() {
        increment_region_splits("cpu");
        increment_region_splits("mem");
    }

    #[test]
    fn set_under_replicated_does_not_panic() {
        set_under_replicated(3);
        set_under_replicated(0);
    }

    #[test]
    fn record_replication_lag_does_not_panic() {
        record_replication_lag(1, 0.05);
        record_replication_lag(2, 0.0);
    }

    #[test]
    fn replication_metric_constants_are_prefixed() {
        assert!(REPLICATION_REQUESTS_TOTAL.starts_with("chronix_cluster_"));
        assert!(REPLICATION_ERRORS_TOTAL.starts_with("chronix_cluster_"));
        assert!(REPLICATION_LATENCY_SECONDS.starts_with("chronix_cluster_"));
    }

    #[test]
    fn circuit_breaker_metric_constant_is_prefixed() {
        assert!(CIRCUIT_BREAKER_OPEN_TOTAL.starts_with("chronix_circuit_breaker_"));
    }

    #[test]
    fn increment_circuit_breaker_open_does_not_panic() {
        increment_circuit_breaker_open(1);
        increment_circuit_breaker_open(2);
    }

    #[test]
    fn increment_replication_requests_does_not_panic() {
        increment_replication_requests(1);
        increment_replication_requests(2);
    }

    #[test]
    fn increment_replication_errors_does_not_panic() {
        increment_replication_errors(1, "retriable");
        increment_replication_errors(2, "permanent");
    }

    #[test]
    fn record_replication_latency_does_not_panic() {
        record_replication_latency(1, Duration::from_millis(15));
        record_replication_latency(2, Duration::from_millis(50));
    }
}
