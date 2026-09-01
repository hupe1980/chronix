//! Per-tenant quota enforcement.
//!
//! Provides a [`QuotaEnforcer`] that checks whether a namespace has
//! available capacity before accepting writes. Enforcement is fast
//! (no I/O, just counter checks) and designed to be called on every
//! write path.

use tracing::warn;

use crate::tenant::error::{Result, TenantError};
use crate::tenant::registry::NamespaceRegistry;

/// Which resource exceeded its quota.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaResource {
    /// Too many distinct time series.
    SeriesCount,
    /// Write rate too high.
    IngestionRate,
    /// Storage limit reached.
    StorageBytes,
    /// Too many measurements.
    Measurements,
}

impl std::fmt::Display for QuotaResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SeriesCount => write!(f, "series_count"),
            Self::IngestionRate => write!(f, "ingestion_rate"),
            Self::StorageBytes => write!(f, "storage_bytes"),
            Self::Measurements => write!(f, "measurements"),
        }
    }
}

/// Quota enforcement engine.
///
/// Checks namespace usage against configured quotas before writes
/// are accepted. When a quota is exceeded, writes are rejected with
/// [`TenantError::QuotaExceeded`] (HTTP 429).
///
/// # Usage Metrics
///
/// The enforcer emits `chronix_namespace_usage_ratio` gauges with
/// labels `{namespace, resource}` for each check, enabling Grafana
/// alerting on approaching limits.
#[derive(Debug)]
pub struct QuotaEnforcer<'a> {
    registry: &'a NamespaceRegistry,
}

impl<'a> QuotaEnforcer<'a> {
    /// Create a new quota enforcer backed by the given registry.
    #[must_use]
    pub fn new(registry: &'a NamespaceRegistry) -> Self {
        Self { registry }
    }

    /// Check whether a write of `new_series` new series and `new_points`
    /// points is allowed in the given namespace.
    ///
    /// # Deprecated
    ///
    /// This method is non-atomic (TOCTOU): it reads the usage snapshot
    /// and checks limits, but does **not** hold a lock during the actual
    /// write. Concurrent callers can all pass the check simultaneously,
    /// allowing the quota to be exceeded. Use
    /// [`check_and_increment_write`](Self::check_and_increment_write) instead,
    /// which atomically checks **and** increments usage under a single lock.
    ///
    /// # Errors
    ///
    /// Returns `QuotaExceeded` if any resource limit would be breached.
    /// Returns `NamespaceNotFound` if the namespace does not exist.
    /// Hidden from public API — use `check_and_increment_write` instead.
    #[doc(hidden)]
    #[deprecated(
        since = "0.2.0",
        note = "Non-atomic (TOCTOU). Use `check_and_increment_write` instead."
    )]
    pub fn check_write(&self, namespace: &str, new_series: u64, new_points: u64) -> Result<()> {
        let state = self.registry.get(namespace)?;
        let quota = &state.info.quota;
        let usage = &state.usage;

        // 1. Series count
        let projected_series = usage.series_count + new_series;
        Self::emit_ratio(
            namespace,
            QuotaResource::SeriesCount,
            projected_series,
            quota.max_series_count,
        );
        if projected_series > quota.max_series_count {
            warn!(
                namespace,
                current = usage.series_count,
                new = new_series,
                limit = quota.max_series_count,
                "series count quota exceeded"
            );
            return Err(TenantError::QuotaExceeded {
                namespace: namespace.to_string(),
                resource: QuotaResource::SeriesCount.to_string(),
                current: usage.series_count,
                limit: quota.max_series_count,
            });
        }

        // 2. Ingestion rate — admission control with burst projection.
        //    We add `new_points` to the tracked rate as a conservative
        //    estimate: if the entire batch lands within one measurement
        //    interval the effective rate spikes by that amount.  This
        //    prevents large bursts from slipping through before the
        //    async rate-tracker updates.
        #[allow(clippy::cast_precision_loss)]
        let projected_rate = usage.ingestion_rate + new_points as f64;
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let projected_rate_u64 = projected_rate as u64;
        Self::emit_ratio(
            namespace,
            QuotaResource::IngestionRate,
            projected_rate_u64,
            quota.max_ingestion_rate,
        );
        #[allow(clippy::cast_precision_loss)]
        let rate_limit = quota.max_ingestion_rate as f64;
        if projected_rate > rate_limit {
            warn!(
                namespace,
                current_rate = usage.ingestion_rate,
                new_points,
                limit = quota.max_ingestion_rate,
                "ingestion rate quota exceeded"
            );
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let current = usage.ingestion_rate as u64;
            return Err(TenantError::QuotaExceeded {
                namespace: namespace.to_string(),
                resource: QuotaResource::IngestionRate.to_string(),
                current,
                limit: quota.max_ingestion_rate,
            });
        }

        // Storage check (soft — actual bytes tracked asynchronously).
        //
        // We apply a 5% safety margin so that writes are rejected
        // before the hard limit is reached.  The async-updated counter
        // can lag behind, so this margin prevents a full flush cycle's
        // worth of data from silently exceeding the quota.
        const STORAGE_QUOTA_MARGIN: f64 = 0.95;
        let effective_limit = (quota.max_storage_bytes as f64 * STORAGE_QUOTA_MARGIN) as u64;
        Self::emit_ratio(
            namespace,
            QuotaResource::StorageBytes,
            usage.storage_bytes,
            quota.max_storage_bytes,
        );
        if usage.storage_bytes > effective_limit {
            return Err(TenantError::QuotaExceeded {
                namespace: namespace.to_string(),
                resource: QuotaResource::StorageBytes.to_string(),
                current: usage.storage_bytes,
                limit: quota.max_storage_bytes,
            });
        }

        Ok(())
    }

    /// Check whether creating a new measurement is allowed.
    ///
    /// # Errors
    ///
    /// Returns `QuotaExceeded` if the measurement count limit would
    /// be breached.
    pub fn check_create_measurement(&self, namespace: &str) -> Result<()> {
        let state = self.registry.get(namespace)?;
        let quota = &state.info.quota;
        let usage = &state.usage;

        let projected = u64::from(usage.measurements) + 1;
        Self::emit_ratio(
            namespace,
            QuotaResource::Measurements,
            projected,
            u64::from(quota.max_measurements),
        );
        if projected > u64::from(quota.max_measurements) {
            return Err(TenantError::QuotaExceeded {
                namespace: namespace.to_string(),
                resource: QuotaResource::Measurements.to_string(),
                current: u64::from(usage.measurements),
                limit: u64::from(quota.max_measurements),
            });
        }

        Ok(())
    }

    /// Atomically check write quota and increment usage.
    ///
    /// This is the preferred method for the write path: it holds the
    /// DashMap shard lock across both the quota check and the usage
    /// update, eliminating the TOCTOU race window that exists when
    /// using [`check_write`](Self::check_write) followed by a separate
    /// increment call.
    ///
    /// # Errors
    ///
    /// Returns `QuotaExceeded` if any resource limit would be breached.
    /// Returns `NamespaceNotFound` if the namespace does not exist.
    pub fn check_and_increment_write(
        &self,
        namespace: &str,
        new_series: u64,
        new_points: u64,
    ) -> Result<()> {
        self.registry
            .check_and_increment_write(namespace, new_series, new_points)
    }

    /// Atomically check measurement quota and increment count.
    ///
    /// # Errors
    ///
    /// Returns `QuotaExceeded` if the measurement limit would be breached.
    /// Returns `NamespaceNotFound` if the namespace does not exist.
    pub fn check_and_increment_measurement(&self, namespace: &str) -> Result<()> {
        self.registry.check_and_increment_measurement(namespace)
    }

    /// Check all quotas for current usage (no projection).
    ///
    /// Returns a list of resources that are currently over quota.
    #[must_use]
    pub fn check_all(&self, namespace: &str) -> Vec<QuotaResource> {
        let Ok(state) = self.registry.get(namespace) else {
            return Vec::new();
        };

        let quota = &state.info.quota;
        let usage = &state.usage;
        let mut violations = Vec::new();

        if usage.series_count > quota.max_series_count {
            violations.push(QuotaResource::SeriesCount);
        }
        #[allow(clippy::cast_precision_loss)]
        if usage.ingestion_rate > quota.max_ingestion_rate as f64 {
            violations.push(QuotaResource::IngestionRate);
        }
        if usage.storage_bytes > quota.max_storage_bytes {
            violations.push(QuotaResource::StorageBytes);
        }
        if u64::from(usage.measurements) > u64::from(quota.max_measurements) {
            violations.push(QuotaResource::Measurements);
        }

        violations
    }

    /// Calculate usage ratio (0.0 – 1.0+) for monitoring.
    #[must_use]
    pub fn usage_ratio(current: u64, limit: u64) -> f64 {
        if limit == 0 {
            // A zero limit with non-zero usage is over-quota.
            // Use f64::MAX instead of f64::INFINITY for JSON-safe serialization
            // (RFC 7159 does not support Infinity).
            return if current > 0 { f64::MAX } else { 0.0 };
        }
        #[allow(clippy::cast_precision_loss)]
        let ratio = current as f64 / limit as f64;
        ratio
    }

    /// Emit a usage ratio gauge metric.
    fn emit_ratio(namespace: &str, resource: QuotaResource, current: u64, limit: u64) {
        let ratio = Self::usage_ratio(current, limit);
        metrics::gauge!(
            "chronix_namespace_usage_ratio",
            "namespace" => namespace.to_string(),
            "resource" => resource.to_string(),
        )
        .set(ratio);
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use chronix_core::{NamespaceId, NamespaceQuota, NamespaceUsage};

    fn setup_registry() -> NamespaceRegistry {
        let registry = NamespaceRegistry::new();
        let id = NamespaceId::new("test-ns").unwrap();
        let quota = NamespaceQuota {
            max_series_count: 100,
            max_ingestion_rate: 1000,
            max_storage_bytes: 1_000_000,
            max_measurements: 10,
            max_request_rps: 0,
            max_request_burst: 0,
        };
        registry
            .create_namespace(id, "test", "admin", quota)
            .unwrap();
        registry
    }

    #[test]
    fn write_within_quota_passes() {
        let registry = setup_registry();
        let enforcer = QuotaEnforcer::new(&registry);
        enforcer.check_write("test-ns", 5, 100).unwrap();
    }

    #[test]
    fn write_exceeds_series_quota() {
        let registry = setup_registry();
        let enforcer = QuotaEnforcer::new(&registry);

        let err = enforcer.check_write("test-ns", 101, 10).unwrap_err();
        assert!(matches!(
            err,
            TenantError::QuotaExceeded {
                resource,
                ..
            } if resource == "series_count"
        ));
    }

    #[test]
    fn write_exceeds_ingestion_rate() {
        let registry = setup_registry();
        let enforcer = QuotaEnforcer::new(&registry);

        let err = enforcer.check_write("test-ns", 0, 1001).unwrap_err();
        assert!(matches!(
            err,
            TenantError::QuotaExceeded {
                resource,
                ..
            } if resource == "ingestion_rate"
        ));
    }

    #[test]
    fn write_exceeds_storage_quota() {
        let registry = setup_registry();
        // Set storage above limit
        let usage = NamespaceUsage {
            storage_bytes: 1_000_001,
            ..Default::default()
        };
        registry.update_usage("test-ns", usage).unwrap();

        let enforcer = QuotaEnforcer::new(&registry);
        let err = enforcer.check_write("test-ns", 0, 1).unwrap_err();
        assert!(matches!(
            err,
            TenantError::QuotaExceeded {
                resource,
                ..
            } if resource == "storage_bytes"
        ));
    }

    #[test]
    fn measurement_quota_enforcement() {
        let registry = setup_registry();
        // Set measurements at limit
        let usage = NamespaceUsage {
            measurements: 10,
            ..Default::default()
        };
        registry.update_usage("test-ns", usage).unwrap();

        let enforcer = QuotaEnforcer::new(&registry);
        let err = enforcer.check_create_measurement("test-ns").unwrap_err();
        assert!(matches!(
            err,
            TenantError::QuotaExceeded {
                resource,
                ..
            } if resource == "measurements"
        ));
    }

    #[test]
    fn check_all_returns_violations() {
        let registry = setup_registry();
        let usage = NamespaceUsage {
            series_count: 200,        // over 100
            storage_bytes: 2_000_000, // over 1_000_000
            ..Default::default()
        };
        registry.update_usage("test-ns", usage).unwrap();

        let enforcer = QuotaEnforcer::new(&registry);
        let violations = enforcer.check_all("test-ns");
        assert_eq!(violations.len(), 2);
        assert!(violations.contains(&QuotaResource::SeriesCount));
        assert!(violations.contains(&QuotaResource::StorageBytes));
    }

    #[test]
    fn check_nonexistent_namespace() {
        let registry = setup_registry();
        let enforcer = QuotaEnforcer::new(&registry);
        let err = enforcer.check_write("nope", 1, 1).unwrap_err();
        assert!(matches!(err, TenantError::NamespaceNotFound(_)));
    }

    #[test]
    fn usage_ratio_calculation() {
        assert!((QuotaEnforcer::usage_ratio(50, 100) - 0.5).abs() < f64::EPSILON);
        assert!((QuotaEnforcer::usage_ratio(100, 100) - 1.0).abs() < f64::EPSILON);
        assert!((QuotaEnforcer::usage_ratio(150, 100) - 1.5).abs() < f64::EPSILON);
        assert!((QuotaEnforcer::usage_ratio(0, 100) - 0.0).abs() < f64::EPSILON);
        assert!((QuotaEnforcer::usage_ratio(0, 0) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn usage_ratio_zero_limit_json_safe() {
        // f64::MAX instead of f64::INFINITY for JSON compatibility.
        let r = QuotaEnforcer::usage_ratio(100, 0);
        assert_eq!(r, f64::MAX);
        assert!(r.is_finite()); // must be JSON-serializable

        // Zero usage with zero limit is just 0.0.
        let r = QuotaEnforcer::usage_ratio(0, 0);
        assert_eq!(r, 0.0);
    }

    #[test]
    fn quota_resource_display() {
        assert_eq!(QuotaResource::SeriesCount.to_string(), "series_count");
        assert_eq!(QuotaResource::IngestionRate.to_string(), "ingestion_rate");
        assert_eq!(QuotaResource::StorageBytes.to_string(), "storage_bytes");
        assert_eq!(QuotaResource::Measurements.to_string(), "measurements");
    }

    #[test]
    fn default_namespace_has_generous_quotas() {
        let registry = NamespaceRegistry::new();
        let enforcer = QuotaEnforcer::new(&registry);
        // Default quota is quite large, should pass easily
        enforcer.check_write("default", 100, 10_000).unwrap();
    }

    // ── Atomic check-and-increment tests ──

    #[test]
    fn check_and_increment_write_updates_usage() {
        let registry = setup_registry();
        let enforcer = QuotaEnforcer::new(&registry);

        enforcer
            .check_and_increment_write("test-ns", 10, 50)
            .unwrap();

        // Verify usage was updated
        let usage = registry.get_usage("test-ns").unwrap();
        assert_eq!(usage.series_count, 10);
    }

    #[test]
    fn check_and_increment_write_rejects_over_quota() {
        let registry = setup_registry();
        let enforcer = QuotaEnforcer::new(&registry);

        let err = enforcer
            .check_and_increment_write("test-ns", 101, 10)
            .unwrap_err();
        assert!(matches!(
            err,
            TenantError::QuotaExceeded { resource, .. }
                if resource == "series_count"
        ));

        // Usage should NOT have been incremented
        let usage = registry.get_usage("test-ns").unwrap();
        assert_eq!(usage.series_count, 0);
    }

    #[test]
    fn check_and_increment_write_sequential_accumulation() {
        let registry = setup_registry();
        let enforcer = QuotaEnforcer::new(&registry);

        // Increment 50 twice = 100, right at limit
        enforcer
            .check_and_increment_write("test-ns", 50, 0)
            .unwrap();
        enforcer
            .check_and_increment_write("test-ns", 50, 0)
            .unwrap();

        // Next increment pushes over limit
        let err = enforcer
            .check_and_increment_write("test-ns", 1, 0)
            .unwrap_err();
        assert!(matches!(err, TenantError::QuotaExceeded { .. }));

        let usage = registry.get_usage("test-ns").unwrap();
        assert_eq!(usage.series_count, 100);
    }

    #[test]
    fn check_and_increment_measurement_works() {
        let registry = setup_registry();
        let enforcer = QuotaEnforcer::new(&registry);

        // Increment 10 times (max_measurements = 10)
        for _ in 0..10 {
            enforcer.check_and_increment_measurement("test-ns").unwrap();
        }

        // 11th should fail
        let err = enforcer
            .check_and_increment_measurement("test-ns")
            .unwrap_err();
        assert!(matches!(err, TenantError::QuotaExceeded { .. }));

        let usage = registry.get_usage("test-ns").unwrap();
        assert_eq!(usage.measurements, 10);
    }

    #[test]
    fn check_and_increment_concurrent_safety() {
        // Verify that concurrent atomic increments don't exceed quota.
        use std::sync::Arc;
        use std::thread;

        let registry = Arc::new(NamespaceRegistry::new());
        let id = NamespaceId::new("concurrent-ns").unwrap();
        let quota = NamespaceQuota {
            max_series_count: 100,
            max_ingestion_rate: 1_000_000,
            max_storage_bytes: 1_000_000_000,
            max_measurements: 1000,
            max_request_rps: 0,
            max_request_burst: 0,
        };
        registry
            .create_namespace(id, "test", "admin", quota)
            .unwrap();

        let handles: Vec<_> = (0..20)
            .map(|_| {
                let reg = Arc::clone(&registry);
                thread::spawn(move || {
                    let mut success = 0u64;
                    for _ in 0..10 {
                        if reg.check_and_increment_write("concurrent-ns", 1, 0).is_ok() {
                            success += 1;
                        }
                    }
                    success
                })
            })
            .collect();

        let total_success: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();

        // Exactly 100 should succeed (quota is 100)
        assert_eq!(total_success, 100);

        let usage = registry.get_usage("concurrent-ns").unwrap();
        assert_eq!(usage.series_count, 100);
    }
}
