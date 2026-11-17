//! Quota usage ratios for monitoring.
//!
//! Enforcement is not here. The series and measurement quotas are admitted
//! by the database, at the point a series becomes known and against the same
//! exact set as the database-wide cardinality budget; the ingestion and
//! request rates by the server, before a write is executed. What remains in
//! this module is the arithmetic a dashboard needs to show how close a
//! namespace is to its limits.

/// Usage as a fraction of a limit: `current / limit`, where values above
/// `1.0` are over quota.
///
/// A zero limit with non-zero usage returns [`f64::MAX`] rather than
/// infinity, which JSON (RFC 7159) cannot carry.
#[must_use]
pub fn usage_ratio(current: u64, limit: u64) -> f64 {
    if limit == 0 {
        return if current > 0 { f64::MAX } else { 0.0 };
    }
    #[allow(clippy::cast_precision_loss)]
    let ratio = current as f64 / limit as f64;
    ratio
}

/// Set the `chronix_namespace_usage_ratio{namespace, resource}` gauge.
pub fn record_usage_ratio(namespace: &str, resource: &str, current: u64, limit: u64) {
    metrics::gauge!(
        "chronix_namespace_usage_ratio",
        "namespace" => namespace.to_string(),
        "resource" => resource.to_string(),
    )
    .set(usage_ratio(current, limit));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ratio_of_a_zero_limit() {
        assert!((usage_ratio(0, 0) - 0.0).abs() < f64::EPSILON);
        assert!((usage_ratio(100, 0) - f64::MAX).abs() < f64::EPSILON);
    }

    #[test]
    fn ratio_over_quota_exceeds_one() {
        assert!((usage_ratio(200, 100) - 2.0).abs() < f64::EPSILON);
        assert!((usage_ratio(50, 100) - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn recording_a_ratio_does_not_panic_without_a_recorder() {
        record_usage_ratio("team-a", "series_count", 5, 10);
    }
}
