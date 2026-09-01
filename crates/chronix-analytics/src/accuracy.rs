//! Forecast accuracy tracking — compares past forecasts with actual values.
//!
//! Computes MAPE, RMSE, and MAE for each model and writes accuracy metrics.

use std::collections::VecDeque;

use metrics::gauge;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::debug;

/// Accuracy metrics for a forecast model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccuracyMetrics {
    /// Model type name.
    pub model_type: String,
    /// Measurement name.
    pub measurement: String,
    /// Model version or identifier.
    pub version: String,
    /// Mean Absolute Percentage Error.
    pub mape: f64,
    /// Root Mean Square Error.
    pub rmse: f64,
    /// Mean Absolute Error.
    pub mae: f64,
    /// Mean Absolute Scaled Error (MASE) — scale-independent metric
    /// recommended by Hyndman & Koehler (2006). Uses in-sample naive
    /// forecast errors as the scaling denominator. `None` when no
    /// training data is provided or when the naive MAE is zero.
    pub mase: Option<f64>,
    /// Number of data points evaluated.
    pub n_points: usize,
    /// Evaluation timestamp (nanos since epoch).
    pub evaluated_at: i64,
}

impl AccuracyMetrics {
    /// Emit Prometheus metrics.
    pub fn emit_prometheus(&self) {
        gauge!(
            "chronix_forecast_mape",
            "measurement" => self.measurement.clone(),
            "model_type" => self.model_type.clone()
        )
        .set(self.mape);
        gauge!(
            "chronix_forecast_rmse",
            "measurement" => self.measurement.clone(),
            "model_type" => self.model_type.clone()
        )
        .set(self.rmse);
        gauge!(
            "chronix_forecast_mae",
            "measurement" => self.measurement.clone(),
            "model_type" => self.model_type.clone()
        )
        .set(self.mae);
        if let Some(mase_val) = self.mase {
            gauge!(
                "chronix_forecast_mase",
                "measurement" => self.measurement.clone(),
                "model_type" => self.model_type.clone()
            )
            .set(mase_val);
        }
    }
}

/// Compute accuracy metrics from forecasted vs actual values.
///
/// # Errors
///
/// Returns `AnalyticsError::Config` if `forecasted` and `actual` have
/// different lengths.
pub fn compute_accuracy(
    forecasted: &[f64],
    actual: &[f64],
    model_type: &str,
    measurement: &str,
    version: &str,
) -> std::result::Result<AccuracyMetrics, crate::error::AnalyticsError> {
    if forecasted.len() != actual.len() {
        return Err(crate::error::AnalyticsError::Config(format!(
            "forecasted length ({}) != actual length ({})",
            forecasted.len(),
            actual.len(),
        )));
    }

    let n = forecasted.len();
    if n == 0 {
        return Ok(AccuracyMetrics {
            model_type: model_type.to_string(),
            measurement: measurement.to_string(),
            version: version.to_string(),
            mape: 0.0,
            rmse: 0.0,
            mae: 0.0,
            mase: None,
            n_points: 0,
            evaluated_at: now_nanos(),
        });
    }

    let mut sum_abs_error = 0.0;
    let mut sum_sq_error = 0.0;
    let mut sum_abs_pct_error = 0.0;
    let mut pct_count = 0usize;

    // MAPE is undefined when actual == 0 (division by zero).
    // We skip such points, matching the standard "filtered MAPE"
    // definition.  If ALL actual values are zero, MAPE returns 0.0
    // rather than NaN — callers should check `n_points` for validity.
    for (f, a) in forecasted.iter().zip(actual.iter()) {
        let error = f - a;
        sum_abs_error += error.abs();
        sum_sq_error += error * error;

        if a.abs() > f64::EPSILON {
            sum_abs_pct_error += (error / a).abs();
            pct_count += 1;
        }
    }

    let mae = sum_abs_error / n as f64;
    let rmse = (sum_sq_error / n as f64).sqrt();
    // Return NaN (not 0.0) when ALL actuals are zero,
    // making MAPE correctly distinguishable from "perfect forecast".
    // Callers (AccuracyTracker, ABTestEvaluator) already guard on
    // `is_nan()` before comparisons.
    let mape = if pct_count > 0 {
        100.0 * sum_abs_pct_error / pct_count as f64
    } else {
        f64::NAN
    };

    Ok(AccuracyMetrics {
        model_type: model_type.to_string(),
        measurement: measurement.to_string(),
        version: version.to_string(),
        mape,
        rmse,
        mae,
        mase: None,
        n_points: n,
        evaluated_at: now_nanos(),
    })
}

/// Compute accuracy metrics including MASE using in-sample training data.
///
/// MASE (Mean Absolute Scaled Error) is a scale-independent metric that
/// compares the forecast MAE against a naive random-walk baseline computed
/// from the `training_actual` series. A MASE < 1 means the forecast is
/// better than the naive baseline; MASE > 1 means it is worse.
///
/// Reference: Hyndman & Koehler (2006), "Another look at measures of
/// forecast accuracy", International Journal of Forecasting.
///
/// # Errors
///
/// Returns `AnalyticsError::Config` if `forecasted` and `actual` have
/// different lengths, or if `training_actual` has fewer than 2 elements.
pub fn compute_accuracy_with_mase(
    forecasted: &[f64],
    actual: &[f64],
    training_actual: &[f64],
    model_type: &str,
    measurement: &str,
    version: &str,
) -> std::result::Result<AccuracyMetrics, crate::error::AnalyticsError> {
    let mut metrics = compute_accuracy(forecasted, actual, model_type, measurement, version)?;
    let mase_val = mase(actual, forecasted, training_actual);
    metrics.mase = if mase_val.is_finite() {
        Some(mase_val)
    } else {
        None
    };
    Ok(metrics)
}

/// Compute the Mean Absolute Scaled Error (MASE).
///
/// Returns `NaN` when `training` has fewer than 2 elements or the naive
/// in-sample MAE is zero (constant training series).
pub fn mase(actual: &[f64], forecasted: &[f64], training: &[f64]) -> f64 {
    if training.len() < 2 || actual.len() != forecasted.len() || actual.is_empty() {
        return f64::NAN;
    }
    // Naive in-sample MAE: mean of |y_t - y_{t-1}| for t=1..n
    let naive_mae: f64 = training
        .windows(2)
        .map(|w| (w[1] - w[0]).abs())
        .sum::<f64>()
        / (training.len() - 1) as f64;
    if naive_mae < f64::EPSILON {
        return f64::NAN;
    }
    let forecast_mae: f64 = actual
        .iter()
        .zip(forecasted.iter())
        .map(|(a, f)| (a - f).abs())
        .sum::<f64>()
        / actual.len() as f64;
    forecast_mae / naive_mae
}

/// Forecast accuracy tracker — stores and queries accuracy metrics.
pub struct ForecastAccuracyTracker {
    metrics: RwLock<VecDeque<AccuracyMetrics>>,
    /// Maximum staleness before warning (nanos).
    max_staleness_nanos: i64,
    /// Maximum stored metrics (FIFO eviction). Default: 10,000.
    max_entries: usize,
}

impl ForecastAccuracyTracker {
    /// Create a new tracker with default staleness (24h).
    #[must_use]
    pub fn new() -> Self {
        Self {
            metrics: RwLock::new(VecDeque::new()),
            // 24 hours in nanos
            max_staleness_nanos: 24 * 3600 * 1_000_000_000,
            max_entries: 10_000,
        }
    }

    /// Set the maximum staleness.
    #[must_use]
    pub fn with_max_staleness_nanos(mut self, nanos: i64) -> Self {
        self.max_staleness_nanos = nanos;
        self
    }

    /// Set maximum stored metrics (FIFO eviction when exceeded).
    #[must_use]
    pub fn with_max_entries(mut self, max: usize) -> Self {
        self.max_entries = max;
        self
    }

    /// Record an accuracy evaluation.
    pub fn record(&self, metrics: AccuracyMetrics) {
        metrics.emit_prometheus();
        debug!(
            measurement = %metrics.measurement,
            model_type = %metrics.model_type,
            mape = metrics.mape,
            rmse = metrics.rmse,
            mae = metrics.mae,
            "Forecast accuracy recorded"
        );
        let mut store = self.metrics.write();
        store.push_back(metrics);
        while store.len() > self.max_entries {
            store.pop_front();
        }
    }

    /// Evaluate accuracy from forecasted vs actual values and record.
    ///
    /// # Errors
    ///
    /// Returns `AnalyticsError::Config` if `forecasted` and `actual` have
    /// different lengths.
    pub fn evaluate(
        &self,
        forecasted: &[f64],
        actual: &[f64],
        model_type: &str,
        measurement: &str,
        version: &str,
    ) -> std::result::Result<AccuracyMetrics, crate::error::AnalyticsError> {
        let m = compute_accuracy(forecasted, actual, model_type, measurement, version)?;
        self.record(m.clone());
        Ok(m)
    }

    /// Get all accuracy metrics.
    #[must_use]
    pub fn all(&self) -> Vec<AccuracyMetrics> {
        self.metrics.read().iter().cloned().collect()
    }

    /// Get accuracy metrics for a measurement.
    #[must_use]
    pub fn for_measurement(&self, measurement: &str) -> Vec<AccuracyMetrics> {
        self.metrics
            .read()
            .iter()
            .filter(|m| m.measurement == measurement)
            .cloned()
            .collect()
    }

    /// Get the latest accuracy metric for a measurement.
    #[must_use]
    pub fn latest(&self, measurement: &str) -> Option<AccuracyMetrics> {
        self.metrics
            .read()
            .iter()
            .filter(|m| m.measurement == measurement)
            .max_by_key(|m| m.evaluated_at)
            .cloned()
    }

    /// Check for stale forecasts — returns measurements with no recent evaluation.
    #[must_use]
    pub fn stale_measurements(&self, known_measurements: &[&str]) -> Vec<String> {
        let now = now_nanos();
        let store = self.metrics.read();

        known_measurements
            .iter()
            .filter(|m| {
                let latest = store
                    .iter()
                    .filter(|acc| acc.measurement == **m)
                    .max_by_key(|acc| acc.evaluated_at);
                match latest {
                    Some(acc) => (now - acc.evaluated_at) > self.max_staleness_nanos,
                    None => true,
                }
            })
            .map(std::string::ToString::to_string)
            .collect()
    }

    /// Count of recorded evaluations.
    #[must_use]
    pub fn count(&self) -> usize {
        self.metrics.read().len()
    }
}

impl Default for ForecastAccuracyTracker {
    fn default() -> Self {
        Self::new()
    }
}

fn now_nanos() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    )
    .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_accuracy_perfect() {
        let actual = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let forecasted = actual.clone();
        let m = compute_accuracy(&forecasted, &actual, "ses", "cpu", "v1").unwrap();
        assert!(m.mape.abs() < f64::EPSILON);
        assert!(m.rmse.abs() < f64::EPSILON);
        assert!(m.mae.abs() < f64::EPSILON);
        assert_eq!(m.n_points, 5);
    }

    #[test]
    fn compute_accuracy_with_error() {
        let actual = vec![10.0, 20.0, 30.0];
        let forecasted = vec![12.0, 18.0, 33.0];
        let m = compute_accuracy(&forecasted, &actual, "holt", "energy", "v1").unwrap();

        // MAE = (2 + 2 + 3) / 3 = 2.333...
        assert!((m.mae - 7.0 / 3.0).abs() < 1e-10);
        // MAPE = 100 * ((2/10) + (2/20) + (3/30)) / 3 = 100 * (0.2 + 0.1 + 0.1) / 3 = 100 * 0.4/3
        assert!((m.mape - 100.0 * 0.4 / 3.0).abs() < 1e-10);
        // RMSE = sqrt((4 + 4 + 9) / 3) = sqrt(17/3)
        assert!((m.rmse - (17.0_f64 / 3.0).sqrt()).abs() < 1e-10);
    }

    #[test]
    fn compute_accuracy_empty() {
        let m = compute_accuracy(&[], &[], "ses", "cpu", "v1").unwrap();
        assert_eq!(m.n_points, 0);
        assert!(m.mape.abs() < f64::EPSILON);
    }

    #[test]
    fn compute_accuracy_zero_actual() {
        // When actual is 0, MAPE skips that point
        let actual = vec![0.0, 10.0];
        let forecasted = vec![1.0, 11.0];
        let m = compute_accuracy(&forecasted, &actual, "ses", "cpu", "v1").unwrap();
        // MAPE only uses the non-zero actual: 100 * |1/10| / 1 = 10%
        assert!((m.mape - 10.0).abs() < 1e-10);
        // MAE = (1 + 1) / 2 = 1
        assert!((m.mae - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn tracker_record_and_query() {
        let tracker = ForecastAccuracyTracker::new();
        let m1 = compute_accuracy(&[10.0], &[10.5], "ses", "cpu", "v1").unwrap();
        let m2 = compute_accuracy(&[20.0], &[21.0], "holt", "energy", "v1").unwrap();
        tracker.record(m1);
        tracker.record(m2);

        assert_eq!(tracker.count(), 2);
        let cpu = tracker.for_measurement("cpu");
        assert_eq!(cpu.len(), 1);
        assert_eq!(cpu[0].model_type, "ses");
    }

    #[test]
    fn tracker_latest() {
        let tracker = ForecastAccuracyTracker::new();

        let mut m1 = compute_accuracy(&[10.0], &[10.5], "ses", "cpu", "v1").unwrap();
        m1.evaluated_at = 100;
        let mut m2 = compute_accuracy(&[10.0], &[10.3], "ses", "cpu", "v2").unwrap();
        m2.evaluated_at = 200;

        tracker.record(m1);
        tracker.record(m2);

        let latest = tracker.latest("cpu").unwrap();
        assert_eq!(latest.version, "v2");
        assert_eq!(latest.evaluated_at, 200);
    }

    #[test]
    fn tracker_evaluate() {
        let tracker = ForecastAccuracyTracker::new();
        let m = tracker
            .evaluate(&[10.0, 20.0], &[11.0, 19.0], "ses", "cpu", "v1")
            .unwrap();
        assert_eq!(m.n_points, 2);
        assert_eq!(tracker.count(), 1);
    }

    #[test]
    fn stale_measurements() {
        let tracker = ForecastAccuracyTracker::new().with_max_staleness_nanos(1_000_000); // 1ms

        // cpu has a recent evaluation
        tracker
            .evaluate(&[10.0], &[10.5], "ses", "cpu", "v1")
            .unwrap();

        // memory has no evaluation at all
        let stale = tracker.stale_measurements(&["cpu", "memory"]);
        assert!(stale.contains(&"memory".to_string()));
        // cpu may or may not be stale depending on timing
    }

    #[test]
    fn accuracy_serde_roundtrip() {
        let m = compute_accuracy(&[1.0, 2.0], &[1.1, 2.2], "ses", "cpu", "v1").unwrap();
        let json = serde_json::to_string(&m).unwrap();
        let de: AccuracyMetrics = serde_json::from_str(&json).unwrap();
        assert_eq!(de.model_type, "ses");
        assert_eq!(de.measurement, "cpu");
        assert!((de.mae - m.mae).abs() < f64::EPSILON);
    }

    #[test]
    fn bounded_accuracy_fifo_eviction() {
        let tracker = ForecastAccuracyTracker::new().with_max_entries(3);

        for i in 0..6 {
            let mut m = compute_accuracy(&[1.0], &[1.1], "ses", &format!("m{i}"), "v1").unwrap();
            m.evaluated_at = i as i64;
            tracker.record(m);
        }

        assert_eq!(tracker.count(), 3);
        let all = tracker.all();
        // Only m3, m4, m5 should remain
        let measurements: Vec<_> = all.iter().map(|m| m.measurement.as_str()).collect();
        assert_eq!(measurements, vec!["m3", "m4", "m5"]);
    }

    #[test]
    fn bounded_accuracy_latest_uses_retained() {
        let tracker = ForecastAccuracyTracker::new().with_max_entries(2);

        // Record 3 for "cpu" → only the last 2 are retained
        for i in 0..3 {
            let mut m = compute_accuracy(&[1.0], &[1.1 + i as f64], "ses", "cpu", &format!("v{i}"))
                .unwrap();
            m.evaluated_at = i as i64;
            tracker.record(m);
        }

        assert_eq!(tracker.count(), 2);
        let latest = tracker.latest("cpu").unwrap();
        assert_eq!(latest.version, "v2");
    }
}
