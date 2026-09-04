//! Core traits and types for anomaly detection.

use serde::{Deserialize, Serialize};

use crate::anomaly::error::AnomalyError;

/// Which detection method produced a score.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DetectorType {
    /// Standard Z-score threshold.
    ZScore,
    /// Modified Z-score using median absolute deviation.
    ModifiedZScore,
    /// Interquartile range fence.
    Iqr,
    /// Forecast-residual based detection.
    ForecastResidual,
    /// Moving average residual based detection.
    MovingAverageResidual,
    /// Dynamic threshold detection.
    DynamicThreshold,
    /// CUSUM (Cumulative Sum) change-point detection.
    Cusum,
    /// Custom plugin detector identified by name.
    Custom(String),
}

/// A single point's anomaly assessment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyScore {
    /// Observation timestamp (nanoseconds).
    pub timestamp: i64,
    /// Observed value.
    pub value: f64,
    /// Normalized score in `[0.0, 1.0]` — higher means more anomalous.
    pub score: f64,
    /// Whether the point was classified as anomalous.
    pub is_anomaly: bool,
    /// Detection method that produced this score.
    pub method: DetectorType,
    /// Threshold used for classification.
    pub threshold: f64,
    /// Human-readable explanation.
    pub details: String,
}

/// Anomaly detection contract.
pub trait AnomalyDetector: Send + Sync {
    /// Fit the detector on historical data.
    fn fit(&mut self, timestamps: &[i64], values: &[f64]) -> Result<(), AnomalyError>;

    /// Score every point in the provided data.
    fn detect(
        &mut self,
        timestamps: &[i64],
        values: &[f64],
    ) -> Result<Vec<AnomalyScore>, AnomalyError>;

    /// Score a single streaming point (O(1) after fitting).
    fn detect_point(&mut self, timestamp: i64, value: f64) -> Result<AnomalyScore, AnomalyError>;

    /// What kind of detector this is.
    fn detector_type(&self) -> DetectorType;
}

/// Validate that `timestamps` and `values` have the same length.
///
/// Call at the start of every `detect()` / `fit()` implementation to
/// produce a clear `AnomalyError::InvalidInput` instead of a panic.
#[inline]
pub fn validate_lengths(timestamps: &[i64], values: &[f64]) -> Result<(), AnomalyError> {
    if timestamps.len() != values.len() {
        return Err(AnomalyError::InvalidInput(format!(
            "timestamps length ({}) != values length ({})",
            timestamps.len(),
            values.len(),
        )));
    }
    Ok(())
}

/// A numerically and dimensionally sound floor for a dispersion estimate.
///
/// # Why a bare epsilon is not enough
///
/// Every z-score detector divides by a fitted σ. On a baseline with no
/// variation — a quantised sensor, a gauge pinned at its limit, a counter
/// that has not moved — σ is 0 and the quotient is `inf`, so a value one ULP
/// away from the mean scores `inf` and is reported as an anomaly with
/// `z=inf` in the details string. Substituting a fixed `1e-12` instead is
/// dimensionally wrong: on a series measured in nanoseconds it is a
/// perfectly ordinary spread, and on one measured in watts it makes 1e-13 of
/// float noise a 0.1-σ event *or* a 10-σ one depending only on the unit.
///
/// The floor here is **relative to the level being measured**:
/// `max(scale, |level|·1e-9, 1e-12)`. A billionth of the level is far below
/// any real signal and far above f64 rounding noise (which is ~1e-16 of the
/// level), so genuine shifts still score large while representation noise
/// scores ~0. The absolute `1e-12` term keeps a level of exactly 0 finite.
#[inline]
#[must_use]
pub fn scale_floor(scale: f64, level: f64) -> f64 {
    const RELATIVE: f64 = 1e-9;
    const ABSOLUTE: f64 = 1e-12;
    scale.max(level.abs() * RELATIVE).max(ABSOLUTE)
}
