//! Forecast Residual anomaly detector.
//!
//! Wraps any [`ForecastModel`] and scores residuals (actual − predicted)
//! using Z-Score statistics.

use crate::forecast::ForecastModel;
use crate::forecast::ModelParams;
use crate::forecast::SesModel;

use crate::anomaly::error::AnomalyError;
use crate::anomaly::traits::{validate_lengths, AnomalyDetector, AnomalyScore, DetectorType};

/// Anomaly detector based on forecast residuals.
///
/// The inner `model` is automatically re-fitted from stored training data
/// during deserialization — no manual `refit()` call needed.
#[derive(serde::Serialize)]
pub struct ForecastResidualDetector {
    threshold: f64,
    residual_mean: f64,
    residual_std: f64,
    #[serde(skip)]
    model: Box<dyn ForecastModel>,
    train_ts: Vec<i64>,
    train_vals: Vec<f64>,
    fitted: bool,
}

impl std::fmt::Debug for ForecastResidualDetector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForecastResidualDetector")
            .field("threshold", &self.threshold)
            .field("residual_mean", &self.residual_mean)
            .field("residual_std", &self.residual_std)
            .field("fitted", &self.fitted)
            .field("train_len", &self.train_ts.len())
            .finish()
    }
}

/// Custom `Deserialize` that automatically re-fits the forecast model
/// from the stored training data, eliminating the need for callers to
/// remember to invoke `refit()`.
impl<'de> serde::Deserialize<'de> for ForecastResidualDetector {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct Raw {
            threshold: f64,
            residual_mean: f64,
            residual_std: f64,
            train_ts: Vec<i64>,
            train_vals: Vec<f64>,
            fitted: bool,
        }

        let raw = Raw::deserialize(deserializer)?;

        // ── Field validation ──────────────────────────────────
        if !raw.threshold.is_finite() {
            return Err(serde::de::Error::custom(
                "threshold must be finite (not NaN or Inf)",
            ));
        }
        if raw.threshold <= 0.0 {
            return Err(serde::de::Error::custom("threshold must be > 0"));
        }
        if !raw.residual_std.is_finite() {
            return Err(serde::de::Error::custom(
                "residual_std must be finite (not NaN or Inf)",
            ));
        }
        if raw.residual_std < 0.0 {
            return Err(serde::de::Error::custom("residual_std must be >= 0"));
        }
        if !raw.residual_mean.is_finite() {
            return Err(serde::de::Error::custom(
                "residual_mean must be finite (not NaN or Inf)",
            ));
        }

        let mut det = ForecastResidualDetector {
            threshold: raw.threshold,
            residual_mean: raw.residual_mean,
            residual_std: raw.residual_std,
            model: default_model(),
            train_ts: raw.train_ts,
            train_vals: raw.train_vals,
            fitted: raw.fitted,
        };

        // Automatically refit the model from stored training data.
        if det.fitted && !det.train_ts.is_empty() {
            if let Err(e) = det.refit() {
                tracing::warn!(
                    error = %e,
                    "failed to refit forecast model during deserialization; \
                     detector will need manual refit()"
                );
                det.fitted = false;
            }
        }

        Ok(det)
    }
}

fn default_model() -> Box<dyn ForecastModel> {
    Box::new(SesModel::new(None))
}

impl ForecastResidualDetector {
    /// Creates a new detector with the given threshold and forecast model.
    ///
    /// If `model` is `None`, defaults to SES with auto-alpha.
    pub fn new(threshold: Option<f64>, model: Option<Box<dyn ForecastModel>>) -> Self {
        Self {
            threshold: threshold.unwrap_or(3.0),
            residual_mean: 0.0,
            residual_std: 0.0,
            model: model.unwrap_or_else(|| Box::new(SesModel::new(None))),
            train_ts: Vec::new(),
            train_vals: Vec::new(),
            fitted: false,
        }
    }

    /// Re-fits the inner forecast model from stored training data.
    ///
    /// Call this after deserializing a `ForecastResidualDetector` to
    /// restore the forecast model (which is not serialized).
    pub fn refit(&mut self) -> Result<(), AnomalyError> {
        if self.train_ts.is_empty() {
            return Err(AnomalyError::InsufficientData { min: 5, got: 0 });
        }
        self.model = Box::new(SesModel::new(None));
        self.fit(&self.train_ts.clone(), &self.train_vals.clone())
    }

    fn compute_residual_stats(residuals: &[f64]) -> (f64, f64) {
        let n = residuals.len() as f64;
        let mean = residuals.iter().sum::<f64>() / n;
        // Use Bessel's correction (n-1) for consistency with simd_std_dev
        // and features::zscore across the codebase.
        let denom = if n > 1.0 { n - 1.0 } else { n };
        let var = residuals.iter().map(|&r| (r - mean).powi(2)).sum::<f64>() / denom;
        (mean, var.sqrt())
    }

    /// Compute strict walk-forward one-step-ahead residuals.
    ///
    /// For each model type, replays the smoothing/fitting equations using
    /// only data seen so far, producing a prediction for the NEXT point
    /// and recording `actual − prediction` as the residual. This
    /// eliminates future-leak bias.
    fn compute_walk_forward_residuals(values: &[f64], params: &ModelParams) -> Vec<f64> {
        if values.is_empty() {
            return Vec::new();
        }
        match params {
            ModelParams::Ses { alpha, .. } => {
                // SES walk-forward: level starts at values[0], predict
                // values[i] using level from values[0..i).
                let alpha = *alpha;
                let one_minus = 1.0 - alpha;
                let mut level = values[0];
                let mut residuals = Vec::with_capacity(values.len().saturating_sub(1));
                for &value in values.iter().skip(1) {
                    residuals.push(value - level);
                    level = alpha * value + one_minus * level;
                }
                residuals
            }
            ModelParams::HoltLinear {
                alpha, beta, phi, ..
            } => {
                // Holt linear walk-forward: predict using level + phi*trend
                let alpha = *alpha;
                let beta = *beta;
                let phi = *phi;
                let mut level = values[0];
                let mut trend = if values.len() > 1 {
                    values[1] - values[0]
                } else {
                    0.0
                };
                let mut residuals = Vec::with_capacity(values.len().saturating_sub(1));
                for &value in values.iter().skip(1) {
                    let forecast = level + phi * trend;
                    residuals.push(value - forecast);
                    let new_level = alpha * value + (1.0 - alpha) * (level + phi * trend);
                    trend = beta * (new_level - level) + (1.0 - beta) * phi * trend;
                    level = new_level;
                }
                residuals
            }
            _ => {
                // Fallback for models without walk-forward support:
                // Use simple lag-1 residuals (value[i] - value[i-1]).
                // This is conservative and doesn't leak future data.
                values.windows(2).map(|w| w[1] - w[0]).collect()
            }
        }
    }

    #[inline]
    fn score_residual(&self, residual: f64) -> f64 {
        if self.residual_std < 1e-15 {
            // Residual std is zero: training residuals were constant.
            // Any non-trivial deviation is highly anomalous.
            return if (residual - self.residual_mean).abs() < 1e-15 {
                0.0
            } else {
                f64::INFINITY
            };
        }
        ((residual - self.residual_mean) / self.residual_std).abs()
    }

    #[inline]
    fn normalize(raw: f64, threshold: f64) -> f64 {
        let x = (raw - threshold) * 2.0;
        1.0 / (1.0 + (-x).exp())
    }
}

impl AnomalyDetector for ForecastResidualDetector {
    fn fit(&mut self, timestamps: &[i64], values: &[f64]) -> Result<(), AnomalyError> {
        let _start = std::time::Instant::now();
        if values.len() < 5 {
            return Err(AnomalyError::InsufficientData {
                min: 5,
                got: values.len(),
            });
        }

        self.model
            .fit(timestamps, values)
            .map_err(|e| AnomalyError::Forecast(e.to_string()))?;

        // Compute strict walk-forward one-step-ahead residuals.
        //
        // For each point i (i ≥ 1), the residual is `values[i] − prediction`,
        // where `prediction` uses ONLY information from values[0..i).
        // This avoids future-leak bias that would occur if we used
        // `model.predict(N)` (which produces N copies of the FINAL level,
        // trained on ALL data including future points).
        let residuals = Self::compute_walk_forward_residuals(values, self.model.params());

        let (mean, std) = Self::compute_residual_stats(&residuals);
        self.residual_mean = mean;
        self.residual_std = std;
        self.train_ts = timestamps.to_vec();
        self.train_vals = values.to_vec();
        self.fitted = true;
        metrics::histogram!("chronix_anomaly_fit_duration_seconds", "method" => "forecast_residual").record(_start.elapsed().as_secs_f64());
        Ok(())
    }

    fn detect(
        &mut self,
        timestamps: &[i64],
        values: &[f64],
    ) -> Result<Vec<AnomalyScore>, AnomalyError> {
        let _start = std::time::Instant::now();
        validate_lengths(timestamps, values)?;
        if !self.fitted {
            return Err(AnomalyError::NotFitted);
        }
        if values.is_empty() {
            return Ok(Vec::new());
        }

        // Use walk-forward residuals consistent with fit() calibration.
        // The first point has no prior state to predict from, so its
        // residual is set to the training mean (z-score ≈ 0).
        let wf = Self::compute_walk_forward_residuals(values, self.model.params());
        let mut residuals = Vec::with_capacity(values.len());
        residuals.push(self.residual_mean); // index 0
        residuals.extend_from_slice(&wf);

        let mut scores = Vec::with_capacity(values.len());
        for (i, &v) in values.iter().enumerate() {
            let residual = residuals[i];
            let raw = self.score_residual(residual);
            let is_anomaly = raw > self.threshold;
            scores.push(AnomalyScore {
                timestamp: timestamps[i],
                value: v,
                score: Self::normalize(raw, self.threshold),
                is_anomaly,
                method: DetectorType::ForecastResidual,
                threshold: self.threshold,
                details: format!("residual={residual:.4} z={raw:.4}"),
            });
        }
        let anomaly_count = scores.iter().filter(|s| s.is_anomaly).count();
        metrics::counter!("chronix_anomaly_detected_total", "method" => "forecast_residual")
            .increment(anomaly_count as u64);
        metrics::histogram!("chronix_anomaly_detect_duration_seconds", "method" => "forecast_residual").record(_start.elapsed().as_secs_f64());
        Ok(scores)
    }

    fn detect_point(&mut self, timestamp: i64, value: f64) -> Result<AnomalyScore, AnomalyError> {
        if !self.fitted {
            return Err(AnomalyError::NotFitted);
        }
        let pred = self
            .model
            .predict(1)
            .map_err(|e| AnomalyError::Forecast(e.to_string()))?;
        let residual = value - pred.values[0];
        let raw = self.score_residual(residual);
        Ok(AnomalyScore {
            timestamp,
            value,
            score: Self::normalize(raw, self.threshold),
            is_anomaly: raw > self.threshold,
            method: DetectorType::ForecastResidual,
            threshold: self.threshold,
            details: format!("residual={residual:.4} z={raw:.4}"),
        })
    }

    fn detector_type(&self) -> DetectorType {
        DetectorType::ForecastResidual
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_spike_in_trend() {
        let ts: Vec<i64> = (0..100).map(|i| i * 1_000_000_000).collect();
        let mut vals: Vec<f64> = (0..100).map(|i| i as f64 * 0.5 + 10.0).collect();
        vals[80] = 500.0; // Spike

        let mut det = ForecastResidualDetector::new(Some(2.0), None);
        det.fit(&ts, &vals).unwrap();
        let scores = det.detect(&ts, &vals).unwrap();
        assert!(scores[80].is_anomaly);
    }

    #[test]
    fn normal_variations_not_flagged() {
        let ts: Vec<i64> = (0..100).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..100).map(|i| 50.0 + (i as f64 * 0.05).sin()).collect();

        let mut det = ForecastResidualDetector::new(Some(3.0), None);
        det.fit(&ts, &vals).unwrap();
        let scores = det.detect(&ts, &vals).unwrap();
        let anomaly_count = scores.iter().filter(|s| s.is_anomaly).count();
        assert!(
            anomaly_count < 5,
            "too many false positives: {anomaly_count}"
        );
    }

    #[test]
    fn insufficient_data() {
        let mut det = ForecastResidualDetector::new(None, None);
        assert!(det.fit(&[0, 1, 2], &[1.0, 2.0, 3.0]).is_err());
    }

    #[test]
    fn detect_empty_input_returns_empty() {
        let ts: Vec<i64> = (0..20).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..20).map(|i| i as f64).collect();

        let mut det = ForecastResidualDetector::new(None, None);
        det.fit(&ts, &vals).unwrap();
        let scores = det.detect(&[], &[]).unwrap();
        assert!(scores.is_empty());
    }

    #[test]
    fn test_deserialize_rejects_negative_threshold() {
        let json = r#"{
            "threshold": -1.0,
            "residual_mean": 0.0,
            "residual_std": 1.0,
            "train_ts": [],
            "train_vals": [],
            "fitted": false
        }"#;
        match serde_json::from_str::<ForecastResidualDetector>(json) {
            Err(err) => assert!(
                err.to_string().contains("threshold must be > 0"),
                "unexpected error: {err}"
            ),
            Ok(_) => panic!("should reject negative threshold"),
        }
    }

    #[test]
    fn test_deserialize_rejects_nan_residual_std() {
        // JSON cannot represent NaN/Inf natively — serde_json rejects
        // out-of-range literals.  We test with a negative value to
        // exercise our validation (the `is_finite()` guard provides
        // defence-in-depth for binary deserializers like postcard).
        let json = r#"{
            "threshold": 3.0,
            "residual_mean": 0.0,
            "residual_std": -0.5,
            "train_ts": [],
            "train_vals": [],
            "fitted": false
        }"#;
        match serde_json::from_str::<ForecastResidualDetector>(json) {
            Err(err) => assert!(
                err.to_string().contains("residual_std must be >= 0"),
                "unexpected error: {err}"
            ),
            Ok(_) => panic!("should reject negative residual_std"),
        }

        // Also verify that serde_json itself rejects Inf (1e400).
        let json_inf = r#"{
            "threshold": 3.0,
            "residual_mean": 0.0,
            "residual_std": 1e400,
            "train_ts": [],
            "train_vals": [],
            "fitted": false
        }"#;
        assert!(
            serde_json::from_str::<ForecastResidualDetector>(json_inf).is_err(),
            "should reject non-finite residual_std"
        );
    }

    #[test]
    fn test_deserialize_rejects_infinite_threshold() {
        // serde_json rejects 1e400 as out-of-range at parse time; the
        // `is_finite()` guard inside our Deserialize impl covers binary
        // formats that can represent Inf.  We verify the overall
        // deserialization fails either way, and also test zero threshold
        // which exercises our own validation path.
        let json_inf = r#"{
            "threshold": 1e400,
            "residual_mean": 0.0,
            "residual_std": 1.0,
            "train_ts": [],
            "train_vals": [],
            "fitted": false
        }"#;
        assert!(
            serde_json::from_str::<ForecastResidualDetector>(json_inf).is_err(),
            "should reject infinite threshold"
        );

        // Zero threshold is also invalid (must be > 0).
        let json_zero = r#"{
            "threshold": 0.0,
            "residual_mean": 0.0,
            "residual_std": 1.0,
            "train_ts": [],
            "train_vals": [],
            "fitted": false
        }"#;
        match serde_json::from_str::<ForecastResidualDetector>(json_zero) {
            Err(err) => assert!(
                err.to_string().contains("threshold must be > 0"),
                "unexpected error: {err}"
            ),
            Ok(_) => panic!("should reject zero threshold"),
        }
    }

    #[test]
    fn test_deserialize_accepts_valid_config() {
        let json = r#"{
            "threshold": 3.0,
            "residual_mean": 0.5,
            "residual_std": 1.2,
            "train_ts": [0, 1000000000, 2000000000, 3000000000, 4000000000],
            "train_vals": [1.0, 2.0, 3.0, 4.0, 5.0],
            "fitted": true
        }"#;
        let result = serde_json::from_str::<ForecastResidualDetector>(json);
        assert!(
            result.is_ok(),
            "valid config should deserialize successfully"
        );
    }
}
