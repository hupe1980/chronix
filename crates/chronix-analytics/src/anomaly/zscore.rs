//! Z-Score based anomaly detector.

use crate::compute::{simd_mean, simd_variance};

use crate::anomaly::error::AnomalyError;
use crate::anomaly::traits::{
    scale_floor, validate_lengths, AnomalyDetector, AnomalyScore, DetectorType,
};

/// Flags points whose absolute Z-Score exceeds a configurable threshold.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct ZScoreDetector {
    threshold: f64,
    mean: f64,
    std_dev: f64,
    fitted: bool,
}

impl ZScoreDetector {
    /// Creates a new detector.  `threshold` defaults to 3.0σ.
    pub fn new(threshold: Option<f64>) -> Self {
        Self {
            threshold: threshold.unwrap_or(3.0),
            mean: 0.0,
            std_dev: 0.0,
            fitted: false,
        }
    }

    /// Absolute z-score against the fitted baseline.
    ///
    /// σ is floored *relative to the fitted mean* (see [`scale_floor`]), so a
    /// zero-variance baseline scores a value 1e-13 away from the mean at
    /// ~1e-4 σ rather than at `inf`. The returned score is always finite for
    /// a finite input, which is what keeps `inf` out of the details string
    /// and out of the alert payloads built from it.
    #[inline]
    fn score_value(&self, value: f64) -> f64 {
        ((value - self.mean) / scale_floor(self.std_dev, self.mean)).abs()
    }

    /// Normalize a raw z-score into `[0, 1]` using a sigmoid mapping.
    #[inline]
    fn normalize(raw: f64, threshold: f64) -> f64 {
        // Maps `threshold` → ~0.5, well-above → ~1.0
        let x = (raw - threshold) * 2.0;
        1.0 / (1.0 + (-x).exp())
    }
}

impl AnomalyDetector for ZScoreDetector {
    #[tracing::instrument(skip_all, level = "debug")]
    fn fit(&mut self, _timestamps: &[i64], values: &[f64]) -> Result<(), AnomalyError> {
        let _start = std::time::Instant::now();
        if values.len() < 3 {
            return Err(AnomalyError::InsufficientData {
                min: 3,
                got: values.len(),
            });
        }
        self.mean = simd_mean(values);
        self.std_dev = simd_variance(values, self.mean).sqrt();
        self.fitted = true;
        metrics::histogram!("chronix_anomaly_fit_duration_seconds", "method" => "zscore")
            .record(_start.elapsed().as_secs_f64());
        Ok(())
    }

    #[tracing::instrument(skip_all, level = "debug")]
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
        let mut scores = Vec::with_capacity(values.len());
        for (i, &v) in values.iter().enumerate() {
            let raw = self.score_value(v);
            let is_anomaly = raw > self.threshold;
            scores.push(AnomalyScore {
                timestamp: timestamps[i],
                value: v,
                score: Self::normalize(raw, self.threshold),
                is_anomaly,
                method: DetectorType::ZScore,
                threshold: self.threshold,
                details: format!("z={raw:.4}"),
            });
        }
        let anomaly_count = scores.iter().filter(|s| s.is_anomaly).count();
        metrics::counter!("chronix_anomaly_detected_total", "method" => "zscore")
            .increment(anomaly_count as u64);
        metrics::histogram!("chronix_anomaly_detect_duration_seconds", "method" => "zscore")
            .record(_start.elapsed().as_secs_f64());
        Ok(scores)
    }

    fn detect_point(&mut self, timestamp: i64, value: f64) -> Result<AnomalyScore, AnomalyError> {
        if !self.fitted {
            return Err(AnomalyError::NotFitted);
        }
        let raw = self.score_value(value);
        Ok(AnomalyScore {
            timestamp,
            value,
            score: Self::normalize(raw, self.threshold),
            is_anomaly: raw > self.threshold,
            method: DetectorType::ZScore,
            threshold: self.threshold,
            details: format!("z={raw:.4}"),
        })
    }

    fn detector_type(&self) -> DetectorType {
        DetectorType::ZScore
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_normal(n: usize, outlier_idx: &[usize], outlier_val: f64) -> (Vec<i64>, Vec<f64>) {
        let ts: Vec<i64> = (0..n as i64).map(|i| i * 1_000_000_000).collect();
        let mut vals: Vec<f64> = (0..n)
            .map(|i| {
                // Deterministic pseudo-normal via simple hash

                ((i as f64 * 0.1).sin() * 5.0) + 50.0
            })
            .collect();
        for &idx in outlier_idx {
            if idx < n {
                vals[idx] = outlier_val;
            }
        }
        (ts, vals)
    }

    #[test]
    fn outliers_flagged() {
        let (ts, vals) = make_normal(200, &[50, 150], 200.0);
        let mut det = ZScoreDetector::new(Some(3.0));
        det.fit(&ts, &vals).unwrap();
        let scores = det.detect(&ts, &vals).unwrap();
        assert!(scores[50].is_anomaly);
        assert!(scores[150].is_anomaly);
        // Normal points should mostly be non-anomalous
        let normals: usize = scores
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != 50 && *i != 150)
            .filter(|(_, s)| !s.is_anomaly)
            .count();
        assert!(normals > 180);
    }

    #[test]
    fn threshold_sensitivity() {
        let (ts, vals) = make_normal(200, &[100], 100.0);
        let mut loose = ZScoreDetector::new(Some(5.0));
        let mut tight = ZScoreDetector::new(Some(1.5));
        loose.fit(&ts, &vals).unwrap();
        tight.fit(&ts, &vals).unwrap();
        let s_loose = loose.detect(&ts, &vals).unwrap();
        let s_tight = tight.detect(&ts, &vals).unwrap();
        let count_loose: usize = s_loose.iter().filter(|s| s.is_anomaly).count();
        let count_tight: usize = s_tight.iter().filter(|s| s.is_anomaly).count();
        assert!(count_tight >= count_loose);
    }

    #[test]
    fn detect_point_streaming() {
        let (ts, vals) = make_normal(100, &[], 0.0);
        let mut det = ZScoreDetector::new(None);
        det.fit(&ts, &vals).unwrap();
        // Normal point
        let s = det.detect_point(999, 50.0).unwrap();
        assert!(!s.is_anomaly);
        // Outlier
        let s = det.detect_point(1000, 500.0).unwrap();
        assert!(s.is_anomaly);
    }

    #[test]
    fn insufficient_data() {
        let mut det = ZScoreDetector::new(None);
        assert!(det.fit(&[0, 1], &[1.0, 2.0]).is_err());
    }

    #[test]
    fn detect_mismatched_lengths_returns_error() {
        let (ts, vals) = make_normal(100, &[], 0.0);
        let mut det = ZScoreDetector::new(None);
        det.fit(&ts, &vals).unwrap();
        // timestamps shorter than values
        let result = det.detect(&ts[..50], &vals);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("timestamps length"), "error: {err}");
        // timestamps longer than values
        let result = det.detect(&ts, &vals[..50]);
        assert!(result.is_err());
    }
}
