//! Moving Average Residual anomaly detector.
//!
//! Computes residuals against a sliding moving average and flags
//! points whose residual Z-Score exceeds the threshold.

use crate::anomaly::error::AnomalyError;
use crate::anomaly::traits::{validate_lengths, AnomalyDetector, AnomalyScore, DetectorType};

/// Detector that compares values against their moving average.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct MovingAverageResidualDetector {
    window_size: usize,
    threshold: f64,
    // Fitted residual stats
    residual_mean: f64,
    residual_std: f64,
    // Ring buffer for streaming
    ring: Vec<f64>,
    ring_sum: f64,
    ring_pos: usize,
    ring_count: usize,
    fitted: bool,
}

impl MovingAverageResidualDetector {
    /// Creates a new detector.  `window_size` defaults to 24, `threshold` to 3.0.
    ///
    /// A zero `window_size` is silently clamped to 1 to prevent modulo-by-zero.
    pub fn new(window_size: Option<usize>, threshold: Option<f64>) -> Self {
        let ws = window_size.unwrap_or(24).max(1);
        Self {
            window_size: ws,
            threshold: threshold.unwrap_or(3.0),
            residual_mean: 0.0,
            residual_std: 0.0,
            ring: vec![0.0; ws],
            ring_sum: 0.0,
            ring_pos: 0,
            ring_count: 0,
            fitted: false,
        }
    }

    fn compute_ma_residuals(values: &[f64], window: usize) -> Vec<f64> {
        let mut residuals = Vec::with_capacity(values.len());
        let mut sum = 0.0;
        for (i, &v) in values.iter().enumerate() {
            sum += v;
            if i >= window {
                sum -= values[i - window];
            }
            // Periodic FP drift recomputation every 1024 steps.
            //
            // This is a **count-based** recomputation strategy rather than
            // an error-based one (e.g. Kahan summation or checking the
            // accumulated drift magnitude). The fixed 1024-step interval
            // is chosen for simplicity and predictable overhead:
            //   Keeps worst-case relative error below ~1e-12 for typical
            //     time-series magnitudes.
            //   Matches the strategy in RollingCorrelation and
            //     RollingStatExpr for consistency across the codebase.
            //   Avoids the per-step branch cost of error-based approaches.
            if i > 0 && i % 1024 == 0 {
                let start = (i + 1).saturating_sub(window);
                sum = values[start..=i].iter().sum();
            }
            let count = (i + 1).min(window);
            let ma = sum / count as f64;
            residuals.push(v - ma);
        }
        residuals
    }

    #[inline]
    fn score_residual(&self, residual: f64) -> f64 {
        if self.residual_std < 1e-15 {
            // Residual std is zero: training residuals were constant.
            // Any non-zero deviation is highly anomalous.
            if (residual - self.residual_mean).abs() < 1e-15 {
                return 0.0;
            }
            return f64::INFINITY;
        }
        ((residual - self.residual_mean) / self.residual_std).abs()
    }

    #[inline]
    fn normalize(raw: f64, threshold: f64) -> f64 {
        let x = (raw - threshold) * 2.0;
        1.0 / (1.0 + (-x).exp())
    }
}

impl AnomalyDetector for MovingAverageResidualDetector {
    fn fit(&mut self, _timestamps: &[i64], values: &[f64]) -> Result<(), AnomalyError> {
        let _start = std::time::Instant::now();
        if values.len() < self.window_size {
            return Err(AnomalyError::InsufficientData {
                min: self.window_size,
                got: values.len(),
            });
        }

        let residuals = Self::compute_ma_residuals(values, self.window_size);
        let n = residuals.len() as f64;
        self.residual_mean = residuals.iter().sum::<f64>() / n;
        // Use Bessel's correction (n-1) for consistency with simd_std_dev
        // and features::zscore across the codebase.
        let denom = if n > 1.0 { n - 1.0 } else { n };
        self.residual_std = (residuals
            .iter()
            .map(|&r| (r - self.residual_mean).powi(2))
            .sum::<f64>()
            / denom)
            .sqrt();

        // Initialize ring buffer with last `window_size` values
        self.ring = vec![0.0; self.window_size];
        self.ring_sum = 0.0;
        self.ring_pos = 0;
        let start = values.len().saturating_sub(self.window_size);
        for &v in &values[start..] {
            self.ring[self.ring_pos] = v;
            self.ring_sum += v;
            self.ring_pos = (self.ring_pos + 1) % self.window_size;
        }
        self.ring_count = values[start..].len();
        self.fitted = true;
        metrics::histogram!("chronix_anomaly_fit_duration_seconds", "method" => "moving_average")
            .record(_start.elapsed().as_secs_f64());
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
        let residuals = Self::compute_ma_residuals(values, self.window_size);
        let mut scores = Vec::with_capacity(values.len());
        for (i, (&v, &r)) in values.iter().zip(residuals.iter()).enumerate() {
            let raw = self.score_residual(r);
            let is_anomaly = raw > self.threshold;
            scores.push(AnomalyScore {
                timestamp: timestamps[i],
                value: v,
                score: Self::normalize(raw, self.threshold),
                is_anomaly,
                method: DetectorType::MovingAverageResidual,
                threshold: self.threshold,
                details: format!("residual={r:.4} z={raw:.4}"),
            });
        }
        let anomaly_count = scores.iter().filter(|s| s.is_anomaly).count();
        metrics::counter!("chronix_anomaly_detected_total", "method" => "moving_average")
            .increment(anomaly_count as u64);
        metrics::histogram!("chronix_anomaly_detect_duration_seconds", "method" => "moving_average").record(_start.elapsed().as_secs_f64());
        Ok(scores)
    }

    fn detect_point(&mut self, timestamp: i64, value: f64) -> Result<AnomalyScore, AnomalyError> {
        if !self.fitted {
            return Err(AnomalyError::NotFitted);
        }
        let count = self.ring_count.min(self.window_size);
        let ma = if count > 0 {
            self.ring_sum / count as f64
        } else {
            value
        };
        let residual = value - ma;
        let raw = self.score_residual(residual);
        Ok(AnomalyScore {
            timestamp,
            value,
            score: Self::normalize(raw, self.threshold),
            is_anomaly: raw > self.threshold,
            method: DetectorType::MovingAverageResidual,
            threshold: self.threshold,
            details: format!("residual={residual:.4} z={raw:.4}"),
        })
    }

    fn detector_type(&self) -> DetectorType {
        DetectorType::MovingAverageResidual
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_sudden_shift() {
        let ts: Vec<i64> = (0..200).map(|i| i * 1_000_000_000).collect();
        let mut vals: Vec<f64> = vec![50.0; 200];
        // Sudden shift at point 100
        vals[100] = 200.0;

        let mut det = MovingAverageResidualDetector::new(Some(24), Some(3.0));
        det.fit(&ts, &vals).unwrap();
        let scores = det.detect(&ts, &vals).unwrap();
        assert!(scores[100].is_anomaly);
    }

    #[test]
    fn gradual_drift_adapts() {
        let ts: Vec<i64> = (0..200).map(|i| i * 1_000_000_000).collect();
        // Gradual linear drift
        let vals: Vec<f64> = (0..200).map(|i| 50.0 + i as f64 * 0.1).collect();
        let mut det = MovingAverageResidualDetector::new(Some(24), Some(3.0));
        det.fit(&ts, &vals).unwrap();
        let scores = det.detect(&ts, &vals).unwrap();
        let anomaly_count = scores.iter().filter(|s| s.is_anomaly).count();
        // Gradual drift should not produce many anomalies
        assert!(
            anomaly_count < 10,
            "too many false positives: {anomaly_count}"
        );
    }

    #[test]
    fn streaming_detect_point() {
        let ts: Vec<i64> = (0..100).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = vec![50.0; 100];
        let mut det = MovingAverageResidualDetector::new(Some(10), None);
        det.fit(&ts, &vals).unwrap();
        let normal = det.detect_point(999, 50.0).unwrap();
        assert!(!normal.is_anomaly);
        let spike = det.detect_point(1000, 500.0).unwrap();
        assert!(spike.is_anomaly);
    }

    #[test]
    fn insufficient_data() {
        let mut det = MovingAverageResidualDetector::new(Some(24), None);
        let ts: Vec<i64> = (0..10).collect();
        let vals: Vec<f64> = vec![1.0; 10];
        assert!(det.fit(&ts, &vals).is_err());
    }
}
