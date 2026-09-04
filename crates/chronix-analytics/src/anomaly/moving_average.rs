//! Moving Average Residual anomaly detector.
//!
//! Computes residuals against a **causal** sliding moving average — the mean
//! of the `window_size` points *preceding* the one being scored — and flags
//! points whose residual Z-Score exceeds the threshold.
//!
//! # One residual definition
//!
//! Excluding the scored point matters: for a window of `w`, including it
//! shrinks the residual by a factor of `(w-1)/w` (25 % at `w = 5`), so a
//! batch definition that includes it and a streaming definition that excludes
//! it disagree on the z-score of the same point by that factor and flag
//! different points. The causal form is the only one a streaming detector can
//! compute, so it is the one both paths use, and [`detect`] is literally a
//! fold of [`detect_point`].
//!
//! [`detect`]: AnomalyDetector::detect
//! [`detect_point`]: AnomalyDetector::detect_point

use crate::anomaly::error::AnomalyError;
use crate::anomaly::traits::{
    scale_floor, validate_lengths, AnomalyDetector, AnomalyScore, DetectorType,
};

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
    /// Pushes since `ring_sum` was last recomputed from the ring itself.
    since_recompute: u32,
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
            since_recompute: 0,
            fitted: false,
        }
    }

    /// Causal moving-average residuals: `r_i = v_i − mean(v_{i−w..i})`, the
    /// average of the `w` points **before** `i`.
    ///
    /// Returns `values.len() − 1` residuals, for `i = 1..n`. `v_0` has no
    /// preceding window and therefore no residual at all; reporting it as
    /// `0.0` — which is what a window that includes the scored point does at
    /// `i = 0` — puts a fabricated exact zero into the calibration sample,
    /// pulling the fitted mean toward zero and shrinking the fitted spread.
    fn causal_ma_residuals(values: &[f64], window: usize) -> Vec<f64> {
        let mut residuals = Vec::with_capacity(values.len().saturating_sub(1));
        let mut sum = 0.0;
        for (i, &v) in values.iter().enumerate() {
            if i > 0 {
                let count = i.min(window);
                residuals.push(v - sum / count as f64);
            }
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
        }
        residuals
    }

    /// Empty the rolling window, so the next observation starts a stream.
    fn reset_window(&mut self) {
        self.ring = vec![0.0; self.window_size];
        self.ring_sum = 0.0;
        self.ring_pos = 0;
        self.ring_count = 0;
        self.since_recompute = 0;
    }

    /// Push one observation into the rolling window, evicting the oldest.
    fn push(&mut self, value: f64) {
        if self.ring_count >= self.window_size {
            self.ring_sum -= self.ring[self.ring_pos];
        } else {
            self.ring_count += 1;
        }
        self.ring[self.ring_pos] = value;
        self.ring_sum += value;
        self.ring_pos = (self.ring_pos + 1) % self.window_size;

        // Same count-based drift recomputation as the batch path.
        self.since_recompute += 1;
        if self.since_recompute >= 1024 {
            self.since_recompute = 0;
            self.ring_sum = self.ring[..self.ring_count.min(self.window_size)]
                .iter()
                .sum();
        }
    }

    /// Absolute z-score of one residual. σ is floored relative to the
    /// residual mean (see [`scale_floor`]) so a constant training window
    /// scores float noise near 0 instead of at `inf`.
    #[inline]
    fn score_residual(&self, residual: f64) -> f64 {
        ((residual - self.residual_mean) / scale_floor(self.residual_std, self.residual_mean)).abs()
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

        // Calibrate on full-window residuals only. The first `window − 1`
        // are computed against a partial window, so they are systematically
        // larger in absolute terms and are not comparable with the ones the
        // detector will score. Including them made a pure linear drift look
        // anomalous: the residual of a ramp is constant once the window is
        // full, so the whole apparent spread came from the warm-up, and
        // every steady-state point then sat several of those "sigmas" from
        // the warm-up mean.
        let all = Self::causal_ma_residuals(values, self.window_size);
        let warm_up = self.window_size.saturating_sub(1).min(all.len());
        let residuals = if all.len() - warm_up >= 2 {
            &all[warm_up..]
        } else {
            &all[..]
        };
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

        // Seed the streaming window with the training tail, so a
        // `detect_point` that continues the fitted series has a full window.
        self.reset_window();
        let start = values.len().saturating_sub(self.window_size);
        for &v in &values[start..] {
            self.push(v);
        }
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
        // The first `window_size` points of the batch come back as
        // "warm-up" and are never anomalies; see `detect_point`.
        //
        // A batch is scored as **its own** causal stream: the rolling
        // window is reset and warmed from the batch's leading points, so
        // point `i` is compared with the points before it in this batch.
        //
        // Without the reset the window carried over from `fit`, so
        // re-scoring the training data compared its first points with the
        // *end* of the series — on any trending series that is a residual
        // the size of the whole trend, and every early point came back an
        // anomaly. `detect` remains a fold of `detect_point`; the reset is
        // what makes the fold start where the batch does.
        self.reset_window();
        let mut scores = Vec::with_capacity(values.len());
        for (i, &v) in values.iter().enumerate() {
            scores.push(self.detect_point(timestamps[i], v)?);
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
        // A point whose window is not yet full cannot be judged: its
        // residual is computed against fewer neighbours than every residual
        // the detector was calibrated on, so it is systematically smaller
        // and comparing the two is a units error. The detector says
        // "warming up" instead of inventing a verdict — which is what it
        // used to do, flagging the first `window` points of every stream
        // whose level was not flat.
        if count < self.window_size {
            self.push(value);
            return Ok(AnomalyScore {
                timestamp,
                value,
                score: 0.0,
                is_anomaly: false,
                method: DetectorType::MovingAverageResidual,
                threshold: self.threshold,
                details: format!("warm-up {count}/{}", self.window_size),
            });
        }
        let residual = value - self.ring_sum / count as f64;
        let raw = self.score_residual(residual);
        // The window has to *move*. Without this the "moving" average stayed
        // frozen at the last training window forever, so a stream that drifted
        // away from its training level scored a residual that grew without
        // bound and every point after the drift was an anomaly.
        self.push(value);
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
