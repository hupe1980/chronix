//! Dynamic Threshold anomaly detector.
//!
//! Uses a rolling window to compute adaptive mean and standard deviation
//! so the threshold follows changes in the data distribution.
//!
//! When a `period` is configured, the detector groups historical data by
//! time-of-day bucket (hour of day for period ≤ 24, or bucket index for
//! larger periods) and compares each new point against the statistics of
//! its matching bucket — making the detector seasonally aware.

use crate::anomaly::error::AnomalyError;
use crate::anomaly::traits::{validate_lengths, AnomalyDetector, AnomalyScore, DetectorType};

/// Anomaly detector with an adaptive rolling threshold.
///
/// Supports optional seasonal awareness via hour-of-day bucketing.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct DynamicThresholdDetector {
    k: f64,
    lookback: usize,
    /// Optional period (number of data points per cycle). When set,
    /// the detector uses per-bucket (hour-of-day) statistics.
    period: Option<usize>,
    // Fitted baseline stats (used for detect_point)
    baseline_mean: f64,
    baseline_std: f64,
    // Ring buffer for streaming
    ring: Vec<f64>,
    ring_sum: f64,
    ring_sum_sq: f64,
    ring_pos: usize,
    ring_count: usize,
    /// Seasonal phase offset: the number of training data points mod period.
    /// Used by `detect_point` to select the correct seasonal bucket.
    seasonal_phase: usize,
    // Seasonal buckets: per-bucket mean and std
    seasonal_means: Vec<f64>,
    seasonal_stds: Vec<f64>,
    fitted: bool,
    /// Number of points since last refit of the baseline.
    points_since_refit: usize,
    /// Refit the baseline from the ring buffer every N points.
    /// Default: 10 × lookback. 0 disables automatic refit.
    refit_interval: usize,
}

impl DynamicThresholdDetector {
    /// Creates a new detector.
    ///
    /// - `lookback` — rolling window size (default 100).
    /// - `k` — number of standard deviations for the threshold (default 3.0).
    ///
    /// A zero `lookback` is silently clamped to 1 to prevent modulo-by-zero.
    pub fn new(lookback: Option<usize>, k: Option<f64>) -> Self {
        let lb = lookback.unwrap_or(100).max(1);
        Self {
            k: k.unwrap_or(3.0),
            lookback: lb,
            period: None,
            baseline_mean: 0.0,
            baseline_std: 0.0,
            ring: vec![0.0; lb],
            ring_sum: 0.0,
            ring_sum_sq: 0.0,
            ring_pos: 0,
            ring_count: 0,
            seasonal_phase: 0,
            seasonal_means: Vec::new(),
            seasonal_stds: Vec::new(),
            fitted: false,
            points_since_refit: 0,
            refit_interval: lb * 10,
        }
    }

    /// Creates a new detector with seasonal awareness.
    ///
    /// - `lookback` — rolling window size (default 100).
    /// - `k` — number of standard deviations for the threshold (default 3.0).
    /// - `period` — number of data points per seasonal cycle (e.g. 24 for hourly
    ///   data with daily seasonality). When set, the detector computes
    ///   per-bucket statistics (same position within each cycle) and compares
    ///   new points against their matching bucket.
    pub fn with_period(lookback: Option<usize>, k: Option<f64>, period: usize) -> Self {
        let mut det = Self::new(lookback, k);
        det.period = Some(period);
        det
    }

    /// Build per-bucket seasonal statistics from training data.
    fn build_seasonal_stats(&mut self, values: &[f64]) {
        if let Some(period) = self.period {
            let mut bucket_sums = vec![0.0; period];
            let mut bucket_sum_sqs = vec![0.0; period];
            let mut bucket_counts = vec![0usize; period];

            for (i, &v) in values.iter().enumerate() {
                let b = i % period;
                bucket_sums[b] += v;
                bucket_sum_sqs[b] += v * v;
                bucket_counts[b] += 1;
            }

            self.seasonal_means = Vec::with_capacity(period);
            self.seasonal_stds = Vec::with_capacity(period);

            for b in 0..period {
                let n = bucket_counts[b] as f64;
                if n > 1.0 {
                    let mean = bucket_sums[b] / n;
                    let var = ((bucket_sum_sqs[b] - n * mean * mean) / (n - 1.0)).max(0.0);
                    self.seasonal_means.push(mean);
                    self.seasonal_stds.push(var.sqrt());
                } else if n > 0.0 {
                    let mean = bucket_sums[b] / n;
                    self.seasonal_means.push(mean);
                    self.seasonal_stds.push(0.0);
                } else {
                    self.seasonal_means.push(self.baseline_mean);
                    self.seasonal_stds.push(self.baseline_std);
                }
            }
        }
    }

    #[inline]
    fn normalize(raw: f64, threshold: f64) -> f64 {
        let x = (raw - threshold) * 2.0;
        1.0 / (1.0 + (-x).exp())
    }
}

impl AnomalyDetector for DynamicThresholdDetector {
    #[tracing::instrument(skip_all, level = "debug")]
    fn fit(&mut self, _timestamps: &[i64], values: &[f64]) -> Result<(), AnomalyError> {
        let _start = std::time::Instant::now();
        if values.len() < self.lookback {
            return Err(AnomalyError::InsufficientData {
                min: self.lookback,
                got: values.len(),
            });
        }

        // Compute baseline from full training data
        let n = values.len() as f64;
        let mean = values.iter().sum::<f64>() / n;
        let var = values.iter().map(|&v| (v - mean).powi(2)).sum::<f64>() / n;
        self.baseline_mean = mean;
        self.baseline_std = var.sqrt();

        // Initialise ring buffer with last `lookback` values
        self.ring = vec![0.0; self.lookback];
        self.ring_sum = 0.0;
        self.ring_sum_sq = 0.0;
        self.ring_pos = 0;
        self.ring_count = 0;
        let start = values.len().saturating_sub(self.lookback);
        for &v in &values[start..] {
            self.ring[self.ring_pos] = v;
            self.ring_sum += v;
            self.ring_sum_sq += v * v;
            self.ring_pos = (self.ring_pos + 1) % self.lookback;
            self.ring_count += 1;
        }
        // Build per-bucket seasonal statistics when period is configured
        self.build_seasonal_stats(values);

        // Track seasonal phase for detect_point's bucket selection
        if let Some(p) = self.period {
            self.seasonal_phase = values.len() % p;
        }

        self.fitted = true;
        metrics::histogram!("chronix_anomaly_fit_duration_seconds", "method" => "dynamic_threshold").record(_start.elapsed().as_secs_f64());
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

        let use_seasonal = self.period.is_some() && !self.seasonal_means.is_empty();
        let mut scores = Vec::with_capacity(values.len());
        let mut sum = 0.0;
        let mut sum_sq = 0.0;

        for (i, &v) in values.iter().enumerate() {
            // Determine reference mean/std: prefer per-bucket seasonal stats
            let (mean, std) = if use_seasonal {
                let b = self
                    .period
                    .map(|p| (self.seasonal_phase + i) % p)
                    .unwrap_or(0);
                (self.seasonal_means[b], self.seasonal_stds[b])
            } else {
                // Compute stats from the window BEFORE adding the current point,
                // so the point being scored does not dilute its own z-score.
                //
                // The rolling window only speaks once it is full. A partial
                // window is not a smaller window, it is a worse estimator:
                // with one prior sample the variance is *exactly* zero, so
                // the second point of every series scored `z = inf` and was
                // reported as an anomaly — on every series, always. The
                // sibling detector never had this because it scores against a
                // residual std fixed at fit time. Until the window
                // fills, the fitted baseline is the honest estimate.
                let count = i.min(self.lookback);
                let (m, var) = if count >= self.lookback {
                    let c = count as f64;
                    let m = sum / c;
                    let var = (sum_sq / c) - m * m;
                    (m, var)
                } else {
                    (self.baseline_mean, self.baseline_std * self.baseline_std)
                };
                // Now accumulate the current point for future iterations.
                sum += v;
                sum_sq += v * v;
                if i >= self.lookback {
                    sum -= values[i - self.lookback];
                    sum_sq -= values[i - self.lookback] * values[i - self.lookback];
                }
                // Periodic FP drift recomputation every 1024 steps.
                // Count-based strategy — see moving_average.rs for rationale.
                if i > 0 && i % 1024 == 0 {
                    let start = (i + 1).saturating_sub(self.lookback);
                    sum = values[start..=i].iter().sum();
                    sum_sq = values[start..=i].iter().map(|&x| x * x).sum();
                }
                (m, var.max(0.0).sqrt())
            };

            let raw = if std < 1e-15 {
                if (v - mean).abs() < 1e-15 {
                    0.0
                } else {
                    f64::INFINITY
                }
            } else {
                ((v - mean) / std).abs()
            };
            let is_anomaly = raw > self.k;
            scores.push(AnomalyScore {
                timestamp: timestamps[i],
                value: v,
                score: Self::normalize(raw, self.k),
                is_anomaly,
                method: DetectorType::DynamicThreshold,
                threshold: self.k,
                details: format!("rolling_mean={mean:.2} rolling_std={std:.4} z={raw:.4}"),
            });
        }
        let anomaly_count = scores.iter().filter(|s| s.is_anomaly).count();
        metrics::counter!("chronix_anomaly_detected_total", "method" => "dynamic_threshold")
            .increment(anomaly_count as u64);
        metrics::histogram!("chronix_anomaly_detect_duration_seconds", "method" => "dynamic_threshold").record(_start.elapsed().as_secs_f64());

        // Advance seasonal phase so consecutive detect() calls use the correct offset.
        if let Some(period) = self.period {
            if period > 0 {
                self.seasonal_phase = (self.seasonal_phase + values.len()) % period;
            }
        }

        Ok(scores)
    }

    fn detect_point(&mut self, timestamp: i64, value: f64) -> Result<AnomalyScore, AnomalyError> {
        if !self.fitted {
            return Err(AnomalyError::NotFitted);
        }

        // When seasonal period is configured, use per-bucket statistics
        let (mean, std) = if let Some(period) = self.period {
            if !self.seasonal_means.is_empty() {
                let b = self.seasonal_phase % period;
                (self.seasonal_means[b], self.seasonal_stds[b])
            } else {
                (self.baseline_mean, self.baseline_std)
            }
        } else {
            // Same rule as the batch path: a partial window is not an
            // estimator, it is a source of zero-variance z-scores.
            let count = self.ring_count.min(self.lookback);
            if count >= self.lookback {
                let c = count as f64;
                let m = self.ring_sum / c;
                let var = (self.ring_sum_sq / c) - m * m;
                (m, var.max(0.0).sqrt())
            } else {
                (self.baseline_mean, self.baseline_std)
            }
        };

        let raw = if std < 1e-15 {
            if (value - mean).abs() < 1e-15 {
                0.0
            } else {
                f64::INFINITY
            }
        } else {
            ((value - mean) / std).abs()
        };
        let score = AnomalyScore {
            timestamp,
            value,
            score: Self::normalize(raw, self.k),
            is_anomaly: raw > self.k,
            method: DetectorType::DynamicThreshold,
            threshold: self.k,
            details: format!("rolling_mean={mean:.2} rolling_std={std:.4} z={raw:.4}"),
        };

        // Advance seasonal phase for next streaming call.
        if let Some(period) = self.period {
            if period > 0 {
                self.seasonal_phase = (self.seasonal_phase + 1) % period;
            }
        }

        // Update ring buffer for non-seasonal rolling statistics.
        // Save old value BEFORE overwriting so eviction subtracts the correct entry.
        let evicted = self.ring[self.ring_pos];
        self.ring[self.ring_pos] = value;
        self.ring_pos = (self.ring_pos + 1) % self.lookback;
        self.ring_count += 1;
        self.ring_sum += value;
        self.ring_sum_sq += value * value;
        if self.ring_count > self.lookback {
            self.ring_sum -= evicted;
            self.ring_sum_sq -= evicted * evicted;
        }

        // Periodically refit the baseline from the ring buffer
        // to track concept drift. Without this, the baseline_mean/std
        // computed during initial fit() grows stale.
        self.points_since_refit += 1;
        if self.refit_interval > 0 && self.points_since_refit >= self.refit_interval {
            let count = self.ring_count.min(self.lookback) as f64;
            if count > 0.0 {
                self.baseline_mean = self.ring_sum / count;
                let var = (self.ring_sum_sq / count) - self.baseline_mean * self.baseline_mean;
                self.baseline_std = var.max(0.0).sqrt();
            }
            self.points_since_refit = 0;
        }

        Ok(score)
    }

    fn detector_type(&self) -> DetectorType {
        DetectorType::DynamicThreshold
    }
}

#[cfg(test)]
mod tests {

    /// A detector must not report an anomaly because its window is still
    /// filling.
    ///
    /// With one prior sample the rolling variance is exactly zero, so any
    /// second value that differs at all scored `z = inf` — every series, every
    /// time. It reached users: the bundled example printed
    /// `rolling_std=0.0000 z=inf` as its first detection on ordinary data,
    /// three points before anything unusual happened.
    #[test]
    fn a_filling_window_does_not_manufacture_anomalies() {
        let n = 120;
        let timestamps: Vec<i64> = (0..n).map(|i| i as i64 * 1_000_000_000).collect();
        // Smooth, entirely unremarkable data with one genuine spike at the end.
        let mut values: Vec<f64> = (0..n)
            .map(|i| 20.0 + ((i as f64) * 0.05).sin() * 0.5)
            .collect();
        values[100] = 200.0;

        let mut d = DynamicThresholdDetector::new(Some(30), Some(3.0));
        d.fit(&timestamps, &values).unwrap();
        let scores = d.detect(&timestamps, &values).unwrap();

        for s in &scores {
            assert!(
                s.score.is_finite(),
                "score must be finite, got {} at ts={} ({})",
                s.score,
                s.timestamp,
                s.details
            );
            assert!(
                !s.details.contains("z=inf"),
                "a z-score of infinity reached the caller: {}",
                s.details
            );
        }

        let flagged: Vec<i64> = scores
            .iter()
            .filter(|s| s.is_anomaly)
            .map(|s| s.timestamp)
            .collect();
        assert_eq!(
            flagged,
            vec![timestamps[100]],
            "only the injected spike is anomalous"
        );
    }
    use super::*;

    #[test]
    fn adapts_to_changing_baseline() {
        // Data: 100 points at ~50, then 100 points at ~150
        let ts: Vec<i64> = (0..200).map(|i| i * 1_000_000_000).collect();
        let mut vals: Vec<f64> = vec![50.0; 100];
        vals.extend(vec![150.0; 100]);

        let mut det = DynamicThresholdDetector::new(Some(50), Some(3.0));
        det.fit(&ts, &vals).unwrap();
        let scores = det.detect(&ts, &vals).unwrap();

        // After adaptation, the later stable region should NOT be anomalous
        let late_anomalies = scores[150..200].iter().filter(|s| s.is_anomaly).count();
        assert!(
            late_anomalies < 5,
            "after adaptation, late anomalies should be low: {late_anomalies}"
        );
    }

    #[test]
    fn detect_point_spike() {
        let ts: Vec<i64> = (0..200).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = vec![50.0; 200];
        let mut det = DynamicThresholdDetector::new(Some(100), None);
        det.fit(&ts, &vals).unwrap();
        let normal = det.detect_point(999, 50.0).unwrap();
        assert!(!normal.is_anomaly);
        let spike = det.detect_point(1000, 500.0).unwrap();
        assert!(spike.is_anomaly);
    }

    #[test]
    fn insufficient_data() {
        let mut det = DynamicThresholdDetector::new(Some(100), None);
        let ts: Vec<i64> = (0..50).collect();
        let vals: Vec<f64> = vec![1.0; 50];
        assert!(det.fit(&ts, &vals).is_err());
    }

    #[test]
    fn seasonal_awareness() {
        // Create data with strong daily seasonality: period = 24
        // Even-bucket values ≈ 10, odd-bucket values ≈ 100
        let period = 24;
        let n_cycles = 10; // 10 full cycles for training
        let n = period * n_cycles;
        let ts: Vec<i64> = (0..n as i64).collect();
        let vals: Vec<f64> = (0..n)
            .map(|i| if i % period < 12 { 10.0 } else { 100.0 })
            .collect();

        let mut det = DynamicThresholdDetector::with_period(Some(50), Some(3.0), period);
        det.fit(&ts, &vals).unwrap();

        // In-season values should NOT be anomalous regardless of position
        let test_ts: Vec<i64> = (n as i64..n as i64 + period as i64).collect();
        let test_vals: Vec<f64> = (0..period)
            .map(|i| if i < 12 { 10.0 } else { 100.0 })
            .collect();
        let scores = det.detect(&test_ts, &test_vals).unwrap();
        let false_positives = scores.iter().filter(|s| s.is_anomaly).count();
        assert_eq!(
            false_positives, 0,
            "seasonally normal values should not be flagged: got {false_positives}"
        );

        // Out-of-season value in a low bucket should be anomalous
        let spike = det.detect_point(9999, 500.0).unwrap();
        assert!(spike.is_anomaly, "out-of-season spike should be flagged");
    }

    #[test]
    fn ring_buffer_rolling_stats_converge() {
        // Verify that detect_point's ring buffer rolling mean converges to the
        // streaming values, not to a frozen initial state (regression test for
        // Bug 1 Session 13 — eviction read-after-write).
        let ts: Vec<i64> = (0..200).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = vec![50.0; 200];
        let mut det = DynamicThresholdDetector::new(Some(20), Some(3.0));
        det.fit(&ts, &vals).unwrap();

        // Feed 50 points at value=50 to fill the ring buffer
        for i in 0..50 {
            det.detect_point(200 + i, 50.0).unwrap();
        }

        // Now shift to value=200 for 50 more points
        for i in 0..50 {
            det.detect_point(250 + i, 200.0).unwrap();
        }

        // After 50 points at 200 (with lookback=20), the rolling mean should
        // have fully converged to ~200. A value of 200 should not be anomalous.
        let score = det.detect_point(300, 200.0).unwrap();
        assert!(
            !score.is_anomaly,
            "after 50 points at 200 with lookback=20, 200.0 should be normal, \
             but got score={:.3} is_anomaly={}",
            score.score, score.is_anomaly
        );
    }

    #[test]
    fn seasonal_bucket_bessel_correction() {
        // period=4, 25 full cycles → 25 observations per bucket
        // Bucket 0 values alternate: 10, 20, 30, 10, 20, 30, ...
        // With 25 values: 9×10 + 8×20 + 8×30 = 90 + 160 + 240 = 490, mean = 19.6
        // Compute expected Bessel-corrected std manually below.
        //
        // Simpler: use 3 full cycles (12 values) with lookback=12
        // bucket 0: [10, 20, 30] → mean=20, sample_var = 100, std=10
        let period = 4;
        let lookback = 12;
        let mut det = DynamicThresholdDetector::with_period(Some(lookback), Some(3.0), period);
        // 3 complete cycles, exactly 12 values — no leftover padding in bucket 0
        let values = vec![
            10.0, 50.0, 50.0, 50.0, // cycle 0: bucket0=10
            20.0, 50.0, 50.0, 50.0, // cycle 1: bucket0=20
            30.0, 50.0, 50.0, 50.0, // cycle 2: bucket0=30
        ];
        let ts: Vec<i64> = (0..values.len() as i64).collect();
        det.fit(&ts, &values).unwrap();

        // Bessel-corrected std for bucket 0: sqrt(100) = 10.0
        let bucket0_std = det.seasonal_stds[0];
        assert!(
            (bucket0_std - 10.0).abs() < 1e-9,
            "expected Bessel-corrected std ~10.0, got {bucket0_std:.6}"
        );
    }
}
