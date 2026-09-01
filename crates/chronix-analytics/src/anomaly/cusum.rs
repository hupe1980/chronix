//! CUSUM (Cumulative Sum) change-point detector.
//!
//! Implements the tabular (two-sided) CUSUM algorithm for detecting
//! shifts in the mean of a sequential process.  Maintains running
//! cumulative sums for positive and negative shifts; a change point
//! is flagged when either sum exceeds a decision threshold `h`.
//!
//! Reference: Page, E. S. (1954). "Continuous inspection schemes."
//! Biometrika, 41(1/2), 100-115.
//!
//! # Algorithm
//!
//! Given a reference (target) mean μ₀ and a minimum detectable shift
//! magnitude `k` (slack value):
//!
//! ```text
//! S⁺ₜ = max(0, S⁺ₜ₋₁ + (xₜ - μ₀) - k)
//! S⁻ₜ = max(0, S⁻ₜ₋₁ - (xₜ - μ₀) - k)
//! ```
//!
//! A change point is signalled when `S⁺ₜ > h` (upward shift) or
//! `S⁻ₜ > h` (downward shift).

use crate::compute::simd_mean;

use crate::anomaly::error::AnomalyError;
use crate::anomaly::traits::{validate_lengths, AnomalyDetector, AnomalyScore, DetectorType};

/// Two-sided tabular CUSUM detector.
///
/// Detects shifts in the mean of a time series by accumulating
/// deviations from a reference mean.  Suitable for streaming
/// (O(1) per point after fitting).
#[derive(serde::Serialize, serde::Deserialize)]
pub struct CusumDetector {
    /// Decision threshold — triggers alarm when cumulative sum exceeds this.
    h: f64,
    /// Slack (allowance) value — minimum shift magnitude to detect.
    /// Commonly set to half the expected shift: k = δ/2.
    k: f64,
    /// Reference mean (μ₀), estimated during fit.
    target_mean: f64,
    /// Reference std dev, used for score normalization.
    target_std: f64,
    /// Running positive cumulative sum.
    s_pos: f64,
    /// Running negative cumulative sum.
    s_neg: f64,
    /// Whether to reset cumulative sums after an alarm.
    ///
    /// - `true`  (default): Western Electric variant — resets S⁺/S⁻ to 0
    ///   after each alarm, making CUSUM re-arm for the next independent shift.
    /// - `false`: Page (1954) standard — cumulative sums continue growing,
    ///   capturing sustained or multi-step shifts.
    reset_after_alarm: bool,
    /// Whether the detector has been fitted.
    fitted: bool,
}

impl CusumDetector {
    /// Creates a new CUSUM detector.
    ///
    /// - `h`: Decision threshold (default: 4.0 × σ). Higher = fewer false alarms.
    /// - `k`: Slack parameter (default: 0.5 × σ). The minimum shift to detect.
    pub fn new(h: Option<f64>, k: Option<f64>) -> Self {
        Self {
            h: h.unwrap_or(0.0), // will be set from data if 0
            k: k.unwrap_or(0.0), // will be set from data if 0
            target_mean: 0.0,
            target_std: 0.0,
            s_pos: 0.0,
            s_neg: 0.0,
            reset_after_alarm: true,
            fitted: false,
        }
    }

    /// Set whether cumulative sums are reset after each alarm.
    ///
    /// `true` = Western Electric variant (default); `false` = Page (1954).
    #[must_use]
    pub fn with_reset_after_alarm(mut self, reset: bool) -> Self {
        self.reset_after_alarm = reset;
        self
    }

    /// Normalize the raw CUSUM statistic into [0, 1] using a sigmoid.
    #[inline]
    fn normalize(raw: f64, h: f64) -> f64 {
        let x = (raw / h.max(1e-15) - 1.0) * 4.0;
        1.0 / (1.0 + (-x).exp())
    }
}

impl AnomalyDetector for CusumDetector {
    fn fit(&mut self, _timestamps: &[i64], values: &[f64]) -> Result<(), AnomalyError> {
        if values.len() < 10 {
            return Err(AnomalyError::InsufficientData {
                min: 10,
                got: values.len(),
            });
        }
        let _start = std::time::Instant::now();

        self.target_mean = simd_mean(values);

        // Compute std dev
        let variance = values
            .iter()
            .map(|&v| (v - self.target_mean).powi(2))
            .sum::<f64>()
            / (values.len() - 1) as f64;
        self.target_std = variance.sqrt().max(1e-15);

        // Default h = 4σ (common choice for ARL₀ ≈ 170)
        // Floor values prevent underflow when σ ≈ 0
        // (constant training data). Without floors, h ≈ 0 causes
        // every subsequent point to trigger a false alarm.
        if self.h <= 0.0 {
            self.h = (4.0 * self.target_std).max(1e-3);
        }
        // Default k = 0.5σ (detects ~1σ shifts optimally)
        if self.k <= 0.0 {
            self.k = (0.5 * self.target_std).max(1e-4);
        }

        // Reset cumulative sums
        self.s_pos = 0.0;
        self.s_neg = 0.0;
        self.fitted = true;

        metrics::histogram!("chronix_anomaly_fit_duration_seconds", "method" => "cusum")
            .record(_start.elapsed().as_secs_f64());
        Ok(())
    }

    fn detect(
        &mut self,
        timestamps: &[i64],
        values: &[f64],
    ) -> Result<Vec<AnomalyScore>, AnomalyError> {
        validate_lengths(timestamps, values)?;
        if !self.fitted {
            return Err(AnomalyError::NotFitted);
        }
        let _start = std::time::Instant::now();

        // Reset cumulative sums for batch detection
        self.s_pos = 0.0;
        self.s_neg = 0.0;

        let mut scores = Vec::with_capacity(values.len());
        for (i, &v) in values.iter().enumerate() {
            let deviation = v - self.target_mean;
            self.s_pos = (self.s_pos + deviation - self.k).max(0.0);
            self.s_neg = (self.s_neg - deviation - self.k).max(0.0);

            let raw = self.s_pos.max(self.s_neg);
            let is_anomaly = raw > self.h;

            let direction = if self.s_pos > self.s_neg {
                "upward"
            } else {
                "downward"
            };

            scores.push(AnomalyScore {
                timestamp: timestamps[i],
                value: v,
                score: Self::normalize(raw, self.h),
                is_anomaly,
                method: DetectorType::Cusum,
                threshold: self.h,
                details: format!(
                    "S+={:.4} S-={:.4} shift={direction}",
                    self.s_pos, self.s_neg
                ),
            });

            // Configurable reset — Western Electric (default) or Page.
            if is_anomaly && self.reset_after_alarm {
                self.s_pos = 0.0;
                self.s_neg = 0.0;
            }
        }

        let anomaly_count = scores.iter().filter(|s| s.is_anomaly).count();
        metrics::counter!("chronix_anomaly_detected_total", "method" => "cusum")
            .increment(anomaly_count as u64);
        metrics::histogram!("chronix_anomaly_detect_duration_seconds", "method" => "cusum")
            .record(_start.elapsed().as_secs_f64());
        Ok(scores)
    }

    fn detect_point(&mut self, timestamp: i64, value: f64) -> Result<AnomalyScore, AnomalyError> {
        if !self.fitted {
            return Err(AnomalyError::NotFitted);
        }

        let deviation = value - self.target_mean;
        self.s_pos = (self.s_pos + deviation - self.k).max(0.0);
        self.s_neg = (self.s_neg - deviation - self.k).max(0.0);

        let raw = self.s_pos.max(self.s_neg);
        let is_anomaly = raw > self.h;

        let direction = if self.s_pos > self.s_neg {
            "upward"
        } else {
            "downward"
        };

        let score = AnomalyScore {
            timestamp,
            value,
            score: Self::normalize(raw, self.h),
            is_anomaly,
            method: DetectorType::Cusum,
            threshold: self.h,
            details: format!(
                "S+={:.4} S-={:.4} shift={direction}",
                self.s_pos, self.s_neg
            ),
        };

        // Configurable reset.
        if is_anomaly && self.reset_after_alarm {
            self.s_pos = 0.0;
            self.s_neg = 0.0;
        }

        Ok(score)
    }

    fn detector_type(&self) -> DetectorType {
        DetectorType::Cusum
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic pseudo-random noise in [-amplitude, amplitude].
    fn noise(i: usize, amplitude: f64) -> f64 {
        let mut z = (i as u64)
            .wrapping_mul(0x9e3779b97f4a7c15)
            .wrapping_add(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z = z ^ (z >> 31);
        let u = (z as f64) / (u64::MAX as f64) * 2.0 - 1.0;
        u * amplitude
    }

    #[test]
    fn cusum_detects_mean_shift() {
        // Use explicit h/k so detection is independent of noise distribution.
        // h=5.0, k=1.0 → requires cumulative deviation > 5.0 to alarm.
        let n = 200usize;
        let ts: Vec<i64> = (0..n).map(|i| i as i64 * 1_000_000_000).collect();
        let mut vals: Vec<f64> = Vec::with_capacity(n);
        for i in 0..n {
            if i < 100 {
                vals.push(100.0 + noise(i, 0.5));
            } else {
                vals.push(115.0 + noise(i, 0.5)); // shifted up by 15
            }
        }

        let mut det = CusumDetector::new(Some(5.0), Some(1.0));
        det.fit(&ts[..100], &vals[..100]).unwrap();

        let scores = det.detect(&ts, &vals).unwrap();
        assert_eq!(scores.len(), n);

        // Stable portion: noise is ±0.5, k=1.0 → deviation-k < 0 → S stays 0
        let anomalies_first_half: Vec<_> = scores[..100].iter().filter(|s| s.is_anomaly).collect();
        assert!(
            anomalies_first_half.is_empty(),
            "no anomalies expected in stable portion, got {}",
            anomalies_first_half.len()
        );

        // Shifted portion: deviation ≈15, 15-1=14 per step → triggers in 1 step
        let anomalies_second_half: Vec<_> = scores[100..].iter().filter(|s| s.is_anomaly).collect();
        assert!(
            !anomalies_second_half.is_empty(),
            "expected at least one change point after mean shift"
        );
    }

    #[test]
    fn cusum_detects_downward_shift() {
        let n = 200usize;
        let ts: Vec<i64> = (0..n).map(|i| i as i64 * 1_000_000_000).collect();
        let mut vals: Vec<f64> = Vec::with_capacity(n);
        for i in 0..n {
            if i < 100 {
                vals.push(100.0 + noise(i, 0.5));
            } else {
                vals.push(85.0 + noise(i, 0.5)); // dropped by 15
            }
        }

        let mut det = CusumDetector::new(Some(5.0), Some(1.0));
        det.fit(&ts[..100], &vals[..100]).unwrap();

        let scores = det.detect(&ts, &vals).unwrap();
        let second_half_anomalies: Vec<_> = scores[100..].iter().filter(|s| s.is_anomaly).collect();
        assert!(
            !second_half_anomalies.is_empty(),
            "expected downward shift detection"
        );
        assert!(
            second_half_anomalies
                .iter()
                .any(|s| s.details.contains("downward")),
            "expected downward direction label"
        );
    }

    #[test]
    fn cusum_stable_no_false_alarms() {
        // Stationary data with noise ±0.5, explicit k=1.0 → all deviations
        // are below k, so CUSUM stays at 0.
        let n = 200usize;
        let ts: Vec<i64> = (0..n).map(|i| i as i64 * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..n).map(|i| 50.0 + noise(i, 0.5)).collect();

        let mut det = CusumDetector::new(Some(5.0), Some(1.0));
        det.fit(&ts, &vals).unwrap();

        let scores = det.detect(&ts, &vals).unwrap();
        let anomalies: Vec<_> = scores.iter().filter(|s| s.is_anomaly).collect();
        assert!(
            anomalies.is_empty(),
            "no anomalies expected on stationary data, got {}",
            anomalies.len()
        );
    }

    #[test]
    fn cusum_streaming_detect_point() {
        let n = 200usize;
        let ts: Vec<i64> = (0..n).map(|i| i as i64 * 1_000_000_000).collect();
        let mut vals: Vec<f64> = Vec::with_capacity(n);
        for i in 0..n {
            if i < 100 {
                vals.push(50.0 + noise(i, 0.3));
            } else {
                vals.push(65.0 + noise(i, 0.3)); // +15 shift
            }
        }
        let mut det = CusumDetector::new(Some(5.0), Some(1.0));
        det.fit(&ts[..100], &vals[..100]).unwrap();

        let mut detected_any = false;
        for (i, &v) in vals.iter().enumerate() {
            let score = det.detect_point(ts[i], v).unwrap();
            if i >= 100 && score.is_anomaly {
                detected_any = true;
            }
        }
        assert!(detected_any, "streaming detect should catch the shift");
    }

    #[test]
    fn cusum_insufficient_data() {
        let mut det = CusumDetector::new(None, None);
        let result = det.fit(&[1, 2, 3], &[1.0, 2.0, 3.0]);
        assert!(result.is_err());
    }

    #[test]
    fn cusum_custom_thresholds() {
        let det = CusumDetector::new(Some(10.0), Some(2.0));
        assert_eq!(det.h, 10.0);
        assert_eq!(det.k, 2.0);
    }

    #[test]
    fn cusum_not_fitted_errors() {
        let mut det = CusumDetector::new(None, None);
        let result = det.detect(&[1], &[1.0]);
        assert!(matches!(result, Err(AnomalyError::NotFitted)));
        let result = det.detect_point(1, 1.0);
        assert!(matches!(result, Err(AnomalyError::NotFitted)));
    }

    #[test]
    fn cusum_score_in_unit_range() {
        let n = 200usize;
        let ts: Vec<i64> = (0..n).map(|i| i as i64 * 1_000_000_000).collect();
        let mut vals: Vec<f64> = Vec::with_capacity(n);
        for i in 0..n {
            if i < 100 {
                vals.push(50.0 + noise(i, 0.3));
            } else {
                vals.push(65.0 + noise(i, 0.3));
            }
        }

        let mut det = CusumDetector::new(None, None);
        det.fit(&ts[..100], &vals[..100]).unwrap();

        let scores = det.detect(&ts, &vals).unwrap();
        for s in &scores {
            assert!(
                (0.0..=1.0).contains(&s.score),
                "score {} out of [0,1] range",
                s.score
            );
        }
    }
}
