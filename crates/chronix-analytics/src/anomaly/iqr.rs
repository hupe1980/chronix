//! IQR (Interquartile Range) anomaly detector.

use crate::anomaly::error::AnomalyError;
use crate::anomaly::traits::{
    scale_floor, validate_lengths, AnomalyDetector, AnomalyScore, DetectorType,
};

/// Flags points outside the fences Q1 − k·IQR .. Q3 + k·IQR.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct IqrDetector {
    k: f64,
    q1: f64,
    q3: f64,
    iqr: f64,
    lower_fence: f64,
    upper_fence: f64,
    fitted: bool,
}

impl IqrDetector {
    /// Creates a new detector. `k` defaults to 1.5.
    pub fn new(k: Option<f64>) -> Self {
        Self {
            k: k.unwrap_or(1.5),
            q1: 0.0,
            q3: 0.0,
            iqr: 0.0,
            lower_fence: 0.0,
            upper_fence: 0.0,
            fitted: false,
        }
    }

    fn percentile(sorted: &[f64], p: f64) -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        // Clamp percentile to valid range to prevent out-of-bounds access.
        let p = p.clamp(0.0, 1.0);
        let idx = p * (sorted.len() - 1) as f64;
        let lo = idx.floor() as usize;
        let hi = (idx.ceil() as usize).min(sorted.len() - 1);
        if lo == hi {
            sorted[lo]
        } else {
            let frac = idx - lo as f64;
            sorted[lo] * (1.0 - frac) + sorted[hi] * frac
        }
    }

    /// The dispersion the fences and the score both divide by.
    ///
    /// With `IQR == 0` — constant or heavily quantised data — the raw fences
    /// collapse onto `Q1 == Q3`, so `42.0 + 1e-13` falls outside them and is
    /// flagged. `score_value` already floored its denominator; the *fences*
    /// did not, so the score said "0.0, not an outlier" while `is_anomaly`
    /// said yes. One floor, used by both.
    #[inline]
    fn effective_iqr(&self) -> f64 {
        scale_floor(self.iqr, (self.q1 + self.q3) / 2.0)
    }

    /// Distance beyond the nearer fence, in units of the effective IQR.
    #[inline]
    fn score_value(&self, value: f64) -> f64 {
        let scale = self.effective_iqr();
        if value < self.lower_fence {
            (self.lower_fence - value) / scale
        } else if value > self.upper_fence {
            (value - self.upper_fence) / scale
        } else {
            0.0
        }
    }

    #[inline]
    fn normalize(raw: f64) -> f64 {
        // raw=0 → 0.0, raw growing → approaches 1.0
        1.0 - 1.0 / (1.0 + raw)
    }
}

impl AnomalyDetector for IqrDetector {
    fn fit(&mut self, _timestamps: &[i64], values: &[f64]) -> Result<(), AnomalyError> {
        let _start = std::time::Instant::now();
        if values.len() < 4 {
            return Err(AnomalyError::InsufficientData {
                min: 4,
                got: values.len(),
            });
        }
        let mut sorted = values.to_vec();
        sorted.sort_unstable_by(f64::total_cmp);
        self.q1 = Self::percentile(&sorted, 0.25);
        self.q3 = Self::percentile(&sorted, 0.75);
        self.iqr = self.q3 - self.q1;
        // The fences use the *floored* dispersion, so they never collapse to
        // a single point on constant data. See `effective_iqr`.
        let scale = self.effective_iqr();
        self.lower_fence = self.q1 - self.k * scale;
        self.upper_fence = self.q3 + self.k * scale;
        self.fitted = true;
        metrics::histogram!("chronix_anomaly_fit_duration_seconds", "method" => "iqr")
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
        let mut scores = Vec::with_capacity(values.len());
        for (i, &v) in values.iter().enumerate() {
            let raw = self.score_value(v);
            let is_anomaly = v < self.lower_fence || v > self.upper_fence;
            scores.push(AnomalyScore {
                timestamp: timestamps[i],
                value: v,
                score: Self::normalize(raw),
                is_anomaly,
                method: DetectorType::Iqr,
                threshold: self.k,
                details: format!(
                    "fences=[{:.2}, {:.2}] iqr={:.4}",
                    self.lower_fence, self.upper_fence, self.iqr
                ),
            });
        }
        let anomaly_count = scores.iter().filter(|s| s.is_anomaly).count();
        metrics::counter!("chronix_anomaly_detected_total", "method" => "iqr")
            .increment(anomaly_count as u64);
        metrics::histogram!("chronix_anomaly_detect_duration_seconds", "method" => "iqr")
            .record(_start.elapsed().as_secs_f64());
        Ok(scores)
    }

    fn detect_point(&mut self, timestamp: i64, value: f64) -> Result<AnomalyScore, AnomalyError> {
        if !self.fitted {
            return Err(AnomalyError::NotFitted);
        }
        let raw = self.score_value(value);
        let is_anomaly = value < self.lower_fence || value > self.upper_fence;
        Ok(AnomalyScore {
            timestamp,
            value,
            score: Self::normalize(raw),
            is_anomaly,
            method: DetectorType::Iqr,
            threshold: self.k,
            details: format!(
                "fences=[{:.2}, {:.2}] iqr={:.4}",
                self.lower_fence, self.upper_fence, self.iqr
            ),
        })
    }

    fn detector_type(&self) -> DetectorType {
        DetectorType::Iqr
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symmetric_bounds() {
        // Symmetric data 0..100 → Q1=25, Q3=75, IQR=50, fences=[-50, 150]
        let ts: Vec<i64> = (0..101).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..101).map(|i| i as f64).collect();
        let mut det = IqrDetector::new(Some(1.5));
        det.fit(&ts, &vals).unwrap();
        assert!((det.q1 - 25.0).abs() < 1.0);
        assert!((det.q3 - 75.0).abs() < 1.0);
        // Points within fences → not anomalous
        let s = det.detect_point(0, 50.0).unwrap();
        assert!(!s.is_anomaly);
        // Point outside → anomalous
        let s = det.detect_point(0, 200.0).unwrap();
        assert!(s.is_anomaly);
    }

    #[test]
    fn detect_extreme_outliers() {
        let ts: Vec<i64> = (0..100).map(|i| i * 1_000_000_000).collect();
        let mut vals: Vec<f64> = vec![50.0; 100];
        vals[10] = 5000.0;
        vals[90] = -5000.0;
        let mut det = IqrDetector::new(None);
        det.fit(&ts, &vals).unwrap();
        let scores = det.detect(&ts, &vals).unwrap();
        assert!(scores[10].is_anomaly);
        assert!(scores[90].is_anomaly);
    }

    #[test]
    fn k_sensitivity() {
        let ts: Vec<i64> = (0..100).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..100).map(|i| i as f64).collect();
        let mut tight = IqrDetector::new(Some(0.5));
        let mut loose = IqrDetector::new(Some(3.0));
        tight.fit(&ts, &vals).unwrap();
        loose.fit(&ts, &vals).unwrap();
        // Tight should flag more
        let st = tight.detect(&ts, &vals).unwrap();
        let sl = loose.detect(&ts, &vals).unwrap();
        let count_t: usize = st.iter().filter(|s| s.is_anomaly).count();
        let count_l: usize = sl.iter().filter(|s| s.is_anomaly).count();
        assert!(count_t >= count_l);
    }

    #[test]
    fn insufficient_data() {
        let mut det = IqrDetector::new(None);
        assert!(det.fit(&[0, 1, 2], &[1.0, 2.0, 3.0]).is_err());
    }

    #[test]
    fn constant_data_same_value_not_anomaly() {
        // All constant values should not be flagged
        let ts: Vec<i64> = (0..20).map(|i| i * 1_000_000_000).collect();
        let vals = vec![42.0; 20];
        let mut det = IqrDetector::new(None);
        det.fit(&ts, &vals).unwrap();
        let scores = det.detect(&ts, &vals).unwrap();
        // All scores should be finite and non-anomalous
        for s in &scores {
            assert!(s.score.is_finite(), "score should be finite");
            assert!(!s.is_anomaly, "constant values should not be flagged");
        }
    }

    #[test]
    fn constant_data_outlier_detected() {
        // Genuine outlier in constant data should be detected
        let ts: Vec<i64> = (0..20).map(|i| i * 1_000_000_000).collect();
        let vals = vec![42.0; 20];
        let mut det = IqrDetector::new(None);
        det.fit(&ts, &vals).unwrap();
        let s = det.detect_point(0, 100.0).unwrap();
        assert!(s.score.is_finite(), "score should be finite");
        assert!(
            s.score > 0.5,
            "genuine deviation from constant data should score high"
        );
    }
}
