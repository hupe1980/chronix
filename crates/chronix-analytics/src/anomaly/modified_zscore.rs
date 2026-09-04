//! Modified Z-Score detector using Median Absolute Deviation (MAD).

use crate::anomaly::error::AnomalyError;
use crate::anomaly::traits::{
    scale_floor, validate_lengths, AnomalyDetector, AnomalyScore, DetectorType,
};

/// MAD-based detector robust to outliers in training data.
///
/// Modified Z-Score = 0.6745 × (value − median) / MAD
#[derive(serde::Serialize, serde::Deserialize)]
pub struct ModifiedZScoreDetector {
    threshold: f64,
    median: f64,
    mad: f64,
    /// Mean absolute deviation about the median, the fallback scale when the
    /// MAD is exactly zero.
    mean_ad: f64,
    fitted: bool,
}

impl ModifiedZScoreDetector {
    /// Creates a new detector. `threshold` defaults to 3.5.
    pub fn new(threshold: Option<f64>) -> Self {
        Self {
            threshold: threshold.unwrap_or(3.5),
            median: 0.0,
            mad: 0.0,
            mean_ad: 0.0,
            fitted: false,
        }
    }

    fn compute_median(data: &mut [f64]) -> f64 {
        data.sort_unstable_by(f64::total_cmp);
        let n = data.len();
        if n.is_multiple_of(2) {
            (data[n / 2 - 1] + data[n / 2]) / 2.0
        } else {
            data[n / 2]
        }
    }

    /// Iglewicz & Hoaglin (1993), *How to Detect and Handle Outliers*, §3.
    ///
    /// The primary statistic is `M_i = 0.6745·(x − x̃)/MAD`. On **quantised**
    /// data — a 0.5 °C thermostat, a percentage rounded to whole numbers, an
    /// integer counter — more than half the sample can equal the median, and
    /// then the MAD is *exactly* zero: a real and common case, not a
    /// degenerate one. Dividing by a `1e-12` floor there gave the value one
    /// quantisation step away a modified z-score of ~1e7, so the detector
    /// flagged the ordinary neighbouring bucket as an extreme outlier.
    ///
    /// Iglewicz & Hoaglin give the alternative for exactly this case:
    /// `M_i = (x − x̃)/(1.253314·MeanAD)`, where `MeanAD` is the *mean*
    /// absolute deviation about the median, whose expectation is `σ/1.253314`
    /// for normal data — hence the constant, and hence no `0.6745` factor
    /// here. Only when both dispersion estimates are zero (a genuinely
    /// constant sample) does the relative floor apply.
    #[inline]
    fn score_value(&self, value: f64) -> f64 {
        let dev = value - self.median;
        if self.mad > 0.0 {
            (0.6745 * dev / self.mad).abs()
        } else if self.mean_ad > 0.0 {
            (dev / (1.253_314 * self.mean_ad)).abs()
        } else {
            (0.6745 * dev / scale_floor(0.0, self.median)).abs()
        }
    }

    #[inline]
    fn normalize(raw: f64, threshold: f64) -> f64 {
        let x = (raw - threshold) * 2.0;
        1.0 / (1.0 + (-x).exp())
    }
}

impl AnomalyDetector for ModifiedZScoreDetector {
    fn fit(&mut self, _timestamps: &[i64], values: &[f64]) -> Result<(), AnomalyError> {
        let _start = std::time::Instant::now();
        if values.len() < 3 {
            return Err(AnomalyError::InsufficientData {
                min: 3,
                got: values.len(),
            });
        }
        let mut sorted = values.to_vec();
        self.median = Self::compute_median(&mut sorted);
        let mut deviations: Vec<f64> = values.iter().map(|&v| (v - self.median).abs()).collect();
        self.mean_ad = deviations.iter().sum::<f64>() / deviations.len() as f64;
        self.mad = Self::compute_median(&mut deviations);
        self.fitted = true;
        metrics::histogram!("chronix_anomaly_fit_duration_seconds", "method" => "modified_zscore")
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
            scores.push(AnomalyScore {
                timestamp: timestamps[i],
                value: v,
                score: Self::normalize(raw, self.threshold),
                is_anomaly: raw > self.threshold,
                method: DetectorType::ModifiedZScore,
                threshold: self.threshold,
                details: format!("modified_z={raw:.4}"),
            });
        }
        let anomaly_count = scores.iter().filter(|s| s.is_anomaly).count();
        metrics::counter!("chronix_anomaly_detected_total", "method" => "modified_zscore")
            .increment(anomaly_count as u64);
        metrics::histogram!("chronix_anomaly_detect_duration_seconds", "method" => "modified_zscore").record(_start.elapsed().as_secs_f64());
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
            method: DetectorType::ModifiedZScore,
            threshold: self.threshold,
            details: format!("modified_z={raw:.4}"),
        })
    }

    fn detector_type(&self) -> DetectorType {
        DetectorType::ModifiedZScore
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn robust_to_outliers_in_training() {
        // Training data with outliers — MAD should be more robust than std dev
        let ts: Vec<i64> = (0..100).map(|i| i * 1_000_000_000).collect();
        let mut vals: Vec<f64> = vec![50.0; 100];
        // Inject a few heavy outliers in training data
        vals[0] = 500.0;
        vals[1] = -400.0;
        vals[99] = 600.0;

        let mut det = ModifiedZScoreDetector::new(Some(3.5));
        det.fit(&ts, &vals).unwrap();

        // Median should be near 50.0
        assert!((det.median - 50.0).abs() < 1.0);

        // A normal value should not be flagged
        let s = det.detect_point(999, 50.0).unwrap();
        assert!(!s.is_anomaly);

        // A genuine outlier should be flagged
        let s = det.detect_point(999, 500.0).unwrap();
        assert!(s.is_anomaly);
    }

    #[test]
    fn detect_outliers() {
        let ts: Vec<i64> = (0..200).map(|i| i * 1_000_000_000).collect();
        let mut vals: Vec<f64> = (0..200)
            .map(|i| (i as f64 * 0.1).sin() * 5.0 + 50.0)
            .collect();
        vals[75] = 200.0;
        vals[175] = -100.0;

        let mut det = ModifiedZScoreDetector::new(None);
        det.fit(&ts, &vals).unwrap();
        let scores = det.detect(&ts, &vals).unwrap();
        assert!(scores[75].is_anomaly);
        assert!(scores[175].is_anomaly);
    }

    #[test]
    fn insufficient_data() {
        let mut det = ModifiedZScoreDetector::new(None);
        assert!(det.fit(&[0, 1], &[1.0, 2.0]).is_err());
    }

    #[test]
    fn constant_data_does_not_flag_same_values() {
        let ts: Vec<i64> = (0..50).map(|i| i * 1_000_000_000).collect();
        let vals = vec![42.0; 50];

        let mut det = ModifiedZScoreDetector::new(Some(3.5));
        det.fit(&ts, &vals).unwrap();

        // Same value should NOT be flagged
        let scores = det.detect(&ts, &vals).unwrap();
        let anomalies: Vec<_> = scores.iter().filter(|s| s.is_anomaly).collect();
        assert!(
            anomalies.is_empty(),
            "Constant data should not flag identical values as anomalies, got {} anomalies",
            anomalies.len(),
        );
    }

    #[test]
    fn constant_data_flags_genuine_deviation() {
        let ts: Vec<i64> = (0..50).map(|i| i * 1_000_000_000).collect();
        let vals = vec![42.0; 50];

        let mut det = ModifiedZScoreDetector::new(Some(3.5));
        det.fit(&ts, &vals).unwrap();

        // A genuinely different value should be flagged
        let s = det.detect_point(999, 100.0).unwrap();
        assert!(
            s.is_anomaly,
            "Large deviation from constant median should be anomaly"
        );
        assert!(s.score.is_finite(), "Score should be finite, not INFINITY");
    }
}
