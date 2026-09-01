//! Preprocessing pipeline — composes preprocessing steps into configurable pipelines.

use crate::preprocess::clock_drift::{ClockDriftDetector, ClockDriftStrategy, DriftReport};
use crate::preprocess::interpolation::Interpolator;
use crate::preprocess::resampling::{ResampleConfig, Resampler};
use crate::preprocess::smoothing::Smoother;

/// Result of a preprocessing pipeline run.
#[derive(Debug, Clone)]
pub struct PreprocessResult {
    /// Output values.
    pub values: Vec<f64>,
    /// Output timestamps (nanoseconds).
    pub timestamps: Vec<i64>,
    /// Number of gaps filled during interpolation.
    pub gaps_filled: usize,
    /// Clock drift report (if drift correction was applied).
    pub drift_report: Option<DriftReport>,
}

/// Preprocessing errors.
#[derive(Debug, thiserror::Error)]
pub enum PreprocessError {
    /// The expected interval is not positive.
    #[error("expected interval must be positive, got {0}")]
    InvalidInterval(i64),

    /// Timestamp and value arrays have different lengths.
    #[error("timestamps and values must have equal length: ts={ts_len}, vals={vals_len}")]
    LengthMismatch {
        /// Length of the timestamp array.
        ts_len: usize,
        /// Length of the values array.
        vals_len: usize,
    },

    /// The smoother alpha value is outside the valid range (0, 1].
    #[error("invalid smoother alpha: must be in (0, 1], got {0}")]
    InvalidAlpha(f64),

    /// The smoother window size must be greater than zero.
    #[error("invalid smoother window: must be > 0")]
    InvalidWindow,

    /// An interpolation error occurred.
    #[error(transparent)]
    Interpolation(#[from] crate::preprocess::interpolation::InterpolationError),
}

/// Configuration for the preprocessing pipeline.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PreprocessConfig {
    /// Expected interval between points (nanoseconds).
    pub expected_interval_ns: i64,
    /// Gap detection tolerance multiplier.
    #[serde(default = "default_tolerance")]
    pub gap_tolerance: f64,
    /// Interpolation method (None = skip).
    pub interpolation: Option<Interpolator>,
    /// Smoothing method (None = skip).
    pub smoothing: Option<Smoother>,
    /// Resampling configuration (None = skip).
    pub resample: Option<ResampleConfig>,
    /// Clock drift correction strategy.
    #[serde(default)]
    pub clock_drift: ClockDriftStrategy,
}

fn default_tolerance() -> f64 {
    1.5
}

impl Default for PreprocessConfig {
    fn default() -> Self {
        Self {
            expected_interval_ns: 1_000_000_000, // 1 second
            gap_tolerance: 1.5,
            interpolation: None,
            smoothing: None,
            resample: None,
            clock_drift: ClockDriftStrategy::None,
        }
    }
}

/// Preprocessing pipeline.
pub struct PreprocessPipeline;

impl PreprocessPipeline {
    /// Run the preprocessing pipeline on the given data.
    ///
    /// Steps are applied in order:
    /// 1. Clock drift correction
    /// 2. Gap interpolation
    /// 3. Smoothing
    /// 4. Resampling
    ///
    /// Steps are skipped if not configured.
    pub fn run(
        config: &PreprocessConfig,
        timestamps: &[i64],
        values: &[f64],
    ) -> Result<PreprocessResult, PreprocessError> {
        let _start = std::time::Instant::now();
        // Validate inputs
        if timestamps.len() != values.len() {
            return Err(PreprocessError::LengthMismatch {
                ts_len: timestamps.len(),
                vals_len: values.len(),
            });
        }
        if config.expected_interval_ns <= 0 {
            return Err(PreprocessError::InvalidInterval(
                config.expected_interval_ns,
            ));
        }

        // Validate smoother config
        if let Some(ref smoother) = config.smoothing {
            match smoother {
                Smoother::Exponential { alpha } => {
                    if *alpha <= 0.0 || *alpha > 1.0 {
                        return Err(PreprocessError::InvalidAlpha(*alpha));
                    }
                }
                Smoother::MovingAverage { window } => {
                    if *window == 0 {
                        return Err(PreprocessError::InvalidWindow);
                    }
                }
                Smoother::WeightedMovingAverage { weights } => {
                    if weights.is_empty() {
                        return Err(PreprocessError::InvalidWindow);
                    }
                }
            }
        }

        let mut current_ts = timestamps.to_vec();
        let mut current_vals = values.to_vec();
        let mut gaps_filled = 0usize;
        let mut drift_report = None;

        // Step 1: Clock drift correction
        if config.clock_drift != ClockDriftStrategy::None {
            let (corrected_ts, report) = ClockDriftDetector::correct(
                &current_ts,
                config.expected_interval_ns,
                config.clock_drift,
            );
            current_ts = corrected_ts;
            drift_report = Some(report);
        }

        // Step 2: Interpolation
        if let Some(ref interpolator) = config.interpolation {
            let (new_ts, new_vals, filled, skipped) =
                interpolator.fill(&current_ts, &current_vals, config.expected_interval_ns)?;
            current_ts = new_ts;
            current_vals = new_vals;
            gaps_filled = filled;
            if skipped > 0 {
                tracing::warn!(gaps_skipped = skipped, "some gaps were too large to fill");
            }
        }

        // Step 3: Smoothing
        if let Some(ref smoother) = config.smoothing {
            current_vals = smoother.smooth(&current_vals);
        }

        // Step 4: Resampling
        if let Some(ref resample_config) = config.resample {
            let (new_ts, new_vals) =
                Resampler::resample(&current_ts, &current_vals, resample_config);
            current_ts = new_ts;
            current_vals = new_vals;
        }

        metrics::histogram!("chronix_preprocess_duration_seconds")
            .record(_start.elapsed().as_secs_f64());
        Ok(PreprocessResult {
            values: current_vals,
            timestamps: current_ts,
            gaps_filled,
            drift_report,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ts(secs: &[i64]) -> Vec<i64> {
        secs.iter().map(|&s| s * 1_000_000_000).collect()
    }

    #[test]
    fn empty_config_passthrough() {
        let ts = make_ts(&[0, 1, 2, 3]);
        let vals = vec![1.0, 2.0, 3.0, 4.0];
        let config = PreprocessConfig::default();
        let result = PreprocessPipeline::run(&config, &ts, &vals).unwrap();
        assert_eq!(result.timestamps, ts);
        assert_eq!(result.values, vals);
        assert_eq!(result.gaps_filled, 0);
        assert!(result.drift_report.is_none());
    }

    #[test]
    fn interpolation_only() {
        let ts = make_ts(&[0, 1, 4, 5]); // gap between 1 and 4
        let vals = vec![10.0, 20.0, 50.0, 60.0];
        let config = PreprocessConfig {
            expected_interval_ns: 1_000_000_000,
            interpolation: Some(Interpolator::Linear),
            ..Default::default()
        };
        let result = PreprocessPipeline::run(&config, &ts, &vals).unwrap();
        assert_eq!(result.gaps_filled, 2);
        assert_eq!(result.timestamps.len(), 6);
    }

    #[test]
    fn smoothing_only() {
        let ts = make_ts(&[0, 1, 2, 3, 4]);
        let vals = vec![10.0, 20.0, 15.0, 25.0, 20.0];
        let config = PreprocessConfig {
            expected_interval_ns: 1_000_000_000,
            smoothing: Some(Smoother::Exponential { alpha: 0.5 }),
            ..Default::default()
        };
        let result = PreprocessPipeline::run(&config, &ts, &vals).unwrap();
        assert_eq!(result.values.len(), 5);
        assert_eq!(result.gaps_filled, 0);
    }

    #[test]
    fn full_pipeline() {
        let ts = make_ts(&[0, 1, 4, 5, 6]); // gap between 1 and 4
        let vals = vec![10.0, 20.0, 50.0, 60.0, 70.0];
        let config = PreprocessConfig {
            expected_interval_ns: 1_000_000_000,
            interpolation: Some(Interpolator::Linear),
            smoothing: Some(Smoother::MovingAverage { window: 3 }),
            ..Default::default()
        };
        let result = PreprocessPipeline::run(&config, &ts, &vals).unwrap();
        assert!(result.gaps_filled > 0);
        assert!(result.values.len() > vals.len());
    }

    #[test]
    fn validation_length_mismatch() {
        let ts = make_ts(&[0, 1, 2]);
        let vals = vec![1.0, 2.0];
        let config = PreprocessConfig::default();
        assert!(PreprocessPipeline::run(&config, &ts, &vals).is_err());
    }

    #[test]
    fn validation_invalid_interval() {
        let ts = make_ts(&[0, 1]);
        let vals = vec![1.0, 2.0];
        let config = PreprocessConfig {
            expected_interval_ns: -1,
            ..Default::default()
        };
        assert!(PreprocessPipeline::run(&config, &ts, &vals).is_err());
    }

    #[test]
    fn validation_invalid_alpha() {
        let ts = make_ts(&[0, 1]);
        let vals = vec![1.0, 2.0];
        let config = PreprocessConfig {
            expected_interval_ns: 1_000_000_000,
            smoothing: Some(Smoother::Exponential { alpha: 0.0 }),
            ..Default::default()
        };
        assert!(PreprocessPipeline::run(&config, &ts, &vals).is_err());
    }

    #[test]
    fn with_clock_drift_correction() {
        let ts = vec![0, 100, 50, 200, 300]; // backward at index 2
        let vals = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let config = PreprocessConfig {
            expected_interval_ns: 100,
            clock_drift: ClockDriftStrategy::Monotonic,
            ..Default::default()
        };
        let result = PreprocessPipeline::run(&config, &ts, &vals).unwrap();
        assert!(result.drift_report.is_some());
        let report = result.drift_report.unwrap();
        assert!(report.corrections_applied > 0);
        // Timestamps should be monotonically increasing
        for i in 1..result.timestamps.len() {
            assert!(result.timestamps[i] > result.timestamps[i - 1]);
        }
    }
}
