//! Simple Exponential Smoothing (SES) forecast model.

use crate::forecast::error::ForecastError;
use crate::forecast::result::{ForecastResult, ModelParams, ModelType};
use crate::forecast::traits::ForecastModel;
use crate::forecast::util::{median_interval, validate_input};

/// Simple Exponential Smoothing model.
///
/// Flat-forecast at last smoothed level. Alpha is auto-optimized via Brent's
/// method (1-D line search) minimizing MSE if not specified.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct SesModel {
    alpha: Option<f64>,
    params: ModelParams,
    last_ts: i64,
    interval_ns: i64,
    n_points: usize,
    fitted: bool,
}

impl SesModel {
    /// Creates a new SES model. If `alpha` is `None`, it will be auto-optimized.
    pub fn new(alpha: Option<f64>) -> Self {
        Self {
            alpha,
            params: ModelParams::Ses {
                alpha: 0.0,
                level: 0.0,
                residual_std: 0.0,
            },
            last_ts: 0,
            interval_ns: 0,
            n_points: 0,
            fitted: false,
        }
    }

    /// Find optimal alpha by Brent's method minimizing MSE.
    fn optimize_alpha(values: &[f64]) -> f64 {
        let result = crate::forecast::optimizer::minimize_brent(
            |alpha| Self::compute_mse(values, alpha),
            0.01,
            0.99,
            1e-8,
            100,
        );
        result.params[0].clamp(0.01, 0.99)
    }

    fn compute_mse(values: &[f64], alpha: f64) -> f64 {
        if values.len() < 2 {
            return 0.0;
        }
        let one_minus = 1.0 - alpha;
        let mut level = values[0];
        let mut sse = 0.0;
        for &value in values.iter().skip(1) {
            let err = value - level;
            sse += err * err;
            level = alpha * value + one_minus * level;
        }
        sse / (values.len() - 1) as f64
    }
}

impl ForecastModel for SesModel {
    #[tracing::instrument(skip_all, level = "debug")]
    fn fit(&mut self, timestamps: &[i64], values: &[f64]) -> Result<(), ForecastError> {
        let _start = std::time::Instant::now();
        validate_input(timestamps, values)?;
        if values.len() < 2 {
            return Err(ForecastError::InsufficientData {
                min: 2,
                got: values.len(),
            });
        }

        let alpha = self.alpha.unwrap_or_else(|| Self::optimize_alpha(values));
        let one_minus = 1.0 - alpha;

        // Compute smoothed level and residual statistics
        let mut level = values[0];
        let mut sse = 0.0;
        for &value in values.iter().skip(1) {
            let err = value - level;
            sse += err * err;
            level = alpha * value + one_minus * level;
        }

        // Correct degrees of freedom — we have n-1 residuals and
        // 1 estimated parameter (α), so divisor = max(n-2, 1).
        let dof = (values.len() as f64 - 2.0).max(1.0);
        let residual_std = (sse / dof).sqrt();

        self.interval_ns = median_interval(timestamps);
        self.last_ts = timestamps[timestamps.len() - 1];
        self.n_points = values.len();

        self.params = ModelParams::Ses {
            alpha,
            level,
            residual_std,
        };
        self.fitted = true;
        metrics::histogram!("chronix_forecast_fit_duration_seconds", "model_type" => "ses")
            .record(_start.elapsed().as_secs_f64());
        Ok(())
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn predict(&self, horizon: usize) -> Result<ForecastResult, ForecastError> {
        let _start = std::time::Instant::now();
        if !self.fitted {
            return Err(ForecastError::NotFitted);
        }

        let ModelParams::Ses {
            alpha,
            level,
            residual_std,
        } = &self.params
        else {
            return Err(ForecastError::InvalidInput(
                "unexpected model params variant".into(),
            ));
        };

        let z = 1.96; // 95% confidence
        let mut values = Vec::with_capacity(horizon);
        let mut timestamps = Vec::with_capacity(horizon);
        let mut lower = Vec::with_capacity(horizon);
        let mut upper = Vec::with_capacity(horizon);

        for h in 1..=horizon {
            let ts = self
                .last_ts
                .saturating_add((h as i64).saturating_mul(self.interval_ns));
            let width = z * residual_std * (1.0 + (h as f64 - 1.0) * alpha * alpha).sqrt();
            values.push(*level);
            timestamps.push(ts);
            lower.push(level - width);
            upper.push(level + width);
        }

        metrics::histogram!("chronix_forecast_predict_duration_seconds", "model_type" => "ses")
            .record(_start.elapsed().as_secs_f64());
        Ok(ForecastResult {
            values,
            timestamps,
            confidence_lower: lower,
            confidence_upper: upper,
            confidence_level: 0.95,
        })
    }

    fn update(&mut self, timestamp: i64, value: f64) -> Result<(), ForecastError> {
        if !self.fitted {
            return Err(ForecastError::NotFitted);
        }

        let ModelParams::Ses {
            alpha,
            ref mut level,
            ..
        } = self.params
        else {
            return Err(ForecastError::InvalidInput(
                "unexpected model params variant".into(),
            ));
        };

        *level = alpha * value + (1.0 - alpha) * *level;
        self.last_ts = timestamp;
        self.n_points += 1;
        Ok(())
    }

    fn model_type(&self) -> ModelType {
        ModelType::Ses
    }

    fn params(&self) -> &ModelParams {
        &self.params
    }
}

impl crate::forecast::storage::ModelStore for SesModel {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ses_constant_series() {
        let ts: Vec<i64> = (0..100).map(|i| i * 1_000_000_000).collect();
        let vals = vec![42.0; 100];
        let mut model = SesModel::new(None);
        model.fit(&ts, &vals).unwrap();
        let result = model.predict(10).unwrap();
        for &v in &result.values {
            assert!((v - 42.0).abs() < 1e-6);
        }
    }

    #[test]
    fn ses_trending_series() {
        let ts: Vec<i64> = (0..100).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..100).map(|i| i as f64 * 2.0).collect();
        let mut model = SesModel::new(Some(0.8));
        model.fit(&ts, &vals).unwrap();
        let result = model.predict(5).unwrap();
        // SES can't track trend perfectly, but forecast should be near last value
        let last = vals.last().unwrap();
        assert!((result.values[0] - last).abs() < 20.0);
    }

    #[test]
    fn ses_auto_alpha() {
        let ts: Vec<i64> = (0..50).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..50).map(|i| (i as f64).sin() * 10.0 + 50.0).collect();
        let mut model = SesModel::new(None);
        model.fit(&ts, &vals).unwrap();
        let ModelParams::Ses { alpha, .. } = model.params() else {
            panic!()
        };
        assert!(*alpha > 0.0 && *alpha <= 1.0);
    }

    #[test]
    fn ses_update() {
        let ts: Vec<i64> = (0..10).map(|i| i * 1_000_000_000).collect();
        let vals = vec![10.0; 10];
        let mut model = SesModel::new(Some(0.5));
        model.fit(&ts, &vals).unwrap();

        // Update with a high value — level should shift
        model.update(10_000_000_000, 100.0).unwrap();
        let ModelParams::Ses { level, .. } = model.params() else {
            panic!()
        };
        assert!(*level > 10.0 && *level < 100.0);
    }

    #[test]
    fn ses_confidence_intervals_widen() {
        let ts: Vec<i64> = (0..50).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..50).map(|i| i as f64 + (i as f64 * 0.5).sin()).collect();
        let mut model = SesModel::new(Some(0.3));
        model.fit(&ts, &vals).unwrap();
        let result = model.predict(10).unwrap();
        // Confidence intervals should widen with horizon
        let width_1 = result.confidence_upper[0] - result.confidence_lower[0];
        let width_10 = result.confidence_upper[9] - result.confidence_lower[9];
        assert!(width_10 > width_1);
    }

    #[test]
    fn ses_insufficient_data() {
        let mut model = SesModel::new(None);
        assert!(model.fit(&[0], &[1.0]).is_err());
    }

    #[test]
    fn ses_not_fitted() {
        let model = SesModel::new(None);
        assert!(model.predict(10).is_err());
    }

    #[test]
    fn metrics_recorded_on_fit_predict() {
        // Metrics macros are no-ops without a recorder, but we verify the
        // code paths execute without panic — the `metrics` crate guarantees
        // this is safe even without a subscriber installed.
        let ts: Vec<i64> = (0..100).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..100).map(|i| 50.0 + (i as f64) * 0.1).collect();
        let mut model = SesModel::new(Some(0.3));
        model.fit(&ts, &vals).unwrap();
        let result = model.predict(10).unwrap();
        assert_eq!(result.values.len(), 10);
        // If metrics crate panicked, we wouldn't reach here
    }
}
