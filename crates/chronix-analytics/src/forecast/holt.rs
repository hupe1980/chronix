//! Holt's Linear Trend (double exponential smoothing) forecast model.

use crate::forecast::error::ForecastError;
use crate::forecast::result::{ForecastResult, ModelParams, ModelType};
use crate::forecast::traits::ForecastModel;
use crate::forecast::util::{median_interval, validate_input};

/// Holt's Linear Trend model with optional damping.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct HoltLinearModel {
    alpha: Option<f64>,
    beta: Option<f64>,
    phi: f64, // damping factor (1.0 = no damping)
    params: ModelParams,
    last_ts: i64,
    interval_ns: i64,
    fitted: bool,
}

impl HoltLinearModel {
    /// Creates a new Holt's Linear Trend model.
    ///
    /// `phi` is the damping factor (0.8–1.0). Use 1.0 for no damping.
    pub fn new(alpha: Option<f64>, beta: Option<f64>, phi: f64) -> Self {
        Self {
            alpha,
            beta,
            phi: phi.clamp(0.8, 1.0),
            params: ModelParams::HoltLinear {
                alpha: 0.0,
                beta: 0.0,
                level: 0.0,
                trend: 0.0,
                phi: 1.0,
                residual_std: 0.0,
            },
            last_ts: 0,
            interval_ns: 0,
            fitted: false,
        }
    }

    fn optimize_params(values: &[f64], phi: f64) -> (f64, f64) {
        let result = crate::forecast::optimizer::minimize_nelder_mead(
            |p| Self::compute_mse(values, p[0], p[1], phi),
            &[0.3, 0.1],
            &[
                crate::forecast::optimizer::Bound::new(0.01, 0.99),
                crate::forecast::optimizer::Bound::new(0.01, 0.99),
            ],
            300,
            1e-8,
        );
        (
            result.params[0].clamp(0.01, 0.99),
            result.params[1].clamp(0.01, 0.99),
        )
    }

    fn compute_mse(values: &[f64], alpha: f64, beta: f64, phi: f64) -> f64 {
        if values.len() < 3 {
            return f64::INFINITY;
        }
        let mut level = values[0];
        let mut trend = values[1] - values[0];
        let mut sse = 0.0;

        for &value in values.iter().skip(1) {
            let forecast = level + phi * trend;
            let err = value - forecast;
            sse += err * err;
            let new_level = alpha * value + (1.0 - alpha) * (level + phi * trend);
            trend = beta * (new_level - level) + (1.0 - beta) * phi * trend;
            level = new_level;
        }
        sse / (values.len() - 1) as f64
    }
}

impl ForecastModel for HoltLinearModel {
    #[tracing::instrument(skip_all, level = "debug")]
    fn fit(&mut self, timestamps: &[i64], values: &[f64]) -> Result<(), ForecastError> {
        let _start = std::time::Instant::now();
        validate_input(timestamps, values)?;
        if values.len() < 3 {
            return Err(ForecastError::InsufficientData {
                min: 3,
                got: values.len(),
            });
        }

        let (alpha, beta) = match (self.alpha, self.beta) {
            (Some(a), Some(b)) => (a, b),
            _ => Self::optimize_params(values, self.phi),
        };

        let mut level = values[0];
        let mut trend = values[1] - values[0];
        let mut sse = 0.0;

        for &value in values.iter().skip(1) {
            let forecast = level + self.phi * trend;
            let err = value - forecast;
            sse += err * err;
            let new_level = alpha * value + (1.0 - alpha) * (level + self.phi * trend);
            trend = beta * (new_level - level) + (1.0 - beta) * self.phi * trend;
            level = new_level;
        }

        // Correct degrees of freedom — we have n-1 residuals and
        // 3 estimated parameters (α, β, ϕ), so divisor = max(n-4, 1).
        let dof = (values.len() as f64 - 4.0).max(1.0);
        let residual_std = (sse / dof).sqrt();

        self.interval_ns = median_interval(timestamps);
        self.last_ts = timestamps[timestamps.len() - 1];

        self.params = ModelParams::HoltLinear {
            alpha,
            beta,
            level,
            trend,
            phi: self.phi,
            residual_std,
        };
        self.fitted = true;
        metrics::histogram!("chronix_forecast_fit_duration_seconds", "model_type" => "holt_linear")
            .record(_start.elapsed().as_secs_f64());
        Ok(())
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn predict(&self, horizon: usize) -> Result<ForecastResult, ForecastError> {
        let _start = std::time::Instant::now();
        if !self.fitted {
            return Err(ForecastError::NotFitted);
        }

        let ModelParams::HoltLinear {
            alpha,
            beta,
            level,
            trend,
            phi,
            residual_std,
            ..
        } = &self.params
        else {
            return Err(ForecastError::InvalidInput(
                "unexpected model params variant".into(),
            ));
        };

        let z = 1.96;
        let mut values = Vec::with_capacity(horizon);
        let mut timestamps = Vec::with_capacity(horizon);
        let mut lower = Vec::with_capacity(horizon);
        let mut upper = Vec::with_capacity(horizon);

        for h in 1..=horizon {
            let ts = self
                .last_ts
                .saturating_add((h as i64).saturating_mul(self.interval_ns));
            // Damped trend: sum of phi^1 + phi^2 + ... + phi^h
            let damped_sum = if (*phi - 1.0).abs() < 1e-10 {
                h as f64
            } else {
                phi * (1.0 - phi.powi(h as i32)) / (1.0 - phi)
            };
            let forecast = level + damped_sum * trend;

            // Correct PI variance per Hyndman et al. (2008) Chapter 6.
            // Var[e_h] = sigma^2 * sum_{j=0..h-1} c_j^2
            // where c_j = 1 + j * alpha * beta for additive Holt's (undamped),
            // and c_j = 1 + alpha * beta * sum_{i=1..j} phi^i for damped Holt's.
            let mut var_sum = 0.0;
            for j in 0..h {
                let cj = if (*phi - 1.0).abs() < 1e-10 {
                    // Undamped: c_j = 1 + j * alpha * beta
                    1.0 + j as f64 * alpha * beta
                } else {
                    // Damped: c_j = 1 + alpha * beta * phi * (1 - phi^j) / (1 - phi)
                    let phi_sum = if j == 0 {
                        0.0
                    } else {
                        phi * (1.0 - phi.powi(j as i32)) / (1.0 - phi)
                    };
                    1.0 + alpha * beta * phi_sum
                };
                var_sum += cj * cj;
            }
            let width = z * residual_std * var_sum.sqrt();
            values.push(forecast);
            timestamps.push(ts);
            lower.push(forecast - width);
            upper.push(forecast + width);
        }

        metrics::histogram!("chronix_forecast_predict_duration_seconds", "model_type" => "holt_linear").record(_start.elapsed().as_secs_f64());
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

        let ModelParams::HoltLinear {
            alpha,
            beta,
            ref mut level,
            ref mut trend,
            phi,
            ..
        } = self.params
        else {
            return Err(ForecastError::InvalidInput(
                "unexpected model params variant".into(),
            ));
        };

        let new_level = alpha * value + (1.0 - alpha) * (*level + phi * *trend);
        *trend = beta * (new_level - *level) + (1.0 - beta) * phi * *trend;
        *level = new_level;
        self.last_ts = timestamp;
        Ok(())
    }

    fn model_type(&self) -> ModelType {
        ModelType::HoltLinear
    }

    fn params(&self) -> &ModelParams {
        &self.params
    }
}

impl crate::forecast::storage::ModelStore for HoltLinearModel {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn holt_linear_trend() {
        let ts: Vec<i64> = (0..100).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..100).map(|i| i as f64 * 3.0 + 10.0).collect();
        let mut model = HoltLinearModel::new(Some(0.8), Some(0.2), 1.0);
        model.fit(&ts, &vals).unwrap();
        let result = model.predict(5).unwrap();
        // Linear trend should be captured
        let last = *vals.last().unwrap();
        assert!((result.values[0] - (last + 3.0)).abs() < 5.0);
    }

    #[test]
    fn holt_damped_trend() {
        let ts: Vec<i64> = (0..50).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..50).map(|i| i as f64 * 2.0).collect();
        let mut model = HoltLinearModel::new(Some(0.8), Some(0.2), 0.9);
        model.fit(&ts, &vals).unwrap();
        let result = model.predict(20).unwrap();
        // Damped trend should decay — later predictions should slow down
        let diff_early = result.values[1] - result.values[0];
        let diff_late = result.values[19] - result.values[18];
        assert!(diff_late < diff_early);
    }

    #[test]
    fn holt_auto_params() {
        let ts: Vec<i64> = (0..30).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..30).map(|i| i as f64 + (i as f64 * 0.3).sin()).collect();
        let mut model = HoltLinearModel::new(None, None, 1.0);
        model.fit(&ts, &vals).unwrap();
        let result = model.predict(5).unwrap();
        assert_eq!(result.values.len(), 5);
    }

    #[test]
    fn holt_update() {
        let ts: Vec<i64> = (0..20).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..20).map(|i| i as f64).collect();
        let mut model = HoltLinearModel::new(Some(0.5), Some(0.1), 1.0);
        model.fit(&ts, &vals).unwrap();
        model.update(20_000_000_000, 100.0).unwrap();
        let ModelParams::HoltLinear { level, .. } = model.params() else {
            panic!()
        };
        assert!(*level > 19.0);
    }

    #[test]
    fn holt_insufficient_data() {
        let mut model = HoltLinearModel::new(None, None, 1.0);
        assert!(model.fit(&[0, 1], &[1.0, 2.0]).is_err());
    }
}
