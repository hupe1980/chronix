//! Linear Regression forecast model.

use crate::forecast::error::ForecastError;
use crate::forecast::result::{ForecastResult, ModelParams, ModelType};
use crate::forecast::traits::ForecastModel;
use crate::forecast::util::{median_interval, validate_input};

/// Ordinary Least Squares linear regression for trend forecasting.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct LinearRegressionModel {
    params: ModelParams,
    n_points: usize,
    // Running sums for online update (Welford's)
    sum_x: f64,
    sum_y: f64,
    sum_xy: f64,
    sum_xx: f64,
    sum_yy: f64,
    fitted: bool,
}

impl LinearRegressionModel {
    /// Creates a new linear regression model.
    pub fn new() -> Self {
        Self {
            params: ModelParams::LinearRegression {
                slope: 0.0,
                intercept: 0.0,
                r_squared: 0.0,
                residual_std: 0.0,
                start_ts: 0,
                interval_ns: 0,
            },
            n_points: 0,
            sum_x: 0.0,
            sum_y: 0.0,
            sum_xy: 0.0,
            sum_xx: 0.0,
            sum_yy: 0.0,
            fitted: false,
        }
    }

    fn recompute_params(&mut self, values: &[f64], start_ts: i64, interval_ns: i64) {
        let n = self.n_points as f64;
        let mean_x = self.sum_x / n;
        let mean_y = self.sum_y / n;

        let ss_xx = self.sum_xx - n * mean_x * mean_x;
        let ss_xy = self.sum_xy - n * mean_x * mean_y;

        let slope = if ss_xx.abs() > 1e-15 {
            ss_xy / ss_xx
        } else {
            0.0
        };
        let intercept = mean_y - slope * mean_x;

        // R² and residual std
        let ss_yy: f64 = values.iter().map(|&y| (y - mean_y).powi(2)).sum();
        let ss_res: f64 = values
            .iter()
            .enumerate()
            .map(|(i, &y)| {
                let x = i as f64;
                let pred = slope * x + intercept;
                (y - pred).powi(2)
            })
            .sum();

        let r_squared = if ss_yy.abs() > 1e-15 {
            1.0 - ss_res / ss_yy
        } else {
            1.0
        };

        let residual_std = if self.n_points > 2 {
            (ss_res / (self.n_points - 2) as f64).sqrt()
        } else {
            0.0
        };

        self.params = ModelParams::LinearRegression {
            slope,
            intercept,
            r_squared,
            residual_std,
            start_ts,
            interval_ns,
        };
    }
}

impl Default for LinearRegressionModel {
    fn default() -> Self {
        Self::new()
    }
}

impl ForecastModel for LinearRegressionModel {
    fn fit(&mut self, timestamps: &[i64], values: &[f64]) -> Result<(), ForecastError> {
        let _start = std::time::Instant::now();
        validate_input(timestamps, values)?;
        if values.len() < 2 {
            return Err(ForecastError::InsufficientData {
                min: 2,
                got: values.len(),
            });
        }

        let start_ts = timestamps[0];
        let interval_ns = median_interval(timestamps);

        // Normalize timestamps to index-based x values
        self.sum_x = 0.0;
        self.sum_y = 0.0;
        self.sum_xy = 0.0;
        self.sum_xx = 0.0;
        self.sum_yy = 0.0;
        self.n_points = values.len();

        for (i, &y) in values.iter().enumerate() {
            let x = i as f64;
            self.sum_x += x;
            self.sum_y += y;
            self.sum_xy += x * y;
            self.sum_xx += x * x;
            self.sum_yy += y * y;
        }

        self.recompute_params(values, start_ts, interval_ns);
        self.fitted = true;
        metrics::histogram!("chronix_forecast_fit_duration_seconds", "model_type" => "linear_regression").record(_start.elapsed().as_secs_f64());
        Ok(())
    }

    fn predict(&self, horizon: usize) -> Result<ForecastResult, ForecastError> {
        let _start = std::time::Instant::now();
        if !self.fitted {
            return Err(ForecastError::NotFitted);
        }

        let ModelParams::LinearRegression {
            slope,
            intercept,
            residual_std,
            start_ts,
            interval_ns,
            ..
        } = &self.params
        else {
            return Err(ForecastError::InvalidInput(
                "unexpected model params variant".into(),
            ));
        };

        let z = 1.96;
        let last_ts =
            start_ts.saturating_add((self.n_points as i64 - 1).saturating_mul(*interval_ns));
        let mut values = Vec::with_capacity(horizon);
        let mut timestamps = Vec::with_capacity(horizon);
        let mut lower = Vec::with_capacity(horizon);
        let mut upper = Vec::with_capacity(horizon);

        // Centered sum of squares: Sxx = Σ(xi - x̄)² = sum_xx - n * x̄²
        let n_f = self.n_points as f64;
        let x_bar = (n_f - 1.0) / 2.0;
        let sxx_centered = (self.sum_xx - n_f * x_bar * x_bar).max(1.0);

        for h in 1..=horizon {
            let x = (self.n_points + h - 1) as f64;
            let ts = last_ts.saturating_add((h as i64).saturating_mul(*interval_ns));
            let pred = slope * x + intercept;
            let width =
                z * residual_std * (1.0 + 1.0 / n_f + (x - x_bar).powi(2) / sxx_centered).sqrt();
            values.push(pred);
            timestamps.push(ts);
            lower.push(pred - width);
            upper.push(pred + width);
        }

        metrics::histogram!("chronix_forecast_predict_duration_seconds", "model_type" => "linear_regression").record(_start.elapsed().as_secs_f64());
        Ok(ForecastResult {
            values,
            timestamps,
            confidence_lower: lower,
            confidence_upper: upper,
            confidence_level: 0.95,
        })
    }

    fn update(&mut self, _timestamp: i64, value: f64) -> Result<(), ForecastError> {
        if !self.fitted {
            return Err(ForecastError::NotFitted);
        }

        let x = self.n_points as f64;
        self.sum_x += x;
        self.sum_y += value;
        self.sum_xy += x * value;
        self.sum_xx += x * x;
        self.sum_yy += value * value;
        self.n_points += 1;

        // Recompute slope/intercept from running sums
        let ModelParams::LinearRegression {
            start_ts,
            interval_ns,
            ..
        } = &self.params
        else {
            return Err(ForecastError::InvalidInput(
                "unexpected model params variant".into(),
            ));
        };

        let start_ts = *start_ts;
        let interval_ns = *interval_ns;

        let n = self.n_points as f64;
        let mean_x = self.sum_x / n;
        let mean_y = self.sum_y / n;
        let ss_xx = self.sum_xx - n * mean_x * mean_x;
        let ss_xy = self.sum_xy - n * mean_x * mean_y;
        let ss_yy = self.sum_yy - n * mean_y * mean_y;
        let slope = if ss_xx.abs() > 1e-15 {
            ss_xy / ss_xx
        } else {
            0.0
        };
        let intercept = mean_y - slope * mean_x;

        // Incremental R² and residual_std from running sums
        let ss_res = (ss_yy - slope * ss_xy).max(0.0);
        let r_squared = if ss_yy.abs() > 1e-15 {
            1.0 - ss_res / ss_yy
        } else {
            // Zero variance in y — a perfect horizontal line explains all data.
            1.0
        };
        let residual_std = if self.n_points > 2 {
            (ss_res / (self.n_points - 2) as f64).sqrt()
        } else {
            0.0
        };

        self.params = ModelParams::LinearRegression {
            slope,
            intercept,
            r_squared,
            residual_std,
            start_ts,
            interval_ns,
        };

        Ok(())
    }

    fn model_type(&self) -> ModelType {
        ModelType::LinearRegression
    }

    fn params(&self) -> &ModelParams {
        &self.params
    }
}

impl crate::forecast::storage::ModelStore for LinearRegressionModel {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn perfectly_linear() {
        let ts: Vec<i64> = (0..100).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..100).map(|i| i as f64 * 3.0 + 5.0).collect();
        let mut model = LinearRegressionModel::new();
        model.fit(&ts, &vals).unwrap();

        let ModelParams::LinearRegression {
            slope,
            intercept,
            r_squared,
            ..
        } = model.params()
        else {
            panic!()
        };
        assert!((slope - 3.0).abs() < 1e-6);
        assert!((intercept - 5.0).abs() < 1e-6);
        assert!((r_squared - 1.0).abs() < 1e-6);

        let result = model.predict(5).unwrap();
        assert!((result.values[0] - (100.0 * 3.0 + 5.0)).abs() < 1e-6);
    }

    #[test]
    fn noisy_linear() {
        let ts: Vec<i64> = (0..200).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..200)
            .map(|i| {
                let noise = ((i * 7 + 3) % 11) as f64 - 5.0;
                i as f64 * 2.0 + 10.0 + noise
            })
            .collect();
        let mut model = LinearRegressionModel::new();
        model.fit(&ts, &vals).unwrap();

        let ModelParams::LinearRegression {
            slope, r_squared, ..
        } = model.params()
        else {
            panic!()
        };
        assert!((slope - 2.0).abs() < 1.0);
        assert!(*r_squared > 0.9);
    }

    #[test]
    fn predict_extrapolates() {
        let ts: Vec<i64> = (0..50).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..50).map(|i| i as f64 * 10.0).collect();
        let mut model = LinearRegressionModel::new();
        model.fit(&ts, &vals).unwrap();
        let result = model.predict(3).unwrap();
        assert!((result.values[0] - 500.0).abs() < 1.0);
        assert!((result.values[1] - 510.0).abs() < 1.0);
        assert!((result.values[2] - 520.0).abs() < 1.0);
    }

    #[test]
    fn online_update() {
        let ts: Vec<i64> = (0..10).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..10).map(|i| i as f64 * 5.0).collect();
        let mut model = LinearRegressionModel::new();
        model.fit(&ts, &vals).unwrap();
        model.update(10_000_000_000, 50.0).unwrap();

        let ModelParams::LinearRegression { slope, .. } = model.params() else {
            panic!()
        };
        assert!((slope - 5.0).abs() < 1.0);
    }

    #[test]
    fn insufficient_data() {
        let mut model = LinearRegressionModel::new();
        assert!(model.fit(&[0], &[1.0]).is_err());
    }
}
