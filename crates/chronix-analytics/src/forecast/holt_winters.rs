//! Holt-Winters (triple exponential smoothing) forecast model.

use crate::forecast::error::ForecastError;
use crate::forecast::result::{ForecastResult, ModelParams, ModelType};
use crate::forecast::traits::ForecastModel;
use crate::forecast::util::{median_interval, validate_input};

/// Safe division that avoids division by near-zero while preserving sign.
/// For multiplicative Holt-Winters, seasonal factors can be negative.
/// Using `y.max(1e-10)` clamps negative values to a small positive, which
/// corrupts the decomposition. Instead, we preserve the sign and only
/// guard against |y| < epsilon.
#[inline]
fn safe_div(x: f64, y: f64) -> f64 {
    const EPS: f64 = 1e-10;
    if y.abs() < EPS {
        x / EPS.copysign(y)
    } else {
        x / y
    }
}

/// Holt-Winters model with additive or multiplicative seasonality.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct HoltWintersModel {
    alpha: Option<f64>,
    beta: Option<f64>,
    gamma: Option<f64>,
    period: Option<usize>,
    multiplicative: bool,
    params: ModelParams,
    last_ts: i64,
    interval_ns: i64,
    /// Seasonal phase offset — the index into the seasonal vector for the
    /// first prediction step. Set to `n_train % period` during fit so that
    /// predictions continue from where training left off in the seasonal cycle.
    seasonal_phase: usize,
    fitted: bool,
}

impl HoltWintersModel {
    /// Creates a new Holt-Winters model.
    ///
    /// If `period` is `None`, it will be auto-detected via autocorrelation.
    /// If `multiplicative` is true, multiplicative seasonality is used.
    pub fn new(
        alpha: Option<f64>,
        beta: Option<f64>,
        gamma: Option<f64>,
        period: Option<usize>,
        multiplicative: bool,
    ) -> Self {
        Self {
            alpha,
            beta,
            gamma,
            period,
            multiplicative,
            params: ModelParams::HoltWinters {
                alpha: 0.0,
                beta: 0.0,
                gamma: 0.0,
                period: 0,
                level: 0.0,
                trend: 0.0,
                seasonal: Vec::new(),
                multiplicative: false,
                residual_std: 0.0,
                seasonal_phase: 0,
            },
            last_ts: 0,
            interval_ns: 0,
            seasonal_phase: 0,
            fitted: false,
        }
    }

    /// Auto-detect the seasonal period, or 1 for "no seasonality".
    ///
    /// One definition, in
    /// [`preprocess::detect_period`](crate::preprocess::detect_period): two
    /// implementations of one thing means one of them is wrong and nothing
    /// says which.
    fn detect_period(values: &[f64]) -> usize {
        crate::preprocess::detect_period(values, values.len() / 2).unwrap_or(1)
    }

    /// Simple double exponential smoothing (level + trend, no seasonality).
    /// Used as graceful degradation when training data is too short for
    /// full seasonal decomposition.
    fn fit_non_seasonal(
        &mut self,
        timestamps: &[i64],
        values: &[f64],
        _start: std::time::Instant,
    ) -> Result<(), ForecastError> {
        let alpha = self.alpha.unwrap_or(0.3);
        let beta = self.beta.unwrap_or(0.05);

        let mut level = values[0];
        let mut trend = if values.len() > 1 {
            values[1] - values[0]
        } else {
            0.0
        };
        let mut sse = 0.0;
        let mut count = 0;

        for &v in &values[1..] {
            let forecast = level + trend;
            let err = v - forecast;
            sse += err * err;
            count += 1;
            let new_level = alpha * v + (1.0 - alpha) * (level + trend);
            trend = beta * (new_level - level) + (1.0 - beta) * trend;
            level = new_level;
        }

        // The shared convention (as in `SesModel` and `HoltLinearModel`): divide the
        // residual sum of squares by the degrees of freedom, not the residual
        // count. This path estimates α and β plus the initial level, so
        // `n - 3`. Dividing by the raw count under-estimates the residual
        // spread, which narrows every prediction interval derived from it and
        // makes forecast-deviation triggers fire above their nominal rate.
        let dof = (values.len() as f64 - 3.0).max(1.0);
        let residual_std = if count > 0 { (sse / dof).sqrt() } else { 0.0 };

        self.interval_ns = median_interval(timestamps);
        self.last_ts = timestamps[timestamps.len() - 1];
        self.seasonal_phase = 0;

        self.params = ModelParams::HoltWinters {
            alpha,
            beta,
            gamma: 0.0,
            period: 1,
            level,
            trend,
            seasonal: vec![if self.multiplicative { 1.0 } else { 0.0 }],
            multiplicative: self.multiplicative,
            residual_std,
            seasonal_phase: 0,
        };
        self.fitted = true;
        metrics::histogram!("chronix_forecast_fit_duration_seconds", "model_type" => "holt_winters")
            .record(_start.elapsed().as_secs_f64());
        Ok(())
    }

    fn optimize_params(values: &[f64], period: usize, multiplicative: bool) -> (f64, f64, f64) {
        let result = crate::forecast::optimizer::minimize_nelder_mead(
            |p| Self::compute_mse(values, p[0], p[1], p[2], period, multiplicative),
            &[0.3, 0.05, 0.1],
            &[
                crate::forecast::optimizer::Bound::new(0.01, 0.99),
                crate::forecast::optimizer::Bound::new(0.001, 0.99),
                crate::forecast::optimizer::Bound::new(0.01, 0.99),
            ],
            500,
            1e-8,
        );
        (
            result.params[0].clamp(0.01, 0.99),
            result.params[1].clamp(0.001, 0.99),
            result.params[2].clamp(0.01, 0.99),
        )
    }

    fn compute_mse(
        values: &[f64],
        alpha: f64,
        beta: f64,
        gamma: f64,
        period: usize,
        multiplicative: bool,
    ) -> f64 {
        if values.len() < 2 * period {
            return f64::INFINITY;
        }

        let (mut level, mut trend, mut seasonal) = Self::initialize(values, period, multiplicative);
        let mut sse = 0.0;

        #[allow(clippy::needless_range_loop)]
        for i in period..values.len() {
            let si = i % period;
            let forecast = if multiplicative {
                (level + trend) * seasonal[si]
            } else {
                level + trend + seasonal[si]
            };
            let err = values[i] - forecast;
            sse += err * err;

            let new_level = if multiplicative {
                alpha * safe_div(values[i], seasonal[si]) + (1.0 - alpha) * (level + trend)
            } else {
                alpha * (values[i] - seasonal[si]) + (1.0 - alpha) * (level + trend)
            };

            let new_trend = beta * (new_level - level) + (1.0 - beta) * trend;

            seasonal[si] = if multiplicative {
                gamma * safe_div(values[i], new_level) + (1.0 - gamma) * seasonal[si]
            } else {
                gamma * (values[i] - new_level) + (1.0 - gamma) * seasonal[si]
            };

            level = new_level;
            trend = new_trend;
        }

        sse / (values.len() - period) as f64
    }

    fn initialize(values: &[f64], period: usize, multiplicative: bool) -> (f64, f64, Vec<f64>) {
        // Level: average of first season
        let level: f64 = values[..period].iter().sum::<f64>() / period as f64;

        // Trend: average of first differences between seasons
        let trend = if values.len() >= 2 * period {
            let mut sum = 0.0;
            for i in 0..period {
                sum += (values[period + i] - values[i]) / period as f64;
            }
            sum / period as f64
        } else {
            0.0
        };

        // Seasonal: deviation from level in first season
        let seasonal: Vec<f64> = if multiplicative {
            values[..period]
                .iter()
                .map(|&v| safe_div(v, level))
                .collect()
        } else {
            values[..period].iter().map(|&v| v - level).collect()
        };

        (level, trend, seasonal)
    }
}

impl ForecastModel for HoltWintersModel {
    #[tracing::instrument(skip_all, level = "debug")]
    fn fit(&mut self, timestamps: &[i64], values: &[f64]) -> Result<(), ForecastError> {
        let _start = std::time::Instant::now();
        validate_input(timestamps, values)?;
        let mut period = self.period.unwrap_or_else(|| Self::detect_period(values));

        // Graceful degradation: if data is insufficient for full seasonal
        // decomposition (< 2×period), fall back to non-seasonal (period=1)
        // instead of returning an error. This produces a level+trend model
        // that is still usable for short-term forecasting.
        if period >= 2 && values.len() < 2 * period {
            tracing::warn!(
                period,
                n = values.len(),
                "Insufficient data for seasonal Holt-Winters (need {}); \
                 falling back to non-seasonal model",
                2 * period,
            );
            period = 1;
        }
        if period < 2 {
            // Non-seasonal: simple double exponential smoothing (level + trend)
            return self.fit_non_seasonal(timestamps, values, _start);
        }

        let (alpha, beta, gamma) = match (self.alpha, self.beta, self.gamma) {
            (Some(a), Some(b), Some(g)) => (a, b, g),
            _ => Self::optimize_params(values, period, self.multiplicative),
        };

        let (mut level, mut trend, mut seasonal) =
            Self::initialize(values, period, self.multiplicative);
        let mut sse = 0.0;
        let mut count = 0;

        #[allow(clippy::needless_range_loop)]
        for i in period..values.len() {
            let si = i % period;
            let forecast = if self.multiplicative {
                (level + trend) * seasonal[si]
            } else {
                level + trend + seasonal[si]
            };
            let err = values[i] - forecast;
            sse += err * err;
            count += 1;

            let new_level = if self.multiplicative {
                alpha * safe_div(values[i], seasonal[si]) + (1.0 - alpha) * (level + trend)
            } else {
                alpha * (values[i] - seasonal[si]) + (1.0 - alpha) * (level + trend)
            };

            let new_trend = beta * (new_level - level) + (1.0 - beta) * trend;

            seasonal[si] = if self.multiplicative {
                gamma * safe_div(values[i], new_level) + (1.0 - gamma) * seasonal[si]
            } else {
                gamma * (values[i] - new_level) + (1.0 - gamma) * seasonal[si]
            };

            level = new_level;
            trend = new_trend;
        }

        // The same convention, seasonal case: α, β and γ are estimated along with
        // the initial level, trend and `period` seasonal states, so the
        // degrees of freedom are `n - (period + 3)`. `fit` guarantees
        // `n >= 2 * period` on this path, so the clamp only ever engages for
        // very large periods.
        let dof = (values.len() as f64 - period as f64 - 3.0).max(1.0);
        let residual_std = if count > 0 { (sse / dof).sqrt() } else { 0.0 };

        self.interval_ns = median_interval(timestamps);
        self.last_ts = timestamps[timestamps.len() - 1];
        self.seasonal_phase = values.len() % period;

        self.params = ModelParams::HoltWinters {
            alpha,
            beta,
            gamma,
            period,
            level,
            trend,
            seasonal,
            multiplicative: self.multiplicative,
            residual_std,
            seasonal_phase: self.seasonal_phase,
        };
        self.fitted = true;
        metrics::histogram!("chronix_forecast_fit_duration_seconds", "model_type" => "holt_winters").record(_start.elapsed().as_secs_f64());
        Ok(())
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn predict(&self, horizon: usize) -> Result<ForecastResult, ForecastError> {
        let _start = std::time::Instant::now();
        if !self.fitted {
            return Err(ForecastError::NotFitted);
        }

        let ModelParams::HoltWinters {
            alpha,
            beta,
            level,
            trend,
            seasonal,
            multiplicative,
            period,
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
            let si = (self.seasonal_phase + h - 1) % period;
            let forecast = if *multiplicative {
                (level + h as f64 * trend) * seasonal[si]
            } else {
                level + h as f64 * trend + seasonal[si]
            };

            // Exact recursive PI variance per Hyndman et al. (2008).
            // For additive Holt-Winters the prediction variance coefficients
            // follow the recurrence:
            //   c_0 = 1
            //   c_j = c_{j-1} + α(1 + β·j) + γ·𝟙[j mod m = 0]
            // The seasonal smoothing parameter γ contributes to error
            // propagation at each seasonal boundary.
            let mut var_sum = 0.0;
            let mut cj = 1.0;
            for j in 0..h {
                if j > 0 {
                    cj += alpha * (1.0 + beta * j as f64);
                    if j % period == 0 {
                        cj += self.gamma.unwrap_or(0.0);
                    }
                }
                var_sum += cj * cj;
            }
            let width = z * residual_std * var_sum.sqrt();
            values.push(forecast);
            timestamps.push(ts);
            lower.push(forecast - width);
            upper.push(forecast + width);
        }

        metrics::histogram!("chronix_forecast_predict_duration_seconds", "model_type" => "holt_winters").record(_start.elapsed().as_secs_f64());
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

        let ModelParams::HoltWinters {
            alpha,
            beta,
            gamma,
            ref mut level,
            ref mut trend,
            ref mut seasonal,
            multiplicative,
            period,
            ..
        } = self.params
        else {
            return Err(ForecastError::InvalidInput(
                "unexpected model params variant".into(),
            ));
        };

        // Determine which seasonal index to update
        let elapsed = timestamp - self.last_ts;
        let steps = (elapsed / self.interval_ns).max(1) as usize;
        let si = (self.seasonal_phase + steps - 1) % period;

        let new_level = if multiplicative {
            alpha * safe_div(value, seasonal[si]) + (1.0 - alpha) * (*level + *trend)
        } else {
            alpha * (value - seasonal[si]) + (1.0 - alpha) * (*level + *trend)
        };

        *trend = beta * (new_level - *level) + (1.0 - beta) * *trend;

        seasonal[si] = if multiplicative {
            gamma * safe_div(value, new_level) + (1.0 - gamma) * seasonal[si]
        } else {
            gamma * (value - new_level) + (1.0 - gamma) * seasonal[si]
        };

        *level = new_level;
        self.last_ts = timestamp;
        // Advance seasonal phase so subsequent predict/update calls use
        // the correct position in the seasonal cycle.
        self.seasonal_phase = (self.seasonal_phase + steps) % period;
        Ok(())
    }

    fn model_type(&self) -> ModelType {
        ModelType::HoltWinters
    }

    fn params(&self) -> &ModelParams {
        &self.params
    }
}

impl crate::forecast::storage::ModelStore for HoltWintersModel {}

#[cfg(test)]
mod tests {
    use super::*;

    fn seasonal_data(n: usize, period: usize) -> (Vec<i64>, Vec<f64>) {
        let ts: Vec<i64> = (0..n).map(|i| i as i64 * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..n)
            .map(|i| {
                let trend = i as f64 * 0.5;
                let season =
                    10.0 * (2.0 * std::f64::consts::PI * (i % period) as f64 / period as f64).sin();
                trend + season + 50.0
            })
            .collect();
        (ts, vals)
    }

    #[test]
    fn hw_additive_seasonal() {
        let (ts, vals) = seasonal_data(200, 24);
        let mut model = HoltWintersModel::new(Some(0.3), Some(0.1), Some(0.2), Some(24), false);
        model.fit(&ts, &vals).unwrap();
        let result = model.predict(24).unwrap();
        assert_eq!(result.values.len(), 24);
        // Predictions should show seasonal pattern
        let range = result.values.iter().copied().fold(f64::INFINITY, f64::min);
        let max = result
            .values
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        assert!(max - range > 1.0); // Should have variation from seasonality
    }

    #[test]
    fn hw_multiplicative() {
        let (ts, vals) = seasonal_data(200, 12);
        // Shift values to be strictly positive for multiplicative
        let vals: Vec<f64> = vals.iter().map(|v| v + 100.0).collect();
        let mut model = HoltWintersModel::new(Some(0.3), Some(0.1), Some(0.2), Some(12), true);
        model.fit(&ts, &vals).unwrap();
        let result = model.predict(12).unwrap();
        assert_eq!(result.values.len(), 12);
    }

    #[test]
    fn hw_auto_period() {
        let (ts, vals) = seasonal_data(200, 7);
        let mut model = HoltWintersModel::new(None, None, None, None, false);
        // This may not detect period=7 exactly, but should fit
        let result = model.fit(&ts, &vals);
        // If auto-detection fails (period < 2), it returns InvalidParams
        // Otherwise it should succeed
        if result.is_ok() {
            let pred = model.predict(7).unwrap();
            assert_eq!(pred.values.len(), 7);
        }
    }

    #[test]
    fn hw_insufficient_data_degrades_gracefully() {
        // period=24 but only 10 points → should fall back to non-seasonal
        let mut model = HoltWintersModel::new(None, None, None, Some(24), false);
        let ts: Vec<i64> = (0..10).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..10).map(|i| 50.0 + i as f64 * 2.0).collect();
        model.fit(&ts, &vals).unwrap(); // should NOT error
        let result = model.predict(5).unwrap();
        assert_eq!(result.values.len(), 5);
        // Trend should be roughly increasing
        assert!(result.values[4] > result.values[0]);
    }

    #[test]
    fn hw_non_seasonal_predicts_trend() {
        // Only 5 points with clear upward trend, no period
        let ts: Vec<i64> = (0..5).map(|i| i * 1_000_000_000).collect();
        let vals = vec![10.0, 20.0, 30.0, 40.0, 50.0];
        let mut model = HoltWintersModel::new(Some(0.8), Some(0.2), None, Some(1), false);
        model.fit(&ts, &vals).unwrap();
        let result = model.predict(3).unwrap();
        assert_eq!(result.values.len(), 3);
        // Should continue upward
        assert!(result.values[0] > 45.0);
    }

    #[test]
    fn hw_update() {
        let (ts, vals) = seasonal_data(100, 12);
        let mut model = HoltWintersModel::new(Some(0.3), Some(0.1), Some(0.2), Some(12), false);
        model.fit(&ts, &vals).unwrap();
        model.update(100_000_000_000, 200.0).unwrap();
        let ModelParams::HoltWinters { level, .. } = model.params() else {
            panic!()
        };
        assert!(*level > 50.0);
    }

    #[test]
    fn hw_multiplicative_negative_seasonal_factors() {
        // Multiplicative HW with data that crosses zero — seasonal factors
        // can become negative. The old `.max(1e-10)` guard would clamp
        // negative factors to a small positive value, corrupting the model.
        let period = 4;
        let n = 80;
        let ts: Vec<i64> = (0..n).map(|i| i as i64 * 1_000_000_000).collect();
        // Pattern: values oscillate around a rising baseline, crossing zero
        let vals: Vec<f64> = (0..n)
            .map(|i| {
                let base = 5.0 + i as f64 * 0.2;
                let seasonal = match i % period {
                    0 => 1.2,
                    1 => 0.8,
                    2 => -0.3, // negative seasonal factor when level > 0
                    3 => 1.3,
                    _ => unreachable!(),
                };
                base * seasonal
            })
            .collect();
        let mut model = HoltWintersModel::new(Some(0.3), Some(0.1), Some(0.2), Some(period), true);
        model.fit(&ts, &vals).unwrap();
        let result = model.predict(period).unwrap();
        assert_eq!(result.values.len(), period);
        // The third forecast (index 2) should be negative, matching the
        // negative seasonal pattern. With the old `.max(1e-10)` bug, all
        // values would be positive.
        assert!(
            result.values[2] < 0.0,
            "Expected negative forecast at seasonal index 2, got {}",
            result.values[2]
        );
        // Other values should be positive
        assert!(result.values[0] > 0.0);
        assert!(result.values[1] > 0.0);
        assert!(result.values[3] > 0.0);
    }

    #[test]
    fn safe_div_preserves_sign() {
        assert!((safe_div(10.0, -0.5) - -20.0).abs() < 1e-6);
        assert!((safe_div(10.0, 0.5) - 20.0).abs() < 1e-6);
        // Near-zero positive → large positive
        assert!(safe_div(1.0, 1e-15) > 0.0);
        // Near-zero negative → large negative
        assert!(safe_div(1.0, -1e-15) < 0.0);
    }
}
