//! Forecast result types and model metadata.

/// Type of forecast model.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ModelType {
    /// Simple Exponential Smoothing.
    Ses,
    /// Holt's Linear Trend (double exponential smoothing).
    HoltLinear,
    /// Holt-Winters (triple exponential smoothing with seasonality).
    HoltWinters,
    /// ARIMA(p,d,q) — autoregressive integrated moving average.
    Arima,
    /// SARIMA(p,d,q)(P,D,Q)m — seasonal ARIMA.
    Sarima,
    /// Ordinary Least Squares linear regression.
    LinearRegression,
    /// Custom plugin model identified by name.
    Custom(String),
}

/// Forecast result with predictions and confidence intervals.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ForecastResult {
    /// Predicted values.
    pub values: Vec<f64>,
    /// Predicted timestamps (nanoseconds).
    pub timestamps: Vec<i64>,
    /// Lower confidence bound.
    pub confidence_lower: Vec<f64>,
    /// Upper confidence bound.
    pub confidence_upper: Vec<f64>,
    /// Confidence level (e.g. 0.95).
    pub confidence_level: f64,
}

/// Fitted model parameters — variant per model type.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ModelParams {
    /// Simple Exponential Smoothing parameters.
    Ses {
        /// Smoothing factor α ∈ (0, 1).
        alpha: f64,
        /// Last smoothed level.
        level: f64,
        /// Standard deviation of in-sample residuals.
        residual_std: f64,
    },
    /// Holt's Linear Trend parameters.
    HoltLinear {
        /// Level smoothing factor α.
        alpha: f64,
        /// Trend smoothing factor β.
        beta: f64,
        /// Last smoothed level.
        level: f64,
        /// Last smoothed trend.
        trend: f64,
        /// Damping factor ϕ (1.0 = undamped).
        phi: f64,
        /// Standard deviation of in-sample residuals.
        residual_std: f64,
    },
    /// Holt-Winters (triple exponential smoothing) parameters.
    HoltWinters {
        /// Level smoothing factor α.
        alpha: f64,
        /// Trend smoothing factor β.
        beta: f64,
        /// Seasonal smoothing factor γ.
        gamma: f64,
        /// Seasonal period length (number of observations per cycle).
        period: usize,
        /// Last smoothed level.
        level: f64,
        /// Last smoothed trend.
        trend: f64,
        /// Seasonal indices (one per period position).
        seasonal: Vec<f64>,
        /// Whether multiplicative (true) or additive (false) seasonality is used.
        multiplicative: bool,
        /// Standard deviation of in-sample residuals.
        residual_std: f64,
        /// Phase offset into the seasonal vector for the first prediction step.
        /// Set to `n_train % period` during fit so predictions continue the cycle.
        #[serde(default)]
        seasonal_phase: usize,
    },
    /// ARIMA(p,d,q) parameters.
    Arima {
        /// Autoregressive order.
        p: usize,
        /// Differencing order.
        d: usize,
        /// Moving average order.
        q: usize,
        /// Autoregressive coefficients φ₁…φₚ.
        ar_coeffs: Vec<f64>,
        /// Moving average coefficients θ₁…θ_q.
        ma_coeffs: Vec<f64>,
        /// Mean of the differenced series, added back to every forecast.
        ///
        /// Zero when the model carries no constant — see
        /// [`ArimaModel`](crate::forecast::ArimaModel) for when one is
        /// included.
        constant: f64,
        /// Standard deviation of in-sample residuals.
        residual_std: f64,
        /// Effective AR order after Burg estimation.
        /// May be less than `p` if the algorithm terminated early
        /// due to near-constant input data.
        #[serde(default)]
        effective_ar_order: Option<usize>,
    },
    /// Seasonal ARIMA (p,d,q)(P,D,Q)m parameters.
    Sarima {
        /// Non-seasonal AR order.
        p: usize,
        /// Non-seasonal differencing order.
        d: usize,
        /// Non-seasonal MA order.
        q: usize,
        /// Seasonal AR order P.
        sp: usize,
        /// Seasonal differencing order D.
        sd: usize,
        /// Seasonal MA order Q.
        sq: usize,
        /// Seasonal period m.
        m: usize,
        /// Non-seasonal AR coefficients.
        ar_coeffs: Vec<f64>,
        /// Non-seasonal MA coefficients.
        ma_coeffs: Vec<f64>,
        /// Seasonal AR coefficients Φ₁…Φ_P.
        sar_coeffs: Vec<f64>,
        /// Seasonal MA coefficients Θ₁…Θ_Q.
        sma_coeffs: Vec<f64>,
        /// Mean of the fully differenced series, added back to every forecast.
        constant: f64,
        /// Standard deviation of in-sample residuals.
        residual_std: f64,
    },
    /// Linear regression parameters.
    LinearRegression {
        /// Regression slope (change per unit index).
        slope: f64,
        /// Regression intercept.
        intercept: f64,
        /// Coefficient of determination R².
        r_squared: f64,
        /// Standard deviation of in-sample residuals.
        residual_std: f64,
        /// Start timestamp of training data (nanoseconds).
        start_ts: i64,
        /// Observation interval (nanoseconds).
        interval_ns: i64,
    },
    /// Opaque parameters for a custom plugin model.
    Custom {
        /// Plugin model name.
        name: String,
        /// Opaque serialized parameter data (interpreted by the plugin).
        data: Vec<u8>,
    },
}

impl ForecastResult {
    /// Rescale the prediction interval to a different confidence level.
    ///
    /// Every model in this crate builds a **symmetric normal** interval —
    /// `value ± z·σ_h`, with `σ_h` whatever the model's horizon-dependent
    /// error scale is — so changing the level is exactly a change of `z`, and
    /// this rescaling is not an approximation of the model's own answer at
    /// that level: it *is* that answer.
    ///
    /// That is only true because the interval is symmetric and normal. It is
    /// not true of the empirical residual quantiles from
    /// [`QuantileForecaster`](crate::forecast::QuantileForecaster), which is
    /// where to go when the residuals are skewed or the tails matter more than
    /// the shape.
    ///
    /// # Errors
    ///
    /// [`ForecastError::InvalidParams`](crate::forecast::ForecastError::InvalidParams)
    /// when `level` is not in `(0, 1)`, or when the result's own
    /// `confidence_level` is not, which would make the ratio meaningless.
    pub fn with_confidence(
        mut self,
        level: f64,
    ) -> Result<Self, crate::forecast::error::ForecastError> {
        use crate::forecast::error::ForecastError;
        use crate::forecast::util::z_for_confidence;

        let invalid = |name: &'static str, value: f64| ForecastError::InvalidParams {
            name,
            value: value.to_string(),
            reason: "confidence level must be strictly between 0 and 1",
        };
        let target = z_for_confidence(level).ok_or_else(|| invalid("level", level))?;
        let current = z_for_confidence(self.confidence_level)
            .ok_or_else(|| invalid("confidence_level", self.confidence_level))?;

        let ratio = target / current;
        for h in 0..self.values.len() {
            let (Some(lo), Some(hi)) = (
                self.confidence_lower.get(h).copied(),
                self.confidence_upper.get(h).copied(),
            ) else {
                continue;
            };
            let point = self.values[h];
            self.confidence_lower[h] = point - (point - lo) * ratio;
            self.confidence_upper[h] = point + (hi - point) * ratio;
        }
        self.confidence_level = level;
        Ok(self)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn sample() -> ForecastResult {
        ForecastResult {
            values: vec![10.0, 20.0],
            timestamps: vec![1, 2],
            confidence_lower: vec![8.0, 16.0],
            confidence_upper: vec![12.0, 24.0],
            confidence_level: 0.95,
        }
    }

    #[test]
    fn normal_quantile_matches_the_textbook_values() {
        use crate::forecast::util::normal_quantile;
        for (p, expected) in [
            (0.5, 0.0),
            (0.975, 1.959_963_984_540_054),
            (0.995, 2.575_829_303_548_901),
            (0.9, 1.281_551_565_544_6),
            (0.025, -1.959_963_984_540_054),
            // Deep in the tail, where the central rational fit is not used.
            (1e-6, -4.753_424_308_822_899),
        ] {
            let got = normal_quantile(p);
            assert!(
                (got - expected).abs() < 1e-6,
                "Φ⁻¹({p}) = {got}, expected {expected}"
            );
        }
        assert!(normal_quantile(0.0).is_infinite());
        assert!(normal_quantile(1.0).is_infinite());
        assert!(normal_quantile(-0.1).is_nan());
        assert!(normal_quantile(1.1).is_nan());
    }

    #[test]
    fn z_for_confidence_is_the_two_sided_multiplier() {
        use crate::forecast::util::z_for_confidence;
        assert!((z_for_confidence(0.95).unwrap() - 1.959_963_98).abs() < 1e-6);
        assert!((z_for_confidence(0.99).unwrap() - 2.575_829_30).abs() < 1e-6);
        assert!((z_for_confidence(0.80).unwrap() - 1.281_551_57).abs() < 1e-6);
        assert!(z_for_confidence(0.0).is_none());
        assert!(z_for_confidence(1.0).is_none());
    }

    #[test]
    fn widening_the_interval_keeps_the_point_forecast() {
        let widened = sample().with_confidence(0.99).unwrap();
        assert_eq!(widened.values, vec![10.0, 20.0]);
        assert_eq!(widened.confidence_level, 0.99);
        // The half-width scales by z(0.99)/z(0.95) ≈ 1.31417. Taken from
        // `z_for_confidence` rather than from the textbook constants: this
        // test is about the rescaling, and the accuracy of Φ⁻¹ itself is
        // pinned separately above.
        use crate::forecast::util::z_for_confidence;
        let ratio = z_for_confidence(0.99).unwrap() / z_for_confidence(0.95).unwrap();
        assert!((ratio - 1.314_17).abs() < 1e-4, "ratio = {ratio}");
        assert!((widened.confidence_upper[0] - (10.0 + 2.0 * ratio)).abs() < 1e-9);
        assert!((widened.confidence_lower[0] - (10.0 - 2.0 * ratio)).abs() < 1e-9);
        assert!((widened.confidence_upper[1] - (20.0 + 4.0 * ratio)).abs() < 1e-9);
    }

    #[test]
    fn narrowing_and_widening_round_trip() {
        let round = sample()
            .with_confidence(0.5)
            .unwrap()
            .with_confidence(0.95)
            .unwrap();
        for h in 0..2 {
            assert!((round.confidence_lower[h] - sample().confidence_lower[h]).abs() < 1e-9);
            assert!((round.confidence_upper[h] - sample().confidence_upper[h]).abs() < 1e-9);
        }
    }

    #[test]
    fn an_out_of_range_level_is_rejected() {
        assert!(sample().with_confidence(0.0).is_err());
        assert!(sample().with_confidence(1.0).is_err());
        assert!(sample().with_confidence(-0.5).is_err());
    }
}
