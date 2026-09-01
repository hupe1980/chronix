//! Configuration types for the Chronix analytics Rust API.

/// Configuration for the `forecast()` API.
#[derive(Debug, Clone)]
pub struct ForecastConfig {
    /// Model type: `"ses"` (the default), `"holt"`, `"holt_winters"`,
    /// `"arima"`, `"sarima"`, `"linear_regression"`, or a name registered with
    /// the model registry. An unrecognised name is an error — see
    /// [`Chronix::auto_forecast`](crate::Chronix::auto_forecast) to have the
    /// model chosen instead of named.
    pub model: Option<String>,
    /// Confidence level for prediction intervals, strictly between 0 and 1.
    ///
    /// The models compute a 95 % interval and it is rescaled to this level,
    /// which is exact for the symmetric normal intervals they produce. For an
    /// empirical, possibly asymmetric interval, use
    /// [`QuantileForecaster`](chronix_analytics::forecast::QuantileForecaster).
    pub confidence: f64,
    /// Seasonal period for Holt-Winters / SARIMA (auto-detected if None).
    pub period: Option<usize>,
    /// ARIMA order (p,d,q) — defaults to (1,1,1).
    pub arima_order: Option<(usize, usize, usize)>,
    /// SARIMA seasonal order (P,D,Q,m) — defaults to (1,1,1,period).
    pub sarima_order: Option<(usize, usize, usize, usize)>,
}

impl Default for ForecastConfig {
    fn default() -> Self {
        Self {
            model: None,
            confidence: 0.95,
            period: None,
            arima_order: None,
            sarima_order: None,
        }
    }
}

/// Configuration for the `detect_anomalies()` API.
#[derive(Debug, Clone)]
pub struct AnomalyConfig {
    /// Detection method: "zscore", "modified_zscore", "iqr", "dynamic_threshold",
    /// "forecast_residual", "moving_average".
    pub method: Option<String>,
    /// Anomaly threshold (standard deviations or IQR multiplier).
    pub threshold: f64,
    /// Window size for moving-average-based detectors.
    pub window_size: Option<usize>,
}

impl Default for AnomalyConfig {
    fn default() -> Self {
        Self {
            method: None,
            threshold: 3.0,
            window_size: None,
        }
    }
}
