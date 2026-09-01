//! Continuous forecast engine — re-fits forecast models as new data arrives.
//!
//! Tracks per-series point counts and re-fits when enough new data accumulates
//! or model accuracy degrades.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use crate::forecast::{
    ArimaModel, ForecastModel, ForecastResult, HoltLinearModel, HoltWintersModel,
    LinearRegressionModel, ModelType, SesModel,
};
use chronix_core::FieldValue;
use dashmap::DashMap;
use metrics::counter;
use tracing::{debug, warn};

use crate::error::{AnalyticsError, Result};

/// Configuration for continuous forecasting on a measurement.
#[derive(Debug, Clone)]
pub struct ContinuousForecastConfig {
    /// Measurement to forecast.
    pub measurement: String,
    /// Field to forecast (default: first f64 field).
    pub field: Option<String>,
    /// Model type to use.
    pub model_type: ModelType,
    /// Re-fit after this many new points.
    pub update_interval_points: usize,
    /// Maximum residual MAE multiplier before full re-fit.
    pub refit_mae_multiplier: f64,
    /// Whether continuous forecasting is enabled.
    pub enabled: bool,
    /// Minimum points needed for initial fit.
    pub min_fit_points: usize,
    /// Maximum buffer size for training data per series (prevents unbounded growth).
    pub max_buffer_size: usize,
    /// Minimum number of 1-step-ahead errors before the rolling MAE is
    /// treated as meaningful.
    ///
    /// A MAE computed from one or two residuals is noise, and it drives an
    /// expensive decision: `current > baseline × refit_mae_multiplier`
    /// triggers a **full model re-fit**. With a tiny window a single unlucky
    /// residual either causes a spurious re-fit — wasted CPU on a constrained
    /// gateway — or, if it lands in the *baseline*, inflates the threshold so
    /// that genuine drift never triggers one.
    pub min_mae_samples: usize,
}

impl ContinuousForecastConfig {
    /// Create a new config.
    #[must_use]
    pub fn new(measurement: impl Into<String>, model_type: ModelType) -> Self {
        Self {
            measurement: measurement.into(),
            field: None,
            model_type,
            update_interval_points: 1000,
            refit_mae_multiplier: 2.0,
            enabled: true,
            min_fit_points: 30,
            max_buffer_size: 10_000,
            min_mae_samples: 10,
        }
    }

    /// Set the field to monitor.
    #[must_use]
    pub fn with_field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }

    /// Set the update interval.
    #[must_use]
    pub fn with_update_interval(mut self, points: usize) -> Self {
        self.update_interval_points = points;
        self
    }

    /// Set minimum fit points.
    #[must_use]
    pub fn with_min_fit_points(mut self, n: usize) -> Self {
        self.min_fit_points = n;
        self
    }

    /// Set maximum buffer size per series.
    #[must_use]
    pub fn with_max_buffer_size(mut self, n: usize) -> Self {
        self.max_buffer_size = n;
        self
    }
}

/// Per-series forecast state.
struct ForecastState {
    timestamps: VecDeque<i64>,
    values: VecDeque<f64>,
    model: Box<dyn ForecastModel>,
    fitted: bool,
    points_since_fit: usize,
    baseline_mae: Option<f64>,
    last_fit_time: Option<Instant>,
    update_interval: usize,
    refit_multiplier: f64,
    /// See [`ContinuousForecastConfig::min_mae_samples`].
    min_mae_samples: usize,
    min_fit_points: usize,
    max_buffer_size: usize,
    /// Last 1-step-ahead prediction, used to compute true
    /// forecast error when the next actual value arrives.
    last_prediction: Option<f64>,
    /// Rolling window of recent absolute 1-step-ahead errors.
    recent_errors: VecDeque<f64>,
}

impl ForecastState {
    fn new(config: &ContinuousForecastConfig) -> Self {
        let model: Box<dyn ForecastModel> = create_model(&config.model_type);
        Self {
            timestamps: VecDeque::with_capacity(config.min_fit_points),
            values: VecDeque::with_capacity(config.min_fit_points),
            model,
            fitted: false,
            points_since_fit: 0,
            baseline_mae: None,
            last_fit_time: None,
            update_interval: config.update_interval_points,
            refit_multiplier: config.refit_mae_multiplier,
            min_mae_samples: config.min_mae_samples,
            min_fit_points: config.min_fit_points,
            max_buffer_size: config.max_buffer_size,
            last_prediction: None,
            recent_errors: VecDeque::with_capacity(50),
        }
    }
}

fn create_model(model_type: &ModelType) -> Box<dyn ForecastModel> {
    match model_type {
        ModelType::Ses => Box::new(SesModel::new(Some(0.3))),
        ModelType::HoltLinear => Box::new(HoltLinearModel::new(Some(0.3), Some(0.1), 1.0)),
        ModelType::HoltWinters => Box::new(HoltWintersModel::new(Some(0.3), Some(0.1), Some(0.1), Some(12), false)),
        ModelType::LinearRegression => Box::new(LinearRegressionModel::new()),
        // Use proper ARIMA(1,1,1) instead of silently falling back to SES.
        ModelType::Arima => Box::new(ArimaModel::new(1, 1, 1)),
        ModelType::Sarima => {
            warn!("SARIMA not yet supported in continuous mode, using ARIMA(1,1,1)");
            Box::new(ArimaModel::new(1, 1, 1))
        }
        ModelType::Custom(name) => {
            crate::registry::global_registry()
                .create_model_default(name)
                .unwrap_or_else(|e| {
                    warn!(plugin = %name, error = %e, "custom model plugin failed, falling back to SES");
                    Box::new(SesModel::new(Some(0.3)))
                })
        }
    }
}

/// Key for per-series state: `(measurement, canonical_tags_key)`.
type ForecastStateKey = (String, String);

/// Update result from processing a point.
#[derive(Debug, Clone)]
pub enum ForecastUpdate {
    /// Not enough data yet.
    Accumulating,
    /// Model was just fitted for the first time.
    InitialFit,
    /// Incremental update applied.
    Incremental,
    /// Full re-fit triggered (accuracy degraded).
    ReFit,
}

/// Continuous forecast engine — re-fits models as new data arrives.
pub struct ContinuousForecastEngine {
    configs: DashMap<String, ContinuousForecastConfig>,
    /// Per-series model state. Uses `Arc<Mutex>` so the DashMap shard
    /// lock is released before acquiring the per-series Mutex (avoids
    /// blocking other series in the same shard during expensive fit()).
    series_state: DashMap<ForecastStateKey, Arc<parking_lot::Mutex<ForecastState>>>,
}

impl ContinuousForecastEngine {
    /// Create a new continuous forecast engine.
    #[must_use]
    pub fn new() -> Self {
        Self {
            configs: DashMap::new(),
            series_state: DashMap::new(),
        }
    }

    /// Enable continuous forecasting for a measurement.
    pub fn enable(&self, config: ContinuousForecastConfig) -> Result<()> {
        if !config.enabled {
            return Err(AnalyticsError::Config("Config is disabled".into()));
        }
        let measurement = config.measurement.clone();
        self.configs.insert(measurement.clone(), config);
        debug!(measurement = %measurement, "Continuous forecasting enabled");
        Ok(())
    }

    /// Disable continuous forecasting for a measurement.
    pub fn disable(&self, measurement: &str) {
        self.configs.remove(measurement);
        self.series_state.retain(|k, _| k.0 != measurement);
    }

    /// Check if a measurement has continuous forecasting enabled.
    #[must_use]
    pub fn is_enabled(&self, measurement: &str) -> bool {
        self.configs.contains_key(measurement)
    }

    /// Process a new data point for a series.
    ///
    /// Returns the update type if the model was updated.
    pub fn process_point(
        &self,
        measurement: &str,
        tags: &BTreeMap<String, String>,
        fields: &BTreeMap<String, FieldValue>,
        timestamp: i64,
    ) -> Option<ForecastUpdate> {
        let config = self.configs.get(measurement)?;

        let value = extract_numeric_value(fields, config.field.as_deref())?;
        let series_key = compute_tags_key(tags);
        let key = (measurement.to_string(), series_key);

        let state_arc = self
            .series_state
            .entry(key)
            .or_insert_with(|| Arc::new(parking_lot::Mutex::new(ForecastState::new(&config))))
            .clone();

        let mut state = state_arc.lock();

        // Record 1-step-ahead forecast error if we had a prediction.
        if let Some(predicted) = state.last_prediction.take() {
            if value.is_finite() && predicted.is_finite() {
                let error = (predicted - value).abs();
                state.recent_errors.push_back(error);
                if state.recent_errors.len() > 50 {
                    state.recent_errors.pop_front();
                }
            }
        }

        state.timestamps.push_back(timestamp);
        state.values.push_back(value);

        // Evict oldest data if buffer exceeds max (O(1) per eviction)
        while state.timestamps.len() > state.max_buffer_size {
            state.timestamps.pop_front();
            state.values.pop_front();
        }

        if !state.fitted {
            if state.values.len() >= state.min_fit_points {
                let ts: Vec<i64> = state.timestamps.iter().copied().collect();
                let vals: Vec<f64> = state.values.iter().copied().collect();
                match state.model.fit(&ts, &vals) {
                    Ok(()) => {
                        state.fitted = true;
                        state.points_since_fit = 0;
                        state.last_fit_time = Some(Instant::now());
                        state.baseline_mae =
                            compute_mae_from_errors(&state.recent_errors, state.min_mae_samples);
                        // Store 1-step-ahead prediction for next arrival.
                        state.last_prediction = state
                            .model
                            .predict(1)
                            .ok()
                            .and_then(|r| r.values.first().copied());
                        debug!(measurement = %measurement, "Forecast model initial fit");
                        counter!("chronix_forecast_fit_total", "measurement" => measurement.to_string()).increment(1);
                        return Some(ForecastUpdate::InitialFit);
                    }
                    Err(e) => {
                        warn!(measurement = %measurement, error = %e, "Initial model fit failed");
                        return None;
                    }
                }
            }
            return Some(ForecastUpdate::Accumulating);
        }

        // Incremental update
        state.points_since_fit += 1;

        if let Err(e) = state.model.update(timestamp, value) {
            warn!(measurement = %measurement, error = %e, "Incremental update failed");
        }

        // Store 1-step-ahead prediction for next error calculation.
        state.last_prediction = state
            .model
            .predict(1)
            .ok()
            .and_then(|r| r.values.first().copied());

        // Check if we need a full re-fit
        if state.points_since_fit >= state.update_interval {
            let current_mae = compute_mae_from_errors(&state.recent_errors, state.min_mae_samples);

            // Set baseline MAE on first check after fit/refit.
            if state.baseline_mae.is_none() {
                state.baseline_mae = current_mae;
            }

            let needs_refit = match (current_mae, state.baseline_mae) {
                (Some(current), Some(baseline)) if baseline > 0.0 => {
                    current > baseline * state.refit_multiplier
                }
                (Some(_), Some(_)) => false, // baseline is zero — model is perfect
                _ => false,                  // not enough data for comparison yet
            };

            if needs_refit {
                // Truncate history to most recent max_buffer_size entries
                // so the re-fitted model trains on recent data, not the entire history.
                let retain = state.max_buffer_size;
                while state.timestamps.len() > retain {
                    state.timestamps.pop_front();
                    state.values.pop_front();
                }

                let ts: Vec<i64> = state.timestamps.iter().copied().collect();
                let vals: Vec<f64> = state.values.iter().copied().collect();
                match state.model.fit(&ts, &vals) {
                    Ok(()) => {
                        state.points_since_fit = 0;
                        state.last_fit_time = Some(Instant::now());
                        state.recent_errors.clear();
                        state.baseline_mae = None; // will be rebuilt from new errors
                                                   // Store 1-step-ahead prediction for next error calculation.
                        state.last_prediction = state
                            .model
                            .predict(1)
                            .ok()
                            .and_then(|r| r.values.first().copied());
                        debug!(measurement = %measurement, "Forecast model re-fit");
                        counter!("chronix_forecast_refit_total", "measurement" => measurement.to_string()).increment(1);
                        return Some(ForecastUpdate::ReFit);
                    }
                    Err(e) => {
                        warn!(measurement = %measurement, error = %e, "Re-fit failed");
                    }
                }
            }

            // Reset counter even if we didn't re-fit
            state.points_since_fit = 0;
            return Some(ForecastUpdate::Incremental);
        }

        Some(ForecastUpdate::Incremental)
    }

    /// Get a forecast for a series.
    pub fn predict(
        &self,
        measurement: &str,
        tags: &BTreeMap<String, String>,
        horizon: usize,
    ) -> Result<ForecastResult> {
        let series_key = compute_tags_key(tags);
        let key = (measurement.to_string(), series_key.clone());

        let state_entry = self
            .series_state
            .get(&key)
            .ok_or_else(|| AnalyticsError::NotConfigured(format!("{measurement}:{series_key}")))?;

        let state = state_entry.lock();
        if !state.fitted {
            return Err(AnalyticsError::Internal("Model not yet fitted".into()));
        }

        state.model.predict(horizon).map_err(AnalyticsError::from)
    }

    /// Number of configured measurements.
    #[must_use]
    pub fn config_count(&self) -> usize {
        self.configs.len()
    }

    /// Number of tracked series.
    #[must_use]
    pub fn series_count(&self) -> usize {
        self.series_state.len()
    }
}

impl Default for ContinuousForecastEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute MAE from the rolling window of 1-step-ahead forecast errors.
///
/// Each error was computed as |predicted(t) - actual(t)| where predicted(t) was
/// the model's 1-step-ahead forecast made *before* observing actual(t). This
/// avoids the previous bug of comparing multi-step future forecasts against
/// historical actuals at mismatched time offsets.
///
/// Returns `None` until at least `min_samples` residuals have accumulated —
/// see [`ContinuousForecastConfig::min_mae_samples`]. Both callers treat
/// `None` as "no comparison possible", which is the safe default: no re-fit is
/// triggered and no baseline is frozen from noise.
fn compute_mae_from_errors(errors: &VecDeque<f64>, min_samples: usize) -> Option<f64> {
    if errors.len() < min_samples.max(1) {
        return None;
    }
    let sum: f64 = errors.iter().sum();
    Some(sum / errors.len() as f64)
}

use crate::util::{compute_tags_key, extract_numeric_value};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mae_requires_a_minimum_sample_count() {
        let mut errors: VecDeque<f64> = VecDeque::new();
        // Below the threshold the MAE is noise and must not be reported.
        for _ in 0..9 {
            errors.push_back(1.0);
            assert_eq!(
                compute_mae_from_errors(&errors, 10),
                None,
                "MAE reported from only {} samples",
                errors.len()
            );
        }
        errors.push_back(1.0);
        assert_eq!(compute_mae_from_errors(&errors, 10), Some(1.0));
    }

    /// `min_samples = 0` must still require at least one residual rather than
    /// dividing by zero.
    #[test]
    fn mae_of_empty_window_is_none() {
        let errors: VecDeque<f64> = VecDeque::new();
        assert_eq!(compute_mae_from_errors(&errors, 0), None);
        assert_eq!(compute_mae_from_errors(&errors, 10), None);
    }

    #[test]
    fn default_config_sets_a_min_mae_sample_count() {
        let cfg = ContinuousForecastConfig::new("m", ModelType::Ses);
        assert!(
            cfg.min_mae_samples >= 2,
            "a 1-sample MAE must not be able to drive a re-fit"
        );
    }

    fn make_fields(value: f64) -> BTreeMap<String, FieldValue> {
        let mut fields = BTreeMap::new();
        fields.insert("value".to_string(), FieldValue::F64(value));
        fields
    }

    fn make_tags(host: &str) -> BTreeMap<String, String> {
        let mut tags = BTreeMap::new();
        tags.insert("host".to_string(), host.to_string());
        tags
    }

    #[test]
    fn enable_disable() {
        let engine = ContinuousForecastEngine::new();
        let config = ContinuousForecastConfig::new("cpu", ModelType::Ses).with_min_fit_points(5);
        engine.enable(config).unwrap();
        assert!(engine.is_enabled("cpu"));
        engine.disable("cpu");
        assert!(!engine.is_enabled("cpu"));
    }

    #[test]
    fn accumulates_before_fitting() {
        let engine = ContinuousForecastEngine::new();
        let config = ContinuousForecastConfig::new("cpu", ModelType::Ses).with_min_fit_points(10);
        engine.enable(config).unwrap();

        let tags = make_tags("host1");
        for i in 0..9 {
            let result = engine.process_point("cpu", &tags, &make_fields(50.0), i * 1000);
            assert!(matches!(result, Some(ForecastUpdate::Accumulating)));
        }
    }

    #[test]
    fn initial_fit() {
        let engine = ContinuousForecastEngine::new();
        let config = ContinuousForecastConfig::new("cpu", ModelType::Ses).with_min_fit_points(10);
        engine.enable(config).unwrap();

        let tags = make_tags("host1");
        let mut last_result = None;
        for i in 0..10 {
            last_result = engine.process_point(
                "cpu",
                &tags,
                &make_fields(50.0 + i as f64),
                i * 1_000_000_000,
            );
        }
        assert!(matches!(last_result, Some(ForecastUpdate::InitialFit)));
    }

    #[test]
    fn incremental_update_after_fit() {
        let engine = ContinuousForecastEngine::new();
        let config = ContinuousForecastConfig::new("cpu", ModelType::Ses)
            .with_min_fit_points(5)
            .with_update_interval(100);
        engine.enable(config).unwrap();

        let tags = make_tags("host1");
        for i in 0..5 {
            engine.process_point(
                "cpu",
                &tags,
                &make_fields(50.0 + i as f64),
                i * 1_000_000_000,
            );
        }
        // After fit, next point should be incremental
        let result = engine.process_point("cpu", &tags, &make_fields(55.0), 5_000_000_000);
        assert!(matches!(result, Some(ForecastUpdate::Incremental)));
    }

    #[test]
    fn predict_after_fit() {
        let engine = ContinuousForecastEngine::new();
        let config = ContinuousForecastConfig::new("cpu", ModelType::Ses).with_min_fit_points(10);
        engine.enable(config).unwrap();

        let tags = make_tags("host1");
        for i in 0..10 {
            engine.process_point(
                "cpu",
                &tags,
                &make_fields(50.0 + i as f64),
                i * 1_000_000_000,
            );
        }

        let forecast = engine.predict("cpu", &tags, 5).unwrap();
        assert_eq!(forecast.values.len(), 5);
        assert_eq!(forecast.timestamps.len(), 5);
    }

    #[test]
    fn predict_before_fit_errors() {
        let engine = ContinuousForecastEngine::new();
        let config = ContinuousForecastConfig::new("cpu", ModelType::Ses).with_min_fit_points(100);
        engine.enable(config).unwrap();

        let tags = make_tags("host1");
        for i in 0..5 {
            engine.process_point("cpu", &tags, &make_fields(50.0), i * 1000);
        }

        assert!(engine.predict("cpu", &tags, 5).is_err());
    }

    #[test]
    fn unconfigured_measurement_ignored() {
        let engine = ContinuousForecastEngine::new();
        let tags = make_tags("host1");
        let result = engine.process_point("cpu", &tags, &make_fields(50.0), 0);
        assert!(result.is_none());
    }

    #[test]
    fn disabled_config_rejected() {
        let engine = ContinuousForecastEngine::new();
        let mut config = ContinuousForecastConfig::new("cpu", ModelType::Ses);
        config.enabled = false;
        assert!(engine.enable(config).is_err());
    }

    #[test]
    fn multiple_series_independent() {
        let engine = ContinuousForecastEngine::new();
        let config = ContinuousForecastConfig::new("cpu", ModelType::Ses).with_min_fit_points(5);
        engine.enable(config).unwrap();

        let tags_a = make_tags("host1");
        let tags_b = make_tags("host2");

        for i in 0..5 {
            engine.process_point(
                "cpu",
                &tags_a,
                &make_fields(50.0 + i as f64),
                i * 1_000_000_000,
            );
            engine.process_point(
                "cpu",
                &tags_b,
                &make_fields(100.0 + i as f64),
                i * 1_000_000_000,
            );
        }

        assert_eq!(engine.series_count(), 2);

        let fa = engine.predict("cpu", &tags_a, 1).unwrap();
        let fb = engine.predict("cpu", &tags_b, 1).unwrap();
        // Series B should predict higher values
        assert!(fb.values[0] > fa.values[0]);
    }

    #[test]
    fn compute_mae_window_based() {
        // Verify that compute_mae uses a rolling window, not a single point.
        let engine = ContinuousForecastEngine::new();
        let config = ContinuousForecastConfig::new("cpu", ModelType::Ses).with_min_fit_points(10);
        engine.enable(config).unwrap();

        let tags = make_tags("test_mae");
        // Feed 10 stable points to trigger initial fit.
        for i in 0..10 {
            engine.process_point("cpu", &tags, &make_fields(50.0), i * 1_000_000_000);
        }

        // After fitting on constant data, MAE should be very small.
        let pred = engine.predict("cpu", &tags, 5);
        assert!(pred.is_ok());
        let values = &pred.unwrap().values;
        // SES on constant data should predict ~50.0.
        for v in values {
            assert!((v - 50.0).abs() < 1.0, "predicted {v} far from 50.0");
        }
    }
}
