//! Accuracy tracking and model staleness monitoring.
//!
//! Tracks sliding-window MAPE, RMSE, MAE for active models, and triggers
//! re-fit when accuracy degrades below configurable thresholds.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::RwLock;

use crate::lifecycle::registry::AccuracyMetrics;

/// Configuration for the accuracy tracker.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AccuracyTrackerConfig {
    /// Window size for sliding-window metrics.  Default: 100.
    pub window_size: usize,
    /// MAPE threshold triggering re-fit.  Default: 0.10 (10%).
    pub refit_mape_threshold: f64,
    /// Number of consecutive windows exceeding threshold before re-fit.
    /// Default: 5.
    pub refit_window_count: usize,
    /// Maximum model age in seconds before staleness warning.  Default: 7 days.
    pub max_model_age_secs: i64,
}

impl Default for AccuracyTrackerConfig {
    fn default() -> Self {
        Self {
            window_size: 100,
            refit_mape_threshold: 0.10,
            refit_window_count: 5,
            max_model_age_secs: 7 * 24 * 3600,
        }
    }
}

/// Observation record for accuracy tracking.
#[derive(Debug, Clone)]
struct Observation {
    actual: f64,
    predicted: f64,
}

/// Per-model accuracy state.
#[derive(Debug)]
struct ModelState {
    observations: VecDeque<Observation>,
    config: AccuracyTrackerConfig,
    consecutive_bad_windows: AtomicUsize,
    /// Model creation timestamp (epoch seconds).
    created_at: i64,
    /// Observation count at last `needs_refit` evaluation.
    last_refit_check_count: AtomicUsize,
}

impl ModelState {
    fn new(config: AccuracyTrackerConfig, created_at: i64) -> Self {
        Self {
            observations: VecDeque::new(),
            config,
            consecutive_bad_windows: AtomicUsize::new(0),
            created_at,
            last_refit_check_count: AtomicUsize::new(0),
        }
    }

    fn add_observation(&mut self, actual: f64, predicted: f64) {
        self.observations
            .push_back(Observation { actual, predicted });
        let max_size = self.config.window_size * 10;
        while self.observations.len() > max_size {
            self.observations.pop_front();
        }
    }

    fn current_metrics(&self) -> Option<AccuracyMetrics> {
        if self.observations.len() < 2 {
            return None;
        }
        let start = self
            .observations
            .len()
            .saturating_sub(self.config.window_size);
        let actuals: Vec<f64> = self
            .observations
            .iter()
            .skip(start)
            .map(|o| o.actual)
            .collect();
        let preds: Vec<f64> = self
            .observations
            .iter()
            .skip(start)
            .map(|o| o.predicted)
            .collect();
        Some(AccuracyMetrics::compute(&actuals, &preds))
    }

    /// Check if the model needs refitting.
    ///
    /// Uses atomic counters so this can be called with `&self` (no
    /// exclusive access required), which lets `AccuracyTracker` use a
    /// read lock instead of a write lock.
    fn needs_refit(&self) -> bool {
        let current_count = self.observations.len();
        let prev = self.last_refit_check_count.load(Ordering::Relaxed);
        // Only update consecutive_bad_windows when new data has arrived.
        if current_count > prev {
            self.last_refit_check_count
                .store(current_count, Ordering::Relaxed);
            if let Some(m) = self.current_metrics() {
                if !m.mape.is_nan() && m.mape > self.config.refit_mape_threshold {
                    self.consecutive_bad_windows.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.consecutive_bad_windows.store(0, Ordering::Relaxed);
                }
            }
        }
        self.consecutive_bad_windows.load(Ordering::Relaxed) >= self.config.refit_window_count
    }

    fn is_stale(&self, now_epoch_secs: i64) -> bool {
        now_epoch_secs - self.created_at > self.config.max_model_age_secs
    }

    fn age_secs(&self, now_epoch_secs: i64) -> i64 {
        now_epoch_secs - self.created_at
    }
}

/// Key for a tracked model: `(measurement, model_name)`.
type ModelKey = (String, String);

/// Accuracy tracker that monitors multiple models.
///
/// Thread-safe — `models` wrapped in `RwLock`.
#[derive(Debug)]
pub struct AccuracyTracker {
    config: AccuracyTrackerConfig,
    models: RwLock<HashMap<ModelKey, ModelState>>,
}

impl AccuracyTracker {
    /// Creates a new accuracy tracker with the given configuration.
    pub fn new(config: AccuracyTrackerConfig) -> Self {
        Self {
            config,
            models: RwLock::new(HashMap::new()),
        }
    }

    /// Register a model for tracking.
    pub fn register_model(&self, measurement: &str, model_name: &str, created_at_epoch_secs: i64) {
        let key = (measurement.to_string(), model_name.to_string());
        self.models
            .write()
            .entry(key)
            .or_insert_with(|| ModelState::new(self.config.clone(), created_at_epoch_secs));
    }

    /// Add an observation (actual vs predicted) for a tracked model.
    pub fn add_observation(
        &self,
        measurement: &str,
        model_name: &str,
        actual: f64,
        predicted: f64,
    ) {
        let key = (measurement.to_string(), model_name.to_string());
        let mut models = self.models.write();
        if let Some(state) = models.get_mut(&key) {
            state.add_observation(actual, predicted);
            if let Some(m) = state.current_metrics() {
                metrics::gauge!("chronix_model_accuracy_mape").set(m.mape);
                metrics::gauge!("chronix_model_accuracy_rmse").set(m.rmse);
            }
        }
    }

    /// Get current accuracy metrics for a model.
    pub fn metrics(&self, measurement: &str, model_name: &str) -> Option<AccuracyMetrics> {
        let key = (measurement.to_string(), model_name.to_string());
        self.models.read().get(&key)?.current_metrics()
    }

    /// Check if a model needs re-fitting based on sustained accuracy degradation.
    pub fn needs_refit(&self, measurement: &str, model_name: &str) -> bool {
        let key = (measurement.to_string(), model_name.to_string());
        self.models
            .read()
            .get(&key)
            .is_some_and(ModelState::needs_refit)
    }

    /// Check if a model is stale (older than `max_model_age_secs`).
    pub fn is_stale(&self, measurement: &str, model_name: &str, now_epoch_secs: i64) -> bool {
        let key = (measurement.to_string(), model_name.to_string());
        self.models
            .read()
            .get(&key)
            .is_some_and(|s| s.is_stale(now_epoch_secs))
    }

    /// Get model age in seconds.
    pub fn model_age_secs(
        &self,
        measurement: &str,
        model_name: &str,
        now_epoch_secs: i64,
    ) -> Option<i64> {
        let key = (measurement.to_string(), model_name.to_string());
        self.models.read().get(&key).map(|s| {
            let age = s.age_secs(now_epoch_secs);
            metrics::gauge!("chronix_model_age_seconds", "measurement" => measurement.to_string())
                .set(age as f64);
            age
        })
    }

    /// Number of tracked models.
    pub fn tracked_model_count(&self) -> usize {
        self.models.read().len()
    }
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    fn make_tracker() -> AccuracyTracker {
        let config = AccuracyTrackerConfig {
            window_size: 10,
            refit_mape_threshold: 0.10,
            refit_window_count: 3,
            max_model_age_secs: 3600,
        };
        let tracker = AccuracyTracker::new(config);
        tracker.register_model("cpu", "ses_model", 1000);
        tracker
    }

    #[test]
    fn test_register_and_add_observation() {
        let tracker = make_tracker();
        tracker.add_observation("cpu", "ses_model", 100.0, 105.0);
        tracker.add_observation("cpu", "ses_model", 200.0, 195.0);

        let m = tracker.metrics("cpu", "ses_model").unwrap();
        assert!(m.mae > 0.0);
    }

    #[test]
    fn test_no_metrics_without_observations() {
        let tracker = make_tracker();
        assert!(tracker.metrics("cpu", "ses_model").is_none());
    }

    #[test]
    fn test_needs_refit_bad_accuracy() {
        let tracker = make_tracker();

        // Add observations with > 10% MAPE
        for i in 0..50 {
            let actual = 100.0 + i as f64;
            let pred = actual * 1.20; // 20% off
            tracker.add_observation("cpu", "ses_model", actual, pred);
        }

        // Check multiple times with new data each round to accumulate
        // consecutive_bad_windows (each call requires fresh observations).
        for round in 0..3 {
            for j in 0..5 {
                let actual = 200.0 + (round * 5 + j) as f64;
                let pred = actual * 1.20;
                tracker.add_observation("cpu", "ses_model", actual, pred);
            }
            tracker.needs_refit("cpu", "ses_model");
        }
        assert!(tracker.needs_refit("cpu", "ses_model"));
    }

    #[test]
    fn test_no_refit_good_accuracy() {
        let tracker = make_tracker();

        for i in 0..50 {
            let actual = 100.0 + i as f64;
            let pred = actual * 1.01; // 1% off
            tracker.add_observation("cpu", "ses_model", actual, pred);
        }

        assert!(!tracker.needs_refit("cpu", "ses_model"));
    }

    #[test]
    fn test_staleness() {
        let tracker = make_tracker();
        // created_at=1000, max_age=3600
        assert!(!tracker.is_stale("cpu", "ses_model", 2000)); // 1000s < 3600s
        assert!(tracker.is_stale("cpu", "ses_model", 5000)); // 4000s > 3600s
    }

    #[test]
    fn test_model_age() {
        let tracker = make_tracker();
        let age = tracker.model_age_secs("cpu", "ses_model", 2000).unwrap();
        assert_eq!(age, 1000);
    }

    #[test]
    fn test_nonexistent_model() {
        let tracker = make_tracker();
        assert!(tracker.metrics("memory", "foo").is_none());
        assert!(!tracker.is_stale("memory", "foo", 0));
    }

    #[test]
    fn test_tracked_model_count() {
        let tracker = make_tracker();
        assert_eq!(tracker.tracked_model_count(), 1);
        tracker.register_model("mem", "detector", 2000);
        assert_eq!(tracker.tracked_model_count(), 2);
    }
}
