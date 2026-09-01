//! Streaming anomaly detection — subscribes to CDC writes and runs
//! per-series anomaly detection in real time.
//!
//! ## Architecture
//!
//! ```text
//! CDC PointWritten events
//!       │
//!       ▼
//! StreamingAnomalyEngine
//!   ├── Per-measurement config (detector type, threshold, field)
//!   ├── Per-series fitted AnomalyDetector (DashMap)
//!   ├── On each point: detect_point() → AnomalyScore
//!   └── Results emitted as scored records
//! ```

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use crate::anomaly::{
    AnomalyDetector, AnomalyScore, CusumDetector, DetectorType, DynamicThresholdDetector,
    IqrDetector, ModifiedZScoreDetector, MovingAverageResidualDetector, ZScoreDetector,
};
use chronix_core::FieldValue;
use dashmap::DashMap;
use metrics::counter;
use parking_lot::RwLock;
use tracing::{debug, trace, warn};

use crate::error::{AnalyticsError, Result};

/// Configuration for streaming anomaly detection on a measurement.
#[derive(Debug, Clone)]
pub struct StreamingAnomalyConfig {
    /// Measurement to monitor.
    pub measurement: String,
    /// Which field to watch (default: first f64 field).
    pub field: Option<String>,
    /// Detector type to use.
    pub detector_type: DetectorType,
    /// Anomaly threshold (meaning depends on detector type).
    pub threshold: f64,
    /// Whether streaming is enabled.
    pub enabled: bool,
    /// Minimum data points before detection starts.
    pub min_fit_points: usize,
    /// Maximum buffer size for training data per series (prevents unbounded growth).
    pub max_buffer_size: usize,
    /// Re-fit the detector every N new observations for concept drift adaptation.
    /// Set to `0` to disable periodic re-fit (only fit once).
    pub refit_interval: usize,
}

impl StreamingAnomalyConfig {
    /// Create a new config.
    #[must_use]
    pub fn new(
        measurement: impl Into<String>,
        detector_type: DetectorType,
        threshold: f64,
    ) -> Self {
        Self {
            measurement: measurement.into(),
            field: None,
            detector_type,
            threshold,
            enabled: true,
            min_fit_points: 30,
            max_buffer_size: 10_000,
            refit_interval: 1_000,
        }
    }

    /// Set the field to monitor.
    #[must_use]
    pub fn with_field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
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

    /// Set re-fit interval (number of new observations between re-fits).
    /// Set to `0` to disable periodic re-fit.
    #[must_use]
    pub fn with_refit_interval(mut self, n: usize) -> Self {
        self.refit_interval = n;
        self
    }
}

/// Per-series state: training buffer + fitted detector.
struct SeriesState {
    timestamps: VecDeque<i64>,
    values: VecDeque<f64>,
    detector: Box<dyn AnomalyDetector>,
    fitted: bool,
    min_fit_points: usize,
    max_buffer_size: usize,
    refit_interval: usize,
    /// Counter of new observations since the last fit.
    observations_since_fit: usize,
}

impl SeriesState {
    fn new(config: &StreamingAnomalyConfig) -> Self {
        let detector: Box<dyn AnomalyDetector> = match &config.detector_type {
            DetectorType::ZScore => Box::new(ZScoreDetector::new(Some(config.threshold))),
            DetectorType::ModifiedZScore => {
                Box::new(ModifiedZScoreDetector::new(Some(config.threshold)))
            }
            DetectorType::Iqr => Box::new(IqrDetector::new(Some(config.threshold))),
            DetectorType::MovingAverageResidual => {
                Box::new(MovingAverageResidualDetector::new(
                    Some(30.min(config.min_fit_points)),
                    Some(config.threshold),
                ))
            }
            DetectorType::DynamicThreshold => {
                Box::new(DynamicThresholdDetector::new(Some(100), Some(config.threshold)))
            }
            DetectorType::Cusum => {
                Box::new(CusumDetector::new(None, None))
            }
            // ForecastResidual requires a forecast model — fallback to ZScore
            DetectorType::ForecastResidual => {
                warn!(
                    measurement = config.measurement,
                    "ForecastResidual detector type not available in streaming mode, \
                     falling back to ZScore"
                );
                Box::new(ZScoreDetector::new(Some(config.threshold)))
            }
            DetectorType::Custom(name) => {
                crate::registry::global_registry()
                    .create_detector_default(name)
                    .unwrap_or_else(|e| {
                        warn!(plugin = %name, error = %e, "custom detector plugin failed, falling back to ZScore");
                        Box::new(ZScoreDetector::new(Some(config.threshold)))
                    })
            }
        };
        Self {
            timestamps: VecDeque::with_capacity(config.min_fit_points),
            values: VecDeque::with_capacity(config.min_fit_points),
            detector,
            fitted: false,
            min_fit_points: config.min_fit_points,
            max_buffer_size: config.max_buffer_size,
            refit_interval: config.refit_interval,
            observations_since_fit: 0,
        }
    }
}

/// Key for per-series state: `(measurement, canonical_tags_key)`.
type SeriesStateKey = (String, String);

/// Real-time streaming anomaly detection engine.
///
/// Processes CDC write events and runs per-series anomaly detection,
/// emitting `AnomalyScore` results.
pub struct StreamingAnomalyEngine {
    /// Per-measurement configurations.
    configs: DashMap<String, StreamingAnomalyConfig>,
    /// Per-series detector state. Uses `Arc<Mutex>` so the DashMap shard
    /// lock is released before acquiring the per-series Mutex (avoids
    /// blocking other series in the same shard during expensive fit()).
    series_state: DashMap<SeriesStateKey, Arc<parking_lot::Mutex<SeriesState>>>,
    /// Emitted anomaly scores (for querying/persistence).
    scores: RwLock<VecDeque<ScoredAnomaly>>,
    /// Maximum stored scores.
    max_scores: usize,
}

/// A scored anomaly result from streaming detection.
#[derive(Debug, Clone)]
pub struct ScoredAnomaly {
    /// The anomaly score from the detector.
    pub score: AnomalyScore,
    /// The measurement this was detected on.
    pub measurement: String,
    /// Tags of the series.
    pub tags: BTreeMap<String, String>,
}

impl StreamingAnomalyEngine {
    /// Create a new streaming anomaly engine.
    #[must_use]
    pub fn new() -> Self {
        Self {
            configs: DashMap::new(),
            series_state: DashMap::new(),
            scores: RwLock::new(VecDeque::new()),
            max_scores: 100_000,
        }
    }

    /// Enable streaming anomaly detection for a measurement.
    pub fn enable(&self, config: StreamingAnomalyConfig) -> Result<()> {
        if !config.enabled {
            return Err(AnalyticsError::Config("Config is disabled".into()));
        }
        let measurement = config.measurement.clone();
        self.configs.insert(measurement.clone(), config);
        debug!(measurement = %measurement, "Streaming anomaly detection enabled");
        Ok(())
    }

    /// Disable streaming anomaly detection for a measurement.
    pub fn disable(&self, measurement: &str) {
        self.configs.remove(measurement);
        // Remove all series state for this measurement
        self.series_state.retain(|k, _| k.0 != measurement);
        debug!(measurement = %measurement, "Streaming anomaly detection disabled");
    }

    /// Check if a measurement has streaming anomaly detection enabled.
    #[must_use]
    pub fn is_enabled(&self, measurement: &str) -> bool {
        self.configs.contains_key(measurement)
    }

    /// Process a CDC write event.
    ///
    /// Returns the anomaly score if detection was performed (i.e., the
    /// series has enough data and is configured).
    pub fn process_write(
        &self,
        measurement: &str,
        tags: &BTreeMap<String, String>,
        fields: &BTreeMap<String, FieldValue>,
        timestamp: i64,
    ) -> Option<ScoredAnomaly> {
        let config = self.configs.get(measurement)?;

        // Extract numeric value from fields
        let value = extract_numeric_value(fields, config.field.as_deref())?;

        // Compute series key for state lookup
        let series_key = compute_tags_key(tags);
        let key = (measurement.to_string(), series_key);

        // Get or create series state — clone the Arc to release the
        // DashMap shard lock before acquiring the per-series Mutex.
        let state_arc = self
            .series_state
            .entry(key)
            .or_insert_with(|| Arc::new(parking_lot::Mutex::new(SeriesState::new(&config))))
            .clone();

        let mut state = state_arc.lock();

        state.observations_since_fit += 1;

        // Evict oldest data if buffer is full (O(1) per eviction)
        while state.timestamps.len() > state.max_buffer_size {
            state.timestamps.pop_front();
            state.values.pop_front();
        }

        // Fit if we have enough data and haven't fitted yet,
        // or re-fit periodically for concept drift adaptation.
        let should_fit = if !state.fitted {
            state.values.len() >= state.min_fit_points
        } else {
            // Periodic re-fit every `refit_interval` new observations
            state.refit_interval > 0 && state.observations_since_fit >= state.refit_interval
        };

        if should_fit {
            let ts: Vec<i64> = state.timestamps.iter().copied().collect();
            let vals: Vec<f64> = state.values.iter().copied().collect();
            match state.detector.fit(&ts, &vals) {
                Ok(()) => {
                    state.fitted = true;
                    state.observations_since_fit = 0;
                    debug!(
                        measurement = %measurement,
                        points = state.values.len(),
                        "Anomaly detector fitted"
                    );
                }
                Err(e) => {
                    warn!(
                        measurement = %measurement,
                        error = %e,
                        "Failed to fit anomaly detector"
                    );
                    return None;
                }
            }
        }

        // Accumulate the current point AFTER fitting so that
        // the detector is trained on strictly historical data. The point
        // is available for future refit cycles but does NOT participate
        // in the model that scores it — eliminating look-ahead bias.
        state.timestamps.push_back(timestamp);
        state.values.push_back(value);

        // Detect if fitted
        if state.fitted {
            match state.detector.detect_point(timestamp, value) {
                Ok(score) => {
                    if score.is_anomaly {
                        counter!(
                            "chronix_streaming_anomaly_detected_total",
                            "measurement" => measurement.to_string()
                        )
                        .increment(1);
                    }

                    let scored = ScoredAnomaly {
                        score,
                        measurement: measurement.to_string(),
                        tags: tags.clone(),
                    };

                    // Store the score
                    let mut scores = self.scores.write();
                    if scores.len() >= self.max_scores {
                        scores.pop_front();
                    }
                    scores.push_back(scored.clone());

                    trace!(
                        measurement = %measurement,
                        score = scored.score.score,
                        is_anomaly = scored.score.is_anomaly,
                        "Anomaly detection result"
                    );

                    return Some(scored);
                }
                Err(e) => {
                    warn!(
                        measurement = %measurement,
                        error = %e,
                        "Anomaly detection failed"
                    );
                }
            }
        }

        None
    }

    /// Get all scored anomalies.
    #[must_use]
    pub fn scores(&self) -> Vec<ScoredAnomaly> {
        self.scores.read().iter().cloned().collect()
    }

    /// Get anomaly scores for a measurement.
    #[must_use]
    pub fn scores_for_measurement(&self, measurement: &str) -> Vec<ScoredAnomaly> {
        self.scores
            .read()
            .iter()
            .filter(|s| s.measurement == measurement)
            .cloned()
            .collect()
    }

    /// Get only anomalous scores.
    #[must_use]
    pub fn anomalies(&self) -> Vec<ScoredAnomaly> {
        self.scores
            .read()
            .iter()
            .filter(|s| s.score.is_anomaly)
            .cloned()
            .collect()
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

    /// Drain all scores.
    pub fn drain_scores(&self) -> Vec<ScoredAnomaly> {
        self.scores.write().drain(..).collect()
    }
}

impl Default for StreamingAnomalyEngine {
    fn default() -> Self {
        Self::new()
    }
}

use crate::util::{compute_tags_key, extract_numeric_value};

#[cfg(test)]
mod tests {
    use super::*;

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
    fn enable_disable_config() {
        let engine = StreamingAnomalyEngine::new();
        let config =
            StreamingAnomalyConfig::new("cpu", DetectorType::ZScore, 3.0).with_min_fit_points(5);
        engine.enable(config).unwrap();
        assert!(engine.is_enabled("cpu"));
        assert!(!engine.is_enabled("memory"));
        assert_eq!(engine.config_count(), 1);

        engine.disable("cpu");
        assert!(!engine.is_enabled("cpu"));
        assert_eq!(engine.config_count(), 0);
    }

    #[test]
    fn process_write_accumulates_training_data() {
        let engine = StreamingAnomalyEngine::new();
        let config =
            StreamingAnomalyConfig::new("cpu", DetectorType::ZScore, 3.0).with_min_fit_points(10);
        engine.enable(config).unwrap();

        let tags = make_tags("host1");

        // Write 5 points — not enough for fitting
        for i in 0..5 {
            let result = engine.process_write("cpu", &tags, &make_fields(50.0), i * 1000);
            assert!(result.is_none());
        }

        assert_eq!(engine.series_count(), 1);
    }

    #[test]
    fn process_write_fits_and_detects() {
        let engine = StreamingAnomalyEngine::new();
        let config =
            StreamingAnomalyConfig::new("cpu", DetectorType::ZScore, 2.0).with_min_fit_points(10);
        engine.enable(config).unwrap();

        let tags = make_tags("host1");

        // Write 10 normal points to fit
        for i in 0..10 {
            engine.process_write(
                "cpu",
                &tags,
                &make_fields(50.0 + (i as f64) * 0.1),
                i * 1000,
            );
        }

        // Write a normal point — should get a score now
        let result = engine.process_write("cpu", &tags, &make_fields(50.5), 10_000);
        assert!(result.is_some());
        let scored = result.unwrap();
        assert!(!scored.score.is_anomaly);

        // Write an extreme outlier
        let result = engine.process_write("cpu", &tags, &make_fields(1000.0), 11_000);
        assert!(result.is_some());
        let scored = result.unwrap();
        assert!(scored.score.is_anomaly);
    }

    #[test]
    fn unconfigured_measurement_ignored() {
        let engine = StreamingAnomalyEngine::new();
        let tags = make_tags("host1");
        let result = engine.process_write("cpu", &tags, &make_fields(50.0), 0);
        assert!(result.is_none());
    }

    #[test]
    fn specific_field_extraction() {
        let engine = StreamingAnomalyEngine::new();
        let config = StreamingAnomalyConfig::new("cpu", DetectorType::ZScore, 2.0)
            .with_field("usage")
            .with_min_fit_points(5);
        engine.enable(config).unwrap();

        let tags = make_tags("host1");
        let mut fields = BTreeMap::new();
        fields.insert("usage".to_string(), FieldValue::F64(50.0));
        fields.insert("idle".to_string(), FieldValue::F64(50.0));

        for i in 0..5 {
            engine.process_write("cpu", &tags, &fields, i * 1000);
        }

        // After fitting, detection should use the "usage" field
        let result = engine.process_write("cpu", &tags, &fields, 5000);
        assert!(result.is_some());
    }

    #[test]
    fn multiple_series_tracked_independently() {
        let engine = StreamingAnomalyEngine::new();
        let config =
            StreamingAnomalyConfig::new("cpu", DetectorType::ZScore, 2.0).with_min_fit_points(5);
        engine.enable(config).unwrap();

        let tags_a = make_tags("host1");
        let tags_b = make_tags("host2");

        for i in 0..5 {
            engine.process_write("cpu", &tags_a, &make_fields(50.0), i * 1000);
            engine.process_write("cpu", &tags_b, &make_fields(100.0), i * 1000);
        }

        assert_eq!(engine.series_count(), 2);
    }

    #[test]
    fn scores_query() {
        let engine = StreamingAnomalyEngine::new();
        let config =
            StreamingAnomalyConfig::new("cpu", DetectorType::ZScore, 2.0).with_min_fit_points(5);
        engine.enable(config).unwrap();

        let tags = make_tags("host1");
        for i in 0..5 {
            engine.process_write("cpu", &tags, &make_fields(50.0), i * 1000);
        }
        // Post-fit point
        engine.process_write("cpu", &tags, &make_fields(50.0), 5000);

        let all = engine.scores();
        assert!(!all.is_empty());
        let cpu = engine.scores_for_measurement("cpu");
        assert_eq!(cpu.len(), all.len());
    }

    #[test]
    fn extract_numeric_from_field_types() {
        let mut fields = BTreeMap::new();
        fields.insert("i64_field".to_string(), FieldValue::I64(42));
        assert_eq!(
            extract_numeric_value(&fields, Some("i64_field")),
            Some(42.0)
        );

        fields.insert("u64_field".to_string(), FieldValue::U64(99));
        assert_eq!(
            extract_numeric_value(&fields, Some("u64_field")),
            Some(99.0)
        );

        fields.insert("bool_field".to_string(), FieldValue::Bool(true));
        assert_eq!(extract_numeric_value(&fields, Some("bool_field")), None);
    }

    #[test]
    fn disabled_config_rejected() {
        let engine = StreamingAnomalyEngine::new();
        let mut config = StreamingAnomalyConfig::new("cpu", DetectorType::ZScore, 3.0);
        config.enabled = false;
        assert!(engine.enable(config).is_err());
    }

    #[test]
    fn test_nan_inf_rejected() {
        use crate::util::extract_numeric_value;
        use chronix_core::FieldValue;

        let mut fields = BTreeMap::new();

        fields.insert("v".to_string(), FieldValue::F64(f64::NAN));
        assert_eq!(extract_numeric_value(&fields, Some("v")), None);

        fields.insert("v".to_string(), FieldValue::F64(f64::INFINITY));
        assert_eq!(extract_numeric_value(&fields, Some("v")), None);

        fields.insert("v".to_string(), FieldValue::F64(f64::NEG_INFINITY));
        assert_eq!(extract_numeric_value(&fields, Some("v")), None);

        fields.insert("v".to_string(), FieldValue::F64(42.0));
        assert_eq!(extract_numeric_value(&fields, Some("v")), Some(42.0));
    }

    #[test]
    fn test_vecdeque_eviction_bounded() {
        let engine = StreamingAnomalyEngine::new();
        let config = StreamingAnomalyConfig::new("cpu", DetectorType::ZScore, 3.0)
            .with_max_buffer_size(50)
            .with_min_fit_points(10);
        engine.enable(config).unwrap();

        let tags = make_tags("host1");

        // Feed 200 points into the same series
        for i in 0..200 {
            engine.process_write(
                "cpu",
                &tags,
                &make_fields(50.0 + (i as f64) * 0.01),
                i * 1000,
            );
        }

        // Should still be exactly 1 series tracked
        assert_eq!(engine.series_count(), 1);
    }
}
