//! Continuous aggregations — incrementally maintained materialized views.
//!
//! A [`ContinuousAggregation`] subscribes to CDC write events for a source
//! measurement and maintains running partial aggregates (sum, count, min, max)
//! per time bucket. When the time window advances past a bucket boundary,
//! the finalized aggregate is emitted as a [`BucketResult`].

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::cdc::error::StreamError;
use crate::cdc::event::CdcEvent;

/// Aggregation functions supported for continuous aggregations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggFunction {
    /// Running sum.
    Sum,
    /// Running count.
    Count,
    /// Running minimum.
    Min,
    /// Running maximum.
    Max,
    /// Mean (computed as sum/count at finalization).
    Mean,
}

/// Configuration for a continuous aggregation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuousAggregationConfig {
    /// Name of this aggregation (used as identifier).
    pub name: String,
    /// Source measurement to subscribe to.
    pub source_measurement: String,
    /// Target measurement to write aggregated results to.
    pub target_measurement: String,
    /// Field name in the source measurement to aggregate.
    pub source_field: String,
    /// Bucket interval (e.g. 60s = 1-minute buckets).
    pub interval: Duration,
    /// Aggregation functions to compute.
    pub functions: Vec<AggFunction>,
    /// Window for accepting late-arriving data (buckets older than this are finalized).
    pub late_arrival_window: Duration,
}

/// Running partial aggregate state for a single bucket.
#[derive(Debug, Clone)]
struct BucketState {
    /// Bucket start timestamp (inclusive, nanoseconds).
    bucket_start_ns: i64,
    sum: f64,
    count: u64,
    min: f64,
    max: f64,
    /// Original tags for this series (preserved for flush_all).
    tags: std::collections::BTreeMap<String, String>,
}

impl BucketState {
    fn new(bucket_start_ns: i64, tags: &std::collections::BTreeMap<String, String>) -> Self {
        Self {
            bucket_start_ns,
            sum: 0.0,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            tags: tags.clone(),
        }
    }

    fn update(&mut self, value: f64) {
        // Skip non-finite values — NaN would permanently corrupt the
        // running sum/mean, and ±Inf would make the average meaningless
        // while polluting min/max with sentinel-like values.
        if !value.is_finite() {
            // Increment a counter so operators can detect noisy sources.
            metrics::counter!("chronix_stream_non_finite_values_dropped").increment(1);
            return;
        }
        self.sum += value;
        self.count += 1;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
    }
}

/// A finalized bucket result, ready to be written to the target measurement.
#[derive(Debug, Clone, PartialEq)]
pub struct BucketResult {
    /// Target measurement name.
    pub target_measurement: String,
    /// Bucket start timestamp (nanoseconds).
    pub bucket_start_ns: i64,
    /// Bucket end timestamp (nanoseconds, exclusive).
    pub bucket_end_ns: i64,
    /// Tags from the source (series grouping).
    pub tags: HashMap<String, String>,
    /// Name of the aggregation.
    pub aggregation_name: String,
    /// Computed aggregate values.
    pub values: HashMap<AggFunction, f64>,
}

/// Default maximum number of distinct series keys tracked simultaneously.
const DEFAULT_MAX_SERIES: usize = 100_000;

/// The continuous aggregation engine.
///
/// Processes CDC write events and maintains partial aggregates per time bucket
/// per series. Finalized buckets are returned when the time advances past the
/// bucket boundary plus the late arrival window.
#[derive(Debug)]
pub struct ContinuousAggregationEngine {
    config: ContinuousAggregationConfig,
    /// Interval in nanoseconds.
    interval_ns: i64,
    /// Late arrival window in nanoseconds.
    late_ns: i64,
    /// Running state: series key (measurement+tags hash) → bucket_start → state.
    buckets: HashMap<String, HashMap<i64, BucketState>>,
    /// Per-series latest data timestamp for deterministic eviction ordering.
    /// When evicting, the series with the oldest `last_data_ts` is removed.
    series_last_ts: HashMap<String, i64>,
    /// Cache of series keys to avoid re-computing escape+concat per event.
    /// Keyed by (measurement, sorted tags string) to prevent hash collisions.
    series_key_cache: HashMap<(String, Vec<(String, String)>), String>,
    /// Highest timestamp seen (for bucket finalization).
    max_timestamp: i64,
    /// Watermark at which the last finalization sweep ran.
    /// Used to skip redundant sweeps when the cutoff hasn't changed.
    last_sweep_cutoff: i64,
    /// Maximum number of tracked series keys. When exceeded, the engine
    /// force-finalizes and evicts series with the oldest max-bucket
    /// timestamp to stay within budget.
    max_series: usize,
}

impl ContinuousAggregationEngine {
    /// Creates a new continuous aggregation engine.
    ///
    /// # Errors
    ///
    /// Returns [`StreamError::InvalidConfig`] if the aggregation interval is
    /// zero or negative.
    pub fn new(config: ContinuousAggregationConfig) -> Result<Self, StreamError> {
        let interval_ns = i64::try_from(config.interval.as_nanos()).unwrap_or(i64::MAX);
        if interval_ns <= 0 {
            return Err(StreamError::InvalidConfig(
                "aggregation interval must be positive".into(),
            ));
        }
        let late_ns = i64::try_from(config.late_arrival_window.as_nanos()).unwrap_or(i64::MAX);
        Ok(Self {
            config,
            interval_ns,
            late_ns,
            buckets: HashMap::new(),
            series_last_ts: HashMap::new(),
            series_key_cache: HashMap::new(),
            max_timestamp: i64::MIN,
            last_sweep_cutoff: i64::MIN,
            max_series: DEFAULT_MAX_SERIES,
        })
    }

    /// Returns the configuration.
    pub fn config(&self) -> &ContinuousAggregationConfig {
        &self.config
    }

    /// Override the maximum number of tracked series (default: 100 000).
    ///
    /// When the number of distinct series keys exceeds this limit, the
    /// engine force-finalizes and evicts the series with the oldest
    /// bucket data to stay within budget.
    #[must_use]
    pub fn with_max_series(mut self, max: usize) -> Self {
        self.max_series = max;
        self
    }

    /// Number of distinct series keys currently tracked.
    pub fn tracked_series_count(&self) -> usize {
        self.buckets.len()
    }

    /// Computes the bucket start timestamp for a given data timestamp.
    ///
    /// Uses Euclidean remainder so negative timestamps are rounded towards
    /// negative infinity, giving correct bucket boundaries before epoch.
    /// This form (`ts - ts.rem_euclid(interval)`) avoids the overflow that
    /// `div_euclid * interval` causes for large timestamps.
    /// Uses `saturating_sub` to prevent underflow for timestamps near `i64::MIN`.
    fn bucket_start(&self, timestamp: i64) -> i64 {
        timestamp.saturating_sub(timestamp.rem_euclid(self.interval_ns))
    }

    /// Processes a CDC event.
    ///
    /// - `PointWritten` events for the source measurement update partial
    ///   aggregates.
    /// - Other events are ignored.
    ///
    /// Returns any finalized bucket results.
    pub fn process_event(&mut self, event: &CdcEvent) -> Vec<BucketResult> {
        match event {
            CdcEvent::PointWritten {
                measurement,
                tags,
                fields,
                timestamp,
                ..
            } => {
                if measurement != &self.config.source_measurement {
                    return Vec::new();
                }

                // Extract the field value
                let value = match fields.get(&self.config.source_field) {
                    Some(chronix_core::types::FieldValue::F64(v)) => *v,
                    Some(chronix_core::types::FieldValue::I64(v)) => *v as f64,
                    Some(chronix_core::types::FieldValue::U64(v)) => *v as f64,
                    _ => return Vec::new(), // field not found or not numeric
                };

                let ts = *timestamp;
                if ts > self.max_timestamp {
                    self.max_timestamp = ts;
                }

                // Cache the series key to avoid re-computing
                // escape+concat for every event in the same series.
                // Use full (measurement, tags) as cache key to
                // prevent collisions that cause cross-series aggregation.
                let cache_key = (
                    measurement.clone(),
                    tags.iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect::<Vec<_>>(),
                );
                let series_key = self
                    .series_key_cache
                    .entry(cache_key)
                    .or_insert_with(|| Self::series_key(measurement, tags))
                    .clone();
                let bucket_start = self.bucket_start(ts);

                // Evict oldest series when at capacity and this is a
                // brand-new series key.
                let mut results = Vec::new();
                let is_new_series = !self.buckets.contains_key(&series_key);
                if is_new_series && self.buckets.len() >= self.max_series {
                    results.extend(self.evict_oldest_series());
                }

                // Update the bucket
                let series_buckets = self.buckets.entry(series_key.clone()).or_default();
                series_buckets
                    .entry(bucket_start)
                    .or_insert_with(|| BucketState::new(bucket_start, tags))
                    .update(value);

                // Track per-series latest timestamp for eviction ordering.
                self.series_last_ts
                    .entry(series_key)
                    .and_modify(|prev| {
                        if ts > *prev {
                            *prev = ts;
                        }
                    })
                    .or_insert(ts);

                // Finalize expired buckets across ALL series, not just
                // the current one. This prevents idle series from
                // retaining stale open buckets indefinitely.
                results.extend(self.finalize_all_series());
                results
            }
            _ => Vec::new(),
        }
    }

    /// Advances the watermark and finalizes expired buckets across all
    /// tracked series.
    ///
    /// Call this periodically (e.g. on a timer) if events may stop
    /// arriving for some series while others continue, to ensure
    /// timely finalization for idle series.
    pub fn advance_watermark(&mut self, timestamp: i64) -> Vec<BucketResult> {
        if timestamp > self.max_timestamp {
            self.max_timestamp = timestamp;
        }
        self.finalize_all_series()
    }

    /// Force-finalizes and removes the series with the oldest bucket
    /// data, making room for a new series key.
    ///
    /// "Oldest" is determined by the minimum `bucket_start_ns` across
    /// all buckets for that series, with `series_last_ts` as a
    /// tiebreaker — the stalest data is evicted first.
    fn evict_oldest_series(&mut self) -> Vec<BucketResult> {
        // Find the series whose newest bucket_start is the smallest
        // (i.e., the series furthest behind the watermark).
        // When multiple series share the same bucket_start, the series
        // with the oldest last-data timestamp is evicted.
        let victim = self
            .buckets
            .iter()
            .map(|(key, buckets)| {
                let newest = buckets.keys().copied().max().unwrap_or(i64::MIN);
                let last_ts = self.series_last_ts.get(key).copied().unwrap_or(i64::MIN);
                (key.clone(), newest, last_ts)
            })
            .min_by_key(|(_, newest, last_ts)| (*newest, *last_ts))
            .map(|(key, _, _)| key);

        let Some(victim_key) = victim else {
            return Vec::new();
        };

        // Force-finalize ALL buckets for the victim series, regardless
        // of whether they've expired — this is eviction, not normal
        // finalization.
        let results = self.force_finalize_series(&victim_key);
        self.buckets.remove(&victim_key);
        self.series_last_ts.remove(&victim_key);
        self.series_key_cache.retain(|_, v| v != &victim_key);
        results
    }

    /// Force-finalizes all buckets for a single series (ignoring cutoff).
    fn force_finalize_series(&mut self, series_key: &str) -> Vec<BucketResult> {
        let mut results = Vec::new();
        if let Some(series_buckets) = self.buckets.get_mut(series_key) {
            let all_starts: Vec<i64> = series_buckets.keys().copied().collect();
            for bucket_start in all_starts {
                if let Some(state) = series_buckets.remove(&bucket_start) {
                    let mut values = HashMap::new();
                    for func in &self.config.functions {
                        let v = match func {
                            AggFunction::Sum => {
                                if state.count > 0 {
                                    state.sum
                                } else {
                                    f64::NAN
                                }
                            }
                            AggFunction::Count => state.count as f64,
                            AggFunction::Min => {
                                if state.count > 0 {
                                    state.min
                                } else {
                                    f64::NAN
                                }
                            }
                            AggFunction::Max => {
                                if state.count > 0 {
                                    state.max
                                } else {
                                    f64::NAN
                                }
                            }
                            AggFunction::Mean => {
                                if state.count > 0 {
                                    state.sum / state.count as f64
                                } else {
                                    f64::NAN
                                }
                            }
                        };
                        values.insert(*func, v);
                    }
                    results.push(BucketResult {
                        target_measurement: self.config.target_measurement.clone(),
                        bucket_start_ns: state.bucket_start_ns,
                        bucket_end_ns: state.bucket_start_ns.saturating_add(self.interval_ns),
                        tags: state.tags.into_iter().collect(),
                        aggregation_name: self.config.name.clone(),
                        values,
                    });
                }
            }
        }
        results
    }

    /// Finalizes expired buckets across ALL series keys.
    ///
    /// Skips the sweep if the finalization cutoff hasn't advanced since
    /// the last call, avoiding O(n) key-clone overhead per event at
    /// steady-state timestamp.
    fn finalize_all_series(&mut self) -> Vec<BucketResult> {
        let cutoff = self.max_timestamp.saturating_sub(self.late_ns);
        if cutoff <= self.last_sweep_cutoff {
            return Vec::new();
        }
        self.last_sweep_cutoff = cutoff;

        // Only clone keys whose buckets contain at least one expired window,
        // avoiding O(total-series) String allocation on every sweep.
        let interval = self.interval_ns;
        let expired_keys: Vec<String> = self
            .buckets
            .iter()
            .filter(|(_, buckets)| {
                buckets
                    .keys()
                    .any(|&start| start.saturating_add(interval) <= cutoff)
            })
            .map(|(k, _)| k.clone())
            .collect();
        let mut results = Vec::new();
        for key in expired_keys {
            results.extend(self.finalize_buckets(&key));
        }

        // Remove series entries whose inner bucket map is now empty.
        // Without this, idle series accumulate as empty maps, leaking
        // memory and adding O(n) clone overhead on every sweep.
        self.buckets.retain(|_, v| !v.is_empty());

        // Clean up series_last_ts for series with no remaining buckets.
        self.series_last_ts
            .retain(|k, _| self.buckets.contains_key(k));
        self.series_key_cache
            .retain(|_, v| self.buckets.contains_key(v));

        results.sort_by_key(|r| r.bucket_start_ns);
        results
    }

    /// Finalizes buckets whose window has expired.
    fn finalize_buckets(&mut self, series_key: &str) -> Vec<BucketResult> {
        let mut results = Vec::new();
        let cutoff = self.max_timestamp.saturating_sub(self.late_ns);

        if let Some(series_buckets) = self.buckets.get_mut(series_key) {
            let finalized: Vec<i64> = series_buckets
                .keys()
                .filter(|&&start| start.saturating_add(self.interval_ns) <= cutoff)
                .copied()
                .collect();

            for bucket_start in finalized {
                if let Some(state) = series_buckets.remove(&bucket_start) {
                    let mut values = HashMap::new();
                    for func in &self.config.functions {
                        let v = match func {
                            AggFunction::Sum => {
                                if state.count > 0 {
                                    state.sum
                                } else {
                                    f64::NAN
                                }
                            }
                            AggFunction::Count => state.count as f64,
                            AggFunction::Min => {
                                if state.count > 0 {
                                    state.min
                                } else {
                                    f64::NAN
                                }
                            }
                            AggFunction::Max => {
                                if state.count > 0 {
                                    state.max
                                } else {
                                    f64::NAN
                                }
                            }
                            AggFunction::Mean => {
                                if state.count > 0 {
                                    state.sum / state.count as f64
                                } else {
                                    f64::NAN
                                }
                            }
                        };
                        values.insert(*func, v);
                    }

                    results.push(BucketResult {
                        target_measurement: self.config.target_measurement.clone(),
                        bucket_start_ns: state.bucket_start_ns,
                        bucket_end_ns: state.bucket_start_ns.saturating_add(self.interval_ns),
                        tags: state
                            .tags
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect(),
                        aggregation_name: self.config.name.clone(),
                        values,
                    });
                }
            }

            results.sort_by_key(|r| r.bucket_start_ns);
        }

        results
    }

    /// Force-finalizes all open buckets (e.g. on shutdown).
    ///
    /// Tags are preserved from the original data points.
    pub fn flush_all(&mut self) -> Vec<BucketResult> {
        // Temporarily set max_timestamp to MAX to finalize everything
        let saved = self.max_timestamp;
        self.max_timestamp = i64::MAX;
        self.last_sweep_cutoff = i64::MIN; // reset so flush actually sweeps

        let all_keys: Vec<String> = self.buckets.keys().cloned().collect();
        let mut results = Vec::new();

        for key in all_keys {
            results.extend(self.finalize_buckets(&key));
        }

        self.max_timestamp = saved;
        // Clean up empty series entries to avoid memory leak
        self.buckets.retain(|_, v| !v.is_empty());
        // Prune cache entries for removed series
        self.series_key_cache
            .retain(|_, v| self.buckets.contains_key(v));
        results
    }

    /// Returns the number of active (non-finalized) buckets.
    pub fn active_bucket_count(&self) -> usize {
        self.buckets
            .values()
            .map(std::collections::HashMap::len)
            .sum()
    }

    fn series_key(measurement: &str, tags: &std::collections::BTreeMap<String, String>) -> String {
        // Escape delimiter characters to prevent cross-series bucket collisions.
        // Without escaping, tags {"a": "b,c=d"} and {"a": "b", "c": "d"}
        // produce identical keys.
        fn escape(s: &str) -> String {
            s.replace('\\', "\\\\")
                .replace(',', "\\,")
                .replace('=', "\\=")
        }
        let mut key = escape(measurement);
        for (k, v) in tags {
            key.push(',');
            key.push_str(&escape(k));
            key.push('=');
            key.push_str(&escape(v));
        }
        key
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chronix_core::types::FieldValue;

    use super::*;

    fn make_config() -> ContinuousAggregationConfig {
        ContinuousAggregationConfig {
            name: "cpu_1min".into(),
            source_measurement: "cpu".into(),
            target_measurement: "cpu_agg".into(),
            source_field: "usage".into(),
            interval: Duration::from_secs(60),
            functions: vec![
                AggFunction::Sum,
                AggFunction::Count,
                AggFunction::Min,
                AggFunction::Max,
                AggFunction::Mean,
            ],
            late_arrival_window: Duration::from_secs(5),
        }
    }

    fn write_event(ts_sec: i64, value: f64) -> CdcEvent {
        CdcEvent::PointWritten {
            measurement: "cpu".into(),
            tags: BTreeMap::from([("host".into(), "srv1".into())]),
            fields: BTreeMap::from([("usage".into(), FieldValue::F64(value))]),
            timestamp: ts_sec * 1_000_000_000,
            seq: ts_sec as u64,
        }
    }

    #[test]
    fn single_bucket_finalization() {
        let mut engine = ContinuousAggregationEngine::new(make_config()).unwrap();

        // Write 60 points in the first minute (0..60) with values 1.0
        for i in 0..60 {
            let results = engine.process_event(&write_event(i, 1.0));
            assert!(results.is_empty(), "bucket should not finalize yet");
        }

        // Write a point at t=66 (past late arrival window) to trigger finalization
        let results = engine.process_event(&write_event(66, 1.0));
        assert_eq!(results.len(), 1, "first bucket should be finalized");

        let r = &results[0];
        assert_eq!(r.target_measurement, "cpu_agg");
        assert_eq!(r.aggregation_name, "cpu_1min");
        assert_eq!(r.bucket_start_ns, 0);
        assert_eq!(r.bucket_end_ns, 60_000_000_000);
        assert!((r.values[&AggFunction::Sum] - 60.0).abs() < 1e-10);
        assert!((r.values[&AggFunction::Count] - 60.0).abs() < 1e-10);
        assert!((r.values[&AggFunction::Min] - 1.0).abs() < 1e-10);
        assert!((r.values[&AggFunction::Max] - 1.0).abs() < 1e-10);
        assert!((r.values[&AggFunction::Mean] - 1.0).abs() < 1e-10);
    }

    #[test]
    fn two_buckets() {
        let mut engine = ContinuousAggregationEngine::new(make_config()).unwrap();
        let mut all_results = Vec::new();

        // Write into first bucket (0..60)
        for i in 0..60 {
            all_results.extend(engine.process_event(&write_event(i, 2.0)));
        }
        // Write into second bucket (60..120)
        for i in 60..120 {
            all_results.extend(engine.process_event(&write_event(i, 3.0)));
        }

        // Advance past second bucket + late window to finalize remaining
        all_results.extend(engine.process_event(&write_event(186, 1.0)));
        assert_eq!(all_results.len(), 2);
        all_results.sort_by_key(|r| r.bucket_start_ns);
        assert!((all_results[0].values[&AggFunction::Mean] - 2.0).abs() < 1e-10);
        assert!((all_results[1].values[&AggFunction::Mean] - 3.0).abs() < 1e-10);
    }

    #[test]
    fn late_data_handled() {
        let mut engine = ContinuousAggregationEngine::new(make_config()).unwrap();
        let mut all_results = Vec::new();

        // Write early data in bucket 0
        all_results.extend(engine.process_event(&write_event(10, 1.0)));

        // Jump forward but within late arrival window (bucket 60)
        all_results.extend(engine.process_event(&write_event(62, 2.0)));

        // Late arrival for the first bucket — should be accepted
        all_results.extend(engine.process_event(&write_event(20, 3.0)));

        // Now advance past late window — both buckets finalize
        all_results.extend(engine.process_event(&write_event(130, 1.0)));
        all_results.sort_by_key(|r| r.bucket_start_ns);
        assert!(!all_results.is_empty());

        // First bucket should have both early + late points: 1.0 + 3.0 = 4.0
        let bucket0 = all_results.iter().find(|r| r.bucket_start_ns == 0).unwrap();
        assert!((bucket0.values[&AggFunction::Sum] - 4.0).abs() < 1e-10);
        assert!((bucket0.values[&AggFunction::Count] - 2.0).abs() < 1e-10);
    }

    #[test]
    fn ignores_wrong_measurement() {
        let mut engine = ContinuousAggregationEngine::new(make_config()).unwrap();

        let evt = CdcEvent::PointWritten {
            measurement: "memory".into(),
            tags: BTreeMap::new(),
            fields: BTreeMap::from([("usage".into(), FieldValue::F64(50.0))]),
            timestamp: 1_000_000_000,
            seq: 1,
        };

        let results = engine.process_event(&evt);
        assert!(results.is_empty());
        assert_eq!(engine.active_bucket_count(), 0);
    }

    #[test]
    fn ignores_non_write_events() {
        let mut engine = ContinuousAggregationEngine::new(make_config()).unwrap();

        let evt = CdcEvent::SeriesDeleted {
            measurement: "cpu".into(),
            tags: BTreeMap::from([("host".into(), "srv1".into())]),
            series_hash: 0,
            seq: 1,
        };
        let results = engine.process_event(&evt);
        assert!(results.is_empty());
    }

    #[test]
    fn flush_all_finalizes_open_buckets() {
        let mut engine = ContinuousAggregationEngine::new(make_config()).unwrap();

        for i in 0..30 {
            engine.process_event(&write_event(i, 1.0));
        }

        assert_eq!(engine.active_bucket_count(), 1);
        let results = engine.flush_all();
        assert_eq!(results.len(), 1);
        assert!((results[0].values[&AggFunction::Count] - 30.0).abs() < 1e-10);
    }

    #[test]
    fn min_max_computed_correctly() {
        let mut engine = ContinuousAggregationEngine::new(make_config()).unwrap();

        engine.process_event(&write_event(0, 5.0));
        engine.process_event(&write_event(1, 2.0));
        engine.process_event(&write_event(2, 8.0));
        engine.process_event(&write_event(3, 1.0));

        let results = engine.flush_all();
        assert_eq!(results.len(), 1);
        assert!((results[0].values[&AggFunction::Min] - 1.0).abs() < 1e-10);
        assert!((results[0].values[&AggFunction::Max] - 8.0).abs() < 1e-10);
    }

    #[test]
    fn i64_field_values() {
        let mut engine = ContinuousAggregationEngine::new(make_config()).unwrap();

        let evt = CdcEvent::PointWritten {
            measurement: "cpu".into(),
            tags: BTreeMap::from([("host".into(), "srv1".into())]),
            fields: BTreeMap::from([("usage".into(), FieldValue::I64(42))]),
            timestamp: 1_000_000_000,
            seq: 1,
        };
        engine.process_event(&evt);
        let results = engine.flush_all();
        assert_eq!(results.len(), 1);
        assert!((results[0].values[&AggFunction::Sum] - 42.0).abs() < 1e-10);
    }

    #[test]
    fn config_accessor() {
        let config = make_config();
        let engine = ContinuousAggregationEngine::new(config.clone()).unwrap();
        assert_eq!(engine.config().name, "cpu_1min");
        assert_eq!(engine.config().source_measurement, "cpu");
    }

    #[test]
    fn test_bucket_start_negative_timestamp() {
        // Use a 10-nanosecond interval for easy reasoning
        let config = ContinuousAggregationConfig {
            name: "test".into(),
            source_measurement: "cpu".into(),
            target_measurement: "cpu_agg".into(),
            source_field: "usage".into(),
            interval: Duration::from_nanos(10),
            functions: vec![AggFunction::Sum],
            late_arrival_window: Duration::from_nanos(0),
        };
        let engine = ContinuousAggregationEngine::new(config).unwrap();

        // bucket_start(-15) should be -20 (Euclidean division rounds towards -∞)
        // not -10 which truncation towards zero would give.
        assert_eq!(engine.bucket_start(-15), -20);
        assert_eq!(engine.bucket_start(-10), -10);
        assert_eq!(engine.bucket_start(-1), -10);
        assert_eq!(engine.bucket_start(0), 0);
        assert_eq!(engine.bucket_start(15), 10);
    }

    #[test]
    fn idle_series_finalized_by_active_series_event() {
        // Regression test for H3: idle series buckets must finalize when
        // the watermark advances via events from OTHER series.
        let mut engine = ContinuousAggregationEngine::new(make_config()).unwrap();

        // Series A: host=srv1, write into bucket 0
        let evt_a = CdcEvent::PointWritten {
            measurement: "cpu".into(),
            tags: BTreeMap::from([("host".into(), "srv1".into())]),
            fields: BTreeMap::from([("usage".into(), FieldValue::F64(10.0))]),
            timestamp: 30_000_000_000, // t=30s in bucket [0, 60)
            seq: 1,
        };
        let results = engine.process_event(&evt_a);
        assert!(results.is_empty());

        // Series B: host=srv2, write into bucket 0
        let evt_b = CdcEvent::PointWritten {
            measurement: "cpu".into(),
            tags: BTreeMap::from([("host".into(), "srv2".into())]),
            fields: BTreeMap::from([("usage".into(), FieldValue::F64(20.0))]),
            timestamp: 40_000_000_000, // t=40s
            seq: 2,
        };
        let results = engine.process_event(&evt_b);
        assert!(results.is_empty());

        // Only series B continues — advance past finalization threshold.
        // This event in series B should trigger finalization of BOTH
        // series A and series B for bucket 0.
        let evt_b2 = CdcEvent::PointWritten {
            measurement: "cpu".into(),
            tags: BTreeMap::from([("host".into(), "srv2".into())]),
            fields: BTreeMap::from([("usage".into(), FieldValue::F64(5.0))]),
            timestamp: 130_000_000_000, // t=130s, well past bucket 0 + late window
            seq: 3,
        };
        let results = engine.process_event(&evt_b2);

        // Both series should have their bucket 0 finalized
        let bucket0: Vec<_> = results.iter().filter(|r| r.bucket_start_ns == 0).collect();
        assert_eq!(bucket0.len(), 2, "both series should finalize bucket 0");

        let srv1 = bucket0
            .iter()
            .find(|r| r.tags.get("host") == Some(&"srv1".into()));
        assert!(srv1.is_some(), "srv1 bucket should be finalized");
        assert!((srv1.unwrap().values[&AggFunction::Sum] - 10.0).abs() < 1e-10);

        let srv2 = bucket0
            .iter()
            .find(|r| r.tags.get("host") == Some(&"srv2".into()));
        assert!(srv2.is_some(), "srv2 bucket should be finalized");
        assert!((srv2.unwrap().values[&AggFunction::Sum] - 20.0).abs() < 1e-10);
    }

    #[test]
    fn advance_watermark_finalizes_idle_series() {
        let mut engine = ContinuousAggregationEngine::new(make_config()).unwrap();

        // Write a single point
        engine.process_event(&write_event(10, 42.0));
        assert_eq!(engine.active_bucket_count(), 1);

        // Advance watermark without any new events
        let results = engine.advance_watermark(200_000_000_000);
        assert_eq!(results.len(), 1);
        assert!((results[0].values[&AggFunction::Sum] - 42.0).abs() < 1e-10);
        assert_eq!(engine.active_bucket_count(), 0);
    }

    #[test]
    fn rejects_zero_interval() {
        let config = ContinuousAggregationConfig {
            name: "bad".into(),
            source_measurement: "cpu".into(),
            target_measurement: "cpu_agg".into(),
            source_field: "usage".into(),
            interval: Duration::ZERO,
            functions: vec![AggFunction::Sum],
            late_arrival_window: Duration::from_secs(5),
        };
        let err = ContinuousAggregationEngine::new(config).unwrap_err();
        assert!(
            err.to_string().contains("positive"),
            "expected positive-interval error, got: {err}"
        );
    }

    #[test]
    fn all_nan_bucket_produces_nan_not_infinity() {
        let config = ContinuousAggregationConfig {
            name: "nan_test".into(),
            source_measurement: "cpu".into(),
            target_measurement: "cpu_agg".into(),
            source_field: "usage".into(),
            interval: Duration::from_secs(10),
            functions: vec![
                AggFunction::Min,
                AggFunction::Max,
                AggFunction::Sum,
                AggFunction::Mean,
            ],
            late_arrival_window: Duration::from_secs(0),
        };
        let mut engine = ContinuousAggregationEngine::new(config).unwrap();

        // Insert only NaN values into a bucket
        let nan_evt = |ts: i64, seq: u64| CdcEvent::PointWritten {
            measurement: "cpu".into(),
            tags: BTreeMap::from([("host".into(), "srv1".into())]),
            fields: BTreeMap::from([("usage".into(), FieldValue::F64(f64::NAN))]),
            timestamp: ts,
            seq,
        };
        engine.process_event(&nan_evt(0, 1));
        engine.process_event(&nan_evt(1_000_000_000, 2));

        // Finalize all buckets
        let results = engine.flush_all();
        // The bucket (all-NaN) should produce NaN for min/max/sum/mean
        for r in &results {
            for (func, &val) in &r.values {
                if matches!(func, AggFunction::Count) {
                    // count should be 0, not NaN
                    assert!(
                        (val - 0.0).abs() < 1e-10,
                        "count should be 0 for all-NaN bucket"
                    );
                } else {
                    assert!(
                        val.is_nan(),
                        "Expected NaN for {func:?} on all-NaN bucket, got {val}"
                    );
                }
            }
        }
    }

    #[test]
    fn bucket_start_near_min_timestamp() {
        let engine = ContinuousAggregationEngine::new(make_config()).unwrap();
        // Should not panic/overflow for timestamps near i64::MIN
        let bs = engine.bucket_start(i64::MIN + 1);
        assert!(bs <= i64::MIN + 1, "bucket_start should be <= timestamp");
        let bs_min = engine.bucket_start(i64::MIN);
        assert_eq!(
            bs_min,
            i64::MIN,
            "bucket_start at i64::MIN should not overflow"
        );
    }

    #[test]
    fn series_key_escapes_delimiters() {
        let tags = BTreeMap::from([("a".into(), "b,c=d".into())]);
        let key1 = ContinuousAggregationEngine::series_key("cpu", &tags);

        let tags2 = BTreeMap::from([("a".into(), "b".into()), ("c".into(), "d".into())]);
        let key2 = ContinuousAggregationEngine::series_key("cpu", &tags2);

        assert_ne!(
            key1, key2,
            "series keys with different tag structures must not collide"
        );
    }

    #[test]
    fn flush_all_cleans_up_empty_entries() {
        let mut engine = ContinuousAggregationEngine::new(make_config()).unwrap();
        engine.process_event(&write_event(0, 1.0));
        engine.process_event(&write_event(1, 2.0));
        // Flush all buckets
        let _ = engine.flush_all();
        // After flush_all, empty series entries should be cleaned up
        assert_eq!(
            engine.active_bucket_count(),
            0,
            "flush_all should clean up empty entries"
        );
    }

    // ── Series cardinality bounding tests ───────────────────

    fn write_event_tagged(ts_sec: i64, value: f64, host: &str) -> CdcEvent {
        CdcEvent::PointWritten {
            measurement: "cpu".into(),
            tags: BTreeMap::from([("host".into(), host.into())]),
            fields: BTreeMap::from([("usage".into(), FieldValue::F64(value))]),
            timestamp: ts_sec * 1_000_000_000,
            seq: ts_sec as u64,
        }
    }

    #[test]
    fn max_series_evicts_oldest_when_exceeded() {
        let mut engine = ContinuousAggregationEngine::new(make_config())
            .unwrap()
            .with_max_series(3);

        // Insert 3 series — should all fit.
        engine.process_event(&write_event_tagged(10, 1.0, "srv1"));
        engine.process_event(&write_event_tagged(20, 2.0, "srv2"));
        engine.process_event(&write_event_tagged(30, 3.0, "srv3"));
        assert_eq!(engine.tracked_series_count(), 3);

        // Insert a 4th series — should evict srv1 (oldest bucket: t=10).
        let results = engine.process_event(&write_event_tagged(40, 4.0, "srv4"));

        // srv1 should have been force-finalized and evicted.
        assert_eq!(engine.tracked_series_count(), 3);
        assert!(
            results
                .iter()
                .any(|r| r.tags.get("host") == Some(&"srv1".into())),
            "evicted series should produce finalized bucket result"
        );
    }

    #[test]
    fn max_series_existing_series_does_not_trigger_eviction() {
        let mut engine = ContinuousAggregationEngine::new(make_config())
            .unwrap()
            .with_max_series(2);

        engine.process_event(&write_event_tagged(10, 1.0, "srv1"));
        engine.process_event(&write_event_tagged(20, 2.0, "srv2"));
        assert_eq!(engine.tracked_series_count(), 2);

        // Writing to an existing series should NOT trigger eviction.
        let results = engine.process_event(&write_event_tagged(30, 3.0, "srv1"));
        assert_eq!(engine.tracked_series_count(), 2);
        // No eviction results — only possible finalization results.
        assert!(
            !results
                .iter()
                .any(|r| r.tags.get("host") == Some(&"srv2".into())),
            "existing series write should not evict other series"
        );
    }

    #[test]
    fn tracked_series_count_accessor() {
        let mut engine = ContinuousAggregationEngine::new(make_config()).unwrap();
        assert_eq!(engine.tracked_series_count(), 0);
        engine.process_event(&write_event(10, 1.0));
        assert_eq!(engine.tracked_series_count(), 1);
    }
}
