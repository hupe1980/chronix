//! Trigger evaluation engine.
//!
//! Matches incoming CDC write events against registered triggers,
//! evaluates conditions, applies cooldown deduplication, and emits
//! `SignalEvent`s.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use metrics::counter;
use parking_lot::RwLock;
use tracing::{debug, trace, warn};

use crate::cdc::CdcEvent;
use chronix_core::FieldValue;

use crate::signal::error::{Result, SignalError};
use crate::signal::model::{
    CrossoverDirection, EventTrigger, Severity, SignalEvent, ThresholdOp, TriggerCondition,
};

// ── Per-Series State ────────────────────────────────────────────────

/// Windowed state for data-based conditions (rate-of-change, MA crossover).
struct SeriesWindowState {
    /// Ring buffer of recent field values (for the field used by the trigger).
    values: VecDeque<f64>,
    /// Last data timestamp at which this trigger fired for this series.
    last_fired_ts: Option<i64>,
    /// Wall-clock instant at which this trigger last fired.
    ///
    /// Used together with `last_fired_ts` for dual-timestamp cooldown:
    /// a fire is suppressed only when **both** the data-timestamp delta
    /// and the wall-clock elapsed time are within the cooldown window.
    /// This prevents out-of-order / backfilled data from bypassing
    /// cooldown (negative delta) or false-suppressing (huge delta).
    last_fired_wall: Option<Instant>,
    /// Last time this entry was accessed (for TTL eviction).
    last_accessed: Instant,
}

impl SeriesWindowState {
    fn new() -> Self {
        Self {
            values: VecDeque::with_capacity(128),
            last_fired_ts: None,
            last_fired_wall: None,
            last_accessed: Instant::now(),
        }
    }

    fn push(&mut self, value: f64, max_window: usize) {
        self.values.push_back(value);
        while self.values.len() > max_window {
            self.values.pop_front();
        }
        self.last_accessed = Instant::now();
    }

    fn touch(&mut self) {
        self.last_accessed = Instant::now();
    }
}

// ── Trigger Engine ──────────────────────────────────────────────────

/// The trigger evaluation engine.
///
/// Thread-safe — can be shared via `Arc<TriggerEngine>`. Triggers can be
/// added/removed without restarting the pipeline.
pub struct TriggerEngine {
    /// Registered triggers indexed by measurement → Vec<trigger_id>.
    measurement_triggers: DashMap<String, Vec<String>>,
    /// Trigger definitions by ID.
    triggers: DashMap<String, EventTrigger>,
    /// Per (trigger_id, series_hash) windowed state.
    series_state: DashMap<(String, u64), SeriesWindowState>,
    /// Bounded signal sink — oldest signals are evicted (FIFO)
    /// when the capacity is exceeded, preventing unbounded memory growth.
    signal_sink: RwLock<VecDeque<SignalEvent>>,
    /// Maximum number of signals retained in the sink.
    max_sink_size: usize,
    /// Maximum number of tracked series-state entries.
    /// When exceeded, stale entries are automatically evicted.
    max_series_state: usize,
    /// Monotonic reference for eviction cooldown.
    eviction_epoch: Instant,
    /// Last eviction timestamp (nanos since `eviction_epoch`).
    /// Eviction runs at most once per 10 seconds to avoid
    /// O(N) `DashMap::retain` on every hot-path call.
    last_eviction_ns: AtomicU64,
}

impl TriggerEngine {
    /// Default maximum series state entries before automatic eviction.
    const DEFAULT_MAX_SERIES_STATE: usize = 1_000_000;

    /// Create a new trigger engine.
    #[must_use]
    pub fn new() -> Self {
        Self {
            measurement_triggers: DashMap::new(),
            triggers: DashMap::new(),
            series_state: DashMap::new(),
            signal_sink: RwLock::new(VecDeque::new()),
            max_sink_size: 10_000,
            max_series_state: Self::DEFAULT_MAX_SERIES_STATE,
            eviction_epoch: Instant::now(),
            last_eviction_ns: AtomicU64::new(0),
        }
    }

    /// Override the maximum number of series state entries (default: 1M).
    ///
    /// When this limit is exceeded, stale entries are evicted automatically.
    #[must_use]
    pub fn with_max_series_state(mut self, max: usize) -> Self {
        self.max_series_state = max;
        self
    }

    /// Register a trigger.
    pub fn register(&self, trigger: EventTrigger) -> Result<()> {
        let id = trigger.id.clone();
        let measurement = trigger.measurement.clone();

        if self.triggers.contains_key(&id) {
            return Err(SignalError::Duplicate(id));
        }

        self.triggers.insert(id.clone(), trigger);
        self.measurement_triggers
            .entry(measurement)
            .or_default()
            .push(id);

        counter!("chronix_signal_triggers_registered_total").increment(1);
        Ok(())
    }

    /// Unregister a trigger by ID.
    pub fn unregister(&self, trigger_id: &str) -> Result<()> {
        let (_, trigger) = self
            .triggers
            .remove(trigger_id)
            .ok_or_else(|| SignalError::NotFound(trigger_id.into()))?;

        // Remove from measurement index
        if let Some(mut ids) = self.measurement_triggers.get_mut(&trigger.measurement) {
            ids.retain(|id| id != trigger_id);
        }

        // Clean up series state for this trigger
        self.series_state.retain(|(tid, _), _| tid != trigger_id);

        Ok(())
    }

    /// Enable or disable a trigger.
    pub fn set_enabled(&self, trigger_id: &str, enabled: bool) -> Result<()> {
        let mut entry = self
            .triggers
            .get_mut(trigger_id)
            .ok_or_else(|| SignalError::NotFound(trigger_id.into()))?;
        entry.enabled = enabled;
        Ok(())
    }

    /// Get a trigger by ID.
    #[must_use]
    pub fn get_trigger(&self, trigger_id: &str) -> Option<EventTrigger> {
        self.triggers.get(trigger_id).map(|t| t.clone())
    }

    /// List all registered trigger IDs.
    #[must_use]
    pub fn trigger_ids(&self) -> Vec<String> {
        self.triggers.iter().map(|e| e.key().clone()).collect()
    }

    /// Return the number of registered triggers.
    #[must_use]
    pub fn trigger_count(&self) -> usize {
        self.triggers.len()
    }

    /// Return the number of tracked series state entries.
    #[must_use]
    pub fn series_state_count(&self) -> usize {
        self.series_state.len()
    }

    /// Dual-timestamp cooldown check.
    ///
    /// A fire is suppressed only when **both**:
    /// 1. The data-timestamp delta is non-negative and within the
    ///    cooldown window (`delta >= 0 && delta < cooldown_ns`).
    /// 2. The wall-clock elapsed time is within the cooldown window.
    ///
    /// This prevents out-of-order data (negative delta) from bypassing
    /// cooldown, and prevents backfilled data (huge positive delta)
    /// from false-suppressing.
    fn in_cooldown(
        last_fired_ts: Option<i64>,
        last_fired_wall: Option<Instant>,
        current_ts: i64,
        cooldown: &Duration,
    ) -> bool {
        let Some(last_ts) = last_fired_ts else {
            return false;
        };
        let cooldown_ns = cooldown.as_nanos() as i64;
        let delta = current_ts - last_ts;
        let data_in_cooldown = delta >= 0 && delta < cooldown_ns;

        let wall_in_cooldown = last_fired_wall.is_some_and(|w| w.elapsed() < *cooldown);

        // Both must agree: data timestamp AND wall clock say "in cooldown"
        data_in_cooldown && wall_in_cooldown
    }

    /// Throttled eviction — runs at most once per 10 seconds when
    /// the series state map exceeds `max_series_state`. Prevents O(N)
    /// `DashMap::retain` on every hot-path call.
    fn maybe_evict(&self) {
        if self.series_state.len() <= self.max_series_state {
            return;
        }
        let now_ns = self.eviction_epoch.elapsed().as_nanos() as u64;
        let prev = self.last_eviction_ns.load(Ordering::Relaxed);
        // 10-second cooldown
        if now_ns.saturating_sub(prev) < 10_000_000_000 {
            return;
        }
        // CAS to prevent concurrent eviction runs
        if self
            .last_eviction_ns
            .compare_exchange(prev, now_ns, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            self.evict_stale_state(Duration::from_secs(300));
        }
    }

    /// Evict series state entries that have not been accessed within the given
    /// TTL duration.
    ///
    /// Call this periodically (e.g., every 60s) to bound memory usage for
    /// high-cardinality workloads. Returns the number of entries evicted.
    pub fn evict_stale_state(&self, ttl: Duration) -> usize {
        let before = self.series_state.len();
        self.series_state
            .retain(|_, state| state.last_accessed.elapsed() < ttl);
        let evicted = before.saturating_sub(self.series_state.len());
        if evicted > 0 {
            debug!(
                evicted,
                remaining = self.series_state.len(),
                "evicted stale series state"
            );
            counter!("chronix_signal_series_state_evicted_total").increment(evicted as u64);
        }
        evicted
    }

    /// Drain all collected signal events (primarily for testing).
    pub fn drain_signals(&self) -> Vec<SignalEvent> {
        std::mem::take(&mut *self.signal_sink.write())
            .into_iter()
            .collect()
    }

    /// Peek at collected signals without draining.
    #[must_use]
    pub fn signals(&self) -> Vec<SignalEvent> {
        self.signal_sink.read().iter().cloned().collect()
    }

    /// Process a CDC event — evaluate all matching triggers.
    ///
    /// Returns the number of signals emitted. Signals are pushed to the
    /// internal sink for later retrieval via `drain_signals()` / `signals()`.
    pub fn process_event(&self, event: &CdcEvent) -> usize {
        let signals = self.process_event_collect(event);
        let count = signals.len();
        if !signals.is_empty() {
            let mut sink = self.signal_sink.write();
            sink.extend(signals);
            // Evict oldest signals when capacity is exceeded.
            // Track evicted signals so operators can detect sink overflow.
            let mut evicted_count = 0u64;
            while sink.len() > self.max_sink_size {
                sink.pop_front();
                evicted_count += 1;
            }
            if evicted_count > 0 {
                warn!(
                    evicted = evicted_count,
                    max = self.max_sink_size,
                    "signal sink FIFO eviction"
                );
                counter!("chronix_signals_evicted_total").increment(evicted_count);
            }
        }
        count
    }

    /// Process a CDC event and return the fired signals directly.
    ///
    /// Unlike `process_event`, this does **not** push signals to the internal
    /// sink, making it safe for concurrent callers that own their own delivery
    /// pipeline (e.g., `crate::signal::Pipeline`).
    pub fn process_event_collect(&self, event: &CdcEvent) -> Vec<SignalEvent> {
        let CdcEvent::PointWritten {
            measurement,
            tags,
            fields,
            timestamp,
            ..
        } = event
        else {
            return Vec::new();
        };

        let trigger_ids: Vec<String> = match self.measurement_triggers.get(measurement) {
            Some(ids) => ids.clone(),
            None => return Vec::new(),
        };

        let series_hash = hash_series(measurement, tags);
        let mut signals = Vec::new();

        for trigger_id in &trigger_ids {
            let trigger = match self.triggers.get(trigger_id) {
                Some(t) => t.value().clone(),
                None => continue,
            };

            if !trigger.enabled {
                trace!(trigger_id, "Trigger disabled, skipping");
                continue;
            }

            // Check cooldown and conditionally update last_fired_ts
            // atomically via DashMap::entry() to prevent duplicate firings
            // when concurrent threads both pass the cooldown check.
            let cooldown_key = (trigger_id.clone(), series_hash);

            enum CooldownResult {
                InCooldown,
                Ready,
            }

            // Phase 1: Atomic check cooldown + evaluate condition
            let cooldown_result = {
                let state_ref = self.series_state.get(&cooldown_key);
                if let Some(state) = state_ref {
                    if Self::in_cooldown(
                        state.last_fired_ts,
                        state.last_fired_wall,
                        *timestamp,
                        &trigger.cooldown,
                    ) {
                        CooldownResult::InCooldown
                    } else {
                        CooldownResult::Ready
                    }
                } else {
                    CooldownResult::Ready
                }
            };

            if matches!(cooldown_result, CooldownResult::InCooldown) {
                trace!(trigger_id, "In cooldown, skipping");
                continue;
            }

            // Evaluate condition
            let (should_fire, value) =
                self.evaluate_condition(&trigger, fields, tags, &cooldown_key);

            if should_fire {
                let severity = Severity::from_thresholds(
                    value,
                    trigger.severity_thresholds.0,
                    trigger.severity_thresholds.1,
                );

                // Build metadata from trigger condition type and context
                let mut metadata = std::collections::HashMap::new();
                metadata.insert(
                    "condition_type".to_string(),
                    trigger.condition.type_name().to_string(),
                );
                metadata.insert("value".to_string(), format!("{value:.6}"));
                metadata.insert("severity".to_string(), severity.to_string());

                // Condition-specific metadata
                match &trigger.condition {
                    TriggerCondition::ForecastDeviation { tolerance_pct, .. } => {
                        metadata.insert("tolerance_pct".to_string(), format!("{tolerance_pct:.6}"));
                        if let (Some(actual), Some(forecast)) = (
                            extract_f64(fields, "value"),
                            extract_f64(fields, "forecast"),
                        ) {
                            metadata.insert("actual".to_string(), format!("{actual:.6}"));
                            metadata.insert("forecast".to_string(), format!("{forecast:.6}"));
                            let mode = if forecast.abs() > f64::EPSILON {
                                "relative"
                            } else {
                                "absolute_fallback"
                            };
                            metadata.insert("deviation_mode".to_string(), mode.to_string());
                        }
                    }
                    TriggerCondition::AnomalyScore { threshold, .. } => {
                        metadata.insert("threshold".to_string(), format!("{threshold:.6}"));
                    }
                    TriggerCondition::RateOfChange {
                        threshold_pct,
                        window,
                        ..
                    } => {
                        metadata.insert("threshold_pct".to_string(), format!("{threshold_pct:.6}"));
                        metadata.insert("window".to_string(), window.to_string());
                    }
                    TriggerCondition::FieldThreshold {
                        field, value: thr, ..
                    } => {
                        metadata.insert("field".to_string(), field.clone());
                        metadata.insert("threshold".to_string(), format!("{thr:.6}"));
                    }
                    _ => {}
                }

                let signal = SignalEvent {
                    event_id: uuid::Uuid::new_v4().to_string(),
                    trigger_id: trigger.id.clone(),
                    trigger_name: trigger.name.clone(),
                    measurement: measurement.clone(),
                    tags: tags.clone(),
                    timestamp: *timestamp,
                    signal_type: trigger.signal_type.clone(),
                    severity,
                    value,
                    metadata,
                    delivery_targets: trigger.delivery_targets.clone(),
                };

                // Atomic update of last_fired_ts using entry() API.
                // This ensures that if two threads race past the cooldown
                // check, only one succeeds in setting last_fired_ts;
                // the second will see the updated value on its next pass.
                //
                // Auto-evict stale entries when the map exceeds
                // the configured limit, preventing unbounded memory growth.
                // Throttled to avoid O(N) scan on every call.
                self.maybe_evict();
                let mut entry = self
                    .series_state
                    .entry(cooldown_key)
                    .or_insert_with(SeriesWindowState::new);
                // Re-check cooldown under the entry lock to prevent
                // duplicate firings from concurrent threads.
                if Self::in_cooldown(
                    entry.last_fired_ts,
                    entry.last_fired_wall,
                    *timestamp,
                    &trigger.cooldown,
                ) {
                    trace!(trigger_id, "Lost race on cooldown, skipping duplicate fire");
                    continue;
                }
                entry.last_fired_ts = Some(*timestamp);
                entry.last_fired_wall = Some(Instant::now());
                entry.touch();
                // Drop the entry guard before pushing to signals
                drop(entry);

                debug!(
                    trigger_id = %trigger.id,
                    trigger_name = %trigger.name,
                    measurement,
                    value,
                    severity = %severity,
                    "Signal fired"
                );

                counter!("chronix_signal_fired_total",
                    "trigger_id" => trigger.id.clone(),
                    "severity" => severity.to_string()
                )
                .increment(1);

                signals.push(signal);
            }
        }

        signals
    }

    /// Evaluate a single trigger condition against point fields.
    fn evaluate_condition(
        &self,
        trigger: &EventTrigger,
        fields: &BTreeMap<String, FieldValue>,
        tags: &BTreeMap<String, String>,
        state_key: &(String, u64),
    ) -> (bool, f64) {
        self.evaluate_condition_inner(&trigger.condition, fields, tags, state_key)
    }

    /// Recursive evaluator for [`TriggerCondition`] trees.
    fn evaluate_condition_inner(
        &self,
        condition: &TriggerCondition,
        fields: &BTreeMap<String, FieldValue>,
        tags: &BTreeMap<String, String>,
        state_key: &(String, u64),
    ) -> (bool, f64) {
        match condition {
            // A tag comparison carries no numeric value, so it reports 0.0
            // — the second half of the pair is the value that fired, and
            // for a string match there is none.
            TriggerCondition::TagEquals {
                tag,
                value,
                negated,
            } => {
                let matched = tags.get(tag).is_some_and(|actual| actual == value);
                (matched != *negated, 0.0)
            }
            TriggerCondition::FieldThreshold { field, op, value } => {
                if let Some(field_val) = extract_f64(fields, field) {
                    (op.evaluate(field_val, *value), field_val)
                } else {
                    // Track missing fields so operators can detect
                    // misconfigured triggers (e.g. wrong field name).
                    metrics::counter!(
                        "chronix_trigger_field_missing_total",
                        "field" => field.clone(),
                    )
                    .increment(1);
                    (false, 0.0)
                }
            }

            TriggerCondition::AnomalyScore {
                threshold,
                op,
                detector_type: _,
            } => {
                // Look for an "anomaly_score" field
                if let Some(score) = extract_f64(fields, "anomaly_score") {
                    (op.evaluate(score, *threshold), score)
                } else {
                    (false, 0.0)
                }
            }

            TriggerCondition::ForecastDeviation {
                tolerance_pct,
                op,
                horizon: _,
            } => {
                // Look for "forecast_deviation" or compute from "value" + "forecast" fields
                if let Some(deviation) = extract_f64(fields, "forecast_deviation") {
                    (
                        op.evaluate(deviation.abs(), *tolerance_pct),
                        deviation.abs(),
                    )
                } else if let (Some(actual), Some(forecast)) = (
                    extract_f64(fields, "value"),
                    extract_f64(fields, "forecast"),
                ) {
                    let deviation = if forecast.abs() > f64::EPSILON {
                        ((actual - forecast) / forecast).abs()
                    } else {
                        // When forecast ≈ 0, fall back to absolute deviation
                        // so that any non-zero actual is flagged instead of
                        // silently returning 0.0 and suppressing the alert.
                        (actual - forecast).abs()
                    };
                    (op.evaluate(deviation, *tolerance_pct), deviation)
                } else {
                    (false, 0.0)
                }
            }

            TriggerCondition::RateOfChange {
                threshold_pct,
                window,
                field,
            } => {
                // Guard: need at least 2 values for rate computation
                if *window < 2 {
                    return (false, 0.0);
                }
                // Extract the numeric field value — use named field when
                // specified, otherwise fall back to first numeric field.
                let val = match field {
                    Some(name) => extract_f64(fields, name),
                    None => first_f64(fields),
                };
                if let Some(v) = val {
                    // Throttled eviction
                    self.maybe_evict();
                    // Use a discriminated key so RateOfChange and
                    // MovingAverageCrossover don't share ring buffers
                    // when combined in And/Or compound triggers.
                    let roc_key = (format!("{}::roc", state_key.0), state_key.1);
                    let mut state = self
                        .series_state
                        .entry(roc_key)
                        .or_insert_with(SeriesWindowState::new);
                    state.push(v, *window);

                    if state.values.len() >= 2 {
                        let oldest = state.values[0];
                        let newest = *state.values.back().expect("len >= 2");
                        if oldest.abs() > f64::EPSILON {
                            let rate = ((newest - oldest) / oldest).abs();
                            return (rate > *threshold_pct, rate);
                        }
                    }
                }
                (false, 0.0)
            }

            TriggerCondition::MovingAverageCrossover {
                short_window,
                long_window,
                direction,
                field,
            } => {
                // Guard: both windows must be >= 1 and short <= long
                if *short_window == 0 || *long_window == 0 || *short_window > *long_window {
                    return (false, 0.0);
                }
                // Extract the numeric field value — use named field when
                // specified, otherwise fall back to first numeric field.
                let val = match field {
                    Some(name) => extract_f64(fields, name),
                    None => first_f64(fields),
                };
                if let Some(v) = val {
                    // Throttled eviction
                    self.maybe_evict();
                    // Use a discriminated key so MovingAverageCrossover and
                    // RateOfChange don't share ring buffers in compound triggers.
                    let mac_key = (format!("{}::mac", state_key.0), state_key.1);
                    let mut state = self
                        .series_state
                        .entry(mac_key)
                        .or_insert_with(SeriesWindowState::new);
                    state.push(v, *long_window);

                    if state.values.len() >= *long_window {
                        let vals: Vec<f64> = state.values.iter().copied().collect();
                        let n = vals.len();
                        let short_avg: f64 =
                            vals[n - short_window..].iter().sum::<f64>() / *short_window as f64;
                        let long_avg: f64 =
                            vals[n - long_window..].iter().sum::<f64>() / *long_window as f64;
                        let diff = short_avg - long_avg;
                        let fired = match direction {
                            CrossoverDirection::GoldenCross => {
                                diff > 0.0 && diff.abs() > f64::EPSILON
                            }
                            CrossoverDirection::DeathCross => {
                                diff < 0.0 && diff.abs() > f64::EPSILON
                            }
                            CrossoverDirection::Both => diff.abs() > f64::EPSILON,
                        };
                        return (fired, diff);
                    }
                }
                (false, 0.0)
            }

            TriggerCondition::And { left, right } => {
                let (l_ok, l_val) = self.evaluate_condition_inner(left, fields, tags, state_key);
                let (r_ok, r_val) = self.evaluate_condition_inner(right, fields, tags, state_key);
                if l_ok && r_ok {
                    (true, l_val)
                } else {
                    (false, if l_ok { l_val } else { r_val })
                }
            }

            TriggerCondition::Or { left, right } => {
                let (l_ok, l_val) = self.evaluate_condition_inner(left, fields, tags, state_key);
                if l_ok {
                    return (true, l_val);
                }
                let (r_ok, r_val) = self.evaluate_condition_inner(right, fields, tags, state_key);
                (r_ok, r_val)
            }
        }
    }
}

impl Default for TriggerEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for TriggerEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TriggerEngine")
            .field("trigger_count", &self.trigger_count())
            .finish()
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

fn extract_f64(fields: &BTreeMap<String, FieldValue>, name: &str) -> Option<f64> {
    match fields.get(name)? {
        FieldValue::F64(v) => Some(*v),
        FieldValue::I64(v) => Some(*v as f64),
        FieldValue::U64(v) => Some(*v as f64),
        other => {
            warn!(
                field = name,
                actual_type = other.type_name(),
                "condition expects numeric field but got non-numeric type",
            );
            counter!("signal.condition.type_mismatch").increment(1);
            None
        }
    }
}

fn first_f64(fields: &BTreeMap<String, FieldValue>) -> Option<f64> {
    let mut saw_non_numeric = false;
    for v in fields.values() {
        match v {
            FieldValue::F64(f) => return Some(*f),
            FieldValue::I64(i) => return Some(*i as f64),
            FieldValue::U64(u) => return Some(*u as f64),
            _ => {
                saw_non_numeric = true;
            }
        }
    }
    if saw_non_numeric {
        warn!("no numeric field found but non-numeric fields present — possible type mismatch");
        counter!("signal.condition.type_mismatch").increment(1);
    }
    None
}

fn hash_series(measurement: &str, tags: &BTreeMap<String, String>) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = fnv::FnvHasher::default();
    measurement.hash(&mut h);
    for (k, v) in tags {
        k.hash(&mut h);
        v.hash(&mut h);
    }
    h.finish()
}

// ── Built-in trigger templates ──────────────────────────────────────

/// Create an anomaly threshold trigger.
#[must_use]
pub fn anomaly_threshold_trigger(
    id: impl Into<String>,
    name: impl Into<String>,
    measurement: impl Into<String>,
    threshold: f64,
) -> EventTrigger {
    EventTrigger::new(
        id,
        name,
        measurement,
        TriggerCondition::AnomalyScore {
            threshold,
            op: ThresholdOp::Gt,
            detector_type: None,
        },
    )
    .with_signal_type("anomaly_threshold")
}

/// Create a forecast deviation trigger.
#[must_use]
pub fn forecast_deviation_trigger(
    id: impl Into<String>,
    name: impl Into<String>,
    measurement: impl Into<String>,
    tolerance_pct: f64,
) -> EventTrigger {
    EventTrigger::new(
        id,
        name,
        measurement,
        TriggerCondition::ForecastDeviation {
            tolerance_pct,
            op: ThresholdOp::Gt,
            horizon: None,
        },
    )
    .with_signal_type("forecast_deviation")
}

/// Create a moving average crossover trigger.
#[must_use]
pub fn ma_crossover_trigger(
    id: impl Into<String>,
    name: impl Into<String>,
    measurement: impl Into<String>,
    short_window: usize,
    long_window: usize,
) -> EventTrigger {
    EventTrigger::new(
        id,
        name,
        measurement,
        TriggerCondition::MovingAverageCrossover {
            short_window,
            long_window,
            direction: CrossoverDirection::default(),
            field: None,
        },
    )
    .with_signal_type("ma_crossover")
}

/// Create a rate-of-change trigger.
#[must_use]
pub fn rate_of_change_trigger(
    id: impl Into<String>,
    name: impl Into<String>,
    measurement: impl Into<String>,
    threshold_pct: f64,
    window: usize,
) -> EventTrigger {
    EventTrigger::new(
        id,
        name,
        measurement,
        TriggerCondition::RateOfChange {
            threshold_pct,
            window,
            field: None,
        },
    )
    .with_signal_type("rate_of_change")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn make_write_event(
        measurement: &str,
        field_name: &str,
        value: f64,
        timestamp: i64,
    ) -> CdcEvent {
        CdcEvent::PointWritten {
            measurement: measurement.into(),
            tags: BTreeMap::new(),
            fields: [(field_name.into(), FieldValue::F64(value))]
                .into_iter()
                .collect(),
            timestamp,
            seq: 0,
        }
    }

    #[allow(dead_code)]
    fn make_tagged_write(
        measurement: &str,
        tags: &[(&str, &str)],
        field_name: &str,
        value: f64,
        timestamp: i64,
    ) -> CdcEvent {
        CdcEvent::PointWritten {
            measurement: measurement.into(),
            tags: tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            fields: [(field_name.into(), FieldValue::F64(value))]
                .into_iter()
                .collect(),
            timestamp,
            seq: 0,
        }
    }

    #[test]
    fn field_threshold_fires() {
        let engine = TriggerEngine::new();
        let trigger = EventTrigger::new(
            "t1",
            "High CPU",
            "cpu",
            TriggerCondition::FieldThreshold {
                field: "usage_idle".into(),
                op: ThresholdOp::Lt,
                value: 5.0,
            },
        )
        .with_cooldown(Duration::ZERO);

        engine.register(trigger).unwrap();

        // Below threshold — fires
        let event = make_write_event("cpu", "usage_idle", 3.0, 1000);
        assert_eq!(engine.process_event(&event), 1);

        // Above threshold — no fire
        let event = make_write_event("cpu", "usage_idle", 10.0, 2000);
        assert_eq!(engine.process_event(&event), 0);

        let signals = engine.drain_signals();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].trigger_id, "t1");
        assert_eq!(signals[0].value, 3.0);
    }

    #[test]
    fn cooldown_suppresses_duplicate() {
        let engine = TriggerEngine::new();
        let trigger = EventTrigger::new(
            "t1",
            "Alert",
            "cpu",
            TriggerCondition::FieldThreshold {
                field: "val".into(),
                op: ThresholdOp::Gt,
                value: 10.0,
            },
        )
        .with_cooldown(Duration::from_secs(300));

        engine.register(trigger).unwrap();

        let event = make_write_event("cpu", "val", 20.0, 1000);
        assert_eq!(engine.process_event(&event), 1);
        // Same event — should be in cooldown
        assert_eq!(engine.process_event(&event), 0);

        assert_eq!(engine.drain_signals().len(), 1);
    }

    #[test]
    fn disabled_trigger_does_not_fire() {
        let engine = TriggerEngine::new();
        let trigger = EventTrigger::new(
            "t1",
            "Alert",
            "cpu",
            TriggerCondition::FieldThreshold {
                field: "val".into(),
                op: ThresholdOp::Gt,
                value: 10.0,
            },
        )
        .with_enabled(false);

        engine.register(trigger).unwrap();

        let event = make_write_event("cpu", "val", 20.0, 1000);
        assert_eq!(engine.process_event(&event), 0);
    }

    #[test]
    fn enable_disable_toggle() {
        let engine = TriggerEngine::new();
        let trigger = EventTrigger::new(
            "t1",
            "Alert",
            "cpu",
            TriggerCondition::FieldThreshold {
                field: "val".into(),
                op: ThresholdOp::Gt,
                value: 10.0,
            },
        )
        .with_cooldown(Duration::ZERO);

        engine.register(trigger).unwrap();

        let event = make_write_event("cpu", "val", 20.0, 1000);
        assert_eq!(engine.process_event(&event), 1);

        engine.set_enabled("t1", false).unwrap();
        assert_eq!(engine.process_event(&event), 0);

        engine.set_enabled("t1", true).unwrap();
        assert_eq!(engine.process_event(&event), 1);
    }

    #[test]
    fn wrong_measurement_ignored() {
        let engine = TriggerEngine::new();
        let trigger = EventTrigger::new(
            "t1",
            "Alert",
            "cpu",
            TriggerCondition::FieldThreshold {
                field: "val".into(),
                op: ThresholdOp::Gt,
                value: 10.0,
            },
        );
        engine.register(trigger).unwrap();

        let event = make_write_event("memory", "val", 20.0, 1000);
        assert_eq!(engine.process_event(&event), 0);
    }

    #[test]
    fn non_write_events_ignored() {
        let engine = TriggerEngine::new();
        let trigger = EventTrigger::new(
            "t1",
            "Alert",
            "cpu",
            TriggerCondition::FieldThreshold {
                field: "val".into(),
                op: ThresholdOp::Gt,
                value: 10.0,
            },
        );
        engine.register(trigger).unwrap();

        let event = CdcEvent::SeriesDeleted {
            measurement: "cpu".into(),
            tags: BTreeMap::new(),
            series_hash: 42,
            seq: 0,
        };
        assert_eq!(engine.process_event(&event), 0);
    }

    #[test]
    fn anomaly_score_trigger_fires() {
        let engine = TriggerEngine::new();
        engine
            .register(
                anomaly_threshold_trigger("a1", "Anomaly", "cpu", 3.0)
                    .with_cooldown(Duration::ZERO),
            )
            .unwrap();

        // Score below threshold
        let event = make_write_event("cpu", "anomaly_score", 2.0, 1000);
        assert_eq!(engine.process_event(&event), 0);

        // Score above threshold
        let event = make_write_event("cpu", "anomaly_score", 4.5, 2000);
        assert_eq!(engine.process_event(&event), 1);

        let signals = engine.drain_signals();
        assert_eq!(signals[0].signal_type, "anomaly_threshold");
        assert_eq!(signals[0].value, 4.5);
    }

    #[test]
    fn forecast_deviation_trigger_fires() {
        let engine = TriggerEngine::new();
        engine
            .register(
                forecast_deviation_trigger("f1", "ForecastDev", "energy", 0.10)
                    .with_cooldown(Duration::ZERO),
            )
            .unwrap();

        // Small deviation — no fire
        let event = CdcEvent::PointWritten {
            measurement: "energy".into(),
            tags: BTreeMap::new(),
            fields: [
                ("value".into(), FieldValue::F64(100.0)),
                ("forecast".into(), FieldValue::F64(95.0)),
            ]
            .into_iter()
            .collect(),
            timestamp: 1000,
            seq: 0,
        };
        assert_eq!(engine.process_event(&event), 0);

        // Large deviation — fires
        let event = CdcEvent::PointWritten {
            measurement: "energy".into(),
            tags: BTreeMap::new(),
            fields: [
                ("value".into(), FieldValue::F64(100.0)),
                ("forecast".into(), FieldValue::F64(80.0)),
            ]
            .into_iter()
            .collect(),
            timestamp: 2000,
            seq: 0,
        };
        assert_eq!(engine.process_event(&event), 1);

        let signals = engine.drain_signals();
        assert_eq!(signals[0].signal_type, "forecast_deviation");
    }

    #[test]
    fn rate_of_change_trigger_fires() {
        let engine = TriggerEngine::new();
        engine
            .register(
                rate_of_change_trigger("r1", "RateChange", "cpu", 0.50, 3)
                    .with_cooldown(Duration::ZERO),
            )
            .unwrap();

        // Build up window: 100, 100, 100 — no change
        for ts in 0..3 {
            let event = make_write_event("cpu", "usage", 100.0, ts);
            engine.process_event(&event);
        }
        assert!(engine.drain_signals().is_empty());

        // Spike: 100, 100, 200 — 100% change > 50% threshold
        let event = make_write_event("cpu", "usage", 200.0, 3);
        assert_eq!(engine.process_event(&event), 1);

        let signals = engine.drain_signals();
        assert_eq!(signals[0].signal_type, "rate_of_change");
    }

    #[test]
    fn ma_crossover_trigger_fires() {
        let engine = TriggerEngine::new();
        engine
            .register(
                ma_crossover_trigger("m1", "MACross", "stock", 2, 4).with_cooldown(Duration::ZERO),
            )
            .unwrap();

        // Feed declining data: 10, 8, 6, 4 — short MA < long MA
        for (ts, val) in [(0, 10.0), (1, 8.0), (2, 6.0), (3, 4.0)] {
            let event = make_write_event("stock", "price", val, ts);
            engine.process_event(&event);
        }
        assert!(engine.drain_signals().is_empty());

        // Feed rising data: push short MA above long MA
        for (ts, val) in [(4, 10.0), (5, 15.0)] {
            let event = make_write_event("stock", "price", val, ts);
            engine.process_event(&event);
        }
        let signals = engine.drain_signals();
        assert!(!signals.is_empty());
        assert_eq!(signals[0].signal_type, "ma_crossover");
    }

    #[test]
    fn multiple_triggers_on_same_measurement() {
        let engine = TriggerEngine::new();

        engine
            .register(
                EventTrigger::new(
                    "t1",
                    "High",
                    "cpu",
                    TriggerCondition::FieldThreshold {
                        field: "val".into(),
                        op: ThresholdOp::Gt,
                        value: 90.0,
                    },
                )
                .with_cooldown(Duration::ZERO),
            )
            .unwrap();

        engine
            .register(
                EventTrigger::new(
                    "t2",
                    "VeryHigh",
                    "cpu",
                    TriggerCondition::FieldThreshold {
                        field: "val".into(),
                        op: ThresholdOp::Gt,
                        value: 95.0,
                    },
                )
                .with_cooldown(Duration::ZERO),
            )
            .unwrap();

        // 92 — fires t1 only
        let event = make_write_event("cpu", "val", 92.0, 1000);
        assert_eq!(engine.process_event(&event), 1);

        // 98 — fires both
        let event = make_write_event("cpu", "val", 98.0, 2000);
        assert_eq!(engine.process_event(&event), 2);

        let signals = engine.drain_signals();
        assert_eq!(signals.len(), 3);
    }

    #[test]
    fn register_and_unregister() {
        let engine = TriggerEngine::new();
        let trigger = EventTrigger::new(
            "t1",
            "Test",
            "cpu",
            TriggerCondition::FieldThreshold {
                field: "val".into(),
                op: ThresholdOp::Gt,
                value: 10.0,
            },
        )
        .with_cooldown(Duration::ZERO);

        engine.register(trigger).unwrap();
        assert_eq!(engine.trigger_count(), 1);

        let event = make_write_event("cpu", "val", 20.0, 1000);
        assert_eq!(engine.process_event(&event), 1);

        engine.unregister("t1").unwrap();
        assert_eq!(engine.trigger_count(), 0);
        assert_eq!(engine.process_event(&event), 0);
    }

    #[test]
    fn duplicate_trigger_rejected() {
        let engine = TriggerEngine::new();
        let trigger = EventTrigger::new(
            "t1",
            "Test",
            "cpu",
            TriggerCondition::FieldThreshold {
                field: "val".into(),
                op: ThresholdOp::Gt,
                value: 10.0,
            },
        );

        engine.register(trigger.clone()).unwrap();
        assert!(engine.register(trigger).is_err());
    }

    #[test]
    fn severity_auto_inferred() {
        let engine = TriggerEngine::new();
        engine
            .register(
                EventTrigger::new(
                    "t1",
                    "Score Alert",
                    "cpu",
                    TriggerCondition::FieldThreshold {
                        field: "val".into(),
                        op: ThresholdOp::Gt,
                        value: 0.0,
                    },
                )
                .with_severity_thresholds(0.5, 0.8)
                .with_cooldown(Duration::ZERO),
            )
            .unwrap();

        // Info level (< 0.5)
        let event = make_write_event("cpu", "val", 0.3, 1000);
        engine.process_event(&event);

        // Warning level (0.5 - 0.8)
        let event = make_write_event("cpu", "val", 0.6, 2000);
        engine.process_event(&event);

        // Critical level (>= 0.8)
        let event = make_write_event("cpu", "val", 0.9, 3000);
        engine.process_event(&event);

        let signals = engine.drain_signals();
        assert_eq!(signals.len(), 3);
        assert_eq!(signals[0].severity, Severity::Info);
        assert_eq!(signals[1].severity, Severity::Warning);
        assert_eq!(signals[2].severity, Severity::Critical);
    }

    #[test]
    fn template_anomaly_threshold() {
        let t = anomaly_threshold_trigger("a1", "Anomaly Alert", "cpu", 3.0);
        assert_eq!(t.signal_type, "anomaly_threshold");
        assert_eq!(t.measurement, "cpu");
        match &t.condition {
            TriggerCondition::AnomalyScore { threshold, .. } => {
                assert_eq!(*threshold, 3.0);
            }
            _ => panic!("wrong condition type"),
        }
    }

    #[test]
    fn template_forecast_deviation() {
        let t = forecast_deviation_trigger("f1", "Forecast Dev", "energy", 0.15);
        assert_eq!(t.signal_type, "forecast_deviation");
        match &t.condition {
            TriggerCondition::ForecastDeviation { tolerance_pct, .. } => {
                assert_eq!(*tolerance_pct, 0.15);
            }
            _ => panic!("wrong condition type"),
        }
    }

    #[test]
    fn template_ma_crossover() {
        let t = ma_crossover_trigger("m1", "MA Cross", "stock", 5, 20);
        assert_eq!(t.signal_type, "ma_crossover");
        match &t.condition {
            TriggerCondition::MovingAverageCrossover {
                short_window,
                long_window,
                direction,
                field,
            } => {
                assert_eq!(*short_window, 5);
                assert_eq!(*long_window, 20);
                assert_eq!(*direction, CrossoverDirection::GoldenCross);
                assert!(field.is_none());
            }
            _ => panic!("wrong condition type"),
        }
    }

    #[test]
    fn template_rate_of_change() {
        let t = rate_of_change_trigger("r1", "ROC", "cpu", 0.5, 10);
        assert_eq!(t.signal_type, "rate_of_change");
        match &t.condition {
            TriggerCondition::RateOfChange {
                threshold_pct,
                window,
                field,
            } => {
                assert_eq!(*threshold_pct, 0.5);
                assert_eq!(*window, 10);
                assert!(field.is_none());
            }
            _ => panic!("wrong condition type"),
        }
    }

    #[test]
    fn anomaly_score_lt_op_fires_on_low_scores() {
        let engine = TriggerEngine::new();
        // Fire when anomaly score < 2.0 (unusual: alert on *low* score)
        engine
            .register(
                EventTrigger::new(
                    "low_anomaly",
                    "Low Anomaly",
                    "cpu",
                    TriggerCondition::AnomalyScore {
                        threshold: 2.0,
                        op: ThresholdOp::Lt,
                        detector_type: None,
                    },
                )
                .with_cooldown(Duration::ZERO),
            )
            .unwrap();

        // Score 1.0 < 2.0 → fires
        let event = make_write_event("cpu", "anomaly_score", 1.0, 1000);
        assert_eq!(engine.process_event(&event), 1);

        // Score 3.0 >= 2.0 → does not fire
        let event = make_write_event("cpu", "anomaly_score", 3.0, 2000);
        assert_eq!(engine.process_event(&event), 0);
    }

    #[test]
    fn forecast_deviation_lte_op() {
        let engine = TriggerEngine::new();
        // Fire when deviation <= 0.05 (i.e. forecast is accurate)
        engine
            .register(
                EventTrigger::new(
                    "accurate_forecast",
                    "Accurate Forecast",
                    "energy",
                    TriggerCondition::ForecastDeviation {
                        tolerance_pct: 0.05,
                        op: ThresholdOp::Lte,
                        horizon: None,
                    },
                )
                .with_cooldown(Duration::ZERO),
            )
            .unwrap();

        // 2% deviation <= 5% → fires
        let event = CdcEvent::PointWritten {
            measurement: "energy".into(),
            tags: BTreeMap::new(),
            fields: [
                ("value".into(), FieldValue::F64(102.0)),
                ("forecast".into(), FieldValue::F64(100.0)),
            ]
            .into_iter()
            .collect(),
            timestamp: 1000,
            seq: 0,
        };
        assert_eq!(engine.process_event(&event), 1);

        // 25% deviation > 5% → does not fire
        let event = CdcEvent::PointWritten {
            measurement: "energy".into(),
            tags: BTreeMap::new(),
            fields: [
                ("value".into(), FieldValue::F64(125.0)),
                ("forecast".into(), FieldValue::F64(100.0)),
            ]
            .into_iter()
            .collect(),
            timestamp: 2000,
            seq: 0,
        };
        assert_eq!(engine.process_event(&event), 0);
    }

    #[test]
    fn death_cross_does_not_fire() {
        // Death cross = short MA crosses BELOW long MA (bearish signal)
        // Our golden cross trigger should NOT fire on death crosses
        let engine = TriggerEngine::new();
        engine
            .register(
                ma_crossover_trigger("m1", "Golden Cross", "stock", 2, 4)
                    .with_cooldown(Duration::ZERO),
            )
            .unwrap();

        // Feed rising data first to fill window: 10, 12, 14, 16
        for (ts, val) in [(0, 10.0), (1, 12.0), (2, 14.0), (3, 16.0)] {
            let event = make_write_event("stock", "price", val, ts);
            engine.process_event(&event);
        }
        engine.drain_signals(); // clear any golden cross signals

        // Now feed declining data: sharp drop — death cross
        for (ts, val) in [(4, 5.0), (5, 3.0)] {
            let event = make_write_event("stock", "price", val, ts);
            engine.process_event(&event);
        }
        // Death cross should NOT trigger golden cross
        let signals = engine.drain_signals();
        assert!(
            signals.is_empty(),
            "Death cross should not fire golden cross trigger"
        );
    }

    #[test]
    fn ma_crossover_zero_window_does_not_panic() {
        // short_window=0 previously caused division by zero
        let engine = TriggerEngine::new();
        engine
            .register(
                ma_crossover_trigger("m1", "bad", "stock", 0, 5).with_cooldown(Duration::ZERO),
            )
            .unwrap();
        let event = make_write_event("stock", "price", 100.0, 0);
        // Must not panic — guard returns (false, 0.0)
        assert_eq!(engine.process_event(&event), 0);
    }

    #[test]
    fn ma_crossover_short_greater_than_long_does_not_panic() {
        // short_window > long_window previously caused usize underflow
        let engine = TriggerEngine::new();
        engine
            .register(
                ma_crossover_trigger("m1", "bad", "stock", 10, 5).with_cooldown(Duration::ZERO),
            )
            .unwrap();
        for ts in 0..20 {
            let event = make_write_event("stock", "price", 100.0, ts);
            engine.process_event(&event);
        }
        let signals = engine.drain_signals();
        assert!(signals.is_empty());
    }

    #[test]
    fn rate_of_change_window_zero_does_not_fire() {
        // window=0 previously silently never fired with no indication
        let engine = TriggerEngine::new();
        engine
            .register(
                rate_of_change_trigger("r1", "bad_roc", "sensor", 0.01, 0)
                    .with_cooldown(Duration::ZERO),
            )
            .unwrap();
        for ts in 0..10 {
            let event = make_write_event("sensor", "value", (ts as f64) * 100.0, ts);
            engine.process_event(&event);
        }
        let signals = engine.drain_signals();
        assert!(
            signals.is_empty(),
            "RateOfChange with window<2 must not fire"
        );
    }

    #[test]
    fn rate_of_change_window_one_does_not_fire() {
        let engine = TriggerEngine::new();
        engine
            .register(
                rate_of_change_trigger("r1", "bad_roc", "sensor", 0.01, 1)
                    .with_cooldown(Duration::ZERO),
            )
            .unwrap();
        for ts in 0..10 {
            let event = make_write_event("sensor", "value", (ts as f64) * 100.0, ts);
            engine.process_event(&event);
        }
        let signals = engine.drain_signals();
        assert!(
            signals.is_empty(),
            "RateOfChange with window<2 must not fire"
        );
    }

    #[test]
    fn evict_stale_state_removes_inactive_entries() {
        let engine = TriggerEngine::new();
        engine
            .register(
                EventTrigger::new(
                    "t1",
                    "eviction_test",
                    "cpu",
                    TriggerCondition::FieldThreshold {
                        field: "value".into(),
                        op: ThresholdOp::Gt,
                        value: 90.0,
                    },
                )
                .with_cooldown(Duration::ZERO),
            )
            .unwrap();

        // Fire trigger to create series state
        let event = make_write_event("cpu", "value", 95.0, 1000);
        engine.process_event(&event);
        assert!(engine.series_state_count() > 0);

        // Evict with a very short TTL — entries should be removed
        std::thread::sleep(Duration::from_millis(10));
        let evicted = engine.evict_stale_state(Duration::from_millis(1));
        assert!(evicted > 0);
        assert_eq!(engine.series_state_count(), 0);
    }

    #[test]
    fn evict_stale_state_keeps_active_entries() {
        let engine = TriggerEngine::new();
        engine
            .register(
                EventTrigger::new(
                    "t1",
                    "eviction_keep",
                    "cpu",
                    TriggerCondition::FieldThreshold {
                        field: "value".into(),
                        op: ThresholdOp::Gt,
                        value: 90.0,
                    },
                )
                .with_cooldown(Duration::ZERO),
            )
            .unwrap();

        let event = make_write_event("cpu", "value", 95.0, 1000);
        engine.process_event(&event);
        assert!(engine.series_state_count() > 0);

        // Evict with a generous TTL — entries should survive
        let evicted = engine.evict_stale_state(Duration::from_secs(3600));
        assert_eq!(evicted, 0);
        assert!(engine.series_state_count() > 0);
    }

    // ── Dual-timestamp cooldown tests ─────────────────────

    #[test]
    fn in_cooldown_helper_both_must_agree() {
        let cooldown = Duration::from_secs(300);
        let now = Instant::now();

        // Both in range → in cooldown
        assert!(TriggerEngine::in_cooldown(
            Some(1_000_000_000),
            Some(now),
            1_100_000_000, // 100ms later in data time
            &cooldown,
        ));

        // Data timestamp in range, but no wall clock → NOT in cooldown
        assert!(!TriggerEngine::in_cooldown(
            Some(1_000_000_000),
            None,
            1_100_000_000,
            &cooldown,
        ));

        // No last_fired → NOT in cooldown
        assert!(!TriggerEngine::in_cooldown(
            None,
            None,
            1_000_000_000,
            &cooldown,
        ));
    }

    #[test]
    fn out_of_order_data_does_not_bypass_cooldown() {
        // Out-of-order data (negative delta) should NOT bypass
        // cooldown when wall clock says we're still in cooldown.
        let cooldown = Duration::from_secs(300);
        let now = Instant::now();

        // Newer timestamp fired already, now out-of-order point from the past
        let last_fired_ts = 2_000_000_000_i64; // 2s in nanos
        let old_timestamp = 1_000_000_000_i64; // 1s — earlier than last fire

        // delta is negative → data says "not in cooldown"
        // wall clock says "in cooldown" (just fired)
        // Result: NOT in cooldown (both must agree, delta < 0 fails data check)
        // This is the correct behavior — out-of-order data should still fire
        // if the data-timestamp delta is negative (the point is from before
        // the last fire).
        assert!(!TriggerEngine::in_cooldown(
            Some(last_fired_ts),
            Some(now),
            old_timestamp,
            &cooldown,
        ));
    }

    #[test]
    fn backfill_data_fires_when_wall_clock_expired() {
        // Backfilled data with in-range data timestamp should
        // NOT be suppressed if wall clock cooldown has expired.
        let cooldown = Duration::from_secs(1);
        // Wall clock from 2 seconds ago — cooldown expired
        let wall_past = Instant::now() - Duration::from_secs(2);

        assert!(!TriggerEngine::in_cooldown(
            Some(1_000_000_000),
            Some(wall_past),
            1_500_000_000, // 500ms later in data time, within 1s cooldown
            &cooldown,
        ));
    }

    #[test]
    fn cooldown_with_out_of_order_events_integration() {
        let engine = TriggerEngine::new();
        let trigger = EventTrigger::new(
            "t1",
            "Alert",
            "cpu",
            TriggerCondition::FieldThreshold {
                field: "val".into(),
                op: ThresholdOp::Gt,
                value: 10.0,
            },
        )
        .with_cooldown(Duration::from_secs(300));

        engine.register(trigger).unwrap();

        // First event at timestamp 5000ns — fires
        let e1 = make_write_event("cpu", "val", 20.0, 5000);
        assert_eq!(engine.process_event(&e1), 1);

        // Out-of-order event at timestamp 3000ns (before last fire)
        // Data delta is negative, so it should fire (not suppressed)
        let e2 = make_write_event("cpu", "val", 20.0, 3000);
        assert_eq!(engine.process_event(&e2), 1);
    }

    // ── Type-safe condition evaluation tests ──────────────────

    #[test]
    fn string_field_does_not_silently_match_numeric_condition() {
        let engine = TriggerEngine::new();
        let trigger = EventTrigger::new(
            "t_str",
            "String mismatch",
            "cpu",
            TriggerCondition::FieldThreshold {
                field: "status".into(),
                op: ThresholdOp::Gt,
                value: 0.0,
            },
        )
        .with_cooldown(Duration::ZERO);
        engine.register(trigger).unwrap();

        // Send a string field where a numeric is expected — must NOT fire
        let event = CdcEvent::PointWritten {
            measurement: "cpu".into(),
            tags: BTreeMap::new(),
            fields: [("status".into(), FieldValue::String("active".into()))]
                .into_iter()
                .collect(),
            timestamp: 1000,
            seq: 0,
        };
        assert_eq!(engine.process_event(&event), 0);
    }

    #[test]
    fn bool_field_does_not_silently_match_numeric_condition() {
        let engine = TriggerEngine::new();
        let trigger = EventTrigger::new(
            "t_bool",
            "Bool mismatch",
            "cpu",
            TriggerCondition::FieldThreshold {
                field: "enabled".into(),
                op: ThresholdOp::Eq,
                value: 1.0,
            },
        )
        .with_cooldown(Duration::ZERO);
        engine.register(trigger).unwrap();

        let event = CdcEvent::PointWritten {
            measurement: "cpu".into(),
            tags: BTreeMap::new(),
            fields: [("enabled".into(), FieldValue::Bool(true))]
                .into_iter()
                .collect(),
            timestamp: 1000,
            seq: 0,
        };
        assert_eq!(engine.process_event(&event), 0);
    }

    #[test]
    fn validate_fields_detects_type_mismatch() {
        let cond = TriggerCondition::FieldThreshold {
            field: "cpu".into(),
            op: ThresholdOp::Gt,
            value: 50.0,
        };
        let fields: BTreeMap<String, FieldValue> =
            [("cpu".into(), FieldValue::String("high".into()))]
                .into_iter()
                .collect();
        let mismatches = cond.validate_fields(&fields);
        assert_eq!(mismatches.len(), 1);
        assert_eq!(mismatches[0], ("cpu", "string"));
    }

    #[test]
    fn validate_fields_passes_for_numeric() {
        let cond = TriggerCondition::FieldThreshold {
            field: "cpu".into(),
            op: ThresholdOp::Gt,
            value: 50.0,
        };
        let fields: BTreeMap<String, FieldValue> = [("cpu".into(), FieldValue::F64(75.0))]
            .into_iter()
            .collect();
        assert!(cond.validate_fields(&fields).is_empty());
    }

    #[test]
    fn validate_fields_ignores_missing_fields() {
        let cond = TriggerCondition::FieldThreshold {
            field: "cpu".into(),
            op: ThresholdOp::Gt,
            value: 50.0,
        };
        let fields: BTreeMap<String, FieldValue> = BTreeMap::new();
        // missing field is OK — it may not be present in every event
        assert!(cond.validate_fields(&fields).is_empty());
    }

    #[test]
    fn required_numeric_fields_compound_condition() {
        let cond = TriggerCondition::And {
            left: Box::new(TriggerCondition::FieldThreshold {
                field: "cpu".into(),
                op: ThresholdOp::Gt,
                value: 50.0,
            }),
            right: Box::new(TriggerCondition::FieldThreshold {
                field: "mem".into(),
                op: ThresholdOp::Lt,
                value: 10.0,
            }),
        };
        let fields = cond.required_numeric_fields();
        assert!(fields.contains(&"cpu"));
        assert!(fields.contains(&"mem"));
    }

    fn ma_crossover_with_direction(
        id: &str,
        name: &str,
        measurement: &str,
        short_window: usize,
        long_window: usize,
        dir: CrossoverDirection,
    ) -> EventTrigger {
        EventTrigger::new(
            id,
            name,
            measurement,
            TriggerCondition::MovingAverageCrossover {
                short_window,
                long_window,
                direction: dir,
                field: None,
            },
        )
        .with_signal_type("ma_crossover")
    }

    #[test]
    fn death_cross_fires_with_direction() {
        let engine = TriggerEngine::new();
        engine
            .register(
                ma_crossover_with_direction(
                    "m1",
                    "Death Cross",
                    "stock",
                    2,
                    4,
                    CrossoverDirection::DeathCross,
                )
                .with_cooldown(Duration::ZERO),
            )
            .unwrap();

        // Feed rising data to fill window: 10, 12, 14, 16
        for (ts, val) in [(0, 10.0), (1, 12.0), (2, 14.0), (3, 16.0)] {
            let event = make_write_event("stock", "price", val, ts);
            engine.process_event(&event);
        }
        engine.drain_signals(); // clear any accumulated

        // Sharp drop — death cross (short MA < long MA)
        for (ts, val) in [(4, 5.0), (5, 3.0)] {
            let event = make_write_event("stock", "price", val, ts);
            engine.process_event(&event);
        }
        let signals = engine.drain_signals();
        assert!(!signals.is_empty(), "Death cross should fire at least once");
        assert_eq!(signals[0].signal_type, "ma_crossover");
    }

    #[test]
    fn both_direction_fires_on_golden_and_death_cross() {
        let engine = TriggerEngine::new();
        engine
            .register(
                ma_crossover_with_direction(
                    "m1",
                    "Both Cross",
                    "stock",
                    2,
                    4,
                    CrossoverDirection::Both,
                )
                .with_cooldown(Duration::ZERO),
            )
            .unwrap();

        // Phase 1: rising data → golden cross
        for (ts, val) in [(0, 10.0), (1, 12.0), (2, 14.0), (3, 16.0)] {
            let event = make_write_event("stock", "price", val, ts);
            engine.process_event(&event);
        }
        let golden = engine.drain_signals();
        assert!(!golden.is_empty(), "Golden cross should fire with Both");

        // Phase 2: sharp decline → death cross
        for (ts, val) in [(4, 5.0), (5, 3.0)] {
            let event = make_write_event("stock", "price", val, ts);
            engine.process_event(&event);
        }
        let death = engine.drain_signals();
        assert!(!death.is_empty(), "Death cross should fire with Both");
    }
}
