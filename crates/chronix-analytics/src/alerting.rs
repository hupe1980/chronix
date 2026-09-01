//! Alerting hooks — configurable actions triggered by anomaly detection.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use metrics::counter;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

/// Action to take when an alert fires.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertAction {
    /// Log a structured tracing event at WARN level.
    Log,
    /// Increment a Prometheus counter.
    Metric,
    /// HTTP POST to a webhook URL.
    Webhook(String),
    /// Emit a CDC event.
    CdcEvent,
}

impl std::fmt::Display for AlertAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Log => write!(f, "log"),
            Self::Metric => write!(f, "metric"),
            Self::Webhook(url) => write!(f, "webhook({url})"),
            Self::CdcEvent => write!(f, "cdc_event"),
        }
    }
}

/// Configuration for an alert.
#[derive(Debug, Clone)]
pub struct AlertConfig {
    /// Unique alert ID.
    pub id: String,
    /// Measurement this alert applies to.
    pub measurement: String,
    /// Anomaly score threshold to fire the alert.
    pub threshold: f64,
    /// Cooldown period between firings for the same series.
    pub cooldown: Duration,
    /// Actions to take when the alert fires.
    pub actions: Vec<AlertAction>,
    /// Whether the alert is enabled.
    pub enabled: bool,
}

impl AlertConfig {
    /// Create a new alert config.
    #[must_use]
    pub fn new(id: impl Into<String>, measurement: impl Into<String>, threshold: f64) -> Self {
        Self {
            id: id.into(),
            measurement: measurement.into(),
            threshold,
            cooldown: Duration::from_secs(300),
            actions: vec![AlertAction::Log, AlertAction::Metric],
            enabled: true,
        }
    }

    /// Set the cooldown period.
    #[must_use]
    pub fn with_cooldown(mut self, cooldown: Duration) -> Self {
        self.cooldown = cooldown;
        self
    }

    /// Set the alert actions.
    #[must_use]
    pub fn with_actions(mut self, actions: Vec<AlertAction>) -> Self {
        self.actions = actions;
        self
    }

    /// Add an action.
    #[must_use]
    pub fn with_action(mut self, action: AlertAction) -> Self {
        self.actions.push(action);
        self
    }
}

/// A fired alert record.
#[derive(Debug, Clone)]
pub struct FiredAlert {
    /// Alert config ID.
    pub alert_id: String,
    /// Measurement.
    pub measurement: String,
    /// Series tags.
    pub tags: BTreeMap<String, String>,
    /// Anomaly score that triggered the alert.
    pub score: f64,
    /// Threshold that was exceeded.
    pub threshold: f64,
    /// Timestamp of the anomalous point.
    pub timestamp: i64,
    /// When the alert was fired.
    pub fired_at: Instant,
}

/// Per-series cooldown key: `(alert_id, canonical_tags_key)`.
type CooldownKey = (String, String);

/// Alert engine — evaluates anomaly scores against configured alerts.
pub struct AlertEngine {
    configs: DashMap<String, AlertConfig>,
    /// Per-alert×series cooldown tracking.
    last_fired: DashMap<CooldownKey, Instant>,
    /// Fired alert history.
    history: RwLock<VecDeque<FiredAlert>>,
    /// Maximum history size.
    max_history: usize,
    /// Evaluation counter for periodic stale-cooldown eviction.
    ///
    /// # Eviction strategy
    ///
    /// Stale cooldown entries are evicted every **1024 evaluations**
    /// (count-based).  This is cheap and prevents unbounded growth for
    /// most workloads.  A time-based eviction (e.g. background timer)
    /// would be more predictable under variable evaluation rates but
    /// adds complexity; the count-based approach is intentional for
    /// simplicity.
    eval_count: AtomicU64,
}

impl AlertEngine {
    /// Create a new alert engine.
    #[must_use]
    pub fn new() -> Self {
        Self {
            configs: DashMap::new(),
            last_fired: DashMap::new(),
            history: RwLock::new(VecDeque::new()),
            max_history: 100_000,
            eval_count: AtomicU64::new(0),
        }
    }

    /// Register an alert configuration.
    pub fn register(&self, config: AlertConfig) {
        let id = config.id.clone();
        self.configs.insert(id.clone(), config);
        debug!(alert_id = %id, "Alert registered");
    }

    /// Unregister an alert.
    pub fn unregister(&self, id: &str) {
        self.configs.remove(id);
        self.last_fired.retain(|k, _| k.0 != id);
    }

    /// Evaluate an anomaly score against all configured alerts for a measurement.
    ///
    /// Returns the fired alert records (if any).
    pub fn evaluate(
        &self,
        measurement: &str,
        tags: &BTreeMap<String, String>,
        score: f64,
        timestamp: i64,
    ) -> Vec<FiredAlert> {
        let mut fired = Vec::new();
        let series_key = compute_tags_key(tags);
        let now = Instant::now();

        // Periodically evict stale cooldowns to prevent unbounded growth.
        let count = self.eval_count.fetch_add(1, Ordering::Relaxed);
        if count.is_multiple_of(1024) && count > 0 {
            self.evict_stale_cooldowns();
        }

        for entry in self.configs.iter() {
            let config = entry.value();
            if !config.enabled || config.measurement != measurement {
                continue;
            }

            // Check threshold
            if score < config.threshold {
                continue;
            }

            // Check cooldown
            let key = (config.id.clone(), series_key.clone());
            if let Some(last) = self.last_fired.get(&key) {
                if now.duration_since(*last) < config.cooldown {
                    continue;
                }
            }

            // Fire!
            self.last_fired.insert(key, now);

            let alert = FiredAlert {
                alert_id: config.id.clone(),
                measurement: measurement.to_string(),
                tags: tags.clone(),
                score,
                threshold: config.threshold,
                timestamp,
                fired_at: now,
            };

            // Execute actions
            for action in &config.actions {
                execute_action(action, &alert);
            }

            counter!(
                "chronix_alert_fired_total",
                "alert_id" => config.id.clone(),
                "measurement" => measurement.to_string()
            )
            .increment(1);

            // Store in history
            let mut hist = self.history.write();
            if hist.len() >= self.max_history {
                hist.pop_front();
            }
            hist.push_back(alert.clone());

            fired.push(alert);
        }

        fired
    }

    /// Get alert firing history.
    #[must_use]
    pub fn history(&self) -> Vec<FiredAlert> {
        self.history.read().iter().cloned().collect()
    }

    /// Number of configured alerts.
    #[must_use]
    pub fn alert_count(&self) -> usize {
        self.configs.len()
    }

    /// Evict expired cooldown entries to prevent unbounded growth.
    ///
    /// Removes entries whose cooldown period has elapsed according to the
    /// alert config. Entries for unregistered alerts are always removed.
    pub fn evict_stale_cooldowns(&self) {
        let now = Instant::now();
        self.last_fired.retain(|key, fired_at| {
            if let Some(config) = self.configs.get(&key.0) {
                // Keep if still within cooldown window
                now.duration_since(*fired_at) < config.cooldown
            } else {
                // Alert was unregistered — remove stale entry
                false
            }
        });
    }

    /// Number of active cooldown entries (for diagnostics).
    #[must_use]
    pub fn cooldown_count(&self) -> usize {
        self.last_fired.len()
    }
}

impl Default for AlertEngine {
    fn default() -> Self {
        Self::new()
    }
}

fn execute_action(action: &AlertAction, alert: &FiredAlert) {
    match action {
        AlertAction::Log => {
            warn!(
                alert_id = %alert.alert_id,
                measurement = %alert.measurement,
                score = alert.score,
                threshold = alert.threshold,
                timestamp = alert.timestamp,
                "Alert fired"
            );
        }
        AlertAction::Metric => {
            counter!(
                "chronix_alert_action_metric_total",
                "alert_id" => alert.alert_id.clone()
            )
            .increment(1);
        }
        AlertAction::Webhook(url) => {
            // In embedded mode, webhook delivery is handled by the
            // Pipeline's DeliveryRouter (FiredAlert → SignalEvent → channels).
            // This action records the intent for observability.
            counter!(
                "chronix_alert_webhook_total",
                "alert_id" => alert.alert_id.clone()
            )
            .increment(1);
            debug!(
                alert_id = %alert.alert_id,
                url = %url,
                "Webhook alert action recorded (delivery via Pipeline DeliveryRouter)"
            );
        }
        AlertAction::CdcEvent => {
            // In embedded mode, CDC event emission is handled by the
            // Pipeline when it converts FiredAlert → SignalEvent.
            // This action records the intent for observability.
            counter!(
                "chronix_alert_cdc_event_total",
                "alert_id" => alert.alert_id.clone()
            )
            .increment(1);
            debug!(
                alert_id = %alert.alert_id,
                "CDC event alert action recorded (delivery via Pipeline)"
            );
        }
    }
}

use crate::util::compute_tags_key;

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tags(host: &str) -> BTreeMap<String, String> {
        let mut tags = BTreeMap::new();
        tags.insert("host".to_string(), host.to_string());
        tags
    }

    #[test]
    fn alert_fires_on_threshold() {
        let engine = AlertEngine::new();
        engine.register(
            AlertConfig::new("alert1", "cpu", 0.8).with_cooldown(Duration::from_millis(0)),
        );

        let tags = make_tags("host1");
        // Below threshold — no alert
        let fired = engine.evaluate("cpu", &tags, 0.5, 1000);
        assert!(fired.is_empty());

        // Above threshold — fires
        let fired = engine.evaluate("cpu", &tags, 0.9, 2000);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].alert_id, "alert1");
        assert!((fired[0].score - 0.9).abs() < f64::EPSILON);
    }

    #[test]
    fn cooldown_suppresses() {
        let engine = AlertEngine::new();
        engine.register(
            AlertConfig::new("alert1", "cpu", 0.5).with_cooldown(Duration::from_secs(3600)),
        );

        let tags = make_tags("host1");
        let fired1 = engine.evaluate("cpu", &tags, 0.9, 1000);
        assert_eq!(fired1.len(), 1);

        // Second firing suppressed by cooldown
        let fired2 = engine.evaluate("cpu", &tags, 0.9, 2000);
        assert!(fired2.is_empty());
    }

    #[test]
    fn different_series_independent_cooldown() {
        let engine = AlertEngine::new();
        engine.register(
            AlertConfig::new("alert1", "cpu", 0.5).with_cooldown(Duration::from_secs(3600)),
        );

        let tags_a = make_tags("host1");
        let tags_b = make_tags("host2");

        let fired_a = engine.evaluate("cpu", &tags_a, 0.9, 1000);
        assert_eq!(fired_a.len(), 1);

        // Different series — fires independently
        let fired_b = engine.evaluate("cpu", &tags_b, 0.9, 2000);
        assert_eq!(fired_b.len(), 1);
    }

    #[test]
    fn unregistered_measurement_ignored() {
        let engine = AlertEngine::new();
        engine.register(AlertConfig::new("alert1", "cpu", 0.5));

        let tags = make_tags("host1");
        let fired = engine.evaluate("memory", &tags, 0.9, 1000);
        assert!(fired.is_empty());
    }

    #[test]
    fn multiple_alerts_same_measurement() {
        let engine = AlertEngine::new();
        engine
            .register(AlertConfig::new("low", "cpu", 0.3).with_cooldown(Duration::from_millis(0)));
        engine
            .register(AlertConfig::new("high", "cpu", 0.8).with_cooldown(Duration::from_millis(0)));

        let tags = make_tags("host1");

        // Score 0.5 — only "low" fires
        let fired = engine.evaluate("cpu", &tags, 0.5, 1000);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].alert_id, "low");

        // Score 0.9 — both fire
        let fired = engine.evaluate("cpu", &tags, 0.9, 2000);
        assert_eq!(fired.len(), 2);
    }

    #[test]
    fn unregister_removes_alert() {
        let engine = AlertEngine::new();
        engine.register(AlertConfig::new("alert1", "cpu", 0.5));
        assert_eq!(engine.alert_count(), 1);

        engine.unregister("alert1");
        assert_eq!(engine.alert_count(), 0);
    }

    #[test]
    fn history_recorded() {
        let engine = AlertEngine::new();
        engine.register(
            AlertConfig::new("alert1", "cpu", 0.5).with_cooldown(Duration::from_millis(0)),
        );

        let tags = make_tags("host1");
        engine.evaluate("cpu", &tags, 0.9, 1000);
        engine.evaluate("cpu", &tags, 0.8, 2000);

        let hist = engine.history();
        assert_eq!(hist.len(), 2);
    }

    #[test]
    fn alert_action_display() {
        assert_eq!(AlertAction::Log.to_string(), "log");
        assert_eq!(AlertAction::Metric.to_string(), "metric");
        assert_eq!(
            AlertAction::Webhook("https://x.com".into()).to_string(),
            "webhook(https://x.com)"
        );
        assert_eq!(AlertAction::CdcEvent.to_string(), "cdc_event");
    }

    #[test]
    fn alert_config_builder() {
        let config = AlertConfig::new("a1", "cpu", 0.5)
            .with_cooldown(Duration::from_secs(60))
            .with_actions(vec![AlertAction::Log])
            .with_action(AlertAction::Metric);
        assert_eq!(config.actions.len(), 2);
        assert_eq!(config.cooldown, Duration::from_secs(60));
    }

    #[test]
    fn disabled_alert_does_not_fire() {
        let engine = AlertEngine::new();
        let mut config = AlertConfig::new("alert1", "cpu", 0.5);
        config.enabled = false;
        engine.register(config);

        let tags = make_tags("host1");
        let fired = engine.evaluate("cpu", &tags, 0.9, 1000);
        assert!(fired.is_empty());
    }

    #[test]
    fn test_cooldown_eviction() {
        let engine = AlertEngine::new();
        engine.register(
            AlertConfig::new("alert1", "cpu", 0.5).with_cooldown(Duration::from_millis(10)),
        );

        let tags = make_tags("host1");
        // Fire the alert
        let fired = engine.evaluate("cpu", &tags, 0.9, 1000);
        assert_eq!(fired.len(), 1);
        assert_eq!(engine.cooldown_count(), 1);

        // Wait for cooldown to expire
        std::thread::sleep(Duration::from_millis(20));

        engine.evict_stale_cooldowns();
        assert_eq!(engine.cooldown_count(), 0);
    }

    #[test]
    fn test_cooldown_count_grows_with_series() {
        let engine = AlertEngine::new();
        engine.register(
            AlertConfig::new("alert1", "cpu", 0.5).with_cooldown(Duration::from_secs(3600)),
        );

        let tags_a = make_tags("host1");
        let tags_b = make_tags("host2");
        let tags_c = make_tags("host3");

        engine.evaluate("cpu", &tags_a, 0.9, 1000);
        engine.evaluate("cpu", &tags_b, 0.9, 2000);
        engine.evaluate("cpu", &tags_c, 0.9, 3000);

        assert_eq!(engine.cooldown_count(), 3);
    }
}
