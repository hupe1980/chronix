//! Signal system data model — triggers, conditions, and signal events.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use chronix_core::FieldValue;

/// Generate a v4 UUID string for `event_id` defaults.
fn default_event_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

// ── SignalEvent ─────────────────────────────────────────────────────

/// A signal event emitted when a trigger fires.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalEvent {
    /// Unique event identifier for idempotent delivery.
    ///
    /// Webhook receivers can use this to deduplicate retried deliveries.
    /// Generated as a v4 UUID when the signal is emitted.
    #[serde(default = "default_event_id")]
    pub event_id: String,
    /// ID of the trigger that fired.
    pub trigger_id: String,
    /// Human-readable trigger name.
    pub trigger_name: String,
    /// Measurement that triggered the signal.
    pub measurement: String,
    /// Tags of the series that triggered the signal.
    pub tags: std::collections::BTreeMap<String, String>,
    /// Timestamp of the data point that caused the signal.
    pub timestamp: i64,
    /// Type of signal (e.g., "anomaly_threshold", "forecast_deviation").
    pub signal_type: String,
    /// Severity level.
    pub severity: Severity,
    /// The value that triggered the signal (e.g., anomaly score).
    pub value: f64,
    /// Additional metadata.
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// Severity levels for signal events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// Informational — no action required.
    Info,
    /// Warning — potential issue, investigate.
    Warning,
    /// Critical — immediate attention required.
    Critical,
}

impl Severity {
    /// Infer severity from a score magnitude.
    ///
    /// - `< low` → Info
    /// - `< high` → Warning
    /// - `>= high` → Critical
    #[must_use]
    pub fn from_thresholds(value: f64, low: f64, high: f64) -> Self {
        if value >= high {
            Self::Critical
        } else if value >= low {
            Self::Warning
        } else {
            Self::Info
        }
    }
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Critical => "critical",
        };
        write!(f, "{s}")
    }
}

// ── CrossoverDirection ───────────────────────────────────────────────

/// Direction of a moving-average crossover event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CrossoverDirection {
    /// Golden cross: short MA crosses above long MA (bullish).
    #[default]
    GoldenCross,
    /// Death cross: short MA crosses below long MA (bearish).
    DeathCross,
    /// Either direction.
    Both,
}

// ── TriggerCondition ────────────────────────────────────────────────

/// Conditions that can cause a trigger to fire.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TriggerCondition {
    /// Fires when anomaly score satisfies `score <op> threshold`.
    AnomalyScore {
        /// The score threshold.
        threshold: f64,
        /// Comparison operator (default: Gt).
        #[serde(default = "ThresholdOp::default_gt")]
        op: ThresholdOp,
        /// Optional detector type name.
        #[serde(default)]
        detector_type: Option<String>,
    },
    /// Fires when actual value deviates from forecast by more than tolerance.
    ForecastDeviation {
        /// Deviation tolerance as a fraction (e.g., 0.15 = 15%).
        tolerance_pct: f64,
        /// Comparison operator for deviation (default: Gt).
        #[serde(default = "ThresholdOp::default_gt")]
        op: ThresholdOp,
        /// Forecast horizon in seconds.
        #[serde(default)]
        horizon: Option<i64>,
    },
    /// Fires when short-term MA crosses long-term MA.
    MovingAverageCrossover {
        /// Short window size.
        short_window: usize,
        /// Long window size.
        long_window: usize,
        /// Crossover direction (default: golden cross).
        #[serde(default)]
        direction: CrossoverDirection,
        /// Optional field name to read. When `None`, the first numeric
        /// field (in alphabetical key order) is used — which can be
        /// ambiguous for multi-field measurements.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        field: Option<String>,
    },
    /// Fires when value changes by more than threshold_pct within window.
    RateOfChange {
        /// Change threshold as a fraction.
        threshold_pct: f64,
        /// Window size in data points.
        window: usize,
        /// Optional field name to read. When `None`, the first numeric
        /// field (in alphabetical key order) is used — which can be
        /// ambiguous for multi-field measurements.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        field: Option<String>,
    },
    /// Fires based on a threshold on a named field.
    FieldThreshold {
        /// Field name to inspect.
        field: String,
        /// Comparison operator.
        op: ThresholdOp,
        /// Threshold value.
        value: f64,
    },
    /// Conjunction: both sub-conditions must hold.
    And {
        /// Left operand.
        left: Box<TriggerCondition>,
        /// Right operand.
        right: Box<TriggerCondition>,
    },
    /// Disjunction: at least one sub-condition must hold.
    Or {
        /// Left operand.
        left: Box<TriggerCondition>,
        /// Right operand.
        right: Box<TriggerCondition>,
    },
}

impl TriggerCondition {
    /// Returns a human-readable name for the condition type.
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::AnomalyScore { .. } => "anomaly_score",
            Self::ForecastDeviation { .. } => "forecast_deviation",
            Self::MovingAverageCrossover { .. } => "moving_average_crossover",
            Self::RateOfChange { .. } => "rate_of_change",
            Self::FieldThreshold { .. } => "field_threshold",
            Self::And { .. } => "and",
            Self::Or { .. } => "or",
        }
    }

    /// Collect all field names that this condition requires to be numeric.
    #[must_use]
    pub fn required_numeric_fields(&self) -> Vec<&str> {
        match self {
            Self::FieldThreshold { field, .. } => vec![field.as_str()],
            Self::AnomalyScore { .. } => vec!["anomaly_score"],
            Self::ForecastDeviation { .. } => {
                vec!["forecast_deviation", "value", "forecast"]
            }
            Self::RateOfChange { field, .. } => field.as_deref().into_iter().collect(),
            Self::MovingAverageCrossover { field, .. } => field.as_deref().into_iter().collect(),
            Self::And { left, right } | Self::Or { left, right } => {
                let mut v = left.required_numeric_fields();
                v.extend(right.required_numeric_fields());
                v
            }
        }
    }

    /// Validate that all required numeric fields in `fields` have a numeric
    /// type.  Returns a list of `(field_name, actual_type)` pairs for fields
    /// that exist but are non-numeric.  Missing fields are not flagged — they
    /// are expected to be absent from some events.
    #[must_use]
    pub fn validate_fields<'a>(
        &'a self,
        fields: &'a BTreeMap<String, FieldValue>,
    ) -> Vec<(&'a str, &'static str)> {
        let mut mismatches = Vec::new();
        for name in self.required_numeric_fields() {
            if let Some(v) = fields.get(name) {
                match v {
                    FieldValue::F64(_) | FieldValue::I64(_) | FieldValue::U64(_) => {}
                    other => mismatches.push((name, other.type_name())),
                }
            }
        }
        mismatches
    }
}

/// Comparison operators for field threshold conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ThresholdOp {
    /// Greater than.
    Gt,
    /// Greater than or equal.
    Gte,
    /// Less than.
    Lt,
    /// Less than or equal.
    Lte,
    /// Equal.
    Eq,
}

impl ThresholdOp {
    /// Evaluate the comparator.
    ///
    /// `Eq` uses a combined relative + absolute tolerance
    /// that works correctly across all magnitudes. The tolerance is
    /// `max(1e-9 * max(|lhs|, |rhs|), 1e-12)` — relative for large
    /// values, absolute near zero.
    #[must_use]
    pub fn evaluate(&self, lhs: f64, rhs: f64) -> bool {
        match self {
            Self::Gt => lhs > rhs,
            Self::Gte => lhs >= rhs,
            Self::Lt => lhs < rhs,
            Self::Lte => lhs <= rhs,
            Self::Eq => {
                let diff = (lhs - rhs).abs();
                let magnitude = lhs.abs().max(rhs.abs());
                // Relative tolerance (1e-9) for normal values,
                // absolute floor (1e-12) near zero.
                diff <= 1e-9_f64 * magnitude.max(1e-3)
            }
        }
    }

    /// Default operator for serde deserialization (Gt).
    #[must_use]
    fn default_gt() -> Self {
        Self::Gt
    }
}

// ── EventTrigger ────────────────────────────────────────────────────

/// A trigger definition that monitors data streams and fires signals.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventTrigger {
    /// Unique identifier.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Measurement to monitor.
    pub measurement: String,
    /// The condition that causes firing.
    pub condition: TriggerCondition,
    /// Cooldown period — suppress duplicate firings within this window.
    #[serde(with = "duration_serde")]
    pub cooldown: Duration,
    /// Whether this trigger is active.
    pub enabled: bool,
    /// Signal type string emitted when this trigger fires.
    pub signal_type: String,
    /// Severity thresholds for auto-inference (low, high).
    #[serde(default = "default_severity_thresholds")]
    pub severity_thresholds: (f64, f64),
    /// Timestamp of creation (nanos since epoch).
    #[serde(default)]
    pub created_at: i64,
    /// Timestamp of last update (nanos since epoch).
    #[serde(default)]
    pub updated_at: i64,
    /// Delivery target descriptions (e.g. "webhook:https://...", "log").
    #[serde(default)]
    pub delivery_targets: Vec<String>,
}

/// Preset severity threshold profiles aligned with different anomaly-detector
/// score ranges.  All built-in detectors normalise scores to \[0, 1\] via a
/// sigmoid, so the thresholds must fall within that range.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeverityPreset {
    /// Default for detectors that produce normalised \[0, 1\] scores.
    /// Warning ≥ 0.5, Critical ≥ 0.8.
    Normalized,
    /// More sensitive — useful for Z-Score / Modified Z-Score detectors.
    /// Warning ≥ 0.4, Critical ≥ 0.7.
    Sensitive,
    /// Less sensitive — useful when false positives are costly.
    /// Warning ≥ 0.6, Critical ≥ 0.9.
    Relaxed,
}

impl SeverityPreset {
    /// Returns `(low, high)` severity thresholds for this preset.
    #[must_use]
    pub const fn thresholds(self) -> (f64, f64) {
        match self {
            Self::Normalized => (0.5, 0.8),
            Self::Sensitive => (0.4, 0.7),
            Self::Relaxed => (0.6, 0.9),
        }
    }
}

fn default_severity_thresholds() -> (f64, f64) {
    SeverityPreset::Normalized.thresholds()
}

impl EventTrigger {
    /// Create a new trigger with the given parameters.
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        measurement: impl Into<String>,
        condition: TriggerCondition,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            measurement: measurement.into(),
            condition,
            cooldown: Duration::from_secs(300), // 5 minutes default
            enabled: true,
            signal_type: "trigger".into(),
            severity_thresholds: default_severity_thresholds(),
            created_at: 0,
            updated_at: 0,
            delivery_targets: Vec::new(),
        }
    }

    /// Set the cooldown duration.
    #[must_use]
    pub fn with_cooldown(mut self, cooldown: Duration) -> Self {
        self.cooldown = cooldown;
        self
    }

    /// Set the signal type.
    #[must_use]
    pub fn with_signal_type(mut self, signal_type: impl Into<String>) -> Self {
        self.signal_type = signal_type.into();
        self
    }

    /// Set severity thresholds.
    #[must_use]
    pub fn with_severity_thresholds(mut self, low: f64, high: f64) -> Self {
        self.severity_thresholds = (low, high);
        self
    }

    /// Set severity thresholds from a preset profile.
    #[must_use]
    pub fn with_severity_preset(mut self, preset: SeverityPreset) -> Self {
        self.severity_thresholds = preset.thresholds();
        self
    }

    /// Enable or disable the trigger.
    #[must_use]
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set delivery targets.
    #[must_use]
    pub fn with_delivery_targets(mut self, targets: Vec<String>) -> Self {
        self.delivery_targets = targets;
        self
    }
}

// ── Duration serde ──────────────────────────────────────────────────

mod duration_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(d.as_millis() as u64)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let ms = u64::deserialize(d)?;
        Ok(Duration::from_millis(ms))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn severity_from_thresholds() {
        // Normalised [0,1] defaults: low=0.5, high=0.8
        assert_eq!(Severity::from_thresholds(0.3, 0.5, 0.8), Severity::Info);
        assert_eq!(Severity::from_thresholds(0.6, 0.5, 0.8), Severity::Warning);
        assert_eq!(Severity::from_thresholds(0.9, 0.5, 0.8), Severity::Critical);
    }

    #[test]
    fn severity_preset_thresholds() {
        assert_eq!(SeverityPreset::Normalized.thresholds(), (0.5, 0.8));
        assert_eq!(SeverityPreset::Sensitive.thresholds(), (0.4, 0.7));
        assert_eq!(SeverityPreset::Relaxed.thresholds(), (0.6, 0.9));
    }

    #[test]
    fn default_thresholds_match_normalized_preset() {
        assert_eq!(
            default_severity_thresholds(),
            SeverityPreset::Normalized.thresholds()
        );
    }

    #[test]
    fn severity_display() {
        assert_eq!(Severity::Info.to_string(), "info");
        assert_eq!(Severity::Warning.to_string(), "warning");
        assert_eq!(Severity::Critical.to_string(), "critical");
    }

    #[test]
    fn threshold_op_evaluate() {
        assert!(ThresholdOp::Gt.evaluate(5.0, 3.0));
        assert!(!ThresholdOp::Gt.evaluate(3.0, 5.0));
        assert!(ThresholdOp::Gte.evaluate(5.0, 5.0));
        assert!(ThresholdOp::Lt.evaluate(3.0, 5.0));
        assert!(ThresholdOp::Lte.evaluate(5.0, 5.0));
        assert!(ThresholdOp::Eq.evaluate(5.0, 5.0));
        assert!(!ThresholdOp::Eq.evaluate(5.0, 5.1));
    }

    #[test]
    fn trigger_builder() {
        let trigger = EventTrigger::new(
            "t1",
            "Test Trigger",
            "cpu",
            TriggerCondition::AnomalyScore {
                threshold: 3.0,
                op: ThresholdOp::Gt,
                detector_type: None,
            },
        )
        .with_cooldown(Duration::from_secs(60))
        .with_signal_type("anomaly_alert")
        .with_severity_preset(SeverityPreset::Sensitive);

        assert_eq!(trigger.id, "t1");
        assert_eq!(trigger.name, "Test Trigger");
        assert_eq!(trigger.measurement, "cpu");
        assert_eq!(trigger.cooldown, Duration::from_secs(60));
        assert_eq!(trigger.signal_type, "anomaly_alert");
        assert!(trigger.enabled);
    }

    #[test]
    fn trigger_serde_roundtrip() {
        let trigger = EventTrigger::new(
            "t1",
            "Anomaly Alert",
            "cpu",
            TriggerCondition::AnomalyScore {
                threshold: 3.0,
                op: ThresholdOp::Gt,
                detector_type: Some("zscore".into()),
            },
        )
        .with_cooldown(Duration::from_secs(120));

        let json = serde_json::to_string(&trigger).unwrap();
        let parsed: EventTrigger = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, "t1");
        assert_eq!(parsed.cooldown, Duration::from_secs(120));
    }

    #[test]
    fn signal_event_serde_roundtrip() {
        let event = SignalEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            trigger_id: "t1".into(),
            trigger_name: "My Trigger".into(),
            measurement: "cpu".into(),
            tags: [("host".into(), "s1".into())].into_iter().collect(),
            timestamp: 1_700_000_000_000,
            signal_type: "anomaly_alert".into(),
            severity: Severity::Warning,
            value: 3.5,
            metadata: HashMap::new(),
        };

        let json = serde_json::to_string(&event).unwrap();
        let parsed: SignalEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.trigger_id, "t1");
        assert_eq!(parsed.severity, Severity::Warning);
        assert_eq!(parsed.value, 3.5);
    }

    #[test]
    fn condition_variants_serialize() {
        let conditions = vec![
            TriggerCondition::AnomalyScore {
                threshold: 3.0,
                op: ThresholdOp::Gt,
                detector_type: None,
            },
            TriggerCondition::ForecastDeviation {
                tolerance_pct: 0.15,
                op: ThresholdOp::Gt,
                horizon: Some(3600),
            },
            TriggerCondition::MovingAverageCrossover {
                short_window: 5,
                long_window: 20,
                direction: CrossoverDirection::default(),
                field: None,
            },
            TriggerCondition::RateOfChange {
                threshold_pct: 0.1,
                window: 10,
                field: None,
            },
            TriggerCondition::FieldThreshold {
                field: "usage_idle".into(),
                op: ThresholdOp::Lt,
                value: 5.0,
            },
        ];

        for cond in &conditions {
            let json = serde_json::to_string(cond).unwrap();
            let parsed: TriggerCondition = serde_json::from_str(&json).unwrap();
            let re_json = serde_json::to_string(&parsed).unwrap();
            assert_eq!(json, re_json);
        }
    }

    #[test]
    fn trigger_disabled() {
        let trigger = EventTrigger::new(
            "t1",
            "Test",
            "cpu",
            TriggerCondition::AnomalyScore {
                threshold: 3.0,
                op: ThresholdOp::Gt,
                detector_type: None,
            },
        )
        .with_enabled(false);
        assert!(!trigger.enabled);
    }
}
