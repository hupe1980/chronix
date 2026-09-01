//! Anomaly false-positive feedback — precision tracking and threshold
//! auto-adjustment recommendations.

use std::collections::{BTreeMap, VecDeque};

use metrics::{counter, gauge};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::debug;

/// Feedback label for an anomaly detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackLabel {
    /// Confirmed anomaly.
    TruePositive,
    /// False alarm.
    FalsePositive,
    /// Unknown / not labeled.
    Unknown,
}

impl std::fmt::Display for FeedbackLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TruePositive => write!(f, "true_positive"),
            Self::FalsePositive => write!(f, "false_positive"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// A feedback entry for an anomaly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyFeedback {
    /// Unique anomaly ID.
    pub anomaly_id: String,
    /// Measurement where the anomaly was detected.
    pub measurement: String,
    /// Series tags.
    pub tags: BTreeMap<String, String>,
    /// Timestamp of the anomaly.
    pub timestamp: i64,
    /// Feedback label.
    pub label: FeedbackLabel,
    /// When feedback was submitted (nanos since epoch).
    pub feedback_at: i64,
    /// Principal who submitted the feedback.
    pub principal: String,
}

/// Precision tracker — computes rolling precision from feedback.
pub struct AnomalyPrecisionTracker {
    feedback: RwLock<VecDeque<AnomalyFeedback>>,
    /// Minimum acceptable precision (default: 0.8).
    min_precision: f64,
    /// Maximum feedback entries (FIFO eviction). Default: 100,000.
    max_entries: usize,
}

/// Precision statistics.
#[derive(Debug, Clone)]
pub struct PrecisionStats {
    /// Total feedback entries (excluding unknown).
    pub total_labeled: usize,
    /// True positives.
    pub true_positives: usize,
    /// False positives.
    pub false_positives: usize,
    /// Precision = TP / (TP + FP). None if no labeled data.
    pub precision: Option<f64>,
    /// Whether precision is below the minimum threshold.
    pub below_threshold: bool,
}

impl AnomalyPrecisionTracker {
    /// Create a new precision tracker.
    #[must_use]
    pub fn new(min_precision: f64) -> Self {
        Self {
            feedback: RwLock::new(VecDeque::new()),
            min_precision,
            max_entries: 100_000,
        }
    }

    /// Set maximum feedback entries (FIFO eviction when exceeded).
    #[must_use]
    pub fn with_max_entries(mut self, max: usize) -> Self {
        self.max_entries = max;
        self
    }

    /// Submit feedback for an anomaly.
    pub fn submit_feedback(&self, feedback: AnomalyFeedback) {
        counter!(
            "chronix_anomaly_feedback_total",
            "label" => feedback.label.to_string()
        )
        .increment(1);

        if feedback.label == FeedbackLabel::FalsePositive {
            counter!("chronix_anomaly_false_positives_total").increment(1);
        }

        let mut store = self.feedback.write();
        store.push_back(feedback);
        while store.len() > self.max_entries {
            store.pop_front();
        }

        // Update precision gauge
        let stats = Self::compute_precision_inner(store.iter(), self.min_precision);
        if let Some(p) = stats.precision {
            gauge!("chronix_anomaly_precision_ratio").set(p);
        }

        debug!(
            total_feedback = store.len(),
            precision = ?stats.precision,
            "Anomaly feedback submitted"
        );
    }

    /// Compute current precision statistics.
    #[must_use]
    pub fn precision(&self) -> PrecisionStats {
        let store = self.feedback.read();
        Self::compute_precision_inner(store.iter(), self.min_precision)
    }

    /// Compute precision for a specific measurement.
    #[must_use]
    pub fn precision_for_measurement(&self, measurement: &str) -> PrecisionStats {
        let store = self.feedback.read();
        Self::compute_precision_inner(
            store.iter().filter(|f| f.measurement == measurement),
            self.min_precision,
        )
    }

    /// Get all feedback entries.
    #[must_use]
    pub fn all_feedback(&self) -> Vec<AnomalyFeedback> {
        self.feedback.read().iter().cloned().collect()
    }

    /// Get feedback for a measurement.
    #[must_use]
    pub fn feedback_for_measurement(&self, measurement: &str) -> Vec<AnomalyFeedback> {
        self.feedback
            .read()
            .iter()
            .filter(|f| f.measurement == measurement)
            .cloned()
            .collect()
    }

    /// Get false-positive feedback entries.
    #[must_use]
    pub fn false_positives(&self) -> Vec<AnomalyFeedback> {
        self.feedback
            .read()
            .iter()
            .filter(|f| f.label == FeedbackLabel::FalsePositive)
            .cloned()
            .collect()
    }

    /// Total feedback count.
    #[must_use]
    pub fn count(&self) -> usize {
        self.feedback.read().len()
    }

    fn compute_precision_inner<'a>(
        entries: impl Iterator<Item = &'a AnomalyFeedback>,
        min_precision: f64,
    ) -> PrecisionStats {
        let mut tp = 0usize;
        let mut fp = 0usize;
        for e in entries {
            match e.label {
                FeedbackLabel::TruePositive => tp += 1,
                FeedbackLabel::FalsePositive => fp += 1,
                FeedbackLabel::Unknown => {}
            }
        }
        let total_labeled = tp + fp;
        let precision = if total_labeled > 0 {
            Some(tp as f64 / total_labeled as f64)
        } else {
            None
        };
        PrecisionStats {
            total_labeled,
            true_positives: tp,
            false_positives: fp,
            precision,
            below_threshold: precision.is_some_and(|p| p < min_precision),
        }
    }
}

impl Default for AnomalyPrecisionTracker {
    fn default() -> Self {
        Self::new(0.8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_feedback(id: &str, measurement: &str, label: FeedbackLabel) -> AnomalyFeedback {
        AnomalyFeedback {
            anomaly_id: id.to_string(),
            measurement: measurement.to_string(),
            tags: BTreeMap::new(),
            timestamp: 1_700_000_000_000,
            label,
            feedback_at: 1_700_000_001_000,
            principal: "analyst".to_string(),
        }
    }

    #[test]
    fn empty_precision() {
        let tracker = AnomalyPrecisionTracker::new(0.8);
        let stats = tracker.precision();
        assert_eq!(stats.total_labeled, 0);
        assert!(stats.precision.is_none());
        assert!(!stats.below_threshold);
    }

    #[test]
    fn perfect_precision() {
        let tracker = AnomalyPrecisionTracker::new(0.8);
        tracker.submit_feedback(make_feedback("a1", "cpu", FeedbackLabel::TruePositive));
        tracker.submit_feedback(make_feedback("a2", "cpu", FeedbackLabel::TruePositive));
        tracker.submit_feedback(make_feedback("a3", "cpu", FeedbackLabel::TruePositive));

        let stats = tracker.precision();
        assert_eq!(stats.true_positives, 3);
        assert_eq!(stats.false_positives, 0);
        assert!((stats.precision.unwrap() - 1.0).abs() < f64::EPSILON);
        assert!(!stats.below_threshold);
    }

    #[test]
    fn precision_with_false_positives() {
        let tracker = AnomalyPrecisionTracker::new(0.8);
        // 3 TP, 2 FP → precision = 0.6
        for i in 0..3 {
            tracker.submit_feedback(make_feedback(
                &format!("tp{i}"),
                "cpu",
                FeedbackLabel::TruePositive,
            ));
        }
        for i in 0..2 {
            tracker.submit_feedback(make_feedback(
                &format!("fp{i}"),
                "cpu",
                FeedbackLabel::FalsePositive,
            ));
        }

        let stats = tracker.precision();
        assert_eq!(stats.total_labeled, 5);
        assert!((stats.precision.unwrap() - 0.6).abs() < f64::EPSILON);
        assert!(stats.below_threshold); // 0.6 < 0.8
    }

    #[test]
    fn unknown_labels_excluded() {
        let tracker = AnomalyPrecisionTracker::new(0.8);
        tracker.submit_feedback(make_feedback("a1", "cpu", FeedbackLabel::TruePositive));
        tracker.submit_feedback(make_feedback("a2", "cpu", FeedbackLabel::Unknown));
        tracker.submit_feedback(make_feedback("a3", "cpu", FeedbackLabel::Unknown));

        let stats = tracker.precision();
        assert_eq!(stats.total_labeled, 1);
        assert!((stats.precision.unwrap() - 1.0).abs() < f64::EPSILON);
        assert_eq!(tracker.count(), 3);
    }

    #[test]
    fn per_measurement_precision() {
        let tracker = AnomalyPrecisionTracker::new(0.8);
        tracker.submit_feedback(make_feedback("a1", "cpu", FeedbackLabel::TruePositive));
        tracker.submit_feedback(make_feedback("a2", "cpu", FeedbackLabel::FalsePositive));
        tracker.submit_feedback(make_feedback("a3", "mem", FeedbackLabel::TruePositive));

        let cpu = tracker.precision_for_measurement("cpu");
        assert!((cpu.precision.unwrap() - 0.5).abs() < f64::EPSILON);

        let mem = tracker.precision_for_measurement("mem");
        assert!((mem.precision.unwrap() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn false_positives_query() {
        let tracker = AnomalyPrecisionTracker::new(0.8);
        tracker.submit_feedback(make_feedback("a1", "cpu", FeedbackLabel::TruePositive));
        tracker.submit_feedback(make_feedback("a2", "cpu", FeedbackLabel::FalsePositive));
        tracker.submit_feedback(make_feedback("a3", "cpu", FeedbackLabel::FalsePositive));

        let fps = tracker.false_positives();
        assert_eq!(fps.len(), 2);
    }

    #[test]
    fn feedback_label_display() {
        assert_eq!(FeedbackLabel::TruePositive.to_string(), "true_positive");
        assert_eq!(FeedbackLabel::FalsePositive.to_string(), "false_positive");
        assert_eq!(FeedbackLabel::Unknown.to_string(), "unknown");
    }

    #[test]
    fn feedback_serde_roundtrip() {
        let fb = make_feedback("a1", "cpu", FeedbackLabel::TruePositive);
        let json = serde_json::to_string(&fb).unwrap();
        let de: AnomalyFeedback = serde_json::from_str(&json).unwrap();
        assert_eq!(de.anomaly_id, "a1");
        assert_eq!(de.label, FeedbackLabel::TruePositive);
    }

    #[test]
    fn bounded_fifo_eviction() {
        let tracker = AnomalyPrecisionTracker::new(0.8).with_max_entries(5);

        // Submit 8 feedback entries, only last 5 should remain
        for i in 0..8 {
            tracker.submit_feedback(make_feedback(
                &format!("a{i}"),
                "cpu",
                FeedbackLabel::TruePositive,
            ));
        }

        assert_eq!(tracker.count(), 5);
        let all = tracker.all_feedback();
        // Oldest entries (a0, a1, a2) should have been evicted
        let ids: Vec<_> = all.iter().map(|f| f.anomaly_id.as_str()).collect();
        assert_eq!(ids, vec!["a3", "a4", "a5", "a6", "a7"]);
    }

    #[test]
    fn bounded_precision_uses_retained_entries() {
        // Max 4 entries: 2 TP then 2 FP → precision=0.5
        // Then add 2 more TP → evicts the 2 original TP, leaving [FP, FP, TP, TP] → 0.5
        let tracker = AnomalyPrecisionTracker::new(0.8).with_max_entries(4);

        tracker.submit_feedback(make_feedback("tp1", "cpu", FeedbackLabel::TruePositive));
        tracker.submit_feedback(make_feedback("tp2", "cpu", FeedbackLabel::TruePositive));
        tracker.submit_feedback(make_feedback("fp1", "cpu", FeedbackLabel::FalsePositive));
        tracker.submit_feedback(make_feedback("fp2", "cpu", FeedbackLabel::FalsePositive));

        let stats = tracker.precision();
        assert!((stats.precision.unwrap() - 0.5).abs() < f64::EPSILON);

        // Add 2 more TP → evicts tp1, tp2 → [fp1, fp2, tp3, tp4]
        tracker.submit_feedback(make_feedback("tp3", "cpu", FeedbackLabel::TruePositive));
        tracker.submit_feedback(make_feedback("tp4", "cpu", FeedbackLabel::TruePositive));

        assert_eq!(tracker.count(), 4);
        let stats = tracker.precision();
        // Still 2TP/2FP = 0.5
        assert!((stats.precision.unwrap() - 0.5).abs() < f64::EPSILON);
    }
}
