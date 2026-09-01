//! Composite signal engine for multi-condition alerting.

use std::collections::HashMap;
use std::time::Instant;

use crate::multivariate::context::MultiSeriesContext;

// ── Signal delivery channel ─────────────────────────────────────────────

use std::sync::mpsc;

/// Default channel capacity for signal delivery.
pub const DEFAULT_SIGNAL_CHANNEL_CAPACITY: usize = 1024;

/// A channel-based signal delivery layer.
///
/// Composite signals are sent through this bounded channel to downstream
/// consumers (alerting, logging, webhook triggers, etc.). The channel
/// applies back-pressure when full, preventing unbounded memory growth.
pub struct SignalSender {
    tx: mpsc::SyncSender<CompositeSignal>,
}

/// Receiver end of the signal delivery channel.
pub struct SignalReceiver {
    rx: mpsc::Receiver<CompositeSignal>,
}

/// Creates a new bounded signal delivery channel (sender + receiver).
///
/// The `capacity` parameter controls the maximum number of unprocessed
/// signals that can be buffered. Use [`DEFAULT_SIGNAL_CHANNEL_CAPACITY`]
/// for a reasonable default. Senders will block when the channel is full.
pub fn signal_channel(capacity: usize) -> (SignalSender, SignalReceiver) {
    let (tx, rx) = mpsc::sync_channel(capacity);
    (SignalSender { tx }, SignalReceiver { rx })
}

impl SignalSender {
    /// Sends a composite signal to the delivery layer.
    ///
    /// Blocks if the channel is full (back-pressure).
    pub fn send(&self, signal: CompositeSignal) -> Result<(), mpsc::SendError<CompositeSignal>> {
        self.tx.send(signal)
    }

    /// Tries to send a signal without blocking.
    ///
    /// Returns `TrySendError::Full` if the channel is at capacity.
    pub fn try_send(
        &self,
        signal: CompositeSignal,
    ) -> Result<(), mpsc::TrySendError<CompositeSignal>> {
        self.tx.try_send(signal)
    }
}

impl SignalReceiver {
    /// Receives the next signal (blocking).
    pub fn recv(&self) -> Result<CompositeSignal, mpsc::RecvError> {
        self.rx.recv()
    }

    /// Tries to receive a signal without blocking.
    pub fn try_recv(&self) -> Result<CompositeSignal, mpsc::TryRecvError> {
        self.rx.try_recv()
    }

    /// Drains all pending signals into a Vec.
    pub fn drain(&self) -> Vec<CompositeSignal> {
        let mut signals = Vec::new();
        while let Ok(sig) = self.rx.try_recv() {
            signals.push(sig);
        }
        signals
    }
}

/// Results from upstream analytics (forecasts, anomaly scores) passed to composite signal conditions.
#[derive(Debug, Default, Clone)]
pub struct AnalyticsResults {
    /// Per-series forecast values (series_id → predicted values).
    pub forecasts: std::collections::HashMap<String, Vec<f64>>,
    /// Per-series anomaly scores (series_id → scores).
    pub anomaly_scores: std::collections::HashMap<String, Vec<f64>>,
}

/// A composite signal emitted when conditions are met.
#[derive(Debug, Clone)]
pub struct CompositeSignal {
    /// Signal type identifier.
    pub signal_type: String,
    /// Confidence 0.0 − 1.0.
    pub confidence: f64,
    /// Which series contributed to this signal.
    pub contributing_series: Vec<String>,
    /// Arbitrary metadata.
    pub metadata: HashMap<String, String>,
}

/// A rule that evaluates against a multi-series context.
pub struct CompositeSignalRule {
    /// Unique name of this rule.
    pub name: String,
    /// Signal type emitted when the rule fires (e.g. "critical").
    pub signal_type: String,
    /// Series that contribute to this composite signal.
    pub contributing_series: Vec<String>,
    /// Cooldown between firings (nanoseconds).
    pub cooldown_ns: i64,
    /// Condition closure: receives context reference and analytics results, returns (should_fire, confidence).
    #[allow(clippy::type_complexity)]
    pub condition: Box<dyn Fn(&MultiSeriesContext, &AnalyticsResults) -> (bool, f64) + Send + Sync>,
}

/// Engine that evaluates composite signal rules.
pub struct CompositeSignalEngine {
    /// Tracks last fire time per rule name.
    last_fired: HashMap<String, Instant>,
}

impl CompositeSignalEngine {
    /// Creates a new engine with no cooldown state.
    pub fn new() -> Self {
        Self {
            last_fired: HashMap::new(),
        }
    }

    /// Evaluate all rules against the context.
    pub fn evaluate(
        &mut self,
        rules: &[CompositeSignalRule],
        ctx: &MultiSeriesContext,
        analytics: &AnalyticsResults,
    ) -> Vec<CompositeSignal> {
        let now = Instant::now();
        let mut signals = Vec::new();

        for rule in rules {
            // Check cooldown
            if let Some(last) = self.last_fired.get(&rule.name) {
                let elapsed_ns =
                    i64::try_from(now.duration_since(*last).as_nanos()).unwrap_or(i64::MAX);
                if elapsed_ns < rule.cooldown_ns {
                    continue;
                }
            }

            let (fire, confidence) = (rule.condition)(ctx, analytics);
            if fire {
                // Populate metadata with contextual information
                let mut metadata = HashMap::new();
                metadata.insert("rule_name".to_string(), rule.name.clone());
                metadata.insert("n_series".to_string(), ctx.matrix.n_series().to_string());
                metadata.insert(
                    "n_timestamps".to_string(),
                    ctx.matrix.n_timestamps().to_string(),
                );
                metadata.insert("confidence".to_string(), format!("{confidence:.6}"));
                if let Some(&first_ts) = ctx.matrix.timestamps.first() {
                    metadata.insert("window_start_ns".to_string(), first_ts.to_string());
                }
                if let Some(&last_ts) = ctx.matrix.timestamps.last() {
                    metadata.insert("window_end_ns".to_string(), last_ts.to_string());
                }
                // Include per-contributing-series latest values
                for sid in &rule.contributing_series {
                    if let Some(idx) = ctx.matrix.series_ids.iter().position(|s| s == sid) {
                        if let Some(&last_val) = ctx.matrix.data[idx].last() {
                            metadata.insert(format!("latest_{sid}"), format!("{last_val:.6}"));
                        }
                    }
                }

                signals.push(CompositeSignal {
                    signal_type: rule.signal_type.clone(),
                    confidence,
                    contributing_series: rule.contributing_series.clone(),
                    metadata,
                });
                self.last_fired.insert(rule.name.clone(), now);
            }
        }
        metrics::counter!("chronix_composite_signal_fired_total").increment(signals.len() as u64);
        signals
    }
}

impl Default for CompositeSignalEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ctx() -> MultiSeriesContext {
        MultiSeriesContext::build(
            vec![
                (
                    "cpu".to_string(),
                    vec![0, 1_000_000_000, 2_000_000_000],
                    vec![90.0, 95.0, 98.0], // high CPU
                ),
                (
                    "memory".to_string(),
                    vec![0, 1_000_000_000, 2_000_000_000],
                    vec![85.0, 88.0, 92.0], // high memory
                ),
            ],
            None,
        )
        .unwrap()
    }

    #[test]
    fn fires_when_conditions_met() {
        let ctx = test_ctx();
        let analytics = AnalyticsResults::default();
        let rules = vec![CompositeSignalRule {
            name: "high_load".to_string(),
            signal_type: "critical".to_string(),
            contributing_series: vec!["cpu".to_string(), "memory".to_string()],
            cooldown_ns: 0,
            condition: Box::new(|ctx, _analytics| {
                let cpu = ctx.matrix.series_by_name("cpu").unwrap();
                let mem = ctx.matrix.series_by_name("memory").unwrap();
                let cpu_high = cpu.iter().any(|&v| v > 90.0);
                let mem_high = mem.iter().any(|&v| v > 85.0);
                (cpu_high && mem_high, 0.9)
            }),
        }];
        let mut engine = CompositeSignalEngine::new();
        let signals = engine.evaluate(&rules, &ctx, &analytics);
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].signal_type, "critical");
        assert!((signals[0].confidence - 0.9).abs() < 1e-6);
    }

    #[test]
    fn does_not_fire_when_partial() {
        let ctx = MultiSeriesContext::build(
            vec![
                (
                    "cpu".to_string(),
                    vec![0, 1_000_000_000],
                    vec![50.0, 55.0], // low CPU
                ),
                (
                    "memory".to_string(),
                    vec![0, 1_000_000_000],
                    vec![90.0, 95.0], // high memory
                ),
            ],
            None,
        )
        .unwrap();
        let analytics = AnalyticsResults::default();
        let rules = vec![CompositeSignalRule {
            name: "high_load".to_string(),
            signal_type: "critical".to_string(),
            contributing_series: vec!["cpu".to_string(), "memory".to_string()],
            cooldown_ns: 0,
            condition: Box::new(|ctx, _analytics| {
                let cpu = ctx.matrix.series_by_name("cpu").unwrap();
                let mem = ctx.matrix.series_by_name("memory").unwrap();
                let cpu_high = cpu.iter().any(|&v| v > 90.0);
                let mem_high = mem.iter().any(|&v| v > 85.0);
                (cpu_high && mem_high, 0.9)
            }),
        }];
        let mut engine = CompositeSignalEngine::new();
        let signals = engine.evaluate(&rules, &ctx, &analytics);
        assert!(signals.is_empty());
    }

    #[test]
    fn cooldown_suppresses_duplicate() {
        let ctx = test_ctx();
        let analytics = AnalyticsResults::default();
        let rules = vec![CompositeSignalRule {
            name: "always_fire".to_string(),
            signal_type: "info".to_string(),
            contributing_series: vec![],
            cooldown_ns: 60_000_000_000, // 60s cooldown
            condition: Box::new(|_, _| (true, 1.0)),
        }];
        let mut engine = CompositeSignalEngine::new();
        let first = engine.evaluate(&rules, &ctx, &analytics);
        assert_eq!(first.len(), 1);
        // Immediately evaluate again — should be suppressed
        let second = engine.evaluate(&rules, &ctx, &analytics);
        assert!(second.is_empty());
    }

    #[test]
    fn analytics_results_condition() {
        let ctx = test_ctx();
        let mut analytics = AnalyticsResults::default();
        analytics
            .anomaly_scores
            .insert("cpu".to_string(), vec![0.1, 0.2, 0.95]);
        let rules = vec![CompositeSignalRule {
            name: "anomaly_alert".to_string(),
            signal_type: "anomaly".to_string(),
            contributing_series: vec!["cpu".to_string()],
            cooldown_ns: 0,
            condition: Box::new(|_ctx, analytics| {
                if let Some(scores) = analytics.anomaly_scores.get("cpu") {
                    let max_score = scores.iter().copied().fold(f64::NEG_INFINITY, f64::max);
                    (max_score > 0.9, max_score)
                } else {
                    (false, 0.0)
                }
            }),
        }];
        let mut engine = CompositeSignalEngine::new();
        let signals = engine.evaluate(&rules, &ctx, &analytics);
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].signal_type, "anomaly");
        assert!((signals[0].confidence - 0.95).abs() < 1e-6);
    }

    #[test]
    fn signal_delivery_channel() {
        let (sender, receiver) = signal_channel(DEFAULT_SIGNAL_CHANNEL_CAPACITY);

        let signal = CompositeSignal {
            signal_type: "test_rule".to_string(),
            confidence: 0.95,
            contributing_series: vec!["cpu".to_string()],
            metadata: HashMap::new(),
        };

        sender.send(signal).unwrap();

        let received = receiver.recv().unwrap();
        assert_eq!(received.signal_type, "test_rule");
        assert!((received.confidence - 0.95).abs() < 1e-10);
    }

    #[test]
    fn bounded_channel_backpressure() {
        // Create a channel with capacity=2
        let (sender, receiver) = signal_channel(2);

        let make_signal = |i: usize| CompositeSignal {
            signal_type: format!("sig_{i}"),
            confidence: 0.5,
            contributing_series: vec![],
            metadata: HashMap::new(),
        };

        // Fill the channel
        sender.send(make_signal(0)).unwrap();
        sender.send(make_signal(1)).unwrap();

        // Third send should fail with try_send (channel full)
        let result = sender.try_send(make_signal(2));
        assert!(result.is_err(), "Channel should be full at capacity=2");

        // Drain one and try again
        let _ = receiver.recv().unwrap();
        sender.try_send(make_signal(2)).unwrap();

        // Drain all
        let drained = receiver.drain();
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].signal_type, "sig_1");
        assert_eq!(drained[1].signal_type, "sig_2");
    }
}
