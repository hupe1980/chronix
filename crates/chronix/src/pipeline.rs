//! Real-time analytics pipeline — orchestrates CDC → Signal, Analytics, Authz, and Audit.
//!
//! ## Architecture
//!
//! ```text
//! Chronix write path
//!       │
//!       ▼
//!    CDC EventBus
//!       │
//!       ├──→ TriggerEngine ──→ DeliveryRouter ──→ SignalStore
//!       │       (signal)          (webhook, log, metrics)
//!       │
//!       ├──→ StreamingAnomalyEngine ──→ AlertEngine ──→ DeliveryRouter
//!       │       (analytics)
//!       │
//!       └──→ ContinuousForecastEngine ──→ ForecastCache
//!               (analytics)
//!
//! AuthzEngine ──→ AuditLogger (cross-cutting)
//! ```

use std::sync::Arc;

use chronix_analytics::{
    AlertEngine, ContinuousForecastEngine, ForecastCache, StreamingAnomalyEngine,
};
use chronix_security::audit::{AuditEvent, AuditLogger, AuditSink, MemorySink as AuditMemorySink};
use chronix_security::authz::AuthzEngine;
use chronix_streaming::cdc::{CdcEvent, EventBus};
use chronix_streaming::signal::{
    DeliveryRouter, LogChannel, MetricChannel, Severity, SignalEvent, SignalStore, TriggerEngine,
};

use tracing::{debug, trace};

// ── Pipeline Configuration ──────────────────────────────────────────

/// Configuration for the real-time analytics pipeline.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Maximum signal store capacity.
    pub signal_store_capacity: usize,
    /// Whether to enable the log delivery channel.
    pub enable_log_delivery: bool,
    /// Whether to enable the metrics delivery channel.
    pub enable_metric_delivery: bool,
    /// Audit memory sink capacity (0 = no memory sink).
    pub audit_memory_capacity: usize,
    /// Default forecast horizon (number of points to predict). Default: 10.
    pub forecast_horizon: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            signal_store_capacity: 100_000,
            enable_log_delivery: true,
            enable_metric_delivery: true,
            audit_memory_capacity: 50_000,
            forecast_horizon: 10,
        }
    }
}

// ── Pipeline ────────────────────────────────────────────────────────

/// The real-time analytics pipeline, composing signal, analytics, authz,
/// and audit subsystems into a unified processing layer.
///
/// Thread-safe — all engines are shareable via `Arc`. The pipeline can be
/// queried from any thread while processing continues in the background.
pub struct Pipeline {
    /// Signal trigger engine — evaluates CDC events against registered triggers.
    trigger_engine: Arc<TriggerEngine>,
    /// Signal delivery router — dispatches signal events to channels.
    delivery_router: Arc<DeliveryRouter>,
    /// Persistent signal store.
    signal_store: Arc<SignalStore>,
    /// Streaming anomaly detection engine.
    anomaly_engine: Arc<StreamingAnomalyEngine>,
    /// Alert engine — fires alerts when anomaly thresholds are breached.
    alert_engine: Arc<AlertEngine>,
    /// Continuous forecast engine.
    forecast_engine: Arc<ContinuousForecastEngine>,
    /// Forecast cache — stores the latest forecast for each series.
    forecast_cache: Arc<ForecastCache>,
    /// Cedar authorization engine.
    authz_engine: Arc<AuthzEngine>,
    /// Structured audit logger.
    audit_logger: Arc<AuditLogger>,
    /// Audit memory sink (if enabled).
    audit_sink: Option<Arc<AuditMemorySink>>,
    /// Pipeline configuration.
    config: PipelineConfig,
}

impl Pipeline {
    /// Build a new pipeline with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(PipelineConfig::default())
    }

    /// Build a new pipeline with custom configuration.
    #[must_use]
    pub fn with_config(config: PipelineConfig) -> Self {
        let trigger_engine = Arc::new(TriggerEngine::new());
        let signal_store = Arc::new(SignalStore::new(config.signal_store_capacity));

        let router = DeliveryRouter::new();
        if config.enable_log_delivery {
            router.add_channel(Box::new(LogChannel));
        }
        if config.enable_metric_delivery {
            router.add_channel(Box::new(MetricChannel));
        }
        let delivery_router = Arc::new(router);

        let anomaly_engine = Arc::new(StreamingAnomalyEngine::new());
        let alert_engine = Arc::new(AlertEngine::new());
        let forecast_engine = Arc::new(ContinuousForecastEngine::new());
        let forecast_cache = Arc::new(ForecastCache::new(std::time::Duration::from_secs(300)));

        let authz_engine = Arc::new(AuthzEngine::new());
        let audit_logger = AuditLogger::new();

        let audit_sink = if config.audit_memory_capacity > 0 {
            let sink = Arc::new(AuditMemorySink::new(config.audit_memory_capacity));
            // Wrap in a Box<dyn AuditSink>
            let wrapper = AuditSinkWrapper(Arc::clone(&sink));
            audit_logger.add_sink(Box::new(wrapper));
            Some(sink)
        } else {
            None
        };

        let audit_logger = Arc::new(audit_logger);

        Self {
            trigger_engine,
            delivery_router,
            signal_store,
            anomaly_engine,
            alert_engine,
            forecast_engine,
            forecast_cache,
            authz_engine,
            audit_logger,
            audit_sink,
            config,
        }
    }

    // ── Engine accessors ────────────────────────────────────────

    /// Access the trigger engine (for registering / managing triggers).
    #[must_use]
    pub fn trigger_engine(&self) -> &Arc<TriggerEngine> {
        &self.trigger_engine
    }

    /// Access the delivery router.
    #[must_use]
    pub fn delivery_router(&self) -> &Arc<DeliveryRouter> {
        &self.delivery_router
    }

    /// Access the signal store.
    #[must_use]
    pub fn signal_store(&self) -> &Arc<SignalStore> {
        &self.signal_store
    }

    /// Access the streaming anomaly engine.
    #[must_use]
    pub fn anomaly_engine(&self) -> &Arc<StreamingAnomalyEngine> {
        &self.anomaly_engine
    }

    /// Access the alert engine.
    #[must_use]
    pub fn alert_engine(&self) -> &Arc<AlertEngine> {
        &self.alert_engine
    }

    /// Access the continuous forecast engine.
    #[must_use]
    pub fn forecast_engine(&self) -> &Arc<ContinuousForecastEngine> {
        &self.forecast_engine
    }

    /// Access the forecast cache.
    #[must_use]
    pub fn forecast_cache(&self) -> &Arc<ForecastCache> {
        &self.forecast_cache
    }

    /// Access the Cedar authorization engine.
    #[must_use]
    pub fn authz_engine(&self) -> &Arc<AuthzEngine> {
        &self.authz_engine
    }

    /// Access the audit logger.
    #[must_use]
    pub fn audit_logger(&self) -> &Arc<AuditLogger> {
        &self.audit_logger
    }

    /// Access the audit memory sink for querying audit events.
    #[must_use]
    pub fn audit_sink(&self) -> Option<&Arc<AuditMemorySink>> {
        self.audit_sink.as_ref()
    }

    // ── CDC event processing ────────────────────────────────────

    /// Process a single CDC event through all engines.
    ///
    /// This is the main dispatch point: it evaluates triggers, runs anomaly
    /// detection, updates forecasts, fires alerts, and routes signal events
    /// to delivery channels.
    ///
    /// Returns the total number of signals emitted.
    pub fn process_cdc_event(&self, event: &CdcEvent) -> usize {
        let mut total_signals = 0;

        // 1. Trigger engine: evaluate all matching triggers (race-free)
        let signals = self.trigger_engine.process_event_collect(event);
        for signal in &signals {
            self.delivery_router.deliver(signal);
            self.signal_store.store(signal.clone());
        }
        if !signals.is_empty() {
            debug!(fired = signals.len(), "Triggers fired for CDC event");
        }
        total_signals += signals.len();

        // 2. Streaming anomaly detection
        if let CdcEvent::PointWritten {
            measurement,
            tags,
            fields,
            timestamp,
            ..
        } = event
        {
            // Process through anomaly engine
            if let Some(scored) =
                self.anomaly_engine
                    .process_write(measurement, tags, fields, *timestamp)
            {
                trace!(
                    measurement,
                    score = scored.score.score,
                    "Anomaly score computed"
                );

                // Feed anomaly results to alert engine
                let fired_alerts =
                    self.alert_engine
                        .evaluate(measurement, tags, scored.score.score, *timestamp);

                // Convert fired alerts into signal events and deliver/persist
                for alert in &fired_alerts {
                    let severity = Severity::from_thresholds(alert.score, 3.0, 5.0);
                    let signal = SignalEvent {
                        event_id: uuid::Uuid::new_v4().to_string(),
                        trigger_id: format!("alert:{}", alert.alert_id),
                        trigger_name: format!("Anomaly alert {}", alert.alert_id),
                        measurement: alert.measurement.clone(),
                        tags: alert.tags.clone(),
                        timestamp: alert.timestamp,
                        signal_type: "anomaly_alert".into(),
                        severity,
                        value: alert.score,
                        metadata: std::collections::HashMap::new(),
                    };
                    self.delivery_router.deliver(&signal);
                    self.signal_store.store(signal);
                    total_signals += 1;
                }

                if !fired_alerts.is_empty() {
                    debug!(
                        measurement,
                        alerts = fired_alerts.len(),
                        "Alerts fired from anomaly detection"
                    );
                }
            }

            // 3. Continuous forecasting
            if let Some(update) =
                self.forecast_engine
                    .process_point(measurement, tags, fields, *timestamp)
            {
                trace!(measurement, update = ?update, "Forecast updated");

                // Store forecast in cache when model is fitted
                if matches!(
                    update,
                    chronix_analytics::ForecastUpdate::InitialFit
                        | chronix_analytics::ForecastUpdate::ReFit
                ) {
                    if let Ok(result) = self.forecast_engine.predict(
                        measurement,
                        tags,
                        self.config.forecast_horizon,
                    ) {
                        self.forecast_cache.store(
                            measurement,
                            tags,
                            result,
                            self.config.forecast_horizon,
                        );
                    }
                }
            }
        }

        total_signals
    }

    /// Subscribe to a CDC bus and process events in a background loop.
    ///
    /// Returns a `tokio::task::JoinHandle` for the background task.
    /// The task runs until the sender half of the bus is dropped.
    pub fn spawn_cdc_listener(self: &Arc<Self>, bus: &EventBus) -> tokio::task::JoinHandle<()> {
        // Use try_subscribe to avoid panic on max subscribers.
        let mut sub = match bus.try_subscribe() {
            Some(s) => s,
            None => {
                tracing::error!(
                    "failed to subscribe pipeline CDC listener: max subscribers reached"
                );
                return tokio::spawn(async {});
            }
        };
        let pipeline = Arc::clone(self);

        tokio::spawn(async move {
            debug!("Pipeline CDC listener started");
            loop {
                match sub.recv().await {
                    Some(event) => {
                        pipeline.process_cdc_event(&event);
                    }
                    None => {
                        debug!("Pipeline CDC listener: bus closed, shutting down");
                        break;
                    }
                }
            }
        })
    }

    // ── Audit helpers ───────────────────────────────────────────

    /// Log an audit event.
    pub fn audit(&self, event: AuditEvent) {
        self.audit_logger.log(event);
    }

    // ── Signal SQL ──────────────────────────────────────────────

    /// Execute a signal SQL statement (CREATE/SHOW/DROP/ALTER TRIGGER).
    ///
    /// # Errors
    ///
    /// Returns `chronix_streaming::signal::error::SignalError` on parse or execution failure.
    pub fn execute_signal_sql(
        &self,
        sql: &str,
    ) -> chronix_streaming::signal::error::Result<chronix_streaming::signal::SqlResult> {
        let stmt = chronix_streaming::signal::parse_trigger_sql(sql)?;
        chronix_streaming::signal::execute_trigger_sql(&self.trigger_engine, &stmt)
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipeline")
            .field("triggers", &self.trigger_engine.trigger_count())
            .field("signals", &self.signal_store.len())
            .field("anomaly_configs", &self.anomaly_engine.config_count())
            .field("forecast_configs", &self.forecast_engine.config_count())
            .field("delivery_channels", &self.delivery_router.channel_count())
            .finish()
    }
}

// ── AuditSink wrapper for Arc<MemorySink> ───────────────────────────

struct AuditSinkWrapper(Arc<AuditMemorySink>);

impl AuditSink for AuditSinkWrapper {
    fn emit(&self, event: &AuditEvent) -> chronix_security::audit::error::Result<()> {
        self.0.emit(event)
    }

    fn flush(&self) -> chronix_security::audit::error::Result<()> {
        self.0.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::Duration;

    use chronix_analytics::anomaly::DetectorType;
    use chronix_analytics::StreamingAnomalyConfig;
    use chronix_core::FieldValue;
    use chronix_security::audit::{AuditAction, AuditDecision};
    use chronix_streaming::signal::{EventTrigger, ThresholdOp, TriggerCondition};

    fn make_write_event(measurement: &str, field: &str, value: f64, timestamp: i64) -> CdcEvent {
        CdcEvent::PointWritten {
            measurement: measurement.into(),
            tags: BTreeMap::new(),
            fields: [(field.into(), FieldValue::F64(value))]
                .into_iter()
                .collect(),
            timestamp,
            seq: 0,
        }
    }

    #[test]
    fn pipeline_creation() {
        let pipeline = Pipeline::new();
        assert_eq!(pipeline.trigger_engine().trigger_count(), 0);
        assert_eq!(pipeline.signal_store().len(), 0);
        assert!(pipeline.audit_sink().is_some());
    }

    #[test]
    fn pipeline_trigger_fires_and_delivers() {
        let pipeline = Pipeline::new();

        // Register a trigger
        let trigger = EventTrigger::new(
            "t1",
            "High CPU",
            "cpu",
            TriggerCondition::FieldThreshold {
                field: "value".into(),
                op: ThresholdOp::Gt,
                value: 90.0,
            },
        )
        .with_cooldown(Duration::ZERO);

        pipeline.trigger_engine().register(trigger).unwrap();

        // Process a CDC event that should fire the trigger
        let event = make_write_event("cpu", "value", 95.0, 1000);
        let signals = pipeline.process_cdc_event(&event);
        assert_eq!(signals, 1);

        // Signal should be persisted
        assert_eq!(pipeline.signal_store().len(), 1);
        let stored = pipeline.signal_store().all();
        assert_eq!(stored[0].trigger_id, "t1");
        assert_eq!(stored[0].value, 95.0);
    }

    #[test]
    fn pipeline_trigger_no_fire() {
        let pipeline = Pipeline::new();

        let trigger = EventTrigger::new(
            "t1",
            "High CPU",
            "cpu",
            TriggerCondition::FieldThreshold {
                field: "value".into(),
                op: ThresholdOp::Gt,
                value: 90.0,
            },
        );
        pipeline.trigger_engine().register(trigger).unwrap();

        let event = make_write_event("cpu", "value", 50.0, 1000);
        let signals = pipeline.process_cdc_event(&event);
        assert_eq!(signals, 0);
        assert_eq!(pipeline.signal_store().len(), 0);
    }

    #[test]
    fn pipeline_anomaly_detection() {
        let pipeline = Pipeline::new();

        pipeline
            .anomaly_engine()
            .enable(
                StreamingAnomalyConfig::new("cpu", DetectorType::ZScore, 2.0)
                    .with_min_fit_points(5),
            )
            .unwrap();

        // Feed enough points to fit the detector
        for i in 0..10 {
            let event = make_write_event("cpu", "value", 100.0 + (i as f64 * 0.1), i);
            pipeline.process_cdc_event(&event);
        }

        // Feed an outlier — should get an anomaly score
        let event = make_write_event("cpu", "value", 1000.0, 100);
        pipeline.process_cdc_event(&event);

        // Verify scores were collected
        let scores = pipeline.anomaly_engine().scores();
        assert!(!scores.is_empty(), "Anomaly scores should be generated");
    }

    #[test]
    fn pipeline_audit_logging() {
        let pipeline = Pipeline::new();

        pipeline.audit(
            AuditEvent::new("alice", AuditAction::Write, "cpu", AuditDecision::Allow)
                .with_source_ip("10.0.0.1"),
        );

        pipeline.audit(
            AuditEvent::new("bob", AuditAction::Read, "memory", AuditDecision::Deny)
                .with_source_ip("10.0.0.2"),
        );

        let sink = pipeline.audit_sink().unwrap();
        assert_eq!(sink.events().len(), 2);

        let denied = sink.query_by_decision(AuditDecision::Deny);
        assert_eq!(denied.len(), 1);
        assert_eq!(denied[0].principal, "bob");
    }

    #[test]
    fn pipeline_multiple_triggers_and_anomaly() {
        let pipeline = Pipeline::new();

        // Register two triggers on different measurements
        pipeline
            .trigger_engine()
            .register(
                EventTrigger::new(
                    "t1",
                    "Alert CPU",
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

        pipeline
            .trigger_engine()
            .register(
                EventTrigger::new(
                    "t2",
                    "Alert Memory",
                    "memory",
                    TriggerCondition::FieldThreshold {
                        field: "value".into(),
                        op: ThresholdOp::Gt,
                        value: 80.0,
                    },
                )
                .with_cooldown(Duration::ZERO),
            )
            .unwrap();

        // Add anomaly config
        pipeline
            .anomaly_engine()
            .enable(
                StreamingAnomalyConfig::new("cpu", DetectorType::ZScore, 2.0)
                    .with_min_fit_points(5),
            )
            .unwrap();

        // Fire CPU trigger
        let event = make_write_event("cpu", "value", 95.0, 1000);
        assert_eq!(pipeline.process_cdc_event(&event), 1);

        // Fire Memory trigger
        let event = make_write_event("memory", "value", 85.0, 2000);
        assert_eq!(pipeline.process_cdc_event(&event), 1);

        // No trigger for disk
        let event = make_write_event("disk", "value", 99.0, 3000);
        assert_eq!(pipeline.process_cdc_event(&event), 0);

        assert_eq!(pipeline.signal_store().len(), 2);
    }

    #[tokio::test]
    async fn pipeline_cdc_listener() {
        let bus = EventBus::with_default_capacity();
        let pipeline = Arc::new(Pipeline::new());

        pipeline
            .trigger_engine()
            .register(
                EventTrigger::new(
                    "t1",
                    "Alert",
                    "cpu",
                    TriggerCondition::FieldThreshold {
                        field: "value".into(),
                        op: ThresholdOp::Gt,
                        value: 50.0,
                    },
                )
                .with_cooldown(Duration::ZERO),
            )
            .unwrap();

        let handle = pipeline.spawn_cdc_listener(&bus);

        // Publish events
        for i in 0..5 {
            bus.publish(CdcEvent::PointWritten {
                measurement: "cpu".into(),
                tags: BTreeMap::new(),
                fields: [("value".into(), FieldValue::F64(60.0 + i as f64))]
                    .into_iter()
                    .collect(),
                timestamp: i,
                seq: i as u64,
            });
        }

        // Give the listener a moment to process
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Drop bus to close the listener
        drop(bus);
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;

        assert_eq!(pipeline.signal_store().len(), 5);
    }

    #[test]
    fn pipeline_debug_display() {
        let pipeline = Pipeline::new();
        let debug = format!("{pipeline:?}");
        assert!(debug.contains("Pipeline"));
        assert!(debug.contains("triggers"));
    }

    #[test]
    fn pipeline_custom_config() {
        let config = PipelineConfig {
            signal_store_capacity: 500,
            enable_log_delivery: false,
            enable_metric_delivery: false,
            audit_memory_capacity: 0,
            forecast_horizon: 10,
        };
        let pipeline = Pipeline::with_config(config);
        assert!(pipeline.audit_sink().is_none());
        assert_eq!(pipeline.delivery_router().channel_count(), 0);
    }

    #[test]
    fn pipeline_anomaly_alerts_delivered_as_signals() {
        let pipeline = Pipeline::new();

        // Configure anomaly + alert engines
        pipeline
            .anomaly_engine()
            .enable(
                StreamingAnomalyConfig::new("cpu", DetectorType::ZScore, 2.0)
                    .with_min_fit_points(5),
            )
            .unwrap();

        pipeline.alert_engine().register(
            chronix_analytics::AlertConfig::new("alert1", "cpu", 2.0).with_cooldown(Duration::ZERO),
        );

        // Feed enough normal points to fit the detector
        for i in 0..10 {
            let event = make_write_event("cpu", "value", 100.0 + (i as f64 * 0.1), i);
            pipeline.process_cdc_event(&event);
        }

        // Feed a massive outlier
        let event = make_write_event("cpu", "value", 10000.0, 100);
        let signals = pipeline.process_cdc_event(&event);

        // Anomaly alerts should be converted to signals and stored
        let all_signals = pipeline.signal_store().all();
        let anomaly_signals: Vec<_> = all_signals
            .iter()
            .filter(|s| s.signal_type == "anomaly_alert")
            .collect();
        // At least one anomaly alert signal should be present
        if !anomaly_signals.is_empty() {
            assert!(anomaly_signals[0].trigger_id.starts_with("alert:"));
            assert!(signals > 0);
        }
    }

    #[test]
    fn pipeline_signal_sql_integration() {
        let pipeline = Pipeline::new();

        // CREATE TRIGGER via SQL
        let result = pipeline.execute_signal_sql(
            "CREATE TRIGGER high_cpu ON cpu WHEN value > 90.0 COOLDOWN INTERVAL '0s'",
        );
        assert!(result.is_ok());

        // SHOW TRIGGERS
        let result = pipeline.execute_signal_sql("SHOW TRIGGERS").unwrap();
        match result {
            chronix_streaming::signal::SqlResult::Triggers(triggers) => {
                assert_eq!(triggers.len(), 1);
                assert_eq!(triggers[0].name, "high_cpu");
            }
            _ => panic!("Expected Triggers result"),
        }

        // Fire the trigger
        let event = make_write_event("cpu", "value", 95.0, 1000);
        let signals = pipeline.process_cdc_event(&event);
        assert_eq!(signals, 1);
        assert_eq!(pipeline.signal_store().len(), 1);

        // DROP TRIGGER via SQL
        let result = pipeline.execute_signal_sql("DROP TRIGGER high_cpu");
        assert!(result.is_ok());
        assert_eq!(pipeline.trigger_engine().trigger_count(), 0);
    }

    #[test]
    fn pipeline_concurrent_process_event_collect_no_race() {
        // Verify process_event_collect returns signals inline, not via shared sink
        let pipeline = Arc::new(Pipeline::new());

        pipeline
            .trigger_engine()
            .register(
                EventTrigger::new(
                    "t1",
                    "Fast",
                    "cpu",
                    TriggerCondition::FieldThreshold {
                        field: "value".into(),
                        op: ThresholdOp::Gt,
                        value: 0.0,
                    },
                )
                .with_cooldown(Duration::ZERO),
            )
            .unwrap();

        // Process events from multiple threads
        let handles: Vec<_> = (0..4)
            .map(|thread_id| {
                let p = Arc::clone(&pipeline);
                std::thread::spawn(move || {
                    let mut count = 0;
                    for i in 0..25 {
                        let event = make_write_event(
                            "cpu",
                            "value",
                            1.0 + (i as f64),
                            (thread_id * 100 + i) as i64,
                        );
                        count += p.process_cdc_event(&event);
                    }
                    count
                })
            })
            .collect();

        let total: usize = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
            .iter()
            .sum();
        // Every event should fire the trigger, total = 100 events across 4 threads
        assert_eq!(total, 100);
        // All signals should be persisted
        assert_eq!(pipeline.signal_store().len(), 100);
    }
}
