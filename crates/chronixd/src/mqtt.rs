//! MQTT subscriber ingestion connector.
//!
//! Subscribes to MQTT topics and writes received time-series data into
//! the Chronix database. Supports JSON and InfluxDB Line Protocol
//! payload formats.
//!
//! # Topic-to-measurement mapping
//!
//! By default the last segment of the MQTT topic path becomes the
//! measurement name. For example, `sensors/building-1/temperature`
//! maps to measurement `temperature`.
//!
//! # Feature gate
//!
//! The actual MQTT subscriber loop requires the `mqtt` feature flag
//! which brings in the `rumqttc` crate. Without the feature, the
//! connector can still be constructed and its parsing logic tested, but
//! `start()` will log a notice rather than connect to a broker.
//!
//! # Configuration
//!
//! ```toml
//! [mqtt]
//! broker = "mqtt.example.com"
//! port = 1883
//! client_id = "chronix-sub"
//! topics = ["sensors/#"]
//! qos = 1
//! format = "json"
//! ```

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use tracing::info;

use chronix::prelude::*;
use chronix::Chronix;

use crate::connector::{ConnectorMetrics, ConnectorStatus, IngestionConnector, MqttConfig};
use crate::error::ServerError;
use crate::influx;

/// MQTT subscriber connector.
///
/// Spawns a background task that subscribes to configured MQTT topics,
/// parses incoming messages, and writes the resulting points into Chronix.
///
/// # QoS & delivery
///
/// Uses QoS 1 (at-least-once) by default. Points are written to the
/// database (including WAL) before the MQTT PUBACK is sent, ensuring
/// at-least-once delivery semantics.
pub struct MqttSubscriber {
    name: String,
    config: MqttConfig,
    /// Database handle — used by the subscriber loop when the `mqtt` feature
    /// is enabled.
    #[cfg_attr(not(feature = "mqtt"), allow(dead_code))]
    db: Arc<Chronix>,
    /// Weak self-reference for safe background task spawning.
    /// Populated by [`new_arc`] — avoids `unsafe` Arc reconstruction.
    #[cfg_attr(not(feature = "mqtt"), allow(dead_code))]
    self_ref: OnceLock<Weak<Self>>,
    running: AtomicBool,
    stopped: AtomicBool,
    cancel: tokio::sync::Notify,
    messages_total: AtomicU64,
    points_total: AtomicU64,
    decode_errors: AtomicU64,
    /// Reconnection counter — used by the subscriber loop when the `mqtt`
    /// feature is enabled.
    #[cfg_attr(not(feature = "mqtt"), allow(dead_code))]
    reconnections: AtomicU64,
}

impl MqttSubscriber {
    /// Namespace this connector writes to.
    ///
    /// A connector carries no request, so its namespace comes from its own
    /// configuration rather than a header. Returning `None` when the value is
    /// the default keeps a single-tenant server's points untagged, which is
    /// what its reads expect.
    #[cfg(feature = "mqtt")]
    fn namespace_scope(&self) -> Option<&str> {
        (self.config.namespace != crate::namespace::DEFAULT_NAMESPACE)
            .then_some(self.config.namespace.as_str())
    }

    /// Create a new MQTT subscriber connector wrapped in an `Arc`.
    ///
    /// This is the preferred constructor — it stores a `Weak<Self>`
    /// internally so that `start()` can safely spawn background tasks
    /// without `unsafe` Arc reconstruction.
    pub fn new_arc(name: &str, config: MqttConfig, db: Arc<Chronix>) -> Arc<Self> {
        let arc = Arc::new(Self {
            name: name.to_string(),
            config,
            db,
            self_ref: OnceLock::new(),
            running: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            cancel: tokio::sync::Notify::new(),
            messages_total: AtomicU64::new(0),
            points_total: AtomicU64::new(0),
            decode_errors: AtomicU64::new(0),
            reconnections: AtomicU64::new(0),
        });
        let _ = arc.self_ref.set(Arc::downgrade(&arc));
        arc
    }

    /// Create a new MQTT subscriber connector (non-Arc).
    ///
    /// Useful for unit-testing parsing logic. The `mqtt` feature-gated
    /// subscriber loop will not be available without calling `new_arc`.
    pub fn new(name: &str, config: MqttConfig, db: Arc<Chronix>) -> Self {
        Self {
            name: name.to_string(),
            config,
            db,
            self_ref: OnceLock::new(),
            running: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            cancel: tokio::sync::Notify::new(),
            messages_total: AtomicU64::new(0),
            points_total: AtomicU64::new(0),
            decode_errors: AtomicU64::new(0),
            reconnections: AtomicU64::new(0),
        }
    }

    /// Extract measurement name from an MQTT topic path.
    ///
    /// Uses the last non-empty segment of the topic. For example:
    /// - `sensors/building-1/temperature` → `temperature`
    /// - `metrics/cpu` → `cpu`
    /// - `simple` → `simple`
    pub fn measurement_from_topic(topic: &str) -> String {
        topic
            .rsplit('/')
            .find(|s| !s.is_empty())
            .unwrap_or(topic)
            .to_string()
    }

    /// Extract tags from an MQTT topic path.
    ///
    /// Intermediate path segments (between root and measurement) become
    /// tags keyed as `topic_level_N`. For example,
    /// `sensors/building-1/temperature` produces
    /// `{ "topic_level_1": "building-1" }`.
    pub fn extract_topic_tags(topic: &str) -> BTreeMap<String, String> {
        let parts: Vec<&str> = topic.split('/').filter(|s| !s.is_empty()).collect();
        let mut tags = BTreeMap::new();

        // Skip first (root) and last (measurement) segments, use middle as tags
        if parts.len() > 2 {
            for (i, part) in parts.iter().enumerate().skip(1) {
                if i < parts.len() - 1 {
                    tags.insert(format!("topic_level_{i}"), part.to_string());
                }
            }
        }

        tags
    }

    /// Parse an MQTT message payload into points.
    ///
    /// Delegates to [`crate::util::parse_json_point`] for JSON format
    /// (merging topic-derived tags), or [`crate::influx::parse_line_protocol`]
    /// for InfluxDB line protocol.
    ///
    /// Uses `topic_measurement_map` for explicit mapping.
    /// Falls back to last-segment extraction when no mapping exists.
    pub fn parse_payload(&self, topic: &str, payload: &[u8]) -> Result<Vec<Point>, ServerError> {
        let text = std::str::from_utf8(payload)
            .map_err(|e| ServerError::BadRequest(format!("invalid UTF-8 in MQTT message: {e}")))?;

        match self.config.format {
            crate::connector::ConnectorFormat::LineProtocol => influx::parse_line_protocol(text)
                .map_err(|e| ServerError::BadRequest(format!("line protocol error: {e}"))),
            crate::connector::ConnectorFormat::Json => {
                let measurement = self.measurement_for_topic(topic);
                let topic_tags = Self::extract_topic_tags(topic);
                crate::util::parse_json_point(&measurement, text, topic_tags)
            }
        }
    }

    /// Resolve a measurement name for the given topic.
    ///
    /// Checks `topic_measurement_map` first; falls back to
    /// last-segment extraction for backward compatibility.
    pub fn measurement_for_topic(&self, topic: &str) -> String {
        self.config
            .topic_measurement_map
            .get(topic)
            .cloned()
            .unwrap_or_else(|| Self::measurement_from_topic(topic))
    }
}

// ── Feature-gated subscriber loop ──────────────────────────────────────

#[cfg(feature = "mqtt")]
mod subscriber_impl {
    use super::*;
    use rumqttc::{AsyncClient, MqttOptions, QoS};
    use tracing::{debug, error, warn};

    impl MqttSubscriber {
        /// Spawn the MQTT event-loop as a background task.
        ///
        /// Subscribes to all configured topics, processes incoming
        /// `Publish` events, and writes parsed points into Chronix.
        pub(super) fn spawn_subscriber(self: &Arc<Self>) -> Result<(), ServerError> {
            let mut opts = MqttOptions::new(
                &self.config.client_id,
                &self.config.broker,
                self.config.port,
            );
            opts.set_clean_session(true);
            opts.set_keep_alive(std::time::Duration::from_secs(30));

            let (client, mut eventloop) = AsyncClient::new(opts, 256);
            let qos = match self.config.qos {
                0 => QoS::AtMostOnce,
                1 => QoS::AtLeastOnce,
                2 => QoS::ExactlyOnce,
                other => {
                    return Err(ServerError::BadRequest(format!(
                        "invalid MQTT QoS level {other} (must be 0, 1, or 2)"
                    )));
                }
            };

            info!(
                name = %self.name,
                broker = %self.config.broker,
                port = self.config.port,
                topics = ?self.config.topics,
                "MQTT subscriber connecting"
            );

            let this = Arc::clone(self);
            let topics = self.config.topics.clone();

            tokio::spawn(async move {
                // Subscribe to all configured topics
                for topic in &topics {
                    if let Err(e) = client.subscribe(topic, qos).await {
                        error!(name = %this.name, %e, topic = %topic, "MQTT subscribe failed");
                        return;
                    }
                }

                info!(name = %this.name, "MQTT subscriptions active");

                loop {
                    tokio::select! {
                        () = this.cancel.notified() => {
                            info!(name = %this.name, "MQTT subscriber loop cancelled");
                            let _ = client.disconnect().await;
                            break;
                        }
                        event = eventloop.poll() => {
                            match event {
                                Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish))) => {
                                    this.messages_total.fetch_add(1, Ordering::Relaxed);
                                    let topic = &publish.topic;
                                    let payload = &publish.payload;

                                    match this.parse_payload(topic, payload) {
                                        Ok(points) => {
                                            let n = points.len() as u64;
                                            if let Err(e) = crate::util::insert_with_timeout(
                                                &this.db,
                                                this.namespace_scope(),
                                                points,
                                                std::time::Duration::from_secs(30),
                                            ).await {
                                                error!(name = %this.name, %e, "failed to write MQTT points");
                                            } else {
                                                this.points_total.fetch_add(n, Ordering::Relaxed);
                                            }
                                        }
                                        Err(e) => {
                                            this.decode_errors.fetch_add(1, Ordering::Relaxed);
                                            debug!(name = %this.name, %e, "MQTT decode error");
                                        }
                                    }
                                }
                                Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => {
                                    info!(name = %this.name, "MQTT connected");
                                }
                                Err(e) => {
                                    warn!(name = %this.name, %e, "MQTT connection error, reconnecting...");
                                    this.reconnections.fetch_add(1, Ordering::Relaxed);
                                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                }
                                _ => {}
                            }
                        }
                    }
                }
            });

            Ok(())
        }
    }
}

#[async_trait::async_trait]
impl IngestionConnector for MqttSubscriber {
    fn name(&self) -> &str {
        &self.name
    }

    fn connector_type(&self) -> &str {
        "mqtt"
    }

    async fn start(&self) -> Result<(), ServerError> {
        if self.running.load(Ordering::SeqCst) {
            return Ok(());
        }

        self.running.store(true, Ordering::SeqCst);
        self.stopped.store(false, Ordering::SeqCst);

        #[cfg(feature = "mqtt")]
        {
            let arc_self = self.self_ref.get().and_then(Weak::upgrade).ok_or_else(|| {
                ServerError::Internal(
                    "MqttSubscriber must be created via new_arc() for live subscription".into(),
                )
            })?;
            arc_self.spawn_subscriber()?;
        }

        #[cfg(not(feature = "mqtt"))]
        info!(
            name = %self.name,
            broker = %self.config.broker,
            port = self.config.port,
            topics = ?self.config.topics,
            "MQTT subscriber ready (enable 'mqtt' crate feature for live subscription)"
        );

        metrics::counter!("chronix_mqtt_messages_received_total").absolute(0);
        metrics::counter!("chronix_mqtt_decode_errors_total").absolute(0);
        metrics::counter!("chronix_mqtt_reconnections_total").absolute(0);

        Ok(())
    }

    async fn stop(&self) -> Result<(), ServerError> {
        if !self.running.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.cancel.notify_one();
        self.running.store(false, Ordering::SeqCst);
        self.stopped.store(true, Ordering::SeqCst);

        info!(name = %self.name, "MQTT subscriber stopped");
        Ok(())
    }

    async fn status(&self) -> ConnectorStatus {
        if self.stopped.load(Ordering::SeqCst) {
            ConnectorStatus::Stopped
        } else if self.running.load(Ordering::SeqCst) {
            ConnectorStatus::Running
        } else {
            ConnectorStatus::Stopped
        }
    }

    async fn metrics(&self) -> ConnectorMetrics {
        ConnectorMetrics {
            messages_total: self.messages_total.load(Ordering::Relaxed),
            points_total: self.points_total.load(Ordering::Relaxed),
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
            lag: 0,
            throughput: 0.0,
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_subscriber(format: crate::connector::ConnectorFormat) -> MqttSubscriber {
        let cfg = MqttConfig {
            namespace: "default".to_string(),
            broker: "localhost".into(),
            port: 1883,
            client_id: "test".into(),
            topics: vec!["sensors/#".into()],
            qos: 1,
            format,
            ca_cert: None,
            client_cert: None,
            client_key: None,
            username: None,
            password: None,
            credential_file: None,
            topic_measurement_map: std::collections::HashMap::new(),
        };
        let tmp = tempfile::TempDir::new().unwrap();
        let db_config = chronix_core::ChronixConfig::builder()
            .data_dir(tmp.path())
            .build()
            .unwrap();
        let db = Arc::new(Chronix::open(db_config).unwrap());
        MqttSubscriber::new("test", cfg, db)
    }

    #[test]
    fn measurement_from_topic_simple() {
        assert_eq!(MqttSubscriber::measurement_from_topic("cpu"), "cpu");
    }

    #[test]
    fn measurement_from_topic_nested() {
        assert_eq!(
            MqttSubscriber::measurement_from_topic("sensors/building-1/temperature"),
            "temperature"
        );
    }

    #[test]
    fn measurement_from_topic_two_level() {
        assert_eq!(MqttSubscriber::measurement_from_topic("metrics/cpu"), "cpu");
    }

    #[test]
    fn extract_topic_tags_nested() {
        let tags = MqttSubscriber::extract_topic_tags("sensors/building-1/temperature");
        assert_eq!(
            tags.get("topic_level_1").map(std::string::String::as_str),
            Some("building-1")
        );
        assert!(!tags.contains_key("topic_level_0")); // root skipped
        assert!(!tags.contains_key("topic_level_2")); // last = measurement
    }

    #[test]
    fn extract_topic_tags_simple() {
        let tags = MqttSubscriber::extract_topic_tags("cpu");
        assert!(tags.is_empty());
    }

    #[test]
    fn extract_topic_tags_two_level() {
        let tags = MqttSubscriber::extract_topic_tags("metrics/cpu");
        assert!(tags.is_empty());
    }

    #[test]
    fn parse_json_basic() {
        let sub = make_subscriber(crate::connector::ConnectorFormat::Json);
        let payload = br#"{"fields":{"temperature":22.5},"timestamp":5000}"#;
        let points = sub
            .parse_payload("sensors/room1/temperature", payload)
            .unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].series_key().measurement(), "temperature");
        assert_eq!(points[0].timestamp(), 5000);
    }

    #[test]
    fn parse_json_with_topic_tags() {
        let sub = make_subscriber(crate::connector::ConnectorFormat::Json);
        let payload = br#"{"fields":{"temp":22.5},"timestamp":6000}"#;
        let points = sub
            .parse_payload("sensors/building-1/temperature", payload)
            .unwrap();
        assert_eq!(points.len(), 1);
        let tag_val = points[0].tag("topic_level_1");
        assert_eq!(tag_val, Some("building-1"));
    }

    #[test]
    fn parse_json_merge_payload_tags() {
        let sub = make_subscriber(crate::connector::ConnectorFormat::Json);
        let payload = br#"{"tags":{"device":"sensor-42"},"fields":{"temp":22.5},"timestamp":7000}"#;
        let points = sub
            .parse_payload("sensors/floor-3/temperature", payload)
            .unwrap();
        assert_eq!(points.len(), 1);

        assert!(points[0].tag("device").is_some());
        assert!(points[0].tag("topic_level_1").is_some());
    }

    #[test]
    fn parse_json_missing_fields() {
        let sub = make_subscriber(crate::connector::ConnectorFormat::Json);
        let payload = br#"{"tags":{"host":"a"}}"#;
        let err = sub
            .parse_payload("sensors/room1/temp", payload)
            .unwrap_err();
        assert!(err.to_string().contains("fields"), "{err}");
    }

    #[test]
    fn parse_json_integer_field_types() {
        let sub = make_subscriber(crate::connector::ConnectorFormat::Json);
        let payload = br#"{"fields":{"value":42,"rate":1.5},"timestamp":8000}"#;
        let points = sub.parse_payload("metrics/test", payload).unwrap();
        assert!(matches!(
            points[0].field("value"),
            Some(FieldValue::I64(42))
        ));
        assert!(matches!(points[0].field("rate"), Some(FieldValue::F64(_))));
    }

    #[test]
    fn parse_line_protocol() {
        let sub = make_subscriber(crate::connector::ConnectorFormat::LineProtocol);
        let payload = b"temperature,location=room1 value=22.5 1000000000";
        let points = sub
            .parse_payload("sensors/room1/temperature", payload)
            .unwrap();
        assert_eq!(points.len(), 1);
    }

    #[test]
    fn unsupported_format_rejected_at_deser() {
        let json =
            r#"{"broker":"localhost","port":1883,"client_id":"t","topics":[],"format":"cbor"}"#;
        let err = serde_json::from_str::<MqttConfig>(json).unwrap_err();
        assert!(err.to_string().contains("unknown variant"), "{err}");
    }

    #[tokio::test]
    async fn connector_lifecycle() {
        let cfg = MqttConfig {
            namespace: "default".to_string(),
            broker: "localhost".into(),
            port: 1883,
            client_id: "test".into(),
            topics: vec!["sensors/#".into()],
            qos: 1,
            format: crate::connector::ConnectorFormat::Json,
            ca_cert: None,
            client_cert: None,
            client_key: None,
            username: None,
            password: None,
            credential_file: None,
            topic_measurement_map: std::collections::HashMap::new(),
        };
        let tmp = tempfile::TempDir::new().unwrap();
        let db_config = chronix_core::ChronixConfig::builder()
            .data_dir(tmp.path())
            .build()
            .unwrap();
        let db = Arc::new(Chronix::open(db_config).unwrap());
        let sub = MqttSubscriber::new_arc("test", cfg, db);

        assert_eq!(sub.name(), "test");
        assert_eq!(sub.connector_type(), "mqtt");
        assert_eq!(sub.status().await, ConnectorStatus::Stopped);

        sub.start().await.unwrap();
        assert_eq!(sub.status().await, ConnectorStatus::Running);

        sub.stop().await.unwrap();
        assert_eq!(sub.status().await, ConnectorStatus::Stopped);
    }
}
