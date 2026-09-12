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
    /// Whether the server enforces namespace isolation.
    ///
    /// Decides whether points get a namespace tag: without it a
    /// multi-tenant deployment could not read its own connector data.
    // Read only by this connector's own loop, which is feature-gated. The
    // guard used to name *both* connector features, so building with only the
    // other one warned.
    #[cfg_attr(not(feature = "mqtt"), allow(dead_code))]
    multi_tenancy: bool,
    running: AtomicBool,
    stopped: AtomicBool,
    cancel: tokio::sync::Notify,
    messages_total: AtomicU64,
    points_total: AtomicU64,
    decode_errors: AtomicU64,
    /// When the connector was constructed, for the throughput figure.
    started_at: std::time::Instant,
    /// Reconnection counter — used by the subscriber loop when the `mqtt`
    /// feature is enabled.
    #[cfg_attr(not(feature = "mqtt"), allow(dead_code))]
    reconnections: AtomicU64,
    /// What the subscriber loop is doing. `running`/`stopped` record only
    /// the caller's intent, so they cannot tell a connected subscriber from
    /// one that never reached the broker.
    #[cfg_attr(not(feature = "mqtt"), allow(dead_code))]
    state: parking_lot::Mutex<ConnectorStatus>,
}

impl MqttSubscriber {
    /// Record what the subscriber loop is doing. `start()`/`stop()` own the
    /// `Stopped` transitions; everything else is an observation.
    #[cfg_attr(not(feature = "mqtt"), allow(dead_code))]
    fn set_state(&self, next: ConnectorStatus) {
        *self.state.lock() = next;
    }

    /// Namespace this connector writes to.
    ///
    /// A connector carries no request, so its namespace comes from its own
    /// configuration rather than a header.
    ///
    /// The decision is **whether the server is multi-tenant**, not whether
    /// the namespace happens to be `default`. Skipping the tag for the
    /// default namespace left a multi-tenant deployment's connector data
    /// untagged, and every scoped read filters on the tag — so the points
    /// were stored and unreachable through the database's own query API.
    /// A single-tenant server's points stay untagged, so a series does not
    /// depend on which route wrote it.
    #[cfg(feature = "mqtt")]
    fn namespace_scope(&self) -> Option<&str> {
        self.multi_tenancy.then_some(self.config.namespace.as_str())
    }

    /// Create a new MQTT subscriber connector wrapped in an `Arc`.
    ///
    /// This is the preferred constructor — it stores a `Weak<Self>`
    /// internally so that `start()` can safely spawn background tasks
    /// without `unsafe` Arc reconstruction.
    pub fn new_arc(
        name: &str,
        config: MqttConfig,
        db: Arc<Chronix>,
        multi_tenancy: bool,
    ) -> Arc<Self> {
        let arc = Arc::new(Self {
            name: name.to_string(),
            config,
            db,
            multi_tenancy,
            self_ref: OnceLock::new(),
            running: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            cancel: tokio::sync::Notify::new(),
            messages_total: AtomicU64::new(0),
            points_total: AtomicU64::new(0),
            decode_errors: AtomicU64::new(0),
            started_at: std::time::Instant::now(),
            state: parking_lot::Mutex::new(ConnectorStatus::Stopped),
            reconnections: AtomicU64::new(0),
        });
        let _ = arc.self_ref.set(Arc::downgrade(&arc));
        arc
    }

    /// Create a new MQTT subscriber connector (non-Arc).
    ///
    /// Useful for unit-testing parsing logic. The `mqtt` feature-gated
    /// subscriber loop will not be available without calling `new_arc`.
    pub fn new(name: &str, config: MqttConfig, db: Arc<Chronix>, multi_tenancy: bool) -> Self {
        Self {
            name: name.to_string(),
            config,
            db,
            multi_tenancy,
            self_ref: OnceLock::new(),
            running: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            cancel: tokio::sync::Notify::new(),
            messages_total: AtomicU64::new(0),
            points_total: AtomicU64::new(0),
            decode_errors: AtomicU64::new(0),
            started_at: std::time::Instant::now(),
            state: parking_lot::Mutex::new(ConnectorStatus::Stopped),
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

    /// How long to wait before the first retry of a failed subscribe.
    const SUBSCRIBE_BACKOFF_MIN: std::time::Duration = std::time::Duration::from_millis(250);
    /// The ceiling the retry backoff doubles up to.
    const SUBSCRIBE_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_secs(30);
    /// Consecutive subscribe failures after which the status names the
    /// error rather than saying "reconnecting". Retrying continues.
    const SUBSCRIBE_ATTEMPTS_BEFORE_FAILED: u32 = 5;
    use rumqttc::{AsyncClient, MqttOptions, QoS};
    use tracing::{debug, error, warn};

    impl MqttSubscriber {
        /// Spawn the MQTT event-loop as a background task.
        ///
        /// Subscribes to all configured topics, processes incoming
        /// `Publish` events, and writes parsed points into Chronix.
        pub(super) fn spawn_subscriber(self: &Arc<Self>) -> Result<(), ServerError> {
            // TLS is not available in this build (see the `rumqttc` note in
            // the workspace manifest), so a configuration that names a
            // certificate is refused rather than quietly connecting in
            // plaintext. Silently downgrading is the worst outcome: the
            // operator believes the link is encrypted and ships broker
            // credentials over it in the clear.
            if self.config.ca_cert.is_some()
                || self.config.client_cert.is_some()
                || self.config.client_key.is_some()
            {
                return Err(ServerError::BadRequest(format!(
                    "MQTT connector `{}` configures TLS, which this build does not \
                     support; remove `ca_cert`/`client_cert`/`client_key` to connect \
                     in plaintext, or terminate TLS in front of the broker",
                    self.name
                )));
            }

            let mut opts = MqttOptions::new(
                &self.config.client_id,
                &self.config.broker,
                self.config.port,
            );
            opts.set_clean_session(true);
            opts.set_keep_alive(std::time::Duration::from_secs(30));

            // Broker authentication. `effective_credentials` existed and
            // nothing called it, so a broker that required a password
            // refused every connection and the configured username was
            // never sent.
            if let Some((username, password)) = self.config.effective_credentials()? {
                opts.set_credentials(username, password);
            }

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
                // Subscribing is retried: the commonest failure is the
                // transient one, a broker still coming up beside us.
                let mut backoff = SUBSCRIBE_BACKOFF_MIN;
                let mut attempts: u32 = 0;
                'subscribe: loop {
                    if this.stopped.load(Ordering::SeqCst) {
                        return;
                    }
                    let mut failure = None;
                    for topic in &topics {
                        if let Err(e) = client.subscribe(topic, qos).await {
                            failure = Some(format!("subscribe to {topic} failed: {e}"));
                            break;
                        }
                    }
                    let Some(reason) = failure else {
                        break 'subscribe;
                    };

                    attempts += 1;
                    if attempts >= SUBSCRIBE_ATTEMPTS_BEFORE_FAILED {
                        error!(name = %this.name, attempts, %reason, "MQTT still cannot subscribe");
                        this.set_state(ConnectorStatus::Failed(reason));
                    } else {
                        warn!(name = %this.name, attempts, %reason, "MQTT subscribe failed, retrying");
                        this.set_state(ConnectorStatus::Reconnecting);
                    }
                    tokio::select! {
                        () = this.cancel.notified() => return,
                        () = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(SUBSCRIBE_BACKOFF_MAX);
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
                                            match crate::util::insert_from_connector(
                                                &this.db,
                                                this.namespace_scope(),
                                                points,
                                                std::time::Duration::from_secs(30),
                                                &this.name,
                                            ).await {
                                                Ok(n) => {
                                                    this.points_total.fetch_add(n, Ordering::Relaxed);
                                                }
                                                Err(e) => {
                                                    error!(name = %this.name, %e, "failed to write MQTT points");
                                                }
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
                                    this.set_state(ConnectorStatus::Running);
                                }
                                Err(e) => {
                                    warn!(name = %this.name, %e, "MQTT connection error, reconnecting...");
                                    this.reconnections.fetch_add(1, Ordering::Relaxed);
                                    this.set_state(ConnectorStatus::Reconnecting);
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
        // Nothing has reached the broker yet; promoted on the first ConnAck.
        self.set_state(ConnectorStatus::Reconnecting);

        #[cfg(feature = "mqtt")]
        {
            let arc_self = self.self_ref.get().and_then(Weak::upgrade).ok_or_else(|| {
                ServerError::Internal(
                    "MqttSubscriber must be created via new_arc() for live subscription".into(),
                )
            })?;
            if let Err(e) = arc_self.spawn_subscriber() {
                self.running.store(false, Ordering::SeqCst);
                self.stopped.store(true, Ordering::SeqCst);
                self.set_state(ConnectorStatus::Failed(e.to_string()));
                return Err(e);
            }
        }

        #[cfg(not(feature = "mqtt"))]
        self.set_state(ConnectorStatus::Idle);
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
        self.set_state(ConnectorStatus::Stopped);

        info!(name = %self.name, "MQTT subscriber stopped");
        Ok(())
    }

    /// What the subscriber loop is doing, not what `start()` intended.
    async fn status(&self) -> ConnectorStatus {
        if self.stopped.load(Ordering::SeqCst) || !self.running.load(Ordering::SeqCst) {
            return ConnectorStatus::Stopped;
        }
        self.state.lock().clone()
    }

    async fn metrics(&self) -> ConnectorMetrics {
        let points = self.points_total.load(Ordering::Relaxed);
        ConnectorMetrics {
            messages_total: self.messages_total.load(Ordering::Relaxed),
            points_total: points,
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
            // MQTT is a push protocol: there is no broker-side offset to be
            // behind, so there is no lag to report. `None` says that; `0`
            // used to claim "caught up", which is a different statement.
            lag: None,
            throughput: crate::connector::throughput(points, self.started_at),
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
        MqttSubscriber::new("test", cfg, db, false)
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
        let sub = MqttSubscriber::new_arc("test", cfg, db, false);

        assert_eq!(sub.name(), "test");
        assert_eq!(sub.connector_type(), "mqtt");
        assert_eq!(sub.status().await, ConnectorStatus::Stopped);

        sub.start().await.unwrap();
        // No broker here, so `Running` is the one thing this must not say:
        // started, trying, not yet connected.
        assert_eq!(sub.status().await, ConnectorStatus::Reconnecting);
        assert!(!sub.is_healthy().await);

        sub.stop().await.unwrap();
        assert_eq!(sub.status().await, ConnectorStatus::Stopped);
    }

    /// The TLS fields were accepted and dropped, so a connector configured
    /// with a CA certificate connected in plaintext and shipped its broker
    /// password in the clear. A refusal is the only honest answer while
    /// the build has no TLS transport.
    #[cfg(feature = "mqtt")]
    #[test]
    fn a_tls_configuration_is_refused_rather_than_downgraded() {
        let mut subscriber = make_subscriber(crate::connector::ConnectorFormat::Json);
        subscriber.config.ca_cert = Some("/etc/ssl/broker-ca.pem".to_string());
        let subscriber = std::sync::Arc::new(subscriber);
        let err = subscriber
            .spawn_subscriber()
            .expect_err("TLS must not be silently ignored");
        assert!(
            err.to_string().contains("does not support"),
            "the error must say why: {err}"
        );
    }

    /// `effective_credentials` existed and nothing called it, so a broker
    /// requiring a password refused every connection.
    #[test]
    fn credentials_come_from_the_file_when_one_is_given() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("creds.json");
        std::fs::write(&path, r#"{"username":"mqtt_user","password":"s3cret"}"#).unwrap();

        let mut cfg = make_subscriber(crate::connector::ConnectorFormat::Json).config;
        cfg.username = Some("inline".into());
        cfg.password = Some("inline".into());
        cfg.credential_file = Some(path.to_string_lossy().into_owned());

        let (user, pass) = cfg.effective_credentials().unwrap().expect("credentials");
        assert_eq!(user, "mqtt_user", "the file must win, so rotation works");
        assert_eq!(pass, "s3cret");
    }
}
