//! Kafka consumer ingestion connector.
//!
//! Consumes messages from Kafka topics and writes time-series data into
//! the Chronix database. Supports InfluxDB Line Protocol and JSON
//! deserialization formats.
//!
//! # Feature gate
//!
//! The consumer loop requires the `kafka` feature, which brings in
//! `krafka` — a pure-Rust, async-native client. Without the feature the
//! connector still constructs and its parsing logic is still testable, and
//! `start()` logs a notice rather than connecting to a broker.
//!
//! `krafka` rather than `rdkafka`: `rdkafka` compiles librdkafka from C
//! through cmake, so enabling Kafka ingestion imposed a C toolchain on every
//! build — including the aarch64 cross-compile the embedded wedge depends on
//! — and put an unaudited FFI surface inside a tree whose policy is
//! `deny(unsafe_code)`. `krafka` is pure Rust with the same policy.
//!
//! # Configuration
//!
//! ```toml
//! [kafka]
//! brokers = "localhost:9092"
//! group_id = "chronix-ingest"
//! topics = ["metrics"]
//! format = "line_protocol"        # or "json"
//! auto_offset_reset = "latest"    # or "earliest"
//!
//! # Optional transport security. Before this was wired up the four fields
//! # below parsed and were then dropped on the floor: a broker configured
//! # for SASL_SSL was connected to in plaintext with no credentials, and the
//! # only symptom was a connection failure that named nothing.
//! security_protocol = "SASL_SSL"  # PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
//! sasl_mechanism = "SCRAM-SHA-512"
//! credential_file = "/run/secrets/kafka.json"
//! ca_cert = "/etc/ssl/private-ca.pem"
//! ```

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use tracing::info;

use chronix::prelude::*;
use chronix::Chronix;

use crate::connector::{ConnectorMetrics, ConnectorStatus, IngestionConnector, KafkaConfig};
use crate::error::ServerError;
use crate::influx;

/// Kafka consumer connector.
///
/// Spawns a background tokio task that polls messages from the configured
/// topics, deserializes them, and writes the resulting points into Chronix.
///
/// # Delivery semantics
///
/// Offsets are committed after points are written to the WAL
/// (at-least-once).
pub struct KafkaConsumer {
    name: String,
    config: KafkaConfig,
    /// Database handle — used by the consumer loop when the `kafka` feature
    /// is enabled.
    #[cfg_attr(not(feature = "kafka"), allow(dead_code))]
    db: Arc<Chronix>,
    /// Weak self-reference for safe background task spawning.
    /// Populated by [`new_arc`] — avoids `unsafe` Arc reconstruction.
    #[cfg_attr(not(feature = "kafka"), allow(dead_code))]
    self_ref: OnceLock<Weak<Self>>,
    /// Whether the server enforces namespace isolation.
    ///
    /// Decides whether points get a namespace tag: without it a
    /// multi-tenant deployment could not read its own connector data.
    #[cfg_attr(not(any(feature = "kafka", feature = "mqtt")), allow(dead_code))]
    multi_tenancy: bool,
    running: AtomicBool,
    stopped: AtomicBool,
    cancel: tokio::sync::Notify,
    messages_total: AtomicU64,
    points_total: AtomicU64,
    decode_errors: AtomicU64,
}

impl KafkaConsumer {
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
    #[cfg(feature = "kafka")]
    fn namespace_scope(&self) -> Option<&str> {
        self.multi_tenancy.then_some(self.config.namespace.as_str())
    }

    /// Create a new Kafka consumer connector wrapped in an `Arc`.
    ///
    /// This is the preferred constructor — it stores a `Weak<Self>`
    /// internally so that `start()` can safely spawn background tasks
    /// without `unsafe` Arc reconstruction.
    pub fn new_arc(
        name: &str,
        config: KafkaConfig,
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
        });
        let _ = arc.self_ref.set(Arc::downgrade(&arc));
        arc
    }

    /// Create a new Kafka consumer connector (non-Arc).
    ///
    /// Useful for unit-testing parsing logic. The `kafka` feature-gated
    /// consumer loop will not be available without calling `new_arc`.
    pub fn new(name: &str, config: KafkaConfig, db: Arc<Chronix>, multi_tenancy: bool) -> Self {
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
        }
    }

    /// Resolve the measurement name for a given topic.
    pub fn measurement_for_topic(&self, topic: &str) -> String {
        self.config
            .topic_measurement_map
            .get(topic)
            .cloned()
            .unwrap_or_else(|| topic.to_string())
    }

    /// Parse a message payload into points.
    ///
    /// Delegates to the shared [`crate::util::parse_json_point`] for JSON
    /// format, or [`crate::influx::parse_line_protocol`] for InfluxDB
    /// line protocol.
    pub fn parse_payload(&self, topic: &str, payload: &[u8]) -> Result<Vec<Point>, ServerError> {
        let text = std::str::from_utf8(payload)
            .map_err(|e| ServerError::BadRequest(format!("invalid UTF-8 in Kafka message: {e}")))?;

        match self.config.format {
            crate::connector::ConnectorFormat::LineProtocol => influx::parse_line_protocol(text)
                .map_err(|e| ServerError::BadRequest(format!("line protocol parse error: {e}"))),
            crate::connector::ConnectorFormat::Json => {
                let measurement = self.measurement_for_topic(topic);
                crate::util::parse_json_point(&measurement, text, BTreeMap::new())
            }
        }
    }
}

// ── Feature-gated consumer loop ────────────────────────────────────────

#[cfg(feature = "kafka")]
mod consumer_impl {
    use super::*;
    use std::time::Duration;

    use krafka::auth::{AuthConfig, TlsConfig};
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use tracing::{debug, error, warn};

    /// How long one `poll` waits for records before yielding to the
    /// cancellation branch.
    const POLL_TIMEOUT: Duration = Duration::from_secs(1);

    /// Ceiling on one batch write, so a stalled insert cannot pin the loop.
    const INSERT_TIMEOUT: Duration = Duration::from_secs(30);

    impl KafkaConsumer {
        /// Translate the connector's stringly-typed offset reset.
        fn offset_reset(&self) -> Result<AutoOffsetReset, ServerError> {
            match self.config.auto_offset_reset.to_ascii_lowercase().as_str() {
                "earliest" => Ok(AutoOffsetReset::Earliest),
                "latest" => Ok(AutoOffsetReset::Latest),
                "none" => Ok(AutoOffsetReset::None),
                other => Err(ServerError::BadRequest(format!(
                    "kafka auto_offset_reset must be earliest, latest or none, got {other:?}"
                ))),
            }
        }

        /// Build the TLS material for an `SSL` / `SASL_SSL` deployment.
        ///
        /// The default trust anchors are the bundled WebPKI roots, which
        /// cover every managed broker; `ca_cert` adds the private CA that
        /// self-hosted clusters issue from. The platform trust store is
        /// deliberately not consulted — it is one more dependency and one
        /// more thing that differs between the build host and the gateway.
        fn tls_config(&self) -> TlsConfig {
            match self.config.ca_cert {
                Some(ref path) => TlsConfig::new().with_ca_cert(path.clone()),
                None => TlsConfig::new(),
            }
        }

        /// Resolve `security_protocol` + `sasl_mechanism` + credentials into
        /// one authentication configuration.
        ///
        /// Every combination is either understood or rejected by name. The
        /// alternative — ignoring what is not understood — is how these four
        /// fields came to be parsed, documented and unused.
        fn auth_config(&self) -> Result<Option<AuthConfig>, ServerError> {
            let protocol = self
                .config
                .security_protocol
                .as_deref()
                .unwrap_or("PLAINTEXT")
                .to_ascii_uppercase();
            let needs_sasl = matches!(protocol.as_str(), "SASL_PLAINTEXT" | "SASL_SSL");
            let needs_tls = matches!(protocol.as_str(), "SSL" | "SASL_SSL");

            match protocol.as_str() {
                "PLAINTEXT" | "SSL" | "SASL_PLAINTEXT" | "SASL_SSL" => {}
                other => {
                    return Err(ServerError::BadRequest(format!(
                        "kafka security_protocol must be PLAINTEXT, SSL, SASL_PLAINTEXT or \
                         SASL_SSL, got {other:?}"
                    )))
                }
            }

            if !needs_sasl {
                if !needs_tls {
                    return Ok(None);
                }
                return Ok(Some(AuthConfig::ssl(self.tls_config())));
            }

            let (username, password) = self.config.effective_credentials()?.ok_or_else(|| {
                ServerError::BadRequest(format!(
                    "kafka security_protocol {protocol} needs credentials: set \
                     credential_file, or sasl_username and sasl_password"
                ))
            })?;

            let mechanism = self
                .config
                .sasl_mechanism
                .as_deref()
                .unwrap_or("PLAIN")
                .to_ascii_uppercase();
            let auth = match mechanism.as_str() {
                "PLAIN" => AuthConfig::sasl_plain(username, password)
                    .map_err(|e| ServerError::BadRequest(format!("kafka SASL/PLAIN: {e}")))?,
                "SCRAM-SHA-256" => AuthConfig::sasl_scram_sha256(username, password),
                "SCRAM-SHA-512" => AuthConfig::sasl_scram_sha512(username, password),
                other => {
                    return Err(ServerError::BadRequest(format!(
                        "kafka sasl_mechanism must be PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512, \
                         got {other:?}"
                    )))
                }
            };

            Ok(Some(if needs_tls {
                auth.with_tls(self.tls_config())
            } else {
                auth
            }))
        }

        /// Spawn the Kafka consumer loop as a background task.
        ///
        /// Each poll batch is parsed, written in one insert, and only then
        /// committed — so a crash between write and commit replays the batch
        /// rather than losing it (at-least-once).
        pub(super) fn spawn_consumer(self: &Arc<Self>) -> Result<(), ServerError> {
            let offset_reset = self.offset_reset()?;
            let auth = self.auth_config()?;
            let this = Arc::clone(self);

            tokio::spawn(async move {
                let mut builder = Consumer::builder()
                    .bootstrap_servers(this.config.brokers.clone())
                    .group_id(this.config.group_id.clone())
                    .client_id(format!("chronixd-{}", this.name))
                    .auto_offset_reset(offset_reset)
                    .enable_auto_commit(false);
                if let Some(auth) = auth {
                    builder = builder.auth(auth);
                }

                let consumer = match builder.build().await {
                    Ok(c) => c,
                    Err(e) => {
                        error!(name = %this.name, %e, "Kafka consumer creation failed");
                        return;
                    }
                };

                let topics: Vec<&str> = this.config.topics.iter().map(String::as_str).collect();
                if let Err(e) = consumer.subscribe(&topics).await {
                    error!(name = %this.name, %e, "Kafka subscribe failed");
                    return;
                }

                info!(
                    name = %this.name,
                    brokers = %this.config.brokers,
                    topics = ?this.config.topics,
                    "Kafka consumer polling started"
                );

                loop {
                    let records = tokio::select! {
                        () = this.cancel.notified() => {
                            info!(name = %this.name, "Kafka consumer loop cancelled");
                            break;
                        }
                        polled = consumer.poll(POLL_TIMEOUT) => match polled {
                            Ok(r) => r,
                            Err(e) => {
                                warn!(name = %this.name, %e, "Kafka poll error");
                                continue;
                            }
                        }
                    };
                    if records.is_empty() {
                        continue;
                    }

                    // One insert per poll batch: a WAL group commit per
                    // record is what makes a Kafka-fed gateway fsync-bound.
                    let mut points = Vec::new();
                    for record in &records {
                        this.messages_total.fetch_add(1, Ordering::Relaxed);
                        let Some(payload) = record.value.as_ref() else {
                            continue; // tombstone
                        };
                        match this.parse_payload(&record.topic, payload) {
                            Ok(parsed) => points.extend(parsed),
                            Err(e) => {
                                this.decode_errors.fetch_add(1, Ordering::Relaxed);
                                debug!(name = %this.name, %e, "Kafka decode error");
                            }
                        }
                    }

                    if !points.is_empty() {
                        match crate::util::insert_from_connector(
                            &this.db,
                            this.namespace_scope(),
                            points,
                            INSERT_TIMEOUT,
                            &this.name,
                        )
                        .await
                        {
                            Ok(n) => this.points_total.fetch_add(n, Ordering::Relaxed),
                            Err(e) => {
                                // Not committing is the whole retry mechanism:
                                // the batch is redelivered instead of dropped.
                                // A *partial* write is not routed here — it
                                // would be redelivered for ever, since its
                                // rejections are deterministic.
                                error!(name = %this.name, %e, "failed to write Kafka points");
                                continue;
                            }
                        };
                    }

                    if let Err(e) = consumer.commit().await {
                        warn!(name = %this.name, %e, "Kafka offset commit failed");
                    }
                }

                if let Err(e) = consumer.close().await {
                    warn!(name = %this.name, %e, "Kafka consumer close failed");
                }
            });

            Ok(())
        }
    }
}

#[async_trait::async_trait]
impl IngestionConnector for KafkaConsumer {
    fn name(&self) -> &str {
        &self.name
    }

    fn connector_type(&self) -> &str {
        "kafka"
    }

    async fn start(&self) -> Result<(), ServerError> {
        if self.running.load(Ordering::SeqCst) {
            return Ok(());
        }

        self.running.store(true, Ordering::SeqCst);
        self.stopped.store(false, Ordering::SeqCst);

        #[cfg(feature = "kafka")]
        {
            let arc_self = self.self_ref.get().and_then(Weak::upgrade).ok_or_else(|| {
                ServerError::Internal(
                    "KafkaConsumer must be created via new_arc() for live consumption".into(),
                )
            })?;
            if let Err(e) = arc_self.spawn_consumer() {
                // A rejected configuration must not leave the connector
                // reporting Running with no loop behind it.
                self.running.store(false, Ordering::SeqCst);
                self.stopped.store(true, Ordering::SeqCst);
                return Err(e);
            }
        }

        #[cfg(not(feature = "kafka"))]
        info!(
            name = %self.name,
            brokers = %self.config.brokers,
            topics = ?self.config.topics,
            "Kafka consumer ready (enable 'kafka' crate feature for live consumption)"
        );

        metrics::counter!("chronix_kafka_messages_consumed_total").absolute(0);
        metrics::counter!("chronix_kafka_deserialization_errors_total").absolute(0);

        Ok(())
    }

    async fn stop(&self) -> Result<(), ServerError> {
        if !self.running.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.cancel.notify_one();
        self.running.store(false, Ordering::SeqCst);
        self.stopped.store(true, Ordering::SeqCst);

        info!(name = %self.name, "Kafka consumer stopped");
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

    fn make_consumer(format: crate::connector::ConnectorFormat) -> KafkaConsumer {
        let cfg = KafkaConfig {
            namespace: "default".to_string(),
            brokers: "localhost:9092".into(),
            group_id: "test".into(),
            topics: vec!["test".into()],
            format,
            auto_offset_reset: "latest".into(),
            topic_measurement_map: Default::default(),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            security_protocol: None,
            ca_cert: None,
            credential_file: None,
        };
        let tmp = tempfile::TempDir::new().unwrap();
        let db_config = chronix_core::ChronixConfig::builder()
            .data_dir(tmp.path())
            .build()
            .unwrap();
        let db = Arc::new(Chronix::open(db_config).unwrap());
        KafkaConsumer::new("test", cfg, db, false)
    }

    #[test]
    fn parse_json_basic() {
        let consumer = make_consumer(crate::connector::ConnectorFormat::Json);
        let payload = br#"{"fields":{"cpu":87.5},"timestamp":1000}"#;
        let points = consumer.parse_payload("cpu_topic", payload).unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].series_key().measurement(), "cpu_topic");
        assert_eq!(points[0].timestamp(), 1000);
    }

    #[test]
    fn parse_json_integer_preserved() {
        let consumer = make_consumer(crate::connector::ConnectorFormat::Json);
        let payload = br#"{"fields":{"f":1.5,"i":42,"b":true,"s":"hello"},"timestamp":3000}"#;
        let points = consumer.parse_payload("multi", payload).unwrap();
        assert!(matches!(points[0].field("f"), Some(FieldValue::F64(_))));
        assert!(matches!(points[0].field("i"), Some(FieldValue::I64(42))));
        assert!(matches!(points[0].field("b"), Some(FieldValue::Bool(true))));
        assert!(matches!(points[0].field("s"), Some(FieldValue::String(_))));
    }

    #[test]
    fn parse_json_no_tags() {
        let consumer = make_consumer(crate::connector::ConnectorFormat::Json);
        let payload = br#"{"fields":{"temp":22.5},"timestamp":2000}"#;
        let points = consumer.parse_payload("sensor", payload).unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].series_key().measurement(), "sensor");
    }

    #[test]
    fn parse_json_missing_fields() {
        let consumer = make_consumer(crate::connector::ConnectorFormat::Json);
        let payload = br#"{"tags":{"host":"a"}}"#;
        let err = consumer.parse_payload("m", payload).unwrap_err();
        assert!(err.to_string().contains("fields"), "{err}");
    }

    #[test]
    fn parse_line_protocol() {
        let consumer = make_consumer(crate::connector::ConnectorFormat::LineProtocol);
        let payload = b"cpu,host=srv1 usage=87.5 1000000000";
        let points = consumer.parse_payload("cpu", payload).unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].series_key().measurement(), "cpu");
    }

    #[test]
    fn measurement_for_topic_default() {
        let consumer = make_consumer(crate::connector::ConnectorFormat::Json);
        assert_eq!(consumer.measurement_for_topic("cpu"), "cpu");
    }

    #[test]
    fn measurement_for_topic_mapped() {
        let mut map = std::collections::HashMap::new();
        map.insert("raw_cpu".to_string(), "cpu_metrics".to_string());

        let cfg = KafkaConfig {
            namespace: "default".to_string(),
            brokers: "localhost:9092".into(),
            group_id: "test".into(),
            topics: vec!["raw_cpu".into()],
            format: crate::connector::ConnectorFormat::Json,
            auto_offset_reset: "latest".into(),
            topic_measurement_map: map,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            security_protocol: None,
            ca_cert: None,
            credential_file: None,
        };
        let tmp = tempfile::TempDir::new().unwrap();
        let db_config = chronix_core::ChronixConfig::builder()
            .data_dir(tmp.path())
            .build()
            .unwrap();
        let db = Arc::new(Chronix::open(db_config).unwrap());

        let consumer = KafkaConsumer::new("test", cfg, db, false);
        assert_eq!(consumer.measurement_for_topic("raw_cpu"), "cpu_metrics");
        assert_eq!(consumer.measurement_for_topic("other"), "other");
    }

    #[tokio::test]
    async fn connector_lifecycle() {
        let cfg = KafkaConfig {
            namespace: "default".to_string(),
            brokers: "localhost:9092".into(),
            group_id: "test".into(),
            topics: vec!["test".into()],
            format: crate::connector::ConnectorFormat::Json,
            auto_offset_reset: "latest".into(),
            topic_measurement_map: Default::default(),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            security_protocol: None,
            ca_cert: None,
            credential_file: None,
        };
        let tmp = tempfile::TempDir::new().unwrap();
        let db_config = chronix_core::ChronixConfig::builder()
            .data_dir(tmp.path())
            .build()
            .unwrap();
        let db = Arc::new(Chronix::open(db_config).unwrap());
        let consumer = KafkaConsumer::new_arc("test", cfg, db, false);

        assert_eq!(consumer.name(), "test");
        assert_eq!(consumer.connector_type(), "kafka");
        assert_eq!(consumer.status().await, ConnectorStatus::Stopped);

        consumer.start().await.unwrap();
        assert_eq!(consumer.status().await, ConnectorStatus::Running);

        consumer.stop().await.unwrap();
        assert_eq!(consumer.status().await, ConnectorStatus::Stopped);
    }

    #[test]
    fn unsupported_format_rejected_at_deser() {
        let toml_str = r#"
            brokers = "localhost:9092"
            group_id = "test"
            topics = ["t"]
            format = "cbor"
        "#;
        let result: Result<crate::connector::KafkaConfig, _> = toml::from_str(toml_str);
        assert!(
            result.is_err(),
            "unknown format variant should fail deserialization"
        );
    }

    /// A connector's namespace tag used to be skipped whenever the value
    /// was `default`, which is the value most deployments leave in place —
    /// so on a multi-tenant server the connector's points were stored
    /// untagged, and every scoped read filters on the tag. The data was in
    /// the database and unreachable through the database's own query API.
    #[cfg(feature = "kafka")]
    #[test]
    fn a_multi_tenant_connector_tags_even_the_default_namespace() {
        let single = make_consumer(crate::connector::ConnectorFormat::LineProtocol);
        assert_eq!(single.config.namespace, crate::namespace::DEFAULT_NAMESPACE);
        assert_eq!(
            single.namespace_scope(),
            None,
            "a single-tenant server's reads are unscoped, so tagging would \
             make a connector's series differ from an HTTP writer's"
        );

        let multi = KafkaConsumer {
            multi_tenancy: true,
            ..single
        };
        assert_eq!(
            multi.namespace_scope(),
            Some("default"),
            "a multi-tenant server filters every read on the tag, so the \
             default namespace needs one too"
        );
    }
}
