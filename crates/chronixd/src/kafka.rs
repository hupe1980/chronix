//! Ingest Kafka records as InfluxDB line protocol or JSON.
//!
//! Build `chronixd` with the `kafka` feature to enable the Krafka consumer.
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
//! # Optional SASL over TLS with a private CA.
//! security_protocol = "SASL_SSL"  # PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
//! sasl_mechanism = "SCRAM-SHA-512"
//! credential_file = "/run/secrets/kafka.json"
//! ca_cert = "/etc/ssl/private-ca.pem"
//! ```

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use tracing::info;

use chronix::Chronix;
use chronix::prelude::*;

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
    /// Set by [`Self::new_arc`].
    #[cfg_attr(not(feature = "kafka"), allow(dead_code))]
    self_ref: OnceLock<Weak<Self>>,
    /// The server's per-namespace ingestion limiter; unlimited until the
    /// server shares its own ([`Self::with_ingest_limiter`]).
    ingest: OnceLock<crate::rate_limit::NamespaceIngestLimiter>,
    /// Whether the server enforces namespace isolation.
    ///
    /// Decides whether points get a namespace tag: without it a multi-tenant
    /// deployment could not read its own connector data.
    #[cfg_attr(not(feature = "kafka"), allow(dead_code))]
    multi_tenancy: bool,
    running: AtomicBool,
    stopped: AtomicBool,
    cancel: tokio::sync::Notify,
    messages_total: AtomicU64,
    points_total: AtomicU64,
    decode_errors: AtomicU64,
    /// Messages behind the newest broker offset, or `-1` before the first
    /// poll has told us. Refreshed from the consumer's **cached** watermarks
    /// after each batch, so reading it costs no broker round trip.
    lag: std::sync::atomic::AtomicI64,
    /// When the connector was constructed, for the throughput figure.
    started_at: std::time::Instant,
    /// What the consumer task is doing. `running`/`stopped` record only the
    /// caller's intent, so they cannot tell a connected consumer from one
    /// that never reached the broker.
    #[cfg_attr(not(feature = "kafka"), allow(dead_code))]
    state: parking_lot::Mutex<ConnectorStatus>,
}

impl KafkaConsumer {
    /// Charge this connector's writes against the server's per-namespace
    /// ingestion limits, as every other write path is.
    #[must_use]
    pub fn with_ingest_limiter(
        self: Arc<Self>,
        limiter: crate::rate_limit::NamespaceIngestLimiter,
    ) -> Arc<Self> {
        let _ = self.ingest.set(limiter);
        self
    }

    #[cfg_attr(not(feature = "kafka"), allow(dead_code))]
    fn ingest_limiter(&self) -> &crate::rate_limit::NamespaceIngestLimiter {
        self.ingest
            .get_or_init(crate::rate_limit::NamespaceIngestLimiter::new)
    }

    /// Record what the consumer task is doing. `start()`/`stop()` own the
    /// `Stopped` transitions; everything else is an observation.
    #[cfg_attr(not(feature = "kafka"), allow(dead_code))]
    fn set_state(&self, next: ConnectorStatus) {
        *self.state.lock() = next;
    }

    /// Scope writes to the configured namespace on multi-tenant servers.
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
            ingest: OnceLock::new(),
            running: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            cancel: tokio::sync::Notify::new(),
            messages_total: AtomicU64::new(0),
            points_total: AtomicU64::new(0),
            decode_errors: AtomicU64::new(0),
            lag: std::sync::atomic::AtomicI64::new(-1),
            started_at: std::time::Instant::now(),
            state: parking_lot::Mutex::new(ConnectorStatus::Stopped),
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
            ingest: OnceLock::new(),
            running: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            cancel: tokio::sync::Notify::new(),
            messages_total: AtomicU64::new(0),
            points_total: AtomicU64::new(0),
            decode_errors: AtomicU64::new(0),
            lag: std::sync::atomic::AtomicI64::new(-1),
            started_at: std::time::Instant::now(),
            state: parking_lot::Mutex::new(ConnectorStatus::Stopped),
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

    use krafka::Kafka;
    use krafka::auth::{AuthConfig, TlsConfig};
    use krafka::consumer::{AutoOffsetReset, Consumer, TopicPartition};
    use tracing::{debug, error, warn};

    const SETUP_BACKOFF_MIN: Duration = Duration::from_millis(250);
    /// The ceiling the retry backoff doubles up to.
    const SETUP_BACKOFF_MAX: Duration = Duration::from_secs(30);
    /// Report the setup error after repeated failures while continuing to retry.
    const SETUP_ATTEMPTS_BEFORE_FAILED: u32 = 8;
    /// Consecutive empty polls after which a connected consumer reports
    /// `Idle` rather than `Running`: connected, and nothing arriving.
    const EMPTY_POLLS_BEFORE_IDLE: u32 = 30;

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

        /// Use the configured CA bundle or the default WebPKI roots.
        fn tls_config(&self) -> TlsConfig {
            match self.config.ca_cert {
                Some(ref path) => TlsConfig::new().with_ca_cert(path.clone()),
                None => TlsConfig::new(),
            }
        }

        /// Build the client authentication settings and validate credentials.
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
                    )));
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
                "PLAIN" => {
                    // Krafka validates on connect. Reject malformed credentials at start().
                    if username.is_empty() || username.contains('\0') || password.contains('\0') {
                        return Err(ServerError::BadRequest(
                            "kafka SASL/PLAIN requires a username and NUL-free credentials".into(),
                        ));
                    }
                    AuthConfig::sasl_plain(username, password)
                }
                "SCRAM-SHA-256" => AuthConfig::sasl_scram_sha256(username, password),
                "SCRAM-SHA-512" => AuthConfig::sasl_scram_sha512(username, password),
                other => {
                    return Err(ServerError::BadRequest(format!(
                        "kafka sasl_mechanism must be PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512, \
                         got {other:?}"
                    )));
                }
            };

            Ok(Some(if needs_tls {
                auth.with_tls(self.tls_config())
            } else {
                auth
            }))
        }

        async fn connect_consumer(
            &self,
            auth: Option<AuthConfig>,
            offset_reset: AutoOffsetReset,
        ) -> krafka::Result<Consumer> {
            let mut builder =
                Kafka::builder(&self.config.brokers).client_id(format!("chronixd-{}", self.name));
            if let Some(auth) = auth {
                builder = builder.security(auth);
            }
            let kafka = builder.connect().await?;
            kafka
                .consumer(&self.config.group_id)
                .auto_offset_reset(offset_reset)
                .enable_auto_commit(false)
                .metrics_push(false)
                .build()
                .await
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
                // A newly started broker can return CoordinatorNotAvailable
                // until it creates the __consumer_offsets topic.
                let mut backoff = SETUP_BACKOFF_MIN;
                let mut attempts: u32 = 0;

                while !this.stopped.load(Ordering::SeqCst) {
                    let connected = tokio::select! {
                        () = this.cancel.notified() => return,
                        result = this.connect_consumer(auth.clone(), offset_reset) => result,
                    };
                    let result = match connected {
                        Ok(consumer) => {
                            let result = this.consume(&consumer).await;
                            if let Err(e) = consumer.close().await {
                                warn!(name = %this.name, %e, "Kafka consumer close failed");
                            }
                            result
                        }
                        Err(e) => Err(e),
                    };
                    let Err(reason) = result else { return };
                    attempts += 1;
                    if attempts >= SETUP_ATTEMPTS_BEFORE_FAILED {
                        error!(
                            name = %this.name, attempts, %reason,
                            "Kafka consumer still cannot start"
                        );
                        this.set_state(ConnectorStatus::Failed(reason.to_string()));
                    } else {
                        warn!(
                            name = %this.name, attempts, %reason,
                            "Kafka consumer setup failed, retrying"
                        );
                        this.set_state(ConnectorStatus::Reconnecting);
                    }

                    tokio::select! {
                        () = this.cancel.notified() => return,
                        () = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(SETUP_BACKOFF_MAX);
                }
            });

            Ok(())
        }

        async fn consume(&self, consumer: &Consumer) -> krafka::Result<()> {
            consumer.subscribe(&self.config.topics).await?;
            let mut joined = false;
            let mut empty_polls: u32 = 0;

            loop {
                let records = tokio::select! {
                    () = self.cancel.notified() => return Ok(()),
                    polled = consumer.poll(POLL_TIMEOUT) => match polled {
                        Ok(records) => records,
                        Err(e) if !joined => return Err(e),
                        Err(e) => {
                            warn!(name = %self.name, %e, "Kafka poll error");
                            continue;
                        }
                    }
                };
                if !joined {
                    // subscribe() registers interest. Polling completes the group join.
                    if records.is_empty()
                        && consumer
                            .group_metadata()
                            .await
                            .is_none_or(|m| m.generation_id() < 0)
                    {
                        continue;
                    }
                    joined = true;
                    info!(
                        name = %self.name,
                        brokers = %self.config.brokers,
                        topics = ?self.config.topics,
                        "Kafka consumer joined group"
                    );
                    self.set_state(ConnectorStatus::Running);
                }
                if records.is_empty() {
                    empty_polls = empty_polls.saturating_add(1);
                    if empty_polls == EMPTY_POLLS_BEFORE_IDLE {
                        self.set_state(ConnectorStatus::Idle);
                    }
                    continue;
                }
                if empty_polls >= EMPTY_POLLS_BEFORE_IDLE {
                    self.set_state(ConnectorStatus::Running);
                }
                empty_polls = 0;

                let mut points = Vec::new();
                for record in &records {
                    self.messages_total.fetch_add(1, Ordering::Relaxed);
                    let Some(payload) = record.value.as_ref() else {
                        continue; // tombstone
                    };
                    match self.parse_payload(&record.topic, payload) {
                        Ok(parsed) => points.extend(parsed),
                        Err(e) => {
                            self.decode_errors.fetch_add(1, Ordering::Relaxed);
                            debug!(name = %self.name, %e, "Kafka decode error");
                        }
                    }
                }

                if !points.is_empty() {
                    match crate::util::insert_from_connector(
                        &self.db,
                        self.ingest_limiter(),
                        self.namespace_scope(),
                        points,
                        INSERT_TIMEOUT,
                        &self.name,
                    )
                    .await
                    {
                        Ok(n) => self.points_total.fetch_add(n, Ordering::Relaxed),
                        Err(e) => {
                            // Poll advances local positions even when offsets are uncommitted.
                            // Rewind every partition before another batch can be committed.
                            let mut offsets = std::collections::HashMap::new();
                            for record in &records {
                                let partition =
                                    TopicPartition::new(&*record.topic, record.partition);
                                offsets.entry(partition).or_insert(record.offset);
                            }
                            consumer.seek_many(offsets).await?;
                            error!(name = %self.name, %e, "Kafka batch write failed, retrying");
                            tokio::select! {
                                () = self.cancel.notified() => return Ok(()),
                                () = tokio::time::sleep(SETUP_BACKOFF_MIN) => {}
                            }
                            continue;
                        }
                    };
                }

                if let Err(e) = consumer.commit().await {
                    warn!(name = %self.name, %e, "Kafka offset commit failed");
                }

                // How far behind the broker we are, from the watermarks the last fetch
                // response already carried — no extra round trip.
                let lag = consumer.lag().await;
                let total: u64 = lag.values().filter_map(|partition| partition.lag).sum();
                self.lag
                    .store(i64::try_from(total).unwrap_or(i64::MAX), Ordering::Relaxed);
                metrics::gauge!("chronix_kafka_consumer_lag", "connector" => self.name.clone())
                    .set(total as f64);
                let stale = lag.values().filter(|p| p.lag.is_some() && p.stale).count();
                if stale > 0 {
                    debug!(
                        name = %self.name,
                        stale,
                        "some partitions' lag is from a stale watermark"
                    );
                }
            }
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
        // Nothing has reached the broker yet; the task promotes this once
        // it joins the consumer group.
        self.set_state(ConnectorStatus::Reconnecting);

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
                self.set_state(ConnectorStatus::Failed(e.to_string()));
                return Err(e);
            }
        }

        // Direct construction is possible even in a build without the connector.
        #[cfg(not(feature = "kafka"))]
        self.set_state(ConnectorStatus::Failed(
            "built without the `kafka` feature".into(),
        ));
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
        self.set_state(ConnectorStatus::Stopped);

        info!(name = %self.name, "Kafka consumer stopped");
        Ok(())
    }

    /// What the consumer task is doing. A stop wins immediately, since it is
    /// the caller's decision and takes effect before the task notices.
    async fn status(&self) -> ConnectorStatus {
        if self.stopped.load(Ordering::SeqCst) || !self.running.load(Ordering::SeqCst) {
            return ConnectorStatus::Stopped;
        }
        self.state.lock().clone()
    }

    async fn metrics(&self) -> ConnectorMetrics {
        let points = self.points_total.load(Ordering::Relaxed);
        let lag = self.lag.load(Ordering::Relaxed);
        ConnectorMetrics {
            messages_total: self.messages_total.load(Ordering::Relaxed),
            points_total: points,
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
            lag: u64::try_from(lag).ok(),
            throughput: crate::connector::throughput(points, self.started_at),
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
        // `Running` is the one thing this must not say without a broker: the status
        // comes from the task, not from the flag `start()` sets.
        #[cfg(feature = "kafka")]
        assert_eq!(consumer.status().await, ConnectorStatus::Reconnecting);
        #[cfg(not(feature = "kafka"))]
        assert!(matches!(
            consumer.status().await,
            ConnectorStatus::Failed(ref why) if why.contains("kafka")
        ));
        assert!(!consumer.is_healthy().await);

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

    /// A connector's namespace tag is stamped even when the value is `default`:
    /// every scoped read filters on the tag, so untagged points would be
    /// unreachable through the query API.
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
