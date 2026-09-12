//! Ingestion connector lifecycle framework.
//!
//! Provides the [`IngestionConnector`] trait for implementing pluggable
//! data sources (e.g., Kafka, MQTT) and the [`ConnectorManager`] that
//! orchestrates their lifecycle (start, stop, status, hot-reload).

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use chronix::Chronix;

use crate::config::ServerConfig;
use crate::error::ServerError;

// ── Connector trait ────────────────────────────────────────────────────

/// Status of an ingestion connector.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorStatus {
    /// Not yet started.
    Stopped,
    /// Running and consuming data.
    Running,
    /// Connected but temporarily not receiving data.
    Idle,
    /// Reconnecting after a failure.
    Reconnecting,
    /// Permanently failed.
    Failed(String),
}

/// Runtime statistics for a connector.
///
/// `lag` is an `Option` because it is not a signal every source has: Kafka
/// has a broker-side high watermark to be behind, MQTT does not. It reported
/// `0` on both — a hard-coded zero on every implementation, published on
/// `/api/v1/connectors` and documented as "consumer lag", so a connector that
/// had fallen an hour behind read as perfectly caught up. `None` says "this
/// source has no such measure"; `Some(0)` says "caught up", and only one of
/// those is a claim.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConnectorMetrics {
    /// Total messages consumed / received.
    pub messages_total: u64,
    /// Total points ingested into the database.
    pub points_total: u64,
    /// Total deserialization / decode errors.
    pub decode_errors: u64,
    /// Messages behind the source's newest offset, where the source has one.
    ///
    /// `None` for a source with no such measure — MQTT, which pushes.
    pub lag: Option<u64>,
    /// Points per second since the connector started.
    pub throughput: f64,
}

/// Points per second since `started_at`.
///
/// Cumulative rather than windowed, which is what "since it started" means and
/// what an operator asking "is this connector doing anything" needs. A
/// windowed rate belongs in Prometheus, over `chronix_*_points_total`.
#[must_use]
pub fn throughput(points_total: u64, started_at: std::time::Instant) -> f64 {
    let secs = started_at.elapsed().as_secs_f64();
    if secs <= 0.0 {
        return 0.0;
    }
    points_total as f64 / secs
}

/// Information about a running connector, returned by the status API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorInfo {
    /// Connector name / ID (e.g., "kafka-main", "mqtt-sensors").
    pub name: String,
    /// Connector type (e.g., "kafka", "mqtt").
    pub connector_type: String,
    /// Current status.
    pub status: ConnectorStatus,
    /// Runtime metrics.
    pub metrics: ConnectorMetrics,
}

/// Trait for pluggable ingestion connectors.
///
/// Connectors consume data from external sources (Kafka topics, MQTT
/// brokers, etc.) and write it into the Chronix database. The
/// [`ConnectorManager`] calls lifecycle methods in the correct order.
///
/// # Implementation notes
///
/// - `start()` should spawn background tasks and return immediately.
/// - `stop()` should drain in-flight data and shut down cleanly.
/// - All methods are async and can fail with [`ServerError`].
#[async_trait::async_trait]
pub trait IngestionConnector: Send + Sync {
    /// Unique name identifying this connector instance.
    fn name(&self) -> &str;

    /// Type identifier (e.g., "kafka", "mqtt").
    fn connector_type(&self) -> &str;

    /// Start the connector. Should spawn background tasks that consume
    /// data and write to the provided database.
    async fn start(&self) -> Result<(), ServerError>;

    /// Gracefully stop the connector. Drain in-flight data, commit
    /// offsets, and release resources.
    async fn stop(&self) -> Result<(), ServerError>;

    /// Current operational status.
    async fn status(&self) -> ConnectorStatus;

    /// Runtime metrics snapshot.
    async fn metrics(&self) -> ConnectorMetrics;

    /// Whether this connector is ingesting, or could be at any moment —
    /// `Running | Idle`. A connector with nothing to read is working; one
    /// that cannot reach its broker is not.
    ///
    /// This does **not** gate `/ready`, which asks only whether the database
    /// is writable: a connector that cannot reach its broker does not stop
    /// this node answering queries. Surfaced on `/api/v1/connectors` and as
    /// the `chronix_connector_up` gauge, which is what an alert watches.
    async fn is_healthy(&self) -> bool {
        matches!(
            self.status().await,
            ConnectorStatus::Running | ConnectorStatus::Idle
        )
    }
}

// ── Connector manager ──────────────────────────────────────────────────

/// Manages the lifecycle of all ingestion connectors.
///
/// Starts / stops connectors based on server configuration and provides
/// a status API for the REST layer.
pub struct ConnectorManager {
    connectors: RwLock<Vec<Arc<dyn IngestionConnector>>>,
    db: Arc<Chronix>,
    /// Maximum retries per connector on startup (0 = fail-fast).
    max_start_retries: u32,
    /// Initial backoff duration in milliseconds (doubles each retry).
    start_retry_backoff_ms: u64,
}

impl ConnectorManager {
    /// Create a new connector manager for the given database.
    pub fn new(db: Arc<Chronix>) -> Self {
        Self {
            connectors: RwLock::new(Vec::new()),
            db,
            max_start_retries: 0,
            start_retry_backoff_ms: 1000,
        }
    }

    /// Set the maximum number of start retries per connector.
    ///
    /// `0` (default) means fail-fast — no retries. Each retry uses
    /// exponential backoff starting from `start_retry_backoff_ms`.
    pub fn with_max_start_retries(mut self, retries: u32) -> Self {
        self.max_start_retries = retries;
        self
    }

    /// Set the initial backoff in milliseconds for start retries.
    ///
    /// The backoff doubles on each subsequent retry: e.g. 1000 ms,
    /// 2000 ms, 4000 ms, …
    pub fn with_start_retry_backoff_ms(mut self, ms: u64) -> Self {
        self.start_retry_backoff_ms = ms;
        self
    }

    /// Reference to the database (for connector construction).
    pub fn db(&self) -> &Arc<Chronix> {
        &self.db
    }

    /// Register a connector. Does **not** start it.
    pub async fn register(&self, connector: Arc<dyn IngestionConnector>) {
        let mut connectors = self.connectors.write().await;
        info!(
            name = connector.name(),
            kind = connector.connector_type(),
            "connector registered"
        );
        connectors.push(connector);
    }

    /// Start all registered connectors, concurrently.
    ///
    /// When `max_start_retries > 0`, each connector's `start()` is retried
    /// with exponential backoff (`start_retry_backoff_ms`, doubling each
    /// attempt).
    ///
    /// Connectors are started in parallel because they are independent: a
    /// Kafka broker that is slow to hand out metadata, or an MQTT endpoint
    /// working through its retry backoff, would otherwise hold up every
    /// connector behind it and stall server startup for the sum of all their
    /// timeouts rather than the longest one.
    ///
    /// All connectors are attempted even if one fails; the first error is
    /// returned once they have all settled. Connectors that did start stay
    /// started — the caller is expected to shut the manager down on error.
    pub async fn start_all(&self) -> Result<(), ServerError> {
        let connectors = self.connectors.read().await;
        let results = futures::future::join_all(connectors.iter().map(|c| async move {
            info!(name = c.name(), "starting connector");
            self.start_with_retry(c.as_ref()).await
        }))
        .await;
        results.into_iter().collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }

    /// Start a single connector with retry + exponential backoff.
    async fn start_with_retry(&self, c: &dyn IngestionConnector) -> Result<(), ServerError> {
        let mut last_err: Option<ServerError> = None;
        for attempt in 0..=self.max_start_retries {
            if attempt > 0 {
                let delay_ms = self.start_retry_backoff_ms * (1u64 << (attempt - 1).min(10));
                warn!(
                    name = c.name(),
                    attempt = attempt,
                    delay_ms = delay_ms,
                    "retrying connector start"
                );
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }

            match c.start().await {
                Ok(()) => {
                    return Ok(());
                }
                Err(e) => {
                    error!(name = c.name(), %e, attempt = attempt, "failed to start connector");
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| ServerError::Internal("connector start failed".into())))
    }

    /// Gracefully stop all connectors.
    pub async fn stop_all(&self) {
        let connectors = self.connectors.read().await;
        for c in connectors.iter() {
            info!(name = c.name(), "stopping connector");
            if let Err(e) = c.stop().await {
                warn!(name = c.name(), %e, "error stopping connector");
            }
        }
    }

    /// Get status of all connectors for the REST API.
    pub async fn list_connectors(&self) -> Vec<ConnectorInfo> {
        let connectors = self.connectors.read().await;
        let mut infos = Vec::with_capacity(connectors.len());
        for c in connectors.iter() {
            infos.push(ConnectorInfo {
                name: c.name().to_string(),
                connector_type: c.connector_type().to_string(),
                status: c.status().await,
                metrics: c.metrics().await,
            });
        }
        infos
    }

    /// Publish each connector's health as `chronix_connector_up`.
    ///
    /// Called from the metrics scrape, like the engine's `statistics()`: a
    /// gauge nobody writes reads as "no data", which Prometheus renders
    /// identically to a quiet one. One gauge per connector rather than a
    /// rollup, so an alert can say *which* one is down.
    pub async fn refresh_gauges(&self) {
        let connectors = self.connectors.read().await;
        for c in connectors.iter() {
            let up = f64::from(u8::from(c.is_healthy().await));
            metrics::gauge!(
                "chronix_connector_up",
                "connector" => c.name().to_string(),
                "type" => c.connector_type().to_string(),
            )
            .set(up);
        }
    }

    /// Reload connectors from updated configuration.
    ///
    /// Stops connectors whose configuration has been removed, and
    /// registers + starts newly-configured connectors. Unchanged
    /// connectors remain running without interruption.
    pub async fn reload(&self, config: &ServerConfig) -> Result<(), ServerError> {
        let mut connectors = self.connectors.write().await;

        // Build a set of desired connector names from the new config
        let mut desired: std::collections::HashSet<String> = std::collections::HashSet::new();

        if config.kafka.is_some() {
            desired.insert("kafka-default".to_string());
        }
        if config.mqtt.is_some() {
            desired.insert("mqtt-default".to_string());
        }

        // Stop and remove connectors that are no longer configured
        let mut to_keep = Vec::new();
        for c in connectors.drain(..) {
            if desired.contains(c.name()) {
                // Already running and still desired — keep it
                desired.remove(c.name());
                to_keep.push(c);
            } else {
                // No longer desired — stop it
                info!(name = c.name(), "stopping removed connector");
                if let Err(e) = c.stop().await {
                    warn!(name = c.name(), %e, "error stopping connector during reload");
                }
            }
        }
        *connectors = to_keep;

        // Register and start newly-configured connectors (with retry)
        if desired.contains("kafka-default") {
            if let Some(ref kafka_cfg) = config.kafka {
                let consumer = crate::kafka::KafkaConsumer::new_arc(
                    "kafka-default",
                    kafka_cfg.clone(),
                    self.db.clone(),
                    config.server.multi_tenancy,
                );
                info!(name = "kafka-default", "registering new Kafka connector");
                connectors.push(consumer.clone());
                self.start_with_retry(consumer.as_ref()).await?;
            }
        }

        if desired.contains("mqtt-default") {
            if let Some(ref mqtt_cfg) = config.mqtt {
                let subscriber = crate::mqtt::MqttSubscriber::new_arc(
                    "mqtt-default",
                    mqtt_cfg.clone(),
                    self.db.clone(),
                    config.server.multi_tenancy,
                );
                info!(name = "mqtt-default", "registering new MQTT connector");
                connectors.push(subscriber.clone());
                self.start_with_retry(subscriber.as_ref()).await?;
            }
        }

        info!(count = connectors.len(), "connector reload complete");
        Ok(())
    }

    /// Number of registered connectors.
    pub async fn count(&self) -> usize {
        self.connectors.read().await.len()
    }
}

// ── Connector configuration ───────────────────────────────────────────

/// Payload deserialization format for ingestion connectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorFormat {
    /// InfluxDB line protocol.
    LineProtocol,
    /// JSON payload.
    Json,
}

/// Kafka consumer connector configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaConfig {
    /// Namespace ingested points are written to.
    ///
    /// Only meaningful with `multi_tenancy = true`; a connector has no
    /// request to carry a header, so its namespace is part of its
    /// configuration. Defaults to `"default"`.
    #[serde(default = "default_connector_namespace")]
    pub namespace: String,
    /// Comma-separated broker addresses.
    pub brokers: String,
    /// Consumer group ID.
    pub group_id: String,
    /// Topics to consume.
    pub topics: Vec<String>,
    /// Deserialization format.
    #[serde(default = "default_kafka_format")]
    pub format: ConnectorFormat,
    /// Auto offset reset: "earliest" or "latest".
    #[serde(default = "default_auto_offset_reset")]
    pub auto_offset_reset: String,
    /// Topic-to-measurement mapping (optional override).
    #[serde(default)]
    pub topic_measurement_map: HashMap<String, String>,
    /// SASL mechanism: "PLAIN", "SCRAM-SHA-256", or "SCRAM-SHA-512".
    #[serde(default)]
    pub sasl_mechanism: Option<String>,
    /// SASL username.
    #[serde(default)]
    pub sasl_username: Option<String>,
    /// SASL password. Prefer `credential_file` for rotation support.
    #[serde(default)]
    pub sasl_password: Option<String>,
    /// Security protocol: "PLAINTEXT", "SSL", "SASL_PLAINTEXT", or "SASL_SSL".
    #[serde(default)]
    pub security_protocol: Option<String>,
    /// PEM CA bundle used to verify the broker under `SSL` / `SASL_SSL`.
    ///
    /// Without it the platform trust store is used, which is right for a
    /// managed broker and wrong for the private CA most self-hosted clusters
    /// run.
    #[serde(default)]
    pub ca_cert: Option<String>,
    /// Path to a JSON credential file for hot-reload rotation.
    ///
    /// The file must contain `{"username": "...", "password": "..."}`.
    /// When the file changes on disk, the connector reloads credentials
    /// on the next reconnect cycle without requiring a full restart.
    #[serde(default)]
    pub credential_file: Option<String>,
}

/// MQTT subscriber connector configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttConfig {
    /// Namespace ingested points are written to.
    ///
    /// Only meaningful with `multi_tenancy = true`; a connector has no
    /// request to carry a header, so its namespace is part of its
    /// configuration. Defaults to `"default"`.
    #[serde(default = "default_connector_namespace")]
    pub namespace: String,
    /// Broker hostname or IP.
    pub broker: String,
    /// Broker port (default: 1883).
    #[serde(default = "default_mqtt_port")]
    pub port: u16,
    /// Client ID.
    pub client_id: String,
    /// Topics to subscribe to (supports wildcards).
    pub topics: Vec<String>,
    /// QoS level: 0, 1, or 2.
    #[serde(default = "default_mqtt_qos")]
    pub qos: u8,
    /// Payload format.
    #[serde(default = "default_mqtt_format")]
    pub format: ConnectorFormat,
    /// Optional CA cert path for TLS.
    #[serde(default)]
    pub ca_cert: Option<String>,
    /// Optional client cert path for mutual TLS.
    #[serde(default)]
    pub client_cert: Option<String>,
    /// Optional client key path for mutual TLS.
    #[serde(default)]
    pub client_key: Option<String>,
    /// MQTT username for broker authentication.
    #[serde(default)]
    pub username: Option<String>,
    /// MQTT password for broker authentication. Prefer `credential_file` for rotation.
    #[serde(default)]
    pub password: Option<String>,
    /// Path to a JSON credential file for hot-reload rotation.
    ///
    /// The file must contain `{"username": "...", "password": "..."}`.
    /// When the file changes on disk, credentials are reloaded on the
    /// next reconnect cycle without requiring a full restart.
    #[serde(default)]
    pub credential_file: Option<String>,
    /// Explicit topic → measurement name mapping.
    ///
    /// When a topic is not in this map, the last path segment is used
    /// (preserving backward-compatible behaviour). Use this to prevent
    /// measurement collisions when multiple topics share the same
    /// last segment (e.g. `sensors/a/temperature` and `sensors/b/temperature`).
    #[serde(default)]
    pub topic_measurement_map: HashMap<String, String>,
}

/// Connectors write to the default namespace unless configured otherwise.
fn default_connector_namespace() -> String {
    crate::namespace::DEFAULT_NAMESPACE.to_string()
}

fn default_kafka_format() -> ConnectorFormat {
    ConnectorFormat::LineProtocol
}

fn default_auto_offset_reset() -> String {
    "latest".to_string()
}

fn default_mqtt_port() -> u16 {
    1883
}

fn default_mqtt_qos() -> u8 {
    1
}

fn default_mqtt_format() -> ConnectorFormat {
    ConnectorFormat::Json
}

/// Credentials loaded from a JSON credential file.
///
/// Used by Kafka and MQTT connectors for hot-reload credential rotation.
/// The credential file must contain `{"username": "...", "password": "..."}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorCredentials {
    /// Authentication username.
    pub username: String,
    /// Authentication password / secret.
    pub password: String,
}

impl ConnectorCredentials {
    /// Loads credentials from a JSON file on disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed.
    pub fn load_from_file(path: &str) -> Result<Self, ServerError> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            ServerError::Internal(format!("failed to read credential file {path}: {e}"))
        })?;
        serde_json::from_str(&content).map_err(|e| {
            ServerError::Internal(format!("failed to parse credential file {path}: {e}"))
        })
    }
}

impl KafkaConfig {
    /// Returns the effective SASL credentials, preferring `credential_file`
    /// over inline `sasl_username`/`sasl_password` for rotation support.
    pub fn effective_credentials(&self) -> Result<Option<(String, String)>, ServerError> {
        if let Some(ref path) = self.credential_file {
            let creds = ConnectorCredentials::load_from_file(path)?;
            return Ok(Some((creds.username, creds.password)));
        }
        match (&self.sasl_username, &self.sasl_password) {
            (Some(u), Some(p)) => Ok(Some((resolve_secret(u)?, resolve_secret(p)?))),
            _ => Ok(None),
        }
    }
}

impl MqttConfig {
    /// Returns the effective credentials, preferring `credential_file`
    /// over inline `username`/`password` for rotation support.
    pub fn effective_credentials(&self) -> Result<Option<(String, String)>, ServerError> {
        if let Some(ref path) = self.credential_file {
            let creds = ConnectorCredentials::load_from_file(path)?;
            return Ok(Some((creds.username, creds.password)));
        }
        match (&self.username, &self.password) {
            (Some(u), Some(p)) => Ok(Some((resolve_secret(u)?, resolve_secret(p)?))),
            _ => Ok(None),
        }
    }
}

/// Resolve a `${VAR}` or `$VAR` reference in a connector credential.
///
/// The published configuration has shown `password = "$CHRONIX_MQTT_PASSWORD"`
/// since connectors existed, and nothing resolved it: the connector
/// authenticated with the literal string `$CHRONIX_MQTT_PASSWORD` and the
/// broker refused it with an error that named nothing. Keeping a broker
/// password out of the config file is the whole reason the syntax is
/// documented, so the syntax now works.
fn resolve_secret(value: &str) -> Result<String, ServerError> {
    crate::config::resolve_env_reference(value).map_err(|var| {
        ServerError::Config(crate::config::ServerConfigError::Invalid(format!(
            "connector credential references ${{{var}}}, which is not set"
        )))
    })
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    /// A mock connector for testing the manager lifecycle.
    struct MockConnector {
        name: String,
        started: AtomicBool,
        stopped: AtomicBool,
        messages: AtomicU64,
    }

    impl MockConnector {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                started: AtomicBool::new(false),
                stopped: AtomicBool::new(false),
                messages: AtomicU64::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl IngestionConnector for MockConnector {
        fn name(&self) -> &str {
            &self.name
        }

        fn connector_type(&self) -> &str {
            "mock"
        }

        async fn start(&self) -> Result<(), ServerError> {
            self.started.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn stop(&self) -> Result<(), ServerError> {
            self.stopped.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn status(&self) -> ConnectorStatus {
            if self.stopped.load(Ordering::SeqCst) {
                ConnectorStatus::Stopped
            } else if self.started.load(Ordering::SeqCst) {
                ConnectorStatus::Running
            } else {
                ConnectorStatus::Stopped
            }
        }

        async fn metrics(&self) -> ConnectorMetrics {
            ConnectorMetrics {
                messages_total: self.messages.load(Ordering::SeqCst),
                points_total: self.messages.load(Ordering::SeqCst),
                ..Default::default()
            }
        }
    }

    #[tokio::test]
    async fn connector_lifecycle_start_stop() {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = chronix_core::ChronixConfig::builder()
            .data_dir(tmp.path())
            .build()
            .unwrap();
        let db = Arc::new(Chronix::open(config).unwrap());

        let manager = ConnectorManager::new(db);
        let mock = Arc::new(MockConnector::new("test-connector"));

        // Register
        manager.register(mock.clone()).await;
        assert_eq!(manager.count().await, 1);

        // Start all
        manager.start_all().await.unwrap();
        assert!(mock.started.load(Ordering::SeqCst));

        // Status
        let infos = manager.list_connectors().await;
        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].name, "test-connector");
        assert_eq!(infos[0].connector_type, "mock");
        assert_eq!(infos[0].status, ConnectorStatus::Running);

        // Health — published as a gauge rather than rolled into a bool.
        manager.refresh_gauges().await;

        // Stop all
        manager.stop_all().await;
        assert!(mock.stopped.load(Ordering::SeqCst));

        let infos = manager.list_connectors().await;
        assert_eq!(infos[0].status, ConnectorStatus::Stopped);
    }

    #[tokio::test]
    async fn connector_manager_empty() {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = chronix_core::ChronixConfig::builder()
            .data_dir(tmp.path())
            .build()
            .unwrap();
        let db = Arc::new(Chronix::open(config).unwrap());

        let manager = ConnectorManager::new(db);

        assert_eq!(manager.count().await, 0);
        manager.refresh_gauges().await;
        assert!(manager.list_connectors().await.is_empty());

        // start/stop with no connectors should be fine
        manager.start_all().await.unwrap();
        manager.stop_all().await;
    }

    #[tokio::test]
    async fn connector_manager_multiple_connectors() {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = chronix_core::ChronixConfig::builder()
            .data_dir(tmp.path())
            .build()
            .unwrap();
        let db = Arc::new(Chronix::open(config).unwrap());

        let manager = ConnectorManager::new(db);

        let c1 = Arc::new(MockConnector::new("kafka-main"));
        let c2 = Arc::new(MockConnector::new("mqtt-sensors"));
        manager.register(c1.clone()).await;
        manager.register(c2.clone()).await;

        assert_eq!(manager.count().await, 2);

        manager.start_all().await.unwrap();
        assert!(c1.started.load(Ordering::SeqCst));
        assert!(c2.started.load(Ordering::SeqCst));

        let infos = manager.list_connectors().await;
        assert_eq!(infos.len(), 2);

        manager.stop_all().await;
        assert!(c1.stopped.load(Ordering::SeqCst));
        assert!(c2.stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn connector_is_healthy_matches_running_and_idle() {
        let mock = MockConnector::new("h");
        // Not started → Stopped → not healthy
        assert!(!mock.is_healthy().await);

        // Started → Running → healthy
        mock.start().await.unwrap();
        assert!(mock.is_healthy().await);

        // Stopped → not healthy
        mock.stop().await.unwrap();
        assert!(!mock.is_healthy().await);
    }

    #[test]
    fn kafka_config_deserialization() {
        let toml_str = r#"
            brokers = "localhost:9092"
            group_id = "chronix-ingest"
            topics = ["metrics", "events"]
            format = "json"
            auto_offset_reset = "earliest"
        "#;

        let cfg: KafkaConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.brokers, "localhost:9092");
        assert_eq!(cfg.group_id, "chronix-ingest");
        assert_eq!(cfg.topics, vec!["metrics", "events"]);
        assert_eq!(cfg.format, ConnectorFormat::Json);
        assert_eq!(cfg.auto_offset_reset, "earliest");
    }

    #[test]
    fn mqtt_config_deserialization() {
        let toml_str = r#"
            broker = "mqtt.example.com"
            port = 8883
            client_id = "chronix-sub"
            topics = ["sensors/#"]
            qos = 1
            ca_cert = "/etc/certs/ca.pem"
        "#;

        let cfg: MqttConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.broker, "mqtt.example.com");
        assert_eq!(cfg.port, 8883);
        assert_eq!(cfg.client_id, "chronix-sub");
        assert_eq!(cfg.qos, 1);
        assert_eq!(cfg.ca_cert.as_deref(), Some("/etc/certs/ca.pem"));
    }

    #[test]
    fn mqtt_config_defaults() {
        let toml_str = r#"
            broker = "localhost"
            client_id = "test"
            topics = ["t"]
        "#;

        let cfg: MqttConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.port, 1883);
        assert_eq!(cfg.qos, 1);
        assert_eq!(cfg.format, ConnectorFormat::Json);
        assert!(cfg.ca_cert.is_none());
    }

    #[test]
    fn connector_metrics_default() {
        let m = ConnectorMetrics::default();
        assert_eq!(m.messages_total, 0);
        assert_eq!(m.points_total, 0);
        assert_eq!(m.decode_errors, 0);
        assert_eq!(m.lag, None);
        assert!((m.throughput - 0.0).abs() < f64::EPSILON);
    }

    // -- Retry tests ------------------------------------------------------

    /// A connector that fails the first N `start()` calls, then succeeds.
    struct FailingConnector {
        name: String,
        remaining_failures: AtomicU64,
    }

    impl FailingConnector {
        fn new(name: &str, fail_count: u64) -> Self {
            Self {
                name: name.to_string(),
                remaining_failures: AtomicU64::new(fail_count),
            }
        }
    }

    #[async_trait::async_trait]
    impl IngestionConnector for FailingConnector {
        fn name(&self) -> &str {
            &self.name
        }
        fn connector_type(&self) -> &str {
            "failing-mock"
        }
        async fn start(&self) -> Result<(), ServerError> {
            let remaining = self.remaining_failures.load(Ordering::SeqCst);
            if remaining > 0 {
                self.remaining_failures.fetch_sub(1, Ordering::SeqCst);
                return Err(ServerError::Internal("transient start failure".into()));
            }
            Ok(())
        }
        async fn stop(&self) -> Result<(), ServerError> {
            Ok(())
        }
        async fn status(&self) -> ConnectorStatus {
            ConnectorStatus::Stopped
        }
        async fn metrics(&self) -> ConnectorMetrics {
            ConnectorMetrics::default()
        }
    }

    #[tokio::test]
    async fn start_all_fail_fast_default() {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = chronix_core::ChronixConfig::builder()
            .data_dir(tmp.path())
            .build()
            .unwrap();
        let db = Arc::new(Chronix::open(config).unwrap());

        // Default: max_start_retries = 0 → fail-fast
        let manager = ConnectorManager::new(db);
        let c = Arc::new(FailingConnector::new("fail-connector", 1));
        manager.register(c.clone()).await;

        let result = manager.start_all().await;
        assert!(result.is_err(), "should fail immediately with no retries");
    }

    #[tokio::test]
    async fn start_all_retry_succeeds_after_failures() {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = chronix_core::ChronixConfig::builder()
            .data_dir(tmp.path())
            .build()
            .unwrap();
        let db = Arc::new(Chronix::open(config).unwrap());

        // Connector fails twice, then succeeds → need at least 2 retries
        let manager = ConnectorManager::new(db)
            .with_max_start_retries(3)
            .with_start_retry_backoff_ms(10); // fast for tests
        let c = Arc::new(FailingConnector::new("flaky", 2));
        manager.register(c.clone()).await;

        let result = manager.start_all().await;
        assert!(result.is_ok(), "should succeed after retries");
        assert_eq!(c.remaining_failures.load(Ordering::SeqCst), 0);
    }

    // ── Credential rotation tests ─────────────────────

    #[test]
    fn kafka_effective_credentials_from_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("creds.json");
        std::fs::write(
            &path,
            r#"{"username":"kafka_user","password":"kafka_pass"}"#,
        )
        .unwrap();

        let config = KafkaConfig {
            namespace: "default".to_string(),
            brokers: "localhost:9092".into(),
            group_id: "grp".into(),
            topics: vec!["test".into()],
            format: ConnectorFormat::LineProtocol,
            auto_offset_reset: "latest".into(),
            topic_measurement_map: Default::default(),
            sasl_mechanism: None,
            sasl_username: Some("inline_user".into()),
            sasl_password: Some("inline_pass".into()),
            security_protocol: None,
            ca_cert: None,
            credential_file: Some(path.to_string_lossy().into_owned()),
        };
        let (user, pass) = config.effective_credentials().unwrap().unwrap();
        assert_eq!(user, "kafka_user", "credential_file should take precedence");
        assert_eq!(pass, "kafka_pass");
    }

    #[test]
    fn kafka_effective_credentials_inline_fallback() {
        let config = KafkaConfig {
            namespace: "default".to_string(),
            brokers: "localhost:9092".into(),
            group_id: "grp".into(),
            topics: vec!["test".into()],
            format: ConnectorFormat::LineProtocol,
            auto_offset_reset: "latest".into(),
            topic_measurement_map: Default::default(),
            sasl_mechanism: None,
            sasl_username: Some("inline_user".into()),
            sasl_password: Some("inline_pass".into()),
            security_protocol: None,
            ca_cert: None,
            credential_file: None,
        };
        let (user, pass) = config.effective_credentials().unwrap().unwrap();
        assert_eq!(user, "inline_user");
        assert_eq!(pass, "inline_pass");
    }

    #[test]
    fn kafka_effective_credentials_none() {
        let config = KafkaConfig {
            namespace: "default".to_string(),
            brokers: "localhost:9092".into(),
            group_id: "grp".into(),
            topics: vec!["test".into()],
            format: ConnectorFormat::LineProtocol,
            auto_offset_reset: "latest".into(),
            topic_measurement_map: Default::default(),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            security_protocol: None,
            ca_cert: None,
            credential_file: None,
        };
        assert!(config.effective_credentials().unwrap().is_none());
    }

    #[test]
    fn mqtt_effective_credentials_from_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("mqtt_creds.json");
        std::fs::write(&path, r#"{"username":"mqtt_user","password":"mqtt_pass"}"#).unwrap();

        let config = MqttConfig {
            namespace: "default".to_string(),
            broker: "localhost".into(),
            port: 1883,
            client_id: "test_client".into(),
            topics: vec!["test".into()],
            qos: 0,
            format: ConnectorFormat::LineProtocol,
            ca_cert: None,
            client_cert: None,
            client_key: None,
            username: Some("inline_user".into()),
            password: Some("inline_pass".into()),
            credential_file: Some(path.to_string_lossy().into_owned()),
            topic_measurement_map: Default::default(),
        };
        let (user, pass) = config.effective_credentials().unwrap().unwrap();
        assert_eq!(user, "mqtt_user", "credential_file should take precedence");
        assert_eq!(pass, "mqtt_pass");
    }

    #[test]
    fn connector_credentials_load_bad_path() {
        let result = ConnectorCredentials::load_from_file("/nonexistent/file.json");
        assert!(result.is_err());
    }
}
