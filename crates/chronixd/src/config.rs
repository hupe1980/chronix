//! Server configuration for chronixd.
//!
//! Extends the embedded `ChronixConfig` with server-specific settings:
//! bind addresses, TLS, gRPC, Prometheus metrics, and ingestion connectors.

use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Top-level server configuration — loaded from TOML or CLI arguments.
///
/// The `[database]` section maps directly to [`chronix_core::ChronixConfig`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// HTTP bind address (REST + Prometheus + health).
    #[serde(default = "default_http_addr")]
    pub http_addr: SocketAddr,

    /// gRPC bind address.
    #[serde(default = "default_grpc_addr")]
    pub grpc_addr: SocketAddr,

    /// Flight SQL bind address.
    #[serde(default = "default_flight_addr")]
    pub flight_addr: SocketAddr,

    /// TLS configuration (optional — plaintext if absent).
    #[serde(default)]
    pub tls: Option<TlsConfig>,

    /// Absolute base URL clients use to reach this server, e.g.
    /// `https://metrics.example.com/chronix`.
    ///
    /// Only affects the `servers` entry of the generated OpenAPI document.
    /// Leave unset unless the spec is published somewhere it cannot be
    /// resolved relative to the server itself — the default relative `/` is
    /// correct behind reverse proxies and for wildcard bind addresses, both of
    /// which an absolute URL derived from `http_addr` would get wrong.
    #[serde(default)]
    pub public_url: Option<String>,

    /// Prometheus metrics path.
    #[serde(default = "default_metrics_path")]
    pub metrics_path: String,

    /// Whether the metrics endpoint requires authentication (default: true).
    /// Set to `false` to allow unauthenticated Prometheus scraping.
    /// When `false` and auth is enabled, the metrics path is automatically
    /// added to `auth.exempt_paths`.
    #[serde(default = "default_metrics_require_auth")]
    pub metrics_require_auth: bool,

    /// Maximum HTTP request body size in bytes (default: 10 MB).
    #[serde(default = "default_max_body_size")]
    pub max_body_size: usize,

    /// Logging format: "text" or "json".
    #[serde(default = "default_log_format")]
    pub log_format: String,

    /// Log level filter (e.g., "info", "debug", "chronixd=debug,chronix=info").
    #[serde(default = "default_log_level")]
    pub log_level: String,

    /// Enable gRPC server reflection (default: false).
    ///
    /// When enabled, the gRPC reflection service is registered **without**
    /// authentication, allowing tools like `grpcurl` to introspect the API.
    /// Disable in production to avoid exposing the service schema.
    #[serde(default)]
    pub grpc_reflection: bool,

    /// gRPC keepalive interval in seconds (0 = disabled).
    #[serde(default = "default_grpc_keepalive_secs")]
    pub grpc_keepalive_secs: u64,

    /// gRPC keepalive timeout in seconds.
    #[serde(default = "default_grpc_keepalive_timeout_secs")]
    pub grpc_keepalive_timeout_secs: u64,

    /// Server-side batch size for `StreamWrite` RPC (default: 10 000 points).
    #[serde(default = "default_stream_batch_size")]
    pub stream_batch_size: usize,

    /// Server-side batch interval in milliseconds for `StreamWrite` RPC (default: 100 ms).
    #[serde(default = "default_stream_batch_interval_ms")]
    pub stream_batch_interval_ms: u64,

    /// Deduplication window in seconds for `StreamWrite` RPC (0 = disabled, default: 300 s / 5 min).
    #[serde(default = "default_dedup_window_secs")]
    pub dedup_window_secs: u64,

    /// Maximum number of dedup-cache entries before oldest-quarter eviction kicks in
    /// (default: 1 000 000).  Each entry costs ~24 bytes (u64 hash + Instant).
    #[serde(default = "default_max_dedup_entries")]
    pub max_dedup_entries: usize,

    /// Startup attempts per ingestion connector before the server gives up.
    ///
    /// `0` (default) is fail-fast: a connector that cannot reach its broker
    /// stops the server rather than leaving it running with a dead ingestion
    /// path. Raise it when connectors and brokers start concurrently.
    #[serde(default)]
    pub connector_start_retries: u32,

    /// Initial backoff between connector start attempts, milliseconds.
    /// Doubles on each retry.
    #[serde(default = "default_connector_backoff_ms")]
    pub connector_start_retry_backoff_ms: u64,

    /// Kafka ingestion connector configuration (optional).
    #[serde(default)]
    pub kafka: Option<crate::connector::KafkaConfig>,

    /// MQTT ingestion connector configuration (optional).
    #[serde(default)]
    pub mqtt: Option<crate::connector::MqttConfig>,

    /// Authentication configuration (optional — unauthenticated if absent).
    #[serde(default)]
    pub auth: Option<AuthConfig>,

    /// Authorization — path to a directory of `.cedar` policy files.
    /// When set, the Cedar authorization engine is enabled.
    /// When absent, all requests are permitted (open mode).
    #[serde(default)]
    pub authz_policy_dir: Option<PathBuf>,

    /// Enable multi-tenant namespace isolation.
    ///
    /// When `true`, every ingested point is stamped with the namespace of the
    /// request that wrote it (`X-Namespace`, or `x-namespace` metadata for
    /// gRPC and Flight SQL), and every read — REST, SQL, PromQL, Flight SQL,
    /// gRPC, Prometheus remote read — is confined to that namespace.
    ///
    /// **Decide before ingesting.** Points written with tenancy off carry no
    /// namespace and are invisible to a scoped read; points written with it on
    /// are not addressable with it off. Switching a populated server is a
    /// re-ingest, not a config change.
    #[serde(default)]
    pub multi_tenancy: bool,

    /// Cluster configuration (optional — standalone if absent).
    #[serde(default)]
    pub cluster: Option<ClusterConfig>,

    /// CORS allowed origins. Empty or absent means restrictive (same-origin only).
    /// Use `["*"]` to allow all origins — requires `allow_unsafe_cors: true`.
    ///
    /// Wildcard CORS is rejected at startup unless explicitly opted-in
    /// via `allow_unsafe_cors`, preventing accidental misconfiguration.
    #[serde(default)]
    pub cors_allowed_origins: Vec<String>,

    /// Set to `true` to allow wildcard (`"*"`) CORS origins.
    /// This is a safety switch — without it, configuring `cors_allowed_origins: ["*"]`
    /// causes a startup error.
    #[serde(default)]
    pub allow_unsafe_cors: bool,

    /// SQL query timeout in seconds (default: 30). 0 = no timeout.
    #[serde(default = "default_sql_query_timeout_secs")]
    pub sql_query_timeout_secs: u64,

    /// PromQL query timeout in seconds (default: 30). 0 = no timeout.
    #[serde(default = "default_prom_query_timeout_secs")]
    pub prom_query_timeout_secs: u64,

    /// Maximum number of rows returned by a single SQL query (default: 100 000).
    #[serde(default = "default_sql_max_rows")]
    pub sql_max_rows: usize,

    /// Maximum number of points accepted in a single write batch (default: 50 000).
    #[serde(default = "default_max_write_batch_size")]
    pub max_write_batch_size: usize,

    /// Write operation timeout in seconds (default: 30). 0 = no timeout.
    ///
    /// All write paths (`insert_batch`) are wrapped with this timeout
    /// so that a stuck downstream operation cannot exhaust the thread pool.
    #[serde(default = "default_write_timeout_secs")]
    pub write_timeout_secs: u64,

    /// Maximum evaluation points for a PromQL range query (default: 11 000).
    ///
    /// This value is **configurable** via the config file or
    /// environment variable `CHRONIXD_MAX_RANGE_QUERY_POINTS`. The default
    /// (11 000) matches the Prometheus server default. Set a lower value
    /// for memory-constrained deployments.
    #[serde(default = "default_max_range_query_points")]
    pub max_range_query_points: u64,

    /// Maximum number of label-sets returned by `/api/v1/prom/series` (default: 10 000).
    #[serde(default = "default_prom_series_limit")]
    pub prom_series_limit: usize,

    /// Byte budget for a PromQL range query's accumulated result
    /// (default: 256 MiB, 0 = unlimited).
    ///
    /// `max_range_query_points` and `prom_series_limit` bound the two
    /// dimensions separately, and their product is not a bound anybody would
    /// choose: 11 000 points × 10 000 series × 16 bytes is ~1.8 GB. The
    /// evaluator has always checked this budget — it was simply never set by
    /// any caller, so it read `0` and meant "unlimited" (R3).
    #[serde(default = "default_prom_max_result_bytes")]
    pub prom_max_result_bytes: usize,

    /// Graceful shutdown drain timeout in seconds (default: 30).
    ///
    /// All servers (HTTP, gRPC, Flight SQL) use this timeout to drain
    /// in-flight requests before forcefully terminating. Must be less
    /// than the Kubernetes `terminationGracePeriodSeconds` (default 30s)
    /// to avoid SIGKILL while writes are still in progress.
    #[serde(default = "default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,

    /// Global rate limit: maximum requests per second (default: 0 = unlimited).
    /// When set, an HTTP 429 is returned once the burst is exhausted.
    #[serde(default)]
    pub rate_limit_rps: u64,

    /// Burst size for rate limiting (default: equals `rate_limit_rps`).
    /// Allows short bursts beyond the steady-state rate.
    #[serde(default)]
    pub rate_limit_burst: u32,

    /// Per-user rate limit — maximum requests per second per
    /// authenticated principal (default: 0 = disabled).
    ///
    /// When set, each authenticated user gets an independent token
    /// bucket so that heavy users cannot crowd out others.
    #[serde(default)]
    pub per_user_rate_limit_rps: u64,

    /// Per-user rate limit burst size (default: equals `per_user_rate_limit_rps`).
    #[serde(default)]
    pub per_user_rate_limit_burst: u32,

    /// Embedded database configuration.
    #[serde(default)]
    pub database: DatabaseConfig,
}

/// Database-specific configuration — mirrors `ChronixConfig` fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Data directory.
    pub data_dir: PathBuf,

    /// WAL fsync policy: "per_write", "per_batch", or "`periodic_<ms>`".
    #[serde(default = "default_wal_fsync")]
    pub wal_fsync_policy: String,

    /// WAL max file size in bytes.
    #[serde(default = "default_wal_max_size")]
    pub wal_max_file_size: usize,

    /// Memtable flush threshold in bytes.
    #[serde(default = "default_memtable_flush")]
    pub memtable_flush_threshold: usize,

    /// Max total memtable memory in bytes.
    #[serde(default = "default_max_memtable_memory")]
    pub max_memtable_memory: usize,

    /// Shard duration in seconds.
    #[serde(default = "default_shard_duration_secs")]
    pub shard_duration_secs: u64,

    /// Retention period in seconds (0 = 30 days default).
    #[serde(default)]
    pub retention_secs: u64,

    /// Zstd compression level (1–22). Only used when `compression = "zstd"`.
    #[serde(default = "default_zstd_level")]
    pub zstd_level: i32,

    /// Train a Zstd dictionary per segment. Only used when
    /// `compression = "zstd"`.
    #[serde(default)]
    pub zstd_dict_training: bool,

    /// Compression codec: "lz4", "zstd", or "none".
    #[serde(default = "default_compression")]
    pub compression: String,

    /// Float encoding: "chimp", "gorilla", or "plain".
    #[serde(default = "default_float_encoding")]
    pub float_encoding: String,

    /// Segment cache size in bytes.
    #[serde(default = "default_segment_cache_size")]
    pub segment_cache_size: usize,

    /// Enable last-value cache.
    #[serde(default)]
    pub enable_last_value_cache: bool,

    /// Compaction concurrency.
    #[serde(default = "default_compaction_concurrency")]
    pub compaction_concurrency: usize,

    /// Max series cardinality.
    #[serde(default = "default_max_cardinality")]
    pub max_series_cardinality: usize,
}

/// TLS configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Path to PEM-encoded certificate file.
    pub cert: PathBuf,
    /// Path to PEM-encoded private key file.
    pub key: PathBuf,
    /// Optional path to PEM-encoded CA certificate for client verification
    /// (mutual TLS).  When set, the server will require connecting clients
    /// to present a certificate signed by this CA.
    #[serde(default)]
    pub client_ca: Option<PathBuf>,
    /// Interval in seconds to check for certificate/key file changes and
    /// hot-reload TLS configuration (0 = disabled, default: 0).
    ///
    /// When enabled, the server monitors cert and key file modification
    /// times and atomically swaps the TLS configuration on change —
    /// zero-downtime certificate rotation.
    #[serde(default)]
    pub reload_interval_secs: u64,
}

/// Authentication configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// API key authentication — if present, API-key auth is enabled.
    #[serde(default)]
    pub api_keys: Vec<ApiKeyEntry>,

    /// JWT/OIDC authentication configuration.
    #[serde(default)]
    pub jwt: Option<JwtAuthConfig>,

    /// Paths exempt from authentication (e.g., "/health", "/ready", "/metrics").
    #[serde(default = "default_exempt_paths")]
    pub exempt_paths: Vec<String>,
}

/// A pre-configured API key entry.
///
/// **Security:** `Debug` output redacts the raw key to prevent credential
/// leakage in log output or config dumps.
#[derive(Clone, Serialize, Deserialize)]
pub struct ApiKeyEntry {
    /// Human-readable name for this key.
    pub name: String,
    /// The raw API key value (will be hashed on load).
    pub key: String,
}

impl std::fmt::Debug for ApiKeyEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiKeyEntry")
            .field("name", &self.name)
            .field("key", &"[REDACTED]")
            .finish()
    }
}

/// JWT authentication configuration.
///
/// **Security:** `Debug` output redacts the secret to prevent credential
/// leakage in log output or config dumps.
#[derive(Clone, Serialize, Deserialize)]
pub struct JwtAuthConfig {
    /// Secret or PEM public key for validation.
    pub secret: String,
    /// Algorithm: "HS256", "RS256", etc.
    #[serde(default = "default_jwt_algorithm")]
    pub algorithm: String,
    /// Expected issuer (optional).
    #[serde(default)]
    pub issuer: Option<String>,
    /// Expected audience (optional).
    #[serde(default)]
    pub audience: Option<String>,
    /// Claim name or dot-separated path for role extraction.
    ///
    /// Defaults to `"roles"`. Use dot notation for nested claims,
    /// e.g. `"realm_access.roles"` (Keycloak) or
    /// `"https://example.com/roles"` (Auth0 custom namespace).
    #[serde(default)]
    pub role_claim: Option<String>,
}

impl std::fmt::Debug for JwtAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtAuthConfig")
            .field("secret", &"[REDACTED]")
            .field("algorithm", &self.algorithm)
            .field("issuer", &self.issuer)
            .field("audience", &self.audience)
            .field("role_claim", &self.role_claim)
            .finish()
    }
}

fn default_exempt_paths() -> Vec<String> {
    vec![
        "/health".to_string(),
        "/healthz".to_string(),
        "/ready".to_string(),
        "/readyz".to_string(),
        // `/metrics` deliberately omitted — operational metrics
        // require authentication by default. Set `metrics_require_auth = false`
        // or add "/metrics" to `auth.exempt_paths` to allow unauthenticated
        // Prometheus scraping.
    ]
}

fn default_jwt_algorithm() -> String {
    "HS256".to_string()
}

/// Cluster operating mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClusterMode {
    /// MetaNode — runs Raft consensus for cluster metadata.
    Meta,
    /// DataNode — hosts region data, registers with MetaNodes.
    Data,
}

/// Cluster configuration — present only when running in cluster mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Operating mode of this node.
    pub mode: ClusterMode,

    /// Unique numeric identifier for this node within the cluster.
    pub node_id: u64,

    /// Bind address for the Raft/admin gRPC service (meta mode only).
    #[serde(default)]
    pub raft_bind_addr: String,

    /// MetaNode peer addresses for cluster bootstrap (meta mode).
    /// Format: `["host1:port", "host2:port"]`
    #[serde(default)]
    pub cluster_peers: Vec<String>,

    /// MetaNode addresses for DataNode registration and heartbeat (data mode).
    /// Format: `["host1:port", "host2:port"]`
    #[serde(default)]
    pub meta_addrs: Vec<String>,

    /// Optional mTLS configuration for inter-node communication.
    /// When present, all inter-node gRPC traffic uses mTLS.
    #[serde(default)]
    pub tls: Option<ClusterTlsConfig>,
}

/// mTLS configuration for inter-node cluster communication.
///
/// All fields must point to PEM-encoded files. The CA certificate is used
/// to verify peer certificates, and the client cert/key pair is presented
/// during TLS handshake for mutual authentication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterTlsConfig {
    /// Path to the PEM-encoded CA certificate used to verify peer certs.
    pub ca_cert: PathBuf,
    /// Path to this node's PEM-encoded certificate.
    pub cert: PathBuf,
    /// Path to this node's PEM-encoded private key.
    pub key: PathBuf,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_addr: default_http_addr(),
            grpc_addr: default_grpc_addr(),
            flight_addr: default_flight_addr(),
            multi_tenancy: false,
            connector_start_retries: 0,
            connector_start_retry_backoff_ms: default_connector_backoff_ms(),
            tls: None,
            public_url: None,
            metrics_path: default_metrics_path(),
            metrics_require_auth: default_metrics_require_auth(),
            max_body_size: default_max_body_size(),
            log_format: default_log_format(),
            log_level: default_log_level(),
            grpc_keepalive_secs: default_grpc_keepalive_secs(),
            grpc_keepalive_timeout_secs: default_grpc_keepalive_timeout_secs(),
            grpc_reflection: false,
            stream_batch_size: default_stream_batch_size(),
            stream_batch_interval_ms: default_stream_batch_interval_ms(),
            dedup_window_secs: default_dedup_window_secs(),
            max_dedup_entries: default_max_dedup_entries(),
            kafka: None,
            mqtt: None,
            auth: None,
            authz_policy_dir: None,
            cluster: None,
            cors_allowed_origins: Vec::new(),
            allow_unsafe_cors: false,
            sql_query_timeout_secs: default_sql_query_timeout_secs(),
            prom_query_timeout_secs: default_prom_query_timeout_secs(),
            sql_max_rows: default_sql_max_rows(),
            max_write_batch_size: default_max_write_batch_size(),
            max_range_query_points: default_max_range_query_points(),
            prom_series_limit: default_prom_series_limit(),
            prom_max_result_bytes: default_prom_max_result_bytes(),
            rate_limit_rps: 0,
            rate_limit_burst: 0,
            per_user_rate_limit_rps: 0,
            per_user_rate_limit_burst: 0,
            database: DatabaseConfig::default(),
            write_timeout_secs: default_write_timeout_secs(),
            shutdown_timeout_secs: default_shutdown_timeout_secs(),
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./chronix-data"),
            wal_fsync_policy: default_wal_fsync(),
            zstd_level: default_zstd_level(),
            zstd_dict_training: false,
            wal_max_file_size: default_wal_max_size(),
            memtable_flush_threshold: default_memtable_flush(),
            max_memtable_memory: default_max_memtable_memory(),
            shard_duration_secs: default_shard_duration_secs(),
            retention_secs: 0,
            compression: default_compression(),
            float_encoding: default_float_encoding(),
            segment_cache_size: default_segment_cache_size(),
            enable_last_value_cache: false,
            compaction_concurrency: default_compaction_concurrency(),
            max_series_cardinality: default_max_cardinality(),
        }
    }
}

impl ServerConfig {
    /// Validate that HTTP, gRPC, and Flight SQL ports are distinct.
    ///
    /// Returns an error if any two server endpoints are configured to
    /// bind to the same `(ip, port)` pair, which would cause a startup
    /// failure with an opaque "address already in use" error.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::Invalid`] describing the conflicting ports.
    pub fn validate_ports(&self) -> Result<(), ServerConfigError> {
        if self.http_addr == self.grpc_addr {
            return Err(ServerConfigError::Invalid(format!(
                "HTTP and gRPC addresses must be distinct, both set to {}",
                self.http_addr,
            )));
        }
        if self.http_addr == self.flight_addr {
            return Err(ServerConfigError::Invalid(format!(
                "HTTP and Flight SQL addresses must be distinct, both set to {}",
                self.http_addr,
            )));
        }
        if self.grpc_addr == self.flight_addr {
            return Err(ServerConfigError::Invalid(format!(
                "gRPC and Flight SQL addresses must be distinct, both set to {}",
                self.grpc_addr,
            )));
        }
        Ok(())
    }

    /// Validate CORS configuration.
    ///
    /// Rejects wildcard CORS (`["*"]`) unless `allow_unsafe_cors` is
    /// explicitly set to `true`, preventing accidental misconfiguration.
    pub fn validate_cors(&self) -> Result<(), ServerConfigError> {
        if self.cors_allowed_origins.len() == 1
            && self.cors_allowed_origins[0] == "*"
            && !self.allow_unsafe_cors
        {
            return Err(ServerConfigError::Invalid(
                "CORS wildcard origin ('*') requires `allow_unsafe_cors = true` in config"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Load configuration from a TOML file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed.
    pub fn from_toml(path: &std::path::Path) -> Result<Self, ServerConfigError> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| ServerConfigError::Io(path.display().to_string(), e))?;
        toml::from_str(&content)
            .map_err(|e| ServerConfigError::Parse(path.display().to_string(), e.to_string()))
    }

    /// Convert to a [`chronix_core::ChronixConfig`] for opening the database.
    ///
    /// # Errors
    ///
    /// Returns an error if the database configuration is invalid.
    pub fn to_chronix_config(&self) -> Result<chronix_core::ChronixConfig, ServerConfigError> {
        use chronix_core::{ChronixConfig, CompressionCodec, FloatEncoding, FsyncPolicy};
        use std::time::Duration;

        let fsync_policy = match self.database.wal_fsync_policy.as_str() {
            "per_write" => FsyncPolicy::PerWrite,
            "per_batch" => FsyncPolicy::PerBatch,
            s if s.starts_with("periodic_") => {
                let ms: u64 = s
                    .strip_prefix("periodic_")
                    .expect("starts_with guard ensures prefix exists")
                    .parse()
                    .map_err(|_| {
                        ServerConfigError::Invalid(format!("invalid periodic fsync interval: {s}"))
                    })?;
                FsyncPolicy::Periodic(Duration::from_millis(ms))
            }
            other => {
                return Err(ServerConfigError::Invalid(format!(
                    "unknown wal_fsync_policy: {other}"
                )));
            }
        };

        let compression = match self.database.compression.as_str() {
            "lz4" => CompressionCodec::Lz4,
            "zstd" => CompressionCodec::Zstd,
            "none" => CompressionCodec::None,
            other => {
                return Err(ServerConfigError::Invalid(format!(
                    "unknown compression codec: {other}"
                )));
            }
        };

        let float_encoding = match self.database.float_encoding.as_str() {
            "chimp" => FloatEncoding::Chimp,
            "gorilla" => FloatEncoding::Gorilla,
            "plain" => FloatEncoding::Plain,
            other => {
                return Err(ServerConfigError::Invalid(format!(
                    "unknown float encoding: {other}"
                )));
            }
        };

        let retention = if self.database.retention_secs > 0 {
            Some(Duration::from_secs(self.database.retention_secs))
        } else {
            None
        };

        let mut builder = ChronixConfig::builder()
            .data_dir(&self.database.data_dir)
            .wal_fsync_policy(fsync_policy)
            .wal_max_file_size(self.database.wal_max_file_size)
            .memtable_flush_threshold(self.database.memtable_flush_threshold)
            .max_memtable_memory(self.database.max_memtable_memory)
            .shard_duration(Duration::from_secs(self.database.shard_duration_secs))
            .compression(compression)
            .zstd_level(self.database.zstd_level)
            .zstd_dict_training(self.database.zstd_dict_training)
            .float_encoding(float_encoding)
            .segment_cache_size(self.database.segment_cache_size)
            .enable_last_value_cache(self.database.enable_last_value_cache)
            .compaction_concurrency(self.database.compaction_concurrency)
            .max_series_cardinality(self.database.max_series_cardinality);

        if let Some(retention) = retention {
            builder = builder.retention(Some(retention));
        }

        builder
            .build()
            .map_err(|e| ServerConfigError::Invalid(e.to_string()))
    }
}

/// Configuration errors.
#[derive(Debug, thiserror::Error)]
pub enum ServerConfigError {
    /// I/O error reading config file.
    #[error("failed to read config file '{0}': {1}")]
    Io(String, #[source] std::io::Error),
    /// TOML parse error.
    #[error("failed to parse config file '{0}': {1}")]
    Parse(String, String),
    /// Invalid configuration value.
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

// ── Defaults ───────────────────────────────────────────────────────────

fn default_http_addr() -> SocketAddr {
    "0.0.0.0:8086".parse().expect("valid address literal")
}

fn default_grpc_addr() -> SocketAddr {
    "0.0.0.0:8087".parse().expect("valid address literal")
}

fn default_flight_addr() -> SocketAddr {
    "0.0.0.0:8817".parse().expect("valid address literal")
}

fn default_metrics_path() -> String {
    "/metrics".to_string()
}

fn default_metrics_require_auth() -> bool {
    true
}

fn default_max_body_size() -> usize {
    10 * 1024 * 1024 // 10 MB
}

fn default_sql_query_timeout_secs() -> u64 {
    30 // 30 seconds
}

fn default_prom_query_timeout_secs() -> u64 {
    30 // 30 seconds
}

fn default_sql_max_rows() -> usize {
    100_000 // 100k rows
}

fn default_max_write_batch_size() -> usize {
    50_000 // 50k points per batch
}

fn default_write_timeout_secs() -> u64 {
    30 // 30 seconds — matches SQL query timeout default
}

fn default_shutdown_timeout_secs() -> u64 {
    30 // 30 seconds — should be ≤ Kubernetes terminationGracePeriodSeconds
}

fn default_max_range_query_points() -> u64 {
    11_000 // matches Prometheus default
}

fn default_prom_series_limit() -> usize {
    10_000 // sane default for /series cardinality cap
}

fn default_prom_max_result_bytes() -> usize {
    256 * 1024 * 1024 // matches ChronixConfig::max_query_result_bytes
}

fn default_log_format() -> String {
    "text".to_string()
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_grpc_keepalive_secs() -> u64 {
    60 // 60 seconds
}

fn default_grpc_keepalive_timeout_secs() -> u64 {
    20 // 20 seconds
}

fn default_stream_batch_size() -> usize {
    10_000 // 10 000 points
}

fn default_stream_batch_interval_ms() -> u64 {
    100 // 100 ms
}

fn default_dedup_window_secs() -> u64 {
    300 // 5 minutes
}

fn default_max_dedup_entries() -> usize {
    1_000_000
}

/// Default Zstd compression level.
const fn default_zstd_level() -> i32 {
    3
}

/// Default initial backoff between connector start attempts.
const fn default_connector_backoff_ms() -> u64 {
    1000
}

fn default_wal_fsync() -> String {
    "per_batch".to_string()
}

fn default_wal_max_size() -> usize {
    32 * 1024 * 1024 // 32 MB
}

fn default_memtable_flush() -> usize {
    64 * 1024 * 1024 // 64 MB
}

fn default_max_memtable_memory() -> usize {
    256 * 1024 * 1024 // 256 MB
}

fn default_shard_duration_secs() -> u64 {
    3600 // 1 hour
}

fn default_compression() -> String {
    "lz4".to_string()
}

fn default_float_encoding() -> String {
    "chimp".to_string()
}

fn default_segment_cache_size() -> usize {
    512 * 1024 * 1024 // 512 MB
}

fn default_compaction_concurrency() -> usize {
    2
}

fn default_max_cardinality() -> usize {
    1_000_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let config = ServerConfig::default();
        assert_eq!(config.http_addr.port(), 8086);
        assert_eq!(config.grpc_addr.port(), 8087);
        assert_eq!(config.flight_addr.port(), 8817);
        assert!(config.tls.is_none());
        assert_eq!(config.max_body_size, 10 * 1024 * 1024);
        assert_eq!(config.log_format, "text");
    }

    #[test]
    fn parse_toml_config() {
        let toml_str = r#"
            http_addr = "127.0.0.1:9086"
            grpc_addr = "127.0.0.1:9087"
            flight_addr = "127.0.0.1:9817"
            log_format = "json"
            log_level = "debug"
            max_body_size = 5242880

            [database]
            data_dir = "/var/lib/chronix"
            compression = "zstd"
            float_encoding = "gorilla"
            retention_secs = 86400
            enable_last_value_cache = true
        "#;

        let config: ServerConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.http_addr.port(), 9086);
        assert_eq!(config.grpc_addr.port(), 9087);
        assert_eq!(config.flight_addr.port(), 9817);
        assert_eq!(config.log_format, "json");
        assert_eq!(config.log_level, "debug");
        assert_eq!(config.max_body_size, 5 * 1024 * 1024);
        assert_eq!(config.database.data_dir, PathBuf::from("/var/lib/chronix"));
        assert_eq!(config.database.compression, "zstd");
        assert_eq!(config.database.retention_secs, 86400);
        assert!(config.database.enable_last_value_cache);
    }

    #[test]
    fn parse_toml_with_tls() {
        let toml_str = r#"
            [tls]
            cert = "/etc/chronix/server.crt"
            key = "/etc/chronix/server.key"

            [database]
            data_dir = "/tmp/db"
        "#;

        let config: ServerConfig = toml::from_str(toml_str).unwrap();
        let tls = config.tls.as_ref().unwrap();
        assert_eq!(tls.cert, PathBuf::from("/etc/chronix/server.crt"));
        assert_eq!(tls.key, PathBuf::from("/etc/chronix/server.key"));
    }

    #[test]
    fn to_chronix_config_happy_path() {
        let toml_str = r#"
            [database]
            data_dir = "/tmp/test-db"
            compression = "lz4"
            float_encoding = "chimp"
            retention_secs = 7200
        "#;

        let config: ServerConfig = toml::from_str(toml_str).unwrap();
        let cc = config.to_chronix_config().unwrap();
        assert_eq!(cc.data_dir.display().to_string(), "/tmp/test-db");
    }

    #[test]
    fn invalid_compression_codec() {
        let toml_str = r#"
            [database]
            data_dir = "/tmp/db"
            compression = "brotli"
        "#;

        let config: ServerConfig = toml::from_str(toml_str).unwrap();
        let err = config.to_chronix_config().unwrap_err();
        assert!(err.to_string().contains("brotli"), "{err}");
    }

    #[test]
    fn invalid_float_encoding() {
        let toml_str = r#"
            [database]
            data_dir = "/tmp/db"
            float_encoding = "elf"
        "#;

        let config: ServerConfig = toml::from_str(toml_str).unwrap();
        let err = config.to_chronix_config().unwrap_err();
        assert!(err.to_string().contains("elf"), "{err}");
    }

    #[test]
    fn invalid_fsync_policy() {
        let toml_str = r#"
            [database]
            data_dir = "/tmp/db"
            wal_fsync_policy = "never"
        "#;

        let config: ServerConfig = toml::from_str(toml_str).unwrap();
        let err = config.to_chronix_config().unwrap_err();
        assert!(err.to_string().contains("never"), "{err}");
    }

    #[test]
    fn periodic_fsync_policy() {
        let toml_str = r#"
            [database]
            data_dir = "/tmp/db"
            wal_fsync_policy = "periodic_500"
        "#;

        let config: ServerConfig = toml::from_str(toml_str).unwrap();
        let cc = config.to_chronix_config().unwrap();
        // Just verify it doesn't error — the policy is valid.
        assert_eq!(cc.data_dir.display().to_string(), "/tmp/db");
    }

    #[test]
    fn validate_ports_default_ok() {
        let config = ServerConfig::default();
        config.validate_ports().unwrap();
    }

    #[test]
    fn validate_ports_http_grpc_conflict() {
        let mut config = ServerConfig::default();
        config.grpc_addr = config.http_addr; // same as HTTP
        let err = config.validate_ports().unwrap_err();
        assert!(err.to_string().contains("HTTP and gRPC"), "{err}");
    }

    #[test]
    fn validate_ports_http_flight_conflict() {
        let mut config = ServerConfig::default();
        config.flight_addr = config.http_addr;
        let err = config.validate_ports().unwrap_err();
        assert!(err.to_string().contains("HTTP and Flight SQL"), "{err}");
    }

    #[test]
    fn validate_ports_grpc_flight_conflict() {
        let mut config = ServerConfig::default();
        config.flight_addr = config.grpc_addr;
        let err = config.validate_ports().unwrap_err();
        assert!(err.to_string().contains("gRPC and Flight SQL"), "{err}");
    }
}
