//! Database configuration types with builder pattern.
//!
//! All configuration is loadable from TOML files, environment variables, and
//! programmatic builders. The builder validates constraints before construction.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::ConfigError;

/// WAL fsync policy — controls the durability vs. performance trade-off.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub enum FsyncPolicy {
    /// Call `fsync()` after every single write. Maximum durability, lowest throughput.
    PerWrite,
    /// Call `fsync()` after each batch commit. Balances durability and throughput (default).
    #[default]
    PerBatch,
    /// Call `fsync()` periodically at the given interval. Highest throughput, risk of data loss
    /// on crash up to one interval.
    Periodic(Duration),
}

impl fmt::Display for FsyncPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PerWrite => write!(f, "per-write"),
            Self::PerBatch => write!(f, "per-batch"),
            Self::Periodic(d) => write!(f, "periodic({d:?})"),
        }
    }
}

/// Compression codec for segment files.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionCodec {
    /// LZ4 — fast compression, moderate ratio (default).
    #[default]
    Lz4,
    /// Zstandard — slower compression, higher ratio.
    Zstd,
    /// No compression.
    None,
}

impl fmt::Display for CompressionCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lz4 => write!(f, "lz4"),
            Self::Zstd => write!(f, "zstd"),
            Self::None => write!(f, "none"),
        }
    }
}

/// Float encoding algorithm.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum FloatEncoding {
    /// Chimp — best compression ratio for time-series floats (default).
    #[default]
    Chimp,
    /// Gorilla (Facebook) — good compression, wide compatibility.
    Gorilla,
    /// Plain IEEE 754 — no compression.
    Plain,
}

impl fmt::Display for FloatEncoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Chimp => write!(f, "chimp"),
            Self::Gorilla => write!(f, "gorilla"),
            Self::Plain => write!(f, "plain"),
        }
    }
}

/// Storage backend configuration.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub enum StorageBackendConfig {
    /// Local filesystem (default for embedded and dev).
    #[default]
    LocalFs,
    /// Amazon S3.
    S3 {
        /// S3 bucket name.
        bucket: String,
        /// AWS region.
        region: String,
        /// Custom endpoint (for MinIO/localstack).
        endpoint: Option<String>,
    },
}

/// WAL configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalConfig {
    /// Fsync policy: `PerWrite`, `PerBatch`, or `Periodic(Duration)`.
    pub fsync_policy: FsyncPolicy,
    /// Maximum WAL file size before rotation (default: 32 MB).
    pub max_file_size: usize,
    /// Maximum number of unflushed WAL files before backpressure (default: 4).
    pub max_unflushed_wals: usize,
    /// Enable LZ4 compression for WAL payloads (default: true).
    /// Reduces WAL size significantly with minimal CPU cost.
    #[serde(default = "default_compress_wal")]
    pub compress: bool,
    /// Group-commit sync timeout in seconds (default: 30).
    /// Controls how long a waiter blocks before promoting itself to sync leader.
    #[serde(default = "default_group_sync_timeout_secs")]
    pub group_sync_timeout_secs: u64,
}

fn default_compress_wal() -> bool {
    true
}

fn default_group_sync_timeout_secs() -> u64 {
    30
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            fsync_policy: FsyncPolicy::default(),
            max_file_size: 32 * 1024 * 1024, // 32 MB
            max_unflushed_wals: 4,
            compress: true,
            group_sync_timeout_secs: 30,
        }
    }
}

// ─── Analytics Configuration ────────────────────────────────────────

/// Analytics engine configuration (forecasting, anomaly detection, preprocessing).
///
/// Configurable via TOML under the `[analytics]` section.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnalyticsConfig {
    /// Default forecast model name (one of: ses, holt, `holt_winters`, arima, sarima, `linear_regression`).
    #[serde(default = "default_forecast_model")]
    pub default_forecast_model: String,
    /// Default anomaly detection method (one of: zscore, `modified_zscore`, iqr, `forecast_residual`, `moving_average`, `dynamic_threshold`).
    #[serde(default = "default_anomaly_method")]
    pub default_anomaly_method: String,
    /// Default confidence level for prediction intervals.
    #[serde(default = "default_confidence")]
    pub default_confidence_level: f64,
    /// Maximum forecast horizon in data points.
    #[serde(default = "default_max_horizon")]
    pub max_forecast_horizon: usize,
    /// Maximum number of training points fed to a model.
    #[serde(default = "default_max_training")]
    pub max_training_points: usize,
    /// Default anomaly detection threshold (standard deviations).
    #[serde(default = "default_anomaly_threshold")]
    pub default_anomaly_threshold: f64,
    /// CPU compute thread count (0 = number of logical CPUs).
    #[serde(default)]
    pub compute_threads: usize,
}

fn default_forecast_model() -> String {
    "ses".into()
}
fn default_anomaly_method() -> String {
    "zscore".into()
}
fn default_confidence() -> f64 {
    0.95
}
fn default_max_horizon() -> usize {
    8760
}
fn default_max_training() -> usize {
    1_000_000
}
fn default_anomaly_threshold() -> f64 {
    3.0
}
fn default_max_query_result_bytes() -> usize {
    256 * 1024 * 1024 // 256 MB
}
fn default_per_query_memory_limit() -> usize {
    256 * 1024 * 1024 // 256 MiB
}
fn default_query_timeout() -> Duration {
    Duration::from_secs(60)
}
fn default_wal_file_threshold() -> usize {
    16
}

impl Default for AnalyticsConfig {
    fn default() -> Self {
        Self {
            default_forecast_model: default_forecast_model(),
            default_anomaly_method: default_anomaly_method(),
            default_confidence_level: default_confidence(),
            max_forecast_horizon: default_max_horizon(),
            max_training_points: default_max_training(),
            default_anomaly_threshold: default_anomaly_threshold(),
            compute_threads: 0,
        }
    }
}

// ─── Per-measurement Analytics Overrides ────────────────────────────

/// Per-measurement analytics configuration overrides.
/// Only fields set to `Some(…)` override the global defaults.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AnalyticsOverride {
    /// Override for the default forecast model.
    pub forecast_model: Option<String>,
    /// Override for the default anomaly method.
    pub anomaly_method: Option<String>,
    /// Override for the default confidence level.
    pub confidence_level: Option<f64>,
    /// Override for the default anomaly threshold.
    pub anomaly_threshold: Option<f64>,
    /// Override for the max forecast horizon.
    pub max_forecast_horizon: Option<usize>,
}

// ─── Multivariate Analysis Configuration ────────────────────────────

/// Configuration for the multivariate analysis engine.
///
/// Configurable via TOML under the `[multivariate]` section.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultivariateConfig {
    /// Maximum number of series per analysis context (default: 100).
    #[serde(default = "mv_default_max_series")]
    pub max_series_per_context: usize,
    /// Default interpolation strategy for time alignment (linear, nearest, spline, nan).
    #[serde(default = "mv_default_interpolation")]
    pub default_interpolation: String,
    /// Default rolling window size for rolling correlation (default: 1000).
    #[serde(default = "mv_default_rolling_window")]
    pub rolling_window_size: usize,
    /// Composite signal cooldown in seconds (default: 60).
    #[serde(default = "mv_default_cooldown_secs")]
    pub composite_signal_cooldown_secs: u64,
    /// PCA variance retention threshold (default: 0.95).
    #[serde(default = "mv_default_pca_threshold")]
    pub pca_variance_threshold: f64,
    /// Maximum VAR lag order (default: 10).
    #[serde(default = "mv_default_var_max_lag")]
    pub var_max_lag: usize,
}

fn mv_default_max_series() -> usize {
    100
}
fn mv_default_interpolation() -> String {
    "linear".into()
}
fn mv_default_rolling_window() -> usize {
    1000
}
fn mv_default_cooldown_secs() -> u64 {
    60
}
fn mv_default_pca_threshold() -> f64 {
    0.95
}
fn mv_default_var_max_lag() -> usize {
    10
}

impl Default for MultivariateConfig {
    fn default() -> Self {
        Self {
            max_series_per_context: mv_default_max_series(),
            default_interpolation: mv_default_interpolation(),
            rolling_window_size: mv_default_rolling_window(),
            composite_signal_cooldown_secs: mv_default_cooldown_secs(),
            pca_variance_threshold: mv_default_pca_threshold(),
            var_max_lag: mv_default_var_max_lag(),
        }
    }
}

/// Default Zstd compression level.
const fn default_zstd_level() -> i32 {
    3
}

/// Top-level database configuration.
///
/// Constructed via the builder pattern: `ChronixConfig::builder()`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChronixConfig {
    /// Path to the data directory.
    pub data_dir: PathBuf,
    /// WAL configuration.
    pub wal: WalConfig,
    /// Memtable flush threshold in bytes (default: 64 MB).
    pub memtable_flush_threshold: usize,
    /// Maximum total memtable memory across active + frozen (default: 256 MB).
    pub max_memtable_memory: usize,
    /// Time-shard duration (default: 1 hour).
    pub shard_duration: Duration,
    /// Out-of-order tolerance in shard units (default: 2).
    pub ooo_shard_tolerance: u32,
    /// Retention period (default: 30 days, None = infinite).
    pub retention: Option<Duration>,
    /// Per-measurement retention overrides.
    pub measurement_retention: HashMap<String, Duration>,
    /// Compression codec for segments (default: LZ4).
    pub compression: CompressionCodec,
    /// Zstd compression level for new segments (1–22, default: 3).
    ///
    /// Only meaningful when `compression` is `Zstd`. Higher is smaller and
    /// slower; the warm tier re-compresses at 9.
    #[serde(default = "default_zstd_level")]
    pub zstd_level: i32,
    /// Train a Zstd dictionary from the first row group of each segment
    /// (default: false).
    ///
    /// The writer collects the first row group's encoded blocks as training
    /// samples and, if there is enough of them (≥ 8 blocks, ≥ 16 KiB),
    /// compresses every later row group against the trained dictionary,
    /// storing it in the segment metadata for the reader. It helps most where
    /// blocks are small and repetitive, and costs a training pass per segment.
    ///
    /// Only meaningful when `compression` is `Zstd`.
    #[serde(default)]
    pub zstd_dict_training: bool,
    /// Float encoding algorithm (default: Chimp).
    pub float_encoding: FloatEncoding,
    /// Storage backend configuration.
    pub storage_backend: StorageBackendConfig,
    /// Segment cache size in bytes (default: 512 MB).
    pub segment_cache_size: usize,
    /// Enable Last Value Cache (default: false).
    pub enable_last_value_cache: bool,
    /// Per-measurement LVC opt-in. When `Some`, only listed measurements
    /// participate in the LVC. When `None`, all measurements use the LVC
    /// (provided `enable_last_value_cache` is `true`).
    pub lvc_measurements: Option<HashSet<String>>,
    /// Maximum concurrent compaction tasks (default: 2).
    pub compaction_concurrency: usize,
    /// Maximum series cardinality per shard (default: 1M).
    pub max_series_cardinality: usize,
    /// Maximum query result size in bytes (default: 256 MB, 0 = unlimited).
    /// Queries whose intermediate batches exceed this are aborted early.
    #[serde(default = "default_max_query_result_bytes")]
    pub max_query_result_bytes: usize,
    /// Per-query memory budget in bytes (default: 256 MiB, 0 = unlimited).
    ///
    /// Each query execution creates a `MemoryTracker` with this
    /// budget.  Intermediate allocations (collected `RecordBatch`es)
    /// are charged against the tracker; if the budget is exceeded the
    /// query fails with `QueryMemoryExceeded` instead of causing OOM.
    #[serde(default = "default_per_query_memory_limit")]
    pub per_query_memory_limit: usize,
    /// Query execution timeout (default: 60s, `Duration::ZERO` = no timeout).
    ///
    /// Queries that exceed this duration are aborted with a
    /// `QueryTimeout` error to prevent runaway scans.
    #[serde(default = "default_query_timeout")]
    pub query_timeout: Duration,
    /// Graceful shutdown timeout (default: 30s).
    pub shutdown_timeout: Duration,
    /// Analytics engine configuration.
    #[serde(default)]
    pub analytics: AnalyticsConfig,
    /// Multivariate analysis engine configuration.
    #[serde(default)]
    pub multivariate: MultivariateConfig,
    /// Per-measurement analytics configuration overrides.
    #[serde(default)]
    pub measurement_analytics: HashMap<String, AnalyticsOverride>,
    /// Soft-delete TTL for dropped measurements.
    ///
    /// When set, `drop_measurement` marks the measurement as "pending
    /// deletion" with a deadline instead of immediately removing its
    /// segments. A background GC pass hard-deletes measurements that
    /// have been pending longer than this TTL, allowing accidental drops
    /// to be recovered within the grace window.
    ///
    /// Default: `None` (immediate hard-delete, original behaviour).
    #[serde(default)]
    pub soft_delete_ttl: Option<Duration>,
    /// WAL file count threshold for write admission control (default: 16).
    ///
    /// When the number of WAL files exceeds this threshold, new writes
    /// are rejected with `DbError::Overloaded` to prevent unbounded
    /// WAL growth. Tune upward for bursty write workloads, downward
    /// for I/O-constrained systems.
    #[serde(default = "default_wal_file_threshold")]
    pub wal_file_threshold: usize,
}

impl ChronixConfig {
    /// Returns a builder with sensible defaults.
    /// A configuration preset for small-footprint deployments — IoT
    /// gateways and edge devices with 512 MB–1 GB of RAM and flash (eMMC/SD)
    /// storage.
    ///
    /// Compared to the default configuration:
    ///
    /// - **Memory budget ≈ 48 MB** — 8 MB memtable flush threshold,
    ///   24 MB total memtable budget, 16 MB segment cache.
    /// - **Flash-friendly WAL** — `Periodic(5s)` fsync coalesces syncs to
    ///   reduce write amplification and flash wear; up to ~5 s of the most
    ///   recent writes may be lost on power failure. Choose
    ///   [`FsyncPolicy::PerBatch`] instead if that is not acceptable.
    /// - **Single compaction worker** — bounds background CPU and I/O.
    ///
    /// All fields remain overridable after construction.
    #[must_use]
    pub fn small(data_dir: impl Into<PathBuf>) -> Self {
        Self::builder()
            .data_dir(data_dir)
            .memtable_flush_threshold(8 * 1024 * 1024) // 8 MB
            .max_memtable_memory(24 * 1024 * 1024) // 24 MB
            .segment_cache_size(16 * 1024 * 1024) // 16 MB
            .compaction_concurrency(1)
            .wal_fsync_policy(FsyncPolicy::Periodic(Duration::from_secs(5)))
            .wal_max_file_size(8 * 1024 * 1024) // 8 MB — bounded replay on tiny devices
            .build()
            .expect("BUG: the small-footprint preset must always validate")
    }

    /// Returns a builder with sensible defaults.
    #[must_use]
    pub fn builder() -> ChronixConfigBuilder {
        ChronixConfigBuilder::default()
    }

    /// Returns effective analytics config for a measurement, applying any per-measurement overrides.
    #[must_use]
    pub fn effective_analytics(&self, measurement: &str) -> AnalyticsConfig {
        let base = &self.analytics;
        match self.measurement_analytics.get(measurement) {
            None => base.clone(),
            Some(ov) => AnalyticsConfig {
                default_forecast_model: ov
                    .forecast_model
                    .clone()
                    .unwrap_or_else(|| base.default_forecast_model.clone()),
                default_anomaly_method: ov
                    .anomaly_method
                    .clone()
                    .unwrap_or_else(|| base.default_anomaly_method.clone()),
                default_confidence_level: ov
                    .confidence_level
                    .unwrap_or(base.default_confidence_level),
                default_anomaly_threshold: ov
                    .anomaly_threshold
                    .unwrap_or(base.default_anomaly_threshold),
                max_forecast_horizon: ov.max_forecast_horizon.unwrap_or(base.max_forecast_horizon),
                max_training_points: base.max_training_points,
                compute_threads: base.compute_threads,
            },
        }
    }

    /// Load configuration from a TOML file, then apply builder overrides.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] if the file cannot be read, parsed, or fails
    /// validation.
    pub fn from_toml(path: &Path) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    /// Validate configuration invariants.
    ///
    /// Called by `from_toml` and [`ChronixConfigBuilder::build`] to enforce
    /// structural invariants on the final configuration.
    /// # Errors
    ///
    /// Returns [`ConfigError`] when any field is out of range or
    /// internally inconsistent (see the individual checks).
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.memtable_flush_threshold == 0 {
            return Err(ConfigError::Validation {
                message: "memtable_flush_threshold must be > 0".into(),
            });
        }
        if self.max_memtable_memory == 0 {
            return Err(ConfigError::Validation {
                message: "max_memtable_memory must be > 0".into(),
            });
        }
        if self.max_memtable_memory < self.memtable_flush_threshold {
            return Err(ConfigError::Validation {
                message: format!(
                    "max_memtable_memory ({}) must be >= memtable_flush_threshold ({})",
                    self.max_memtable_memory, self.memtable_flush_threshold
                ),
            });
        }
        if self.shard_duration.is_zero() {
            return Err(ConfigError::Validation {
                message: "shard_duration must be > 0".into(),
            });
        }
        if let Some(retention) = &self.retention {
            if *retention < self.shard_duration {
                return Err(ConfigError::Validation {
                    message: format!(
                        "retention ({retention:?}) must be >= shard_duration ({:?})",
                        self.shard_duration
                    ),
                });
            }
        }
        if self.compaction_concurrency == 0 {
            return Err(ConfigError::Validation {
                message: "compaction_concurrency must be > 0".into(),
            });
        }
        if self.max_series_cardinality == 0 {
            return Err(ConfigError::Validation {
                message: "max_series_cardinality must be > 0".into(),
            });
        }
        if self.wal.max_file_size == 0 {
            return Err(ConfigError::Validation {
                message: "WAL max_file_size must be > 0".into(),
            });
        }
        if self.wal.max_unflushed_wals == 0 {
            return Err(ConfigError::Validation {
                message: "WAL max_unflushed_wals must be > 0".into(),
            });
        }

        // Validate compaction and analytics settings at startup
        // to surface misconfigurations early.
        if self.shutdown_timeout.is_zero() {
            return Err(ConfigError::Validation {
                message: "shutdown_timeout must be > 0".into(),
            });
        }
        if self.analytics.max_forecast_horizon == 0 {
            return Err(ConfigError::Validation {
                message: "analytics.max_forecast_horizon must be > 0".into(),
            });
        }
        if self.analytics.max_training_points == 0 {
            return Err(ConfigError::Validation {
                message: "analytics.max_training_points must be > 0".into(),
            });
        }
        if self.analytics.default_anomaly_threshold <= 0.0 {
            return Err(ConfigError::Validation {
                message: format!(
                    "analytics.default_anomaly_threshold must be > 0.0, got {}",
                    self.analytics.default_anomaly_threshold
                ),
            });
        }
        if self.multivariate.max_series_per_context == 0 {
            return Err(ConfigError::Validation {
                message: "multivariate.max_series_per_context must be > 0".into(),
            });
        }
        if self.multivariate.rolling_window_size == 0 {
            return Err(ConfigError::Validation {
                message: "multivariate.rolling_window_size must be > 0".into(),
            });
        }
        if self.multivariate.var_max_lag == 0 {
            return Err(ConfigError::Validation {
                message: "multivariate.var_max_lag must be > 0".into(),
            });
        }
        Ok(())
    }
}

/// Builder for [`ChronixConfig`] with validation on `build()`.
#[derive(Debug, Clone)]
pub struct ChronixConfigBuilder {
    data_dir: Option<PathBuf>,
    wal: WalConfig,
    memtable_flush_threshold: usize,
    max_memtable_memory: usize,
    shard_duration: Duration,
    ooo_shard_tolerance: u32,
    retention: Option<Duration>,
    measurement_retention: HashMap<String, Duration>,
    compression: CompressionCodec,
    zstd_level: i32,
    zstd_dict_training: bool,
    float_encoding: FloatEncoding,
    storage_backend: StorageBackendConfig,
    segment_cache_size: usize,
    enable_last_value_cache: bool,
    lvc_measurements: Option<HashSet<String>>,
    compaction_concurrency: usize,
    max_series_cardinality: usize,
    max_query_result_bytes: usize,
    per_query_memory_limit: usize,
    query_timeout: Duration,
    shutdown_timeout: Duration,
    analytics: AnalyticsConfig,
    multivariate: MultivariateConfig,
    measurement_analytics: HashMap<String, AnalyticsOverride>,
    soft_delete_ttl: Option<Duration>,
    wal_file_threshold: usize,
}

impl Default for ChronixConfigBuilder {
    fn default() -> Self {
        Self {
            data_dir: None,
            wal: WalConfig::default(),
            memtable_flush_threshold: 64 * 1024 * 1024, // 64 MB
            max_memtable_memory: 256 * 1024 * 1024,     // 256 MB
            shard_duration: Duration::from_secs(3600),  // 1 hour
            ooo_shard_tolerance: 2,
            retention: Some(Duration::from_secs(30 * 24 * 3600)), // 30 days
            measurement_retention: HashMap::new(),
            compression: CompressionCodec::default(),
            zstd_level: default_zstd_level(),
            zstd_dict_training: false,
            float_encoding: FloatEncoding::default(),
            storage_backend: StorageBackendConfig::default(),
            segment_cache_size: 512 * 1024 * 1024, // 512 MB
            enable_last_value_cache: false,
            lvc_measurements: None,
            compaction_concurrency: 2,
            max_series_cardinality: 1_000_000,
            max_query_result_bytes: default_max_query_result_bytes(),
            per_query_memory_limit: default_per_query_memory_limit(),
            query_timeout: default_query_timeout(),
            shutdown_timeout: Duration::from_secs(30),
            analytics: AnalyticsConfig::default(),
            multivariate: MultivariateConfig::default(),
            measurement_analytics: HashMap::new(),
            soft_delete_ttl: None,
            wal_file_threshold: default_wal_file_threshold(),
        }
    }
}

impl ChronixConfigBuilder {
    /// Set the data directory path (required).
    #[must_use]
    pub fn data_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.data_dir = Some(path.into());
        self
    }

    /// Set the WAL fsync policy.
    #[must_use]
    pub fn wal_fsync_policy(mut self, policy: FsyncPolicy) -> Self {
        self.wal.fsync_policy = policy;
        self
    }

    /// Set the maximum WAL file size.
    #[must_use]
    pub fn wal_max_file_size(mut self, size: usize) -> Self {
        self.wal.max_file_size = size;
        self
    }

    /// Set the maximum number of unflushed WAL files.
    #[must_use]
    pub fn wal_max_unflushed(mut self, count: usize) -> Self {
        self.wal.max_unflushed_wals = count;
        self
    }

    /// Set the memtable flush threshold in bytes.
    #[must_use]
    pub fn memtable_flush_threshold(mut self, size: usize) -> Self {
        self.memtable_flush_threshold = size;
        self
    }

    /// Set the maximum total memtable memory.
    #[must_use]
    pub fn max_memtable_memory(mut self, size: usize) -> Self {
        self.max_memtable_memory = size;
        self
    }

    /// Set the time-shard duration.
    #[must_use]
    pub fn shard_duration(mut self, duration: Duration) -> Self {
        self.shard_duration = duration;
        self
    }

    /// Set the retention period (None for infinite).
    #[must_use]
    pub fn retention(mut self, duration: Option<Duration>) -> Self {
        self.retention = duration;
        self
    }

    /// Set the compression codec.
    #[must_use]
    pub fn compression(mut self, codec: CompressionCodec) -> Self {
        self.compression = codec;
        self
    }

    /// Set the Zstd compression level for new segments (1–22).
    #[must_use]
    pub fn zstd_level(mut self, level: i32) -> Self {
        self.zstd_level = level;
        self
    }

    /// Enable Zstd dictionary training for new segments.
    #[must_use]
    pub fn zstd_dict_training(mut self, enabled: bool) -> Self {
        self.zstd_dict_training = enabled;
        self
    }

    /// Set the float encoding algorithm.
    #[must_use]
    pub fn float_encoding(mut self, encoding: FloatEncoding) -> Self {
        self.float_encoding = encoding;
        self
    }

    /// Set the out-of-order shard tolerance (in shard units).
    #[must_use]
    pub fn ooo_shard_tolerance(mut self, tolerance: u32) -> Self {
        self.ooo_shard_tolerance = tolerance;
        self
    }

    /// Add a per-measurement retention override.
    #[must_use]
    pub fn measurement_retention(
        mut self,
        measurement: impl Into<String>,
        duration: Duration,
    ) -> Self {
        self.measurement_retention
            .insert(measurement.into(), duration);
        self
    }

    /// Set the storage backend.
    #[must_use]
    pub fn storage_backend(mut self, backend: StorageBackendConfig) -> Self {
        self.storage_backend = backend;
        self
    }

    /// Set the maximum concurrent compaction tasks.
    #[must_use]
    pub fn compaction_concurrency(mut self, concurrency: usize) -> Self {
        self.compaction_concurrency = concurrency;
        self
    }

    /// Enable or disable the Last Value Cache.
    #[must_use]
    pub fn enable_last_value_cache(mut self, enable: bool) -> Self {
        self.enable_last_value_cache = enable;
        self
    }

    /// Restrict the Last Value Cache to specific measurements.
    ///
    /// When set, only the listed measurements participate in the LVC.
    /// Requires `enable_last_value_cache(true)` to have any effect.
    #[must_use]
    pub fn lvc_measurements(mut self, measurements: HashSet<String>) -> Self {
        self.lvc_measurements = Some(measurements);
        self
    }

    /// Set the segment cache size in bytes.
    #[must_use]
    pub fn segment_cache_size(mut self, size: usize) -> Self {
        self.segment_cache_size = size;
        self
    }

    /// Set the maximum series cardinality per shard.
    #[must_use]
    pub fn max_series_cardinality(mut self, max: usize) -> Self {
        self.max_series_cardinality = max;
        self
    }

    /// Set the maximum query result size in bytes (0 = unlimited).
    #[must_use]
    pub fn max_query_result_bytes(mut self, max: usize) -> Self {
        self.max_query_result_bytes = max;
        self
    }

    /// Set the per-query memory budget in bytes (0 = unlimited).
    #[must_use]
    pub fn per_query_memory_limit(mut self, limit: usize) -> Self {
        self.per_query_memory_limit = limit;
        self
    }

    /// Set the query execution timeout (`Duration::ZERO` = no timeout).
    #[must_use]
    pub fn query_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = timeout;
        self
    }

    /// Set the graceful shutdown timeout.
    #[must_use]
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Set the analytics engine configuration.
    #[must_use]
    pub fn analytics(mut self, config: AnalyticsConfig) -> Self {
        self.analytics = config;
        self
    }

    /// Set the multivariate analysis engine configuration.
    #[must_use]
    pub fn multivariate(mut self, config: MultivariateConfig) -> Self {
        self.multivariate = config;
        self
    }

    /// Set the soft-delete TTL for dropped measurements.
    ///
    /// When set, `drop_measurement` marks the measurement as pending
    /// deletion instead of immediately removing data.
    #[must_use]
    pub fn soft_delete_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.soft_delete_ttl = ttl;
        self
    }

    /// Set the WAL file count admission threshold (default: 16).
    #[must_use]
    pub fn wal_file_threshold(mut self, count: usize) -> Self {
        self.wal_file_threshold = count;
        self
    }

    /// Add a per-measurement analytics override.
    #[must_use]
    pub fn measurement_analytics_override(
        mut self,
        measurement: &str,
        override_config: AnalyticsOverride,
    ) -> Self {
        self.measurement_analytics
            .insert(measurement.to_string(), override_config);
        self
    }

    /// Build the configuration, validating all constraints.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] if a required field is missing or a constraint
    /// is violated.
    pub fn build(self) -> Result<ChronixConfig, ConfigError> {
        const VALID_FORECAST_MODELS: &[&str] = &[
            "ses",
            "holt",
            "holt_winters",
            "arima",
            "sarima",
            "linear_regression",
        ];
        const VALID_ANOMALY_METHODS: &[&str] = &[
            "zscore",
            "modified_zscore",
            "iqr",
            "forecast_residual",
            "moving_average",
            "dynamic_threshold",
        ];

        let data_dir = self
            .data_dir
            .ok_or(ConfigError::MissingField { field: "data_dir" })?;

        // Construct the config first, then validate shared invariants via
        // the single `ChronixConfig::validate()` method (FINDING-11).
        let config = ChronixConfig {
            data_dir,
            wal: self.wal,
            memtable_flush_threshold: self.memtable_flush_threshold,
            max_memtable_memory: self.max_memtable_memory,
            shard_duration: self.shard_duration,
            ooo_shard_tolerance: self.ooo_shard_tolerance,
            retention: self.retention,
            measurement_retention: self.measurement_retention,
            compression: self.compression,
            zstd_level: self.zstd_level,
            zstd_dict_training: self.zstd_dict_training,
            float_encoding: self.float_encoding,
            storage_backend: self.storage_backend,
            segment_cache_size: self.segment_cache_size,
            enable_last_value_cache: self.enable_last_value_cache,
            lvc_measurements: self.lvc_measurements,
            compaction_concurrency: self.compaction_concurrency,
            max_series_cardinality: self.max_series_cardinality,
            max_query_result_bytes: self.max_query_result_bytes,
            per_query_memory_limit: self.per_query_memory_limit,
            query_timeout: self.query_timeout,
            shutdown_timeout: self.shutdown_timeout,
            analytics: self.analytics,
            multivariate: self.multivariate,
            measurement_analytics: self.measurement_analytics,
            soft_delete_ttl: self.soft_delete_ttl,
            wal_file_threshold: self.wal_file_threshold,
        };

        // Shared structural validation (same checks as from_toml path).
        config.validate()?;

        // Builder-only checks below (FINDING-12, -13, -14, -15).

        // FINDING-12: Warn when segment_cache_size is zero.
        if config.segment_cache_size == 0 {
            tracing::warn!("segment_cache_size is 0 — segment caching is effectively disabled");
        }

        // FINDING-14: Validate confidence level is in (0, 1].
        let cl = config.analytics.default_confidence_level;
        if !(0.0..=1.0).contains(&cl) || cl == 0.0 {
            return Err(ConfigError::Validation {
                message: format!("default_confidence_level must be in (0.0, 1.0], got {cl}"),
            });
        }

        // FINDING-15: Validate PCA variance_threshold is in (0, 1].
        let vt = config.multivariate.pca_variance_threshold;
        if !(0.0..=1.0).contains(&vt) || vt == 0.0 {
            return Err(ConfigError::Validation {
                message: format!("pca_variance_threshold must be in (0.0, 1.0], got {vt}"),
            });
        }

        // FINDING-13: Validate analytics config string fields against
        // known values to catch typos at startup rather than at runtime.
        if !VALID_FORECAST_MODELS.contains(&config.analytics.default_forecast_model.as_str()) {
            return Err(ConfigError::Validation {
                message: format!(
                    "unknown default_forecast_model '{}', valid values: {VALID_FORECAST_MODELS:?}",
                    config.analytics.default_forecast_model
                ),
            });
        }

        if !VALID_ANOMALY_METHODS.contains(&config.analytics.default_anomaly_method.as_str()) {
            return Err(ConfigError::Validation {
                message: format!(
                    "unknown default_anomaly_method '{}', valid values: {VALID_ANOMALY_METHODS:?}",
                    config.analytics.default_anomaly_method
                ),
            });
        }

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults_valid() {
        let config = ChronixConfig::builder().data_dir("./data").build().unwrap();

        assert_eq!(config.data_dir, PathBuf::from("./data"));
        assert_eq!(config.wal.fsync_policy, FsyncPolicy::PerBatch);
        assert_eq!(config.wal.max_file_size, 32 * 1024 * 1024);
        assert_eq!(config.wal.max_unflushed_wals, 4);
        assert_eq!(config.memtable_flush_threshold, 64 * 1024 * 1024);
        assert_eq!(config.max_memtable_memory, 256 * 1024 * 1024);
        assert_eq!(config.shard_duration, Duration::from_secs(3600));
        assert_eq!(config.compression, CompressionCodec::Lz4);
        assert_eq!(config.float_encoding, FloatEncoding::Chimp);
        assert!(!config.enable_last_value_cache);
        assert_eq!(config.compaction_concurrency, 2);
        assert_eq!(config.max_series_cardinality, 1_000_000);
    }

    #[test]
    fn builder_missing_data_dir() {
        let err = ChronixConfig::builder().build().unwrap_err();
        assert!(matches!(
            err,
            ConfigError::MissingField { field: "data_dir" }
        ));
    }

    #[test]
    fn builder_zero_flush_threshold() {
        let err = ChronixConfig::builder()
            .data_dir("./data")
            .memtable_flush_threshold(0)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("memtable_flush_threshold"));
    }

    #[test]
    fn builder_memory_less_than_threshold() {
        let err = ChronixConfig::builder()
            .data_dir("./data")
            .memtable_flush_threshold(100)
            .max_memtable_memory(50)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("max_memtable_memory"));
    }

    #[test]
    fn builder_retention_less_than_shard() {
        let err = ChronixConfig::builder()
            .data_dir("./data")
            .shard_duration(Duration::from_secs(3600))
            .retention(Some(Duration::from_secs(60)))
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("retention"));
    }

    #[test]
    fn builder_zero_shard_duration() {
        let err = ChronixConfig::builder()
            .data_dir("./data")
            .shard_duration(Duration::ZERO)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("shard_duration"));
    }

    #[test]
    fn builder_no_retention() {
        let config = ChronixConfig::builder()
            .data_dir("./data")
            .retention(None)
            .build()
            .unwrap();
        assert!(config.retention.is_none());
    }

    #[test]
    fn builder_custom_wal() {
        let config = ChronixConfig::builder()
            .data_dir("./data")
            .wal_fsync_policy(FsyncPolicy::PerWrite)
            .wal_max_file_size(16 * 1024 * 1024)
            .wal_max_unflushed(8)
            .build()
            .unwrap();
        assert_eq!(config.wal.fsync_policy, FsyncPolicy::PerWrite);
        assert_eq!(config.wal.max_file_size, 16 * 1024 * 1024);
        assert_eq!(config.wal.max_unflushed_wals, 8);
    }

    #[test]
    fn config_serde_roundtrip() {
        let config = ChronixConfig::builder().data_dir("./data").build().unwrap();
        let json = serde_json::to_string(&config).unwrap();
        let back: ChronixConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.data_dir, back.data_dir);
        assert_eq!(config.wal.max_file_size, back.wal.max_file_size);
    }

    /// Miri runs with filesystem isolation, so the three tests that reach the
    /// filesystem carry their own `ignore` rather than being named in the CI
    /// command: a `--skip` list has to be edited every time such a test is
    /// added, and the one nobody remembered to add is the one that breaks the
    /// job.
    #[test]
    #[cfg_attr(miri, ignore = "reaches the filesystem")]
    fn config_toml_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");

        let config = ChronixConfig::builder().data_dir("./data").build().unwrap();

        let toml_str = toml::to_string_pretty(&config).unwrap();
        std::fs::write(&path, &toml_str).unwrap();

        let loaded = ChronixConfig::from_toml(&path).unwrap();
        assert_eq!(config.data_dir, loaded.data_dir);
    }

    #[test]
    fn fsync_policy_default() {
        assert_eq!(FsyncPolicy::default(), FsyncPolicy::PerBatch);
    }

    #[test]
    fn compression_codec_default() {
        assert_eq!(CompressionCodec::default(), CompressionCodec::Lz4);
    }

    #[test]
    fn float_encoding_default() {
        assert_eq!(FloatEncoding::default(), FloatEncoding::Chimp);
    }

    #[test]
    fn builder_zero_max_memtable_memory() {
        let err = ChronixConfig::builder()
            .data_dir("./data")
            .max_memtable_memory(0)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("max_memtable_memory"));
    }

    #[test]
    fn builder_zero_compaction_concurrency() {
        let err = ChronixConfig::builder()
            .data_dir("./data")
            .compaction_concurrency(0)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("compaction_concurrency"));
    }

    #[test]
    fn builder_zero_wal_max_file_size() {
        let err = ChronixConfig::builder()
            .data_dir("./data")
            .wal_max_file_size(0)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("max_file_size"));
    }

    #[test]
    fn builder_zero_wal_max_unflushed() {
        let err = ChronixConfig::builder()
            .data_dir("./data")
            .wal_max_unflushed(0)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("max_unflushed"));
    }

    #[test]
    #[cfg_attr(miri, ignore = "reaches the filesystem")]
    fn from_toml_nonexistent_file() {
        let err = ChronixConfig::from_toml(std::path::Path::new("/no/such/file.toml")).unwrap_err();
        assert!(matches!(err, ConfigError::Io(_)));
    }

    #[test]
    #[cfg_attr(miri, ignore = "reaches the filesystem")]
    fn from_toml_invalid_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.toml");
        std::fs::write(&path, "this is not valid toml [[[").unwrap();
        let err = ChronixConfig::from_toml(&path).unwrap_err();
        assert!(matches!(err, ConfigError::Parse(_)));
    }

    #[test]
    fn fsync_policy_display() {
        assert_eq!(FsyncPolicy::PerWrite.to_string(), "per-write");
        assert_eq!(FsyncPolicy::PerBatch.to_string(), "per-batch");
        let periodic = FsyncPolicy::Periodic(Duration::from_millis(500));
        assert!(periodic.to_string().contains("500"));
    }

    #[test]
    fn compression_codec_display() {
        assert_eq!(CompressionCodec::Lz4.to_string(), "lz4");
        assert_eq!(CompressionCodec::Zstd.to_string(), "zstd");
        assert_eq!(CompressionCodec::None.to_string(), "none");
    }

    #[test]
    fn float_encoding_display() {
        assert_eq!(FloatEncoding::Chimp.to_string(), "chimp");
        assert_eq!(FloatEncoding::Gorilla.to_string(), "gorilla");
        assert_eq!(FloatEncoding::Plain.to_string(), "plain");
    }

    #[test]
    fn wal_config_serde_roundtrip() {
        let config = WalConfig {
            fsync_policy: FsyncPolicy::Periodic(Duration::from_secs(5)),
            max_file_size: 16 * 1024 * 1024,
            max_unflushed_wals: 8,
            compress: true,
            group_sync_timeout_secs: 5,
        };
        let json = serde_json::to_string(&config).unwrap();
        let back: WalConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, back);
    }

    #[test]
    fn storage_backend_s3_serde_roundtrip() {
        let config = StorageBackendConfig::S3 {
            bucket: "my-bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some("http://localhost:9000".into()),
        };
        let json = serde_json::to_string(&config).unwrap();
        let back: StorageBackendConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, back);
    }

    // ── MultivariateConfig tests ────────────────────────────────────────

    #[test]
    fn multivariate_config_defaults() {
        let config = MultivariateConfig::default();
        assert_eq!(config.max_series_per_context, 100);
        assert_eq!(config.default_interpolation, "linear");
        assert_eq!(config.rolling_window_size, 1000);
        assert_eq!(config.composite_signal_cooldown_secs, 60);
        assert!((config.pca_variance_threshold - 0.95).abs() < 1e-9);
        assert_eq!(config.var_max_lag, 10);
    }

    #[test]
    fn multivariate_config_serde_roundtrip() {
        let config = MultivariateConfig {
            max_series_per_context: 50,
            default_interpolation: "nearest".into(),
            rolling_window_size: 500,
            composite_signal_cooldown_secs: 120,
            pca_variance_threshold: 0.99,
            var_max_lag: 5,
        };
        let json = serde_json::to_string(&config).unwrap();
        let back: MultivariateConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.max_series_per_context, back.max_series_per_context);
        assert_eq!(config.default_interpolation, back.default_interpolation);
        assert_eq!(config.rolling_window_size, back.rolling_window_size);
        assert_eq!(
            config.composite_signal_cooldown_secs,
            back.composite_signal_cooldown_secs
        );
        assert!((config.pca_variance_threshold - back.pca_variance_threshold).abs() < 1e-9);
        assert_eq!(config.var_max_lag, back.var_max_lag);
    }

    #[test]
    fn multivariate_config_in_chronix_config() {
        let config = ChronixConfig::builder().data_dir("./data").build().unwrap();
        assert_eq!(config.multivariate.max_series_per_context, 100);
        assert_eq!(config.multivariate.var_max_lag, 10);
    }

    #[test]
    fn per_measurement_analytics_override() {
        let config = ChronixConfig::builder()
            .data_dir(std::path::Path::new("/tmp/test"))
            .measurement_analytics_override(
                "cpu",
                AnalyticsOverride {
                    forecast_model: Some("holt_winters".to_string()),
                    anomaly_threshold: Some(2.5),
                    ..Default::default()
                },
            )
            .build()
            .unwrap();

        let cpu_cfg = config.effective_analytics("cpu");
        assert_eq!(cpu_cfg.default_forecast_model, "holt_winters");
        assert!((cpu_cfg.default_anomaly_threshold - 2.5).abs() < 1e-10);
        // Non-overridden fields use global defaults
        assert_eq!(cpu_cfg.default_anomaly_method, "zscore");

        // Measurement without override gets global defaults
        let mem_cfg = config.effective_analytics("memory");
        assert_eq!(mem_cfg.default_forecast_model, "ses");
    }

    #[test]
    fn multivariate_config_builder_override() {
        let mv = MultivariateConfig {
            max_series_per_context: 25,
            ..MultivariateConfig::default()
        };
        let config = ChronixConfig::builder()
            .data_dir("./data")
            .multivariate(mv)
            .build()
            .unwrap();
        assert_eq!(config.multivariate.max_series_per_context, 25);
    }

    // ── Compaction / analytics / multivariate validation tests ────

    #[test]
    fn validate_zero_shutdown_timeout() {
        let err = ChronixConfig::builder()
            .data_dir("./data")
            .shutdown_timeout(Duration::ZERO)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("shutdown_timeout"));
    }

    #[test]
    fn validate_zero_max_forecast_horizon() {
        let analytics = AnalyticsConfig {
            max_forecast_horizon: 0,
            ..AnalyticsConfig::default()
        };
        let err = ChronixConfig::builder()
            .data_dir("./data")
            .analytics(analytics)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("max_forecast_horizon"));
    }

    #[test]
    fn validate_zero_max_training_points() {
        let analytics = AnalyticsConfig {
            max_training_points: 0,
            ..AnalyticsConfig::default()
        };
        let err = ChronixConfig::builder()
            .data_dir("./data")
            .analytics(analytics)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("max_training_points"));
    }

    #[test]
    fn validate_negative_anomaly_threshold() {
        let analytics = AnalyticsConfig {
            default_anomaly_threshold: -1.0,
            ..AnalyticsConfig::default()
        };
        let err = ChronixConfig::builder()
            .data_dir("./data")
            .analytics(analytics)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("default_anomaly_threshold"));
    }

    #[test]
    fn validate_zero_max_series_per_context() {
        let mv = MultivariateConfig {
            max_series_per_context: 0,
            ..MultivariateConfig::default()
        };
        let err = ChronixConfig::builder()
            .data_dir("./data")
            .multivariate(mv)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("max_series_per_context"));
    }

    #[test]
    fn validate_zero_rolling_window_size() {
        let mv = MultivariateConfig {
            rolling_window_size: 0,
            ..MultivariateConfig::default()
        };
        let err = ChronixConfig::builder()
            .data_dir("./data")
            .multivariate(mv)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("rolling_window_size"));
    }

    #[test]
    fn validate_zero_var_max_lag() {
        let mv = MultivariateConfig {
            var_max_lag: 0,
            ..MultivariateConfig::default()
        };
        let err = ChronixConfig::builder()
            .data_dir("./data")
            .multivariate(mv)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("var_max_lag"));
    }

    #[test]
    fn wal_file_threshold_default_is_16() {
        let config = ChronixConfig::builder().data_dir("./data").build().unwrap();
        assert_eq!(config.wal_file_threshold, 16);
    }

    #[test]
    fn wal_file_threshold_configurable() {
        let config = ChronixConfig::builder()
            .data_dir("./data")
            .wal_file_threshold(32)
            .build()
            .unwrap();
        assert_eq!(config.wal_file_threshold, 32);
    }

    #[test]
    fn small_preset_is_valid_and_bounded() {
        let cfg = ChronixConfig::small("/tmp/chronix-small");
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.memtable_flush_threshold, 8 * 1024 * 1024);
        assert_eq!(cfg.max_memtable_memory, 24 * 1024 * 1024);
        assert_eq!(cfg.segment_cache_size, 16 * 1024 * 1024);
        assert_eq!(cfg.compaction_concurrency, 1);
        assert!(matches!(cfg.wal.fsync_policy, FsyncPolicy::Periodic(_)));
        assert_eq!(cfg.wal.max_file_size, 8 * 1024 * 1024);
    }
}
