//! Tracing configuration types.

use serde::{Deserialize, Serialize};

/// Log output format.
///
/// Re-exported from [`crate::config`] rather than declared again: there were
/// two of these — `Text`/`Json` in the config file and `Pretty`/`Json`/
/// `Compact` here — and only the first was ever read, because `main` installed
/// its own subscriber instead of calling [`init_tracing`](super::init_tracing).
/// A second enum for one setting is how `log_format = "json"` came to mean two
/// different things depending on which initialiser ran.
pub use crate::config::LogFormat;

/// Sampling strategy for distributed traces.
///
/// `rename_all = "snake_case"` so the TOML reads
/// `sampling = "always_on"` or `sampling = { ratio = 0.01 }`. Without it the
/// variant names were the Rust ones, so the form the documentation showed —
/// and the only form anybody would write — did not parse.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SamplingStrategy {
    /// Sample all traces (1.0 = 100%).
    AlwaysOn,
    /// Sample no traces.
    AlwaysOff,
    /// Probabilistic sampling with the given ratio (0.0–1.0).
    Ratio(f64),
}

impl Default for SamplingStrategy {
    fn default() -> Self {
        Self::Ratio(0.01) // 1% default
    }
}

/// OTLP export configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OtlpConfig {
    /// OTLP gRPC endpoint (e.g. `https://collector:4317`).
    pub endpoint: String,
    /// Sampling strategy.
    #[serde(default)]
    pub sampling: SamplingStrategy,
    /// Whether to always sample traces that contain errors.
    ///
    /// # Sampling Behavior
    ///
    /// **Warning:** Enabling this overrides the sampling strategy to
    /// `AlwaysOn`, recording **100% of ALL traces** — not just those
    /// containing errors.  This is a fundamental limitation of
    /// head-based sampling: the sampling decision is made at span
    /// creation, before any error can occur.  The only way to
    /// guarantee every error-bearing trace is captured is to record
    /// everything.
    ///
    /// **Recommendation:** for production, keep this `false` and
    /// deploy a tail-sampling collector (e.g. OpenTelemetry Collector
    /// with `tail_sampling` processor) that can inspect completed
    /// traces and retain those with error spans.
    #[serde(default)]
    pub always_sample_errors: bool,
    /// Boosted sampling ratio applied instead of `AlwaysOn`
    /// when `always_sample_errors` is set. Caps the effective ratio
    /// instead of forcing 100%.
    ///
    /// For example, if the base `sampling` is `Ratio(0.01)` (1%) and
    /// `error_boosted_ratio` is `Some(0.1)`, the sampler uses 10%
    /// instead of 100%. Set to `None` (default) to keep the legacy
    /// `AlwaysOn` behavior.
    #[serde(default)]
    pub error_boosted_ratio: Option<f64>,
    /// Require TLS for the OTLP endpoint.
    ///
    /// When `true` (default), endpoints using `http://` are rejected
    /// to prevent trace data from being sent in cleartext.
    /// Set to `false` only for local development / testing.
    #[serde(default = "default_require_tls")]
    pub require_tls: bool,
}

fn default_require_tls() -> bool {
    true
}

impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:4317".to_string(),
            sampling: SamplingStrategy::default(),
            always_sample_errors: false,
            error_boosted_ratio: None,
            require_tls: true,
        }
    }
}

/// Top-level tracing configuration — what [`init_tracing`](super::init_tracing)
/// takes.
///
/// This is *not* the shape of the `[tracing]` section; see
/// [`crate::config::TracingSettings`], which carries only what belongs to
/// tracing, and [`crate::config::ServerConfig::tracing_config`], which
/// composes this from it and from `[server]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracingConfig {
    /// Service name for trace identification.
    pub service_name: String,
    /// Log output format.
    #[serde(default)]
    pub log_format: LogFormat,
    /// Environment filter directive (e.g. `"chronix=debug,info"`).
    ///
    /// Defaults to `"info"` if not set. Overridden by `RUST_LOG` env var.
    ///
    /// # Runtime Adjustment
    ///
    /// The filter can be changed at runtime via
    /// [`update_log_filter`](crate::otel::update_log_filter), which swaps
    /// the underlying `EnvFilter` through a `reload::Layer`.
    #[serde(default = "default_filter")]
    pub log_filter: String,
    /// Optional OTLP export configuration.
    ///
    /// When `None`, only structured logging is active (no trace export).
    pub otlp: Option<OtlpConfig>,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            service_name: "chronix".to_string(),
            log_format: LogFormat::default(),
            log_filter: default_filter(),
            otlp: None,
        }
    }
}

fn default_filter() -> String {
    "info".to_string()
}
