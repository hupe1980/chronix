//! Distributed tracing configuration and initialization for Chronix.
//!
//! Provides a configurable tracing setup that integrates structured
//! logging with optional OpenTelemetry trace export via OTLP.
//!
//! # Features
//!
//! - **Structured logging** via `tracing-subscriber` with `EnvFilter`
//! - **JSON log format** for production deployments
//! - **OpenTelemetry integration** (feature-gated `otlp`):
//!   - W3C Trace Context propagation
//!   - OTLP gRPC export to an OpenTelemetry Collector
//!   - Configurable sampling (ratio-based with always-on for errors)
//!   - Trace-log correlation (trace/span IDs embedded in log output)
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use chronixd::otel::{TracingConfig, init_tracing};
//!
//! # async fn example() {
//! let config = TracingConfig {
//!     service_name: "chronixd".into(),
//!     log_format: chronixd::otel::LogFormat::Json,
//!     ..Default::default()
//! };
//! let _guard = init_tracing(&config).expect("tracing init");
//! # }
//! ```

#![warn(missing_docs)]
#![deny(unsafe_code)]

mod config;
mod init;
mod propagation;

pub use config::{LogFormat, OtlpConfig, SamplingStrategy, TracingConfig};
#[cfg(feature = "otlp")]
pub use init::replace_tracer_provider;
pub use init::{init_tracing, is_otel_active, update_log_filter, TracingGuard};
pub use propagation::{
    extract_trace_context, inject_trace_context, make_trace_interceptor, with_trace_context,
};
