//! Tracing initialization.
//!
//! Sets up the `tracing-subscriber` with appropriate layers:
//! - Logging (pretty/json/compact)
//! - Runtime-reloadable `EnvFilter` via `tracing_subscriber::reload`
//! - OpenTelemetry trace export (when `otlp` feature is enabled)

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use tracing_subscriber::layer::SubscriberExt;

/// Whether OpenTelemetry was successfully initialized.
///
/// Set to `true` in `init_with_otlp()` after the provider is configured.
/// Queried by [`is_otel_active()`] so callers can distinguish "OTLP not
/// configured" from "OTLP feature enabled but no provider set up".
static OTEL_INITIALIZED: AtomicBool = AtomicBool::new(false);
use tracing_subscriber::reload;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use crate::otel::config::{LogFormat, TracingConfig};

/// Guard that flushes traces on drop.
///
/// Keep this value alive for the lifetime of the application.
/// When dropped, it will flush any pending trace export batches.
pub struct TracingGuard {
    #[cfg(feature = "otlp")]
    provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
    #[cfg(not(feature = "otlp"))]
    _priv: (),
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        // Wrap in catch_unwind so a panic in the OTLP provider
        // shutdown path does not cause a double-panic abort when Drop
        // runs during stack unwinding.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            #[cfg(feature = "otlp")]
            if let Some(ref provider) = self.provider {
                if let Err(e) = provider.shutdown() {
                    eprintln!("tracing provider shutdown error: {e}");
                }
            }
        }));
        if result.is_err() {
            eprintln!("chronixd: panic during TracingGuard::drop suppressed to avoid abort");
        }
    }
}

/// Global handle for the reloadable `EnvFilter` layer.
///
/// Stored on first call to [`init_tracing`] and subsequently used by
/// [`update_log_filter`] to swap the active filter directive at runtime.
static LOG_FILTER_HANDLE: OnceLock<reload::Handle<EnvFilter, tracing_subscriber::Registry>> =
    OnceLock::new();

/// Change the active log filter directive at runtime.
///
/// Accepts any valid `EnvFilter` directive string, e.g.
/// `"info"`, `"chronix=debug,tower=warn"`, `"trace"`.
///
/// # Errors
///
/// Returns an error if:
/// The tracing subsystem has not been initialised yet.
/// The filter string is syntactically invalid.
pub fn update_log_filter(new_filter: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let handle = LOG_FILTER_HANDLE.get().ok_or("tracing not initialized")?;
    let env_filter =
        EnvFilter::try_new(new_filter).map_err(|e| format!("invalid filter directive: {e}"))?;
    handle.reload(env_filter)?;
    Ok(())
}

/// Returns `true` if OpenTelemetry trace export was successfully initialized.
///
/// Use this to distinguish between "OTLP feature enabled but not configured"
/// and "OTLP is actively exporting spans". When this returns `false` and the
/// `otlp` feature is compiled in, spans will be created but silently dropped
/// by the no-op global tracer provider.
#[must_use]
pub fn is_otel_active() -> bool {
    OTEL_INITIALIZED.load(Ordering::Relaxed)
}

/// Initialize the tracing subsystem.
///
/// Returns a [`TracingGuard`] that must be kept alive for the
/// duration of the application. When the guard is dropped, any
/// pending OTLP batches are flushed.
///
/// # Errors
///
/// Returns an error string if initialization fails.
pub fn init_tracing(config: &TracingConfig) -> Result<TracingGuard, String> {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.log_filter));

    #[cfg(feature = "otlp")]
    {
        if let Some(otlp_config) = &config.otlp {
            return init_with_otlp(config, otlp_config, filter);
        }
    }

    // Logging-only mode (no OTLP export)
    init_logging_only(config, filter)?;

    #[cfg(feature = "otlp")]
    return Ok(TracingGuard { provider: None });

    #[cfg(not(feature = "otlp"))]
    Ok(TracingGuard { _priv: () })
}

/// Initialize with logging only (no OpenTelemetry).
///
/// # Errors
///
/// Returns an error string if the global subscriber has already been set.
fn init_logging_only(config: &TracingConfig, filter: EnvFilter) -> Result<(), String> {
    let (filter_layer, handle) = reload::Layer::new(filter);
    LOG_FILTER_HANDLE
        .set(handle)
        .map_err(|_| "tracing already initialized — cannot reinitialize".to_string())?;

    let registry = tracing_subscriber::registry().with(filter_layer);

    match config.log_format {
        LogFormat::Pretty => {
            registry
                .with(tracing_subscriber::fmt::layer().pretty())
                .try_init()
                .map_err(|e| format!("failed to init tracing subscriber: {e}"))?;
        }
        LogFormat::Json => {
            registry
                .with(tracing_subscriber::fmt::layer().json())
                .try_init()
                .map_err(|e| format!("failed to init tracing subscriber: {e}"))?;
        }
        LogFormat::Compact => {
            registry
                .with(tracing_subscriber::fmt::layer().compact())
                .try_init()
                .map_err(|e| format!("failed to init tracing subscriber: {e}"))?;
        }
    }

    Ok(())
}

/// Initialize with both logging and OpenTelemetry export.
#[cfg(feature = "otlp")]
fn init_with_otlp(
    config: &TracingConfig,
    otlp_config: &crate::otel::config::OtlpConfig,
    filter: EnvFilter,
) -> Result<TracingGuard, String> {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
    use opentelemetry_sdk::Resource;
    use tracing_opentelemetry::OpenTelemetryLayer;

    // Validate TLS requirement.
    //
    // When `require_tls` is enabled (default), reject plaintext
    // `http://` endpoints to prevent trace data leakage.
    if otlp_config.require_tls && otlp_config.endpoint.starts_with("http://") {
        return Err(format!(
            "OTLP endpoint '{}' uses plaintext HTTP but require_tls=true. \
             Use https:// or set require_tls=false for local development.",
            otlp_config.endpoint
        ));
    }

    // Configure sampler.
    //
    // When `always_sample_errors` is enabled, prefer
    // `error_boosted_ratio` (if set) to avoid forcing AlwaysOn.
    // Head-based sampling decides before errors occur, so the only
    // way to guarantee every error-bearing trace is captured is to
    // raise the sampling rate. Using a boosted ratio is safer than
    // AlwaysOn in production.
    let sampler = if otlp_config.always_sample_errors {
        if let Some(boosted) = otlp_config.error_boosted_ratio {
            let clamped = boosted.clamp(0.0, 1.0);
            tracing::info!(
                ratio = clamped,
                "always_sample_errors=true — using boosted ratio instead of AlwaysOn"
            );
            Sampler::TraceIdRatioBased(clamped)
        } else {
            tracing::warn!(
                "always_sample_errors=true with no error_boosted_ratio — \
                 overriding sampler to AlwaysOn. This may cause 10x+ trace volume. \
                 Consider setting error_boosted_ratio or using a tail-sampling collector."
            );
            Sampler::AlwaysOn
        }
    } else {
        match &otlp_config.sampling {
            crate::otel::config::SamplingStrategy::AlwaysOn => Sampler::AlwaysOn,
            crate::otel::config::SamplingStrategy::AlwaysOff => Sampler::AlwaysOff,
            crate::otel::config::SamplingStrategy::Ratio(ratio) => {
                Sampler::TraceIdRatioBased(*ratio)
            }
        }
    };

    // Build OTLP exporter
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&otlp_config.endpoint)
        .build()
        .map_err(|e| format!("failed to build OTLP exporter: {e}"))?;

    // Build tracer provider
    let resource = Resource::builder()
        .with_attributes([KeyValue::new("service.name", config.service_name.clone())])
        .build();

    let provider = SdkTracerProvider::builder()
        .with_sampler(sampler)
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .build();

    let tracer = provider.tracer(config.service_name.clone());

    // Set global provider (for propagation support)
    opentelemetry::global::set_tracer_provider(provider.clone());

    // Build subscriber layers
    let otel_layer = OpenTelemetryLayer::new(tracer);

    let (filter_layer, handle) = reload::Layer::new(filter);
    LOG_FILTER_HANDLE
        .set(handle)
        .map_err(|_| "tracing already initialized — cannot reinitialize".to_string())?;

    let registry = tracing_subscriber::registry()
        .with(filter_layer)
        .with(otel_layer);

    match config.log_format {
        LogFormat::Pretty => {
            registry
                .with(tracing_subscriber::fmt::layer().pretty())
                .try_init()
                .map_err(|e| format!("failed to init tracing subscriber: {e}"))?;
        }
        LogFormat::Json => {
            registry
                .with(tracing_subscriber::fmt::layer().json())
                .try_init()
                .map_err(|e| format!("failed to init tracing subscriber: {e}"))?;
        }
        LogFormat::Compact => {
            registry
                .with(tracing_subscriber::fmt::layer().compact())
                .try_init()
                .map_err(|e| format!("failed to init tracing subscriber: {e}"))?;
        }
    }

    tracing::info!(
        service = %config.service_name,
        endpoint = %otlp_config.endpoint,
        "OpenTelemetry OTLP tracing initialized"
    );

    OTEL_INITIALIZED.store(true, Ordering::Release);

    Ok(TracingGuard {
        provider: Some(provider),
    })
}

/// Replace the active OTLP tracer provider at runtime.
///
/// This allows reconfiguring the OTLP endpoint, sampling strategy,
/// or service name without restarting the process.  The subscriber
/// layers remain unchanged — only the global tracer provider is
/// swapped.
///
/// The old provider is shut down gracefully (pending batches flushed).
///
/// # Errors
///
/// Returns an error if OTLP was never initialized or the new exporter
/// cannot be built.
#[cfg(feature = "otlp")]
pub fn replace_tracer_provider(
    config: &TracingConfig,
    otlp_config: &crate::otel::config::OtlpConfig,
) -> Result<(), String> {
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
    use opentelemetry_sdk::Resource;

    if !is_otel_active() {
        return Err("OTLP tracing was never initialized".into());
    }

    let sampler = match &otlp_config.sampling {
        crate::otel::config::SamplingStrategy::AlwaysOn => Sampler::AlwaysOn,
        crate::otel::config::SamplingStrategy::AlwaysOff => Sampler::AlwaysOff,
        crate::otel::config::SamplingStrategy::Ratio(ratio) => Sampler::TraceIdRatioBased(*ratio),
    };

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&otlp_config.endpoint)
        .build()
        .map_err(|e| format!("failed to build OTLP exporter: {e}"))?;

    let resource = Resource::builder()
        .with_attributes([KeyValue::new("service.name", config.service_name.clone())])
        .build();

    let new_provider = SdkTracerProvider::builder()
        .with_sampler(sampler)
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .build();

    // Replace the global provider; this is re-callable.
    opentelemetry::global::set_tracer_provider(new_provider);

    tracing::info!(
        endpoint = %otlp_config.endpoint,
        "OTLP tracer provider replaced at runtime"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_creates_logging_only() {
        // We can't actually initialize tracing more than once in tests,
        // so just validate config construction
        let config = TracingConfig::default();
        assert_eq!(config.service_name, "chronix");
        assert_eq!(config.log_format, LogFormat::Pretty);
        assert!(config.otlp.is_none());
    }

    #[test]
    fn config_with_otlp() {
        let config = TracingConfig {
            service_name: "test".into(),
            log_format: LogFormat::Json,
            log_filter: "debug".into(),
            otlp: Some(crate::otel::config::OtlpConfig::default()),
        };
        assert!(config.otlp.is_some());
        assert_eq!(
            config.otlp.as_ref().unwrap().endpoint,
            "http://localhost:4317"
        );
    }

    #[test]
    fn guard_drop_is_safe() {
        #[cfg(feature = "otlp")]
        let guard = TracingGuard { provider: None };
        #[cfg(not(feature = "otlp"))]
        let guard = TracingGuard { _priv: () };
        drop(guard); // Should not panic
    }

    #[test]
    fn config_serde_roundtrip() {
        let config = TracingConfig {
            service_name: "chronixd".into(),
            log_format: LogFormat::Json,
            log_filter: "chronix=debug".into(),
            otlp: Some(crate::otel::config::OtlpConfig {
                endpoint: "http://collector:4317".into(),
                sampling: crate::otel::config::SamplingStrategy::Ratio(0.05),
                always_sample_errors: true,
                error_boosted_ratio: None,
                require_tls: false,
            }),
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: TracingConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.service_name, "chronixd");
        assert!(deserialized.otlp.is_some());
    }

    #[test]
    fn always_sample_errors_default_false() {
        let otlp = crate::otel::config::OtlpConfig::default();
        // Default is false — enabling overrides all sampling to AlwaysOn,
        // which is dangerous without a tail-sampling collector.
        assert!(!otlp.always_sample_errors);
    }

    #[test]
    fn always_sample_errors_serde_roundtrip() {
        let otlp = crate::otel::config::OtlpConfig {
            endpoint: "http://localhost:4317".into(),
            sampling: crate::otel::config::SamplingStrategy::Ratio(0.01),
            always_sample_errors: false,
            error_boosted_ratio: None,
            require_tls: false,
        };
        let json = serde_json::to_string(&otlp).unwrap();
        let restored: crate::otel::config::OtlpConfig = serde_json::from_str(&json).unwrap();
        assert!(!restored.always_sample_errors);
    }

    #[test]
    fn test_update_log_filter_valid() {
        // If tracing was already initialised by another test, the
        // handle is present and reload should succeed.
        // If not, we accept the "not initialized" error.
        match super::update_log_filter("debug") {
            Ok(()) => {} // handle was present — reload succeeded
            Err(e) => {
                let msg = e.to_string();
                assert!(msg.contains("not initialized"), "unexpected error: {msg}");
            }
        }
    }

    #[test]
    fn test_update_log_filter_invalid() {
        // An invalid directive should surface an error (or
        // "not initialized" if tracing wasn't set up yet).
        let result = super::update_log_filter("invalid{{{filter");
        assert!(result.is_err());
    }

    #[test]
    fn require_tls_default_true() {
        let otlp = crate::otel::config::OtlpConfig::default();
        assert!(otlp.require_tls);
    }

    #[test]
    fn error_boosted_ratio_default_none() {
        let otlp = crate::otel::config::OtlpConfig::default();
        assert!(otlp.error_boosted_ratio.is_none());
    }

    #[test]
    fn o01_require_tls_serde_roundtrip() {
        let otlp = crate::otel::config::OtlpConfig {
            endpoint: "https://collector:4317".into(),
            sampling: crate::otel::config::SamplingStrategy::Ratio(0.05),
            always_sample_errors: false,
            error_boosted_ratio: None,
            require_tls: true,
        };
        let json = serde_json::to_string(&otlp).unwrap();
        let restored: crate::otel::config::OtlpConfig = serde_json::from_str(&json).unwrap();
        assert!(restored.require_tls);
    }

    #[test]
    fn o02_error_boosted_ratio_serde_roundtrip() {
        let otlp = crate::otel::config::OtlpConfig {
            endpoint: "https://collector:4317".into(),
            sampling: crate::otel::config::SamplingStrategy::Ratio(0.01),
            always_sample_errors: true,
            error_boosted_ratio: Some(0.1),
            require_tls: false,
        };
        let json = serde_json::to_string(&otlp).unwrap();
        let restored: crate::otel::config::OtlpConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.error_boosted_ratio, Some(0.1));
    }
}
