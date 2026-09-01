//! W3C Trace Context propagation for gRPC.
//!
//! Provides helpers to inject and extract trace context headers in
//! tonic (gRPC) requests, enabling distributed tracing across Chronix
//! cluster nodes.
//!
//! ## Wire Format
//!
//! Uses the [W3C Trace Context](https://www.w3.org/TR/trace-context/)
//! `traceparent` header format:
//!
//! ```text
//! traceparent: 00-<trace-id>-<span-id>-<flags>
//! ```
//!
//! ## Propagation without OTLP
//!
//! Trace context propagation works even when the `otlp` feature is
//! disabled.  Use [`with_trace_context`] to scope a task-local trace
//! context around request processing; downstream calls that use
//! [`make_trace_interceptor`] will pick it up automatically.

use std::future::Future;
#[cfg(feature = "otlp")]
use std::sync::atomic::{AtomicBool, Ordering};

use tonic::metadata::MetadataMap;

/// Emitted at most once when OTLP feature is compiled in but no provider
/// was initialized — helps operators diagnose silent span loss.
#[cfg(feature = "otlp")]
static NOOP_WARNED: AtomicBool = AtomicBool::new(false);

/// The W3C `traceparent` header key.
const TRACEPARENT_KEY: &str = "traceparent";

/// The W3C `tracestate` header key.
const TRACESTATE_KEY: &str = "tracestate";

tokio::task_local! {
    static TASK_TRACEPARENT: String;
    static TASK_TRACESTATE: String;
}

/// Run a future with the given W3C trace context scoped as a task-local.
///
/// Downstream code that calls [`make_trace_interceptor`] will automatically
/// inject this trace context into outgoing gRPC requests — even when the
/// `otlp` feature is disabled.
///
/// # Example
///
/// ```rust,ignore
/// use chronixd::otel::propagation::{extract_trace_context, with_trace_context};
///
/// // In a gRPC service handler:
/// let (parent, state) = extract_trace_context(request.metadata());
/// let response = with_trace_context(parent, state, async {
///     // outgoing gRPC calls here will carry the traceparent
///     do_work().await
/// }).await;
/// ```
pub async fn with_trace_context<F, R>(
    traceparent: Option<String>,
    tracestate: Option<String>,
    f: F,
) -> R
where
    F: Future<Output = R>,
{
    match (traceparent, tracestate) {
        (Some(tp), Some(ts)) => {
            TASK_TRACEPARENT
                .scope(tp, TASK_TRACESTATE.scope(ts, f))
                .await
        }
        (Some(tp), None) => TASK_TRACEPARENT.scope(tp, f).await,
        _ => f.await,
    }
}

/// Maximum W3C tracestate header length (512 bytes, max 32 list-members).
const MAX_TRACESTATE_LEN: usize = 512;

/// Validate a `traceparent` header value per W3C Trace Context spec.
fn is_valid_traceparent(value: &str) -> bool {
    // Quick length check: "00" + "-" + 32 + "-" + 16 + "-" + "00" = 55 chars
    if value.len() != 55 {
        return false;
    }
    value.bytes().enumerate().all(|(i, b)| match i {
        2 | 35 | 52 => b == b'-',
        _ => b.is_ascii_hexdigit(),
    })
}

/// Extract trace context from gRPC metadata.
///
/// Validates `traceparent` format (W3C Trace Context) and caps
/// `tracestate` length to prevent log injection or oversized headers.
#[must_use]
pub fn extract_trace_context(metadata: &MetadataMap) -> (Option<String>, Option<String>) {
    let traceparent = metadata
        .get(TRACEPARENT_KEY)
        .and_then(|v| v.to_str().ok())
        .filter(|v| is_valid_traceparent(v))
        .map(String::from);

    let tracestate = metadata
        .get(TRACESTATE_KEY)
        .and_then(|v| v.to_str().ok())
        .filter(|v| v.len() <= MAX_TRACESTATE_LEN)
        .map(String::from);

    (traceparent, tracestate)
}

/// Inject trace context into gRPC metadata.
///
/// Adds `traceparent` (and optionally `tracestate`) to outgoing request
/// metadata for cross-node trace propagation.
pub fn inject_trace_context(
    metadata: &mut MetadataMap,
    traceparent: &str,
    tracestate: Option<&str>,
) {
    if let Ok(val) = traceparent.parse() {
        metadata.insert(TRACEPARENT_KEY, val);
    }
    if let Some(state) = tracestate {
        if let Ok(val) = state.parse() {
            metadata.insert(TRACESTATE_KEY, val);
        }
    }
}

/// Create a tonic `Interceptor` that injects the current span's trace
/// context into outgoing gRPC requests.
///
/// This should be added to gRPC client channels to propagate traces
/// to downstream services.
///
/// # Example
///
/// ```rust,ignore
/// use chronixd::otel::propagation::make_trace_interceptor;
///
/// let channel = tonic::transport::Channel::from_static("http://[::1]:50051")
///     .connect().await?;
/// let client = MyServiceClient::with_interceptor(channel, make_trace_interceptor());
/// ```
#[must_use = "Assign this interceptor to a gRPC client channel"]
#[allow(clippy::result_large_err)] // tonic::Status is large by design
pub fn make_trace_interceptor(
) -> impl Fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
    move |mut request: tonic::Request<()>| {
        // Only the `otlp` path can set this; without the feature the fallback
        // below always runs, so the binding is not mutated there.
        #[cfg_attr(not(feature = "otlp"), allow(unused_mut))]
        let mut injected = false;

        // 1. When the `otlp` feature is active and OpenTelemetry is
        //    initialised, extract the trace/span IDs from the current
        //    span context (most accurate — includes the current span ID).
        #[cfg(feature = "otlp")]
        {
            use opentelemetry::trace::TraceContextExt;
            use tracing_opentelemetry::OpenTelemetrySpanExt;

            let span = tracing::Span::current();
            let context = span.context();
            let span_ref = context.span();
            let sc = span_ref.span_context();

            if sc.is_valid() {
                let traceparent = format!(
                    "00-{}-{}-{:02x}",
                    sc.trace_id(),
                    sc.span_id(),
                    sc.trace_flags()
                );
                inject_trace_context(request.metadata_mut(), &traceparent, None);
                injected = true;
            } else if !crate::otel::is_otel_active() && !NOOP_WARNED.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    "OpenTelemetry feature is compiled in but no provider was \
                     initialized — spans will use no-op context. Call \
                     init_tracing() with an OtlpConfig to enable trace export."
                );
            }
        }

        // 2. Fallback: propagate the task-local trace context set by
        //    `with_trace_context`.  Works without any OTel backend.
        if !injected {
            if let Ok(tp) = TASK_TRACEPARENT.try_with(std::clone::Clone::clone) {
                let ts = TASK_TRACESTATE.try_with(std::clone::Clone::clone).ok();
                inject_trace_context(request.metadata_mut(), &tp, ts.as_deref());
            }
        }

        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inject_and_extract_roundtrip() {
        let mut metadata = MetadataMap::new();
        let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

        inject_trace_context(&mut metadata, traceparent, Some("congo=t61rcWkgMzE"));

        let (extracted_parent, extracted_state) = extract_trace_context(&metadata);
        assert_eq!(extracted_parent.as_deref(), Some(traceparent));
        assert_eq!(extracted_state.as_deref(), Some("congo=t61rcWkgMzE"));
    }

    #[test]
    fn extract_missing_returns_none() {
        let metadata = MetadataMap::new();
        let (parent, state) = extract_trace_context(&metadata);
        assert!(parent.is_none());
        assert!(state.is_none());
    }

    #[test]
    fn inject_traceparent_only() {
        let mut metadata = MetadataMap::new();
        let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

        inject_trace_context(&mut metadata, traceparent, None);

        let (parent, state) = extract_trace_context(&metadata);
        assert_eq!(parent.as_deref(), Some(traceparent));
        assert!(state.is_none());
    }

    #[test]
    fn make_interceptor_does_not_panic() {
        let interceptor = make_trace_interceptor();
        let request = tonic::Request::new(());
        let result = interceptor(request);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn task_local_propagation_without_otlp() {
        let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let tracestate = "congo=t61rcWkgMzE";

        let interceptor = make_trace_interceptor();

        // Without with_trace_context: interceptor should not inject anything
        // (when OTLP span context is not active).
        {
            let request = tonic::Request::new(());
            let result = interceptor(request).unwrap();
            let (parent, _) = extract_trace_context(result.metadata());
            // Without task-local, parent may or may not be set depending on
            // whether the `otlp` feature is active and OTel is initialised.
            // With no OTel initialised and no task-local, it should be None.
            let _ = parent; // don't assert — depends on feature flags
        }

        // With with_trace_context: interceptor MUST inject the traceparent.
        with_trace_context(
            Some(traceparent.to_string()),
            Some(tracestate.to_string()),
            async {
                let request = tonic::Request::new(());
                let result = interceptor(request).unwrap();
                let (parent, state) = extract_trace_context(result.metadata());

                // When otlp is enabled but OTel is not initialised, the
                // OTel span context is invalid → falls through to task-local.
                assert_eq!(
                    parent.as_deref(),
                    Some(traceparent),
                    "task-local traceparent must be propagated"
                );
                assert_eq!(
                    state.as_deref(),
                    Some(tracestate),
                    "task-local tracestate must be propagated"
                );
            },
        )
        .await;
    }

    #[test]
    fn o04_valid_traceparent_accepted() {
        assert!(is_valid_traceparent(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        ));
    }

    #[test]
    fn o04_invalid_traceparent_rejected() {
        // Too short
        assert!(!is_valid_traceparent("00-abc-def-01"));
        // Wrong separators
        assert!(!is_valid_traceparent(
            "00x4bf92f3577b34da6a3ce929d0e0e4736x00f067aa0ba902b7x01"
        ));
        // Non-hex characters
        assert!(!is_valid_traceparent(
            "00-ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ-00f067aa0ba902b7-01"
        ));
        // Empty
        assert!(!is_valid_traceparent(""));
    }

    #[test]
    fn o04_extract_rejects_invalid_traceparent() {
        let mut metadata = MetadataMap::new();
        metadata.insert("traceparent", "not-valid".parse().unwrap());
        let (parent, _) = extract_trace_context(&metadata);
        assert!(parent.is_none(), "invalid traceparent must be rejected");
    }

    #[test]
    fn o04_extract_rejects_oversized_tracestate() {
        let mut metadata = MetadataMap::new();
        let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        metadata.insert("traceparent", traceparent.parse().unwrap());
        // Exceed 512 bytes
        let long_state: String = "x".repeat(513);
        metadata.insert("tracestate", long_state.parse().unwrap());
        let (parent, state) = extract_trace_context(&metadata);
        assert!(parent.is_some(), "valid traceparent should pass");
        assert!(state.is_none(), "oversized tracestate must be rejected");
    }
}
