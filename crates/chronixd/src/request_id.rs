//! Request-ID middleware for HTTP endpoints.
//!
//! For every incoming request the middleware either extracts an existing
//! `X-Request-Id` header (forwarded by upstream proxies / load-balancers)
//! or generates a new UUID v4. The ID is:
//!
//! 1. Inserted into the current [`tracing::Span`] as the `request_id` field.
//! 2. Echoed back in the `X-Request-Id` response header.
//!
//! This allows structured logs, metrics, and audit events to be correlated
//! to a single request end-to-end.

use axum::{extract::Request, http::HeaderValue, middleware::Next, response::Response};
use tracing::Span;

/// The canonical header name used for request IDs.
pub const REQUEST_ID_HEADER: &str = "x-request-id";

/// Maximum length for caller-supplied request IDs (matches Envoy/Istio).
const MAX_REQUEST_ID_LEN: usize = 128;

/// Axum middleware that assigns a unique request ID to every request.
///
/// If the incoming request already contains a valid `X-Request-Id` header
/// its value is reused; otherwise a new UUID v4 is generated.
///
/// The caller-supplied value is validated for length (≤128 chars)
/// and charset (alphanumeric, `-`, `_`, `.`, `~`, `:`) to prevent log
/// injection and DoS via oversized headers.
pub async fn request_id_layer(request: Request, next: Next) -> Response {
    // Prefer the caller-supplied ID so that requests can be traced across
    // multiple hops, but only if it passes validation.
    let request_id = request
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .filter(|s| s.len() <= MAX_REQUEST_ID_LEN)
        .filter(|s| {
            s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.~:".contains(&b))
        })
        .map(String::from)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    // Record the ID in the current tracing span so that every log line
    // emitted while processing this request carries the correlation ID.
    Span::current().record("request_id", request_id.as_str());

    let mut response = next.run(request).await;

    // Echo the ID back to the caller.
    if let Ok(val) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert(REQUEST_ID_HEADER, val);
    }

    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt;

    async fn ok_handler() -> &'static str {
        "ok"
    }

    fn test_router() -> Router {
        Router::new()
            .route("/test", get(ok_handler))
            .layer(axum::middleware::from_fn(request_id_layer))
    }

    #[tokio::test]
    async fn generates_request_id_when_missing() {
        let app = test_router();
        let req = Request::builder().uri("/test").body(Body::empty()).unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert!(resp.headers().contains_key(REQUEST_ID_HEADER));
        let id = resp.headers()[REQUEST_ID_HEADER].to_str().unwrap();
        // UUID v4 is 36 chars (8-4-4-4-12)
        assert_eq!(id.len(), 36, "expected UUID v4 format, got: {id}");
    }

    #[tokio::test]
    async fn preserves_caller_supplied_request_id() {
        let app = test_router();
        let req = Request::builder()
            .uri("/test")
            .header(REQUEST_ID_HEADER, "my-custom-id-123")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        let id = resp.headers()[REQUEST_ID_HEADER].to_str().unwrap();
        assert_eq!(id, "my-custom-id-123");
    }
}
