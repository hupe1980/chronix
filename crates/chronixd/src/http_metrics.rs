//! HTTP request duration middleware.
//!
//! Records a Prometheus histogram (`chronix_http_request_duration_seconds`)
//! for every HTTP request. Labels include `method`, `path` (route pattern),
//! and `status` to enable SLO monitoring without high-cardinality blow-up.

use axum::{
    extract::{MatchedPath, Request},
    middleware::Next,
    response::Response,
};

/// Axum middleware that records request duration as a Prometheus histogram.
///
/// The `path` label uses the matched route pattern (e.g. `/api/v1/prom/label/{name}/values`)
/// rather than the fully-resolved URL, keeping label cardinality bounded.
pub async fn request_duration_layer(request: Request, next: Next) -> Response {
    let method = request.method().to_string();

    // Prefer the route pattern registered with axum to avoid high-cardinality
    // label explosion from path parameters / query strings.
    let path = request
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_owned())
        .unwrap_or_else(|| request.uri().path().to_owned());

    let start = tokio::time::Instant::now();
    let response = next.run(request).await;
    let elapsed = start.elapsed().as_secs_f64();

    let status = response.status().as_u16().to_string();

    metrics::histogram!(
        "chronix_http_request_duration_seconds",
        "method" => method,
        "path" => path,
        "status" => status,
    )
    .record(elapsed);

    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::{routing::get, Router};
    use tower::ServiceExt as _;

    #[tokio::test]
    async fn records_histogram_for_request() {
        let app: Router = Router::new()
            .route("/ping", get(|| async { "pong" }))
            .layer(axum::middleware::from_fn(request_duration_layer));

        let req = axum::extract::Request::builder()
            .uri("/ping")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), 200);
    }
}
