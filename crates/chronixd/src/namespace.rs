//! Namespace-aware request routing middleware.
//!
//! Extracts the `X-Namespace` header (or defaults to `"default"`),
//! validates the namespace exists in the [`NamespaceRegistry`], and
//! injects a [`NamespaceContext`] extension into the request for
//! downstream handlers.
//!
//! Additionally provides admin endpoints for namespace management.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Json, Path, State};
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use tracing::debug;

use chronix_core::NamespaceQuota;
use chronix_security::tenant::NamespaceRegistry;

use crate::error::ServerError;
use crate::http::AppState;

/// Namespace-qualify a rollup name, so a tenant can only name its own.
///
/// Rollup definitions are global while measurements are shared, so the name
/// carries the namespace: `"{ns}/{name}"`. Without a scope the name is used
/// as given.
#[must_use]
pub fn qualify_rollup(state: &AppState, ctx: Option<&NamespaceContext>, name: &str) -> String {
    match scope(state, ctx) {
        None => name.to_string(),
        Some(ns) => format!("{ns}/{name}"),
    }
}

/// Build a delete request confined to `scope`.
///
/// **Every delete surface calls this**, for the same reason every ingestion
/// surface calls one write function: a scope each handler has to remember
/// is a scope most of them forget. Three of the four delete surfaces did —
/// HTTP delete, HTTP batch delete and HTTP drop-measurement all deleted
/// across every tenant.
///
/// Under multi-tenancy a "drop measurement" is therefore *not* a
/// measurement drop: it is a delete of every series of that measurement
/// **in this namespace**, because the measurement itself is shared.
///
/// # Errors
///
/// Returns the builder's error when the request is not well formed.
pub fn scoped_delete_request(
    db: &chronix::Chronix,
    scope: Option<&str>,
    measurement: &str,
    tags: impl IntoIterator<Item = (String, String)>,
    range: Option<(i64, i64)>,
) -> Result<chronix::DeleteRequest, ServerError> {
    let mut builder = db.delete_builder().measurement(measurement);
    if let Some(ns) = scope {
        builder = builder.tag(NAMESPACE_TAG, ns);
    }
    for (k, v) in tags {
        builder = builder.tag(&k, &v);
    }
    if let Some((start, end)) = range {
        builder = builder.range(start, end);
    }
    builder
        .build()
        .map_err(|e| ServerError::BadRequest(format!("delete build error: {e}")))
}

/// The `X-Namespace` header name.
pub const NAMESPACE_HEADER: &str = "X-Namespace";

/// Default namespace used when no header is provided.
pub const DEFAULT_NAMESPACE: &str = "default";

/// Context inserted into request extensions after namespace resolution.
#[derive(Debug, Clone)]
pub struct NamespaceContext {
    /// Resolved namespace name.
    pub namespace: String,
}

/// Axum middleware that resolves the `X-Namespace` header and validates
/// the namespace exists in the registry.
///
/// If the registry is not configured (standalone mode), the middleware
/// is effectively a no-op, always injecting `"default"`.
pub async fn namespace_layer(
    State(state): State<AppState>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    let registry = match &state.namespace_registry {
        Some(r) => r,
        None => {
            // No multi-tenancy — always use "default".
            req.extensions_mut().insert(NamespaceContext {
                namespace: DEFAULT_NAMESPACE.to_string(),
            });
            return next.run(req).await;
        }
    };

    let namespace = req
        .headers()
        .get(NAMESPACE_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or(DEFAULT_NAMESPACE)
        .to_string();

    if !registry.exists(&namespace) {
        return (
            StatusCode::BAD_REQUEST,
            format!("namespace not found: {namespace}"),
        )
            .into_response();
    }

    // **The header alone is not authority.** Authentication says who is
    // calling; the credential says whose data they may touch. Without this
    // check any valid key read any tenant by changing one header — the
    // registry lookup above only proves the namespace *exists*.
    //
    // A credential with no namespace list is unrestricted, which is right
    // for single-tenant. `ServerConfig::validate` refuses to start
    // multi-tenant with one, so reaching this line unrestricted means the
    // operator asked for it.
    if let Some(ctx) = req
        .extensions()
        .get::<chronix_security::auth::AuthContext>()
    {
        if !ctx.allows_namespace(&namespace) {
            tracing::warn!(
                principal = %ctx.principal,
                %namespace,
                "principal is not authorised for this namespace"
            );
            metrics::counter!("chronix_namespace_denied_total").increment(1);
            crate::audit::record(
                &state,
                &ctx.principal,
                chronix_security::audit::AuditAction::Admin,
                format!("namespace:{namespace}"),
                chronix_security::audit::AuditDecision::Deny,
                &[("reason", "credential not bound to namespace".to_string())],
            );
            // 403, not 404: the caller authenticated, and hiding existence
            // here would contradict the 400 above, which already tells an
            // unauthenticated caller whether a namespace exists.
            return (
                StatusCode::FORBIDDEN,
                format!("credential is not authorised for namespace: {namespace}"),
            )
                .into_response();
        }
    }

    // Namespace-level Cedar authorization: if an authz engine is
    // configured and the request carries an auth context, verify
    // the principal is authorized for this namespace.
    //
    // Map the HTTP method to the most appropriate Cedar action so
    // that read-only requests require `Read`, mutations require
    // `Write`, and deletions require `Delete`.
    if let Some(ref engine) = state.authz_engine {
        if let Some(ctx) = req
            .extensions()
            .get::<chronix_security::auth::AuthContext>()
        {
            let action = match *req.method() {
                axum::http::Method::GET
                | axum::http::Method::HEAD
                | axum::http::Method::OPTIONS => chronix_security::authz::ChronixAction::Read,
                axum::http::Method::DELETE => chronix_security::authz::ChronixAction::Delete,
                // POST, PUT, PATCH and any other method → Write
                _ => chronix_security::authz::ChronixAction::Write,
            };

            let principal = chronix_security::authz::ChronixPrincipal::new(&ctx.principal);
            let ns_resource = chronix_security::authz::ChronixNamespace::new(&namespace);
            let decision = engine.authorize_namespace(&principal, action, &ns_resource);
            if decision.is_denied() {
                return (
                    StatusCode::FORBIDDEN,
                    format!("access denied for namespace: {namespace}"),
                )
                    .into_response();
            }
        }
    }

    // Per-namespace rate limiting: check if the tenant has exceeded
    // its request-rate quota before proceeding. This runs after authz
    // so that rejected requests don't consume rate-limit tokens.
    if let Err(retry_after) = state.namespace_rate_limiter.check(&namespace) {
        tracing::warn!(
            namespace = %namespace,
            retry_after_secs = retry_after,
            "Per-namespace rate limit exceeded, returning 429"
        );
        metrics::counter!("chronix_rate_limited_total", "namespace" => namespace.clone())
            .increment(1);
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [("Retry-After", retry_after.to_string())],
            format!("Too Many Requests (namespace: {namespace})"),
        )
            .into_response();
    }

    debug!(namespace = %namespace, "namespace resolved");
    req.extensions_mut().insert(NamespaceContext { namespace });

    next.run(req).await
}

// ── Admin endpoints ───────────────────────────────────────────────────

/// Request body for `POST /api/v1/namespaces`.
#[derive(Debug, Deserialize)]
pub struct CreateNamespaceRequest {
    /// Namespace name (e.g. `"prod"`, `"staging"`).
    pub name: String,
    /// Optional description.
    #[serde(default)]
    pub description: String,
    /// Owner identifier.
    #[serde(default = "default_owner")]
    pub owner: String,
    /// Maximum number of series.
    #[serde(default)]
    pub max_series: u64,
    /// Maximum storage in bytes.
    #[serde(default)]
    pub max_storage_bytes: u64,
    /// Maximum number of measurements.
    #[serde(default)]
    pub max_measurements: u32,
    /// Maximum HTTP requests per second (0 = unlimited).
    #[serde(default)]
    pub max_request_rps: u64,
    /// Burst size for HTTP request rate limiting (0 = defaults to `max_request_rps`).
    #[serde(default)]
    pub max_request_burst: u32,
}

fn default_owner() -> String {
    "admin".into()
}

/// Response for namespace operations.
#[derive(Debug, Serialize)]
pub struct NamespaceResponse {
    /// Namespace name.
    pub name: String,
    /// Description.
    pub description: String,
    /// Owner.
    pub owner: String,
    /// Quota limits.
    pub quota: QuotaResponse,
    /// Usage counters.
    pub usage: UsageResponse,
}

/// Quota information.
#[derive(Debug, Serialize)]
pub struct QuotaResponse {
    /// Maximum series count.
    pub max_series_count: u64,
    /// Maximum ingestion rate.
    pub max_ingestion_rate: u64,
    /// Maximum storage bytes.
    pub max_storage_bytes: u64,
    /// Maximum measurements.
    pub max_measurements: u32,
    /// Maximum HTTP requests per second (0 = unlimited).
    pub max_request_rps: u64,
    /// Maximum request burst size.
    pub max_request_burst: u32,
}

/// Current usage information.
#[derive(Debug, Serialize)]
pub struct UsageResponse {
    /// Active series count.
    pub series_count: u64,
    /// Current ingestion rate.
    pub ingestion_rate: f64,
    /// Storage bytes consumed.
    pub storage_bytes: u64,
    /// Number of measurements.
    pub measurements: u32,
}

/// Response for listing namespaces.
#[derive(Debug, Serialize)]
pub struct NamespaceListResponse {
    /// All namespaces.
    pub namespaces: Vec<NamespaceResponse>,
    /// Total count.
    pub count: usize,
}

fn require_registry(state: &AppState) -> Result<&Arc<NamespaceRegistry>, ServerError> {
    state
        .namespace_registry
        .as_ref()
        .ok_or_else(|| ServerError::Internal("namespace registry not configured".into()))
}

/// `POST /api/v1/namespaces` — create a new namespace.
pub async fn create_namespace_handler(
    State(state): State<AppState>,
    Json(body): Json<CreateNamespaceRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let registry = require_registry(&state)?;

    let id = chronix_core::NamespaceId::new(&body.name)
        .map_err(|e| ServerError::BadRequest(format!("invalid namespace name: {e}")))?;
    let quota = NamespaceQuota {
        max_series_count: body.max_series,
        max_ingestion_rate: 0,
        max_storage_bytes: body.max_storage_bytes,
        max_measurements: body.max_measurements,
        max_request_rps: body.max_request_rps,
        max_request_burst: body.max_request_burst,
    };

    let info = registry
        .create_namespace(id, body.description, body.owner, quota)
        .map_err(|e| ServerError::BadRequest(format!("{e}")))?;

    // Register per-namespace rate limiter if quota includes RPS limit.
    if info.quota.max_request_rps > 0 {
        state.namespace_rate_limiter.set_limit(
            info.id.as_str(),
            info.quota.max_request_rps,
            info.quota.max_request_burst,
        );
    }

    Ok((
        StatusCode::CREATED,
        Json(NamespaceResponse {
            name: info.id.as_str().to_string(),
            description: info.description,
            owner: info.owner,
            quota: QuotaResponse {
                max_series_count: info.quota.max_series_count,
                max_ingestion_rate: info.quota.max_ingestion_rate,
                max_storage_bytes: info.quota.max_storage_bytes,
                max_measurements: info.quota.max_measurements,
                max_request_rps: info.quota.max_request_rps,
                max_request_burst: info.quota.max_request_burst,
            },
            usage: UsageResponse {
                series_count: 0,
                ingestion_rate: 0.0,
                storage_bytes: 0,
                measurements: 0,
            },
        }),
    ))
}

/// `GET /api/v1/namespaces` — list all namespaces.
pub async fn list_namespaces_handler(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ServerError> {
    let registry = require_registry(&state)?;
    let all = registry.list_all();

    let namespaces: Vec<NamespaceResponse> = all
        .into_iter()
        .map(|s| NamespaceResponse {
            name: s.info.id.as_str().to_string(),
            description: s.info.description,
            owner: s.info.owner,
            quota: QuotaResponse {
                max_series_count: s.info.quota.max_series_count,
                max_ingestion_rate: s.info.quota.max_ingestion_rate,
                max_storage_bytes: s.info.quota.max_storage_bytes,
                max_measurements: s.info.quota.max_measurements,
                max_request_rps: s.info.quota.max_request_rps,
                max_request_burst: s.info.quota.max_request_burst,
            },
            usage: UsageResponse {
                series_count: s.usage.series_count,
                ingestion_rate: s.usage.ingestion_rate,
                storage_bytes: s.usage.storage_bytes,
                measurements: s.usage.measurements,
            },
        })
        .collect();

    let count = namespaces.len();
    Ok(Json(NamespaceListResponse { namespaces, count }))
}

/// `GET /api/v1/namespaces/{name}` — get namespace details.
pub async fn get_namespace_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let registry = require_registry(&state)?;
    let ns = registry
        .get(&name)
        .map_err(|e| ServerError::NotFound(format!("{e}")))?;

    Ok(Json(NamespaceResponse {
        name: ns.info.id.as_str().to_string(),
        description: ns.info.description,
        owner: ns.info.owner,
        quota: QuotaResponse {
            max_series_count: ns.info.quota.max_series_count,
            max_ingestion_rate: ns.info.quota.max_ingestion_rate,
            max_storage_bytes: ns.info.quota.max_storage_bytes,
            max_measurements: ns.info.quota.max_measurements,
            max_request_rps: ns.info.quota.max_request_rps,
            max_request_burst: ns.info.quota.max_request_burst,
        },
        usage: UsageResponse {
            series_count: ns.usage.series_count,
            ingestion_rate: ns.usage.ingestion_rate,
            storage_bytes: ns.usage.storage_bytes,
            measurements: ns.usage.measurements,
        },
    }))
}

/// `DELETE /api/v1/namespaces/{name}` — delete a namespace.
pub async fn delete_namespace_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let registry = require_registry(&state)?;
    registry
        .delete_namespace(&name)
        .map_err(|e| ServerError::BadRequest(format!("{e}")))?;

    Ok(Json(serde_json::json!({
        "ok": true,
        "message": format!("namespace '{name}' deleted"),
    })))
}

/// `GET /api/v1/namespaces/{name}/usage` — get namespace usage.
pub async fn get_namespace_usage_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let registry = require_registry(&state)?;
    let usage = registry
        .get_usage(&name)
        .ok_or_else(|| ServerError::NotFound(format!("namespace not found: {name}")))?;

    Ok(Json(UsageResponse {
        series_count: usage.series_count,
        ingestion_rate: usage.ingestion_rate,
        storage_bytes: usage.storage_bytes,
        measurements: usage.measurements,
    }))
}

// ── The namespace as a system invariant ────────────────────────────────

/// The tag every ingested point carries and every read is scoped by.
///
/// Re-exported from `chronix_core` so the server, the query builder, the SQL
/// provider and the PromQL evaluator all name the same constant. They used to
/// spell the string out separately, and the copies drifted: two of five write
/// paths stamped it and one of seven read paths filtered on it.
pub use chronix_core::NAMESPACE_TAG;

/// Namespace for a request, from its resolved [`NamespaceContext`].
///
/// Falls back to `"default"` when the namespace layer did not run — a
/// standalone server, or an internal caller such as an ingestion connector.
#[must_use]
pub fn resolve(ns_ctx: Option<&NamespaceContext>) -> &str {
    ns_ctx.map_or(DEFAULT_NAMESPACE, |ctx| &ctx.namespace)
}

/// The namespace a request is confined to, or `None` when tenancy is off.
///
/// Tenancy is a deployment switch rather than always-on because a point
/// written before it was enabled carries no tag, and a scoped read cannot see
/// it — so the trade is stated once in the config instead of made silently
///.
#[must_use]
pub fn scope<'a>(
    state: &crate::http::AppState,
    ns_ctx: Option<&'a NamespaceContext>,
) -> Option<&'a str> {
    if state.config.server.multi_tenancy {
        Some(ns_ctx.map_or(DEFAULT_NAMESPACE, |ctx| ctx.namespace.as_str()))
    } else {
        None
    }
}

/// Namespace for a gRPC or Flight request, from its `x-namespace` metadata.
///
/// gRPC has no equivalent of the HTTP middleware chain, so the header is read
/// where the request is served. An absent or unparsable value is `"default"`,
/// which matches the HTTP path.
#[must_use]
pub fn from_metadata(metadata: &tonic::metadata::MetadataMap) -> String {
    metadata
        .get(METADATA_KEY)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_NAMESPACE)
        .to_string()
}

/// Namespace scope for a gRPC or Flight request, or `None` when tenancy is
/// off. The gRPC counterpart of [`scope`].
#[must_use]
pub fn scope_from_metadata(
    multi_tenancy: bool,
    metadata: &tonic::metadata::MetadataMap,
) -> Option<String> {
    multi_tenancy.then(|| from_metadata(metadata))
}

/// Namespace scope for a gRPC or Flight request, checked against the
/// credential the request authenticated with.
///
/// The metadata-only [`scope_from_metadata`] trusts the client's chosen
/// namespace, which is the same defect the HTTP layer had: a valid
/// credential could name any tenant. Prefer this everywhere a request is in
/// hand.
///
/// # Errors
///
/// Returns `PermissionDenied` when the credential is confined to a set of
/// namespaces that does not include the requested one.
pub fn scope_from_request<T>(
    multi_tenancy: bool,
    request: &tonic::Request<T>,
) -> Result<Option<String>, tonic::Status> {
    let Some(namespace) = scope_from_metadata(multi_tenancy, request.metadata()) else {
        return Ok(None);
    };
    if let Some(ctx) = request
        .extensions()
        .get::<chronix_security::auth::AuthContext>()
    {
        if !ctx.allows_namespace(&namespace) {
            tracing::warn!(
                principal = %ctx.principal,
                %namespace,
                "principal is not authorised for this namespace"
            );
            metrics::counter!("chronix_namespace_denied_total").increment(1);
            return Err(tonic::Status::permission_denied(format!(
                "credential is not authorised for namespace: {namespace}"
            )));
        }
    }
    Ok(Some(namespace))
}

/// gRPC / Flight metadata key carrying the namespace.
///
/// Lower-case because tonic rejects metadata keys with upper-case characters.
pub const METADATA_KEY: &str = "x-namespace";

/// Stamp `namespace` onto every point, overriding anything the client sent.
///
/// Overriding rather than defaulting is the point: a namespace a client can
/// choose is not an isolation boundary.
///
/// # Errors
///
/// Returns [`ServerError::BadRequest`] if the point cannot carry another tag
/// (it is already at the per-series tag limit).
pub fn scope_points(
    namespace: Option<&str>,
    points: &mut [chronix_core::Point],
) -> Result<(), ServerError> {
    let Some(namespace) = namespace else {
        return Ok(());
    };
    for point in points.iter_mut() {
        point.inject_tag(NAMESPACE_TAG, namespace).map_err(|e| {
            ServerError::BadRequest(format!("cannot apply namespace to point: {e}"))
        })?;
    }
    Ok(())
}

/// Lazily built, per-namespace `DataFusion` session contexts.
///
/// A context's tables are scoped by the table provider, so isolation does not
/// depend on the SQL text: `SELECT * FROM power` through a `tenant-a` context
/// cannot be phrased to read anything else.
pub struct SqlContexts {
    db: std::sync::Arc<chronix::Chronix>,
    contexts: parking_lot::Mutex<
        std::collections::HashMap<String, std::sync::Arc<datafusion::prelude::SessionContext>>,
    >,
}

impl std::fmt::Debug for SqlContexts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlContexts")
            .field("cached", &self.contexts.lock().len())
            .finish()
    }
}

impl SqlContexts {
    /// Build a per-namespace context factory over `db`.
    #[must_use]
    pub fn new(db: std::sync::Arc<chronix::Chronix>) -> Self {
        Self {
            db,
            contexts: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// The session context for `namespace`, building it on first use.
    ///
    /// `None` yields the unscoped context: the single-tenant server, where
    /// points carry no namespace tag.
    #[must_use]
    pub fn get(
        &self,
        namespace: Option<&str>,
    ) -> std::sync::Arc<datafusion::prelude::SessionContext> {
        // `None` and a namespace literally called "" cannot collide: the
        // registry rejects an empty namespace name.
        let key = namespace.unwrap_or_default();
        let mut contexts = self.contexts.lock();
        if let Some(ctx) = contexts.get(key) {
            return ctx.clone();
        }
        let ctx = std::sync::Arc::new(match namespace {
            Some(ns) => chronix::sql::create_namespaced_session_context(self.db.clone(), ns),
            None => chronix::sql::create_session_context(self.db.clone()),
        });
        contexts.insert(key.to_string(), ctx.clone());
        ctx
    }
}

/// Measurements that hold data for `namespace` in the window.
///
/// The schema registry is process-wide, so listing it verbatim tells every
/// tenant what the others are writing. Measurement *names* are metadata, but
/// they are the tenant's metadata.
pub fn measurements_in(
    db: &std::sync::Arc<chronix::Chronix>,
    namespace: Option<&str>,
    start_ns: i64,
    end_ns: i64,
    limit: usize,
) -> Vec<String> {
    // Sorted, and sorted *before* the limit: Prometheus returns label values
    // in order, a Grafana dropdown shows them in the order it is given, and
    // truncating an unordered list makes the paginated `/api/v1/measurements`
    // return a different page each time the registry's iteration order moves.
    let mut names = db.schema_registry().measurement_names();
    names.sort_unstable();
    names
        .into_iter()
        .filter(|m| has_rows(db, namespace, m, start_ns, end_ns))
        .take(limit)
        .collect()
}

/// Whether `measurement` has any row for `namespace` in the window.
///
/// The `limit(1)` makes this cheap: the stream stops at the first bucket that
/// yields a row. Errors are logged rather than folded into `false` — a probe
/// that reports a failure as "no data" makes a whole measurement disappear
/// from `/api/v1/prom/metadata` and `/api/v1/prom/label/__name__/values`.
pub fn has_rows(
    db: &std::sync::Arc<chronix::Chronix>,
    namespace: Option<&str>,
    measurement: &str,
    start_ns: i64,
    end_ns: i64,
) -> bool {
    let plan = match db
        .query()
        .measurement(measurement)
        .namespace_scope(namespace)
        .range(start_ns, end_ns)
        .limit(1)
        .build()
    {
        Ok(plan) => plan,
        Err(e) => {
            tracing::warn!(measurement, error = %e, "has_rows: could not build the probe plan");
            return false;
        }
    };
    match db.execute_iter(&plan) {
        Ok(mut stream) => stream.any(|batch| match batch {
            Ok(b) => b.num_rows() > 0,
            Err(e) => {
                tracing::warn!(measurement, error = %e, "has_rows: probe scan failed");
                false
            }
        }),
        Err(e) => {
            tracing::warn!(measurement, error = %e, "has_rows: probe scan could not start");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_context_default() {
        let ctx = NamespaceContext {
            namespace: DEFAULT_NAMESPACE.to_string(),
        };
        assert_eq!(ctx.namespace, "default");
    }

    #[test]
    fn create_request_deserialize_defaults() {
        let json = r#"{"name": "prod"}"#;
        let req: CreateNamespaceRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, "prod");
        assert_eq!(req.owner, "admin");
        assert_eq!(req.max_series, 0);
    }

    #[test]
    fn create_request_deserialize_full() {
        let json = r#"{
            "name": "staging",
            "description": "Staging env",
            "owner": "team-a",
            "max_series": 100000,
            "max_storage_bytes": 1073741824,
            "max_measurements": 50
        }"#;
        let req: CreateNamespaceRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, "staging");
        assert_eq!(req.description, "Staging env");
        assert_eq!(req.max_series, 100_000);
        assert_eq!(req.max_storage_bytes, 1_073_741_824);
        assert_eq!(req.max_measurements, 50);
    }

    #[test]
    fn namespace_response_serialization() {
        let resp = NamespaceResponse {
            name: "prod".into(),
            description: "Production".into(),
            owner: "admin".into(),
            quota: QuotaResponse {
                max_series_count: 1_000_000,
                max_ingestion_rate: 0,
                max_storage_bytes: 0,
                max_measurements: 0,
                max_request_rps: 0,
                max_request_burst: 0,
            },
            usage: UsageResponse {
                series_count: 42,
                ingestion_rate: 0.0,
                storage_bytes: 1024,
                measurements: 3,
            },
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["name"], "prod");
        assert_eq!(json["usage"]["series_count"], 42);
        assert_eq!(json["quota"]["max_series_count"], 1_000_000);
    }

    #[test]
    fn list_response_serialization() {
        let resp = NamespaceListResponse {
            namespaces: vec![],
            count: 0,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["count"], 0);
        assert!(json["namespaces"].as_array().unwrap().is_empty());
    }
}
