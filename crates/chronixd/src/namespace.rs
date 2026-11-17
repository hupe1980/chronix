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

/// Whether `path` is a control-plane route rather than a data one.
///
/// The two are authorized differently — a capability on
/// `Chronix::System` versus an action on a `Chronix::Namespace` — and this
/// is the only place the split is expressed. `every_route_is_gated` walks
/// the router and checks that each path this returns `true` for is in fact
/// behind a capability layer, so the two cannot drift.
///
/// Health, readiness, metrics, the OpenAPI document and the console's static
/// shell are neither: they carry no tenant data, and a liveness probe that fails because a policy
/// file changed is an outage a policy file should not be able to cause.
#[must_use]
pub fn is_control_plane(path: &str) -> bool {
    matches!(
        path,
        "/health" | "/healthz" | "/ready" | "/readyz" | "/metrics"
    ) || path == "/api/v1/openapi.json"
        // The console's static shell: identical for every caller, no data.
        || path == "/console"
        || path.starts_with("/console/")
        || path.starts_with("/api/v1/admin")
        || path.starts_with("/api/v1/namespaces")
}

/// The action a data request asks for, from the **route** rather than the
/// method.
///
/// The method is a convention this server breaks in both directions:
/// `POST /api/v1/chronix/sql`, `POST /api/v1/chronix/query` and
/// `POST /api/v1/query` are reads (Grafana POSTs PromQL by default), and the
/// bulk delete routes are `POST` but are deletes. Classifying by method would
/// force a read-only data source to hold `Write` and let any writer delete.
///
/// Every data route is classified here explicitly, and `_ => None` is
/// deliberate: `every_data_route_is_classified` walks the router and fails on
/// a route this does not name. The path is the matched route pattern
/// (`/api/v1/measurements/{name}`), not the request path, so a measurement
/// name cannot reach a different arm.
#[must_use]
pub fn action_for_route(
    path: &str,
    method: &axum::http::Method,
) -> Option<chronix_security::authz::ChronixAction> {
    use axum::http::Method;
    use chronix_security::authz::ChronixAction::{Delete, Read, Write};

    // A safe reading of the method, for routes whose effect follows it.
    let by_method = || match *method {
        Method::GET | Method::HEAD | Method::OPTIONS => Read,
        Method::DELETE => Delete,
        _ => Write,
    };

    Some(match path {
        // ── Reads that are POSTs ────────────────────────────────────
        // Chronix's own query API takes a JSON body, and the Prometheus API
        // accepts both methods because Grafana sends a form body by default.
        "/api/v1/chronix/query"
        | "/api/v1/chronix/query/explain"
        | "/api/v1/chronix/sql"
        | "/api/v1/chronix/promql/explain"
        // MCP: every tool reads, whatever the method carries the message.
        | "/mcp"
        | "/api/v1/query"
        | "/api/v1/query_range"
        | "/api/v1/labels"
        | "/api/v1/label/{name}/values"
        | "/api/v1/series"
        | "/api/v1/prom/query"
        | "/api/v1/prom/query_range"
        | "/api/v1/prom/labels"
        | "/api/v1/prom/label/{name}/values"
        | "/api/v1/prom/series"
        | "/api/v1/prom/read" => Read,

        // Reading rows out to a file is still reading them; the *egress* is
        // what the `data_export` audit record is for.
        "/api/v1/export/parquet" => Read,

        // ── Deletes that are POSTs ──────────────────────────────────
        "/api/v1/delete" | "/api/v1/delete_batch" => Delete,

        // ── Reads, by method ────────────────────────────────────────
        "/api/v1/metadata"
        | "/api/v1/prom/metadata"
        | "/api/v1/status/buildinfo"
        | "/api/v1/status/tsdb"
        | "/api/v1/rules"
        | "/api/v1/alerts"
        | "/api/v1/query_exemplars"
        | "/api/v1/measurements"
        | "/api/v1/measurements/{name}/schema"
        | "/api/v1/signals"
        | "/api/v1/annotations"
        | "/api/v1/annotations/stream"
        | "/api/v1/cdc/stream"
        | "/api/v1/dashboards/export"
        | "/api/v1/connectors" => Read,

        // ── Writes ──────────────────────────────────────────────────
        "/api/v1/write"
        | "/write"
        | "/api/v2/write"
        | "/api/v1/write/influx"
        | "/api/v1/prom/write"
        | "/v1/metrics"
        | "/api/v1/otlp/metrics"
        | "/api/v1/measurements/{name}/schema/fields"
        | "/api/v1/measurements/{name}/restore"
        | "/api/v1/rollups/{name}/refresh" => Write,

        // ── Both, by method ─────────────────────────────────────────
        // `GET` lists, `POST` creates, `DELETE` removes — the method is the
        // effect here, so reading it is right rather than convenient.
        "/api/v1/measurements/{name}"
        | "/api/v1/rollups"
        | "/api/v1/rollups/{name}"
        | "/api/v1/detectors"
        | "/api/v1/detectors/{name}"
        | "/api/v1/triggers"
        | "/api/v1/triggers/{name}" => by_method(),

        _ => return None,
    })
}

/// The audit category a refused data action is recorded under.
///
/// The two enums are separate on purpose — one is what a policy decides
/// about, the other is what a trail is searched by — so the mapping is
/// written down once, here, exhaustively.
#[must_use]
pub fn audit_action_for(
    action: chronix_security::authz::ChronixAction,
) -> chronix_security::audit::AuditAction {
    use chronix_security::authz::ChronixAction;
    match action {
        ChronixAction::Read => chronix_security::audit::AuditAction::Read,
        ChronixAction::Write => chronix_security::audit::AuditAction::Write,
        ChronixAction::Delete => chronix_security::audit::AuditAction::Delete,
        // The administrative gate records its own refusals; a data action is
        // all that reaches here, and `is_administrative` is checked above it.
        _ => chronix_security::audit::AuditAction::Admin,
    }
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

/// Resolve the request's namespace, then decide whether it may act in it.
///
/// **The data-plane gate.** In this order:
///
/// 1. the namespace is resolved from `X-Namespace`, defaulting to `default`,
///    and must exist in the registry;
/// 2. the **credential** must be bound to it (a header is not authority);
/// 3. when a policy engine is configured, it must permit the action the HTTP
///    method asks for.
///
/// The registry is not optional, so there is no path that skips the gate. A
/// namespace always exists: `default` is the one every point is written under,
/// and both `NamespaceRegistry::new` and `open` create it.
pub async fn namespace_layer(
    State(state): State<AppState>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    let namespace = req
        .headers()
        .get(NAMESPACE_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_NAMESPACE)
        .to_string();

    // The control plane is not *in* a namespace, so none of the three checks
    // below apply to it: it is authorized by the capability its route group
    // asks for, on `Chronix::System`.
    //
    // The credential binding is the one that bites if this is forgotten.
    // `validate_tenancy` requires every key to name its namespaces under
    // multi-tenancy — administrative keys included — so an admin key bound to
    // `tenant-a` was refused on every administrative endpoint unless it also
    // listed `default`, which is the namespace an admin request does not
    // have and nothing tells you to add.
    //
    // The metrics path is configurable, so the pure predicate cannot know it;
    // an operator who moves the scrape must not thereby put it behind a data
    // policy.
    let path = req.uri().path().to_string();
    if is_control_plane(&path) || path == state.config.server.metrics_path {
        req.extensions_mut().insert(NamespaceContext { namespace });
        return next.run(req).await;
    }

    if !state.namespace_registry.exists(&namespace) {
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
        && !ctx.allows_namespace(&namespace)
    {
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

    // Namespace-level Cedar authorization.
    if let Some(engine) = state.authz_engine.as_ref()
        && let Some(ctx) = req
            .extensions()
            .get::<chronix_security::auth::AuthContext>()
    {
        // The **matched route**, so `/api/v1/measurements/{name}` is one
        // arm whatever the measurement is called. A request that matched
        // no route cannot reach a handler, so an unclassified one is a
        // refusal rather than a guess.
        let matched = req
            .extensions()
            .get::<axum::extract::MatchedPath>()
            .map(|m| m.as_str().to_string());
        let Some(action) = matched
            .as_deref()
            .and_then(|p| action_for_route(p, req.method()))
        else {
            tracing::warn!(
                path = %path,
                method = %req.method(),
                "no data action is classified for this route; refusing"
            );
            return (
                StatusCode::FORBIDDEN,
                "this route has no authorization classification".to_string(),
            )
                .into_response();
        };
        // `ctx.principal()` rather than a principal built here: roles
        // come with the credential. This built a bare
        // `ChronixPrincipal::new(&ctx.principal)` with no roles at all,
        // so every `principal in Chronix::Role::"…"` policy — which is
        // every policy in the guide — matched nothing on a data request
        // while the same role worked on an administrative one.
        let ns_resource = chronix_security::authz::ChronixNamespace::new(&namespace);
        let decision = engine.authorize_namespace(&ctx.principal(), action, &ns_resource);
        if decision.is_denied() {
            tracing::warn!(
                principal = %ctx.principal,
                %namespace,
                %action,
                "namespace access denied by policy"
            );
            // **A refusal is the event the trail exists for.** The
            // credential-binding denial above was recorded and this one
            // was not, so a policy denial left a `warn!` in the process
            // log — which does not survive a restart and cannot be shown
            // to be unedited. One gate had the rule and the other did not.
            crate::audit::record(
                &state,
                &ctx.principal,
                audit_action_for(action),
                namespace.clone(),
                chronix_security::audit::AuditDecision::Deny,
                &[("reason", "denied by policy".to_string())],
            );
            metrics::counter!("chronix_authz_denied_total").increment(1);
            return (
                StatusCode::FORBIDDEN,
                format!("access denied: {action} on namespace {namespace}"),
            )
                .into_response();
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
///
/// Every quota is optional, and `0` — the default — means unlimited, so a
/// namespace created without a limit is unlimited rather than unwritable.
/// An unknown field is refused: a misspelt quota would otherwise create an
/// unlimited namespace without a word.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateNamespaceRequest {
    /// Namespace name (e.g. `"prod"`, `"staging"`).
    pub name: String,
    /// Optional description.
    #[serde(default)]
    pub description: String,
    /// Owner identifier.
    #[serde(default = "default_owner")]
    pub owner: String,
    /// Maximum number of distinct series (`0` = unlimited).
    #[serde(default)]
    pub max_series: u64,
    /// Maximum points per second (`0` = unlimited).
    #[serde(default)]
    pub max_ingestion_rate: u64,
    /// Maximum number of measurements (`0` = unlimited).
    #[serde(default)]
    pub max_measurements: u32,
    /// Maximum requests per second (`0` = unlimited).
    #[serde(default)]
    pub max_request_rps: u64,
    /// Burst size for request rate limiting (`0` = `max_request_rps`).
    #[serde(default)]
    pub max_request_burst: u32,
    /// Removed — present only so a request carrying it is refused by name.
    #[serde(default)]
    max_storage_bytes: Option<serde_json::Value>,
}

fn default_owner() -> String {
    "admin".into()
}

/// Refuse a request that still sets the removed storage quota.
fn refuse_storage_quota(field: Option<&serde_json::Value>) -> Result<(), ServerError> {
    if field.is_some() {
        return Err(ServerError::InvalidQuota(
            "max_storage_bytes was removed: namespaces share segments, so per-namespace bytes \
             cannot be measured; use retention to bound disk"
                .to_string(),
        ));
    }
    Ok(())
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
    /// Usage, exact, computed on request.
    pub usage: UsageResponse,
}

/// Quota information. `0` means unlimited.
#[derive(Debug, Serialize)]
pub struct QuotaResponse {
    /// Maximum series count.
    pub max_series_count: u64,
    /// Maximum points per second.
    pub max_ingestion_rate: u64,
    /// Maximum measurements.
    pub max_measurements: u32,
    /// Maximum requests per second.
    pub max_request_rps: u64,
    /// Maximum request burst size.
    pub max_request_burst: u32,
}

impl From<&NamespaceQuota> for QuotaResponse {
    fn from(q: &NamespaceQuota) -> Self {
        Self {
            max_series_count: q.max_series_count,
            max_ingestion_rate: q.max_ingestion_rate,
            max_measurements: q.max_measurements,
            max_request_rps: q.max_request_rps,
            max_request_burst: q.max_request_burst,
        }
    }
}

/// A namespace's usage.
///
/// `series` and `measurements` are the database's own counts — the set new
/// series are admitted against, so they cannot drift from what is enforced;
/// `ingestion_rate` is the points admitted over the last whole second.
#[derive(Debug, Serialize)]
pub struct UsageResponse {
    /// Distinct series in the namespace.
    pub series: u64,
    /// Measurements the namespace has written to and not dropped.
    pub measurements: u32,
    /// Points per second admitted over the last whole second.
    pub ingestion_rate: u64,
}

/// `GET /api/v1/namespaces/{name}/usage`: usage beside the quota it is
/// measured against.
#[derive(Debug, Serialize)]
pub struct UsageReport {
    /// The usage.
    #[serde(flatten)]
    pub usage: UsageResponse,
    /// The quota.
    pub quota: QuotaResponse,
}

fn usage_of(state: &AppState, namespace: &str) -> UsageResponse {
    let usage = state.db.namespace_usage(namespace);
    UsageResponse {
        series: usage.series,
        measurements: usage.measurements,
        ingestion_rate: state.namespace_ingest_limiter.observed_rate(namespace),
    }
}

/// Put a namespace's quota in force everywhere it is enforced: the database
/// (series and measurements), the request limiter, the ingestion limiter.
///
/// The one function create, update and startup all call, so a quota cannot
/// be persisted on one path and applied on another.
pub fn apply_quota(
    db: &chronix::Chronix,
    requests: &crate::rate_limit::NamespaceRateLimiter,
    ingest: &crate::rate_limit::NamespaceIngestLimiter,
    namespace: &str,
    quota: &NamespaceQuota,
) {
    if quota.max_series_count == 0 && quota.max_measurements == 0 {
        db.clear_namespace_limits(namespace);
    } else {
        let unlimited_if_zero = |v: u64| if v == 0 { u64::MAX } else { v };
        db.set_namespace_limits(
            namespace,
            chronix::NamespaceLimits {
                max_series: unlimited_if_zero(quota.max_series_count),
                max_measurements: if quota.max_measurements == 0 {
                    u32::MAX
                } else {
                    quota.max_measurements
                },
            },
        );
    }
    requests.set_limit(namespace, quota.max_request_rps, quota.max_request_burst);
    ingest.set_limit(namespace, quota.max_ingestion_rate);
}

fn apply(state: &AppState, namespace: &str, quota: &NamespaceQuota) {
    apply_quota(
        &state.db,
        &state.namespace_rate_limiter,
        &state.namespace_ingest_limiter,
        namespace,
        quota,
    );
}

/// Response for listing namespaces.
#[derive(Debug, Serialize)]
pub struct NamespaceListResponse {
    /// All namespaces.
    pub namespaces: Vec<NamespaceResponse>,
    /// Total count.
    pub count: usize,
}

/// The namespace registry.
fn registry(state: &AppState) -> &Arc<NamespaceRegistry> {
    &state.namespace_registry
}

/// Classify a registry error for the wire.
///
/// Every `TenantError` reached the caller as `400 BAD_REQUEST`, so a full
/// disk was reported as *invalid namespace config* — a message that sends an
/// operator to re-read their request body while the problem is the volume.
/// A persistence failure is the **server's**; everything else here is the
/// caller's.
fn tenant_error(e: chronix_security::tenant::TenantError) -> ServerError {
    match e {
        chronix_security::tenant::TenantError::Persist(detail) => ServerError::Internal(detail),
        other => ServerError::BadRequest(other.to_string()),
    }
}

fn namespace_response(
    state: &AppState,
    info: chronix_security::tenant::NamespaceInfo,
) -> NamespaceResponse {
    let usage = usage_of(state, info.id.as_str());
    NamespaceResponse {
        name: info.id.as_str().to_string(),
        description: info.description,
        owner: info.owner,
        quota: QuotaResponse::from(&info.quota),
        usage,
    }
}

/// `POST /api/v1/namespaces` — create a new namespace.
pub async fn create_namespace_handler(
    State(state): State<AppState>,
    req_extensions: axum::http::Extensions,
    Json(body): Json<CreateNamespaceRequest>,
) -> Result<impl IntoResponse, ServerError> {
    refuse_storage_quota(body.max_storage_bytes.as_ref())?;
    let registry = registry(&state);

    let id = chronix_core::NamespaceId::new(&body.name)
        .map_err(|e| ServerError::BadRequest(format!("invalid namespace name: {e}")))?;
    let quota = NamespaceQuota {
        max_series_count: body.max_series,
        max_ingestion_rate: body.max_ingestion_rate,
        max_measurements: body.max_measurements,
        max_request_rps: body.max_request_rps,
        max_request_burst: body.max_request_burst,
    };
    quota.validate().map_err(ServerError::InvalidQuota)?;

    let info = registry
        .create_namespace(id, body.description, body.owner, quota)
        .map_err(tenant_error)?;
    apply(&state, info.id.as_str(), &info.quota);

    // Creating a tenant is a permission change in everything but name, and
    // it was recorded nowhere: `NamespaceCreate` existed as a category and
    // was constructed by nothing.
    crate::audit::record(
        &state,
        &crate::audit::principal_of(&req_extensions),
        chronix_security::audit::AuditAction::NamespaceCreate,
        info.id.as_str().to_string(),
        chronix_security::audit::AuditDecision::Allow,
        &[
            ("owner", info.owner.clone()),
            ("max_series", info.quota.max_series_count.to_string()),
        ],
    );

    Ok((StatusCode::CREATED, Json(namespace_response(&state, info))))
}

/// `GET /api/v1/namespaces` — list all namespaces.
pub async fn list_namespaces_handler(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ServerError> {
    let namespaces: Vec<NamespaceResponse> = registry(&state)
        .list_all()
        .into_iter()
        .map(|s| namespace_response(&state, s.info))
        .collect();
    let count = namespaces.len();
    Ok(Json(NamespaceListResponse { namespaces, count }))
}

/// `GET /api/v1/namespaces/{name}` — get namespace details.
pub async fn get_namespace_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let ns = registry(&state)
        .get(&name)
        .map_err(|e| ServerError::NotFound(format!("{e}")))?;
    Ok(Json(namespace_response(&state, ns.info)))
}

/// `DELETE /api/v1/namespaces/{name}` — delete a namespace.
pub async fn delete_namespace_handler(
    State(state): State<AppState>,
    req_extensions: axum::http::Extensions,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let registry = registry(&state);
    registry.delete_namespace(&name).map_err(tenant_error)?;
    state.db.clear_namespace_limits(&name);
    state.namespace_rate_limiter.remove(&name);
    state.namespace_ingest_limiter.remove(&name);

    // Deleting a tenant removes every credential's route to its data. If one
    // operation in this server belongs in a tamper-evident trail it is this
    // one, and it was in none.
    crate::audit::record(
        &state,
        &crate::audit::principal_of(&req_extensions),
        chronix_security::audit::AuditAction::NamespaceDelete,
        name.clone(),
        chronix_security::audit::AuditDecision::Allow,
        &[],
    );

    Ok(Json(serde_json::json!({
        "ok": true,
        "message": format!("namespace '{name}' deleted"),
    })))
}

/// Request body for `PUT /api/v1/namespaces/{name}/quota`.
///
/// Every field is required: a quota is a complete statement of what a tenant
/// may consume, and a partial update whose omitted fields mean "leave alone"
/// is indistinguishable from one whose omitted fields mean "unlimited". `0`
/// is how a field says "unlimited".
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateQuotaRequest {
    /// Maximum series the namespace may hold.
    pub max_series: u64,
    /// Maximum points per second.
    pub max_ingestion_rate: u64,
    /// Maximum distinct measurements.
    pub max_measurements: u32,
    /// Requests per second.
    pub max_request_rps: u64,
    /// Burst allowance for the request rate.
    pub max_request_burst: u32,
    /// Removed — present only so a request carrying it is refused by name.
    #[serde(default)]
    max_storage_bytes: Option<serde_json::Value>,
}

/// `PUT /api/v1/namespaces/{name}/quota` — change a tenant's quota.
///
/// Persisted, then applied to the database and both limiters before the
/// response: a quota the API reports as changed and nothing enforces is the
/// shape of a setting nothing reads.
///
/// # Errors
///
/// `404` when the namespace does not exist, `400` when the quota is out of
/// bounds or sets a field that was removed.
pub async fn update_namespace_quota_handler(
    State(state): State<AppState>,
    req_extensions: axum::http::Extensions,
    Path(name): Path<String>,
    Json(body): Json<UpdateQuotaRequest>,
) -> Result<impl IntoResponse, ServerError> {
    refuse_storage_quota(body.max_storage_bytes.as_ref())?;
    let registry = registry(&state);
    let quota = NamespaceQuota {
        max_series_count: body.max_series,
        max_ingestion_rate: body.max_ingestion_rate,
        max_measurements: body.max_measurements,
        max_request_rps: body.max_request_rps,
        max_request_burst: body.max_request_burst,
    };
    registry
        .update_quota(&name, quota.clone())
        .map_err(tenant_error)?;
    apply(&state, &name, &quota);

    crate::audit::record(
        &state,
        &crate::audit::principal_of(&req_extensions),
        chronix_security::audit::AuditAction::QuotaChange,
        name.clone(),
        chronix_security::audit::AuditDecision::Allow,
        &[
            ("max_series", quota.max_series_count.to_string()),
            ("max_ingestion_rate", quota.max_ingestion_rate.to_string()),
            ("max_measurements", quota.max_measurements.to_string()),
            ("max_request_rps", quota.max_request_rps.to_string()),
        ],
    );

    Ok(Json(QuotaResponse::from(&quota)))
}

/// `GET /api/v1/namespaces/{name}/usage` — usage beside its quota.
pub async fn get_namespace_usage_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let ns = registry(&state)
        .get(&name)
        .map_err(|_| ServerError::NotFound(format!("namespace not found: {name}")))?;
    let usage = usage_of(&state, &name);
    // Refreshed on read, so the gauge an alert watches is never older than
    // the last time anybody looked — and is set at all.
    chronix_security::tenant::record_usage_ratio(
        &name,
        "series_count",
        usage.series,
        ns.info.quota.max_series_count,
    );
    Ok(Json(UsageReport {
        usage,
        quota: QuotaResponse::from(&ns.info.quota),
    }))
}

// ── The namespace as a system invariant ────────────────────────────────

/// The tag every ingested point carries and every read is scoped by,
/// re-exported from `chronix_core` so every write and read path names the
/// same constant.
pub use chronix_core::NAMESPACE_TAG;

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
/// credential the request authenticated with. Prefer this to
/// [`scope_from_metadata`], which trusts the namespace the client names.
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
        && !ctx.allows_namespace(&namespace)
    {
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
    Ok(Some(namespace))
}

/// The gRPC and Flight SQL counterpart of the HTTP namespace gate.
///
/// **Every RPC that touches data calls this**, naming the action it performs,
/// for the same reason every ingestion surface calls one write function: a
/// check each RPC has to remember is a check most of them forget. All of
/// them did. [`scope_from_request`] checked the credential's namespace
/// binding and stopped there, so Cedar — which lives in an axum middleware —
/// was never consulted on these two surfaces at all: a policy that denied a
/// principal `Read` on a namespace was enforced on HTTP and bypassed by
/// pointing any Flight SQL client at the same server.
///
/// Authorization is *not* optional per surface. It is optional per
/// deployment, and the deployment says so by configuring a policy directory.
///
/// # Errors
///
/// `PermissionDenied` when the credential is not bound to the namespace, or
/// when a configured policy denies `action` on it.
pub fn authorize_request<T>(
    multi_tenancy: bool,
    authz: Option<&chronix_security::authz::AuthzEngine>,
    audit: Option<&chronix_security::audit::AuditLogger>,
    limiter: &crate::rate_limit::NamespaceRateLimiter,
    action: chronix_security::authz::ChronixAction,
    request: &tonic::Request<T>,
) -> Result<Option<String>, tonic::Status> {
    debug_assert!(
        !action.is_administrative(),
        "{action} applies to the system; these surfaces carry data actions"
    );
    let scope = scope_from_request(multi_tenancy, request)?;
    authorize_policy(authz, audit, action, request, scope.as_deref())?;

    // The request-rate quota, after the policy as on HTTP so a refused
    // request spends no token. These surfaces never consulted it: a tenant
    // limited to one request a second on HTTP was unlimited on gRPC and
    // Flight SQL.
    let namespace = scope.as_deref().unwrap_or(DEFAULT_NAMESPACE);
    if let Err(wait) = limiter.check_wait(namespace) {
        metrics::counter!("chronix_rate_limited_total", "namespace" => namespace.to_string())
            .increment(1);
        let ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX).max(1);
        let mut status = tonic::Status::resource_exhausted(format!(
            "namespace {namespace:?} is over its request rate; retry after {} ms",
            ms
        ));
        crate::error::pushback(&mut status, ms);
        return Err(status);
    }
    Ok(scope)
}

/// The Cedar half of [`authorize_request`].
fn authorize_policy<T>(
    authz: Option<&chronix_security::authz::AuthzEngine>,
    audit: Option<&chronix_security::audit::AuditLogger>,
    action: chronix_security::authz::ChronixAction,
    request: &tonic::Request<T>,
    scope: Option<&str>,
) -> Result<(), tonic::Status> {
    let Some(engine) = authz else {
        return Ok(());
    };
    let Some(ctx) = request
        .extensions()
        .get::<chronix_security::auth::AuthContext>()
    else {
        // No credential, and a policy engine cannot decide about nobody.
        // Authentication being off is the operator's decision; it is checked
        // where it is made, not re-litigated per RPC.
        return Ok(());
    };

    // A single-tenant deployment still has a namespace to name: `default` is
    // the one every point is written under. Scoping is off, authorization is
    // not — a policy naming `Chronix::Namespace::"default"` governs it.
    let namespace = scope.unwrap_or(DEFAULT_NAMESPACE).to_string();
    let resource = chronix_security::authz::ChronixNamespace::new(&namespace);
    if engine
        .authorize_namespace(&ctx.principal(), action, &resource)
        .is_denied()
    {
        tracing::warn!(
            principal = %ctx.principal,
            %namespace,
            %action,
            "namespace access denied by policy"
        );
        // Recorded here as it is on HTTP. A trail that holds a refused
        // request on one protocol and not another is a trail that answers
        // "was this attempted?" with "depends which port".
        if let Some(logger) = audit {
            logger.log(
                chronix_security::audit::AuditEvent::new(
                    &ctx.principal,
                    audit_action_for(action),
                    namespace.clone(),
                    chronix_security::audit::AuditDecision::Deny,
                )
                .with_metadata("reason", "denied by policy"),
            );
        }
        metrics::counter!("chronix_authz_denied_total").increment(1);
        return Err(tonic::Status::permission_denied(format!(
            "access denied: {action} on namespace {namespace}"
        )));
    }
    Ok(())
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
            "max_ingestion_rate": 5000,
            "max_measurements": 50
        }"#;
        let req: CreateNamespaceRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, "staging");
        assert_eq!(req.description, "Staging env");
        assert_eq!(req.max_series, 100_000);
        assert_eq!(req.max_ingestion_rate, 5000);
        assert_eq!(req.max_measurements, 50);
        assert!(req.max_storage_bytes.is_none());
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
                max_measurements: 0,
                max_request_rps: 0,
                max_request_burst: 0,
            },
            usage: UsageResponse {
                series: 42,
                measurements: 3,
                ingestion_rate: 0,
            },
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["name"], "prod");
        assert_eq!(json["usage"]["series"], 42);
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
