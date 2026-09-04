//! Signal triggers over HTTP.
//!
//! The trigger engine, the delivery router and the signal store all worked and
//! were all unreachable: nothing in this server ever constructed a
//! [`Pipeline`](chronix::Pipeline), so `CREATE TRIGGER` existed for embedded
//! callers only. These endpoints are the surface it was missing.
//!
//! # Every statement is scoped to the caller's namespace
//!
//! A trigger names a *measurement*, and a measurement is shared: every tenant
//! writing `cpu` writes the same measurement, told apart only by the namespace
//! tag on the series. An unscoped trigger fires on every tenant's data and
//! delivers one tenant's values to another tenant's webhook — a forgotten
//! namespace filter with an outbound HTTP request attached. So the statement
//! goes through
//! [`Pipeline::execute_signal_sql_scoped`](chronix::Pipeline::execute_signal_sql_scoped),
//! which `AND`s the condition with the namespace tag and puts the trigger in
//! the tenant's own key space.

use axum::extract::{Path, State};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::ServerError;
use crate::http::types::AppState;

/// Body of `POST /api/v1/triggers`.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct TriggerSqlRequest {
    /// A trigger DSL statement: `CREATE TRIGGER`, `ALTER TRIGGER`,
    /// `DROP TRIGGER` or `SHOW TRIGGERS`.
    pub sql: String,
}

/// One trigger, as the tenant that created it sees it.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct TriggerView {
    /// Trigger name, as written.
    pub name: String,
    /// Measurement the trigger watches.
    pub measurement: String,
    /// Whether the trigger is currently evaluated.
    pub enabled: bool,
    /// Channels the trigger delivers to.
    pub delivery: Vec<String>,
}

/// Response of the trigger endpoints.
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(untagged)]
pub enum TriggerResponse {
    /// A listing, from `SHOW TRIGGERS` or `GET /api/v1/triggers`.
    Triggers {
        /// The caller's triggers.
        triggers: Vec<TriggerView>,
    },
    /// An acknowledgement, from a statement that changed something.
    Ack {
        /// What happened, in one line.
        message: String,
    },
}

/// A fired signal.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct SignalView {
    /// Unique event id, for idempotent handling by a receiver.
    pub event_id: String,
    /// Name of the trigger that fired.
    pub trigger_name: String,
    /// Measurement the signal is about.
    pub measurement: String,
    /// Tags of the series.
    pub tags: std::collections::BTreeMap<String, String>,
    /// Nanosecond timestamp of the point that caused the signal.
    pub timestamp: i64,
    /// Severity.
    pub severity: String,
    /// The value that triggered it.
    pub value: f64,
}

/// The pipeline, or a 404 explaining that triggers are not configured.
fn pipeline(state: &AppState) -> Result<&std::sync::Arc<chronix::Pipeline>, ServerError> {
    state.pipeline.as_ref().ok_or_else(|| {
        ServerError::NotFound(
            "signal triggers are not enabled: add a [triggers] section to the server \
             configuration"
                .into(),
        )
    })
}

fn to_response(result: chronix::chronix_streaming::signal::SqlResult) -> TriggerResponse {
    use chronix::chronix_streaming::signal::SqlResult;
    match result {
        SqlResult::Triggers(triggers) => TriggerResponse::Triggers {
            triggers: triggers
                .into_iter()
                .map(|t| TriggerView {
                    name: t.name,
                    measurement: t.measurement,
                    enabled: t.enabled,
                    delivery: t.delivery,
                })
                .collect(),
        },
        other => TriggerResponse::Ack {
            message: format!("{other:?}"),
        },
    }
}

/// `POST /api/v1/triggers` — run one trigger DSL statement.
///
/// # Errors
///
/// Returns 404 when triggers are not configured, and 400 when the statement
/// does not parse or cannot be executed — including a `DELIVER webhook(…)`
/// with no signing secret configured, which is refused rather than accepted
/// and dropped.
#[utoipa::path(
    post,
    path = "/api/v1/triggers",
    request_body = TriggerSqlRequest,
    responses((status = 200, description = "Statement executed", body = TriggerResponse)),
    tag = "triggers"
)]
pub async fn trigger_sql_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    Json(req): Json<TriggerSqlRequest>,
) -> Result<Json<TriggerResponse>, ServerError> {
    let pipeline = pipeline(&state)?;
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);

    let result = pipeline
        .execute_signal_sql_scoped(&req.sql, scope.as_deref())
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;
    Ok(Json(to_response(result)))
}

/// `GET /api/v1/triggers` — the caller's triggers.
///
/// # Errors
///
/// Returns 404 when triggers are not configured.
#[utoipa::path(
    get,
    path = "/api/v1/triggers",
    responses((status = 200, description = "The caller's triggers", body = TriggerResponse)),
    tag = "triggers"
)]
pub async fn list_triggers_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
) -> Result<Json<TriggerResponse>, ServerError> {
    let pipeline = pipeline(&state)?;
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);

    let result = pipeline
        .execute_signal_sql_scoped("SHOW TRIGGERS", scope.as_deref())
        .map_err(|e| ServerError::Internal(e.to_string()))?;
    Ok(Json(to_response(result)))
}

/// `DELETE /api/v1/triggers/{name}` — drop one of the caller's triggers.
///
/// # Errors
///
/// Returns 404 when triggers are not configured, and 400 when the caller has
/// no trigger of that name — which is also the answer when another tenant
/// has one, because a tenant must not be able to learn that.
#[utoipa::path(
    delete,
    path = "/api/v1/triggers/{name}",
    params(("name" = String, Path, description = "Trigger name")),
    responses((status = 200, description = "Trigger dropped", body = TriggerResponse)),
    tag = "triggers"
)]
pub async fn drop_trigger_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    Path(name): Path<String>,
) -> Result<Json<TriggerResponse>, ServerError> {
    let pipeline = pipeline(&state)?;
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);

    // The name goes through the DSL rather than into a string, so it is
    // validated as an identifier before it reaches the engine.
    let result = pipeline
        .execute_signal_sql_scoped(&format!("DROP TRIGGER {name}"), scope.as_deref())
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;
    Ok(Json(to_response(result)))
}

/// `GET /api/v1/signals` — recently fired signals for the caller.
///
/// # Errors
///
/// Returns 404 when triggers are not configured.
#[utoipa::path(
    get,
    path = "/api/v1/signals",
    responses((status = 200, description = "Recently fired signals", body = [SignalView])),
    tag = "triggers"
)]
pub async fn list_signals_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
) -> Result<Json<Vec<SignalView>>, ServerError> {
    let pipeline = pipeline(&state)?;
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0));

    let signals = pipeline
        .signal_store()
        .all()
        .into_iter()
        // The store is process-wide, so it is filtered on the way out. The
        // namespace is a tag on the series the signal is about, which is the
        // same thing the trigger's own scoping matched on.
        .filter(|s| match scope {
            None => true,
            Some(ns) => s.tags.get(chronix_core::NAMESPACE_TAG).map(String::as_str) == Some(ns),
        })
        .map(|s| SignalView {
            event_id: s.event_id,
            trigger_name: s.trigger_name,
            measurement: s.measurement,
            tags: s.tags,
            timestamp: s.timestamp,
            severity: format!("{:?}", s.severity).to_lowercase(),
            value: s.value,
        })
        .collect();

    Ok(Json(signals))
}
