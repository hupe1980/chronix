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
///
/// The key is `query`, as it is on `/api/v1/chronix/sql` and as the
/// Prometheus endpoints' `?query=` parameter is: one name for "the statement
/// to run" across the whole API. It was `sql`, which is both a second name
/// and the wrong one — the trigger DSL is not SQL.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct TriggerSqlRequest {
    /// A trigger DSL statement: `CREATE TRIGGER`, `ALTER TRIGGER`,
    /// `DROP TRIGGER` or `SHOW TRIGGERS`.
    pub query: String,
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
    req_extensions: axum::http::Extensions,
    Json(req): Json<TriggerSqlRequest>,
) -> Result<Json<TriggerResponse>, ServerError> {
    let pipeline = pipeline(&state)?;
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);

    let result = pipeline
        .execute_signal_sql_scoped(&req.query, scope.as_deref())
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    // A trigger sends data somewhere — a webhook, a log, a metric — so
    // creating one is a data-egress decision, and `TriggerCreate` was a
    // category nothing produced.
    //
    // Only a `CREATE`: this endpoint runs one statement of the DSL, and a
    // `SHOW TRIGGERS` through it is a read.
    if let chronix::chronix_streaming::signal::SqlResult::Created(ref name) = result {
        crate::audit::record(
            &state,
            &crate::audit::principal_of(&req_extensions),
            chronix_security::audit::AuditAction::TriggerCreate,
            name.clone(),
            chronix_security::audit::AuditDecision::Allow,
            &[("namespace", scope.clone().unwrap_or_default())],
        );
    }
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

/// `GET /api/v1/triggers/{name}` — read one of the caller's triggers.
///
/// `SHOW TRIGGERS` was the only way to read a trigger back, so a client that
/// had just created one had to list everything and search for it — and a
/// deployment with many triggers paid for the whole listing to answer a
/// question about one.
///
/// # Errors
///
/// Returns 404 when triggers are not configured **and** when the caller has no
/// trigger of that name — which is also the answer when another tenant has
/// one, because a tenant must not be able to learn that.
#[utoipa::path(
    get,
    path = "/api/v1/triggers/{name}",
    params(("name" = String, Path, description = "Trigger name")),
    responses(
        (status = 200, description = "The trigger", body = TriggerView),
        (status = 404, description = "No such trigger in this namespace"),
    ),
    tag = "triggers"
)]
pub async fn get_trigger_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    Path(name): Path<String>,
) -> Result<Json<TriggerView>, ServerError> {
    let pipeline = pipeline(&state)?;
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);

    let result = pipeline
        .execute_signal_sql_scoped("SHOW TRIGGERS", scope.as_deref())
        .map_err(|e| ServerError::Internal(e.to_string()))?;
    let TriggerResponse::Triggers { triggers } = to_response(result) else {
        return Err(ServerError::Internal(
            "SHOW TRIGGERS answered a listing".into(),
        ));
    };
    triggers
        .into_iter()
        .find(|t| t.name == name)
        .map(Json)
        .ok_or_else(|| ServerError::NotFound(format!("trigger '{name}'")))
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
    req_extensions: axum::http::Extensions,
    Path(name): Path<String>,
) -> Result<Json<TriggerResponse>, ServerError> {
    let pipeline = pipeline(&state)?;
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);

    // The name goes through the DSL rather than into a string, so it is
    // validated as an identifier before it reaches the engine.
    let result = pipeline
        .execute_signal_sql_scoped(&format!("DROP TRIGGER {name}"), scope.as_deref())
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    // Removing an alert is how an alert stops firing, which is worth as much
    // in a trail as creating one.
    crate::audit::record(
        &state,
        &crate::audit::principal_of(&req_extensions),
        chronix_security::audit::AuditAction::TriggerDrop,
        name.clone(),
        chronix_security::audit::AuditDecision::Allow,
        &[("namespace", scope.clone().unwrap_or_default())],
    );
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

    // One ring per namespace, so this reads the caller's own rather than
    // reading everything and filtering. The filter hid the fact that the
    // capacity was still shared: a noisy tenant evicted a quiet one's signals,
    // and the quiet one just saw fewer of its own.
    let signals = pipeline
        .signal_store()
        .all_in(scope)
        .into_iter()
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
