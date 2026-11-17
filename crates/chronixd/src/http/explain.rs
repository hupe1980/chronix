//! What chronixd says when a PromQL query answers with nothing.
//!
//! An empty result is not an error, so the explanation travels in the
//! response's `warnings`, which Prometheus clients already show. Two kinds,
//! both computed **only** when the result is empty, from the label sets the
//! label endpoints read, and scoped to the caller's namespace:
//!
//! - a typo: `metric "http_requets_total" matches no series — did you mean
//!   "http_requests_total"?`, and the same for a label key no series carries;
//! - with `explain_empty=true`, which matcher removed the last series and the
//!   values its label does have.

use std::sync::Arc;

use chronix::Chronix;
use chronix::diagnostics::suggest;
use chronix::promql::{ClassicView, Expr, MetricRef, PromQLValue};

use super::prom::{Selector, observed_label_sets};

/// Prometheus's default lookback: an instant selector reads this far back.
const LOOKBACK_NS: i64 = 5 * 60 * 1_000_000_000;

/// Whether a query's value is an empty vector or matrix.
pub(super) fn is_empty(value: &PromQLValue) -> bool {
    match value {
        PromQLValue::Vector(s) | PromQLValue::Matrix(s) => s.is_empty(),
        PromQLValue::Scalar(_) | PromQLValue::String(_) => false,
    }
}

/// The warnings for an empty result of `expr`, evaluated over
/// `[start_ns, end_ns]` (equal for an instant query).
pub(super) fn empty_result_warnings(
    db: &Arc<Chronix>,
    scope: Option<&str>,
    expr: &Expr,
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    explain: bool,
) -> Vec<String> {
    let mut out = Vec::new();
    let registry = db.schema_registry();
    for sel in chronix::promql::selectors(expr) {
        // A selector without a metric name would have to be checked against
        // every metric; it is skipped rather than made expensive.
        let Some(metric) = sel.metric else { continue };
        let visible = |t: &MetricRef| db.has_measurement_in(scope, &t.measurement);

        let targets: Vec<MetricRef> = chronix::promql::metric::resolve(registry, metric)
            .into_iter()
            .filter(|t| db.schema(&t.measurement).is_some() && visible(t))
            .collect();
        if targets.is_empty() {
            // Classic `_bucket`/`_count`/`_sum` views resolve but are never
            // listed, so they are never offered as a correction either.
            let names: Vec<String> = chronix::promql::all_metrics(registry)
                .into_iter()
                .filter(|t| t.view == ClassicView::Native && visible(t))
                .map(|t| t.name)
                .collect();
            if let Some(hint) = suggest(metric, names.iter().map(String::as_str)).hint() {
                out.push(format!("metric \"{metric}\" matches no series — {hint}"));
            }
            if explain {
                let e = chronix::promql::explain_empty(Some(metric), &[], sel.matchers, false);
                out.push(e.to_string());
            }
            continue;
        }

        // The window this selector read: its range (or the lookback), shifted
        // by its offset.
        let offset_ns = secs_to_ns(sel.offset_secs);
        let reach = secs_to_ns(sel.range_secs).max(LOOKBACK_NS);
        let from = start_ns.saturating_sub(offset_ns).saturating_sub(reach);
        let to = end_ns.saturating_sub(offset_ns);
        let selector = Selector {
            targets,
            pushdown: Vec::new(),
            compiled: Vec::new(),
        };
        let Ok((sets, truncated)) = observed_label_sets(
            db,
            scope,
            from,
            to,
            limit,
            std::slice::from_ref(&selector),
            false,
        ) else {
            continue;
        };

        if !sets.is_empty() {
            let mut keys: Vec<&str> = sets
                .iter()
                .flat_map(|set| set.iter().map(|(k, _)| k.as_str()))
                .filter(|k| *k != "__name__")
                .collect();
            keys.sort_unstable();
            keys.dedup();
            for m in sel.matchers {
                if m.name == "__name__" || m.matches_empty() || keys.contains(&m.name.as_str()) {
                    continue;
                }
                if let Some(hint) = suggest(&m.name, keys.iter().copied()).hint() {
                    out.push(format!(
                        "label \"{}\" is not on any series of \"{metric}\" — {hint}",
                        m.name
                    ));
                }
            }
        }

        if explain {
            let e = chronix::promql::explain_empty(Some(metric), &sets, sel.matchers, truncated);
            // A selector that still matches something is not why the result
            // is empty; say nothing about it.
            if sets.is_empty() || e.emptied_by.is_some() {
                out.push(e.to_string());
            }
        }
    }
    out.dedup();
    out
}

#[allow(clippy::cast_possible_truncation)] // a duration in seconds, as nanoseconds
fn secs_to_ns(secs: f64) -> i64 {
    if secs.is_finite() {
        (secs * 1e9) as i64
    } else {
        0
    }
}

// ── `POST /api/v1/chronix/promql/explain` ─────────────────────────────────

/// Body of `POST /api/v1/chronix/promql/explain`.
///
/// Times take the Prometheus forms (RFC 3339 or Unix seconds, as a string or
/// a number); `step` a duration (`30s`) or seconds.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromqlExplainRequest {
    /// The PromQL query.
    pub query: String,
    /// Evaluation instant of an instant query (default: now).
    #[serde(default)]
    pub time: Option<serde_json::Value>,
    /// Range start; with `end` and `step`, a range query.
    #[serde(default)]
    pub start: Option<serde_json::Value>,
    /// Range end.
    #[serde(default)]
    pub end: Option<serde_json::Value>,
    /// Range step.
    #[serde(default)]
    pub step: Option<serde_json::Value>,
    /// Evaluate each node and report the series and samples it produced.
    #[serde(default)]
    pub analyze: bool,
}

fn arg_text(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// The explain tree for `req`, in `scope`, under the server's PromQL bounds.
///
/// Shared by the HTTP route and the MCP `explain` tool, so both answer the
/// same question the same way.
pub(crate) async fn promql_explain(
    state: &super::AppState,
    scope: Option<String>,
    req: PromqlExplainRequest,
) -> Result<chronix::promql::PromqlExplain, crate::error::ServerError> {
    use super::prom_params::{parse_duration_ns, parse_time_ns};
    use crate::error::ServerError;

    let time = |key: &str, v: &Option<serde_json::Value>| -> Result<Option<i64>, ServerError> {
        v.as_ref()
            .map(|v| parse_time_ns(key, &arg_text(v)))
            .transpose()
    };
    let start = time("start", &req.start)?;
    let end = time("end", &req.end)?;
    let step = req
        .step
        .as_ref()
        .map(|v| parse_duration_ns("step", &arg_text(v)))
        .transpose()?;
    let range = match (start, end, step) {
        (None, None, None) => None,
        (Some(s), Some(e), Some(st)) => {
            if s > e {
                return Err(ServerError::BadRequest(
                    "end must not be before start".into(),
                ));
            }
            if st <= 0 {
                return Err(ServerError::BadRequest("step must be positive".into()));
            }
            let max = state.config.server.max_range_query_points;
            #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
            let points = ((i128::from(e) - i128::from(s)) / i128::from(st)) as u64 + 1;
            if points > max {
                return Err(ServerError::BadRequest(format!(
                    "the range would produce {points} evaluation points, over the limit of {max} \
                     (server.max_range_query_points); widen step or narrow the range"
                )));
            }
            Some((s, e, st))
        }
        _ => {
            return Err(ServerError::BadRequest(
                "a range explain needs start, end and step together; an instant one needs none \
                 of them"
                    .into(),
            ));
        }
    };
    let at = match (range, time("time", &req.time)?) {
        (Some((_, e, _)), _) => e,
        (None, Some(t)) => t,
        (None, None) => super::types::now_nanos_i64(),
    };

    let expr = chronix::promql::parse(&req.query)
        .map_err(|e| ServerError::BadRequest(super::prom::positioned(&req.query, &e)))?;

    let timeout_secs = state.config.server.prom_query_timeout_secs;
    let base = chronix::promql::eval::QueryParams {
        time: at,
        max_series: state.config.server.prom_series_limit,
        max_memory_bytes: state.config.server.prom_max_result_bytes,
        deadline: chronix::promql::eval::Deadline::after(std::time::Duration::from_secs(
            timeout_secs,
        )),
        ..Default::default()
    };
    let range_params = range.map(|(s, e, st)| chronix::promql::eval::QueryParams {
        start: Some(s),
        end: Some(e),
        step: Some(st),
        ..base.clone()
    });
    let evaluator = chronix::promql::PromQLEvaluator::new(state.db.clone()).with_namespace(scope);
    let slot = state
        .query_slots
        .acquire(std::time::Duration::from_secs(timeout_secs))
        .await?;
    let analyze = req.analyze;
    tokio::task::spawn_blocking(move || {
        let _slot = slot;
        chronix::promql::explain_tree(&evaluator, &expr, &base, range_params.as_ref(), analyze)
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))
}

/// `POST /api/v1/chronix/promql/explain` — a PromQL query as a tree of
/// sub-expressions; with `analyze`, each annotated with the series and
/// samples it produced.
pub async fn promql_explain_handler(
    axum::extract::State(state): axum::extract::State<super::AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    axum::Json(req): axum::Json<PromqlExplainRequest>,
) -> Result<axum::Json<serde_json::Value>, crate::error::ServerError> {
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let plan = promql_explain(&state, scope, req).await?;
    Ok(axum::Json(serde_json::json!({ "tree": plan.tree })))
}
