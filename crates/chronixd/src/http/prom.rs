//! Prometheus-compatible (PromQL) endpoint handlers.

use arrow::array::Array;
use axum::extract::{Json, Path, State};
use axum::response::IntoResponse;
use serde::Serialize;

use crate::error::ServerError;

use super::types::AppState;

/// A Prometheus API error, as a response with the status code Prometheus
/// uses for that `errorType`.
///
/// The status code is not cosmetic. Prometheus's own clients — and Grafana —
/// branch on it: a 400 is a permanent failure to be reported to the user, a
/// 503 is a timeout worth retrying. Chronix answered every error with HTTP
/// 200 and an error body, so a failing query looked to Grafana like a
/// successful one that returned nothing.
fn prom_error(error_type: &str, message: impl Into<String>) -> axum::response::Response {
    let code = match error_type {
        "bad_data" => axum::http::StatusCode::BAD_REQUEST,
        "timeout" => axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "canceled" => axum::http::StatusCode::SERVICE_UNAVAILABLE,
        // "execution" and anything else: Prometheus answers 422 for a query
        // that parsed but could not be evaluated.
        "internal" => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        _ => axum::http::StatusCode::UNPROCESSABLE_ENTITY,
    };
    (
        code,
        Json(PromResponse {
            status: "error".into(),
            data: None,
            error: Some(message.into()),
            error_type: Some(error_type.into()),
            warnings: Vec::new(),
        }),
    )
        .into_response()
}

/// A [`ServerError`] on a Prometheus-API route.
///
/// Exists so the handlers can keep writing `?` and still answer in the
/// envelope their clients parse: `From<ServerError>` makes the conversion
/// implicit, `IntoResponse` makes it the Prometheus shape.
#[derive(Debug)]
pub struct PromApiError(ServerError);

impl From<ServerError> for PromApiError {
    fn from(e: ServerError) -> Self {
        Self(e)
    }
}

impl axum::response::IntoResponse for PromApiError {
    fn into_response(self) -> axum::response::Response {
        prom_server_error(&self.0)
    }
}

/// Render a [`ServerError`] in the Prometheus error envelope.
///
/// Grafana branches on `errorType`, so every route of this API — the
/// discovery endpoints included — has to answer in the same shape.
fn prom_server_error(e: &ServerError) -> axum::response::Response {
    match e {
        ServerError::BadRequest(msg) => prom_error("bad_data", msg.clone()),
        ServerError::NotFound(msg) => prom_error("bad_data", msg.clone()),
        ServerError::WriteTimeout(_) => prom_error("timeout", e.to_string()),
        // Anything else is ours, and its detail is redacted the same way
        // `ServerError`'s own renderer redacts a 5xx.
        other => {
            tracing::error!(error = %other, "prometheus endpoint error");
            prom_error("internal", "an internal error occurred")
        }
    }
}

/// The warning Prometheus attaches when a `limit` cut the result.
///
/// Verbatim: it is the string clients and dashboards match on.
pub const TRUNCATED_WARNING: &str = "results truncated due to limit";

/// Prometheus-compatible API response envelope.
#[derive(Debug, Serialize)]
pub struct PromResponse {
    /// Status: "success" or "error".
    pub status: String,
    /// Result type: "vector", "matrix", "scalar", "string".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<PromData>,
    /// Error message (if status == "error").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Error type (if status == "error").
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    /// Non-fatal annotations. Present only when non-empty, as upstream does.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// Prometheus API response whose `data` is a bare array.
///
/// `/api/v1/labels`, `/api/v1/label/<name>/values` and `/api/v1/series`
/// answer `{"status":"success","data":[…]}` — a flat array, with no
/// `resultType` wrapper. Only `/query` and `/query_range` wrap their result.
/// Chronix wrapped all five, so Grafana could not read a label list, a label
/// value list, or a series list from it: every variable dropdown and every
/// metric browser came up empty against a server whose data was fine. The
/// endpoints had tests, and the tests asserted the shape the server produced.
#[derive(Debug, Serialize)]
pub struct PromListResponse {
    /// Status: "success" or "error".
    pub status: String,
    /// The results, as a bare array.
    pub data: serde_json::Value,
    /// Error message (if status == "error").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Error type (if status == "error").
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    /// Non-fatal annotations. Present only when non-empty, as upstream does.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl PromListResponse {
    /// A successful list response, `truncated` saying whether a limit cut it.
    fn success(data: serde_json::Value, truncated: bool) -> Self {
        Self {
            status: "success".into(),
            data,
            error: None,
            error_type: None,
            warnings: if truncated {
                vec![TRUNCATED_WARNING.to_string()]
            } else {
                Vec::new()
            },
        }
    }
}

/// Prometheus API data payload.
#[derive(Debug, Serialize)]
pub struct PromData {
    /// "vector", "matrix", "scalar", "string"
    #[serde(rename = "resultType")]
    pub result_type: String,
    /// The results.
    pub result: serde_json::Value,
}

/// Export one query's storage-read counters.
///
/// A range query is supposed to read its window once rather than once per
/// step, and whether it does depends on the shape of the query. A deployment
/// that cannot see the hit ratio cannot tell a query that defeats the cache
/// from one that uses it — which is the situation the counters were added to
/// end, and did not, because nothing exported them.
fn record_scan_stats(kind: &'static str, stats: chronix::promql::eval::ScanStats) {
    metrics::counter!("chronix_promql_scans_total", "kind" => kind).increment(stats.scans);
    metrics::counter!("chronix_promql_scan_cache_hits_total", "kind" => kind)
        .increment(stats.cache_hits);
}

/// `GET/POST /api/v1/prom/query` handler.
pub async fn prom_instant_query_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    params: super::PromParams,
) -> axum::response::Response {
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let query_str = match params.require("query") {
        Ok(q) => q.to_string(),
        Err(e) => return prom_error("bad_data", e.to_string()),
    };
    let eval_time_ns = match params.time("time") {
        Ok(Some(ts)) => ts,
        Ok(None) => super::types::now_nanos_i64(),
        Err(e) => return prom_error("bad_data", e.to_string()),
    };

    let expr = match chronix::promql::parse(&query_str) {
        Ok(e) => e,
        Err(e) => return prom_error("bad_data", e.to_string()),
    };

    let limit = match params.limit() {
        Ok(l) => l,
        Err(e) => return prom_error("bad_data", e.to_string()),
    };

    let query_kind = "instant";
    let evaluator = chronix::promql::PromQLEvaluator::new(state.db.clone()).with_namespace(scope);
    // The same bounds the range query sets. One selector must not have a
    // series cap and a memory budget on `/query_range` and neither here.
    let timeout_secs = state.config.server.prom_query_timeout_secs;
    let params = chronix::promql::eval::QueryParams {
        time: eval_time_ns,
        max_series: state.config.server.prom_series_limit,
        max_memory_bytes: state.config.server.prom_max_result_bytes,
        // The evaluation carries its own deadline. The `tokio::time::timeout`
        // below stays as a backstop, but it only cancels the *wait*: a
        // blocking task cannot be cancelled, so without this the scan ran on
        // after the client had been answered.
        deadline: chronix::promql::eval::Deadline::after(std::time::Duration::from_secs(
            timeout_secs,
        )),
        ..Default::default()
    };
    // The evaluator carries the scan counters, and it moves into the blocking
    // task — so the stats have to come back out with the result. Without this
    // the counters existed, were asserted by a test, and were visible to
    // nobody running the server.
    let task = tokio::task::spawn_blocking(move || {
        let value = evaluator.instant_query(&expr, &params);
        (value, evaluator.scan_stats())
    });
    let result = if timeout_secs > 0 {
        match tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), task).await {
            Ok(r) => r,
            Err(_) => {
                return prom_error("timeout", format!("query timed out after {timeout_secs}s"));
            }
        }
    } else {
        task.await
    };

    let result = result.map(|(value, stats)| {
        record_scan_stats(query_kind, stats);
        value
    });

    match result {
        Ok(Ok(value)) => Json(prom_value_to_response(value, limit)).into_response(),
        Ok(Err(e)) => prom_error("execution", e.to_string()),
        Err(e) => prom_error("internal", format!("query task failed: {e}")),
    }
}

/// `GET/POST /api/v1/prom/query_range` handler.
pub async fn prom_range_query_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    params: super::PromParams,
) -> axum::response::Response {
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let (query_str, start_ns, end_ns, step_ns) = match (|| {
        Ok::<_, ServerError>((
            params.require("query")?.to_string(),
            parse_required_time(&params, "start")?,
            parse_required_time(&params, "end")?,
            params.duration("step")?.ok_or_else(|| {
                ServerError::BadRequest("missing required parameter: step".into())
            })?,
        ))
    })() {
        Ok(v) => v,
        Err(e) => return prom_error("bad_data", e.to_string()),
    };

    if start_ns > end_ns {
        return prom_error("bad_data", "end timestamp must not be before start time");
    }

    // Guard against excessive evaluation points that could OOM the server.
    let max_points = state.config.server.max_range_query_points;
    // i128 avoids overflow when the range spans the whole i64 domain.
    let span = (i128::from(end_ns) - i128::from(start_ns)) as u64;
    let num_points = span.checked_div(step_ns as u64).unwrap_or(0) + 1;
    if num_points > max_points {
        return prom_error(
            "bad_data",
            format!(
                "query would produce {num_points} evaluation points, exceeding limit of {max_points}"
            ),
        );
    }

    let limit = match params.limit() {
        Ok(l) => l,
        Err(e) => return prom_error("bad_data", e.to_string()),
    };

    let expr = match chronix::promql::parse(&query_str) {
        Ok(e) => e,
        Err(e) => return prom_error("bad_data", e.to_string()),
    };

    let query_kind = "range";
    let evaluator = chronix::promql::PromQLEvaluator::new(state.db.clone()).with_namespace(scope);
    let qparams = chronix::promql::eval::QueryParams {
        time: end_ns,
        start: Some(start_ns),
        end: Some(end_ns),
        step: Some(step_ns),
        max_series: state.config.server.prom_series_limit,
        max_memory_bytes: state.config.server.prom_max_result_bytes,
        max_points: usize::try_from(max_points).unwrap_or(usize::MAX),
        // As on the instant query: the deadline travels with the evaluation
        // so a step boundary can stop it, rather than only cancelling the
        // wait for a task that keeps scanning.
        deadline: chronix::promql::eval::Deadline::after(std::time::Duration::from_secs(
            state.config.server.prom_query_timeout_secs,
        )),
        ..Default::default()
    };

    let timeout_secs = state.config.server.prom_query_timeout_secs;
    let task = tokio::task::spawn_blocking(move || {
        let value = evaluator.range_query(&expr, &qparams);
        (value, evaluator.scan_stats())
    });
    let result = if timeout_secs > 0 {
        match tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), task).await {
            Ok(r) => r,
            Err(_) => {
                return prom_error("timeout", format!("query timed out after {timeout_secs}s"));
            }
        }
    } else {
        task.await
    };

    let result = result.map(|(value, stats)| {
        record_scan_stats(query_kind, stats);
        value
    });

    match result {
        Ok(Ok(value)) => Json(prom_value_to_response(value, limit)).into_response(),
        Ok(Err(e)) => prom_error("execution", e.to_string()),
        Err(e) => prom_error("internal", format!("query task failed: {e}")),
    }
}

/// A `start`/`end` parameter, which the range API requires.
fn parse_required_time(params: &super::PromParams, key: &str) -> Result<i64, ServerError> {
    params
        .time(key)?
        .ok_or_else(|| ServerError::BadRequest(format!("missing required parameter: {key}")))
}

/// `GET /api/v1/status/buildinfo` — what Grafana probes on connect to
/// decide which Prometheus features to offer.
///
/// Grafana's datasource calls this when you press "Save & test" and when it
/// negotiates capabilities. A 404 here does not stop queries working, but it
/// does make the datasource report itself as unhealthy, which is the first
/// thing a new user sees.
pub async fn prom_buildinfo_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "success",
        "data": {
            "version": env!("CARGO_PKG_VERSION"),
            "revision": option_env!("CHRONIX_GIT_SHA").unwrap_or("unknown"),
            "branch": "",
            "buildUser": "",
            "buildDate": "",
            "goVersion": "",
            // Chronix is not Prometheus, and says so where a client can see
            // it without breaking the schema Grafana parses.
            "application": "chronixd",
        }
    }))
}

/// `GET /api/v1/rules` — chronix has no recording or alerting rules.
///
/// Answered as an empty, well-formed group list rather than a 404: Grafana's
/// rule browser treats a missing endpoint as an error and an empty list as
/// "nothing configured", and the second is the truth.
pub async fn prom_empty_rules_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "success", "data": {"groups": []}}))
}

/// `GET /api/v1/alerts` — chronix has no alerting rules; its triggers are a
/// different feature with its own API.
pub async fn prom_empty_alerts_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "success", "data": {"alerts": []}}))
}

/// `GET /api/v1/query_exemplars` — exemplars are not stored.
pub async fn prom_empty_exemplars_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "success", "data": []}))
}

/// `GET /api/v1/prom/labels` — every label name in the window.
pub async fn prom_labels_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    prom_params: super::PromParams,
) -> Result<Json<PromListResponse>, PromApiError> {
    let db = state.db.clone();
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let params = PromSeriesQuery::from_params(&prom_params)?;
    let (start_ns, end_ns) = params.window()?;
    let matchers = params.matchers;
    let requested = prom_params.limit()?;
    // The server's cap bounds the *enumeration*; the request's `limit` bounds
    // the label **names** returned, as it does upstream. Passing `limit` down
    // here would report only the names the first `limit` series carried.
    let limit = state.config.server.prom_series_limit;

    let (mut labels, mut truncated) =
        tokio::task::spawn_blocking(move || -> Result<(Vec<String>, bool), ServerError> {
            let selectors = parse_selectors(&db, &matchers)?;
            let mut label_set = std::collections::BTreeSet::new();
            let (sets, truncated) = observed_label_sets(
                &db,
                scope.as_deref(),
                start_ns,
                end_ns,
                limit,
                &selectors,
                false,
            )?;
            for set in sets {
                for (key, _) in set {
                    label_set.insert(key);
                }
            }
            Ok((label_set.into_iter().collect(), truncated))
        })
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))??;

    // Upstream applies `limit` to the *label names* returned, not to the
    // series scanned to find them.
    if let Some(n) = requested {
        if labels.len() > n {
            labels.truncate(n);
            truncated = true;
        }
    }

    Ok(Json(PromListResponse::success(
        serde_json::to_value(labels).unwrap_or_default(),
        truncated,
    )))
}

/// `GET /api/v1/prom/label/{name}/values` — label values for a given label.
///
/// `__name__` is answered from the same enumeration as every other label, so
/// the names Grafana's metric browser offers are the names a query returns.
/// Listing measurements here instead is what made the browser offer `cpu` for
/// a measurement whose series are `cpu_usage` and `cpu_load`.
pub async fn prom_label_values_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    Path(label_name): Path<String>,
    prom_params: super::PromParams,
) -> Result<Json<PromListResponse>, PromApiError> {
    let db = state.db.clone();
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let params = PromSeriesQuery::from_params(&prom_params)?;
    let (start_ns, end_ns) = params.window()?;
    let matchers = params.matchers;
    let requested = prom_params.limit()?;
    // As for `/labels`: `limit` bounds the label **values** returned, not the
    // series read to find them.
    let limit = state.config.server.prom_series_limit;

    let (mut values, mut truncated) =
        tokio::task::spawn_blocking(move || -> Result<(Vec<String>, bool), ServerError> {
            let selectors = parse_selectors(&db, &matchers)?;
            let mut values = std::collections::BTreeSet::new();
            let (sets, truncated) = observed_label_sets(
                &db,
                scope.as_deref(),
                start_ns,
                end_ns,
                limit,
                &selectors,
                false,
            )?;
            for set in sets {
                for (key, value) in set {
                    if key == label_name {
                        values.insert(value);
                    }
                }
            }
            Ok((values.into_iter().collect(), truncated))
        })
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))??;

    if let Some(n) = requested {
        if values.len() > n {
            values.truncate(n);
            truncated = true;
        }
    }

    Ok(Json(PromListResponse::success(
        serde_json::to_value(values).unwrap_or_default(),
        truncated,
    )))
}

/// The label sets an enumeration found, and whether it stopped at the limit.
type ObservedLabelSets = (Vec<Vec<(String, String)>>, bool);

/// One parsed `match[]` selector: the metrics it names and the rest of its
/// matchers, compiled.
///
/// A metric is a `(measurement, field)` pair, so a selector names a *set* of
/// them: `{__name__=~"cpu.+"}` may reach two fields of one measurement and one
/// of another. `chronix::promql::metric` owns the mapping, and the evaluator
/// resolves a selector through the same function — the two having their own
/// answers is what let `/series?match[]={__name__=~".+"}` return nothing while
/// `/query` with that selector returned every series.
struct Selector {
    /// The metrics the selector names.
    targets: Vec<chronix::promql::MetricRef>,
    /// Equality matchers, pushed into the scan.
    pushdown: Vec<(String, String)>,
    /// Every matcher except `__name__`, applied to the label sets produced.
    compiled: Vec<chronix::promql::CompiledMatcher>,
}

/// Parse `match[]` selectors into metrics plus compiled matchers.
///
/// A selector that constrains no metric name is dropped rather than treated as
/// "everything": `match[]={host="a"}` cannot be answered without scanning every
/// measurement, and Prometheus requires at least one non-empty matcher anyway.
fn parse_selectors(
    db: &chronix::Chronix,
    matchers: &[String],
) -> Result<Vec<Selector>, ServerError> {
    let registry = db.schema_registry();
    let mut out = Vec::new();
    for raw in matchers {
        let expr =
            chronix::promql::parse(raw).map_err(|e| ServerError::BadRequest(e.to_string()))?;
        let chronix::promql::Expr::VectorSelector { name, matchers, .. } = &expr else {
            return Err(ServerError::BadRequest(format!(
                "match[] must be a series selector, got: {raw}"
            )));
        };
        let named = name.as_deref().or_else(|| {
            matchers
                .iter()
                .find(|m| m.name == "__name__" && m.op == chronix::promql::MatchOp::Equal)
                .map(|m| m.value.as_str())
        });
        let name_filters: Vec<chronix::promql::CompiledMatcher> = matchers
            .iter()
            .filter(|m| m.name == "__name__" && m.op != chronix::promql::MatchOp::Equal)
            .map(chronix::promql::CompiledMatcher::compile)
            .collect::<Result<_, _>>()
            .map_err(|e| ServerError::BadRequest(e.to_string()))?;

        let mut targets = match named {
            Some(n) => chronix::promql::metric::resolve(registry, n),
            None if name_filters.is_empty() => continue,
            None => chronix::promql::all_metrics(registry),
        };
        // The registry still lists a measurement pending a soft-delete;
        // `db.schema` is where every discovery surface agrees on "gone or
        // not", so a dropped metric does not linger in `/series` or
        // `/labels` for its whole grace period.
        targets.retain(|t| db.schema(&t.measurement).is_some());
        targets.retain(|t| {
            let labels = [("__name__".to_string(), t.name.clone())];
            name_filters.iter().all(|f| f.matches(&labels))
        });

        out.push(Selector {
            targets,
            pushdown: matchers
                .iter()
                .filter(|m| {
                    m.name != "__name__"
                        && m.op == chronix::promql::MatchOp::Equal
                        && m.name != crate::namespace::NAMESPACE_TAG
                })
                .map(|m| (m.name.clone(), m.value.clone()))
                .collect(),
            compiled: chronix::promql::compile_label_matchers(matchers)
                .map_err(|e| ServerError::BadRequest(e.to_string()))?,
        });
    }
    Ok(out)
}

/// Optional `start` / `end` bounds and `match[]` selectors, shared by the
/// label and series endpoints.
///
/// Parsed from the raw query string rather than through `serde_urlencoded`,
/// which has no representation for a repeated key: `?match[]=a&match[]=b` is
/// how every Prometheus client sends matchers, and deserializing it into a
/// `Vec<String>` failed with `400 Bad Request` on *every* request — including
/// the single-matcher form.
#[derive(Debug, Default)]
pub struct PromSeriesQuery {
    /// Label matchers in the form `metric_name{label="value"}`.
    pub matchers: Vec<String>,
    /// Optional start of the window, nanoseconds.
    pub start: Option<i64>,
    /// Optional end of the window, nanoseconds.
    pub end: Option<i64>,
}

impl PromSeriesQuery {
    /// Read the parameters from a request, whether they arrived in the query
    /// string or in a POST form body.
    ///
    /// `start` and `end` accept RFC 3339 as well as a Unix timestamp, and an
    /// unparseable one is an **error** rather than a silent fall back to the
    /// default window: `start=yesterday` used to widen the scan to the last
    /// hour and answer as though it had understood.
    fn from_params(params: &super::PromParams) -> Result<Self, ServerError> {
        let mut matchers: Vec<String> = params
            .get_all("match[]")
            .into_iter()
            .map(str::to_string)
            .collect();
        // Prometheus clients send `match[]`; hand-written curl calls use the
        // unbracketed spelling, and repeats of either are a union.
        matchers.extend(params.get_all("match").into_iter().map(str::to_string));
        Ok(Self {
            matchers,
            start: params.time("start")?,
            end: params.time("end")?,
        })
    }

    /// Resolve the window, defaulting to the last hour.
    ///
    /// Prometheus bounds these endpoints by time for the same reason: an
    /// unbounded label enumeration is a full scan of every series ever
    /// written, issued by a dashboard on every page load.
    fn window(&self) -> Result<(i64, i64), ServerError> {
        let now_ns = crate::util::now_nanos()?;
        let one_hour_ns: i64 = 3_600_000_000_000;
        Ok((
            self.start.unwrap_or(now_ns - one_hour_ns),
            self.end.unwrap_or(now_ns),
        ))
    }
}

/// Every distinct label set present in a window, for the metrics the
/// selectors name — or for every metric, when there are none.
///
/// One scan behind `/series`, `/labels` and `/label/{name}/values`, because
/// three near-identical enumerations is the shape that drifts: they answered
/// the same selector syntax and disagreed about what a metric is.
///
/// `project_value` decides how precise the answer is, and it is a real
/// trade-off rather than an oversight:
///
/// - `true` (`/series`) projects each metric's own field and skips a row where
///   it is null, so a series is listed only if it has a sample. A measurement
///   gains fields over its life, and the rows written before `free` existed
///   are not `mem_free` series. One scan per metric.
/// - `false` (the label endpoints, which Grafana calls to fill a dropdown on
///   every keystroke) projects tag columns only and scans once per
///   *measurement*, attributing every row to each of that measurement's
///   metrics. Decoding one dictionary block per tag instead of every sample in
///   the window is the difference between a responsive editor and a scan, and
///   the label names and values it reports are the same set.
///
/// The `truncated` half of the return is how the caller knows to put
/// `results truncated due to limit` in the response.
fn observed_label_sets(
    db: &std::sync::Arc<chronix::Chronix>,
    namespace: Option<&str>,
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    selectors: &[Selector],
    project_value: bool,
) -> Result<ObservedLabelSets, ServerError> {
    let mut out: std::collections::BTreeSet<Vec<(String, String)>> =
        std::collections::BTreeSet::new();
    let mut truncated = false;

    // With no selector every metric is in scope; with selectors, only the ones
    // they name — which is what makes a Grafana label dropdown show the values
    // for the metric being edited rather than for the whole database.
    let all;
    let targets: Vec<(&chronix::promql::MetricRef, Option<usize>)> = if selectors.is_empty() {
        all = chronix::promql::all_metrics(db.schema_registry())
            .into_iter()
            .filter(|t| db.schema(&t.measurement).is_some())
            .collect::<Vec<_>>();
        all.iter().map(|t| (t, None)).collect()
    } else {
        selectors
            .iter()
            .enumerate()
            .flat_map(|(idx, sel)| sel.targets.iter().map(move |t| (t, Some(idx))))
            .collect()
    };

    // One scan per group. Reading the value column forces a group per metric;
    // without it the metrics of one measurement share a scan and differ only
    // in the `__name__` they contribute.
    let mut groups: Vec<Scan<'_>> = Vec::new();
    for (target, selector) in targets {
        let field = project_value.then_some(target.field.as_str());
        let key = (target.measurement.as_str(), field, selector);
        if let Some(existing) = groups.iter_mut().find(|g| g.key == key) {
            existing.names.push(target.name.as_str());
        } else {
            groups.push(Scan {
                key,
                names: vec![target.name.as_str()],
            });
        }
    }

    for group in &groups {
        let (measurement, field, selector_idx) = group.key;
        let selector = selector_idx.and_then(|i| selectors.get(i));
        let Some(schema) = db.schema(measurement) else {
            continue;
        };
        let tag_names: Vec<String> = schema
            .tag_names()
            .iter()
            .map(std::string::ToString::to_string)
            .filter(|t| t != crate::namespace::NAMESPACE_TAG)
            .collect();

        let mut builder = db
            .query()
            .measurement(measurement)
            .namespace_scope(namespace)
            .range(start_ns, end_ns);
        for tag in &tag_names {
            builder = builder.field(tag);
        }
        if let Some(field) = field {
            builder = builder.field(field);
        }
        if let Some(sel) = selector {
            for (key, value) in &sel.pushdown {
                builder = builder.tag(key, value);
            }
        }
        let Ok(plan) = builder.build() else { continue };
        let Ok(stream) = db.execute_iter(&plan) else {
            continue;
        };

        for batch in stream {
            let batch = batch.map_err(|e| {
                ServerError::Internal(format!("failed to read '{measurement}': {e}"))
            })?;
            let columns: Vec<(&String, Option<&arrow::array::StringArray>)> = tag_names
                .iter()
                .map(|tag| {
                    (
                        tag,
                        batch
                            .column_by_name(tag)
                            .and_then(|c| c.as_any().downcast_ref::<arrow::array::StringArray>()),
                    )
                })
                .collect();
            let value_col = field.and_then(|f| batch.column_by_name(f));
            if field.is_some() && value_col.is_none() {
                // The batch predates the field: no sample of this metric.
                continue;
            }

            for row in 0..batch.num_rows() {
                if value_col.is_some_and(|c| arrow::array::Array::is_null(c.as_ref(), row)) {
                    continue;
                }
                // Build the whole label set before testing it: a non-equality
                // matcher on one tag decides whether the *other* tags' values
                // on this row exist at all.
                let mut tags: Vec<(String, String)> = Vec::with_capacity(tag_names.len() + 1);
                for (tag, arr) in &columns {
                    if let Some(arr) = arr {
                        if !arr.is_null(row) {
                            tags.push(((*tag).clone(), arr.value(row).to_string()));
                        }
                    }
                }
                for name in &group.names {
                    let mut pairs = tags.clone();
                    pairs.push(("__name__".to_string(), (*name).to_string()));
                    pairs.sort();
                    if let Some(sel) = selector {
                        if !chronix::promql::label_set_matches(&sel.compiled, &pairs) {
                            continue;
                        }
                    }
                    out.insert(pairs);
                    // Keep the smallest `limit` sets rather than stopping at
                    // the first `limit` the scan meets, so a truncated answer
                    // is the sorted **prefix** at every limit instead of
                    // moving with the data's physical layout. Memory stays
                    // bounded at `limit + 1`; the cost is the early exit, and
                    // it is paid only in the truncating case.
                    if out.len() > limit {
                        out.pop_last();
                        truncated = true;
                    }
                }
            }
        }
    }

    Ok((out.into_iter().collect(), truncated))
}

/// The cardinality cap one request enumerates under, and its source.
///
/// The request's `limit` and the server's `prom_series_limit` are both
/// ceilings, so the effective one is the smaller. A `limit` above the
/// server's cap does not raise it — it is an operator's bound, not a
/// client's.
fn effective_limit(server_cap: usize, requested: Option<usize>) -> usize {
    match requested {
        Some(n) => n.min(server_cap),
        None => server_cap,
    }
}

/// One scan of `observed_label_sets`: a measurement, optionally a value
/// column, and the metric names its rows are attributed to.
struct Scan<'a> {
    /// Measurement, the value column when one is projected, and the index of
    /// the selector this scan answers.
    key: (&'a str, Option<&'a str>, Option<usize>),
    /// The metric names every row of the scan is attributed to.
    names: Vec<&'a str>,
}

/// `GET /api/v1/prom/series` handler.
///
/// Enumerates the distinct label-sets present in the data for the given
/// matchers — conforming to the Prometheus `/api/v1/series` contract, and
/// through the same metric resolution the evaluator uses, so a selector that
/// answers here answers there.
pub async fn prom_series_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    prom_params: super::PromParams,
) -> Result<Json<PromListResponse>, PromApiError> {
    let db = state.db.clone();
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let params = PromSeriesQuery::from_params(&prom_params)?;
    let (start_ns, end_ns) = params.window()?;
    let matchers = params.matchers;
    let requested = prom_params.limit()?;
    let series_limit = effective_limit(state.config.server.prom_series_limit, requested);

    let (series_list, truncated) = tokio::task::spawn_blocking(
        move || -> Result<(Vec<serde_json::Value>, bool), ServerError> {
            let selectors = parse_selectors(&db, &matchers)?;
            let (sets, truncated) = observed_label_sets(
                &db,
                scope.as_deref(),
                start_ns,
                end_ns,
                series_limit,
                &selectors,
                true,
            )?;
            Ok((
                sets.into_iter()
                    .map(|pairs| {
                        serde_json::Value::Object(
                            pairs
                                .into_iter()
                                .map(|(k, v)| (k, serde_json::Value::String(v)))
                                .collect(),
                        )
                    })
                    .collect(),
                truncated,
            ))
        },
    )
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))??;

    Ok(Json(PromListResponse::success(
        serde_json::to_value(series_list).unwrap_or_default(),
        truncated,
    )))
}

// ── Prometheus metadata endpoint ────────────────────────────────────────────

/// Prometheus metadata API response (no `resultType` wrapper — matches
/// the official `/api/v1/metadata` format used by Grafana).
#[derive(Debug, Serialize)]
pub struct PromMetadataResponse {
    status: String,
    data: serde_json::Value,
}

/// `GET /api/v1/prom/metadata` — target metadata (Grafana compatibility).
pub async fn prom_metadata_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
) -> Result<Json<PromMetadataResponse>, PromApiError> {
    let db = state.db.clone();
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let limit = state.config.server.prom_series_limit;
    let now_ns = crate::util::now_nanos()?;
    let start_ns = now_ns - 3_600_000_000_000;

    let metadata =
        tokio::task::spawn_blocking(move || -> Result<serde_json::Value, ServerError> {
            let registry = db.schema_registry();
            // Only measurements this namespace actually holds data for: the
            // schema registry is process-wide, so listing it verbatim told
            // every tenant what the others were writing.
            let names =
                crate::namespace::measurements_in(&db, scope.as_deref(), start_ns, now_ns, limit);

            let mut result = serde_json::Map::new();
            for name in &names {
                for metric in chronix::promql::metrics_of(registry, name) {
                    // Keyed by the **metric** name, which is what a query
                    // returns and what a selector accepts. Keying by
                    // measurement told Grafana's browser about `cpu` when the
                    // series are `cpu_usage` and `cpu_load`.
                    result.insert(
                        metric.name,
                        serde_json::json!([{
                            "type": "gauge",
                            "help": format!("Field {} in measurement {name}", metric.field),
                            "unit": ""
                        }]),
                    );
                }
            }

            Ok(serde_json::Value::Object(result))
        })
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))??;

    Ok(Json(PromMetadataResponse {
        status: "success".into(),
        data: metadata,
    }))
}

/// Render a PromQL value, applying the request's `limit` to the number of
/// series and saying so when it bites.
///
/// Prometheus limits *series*, not samples, and leaves a scalar or a string
/// alone — `limit` on `/query?query=1` means nothing, so it does nothing.
fn prom_value_to_response(
    value: chronix::promql::PromQLValue,
    limit: Option<usize>,
) -> PromResponse {
    use chronix::promql::PromQLValue;

    let mut truncated = false;
    let value = match (value, limit) {
        (PromQLValue::Vector(mut series), Some(n)) if series.len() > n => {
            series.truncate(n);
            truncated = true;
            PromQLValue::Vector(series)
        }
        (PromQLValue::Matrix(mut series), Some(n)) if series.len() > n => {
            series.truncate(n);
            truncated = true;
            PromQLValue::Matrix(series)
        }
        (other, _) => other,
    };

    let (result_type, result) = match value {
        PromQLValue::Scalar(v) => ("scalar".into(), serde_json::json!([0, v.to_string()])),
        PromQLValue::String(s) => ("string".into(), serde_json::json!([0, s])),
        PromQLValue::Vector(series) => {
            let data: Vec<serde_json::Value> = series
                .iter()
                .map(|s| {
                    let labels: serde_json::Map<String, serde_json::Value> = s
                        .labels
                        .iter()
                        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                        .collect();

                    let value = s.samples.first().map(|sample| {
                        let ts = sample.timestamp as f64 / 1_000_000_000.0;
                        serde_json::json!([ts, sample.value.to_string()])
                    });

                    serde_json::json!({
                        "metric": labels,
                        "value": value,
                    })
                })
                .collect();
            ("vector".into(), serde_json::json!(data))
        }
        PromQLValue::Matrix(series) => {
            let data: Vec<serde_json::Value> = series
                .iter()
                .map(|s| {
                    let labels: serde_json::Map<String, serde_json::Value> = s
                        .labels
                        .iter()
                        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                        .collect();

                    let values: Vec<serde_json::Value> = s
                        .samples
                        .iter()
                        .map(|sample| {
                            let ts = sample.timestamp as f64 / 1_000_000_000.0;
                            serde_json::json!([ts, sample.value.to_string()])
                        })
                        .collect();

                    serde_json::json!({
                        "metric": labels,
                        "values": values,
                    })
                })
                .collect();
            ("matrix".into(), serde_json::json!(data))
        }
    };

    PromResponse {
        status: "success".into(),
        data: Some(PromData {
            result_type,
            result,
        }),
        error: None,
        error_type: None,
        warnings: if truncated {
            vec![TRUNCATED_WARNING.to_string()]
        } else {
            Vec::new()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prom_response_format() {
        let resp = PromResponse {
            status: "success".into(),
            data: Some(PromData {
                result_type: "vector".into(),
                result: serde_json::json!([]),
            }),
            error: None,
            error_type: None,
            warnings: Vec::new(),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["status"], "success");
        assert_eq!(json["data"]["resultType"], "vector");
    }
}
