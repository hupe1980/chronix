//! Prometheus-compatible (PromQL) endpoint handlers.

use arrow::array::Array;
use axum::extract::{Json, Path, State};
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use crate::error::ServerError;

use super::types::{secs_to_nanos_i64, AppState};

/// `GET/POST /api/v1/prom/query` — PromQL instant query.
#[derive(Debug, Deserialize)]
pub struct PromInstantQuery {
    /// The PromQL expression.
    pub query: String,
    /// Evaluation timestamp (Unix seconds, float). Defaults to now.
    #[serde(default)]
    pub time: Option<f64>,
}

/// `GET/POST /api/v1/prom/query_range` — PromQL range query.
#[derive(Debug, Deserialize)]
pub struct PromRangeQuery {
    /// The PromQL expression.
    pub query: String,
    /// Start timestamp (Unix seconds, float).
    pub start: f64,
    /// End timestamp (Unix seconds, float).
    pub end: f64,
    /// Step in seconds (float).
    pub step: f64,
}

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
}

impl PromListResponse {
    /// A successful list response.
    fn success(data: serde_json::Value) -> Self {
        Self {
            status: "success".into(),
            data,
            error: None,
            error_type: None,
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
    params: axum::extract::Query<PromInstantQuery>,
) -> impl IntoResponse {
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let query_str = &params.query;
    let eval_time_secs = params.time.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64()
    });
    let eval_time_ns = secs_to_nanos_i64(eval_time_secs);

    let expr = match chronix::promql::parse(query_str) {
        Ok(e) => e,
        Err(e) => {
            return Json(PromResponse {
                status: "error".into(),
                data: None,
                error: Some(e.to_string()),
                error_type: Some("bad_data".into()),
            });
        }
    };

    let query_kind = "instant";
    let evaluator = chronix::promql::PromQLEvaluator::new(state.db.clone()).with_namespace(scope);
    let params = chronix::promql::eval::QueryParams {
        time: eval_time_ns,
        ..Default::default()
    };

    let timeout_secs = state.config.prom_query_timeout_secs;
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
                return Json(PromResponse {
                    status: "error".into(),
                    data: None,
                    error: Some(format!("query timed out after {timeout_secs}s")),
                    error_type: Some("timeout".into()),
                });
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
        Ok(Ok(value)) => Json(prom_value_to_response(value)),
        Ok(Err(e)) => Json(PromResponse {
            status: "error".into(),
            data: None,
            error: Some(e.to_string()),
            error_type: Some("execution".into()),
        }),
        Err(e) => Json(PromResponse {
            status: "error".into(),
            data: None,
            error: Some(format!("query task failed: {e}")),
            error_type: Some("internal".into()),
        }),
    }
}

/// `GET/POST /api/v1/prom/query_range` handler.
pub async fn prom_range_query_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    params: axum::extract::Query<PromRangeQuery>,
) -> impl IntoResponse {
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let query_str = &params.query;
    let start_ns = secs_to_nanos_i64(params.start);
    let end_ns = secs_to_nanos_i64(params.end);
    let step_ns = secs_to_nanos_i64(params.step);

    // Reject NaN/Infinity in start/end/step early so callers get a clear
    // error instead of silently querying around epoch 0 or i64::MAX.
    if params.start.is_nan()
        || params.start.is_infinite()
        || params.end.is_nan()
        || params.end.is_infinite()
        || params.step.is_nan()
        || params.step.is_infinite()
    {
        return Json(PromResponse {
            status: "error".into(),
            data: None,
            error: Some("start, end, and step must be finite numbers".into()),
            error_type: Some("bad_data".into()),
        });
    }

    if step_ns <= 0 {
        return Json(PromResponse {
            status: "error".into(),
            data: None,
            error: Some("step must be positive".into()),
            error_type: Some("bad_data".into()),
        });
    }

    if start_ns > end_ns {
        return Json(PromResponse {
            status: "error".into(),
            data: None,
            error: Some("end timestamp must not be before start time".into()),
            error_type: Some("bad_data".into()),
        });
    }

    // Guard against excessive evaluation points that could OOM the server.
    let max_points = state.config.max_range_query_points;
    // Use i128 to avoid i64 overflow when end_ns and start_ns span the full i64 range.
    let span = (end_ns as i128 - start_ns as i128) as u64;
    let num_points = span.checked_div(step_ns as u64).unwrap_or(0) + 1;
    if num_points > max_points {
        return Json(PromResponse {
            status: "error".into(),
            data: None,
            error: Some(format!(
                "query would produce {num_points} evaluation points, exceeding limit of {max_points}"
            )),
            error_type: Some("bad_data".into()),
        });
    }

    let expr = match chronix::promql::parse(query_str) {
        Ok(e) => e,
        Err(e) => {
            return Json(PromResponse {
                status: "error".into(),
                data: None,
                error: Some(e.to_string()),
                error_type: Some("bad_data".into()),
            });
        }
    };

    let query_kind = "range";
    let evaluator = chronix::promql::PromQLEvaluator::new(state.db.clone()).with_namespace(scope);
    let qparams = chronix::promql::eval::QueryParams {
        time: end_ns,
        start: Some(start_ns),
        end: Some(end_ns),
        step: Some(step_ns),
        max_series: state.config.prom_series_limit,
        max_memory_bytes: state.config.prom_max_result_bytes,
        ..Default::default()
    };

    let timeout_secs = state.config.prom_query_timeout_secs;
    let task = tokio::task::spawn_blocking(move || {
        let value = evaluator.range_query(&expr, &qparams);
        (value, evaluator.scan_stats())
    });
    let result = if timeout_secs > 0 {
        match tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), task).await {
            Ok(r) => r,
            Err(_) => {
                return Json(PromResponse {
                    status: "error".into(),
                    data: None,
                    error: Some(format!("query timed out after {timeout_secs}s")),
                    error_type: Some("timeout".into()),
                });
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
        Ok(Ok(value)) => Json(prom_value_to_response(value)),
        Ok(Err(e)) => Json(PromResponse {
            status: "error".into(),
            data: None,
            error: Some(e.to_string()),
            error_type: Some("execution".into()),
        }),
        Err(e) => Json(PromResponse {
            status: "error".into(),
            data: None,
            error: Some(format!("query task failed: {e}")),
            error_type: Some("internal".into()),
        }),
    }
}

/// `GET /api/v1/prom/labels` — every label name in the window.
pub async fn prom_labels_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
) -> Result<Json<PromListResponse>, ServerError> {
    let db = state.db.clone();
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let params = PromSeriesQuery::parse(raw_query.as_deref());
    let (start_ns, end_ns) = params.window()?;
    let selectors = parse_selectors(&params.matchers)?;
    let limit = state.config.prom_series_limit;

    let labels = tokio::task::spawn_blocking(move || -> Result<Vec<String>, ServerError> {
        let mut label_set = std::collections::BTreeSet::new();
        label_set.insert("__name__".to_string());
        for (key, _) in observed_labels(&db, scope.as_deref(), start_ns, end_ns, limit, &selectors)?
        {
            label_set.insert(key);
        }
        Ok(label_set.into_iter().collect())
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))??;

    Ok(Json(PromListResponse::success(
        serde_json::to_value(labels).unwrap_or_default(),
    )))
}

/// `GET /api/v1/prom/label/{name}/values` — label values for a given label.
pub async fn prom_label_values_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    Path(label_name): Path<String>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
) -> Result<Json<PromListResponse>, ServerError> {
    let db = state.db.clone();
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let params = PromSeriesQuery::parse(raw_query.as_deref());
    let (start_ns, end_ns) = params.window()?;
    let selectors = parse_selectors(&params.matchers)?;
    let limit = state.config.prom_series_limit;

    let values = tokio::task::spawn_blocking(move || -> Result<Vec<String>, ServerError> {
        if label_name == "__name__" {
            let names =
                crate::namespace::measurements_in(&db, scope.as_deref(), start_ns, end_ns, limit);
            if selectors.is_empty() {
                return Ok(names);
            }
            // With matchers, `__name__`'s values are the measurements the
            // selectors actually name.
            return Ok(names
                .into_iter()
                .filter(|m| selectors.iter().any(|sel| sel.measurement == *m))
                .collect());
        }
        let mut values = std::collections::BTreeSet::new();
        for (key, value) in
            observed_labels(&db, scope.as_deref(), start_ns, end_ns, limit, &selectors)?
        {
            if key == label_name {
                values.insert(value);
            }
        }
        Ok(values.into_iter().collect())
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))??;

    Ok(Json(PromListResponse::success(
        serde_json::to_value(values).unwrap_or_default(),
    )))
}

/// One parsed `match[]` selector: the measurement it names and the rest of its
/// matchers, compiled.
struct Selector {
    measurement: String,
    /// Equality matchers, pushed into the scan.
    pushdown: Vec<(String, String)>,
    /// Every matcher except `__name__`, applied to the label sets produced.
    compiled: Vec<chronix::promql::CompiledMatcher>,
}

/// Parse `match[]` selectors into measurements plus compiled matchers.
///
/// A selector that names no measurement is dropped rather than treated as
/// "everything": `match[]={host="a"}` cannot be answered without scanning every
/// measurement, and Prometheus requires at least one non-empty matcher anyway.
fn parse_selectors(matchers: &[String]) -> Result<Vec<Selector>, ServerError> {
    let mut out = Vec::new();
    for raw in matchers {
        let expr =
            chronix::promql::parse(raw).map_err(|e| ServerError::BadRequest(e.to_string()))?;
        let chronix::promql::Expr::VectorSelector { name, matchers, .. } = &expr else {
            return Err(ServerError::BadRequest(format!(
                "match[] must be a series selector, got: {raw}"
            )));
        };
        let Some(measurement) = name.as_deref().or_else(|| {
            matchers
                .iter()
                .find(|m| m.name == "__name__" && m.op == chronix::promql::MatchOp::Equal)
                .map(|m| m.value.as_str())
        }) else {
            continue;
        };
        out.push(Selector {
            measurement: measurement.to_string(),
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
    /// Optional start time filter (seconds since epoch).
    pub start: Option<f64>,
    /// Optional end time filter (seconds since epoch).
    pub end: Option<f64>,
}

impl PromSeriesQuery {
    /// Parse from a raw `application/x-www-form-urlencoded` query string.
    fn parse(raw: Option<&str>) -> Self {
        let mut out = Self::default();
        for (key, value) in form_urlencoded::parse(raw.unwrap_or_default().as_bytes()) {
            match key.as_ref() {
                // Prometheus clients send `match[]`; accept the unbracketed
                // spelling too, since hand-written curl calls use it.
                "match[]" | "match" => out.matchers.push(value.into_owned()),
                "start" => out.start = value.parse().ok(),
                "end" => out.end = value.parse().ok(),
                _ => {}
            }
        }
        out
    }

    /// Resolve the window, defaulting to the last hour.
    ///
    /// Prometheus bounds these endpoints by time for the same reason: an
    /// unbounded label enumeration is a full scan of every series ever
    /// written, issued by a dashboard on every page load.
    fn window(&self) -> Result<(i64, i64), ServerError> {
        let now_ns = crate::util::now_nanos()?;
        let one_hour_ns: i64 = 3_600_000_000_000;
        let start = self
            .start
            .map_or(now_ns - one_hour_ns, super::types::secs_to_nanos_i64);
        let end = self.end.map_or(now_ns, super::types::secs_to_nanos_i64);
        Ok((start, end))
    }
}

/// Distinct `(label, value)` pairs actually present for `namespace`, restricted
/// to the given selectors when there are any.
///
/// Derived from the data rather than from the tag inverted index, which is
/// built from *segments*: a label whose only points are still in the memtable
/// was missing from every Grafana dropdown until the first flush, and the
/// index carries no namespace dimension, so it answered with every tenant's
/// values.
fn observed_labels(
    db: &std::sync::Arc<chronix::Chronix>,
    namespace: Option<&str>,
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    selectors: &[Selector],
) -> Result<Vec<(String, String)>, ServerError> {
    let mut out = std::collections::BTreeSet::new();

    // With no selector, every measurement is in scope; with selectors, only
    // the ones they name — which is what makes a Grafana label dropdown show
    // the values for the metric being edited rather than for the whole
    // database.
    let scanned: Vec<(String, Option<&Selector>)> = if selectors.is_empty() {
        db.schema_registry()
            .measurement_names()
            .into_iter()
            .map(|m| (m, None))
            .collect()
    } else {
        selectors
            .iter()
            .map(|sel| (sel.measurement.clone(), Some(sel)))
            .collect()
    };

    for (measurement, selector) in scanned {
        let Some(schema) = db.schema(&measurement) else {
            continue;
        };
        let tag_names: Vec<String> = schema
            .tag_names()
            .iter()
            .map(std::string::ToString::to_string)
            .filter(|t| t != crate::namespace::NAMESPACE_TAG)
            .collect();
        if tag_names.is_empty() {
            continue;
        }

        // Project the tag columns only: field values are never inspected, and
        // not decoding them is the difference between reading a handful of
        // dictionary blocks and reading every sample in the window.
        let mut builder = db
            .query()
            .measurement(&measurement)
            .namespace_scope(namespace)
            .range(start_ns, end_ns);
        for tag in &tag_names {
            builder = builder.field(tag);
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

        'batches: for batch in stream {
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

            for row in 0..batch.num_rows() {
                // Build the whole label set before testing it: a non-equality
                // matcher on one tag decides whether the *other* tags' values
                // on this row exist at all.
                let mut pairs: Vec<(String, String)> =
                    vec![("__name__".to_string(), measurement.clone())];
                for (tag, arr) in &columns {
                    if let Some(arr) = arr {
                        if !arr.is_null(row) {
                            pairs.push(((*tag).clone(), arr.value(row).to_string()));
                        }
                    }
                }
                if let Some(sel) = selector {
                    if !chronix::promql::label_set_matches(&sel.compiled, &pairs) {
                        continue;
                    }
                }
                for (key, value) in pairs.into_iter().skip(1) {
                    out.insert((key, value));
                    if out.len() >= limit {
                        tracing::warn!(
                            "prom label enumeration hit the series limit ({limit}), truncating"
                        );
                        break 'batches;
                    }
                }
            }
        }
    }

    Ok(out.into_iter().collect())
}

/// `GET /api/v1/prom/series` handler.
///
/// Enumerates the distinct label-sets present in the data for the given
/// matchers — conforming to the Prometheus `/api/v1/series` contract.
pub async fn prom_series_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
) -> Result<Json<PromListResponse>, ServerError> {
    let db = state.db.clone();
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let params = PromSeriesQuery::parse(raw_query.as_deref());
    let matchers = params.matchers;
    let series_limit = state.config.prom_series_limit;

    // Convert optional Prometheus time bounds (seconds since
    // epoch, f64) to nanosecond timestamps for query range filtering.
    // When no range is provided, default to the last hour to prevent
    // accidental full-table scans on large datasets (Prometheus convention).
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    let one_hour_ns: i64 = 3_600_000_000_000;
    let start_ns = params
        .start
        .map_or(now_ns - one_hour_ns, |s| (s * 1_000_000_000.0) as i64);
    let end_ns = params.end.map_or(now_ns, |e| (e * 1_000_000_000.0) as i64);

    let series_list =
        tokio::task::spawn_blocking(move || -> Result<Vec<serde_json::Value>, ServerError> {
            let mut result = Vec::new();

            for matcher_str in &matchers {
                // Parse the matcher as a PromQL selector
                let expr = chronix::promql::parse(matcher_str)
                    .map_err(|e| ServerError::BadRequest(e.to_string()))?;

                if let chronix::promql::Expr::VectorSelector { name, matchers, .. } = &expr {
                    // Resolve the measurement name from either the explicit
                    // name or a `__name__="..."` equality matcher.
                    let measurement = name.as_deref().or_else(|| {
                        matchers
                            .iter()
                            .find(|m| {
                                m.name == "__name__" && m.op == chronix::promql::MatchOp::Equal
                            })
                            .map(|m| m.value.as_str())
                    });
                    let Some(measurement) = measurement else {
                        continue;
                    };

                    // Everything except `__name__` still has to be applied:
                    // resolving only the measurement and enumerating the whole
                    // of it answers `{__name__="cpu",host="a"}` with every
                    // host.
                    let compiled = chronix::promql::compile_label_matchers(matchers)
                        .map_err(|e| ServerError::BadRequest(e.to_string()))?;
                    // Equality matchers go into the scan; the rest are applied
                    // to the label sets it produces.
                    let pushdown: Vec<(String, String)> = matchers
                        .iter()
                        .filter(|m| {
                            m.name != "__name__"
                                && m.op == chronix::promql::MatchOp::Equal
                                && m.name != crate::namespace::NAMESPACE_TAG
                        })
                        .map(|m| (m.name.clone(), m.value.clone()))
                        .collect();

                    let tag_names: Vec<String> = db
                        .schema(measurement)
                        .map(|s| {
                            s.tag_names()
                                .iter()
                                .map(std::string::ToString::to_string)
                                .filter(|t| t != crate::namespace::NAMESPACE_TAG)
                                .collect()
                        })
                        .unwrap_or_default();

                    // Query actual data to discover real label-sets.
                    // Bounded by the requested time range (default 1h).
                    //
                    // Only the tag columns are projected — field values are
                    // never inspected here, and not decoding them is the
                    // difference between reading a handful of dictionary
                    // blocks and reading every sample in the window.
                    let mut builder = db
                        .query()
                        .measurement(measurement)
                        .namespace_scope(scope.as_deref())
                        .range(start_ns, end_ns);
                    for tag in &tag_names {
                        builder = builder.field(tag);
                    }
                    for (key, value) in &pushdown {
                        builder = builder.tag(key, value);
                    }
                    let plan = builder.build().map_err(|e| {
                        ServerError::Internal(format!(
                            "failed to build query for '{measurement}': {e}"
                        ))
                    })?;
                    let stream = db.execute_iter(&plan).map_err(|e| {
                        ServerError::Internal(format!(
                            "failed to execute query for '{measurement}': {e}"
                        ))
                    })?;

                    // Collect distinct label-sets from the actual data,
                    // enforcing the cardinality limit for safety. Streaming
                    // means hitting the limit stops the scan rather than
                    // stopping the loop over an already-materialised result.
                    let mut seen = std::collections::HashSet::new();
                    'batches: for batch in stream {
                        let batch = batch.map_err(|e| {
                            ServerError::Internal(format!("failed to read '{measurement}': {e}"))
                        })?;
                        for row in 0..batch.num_rows() {
                            if result.len() >= series_limit {
                                tracing::warn!(
                                "prom /series hit cardinality limit ({series_limit}), truncating"
                            );
                                break 'batches;
                            }

                            let mut pairs: Vec<(String, String)> =
                                vec![("__name__".to_string(), measurement.to_owned())];
                            for tag in &tag_names {
                                if let Some(col) = batch.column_by_name(tag) {
                                    if let Some(arr) =
                                        col.as_any().downcast_ref::<arrow::array::StringArray>()
                                    {
                                        if !arr.is_null(row) {
                                            pairs.push((tag.clone(), arr.value(row).to_string()));
                                        }
                                    }
                                }
                            }
                            if !chronix::promql::label_set_matches(&compiled, &pairs) {
                                continue;
                            }
                            let key = format!("{pairs:?}");
                            if seen.insert(key) {
                                result.push(serde_json::Value::Object(
                                    pairs
                                        .into_iter()
                                        .map(|(k, v)| (k, serde_json::Value::String(v)))
                                        .collect(),
                                ));
                            }
                        }
                    }
                }
            }

            Ok(result)
        })
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))??;

    Ok(Json(PromListResponse::success(
        serde_json::to_value(series_list).unwrap_or_default(),
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
) -> Result<Json<PromMetadataResponse>, ServerError> {
    let db = state.db.clone();
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let limit = state.config.prom_series_limit;
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
                if let Some(schema) = registry.lookup(name) {
                    let fields: Vec<serde_json::Value> = schema
                        .field_names()
                        .iter()
                        .map(|f| {
                            serde_json::json!({
                                "type": "gauge",
                                "help": format!("Field {f} in measurement {name}"),
                                "unit": ""
                            })
                        })
                        .collect();
                    result.insert(name.clone(), serde_json::Value::Array(fields));
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

fn prom_value_to_response(value: chronix::promql::PromQLValue) -> PromResponse {
    use chronix::promql::PromQLValue;

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
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["status"], "success");
        assert_eq!(json["data"]["resultType"], "vector");
    }
}
