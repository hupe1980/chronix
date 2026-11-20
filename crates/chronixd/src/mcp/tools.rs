//! The tools.
//!
//! Each one is an adapter over something the HTTP API already does — most
//! call the route's own handler, so a tool and its route cannot disagree
//! about scope, bounds or errors. A tool that needs logic of its own is a
//! sign the logic belongs in the facade.

use std::collections::BTreeMap;

use axum::extract::{Extension, Path, State};
use axum::response::IntoResponse;
use rmcp::schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::error::ServerError;
use crate::http::{AppState, PromParams};
use crate::namespace::NamespaceContext;

/// What a tool returns: a structure for programs, a sentence for agents.
pub(crate) struct Output {
    pub structured: Value,
    pub summary: String,
}

/// A refusal, as the client of the matching HTTP route would see it.
#[derive(Debug)]
pub(crate) struct ToolError {
    pub message: String,
    pub hint: Option<String>,
}

impl ToolError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            hint: None,
        }
    }
}

impl From<ServerError> for ToolError {
    fn from(e: ServerError) -> Self {
        let (message, hint) = e.client_view();
        Self { message, hint }
    }
}

/// The JSON a handler answered, or the refusal it answered with.
async fn answer(resp: axum::response::Response) -> Result<Value, ToolError> {
    let status = resp.status();
    // Bounded by what a handler can produce; the tool bounds it again below.
    let bytes = axum::body::to_bytes(resp.into_body(), 64 * 1024 * 1024)
        .await
        .map_err(|e| ToolError::new(e.to_string()))?;
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|e| ToolError::new(format!("bad answer: {e}")))?;
    let prom_error = value.get("status").and_then(Value::as_str) == Some("error");
    if status.is_success() && !prom_error {
        return Ok(value);
    }
    Err(ToolError {
        message: value
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("the request failed")
            .to_owned(),
        hint: value.get("hint").and_then(Value::as_str).map(str::to_owned),
    })
}

/// Who is asking: the server state and the request's resolved namespace.
#[derive(Clone)]
pub(crate) struct Ctx {
    pub state: AppState,
    pub ns: Option<NamespaceContext>,
    pub max_bytes: usize,
}

impl Ctx {
    fn ext(&self) -> Option<Extension<NamespaceContext>> {
        self.ns.clone().map(Extension)
    }

    fn scope(&self) -> Option<String> {
        crate::namespace::scope(&self.state, self.ns.as_ref()).map(str::to_owned)
    }

    fn now(&self) -> i64 {
        i64::try_from(jiff::Timestamp::now().as_nanosecond()).unwrap_or(i64::MAX)
    }

    fn time(&self, key: &str, spec: Option<&TimeArg>, default: &str) -> Result<i64, ToolError> {
        let text = spec.map_or_else(|| default.to_owned(), TimeArg::text);
        Ok(super::time::parse(key, &text, self.now())?)
    }
}

/// A time: `"now"`, `"now-7d"`, RFC 3339, or Unix seconds.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(untagged)]
pub(crate) enum TimeArg {
    /// `"now"`, `"now-7d"`, `"2026-11-17T08:00:00Z"`, or `"1763366400"`.
    Text(String),
    /// Unix seconds.
    Seconds(f64),
}

impl TimeArg {
    fn text(&self) -> String {
        match self {
            Self::Text(s) => s.clone(),
            Self::Seconds(n) => n.to_string(),
        }
    }
}

fn rfc3339(ns: i64) -> String {
    jiff::Timestamp::from_nanosecond(i128::from(ns)).map_or_else(
        |_| ns.to_string(),
        |t| t.strftime("%Y-%m-%dT%H:%M:%SZ").to_string(),
    )
}

/// Keep the first items of `items` that fit in `max_items` and `max_bytes`.
fn bounded(items: Vec<Value>, max_items: usize, max_bytes: usize) -> (Vec<Value>, bool) {
    let total = items.len();
    let mut out = Vec::new();
    let mut bytes = 0;
    for item in items.into_iter().take(max_items) {
        let size = serde_json::to_vec(&item).map_or(0, |v| v.len()) + 1;
        if bytes + size > max_bytes && !out.is_empty() {
            break;
        }
        bytes += size;
        out.push(item);
    }
    let truncated = out.len() < total;
    (out, truncated)
}

fn truncation_note(truncated: bool) -> &'static str {
    if truncated { " (truncated)" } else { "" }
}

// ── list_metrics ─────────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(deny_unknown_fields)]
pub(crate) struct ListMetricsArgs {
    /// Regular expression the metric name must match, e.g. `"^pv_"`.
    #[serde(rename = "match")]
    pub pattern: Option<String>,
    /// At most this many metrics (default 500, at most 5000).
    pub limit: Option<usize>,
}

pub(crate) async fn list_metrics(ctx: &Ctx, args: ListMetricsArgs) -> Result<Output, ToolError> {
    let pattern = args
        .pattern
        .as_deref()
        .map(regex::Regex::new)
        .transpose()
        .map_err(|e| ToolError::new(format!("match is not a regular expression: {e}")))?;
    let limit = args.limit.unwrap_or(500).clamp(1, 5000);
    let db = ctx.state.db.clone();
    let scope = ctx.scope();
    let metrics = tokio::task::spawn_blocking(move || {
        // Every measurement with data at any time: a directory copied off a
        // device may hold nothing from the last hour.
        let names =
            crate::namespace::measurements_in(&db, scope.as_deref(), 0, i64::MAX, usize::MAX);
        let registry = db.schema_registry();
        let mut out = Vec::new();
        for m in &names {
            let histogram_fields: Vec<String> = db
                .schema(m)
                .map(|s| {
                    s.columns()
                        .iter()
                        .filter(|c| {
                            c.column_type == chronix::chronix_core::schema::ColumnType::Histogram
                        })
                        .map(|c| c.name.clone())
                        .collect()
                })
                .unwrap_or_default();
            for metric in chronix::promql::metrics_of(registry, m) {
                let kind = if histogram_fields.contains(&metric.field) {
                    "histogram"
                } else if metric.name.ends_with("_total") {
                    "counter"
                } else {
                    "gauge"
                };
                out.push((metric.name, kind, metric.measurement, metric.field));
            }
        }
        out
    })
    .await
    .map_err(|e| ToolError::new(e.to_string()))?;

    let items: Vec<Value> = metrics
        .into_iter()
        .filter(|(name, ..)| pattern.as_ref().is_none_or(|p| p.is_match(name)))
        .map(|(name, kind, measurement, field)| {
            json!({ "name": name, "type": kind, "measurement": measurement, "field": field })
        })
        .collect();
    let total = items.len();
    let (metrics, truncated) = bounded(items, limit, ctx.max_bytes);
    let shown: Vec<&str> = metrics
        .iter()
        .take(5)
        .filter_map(|m| m["name"].as_str())
        .collect();
    let summary = format!(
        "{total} metric{}{}{}",
        if total == 1 { "" } else { "s" },
        if shown.is_empty() {
            String::new()
        } else {
            format!(
                ": {}{}",
                shown.join(", "),
                if total > shown.len() { ", …" } else { "" }
            )
        },
        truncation_note(truncated)
    );
    Ok(Output {
        structured: json!({ "metrics": metrics, "truncated": truncated }),
        summary,
    })
}

// ── label_values ─────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(deny_unknown_fields)]
pub(crate) struct LabelValuesArgs {
    /// The label, e.g. `"device"`.
    pub label: String,
    /// Only values that series of this metric carry, e.g. `"pv_power_w"`.
    pub metric: Option<String>,
    /// Window start (default `"now-30d"`).
    pub start: Option<TimeArg>,
    /// Window end (default `"now"`).
    pub end: Option<TimeArg>,
    /// At most this many values (default 500, at most 5000).
    pub limit: Option<usize>,
}

pub(crate) async fn label_values(ctx: &Ctx, args: LabelValuesArgs) -> Result<Output, ToolError> {
    let start = ctx.time("start", args.start.as_ref(), "now-30d")?;
    let end = ctx.time("end", args.end.as_ref(), "now")?;
    let mut pairs = vec![
        ("start".to_owned(), (start as f64 / 1e9).to_string()),
        ("end".to_owned(), (end as f64 / 1e9).to_string()),
    ];
    if let Some(m) = &args.metric {
        pairs.push(("match[]".to_owned(), m.clone()));
    }
    let resp = crate::http::prom_label_values_handler(
        State(ctx.state.clone()),
        ctx.ext(),
        Path(args.label.clone()),
        PromParams::from_pairs(pairs),
    )
    .await
    .into_response();
    let body = answer(resp).await?;
    let values: Vec<Value> = body["data"].as_array().cloned().unwrap_or_default();
    let total = values.len();
    let (values, truncated) = bounded(
        values,
        args.limit.unwrap_or(500).clamp(1, 5000),
        ctx.max_bytes,
    );
    let shown: Vec<&str> = values.iter().take(8).filter_map(Value::as_str).collect();
    Ok(Output {
        summary: format!(
            "{} has {total} value{}{}{}",
            args.label,
            if total == 1 { "" } else { "s" },
            if shown.is_empty() {
                String::new()
            } else {
                format!(
                    ": {}{}",
                    shown.join(", "),
                    if total > shown.len() { ", …" } else { "" }
                )
            },
            truncation_note(truncated)
        ),
        structured: json!({ "label": args.label, "values": values, "truncated": truncated }),
    })
}

// ── query_promql ─────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(deny_unknown_fields)]
pub(crate) struct QueryPromqlArgs {
    /// The PromQL query, e.g. `"avg_over_time(pv_power_w[1h])"`.
    pub query: String,
    /// Instant query: evaluate at this time (default `"now"`).
    pub time: Option<TimeArg>,
    /// Range query: from this time. Give `start` and `end` together.
    pub start: Option<TimeArg>,
    /// Range query: to this time.
    pub end: Option<TimeArg>,
    /// Range query: step, e.g. `"5m"` (default: about 250 points).
    pub step: Option<String>,
}

fn number(v: &Value) -> Value {
    v.as_str()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|f| f.is_finite())
        .map_or_else(|| v.clone(), |f| json!(f))
}

fn point(p: &Value) -> Value {
    json!([p[0].clone(), number(&p[1])])
}

pub(crate) async fn query_promql(ctx: &Ctx, args: QueryPromqlArgs) -> Result<Output, ToolError> {
    let mut pairs = vec![
        ("query".to_owned(), args.query.clone()),
        ("explain_empty".to_owned(), "true".to_owned()),
    ];
    let range = args.start.is_some() || args.end.is_some();
    let resp = if range {
        let start = ctx.time("start", args.start.as_ref(), "now-1h")?;
        let end = ctx.time("end", args.end.as_ref(), "now")?;
        if end <= start {
            return Err(ToolError::new("end must be after start"));
        }
        let step = match &args.step {
            Some(s) => s.clone(),
            None => format!("{}s", ((end - start) / 250 / 1_000_000_000).max(1)),
        };
        pairs.push(("start".into(), (start as f64 / 1e9).to_string()));
        pairs.push(("end".into(), (end as f64 / 1e9).to_string()));
        pairs.push(("step".into(), step));
        crate::http::prom_range_query_handler(
            State(ctx.state.clone()),
            ctx.ext(),
            PromParams::from_pairs(pairs),
        )
        .await
    } else {
        let at = ctx.time("time", args.time.as_ref(), "now")?;
        pairs.push(("time".into(), (at as f64 / 1e9).to_string()));
        crate::http::prom_instant_query_handler(
            State(ctx.state.clone()),
            ctx.ext(),
            PromParams::from_pairs(pairs),
        )
        .await
    };
    let body = answer(resp).await?;
    let data = &body["data"];
    let result_type = data["resultType"].as_str().unwrap_or("vector").to_owned();
    let warnings = body["warnings"].as_array().cloned().unwrap_or_default();
    let series: Vec<Value> = match &data["result"] {
        Value::Array(list) if result_type == "scalar" || result_type == "string" => {
            vec![json!({ "labels": {}, "points": [point(&Value::Array(list.clone()))] })]
        }
        Value::Array(list) => list
            .iter()
            .map(|s| {
                let points: Vec<Value> = if let Some(v) = s.get("value") {
                    vec![point(v)]
                } else {
                    s["values"]
                        .as_array()
                        .map(|vs| vs.iter().map(point).collect())
                        .unwrap_or_default()
                };
                let mut out = json!({ "labels": s["metric"].clone(), "points": points });
                for key in ["histogram", "histograms"] {
                    if let Some(h) = s.get(key) {
                        out[key] = h.clone();
                    }
                }
                out
            })
            .collect(),
        _ => Vec::new(),
    };
    let total = series.len();
    let (series, truncated) = bounded(series, usize::MAX, ctx.max_bytes);
    let summary = if total == 0 {
        match warnings.first().and_then(Value::as_str) {
            Some(w) => format!("no series — {w}"),
            None => "no series".to_owned(),
        }
    } else {
        let first = &series[0];
        let last = first["points"]
            .as_array()
            .and_then(|p| p.last())
            .map(|p| p[1].to_string())
            .unwrap_or_default();
        format!(
            "{total} series ({result_type}); first {} = {last}{}",
            first["labels"],
            truncation_note(truncated)
        )
    };
    Ok(Output {
        structured: json!({
            "resultType": result_type,
            "series": series,
            "truncated": truncated,
            "warnings": warnings,
        }),
        summary,
    })
}

// ── query_sql ────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(deny_unknown_fields)]
pub(crate) struct QuerySqlArgs {
    /// A read-only SQL query, e.g. `"SELECT device, max(power_w) FROM pv GROUP BY device"`.
    /// Tables are measurements; `_time` is the timestamp column.
    pub query: String,
}

pub(crate) async fn query_sql(ctx: &Ctx, args: QuerySqlArgs) -> Result<Output, ToolError> {
    let resp = crate::http::sql_handler(
        State(ctx.state.clone()),
        ctx.ext(),
        axum::Json(crate::http::SqlRequest { query: args.query }),
    )
    .await
    .into_response();
    let body = answer(resp).await?;
    let rows = body["rows"].as_array().cloned().unwrap_or_default();
    let total = rows.len();
    let (rows, cut) = bounded(rows, usize::MAX, ctx.max_bytes);
    let truncated = cut || body["truncated"].as_bool().unwrap_or(false);
    let columns: Vec<Value> = body["columns"]
        .as_array()
        .map(|cs| {
            cs.iter()
                .map(|c| json!({ "name": c["name"], "type": c["data_type"] }))
                .collect()
        })
        .unwrap_or_default();
    let names: Vec<&str> = columns.iter().filter_map(|c| c["name"].as_str()).collect();
    Ok(Output {
        summary: format!(
            "{total} row{} × {} column{} ({}){}",
            if total == 1 { "" } else { "s" },
            names.len(),
            if names.len() == 1 { "" } else { "s" },
            names.join(", "),
            truncation_note(truncated)
        ),
        structured: json!({ "columns": columns, "rows": rows, "truncated": truncated }),
    })
}

// ── explain ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(rename_all = "lowercase")]
pub(crate) enum Language {
    /// PromQL: a tree of sub-expressions.
    Promql,
    /// SQL: DataFusion's plan.
    Sql,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(deny_unknown_fields)]
pub(crate) struct ExplainArgs {
    /// The query.
    pub query: String,
    /// `"promql"` or `"sql"`.
    pub language: Language,
    /// Run it and report what each step produced (default false).
    pub analyze: Option<bool>,
    /// PromQL: evaluate at this time (default `"now"`).
    pub time: Option<TimeArg>,
}

pub(crate) async fn explain(ctx: &Ctx, args: ExplainArgs) -> Result<Output, ToolError> {
    let analyze = args.analyze.unwrap_or(false);
    match args.language {
        Language::Promql => {
            let at = ctx.time("time", args.time.as_ref(), "now")?;
            let req = crate::http::PromqlExplainRequest {
                query: args.query,
                time: Some(json!((at as f64) / 1e9)),
                analyze,
                ..Default::default()
            };
            let plan = crate::http::promql_explain(&ctx.state, ctx.scope(), req).await?;
            let tree =
                serde_json::to_value(&plan.tree).map_err(|e| ToolError::new(e.to_string()))?;
            let mut lines = Vec::new();
            render(&plan.tree, 0, &mut lines);
            Ok(Output {
                summary: lines.join("\n"),
                structured: json!({ "plan": lines.join("\n"), "tree": tree }),
            })
        }
        Language::Sql => {
            let query = format!(
                "EXPLAIN {}{}",
                if analyze { "ANALYZE " } else { "" },
                args.query
            );
            let out = query_sql(ctx, QuerySqlArgs { query }).await?;
            let plan: Vec<String> = out.structured["rows"]
                .as_array()
                .map(|rows| {
                    rows.iter()
                        .filter_map(|r| r.as_array().and_then(|c| c.last()).and_then(Value::as_str))
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default();
            let text = plan.join("\n");
            Ok(Output {
                summary: text.clone(),
                structured: json!({ "plan": text }),
            })
        }
    }
}

/// The explain tree as indented lines: `rate(m[5m])  — 2 series, 38 samples`.
fn render(node: &chronix::promql::ExplainNode, depth: usize, out: &mut Vec<String>) {
    let mut line = format!("{}{}", "  ".repeat(depth), node.text);
    if let Some(series) = node.series {
        line.push_str(&format!(
            "  — {series} series, {} samples",
            node.samples.unwrap_or(0)
        ));
    }
    if let Some(e) = &node.error {
        line.push_str(&format!("  — error: {e}"));
    }
    out.push(line);
    for c in &node.children {
        render(c, depth + 1, out);
    }
}

// ── cardinality ──────────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(deny_unknown_fields)]
pub(crate) struct CardinalityArgs {
    /// How many entries in each top list (default 20, at most 100).
    pub top: Option<usize>,
}

pub(crate) async fn cardinality(ctx: &Ctx, args: CardinalityArgs) -> Result<Output, ToolError> {
    let top = args.top.unwrap_or(20).clamp(1, 100);
    let resp = crate::http::prom_status_tsdb_handler(
        State(ctx.state.clone()),
        ctx.ext(),
        PromParams::from_pairs([("limit", top.to_string())]),
    )
    .await
    .into_response();
    let body = answer(resp).await?;
    let data = body["data"].clone();
    let series = data["headStats"]["numSeries"].as_u64().unwrap_or(0);
    let budget = &data["chronix"];
    let biggest = data["seriesCountByMetricName"]
        .as_array()
        .and_then(|l| l.first())
        .map(|m| {
            format!(
                "; most series: {} ({})",
                m["name"].as_str().unwrap_or("?"),
                m["value"]
            )
        })
        .unwrap_or_default();
    let headroom = match (budget["budget"].as_u64(), budget["headroom"].as_u64()) {
        (Some(b), Some(h)) => format!(" of a budget of {b} ({h} left)"),
        _ => String::new(),
    };
    Ok(Output {
        summary: format!("{series} series{headroom}{biggest}"),
        structured: data,
    })
}

// ── forecast and detect_anomalies: one series, resolved first ────────────

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(deny_unknown_fields)]
pub(crate) struct ForecastArgs {
    /// The metric, as PromQL names it, e.g. `"pv_power_w"`. Or give
    /// `measurement` and `field`.
    pub metric: Option<String>,
    /// The measurement (with `field`, instead of `metric`).
    pub measurement: Option<String>,
    /// The field (with `measurement`).
    pub field: Option<String>,
    /// Labels that pick one series, e.g. `{"device": "inverter-1"}`.
    pub labels: Option<BTreeMap<String, String>>,
    /// History start (default `"now-7d"`).
    pub start: Option<TimeArg>,
    /// History end (default `"now"`).
    pub end: Option<TimeArg>,
    /// Points to forecast (at most 10000): steps when `step` is given,
    /// otherwise at the series' own spacing.
    pub horizon: usize,
    /// `"auto"` (default: chosen by cross-validation), `"ses"`, `"holt"`,
    /// `"holt_winters"`, `"arima"`, `"sarima"` or `"linear_regression"`.
    pub model: Option<String>,
    /// Fit on the series averaged into buckets this wide, e.g. `"15m"`.
    /// Use it for long windows: a week at `"15m"` is 672 points and shows a
    /// daily season; a week of raw 10 s samples is 60480. At most 5000
    /// buckets.
    pub step: Option<String>,
    /// Floor for the forecast and its interval, e.g. `0` for power that
    /// cannot be negative. Reported back as `clamped_below`.
    pub min: Option<f64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(deny_unknown_fields)]
pub(crate) struct DetectArgs {
    /// The metric, as PromQL names it, e.g. `"heat_pump_power_w"`. Or give
    /// `measurement` and `field`.
    pub metric: Option<String>,
    /// The measurement (with `field`, instead of `metric`).
    pub measurement: Option<String>,
    /// The field (with `measurement`).
    pub field: Option<String>,
    /// Labels that pick one series, e.g. `{"device": "heat-pump"}`.
    pub labels: Option<BTreeMap<String, String>>,
    /// Window start (default `"now-7d"`).
    pub start: Option<TimeArg>,
    /// Window end (default `"now"`).
    pub end: Option<TimeArg>,
    /// `"zscore"` (default), `"modified_zscore"`, `"iqr"`,
    /// `"dynamic_threshold"`, `"forecast_residual"` or `"moving_average"`.
    pub method: Option<String>,
    /// Score above which a point is anomalous (default 3.0).
    pub threshold: Option<f64>,
}

/// The one series a metric and labels select in a window.
struct OneSeries {
    name: String,
    measurement: String,
    field: String,
    /// Every label of the series but `__name__`, the namespace tag included.
    tags: Vec<(String, String)>,
}

fn selector(name: &str, labels: &BTreeMap<String, String>) -> String {
    let matchers: Vec<String> = labels
        .iter()
        .map(|(k, v)| format!("{k}={}", serde_json::to_string(v).unwrap_or_default()))
        .collect();
    format!("{name}{{{}}}", matchers.join(", "))
}

async fn one_series(
    ctx: &Ctx,
    metric: Option<&str>,
    measurement: Option<&str>,
    field: Option<&str>,
    labels: &BTreeMap<String, String>,
    (start, end): (i64, i64),
) -> Result<OneSeries, ToolError> {
    let name = match (metric, measurement, field) {
        (Some(m), None, None) => m.to_owned(),
        (None, Some(m), Some(f)) => chronix::promql::metric_name(m, f),
        _ => {
            return Err(ToolError::new(
                "name the series by `metric`, or by `measurement` and `field` — one of the two",
            ));
        }
    };
    let target = chronix::promql::metric::resolve(ctx.state.db.schema_registry(), &name)
        .into_iter()
        .find(|t| t.view == chronix::promql::ClassicView::Native)
        .ok_or_else(|| ToolError {
            message: format!("no metric is called {name:?}"),
            hint: Some("list_metrics shows the metrics that exist".into()),
        })?;
    let resp = crate::http::prom_series_handler(
        State(ctx.state.clone()),
        ctx.ext(),
        PromParams::from_pairs([
            ("match[]".to_owned(), selector(&name, labels)),
            ("start".to_owned(), (start as f64 / 1e9).to_string()),
            ("end".to_owned(), (end as f64 / 1e9).to_string()),
        ]),
    )
    .await
    .into_response();
    let body = answer(resp).await?;
    let sets = body["data"].as_array().cloned().unwrap_or_default();
    match sets.as_slice() {
        [] => Err(ToolError {
            message: format!(
                "{} has no samples between {} and {}",
                selector(&name, labels),
                rfc3339(start),
                rfc3339(end)
            ),
            hint: Some("widen start, or check the labels with label_values".into()),
        }),
        [one] => {
            let mut tags: Vec<(String, String)> = one
                .as_object()
                .map(|o| {
                    o.iter()
                        .filter(|(k, _)| k.as_str() != "__name__")
                        .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_owned()))
                        .collect()
                })
                .unwrap_or_default();
            if let Some(ns) = ctx.scope() {
                tags.push((chronix::chronix_core::NAMESPACE_TAG.to_owned(), ns));
            }
            Ok(OneSeries {
                name,
                measurement: target.measurement,
                field: target.field,
                tags,
            })
        }
        many => {
            let shown: Vec<String> = many
                .iter()
                .take(5)
                .map(|s| {
                    s.as_object()
                        .map(|o| {
                            o.iter()
                                .filter(|(k, _)| k.as_str() != "__name__")
                                .map(|(k, v)| format!("{k}={}", v.as_str().unwrap_or_default()))
                                .collect::<Vec<_>>()
                                .join(", ")
                        })
                        .map(|l| format!("{{{l}}}"))
                        .unwrap_or_default()
                })
                .collect();
            Err(ToolError {
                message: format!(
                    "{} matches {} series; these need one: {}{}",
                    selector(&name, labels),
                    many.len(),
                    shown.join(" "),
                    if many.len() > shown.len() { " …" } else { "" }
                ),
                hint: Some("add `labels` that pick one of them".into()),
            })
        }
    }
}

fn tag_refs(tags: &[(String, String)]) -> Vec<(&str, &str)> {
    tags.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()
}

/// Most buckets a `step` may cut a forecast window into: enough for a week at
/// two minutes, few enough that model selection answers interactively.
const MAX_FORECAST_BUCKETS: i64 = 5_000;

pub(crate) async fn forecast(ctx: &Ctx, args: ForecastArgs) -> Result<Output, ToolError> {
    if args.horizon == 0 || args.horizon > 10_000 {
        return Err(ToolError::new("horizon must be between 1 and 10000 points"));
    }
    let start = ctx.time("start", args.start.as_ref(), "now-7d")?;
    let end = ctx.time("end", args.end.as_ref(), "now")?;
    let labels = args.labels.clone().unwrap_or_default();
    let series = one_series(
        ctx,
        args.metric.as_deref(),
        args.measurement.as_deref(),
        args.field.as_deref(),
        &labels,
        (start, end),
    )
    .await?;
    let step_ns = match &args.step {
        Some(s) => {
            let step = crate::http::parse_duration_ns("step", s)?;
            let buckets = (end - start) / step + 1;
            if buckets > MAX_FORECAST_BUCKETS {
                return Err(ToolError {
                    message: format!(
                        "step {s} splits the window into {buckets} buckets; at most {MAX_FORECAST_BUCKETS}"
                    ),
                    hint: Some("choose a larger step, or a shorter window".into()),
                });
            }
            Some(step)
        }
        None => None,
    };
    let model = args.model.clone().unwrap_or_else(|| "auto".into());
    let db = ctx.state.db.clone();
    let horizon = args.horizon;
    let min = args.min;
    if min.is_some_and(|m| !m.is_finite()) {
        return Err(ToolError::new("min must be a finite number"));
    }
    let slot = ctx
        .state
        .query_slots
        .acquire(std::time::Duration::from_secs(
            ctx.state.config.server.sql_query_timeout_secs,
        ))
        .await?;
    let (m, f, tags) = (
        series.measurement.clone(),
        series.field.clone(),
        series.tags.clone(),
    );
    let (result, selection) = tokio::task::spawn_blocking(move || {
        let _slot = slot;
        let tags = tag_refs(&tags);
        if model == "auto" {
            let options = chronix::chronix_analytics::forecast::AutoForecastOptions {
                lower_bound: min,
                ..Default::default()
            };
            match step_ns {
                Some(step) => db.auto_forecast_at_step(
                    &m,
                    &f,
                    &tags,
                    start,
                    end,
                    step,
                    horizon,
                    Some(options),
                ),
                None => db.auto_forecast(&m, &f, &tags, start, end, horizon, Some(options)),
            }
            .map(|a| {
                let selection = json!({
                    "model": a.selection.label,
                    "metric": format!("{:?}", a.selection.metric).to_lowercase(),
                    "score": a.selection.score,
                });
                (a.result, Some(selection))
            })
        } else {
            let cfg = chronix::analytics::ForecastConfig {
                model: Some(model.clone()),
                step_ns,
                lower_bound: min,
                ..Default::default()
            };
            db.forecast(&m, &f, &tags, start, end, horizon, Some(cfg))
                .map(|r| (r, None))
        }
    })
    .await
    .map_err(|e| ToolError::new(e.to_string()))?
    .map_err(ServerError::from)?;

    let model_name = selection
        .as_ref()
        .and_then(|s| s["model"].as_str().map(str::to_owned))
        .unwrap_or_else(|| args.model.clone().unwrap_or_default());
    let step_s = match result.timestamps.as_slice() {
        [a, b, ..] => (b - a) as f64 / 1e9,
        _ => 0.0,
    };
    let points: Vec<Value> = result
        .timestamps
        .iter()
        .zip(&result.values)
        .zip(result.confidence_lower.iter().zip(&result.confidence_upper))
        .map(|((t, v), (lo, hi))| json!({ "t": rfc3339(*t), "value": v, "lower": lo, "upper": hi }))
        .collect();
    let summary = match (points.first(), points.last()) {
        (Some(first), Some(last)) => format!(
            "{}: {} points ahead to {} by {model_name}; next {:.3} ({:.3} to {:.3}), last {:.3}",
            selector(&series.name, &labels),
            points.len(),
            last["t"].as_str().unwrap_or_default(),
            first["value"].as_f64().unwrap_or(f64::NAN),
            first["lower"].as_f64().unwrap_or(f64::NAN),
            first["upper"].as_f64().unwrap_or(f64::NAN),
            last["value"].as_f64().unwrap_or(f64::NAN),
        ),
        _ => format!("{}: no forecast", series.name),
    };
    let (points, truncated) = bounded(points, usize::MAX, ctx.max_bytes);
    Ok(Output {
        structured: json!({
            "metric": series.name,
            "model": model_name,
            "selection": selection,
            "step_s": step_s,
            "interval_level": result.confidence_level,
            "clamped_below": min,
            "points": points,
            "truncated": truncated,
        }),
        summary,
    })
}

pub(crate) async fn detect_anomalies(ctx: &Ctx, args: DetectArgs) -> Result<Output, ToolError> {
    let start = ctx.time("start", args.start.as_ref(), "now-7d")?;
    let end = ctx.time("end", args.end.as_ref(), "now")?;
    let labels = args.labels.clone().unwrap_or_default();
    let series = one_series(
        ctx,
        args.metric.as_deref(),
        args.measurement.as_deref(),
        args.field.as_deref(),
        &labels,
        (start, end),
    )
    .await?;
    let method = args.method.clone().unwrap_or_else(|| "zscore".into());
    let cfg = chronix::analytics::AnomalyConfig {
        method: Some(method.clone()),
        threshold: args.threshold.unwrap_or(3.0),
        ..Default::default()
    };
    let db = ctx.state.db.clone();
    let slot = ctx
        .state
        .query_slots
        .acquire(std::time::Duration::from_secs(
            ctx.state.config.server.sql_query_timeout_secs,
        ))
        .await?;
    let (m, f, tags) = (
        series.measurement.clone(),
        series.field.clone(),
        series.tags.clone(),
    );
    let scores = tokio::task::spawn_blocking(move || {
        let _slot = slot;
        db.detect_anomalies(&m, &f, &tag_refs(&tags), start, end, Some(cfg))
    })
    .await
    .map_err(|e| ToolError::new(e.to_string()))?
    .map_err(ServerError::from)?;

    let mut anomalies: Vec<_> = scores.into_iter().filter(|s| s.is_anomaly).collect();
    anomalies.sort_by(|a, b| b.score.abs().total_cmp(&a.score.abs()));
    let total = anomalies.len();
    let items: Vec<Value> = anomalies
        .iter()
        .map(|a| json!({ "t": rfc3339(a.timestamp), "value": a.value, "score": a.score }))
        .collect();
    let (items, truncated) = bounded(items, 100, ctx.max_bytes);
    let window = format!("{} to {}", rfc3339(start), rfc3339(end));
    let summary = match anomalies.first() {
        None => format!(
            "no anomalies in {} ({method}) from {window}",
            selector(&series.name, &labels)
        ),
        Some(a) => format!(
            "{total} anomal{} in {} ({method}) from {window}; strongest at {}, value {:.3}, score {:.2}",
            if total == 1 { "y" } else { "ies" },
            selector(&series.name, &labels),
            rfc3339(a.timestamp),
            a.value,
            a.score
        ),
    };
    Ok(Output {
        structured: json!({
            "metric": series.name,
            "method": method,
            "anomalies": items,
            "total": total,
            "truncated": truncated,
        }),
        summary,
    })
}
