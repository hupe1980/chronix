//! Query, SQL, and explain endpoint handlers.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Json, State};
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use chronix::prelude::*;

use crate::error::ServerError;
use crate::namespace::NamespaceContext;

use super::types::{
    arrow_value_to_json, resolve_namespace, AppState, TimeRangeRequest, NAMESPACE_TAG,
};

// ── Request / response types ───────────────────────────────────────────

/// JSON body for a query request.
///
/// `deny_unknown_fields`: a misspelled or stale key used to be dropped in
/// silence, so a body carrying `start`/`end` at the top level — which is what
/// the documentation showed — ran unfiltered and answered the whole
/// measurement as though it had understood the range.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRequest {
    /// Measurement to query.
    pub measurement: String,
    /// Optional time range filter.
    #[serde(default)]
    pub range: Option<TimeRangeRequest>,
    /// Tag filters (exact match).
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    /// Specific fields to return (empty = all).
    #[serde(default)]
    pub fields: Vec<String>,
    /// Maximum number of rows to return.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Number of rows to skip.
    #[serde(default)]
    pub offset: Option<usize>,
}

/// A row in the JSON query response.
#[derive(Debug, Serialize)]
pub struct QueryRow {
    /// Row timestamp (nanoseconds since epoch).
    pub timestamp: i64,
    /// Tag values for this row.
    pub tags: BTreeMap<String, String>,
    /// Field values for this row.
    pub fields: BTreeMap<String, serde_json::Value>,
}

/// JSON body for SQL query.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlRequest {
    /// The SQL query string.
    pub query: String,
}

/// A column in the SQL result schema.
#[derive(Debug, Serialize)]
pub struct SqlColumn {
    /// Column name.
    pub name: String,
    /// Arrow data type as string.
    pub data_type: String,
}

/// JSON response for SQL queries.
#[derive(Debug, Serialize)]
pub struct SqlResponse {
    /// Column metadata.
    pub columns: Vec<SqlColumn>,
    /// Row-oriented results.
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Number of rows returned.
    pub row_count: usize,
    /// Whether `sql_max_rows` cut the result short.
    ///
    /// Always present, and `false` far more often than `true` — which is the
    /// point. It was absent, so a query whose answer was 6 000 rows returned
    /// the first 5 with `row_count: 5` and nothing else: an aggregate over a
    /// truncated scan is not a partial answer, it is a **wrong** one, and the
    /// client had no way to tell the two apart. The server logged a `warn!`,
    /// which is not where the person reading the number is looking.
    pub truncated: bool,
}

// ── Handlers ───────────────────────────────────────────────────────────

/// `POST /api/v1/chronix/query` — query time-series data.
pub async fn query_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<NamespaceContext>>,
    Json(body): Json<QueryRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let db = state.db.clone();
    let measurement = body.measurement.clone();
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);

    // Check measurement exists
    if db.schema(&measurement).is_none() {
        return Err(ServerError::NotFound(measurement));
    }

    // The same ceiling `/api/v1/chronix/sql` enforces. This endpoint had
    // none: `{"measurement":"cpu"}` with no `limit` materialised every row of
    // the measurement into a JSON array, so the one query surface anybody
    // reaches for first was also the only unbounded one.
    //
    // It **refuses** rather than truncating, because the documented response
    // is a bare array with no field a `truncated` flag could go in — and
    // because the request already carries the answer: a caller who wants the
    // first N says `limit`.
    let ceiling = state.config.server.sql_max_rows;
    // A `limit` above the ceiling is refused *before* the scan: the caller
    // has asked for something this server will not return, and answering the
    // ceiling's worth instead would be the silent truncation this endpoint
    // exists not to do.
    if ceiling > 0 && body.limit.is_some_and(|n| n > ceiling) {
        return Err(ServerError::BadRequest(format!(
            "limit exceeds the server's sql_max_rows limit of {ceiling}"
        )));
    }

    let result = tokio::task::spawn_blocking(move || {
        let mut builder = db
            .query()
            .measurement(&body.measurement)
            .namespace_scope(scope.as_deref());

        if let Some(ref range) = body.range {
            builder = builder.range(range.start, range.end);
        }

        for (key, value) in &body.tags {
            builder = builder.tag(key, value);
        }

        if !body.fields.is_empty() {
            for f in &body.fields {
                builder = builder.field(f);
            }
        }

        let plan = builder
            .build()
            .map_err(|e| chronix::DbError::Internal(format!("query build error: {e}")))?;

        // Apply offset + limit incrementally: skip `offset` rows without
        // converting them to QueryRow, then convert only `limit` rows.
        let offset = body.offset.unwrap_or(0);
        let limit = body.limit.unwrap_or(usize::MAX);
        // One row past the ceiling is enough to know the answer did not fit.
        let hard_stop = if ceiling == 0 {
            usize::MAX
        } else {
            ceiling.saturating_add(1)
        };
        let limit = limit.min(hard_stop);

        // `execute_iter` reads one time bucket of segments per step, so a
        // small `limit` over a huge range stops after the first bucket
        // instead of reading — and holding — the whole range first.
        let mut rows = Vec::new();
        let mut skipped = 0usize;
        for batch in db.execute_iter(&plan)? {
            let batch = batch?;
            let n = batch.num_rows();

            // Fast-skip entire batches that fall within the offset.
            if skipped + n <= offset {
                skipped += n;
                continue;
            }

            // Determine how many rows from this batch to skip/take.
            let batch_skip = offset.saturating_sub(skipped);
            let remaining = limit - rows.len();
            let batch_take = (n - batch_skip).min(remaining);

            let sliced = batch.slice(batch_skip, batch_take);
            rows.extend(record_batch_to_rows(&sliced));
            skipped += batch_skip;

            if rows.len() >= limit {
                break;
            }
        }

        Ok::<_, chronix::DbError>(rows)
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))?
    .map_err(ServerError::Db)?;

    if ceiling > 0 && result.len() > ceiling {
        metrics::counter!("chronix_sql_results_truncated_total").increment(1);
        return Err(ServerError::BadRequest(format!(
            "result exceeds the server's sql_max_rows limit of {ceiling}; \
             pass a `limit`, narrow `range`, or add tag filters"
        )));
    }

    metrics::counter!("chronix_http_queries_total").increment(1);
    Ok(Json(result))
}

/// Map a DataFusion execution error to the right status.
///
/// A query that fails *while running* is not automatically the server's fault:
/// an aggregate can reject its own arguments only once it has seen them, and
/// `forecast(v, _time, 2000000)` — over the configured
/// `analytics.max_forecast_horizon` — did exactly that. Mapping every
/// execution error to `Internal` made it a `500` whose body is redacted to
/// "an internal error occurred", so the message naming the setting to change
/// never reached the caller.
fn sql_execution_error(e: &datafusion::error::DataFusionError) -> ServerError {
    match e {
        // The caller's query is wrong: planning, resolution, or an argument a
        // function refuses.
        datafusion::error::DataFusionError::Plan(_)
        | datafusion::error::DataFusionError::SchemaError(..)
        | datafusion::error::DataFusionError::SQL(..)
        | datafusion::error::DataFusionError::NotImplemented(_) => {
            ServerError::BadRequest(format!("SQL error: {e}"))
        }
        // The query asked for more than this server will give it.
        datafusion::error::DataFusionError::ResourcesExhausted(_) => {
            ServerError::BadRequest(format!("SQL error: {e}"))
        }
        _ => ServerError::Internal(format!("SQL execution error: {e}")),
    }
}

/// `POST /api/v1/chronix/sql` — execute a read-only SQL query.
pub async fn sql_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<NamespaceContext>>,
    Json(body): Json<SqlRequest>,
) -> Result<Json<SqlResponse>, ServerError> {
    let sql = body.query;

    // The namespace scopes the *tables*, not just the plan cache: a context
    // built for one namespace exposes tables that carry a mandatory filter,
    // so no SQL text can reach another tenant's rows. Scoping only the cache
    // key — which is what this used to do — left every query unscoped.
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let ctx = state.sql_ctx(scope.as_deref());

    // Check SQL plan cache before parsing.
    const SQL_PLAN_CACHE_MAX: usize = 1000;
    /// Cached plans expire after 5 minutes so that schema changes
    /// (add/remove columns) are picked up without a server restart.
    const SQL_PLAN_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(300);
    let cache_key = (scope.clone().unwrap_or_default(), sql.clone());
    let cached_plan = {
        let mut cache = state.sql_plan_cache.lock();
        cache
            .get_mut(&cache_key)
            .and_then(|(plan, inserted_at, last_access)| {
                if inserted_at.elapsed() < SQL_PLAN_CACHE_TTL {
                    *last_access = std::time::Instant::now(); // track LRU access
                    Some(Arc::clone(plan))
                } else {
                    None // expired — will be replaced below
                }
            })
    };

    let df = if let Some(plan) = cached_plan {
        metrics::counter!("chronix_sql_plan_cache_hits_total").increment(1);
        ctx.execute_logical_plan((*plan).clone())
            .await
            .map_err(|e| ServerError::BadRequest(format!("SQL error: {e}")))?
    } else {
        metrics::counter!("chronix_sql_plan_cache_misses_total").increment(1);
        // Plan and verify *before* executing. Using `ctx.sql()` here
        // applied DDL/`SET` side effects to the process-wide shared session
        // before the read-only check below could reject them — and cached the
        // offending plan, so every repeat request re-applied them.
        let plan = chronix::sql::plan_read_only(&ctx, &sql)
            .await
            .map_err(|e| ServerError::BadRequest(format!("SQL error: {e}")))?;
        let df = ctx
            .execute_logical_plan(plan.clone())
            .await
            .map_err(|e| ServerError::BadRequest(format!("SQL error: {e}")))?;
        // Only verified read-only plans reach the cache.
        let plan = Arc::new(plan);
        let now = std::time::Instant::now();
        let mut cache = state.sql_plan_cache.lock();
        // Evict expired entries before checking capacity.
        cache.retain(|_, (_, inserted_at, _)| inserted_at.elapsed() < SQL_PLAN_CACHE_TTL);
        // LRU eviction — remove least-recently-used entry instead of
        // clearing the entire cache.
        if cache.len() >= SQL_PLAN_CACHE_MAX {
            if let Some(lru_key) = cache
                .iter()
                .min_by_key(|(_, (_, _, la))| *la)
                .map(|(k, _)| k.clone())
            {
                cache.remove(&lru_key);
            }
        }
        cache.insert(cache_key, (plan, now, now));
        df
    };

    // Defense in depth: a cache hit bypasses `plan_read_only`, so re-verify.
    // Nothing unverified should be in the cache, but this is cheap and keeps
    // the invariant local rather than relying on the insert site above.
    chronix::sql::verify_read_only(df.logical_plan())
        .map_err(|e| ServerError::BadRequest(format!("SQL error: {e}")))?;

    // Enforce query timeout (default: 30 s).
    // Uses execute_stream() for incremental processing — no excess data
    // is materialised beyond the max_rows limit.
    use futures::StreamExt;
    let timeout_secs = state.config.server.sql_query_timeout_secs;
    let stream_future = df.execute_stream();
    let mut stream = if timeout_secs > 0 {
        tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), stream_future)
            .await
            .map_err(|_| {
                ServerError::Internal(format!("SQL query timed out after {timeout_secs}s"))
            })?
            .map_err(|e| sql_execution_error(&e))?
    } else {
        stream_future.await.map_err(|e| sql_execution_error(&e))?
    };

    let mut columns = Vec::new();
    let mut rows = Vec::new();
    let mut row_count = 0;
    let max_rows = state.config.server.sql_max_rows;

    // Build column metadata from the stream schema.
    {
        let schema = stream.schema();
        for field in schema.fields() {
            columns.push(SqlColumn {
                name: field.name().clone(),
                data_type: format!("{}", field.data_type()),
            });
        }
    }

    // Consume batches incrementally, respecting max_rows with early termination.
    // The deadline covers the entire batch consumption loop, not just
    // stream creation. Dropping the stream on timeout cancels the DataFusion
    // execution underneath.
    let deadline = if timeout_secs > 0 {
        Some(tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs))
    } else {
        None
    };
    let mut truncated = false;
    while let Some(batch_result) = {
        if let Some(dl) = deadline {
            match tokio::time::timeout_at(dl, stream.next()).await {
                Ok(v) => v,
                Err(_) => {
                    return Err(ServerError::Internal(format!(
                        "SQL query timed out after {timeout_secs}s during result streaming"
                    )));
                }
            }
        } else {
            stream.next().await
        }
    } {
        let batch = batch_result.map_err(|e| sql_execution_error(&e))?;
        for row_idx in 0..batch.num_rows() {
            if row_count >= max_rows {
                tracing::warn!(max_rows, "SQL result truncated to max_rows limit");
                truncated = true;
                break;
            }
            let mut row = Vec::with_capacity(batch.num_columns());
            for col_idx in 0..batch.num_columns() {
                row.push(arrow_value_to_json(batch.column(col_idx), row_idx));
            }
            rows.push(row);
            row_count += 1;
        }
        if truncated {
            break;
        }
    }

    metrics::counter!("chronix_sql_queries_total").increment(1);
    if truncated {
        metrics::counter!("chronix_sql_results_truncated_total").increment(1);
    }
    Ok(Json(SqlResponse {
        columns,
        rows,
        row_count,
        truncated,
    }))
}

/// `POST /api/v1/chronix/query/explain` — return the query plan without running it.
pub async fn query_explain_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<NamespaceContext>>,
    Json(body): Json<QueryRequest>,
) -> Result<Json<serde_json::Value>, ServerError> {
    let db = state.db.clone();
    let measurement = body.measurement.clone();
    let namespace = resolve_namespace(ns_ctx.as_ref().map(|e| &e.0)).to_string();

    // Check measurement exists
    if db.schema(&measurement).is_none() {
        return Err(ServerError::NotFound(measurement));
    }

    let plan = tokio::task::spawn_blocking(move || {
        let mut builder = db.query().measurement(&body.measurement);
        // Structural namespace isolation.
        builder = builder.namespace(&namespace);

        if let Some(ref range) = body.range {
            builder = builder.range(range.start, range.end);
        }
        for (key, value) in &body.tags {
            builder = builder.tag(key, value);
        }
        if !body.fields.is_empty() {
            for f in &body.fields {
                builder = builder.field(f);
            }
        }

        builder
            .build()
            .map_err(|e| chronix::DbError::Internal(format!("query build error: {e}")))
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))?
    .map_err(ServerError::Db)?;

    let plan_json = query_plan_to_json(&plan);

    Ok(Json(serde_json::json!({
        "measurement": measurement,
        "plan": plan_json,
    })))
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Convert an Arrow `RecordBatch` to JSON-serializable rows.
pub(super) fn record_batch_to_rows(batch: &arrow::record_batch::RecordBatch) -> Vec<QueryRow> {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    let schema = batch.schema();
    let num_rows = batch.num_rows();
    let mut rows = Vec::with_capacity(num_rows);

    // Pre-resolve column types once — avoids per-cell downcasting in O(rows × columns).
    enum TypedCol<'a> {
        Timestamp(&'a Int64Array),
        TagString(&'a StringArray, String),
        FieldString(&'a StringArray, String),
        FieldF64(&'a Float64Array, String),
        FieldI64(&'a Int64Array, String),
        FieldU64(&'a UInt64Array, String),
        FieldBool(&'a BooleanArray, String),
        Skip,
    }

    let typed_cols: Vec<TypedCol<'_>> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(col_idx, field)| {
            let col = batch.column(col_idx);
            let name = field.name();

            if name == chronix_core::TIME_COLUMN {
                return col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map_or(TypedCol::Skip, TypedCol::Timestamp);
            }

            if name == NAMESPACE_TAG {
                return TypedCol::Skip;
            }

            match field.data_type() {
                DataType::Utf8 => {
                    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                        // The scan stamps the role, because a tag and a
                        // string field are the same Arrow type. Without it
                        // every tag fell through to `fields` and `tags` came
                        // back empty on every row.
                        let is_tag = field
                            .metadata()
                            .get(chronix::db::ROLE_KEY)
                            .map(std::string::String::as_str)
                            == Some("tag");
                        if is_tag {
                            TypedCol::TagString(arr, name.clone())
                        } else {
                            TypedCol::FieldString(arr, name.clone())
                        }
                    } else {
                        TypedCol::Skip
                    }
                }
                DataType::Float64 => col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .map_or(TypedCol::Skip, |arr| TypedCol::FieldF64(arr, name.clone())),
                DataType::Int64 => col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map_or(TypedCol::Skip, |arr| TypedCol::FieldI64(arr, name.clone())),
                DataType::UInt64 => col
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .map_or(TypedCol::Skip, |arr| TypedCol::FieldU64(arr, name.clone())),
                DataType::Boolean => col
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .map_or(TypedCol::Skip, |arr| TypedCol::FieldBool(arr, name.clone())),
                _ => {
                    tracing::debug!(
                        column = name,
                        dtype = ?field.data_type(),
                        "skipping unsupported column type in HTTP response"
                    );
                    TypedCol::Skip
                }
            }
        })
        .collect();

    for row_idx in 0..num_rows {
        let mut timestamp = 0i64;
        let mut tags = BTreeMap::new();
        let mut fields = BTreeMap::new();

        for tc in &typed_cols {
            match tc {
                TypedCol::Timestamp(arr) => timestamp = arr.value(row_idx),
                TypedCol::TagString(arr, name) => {
                    if !arr.is_null(row_idx) {
                        tags.insert(name.clone(), arr.value(row_idx).to_string());
                    }
                }
                TypedCol::FieldString(arr, name) => {
                    if !arr.is_null(row_idx) {
                        fields.insert(
                            name.clone(),
                            serde_json::Value::String(arr.value(row_idx).to_string()),
                        );
                    }
                }
                TypedCol::FieldF64(arr, name) => {
                    if !arr.is_null(row_idx) {
                        fields.insert(name.clone(), serde_json::json!(arr.value(row_idx)));
                    }
                }
                TypedCol::FieldI64(arr, name) => {
                    if !arr.is_null(row_idx) {
                        fields.insert(name.clone(), serde_json::json!(arr.value(row_idx)));
                    }
                }
                TypedCol::FieldU64(arr, name) => {
                    if !arr.is_null(row_idx) {
                        fields.insert(name.clone(), serde_json::json!(arr.value(row_idx)));
                    }
                }
                TypedCol::FieldBool(arr, name) => {
                    if !arr.is_null(row_idx) {
                        fields.insert(name.clone(), serde_json::json!(arr.value(row_idx)));
                    }
                }
                TypedCol::Skip => {}
            }
        }

        rows.push(QueryRow {
            timestamp,
            tags,
            fields,
        });
    }

    rows
}

/// Convert a [`QueryPlan`] into a JSON description for the explain endpoint.
fn query_plan_to_json(plan: &QueryPlan) -> serde_json::Value {
    match plan {
        QueryPlan::Scan {
            measurement,
            tag_filters,
            projection,
            time_range,
            namespace_id,
            field_predicates: _,
        } => {
            let filters: Vec<serde_json::Value> = tag_filters
                .iter()
                .map(|f| serde_json::json!({ "key": f.key, "value": f.value }))
                .collect();
            serde_json::json!({
                "node": "Scan",
                "measurement": measurement,
                "tag_filters": filters,
                "projection": if projection.is_empty() { serde_json::json!("*") } else { serde_json::json!(projection) },
                "time_range": { "start": time_range.start, "end": time_range.end },
                "namespace_id": namespace_id,
            })
        }
        QueryPlan::Aggregate {
            source,
            functions,
            group_by,
            estimated_cardinality,
        } => {
            let fns: Vec<String> = functions.iter().map(|f| format!("{f:?}")).collect();
            let strategy = chronix_query::aggregate::choose_strategy(*estimated_cardinality);
            serde_json::json!({
                "node": "Aggregate",
                "functions": fns,
                "group_by": group_by,
                "estimated_cardinality": estimated_cardinality,
                "strategy": format!("{strategy:?}"),
                "source": query_plan_to_json(source),
            })
        }
        QueryPlan::Downsample {
            source,
            interval,
            function,
        } => {
            serde_json::json!({
                "node": "Downsample",
                "interval_ms": interval.as_millis() as u64,
                "function": format!("{function:?}"),
                "source": query_plan_to_json(source),
            })
        }
        QueryPlan::Limit {
            source,
            limit,
            offset,
        } => {
            serde_json::json!({
                "node": "Limit",
                "limit": limit,
                "offset": offset,
                "source": query_plan_to_json(source),
            })
        }
        QueryPlan::Window {
            source,
            functions,
            partition_by,
            value_column,
        } => {
            let fns: Vec<String> = functions.iter().map(|f| format!("{f:?}")).collect();
            serde_json::json!({
                "node": "Window",
                "functions": fns,
                "partition_by": partition_by,
                "value_column": value_column,
                "source": query_plan_to_json(source),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn record_batch_to_rows_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));

        let ts = Int64Array::from(vec![1000, 2000]);
        let vals = Float64Array::from(vec![10.5, 20.5]);

        let batch =
            arrow::record_batch::RecordBatch::try_new(schema, vec![Arc::new(ts), Arc::new(vals)])
                .unwrap();

        let rows = record_batch_to_rows(&batch);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].timestamp, 1000);
        assert_eq!(rows[0].fields.get("value"), Some(&serde_json::json!(10.5)));
        assert_eq!(rows[1].timestamp, 2000);
    }

    #[test]
    fn record_batch_to_rows_tags_via_metadata() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("role".to_string(), "tag".to_string());

        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("host", DataType::Utf8, true).with_metadata(metadata),
            Field::new("usage", DataType::Float64, true),
        ]));

        let ts = Int64Array::from(vec![1000]);
        let hosts = StringArray::from(vec!["srv1"]);
        let vals = Float64Array::from(vec![99.9]);

        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(ts), Arc::new(hosts), Arc::new(vals)],
        )
        .unwrap();

        let rows = record_batch_to_rows(&batch);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tags.get("host"), Some(&"srv1".to_string()));
        assert_eq!(rows[0].fields.get("usage"), Some(&serde_json::json!(99.9)));
    }

    #[test]
    fn record_batch_to_rows_all_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("f", DataType::Float64, true),
            Field::new("i", DataType::Int64, true),
            Field::new("u", DataType::UInt64, true),
            Field::new("b", DataType::Boolean, true),
        ]));

        let ts = Int64Array::from(vec![1000]);
        let f = Float64Array::from(vec![1.5]);
        let i = Int64Array::from(vec![42]);
        let u = UInt64Array::from(vec![100u64]);
        let b = BooleanArray::from(vec![true]);

        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(ts),
                Arc::new(f),
                Arc::new(i),
                Arc::new(u),
                Arc::new(b),
            ],
        )
        .unwrap();

        let rows = record_batch_to_rows(&batch);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].fields.get("f"), Some(&serde_json::json!(1.5)));
        assert_eq!(rows[0].fields.get("i"), Some(&serde_json::json!(42)));
        assert_eq!(rows[0].fields.get("u"), Some(&serde_json::json!(100)));
        assert_eq!(rows[0].fields.get("b"), Some(&serde_json::json!(true)));
    }

    #[test]
    fn record_batch_to_rows_null_skipped() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));

        let ts = Int64Array::from(vec![1000]);
        let vals = Float64Array::from(vec![None]);

        let batch =
            arrow::record_batch::RecordBatch::try_new(schema, vec![Arc::new(ts), Arc::new(vals)])
                .unwrap();

        let rows = record_batch_to_rows(&batch);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].fields.is_empty());
    }

    #[test]
    fn record_batch_to_rows_string_field() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let ts = Int64Array::from(vec![1000]);
        let names = StringArray::from(vec!["hello"]);

        let batch =
            arrow::record_batch::RecordBatch::try_new(schema, vec![Arc::new(ts), Arc::new(names)])
                .unwrap();

        let rows = record_batch_to_rows(&batch);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].fields.get("name"),
            Some(&serde_json::Value::String("hello".into()))
        );
    }
}
