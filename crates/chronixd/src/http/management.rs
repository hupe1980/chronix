//! Measurement, rollup, export, health, and administrative endpoint handlers.

use std::collections::BTreeMap;

use axum::extract::{Json, Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::error::ServerError;

use super::types::{
    measurement_schema_to_info, AppState, MeasurementInfo, PaginatedResponse, PaginationParams,
    TimeRangeRequest, DEFAULT_LIST_LIMIT,
};

// ── Measurement handlers ───────────────────────────────────────────────

/// `GET /api/v1/measurements` — list all measurements with schema info.
///
/// Supports optional `offset` and `limit` query parameters for pagination.
pub async fn list_measurements_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    Query(pagination): Query<PaginationParams>,
) -> Result<Json<PaginatedResponse<MeasurementInfo>>, ServerError> {
    let db = state.db.clone();
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let limit = state.config.prom_series_limit;

    let all_infos = tokio::task::spawn_blocking(move || {
        let registry = db.schema_registry();
        let names = if scope.is_some() {
            crate::namespace::measurements_in(&db, scope.as_deref(), i64::MIN, i64::MAX, limit)
        } else {
            registry.measurement_names()
        };

        let mut infos = Vec::with_capacity(names.len());
        for name in names {
            if let Some(schema) = registry.lookup(&name) {
                infos.push(measurement_schema_to_info(&name, &schema));
            }
        }

        infos
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))?;

    let total = all_infos.len();
    let offset = pagination.offset.unwrap_or(0);
    let limit = pagination.limit.unwrap_or(DEFAULT_LIST_LIMIT);
    let items: Vec<MeasurementInfo> = all_infos.into_iter().skip(offset).take(limit).collect();

    Ok(Json(PaginatedResponse {
        items,
        total,
        offset,
        limit,
    }))
}

/// `GET /api/v1/measurements/:name/schema` — measurement schema.
pub async fn get_schema_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<MeasurementInfo>, ServerError> {
    let schema = state
        .db
        .schema(&name)
        .ok_or_else(|| ServerError::NotFound(name.clone()))?;

    Ok(Json(measurement_schema_to_info(&name, &schema)))
}

/// `DELETE /api/v1/measurements/:name` — drop a measurement.
pub async fn drop_measurement_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let db = state.db.clone();
    let measurement_name = name.clone();
    tokio::task::spawn_blocking(move || db.drop_measurement(&measurement_name))
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?
        .map_err(ServerError::Db)?;

    tracing::info!(
        measurement = %name,
        "audit: measurement dropped"
    );

    Ok(StatusCode::NO_CONTENT)
}

// ── Delete handlers ────────────────────────────────────────────────────

/// JSON body for predicate-based delete.
#[derive(Debug, Deserialize)]
pub struct DeleteBody {
    /// Measurement to delete from.
    pub measurement: String,
    /// Tag filters — only matching series are deleted.
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    /// Optional time range — restricts deletion to this window.
    #[serde(default)]
    pub range: Option<TimeRangeRequest>,
}

/// `POST /api/v1/delete` — predicate-based delete.
pub async fn delete_handler(
    State(state): State<AppState>,
    Json(body): Json<DeleteBody>,
) -> Result<Json<serde_json::Value>, ServerError> {
    let db = state.db.clone();

    let deleted = tokio::task::spawn_blocking(move || {
        let mut builder = db.delete_builder().measurement(&body.measurement);
        for (k, v) in &body.tags {
            builder = builder.tag(k, v);
        }
        if let Some(ref range) = body.range {
            builder = builder.range(range.start, range.end);
        }
        let request = builder
            .build()
            .map_err(|e| chronix::DbError::Internal(format!("delete build error: {e}")))?;
        db.execute_delete(&request)
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))?
    .map_err(ServerError::Db)?;

    // Surface partial deletes — a caller with an erasure obligation
    // must be able to tell a complete delete from one that skipped segments.
    Ok(Json(serde_json::json!({
        "deleted": deleted.series_tombstoned,
        "segments_skipped": deleted.segments_skipped,
        "complete": deleted.is_complete(),
    })))
}

/// JSON body for a bulk delete request.
#[derive(Debug, Deserialize)]
pub struct DeleteBatchBody {
    /// Array of individual delete requests.
    pub deletes: Vec<DeleteBody>,
}

/// Result of an individual delete within a batch.
#[derive(Debug, Serialize)]
pub struct DeleteBatchItemResult {
    /// The measurement targeted by this delete.
    pub measurement: String,
    /// Number of data points deleted, or `null` on error.
    pub deleted: Option<u64>,
    /// Segments that could not be scanned and may still hold matching
    /// data. Non-zero means this delete was only partially applied.
    pub segments_skipped: u64,
    /// Error message, if the individual delete failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// `POST /api/v1/delete_batch` — bulk predicate-based delete.
pub async fn delete_batch_handler(
    State(state): State<AppState>,
    Json(body): Json<DeleteBatchBody>,
) -> Result<Json<Vec<DeleteBatchItemResult>>, ServerError> {
    if body.deletes.is_empty() {
        return Err(ServerError::BadRequest(
            "deletes array must not be empty".into(),
        ));
    }

    let db = state.db.clone();

    let results = tokio::task::spawn_blocking(move || {
        let mut results = Vec::with_capacity(body.deletes.len());
        for item in &body.deletes {
            let res = (|| -> std::result::Result<chronix::DeleteOutcome, chronix::DbError> {
                let mut builder = db.delete_builder().measurement(&item.measurement);
                for (k, v) in &item.tags {
                    builder = builder.tag(k, v);
                }
                if let Some(ref range) = item.range {
                    builder = builder.range(range.start, range.end);
                }
                let request = builder
                    .build()
                    .map_err(|e| chronix::DbError::Internal(format!("delete build error: {e}")))?;
                db.execute_delete(&request)
            })();

            match res {
                Ok(outcome) => results.push(DeleteBatchItemResult {
                    measurement: item.measurement.clone(),
                    deleted: Some(outcome.series_tombstoned),
                    segments_skipped: outcome.segments_skipped,
                    error: None,
                }),
                Err(e) => results.push(DeleteBatchItemResult {
                    measurement: item.measurement.clone(),
                    deleted: None,
                    segments_skipped: 0,
                    error: Some(e.to_string()),
                }),
            }
        }
        results
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(Json(results))
}

// ── Rollup handlers ────────────────────────────────────────────────────

/// Rollup info for the list endpoint.
#[derive(Debug, Serialize)]
pub struct RollupInfo {
    /// Rollup configuration name.
    pub name: String,
    /// Source measurement being aggregated.
    pub source_measurement: String,
    /// Target measurement storing the roll-up data.
    pub target_measurement: String,
    /// Aggregation window size in seconds.
    pub window_seconds: u64,
    /// Aggregation functions applied.
    pub aggregations: Vec<String>,
}

/// `GET /api/v1/rollups` — list rollup configurations.
///
/// Supports optional `offset` and `limit` query parameters for pagination.
pub async fn list_rollups_handler(
    State(state): State<AppState>,
    Query(pagination): Query<PaginationParams>,
) -> Result<Json<PaginatedResponse<RollupInfo>>, ServerError> {
    let db = state.db.clone();

    let rollups = tokio::task::spawn_blocking(move || db.list_rollups())
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?
        .map_err(ServerError::Db)?;

    let all_infos: Vec<RollupInfo> = rollups
        .into_iter()
        .map(|r| RollupInfo {
            name: r.name.clone(),
            source_measurement: r.source_measurement.clone(),
            target_measurement: r.target_measurement.clone(),
            window_seconds: (r.interval_ns / 1_000_000_000) as u64,
            aggregations: r.aggregations.iter().map(|a| format!("{a:?}")).collect(),
        })
        .collect();

    let total = all_infos.len();
    let offset = pagination.offset.unwrap_or(0);
    let limit = pagination.limit.unwrap_or(DEFAULT_LIST_LIMIT);
    let items: Vec<RollupInfo> = all_infos.into_iter().skip(offset).take(limit).collect();

    Ok(Json(PaginatedResponse {
        items,
        total,
        offset,
        limit,
    }))
}

/// JSON body for creating a rollup configuration.
#[derive(Debug, Deserialize)]
pub struct CreateRollupRequest {
    /// Unique rollup name.
    pub name: String,
    /// Source measurement to aggregate from.
    pub source_measurement: String,
    /// Target measurement where rollup points are stored.
    pub target_measurement: String,
    /// Aggregation interval in seconds.
    pub interval_seconds: u64,
    /// Aggregation functions to apply (avg, min, max, sum, count, last).
    pub aggregations: Vec<String>,
    /// Tags to group by.
    #[serde(default)]
    pub group_by_tags: Vec<String>,
    /// Optional retention in seconds for rollup data.
    #[serde(default)]
    pub retention_seconds: Option<u64>,
}

/// `POST /api/v1/rollups` — create a rollup configuration.
pub async fn create_rollup_handler(
    State(state): State<AppState>,
    Json(body): Json<CreateRollupRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let db = state.db.clone();
    let rollup_name = body.name.clone();

    // Parse aggregation function names.
    let agg_fns: Vec<chronix::RollupAggFn> = body
        .aggregations
        .iter()
        .map(|s| match s.to_lowercase().as_str() {
            "avg" => Ok(chronix::RollupAggFn::Avg),
            "min" => Ok(chronix::RollupAggFn::Min),
            "max" => Ok(chronix::RollupAggFn::Max),
            "sum" => Ok(chronix::RollupAggFn::Sum),
            "count" => Ok(chronix::RollupAggFn::Count),
            "last" => Ok(chronix::RollupAggFn::Last),
            other => Err(ServerError::BadRequest(format!(
                "unknown aggregation function: {other}"
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;

    let interval_ns = body
        .interval_seconds
        .checked_mul(1_000_000_000)
        .ok_or_else(|| ServerError::BadRequest("interval_seconds overflow".into()))?
        as i64;

    tokio::task::spawn_blocking(move || {
        let mut builder = chronix::RollupBuilder::new()
            .name(&body.name)
            .source(&body.source_measurement)
            .target(&body.target_measurement)
            .interval_ns(interval_ns);

        for agg in &agg_fns {
            builder = builder.aggregation(*agg);
        }
        for tag in &body.group_by_tags {
            builder = builder.group_by(tag);
        }
        if let Some(retention_secs) = body.retention_seconds {
            builder = builder.retention_ns(retention_secs as i64 * 1_000_000_000);
        }

        let config = builder
            .build()
            .map_err(|e| ServerError::BadRequest(format!("invalid rollup config: {e}")))?;
        db.create_rollup(config).map_err(ServerError::Db)?;
        Ok::<_, ServerError>(())
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))??;

    tracing::info!(rollup = %rollup_name, "rollup created");

    Ok(StatusCode::CREATED)
}

/// `DELETE /api/v1/rollups/:name` — delete a rollup configuration.
pub async fn delete_rollup_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let db = state.db.clone();
    let rollup_name = name.clone();

    let removed = tokio::task::spawn_blocking(move || db.delete_rollup(&rollup_name))
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?
        .map_err(ServerError::Db)?;

    if !removed {
        return Err(ServerError::NotFound(format!("rollup '{name}' not found")));
    }

    tracing::info!(rollup = %name, "rollup deleted");

    Ok(StatusCode::NO_CONTENT)
}

// ── Parquet export ─────────────────────────────────────────────────────

/// `POST /api/v1/export/parquet` — trigger Parquet export.
#[derive(Debug, Deserialize)]
pub struct ExportRequest {
    /// Measurement to export.
    pub measurement: String,
    /// Optional time range filter.
    #[serde(default)]
    pub range: Option<TimeRangeRequest>,
    /// Output file path (only the filename component is used).
    pub output_path: String,
}

/// `POST /api/v1/export/parquet` — export a measurement/time-window to a
/// Parquet file on the server's filesystem.
pub async fn export_parquet_handler(
    State(state): State<AppState>,
    Json(body): Json<ExportRequest>,
) -> Result<Json<serde_json::Value>, ServerError> {
    let db = state.db.clone();

    // Restrict export path to the database data directory to prevent path traversal.
    let data_dir = state.db.data_dir().to_path_buf();
    let export_dir = data_dir.join("exports");

    let export = tokio::task::spawn_blocking(move || {
        // Ensure the export directory exists.
        std::fs::create_dir_all(&export_dir)
            .map_err(|e| chronix::DbError::Internal(format!("failed to create export dir: {e}")))?;

        // Only allow the filename component — reject any path separators.
        let file_name = std::path::Path::new(&body.output_path)
            .file_name()
            .ok_or_else(|| {
                chronix::DbError::Internal("invalid export path: no filename".to_string())
            })?;
        let safe_path = export_dir.join(file_name);
        // Verify the canonical path still lives under the export directory.
        let canonical = safe_path
            .canonicalize()
            .unwrap_or_else(|_| safe_path.clone());
        if !canonical.starts_with(&export_dir) {
            return Err(chronix::DbError::Internal(
                "path traversal not allowed".to_string(),
            ));
        }

        let mut builder = db.query().measurement(&body.measurement);
        if let Some(ref range) = body.range {
            builder = builder.range(range.start, range.end);
        }
        let plan = builder
            .build()
            .map_err(|e| chronix::DbError::Internal(format!("query build error: {e}")))?;
        let config = chronix::ParquetExportConfig::default();
        db.export_parquet(&plan, &safe_path, &config)
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))?
    .map_err(ServerError::Db)?;

    // Surface the size and whether a size budget cut the export short — a
    // caller uploading the file needs to know it is incomplete.
    Ok(Json(serde_json::json!({
        "rows_written": export.rows_written,
        "bytes_written": export.bytes_written,
        "truncated": export.truncated,
    })))
}

// ── Health / Readiness ────────────────────────────────────

/// `GET /health` — liveness probe.
///
/// Returns 200 with basic liveness info.  This endpoint is intentionally
/// un-versioned so load-balancers and Kubernetes liveness probes can hit
/// it without knowing the API version.
///
/// # Always returns 200 during shutdown
///
/// This endpoint returns `200 OK` even while the server is draining.
/// This is **by design**: Kubernetes liveness probes should use `/health`
/// (always 200 while the process is alive), while **readiness** probes
/// should use `/ready` (returns 503 when the database is closed or the
/// server is shutting down). If you need traffic to stop being routed
/// during graceful shutdown, configure the readiness probe, not the
/// liveness probe.
///
/// # Kubernetes startup probe
///
/// For slow-starting instances (large WAL replay, initial compaction),
/// configure a Kubernetes `startupProbe` pointing at `/ready` with
/// generous `failureThreshold` and `periodSeconds`:
///
/// ```yaml
/// startupProbe:
///   httpGet:
///     path: /ready
///     port: 4242
///   failureThreshold: 30
///   periodSeconds: 10
/// ```
///
/// This prevents the liveness probe from killing a still-initialising pod.
pub async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    let uptime_secs = state.start_time.elapsed().as_secs();
    Json(serde_json::json!({
        "status": "ok",
        "uptime_secs": uptime_secs,
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

/// `GET /ready` — readiness probe (distinct from `/health`).
///
/// Returns **200** with `{"ready": true}` when the database is open and
/// accepting writes.  Returns **503** with `{"ready": false}` when the
/// server is still initialising or the database is closed.
///
/// Kubernetes `readinessProbe` should point here so traffic is only
/// routed to instances that can actually serve requests.
pub async fn ready_handler(State(state): State<AppState>) -> impl IntoResponse {
    // If the db handle exists and isn't closed, we're ready.
    // Try a lightweight operation to verify.
    let db = state.db.clone();
    match tokio::task::spawn_blocking(move || db.schema_registry().measurement_count()).await {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({ "ready": true }))),
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "ready": false })),
        ),
    }
}

// ── Connectors ─────────────────────────────────────────────────────────

/// `GET /api/v1/connectors` — list active connectors with status and metrics.
///
/// Supports optional `offset` and `limit` query parameters for pagination.
pub async fn list_connectors_handler(
    State(state): State<AppState>,
    Query(pagination): Query<PaginationParams>,
) -> Json<PaginatedResponse<crate::connector::ConnectorInfo>> {
    let all_connectors = match &state.connector_manager {
        Some(manager) => manager.list_connectors().await,
        None => Vec::new(),
    };

    let total = all_connectors.len();
    let offset = pagination.offset.unwrap_or(0);
    let limit = pagination.limit.unwrap_or(DEFAULT_LIST_LIMIT);
    let items: Vec<crate::connector::ConnectorInfo> = all_connectors
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect();

    Json(PaginatedResponse {
        items,
        total,
        offset,
        limit,
    })
}

// ── Admin: runtime log-level adjustment ─────────────────────────

/// JSON body for `PUT /api/v1/admin/log-level`.
#[derive(Debug, Deserialize)]
pub struct UpdateLogLevelRequest {
    /// New `EnvFilter` directive, e.g. `"debug"` or `"chronix=trace,tower=info"`.
    pub filter: String,
}

/// Update the active log filter directive at runtime.
///
/// Accepts `{"filter": "<directive>"}`.  Returns 200 on success, 400 on
/// invalid filter syntax.
pub async fn update_log_level_handler(
    Json(body): Json<UpdateLogLevelRequest>,
) -> impl IntoResponse {
    match crate::otel::update_log_filter(&body.filter) {
        Ok(()) => {
            debug!(new_filter = %body.filter, "log filter updated at runtime");
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "status": "ok",
                    "filter": body.filter,
                })),
            )
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": e.to_string(),
            })),
        ),
    }
}

// ── Dashboard export ───────────────────────────────────────────────────

/// `GET /api/v1/dashboards/export` — export bundled Grafana dashboard JSONs.
///
/// Looks for dashboards in the following locations (in order):
/// 1. `<data_dir>/dashboards/`
/// 2. `./dashboards/` (relative to the working directory)
/// 3. Compile-time fallback via `CARGO_MANIFEST_DIR` (dev builds only)
///
/// # `CARGO_MANIFEST_DIR` path leak
///
/// The third fallback candidate embeds the **build-time** source tree
/// path via `env!("CARGO_MANIFEST_DIR")`. In release/production builds
/// this directory typically does not exist, so the fallback is harmless.
/// However, the path is compiled into the binary and could be observed
/// via `strings(1)`. If this is a concern, use `include_dir!` to embed
/// dashboards at compile time or strip the binary with `--strip=symbols`.
///
/// All filesystem I/O is offloaded to a blocking thread pool to avoid
/// stalling the async runtime.
pub async fn export_dashboards_handler(
    State(state): State<AppState>,
) -> Result<Json<Vec<serde_json::Value>>, ServerError> {
    let data_dir = state.config.database.data_dir.clone();

    let dashboards =
        tokio::task::spawn_blocking(move || -> Result<Vec<serde_json::Value>, ServerError> {
            #[allow(unused_mut)]
            let mut candidates = vec![
                data_dir.join("dashboards"),
                std::path::PathBuf::from("dashboards"),
            ];
            // Compile-time fallback: only included in debug builds to avoid
            // leaking CARGO_MANIFEST_DIR (build-time source path) into
            // release binaries.
            #[cfg(debug_assertions)]
            candidates.push(
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .and_then(|p| p.parent())
                    .map(|p| p.join("dashboards"))
                    .unwrap_or_default(),
            );

            let dashboards_dir = candidates.iter().find(|p| p.exists()).ok_or_else(|| {
                ServerError::Internal(format!(
                    "dashboards directory not found (searched: {})",
                    candidates
                        .iter()
                        .map(|p| p.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })?;

            let mut dashboards = Vec::new();

            if dashboards_dir.exists() {
                let entries = std::fs::read_dir(dashboards_dir).map_err(|e| {
                    ServerError::Internal(format!("cannot read dashboards dir: {e}"))
                })?;

                for entry in entries {
                    let entry = entry
                        .map_err(|e| ServerError::Internal(format!("dir entry error: {e}")))?;
                    let path = entry.path();
                    if path.extension().is_some_and(|ext| ext == "json") {
                        let content = std::fs::read_to_string(&path).map_err(|e| {
                            ServerError::Internal(format!("cannot read {}: {e}", path.display()))
                        })?;
                        let value: serde_json::Value =
                            serde_json::from_str(&content).map_err(|e| {
                                ServerError::Internal(format!(
                                    "invalid JSON in {}: {e}",
                                    path.display()
                                ))
                            })?;
                        dashboards.push(value);
                    }
                }
            }

            Ok(dashboards)
        })
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))??;

    Ok(Json(dashboards))
}
