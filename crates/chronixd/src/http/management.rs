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
    // No cardinality cap on the *listing*. This used to pass
    // `prom_series_limit` into `measurements_in`, so a tenant with more
    // measurements than the cap got a truncated list whose `total` reported
    // the truncated length — a paginated endpoint that says "total: 10000"
    // and can never reach the rest. Pagination is the bound here, and it is
    // exact.
    //
    // The cost is one `LIMIT 1` probe per measurement under tenancy, which is
    // bounded by the number of *measurements* (tens to hundreds) rather than
    // by series cardinality — and it was already paying up to
    // `prom_series_limit` of them. A per-namespace measurement index would
    // make it a lookup; that is a backlog item, not a hazard.
    let all_infos = tokio::task::spawn_blocking(move || {
        let registry = db.schema_registry();
        let names = if scope.is_some() {
            crate::namespace::measurements_in(&db, scope.as_deref(), i64::MIN, i64::MAX, usize::MAX)
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
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    Path(name): Path<String>,
) -> Result<Json<MeasurementInfo>, ServerError> {
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);

    // The schema registry is process-wide, so answering from it directly
    // told a tenant that another tenant's measurement exists — and its
    // column names. A namespace may only see a measurement it holds data
    // for, which is the same rule `/measurements` and `/metadata` follow.
    if let Some(ref ns) = scope {
        let now_ns = crate::util::now_nanos()?;
        let visible =
            crate::namespace::measurements_in(&state.db, Some(ns), i64::MIN, now_ns, usize::MAX);
        if !visible.iter().any(|m| m == &name) {
            return Err(ServerError::NotFound(name));
        }
    }

    let schema = state
        .db
        .schema(&name)
        .ok_or_else(|| ServerError::NotFound(name.clone()))?;

    Ok(Json(measurement_schema_to_info(&name, &schema)))
}

/// `POST /api/v1/measurements/:name/schema/fields` — declare a field column.
///
/// Schema-on-write covers everything else: a column appears when the first
/// point carrying it is written. A **decimal** column is the exception,
/// because its scale is part of its type and cannot change afterwards, so the
/// request body carries `type: "decimal"` and the `scale` the column is fixed
/// at.
///
/// Returns the measurement's schema as it now stands. Declaring a column
/// that already exists with exactly this type is a no-op and succeeds;
/// declaring one that exists with a different type is a 400, because that
/// is a type change and Chronix does not have those.
pub async fn declare_field_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    Path(name): Path<String>,
    Json(req): Json<crate::http::types::DeclareFieldRequest>,
) -> Result<Json<MeasurementInfo>, ServerError> {
    // Under multi-tenancy a measurement is **shared** — every tenant writing
    // `meter` writes the same measurement, distinguished by the namespace
    // tag — so its schema is shared too, and a declaration is a declaration
    // for all of them. That is the same thing a write does when it
    // introduces a column, so it needs no extra scoping; the extension is
    // taken only so the handler signature matches the other schema routes.
    let _ = &ns_ctx;
    let measurement = name.clone();

    let mut column_type: chronix_core::ColumnType = req
        .column_type
        .parse()
        .map_err(|e: chronix_core::SchemaError| ServerError::BadRequest(e.to_string()))?;
    // `scale` is a shorthand for the scale inside `type`, and only means
    // anything for a decimal: silently ignoring it on an `int64` would let
    // `{"type": "int64", "scale": 4}` look like it had been honoured.
    if let Some(scale) = req.scale {
        match column_type {
            chronix_core::ColumnType::Decimal { .. } => {
                column_type = chronix_core::ColumnType::Decimal { scale };
            }
            other => {
                return Err(ServerError::BadRequest(format!(
                    "'scale' applies only to a decimal column, not to {other}"
                )))
            }
        }
    }

    state
        .db
        .declare_field(&measurement, &req.name, column_type)
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let schema = state
        .db
        .schema(&measurement)
        .ok_or_else(|| ServerError::NotFound(measurement.clone()))?;
    Ok(Json(measurement_schema_to_info(&name, &schema)))
}

/// `DELETE /api/v1/measurements/:name` — drop a measurement.
pub async fn drop_measurement_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let db = state.db.clone();
    let measurement_name = name.clone();

    // Under multi-tenancy a measurement is **shared**: every tenant writing
    // `cpu` writes the same measurement, distinguished by the namespace tag.
    // So "drop cpu" cannot mean `drop_measurement`, which would delete every
    // tenant's data and the schema with it — it means "delete this
    // namespace's series of cpu". Without a scope there is one tenant and
    // the real drop is what was asked for.
    match scope {
        None => {
            tokio::task::spawn_blocking(move || db.drop_measurement(&measurement_name))
                .await
                .map_err(|e| ServerError::Internal(e.to_string()))?
                .map_err(ServerError::Db)?;
        }
        Some(ns) => {
            let request = crate::namespace::scoped_delete_request(
                &db,
                Some(&ns),
                &measurement_name,
                [],
                None,
            )?;
            tokio::task::spawn_blocking(move || db.execute_delete(&request))
                .await
                .map_err(|e| ServerError::Internal(e.to_string()))?
                .map_err(ServerError::Db)?;
        }
    }

    tracing::info!(
        measurement = %name,
        "audit: measurement dropped"
    );

    Ok(StatusCode::NO_CONTENT)
}

/// `POST /api/v1/measurements/{name}/restore` — undo a pending drop.
///
/// Only meaningful when `[database] soft_delete_ttl_secs` is set: a drop then
/// marks the measurement pending and a background pass hard-deletes it once
/// the deadline passes, so until then it can be brought back. Without the
/// setting a drop is immediate and there is nothing to restore, which is what
/// `404` means here.
///
/// This exists because the setting did not: `soft_delete_ttl` and
/// `restore_measurement` were both in the engine, tested, and reachable only
/// from the embedded API — so the server's most destructive call, dropping a
/// measurement, had no undo even though the engine implemented one. A grace
/// window nothing can act on is worse than none, because it reads as a safety
/// net.
///
/// Under multi-tenancy the drop handler deletes *this namespace's series*
/// rather than the measurement, so there is no pending drop to undo and the
/// answer is the same `404`.
///
/// # Errors
///
/// `404` when the measurement is not pending deletion; `500` if the restore
/// itself fails.
pub async fn restore_measurement_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    if scope.is_some() {
        return Err(ServerError::NotFound(format!(
            "measurement '{name}' is not pending deletion: under multi-tenancy a drop \
             removes this namespace's series rather than the measurement, and that is \
             not reversible"
        )));
    }

    let db = state.db.clone();
    let measurement = name.clone();
    let restored = tokio::task::spawn_blocking(move || db.restore_measurement(&measurement))
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?
        .map_err(ServerError::Db)?;

    if !restored {
        return Err(ServerError::NotFound(format!(
            "measurement '{name}' is not pending deletion: either it was never dropped, \
             the grace period has passed, or `soft_delete_ttl_secs` is not configured"
        )));
    }

    tracing::info!(measurement = %name, "audit: pending measurement drop restored");
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
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    auth_ctx: Option<axum::extract::Extension<chronix_security::auth::AuthContext>>,
    Json(body): Json<DeleteBody>,
) -> Result<Json<serde_json::Value>, ServerError> {
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let db = state.db.clone();

    let request = crate::namespace::scoped_delete_request(
        &db,
        scope.as_deref(),
        &body.measurement,
        body.tags,
        body.range.map(|r| (r.start, r.end)),
    )?;
    let deleted = tokio::task::spawn_blocking(move || db.execute_delete(&request))
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?
        .map_err(ServerError::Db)?;

    // A delete is the one operation nothing can undo, so it is recorded
    // whether or not anyone is watching the logs.
    let principal = auth_ctx
        .as_ref()
        .map_or("anonymous", |c| c.0.principal.as_str());
    crate::audit::record(
        &state,
        principal,
        chronix_security::audit::AuditAction::Delete,
        body.measurement.clone(),
        chronix_security::audit::AuditDecision::Allow,
        &[
            ("namespace", scope.clone().unwrap_or_default()),
            ("series_tombstoned", deleted.series_tombstoned.to_string()),
            ("complete", deleted.is_complete().to_string()),
        ],
    );

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
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    Json(body): Json<DeleteBatchBody>,
) -> Result<Json<Vec<DeleteBatchItemResult>>, ServerError> {
    if body.deletes.is_empty() {
        return Err(ServerError::BadRequest(
            "deletes array must not be empty".into(),
        ));
    }

    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let db = state.db.clone();

    // Every request is built through the one scoping helper, so a batch
    // cannot be the surface that forgets.
    let requests: Vec<chronix::DeleteRequest> = body
        .deletes
        .iter()
        .map(|item| {
            crate::namespace::scoped_delete_request(
                &db,
                scope.as_deref(),
                &item.measurement,
                item.tags.clone(),
                item.range.as_ref().map(|r| (r.start, r.end)),
            )
        })
        .collect::<Result<_, _>>()?;

    let results = tokio::task::spawn_blocking(move || {
        let mut results = Vec::with_capacity(body.deletes.len());
        for (item, request) in body.deletes.iter().zip(&requests) {
            let res = db.execute_delete(request);

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
    /// The bucket width, spelled the way it is written: `"15m"`, `"1d"`,
    /// `"1mo"`.
    ///
    /// It used to be `window_seconds`, which cannot say "a month" and reads
    /// "86400" for a tier whose buckets are 23 or 25 hours long. The string
    /// is the one `every` accepts, so what this endpoint reports can be typed
    /// straight back into a create request.
    pub every: String,
    /// The IANA zone the buckets are read against, or `null` for UTC.
    pub timezone: Option<String>,
    /// Aggregation functions applied.
    pub aggregations: Vec<String>,
    /// Exclusive end of the newest bucket materialised so far, in
    /// nanoseconds, or `null` if nothing has been.
    pub materialised_until: Option<i64>,
    /// Bucket ranges below the watermark waiting to be recomputed because a
    /// backfill or a delete changed their input, as `[from, to)` pairs. A
    /// non-empty list means this tier does not yet agree with the raw data,
    /// and retention will not drop that raw data until it does.
    pub pending_repairs: Vec<(i64, i64)>,
}

/// `GET /api/v1/rollups` — list rollup configurations.
///
/// Supports optional `offset` and `limit` query parameters for pagination.
pub async fn list_rollups_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    Query(pagination): Query<PaginationParams>,
) -> Result<Json<PaginatedResponse<RollupInfo>>, ServerError> {
    // Rollup names are namespace-qualified when multi-tenancy is on, so a
    // tenant sees its own and the prefix is stripped from what it sees.
    let prefix =
        crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(|ns| format!("{ns}/"));
    let db = state.db.clone();

    let all_infos: Vec<RollupInfo> = tokio::task::spawn_blocking(move || {
        let rollups = db.list_rollups()?;
        let mut infos = Vec::with_capacity(rollups.len());
        for r in rollups {
            let visible_name = match &prefix {
                None => r.name.clone(),
                Some(p) => match r.name.strip_prefix(p.as_str()) {
                    Some(rest) => rest.to_string(),
                    None => continue, // another tenant's rollup
                },
            };
            let state = db.rollup_state(&r.name)?;
            infos.push(RollupInfo {
                name: visible_name,
                source_measurement: r.source_measurement.clone(),
                target_measurement: r.target_measurement.clone(),
                every: r.bucket.width().to_string(),
                timezone: r.bucket.timezone().map(str::to_string),
                aggregations: r.aggregations.iter().map(|a| format!("{a:?}")).collect(),
                materialised_until: state.materialised_until,
                pending_repairs: state.pending_invalidations().to_vec(),
            });
        }
        Ok::<_, chronix::DbError>(infos)
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))?
    .map_err(ServerError::Db)?;

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

/// Query parameters for a rollup refresh.
#[derive(Debug, serde::Deserialize)]
pub struct RefreshParams {
    /// Inclusive start of the range to recompute, nanoseconds.
    pub start: i64,
    /// Inclusive end of the range to recompute, nanoseconds.
    pub end: i64,
}

/// What a refresh did.
#[derive(Debug, serde::Serialize)]
pub struct RefreshResponse {
    /// The rollup that was recomputed.
    pub rollup: String,
    /// Rollup points written by the recomputation.
    pub points_written: usize,
}

/// `POST /api/v1/rollups/{name}/refresh?start=&end=` — recompute a range.
///
/// The engine already repairs the ranges a `backfill` or a delete touched,
/// on its own schedule. This is the escape hatch for a change it could not
/// have observed — an out-of-band restore, a corrected import — and for an
/// operator who does not want to wait for the next maintenance pass.
pub async fn refresh_rollup_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Query(params): Query<RefreshParams>,
) -> Result<Json<RefreshResponse>, ServerError> {
    // Names are namespace-qualified, so a tenant can only refresh its own.
    let name = crate::namespace::qualify_rollup(&state, ns_ctx.as_ref().map(|e| &e.0), &name);
    if params.end < params.start {
        return Err(ServerError::BadRequest(
            "end must not be before start".into(),
        ));
    }
    let db = state.db.clone();
    let rollup = name.clone();
    let points_written =
        tokio::task::spawn_blocking(move || db.refresh_rollup(&name, params.start, params.end))
            .await
            .map_err(|e| ServerError::Internal(e.to_string()))?
            .map_err(ServerError::Db)?;
    Ok(Json(RefreshResponse {
        rollup,
        points_written,
    }))
}

/// JSON body for creating a rollup configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateRollupRequest {
    /// Unique rollup name.
    pub name: String,
    /// Source measurement to aggregate from.
    pub source_measurement: String,
    /// Target measurement where rollup points are stored.
    pub target_measurement: String,
    /// Bucket width: `"30s"`, `"15m"`, `"1h"`, `"1d"`, `"1w"`, `"1mo"`,
    /// `"1y"`.
    ///
    /// The **unit** decides what a bucket means: sub-day units are a fixed
    /// span that never varies, super-day units follow the calendar of
    /// `timezone`. The month is `mo`; `m` is always the minute.
    pub every: String,
    /// IANA time zone the buckets are read against, e.g. `"Europe/Berlin"`.
    ///
    /// Without it a `1d` tier buckets on **UTC** midnight, which is 02:00
    /// local in Berlin in summer — so a "daily" total is a day's worth of
    /// somebody else's day.
    #[serde(default)]
    pub timezone: Option<String>,
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
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    Json(body): Json<CreateRollupRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let db = state.db.clone();
    // A rollup definition is global, but measurements are shared between
    // tenants — so an unscoped rollup aggregates *every* tenant's rows into
    // one target series, and two tenants asking for `cpu_1m` collide on the
    // name. The name is qualified per namespace, and the namespace tag joins
    // the group-by so each tenant's rows aggregate separately and the target
    // stays readable through the ordinary scoped read path.
    let rollup_name = match &scope {
        None => body.name.clone(),
        Some(ns) => format!("{ns}/{}", body.name),
    };

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
            "first" => Ok(chronix::RollupAggFn::First),
            "last" => Ok(chronix::RollupAggFn::Last),
            other => Err(ServerError::BadRequest(format!(
                "unknown aggregation function: {other}"
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Parsed here rather than inside the blocking task so a typo is a `400`
    // naming what was wrong, not a `500`.
    let bucket = chronix::timebucket::TimeBucket::parse(&body.every, body.timezone.as_deref())
        .map_err(|e| ServerError::BadRequest(e.0))?;

    let name_for_builder = rollup_name.clone();
    tokio::task::spawn_blocking(move || {
        let mut builder = chronix::RollupBuilder::new()
            .name(&name_for_builder)
            .source(&body.source_measurement)
            .target(&body.target_measurement)
            .bucket(bucket);

        for agg in &agg_fns {
            builder = builder.aggregation(*agg);
        }
        for tag in &body.group_by_tags {
            builder = builder.group_by(tag);
        }
        if scope.is_some() {
            builder = builder.group_by(crate::namespace::NAMESPACE_TAG);
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
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let db = state.db.clone();
    let rollup_name =
        crate::namespace::qualify_rollup(&state, ns_ctx.as_ref().map(|e| &e.0), &name);

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
#[serde(deny_unknown_fields)]
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
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    Json(body): Json<ExportRequest>,
) -> Result<Json<serde_json::Value>, ServerError> {
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
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

        // Scoped like every other read: an export used to write **every**
        // tenant's rows into a file the caller then downloads.
        let mut builder = db
            .query()
            .measurement(&body.measurement)
            .namespace_scope(scope.as_deref());
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
/// accepting writes. Returns **503** with `{"ready": false, "reason": …}`
/// when it is not — closed, or the WAL poisoned by a failed `fsync`, after
/// which every write is refused until the database is reopened.
///
/// Transient back-pressure is deliberately **not** unready: a full memtable
/// waiting on a flush is the moment to keep serving and let back-pressure
/// work, not the moment to leave the load balancer.
pub async fn ready_handler(State(state): State<AppState>) -> impl IntoResponse {
    let db = state.db.clone();
    let verdict = tokio::task::spawn_blocking(move || db.check_writable()).await;
    match verdict {
        Ok(Ok(())) => (StatusCode::OK, Json(serde_json::json!({ "ready": true }))),
        Ok(Err(e)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "ready": false, "reason": e.to_string() })),
        ),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "ready": false, "reason": e.to_string() })),
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
#[serde(deny_unknown_fields)]
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
) -> Result<impl IntoResponse, ServerError> {
    // `ServerError` rather than a bare `{"error": …}`: this handler answered
    // in an envelope of its own, without the `code` every other error on this
    // server carries.
    crate::otel::update_log_filter(&body.filter)
        .map_err(|e| ServerError::BadRequest(format!("invalid log filter: {e}")))?;
    debug!(new_filter = %body.filter, "log filter updated at runtime");
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "ok",
            "filter": body.filter,
        })),
    ))
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
