//! Declared detectors over HTTP: `GET`/`POST /api/v1/detectors`,
//! `DELETE /api/v1/detectors/{name}`.
//!
//! The JSON mirrors the rollup routes: buckets are the strings `every`
//! takes (`"15m"`, `"1d"`, `"1mo"`) with an optional IANA `timezone`, and a
//! tenant's declarations are namespace-qualified and confined to its own
//! rows, exactly as its rollups are.

use std::collections::BTreeMap;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use super::{AppState, DEFAULT_LIST_LIMIT, PaginatedResponse, PaginationParams};
use crate::error::ServerError;

/// A season in a request: `slot`-wide slots in each `period`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeasonRequest {
    /// Slot width, e.g. `"15m"` or `"1mo"`.
    pub slot: String,
    /// Period width, e.g. `"1d"` or `"1y"`.
    pub period: String,
}

/// CUSUM parameters in a request.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CusumRequest {
    /// Allowance, in standard deviations.
    pub k: f64,
    /// Decision interval, in standard deviations.
    pub h: f64,
}

/// `POST /api/v1/detectors` body.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclareDetectorRequest {
    /// Unique name.
    pub name: String,
    /// The measurement to watch.
    pub source_measurement: String,
    /// The numeric field to score.
    pub field: String,
    /// Judge only rows whose tags equal these.
    #[serde(default)]
    pub r#where: BTreeMap<String, String>,
    /// One baseline per combination of these tags; omitted = per series.
    #[serde(default)]
    pub group_by_tags: Option<Vec<String>>,
    /// Learn the baseline per slot of this season.
    #[serde(default)]
    pub season: Option<SeasonRequest>,
    /// IANA zone the season is read in; `null` = UTC.
    #[serde(default)]
    pub timezone: Option<String>,
    /// Flag points beyond this many standard deviations.
    #[serde(default)]
    pub zscore: Option<f64>,
    /// Flag a persistent shift.
    #[serde(default)]
    pub cusum: Option<CusumRequest>,
    /// Samples a slot needs before it is scored.
    #[serde(default)]
    pub min_samples: Option<u32>,
    /// Half-life of the baseline, in periods.
    #[serde(default)]
    pub forgetting: Option<u32>,
    /// Where verdicts go; default `<name>_verdicts`.
    #[serde(default)]
    pub output_measurement: Option<String>,
    /// How far behind now to judge, in seconds.
    #[serde(default)]
    pub settle_seconds: Option<u64>,
    /// Source points per maintenance pass.
    #[serde(default)]
    pub max_points_per_pass: Option<u32>,
}

/// A detector as the list route reports it.
#[derive(Debug, Serialize)]
pub struct DetectorResponse {
    /// Name, without the namespace prefix.
    pub name: String,
    /// Source measurement.
    pub source_measurement: String,
    /// Scored field.
    pub field: String,
    /// Row filter.
    pub r#where: BTreeMap<String, String>,
    /// Season, as `{slot, period}` strings.
    pub season: Option<BTreeMap<&'static str, String>>,
    /// Zone of the season.
    pub timezone: Option<String>,
    /// z-score threshold.
    pub zscore: Option<f64>,
    /// CUSUM parameters.
    pub cusum: Option<CusumRequest>,
    /// Where verdicts are written.
    pub output_measurement: String,
    /// Newest source timestamp judged, nanoseconds.
    pub watermark_ns: Option<i64>,
    /// Series (or tag groups) with a baseline.
    pub series: usize,
    /// Points that arrived too late to be judged.
    pub late_points: u64,
    /// `{"state": "active" | "inactive" | "degraded", "reason"?: …}`.
    pub status: chronix::DetectorStatus,
}

fn response(info: chronix::DetectorInfo, visible_name: String) -> DetectorResponse {
    let d = info.declaration;
    DetectorResponse {
        name: visible_name,
        source_measurement: d.source_measurement,
        field: d.field,
        r#where: d
            .where_tags
            .into_iter()
            .filter(|(k, _)| k != crate::namespace::NAMESPACE_TAG)
            .collect(),
        season: d.season.map(|s| {
            BTreeMap::from([
                ("slot", s.slot().width().to_string()),
                ("period", s.period().width().to_string()),
            ])
        }),
        timezone: d
            .season
            .and_then(|s| s.slot().timezone().map(str::to_string)),
        zscore: d.zscore,
        cusum: d.cusum.map(|(k, h)| CusumRequest { k, h }),
        output_measurement: d.output_measurement,
        watermark_ns: info.watermark_ns,
        series: info.series,
        late_points: info.late_points,
        status: info.status,
    }
}

/// `GET /api/v1/detectors` — list declared detectors.
pub async fn list_detectors_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    Query(pagination): Query<PaginationParams>,
) -> Result<Json<PaginatedResponse<DetectorResponse>>, ServerError> {
    let prefix =
        crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(|ns| format!("{ns}/"));
    let db = state.db.clone();
    let infos = tokio::task::spawn_blocking(move || db.detectors())
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?
        .map_err(ServerError::Db)?;
    let all: Vec<DetectorResponse> = infos
        .into_iter()
        .filter_map(|info| {
            let visible = match &prefix {
                None => info.declaration.name.clone(),
                // Another tenant's detector is not listed.
                Some(p) => info.declaration.name.strip_prefix(p.as_str())?.to_string(),
            };
            Some(response(info, visible))
        })
        .collect();
    let total = all.len();
    let offset = pagination.offset.unwrap_or(0);
    let limit = pagination.limit.unwrap_or(DEFAULT_LIST_LIMIT);
    Ok(Json(PaginatedResponse {
        items: all.into_iter().skip(offset).take(limit).collect(),
        total,
        offset,
        limit,
    }))
}

/// `POST /api/v1/detectors` — declare a detector. `201` on creation and on
/// an identical re-declaration; `409` if the name holds a different one.
pub async fn declare_detector_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    req_extensions: axum::http::Extensions,
    Json(body): Json<DeclareDetectorRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let qualified = match &scope {
        None => body.name.clone(),
        Some(ns) => format!("{ns}/{}", body.name),
    };

    let mut builder = chronix::DetectorDeclaration::builder(&qualified)
        .source(&body.source_measurement, &body.field);
    for (k, v) in &body.r#where {
        if k == crate::namespace::NAMESPACE_TAG {
            return Err(ServerError::BadRequest(format!(
                "'{k}' is set by the server from the caller's namespace"
            )));
        }
        builder = builder.where_tag(k, v);
    }
    // A tenant's detector judges the tenant's rows and nothing else.
    if let Some(ns) = &scope {
        builder = builder.where_tag(crate::namespace::NAMESPACE_TAG, ns);
    }
    if let Some(tags) = &body.group_by_tags {
        builder = builder.group_by(tags.iter().cloned());
    }
    if let Some(season) = &body.season {
        let parse = |w: &str| {
            chronix_core::timebucket::TimeBucket::parse(w, body.timezone.as_deref())
                .map_err(|e| ServerError::BadRequest(e.0))
        };
        let season = chronix::Season::new(parse(&season.slot)?, parse(&season.period)?)
            .map_err(ServerError::Db)?;
        builder = builder.season(season);
    }
    if let Some(t) = body.zscore {
        builder = builder.zscore(t);
    }
    if let Some(c) = &body.cusum {
        builder = builder.cusum(c.k, c.h);
    }
    if let Some(n) = body.min_samples {
        builder = builder.min_samples(n);
    }
    if let Some(h) = body.forgetting {
        builder = builder.forgetting(h);
    }
    if let Some(out) = &body.output_measurement {
        builder = builder.output(out);
    }
    if let Some(s) = body.settle_seconds {
        builder = builder.settle(std::time::Duration::from_secs(s));
    }
    if let Some(n) = body.max_points_per_pass {
        builder = builder.max_points_per_pass(n);
    }
    let declaration = builder.build().map_err(ServerError::Db)?;

    let db = state.db.clone();
    let name_for_lookup = qualified.clone();
    let info = tokio::task::spawn_blocking(move || {
        db.declare_detector(declaration)?;
        Ok::<_, chronix::DbError>(
            db.detectors()?
                .into_iter()
                .find(|d| d.declaration.name == name_for_lookup),
        )
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))?
    .map_err(ServerError::Db)?
    .ok_or_else(|| ServerError::Internal("a declared detector was not listed".into()))?;

    tracing::info!(detector = %qualified, "detector declared");
    crate::audit::record(
        &state,
        &crate::audit::principal_of(&req_extensions),
        chronix_security::audit::AuditAction::CreateDetector,
        qualified,
        chronix_security::audit::AuditDecision::Allow,
        &[("source", body.source_measurement.clone())],
    );
    Ok((StatusCode::CREATED, Json(response(info, body.name))))
}

/// `DELETE /api/v1/detectors/{name}` — drop a detector and its state; its
/// verdicts stay.
pub async fn drop_detector_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    req_extensions: axum::http::Extensions,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let qualified = crate::namespace::qualify_rollup(&state, ns_ctx.as_ref().map(|e| &e.0), &name);
    let db = state.db.clone();
    let target = qualified.clone();
    tokio::task::spawn_blocking(move || db.drop_detector(&target))
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?
        .map_err(ServerError::Db)?;
    tracing::info!(detector = %qualified, "detector dropped");
    crate::audit::record(
        &state,
        &crate::audit::principal_of(&req_extensions),
        chronix_security::audit::AuditAction::DropDetector,
        qualified,
        chronix_security::audit::AuditDecision::Allow,
        &[],
    );
    Ok(StatusCode::NO_CONTENT)
}
