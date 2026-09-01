//! Write endpoint handlers (JSON and InfluxDB Line Protocol).

use std::collections::BTreeMap;

use axum::extract::{Json, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Deserialize;
use tracing::debug;

use chronix::prelude::*;

use crate::error::ServerError;
use crate::influx;
use crate::namespace::NamespaceContext;

use super::types::AppState;

/// JSON body for a single write point.
#[derive(Debug, Deserialize)]
pub struct WritePointRequest {
    /// Target measurement name.
    pub measurement: String,
    /// Key-value tag set (indexed metadata).
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    /// Field values (the actual data).
    pub fields: BTreeMap<String, serde_json::Value>,
    /// Optional timestamp (nanoseconds since epoch). Defaults to server time.
    #[serde(default)]
    pub timestamp: Option<i64>,
}

/// JSON body for write: either a single point or an array.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum WriteBody {
    /// A single data point.
    Single(WritePointRequest),
    /// A batch of data points.
    Batch(Vec<WritePointRequest>),
}

/// `POST /api/v1/write` — write one or more points.
pub async fn write_handler(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    ns_ctx: Option<axum::extract::Extension<NamespaceContext>>,
    Json(body): Json<WriteBody>,
) -> Result<impl IntoResponse, ServerError> {
    // Idempotency-Key deduplication — reject duplicate writes.
    if let Some(ref dedup) = state.write_dedup_cache {
        if let Some(idem_key) = headers.get("idempotency-key").and_then(|v| v.to_str().ok()) {
            if dedup.check_duplicate(idem_key) {
                metrics::counter!("chronix_write_dedup_rejected_total").increment(1);
                return Err(ServerError::Conflict(
                    "duplicate write: Idempotency-Key already seen".into(),
                ));
            }
        }
    }

    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let points_raw = match body {
        WriteBody::Single(p) => vec![p],
        WriteBody::Batch(ps) => ps,
    };

    if points_raw.is_empty() {
        return Err(ServerError::BadRequest("empty write request".into()));
    }

    let max_batch = state.config.max_write_batch_size;
    if points_raw.len() > max_batch {
        return Err(ServerError::BadRequest(format!(
            "batch size {} exceeds limit of {max_batch}",
            points_raw.len()
        )));
    }

    // The namespace is applied by `insert_with_timeout`, which every write
    // surface shares — a handler that stamped it itself is a handler that
    // could forget to, and five of them did.
    let points = points_raw
        .into_iter()
        .map(convert_write_point)
        .collect::<Result<Vec<Point>, ServerError>>()?;

    let count = points.len();

    crate::util::insert_with_timeout(&state.db, scope.as_deref(), points, state.write_timeout)
        .await?;

    debug!(count, "wrote points via REST");
    metrics::counter!("chronix_http_points_written_total").increment(count as u64);
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /api/v1/write/influx` — InfluxDB Line Protocol ingestion.
pub async fn write_influx_handler(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    ns_ctx: Option<axum::extract::Extension<NamespaceContext>>,
    body: String,
) -> Result<impl IntoResponse, ServerError> {
    // Idempotency-Key deduplication — reject duplicate writes.
    if let Some(ref dedup) = state.write_dedup_cache {
        if let Some(idem_key) = headers.get("idempotency-key").and_then(|v| v.to_str().ok()) {
            if dedup.check_duplicate(idem_key) {
                metrics::counter!("chronix_write_dedup_rejected_total").increment(1);
                return Err(ServerError::Conflict(
                    "duplicate write: Idempotency-Key already seen".into(),
                ));
            }
        }
    }

    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let points = influx::parse_line_protocol(&body)?;

    if points.is_empty() {
        return Err(ServerError::BadRequest("empty line protocol body".into()));
    }

    let max_batch = state.config.max_write_batch_size;
    if points.len() > max_batch {
        return Err(ServerError::BadRequest(format!(
            "batch size {} exceeds limit of {max_batch}",
            points.len()
        )));
    }

    let count = points.len();

    crate::util::insert_with_timeout(&state.db, scope.as_deref(), points, state.write_timeout)
        .await?;

    debug!(count, "wrote points via InfluxDB Line Protocol");
    metrics::counter!("chronix_influx_points_written_total").increment(count as u64);
    Ok(StatusCode::NO_CONTENT)
}

fn convert_write_point(req: WritePointRequest) -> Result<Point, ServerError> {
    let timestamp = match req.timestamp {
        Some(ts) => ts,
        None => crate::util::now_nanos()?,
    };

    let key = SeriesKey::new(&req.measurement, req.tags)
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let fields: BTreeMap<String, FieldValue> = req
        .fields
        .into_iter()
        .map(|(k, v)| {
            let fv = crate::util::json_value_to_field(&k, v)
                .map_err(|e| ServerError::BadRequest(e.to_string()))?;
            Ok((k, fv))
        })
        .collect::<Result<_, ServerError>>()?;

    Point::new(key, fields, timestamp).map_err(|e| ServerError::BadRequest(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn convert_write_point_basic() {
        let req = WritePointRequest {
            measurement: "cpu".into(),
            tags: BTreeMap::from([("host".into(), "srv1".into())]),
            fields: BTreeMap::from([("usage".into(), serde_json::json!(72.5))]),
            timestamp: Some(1000),
        };
        let point = convert_write_point(req).unwrap();
        assert_eq!(point.series_key().measurement(), "cpu");
        assert_eq!(point.timestamp(), 1000);
        assert_eq!(point.tag("host"), Some("srv1"));
    }

    #[test]
    fn convert_write_point_auto_timestamp() {
        let req = WritePointRequest {
            measurement: "m".into(),
            tags: BTreeMap::new(),
            fields: BTreeMap::from([("v".into(), serde_json::json!(1.0))]),
            timestamp: None,
        };
        let point = convert_write_point(req).unwrap();
        assert!(point.timestamp() > 0);
    }

    #[test]
    fn convert_write_point_integer_preserved() {
        let req = WritePointRequest {
            measurement: "m".into(),
            tags: BTreeMap::new(),
            fields: BTreeMap::from([("count".into(), serde_json::json!(42))]),
            timestamp: Some(1000),
        };
        let point = convert_write_point(req).unwrap();
        assert!(matches!(point.field("count"), Some(FieldValue::I64(42))));
    }

    #[test]
    fn convert_write_point_empty_measurement_error() {
        let req = WritePointRequest {
            measurement: "".into(),
            tags: BTreeMap::new(),
            fields: BTreeMap::from([("v".into(), serde_json::json!(1.0))]),
            timestamp: Some(1000),
        };
        let err = convert_write_point(req).unwrap_err();
        assert!(matches!(err, ServerError::BadRequest(_)));
    }
}
