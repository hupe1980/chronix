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
#[serde(deny_unknown_fields)]
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

/// JSON body for write: either a single point or an array of them.
#[derive(Debug)]
pub enum WriteBody {
    /// A single data point.
    Single(WritePointRequest),
    /// A batch of data points.
    Batch(Vec<WritePointRequest>),
}

impl WriteBody {
    /// Parse a request body, dispatching on its first token.
    ///
    /// Hand-written rather than `#[serde(untagged)]`, for two reasons that
    /// point the same way. An untagged enum discards its variants' errors and
    /// reports only that none matched, so one mistyped key in a batch of
    /// fifty thousand names nothing; and it buffers the whole body into a
    /// `Content` tree before trying a variant, costing a full copy per write.
    /// Dispatching on `[` versus `{` decides the shape first, so the caller
    /// gets serde's own message with the position and the body is
    /// deserialised once.
    fn from_slice(body: &[u8]) -> Result<Self, ServerError> {
        let first = body.iter().find(|b| !b.is_ascii_whitespace());
        match first {
            Some(b'[') => serde_json::from_slice(body)
                .map(WriteBody::Batch)
                .map_err(|e| ServerError::BadRequest(format!("invalid write batch: {e}"))),
            Some(b'{') => serde_json::from_slice(body)
                .map(WriteBody::Single)
                .map_err(|e| ServerError::BadRequest(format!("invalid write point: {e}"))),
            Some(other) => Err(ServerError::BadRequest(format!(
                "a write body is a JSON object (one point) or an array of them; \
                 this one starts with {:?}",
                *other as char
            ))),
            None => Err(ServerError::BadRequest("empty write request".into())),
        }
    }
}

impl<S: Send + Sync> axum::extract::FromRequest<S> for WriteBody {
    type Rejection = ServerError;

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = axum::body::Bytes::from_request(req, state)
            .await
            .map_err(|e| ServerError::BadRequest(format!("cannot read request body: {e}")))?;
        Self::from_slice(&bytes)
    }
}

/// `POST /api/v1/write` — write one or more points.
pub async fn write_handler(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    ns_ctx: Option<axum::extract::Extension<NamespaceContext>>,
    axum::extract::Query(backfill): axum::extract::Query<crate::util::BackfillParam>,
    body: WriteBody,
) -> Result<impl IntoResponse, ServerError> {
    // Idempotency-Key deduplication. The claim is released automatically
    // unless the write succeeds, so a failed write does not turn the
    // client's retry into a 409.
    let claim = match (&state.write_dedup_cache, headers.get("idempotency-key")) {
        (Some(dedup), Some(key)) => {
            let key = key.to_str().map_err(|_| {
                ServerError::BadRequest("Idempotency-Key is not valid UTF-8".into())
            })?;
            match dedup.claim(key) {
                Some(c) => Some(c),
                None => {
                    metrics::counter!("chronix_write_dedup_rejected_total").increment(1);
                    return Err(ServerError::Conflict(
                        "duplicate write: Idempotency-Key already seen".into(),
                    ));
                }
            }
        }
        _ => None,
    };

    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let points_raw = match body {
        WriteBody::Single(p) => vec![p],
        WriteBody::Batch(ps) => ps,
    };

    if points_raw.is_empty() {
        return Err(ServerError::BadRequest("empty write request".into()));
    }

    let max_batch = state.config.server.max_write_batch_size;
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

    crate::util::insert_batch_with_mode(
        &state.db,
        scope.as_deref(),
        points,
        state.write_timeout,
        backfill.mode(),
    )
    .await?;

    debug!(count, "wrote points via REST");
    metrics::counter!("chronix_http_points_written_total").increment(count as u64);
    // The write landed, so the key now means "already done".
    if let Some(claim) = claim {
        claim.commit();
    }
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /api/v1/write/influx` — InfluxDB Line Protocol ingestion.
pub async fn write_influx_handler(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    ns_ctx: Option<axum::extract::Extension<NamespaceContext>>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    body: String,
) -> Result<axum::response::Response, ServerError> {
    // `precision` applies to every timestamp in the body. Telegraf and the
    // InfluxDB clients set it; ignoring it put a millisecond client's data
    // in 1970. `db`, `bucket`, `org`, `rp` and `u`/`p` are accepted and
    // ignored: chronix has one database per process and scopes by namespace
    // header, and rejecting them would break a client that always sends them.
    // `RawQuery` rather than `Query`, because a repeated key is legal here
    // and `precision` and `backfill` sit beside parameters chronix ignores.
    let (precision, backfill) = {
        let mut p = influx::Precision::Nanoseconds;
        let mut backfill = false;
        for (k, v) in form_urlencoded::parse(raw_query.unwrap_or_default().as_bytes()) {
            match k.as_ref() {
                "precision" => p = influx::Precision::parse(&v)?,
                "backfill" => backfill = v == "true" || v == "1",
                _ => {}
            }
        }
        (p, crate::util::WriteMode::from_flag(backfill))
    };
    // Idempotency-Key deduplication. The claim is released automatically
    // unless the write succeeds, so a failed write does not turn the
    // client's retry into a 409.
    let claim = match (&state.write_dedup_cache, headers.get("idempotency-key")) {
        (Some(dedup), Some(key)) => {
            let key = key.to_str().map_err(|_| {
                ServerError::BadRequest("Idempotency-Key is not valid UTF-8".into())
            })?;
            match dedup.claim(key) {
                Some(c) => Some(c),
                None => {
                    metrics::counter!("chronix_write_dedup_rejected_total").increment(1);
                    return Err(ServerError::Conflict(
                        "duplicate write: Idempotency-Key already seen".into(),
                    ));
                }
            }
        }
        _ => None,
    };

    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let parsed = influx::parse_line_protocol_partial(&body, precision);

    if parsed.points.is_empty() {
        return if parsed.errors.is_empty() {
            Err(ServerError::BadRequest("empty line protocol body".into()))
        } else {
            // Nothing usable: a plain 400, whose message names the first
            // failure so an operator can find the line.
            Err(ServerError::BadRequest(format!(
                "unable to parse: {}",
                parsed.errors.join("; ")
            )))
        };
    }

    let max_batch = state.config.server.max_write_batch_size;
    if parsed.points.len() > max_batch {
        return Err(ServerError::BadRequest(format!(
            "batch size {} exceeds limit of {max_batch}",
            parsed.points.len()
        )));
    }

    let count = parsed.points.len();

    crate::util::insert_batch_with_mode(
        &state.db,
        scope.as_deref(),
        parsed.points,
        state.write_timeout,
        backfill,
    )
    .await?;

    debug!(count, "wrote points via InfluxDB Line Protocol");
    metrics::counter!("chronix_influx_points_written_total").increment(count as u64);

    // InfluxDB's partial-write semantics: the good lines are stored and the
    // response says which were not, with a message clients recognise as
    // permanent. Failing the whole batch made Telegraf retry it for ever.
    if parsed.errors.is_empty() {
        if let Some(claim) = claim {
            claim.commit();
        }
        Ok(StatusCode::NO_CONTENT.into_response())
    } else {
        metrics::counter!("chronix_influx_lines_rejected_total")
            .increment(parsed.errors.len() as u64);
        // The accepted lines *were* stored, so the key is spent: a retry of
        // the identical body must not write them a second time.
        if let Some(claim) = claim {
            claim.commit();
        }
        Ok((
            StatusCode::BAD_REQUEST,
            // `error` and `code` first: every error this server returns
            // carries that envelope, whichever layer rejected the line.
            Json(serde_json::json!({
                "error": format!("partial write: {}", parsed.errors.join("; ")),
                "code": "PARTIAL_WRITE",
                "written": count,
                "rejected": parsed.errors.len(),
            })),
        )
            .into_response())
    }
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
