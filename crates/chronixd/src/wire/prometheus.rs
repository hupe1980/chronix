//! Prometheus remote write and remote read handlers.
//!
//! - `POST /api/v1/prom/write` — accepts Snappy-compressed protobuf `WriteRequest`
//! - `POST /api/v1/prom/read`  — accepts Snappy-compressed protobuf `ReadRequest`
//!
//! Wire format: content-type `application/x-protobuf`, body is Snappy-compressed.
//!
//! ## Compatibility
//!
//! These endpoints implement the Prometheus Remote Write 1.0 and Remote Read
//! protocols, enabling Chronix to act as a long-term storage backend for
//! Prometheus. Configure Prometheus with:
//!
//! ```yaml
//! remote_write:
//!   - url: "http://<chronix-host>:8080/api/v1/prom/write"
//!
//! remote_read:
//!   - url: "http://<chronix-host>:8080/api/v1/prom/read"
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::Array;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use prost::Message;
use tracing::debug;

use chronix::prelude::*;
use chronix::Chronix;

use crate::error::ServerError;
use crate::http::AppState;
use crate::prom_proto;

/// `POST /api/v1/prom/write` — Prometheus remote write endpoint.
///
/// Accepts Snappy-compressed protobuf `WriteRequest`. Each `TimeSeries`
/// is mapped to a Chronix point with:
/// - `__name__` label → measurement name
/// - Other labels → tags
/// - Each sample → a point with field `value` (float64)
pub async fn remote_write_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    body: Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    // Guard against decompression bombs: check the declared decompressed
    // size before allocating memory.  We cap at `max_body_size` (default
    // 10 MB) to bound memory usage.
    let max_decompressed = state.config.max_body_size;
    let declared_len = snap::raw::decompress_len(&body)
        .map_err(|e| ServerError::BadRequest(format!("snappy frame error: {e}")))?;
    if declared_len > max_decompressed {
        return Err(ServerError::BadRequest(format!(
            "decompressed size {declared_len} exceeds limit {max_decompressed}"
        )));
    }

    // Decompress Snappy
    let decompressed = snap::raw::Decoder::new()
        .decompress_vec(&body)
        .map_err(|e| ServerError::BadRequest(format!("snappy decompress error: {e}")))?;

    // Decode protobuf
    let write_req = prom_proto::WriteRequest::decode(decompressed.as_slice())
        .map_err(|e| ServerError::BadRequest(format!("protobuf decode error: {e}")))?;

    let points = convert_write_request(&write_req)?;

    if points.is_empty() {
        return Ok(StatusCode::NO_CONTENT);
    }

    let count = points.len();

    crate::util::insert_with_timeout(&state.db, scope.as_deref(), points, state.write_timeout)
        .await?;

    debug!(count, "wrote points via Prometheus remote write");
    metrics::counter!("chronix_prom_remote_write_samples_total").increment(count as u64);
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /api/v1/prom/read` — Prometheus remote read endpoint.
///
/// Accepts Snappy-compressed protobuf `ReadRequest`. Each query's label
/// matchers are translated to Chronix tag filters.
pub async fn remote_read_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    body: Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    // Guard against decompression bombs.
    let max_decompressed = state.config.max_body_size;
    let declared_len = snap::raw::decompress_len(&body)
        .map_err(|e| ServerError::BadRequest(format!("snappy frame error: {e}")))?;
    if declared_len > max_decompressed {
        return Err(ServerError::BadRequest(format!(
            "decompressed size {declared_len} exceeds limit {max_decompressed}"
        )));
    }

    // Decompress Snappy
    let decompressed = snap::raw::Decoder::new()
        .decompress_vec(&body)
        .map_err(|e| ServerError::BadRequest(format!("snappy decompress error: {e}")))?;

    // Decode protobuf
    let read_req = prom_proto::ReadRequest::decode(decompressed.as_slice())
        .map_err(|e| ServerError::BadRequest(format!("protobuf decode error: {e}")))?;

    let db = state.db.clone();
    let response =
        tokio::task::spawn_blocking(move || execute_read_request(&db, scope.as_deref(), &read_req))
            .await
            .map_err(|e| ServerError::Internal(e.to_string()))??;

    // Encode + compress response
    let encoded = response.encode_to_vec();
    let compressed = snap::raw::Encoder::new()
        .compress_vec(&encoded)
        .map_err(|e| ServerError::Internal(format!("snappy compress error: {e}")))?;

    metrics::counter!("chronix_prom_remote_read_queries_total").increment(1);

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/x-protobuf")],
        compressed,
    ))
}

/// Convert a Prometheus `WriteRequest` to Chronix points.
fn convert_write_request(req: &prom_proto::WriteRequest) -> Result<Vec<Point>, ServerError> {
    let mut points = Vec::new();

    for ts in &req.timeseries {
        let mut measurement = String::new();
        let mut tags = BTreeMap::new();

        for label in &ts.labels {
            if label.name == "__name__" {
                measurement.clone_from(&label.value);
            } else {
                tags.insert(label.name.clone(), label.value.clone());
            }
        }

        if measurement.is_empty() {
            return Err(ServerError::BadRequest(
                "time series missing __name__ label".into(),
            ));
        }

        for sample in &ts.samples {
            // Staleness markers and NaN gauges are routine on this wire and
            // must not fail the batch — see `util::storable_sample`.
            let Some(value) = crate::util::storable_sample(sample.value) else {
                continue;
            };

            // Prometheus sends timestamps in milliseconds; Chronix uses nanoseconds
            let timestamp_ns = sample.timestamp * 1_000_000;

            let key = SeriesKey::new(&measurement, tags.clone())
                .map_err(|e| ServerError::BadRequest(e.to_string()))?;

            let mut fields = BTreeMap::new();
            fields.insert("value".to_string(), FieldValue::F64(value));

            let point = Point::new(key, fields, timestamp_ns)
                .map_err(|e| ServerError::BadRequest(e.to_string()))?;
            points.push(point);
        }
    }

    Ok(points)
}

/// Execute a Prometheus `ReadRequest` against the Chronix database.
fn execute_read_request(
    db: &Arc<Chronix>,
    namespace: Option<&str>,
    req: &prom_proto::ReadRequest,
) -> Result<prom_proto::ReadResponse, ServerError> {
    let mut results = Vec::new();

    for query in &req.queries {
        let timeseries = execute_single_query(db, namespace, query)?;
        results.push(prom_proto::QueryResult { timeseries });
    }

    Ok(prom_proto::ReadResponse { results })
}

/// One matcher that cannot be pushed into the scan, applied per series.
struct PostFilter {
    /// The label it constrains.
    label: String,
    /// How it constrains it.
    kind: PostFilterKind,
}

/// The kinds of matcher the scan cannot express.
enum PostFilterKind {
    /// `label != value`.
    NotEqual(String),
    /// `label =~ value` (or `!~` when negated). Anchored, `(?s)`, as
    /// Prometheus anchors its own.
    Regex {
        /// The compiled, anchored pattern.
        re: regex::Regex,
        /// `true` for `!~`.
        negated: bool,
    },
}

impl PostFilter {
    /// Does a series' label set satisfy this matcher?
    ///
    /// An absent label is the empty string, which is Prometheus's rule and
    /// the reason `job!="a"` also selects series with no `job` at all.
    fn matches(&self, labels: &std::collections::BTreeMap<String, String>) -> bool {
        let value = labels.get(&self.label).map_or("", String::as_str);
        match &self.kind {
            PostFilterKind::NotEqual(v) => value != v,
            PostFilterKind::Regex { re, negated } => re.is_match(value) != *negated,
        }
    }
}

/// Which **metrics** a query's `__name__` matchers select.
///
/// A metric is one `(measurement, field)` pair, resolved through
/// `promql::metric` — the same function the evaluator and the discovery
/// endpoints use. Resolving a *measurement* here meant a federating Prometheus
/// asking for `cpu_usage` got nothing, and asking for `cpu` got whichever
/// field happened to sort first, under the measurement's name, with the other
/// fields invisible.
///
/// The candidate set is restricted to measurements the namespace holds, so the
/// schema registry — which is process-wide — cannot tell one tenant what
/// another is writing.
fn resolve_metrics(
    db: &Arc<Chronix>,
    namespace: Option<&str>,
    query: &prom_proto::Query,
) -> Result<Vec<chronix::promql::MetricRef>, ServerError> {
    let mut exact: Option<String> = None;
    let mut patterns: Vec<(regex::Regex, bool)> = Vec::new();
    for matcher in &query.matchers {
        if matcher.name != "__name__" {
            continue;
        }
        let match_type =
            prom_proto::label_matcher::Type::try_from(matcher.r#type).map_err(|_| {
                ServerError::BadRequest(format!("unknown label matcher type: {}", matcher.r#type))
            })?;
        match match_type {
            prom_proto::label_matcher::Type::Eq => exact = Some(matcher.value.clone()),
            prom_proto::label_matcher::Type::Re | prom_proto::label_matcher::Type::Nre => {
                let re = regex::Regex::new(&format!("^(?s:{})$", matcher.value))
                    .map_err(|e| ServerError::BadRequest(format!("invalid __name__ regex: {e}")))?;
                patterns.push((re, match_type == prom_proto::label_matcher::Type::Nre));
            }
            prom_proto::label_matcher::Type::Neq => {
                patterns.push((
                    regex::Regex::new(&format!("^{}$", regex::escape(&matcher.value)))
                        .map_err(|e| ServerError::Internal(e.to_string()))?,
                    true,
                ));
            }
        }
    }

    if exact.is_none() && patterns.is_empty() {
        return Err(ServerError::BadRequest(
            "read query must include a __name__ matcher".into(),
        ));
    }

    let start_ns = query.start_timestamp_ms.saturating_mul(1_000_000);
    let end_ns = query.end_timestamp_ms.saturating_mul(1_000_000);
    let held: std::collections::HashSet<String> =
        crate::namespace::measurements_in(db, namespace, start_ns, end_ns, usize::MAX)
            .into_iter()
            .collect();
    let registry = db.schema_registry();

    let candidates = match &exact {
        Some(name) => chronix::promql::metric::resolve(registry, name),
        None => chronix::promql::all_metrics(registry),
    };
    Ok(candidates
        .into_iter()
        .filter(|m| held.contains(&m.measurement))
        .filter(|m| {
            patterns
                .iter()
                .all(|(re, neg)| re.is_match(&m.name) != *neg)
        })
        .collect())
}

/// Execute a single Prometheus read query, over every measurement its
/// `__name__` matchers select.
fn execute_single_query(
    db: &Arc<Chronix>,
    namespace: Option<&str>,
    query: &prom_proto::Query,
) -> Result<Vec<prom_proto::TimeSeries>, ServerError> {
    let mut out = Vec::new();
    for metric in resolve_metrics(db, namespace, query)? {
        out.extend(read_one_metric(db, namespace, query, &metric)?);
    }
    Ok(out)
}

/// Read one metric, applying every matcher.
fn read_one_metric(
    db: &Arc<Chronix>,
    namespace: Option<&str>,
    query: &prom_proto::Query,
    metric: &chronix::promql::MetricRef,
) -> Result<Vec<prom_proto::TimeSeries>, ServerError> {
    let measurement = metric.measurement.as_str();
    // Equality matchers are pushed into the scan; the rest are applied per
    // series below. They used to be collected and dropped, behind a comment
    // claiming a post-filter that did not exist — so `job!="a"` returned
    // *every* job.
    let mut tag_filters: Vec<(&str, &str)> = Vec::new();
    let mut post_filters: Vec<PostFilter> = Vec::new();

    for matcher in &query.matchers {
        if matcher.name == "__name__" {
            continue; // resolved into the measurement list already
        }
        let match_type =
            prom_proto::label_matcher::Type::try_from(matcher.r#type).map_err(|_| {
                ServerError::BadRequest(format!("unknown label matcher type: {}", matcher.r#type))
            })?;
        match match_type {
            prom_proto::label_matcher::Type::Eq => {
                tag_filters.push((&matcher.name, &matcher.value));
            }
            prom_proto::label_matcher::Type::Neq => post_filters.push(PostFilter {
                label: matcher.name.clone(),
                kind: PostFilterKind::NotEqual(matcher.value.clone()),
            }),
            prom_proto::label_matcher::Type::Re | prom_proto::label_matcher::Type::Nre => {
                let re = regex::Regex::new(&format!("^(?s:{})$", matcher.value)).map_err(|e| {
                    ServerError::BadRequest(format!(
                        "invalid regex for label {}: {e}",
                        matcher.name
                    ))
                })?;
                post_filters.push(PostFilter {
                    label: matcher.name.clone(),
                    kind: PostFilterKind::Regex {
                        re,
                        negated: match_type == prom_proto::label_matcher::Type::Nre,
                    },
                });
            }
        }
    }

    // Query time range: Prometheus sends milliseconds. Saturating, because a
    // client is free to send a timestamp whose nanoseconds do not fit.
    let start_ns = query.start_timestamp_ms.saturating_mul(1_000_000);
    let end_ns = query.end_timestamp_ms.saturating_mul(1_000_000);

    let mut builder = db
        .query()
        .measurement(measurement)
        .namespace_scope(namespace)
        .range(start_ns, end_ns);
    for (k, v) in &tag_filters {
        builder = builder.tag(*k, *v);
    }

    let plan = builder
        .build()
        .map_err(|e| ServerError::Internal(format!("query build error: {e}")))?;
    // Group samples by tag combination → series. Streaming the scan keeps
    // peak memory to the series map plus one batch rather than the series map
    // plus every batch that fed it.
    let mut series_map: BTreeMap<BTreeMap<String, String>, Vec<prom_proto::Sample>> =
        BTreeMap::new();

    for batch in db.execute_iter(&plan).map_err(ServerError::Db)? {
        let batch = batch.map_err(ServerError::Db)?;
        let batch = &batch;
        let schema = batch.schema();
        let num_rows = batch.num_rows();

        // Find column indices
        let ts_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "timestamp")
            .unwrap_or(0);

        // The metric's own field. Taking "the first non-string column"
        // returned one field of a multi-field measurement and hid the rest.
        let val_idx = schema
            .fields()
            .iter()
            .position(|f| *f.name() == metric.field);

        // Find tag columns
        let tag_indices: Vec<(String, usize)> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| {
                f.name() != "timestamp" && matches!(f.data_type(), arrow::datatypes::DataType::Utf8)
            })
            .map(|(i, f)| (f.name().clone(), i))
            .collect();

        for row in 0..num_rows {
            let timestamp_ms = {
                let col = batch.column(ts_idx);
                if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                    arr.value(row) / 1_000_000 // ns → ms
                } else {
                    continue;
                }
            };

            let value = if let Some(vi) = val_idx {
                let col = batch.column(vi);
                // A row written before this field existed carries a null, and
                // a null is not a sample of zero.
                if arrow::array::Array::is_null(col.as_ref(), row) {
                    continue;
                }
                if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Float64Array>() {
                    arr.value(row)
                } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                    arr.value(row) as f64
                } else {
                    continue;
                }
            } else {
                continue;
            };

            let mut row_tags = BTreeMap::new();
            for (name, idx) in &tag_indices {
                let col = batch.column(*idx);
                if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
                    if !arr.is_null(row) {
                        row_tags.insert(name.clone(), arr.value(row).to_string());
                    }
                }
            }

            series_map
                .entry(row_tags)
                .or_default()
                .push(prom_proto::Sample {
                    value,
                    timestamp: timestamp_ms,
                });
        }
    }

    // Convert to TimeSeries, applying the matchers the scan could not.
    let mut result = Vec::with_capacity(series_map.len());
    for (mut tags, samples) in series_map {
        // The namespace tag is the server's own bookkeeping. Returning it as
        // a label told each tenant its own scope name and, worse, made a
        // round trip through remote write and read change the series.
        tags.remove(crate::namespace::NAMESPACE_TAG);
        if !post_filters.iter().all(|f| f.matches(&tags)) {
            continue;
        }
        let mut labels = vec![prom_proto::Label {
            name: "__name__".to_string(),
            value: metric.name.clone(),
        }];
        for (k, v) in tags {
            labels.push(prom_proto::Label { name: k, value: v });
        }
        result.push(prom_proto::TimeSeries {
            labels,
            samples,
            exemplars: Vec::new(),
        });
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn convert_write_request_basic() {
        let req = prom_proto::WriteRequest {
            timeseries: vec![prom_proto::TimeSeries {
                labels: vec![
                    prom_proto::Label {
                        name: "__name__".into(),
                        value: "cpu_usage".into(),
                    },
                    prom_proto::Label {
                        name: "host".into(),
                        value: "srv1".into(),
                    },
                ],
                samples: vec![
                    prom_proto::Sample {
                        value: 72.5,
                        timestamp: 1000,
                    },
                    prom_proto::Sample {
                        value: 85.0,
                        timestamp: 2000,
                    },
                ],
                exemplars: vec![],
            }],
        };

        let points = convert_write_request(&req).unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].series_key().measurement(), "cpu_usage");
        assert_eq!(points[0].tag("host"), Some("srv1"));
        // Timestamp: 1000ms → 1_000_000_000ns
        assert_eq!(points[0].timestamp(), 1_000_000_000);
        assert!(matches!(
            points[0].field("value"),
            Some(FieldValue::F64(v)) if (*v - 72.5).abs() < f64::EPSILON
        ));
    }

    #[test]
    fn convert_write_request_missing_name() {
        let req = prom_proto::WriteRequest {
            timeseries: vec![prom_proto::TimeSeries {
                labels: vec![prom_proto::Label {
                    name: "host".into(),
                    value: "srv1".into(),
                }],
                samples: vec![prom_proto::Sample {
                    value: 1.0,
                    timestamp: 1000,
                }],
                exemplars: vec![],
            }],
        };

        let err = convert_write_request(&req).unwrap_err();
        assert!(matches!(err, ServerError::BadRequest(_)));
    }

    /// A staleness marker must not fail the batch.
    ///
    /// Prometheus marks the end of a series' life with a `NaN` carrying the
    /// payload `0x7ff0000000000002`, and it sends one every time a scrape
    /// target disappears. `Point::new` rejects non-finite floats, and that
    /// rejection used to become a `400` for the whole request — which
    /// Prometheus responds to by retrying the same batch indefinitely, so one
    /// vanished target stalled the remote-write queue and stopped ingestion
    /// altogether. The valid samples beside it must still land.
    #[test]
    fn a_staleness_marker_is_skipped_not_fatal() {
        const STALE_NAN: u64 = 0x7ff0_0000_0000_0002;
        let req = prom_proto::WriteRequest {
            timeseries: vec![prom_proto::TimeSeries {
                labels: vec![prom_proto::Label {
                    name: "__name__".into(),
                    value: "up".into(),
                }],
                samples: vec![
                    prom_proto::Sample {
                        value: 1.0,
                        timestamp: 1_700_000_000_000,
                    },
                    prom_proto::Sample {
                        value: f64::from_bits(STALE_NAN),
                        timestamp: 1_700_000_015_000,
                    },
                    prom_proto::Sample {
                        value: 1.0,
                        timestamp: 1_700_000_030_000,
                    },
                ],
                exemplars: vec![],
            }],
        };

        let points = convert_write_request(&req).expect("a staleness marker must not 400");
        assert_eq!(
            points.len(),
            2,
            "the marker is dropped and the two real samples are kept"
        );
    }

    /// The same rule for ±Inf, which a counter reset or a division by zero on
    /// the sender's side can produce.
    #[test]
    fn infinite_samples_are_skipped_not_fatal() {
        let req = prom_proto::WriteRequest {
            timeseries: vec![prom_proto::TimeSeries {
                labels: vec![prom_proto::Label {
                    name: "__name__".into(),
                    value: "ratio".into(),
                }],
                samples: vec![
                    prom_proto::Sample {
                        value: f64::INFINITY,
                        timestamp: 1_700_000_000_000,
                    },
                    prom_proto::Sample {
                        value: 0.5,
                        timestamp: 1_700_000_015_000,
                    },
                ],
                exemplars: vec![],
            }],
        };
        let points = convert_write_request(&req).expect("must not 400");
        assert_eq!(points.len(), 1);
    }

    #[test]
    fn convert_write_request_empty() {
        let req = prom_proto::WriteRequest { timeseries: vec![] };
        let points = convert_write_request(&req).unwrap();
        assert!(points.is_empty());
    }

    #[test]
    fn convert_write_request_multiple_series() {
        let req = prom_proto::WriteRequest {
            timeseries: vec![
                prom_proto::TimeSeries {
                    labels: vec![
                        prom_proto::Label {
                            name: "__name__".into(),
                            value: "cpu".into(),
                        },
                        prom_proto::Label {
                            name: "host".into(),
                            value: "a".into(),
                        },
                    ],
                    samples: vec![prom_proto::Sample {
                        value: 10.0,
                        timestamp: 100,
                    }],
                    exemplars: vec![],
                },
                prom_proto::TimeSeries {
                    labels: vec![
                        prom_proto::Label {
                            name: "__name__".into(),
                            value: "mem".into(),
                        },
                        prom_proto::Label {
                            name: "host".into(),
                            value: "b".into(),
                        },
                    ],
                    samples: vec![prom_proto::Sample {
                        value: 20.0,
                        timestamp: 200,
                    }],
                    exemplars: vec![],
                },
            ],
        };

        let points = convert_write_request(&req).unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].series_key().measurement(), "cpu");
        assert_eq!(points[1].series_key().measurement(), "mem");
    }

    #[test]
    fn snappy_roundtrip() {
        let req = prom_proto::WriteRequest {
            timeseries: vec![prom_proto::TimeSeries {
                labels: vec![prom_proto::Label {
                    name: "__name__".into(),
                    value: "test".into(),
                }],
                samples: vec![prom_proto::Sample {
                    value: 42.0,
                    timestamp: 1000,
                }],
                exemplars: vec![],
            }],
        };

        // Encode → compress → decompress → decode
        let encoded = req.encode_to_vec();
        let compressed = snap::raw::Encoder::new().compress_vec(&encoded).unwrap();
        let decompressed = snap::raw::Decoder::new()
            .decompress_vec(&compressed)
            .unwrap();
        let decoded = prom_proto::WriteRequest::decode(decompressed.as_slice()).unwrap();

        assert_eq!(decoded.timeseries.len(), 1);
        assert_eq!(decoded.timeseries[0].labels[0].value, "test");
    }
}
