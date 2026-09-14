//! OpenTelemetry OTLP metrics ingestion handler.
//!
//! `POST /api/v1/otlp/metrics` — accepts OTLP protobuf or JSON payloads
//! and maps them to Chronix points.
//!
//! Mapping:
//! - Metric name → measurement name
//! - Resource attributes + data-point attributes → tags
//! - Gauge data points → field `gauge` (float64)
//! - Sum data points → field `value` (float64)
//! - Histogram → field `value` (a native histogram with custom buckets)
//! - Exponential histogram → field `value` (a native histogram)
//! - Summary → fields: `count`, `sum`, `quantile_<q>`
//!
//! ## Both histogram shapes become one histogram
//!
//! An OTLP explicit-bucket histogram used to be spread across one `bucket_<b>`
//! field per boundary, plus `count` and `sum`. That stores the data and
//! answers nothing: `histogram_quantile` has no bucketed series to work on,
//! and a Grafana histogram panel over it renders as a summary. It is also the
//! per-bucket cardinality that native histograms exist to remove.
//!
//! Both shapes now land in a single `value` column of type histogram:
//! explicit bounds as schema −53 (custom buckets), exponential as the
//! matching base-2 schema. `histogram_quantile`, `histogram_count`,
//! `histogram_sum` and the rest read them, a rollup merges rather than drops
//! them, and the metric keeps its own name because the field is `value`.

use std::collections::BTreeMap;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use prost::Message;

/// Saturating u64→i64 conversion; clamps values > i64::MAX.
#[inline]
fn saturating_u64_to_i64(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}
use tracing::debug;

use chronix::prelude::*;

use crate::error::ServerError;
use crate::http::AppState;
use crate::otlp;

/// `POST /api/v1/otlp/metrics` — OpenTelemetry OTLP metrics endpoint.
///
/// Supports both protobuf (`application/x-protobuf`) and JSON
/// (`application/json`) content types.
pub async fn otlp_metrics_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    axum::extract::Query(backfill): axum::extract::Query<crate::util::BackfillParam>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/x-protobuf");

    let request = if content_type.contains("json") {
        // JSON format — parse via serde
        let json_str = std::str::from_utf8(&body)
            .map_err(|e| ServerError::BadRequest(format!("invalid UTF-8: {e}")))?;
        serde_json::from_str::<OtlpJsonRequest>(json_str)
            .map_err(|e| ServerError::BadRequest(format!("OTLP JSON parse error: {e}")))?
            .into_proto()
    } else {
        // Protobuf format
        otlp::ExportMetricsServiceRequest::decode(body.as_ref())
            .map_err(|e| ServerError::BadRequest(format!("OTLP protobuf decode error: {e}")))?
    };

    let points = convert_otlp_request(&request)?;

    if points.is_empty() {
        return Ok(StatusCode::NO_CONTENT);
    }

    let count = points.len();

    crate::util::insert_batch_with_mode(
        &state.db,
        scope.as_deref(),
        points,
        state.write_timeout,
        backfill.mode(),
    )
    .await?;

    debug!(count, "wrote points via OTLP metrics");
    metrics::counter!("chronix_otlp_metrics_points_total").increment(count as u64);
    Ok(StatusCode::OK)
}

/// Convert an OTLP `ExportMetricsServiceRequest` to Chronix points.
fn convert_otlp_request(
    req: &otlp::ExportMetricsServiceRequest,
) -> Result<Vec<Point>, ServerError> {
    let mut points = Vec::new();

    for rm in &req.resource_metrics {
        // Extract resource attributes as base tags
        let resource_tags = rm
            .resource
            .as_ref()
            .map(|r| extract_attributes(&r.attributes))
            .unwrap_or_default();

        for sm in &rm.scope_metrics {
            // Optionally add scope name as tag
            let scope_tags = sm
                .scope
                .as_ref()
                .map(|s| {
                    let mut t = BTreeMap::new();
                    if !s.name.is_empty() {
                        t.insert("otel_scope_name".to_string(), s.name.clone());
                    }
                    if !s.version.is_empty() {
                        t.insert("otel_scope_version".to_string(), s.version.clone());
                    }
                    t
                })
                .unwrap_or_default();

            for metric in &sm.metrics {
                let measurement = &metric.name;

                if let Some(ref data) = metric.data {
                    convert_metric_data(
                        measurement,
                        data,
                        &resource_tags,
                        &scope_tags,
                        &mut points,
                    )?;
                }
            }
        }
    }

    Ok(points)
}

/// Convert a single metric's data points to Chronix points.
fn convert_metric_data(
    measurement: &str,
    data: &otlp::metric::Data,
    resource_tags: &BTreeMap<String, String>,
    scope_tags: &BTreeMap<String, String>,
    points: &mut Vec<Point>,
) -> Result<(), ServerError> {
    use otlp::metric::Data;

    match data {
        Data::Gauge(gauge) => {
            for dp in &gauge.data_points {
                let tags = merge_tags(resource_tags, scope_tags, &dp.attributes);
                let timestamp_ns = saturating_u64_to_i64(dp.time_unix_nano);
                let Some(value) = crate::util::storable_sample(number_value(dp)) else {
                    continue;
                };

                let key = SeriesKey::new(measurement, tags)
                    .map_err(|e| ServerError::BadRequest(e.to_string()))?;
                let mut fields = BTreeMap::new();
                fields.insert("gauge".to_string(), FieldValue::F64(value));

                let point = Point::new(key, fields, timestamp_ns)
                    .map_err(|e| ServerError::BadRequest(e.to_string()))?;
                points.push(point);
            }
        }
        Data::Sum(sum) => {
            for dp in &sum.data_points {
                let tags = merge_tags(resource_tags, scope_tags, &dp.attributes);
                let timestamp_ns = saturating_u64_to_i64(dp.time_unix_nano);
                let Some(value) = crate::util::storable_sample(number_value(dp)) else {
                    continue;
                };

                let key = SeriesKey::new(measurement, tags)
                    .map_err(|e| ServerError::BadRequest(e.to_string()))?;
                let mut fields = BTreeMap::new();
                fields.insert("value".to_string(), FieldValue::F64(value));

                let point = Point::new(key, fields, timestamp_ns)
                    .map_err(|e| ServerError::BadRequest(e.to_string()))?;
                points.push(point);
            }
        }
        Data::Histogram(histogram) => {
            for dp in &histogram.data_points {
                let tags = merge_tags(resource_tags, scope_tags, &dp.attributes);
                let timestamp_ns = saturating_u64_to_i64(dp.time_unix_nano);

                let key = SeriesKey::new(measurement, tags)
                    .map_err(|e| ServerError::BadRequest(e.to_string()))?;
                let mut fields = BTreeMap::new();
                let histogram = crate::wire::histogram::from_otlp_explicit(
                    &dp.explicit_bounds,
                    &dp.bucket_counts,
                    dp.count,
                    dp.sum,
                )
                .map_err(|e| {
                    ServerError::BadRequest(format!(
                        "metric {measurement}: explicit-bucket histogram: {e}"
                    ))
                })?;
                fields.insert(
                    chronix::promql::metric::VALUE_FIELD.to_string(),
                    FieldValue::Histogram(Box::new(histogram)),
                );

                let point = Point::new(key, fields, timestamp_ns)
                    .map_err(|e| ServerError::BadRequest(e.to_string()))?;
                points.push(point);
            }
        }
        Data::ExponentialHistogram(histogram) => {
            for dp in &histogram.data_points {
                let tags = merge_tags(resource_tags, scope_tags, &dp.attributes);
                let timestamp_ns = saturating_u64_to_i64(dp.time_unix_nano);

                let key = SeriesKey::new(measurement, tags)
                    .map_err(|e| ServerError::BadRequest(e.to_string()))?;

                fn buckets(
                    b: &Option<otlp::exponential_histogram_data_point::Buckets>,
                ) -> Option<crate::wire::histogram::OtlpBuckets<'_>> {
                    b.as_ref().map(|b| crate::wire::histogram::OtlpBuckets {
                        offset: b.offset,
                        counts: &b.bucket_counts,
                    })
                }
                let histogram = crate::wire::histogram::from_otlp_exponential(
                    dp.scale,
                    dp.zero_threshold,
                    dp.zero_count,
                    dp.count,
                    dp.sum,
                    buckets(&dp.positive),
                    buckets(&dp.negative),
                )
                .map_err(|e| {
                    ServerError::BadRequest(format!(
                        "metric {measurement}: exponential histogram: {e}"
                    ))
                })?;

                let mut fields = BTreeMap::new();
                fields.insert(
                    chronix::promql::metric::VALUE_FIELD.to_string(),
                    FieldValue::Histogram(Box::new(histogram)),
                );

                let point = Point::new(key, fields, timestamp_ns)
                    .map_err(|e| ServerError::BadRequest(e.to_string()))?;
                points.push(point);
            }
        }
        Data::Summary(summary) => {
            for dp in &summary.data_points {
                let tags = merge_tags(resource_tags, scope_tags, &dp.attributes);
                let timestamp_ns = saturating_u64_to_i64(dp.time_unix_nano);

                let key = SeriesKey::new(measurement, tags)
                    .map_err(|e| ServerError::BadRequest(e.to_string()))?;
                let mut fields = BTreeMap::new();
                fields.insert("count".to_string(), FieldValue::U64(dp.count));
                // OTLP leaves `sum` unset for a summary that recorded negative
                // events; an unset sum is not a sum of zero, so the field is
                // simply absent rather than fabricated.
                if let Some(sum) = dp.sum.and_then(crate::util::storable_sample) {
                    fields.insert("sum".to_string(), FieldValue::F64(sum));
                }

                // A quantile with no observations behind it is NaN, which is
                // the normal state of a freshly started summary.
                for qv in &dp.quantile_values {
                    if let Some(v) = crate::util::storable_sample(qv.value) {
                        fields.insert(format!("quantile_{}", qv.quantile), FieldValue::F64(v));
                    }
                }

                let point = Point::new(key, fields, timestamp_ns)
                    .map_err(|e| ServerError::BadRequest(e.to_string()))?;
                points.push(point);
            }
        }
    }

    Ok(())
}

/// Extract the numeric value from an OTLP `NumberDataPoint`.
fn number_value(dp: &otlp::NumberDataPoint) -> f64 {
    use otlp::number_data_point::Value;
    match &dp.value {
        Some(Value::AsDouble(v)) => *v,
        Some(Value::AsInt(v)) => *v as f64,
        None => 0.0,
    }
}

/// Extract string attributes from OTLP `KeyValue` list.
fn extract_attributes(attrs: &[otlp::KeyValue]) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::new();
    for kv in attrs {
        if let Some(ref val) = kv.value {
            let s = any_value_to_string(val);
            if !s.is_empty() {
                tags.insert(kv.key.clone(), s);
            }
        }
    }
    tags
}

/// Convert an OTLP `AnyValue` to a string for tag storage.
fn any_value_to_string(val: &otlp::AnyValue) -> String {
    use otlp::any_value::Value;
    match &val.value {
        Some(Value::StringValue(s)) => s.clone(),
        Some(Value::BoolValue(b)) => b.to_string(),
        Some(Value::IntValue(i)) => i.to_string(),
        Some(Value::DoubleValue(d)) => d.to_string(),
        Some(Value::BytesValue(b)) => hex_encode(b),
        _ => String::new(),
    }
}

/// Hex-encode bytes for attribute values.
///
/// OTLP bytes attributes are encoded as lowercase hex strings for
/// safe embedding in tags and field values.
fn hex_encode(data: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(data.len() * 2);
    for byte in data {
        write!(s, "{byte:02x}").ok();
    }
    s
}

/// Merge resource tags, scope tags, and data-point attributes.
fn merge_tags(
    resource: &BTreeMap<String, String>,
    scope: &BTreeMap<String, String>,
    attrs: &[otlp::KeyValue],
) -> BTreeMap<String, String> {
    let mut tags = resource.clone();
    tags.extend(scope.iter().map(|(k, v)| (k.clone(), v.clone())));
    tags.extend(extract_attributes(attrs));
    tags
}

/// Simplified JSON representation for OTLP JSON ingestion.
///
/// This supports the subset needed for OpenTelemetry Collector `otlphttp`
/// exporter JSON format. Full spec conformance defers to protobuf.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpJsonRequest {
    #[serde(default)]
    resource_metrics: Vec<OtlpJsonResourceMetrics>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpJsonResourceMetrics {
    #[serde(default)]
    resource: Option<OtlpJsonResource>,
    #[serde(default)]
    scope_metrics: Vec<OtlpJsonScopeMetrics>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpJsonResource {
    #[serde(default)]
    attributes: Vec<OtlpJsonKeyValue>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpJsonScopeMetrics {
    #[serde(default)]
    scope: Option<OtlpJsonScope>,
    #[serde(default)]
    metrics: Vec<OtlpJsonMetric>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpJsonScope {
    #[serde(default)]
    name: String,
    #[serde(default)]
    version: String,
}

/// Deserializers for protobuf-JSON 64-bit integers.
///
/// The protobuf JSON mapping encodes `int64`, `uint64`, `fixed64` and
/// `sfixed64` **as strings**, and that is what a conformant OTLP/JSON
/// exporter sends: the OpenTelemetry Collector's `otlphttp` exporter with
/// `encoding: json` writes `"timeUnixNano": "1700000000000000000"`. Reading
/// these as bare numbers rejected every real OTLP/JSON payload with
/// `400 Bad Request`, while the tree's own tests — which wrote numbers —
/// passed. A protocol is defined by its senders.
///
/// Numbers are still accepted, because the mapping says a parser must accept
/// both forms.
mod json_int {
    use serde::de::{Deserialize, Deserializer, Error, Unexpected};

    /// Either form of a JSON-encoded 64-bit integer.
    #[derive(serde::Deserialize)]
    #[serde(untagged)]
    enum StringOrNumber {
        String(String),
        Unsigned(u64),
        Signed(i64),
        Float(f64),
    }

    /// `u64` from a JSON string or number.
    pub fn u64<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
        match StringOrNumber::deserialize(d)? {
            StringOrNumber::String(s) => s
                .parse::<u64>()
                .map_err(|_| D::Error::invalid_value(Unexpected::Str(&s), &"a 64-bit integer")),
            StringOrNumber::Unsigned(v) => Ok(v),
            StringOrNumber::Signed(v) => u64::try_from(v)
                .map_err(|_| D::Error::invalid_value(Unexpected::Signed(v), &"a u64")),
            StringOrNumber::Float(v) => Ok(v as u64),
        }
    }

    /// Optional `i64`, absent when the key is missing or null.
    pub fn opt_i64<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Option<::std::primitive::i64>, D::Error> {
        Ok(Option::<StringOrNumber>::deserialize(d)?.map(|v| match v {
            StringOrNumber::String(s) => s.parse::<::std::primitive::i64>().unwrap_or_default(),
            StringOrNumber::Signed(x) => x,
            StringOrNumber::Unsigned(x) => {
                ::std::primitive::i64::try_from(x).unwrap_or(::std::primitive::i64::MAX)
            }
            StringOrNumber::Float(x) => x as ::std::primitive::i64,
        }))
    }

    /// A list of `u64`, each in either form.
    pub fn vec_u64<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Vec<::std::primitive::u64>, D::Error> {
        Ok(Vec::<StringOrNumber>::deserialize(d)?
            .into_iter()
            .map(|v| match v {
                StringOrNumber::String(s) => s.parse::<::std::primitive::u64>().unwrap_or_default(),
                StringOrNumber::Unsigned(x) => x,
                StringOrNumber::Signed(x) => ::std::primitive::u64::try_from(x).unwrap_or_default(),
                StringOrNumber::Float(x) => x as ::std::primitive::u64,
            })
            .collect())
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpJsonKeyValue {
    key: String,
    value: OtlpJsonAnyValue,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpJsonAnyValue {
    #[serde(default)]
    string_value: Option<String>,
    #[serde(default, deserialize_with = "json_int::opt_i64")]
    int_value: Option<i64>,
    #[serde(default)]
    double_value: Option<f64>,
    #[serde(default)]
    bool_value: Option<bool>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpJsonMetric {
    name: String,
    #[serde(default)]
    gauge: Option<OtlpJsonGauge>,
    #[serde(default)]
    sum: Option<OtlpJsonSum>,
    #[serde(default)]
    histogram: Option<OtlpJsonHistogram>,
    #[serde(default)]
    exponential_histogram: Option<OtlpJsonExponentialHistogram>,
    #[serde(default)]
    summary: Option<OtlpJsonSummary>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpJsonGauge {
    #[serde(default)]
    data_points: Vec<OtlpJsonNumberDataPoint>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpJsonSum {
    #[serde(default)]
    data_points: Vec<OtlpJsonNumberDataPoint>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpJsonNumberDataPoint {
    #[serde(default)]
    attributes: Vec<OtlpJsonKeyValue>,
    #[serde(default, deserialize_with = "json_int::u64")]
    time_unix_nano: u64,
    #[serde(default)]
    as_double: Option<f64>,
    #[serde(default, deserialize_with = "json_int::opt_i64")]
    as_int: Option<i64>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpJsonHistogram {
    #[serde(default)]
    data_points: Vec<OtlpJsonHistogramDataPoint>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpJsonHistogramDataPoint {
    #[serde(default)]
    attributes: Vec<OtlpJsonKeyValue>,
    #[serde(default, deserialize_with = "json_int::u64")]
    time_unix_nano: u64,
    #[serde(default, deserialize_with = "json_int::u64")]
    count: u64,
    #[serde(default)]
    sum: Option<f64>,
    #[serde(default, deserialize_with = "json_int::vec_u64")]
    bucket_counts: Vec<u64>,
    #[serde(default)]
    explicit_bounds: Vec<f64>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpJsonExponentialHistogram {
    #[serde(default)]
    data_points: Vec<OtlpJsonExponentialHistogramDataPoint>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpJsonExponentialHistogramDataPoint {
    #[serde(default)]
    attributes: Vec<OtlpJsonKeyValue>,
    #[serde(default, deserialize_with = "json_int::u64")]
    time_unix_nano: u64,
    #[serde(default, deserialize_with = "json_int::u64")]
    count: u64,
    #[serde(default)]
    sum: Option<f64>,
    #[serde(default)]
    scale: i32,
    #[serde(default, deserialize_with = "json_int::u64")]
    zero_count: u64,
    #[serde(default)]
    zero_threshold: f64,
    #[serde(default)]
    positive: Option<OtlpJsonBuckets>,
    #[serde(default)]
    negative: Option<OtlpJsonBuckets>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpJsonBuckets {
    #[serde(default)]
    offset: i32,
    #[serde(default, deserialize_with = "json_int::vec_u64")]
    bucket_counts: Vec<u64>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpJsonSummary {
    #[serde(default)]
    data_points: Vec<OtlpJsonSummaryDataPoint>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpJsonSummaryDataPoint {
    #[serde(default)]
    attributes: Vec<OtlpJsonKeyValue>,
    #[serde(default, deserialize_with = "json_int::u64")]
    time_unix_nano: u64,
    #[serde(default, deserialize_with = "json_int::u64")]
    count: u64,
    #[serde(default)]
    sum: Option<f64>,
    #[serde(default)]
    quantile_values: Vec<OtlpJsonValueAtQuantile>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpJsonValueAtQuantile {
    #[serde(default)]
    quantile: f64,
    #[serde(default)]
    value: f64,
}

impl OtlpJsonRequest {
    /// Convert JSON request to proto for unified handling.
    fn into_proto(self) -> otlp::ExportMetricsServiceRequest {
        let resource_metrics = self
            .resource_metrics
            .into_iter()
            .map(|rm| {
                let resource = rm.resource.map(|r| otlp::Resource {
                    attributes: r.attributes.into_iter().map(json_kv_to_proto).collect(),
                    dropped_attributes_count: 0,
                });

                let scope_metrics = rm
                    .scope_metrics
                    .into_iter()
                    .map(|sm| {
                        let scope = sm.scope.map(|s| otlp::InstrumentationScope {
                            name: s.name,
                            version: s.version,
                            attributes: vec![],
                        });

                        let metrics = sm
                            .metrics
                            .into_iter()
                            .map(|m| {
                                let data = if let Some(gauge) = m.gauge {
                                    Some(otlp::metric::Data::Gauge(otlp::Gauge {
                                        data_points: gauge
                                            .data_points
                                            .into_iter()
                                            .map(json_number_dp_to_proto)
                                            .collect(),
                                    }))
                                } else if let Some(sum) = m.sum {
                                    Some(otlp::metric::Data::Sum(otlp::Sum {
                                        data_points: sum
                                            .data_points
                                            .into_iter()
                                            .map(json_number_dp_to_proto)
                                            .collect(),
                                        aggregation_temporality: 0,
                                        is_monotonic: false,
                                    }))
                                } else if let Some(hist) = m.histogram {
                                    Some(otlp::metric::Data::Histogram(otlp::Histogram {
                                        data_points: hist
                                            .data_points
                                            .into_iter()
                                            .map(json_histogram_dp_to_proto)
                                            .collect(),
                                        aggregation_temporality: 0,
                                    }))
                                } else if let Some(exp) = m.exponential_histogram {
                                    Some(otlp::metric::Data::ExponentialHistogram(
                                        otlp::ExponentialHistogram {
                                            data_points: exp
                                                .data_points
                                                .into_iter()
                                                .map(json_exponential_histogram_dp_to_proto)
                                                .collect(),
                                            aggregation_temporality: 0,
                                        },
                                    ))
                                } else if let Some(summary) = m.summary {
                                    Some(otlp::metric::Data::Summary(otlp::Summary {
                                        data_points: summary
                                            .data_points
                                            .into_iter()
                                            .map(json_summary_dp_to_proto)
                                            .collect(),
                                    }))
                                } else {
                                    None
                                };

                                otlp::Metric {
                                    name: m.name,
                                    description: String::new(),
                                    unit: String::new(),
                                    data,
                                }
                            })
                            .collect();

                        otlp::ScopeMetrics { scope, metrics }
                    })
                    .collect();

                otlp::ResourceMetrics {
                    resource,
                    scope_metrics,
                }
            })
            .collect();

        otlp::ExportMetricsServiceRequest { resource_metrics }
    }
}

fn json_kv_to_proto(kv: OtlpJsonKeyValue) -> otlp::KeyValue {
    let value = if let Some(s) = kv.value.string_value {
        Some(otlp::any_value::Value::StringValue(s))
    } else if let Some(i) = kv.value.int_value {
        Some(otlp::any_value::Value::IntValue(i))
    } else if let Some(d) = kv.value.double_value {
        Some(otlp::any_value::Value::DoubleValue(d))
    } else {
        kv.value.bool_value.map(otlp::any_value::Value::BoolValue)
    };

    otlp::KeyValue {
        key: kv.key,
        value: Some(otlp::AnyValue { value }),
    }
}

fn json_histogram_dp_to_proto(dp: OtlpJsonHistogramDataPoint) -> otlp::HistogramDataPoint {
    otlp::HistogramDataPoint {
        attributes: dp.attributes.into_iter().map(json_kv_to_proto).collect(),
        start_time_unix_nano: 0,
        time_unix_nano: dp.time_unix_nano,
        count: dp.count,
        sum: dp.sum,
        bucket_counts: dp.bucket_counts,
        explicit_bounds: dp.explicit_bounds,
    }
}

fn json_exponential_histogram_dp_to_proto(
    dp: OtlpJsonExponentialHistogramDataPoint,
) -> otlp::ExponentialHistogramDataPoint {
    let buckets = |b: Option<OtlpJsonBuckets>| {
        b.map(|b| otlp::exponential_histogram_data_point::Buckets {
            offset: b.offset,
            bucket_counts: b.bucket_counts,
        })
    };
    otlp::ExponentialHistogramDataPoint {
        attributes: dp.attributes.into_iter().map(json_kv_to_proto).collect(),
        start_time_unix_nano: 0,
        time_unix_nano: dp.time_unix_nano,
        count: dp.count,
        sum: dp.sum,
        scale: dp.scale,
        zero_count: dp.zero_count,
        positive: buckets(dp.positive),
        negative: buckets(dp.negative),
        flags: 0,
        min: None,
        max: None,
        zero_threshold: dp.zero_threshold,
    }
}

fn json_summary_dp_to_proto(dp: OtlpJsonSummaryDataPoint) -> otlp::SummaryDataPoint {
    otlp::SummaryDataPoint {
        attributes: dp.attributes.into_iter().map(json_kv_to_proto).collect(),
        start_time_unix_nano: 0,
        time_unix_nano: dp.time_unix_nano,
        count: dp.count,
        sum: dp.sum,
        quantile_values: dp
            .quantile_values
            .into_iter()
            .map(|q| otlp::summary_data_point::ValueAtQuantile {
                quantile: q.quantile,
                value: q.value,
            })
            .collect(),
    }
}

fn json_number_dp_to_proto(dp: OtlpJsonNumberDataPoint) -> otlp::NumberDataPoint {
    let value = if let Some(d) = dp.as_double {
        Some(otlp::number_data_point::Value::AsDouble(d))
    } else {
        dp.as_int.map(otlp::number_data_point::Value::AsInt)
    };

    otlp::NumberDataPoint {
        attributes: dp.attributes.into_iter().map(json_kv_to_proto).collect(),
        start_time_unix_nano: 0,
        time_unix_nano: dp.time_unix_nano,
        value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_gauge_request(
        metric_name: &str,
        value: f64,
        timestamp_ns: u64,
    ) -> otlp::ExportMetricsServiceRequest {
        otlp::ExportMetricsServiceRequest {
            resource_metrics: vec![otlp::ResourceMetrics {
                resource: Some(otlp::Resource {
                    attributes: vec![otlp::KeyValue {
                        key: "service.name".into(),
                        value: Some(otlp::AnyValue {
                            value: Some(otlp::any_value::Value::StringValue("test-svc".into())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![otlp::ScopeMetrics {
                    scope: Some(otlp::InstrumentationScope {
                        name: "test-lib".into(),
                        version: "1.0".into(),
                        attributes: vec![],
                    }),
                    metrics: vec![otlp::Metric {
                        name: metric_name.into(),
                        description: String::new(),
                        unit: String::new(),
                        data: Some(otlp::metric::Data::Gauge(otlp::Gauge {
                            data_points: vec![otlp::NumberDataPoint {
                                attributes: vec![otlp::KeyValue {
                                    key: "host".into(),
                                    value: Some(otlp::AnyValue {
                                        value: Some(otlp::any_value::Value::StringValue(
                                            "srv1".into(),
                                        )),
                                    }),
                                }],
                                start_time_unix_nano: 0,
                                time_unix_nano: timestamp_ns,
                                value: Some(otlp::number_data_point::Value::AsDouble(value)),
                            }],
                        })),
                    }],
                }],
            }],
        }
    }

    #[test]
    fn convert_gauge() {
        let req = make_gauge_request("cpu_usage", 72.5, 1_000_000_000);
        let points = convert_otlp_request(&req).unwrap();

        assert_eq!(points.len(), 1);
        assert_eq!(points[0].series_key().measurement(), "cpu_usage");
        assert_eq!(points[0].timestamp(), 1_000_000_000);
        assert!(matches!(
            points[0].field("gauge"),
            Some(FieldValue::F64(v)) if (*v - 72.5).abs() < f64::EPSILON
        ));
        // Resource attribute → tag
        assert_eq!(points[0].tag("service.name"), Some("test-svc"));
        // Data point attribute → tag
        assert_eq!(points[0].tag("host"), Some("srv1"));
        // Scope tags
        assert_eq!(points[0].tag("otel_scope_name"), Some("test-lib"));
    }

    #[test]
    fn convert_sum() {
        let req = otlp::ExportMetricsServiceRequest {
            resource_metrics: vec![otlp::ResourceMetrics {
                resource: None,
                scope_metrics: vec![otlp::ScopeMetrics {
                    scope: None,
                    metrics: vec![otlp::Metric {
                        name: "requests_total".into(),
                        description: String::new(),
                        unit: String::new(),
                        data: Some(otlp::metric::Data::Sum(otlp::Sum {
                            data_points: vec![otlp::NumberDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 0,
                                time_unix_nano: 2_000_000_000,
                                value: Some(otlp::number_data_point::Value::AsInt(150)),
                            }],
                            aggregation_temporality: 2,
                            is_monotonic: true,
                        })),
                    }],
                }],
            }],
        };

        let points = convert_otlp_request(&req).unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].series_key().measurement(), "requests_total");
        assert!(matches!(
            points[0].field("value"),
            Some(FieldValue::F64(v)) if (*v - 150.0).abs() < f64::EPSILON
        ));
    }

    #[test]
    fn convert_histogram() {
        let req = otlp::ExportMetricsServiceRequest {
            resource_metrics: vec![otlp::ResourceMetrics {
                resource: None,
                scope_metrics: vec![otlp::ScopeMetrics {
                    scope: None,
                    metrics: vec![otlp::Metric {
                        name: "http_duration".into(),
                        description: String::new(),
                        unit: String::new(),
                        data: Some(otlp::metric::Data::Histogram(otlp::Histogram {
                            data_points: vec![otlp::HistogramDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 0,
                                time_unix_nano: 3_000_000_000,
                                count: 100,
                                sum: Some(50.5),
                                bucket_counts: vec![10, 40, 30, 20],
                                explicit_bounds: vec![0.01, 0.1, 1.0],
                            }],
                            aggregation_temporality: 2,
                        })),
                    }],
                }],
            }],
        };

        let points = convert_otlp_request(&req).unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].series_key().measurement(), "http_duration");

        // One histogram column under `value`, not one field per boundary.
        // The metric therefore keeps its own name — `http_duration`, not
        // `http_duration_bucket_0.01` and six siblings — and answers
        // `histogram_quantile` the way a Prometheus user expects.
        let Some(FieldValue::Histogram(h)) = points[0].field("value") else {
            panic!("expected a histogram column, got {:?}", points[0].fields());
        };
        assert_eq!(h.schema, chronix_core::histogram::CUSTOM_BUCKETS_SCHEMA);
        assert_eq!(h.custom_values, vec![0.01, 0.1, 1.0]);
        assert_eq!(h.count, 100.0);
        assert!((h.sum - 50.5).abs() < f64::EPSILON);
        // 10 + 40 + 30 + 20, the last being the +Inf overflow.
        assert_eq!(h.positive.len(), 4);
        assert!((h.fraction(f64::NEG_INFINITY, f64::INFINITY) - 1.0).abs() < 1e-12);
    }

    /// An OTLP exponential histogram is stored, not dropped.
    ///
    /// Before the `ExponentialHistogram` arm existed the message decoded to a
    /// `data` of `None` and the whole data point vanished without a word — an
    /// OTel SDK configured for base-2 histograms wrote nothing and got 200 OK.
    #[test]
    fn convert_exponential_histogram() {
        let req = otlp::ExportMetricsServiceRequest {
            resource_metrics: vec![otlp::ResourceMetrics {
                resource: None,
                scope_metrics: vec![otlp::ScopeMetrics {
                    scope: None,
                    metrics: vec![otlp::Metric {
                        name: "rpc_latency".into(),
                        description: String::new(),
                        unit: String::new(),
                        data: Some(otlp::metric::Data::ExponentialHistogram(
                            otlp::ExponentialHistogram {
                                data_points: vec![otlp::ExponentialHistogramDataPoint {
                                    attributes: vec![],
                                    start_time_unix_nano: 0,
                                    time_unix_nano: 5_000_000_000,
                                    count: 4,
                                    sum: Some(12.0),
                                    scale: 0,
                                    zero_count: 1,
                                    // OTLP index 1 covers (2, 4]; index 2 covers (4, 8].
                                    positive: Some(
                                        otlp::exponential_histogram_data_point::Buckets {
                                            offset: 1,
                                            bucket_counts: vec![2, 1],
                                        },
                                    ),
                                    negative: None,
                                    flags: 0,
                                    min: None,
                                    max: None,
                                    zero_threshold: 0.5,
                                }],
                                aggregation_temporality: 2,
                            },
                        )),
                    }],
                }],
            }],
        };

        let points = convert_otlp_request(&req).unwrap();
        assert_eq!(points.len(), 1);
        let Some(FieldValue::Histogram(h)) = points[0].field("value") else {
            panic!("expected a histogram column");
        };
        assert_eq!(h.schema, 0);
        assert_eq!(h.zero_count, 1.0);
        assert_eq!(h.zero_threshold, 0.5);
        assert_eq!(h.count, 4.0);
        // OTLP index 1 is Prometheus index 2, whose bounds are (2, 4].
        assert_eq!(h.positive[0].index, 2);
        assert_eq!(h.bucket_bounds(2, false), (2.0, 4.0));
        assert_eq!(h.positive[1].index, 3);
        assert_eq!(h.bucket_bounds(3, false), (4.0, 8.0));
    }

    #[test]
    fn convert_summary() {
        let req = otlp::ExportMetricsServiceRequest {
            resource_metrics: vec![otlp::ResourceMetrics {
                resource: None,
                scope_metrics: vec![otlp::ScopeMetrics {
                    scope: None,
                    metrics: vec![otlp::Metric {
                        name: "rpc_duration".into(),
                        description: String::new(),
                        unit: String::new(),
                        data: Some(otlp::metric::Data::Summary(otlp::Summary {
                            data_points: vec![otlp::SummaryDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 0,
                                time_unix_nano: 4_000_000_000,
                                count: 200,
                                sum: Some(100.0),
                                quantile_values: vec![
                                    otlp::summary_data_point::ValueAtQuantile {
                                        quantile: 0.5,
                                        value: 0.4,
                                    },
                                    otlp::summary_data_point::ValueAtQuantile {
                                        quantile: 0.99,
                                        value: 1.2,
                                    },
                                ],
                            }],
                        })),
                    }],
                }],
            }],
        };

        let points = convert_otlp_request(&req).unwrap();
        assert_eq!(points.len(), 1);
        assert!(matches!(
            points[0].field("quantile_0.5"),
            Some(FieldValue::F64(v)) if (*v - 0.4).abs() < f64::EPSILON
        ));
        assert!(matches!(
            points[0].field("quantile_0.99"),
            Some(FieldValue::F64(v)) if (*v - 1.2).abs() < f64::EPSILON
        ));
    }

    #[test]
    fn convert_json_gauge() {
        let json = r#"{
            "resourceMetrics": [{
                "resource": {
                    "attributes": [{
                        "key": "service.name",
                        "value": {"stringValue": "my-app"}
                    }]
                },
                "scopeMetrics": [{
                    "scope": {"name": "my-lib", "version": "1.0"},
                    "metrics": [{
                        "name": "cpu_temp",
                        "gauge": {
                            "dataPoints": [{
                                "attributes": [],
                                "timeUnixNano": 5000000000,
                                "asDouble": 65.3
                            }]
                        }
                    }]
                }]
            }]
        }"#;

        let json_req: OtlpJsonRequest = serde_json::from_str(json).unwrap();
        let proto = json_req.into_proto();
        let points = convert_otlp_request(&proto).unwrap();

        assert_eq!(points.len(), 1);
        assert_eq!(points[0].series_key().measurement(), "cpu_temp");
        assert_eq!(points[0].tag("service.name"), Some("my-app"));
        assert!(matches!(
            points[0].field("gauge"),
            Some(FieldValue::F64(v)) if (*v - 65.3).abs() < f64::EPSILON
        ));
    }

    #[test]
    fn empty_request_produces_no_points() {
        let req = otlp::ExportMetricsServiceRequest {
            resource_metrics: vec![],
        };
        let points = convert_otlp_request(&req).unwrap();
        assert!(points.is_empty());
    }
}
