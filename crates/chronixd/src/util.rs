//! Shared utilities used across the HTTP, gRPC, Flight SQL, and
//! connector layers.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chronix::Chronix;
use chronix_core::{ColumnType, FieldValue, Point};

use crate::error::ServerError;

/// Stamp `namespace` on every point and insert the batch under a deadline.
///
/// **The server's only write path.** Every ingestion surface — REST JSON,
/// Line Protocol, OTLP, Prometheus remote write, gRPC, Flight SQL `DoPut`,
/// and the Kafka and MQTT connectors — goes through here, which is what makes
/// the namespace stamp an invariant rather than something each handler has to
/// remember (D45, R19). The namespace is applied, not merged.
///
/// Prevents a stuck downstream write from exhausting the tokio thread
/// pool. A zero timeout disables the deadline and waits indefinitely.
///
/// # Errors
/// - `ServerError::BadRequest` if a point cannot carry the namespace tag.
/// - `ServerError::WriteTimeout` if the operation exceeds `timeout`.
/// - `ServerError::Internal` if the blocking task panics.
/// - `ServerError::Db` (or cardinality-specific variants) on database errors.
pub async fn insert_with_timeout(
    db: &Arc<Chronix>,
    namespace: Option<&str>,
    mut points: Vec<Point>,
    timeout: Duration,
) -> Result<(), ServerError> {
    crate::namespace::scope_points(namespace, &mut points)?;
    let db = db.clone();
    let future = tokio::task::spawn_blocking(move || db.insert_batch(&points));

    let join_result = if timeout.is_zero() {
        future.await
    } else {
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| ServerError::WriteTimeout(timeout))?
    };

    let insert_result = join_result
        .map_err(|e| ServerError::Internal(e.to_string()))?
        .map_err(|e| match &e {
            chronix::DbError::CardinalityExceeded { .. } => {
                ServerError::Backpressure(e.to_string())
            }
            _ => ServerError::Db(e),
        })?;

    if insert_result.is_partial() {
        tracing::warn!(
            wal_committed = insert_result.wal_committed,
            memtable_inserted = insert_result.memtable_inserted,
            memtable_errors = insert_result.errors.len(),
            "Partial insert: all points are WAL-durable but some memtable insertions failed"
        );
    }

    Ok(())
}

/// Current wall-clock time as nanoseconds since the Unix epoch.
///
/// Uses `i64` which supports dates up to approximately year 2262.
///
/// # Errors
/// Returns `ServerError::Internal` if the system clock is before the Unix epoch.
pub fn now_nanos() -> Result<i64, ServerError> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| ServerError::Internal("system clock before Unix epoch".into()))?
        .as_nanos();
    i64::try_from(nanos).map_err(|_| {
        ServerError::Internal("system clock nanoseconds overflow i64 (past year 2262)".into())
    })
}

/// Convert a [`ColumnType`] to a human-readable wire format string.
pub fn column_type_to_str(ct: ColumnType) -> &'static str {
    match ct {
        ColumnType::F64 => "float64",
        ColumnType::I64 => "int64",
        ColumnType::U64 => "uint64",
        ColumnType::Bool => "bool",
        ColumnType::String => "string",
        ColumnType::Timestamp => "timestamp",
    }
}

/// Convert a [`ColumnType`] to an Arrow [`DataType`](arrow::datatypes::DataType).
pub fn column_type_to_arrow(ct: ColumnType) -> arrow::datatypes::DataType {
    match ct {
        ColumnType::F64 => arrow::datatypes::DataType::Float64,
        ColumnType::I64 => arrow::datatypes::DataType::Int64,
        ColumnType::U64 => arrow::datatypes::DataType::UInt64,
        ColumnType::Bool => arrow::datatypes::DataType::Boolean,
        ColumnType::String => arrow::datatypes::DataType::Utf8,
        ColumnType::Timestamp => arrow::datatypes::DataType::Int64,
    }
}

/// Parse a JSON object with `tags`, `fields`, and optional `timestamp`
/// into Chronix field values.
///
/// This is the shared parsing logic used by Kafka, MQTT, and REST
/// ingestion paths. The `extra_tags` parameter allows callers to inject
/// tags derived from the source (e.g., MQTT topic segments).
///
/// # Expected JSON format
///
/// ```json
/// {
///   "tags": { "host": "srv1" },
///   "fields": { "cpu": 87.5, "mem": 42 },
///   "timestamp": 1609459200000000000
/// }
/// ```
pub fn parse_json_point(
    measurement: &str,
    text: &str,
    extra_tags: BTreeMap<String, String>,
) -> Result<Vec<chronix_core::Point>, ServerError> {
    let v: serde_json::Value =
        serde_json::from_str(text).map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let mut tags = extra_tags;

    // Merge explicit tags from payload
    if let Some(t) = v.get("tags") {
        let payload_tags: BTreeMap<String, String> = serde_json::from_value(t.clone())
            .map_err(|e| ServerError::BadRequest(format!("invalid tags: {e}")))?;
        tags.extend(payload_tags);
    }

    let fields_val = v
        .get("fields")
        .ok_or_else(|| ServerError::BadRequest("missing 'fields' in JSON".into()))?;
    let fields_map: serde_json::Map<String, serde_json::Value> = match fields_val {
        serde_json::Value::Object(m) => m.clone(),
        _ => {
            return Err(ServerError::BadRequest(
                "'fields' must be a JSON object".into(),
            ))
        }
    };

    let mut fields = BTreeMap::new();
    for (k, val) in fields_map {
        let fv = json_value_to_field(&k, val)?;
        fields.insert(k, fv);
    }

    let timestamp = match v.get("timestamp").and_then(serde_json::Value::as_i64) {
        Some(ts) => ts,
        None => now_nanos()?,
    };

    let key = chronix_core::SeriesKey::new(measurement, tags)
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;
    let point = chronix_core::Point::new(key, fields, timestamp)
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    Ok(vec![point])
}

/// Convert a single JSON value to a [`FieldValue`].
///
/// Integer types are preferred over floats when the JSON number has no
/// fractional part.
pub fn json_value_to_field(key: &str, val: serde_json::Value) -> Result<FieldValue, ServerError> {
    match val {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(FieldValue::I64(i))
            } else if let Some(u) = n.as_u64() {
                Ok(FieldValue::U64(u))
            } else if let Some(f) = n.as_f64() {
                Ok(FieldValue::F64(f))
            } else {
                Err(ServerError::BadRequest(format!(
                    "unsupported number for field '{key}'"
                )))
            }
        }
        serde_json::Value::Bool(b) => Ok(FieldValue::Bool(b)),
        serde_json::Value::String(s) => Ok(FieldValue::String(s)),
        _ => Err(ServerError::BadRequest(format!(
            "unsupported value type for field '{key}'"
        ))),
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_nanos_positive() {
        let ts = now_nanos().unwrap();
        assert!(ts > 0);
    }

    #[test]
    fn column_type_to_str_all_variants() {
        assert_eq!(column_type_to_str(ColumnType::F64), "float64");
        assert_eq!(column_type_to_str(ColumnType::I64), "int64");
        assert_eq!(column_type_to_str(ColumnType::U64), "uint64");
        assert_eq!(column_type_to_str(ColumnType::Bool), "bool");
        assert_eq!(column_type_to_str(ColumnType::String), "string");
        assert_eq!(column_type_to_str(ColumnType::Timestamp), "timestamp");
    }

    #[test]
    fn column_type_to_arrow_all_variants() {
        use arrow::datatypes::DataType;
        assert_eq!(column_type_to_arrow(ColumnType::F64), DataType::Float64);
        assert_eq!(column_type_to_arrow(ColumnType::I64), DataType::Int64);
        assert_eq!(column_type_to_arrow(ColumnType::U64), DataType::UInt64);
        assert_eq!(column_type_to_arrow(ColumnType::Bool), DataType::Boolean);
        assert_eq!(column_type_to_arrow(ColumnType::String), DataType::Utf8);
        assert_eq!(column_type_to_arrow(ColumnType::Timestamp), DataType::Int64);
    }

    #[test]
    fn json_value_to_field_integer() {
        let fv = json_value_to_field("x", serde_json::json!(42)).unwrap();
        assert!(matches!(fv, FieldValue::I64(42)));
    }

    #[test]
    fn json_value_to_field_float() {
        let fv = json_value_to_field("x", serde_json::json!(std::f64::consts::PI)).unwrap();
        assert!(matches!(fv, FieldValue::F64(_)));
    }

    #[test]
    fn json_value_to_field_bool() {
        let fv = json_value_to_field("x", serde_json::json!(true)).unwrap();
        assert!(matches!(fv, FieldValue::Bool(true)));
    }

    #[test]
    fn json_value_to_field_string() {
        let fv = json_value_to_field("x", serde_json::json!("hello")).unwrap();
        assert!(matches!(fv, FieldValue::String(_)));
    }

    #[test]
    fn json_value_to_field_null_rejected() {
        let err = json_value_to_field("x", serde_json::Value::Null).unwrap_err();
        assert!(err.to_string().contains("unsupported"));
    }

    #[test]
    fn parse_json_point_basic() {
        let json = r#"{"tags":{"host":"srv1"},"fields":{"cpu":87.5},"timestamp":1000}"#;
        let points = parse_json_point("metrics", json, BTreeMap::new()).unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].series_key().measurement(), "metrics");
        assert_eq!(points[0].timestamp(), 1000);
    }

    #[test]
    fn parse_json_point_extra_tags_merged() {
        let json = r#"{"tags":{"host":"srv1"},"fields":{"v":1},"timestamp":1000}"#;
        let mut extra = BTreeMap::new();
        extra.insert("region".to_string(), "us-east".to_string());
        let points = parse_json_point("m", json, extra).unwrap();

        assert_eq!(points[0].tag("host"), Some("srv1"));
        assert_eq!(points[0].tag("region"), Some("us-east"));
    }

    #[test]
    fn parse_json_point_missing_fields_error() {
        let json = r#"{"tags":{"host":"a"}}"#;
        let err = parse_json_point("m", json, BTreeMap::new()).unwrap_err();
        assert!(err.to_string().contains("fields"), "{err}");
    }

    #[test]
    fn parse_json_point_integer_preserved() {
        let json = r#"{"fields":{"count":42,"rate":1.5},"timestamp":1000}"#;
        let points = parse_json_point("m", json, BTreeMap::new()).unwrap();
        assert!(matches!(
            points[0].field("count"),
            Some(FieldValue::I64(42))
        ));
        assert!(matches!(points[0].field("rate"), Some(FieldValue::F64(_))));
    }
}

// ── Non-finite samples on the wire ─────────────────────────────────────

/// Metric name for samples a wire protocol handed us that the engine cannot
/// store.
pub const SKIPPED_SAMPLES_METRIC: &str = "chronix_wire_non_finite_samples_skipped_total";

/// Decide what to do with a sample value arriving over a wire protocol.
///
/// Returns `Some(value)` for a value the engine can store, and `None` for one
/// it cannot — `NaN`, `±Inf` — after counting it.
///
/// **Why this is a skip and not a rejection.** `Point::new` refuses non-finite
/// `f64` on purpose: a `NaN` in storage poisons every aggregate that reads it.
/// But both of the protocols chronix advertises as drop-in replacements emit
/// non-finite values as a matter of routine:
///
/// Prometheus remote write signals the end of a series' life with a
///   **staleness marker**, which on the wire is a specific `NaN` payload. It
///   is sent every time a scrape target disappears.
/// OTLP summary quantiles are `NaN` when nothing has been observed yet, and
///   a gauge is free to report `NaN` or `±Inf`.
///
/// Propagating the engine's rejection to the request turned each of those into
/// a `400` for the **whole batch**. Prometheus does not drop a batch it cannot
/// deliver — it retries it, forever — so the first target to go away stalled
/// the remote-write queue and stopped ingestion entirely, from a valid
/// message. A protocol surface has to be judged by what real senders actually
/// send (D2), and dropping the sample it cannot represent while accepting the
/// rest is what every other receiver does.
#[must_use]
pub fn storable_sample(value: f64) -> Option<f64> {
    if value.is_finite() {
        return Some(value);
    }
    metrics::counter!(SKIPPED_SAMPLES_METRIC).increment(1);
    None
}

#[cfg(test)]
mod non_finite_tests {
    use super::storable_sample;

    #[test]
    fn finite_values_pass_through() {
        assert_eq!(storable_sample(1.5), Some(1.5));
        assert_eq!(storable_sample(0.0), Some(0.0));
        assert_eq!(storable_sample(-0.0), Some(-0.0));
    }

    #[test]
    fn non_finite_values_are_skipped() {
        assert_eq!(storable_sample(f64::NAN), None);
        assert_eq!(storable_sample(f64::INFINITY), None);
        assert_eq!(storable_sample(f64::NEG_INFINITY), None);
        // Prometheus's staleness marker is a NaN with a specific payload.
        assert_eq!(
            storable_sample(f64::from_bits(0x7ff0_0000_0000_0002)),
            None,
            "a Prometheus staleness marker must be skipped, not rejected"
        );
    }
}
