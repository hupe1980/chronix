//! Shared utilities used across the HTTP, gRPC, Flight SQL, and
//! connector layers.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chronix::Chronix;
use chronix_core::{ColumnType, FieldValue, Point};

use crate::error::ServerError;

/// Whether a batch is live ingestion or an import of history.
///
/// An **explicit opt-in**, never an automatic fallback for a late point: the
/// out-of-order window is what bounds the number of open memtables, and
/// routing anything late to the backfill path would let one client with a
/// wrong clock open a shard per hour of history. The caller knows whether it
/// is importing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteMode {
    /// Live ingestion, held to the out-of-order window.
    #[default]
    Live,
    /// Importing history: the window is not enforced. Everything else —
    /// admission, the cardinality budget, the schema, the future-timestamp
    /// bound — is unchanged.
    Backfill,
}

impl WriteMode {
    /// The mode a request's `?backfill=` parameter asks for.
    #[must_use]
    pub fn from_flag(backfill: bool) -> Self {
        if backfill {
            Self::Backfill
        } else {
            Self::Live
        }
    }
}

/// The `?backfill=` query parameter, on every network write path.
///
/// Its own extractor rather than a field on each handler's body type: line
/// protocol and OTLP have no JSON body to put it in, and one spelling across
/// six surfaces is the whole point.
///
/// Deliberately **not** `deny_unknown_fields`, unlike the request bodies: a
/// query string is decorated by proxies and agents, and a remote-write sender
/// treats a 400 as final, so refusing an unread parameter would drop data.
/// The *value* is still checked — `?backfill=yes` is a 400, not a silent
/// `false`.
#[derive(Debug, Clone, Copy, Default, serde::Deserialize)]
pub struct BackfillParam {
    /// `true` writes outside the out-of-order window.
    #[serde(default)]
    pub backfill: bool,
}

impl BackfillParam {
    /// The write mode this parameter asks for.
    #[must_use]
    pub fn mode(self) -> WriteMode {
        WriteMode::from_flag(self.backfill)
    }
}

/// The reasons a write can be refused, and the label values
/// `chronix_write_errors_total` carries.
///
/// Listed once so [`register_write_metrics`] can publish each at zero: a
/// counter that has never fired is **absent** from a scrape, and absent reads
/// on a dashboard as "No data" — the same thing an operator sees when a
/// metric does not exist at all. Prometheus's own guidance is to initialise
/// every label value known in advance.
pub const WRITE_ERROR_REASONS: [&str; 4] = ["timeout", "panic", "rejected", "storage_full"];

/// Publish the write path's counters at zero, at startup.
///
/// Only the write path: every `chronixd` has one, so these series are true on
/// every deployment. Counters belonging to a subsystem that may not be
/// running — the object-store cache, the signal channels — are deliberately
/// **not** pre-registered, because "0 cache hits" on a server with no cold
/// tier claims a subsystem that is not there. Absent is the honest answer for
/// those; zero is the honest answer for these.
pub fn register_write_metrics() {
    for reason in WRITE_ERROR_REASONS {
        metrics::counter!("chronix_write_errors_total", "reason" => reason).increment(0);
    }
    metrics::counter!("chronix_points_written_total").increment(0);
    metrics::counter!("chronix_backfill_points_total").increment(0);
}

/// Stamp `namespace` on every point and insert the batch under a deadline.
///
/// **The server's only write path.** Every ingestion surface — REST JSON,
/// Line Protocol, OTLP, Prometheus remote write, gRPC, Flight SQL `DoPut`,
/// and the Kafka and MQTT connectors — goes through here, which is what makes
/// the namespace stamp an invariant rather than something each handler has to
/// remember. The namespace is applied, not merged.
///
/// # The deadline bounds the wait, not the write
///
/// `timeout` stops this task waiting; it does **not** stop the write. The
/// insert runs on a blocking thread, a blocking task cannot be cancelled, and
/// a durable write must not be abandoned half-way in any case — so when the
/// deadline fires the write is still running and will very likely land. The
/// only honest answer is that the outcome is *unknown*, which is what
/// [`ServerError::WriteTimeout`] says; a retry is safe because a point is
/// identified by its series and its timestamp.
///
/// What actually bounds concurrent writes is admission control in the engine:
/// a memtable at capacity refuses the batch with `DbError::TransientOverload`,
/// which reaches the client as `503 BACKPRESSURE` with a `Retry-After`. This
/// deadline used to claim it "prevents a stuck downstream write from
/// exhausting the tokio thread pool", which it never did — the thread is held
/// either way.
///
/// A zero timeout disables the deadline and waits indefinitely.
///
/// # Errors
/// - `ServerError::BadRequest` if a point cannot carry the namespace tag.
/// - `ServerError::WriteTimeout` if the wait exceeds `timeout`.
/// - `ServerError::Internal` if the blocking task panics.
/// - `ServerError::Db` (or cardinality-specific variants) on database errors.
pub async fn insert_with_timeout(
    db: &Arc<Chronix>,
    namespace: Option<&str>,
    points: Vec<Point>,
    timeout: Duration,
) -> Result<(), ServerError> {
    insert_batch_with_mode(db, namespace, points, timeout, WriteMode::Live).await
}

/// [`insert_with_timeout`], with the write mode chosen by the caller.
///
/// # Errors
///
/// As [`insert_with_timeout`].
pub async fn insert_batch_with_mode(
    db: &Arc<Chronix>,
    namespace: Option<&str>,
    mut points: Vec<Point>,
    timeout: Duration,
    mode: WriteMode,
) -> Result<(), ServerError> {
    crate::namespace::scope_points(namespace, &mut points)?;
    let db = db.clone();
    let batch_size = points.len();
    // Every protocol funnels through here, so this is the one place a
    // total write rate can be counted. The per-protocol counters answer
    // "which client", not "how much is this server taking".
    #[allow(clippy::cast_precision_loss)]
    metrics::histogram!("chronix_batch_size").record(batch_size as f64);
    let started = std::time::Instant::now();

    let future = tokio::task::spawn_blocking(move || match mode {
        WriteMode::Live => db.insert_batch(&points),
        WriteMode::Backfill => db.backfill(&points),
    });

    let join_result = if timeout.is_zero() {
        future.await
    } else {
        tokio::time::timeout(timeout, future).await.map_err(|_| {
            metrics::counter!("chronix_write_errors_total", "reason" => "timeout").increment(1);
            ServerError::WriteTimeout(timeout)
        })?
    };
    metrics::histogram!("chronix_write_duration_seconds").record(started.elapsed().as_secs_f64());

    // Every database error keeps its identity here and is classified once, in
    // `error::classify_db` — a cardinality rejection as a permanent `400`, a
    // full memtable as a retryable `503`, a poisoned WAL as a `503` an
    // operator has to clear. Flattening them here is how a full memtable
    // spent several milestones reaching clients as `500 DATABASE_ERROR: an
    // internal error occurred`.
    let insert_result = join_result
        .map_err(|e| {
            metrics::counter!("chronix_write_errors_total", "reason" => "panic").increment(1);
            ServerError::Internal(e.to_string())
        })?
        .map_err(|e| {
            metrics::counter!("chronix_write_errors_total", "reason" => "rejected").increment(1);
            ServerError::Db(e)
        })?;

    metrics::counter!("chronix_points_written_total").increment(insert_result.accepted as u64);
    if mode == WriteMode::Backfill {
        metrics::counter!("chronix_backfill_points_total").increment(insert_result.accepted as u64);
    }

    if insert_result.is_partial() {
        // Rejected points are *reported*, not only logged. A sender told
        // "204 No Content" has no way to learn that half its batch was
        // outside the out-of-order window, and the only place that
        // information existed was this server's log.
        let rejected = insert_result.rejected.len();
        tracing::warn!(
            accepted = insert_result.accepted,
            rejected,
            "Partial insert: some points were rejected"
        );
        metrics::counter!("chronix_write_points_rejected_total").increment(rejected as u64);
        let reason = insert_result
            .rejected
            .first()
            .map_or_else(String::new, |(_, e)| format!(": {e}"));
        return Err(ServerError::PartialWrite {
            accepted: insert_result.accepted,
            rejected,
            reason,
        });
    }

    Ok(())
}

/// Write a batch from an ingestion connector, and report what happened.
///
/// Connectors need a different answer from request handlers. A **partial**
/// write is not a reason to retry: the rejected points were refused
/// deterministically — a timestamp outside the out-of-order window will
/// still be outside it — so redelivering the batch rejects them again, for
/// ever, while rewriting the accepted ones each time round. Kafka's
/// at-least-once loop turns that into an infinite redelivery.
///
/// So a partial write is committed with a warning and the accepted points
/// counted; only a genuine failure asks the caller to retry.
///
/// # Errors
///
/// Returns the underlying error when nothing was written.
pub async fn insert_from_connector(
    db: &Arc<Chronix>,
    namespace: Option<&str>,
    points: Vec<Point>,
    timeout: std::time::Duration,
    connector: &str,
) -> Result<u64, ServerError> {
    let total = points.len() as u64;
    match insert_with_timeout(db, namespace, points, timeout).await {
        Ok(()) => Ok(total),
        Err(ServerError::PartialWrite {
            accepted,
            rejected,
            reason,
        }) => {
            tracing::warn!(
                connector,
                accepted,
                rejected,
                "connector batch partially written; the rejected points are \
                 refused deterministically, so the batch is not retried{reason}"
            );
            Ok(accepted as u64)
        }
        Err(e) => Err(e),
    }
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
pub fn column_type_to_str(ct: ColumnType) -> String {
    match ct {
        ColumnType::F64 => "float64".to_string(),
        ColumnType::I64 => "int64".to_string(),
        ColumnType::U64 => "uint64".to_string(),
        ColumnType::Bool => "bool".to_string(),
        ColumnType::String => "string".to_string(),
        ColumnType::Timestamp => "timestamp".to_string(),
        // The scale is part of the type, so it is part of the name: a
        // client that reads `decimal` alone cannot tell a schema endpoint
        // what the column actually stores.
        ColumnType::Decimal { scale } => {
            format!("decimal({}, {scale})", chronix_core::DECIMAL_PRECISION)
        }
    }
}

/// Render one cell of a `Decimal128` column as its exact digits.
///
/// Returns `None` for a null cell, or for a column whose scale is outside
/// what a Chronix decimal can carry.
///
/// Every wire encoder — HTTP JSON, gRPC, the schema endpoint — goes through
/// this one function, so a decimal reads the same on all of them and reads
/// the same as `FieldValue`'s own `Display`. The alternative was three
/// formatters that agree until one of them is changed.
#[must_use]
pub fn decimal_cell_to_string(arr: &arrow::array::Decimal128Array, row: usize) -> Option<String> {
    use arrow::array::Array;
    if arr.is_null(row) {
        return None;
    }
    let scale = u8::try_from(arr.scale()).ok()?;
    chronix_core::Decimal::new(arr.value(row), scale)
        .ok()
        .map(|d| d.to_string())
}

/// Convert a [`ColumnType`] to an Arrow [`DataType`](arrow::datatypes::DataType).
pub fn column_type_to_arrow(ct: ColumnType) -> arrow::datatypes::DataType {
    chronix_query::column_type_to_arrow(ct)
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

/// The JSON key that marks a field value as an exact decimal.
///
/// `{"z1nb": {"decimal": "1234.5678"}}` rather than `{"z1nb": 1234.5678}`,
/// and the difference is not decoration. A bare JSON number is parsed by
/// `serde_json` into an `f64`, so by the time the write handler sees it the
/// digits are already gone — `1234.5678` has become the nearest double, and
/// no amount of care downstream can recover what it was. The digits have to
/// arrive as digits, which in JSON means as a string.
pub const DECIMAL_JSON_KEY: &str = "decimal";

/// Convert a single JSON value to a [`FieldValue`].
///
/// Integer types are preferred over floats when the JSON number has no
/// fractional part. An exact decimal is written as
/// `{"decimal": "1234.5678"}` — see [`DECIMAL_JSON_KEY`].
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
        // `{"decimal": "…"}`, and also the `{"Decimal": "…"}` that
        // `FieldValue`'s own serialisation produces, so a value read back
        // out of a CDC event can be written straight back in.
        serde_json::Value::Object(map) if map.len() == 1 => {
            let (tag, inner) = map
                .into_iter()
                .next()
                .expect("len() == 1 guarantees one entry");
            if !tag.eq_ignore_ascii_case(DECIMAL_JSON_KEY) {
                return Err(ServerError::BadRequest(format!(
                    "unsupported value type for field '{key}': expected \
                     {{\"{DECIMAL_JSON_KEY}\": \"…\"}}, got {{\"{tag}\": …}}"
                )));
            }
            let digits = match inner {
                serde_json::Value::String(s) => s,
                // A JSON number here has already been through an `f64`, so
                // accepting it would defeat the point of asking for a
                // decimal in the first place.
                other => {
                    return Err(ServerError::BadRequest(format!(
                        "decimal field '{key}' must be a string of digits, not {other} — \
                         a JSON number is parsed as a double and loses the value"
                    )))
                }
            };
            digits
                .parse::<chronix_core::Decimal>()
                .map(FieldValue::Decimal)
                .map_err(|e| {
                    ServerError::BadRequest(format!("invalid decimal for field '{key}': {e}"))
                })
        }
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
        assert_eq!(
            column_type_to_str(ColumnType::Decimal { scale: 4 }),
            "decimal(38, 4)"
        );
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
        assert_eq!(
            column_type_to_arrow(ColumnType::Decimal { scale: 4 }),
            DataType::Decimal128(38, 4)
        );
    }

    #[test]
    fn json_value_to_field_integer() {
        let fv = json_value_to_field("x", serde_json::json!(42)).unwrap();
        assert!(matches!(fv, FieldValue::I64(42)));
    }

    #[test]
    fn json_value_to_field_decimal() {
        let fv = json_value_to_field("z1nb", serde_json::json!({"decimal": "1234.5678"})).unwrap();
        match fv {
            FieldValue::Decimal(d) => {
                assert_eq!(d.mantissa(), 12_345_678);
                assert_eq!(d.scale(), 4);
            }
            other => panic!("expected Decimal, got {other:?}"),
        }
        // The capitalised form is what `FieldValue` itself serialises to.
        assert!(matches!(
            json_value_to_field("z", serde_json::json!({"Decimal": "0.3"})).unwrap(),
            FieldValue::Decimal(_)
        ));
    }

    #[test]
    fn a_decimal_written_as_a_json_number_is_refused() {
        // Accepting it would mean accepting whatever `f64` the parser made
        // of the digits, which is the loss the type exists to prevent.
        let err = json_value_to_field("z", serde_json::json!({"decimal": 1234.5678})).unwrap_err();
        assert!(err.to_string().contains("string of digits"), "{err}");
    }

    #[test]
    fn an_unknown_single_key_object_is_refused() {
        assert!(json_value_to_field("z", serde_json::json!({"nope": "1"})).is_err());
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
/// send, and dropping the sample it cannot represent while accepting the
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
