use std::sync::Arc;

use arrow::array::{AsArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, TimeUnit, TimestampNanosecondType};
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

// ── time_bucket ─────────────────────────────────────────────────────────

/// `time_bucket(interval_str, timestamp [, timezone])` — bucket timestamps
/// by interval, with optional timezone-aware bucketing for DST correctness.
///
/// Interval format: `'5m'`, `'1h'`, `'1d'`, `'30s'`, `'15m'`.
///
/// ```sql
/// SELECT time_bucket('5m', _time) AS bucket, AVG(value)
/// FROM cpu
/// GROUP BY bucket
/// ORDER BY bucket
/// ```
///
/// Optional third argument specifies an IANA timezone name
/// (e.g. `'America/New_York'`). When set, bucketing is performed in
/// local time so that day/hour boundaries align correctly across DST
/// transitions.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct TimeBucketUdf {
    signature: Signature,
}

impl TimeBucketUdf {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::OneOf(vec![
                    TypeSignature::Exact(vec![
                        DataType::Utf8,
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8,
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        DataType::Utf8,
                    ]),
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for TimeBucketUdf {
    fn name(&self) -> &'static str {
        "time_bucket"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = args.args;
        // Parse interval from first argument
        let interval_str = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            _ => {
                return Err(datafusion::common::DataFusionError::Plan(
                    "time_bucket: first argument must be a string interval like '5m'".into(),
                ))
            }
        };
        let interval_nanos = parse_interval(&interval_str)?;

        // Parse optional timezone (3rd argument).
        let tz: Option<chrono_tz::Tz> = if args.len() >= 3 {
            match &args[2] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(tz_str))) => {
                    let parsed: chrono_tz::Tz = tz_str.parse().map_err(|_| {
                        datafusion::common::DataFusionError::Plan(format!(
                            "time_bucket: unknown timezone '{tz_str}'. Use IANA names like 'America/New_York'"
                        ))
                    })?;
                    Some(parsed)
                }
                _ => None,
            }
        } else {
            None
        };

        // Get timestamps
        match &args[1] {
            ColumnarValue::Array(arr) => {
                let ts_array = arr.as_primitive::<TimestampNanosecondType>();
                let result: TimestampNanosecondArray = ts_array
                    .iter()
                    .map(|opt: Option<i64>| {
                        opt.map(|ts| bucket_timestamp(ts, interval_nanos, tz.as_ref()))
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(ts), tz_arc)) => {
                let bucketed = bucket_timestamp(*ts, interval_nanos, tz.as_ref());
                Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                    Some(bucketed),
                    tz_arc.clone(),
                )))
            }
            _ => Err(datafusion::common::DataFusionError::Plan(
                "time_bucket: second argument must be a timestamp".into(),
            )),
        }
    }
}

/// Bucket a single nanosecond UTC timestamp.
///
/// When `tz` is `Some`, converts to local time, truncates, then converts
/// back to UTC. This ensures day/hour boundaries align with the local
/// clock, handling DST transitions correctly.
fn bucket_timestamp(ts_nanos: i64, interval_nanos: i64, tz: Option<&chrono_tz::Tz>) -> i64 {
    match tz {
        None => ts_nanos - ts_nanos.rem_euclid(interval_nanos),
        Some(tz) => {
            use chrono::{DateTime, TimeZone};
            let utc_dt = DateTime::from_timestamp_nanos(ts_nanos);
            let local_dt = utc_dt.with_timezone(tz);
            let local_nanos = local_dt.timestamp_nanos_opt().unwrap_or(ts_nanos);
            let bucketed_local = local_nanos - local_nanos.rem_euclid(interval_nanos);
            // Convert bucketed local time back to UTC.
            let bucketed_naive = DateTime::from_timestamp(
                bucketed_local.div_euclid(1_000_000_000),
                (bucketed_local.rem_euclid(1_000_000_000)) as u32,
            )
            .map_or_else(|| utc_dt.naive_utc(), |dt| dt.naive_utc());
            // Use the timezone to convert back; earliest is safest for ambiguous times.
            match tz.from_local_datetime(&bucketed_naive) {
                chrono::LocalResult::Single(dt) => dt.timestamp_nanos_opt().unwrap_or(ts_nanos),
                chrono::LocalResult::Ambiguous(earliest, _) => {
                    earliest.timestamp_nanos_opt().unwrap_or(ts_nanos)
                }
                chrono::LocalResult::None => {
                    // Gap (spring forward): use next valid time.
                    let _ = bucketed_naive;
                    ts_nanos - ts_nanos.rem_euclid(interval_nanos) // fallback to UTC
                }
            }
        }
    }
}

pub(super) fn parse_interval(s: &str) -> DFResult<i64> {
    let s = s.trim();
    if s.is_empty() {
        return Err(datafusion::common::DataFusionError::Plan(
            "empty interval string".into(),
        ));
    }
    let (num_str, suffix) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
    let num: i64 = num_str.parse().map_err(|_| {
        datafusion::common::DataFusionError::Plan(format!("invalid interval number: '{num_str}'"))
    })?;
    let nanos_per_unit: i64 = match suffix.trim() {
        "ns" => 1,
        "us" | "µs" => 1_000,
        "ms" => 1_000_000,
        "s" => 1_000_000_000,
        "m" | "min" => 60 * 1_000_000_000,
        "h" => 3_600 * 1_000_000_000,
        "d" => 86_400 * 1_000_000_000,
        "w" => 7 * 86_400 * 1_000_000_000,
        other => {
            return Err(datafusion::common::DataFusionError::Plan(format!(
                "unknown interval unit: '{other}'. Use ns, us, ms, s, m, h, d, w"
            )))
        }
    };
    let result = num.checked_mul(nanos_per_unit).ok_or_else(|| {
        datafusion::common::DataFusionError::Plan(format!(
            "interval overflow: {num} * {nanos_per_unit} exceeds i64 range"
        ))
    })?;
    if result <= 0 {
        return Err(datafusion::common::DataFusionError::Plan(
            "interval must be positive (e.g. '5m', '1h')".into(),
        ));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_interval_various_units() {
        assert_eq!(parse_interval("5m").unwrap(), 5 * 60 * 1_000_000_000);
        assert_eq!(parse_interval("1h").unwrap(), 3_600 * 1_000_000_000);
        assert_eq!(parse_interval("30s").unwrap(), 30 * 1_000_000_000);
        assert_eq!(parse_interval("1d").unwrap(), 86_400 * 1_000_000_000);
        assert_eq!(parse_interval("100ms").unwrap(), 100 * 1_000_000);
        assert!(parse_interval("").is_err());
        assert!(parse_interval("5x").is_err());
    }

    #[test]
    fn parse_interval_rejects_zero() {
        let err = parse_interval("0s").unwrap_err();
        assert!(
            err.to_string().contains("positive"),
            "expected positive-interval error, got: {err}"
        );
        let err = parse_interval("0m").unwrap_err();
        assert!(
            err.to_string().contains("positive"),
            "expected positive-interval error, got: {err}"
        );
    }

    #[test]
    fn time_bucket_udf() {
        let udf = TimeBucketUdf::new();
        let interval = ColumnarValue::Scalar(ScalarValue::Utf8(Some("1h".to_string())));
        let ts = ColumnarValue::Array(Arc::new(arrow::array::TimestampNanosecondArray::from(
            vec![
                3_661_000_000_000i64, // 1h 1m 1s
            ],
        )));
        let result =
            crate::sql::functions::helpers::invoke_udf(&udf, Vec::from([interval, ts]), 1).unwrap();
        match &result {
            ColumnarValue::Array(arr) => {
                let ta = arr
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                    .unwrap();
                // Should be truncated to 3600s = 3_600_000_000_000 ns
                assert_eq!(ta.value(0), 3_600_000_000_000);
            }
            _ => panic!("expected array"),
        }
    }
}
