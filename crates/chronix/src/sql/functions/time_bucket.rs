use std::sync::Arc;

use arrow::array::{AsArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, TimeUnit, TimestampNanosecondType};
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use chronix_core::timebucket::TimeBucket;

// ── time_bucket ─────────────────────────────────────────────────────────

/// `time_bucket(width, timestamp [, timezone])` — the SQL face of
/// [`TimeBucket`](chronix_core::timebucket::TimeBucket).
///
/// Widths: `'30s'`, `'15m'`, `'1h'`, `'1d'`, `'1w'`, `'1mo'`, `'3mo'`, `'1y'`.
/// The **unit** decides what the bucket means — sub-day units are a fixed span
/// that never varies, super-day units follow the local calendar — and the
/// month is `mo`, never `M`. See the [module
/// documentation](crate::timebucket) for the whole rule.
///
/// ```sql
/// SELECT time_bucket('5m', _time) AS bucket, AVG(value)
/// FROM cpu
/// GROUP BY bucket
/// ORDER BY bucket
/// ```
///
/// The optional third argument is an IANA time zone name
/// (e.g. `'Europe/Berlin'`): a day then runs from local midnight to local
/// midnight, and a month is that zone's calendar month.
///
/// ```sql
/// -- energy consumed per calendar month, in the meter's own time zone
/// SELECT time_bucket('1mo', _time, 'Europe/Berlin') AS month,
///        MAX(reading) - MIN(reading) AS kwh
/// FROM meter GROUP BY month ORDER BY month
/// ```
///
/// This function used to truncate the *local clock* for every width, which is
/// right for a day and wrong for an hour: on an autumn fall-back the repeated
/// local hour made two distinct UTC hours share one "one hour" bucket.
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
                    // …and with an origin. Written as a timestamp, or as a
                    // string DataFusion coerces to one — `'2024-01-15'` is
                    // what anybody actually types.
                    TypeSignature::Exact(vec![
                        DataType::Utf8,
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        DataType::Utf8,
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8,
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        DataType::Utf8,
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
        // The zone is the optional third argument.
        let tz_str: Option<&str> = if args.len() >= 3 {
            match &args[2] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(tz))) => Some(tz.as_str()),
                _ => None,
            }
        } else {
            None
        };
        let mut bucket = TimeBucket::parse(&interval_str, tz_str)
            .map_err(|e| datafusion::common::DataFusionError::Plan(format!("time_bucket: {e}")))?;

        // The origin is the optional fourth argument. `TimeBucket` is
        // `Copy`, and both forms go through the same parser the rollup API
        // uses, so a local origin lands in the bucket's own zone.
        if let Some(origin) = args.get(3) {
            let plan_err = |e: chronix_core::timebucket::BucketParseError| {
                datafusion::common::DataFusionError::Plan(format!("time_bucket: {e}"))
            };
            bucket = match origin {
                ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(ts), _)) => {
                    bucket.with_origin(*ts).map_err(plan_err)?
                }
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(text))) => {
                    bucket.with_origin_str(text).map_err(plan_err)?
                }
                // A NULL origin is the default anchor, not an error, so
                // `time_bucket(w, t, tz, NULL)` stays writable in generated
                // SQL.
                ColumnarValue::Scalar(s) if s.is_null() => bucket,
                _ => {
                    return Err(datafusion::common::DataFusionError::Plan(
                        "time_bucket: the origin must be a constant timestamp or date string, \
                         e.g. TIMESTAMP '2024-01-15 00:00:00' or '2024-01-15'"
                            .into(),
                    ))
                }
            };
        }

        // Get timestamps
        match &args[1] {
            ColumnarValue::Array(arr) => {
                let ts_array = arr.as_primitive::<TimestampNanosecondType>();
                let result: TimestampNanosecondArray = ts_array
                    .iter()
                    .map(|opt: Option<i64>| opt.map(|ts| bucket.start_of(ts)))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(ts), tz_arc)) => {
                let bucketed = bucket.start_of(*ts);
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use chronix_core::timebucket::BucketWidth;

    /// The widths a `time_bucket()` caller writes, parsed by the one parser.
    ///
    /// The bucketing itself is pinned in `crate::timebucket`; what belongs
    /// here is that this surface reaches it, and that a mistake at this
    /// surface is a planning error naming what was wrong.
    #[test]
    fn the_udf_parses_the_widths_it_documents() {
        for text in ["30s", "5m", "1h", "1d", "1w", "1mo", "3mo", "1y"] {
            assert!(text.parse::<BucketWidth>().is_ok(), "{text}");
        }
    }

    /// Drive the UDF the way the physical planner does.
    ///
    /// The parser and the bucketing each have their own tests; this is the
    /// one that proves the three arguments are wired to them — which is where
    /// a surface breaks without any of its parts being wrong.
    #[test]
    fn the_udf_buckets_an_array_in_a_zone() {
        use arrow::array::{Array, TimestampNanosecondArray};

        let ts = |s: &str| -> i64 {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap()
        };
        // Two instants an hour apart across Berlin's autumn transition, and
        // one ordinary instant. The first two used to share a bucket.
        let input: TimestampNanosecondArray = vec![
            Some(ts("2026-10-25T00:30:00Z")),
            Some(ts("2026-10-25T01:30:00Z")),
            None,
        ]
        .into();
        let out = super::super::helpers::invoke_udf(
            &TimeBucketUdf::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("1h".into()))),
                ColumnarValue::Array(Arc::new(input)),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("Europe/Berlin".into()))),
            ],
            3,
        )
        .unwrap();
        let ColumnarValue::Array(arr) = out else {
            panic!("expected an array")
        };
        let got = arr.as_primitive::<TimestampNanosecondType>();
        assert_eq!(got.value(0), ts("2026-10-25T00:00:00Z"));
        assert_eq!(got.value(1), ts("2026-10-25T01:00:00Z"));
        assert!(got.is_null(2), "a null timestamp buckets to null");
    }

    /// A calendar width reaches the UDF too — the case that could not be
    /// expressed at all before.
    #[test]
    fn the_udf_buckets_a_calendar_month() {
        use arrow::array::TimestampNanosecondArray;
        let ts = |s: &str| -> i64 {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap()
        };
        let input: TimestampNanosecondArray = vec![Some(ts("2024-02-17T12:00:00Z"))].into();
        let out = super::super::helpers::invoke_udf(
            &TimeBucketUdf::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("1mo".into()))),
                ColumnarValue::Array(Arc::new(input)),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("Europe/Berlin".into()))),
            ],
            1,
        )
        .unwrap();
        let ColumnarValue::Array(arr) = out else {
            panic!("expected an array")
        };
        assert_eq!(
            arr.as_primitive::<TimestampNanosecondType>().value(0),
            ts("2024-01-31T23:00:00Z"),
            "1 February 2024, Berlin local midnight"
        );
    }

    #[test]
    fn a_bad_width_or_zone_is_a_planning_error_that_says_what() {
        let err = TimeBucket::parse("5x", None).unwrap_err();
        assert!(err.0.contains("unknown bucket unit"), "{err}");
        let err = TimeBucket::parse("0m", None).unwrap_err();
        assert!(err.0.contains("positive"), "{err}");
        let err = TimeBucket::parse("1h", Some("Mars/Olympus")).unwrap_err();
        assert!(err.0.contains("Mars/Olympus"), "{err}");
    }
}
