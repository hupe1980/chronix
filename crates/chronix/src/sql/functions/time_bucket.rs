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
/// Without a zone this is plain truncation of the UTC instant.
///
/// # Why the zoned path reads a *naive* clock
///
/// With a zone the bucket boundary is a local wall-clock boundary — local
/// midnight, local hour — so the truncation has to happen on the local clock
/// reading, not on the instant.
///
/// The distinction is easy to lose, and this function lost it:
/// `utc_dt.with_timezone(tz).timestamp_nanos_opt()` returns the **same number**
/// it was given. `with_timezone` changes how an instant is displayed, never
/// which instant it is, so the code truncated the UTC clock and then
/// interpreted the result as a local time. Whenever the UTC date and the local
/// date differ — which is most of every day, in every zone — the bucket landed
/// a whole interval out: 02:00 UTC on the 15th is 22:00 on the *14th* in New
/// York, and it was bucketed to the 15th.
///
/// `naive_local()` is the local clock reading; `and_utc()` then reads that
/// reading as a count of nanoseconds, which is the number to truncate.
fn bucket_timestamp(ts_nanos: i64, interval_nanos: i64, tz: Option<&chrono_tz::Tz>) -> i64 {
    let utc_truncated = ts_nanos - ts_nanos.rem_euclid(interval_nanos);
    let Some(tz) = tz else {
        return utc_truncated;
    };

    use chrono::{DateTime, TimeZone};
    let utc_dt = DateTime::from_timestamp_nanos(ts_nanos);

    // The local wall-clock reading, as nanoseconds on that clock.
    let Some(local_nanos) = utc_dt
        .with_timezone(tz)
        .naive_local()
        .and_utc()
        .timestamp_nanos_opt()
    else {
        return utc_truncated;
    };
    let bucketed_local = local_nanos - local_nanos.rem_euclid(interval_nanos);
    let bucketed_naive = DateTime::from_timestamp_nanos(bucketed_local).naive_utc();

    match tz.from_local_datetime(&bucketed_naive) {
        chrono::LocalResult::Single(dt) => dt.timestamp_nanos_opt().unwrap_or(utc_truncated),
        // Autumn fall-back: the local time occurs twice. The earliest is the
        // start of the bucket, which is what a boundary means.
        chrono::LocalResult::Ambiguous(earliest, _) => {
            earliest.timestamp_nanos_opt().unwrap_or(utc_truncated)
        }
        // Spring forward: the local time does not exist — the clock jumped
        // over it. The bucket starts at the instant the clock resumed, which
        // is the transition itself, so walk forward to the first local time
        // that does exist rather than silently falling back to a UTC bucket
        // (which is not a boundary in this zone at all).
        chrono::LocalResult::None => {
            const MINUTE_NS: i64 = 60 * 1_000_000_000;
            // A DST gap is at most a couple of hours; 180 one-minute steps
            // covers every transition in the tz database.
            (1..=180)
                .find_map(|step| {
                    let probe = DateTime::from_timestamp_nanos(
                        bucketed_local.saturating_add(step * MINUTE_NS),
                    )
                    .naive_utc();
                    match tz.from_local_datetime(&probe) {
                        chrono::LocalResult::Single(dt) => dt.timestamp_nanos_opt(),
                        chrono::LocalResult::Ambiguous(earliest, _) => {
                            earliest.timestamp_nanos_opt()
                        }
                        chrono::LocalResult::None => None,
                    }
                })
                .unwrap_or(utc_truncated)
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

#[cfg(test)]
mod timezone_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    const DAY: i64 = 86_400 * 1_000_000_000;
    const HOUR: i64 = 3_600 * 1_000_000_000;

    /// Parse an RFC3339 instant to epoch nanoseconds.
    fn ts(s: &str) -> i64 {
        chrono::DateTime::parse_from_rfc3339(s)
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap()
    }

    /// A zoned day bucket must be the local midnight, not the UTC midnight
    /// re-labelled.
    ///
    /// The old implementation read the *UTC* clock, truncated that, and then
    /// interpreted the result as a local time. Whenever the UTC date and the
    /// local date differ — which is most of every day for a western zone —
    /// that lands a whole day out.
    #[test]
    fn a_zoned_day_bucket_is_local_midnight() {
        let ny: chrono_tz::Tz = "America/New_York".parse().unwrap();

        // 02:00 UTC on the 15th is 22:00 on the *14th* in New York, so the
        // day bucket is the 14th's local midnight = 04:00Z on the 14th.
        let got = bucket_timestamp(ts("2024-03-15T02:00:00Z"), DAY, Some(&ny));
        assert_eq!(
            got,
            ts("2024-03-14T04:00:00Z"),
            "22:00 local on the 14th belongs to the 14th's bucket"
        );

        // And an instant that is already the same date in both zones.
        let got = bucket_timestamp(ts("2024-03-15T18:00:00Z"), DAY, Some(&ny));
        assert_eq!(got, ts("2024-03-15T04:00:00Z"));
    }

    /// East of Greenwich, the error ran the other way.
    #[test]
    fn a_zoned_day_bucket_east_of_utc() {
        let tokyo: chrono_tz::Tz = "Asia/Tokyo".parse().unwrap();

        // 20:00 UTC on the 15th is 05:00 on the *16th* in Tokyo, so the bucket
        // is the 16th's local midnight = 15:00Z on the 15th.
        let got = bucket_timestamp(ts("2024-03-15T20:00:00Z"), DAY, Some(&tokyo));
        assert_eq!(got, ts("2024-03-15T15:00:00Z"));
    }

    /// The whole reason for zoned bucketing: a DST day is 23 or 25 hours long,
    /// and both its local midnights must still be bucket boundaries.
    #[test]
    fn dst_days_keep_their_local_midnights() {
        let ny: chrono_tz::Tz = "America/New_York".parse().unwrap();

        // Spring forward: 2024-03-10 is a 23-hour day in New York.
        let before = bucket_timestamp(ts("2024-03-10T06:00:00Z"), DAY, Some(&ny)); // 01:00 EST
        let after = bucket_timestamp(ts("2024-03-10T20:00:00Z"), DAY, Some(&ny)); // 16:00 EDT
        assert_eq!(before, ts("2024-03-10T05:00:00Z"), "local midnight EST");
        assert_eq!(after, before, "both instants are the same local day");

        // The next local midnight is 23 hours later, not 24.
        let next = bucket_timestamp(ts("2024-03-11T12:00:00Z"), DAY, Some(&ny));
        assert_eq!(next - before, 23 * HOUR, "a spring-forward day is 23 hours");
    }

    /// An unzoned bucket is plain UTC truncation, unchanged.
    #[test]
    fn an_unzoned_bucket_truncates_utc() {
        assert_eq!(
            bucket_timestamp(ts("2024-03-15T02:00:00Z"), DAY, None),
            ts("2024-03-15T00:00:00Z")
        );
        assert_eq!(
            bucket_timestamp(ts("2024-03-15T02:34:56Z"), HOUR, None),
            ts("2024-03-15T02:00:00Z")
        );
    }

    /// Sub-day buckets are unaffected by the zone whenever the offset is a
    /// whole number of hours — but a half-hour zone is exactly where a wrong
    /// implementation shows up.
    #[test]
    fn a_half_hour_zone_buckets_on_its_own_clock() {
        let kolkata: chrono_tz::Tz = "Asia/Kolkata".parse().unwrap(); // UTC+05:30
                                                                      // 00:10 UTC is 05:40 local; the hour bucket is 05:00 local = 23:30Z
                                                                      // the previous day.
        let got = bucket_timestamp(ts("2024-03-15T00:10:00Z"), HOUR, Some(&kolkata));
        assert_eq!(got, ts("2024-03-14T23:30:00Z"));
    }
}
