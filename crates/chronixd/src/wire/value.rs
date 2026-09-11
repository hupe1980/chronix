//! The one Arrow → JSON encoding, shared by every Chronix wire surface.
//!
//! The `match` over [`DataType`] has **no `_` arm**. `DataType` is not
//! `#[non_exhaustive]`, so an Arrow release that adds a variant breaks this
//! build rather than quietly degrading a response — coverage is a compile
//! error, not a promise. Every surface used to carry its own table over a
//! different subset, and a type nobody had enumerated reached the client as
//! the string `"<unsupported: Date32>"` under `200 OK`.
//!
//! Each value is rendered losslessly, and the column's `data_type` in the
//! response says how to read it back:
//!
//! | Arrow type | JSON |
//! |---|---|
//! | `Null` | `null` |
//! | `Boolean` | `true` / `false` |
//! | `Int*`, `UInt*` | number, exact — no `f64` round trip |
//! | `Float16/32/64` | number; **non-finite becomes `null`** |
//! | `Utf8`, `LargeUtf8`, `Utf8View` | string |
//! | `Binary*`, `FixedSizeBinary` | base64 string |
//! | `Timestamp`, `Date32/64`, `Time32/64`, `Duration` | integer, in the unit `data_type` names |
//! | `Interval(..)` | object — `months` / `days` / `nanoseconds` |
//! | `Decimal32/64/128/256` | string of digits |
//! | `List*`, `FixedSizeList` | array |
//! | `Struct` | object keyed by field name |
//! | `Map` | array of `{"key": …, "value": …}` |
//! | `Union` | the value of the active variant |
//! | `Dictionary`, `RunEndEncoded` | the decoded value |
//!
//! Three encodings are deliberate rather than obvious:
//!
//! - **Temporal values are integers**, in the unit `data_type` names. One
//!   rule for every temporal type is worth more than per-type prettiness,
//!   and `_time` has always been an integer on this API.
//! - **A decimal is a string.** A JSON number is parsed back through an
//!   `f64` by every client, which is the loss the column type exists to
//!   prevent. Arrow's `value_as_string` renders it, which also keeps a
//!   negative scale exact.
//! - **A non-finite float is `null`.** JSON has no `NaN` literal, and a
//!   string in a column declared `Float64` would be the poison value this
//!   module exists to remove. Read over Flight SQL to keep it.

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use base64::Engine as _;
use serde_json::Value as Json;

/// Encode a single cell as JSON. See the [module docs](self) for the table.
///
/// `idx` must be in bounds for `col`.
#[must_use]
pub fn to_json(col: &dyn Array, idx: usize) -> Json {
    use arrow::array::*;

    if col.is_null(idx) {
        return Json::Null;
    }

    // A downcast that disagrees with `data_type()` is an Arrow invariant
    // violation, not a shape the data can take. Loud in debug, `null` in
    // release — the one case where `null` is not a silent degradation,
    // because it cannot happen without a bug in Arrow itself.
    macro_rules! cast {
        ($ty:ty) => {
            match col.as_any().downcast_ref::<$ty>() {
                Some(a) => a,
                None => return mismatch(col.data_type()),
            }
        };
    }
    /// A primitive rendered by `serde_json`'s own numeric conversion.
    macro_rules! num {
        ($ty:ty) => {
            serde_json::json!(cast!($ty).value(idx))
        };
    }
    /// Any decimal width, as exact digits.
    macro_rules! decimal {
        ($ty:ty) => {
            Json::String(cast!($ty).value_as_string(idx))
        };
    }
    /// A nested array: encode every element of the child.
    macro_rules! list {
        ($ty:ty) => {{
            let inner = cast!($ty).value(idx);
            json_array(&inner)
        }};
    }
    /// Bytes, base64-encoded (`STANDARD`, padded).
    macro_rules! bytes {
        ($ty:ty) => {
            Json::String(base64::engine::general_purpose::STANDARD.encode(cast!($ty).value(idx)))
        };
    }

    match col.data_type() {
        // `Null` has no values at all, so `is_null` above already returned.
        DataType::Null => Json::Null,
        DataType::Boolean => serde_json::json!(cast!(BooleanArray).value(idx)),

        DataType::Int8 => num!(Int8Array),
        DataType::Int16 => num!(Int16Array),
        DataType::Int32 => num!(Int32Array),
        DataType::Int64 => num!(Int64Array),
        DataType::UInt8 => num!(UInt8Array),
        DataType::UInt16 => num!(UInt16Array),
        DataType::UInt32 => num!(UInt32Array),
        DataType::UInt64 => num!(UInt64Array),

        DataType::Float16 => finite(f64::from(cast!(Float16Array).value(idx))),
        DataType::Float32 => finite(f64::from(cast!(Float32Array).value(idx))),
        DataType::Float64 => finite(cast!(Float64Array).value(idx)),

        DataType::Utf8 => Json::String(cast!(StringArray).value(idx).to_owned()),
        DataType::LargeUtf8 => Json::String(cast!(LargeStringArray).value(idx).to_owned()),
        DataType::Utf8View => Json::String(cast!(StringViewArray).value(idx).to_owned()),

        DataType::Binary => bytes!(BinaryArray),
        DataType::LargeBinary => bytes!(LargeBinaryArray),
        DataType::BinaryView => bytes!(BinaryViewArray),
        DataType::FixedSizeBinary(_) => bytes!(FixedSizeBinaryArray),

        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => num!(TimestampSecondArray),
            TimeUnit::Millisecond => num!(TimestampMillisecondArray),
            TimeUnit::Microsecond => num!(TimestampMicrosecondArray),
            TimeUnit::Nanosecond => num!(TimestampNanosecondArray),
        },
        DataType::Date32 => num!(Date32Array),
        DataType::Date64 => num!(Date64Array),
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => num!(Time32SecondArray),
            TimeUnit::Millisecond => num!(Time32MillisecondArray),
            // Arrow has no 32-bit micro/nanosecond time; the type is
            // unconstructible, so there is nothing to encode.
            TimeUnit::Microsecond | TimeUnit::Nanosecond => mismatch(col.data_type()),
        },
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => num!(Time64MicrosecondArray),
            TimeUnit::Nanosecond => num!(Time64NanosecondArray),
            TimeUnit::Second | TimeUnit::Millisecond => mismatch(col.data_type()),
        },
        DataType::Duration(unit) => match unit {
            TimeUnit::Second => num!(DurationSecondArray),
            TimeUnit::Millisecond => num!(DurationMillisecondArray),
            TimeUnit::Microsecond => num!(DurationMicrosecondArray),
            TimeUnit::Nanosecond => num!(DurationNanosecondArray),
        },
        // An interval is not a number of anything: months, days and
        // nanoseconds are independent because a month is not a span. An
        // object keeps the three parts a caller needs to apply it.
        DataType::Interval(unit) => match unit {
            IntervalUnit::YearMonth => {
                serde_json::json!({ "months": cast!(IntervalYearMonthArray).value(idx) })
            }
            IntervalUnit::DayTime => {
                let v = cast!(IntervalDayTimeArray).value(idx);
                serde_json::json!({ "days": v.days, "milliseconds": v.milliseconds })
            }
            IntervalUnit::MonthDayNano => {
                let v = cast!(IntervalMonthDayNanoArray).value(idx);
                serde_json::json!({
                    "months": v.months,
                    "days": v.days,
                    "nanoseconds": v.nanoseconds,
                })
            }
        },

        DataType::Decimal32(..) => decimal!(Decimal32Array),
        DataType::Decimal64(..) => decimal!(Decimal64Array),
        DataType::Decimal128(..) => decimal!(Decimal128Array),
        DataType::Decimal256(..) => decimal!(Decimal256Array),

        DataType::List(_) => list!(ListArray),
        DataType::LargeList(_) => list!(LargeListArray),
        DataType::FixedSizeList(..) => list!(FixedSizeListArray),
        DataType::ListView(_) => list!(ListViewArray),
        DataType::LargeListView(_) => list!(LargeListViewArray),

        DataType::Struct(fields) => {
            let arr = cast!(StructArray);
            let mut obj = serde_json::Map::with_capacity(fields.len());
            for (i, f) in fields.iter().enumerate() {
                obj.insert(f.name().clone(), to_json(arr.column(i).as_ref(), idx));
            }
            Json::Object(obj)
        }
        // A map key need not be a string, so this is an array of pairs
        // rather than a JSON object — which would have to stringify keys and
        // could collide two distinct ones.
        DataType::Map(..) => {
            let entries = cast!(MapArray).value(idx);
            let (keys, values) = (entries.column(0), entries.column(1));
            Json::Array(
                (0..entries.len())
                    .map(|i| {
                        serde_json::json!({
                            "key": to_json(keys.as_ref(), i),
                            "value": to_json(values.as_ref(), i),
                        })
                    })
                    .collect(),
            )
        }
        // `UnionArray::value` slices the active child to one element.
        DataType::Union(..) => {
            let child = cast!(UnionArray).value(idx);
            to_json(child.as_ref(), 0)
        }

        // Both encodings address a *physical* value from a *logical* index.
        DataType::Dictionary(..) => {
            use arrow::array::AsArray as _;
            let d = col.as_any_dictionary();
            // `normalized_keys` is O(len); resolving one key is not.
            let keys = d.keys();
            let Some(k) = key_index(keys, idx) else {
                return Json::Null;
            };
            to_json(d.values().as_ref(), k)
        }
        DataType::RunEndEncoded(run_ends, _) => match run_ends.data_type() {
            DataType::Int16 => run_value(cast!(RunArray<arrow::datatypes::Int16Type>), idx),
            DataType::Int32 => run_value(cast!(RunArray<arrow::datatypes::Int32Type>), idx),
            DataType::Int64 => run_value(cast!(RunArray<arrow::datatypes::Int64Type>), idx),
            _ => mismatch(col.data_type()),
        },
    }
}

/// Encode every element of an array, for a list cell.
fn json_array(arr: &ArrayRef) -> Json {
    Json::Array((0..arr.len()).map(|i| to_json(arr.as_ref(), i)).collect())
}

/// A run-end encoded cell, resolved through its physical index.
fn run_value<R: arrow::datatypes::RunEndIndexType>(
    arr: &arrow::array::RunArray<R>,
    idx: usize,
) -> Json {
    let physical = arr.get_physical_index(idx);
    to_json(arr.values().as_ref(), physical)
}

/// The dictionary key at `idx`, as a `usize`, whatever its integer width.
fn key_index(keys: &dyn Array, idx: usize) -> Option<usize> {
    use arrow::array::*;
    use arrow::datatypes::DataType as D;
    if keys.is_null(idx) {
        return None;
    }
    macro_rules! k {
        ($ty:ty) => {
            keys.as_any()
                .downcast_ref::<$ty>()
                .and_then(|a| usize::try_from(a.value(idx)).ok())
        };
    }
    match keys.data_type() {
        D::Int8 => k!(Int8Array),
        D::Int16 => k!(Int16Array),
        D::Int32 => k!(Int32Array),
        D::Int64 => k!(Int64Array),
        D::UInt8 => k!(UInt8Array),
        D::UInt16 => k!(UInt16Array),
        D::UInt32 => k!(UInt32Array),
        D::UInt64 => k!(UInt64Array),
        _ => None,
    }
}

/// A float that JSON can hold, or `null`. See the [module docs](self).
fn finite(v: f64) -> Json {
    if v.is_finite() {
        serde_json::json!(v)
    } else {
        Json::Null
    }
}

/// An array whose concrete type disagrees with its `DataType` — an Arrow
/// invariant violation. Loud in debug, `null` in release.
fn mismatch(dt: &DataType) -> Json {
    debug_assert!(false, "arrow array does not match its declared type {dt}");
    tracing::error!(data_type = %dt, "arrow array does not match its declared type");
    Json::Null
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::to_json;
    use arrow::array::*;
    use arrow::datatypes::{
        DataType, Field, Int32Type, Int64Type, IntervalDayTime, IntervalMonthDayNano,
    };
    use serde_json::json;
    use std::sync::Arc;

    /// The encodings SQL can reach are pinned in
    /// `chronixd/tests/suite/value_fidelity.rs`, against the real server.
    /// These are the ones only a hand-built array can produce.

    #[test]
    fn a_dictionary_resolves_to_its_value() {
        let arr: DictionaryArray<Int32Type> = vec![Some("a"), Some("b"), Some("a"), None]
            .into_iter()
            .collect();
        assert_eq!(to_json(&arr, 0), json!("a"));
        assert_eq!(to_json(&arr, 1), json!("b"));
        assert_eq!(to_json(&arr, 2), json!("a"));
        assert_eq!(to_json(&arr, 3), serde_json::Value::Null);
    }

    #[test]
    fn a_run_end_encoding_resolves_through_its_physical_index() {
        // Three logical rows, one physical value: the bug this guards is
        // reading the *logical* index out of the values array.
        let mut b = PrimitiveRunBuilder::<Int32Type, Int64Type>::new();
        b.extend([Some(7), Some(7), Some(7), Some(9)]);
        let arr = b.finish();
        assert_eq!(to_json(&arr, 0), json!(7));
        assert_eq!(to_json(&arr, 2), json!(7));
        assert_eq!(to_json(&arr, 3), json!(9));
    }

    #[test]
    fn a_map_is_an_array_of_pairs_because_a_key_need_not_be_a_string() {
        let mut b = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        b.keys().append_value("k1");
        b.values().append_value(1);
        b.keys().append_value("k2");
        b.values().append_value(2);
        b.append(true).unwrap();
        let arr = b.finish();
        assert_eq!(
            to_json(&arr, 0),
            json!([{"key": "k1", "value": 1}, {"key": "k2", "value": 2}])
        );
    }

    #[test]
    fn a_union_reports_the_active_variant() {
        let ints = Int32Array::from(vec![5]);
        let strs = StringArray::from(vec!["five"]);
        let fields = union_fields();
        let arr = UnionArray::try_new(
            fields,
            vec![0_i8, 1].into(),
            Some(vec![0_i32, 0].into()),
            vec![Arc::new(ints) as ArrayRef, Arc::new(strs) as ArrayRef],
        )
        .unwrap();
        assert_eq!(to_json(&arr, 0), json!(5));
        assert_eq!(to_json(&arr, 1), json!("five"));
    }

    fn union_fields() -> arrow::datatypes::UnionFields {
        [
            (0, Arc::new(Field::new("i", DataType::Int32, false))),
            (1, Arc::new(Field::new("s", DataType::Utf8, false))),
        ]
        .into_iter()
        .collect()
    }

    #[test]
    fn a_half_float_widens_without_a_placeholder() {
        // Built by casting, so this needs no `half` dependency of its own.
        let src = Float64Array::from(vec![1.5]);
        let arr = arrow::compute::cast(&src, &DataType::Float16).unwrap();
        assert_eq!(to_json(arr.as_ref(), 0), json!(1.5));
    }

    #[test]
    fn a_non_finite_float_is_null_not_a_string() {
        // JSON has no `NaN` literal, and a string in a `Float64` column is
        // the poison value this module exists to remove.
        let arr = Float64Array::from(vec![f64::NAN, f64::INFINITY, 1.0]);
        assert_eq!(to_json(&arr, 0), serde_json::Value::Null);
        assert_eq!(to_json(&arr, 1), serde_json::Value::Null);
        assert_eq!(to_json(&arr, 2), json!(1.0));
    }

    #[test]
    fn an_integer_keeps_every_digit() {
        // The reason integers are not routed through `f64`: 2^63-1 and
        // 2^64-1 are both past what a double can hold exactly.
        let i = Int64Array::from(vec![i64::MAX, i64::MIN]);
        assert_eq!(to_json(&i, 0), json!(9_223_372_036_854_775_807_i64));
        assert_eq!(to_json(&i, 1), json!(-9_223_372_036_854_775_808_i64));
        let u = UInt64Array::from(vec![u64::MAX]);
        assert_eq!(to_json(&u, 0), json!(18_446_744_073_709_551_615_u64));
    }

    #[test]
    fn a_decimal_of_any_width_is_digits_including_a_negative_scale() {
        // A negative scale is legal in Arrow. Reading it into a `u8` — which
        // the hand-rolled converter did — turned the cell into `null`.
        let neg = Decimal128Array::from(vec![123_i128])
            .with_precision_and_scale(10, -2)
            .unwrap();
        assert_eq!(to_json(&neg, 0), json!("12300"));

        let small = Decimal32Array::from(vec![12_345_i32])
            .with_precision_and_scale(9, 4)
            .unwrap();
        assert_eq!(to_json(&small, 0), json!("1.2345"));

        let big = Decimal256Array::from(vec![arrow::datatypes::i256::from_i128(100_i128)])
            .with_precision_and_scale(40, 2)
            .unwrap();
        assert_eq!(to_json(&big, 0), json!("1.00"));
    }

    #[test]
    fn the_two_decimal_renderings_agree() {
        // There are two on purpose: this module uses Arrow's own
        // `value_as_string`, because it is the only one that handles every
        // decimal width and a negative scale; the point-shaped `/query` and
        // gRPC surfaces use `chronix_core::Decimal`'s `Display`, because that
        // shape must parse back through `Decimal::from_str` when the point is
        // written again. Two renderings of one value is the drift this pass
        // exists to remove, so where both are reachable they are pinned equal.
        for (mantissa, scale) in [
            (0_i128, 0_u8),
            (1, 0),
            (-1, 0),
            (12_345, 4),
            (-12_345, 4),
            (7, 6),
            (-7, 6),
            (3_010, 2),
            (i128::from(u64::MAX), 17),
            (300_000_000_000_000_004, 17),
        ] {
            let arr = Decimal128Array::from(vec![mantissa])
                .with_precision_and_scale(
                    chronix_core::DECIMAL_PRECISION,
                    i8::try_from(scale).unwrap(),
                )
                .unwrap();
            let via_arrow = to_json(&arr, 0);
            let via_chronix = chronix_core::Decimal::new(mantissa, scale)
                .unwrap()
                .to_string();
            assert_eq!(
                via_arrow,
                json!(via_chronix),
                "renderings disagree for {mantissa}e-{scale}"
            );
        }
    }

    #[test]
    fn an_interval_keeps_its_three_parts_separate() {
        // A month is not a span of days and a day is not a span of hours, so
        // collapsing these to one number would be wrong for exactly the
        // reason `TimeBucket` exists.
        let ym = IntervalYearMonthArray::from(vec![14]);
        assert_eq!(to_json(&ym, 0), json!({"months": 14}));

        let dt = IntervalDayTimeArray::from(vec![IntervalDayTime::new(3, 500)]);
        assert_eq!(to_json(&dt, 0), json!({"days": 3, "milliseconds": 500}));

        let mdn = IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNano::new(1, 2, 3)]);
        assert_eq!(
            to_json(&mdn, 0),
            json!({"months": 1, "days": 2, "nanoseconds": 3})
        );
    }

    #[test]
    fn bytes_are_base64() {
        let arr = BinaryArray::from_vec(vec![b"abc"]);
        assert_eq!(to_json(&arr, 0), json!("YWJj"));
        let fixed =
            FixedSizeBinaryArray::try_from_iter(vec![vec![1_u8, 2, 3]].into_iter()).unwrap();
        assert_eq!(to_json(&fixed, 0), json!("AQID"));
    }

    #[test]
    fn a_nested_list_recurses() {
        let mut b = ListBuilder::new(Int64Builder::new());
        b.values().append_value(1);
        b.values().append_value(2);
        b.append(true);
        b.append(false);
        let arr = b.finish();
        assert_eq!(to_json(&arr, 0), json!([1, 2]));
        assert_eq!(to_json(&arr, 1), serde_json::Value::Null);
    }

    #[test]
    fn a_struct_is_an_object_keyed_by_field_name() {
        let arr = StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Int64, false)),
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
        ]);
        assert_eq!(to_json(&arr, 0), json!({"a": 1, "b": null}));
    }
}
