#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap
//! A decimal field is exact from the write call to the query result.
//!
//! The claim these tests exist to defend is narrow and absolute: **a value
//! written as a `Decimal` never passes through binary floating point**. Not
//! on the way in, not in the write-ahead log, not in a `.csx` segment, not in
//! an Arrow batch, not in SQL, and not in a rollup. Every step below is a
//! place where a plausible implementation would have converted, and where a
//! conversion would be invisible: `0.1` through an `f64` and back still
//! *prints* as `0.1`.
//!
//! So the values here are chosen to be the ones binary floating point cannot
//! hold. `0.1 + 0.2 == 0.30000000000000004` in an `f64`; if any step in the
//! path had converted, the sum below would carry that tail.

use arrow::array::Array;
use chronix::prelude::*;
use chronix::{fields, tags, Chronix};
use std::sync::Arc;

fn open(dir: &tempfile::TempDir) -> Arc<Chronix> {
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    Arc::new(Chronix::open(config).unwrap())
}

fn dec(text: &str) -> FieldValue {
    FieldValue::Decimal(text.parse::<Decimal>().unwrap())
}

/// The exact digits of a `Decimal128` cell, or `None` for NULL.
fn decimal_cell(
    batch: &arrow::record_batch::RecordBatch,
    column: &str,
    row: usize,
) -> Option<String> {
    let col = batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("column '{column}' in {:?}", batch.schema()));
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::Decimal128Array>()
        .unwrap_or_else(|| {
            panic!(
                "column '{column}' is {:?}, not a decimal — a decimal that reads back as \
                 anything else has already lost",
                col.data_type()
            )
        });
    if arr.is_null(row) {
        return None;
    }
    let scale = u8::try_from(arr.scale()).unwrap();
    Some(Decimal::new(arr.value(row), scale).unwrap().to_string())
}

fn scan(db: &Chronix, measurement: &str) -> arrow::record_batch::RecordBatch {
    let plan = db
        .query()
        .measurement(measurement)
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    db.execute(&plan).unwrap()
}

// ── The path: memtable, WAL, segment ───────────────────────────────────

#[test]
fn a_decimal_survives_the_memtable() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("meter", tags! { "dev" => "main" }).unwrap();
    db.insert(&Point::new(key, fields! { "z1nb" => dec("1234.5678") }, 1_000).unwrap())
        .unwrap();

    let batch = scan(&db, "meter");
    assert_eq!(
        decimal_cell(&batch, "z1nb", 0).as_deref(),
        Some("1234.5678")
    );
}

#[test]
fn a_decimal_survives_a_flush_to_a_segment() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    // Seventeen digits, declared up front: past an f64's 15–16, and 0.1 and
    // 0.3 are both non-terminating in binary.
    db.declare_field("meter", "v", ColumnType::Decimal { scale: 17 })
        .unwrap();
    let key = SeriesKey::new("meter", tags! { "dev" => "main" }).unwrap();
    for (i, text) in ["0.1", "0.2", "0.30000000000000004"].iter().enumerate() {
        db.insert(
            &Point::new(key.clone(), fields! { "v" => dec(text) }, 1_000 + i as i64).unwrap(),
        )
        .unwrap();
    }
    db.flush().unwrap();

    let batch = scan(&db, "meter");
    let mut seen: Vec<String> = (0..batch.num_rows())
        .filter_map(|r| decimal_cell(&batch, "v", r))
        .collect();
    seen.sort();
    assert_eq!(
        seen,
        [
            "0.10000000000000000",
            "0.20000000000000000",
            "0.30000000000000004"
        ]
    );
}

#[test]
fn a_decimal_column_and_its_scale_survive_a_restart() {
    // The catalog manifest is a schema's only durable record, so a decimal
    // column that forgot its scale on the way through it would come back as
    // a different type — and every value already on disk would then be
    // wrong by a power of ten.
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open(&dir);
        db.declare_field("meter", "z1nb", ColumnType::Decimal { scale: 4 })
            .unwrap();
        let key = SeriesKey::new("meter", tags! { "dev" => "main" }).unwrap();
        db.insert(&Point::new(key, fields! { "z1nb" => dec("1234.5678") }, 1_000).unwrap())
            .unwrap();
        db.close().unwrap();
    }
    let db = open(&dir);
    assert_eq!(
        db.schema("meter")
            .unwrap()
            .column("z1nb")
            .unwrap()
            .column_type,
        ColumnType::Decimal { scale: 4 }
    );
    let batch = scan(&db, "meter");
    assert_eq!(
        decimal_cell(&batch, "z1nb", 0).as_deref(),
        Some("1234.5678")
    );
}

#[test]
fn a_38_digit_value_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("big", tags! {}).unwrap();
    // The widest value the format can hold, which needs the codec's wide
    // form: an i64 mantissa cannot reach it.
    let widest = "999999999999999999999999999999999.99999";
    db.insert(&Point::new(key, fields! { "v" => dec(widest) }, 1).unwrap())
        .unwrap();
    db.flush().unwrap();
    let batch = scan(&db, "big");
    assert_eq!(decimal_cell(&batch, "v", 0).as_deref(), Some(widest));
}

// ── The scale belongs to the column ────────────────────────────────────

#[test]
fn a_narrower_value_is_widened_to_the_column_scale() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    db.declare_field("meter", "z1nb", ColumnType::Decimal { scale: 4 })
        .unwrap();
    let key = SeriesKey::new("meter", tags! {}).unwrap();
    db.insert(&Point::new(key, fields! { "z1nb" => dec("1.5") }, 1).unwrap())
        .unwrap();

    let batch = scan(&db, "meter");
    assert_eq!(decimal_cell(&batch, "z1nb", 0).as_deref(), Some("1.5000"));
}

#[test]
fn a_value_needing_more_digits_than_the_column_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    db.declare_field("meter", "z1nb", ColumnType::Decimal { scale: 2 })
        .unwrap();
    let key = SeriesKey::new("meter", tags! {}).unwrap();
    let err = db
        .insert(&Point::new(key, fields! { "z1nb" => dec("1.005") }, 1).unwrap())
        .unwrap_err();
    let message = err.to_string();
    // Refused, and refused informatively: the message has to say what the
    // column stores and what arrived, or the writer cannot act on it.
    assert!(message.contains("scale"), "{message}");
    assert!(message.contains("1.005"), "{message}");
    // And nothing was written.
    assert_eq!(scan(&db, "meter").num_rows(), 0);
}

#[test]
fn declaring_the_same_column_twice_is_fine_and_a_different_type_is_not() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    db.declare_field("meter", "z1nb", ColumnType::Decimal { scale: 4 })
        .unwrap();
    db.declare_field("meter", "z1nb", ColumnType::Decimal { scale: 4 })
        .unwrap();
    assert!(db
        .declare_field("meter", "z1nb", ColumnType::Decimal { scale: 6 })
        .is_err());
    assert!(db.declare_field("meter", "z1nb", ColumnType::F64).is_err());
}

#[test]
fn a_batch_creating_the_column_takes_its_widest_scale() {
    // Which point of a batch happens to come first must not decide how many
    // digits the column keeps for ever.
    for order in [["1.5", "1.4999"], ["1.4999", "1.5"]] {
        let dir = tempfile::tempdir().unwrap();
        let db = open(&dir);
        let key = SeriesKey::new("meter", tags! {}).unwrap();
        let points: Vec<Point> = order
            .iter()
            .enumerate()
            .map(|(i, text)| {
                Point::new(key.clone(), fields! { "v" => dec(text) }, 1 + i as i64).unwrap()
            })
            .collect();
        let result = db.insert_batch(&points).unwrap();
        assert!(result.rejected.is_empty(), "order {order:?}: {result:?}");

        let schema = db.schema("meter").unwrap();
        assert_eq!(
            schema.column("v").unwrap().column_type,
            ColumnType::Decimal { scale: 4 },
            "order {order:?}"
        );
    }
}

#[test]
fn the_schema_reports_the_decimal_type_with_its_scale() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("meter", tags! {}).unwrap();
    db.insert(&Point::new(key, fields! { "v" => dec("1.5000") }, 1).unwrap())
        .unwrap();
    let schema = db.schema("meter").unwrap();
    let column = schema.column("v").unwrap();
    assert_eq!(column.column_type, ColumnType::Decimal { scale: 4 });
    assert_eq!(column.column_type.to_string(), "decimal(38, 4)");
    // And the type round-trips through its own text form, which is what the
    // schema endpoint publishes and the declare endpoint accepts.
    assert_eq!(
        "decimal(38, 4)".parse::<ColumnType>().unwrap(),
        column.column_type
    );
}

// ── Aggregation stays exact ────────────────────────────────────────────

#[test]
fn the_native_sum_of_decimals_is_exact() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("meter", tags! {}).unwrap();
    for (i, text) in ["0.1", "0.2"].iter().enumerate() {
        db.insert(&Point::new(key.clone(), fields! { "v" => dec(text) }, 1 + i as i64).unwrap())
            .unwrap();
    }

    let plan = db
        .query()
        .measurement("meter")
        .range(i64::MIN, i64::MAX)
        .aggregate(AggFn::Sum)
        .field("v")
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    // 0.1 + 0.2 in an f64 is 0.30000000000000004. Here it is 0.3.
    assert_eq!(decimal_cell(&batch, "v_sum", 0).as_deref(), Some("0.3"));
}

#[test]
fn min_max_first_and_last_stay_decimals() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("meter", tags! {}).unwrap();
    for (i, text) in ["1.11", "3.33", "2.22"].iter().enumerate() {
        db.insert(&Point::new(key.clone(), fields! { "v" => dec(text) }, 1 + i as i64).unwrap())
            .unwrap();
    }
    let plan = db
        .query()
        .measurement("meter")
        .range(i64::MIN, i64::MAX)
        .aggregate(AggFn::Min)
        .aggregate(AggFn::Max)
        .aggregate(AggFn::First)
        .aggregate(AggFn::Last)
        .field("v")
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    assert_eq!(decimal_cell(&batch, "v_min", 0).as_deref(), Some("1.11"));
    assert_eq!(decimal_cell(&batch, "v_max", 0).as_deref(), Some("3.33"));
    assert_eq!(decimal_cell(&batch, "v_first", 0).as_deref(), Some("1.11"));
    assert_eq!(decimal_cell(&batch, "v_last", 0).as_deref(), Some("2.22"));
}

#[test]
fn the_average_of_decimals_is_carried_to_a_stated_scale() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("meter", tags! {}).unwrap();
    // 1/3 has no finite decimal form, so this is the aggregate that has to
    // round — and the rule it rounds by is documented, not a double's.
    for (i, text) in ["1.00", "1.00", "2.00"].iter().enumerate() {
        db.insert(&Point::new(key.clone(), fields! { "v" => dec(text) }, 1 + i as i64).unwrap())
            .unwrap();
    }
    let plan = db
        .query()
        .measurement("meter")
        .range(i64::MIN, i64::MAX)
        .aggregate(AggFn::Avg)
        .field("v")
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    // (1 + 1 + 2) / 3 = 1.333333…, at the column's scale (2) plus six.
    assert_eq!(
        decimal_cell(&batch, "v_avg", 0).as_deref(),
        Some("1.33333333")
    );
}

#[test]
fn a_count_of_decimals_is_a_count() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("meter", tags! {}).unwrap();
    for i in 0..3 {
        db.insert(&Point::new(key.clone(), fields! { "v" => dec("1.00") }, 1 + i).unwrap())
            .unwrap();
    }
    let plan = db
        .query()
        .measurement("meter")
        .range(i64::MIN, i64::MAX)
        .aggregate(AggFn::Count)
        .field("v")
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    let col = batch
        .column_by_name("v_count")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("a count is a count, not a decimal");
    assert!((col.value(0) - 3.0).abs() < f64::EPSILON);
}

#[test]
fn a_grouped_sum_of_decimals_is_exact_per_group() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    for (dev, texts) in [("a", ["0.1", "0.2"]), ("b", ["0.7", "0.1"])] {
        let key = SeriesKey::new("meter", tags! { "dev" => dev }).unwrap();
        for (i, text) in texts.iter().enumerate() {
            db.insert(
                &Point::new(key.clone(), fields! { "v" => dec(text) }, 1 + i as i64).unwrap(),
            )
            .unwrap();
        }
    }
    let plan = db
        .query()
        .measurement("meter")
        .range(i64::MIN, i64::MAX)
        .aggregate(AggFn::Sum)
        .field("v")
        .group_by(&["dev"])
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    let devices = batch
        .column_by_name("dev")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let mut got: Vec<(String, String)> = (0..batch.num_rows())
        .map(|r| {
            (
                devices.value(r).to_string(),
                decimal_cell(&batch, "v_sum", r).unwrap(),
            )
        })
        .collect();
    got.sort();
    assert_eq!(
        got,
        [
            ("a".to_string(), "0.3".to_string()),
            ("b".to_string(), "0.8".to_string())
        ]
    );
}

// ── SQL ────────────────────────────────────────────────────────────────

#[cfg(feature = "sql")]
#[test]
fn sql_sees_a_decimal_column_and_sums_it_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("meter", tags! {}).unwrap();
    for (i, text) in ["0.1", "0.2"].iter().enumerate() {
        db.insert(&Point::new(key.clone(), fields! { "v" => dec(text) }, 1 + i as i64).unwrap())
            .unwrap();
    }
    db.flush().unwrap();

    let batches = db.sql("SELECT sum(v) AS total FROM meter").unwrap();
    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Decimal128Array>()
        .expect("DataFusion aggregates a decimal as a decimal");
    let scale = u8::try_from(col.scale()).unwrap();
    assert_eq!(
        Decimal::new(col.value(0), scale).unwrap().to_string(),
        "0.3"
    );
}

#[cfg(feature = "sql")]
#[test]
fn sql_compares_a_decimal_without_going_through_a_double() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("meter", tags! {}).unwrap();
    for (i, text) in ["1.10", "1.20", "1.30"].iter().enumerate() {
        db.insert(&Point::new(key.clone(), fields! { "v" => dec(text) }, 1 + i as i64).unwrap())
            .unwrap();
    }
    db.flush().unwrap();

    let batches = db.sql("SELECT count(*) FROM meter WHERE v > 1.15").unwrap();
    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(col.value(0), 2);
}

// ── Rollups ────────────────────────────────────────────────────────────

#[test]
fn a_rollup_of_a_decimal_column_stays_exact() {
    use chronix::rollup::{compute_rollup_points, RollupAggFn, RollupBuilder};

    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("meter", tags! {}).unwrap();
    // Four quarter-hour registers whose f64 sum would not be their decimal
    // sum: 0.1 + 0.2 + 0.3 + 0.4 is 1.0000000000000002 in a double.
    for (i, text) in ["0.1", "0.2", "0.3", "0.4"].iter().enumerate() {
        db.insert(&Point::new(key.clone(), fields! { "v" => dec(text) }, 1 + i as i64).unwrap())
            .unwrap();
    }

    let config = RollupBuilder::new()
        .name("daily")
        .source("meter")
        .target("meter_daily")
        .every("1d")
        .aggregation(RollupAggFn::Sum)
        .build()
        .unwrap();
    let batch = scan(&db, "meter");
    let points = compute_rollup_points(&[batch], &config);
    assert_eq!(points.len(), 1);
    match points[0].field("v_sum") {
        Some(FieldValue::Decimal(d)) => assert_eq!(d.to_string(), "1.0"),
        other => panic!("a rollup of a decimal must stay a decimal, got {other:?}"),
    }
}

// ── Nulls ──────────────────────────────────────────────────────────────

#[test]
fn an_absent_decimal_is_null_not_zero() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("meter", tags! {}).unwrap();
    db.insert(
        &Point::new(
            key.clone(),
            fields! { "v" => dec("1.50"), "other" => 1.0 },
            1,
        )
        .unwrap(),
    )
    .unwrap();
    // A second point with only the other field: `v` is absent, not zero.
    db.insert(&Point::new(key, fields! { "other" => 2.0 }, 2).unwrap())
        .unwrap();
    db.flush().unwrap();

    let batch = scan(&db, "meter");
    let present: Vec<Option<String>> = (0..batch.num_rows())
        .map(|r| decimal_cell(&batch, "v", r))
        .collect();
    assert_eq!(present.iter().filter(|v| v.is_some()).count(), 1);
    assert_eq!(present.iter().filter(|v| v.is_none()).count(), 1);
}

// ── Type conflicts ─────────────────────────────────────────────────────

#[test]
fn a_decimal_and_a_float_are_different_types() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("meter", tags! {}).unwrap();
    db.insert(&Point::new(key.clone(), fields! { "v" => dec("1.50") }, 1).unwrap())
        .unwrap();
    // Writing an f64 into a decimal column is a type conflict, not a
    // silent conversion — the whole point of having asked for exactness.
    assert!(db
        .insert(&Point::new(key, fields! { "v" => 1.5_f64 }, 2).unwrap())
        .is_err());
}
