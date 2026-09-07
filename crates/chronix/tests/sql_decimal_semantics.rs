#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap
//! What SQL does with an exact decimal column — pinned, not assumed.
//!
//! Storing a value exactly is only half the promise. The other half is that
//! the query someone actually writes — `WHERE z1nb_q > 0.1`, `SELECT
//! sum(z1nb_q)`, `GROUP BY tariff` — compares and folds it exactly too. Every
//! one of those is a place where a plausible implementation converts to a
//! `double` first, and where the conversion is invisible until the twentieth
//! digit.
//!
//! So the values here are chosen so that the `f64` answer and the exact
//! answer **differ**. If any of these tests starts failing after a DataFusion
//! upgrade, it is because the comparison moved into binary floating point.

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

/// Write `values` as a decimal column of the given scale, one point each.
fn seed(db: &Chronix, scale: u8, values: &[&str]) {
    db.declare_field("meter", "v", ColumnType::Decimal { scale })
        .unwrap();
    let key = SeriesKey::new("meter", tags! { "dev" => "main" }).unwrap();
    for (i, text) in values.iter().enumerate() {
        db.insert(
            &Point::new(
                key.clone(),
                fields! { "v" => text.parse::<Decimal>().unwrap() },
                1_000 + i as i64,
            )
            .unwrap(),
        )
        .unwrap();
    }
    db.flush().unwrap();
}

fn count(db: &Chronix, sql: &str) -> i64 {
    let batches = db.sql(sql).unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap_or_else(|| panic!("count is not Int64 for: {sql}"))
        .value(0)
}

/// The single decimal cell a query returned, as exact digits.
fn one_decimal(db: &Chronix, sql: &str) -> String {
    let batches = db.sql(sql).unwrap();
    let col = batches[0].column(0);
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::Decimal128Array>()
        .unwrap_or_else(|| {
            panic!(
                "expected a decimal result for `{sql}`, got {:?} — the exactness \
                 was lost somewhere in planning",
                col.data_type()
            )
        });
    let scale = u8::try_from(arr.scale()).unwrap();
    Decimal::new(arr.value(0), scale).unwrap().to_string()
}

// ── Comparison ─────────────────────────────────────────────────────────

#[test]
fn a_literal_comparison_does_not_go_through_a_double() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    // One ulp above 0.1 at twenty places. As `f64` both this value and the
    // literal `0.1` are the same double, so a float comparison answers 0.
    seed(&db, 20, &["0.10000000000000000001"]);

    assert_eq!(
        count(&db, "SELECT count(*) FROM meter WHERE v > 0.1"),
        1,
        "a value one ulp above the literal must be greater than it"
    );
    assert_eq!(
        count(&db, "SELECT count(*) FROM meter WHERE v = 0.1"),
        0,
        "and must not be equal to it"
    );
}

#[test]
fn a_between_range_is_exact_at_its_endpoints() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed(&db, 2, &["1.14", "1.15", "1.16"]);
    assert_eq!(
        count(
            &db,
            "SELECT count(*) FROM meter WHERE v BETWEEN 1.15 AND 1.16"
        ),
        2
    );
    assert_eq!(count(&db, "SELECT count(*) FROM meter WHERE v > 1.15"), 1);
    assert_eq!(count(&db, "SELECT count(*) FROM meter WHERE v >= 1.15"), 2);
}

#[test]
fn an_in_list_matches_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed(&db, 4, &["0.1000", "0.2000", "0.3000"]);
    assert_eq!(
        count(&db, "SELECT count(*) FROM meter WHERE v IN (0.1, 0.3)"),
        2
    );
}

// ── Aggregation and arithmetic ─────────────────────────────────────────

#[test]
fn sum_is_exact_where_a_double_would_drift() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    // Ten hundredths. In an f64 this sums to 0.9999999999999999.
    seed(&db, 2, &["0.10"; 10]);
    assert_eq!(one_decimal(&db, "SELECT sum(v) FROM meter"), "1.00");
}

#[test]
fn min_and_max_stay_decimals() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed(&db, 4, &["1.1111", "3.3333", "2.2222"]);
    assert_eq!(one_decimal(&db, "SELECT min(v) FROM meter"), "1.1111");
    assert_eq!(one_decimal(&db, "SELECT max(v) FROM meter"), "3.3333");
}

#[test]
fn arithmetic_on_a_decimal_column_stays_decimal() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed(&db, 2, &["1.15"]);
    // A price times a quantity is the shape every settlement has.
    assert_eq!(one_decimal(&db, "SELECT v * 3 FROM meter"), "3.45");
    assert_eq!(one_decimal(&db, "SELECT v + 0.05 FROM meter"), "1.20");
    assert_eq!(one_decimal(&db, "SELECT v - 0.15 FROM meter"), "1.00");
}

#[test]
fn a_difference_of_registers_is_the_consumption() {
    // `last - first` per bucket is what turns a meter reading into
    // consumption, and it is the one subtraction a settlement rests on.
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed(&db, 4, &["1000.0001", "1000.2001"]);
    assert_eq!(
        one_decimal(&db, "SELECT max(v) - min(v) FROM meter"),
        "0.2000"
    );
}

#[test]
fn grouping_by_a_decimal_column_groups_on_the_value_not_a_float() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    // Two values that are the same double and different decimals.
    seed(
        &db,
        20,
        &["0.10000000000000000001", "0.10000000000000000002"],
    );
    assert_eq!(
        count(&db, "SELECT count(*) FROM (SELECT v FROM meter GROUP BY v)"),
        2,
        "two distinct decimals must not collapse into one group"
    );
}

// ── The explicit way out ───────────────────────────────────────────────

#[test]
fn casting_to_double_is_available_and_is_the_only_lossy_step() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed(&db, 2, &["1.15"]);
    let batches = db
        .sql("SELECT CAST(v AS DOUBLE) AS approx FROM meter")
        .unwrap();
    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("an explicit cast produces a float, and says so in the schema");
    assert!((col.value(0) - 1.15).abs() < 1e-12);
}

#[test]
fn the_column_type_is_visible_to_a_client() {
    // A client that reads a string of digits needs the schema to tell it
    // what those digits mean; `decimal(38, 4)` is that.
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed(&db, 4, &["1.0000"]);
    let batches = db.sql("SELECT v FROM meter LIMIT 1").unwrap();
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &arrow::datatypes::DataType::Decimal128(38, 4)
    );
}

#[test]
fn a_float_column_is_untouched_by_the_literal_change() {
    // Parsing `1.5` as a decimal is only safe because `Float64 op
    // Decimal128` still coerces to `Float64`. If that ever changes, every
    // existing float query silently changes shape, so it is pinned here
    // rather than assumed.
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("cpu", tags! { "h" => "a" }).unwrap();
    db.insert(&Point::new(key, fields! { "usage" => 2.0_f64 }, 1).unwrap())
        .unwrap();
    db.flush().unwrap();

    for sql in [
        "SELECT usage * 1.5 FROM cpu",
        "SELECT usage + 1.5 FROM cpu",
        "SELECT avg(usage) FROM cpu",
    ] {
        let batches = db.sql(sql).unwrap();
        assert_eq!(
            batches[0].schema().field(0).data_type(),
            &arrow::datatypes::DataType::Float64,
            "`{sql}` must still produce a float"
        );
    }
    // And a float predicate still filters.
    assert_eq!(count(&db, "SELECT count(*) FROM cpu WHERE usage > 1.5"), 1);
}

#[test]
fn an_explicit_cast_is_the_way_to_force_a_float_literal() {
    // Pinned because the obvious guess is wrong and was written into a
    // comment before this test existed: `1.5e0` is **not** a float here.
    // `parse_float_as_decimal` converts the exponent form too, so the only
    // way to insist on a float literal is to say so.
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed(&db, 2, &["1.15"]);

    assert_eq!(
        db.sql("SELECT v + 1.5e0 FROM meter").unwrap()[0]
            .schema()
            .field(0)
            .data_type(),
        &arrow::datatypes::DataType::Decimal128(38, 2),
        "the exponent form is a decimal too"
    );
    assert_eq!(
        db.sql("SELECT v + CAST(1.5 AS DOUBLE) FROM meter").unwrap()[0]
            .schema()
            .field(0)
            .data_type(),
        &arrow::datatypes::DataType::Float64,
        "an explicit cast is the escape hatch"
    );
}

// ── Time bucketing, the shape a settlement report has ──────────────────

#[test]
fn a_quarter_hour_report_sums_exactly_per_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    db.declare_field("meter", "v", ColumnType::Decimal { scale: 4 })
        .unwrap();
    let key = SeriesKey::new("meter", tags! { "dev" => "main" }).unwrap();
    // Two buckets an hour apart, five hundredths each.
    let hour = 3_600_000_000_000_i64;
    for bucket in 0..2_i64 {
        for i in 0..5_i64 {
            db.insert(
                &Point::new(
                    key.clone(),
                    fields! { "v" => "0.0100".parse::<Decimal>().unwrap() },
                    bucket * hour + i * 60_000_000_000,
                )
                .unwrap(),
            )
            .unwrap();
        }
    }
    db.flush().unwrap();

    let batches = db
        .sql(
            "SELECT time_bucket('1h', _time) AS b, sum(v) AS total \
             FROM meter GROUP BY b ORDER BY b",
        )
        .unwrap();
    let batch = &batches[0];
    let totals = batch
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Decimal128Array>()
        .expect("a per-bucket total of a decimal column is a decimal");
    let scale = u8::try_from(totals.scale()).unwrap();
    assert_eq!(batch.num_rows(), 2);
    for row in 0..2 {
        assert_eq!(
            Decimal::new(totals.value(row), scale).unwrap().to_string(),
            "0.0500"
        );
    }
}
