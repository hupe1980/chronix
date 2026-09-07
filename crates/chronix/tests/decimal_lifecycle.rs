#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap
//! A decimal column through the machinery that rewrites, copies and prunes it.
//!
//! `exact_decimals.rs` covers the straight line: write, read, aggregate. This
//! covers the paths that take a decimal column apart and put it back —
//! compaction, Parquet export, the change stream — and the one that decides
//! *not* to read it. Each of those had a type dispatch that predated the
//! type, and each of those dispatches ended in a branch that dropped what it
//! did not recognise.
//!
//! The pruning tests are the sharpest: a zone map that prunes a row group it
//! should have kept produces a query that returns *fewer rows*, silently and
//! deterministically, which is the worst failure this database can have.

use arrow::array::Array;
use chronix::chronix_query::{self, plan::FieldPredicate, plan::ZoneMapOp};
use chronix::prelude::*;
use chronix::{fields, tags, Chronix, ParquetExportConfig};
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

fn digits(batch: &arrow::record_batch::RecordBatch, column: &str, row: usize) -> Option<String> {
    let arr = batch
        .column_by_name(column)?
        .as_any()
        .downcast_ref::<arrow::array::Decimal128Array>()
        .unwrap_or_else(|| panic!("'{column}' is not a decimal in {:?}", batch.schema()));
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

/// Write `count` registers a nanosecond apart, flushing every `per_segment`
/// so the database ends up with several segments to merge.
fn seed_segments(db: &Chronix, count: i64, per_segment: i64) {
    db.declare_field("meter", "z1nb", ColumnType::Decimal { scale: 4 })
        .unwrap();
    let key = SeriesKey::new("meter", tags! { "dev" => "main" }).unwrap();
    for i in 0..count {
        // A monotone register: 1000.0000, 1000.0001, …
        let value = Decimal::new(i128::from(10_000_000 + i), 4).unwrap();
        db.insert(&Point::new(key.clone(), fields! { "z1nb" => value }, 1_000 + i).unwrap())
            .unwrap();
        if (i + 1) % per_segment == 0 {
            db.flush().unwrap();
        }
    }
    db.flush().unwrap();
}

// ── Compaction ─────────────────────────────────────────────────────────

#[test]
fn compaction_preserves_every_digit_and_the_column_type() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed_segments(&db, 40, 10);

    let before = scan(&db, "meter");
    let before_values: Vec<Option<String>> = (0..before.num_rows())
        .map(|r| digits(&before, "z1nb", r))
        .collect();
    assert_eq!(before_values.len(), 40);

    db.compact().unwrap();

    let after = scan(&db, "meter");
    let after_values: Vec<Option<String>> = (0..after.num_rows())
        .map(|r| digits(&after, "z1nb", r))
        .collect();
    assert_eq!(
        after_values, before_values,
        "compaction rewrote the segments and changed the values"
    );
    // And the column is still a decimal at its own scale, not a float that
    // happens to print the same.
    assert_eq!(
        after.schema().field_with_name("z1nb").unwrap().data_type(),
        &arrow::datatypes::DataType::Decimal128(38, 4)
    );
}

#[test]
fn a_decimal_column_survives_compaction_and_a_restart_together() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open(&dir);
        seed_segments(&db, 40, 10);
        db.compact().unwrap();
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
    assert_eq!(batch.num_rows(), 40);
    assert_eq!(digits(&batch, "z1nb", 0).as_deref(), Some("1000.0000"));
}

// ── Zone-map pruning ───────────────────────────────────────────────────

#[test]
fn a_predicate_on_a_decimal_column_never_prunes_a_matching_row() {
    // The bound the zone map holds is a *mantissa*, and the predicate is an
    // `f64`. Comparing them naively prunes on a value that is not quite the
    // one the caller wrote — and a pruning step is only ever allowed to be
    // wrong in the direction of more work.
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed_segments(&db, 40, 10);

    // Every row: 1000.0000 … 1000.0039. A bound *inside* that range is the
    // interesting one — it is where a zone map has to decide per row group.
    for (op, name, at_least) in [
        (ZoneMapOp::GtEq, ">=", 40_usize),
        (ZoneMapOp::Gt, ">", 39),
        (ZoneMapOp::LtEq, "<=", 1),
        (ZoneMapOp::Eq, "=", 1),
    ] {
        let mut plan = db
            .query()
            .measurement("meter")
            .range(i64::MIN, i64::MAX)
            .build()
            .unwrap();
        chronix_query::plan::set_field_predicates(
            &mut plan,
            vec![FieldPredicate {
                column: "z1nb".into(),
                op,
                value: 1000.0,
            }],
        );
        let batch = db.execute(&plan).unwrap();
        let kept = (0..batch.num_rows())
            .filter_map(|r| digits(&batch, "z1nb", r))
            .count();
        assert!(
            kept >= at_least,
            "`z1nb {name} 1000.0` kept {kept} rows, fewer than the {at_least} that match — \
             the zone map pruned a row group it should have read"
        );
    }
}

#[test]
fn a_predicate_far_outside_the_range_still_prunes() {
    // The other half: a bound that cannot match must actually skip work,
    // or the widening above has simply disabled pruning.
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed_segments(&db, 40, 10);

    let mut plan = db
        .query()
        .measurement("meter")
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    chronix_query::plan::set_field_predicates(
        &mut plan,
        vec![FieldPredicate {
            column: "z1nb".into(),
            op: ZoneMapOp::Gt,
            value: 9_999_999.0,
        }],
    );
    let batch = db.execute(&plan).unwrap();
    let matching = (0..batch.num_rows())
        .filter_map(|r| digits(&batch, "z1nb", r))
        .count();
    assert_eq!(matching, 0, "nothing is above 9 999 999");
}

// ── Parquet export ─────────────────────────────────────────────────────

#[test]
fn a_decimal_column_exports_to_parquet_as_a_decimal() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed_segments(&db, 20, 20);

    let out = dir.path().join("meter.parquet");
    let plan = db
        .query()
        .measurement("meter")
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    let result = db
        .export_parquet(&plan, &out, &ParquetExportConfig::default())
        .unwrap();
    assert_eq!(result.rows_written, 20);

    // Read it back with a plain Parquet reader — the format has a DECIMAL
    // logical type, so an exported register is still a register to every
    // other tool in the ecosystem, not a double or a string.
    let file = std::fs::File::open(&out).unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(
        batch.schema().field_with_name("z1nb").unwrap().data_type(),
        &arrow::datatypes::DataType::Decimal128(38, 4)
    );
    assert_eq!(digits(&batch, "z1nb", 0).as_deref(), Some("1000.0000"));
    assert_eq!(digits(&batch, "z1nb", 19).as_deref(), Some("1000.0019"));
}

// ── The change stream ──────────────────────────────────────────────────

#[test]
fn a_decimal_reaches_the_change_stream_as_a_decimal() {
    use chronix::chronix_streaming::cdc::{CdcEvent, SubscriptionFilter};

    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    db.declare_field("meter", "z1nb", ColumnType::Decimal { scale: 4 })
        .unwrap();
    let sub = db.subscribe(SubscriptionFilter::default());

    let key = SeriesKey::new("meter", tags! { "dev" => "main" }).unwrap();
    db.insert(&Point::new(key, fields! { "z1nb" => dec("1234.5678") }, 1).unwrap())
        .unwrap();

    let mut sub = sub;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let event = loop {
        if let Some(e) = sub.try_recv() {
            break e;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no change event arrived"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    };
    match event {
        CdcEvent::PointWritten { fields, .. } => match fields.get("z1nb") {
            Some(FieldValue::Decimal(d)) => assert_eq!(d.to_string(), "1234.5678"),
            other => panic!("expected a decimal on the change stream, got {other:?}"),
        },
        other => panic!("expected a write event, got {other:?}"),
    }
}

// ── PromQL ─────────────────────────────────────────────────────────────

#[test]
fn promql_can_graph_a_decimal_series_and_says_it_is_a_float() {
    // PromQL has one numeric type and it is `f64`, so a register selected
    // here *is* converted — this pins that it converts rather than either
    // failing or, worse, returning nothing. Refusing would mean a register
    // cannot be graphed at all; the exactness that matters lives on the
    // storage and SQL paths, which never come this way.
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed_segments(&db, 5, 5);

    let result = db
        .promql("meter_z1nb", 1_004)
        .expect("a decimal series must be selectable from PromQL");
    let samples: Vec<f64> = match &result {
        chronix::promql::ast::PromQLValue::Vector(series) => series
            .iter()
            .flat_map(|s| s.samples.iter().map(|p| p.value))
            .collect(),
        other => panic!("expected an instant vector, got {other:?}"),
    };
    assert_eq!(samples.len(), 1, "one series, one sample: {result:?}");
    assert!(
        (samples[0] - 1000.0004).abs() < 1e-9,
        "got {samples:?} — the register should read back as its value"
    );
}

// ── Deletes ────────────────────────────────────────────────────────────

#[test]
fn deleting_a_range_of_a_decimal_series_leaves_the_rest_exact() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed_segments(&db, 20, 20);

    let request = db
        .delete_builder()
        .measurement("meter")
        .range(1_000, 1_009)
        .build()
        .unwrap();
    db.execute_delete(&request).unwrap();

    let batch = scan(&db, "meter");
    let remaining: Vec<String> = (0..batch.num_rows())
        .filter_map(|r| digits(&batch, "z1nb", r))
        .collect();
    assert_eq!(remaining.len(), 10);
    assert_eq!(remaining[0], "1000.0010");
    assert_eq!(remaining[9], "1000.0019");
}
