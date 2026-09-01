//! Series-cardinality accounting must survive deletes.
//!
//! The cardinality tracker is the admission gate on the write path: exceed
//! `max_series_cardinality` and every subsequent new series is rejected. That
//! makes the tracker's *downward* accuracy a write-availability property, not
//! a statistics detail — an over-count is a permanent, restart-only outage on
//! a database that creates and drops series over its lifetime, which is
//! exactly what a gateway with rotating device identifiers does.
//!
//! These tests drive the public API (`drop_measurement`, `delete_series`,
//! `statistics`) rather than the internal counters, because the internal
//! counters are what got this wrong: two structures tracked the same fact and
//! only one of them was maintained on the delete path (R1 — two
//! implementations of one semantic diverge silently).

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap

use std::collections::BTreeMap;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};
use tempfile::TempDir;

fn db_with_limit(dir: &TempDir, max_series: usize) -> Chronix {
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .max_series_cardinality(max_series)
        .build()
        .unwrap();
    Chronix::open(config).unwrap()
}

fn point(measurement: &str, host: &str, ts: i64) -> Point {
    let key = SeriesKey::new(measurement, tags! { "host" => host }).unwrap();
    Point::new(key, fields! { "v" => 1.0 }, ts).unwrap()
}

/// Dropping a measurement must return its series to the cardinality budget.
///
/// Without this, a create/drop cycle ratchets the counter upward forever and
/// the database eventually refuses every new series while holding almost no
/// data at all.
#[test]
fn drop_measurement_returns_series_to_the_cardinality_budget() {
    let tmp = TempDir::new().unwrap();
    let db = db_with_limit(&tmp, 4);

    // Fill the budget exactly.
    for i in 0..4 {
        db.insert(&point("m1", &format!("h{i}"), 1_000 + i))
            .unwrap();
    }
    assert_eq!(db.statistics().series_count, 4, "four series are live");

    // Drop them all. The budget must be free again.
    db.drop_measurement("m1").unwrap();
    assert_eq!(
        db.statistics().series_count,
        0,
        "dropping the only measurement must leave zero series counted"
    );

    // The whole budget must be usable again.
    for i in 0..4 {
        db.insert(&point("m2", &format!("h{i}"), 2_000 + i))
            .unwrap_or_else(|e| panic!("series {i} rejected after drop_measurement: {e}"));
    }
    assert_eq!(db.statistics().series_count, 4);
}

/// The same property for a single-series delete.
#[test]
fn delete_series_returns_its_slot_to_the_cardinality_budget() {
    let tmp = TempDir::new().unwrap();
    let db = db_with_limit(&tmp, 2);

    db.insert(&point("m", "a", 1)).unwrap();
    db.insert(&point("m", "b", 2)).unwrap();
    assert_eq!(db.statistics().series_count, 2);

    // At the limit a third series must be refused.
    assert!(
        db.insert(&point("m", "c", 3)).is_err(),
        "the limit must actually bind"
    );

    let mut tags = BTreeMap::new();
    tags.insert("host".to_string(), "a".to_string());
    db.delete_series("m", &tags).unwrap();
    assert_eq!(
        db.statistics().series_count,
        1,
        "the deleted series must stop counting against the limit"
    );

    // The freed slot must be reusable.
    db.insert(&point("m", "c", 3))
        .expect("a freed cardinality slot must be reusable");
}

/// A create/drop cycle must not ratchet the counter upward.
///
/// This is the shape that turns the accounting defect into an outage: each
/// cycle is well inside the limit, but an accumulating counter crosses it.
#[test]
fn repeated_create_and_drop_cycles_do_not_exhaust_the_budget() {
    let tmp = TempDir::new().unwrap();
    let db = db_with_limit(&tmp, 8);

    for cycle in 0..10 {
        for i in 0..8 {
            db.insert(&point(
                "cycling",
                &format!("h{i}"),
                10_000 + cycle * 100 + i,
            ))
            .unwrap_or_else(|e| panic!("cycle {cycle}, series {i} rejected: {e}"));
        }
        assert_eq!(
            db.statistics().series_count,
            8,
            "cycle {cycle}: exactly eight series are live"
        );
        db.drop_measurement("cycling").unwrap();
        assert_eq!(
            db.statistics().series_count,
            0,
            "cycle {cycle}: the drop must return the whole budget"
        );
    }
}

/// `series_count` is an exact number and callers may rely on it as one.
///
/// It backs the `chronix_series_cardinality` gauge and the admission check, so
/// "approximately 1000" is not a useful answer for either.
#[test]
fn series_count_is_exact() {
    let tmp = TempDir::new().unwrap();
    let db = db_with_limit(&tmp, 100_000);

    for i in 0..1_000 {
        db.insert(&point("exact", &format!("host-{i}"), i)).unwrap();
    }
    assert_eq!(
        db.statistics().series_count,
        1_000,
        "cardinality accounting must be exact, not estimated"
    );

    // Re-inserting the same series must not change the count.
    for i in 0..1_000 {
        db.insert(&point("exact", &format!("host-{i}"), 10_000 + i))
            .unwrap();
    }
    assert_eq!(db.statistics().series_count, 1_000);
}
