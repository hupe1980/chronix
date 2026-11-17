//! `Chronix::cardinality` reports the set the budget admits against.
//!
//! The numbers a cardinality view shows must be the numbers a refusal
//! reports, so these tests write a known distribution and check every list,
//! then spend the reported headroom and expect the next series refused.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap

use std::collections::BTreeMap;

use chronix::chronix_core::histogram::{Histogram, ResetHint};
use chronix::prelude::*;
use chronix::{CardinalityScope, NameCount, NamespaceLimits, fields};
use chronix_core::NAMESPACE_TAG;
use tempfile::TempDir;

const T0: i64 = 1_700_000_000_000_000_000;

fn open(dir: &TempDir, budget: usize) -> Chronix {
    Chronix::open(
        ChronixConfig::builder()
            .data_dir(dir.path())
            .max_series_cardinality(budget)
            .build()
            .unwrap(),
    )
    .unwrap()
}

fn write(db: &Chronix, measurement: &str, tags: &[(&str, &str)], ts: i64) {
    let tags: BTreeMap<String, String> = tags
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    let key = SeriesKey::new(measurement, tags).unwrap();
    db.insert(&Point::new(key, fields! { "v" => 1.0, "w" => 2.0 }, ts).unwrap())
        .unwrap();
}

fn nc(name: &str, value: u64) -> NameCount {
    NameCount {
        name: name.to_string(),
        value,
    }
}

/// grid: 3 series (device a/b/c, site home); temp: 2 (room x/y, site home);
/// load: 1 (site cabin). `load` has two fields and is still one series.
fn distribution(db: &Chronix) {
    for d in ["a", "b", "c"] {
        write(db, "grid", &[("device", d), ("site", "home")], T0);
    }
    for r in ["x", "y"] {
        write(db, "temp", &[("room", r), ("site", "home")], T0 + 1_000_000);
    }
    write(db, "load", &[("site", "cabin")], T0 + 2_000_000);
}

#[test]
fn every_list_counts_the_known_series() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir, 1_000);
    distribution(&db);

    let s = db.cardinality(10, None).unwrap();
    assert_eq!(s.num_series, 6);
    assert_eq!(s.used, 6);
    assert_eq!(
        s.series_by_measurement,
        vec![nc("grid", 3), nc("temp", 2), nc("load", 1)]
    );
    assert_eq!(
        s.label_value_counts,
        vec![
            nc("__name__", 3),
            nc("device", 3),
            nc("room", 2),
            nc("site", 2)
        ]
    );
    // grid+temp+load = 4+4+4; a+b+c; x+y; home+cabin
    assert_eq!(
        s.label_memory_bytes,
        vec![
            nc("__name__", 12),
            nc("site", 9),
            nc("device", 3),
            nc("room", 2)
        ]
    );
    assert_eq!(
        s.series_by_label_pair,
        vec![
            nc("site=home", 5),
            nc("__name__=grid", 3),
            nc("__name__=temp", 2),
            nc("__name__=load", 1),
            nc("device=a", 1),
            nc("device=b", 1),
            nc("device=c", 1),
            nc("room=x", 1),
            nc("room=y", 1),
            nc("site=cabin", 1),
        ]
    );
    assert_eq!(s.scope, CardinalityScope::Database);
    assert!(s.enforced);
    // Unflushed data counts: bounds come from the memtable too.
    assert_eq!(s.min_time_ms, T0 / 1_000_000);
    assert_eq!(s.max_time_ms, (T0 + 2_000_000) / 1_000_000);
}

#[test]
fn lists_are_truncated_to_the_limit_and_the_limit_is_checked() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir, 1_000);
    distribution(&db);

    let s = db.cardinality(2, None).unwrap();
    assert_eq!(s.series_by_measurement, vec![nc("grid", 3), nc("temp", 2)]);
    assert_eq!(s.series_by_label_pair.len(), 2);
    assert_eq!(s.num_series, 6, "the limit trims lists, not the count");

    assert!(matches!(
        db.cardinality(0, None),
        Err(DbError::InvalidRequest(_))
    ));
    assert!(matches!(
        db.cardinality(10_001, None),
        Err(DbError::InvalidRequest(_))
    ));
    assert!(db.cardinality(10_000, None).is_ok());
}

#[test]
fn an_empty_database_reports_zeroes() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir, 100);
    let s = db.cardinality(10, None).unwrap();
    assert_eq!(s.num_series, 0);
    assert_eq!(s.segment_count, 0);
    assert_eq!((s.min_time_ms, s.max_time_ms), (0, 0));
    assert!(s.series_by_measurement.is_empty());
    assert!(s.label_value_counts.is_empty());
    assert_eq!(s.budget, Some(100));
    assert_eq!(s.headroom, Some(100));
}

#[test]
fn flushed_segments_are_counted_and_bound_the_time_range() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir, 1_000);
    distribution(&db);
    db.flush().unwrap();
    let s = db.cardinality(10, None).unwrap();
    assert_eq!(s.segment_count as usize, db.segment_count());
    assert!(s.segment_count > 0);
    assert_eq!(s.min_time_ms, T0 / 1_000_000);
    assert_eq!(s.max_time_ms, (T0 + 2_000_000) / 1_000_000);
}

#[test]
fn a_native_histogram_is_one_series_and_its_classic_view_is_not_listed() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir, 1_000);
    let mut h = Histogram::empty(3);
    h.reset_hint = ResetHint::NoReset;
    h.observe(0.5);
    let mut fields: BTreeMap<String, FieldValue> = BTreeMap::new();
    fields.insert("latency".into(), FieldValue::Histogram(Box::new(h)));
    let key = SeriesKey::new("http", tags! { "route" => "/api" }).unwrap();
    db.insert(&Point::new(key, fields, T0).unwrap()).unwrap();

    let s = db.cardinality(10, None).unwrap();
    assert_eq!(s.series_by_measurement, vec![nc("http", 1)]);
    let names: Vec<&str> = s
        .series_by_label_pair
        .iter()
        .map(|p| p.name.as_str())
        .collect();
    assert!(
        names
            .iter()
            .all(|n| !n.contains("_bucket") && !n.starts_with("le="))
    );
}

#[test]
fn spending_the_headroom_reaches_the_refusal_exactly() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir, 10);
    distribution(&db);

    let s = db.cardinality(10, None).unwrap();
    assert_eq!((s.budget, s.used, s.headroom), (Some(10), 6, Some(4)));
    for i in 0..s.headroom.unwrap() {
        write(&db, "extra", &[("i", &i.to_string())], T0);
    }
    let refused = db.insert(
        &Point::new(
            SeriesKey::new("extra", tags! { "i" => "one-too-many" }).unwrap(),
            fields! { "v" => 1.0 },
            T0,
        )
        .unwrap(),
    );
    match refused {
        Err(DbError::CardinalityExceeded { current, limit }) => {
            let after = db.cardinality(10, None).unwrap();
            assert_eq!(current as u64, after.used);
            assert_eq!(limit, 10);
            assert_eq!(after.headroom, Some(0));
        }
        other => panic!("expected CardinalityExceeded, got {other:?}"),
    }
}

#[test]
fn a_namespace_sees_only_its_own_series_and_its_own_limit() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir, 1_000);
    for host in ["h1", "h2", "h3"] {
        write(&db, "cpu", &[(NAMESPACE_TAG, "acme"), ("host", host)], T0);
    }
    write(&db, "cpu", &[(NAMESPACE_TAG, "globex"), ("host", "g1")], T0);
    write(
        &db,
        "secret",
        &[(NAMESPACE_TAG, "globex"), ("host", "g2")],
        T0,
    );
    db.set_namespace_limits(
        "acme",
        NamespaceLimits {
            max_series: 5,
            max_measurements: u32::MAX,
        },
    );

    let s = db.cardinality(10, Some("acme")).unwrap();
    assert_eq!(s.scope, CardinalityScope::Namespace("acme".into()));
    assert_eq!(s.num_series, 3);
    assert_eq!(s.used, db.namespace_usage("acme").series);
    assert_eq!((s.budget, s.headroom), (Some(5), Some(2)));
    assert!(s.enforced);
    assert_eq!(s.series_by_measurement, vec![nc("cpu", 3)]);
    let everything: Vec<&str> = s
        .label_value_counts
        .iter()
        .chain(&s.series_by_label_pair)
        .map(|n| n.name.as_str())
        .collect();
    assert!(everything.iter().all(|n| !n.contains(NAMESPACE_TAG)));
    assert!(
        everything
            .iter()
            .all(|n| !n.contains("secret") && !n.contains('g'))
    );

    // No limit set: no budget, no headroom, still exact usage.
    let g = db.cardinality(10, Some("globex")).unwrap();
    assert_eq!((g.budget, g.headroom, g.used), (None, None, 2));
}
