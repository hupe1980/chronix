#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! `last_value()` must return the newest point for a series, including
//! when a late (out-of-order) write has landed in the memtable behind an
//! already-flushed newer point.

use std::collections::BTreeMap;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

const HOUR_NS: i64 = 3_600_000_000_000;

fn open_without_lvc(dir: &tempfile::TempDir) -> Chronix {
    // The last-value cache answers correctly on its own; this exercises the
    // memtable/segment fallback that runs whenever the cache misses (cold
    // start, eviction, or a measurement outside `lvc_measurements`).
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .enable_last_value_cache(false)
        .build()
        .unwrap();
    Chronix::open(config).unwrap()
}

fn value(db: &Chronix, ts_expected: i64) {
    let tags: BTreeMap<String, String> = BTreeMap::from([("h".to_string(), "a".to_string())]);
    let got = db.last_value("m", &tags).unwrap().expect("a point");
    assert_eq!(
        got.timestamp(),
        ts_expected,
        "last_value returned a stale point"
    );
}

#[test]
fn late_write_does_not_shadow_a_newer_flushed_point() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_without_lvc(&dir);
    let key = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();

    // Newest point, flushed to a segment.
    let newest = 2 * HOUR_NS;
    db.insert(&Point::new(key.clone(), fields! { "v" => 2.0 }, newest).unwrap())
        .unwrap();
    db.flush().unwrap();

    // A late arrival within the out-of-order tolerance stays in the memtable.
    let late = HOUR_NS;
    db.insert(&Point::new(key.clone(), fields! { "v" => 1.0 }, late).unwrap())
        .unwrap();

    value(&db, newest);
}

#[test]
fn memtable_point_wins_when_it_is_actually_newer() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_without_lvc(&dir);
    let key = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();

    db.insert(&Point::new(key.clone(), fields! { "v" => 1.0 }, HOUR_NS).unwrap())
        .unwrap();
    db.flush().unwrap();
    db.insert(&Point::new(key.clone(), fields! { "v" => 2.0 }, 2 * HOUR_NS).unwrap())
        .unwrap();

    value(&db, 2 * HOUR_NS);
}

#[test]
fn segment_only_series_reads_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_without_lvc(&dir);
    let key = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();

    db.insert(&Point::new(key.clone(), fields! { "v" => 1.0 }, HOUR_NS).unwrap())
        .unwrap();
    db.insert(&Point::new(key.clone(), fields! { "v" => 2.0 }, 2 * HOUR_NS).unwrap())
        .unwrap();
    db.flush().unwrap();

    value(&db, 2 * HOUR_NS);
}
