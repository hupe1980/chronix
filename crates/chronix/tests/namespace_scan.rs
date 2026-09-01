#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! A namespaced query must see flushed data.
//!
//! `chronixd` injects a hidden `__namespace__` tag into every written point
//! and a matching tag filter into every query, as data-level defence in depth
//! behind the storage-path isolation. That makes *every* HTTP query a
//! tag-filtered query, which is a code path the embedded tests never take —
//! they build plans without a namespace.

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};
use std::sync::Arc;

const NS_TAG: &str = "__namespace__";

fn open(dir: &tempfile::TempDir) -> Arc<Chronix> {
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    Arc::new(Chronix::open(config).unwrap())
}

fn seed(db: &Arc<Chronix>) {
    for i in 0..10i64 {
        let key = SeriesKey::new(
            "m",
            tags! { "host" => format!("h{}", i % 2).as_str(), NS_TAG => "default" },
        )
        .unwrap();
        db.insert(&Point::new(key, fields! { "usage" => i as f64 }, 1_000 + i).unwrap())
            .unwrap();
    }
}

fn count(db: &Arc<Chronix>, plan: &chronix_query::plan::QueryPlan) -> usize {
    db.execute_iter(plan)
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum()
}

/// The whole measurement, queried inside a namespace, after a flush.
#[test]
fn a_namespaced_scan_sees_flushed_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed(&db);

    let plan = db
        .query()
        .measurement("m")
        .range(0, i64::MAX)
        .namespace("default")
        .build()
        .unwrap();

    assert_eq!(count(&db, &plan), 10, "memtable scan sees every row");

    db.flush().unwrap();

    // Both read paths, because they have disagreed before (R1).
    let via_execute = db.execute(&plan).unwrap().num_rows();
    let via_iter = count(&db, &plan);
    assert_eq!(
        (via_execute, via_iter),
        (10, 10),
        "the same query must see the same rows once they are in a segment"
    );
}

/// The same query narrowed by a real tag. This one worked while the one above
/// returned nothing, which is what made the defect look like a filtering bug
/// rather than a projection bug.
#[test]
fn a_namespaced_scan_with_a_tag_filter_sees_flushed_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed(&db);
    db.flush().unwrap();

    let plan = db
        .query()
        .measurement("m")
        .range(0, i64::MAX)
        .namespace("default")
        .tag("host", "h0")
        .build()
        .unwrap();

    assert_eq!(count(&db, &plan), 5);
}

/// A tag filter that names only *some* of a measurement's tags must still
/// return the matching rows.
///
/// This is the general form of the namespace defect, and it has nothing to do
/// with namespaces: series blooms are built from **complete** series keys, and
/// the pruning step built a `SeriesKey` out of whatever tag filters happened
/// to be present. A partial key hashes to something no bloom contains, so
/// every segment was pruned and the query returned nothing at all.
///
/// Every earlier test filtered on a measurement with exactly one tag, where a
/// one-tag filter *is* complete — so the bug was invisible to the whole suite
/// while making `chronixd` return nothing for any flushed data, since it
/// injects a hidden `__namespace__` tag into every point and every query.
#[test]
fn a_partial_tag_filter_still_matches() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);

    for i in 0..10i64 {
        let key = SeriesKey::new(
            "m",
            tags! { "host" => format!("h{}", i % 2).as_str(), "region" => "eu" },
        )
        .unwrap();
        db.insert(&Point::new(key, fields! { "usage" => i as f64 }, 1_000 + i).unwrap())
            .unwrap();
    }
    db.flush().unwrap();

    // `region` alone: a strict subset of {host, region}.
    let plan = db
        .query()
        .measurement("m")
        .range(0, i64::MAX)
        .tag("region", "eu")
        .build()
        .unwrap();
    assert_eq!(
        db.execute(&plan).unwrap().num_rows(),
        10,
        "filtering on one of two tags must match every row that carries it"
    );

    // `host` alone: the other subset.
    let plan = db
        .query()
        .measurement("m")
        .range(0, i64::MAX)
        .tag("host", "h0")
        .build()
        .unwrap();
    assert_eq!(
        db.execute(&plan).unwrap().num_rows(),
        5,
        "and the other partial filter must match its half"
    );

    // Both tags: the complete key, which is the case that always worked.
    let plan = db
        .query()
        .measurement("m")
        .range(0, i64::MAX)
        .tag("host", "h0")
        .tag("region", "eu")
        .build()
        .unwrap();
    assert_eq!(db.execute(&plan).unwrap().num_rows(), 5);
}
