#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Startup declaration is atomic and preserves a populated rollup.

use std::sync::Barrier;
use std::time::Duration;

use chronix::prelude::*;
use chronix::{DbError, RollupConfig, fields, tags};

fn declaration() -> RollupConfig {
    RollupBuilder::new()
        .name("qh")
        .source("power")
        .target("power_15m")
        .every("15m")
        .aggregation(RollupAggFn::Avg)
        .aggregation(RollupAggFn::Count)
        .group_by("host")
        .group_by("point")
        .build()
        .unwrap()
}

// 052-rollup-contract/SR-001
#[test]
fn redeclaration_preserves_the_watermark_and_reports_every_differing_property() {
    const HOUR: i64 = 3_600_000_000_000;
    let dir = tempfile::tempdir().unwrap();
    let mut config = ChronixConfig::small(dir.path());
    config.maintenance_interval = Duration::ZERO;
    let definition = declaration();
    let watermark = {
        let db = Chronix::open(config.clone()).unwrap();
        db.ensure_rollup(definition.clone()).unwrap();
        let key = SeriesKey::new("power", tags! {"host" => "gateway", "point" => "grid"}).unwrap();
        for timestamp in [0, 30 * HOUR] {
            let point = Point::new(key.clone(), fields! { "watts" => 1.0 }, timestamp).unwrap();
            db.insert_batch(&[point]).unwrap().into_complete().unwrap();
        }
        assert_eq!(db.materialise_rollups().unwrap(), 1);
        let watermark = db.rollup_state("qh").unwrap().materialised_until.unwrap();
        db.close().unwrap();
        watermark
    };

    let db = Chronix::open(config).unwrap();
    let mut reordered = definition.clone();
    reordered.aggregations.reverse();
    reordered.group_by_tags.reverse();
    db.ensure_rollup(reordered).unwrap();
    assert!(matches!(
        db.create_rollup(definition.clone()),
        Err(DbError::Conflict { .. })
    ));

    let mut conflicting = definition;
    conflicting.target_measurement = "another_target".into();
    conflicting.source_measurement = "another_source".into();
    conflicting.bucket = TimeBucket::fixed_ns(HOUR);
    conflicting.aggregations.pop();
    conflicting.group_by_tags.pop();
    conflicting.retention_ns = Some(30 * HOUR);
    let err = db.ensure_rollup(conflicting).unwrap_err();
    let DbError::RollupDefinitionConflict { name, fields } = err else {
        panic!("expected definition conflict, got {err}");
    };
    assert_eq!(name, "qh");
    assert_eq!(
        fields,
        [
            "source_measurement",
            "target_measurement",
            "bucket",
            "aggregations",
            "group_by_tags",
            "retention_ns"
        ]
    );
    assert_eq!(db.list_rollups().unwrap().len(), 1);
    assert_eq!(
        db.rollup_state("qh").unwrap().materialised_until,
        Some(watermark)
    );
    assert_eq!(db.materialise_rollups().unwrap(), 0);
    assert_eq!(db.rollup("qh", 0, 0).unwrap().num_rows(), 1);
}

#[test]
fn concurrent_initializers_agree_on_one_definition() {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open(ChronixConfig::small(dir.path())).unwrap();
    let start = Barrier::new(4);
    let results = std::thread::scope(|threads| {
        let handles: Vec<_> = ["even", "odd", "even", "odd"]
            .into_iter()
            .map(|target| {
                let db = &db;
                let start = &start;
                threads.spawn(move || {
                    let mut config = declaration();
                    config.target_measurement = target.into();
                    start.wait();
                    (target, db.ensure_rollup(config))
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    let declarations = db.list_rollups().unwrap();
    assert_eq!(declarations.len(), 1);
    for (target, result) in results {
        if target == declarations[0].target_measurement {
            result.unwrap();
        } else {
            assert!(
                matches!(result, Err(DbError::RollupDefinitionConflict { fields, .. }) if fields == ["target_measurement"])
            );
        }
    }
}

// 052-rollup-contract/SR-003
#[test]
fn failed_catalog_write_does_not_publish_a_declaration_or_poison_retry() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = ChronixConfig::small(dir.path());
    config.maintenance_interval = Duration::ZERO;
    let db = Chronix::open(config.clone()).unwrap();
    let wal = dir.path().join("catalog").join("manifest.wal");
    // The manifest is opened lazily. Simulate an unwritable path independently
    // of the user's filesystem privileges.
    std::fs::create_dir(&wal).unwrap();
    assert!(db.ensure_rollup(declaration()).is_err());
    assert!(db.list_rollups().unwrap().is_empty());
    std::fs::remove_dir(&wal).unwrap();
    db.ensure_rollup(declaration()).unwrap();
    db.close().unwrap();
    drop(db);
    let reopened = Chronix::open(config).unwrap();
    assert_eq!(reopened.list_rollups().unwrap().len(), 1);
    reopened.ensure_rollup(declaration()).unwrap();
}

#[test]
fn simultaneous_maintenance_calls_materialise_the_bucket_once() {
    const HOUR: i64 = 3_600_000_000_000;
    let dir = tempfile::tempdir().unwrap();
    let mut config = ChronixConfig::small(dir.path());
    config.maintenance_interval = Duration::ZERO;
    let db = Chronix::open(config).unwrap();
    db.ensure_rollup(declaration()).unwrap();
    let key = SeriesKey::new("power", tags! {"host" => "gateway", "point" => "grid"}).unwrap();
    for timestamp in [0, 30 * HOUR] {
        let point = Point::new(key.clone(), fields! { "watts" => 1.0 }, timestamp).unwrap();
        db.insert_batch(&[point]).unwrap().into_complete().unwrap();
    }

    let start = Barrier::new(4);
    std::thread::scope(|threads| {
        let handles: Vec<_> = (0..4)
            .map(|_| {
                threads.spawn(|| {
                    start.wait();
                    db.materialise_rollups().unwrap()
                })
            })
            .collect();
        let written: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(written, 1);
    });
}
