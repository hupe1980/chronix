#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Rollup-aware retention: raw data must never be dropped before its
//! aggregate exists.

use chronix::prelude::*;
use chronix::rollup::{RollupAggFn, RollupBuilder};
use chronix::{fields, tags, Chronix};
use std::sync::Arc;

fn open(dir: &tempfile::TempDir) -> Arc<Chronix> {
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    Arc::new(Chronix::open(config).unwrap())
}

/// A segment that cannot be read cannot be rolled up — so retention must
/// preserve it. Dropping it destroys the raw data *and* the aggregate that
/// was supposed to replace it.
#[test]
fn unreadable_segment_is_not_dropped_by_rollup_aware_retention() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);

    db.create_rollup(
        RollupBuilder::new()
            .name("raw_to_1m")
            .source("raw")
            .target("raw_1m")
            .interval_ns(60_000_000_000)
            .aggregation(RollupAggFn::Avg)
            .build()
            .unwrap(),
    )
    .unwrap();

    let key = SeriesKey::new("raw", tags! { "h" => "a" }).unwrap();
    // Old data so retention will consider it expired.
    for i in 0..50i64 {
        db.insert(
            &Point::new(key.clone(), fields! { "v" => i as f64 }, i * 1_000_000_000).unwrap(),
        )
        .unwrap();
    }
    db.flush().unwrap();

    // Find the segment file and corrupt it so it cannot be read.
    let segs: Vec<_> = {
        let cat = db.catalog().read();
        cat.active_segments_for_measurement("raw")
            .iter()
            .map(|e| e.path.clone())
            .collect()
    };
    assert_eq!(segs.len(), 1, "expected exactly one segment");
    let seg_path = segs[0].clone();
    assert!(seg_path.exists());
    // Truncate to garbage — open() will fail the magic/checksum check.
    std::fs::write(&seg_path, b"NOT A SEGMENT").unwrap();

    // Retention with a tiny window: everything is expired.
    let result = db.enforce_retention(1).unwrap();

    assert!(
        seg_path.exists(),
        "retention deleted a segment whose rollup could not be computed — \
         raw data destroyed and no aggregate produced"
    );
    assert_eq!(
        result.segments_deleted, 0,
        "unreadable segment must not be counted as deleted"
    );
}

/// The happy path must still drop data once the aggregate exists.
#[test]
fn readable_segment_is_dropped_after_rollup() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);

    db.create_rollup(
        RollupBuilder::new()
            .name("raw_to_1m")
            .source("raw")
            .target("raw_1m")
            .interval_ns(60_000_000_000)
            .aggregation(RollupAggFn::Avg)
            .build()
            .unwrap(),
    )
    .unwrap();

    let key = SeriesKey::new("raw", tags! { "h" => "a" }).unwrap();
    for i in 0..120i64 {
        db.insert(
            &Point::new(key.clone(), fields! { "v" => i as f64 }, i * 1_000_000_000).unwrap(),
        )
        .unwrap();
    }
    db.flush().unwrap();

    let result = db.enforce_retention(1).unwrap();
    assert!(
        result.segments_deleted > 0,
        "expected expired segments to be dropped"
    );

    // The aggregate survived the drop.
    assert!(
        db.schema("raw_1m").is_some(),
        "rollup target missing after retention"
    );
}

/// hems #2: the 1 s→1 min→15 min cascade. Raw retention trades raw data for
/// the long-retention aggregate, so **every** tier must exist before the raw
/// segment is dropped — not just the first one.
#[test]
fn retention_materialises_the_whole_cascade_before_dropping_raw() {
    const SEC: i64 = 1_000_000_000;
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);

    // 1 s raw → 1 min → 15 min, mirroring hemsd's configuration.
    db.create_rollup(
        RollupBuilder::new()
            .name("raw_to_1m")
            .source("power")
            .target("power_1m")
            .interval_ns(60 * SEC)
            .aggregation(RollupAggFn::Avg)
            .build()
            .unwrap(),
    )
    .unwrap();
    db.create_rollup(
        RollupBuilder::new()
            .name("1m_to_15m")
            .source("power_1m")
            .target("power_15m")
            .interval_ns(900 * SEC)
            .aggregation(RollupAggFn::Avg)
            .build()
            .unwrap(),
    )
    .unwrap();

    // One hour of 1 s samples.
    let key = SeriesKey::new("power", tags! { "meter" => "main" }).unwrap();
    let points: Vec<_> = (0..3600i64)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "watts" => 1000.0 + (i % 100) as f64 },
                i * SEC,
            )
            .unwrap()
        })
        .collect();
    assert!(
        db.insert_batch(&points).unwrap().is_complete(),
        "insert was partial"
    );
    db.flush().unwrap();

    assert!(
        db.schema("power_15m").is_none(),
        "15 min tier should not exist before retention runs"
    );

    let result = db.enforce_retention(1).unwrap();
    assert!(result.segments_deleted > 0, "raw data should have expired");

    // Both tiers must exist — the cascade ran to completion.
    assert!(
        db.schema("power_1m").is_some(),
        "1 min tier missing after retention"
    );
    assert!(
        db.schema("power_15m").is_some(),
        "15 min tier missing — retention dropped raw data while the \
         long-retention aggregate was never materialised"
    );

    // And the 15 min tier holds real aggregated values.
    let plan = db
        .query()
        .measurement("power_15m")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    let rows: usize = db
        .execute_stream(&plan)
        .unwrap()
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum();
    assert!(rows > 0, "15 min tier is empty");
}
