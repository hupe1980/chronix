#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! `execute()` and `execute_stream()` must agree on
//! last-write-wins ordering between the memtable and on-disk segments.
//!
//! The third pairing — **one frozen memtable against another** — is pinned in
//! `chronix_engine::memtable::flush::tests::the_newest_frozen_memtable_wins_a_duplicate`,
//! because two *unflushed* frozen memtables cannot be produced from this level
//! on purpose: the router freezes and drains in one call. They coexist for
//! real in two states — sustained ingest, where the queue sits at its depth of
//! two, and after a **failed flush**, which is where a full disk leaves it —
//! and in both the older value used to win.

use arrow::array::{Array, Float64Array};
use arrow::record_batch::RecordBatch;
use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

fn open(dir: &tempfile::TempDir) -> Chronix {
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    Chronix::open(config).unwrap()
}

fn value_of(batch: &RecordBatch, col: &str) -> Option<f64> {
    let idx = batch.schema().index_of(col).ok()?;
    let arr = batch.column(idx).as_any().downcast_ref::<Float64Array>()?;
    arr.is_valid(0).then(|| arr.value(0))
}

/// A point overwritten in the memtable *after* its original was flushed to a
/// segment must win. Both read paths must report the new value.
#[test]
fn memtable_overwrite_wins_over_flushed_segment() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();

    // Old value → flushed to a segment.
    db.insert(&Point::new(key.clone(), fields! { "v" => 1.0 }, 1_000).unwrap())
        .unwrap();
    db.flush().unwrap();

    // New value for the same (series, ts) → stays in the memtable.
    db.insert(&Point::new(key.clone(), fields! { "v" => 2.0 }, 1_000).unwrap())
        .unwrap();

    let plan = db
        .query()
        .measurement("m")
        .range(0, i64::MAX)
        .build()
        .unwrap();

    let batch = db.execute(&plan).unwrap();
    assert_eq!(batch.num_rows(), 1, "dedup must collapse to one row");
    assert_eq!(
        value_of(&batch, "v"),
        Some(2.0),
        "execute(): the memtable write is newer and must win"
    );

    let chunks = db.execute_stream(&plan).unwrap();
    let total: usize = chunks.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 1, "dedup must collapse to one row");
    assert_eq!(
        value_of(&chunks[0], "v"),
        Some(2.0),
        "execute_stream(): the memtable write is newer and must win"
    );
}

/// The same invariant, seen through SQL (which reads via `execute_stream`).
#[tokio::test]
async fn memtable_overwrite_wins_through_sql() {
    let dir = tempfile::tempdir().unwrap();
    let db = std::sync::Arc::new(open(&dir));
    let key = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();

    db.insert(&Point::new(key.clone(), fields! { "v" => 1.0 }, 1_000).unwrap())
        .unwrap();
    db.flush().unwrap();
    db.insert(&Point::new(key.clone(), fields! { "v" => 2.0 }, 1_000).unwrap())
        .unwrap();

    let ctx = chronix::sql::create_session_context(db);
    let batches = ctx
        .sql("SELECT v FROM m")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 1);
    assert_eq!(value_of(&batches[0], "v"), Some(2.0));
}

/// Two flushed segments: the later flush must win in both read paths.
#[test]
fn later_segment_wins_over_earlier_segment() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();

    db.insert(&Point::new(key.clone(), fields! { "v" => 1.0 }, 1_000).unwrap())
        .unwrap();
    db.flush().unwrap();
    db.insert(&Point::new(key.clone(), fields! { "v" => 2.0 }, 1_000).unwrap())
        .unwrap();
    db.flush().unwrap();

    let plan = db
        .query()
        .measurement("m")
        .range(0, i64::MAX)
        .build()
        .unwrap();

    let batch = db.execute(&plan).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(value_of(&batch, "v"), Some(2.0), "execute()");

    let chunks = db.execute_stream(&plan).unwrap();
    let total: usize = chunks.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 1);
    assert_eq!(value_of(&chunks[0], "v"), Some(2.0), "execute_stream()");
}
