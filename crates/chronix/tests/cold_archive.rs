#![cfg(feature = "object-store")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! The cold tier as a whole operation: archive, then read it back.
//!
//! The subsystem used to have no operation at all. `TieringEngine` uploaded a
//! segment and deleted the local file — by default — while nothing anywhere
//! updated the catalog, so the only documented way to use the feature left the
//! database pointing at a file that was gone. The read path had a branch for
//! remote catalog entries that nothing could produce, and the trait that would
//! have fetched them had no implementation in the tree.
//!
//! These tests pin the contract the operation now has: archived data leaves
//! the hot database, stays queryable through the archive table, and a failed
//! upload leaves the segment exactly where it was.

use std::sync::Arc;
use std::time::Duration;

use chronix::cold_archive::ArchiveConfig;
use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

/// Seed `n` points ending `age` before now, then flush them to a segment.
fn seed(db: &Chronix, measurement: &str, host: &str, n: i64, age: Duration) -> i64 {
    let now_ns = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
    .unwrap();
    let end = now_ns - i64::try_from(age.as_nanos()).unwrap();
    let key = SeriesKey::new(measurement, tags! { "host" => host }).unwrap();
    let points: Vec<Point> = (0..n)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "watts" => 100.0 + i as f64 },
                end - (n - i) * 1_000_000_000,
            )
            .unwrap()
        })
        .collect();
    assert!(db.insert_batch(&points).unwrap().is_complete());
    db.flush().unwrap();
    end
}

fn row_count(db: &Chronix, measurement: &str) -> usize {
    let plan = db.query().measurement(measurement).build().unwrap();
    db.execute_iter(&plan)
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum()
}

#[tokio::test]
async fn archiving_moves_a_segment_out_of_the_hot_database() {
    let dir = tempfile::tempdir().unwrap();
    let archive = tempfile::tempdir().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());

    // One old segment (archivable) and one recent one (not).
    seed(&db, "power", "old", 100, Duration::from_secs(90 * 86_400));
    seed(&db, "power", "new", 100, Duration::from_secs(60));
    assert_eq!(row_count(&db, "power"), 200);
    let segments_before = db.statistics().segment_count;
    assert_eq!(segments_before, 2, "one segment per flush");

    let url = format!("file://{}/", archive.path().display());
    let outcome = db
        .archive_cold_segments(&ArchiveConfig {
            cold_after: Duration::from_secs(30 * 86_400),
            remote_url: url.clone(),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(outcome.segments, 1, "only the old segment is cold");
    assert_eq!(outcome.failed, 0);
    assert!(outcome.bytes > 0);

    // The hot database no longer holds it — catalog, index and file.
    assert_eq!(db.statistics().segment_count, 1);
    assert_eq!(
        row_count(&db, "power"),
        100,
        "archived rows must leave the hot tier"
    );

    // And it survived the trip: the archive is queryable as its own table.
    let ctx = datafusion::prelude::SessionContext::new();
    chronix::sql::cold_tier::register_cold_tier(&ctx, &url, "power_archive")
        .await
        .unwrap();
    let batches = ctx
        .sql("SELECT count(*) AS n FROM power_archive")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        n, 100,
        "every archived row must be readable from the archive"
    );
}

/// Nothing to archive is not an error, and must not touch anything.
#[tokio::test]
async fn nothing_is_archived_when_no_segment_is_old_enough() {
    let dir = tempfile::tempdir().unwrap();
    let archive = tempfile::tempdir().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());
    seed(&db, "power", "recent", 50, Duration::from_secs(60));

    let outcome = db
        .archive_cold_segments(&ArchiveConfig {
            cold_after: Duration::from_secs(30 * 86_400),
            remote_url: format!("file://{}/", archive.path().display()),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(outcome, Default::default());
    assert_eq!(row_count(&db, "power"), 50);
}

/// A failed upload must leave the segment hot and readable.
///
/// This is the direction that matters: the step after a successful archive is
/// a delete, so "we could not verify the upload" has to mean "keep it", not
/// "carry on". The old engine logged a warning and returned success.
#[tokio::test]
async fn a_failed_upload_leaves_the_segment_in_the_hot_tier() {
    let dir = tempfile::tempdir().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());
    seed(&db, "power", "old", 100, Duration::from_secs(90 * 86_400));

    // A file:// URL under a path that cannot be created — the parent is a
    // regular file, so every upload fails.
    let blocker = dir.path().join("not-a-dir");
    std::fs::write(&blocker, b"x").unwrap();
    let url = format!("file://{}/archive/", blocker.display());

    let outcome = db
        .archive_cold_segments(&ArchiveConfig {
            cold_after: Duration::from_secs(30 * 86_400),
            remote_url: url,
            ..Default::default()
        })
        .await;

    // Either the store refuses to open or the upload fails; both must leave
    // the data hot.
    if let Ok(outcome) = outcome {
        assert_eq!(outcome.segments, 0, "nothing may be reported as archived");
        assert_eq!(outcome.failed, 1);
    }
    assert_eq!(
        row_count(&db, "power"),
        100,
        "a failed archive must not remove data"
    );
}
