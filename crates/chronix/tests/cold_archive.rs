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

/// Seed `n` one-second-spaced points starting `age` before now, then flush.
///
/// Returns the first timestamp; point `i` is at `base + i` seconds.
///
/// The base is floored to the shard duration so all `n` points land in one
/// shard. Anchoring on a bare `now - age` made the segment count a function of
/// where the wall clock happened to sit inside the hour: the same test wrote
/// one segment or two depending on the time of day.
fn seed(db: &Chronix, measurement: &str, host: &str, n: i64, age: Duration) -> i64 {
    const SHARD_NS: i64 = 3_600 * 1_000_000_000;
    assert!(n < 3_600, "seed must stay inside one shard");

    let now_ns = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
    .unwrap();
    let base = (now_ns - i64::try_from(age.as_nanos()).unwrap()).div_euclid(SHARD_NS) * SHARD_NS;

    let key = SeriesKey::new(measurement, tags! { "host" => host }).unwrap();
    let points: Vec<Point> = (0..n)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "watts" => 100.0 + i as f64 },
                base + i * 1_000_000_000,
            )
            .unwrap()
        })
        .collect();
    assert!(db.insert_batch(&points).unwrap().is_complete());
    db.flush().unwrap();
    base
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

    assert_eq!(outcome.objects, 1, "only the old shard is cold");
    assert_eq!(outcome.segments, 1, "one source segment was dropped");
    assert_eq!(outcome.rows, 100);
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
    let batches = archive_rows(
        &url,
        "power",
        "power_archive",
        "SELECT count(*) AS n FROM power_archive",
    )
    .await;
    assert_eq!(
        scalar_i64(&batches),
        100,
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

// ── The archive is built through the read path ──────────────────────────
//
// An archive built by copying segment bytes is not a copy of what the
// database would answer. Three properties follow from that, each of which
// the byte-copy archiver violated:
//
// 1. A deleted row must stay deleted. Archiving copied the row groups
//    verbatim, so a tombstoned row was written to the archive and then the
//    only thing masking it — the segment it belonged to — was dropped. The
//    delete was undone by a background maintenance operation.
// 2. An overwritten point must be archived once, at its newest value.
//    Two overlapping segments holding the same `(series, timestamp)` are
//    deduplicated last-write-wins on read; archived verbatim, both copies
//    landed in the archive and `count(*)` disagreed with the hot tier.
// 3. The archive must speak the same schema as the hot tier, so that a
//    query moved from one to the other is the same query.

/// Read every row of an archive prefix through DataFusion.
async fn archive_rows(
    url: &str,
    measurement: &str,
    table: &str,
    sql: &str,
) -> Vec<arrow::record_batch::RecordBatch> {
    let ctx = datafusion::prelude::SessionContext::new();
    chronix::sql::cold_tier::register_cold_tier(&ctx, url, measurement, table)
        .await
        .unwrap();
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

fn scalar_i64(batches: &[arrow::record_batch::RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0)
}

/// A tombstoned row must not reappear in the archive.
#[tokio::test]
async fn a_deleted_row_is_not_resurrected_by_archiving() {
    let dir = tempfile::tempdir().unwrap();
    let archive = tempfile::tempdir().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());

    let base = seed(&db, "power", "old", 100, Duration::from_secs(90 * 86_400));

    // Delete the newest 40 of the 100 points: `base + 60s` through `base + 99s`.
    let deleted_from = base + 60 * 1_000_000_000;
    let end = base + 99 * 1_000_000_000;
    let req = db
        .delete_builder()
        .measurement("power")
        .tag("host", "old")
        .range(deleted_from, end)
        .build()
        .unwrap();
    db.execute_delete(&req).unwrap();
    assert_eq!(row_count(&db, "power"), 60, "the delete must apply hot");

    let url = format!("file://{}/", archive.path().display());
    let outcome = db
        .archive_cold_segments(&ArchiveConfig {
            cold_after: Duration::from_secs(30 * 86_400),
            remote_url: url.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(outcome.objects, 1);

    let batches = archive_rows(
        &url,
        "power",
        "power_archive",
        "SELECT count(*) AS n FROM power_archive",
    )
    .await;
    assert_eq!(
        scalar_i64(&batches),
        60,
        "a deleted row must not come back in the archive"
    );
}

/// An overwritten point must be archived once, at its newest value.
#[tokio::test]
async fn overlapping_segments_are_deduplicated_into_the_archive() {
    let dir = tempfile::tempdir().unwrap();
    let archive = tempfile::tempdir().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());

    // Same series, same timestamps, written twice with different values and
    // flushed each time: two overlapping segments, deduplicated on read.
    let base = seed(&db, "power", "dup", 10, Duration::from_secs(90 * 86_400));
    let key = SeriesKey::new("power", tags! { "host" => "dup" }).unwrap();
    let overwrite: Vec<Point> = (0..10)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "watts" => 999.0 },
                base + i * 1_000_000_000,
            )
            .unwrap()
        })
        .collect();
    assert!(db.insert_batch(&overwrite).unwrap().is_complete());
    db.flush().unwrap();

    assert_eq!(db.statistics().segment_count, 2, "two overlapping segments");
    assert_eq!(row_count(&db, "power"), 10, "hot dedups last-write-wins");

    let url = format!("file://{}/", archive.path().display());
    let outcome = db
        .archive_cold_segments(&ArchiveConfig {
            cold_after: Duration::from_secs(30 * 86_400),
            remote_url: url.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(outcome.objects, 1, "one object per (measurement, shard)");
    assert_eq!(outcome.segments, 2, "both source segments were dropped");

    let batches = archive_rows(
        &url,
        "power",
        "dup_archive",
        "SELECT count(*) AS n, sum(watts) AS s FROM dup_archive",
    )
    .await;
    assert_eq!(
        scalar_i64(&batches),
        10,
        "an overwritten point must be archived once, not twice"
    );
    let s = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    assert!(
        (s - 9_990.0).abs() < 1e-6,
        "the archived value must be the winning one, got {s}"
    );
}

/// The archive must carry the hot tier's schema, so a query moved from one
/// to the other is the same query.
#[tokio::test]
async fn the_archive_speaks_the_hot_schema() {
    let dir = tempfile::tempdir().unwrap();
    let archive = tempfile::tempdir().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());
    seed(&db, "power", "old", 20, Duration::from_secs(90 * 86_400));

    let url = format!("file://{}/", archive.path().display());
    db.archive_cold_segments(&ArchiveConfig {
        cold_after: Duration::from_secs(30 * 86_400),
        remote_url: url.clone(),
        ..Default::default()
    })
    .await
    .unwrap();

    // `time_bucket` is the reason this matters: it needs a real timestamp
    // column, and the hot tier calls that column `_time`.
    let batches = archive_rows(
        &url,
        "power",
        "power_archive",
        "SELECT count(*) AS n FROM power_archive \
         WHERE _time > '1970-01-01T00:00:00Z'::timestamp",
    )
    .await;
    assert_eq!(
        scalar_i64(&batches),
        20,
        "the archive's timestamp column must be `_time`, typed as a timestamp"
    );
}

// ── The completeness gates ──────────────────────────────────────────────
//
// Deduplication is only correct over *every* segment covering a range, so a
// group is archived only when nothing else can still speak for its rows.
// Both gates fail in the safe direction: the data stays hot and the next pass
// retries.

/// A group whose rows are all deleted needs no object — but the segments must
/// still go, because carrying tombstone shadow for ever is what the pass is
/// for.
#[tokio::test]
async fn a_fully_deleted_group_is_dropped_without_an_object() {
    let dir = tempfile::tempdir().unwrap();
    let archive = tempfile::tempdir().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());

    seed(&db, "power", "gone", 30, Duration::from_secs(90 * 86_400));
    let req = db
        .delete_builder()
        .measurement("power")
        .tag("host", "gone")
        .build()
        .unwrap();
    db.execute_delete(&req).unwrap();
    assert_eq!(row_count(&db, "power"), 0);

    let url = format!("file://{}/", archive.path().display());
    let outcome = db
        .archive_cold_segments(&ArchiveConfig {
            cold_after: Duration::from_secs(30 * 86_400),
            remote_url: url.clone(),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(outcome.objects, 0, "there is nothing to archive");
    assert_eq!(
        outcome.segments, 1,
        "the empty segment must still be dropped"
    );
    assert_eq!(outcome.rows, 0);
    assert_eq!(outcome.failed, 0);
    assert_eq!(db.statistics().segment_count, 0);
}

/// Unflushed rows overlapping a group would be archived *and* stay hot. The
/// group must be left alone until they are flushed.
#[tokio::test]
async fn a_group_with_unflushed_rows_in_range_stays_hot() {
    let dir = tempfile::tempdir().unwrap();
    let archive = tempfile::tempdir().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());

    let base = seed(&db, "power", "old", 20, Duration::from_secs(90 * 86_400));

    // A late-arriving point inside the sealed segment's range, left in the
    // memtable.
    let key = SeriesKey::new("power", tags! { "host" => "late" }).unwrap();
    let late = Point::new(key, fields! { "watts" => 1.0 }, base + 5_000_000_000).unwrap();
    assert!(db.insert_batch(&[late]).unwrap().is_complete());

    let outcome = db
        .archive_cold_segments(&ArchiveConfig {
            cold_after: Duration::from_secs(30 * 86_400),
            remote_url: format!("file://{}/", archive.path().display()),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(outcome, Default::default(), "nothing may be archived yet");
    assert_eq!(
        row_count(&db, "power"),
        21,
        "all rows stay hot and readable"
    );

    // Once flushed, the group is complete and archives as one object.
    db.flush().unwrap();
    let outcome = db
        .archive_cold_segments(&ArchiveConfig {
            cold_after: Duration::from_secs(30 * 86_400),
            remote_url: format!("file://{}/", archive.path().display()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(outcome.objects, 1);
    assert_eq!(outcome.rows, 21, "the late point must be in the archive");
}

/// Two measurements archive to two partitions, each with its own schema.
///
/// The old layout partitioned by namespace and put every measurement in one
/// flat listing. One Parquet listing table has one schema and `power` and
/// `cpu` do not share one, so that could never have worked for more than one
/// measurement — the second one's objects read as nulls or failed the
/// registration outright.
#[tokio::test]
async fn two_measurements_archive_to_two_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let archive = tempfile::tempdir().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());

    seed(&db, "power", "meter", 20, Duration::from_secs(90 * 86_400));

    // A second measurement with a different field, in the same shard.
    let base = seed(&db, "power", "meter2", 20, Duration::from_secs(90 * 86_400));
    let key = SeriesKey::new("cpu", tags! { "host" => "h1" }).unwrap();
    let points: Vec<Point> = (0..15)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "usage_idle" => 50.0 + i as f64 },
                base + i * 1_000_000_000,
            )
            .unwrap()
        })
        .collect();
    assert!(db.insert_batch(&points).unwrap().is_complete());
    db.flush().unwrap();

    let url = format!("file://{}/", archive.path().display());
    let outcome = db
        .archive_cold_segments(&ArchiveConfig {
            cold_after: Duration::from_secs(30 * 86_400),
            remote_url: url.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(outcome.objects, 2, "one object per (measurement, shard)");
    assert_eq!(outcome.rows, 55, "40 power rows and 15 cpu rows");

    // Each measurement registers as its own table, with its own columns.
    let power = archive_rows(
        &url,
        "power",
        "power_cold",
        "SELECT count(*) AS n, sum(watts) AS s FROM power_cold",
    )
    .await;
    assert_eq!(scalar_i64(&power), 40);

    let cpu = archive_rows(
        &url,
        "cpu",
        "cpu_cold",
        "SELECT count(*) AS n FROM cpu_cold WHERE usage_idle > 55",
    )
    .await;
    assert_eq!(
        scalar_i64(&cpu),
        9,
        "the cpu archive must carry cpu's own columns"
    );
}

/// A measurement that gained a field between flushes archives as one object.
///
/// The writer commits to one schema, and the read path emits whatever columns
/// each segment holds — so a field added after the first flush makes the
/// second bucket's batch a different shape. The archive schema comes from the
/// registry (every column the measurement has ever had) and each batch is
/// aligned to it, with nulls for what a segment predates. Without that the
/// encode fails halfway through, after the group has been read.
#[tokio::test]
async fn a_measurement_whose_schema_grew_archives_as_one_object() {
    let dir = tempfile::tempdir().unwrap();
    let archive = tempfile::tempdir().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());

    let base = seed(&db, "power", "meter", 10, Duration::from_secs(90 * 86_400));

    // A second flush of the same measurement, with an extra field.
    let key = SeriesKey::new("power", tags! { "host" => "meter" }).unwrap();
    let points: Vec<Point> = (10..20)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "watts" => 100.0 + i as f64, "amps" => i as f64 },
                base + i * 1_000_000_000,
            )
            .unwrap()
        })
        .collect();
    assert!(db.insert_batch(&points).unwrap().is_complete());
    db.flush().unwrap();

    let url = format!("file://{}/", archive.path().display());
    let outcome = db
        .archive_cold_segments(&ArchiveConfig {
            cold_after: Duration::from_secs(30 * 86_400),
            remote_url: url.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(outcome.objects, 1, "one shard, one object");
    assert_eq!(outcome.rows, 20);
    assert_eq!(outcome.failed, 0);

    // The rows that predate `amps` carry NULL, not a sentinel.
    let batches = archive_rows(
        &url,
        "power",
        "power_cold",
        "SELECT count(*) AS n, count(amps) AS with_amps FROM power_cold",
    )
    .await;
    assert_eq!(scalar_i64(&batches), 20);
    let with_amps = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        with_amps, 10,
        "the rows written before the field existed must be NULL, not 0.0"
    );
}
