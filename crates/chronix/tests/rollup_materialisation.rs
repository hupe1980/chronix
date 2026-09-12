#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Rollups are materialised exactly once per bucket, over every row that
//! can ever reach the bucket, and the watermark that says so survives a
//! restart.

use chronix::prelude::*;
use chronix::rollup::{RollupAggFn, RollupBuilder};
use chronix::{fields, tags, Chronix};
use std::time::Duration;

const HOUR: i64 = 3_600_000_000_000;
const MINUTE: i64 = 60_000_000_000;

fn config(dir: &std::path::Path) -> ChronixConfig {
    ChronixConfig::builder()
        .data_dir(dir)
        .shard_duration(std::time::Duration::from_secs(3600))
        .ooo_shard_tolerance(2)
        .build()
        .unwrap()
}

fn rollup(db: &Chronix, name: &str, source: &str, target: &str, interval: i64) {
    db.create_rollup(
        RollupBuilder::new()
            .name(name)
            .source(source)
            .target(target)
            .bucket(TimeBucket::fixed_ns(interval))
            .aggregation(RollupAggFn::Avg)
            .aggregation(RollupAggFn::Max)
            .aggregation(RollupAggFn::Count)
            .group_by("h")
            .build()
            .unwrap(),
    )
    .unwrap();
}

fn write_minute_values(db: &Chronix, host: &str, from_ns: i64, minutes: i64) {
    let key = SeriesKey::new("raw", tags! { "h" => host }).unwrap();
    let points: Vec<Point> = (0..minutes * 60)
        .map(|i| {
            // value = minute index within the write, so avg == minute.
            let v = (i / 60) as f64;
            Point::new(
                key.clone(),
                fields! { "v" => v },
                from_ns + i * 1_000_000_000,
            )
            .unwrap()
        })
        .collect();
    assert!(db.insert_batch(&points).unwrap().is_complete());
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

fn f64_col(batch: &arrow::record_batch::RecordBatch, name: &str, row: usize) -> f64 {
    batch
        .column_by_name(name)
        .expect("column present in the rollup batch")
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(row)
}

fn ts_col(batch: &arrow::record_batch::RecordBatch, row: usize) -> i64 {
    batch
        .column_by_name(chronix_core::TIME_COLUMN)
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(row)
}

/// Only buckets outside the out-of-order window are materialised, their
/// values are exact, and a second call materialises nothing twice.
#[test]
fn materialises_final_buckets_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(dir.path())).unwrap();
    rollup(&db, "raw_to_1m", "raw", "raw_1m", MINUTE);

    // Hours 0..4 of data; the newest write is in shard 4, so with a
    // tolerance of 2 shards everything before shard 2 is final.
    for hour in 0..5 {
        write_minute_values(&db, "a", hour * HOUR, 60);
    }
    db.flush().unwrap();

    let written = db.materialise_rollups().unwrap();
    assert_eq!(written, 120, "two final hours of 1-minute buckets");
    let m1 = scan(&db, "raw_1m");
    assert_eq!(m1.num_rows(), 120);
    for row in 0..120 {
        let minute = (row % 60) as f64;
        assert_eq!(ts_col(&m1, row), (row as i64) * MINUTE, "bucket start");
        assert!((f64_col(&m1, "v_avg", row) - minute).abs() < 1e-9);
        assert_eq!(f64_col(&m1, "v_max", row), minute);
        assert_eq!(f64_col(&m1, "v_count", row), 60.0);
    }

    assert_eq!(db.materialise_rollups().unwrap(), 0, "nothing new is final");

    // Writing further ahead closes the window behind it.
    write_minute_values(&db, "a", 7 * HOUR, 1);
    assert_eq!(db.materialise_rollups().unwrap(), 180, "hours 2, 3 and 4");
    assert_eq!(scan(&db, "raw_1m").num_rows(), 300);
    db.close().unwrap();
}

/// A tier fed by another tier advances exactly as far as its source has.
#[test]
fn a_cascade_stays_consistent_by_construction() {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(dir.path())).unwrap();
    rollup(&db, "raw_to_1m", "raw", "raw_1m", MINUTE);
    rollup(&db, "1m_to_15m", "raw_1m", "raw_15m", 15 * MINUTE);

    for hour in 0..5 {
        write_minute_values(&db, "a", hour * HOUR, 60);
    }
    db.flush().unwrap();
    db.materialise_rollups().unwrap();

    let m15 = scan(&db, "raw_15m");
    assert_eq!(m15.num_rows(), 8, "two final hours of 15-minute buckets");
    // Each 15 min bucket averages fifteen 1 min averages k..k+15 → k + 7.
    for row in 0..8 {
        let k = ((row % 4) * 15) as f64;
        assert!(
            (f64_col(&m15, "v_avg_avg", row) - (k + 7.0)).abs() < 1e-9,
            "{m15:?}"
        );
        assert_eq!(f64_col(&m15, "v_max_max", row), k + 14.0);
        assert_eq!(f64_col(&m15, "v_count_count", row), 15.0);
    }
    db.close().unwrap();
}

/// The watermark is persisted: a restart neither re-materialises nor
/// forgets.
#[test]
fn the_watermark_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Chronix::open(config(dir.path())).unwrap();
        rollup(&db, "raw_to_1m", "raw", "raw_1m", MINUTE);
        for hour in 0..5 {
            write_minute_values(&db, "a", hour * HOUR, 60);
        }
        assert_eq!(db.materialise_rollups().unwrap(), 120);
        db.close().unwrap();
    }
    let db = Chronix::open(config(dir.path())).unwrap();
    assert_eq!(db.materialise_rollups().unwrap(), 0, "already materialised");
    assert_eq!(scan(&db, "raw_1m").num_rows(), 120);
    write_minute_values(&db, "a", 7 * HOUR, 1);
    assert_eq!(db.materialise_rollups().unwrap(), 180);
    db.close().unwrap();
}

/// Two series in one measurement are rolled up separately.
#[test]
fn groups_by_the_configured_tags() {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(dir.path())).unwrap();
    rollup(&db, "raw_to_1m", "raw", "raw_1m", MINUTE);
    write_minute_values(&db, "a", 0, 2);
    write_minute_values(&db, "b", 0, 2);
    write_minute_values(&db, "a", 5 * HOUR, 1);
    db.materialise_rollups().unwrap();
    let m1 = scan(&db, "raw_1m");
    assert_eq!(m1.num_rows(), 4, "two buckets × two hosts: {m1:?}");
    db.close().unwrap();
}

/// `first` and `last` are what turn a meter reading into consumption per
/// bucket — the design partner's request — and re-materialising a range
/// (what a crash between the backfill and the watermark save would cause)
/// changes nothing, because a bucket point overwrites its earlier self.
#[test]
fn first_and_last_are_exact_and_rematerialising_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(dir.path())).unwrap();
    db.create_rollup(
        RollupBuilder::new()
            .name("meter_15m")
            .source("meter")
            .target("meter_15m")
            .bucket(TimeBucket::fixed_ns(15 * MINUTE))
            .aggregation(RollupAggFn::First)
            .aggregation(RollupAggFn::Last)
            .group_by("h")
            .build()
            .unwrap(),
    )
    .unwrap();

    // A monotonically increasing meter, one reading per second for an hour,
    // written out of order inside each minute to make `first` earn its name.
    let key = SeriesKey::new("meter", tags! { "h" => "a" }).unwrap();
    let mut points: Vec<Point> = (0..3600i64)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "kwh" => i as f64 },
                i * 1_000_000_000,
            )
            .unwrap()
        })
        .collect();
    points.reverse();
    assert!(db.insert_batch(&points).unwrap().is_complete());
    write_minute_values(&db, "a", 5 * HOUR, 1);
    db.flush().unwrap();

    assert_eq!(db.materialise_rollups().unwrap(), 4);
    let check = |db: &Chronix| {
        let m = scan(db, "meter_15m");
        assert_eq!(m.num_rows(), 4, "{m:?}");
        for row in 0..4 {
            let start = (row * 900) as f64;
            assert_eq!(f64_col(&m, "kwh_first", row), start);
            assert_eq!(f64_col(&m, "kwh_last", row), start + 899.0);
        }
    };
    check(&db);

    // Re-materialise the same hour: the same four points, overwritten.
    db.create_rollup(
        RollupBuilder::new()
            .name("meter_15m_again")
            .source("meter")
            .target("meter_15m")
            .bucket(TimeBucket::fixed_ns(15 * MINUTE))
            .aggregation(RollupAggFn::First)
            .aggregation(RollupAggFn::Last)
            .group_by("h")
            .build()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(db.materialise_rollups().unwrap(), 4);
    check(&db);
    db.close().unwrap();
}

/// The background pass materialises even when it has nothing to compact —
/// the ordinary state of a gateway that writes a few megabytes an hour and
/// never reaches the compaction trigger.
#[test]
fn compact_materialises_even_with_nothing_to_compact() {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(dir.path())).unwrap();
    rollup(&db, "raw_to_1m", "raw", "raw_1m", MINUTE);
    for hour in 0..5 {
        write_minute_values(&db, "a", hour * HOUR, 60);
    }
    db.flush().unwrap();
    assert_eq!(
        db.compact().unwrap(),
        0,
        "one segment per shard: nothing to compact"
    );
    assert_eq!(
        scan(&db, "raw_1m").num_rows(),
        120,
        "…but the final buckets are materialised"
    );
    db.close().unwrap();
}

/// The real-time view: materialised buckets below the watermark, live
/// buckets above it, one row per bucket either way — and the two halves
/// agree with each other.
#[test]
fn the_rollup_view_covers_the_unmaterialised_tail() {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(dir.path())).unwrap();
    rollup(&db, "raw_to_1m", "raw", "raw_1m", MINUTE);
    for hour in 0..5 {
        write_minute_values(&db, "a", hour * HOUR, 60);
    }
    db.flush().unwrap();
    assert_eq!(
        db.materialise_rollups().unwrap(),
        120,
        "hours 0 and 1 are final"
    );

    // All five hours: 120 materialised + 180 live.
    let view = db.rollup("raw_to_1m", 0, 5 * HOUR - 1).unwrap();
    assert_eq!(view.num_rows(), 300, "{view:?}");
    for row in 0..300 {
        let minute = (row % 60) as f64;
        assert_eq!(ts_col(&view, row), (row as i64) * MINUTE);
        assert!(
            (f64_col(&view, "v_avg", row) - minute).abs() < 1e-9,
            "row {row}"
        );
        assert_eq!(f64_col(&view, "v_count", row), 60.0);
    }

    // A window entirely in the live tail, and one straddling the watermark.
    assert_eq!(
        db.rollup("raw_to_1m", 3 * HOUR, 4 * HOUR - 1)
            .unwrap()
            .num_rows(),
        60
    );
    let straddle = db
        .rollup("raw_to_1m", 2 * HOUR - 30 * MINUTE, 2 * HOUR + 29 * MINUTE)
        .unwrap();
    assert_eq!(straddle.num_rows(), 60);
    assert_eq!(ts_col(&straddle, 0), 2 * HOUR - 30 * MINUTE);

    // Once everything is final the view is the table.
    write_minute_values(&db, "a", 7 * HOUR, 1);
    db.materialise_rollups().unwrap();
    let after = db.rollup("raw_to_1m", 0, 5 * HOUR - 1).unwrap();
    assert_eq!(after.num_rows(), 300);
    assert!(db.rollup("nope", 0, 1).is_err());
    db.close().unwrap();
}

// ─── The invalidation log ─────────────────────────────────────────────
//
// A rollup bucket is aggregated when its input looks final, and "final" is
// a statement about the live write path only. A backfill, a delete or an
// import can change a bucket's input long afterwards. Each of those records
// the range it touched, and the next pass recomputes exactly those buckets.
// Without it the tier is silently stale for ever and retention then drops
// the raw data it was supposed to summarise.

/// A backfill below the watermark makes its buckets right again.
#[test]
fn a_backfill_below_the_watermark_repairs_its_buckets() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(tmp.path())).unwrap();
    rollup(&db, "raw_1m", "raw", "raw_1m", MINUTE);

    // Host b writes hours 0..5, so hours 0..3 fall out of the window and
    // are materialised.
    write_minute_values(&db, "b", 0, 1);
    write_minute_values(&db, "b", 5 * HOUR, 1);
    db.flush().unwrap();
    db.materialise_rollups().unwrap();

    let before = scan(&db, "raw_1m");
    assert_eq!(before.num_rows(), 1, "one bucket for host b");
    assert_eq!(f64_col(&before, "v_count", 0), 60.0);

    // Host a's backlog arrives for the same minute, far below the window.
    let key = SeriesKey::new("raw", tags! { "h" => "a" }).unwrap();
    let points: Vec<Point> = (0..60)
        .map(|i| Point::new(key.clone(), fields! { "v" => 7.0 }, i * 1_000_000_000).unwrap())
        .collect();
    assert!(db.backfill(&points).unwrap().is_complete());
    db.flush().unwrap();

    // The next pass repairs the bucket rather than skipping it.
    db.materialise_rollups().unwrap();
    let after = scan(&db, "raw_1m");
    assert_eq!(after.num_rows(), 2, "host a's bucket is now materialised");
    let mut counts: Vec<f64> = (0..after.num_rows())
        .map(|r| f64_col(&after, "v_count", r))
        .collect();
    counts.sort_by(f64::total_cmp);
    assert_eq!(counts, vec![60.0, 60.0], "both buckets are complete");
    let avgs: Vec<f64> = (0..after.num_rows())
        .map(|r| f64_col(&after, "v_avg", r))
        .collect();
    assert!(
        avgs.contains(&7.0),
        "host a's average is its own, not merged"
    );
    db.close().unwrap();
}

/// A delete below the watermark removes the aggregate it fed. An erasure
/// request that leaves the derived tier untouched has not erased anything.
#[test]
fn a_delete_below_the_watermark_removes_the_aggregate() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(tmp.path())).unwrap();
    rollup(&db, "raw_1m", "raw", "raw_1m", MINUTE);

    write_minute_values(&db, "a", 0, 1);
    write_minute_values(&db, "b", 0, 1);
    write_minute_values(&db, "b", 5 * HOUR, 1);
    db.flush().unwrap();
    db.materialise_rollups().unwrap();
    assert_eq!(scan(&db, "raw_1m").num_rows(), 2);

    db.delete_series("raw", &tags! { "h" => "a" }).unwrap();
    db.materialise_rollups().unwrap();

    let after = scan(&db, "raw_1m");
    assert_eq!(
        after.num_rows(),
        1,
        "the deleted series' aggregate must go with it"
    );
    let hosts = after
        .column_by_name("h")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(hosts.value(0), "b");
    db.close().unwrap();
}

/// `refresh_rollup` recomputes a range on demand — the escape hatch for a
/// change the engine could not have observed.
#[test]
fn refresh_rollup_recomputes_a_range_on_demand() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(tmp.path())).unwrap();
    rollup(&db, "raw_1m", "raw", "raw_1m", MINUTE);
    write_minute_values(&db, "a", 0, 2);
    write_minute_values(&db, "a", 5 * HOUR, 1);
    db.flush().unwrap();
    db.materialise_rollups().unwrap();
    let first = scan(&db, "raw_1m");
    assert_eq!(first.num_rows(), 2);

    // Recomputing changes nothing when the source has not changed.
    db.refresh_rollup("raw_1m", 0, 2 * MINUTE).unwrap();
    let again = scan(&db, "raw_1m");
    assert_eq!(again.num_rows(), 2, "recomputing is idempotent");
    assert_eq!(f64_col(&again, "v_count", 0), 60.0);

    assert!(db.refresh_rollup("nope", 0, 1).is_err());
    db.close().unwrap();
}

/// Rollup definitions and watermarks live in the catalog, which is fsynced
/// and CRC'd. They used to live in a JSON file written without an fsync
/// whose loss was logged and swallowed — leaving a database that believed
/// it had no rollups and a retention pass that dropped the raw data anyway.
#[test]
fn rollup_definitions_survive_a_restart_through_the_catalog() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let db = Chronix::open(config(tmp.path())).unwrap();
        rollup(&db, "raw_1m", "raw", "raw_1m", MINUTE);
        write_minute_values(&db, "a", 0, 1);
        write_minute_values(&db, "a", 5 * HOUR, 1);
        db.flush().unwrap();
        db.materialise_rollups().unwrap();
        db.close().unwrap();
    }
    // No stray state file beside the data directory any more.
    assert!(!tmp.path().join("rollup_registry.json").exists());

    let db = Chronix::open(config(tmp.path())).unwrap();
    assert_eq!(db.list_rollups().unwrap().len(), 1);
    // The watermark came back with it: materialising again writes nothing.
    let before = scan(&db, "raw_1m").num_rows();
    assert_eq!(db.materialise_rollups().unwrap(), 0);
    assert_eq!(scan(&db, "raw_1m").num_rows(), before);
    db.close().unwrap();
}

/// A rollup that would make a measurement feed itself is refused: the chain
/// walk would otherwise loop for ever and retention would wait on a
/// watermark that can never pass.
#[test]
fn a_rollup_cycle_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(tmp.path())).unwrap();
    rollup(&db, "a_to_b", "a", "b", MINUTE);
    rollup(&db, "b_to_c", "b", "c", MINUTE);

    let cycle = RollupBuilder::new()
        .name("c_to_a")
        .source("c")
        .target("a")
        .bucket(TimeBucket::fixed_ns(MINUTE))
        .aggregation(RollupAggFn::Avg)
        .build()
        .unwrap();
    assert!(
        db.create_rollup(cycle).is_err(),
        "c → a closes the loop a → b → c"
    );
    assert_eq!(db.list_rollups().unwrap().len(), 2);
    db.close().unwrap();
}

/// An integer field is rolled up like any other number. It used to be
/// skipped silently — no rollup, no error — and then retention dropped the
/// raw data, so an integer meter reading lost its history entirely.
#[test]
fn an_integer_field_is_rolled_up() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(tmp.path())).unwrap();
    rollup(&db, "raw_1m", "raw", "raw_1m", MINUTE);

    let key = SeriesKey::new("raw", tags! { "h" => "a" }).unwrap();
    let points: Vec<Point> = (0..60)
        .map(|i| Point::new(key.clone(), fields! { "n" => 5_i64 }, i * 1_000_000_000).unwrap())
        .collect();
    assert!(db.insert_batch(&points).unwrap().is_complete());
    write_minute_values(&db, "a", 5 * HOUR, 1);
    db.flush().unwrap();
    db.materialise_rollups().unwrap();

    let batch = scan(&db, "raw_1m");
    let row = (0..batch.num_rows())
        .find(|r| f64_col(&batch, "n_count", *r) > 0.0)
        .expect("the integer field produced a bucket");
    assert_eq!(f64_col(&batch, "n_avg", row), 5.0);
    assert_eq!(f64_col(&batch, "n_count", row), 60.0);
    db.close().unwrap();
}

/// Retention will not drop raw data whose rollup has a repair pending: the
/// aggregate is not yet what the raw data says.
#[test]
fn retention_waits_for_a_pending_repair() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(tmp.path())).unwrap();
    rollup(&db, "raw_1m", "raw", "raw_1m", MINUTE);

    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
    .unwrap();
    let old = now - 48 * HOUR;
    write_minute_values(&db, "a", old, 1);
    write_minute_values(&db, "a", now, 1);
    db.flush().unwrap();
    db.materialise_rollups().unwrap();

    // A backfill into the old hour leaves a repair pending.
    let key = SeriesKey::new("raw", tags! { "h" => "z" }).unwrap();
    let points: Vec<Point> = (0..10)
        .map(|i| Point::new(key.clone(), fields! { "v" => 1.0 }, old + i * 1_000_000_000).unwrap())
        .collect();
    assert!(db.backfill(&points).unwrap().is_complete());
    db.flush().unwrap();

    // A retention pass that runs before the repair must keep the raw data.
    let raw_before = scan(&db, "raw").num_rows();
    let result = db
        .enforce_retention(Duration::from_secs(24 * 3600))
        .unwrap();
    assert!(
        scan(&db, "raw").num_rows() > 0,
        "raw data was dropped while its rollup was still stale ({} segments deleted, {} rows before)",
        result.segments_deleted,
        raw_before
    );
    db.close().unwrap();
}

/// A database that only ever backfills — an import, a restore — still
/// materialises. `backfill` deliberately does not advance the live write
/// window, and the materialiser used to read that window alone, so such a
/// database reported "nothing is final" for ever.
#[test]
fn a_backfill_only_database_still_materialises() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(tmp.path())).unwrap();
    rollup(&db, "raw_1m", "raw", "raw_1m", MINUTE);

    let key = SeriesKey::new("raw", tags! { "h" => "a" }).unwrap();
    // Ten hours of minute-resolution history, nothing through the live path
    // at all. The window is anchored on the newest imported point, so the
    // oldest eight hours are final the moment the import lands.
    let points: Vec<Point> = (0..600)
        .map(|i| Point::new(key.clone(), fields! { "v" => 2.0 }, i * 60 * 1_000_000_000).unwrap())
        .collect();
    assert!(db.backfill(&points).unwrap().is_complete());
    db.flush().unwrap();

    let written = db.materialise_rollups().unwrap();
    assert!(written > 0, "an imported history must be materialised");
    // The newest point is in shard 9, the window covers shards 8 and 9, so
    // the seven whole hours below it are final: 420 minute buckets.
    assert_eq!(
        scan(&db, "raw_1m").num_rows(),
        420,
        "everything below the out-of-order window must be materialised"
    );
    db.close().unwrap();
}

/// A repair that has not run yet survives a restart, and runs afterwards.
///
/// The invalidation log is what decides whether retention may drop raw
/// data, so losing it across a restart would silently un-protect that data.
/// It lives in the catalog manifest for exactly this reason.
#[test]
fn a_pending_repair_survives_a_restart_and_then_runs() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let db = Chronix::open(config(tmp.path())).unwrap();
        rollup(&db, "raw_1m", "raw", "raw_1m", MINUTE);
        write_minute_values(&db, "b", 0, 1);
        write_minute_values(&db, "b", 5 * HOUR, 1);
        db.flush().unwrap();
        db.materialise_rollups().unwrap();
        assert_eq!(scan(&db, "raw_1m").num_rows(), 1);

        // Backfill host a into the already-materialised minute, then close
        // *without* letting the repair run.
        let key = SeriesKey::new("raw", tags! { "h" => "a" }).unwrap();
        let points: Vec<Point> = (0..60)
            .map(|i| Point::new(key.clone(), fields! { "v" => 3.0 }, i * 1_000_000_000).unwrap())
            .collect();
        assert!(db.backfill(&points).unwrap().is_complete());
        assert_eq!(
            db.rollup_state("raw_1m").unwrap().pending_invalidations(),
            &[(0, MINUTE)],
            "the backfill must have marked its bucket"
        );
        db.close().unwrap();
    }

    let db = Chronix::open(config(tmp.path())).unwrap();
    assert_eq!(
        db.rollup_state("raw_1m").unwrap().pending_invalidations(),
        &[(0, MINUTE)],
        "the pending repair was lost by the restart"
    );
    db.materialise_rollups().unwrap();
    assert!(
        db.rollup_state("raw_1m")
            .unwrap()
            .pending_invalidations()
            .is_empty(),
        "the repair should have run"
    );
    assert_eq!(
        scan(&db, "raw_1m").num_rows(),
        2,
        "both series' buckets are now present"
    );
    db.close().unwrap();
}

/// A string field is skipped, and the accumulator says which.
///
/// A rollup aggregates numbers — a string has no `avg` and no `sum` — so the
/// column is left out and the target is narrower than its source. That is
/// the right behaviour and the wrong way to *deliver* it silently: this code
/// has been wrong in exactly this shape twice, once for integer columns and
/// once for decimals, and both times the symptom was a rollup that produced
/// nothing, reported nothing, and was then followed by a retention pass that
/// dropped the raw rows anyway.
///
/// So the accumulator records what it skipped and the materialiser logs it
/// once per pass. This pins the record; the log line is what an operator
/// sees.
#[test]
fn a_rollup_reports_the_field_columns_it_cannot_aggregate() {
    use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let config = RollupBuilder::new()
        .name("hourly")
        .source("device")
        .target("device_hourly")
        .bucket(TimeBucket::fixed_ns(HOUR))
        .aggregation(RollupAggFn::Avg)
        .group_by("h")
        .build()
        .unwrap();

    // One of each: a tag, a numeric field, and a string field.
    let schema = Arc::new(Schema::new(vec![
        Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
        Field::new("h", DataType::Utf8, true),
        Field::new("watts", DataType::Float64, true),
        Field::new("firmware", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![0i64, MINUTE])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "a"])),
            Arc::new(Float64Array::from(vec![1.0, 3.0])),
            Arc::new(StringArray::from(vec!["v1.2", "v1.2"])),
        ],
    )
    .unwrap();

    let mut acc = chronix::rollup::RollupAccumulator::new(&config);
    let _ = acc.push(&batch);
    assert_eq!(
        acc.skipped_columns(),
        vec!["firmware"],
        "the string field is skipped, and the group-by tag is not a skip"
    );

    // And the numeric field is still aggregated — otherwise the assertion
    // above would pass on a rollup that produced nothing at all.
    let points = acc.finish();
    assert!(
        points
            .iter()
            .any(|p| p.field_keys().any(|k| k == "watts_avg")),
        "the numeric field must still be rolled up: {:?}",
        points.first().map(|p| p.field_keys().collect::<Vec<_>>())
    );
}
