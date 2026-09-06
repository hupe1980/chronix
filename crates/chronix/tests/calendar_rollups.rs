#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! A rollup tier can be a *calendar* tier, and it aggregates the calendar.
//!
//! The tiers people actually ask a time-series database for are "per day" and
//! "per month" — a daily energy total, a monthly bill, a § 14a evidence
//! record — and neither could be declared. A rollup's interval was an `i64` of
//! nanoseconds, so:
//!
//! - `86_400_000_000_000` is a *UTC* day. In Berlin that runs 02:00 to 02:00
//!   local in summer and 01:00 to 01:00 in winter, so every "daily" total was
//!   a day's worth of somebody else's day, and the two halves of the year did
//!   not even agree with each other.
//! - A month is not a fixed number of nanoseconds at all, so a monthly tier
//!   was **unrepresentable**.
//!
//! These drive the real materialiser end to end, because the arithmetic that
//! had to change is not the bucketing — it is every place that used to reach
//! the next bucket by *adding the width*, which a calendar bucket does not
//! have.

use chronix::prelude::*;
use chronix::rollup::{RollupAggFn, RollupBuilder};
use chronix::{fields, tags, Chronix};

const HOUR: i64 = 3_600_000_000_000;

fn open(dir: &std::path::Path) -> Chronix {
    Chronix::open(
        ChronixConfig::builder()
            .data_dir(dir)
            .shard_duration(std::time::Duration::from_secs(6 * 3600))
            // The tiers under test span months, so nothing may be refused for
            // being outside the out-of-order window.
            .ooo_shard_tolerance(2)
            .build()
            .unwrap(),
    )
    .unwrap()
}

fn ts(s: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(s)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap()
}

/// One point per hour over `hours`, each worth 1.0, backfilled so the
/// out-of-order window never refuses history.
fn write_hourly(db: &Chronix, from: i64, hours: i64) {
    let key = SeriesKey::new("meter", tags! { "h" => "a" }).unwrap();
    let points: Vec<Point> = (0..hours)
        .map(|i| Point::new(key.clone(), fields! { "v" => 1.0 }, from + i * HOUR).unwrap())
        .collect();
    db.backfill(&points).unwrap().into_complete().unwrap();
}

/// Every `(bucket_start, count)` a materialised tier holds.
fn buckets(db: &Chronix, measurement: &str) -> Vec<(i64, i64)> {
    let plan = db
        .query()
        .measurement(measurement)
        .range(i64::MIN + 1, i64::MAX - 1)
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    if batch.num_rows() == 0 {
        return Vec::new();
    }
    let times = batch
        .column_by_name(chronix_core::TIME_COLUMN)
        .expect("the time column")
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("timestamps are Int64 in a scan batch")
        .clone();
    let counts = batch
        .column_by_name("v_count")
        .expect("the count column")
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("aggregates are Float64")
        .clone();
    #[allow(clippy::cast_possible_truncation)]
    let mut out: Vec<(i64, i64)> = (0..batch.num_rows())
        .map(|i| (times.value(i), counts.value(i) as i64))
        .collect();
    out.sort_unstable();
    out
}

/// A daily tier in a zone buckets on **local** midnight, and its transition
/// days are 23 and 25 hours.
///
/// The property that makes this worth a tier rather than a query: the count
/// per bucket is the number of hourly readings in that *local* day, which is
/// 23 in spring and 25 in autumn. A fixed 24-hour bucket cannot produce
/// either, and produces a boundary two hours into the local day besides.
#[test]
fn a_daily_tier_in_a_zone_follows_the_local_calendar() {
    let tmp = tempfile::tempdir().unwrap();
    let db = open(tmp.path());

    // Berlin, 2026: spring forward 29 March, fall back 25 October.
    // Cover both, plus ordinary days either side.
    db.create_rollup(
        RollupBuilder::new()
            .name("daily")
            .source("meter")
            .target("meter_daily")
            .every("1d")
            .timezone("Europe/Berlin")
            .aggregation(RollupAggFn::Count)
            .group_by("h")
            .build()
            .unwrap(),
    )
    .unwrap();

    // 27 March 00:00 local (CET, 23:00Z on the 26th) through 2 April.
    write_hourly(&db, ts("2024-03-28T23:00:00Z"), 7 * 24);
    db.flush().unwrap();
    db.materialise_rollups().unwrap();

    let got = buckets(&db, "meter_daily");
    assert!(!got.is_empty(), "the tier materialised something");

    // The 29th of March is a 23-hour day in Berlin. Find its bucket by its
    // local midnight, 2026-03-28T23:00:00Z.
    let spring = ts("2024-03-30T23:00:00Z");
    let (start, count) = got
        .iter()
        .find(|(s, _)| *s == spring)
        .copied()
        .unwrap_or_else(|| panic!("no bucket at Berlin's 31 March midnight; got {got:?}"));
    assert_eq!(start, spring, "the boundary is local midnight, not 00:00Z");
    assert_eq!(count, 23, "31 March 2024 is a 23-hour day in Berlin");

    // An ordinary day either side is 24.
    for day in ["2024-03-29T23:00:00Z", "2024-04-01T22:00:00Z"] {
        let want = ts(day);
        let (_, count) = got
            .iter()
            .find(|(s, _)| *s == want)
            .copied()
            .unwrap_or_else(|| panic!("no bucket at {day}; got {got:?}"));
        assert_eq!(count, 24, "{day} is an ordinary 24-hour day");
    }
}

/// The autumn half: a 25-hour day.
///
/// Kept separate from the spring case because the two failed differently —
/// a fixed bucket loses an hour in one direction and doubles one in the other.
#[test]
fn a_fall_back_day_is_twenty_five_hours_in_its_tier() {
    let tmp = tempfile::tempdir().unwrap();
    let db = open(tmp.path());
    db.create_rollup(
        RollupBuilder::new()
            .name("daily")
            .source("meter")
            .target("meter_daily")
            .every("1d")
            .timezone("Europe/Berlin")
            .aggregation(RollupAggFn::Count)
            .group_by("h")
            .build()
            .unwrap(),
    )
    .unwrap();

    // 23 October 00:00 local (CEST, 22:00Z on the 22nd) through 29 October.
    write_hourly(&db, ts("2024-10-24T22:00:00Z"), 7 * 24);
    db.flush().unwrap();
    db.materialise_rollups().unwrap();

    let got = buckets(&db, "meter_daily");
    let autumn = ts("2024-10-26T22:00:00Z"); // 27 October, Berlin local midnight
    let (_, count) = got
        .iter()
        .find(|(s, _)| *s == autumn)
        .copied()
        .unwrap_or_else(|| panic!("no bucket at Berlin's 27 October midnight; got {got:?}"));
    assert_eq!(count, 25, "27 October 2024 is a 25-hour day in Berlin");
}

/// A monthly tier — the shape a bill or a § 14a evidence record has, and one
/// that could not be declared at all.
///
/// February is the test that matters: 28 days where the nominal month is
/// 30.44, so anything that reached the next bucket by adding a width would
/// straddle the boundary.
#[test]
fn a_monthly_tier_aggregates_calendar_months() {
    let tmp = tempfile::tempdir().unwrap();
    let db = open(tmp.path());
    db.create_rollup(
        RollupBuilder::new()
            .name("monthly")
            .source("meter")
            .target("meter_monthly")
            .every("1mo")
            .timezone("Europe/Berlin")
            .aggregation(RollupAggFn::Count)
            .group_by("h")
            .build()
            .unwrap(),
    )
    .unwrap();

    // 1 January 2024 local (CET, 23:00Z on 31 Dec 2023) through the end of
    // April: a bucket is materialised only once it is **final**, so the data
    // has to run past March for March's bucket to close.
    let start = ts("2023-12-31T23:00:00Z");
    write_hourly(&db, start, 121 * 24);
    db.flush().unwrap();
    db.materialise_rollups().unwrap();

    let got = buckets(&db, "meter_monthly");
    let want = [
        ("2023-12-31T23:00:00Z", 31 * 24), // January 2024
        ("2024-01-31T23:00:00Z", 29 * 24), // February 2024 — a leap year
        ("2024-02-29T23:00:00Z", 31 * 24), // March 2024, less the spring hour
    ];
    for (i, (iso, hours)) in want.iter().enumerate() {
        let bucket = ts(iso);
        let (_, count) = got
            .iter()
            .find(|(s, _)| *s == bucket)
            .copied()
            .unwrap_or_else(|| panic!("no month bucket at {iso}; got {got:?}"));
        // March 2027 crosses the spring transition, so it holds one hourly
        // reading fewer than its 31 days suggest.
        let expected = if i == 2 { hours - 1 } else { *hours };
        assert_eq!(count, expected, "{iso}");
    }
}

/// A calendar tier survives a restart with its zone.
///
/// The catalog is `postcard`, which is not self-describing: a zone that was
/// skipped when absent made every later rollup in the file unreadable. JSON
/// round-trips hid it completely.
#[test]
fn a_calendar_tier_survives_a_restart_with_its_zone() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let db = open(tmp.path());
        // A tier with no zone *before* one with a zone, so a mis-encoded
        // optional field corrupts what follows it.
        db.create_rollup(
            RollupBuilder::new()
                .name("hourly")
                .source("meter")
                .target("meter_hourly")
                .every("1h")
                .aggregation(RollupAggFn::Count)
                .build()
                .unwrap(),
        )
        .unwrap();
        db.create_rollup(
            RollupBuilder::new()
                .name("monthly")
                .source("meter")
                .target("meter_monthly")
                .every("1mo")
                .timezone("Europe/Berlin")
                .aggregation(RollupAggFn::Count)
                .build()
                .unwrap(),
        )
        .unwrap();
        write_hourly(&db, ts("2023-12-31T23:00:00Z"), 48);
        db.flush().unwrap();
        db.close().unwrap();
    }

    let db = open(tmp.path());
    let rollups = db.list_rollups().unwrap();
    let monthly = rollups
        .iter()
        .find(|r| r.name == "monthly")
        .expect("the monthly tier survived");
    assert_eq!(monthly.bucket.width().to_string(), "1mo");
    assert_eq!(monthly.bucket.timezone(), Some("Europe/Berlin"));
    let hourly = rollups
        .iter()
        .find(|r| r.name == "hourly")
        .expect("the hourly tier survived");
    assert_eq!(hourly.bucket.width().to_string(), "1h");
    assert_eq!(hourly.bucket.timezone(), None);
}

/// A late write into a calendar bucket invalidates *that whole bucket*.
///
/// The invalidation range is computed from the bucket the write lands in, and
/// reaching its end used to be `start + width`. For a monthly tier that is
/// 30.44 days, which ends inside the next month for a 31-day one and short of
/// the end for a 28-day one — so a repair would rewrite part of the wrong
/// bucket and leave part of the right one stale.
#[test]
fn a_late_write_repairs_the_whole_calendar_bucket() {
    let tmp = tempfile::tempdir().unwrap();
    let db = open(tmp.path());
    db.create_rollup(
        RollupBuilder::new()
            .name("monthly")
            .source("meter")
            .target("meter_monthly")
            .every("1mo")
            .timezone("Europe/Berlin")
            .aggregation(RollupAggFn::Count)
            .group_by("h")
            .build()
            .unwrap(),
    )
    .unwrap();

    // January through March 2024: February's bucket has to be **final**
    // before it is materialised, so the data runs past it.
    write_hourly(&db, ts("2023-12-31T23:00:00Z"), 91 * 24);
    db.flush().unwrap();
    db.materialise_rollups().unwrap();

    let before = buckets(&db, "meter_monthly");
    let feb = ts("2024-01-31T23:00:00Z");
    let (_, feb_before) = before.iter().find(|(s, _)| *s == feb).copied().unwrap();
    assert_eq!(feb_before, 29 * 24);

    // A reading arriving late for the *last hour of February*, which is the
    // one an `start + nominal width` range would have missed.
    let key = SeriesKey::new("meter", tags! { "h" => "a" }).unwrap();
    db.backfill(&[Point::new(key, fields! { "v" => 1.0 }, ts("2024-02-29T22:30:00Z")).unwrap()])
        .unwrap()
        .into_complete()
        .unwrap();
    db.flush().unwrap();
    db.materialise_rollups().unwrap();

    let after = buckets(&db, "meter_monthly");
    let (_, feb_after) = after
        .iter()
        .find(|(s, _)| *s == feb)
        .copied()
        .unwrap_or_else(|| panic!("February's bucket vanished; got {after:?}"));
    assert_eq!(
        feb_after,
        feb_before + 1,
        "the late reading was folded into February, not into March"
    );

    // …and March is untouched.
    assert!(
        after.iter().all(|(s, _)| *s != ts("2024-02-29T23:00:00Z")),
        "no March bucket exists yet, so the repair did not invent one: {after:?}"
    );
}
