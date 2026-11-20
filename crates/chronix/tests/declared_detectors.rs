#![cfg(feature = "analytics")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! A detector declared like a rollup: persisted, advanced by the store, and
//! resumed — not relearned — after a restart or a crash.

use chronix::detector::{DetectorDeclaration, DetectorStatus, Season};
use chronix::prelude::*;
use chronix::{Chronix, DbError, fields, tags};
use std::time::Duration;

const HOUR: i64 = 3_600_000_000_000;
const DAY: i64 = 24 * HOUR;
/// 2023-01-01T00:00:00Z — two years of history still ends in the past.
const BASE: i64 = 1_672_531_200_000_000_000;

fn config(dir: &std::path::Path) -> ChronixConfig {
    ChronixConfig::builder()
        .data_dir(dir)
        // The test drives every pass itself.
        .maintenance_interval(Duration::ZERO)
        .build()
        .unwrap()
}

fn utc(w: &str) -> TimeBucket {
    TimeBucket::parse(w, None).unwrap()
}

/// A deterministic, bounded pseudo-noise in [-1, 1].
fn noise(i: i64) -> f64 {
    let x = (i.wrapping_mul(2_654_435_761) % 1_000) as f64 / 1_000.0;
    x * 2.0 - 1.0
}

/// Hourly values for `days` days from `from_day`: a daily shape plus noise.
fn write_hours(db: &Chronix, host: &str, from_day: i64, days: i64, scale: f64) {
    let key = SeriesKey::new("pv", tags! { "host" => host }).unwrap();
    let points: Vec<Point> = (from_day * 24..(from_day + days) * 24)
        .map(|h| {
            let hour_of_day = (h % 24) as f64;
            let v = scale * (50.0 + 30.0 * (hour_of_day / 24.0 * std::f64::consts::TAU).sin())
                + noise(h);
            Point::new(key.clone(), fields! { "power" => v }, BASE + h * HOUR).unwrap()
        })
        .collect();
    db.backfill(&points).unwrap().into_complete().unwrap();
}

fn daily(name: &str) -> DetectorDeclaration {
    DetectorDeclaration::builder(name)
        .source("pv", "power")
        .season(Season::new(utc("1h"), utc("1d")).unwrap())
        .zscore(4.0)
        .settle(Duration::ZERO)
        .build()
        .unwrap()
}

fn rows(db: &Chronix, measurement: &str) -> arrow::record_batch::RecordBatch {
    let plan = db
        .query()
        .measurement(measurement)
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    db.execute(&plan).unwrap()
}

fn anomalies(db: &Chronix, measurement: &str) -> Vec<(i64, String)> {
    let b = rows(db, measurement);
    if b.num_rows() == 0 {
        return Vec::new();
    }
    let ts = b
        .column_by_name(chronix_core::TIME_COLUMN)
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let flag = b
        .column_by_name("anomaly")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    let reason = b
        .column_by_name("reason")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    (0..b.num_rows())
        .filter(|&i| flag.value(i))
        .map(|i| (ts.value(i), reason.value(i).to_string()))
        .collect()
}

#[test]
fn a_declared_detector_writes_verdicts_and_is_listed() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(tmp.path())).unwrap();
    // Declared before the first write: a gateway declares at start-up.
    db.declare_detector(daily("pv_health")).unwrap();
    let listed = db.detectors().unwrap();
    assert!(matches!(listed[0].status, DetectorStatus::Inactive { .. }));

    write_hours(&db, "a", 0, 10, 1.0);
    let written = db.advance_detectors().unwrap();
    // 10 days × 24 hours, minus the warm-up: each hourly slot needs 3 days.
    assert_eq!(written, 7 * 24, "one verdict per point past warm-up");

    let b = rows(&db, "pv_health_verdicts");
    for col in [
        "value",
        "baseline",
        "stddev",
        "z",
        "cusum_pos",
        "cusum_neg",
        "anomaly",
        "reason",
        "host",
        "detector",
    ] {
        assert!(
            b.column_by_name(col).is_some(),
            "verdict column {col} missing"
        );
    }
    assert!(
        anomalies(&db, "pv_health_verdicts").is_empty(),
        "clean data raised an alarm"
    );

    let info = &db.detectors().unwrap()[0];
    assert_eq!(info.status, DetectorStatus::Active);
    assert_eq!(info.series, 1);
    assert!(info.watermark_ns.is_some());
    assert_eq!(info.declaration.name, "pv_health");
}

#[test]
fn redeclaring_is_idempotent_and_redefining_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(tmp.path())).unwrap();
    db.declare_detector(daily("d")).unwrap();
    db.declare_detector(daily("d")).unwrap();
    let different = DetectorDeclaration::builder("d")
        .source("pv", "power")
        .zscore(2.0)
        .build()
        .unwrap();
    assert!(matches!(
        db.declare_detector(different),
        Err(DbError::Conflict {
            kind: "detector",
            ..
        })
    ));
    db.drop_detector("d").unwrap();
    assert!(matches!(
        db.drop_detector("d"),
        Err(DbError::NotFound {
            kind: "detector",
            ..
        })
    ));
    assert!(db.detectors().unwrap().is_empty());
}

#[test]
fn dropping_a_detector_keeps_its_verdicts() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(tmp.path())).unwrap();
    write_hours(&db, "a", 0, 5, 1.0);
    db.declare_detector(daily("d")).unwrap();
    db.advance_detectors().unwrap();
    let before = rows(&db, "d_verdicts").num_rows();
    assert!(before > 0);
    db.drop_detector("d").unwrap();
    assert_eq!(rows(&db, "d_verdicts").num_rows(), before);
}

/// The defining property: a restart resumes. No relearning (which would
/// mean three days of silence), and no burst (which would mean re-judging
/// what was already judged).
#[test]
fn a_restart_resumes_rather_than_relearns() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let db = Chronix::open(config(tmp.path())).unwrap();
        db.declare_detector(daily("d")).unwrap();
        write_hours(&db, "a", 0, 10, 1.0);
        assert_eq!(db.advance_detectors().unwrap(), 7 * 24);
        db.close().unwrap();
    }
    let db = Chronix::open(config(tmp.path())).unwrap();
    assert_eq!(
        db.advance_detectors().unwrap(),
        0,
        "a pass with nothing new judged again"
    );
    write_hours(&db, "a", 10, 1, 1.0);
    assert_eq!(
        db.advance_detectors().unwrap(),
        24,
        "day 11 must be judged at once: the baseline survived the restart"
    );
}

#[test]
fn a_fault_is_caught_and_flagged() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(tmp.path())).unwrap();
    db.declare_detector(
        DetectorDeclaration::builder("d")
            .source("pv", "power")
            .season(Season::new(utc("1h"), utc("1d")).unwrap())
            .zscore(4.0)
            .cusum(0.5, 5.0)
            .settle(Duration::ZERO)
            .build()
            .unwrap(),
    )
    .unwrap();
    write_hours(&db, "a", 0, 20, 1.0);
    db.advance_detectors().unwrap();
    assert!(anomalies(&db, "d_verdicts").is_empty());
    // A panel fails: 30 % less from day 20 on.
    write_hours(&db, "a", 20, 3, 0.7);
    db.advance_detectors().unwrap();
    let flagged = anomalies(&db, "d_verdicts");
    assert!(!flagged.is_empty(), "a 30 % drop went unnoticed");
    assert!(
        flagged.iter().all(|(ts, _)| *ts >= BASE + 20 * DAY),
        "flagged before the fault"
    );
}

#[test]
fn a_late_point_is_counted_not_judged() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(tmp.path())).unwrap();
    db.declare_detector(daily("d")).unwrap();
    write_hours(&db, "a", 0, 5, 1.0);
    db.advance_detectors().unwrap();
    let before = rows(&db, "d_verdicts").num_rows();

    let key = SeriesKey::new("pv", tags! { "host" => "a" }).unwrap();
    let late = Point::new(key, fields! { "power" => 1.0 }, BASE + 2 * DAY + 1).unwrap();
    db.backfill(&[late]).unwrap().into_complete().unwrap();
    db.advance_detectors().unwrap();

    assert_eq!(
        rows(&db, "d_verdicts").num_rows(),
        before,
        "a late point was judged"
    );
    assert_eq!(db.detectors().unwrap()[0].late_points, 1);
}

#[test]
fn a_long_backlog_catches_up_over_bounded_passes() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(tmp.path())).unwrap();
    write_hours(&db, "a", 0, 30, 1.0); // 720 points
    db.declare_detector(
        DetectorDeclaration::builder("d")
            .source("pv", "power")
            .season(Season::new(utc("1h"), utc("1d")).unwrap())
            .zscore(4.0)
            .settle(Duration::ZERO)
            .max_points_per_pass(100)
            .build()
            .unwrap(),
    )
    .unwrap();
    let mut passes = 0;
    let mut total = 0;
    loop {
        passes += 1;
        let n = db.advance_detectors().unwrap();
        total += n;
        assert!(n <= 100, "a pass judged {n} points against a cap of 100");
        let wm = db.detectors().unwrap()[0].watermark_ns.unwrap();
        if wm == BASE + 30 * DAY - HOUR {
            break;
        }
        assert!(
            passes <= 10,
            "720 points at 100 per pass took more than 8 passes"
        );
    }
    assert_eq!(
        total,
        27 * 24,
        "every point past warm-up judged exactly once"
    );
}

/// Two years of a monthly PV performance ratio — high in summer, low in
/// winter — learned per month. The winter trough is normal and raises
/// nothing; a 15 % degradation in the second summer is caught.
#[test]
fn a_seasonal_baseline_tolerates_winter_and_catches_degradation() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(tmp.path())).unwrap();
    db.declare_detector(
        DetectorDeclaration::builder("roof")
            .source("ratio", "value")
            .season(Season::new(utc("1mo"), utc("1y")).unwrap())
            .zscore(4.0)
            .cusum(0.5, 5.0)
            .settle(Duration::ZERO)
            .build()
            .unwrap(),
    )
    .unwrap();
    let key = SeriesKey::new("ratio", tags! { "array" => "south" }).unwrap();
    let level = [
        0.35, 0.45, 0.6, 0.75, 0.85, 0.9, 0.9, 0.85, 0.75, 0.6, 0.45, 0.35,
    ];
    let fault_from = 365 + 150; // early June of the second year
    let points: Vec<Point> = (0..730)
        .map(|d| {
            let ts = BASE + d * DAY + 12 * HOUR;
            let month =
                chronix_core::timebucket::civil_from_days(ts.div_euclid(DAY)).1 as usize - 1;
            let fault = if d >= fault_from { 0.85 } else { 1.0 };
            let v = level[month] * fault + 0.01 * noise(d);
            Point::new(key.clone(), fields! { "value" => v }, ts).unwrap()
        })
        .collect();
    db.backfill(&points).unwrap().into_complete().unwrap();
    db.advance_detectors().unwrap();

    let flagged = anomalies(&db, "roof_verdicts");
    let fault_ts = BASE + fault_from * DAY;
    let false_alarms: Vec<_> = flagged.iter().filter(|(ts, _)| *ts < fault_ts).collect();
    assert!(
        false_alarms.is_empty(),
        "the season raised alarms: {false_alarms:?}"
    );
    let caught = flagged.iter().find(|(ts, _)| *ts >= fault_ts);
    let (ts, _) = caught.expect("a 15 % degradation went unnoticed");
    assert!(
        *ts < fault_ts + 14 * DAY,
        "caught only after {} days",
        (ts - fault_ts) / DAY
    );
}

// ── Crash ──────────────────────────────────────────────────────────────

fn crash_child(dir: &std::path::Path) -> ! {
    let db = Chronix::open(config(dir)).unwrap();
    db.declare_detector(daily("d")).unwrap();
    let mut day = 0;
    let deadline = std::time::Instant::now() + Duration::from_millis(800);
    while std::time::Instant::now() < deadline {
        write_hours(&db, "a", day, 1, 1.0);
        let _ = db.advance_detectors();
        day += 1;
    }
    std::process::abort();
}

/// Abort at an unpredictable moment of a pass; after reopening, every
/// source point past warm-up has exactly one verdict — none skipped because
/// a state outlived its verdicts, none doubled.
#[test]
fn a_crash_mid_pass_skips_nothing_and_doubles_nothing() {
    if let Ok(dir) = std::env::var("CHRONIX_DETECTOR_CRASH_DIR") {
        crash_child(std::path::Path::new(&dir));
    }
    let tmp = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "a_crash_mid_pass_skips_nothing_and_doubles_nothing",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("CHRONIX_DETECTOR_CRASH_DIR", tmp.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success(), "the child must abort, not exit cleanly");

    let db = Chronix::open(config(tmp.path())).unwrap();
    db.advance_detectors().unwrap();
    let source = rows(&db, "pv").num_rows();
    assert!(source >= 24 * 4, "the child barely ran");
    let verdicts = rows(&db, "d_verdicts");
    let ts = verdicts
        .column_by_name(chronix_core::TIME_COLUMN)
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let unique: std::collections::BTreeSet<i64> = ts.values().iter().copied().collect();
    assert_eq!(
        unique.len(),
        verdicts.num_rows(),
        "a point was judged twice"
    );
    assert_eq!(
        verdicts.num_rows(),
        source - 3 * 24,
        "every point past warm-up has exactly one verdict"
    );
}

/// `where_tag` confines a detector to some rows of its source — one site,
/// or, on a server, one tenant's namespace.
#[test]
fn a_where_tag_confines_the_detector_to_its_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(tmp.path())).unwrap();
    write_hours(&db, "a", 0, 5, 1.0);
    write_hours(&db, "b", 0, 5, 1.0);
    db.declare_detector(
        DetectorDeclaration::builder("only_a")
            .source("pv", "power")
            .where_tag("host", "a")
            .season(Season::new(utc("1h"), utc("1d")).unwrap())
            .zscore(4.0)
            .settle(Duration::ZERO)
            .build()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(db.advance_detectors().unwrap(), 2 * 24);
    let b = rows(&db, "only_a_verdicts");
    let host = b
        .column_by_name("host")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert!((0..b.num_rows()).all(|i| host.value(i) == "a"));
    assert_eq!(db.detectors().unwrap()[0].series, 1);
}

fn column<'a, T: 'static>(b: &'a arrow::record_batch::RecordBatch, name: &str) -> &'a T {
    b.column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<T>()
        .unwrap()
}

/// A shape that follows the Berlin wall clock — 100 + the local quarter hour
/// exactly — across the 25-hour night of 25 October 2026. Every quarter hour is
/// judged against the same quarter hour of the days before, so nothing is
/// flagged; slots counted by elapsed time would compare 14:00 with 13:00.
#[test]
fn a_wall_clock_shape_survives_the_fall_back_night() {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(dir.path())).unwrap();
    let berlin = |w| TimeBucket::parse(w, Some("Europe/Berlin")).unwrap();
    let quarter = 15 * 60 * 1_000_000_000_i64;
    let clock = berlin("15m");
    let key = SeriesKey::new("load", tags! { "site" => "home" }).unwrap();
    // 2026-10-11 00:00 CEST → 2026-10-28.
    let from = 1_791_669_600_000_000_000_i64;
    let points: Vec<Point> = (0..17 * 96 + 4)
        .map(|i| {
            let ts = from + i * quarter;
            let q = clock.wall_clock_ns(ts).rem_euclid(DAY) / quarter;
            let v = 100.0 + q as f64;
            Point::new(key.clone(), fields! { "kw" => v }, ts).unwrap()
        })
        .collect();
    db.backfill(&points).unwrap().into_complete().unwrap();
    db.declare_detector(
        DetectorDeclaration::builder("load_shape")
            .source("load", "kw")
            .season(Season::new(berlin("15m"), berlin("1d")).unwrap())
            .zscore(4.0)
            .settle(Duration::ZERO)
            .build()
            .unwrap(),
    )
    .unwrap();
    db.advance_detectors().unwrap();
    assert_eq!(anomalies(&db, "load_shape_verdicts"), Vec::new());

    // 14:00 CET on 25 October is 13:00Z: slot 56, as on every other day.
    let b = rows(&db, "load_shape_verdicts");
    let ts = column::<arrow::array::Int64Array>(&b, chronix_core::TIME_COLUMN);
    let slot = column::<arrow::array::Int64Array>(&b, "slot");
    let at = (0..b.num_rows())
        .find(|&i| ts.value(i) == 1_792_933_200_000_000_000)
        .expect("14:00 on the 25-hour day was judged");
    assert_eq!(slot.value(at), 56);
}

/// `.forgetting(1)` halves a slot's past once per year. January 2023 at 10,
/// January 2024 at 20: on 1 January 2025 the baseline is
/// (31·½·10 + 31·20) / (31·½ + 31) = 16.67. Dividing by a nominal year
/// would make 2023 and 2024 one Berlin year and say 15.
#[test]
fn forgetting_halves_once_per_calendar_year() {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(dir.path())).unwrap();
    let berlin = |w| TimeBucket::parse(w, Some("Europe/Berlin")).unwrap();
    let key = SeriesKey::new("ratio", tags! { "array" => "south" }).unwrap();
    let noon = |days_since_2023: i64| BASE + days_since_2023 * DAY + 12 * HOUR;
    let mut points = Vec::new();
    for d in 0..31 {
        points.push(Point::new(key.clone(), fields! { "pr" => 10.0 }, noon(d)).unwrap());
        points.push(Point::new(key.clone(), fields! { "pr" => 20.0 }, noon(365 + d)).unwrap());
    }
    let first_of_2025 = noon(731);
    points.push(Point::new(key.clone(), fields! { "pr" => 20.0 }, first_of_2025).unwrap());
    db.backfill(&points).unwrap().into_complete().unwrap();
    db.declare_detector(
        DetectorDeclaration::builder("roof")
            .source("ratio", "pr")
            .season(Season::new(berlin("1mo"), berlin("1y")).unwrap())
            .cusum(0.5, 1e9) // judge without flagging, so every point teaches
            .forgetting(1)
            .settle(Duration::ZERO)
            .build()
            .unwrap(),
    )
    .unwrap();
    db.advance_detectors().unwrap();
    let b = rows(&db, "roof_verdicts");
    let ts = column::<arrow::array::Int64Array>(&b, chronix_core::TIME_COLUMN);
    let baseline = column::<arrow::array::Float64Array>(&b, "baseline");
    let at = (0..b.num_rows())
        .find(|&i| ts.value(i) == first_of_2025)
        .unwrap();
    let want = (15.5 * 10.0 + 31.0 * 20.0) / 46.5;
    assert!(
        (baseline.value(at) - want).abs() < 1e-9,
        "baseline {} on 2025-01-01, want {want}",
        baseline.value(at)
    );
}

/// An active detector that writes nothing is warming up, and the listing
/// says so; once warm, every verdict names the slot that judged it.
#[test]
fn warming_slots_say_why_there_are_no_verdicts_yet() {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open(config(dir.path())).unwrap();
    db.declare_detector(daily("warm")).unwrap();
    assert_eq!(
        db.detectors().unwrap()[0].warming_slots,
        Some(0),
        "nothing admitted yet"
    );

    write_hours(&db, "a", 0, 2, 1.0);
    db.advance_detectors().unwrap();
    let info = &db.detectors().unwrap()[0];
    assert_eq!(info.status, DetectorStatus::Active);
    assert_eq!(
        info.warming_slots,
        Some(24),
        "two samples of each hour, three needed"
    );
    assert_eq!(rows(&db, "warm_verdicts").num_rows(), 0);

    write_hours(&db, "a", 2, 2, 1.0);
    db.advance_detectors().unwrap();
    assert_eq!(db.detectors().unwrap()[0].warming_slots, Some(0));
    let b = rows(&db, "warm_verdicts");
    assert!(b.num_rows() > 0);
    let ts = column::<arrow::array::Int64Array>(&b, chronix_core::TIME_COLUMN);
    let slot = column::<arrow::array::Int64Array>(&b, "slot");
    for i in 0..b.num_rows() {
        assert_eq!(slot.value(i), (ts.value(i) - BASE).rem_euclid(DAY) / HOUR);
    }
}
