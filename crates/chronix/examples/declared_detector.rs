//! A roof that slowly stops delivering — caught by a detector the store
//! keeps, against a baseline that knows December from June.
//!
//! The case is a home energy gateway's plant-health monitor. A PV array's
//! raw output is useless to watch: every cloudy day and every winter looks
//! like a fault. What is watched is the **performance ratio** — actual
//! production over what a physical model says a clean, healthy array would
//! produce — and even that has a season (low sun, snow, soiling), so it is
//! judged against *this month's* normal, learned from last year.
//!
//! Three steps, and only the first is the application's:
//!
//! 1. **Materialise the derived signal.** `actual / modelled` is not a stored
//!    column, so the application writes it as its own measurement, once a
//!    day. (The model here is a toy; a real one is a clear-sky model per
//!    slot.)
//! 2. **Declare the detector** — once, idempotently, on every start.
//! 3. **Read the verdicts** — ordinary rows, queryable, subscribable.
//!
//! The store learns, persists and advances the baseline; a reboot resumes
//! it. Four hundred simulated days run through the real engine below, with a
//! 15 % degradation from day 380; the example asserts no seasonal false
//! alarm before it and a catch within days after it.
//!
//! Run: `cargo run -p chronix --example declared_detector`

use std::time::Duration;

use chronix::detector::{DetectorDeclaration, Season};
use chronix::prelude::*;
use chronix::{Chronix, fields, tags};

const DAY: i64 = 86_400_000_000_000;
/// 2023-01-01T12:00:00Z.
const START: i64 = 1_672_574_400_000_000_000;
const DAYS: i64 = 400;
const FAULT_FROM: i64 = 380;

/// A healthy array's performance ratio by month: lower in winter (low sun,
/// snow, more diffuse light), higher in late spring.
const HEALTHY: [f64; 12] = [
    0.62, 0.66, 0.74, 0.80, 0.84, 0.83, 0.82, 0.81, 0.78, 0.72, 0.65, 0.60,
];

/// Deterministic day-to-day scatter of ±0.03.
fn scatter(day: i64) -> f64 {
    let x = (day.wrapping_mul(2_654_435_761) % 1_000) as f64 / 1_000.0;
    (x * 2.0 - 1.0) * 0.03
}

fn month_of(ts: i64) -> usize {
    chrono::DateTime::from_timestamp_nanos(ts)
        .format("%m")
        .to_string()
        .parse::<usize>()
        .unwrap_or(1)
        - 1
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let db = Chronix::open(
        ChronixConfig::builder()
            .data_dir(dir.path())
            // The example drives the passes itself; a gateway leaves this at
            // its default and the maintenance thread does it.
            .maintenance_interval(Duration::ZERO)
            .build()?,
    )?;

    // ── 2. Declare (before any data: a gateway does this on start-up) ──
    let berlin = |w| TimeBucket::parse(w, Some("Europe/Berlin"));
    db.declare_detector(
        DetectorDeclaration::builder("roof_health")
            .source("pv_ratio", "value")
            // Each month is judged against the same month's normal.
            .season(Season::new(berlin("1mo")?, berlin("1y")?)?)
            // A single day far outside this month's band…
            .zscore(4.0)
            // …or a persistent drop of two standard deviations or more.
            .cusum(1.0, 5.0)
            // History is backfilled below; judge it all.
            .settle(Duration::ZERO)
            .build()?,
    )?;
    println!("declared: {:?}", db.detectors()?[0].status);

    // ── 1. Materialise the derived signal, a day at a time ─────────────
    let key = SeriesKey::new("pv_ratio", tags! { "array" => "south" })?;
    let points: Vec<Point> = (0..DAYS)
        .map(|d| {
            let ts = START + d * DAY;
            let health = if d >= FAULT_FROM { 0.85 } else { 1.0 };
            let ratio = HEALTHY[month_of(ts)] * health + scatter(d);
            Point::new(key.clone(), fields! { "value" => ratio }, ts)
        })
        .collect::<Result<_, _>>()?;
    db.backfill(&points)?.into_complete()?;

    // The maintenance thread does this on its own schedule.
    let judged = db.advance_detectors()?;
    let info = &db.detectors()?[0];
    println!(
        "judged {judged} days; {} series; status {:?}",
        info.series, info.status
    );

    // ── 3. Read the verdicts ───────────────────────────────────────────
    let rows = db.sql(
        "SELECT _time, value, baseline, z, reason \
         FROM roof_health_verdicts WHERE anomaly ORDER BY _time",
    )?;
    let mut alarms: Vec<i64> = Vec::new();
    for batch in &rows {
        if let Some(col) = batch.column_by_name(chronix_core::TIME_COLUMN) {
            let as_ns = arrow::compute::cast(col, &arrow::datatypes::DataType::Int64)?;
            if let Some(a) = as_ns.as_any().downcast_ref::<arrow::array::Int64Array>() {
                alarms.extend(a.values().iter().copied());
            }
        }
    }
    println!("{}", arrow::util::pretty::pretty_format_batches(&rows)?);

    let fault_ts = START + FAULT_FROM * DAY;
    let false_alarms = alarms.iter().filter(|&&t| t < fault_ts).count();
    let first = alarms.iter().find(|&&t| t >= fault_ts).copied();
    assert_eq!(false_alarms, 0, "a season raised an alarm");
    let first = first.ok_or("the degradation went unnoticed")?;
    println!(
        "no false alarm in {FAULT_FROM} days of seasons; degradation caught {} day(s) after it began",
        (first - fault_ts) / DAY
    );
    assert!(first - fault_ts <= 7 * DAY, "caught too late");

    db.close()?;
    Ok(())
}
