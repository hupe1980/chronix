//! A roof that slowly stops delivering — caught by a detector the store
//! keeps, quarter hour by quarter hour on the Berlin wall clock.
//!
//! The case is a home energy gateway's plant-health monitor. Raw PV output
//! is useless to watch: every cloudy day looks like a fault. What is watched
//! is the **performance ratio** (IEC 61724-1) — actual production over what a
//! physical model says a clean, healthy array would produce in that quarter
//! hour — which the application computes and writes as its own measurement.
//!
//! Three steps, and only the first is the application's:
//!
//! 1. **Write the derived signal**, one point per daylight quarter hour.
//! 2. **Declare the detector** — once, idempotently, on every start.
//! 3. **Read the verdicts** — ordinary rows, queryable, subscribable.
//!
//! The season is `15m` slots in a Europe/Berlin `1d`: 14:00 is judged
//! against 14:00 on every day, including the 23-hour day in March and the
//! 25-hour day in October. Forgetting lets each slot follow the seasons. A
//! simulated year runs through the real engine below, with a 15 %
//! degradation from 1 November; the example asserts no alarm before it —
//! across both clock changes — and a catch within days after it.
//!
//! Run: `cargo run -p chronix --example declared_detector`

use std::time::Duration;

use chronix::detector::{DetectorDeclaration, Season};
use chronix::prelude::*;
use chronix::{Chronix, fields, tags};

const DAY: i64 = 86_400_000_000_000;
const QUARTER: i64 = 900_000_000_000;
/// 2025-12-01T00:00:00Z.
const START: i64 = 1_764_547_200_000_000_000;
const DAYS: i64 = 350;

/// A healthy array's performance ratio on day `day` of the simulation: 0.80,
/// a little higher in winter — a cold panel converts better — and lower in a
/// hot July.
fn healthy(day: i64) -> f64 {
    // Day 0 is 1 December; the coldest point is mid-January.
    let phase = (day - 45) as f64 / 365.25 * std::f64::consts::TAU;
    0.80 + 0.03 * phase.cos()
}

/// Deterministic scatter of ±0.02.
fn scatter(i: i64) -> f64 {
    let x = (i.wrapping_mul(2_654_435_761) % 1_000) as f64 / 1_000.0;
    (x * 2.0 - 1.0) * 0.02
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
            // Each quarter hour against the same quarter hour of the days before.
            .season(Season::new(berlin("15m")?, berlin("1d")?)?)
            // A slot's past halves in weight every week: winter's normal is
            // not July's, and the baseline follows the year.
            .forgetting(7)
            // A slot sees one point a day: judge nothing on less than a
            // week of it. (Under a half-life of h periods a slot fed once a
            // period holds about 1.44·h + 0.5 points, so the warm-up must
            // stay below that or the slot never judges.)
            .min_samples(7)
            // A single quarter hour far outside its band…
            .zscore(5.0)
            // …or a persistent drop.
            .cusum(0.5, 8.0)
            // History is backfilled below; judge it all.
            .settle(Duration::ZERO)
            .build()?,
    )?;

    // ── 1. Write the derived signal: daylight quarter hours, local time ──
    let clock = berlin("15m")?;
    let fault_from = START + 335 * DAY; // 2026-11-01
    let key = SeriesKey::new("pv_ratio", tags! { "array" => "south" })?;
    let mut points = Vec::new();
    for i in 0..DAYS * 96 {
        let ts = START + i * QUARTER;
        let local = clock.wall_clock_ns(ts);
        let hour = local.rem_euclid(DAY) / (4 * QUARTER);
        if !(9..16).contains(&hour) {
            continue; // the model says nothing useful in the dark
        }
        let day = (local - clock.wall_clock_ns(START)).div_euclid(DAY);
        let health = if ts >= fault_from { 0.85 } else { 1.0 };
        let ratio = healthy(day) * health + scatter(i);
        points.push(Point::new(key.clone(), fields! { "value" => ratio }, ts)?);
    }
    db.backfill(&points)?.into_complete()?;

    // The maintenance thread does this on its own schedule.
    let mut judged = 0;
    loop {
        let n = db.advance_detectors()?;
        if n == 0 {
            break;
        }
        judged += n;
    }
    let info = &db.detectors()?[0];
    println!(
        "{} points written, {judged} judged; status {:?}, {} slot(s) still warming",
        points.len(),
        info.status,
        info.warming_slots.unwrap_or(0)
    );

    // ── 3. Read the verdicts ───────────────────────────────────────────
    let rows = db.sql(
        "SELECT _time, slot, value, baseline, z, reason \
         FROM roof_health_verdicts WHERE anomaly ORDER BY _time LIMIT 5",
    )?;
    println!("{}", arrow::util::pretty::pretty_format_batches(&rows)?);

    let count = |sql: &str| -> Result<i64, Box<dyn std::error::Error>> {
        let b = db.sql(sql)?;
        let col = b.first().ok_or("no result")?.column(0);
        let n = arrow::compute::cast(col, &arrow::datatypes::DataType::Int64)?;
        Ok(n.as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .map_or(0, |a| a.value(0)))
    };
    let fault = fault_from;
    let false_alarms = count(&format!(
        "SELECT count(*) FROM roof_health_verdicts WHERE anomaly AND _time < {fault}"
    ))?;
    // 29 March and 25 October 2026, local midnight to midnight.
    let transition_days = count(
        "SELECT count(*) FROM roof_health_verdicts \
         WHERE (_time >= 1774738800000000000 AND _time < 1774821600000000000) \
            OR (_time >= 1792879200000000000 AND _time < 1792969200000000000)",
    )?;
    let first = count(&format!(
        "SELECT min(_time) FROM roof_health_verdicts WHERE anomaly AND _time >= {fault}"
    ))?;
    assert!(transition_days > 0, "both clock changes were judged");
    assert_eq!(false_alarms, 0, "an alarm before the fault");
    assert!(first > 0, "the degradation went unnoticed");
    println!(
        "no false alarm in {} days — {transition_days} verdicts on the two clock-change days — \
         and the degradation caught {:.1} day(s) after it began",
        (fault - START) / DAY,
        (first - fault) as f64 / DAY as f64
    );
    assert!(first - fault <= 3 * DAY, "caught too late");

    db.close()?;
    Ok(())
}
