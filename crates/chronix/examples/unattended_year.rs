#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # A year unattended
//!
//! Runs the design partner's deployment shape — raw data on a short
//! retention, a rollup tier kept for years, maintenance on every pass — through
//! **400 simulated days** and prints what the database looks like as it goes.
//!
//! Every other test and example in this tree writes its data and asserts
//! within the same second, so nothing observed the *shape* of the system after
//! a long run. Five defects were sitting in that gap, and this is the cheapest
//! way to ask the question again: run the real engine for a long time and read
//! the table.
//!
//! Two things to read it for:
//!
//! - **A number that only grows.** `series` must track the live series, not
//!   every series ever written — it is the cardinality budget, and a budget
//!   that only grows eventually refuses writes. `raw` must plateau at the
//!   retention window; `rollup` is *expected* to grow, because that tier is
//!   kept for years, and mistaking the second for the first is how the first
//!   reading of this table went wrong.
//! - **A number that never changes.** `deleted` and `kept` are what retention
//!   did and what it held back. A retention figure that repeats on every pass
//!   while the disk climbs is a pass reporting inspection as work — which is
//!   what `shards_dropped` used to do, counting expired shards rather than
//!   removed ones.
//!
//! ```sh
//! cargo run -p chronix --example unattended_year
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use chronix::prelude::*;
use chronix::{fields, tags, Chronix, RollupAggFn, RollupBuilder};

const DAY_NS: i64 = 86_400_000_000_000;
const DAYS: i64 = 400;
const DEVICES: usize = 5;
/// Hourly raw readings: enough to exercise every pass, small enough that the
/// example stays a smoke test.
const READINGS_PER_DAY: i64 = 24;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let db = Arc::new(Chronix::open(
        ChronixConfig::builder()
            .data_dir(dir.path())
            .shard_duration(Duration::from_secs(86_400))
            // Raw data for a week; the aggregates for three years.
            .retention(Some(Duration::from_secs(86_400 * 7)))
            // The passes are driven explicitly below, one per simulated day.
            .maintenance_interval(Duration::from_secs(86_400 * 365))
            // The gateway preset's policy: on eMMC and SD the fsync rate is
            // the wear rate. It is also what makes 400 days of maintenance
            // fast enough to be a CI smoke test.
            .wal_fsync_policy(FsyncPolicy::Periodic(Duration::from_secs(5)))
            .build()?,
    )?);

    db.create_rollup(
        RollupBuilder::new()
            .name("hourly")
            .source("power")
            .target("power_1h")
            .every("1h")
            .aggregation(RollupAggFn::Avg)
            .group_by("device")
            .retention_ns(3 * 365 * DAY_NS)
            .build()?,
    )?;

    // The fixture ends yesterday, so no reading is ahead of the wall clock.
    let now_ns = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos(),
    )?;
    let start = (now_ns - DAYS * DAY_NS) / DAY_NS * DAY_NS;
    let keys: Vec<SeriesKey> = (0..DEVICES)
        .map(|i| SeriesKey::new("power", tags! { "device" => format!("dev-{i}") }).unwrap())
        .collect();

    println!("Simulating {DAYS} days of {DEVICES} devices at {READINGS_PER_DAY} readings/day.");
    println!("Raw retention 7 days; hourly rollup kept 3 years.\n");
    // `disk_usage_bytes()` is cached for a minute — it needs a directory walk
    // and `statistics()` runs on every metrics scrape — so it is printed once
    // at the end rather than per row, where the cache would make the column
    // read as a step change that never happened.
    println!(
        "{:>4}  {:>7}  {:>4}  {:>7}  {:>8}  {:>5}  {:>8}",
        "day", "series", "raw", "rollup", "deleted", "kept", "pass ms"
    );

    let mut peak_pass_ms = 0.0f64;
    for day in 0..DAYS {
        let day_start = start + day * DAY_NS;
        let points: Vec<Point> = (0..READINGS_PER_DAY)
            .flat_map(|h| {
                let keys = &keys;
                (0..DEVICES).map(move |i| {
                    Point::new(
                        keys[i].clone(),
                        fields! { "w" => 100.0 + h as f64 + i as f64 },
                        day_start + h * 3_600_000_000_000,
                    )
                    .unwrap()
                })
            })
            .collect();
        let res = db.insert_batch(&points)?;
        assert!(res.is_complete(), "day {day}: {:?}", res.rejected);

        let t = Instant::now();
        db.flush()?;
        db.materialise_rollups()?;
        db.compact()?;
        let retention = db.enforce_configured_retention()?;
        db.gc()?;
        let pass_ms = t.elapsed().as_secs_f64() * 1000.0;
        peak_pass_ms = peak_pass_ms.max(pass_ms);

        if day % 50 == 0 || day == DAYS - 1 {
            let stats = db.statistics();
            let (raw, rollup) = {
                let catalog = db.catalog().read();
                let all = catalog.all_segments();
                (
                    all.iter().filter(|e| e.measurement == "power").count(),
                    all.iter().filter(|e| e.measurement == "power_1h").count(),
                )
            };
            println!(
                "{day:>4}  {:>7}  {raw:>4}  {rollup:>7}  {:>8}  {:>5}  {pass_ms:>8.1}",
                stats.series_count, retention.segments_deleted, retention.segments_preserved,
            );
        }
    }

    // The properties the table is read for, asserted so the example fails
    // rather than merely looking wrong. This is the smoke test CI runs.
    let stats = db.statistics();
    // Five raw series plus the five the rollup tier writes. The point is that
    // it is *flat*: before the cardinality repair this climbed by five every
    // week as retention deleted raw shards without releasing what they held.
    assert_eq!(
        stats.series_count,
        DEVICES * 2,
        "the cardinality budget must hold the live series, not every series ever written"
    );
    let raw_segments = {
        let catalog = db.catalog().read();
        catalog
            .all_segments()
            .iter()
            .filter(|e| e.measurement == "power")
            .count()
    };
    assert!(
        raw_segments <= 10,
        "raw data must plateau at the retention window, not accumulate ({raw_segments} segments)"
    );

    // Deliberately not `disk_usage_bytes()`: it is cached for a minute, so
    // inside a run this short it would report a figure from the first day and
    // present it as the last.
    println!(
        "\nAfter {DAYS} days: {} series, {raw_segments} raw segments, resident heap {} KiB, \
         slowest maintenance pass {peak_pass_ms:.0} ms.",
        stats.series_count,
        stats.resident_memory_bytes() / 1024,
    );
    println!(
        "The rollup tier is what grows: it is kept for three years by design. \
         Raw data, the series budget and the pass duration are flat."
    );

    db.close()?;
    Ok(())
}
