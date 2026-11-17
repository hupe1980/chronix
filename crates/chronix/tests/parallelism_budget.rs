#![cfg(feature = "sql")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! One parallelism budget: `ChronixConfig::small` keeps a long read and a
//! model search on the caller's thread, `Auto` uses chronix's own pool, and an
//! injected pool is the only one used. Rayon's global pool is never touched.
//!
//! The probe records the thread of every segment read and every analytics
//! work item. It is process-global, so the tests take one lock.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use chronix::prelude::*;
use chronix::{Chronix, fields, tags};
use chronix_analytics::par::probe;

const HOUR: i64 = 3_600_000_000_000;
const DAY: i64 = 24 * HOUR;
/// 2023-01-01T00:00:00Z.
const BASE: i64 = 1_672_531_200_000_000_000;
const DAYS: i64 = 30;
const HOSTS: usize = 6;

static PROBE: Mutex<()> = Mutex::new(());

/// One shard, no background passes: the segments the test writes are the
/// segments the read sees.
fn tune(mut config: ChronixConfig) -> ChronixConfig {
    config.shard_duration = Duration::from_secs(40 * 86_400);
    config.maintenance_interval = Duration::ZERO;
    config
}

fn small(dir: &std::path::Path) -> ChronixConfig {
    tune(ChronixConfig::small(dir))
}

fn auto(dir: &std::path::Path) -> ChronixConfig {
    tune(ChronixConfig::builder().data_dir(dir).build().unwrap())
}

fn hourly(host: &str, offset: f64) -> Vec<Point> {
    let key = SeriesKey::new("meter", tags! { "host" => host }).unwrap();
    (0..DAYS * 24)
        .map(|h| {
            let v = offset
                + 10.0 * ((h % 24) as f64 / 24.0 * std::f64::consts::TAU).sin()
                + (h % 7) as f64 * 0.1;
            Point::new(key.clone(), fields! { "power" => v }, BASE + h * HOUR).unwrap()
        })
        .collect()
}

/// Six hosts, each spanning all thirty days and flushed on its own, so one
/// time range is covered by six overlapping segments — then a seventh flush
/// that rewrites `h0` for the last day, which last-write-wins must resolve.
fn populate(db: &Chronix) {
    for i in 0..HOSTS {
        db.backfill(&hourly(&format!("h{i}"), i as f64 * 100.0))
            .unwrap()
            .into_complete()
            .unwrap();
        db.flush().unwrap();
    }
    let rewrite: Vec<Point> = hourly("h0", 5_000.0)
        .into_iter()
        .filter(|p| p.timestamp() >= BASE + (DAYS - 1) * DAY)
        .collect();
    db.backfill(&rewrite).unwrap().into_complete().unwrap();
    db.flush().unwrap();
    assert!(
        db.segment_count() > 4,
        "the read must span more than four segments"
    );
}

fn read(db: &Chronix) -> arrow::record_batch::RecordBatch {
    let plan = db
        .query()
        .measurement("meter")
        .range(BASE, BASE + DAYS * DAY)
        .build()
        .unwrap();
    db.execute(&plan).unwrap()
}

fn forecast(db: &Chronix) -> Vec<f64> {
    db.auto_forecast(
        "meter",
        "power",
        &[("host", "h1")],
        BASE + (DAYS - 7) * DAY,
        BASE + DAYS * DAY,
        24,
        None,
    )
    .unwrap()
    .result
    .values
}

#[test]
fn a_thirty_day_read_under_small_runs_nothing_on_any_pool() {
    let _one = PROBE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let me = std::thread::current().name().map(str::to_owned);

    let (serial_rows, serial_forecast) = {
        let db = Chronix::open(small(dir.path())).unwrap();
        populate(&db);
        probe::start();
        let rows = read(&db);
        let fc = forecast(&db);
        let seen = probe::take();
        assert!(
            seen.len() > 4,
            "the probe saw the scan and the search: {seen:?}"
        );
        assert!(
            seen.iter().all(|s| !s.rayon_worker && s.thread == me),
            "under `small`, every segment read and model fit runs on the caller's thread: {seen:?}"
        );
        db.close().unwrap();
        (rows, fc)
    };

    let db = Chronix::open(auto(dir.path())).unwrap();
    probe::start();
    let rows = read(&db);
    let fc = forecast(&db);
    let seen = probe::take();
    assert!(
        seen.iter().any(|s| s.rayon_worker),
        "`Auto` fans out on a multi-core machine: {seen:?}"
    );
    assert!(
        seen.iter().filter(|s| s.rayon_worker).all(|s| s
            .thread
            .as_deref()
            .is_some_and(|n| n.starts_with("chronix-par-"))),
        "parallel work runs on chronix's own pool, never rayon's global one: {seen:?}"
    );

    // Same answer either way, including the rewritten day.
    assert_eq!(rows, serial_rows);
    assert_eq!(fc, serial_forecast);
}

#[test]
fn an_injected_pool_is_the_only_pool_used() {
    let _one = PROBE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .thread_name(|i| format!("host-{i}"))
            .build()
            .unwrap(),
    );
    // `small` says serial; the injected pool's size is the budget instead.
    let db = Chronix::open_with_pool(small(dir.path()), Arc::clone(&pool)).unwrap();
    populate(&db);

    probe::start();
    let _ = read(&db);
    let _ = forecast(&db);
    let seen = probe::take();
    assert!(seen.iter().any(|s| s.rayon_worker), "{seen:?}");
    assert!(
        seen.iter()
            .filter(|s| s.rayon_worker)
            .all(|s| s.thread.as_deref().is_some_and(|n| n.starts_with("host-"))),
        "every parallel item ran on the host's pool: {seen:?}"
    );
    db.close().unwrap();
}

#[test]
fn sql_plans_for_the_budget() {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open(small(dir.path())).unwrap();
    let partitions = db.session_context().state().config().target_partitions();
    assert_eq!(partitions, 1, "`small` plans SQL for one partition");
    db.close().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let mut config = auto(dir.path());
    config.parallelism = Parallelism::threads(3);
    let db = Chronix::open(config).unwrap();
    assert_eq!(db.session_context().state().config().target_partitions(), 3);
    db.close().unwrap();
}
