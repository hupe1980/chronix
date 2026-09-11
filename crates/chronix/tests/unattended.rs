#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! A year on a box nobody touches.
//!
//! Every test here drives a database over a span longer than one sitting:
//! writes that stop, devices that come and go, shards that expire while a
//! rollup still needs them. That is the design partner's deployment — an
//! unattended gateway with days-long offline stretches — and it is the one
//! shape the rest of the suite never runs, because every other test writes
//! its data and asserts within the same second.

use std::sync::Arc;
use std::time::Duration;

use chronix::prelude::*;
use chronix::rollup::{RollupAggFn, RollupBuilder};
use chronix::{fields, tags, Chronix};

const DAY: i64 = 86_400_000_000_000;
const HOUR: i64 = 3_600_000_000_000;

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
    .unwrap()
}

fn open(dir: &tempfile::TempDir, retention_days: u64) -> Arc<Chronix> {
    Arc::new(
        Chronix::open(
            ChronixConfig::builder()
                .data_dir(dir.path())
                .shard_duration(Duration::from_secs(86_400))
                .retention(Some(Duration::from_secs(86_400 * retention_days)))
                // The maintenance thread must not race the explicit passes
                // these tests drive.
                .maintenance_interval(Duration::from_secs(86_400 * 365))
                .build()
                .unwrap(),
        )
        .unwrap(),
    )
}

fn day_of(db: &Chronix, key: &SeriesKey, day_start: i64) {
    let points: Vec<Point> = (0..24)
        .map(|h| {
            Point::new(
                key.clone(),
                fields! { "w" => 100.0 + h as f64 },
                day_start + h * HOUR,
            )
            .unwrap()
        })
        .collect();
    let res = db.insert_batch(&points).unwrap();
    assert!(res.is_complete(), "rejected: {:?}", res.rejected);
}

fn rows_in(db: &Chronix, measurement: &str) -> usize {
    let plan = db
        .query()
        .measurement(measurement)
        .range(0, i64::MAX / 2)
        .build()
        .unwrap();
    db.execute(&plan).map(|b| b.num_rows()).unwrap_or(0)
}

/// Retention measures age from the newest data the database holds, capped by
/// the wall clock — never from the wall clock alone.
///
/// A gateway that stops writing does not stop existing. Its sensors go
/// offline, its uplink drops, the house is empty for a month; the clock keeps
/// moving and the data does not. Measuring "older than seven days" against
/// the clock alone empties such a database completely, and there is no
/// recovering from it.
///
/// The same argument in its sharper form is a clock that is simply *wrong*:
/// a gateway with no battery-backed RTC, an NTP server handing out a date in
/// the next century, a VM restored from a snapshot. One bad reading of the
/// clock is a permanent, total delete. QuestDB caps its data-driven TTL with
/// the wall clock for exactly this reason.
#[test]
fn a_quiet_database_keeps_its_history() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir, 7);
    let key = SeriesKey::new("power", tags! { "device" => "meter" }).unwrap();

    // Three days of readings that ended a month ago: within seven days of
    // *each other*, and long past seven days of wall-clock age.
    let last_write = (now_ns() - 30 * DAY) / DAY * DAY;
    for back in (0..3).rev() {
        day_of(&db, &key, last_write - back * DAY);
    }
    db.flush().unwrap();
    assert_eq!(
        rows_in(&db, "power"),
        72,
        "premise: three days were written"
    );

    db.enforce_configured_retention().unwrap();

    assert_eq!(
        rows_in(&db, "power"),
        72,
        "a seven-day rule deleted data that is three days old, because the \
         wall clock moved and the data did not"
    );
}

/// …and it resumes the moment real data confirms what time it is.
///
/// The cap delays retention, it does not disable it: one fresh write moves
/// the reference to the present and the backlog expires on the next pass.
#[test]
fn a_write_after_a_quiet_month_expires_the_backlog() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir, 7);
    let key = SeriesKey::new("power", tags! { "device" => "meter" }).unwrap();

    let last_write = (now_ns() - 30 * DAY) / DAY * DAY;
    day_of(&db, &key, last_write);
    db.flush().unwrap();
    db.enforce_configured_retention().unwrap();
    assert_eq!(
        rows_in(&db, "power"),
        24,
        "premise: the quiet history survived"
    );

    // The gateway comes back.
    let yesterday = (now_ns() - DAY) / DAY * DAY;
    day_of(&db, &key, yesterday);
    db.flush().unwrap();
    db.enforce_configured_retention().unwrap();

    assert_eq!(
        rows_in(&db, "power"),
        24,
        "only today's readings should remain once the clock is confirmed"
    );
}

/// The cardinality budget is released by retention, not only by a delete.
///
/// `max_series_cardinality` is an admission limit, and `known_series` is the
/// counter it is checked against. Retention deletes the data and leaves the
/// counter alone, so a gateway that sees any tag churn at all — a replaced
/// meter, a firmware version, a session id — climbs towards the limit for
/// ever and eventually refuses every write, with the data it is counting
/// long since deleted. A restart clears it, which means the admission
/// decision depends on how long the process has been up.
#[test]
fn retention_releases_the_cardinality_of_the_series_it_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir, 7);

    // One new device a day for a month: thirty series ever, never more than
    // eight of them inside the retention window.
    let start = (now_ns() - 30 * DAY) / DAY * DAY;
    for day in 0..30 {
        let key = SeriesKey::new("power", tags! { "device" => format!("dev-{day}") }).unwrap();
        day_of(&db, &key, start + day * DAY);
        db.flush().unwrap();
        db.enforce_configured_retention().unwrap();
        db.gc().unwrap();
    }

    let series = db.statistics().series_count;
    assert!(
        series <= 9,
        "thirty days of device churn under a seven-day rule left {series} series \
         in the cardinality budget; only the live ones should be counted"
    );
}

/// A measurement that retention emptied is still a table its tenant can
/// query — and a measurement that was *dropped* is not.
///
/// These pull in opposite directions and the difference matters. Under a
/// namespace, "the table resolves" is itself an answer, so the measurement
/// list is scoped: it used to come from the process-wide schema registry, and
/// `SELECT * FROM another_tenants_measurement` returned zero rows where a
/// name that does not exist errors — an oracle for enumerating other
/// tenants. But scoping it to *series that currently exist* would make a
/// tenant's own dashboard start erroring the week their sensor went quiet,
/// which is worse than an empty graph. So the index follows the
/// **measurement**, not its rows: emptied by retention it stays, dropped it
/// goes.
#[test]
fn a_dropped_measurement_leaves_its_tenant_s_table_list() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir, 7);
    let key = SeriesKey::new(
        "power",
        tags! { chronix_core::NAMESPACE_TAG => "tenant-a", "device" => "meter" },
    )
    .unwrap();

    let start = (now_ns() - 30 * DAY) / DAY * DAY;
    day_of(&db, &key, start);
    db.flush().unwrap();
    assert_eq!(
        db.measurement_names_in(Some("tenant-a")),
        vec!["power".to_string()],
        "premise: the measurement is listed while it holds data"
    );

    // A live write elsewhere moves the retention reference to the present,
    // which is what lets the tenant's month-old shard expire.
    let other = SeriesKey::new("heartbeat", tags! { "src" => "gw" }).unwrap();
    day_of(&db, &other, (now_ns() - DAY) / DAY * DAY);
    db.flush().unwrap();
    db.enforce_configured_retention().unwrap();
    db.gc().unwrap();

    assert_eq!(
        rows_in(&db, "power"),
        0,
        "premise: retention dropped the data"
    );
    assert_eq!(
        db.measurement_names_in(Some("tenant-a")),
        vec!["power".to_string()],
        "an emptied measurement is a table with no rows, not a table that is gone"
    );

    // Dropping it is the other thing entirely.
    db.drop_measurement("power").unwrap();
    assert!(
        db.measurement_names_in(Some("tenant-a")).is_empty(),
        "a dropped measurement is still listed for the tenant"
    );
    assert!(
        !db.has_measurement_in(Some("tenant-a"), "power"),
        "a dropped measurement still resolves for the tenant"
    );
}

/// Retention reports the shards it dropped, not the shards it looked at.
///
/// `shards_dropped` is the number in the log line and behind
/// `chronix_retention_shards_dropped_total`. It counted every *expired*
/// shard, including the ones it then declined to touch because a rollup
/// still needed them — so an operator watching a disk that will not shrink
/// read "Retention enforced, shards=13" on every pass, for ever.
#[test]
fn retention_reports_the_shards_it_actually_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir, 7);
    db.create_rollup(
        RollupBuilder::new()
            .name("hourly")
            .source("power")
            .target("power_1h")
            .every("1h")
            .aggregation(RollupAggFn::Avg)
            .group_by("device")
            // The tier outlives the raw data — that is what a rollup is for.
            .retention_ns(i64::MAX / 4)
            .build()
            .unwrap(),
    )
    .unwrap();

    let key = SeriesKey::new("power", tags! { "device" => "meter" }).unwrap();
    let start = (now_ns() - 30 * DAY) / DAY * DAY;
    for day in 0..30 {
        day_of(&db, &key, start + day * DAY);
    }
    db.flush().unwrap();
    db.materialise_rollups().unwrap();
    // The aggregates must reach the catalog: an expired shard that holds
    // only a protected rollup segment is exactly the case that was
    // mis-reported.
    db.flush().unwrap();

    // Every pass after the first has nothing left to drop: the raw shards
    // are gone and the rollup tier is protected by its own retention.
    db.enforce_configured_retention().unwrap();
    let second = db.enforce_configured_retention().unwrap();

    assert_eq!(
        second.segments_deleted, 0,
        "premise: the second pass has nothing left to delete"
    );
    assert_eq!(
        second.shards_dropped, 0,
        "retention reported {} dropped shards while deleting nothing",
        second.shards_dropped
    );
}

/// Nothing the engine holds grows per segment without being reported.
///
/// The per-segment term used to be a metadata index holding every segment's
/// header, column statistics and per-tag bloom filters — 890 bytes a segment,
/// the term that grows on a multi-year rollup tier, read by nothing but its
/// own gauge. It is gone; what is left has to stay accounted for, so the sum
/// is asserted here across twenty days of segments as well as in
/// `resident_memory`.
#[test]
fn resident_memory_is_the_sum_of_its_terms_at_every_size() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir, 3650);
    let key = SeriesKey::new("power", tags! { "device" => "meter" }).unwrap();
    let start = (now_ns() - 20 * DAY) / DAY * DAY;
    for day in 0..20 {
        day_of(&db, &key, start + day * DAY);
        db.flush().unwrap();
    }

    let stats = db.statistics();
    assert!(
        stats.segment_count >= 20,
        "premise: at least one segment a day ({} segments)",
        stats.segment_count
    );
    assert_eq!(
        stats.resident_memory_bytes(),
        stats.memtable_memory_bytes
            + stats.interner_memory_bytes
            + stats.wal_buffer_bytes
            + stats.catalog_memory_bytes,
        "resident memory must be the sum of every term it reports"
    );
    assert!(
        stats.catalog_memory_bytes > 0,
        "the catalog is the per-segment term now, and it has to report itself"
    );
}

/// The repair and the rebuild agree.
///
/// There are two implementations of "which series does this database hold?":
/// the repair that runs after retention, and the rebuild `open()` performs
/// from the segment sidecars. Only one of them existed before, which is why
/// a restart healed a budget a running process could not: two implementations
/// of one promise, where only one was ever checked. This drives both and
/// compares.
#[test]
fn the_running_budget_and_a_restart_agree() {
    let dir = tempfile::tempdir().unwrap();
    let start = (now_ns() - 30 * DAY) / DAY * DAY;
    let counted = {
        let db = open(&dir, 7);
        for day in 0..30 {
            let key = SeriesKey::new("power", tags! { "device" => format!("dev-{day}") }).unwrap();
            day_of(&db, &key, start + day * DAY);
            db.flush().unwrap();
            db.enforce_configured_retention().unwrap();
            db.gc().unwrap();
        }
        let counted = db.statistics().series_count;
        db.close().unwrap();
        counted
    };

    let reopened = open(&dir, 7);
    assert_eq!(
        reopened.statistics().series_count,
        counted,
        "the cardinality budget changed across a restart, so the admission \
         decision depended on how long the process had been up"
    );
}

/// The last-value cache does not answer with data retention deleted.
///
/// A copy is not an answer. A device that goes offline for longer than
/// the retention window used to keep reporting its final reading through
/// `last_value()` for ever, while every other read path correctly said
/// nothing — on an energy dashboard, a stale number presented as current.
#[test]
fn the_last_value_cache_forgets_what_retention_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Chronix::open(
            ChronixConfig::builder()
                .data_dir(dir.path())
                .shard_duration(Duration::from_secs(86_400))
                .retention(Some(Duration::from_secs(86_400 * 7)))
                .enable_last_value_cache(true)
                .maintenance_interval(Duration::from_secs(86_400 * 365))
                .build()
                .unwrap(),
        )
        .unwrap(),
    );

    let gone_tags: std::collections::BTreeMap<String, String> =
        [("device".to_string(), "retired".to_string())].into();
    let live_tags: std::collections::BTreeMap<String, String> =
        [("device".to_string(), "current".to_string())].into();
    let gone = SeriesKey::new("power", gone_tags.clone()).unwrap();
    day_of(&db, &gone, (now_ns() - 30 * DAY) / DAY * DAY);
    db.flush().unwrap();
    assert!(
        db.last_value("power", &gone_tags).unwrap().is_some(),
        "premise: the cache answers while the data exists"
    );

    // A live series elsewhere is what moves the retention reference to the
    // present and expires the retired device's shard.
    let live = SeriesKey::new("power", live_tags.clone()).unwrap();
    day_of(&db, &live, (now_ns() - DAY) / DAY * DAY);
    db.flush().unwrap();
    db.enforce_configured_retention().unwrap();
    db.gc().unwrap();

    assert!(
        db.last_value("power", &gone_tags).unwrap().is_none(),
        "the cache still answers for a series whose data retention deleted"
    );
    assert!(
        db.last_value("power", &live_tags).unwrap().is_some(),
        "the live series must keep its cached value"
    );
}
