//! The built-in maintenance thread.
//!
//! A `Chronix` maintains itself: one thread per open database flushes a
//! memtable as soon as it crosses its threshold, and every
//! [`ChronixConfig::maintenance_interval`](chronix_core::ChronixConfig::maintenance_interval)
//! runs the background passes — compaction (which materialises rollups),
//! garbage collection, retention. The embedded caller starts nothing;
//! `chronixd` starts nothing either.
//!
//! Before this thread existed, flushes were signalled to a scheduler that
//! only a caller could start — and `chronixd` never started it, so a
//! memtable grew until admission control refused writes, and stayed refused
//! until a 30-second compaction tick (backing off to four minutes when idle)
//! happened to flush it. An embedded database with no scheduler at all
//! never flushed, compacted, materialised or expired anything, whatever its
//! configuration said. "No background daemons required" meant "no
//! background work happens".
//!
//! The thread holds only a `Weak` reference, so it never keeps a database
//! alive; it takes a handle for the duration of each pass and lets go.

use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use tracing::{debug, warn};

use crate::db::{Chronix, DbInner};

/// How often the thread wakes on its own when nothing signals it.
const IDLE_TICK: Duration = Duration::from_secs(1);

/// Clears the liveness flag however the thread leaves — return or panic.
struct AliveGuard(Arc<std::sync::atomic::AtomicBool>);

impl Drop for AliveGuard {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
        metrics::gauge!("chronix_maintenance_running").set(0.0);
    }
}

/// Spawn the maintenance thread for a freshly opened database.
pub(crate) fn spawn(db: &Chronix) {
    let weak = Arc::downgrade(&db.inner);
    let lifecycle = Arc::clone(&db.lifecycle);
    let wake = Arc::clone(&db.maintenance_wake);
    let stop = Arc::clone(&db.stop_maintenance);
    let alive = Arc::clone(&db.maintenance_alive);
    let interval = db.config.maintenance_interval;
    let flush_threshold = db.config.memtable_flush_threshold;
    alive.store(true, std::sync::atomic::Ordering::Release);
    let handle = std::thread::Builder::new()
        .name("chronix::maintenance".into())
        .spawn(move || {
            let _alive = AliveGuard(alive);
            run(&weak, &lifecycle, &wake, &stop, interval, flush_threshold);
        })
        .expect("spawning the maintenance thread cannot fail on a working OS");
    let _ = db.maintenance_thread_id.set(handle.thread().id());
    *db.maintenance_thread.lock() = Some(handle);
}

fn run(
    weak: &Weak<DbInner>,
    lifecycle: &parking_lot::Mutex<()>,
    wake: &(parking_lot::Mutex<bool>, parking_lot::Condvar),
    stop: &std::sync::atomic::AtomicBool,
    interval: Duration,
    flush_threshold: usize,
) {
    let mut last_pass = Instant::now();
    let tick = if interval.is_zero() {
        IDLE_TICK
    } else {
        interval.min(IDLE_TICK)
    };
    loop {
        // Sleep until a writer wakes us or the tick elapses — without a
        // handle: a strong reference held here would inflate the count a
        // user's `Drop` reads.
        {
            let (flag, cv) = wake;
            let mut requested = flag.lock();
            if !*requested {
                cv.wait_for(&mut requested, tick);
            }
            *requested = false;
        }
        // Reported from the loop rather than once at spawn: `Chronix::open`
        // runs before `chronixd` installs its Prometheus recorder, so a
        // gauge set at spawn is written to no recorder at all and the metric
        // never appears — which is the failure mode this gauge exists to
        // catch, arrived at from the inside. Refreshed at most once a tick,
        // so an absence longer than that means the same as a zero.
        metrics::gauge!("chronix_maintenance_running").set(1.0);
        // The stop flag is checked *before* the lifecycle lock: a user's
        // `Drop` holds that lock while `close()` joins this thread, so a
        // check that needed the lock would spin forever.
        if stop.load(std::sync::atomic::Ordering::Acquire) || weak.strong_count() == 0 {
            return;
        }
        // Take and drop the handle under the lifecycle lock, so a user's
        // `Drop` never sees a count this handle inflated. `try_lock`, never
        // `lock`: a user closing under that lock is joining this thread.
        let Some(_lifecycle) = lifecycle.try_lock() else {
            continue;
        };
        let Some(probe) = weak.upgrade() else { return };
        if stop.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        let db = Chronix {
            inner: probe,
            maintenance: true,
        };

        // A panic in one pass used to end the thread, and with it every
        // flush, compaction, rollup and retention pass for the life of the
        // process. Catching it costs a counter and keeps the *other* passes
        // running; `AliveGuard` still covers the case where the loop leaves
        // for a reason this does not catch.
        let closed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            one_pass(&db, &mut last_pass, interval, flush_threshold)
        }));
        match closed {
            Ok(true) => return,
            Ok(false) => {}
            Err(_) => {
                metrics::counter!("chronix_maintenance_panics_total").increment(1);
                warn!(
                    "maintenance: a pass panicked; the thread continues, but something \
                     is wrong — see the panic above"
                );
            }
        }
        // `db` drops here (inert: the maintenance handle never closes),
        // still under the lifecycle lock.
        drop(db);
    }
}

/// One maintenance pass. Returns `true` when the database has closed and the
/// thread should stop.
fn one_pass(
    db: &Chronix,
    last_pass: &mut Instant,
    interval: Duration,
    flush_threshold: usize,
) -> bool {
    if db.shards.total_memory() > flush_threshold {
        match db.flush() {
            Ok(results) => debug!(segments = results.len(), "maintenance: flushed"),
            Err(crate::error::DbError::Closed) => return true,
            Err(e) => warn!(error = %e, "maintenance: flush failed"),
        }
    }

    if !interval.is_zero() && last_pass.elapsed() >= interval {
        *last_pass = Instant::now();
        match db.compact() {
            Ok(tasks) if tasks > 0 => debug!(tasks, "maintenance: compacted"),
            Ok(_) => {}
            Err(crate::error::DbError::Closed) => return true,
            Err(e) => warn!(error = %e, "maintenance: compaction failed"),
        }
        if let Err(e) = db.gc() {
            warn!(error = %e, "maintenance: gc failed");
        }
        // Every configured rule, not only a global one: a database that
        // sets a retention for one measurement, or a rollup that
        // declares its own, used to expire nothing whatsoever.
        if db.has_retention_rules() {
            match db.enforce_configured_retention() {
                Ok(r) if r.segments_deleted > 0 => {
                    tracing::info!(
                        shards = r.shards_dropped,
                        segments = r.segments_deleted,
                        bytes = r.bytes_freed,
                        "maintenance: retention enforced"
                    );
                }
                Ok(_) => {}
                Err(crate::error::DbError::Closed) => return true,
                Err(e) => warn!(error = %e, "maintenance: retention failed"),
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use chronix_core::{ChronixConfig, FieldValue, Point, SeriesKey};
    use tempfile::TempDir;

    use crate::db::Chronix;

    fn test_point(measurement: &str, host: &str, ts: i64, value: f64) -> Point {
        let tags = BTreeMap::from([("host".to_string(), host.to_string())]);
        let fields = BTreeMap::from([("value".to_string(), FieldValue::F64(value))]);
        let key = SeriesKey::new(measurement, tags).unwrap();
        Point::new(key, fields, ts).unwrap()
    }

    fn wait_until(deadline: Duration, mut cond: impl FnMut() -> bool) -> bool {
        let end = std::time::Instant::now() + deadline;
        while std::time::Instant::now() < end {
            if cond() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        cond()
    }

    /// A database that has lost its maintenance thread says so, before the
    /// memtable fills and after it.
    ///
    /// Everything a database does for itself happens on that thread. When it
    /// ends, nothing flushes, compacts, materialises a rollup or expires a
    /// shard again — and nothing reported it: writes were accepted until the
    /// memtable filled and then refused as `TransientOverload`, whose message
    /// promises that "a flush is signalled and will resolve it". Nothing was
    /// left to keep that promise, and `/ready` answered `200` throughout, so
    /// an orchestrator never restarted the one process that needed it.
    #[test]
    fn a_database_without_its_maintenance_thread_is_not_writable() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(64 * 1024)
            .max_memtable_memory(64 * 1024)
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();
        db.check_writable()
            .expect("a freshly opened database is writable");

        // Stop the thread the way its own death would.
        db.stop_maintenance
            .store(true, std::sync::atomic::Ordering::Release);
        db.wake_maintenance();
        assert!(
            wait_until(Duration::from_secs(5), || !db
                .maintenance_alive
                .load(std::sync::atomic::Ordering::Acquire)),
            "the thread clears the liveness flag as it leaves"
        );

        // Reported *before* the memtable fills, which is the point: there is
        // still time to restart.
        let err = db
            .check_writable()
            .expect_err("a database with no maintenance thread is not writable");
        assert!(
            matches!(err, crate::error::DbError::PersistentOverload { .. }),
            "expected a persistent overload, got {err}"
        );
        assert!(
            err.to_string().contains("maintenance thread"),
            "the reason names what is missing: {err}"
        );

        // …and once it fills, the refusal is persistent rather than the
        // "back off, a flush is coming" that nothing would deliver.
        let mut ts = 1_700_000_000_000_000_000i64;
        let mut saw_persistent = false;
        for round in 0..200 {
            let batch: Vec<Point> = (0..200)
                .map(|i| {
                    ts += 1_000_000;
                    test_point("cpu", &format!("h{}", round * 200 + i), ts, 1.5)
                })
                .collect();
            match db.insert_batch(&batch) {
                Ok(_) => {}
                Err(crate::error::DbError::PersistentOverload { .. }) => {
                    saw_persistent = true;
                    break;
                }
                Err(e) => panic!("expected a persistent overload, got {e}"),
            }
        }
        assert!(
            saw_persistent,
            "a full memtable with no thread to flush it is not transient"
        );
        db.close().unwrap();
    }

    /// A memtable over its threshold is flushed without anybody calling
    /// `flush()`.
    #[test]
    fn a_full_memtable_is_flushed_by_the_thread() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(4 * 1024)
            .maintenance_interval(Duration::ZERO)
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();
        for i in 0..200u64 {
            db.insert(&test_point(
                "cpu",
                "srv-1",
                (i * 1_000_000_000) as i64,
                i as f64,
            ))
            .unwrap();
        }
        assert!(
            wait_until(Duration::from_secs(10), || {
                !db.catalog()
                    .read()
                    .active_segments_for_measurement("cpu")
                    .is_empty()
            }),
            "the maintenance thread should have flushed"
        );
        let plan = db
            .query()
            .measurement("cpu")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        assert_eq!(db.execute(&plan).unwrap().num_rows(), 200);
        db.close().unwrap();
    }

    /// The configured retention is enforced by the background pass — the
    /// only pass there is. It used to be enforced by nothing.
    #[test]
    fn the_configured_retention_is_enforced_by_the_thread() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .retention(Some(Duration::from_secs(3600)))
            .maintenance_interval(Duration::from_millis(50))
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        // Data far in the past, so it is expired, and one recent point so
        // the out-of-order window has closed over the old shard.
        for i in 0..5u64 {
            db.insert(&test_point(
                "cpu",
                "srv-1",
                (i * 1_000_000_000) as i64,
                i as f64,
            ))
            .unwrap();
        }
        let now_ns = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        )
        .unwrap();
        db.insert(&test_point("cpu", "srv-1", now_ns, 99.0))
            .unwrap();
        db.flush().unwrap();

        assert!(
            wait_until(Duration::from_secs(10), || {
                db.catalog()
                    .read()
                    .active_segments_for_measurement("cpu")
                    .len()
                    == 1
            }),
            "the expired shard must have been dropped by the maintenance thread"
        );
        let plan = db
            .query()
            .measurement("cpu")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        assert_eq!(db.execute(&plan).unwrap().num_rows(), 1);
        db.close().unwrap();
    }

    /// Compaction happens on the interval, and dropping the last handle
    /// while the thread runs closes cleanly.
    #[test]
    fn compaction_runs_on_the_interval_and_drop_is_clean() {
        let tmp = TempDir::new().unwrap();
        // Build the segments with the periodic passes off, so the count is
        // known before the thread under test gets to compact them.
        {
            let db = Chronix::open(
                ChronixConfig::builder()
                    .data_dir(tmp.path())
                    .maintenance_interval(Duration::ZERO)
                    .build()
                    .unwrap(),
            )
            .unwrap();
            for i in 0..10u64 {
                db.insert(&test_point(
                    "cpu",
                    "srv-1",
                    (i * 1_000_000_000) as i64,
                    i as f64,
                ))
                .unwrap();
                db.flush().unwrap();
            }
            db.close().unwrap();
        }
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .maintenance_interval(Duration::from_millis(50))
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();
        let before = 10;
        assert!(
            wait_until(Duration::from_secs(10), || {
                db.catalog()
                    .read()
                    .active_segments_for_measurement("cpu")
                    .len()
                    < before
            }),
            "compaction should have reduced the segment count"
        );
        let plan = db
            .query()
            .measurement("cpu")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        assert_eq!(db.execute(&plan).unwrap().num_rows(), 10);
        drop(db);
        // Reopening proves the drop released the lock and left a clean state.
        let db = Chronix::open(
            ChronixConfig::builder()
                .data_dir(tmp.path())
                .build()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(db.wal_replayed_records(), 0);
        db.close().unwrap();
    }
}
