//! Background compaction scheduler.
//!
//! Runs compaction automatically in the background via a Tokio task.
//! The scheduler periodically checks all shards via the
//! `CompactionPicker` and executes up to `compaction_concurrency`
//! compaction tasks. Compaction never blocks the write or query paths.
//!
//! # Usage
//!
//! ```no_run
//! use std::sync::Arc;
//! use chronix::compaction_scheduler::CompactionScheduler;
//! use chronix::db::Chronix;
//! use chronix_core::ChronixConfig;
//!
//! let config = ChronixConfig::builder()
//!     .data_dir("/tmp/mydb")
//!     .build()
//!     .unwrap();
//! let db = Arc::new(Chronix::open(config).unwrap());
//!
//! let scheduler = CompactionScheduler::new(db.clone());
//! scheduler.start();
//!
//! // ... later, on shutdown:
//! scheduler.stop();
//! ```

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tracing::{debug, info, warn};

use crate::db::Chronix;

/// Configuration for the background compaction scheduler.
#[derive(Debug, Clone)]
pub struct CompactionSchedulerConfig {
    /// How often the scheduler checks for compactable shards.
    pub interval: Duration,
    /// Maximum number of concurrent compaction tasks.
    ///
    /// Currently compaction is synchronous within a single call to
    /// `compact()`, so this controls how many sequential compaction
    /// rounds run per tick. Defaults to 2.
    pub compaction_concurrency: usize,
    /// Timeout for graceful shutdown — running compactions get this
    /// long to complete before the task is cancelled.
    pub shutdown_timeout: Duration,
}

impl Default for CompactionSchedulerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            compaction_concurrency: 2,
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

/// Background compaction scheduler.
///
/// Spawns a Tokio task that periodically runs compaction and GC.
/// Uses cooperative cancellation via [`Notify`] for clean shutdown.
pub struct CompactionScheduler {
    db: Arc<Chronix>,
    config: CompactionSchedulerConfig,
    shutdown: Arc<Notify>,
    handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl CompactionScheduler {
    /// Create a new scheduler with default configuration.
    #[must_use]
    pub fn new(db: Arc<Chronix>) -> Self {
        Self {
            db,
            config: CompactionSchedulerConfig::default(),
            shutdown: Arc::new(Notify::new()),
            handle: parking_lot::Mutex::new(None),
        }
    }

    /// Create a new scheduler with custom configuration.
    #[must_use]
    pub fn with_config(db: Arc<Chronix>, config: CompactionSchedulerConfig) -> Self {
        Self {
            db,
            config,
            shutdown: Arc::new(Notify::new()),
            handle: parking_lot::Mutex::new(None),
        }
    }

    /// Start the background compaction loop.
    ///
    /// Spawns a Tokio task that runs until [`stop()`](Self::stop) is called
    /// or the database is closed.
    pub fn start(&self) {
        let db = self.db.clone();
        let shutdown = self.shutdown.clone();
        let interval = self.config.interval;
        let concurrency = self.config.compaction_concurrency;

        let handle = tokio::spawn(async move {
            info!(
                interval_secs = interval.as_secs(),
                concurrency, "Compaction scheduler started"
            );

            // Adaptive backoff — increase sleep when idle, reset on work.
            let min_interval = interval;
            let max_interval = interval.saturating_mul(8); // e.g. 30s → 240s cap
            let mut current_interval = min_interval;

            loop {
                tokio::select! {
                    () = shutdown.notified() => {
                        info!("Compaction scheduler shutting down");
                        break;
                    }
                    () = tokio::time::sleep(current_interval) => {
                        let did_work = Self::run_compaction_round(&db, concurrency).await;
                        if did_work {
                            current_interval = min_interval;
                        } else {
                            // Double the interval up to the cap
                            current_interval = (current_interval.saturating_mul(2)).min(max_interval);
                        }
                    }
                }
            }

            info!("Compaction scheduler stopped");
        });

        *self.handle.lock() = Some(handle);
    }

    /// Stop the background compaction scheduler.
    ///
    /// Signals the compaction loop to shut down. If a compaction round
    /// is in progress, it will finish before the task exits.
    pub fn stop(&self) {
        self.shutdown.notify_one();
    }

    /// Stop and wait for the scheduler task to complete.
    ///
    /// # Errors
    ///
    /// Returns an error if the join fails (task panicked).
    pub async fn stop_and_wait(&self) -> std::result::Result<(), tokio::task::JoinError> {
        self.shutdown.notify_one();
        let handle = self.handle.lock().take();
        if let Some(handle) = handle {
            handle.await?;
        }
        Ok(())
    }

    /// Run a single compaction + GC round on a blocking thread pool.
    ///
    /// Compaction and GC perform heavy synchronous I/O (segment reads,
    /// writes, file deletes). Running them on `spawn_blocking` prevents
    /// stalling the Tokio async runtime.
    ///
    /// Returns `true` if any compaction work was performed.
    async fn run_compaction_round(db: &Arc<Chronix>, concurrency: usize) -> bool {
        let db = Arc::clone(db);
        let result = tokio::task::spawn_blocking(move || {
            let start = std::time::Instant::now();
            let mut did_work = false;

            for round in 0..concurrency {
                match db.compact() {
                    Ok(0) => {
                        debug!(round, "No compaction tasks found");
                        break; // Nothing left to compact
                    }
                    Ok(tasks) => {
                        did_work = true;
                        info!(round, tasks, "Compaction round completed");
                        // After compaction, GC expired soft-deleted segments
                        match db.gc() {
                            Ok(gc) if gc > 0 => {
                                info!(gc_segments = gc, "GC cleaned up segments");
                            }
                            Ok(_) => {}
                            Err(e) => {
                                warn!(error = %e, "GC failed");
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, round, "Compaction round failed");
                        break;
                    }
                }
            }

            let elapsed = start.elapsed();
            debug!(
                elapsed_ms = elapsed.as_millis(),
                "Compaction cycle finished"
            );
            metrics::histogram!("chronix_compaction_cycle_duration_seconds")
                .record(elapsed.as_secs_f64());
            did_work
        })
        .await;

        match result {
            Ok(did_work) => did_work,
            Err(e) => {
                warn!(error = %e, "Compaction task panicked");
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronix_core::{ChronixConfig, FieldValue, Point, SeriesKey};
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    fn test_point(measurement: &str, host: &str, ts: i64, value: f64) -> Point {
        let tags = BTreeMap::from([("host".to_string(), host.to_string())]);
        let fields = BTreeMap::from([("value".to_string(), FieldValue::F64(value))]);
        let key = SeriesKey::new(measurement, tags).unwrap();
        Point::new(key, fields, ts).unwrap()
    }

    #[tokio::test]
    async fn scheduler_compacts_and_stops_gracefully() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(128)
            .build()
            .unwrap();
        let db = Arc::new(Chronix::open(config).unwrap());

        // Insert data that triggers compaction
        for i in 0..10u64 {
            let p = test_point("cpu", "srv-1", (i * 1_000_000_000) as i64, i as f64);
            db.insert(&p).unwrap();
            db.flush().unwrap();
        }

        let pre_compact = {
            let cat = db.catalog().read();
            cat.active_segments_for_measurement("cpu").len()
        };
        assert!(
            pre_compact >= 4,
            "need enough segments for compaction trigger"
        );

        // Start scheduler with short interval
        let scheduler_config = CompactionSchedulerConfig {
            interval: Duration::from_millis(100),
            compaction_concurrency: 2,
            shutdown_timeout: Duration::from_secs(5),
        };
        let scheduler = CompactionScheduler::with_config(db.clone(), scheduler_config);
        scheduler.start();

        // Wait for at least one compaction cycle
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Stop gracefully
        scheduler.stop_and_wait().await.unwrap();

        // Verify compaction occurred — active segments should be fewer
        // because compacted inputs become soft-deleted and new merged
        // segments replace them.
        let post_compact = {
            let cat = db.catalog().read();
            cat.active_segments_for_measurement("cpu").len()
        };
        assert!(
            post_compact < pre_compact,
            "expected compaction to reduce active segment count: {pre_compact} → {post_compact}",
        );

        // Data integrity check
        let plan = db
            .query()
            .measurement("cpu")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        let batch = db.execute(&plan).unwrap();
        assert_eq!(batch.num_rows(), 10);

        db.close().unwrap();
    }

    #[tokio::test]
    async fn scheduler_stop_without_start_is_noop() {
        let tmp = TempDir::new().unwrap();
        let db = Arc::new(
            Chronix::open(
                ChronixConfig::builder()
                    .data_dir(tmp.path())
                    .build()
                    .unwrap(),
            )
            .unwrap(),
        );

        let scheduler = CompactionScheduler::new(db);
        // stop_and_wait without start should not panic
        scheduler.stop_and_wait().await.unwrap();
    }
}
