//! Background flush scheduler.
//!
//! Decouples the write path from the expensive flush operation
//! (segment writing, bloom filter construction, WAL truncation) by
//! running flushes on a dedicated Tokio blocking thread.
//!
//! The scheduler is notified when memtable memory exceeds the flush
//! threshold.  It coalesces rapid-fire notifications and runs at most
//! one flush round per debounce interval.
//!
//! # Design
//!
//! ```text
//! insert() ─── maybe_notify_flush() ──► FlushScheduler (Tokio task)
//!                                           │
//!                                           ▼
//!                                    spawn_blocking(flush_round)
//!                                           │
//!                                           ▼
//!                                    flush_shard() × N  →  WAL truncate
//! ```

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tracing::{debug, info, warn};

use crate::db::Chronix;

/// Configuration for the background flush scheduler.
#[derive(Debug, Clone)]
pub struct FlushSchedulerConfig {
    /// Minimum interval between consecutive flush rounds.
    ///
    /// Prevents the scheduler from thrashing when many inserts cross
    /// the threshold in rapid succession. Defaults to 100 ms.
    pub debounce: Duration,
    /// Maximum debounce interval under sustained load. When
    /// flush notifications arrive faster than `debounce`, the actual
    /// delay is clamped to `max_debounce` to prevent unbounded
    /// latency growth. Defaults to 1 second.
    pub max_debounce: Duration,
    /// Timeout for graceful shutdown — the scheduler waits this long
    /// for a running flush to finish before cancelling it.
    pub shutdown_timeout: Duration,
}

impl Default for FlushSchedulerConfig {
    fn default() -> Self {
        Self {
            debounce: Duration::from_millis(100),
            max_debounce: Duration::from_secs(1),
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

/// Background flush scheduler.
///
/// Listens for flush notifications from the write path and performs
/// the expensive flush work on a blocking thread pool, so inserts
/// are never stalled by I/O.
pub struct FlushScheduler {
    db: Arc<Chronix>,
    config: FlushSchedulerConfig,
    shutdown: Arc<Notify>,
    handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl FlushScheduler {
    /// Create a new flush scheduler with default configuration.
    #[must_use]
    pub fn new(db: Arc<Chronix>) -> Self {
        Self {
            db,
            config: FlushSchedulerConfig::default(),
            shutdown: Arc::new(Notify::new()),
            handle: parking_lot::Mutex::new(None),
        }
    }

    /// Create a new flush scheduler with custom configuration.
    #[must_use]
    pub fn with_config(db: Arc<Chronix>, config: FlushSchedulerConfig) -> Self {
        Self {
            db,
            config,
            shutdown: Arc::new(Notify::new()),
            handle: parking_lot::Mutex::new(None),
        }
    }

    /// Start the background flush loop.
    ///
    /// Spawns a Tokio task that waits for flush notifications
    /// (triggered by the write path via `Chronix::flush_notify()`)
    /// and performs the work on a blocking thread.
    pub fn start(&self) {
        let db = self.db.clone();
        let shutdown = self.shutdown.clone();
        let flush_notify = self.db.flush_notify().clone();
        let debounce = self.config.debounce;

        let handle = tokio::spawn(async move {
            info!("Flush scheduler started");

            loop {
                tokio::select! {
                    biased;
                    () = shutdown.notified() => {
                        info!("Flush scheduler shutting down");
                        // Drain one final flush on shutdown.
                        Self::run_flush_round(&db).await;
                        break;
                    }
                    () = flush_notify.notified() => {
                        // Debounce: coalesce multiple rapid notifications.
                        tokio::time::sleep(debounce).await;
                        Self::run_flush_round(&db).await;
                    }
                }
            }

            info!("Flush scheduler stopped");
        });

        *self.handle.lock() = Some(handle);
    }

    /// Signal the scheduler to stop.
    pub fn stop(&self) {
        self.shutdown.notify_one();
    }

    /// Signal the scheduler to stop and wait for the task to complete.
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

    /// Run a single flush round on a blocking thread pool.
    ///
    /// Flushes all shards whose memtable memory exceeds the threshold,
    /// then truncates the WAL up to the max flushed sequence number.
    async fn run_flush_round(db: &Arc<Chronix>) {
        let db = Arc::clone(db);
        let result = tokio::task::spawn_blocking(move || {
            let start = std::time::Instant::now();

            let mem = db.shards().total_memory();
            if mem <= db.config().memtable_flush_threshold {
                debug!(
                    mem_bytes = mem,
                    "Flush scheduler: memory below threshold, skipping"
                );
                return;
            }

            let mut global_max_wal_seq: Option<u64> = None;
            let mut first_error: Option<crate::error::DbError> = None;
            let mut all_flushed = true;

            for shard_id in db.shards().active_shard_ids() {
                // Re-check after each shard — memory may have dropped below threshold.
                if db.shards().total_memory() <= db.config().memtable_flush_threshold {
                    all_flushed = false;
                    break;
                }

                match db.flush_shard(shard_id) {
                    Ok(results) => {
                        for r in &results {
                            if let Some(seq) = r.max_wal_seq {
                                global_max_wal_seq =
                                    Some(global_max_wal_seq.map_or(seq, |c| c.max(seq)));
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            shard = %shard_id,
                            error = %e,
                            "Background flush failed for shard"
                        );
                        if first_error.is_none() {
                            first_error = Some(e);
                        }
                    }
                }
            }

            // Truncate WAL only if ALL shard flushes succeeded AND all
            // shards were visited.
            //
            // Cap the truncation point at the minimum WAL
            // sequence still held by any *active* (unflushed) memtable.
            // Without this, concurrent writers that have inserted records
            // into the current active memtable during the flush round
            // could lose data on crash — the WAL entries backing their
            // in-memory data would already be deleted.  This matches the
            // protection in `Chronix::flush()`.
            if first_error.is_none() && all_flushed {
                if let Some(mut max_seq) = global_max_wal_seq {
                    if let Some(active_min) = db.shards().min_active_wal_seq() {
                        max_seq = max_seq.min(active_min.saturating_sub(1));
                    }
                    if let Err(e) = db.wal().truncate_before(max_seq) {
                        warn!(error = %e, "Failed to truncate WAL after background flush");
                    }
                }
            }

            let elapsed = start.elapsed();
            let final_mem = db.shards().total_memory();
            info!(
                elapsed_ms = elapsed.as_millis(),
                mem_before = mem,
                mem_after = final_mem,
                "Background flush round completed"
            );
            metrics::histogram!("chronix_flush_duration_seconds").record(elapsed.as_secs_f64());
        })
        .await;

        if let Err(e) = result {
            warn!(error = %e, "Background flush task panicked");
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
    async fn flush_scheduler_flushes_on_signal() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(256) // Low threshold
            .build()
            .unwrap();
        let db = Arc::new(Chronix::open(config).unwrap());

        let flush_config = FlushSchedulerConfig {
            debounce: Duration::from_millis(10),
            max_debounce: Duration::from_millis(100),
            shutdown_timeout: Duration::from_secs(5),
        };
        let scheduler = FlushScheduler::with_config(db.clone(), flush_config);
        scheduler.start();

        // Insert enough data to exceed threshold — this triggers
        // maybe_flush() which signals the scheduler via flush_notify.
        for i in 0..20u64 {
            let p = test_point("cpu", "srv-1", (i * 1_000_000_000) as i64, i as f64);
            db.insert(&p).unwrap();
        }

        // Poll for the background flush rather than sleeping a fixed 100 ms:
        // a single fixed sleep raced the debounce window and flaked under
        // parallel test load / CPU contention.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let seg_count = loop {
            let seg_count = {
                let cat = db.catalog().read();
                cat.active_segments_for_measurement("cpu").len()
            };
            if seg_count > 0 || std::time::Instant::now() >= deadline {
                break seg_count;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert!(
            seg_count > 0,
            "expected background flush to create segments"
        );

        scheduler.stop_and_wait().await.unwrap();
        db.close().unwrap();
    }

    #[tokio::test]
    async fn flush_scheduler_stop_without_start_is_noop() {
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

        let scheduler = FlushScheduler::new(db);
        scheduler.stop_and_wait().await.unwrap();
    }

    #[tokio::test]
    async fn flush_scheduler_handles_empty_db() {
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

        let flush_config = FlushSchedulerConfig {
            debounce: Duration::from_millis(10),
            max_debounce: Duration::from_millis(100),
            shutdown_timeout: Duration::from_secs(5),
        };
        let scheduler = FlushScheduler::with_config(db.clone(), flush_config);
        scheduler.start();

        // Signal on empty DB via the db's notify — should be a no-op, not panic
        db.flush_notify().notify_one();
        tokio::time::sleep(Duration::from_millis(50)).await;

        scheduler.stop_and_wait().await.unwrap();
        db.close().unwrap();
    }
}
