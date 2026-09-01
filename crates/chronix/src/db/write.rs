//! Write-path methods for [`Chronix`] — insert, admission control, backpressure.

use std::collections::HashSet;

use metrics::{counter, gauge, histogram};
use tracing::{debug, warn};

use chronix_core::{wal_encode, wal_encode_write_point, Point, SeriesKey, Timestamp, WalEntry};
use chronix_streaming::cdc::CdcEvent;

use crate::error::{DbError, InsertResult, Result};

impl super::Chronix {
    /// Insert a single data point.
    ///
    /// The point is first written to the WAL for durability, then inserted
    /// into the active memtable. Schema-on-write validation is applied.
    ///
    /// ## Schema Durability
    ///
    /// Schema changes (new measurements, new columns) are captured by
    /// `SchemaRegistry::register_point` and written to the WAL as
    /// [`WalEntry::SchemaChange`] entries in an **atomic batch** together
    /// with the data write.  On crash recovery, `Chronix::open` replays
    /// these entries to rebuild the in-memory schema registry, so schema
    /// changes are never lost even if a crash occurs before the next
    /// catalog flush.  See also [`insert_batch`](Self::insert_batch) which
    /// follows the same pattern.
    ///
    /// # Deduplication
    ///
    /// This method does **not** perform write-time deduplication.  If the
    /// same data point (identical series key + timestamp) is written more
    /// than once — e.g. due to a client retry — both copies are persisted.
    /// Deduplication is instead handled at **read time** via
    /// [`sort_merge_dedup`](chronix_query::dedup::sort_merge_dedup), which
    /// applies last-write-wins semantics across memtable and segment
    /// batches.  This design avoids the latency and lock contention of a
    /// write-side uniqueness check while still guaranteeing correct query
    /// results.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed, WAL write fails, or
    /// schema validation fails.
    #[must_use = "insert errors indicate data loss"]
    pub fn insert(&self, point: &Point) -> Result<()> {
        let _start = std::time::Instant::now();
        self.check_open()?;
        self.apply_backpressure();
        self.check_admission()?;
        self.check_cardinality(point)?;

        // Schema-on-write: detect field type conflicts and emit
        // a counter metric so operators can alert on mistyped writes.
        let schema_actions = match self.schema.register_point(point) {
            Ok(actions) => actions,
            Err(chronix_core::SchemaError::TypeConflict {
                measurement,
                field,
                expected,
                got,
            }) => {
                counter!("chronix_field_type_conflicts_total").increment(1);
                warn!(
                    measurement = %measurement,
                    field = %field,
                    expected = %expected,
                    got = %got,
                    "field type conflict detected — rejecting write"
                );
                return Err(chronix_core::SchemaError::TypeConflict {
                    measurement,
                    field,
                    expected,
                    got,
                }
                .into());
            }
            Err(e) => return Err(e.into()),
        };

        // Schema change + data write as a single atomic WAL
        // batch record. On crash recovery, either both survive (CRC valid)
        // or neither (CRC mismatch). Catalog persistence happens AFTER WAL.
        // Encode point directly without cloning into a WalEntry.
        let data_payload = wal_encode_write_point(point)
            .map_err(|e| DbError::Internal(format!("Failed to serialize WAL entry: {e}")))?;

        let wal_seq = if !schema_actions.is_empty() {
            let schema_entry = WalEntry::SchemaChange {
                actions: schema_actions,
            };
            let schema_payload = wal_encode(&schema_entry).map_err(|e| {
                DbError::Internal(format!("Failed to serialize schema WAL entry: {e}"))
            })?;

            // Atomic batch: schema + data in one CRC-protected record.
            let payloads: Vec<&[u8]> = vec![schema_payload.as_slice(), data_payload.as_slice()];
            let seq = self.wal.append_batch(&payloads)?;

            // Persist schema to catalog AFTER WAL is durable.
            if let Some(ms) = self.schema.lookup(point.series_key().measurement()) {
                let mut catalog = self.catalog.write();
                if let Err(e) = catalog.set_schema((*ms).clone()) {
                    tracing::warn!(error = %e, "failed to persist schema to catalog");
                }
            }

            seq
        } else {
            self.wal.append_durable(&data_payload)?
        };

        // Insert into memtable via shard router
        self.shards.insert_with_wal_seq(point, wal_seq)?;

        // Only allocate CDC event strings when subscribers exist.
        if self.cdc_bus.subscriber_count() > 0 {
            self.cdc_bus.publish(CdcEvent::PointWritten {
                measurement: point.series_key().measurement().to_string(),
                tags: point
                    .series_key()
                    .tags()
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                fields: point
                    .fields()
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.clone()))
                    .collect(),
                timestamp: point.timestamp(),
                seq: 0,
            });
        }

        // Update last-value cache (respects per-measurement config)
        if self.lvc_enabled_for(point.series_key().measurement()) {
            self.lvc.update(point);
        }

        // Check if flush is needed
        self.maybe_flush()?;

        // Ingest-time downsampling — accumulate and emit on bucket boundary
        if self.ingest_downsampler.has_rules() {
            let rollup_points = self.ingest_downsampler.process(point);
            if !rollup_points.is_empty() {
                if let Err(e) = self.insert_batch(&rollup_points) {
                    tracing::warn!(
                        error = %e,
                        count = rollup_points.len(),
                        "ingest-time downsampling rollup insertion failed"
                    );
                    counter!("chronix_downsample_insert_errors_total").increment(1);
                }
            }
        }

        histogram!("chronix_write_duration_seconds").record(_start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Insert a batch of data points.
    ///
    /// All points are first written to the WAL in a single batch operation,
    /// then inserted into the memtable. This is significantly more efficient
    /// than calling [`insert`](Self::insert) in a loop.
    ///
    /// # Deduplication
    ///
    /// Like [`insert`](Self::insert), this method does **not** perform
    /// write-time deduplication.  Duplicate points (same series key +
    /// timestamp) from retried batch writes are resolved at read time by
    /// [`sort_merge_dedup`](chronix_query::dedup::sort_merge_dedup) using
    /// last-write-wins ordering.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed, WAL write fails, or
    /// schema validation fails on any point.
    #[must_use = "insert_batch errors indicate data loss"]
    pub fn insert_batch(&self, points: &[Point]) -> Result<InsertResult> {
        self.check_open()?;
        self.apply_backpressure();
        self.check_admission()?;

        if points.is_empty() {
            return Ok(InsertResult {
                wal_committed: 0,
                memtable_inserted: 0,
                errors: Vec::new(),
            });
        }

        // Cardinality check for all points in the batch
        self.check_cardinality_batch(points)?;

        // Schema-on-write for all points first.
        // Collect schema changes but do NOT persist to catalog
        // until AFTER the WAL has been durably written. This ensures that
        // a crash between schema mutation and WAL write doesn't leave the
        // catalog in an inconsistent state.
        let mut all_schema_actions = Vec::new();
        let mut affected_measurements = Vec::new();
        for point in points {
            // Detect field type conflicts in batch path and emit metric.
            let schema_actions = match self.schema.register_point(point) {
                Ok(actions) => actions,
                Err(chronix_core::SchemaError::TypeConflict {
                    measurement,
                    field,
                    expected,
                    got,
                }) => {
                    counter!("chronix_field_type_conflicts_total").increment(1);
                    warn!(
                        measurement = %measurement,
                        field = %field,
                        expected = %expected,
                        got = %got,
                        "field type conflict detected in batch — rejecting entire batch"
                    );
                    return Err(chronix_core::SchemaError::TypeConflict {
                        measurement,
                        field,
                        expected,
                        got,
                    }
                    .into());
                }
                Err(e) => return Err(e.into()),
            };

            if !schema_actions.is_empty() {
                all_schema_actions.extend(schema_actions);
                let meas = point.series_key().measurement().to_string();
                if !affected_measurements.contains(&meas) {
                    affected_measurements.push(meas);
                }
            }
        }

        // Build WAL payloads: schema changes first, then data points.
        let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(points.len() + 1);

        if !all_schema_actions.is_empty() {
            let schema_entry = WalEntry::SchemaChange {
                actions: all_schema_actions,
            };
            payloads.push(wal_encode(&schema_entry).map_err(|e| {
                DbError::Internal(format!("Failed to serialize schema WAL entry: {e}"))
            })?);
        }

        // Encode points directly without cloning into WalEntry.
        for p in points {
            payloads.push(
                wal_encode_write_point(p).map_err(|e| {
                    DbError::Internal(format!("Failed to serialize WAL entry: {e}"))
                })?,
            );
        }
        let refs: Vec<&[u8]> = payloads.iter().map(|p: &Vec<u8>| p.as_slice()).collect();
        let wal_seq = self.wal.append_batch(&refs)?;

        // Persist schema to catalog AFTER WAL is durable.
        // On crash recovery, WAL replay will re-apply schema changes.
        if !affected_measurements.is_empty() {
            let mut catalog = self.catalog.write();
            for meas in &affected_measurements {
                if let Some(ms) = self.schema.lookup(meas) {
                    if let Err(e) = catalog.set_schema((*ms).clone()) {
                        tracing::warn!(error = %e, measurement = %meas, "failed to persist schema to catalog");
                    }
                }
            }
        }

        // Insert into memtable — best-effort after WAL commit.
        //
        // Once the WAL has all points, we must attempt to insert every
        // point into the memtable. If a single point fails (e.g. shard
        // out-of-range), we log the error and continue with the remaining
        // points rather than returning early and leaving them only in the
        // WAL. This prevents partial-state divergence where some points
        // exist in the memtable + WAL while others exist only in the WAL
        // (to be recovered on restart). The first error encountered is
        // returned to the caller after all points have been attempted.
        let mut memtable_inserted = 0usize;
        let mut errors: Vec<(usize, DbError)> = Vec::new();
        // Only collect CDC events when subscribers exist.
        let has_cdc_subscribers = self.cdc_bus.subscriber_count() > 0;
        let mut cdc_events: Vec<CdcEvent> = Vec::new();
        for (i, point) in points.iter().enumerate() {
            // All points in the batch share the single atomic WAL sequence
            // number — the entire batch was written as one WAL record.
            let seq = wal_seq;
            if let Err(e) = self.shards.insert_with_wal_seq(point, seq) {
                warn!(
                    seq,
                    measurement = point.series_key().measurement(),
                    error = %e,
                    "Failed to insert point into memtable after WAL commit"
                );
                // Enqueue for retry on next flush cycle instead of
                // waiting for a full restart + WAL replay.
                self.recovery_queue.lock().push((point.clone(), seq, 0));
                counter!("chronix_recovery_queue_enqueued_total").increment(1);
                errors.push((i, e.into()));
                continue;
            }

            memtable_inserted += 1;

            // Collect CDC events for batch publish (single flush).
            if has_cdc_subscribers {
                cdc_events.push(CdcEvent::PointWritten {
                    measurement: point.series_key().measurement().to_string(),
                    tags: point
                        .series_key()
                        .tags()
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                    fields: point
                        .fields()
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.clone()))
                        .collect(),
                    timestamp: point.timestamp(),
                    seq: 0,
                });
            }

            // Update last-value cache (respects per-measurement config)
            if self.lvc_enabled_for(point.series_key().measurement()) {
                self.lvc.update(point);
            }
        }

        // Batch-publish CDC events — single durable-log flush
        // instead of one fsync per point.
        if !cdc_events.is_empty() {
            self.cdc_bus.publish_batch(&mut cdc_events);
        }

        // Check if flush is needed
        self.maybe_flush()?;

        if !errors.is_empty() {
            warn!(
                wal_committed = points.len(),
                memtable_inserted,
                memtable_errors = errors.len(),
                "Partial insert: WAL committed but some memtable insertions failed"
            );
        }

        Ok(InsertResult {
            wal_committed: points.len(),
            memtable_inserted,
            errors,
        })
    }

    /// Scan the memtable for points in the given time range.
    ///
    /// This is a low-level method that only reads from the in-memory
    /// buffers, not from persisted segments. For full queries, use
    /// [`execute`](Self::execute).
    pub fn scan_memtable(
        &self,
        series_key: &SeriesKey,
        min_ts: Timestamp,
        max_ts: Timestamp,
    ) -> Vec<Point> {
        self.shards.scan(series_key, min_ts, max_ts)
    }

    /// Apply compaction back-pressure when L0 segments pile up.
    ///
    /// Checks the total L0 segment count (currently tracked as the total
    /// catalog segment count since all fresh flushes are L0) and yields for
    /// a linearly escalating delay if the count exceeds the heavy threshold.
    /// When compaction catches up, the delay drops back to 0 automatically.
    ///
    /// Uses `std::thread::yield_now()` + a short spin instead of
    /// `std::thread::sleep()` which would stall a tokio worker thread.
    fn apply_backpressure(&self) {
        // This function blocks the current OS thread via
        // `thread::park_timeout`.  Callers from async code MUST use
        // `spawn_blocking` or `block_in_place` to avoid stalling a tokio
        // worker thread.
        //
        // NOTE: A previous `debug_assert!(tokio::task::try_id().is_none())`
        // was removed because `try_id()` returns `Some` inside
        // `spawn_blocking` closures as well — Tokio assigns task IDs to
        // blocking tasks.  There is no public Tokio API to distinguish
        // async-worker context from blocking-pool context, so the check
        // produced false positives for correct call-sites.

        let l0_count = {
            let catalog = self.catalog.read();
            catalog.segment_count()
        };
        let delay = self.compaction_picker.backpressure_delay_ms(l0_count);
        gauge!("chronix_compaction_backpressure_active").set(f64::from(u8::from(delay > 0)));
        if delay > 0 {
            debug!(l0_count, delay_ms = delay, "Write backpressure applied");
            // Use parking_lot's wait_for which does NOT stall
            // the entire tokio thread. For the synchronous write path,
            // thread::park_timeout is acceptable (the write path is
            // called from blocking I/O, not async futures).
            std::thread::park_timeout(std::time::Duration::from_millis(delay));
        }
    }

    /// Write admission control — reject writes when the system is
    /// overloaded rather than accepting them and degrading.
    ///
    /// Checks:
    /// 1. Memtable memory exceeds `max_memtable_memory` — the system
    ///    cannot buffer more writes until a flush completes.
    /// 2. WAL file count exceeds a safe threshold (16 files) — the WAL
    ///    is not being truncated fast enough.
    ///
    /// Returns `Err(DbError::TransientOverload)` for memtable pressure
    /// (will resolve after flush) or `Err(DbError::PersistentOverload)` for
    /// WAL backlog (may need operator attention).
    ///
    fn check_admission(&self) -> Result<()> {
        // Check memtable memory against hard limit.
        let mem = self.shards.total_memory();
        let max_mem = self.config.max_memtable_memory;
        if mem >= max_mem {
            warn!(
                memtable_bytes = mem,
                max_memtable_memory = max_mem,
                "write rejected — memtable memory at capacity"
            );
            counter!("chronix_write_admission_rejected_total", "reason" => "memtable_full")
                .increment(1);
            // Signal flush so the system can recover.
            self.flush_notify.notify_one();
            return Err(DbError::TransientOverload {
                reason: format!(
                    "memtable memory at capacity ({mem} bytes >= {max_mem} bytes limit)"
                ),
            });
        }

        // Check WAL file count as a proxy for WAL backlog.
        const WAL_FILE_COUNT_THRESHOLD: usize = 16;
        if let Ok(wal_files) = self.wal.file_count() {
            if wal_files > WAL_FILE_COUNT_THRESHOLD {
                warn!(
                    wal_files,
                    threshold = WAL_FILE_COUNT_THRESHOLD,
                    "write rejected — WAL backlog too large"
                );
                counter!("chronix_write_admission_rejected_total", "reason" => "wal_backlog")
                    .increment(1);
                return Err(DbError::PersistentOverload {
                    reason: format!(
                        "WAL backlog too large ({wal_files} files > {WAL_FILE_COUNT_THRESHOLD} threshold)"
                    ),
                });
            }
        }

        Ok(())
    }

    /// Check cardinality for a single point.
    ///
    /// If the series key is already known, this is a no-op. If it is new,
    /// verifies the exact live-series count won't exceed
    /// `max_series_cardinality`.
    ///
    /// The count comes from `known_series`, which is the *only* cardinality
    /// bookkeeping in the engine. It used to be shadowed by a HyperLogLog
    /// sketch kept "for O(1) counting", and that second structure was wrong in
    /// both directions: `count()` is a 16 384-register scan under a global
    /// mutex (not O(1), and slower than the sharded length it replaced), an
    /// HLL cannot *remove*, so every delete inflated it permanently until the
    /// limit rejected all writes, and the register index was taken from
    /// FNV-1a's low bits — the bits FNV avalanches worst — which undercounted
    /// 1 000 series by 4.3 % against a documented 0.8 % error bound (R1).
    fn check_cardinality(&self, point: &Point) -> Result<()> {
        let canonical = point.series_key().canonical_form();
        // Fast path — lock-free per-shard read.
        if self.known_series.contains(canonical) {
            return Ok(());
        }
        let limit = self.config.max_series_cardinality;
        let current = self.known_series.len();
        if current >= limit {
            return Err(DbError::CardinalityExceeded { current, limit });
        }
        // Early warning when approaching cardinality limit.
        Self::cardinality_early_warning(current, limit);
        self.known_series.insert(canonical.to_string());
        Ok(())
    }

    /// Check cardinality for a batch of points.
    ///
    /// Counts the net-new unique series in the batch and validates the
    /// combined total will not exceed the limit. If the limit would be
    /// exceeded, no series from the batch are registered, ensuring
    /// atomic acceptance.
    fn check_cardinality_batch(&self, points: &[Point]) -> Result<()> {
        let limit = self.config.max_series_cardinality;

        // Collect net-new canonicals from this batch (lock-free reads).
        let mut new_canonicals: HashSet<String> = HashSet::new();
        for point in points {
            let canonical = point.series_key().canonical_form();
            if !self.known_series.contains(canonical) {
                new_canonicals.insert(canonical.to_string());
            }
        }

        if new_canonicals.is_empty() {
            return Ok(());
        }

        let current = self.known_series.len();
        let new_total = current + new_canonicals.len();
        if new_total > limit {
            return Err(DbError::CardinalityExceeded { current, limit });
        }
        // Early warning when approaching cardinality limit.
        Self::cardinality_early_warning(new_total, limit);

        // Commit all new canonicals via per-shard inserts.
        for canonical in new_canonicals {
            self.known_series.insert(canonical);
        }

        Ok(())
    }

    /// Emit early-warning logs when cardinality approaches the limit.
    fn cardinality_early_warning(current: usize, limit: usize) {
        let pct = (current as f64 / limit as f64) * 100.0;
        if pct >= 90.0 {
            tracing::error!(
                current,
                limit,
                "series cardinality at {pct:.0}% of limit — writes will soon be rejected"
            );
            metrics::gauge!("chronix_cardinality_pct").set(pct);
        } else if pct >= 80.0 {
            tracing::warn!(
                current,
                limit,
                "series cardinality at {pct:.0}% of limit — approaching rejection threshold"
            );
            metrics::gauge!("chronix_cardinality_pct").set(pct);
        }
    }

    /// Drain the recovery queue, retrying memtable insertion for
    /// points that previously failed after WAL commit.
    ///
    /// Called at the start of [`maybe_flush`] so that recovered points
    /// are included in the next flush cycle.  Points that still fail
    /// are re-enqueued for the next attempt up to [`MAX_RECOVERY_RETRIES`]
    /// times. After the retry limit, the point is dropped from the queue
    /// (it remains durable in the WAL and will be recovered on restart).
    fn drain_recovery_queue(&self) {
        /// Maximum number of retry attempts before a point is dropped from
        /// the in-memory recovery queue. The point survives in the WAL and
        /// will be recovered on the next restart.
        const MAX_RECOVERY_RETRIES: u32 = 5;
        /// Hard cap on recovery queue length to bound memory usage.
        const MAX_RECOVERY_QUEUE_LEN: usize = 10_000;

        let pending: Vec<(Point, u64, u32)> = {
            let mut q = self.recovery_queue.lock();
            if q.is_empty() {
                return;
            }
            std::mem::take(&mut *q)
        };

        let total = pending.len();
        let mut requeued = 0usize;
        let mut dropped = 0usize;
        for (point, seq, attempt) in pending {
            if let Err(e) = self.shards.insert_with_wal_seq(&point, seq) {
                let next_attempt = attempt + 1;
                if next_attempt >= MAX_RECOVERY_RETRIES {
                    tracing::warn!(
                        seq,
                        measurement = point.series_key().measurement(),
                        attempt = next_attempt,
                        error = %e,
                        "recovery queue retry limit reached — dropping \
                         (point remains durable in WAL for restart recovery)"
                    );
                    counter!("chronix_recovery_queue_dropped_total").increment(1);
                    dropped += 1;
                } else {
                    let mut q = self.recovery_queue.lock();
                    if q.len() < MAX_RECOVERY_QUEUE_LEN {
                        tracing::debug!(
                            seq,
                            measurement = point.series_key().measurement(),
                            attempt = next_attempt,
                            error = %e,
                            "recovery queue retry still failing — re-enqueuing"
                        );
                        q.push((point, seq, next_attempt));
                        requeued += 1;
                    } else {
                        tracing::warn!(
                            seq,
                            "recovery queue at capacity — dropping point \
                             (durable in WAL)"
                        );
                        counter!("chronix_recovery_queue_dropped_total").increment(1);
                        dropped += 1;
                    }
                }
            } else {
                counter!("chronix_recovery_queue_recovered_total").increment(1);
            }
        }

        gauge!("chronix_recovery_queue_depth").set(requeued as f64);
        if requeued > 0 || dropped > 0 {
            tracing::warn!(
                total,
                requeued,
                dropped,
                recovered = total - requeued - dropped,
                "recovery queue partially drained"
            );
        } else if total > 0 {
            tracing::info!(recovered = total, "recovery queue fully drained");
        }
    }

    /// Check if a flush is needed and signal the background scheduler.
    ///
    /// This method is called at the end of `insert()` / `insert_batch()`.
    /// Instead of performing the expensive flush inline (which blocks
    /// the write path), it notifies the background
    /// [`FlushScheduler`](crate::flush_scheduler::FlushScheduler) via
    /// an `Arc<Notify>`.  If no scheduler is running, the notification
    /// is a no-op and the explicit `flush()` / `close()` methods still
    /// flush correctly.
    fn maybe_flush(&self) -> Result<()> {
        // Drain the recovery queue before checking memory — retry
        // points that failed memtable insertion on previous writes.
        self.drain_recovery_queue();

        let mem = self.shards.total_memory();
        gauge!("chronix_memtable_memory_bytes").set(mem as f64);
        if mem > self.config.memtable_flush_threshold {
            // Signal the background flush scheduler — non-blocking.
            self.flush_notify.notify_one();
        }

        // Emergency flush — if total memtable memory exceeds 90% of
        // the hard limit (`max_memtable_memory`), trigger an immediate
        // flush of the active shard to prevent OOM and reduce the window
        // where `CapacityExceeded` errors are returned to clients.
        let emergency_threshold = self.config.max_memtable_memory * 9 / 10;
        if mem > emergency_threshold {
            warn!(
                memtable_bytes = mem,
                max_memtable_memory = self.config.max_memtable_memory,
                "memtable memory exceeds 90% of max — triggering emergency flush"
            );
            counter!("chronix_memtable_emergency_flush_total").increment(1);
            self.flush_notify.notify_one();
        }

        Ok(())
    }
}
