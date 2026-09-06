//! Lifecycle methods for [`Chronix`] — flush, close, compaction, GC, retention.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};

use metrics::{counter, gauge, histogram};
use tracing::{debug, info, warn};

use chronix_core::{SegmentState, ShardId};
use chronix_engine::cache::metadata::CachedSegmentMeta;
use chronix_engine::index::{CatalogColumnStats, SegmentCatalogEntry, TimeIndexEntry};
use chronix_engine::memtable::FlushResult;
use chronix_engine::segment::reader::SegmentReader;
use chronix_query::plan::QueryPlan;

use chronix_engine::compaction::CompactionExecutor;

use crate::error::{DbError, Result};
use crate::export::ParquetExportConfig;
use crate::retention;

impl super::Chronix {
    /// Flush every memtable to segment files and advance the WAL floor.
    ///
    /// Normally the maintenance thread does this when a memtable crosses
    /// its threshold; call it to make everything durable in segments now.
    /// One flush runs at a time; a concurrent caller waits for it.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Closed`] if the database is already closed, or
    /// the first flush error. A shard whose flush failed keeps its
    /// memtable — and its WAL records — for the next attempt; nothing is
    /// lost by a failed flush.
    #[must_use = "flush errors may indicate data not persisted"]
    pub fn flush(&self) -> Result<Vec<FlushResult>> {
        let start = std::time::Instant::now();
        self.check_open()?;
        let _gate = self.flush_gate.lock();

        let mut all_results = Vec::new();
        let mut first_error: Option<DbError> = None;
        for shard_id in self.shards.shard_ids() {
            match self.flush_shard(shard_id) {
                Ok(results) => all_results.extend(results),
                Err(e) => {
                    warn!(shard = %shard_id, error = %e, "Flush failed for shard");
                    first_error.get_or_insert(e);
                }
            }
        }
        // The floor is what the memtables say it is, so it can be raised
        // after a partial failure too: the failed shard's records are
        // still unflushed and still cap it.
        self.raise_wal_floor_to_unflushed();
        self.shards.retire_idle_shards();

        histogram!("chronix_flush_duration_seconds").record(start.elapsed().as_secs_f64());
        match first_error {
            Some(e) => Err(e),
            None => Ok(all_results),
        }
    }

    /// Record in the catalog that every WAL record below the oldest
    /// unflushed one is in a segment, and drop the WAL files that hold
    /// nothing newer.
    ///
    /// **The floor is derived from what is unflushed, never from what was
    /// just flushed.** `floor = min_unflushed_seq − 1`, or the newest
    /// sequence number when nothing is unflushed. "Unflushed" covers every
    /// memtable — active or frozen, in any shard — and every write between
    /// its WAL append and its memtable insert, which the write epoch
    /// excludes by construction: writers hold the epoch for read across
    /// that gap, and this takes it for write.
    ///
    /// Deriving it from what was *just flushed* instead cannot see a frozen
    /// memtable another flush is still writing, nor a write appended but not
    /// yet inserted. Either lets the floor pass an acknowledged record, and
    /// a record below the floor is never replayed.
    ///
    /// The floor lives in the catalog because truncation cannot express it:
    /// the active WAL file is never deleted, so without a recorded floor
    /// every open replayed it in full.
    pub(super) fn raise_wal_floor_to_unflushed(&self) {
        let floor = {
            let _no_writer_in_flight = self.write_epoch.write();
            match self.shards.min_unflushed_wal_seq() {
                Some(oldest) => oldest.saturating_sub(1),
                None => self.wal.current_sequence(),
            }
        };
        if floor == 0 {
            return;
        }
        if let Err(e) = self.catalog.write().set_wal_floor(floor) {
            warn!(error = %e, "Failed to record the WAL floor in the catalog");
            return;
        }
        if let Err(e) = self.wal.truncate_before(floor) {
            warn!(error = %e, "Failed to truncate WAL after flush");
        }
    }

    /// Close the database, flushing all pending data.
    ///
    /// After closing, all subsequent operations will return
    /// [`DbError::Closed`].
    ///
    /// # Errors
    ///
    /// Returns any error encountered while flushing shards or syncing
    /// the WAL during shutdown. A failed close can be retried.
    #[must_use = "close errors may indicate data not persisted"]
    pub fn close(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(()); // Already closed
        }

        info!("Closing Chronix database");

        // Stop the maintenance thread first so no pass runs under a closing
        // database. A close that runs *on* that thread — the last handle
        // dropped there — must not join itself.
        self.stop_maintenance.store(true, Ordering::Release);
        self.wake_maintenance();
        let on_maintenance_thread = self
            .maintenance_thread_id
            .get()
            .is_some_and(|id| *id == std::thread::current().id());
        if !on_maintenance_thread {
            let handle = self.maintenance_thread.lock().take();
            if let Some(handle) = handle {
                if handle.join().is_err() {
                    warn!("maintenance thread panicked");
                }
            }
        }

        {
            let _gate = self.flush_gate.lock();
            let mut any_failed = false;
            for shard_id in self.shards.shard_ids() {
                if let Err(e) = self.flush_shard(shard_id) {
                    warn!(shard = %shard_id, error = %e, "Error flushing shard during close");
                    any_failed = true;
                }
            }
            // Everything is in segments now, so the floor reaches the newest
            // record — including a delete record, which needs no replay
            // because its tombstones live in the catalog. Then leave an
            // *empty* active WAL file behind: rotate, and drop the file that
            // held the flushed records, so the next open reads nothing.
            self.raise_wal_floor_to_unflushed();
            if !any_failed {
                match self.wal.rotate() {
                    Ok(()) => self.raise_wal_floor_to_unflushed(),
                    Err(e) => warn!(error = %e, "Failed to rotate WAL during close"),
                }
            }
        }

        // Force catalog snapshot
        let mut catalog = self.catalog.write();
        catalog.force_snapshot()?;

        // Sync WAL
        self.wal.sync()?;

        // Mark as closed only after everything has been persisted
        // successfully, so a failed close can be retried.
        self.closed.store(true, Ordering::Release);

        // A closed database is not this process's any more: release the
        // directory lock now rather than when the last handle drops, so
        // `close()` followed by `open()` of the same directory works.
        if let Err(e) = fs2::FileExt::unlock(&self._lock_file) {
            warn!(error = %e, "Failed to release the database lock file");
        }

        info!("Chronix database closed");
        Ok(())
    }

    /// Whether the database can accept writes at all, and why not.
    ///
    /// **Persistent** conditions only: closed, a WAL poisoned by a failed
    /// `fsync`, or a maintenance thread that has ended — each refuses every
    /// write until the database is reopened. Transient back-pressure — a full
    /// memtable waiting on a flush — is deliberately not unready: that is the
    /// moment to keep serving and let back-pressure work, not the moment to
    /// leave the load balancer.
    ///
    /// The maintenance thread is here because a database that has lost it
    /// looks perfectly healthy right up to the moment it stops accepting
    /// writes for ever: nothing flushes, compacts, materialises a rollup or
    /// expires a shard again, and the memtable fills at whatever rate the
    /// workload writes. Reporting it while writes still succeed is what gives
    /// an orchestrator time to restart the process instead of discovering it
    /// through a wall of refusals.
    ///
    /// `chronixd`'s `/ready` is the caller.
    ///
    /// # Errors
    ///
    /// [`DbError::Closed`] or [`DbError::PersistentOverload`], with the
    /// reason.
    pub fn check_writable(&self) -> Result<()> {
        self.check_open()?;
        if self.wal.is_poisoned() {
            return Err(DbError::PersistentOverload {
                reason: "the WAL writer is poisoned after a failed fsync; the \
                         database must be closed and reopened"
                    .into(),
            });
        }
        if !self.maintenance_alive.load(Ordering::Acquire) {
            return Err(DbError::PersistentOverload {
                reason: "the maintenance thread is not running, so nothing flushes, \
                         compacts, materialises a rollup or expires a shard; the \
                         database must be closed and reopened"
                    .into(),
            });
        }
        Ok(())
    }

    /// Check if the database is open.
    pub(super) fn check_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(DbError::Closed);
        }
        Ok(())
    }

    /// Export query results to a Parquet file.
    ///
    /// For a `Scan` plan the export is genuinely streaming end to end: the
    /// query is driven by [`execute_iter`](Self::execute_iter), which reads
    /// one time-disjoint bucket of segments at a time, and each batch is
    /// written and dropped before the next is read. Peak memory is bounded by
    /// the busiest time bucket, not by the size of the export — a gateway can
    /// export a window far larger than its RAM.
    ///
    /// Aggregate, downsample and window plans have to see their whole input,
    /// so those fall back to materialised execution.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed, the query fails,
    /// or the Parquet file cannot be written.
    pub fn export_parquet(
        &self,
        plan: &QueryPlan,
        output_path: &std::path::Path,
        parquet_config: &ParquetExportConfig,
    ) -> Result<crate::export::ParquetExportResult> {
        self.check_open()?;

        let result = if matches!(plan, QueryPlan::Scan { .. }) {
            // An export is explicitly for a window larger than RAM, so it is
            // *meant* to outlast a request deadline; `query_timeout` is a
            // bound on somebody's patience and there is nobody here.
            crate::export::write_parquet(
                self.execute_iter(plan)?.without_deadline(),
                output_path,
                parquet_config,
            )?
        } else {
            let batches = self.execute_stream(plan)?.into_iter().map(Ok);
            crate::export::write_parquet(batches, output_path, parquet_config)?
        };
        info!(
            path = %output_path.display(),
            rows = result.rows_written,
            bytes = result.bytes_written,
            truncated = result.truncated,
            "Parquet export complete"
        );
        if result.truncated {
            warn!(
                path = %output_path.display(),
                rows = result.rows_written,
                "Parquet export hit its size budget and is incomplete"
            );
        }
        Ok(result)
    }

    /// Enforce a retention policy, dropping every shard whose data falls
    /// entirely before `now - retention`.
    ///
    /// Takes a [`Duration`](std::time::Duration), as
    /// [`ChronixConfig::retention`](chronix_core::ChronixConfig) does. It used
    /// to take a bare `i64` of **nanoseconds**, so the natural reading of the
    /// configuration — `retention(Duration::from_secs(86_400))` — translated
    /// to `enforce_retention(86_400)`, which is 86 microseconds of retention
    /// and deletes the database. A unit a caller has to remember is not a
    /// unit; this one cannot be got wrong.
    ///
    /// Returns the number of segments deleted.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed.
    #[must_use = "retention errors must be handled"]
    pub fn enforce_retention(
        &self,
        retention: std::time::Duration,
    ) -> Result<retention::RetentionResult> {
        let ns = i64::try_from(retention.as_nanos()).unwrap_or(i64::MAX);
        self.enforce_retention_inner(Some(ns))
    }

    /// Enforce every configured retention rule — the global one if there is
    /// one, plus every per-measurement and per-rollup rule.
    ///
    /// This is what the maintenance thread calls. It used to call
    /// `enforce_retention` only when a *global* retention was configured,
    /// so a database that set a rule for one measurement, or a rollup that
    /// declared its own `retention_ns`, expired nothing at all.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed.
    #[must_use = "retention errors must be handled"]
    pub fn enforce_configured_retention(&self) -> Result<retention::RetentionResult> {
        let global = self
            .config
            .retention
            .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX));
        self.enforce_retention_inner(global)
    }

    /// Is any retention rule configured at all?
    #[must_use]
    pub(crate) fn has_retention_rules(&self) -> bool {
        self.config.retention.is_some()
            || !self.config.measurement_retention.is_empty()
            || self
                .rollup_registry
                .read()
                .list()
                .iter()
                .any(|c| c.retention_ns.is_some())
    }

    #[allow(clippy::too_many_lines)]
    fn enforce_retention_inner(
        &self,
        global_retention_ns: Option<i64>,
    ) -> Result<retention::RetentionResult> {
        self.check_open()?;

        let now_ns = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        )
        .unwrap_or(i64::MAX);
        // Without a global rule nothing expires by age alone; the
        // per-measurement pass below still runs.
        let global_cutoff =
            global_retention_ns.map_or(i64::MIN, |ns| retention::retention_cutoff(now_ns, ns));

        // Build per-measurement cutoffs from config overrides, plus the
        // retention each rollup declares for its own target measurement.
        let mut per_measurement_cutoffs: std::collections::HashMap<String, i64> = self
            .config
            .measurement_retention
            .iter()
            .map(|(m, dur)| {
                let retention_ns_m = i64::try_from(dur.as_nanos()).unwrap_or(i64::MAX);
                (
                    m.clone(),
                    retention::retention_cutoff(now_ns, retention_ns_m),
                )
            })
            .collect();
        for rollup in self.rollup_registry.read().list() {
            if let Some(r) = rollup.retention_ns {
                per_measurement_cutoffs
                    .entry(rollup.target_measurement.clone())
                    .or_insert_with(|| retention::retention_cutoff(now_ns, r));
            }
        }

        // Rollups first: a shard may only be dropped once every rollup its
        // measurements feed has been materialised past it.
        if let Err(e) = self.materialise_rollups() {
            warn!(error = %e, "retention: rollup materialisation failed — raw data that feeds a rollup is preserved");
        }

        // Shard bounds and the segments in each shard come from **one**
        // snapshot of the catalog, taken under one lock.
        //
        // They used to come from different places: the bounds from the time
        // index and the segment list from the catalog, read one after the
        // other. A flush or a compaction landing between the two put a
        // segment in the catalog that the index did not know about, so its
        // shard looked entirely expired and the pass deleted a segment
        // newer than the cutoff. Deriving both from the same snapshot makes
        // the window unrepresentable rather than merely unlikely.
        let (shard_bounds, segments_by_shard) = {
            let catalog = self.catalog.read();
            let mut bounds: BTreeMap<ShardId, (i64, i64)> = BTreeMap::new();
            let mut by_shard: BTreeMap<ShardId, Vec<SegmentCatalogEntry>> = BTreeMap::new();
            for entry in catalog.all_segments() {
                if entry.state != SegmentState::Active {
                    continue;
                }
                let b = bounds.entry(entry.shard_id).or_insert((i64::MAX, i64::MIN));
                b.0 = b.0.min(entry.min_timestamp);
                b.1 = b.1.max(entry.max_timestamp);
                by_shard
                    .entry(entry.shard_id)
                    .or_default()
                    .push(entry.clone());
            }
            (bounds, by_shard)
        };

        // Use the global cutoff for whole-shard drops.
        let expired = retention::shards_to_drop(&shard_bounds, global_cutoff);

        let mut total_segments: usize = 0;
        let mut total_bytes: u64 = 0;

        for &shard_id in &expired {
            // From the same snapshot the bounds came from.
            let entries: Vec<SegmentCatalogEntry> = segments_by_shard
                .get(&shard_id)
                .cloned()
                .unwrap_or_default();

            // Rollup-aware retention: raw data is dropped only once every
            // rollup it feeds — the whole chain — has been materialised
            // past this shard. A segment that is still needed is preserved
            // and counted; the next pass tries again.
            let shard_end = shard_bounds.get(&shard_id).map_or(i64::MIN, |b| b.1);
            let mut protected: std::collections::HashSet<chronix_core::SegmentId> =
                std::collections::HashSet::new();
            for entry in &entries {
                // A measurement with its own retention — a rollup tier kept
                // for years beside raw data kept for days — is judged by
                // that, not by the global cutoff that expired the shard.
                if let Some(&own_cutoff) = per_measurement_cutoffs.get(&entry.measurement) {
                    if entry.max_timestamp >= own_cutoff {
                        protected.insert(entry.segment_id);
                        continue;
                    }
                }
                if !self.rollups_materialised_past(&entry.measurement, shard_end.saturating_add(1))
                {
                    warn!(
                        segment_id = ?entry.segment_id,
                        measurement = %entry.measurement,
                        "retention: rollups not yet materialised past this shard — preserving segment"
                    );
                    counter!("chronix_retention_segments_awaiting_rollup_total").increment(1);
                    protected.insert(entry.segment_id);
                }
            }

            let mut catalog = self.catalog.write();
            let mut time_idx = self.time_index.write();
            let mut blooms = self.blooms.write();

            for entry in &entries {
                if protected.contains(&entry.segment_id) {
                    continue;
                }
                total_bytes += entry.byte_size;
                total_segments += 1;

                // Remove catalog entry FIRST so queries stop referencing
                // this segment before its files are deleted (crash-safe order).
                if let Err(e) = catalog.remove_segment(entry.segment_id) {
                    warn!(segment_id = ?entry.segment_id, error = %e, "retention: failed to remove catalog entry");
                }
                blooms.remove(&entry.segment_id.0);
                self.tag_index.remove_segment(entry.segment_id);
                self.metadata_cache.remove(entry.segment_id);
                self.segment_cache.invalidate_segment(entry.segment_id);

                // Remove files after catalog is updated
                if let Err(e) = std::fs::remove_file(&entry.path) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        warn!(path = %entry.path.display(), error = %e, "retention: failed to remove segment file");
                    }
                }
                if let Err(e) = chronix_engine::index::series_index::remove(&entry.path) {
                    warn!(path = %entry.path.display(), error = %e, "retention: failed to remove series index");
                }
            }

            // The shard's time index goes only when nothing in it survived.
            if protected.is_empty() {
                time_idx.remove(&shard_id);
            } else if let Some(ti) = time_idx.get_mut(&shard_id) {
                for entry in &entries {
                    if !protected.contains(&entry.segment_id) {
                        let _ = ti.remove_segment(entry.segment_id);
                    }
                }
            }
        }

        // Per-measurement retention: drop individual segments in
        // non-expired shards when a measurement has a shorter retention.
        if !per_measurement_cutoffs.is_empty() {
            let seg_to_drop: Vec<SegmentCatalogEntry> = {
                let catalog = self.catalog.read();
                catalog
                    .all_segments()
                    .into_iter()
                    .filter(|e| {
                        if let Some(&m_cutoff) = per_measurement_cutoffs.get(&e.measurement) {
                            e.max_timestamp < m_cutoff
                        } else {
                            false
                        }
                    })
                    .cloned()
                    .collect()
            };

            // A measurement's own retention is still subject to the rollup
            // gate: dropping raw data whose 15-minute tier has not been
            // computed destroys exactly what the tier was traded for. The
            // whole-shard pass above checks this; this one did not.
            let seg_to_drop: Vec<SegmentCatalogEntry> = seg_to_drop
                .into_iter()
                .filter(|e| {
                    if self.rollups_materialised_past(
                        &e.measurement,
                        e.max_timestamp.saturating_add(1),
                    ) {
                        true
                    } else {
                        warn!(
                            segment_id = ?e.segment_id,
                            measurement = %e.measurement,
                            "retention: rollups not yet materialised past this segment — preserving it"
                        );
                        counter!("chronix_retention_segments_awaiting_rollup_total").increment(1);
                        false
                    }
                })
                .collect();

            if !seg_to_drop.is_empty() {
                let mut catalog = self.catalog.write();
                let mut time_idx = self.time_index.write();
                let mut blooms = self.blooms.write();

                for entry in &seg_to_drop {
                    total_bytes += entry.byte_size;
                    total_segments += 1;

                    // Remove catalog/index entries FIRST (crash-safe order)
                    if let Err(e) = catalog.remove_segment(entry.segment_id) {
                        warn!(segment_id = ?entry.segment_id, error = %e, "retention: failed to remove catalog entry");
                    }
                    blooms.remove(&entry.segment_id.0);
                    self.tag_index.remove_segment(entry.segment_id);
                    self.metadata_cache.remove(entry.segment_id);
                    self.segment_cache.invalidate_segment(entry.segment_id);
                    if let Some(ti) = time_idx.get_mut(&entry.shard_id) {
                        if !ti.remove_segment(entry.segment_id) {
                            warn!(segment_id = ?entry.segment_id, "retention: time-index entry not found");
                        }
                    }

                    // Remove files after catalog is updated
                    if let Err(e) = std::fs::remove_file(&entry.path) {
                        if e.kind() != std::io::ErrorKind::NotFound {
                            warn!(path = %entry.path.display(), error = %e, "retention: failed to remove segment file");
                        }
                    }
                    if let Err(e) = chronix_engine::index::series_index::remove(&entry.path) {
                        warn!(path = %entry.path.display(), error = %e, "retention: failed to remove series index");
                    }
                }
            }
        }

        let result = retention::RetentionResult {
            shards_dropped: expired.len(),
            segments_deleted: total_segments,
            bytes_freed: total_bytes,
        };

        if result.shards_dropped > 0 {
            counter!("chronix_retention_shards_dropped_total")
                .increment(result.shards_dropped as u64);
            info!(
                shards = result.shards_dropped,
                segments = result.segments_deleted,
                bytes = result.bytes_freed,
                "Retention enforced"
            );
        }

        Ok(result)
    }

    // ── Compaction ──────────────────────────────────────────────────

    /// Run compaction on all eligible shards.
    ///
    /// Uses the `CompactionPicker` to identify shards with enough L0
    /// segments to merit compaction, then runs the
    /// [`CompactionExecutor`] for each task. Input segments are removed
    /// from the catalog and disk after successful compaction.
    ///
    /// # Returns
    ///
    /// The number of compaction tasks executed.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed or I/O fails.
    #[allow(clippy::too_many_lines)]
    pub fn compact(&self) -> Result<usize> {
        self.check_open()?;

        // Prevent concurrent compaction runs. If another thread
        // is already compacting, return immediately with 0 tasks.
        if self.compaction_running.swap(true, Ordering::AcqRel) {
            return Ok(0);
        }
        // Ensure the flag is cleared even on panic / early return.
        struct CompactionGuard<'a>(&'a AtomicBool);
        impl Drop for CompactionGuard<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _guard = CompactionGuard(&self.compaction_running);

        // Flush first so all memtable data is in segments
        self.flush()?;

        // Rollups ride on the background pass whether or not anything gets
        // compacted: a bucket becomes final by time passing, not by segment
        // count — and a gateway writing a few megabytes an hour never
        // reaches the compaction trigger at all. This runs *before* the
        // early return below for exactly that reason; it once ran after it,
        // and the `storage_lifecycle` example showed an empty rollup table.
        if let Err(e) = self.materialise_rollups() {
            warn!(error = %e, "Rollup materialisation failed");
        }

        let segments: Vec<SegmentCatalogEntry> = {
            let catalog = self.catalog.read();
            catalog
                .all_segments()
                .into_iter()
                .filter(|e| e.state == SegmentState::Active)
                .cloned()
                .collect()
        };

        let segments_dir = self.config.data_dir.join("segments");
        let tasks = self.compaction_picker.pick(&segments, &segments_dir);

        // Emit compaction backlog metrics.
        let pending_input_segments: usize = tasks.iter().map(|t| t.input_segments.len()).sum();
        gauge!("chronix_compaction_pending_segments").set(pending_input_segments as f64);
        gauge!("chronix_compaction_pending_tasks").set(tasks.len() as f64);

        if tasks.is_empty() {
            return Ok(0);
        }

        let executor = CompactionExecutor::new(
            self.config.compression != chronix_core::CompressionCodec::None,
            self.config.float_encoding,
            65_536,
            self.config.compression,
            3,
        );
        let tombstones = self.tombstones.read().clone();
        let mut completed = 0;
        let compaction_start = std::time::Instant::now();

        // Ensure output directories exist before spawning threads.
        for task in &tasks {
            if let Some(parent) = task.output_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
        }

        // Run compaction I/O in parallel — each task operates on a unique
        // (shard_id, measurement) combination, so there is zero data overlap.
        let max_par = self.config.compaction_concurrency.max(1);
        let execution_results: Vec<_> = std::thread::scope(|s| {
            let mut all_results = Vec::with_capacity(tasks.len());
            for chunk in tasks.chunks(max_par) {
                let handles: Vec<_> = chunk
                    .iter()
                    .map(|task| {
                        let executor = &executor;
                        let tombstones = &tombstones;
                        s.spawn(move || executor.execute(task, tombstones))
                    })
                    .collect();
                for h in handles {
                    all_results.push(h.join().unwrap_or_else(|_| {
                        Err(chronix_engine::compaction::CompactionError::Internal(
                            "compaction thread panicked".into(),
                        ))
                    }));
                }
            }
            all_results
        });

        // Process results sequentially — catalog/index updates require
        // write locks and must not race.
        for (task, result) in tasks.iter().zip(execution_results) {
            match result {
                Ok(meta) if meta.row_count == 0 => {
                    // All rows tombstoned — no output segment to register,
                    // but clean up old input segments from indexes.
                    {
                        let mut time_idx = self.time_index.write();
                        let mut blooms = self.blooms.write();

                        for input in &task.input_segments {
                            if let Some(idx) = time_idx.get_mut(&input.shard_id) {
                                if !idx.remove_segment(input.segment_id) {
                                    warn!(segment_id = ?input.segment_id, "compact: stale time-index entry already removed");
                                }
                            }
                            blooms.remove(&input.segment_id.0);
                            self.tag_index.remove_segment(input.segment_id);
                            self.metadata_cache.remove(input.segment_id);
                            self.segment_cache.invalidate_segment(input.segment_id);
                        }
                    }

                    // Soft-delete the input segments so GC picks them up
                    {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        let mut catalog = self.catalog.write();
                        for input in &task.input_segments {
                            if let Err(e) = catalog.soft_delete_segment(input.segment_id, now_ms) {
                                warn!(segment_id = ?input.segment_id, error = %e, "compact: failed to soft-delete input segment");
                            }
                        }
                    }

                    info!(
                        shard = %task.shard_id,
                        input = task.input_segments.len(),
                        "Compaction: all rows tombstoned — input segments cleaned up"
                    );
                    completed += 1;
                }
                Ok(meta) => {
                    // Register the new compacted segment
                    let segment_id = {
                        let mut catalog = self.catalog.write();
                        let seg_id = catalog.next_segment_id();

                        let (col_stats, seg_header, col_metas) =
                            match SegmentReader::open(&meta.path) {
                                Ok(reader) => {
                                    let stats = reader
                                        .column_metadata()
                                        .iter()
                                        .map(|cm| CatalogColumnStats {
                                            name: cm.name.clone(),
                                            data_type: cm.data_type,
                                            role: cm.role,
                                            stats: cm.stats.clone(),
                                        })
                                        .collect();
                                    let header = reader.header().clone();
                                    let cols = reader.column_metadata().to_vec();
                                    (stats, Some(header), cols)
                                }
                                Err(e) => {
                                    warn!(
                                        segment = %meta.path.display(),
                                        error = %e,
                                        "Could not read column stats for compacted segment"
                                    );
                                    (Vec::new(), None, Vec::new())
                                }
                            };

                        let entry = SegmentCatalogEntry {
                            segment_id: seg_id,
                            shard_id: task.shard_id,
                            measurement: task.input_segments[0].measurement.clone(),
                            path: meta.path.clone(),
                            min_timestamp: meta.min_timestamp,
                            max_timestamp: meta.max_timestamp,
                            row_count: meta.row_count,
                            series_count: meta.series_count,
                            byte_size: meta.byte_size,
                            row_group_count: meta.row_group_count,
                            column_count: meta.column_count,
                            column_stats: col_stats,
                            state: SegmentState::Active,
                        };
                        // Output in, inputs out, tombstones retargeted — one
                        // step, so a delete issued during the merge keeps
                        // masking the rows the merge carried over.
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        let inputs: Vec<chronix_core::SegmentId> =
                            task.input_segments.iter().map(|s| s.segment_id).collect();
                        catalog.complete_compaction(entry, &inputs, now_ms)?;
                        // Keep the in-memory set in step with the catalog's.
                        self.tombstones.write().extend_to_compaction_output(
                            &inputs.iter().map(|i| i.0).collect::<Vec<_>>(),
                            seg_id.0,
                        );

                        // Populate metadata cache for new segment
                        if let Some(header) = seg_header {
                            self.metadata_cache.insert(CachedSegmentMeta {
                                segment_id: seg_id,
                                header,
                                columns: col_metas,
                            });
                        }

                        seg_id
                    };

                    // The writer already knows the compacted segment's
                    // series; persist them beside it before the old index
                    // entries go, so a concurrent tag-filtered query never
                    // sees a window with neither.
                    if let Err(e) = chronix_engine::index::series_index::write(
                        &meta.path,
                        &meta.series_keys,
                        chronix_engine::index::series_index::SegmentStamp::of(&meta.header),
                    ) {
                        warn!(
                            segment = %meta.path.display(),
                            error = %e,
                            "Failed to write series index for compacted segment"
                        );
                    }

                    // Clean up old segment files and indexes
                    {
                        let mut time_idx = self.time_index.write();
                        let mut blooms = self.blooms.write();

                        for input in &task.input_segments {
                            // Remove old index entries (files are kept until GC)
                            if let Some(idx) = time_idx.get_mut(&input.shard_id) {
                                if !idx.remove_segment(input.segment_id) {
                                    warn!(segment_id = ?input.segment_id, "compact: stale time-index entry already removed");
                                }
                            }
                            blooms.remove(&input.segment_id.0);
                            self.metadata_cache.remove(input.segment_id);
                            self.segment_cache.invalidate_segment(input.segment_id);
                        }

                        // Add new time index entry
                        let idx = time_idx.entry(task.shard_id).or_default();
                        idx.add_segment(TimeIndexEntry {
                            segment_id,
                            min_ts: meta.min_timestamp,
                            max_ts: meta.max_timestamp,
                        });
                    }

                    // Atomically swap old tag index entries with new ones —
                    // no window where concurrent queries miss data.
                    let tag_pairs =
                        chronix_engine::index::series_index::tag_pairs(&meta.series_keys);
                    if !tag_pairs.is_empty() {
                        let old_ids: Vec<chronix_core::SegmentId> =
                            task.input_segments.iter().map(|s| s.segment_id).collect();
                        self.tag_index
                            .replace_segments(&old_ids, segment_id, &tag_pairs);
                    }
                    if let Some(bloom) =
                        chronix_engine::index::series_index::bloom(&meta.series_keys)
                    {
                        self.blooms.write().insert(segment_id.0, bloom);
                    }

                    info!(
                        shard = %task.shard_id,
                        input = task.input_segments.len(),
                        output_rows = meta.row_count,
                        output_bytes = meta.byte_size,
                        "Compaction task completed"
                    );
                    completed += 1;
                }
                Err(e) => {
                    warn!(
                        shard = %task.shard_id,
                        error = %e,
                        "Compaction task failed"
                    );
                }
            }
        }

        if completed > 0 {
            info!(tasks = completed, "Compaction pass completed");

            // Emit compaction metrics.
            let merged_segments: usize = tasks.iter().map(|t| t.input_segments.len()).sum();
            counter!("chronix_compaction_segments_merged_total").increment(merged_segments as u64);
            counter!("chronix_compaction_tasks_completed_total").increment(completed as u64);
            histogram!("chronix_compaction_duration_seconds")
                .record(compaction_start.elapsed().as_secs_f64());
            // Reset pending gauge after compaction completes.
            gauge!("chronix_compaction_pending_segments").set(0.0);
            gauge!("chronix_compaction_pending_tasks").set(0.0);

            // Compaction is what materialises a delete: the merge drops
            // tombstoned rows as it writes the output segment. Reclaiming
            // afterwards is therefore the one moment a tombstone can be
            // proven dead.
            self.gc_tombstones();
        }

        if completed > 0 {
            // After compaction, check for overlapping segments in
            // all measurements that were compacted.
            {
                let compacted_measurements: HashSet<String> = tasks
                    .iter()
                    .filter_map(|t| t.input_segments.first())
                    .map(|s| s.measurement.clone())
                    .collect();
                for m in &compacted_measurements {
                    self.detect_segment_overlaps(m);
                }
            }
        }

        Ok(completed)
    }

    /// Garbage-collect segments that have been soft-deleted past the grace
    /// period.
    ///
    /// Returns the number of segments hard-deleted.
    ///
    /// The default grace period is 5 minutes (300 000 ms). Callers can
    /// invoke this after `compact()` or on a periodic schedule.
    pub fn gc(&self) -> Result<usize> {
        self.gc_with_grace(300_000)
    }

    /// Garbage-collect with a custom grace period (in milliseconds).
    pub fn gc_with_grace(&self, grace_period_ms: u64) -> Result<usize> {
        self.check_open()?;

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let expired: Vec<(
            chronix_core::SegmentId,
            chronix_core::ShardId,
            std::path::PathBuf,
        )> = {
            let catalog = self.catalog.read();
            catalog
                .expired_soft_deleted(now_ms, grace_period_ms)
                .into_iter()
                .map(|e| (e.segment_id, e.shard_id, e.path.clone()))
                .collect()
        };

        if expired.is_empty() {
            return Ok(0);
        }

        // Lock order: catalog → time_index → blooms (consistent with
        // execute_retention, compact, drop_measurement).
        let mut catalog = self.catalog.write();
        let mut time_idx = self.time_index.write();
        let mut blooms = self.blooms.write();
        let mut removed = 0usize;
        for (seg_id, shard_id, path) in &expired {
            // Remove files from disk
            if let Err(e) = std::fs::remove_file(path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!(path = %path.display(), error = %e, "gc: failed to remove segment file");
                }
            }
            if let Err(e) = chronix_engine::index::series_index::remove(path) {
                warn!(path = %path.display(), error = %e, "gc: failed to remove series index");
            }
            // Remove from catalog (manifest entry recorded)
            if let Err(e) = catalog.remove_segment(*seg_id) {
                warn!(segment_id = ?seg_id, error = %e, "gc: failed to remove catalog entry");
            }
            // Clean up all in-memory indexes to prevent stale entries
            if let Some(ti) = time_idx.get_mut(shard_id) {
                if !ti.remove_segment(*seg_id) {
                    warn!(segment_id = ?seg_id, "gc: stale time-index entry already removed");
                }
            }
            blooms.remove(&seg_id.0);
            self.tag_index.remove_segment(*seg_id);
            self.metadata_cache.remove(*seg_id);
            self.segment_cache.invalidate_segment(*seg_id);
            removed += 1;
        }

        if removed > 0 {
            counter!("chronix_gc_segments_deleted_total").increment(removed as u64);
            info!(segments = removed, "GC: hard-deleted expired segments");

            // After GC removes segments, garbage-collect tombstones
            // whose target segments no longer exist in any active segment.
            let tombstones_removed = self.gc_tombstones();
            if tombstones_removed > 0 {
                info!(
                    tombstones = tombstones_removed,
                    "GC: cleaned up stale tombstones"
                );
            }
        }

        // Hard-delete measurements whose soft-delete TTL has elapsed.
        let measurement_drops = self.gc_pending_measurement_drops()?;
        if measurement_drops > 0 {
            info!(
                measurements = measurement_drops,
                "GC: hard-deleted pending measurement drops"
            );
        }

        Ok(removed)
    }

    /// Hard-delete measurements whose soft-delete TTL has elapsed.
    ///
    /// Scans the `pending_measurement_drops` map and removes any entries
    /// whose deadline is in the past. For each expired entry, the full
    /// hard-delete path (segment removal, schema cleanup, index cleanup)
    /// is executed.
    ///
    /// Returns the number of measurements hard-deleted.
    pub fn gc_pending_measurement_drops(&self) -> Result<usize> {
        let now_ms = super::chrono_timestamp_ms();

        // Collect expired measurements under a short read lock.
        let expired: Vec<String> = {
            let pending = self.pending_measurement_drops.read();
            pending
                .iter()
                .filter(|(_, &deadline)| now_ms >= deadline)
                .map(|(m, _)| m.clone())
                .collect()
        };

        if expired.is_empty() {
            return Ok(0);
        }

        for measurement in &expired {
            info!(measurement, "GC: hard-deleting soft-deleted measurement");
            // Remove from pending map first so concurrent queries stop
            // filtering it out even if the hard-delete partially fails.
            {
                let mut pending = self.pending_measurement_drops.write();
                pending.remove(measurement.as_str());
            }
            // Execute the hard-delete path (same as immediate drop).
            self.hard_delete_measurement(measurement)?;
        }

        Ok(expired.len())
    }

    /// Restore a measurement that was soft-deleted but whose TTL hasn't
    /// elapsed yet.
    ///
    /// Removes the measurement from the pending-deletion map so it
    /// becomes visible in queries again. Returns `true` if the
    /// measurement was pending deletion and was restored, `false` if it
    /// was not pending.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed.
    pub fn restore_measurement(&self, measurement: &str) -> Result<bool> {
        self.check_open()?;
        let mut pending = self.pending_measurement_drops.write();
        let was_pending = pending.remove(measurement).is_some();
        if was_pending {
            info!(measurement, "Measurement restored from soft-delete");
        }
        Ok(was_pending)
    }

    /// Check whether a measurement is pending soft-delete.
    #[must_use]
    pub fn is_measurement_pending_drop(&self, measurement: &str) -> bool {
        let pending = self.pending_measurement_drops.read();
        pending.contains_key(measurement)
    }

    /// Flush a single shard to per-measurement segment files.
    ///
    /// Registration — catalog, indexes, sidecar — runs *inside* the engine's
    /// flush window, so the memtable is released only once every segment it
    /// produced is in the catalog. A failure at any step keeps the memtable
    /// frozen for the next attempt and removes the files of this attempt.
    #[allow(clippy::too_many_lines)]
    pub fn flush_shard(&self, shard_id: ShardId) -> Result<Vec<FlushResult>> {
        let shard_flush_start = std::time::Instant::now();
        let outcome = self.shards.flush_shard_with(shard_id, |results| {
            self.register_flushed(shard_id, results)
                .map_err(|e| chronix_engine::memtable::MemtableError::Flush(e.to_string()))
        });
        match outcome {
            Ok(results) => {
                let flushed_measurements: HashSet<&str> =
                    results.iter().map(|r| r.measurement.as_str()).collect();
                for m in flushed_measurements {
                    self.detect_segment_overlaps(m);
                }
                let shard_label = shard_id.to_string();
                histogram!("chronix_flush_shard_duration_seconds", "shard" => shard_label.clone())
                    .record(shard_flush_start.elapsed().as_secs_f64());
                let rows: u64 = results.iter().map(|r| r.segment_meta.row_count).sum();
                let bytes: u64 = results.iter().map(|r| r.segment_meta.byte_size).sum();
                counter!("chronix_flush_shard_rows_total", "shard" => shard_label.clone())
                    .increment(rows);
                counter!("chronix_flush_shard_bytes_total", "shard" => shard_label)
                    .increment(bytes);
                Ok(results)
            }
            Err(chronix_engine::memtable::MemtableError::NoFrozenMemtable) => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    }

    /// Register freshly written segments: catalog entry, metadata cache,
    /// time index, series sidecar, bloom and tag index.
    fn register_flushed(&self, shard_id: ShardId, results: &[FlushResult]) -> Result<()> {
        for result in results {
            let meta = &result.segment_meta;
            debug!(
                shard = %shard_id,
                measurement = %result.measurement,
                segment = %meta.path.display(),
                rows = meta.row_count,
                "Shard flushed to segment"
            );
            if meta.uncompressed_bytes > 0 && meta.byte_size > 0 {
                histogram!("chronix_segment_compression_ratio")
                    .record(meta.uncompressed_bytes as f64 / meta.byte_size as f64);
            }

            // The sidecar first: it is derived data, and a segment without
            // one is rebuilt at open, but the catalog entry is the moment the
            // segment exists as far as anyone else is concerned.
            chronix_engine::index::series_index::write(
                &meta.path,
                &result.series_keys,
                chronix_engine::index::series_index::SegmentStamp::of(&meta.header),
            )?;

            let col_stats: Vec<CatalogColumnStats> = meta
                .column_metas
                .iter()
                .map(|cm| CatalogColumnStats {
                    name: cm.name.clone(),
                    data_type: cm.data_type,
                    role: cm.role,
                    stats: cm.stats.clone(),
                })
                .collect();

            let segment_id = {
                let mut catalog = self.catalog.write();
                let seg_id = catalog.next_segment_id();
                catalog.add_segment(SegmentCatalogEntry {
                    segment_id: seg_id,
                    shard_id,
                    measurement: result.measurement.clone(),
                    path: meta.path.clone(),
                    min_timestamp: meta.min_timestamp,
                    max_timestamp: meta.max_timestamp,
                    row_count: meta.row_count,
                    series_count: meta.series_count,
                    byte_size: meta.byte_size,
                    row_group_count: meta.row_group_count,
                    column_count: meta.column_count,
                    column_stats: col_stats,
                    state: SegmentState::Active,
                })?;
                seg_id
            };

            self.metadata_cache.insert(CachedSegmentMeta {
                segment_id,
                header: meta.header.clone(),
                columns: meta.column_metas.clone(),
            });
            {
                let mut indices = self.time_index.write();
                let idx = indices.entry(shard_id).or_default();
                idx.add_segment(TimeIndexEntry {
                    segment_id,
                    min_ts: meta.min_timestamp,
                    max_ts: meta.max_timestamp,
                });
            }
            {
                let mut blooms = self.blooms.write();
                Self::index_series_keys(
                    &mut blooms,
                    &self.tag_index,
                    segment_id,
                    &result.series_keys,
                );
            }
        }
        Ok(())
    }

    /// Detect overlapping segments at the same compaction level
    /// for a given measurement.
    ///
    /// After compaction or flush, segments for the same measurement
    /// should ideally have non-overlapping time ranges.  Overlapping
    /// segments are not an error (sort-merge dedup handles them), but
    /// they indicate potential compaction bugs or excessive out-of-order
    /// writes.
    ///
    /// Emits a warning log and increments a metric counter for each
    /// overlap detected.  Returns the number of overlapping segment
    /// pairs found.
    pub(super) fn detect_segment_overlaps(&self, measurement: &str) -> usize {
        let mut segments: Vec<(chronix_core::SegmentId, i64, i64)> = {
            let catalog = self.catalog.read();
            catalog
                .active_segments_for_measurement(measurement)
                .iter()
                .map(|e| (e.segment_id, e.min_timestamp, e.max_timestamp))
                .collect()
        };

        if segments.len() < 2 {
            return 0;
        }

        // Sort by min_timestamp for efficient overlap detection.
        segments.sort_by_key(|s| s.1);

        let mut overlap_count = 0usize;
        for i in 0..segments.len() - 1 {
            let (id_a, _min_a, max_a) = segments[i];
            let (id_b, min_b, _max_b) = segments[i + 1];
            // Overlap: segment A's max >= segment B's min
            if max_a >= min_b {
                overlap_count += 1;
                warn!(
                    measurement,
                    segment_a = ?id_a,
                    segment_b = ?id_b,
                    overlap_start = min_b,
                    overlap_end = max_a,
                    "overlapping segments detected at same level"
                );
            }
        }

        if overlap_count > 0 {
            counter!("chronix_segment_overlaps_detected_total").increment(overlap_count as u64);
            gauge!("chronix_segment_overlaps_current").set(overlap_count as f64);
        }

        overlap_count
    }
}
