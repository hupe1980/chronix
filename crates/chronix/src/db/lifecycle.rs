//! Lifecycle methods for [`Chronix`] — flush, close, compaction, GC, retention.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};

use metrics::{counter, gauge, histogram};
use tracing::{debug, info, warn};

use chronix_core::{SegmentState, ShardId};
use chronix_engine::index::{CatalogColumnStats, SegmentCatalogEntry};
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

        // Every cutoff is measured from here, and it is deliberately *not*
        // the wall clock: it is the clock capped by the newest timestamp the
        // database holds, so one bad reading of the clock cannot delete
        // everything and a gateway whose sensors went quiet keeps its
        // history. See `retention::retention_reference`.
        //
        // Computed from the same snapshot the bounds came from, plus the
        // shard live writes are landing in — a database whose newest data is
        // still unflushed must not read as one that stopped writing. The
        // shard's *start* is used rather than its end, which is the
        // conservative direction: it can only hold data back.
        let now_ns = Self::now_ns();
        let newest_data_ns = self.newest_data_ns(shard_bounds.values().map(|b| b.1).max());
        let reference_ns = retention::retention_reference(now_ns, newest_data_ns);

        // Without a global rule nothing expires by age alone; the
        // per-measurement pass below still runs.
        let global_cutoff = global_retention_ns
            .map_or(i64::MIN, |ns| retention::retention_cutoff(reference_ns, ns));

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
                    retention::retention_cutoff(reference_ns, retention_ns_m),
                )
            })
            .collect();
        for rollup in self.rollup_registry.read().list() {
            if let Some(r) = rollup.retention_ns {
                per_measurement_cutoffs
                    .entry(rollup.target_measurement.clone())
                    .or_insert_with(|| retention::retention_cutoff(reference_ns, r));
            }
        }

        // Use the global cutoff for whole-shard drops.
        let expired = retention::shards_to_drop(&shard_bounds, global_cutoff);

        let mut total_segments: usize = 0;
        let mut total_bytes: u64 = 0;
        // Shards this pass actually removed, and segments it declined to
        // remove because a rollup still needs them. `shards_dropped` used to
        // be `expired.len()` — the shards the pass *looked at* — so an
        // operator watching a disk that would not shrink read
        // "Retention enforced, shards=13" on every pass, for ever, while the
        // pass deleted nothing. The tell for this class is a number that is
        // always the same.
        let mut shards_dropped: usize = 0;
        let mut segments_preserved: usize = 0;
        let mut segments_awaiting_readers: usize = 0;

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
                    segments_preserved += 1;
                    warn!(
                        segment_id = ?entry.segment_id,
                        measurement = %entry.measurement,
                        "retention: rollups not yet materialised past this shard — preserving segment"
                    );
                    counter!("chronix_retention_segments_awaiting_rollup_total").increment(1);
                    protected.insert(entry.segment_id);
                }
            }

            let doomed: Vec<SegmentCatalogEntry> = entries
                .iter()
                .filter(|e| !protected.contains(&e.segment_id))
                .cloned()
                .collect();
            let retired = self.retire_segments(&doomed);
            total_segments += retired.total();
            total_bytes += retired.bytes_freed;
            segments_awaiting_readers += retired.deferred;

            // A shard counts as dropped only when nothing in it survived —
            // and only when the retirement actually took, so a manifest that
            // could not be written does not report a shard as dropped.
            if protected.is_empty() && retired.total() == doomed.len() {
                shards_dropped += 1;
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

            let retired = self.retire_segments(&seg_to_drop);
            total_segments += retired.total();
            total_bytes += retired.bytes_freed;
            segments_awaiting_readers += retired.deferred;
        }

        // A pass that removed segments changed what the database holds, so
        // every value derived from that has to be repaired — the cardinality
        // budget above all, because it is an admission limit.
        if total_segments > 0 {
            self.repair_live_series();
        }

        let result = retention::RetentionResult {
            shards_dropped,
            segments_deleted: total_segments,
            segments_preserved,
            segments_awaiting_readers,
            bytes_freed: total_bytes,
        };

        if result.shards_dropped > 0 || result.segments_deleted > 0 {
            counter!("chronix_retention_shards_dropped_total")
                .increment(result.shards_dropped as u64);
            info!(
                shards = result.shards_dropped,
                segments = result.segments_deleted,
                preserved = result.segments_preserved,
                awaiting_readers = result.segments_awaiting_readers,
                bytes = result.bytes_freed,
                "Retention enforced"
            );
        } else if result.segments_preserved > 0 {
            // Nothing was deleted *and* something was held back: the one
            // case an operator investigating disk usage needs to see.
            info!(
                preserved = result.segments_preserved,
                "Retention preserved every expired segment — rollups have not caught up"
            );
        }

        Ok(result)
    }

    /// Re-derive the cardinality budget from what the database still holds.
    ///
    /// `known_series` is the counter `max_series_cardinality` is checked
    /// against on every write. It was maintained by writes and by whole-series
    /// deletes, and **not by retention** — so a deployment with any tag churn
    /// climbed towards the limit for ever and eventually refused every write,
    /// with the data it was counting long since deleted. A restart healed it,
    /// which is the tell: the admission decision depended on process uptime.
    ///
    /// A repair rather than a second counter, because this is a derived value
    /// whose input keeps moving. It runs only on a pass that actually deleted
    /// something, so the cost — one sidecar read per surviving segment, no
    /// segment decoded — is paid once per shard rotation rather than per tick.
    ///
    /// The live set is the segments on disk **plus the memtables**: a series
    /// written a second ago is in no segment yet.
    pub(crate) fn repair_live_series(&self) {
        let started = std::time::Instant::now();
        // Memtables first, then the catalog — the order is load-bearing.
        // Data only ever moves one way, memtable → segment, so a flush
        // landing between the two reads is seen by the *second* one. Reading
        // the catalog first would leave a window in which a series had been
        // flushed out of the memtable and its segment had not yet been read,
        // and the repair would release a series whose data is on disk.
        let in_memory = self.shards.live_series();
        // The sidecars are read *outside* the catalog lock, under a lease.
        // Holding the read lock across that I/O blocked every `catalog.write()`
        // — which is where a flush registers its segment — for the whole pass,
        // and the pass is measured in seconds on a large instance. The lease
        // gives the same guarantee the lock did, that these files are still
        // there, without stopping the write path (see `db::leases`).
        let (entries, _lease) = {
            let catalog = self.catalog.read();
            let entries: Vec<SegmentCatalogEntry> = catalog
                .all_segments()
                .into_iter()
                .filter(|e| e.state == SegmentState::Active)
                .cloned()
                .collect();
            let lease = self
                .segment_leases
                .acquire(entries.iter().map(|e| e.segment_id));
            (entries, lease)
        };
        let on_disk = Self::series_on_disk(&entries);
        let before = self.known_series.len();
        self.known_series
            .retain(|k| on_disk.contains(k.as_str()) || in_memory.contains(k.as_str()));
        let released = before.saturating_sub(self.known_series.len());
        if released == 0 {
            return;
        }

        // The last-value cache is a copy of each series' newest row, so it
        // has to forget the series the data no longer holds — otherwise a
        // device offline for longer than the retention window went on
        // answering `last_value()` with a reading nothing else can return.
        // Asked as a predicate rather than handed a set, so a million-series
        // budget is not copied to prune a cache.
        self.lvc
            .retain_series(|canonical| self.known_series.contains(canonical));

        // The namespace index is deliberately *not* pruned here. It answers
        // "may this namespace see this measurement", and a tenant whose
        // sensor went quiet for longer than the retention window must still
        // be able to query the table — a scoped `table_exist` that says no
        // turns an empty graph into a planning error. The index follows the
        // measurement, so it is pruned where a measurement is *dropped*.

        metrics::counter!("chronix_series_released_total").increment(released as u64);
        // The duration is reported because the pass is linear in the total
        // number of series entries across every surviving segment's sidecar,
        // and it runs on the maintenance thread — the same thread a full
        // memtable is waiting on. Measured at 25 ms for 500 segments × 50
        // series and 2.0 s for 8 000 × 500, so an operator seeing the latter
        // is seeing a real stall rather than guessing about one.
        info!(
            released,
            remaining = self.known_series.len(),
            duration_ms = started.elapsed().as_millis(),
            "Cardinality budget released for series whose data is gone"
        );
    }

    /// The canonical form of every series the active segments hold, read from
    /// the per-segment series sidecars — no segment is decoded.
    fn series_on_disk(entries: &[SegmentCatalogEntry]) -> std::collections::HashSet<String> {
        let mut known = std::collections::HashSet::new();
        for entry in entries {
            for key in Self::series_keys_of(&entry.path, &entry.measurement) {
                known.insert(key.canonical_form().to_string());
            }
        }
        known
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

        // Leased for the merge: a task opens its inputs on a worker thread,
        // minutes after this snapshot on a large shard, and retention or a
        // drop running beside it would otherwise unlink one and fail the
        // task. Released before the results are processed, because this pass
        // *retires* those same inputs and a pass holding its own lease would
        // defer its own reclamation. See `db::leases`.
        let (segments, input_lease) = {
            let catalog = self.catalog.read();
            let segs: Vec<SegmentCatalogEntry> = catalog
                .all_segments()
                .into_iter()
                .filter(|e| e.state == SegmentState::Active)
                .cloned()
                .collect();
            let lease = self
                .segment_leases
                .acquire(segs.iter().map(|e| e.segment_id));
            (segs, lease)
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
        // Every input has been read; from here the pass only opens outputs.
        drop(input_lease);

        // Process results sequentially — catalog/index updates require
        // write locks and must not race.
        for (task, result) in tasks.iter().zip(execution_results) {
            match result {
                Ok(meta) if meta.row_count == 0 => {
                    // All rows tombstoned — no output segment to register,
                    // so the inputs are simply retired.
                    self.retire_segments(&task.input_segments);

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

                        let col_stats: Vec<CatalogColumnStats> =
                            match SegmentReader::open(&meta.path) {
                                Ok(reader) => reader
                                    .column_metadata()
                                    .iter()
                                    .map(|cm| CatalogColumnStats {
                                        name: cm.name.clone(),
                                        data_type: cm.data_type,
                                        role: cm.role,
                                        decimal_scale: cm.decimal_scale,
                                        stats: cm.stats.clone(),
                                    })
                                    .collect(),
                                Err(e) => {
                                    warn!(
                                        segment = %meta.path.display(),
                                        error = %e,
                                        "Could not read column stats for compacted segment"
                                    );
                                    Vec::new()
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

                    // Clean up the inputs' derived index entries. Their
                    // files are kept until the reclamation below.
                    {
                        let mut blooms = self.blooms.write();
                        for input in &task.input_segments {
                            blooms.remove(&input.segment_id.0);
                            self.segment_cache.invalidate_segment(input.segment_id);
                        }
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

                    // The inputs are already out of every query — the catalog
                    // swap above retired them in one transaction with the
                    // output, which is what keeps a delete issued mid-merge
                    // masking the rows the merge carried over. Their bytes go
                    // now unless a scan snapshotted them first, in which case
                    // the next `gc()` finishes the job.
                    self.reclaim_retired(&task.input_segments);

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

    /// Retire a set of segments.
    ///
    /// **Their rows leave the database now; their files leave when the last
    /// reader that could open them is gone.** Every pass that removes a
    /// segment goes through here — retention, a measurement drop, cold-tier
    /// archiving, the inputs of a compaction — so the four of them cannot
    /// disagree about the order of the catalog write, the index cleanup and
    /// the unlink, and none of them can pull a file out from under a scan
    /// that has already snapshotted its path (see `db::leases`).
    ///
    /// A segment a reader still holds is marked
    /// [`SegmentState::SoftDeleted`] — invisible to every query from this
    /// moment — and its file is unlinked by the next [`gc`](Self::gc).
    ///
    /// The caller must hold no catalog or bloom lock.
    pub(crate) fn retire_segments(&self, entries: &[SegmentCatalogEntry]) -> Retired {
        let mut out = Retired::default();
        if entries.is_empty() {
            return out;
        }
        let now_ms = super::chrono_timestamp_ms();
        let mut to_unlink: Vec<std::path::PathBuf> = Vec::with_capacity(entries.len());

        {
            let mut catalog = self.catalog.write();
            let mut blooms = self.blooms.write();

            // The derived indexes go unconditionally and first: a retired
            // segment is invisible to queries whether or not its bytes can go
            // yet.
            for entry in entries {
                blooms.remove(&entry.segment_id.0);
                self.tag_index.remove_segment(entry.segment_id);
                self.segment_cache.invalidate_segment(entry.segment_id);
            }

            // One fsync for the whole retirement rather than one per segment:
            // on flash-backed storage the fsync rate is the wear rate, and a
            // retention pass expiring a day of shards is dozens of them.
            let synced = catalog.in_one_sync(|cat| {
                for entry in entries {
                    // Asked while the catalog write lock is held, so no reader
                    // can take a lease between the answer and the unlink it
                    // decides.
                    if self.segment_leases.is_leased(entry.segment_id) {
                        match cat.soft_delete_segment(entry.segment_id, now_ms) {
                            // Not in the catalog any more: another pass took
                            // it between this one's snapshot and its write
                            // lock, so it is not this pass's to report.
                            Ok(false) => {}
                            Ok(true) => out.deferred += 1,
                            Err(e) => {
                                warn!(segment_id = ?entry.segment_id, error = %e, "retire: failed to mark a segment a reader still holds");
                            }
                        }
                        continue;
                    }
                    match cat.remove_segment(entry.segment_id) {
                        // Already gone: another pass retired it between this
                        // one's snapshot and its write lock. Not this pass's
                        // segment to count, and not its file to unlink.
                        Ok(None) => continue,
                        Ok(Some(_)) => {}
                        Err(e) => {
                            warn!(segment_id = ?entry.segment_id, error = %e, "retire: failed to remove catalog entry");
                            continue;
                        }
                    }
                    to_unlink.push(entry.path.clone());
                    out.removed += 1;
                    out.bytes_freed += entry.byte_size;
                }
                Ok(())
            });
            if let Err(e) = synced {
                warn!(error = %e, "retire: manifest sync failed — the files stay until the next pass");
                return Retired::default();
            }
        }

        // Only now: the catalog no longer names these paths, durably, so a
        // crash here leaves a file nothing points at, which `open()` removes.
        for path in &to_unlink {
            Self::unlink_segment_files(path);
        }

        if out.deferred > 0 {
            debug!(
                segments = out.deferred,
                "retire: segments held by a running scan — their files go on the next GC"
            );
            counter!("chronix_segments_retired_awaiting_readers_total")
                .increment(out.deferred as u64);
        }
        out
    }

    /// Remove a segment's file and its series sidecar, tolerating a file that
    /// is already gone.
    fn unlink_segment_files(path: &std::path::Path) {
        if let Err(e) = std::fs::remove_file(path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!(path = %path.display(), error = %e, "failed to remove segment file");
            }
        }
        if let Err(e) = chronix_engine::index::series_index::remove(path) {
            warn!(path = %path.display(), error = %e, "failed to remove series index");
        }
    }

    /// Unlink the files of segments the catalog has already retired, skipping
    /// any a reader still holds.
    ///
    /// Returns the number of files removed.
    fn reclaim_retired(&self, entries: &[SegmentCatalogEntry]) -> usize {
        if entries.is_empty() {
            return 0;
        }
        let mut to_unlink: Vec<std::path::PathBuf> = Vec::with_capacity(entries.len());
        {
            let mut catalog = self.catalog.write();
            let synced = catalog.in_one_sync(|cat| {
                for entry in entries {
                    if self.segment_leases.is_leased(entry.segment_id) {
                        continue;
                    }
                    match cat.remove_segment(entry.segment_id) {
                        Ok(None) => continue,
                        Ok(Some(_)) => {}
                        Err(e) => {
                            warn!(segment_id = ?entry.segment_id, error = %e, "gc: failed to remove catalog entry");
                            continue;
                        }
                    }
                    to_unlink.push(entry.path.clone());
                }
                Ok(())
            });
            if let Err(e) = synced {
                warn!(error = %e, "gc: manifest sync failed — the files stay until the next pass");
                return 0;
            }
        }
        for path in &to_unlink {
            Self::unlink_segment_files(path);
        }
        to_unlink.len()
    }

    /// Unlink the files of segments that were retired while a reader still
    /// held them.
    ///
    /// Returns the number of segment files removed.
    ///
    /// A retirement pass — retention, a drop, archiving, a compaction's
    /// inputs — takes a segment out of the catalog and unlinks it in one
    /// step, unless a running scan has leased the path; then the entry is
    /// marked [`SegmentState::SoftDeleted`], the rows are gone from every
    /// query, and the bytes wait here for the reader to finish. This is a
    /// step of every maintenance pass, so the wait is bounded by the
    /// maintenance interval rather than by anything the reader does.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed.
    pub fn gc(&self) -> Result<usize> {
        self.check_open()?;

        let retired: Vec<SegmentCatalogEntry> = {
            let catalog = self.catalog.read();
            catalog.retired_segments().into_iter().cloned().collect()
        };

        let removed = self.reclaim_retired(&retired);
        if removed > 0 {
            counter!("chronix_gc_segments_deleted_total").increment(removed as u64);
            info!(
                segments = removed,
                "GC: removed the files of segments a reader had been holding"
            );
            // Removing the last segment a series had is the moment it stops
            // existing, and the cardinality budget is an admission limit.
            self.repair_live_series();
        }

        // Unconditionally, because this is the only caller: a tombstone is
        // reclaimable once every segment it was issued against has left the
        // catalog, and segments now leave through `retire_segments` without
        // passing through here at all. Gating it on `removed > 0` — which it
        // was, when every retirement was a soft delete this pass collected —
        // would leave the set growing for ever on a database whose reads
        // never collide with its retirements, which is most of them.
        let tombstones_removed = self.gc_tombstones();
        if tombstones_removed > 0 {
            info!(
                tombstones = tombstones_removed,
                "GC: cleaned up stale tombstones"
            );
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

    /// The newest timestamp this database holds, on disk or in memory.
    ///
    /// `on_disk` is the maximum over active segments, which the caller
    /// supplies from whatever catalog snapshot it already holds — retention
    /// derives its shard bounds from the same one, and taking a second read
    /// would let a flush land between them.
    ///
    /// The in-memory term matters: a database whose newest data is still
    /// unflushed must not read as one that stopped writing. The active
    /// shard's *start* is used rather than its end, which can only hold the
    /// reference back — the conservative direction.
    fn newest_data_ns(&self, on_disk: Option<i64>) -> Option<i64> {
        let shard_ns = i64::try_from(self.config.shard_duration.as_nanos()).unwrap_or(i64::MAX);
        let in_memory = self
            .shards
            .active_shard()
            .map(|s| s.0.saturating_mul(shard_ns));
        match (on_disk, in_memory) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        }
    }

    /// Hard-delete measurements whose soft-delete TTL has elapsed.
    ///
    /// Scans the catalog's pending-drop set and hard-deletes any entry whose
    /// deadline is in the past. Each hard delete cancels its own pending-drop
    /// entry once it has actually removed the data (`hard_delete_measurement`),
    /// so a delete that fails part-way leaves the entry pending for the next
    /// pass to retry rather than forgetting it.
    ///
    /// Returns the number of measurements hard-deleted.
    ///
    /// The deadline is measured against the same reference retention uses,
    /// not the raw wall clock: see the comment in the body.
    pub fn gc_pending_measurement_drops(&self) -> Result<usize> {
        // The same reference retention measures age from, for the same
        // reason: this pass is irreversible, and a soft delete's whole
        // purpose is to leave a window in which a mistake can be undone.
        // Reading `SystemTime::now()` alone means one bad reading — a
        // gateway with no battery-backed RTC, an NTP server handing out a
        // date in the next century — closes that window instantly and
        // destroys the data it existed to protect.
        //
        // The cost is retention's, stated the same way: a database that has
        // stopped receiving data stops reclaiming this disk. That is the
        // conservative direction for an undo window, and a live server's
        // newest write is a moment old, so the clock decides in practice.
        let reference_ns = retention::retention_reference(
            Self::now_ns(),
            self.newest_data_ns(
                self.catalog
                    .read()
                    .all_segments()
                    .into_iter()
                    .filter(|e| e.state == SegmentState::Active)
                    .map(|e| e.max_timestamp)
                    .max(),
            ),
        );
        let reference_ms = u64::try_from(reference_ns / 1_000_000).unwrap_or(0);

        let expired: Vec<String> = self
            .catalog
            .read()
            .pending_measurement_drops()
            .iter()
            .filter(|(_, &deadline)| reference_ms >= deadline)
            .map(|(m, _)| m.clone())
            .collect();

        if expired.is_empty() {
            return Ok(0);
        }

        for measurement in &expired {
            info!(measurement, "GC: hard-deleting soft-deleted measurement");
            self.hard_delete_measurement(measurement)?;
        }

        Ok(expired.len())
    }

    /// Restore a measurement that was soft-deleted but whose TTL hasn't
    /// elapsed yet.
    ///
    /// Cancels the pending drop, durably, so it becomes visible in queries
    /// again. Returns `true` if the measurement was pending deletion and was
    /// restored, `false` if it was not pending.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed, or the manifest cannot be
    /// written.
    pub fn restore_measurement(&self, measurement: &str) -> Result<bool> {
        self.check_open()?;
        let was_pending = self
            .catalog
            .write()
            .cancel_measurement_pending_drop(measurement)?;
        if was_pending {
            info!(measurement, "Measurement restored from soft-delete");
        }
        Ok(was_pending)
    }

    /// Check whether a measurement is pending soft-delete.
    #[must_use]
    pub fn is_measurement_pending_drop(&self, measurement: &str) -> bool {
        self.catalog.read().is_measurement_pending_drop(measurement)
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
    /// series sidecar, bloom and tag index.
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
                    decimal_scale: cm.decimal_scale,
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

/// What one call to [`Chronix::retire_segments`] did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Retired {
    /// Segments whose catalog entry and file are both gone.
    pub(crate) removed: usize,
    /// Segments taken out of every query but whose file a running scan still
    /// holds. Unlinked by the next [`Chronix::gc`].
    pub(crate) deferred: usize,
    /// Bytes actually reclaimed — the deferred segments' bytes are not
    /// counted, because they are still on the disk.
    pub(crate) bytes_freed: u64,
}

impl Retired {
    /// Segments this pass removed from the database, whether or not their
    /// bytes have gone yet.
    pub(crate) const fn total(self) -> usize {
        self.removed + self.deferred
    }
}
