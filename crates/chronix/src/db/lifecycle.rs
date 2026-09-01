//! Lifecycle methods for [`Chronix`] — flush, close, compaction, GC, retention.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};

use metrics::{counter, gauge, histogram};
use tracing::{debug, info, warn};

use chronix_core::{SegmentState, ShardId};
use chronix_engine::cache::metadata::CachedSegmentMeta;
use chronix_engine::index::{
    CatalogColumnStats, SegmentCatalogEntry, SeriesBloomFilter, TimeIndexEntry,
};
use chronix_engine::memtable::FlushResult;
use chronix_engine::segment::reader::SegmentReader;
use chronix_query::plan::QueryPlan;

use chronix_engine::compaction::CompactionExecutor;

use crate::error::{DbError, Result};
use crate::export::ParquetExportConfig;
use crate::retention;

impl super::Chronix {
    /// Force a flush of the active memtable to a segment file.
    ///
    /// This is normally triggered automatically when the memtable reaches
    /// the configured threshold, but can be called manually for testing
    /// or to ensure data is persisted before shutdown.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Closed`] if the database is already closed, or
    /// propagates any I/O or storage error from the flush pipeline.
    #[must_use = "flush errors may indicate data not persisted"]
    pub fn flush(&self) -> Result<Vec<FlushResult>> {
        let _start = std::time::Instant::now();
        self.check_open()?;

        let shard_ids = self.shards.active_shard_ids();
        let mut all_results = Vec::new();
        let mut global_max_wal_seq: Option<u64> = None;

        for shard_id in shard_ids {
            match self.flush_shard(shard_id) {
                Ok(results) => {
                    for r in &results {
                        if let Some(seq) = r.max_wal_seq {
                            global_max_wal_seq =
                                Some(global_max_wal_seq.map_or(seq, |c| c.max(seq)));
                        }
                    }
                    all_results.extend(results);
                }
                Err(e) => {
                    warn!(shard = %shard_id, error = %e, "Flush failed for shard");
                    return Err(e);
                }
            }
        }

        // Truncate WAL only after ALL shards have been safely flushed,
        // preventing early truncation from discarding entries needed by
        // other shards on crash recovery.
        //
        // Additionally, cap the truncation point at the minimum WAL
        // sequence still held in any active memtable — concurrent
        // writers may have inserted records during the sequential
        // flush, and those records must be preserved.
        if let Some(mut max_seq) = global_max_wal_seq {
            if let Some(active_min) = self.shards.min_active_wal_seq() {
                max_seq = max_seq.min(active_min.saturating_sub(1));
            }
            if let Err(e) = self.wal.truncate_before(max_seq) {
                warn!(error = %e, "Failed to truncate WAL after flush");
            }
        }

        histogram!("chronix_flush_duration_seconds").record(_start.elapsed().as_secs_f64());
        Ok(all_results)
    }

    /// Close the database, flushing all pending data.
    ///
    /// After closing, all subsequent operations will return
    /// [`DbError::Closed`].
    ///
    /// # Errors
    ///
    /// Returns any error encountered while flushing shards or syncing
    /// the WAL during shutdown.
    #[must_use = "close errors may indicate data not persisted"]
    pub fn close(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(()); // Already closed
        }

        info!("Closing Chronix database");

        // Flush ingest-time downsample accumulators before flushing shards
        if self.ingest_downsampler.has_rules() {
            if let Err(e) = self.flush_ingest_downsampling() {
                tracing::warn!(error = %e, "failed to flush ingest downsampling on close");
            }
        }

        // Flush all shards and collect WAL sequence numbers
        let mut global_max_wal_seq: Option<u64> = None;
        let mut any_failed = false;
        for shard_id in self.shards.all_shard_ids() {
            match self.flush_shard(shard_id) {
                Ok(results) => {
                    for r in &results {
                        if let Some(seq) = r.max_wal_seq {
                            global_max_wal_seq =
                                Some(global_max_wal_seq.map_or(seq, |c| c.max(seq)));
                        }
                    }
                }
                Err(e) => {
                    warn!(shard = %shard_id, error = %e, "Error flushing shard during close");
                    any_failed = true;
                }
            }
        }

        // Truncate WAL only if ALL shards flushed successfully —
        // a failed shard still needs its WAL entries for replay
        // on next startup.
        if !any_failed {
            if let Some(max_seq) = global_max_wal_seq {
                if let Err(e) = self.wal.truncate_before(max_seq) {
                    warn!(error = %e, "Failed to truncate WAL during close");
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

        info!("Chronix database closed");
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
            crate::export::write_parquet(self.execute_iter(plan)?, output_path, parquet_config)?
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

    /// Enforce a retention policy, dropping all shards whose data
    /// falls entirely before `cutoff_ns`.
    ///
    /// Returns the number of segments deleted.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed.
    #[must_use = "retention errors must be handled"]
    pub fn enforce_retention(&self, retention_ns: i64) -> Result<retention::RetentionResult> {
        self.check_open()?;

        let now_ns = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        )
        .unwrap_or(i64::MAX);
        let global_cutoff = retention::retention_cutoff(now_ns, retention_ns);

        // Build per-measurement cutoffs from config overrides.
        let per_measurement_cutoffs: std::collections::HashMap<String, i64> = self
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

        // Collect shard bounds from time index
        let shard_bounds: BTreeMap<ShardId, (i64, i64)> = {
            let time_idx = self.time_index.read();
            time_idx
                .iter()
                .map(|(&shard_id, idx)| {
                    let all = idx.all_entries();
                    let (min, max) = all.iter().fold((i64::MAX, i64::MIN), |(lo, hi), e| {
                        (lo.min(e.min_ts), hi.max(e.max_ts))
                    });
                    (shard_id, (min, max))
                })
                .collect()
        };

        // Use the global cutoff for whole-shard drops.
        let expired = retention::shards_to_drop(&shard_bounds, global_cutoff);

        let mut total_segments: usize = 0;
        let mut total_bytes: u64 = 0;

        for &shard_id in &expired {
            // Find all segments in this shard
            let entries: Vec<SegmentCatalogEntry> = {
                let catalog = self.catalog.read();
                catalog
                    .all_segments()
                    .into_iter()
                    .filter(|e| e.shard_id == shard_id)
                    .cloned()
                    .collect()
            };

            // Rollup-aware retention: compute rollups before dropping
            // segments so that aggregated data is preserved.
            // Track segments whose rollups failed — we must NOT delete
            // these, as the aggregated data would be lost.
            let mut rollup_failed_segments: std::collections::HashSet<chronix_core::SegmentId> =
                std::collections::HashSet::new();
            for entry in &entries {
                let rollups: Vec<crate::rollup::RollupConfig> = {
                    let reg = self.rollup_registry.read();
                    reg.rollups_for_source(&entry.measurement)
                        .into_iter()
                        .cloned()
                        .collect()
                };
                if !rollups.is_empty() {
                    // A segment that cannot be read cannot be rolled
                    // up, so it must be preserved. Falling through to the
                    // delete loop here destroyed the raw data *and* the
                    // aggregate that was supposed to replace it — silent,
                    // permanent loss.
                    match SegmentReader::open(&entry.path).and_then(|r| r.read_all()) {
                        Ok(batch) => {
                            let batches = [batch];
                            for rollup_config in &rollups {
                                let rollup_points =
                                    crate::rollup::compute_rollup_points(&batches, rollup_config);
                                if !rollup_points.is_empty() {
                                    if let Err(e) = self.insert_batch(&rollup_points) {
                                        warn!(
                                            rollup = %rollup_config.name,
                                            error = %e,
                                            "Failed to compute rollup before retention drop — preserving segment"
                                        );
                                        rollup_failed_segments.insert(entry.segment_id);
                                    } else {
                                        info!(
                                            rollup = %rollup_config.name,
                                            points = rollup_points.len(),
                                            shard = %shard_id,
                                            "Rollup computed before retention drop"
                                        );
                                        // Drive the *whole* chain, not
                                        // just the first tier. For a
                                        // 1 s→1 min→15 min cascade the raw data
                                        // is being traded for the 15 min tier;
                                        // dropping it while only the 1 min tier
                                        // exists loses the long-retention
                                        // aggregate if the intermediate tier is
                                        // itself expired before it compacts.
                                        if !self.cascade_rollup(
                                            &rollup_config.target_measurement,
                                            &rollup_points,
                                            1,
                                        ) {
                                            warn!(
                                                rollup = %rollup_config.name,
                                                "Rollup chain incomplete — preserving source segment"
                                            );
                                            rollup_failed_segments.insert(entry.segment_id);
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                segment_id = ?entry.segment_id,
                                path = %entry.path.display(),
                                error = %e,
                                "Cannot read segment to compute its rollup — preserving segment"
                            );
                            counter!("chronix_retention_unreadable_segments_total").increment(1);
                            rollup_failed_segments.insert(entry.segment_id);
                        }
                    }
                }
            }

            let mut catalog = self.catalog.write();
            let mut time_idx = self.time_index.write();
            let mut blooms = self.blooms.write();

            for entry in &entries {
                // Skip segments whose rollup insertion failed — dropping
                // them would lose the pre-aggregated data permanently.
                if rollup_failed_segments.contains(&entry.segment_id) {
                    warn!(segment_id = ?entry.segment_id, "retention: skipping segment with failed rollup");
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
                if let Err(e) = std::fs::remove_file(entry.path.with_extension("bloom")) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        warn!(path = %entry.path.display(), error = %e, "retention: failed to remove bloom sidecar");
                    }
                }
            }

            time_idx.remove(&shard_id);
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
                    if let Err(e) = std::fs::remove_file(entry.path.with_extension("bloom")) {
                        if e.kind() != std::io::ErrorKind::NotFound {
                            warn!(path = %entry.path.display(), error = %e, "retention: failed to remove bloom sidecar");
                        }
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
                        catalog.add_segment(entry)?;

                        // Soft-delete input segments (grace period before hard-delete)
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        for input in &task.input_segments {
                            if let Err(e) = catalog.soft_delete_segment(input.segment_id, now_ms) {
                                warn!(segment_id = ?input.segment_id, error = %e, "compaction: failed to soft-delete segment");
                            }
                        }

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

                    // Pre-compute tag pairs from the compacted segment BEFORE
                    // removing old segments from the tag index, to avoid a window
                    // where concurrent tag-filtered queries miss data.
                    let measurement = &task.input_segments[0].measurement;
                    let new_series_keys = SegmentReader::open(&meta.path)
                        .ok()
                        .and_then(|reader| reader.read_all().ok())
                        .map(|batch| Self::extract_series_keys_from_batch(&batch, measurement))
                        .unwrap_or_default();

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
                    if !new_series_keys.is_empty() {
                        let tag_pairs: Vec<(&str, &str)> = new_series_keys
                            .iter()
                            .flat_map(|sk| sk.tags().iter().map(|(k, v)| (k.as_ref(), v.as_ref())))
                            .collect();

                        let old_ids: Vec<chronix_core::SegmentId> =
                            task.input_segments.iter().map(|s| s.segment_id).collect();

                        self.tag_index
                            .replace_segments(&old_ids, segment_id, &tag_pairs);

                        // Build and persist bloom filter
                        let mut bloom = SeriesBloomFilter::new(new_series_keys.len(), 0.01);
                        for key in &new_series_keys {
                            bloom.insert(key);
                        }

                        let bloom_path = meta.path.with_extension("bloom");
                        match bloom.to_bytes() {
                            Ok(bytes) => {
                                if let Err(e) = std::fs::write(&bloom_path, bytes) {
                                    warn!(
                                        bloom = %bloom_path.display(),
                                        error = %e,
                                        "Failed to write bloom sidecar for compacted segment"
                                    );
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, "Failed to serialize bloom for compacted segment");
                            }
                        }

                        self.blooms.write().insert(segment_id.0, bloom);
                    }

                    // Compute rollups if configured for this measurement
                    let rollups: Vec<crate::rollup::RollupConfig> = {
                        let reg = self.rollup_registry.read();
                        reg.rollups_for_source(measurement)
                            .into_iter()
                            .cloned()
                            .collect()
                    };

                    if !rollups.is_empty() {
                        // Read the compacted segment to get batches for rollup
                        if let Ok(reader) = SegmentReader::open(&meta.path) {
                            if let Ok(batch) = reader.read_all() {
                                let rows_in = batch.num_rows();
                                let batches = [batch];
                                for rollup_config in &rollups {
                                    let rollup_points = crate::rollup::compute_rollup_points(
                                        &batches,
                                        rollup_config,
                                    );
                                    if !rollup_points.is_empty() {
                                        counter!("chronix_rollup_computations_total").increment(1);
                                        counter!("chronix_rollup_rows_processed_total")
                                            .increment(rows_in as u64);
                                        // Insert rollup points into target measurement
                                        if let Err(e) = self.insert_batch(&rollup_points) {
                                            warn!(
                                                rollup = %rollup_config.name,
                                                error = %e,
                                                "Failed to insert rollup points"
                                            );
                                        } else {
                                            info!(
                                                rollup = %rollup_config.name,
                                                points = rollup_points.len(),
                                                "Rollup points computed during compaction"
                                            );

                                            // Multi-tier chain: cascade to next
                                            // tier if the target measurement has
                                            // its own rollup(s) configured.
                                            if !self.cascade_rollup(
                                                &rollup_config.target_measurement,
                                                &rollup_points,
                                                1,
                                            ) {
                                                warn!(
                                                    rollup = %rollup_config.name,
                                                    "Rollup chain incomplete after compaction"
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
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
            if let Err(e) = std::fs::remove_file(path.with_extension("bloom")) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!(path = %path.display(), error = %e, "gc: failed to remove bloom sidecar");
                }
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
    #[allow(clippy::too_many_lines)]
    pub fn flush_shard(&self, shard_id: ShardId) -> Result<Vec<FlushResult>> {
        let _shard_flush_start = std::time::Instant::now();
        match self.shards.flush_shard(shard_id) {
            Ok(results) => {
                // Accumulate per-shard totals for flush metrics.
                let mut shard_total_rows: u64 = 0;
                let mut shard_total_bytes: u64 = 0;

                for result in &results {
                    let meta = &result.segment_meta;
                    shard_total_rows += meta.row_count;
                    shard_total_bytes += meta.byte_size;
                    debug!(
                        shard = %shard_id,
                        measurement = %result.measurement,
                        segment = %meta.path.display(),
                        rows = meta.row_count,
                        "Shard flushed to segment"
                    );

                    // Emit compression ratio metric
                    if meta.uncompressed_bytes > 0 && meta.byte_size > 0 {
                        histogram!("chronix_segment_compression_ratio")
                            .record(meta.uncompressed_bytes as f64 / meta.byte_size as f64);
                    }

                    // Build column stats from finalize() metadata (no disk I/O).
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
                    let segment_header = meta.header.clone();
                    let column_metas = meta.column_metas.clone();

                    // Register in catalog — brief lock, no I/O
                    let segment_id = {
                        let mut catalog = self.catalog.write();
                        let seg_id = catalog.next_segment_id();

                        let entry = SegmentCatalogEntry {
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
                        };
                        catalog.add_segment(entry)?;
                        seg_id
                    };

                    // Populate metadata cache
                    self.metadata_cache.insert(CachedSegmentMeta {
                        segment_id,
                        header: segment_header,
                        columns: column_metas,
                    });

                    // Update time index
                    {
                        let mut indices = self.time_index.write();
                        let idx = indices.entry(shard_id).or_default();
                        idx.add_segment(TimeIndexEntry {
                            segment_id,
                            min_ts: meta.min_timestamp,
                            max_ts: meta.max_timestamp,
                        });
                    }

                    // Build, persist, and register bloom filter from series keys
                    if !result.series_keys.is_empty() {
                        let mut bloom = SeriesBloomFilter::new(
                            result.series_keys.len(),
                            0.01, // 1% false positive rate
                        );
                        for key in &result.series_keys {
                            bloom.insert(key);
                        }

                        // Persist bloom filter as sidecar file
                        let bloom_path = meta.path.with_extension("bloom");
                        match bloom.to_bytes() {
                            Ok(bytes) => {
                                if let Err(e) = std::fs::write(&bloom_path, bytes) {
                                    warn!(
                                        bloom = %bloom_path.display(),
                                        error = %e,
                                        "Failed to write bloom filter sidecar"
                                    );
                                }
                            }
                            Err(e) => {
                                warn!(
                                    bloom = %bloom_path.display(),
                                    error = %e,
                                    "Failed to serialize bloom filter"
                                );
                            }
                        }

                        self.blooms.write().insert(segment_id.0, bloom);
                    }

                    // Populate inverted index with tag values from flushed series
                    {
                        let tag_pairs: Vec<(&str, &str)> = result
                            .series_keys
                            .iter()
                            .flat_map(|sk| sk.tags().iter().map(|(k, v)| (k.as_ref(), v.as_ref())))
                            .collect();
                        if !tag_pairs.is_empty() {
                            self.tag_index.add_segment(segment_id, &tag_pairs);
                        }
                    }
                }

                // After flush, check for overlapping segments for
                // each measurement that received new segments.
                {
                    let flushed_measurements: HashSet<&str> =
                        results.iter().map(|r| r.measurement.as_str()).collect();
                    for m in flushed_measurements {
                        self.detect_segment_overlaps(m);
                    }
                }

                // Return results — callers (flush, maybe_flush) are responsible
                // for WAL truncation after ALL shards have been flushed, so that
                // early truncation cannot discard entries needed by other shards.

                // Emit per-shard flush metrics so operators can identify
                // slow or oversized shards independently.
                let shard_label = shard_id.to_string();
                histogram!("chronix_flush_shard_duration_seconds", "shard" => shard_label.clone())
                    .record(_shard_flush_start.elapsed().as_secs_f64());
                counter!("chronix_flush_shard_rows_total", "shard" => shard_label.clone())
                    .increment(shard_total_rows);
                counter!("chronix_flush_shard_bytes_total", "shard" => shard_label)
                    .increment(shard_total_bytes);

                Ok(results)
            }
            Err(chronix_engine::memtable::MemtableError::NoFrozenMemtable) => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
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
