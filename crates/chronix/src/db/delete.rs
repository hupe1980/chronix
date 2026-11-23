//! Delete-path methods for [`Chronix`] — drop, tombstone, hard-delete.

use std::collections::BTreeMap;

use metrics::counter;
use tracing::{info, warn};

use arrow::array::Array;
use chronix_core::{SeriesKey, Tombstone};
use chronix_engine::index::SegmentCatalogEntry;
#[cfg(feature = "streaming")]
use chronix_streaming::cdc::CdcEvent;

use crate::delete::{DeleteBuilder, DeleteOutcome, DeleteRequest};
use crate::error::{DbError, Result};

impl super::Chronix {
    /// Drop an entire measurement and all its data.
    ///
    /// This removes:
    /// * All on-disk segments for the measurement
    /// * The measurement schema from the catalog and registry
    /// * Bloom filters for the dropped segments
    /// * Time-index entries for the dropped segments
    ///
    /// In-flight memtable data is flushed first so that all data is in
    /// segments before removal.  The cardinality tracker (`known_series`)
    /// is cleared to reflect the reduced series universe — it will be
    /// lazily rebuilt by subsequent inserts.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed, flush fails, or
    /// segment deletion encounters an I/O error.
    pub fn drop_measurement(&self, measurement: &str) -> Result<()> {
        self.ensure_writable("dropping a measurement")?;
        self.check_open()?;

        // When soft_delete_ttl is configured, mark the measurement
        // as pending deletion instead of immediately removing data.
        // The background GC pass will hard-delete it after the TTL elapses.
        //
        // Persisted in the catalog manifest, not an in-memory map: the same
        // fix as a tombstone — an in-memory-only pending drop is
        // undone by every restart, which silently un-drops a measurement an
        // operator was told was gone and forgets the deadline that was
        // supposed to reclaim its disk.
        if let Some(ttl) = self.config.soft_delete_ttl {
            let now_ms = super::unix_ms_now();
            let deadline_ms = now_ms + ttl.as_millis() as u64;
            self.catalog
                .write()
                .set_measurement_pending_drop(measurement, deadline_ms)?;
            info!(
                measurement,
                deadline_ms,
                ttl_secs = ttl.as_secs(),
                "Measurement marked for soft-delete; will be hard-deleted after TTL"
            );

            // Emit CDC event so subscribers know about the pending drop.
            #[cfg(feature = "streaming")]
            self.cdc_bus.publish(CdcEvent::MeasurementDropped {
                measurement: measurement.to_string(),
                seq: 0,
            });

            return Ok(());
        }

        // Original hard-delete path (soft_delete_ttl = None).
        self.hard_delete_measurement(measurement)
    }

    /// Internal hard-delete implementation for a measurement.
    ///
    /// Flushes memtables, retires every segment of the measurement, and
    /// clears the catalog, schema, indexes and caches. Used by the immediate
    /// `drop_measurement`
    pub(super) fn hard_delete_measurement(&self, measurement: &str) -> Result<()> {
        // 1. Flush all shards so memtable data lands in segments
        self.flush()?;

        // 2. Collect segment IDs for this measurement
        let entries: Vec<SegmentCatalogEntry> = {
            let catalog = self.catalog.read();
            catalog
                .segments_for_measurement(measurement)
                .into_iter()
                .cloned()
                .collect()
        };

        // 3. Retire every segment of the measurement — the same path
        //    retention, compaction and archiving take, so a scan already
        //    holding one of these paths keeps its answer and the file goes on
        //    the next GC instead.
        self.retire_segments(&entries);

        {
            let mut catalog = self.catalog.write();
            // 4. Remove schema from catalog
            catalog.remove_schema(measurement)?;

            // Cancel any pending soft-delete: this *is* the hard delete it
            // was waiting for, and a stale pending-drop entry left behind
            // would apply to a measurement re-created under the same name
            // by a later write.
            catalog.cancel_measurement_pending_drop(measurement)?;
        }

        // 5. Remove schema from in-memory registry
        let _ = self.schema.remove(measurement);

        // 6. Reset cardinality tracker.
        //
        // Selectively remove only series belonging to the dropped measurement.
        // Canonical forms are prefixed with `measurement\0`, so `starts_with`
        // is unambiguous (NUL cannot appear in measurement or tag names).
        {
            let prefix = format!("{measurement}\0");
            self.known_series
                .retain(|canonical| !canonical.starts_with(&prefix));
            self.recount_namespace_series();
        }

        // 7. Forget the measurement in the namespace index.
        //
        // That index answers "may this namespace see this measurement", and a
        // dropped measurement exists for nobody. It follows the *measurement*
        // rather than its rows on purpose: retention emptying a measurement
        // must leave the table resolvable, or a tenant whose sensor went quiet
        // for longer than the retention window gets a planning error where an
        // empty result belongs.
        self.namespace_measurements.retain(|_, set| {
            set.remove(measurement);
            !set.is_empty()
        });

        // 8. Evict the measurement from the last-value cache. The
        //    per-segment indexes went with the retirement in step 3.
        self.lvc.evict_measurement(measurement);

        info!(measurement, segments = entries.len(), "Measurement dropped");

        // Emit CDC event for subscribers (seq auto-assigned by publish)
        #[cfg(feature = "streaming")]
        self.cdc_bus.publish(CdcEvent::MeasurementDropped {
            measurement: measurement.to_string(),
            seq: 0,
        });

        Ok(())
    }

    /// Delete every stored point of one series.
    ///
    /// "Every stored point" is meant literally, and the distinction matters:
    /// the tombstone's upper bound is resolved to the newest timestamp the
    /// series actually has, so a point written *afterwards* re-creates the
    /// series rather than disappearing into a standing delete. Re-provisioning
    /// a device under an identifier that had once been deleted is the ordinary
    /// case here.
    ///
    /// This delegates to [`execute_delete`](Self::execute_delete) rather than
    /// repeating it, so both paths flush first and both evict the last-value
    /// cache.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed or the delete fails.
    pub fn delete_series(&self, measurement: &str, tags: &BTreeMap<String, String>) -> Result<()> {
        self.ensure_writable("deleting")?;
        // Validated here rather than only inside the CDC event: a tag set the
        // canonical form cannot represent is a bad request whether or not
        // anybody is subscribed.
        let key = SeriesKey::new(measurement.to_string(), tags.clone())
            .map_err(|e| DbError::InvalidRequest(format!("invalid series key: {e}")))?;
        #[cfg(not(feature = "streaming"))]
        let _ = &key;

        let req = DeleteRequest {
            measurement: measurement.to_string(),
            tag_filters: tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            time_start: None,
            time_end: None,
        };
        self.execute_delete(&req)?;

        #[cfg(feature = "streaming")]
        self.cdc_bus.publish(CdcEvent::SeriesDeleted {
            measurement: measurement.to_string(),
            tags: tags.clone(),
            series_hash: key.hash_fnv(),
            seq: 0,
        });

        Ok(())
    }

    /// Reclaim tombstones that can no longer mask anything.
    ///
    /// A tombstone records the segments it was issued against. A segment
    /// leaves the catalog only by being rewritten — compaction applies
    /// tombstones as it merges — or by being deleted outright, so once none of
    /// a tombstone's segments remains, no stored row can still match it and
    /// the tombstone is provably dead.
    ///
    /// The question that matters is whether the delete has been
    /// *materialised* — not whether the series is still known, nor whether
    /// the measurement has active segments. Either weaker rule resurrects
    /// data.
    ///
    /// Returns the number of tombstones reclaimed.
    pub fn gc_tombstones(&self) -> usize {
        if self.config.read_only {
            return 0;
        }
        let removed = {
            let mut catalog = self.catalog.write();
            match catalog.reclaim_tombstones() {
                Ok(n) => n,
                Err(e) => {
                    warn!(error = %e, "failed to reclaim tombstones from the catalog");
                    return 0;
                }
            }
        };

        if removed > 0 {
            // Re-read rather than mirror the removals: the catalog is the
            // authority on which tombstones exist, and keeping one copy in
            // step with another by replaying edits is what let the two drift.
            let refreshed = self.catalog.read().tombstones().clone();
            *self.tombstones.write() = refreshed;

            counter!("chronix_tombstone_gc_total").increment(removed as u64);
            info!(reclaimed = removed, "GC: reclaimed materialised tombstones");
        }
        removed
    }

    // ── Predicate Delete ────────────────────────────────────────────

    /// Create a new delete request builder.
    ///
    /// Returns a [`DeleteBuilder`] for ergonomic construction of
    /// predicate-based delete requests.
    ///
    /// # Example
    ///
    /// ```ignore
    /// db.delete_builder()
    ///     .measurement("cpu")
    ///     .tag("host", "server-01")
    ///     .before(cutoff_ts)
    ///     .build()?;
    /// ```
    pub fn delete_builder(&self) -> DeleteBuilder {
        DeleteBuilder::new()
    }

    /// Execute a predicate-based delete request.
    ///
    /// Every series matching the measurement and tag filters is tombstoned
    /// over the request's time range. When the request names no range, the
    /// upper bound is resolved **per series** to the newest timestamp that
    /// series actually has, so the delete covers exactly the data that exists
    /// and a later write re-creates the series.
    ///
    /// The delete is durable before this returns: the resolved tombstones are
    /// appended to the catalog manifest and fsynced. They are logged to the
    /// data WAL as well, so a point-in-time restore replays them, but the
    /// catalog is what makes the delete survive a restart — the data WAL is
    /// truncated once the memtable it covers has been flushed.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed, or if the tombstones
    /// cannot be persisted.
    #[must_use = "delete errors must be handled"]
    pub fn execute_delete(&self, req: &DeleteRequest) -> Result<DeleteOutcome> {
        let _writer = self.rollup_pass.read();
        self.execute_delete_in_pass(req)
    }

    /// The caller holds the rollup pass gate for a correction or a refresh.
    pub(super) fn execute_delete_in_pass(&self, req: &DeleteRequest) -> Result<DeleteOutcome> {
        self.ensure_writable("deleting")?;
        self.check_open()?;

        // Flush all memtables so series not yet on disk are included in the scan.
        let _ = self.flush()?;

        let measurement = &req.measurement;
        let (start, end) = req.effective_range();

        // Find active segments for measurement in the time range, and lease
        // them: the scan below opens each one, and a retirement pass running
        // beside it would otherwise unlink one and count it as "skipped" —
        // turning a complete delete into a partial one for a reason that has
        // nothing to do with the data (see `db::leases`).
        let (entries, _lease) = {
            let catalog = self.catalog.read();
            let entries: Vec<SegmentCatalogEntry> = catalog
                .active_segments_for_measurement(measurement)
                .into_iter()
                .filter(|e| e.min_timestamp <= end && e.max_timestamp >= start)
                .cloned()
                .collect();
            let lease = self
                .segment_leases
                .acquire(entries.iter().map(|e| e.segment_id));
            (entries, lease)
        };

        // Per matched series: the newest timestamp seen inside the requested
        // window. That is the resolved upper bound for an unranged delete.
        let mut matched: BTreeMap<String, (SeriesKey, i64)> = BTreeMap::new();

        // Every segment scanned, including those that matched nothing: the
        // tombstone is reclaimable only once all of them have been rewritten,
        // and a segment that was skipped must keep it alive.
        let mut scanned_segments: Vec<u64> = Vec::new();

        // A segment that cannot be read is *not* deleted from. Count
        // those so the caller can tell a complete delete from a partial one
        // instead of receiving a plain success.
        let mut segments_skipped: u64 = 0;

        let segments_dir = self.segments_dir();
        for entry in &entries {
            scanned_segments.push(entry.segment_id.0);

            let reader = match self.open_segment(entry.file.resolve(&segments_dir)) {
                Ok(r) => r,
                Err(e) => {
                    warn!(segment = %entry.file, error = %e, "Skipping unreadable segment in delete");
                    segments_skipped += 1;
                    continue;
                }
            };

            let batch = match reader.read_all() {
                Ok(b) => b,
                Err(e) => {
                    warn!(segment = %entry.file, error = %e, "Failed to read segment in delete");
                    segments_skipped += 1;
                    continue;
                }
            };

            if batch.num_rows() == 0 {
                continue;
            }

            // Build tag filter refs
            let tag_refs: Vec<(&str, &str)> = req
                .tag_filters
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();

            let filtered = chronix_query::filter::filter_batch(&batch, start, end, &tag_refs)?;

            if filtered.num_rows() == 0 {
                continue;
            }

            // Tombstone all matching series hashes — use schema registry
            // to identify actual tag columns (not just Utf8 fields).
            let tags_col_names: Vec<String> = self
                .schema(measurement)
                .map(|ms| ms.tag_names().into_iter().map(String::from).collect())
                .unwrap_or_else(|| {
                    // Fallback: infer from Utf8 columns if schema is missing
                    filtered
                        .schema()
                        .fields()
                        .iter()
                        .filter(|f| {
                            f.name() != chronix_core::TIME_COLUMN
                                && f.data_type() == &arrow::datatypes::DataType::Utf8
                        })
                        .map(|f| f.name().clone())
                        .collect()
                });

            // The newest timestamp per series is what resolves an unranged
            // delete's upper bound, so the row loop cannot stop at the first
            // row of a series.
            let ts_col = filtered
                .column_by_name(chronix_core::TIME_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<arrow::array::Int64Array>())
                .ok_or_else(|| DbError::Internal("segment has no timestamp column".into()))?;

            for row in 0..filtered.num_rows() {
                let mut tags = BTreeMap::new();
                for col_name in &tags_col_names {
                    if let Some(col) = filtered.column_by_name(col_name)
                        && let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>()
                        && arr.is_valid(row)
                    {
                        tags.insert(col_name.clone(), arr.value(row).to_string());
                    }
                }
                let Ok(key) = SeriesKey::new(measurement.clone(), tags) else {
                    continue;
                };
                let ts = ts_col.value(row);
                matched
                    .entry(key.canonical_form().to_string())
                    .and_modify(|(_, max_ts)| *max_ts = (*max_ts).max(ts))
                    .or_insert((key, ts));
            }
        }

        // Resolve each match into a tombstone.
        //
        // The upper bound is the request's when it named one, and otherwise the
        // newest timestamp the series actually has: "delete what is stored", never
        // "mask this series forever", so later writes to it stay visible.
        let tombstones: Vec<Tombstone> = matched
            .iter()
            .map(|(canonical, (_, max_ts))| {
                let upper = req.time_end.unwrap_or(*max_ts);
                Tombstone::ranged(canonical.clone(), start, upper)
                    .with_segments(scanned_segments.iter().copied())
            })
            .collect();

        let tombstoned_count = tombstones.len() as u64;

        if !tombstones.is_empty() {
            // Persist before touching in-memory state, so a crash in the middle leaves
            // a database that has either applied the delete or not seen it.
            //
            // The catalog manifest is the only durable record, fsynced per append. No
            // WAL record is written: every point a tombstone covers is already flushed
            // and below the WAL floor, and on flash each extra fsync is wear.
            self.catalog
                .write()
                .record_tombstones(&tombstones)
                .map_err(|e| DbError::Internal(format!("Failed to persist tombstones: {e}")))?;

            {
                let mut live = self.tombstones.write();
                for tombstone in &tombstones {
                    live.insert(tombstone.clone());
                }
            }

            // `known_series` is the cardinality budget, and a series is
            // released from it only by a delete that covered all of that
            // series — which is exactly an unbounded request. A ranged delete
            // leaves the series alive, and releasing it there under-counts the
            // budget.
            //
            // Note the bound that is *not* usable here: the per-series maximum
            // collected above is the maximum inside the scanned window, so
            // comparing it against `time_end` is vacuous — it can never exceed
            // it. A `[.., X]` delete on a series holding data after `X` has to
            // read as partial, and only an absent bound proves it is not.
            let covers_whole_series = req.time_start.is_none() && req.time_end.is_none();
            for (canonical, (key, _)) in &matched {
                if covers_whole_series && self.known_series.remove(canonical.as_str()).is_some() {
                    self.release_namespace_series(canonical);
                }
                self.lvc.evict_by_key(key);
            }
            // A delete changes what every rollup fed by this measurement
            // should say. The buckets it touched are recomputed by the next
            // pass — which deletes the stale aggregates and writes the new
            // ones — so a deletion request reaches the derived tiers too,
            // rather than leaving the deleted rows summarised for ever.
            {
                let changed = self.rollup_registry.write().invalidate_source_range(
                    measurement,
                    start,
                    end.saturating_add(1),
                );
                if let Err(e) = self.persist_rollup_states(&changed) {
                    warn!(error = %e, "could not persist rollup invalidations after a delete");
                }
            }

            // The budget is rebuilt from the sidecars at open, so they have
            // to forget the series too — or the count came back at every
            // restart and the limit refused the replacement device.
            if covers_whole_series {
                let gone: std::collections::HashSet<&str> =
                    matched.keys().map(String::as_str).collect();
                for entry in &entries {
                    if let Err(e) = chronix_engine::index::series_index::remove_series(
                        &entry.file.resolve(&segments_dir),
                        &gone,
                    ) {
                        warn!(segment = %entry.file, error = %e, "could not rewrite the series index after a delete");
                    }
                }
            }
        }

        if segments_skipped > 0 {
            warn!(
                measurement,
                segments_skipped,
                "Predicate delete was PARTIAL — some segments could not be scanned"
            );
            metrics::counter!("chronix_delete_segments_skipped_total").increment(segments_skipped);
        }

        info!(
            measurement,
            tombstoned = tombstoned_count,
            segments_scanned = entries.len(),
            segments_skipped,
            "Predicate delete complete (durable in the catalog manifest)"
        );
        Ok(DeleteOutcome {
            series_tombstoned: tombstoned_count,
            segments_skipped,
        })
    }
}
