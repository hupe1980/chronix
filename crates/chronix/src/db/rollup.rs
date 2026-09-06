//! Rollup definition, materialisation and read methods for [`Chronix`].

use std::sync::Arc;

use metrics::counter;
use tracing::{info, warn};

use arrow::record_batch::RecordBatch;
use chronix_core::Point;

use crate::error::{DbError, Result};
use crate::rollup::RollupConfig;

impl super::Chronix {
    /// Materialise every rollup as far as its input is final.
    ///
    /// For each rollup, the buckets between its watermark and the newest
    /// bucket whose input can no longer change are aggregated **once**, over
    /// every row that reaches them — segments and memtable, deduplicated,
    /// tombstones applied, one time-disjoint bucket of segments in memory at
    /// a time — written to the target measurement, and the watermark is
    /// advanced and persisted.
    ///
    /// "Can no longer change" is exact rather than a grace period. A raw
    /// measurement accepts live writes only within the out-of-order window,
    /// so every bucket that ends before the window's oldest shard is final.
    /// A rollup whose source is itself a rollup target is final exactly as
    /// far as that source has been materialised. Chains therefore stay
    /// consistent by construction, and a device that was offline for days
    /// materialises its backlog the moment it writes again, because the
    /// window is anchored on the newest write rather than on the clock.
    ///
    /// This is the **only** place rollups are computed: a bucket aggregated
    /// from whatever segment happens to be in hand is a partial bucket that
    /// last-write-wins then resolves to a fraction of the truth. Writes go
    /// through [`backfill`](Self::backfill), not the live path, whose
    /// out-of-order window rejects anything older than two shards by design.
    ///
    /// Called by [`compact`](Self::compact) and by
    /// [`enforce_retention`](Self::enforce_retention) before it drops a
    /// shard; callers that neither compact nor enforce retention call it
    /// themselves. Returns the number of rollup points written.
    ///
    /// # Errors
    ///
    /// Returns the first error from reading the source or writing the
    /// target. A rollup that fails keeps its watermark, so nothing is
    /// skipped; retention will not drop the raw data it needs.
    pub fn materialise_rollups(&self) -> Result<usize> {
        self.check_open()?;

        let configs: Vec<RollupConfig> = self
            .rollup_registry
            .read()
            .list()
            .into_iter()
            .cloned()
            .collect();
        if configs.is_empty() {
            return Ok(0);
        }

        let shard_ns = i64::try_from(self.config.shard_duration.as_nanos()).unwrap_or(i64::MAX);
        // The oldest shard a live write can still land in; everything
        // before it is aggregated now. This is a *guess* at finality, not a
        // proof — a backfill, a delete or an import can change a bucket
        // afterwards, and each of those records an invalidation that the
        // next pass drains. That is what lets the watermark be aggressive
        // rather than exact.
        let live_floor: Option<i64> = self.newest_shard().map(|active| {
            active
                .0
                .saturating_sub(i64::from(self.config.ooo_shard_tolerance))
                .saturating_mul(shard_ns)
        });

        let mut written = 0usize;
        let mut first_error: Option<DbError> = None;
        // Chains: a tier can only advance once its source has, so iterate
        // until a full pass changes nothing. Bounded by the chain depth.
        for _pass in 0..configs.len().max(1) {
            let mut progressed = false;
            for config in &configs {
                match self.materialise_one(config, live_floor) {
                    Ok((n, moved)) => {
                        written += n;
                        progressed |= moved;
                    }
                    Err(e) => {
                        // One rollup's failure must not starve the others:
                        // a stuck watermark holds its own raw data, and used
                        // to hold every rollup later in the alphabet too.
                        warn!(rollup = %config.name, error = %e, "rollup materialisation failed");
                        counter!("chronix_rollup_failures_total").increment(1);
                        first_error.get_or_insert(e);
                    }
                }
            }
            if !progressed {
                break;
            }
        }
        match first_error {
            Some(e) if written == 0 => Err(e),
            _ => Ok(written),
        }
    }

    /// The newest shard the database knows of — the newest write this
    /// session admitted, or the newest timestamp on disk.
    ///
    /// Anchoring on the data rather than on the clock is what lets a device
    /// that was offline for days materialise its backlog the moment it
    /// writes again. Consulting the catalog as well as the router is what
    /// makes a database that only ever `backfill`s — an import, a restore —
    /// materialise at all: `backfill` deliberately does not advance the
    /// live write window, so the router alone said "no data" for ever.
    fn newest_shard(&self) -> Option<chronix_core::ShardId> {
        let from_writes = self.shards.active_shard();
        let from_disk = self
            .catalog
            .read()
            .all_segments()
            .iter()
            .filter(|e| e.state == chronix_core::SegmentState::Active)
            .map(|e| e.max_timestamp)
            .max()
            .map(|ts| chronix_core::ShardId::from_timestamp(ts, self.config.shard_duration));
        match (from_writes, from_disk) {
            (Some(a), Some(b)) => Some(if a.0 >= b.0 { a } else { b }),
            (a, b) => a.or(b),
        }
    }

    /// Recompute one rollup's invalidated buckets, then advance it as far
    /// as its input is final. Returns `(points written, made progress)`.
    fn materialise_one(
        &self,
        config: &RollupConfig,
        live_floor: Option<i64>,
    ) -> Result<(usize, bool)> {
        let (state, source_is_rollup, source_until) = {
            let reg = self.rollup_registry.read();
            let upstream = reg
                .list()
                .into_iter()
                .find(|c| c.target_measurement == config.source_measurement)
                .map(|c| reg.state(&c.name).materialised_until);
            (
                reg.state(&config.name),
                upstream.is_some(),
                upstream.flatten(),
            )
        };

        let mut written = 0usize;
        let mut progressed = false;

        // ── 1. Repair: recompute the buckets a later write changed ──────
        //
        // Each range is deleted from the target and rewritten, so a bucket
        // whose source rows are now gone loses its aggregate instead of
        // keeping a stale one. The rewrite lands in a segment the delete's
        // tombstone does not name, so it is visible immediately.
        for (from, to) in state.pending_invalidations().to_vec() {
            self.delete_target_range(config, from, to)?;
            let n = self.materialise_range(config, from, to)?;
            written += n;
            self.wal.sync()?;
            let names: Vec<String> = {
                let mut reg = self.rollup_registry.write();
                reg.clear_invalidations(&config.name, &[(from, to)]);
                // A repaired range changes this tier's output, so every
                // tier downstream of it has to be repaired too.
                let downstream = reg.invalidate_source_range(&config.target_measurement, from, to);
                std::iter::once(config.name.clone())
                    .chain(downstream)
                    .collect()
            };
            self.persist_rollup_states(&self.snapshot_rollup_states(&names))?;
            progressed = true;
            info!(rollup = %config.name, from, to, points = n, "rollup range recomputed");
            counter!("chronix_rollup_ranges_recomputed_total").increment(1);
        }

        // ── 2. Advance: aggregate everything newly final ────────────────
        let final_before = if source_is_rollup {
            source_until
        } else {
            live_floor
        };
        let Some(final_before) = final_before else {
            return Ok((written, progressed));
        };
        let to = config.bucket.start_of(final_before);
        let from = state.materialised_until.unwrap_or(i64::MIN);
        if to <= from {
            return Ok((written, progressed));
        }

        let n = self.materialise_range(config, from, to)?;
        written += n;
        // The points must be durable before the watermark that claims them
        // is. Under `FsyncPolicy::Periodic` — the gateway preset — the WAL
        // append is only in the operating system's buffers, so a crash in
        // between left a watermark past rows that never existed, and
        // nothing recomputes a bucket below the watermark.
        self.wal.sync()?;
        self.rollup_registry
            .write()
            .set_materialised_until(&config.name, to);
        self.persist_rollup_states(
            &self.snapshot_rollup_states(std::slice::from_ref(&config.name)),
        )?;
        if n > 0 {
            info!(rollup = %config.name, points = n, until = to, "Rollup materialised");
            counter!("chronix_rollup_points_written_total").increment(n as u64);
        }
        Ok((written, true))
    }

    /// Mask the target measurement's existing aggregates over `[from, to)`,
    /// so a recomputed bucket replaces them rather than merging with them.
    fn delete_target_range(&self, config: &RollupConfig, from: i64, to: i64) -> Result<()> {
        if self.schema(&config.target_measurement).is_none() {
            return Ok(()); // nothing materialised there yet
        }
        let outcome = self.execute_delete(&crate::delete::DeleteRequest {
            measurement: config.target_measurement.clone(),
            tag_filters: Vec::new(),
            time_start: Some(from),
            time_end: Some(to.saturating_sub(1)),
        })?;
        if outcome.segments_skipped > 0 {
            return Err(DbError::Internal(format!(
                "recomputing {}: {} segment(s) of the target could not be read, so the old \
                 aggregates cannot be replaced",
                config.name, outcome.segments_skipped
            )));
        }
        Ok(())
    }

    /// Persist rollup state to the catalog.
    ///
    /// Takes the states **by value**, never the registry lock: the catalog
    /// is lock level 1 and the registry level 5, so holding the registry
    /// across a catalog write inverts the order the whole engine is built
    /// on. Callers snapshot under the registry lock, release it, then call
    /// this.
    pub(super) fn persist_rollup_states(
        &self,
        states: &[(String, crate::rollup::RollupState)],
    ) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        let mut catalog = self.catalog.write();
        for (name, state) in states {
            let bytes = postcard::to_stdvec(state)
                .map_err(|e| DbError::Internal(format!("encoding rollup state: {e}")))?;
            catalog
                .set_rollup_state(name, bytes)
                .map_err(|e| DbError::Internal(format!("persisting rollup state: {e}")))?;
        }
        Ok(())
    }

    /// Snapshot the named rollups' state, without holding the registry.
    pub(super) fn snapshot_rollup_states(
        &self,
        names: &[String],
    ) -> Vec<(String, crate::rollup::RollupState)> {
        let reg = self.rollup_registry.read();
        names.iter().map(|n| (n.clone(), reg.state(n))).collect()
    }

    /// Recompute `[start, end]` of a rollup from its source, now.
    ///
    /// The escape hatch for a range the engine could not know had changed —
    /// an out-of-band restore, a corrected import — and the operation a
    /// backfill or a delete schedules automatically.
    ///
    /// # Errors
    ///
    /// Returns an error if `name` is not a rollup, or if the recomputation
    /// fails.
    pub fn refresh_rollup(&self, name: &str, start: i64, end: i64) -> Result<usize> {
        self.check_open()?;
        let config = {
            let reg = self.rollup_registry.read();
            reg.get(name)
                .cloned()
                .ok_or_else(|| DbError::Internal(format!("no rollup named {name}")))?
        };
        // `next`, not `+ width`: the bucket holding `end` is a month long
        // when the tier is monthly and 25 hours long on a fall-back day.
        let from = config.bucket.start_of(start);
        let to = config.bucket.next(config.bucket.start_of(end));
        self.delete_target_range(&config, from, to)?;
        let written = self.materialise_range(&config, from, to)?;
        self.wal.sync()?;
        let downstream = self.rollup_registry.write().invalidate_source_range(
            &config.target_measurement,
            from,
            to,
        );
        self.persist_rollup_states(&self.snapshot_rollup_states(&downstream))?;
        Ok(written)
    }

    /// Aggregate `[from, to)` of a rollup's source into its target.
    fn materialise_range(&self, config: &RollupConfig, from: i64, to: i64) -> Result<usize> {
        /// Rollup points are written in chunks so a long backlog never
        /// sits in memory whole.
        const CHUNK: usize = 8_192;
        /// Flush after this many chunks, so a long backlog neither holds a
        /// year of shards open nor writes a segment per bucket.
        const FLUSH_EVERY_CHUNKS: usize = 8;

        let plan = self
            .query()
            .measurement(&config.source_measurement)
            .range(from, to.saturating_sub(1))
            .build()
            .map_err(|e| DbError::Internal(format!("rollup scan plan: {e}")))?;

        let mut acc = crate::rollup::RollupAccumulator::new(config);
        let mut pending: Vec<Point> = Vec::new();
        let mut written = 0usize;
        let mut flushes = 0usize;
        // Not bounded by `query_timeout`: nobody is waiting on a
        // materialisation, and a gateway catching up after a week
        // offline is *supposed* to take longer than a request would.
        // A deadline here would fail the pass, and the next pass, for ever.
        for batch in self.execute_iter(&plan)?.without_deadline() {
            pending.extend(acc.push(&batch?));
            if pending.len() >= CHUNK {
                written += self.backfill(&pending)?.into_complete()?;
                pending.clear();
                // A backfill opens a memtable per shard it touches, so a
                // long backlog is flushed as it goes. Counting chunks, not
                // buckets: a 15-minute tier over a year is 35 000 buckets
                // and the bucket-distance test flushed on nearly every one,
                // leaving hundreds of tiny segments behind.
                flushes += 1;
                if flushes.is_multiple_of(FLUSH_EVERY_CHUNKS) {
                    self.flush()?;
                }
            }
        }
        // The accumulator emits a bucket as soon as a later one starts, so
        // out-of-order input would write a partial aggregate and
        // last-write-wins would keep whichever partial came last. Refusing
        // is the only safe answer: the watermark then does not advance and
        // retention will not drop the raw data.
        let unordered = acc.saw_unordered_input();
        pending.extend(acc.finish());
        if !pending.is_empty() {
            written += self.backfill(&pending)?.into_complete()?;
        }
        if unordered {
            return Err(DbError::Internal(format!(
                "rollup {}: the scan of {} returned rows out of timestamp order",
                config.name, config.source_measurement
            )));
        }
        Ok(written)
    }

    /// Read a rollup over `[start, end]` — the materialised buckets **plus**
    /// the buckets that are not yet final, aggregated on the fly from the
    /// source.
    ///
    /// A materialised rollup lags the newest write by the out-of-order
    /// window, which is right for a three-year tier and wrong for a
    /// dashboard of the last hour. This is the real-time view: everything
    /// below the watermark comes from the target measurement, everything
    /// above it is computed from the raw data in the same pass, with the
    /// same accumulator, so the two halves cannot disagree. The live half
    /// is bounded by the window — at most a few shards of source data.
    ///
    /// Returns one row per `(bucket, tag group)`, ascending by bucket, with
    /// the target measurement's columns (`<field>_<agg>`), or an empty batch
    /// with that schema.
    ///
    /// # Errors
    ///
    /// Returns an error if `name` is not a rollup, the database is closed,
    /// or the scan fails.
    pub fn rollup(&self, name: &str, start: i64, end: i64) -> Result<RecordBatch> {
        self.check_open()?;
        let (config, watermark) = {
            let reg = self.rollup_registry.read();
            let config = reg
                .get(name)
                .cloned()
                .ok_or_else(|| DbError::Internal(format!("no rollup named {name}")))?;
            (
                config,
                reg.state(name).materialised_until.unwrap_or(i64::MIN),
            )
        };

        let mut points: Vec<Point> = Vec::new();

        // Materialised half: the target measurement below the watermark.
        if start < watermark {
            let plan = self
                .query()
                .measurement(&config.target_measurement)
                .range(start, end.min(watermark.saturating_sub(1)))
                .build()
                .map_err(|e| DbError::Internal(format!("rollup view plan: {e}")))?;
            for batch in self.execute_iter(&plan)? {
                points.extend(crate::rollup::record_batch_to_points(
                    &batch?,
                    &config.target_measurement,
                ));
            }
        }

        // Live half: the source above the watermark, aggregated now.
        if end >= watermark {
            let live_from = config.bucket.start_of(start.max(watermark));
            let plan = self
                .query()
                .measurement(&config.source_measurement)
                .range(live_from, end)
                .build()
                .map_err(|e| DbError::Internal(format!("rollup view plan: {e}")))?;
            let mut acc = crate::rollup::RollupAccumulator::new(&config);
            for batch in self.execute_iter(&plan)? {
                points.extend(acc.push(&batch?));
            }
            points.extend(acc.finish());
        }

        points.retain(|p| p.timestamp() >= start && p.timestamp() <= end);
        points.sort_by(|a, b| {
            a.timestamp().cmp(&b.timestamp()).then_with(|| {
                a.series_key()
                    .canonical_form()
                    .cmp(b.series_key().canonical_form())
            })
        });
        crate::rollup::points_to_record_batch(&points).map_or_else(
            || {
                let schema = self.schema(&config.target_measurement).map_or_else(
                    || Arc::new(arrow::datatypes::Schema::empty()),
                    |ms| Self::measurement_to_arrow_schema(&ms, &[]),
                );
                Ok(RecordBatch::new_empty(schema))
            },
            Ok,
        )
    }

    /// Has every rollup fed by `measurement` — directly or through other
    /// rollups — been materialised past `until` (exclusive)?
    ///
    /// The question retention asks before dropping raw data.
    pub(crate) fn rollups_materialised_past(&self, measurement: &str, until: i64) -> bool {
        let reg = self.rollup_registry.read();
        reg.rollups_rooted_at(measurement).iter().all(|c| {
            // The bucket *containing* `until` is done only once the
            // watermark is past its end: aligning `until` down and
            // comparing `>=` accepted a watermark that had not yet
            // aggregated the bucket the last rows fall in.
            let boundary = c.bucket.next(c.bucket.start_of(until));
            let state = reg.state(&c.name);
            state.materialised_until.is_some_and(|m| m >= boundary)
                // A range still waiting to be recomputed overlaps the data
                // about to be dropped: the aggregate is not yet what the
                // raw data says, so the raw data stays.
                && !state
                    .pending_invalidations()
                    .iter()
                    .any(|(lo, _)| *lo <= until)
        })
    }

    // ── Rollup API ──────────────────────────────────────────────────

    /// Register a rollup configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed or validation fails.
    pub fn create_rollup(&self, config: RollupConfig) -> Result<()> {
        self.check_open()?;
        if config.source_measurement == config.target_measurement {
            return Err(DbError::Internal(
                "a rollup's source and target must differ".into(),
            ));
        }
        let name = config.name.clone();
        let bytes = postcard::to_stdvec(&config)
            .map_err(|e| DbError::Internal(format!("encoding rollup: {e}")))?;
        {
            let mut registry = self.rollup_registry.write();
            if registry.would_cycle(&config) {
                return Err(DbError::Internal(format!(
                    "rollup {name} would make {} feed itself",
                    config.source_measurement
                )));
            }
            registry
                .add(config)
                .map_err(|e| DbError::Internal(format!("rollup registration failed: {e}")))?;
        }
        self.catalog
            .write()
            .set_rollup(&name, bytes)
            .map_err(|e| DbError::Internal(format!("rollup persistence failed: {e}")))?;
        Ok(())
    }

    /// A rollup's materialisation state: how far it has been aggregated,
    /// and which ranges below that are waiting to be recomputed because a
    /// later write changed their input.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed.
    pub fn rollup_state(&self, name: &str) -> Result<crate::rollup::RollupState> {
        self.check_open()?;
        Ok(self.rollup_registry.read().state(name))
    }

    /// List all registered rollup configurations.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed.
    pub fn list_rollups(&self) -> Result<Vec<RollupConfig>> {
        self.check_open()?;
        Ok(self
            .rollup_registry
            .read()
            .list()
            .into_iter()
            .cloned()
            .collect())
    }

    /// Remove a rollup configuration by name.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed or the rollup
    /// doesn't exist.
    pub fn delete_rollup(&self, name: &str) -> Result<bool> {
        self.check_open()?;
        let removed = self.rollup_registry.write().remove(name).is_some();
        if removed {
            self.catalog
                .write()
                .remove_rollup(name)
                .map_err(|e| DbError::Internal(format!("rollup persistence failed: {e}")))?;
        }
        Ok(removed)
    }
}
