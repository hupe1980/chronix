//! Rollup definition, materialisation and read methods for [`Chronix`].

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use metrics::counter;
use tracing::{info, warn};

use arrow::record_batch::RecordBatch;
use chronix_core::{ColumnRole, ColumnType, MeasurementSchema, Point};

use crate::error::{DbError, Result};
use crate::rollup::{RollupAggFn, RollupConfig};

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
        self.ensure_writable("materialising rollups")?;
        let _pass = self.rollup_pass.write();
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
            self.persist_rollup_states(&names)?;
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
        self.persist_rollup_states(std::slice::from_ref(&config.name))?;
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
        let outcome = self.execute_delete_in_pass(&crate::delete::DeleteRequest {
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

    /// Serialize state persistence before taking a snapshot. A delayed writer
    /// then sees current state, even if another writer has already committed
    /// newer invalidations or an operator has deleted the declaration.
    pub(super) fn persist_rollup_states(&self, names: &[String]) -> Result<()> {
        if names.is_empty() {
            return Ok(());
        }
        let mut catalog = self.catalog.write();
        for name in names {
            let state = {
                let registry = self.rollup_registry.read();
                if registry.get(name).is_none() {
                    continue;
                }
                registry.state(name)
            };
            let bytes = postcard::to_stdvec(&state)
                .map_err(|e| DbError::Internal(format!("encoding rollup state: {e}")))?;
            catalog.set_declared_state(
                chronix_engine::index::catalog::DeclaredKind::Rollup,
                name,
                bytes,
            )?;
        }
        Ok(())
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
        self.ensure_writable("refreshing a rollup")?;
        let _pass = self.rollup_pass.write();
        self.check_open()?;
        let config = {
            let reg = self.rollup_registry.read();
            reg.get(name).cloned().ok_or_else(|| DbError::NotFound {
                kind: "rollup",
                name: name.to_string(),
            })?
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
        self.persist_rollup_states(&downstream)?;
        Ok(written)
    }

    /// Aggregate `[from, to)` of a rollup's source into its target.
    fn materialise_range(&self, config: &RollupConfig, from: i64, to: i64) -> Result<usize> {
        // A rollup reads the source decrypted and writes an aggregate into a
        // *different* measurement, whose columns are named after the
        // aggregation (`avg_x`, `last_x`) and are therefore not the declared
        // encrypted ones — so the aggregate would land in plaintext. Checked
        // here as well as at `create_rollup`, because a source measurement
        // can gain an encrypted column after the rollup was created, and
        // this pass runs unattended.
        #[cfg(feature = "field-encryption")]
        {
            let names = self.column_names_of(&config.source_measurement);
            self.refuse_encrypted_columns(
                names.iter().map(String::as_str),
                &format!("materialising rollup '{}'", config.name),
            )?;
        }

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
                written += self.backfill_in_pass(&pending)?.into_complete()?;
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
        // A rollup aggregates numbers, so a string field has no `avg` and is
        // skipped — which leaves the target quietly narrower than its source.
        // Said out loud once per pass, because the two times this code was
        // wrong before (integer columns, then decimal ones) it was wrong in
        // exactly this way: the column was missing, and nothing said so.
        let skipped = acc.skipped_columns();
        if !skipped.is_empty() {
            warn!(
                rollup = %config.name,
                source = %config.source_measurement,
                columns = %skipped.join(", "),
                "Rollup skipped non-numeric field column(s): the target has no aggregate for them"
            );
        }
        pending.extend(acc.finish());
        if !pending.is_empty() {
            written += self.backfill_in_pass(&pending)?.into_complete()?;
        }
        if unordered {
            return Err(DbError::Internal(format!(
                "rollup {}: the scan of {} returned rows out of timestamp order",
                config.name, config.source_measurement
            )));
        }
        Ok(written)
    }

    /// Read stored buckets below the watermark and aggregate newer buckets
    /// from source observations.
    ///
    /// Selects bucket starts in the inclusive range and aggregates all available
    /// observations in each selected bucket, even when `end` is its start.
    /// Returns one row per (bucket, tag group), in ascending bucket order.
    /// Once the source schema is known, empty and populated batches have
    /// the same timestamp, grouping tags and aggregate fields.
    ///
    /// # Errors
    ///
    /// Returns an error if `name` is not a rollup, the database is closed,
    /// or the scan fails.
    pub fn rollup(&self, name: &str, start: i64, end: i64) -> Result<RecordBatch> {
        self.rollup_where(name, start, end, &[])
    }

    /// [`rollup`](Self::rollup) restricted to the series matching `tags`.
    ///
    /// Filters apply to both stored and live buckets. Only grouping tags are
    /// available on the stored target.
    ///
    /// # Errors
    ///
    /// As [`rollup`](Self::rollup), plus [`DbError::InvalidRequest`] if a tag
    /// key is not one the rollup groups by.
    pub fn rollup_where(
        &self,
        name: &str,
        start: i64,
        end: i64,
        tags: &[(&str, &str)],
    ) -> Result<RecordBatch> {
        self.check_open()?;
        let (config, watermark) = {
            let reg = self.rollup_registry.read();
            let config = reg.get(name).cloned().ok_or_else(|| DbError::NotFound {
                kind: "rollup",
                name: name.to_string(),
            })?;
            (
                config,
                reg.state(name).materialised_until.unwrap_or(i64::MIN),
            )
        };

        for (key, _) in tags {
            if !config.group_by_tags.iter().any(|g| g == key) {
                return Err(DbError::InvalidRequest(format!(
                    "rollup {name:?} does not group by tag {key:?}; it groups by {:?}",
                    config.group_by_tags
                )));
            }
        }

        if start > end {
            return Err(DbError::InvalidRequest(
                "rollup range start must be at or before end".into(),
            ));
        }
        let mut points: Vec<Point> = Vec::new();

        // Materialised half: the target measurement below the watermark.
        if start < watermark {
            let mut q = self
                .query()
                .measurement(&config.target_measurement)
                .range(start, end.min(watermark.saturating_sub(1)));
            for (k, v) in tags {
                q = q.tag(*k, *v);
            }
            let plan = q
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
        let lower = start.max(watermark);
        let containing = config.bucket.start_of(lower);
        let live_from = if containing < lower {
            config.bucket.next(containing)
        } else {
            containing
        };
        let last_bucket = config.bucket.start_of(end);
        if live_from <= last_bucket {
            // The last representable bucket has no representable successor.
            let next = config.bucket.next(last_bucket);
            let live_end = if next == i64::MAX && config.bucket.start_of(i64::MAX) == last_bucket {
                i64::MAX
            } else {
                next.saturating_sub(1)
            };
            let mut q = self
                .query()
                .measurement(&config.source_measurement)
                .range(live_from, live_end);
            for (k, v) in tags {
                q = q.tag(*k, *v);
            }
            let plan = q
                .build()
                .map_err(|e| DbError::Internal(format!("rollup view plan: {e}")))?;
            let mut acc = crate::rollup::RollupAccumulator::new(&config);
            for batch in self.execute_iter(&plan)? {
                points.extend(acc.push(&batch?));
            }
            if acc.saw_unordered_input() {
                return Err(DbError::Internal(format!(
                    "rollup {name}: source scan returned rows out of timestamp order"
                )));
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
        let schema = self.rollup_read_schema(&config)?;
        if points.is_empty() {
            return Ok(RecordBatch::new_empty(schema));
        }
        let batch = chronix_query::points_to_record_batch(&points)?;
        if batch.schema_ref() == &schema {
            return Ok(batch);
        }
        Ok(chronix_query::dedup::align_batch(&batch, &schema)?)
    }

    fn rollup_read_schema(&self, config: &RollupConfig) -> Result<Arc<arrow::datatypes::Schema>> {
        let Some(source) = self.schema(&config.source_measurement) else {
            return Ok(self.schema(&config.target_measurement).map_or_else(
                || Arc::new(arrow::datatypes::Schema::empty()),
                |target| Self::measurement_to_arrow_schema(&target, &[]),
            ));
        };

        // Use the same column order as a point batch: time, sorted tags,
        // sorted fields. A sparse group then only changes which cells are null.
        let mut output = MeasurementSchema::new(&config.target_measurement);
        let tags: BTreeSet<_> = config.group_by_tags.iter().map(String::as_str).collect();
        for tag in &tags {
            let _ = output.add_tag(tag);
        }
        let mut fields = BTreeMap::new();
        for column in source.columns() {
            if column.role != ColumnRole::Field
                || column.name == "series_key_hash"
                || tags.contains(column.name.as_str())
            {
                continue;
            }
            for &agg in &config.aggregations {
                if let Some(ty) = aggregate_column_type(column.column_type, agg) {
                    fields.insert(format!("{}_{agg}", column.name), ty);
                }
            }
        }
        for (name, ty) in fields {
            output.declare_field(&name, ty)?;
        }
        Ok(Self::measurement_to_arrow_schema(&output, &[]))
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

    /// Register a rollup under a new name.
    ///
    /// Use [`ensure_rollup`](Self::ensure_rollup) to apply a declaration on
    /// every startup.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Conflict`] if the name already exists, or an error
    /// if the database is closed, validation fails, or persistence fails.
    pub fn create_rollup(&self, config: RollupConfig) -> Result<()> {
        self.register_rollup(config, false)
    }

    /// Ensure a named rollup exists with the requested definition.
    ///
    /// Call this on every startup, including before the first source write.
    /// Equivalent declarations preserve the stored definition and its watermark.
    /// Aggregations and grouping tags compare as sets. Bucket width, zone and
    /// origin, source and target names, and retention must match.
    ///
    /// # Errors
    ///
    /// [`DbError::RollupDefinitionConflict`] lists each differing property.
    /// Creation has the same validation and persistence errors as
    /// [`create_rollup`](Self::create_rollup).
    pub fn ensure_rollup(&self, config: RollupConfig) -> Result<()> {
        self.register_rollup(config, true)
    }

    fn register_rollup(&self, config: RollupConfig, accept_equivalent: bool) -> Result<()> {
        self.ensure_writable("creating a rollup")?;
        self.check_open()?;
        if config.source_measurement == config.target_measurement {
            return Err(DbError::InvalidRequest(
                "a rollup's source and target must differ".into(),
            ));
        }
        // Fast feedback: the same rule the materialiser enforces, at the
        // moment somebody asks for the rollup rather than hours later in a
        // background pass.
        #[cfg(feature = "field-encryption")]
        {
            let names = self.column_names_of(&config.source_measurement);
            self.refuse_encrypted_columns(
                names.iter().map(String::as_str),
                &format!("a rollup over '{}'", config.source_measurement),
            )?;
        }

        // Acquire the catalog (level 1) before the registry (level 5).
        // Keep the name reserved until persistence succeeds so a concurrent
        // startup cannot acknowledge a declaration that failed to reach disk.
        let mut catalog = self.catalog.write();
        let mut registry = self.rollup_registry.write();
        if let Some(existing) = registry.get(&config.name) {
            if !accept_equivalent {
                return Err(DbError::Conflict {
                    kind: "rollup",
                    name: config.name,
                });
            }
            let fields = existing.differing_fields(&config);
            if fields.is_empty() {
                return Ok(());
            }
            return Err(DbError::RollupDefinitionConflict {
                name: config.name,
                fields,
            });
        }
        if registry.would_cycle(&config) {
            return Err(DbError::InvalidRequest(format!(
                "rollup {} would make {} feed itself",
                config.name, config.source_measurement
            )));
        }
        let bytes = postcard::to_stdvec(&config)
            .map_err(|e| DbError::Internal(format!("encoding rollup: {e}")))?;
        catalog.set_declared(
            chronix_engine::index::catalog::DeclaredKind::Rollup,
            &config.name,
            bytes,
        )?;
        registry
            .add(config)
            .map_err(|e| DbError::Internal(format!("rollup registration failed: {e}")))?;
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
        self.ensure_writable("deleting a rollup")?;
        let _pass = self.rollup_pass.write();
        self.check_open()?;
        let mut catalog = self.catalog.write();
        let mut registry = self.rollup_registry.write();
        if registry.get(name).is_none() {
            return Ok(false);
        }
        catalog.remove_declared(chronix_engine::index::catalog::DeclaredKind::Rollup, name)?;
        Ok(registry.remove(name).is_some())
    }
}

/// Type emitted by the rollup accumulator for an aggregatable source field.
fn aggregate_column_type(source: ColumnType, agg: RollupAggFn) -> Option<ColumnType> {
    use ColumnType::{Decimal, F64, Histogram, I64, U64};
    use RollupAggFn::{Avg, Count, Max, Min};
    match (source, agg) {
        (F64 | I64 | U64 | Decimal { .. } | Histogram, Count) => Some(U64),
        (F64 | I64 | U64, _) => Some(F64),
        (Decimal { scale }, Avg) => Some(Decimal {
            scale: scale
                .saturating_add(chronix_query::aggregate::AVG_EXTRA_SCALE)
                .min(chronix_core::MAX_DECIMAL_SCALE),
        }),
        (Decimal { .. }, _) => Some(source),
        (Histogram, Min | Max)
        | (ColumnType::Timestamp | ColumnType::String | ColumnType::Bool, _) => None,
        (Histogram, _) => Some(Histogram),
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::*;

    #[test]
    fn historical_correction_waiting_for_a_pass_invalidates_its_new_watermark() {
        const HOUR: i64 = 3_600_000_000_000;
        let dir = tempfile::tempdir().unwrap();
        let mut config = ChronixConfig::small(dir.path());
        config.maintenance_interval = std::time::Duration::ZERO;
        let db = Chronix::open(config).unwrap();
        let declaration = RollupBuilder::new()
            .name("qh")
            .source("power")
            .target("summary")
            .every("15m")
            .aggregation(RollupAggFn::Avg)
            .build()
            .unwrap();
        db.ensure_rollup(declaration.clone()).unwrap();
        let series = SeriesKey::new("power", Default::default()).unwrap();
        let original = Point::new(series.clone(), crate::fields! {"watts" => 0.0}, 0).unwrap();
        let correction = Point::new(series, crate::fields! {"watts" => 10.0}, 1).unwrap();
        db.insert(&original).unwrap();
        let pass = db.rollup_pass.write();
        let ready = std::sync::Barrier::new(2);
        // Hold the gate across the same scan + watermark transition used
        // by maintenance while the corrective writer starts up.
        std::thread::scope(|writers| {
            let handle = writers.spawn(|| {
                ready.wait();
                db.backfill(&[correction]).unwrap().into_complete().unwrap();
            });
            ready.wait();
            db.materialise_one(&declaration, Some(HOUR)).unwrap();
            drop(pass);
            handle.join().unwrap();
        });
        assert_eq!(
            db.rollup_state("qh").unwrap().pending_invalidations(),
            &[(0, HOUR / 4)]
        );
        db.materialise_rollups().unwrap();
        let batch = db.rollup("qh", 0, 0).unwrap();
        let avg = batch
            .column_by_name("watts_avg")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(avg.value(0), 5.0);
    }

    #[test]
    fn delayed_state_persistence_cannot_resurrect_a_dropped_rollup() {
        let dir = tempfile::tempdir().unwrap();
        let db = Chronix::open(ChronixConfig::small(dir.path())).unwrap();
        let config = RollupBuilder::new()
            .name("qh")
            .source("power")
            .target("summary")
            .every("15m")
            .aggregation(RollupAggFn::Count)
            .build()
            .unwrap();
        db.ensure_rollup(config).unwrap();
        // A writer computed this list before being descheduled. Deletion
        // lands before it gets the catalog lock to persist the invalidations.
        let changed = vec!["qh".to_owned()];
        db.delete_rollup("qh").unwrap();
        db.persist_rollup_states(&changed).unwrap();
        assert_eq!(db.catalog.read().rollups().count(), 0);
    }
}
