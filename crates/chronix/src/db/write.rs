//! Write-path methods for [`Chronix`] — admission, schema, durability, memtable.
//!
//! The order is the design:
//!
//! 1. **Admission** decides, for every point, whether it is accepted at all:
//!    the database is open, the memtable and WAL have room, the timestamp is
//!    inside the out-of-order window, and the series fits the cardinality
//!    budget. Nothing is durable yet, so a rejection costs nothing — and a
//!    rejected point cannot come back. That ordering is load-bearing:
//!    replay bypasses the window check (the active shard is unknown at that
//!    point), so anything appended to the WAL *is* accepted, whatever the
//!    caller was told.
//! 2. **Schema** registration is transactional per batch, and the columns a
//!    batch adds are persisted to the catalog manifest before any data that
//!    needs them is written. The manifest is the schema's only durable
//!    record; there is no schema entry in the data WAL.
//! 3. **WAL** append — one record for the batch — is the acknowledgement.
//! 4. **Memtable** insertion of what the WAL holds, unconditionally.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;

use metrics::{counter, gauge, histogram};
use tracing::{debug, warn};

use chronix_core::{wal_encode_write_point, Point, SeriesKey, Timestamp};
use chronix_streaming::cdc::CdcEvent;

use crate::error::{DbError, InsertResult, Result};

impl super::Chronix {
    /// Insert a single data point.
    ///
    /// Equivalent to [`insert_batch`](Self::insert_batch) with one point,
    /// with a rejection reported as the error rather than as a partial
    /// result.
    ///
    /// # Deduplication
    ///
    /// Writes are not deduplicated. If the same `(series, timestamp)` is
    /// written twice, the later write wins at read time — on every read
    /// path — and compaction drops the older copy.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed, the write is refused by
    /// admission control, the point's timestamp is outside the out-of-order
    /// window, the series would exceed the cardinality budget, a field's
    /// type conflicts with the schema, or the WAL append fails. In every
    /// case nothing was written.
    #[must_use = "insert errors indicate data loss"]
    pub fn insert(&self, point: &Point) -> Result<()> {
        let result = self.insert_batch(std::slice::from_ref(point))?;
        match result.rejected.into_iter().next() {
            None => Ok(()),
            Some((_, err)) => Err(err),
        }
    }

    /// Insert a batch of data points.
    ///
    /// The accepted points are written to the WAL as **one** record and
    /// then inserted into the memtable; the rejected ones are reported in
    /// [`InsertResult::rejected`] with their reasons. This is significantly
    /// cheaper than calling [`insert`](Self::insert) in a loop — one fsync
    /// under `FsyncPolicy::PerBatch` instead of one per point.
    ///
    /// # Errors
    ///
    /// Returns an error — and writes nothing — if the database is closed,
    /// admission control refuses the write, the batch would exceed the
    /// cardinality budget, any field's type conflicts with the schema, or
    /// the WAL append fails. Under `FsyncPolicy::Periodic` "written" means
    /// in the operating system's buffers; the periodic sync makes it
    /// durable.
    #[must_use = "insert_batch errors indicate data loss"]
    pub fn insert_batch(&self, points: &[Point]) -> Result<InsertResult> {
        self.write_batch(points, true)
    }

    /// Insert points **outside** the out-of-order window.
    ///
    /// Live ingestion is held to `±ooo_shard_tolerance` shards of the newest
    /// write, which is what keeps segments time-partitioned and the number
    /// of open memtables bounded. Importing history — a migration, a
    /// device's offline backlog, a rollup being materialised for a closed
    /// window — is the one legitimate reason to write far into the past,
    /// and it is a different operation with a different cost: the shards it
    /// touches are opened, flushed and later compacted like any other, and
    /// a query over that time range merges the new segments with the old.
    ///
    /// Everything else — admission control, the cardinality budget, the
    /// schema, one WAL record per call — is exactly as for
    /// [`insert_batch`](Self::insert_batch), and so is the result: the only
    /// per-point rejection here is a timestamp beyond
    /// `future_write_tolerance`, and it is reported, not swallowed.
    ///
    /// Backfilling a rollup's source below its watermark marks those
    /// buckets for re-materialisation; see
    /// [`materialise_rollups`](Self::materialise_rollups).
    ///
    /// # Errors
    ///
    /// As [`insert_batch`](Self::insert_batch).
    #[must_use = "backfill errors indicate data loss"]
    pub fn backfill(&self, points: &[Point]) -> Result<InsertResult> {
        self.write_batch(points, false)
    }

    /// Declare a field column before anything is written to it.
    ///
    /// Chronix is schema-on-write: a measurement's columns appear as the
    /// first point that carries them is written, and for a float or a
    /// counter that is all anyone needs. A **decimal** column is the
    /// exception, because its scale is part of its type and is fixed by
    /// whatever creates the column. Letting the first meter reading decide
    /// how many fractional digits a settlement register keeps is a coin
    /// toss: a device that happens to report `231.4` first pins the column
    /// at one digit, and `231.45` is then refused for ever.
    ///
    /// Declaring it makes that a decision:
    ///
    /// ```no_run
    /// # use chronix::Chronix;
    /// # use chronix_core::{ChronixConfig, ColumnType};
    /// # let config = ChronixConfig::builder().data_dir("/tmp/d").build().unwrap();
    /// # let db = Chronix::open(config).unwrap();
    /// // A quarter-hour register under BK 618-25-02, in kWh to four places.
    /// db.declare_field("meter", "z1nb_q", ColumnType::Decimal { scale: 4 })?;
    /// # Ok::<(), chronix::DbError>(())
    /// ```
    ///
    /// The measurement is created if it does not exist. Declaring a column
    /// that already exists with exactly this type is a no-op; declaring one
    /// that exists with a different type — including a different decimal
    /// scale — is an error, because that is a type change and Chronix does
    /// not have those.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed, the column exists with a
    /// different type, the name is invalid or reserved, or the catalog
    /// cannot be written.
    pub fn declare_field(
        &self,
        measurement: &str,
        field: &str,
        column_type: chronix_core::ColumnType,
    ) -> Result<()> {
        self.check_open()?;
        let actions = self.schema.declare_field(measurement, field, column_type)?;
        if !actions.is_empty() {
            self.persist_schema_actions(&actions)?;
        }
        Ok(())
    }

    /// Write the schemas the given actions touched to the catalog manifest.
    ///
    /// The manifest is a schema's only durable record, and it is written
    /// before any data that needs it.
    fn persist_schema_actions(&self, actions: &[chronix_core::schema::SchemaAction]) -> Result<()> {
        let measurements: HashSet<&str> = actions
            .iter()
            .map(|a| match a {
                chronix_core::schema::SchemaAction::CreateMeasurement(ms) => ms.measurement(),
                chronix_core::schema::SchemaAction::AddColumn { measurement, .. } => {
                    measurement.as_str()
                }
            })
            .collect();
        let mut catalog = self.catalog.write();
        for measurement in measurements {
            if let Some(ms) = self.schema.lookup(measurement) {
                catalog
                    .set_schema((*ms).clone())
                    .map_err(|e| DbError::Internal(format!("persisting schema: {e}")))?;
            }
        }
        Ok(())
    }

    /// The write path. `enforce_window` is the only difference between a
    /// live insert and a backfill.
    fn write_batch(&self, points: &[Point], enforce_window: bool) -> Result<InsertResult> {
        let start = std::time::Instant::now();
        self.check_open()?;
        if points.is_empty() {
            return Ok(InsertResult::default());
        }
        self.apply_backpressure();
        self.check_admission()?;

        // ── 1. Admission, per point, before anything is durable ────────
        //
        // A timestamp ahead of the wall clock is refused first, whether or
        // not the window is enforced: admitting one would anchor the
        // out-of-order window there and reject every real write that
        // follows — one device with a broken clock used to brick ingestion
        // until the next restart.
        let future_limit = Self::now_ns().saturating_add(
            i64::try_from(self.config.future_write_tolerance.as_nanos()).unwrap_or(i64::MAX),
        );
        let mut rejected: Vec<(usize, DbError)> = Vec::new();
        let mut admitted: Vec<&Point> = Vec::with_capacity(points.len());
        for (i, point) in points.iter().enumerate() {
            if point.timestamp() > future_limit {
                rejected.push((
                    i,
                    DbError::FutureTimestamp {
                        timestamp: point.timestamp(),
                        limit: future_limit,
                    },
                ));
                continue;
            }
            if !enforce_window {
                admitted.push(point);
                continue;
            }
            match self.shards.admit(point.timestamp()) {
                Ok(_) => admitted.push(point),
                Err(e) => rejected.push((i, e.into())),
            }
        }
        if admitted.is_empty() {
            counter!("chronix_write_rejected_points_total").increment(rejected.len() as u64);
            return Ok(InsertResult {
                accepted: 0,
                rejected,
            });
        }
        self.check_cardinality(&admitted)?;

        // ── 2. Schema: all of the batch or none of it, persisted first ──
        let actions = match self.schema.register_batch(&admitted) {
            Ok(actions) => actions,
            Err(e @ chronix_core::SchemaError::TypeConflict { .. }) => {
                counter!("chronix_field_type_conflicts_total").increment(1);
                warn!(error = %e, "field type conflict — rejecting batch");
                return Err(e.into());
            }
            Err(e) => return Err(e.into()),
        };
        if !actions.is_empty() {
            self.persist_schema_actions(&actions)?;
        }

        // ── 2b. Decimals take their column's scale, before the WAL ─────
        //
        // A decimal column stores one number of fractional digits. A value
        // that arrived with fewer is widened here — `1.5` into a scale-4
        // column becomes `1.5000` — so that the WAL record, the memtable
        // batch and every segment written from it agree. Without it two
        // segments of the same column could carry different scales, and a
        // scan across them could not produce one Arrow schema.
        //
        // A batch with no decimal fields returns `None` and copies nothing,
        // which is every batch on the ordinary metrics path.
        let normalized = self.schema.normalize_decimals(&admitted)?;
        let admitted: Vec<&Point> = match &normalized {
            Some(points) => points.iter().collect(),
            None => admitted,
        };

        // ── 3. WAL: one record, one acknowledgement ────────────────────
        let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(admitted.len());
        for point in &admitted {
            payloads.push(
                wal_encode_write_point(point).map_err(|e| {
                    DbError::Internal(format!("Failed to serialize WAL entry: {e}"))
                })?,
            );
        }
        let refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
        // The write epoch covers the gap between the WAL append and the
        // memtable insert. A flush computing the WAL floor takes it for
        // write, so it can never see a record that is durable but in no
        // memtable yet — the state in which a floor raised past it loses
        // the record at the next crash.
        let _in_flight = self.write_epoch.read();
        let wal_seq = self.wal.append_batch(&refs)?;

        // ── 4. Memtable: what the WAL holds is inserted ───────────────
        let has_cdc_subscribers = self.cdc_bus.subscriber_count() > 0;
        let mut cdc_events: Vec<CdcEvent> = Vec::new();
        for point in &admitted {
            self.shards.insert_admitted(point, wal_seq)?;
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
            if self.lvc_enabled_for(point.series_key().measurement()) {
                self.lvc.update(point);
            }
        }
        if !cdc_events.is_empty() {
            self.cdc_bus.publish_batch(&mut cdc_events);
        }

        drop(_in_flight);

        // A write below a rollup's watermark changes a bucket that has
        // already been aggregated. Record the range so the next
        // materialisation pass recomputes exactly those buckets: without
        // it, importing history or replaying a device's offline backlog
        // left the rollup tiers silently stale for ever, and retention then
        // dropped the raw data they were supposed to summarise.
        if !enforce_window {
            self.invalidate_rollups_for(&admitted);
        }

        self.maybe_flush();

        if !rejected.is_empty() {
            counter!("chronix_write_rejected_points_total").increment(rejected.len() as u64);
            debug!(
                accepted = admitted.len(),
                rejected = rejected.len(),
                "partial insert: some points fell outside the out-of-order window"
            );
        }
        histogram!("chronix_write_duration_seconds").record(start.elapsed().as_secs_f64());
        Ok(InsertResult {
            accepted: admitted.len(),
            rejected,
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
    /// Yields for a linearly escalating delay once the segment count
    /// exceeds the heavy threshold; the delay drops back to zero as
    /// compaction catches up.
    ///
    /// This blocks the current OS thread. Callers from async code must use
    /// `spawn_blocking` or `block_in_place` so a tokio worker is not
    /// stalled.
    fn apply_backpressure(&self) {
        // The count that compaction can reduce: the most segments any one
        // (shard, measurement) currently has. It used to be the catalog's
        // *total* segment count, which grows by one per shard forever, so
        // a database with a couple of days of hourly shards slept 500 ms on
        // every write batch, permanently, with nothing to compact.
        let l0_count = {
            let catalog = self.catalog.read();
            catalog.max_segments_per_shard_measurement()
        };
        let delay = self.compaction_picker.backpressure_delay_ms(l0_count);
        gauge!("chronix_compaction_backpressure_active").set(f64::from(u8::from(delay > 0)));
        if delay > 0 {
            debug!(l0_count, delay_ms = delay, "Write backpressure applied");
            std::thread::park_timeout(std::time::Duration::from_millis(delay));
        }
    }

    /// Write admission control — reject writes when the system is
    /// overloaded rather than accepting them and degrading.
    ///
    /// Returns [`DbError::TransientOverload`] when memtable memory is at
    /// `max_memtable_memory` (a flush is signalled and will resolve it) and
    /// [`DbError::PersistentOverload`] when the WAL writer is poisoned by an
    /// fsync failure, which needs the database reopened. A WAL that is full
    /// (`max_unflushed_wals` files) surfaces as the append error itself.
    ///
    /// **Which of the two a full memtable is depends on the maintenance
    /// thread.** `TransientOverload` is a promise — "back off, the flush that
    /// clears this is already signalled" — and it is only true while
    /// somebody is left to perform the flush. If that thread has ended, the
    /// same condition never clears and the database has to be reopened, so
    /// the honest answer is the persistent one. The server renders the two
    /// differently on purpose: `Retry-After: 1` against `Retry-After: 60`
    /// and a message naming the remedy.
    fn check_admission(&self) -> Result<()> {
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
            if !self.maintenance_alive.load(Ordering::Acquire) {
                return Err(DbError::PersistentOverload {
                    reason: format!(
                        "memtable memory at capacity ({mem} bytes >= {max_mem} bytes limit) and                          the maintenance thread is not running, so no flush will clear it;                          reopen the database"
                    ),
                });
            }
            self.wake_maintenance();
            return Err(DbError::TransientOverload {
                reason: format!(
                    "memtable memory at capacity ({mem} bytes >= {max_mem} bytes limit)"
                ),
            });
        }

        if self.wal.is_poisoned() {
            counter!("chronix_write_admission_rejected_total", "reason" => "wal_poisoned")
                .increment(1);
            return Err(DbError::PersistentOverload {
                reason: "the WAL writer is poisoned after an fsync failure; reopen the database"
                    .into(),
            });
        }
        Ok(())
    }

    /// Admit the net-new series of a batch against the cardinality budget,
    /// atomically: either every new series is registered or none is.
    ///
    /// `known_series` is the only cardinality bookkeeping in the engine, and
    /// it is exact — rebuilt from the segment sidecars at open and kept in
    /// step by writes and whole-series deletes, which rewrite the sidecars
    /// so the count survives a restart.
    fn check_cardinality(&self, points: &[&Point]) -> Result<()> {
        let mut new_canonicals: HashSet<&str> = HashSet::new();
        for point in points {
            let canonical = point.series_key().canonical_form();
            if !self.known_series.contains(canonical) {
                new_canonicals.insert(canonical);
            }
        }
        if new_canonicals.is_empty() {
            return Ok(());
        }

        let limit = self.config.max_series_cardinality;
        let current = self.known_series.len();
        let new_total = current + new_canonicals.len();
        if new_total > limit {
            return Err(DbError::CardinalityExceeded { current, limit });
        }
        Self::cardinality_early_warning(new_total, limit);
        for canonical in new_canonicals {
            // The namespace index is derived from the same set, so it is
            // maintained at the one point a series becomes known rather than
            // recomputed — otherwise the SQL planner's per-reference lookup
            // would be a scan of every series.
            crate::db::accessors::record_series(&self.namespace_measurements, canonical);
            self.known_series.insert(canonical.to_string());
        }
        Ok(())
    }

    /// Mark the rollup buckets covering these points as needing
    /// recomputation, if any rollup is fed by their measurement and has
    /// already passed them.
    fn invalidate_rollups_for(&self, points: &[&Point]) {
        let mut ranges: HashMap<&str, (i64, i64)> = HashMap::new();
        for point in points {
            let entry = ranges
                .entry(point.series_key().measurement())
                .or_insert((point.timestamp(), point.timestamp()));
            entry.0 = entry.0.min(point.timestamp());
            entry.1 = entry.1.max(point.timestamp());
        }
        let mut changed: Vec<String> = Vec::new();
        {
            let mut reg = self.rollup_registry.write();
            for (measurement, (lo, hi)) in ranges {
                changed.extend(reg.invalidate_source_range(measurement, lo, hi.saturating_add(1)));
            }
        }
        if changed.is_empty() {
            return;
        }
        let states = self.snapshot_rollup_states(&changed);
        if let Err(e) = self.persist_rollup_states(&states) {
            warn!(error = %e, "could not persist rollup invalidations");
        } else {
            counter!("chronix_rollup_invalidations_total").increment(changed.len() as u64);
        }
    }

    /// The wall clock in nanoseconds since the epoch.
    pub(super) fn now_ns() -> i64 {
        i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        )
        .unwrap_or(i64::MAX)
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

    /// Wake the maintenance thread when the memtable is over its flush
    /// threshold. Non-blocking: the flush happens on that thread.
    fn maybe_flush(&self) {
        let mem = self.shards.total_memory();
        gauge!("chronix_memtable_memory_bytes").set(mem as f64);
        if mem > self.config.memtable_flush_threshold {
            self.wake_maintenance();
        }
        // Past 90 % of the hard limit, say so: the next writes will be
        // refused by admission control until the flush lands.
        let emergency_threshold = self.config.max_memtable_memory * 9 / 10;
        if mem > emergency_threshold {
            warn!(
                memtable_bytes = mem,
                max_memtable_memory = self.config.max_memtable_memory,
                "memtable memory exceeds 90% of max — triggering emergency flush"
            );
            counter!("chronix_memtable_emergency_flush_total").increment(1);
            self.wake_maintenance();
        }
    }
}
