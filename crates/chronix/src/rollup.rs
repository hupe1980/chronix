//! Rollup configuration, the streaming bucket accumulator, and the
//! materialisation watermarks.
//!
//! Rollups downsample raw data into coarser time buckets, enabling fast
//! long-range queries and a long-retention tier. A rollup is
//! **materialised**, never computed from a segment: every bucket is
//! aggregated exactly once, over all the data that can ever reach it, by
//! [`Chronix::materialise_rollups`](crate::Chronix::materialise_rollups),
//! and the registry records how far each rollup has got.
//!
//! ## Multi-tier chains
//!
//! ```text
//! raw (1s) ──[1min rollup]──► raw_1m ──[15min rollup]──► raw_15m
//! ```
//!
//! A tier whose source is itself a rollup target is materialised only as
//! far as its source has been, so the chain stays consistent by
//! construction.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Error type for rollup configuration and validation.
#[derive(Debug, Clone, thiserror::Error)]
pub enum RollupError {
    /// A required configuration field is missing.
    #[error("{0}")]
    InvalidConfig(String),
    /// A rollup with the given name already exists.
    #[error("rollup '{0}' already exists")]
    AlreadyExists(String),
    /// Persistence error (save / load).
    #[error("rollup persistence error: {0}")]
    Persistence(String),
}

/// Aggregation functions supported by rollups.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum RollupAggFn {
    /// Arithmetic mean.
    Avg,
    /// Minimum value.
    Min,
    /// Maximum value.
    Max,
    /// Sum of all values.
    Sum,
    /// Count of data points.
    Count,
    /// First (oldest) value in the bucket — with `Last`, what a meter
    /// reading needs to become consumption per bucket.
    First,
    /// Last (most recent) value.
    Last,
}

impl std::fmt::Display for RollupAggFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Avg => write!(f, "avg"),
            Self::Min => write!(f, "min"),
            Self::Max => write!(f, "max"),
            Self::Sum => write!(f, "sum"),
            Self::Count => write!(f, "count"),
            Self::First => write!(f, "first"),
            Self::Last => write!(f, "last"),
        }
    }
}

/// Rollup configuration — defines how raw data is aggregated into
/// a target measurement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollupConfig {
    /// Name of this rollup (used as identifier for deletion).
    pub name: String,
    /// Source measurement to aggregate from.
    pub source_measurement: String,
    /// Target measurement to write aggregated data to.
    pub target_measurement: String,
    /// Aggregation interval in nanoseconds.
    pub interval_ns: i64,
    /// Aggregation functions to compute.
    pub aggregations: Vec<RollupAggFn>,
    /// Tags to preserve in rolled-up data (group-by).
    pub group_by_tags: Vec<String>,
    /// Optional retention for the target measurement (in nanoseconds).
    pub retention_ns: Option<i64>,
}

/// Builder for constructing [`RollupConfig`] ergonomically.
#[derive(Debug, Clone)]
pub struct RollupBuilder {
    name: Option<String>,
    source_measurement: Option<String>,
    target_measurement: Option<String>,
    interval_ns: Option<i64>,
    aggregations: Vec<RollupAggFn>,
    group_by_tags: Vec<String>,
    retention_ns: Option<i64>,
}

impl RollupBuilder {
    /// Create a new rollup builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            name: None,
            source_measurement: None,
            target_measurement: None,
            interval_ns: None,
            aggregations: Vec::new(),
            group_by_tags: Vec::new(),
            retention_ns: None,
        }
    }

    /// Set the rollup name.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set the source measurement.
    #[must_use]
    pub fn source(mut self, measurement: impl Into<String>) -> Self {
        self.source_measurement = Some(measurement.into());
        self
    }

    /// Set the target measurement.
    #[must_use]
    pub fn target(mut self, measurement: impl Into<String>) -> Self {
        self.target_measurement = Some(measurement.into());
        self
    }

    /// Set the aggregation interval in nanoseconds.
    #[must_use]
    pub fn interval_ns(mut self, ns: i64) -> Self {
        self.interval_ns = Some(ns);
        self
    }

    /// Add an aggregation function.
    #[must_use]
    pub fn aggregation(mut self, agg: RollupAggFn) -> Self {
        self.aggregations.push(agg);
        self
    }

    /// Add a group-by tag.
    #[must_use]
    pub fn group_by(mut self, tag: impl Into<String>) -> Self {
        self.group_by_tags.push(tag.into());
        self
    }

    /// Set the target measurement retention period.
    #[must_use]
    pub fn retention_ns(mut self, ns: i64) -> Self {
        self.retention_ns = Some(ns);
        self
    }

    /// Build the rollup configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if required fields are missing.
    pub fn build(self) -> std::result::Result<RollupConfig, RollupError> {
        let name = self
            .name
            .ok_or(RollupError::InvalidConfig("rollup name is required".into()))?;
        let source = self.source_measurement.ok_or(RollupError::InvalidConfig(
            "source measurement is required".into(),
        ))?;
        let target = self.target_measurement.ok_or(RollupError::InvalidConfig(
            "target measurement is required".into(),
        ))?;
        let interval = self
            .interval_ns
            .ok_or(RollupError::InvalidConfig("interval_ns is required".into()))?;

        if interval <= 0 {
            return Err(RollupError::InvalidConfig(
                "interval_ns must be positive".into(),
            ));
        }
        if self.aggregations.is_empty() {
            return Err(RollupError::InvalidConfig(
                "at least one aggregation function is required".into(),
            ));
        }
        if source == target {
            return Err(RollupError::InvalidConfig(
                "source and target measurements must differ".into(),
            ));
        }

        Ok(RollupConfig {
            name,
            source_measurement: source,
            target_measurement: target,
            interval_ns: interval,
            aggregations: self.aggregations,
            group_by_tags: self.group_by_tags,
            retention_ns: self.retention_ns,
        })
    }
}

impl Default for RollupBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// The maximum number of invalidated ranges kept per rollup before they are
/// merged into their hull.
///
/// A bounded log: a pathological writer scattering single points across a
/// year cannot make the catalog grow without limit. Merging costs
/// re-aggregation work, never correctness — the hull covers every range it
/// replaces.
const MAX_INVALID_RANGES: usize = 64;

/// How far a rollup has been materialised, and which buckets below that
/// watermark have to be computed again.
///
/// The watermark alone is not enough. A rollup bucket is aggregated when
/// its input is final, but "final" is a statement about the *live* write
/// path: a backfill, a delete, or an import can change a bucket's input
/// long afterwards. Every such write records the range it touched here, and
/// the next materialisation pass recomputes exactly those buckets before it
/// advances the watermark.
///
/// This is the same shape as TimescaleDB's invalidation log, with one
/// difference that matters: an invalidation outside Timescale's refresh
/// window is never revisited and the bucket stays wrong for ever, whereas
/// these are drained by the ordinary background pass. It is also what lets
/// the watermark be *aggressive* rather than exact — a bucket that turns out
/// not to have been final is repaired, not lost.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollupState {
    /// Exclusive end of the newest bucket that has been materialised, or
    /// `None` if nothing has been.
    pub materialised_until: Option<i64>,
    /// Half-open `[from, to)` ranges below the watermark whose buckets must
    /// be recomputed. Disjoint and ascending.
    #[serde(default)]
    pub invalid: Vec<(i64, i64)>,
}

impl RollupState {
    /// Record that `[from, to)` has to be recomputed.
    ///
    /// Ranges are kept disjoint, ascending and bounded in number; the parts
    /// at or above the watermark are dropped, because the watermark has not
    /// claimed them yet.
    pub fn invalidate(&mut self, from: i64, to: i64) {
        let Some(watermark) = self.materialised_until else {
            return;
        };
        let to = to.min(watermark);
        if from >= to {
            return;
        }
        self.invalid.push((from, to));
        self.invalid.sort_unstable();
        let mut merged: Vec<(i64, i64)> = Vec::with_capacity(self.invalid.len());
        for (lo, hi) in self.invalid.drain(..) {
            match merged.last_mut() {
                Some(last) if lo <= last.1 => last.1 = last.1.max(hi),
                _ => merged.push((lo, hi)),
            }
        }
        if merged.len() > MAX_INVALID_RANGES {
            let lo = merged.first().map_or(from, |r| r.0);
            let hi = merged.last().map_or(to, |r| r.1);
            merged = vec![(lo, hi)];
        }
        self.invalid = merged;
    }

    /// The ranges to recompute, and the total span they cover.
    #[must_use]
    pub fn pending_invalidations(&self) -> &[(i64, i64)] {
        &self.invalid
    }
}

/// Registry of rollup configurations and their materialisation state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RollupRegistry {
    configs: BTreeMap<String, RollupConfig>,
    #[serde(default)]
    state: BTreeMap<String, RollupState>,
}

impl RollupRegistry {
    /// Create a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a rollup configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if a rollup with the same name already exists.
    pub fn add(&mut self, config: RollupConfig) -> std::result::Result<(), RollupError> {
        if self.configs.contains_key(&config.name) {
            return Err(RollupError::AlreadyExists(config.name));
        }
        self.configs.insert(config.name.clone(), config);
        Ok(())
    }

    /// Remove a rollup configuration by name.
    #[must_use]
    pub fn remove(&mut self, name: &str) -> Option<RollupConfig> {
        self.state.remove(name);
        self.configs.remove(name)
    }

    /// The materialisation state of a rollup.
    #[must_use]
    pub fn state(&self, name: &str) -> RollupState {
        self.state.get(name).cloned().unwrap_or_default()
    }

    /// Record that `name` has been materialised up to `until` (exclusive).
    /// Never moves a watermark backwards.
    pub fn set_materialised_until(&mut self, name: &str, until: i64) {
        let entry = self.state.entry(name.to_string()).or_default();
        if entry.materialised_until.is_none_or(|cur| until > cur) {
            entry.materialised_until = Some(until);
        }
    }

    /// Replace a rollup's state wholesale (used when loading a catalog).
    pub fn set_state(&mut self, name: &str, state: RollupState) {
        self.state.insert(name.to_string(), state);
    }

    /// Record that `[from, to)` of `measurement` changed, so every rollup
    /// fed by it — directly or through another rollup — has to recompute
    /// the buckets covering that range.
    ///
    /// Returns the names whose state changed.
    pub fn invalidate_source_range(
        &mut self,
        measurement: &str,
        from: i64,
        to: i64,
    ) -> Vec<String> {
        let affected: Vec<(String, i64)> = self
            .rollups_rooted_at(measurement)
            .into_iter()
            .map(|c| (c.name.clone(), c.interval_ns))
            .collect();
        let mut changed = Vec::new();
        for (name, interval) in affected {
            let lo = align_to_bucket(from, interval);
            let hi = align_to_bucket(to.saturating_sub(1), interval).saturating_add(interval);
            let entry = self.state.entry(name.clone()).or_default();
            let before = entry.invalid.clone();
            entry.invalidate(lo, hi);
            if entry.invalid != before {
                changed.push(name);
            }
        }
        changed
    }

    /// Drop the invalidated ranges of `name` that have been recomputed.
    pub fn clear_invalidations(&mut self, name: &str, done: &[(i64, i64)]) {
        if let Some(state) = self.state.get_mut(name) {
            state.invalid.retain(|r| !done.contains(r));
        }
    }

    /// Is `measurement` the target of some rollup?
    #[must_use]
    pub fn is_rollup_target(&self, measurement: &str) -> bool {
        self.configs
            .values()
            .any(|c| c.target_measurement == measurement)
    }

    /// Would adding `candidate` make a measurement feed itself?
    ///
    /// A cycle is not a theoretical worry: `a → b` plus `b → a` makes the
    /// materialiser's chain loop write each tier from the other's output
    /// for ever, and retention then waits on a watermark that can never
    /// pass.
    #[must_use]
    pub fn would_cycle(&self, candidate: &RollupConfig) -> bool {
        if candidate.source_measurement == candidate.target_measurement {
            return true;
        }
        // Walk forward from the candidate's target: reaching its source
        // means the new edge closes a loop.
        let mut frontier = vec![candidate.target_measurement.as_str()];
        let mut seen: std::collections::HashSet<&str> = frontier.iter().copied().collect();
        while let Some(m) = frontier.pop() {
            if m == candidate.source_measurement {
                return true;
            }
            for c in self.rollups_for_source(m) {
                if seen.insert(&c.target_measurement) {
                    frontier.push(&c.target_measurement);
                }
            }
        }
        false
    }

    /// Every rollup fed, directly or through other rollups, by `source`.
    ///
    /// This is the set that must be materialised past a window before the
    /// raw data in it may be dropped: a 1 s → 1 min → 15 min cascade whose
    /// 15 min tier has not caught up has not yet produced the aggregate the
    /// raw data is being traded for.
    #[must_use]
    pub fn rollups_rooted_at(&self, source: &str) -> Vec<&RollupConfig> {
        let mut out: Vec<&RollupConfig> = Vec::new();
        let mut frontier: Vec<&str> = vec![source];
        let mut seen: std::collections::HashSet<&str> = frontier.iter().copied().collect();
        while let Some(m) = frontier.pop() {
            for c in self.rollups_for_source(m) {
                if out.iter().any(|o| o.name == c.name) {
                    continue;
                }
                out.push(c);
                // `seen` bounds the walk even if a cycle reached the
                // registry some other way; `would_cycle` refuses to create
                // one, but a loop here would hang a background pass.
                if seen.insert(&c.target_measurement) {
                    frontier.push(&c.target_measurement);
                }
            }
        }
        out
    }

    /// List all rollup configurations.
    #[must_use]
    pub fn list(&self) -> Vec<&RollupConfig> {
        self.configs.values().collect()
    }

    /// Get a rollup configuration by name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&RollupConfig> {
        self.configs.get(name)
    }

    /// Get all rollups for a given source measurement.
    #[must_use]
    pub fn rollups_for_source(&self, source: &str) -> Vec<&RollupConfig> {
        self.configs
            .values()
            .filter(|c| c.source_measurement == source)
            .collect()
    }

    /// Every rollup's state, for persistence.
    pub fn states(&self) -> impl Iterator<Item = (&String, &RollupState)> {
        self.state.iter()
    }
}

/// Incremental rollup accumulator for a single time bucket.
#[derive(Debug, Clone)]
pub struct BucketAccumulator {
    /// The time bucket start (aligned to interval).
    pub bucket_start_ns: i64,
    /// Running stats per field per aggregation.
    stats: BTreeMap<String, FieldStats>,
}

/// Per-field running statistics for aggregation.
#[derive(Debug, Clone)]
struct FieldStats {
    sum: f64,
    min: f64,
    max: f64,
    count: u64,
    first_ts: i64,
    first_value: f64,
    last_ts: i64,
    last_value: f64,
}

impl FieldStats {
    fn new() -> Self {
        Self {
            sum: 0.0,
            min: f64::MAX,
            max: f64::MIN,
            count: 0,
            first_ts: i64::MAX,
            first_value: 0.0,
            last_ts: i64::MIN,
            last_value: 0.0,
        }
    }

    fn update(&mut self, value: f64, timestamp: i64) {
        // Skip NaN values to prevent permanently corrupting the running sum.
        // NaN propagates through arithmetic (sum, min, max, avg) and would
        // poison aggregation results for the entire bucket.
        if value.is_nan() {
            return;
        }
        self.sum += value;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.count += 1;
        if timestamp < self.first_ts {
            self.first_ts = timestamp;
            self.first_value = value;
        }
        if timestamp >= self.last_ts {
            self.last_ts = timestamp;
            self.last_value = value;
        }
    }
}

impl BucketAccumulator {
    /// Create a new accumulator for a time bucket.
    #[must_use]
    pub fn new(bucket_start_ns: i64) -> Self {
        Self {
            bucket_start_ns,
            stats: BTreeMap::new(),
        }
    }

    /// Accumulate a value for a field.
    pub fn accumulate(&mut self, field: &str, value: f64, timestamp: i64) {
        self.stats
            .entry(field.to_string())
            .or_insert_with(FieldStats::new)
            .update(value, timestamp);
    }

    /// Emit aggregated values for the requested functions.
    ///
    /// Fields where every value was NaN (count == 0) are silently omitted
    /// to avoid emitting sentinel values (`f64::MAX`, `f64::MIN`, `0.0`)
    /// that would corrupt downstream dashboards and alerts.
    #[must_use]
    pub fn emit(&self, agg_fns: &[RollupAggFn]) -> BTreeMap<String, BTreeMap<RollupAggFn, f64>> {
        let mut result = BTreeMap::new();
        for (field, stats) in &self.stats {
            // Skip fields with no valid observations — all values were NaN.
            if stats.count == 0 {
                continue;
            }
            let mut aggs = BTreeMap::new();
            for &agg_fn in agg_fns {
                let value = match agg_fn {
                    RollupAggFn::Avg => {
                        if stats.count > 0 {
                            #[allow(clippy::cast_precision_loss)]
                            {
                                stats.sum / stats.count as f64
                            }
                        } else {
                            0.0
                        }
                    }
                    RollupAggFn::Min => stats.min,
                    RollupAggFn::Max => stats.max,
                    RollupAggFn::Sum => stats.sum,
                    #[allow(clippy::cast_precision_loss)]
                    RollupAggFn::Count => stats.count as f64,
                    RollupAggFn::First => stats.first_value,
                    RollupAggFn::Last => stats.last_value,
                };
                aggs.insert(agg_fn, value);
            }
            result.insert(field.clone(), aggs);
        }
        result
    }
}

/// Align a timestamp to a bucket boundary.
///
/// Uses Euclidean remainder so negative timestamps are correctly
/// snapped to the lower boundary, e.g. `align_to_bucket(-5, 10) == -10`.
/// This form (`ts - ts.rem_euclid(interval)`) avoids the overflow that
/// `div_euclid * interval` causes for large timestamps near `i64::MAX`.
///
/// If `interval_ns` is zero or negative, it is clamped to 1 to prevent
/// a `rem_euclid(0)` panic.
#[must_use]
pub fn align_to_bucket(ts: i64, interval_ns: i64) -> i64 {
    let safe_interval = interval_ns.max(1);
    // `rem_euclid` is non-negative, so this can only underflow, and only
    // within one interval of the floor. Saturating is exactly right there:
    // the bucket containing `i64::MIN` starts at `i64::MIN`.
    ts.saturating_sub(ts.rem_euclid(safe_interval))
}

/// Streaming rollup accumulator: folds time-ordered batches into buckets
/// and emits a bucket once a later one has started.
///
/// **Rows must arrive in ascending timestamp order**, within a batch and
/// across calls, which is what the scan iterator produces
/// ([`Chronix::execute_iter`](crate::Chronix::execute_iter) sorts every
/// bucket it emits). Then, when a row for bucket `B` arrives, every open
/// bucket before `B` is complete for every tag group, and memory is one
/// open bucket per group rather than the whole input.
///
/// The precondition is checked rather than assumed: a row that goes
/// backwards past an already-emitted bucket sets `saw_unordered_input`, and
/// the materialiser turns that into an error instead of writing a partial
/// aggregate. It was silently violated once — a single-segment scan came
/// back in series-major order, so each series closed the same bucket in
/// turn and last-write-wins kept one series' worth of a 20-series
/// aggregate.
pub struct RollupAccumulator<'a> {
    config: &'a RollupConfig,
    open: BTreeMap<(i64, BTreeMap<String, String>), BucketAccumulator>,
    /// The newest bucket start emitted so far — nothing older may arrive.
    closed_through: Option<i64>,
    /// Set when a row arrived for a bucket that had already been emitted.
    unordered: bool,
    /// When set, `push` emits nothing and every bucket is held until
    /// `finish` — for callers whose input is complete but unordered.
    hold_everything: bool,
}

impl<'a> RollupAccumulator<'a> {
    /// Create an accumulator for `config`.
    #[must_use]
    pub fn new(config: &'a RollupConfig) -> Self {
        Self {
            config,
            open: BTreeMap::new(),
            closed_through: None,
            unordered: false,
            hold_everything: false,
        }
    }

    /// An accumulator for input that is complete but not time-ordered: no
    /// bucket is emitted until [`finish`](Self::finish), so memory is the
    /// whole input rather than one open bucket per group.
    #[must_use]
    pub fn unordered(config: &'a RollupConfig) -> Self {
        Self {
            hold_everything: true,
            ..Self::new(config)
        }
    }

    /// `true` if a row arrived for a bucket that had already been emitted,
    /// which means the input was not time-ordered and the aggregates this
    /// accumulator produced are partial.
    #[must_use]
    pub fn saw_unordered_input(&self) -> bool {
        self.unordered
    }

    /// Fold one batch in. Returns the rollup points of every bucket that is
    /// now provably complete.
    pub fn push(&mut self, batch: &arrow::record_batch::RecordBatch) -> Vec<chronix_core::Point> {
        use arrow::array::{Array, Float64Array, Int64Array, StringArray, UInt64Array};

        let Ok(ts_idx) = batch.schema().index_of(chronix_core::TIME_COLUMN) else {
            return Vec::new();
        };
        let Some(ts_array) = batch.column(ts_idx).as_any().downcast_ref::<Int64Array>() else {
            return Vec::new();
        };

        let schema = batch.schema();
        // Every numeric field, not just `Float64`: an integer meter reading
        // is exactly the thing a rollup exists for, and skipping it used to
        // produce no rollup, no error, and then a retention pass that
        // dropped the raw data anyway.
        let field_indices: Vec<(usize, String)> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| {
                f.name() != chronix_core::TIME_COLUMN
                    && f.name() != "series_key_hash"
                    && !self.config.group_by_tags.contains(f.name())
                    && matches!(
                        f.data_type(),
                        arrow::datatypes::DataType::Float64
                            | arrow::datatypes::DataType::Int64
                            | arrow::datatypes::DataType::UInt64
                    )
            })
            .map(|(i, f)| (i, f.name().clone()))
            .collect();
        let tag_indices: Vec<(usize, String)> = self
            .config
            .group_by_tags
            .iter()
            .filter_map(|tag| schema.index_of(tag).ok().map(|idx| (idx, tag.clone())))
            .collect();

        let mut newest_bucket = i64::MIN;
        for row in 0..batch.num_rows() {
            let ts = ts_array.value(row);
            let bucket = align_to_bucket(ts, self.config.interval_ns);
            newest_bucket = newest_bucket.max(bucket);
            if self.closed_through.is_some_and(|c| bucket <= c) {
                self.unordered = true;
            }

            let mut tag_group = BTreeMap::new();
            for (idx, name) in &tag_indices {
                if let Some(arr) = batch.column(*idx).as_any().downcast_ref::<StringArray>() {
                    if arr.is_valid(row) {
                        tag_group.insert(name.clone(), arr.value(row).to_string());
                    }
                }
            }
            let acc = self
                .open
                .entry((bucket, tag_group))
                .or_insert_with(|| BucketAccumulator::new(bucket));
            for (idx, name) in &field_indices {
                let col = batch.column(*idx);
                #[allow(clippy::cast_precision_loss)]
                let value = if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
                    a.is_valid(row).then(|| a.value(row))
                } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                    a.is_valid(row).then(|| a.value(row) as f64)
                } else if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
                    a.is_valid(row).then(|| a.value(row) as f64)
                } else {
                    None
                };
                if let Some(v) = value {
                    acc.accumulate(name, v, ts);
                }
            }
        }

        if self.hold_everything {
            return Vec::new();
        }

        // Everything strictly before the newest bucket seen is complete.
        let closed: Vec<(i64, BTreeMap<String, String>)> = self
            .open
            .keys()
            .filter(|(b, _)| *b < newest_bucket)
            .cloned()
            .collect();
        let mut out = Vec::with_capacity(closed.len());
        for key in closed {
            if let Some(acc) = self.open.remove(&key) {
                self.closed_through = Some(self.closed_through.map_or(key.0, |c| c.max(key.0)));
                out.extend(self.emit(key.0, &key.1, &acc));
            }
        }
        out
    }

    /// The newest bucket start seen so far, if any.
    #[must_use]
    pub fn newest_bucket(&self) -> Option<i64> {
        self.open.keys().map(|(b, _)| *b).max()
    }

    /// Emit every bucket still open. Call once the input is exhausted.
    #[must_use]
    pub fn finish(self) -> Vec<chronix_core::Point> {
        let mut out = Vec::with_capacity(self.open.len());
        for ((bucket, tags), acc) in &self.open {
            out.extend(self.emit(*bucket, tags, acc));
        }
        out
    }

    fn emit(
        &self,
        bucket: i64,
        tags: &BTreeMap<String, String>,
        acc: &BucketAccumulator,
    ) -> Option<chronix_core::Point> {
        let aggregated = acc.emit(&self.config.aggregations);
        let mut fields = BTreeMap::new();
        for (field_name, agg_values) in &aggregated {
            for (agg_fn, value) in agg_values {
                fields.insert(
                    format!("{field_name}_{agg_fn}"),
                    chronix_core::FieldValue::F64(*value),
                );
            }
        }
        if fields.is_empty() {
            return None;
        }
        let series_key =
            chronix_core::SeriesKey::new(self.config.target_measurement.clone(), tags.clone())
                .ok()?;
        chronix_core::Point::new(series_key, fields, bucket).ok()
    }
}

/// Compute rollup points from Arrow `RecordBatches` that together hold
/// every row of the buckets they cover, in any order.
///
/// A convenience over [`RollupAccumulator`] for callers holding the whole
/// input in memory. Unlike the streaming accumulator it makes no ordering
/// assumption: nothing is emitted until every batch has been folded in.
#[must_use]
pub fn compute_rollup_points(
    batches: &[arrow::record_batch::RecordBatch],
    config: &RollupConfig,
) -> Vec<chronix_core::Point> {
    let mut acc = RollupAccumulator::unordered(config);
    for batch in batches {
        debug_assert!(
            acc.push(batch).is_empty(),
            "an unordered accumulator emits nothing early"
        );
    }
    let mut all = acc.finish();
    all.sort_by(|a, b| {
        a.timestamp().cmp(&b.timestamp()).then_with(|| {
            a.series_key()
                .canonical_form()
                .cmp(b.series_key().canonical_form())
        })
    });
    dedup_bucket_points(all)
}

/// Keep one point per `(series, bucket)`, the last one written.
fn dedup_bucket_points(points: Vec<chronix_core::Point>) -> Vec<chronix_core::Point> {
    let mut by_key: BTreeMap<(String, i64), chronix_core::Point> = BTreeMap::new();
    for p in points {
        by_key.insert(
            (p.series_key().canonical_form().to_string(), p.timestamp()),
            p,
        );
    }
    by_key.into_values().collect()
}

/// Convert a `RecordBatch` of `measurement` back into points: `_time`,
/// every `Utf8` column as a tag, every `Float64` column as a field.
#[must_use]
pub fn record_batch_to_points(
    batch: &arrow::record_batch::RecordBatch,
    measurement: &str,
) -> Vec<chronix_core::Point> {
    use arrow::array::{Array, Float64Array, Int64Array, StringArray};
    let schema = batch.schema();
    let Some(ts) = batch
        .column_by_name(chronix_core::TIME_COLUMN)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
    else {
        return Vec::new();
    };
    let tags: Vec<(&str, &StringArray)> = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, f)| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|a| (f.name().as_str(), a))
        })
        .collect();
    let fields: Vec<(&str, &Float64Array)> = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, f)| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(|a| (f.name().as_str(), a))
        })
        .collect();
    let mut out = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if ts.is_null(row) {
            continue;
        }
        let tag_map: BTreeMap<String, String> = tags
            .iter()
            .filter(|(_, a)| a.is_valid(row))
            .map(|(k, a)| ((*k).to_string(), a.value(row).to_string()))
            .collect();
        let field_map: BTreeMap<String, chronix_core::FieldValue> = fields
            .iter()
            .filter(|(_, a)| a.is_valid(row))
            .map(|(k, a)| {
                (
                    (*k).to_string(),
                    chronix_core::FieldValue::F64(a.value(row)),
                )
            })
            .collect();
        if field_map.is_empty() {
            continue;
        }
        if let Ok(key) = chronix_core::SeriesKey::new(measurement, tag_map) {
            if let Ok(p) = chronix_core::Point::new(key, field_map, ts.value(row)) {
                out.push(p);
            }
        }
    }
    out
}

/// Convert a slice of [`chronix_core::Point`] into an Arrow `RecordBatch`.
///
/// This function assembles a flat table with a `_time` column, one
/// `Utf8` column per distinct tag key, and one `Float64` column per
/// distinct field key.  Returns `None` if `points` is empty.
#[must_use]
pub fn points_to_record_batch(
    points: &[chronix_core::Point],
) -> Option<arrow::record_batch::RecordBatch> {
    use arrow::array::{Float64Builder, Int64Builder, StringBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::BTreeSet;
    use std::sync::Arc;

    if points.is_empty() {
        return None;
    }

    // Discover all tag keys and field keys
    let mut tag_keys: BTreeSet<String> = BTreeSet::new();
    let mut field_keys: BTreeSet<String> = BTreeSet::new();
    for p in points {
        for k in p.series_key().tag_keys() {
            tag_keys.insert(k.to_string());
        }
        for k in p.field_keys() {
            field_keys.insert(k.to_string());
        }
    }

    // Build schema: timestamp + tags (Utf8) + fields (Float64)
    let mut fields = vec![Field::new(
        chronix_core::TIME_COLUMN,
        DataType::Int64,
        false,
    )];
    for tag in &tag_keys {
        fields.push(Field::new(tag, DataType::Utf8, true));
    }
    for field in &field_keys {
        fields.push(Field::new(field, DataType::Float64, true));
    }
    let schema = Arc::new(Schema::new(fields));

    // Build arrays
    let mut ts_builder = Int64Builder::with_capacity(points.len());
    let mut tag_builders: Vec<StringBuilder> =
        tag_keys.iter().map(|_| StringBuilder::new()).collect();
    let mut field_builders: Vec<Float64Builder> = field_keys
        .iter()
        .map(|_| Float64Builder::with_capacity(points.len()))
        .collect();

    for p in points {
        ts_builder.append_value(p.timestamp());
        for (i, key) in tag_keys.iter().enumerate() {
            match p.series_key().tag(key) {
                Some(v) => tag_builders[i].append_value(v),
                None => tag_builders[i].append_null(),
            }
        }
        for (i, key) in field_keys.iter().enumerate() {
            match p.field(key) {
                Some(chronix_core::FieldValue::F64(v)) => field_builders[i].append_value(*v),
                _ => field_builders[i].append_null(),
            }
        }
    }

    let mut columns: Vec<Arc<dyn arrow::array::Array>> = vec![Arc::new(ts_builder.finish())];
    for b in &mut tag_builders {
        columns.push(Arc::new(b.finish()));
    }
    for b in &mut field_builders {
        columns.push(Arc::new(b.finish()));
    }

    arrow::record_batch::RecordBatch::try_new(schema, columns).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollup_builder_valid() {
        let config = RollupBuilder::new()
            .name("cpu_5min")
            .source("cpu")
            .target("cpu_5min_agg")
            .interval_ns(300_000_000_000)
            .aggregation(RollupAggFn::Avg)
            .aggregation(RollupAggFn::Max)
            .group_by("host")
            .build()
            .unwrap();

        assert_eq!(config.name, "cpu_5min");
        assert_eq!(config.source_measurement, "cpu");
        assert_eq!(config.target_measurement, "cpu_5min_agg");
        assert_eq!(config.aggregations.len(), 2);
        assert_eq!(config.group_by_tags, vec!["host"]);
    }

    #[test]
    fn rollup_builder_missing_name() {
        let result = RollupBuilder::new()
            .source("cpu")
            .target("cpu_5min")
            .interval_ns(300_000_000_000)
            .aggregation(RollupAggFn::Avg)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn rollup_builder_same_source_target() {
        let result = RollupBuilder::new()
            .name("test")
            .source("cpu")
            .target("cpu")
            .interval_ns(300_000_000_000)
            .aggregation(RollupAggFn::Avg)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn rollup_builder_negative_interval() {
        let result = RollupBuilder::new()
            .name("test")
            .source("cpu")
            .target("cpu_5min")
            .interval_ns(-1)
            .aggregation(RollupAggFn::Avg)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn rollup_builder_no_aggregations() {
        let result = RollupBuilder::new()
            .name("test")
            .source("cpu")
            .target("cpu_5min")
            .interval_ns(300_000_000_000)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn rollup_registry_crud() {
        let mut registry = RollupRegistry::new();

        let config = RollupBuilder::new()
            .name("cpu_5min")
            .source("cpu")
            .target("cpu_5min_agg")
            .interval_ns(300_000_000_000)
            .aggregation(RollupAggFn::Avg)
            .build()
            .unwrap();

        registry.add(config).unwrap();
        assert_eq!(registry.list().len(), 1);
        assert!(registry.get("cpu_5min").is_some());

        // Duplicate name
        let dup = RollupBuilder::new()
            .name("cpu_5min")
            .source("mem")
            .target("mem_5min")
            .interval_ns(300_000_000_000)
            .aggregation(RollupAggFn::Sum)
            .build()
            .unwrap();
        assert!(registry.add(dup).is_err());

        // Remove
        assert!(registry.remove("cpu_5min").is_some());
        assert!(registry.list().is_empty());
    }

    #[test]
    fn rollups_for_source() {
        let mut registry = RollupRegistry::new();

        let c1 = RollupBuilder::new()
            .name("cpu_5min")
            .source("cpu")
            .target("cpu_5min_agg")
            .interval_ns(300_000_000_000)
            .aggregation(RollupAggFn::Avg)
            .build()
            .unwrap();

        let c2 = RollupBuilder::new()
            .name("mem_5min")
            .source("mem")
            .target("mem_5min_agg")
            .interval_ns(300_000_000_000)
            .aggregation(RollupAggFn::Max)
            .build()
            .unwrap();

        registry.add(c1).unwrap();
        registry.add(c2).unwrap();

        assert_eq!(registry.rollups_for_source("cpu").len(), 1);
        assert_eq!(registry.rollups_for_source("mem").len(), 1);
        assert_eq!(registry.rollups_for_source("disk").len(), 0);
    }

    #[test]
    fn bucket_accumulator_basic() {
        let mut acc = BucketAccumulator::new(0);
        acc.accumulate("value", 10.0, 1000);
        acc.accumulate("value", 20.0, 2000);
        acc.accumulate("value", 30.0, 3000);

        let aggs = acc.emit(&[
            RollupAggFn::Avg,
            RollupAggFn::Min,
            RollupAggFn::Max,
            RollupAggFn::Sum,
            RollupAggFn::Count,
            RollupAggFn::Last,
        ]);

        let field_aggs = aggs.get("value").unwrap();
        assert!((field_aggs[&RollupAggFn::Avg] - 20.0).abs() < f64::EPSILON);
        assert!((field_aggs[&RollupAggFn::Min] - 10.0).abs() < f64::EPSILON);
        assert!((field_aggs[&RollupAggFn::Max] - 30.0).abs() < f64::EPSILON);
        assert!((field_aggs[&RollupAggFn::Sum] - 60.0).abs() < f64::EPSILON);
        assert!((field_aggs[&RollupAggFn::Count] - 3.0).abs() < f64::EPSILON);
        assert!((field_aggs[&RollupAggFn::Last] - 30.0).abs() < f64::EPSILON);
    }

    #[test]
    fn align_to_bucket_snaps_down() {
        assert_eq!(align_to_bucket(1_500, 1_000), 1_000);
        assert_eq!(align_to_bucket(2_000, 1_000), 2_000);
        assert_eq!(align_to_bucket(999, 1_000), 0);
    }

    #[test]
    fn bucket_accumulator_last_picks_latest() {
        let mut acc = BucketAccumulator::new(0);
        acc.accumulate("value", 100.0, 3000);
        acc.accumulate("value", 200.0, 1000); // earlier timestamp
        acc.accumulate("value", 300.0, 5000); // latest

        let aggs = acc.emit(&[RollupAggFn::Last]);
        let field_aggs = aggs.get("value").unwrap();
        assert!((field_aggs[&RollupAggFn::Last] - 300.0).abs() < f64::EPSILON);
    }

    #[test]
    fn compute_rollup_points_basic() {
        use arrow::array::{Float64Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        // Create 6 data points: 3 in first 10s bucket, 3 in second
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
            Field::new("cpu", DataType::Float64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 5, 9, 11, 15, 19])),
                Arc::new(StringArray::from(vec!["a", "a", "a", "a", "a", "a"])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0])),
            ],
        )
        .unwrap();

        let config = RollupBuilder::new()
            .name("test")
            .source("cpu_raw")
            .target("cpu_10s")
            .interval_ns(10)
            .aggregation(RollupAggFn::Avg)
            .aggregation(RollupAggFn::Min)
            .aggregation(RollupAggFn::Max)
            .group_by("host")
            .build()
            .unwrap();

        let points = compute_rollup_points(&[batch], &config);

        // 2 buckets × 1 host = 2 points
        assert_eq!(points.len(), 2);

        // Each point should target the "cpu_10s" measurement
        for p in &points {
            assert_eq!(p.series_key().measurement(), "cpu_10s");
            // Should have cpu_avg, cpu_min, cpu_max fields
            assert!(p.field("cpu_avg").is_some());
            assert!(p.field("cpu_min").is_some());
            assert!(p.field("cpu_max").is_some());
        }

        // Bucket 0: values [10, 20, 30] → avg=20, min=10, max=30
        let bucket0 = points.iter().find(|p| p.timestamp() == 0).unwrap();
        match bucket0.field("cpu_avg").unwrap() {
            chronix_core::FieldValue::F64(v) => assert!((v - 20.0).abs() < f64::EPSILON),
            _ => panic!("Expected F64"),
        }
        match bucket0.field("cpu_min").unwrap() {
            chronix_core::FieldValue::F64(v) => assert!((v - 10.0).abs() < f64::EPSILON),
            _ => panic!("Expected F64"),
        }
        match bucket0.field("cpu_max").unwrap() {
            chronix_core::FieldValue::F64(v) => assert!((v - 30.0).abs() < f64::EPSILON),
            _ => panic!("Expected F64"),
        }
    }

    #[test]
    fn points_to_record_batch_roundtrip() {
        // Create a few rollup points
        let tags = BTreeMap::from([("host".to_string(), "srv-1".to_string())]);
        let fields = BTreeMap::from([
            ("cpu_avg".to_string(), chronix_core::FieldValue::F64(25.0)),
            ("cpu_max".to_string(), chronix_core::FieldValue::F64(30.0)),
        ]);
        let key = chronix_core::SeriesKey::new("cpu_5min", tags).unwrap();
        let p = chronix_core::Point::new(key, fields, 1000).unwrap();

        let batch = points_to_record_batch(&[p]).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.schema().index_of(chronix_core::TIME_COLUMN).is_ok());
        assert!(batch.schema().index_of("host").is_ok());
        assert!(batch.schema().index_of("cpu_avg").is_ok());
        assert!(batch.schema().index_of("cpu_max").is_ok());
    }

    #[test]
    fn points_to_record_batch_empty() {
        assert!(points_to_record_batch(&[]).is_none());
    }

    /// The registry round-trips through the same encoding the catalog
    /// stores, definitions and watermarks alike.
    #[test]
    fn a_registry_round_trips_through_postcard() {
        let mut registry = RollupRegistry::new();
        registry
            .add(
                RollupBuilder::new()
                    .name("cpu_5min")
                    .source("cpu")
                    .target("cpu_5min_agg")
                    .interval_ns(300_000_000_000)
                    .aggregation(RollupAggFn::Avg)
                    .group_by("host")
                    .retention_ns(86_400_000_000_000 * 30)
                    .build()
                    .unwrap(),
            )
            .unwrap();
        registry.set_materialised_until("cpu_5min", 900_000_000_000);
        registry.invalidate_source_range("cpu", 0, 600_000_000_000);

        let config = registry.get("cpu_5min").unwrap();
        let config_bytes = postcard::to_stdvec(config).unwrap();
        let state_bytes = postcard::to_stdvec(&registry.state("cpu_5min")).unwrap();

        let mut restored = RollupRegistry::new();
        restored
            .add(postcard::from_bytes::<RollupConfig>(&config_bytes).unwrap())
            .unwrap();
        restored.set_state(
            "cpu_5min",
            postcard::from_bytes::<RollupState>(&state_bytes).unwrap(),
        );

        let back = restored.get("cpu_5min").unwrap();
        assert_eq!(back.source_measurement, "cpu");
        assert_eq!(back.interval_ns, 300_000_000_000);
        assert_eq!(back.retention_ns, Some(86_400_000_000_000 * 30));
        let state = restored.state("cpu_5min");
        assert_eq!(state.materialised_until, Some(900_000_000_000));
        assert_eq!(
            state.pending_invalidations(),
            &[(0, 600_000_000_000)],
            "a pending repair must survive a restart, or the bucket stays wrong"
        );
    }

    /// Invalidated ranges are clipped to the watermark, coalesced, and
    /// bounded in number.
    #[test]
    fn invalidations_are_clipped_coalesced_and_bounded() {
        let mut state = RollupState::default();
        // Nothing is materialised yet, so nothing needs repairing.
        state.invalidate(0, 100);
        assert!(state.pending_invalidations().is_empty());

        state.materialised_until = Some(1_000);
        // Clipped at the watermark: buckets above it are not claimed yet.
        state.invalidate(900, 5_000);
        assert_eq!(state.pending_invalidations(), &[(900, 1_000)]);
        // Adjacent and overlapping ranges coalesce.
        state.invalidate(800, 900);
        assert_eq!(state.pending_invalidations(), &[(800, 1_000)]);
        state.invalidate(100, 200);
        assert_eq!(state.pending_invalidations(), &[(100, 200), (800, 1_000)]);

        // A writer scattering points cannot grow the log without limit.
        for i in 0..200 {
            state.invalidate(i * 3, i * 3 + 1);
        }
        assert!(
            state.pending_invalidations().len() <= MAX_INVALID_RANGES,
            "the invalidation log must stay bounded"
        );
        let lo = state.pending_invalidations().first().unwrap().0;
        let hi = state.pending_invalidations().last().unwrap().1;
        assert!(
            lo <= 0 && hi >= 1_000,
            "merging must cover what it replaces"
        );
    }

    /// A range is aligned to the rollup's interval, not to the write.
    #[test]
    fn invalidating_a_source_range_covers_whole_buckets() {
        let mut registry = RollupRegistry::new();
        registry
            .add(
                RollupBuilder::new()
                    .name("m_1m")
                    .source("m")
                    .target("m_1m")
                    .interval_ns(60)
                    .aggregation(RollupAggFn::Avg)
                    .build()
                    .unwrap(),
            )
            .unwrap();
        registry.set_materialised_until("m_1m", 600);
        // A single point at 125 sits in the bucket [120, 180).
        let changed = registry.invalidate_source_range("m", 125, 126);
        assert_eq!(changed, vec!["m_1m"]);
        assert_eq!(
            registry.state("m_1m").pending_invalidations(),
            &[(120, 180)]
        );
    }

    /// A cycle is refused, and the chain walk terminates regardless.
    #[test]
    fn a_cycle_is_refused_and_the_walk_terminates() {
        let mut registry = RollupRegistry::new();
        let edge = |name: &str, from: &str, to: &str| {
            RollupBuilder::new()
                .name(name)
                .source(from)
                .target(to)
                .interval_ns(60)
                .aggregation(RollupAggFn::Avg)
                .build()
                .unwrap()
        };
        registry.add(edge("a_b", "a", "b")).unwrap();
        registry.add(edge("b_c", "b", "c")).unwrap();
        assert!(registry.would_cycle(&edge("c_a", "c", "a")));
        assert!(!registry.would_cycle(&edge("c_d", "c", "d")));
        // The builder already refuses source == target, so a self-edge
        // cannot even be constructed; `would_cycle` covers it too for the
        // deserialised path.
        assert!(RollupBuilder::new()
            .name("self")
            .source("a")
            .target("a")
            .interval_ns(60)
            .aggregation(RollupAggFn::Avg)
            .build()
            .is_err());
        assert_eq!(registry.rollups_rooted_at("a").len(), 2);
    }

    /// `align_to_bucket` must not overflow at either end of the range.
    #[test]
    fn aligning_a_bucket_saturates_at_the_floor() {
        assert_eq!(align_to_bucket(-5, 10), -10);
        assert_eq!(align_to_bucket(0, 10), 0);
        assert_eq!(
            align_to_bucket(i64::MAX, 60),
            i64::MAX - i64::MAX.rem_euclid(60)
        );
        // The bucket holding `i64::MIN` starts there; this used to panic in
        // a debug build and wrap in a release one.
        assert_eq!(align_to_bucket(i64::MIN, 60), i64::MIN);
        assert_eq!(align_to_bucket(i64::MIN, 1), i64::MIN);
    }
}
