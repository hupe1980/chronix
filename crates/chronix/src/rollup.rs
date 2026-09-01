//! Rollup configuration and incremental rollup computation.
//!
//! Rollups downsample raw data into coarser time buckets, enabling fast
//! long-range queries.  Rollup definitions are stored in the catalog and
//! executed incrementally during compaction.
//!
//! ## Multi-tier chains
//!
//! ```text
//! raw (10s) ──[5min rollup]──► raw_5min ──[1h rollup]──► raw_1h ──[1d rollup]──► raw_1d
//! ```

use std::collections::BTreeMap;
use std::path::Path;

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

/// Registry of rollup configurations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RollupRegistry {
    configs: BTreeMap<String, RollupConfig>,
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
        self.configs.remove(name)
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

    /// Persist the registry to a JSON file at the given path.
    ///
    /// The file is written atomically (write to `.tmp`, then rename) to avoid
    /// corruption on crash.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or file I/O fails.
    pub fn save(&self, path: &Path) -> std::result::Result<(), RollupError> {
        let json = serde_json::to_string_pretty(self).map_err(|e| {
            RollupError::Persistence(format!("failed to serialize rollup registry: {e}"))
        })?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, json.as_bytes()).map_err(|e| {
            RollupError::Persistence(format!(
                "failed to write rollup registry to {}: {e}",
                tmp.display()
            ))
        })?;
        std::fs::rename(&tmp, path).map_err(|e| {
            RollupError::Persistence(format!(
                "failed to rename {} -> {}: {e}",
                tmp.display(),
                path.display()
            ))
        })?;
        Ok(())
    }

    /// Load the registry from a JSON file.
    ///
    /// If the file does not exist an empty registry is returned.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be read or parsed.
    pub fn load(path: &Path) -> std::result::Result<Self, RollupError> {
        if !path.exists() {
            return Ok(Self::new());
        }
        let data = std::fs::read_to_string(path).map_err(|e| {
            RollupError::Persistence(format!(
                "failed to read rollup registry from {}: {e}",
                path.display()
            ))
        })?;
        let registry: Self = serde_json::from_str(&data).map_err(|e| {
            RollupError::Persistence(format!(
                "failed to parse rollup registry from {}: {e}",
                path.display()
            ))
        })?;
        Ok(registry)
    }

    /// Return the canonical filename for persisted rollup registries.
    #[must_use]
    pub fn filename() -> &'static str {
        "rollup_registry.json"
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
    ts - ts.rem_euclid(safe_interval)
}

/// Compute rollup points from Arrow `RecordBatches`.
///
/// Scans batches for a timestamp column and float field columns, groups
/// values into time buckets according to `config.interval_ns`, and emits
/// one [`chronix_core::Point`] per bucket per tag group containing all
/// requested aggregations as separate fields.
///
/// # Arguments
///
/// * `batches` — Input record batches (must contain a `"timestamp"` column).
/// * `config` — Rollup configuration defining interval, aggregations, etc.
///
/// # Returns
///
/// Vector of rollup [`chronix_core::Point`]s to be inserted into the target
/// measurement.
#[must_use]
pub fn compute_rollup_points(
    batches: &[arrow::record_batch::RecordBatch],
    config: &RollupConfig,
) -> Vec<chronix_core::Point> {
    use arrow::array::{Array, Float64Array, Int64Array, StringArray};

    // Group key = (bucket_start, tag_group_key)
    let mut accumulators: BTreeMap<(i64, BTreeMap<String, String>), BucketAccumulator> =
        BTreeMap::new();

    for batch in batches {
        // Find timestamp column index
        let Ok(ts_idx) = batch.schema().index_of("timestamp") else {
            continue;
        };
        let ts_array = batch.column(ts_idx).as_any().downcast_ref::<Int64Array>();
        let Some(ts_array) = ts_array else {
            continue;
        };

        // Build list of field columns (Float64 only) that aren't tags/timestamp
        let schema = batch.schema();
        let field_indices: Vec<(usize, String)> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| {
                f.name() != "timestamp"
                    && f.name() != "series_key_hash"
                    && !config.group_by_tags.contains(&f.name().clone())
                    && f.data_type() == &arrow::datatypes::DataType::Float64
            })
            .map(|(i, f)| (i, f.name().clone()))
            .collect();

        // Build tag column indices
        let tag_indices: Vec<(usize, String)> = config
            .group_by_tags
            .iter()
            .filter_map(|tag| schema.index_of(tag).ok().map(|idx| (idx, tag.clone())))
            .collect();

        for row in 0..batch.num_rows() {
            let ts = ts_array.value(row);
            let bucket = align_to_bucket(ts, config.interval_ns);

            // Extract tag group
            let mut tag_group = BTreeMap::new();
            for (idx, name) in &tag_indices {
                if let Some(arr) = batch.column(*idx).as_any().downcast_ref::<StringArray>() {
                    if arr.is_valid(row) {
                        tag_group.insert(name.clone(), arr.value(row).to_string());
                    }
                }
            }

            let acc = accumulators
                .entry((bucket, tag_group))
                .or_insert_with(|| BucketAccumulator::new(bucket));

            for (idx, name) in &field_indices {
                if let Some(arr) = batch.column(*idx).as_any().downcast_ref::<Float64Array>() {
                    if arr.is_valid(row) {
                        acc.accumulate(name, arr.value(row), ts);
                    }
                }
            }
        }
    }

    // Emit rollup points
    let mut points = Vec::new();
    for ((bucket, tags), acc) in &accumulators {
        let aggregated = acc.emit(&config.aggregations);

        // Build fields map: each agg_fn becomes a separate field
        let mut fields = BTreeMap::new();
        for (field_name, agg_values) in &aggregated {
            for (agg_fn, value) in agg_values {
                let key = format!("{field_name}_{agg_fn}");
                fields.insert(key, chronix_core::FieldValue::F64(*value));
            }
        }

        if fields.is_empty() {
            continue;
        }

        if let Ok(series_key) =
            chronix_core::SeriesKey::new(config.target_measurement.clone(), tags.clone())
        {
            if let Ok(point) = chronix_core::Point::new(series_key, fields, *bucket) {
                points.push(point);
            }
        }
    }

    points
}

/// Convert a slice of [`chronix_core::Point`] into an Arrow `RecordBatch`.
///
/// This function assembles a flat table with a `timestamp` column, one
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
    let mut fields = vec![Field::new("timestamp", DataType::Int64, false)];
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

// ── Ingest-time downsampling ────────────────────────────────────────

/// Ingest-time downsampler that aggregates incoming points in memory
/// and emits rolled-up points when a time bucket boundary is crossed.
///
/// Unlike compaction-time rollups which process flushed segments,
/// ingest-time downsampling produces aggregated data immediately on
/// write, enabling real-time low-resolution views.
///
/// Thread-safe: protected by `parking_lot::Mutex` internally.
pub struct IngestDownsampler {
    rules: Vec<RollupConfig>,
    /// (canonical_series_key, rule_index) → current bucket accumulator
    state: parking_lot::Mutex<std::collections::HashMap<(String, usize), IngestBucketState>>,
}

/// Per-series per-rule in-flight accumulator state.
struct IngestBucketState {
    bucket_start: i64,
    acc: BucketAccumulator,
    tags: BTreeMap<String, String>,
}

impl IngestDownsampler {
    /// Create a new downsampler with the given rules.
    pub fn new(rules: Vec<RollupConfig>) -> Self {
        Self {
            rules,
            state: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Returns true if there are any active rules.
    pub fn has_rules(&self) -> bool {
        !self.rules.is_empty()
    }

    /// Returns the currently registered rules.
    pub fn rules(&self) -> &[RollupConfig] {
        &self.rules
    }

    /// Process a point, accumulating it into the appropriate bucket.
    /// Returns any completed (flushed) rollup points when a bucket
    /// boundary is crossed.
    pub fn process(&self, point: &chronix_core::Point) -> Vec<chronix_core::Point> {
        let measurement = point.series_key().measurement();
        let mut emitted = Vec::new();

        for (rule_idx, rule) in self.rules.iter().enumerate() {
            if rule.source_measurement != measurement {
                continue;
            }

            let canonical = point.series_key().canonical_form().to_string();
            let key = (canonical, rule_idx);
            let bucket = align_to_bucket(point.timestamp(), rule.interval_ns);

            // Extract tag group for this rule
            let tag_group: BTreeMap<String, String> = rule
                .group_by_tags
                .iter()
                .filter_map(|t| {
                    point
                        .series_key()
                        .tag(t)
                        .map(|v| (t.clone(), v.to_string()))
                })
                .collect();

            let mut state = self.state.lock();
            let entry = state.entry(key);

            match entry {
                std::collections::hash_map::Entry::Occupied(mut occ) => {
                    let bs = occ.get_mut();
                    if bucket != bs.bucket_start {
                        // Bucket boundary crossed — emit completed bucket
                        if let Some(p) = Self::emit_point(&bs.acc, &bs.tags, rule) {
                            emitted.push(p);
                        }
                        // Start new bucket
                        bs.bucket_start = bucket;
                        bs.acc = BucketAccumulator::new(bucket);
                        bs.tags = tag_group;
                    }
                    Self::accumulate_fields(&mut bs.acc, point);
                }
                std::collections::hash_map::Entry::Vacant(vac) => {
                    let mut acc = BucketAccumulator::new(bucket);
                    Self::accumulate_fields(&mut acc, point);
                    vac.insert(IngestBucketState {
                        bucket_start: bucket,
                        acc,
                        tags: tag_group,
                    });
                }
            }
        }

        emitted
    }

    /// Flush all in-flight accumulators, emitting partial-bucket points.
    /// Called on shutdown or periodic flush.
    pub fn flush_all(&self) -> Vec<chronix_core::Point> {
        let mut state = self.state.lock();
        let mut emitted = Vec::new();

        for ((_, rule_idx), bs) in state.drain() {
            if let Some(rule) = self.rules.get(rule_idx) {
                if let Some(p) = Self::emit_point(&bs.acc, &bs.tags, rule) {
                    emitted.push(p);
                }
            }
        }

        emitted
    }

    fn accumulate_fields(acc: &mut BucketAccumulator, point: &chronix_core::Point) {
        let ts = point.timestamp();
        for (field_name, field_value) in point.fields() {
            if let chronix_core::FieldValue::F64(v) = field_value {
                acc.accumulate(field_name, *v, ts);
            }
        }
    }

    fn emit_point(
        acc: &BucketAccumulator,
        tags: &BTreeMap<String, String>,
        rule: &RollupConfig,
    ) -> Option<chronix_core::Point> {
        let aggregated = acc.emit(&rule.aggregations);
        let mut fields = BTreeMap::new();
        for (field_name, agg_values) in &aggregated {
            for (agg_fn, value) in agg_values {
                let key = format!("{field_name}_{agg_fn}");
                fields.insert(key, chronix_core::FieldValue::F64(*value));
            }
        }
        if fields.is_empty() {
            return None;
        }
        let sk =
            chronix_core::SeriesKey::new(rule.target_measurement.clone(), tags.clone()).ok()?;
        chronix_core::Point::new(sk, fields, acc.bucket_start_ns).ok()
    }
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
            Field::new("timestamp", DataType::Int64, false),
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
        assert!(batch.schema().index_of("timestamp").is_ok());
        assert!(batch.schema().index_of("host").is_ok());
        assert!(batch.schema().index_of("cpu_avg").is_ok());
        assert!(batch.schema().index_of("cpu_max").is_ok());
    }

    #[test]
    fn points_to_record_batch_empty() {
        assert!(points_to_record_batch(&[]).is_none());
    }

    #[test]
    fn registry_save_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(RollupRegistry::filename());

        let mut registry = RollupRegistry::new();
        let c1 = RollupBuilder::new()
            .name("cpu_5min")
            .source("cpu")
            .target("cpu_5min_agg")
            .interval_ns(300_000_000_000)
            .aggregation(RollupAggFn::Avg)
            .aggregation(RollupAggFn::Max)
            .group_by("host")
            .build()
            .unwrap();
        let c2 = RollupBuilder::new()
            .name("mem_1h")
            .source("mem")
            .target("mem_1h_agg")
            .interval_ns(3_600_000_000_000)
            .aggregation(RollupAggFn::Sum)
            .aggregation(RollupAggFn::Count)
            .retention_ns(86_400_000_000_000 * 30) // 30 days
            .build()
            .unwrap();
        registry.add(c1).unwrap();
        registry.add(c2).unwrap();
        registry.save(&path).unwrap();

        // Load into a fresh registry
        let loaded = RollupRegistry::load(&path).unwrap();
        assert_eq!(loaded.list().len(), 2);

        let cpu = loaded.get("cpu_5min").unwrap();
        assert_eq!(cpu.source_measurement, "cpu");
        assert_eq!(cpu.target_measurement, "cpu_5min_agg");
        assert_eq!(cpu.interval_ns, 300_000_000_000);
        assert_eq!(cpu.aggregations.len(), 2);
        assert!(cpu.aggregations.contains(&RollupAggFn::Avg));
        assert!(cpu.aggregations.contains(&RollupAggFn::Max));
        assert_eq!(cpu.group_by_tags, vec!["host"]);

        let mem = loaded.get("mem_1h").unwrap();
        assert_eq!(mem.source_measurement, "mem");
        assert_eq!(mem.retention_ns, Some(86_400_000_000_000 * 30));
    }

    #[test]
    fn registry_load_missing_file_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");
        let registry = RollupRegistry::load(&path).unwrap();
        assert!(registry.list().is_empty());
    }

    #[test]
    fn registry_load_corrupt_file_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(RollupRegistry::filename());
        std::fs::write(&path, b"not valid json{{{").unwrap();
        assert!(RollupRegistry::load(&path).is_err());
    }

    #[test]
    fn registry_save_overwrites_previous() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(RollupRegistry::filename());

        let mut registry = RollupRegistry::new();
        let c1 = RollupBuilder::new()
            .name("r1")
            .source("s1")
            .target("t1")
            .interval_ns(1_000_000_000)
            .aggregation(RollupAggFn::Avg)
            .build()
            .unwrap();
        registry.add(c1).unwrap();
        registry.save(&path).unwrap();

        // Remove and save again
        let _ = registry.remove("r1");
        registry.save(&path).unwrap();

        let loaded = RollupRegistry::load(&path).unwrap();
        assert!(loaded.list().is_empty());
    }

    #[test]
    fn ingest_downsampler_emits_on_bucket_boundary() {
        let rule = RollupBuilder::new()
            .name("cpu_10s")
            .source("cpu")
            .target("cpu_10s_agg")
            .interval_ns(10)
            .aggregation(RollupAggFn::Avg)
            .aggregation(RollupAggFn::Min)
            .aggregation(RollupAggFn::Max)
            .aggregation(RollupAggFn::Count)
            .group_by("host")
            .build()
            .unwrap();

        let ds = IngestDownsampler::new(vec![rule]);
        assert!(ds.has_rules());

        let mk = |ts: i64, val: f64| -> chronix_core::Point {
            let tags = BTreeMap::from([("host".to_string(), "srv1".to_string())]);
            let fields =
                BTreeMap::from([("value".to_string(), chronix_core::FieldValue::F64(val))]);
            let sk = chronix_core::SeriesKey::new("cpu", tags).unwrap();
            chronix_core::Point::new(sk, fields, ts).unwrap()
        };

        // Insert 3 points in bucket [0..10)
        assert!(ds.process(&mk(1, 10.0)).is_empty());
        assert!(ds.process(&mk(5, 20.0)).is_empty());
        assert!(ds.process(&mk(9, 30.0)).is_empty());

        // Crossing to bucket [10..20) should emit bucket [0..10)
        let emitted = ds.process(&mk(12, 40.0));
        assert_eq!(emitted.len(), 1);

        let p = &emitted[0];
        assert_eq!(p.series_key().measurement(), "cpu_10s_agg");
        assert_eq!(p.timestamp(), 0); // bucket start
        assert_eq!(p.series_key().tag("host"), Some("srv1"));

        // avg(10,20,30) = 20.0
        match p.field("value_avg").unwrap() {
            chronix_core::FieldValue::F64(v) => assert!((v - 20.0).abs() < f64::EPSILON),
            _ => panic!("Expected F64"),
        }
        // min = 10.0
        match p.field("value_min").unwrap() {
            chronix_core::FieldValue::F64(v) => assert!((v - 10.0).abs() < f64::EPSILON),
            _ => panic!("Expected F64"),
        }
        // max = 30.0
        match p.field("value_max").unwrap() {
            chronix_core::FieldValue::F64(v) => assert!((v - 30.0).abs() < f64::EPSILON),
            _ => panic!("Expected F64"),
        }
        // count = 3
        match p.field("value_count").unwrap() {
            chronix_core::FieldValue::F64(v) => assert!((v - 3.0).abs() < f64::EPSILON),
            _ => panic!("Expected F64"),
        }
    }

    #[test]
    fn ingest_downsampler_flush_all_emits_partial_buckets() {
        let rule = RollupBuilder::new()
            .name("cpu_1s")
            .source("cpu")
            .target("cpu_1s_agg")
            .interval_ns(1_000_000_000) // 1 second
            .aggregation(RollupAggFn::Sum)
            .group_by("host")
            .build()
            .unwrap();

        let ds = IngestDownsampler::new(vec![rule]);

        let tags = BTreeMap::from([("host".to_string(), "a".to_string())]);
        let fields = BTreeMap::from([("value".to_string(), chronix_core::FieldValue::F64(42.0))]);
        let sk = chronix_core::SeriesKey::new("cpu", tags).unwrap();
        let p = chronix_core::Point::new(sk, fields, 500_000_000).unwrap(); // mid-bucket

        assert!(ds.process(&p).is_empty()); // still in same bucket

        let flushed = ds.flush_all();
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].series_key().measurement(), "cpu_1s_agg");
        match flushed[0].field("value_sum").unwrap() {
            chronix_core::FieldValue::F64(v) => assert!((v - 42.0).abs() < f64::EPSILON),
            _ => panic!("Expected F64"),
        }

        // After flush, state should be empty
        assert!(ds.flush_all().is_empty());
    }

    #[test]
    fn ingest_downsampler_ignores_unmatched_measurements() {
        let rule = RollupBuilder::new()
            .name("cpu_rule")
            .source("cpu")
            .target("cpu_agg")
            .interval_ns(100)
            .aggregation(RollupAggFn::Avg)
            .group_by("host")
            .build()
            .unwrap();

        let ds = IngestDownsampler::new(vec![rule]);

        // Insert a "mem" point — should not match "cpu" rule
        let tags = BTreeMap::from([("host".to_string(), "b".to_string())]);
        let fields = BTreeMap::from([("value".to_string(), chronix_core::FieldValue::F64(1.0))]);
        let sk = chronix_core::SeriesKey::new("mem", tags).unwrap();
        let p = chronix_core::Point::new(sk, fields, 50).unwrap();

        assert!(ds.process(&p).is_empty());
        assert!(ds.flush_all().is_empty());
    }

    #[test]
    fn ingest_downsampler_no_rules() {
        let ds = IngestDownsampler::new(Vec::new());
        assert!(!ds.has_rules());
        assert!(ds.rules().is_empty());
        assert!(ds.flush_all().is_empty());
    }

    #[test]
    fn ingest_downsampler_multiple_series() {
        let rule = RollupBuilder::new()
            .name("cpu_5")
            .source("cpu")
            .target("cpu_5_agg")
            .interval_ns(10)
            .aggregation(RollupAggFn::Avg)
            .group_by("host")
            .build()
            .unwrap();

        let ds = IngestDownsampler::new(vec![rule]);

        let mk = |host: &str, ts: i64, val: f64| -> chronix_core::Point {
            let tags = BTreeMap::from([("host".to_string(), host.to_string())]);
            let fields =
                BTreeMap::from([("value".to_string(), chronix_core::FieldValue::F64(val))]);
            let sk = chronix_core::SeriesKey::new("cpu", tags).unwrap();
            chronix_core::Point::new(sk, fields, ts).unwrap()
        };

        // Two series in same bucket
        assert!(ds.process(&mk("a", 1, 10.0)).is_empty());
        assert!(ds.process(&mk("b", 2, 100.0)).is_empty());
        assert!(ds.process(&mk("a", 3, 20.0)).is_empty());
        assert!(ds.process(&mk("b", 4, 200.0)).is_empty());

        // Cross to new bucket — should emit 2 points (one per series)
        let mut emitted = ds.process(&mk("a", 11, 50.0));
        // "a" crosses boundary, "b" hasn't yet
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].series_key().tag("host"), Some("a"));
        match emitted[0].field("value_avg").unwrap() {
            chronix_core::FieldValue::F64(v) => assert!((v - 15.0).abs() < f64::EPSILON), // avg(10,20)
            _ => panic!("Expected F64"),
        }

        // Cross "b" too
        emitted = ds.process(&mk("b", 12, 300.0));
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].series_key().tag("host"), Some("b"));
        match emitted[0].field("value_avg").unwrap() {
            chronix_core::FieldValue::F64(v) => assert!((v - 150.0).abs() < f64::EPSILON), // avg(100,200)
            _ => panic!("Expected F64"),
        }
    }
}
