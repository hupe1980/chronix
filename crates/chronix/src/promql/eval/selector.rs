//! Vector and matrix selector evaluation for PromQL.
//!
//! Handles instant vector selectors (`metric{label="val"}`) and range
//! vector / matrix selectors (`metric{label="val"}[5m]`).
//!
//! # One fetch, one conversion
//!
//! Both selectors need the same three things — a scan over a window, a
//! conversion of Arrow batches into labelled samples, and a window filter —
//! and they differ only in which window they ask for and how much of it they
//! keep. They therefore share [`fetch_scan`](PromQLEvaluator::fetch_scan) and
//! [`collect_series`]; two copies of a hundred lines of batch decoding is
//! exactly the shape of divergence that has produced wrong answers here
//! before.
//!
//! # Window boundaries
//!
//! Range selectors and the lookback window are **left-open and
//! right-closed** — `(start, end]`. Prometheus 3.0 made this change so that
//! perfectly spaced samples yield a constant sample count: with 1 m samples,
//! `[5m]` selects five of them regardless of whether a sample happens to land
//! exactly on the left boundary.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::promql::ast::{Duration, Expr, LabelMatcher, MatchOp, PromQLValue, Sample, Series};

use super::{compile_post_filters, CompiledMatcher, EvalError, PromQLEvaluator, QueryParams};
use super::{ScanKey, SCAN_CACHE_CAPACITY};

/// Samples grouped by label set, ascending by timestamp within each group.
type SeriesMap = BTreeMap<Vec<(String, String)>, Vec<Sample>>;

// ── PromQLEvaluator methods ────────────────────────────────────────────

impl PromQLEvaluator {
    /// Read `[start, end)` for a measurement, reusing an earlier read of the
    /// same window.
    ///
    /// A range query evaluates every selector once per step against an
    /// identical widened window, so without the cache the widening turns
    /// O(steps) narrow scans into O(steps) full-range scans — strictly worse
    /// than not widening at all.
    fn fetch_scan(
        &self,
        measurement: &str,
        matchers: &[LabelMatcher],
        start: i64,
        end: i64,
    ) -> Result<Arc<Vec<RecordBatch>>, EvalError> {
        let mut tags: Vec<(String, String)> = matchers
            .iter()
            .filter(|m| {
                m.name != "__name__"
                    && m.name != chronix_core::NAMESPACE_TAG
                    && m.op == MatchOp::Equal
            })
            .map(|m| (m.name.clone(), m.value.clone()))
            .collect();
        // The namespace scope is part of the cache key as well as the scan:
        // two tenants asking for the same window must not share an entry.
        if let Some(ref ns) = self.namespace {
            tags.push((chronix_core::NAMESPACE_TAG.to_string(), ns.clone()));
        }
        tags.sort();

        let key = ScanKey {
            measurement: measurement.to_string(),
            tags,
            start,
            end,
        };
        if let Some(hit) = self.scan_cache.borrow().get(&key) {
            let mut stats = self.scan_stats.get();
            stats.cache_hits += 1;
            self.scan_stats.set(stats);
            return Ok(Arc::clone(hit));
        }
        let mut stats = self.scan_stats.get();
        stats.scans += 1;
        self.scan_stats.set(stats);

        let mut builder = self.db.query().measurement(measurement);
        for (k, v) in &key.tags {
            builder = builder.tag(k, v);
        }
        let plan = builder
            .range(start, end)
            .build()
            .map_err(|e| EvalError(e.to_string()))?;
        let batches = Arc::new(
            self.db
                .execute_stream(&plan)
                .map_err(|e| EvalError(e.to_string()))?,
        );

        let mut cache = self.scan_cache.borrow_mut();
        if cache.len() < SCAN_CACHE_CAPACITY {
            cache.insert(key, Arc::clone(&batches));
        }
        Ok(batches)
    }

    pub(crate) fn eval_vector_selector(
        &self,
        name: &Option<String>,
        matchers: &[LabelMatcher],
        offset: &Option<Duration>,
        params: &QueryParams,
    ) -> Result<PromQLValue, EvalError> {
        // Check for __name__ regex/not-equal matchers — they require
        // iterating over all known measurements.
        let name_regex_matcher = matchers.iter().find(|m| {
            m.name == "__name__"
                && matches!(
                    m.op,
                    MatchOp::RegexMatch | MatchOp::RegexNotMatch | MatchOp::NotEqual
                )
        });

        if let (None, Some(matcher)) = (name.as_ref(), name_regex_matcher) {
            // Multi-measurement query: resolve matching measurements
            // and union the results.
            let all_measurements = self.db.schema_registry().measurement_names();
            let name_filter = CompiledMatcher::compile(matcher)
                .map_err(|e| EvalError(format!("invalid __name__ matcher: {e}")))?;

            let mut all_series = Vec::new();
            for measurement in &all_measurements {
                let fake_labels = vec![("__name__".to_string(), measurement.clone())];
                if !name_filter.matches(&fake_labels) {
                    continue;
                }
                // Evaluate with this specific measurement
                let sub_name = Some(measurement.clone());
                if let PromQLValue::Vector(series) =
                    self.eval_vector_selector(&sub_name, matchers, offset, params)?
                {
                    all_series.extend(series);
                }
            }
            return Ok(PromQLValue::Vector(all_series));
        }

        let measurement = resolve_measurement(name, matchers, "vector selector")?;
        let offset_ns = offset.map_or(0, |d| d.as_nanos());
        let eval_time = params.time - offset_ns;
        let lookback_start = eval_time - params.lookback_delta;

        let (fetch_start, fetch_end) = fetch_window(
            params,
            offset_ns,
            params.lookback_delta,
            lookback_start,
            eval_time,
        );
        let batches = self.fetch_scan(measurement, matchers, fetch_start, fetch_end)?;
        let post_filters = compile_post_filters(matchers)?;
        let series_map = collect_series(
            &batches,
            measurement,
            &post_filters,
            lookback_start,
            eval_time,
        )?;

        // An instant vector is the newest sample in the lookback window.
        let result: Vec<Series> = series_map
            .into_iter()
            .filter_map(|(labels, samples)| {
                let latest = samples.last().copied()?;
                Some(Series {
                    labels,
                    samples: vec![latest],
                })
            })
            .collect();

        Ok(PromQLValue::Vector(result))
    }

    #[allow(clippy::trivially_copy_pass_by_ref)] // &Duration keeps call sites uniform
    pub(crate) fn eval_matrix_selector(
        &self,
        vector: &Expr,
        range: &Duration,
        params: &QueryParams,
    ) -> Result<PromQLValue, EvalError> {
        let Expr::VectorSelector {
            name,
            matchers,
            offset,
        } = vector
        else {
            return Err(EvalError("matrix selector requires vector selector".into()));
        };

        let measurement = resolve_measurement(name, matchers, "matrix selector")?;
        let offset_ns = offset.map_or(0, |d| d.as_nanos());
        let eval_time = params.time - offset_ns;
        let range_start = eval_time - range.as_nanos();

        let (fetch_start, fetch_end) =
            fetch_window(params, offset_ns, range.as_nanos(), range_start, eval_time);
        let batches = self.fetch_scan(measurement, matchers, fetch_start, fetch_end)?;
        let post_filters = compile_post_filters(matchers)?;
        let series_map =
            collect_series(&batches, measurement, &post_filters, range_start, eval_time)?;

        let result: Vec<Series> = series_map
            .into_iter()
            .map(|(labels, samples)| Series { labels, samples })
            .collect();

        Ok(PromQLValue::Matrix(result))
    }
}

// ── Helper functions ───────────────────────────────────────────────────

/// The measurement a selector reads, from either the bare name or a
/// `__name__="…"` matcher.
fn resolve_measurement<'a>(
    name: &'a Option<String>,
    matchers: &'a [LabelMatcher],
    what: &str,
) -> Result<&'a str, EvalError> {
    name.as_deref()
        .or_else(|| {
            matchers
                .iter()
                .find(|m| m.name == "__name__" && m.op == MatchOp::Equal)
                .map(|m| m.value.as_str())
        })
        .ok_or_else(|| EvalError(format!("{what} requires metric name")))
}

/// The window actually read from storage.
///
/// Inside a range query this is the whole query window, read once and cached;
/// otherwise it is just the window this step needs. Either way it is a
/// superset of `(window_start, eval_time]`, and [`collect_series`] narrows it.
///
/// `reach_ns` is how far back this selector looks from its evaluation
/// instant — the lookback delta for an instant vector, the range for a matrix
/// selector. It has to be part of the widening rather than applied
/// afterwards, because **the window must be identical at every step or the
/// cache never hits**: taking the widened window and then intersecting it with
/// the step's own `[t − range, t]` gives a different lower bound at each step,
/// which is a cache miss per step and the full-range read back again for any
/// range longer than the lookback.
fn fetch_window(
    params: &QueryParams,
    offset_ns: i64,
    reach_ns: i64,
    window_start: i64,
    eval_time: i64,
) -> (i64, i64) {
    match (params.range_fetch_start, params.range_fetch_end) {
        // `range_fetch_start` is `query_start - lookback_delta`; recover the
        // query start so the selector's own reach can be applied instead.
        (Some(rs), Some(re)) => {
            let query_start = rs.saturating_add(params.lookback_delta);
            (
                query_start
                    .saturating_sub(reach_ns)
                    .saturating_sub(offset_ns),
                re.saturating_sub(offset_ns).saturating_add(1),
            )
        }
        _ => (window_start, eval_time + 1),
    }
}

/// Turn scan batches into per-series samples inside `(window_start, window_end]`.
///
/// The window is **left-open**: a sample landing exactly on the older
/// boundary belongs to the previous window, so that evenly spaced samples
/// produce a constant count per range (Prometheus 3.0).
fn collect_series(
    batches: &[RecordBatch],
    measurement: &str,
    post_filters: &[CompiledMatcher],
    window_start: i64,
    window_end: i64,
) -> Result<SeriesMap, EvalError> {
    let mut series_map: SeriesMap = BTreeMap::new();

    for batch in batches {
        let schema = batch.schema();
        let num_rows = batch.num_rows();

        let ts_col_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "timestamp" || f.name() == "_time" || f.name() == "time")
            .ok_or_else(|| EvalError("no timestamp column".into()))?;

        let mut tag_cols = Vec::new();
        let mut field_cols = Vec::new();
        for (idx, field) in schema.fields().iter().enumerate() {
            if idx == ts_col_idx {
                continue;
            }
            // The namespace marker is internal: it must not become a label,
            // so it can neither be seen by a client nor matched on. A
            // `__namespace__=…` matcher therefore falls through to the post
            // filters and matches nothing, which is the fail-closed direction.
            if field.name() == chronix_core::NAMESPACE_TAG {
                continue;
            }
            if matches!(field.data_type(), arrow::datatypes::DataType::Utf8) {
                tag_cols.push((idx, field.name().clone()));
            } else {
                field_cols.push((idx, field.name().clone()));
            }
        }

        let ts_values = batch
            .column(ts_col_idx)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| EvalError("timestamp column is not Int64".into()))?;

        for row in 0..num_rows {
            let timestamp = ts_values.value(row);
            if timestamp <= window_start || timestamp > window_end {
                continue;
            }

            let mut labels: Vec<(String, String)> =
                vec![("__name__".to_string(), measurement.to_string())];
            for (idx, name) in &tag_cols {
                if let Some(arr) = batch
                    .column(*idx)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                {
                    if !arrow::array::Array::is_null(arr, row) {
                        labels.push((name.clone(), arr.value(row).to_string()));
                    }
                }
            }
            labels.sort();

            if !post_filters.iter().all(|f| f.matches(&labels)) {
                continue;
            }

            for (idx, field_name) in &field_cols {
                let col = batch.column(*idx);
                if arrow::array::Array::is_null(col.as_ref(), row) {
                    continue;
                }
                let value = extract_f64(col, row)?;
                let mut field_labels = labels.clone();
                if field_cols.len() > 1 {
                    if let Some(entry) = field_labels.iter_mut().find(|(k, _)| k == "__name__") {
                        entry.1 = format!("{measurement}_{field_name}");
                    }
                }
                series_map
                    .entry(field_labels)
                    .or_default()
                    .push(Sample { timestamp, value });
            }
        }
    }

    for samples in series_map.values_mut() {
        samples.sort_by_key(|s| s.timestamp);
    }
    Ok(series_map)
}

pub(crate) fn extract_f64(col: &arrow::array::ArrayRef, row: usize) -> Result<f64, EvalError> {
    if let Some(a) = col.as_any().downcast_ref::<arrow::array::Float64Array>() {
        Ok(a.value(row))
    } else if let Some(a) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
        Ok(a.value(row) as f64)
    } else if let Some(a) = col.as_any().downcast_ref::<arrow::array::UInt64Array>() {
        Ok(a.value(row) as f64)
    } else if let Some(a) = col.as_any().downcast_ref::<arrow::array::Float32Array>() {
        Ok(f64::from(a.value(row)))
    } else if let Some(a) = col.as_any().downcast_ref::<arrow::array::BooleanArray>() {
        Ok(if a.value(row) { 1.0 } else { 0.0 })
    } else {
        Err(EvalError(format!(
            "cannot extract f64 from {:?}",
            col.data_type()
        )))
    }
}
