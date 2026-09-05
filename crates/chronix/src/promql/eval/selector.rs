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
//! # One metric, one field
//!
//! A selector names a *metric*, and a metric is one `(measurement, field)`
//! pair — [`crate::promql::metric`] owns that mapping and is the only place
//! that knows it. A selector that names nothing storable resolves to no
//! targets and evaluates to an empty vector, which is what PromQL expects of
//! an unknown metric.
//!
//! The scheme this replaced named a series after its *measurement* when the
//! measurement held one field and after `measurement_field` when it held
//! more. Three things followed, and all three were wrong: writing a second
//! field renamed the first one's whole history; the `measurement_field` names
//! a query returned could not be typed back in, because a `__name__` matcher
//! only ever resolved a measurement; and a bare measurement selector produced
//! one series per field with identical labels, so `sum(cpu)` added a
//! percentage to a load average and `rate(cpu[5m])` returned a vector holding
//! duplicate label sets — which PromQL cannot represent.
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

use crate::promql::ast::{
    AtModifier, Duration, Expr, LabelMatcher, MatchOp, PromQLValue, Sample, Series,
};
use crate::promql::metric::{self, MetricRef};

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

    /// The metrics a selector reads.
    ///
    /// One resolver for every `__name__` shape — a bare name, an equality
    /// matcher, a regex, a negation — because the discovery endpoints answer
    /// the same syntax and the two disagreeing is what let
    /// `/series?match[]={__name__=~".+"}` return nothing while `/query` with
    /// that selector returned every series.
    fn resolve_targets(
        &self,
        name: &Option<String>,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<MetricRef>, EvalError> {
        let registry = self.db.schema_registry();

        let named = name.as_deref().or_else(|| {
            matchers
                .iter()
                .find(|m| m.name == "__name__" && m.op == MatchOp::Equal)
                .map(|m| m.value.as_str())
        });

        let name_matchers: Vec<_> = matchers
            .iter()
            .filter(|m| m.name == "__name__" && m.op != MatchOp::Equal)
            .map(CompiledMatcher::compile)
            .collect::<Result<_, _>>()?;

        // A name-less selector reaches every metric. The parser has already
        // refused the shape that means "the whole database" — one whose every
        // matcher is satisfied by an absent label — which is Prometheus's rule
        // and the reason `{host="a"}` is legal here.
        let mut candidates = match named {
            Some(n) => metric::resolve(registry, n),
            None => metric::all_metrics(registry),
        };

        if !name_matchers.is_empty() {
            candidates.retain(|m| {
                let labels = [("__name__".to_string(), m.name.clone())];
                name_matchers.iter().all(|f| f.matches(&labels))
            });
        }
        Ok(candidates)
    }

    /// Read one metric's samples over `(window_start, window_end]`.
    fn read_metric(
        &self,
        target: &MetricRef,
        matchers: &[LabelMatcher],
        post_filters: &[CompiledMatcher],
        fetch: (i64, i64),
        window: (i64, i64),
    ) -> Result<SeriesMap, EvalError> {
        let batches = self.fetch_scan(&target.measurement, matchers, fetch.0, fetch.1)?;
        collect_series(&batches, target, post_filters, window.0, window.1)
    }

    pub(crate) fn eval_vector_selector(
        &self,
        name: &Option<String>,
        matchers: &[LabelMatcher],
        offset: &Option<Duration>,
        at: Option<AtModifier>,
        params: &QueryParams,
    ) -> Result<PromQLValue, EvalError> {
        let targets = self.resolve_targets(name, matchers)?;
        let offset_ns = offset.map_or(0, |d| d.as_nanos());
        let eval_time = pinned_eval_time(params, at).saturating_sub(offset_ns);
        let lookback_start = eval_time - params.lookback_delta;

        let fetch = fetch_window(
            params,
            offset_ns,
            params.lookback_delta,
            lookback_start,
            eval_time,
            at.is_some(),
        );
        let post_filters = compile_post_filters(matchers)?;

        let mut result = Vec::new();
        for target in &targets {
            let series_map = self.read_metric(
                target,
                matchers,
                &post_filters,
                fetch,
                (lookback_start, eval_time),
            )?;
            // An instant vector is the newest sample in the lookback window.
            result.extend(series_map.into_iter().filter_map(|(labels, samples)| {
                let latest = samples.last().copied()?;
                Some(Series {
                    labels,
                    samples: vec![latest],
                })
            }));
        }

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
            at,
        } = vector
        else {
            return Err(EvalError("matrix selector requires vector selector".into()));
        };

        let targets = self.resolve_targets(name, matchers)?;
        let offset_ns = offset.map_or(0, |d| d.as_nanos());
        let eval_time = pinned_eval_time(params, *at).saturating_sub(offset_ns);
        let range_start = eval_time - range.as_nanos();

        let fetch = fetch_window(
            params,
            offset_ns,
            range.as_nanos(),
            range_start,
            eval_time,
            at.is_some(),
        );
        let post_filters = compile_post_filters(matchers)?;

        let mut result = Vec::new();
        for target in &targets {
            let series_map = self.read_metric(
                target,
                matchers,
                &post_filters,
                fetch,
                (range_start, eval_time),
            )?;
            result.extend(
                series_map
                    .into_iter()
                    .map(|(labels, samples)| Series { labels, samples }),
            );
        }

        Ok(PromQLValue::Matrix(result))
    }
}

// ── Helper functions ───────────────────────────────────────────────────

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
    pinned: bool,
) -> (i64, i64) {
    // A selector pinned with `@` reads one fixed window whatever the step, and
    // that window is usually *outside* the range query's prefetch span — so
    // widening to the prefetch window would miss the data entirely. Its own
    // window is also the cheapest scan available, since it is read once.
    if pinned {
        return (window_start, eval_time.saturating_add(1));
    }
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

/// The instant a selector evaluates at, before its offset is applied.
///
/// `@` replaces the evaluation time outright; `@ start()` and `@ end()` take
/// the range query's own bounds, which for an instant query are both the
/// evaluation time — the same resolution Prometheus's engine makes.
fn pinned_eval_time(params: &QueryParams, at: Option<AtModifier>) -> i64 {
    match at {
        None => params.time,
        Some(at) => at.resolve(
            params.start.unwrap_or(params.time),
            params.end.unwrap_or(params.time),
        ),
    }
}

/// Turn scan batches into per-series samples inside `(window_start, window_end]`.
///
/// Reads exactly one field — `target.field` — and labels every series with
/// `target.name`. A batch that does not carry the field contributes nothing:
/// a measurement gains columns over its life, and a segment written before
/// the field existed has no value to report, not a zero.
///
/// The window is **left-open**: a sample landing exactly on the older
/// boundary belongs to the previous window, so that evenly spaced samples
/// produce a constant count per range (Prometheus 3.0).
fn collect_series(
    batches: &[RecordBatch],
    target: &MetricRef,
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
            .position(|f| f.name() == chronix_core::TIME_COLUMN)
            .ok_or_else(|| EvalError("no timestamp column".into()))?;

        let Some(value_col_idx) = schema
            .fields()
            .iter()
            .position(|f| *f.name() == target.field)
        else {
            continue;
        };

        let mut tag_cols = Vec::new();
        for (idx, field) in schema.fields().iter().enumerate() {
            if idx == ts_col_idx || idx == value_col_idx {
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
            }
        }

        let ts_values = batch
            .column(ts_col_idx)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| EvalError("timestamp column is not Int64".into()))?;
        let value_col = batch.column(value_col_idx);

        for row in 0..num_rows {
            let timestamp = ts_values.value(row);
            if timestamp <= window_start || timestamp > window_end {
                continue;
            }
            if arrow::array::Array::is_null(value_col.as_ref(), row) {
                continue;
            }

            let mut labels: Vec<(String, String)> =
                vec![("__name__".to_string(), target.name.clone())];
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

            let value = extract_f64(value_col, row)?;
            series_map
                .entry(labels)
                .or_default()
                .push(Sample { timestamp, value });
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
