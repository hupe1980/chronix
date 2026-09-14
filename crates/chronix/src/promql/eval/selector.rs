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
use crate::promql::metric::{self, ClassicView, MetricRef};

use super::{CompiledMatcher, EvalError, PromQLEvaluator, QueryParams, compile_post_filters};
use super::{SCAN_CACHE_CAPACITY, ScanKey};

/// Samples grouped by label set, ascending by timestamp within each group.
type SeriesMap = BTreeMap<Vec<(String, String)>, SeriesSamples>;

/// What one series accumulated during a scan.
///
/// Two lists rather than one, matching [`Series`] — see its documentation for
/// why floats and histograms are kept apart. In practice exactly one of these
/// is non-empty for a given series: a column is `Binary` (a histogram) or it
/// is numeric, and that is a property of the schema rather than of a row.
#[derive(Default)]
pub(crate) struct SeriesSamples {
    floats: Vec<Sample>,
    histograms: Vec<crate::promql::ast::HistogramSample>,
}

impl SeriesSamples {
    /// The newest sample, as a one-sample series — what an instant vector is.
    fn latest(&self, labels: Vec<(String, String)>) -> Option<Series> {
        if let Some(h) = self.histograms.last() {
            return Some(Series::histograms(labels, vec![h.clone()]));
        }
        let latest = *self.floats.last()?;
        Some(Series::floats(labels, vec![latest]))
    }

    /// Everything, as a range-vector series.
    fn all(self, labels: Vec<(String, String)>) -> Series {
        if self.histograms.is_empty() {
            Series::floats(labels, self.floats)
        } else {
            Series::histograms(labels, self.histograms)
        }
    }

    fn sort(&mut self) {
        self.floats.sort_by_key(|s| s.timestamp);
        self.histograms.sort_by_key(|s| s.timestamp);
    }
}

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
        deadline: Option<super::Deadline>,
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

        // A cache miss means segment I/O — the other unit of work an
        // evaluation is made of, and the one a single-step instant query is
        // entirely made of.
        if let Some(deadline) = deadline {
            deadline.check()?;
        }

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

        // The registry still lists a measurement pending a soft-delete;
        // `db.schema` is the one place that answers "gone or not" and every
        // discovery surface has to agree with it, or a metric a query can
        // no longer read stays offered by the browser that found it.
        candidates.retain(|m| self.db.schema(&m.measurement).is_some());

        if !name_matchers.is_empty() {
            candidates.retain(|m| {
                let labels = [("__name__".to_string(), m.name.clone())];
                name_matchers.iter().all(|f| f.matches(&labels))
            });
        }
        Ok(candidates)
    }

    /// Read one metric's samples over `(window_start, window_end]`.
    ///
    /// The `le` matcher of a classic bucket selector is the one matcher that
    /// cannot be pushed into the scan. `le` is not a stored tag — the bucket
    /// view invents it from the histogram's boundaries — so a scan filtered on
    /// `le="0.5"` matches no rows at all, and `foo_bucket{le="0.5"}` answered
    /// nothing while bare `foo_bucket` answered every boundary. It is moved to
    /// the post filters, where the label exists by the time it is tested.
    fn read_metric(
        &self,
        target: &MetricRef,
        matchers: &[LabelMatcher],
        post_filters: &[CompiledMatcher],
        fetch: (i64, i64),
        window: (i64, i64),
        deadline: Option<super::Deadline>,
    ) -> Result<SeriesMap, EvalError> {
        if target.view == ClassicView::Bucket && matchers.iter().any(is_le_equality) {
            let scan_matchers: Vec<LabelMatcher> = matchers
                .iter()
                .filter(|m| !is_le_equality(m))
                .cloned()
                .collect();
            // Recompiled from the selector rather than cloned onto the end
            // of `post_filters`: this is `compile_post_filters` plus the `le`
            // equalities, and building it from one source keeps the two sets
            // from drifting apart.
            let filters: Vec<CompiledMatcher> = matchers
                .iter()
                .filter(|m| m.name != "__name__" && (m.op != MatchOp::Equal || is_le_equality(m)))
                .map(CompiledMatcher::compile)
                .collect::<Result<_, _>>()?;
            let batches = self.fetch_scan(
                &target.measurement,
                &scan_matchers,
                fetch.0,
                fetch.1,
                deadline,
            )?;
            return collect_series(&batches, target, &filters, window.0, window.1);
        }
        let batches = self.fetch_scan(&target.measurement, matchers, fetch.0, fetch.1, deadline)?;
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
                params.deadline,
            )?;
            // An instant vector is the newest sample in the lookback window.
            result.extend(
                series_map
                    .into_iter()
                    .filter_map(|(labels, samples)| samples.latest(labels)),
            );
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
                params.deadline,
            )?;
            result.extend(
                series_map
                    .into_iter()
                    .map(|(labels, samples)| samples.all(labels)),
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
                    && !arrow::array::Array::is_null(arr, row)
                {
                    labels.push((name.clone(), arr.value(row).to_string()));
                }
            }
            labels.sort();

            // A histogram column is `Binary`: the sample *is* a distribution,
            // so it goes in the other list rather than through `extract_f64`,
            // which has no float to extract.
            if let Some(bin) = value_col
                .as_any()
                .downcast_ref::<arrow::array::BinaryArray>()
            {
                // A blob this build cannot decode is skipped rather than
                // failing the whole query: one unreadable sample must not
                // take a dashboard down, and the gap is visible as a
                // missing point.
                let Ok(h) =
                    postcard::from_bytes::<chronix_core::histogram::Histogram>(bin.value(row))
                else {
                    continue;
                };
                match target.view {
                    ClassicView::Native => {
                        if !post_filters.iter().all(|f| f.matches(&labels)) {
                            continue;
                        }
                        series_map.entry(labels).or_default().histograms.push(
                            crate::promql::ast::HistogramSample {
                                timestamp,
                                histogram: Box::new(h),
                            },
                        );
                    }
                    ClassicView::Count => {
                        push_float(&mut series_map, labels, post_filters, timestamp, h.count);
                    }
                    ClassicView::Sum => {
                        push_float(&mut series_map, labels, post_filters, timestamp, h.sum);
                    }
                    ClassicView::Bucket => {
                        // One series per boundary, carrying a **cumulative**
                        // count — which is what `foo_bucket` has always meant
                        // and what `histogram_quantile`'s classic path reads.
                        for (le, cumulative) in h.classic_buckets() {
                            let mut bucket_labels = labels.clone();
                            // The view's `le` wins over a tag of the same
                            // name: two labels called `le` is not a label set,
                            // and the boundary is the one this series is
                            // about.
                            bucket_labels.retain(|(k, _)| k != LE_LABEL);
                            bucket_labels.push((LE_LABEL.to_string(), format_le(le)));
                            bucket_labels.sort();
                            push_float(
                                &mut series_map,
                                bucket_labels,
                                post_filters,
                                timestamp,
                                cumulative,
                            );
                        }
                    }
                }
            } else {
                // A classic view of a column that is not a histogram reads
                // nothing. `metric::resolve` does not produce one, so this is
                // a column whose type changed under a resolved selector.
                if target.view != ClassicView::Native {
                    continue;
                }
                if !post_filters.iter().all(|f| f.matches(&labels)) {
                    continue;
                }
                series_map.entry(labels).or_default().floats.push(Sample {
                    timestamp,
                    value: extract_f64(value_col, row)?,
                });
            }
        }
    }

    for samples in series_map.values_mut() {
        samples.sort();
    }
    Ok(series_map)
}

/// The label a classic bucket series carries its upper bound in.
const LE_LABEL: &str = "le";

/// Is this an `le="…"` equality — the matcher the scan cannot push down?
fn is_le_equality(m: &LabelMatcher) -> bool {
    m.name == LE_LABEL && m.op == MatchOp::Equal
}

/// Record one float sample under `labels`, if the label set survives filtering.
///
/// The filters are applied **here** rather than before the histogram is
/// decoded, because the bucket view adds `le` to the label set: a
/// `foo_bucket{le="0.5"}` selector filtered before the label existed would see
/// an absent `le`, which reads as the empty string, and match nothing.
fn push_float(
    series_map: &mut SeriesMap,
    labels: Vec<(String, String)>,
    post_filters: &[CompiledMatcher],
    timestamp: i64,
    value: f64,
) {
    if !post_filters.iter().all(|f| f.matches(&labels)) {
        return;
    }
    series_map
        .entry(labels)
        .or_default()
        .floats
        .push(Sample { timestamp, value });
}

/// A bucket boundary as Prometheus writes it in an `le` label.
///
/// `+Inf` rather than Rust's `inf`, because that is the literal a dashboard
/// written years ago has in its query and the one `histogram_quantile`'s
/// classic path looks for.
fn format_le(le: f64) -> String {
    if le.is_infinite() {
        if le > 0.0 { "+Inf" } else { "-Inf" }.to_string()
    } else {
        le.to_string()
    }
}

/// One sample's value, as the `f64` PromQL is defined over.
///
/// PromQL has one numeric type, so a `Decimal` column selected here is
/// converted — and converted through the workspace's single conversion
/// function, so the boundary is the one named in
/// [`chronix_query::extract_f64`] rather than another copy of it. A register
/// on a dashboard is exactly the case where that is the right answer; a
/// register in a settlement is not, and SQL is where that query belongs.
///
/// The caller has already skipped null rows, so a `None` from the shared
/// function here means an unsupported column type.
pub(crate) fn extract_f64(col: &arrow::array::ArrayRef, row: usize) -> Result<f64, EvalError> {
    // Two types PromQL accepts that the storage layer never produces, so
    // they are not in the shared extractor: a 32-bit float and a boolean.
    if let Some(a) = col.as_any().downcast_ref::<arrow::array::Float32Array>() {
        return Ok(f64::from(a.value(row)));
    }
    if let Some(a) = col.as_any().downcast_ref::<arrow::array::BooleanArray>() {
        return Ok(if a.value(row) { 1.0 } else { 0.0 });
    }
    chronix_query::extract_f64(col.as_ref(), row)
        .ok_or_else(|| EvalError(format!("cannot extract f64 from {:?}", col.data_type())))
}
