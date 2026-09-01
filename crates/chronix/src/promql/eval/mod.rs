//! PromQL evaluator — evaluates a parsed AST against Chronix data.
//!
//! The evaluator is split into focused submodules:
//! - `selector` — vector and matrix selector evaluation
//! - `function` — built-in PromQL function dispatch
//! - `aggregation` — aggregation operators (sum, avg, topk, …)
//! - `binary` — binary expression evaluation and set operations

mod aggregation;
mod binary;
pub(crate) mod function;
mod selector;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use regex::Regex;

use crate::promql::ast::{
    Duration, Expr, LabelMatcher, MatchOp, PromQLValue, Sample, Series, UnaryOp,
};
use arrow::record_batch::RecordBatch;

use crate::Chronix;

// ── Types ──────────────────────────────────────────────────────────────

/// Label-set key → (le, count) bucket pairs, used by `histogram_quantile`.
pub(crate) type HistogramBuckets = BTreeMap<Vec<(String, String)>, Vec<(f64, f64)>>;

/// One label matcher, with its regex compiled once.
///
/// Public because a selector's matchers have to be applied in two places — the
/// evaluator, and the server's `/series` and `/label/…/values` endpoints, which
/// answer "which label sets exist?" for the same selector syntax. One
/// implementation, so the two cannot disagree.
pub struct CompiledMatcher {
    pub(crate) name: String,
    pub(crate) op: MatchOp,
    pub(crate) value: String,
    pub(crate) regex: Option<Regex>,
}

impl CompiledMatcher {
    /// Compile one matcher, anchoring and flag-setting any regex the way
    /// Prometheus 3.x does.
    ///
    /// # Errors
    ///
    /// [`EvalError`] if the pattern is not a valid regular expression.
    pub fn compile(m: &LabelMatcher) -> Result<Self, EvalError> {
        let regex = match m.op {
            MatchOp::RegexMatch | MatchOp::RegexNotMatch => {
                // Fully anchored, and `.` matches newline: Prometheus 3.0
                // switched RE2 to the `s` flag, so `{job=~".+"}` selects a
                // value containing a newline the way the server does.
                let pattern = format!("^(?s:{})$", m.value);
                Some(
                    Regex::new(&pattern)
                        .map_err(|e| EvalError(format!("invalid regex '{}': {e}", m.value)))?,
                )
            }
            _ => None,
        };
        Ok(Self {
            name: m.name.clone(),
            op: m.op,
            value: m.value.clone(),
            regex,
        })
    }

    /// Whether the given label set satisfies this matcher.
    ///
    /// An absent label is treated as the empty string, which is what makes
    /// `{foo!="bar"}` select series that carry no `foo` at all — the
    /// Prometheus rule, and the one that is easy to get backwards.
    #[must_use]
    pub fn matches(&self, labels: &[(String, String)]) -> bool {
        let label_value = labels
            .iter()
            .find(|(k, _)| k == &self.name)
            .map(|(_, v)| v.as_str())
            .unwrap_or("");

        match self.op {
            MatchOp::Equal => label_value == self.value,
            MatchOp::NotEqual => label_value != self.value,
            MatchOp::RegexMatch => self
                .regex
                .as_ref()
                .is_some_and(|re| re.is_match(label_value)),
            MatchOp::RegexNotMatch => self
                .regex
                .as_ref()
                .is_none_or(|re| !re.is_match(label_value)),
        }
    }
}

/// Compile post-filters from matchers that aren't simple equality (which
/// are pushed down into the Chronix query engine).
pub(crate) fn compile_post_filters(
    matchers: &[LabelMatcher],
) -> Result<Vec<CompiledMatcher>, EvalError> {
    matchers
        .iter()
        .filter(|m| m.name != "__name__" && m.op != MatchOp::Equal)
        .map(CompiledMatcher::compile)
        .collect()
}

/// Every matcher of a selector except `__name__`, compiled so a label set can
/// be tested against the selector as a whole.
///
/// The `__name__` matcher is excluded because it selects the *measurement*,
/// which callers resolve before they have any label sets to test.
///
/// # Errors
///
/// [`EvalError`] if any pattern is not a valid regular expression.
pub fn compile_label_matchers(
    matchers: &[LabelMatcher],
) -> Result<Vec<CompiledMatcher>, EvalError> {
    matchers
        .iter()
        .filter(|m| m.name != "__name__")
        .map(CompiledMatcher::compile)
        .collect()
}

/// Whether a label set satisfies every matcher in `compiled`.
#[must_use]
pub fn label_set_matches(compiled: &[CompiledMatcher], labels: &[(String, String)]) -> bool {
    compiled.iter().all(|m| m.matches(labels))
}

// ── Core types ─────────────────────────────────────────────────────────

/// Identifies one raw selector fetch: a measurement, the equality tag
/// filters pushed into the scan, and the window actually read.
///
/// A range query evaluates the same selector once per step over the *same*
/// widened window, so this is what makes the fetch happen once instead of
/// once per step.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct ScanKey {
    pub(crate) measurement: String,
    pub(crate) tags: Vec<(String, String)>,
    pub(crate) start: i64,
    pub(crate) end: i64,
}

/// Distinct selector windows an evaluator will hold batches for.
///
/// The cache trades memory for reads: each entry holds one selector's whole
/// query window, which `execute_stream` already bounds by
/// `max_query_result_bytes`, so the cap is what bounds the *number* of such
/// windows alive at once. Eight covers every query shape anybody writes by
/// hand — a panel expression has one to three selectors — and a query with
/// more simply stops caching and re-reads, which is what the code did before
/// the cache existed. Growing instead would turn a pathological query into an
/// out-of-memory condition, which is a worse failure than a slow one.
const SCAN_CACHE_CAPACITY: usize = 8;

/// How much storage work one evaluator actually did.
///
/// Exposed because the claim "a range query reads its window once" is a claim
/// about behaviour, and a claim in a comment is a claim to be tested (R3). The
/// comment was there for several releases while the code did the opposite.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanStats {
    /// Selector evaluations that read from storage.
    pub scans: u64,
    /// Selector evaluations served from an earlier read of the same window.
    pub cache_hits: u64,
}

/// PromQL evaluator backed by a Chronix database.
///
/// One evaluator serves one query. Both caches are therefore query-scoped:
/// they are what make a range query read its window once rather than once per
/// step, and they are dropped with the query.
pub struct PromQLEvaluator {
    pub(crate) db: Arc<Chronix>,
    /// Namespace every selector is scoped to, if any.
    ///
    /// Set by the server from the request's namespace. Scoping the
    /// *evaluator* rather than rewriting the parsed query means every
    /// selector — including the ones a `__name__` regex expands to — is
    /// scoped by construction, and the marker tag never reaches a label set.
    pub(crate) namespace: Option<String>,
    /// Compiled regexes used by `label_replace()`, so a range query does not
    /// recompile the same pattern at every step.
    pub(crate) regex_cache: std::cell::RefCell<HashMap<String, regex::Regex>>,
    /// Raw scan results keyed by the window they cover.
    pub(crate) scan_cache: std::cell::RefCell<HashMap<ScanKey, Arc<Vec<RecordBatch>>>>,
    pub(crate) scan_stats: std::cell::Cell<ScanStats>,
}

/// Query parameters for PromQL evaluation.
#[derive(Debug, Clone)]
pub struct QueryParams {
    /// Evaluation timestamp in nanoseconds (for instant queries).
    pub time: i64,
    /// Start of evaluation window in nanoseconds (for range queries).
    pub start: Option<i64>,
    /// End of evaluation window in nanoseconds (for range queries).
    pub end: Option<i64>,
    /// Step interval in nanoseconds (for range queries).
    pub step: Option<i64>,
    /// Default lookback delta in nanoseconds (default: 5m).
    pub lookback_delta: i64,
    /// When set, selectors read the whole `[range_fetch_start,
    /// range_fetch_end]` window once and serve every step from that one
    /// read, instead of issuing a narrow scan per step.
    ///
    /// Widening the window without caching it is strictly worse than not
    /// widening it — it turns O(steps) narrow scans into O(steps) *full-range*
    /// scans — so the two fields are only meaningful together with the
    /// evaluator's scan cache.
    pub range_fetch_start: Option<i64>,
    /// End of the prefetch window (inclusive), set by `range_query()`.
    pub range_fetch_end: Option<i64>,
    /// Maximum number of distinct series in a range query result (0 = unlimited).
    pub max_series: usize,
    /// Maximum bytes the range-query accumulator may use (0 = unlimited).
    pub max_memory_bytes: usize,
}

impl Default for QueryParams {
    fn default() -> Self {
        Self {
            time: 0,
            start: None,
            end: None,
            step: None,
            lookback_delta: 300_000_000_000, // 5 minutes in nanoseconds
            range_fetch_start: None,
            range_fetch_end: None,
            max_series: 0,
            max_memory_bytes: 0,
        }
    }
}

/// Evaluation error.
#[derive(Debug)]
pub struct EvalError(pub String);

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PromQL evaluation error: {}", self.0)
    }
}

impl std::error::Error for EvalError {}

// ── PromQLEvaluator core methods ───────────────────────────────────────

impl PromQLEvaluator {
    /// Create a new evaluator backed by the given database.
    pub fn new(db: Arc<Chronix>) -> Self {
        Self {
            db,
            namespace: None,
            regex_cache: std::cell::RefCell::new(HashMap::new()),
            scan_cache: std::cell::RefCell::new(HashMap::new()),
            scan_stats: std::cell::Cell::new(ScanStats::default()),
        }
    }

    /// Scope every selector this evaluator reads to `namespace`.
    ///
    /// `None` means no tenancy: the single-tenant server and the embedded
    /// API, where points carry no namespace tag.
    #[must_use]
    pub fn with_namespace(mut self, namespace: Option<String>) -> Self {
        self.namespace = namespace;
        self
    }

    /// Storage reads and cache hits this evaluator has accumulated.
    #[must_use]
    pub fn scan_stats(&self) -> ScanStats {
        self.scan_stats.get()
    }

    /// Evaluate an instant query at a single point in time.
    pub fn instant_query(
        &self,
        expr: &Expr,
        params: &QueryParams,
    ) -> Result<PromQLValue, EvalError> {
        self.eval(expr, params)
    }

    /// Evaluate a range query over a time window.
    ///
    /// Sets `range_fetch_start`/`range_fetch_end` on each step's
    /// `QueryParams`, so a selector reads `[start - lookback_delta, end]`
    /// once and every later step is served from the evaluator's scan cache:
    /// O(1) scans rather than O(steps).
    pub fn range_query(&self, expr: &Expr, params: &QueryParams) -> Result<PromQLValue, EvalError> {
        let start = params
            .start
            .ok_or_else(|| EvalError("range query requires start".into()))?;
        let end = params
            .end
            .ok_or_else(|| EvalError("range query requires end".into()))?;
        let step = params
            .step
            .ok_or_else(|| EvalError("range query requires step".into()))?;

        if step <= 0 {
            return Err(EvalError("step must be positive".into()));
        }

        // Collect results at each step
        let mut series_map: BTreeMap<Vec<(String, String)>, Vec<Sample>> = BTreeMap::new();
        let max_series = params.max_series;
        let max_memory = params.max_memory_bytes;
        // Track estimated bytes to bound memory usage.
        let mut estimated_bytes: usize = 0;

        let mut t = start;
        while t <= end {
            let step_params = QueryParams {
                time: t,
                step: params.step,
                lookback_delta: params.lookback_delta,
                range_fetch_start: Some(start - params.lookback_delta),
                range_fetch_end: Some(end),
                max_series,
                max_memory_bytes: max_memory,
                ..Default::default()
            };
            let result = self.eval(expr, &step_params)?;
            if let PromQLValue::Vector(series_list) = result {
                for series in series_list {
                    if max_series > 0
                        && !series_map.contains_key(&series.labels)
                        && series_map.len() >= max_series
                    {
                        return Err(EvalError(format!(
                            "query exceeded maximum series limit ({max_series})"
                        )));
                    }
                    for sample in &series.samples {
                        // The step timestamp, not the sample's own. A range
                        // query returns one point per step per series; a bare
                        // selector otherwise reports raw scrape times, so the
                        // matrix is not step-aligned and two steps that see
                        // the same newest sample emit it twice — a duplicate
                        // timestamp, which the HTTP API forbids and Grafana
                        // renders as a flat spot. The selector keeps the
                        // original timestamp internally so `timestamp()` can
                        // still read it; this is where it stops mattering.
                        series_map
                            .entry(series.labels.clone())
                            .or_default()
                            .push(Sample {
                                timestamp: t,
                                value: sample.value,
                            });
                        // Each Sample is 16 bytes (i64 + f64).
                        estimated_bytes += std::mem::size_of::<Sample>();
                    }
                    // Reject if accumulated memory exceeds budget.
                    if max_memory > 0 && estimated_bytes > max_memory {
                        return Err(EvalError(format!(
                            "range query exceeded memory limit ({estimated_bytes} bytes > {max_memory} bytes)"
                        )));
                    }
                }
            }
            t += step;
        }

        let matrix: Vec<Series> = series_map
            .into_iter()
            .map(|(labels, samples)| Series { labels, samples })
            .collect();

        Ok(PromQLValue::Matrix(matrix))
    }

    pub(crate) fn eval(&self, expr: &Expr, params: &QueryParams) -> Result<PromQLValue, EvalError> {
        match expr {
            Expr::NumberLiteral(n) => Ok(PromQLValue::Scalar(*n)),
            Expr::StringLiteral(s) => Ok(PromQLValue::String(s.clone())),
            Expr::Paren(inner) => self.eval(inner, params),
            Expr::UnaryExpr { op, expr } => {
                let val = self.eval(expr, params)?;
                match op {
                    UnaryOp::Neg => match val {
                        PromQLValue::Scalar(n) => Ok(PromQLValue::Scalar(-n)),
                        PromQLValue::Vector(series) => {
                            let negated = series
                                .into_iter()
                                .map(|mut s| {
                                    // Prometheus drops __name__ on negation.
                                    s.labels.retain(|(k, _)| k != "__name__");
                                    for sample in &mut s.samples {
                                        sample.value = -sample.value;
                                    }
                                    s
                                })
                                .collect();
                            Ok(PromQLValue::Vector(negated))
                        }
                        _ => Err(EvalError("cannot negate matrix or string".into())),
                    },
                }
            }
            Expr::VectorSelector {
                name,
                matchers,
                offset,
            } => self.eval_vector_selector(name, matchers, offset, params),

            Expr::MatrixSelector { vector, range } => {
                self.eval_matrix_selector(vector, range, params)
            }

            Expr::Call { func, args } => self.eval_function(func, args, params),

            Expr::Aggregation {
                op,
                expr,
                param,
                modifier,
            } => self.eval_aggregation(*op, expr, param.as_deref(), modifier, params),

            Expr::BinaryExpr {
                op,
                lhs,
                rhs,
                bool_mod,
                matching,
            } => self.eval_binary(*op, lhs, rhs, *bool_mod, matching.as_ref(), params),

            Expr::Subquery {
                expr: inner,
                range,
                step,
                offset,
            } => self.eval_subquery(inner, *range, *step, *offset, params),
        }
    }

    /// Evaluates a PromQL subquery: `expr[range:step]`.
    ///
    /// Evaluates the inner expression as an instant query at each step
    /// within the window `[eval_time - range, eval_time]`, collecting
    /// the results into a range vector (matrix).
    fn eval_subquery(
        &self,
        inner: &Expr,
        range: Duration,
        step: Option<Duration>,
        offset: Option<Duration>,
        params: &QueryParams,
    ) -> Result<PromQLValue, EvalError> {
        let eval_time = params.time;
        let offset_ns = offset.map_or(0, |o| o.as_nanos());
        let range_ns = range.as_nanos();
        let step_ns = if let Some(s) = step {
            let s_ns = s.as_nanos();
            if s_ns <= 0 {
                return Err(EvalError("subquery step must be positive".into()));
            }
            s_ns
        } else {
            // Default step: use the global step if available, otherwise 1 minute
            params.step.unwrap_or(60_000_000_000)
        };

        let window_end = eval_time - offset_ns;
        let window_start = window_end - range_ns;

        // Collect results at each step within the subquery window
        let mut series_map: BTreeMap<Vec<(String, String)>, Vec<Sample>> = BTreeMap::new();

        // The grid is aligned to absolute multiples of the step, not to the
        // evaluation time. Prometheus does the same, and the reason is that a
        // subquery inside a *range* query is evaluated once per outer step: a
        // grid anchored to the outer evaluation time slides underneath the
        // inner query, so `max_over_time(rate(x[5m])[1h:1m])` would sample a
        // different set of points at every step and return a jittering series
        // rather than a stable one.
        let mut t = window_start
            .div_euclid(step_ns)
            .checked_mul(step_ns)
            .ok_or_else(|| EvalError("subquery window overflows the step grid".into()))?;
        if t < window_start {
            t += step_ns;
        }
        while t <= window_end {
            let step_params = QueryParams {
                time: t,
                step: Some(step_ns),
                lookback_delta: params.lookback_delta,
                range_fetch_start: Some(window_start - params.lookback_delta),
                range_fetch_end: Some(window_end),
                ..Default::default()
            };
            let result = self.eval(inner, &step_params)?;
            if let PromQLValue::Vector(series_list) = result {
                for series in series_list {
                    for sample in &series.samples {
                        series_map
                            .entry(series.labels.clone())
                            .or_default()
                            .push(*sample);
                    }
                }
            }
            t += step_ns;
        }

        let matrix: Vec<Series> = series_map
            .into_iter()
            .map(|(labels, samples)| Series { labels, samples })
            .collect();

        Ok(PromQLValue::Matrix(matrix))
    }
}
