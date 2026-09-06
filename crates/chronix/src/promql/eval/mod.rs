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

/// Refuse a vector holding two series with the same label set.
///
/// Prometheus's message, verbatim, because clients and dashboards match on it:
/// `vector cannot contain metrics with the same labelset`.
fn check_no_duplicate_labels(series: &[Series]) -> Result<(), EvalError> {
    let mut seen: std::collections::HashSet<&[(String, String)]> =
        std::collections::HashSet::with_capacity(series.len());
    for s in series {
        if !seen.insert(s.labels.as_slice()) {
            return Err(EvalError(
                "vector cannot contain metrics with the same labelset".into(),
            ));
        }
    }
    Ok(())
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
/// about behaviour, and a claim in a comment is a claim to be tested. The
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

/// The step a subquery written `[range:]` evaluates at.
///
/// Prometheus resolves a missing subquery step to the engine's evaluation
/// interval, whose default is one minute in every standard deployment. It is a
/// property of the engine, not of the query, which is why it is a constant
/// here rather than a fall-back to the caller's step.
const DEFAULT_SUBQUERY_STEP_NS: i64 = 60_000_000_000;

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
    /// Maximum evaluation points a range query may produce (0 = unlimited).
    ///
    /// Prometheus's own cap is 11,000, and this defaults to the same, because
    /// a range query costs one full evaluation of the expression *per step*.
    /// Without it `start=0, end=now, step=1ns` is an accepted query that runs
    /// for longer than the universe has existed, and the evaluator is a public
    /// embedded API where no HTTP-layer guard stands in front of it.
    pub max_points: usize,
    /// When this evaluation must stop, and the budget it was given.
    ///
    /// **A deadline the evaluation carries.** `prom_query_timeout_secs` was a
    /// `tokio::time::timeout` around the `spawn_blocking` handle that runs
    /// this evaluator, and dropping a `JoinHandle` cancels nothing: the client
    /// got its timeout while the scan ran on to completion, holding a blocking
    /// thread and its accumulator. A repeated expensive query therefore cost
    /// unbounded work on a server that had already answered. Checked between
    /// steps and before each selector scan, which are the two units of work a
    /// range query is made of.
    ///
    /// `None` means no deadline. The HTTP layer keeps its own `timeout` as a
    /// backstop for the case where a single step outlives the budget.
    pub deadline: Option<Deadline>,
}

/// A wall-clock budget for one evaluation.
///
/// `Instant` is not `Default` and a deadline has to survive being copied into
/// every per-step [`QueryParams`], so it carries both the expiry and the
/// budget it came from — the budget is what the error message names, and an
/// expiry alone cannot say what it was.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deadline {
    expires_at: std::time::Instant,
    budget: std::time::Duration,
}

impl Deadline {
    /// A deadline `budget` from now. `None` for a zero budget.
    #[must_use]
    pub fn after(budget: std::time::Duration) -> Option<Self> {
        (!budget.is_zero()).then(|| Self {
            expires_at: std::time::Instant::now() + budget,
            budget,
        })
    }

    /// `Err` once the budget is spent.
    ///
    /// # Errors
    /// [`EvalError`] naming the budget, so the caller learns what bound it.
    pub fn check(self) -> Result<(), EvalError> {
        if std::time::Instant::now() >= self.expires_at {
            return Err(EvalError(format!(
                "query exceeded its time budget of {:?}",
                self.budget
            )));
        }
        Ok(())
    }
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
            max_points: 11_000,
            deadline: None,
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
        if start > end {
            return Err(EvalError("start must not be after end".into()));
        }

        // Cost the query before running it. A range query evaluates the whole
        // expression once per step, so the step count *is* the cost, and it is
        // knowable up front. `i128` because the span can be the whole i64
        // domain.
        let points = (i128::from(end) - i128::from(start)) / i128::from(step) + 1;
        if params.max_points > 0 && points > params.max_points as i128 {
            return Err(EvalError(format!(
                "query would produce {points} evaluation points, exceeding the limit of {}",
                params.max_points
            )));
        }

        // Collect results at each step
        let mut series_map: BTreeMap<Vec<(String, String)>, Vec<Sample>> = BTreeMap::new();
        let max_series = params.max_series;
        let max_memory = params.max_memory_bytes;
        // Track estimated bytes to bound memory usage.
        let mut estimated_bytes: usize = 0;

        let mut t = start;
        while t <= end {
            // One step is one full evaluation of the expression, so this is
            // the granularity at which a range query can be stopped.
            if let Some(deadline) = params.deadline {
                deadline.check()?;
            }
            let step_params = QueryParams {
                time: t,
                // The query's own bounds travel with every step, because
                // `@ start()` and `@ end()` resolve against them: a `start()`
                // that saw only the step time would pin each step to itself,
                // which is the one thing the modifier exists not to do.
                start: Some(start),
                end: Some(end),
                step: params.step,
                lookback_delta: params.lookback_delta,
                range_fetch_start: Some(start - params.lookback_delta),
                range_fetch_end: Some(end),
                max_series,
                max_memory_bytes: max_memory,
                max_points: params.max_points,
                deadline: params.deadline,
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
            // `t + step` overflows when `end` sits near `i64::MAX`. In a
            // release build that wraps to a large negative number, `t <= end`
            // stays true, and the loop never ends — a spinning blocking thread
            // per query, which the caller's timeout does not reclaim. The step
            // count guard above does not cover it: a *large* step keeps the
            // count small, and three points is what `end = i64::MAX` with a
            // step of `i64::MAX / 2` costs.
            let Some(next) = t.checked_add(step) else {
                break;
            };
            t = next;
        }

        let matrix: Vec<Series> = series_map
            .into_iter()
            .map(|(labels, samples)| Series { labels, samples })
            .collect();

        Ok(PromQLValue::Matrix(matrix))
    }

    pub(crate) fn eval(&self, expr: &Expr, params: &QueryParams) -> Result<PromQLValue, EvalError> {
        let value = self.eval_node(expr, params)?;
        // Prometheus checks every node's output, not only the query's: a
        // vector holding two series with one label set has no meaning, and
        // whichever operator consumes it next would silently pick one. The
        // shapes that produce it here are a function that drops `__name__`
        // from a selector spanning several metrics —
        // `rate({__name__=~"cpu.+"}[1m])` over `cpu_usage` and `cpu_load` —
        // and two `(measurement, field)` pairs that spell the same name.
        if let PromQLValue::Vector(series) = &value {
            check_no_duplicate_labels(series)?;
        }
        Ok(value)
    }

    fn eval_node(&self, expr: &Expr, params: &QueryParams) -> Result<PromQLValue, EvalError> {
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
                at,
            } => self.eval_vector_selector(name, matchers, offset, *at, params),

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
                at,
            } => self.eval_subquery(inner, *range, *step, *offset, *at, params),
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
        at: Option<crate::promql::ast::AtModifier>,
        params: &QueryParams,
    ) -> Result<PromQLValue, EvalError> {
        let eval_time = match at {
            None => params.time,
            Some(at) => at.resolve(
                params.start.unwrap_or(params.time),
                params.end.unwrap_or(params.time),
            ),
        };
        let offset_ns = offset.map_or(0, |o| o.as_nanos());
        let range_ns = range.as_nanos();
        let step_ns = if let Some(s) = step {
            let s_ns = s.as_nanos();
            if s_ns <= 0 {
                return Err(EvalError("subquery step must be positive".into()));
            }
            s_ns
        } else {
            // The engine's own evaluation interval, **not** the outer step.
            //
            // `noStepSubqueryIntervalFn` in Prometheus is a property of the
            // engine, and it has nothing to do with the resolution the caller
            // asked for. Inheriting the outer step made the same dashboard
            // panel mean different things at different zoom levels — and at a
            // one-second step, `x[5m:]` is 300 inner evaluations per outer
            // step instead of five.
            DEFAULT_SUBQUERY_STEP_NS
        };

        let window_end = eval_time - offset_ns;
        let window_start = window_end - range_ns;

        // See the note on `range_fetch_start` below: inside a range query this
        // is constant across outer steps, which is what makes the scan cache
        // work at all for a subquery.
        let (prefetch_start, prefetch_end) =
            match (params.range_fetch_start, params.range_fetch_end) {
                (Some(rs), Some(re)) => (
                    rs.saturating_sub(range_ns).saturating_sub(offset_ns),
                    re.saturating_sub(offset_ns),
                ),
                _ => (
                    window_start.saturating_sub(params.lookback_delta),
                    window_end,
                ),
            };

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
        // Left-**open**: a grid point landing exactly on `window_start`
        // belongs to the previous window. Prometheus advances by one interval
        // when the aligned start is `<=` the window start, for the same reason
        // a range selector excludes its left boundary — otherwise an evenly
        // sampled series yields n+1 points in some windows and n in others,
        // and `count_over_time` flickers between the two.
        if t <= window_start {
            t = t
                .checked_add(step_ns)
                .ok_or_else(|| EvalError("subquery window overflows the step grid".into()))?;
        }

        // A subquery costs one inner evaluation per inner step, and inside a
        // range query it pays that once per *outer* step — so an unbounded
        // inner step count is the same denial of service as an unbounded outer
        // one, multiplied. `x[1h:1ns]` is 3.6 * 10^12 inner evaluations.
        let points = (i128::from(window_end) - i128::from(t)) / i128::from(step_ns) + 1;
        if params.max_points > 0 && points > params.max_points as i128 {
            return Err(EvalError(format!(
                "subquery would produce {points} evaluation points, exceeding the limit of {}",
                params.max_points
            )));
        }

        while t <= window_end {
            if let Some(deadline) = params.deadline {
                deadline.check()?;
            }
            let step_params = QueryParams {
                time: t,
                step: Some(step_ns),
                lookback_delta: params.lookback_delta,
                // The prefetch window is the **outer** query's, widened by
                // this subquery's reach — not this subquery's own window.
                //
                // The subquery's window moves with the outer step, so deriving
                // the prefetch from it made every outer step ask for a
                // different range and miss the scan cache: 138 reads of the
                // same data for a 20-step range query, and the eight-entry cap
                // meant the misses could not even accumulate into hits.
                // Every inner step of every outer step falls inside
                // `[outer_start - range - offset - lookback, outer_end -
                // offset]`, which is the same at all of them.
                range_fetch_start: Some(prefetch_start),
                range_fetch_end: Some(prefetch_end),
                // Likewise inside a subquery: a `@ start()` written under one
                // still means the outer query's start.
                start: params.start,
                end: params.end,
                // The budget belongs to the query, not to the outermost node:
                // dropping it here made the inner evaluation of a subquery the
                // one unbudgeted path in the evaluator.
                max_series: params.max_series,
                max_memory_bytes: params.max_memory_bytes,
                max_points: params.max_points,
                deadline: params.deadline,
            };
            let result = self.eval(inner, &step_params)?;
            if let PromQLValue::Vector(series_list) = result {
                for series in series_list {
                    for sample in &series.samples {
                        // The **step** timestamp, not the sample's own. The
                        // inner expression was evaluated *at* `t`, so that is
                        // the instant its result belongs to; an instant vector
                        // otherwise reports the raw scrape time, and adjacent
                        // steps that see the same newest sample then emit the
                        // same timestamp twice. A range vector with duplicate
                        // timestamps is not one: `irate` reads `dt == 0` from
                        // it, and the matrix handed to a client is neither
                        // step-aligned nor strictly increasing.
                        series_map
                            .entry(series.labels.clone())
                            .or_default()
                            .push(Sample {
                                timestamp: t,
                                value: sample.value,
                            });
                    }
                }
            }
            let Some(next) = t.checked_add(step_ns) else {
                break;
            };
            t = next;
        }

        let matrix: Vec<Series> = series_map
            .into_iter()
            .map(|(labels, samples)| Series { labels, samples })
            .collect();

        Ok(PromQLValue::Matrix(matrix))
    }
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod deadline_tests {
    use super::{Deadline, PromQLEvaluator, QueryParams};
    use std::sync::Arc;
    use std::time::Duration;

    /// A zero budget is no budget, so the embedded API keeps its old shape.
    #[test]
    fn a_zero_budget_is_no_deadline() {
        assert!(Deadline::after(Duration::ZERO).is_none());
        assert!(Deadline::after(Duration::from_secs(1)).is_some());
    }

    /// A spent budget names itself, so the caller learns what bound it.
    #[test]
    fn a_spent_budget_says_what_it_was() {
        let deadline = Deadline::after(Duration::from_nanos(1)).expect("non-zero");
        std::thread::sleep(Duration::from_millis(2));
        let err = deadline.check().expect_err("the budget is spent");
        assert!(err.0.contains("time budget"), "{err}");
    }

    /// An evaluation that starts past its deadline stops before it scans.
    ///
    /// `prom_query_timeout_secs` used to be a `tokio::time::timeout` around
    /// the `spawn_blocking` handle running this evaluator, and dropping a
    /// `JoinHandle` cancels nothing: the client got its timeout while the
    /// scan ran on, holding a blocking thread and its accumulator. So the
    /// deadline had to become something the evaluation itself carries.
    #[test]
    fn an_expired_deadline_stops_a_range_query_before_its_first_step() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let config = chronix_core::ChronixConfig::builder()
            .data_dir(tmp.path().to_path_buf())
            .build()
            .expect("config");
        let db = Arc::new(crate::Chronix::open(config).expect("open"));
        let now = 1_700_000_000_000_000_000i64;
        let points: Vec<chronix_core::Point> = (0..100)
            .map(|i| {
                chronix_core::Point::new(
                    chronix_core::SeriesKey::new("cpu", crate::tags! { "host" => "a" })
                        .expect("key"),
                    crate::fields! { "value" => f64::from(i) },
                    now + i64::from(i) * 1_000_000_000,
                )
                .expect("point")
            })
            .collect();
        let _ = db.insert_batch(&points).expect("insert");

        let expr = crate::promql::parse("cpu").expect("parse");
        let evaluator = PromQLEvaluator::new(db);
        let deadline = Deadline::after(Duration::from_nanos(1));
        std::thread::sleep(Duration::from_millis(2));
        let params = QueryParams {
            start: Some(now),
            end: Some(now + 100_000_000_000),
            step: Some(1_000_000_000),
            deadline,
            ..Default::default()
        };
        let err = evaluator
            .range_query(&expr, &params)
            .expect_err("an expired deadline must stop the evaluation");
        assert!(err.0.contains("time budget"), "{err}");

        // …and with no deadline the same query answers, so the guard above is
        // the deadline biting and not the query being broken.
        let ok = QueryParams {
            deadline: None,
            ..params
        };
        evaluator
            .range_query(&expr, &ok)
            .expect("the same query without a budget answers");
    }
}
