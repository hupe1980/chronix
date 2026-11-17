//! Why a PromQL selector selected nothing.
//!
//! An empty result is the commonest dead end in `PromQL`: a dashboard draws
//! "no data", which reads the same as an outage. [`explain_empty`] re-applies
//! a selector's matchers one at a time to the label sets of its metric and
//! names the matcher that removed the last series, with the values that
//! label does have:
//!
//! ```text
//! {job="api", env="prod", code=~"5.."}: 1240 series match job="api", 310 after env="prod", 0 after code=~"5.." — code has: ok, client_error, server_error
//! ```
//!
//! The label sets are the caller's to supply (`chronixd` reads the same ones
//! its label endpoints serve), so an embedded program can use this over its
//! own.

use std::fmt;

use crate::promql::ast::{DurationExpr, Expr, LabelMatcher, MatchOp};
use crate::promql::eval::CompiledMatcher;

/// How many of a label's values an explanation lists before `+N more`.
const VALUES_SHOWN: usize = 10;

/// One vector selector in a query, with the window it reads.
#[derive(Debug, Clone, Copy)]
pub struct SelectorRef<'a> {
    /// The metric name, when the selector names one (`m{…}` or `__name__="m"`).
    pub metric: Option<&'a str>,
    /// Every matcher, `__name__` included.
    pub matchers: &'a [LabelMatcher],
    /// The range of a matrix selector, in seconds; `0` for an instant one.
    pub range_secs: f64,
    /// The selector's `offset`, in seconds.
    pub offset_secs: f64,
}

/// Every vector selector in `expr`, in the order they are written.
#[must_use]
pub fn selectors(expr: &Expr) -> Vec<SelectorRef<'_>> {
    let mut out = Vec::new();
    collect(expr, 0.0, &mut out);
    out
}

fn collect<'a>(expr: &'a Expr, range_secs: f64, out: &mut Vec<SelectorRef<'a>>) {
    match expr {
        Expr::VectorSelector {
            name,
            matchers,
            offset,
            ..
        } => {
            let metric = name.as_deref().or_else(|| {
                matchers
                    .iter()
                    .find(|m| m.name == "__name__" && m.op == MatchOp::Equal)
                    .map(|m| m.value.as_str())
            });
            out.push(SelectorRef {
                metric,
                matchers,
                range_secs,
                offset_secs: offset.as_ref().map_or(0.0, DurationExpr::as_secs),
            });
        }
        Expr::MatrixSelector { vector, range, .. } => {
            collect(vector, range.as_secs(), out);
        }
        Expr::Call { args, .. } => {
            for a in args {
                collect(a, range_secs, out);
            }
        }
        Expr::Aggregation { expr, param, .. } => {
            if let Some(p) = param {
                collect(p, range_secs, out);
            }
            collect(expr, range_secs, out);
        }
        Expr::BinaryExpr { lhs, rhs, .. } => {
            collect(lhs, range_secs, out);
            collect(rhs, range_secs, out);
        }
        Expr::UnaryExpr { expr, .. } | Expr::Paren(expr) => collect(expr, range_secs, out),
        Expr::Subquery { expr, range, .. } => collect(expr, range.as_secs(), out),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
    }
}

/// What [`explain_empty`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmptyExplanation {
    /// The selector as written: `http_requests_total{job="api"}`.
    pub selector: String,
    /// The metric the counts start from, when the selector names one.
    pub metric: Option<String>,
    /// How many label sets the metric has in the window, before any matcher.
    pub total: usize,
    /// Each matcher in written order, with how many sets survive it.
    pub steps: Vec<(String, usize)>,
    /// The first matcher after which nothing is left.
    pub emptied_by: Option<String>,
    /// That matcher's label.
    pub label: Option<String>,
    /// Up to ten values the label has on the sets that reached the matcher.
    pub label_values: Vec<String>,
    /// How many more values there are.
    pub more_values: usize,
    /// The label sets were truncated, so every count is a lower bound.
    pub lower_bound: bool,
}

/// Apply `matchers` one at a time to `label_sets` and say where they ran out.
///
/// `label_sets` are the metric's series in the window (each a sorted list of
/// `(label, value)` pairs); `__name__` matchers are skipped, because the sets
/// already are that metric's. `truncated` says the sets are a prefix of a
/// larger set, which makes every count a lower bound.
#[must_use]
pub fn explain_empty(
    metric: Option<&str>,
    label_sets: &[Vec<(String, String)>],
    matchers: &[LabelMatcher],
    truncated: bool,
) -> EmptyExplanation {
    let shown: Vec<&LabelMatcher> = matchers.iter().filter(|m| m.name != "__name__").collect();
    let mut surviving: Vec<&Vec<(String, String)>> = label_sets.iter().collect();
    let mut steps = Vec::with_capacity(shown.len());
    let mut emptied_by = None;
    let mut label = None;
    let mut label_values = Vec::new();
    let mut more_values = 0;
    for m in &shown {
        let Ok(compiled) = CompiledMatcher::compile(m) else {
            continue;
        };
        let after: Vec<&Vec<(String, String)>> = surviving
            .iter()
            .copied()
            .filter(|set| compiled.matches(set))
            .collect();
        steps.push((render_matcher(m), after.len()));
        if after.is_empty() && !surviving.is_empty() {
            let mut values: Vec<String> = surviving
                .iter()
                .filter_map(|set| {
                    set.iter()
                        .find(|(k, _)| *k == m.name)
                        .map(|(_, v)| v.clone())
                })
                .collect();
            values.sort_unstable();
            values.dedup();
            more_values = values.len().saturating_sub(VALUES_SHOWN);
            values.truncate(VALUES_SHOWN);
            label_values = values;
            emptied_by = Some(render_matcher(m));
            label = Some(m.name.clone());
            break;
        }
        surviving = after;
    }
    EmptyExplanation {
        selector: render_selector(metric, &shown),
        metric: metric.map(str::to_string),
        total: label_sets.len(),
        steps,
        emptied_by,
        label,
        label_values,
        more_values,
        lower_bound: truncated,
    }
}

impl fmt::Display for EmptyExplanation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n = |count: usize| {
            if self.lower_bound && count > 0 {
                format!("≥{count}")
            } else {
                count.to_string()
            }
        };
        write!(f, "{}: ", self.selector)?;
        if self.total == 0 {
            return match &self.metric {
                Some(m) => write!(f, "no series of \"{m}\" in this window"),
                None => write!(f, "no series in this window"),
            };
        }
        let mut parts = Vec::with_capacity(self.steps.len() + 1);
        match &self.metric {
            Some(m) => parts.push(format!("{} series of \"{m}\"", n(self.total))),
            None => {
                if let Some((first, count)) = self.steps.first() {
                    parts.push(format!("{} series match {first}", n(*count)));
                }
            }
        }
        let rest = if self.metric.is_some() { 0 } else { 1 };
        for (matcher, count) in self.steps.iter().skip(rest) {
            parts.push(format!("{} after {matcher}", n(*count)));
        }
        write!(f, "{}", parts.join(", "))?;
        if let Some(label) = &self.label {
            if self.label_values.is_empty() {
                write!(f, " — no series has \"{label}\"")?;
            } else {
                write!(f, " — {label} has: {}", self.label_values.join(", "))?;
                if self.more_values > 0 {
                    write!(f, ", +{} more", self.more_values)?;
                }
            }
        }
        Ok(())
    }
}

fn render_matcher(m: &LabelMatcher) -> String {
    let op = match m.op {
        MatchOp::Equal => "=",
        MatchOp::NotEqual => "!=",
        MatchOp::RegexMatch => "=~",
        MatchOp::RegexNotMatch => "!~",
    };
    let value = m.value.replace('\\', "\\\\").replace('"', "\\\"");
    format!("{}{op}\"{value}\"", m.name)
}

fn render_selector(metric: Option<&str>, matchers: &[&LabelMatcher]) -> String {
    let inner: Vec<String> = matchers.iter().map(|m| render_matcher(m)).collect();
    match (metric, inner.is_empty()) {
        (Some(m), true) => m.to_string(),
        (Some(m), false) => format!("{m}{{{}}}", inner.join(", ")),
        (None, _) => format!("{{{}}}", inner.join(", ")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(name: &str, op: MatchOp, value: &str) -> LabelMatcher {
        LabelMatcher {
            name: name.into(),
            op,
            value: value.into(),
        }
    }

    /// 1240 series: 310 in prod, none with a 5xx code — the codes are words.
    fn fixture() -> Vec<Vec<(String, String)>> {
        let mut sets = Vec::new();
        for i in 0..1240 {
            let env = if i < 310 { "prod" } else { "dev" };
            let code = ["ok", "client_error", "server_error"][i % 3];
            sets.push(vec![
                ("code".into(), code.into()),
                ("env".into(), env.into()),
                ("inst".into(), format!("{i}")),
                ("job".into(), "api".into()),
            ]);
        }
        sets
    }

    #[test]
    fn the_matcher_that_emptied_it_is_named_with_the_values_that_exist() {
        let matchers = [
            m("job", MatchOp::Equal, "api"),
            m("env", MatchOp::Equal, "prod"),
            m("code", MatchOp::RegexMatch, "5.."),
        ];
        let e = explain_empty(None, &fixture(), &matchers, false);
        assert_eq!(
            e.to_string(),
            r#"{job="api", env="prod", code=~"5.."}: 1240 series match job="api", 310 after env="prod", 0 after code=~"5.." — code has: client_error, ok, server_error"#
        );
        assert_eq!(e.emptied_by.as_deref(), Some(r#"code=~"5..""#));
    }

    #[test]
    fn a_named_metric_starts_the_count() {
        let matchers = [
            m("__name__", MatchOp::Equal, "http_requests_total"),
            m("job", MatchOp::Equal, "web"),
        ];
        let e = explain_empty(Some("http_requests_total"), &fixture(), &matchers, false);
        assert_eq!(
            e.to_string(),
            r#"http_requests_total{job="web"}: 1240 series of "http_requests_total", 0 after job="web" — job has: api"#
        );
    }

    #[test]
    fn more_than_ten_values_are_counted_not_listed() {
        let matchers = [m("inst", MatchOp::Equal, "nope")];
        let e = explain_empty(None, &fixture(), &matchers, false);
        assert_eq!(e.label_values.len(), 10);
        assert!(e.to_string().ends_with(", +1230 more"), "{e}");
    }

    #[test]
    fn a_truncated_read_gives_lower_bounds() {
        let matchers = [
            m("job", MatchOp::Equal, "api"),
            m("code", MatchOp::Equal, "500"),
        ];
        let e = explain_empty(None, &fixture(), &matchers, true);
        assert!(
            e.to_string().starts_with(
                r#"{job="api", code="500"}: ≥1240 series match job="api", 0 after code="500""#
            ),
            "{e}"
        );
    }

    #[test]
    fn no_series_at_all_says_so() {
        let e = explain_empty(Some("m"), &[], &[m("job", MatchOp::Equal, "x")], false);
        assert_eq!(
            e.to_string(),
            r#"m{job="x"}: no series of "m" in this window"#
        );
    }

    #[test]
    fn a_label_no_series_carries_is_named() {
        let e = explain_empty(None, &fixture(), &[m("jbo", MatchOp::Equal, "api")], false);
        assert_eq!(
            e.to_string(),
            r#"{jbo="api"}: 0 series match jbo="api" — no series has "jbo""#
        );
    }

    #[test]
    fn every_selector_in_a_query_is_found() {
        let expr = crate::promql::parse(
            r#"sum(rate(a{x="1"}[5m] offset 1m)) / on(x) b + count_over_time(c[1h:1m])"#,
        )
        .unwrap();
        let s = selectors(&expr);
        let found: Vec<(Option<&str>, f64, f64)> = s
            .iter()
            .map(|s| (s.metric, s.range_secs, s.offset_secs))
            .collect();
        assert_eq!(
            found,
            [
                (Some("a"), 300.0, 60.0),
                (Some("b"), 0.0, 0.0),
                (Some("c"), 3600.0, 0.0)
            ]
        );
    }
}

// ── The expression tree, for `promql_explain` ──────────────────────────────

/// A PromQL query as a tree of sub-expressions, from
/// [`Chronix::promql_explain`](crate::Chronix::promql_explain).
#[derive(Debug, Clone, serde::Serialize)]
#[non_exhaustive]
pub struct PromqlExplain {
    /// The root expression.
    pub tree: ExplainNode,
}

/// One sub-expression of a PromQL query.
///
/// With `analyze`, each node is evaluated on its own over the query's window
/// and reports what it produced — the question "which step lost my series?"
/// answered by reading down the tree.
#[derive(Debug, Clone, serde::Serialize)]
#[non_exhaustive]
pub struct ExplainNode {
    /// What the node is: `selector`, `range selector`, a function or
    /// aggregation name, an operator, `number`, `subquery`.
    pub node: String,
    /// The sub-expression as PromQL.
    pub text: String,
    /// Series the node outputs (`analyze` only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub series: Option<usize>,
    /// Samples across those series (`analyze` only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub samples: Option<usize>,
    /// Why the node could not be evaluated alone (`analyze` only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// The node's operands, in the order they are written.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<ExplainNode>,
}

/// Evaluates one node of the tree on its own.
type NodeEval<'a> = &'a dyn Fn(&Expr) -> Result<crate::promql::PromQLValue, String>;

/// The tree for `expr`; with `analyze`, every node evaluated through
/// `evaluator` — the instant `params`, or `range` when given. A range-vector
/// node has no value per step, so it is read at the instant `params`.
///
/// The evaluator decides scope and bounds: a server passes one restricted to
/// the caller's namespace, with its series, memory and deadline limits.
#[must_use]
pub fn explain_tree(
    evaluator: &crate::promql::PromQLEvaluator,
    expr: &Expr,
    params: &crate::promql::eval::QueryParams,
    range: Option<&crate::promql::eval::QueryParams>,
    analyze: bool,
) -> PromqlExplain {
    let eval = |e: &Expr| {
        let range_vector = matches!(e, Expr::MatrixSelector { .. } | Expr::Subquery { .. });
        match range {
            Some(r) if !range_vector => evaluator.range_query(e, r),
            _ => evaluator.instant_query(e, params),
        }
        .map_err(|err| err.0)
    };
    let eval: Option<NodeEval<'_>> = if analyze { Some(&eval) } else { None };
    PromqlExplain {
        tree: tree(expr, eval),
    }
}

/// Build the tree for `expr`. With `eval`, every node is evaluated through it.
fn tree(expr: &Expr, eval: Option<NodeEval<'_>>) -> ExplainNode {
    // Parentheses are syntax, not a step: show what they hold.
    if let Expr::Paren(inner) = expr {
        return tree(inner, eval);
    }
    let (node, children): (String, Vec<&Expr>) = match expr {
        Expr::NumberLiteral(_) => ("number".into(), vec![]),
        Expr::StringLiteral(_) => ("string".into(), vec![]),
        Expr::VectorSelector { .. } => ("selector".into(), vec![]),
        Expr::MatrixSelector { .. } => ("range selector".into(), vec![]),
        Expr::Call { func, args } => (func.clone(), args.iter().collect()),
        Expr::Aggregation {
            op, expr, param, ..
        } => (
            op.to_string(),
            param
                .iter()
                .map(AsRef::as_ref)
                .chain([expr.as_ref()])
                .collect(),
        ),
        Expr::BinaryExpr { op, lhs, rhs, .. } => (op.to_string(), vec![lhs, rhs]),
        Expr::UnaryExpr { expr, .. } => ("-".into(), vec![expr]),
        Expr::Subquery { expr, .. } => ("subquery".into(), vec![expr]),
        Expr::Paren(_) => unreachable!("handled above"),
    };
    let mut out = ExplainNode {
        node,
        text: expr.to_string(),
        series: None,
        samples: None,
        error: None,
        children: children.into_iter().map(|c| tree(c, eval)).collect(),
    };
    if let Some(eval) = eval
        && !matches!(expr, Expr::StringLiteral(_))
    {
        use crate::promql::PromQLValue as V;
        match eval(expr) {
            Ok(V::Vector(s) | V::Matrix(s)) => {
                out.samples = Some(s.iter().map(|x| x.samples.len() + x.histograms.len()).sum());
                out.series = Some(s.len());
            }
            Ok(V::Scalar(_)) => {
                out.series = Some(1);
                out.samples = Some(1);
            }
            Ok(V::String(_)) => {}
            Err(e) => out.error = Some(e),
        }
    }
    out
}
