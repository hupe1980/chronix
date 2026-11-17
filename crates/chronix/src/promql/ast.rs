//! `PromQL` abstract syntax tree types.

use std::fmt;

/// A duration in a range selector, a subquery or an offset.
///
/// Either a literal or an arithmetic expression over durations, numbers
/// (seconds) and the duration functions `step()`, `range()`, `min_of` and
/// `max_of`. The evaluator resolves every expression to a [`Literal`] once,
/// at the start of a query, against that query's step and range; the
/// accessors below read the literal.
///
/// [`Literal`]: DurationExpr::Literal
#[derive(Debug, Clone, PartialEq)]
pub enum DurationExpr {
    /// Seconds: `5m` is `Literal(300.0)`, a bare `5` is `Literal(5.0)`.
    Literal(f64),
    /// `step()`: the range query's step; 0 s for an instant query.
    Step,
    /// `range()`: the range query's end minus its start; 0 s for an instant query.
    Range,
    /// `lhs <op> rhs`.
    Binary {
        /// The operator.
        op: DurationOp,
        /// Left operand.
        lhs: Box<DurationExpr>,
        /// Right operand.
        rhs: Box<DurationExpr>,
    },
    /// `-expr`.
    Neg(Box<DurationExpr>),
    /// `min_of(a, b)`.
    MinOf(Box<DurationExpr>, Box<DurationExpr>),
    /// `max_of(a, b)`.
    MaxOf(Box<DurationExpr>, Box<DurationExpr>),
}

/// An arithmetic operator inside a duration expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurationOp {
    /// `+`
    Add,
    /// `-`
    Sub,
    /// `*`
    Mul,
    /// `/`
    Div,
    /// `%`
    Mod,
    /// `^`
    Pow,
}

impl DurationExpr {
    /// The literal value in seconds, or `0.0` for an unresolved expression.
    ///
    /// Every public evaluation entry point resolves expressions first, so the
    /// evaluator only ever sees literals.
    #[must_use]
    pub fn as_secs(&self) -> f64 {
        match self {
            Self::Literal(secs) => *secs,
            _ => 0.0,
        }
    }

    /// The literal value in nanoseconds, clamped to the `i64` range.
    #[must_use]
    pub fn as_nanos(&self) -> i64 {
        secs_to_nanos_safe(self.as_secs())
    }

    /// The literal value in milliseconds, clamped to the `i64` range.
    #[must_use]
    pub fn as_millis(&self) -> i64 {
        float_to_i64_safe(self.as_secs() * 1_000.0)
    }

    /// Evaluate against a query's step and range, both in seconds.
    ///
    /// # Errors
    /// A division or modulo by zero, or a result that is not finite.
    pub fn resolve(&self, step_secs: f64, range_secs: f64) -> Result<f64, String> {
        let v = self.eval(step_secs, range_secs)?;
        if !v.is_finite() {
            return Err(format!("duration expression `{self}` is not finite"));
        }
        Ok(v)
    }

    fn eval(&self, step: f64, range: f64) -> Result<f64, String> {
        Ok(match self {
            Self::Literal(v) => *v,
            Self::Step => step,
            Self::Range => range,
            Self::Neg(e) => -e.eval(step, range)?,
            Self::MinOf(a, b) => nan_aware(a.eval(step, range)?, b.eval(step, range)?, f64::min),
            Self::MaxOf(a, b) => nan_aware(a.eval(step, range)?, b.eval(step, range)?, f64::max),
            Self::Binary { op, lhs, rhs } => {
                let (a, b) = (lhs.eval(step, range)?, rhs.eval(step, range)?);
                match op {
                    DurationOp::Add => a + b,
                    DurationOp::Sub => a - b,
                    DurationOp::Mul => a * b,
                    DurationOp::Div if b == 0.0 => {
                        return Err(format!("division by zero in duration expression `{self}`"));
                    }
                    DurationOp::Div => a / b,
                    DurationOp::Mod if b == 0.0 => {
                        return Err(format!("modulo by zero in duration expression `{self}`"));
                    }
                    DurationOp::Mod => a % b,
                    DurationOp::Pow => a.powf(b),
                }
            }
        })
    }
}

/// `min_of`/`max_of` ignore a `NaN` argument when the other is a number.
fn nan_aware(a: f64, b: f64, f: fn(f64, f64) -> f64) -> f64 {
    match (a.is_nan(), b.is_nan()) {
        (true, _) => b,
        (_, true) => a,
        _ => f(a, b),
    }
}

impl fmt::Display for DurationExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Literal(secs) => write!(f, "{secs}s"),
            Self::Step => write!(f, "step()"),
            Self::Range => write!(f, "range()"),
            Self::Neg(e) => write!(f, "-{e}"),
            Self::MinOf(a, b) => write!(f, "min_of({a}, {b})"),
            Self::MaxOf(a, b) => write!(f, "max_of({a}, {b})"),
            Self::Binary { op, lhs, rhs } => {
                let op = match op {
                    DurationOp::Add => "+",
                    DurationOp::Sub => "-",
                    DurationOp::Mul => "*",
                    DurationOp::Div => "/",
                    DurationOp::Mod => "%",
                    DurationOp::Pow => "^",
                };
                write!(f, "({lhs} {op} {rhs})")
            }
        }
    }
}

/// `anchored` or `smoothed` after a range selector: `rate(x[5m] smoothed)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeModifier {
    /// Use the newest sample at or before the range start as the left
    /// boundary, and the last sample in the range as the right; no
    /// extrapolation.
    Anchored,
    /// Interpolate linearly at both range boundaries.
    Smoothed,
}

// ─── Safe numeric helpers ────────────────────────────────────────────

/// Convert seconds (f64) to nanoseconds (i64) without saturating-cast UB.
#[inline]
fn secs_to_nanos_safe(secs: f64) -> i64 {
    let ns = secs * 1_000_000_000.0;
    float_to_i64_safe(ns)
}

/// Clamp an f64 into the representable `i64` range before casting.
#[inline]
fn float_to_i64_safe(v: f64) -> i64 {
    #[allow(clippy::cast_possible_truncation)]
    if v.is_nan() {
        0
    } else if v >= i64::MAX as f64 {
        i64::MAX
    } else if v <= i64::MIN as f64 {
        i64::MIN
    } else {
        v as i64
    }
}

/// A label matcher.
#[derive(Debug, Clone, PartialEq)]
pub struct LabelMatcher {
    /// Label name.
    pub name: String,
    /// Match operator.
    pub op: MatchOp,
    /// Match value.
    pub value: String,
}

impl LabelMatcher {
    /// Whether this matcher is satisfied by a series that does **not** carry
    /// the label at all.
    ///
    /// An absent label reads as the empty string, so this is "does it match
    /// the empty value". A selector made only of such matchers names the whole
    /// database, which is why Prometheus refuses one.
    ///
    /// An invalid regex answers `false`; it is reported where the matcher is
    /// compiled, and answering `true` here would turn a bad pattern into the
    /// wrong error.
    #[must_use]
    pub fn matches_empty(&self) -> bool {
        match self.op {
            MatchOp::Equal => self.value.is_empty(),
            MatchOp::NotEqual => !self.value.is_empty(),
            MatchOp::RegexMatch | MatchOp::RegexNotMatch => {
                // Anchored and `(?s)`, exactly as `CompiledMatcher` anchors.
                let Ok(re) = regex::Regex::new(&format!("^(?s:{})$", self.value)) else {
                    return false;
                };
                re.is_match("") == (self.op == MatchOp::RegexMatch)
            }
        }
    }
}

/// Match operator for label matchers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchOp {
    /// `=`
    Equal,
    /// `!=`
    NotEqual,
    /// `=~`
    RegexMatch,
    /// `!~`
    RegexNotMatch,
}

impl fmt::Display for MatchOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MatchOp::Equal => write!(f, "="),
            MatchOp::NotEqual => write!(f, "!="),
            MatchOp::RegexMatch => write!(f, "=~"),
            MatchOp::RegexNotMatch => write!(f, "!~"),
        }
    }
}

/// Aggregation modifier: `by (labels)` or `without (labels)`.
#[derive(Debug, Clone, PartialEq)]
pub enum AggregationModifier {
    /// Aggregate by the specified labels.
    By(Vec<String>),
    /// Aggregate without the specified labels.
    Without(Vec<String>),
}

/// Aggregation operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationOp {
    /// `sum` aggregation.
    Sum,
    /// `avg` aggregation.
    Avg,
    /// `min` aggregation.
    Min,
    /// `max` aggregation.
    Max,
    /// `count` aggregation.
    Count,
    /// `stddev` aggregation.
    Stddev,
    /// `stdvar` aggregation.
    Stdvar,
    /// `topk` aggregation.
    Topk,
    /// `bottomk` aggregation.
    Bottomk,
    /// `quantile` aggregation.
    Quantile,
    /// `count_values` aggregation.
    CountValues,
    /// `group` aggregation.
    Group,
}

impl AggregationOp {
    /// Every aggregation operator's name, as a query spells it.
    pub const NAMES: &'static [&'static str] = &[
        "sum",
        "avg",
        "min",
        "max",
        "count",
        "stddev",
        "stdvar",
        "topk",
        "bottomk",
        "quantile",
        "count_values",
        "group",
    ];

    /// Parses an aggregation operator from a string (case-insensitive).
    #[must_use]
    pub fn parse_op(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "sum" => Some(Self::Sum),
            "avg" => Some(Self::Avg),
            "min" => Some(Self::Min),
            "max" => Some(Self::Max),
            "count" => Some(Self::Count),
            "stddev" => Some(Self::Stddev),
            "stdvar" => Some(Self::Stdvar),
            "topk" => Some(Self::Topk),
            "bottomk" => Some(Self::Bottomk),
            "quantile" => Some(Self::Quantile),
            "count_values" => Some(Self::CountValues),
            "group" => Some(Self::Group),
            _ => None,
        }
    }
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    /// Addition (`+`).
    Add,
    /// Subtraction (`-`).
    Sub,
    /// Multiplication (`*`).
    Mul,
    /// Division (`/`).
    Div,
    /// Modulo (`%`).
    Mod,
    /// Exponentiation (`^`).
    Pow,
    /// Equal comparison (`==`).
    Eql,
    /// Not-equal comparison (`!=`).
    Neq,
    /// Less-than comparison (`<`).
    Lss,
    /// Greater-than comparison (`>`).
    Gtr,
    /// Less-than-or-equal comparison (`<=`).
    Lte,
    /// Greater-than-or-equal comparison (`>=`).
    Gte,
    /// Logical AND set operator.
    And,
    /// Logical OR set operator.
    Or,
    /// UNLESS set operator.
    Unless,
}

impl BinaryOp {
    /// Precedence (higher = binds tighter).
    #[must_use]
    pub fn precedence(self) -> u8 {
        match self {
            BinaryOp::Or => 1,
            // Prometheus: `and` and `unless` share the same precedence.
            BinaryOp::And | BinaryOp::Unless => 2,
            BinaryOp::Eql
            | BinaryOp::Neq
            | BinaryOp::Lss
            | BinaryOp::Gtr
            | BinaryOp::Lte
            | BinaryOp::Gte => 3,
            BinaryOp::Add | BinaryOp::Sub => 4,
            BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => 5,
            BinaryOp::Pow => 6,
        }
    }

    /// Right-associative (only Pow).
    #[must_use]
    pub fn is_right_assoc(self) -> bool {
        matches!(self, BinaryOp::Pow)
    }

    /// Returns `true` if this is a comparison operator.
    #[must_use]
    pub fn is_comparison(self) -> bool {
        matches!(
            self,
            BinaryOp::Eql
                | BinaryOp::Neq
                | BinaryOp::Lss
                | BinaryOp::Gtr
                | BinaryOp::Lte
                | BinaryOp::Gte
        )
    }

    /// Returns `true` if this is a set operator (`and`, `or`, `unless`).
    #[must_use]
    pub fn is_set_operator(self) -> bool {
        matches!(self, BinaryOp::And | BinaryOp::Or | BinaryOp::Unless)
    }

    /// Returns `true` if this is an arithmetic operator (`+`, `-`, `*`, `/`, `%`, `^`).
    #[must_use]
    pub fn is_arithmetic(self) -> bool {
        matches!(
            self,
            BinaryOp::Add
                | BinaryOp::Sub
                | BinaryOp::Mul
                | BinaryOp::Div
                | BinaryOp::Mod
                | BinaryOp::Pow
        )
    }
}

/// Vector matching for binary operations.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorMatching {
    /// Matching cardinality.
    pub card: VectorMatchingCardinality,
    /// Labels used for matching.
    pub labels: Vec<String>,
    /// `true` for `on(...)`, `false` for `ignoring(...)`.
    pub on: bool,
    /// Labels to include from the "one" side in group_left/group_right.
    pub include: Vec<String>,
}

/// Cardinality of a binary vector matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorMatchingCardinality {
    /// One-to-one matching.
    OneToOne,
    /// Many-to-one matching (with `group_left`).
    ManyToOne,
    /// One-to-many matching (with `group_right`).
    OneToMany,
    /// Many-to-many matching.
    ManyToMany,
}

/// The `PromQL` AST node.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A numeric literal: `42`, `1.5`.
    NumberLiteral(f64),

    /// A string literal: `"hello"`.
    StringLiteral(String),

    /// An instant vector selector: `metric_name{label="value"}`.
    VectorSelector {
        /// The metric name (can be empty if __name__ matcher is present).
        name: Option<String>,
        /// Label matchers.
        matchers: Vec<LabelMatcher>,
        /// Optional offset: `offset 5m`. Negative shifts forward in time.
        offset: Option<DurationExpr>,
        /// Optional `@` modifier, pinning the evaluation instant.
        at: Option<AtModifier>,
        /// `metric smoothed`: interpolate at the evaluation time.
        smoothed: bool,
    },

    /// A matrix selector: `metric_name{...}[5m]`.
    MatrixSelector {
        /// The vector selector.
        vector: Box<Expr>,
        /// The range duration.
        range: DurationExpr,
        /// `anchored` or `smoothed`, if given.
        modifier: Option<RangeModifier>,
    },

    /// A function call: `rate(http_requests_total[5m])`.
    Call {
        /// Function name.
        func: String,
        /// Function arguments.
        args: Vec<Expr>,
    },

    /// An aggregation: `sum by (host) (metric)`.
    Aggregation {
        /// Aggregation operator.
        op: AggregationOp,
        /// Expression to aggregate.
        expr: Box<Expr>,
        /// Optional parameter (e.g. k for `topk`).
        param: Option<Box<Expr>>,
        /// Optional `by` or `without` modifier.
        modifier: Option<AggregationModifier>,
    },

    /// A binary expression: `a + b`.
    BinaryExpr {
        /// Binary operator.
        op: BinaryOp,
        /// Left-hand side expression.
        lhs: Box<Expr>,
        /// Right-hand side expression.
        rhs: Box<Expr>,
        /// Whether the `bool` modifier is present.
        bool_mod: bool,
        /// Optional vector matching clause.
        matching: Option<VectorMatching>,
    },

    /// A unary negation: `-expr`.
    UnaryExpr {
        /// Unary operator.
        op: UnaryOp,
        /// Inner expression.
        expr: Box<Expr>,
    },

    /// Parenthesized expression.
    Paren(Box<Expr>),

    /// A subquery: `metric[5m:1m]`.
    Subquery {
        /// Inner expression.
        expr: Box<Expr>,
        /// Range duration.
        range: DurationExpr,
        /// Optional step duration.
        step: Option<DurationExpr>,
        /// Optional offset. Negative shifts forward in time.
        offset: Option<DurationExpr>,
        /// Optional `@` modifier, pinning the evaluation instant.
        at: Option<AtModifier>,
    },
}

/// The `@` modifier: pins a selector's evaluation to a fixed instant.
///
/// `foo @ 1609746000` always reads the sample at that Unix second, whatever
/// time the query is evaluated at, and `foo @ start()` / `@ end()` resolve to
/// a range query's own bounds. The point of it is that one expression can
/// compare a moving value against a fixed one —
/// `rate(x[5m]) / rate(x[5m] @ start())` — which is otherwise not expressible.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AtModifier {
    /// An absolute instant, in Unix **seconds** as written.
    Timestamp(f64),
    /// The range query's start (`@ start()`); the evaluation time for an
    /// instant query, which is what Prometheus does.
    Start,
    /// The range query's end (`@ end()`).
    End,
}

impl AtModifier {
    /// Resolve to an absolute nanosecond instant.
    ///
    /// `start` and `end` are the range query's bounds; for an instant query
    /// both are the evaluation time.
    #[must_use]
    pub fn resolve(self, start: i64, end: i64) -> i64 {
        match self {
            Self::Timestamp(secs) => secs_to_nanos_safe(secs),
            Self::Start => start,
            Self::End => end,
        }
    }
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    /// Negation (`-`).
    Neg,
}

// ── Query types ────────────────────────────────────────────────────────

/// A single sample (timestamp + value).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sample {
    /// Timestamp in nanoseconds.
    pub timestamp: i64,
    /// Sample value.
    pub value: f64,
}

/// A series of samples with labels.
#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    /// Label key-value pairs identifying the series.
    pub labels: Vec<(String, String)>,
    /// Float samples in the series.
    pub samples: Vec<Sample>,
    /// Native-histogram samples in the series.
    ///
    /// A **separate list**, not a sum type on [`Sample`], which is the shape
    /// Prometheus itself uses (`Series.Floats` beside `Series.Histograms`).
    /// The reason is not taste: nearly every operator in PromQL — `rate`,
    /// the arithmetic, the comparison operators, the `*_over_time` family —
    /// is defined on floats and has nothing to say about a distribution.
    /// Making `Sample::value` a sum type would put a `match` in all of them
    /// for a variant they must reject anyway, and would touch the code paths
    /// where a wrong answer costs the most.
    ///
    /// Usually empty. A series carries floats or histograms, not both — a
    /// metric that changed type mid-range is the one case where both are
    /// populated, and the functions that meet it say so rather than mixing
    /// them.
    pub histograms: Vec<HistogramSample>,
}

/// One native-histogram observation in a series.
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramSample {
    /// Timestamp in nanoseconds.
    pub timestamp: i64,
    /// The distribution observed at that instant.
    ///
    /// Boxed: a `Series` holds a vector of these, and a histogram carries
    /// three `Vec`s of its own.
    pub histogram: Box<chronix_core::histogram::Histogram>,
}

impl Series {
    /// A series of float samples — the ordinary case.
    #[must_use]
    pub fn floats(labels: Vec<(String, String)>, samples: Vec<Sample>) -> Self {
        Self {
            labels,
            samples,
            histograms: Vec::new(),
        }
    }

    /// A series of histogram samples.
    #[must_use]
    pub fn histograms(labels: Vec<(String, String)>, histograms: Vec<HistogramSample>) -> Self {
        Self {
            labels,
            samples: Vec::new(),
            histograms,
        }
    }

    /// Whether this series carries histogram samples rather than floats.
    #[must_use]
    pub fn is_histogram(&self) -> bool {
        !self.histograms.is_empty()
    }
}

/// The result of a `PromQL` evaluation.
#[derive(Debug, Clone, PartialEq)]
pub enum PromQLValue {
    /// An instant vector: one sample per series.
    Vector(Vec<Series>),
    /// A range vector / matrix: multiple samples per series.
    Matrix(Vec<Series>),
    /// A scalar value.
    Scalar(f64),
    /// A string value.
    String(String),
}

// ── Printing ────────────────────────────────────────────────────────────
//
// `Display` renders an expression back to PromQL that parses to the same
// tree (`print_round_trips` in the tests holds that), so an explain tree, a
// log line or an error can show a sub-expression as the user would write it.

/// A PromQL string literal, double-quoted with `"` and `\` escaped.
fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn number(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_owned()
    } else if v.is_infinite() {
        if v > 0.0 { "Inf" } else { "-Inf" }.to_owned()
    } else {
        format!("{v}")
    }
}

impl fmt::Display for LabelMatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}{}", self.name, self.op, quote(&self.value))
    }
}

impl fmt::Display for AggregationOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
            Self::Count => "count",
            Self::Stddev => "stddev",
            Self::Stdvar => "stdvar",
            Self::Topk => "topk",
            Self::Bottomk => "bottomk",
            Self::Quantile => "quantile",
            Self::CountValues => "count_values",
            Self::Group => "group",
        };
        f.write_str(name)
    }
}

impl fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let op = match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Mod => "%",
            Self::Pow => "^",
            Self::Eql => "==",
            Self::Neq => "!=",
            Self::Lss => "<",
            Self::Gtr => ">",
            Self::Lte => "<=",
            Self::Gte => ">=",
            Self::And => "and",
            Self::Or => "or",
            Self::Unless => "unless",
        };
        f.write_str(op)
    }
}

impl fmt::Display for AtModifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timestamp(t) => write!(f, "@ {}", number(*t)),
            Self::Start => f.write_str("@ start()"),
            Self::End => f.write_str("@ end()"),
        }
    }
}

/// A literal duration as a user writes it: `5m`, `1h30m`, `500ms`.
fn duration_literal(secs: f64) -> String {
    #[allow(clippy::cast_possible_truncation)]
    let mut ms = (secs.abs() * 1000.0).round() as i64;
    if ms == 0 {
        return "0s".to_owned();
    }
    let mut out = String::new();
    if secs < 0.0 {
        out.push('-');
    }
    for (unit, size) in [
        ("y", 365 * 86_400_000_i64),
        ("w", 7 * 86_400_000),
        ("d", 86_400_000),
        ("h", 3_600_000),
        ("m", 60_000),
        ("s", 1_000),
        ("ms", 1),
    ] {
        if ms >= size {
            out.push_str(&format!("{}{unit}", ms / size));
            ms %= size;
        }
    }
    out
}

/// A duration inside `[…]`: a literal as written, an expression as is.
struct Dur<'a>(&'a DurationExpr);

impl fmt::Display for Dur<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            DurationExpr::Literal(secs) => f.write_str(&duration_literal(*secs)),
            e => write!(f, "{e}"),
        }
    }
}

/// A duration after `offset`: bare when it is a literal, parenthesised when
/// it is an expression.
struct DurationArg<'a>(&'a DurationExpr);

impl fmt::Display for DurationArg<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            DurationExpr::Literal(secs) => f.write_str(&duration_literal(*secs)),
            e => write!(f, "({e})"),
        }
    }
}

fn labels(list: &[String]) -> String {
    list.join(", ")
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::NumberLiteral(v) => f.write_str(&number(*v)),
            Expr::StringLiteral(s) => f.write_str(&quote(s)),
            Expr::VectorSelector {
                name,
                matchers,
                offset,
                at,
                smoothed,
            } => {
                let shown: Vec<&LabelMatcher> = matchers
                    .iter()
                    .filter(|m| !(name.is_some() && m.name == "__name__" && m.op == MatchOp::Equal))
                    .collect();
                if let Some(n) = name {
                    f.write_str(n)?;
                }
                if !shown.is_empty() || name.is_none() {
                    f.write_str("{")?;
                    for (i, m) in shown.iter().enumerate() {
                        if i > 0 {
                            f.write_str(", ")?;
                        }
                        write!(f, "{m}")?;
                    }
                    f.write_str("}")?;
                }
                if *smoothed {
                    f.write_str(" smoothed")?;
                }
                if let Some(at) = at {
                    write!(f, " {at}")?;
                }
                if let Some(off) = offset {
                    write!(f, " offset {}", DurationArg(off))?;
                }
                Ok(())
            }
            Expr::MatrixSelector {
                vector,
                range,
                modifier,
            } => {
                // `offset` and `@` follow the range: `m[5m] offset 1h`.
                let Expr::VectorSelector {
                    name,
                    matchers,
                    offset,
                    at,
                    smoothed,
                } = vector.as_ref()
                else {
                    return write!(f, "{vector}[{}]", Dur(range));
                };
                let bare = Expr::VectorSelector {
                    name: name.clone(),
                    matchers: matchers.clone(),
                    offset: None,
                    at: None,
                    smoothed: *smoothed,
                };
                write!(f, "{bare}[{}]", Dur(range))?;
                match modifier {
                    Some(RangeModifier::Anchored) => f.write_str(" anchored")?,
                    Some(RangeModifier::Smoothed) => f.write_str(" smoothed")?,
                    None => {}
                }
                if let Some(at) = at {
                    write!(f, " {at}")?;
                }
                if let Some(off) = offset {
                    write!(f, " offset {}", DurationArg(off))?;
                }
                Ok(())
            }
            Expr::Call { func, args } => {
                write!(f, "{func}(")?;
                for (i, a) in args.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{a}")?;
                }
                f.write_str(")")
            }
            Expr::Aggregation {
                op,
                expr,
                param,
                modifier,
            } => {
                write!(f, "{op}")?;
                match modifier {
                    Some(AggregationModifier::By(l)) => write!(f, " by ({})", labels(l))?,
                    Some(AggregationModifier::Without(l)) => {
                        write!(f, " without ({})", labels(l))?;
                    }
                    None => {}
                }
                f.write_str(" (")?;
                if let Some(p) = param {
                    write!(f, "{p}, ")?;
                }
                write!(f, "{expr})")
            }
            Expr::BinaryExpr {
                op,
                lhs,
                rhs,
                bool_mod,
                matching,
            } => {
                write!(f, "{lhs} {op}")?;
                if *bool_mod {
                    f.write_str(" bool")?;
                }
                if let Some(m) = matching {
                    if m.on {
                        write!(f, " on ({})", labels(&m.labels))?;
                    } else if !m.labels.is_empty() {
                        write!(f, " ignoring ({})", labels(&m.labels))?;
                    }
                    match m.card {
                        VectorMatchingCardinality::ManyToOne => {
                            write!(f, " group_left ({})", labels(&m.include))?;
                        }
                        VectorMatchingCardinality::OneToMany => {
                            write!(f, " group_right ({})", labels(&m.include))?;
                        }
                        VectorMatchingCardinality::OneToOne
                        | VectorMatchingCardinality::ManyToMany => {}
                    }
                }
                write!(f, " {rhs}")
            }
            Expr::UnaryExpr {
                op: UnaryOp::Neg,
                expr,
            } => write!(f, "-{expr}"),
            Expr::Paren(inner) => write!(f, "({inner})"),
            Expr::Subquery {
                expr,
                range,
                step,
                offset,
                at,
            } => {
                write!(f, "{expr}[{}:", Dur(range))?;
                if let Some(s) = step {
                    write!(f, "{}", Dur(s))?;
                }
                f.write_str("]")?;
                if let Some(at) = at {
                    write!(f, " {at}")?;
                }
                if let Some(off) = offset {
                    write!(f, " offset {}", DurationArg(off))?;
                }
                Ok(())
            }
        }
    }
}
