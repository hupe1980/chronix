//! `PromQL` abstract syntax tree types.

use std::fmt;

/// Duration in seconds (for range selectors, offsets, etc.).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Duration(pub f64);

impl Duration {
    /// Returns the duration in seconds.
    #[must_use]
    pub fn as_secs(&self) -> f64 {
        self.0
    }

    /// Returns the duration in nanoseconds.
    ///
    /// Clamps to `i64::MIN..=i64::MAX` to avoid undefined saturating
    /// behaviour when the float overflows.
    #[must_use]
    pub fn as_nanos(&self) -> i64 {
        secs_to_nanos_safe(self.0)
    }

    /// Returns the duration in milliseconds.
    ///
    /// Clamps to `i64::MIN..=i64::MAX`.
    #[must_use]
    pub fn as_millis(&self) -> i64 {
        let ms = self.0 * 1_000.0;
        float_to_i64_safe(ms)
    }
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
        offset: Option<Duration>,
        /// Optional `@` modifier, pinning the evaluation instant.
        at: Option<AtModifier>,
    },

    /// A matrix selector: `metric_name{...}[5m]`.
    MatrixSelector {
        /// The vector selector.
        vector: Box<Expr>,
        /// The range duration.
        range: Duration,
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
        range: Duration,
        /// Optional step duration.
        step: Option<Duration>,
        /// Optional offset. Negative shifts forward in time.
        offset: Option<Duration>,
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
    /// Data samples in the series.
    pub samples: Vec<Sample>,
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
