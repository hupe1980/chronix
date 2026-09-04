//! `PromQL` query engine for Chronix.
//!
//! Provides a parser and evaluator implementing a substantial subset of the
//! Prometheus Query Language (`PromQL`), enabling Grafana Prometheus data-source
//! compatibility.
//!
//! # Architecture
//!
//! ```text
//! input string → Lexer → tokens → Parser → AST → Evaluator → result vectors
//! ```

pub mod ast;
pub mod eval;
mod lexer;
mod parser;

// Named rather than globbed: `pub use ast::*` makes every future item in
// `ast` part of this crate's public API by default, which is the opposite of
// how a surface should grow.
pub use ast::{
    AggregationModifier, AggregationOp, AtModifier, BinaryOp, Duration, Expr, LabelMatcher,
    MatchOp, PromQLValue, Sample, Series, UnaryOp, VectorMatching, VectorMatchingCardinality,
};
pub use eval::{compile_label_matchers, label_set_matches, CompiledMatcher, PromQLEvaluator};
pub use parser::{parse, ParseError};
