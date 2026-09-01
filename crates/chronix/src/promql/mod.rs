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
pub mod lexer;
pub mod parser;

pub use ast::*;
pub use eval::{compile_label_matchers, label_set_matches, CompiledMatcher, PromQLEvaluator};
pub use parser::parse;
