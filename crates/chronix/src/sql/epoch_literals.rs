//! Comparing a timestamp column with an epoch integer.
//!
//! Chronix's native API speaks nanoseconds since the Unix epoch: `insert`
//! takes an `i64`, `range` takes two, every wire protocol carries them, and
//! every example prints them. So the first time predicate a user writes in
//! SQL is the one they already have the numbers for:
//!
//! ```sql
//! SELECT * FROM cpu WHERE _time >= 1700000000000000000
//! ```
//!
//! DataFusion rejects it. `_time` is `Timestamp(Nanosecond)`, the literal is
//! `Int64`, and its type coercion has no common type for the two — the error
//! is `Cannot infer common argument type for comparison operation
//! Timestamp(ns) >= Int64`, and `BETWEEN` fails worse, with an internal
//! "this is likely a bug in DataFusion" message. The workaround
//! (`to_timestamp_nanos(...)`) is not one anybody guesses.
//!
//! This rule rewrites the literal instead, before coercion runs. It is
//! deliberately narrow: only a comparison, only where one side is a
//! timestamp-typed expression and the other an integer literal.
//!
//! The scale is **the column's own unit**, which is the only reading that
//! round-trips: a value this database handed out in nanoseconds compares
//! equal to itself.

use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator};
use datafusion::optimizer::AnalyzerRule;

/// Rewrites integer literals compared against timestamp columns.
#[derive(Debug, Default)]
pub struct EpochLiteralRule;

impl AnalyzerRule for EpochLiteralRule {
    fn name(&self) -> &str {
        "chronix_epoch_literals"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| {
            let schema = Arc::clone(plan.schema());
            plan.map_expressions(|expr| expr.transform_up(|e| rewrite_comparison(e, &schema)))
        })
        .map(|t| t.data)
    }
}

/// Rewrite one comparison, if it pairs a timestamp with an integer literal.
fn rewrite_comparison(
    expr: Expr,
    schema: &datafusion::common::DFSchema,
) -> Result<Transformed<Expr>> {
    // `BETWEEN` and `IN` are their own nodes, not sugar over `BinaryExpr`,
    // so they need handling of their own — and `BETWEEN` failed the worst of
    // the three, with an internal "this is likely a bug in DataFusion"
    // message rather than a type error.
    match &expr {
        Expr::Between(between) => {
            let Some(unit) = time_unit_of(&between.expr, schema) else {
                return Ok(Transformed::no(expr));
            };
            let low = as_integer(&between.low).map(|v| timestamp_scalar(v, unit));
            let high = as_integer(&between.high).map(|v| timestamp_scalar(v, unit));
            if low.is_none() && high.is_none() {
                return Ok(Transformed::no(expr));
            }
            let mut rewritten = between.clone();
            if let Some(low) = low {
                rewritten.low = Box::new(Expr::Literal(low, None));
            }
            if let Some(high) = high {
                rewritten.high = Box::new(Expr::Literal(high, None));
            }
            return Ok(Transformed::yes(Expr::Between(rewritten)));
        }
        Expr::InList(in_list) => {
            let Some(unit) = time_unit_of(&in_list.expr, schema) else {
                return Ok(Transformed::no(expr));
            };
            if !in_list.list.iter().any(|e| as_integer(e).is_some()) {
                return Ok(Transformed::no(expr));
            }
            let mut rewritten = in_list.clone();
            rewritten.list = in_list
                .list
                .iter()
                .map(|e| match as_integer(e) {
                    Some(v) => Expr::Literal(timestamp_scalar(v, unit), None),
                    None => e.clone(),
                })
                .collect();
            return Ok(Transformed::yes(Expr::InList(rewritten)));
        }
        _ => {}
    }

    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = &expr else {
        return Ok(Transformed::no(expr));
    };
    if !is_comparison(*op) {
        return Ok(Transformed::no(expr));
    }

    // One side timestamp-typed, the other an integer literal.
    let converted = match (time_unit_of(left, schema), as_integer(right)) {
        (Some(unit), Some(v)) => Some(Expr::BinaryExpr(BinaryExpr {
            left: left.clone(),
            op: *op,
            right: Box::new(Expr::Literal(timestamp_scalar(v, unit), None)),
        })),
        _ => match (as_integer(left), time_unit_of(right, schema)) {
            (Some(v), Some(unit)) => Some(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Literal(timestamp_scalar(v, unit), None)),
                op: *op,
                right: right.clone(),
            })),
            _ => None,
        },
    };

    Ok(match converted {
        Some(e) => Transformed::yes(e),
        None => Transformed::no(expr),
    })
}

/// The operators worth rewriting.
///
/// Arithmetic is left alone: `_time + 1` is an interval question, not an
/// epoch one, and silently reinterpreting the operand would be a guess.
const fn is_comparison(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
    )
}

/// The time unit of `expr`, when it is a timestamp.
fn time_unit_of(expr: &Expr, schema: &datafusion::common::DFSchema) -> Option<TimeUnit> {
    match expr.get_type(schema) {
        Ok(DataType::Timestamp(unit, _)) => Some(unit),
        _ => None,
    }
}

/// The value of `expr`, when it is an integer literal.
///
/// Only the signed and unsigned integer literals a SQL parser produces for a
/// bare number. A float is excluded on purpose: `1.7e18` is not a value
/// anybody types meaning an instant, and it cannot represent one exactly.
fn as_integer(expr: &Expr) -> Option<i64> {
    let Expr::Literal(scalar, _) = expr else {
        return None;
    };
    match scalar {
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::Int32(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int8(Some(v)) => Some(i64::from(*v)),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok(),
        ScalarValue::UInt32(Some(v)) => Some(i64::from(*v)),
        ScalarValue::UInt16(Some(v)) => Some(i64::from(*v)),
        ScalarValue::UInt8(Some(v)) => Some(i64::from(*v)),
        _ => None,
    }
}

/// The literal to compare against, in the column's own unit.
fn timestamp_scalar(value: i64, unit: TimeUnit) -> ScalarValue {
    match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(value), None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(value), None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(value), None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(value), None),
    }
}
