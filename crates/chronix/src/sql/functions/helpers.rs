//! Shared argument extraction for the scalar and aggregate functions.
//!
//! The window functions have their own, in [`window`](super::window): they
//! receive plain `ArrayRef`s rather than `ColumnarValue`s.

use arrow::datatypes::{TimeUnit, TimestampNanosecondType};
use datafusion::common::Result as DFResult;
#[cfg(test)]
use datafusion::logical_expr::ColumnarValue;

/// Extract nanos from either `Timestamp(Nanosecond)` or `Int64` columns.
pub(super) fn extract_timestamps(
    arr: &arrow::array::ArrayRef,
) -> DFResult<arrow::array::Int64Array> {
    use arrow::array::{Array, AsArray, Int64Array};
    use arrow::datatypes::DataType;

    match arr.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let ts = arr.as_primitive::<TimestampNanosecondType>();
            // Reinterpret as i64 — same underlying representation
            Ok(Int64Array::from(ts.iter().collect::<Vec<_>>()))
        }
        DataType::Int64 => {
            let i = arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                datafusion::common::DataFusionError::Internal("expected Int64".into())
            })?;
            Ok(i.clone())
        }
        other => Err(datafusion::common::DataFusionError::Plan(format!(
            "expected timestamp or int64, got {other}"
        ))),
    }
}

/// Invoke a scalar UDF the way DataFusion does, for tests.
///
/// DataFusion 55 replaced `invoke_batch(&[ColumnarValue], usize)` with
/// `invoke_with_args(ScalarFunctionArgs)`, which carries the argument fields,
/// the resolved return field and the session config alongside the values.
/// Tests only care about the values and the row count, so this builds the rest
/// from the UDF's own `return_type` — the same way the physical planner does.
#[cfg(test)]
pub(super) fn invoke_udf(
    udf: &dyn datafusion::logical_expr::ScalarUDFImpl,
    args: Vec<ColumnarValue>,
    number_rows: usize,
) -> DFResult<ColumnarValue> {
    use std::sync::Arc;

    use arrow::datatypes::Field;
    use datafusion::logical_expr::ScalarFunctionArgs;

    let arg_types: Vec<_> = args.iter().map(ColumnarValue::data_type).collect();
    let return_type = udf.return_type(&arg_types)?;
    let arg_fields = arg_types
        .iter()
        .enumerate()
        .map(|(i, t)| Arc::new(Field::new(format!("arg{i}"), t.clone(), true)))
        .collect();

    udf.invoke_with_args(ScalarFunctionArgs {
        args,
        arg_fields,
        number_rows,
        return_field: Arc::new(Field::new("result", return_type, true)),
        config_options: Arc::new(datafusion::config::ConfigOptions::default()),
    })
}
