//! `DataFusion` `TableProvider` for a single Chronix measurement.
//!
//! Registers a Chronix measurement as a `DataFusion` table with:
//! - `_time` column as `Timestamp(Nanosecond)` for SQL ergonomics
//! - Tag columns as `Utf8`
//! - Field columns with their native types
//!
//! Supports predicate pushdown for time range and tag equality filters.

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;

use chronix_core::{ColumnRole, ColumnType, MeasurementSchema};

use super::exec::ChronixExec;
use crate::Chronix;

/// `DataFusion` `TableProvider` backed by a single Chronix measurement.
///
/// Created automatically by [`ChronixSchemaProvider`](super::ChronixSchemaProvider)
/// when `DataFusion` resolves table names. Supports predicate pushdown for
/// time range and tag equality filters.
pub struct ChronixTableProvider {
    db: Arc<Chronix>,
    measurement: String,
    schema: SchemaRef,
    /// Namespace this table is scoped to, if any.
    ///
    /// When set, every scan carries a mandatory `__namespace__` tag filter
    /// that no SQL text can remove — the filter is applied by the provider,
    /// not derived from the query. Scoping the *provider* rather than
    /// rewriting each plan is what makes the guarantee structural: a table
    /// reached through a scoped context cannot read another namespace's rows
    /// however the query is phrased.
    namespace: Option<String>,
}

impl fmt::Debug for ChronixTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChronixTableProvider")
            .field("measurement", &self.measurement)
            .finish()
    }
}

impl ChronixTableProvider {
    /// Create a new provider for the given measurement.
    ///
    /// # Errors
    ///
    /// Returns an error if the measurement does not exist.
    pub fn try_new(db: Arc<Chronix>, measurement: &str) -> Result<Self, DataFusionError> {
        Self::try_new_scoped(db, measurement, None)
    }

    /// Create a provider scoped to `namespace`.
    ///
    /// # Errors
    ///
    /// Returns an error if the measurement does not exist.
    pub fn try_new_scoped(
        db: Arc<Chronix>,
        measurement: &str,
        namespace: Option<String>,
    ) -> Result<Self, DataFusionError> {
        let ms = db.schema(measurement).ok_or_else(|| {
            DataFusionError::Plan(format!("measurement '{measurement}' not found"))
        })?;
        let schema = measurement_schema_to_arrow(&ms);
        Ok(Self {
            db,
            measurement: measurement.to_string(),
            schema,
            namespace,
        })
    }

    /// The namespace this table is scoped to, if any.
    #[must_use]
    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    /// The measurement name backing this table.
    #[must_use]
    pub fn measurement(&self) -> &str {
        &self.measurement
    }
}

/// Convert a Chronix [`MeasurementSchema`] to an Arrow [`Schema`] for `DataFusion`.
///
/// Timestamp column → `_time` with `Timestamp(Nanosecond, None)`
/// Tag columns → `Utf8`
/// Field columns → native Arrow types
///
/// # Column order
///
/// Columns are emitted in Chronix's **canonical** order — timestamp first,
/// then tags sorted by name, then fields sorted by name — which is the same
/// order the storage layer produces record batches in
/// (`chronix_query::convert`).
///
/// This deliberately does *not* follow `MeasurementSchema::columns()`
/// registration order. Registration order depends on the accident of which
/// field happened to be written first, so it (a) made a measurement's SQL
/// column order a function of its write history and (b) disagreed with the
/// storage batch order whenever fields were registered non-alphabetically,
/// which previously caused projected queries to return the wrong column's
/// data.
#[must_use]
pub fn measurement_schema_to_arrow(ms: &MeasurementSchema) -> SchemaRef {
    // The namespace tag is an internal marker, not user data: it must not
    // appear in `SELECT *`, in `DESCRIBE`, or in the schema a client reads
    // back. Dropping it here also means no SQL text can reference it, so the
    // mandatory filter a scoped provider applies cannot be contradicted.
    let mut columns: Vec<_> = ms
        .columns()
        .iter()
        .filter(|col| col.name != chronix_core::NAMESPACE_TAG)
        .collect();
    columns.sort_by_key(|col| {
        let group = match col.role {
            ColumnRole::Timestamp => 0,
            ColumnRole::Tag => 1,
            ColumnRole::Field => 2,
        };
        (group, col.name.as_str())
    });

    let fields: Vec<Field> = columns
        .into_iter()
        .map(|col| match col.role {
            ColumnRole::Timestamp => Field::new(
                "_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            ColumnRole::Tag => Field::new(&col.name, DataType::Utf8, true),
            ColumnRole::Field => {
                let dt = match col.column_type {
                    ColumnType::F64 => DataType::Float64,
                    ColumnType::I64 => DataType::Int64,
                    ColumnType::U64 => DataType::UInt64,
                    ColumnType::Bool => DataType::Boolean,
                    ColumnType::String => DataType::Utf8,
                    ColumnType::Timestamp => {
                        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
                    }
                };
                Field::new(&col.name, dt, true)
            }
        })
        .collect();
    Arc::new(Schema::new(fields))
}

// ── Predicate pushdown ──────────────────────────────────────────────────

struct PushdownPredicates {
    time_start: i64,
    time_end: i64,
    tag_filters: Vec<(String, String)>,
    field_predicates: Vec<chronix_engine::segment::FieldPredicate>,
}

fn extract_pushdown(filters: &[Expr], db: &Chronix, measurement: &str) -> PushdownPredicates {
    let mut result = PushdownPredicates {
        time_start: i64::MIN,
        time_end: i64::MAX,
        tag_filters: Vec::new(),
        field_predicates: Vec::new(),
    };
    let ms = db.schema(measurement);
    for filter in filters {
        extract_from_expr(filter, &mut result, ms.as_deref());
    }
    result
}

fn extract_from_expr(expr: &Expr, result: &mut PushdownPredicates, ms: Option<&MeasurementSchema>) {
    use datafusion::logical_expr::Operator;

    if let Expr::BinaryExpr(be) = expr {
        // Recurse into AND
        if be.op == Operator::And {
            extract_from_expr(&be.left, result, ms);
            extract_from_expr(&be.right, result, ms);
            return;
        }

        // Column op Literal
        if let (Some(col_name), Some(scalar)) = (column_name(&be.left), scalar_value(&be.right)) {
            apply_predicate(&col_name, be.op, &scalar, result, ms);
        }
        // Literal op Column (reversed)
        else if let (Some(scalar), Some(col_name)) =
            (scalar_value(&be.left), column_name(&be.right))
        {
            if let Some(rop) = reverse_op(be.op) {
                apply_predicate(&col_name, rop, &scalar, result, ms);
            }
        }
    }
}

fn column_name(expr: &Expr) -> Option<String> {
    if let Expr::Column(col) = expr {
        Some(col.name().to_string())
    } else {
        None
    }
}

fn scalar_value(expr: &Expr) -> Option<datafusion::common::ScalarValue> {
    if let Expr::Literal(sv, _metadata) = expr {
        Some(sv.clone())
    } else {
        None
    }
}

fn reverse_op(
    op: datafusion::logical_expr::Operator,
) -> Option<datafusion::logical_expr::Operator> {
    use datafusion::logical_expr::Operator;
    match op {
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        Operator::Eq => Some(Operator::Eq),
        _ => None,
    }
}

fn apply_predicate(
    col_name: &str,
    op: datafusion::logical_expr::Operator,
    scalar: &datafusion::common::ScalarValue,
    result: &mut PushdownPredicates,
    ms: Option<&MeasurementSchema>,
) {
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;

    // Time predicates
    if col_name == "_time" || col_name == "timestamp" || col_name == "time" {
        let nanos = match scalar {
            ScalarValue::Int64(Some(v)) => Some(*v),
            ScalarValue::TimestampNanosecond(Some(v), _) => Some(*v),
            _ => None,
        };
        if let Some(ts) = nanos {
            match op {
                Operator::GtEq => result.time_start = result.time_start.max(ts),
                Operator::Gt => result.time_start = result.time_start.max(ts.saturating_add(1)),
                Operator::LtEq => result.time_end = result.time_end.min(ts),
                Operator::Lt => result.time_end = result.time_end.min(ts.saturating_sub(1)),
                Operator::Eq => {
                    result.time_start = result.time_start.max(ts);
                    result.time_end = result.time_end.min(ts);
                }
                _ => {}
            }
        }
    }
    // Tag equality predicates
    else if let Some(ms) = ms {
        if let ScalarValue::Utf8(Some(val)) = scalar {
            if op == Operator::Eq
                && ms
                    .column(col_name)
                    .is_some_and(|c| c.role == ColumnRole::Tag)
            {
                result.tag_filters.push((col_name.to_string(), val.clone()));
            }
        }
        // Numeric field predicates — zone-map pushdown
        else if ms
            .column(col_name)
            .is_some_and(|c| c.role == ColumnRole::Field)
        {
            let f64_val = match scalar {
                ScalarValue::Float64(Some(v)) => Some(*v),
                ScalarValue::Int64(Some(v)) => Some(*v as f64),
                ScalarValue::UInt64(Some(v)) => Some(*v as f64),
                _ => None,
            };
            let zm_op = match op {
                Operator::Eq => Some(chronix_engine::segment::ZoneMapOp::Eq),
                Operator::Gt => Some(chronix_engine::segment::ZoneMapOp::Gt),
                Operator::GtEq => Some(chronix_engine::segment::ZoneMapOp::GtEq),
                Operator::Lt => Some(chronix_engine::segment::ZoneMapOp::Lt),
                Operator::LtEq => Some(chronix_engine::segment::ZoneMapOp::LtEq),
                _ => None,
            };
            if let (Some(val), Some(zop)) = (f64_val, zm_op) {
                result
                    .field_predicates
                    .push(chronix_engine::segment::FieldPredicate {
                        column: col_name.to_string(),
                        op: zop,
                        value: val,
                    });
            }
        }
    }
}

fn classify_filter(expr: &Expr, ms: Option<&MeasurementSchema>) -> TableProviderFilterPushDown {
    use datafusion::logical_expr::Operator;

    match expr {
        Expr::BinaryExpr(be) => {
            if be.op == Operator::And {
                let left = classify_filter(&be.left, ms);
                let right = classify_filter(&be.right, ms);
                return match (left, right) {
                    (TableProviderFilterPushDown::Exact, TableProviderFilterPushDown::Exact) => {
                        TableProviderFilterPushDown::Exact
                    }
                    (TableProviderFilterPushDown::Unsupported, _)
                    | (_, TableProviderFilterPushDown::Unsupported) => {
                        TableProviderFilterPushDown::Inexact
                    }
                    _ => TableProviderFilterPushDown::Inexact,
                };
            }
            let col = column_name(&be.left).or_else(|| column_name(&be.right));
            if let Some(name) = col {
                if name == "_time" || name == "timestamp" || name == "time" {
                    return TableProviderFilterPushDown::Exact;
                }
                if let Some(ms) = ms {
                    if ms.column(&name).is_some_and(|c| c.role == ColumnRole::Tag)
                        && be.op == Operator::Eq
                    {
                        return TableProviderFilterPushDown::Exact;
                    }
                    // Zone-map pushdown for numeric field predicates.
                    if ms
                        .column(&name)
                        .is_some_and(|c| c.role == ColumnRole::Field)
                        && matches!(
                            be.op,
                            Operator::Eq
                                | Operator::Gt
                                | Operator::GtEq
                                | Operator::Lt
                                | Operator::LtEq
                        )
                    {
                        return TableProviderFilterPushDown::Inexact;
                    }
                }
            }
            TableProviderFilterPushDown::Unsupported
        }
        _ => TableProviderFilterPushDown::Unsupported,
    }
}

#[async_trait]
impl TableProvider for ChronixTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut pushdown = extract_pushdown(filters, &self.db, &self.measurement);
        // Mandatory namespace scope, applied after user predicates so it can
        // neither be dropped nor overridden by them.
        if let Some(ref ns) = self.namespace {
            pushdown
                .tag_filters
                .push((chronix_core::NAMESPACE_TAG.to_string(), ns.clone()));
        }
        Ok(Arc::new(ChronixExec::new(
            self.db.clone(),
            self.measurement.clone(),
            self.schema.clone(),
            projection.map(Vec::as_slice),
            pushdown.time_start,
            pushdown.time_end,
            pushdown.tag_filters,
            pushdown.field_predicates,
            limit,
        )?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        let ms = self.db.schema(&self.measurement);
        Ok(filters
            .iter()
            .map(|f| classify_filter(f, ms.as_deref()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_conversion_produces_correct_arrow_types() {
        let mut ms = MeasurementSchema::new("cpu");
        let _ = ms.add_tag("host");
        ms.add_field("usage_idle", &chronix_core::FieldValue::F64(0.0))
            .unwrap();
        ms.add_field("count", &chronix_core::FieldValue::I64(0))
            .unwrap();

        let schema = measurement_schema_to_arrow(&ms);
        assert_eq!(schema.fields().len(), 4);

        let time_field = schema.field(0);
        assert_eq!(time_field.name(), "_time");
        assert_eq!(
            *time_field.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        let host = schema.field(1);
        assert_eq!(host.name(), "host");
        assert_eq!(*host.data_type(), DataType::Utf8);

        // Fields come out sorted by name, not in registration order
        // ("usage_idle" was registered first but sorts after "count").
        let count = schema.field(2);
        assert_eq!(count.name(), "count");
        assert_eq!(*count.data_type(), DataType::Int64);

        let usage = schema.field(3);
        assert_eq!(usage.name(), "usage_idle");
        assert_eq!(*usage.data_type(), DataType::Float64);
    }

    /// The Arrow schema must be in Chronix's canonical column order
    /// (timestamp, tags sorted, fields sorted) so that it agrees positionally
    /// with the record batches the storage layer produces.
    #[test]
    fn schema_conversion_is_canonically_ordered() {
        let mut ms = MeasurementSchema::new("m");
        // Register tags and fields in deliberately unsorted order.
        let _ = ms.add_tag("zone");
        let _ = ms.add_tag("host");
        ms.add_field("zeta", &chronix_core::FieldValue::F64(0.0))
            .unwrap();
        ms.add_field("alpha", &chronix_core::FieldValue::F64(0.0))
            .unwrap();

        let schema = measurement_schema_to_arrow(&ms);
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["_time", "host", "zone", "alpha", "zeta"]);
    }
}
