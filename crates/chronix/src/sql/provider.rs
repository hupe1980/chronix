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
pub(crate) struct ChronixTableProvider {
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
    /// Create a provider for `measurement`, scoped to `namespace`.
    ///
    /// # Errors
    ///
    /// Returns an error if the measurement does not exist.
    pub(crate) fn try_new_scoped(
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
/// which makes projected queries return the wrong column's data.
#[must_use]
pub fn measurement_schema_to_arrow(ms: &MeasurementSchema) -> SchemaRef {
    // The namespace tag is an internal marker, not user data: it must not
    // appear in `SELECT *`, in `DESCRIBE`, or in the schema a client reads
    // back. Dropping it here also means no SQL text can reference it, so the
    // mandatory filter a scoped provider applies cannot be contradicted.
    schema_to_arrow(ms, false)
}

/// The same schema, but keeping the namespace tag.
///
/// Used by the cold archive.
///
/// Used for the cold archive. A SQL session is already scoped to one tenant,
/// so the hot table hides the tag; an archive object covers every tenant in a
/// shard, and dropping the only column that says whose row this is would be
/// data loss the moment two tenants share a measurement name.
#[cfg(feature = "object-store")]
#[must_use]
pub(crate) fn measurement_schema_to_archive_arrow(ms: &MeasurementSchema) -> SchemaRef {
    schema_to_arrow(ms, true)
}

fn schema_to_arrow(ms: &MeasurementSchema, keep_namespace: bool) -> SchemaRef {
    let mut columns: Vec<_> = ms
        .columns()
        .iter()
        .filter(|col| keep_namespace || col.name != chronix_core::NAMESPACE_TAG)
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

/// What the engine can do with one `column op literal` predicate.
///
/// This is the **single** decision, and both the classifier DataFusion asks
/// and the code that builds the scan read it. They used to decide
/// separately, and disagreed: the classifier answered `Exact` for every
/// operator on a time column and for every tag equality, while the builder
/// applied only five operators and only `Utf8` literals. DataFusion
/// *removes* an Exact filter from the plan, so `WHERE _time <> X` and
/// `WHERE host = region` were dropped by the planner and never applied by
/// anyone — the query returned rows its own predicate excluded.
enum Pushdown {
    /// Fully applied by the engine; DataFusion may drop the filter.
    Exact(PushdownTerm),
    /// Applied as a pruning hint; DataFusion must still evaluate it.
    Hint(chronix_engine::segment::FieldPredicate),
    /// Not applied at all.
    None,
}

/// A predicate the engine applies exactly.
enum PushdownTerm {
    /// Narrow the scan's time range.
    Time {
        /// Inclusive lower bound, if this predicate sets one.
        start: Option<i64>,
        /// Inclusive upper bound, if this predicate sets one.
        end: Option<i64>,
    },
    /// Require a tag to equal a value.
    Tag(String, String),
}

/// Decide what the engine does with `col_name op scalar`.
fn plan_predicate(
    col_name: &str,
    op: datafusion::logical_expr::Operator,
    scalar: &datafusion::common::ScalarValue,
    ms: Option<&MeasurementSchema>,
) -> Pushdown {
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;

    if col_name == "_time" || col_name == "timestamp" || col_name == "time" {
        // Only a literal that is genuinely a nanosecond instant, and only
        // the five range operators. `<>`, `IS DISTINCT FROM` and a
        // non-literal right-hand side are somebody else's job.
        let ts = match scalar {
            ScalarValue::Int64(Some(v)) => *v,
            ScalarValue::TimestampNanosecond(Some(v), _) => *v,
            _ => return Pushdown::None,
        };
        return match op {
            Operator::GtEq => Pushdown::Exact(PushdownTerm::Time {
                start: Some(ts),
                end: None,
            }),
            Operator::Gt => Pushdown::Exact(PushdownTerm::Time {
                start: Some(ts.saturating_add(1)),
                end: None,
            }),
            Operator::LtEq => Pushdown::Exact(PushdownTerm::Time {
                start: None,
                end: Some(ts),
            }),
            Operator::Lt => Pushdown::Exact(PushdownTerm::Time {
                start: None,
                end: Some(ts.saturating_sub(1)),
            }),
            Operator::Eq => Pushdown::Exact(PushdownTerm::Time {
                start: Some(ts),
                end: Some(ts),
            }),
            _ => Pushdown::None,
        };
    }

    let Some(ms) = ms else {
        return Pushdown::None;
    };
    let Some(column) = ms.column(col_name) else {
        return Pushdown::None;
    };

    if column.role == ColumnRole::Tag {
        // Tag equality against a string literal, and nothing else: the
        // engine's tag filter is an exact string match.
        if op == Operator::Eq {
            if let ScalarValue::Utf8(Some(val)) = scalar {
                return Pushdown::Exact(PushdownTerm::Tag(col_name.to_string(), val.clone()));
            }
        }
        return Pushdown::None;
    }

    if column.role == ColumnRole::Field {
        #[allow(clippy::cast_precision_loss)] // a zone-map bound, not a value
        let value = match scalar {
            ScalarValue::Float64(Some(v)) => *v,
            ScalarValue::Int64(Some(v)) => *v as f64,
            ScalarValue::UInt64(Some(v)) => *v as f64,
            _ => return Pushdown::None,
        };
        let zm_op = match op {
            Operator::Eq => chronix_engine::segment::ZoneMapOp::Eq,
            Operator::Gt => chronix_engine::segment::ZoneMapOp::Gt,
            Operator::GtEq => chronix_engine::segment::ZoneMapOp::GtEq,
            Operator::Lt => chronix_engine::segment::ZoneMapOp::Lt,
            Operator::LtEq => chronix_engine::segment::ZoneMapOp::LtEq,
            _ => return Pushdown::None,
        };
        // A zone map prunes row groups; it does not filter rows, so the
        // filter still has to be evaluated.
        return Pushdown::Hint(chronix_engine::segment::FieldPredicate {
            column: col_name.to_string(),
            op: zm_op,
            value,
        });
    }

    Pushdown::None
}

fn apply_predicate(
    col_name: &str,
    op: datafusion::logical_expr::Operator,
    scalar: &datafusion::common::ScalarValue,
    result: &mut PushdownPredicates,
    ms: Option<&MeasurementSchema>,
) {
    match plan_predicate(col_name, op, scalar, ms) {
        Pushdown::Exact(PushdownTerm::Time { start, end }) => {
            if let Some(s) = start {
                result.time_start = result.time_start.max(s);
            }
            if let Some(e) = end {
                result.time_end = result.time_end.min(e);
            }
        }
        Pushdown::Exact(PushdownTerm::Tag(name, value)) => {
            result.tag_filters.push((name, value));
        }
        Pushdown::Hint(pred) => result.field_predicates.push(pred),
        Pushdown::None => {}
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
            // Ask the same function the scan builder asks, with the same
            // operands, so the answer cannot differ from what is applied.
            let (name, op, scalar) = match (
                column_name(&be.left),
                scalar_value(&be.right),
                column_name(&be.right),
                scalar_value(&be.left),
            ) {
                (Some(name), Some(scalar), _, _) => (name, be.op, scalar),
                // `literal op column` — the operator flips with the operands.
                (_, _, Some(name), Some(scalar)) => match reverse_op(be.op) {
                    Some(op) => (name, op, scalar),
                    None => return TableProviderFilterPushDown::Unsupported,
                },
                _ => return TableProviderFilterPushDown::Unsupported,
            };
            match plan_predicate(&name, op, &scalar, ms) {
                Pushdown::Exact(_) => TableProviderFilterPushDown::Exact,
                Pushdown::Hint(_) => TableProviderFilterPushDown::Inexact,
                Pushdown::None => TableProviderFilterPushDown::Unsupported,
            }
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
