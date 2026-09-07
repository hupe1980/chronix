//! Schema registry with schema-on-write and additive evolution.
//!
//! The schema registry tracks the column definitions for each measurement.
//! Schemas are created automatically on first write and evolve additively:
//! new fields/tags are added automatically, but type changes are rejected.
//!
//! # Schema Evolution Strategy
//!
//! Chronix uses a **schema-on-write** approach to schema evolution:
//!
//! - **Automatic creation**: The first write to a measurement creates the
//!   schema with the timestamp column plus all tags and fields from that point.
//! - **Additive evolution**: Subsequent writes may introduce new tag or field
//!   columns.  These are appended to the schema automatically — no DDL
//!   statement is required.
//! - **Type safety**: If a field name already exists with a different type,
//!   the write is rejected with [`SchemaError::TypeConflict`].
//! - **Durability**: the catalog manifest is the only durable record of a
//!   schema; the database persists every action `register_batch` returns
//!   before it writes the data that needs it
//! - **Query-time safety**: Queries that reference a column not present in
//!   older segments return zero rows for those segments (no panic).
//!
//! ## Current Limitations
//!
//! - **No column removal**: Once a tag or field is added, it cannot be
//!   dropped.  Obsolete columns simply receive null values in new writes.
//! - **No column rename**: Renaming requires writing a migration that copies
//!   data from the old column to a new one and stops writing the old name.
//! - **No type change**: A field's type is immutable once established.  To
//!   change a field from `i64` to `f64`, create a new field name.
//! - **No column reorder**: Columns are appended in arrival order.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::error::SchemaError;
use crate::types::{FieldValue, SeriesKey};

/// The role of a column within a measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnRole {
    /// The timestamp column (exactly one per measurement).
    Timestamp,
    /// A tag column (string-typed, indexed for filtering).
    Tag,
    /// A field/value column (typed, stores the actual metrics).
    Field,
}

impl fmt::Display for ColumnRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timestamp => write!(f, "timestamp"),
            Self::Tag => write!(f, "tag"),
            Self::Field => write!(f, "field"),
        }
    }
}

/// The data type of a column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnType {
    /// Nanosecond timestamp (i64).
    Timestamp,
    /// UTF-8 string (for tags and string fields).
    String,
    /// 64-bit floating point.
    F64,
    /// Signed 64-bit integer.
    I64,
    /// Unsigned 64-bit integer.
    U64,
    /// Boolean.
    Bool,
    /// Exact fixed-point decimal with a fixed number of fractional digits.
    ///
    /// The scale is part of the column's type, not of each value, and it is
    /// fixed the first time the column is written (or declared). Every later
    /// write is rescaled to it losslessly — `1.5` into a scale-4 column is
    /// stored as `1.5000` — and refused if that would drop a digit.
    ///
    /// # Why the scale cannot drift
    ///
    /// A decimal column is read back as Arrow `Decimal128(38, scale)`. Two
    /// segments of the same column whose scales differ would produce two
    /// incompatible Arrow schemas, and the batches from a scan that touched
    /// both could not be concatenated. Fixing the scale at the column makes
    /// that unrepresentable rather than a read-time failure.
    Decimal {
        /// Digits after the decimal point, `0..=38`.
        scale: u8,
    },
}

impl ColumnType {
    /// Derive the column type from a [`FieldValue`].
    ///
    /// For a decimal this reports the *value's* scale, which is what a
    /// column created by this value would carry. Use
    /// [`accepts_value`](Self::accepts_value) to ask whether an existing
    /// column can store it.
    #[inline]
    #[must_use]
    pub fn from_field_value(value: &FieldValue) -> Self {
        match value {
            FieldValue::F64(_) => Self::F64,
            FieldValue::I64(_) => Self::I64,
            FieldValue::U64(_) => Self::U64,
            FieldValue::Bool(_) => Self::Bool,
            FieldValue::String(_) => Self::String,
            FieldValue::Decimal(d) => Self::Decimal { scale: d.scale() },
        }
    }

    /// Can a column of this type store `value` without losing anything?
    ///
    /// Every type but [`Decimal`](Self::Decimal) is a plain type-identity
    /// check. A decimal column accepts any value that
    /// [`rescale`](crate::Decimal::rescale)s to the column's scale exactly —
    /// so `1.5` and `1.50000` both fit a scale-4 column, and `1.50001` does
    /// not.
    #[must_use]
    pub fn accepts_value(&self, value: &FieldValue) -> bool {
        match (self, value) {
            (Self::Decimal { scale }, FieldValue::Decimal(d)) => d.rescale(*scale).is_ok(),
            _ => *self == Self::from_field_value(value),
        }
    }

    /// The scale of a decimal column, or `None` for every other type.
    #[inline]
    #[must_use]
    pub const fn decimal_scale(&self) -> Option<u8> {
        match self {
            Self::Decimal { scale } => Some(*scale),
            _ => None,
        }
    }
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timestamp => write!(f, "timestamp"),
            Self::String => write!(f, "string"),
            Self::F64 => write!(f, "f64"),
            Self::I64 => write!(f, "i64"),
            Self::U64 => write!(f, "u64"),
            Self::Bool => write!(f, "bool"),
            // The precision is fixed at 38 for every decimal column, so the
            // scale is the only part worth printing — but printing it in
            // SQL's own `decimal(p,s)` shape keeps the error message and the
            // `SHOW COLUMNS` output readable as a type.
            Self::Decimal { scale } => {
                write!(f, "decimal({}, {scale})", crate::decimal::DECIMAL_PRECISION)
            }
        }
    }
}

impl std::str::FromStr for ColumnType {
    type Err = SchemaError;

    /// Parse the form [`Display`](fmt::Display) produces, plus the longer
    /// aliases the HTTP schema endpoint reports (`float64`, `int64`,
    /// `uint64`, `boolean`).
    ///
    /// A parser exists so that a column type can travel as text — a schema
    /// response a client reads and a declaration it posts back are then the
    /// same vocabulary, rather than one shape to read and another to write.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lower = s.trim().to_ascii_lowercase();
        match lower.as_str() {
            "timestamp" => return Ok(Self::Timestamp),
            "string" | "utf8" => return Ok(Self::String),
            "f64" | "float64" | "double" => return Ok(Self::F64),
            "i64" | "int64" => return Ok(Self::I64),
            "u64" | "uint64" => return Ok(Self::U64),
            "bool" | "boolean" => return Ok(Self::Bool),
            "decimal" => return Ok(Self::Decimal { scale: 0 }),
            _ => {}
        }

        // `decimal(38, 4)` — the precision is fixed, so it is checked
        // rather than honoured: a caller who writes another one is asking
        // for something this format cannot store, and should be told so.
        let invalid = || SchemaError::InvalidName {
            name: s.to_string(),
            reason: "not a column type: expected timestamp, string, f64, i64, u64, bool, \
                     or decimal(38, <scale>)"
                .to_string(),
        };
        let args = lower
            .strip_prefix("decimal(")
            .and_then(|rest| rest.strip_suffix(')'))
            .ok_or_else(invalid)?;
        let (precision, scale) = args.split_once(',').ok_or_else(invalid)?;
        let precision: u8 = precision.trim().parse().map_err(|_| invalid())?;
        if precision != crate::decimal::DECIMAL_PRECISION {
            return Err(SchemaError::InvalidName {
                name: s.to_string(),
                reason: format!(
                    "decimal precision must be {}, the only one this format stores",
                    crate::decimal::DECIMAL_PRECISION
                ),
            });
        }
        let scale: u8 = scale.trim().parse().map_err(|_| invalid())?;
        if scale > crate::decimal::MAX_DECIMAL_SCALE {
            return Err(SchemaError::InvalidName {
                name: s.to_string(),
                reason: format!(
                    "decimal scale {scale} exceeds the maximum of {}",
                    crate::decimal::MAX_DECIMAL_SCALE
                ),
            });
        }
        Ok(Self::Decimal { scale })
    }
}

/// A column definition within a measurement schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// Data type.
    pub column_type: ColumnType,
    /// Role: timestamp, tag, or field.
    pub role: ColumnRole,
}

/// Schema for a single measurement (table).
///
/// Contains an ordered list of column definitions. The first column is always
/// the timestamp. Tags and fields follow. Columns are private to preserve
/// invariants — use [`add_tag()`](Self::add_tag) and
/// [`add_field()`](Self::add_field) for modification.
#[derive(Debug, Clone, Serialize)]
pub struct MeasurementSchema {
    measurement: String,
    columns: Vec<ColumnDef>,
    /// O(1) column lookup index (name → position in `columns`).
    #[serde(skip)]
    column_index: HashMap<String, usize>,
}

impl PartialEq for MeasurementSchema {
    fn eq(&self, other: &Self) -> bool {
        self.measurement == other.measurement && self.columns == other.columns
    }
}

impl Eq for MeasurementSchema {}

impl<'de> serde::Deserialize<'de> for MeasurementSchema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            measurement: String,
            columns: Vec<ColumnDef>,
        }
        let h = Helper::deserialize(deserializer)?;
        let column_index = h
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        Ok(Self {
            measurement: h.measurement,
            columns: h.columns,
            column_index,
        })
    }
}

impl MeasurementSchema {
    /// Create a new schema with just the timestamp column.
    #[must_use]
    pub fn new(measurement: impl Into<String>) -> Self {
        let ts_col = ColumnDef {
            name: crate::TIME_COLUMN.to_string(),
            column_type: ColumnType::Timestamp,
            role: ColumnRole::Timestamp,
        };
        let mut column_index = HashMap::new();
        column_index.insert(ts_col.name.clone(), 0);
        Self {
            measurement: measurement.into(),
            columns: vec![ts_col],
            column_index,
        }
    }

    /// Returns the measurement name.
    #[inline]
    #[must_use]
    pub fn measurement(&self) -> &str {
        &self.measurement
    }

    /// Returns the column definitions (timestamp first, then tags, then fields).
    #[inline]
    #[must_use]
    pub fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }

    /// Add a tag column if it doesn't already exist.
    ///
    /// Returns `true` if the tag was newly added, `false` if it already
    /// exists as a tag. Logs a warning if the name conflicts
    /// with an existing field column.
    #[must_use]
    pub fn add_tag(&mut self, name: &str) -> bool {
        if let Some(existing) = self.columns.iter().find(|c| c.name == name) {
            if existing.role != ColumnRole::Tag {
                // Reject writes with conflicting roles
                // instead of silently ignoring them.
                tracing::error!(
                    measurement = %self.measurement,
                    column = name,
                    existing_role = ?existing.role,
                    "tag name conflicts with existing column role — rejecting"
                );
            }
            return false;
        }
        let idx = self.columns.len();
        self.columns.push(ColumnDef {
            name: name.to_string(),
            column_type: ColumnType::String,
            role: ColumnRole::Tag,
        });
        self.column_index.insert(name.to_string(), idx);
        true
    }

    /// Add a field column if it doesn't already exist.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::TypeConflict`] if the field already exists with a
    /// different type.
    pub fn add_field(&mut self, name: &str, field_value: &FieldValue) -> Result<bool, SchemaError> {
        let new_type = ColumnType::from_field_value(field_value);

        if let Some(existing) = self.columns.iter().find(|c| c.name == name) {
            // Check for role conflict (tag with same name as field)
            if existing.role != ColumnRole::Field {
                return Err(SchemaError::TypeConflict {
                    measurement: self.measurement.clone(),
                    field: name.to_string(),
                    expected: format!("{:?}", existing.role),
                    got: "Field".to_string(),
                });
            }
            if !existing.column_type.accepts_value(field_value) {
                return Err(Self::mismatch(
                    &self.measurement,
                    name,
                    existing.column_type,
                    field_value,
                ));
            }
            return Ok(false);
        }

        let idx = self.columns.len();
        self.columns.push(ColumnDef {
            name: name.to_string(),
            column_type: new_type,
            role: ColumnRole::Field,
        });
        self.column_index.insert(name.to_string(), idx);
        Ok(true)
    }

    /// The error a value that does not fit `existing` should be rejected
    /// with.
    ///
    /// Two shapes, because they have two different fixes: a decimal whose
    /// only problem is carrying more fractional digits than the column
    /// declares is a [`SchemaError::DecimalScaleConflict`], and everything
    /// else is a [`SchemaError::TypeConflict`].
    fn mismatch(
        measurement: &str,
        field: &str,
        existing: ColumnType,
        value: &FieldValue,
    ) -> SchemaError {
        match (existing, value) {
            // A decimal that fits the column's *scale* and still cannot be
            // stored has run out of digits, not places: widening `10³⁷` from
            // scale 0 to scale 4 needs 42 of them. Reporting that as a scale
            // conflict prints "the column stores 4 places and the value
            // needs 0", which is true and useless.
            (ColumnType::Decimal { scale }, FieldValue::Decimal(d)) if d.scale() <= scale => {
                SchemaError::InvalidFieldValue {
                    field: field.to_string(),
                    reason: format!(
                        "{d} does not fit decimal({precision}, {scale}): it needs {needed} \
                         significant digits at that scale, and the maximum is {precision}",
                        precision = crate::decimal::DECIMAL_PRECISION,
                        needed = d.mantissa().unsigned_abs().to_string().len()
                            + usize::from(scale - d.scale()),
                    ),
                }
            }
            (ColumnType::Decimal { scale }, FieldValue::Decimal(d)) => {
                SchemaError::DecimalScaleConflict {
                    measurement: measurement.to_string(),
                    field: field.to_string(),
                    declared: scale,
                    got: d.scale(),
                    value: d.to_string(),
                }
            }
            _ => SchemaError::TypeConflict {
                measurement: measurement.to_string(),
                field: field.to_string(),
                expected: existing.to_string(),
                got: ColumnType::from_field_value(value).to_string(),
            },
        }
    }

    /// Add a field column with an explicit type.
    ///
    /// The type-first form of [`add_field`](Self::add_field): it is how a
    /// decimal column gets the scale it needs *before* the first value
    /// arrives, and how a batch that introduces a decimal field creates the
    /// column at the widest scale the batch carries rather than at whatever
    /// scale the first point happened to have.
    ///
    /// Returns `true` if the column was newly added, `false` if it already
    /// existed with exactly this type.
    ///
    /// # Errors
    ///
    /// [`SchemaError::TypeConflict`] if the name already exists as a tag or
    /// with a different type — including a decimal with a different scale,
    /// which is a different type.
    pub fn declare_field(
        &mut self,
        name: &str,
        column_type: ColumnType,
    ) -> Result<bool, SchemaError> {
        if let Some(existing) = self.columns.iter().find(|c| c.name == name) {
            if existing.role != ColumnRole::Field {
                return Err(SchemaError::TypeConflict {
                    measurement: self.measurement.clone(),
                    field: name.to_string(),
                    expected: format!("{:?}", existing.role),
                    got: "Field".to_string(),
                });
            }
            if existing.column_type != column_type {
                return Err(SchemaError::TypeConflict {
                    measurement: self.measurement.clone(),
                    field: name.to_string(),
                    expected: existing.column_type.to_string(),
                    got: column_type.to_string(),
                });
            }
            return Ok(false);
        }
        SeriesKey::validate_name(name, "field name")?;
        if let ColumnType::Decimal { scale } = column_type {
            if scale > crate::decimal::MAX_DECIMAL_SCALE {
                return Err(SchemaError::InvalidFieldValue {
                    field: name.to_string(),
                    reason: format!(
                        "decimal scale {scale} exceeds the maximum of {}",
                        crate::decimal::MAX_DECIMAL_SCALE
                    ),
                });
            }
        }
        let idx = self.columns.len();
        self.columns.push(ColumnDef {
            name: name.to_string(),
            column_type,
            role: ColumnRole::Field,
        });
        self.column_index.insert(name.to_string(), idx);
        Ok(true)
    }

    /// Look up a column by name — O(1) via the column index.
    #[inline]
    #[must_use]
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.column_index
            .get(name)
            .and_then(|&i| self.columns.get(i))
    }

    /// Push a pre-built column definition (e.g. during WAL replay).
    ///
    /// Silently ignores duplicates.
    pub fn push_column(&mut self, col: ColumnDef) {
        if self.column_index.contains_key(&col.name) {
            return; // duplicate — already present
        }
        let idx = self.columns.len();
        self.column_index.insert(col.name.clone(), idx);
        self.columns.push(col);
    }

    /// Returns the number of tag columns.
    #[must_use]
    pub fn tag_count(&self) -> usize {
        self.columns
            .iter()
            .filter(|c| c.role == ColumnRole::Tag)
            .count()
    }

    /// Returns the number of field columns.
    #[must_use]
    pub fn field_count(&self) -> usize {
        self.columns
            .iter()
            .filter(|c| c.role == ColumnRole::Field)
            .count()
    }

    /// Returns the names of all tag columns.
    #[must_use]
    pub fn tag_names(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|c| c.role == ColumnRole::Tag)
            .map(|c| c.name.as_str())
            .collect()
    }

    /// Returns the names of all field columns.
    #[must_use]
    pub fn field_names(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|c| c.role == ColumnRole::Field)
            .map(|c| c.name.as_str())
            .collect()
    }
}

/// A schema action that can be applied to the registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaAction {
    /// Create a new measurement with the given schema.
    CreateMeasurement(MeasurementSchema),
    /// Add a column to an existing measurement.
    AddColumn {
        /// Measurement name.
        measurement: String,
        /// Column definition to add.
        column: ColumnDef,
    },
}

/// Thread-safe, in-memory schema registry.
///
/// Tracks the schema for every measurement. Schemas are created automatically
/// on first write and evolve additively (new tags/fields added, type conflicts
/// rejected).
///
/// # Schema Evolution
///
/// The registry follows a **schema-on-write** model:
///
/// | Operation          | Supported | Mechanism           |
/// |--------------------|-----------|---------------------|
/// | Add tag/field      | Yes       | Automatic on write  |
/// | Type conflict      | Rejected  | `SchemaError`       |
/// | Remove column      | No        | N/A (append-only)   |
/// | Rename column      | No        | N/A                 |
/// | Change column type | No        | Write new field     |
///
/// Durability is the catalog manifest's job: the database persists every
/// action this registry returns before it writes the data that needs it.
#[derive(Debug, Clone)]
pub struct SchemaRegistry {
    /// `DashMap` for per-key concurrency without
    /// global `RwLock` contention or TOCTOU races.
    schemas: Arc<DashMap<String, Arc<MeasurementSchema>>>,
    /// Serialises schema *changes*. Reads never take it.
    ///
    /// A batch that adds columns to several measurements must be validated
    /// as a whole and applied as a whole; a per-measurement entry lock
    /// cannot express that without an ordering rule every caller has to
    /// remember. Changes are rare — a measurement gains a column a handful
    /// of times in its life — so one mutex costs nothing on the write path,
    /// which takes the lock-free fast path below whenever nothing changes.
    changes: Arc<std::sync::Mutex<()>>,
}

impl SchemaRegistry {
    /// Create an empty schema registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            schemas: Arc::new(DashMap::new()),
            changes: Arc::new(std::sync::Mutex::new(())),
        }
    }

    /// Register every point of a batch, or none of them.
    ///
    /// Validates the whole batch against the current schemas *and* against
    /// the columns earlier points of the same batch introduce, and only then
    /// applies the additions. A type conflict anywhere in the batch rejects
    /// it with the registry untouched — which is the property that lets the
    /// caller persist every returned action unconditionally. Registering
    /// point by point left the columns of the first points behind when a
    /// later point was refused, and nothing ever persisted them.
    ///
    /// Returns the actions the batch caused, in application order. Empty
    /// when the batch fits the existing schemas, which is the common case
    /// and takes no lock.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::TypeConflict`] if any field's type conflicts
    /// with the schema, or with the type another point of the batch gives
    /// the same field.
    pub fn register_batch(
        &self,
        points: &[&crate::types::Point],
    ) -> Result<Vec<SchemaAction>, SchemaError> {
        // Fast path: every point fits an existing schema. Read-only.
        let mut all_known = true;
        for point in points {
            match self.schemas.get(point.series_key().measurement()) {
                Some(schema) => {
                    if !Self::point_fits(&schema, point)? {
                        all_known = false;
                    }
                }
                None => all_known = false,
            }
        }
        if all_known {
            return Ok(Vec::new());
        }

        // Slow path: build the new schemas on private copies, validating as
        // we go, and commit only once the whole batch has been accepted.
        let _guard = self
            .changes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut pending: std::collections::BTreeMap<String, MeasurementSchema> =
            std::collections::BTreeMap::new();
        let mut actions = Vec::new();

        // A decimal column's scale is fixed when the column is created, so
        // *which* point of the batch creates it must not decide it. One
        // pre-pass takes the widest scale the batch carries for each new
        // field, and the column is created at that scale — a batch of
        // `1.5` then `1.4999` behaves the same as the reverse.
        let widest = Self::widest_decimal_scales(points);

        for point in points {
            let name = point.series_key().measurement();
            let schema = match pending.get_mut(name) {
                Some(s) => s,
                None => {
                    let fresh = match self.schemas.get(name) {
                        Some(existing) => (**existing).clone(),
                        None => {
                            let s = MeasurementSchema::new(name);
                            actions.push(SchemaAction::CreateMeasurement(s.clone()));
                            s
                        }
                    };
                    pending.entry(name.to_string()).or_insert(fresh)
                }
            };
            for tag_key in point.series_key().tag_keys() {
                if schema.add_tag(tag_key) {
                    actions.push(SchemaAction::AddColumn {
                        measurement: name.to_string(),
                        column: ColumnDef {
                            name: tag_key.to_string(),
                            column_type: ColumnType::String,
                            role: ColumnRole::Tag,
                        },
                    });
                }
            }
            for (field_name, field_value) in point.fields() {
                let column_type = match ColumnType::from_field_value(field_value) {
                    ColumnType::Decimal { scale } => ColumnType::Decimal {
                        scale: widest
                            .get(&(name, field_name.as_ref()))
                            .copied()
                            .unwrap_or(scale),
                    },
                    other => other,
                };
                match schema.column(field_name.as_ref()) {
                    Some(existing) => {
                        if !existing.column_type.accepts_value(field_value) {
                            return Err(MeasurementSchema::mismatch(
                                name,
                                field_name.as_ref(),
                                existing.column_type,
                                field_value,
                            ));
                        }
                    }
                    None => {
                        if schema.declare_field(field_name.as_ref(), column_type)? {
                            actions.push(SchemaAction::AddColumn {
                                measurement: name.to_string(),
                                column: ColumnDef {
                                    name: field_name.to_string(),
                                    column_type,
                                    role: ColumnRole::Field,
                                },
                            });
                        }
                    }
                }
            }
        }

        for (name, schema) in pending {
            self.schemas.insert(name, Arc::new(schema));
        }
        Ok(actions)
    }

    /// The widest decimal scale each `(measurement, field)` in the batch
    /// carries. Empty — and free — for a batch with no decimal fields.
    fn widest_decimal_scales<'a>(
        points: &[&'a crate::types::Point],
    ) -> HashMap<(&'a str, &'a str), u8> {
        let mut widest: HashMap<(&str, &str), u8> = HashMap::new();
        for point in points {
            for (field_name, field_value) in point.fields() {
                if let FieldValue::Decimal(d) = field_value {
                    let key = (point.series_key().measurement(), field_name.as_ref());
                    let entry = widest.entry(key).or_insert(0);
                    *entry = (*entry).max(d.scale());
                }
            }
        }
        widest
    }

    /// Rescale every decimal field of `points` to its column's scale.
    ///
    /// A decimal column stores one number of fractional digits, and this is
    /// where a value that arrived with fewer is widened to it — `1.5` into a
    /// scale-4 column becomes `1.5000`. Without it the memtable, the WAL and
    /// two segments of the same column could each hold a different scale,
    /// and a scan across them could not produce one Arrow schema.
    ///
    /// Returns `None` when nothing needed changing, which is the common case
    /// and copies nothing: a batch with no decimal fields never allocates.
    /// Otherwise returns the whole batch, with the affected points rewritten.
    ///
    /// Call *after* [`register_batch`](Self::register_batch), so the columns
    /// the batch introduces already exist.
    ///
    /// # Errors
    ///
    /// [`SchemaError::DecimalScaleConflict`] if a value carries more
    /// fractional digits than its column stores. `register_batch` refuses
    /// the same batch for the same reason, so reaching this is a sign the
    /// two were called out of order.
    pub fn normalize_decimals(
        &self,
        points: &[&crate::types::Point],
    ) -> Result<Option<Vec<crate::types::Point>>, SchemaError> {
        // Nothing to do for the overwhelming majority of batches: one pass
        // that touches no schema and allocates nothing.
        if !points
            .iter()
            .any(|p| p.fields().iter().any(|(_, v)| v.is_decimal()))
        {
            return Ok(None);
        }

        let mut out: Vec<crate::types::Point> = Vec::with_capacity(points.len());
        for point in points {
            let mut rewritten: Option<crate::types::Point> = None;
            let schema = self.schemas.get(point.series_key().measurement());
            for (field_name, field_value) in point.fields() {
                let FieldValue::Decimal(value) = field_value else {
                    continue;
                };
                let Some(scale) = schema
                    .as_ref()
                    .and_then(|s| s.column(field_name.as_ref()))
                    .and_then(|c| c.column_type.decimal_scale())
                else {
                    continue;
                };
                if scale == value.scale() {
                    continue;
                }
                let rescaled = value.rescale(scale).map_err(|_| {
                    // The same two failures, told apart the same way as in
                    // `MeasurementSchema::mismatch`: too many decimal places
                    // for the column, or too many significant digits once
                    // the places are added.
                    MeasurementSchema::mismatch(
                        point.series_key().measurement(),
                        field_name.as_ref(),
                        ColumnType::Decimal { scale },
                        field_value,
                    )
                })?;
                rewritten
                    .get_or_insert_with(|| (*point).clone())
                    .set_field(field_name.as_ref(), FieldValue::Decimal(rescaled));
            }
            out.push(rewritten.unwrap_or_else(|| (*point).clone()));
        }
        Ok(Some(out))
    }

    /// Declare a field column before anything is written to it.
    ///
    /// The reason this exists is decimals: a decimal column's scale is fixed
    /// by whatever creates the column, and letting the first meter reading
    /// decide how many fractional digits a settlement register keeps is a
    /// coin toss. Declaring `Decimal { scale: 4 }` up front makes it a
    /// decision.
    ///
    /// Returns the actions to persist — empty when the column already
    /// existed with exactly this type.
    ///
    /// # Errors
    ///
    /// [`SchemaError::TypeConflict`] if the column exists with a different
    /// type, a different decimal scale, or as a tag.
    pub fn declare_field(
        &self,
        measurement: &str,
        field: &str,
        column_type: ColumnType,
    ) -> Result<Vec<SchemaAction>, SchemaError> {
        let _guard = self
            .changes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut actions = Vec::new();
        let mut schema = match self.schemas.get(measurement) {
            Some(existing) => (**existing).clone(),
            None => {
                let fresh = MeasurementSchema::new(measurement);
                actions.push(SchemaAction::CreateMeasurement(fresh.clone()));
                fresh
            }
        };
        if schema.declare_field(field, column_type)? {
            actions.push(SchemaAction::AddColumn {
                measurement: measurement.to_string(),
                column: ColumnDef {
                    name: field.to_string(),
                    column_type,
                    role: ColumnRole::Field,
                },
            });
        } else if actions.is_empty() {
            // Nothing changed and nothing was created: no action to persist.
            return Ok(Vec::new());
        }
        self.schemas
            .insert(measurement.to_string(), Arc::new(schema));
        Ok(actions)
    }

    /// Does `point` fit `schema` without any change? A type conflict is an
    /// error; a missing column is `Ok(false)`.
    fn point_fits(
        schema: &MeasurementSchema,
        point: &crate::types::Point,
    ) -> Result<bool, SchemaError> {
        for tag_key in point.series_key().tag_keys() {
            if schema.column(tag_key).is_none() {
                return Ok(false);
            }
        }
        for (field_name, field_value) in point.fields() {
            match schema.column(field_name.as_ref()) {
                Some(existing) => {
                    if !existing.column_type.accepts_value(field_value) {
                        return Err(MeasurementSchema::mismatch(
                            schema.measurement(),
                            field_name.as_ref(),
                            existing.column_type,
                            field_value,
                        ));
                    }
                }
                None => return Ok(false),
            }
        }
        Ok(true)
    }

    /// Register a single point — [`register_batch`](Self::register_batch)
    /// for one point.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::TypeConflict`] if a field's type doesn't match
    /// the existing schema.
    pub fn register_point(
        &self,
        point: &crate::types::Point,
    ) -> Result<Vec<SchemaAction>, SchemaError> {
        self.register_batch(&[point])
    }

    /// Look up the schema for a measurement.
    ///
    /// Returns `Arc<MeasurementSchema>` for an O(1) clone.
    #[must_use]
    pub fn lookup(&self, measurement: &str) -> Option<Arc<MeasurementSchema>> {
        self.schemas.get(measurement).map(|r| r.value().clone())
    }

    /// Register a pre-built schema directly (e.g. during catalog replay on
    /// startup).  If the measurement already has a schema, the existing one
    /// is kept unchanged.
    pub fn register_schema(&self, name: &str, schema: MeasurementSchema) {
        self.schemas
            .entry(name.to_string())
            .or_insert(Arc::new(schema));
    }

    /// Register a measurement from a [`MeasurementSchema`] (WAL replay).
    ///
    /// Idempotent: if the measurement already exists the call is a no-op.
    pub fn register_measurement(&self, ms: MeasurementSchema) {
        self.schemas
            .entry(ms.measurement().to_string())
            .or_insert(Arc::new(ms));
    }

    /// Apply an `AddColumn` action from WAL replay.
    ///
    /// Idempotent: if the column already exists it is not duplicated.
    /// Checks for role conflicts (e.g., tag vs field with same name).
    pub fn apply_add_column(&self, measurement: &str, column: ColumnDef) {
        if let Some(mut schema_arc) = self.schemas.get_mut(measurement) {
            if let Some(existing) = schema_arc
                .value()
                .columns()
                .iter()
                .find(|c| c.name == column.name)
            {
                // Column exists — check for role conflict
                if existing.role != column.role {
                    tracing::warn!(
                        measurement,
                        column = %column.name,
                        existing_role = ?existing.role,
                        new_role = ?column.role,
                        "WAL replay column role conflict — skipping"
                    );
                }
                // Already exists (same or conflicting role) — skip
                return;
            }
            let schema = Arc::make_mut(schema_arc.value_mut());
            match column.role {
                ColumnRole::Tag => {
                    let _ = schema.add_tag(&column.name);
                }
                ColumnRole::Field => {
                    // For WAL replay, we push the column definition directly
                    // since we know the type is correct.
                    schema.push_column(column);
                }
                ColumnRole::Timestamp => {
                    // Timestamp column already exists in every schema.
                }
            }
        }
    }

    /// Returns the number of registered measurements.
    #[must_use]
    pub fn measurement_count(&self) -> usize {
        self.schemas.len()
    }

    /// Returns all registered measurement names.
    #[must_use]
    pub fn measurement_names(&self) -> Vec<String> {
        self.schemas.iter().map(|r| r.key().clone()).collect()
    }

    /// Remove a measurement schema. Returns the removed schema if it existed.
    #[must_use]
    pub fn remove(&self, measurement: &str) -> Option<Arc<MeasurementSchema>> {
        self.schemas.remove(measurement).map(|(_, v)| v)
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FieldValue, Point, SeriesKey};
    use std::collections::BTreeMap;

    fn make_point(
        measurement: &str,
        tags: &[(&str, &str)],
        fields: &[(&str, FieldValue)],
        ts: i64,
    ) -> Point {
        let tag_map: BTreeMap<String, String> = tags
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        let field_map: BTreeMap<String, FieldValue> = fields
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        let key = SeriesKey::new(measurement, tag_map).unwrap();
        Point::new(key, field_map, ts).unwrap()
    }

    #[test]
    fn measurement_schema_new() {
        let schema = MeasurementSchema::new("cpu");
        assert_eq!(schema.measurement(), "cpu");
        assert_eq!(schema.columns().len(), 1);
        assert_eq!(schema.columns()[0].name, crate::TIME_COLUMN);
        assert_eq!(schema.columns()[0].role, ColumnRole::Timestamp);
    }

    #[test]
    fn measurement_schema_add_tag() {
        let mut schema = MeasurementSchema::new("cpu");
        assert!(schema.add_tag("host"));
        assert!(!schema.add_tag("host")); // duplicate
        assert_eq!(schema.tag_count(), 1);
        assert_eq!(schema.columns()[1].name, "host");
        assert_eq!(schema.columns()[1].role, ColumnRole::Tag);
    }

    #[test]
    fn measurement_schema_add_field() {
        let mut schema = MeasurementSchema::new("cpu");
        let added = schema.add_field("value", &FieldValue::F64(1.0)).unwrap();
        assert!(added);
        let added = schema.add_field("value", &FieldValue::F64(2.0)).unwrap();
        assert!(!added); // same type, no error
        assert_eq!(schema.field_count(), 1);
    }

    #[test]
    fn measurement_schema_type_conflict() {
        let mut schema = MeasurementSchema::new("cpu");
        schema.add_field("value", &FieldValue::F64(1.0)).unwrap();
        let err = schema.add_field("value", &FieldValue::I64(1)).unwrap_err();
        assert!(matches!(err, SchemaError::TypeConflict { .. }));
    }

    #[test]
    fn schema_registry_auto_create() {
        let registry = SchemaRegistry::new();
        let point = make_point(
            "cpu",
            &[("host", "srv1")],
            &[("value", FieldValue::F64(72.5))],
            1_000_000_000,
        );
        let actions = registry.register_point(&point).unwrap();
        // Should create measurement + add host tag + add value field
        assert_eq!(actions.len(), 3);
        assert!(matches!(&actions[0], SchemaAction::CreateMeasurement(_)));

        let schema = registry.lookup("cpu").unwrap();
        assert_eq!(schema.measurement(), "cpu");
        assert_eq!(schema.tag_count(), 1);
        assert_eq!(schema.field_count(), 1);
    }

    #[test]
    fn schema_registry_additive_evolution() {
        let registry = SchemaRegistry::new();

        // First write: creates schema with "value" field
        let p1 = make_point(
            "cpu",
            &[("host", "srv1")],
            &[("value", FieldValue::F64(72.5))],
            1,
        );
        registry.register_point(&p1).unwrap();

        // Second write: adds new "cores" field and new "dc" tag
        let p2 = make_point(
            "cpu",
            &[("host", "srv1"), ("dc", "eu")],
            &[
                ("value", FieldValue::F64(73.0)),
                ("cores", FieldValue::I64(8)),
            ],
            2,
        );
        let actions = registry.register_point(&p2).unwrap();
        // Should only add new tag "dc" and new field "cores" (no create, "host" + "value" already exist)
        assert_eq!(actions.len(), 2);

        let schema = registry.lookup("cpu").unwrap();
        assert_eq!(schema.tag_count(), 2);
        assert_eq!(schema.field_count(), 2);
    }

    #[test]
    fn schema_registry_type_conflict_rejected() {
        let registry = SchemaRegistry::new();

        let p1 = make_point("cpu", &[], &[("value", FieldValue::F64(72.5))], 1);
        registry.register_point(&p1).unwrap();

        let p2 = make_point("cpu", &[], &[("value", FieldValue::I64(72))], 2);
        let err = registry.register_point(&p2).unwrap_err();
        assert!(matches!(err, SchemaError::TypeConflict { .. }));
    }

    #[test]
    fn schema_registry_multiple_measurements() {
        let registry = SchemaRegistry::new();

        let p1 = make_point("cpu", &[], &[("value", FieldValue::F64(72.5))], 1);
        let p2 = make_point("mem", &[], &[("used", FieldValue::U64(1024))], 1);

        registry.register_point(&p1).unwrap();
        registry.register_point(&p2).unwrap();

        assert_eq!(registry.measurement_count(), 2);
        assert!(registry.lookup("cpu").is_some());
        assert!(registry.lookup("mem").is_some());
        assert!(registry.lookup("disk").is_none());
    }

    #[test]
    fn schema_registry_remove() {
        let registry = SchemaRegistry::new();
        let p = make_point("cpu", &[], &[("value", FieldValue::F64(1.0))], 1);
        registry.register_point(&p).unwrap();
        assert!(registry.lookup("cpu").is_some());

        let removed = registry.remove("cpu");
        assert!(removed.is_some());
        assert!(registry.lookup("cpu").is_none());
        assert_eq!(registry.measurement_count(), 0);
    }

    #[test]
    fn schema_registry_measurement_names() {
        let registry = SchemaRegistry::new();
        let p1 = make_point("cpu", &[], &[("v", FieldValue::F64(1.0))], 1);
        let p2 = make_point("mem", &[], &[("v", FieldValue::U64(1))], 1);
        registry.register_point(&p1).unwrap();
        registry.register_point(&p2).unwrap();

        let mut names = registry.measurement_names();
        names.sort();
        assert_eq!(names, vec!["cpu", "mem"]);
    }

    #[test]
    fn schema_serde_roundtrip() {
        let mut schema = MeasurementSchema::new("cpu");
        let _ = schema.add_tag("host");
        schema.add_field("value", &FieldValue::F64(1.0)).unwrap();

        let json = serde_json::to_string(&schema).unwrap();
        let back: MeasurementSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(schema, back);
    }

    #[test]
    fn a_decimal_column_accepts_a_narrower_value_and_refuses_a_finer_one() {
        let mut schema = MeasurementSchema::new("meter");
        schema
            .declare_field("z1nb", ColumnType::Decimal { scale: 4 })
            .unwrap();
        // Fewer places: widened losslessly at write time, no schema change.
        assert!(!schema
            .add_field("z1nb", &FieldValue::Decimal("1.5".parse().unwrap()))
            .unwrap());
        // Trailing zeros past the column's scale are still exact.
        assert!(!schema
            .add_field("z1nb", &FieldValue::Decimal("1.50000".parse().unwrap()))
            .unwrap());
        // A real fifth digit would have to be rounded.
        let err = schema
            .add_field("z1nb", &FieldValue::Decimal("1.00005".parse().unwrap()))
            .unwrap_err();
        assert!(
            matches!(
                err,
                SchemaError::DecimalScaleConflict {
                    declared: 4,
                    got: 5,
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn running_out_of_digits_is_not_reported_as_a_scale_conflict() {
        // `10^37` at scale 0 fits; the same value at scale 4 does not, and
        // saying "the column stores 4 places and the value needs 0" would be
        // true and useless.
        let mut schema = MeasurementSchema::new("m");
        schema
            .declare_field("v", ColumnType::Decimal { scale: 4 })
            .unwrap();
        let huge = crate::decimal::Decimal::new(10_i128.pow(37), 0).unwrap();
        let err = schema
            .add_field("v", &FieldValue::Decimal(huge))
            .unwrap_err();
        match err {
            SchemaError::InvalidFieldValue { ref reason, .. } => {
                assert!(reason.contains("significant digits"), "{reason}");
            }
            other => panic!("expected a precision error, got {other:?}"),
        }
    }

    #[test]
    fn a_decimal_and_another_type_are_still_a_type_conflict() {
        let mut schema = MeasurementSchema::new("m");
        schema
            .declare_field("v", ColumnType::Decimal { scale: 2 })
            .unwrap();
        let err = schema.add_field("v", &FieldValue::F64(1.5)).unwrap_err();
        assert!(matches!(err, SchemaError::TypeConflict { .. }), "{err:?}");
    }

    #[test]
    fn a_batch_creating_a_decimal_column_takes_its_widest_scale() {
        let registry = SchemaRegistry::new();
        // Coarse first, fine second — and the reverse — must agree.
        for order in [["1.5", "1.4999"], ["1.4999", "1.5"]] {
            let registry = registry.clone();
            let points: Vec<Point> = order
                .iter()
                .enumerate()
                .map(|(i, text)| {
                    make_point(
                        "meter",
                        &[],
                        &[("v", FieldValue::Decimal(text.parse().unwrap()))],
                        i as i64,
                    )
                })
                .collect();
            let refs: Vec<&Point> = points.iter().collect();
            registry.register_batch(&refs).unwrap();
            assert_eq!(
                registry
                    .lookup("meter")
                    .unwrap()
                    .column("v")
                    .unwrap()
                    .column_type,
                ColumnType::Decimal { scale: 4 },
                "order {order:?}"
            );
        }
    }

    #[test]
    fn normalize_decimals_widens_to_the_column_and_copies_nothing_otherwise() {
        let registry = SchemaRegistry::new();
        let point = make_point(
            "meter",
            &[],
            &[("v", FieldValue::Decimal("1.5000".parse().unwrap()))],
            1,
        );
        registry.register_batch(&[&point]).unwrap();

        // A batch with no decimal fields never allocates.
        let plain = make_point("cpu", &[], &[("v", FieldValue::F64(1.0))], 1);
        assert!(registry.normalize_decimals(&[&plain]).unwrap().is_none());

        // A narrower value is rewritten at the column's scale.
        let narrow = make_point(
            "meter",
            &[],
            &[("v", FieldValue::Decimal("1.5".parse().unwrap()))],
            2,
        );
        let out = registry.normalize_decimals(&[&narrow]).unwrap().unwrap();
        match out[0].field("v") {
            Some(FieldValue::Decimal(d)) => {
                assert_eq!(d.scale(), 4);
                assert_eq!(d.to_string(), "1.5000");
            }
            other => panic!("expected a decimal, got {other:?}"),
        }
    }

    #[test]
    fn column_type_from_field_value() {
        assert_eq!(
            ColumnType::from_field_value(&FieldValue::F64(1.0)),
            ColumnType::F64
        );
        assert_eq!(
            ColumnType::from_field_value(&FieldValue::I64(1)),
            ColumnType::I64
        );
        assert_eq!(
            ColumnType::from_field_value(&FieldValue::U64(1)),
            ColumnType::U64
        );
        assert_eq!(
            ColumnType::from_field_value(&FieldValue::Bool(true)),
            ColumnType::Bool
        );
        assert_eq!(
            ColumnType::from_field_value(&FieldValue::String("x".into())),
            ColumnType::String
        );
    }

    #[test]
    fn column_type_display() {
        assert_eq!(ColumnType::Timestamp.to_string(), "timestamp");
        assert_eq!(ColumnType::String.to_string(), "string");
        assert_eq!(ColumnType::F64.to_string(), "f64");
        assert_eq!(ColumnType::I64.to_string(), "i64");
        assert_eq!(ColumnType::U64.to_string(), "u64");
        assert_eq!(ColumnType::Bool.to_string(), "bool");
    }

    #[test]
    fn column_role_display() {
        assert_eq!(ColumnRole::Timestamp.to_string(), "timestamp");
        assert_eq!(ColumnRole::Tag.to_string(), "tag");
        assert_eq!(ColumnRole::Field.to_string(), "field");
    }

    #[test]
    fn measurement_schema_column_not_found() {
        let schema = MeasurementSchema::new("cpu");
        assert!(schema.column("nonexistent").is_none());
    }

    #[test]
    fn schema_registry_remove_nonexistent() {
        let registry = SchemaRegistry::new();
        let removed = registry.remove("nope");
        assert!(removed.is_none());
    }

    #[test]
    fn schema_action_serde_roundtrip() {
        let action = SchemaAction::CreateMeasurement(MeasurementSchema::new("cpu"));
        let json = serde_json::to_string(&action).unwrap();
        let back: SchemaAction = serde_json::from_str(&json).unwrap();
        assert_eq!(action, back);

        let action = SchemaAction::AddColumn {
            measurement: "cpu".into(),
            column: ColumnDef {
                name: "value".into(),
                column_type: ColumnType::F64,
                role: ColumnRole::Field,
            },
        };
        let json = serde_json::to_string(&action).unwrap();
        let back: SchemaAction = serde_json::from_str(&json).unwrap();
        assert_eq!(action, back);
    }

    #[test]
    fn measurement_schema_accessors() {
        let mut schema = MeasurementSchema::new("cpu");
        assert!(schema.add_tag("host"));
        assert_eq!(schema.measurement(), "cpu");
        assert_eq!(schema.columns().len(), 2);
    }
}
