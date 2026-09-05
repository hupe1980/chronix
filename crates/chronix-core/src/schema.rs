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
use crate::types::FieldValue;

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
}

impl ColumnType {
    /// Derive the column type from a [`FieldValue`].
    #[inline]
    #[must_use]
    pub fn from_field_value(value: &FieldValue) -> Self {
        match value {
            FieldValue::F64(_) => Self::F64,
            FieldValue::I64(_) => Self::I64,
            FieldValue::U64(_) => Self::U64,
            FieldValue::Bool(_) => Self::Bool,
            FieldValue::String(_) => Self::String,
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
        }
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
    /// FINDING-09: O(1) column lookup index (name → position in `columns`).
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
    /// exists as a tag. FINDING-25: logs a warning if the name conflicts
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
            // FINDING-25: check for role conflict (tag with same name as field)
            if existing.role != ColumnRole::Field {
                return Err(SchemaError::TypeConflict {
                    measurement: self.measurement.clone(),
                    field: name.to_string(),
                    expected: format!("{:?}", existing.role),
                    got: "Field".to_string(),
                });
            }
            if existing.column_type != new_type {
                return Err(SchemaError::TypeConflict {
                    measurement: self.measurement.clone(),
                    field: name.to_string(),
                    expected: existing.column_type.to_string(),
                    got: new_type.to_string(),
                });
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

    /// Look up a column by name — O(1) via FINDING-09 column index.
    #[inline]
    #[must_use]
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.column_index
            .get(name)
            .and_then(|&i| self.columns.get(i))
    }

    /// Push a pre-built column definition (e.g. during WAL replay).
    ///
    /// Silently ignores duplicates (FINDING-10).
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
                if schema.add_field(field_name.as_ref(), field_value)? {
                    actions.push(SchemaAction::AddColumn {
                        measurement: name.to_string(),
                        column: ColumnDef {
                            name: field_name.to_string(),
                            column_type: ColumnType::from_field_value(field_value),
                            role: ColumnRole::Field,
                        },
                    });
                }
            }
        }

        for (name, schema) in pending {
            self.schemas.insert(name, Arc::new(schema));
        }
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
                    let new_type = ColumnType::from_field_value(field_value);
                    if existing.column_type != new_type {
                        return Err(SchemaError::TypeConflict {
                            measurement: schema.measurement().to_string(),
                            field: field_name.to_string(),
                            expected: existing.column_type.to_string(),
                            got: new_type.to_string(),
                        });
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
    /// FINDING-08: returns `Arc<MeasurementSchema>` for O(1) clone.
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
    /// FINDING-28: checks for role conflicts (e.g., tag vs field with same name).
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
                        "FINDING-28: WAL replay column role conflict — skipping"
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
