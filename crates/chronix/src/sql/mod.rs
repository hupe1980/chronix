//! SQL query engine powered by Apache `DataFusion`.
//!
//! This module integrates `DataFusion` as the SQL execution engine for Chronix,
//! enabling full SQL support including:
//!
//! - Standard SQL: `SELECT`, `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`
//! - Aggregate functions: `SUM`, `AVG`, `MIN`, `MAX`, `COUNT`
//! - Time-series functions: `time_bucket()`, `first()`, `last()`, `rate()`, `irate()`
//! - Window functions: `ROW_NUMBER`, `RANK`, `LAG`, `LEAD`, rolling aggregates
//! - `ASOF JOIN` for cross-series alignment with tolerance window
//!
//! Chronix measurements are registered as `DataFusion` tables via a custom
//! catalog provider that dynamically reflects the database schema.

pub mod asof_join;
#[cfg(feature = "object-store")]
pub mod cold_tier;
mod context;
mod exec;
mod functions;
mod provider;
pub mod readonly;

pub use asof_join::{execute_asof_join, AsofJoinExec};
pub use context::{
    create_namespaced_session_context, create_session_context, ChronixCatalogProvider,
    ChronixSchemaProvider,
};
pub use functions::register_udfs;
pub use provider::{measurement_schema_to_arrow, ChronixTableProvider};
pub use readonly::{plan_read_only, sql_read_only, verify_read_only};
