//! SQL query engine powered by Apache `DataFusion`.
//!
//! This module integrates `DataFusion` as the SQL execution engine for Chronix,
//! enabling full SQL support including:
//!
//! - Standard SQL: `SELECT`, `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`
//! - Aggregate functions: `SUM`, `AVG`, `MIN`, `MAX`, `COUNT`
//! - Time-series functions: `time_bucket()`, `first()`, `last()`, `rate()`, `irate()`
//! - Window functions: `ROW_NUMBER`, `RANK`, `LAG`, `LEAD`, rolling aggregates
//!
//! `ASOF JOIN` is **not** SQL syntax — DataFusion's parser has none, and this
//! crate adds none. It is a Rust API, [`execute_asof_join`], over two physical
//! plans; see [`asof_join`] for the shape of a call.
//!
//! Chronix measurements are registered as `DataFusion` tables via a custom
//! catalog provider that dynamically reflects the database schema.
//!
//! # Stability
//!
//! Everything exported here takes or returns a `DataFusion` type, so this
//! module's signatures move when `DataFusion` does — a major bump there is a
//! breaking change here, whatever this crate's own version says. Callers who
//! want that coupling use these; callers who do not use
//! [`Chronix::sql`](crate::Chronix::sql), which returns Arrow `RecordBatch`es
//! and nothing else.

pub mod asof_join;
mod batch;
#[cfg(feature = "object-store")]
pub mod cold_tier;
mod context;
mod epoch_literals;
mod exec;
mod functions;
mod provider;
pub mod readonly;

pub use asof_join::{execute_asof_join, AsofJoinExec};
#[cfg(feature = "object-store")]
pub(crate) use batch::to_archive_batch;
pub use context::{create_namespaced_session_context, create_session_context};
pub use functions::register_udfs;
#[cfg(feature = "object-store")]
pub(crate) use provider::measurement_schema_to_archive_arrow;
pub use provider::measurement_schema_to_arrow;
pub use readonly::{plan_read_only, sql_read_only, verify_read_only};
