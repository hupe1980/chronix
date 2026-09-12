//! # Chronix
//!
//! The embedded-first time-series database with analytics built in.
//!
//! One dependency gives you a crash-safe, columnar time-series engine with
//! SQL (DataFusion), PromQL, forecasting and anomaly detection — in
//! process, no server, no sidecar. The same engine runs as the `chronixd`
//! server behind Grafana, Prometheus and Telegraf.
//!
//! ## Quick start
//!
//! ```no_run
//! use chronix::prelude::*;
//!
//! let db = Chronix::open_small("/tmp/mydb")?;
//!
//! let key = SeriesKey::new("cpu", tags! { "host" => "server-01" })?;
//! let point = Point::new(key, fields! { "usage" => 95.5 }, 1_700_000_000_000_000_000)?;
//! db.insert(&point)?;
//!
//! for batch in db.sql("SELECT host, avg(usage) FROM cpu GROUP BY host")? {
//!     println!("{batch:?}");
//! }
//! db.close()?;
//! # Ok::<(), chronix::DbError>(())
//! ```
//!
//! ## Where things are
//!
//! - [`Chronix`] — open, insert, query, [`sql`](Chronix::sql), forecast,
//!   rollups, retention, backup. A cheap-to-clone handle.
//! - [`prelude`] — the types and macros a typical program needs.
//! - [`sql`], [`promql`] — the two query languages, for callers that want
//!   more than [`Chronix::sql`] and [`Chronix::promql`] offer.
//! - [`rollup`], [`retention`], [`export`], [`pipeline`] — background work
//!   and interop.
//! - The engine crates are re-exported under their own names
//!   ([`chronix_core`], [`chronix_engine`], [`chronix_query`],
//!   [`chronix_analytics`], [`chronix_streaming`], [`chronix_security`],
//!   [`chronix_encoding`]) for advanced use.
//!
//! ## Stability
//!
//! Three tiers, and the difference matters when a dependency bumps:
//!
//! 1. **[`Chronix`], [`prelude`] and the types they name** — the supported
//!    surface. Arrow's `RecordBatch` is the only third-party type in it, and
//!    deliberately so: a columnar database that hides its batches is a
//!    database you cannot stream out of.
//! 2. **[`sql`] and [`promql`]** — for callers who want more than
//!    [`Chronix::sql`] and [`Chronix::promql`] offer. `sql` takes and returns
//!    `DataFusion` types, so it moves when `DataFusion` does.
//! 3. **The re-exported engine crates** — no promise beyond their own.
//!
//! Within 0.x none of this is frozen; a breaking change bumps the minor
//! (`CONTRIBUTING.md`). `public_api` pins the tier-1 surface so that a change
//! to it is a deliberate edit rather than a side effect.

#![warn(missing_docs)]
#![deny(unsafe_code)]
#![allow(clippy::module_name_repetitions)]

/// Analytics configuration types for the Rust API.
pub mod analytics;
#[cfg(feature = "object-store")]
pub mod cold_archive;
pub mod db;
pub mod delete;
pub mod error;
pub mod export;
/// Compile-time lock ordering enforcement.
///
/// Internal: the lock hierarchy is an invariant of this crate's own
/// implementation, not a contract with callers, and the wrapper's signatures
/// are `parking_lot`'s.
pub(crate) mod lock_order;
mod maintenance;
#[macro_use]
pub mod macros;
#[cfg(feature = "pipeline")]
pub mod pipeline;
pub mod promql;
pub mod retention;
pub mod rollup;
#[cfg(feature = "sql")]
pub mod sql;

// The engine crates, for advanced use. No aliases: a module alias is a
// second name for the same thing, and every one of them was a name the
// docs could not resolve.
pub use chronix_analytics;
pub use chronix_core;
pub use chronix_encoding;
pub use chronix_engine;
pub use chronix_query;
#[cfg(feature = "security")]
pub use chronix_security;
#[cfg(feature = "streaming")]
pub use chronix_streaming;

// Primary export
pub use analytics::{AnomalyConfig, ForecastConfig};
pub use db::BackupManifest;
pub use db::Chronix;
pub use db::DatabaseStatistics;
pub use delete::{DeleteBuilder, DeleteOutcome, DeleteRequest};
pub use error::DbError;
pub use error::InsertResult;
pub use export::{ParquetCompression, ParquetExportConfig};
#[cfg(feature = "pipeline")]
pub use pipeline::{Pipeline, PipelineConfig};
pub use rollup::{RollupAggFn, RollupBuilder, RollupConfig, RollupRegistry, RollupState};

/// Everything a typical program needs: `use chronix::prelude::*;`.
pub mod prelude {
    pub use chronix_core::{
        BucketWidth, ChronixConfig, ChronixConfigBuilder, ChronixError, ColumnDef, ColumnRole,
        ColumnType, CompressionCodec, Decimal, FieldValue, FloatEncoding, FsyncPolicy,
        MeasurementSchema, Point, SeriesKey, TimeBucket, Timestamp,
    };
    pub use chronix_query::{AggFn, QueryBuilder, QueryPlan};

    pub use arrow::record_batch::RecordBatch;

    pub use crate::db::Chronix;
    pub use crate::error::{DbError, InsertResult};
    pub use crate::rollup::{RollupAggFn, RollupBuilder};
    pub use crate::{fields, tags};
}
