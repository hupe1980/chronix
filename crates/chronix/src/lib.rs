//! # Chronix
//!
//! An embedded-first time-series database with forecasting and anomaly
//! detection in the engine.
//!
//! One dependency gives you a crash-safe columnar store with SQL
//! (DataFusion), PromQL, exact decimals, rollups, retention and analytics —
//! in process, with no server. The same engine runs as the `chronixd` server
//! behind Grafana, Prometheus, Telegraf and the OpenTelemetry Collector.
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
//! - [`Chronix`] — open, insert, query, [`sql`](Chronix::sql),
//!   [`promql`](Chronix::promql), forecast, rollups, detectors, retention,
//!   backup. A cheap-to-clone handle.
//! - [`prelude`] — the types and macros a typical program needs.
//! - [`sql`], [`promql`] — the query languages, for more than the handle's
//!   one-call methods offer.
//! - [`rollup`], [`retention`], [`export`], [`pipeline`] — background work
//!   and interop.
//! - The engine crates are re-exported under their own names for advanced
//!   use.
//!
//! ## Feature flags
//!
//! | Flag | Default | Adds |
//! |---|---|---|
//! | `sql` | on | SQL through DataFusion; implies `analytics` |
//! | `analytics` | on | Forecasting, anomaly detection, declared detectors |
//! | `field-encryption` | on | Per-column AES-256-GCM |
//! | `streaming` | off | Change feed, subscriptions, triggers and webhooks |
//! | `security` | off | API keys, JWT, mTLS, Cedar policies, audit, namespaces |
//! | `pipeline` | off | The real-time pipeline; implies `streaming` and `security` |
//! | `object-store` | off | Parquet cold tier on S3, GCS or Azure; implies `sql` |
//!
//! `default-features = false` is the smallest build: storage, PromQL,
//! rollups and retention.
//!
//! ## Stability
//!
//! 1. **[`Chronix`], [`prelude`] and the types they name** are the supported
//!    surface. Arrow's `RecordBatch` is the one third-party type in it.
//! 2. **[`sql`] and [`promql`]** take and return DataFusion types, and move
//!    when DataFusion does.
//! 3. **The re-exported engine crates** promise nothing beyond their own API.
//!
//! Before 1.0 a breaking change bumps the minor version. The on-disk format
//! is frozen at generation 2. Every change is in the
//! [changelog](https://github.com/hupe1980/chronix/blob/main/CHANGELOG.md).

#![warn(missing_docs)]
#![deny(unsafe_code)]
#![allow(clippy::module_name_repetitions)]

/// Analytics configuration types for the Rust API.
#[cfg(feature = "analytics")]
pub mod analytics;
#[cfg(feature = "object-store")]
pub mod cold_archive;
pub mod db;
pub mod delete;
pub mod detector;
pub mod diagnostics;
pub mod error;
pub mod export;
pub mod inspect;
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
pub mod tenancy;

// The engine crates, for advanced use. No aliases: a module alias is a
// second name for the same thing, and every one of them was a name the
// docs could not resolve.
#[cfg(feature = "analytics")]
pub use chronix_analytics;
pub use chronix_core;
pub use chronix_encoding;
pub use chronix_engine;
pub use chronix_query;
#[cfg(feature = "security")]
pub use chronix_security;
#[cfg(feature = "streaming")]
pub use chronix_streaming;

// Query results are Arrow `RecordBatch`es; this is the `arrow` they are in,
// so a caller printing or converting one needs no second, possibly
// mismatched, `arrow` dependency (`chronix::arrow::util::pretty`).
pub use arrow;

// Primary export
#[cfg(feature = "analytics")]
pub use analytics::{AnomalyConfig, ForecastConfig};
pub use chronix_core::Parallelism;
pub use db::BackupManifest;
pub use db::Chronix;
pub use db::DatabaseStatistics;
pub use db::SegmentInfo;
pub use db::{CardinalityScope, CardinalityStats, MAX_CARDINALITY_LIMIT, NameCount};
pub use delete::{DeleteBuilder, DeleteOutcome, DeleteRequest};
pub use detector::{DetectorBuilder, DetectorDeclaration, DetectorInfo, DetectorStatus, Season};
pub use error::DbError;
pub use error::InsertResult;
pub use error::QuotaResource;
pub use export::{ParquetCompression, ParquetExportConfig};
#[cfg(feature = "pipeline")]
pub use pipeline::{Pipeline, PipelineConfig};
pub use rollup::{RollupAggFn, RollupBuilder, RollupConfig, RollupRegistry, RollupState};
pub use tenancy::{NamespaceLimits, NamespaceUsage};

/// Everything a typical program needs: `use chronix::prelude::*;`.
pub mod prelude {
    pub use chronix_core::{
        BucketWidth, ChronixConfig, ChronixConfigBuilder, ChronixError, ColumnDef, ColumnRole,
        ColumnType, CompressionCodec, Decimal, FieldValue, FloatEncoding, FsyncPolicy,
        MeasurementSchema, Parallelism, Point, SeriesKey, TimeBucket, Timestamp,
    };
    pub use chronix_query::{AggFn, QueryBuilder, QueryPlan};

    pub use arrow::record_batch::RecordBatch;

    pub use crate::db::Chronix;
    pub use crate::error::{DbError, InsertResult};
    pub use crate::rollup::{RollupAggFn, RollupBuilder};
    pub use crate::{fields, tags};
}
