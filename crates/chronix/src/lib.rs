//! # Chronix
//!
//! An embedded, analytics-optimised time-series database written in Rust.
//!
//! Chronix delivers high write throughput on commodity hardware via a
//! WAL-backed durable write path, a schema-on-write data model, and
//! columnar storage designed for efficient analytical queries.
//!
//! ## Quick Start
//!
//! ```no_run
//! use chronix::prelude::*;
//! use chronix::{tags, fields};
//!
//! let config = ChronixConfig::builder()
//!     .data_dir("/tmp/mydb")
//!     .build()
//!     .unwrap();
//!
//! let db = Chronix::open(config).unwrap();
//!
//! let key = SeriesKey::new("cpu", tags! {
//!     "host" => "server-01",
//!     "region" => "us-east",
//! }).unwrap();
//!
//! let point = Point::new(key, fields! {
//!     "usage_idle" => 95.5_f64,
//!     "usage_system" => 1.2_f64,
//! }, 1_700_000_000_000).unwrap();
//!
//! db.insert(&point).unwrap();
//! db.close().unwrap();
//! ```
//!
//! ## Crate Structure
//!
//! - [`chronix_core`] — Fundamental types, schema, errors, configuration.
//! - [`chronix_wal`] — Write-Ahead Log for crash-safe durability.
//! - [`chronix_encoding`] — Column encoders/decoders (Chimp, Gorilla, delta, etc.).
//! - [`chronix_segment`] — Columnar segment file reader/writer.
//! - [`chronix_memtable`] — Lock-free in-memory buffer with shard routing.
//! - [`chronix_storage`] — Pluggable storage backend (local FS, future: S3).
//! - [`chronix_index`] — Time index, bloom filters, segment catalog.
//! - [`chronix_query`] — Query planning, filtering, aggregation, downsampling.
//! - **`chronix`** (this crate) — Unified public API.

#![warn(missing_docs)]
#![deny(unsafe_code)]
#![allow(clippy::module_name_repetitions)]

/// Analytics configuration types for the Rust API.
pub mod analytics;
#[cfg(feature = "object-store")]
pub mod cold_archive;
pub mod compaction_scheduler;
pub mod db;
pub mod delete;
pub mod error;
pub mod export;
pub mod flush_scheduler;
/// Compile-time lock ordering enforcement.
pub mod lock_order;
#[macro_use]
pub mod macros;
pub mod pipeline;
pub mod promql;
pub mod retention;
pub mod rollup;
pub mod sql;
pub mod warm_tier;

// Re-export sub-crates for advanced usage
pub use chronix_analytics;
pub use chronix_analytics::anomaly as chronix_anomaly;
pub use chronix_analytics::compute as chronix_compute;
pub use chronix_analytics::forecast as chronix_forecast;
pub use chronix_analytics::multivariate as chronix_multivariate;
pub use chronix_analytics::preprocess as chronix_preprocess;
pub use chronix_core;
pub use chronix_encoding;
pub use chronix_engine::cache as chronix_cache;
pub use chronix_engine::compaction as chronix_compaction;
pub use chronix_engine::index as chronix_index;
pub use chronix_engine::memtable as chronix_memtable;
#[cfg(feature = "object-store")]
pub use chronix_engine::objstore as chronix_objstore;
pub use chronix_engine::segment as chronix_segment;
pub use chronix_engine::storage as chronix_storage;
pub use chronix_engine::wal as chronix_wal;
pub use chronix_query;
pub use chronix_security::audit as chronix_audit;
pub use chronix_security::authz as chronix_authz;
pub use chronix_streaming::cdc as chronix_stream;
pub use chronix_streaming::signal as chronix_signal;

// Primary export
pub use analytics::{AnomalyConfig, ForecastConfig};
pub use db::BackupManifest;
pub use db::Chronix;
pub use db::DatabaseStatistics;
pub use delete::{DeleteBuilder, DeleteOutcome, DeleteRequest};
pub use error::DbError;
pub use error::InsertResult;
pub use export::{ParquetCompression, ParquetExportConfig};
pub use pipeline::{Pipeline, PipelineConfig};
pub use rollup::{RollupAggFn, RollupBuilder, RollupConfig, RollupRegistry};
pub use warm_tier::{WarmTierConfig, WarmTierResult};

/// Convenience prelude — import everything you need with `use chronix::prelude::*`.
pub mod prelude {
    // Core types
    pub use chronix_core::{
        ChronixConfig, ChronixConfigBuilder, ChronixError, ColumnDef, ColumnRole, ColumnType,
        CompressionCodec, ConfigError, FieldValue, FloatEncoding, FsyncPolicy, MeasurementSchema,
        Point, SchemaAction, SchemaError, SchemaRegistry, SegmentId, SeriesKey, ShardId,
        StorageBackendConfig, Timestamp, WalConfig, WalError,
    };

    // WAL
    pub use chronix_engine::wal::{replay_all, WalReader, WalRecord, WalRecordType, WalWriter};

    // Query
    pub use chronix_query::pruning::PruningStats;
    pub use chronix_query::{AggFn, QueryBuilder, QueryPlan};

    // Arrow interop
    pub use arrow::record_batch::RecordBatch;

    // Facade
    pub use crate::db::Chronix;
    pub use crate::error::DbError;
}
