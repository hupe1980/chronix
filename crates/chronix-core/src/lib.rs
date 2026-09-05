//! # chronix-core
//!
//! Core data model, types, errors, and configuration for the Chronix time-series
//! database. This crate provides the foundational types that all other Chronix
//! crates depend on.
//!
//! ## Overview
//!
//! - **Data model:** [`FieldValue`], [`SeriesKey`], [`Point`] — the fundamental
//!   units of time-series data.
//! - **Schema:** [`MeasurementSchema`], [`SchemaRegistry`] — schema-on-write with
//!   additive evolution.
//! - **Errors:** [`ChronixError`] — crate-level and top-level error hierarchy.
//! - **Configuration:** [`ChronixConfig`], [`WalConfig`] — full database
//!   configuration with builder pattern.

#![warn(missing_docs)]
#![deny(unsafe_code)]
#![allow(clippy::module_name_repetitions)]

pub mod config;
pub mod error;
pub mod schema;
pub mod types;
pub mod wal_codec;

pub use config::{
    AnalyticsConfig, ChronixConfig, ChronixConfigBuilder, CompressionCodec, FloatEncoding,
    FsyncPolicy, WalConfig,
};
pub use error::{ChronixError, ConfigError, SchemaError, WalError};
pub use schema::{
    ColumnDef, ColumnRole, ColumnType, MeasurementSchema, SchemaAction, SchemaRegistry,
};
pub use types::{
    canonical_from_pairs, push_canonical, FieldValue, Fields, NamespaceId, NamespaceQuota,
    NamespaceUsage, Point, SegmentId, SegmentState, SeriesKey, ShardId, Tags, Timestamp, Tombstone,
    TombstoneSet, WalEntry, KV_SEPARATOR, MAX_STRING_FIELD_LENGTH, NAMESPACE_TAG,
    RESERVED_COLUMN_NAMES, TAG_SEPARATOR,
};
pub use wal_codec::{
    decode as wal_decode, encode as wal_encode, encode_write_point as wal_encode_write_point,
    CodecError as WalCodecError,
};
