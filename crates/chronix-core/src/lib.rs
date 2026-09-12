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
//! - **Exact decimals:** [`Decimal`] — `mantissa × 10⁻ˢᶜᵃˡᵉ` for the one class
//!   of series whose value is a legal quantity: metering, billing and
//!   settlement. See [`decimal`] for why a `f64` cannot carry those.
//! - **Schema:** [`MeasurementSchema`], [`SchemaRegistry`] — schema-on-write with
//!   additive evolution.
//! - **Calendar buckets:** [`TimeBucket`] — the one answer to "what is a day",
//!   shared by SQL's `time_bucket()`, a rollup tier and the native
//!   `downsample()` plan, so the three cannot disagree. See [`timebucket`].
//! - **Errors:** [`ChronixError`] — crate-level and top-level error hierarchy.
//! - **Configuration:** [`ChronixConfig`], [`WalConfig`] — full database
//!   configuration with builder pattern.

#![warn(missing_docs)]
#![deny(unsafe_code)]
#![allow(clippy::module_name_repetitions)]

pub mod config;
pub mod decimal;
pub mod error;
pub mod schema;
pub mod timebucket;
pub mod types;
pub mod wal_codec;

pub use config::{
    AnalyticsConfig, Checkpoints, ChronixConfig, ChronixConfigBuilder, CompressionCodec,
    FieldEncryption, FloatEncoding, FsyncPolicy, WalConfig,
};
pub use decimal::{
    pow10, Decimal, DecimalError, DECIMAL_PRECISION, MAX_DECIMAL_MANTISSA, MAX_DECIMAL_SCALE,
};
pub use error::{ChronixError, ConfigError, SchemaError, WalError};
pub use schema::{
    ColumnDef, ColumnRole, ColumnType, MeasurementSchema, SchemaAction, SchemaRegistry,
};
pub use timebucket::{BucketParseError, BucketWidth, TimeBucket};
pub use types::{
    canonical_from_pairs, push_canonical, FieldValue, Fields, NamespaceId, NamespaceQuota,
    NamespaceUsage, Point, SegmentFile, SegmentId, SegmentState, SeriesKey, ShardId, Tags,
    Timestamp, Tombstone, TombstoneSet, WalEntry, KV_SEPARATOR, MAX_STRING_FIELD_LENGTH,
    NAMESPACE_TAG, RESERVED_COLUMN_NAMES, TAG_SEPARATOR, TIME_COLUMN,
};
pub use wal_codec::{
    decode as wal_decode, encode as wal_encode, encode_write_point as wal_encode_write_point,
    CodecError as WalCodecError,
};
