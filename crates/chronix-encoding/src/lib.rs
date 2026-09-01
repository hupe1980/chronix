//! # chronix-encoding
//!
//! Column-oriented encoding and compression for the Chronix time-series database.
//!
//! This crate provides type-specific encoders that exploit the statistical
//! properties of time-series data to achieve high compression ratios:
//!
//! | Column type | Encoder | Typical ratio |
//! |-------------|---------|---------------|
//! | Timestamps | [`DeltaOfDeltaEncoder`] — delta-of-delta + bit-packing | 8–64× |
//! | Floats | [`AlpEncoder`] — decimal reconstruction + FOR bit-packing | 4–20× |
//! | Floats | [`ChimpEncoder`] / [`GorillaEncoder`] — XOR-based | 1.3–4× |
//! | Integers | [`IntegerEncoder`] — delta + ZigZag + bit-packing | 4–16× |
//! | Integers | [`ForEncoder`] — Frame-of-Reference (narrow range) | 4–32× |
//! | Strings | [`DictionaryEncoder`] — string table + index | 4–64× |
//! | Booleans | [`BitmapEncoder`] — 1 bit per value | 8× |
//! | Any | [`PlainEncoder`] — uncompressed fallback | 1× |
//!
//! ## Unified API
//!
//! [`ColumnEncoder`] / [`ColumnDecoder`] auto-select the best encoding per
//! column type and transparently fall back to plain when a specialized
//! encoder cannot achieve sufficient compression.
//!
//! ## Adaptive Selection
//!
//! [`AdaptiveSelector`] samples values at write time and classifies the data
//! distribution (`Constant`, `SlowlyVarying`, `Periodic`, `Random`) to pick
//! the optimal encoder. The choice is stored in column metadata for the decoder.
//!
//! ## Design Principles
//!
//! 1. **Lossless** — every encoder guarantees bitwise-exact roundtrip.
//! 2. **Zero-copy** — decoders operate on byte slices where possible.
//! 3. **Auto-selection** — the unified API picks the best encoder per column.
//! 4. **Fallback** — plain encoding is used when compression is not beneficial.

#![warn(missing_docs)]
#![deny(unsafe_code)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_lossless)]
#![allow(clippy::cast_possible_wrap)]

pub mod adaptive;
pub mod alp;
pub mod bitmap;
pub mod chimp;
pub(crate) mod coding;
pub mod delta;
pub mod dictionary;
pub mod error;
pub mod for_encoding;
pub mod gorilla;
pub mod integer;
pub mod patas;
pub mod plain;
pub mod rle;
pub(crate) mod simd;
pub mod unified;

pub use adaptive::{AdaptiveSelector, FloatPattern, IntegerPattern, StringPattern};
pub use alp::{AlpDecoder, AlpEncoder};
pub use bitmap::{BitmapDecoder, BitmapEncoder};
pub use chimp::{Chimp128Decoder, Chimp128Encoder, ChimpDecoder, ChimpEncoder};
pub use delta::{DeltaOfDeltaDecoder, DeltaOfDeltaEncoder};
pub use dictionary::{DictionaryDecoder, DictionaryEncoder};
pub use error::EncodingError;
pub use for_encoding::{ForDecoder, ForEncoder};
pub use gorilla::{GorillaDecoder, GorillaEncodeScratch, GorillaEncoder};
pub use integer::{IntegerDecoder, IntegerEncoder, VarintDecoder, VarintEncoder};
pub use patas::{PatasDecoder, PatasEncoder};
pub use plain::{PlainDecoder, PlainEncoder};
pub use rle::{RleDecoder, RleEncoder};
pub use unified::{ColumnDecoder, ColumnEncoder, DecodedColumn, EncodedBlock, EncodingType};
