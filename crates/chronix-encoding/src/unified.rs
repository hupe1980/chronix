//! Unified column encoding API with automatic encoder selection.
//!
//! [`ColumnEncoder`] selects the best encoding for each column type and
//! transparently falls back to [`PlainEncoder`] when the
//! specialised encoder does not achieve at least 2× compression.
//!
//! [`ColumnDecoder`] reads the [`EncodingType`] tag from an [`EncodedBlock`]
//! and dispatches to the correct decoder.
//!
//! ## Wire format
//!
//! ```text
//! [encoding_type: u8]
//! [payload …]
//! ```

use chronix_core::config::FloatEncoding;

use crate::alp::AlpDecoder;
use crate::bitmap::{BitmapDecoder, BitmapEncoder};
use crate::chimp::{Chimp128Decoder, ChimpDecoder};
use crate::delta::{DeltaOfDeltaDecoder, DeltaOfDeltaEncoder};
use crate::dictionary::{DictionaryDecoder, DictionaryEncoder};
use crate::error::{EncodingError, Result};
use crate::for_encoding::{ForDecoder, ForEncoder};
use crate::gorilla::{GorillaDecoder, GorillaEncoder};
use crate::integer::{IntegerDecoder, IntegerEncoder, VarintDecoder, VarintEncoder};
use crate::patas::PatasDecoder;
use crate::plain::{PlainDecoder, PlainEncoder};
use crate::rle::{RleDecoder, RleEncoder};

// ---------------------------------------------------------------------------
// EncodingType
// ---------------------------------------------------------------------------

/// Discriminant tag stored as the first byte of every [`EncodedBlock`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EncodingType {
    /// Delta-of-delta for timestamps.
    DeltaOfDelta = 0,
    /// Chimp XOR for floats.
    Chimp = 1,
    /// Gorilla XOR for floats.
    Gorilla = 2,
    /// Delta + `ZigZag` for i64.
    IntegerI64 = 3,
    /// Delta + `ZigZag` for u64.
    IntegerU64 = 4,
    /// Dictionary encoding for strings.
    Dictionary = 5,
    /// Bitmap for booleans.
    Bitmap = 6,
    /// Plain f64.
    PlainF64 = 7,
    /// Plain i64.
    PlainI64 = 8,
    /// Plain u64.
    PlainU64 = 9,
    /// Plain bool.
    PlainBool = 10,
    /// Plain string.
    PlainString = 11,
    /// Nullable wrapper — validity bitmap + inner encoded payload.
    ///
    /// Payload format:
    /// ```text
    /// [inner_encoding: u8][total_count: u32 LE][validity_bitmap: ceil(count/8) bytes][inner_payload…]
    /// ```
    /// Bit `1` in the validity bitmap means the value is present; `0` means null.
    /// Only non-null values are encoded in `inner_payload`.
    Nullable = 12,
    /// Run-length encoding for columns with repeated consecutive values.
    Rle = 13,
    /// Delta + ZigZag + LEB128 varint for i64.
    ///
    /// Better than [`IntegerI64`](Self::IntegerI64) for sparse or highly
    /// variable data where most deltas are small but occasional outliers
    /// would inflate the fixed bit width.
    VarintI64 = 14,
    /// Delta + ZigZag + LEB128 varint for u64.
    VarintU64 = 15,
    /// Chimp128 ring-buffer XOR for floats.
    Chimp128 = 16,
    /// Frame-of-Reference for narrow-range integers.
    ForI64 = 17,
    /// Frame-of-Reference for narrow-range unsigned integers.
    ForU64 = 18,
    /// Patas byte-aligned XOR for floats (VLDB 2023).
    Patas = 19,
    /// ALP adaptive lossless floating-point compression (SIGMOD 2024).
    ///
    /// The default choice for `f64` columns whose values originated as
    /// decimals — which most sensor, meter and price data does. See
    /// [`crate::alp`].
    Alp = 20,
}

impl EncodingType {
    /// Convert a `u8` tag to an `EncodingType`.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::UnsupportedEncoding`] for unknown tags.
    pub fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            0 => Ok(Self::DeltaOfDelta),
            1 => Ok(Self::Chimp),
            2 => Ok(Self::Gorilla),
            3 => Ok(Self::IntegerI64),
            4 => Ok(Self::IntegerU64),
            5 => Ok(Self::Dictionary),
            6 => Ok(Self::Bitmap),
            7 => Ok(Self::PlainF64),
            8 => Ok(Self::PlainI64),
            9 => Ok(Self::PlainU64),
            10 => Ok(Self::PlainBool),
            11 => Ok(Self::PlainString),
            12 => Ok(Self::Nullable),
            13 => Ok(Self::Rle),
            14 => Ok(Self::VarintI64),
            15 => Ok(Self::VarintU64),
            16 => Ok(Self::Chimp128),
            17 => Ok(Self::ForI64),
            18 => Ok(Self::ForU64),
            19 => Ok(Self::Patas),
            20 => Ok(Self::Alp),
            other => Err(EncodingError::UnsupportedEncoding { tag: other }),
        }
    }

    /// Return the `u8` discriminant.
    #[must_use]
    pub fn tag(self) -> u8 {
        self as u8
    }
}

impl std::fmt::Display for EncodingType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::DeltaOfDelta => "delta-of-delta",
            Self::Chimp => "chimp",
            Self::Gorilla => "gorilla",
            Self::IntegerI64 => "integer-i64",
            Self::IntegerU64 => "integer-u64",
            Self::Dictionary => "dictionary",
            Self::Bitmap => "bitmap",
            Self::PlainF64 => "plain-f64",
            Self::PlainI64 => "plain-i64",
            Self::PlainU64 => "plain-u64",
            Self::PlainBool => "plain-bool",
            Self::PlainString => "plain-string",
            Self::Rle => "rle",
            Self::Nullable => "nullable",
            Self::VarintI64 => "varint-i64",
            Self::VarintU64 => "varint-u64",
            Self::Chimp128 => "chimp128",
            Self::ForI64 => "for-i64",
            Self::ForU64 => "for-u64",
            Self::Patas => "patas",
            Self::Alp => "alp",
        })
    }
}

// ---------------------------------------------------------------------------
// EncodedBlock
// ---------------------------------------------------------------------------

/// An encoded column block ready to be written to a segment.
///
/// Contains the encoding type discriminant and the raw payload bytes.
#[derive(Debug, Clone)]
pub struct EncodedBlock {
    /// Which encoder produced this block.
    pub encoding: EncodingType,
    /// Raw encoded payload (without the type tag).
    pub payload: Vec<u8>,
}

impl EncodedBlock {
    /// Serialize to a byte vector: `[tag: u8][payload…]`.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + self.payload.len());
        out.push(self.encoding.tag());
        out.extend_from_slice(&self.payload);
        out
    }

    /// Parse an `EncodedBlock` from raw bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the tag byte is unknown or the data is empty.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(EncodingError::CorruptData {
                detail: "empty encoded block".to_string(),
            });
        }
        let encoding = EncodingType::from_tag(data[0])?;
        let payload = data[1..].to_vec();
        Ok(Self { encoding, payload })
    }
}

// ---------------------------------------------------------------------------
// Minimum compression ratio for specialized encoder selection
// ---------------------------------------------------------------------------

/// Specialized encoders must achieve at least this ratio (encoded < raw / RATIO)
/// or the plain encoder is used instead.
const MIN_COMPRESSION_RATIO: f64 = 1.5;

// ---------------------------------------------------------------------------
// ColumnEncoder
// ---------------------------------------------------------------------------

/// Unified column encoder that auto-selects the best encoding per column type.
///
/// Each `encode_*` method tries the specialised encoder for the column type
/// (delta-of-delta for timestamps, Chimp/Gorilla for floats, delta+ZigZag
/// for integers, dictionary for strings, bitmap for booleans) and
/// transparently falls back to [`PlainEncoder`] when
/// compression does not reach the minimum ratio threshold.
///
/// All methods return an [`EncodedBlock`] containing the encoding tag and
/// the raw payload.  Nullable variants wrap a validity bitmap around the
/// inner encoded payload.
#[derive(Debug, Clone, Copy)]
pub struct ColumnEncoder;

impl ColumnEncoder {
    /// Encode a timestamp column (sorted i64 nanoseconds).
    ///
    /// Uses delta-of-delta, falling back to plain if compression is poor.
    ///
    /// # Errors
    ///
    /// Returns an error if the input is empty.
    pub fn encode_timestamps(values: &[i64]) -> Result<EncodedBlock> {
        let raw_size = values.len() * 8;
        let payload = DeltaOfDeltaEncoder::encode(values)?;
        if should_use_specialized(&payload, raw_size) {
            Ok(EncodedBlock {
                encoding: EncodingType::DeltaOfDelta,
                payload,
            })
        } else {
            Ok(EncodedBlock {
                encoding: EncodingType::PlainI64,
                payload: PlainEncoder::encode_i64(values)?,
            })
        }
    }

    /// Encode a float column using the specified encoding strategy.
    ///
    /// Uses sample-based adaptive selection to avoid wasting CPU on
    /// triple-encoding: analyzes the first 1024 values to pick the best
    /// encoder, then encodes the full column once.
    ///
    /// # Errors
    ///
    /// Returns an error if the input is empty.
    pub fn encode_f64(values: &[f64], strategy: FloatEncoding) -> Result<EncodedBlock> {
        match strategy {
            FloatEncoding::Chimp => {
                // Sample-based selection: analyze first 1024 values to
                // pick the optimal encoder, then encode the full column
                // exactly once — avoids the previous triple-encode waste.
                let selector = crate::adaptive::AdaptiveSelector::new();
                selector.encode_f64_adaptive(values)
            }
            FloatEncoding::Gorilla => {
                let raw_size = values.len() * 8;
                let gorilla_payload = GorillaEncoder::encode(values)?;
                if should_use_specialized(&gorilla_payload, raw_size) {
                    return Ok(EncodedBlock {
                        encoding: EncodingType::Gorilla,
                        payload: gorilla_payload,
                    });
                }
                Ok(EncodedBlock {
                    encoding: EncodingType::PlainF64,
                    payload: PlainEncoder::encode_f64(values)?,
                })
            }
            FloatEncoding::Plain => Ok(EncodedBlock {
                encoding: EncodingType::PlainF64,
                payload: PlainEncoder::encode_f64(values)?,
            }),
        }
    }

    /// Encode a signed integer column.
    ///
    /// Tries RLE (constant), then both fixed-bit-width and varint delta
    /// encoding, picking the smallest result.  Falls back to plain if
    /// neither specialised encoding achieves sufficient compression.
    ///
    /// # Errors
    ///
    /// Returns an error if the input is empty.
    pub fn encode_i64(values: &[i64]) -> Result<EncodedBlock> {
        let selector = crate::adaptive::AdaptiveSelector::new();
        let pattern = selector.analyze_integers(values);

        // Try RLE for constant / repetitive data
        if pattern == crate::adaptive::IntegerPattern::Constant {
            let raw_size = values.len() * 8;
            let rle_payload = RleEncoder::encode_i64(values)?;
            if should_use_specialized(&rle_payload, raw_size) {
                return Ok(EncodedBlock {
                    encoding: EncodingType::Rle,
                    payload: rle_payload,
                });
            }
        }

        let raw_size = values.len() * 8;

        // Trial-encode with all integer encoders including FOR.
        let fixed_payload = IntegerEncoder::encode_i64(values)?;
        let varint_payload = VarintEncoder::encode_i64(values)?;
        let for_payload = ForEncoder::encode_i64(values)?;

        // Pick the smallest of fixed, varint, and FOR.
        let mut best_enc = EncodingType::IntegerI64;
        let mut best_payload = fixed_payload;

        if varint_payload.len() < best_payload.len() {
            best_enc = EncodingType::VarintI64;
            best_payload = varint_payload;
        }
        if for_payload.len() < best_payload.len() {
            best_enc = EncodingType::ForI64;
            best_payload = for_payload;
        }

        if should_use_specialized(&best_payload, raw_size) {
            Ok(EncodedBlock {
                encoding: best_enc,
                payload: best_payload,
            })
        } else {
            Ok(EncodedBlock {
                encoding: EncodingType::PlainI64,
                payload: PlainEncoder::encode_i64(values)?,
            })
        }
    }

    /// Encode an unsigned integer column.
    ///
    /// Tries RLE (constant), then both fixed-bit-width and varint delta
    /// encoding, picking the smallest result.  Falls back to plain if
    /// neither specialised encoding achieves sufficient compression.
    ///
    /// # Errors
    ///
    /// Returns an error if the input is empty.
    pub fn encode_u64(values: &[u64]) -> Result<EncodedBlock> {
        // Try RLE for constant / repetitive data — reinterpret as i64 for analysis
        let as_i64: Vec<i64> = values.iter().map(|&v| v as i64).collect();
        let selector = crate::adaptive::AdaptiveSelector::new();
        if selector.analyze_integers(&as_i64) == crate::adaptive::IntegerPattern::Constant {
            let raw_size = values.len() * 8;
            let rle_payload = RleEncoder::encode_u64(values)?;
            if should_use_specialized(&rle_payload, raw_size) {
                return Ok(EncodedBlock {
                    encoding: EncodingType::Rle,
                    payload: rle_payload,
                });
            }
        }

        let raw_size = values.len() * 8;
        let fixed_payload = IntegerEncoder::encode_u64(values)?;
        let varint_payload = VarintEncoder::encode_u64(values)?;
        let for_payload = ForEncoder::encode_u64(values)?;

        // Pick the smallest of fixed, varint, and FOR.
        let mut best_enc = EncodingType::IntegerU64;
        let mut best_payload = fixed_payload;

        if varint_payload.len() < best_payload.len() {
            best_enc = EncodingType::VarintU64;
            best_payload = varint_payload;
        }
        if for_payload.len() < best_payload.len() {
            best_enc = EncodingType::ForU64;
            best_payload = for_payload;
        }

        if should_use_specialized(&best_payload, raw_size) {
            Ok(EncodedBlock {
                encoding: best_enc,
                payload: best_payload,
            })
        } else {
            Ok(EncodedBlock {
                encoding: EncodingType::PlainU64,
                payload: PlainEncoder::encode_u64(values)?,
            })
        }
    }

    /// Encode a string/tag column.
    ///
    /// Uses dictionary encoding, falls back to plain if compression is poor
    /// or the dictionary overflows.
    ///
    /// # Errors
    ///
    /// Returns an error if the input is empty.
    pub fn encode_string(values: &[&str]) -> Result<EncodedBlock> {
        let raw_size: usize = values.iter().map(|s| 4 + s.len()).sum();

        match DictionaryEncoder::encode(values) {
            Ok(payload) if should_use_specialized(&payload, raw_size) => Ok(EncodedBlock {
                encoding: EncodingType::Dictionary,
                payload,
            }),
            // Dictionary didn't compress enough, or overflowed — fall back to plain
            Ok(_) | Err(EncodingError::DictionaryOverflow { .. }) => Ok(EncodedBlock {
                encoding: EncodingType::PlainString,
                payload: PlainEncoder::encode_string(values)?,
            }),
            // Propagate other errors (e.g. EmptyInput)
            Err(e) => Err(e),
        }
    }

    /// Encode a boolean column using 1-bit bitmap encoding.
    ///
    /// Always uses bitmap encoding (1 bit per value is optimal).
    ///
    /// # Errors
    ///
    /// Returns an error if the input is empty.
    pub fn encode_bool(values: &[bool]) -> Result<EncodedBlock> {
        // Try RLE for constant boolean columns
        if !values.is_empty() {
            let first = values[0];
            if values.iter().all(|&v| v == first) {
                let raw_size = values.len(); // 1 byte per bool raw
                let rle_payload = RleEncoder::encode_bool(values)?;
                if should_use_specialized(&rle_payload, raw_size) {
                    return Ok(EncodedBlock {
                        encoding: EncodingType::Rle,
                        payload: rle_payload,
                    });
                }
            }
        }
        Ok(EncodedBlock {
            encoding: EncodingType::Bitmap,
            payload: BitmapEncoder::encode(values)?,
        })
    }

    // ── Nullable encoding methods ────────────────────────────────────

    /// Encode a nullable float column.
    ///
    /// Separates non-null values, encodes them normally, then wraps with
    /// a validity bitmap so null positions are preserved on decode.
    ///
    /// # Errors
    ///
    /// Returns an error if all values are null (no data to encode) or
    /// the input is empty.
    pub fn encode_f64_nullable(
        values: &[Option<f64>],
        strategy: FloatEncoding,
    ) -> Result<EncodedBlock> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "nullable f64 encoder",
            });
        }
        let (non_null, validity) = extract_non_null_f64(values);
        if non_null.is_empty() {
            // All nulls — encode as nullable with an empty inner block
            return Ok(wrap_nullable(
                EncodingType::PlainF64,
                values.len(),
                &validity,
                &[],
            ));
        }
        let inner = Self::encode_f64(&non_null, strategy)?;
        Ok(wrap_nullable(
            inner.encoding,
            values.len(),
            &validity,
            &inner.payload,
        ))
    }

    /// Encode a nullable signed integer column.
    ///
    /// # Errors
    ///
    /// Returns an error if the input is empty.
    pub fn encode_i64_nullable(values: &[Option<i64>]) -> Result<EncodedBlock> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "nullable i64 encoder",
            });
        }
        let (non_null, validity) = extract_non_null_i64(values);
        if non_null.is_empty() {
            return Ok(wrap_nullable(
                EncodingType::PlainI64,
                values.len(),
                &validity,
                &[],
            ));
        }
        let inner = Self::encode_i64(&non_null)?;
        Ok(wrap_nullable(
            inner.encoding,
            values.len(),
            &validity,
            &inner.payload,
        ))
    }

    /// Encode a nullable unsigned integer column.
    ///
    /// # Errors
    ///
    /// Returns an error if the input is empty.
    pub fn encode_u64_nullable(values: &[Option<u64>]) -> Result<EncodedBlock> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "nullable u64 encoder",
            });
        }
        let (non_null, validity) = extract_non_null_u64(values);
        if non_null.is_empty() {
            return Ok(wrap_nullable(
                EncodingType::PlainU64,
                values.len(),
                &validity,
                &[],
            ));
        }
        let inner = Self::encode_u64(&non_null)?;
        Ok(wrap_nullable(
            inner.encoding,
            values.len(),
            &validity,
            &inner.payload,
        ))
    }

    /// Encode a nullable string column.
    ///
    /// # Errors
    ///
    /// Returns an error if the input is empty.
    pub fn encode_string_nullable(values: &[Option<&str>]) -> Result<EncodedBlock> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "nullable string encoder",
            });
        }
        let (non_null, validity): (Vec<&str>, Vec<bool>) = values
            .iter()
            .map(|v| match v {
                Some(s) => (*s, true),
                None => ("", false),
            })
            .unzip();
        let non_null_only: Vec<&str> = non_null
            .into_iter()
            .zip(validity.iter())
            .filter(|(_, v)| **v)
            .map(|(s, _)| s)
            .collect();
        if non_null_only.is_empty() {
            return Ok(wrap_nullable(
                EncodingType::PlainString,
                values.len(),
                &validity,
                &[],
            ));
        }
        let inner = Self::encode_string(&non_null_only)?;
        Ok(wrap_nullable(
            inner.encoding,
            values.len(),
            &validity,
            &inner.payload,
        ))
    }

    /// Encode a nullable boolean column.
    ///
    /// # Errors
    ///
    /// Returns an error if the input is empty.
    pub fn encode_bool_nullable(values: &[Option<bool>]) -> Result<EncodedBlock> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "nullable bool encoder",
            });
        }
        let validity: Vec<bool> = values.iter().map(std::option::Option::is_some).collect();
        let non_null: Vec<bool> = values.iter().filter_map(|v| *v).collect();
        if non_null.is_empty() {
            return Ok(wrap_nullable(
                EncodingType::Bitmap,
                values.len(),
                &validity,
                &[],
            ));
        }
        let inner = Self::encode_bool(&non_null)?;
        Ok(wrap_nullable(
            inner.encoding,
            values.len(),
            &validity,
            &inner.payload,
        ))
    }
}

// ---------------------------------------------------------------------------
// ColumnDecoder
// ---------------------------------------------------------------------------

/// Typed column values returned by [`ColumnDecoder`].
///
/// Each variant wraps a `Vec` of the corresponding Rust type.  Nullable
/// variants use `Option<T>` where `None` represents a null value.
///
/// Has a custom [`PartialEq`] implementation that uses bitwise comparison
/// for `f64` values so that `NaN == NaN` holds.
#[derive(Debug, Clone)]
pub enum DecodedColumn {
    /// Signed 64-bit integers (timestamps or i64 fields).
    I64(Vec<i64>),
    /// Unsigned 64-bit integers.
    U64(Vec<u64>),
    /// 64-bit floating point values.
    F64(Vec<f64>),
    /// Boolean values.
    Bool(Vec<bool>),
    /// UTF-8 string values.
    String(Vec<String>),
    /// Nullable signed 64-bit integers.
    NullableI64(Vec<Option<i64>>),
    /// Nullable unsigned 64-bit integers.
    NullableU64(Vec<Option<u64>>),
    /// Nullable 64-bit floating point values.
    NullableF64(Vec<Option<f64>>),
    /// Nullable boolean values.
    NullableBool(Vec<Option<bool>>),
    /// Nullable UTF-8 string values.
    NullableString(Vec<Option<String>>),
}

/// Bitwise f64 comparison helper (NaN-safe: NaN == NaN).
fn f64_slice_eq(a: &[f64], b: &[f64]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits())
}

/// Bitwise optional f64 comparison helper (NaN-safe).
fn opt_f64_slice_eq(a: &[Option<f64>], b: &[Option<f64>]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| match (x, y) {
            (Some(x), Some(y)) => x.to_bits() == y.to_bits(),
            (None, None) => true,
            _ => false,
        })
}

impl PartialEq for DecodedColumn {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::I64(a), Self::I64(b)) => a == b,
            (Self::U64(a), Self::U64(b)) => a == b,
            (Self::F64(a), Self::F64(b)) => f64_slice_eq(a, b),
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::String(a), Self::String(b)) => a == b,
            (Self::NullableI64(a), Self::NullableI64(b)) => a == b,
            (Self::NullableU64(a), Self::NullableU64(b)) => a == b,
            (Self::NullableF64(a), Self::NullableF64(b)) => opt_f64_slice_eq(a, b),
            (Self::NullableBool(a), Self::NullableBool(b)) => a == b,
            (Self::NullableString(a), Self::NullableString(b)) => a == b,
            _ => false,
        }
    }
}

impl DecodedColumn {
    /// Returns the number of values in this decoded column.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::I64(v) => v.len(),
            Self::U64(v) => v.len(),
            Self::F64(v) => v.len(),
            Self::Bool(v) => v.len(),
            Self::String(v) => v.len(),
            Self::NullableI64(v) => v.len(),
            Self::NullableU64(v) => v.len(),
            Self::NullableF64(v) => v.len(),
            Self::NullableBool(v) => v.len(),
            Self::NullableString(v) => v.len(),
        }
    }

    /// Returns `true` if this decoded column contains no values.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Unified column decoder that dispatches based on the [`EncodingType`] tag.
///
/// Reads the encoding discriminant from an [`EncodedBlock`] and delegates
/// to the corresponding specialised decoder (delta-of-delta, Chimp,
/// Gorilla, integer, dictionary, bitmap, plain, RLE, or nullable wrapper).
///
/// Returns a [`DecodedColumn`] that preserves the original column type.
#[derive(Debug, Clone, Copy)]
pub struct ColumnDecoder;

impl ColumnDecoder {
    /// Decode an [`EncodedBlock`] back to typed column values.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload is corrupt or the encoding type
    /// is unsupported.
    pub fn decode(block: &EncodedBlock) -> Result<DecodedColumn> {
        match block.encoding {
            EncodingType::DeltaOfDelta => {
                let values = DeltaOfDeltaDecoder::decode(&block.payload)?;
                Ok(DecodedColumn::I64(values))
            }
            EncodingType::Chimp => {
                let values = ChimpDecoder::decode(&block.payload)?;
                Ok(DecodedColumn::F64(values))
            }
            EncodingType::Chimp128 => {
                let values = Chimp128Decoder::decode(&block.payload)?;
                Ok(DecodedColumn::F64(values))
            }
            EncodingType::Patas => {
                let values = PatasDecoder::decode(&block.payload)?;
                Ok(DecodedColumn::F64(values))
            }
            EncodingType::Alp => {
                let values = AlpDecoder::decode(&block.payload)?;
                Ok(DecodedColumn::F64(values))
            }
            EncodingType::Gorilla => {
                let values = GorillaDecoder::decode(&block.payload)?;
                Ok(DecodedColumn::F64(values))
            }
            EncodingType::IntegerI64 => {
                let values = IntegerDecoder::decode_i64(&block.payload)?;
                Ok(DecodedColumn::I64(values))
            }
            EncodingType::IntegerU64 => {
                let values = IntegerDecoder::decode_u64(&block.payload)?;
                Ok(DecodedColumn::U64(values))
            }
            EncodingType::Dictionary => {
                let values = DictionaryDecoder::decode(&block.payload)?;
                Ok(DecodedColumn::String(values))
            }
            EncodingType::Bitmap => {
                let values = BitmapDecoder::decode(&block.payload)?;
                Ok(DecodedColumn::Bool(values))
            }
            EncodingType::PlainF64 => {
                let values = PlainDecoder::decode_f64(&block.payload)?;
                Ok(DecodedColumn::F64(values))
            }
            EncodingType::PlainI64 => {
                let values = PlainDecoder::decode_i64(&block.payload)?;
                Ok(DecodedColumn::I64(values))
            }
            EncodingType::PlainU64 => {
                let values = PlainDecoder::decode_u64(&block.payload)?;
                Ok(DecodedColumn::U64(values))
            }
            EncodingType::PlainBool => {
                let values = PlainDecoder::decode_bool(&block.payload)?;
                Ok(DecodedColumn::Bool(values))
            }
            EncodingType::PlainString => {
                let values = PlainDecoder::decode_string(&block.payload)?;
                Ok(DecodedColumn::String(values))
            }
            EncodingType::Nullable => Self::decode_nullable(&block.payload),
            EncodingType::Rle => Self::decode_rle(&block.payload),
            EncodingType::VarintI64 => {
                let values = VarintDecoder::decode_i64(&block.payload)?;
                Ok(DecodedColumn::I64(values))
            }
            EncodingType::VarintU64 => {
                let values = VarintDecoder::decode_u64(&block.payload)?;
                Ok(DecodedColumn::U64(values))
            }
            EncodingType::ForI64 => {
                let values = ForDecoder::decode_i64(&block.payload)?;
                Ok(DecodedColumn::I64(values))
            }
            EncodingType::ForU64 => {
                let values = ForDecoder::decode_u64(&block.payload)?;
                Ok(DecodedColumn::U64(values))
            }
        }
    }

    /// Decode a nullable-wrapped payload.
    ///
    /// # Wire format
    ///
    /// ```text
    /// [inner_encoding: u8][total_count: u32 LE][validity_bitmap: ceil(count/8) bytes][inner_payload…]
    /// ```
    fn decode_nullable(payload: &[u8]) -> Result<DecodedColumn> {
        if payload.len() < 5 {
            return Err(EncodingError::CorruptData {
                detail: "nullable payload too short".to_string(),
            });
        }

        let inner_enc = EncodingType::from_tag(payload[0])?;
        // The bitmap length bounds this to 8× the payload, but an 8× blow-up
        // from a corrupt header is still worth refusing at the same ceiling
        // every other decoder uses.
        let total_count = crate::coding::checked_decode_count(
            u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]) as usize,
            "nullable",
        )?;
        let bitmap_bytes = total_count.div_ceil(8);
        let header_len = 5 + bitmap_bytes;

        if payload.len() < header_len {
            return Err(EncodingError::CorruptData {
                detail: "nullable bitmap truncated".to_string(),
            });
        }

        let bitmap = &payload[5..header_len];
        let inner_payload = &payload[header_len..];

        // Build validity vector
        let validity: Vec<bool> = (0..total_count)
            .map(|i| bitmap[i / 8] & (1 << (i % 8)) != 0)
            .collect();

        let non_null_count = validity.iter().filter(|v| **v).count();

        // All nulls — no inner data
        if non_null_count == 0 {
            return match inner_enc {
                EncodingType::PlainF64
                | EncodingType::Chimp
                | EncodingType::Chimp128
                | EncodingType::Gorilla
                | EncodingType::Patas
                | EncodingType::Alp => Ok(DecodedColumn::NullableF64(vec![None; total_count])),
                EncodingType::PlainI64
                | EncodingType::IntegerI64
                | EncodingType::VarintI64
                | EncodingType::ForI64
                | EncodingType::DeltaOfDelta => {
                    Ok(DecodedColumn::NullableI64(vec![None; total_count]))
                }
                EncodingType::PlainU64
                | EncodingType::IntegerU64
                | EncodingType::VarintU64
                | EncodingType::ForU64 => Ok(DecodedColumn::NullableU64(vec![None; total_count])),
                EncodingType::PlainString | EncodingType::Dictionary => {
                    Ok(DecodedColumn::NullableString(vec![None; total_count]))
                }
                EncodingType::Bitmap | EncodingType::PlainBool => {
                    Ok(DecodedColumn::NullableBool(vec![None; total_count]))
                }
                _ => Err(EncodingError::CorruptData {
                    detail: format!("unexpected inner encoding in nullable: {inner_enc}"),
                }),
            };
        }

        // Decode non-null values
        let inner_block = EncodedBlock {
            encoding: inner_enc,
            payload: inner_payload.to_vec(),
        };
        let decoded = Self::decode(&inner_block)?;

        // Scatter non-null values back into positions using validity bitmap
        match decoded {
            DecodedColumn::F64(vals) => Ok(DecodedColumn::NullableF64(scatter(vals, &validity))),
            DecodedColumn::I64(vals) => Ok(DecodedColumn::NullableI64(scatter(vals, &validity))),
            DecodedColumn::U64(vals) => Ok(DecodedColumn::NullableU64(scatter(vals, &validity))),
            DecodedColumn::Bool(vals) => Ok(DecodedColumn::NullableBool(scatter(vals, &validity))),
            DecodedColumn::String(vals) => {
                Ok(DecodedColumn::NullableString(scatter(vals, &validity)))
            }
            _ => Err(EncodingError::CorruptData {
                detail: "unexpected decoded type in nullable wrapper".to_string(),
            }),
        }
    }

    /// Decode an RLE-encoded payload, dispatching by the value-type byte.
    fn decode_rle(payload: &[u8]) -> Result<DecodedColumn> {
        let vtype = RleDecoder::value_type(payload)?;
        match vtype {
            0 => Ok(DecodedColumn::I64(RleDecoder::decode_i64(payload)?)),
            1 => Ok(DecodedColumn::U64(RleDecoder::decode_u64(payload)?)),
            2 => Ok(DecodedColumn::F64(RleDecoder::decode_f64(payload)?)),
            3 => Ok(DecodedColumn::Bool(RleDecoder::decode_bool(payload)?)),
            4 => Ok(DecodedColumn::String(RleDecoder::decode_string(payload)?)),
            other => Err(EncodingError::CorruptData {
                detail: format!("unknown RLE value type: {other}"),
            }),
        }
    }

    /// Convenience: decode from raw bytes (tag + payload).
    ///
    /// # Errors
    ///
    /// Returns an error if the data is empty, the tag is unknown, or
    /// the payload is corrupt.
    pub fn decode_bytes(data: &[u8]) -> Result<DecodedColumn> {
        let block = EncodedBlock::from_bytes(data)?;
        Self::decode(&block)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns `true` if the specialized encoder achieved at least
/// [`MIN_COMPRESSION_RATIO`].
pub(crate) fn should_use_specialized(encoded: &[u8], raw_size: usize) -> bool {
    if raw_size == 0 {
        return false;
    }
    #[allow(clippy::cast_precision_loss)]
    let ratio = raw_size as f64 / encoded.len() as f64;
    ratio >= MIN_COMPRESSION_RATIO
}

// ---------------------------------------------------------------------------
// Nullable encoding helpers
// ---------------------------------------------------------------------------

/// Build a validity bitmap from a boolean slice.
///
/// Bit `1` at position `i` means value `i` is present (non-null).
fn encode_validity_bitmap(validity: &[bool]) -> Vec<u8> {
    let byte_count = validity.len().div_ceil(8);
    let mut bitmap = vec![0u8; byte_count];
    for (i, &valid) in validity.iter().enumerate() {
        if valid {
            bitmap[i / 8] |= 1 << (i % 8);
        }
    }
    bitmap
}

/// Wrap an encoded inner block with a nullable header.
fn wrap_nullable(
    inner_enc: EncodingType,
    total_count: usize,
    validity: &[bool],
    inner_payload: &[u8],
) -> EncodedBlock {
    let bitmap = encode_validity_bitmap(validity);
    let mut payload = Vec::with_capacity(1 + 4 + bitmap.len() + inner_payload.len());
    payload.push(inner_enc.tag());
    payload.extend_from_slice(&(total_count as u32).to_le_bytes());
    payload.extend_from_slice(&bitmap);
    payload.extend_from_slice(inner_payload);
    EncodedBlock {
        encoding: EncodingType::Nullable,
        payload,
    }
}

/// Extract non-null `f64` values and a validity bitmap.
fn extract_non_null_f64(values: &[Option<f64>]) -> (Vec<f64>, Vec<bool>) {
    let validity: Vec<bool> = values.iter().map(std::option::Option::is_some).collect();
    let non_null: Vec<f64> = values.iter().filter_map(|v| *v).collect();
    (non_null, validity)
}

/// Extract non-null `i64` values and a validity bitmap.
fn extract_non_null_i64(values: &[Option<i64>]) -> (Vec<i64>, Vec<bool>) {
    let validity: Vec<bool> = values.iter().map(std::option::Option::is_some).collect();
    let non_null: Vec<i64> = values.iter().filter_map(|v| *v).collect();
    (non_null, validity)
}

/// Extract non-null `u64` values and a validity bitmap.
fn extract_non_null_u64(values: &[Option<u64>]) -> (Vec<u64>, Vec<bool>) {
    let validity: Vec<bool> = values.iter().map(std::option::Option::is_some).collect();
    let non_null: Vec<u64> = values.iter().filter_map(|v| *v).collect();
    (non_null, validity)
}

/// Scatter non-null values back into their positions using a validity bitmap.
///
/// `values` contains only the non-null elements in order.
/// `validity[i]` is `true` for positions that have a value.
fn scatter<T: Default + Clone>(values: Vec<T>, validity: &[bool]) -> Vec<Option<T>> {
    let mut result = Vec::with_capacity(validity.len());
    let mut val_iter = values.into_iter();
    for &valid in validity {
        if valid {
            result.push(val_iter.next().map(Some).unwrap_or(None));
        } else {
            result.push(None);
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- EncodingType tag roundtrip ----------------------------------------

    #[test]
    fn encoding_type_tag_roundtrip() {
        let all = [
            EncodingType::DeltaOfDelta,
            EncodingType::Chimp,
            EncodingType::Gorilla,
            EncodingType::IntegerI64,
            EncodingType::IntegerU64,
            EncodingType::Dictionary,
            EncodingType::Bitmap,
            EncodingType::PlainF64,
            EncodingType::PlainI64,
            EncodingType::PlainU64,
            EncodingType::PlainBool,
            EncodingType::PlainString,
            EncodingType::Nullable,
            EncodingType::Rle,
        ];
        for enc in &all {
            let tag = enc.tag();
            let recovered = EncodingType::from_tag(tag).unwrap();
            assert_eq!(*enc, recovered);
        }
    }

    #[test]
    fn encoding_type_unknown_tag() {
        assert!(EncodingType::from_tag(255).is_err());
    }

    // -- EncodedBlock serialization ---------------------------------------

    #[test]
    fn encoded_block_roundtrip() {
        let block = EncodedBlock {
            encoding: EncodingType::Chimp,
            payload: vec![1, 2, 3, 4],
        };
        let bytes = block.to_bytes();
        assert_eq!(bytes[0], EncodingType::Chimp.tag());
        let recovered = EncodedBlock::from_bytes(&bytes).unwrap();
        assert_eq!(recovered.encoding, block.encoding);
        assert_eq!(recovered.payload, block.payload);
    }

    #[test]
    fn encoded_block_empty_error() {
        assert!(EncodedBlock::from_bytes(&[]).is_err());
    }

    // -- Timestamp encoding -----------------------------------------------

    #[test]
    fn encode_timestamps_monotonic() {
        let ts: Vec<i64> = (0..1000).map(|i| 1_000_000 + i * 10_000).collect();
        let block = ColumnEncoder::encode_timestamps(&ts).unwrap();
        // Should use delta-of-delta for monotonic timestamps
        assert_eq!(block.encoding, EncodingType::DeltaOfDelta);
        let decoded = ColumnDecoder::decode(&block).unwrap();
        assert_eq!(decoded, DecodedColumn::I64(ts));
    }

    #[test]
    fn encode_timestamps_bytes_roundtrip() {
        let ts: Vec<i64> = (0..100).map(|i| 1_000_000 + i * 10_000).collect();
        let block = ColumnEncoder::encode_timestamps(&ts).unwrap();
        let raw = block.to_bytes();
        let decoded = ColumnDecoder::decode_bytes(&raw).unwrap();
        assert_eq!(decoded, DecodedColumn::I64(ts));
    }

    // -- Float encoding ---------------------------------------------------

    #[test]
    fn encode_f64_adaptive_strategy() {
        // `FloatEncoding::Chimp` runs the adaptive selector; on two-decimal
        // values that means ALP.
        let values: Vec<f64> = (0..500).map(|i| 20.0 + (i as f64) * 0.01).collect();
        let block = ColumnEncoder::encode_f64(&values, FloatEncoding::Chimp).unwrap();
        assert_eq!(block.encoding, EncodingType::Alp);
        let decoded = ColumnDecoder::decode(&block).unwrap();
        assert_eq!(decoded, DecodedColumn::F64(values));
    }

    #[test]
    fn encode_f64_gorilla_strategy() {
        let values: Vec<f64> = (0..500).map(|i| 20.0 + (i as f64) * 0.01).collect();
        let block = ColumnEncoder::encode_f64(&values, FloatEncoding::Gorilla).unwrap();
        assert!(
            block.encoding == EncodingType::Gorilla || block.encoding == EncodingType::PlainF64
        );
        let decoded = ColumnDecoder::decode(&block).unwrap();
        assert_eq!(decoded, DecodedColumn::F64(values));
    }

    #[test]
    fn encode_f64_plain_strategy() {
        let values = vec![1.0, 2.0, 3.0];
        let block = ColumnEncoder::encode_f64(&values, FloatEncoding::Plain).unwrap();
        assert_eq!(block.encoding, EncodingType::PlainF64);
        let decoded = ColumnDecoder::decode(&block).unwrap();
        assert_eq!(decoded, DecodedColumn::F64(values));
    }

    // -- Integer encoding -------------------------------------------------

    #[test]
    fn encode_i64_with_fallback() {
        let values: Vec<i64> = (0..1000).collect();
        let block = ColumnEncoder::encode_i64(&values).unwrap();
        assert!(
            block.encoding == EncodingType::IntegerI64 || block.encoding == EncodingType::PlainI64
        );
        let decoded = ColumnDecoder::decode(&block).unwrap();
        assert_eq!(decoded, DecodedColumn::I64(values));
    }

    #[test]
    fn encode_u64_with_fallback() {
        let values: Vec<u64> = (0..1000).collect();
        let block = ColumnEncoder::encode_u64(&values).unwrap();
        assert!(
            block.encoding == EncodingType::IntegerU64 || block.encoding == EncodingType::PlainU64
        );
        let decoded = ColumnDecoder::decode(&block).unwrap();
        assert_eq!(decoded, DecodedColumn::U64(values));
    }

    // -- String encoding --------------------------------------------------

    #[test]
    fn encode_string_low_cardinality() {
        let raw = vec!["us-east-1"; 500];
        let block = ColumnEncoder::encode_string(&raw).unwrap();
        assert_eq!(block.encoding, EncodingType::Dictionary);
        let decoded = ColumnDecoder::decode(&block).unwrap();
        let expected: Vec<String> = raw.iter().map(|s| (*s).to_owned()).collect();
        assert_eq!(decoded, DecodedColumn::String(expected));
    }

    #[test]
    fn encode_string_high_cardinality_fallback() {
        // All unique — dictionary may still compress, or fall back to plain
        let raw: Vec<String> = (0..200).map(|i| format!("uuid-{i}")).collect();
        let refs: Vec<&str> = raw.iter().map(String::as_str).collect();
        let block = ColumnEncoder::encode_string(&refs).unwrap();
        let decoded = ColumnDecoder::decode(&block).unwrap();
        assert_eq!(decoded, DecodedColumn::String(raw));
    }

    // -- Boolean encoding -------------------------------------------------

    #[test]
    fn encode_bool_bitmap() {
        let values: Vec<bool> = (0..100).map(|i| i % 3 == 0).collect();
        let block = ColumnEncoder::encode_bool(&values).unwrap();
        assert_eq!(block.encoding, EncodingType::Bitmap);
        let decoded = ColumnDecoder::decode(&block).unwrap();
        assert_eq!(decoded, DecodedColumn::Bool(values));
    }

    // -- Empty input errors -----------------------------------------------

    #[test]
    fn empty_inputs_error() {
        assert!(ColumnEncoder::encode_timestamps(&[]).is_err());
        assert!(ColumnEncoder::encode_f64(&[], FloatEncoding::Chimp).is_err());
        assert!(ColumnEncoder::encode_i64(&[]).is_err());
        assert!(ColumnEncoder::encode_u64(&[]).is_err());
        let empty: &[&str] = &[];
        assert!(ColumnEncoder::encode_string(empty).is_err());
        assert!(ColumnEncoder::encode_bool(&[]).is_err());
    }

    // -- MIN_COMPRESSION_RATIO helper -------------------------------------

    #[test]
    fn should_use_specialized_logic() {
        // 100 bytes raw, 50 bytes encoded → 2× → should use
        assert!(should_use_specialized(&[0u8; 50], 100));
        // 100 bytes raw, 80 bytes encoded → 1.25× → should NOT use
        assert!(!should_use_specialized(&[0u8; 80], 100));
        // 0 raw → false
        assert!(!should_use_specialized(&[0u8; 10], 0));
    }

    // -- Nullable encoding roundtrips -------------------------------------

    #[test]
    fn nullable_f64_mixed() {
        let values = vec![Some(1.0), None, Some(3.0), None, Some(5.0)];
        let block = ColumnEncoder::encode_f64_nullable(&values, FloatEncoding::Plain).unwrap();
        assert_eq!(block.encoding, EncodingType::Nullable);
        let decoded = ColumnDecoder::decode(&block).unwrap();
        assert_eq!(decoded, DecodedColumn::NullableF64(values));
    }

    #[test]
    fn nullable_f64_all_present() {
        let values = vec![Some(1.0), Some(2.0), Some(3.0)];
        let block = ColumnEncoder::encode_f64_nullable(&values, FloatEncoding::Plain).unwrap();
        let decoded = ColumnDecoder::decode(&block).unwrap();
        assert_eq!(decoded, DecodedColumn::NullableF64(values));
    }

    #[test]
    fn nullable_f64_all_null() {
        let values: Vec<Option<f64>> = vec![None, None, None];
        let block = ColumnEncoder::encode_f64_nullable(&values, FloatEncoding::Plain).unwrap();
        let decoded = ColumnDecoder::decode(&block).unwrap();
        assert_eq!(decoded, DecodedColumn::NullableF64(values));
    }

    #[test]
    fn nullable_i64_mixed() {
        let values = vec![Some(10), None, Some(30), Some(40), None];
        let block = ColumnEncoder::encode_i64_nullable(&values).unwrap();
        assert_eq!(block.encoding, EncodingType::Nullable);
        let decoded = ColumnDecoder::decode(&block).unwrap();
        assert_eq!(decoded, DecodedColumn::NullableI64(values));
    }

    #[test]
    fn nullable_u64_mixed() {
        let values = vec![None, Some(100u64), Some(200), None];
        let block = ColumnEncoder::encode_u64_nullable(&values).unwrap();
        let decoded = ColumnDecoder::decode(&block).unwrap();
        assert_eq!(decoded, DecodedColumn::NullableU64(values));
    }

    #[test]
    fn nullable_string_mixed() {
        let values = vec![Some("hello"), None, Some("world"), None];
        let block = ColumnEncoder::encode_string_nullable(&values).unwrap();
        let decoded = ColumnDecoder::decode(&block).unwrap();
        let expected: Vec<Option<String>> =
            values.iter().map(|v| v.map(ToString::to_string)).collect();
        assert_eq!(decoded, DecodedColumn::NullableString(expected));
    }

    #[test]
    fn nullable_f64_bytes_roundtrip() {
        let values = vec![Some(1.0), None, Some(3.0)];
        let block = ColumnEncoder::encode_f64_nullable(&values, FloatEncoding::Plain).unwrap();
        let raw = block.to_bytes();
        let decoded = ColumnDecoder::decode_bytes(&raw).unwrap();
        assert_eq!(decoded, DecodedColumn::NullableF64(values));
    }

    #[test]
    fn nullable_empty_errors() {
        let empty_f64: &[Option<f64>] = &[];
        assert!(ColumnEncoder::encode_f64_nullable(empty_f64, FloatEncoding::Plain).is_err());
        let empty_i64: &[Option<i64>] = &[];
        assert!(ColumnEncoder::encode_i64_nullable(empty_i64).is_err());
        let empty_u64: &[Option<u64>] = &[];
        assert!(ColumnEncoder::encode_u64_nullable(empty_u64).is_err());
        let empty_str: &[Option<&str>] = &[];
        assert!(ColumnEncoder::encode_string_nullable(empty_str).is_err());
        let empty_bool: &[Option<bool>] = &[];
        assert!(ColumnEncoder::encode_bool_nullable(empty_bool).is_err());
    }

    #[test]
    fn nullable_distinguishes_zero_from_null() {
        // The critical test: 0.0 is encoded as 0.0, null is encoded as None.
        // Without it, zeroes are indistinguishable from nulls.
        let values = vec![Some(0.0), None, Some(0.0), None];
        let block = ColumnEncoder::encode_f64_nullable(&values, FloatEncoding::Plain).unwrap();
        let decoded = ColumnDecoder::decode(&block).unwrap();
        match &decoded {
            DecodedColumn::NullableF64(vals) => {
                assert_eq!(vals[0], Some(0.0));
                assert_eq!(vals[1], None); // null, NOT 0.0
                assert_eq!(vals[2], Some(0.0));
                assert_eq!(vals[3], None); // null, NOT 0.0
            }
            other => panic!("Expected NullableF64, got {other:?}"),
        }
    }

    #[test]
    fn nullable_i64_distinguishes_zero_from_null() {
        let values = vec![Some(0i64), None, Some(0), None, Some(42)];
        let block = ColumnEncoder::encode_i64_nullable(&values).unwrap();
        let decoded = ColumnDecoder::decode(&block).unwrap();
        match &decoded {
            DecodedColumn::NullableI64(vals) => {
                assert_eq!(vals[0], Some(0));
                assert_eq!(vals[1], None);
                assert_eq!(vals[2], Some(0));
                assert_eq!(vals[3], None);
                assert_eq!(vals[4], Some(42));
            }
            other => panic!("Expected NullableI64, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip_timestamps(mut values in proptest::collection::vec(any::<i64>(), 1..300)) {
            values.sort_unstable();
            let block = ColumnEncoder::encode_timestamps(&values).unwrap();
            let decoded = ColumnDecoder::decode(&block).unwrap();
            prop_assert_eq!(DecodedColumn::I64(values), decoded);
        }

        #[test]
        fn roundtrip_i64(values in proptest::collection::vec(any::<i64>(), 1..300)) {
            let block = ColumnEncoder::encode_i64(&values).unwrap();
            let decoded = ColumnDecoder::decode(&block).unwrap();
            prop_assert_eq!(DecodedColumn::I64(values), decoded);
        }

        #[test]
        fn roundtrip_u64(values in proptest::collection::vec(any::<u64>(), 1..300)) {
            let block = ColumnEncoder::encode_u64(&values).unwrap();
            let decoded = ColumnDecoder::decode(&block).unwrap();
            prop_assert_eq!(DecodedColumn::U64(values), decoded);
        }

        #[test]
        fn roundtrip_f64(values in proptest::collection::vec(any::<f64>(), 1..300)) {
            let block = ColumnEncoder::encode_f64(&values, FloatEncoding::Gorilla).unwrap();
            let decoded = ColumnDecoder::decode(&block).unwrap();
            prop_assert_eq!(DecodedColumn::F64(values), decoded);
        }

        #[test]
        fn roundtrip_bool(values in proptest::collection::vec(any::<bool>(), 1..300)) {
            let block = ColumnEncoder::encode_bool(&values).unwrap();
            let decoded = ColumnDecoder::decode(&block).unwrap();
            prop_assert_eq!(DecodedColumn::Bool(values), decoded);
        }

        #[test]
        fn roundtrip_string(values in proptest::collection::vec("[a-z]{0,8}", 1..100)) {
            let refs: Vec<&str> = values.iter().map(String::as_str).collect();
            let block = ColumnEncoder::encode_string(&refs).unwrap();
            let decoded = ColumnDecoder::decode(&block).unwrap();
            prop_assert_eq!(DecodedColumn::String(values), decoded);
        }

        #[test]
        fn roundtrip_nullable_i64(values in proptest::collection::vec(any::<Option<i64>>(), 1..300)) {
            let block = ColumnEncoder::encode_i64_nullable(&values).unwrap();
            let decoded = ColumnDecoder::decode(&block).unwrap();
            prop_assert_eq!(DecodedColumn::NullableI64(values), decoded);
        }

        #[test]
        fn roundtrip_nullable_u64(values in proptest::collection::vec(any::<Option<u64>>(), 1..300)) {
            let block = ColumnEncoder::encode_u64_nullable(&values).unwrap();
            let decoded = ColumnDecoder::decode(&block).unwrap();
            prop_assert_eq!(DecodedColumn::NullableU64(values), decoded);
        }

        #[test]
        fn roundtrip_nullable_f64(values in proptest::collection::vec(any::<Option<f64>>(), 1..300)) {
            let block = ColumnEncoder::encode_f64_nullable(&values, FloatEncoding::Gorilla).unwrap();
            let decoded = ColumnDecoder::decode(&block).unwrap();
            prop_assert_eq!(DecodedColumn::NullableF64(values), decoded);
        }

        #[test]
        fn roundtrip_nullable_bool(values in proptest::collection::vec(any::<Option<bool>>(), 1..300)) {
            let block = ColumnEncoder::encode_bool_nullable(&values).unwrap();
            let decoded = ColumnDecoder::decode(&block).unwrap();
            prop_assert_eq!(DecodedColumn::NullableBool(values), decoded);
        }

        #[test]
        fn roundtrip_nullable_string(values in proptest::collection::vec(
            proptest::option::of("[a-z]{0,8}"), 1..100
        )) {
            let refs: Vec<Option<&str>> = values.iter().map(|o| o.as_deref()).collect();
            let block = ColumnEncoder::encode_string_nullable(&refs).unwrap();
            let decoded = ColumnDecoder::decode(&block).unwrap();
            let expected: Vec<Option<String>> = values.clone();
            prop_assert_eq!(DecodedColumn::NullableString(expected), decoded);
        }
    }
}
