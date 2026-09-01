//! Run-Length Encoding (RLE) for columns with repeated consecutive values.
//!
//! Time-series data frequently contains runs of identical values (e.g.
//! constant status codes, boolean flags, repeated tags).  RLE encodes
//! each run as `(value, run_length)`, which can dramatically reduce size
//! when long runs are present.
//!
//! ## Wire format
//!
//! ```text
//! [value_type: u8][num_values: u32 LE][num_runs: u32 LE][run_data…]
//! ```
//!
//! `value_type`: 0=i64, 1=u64, 2=f64, 3=bool, 4=string
//!
//! ### Numeric types (i64 / u64 / f64)
//!
//! Each run: `[value: 8 bytes LE][run_length: u32 LE]`
//!
//! ### Bool
//!
//! Each run: `[value: 1 byte (0 or 1)][run_length: u32 LE]`
//!
//! ### String
//!
//! Each run: `[string_len: u32 LE][string_bytes…][run_length: u32 LE]`

use crate::coding::{checked_count, checked_decode_count};
use crate::error::{EncodingError, Result};

/// Value type discriminants stored at the start of an RLE payload.
const VALUE_TYPE_I64: u8 = 0;
const VALUE_TYPE_U64: u8 = 1;
const VALUE_TYPE_F64: u8 = 2;
const VALUE_TYPE_BOOL: u8 = 3;
const VALUE_TYPE_STRING: u8 = 4;

// ---------------------------------------------------------------------------
// Encoder
// ---------------------------------------------------------------------------

/// Run-Length Encoder for data with repeated consecutive values.
pub struct RleEncoder;

/// Run-Length Decoder.
pub struct RleDecoder;

// ---------------------------------------------------------------------------
// i64
// ---------------------------------------------------------------------------

impl RleEncoder {
    /// Encode a slice of `i64` values using run-length encoding.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty, or
    /// [`EncodingError::CorruptData`] if the count exceeds `u32::MAX`.
    pub fn encode_i64(values: &[i64]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "RLE i64 encoder",
            });
        }
        let num_values = checked_count(values.len())?;
        let runs = build_runs(values, |a, b| a == b);
        let num_runs = checked_count(runs.len())?;

        let mut buf = Vec::with_capacity(9 + runs.len() * 12);
        buf.push(VALUE_TYPE_I64);
        buf.extend_from_slice(&num_values.to_le_bytes());
        buf.extend_from_slice(&num_runs.to_le_bytes());
        for (val, run_len) in &runs {
            buf.extend_from_slice(&val.to_le_bytes());
            buf.extend_from_slice(&run_len.to_le_bytes());
        }
        Ok(buf)
    }

    /// Encode a slice of `u64` values using run-length encoding.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode_u64(values: &[u64]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "RLE u64 encoder",
            });
        }
        let num_values = checked_count(values.len())?;
        let runs = build_runs(values, |a, b| a == b);
        let num_runs = checked_count(runs.len())?;

        let mut buf = Vec::with_capacity(9 + runs.len() * 12);
        buf.push(VALUE_TYPE_U64);
        buf.extend_from_slice(&num_values.to_le_bytes());
        buf.extend_from_slice(&num_runs.to_le_bytes());
        for (val, run_len) in &runs {
            buf.extend_from_slice(&val.to_le_bytes());
            buf.extend_from_slice(&run_len.to_le_bytes());
        }
        Ok(buf)
    }

    /// Encode a slice of `f64` values using run-length encoding.
    ///
    /// Uses `to_bits()` for equality comparison so that `NaN == NaN`.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode_f64(values: &[f64]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "RLE f64 encoder",
            });
        }
        let num_values = checked_count(values.len())?;
        let runs = build_runs(values, |a, b| a.to_bits() == b.to_bits());
        let num_runs = checked_count(runs.len())?;

        let mut buf = Vec::with_capacity(9 + runs.len() * 12);
        buf.push(VALUE_TYPE_F64);
        buf.extend_from_slice(&num_values.to_le_bytes());
        buf.extend_from_slice(&num_runs.to_le_bytes());
        for (val, run_len) in &runs {
            buf.extend_from_slice(&val.to_bits().to_le_bytes());
            buf.extend_from_slice(&run_len.to_le_bytes());
        }
        Ok(buf)
    }

    /// Encode a slice of `bool` values using run-length encoding.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode_bool(values: &[bool]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "RLE bool encoder",
            });
        }
        let num_values = checked_count(values.len())?;
        let runs = build_runs(values, |a, b| a == b);
        let num_runs = checked_count(runs.len())?;

        let mut buf = Vec::with_capacity(9 + runs.len() * 5);
        buf.push(VALUE_TYPE_BOOL);
        buf.extend_from_slice(&num_values.to_le_bytes());
        buf.extend_from_slice(&num_runs.to_le_bytes());
        for (val, run_len) in &runs {
            buf.push(u8::from(*val));
            buf.extend_from_slice(&run_len.to_le_bytes());
        }
        Ok(buf)
    }

    /// Encode a slice of string references using run-length encoding.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode_string(values: &[&str]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "RLE string encoder",
            });
        }
        let num_values = checked_count(values.len())?;
        let runs = build_runs(values, |a, b| a == b);
        let num_runs = checked_count(runs.len())?;

        let mut buf = Vec::with_capacity(9 + runs.len() * 16);
        buf.push(VALUE_TYPE_STRING);
        buf.extend_from_slice(&num_values.to_le_bytes());
        buf.extend_from_slice(&num_runs.to_le_bytes());
        for (val, run_len) in &runs {
            let s_bytes = val.as_bytes();
            let s_len = checked_count(s_bytes.len())?;
            buf.extend_from_slice(&s_len.to_le_bytes());
            buf.extend_from_slice(s_bytes);
            buf.extend_from_slice(&run_len.to_le_bytes());
        }
        Ok(buf)
    }
}

// ---------------------------------------------------------------------------
// Decoder
// ---------------------------------------------------------------------------

impl RleDecoder {
    /// Peek at the value-type discriminant in an RLE payload and dispatch
    /// to the appropriate typed decoder.
    ///
    /// This is used by [`crate::unified::ColumnDecoder`] to decode an
    /// `EncodingType::Rle` block without knowing the column type in advance.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload is too short or the type byte is
    /// unknown.
    pub fn value_type(data: &[u8]) -> Result<u8> {
        if data.is_empty() {
            return Err(corrupt("RLE payload empty — no value type byte"));
        }
        Ok(data[0])
    }

    /// Decode an RLE-encoded `i64` payload.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is truncated or
    /// the run lengths don't sum to the declared value count.
    pub fn decode_i64(data: &[u8]) -> Result<Vec<i64>> {
        let (num_values, num_runs, mut pos) = read_header(data)?;
        let mut result = Vec::with_capacity(num_values);
        for _ in 0..num_runs {
            if pos + 12 > data.len() {
                return Err(corrupt("i64 run data truncated"));
            }
            let val = i64::from_le_bytes(
                data[pos..pos + 8]
                    .try_into()
                    .map_err(|_| corrupt("i64 value bytes"))?,
            );
            pos += 8;
            let run_len = u32::from_le_bytes(
                data[pos..pos + 4]
                    .try_into()
                    .map_err(|_| corrupt("i64 run length bytes"))?,
            ) as usize;
            pos += 4;
            checked_run(result.len(), run_len, num_values)?;
            result.extend(std::iter::repeat_n(val, run_len));
        }
        verify_count(num_values, result.len())?;
        Ok(result)
    }

    /// Decode an RLE-encoded `u64` payload.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] on truncation or count mismatch.
    pub fn decode_u64(data: &[u8]) -> Result<Vec<u64>> {
        let (num_values, num_runs, mut pos) = read_header(data)?;
        let mut result = Vec::with_capacity(num_values);
        for _ in 0..num_runs {
            if pos + 12 > data.len() {
                return Err(corrupt("u64 run data truncated"));
            }
            let val = u64::from_le_bytes(
                data[pos..pos + 8]
                    .try_into()
                    .map_err(|_| corrupt("u64 value bytes"))?,
            );
            pos += 8;
            let run_len = u32::from_le_bytes(
                data[pos..pos + 4]
                    .try_into()
                    .map_err(|_| corrupt("u64 run length bytes"))?,
            ) as usize;
            pos += 4;
            checked_run(result.len(), run_len, num_values)?;
            result.extend(std::iter::repeat_n(val, run_len));
        }
        verify_count(num_values, result.len())?;
        Ok(result)
    }

    /// Decode an RLE-encoded `f64` payload.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] on truncation or count mismatch.
    pub fn decode_f64(data: &[u8]) -> Result<Vec<f64>> {
        let (num_values, num_runs, mut pos) = read_header(data)?;
        let mut result = Vec::with_capacity(num_values);
        for _ in 0..num_runs {
            if pos + 12 > data.len() {
                return Err(corrupt("f64 run data truncated"));
            }
            let bits = u64::from_le_bytes(
                data[pos..pos + 8]
                    .try_into()
                    .map_err(|_| corrupt("f64 value bytes"))?,
            );
            let val = f64::from_bits(bits);
            pos += 8;
            let run_len = u32::from_le_bytes(
                data[pos..pos + 4]
                    .try_into()
                    .map_err(|_| corrupt("f64 run length bytes"))?,
            ) as usize;
            pos += 4;
            checked_run(result.len(), run_len, num_values)?;
            result.extend(std::iter::repeat_n(val, run_len));
        }
        verify_count(num_values, result.len())?;
        Ok(result)
    }

    /// Decode an RLE-encoded `bool` payload.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] on truncation or count mismatch.
    pub fn decode_bool(data: &[u8]) -> Result<Vec<bool>> {
        let (num_values, num_runs, mut pos) = read_header(data)?;
        let mut result = Vec::with_capacity(num_values);
        for _ in 0..num_runs {
            if pos + 5 > data.len() {
                return Err(corrupt("bool run data truncated"));
            }
            let val = data[pos] != 0;
            pos += 1;
            let run_len = u32::from_le_bytes(
                data[pos..pos + 4]
                    .try_into()
                    .map_err(|_| corrupt("bool run length bytes"))?,
            ) as usize;
            pos += 4;
            checked_run(result.len(), run_len, num_values)?;
            result.extend(std::iter::repeat_n(val, run_len));
        }
        verify_count(num_values, result.len())?;
        Ok(result)
    }

    /// Decode an RLE-encoded string payload.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] on truncation, invalid UTF-8,
    /// or count mismatch.
    pub fn decode_string(data: &[u8]) -> Result<Vec<String>> {
        let (num_values, num_runs, mut pos) = read_header(data)?;
        let mut result = Vec::with_capacity(num_values);
        for _ in 0..num_runs {
            if pos + 4 > data.len() {
                return Err(corrupt("string run: string_len truncated"));
            }
            let s_len = u32::from_le_bytes(
                data[pos..pos + 4]
                    .try_into()
                    .map_err(|_| corrupt("string length bytes"))?,
            ) as usize;
            pos += 4;
            if pos + s_len > data.len() {
                return Err(corrupt("string run: string bytes truncated"));
            }
            let s = std::str::from_utf8(&data[pos..pos + s_len])
                .map_err(|e| EncodingError::CorruptData {
                    detail: format!("RLE string: invalid UTF-8: {e}"),
                })?
                .to_owned();
            pos += s_len;
            if pos + 4 > data.len() {
                return Err(corrupt("string run: run_length truncated"));
            }
            let run_len = u32::from_le_bytes(
                data[pos..pos + 4]
                    .try_into()
                    .map_err(|_| corrupt("string run length bytes"))?,
            ) as usize;
            pos += 4;
            checked_run(result.len(), run_len, num_values)?;
            result.extend(std::iter::repeat_n(s, run_len));
        }
        verify_count(num_values, result.len())?;
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Build runs from a slice using a custom equality function.
fn build_runs<T: Copy, F: Fn(&T, &T) -> bool>(values: &[T], eq: F) -> Vec<(T, u32)> {
    let mut runs = Vec::new();
    if values.is_empty() {
        return runs;
    }
    let mut current = values[0];
    let mut count: u32 = 1;
    for &v in &values[1..] {
        if eq(&v, &current) {
            // Split the run at u32::MAX to prevent silent data loss
            // on decode (saturating_add would cap the count, producing
            // fewer values than were encoded).
            if count == u32::MAX {
                runs.push((current, count));
                count = 1;
            } else {
                count += 1;
            }
        } else {
            runs.push((current, count));
            current = v;
            count = 1;
        }
    }
    runs.push((current, count));
    runs
}

/// Smallest number of payload bytes one run can occupy.
///
/// The boolean form is the cheapest: a one-byte value plus a `u32` run
/// length. It is what bounds `num_runs` against the payload actually present.
const MIN_RUN_BYTES: usize = 5;

/// Read the common `[value_type: u8][num_values: u32][num_runs: u32]` header.
///
/// Returns `(num_values, num_runs, position_after_header)`.
///
/// Both counts are validated here rather than by the caller, because RLE was
/// the one decoder that reached neither guard. `num_values` sized a
/// `Vec::with_capacity` directly from the header — a 9-byte payload declaring
/// `u32::MAX` reserved 34 GiB — the same unbounded-header defect the other
/// codecs carry a ceiling for. `num_runs` was trusted the same way and drove the
/// decode loop.
fn read_header(data: &[u8]) -> Result<(usize, usize, usize)> {
    // 1 byte type + 4 bytes num_values + 4 bytes num_runs = 9
    if data.len() < 9 {
        return Err(corrupt("RLE header too short"));
    }
    // data[0] is value_type — callers check it separately
    let num_values = checked_decode_count(
        u32::from_le_bytes(
            data[1..5]
                .try_into()
                .map_err(|_| corrupt("header num_values bytes"))?,
        ) as usize,
        "RLE",
    )?;
    let num_runs = u32::from_le_bytes(
        data[5..9]
            .try_into()
            .map_err(|_| corrupt("header num_runs bytes"))?,
    ) as usize;
    let max_runs = (data.len() - 9) / MIN_RUN_BYTES;
    if num_runs > max_runs {
        return Err(corrupt(
            "RLE block declares more runs than the payload can hold",
        ));
    }
    Ok((num_values, num_runs, 9))
}

/// Check that a run fits inside the block's declared value count *before*
/// materialising it.
///
/// Bounding `num_values` alone is not enough: run lengths are independent
/// `u32`s, so a block declaring three values and one run of `u32::MAX`
/// expands to 34 GiB and only then fails `verify_count`. The count check has
/// to happen before the `extend`, not after it — a post-hoc check on an
/// allocation that already happened is not a bound.
#[inline]
fn checked_run(decoded: usize, run_len: usize, num_values: usize) -> Result<()> {
    if decoded + run_len > num_values {
        return Err(corrupt(
            "RLE run lengths exceed the declared value count — header is corrupt",
        ));
    }
    Ok(())
}

/// Verify that the decoded count matches the declared count.
fn verify_count(expected: usize, actual: usize) -> Result<()> {
    if expected != actual {
        return Err(EncodingError::CountMismatch { expected, actual });
    }
    Ok(())
}

/// Shorthand for a corrupt-data error.
fn corrupt(detail: &str) -> EncodingError {
    EncodingError::CorruptData {
        detail: detail.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── i64 ──────────────────────────────────────────────────────────

    #[test]
    fn i64_constant_roundtrip() {
        let values = vec![42_i64; 1000];
        let encoded = RleEncoder::encode_i64(&values).unwrap();
        // 1 run -> header(8) + 1*(8+4) = 20 bytes, much smaller than 8000
        assert!(encoded.len() < values.len() * 8);
        let decoded = RleDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn i64_varied_roundtrip() {
        let values: Vec<i64> = (0..500).collect();
        let encoded = RleEncoder::encode_i64(&values).unwrap();
        let decoded = RleDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn i64_runs_roundtrip() {
        // 3 runs: 100×1, 200×2, 100×3
        let mut values = Vec::new();
        values.extend(std::iter::repeat_n(1_i64, 100));
        values.extend(std::iter::repeat_n(2_i64, 200));
        values.extend(std::iter::repeat_n(3_i64, 100));
        let encoded = RleEncoder::encode_i64(&values).unwrap();
        let decoded = RleDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn i64_single_value() {
        let values = vec![99_i64];
        let encoded = RleEncoder::encode_i64(&values).unwrap();
        let decoded = RleDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn i64_empty_error() {
        assert!(RleEncoder::encode_i64(&[]).is_err());
    }

    #[test]
    fn i64_negative_values() {
        let values = vec![-1_i64; 50];
        let encoded = RleEncoder::encode_i64(&values).unwrap();
        let decoded = RleDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    // ── u64 ──────────────────────────────────────────────────────────

    #[test]
    fn u64_constant_roundtrip() {
        let values = vec![100_u64; 500];
        let encoded = RleEncoder::encode_u64(&values).unwrap();
        let decoded = RleDecoder::decode_u64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn u64_varied_roundtrip() {
        let values: Vec<u64> = (0..200).collect();
        let encoded = RleEncoder::encode_u64(&values).unwrap();
        let decoded = RleDecoder::decode_u64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn u64_empty_error() {
        assert!(RleEncoder::encode_u64(&[]).is_err());
    }

    // ── f64 ──────────────────────────────────────────────────────────

    #[test]
    fn f64_constant_roundtrip() {
        let values = vec![2.75_f64; 1000];
        let encoded = RleEncoder::encode_f64(&values).unwrap();
        assert!(encoded.len() < values.len() * 8);
        let decoded = RleDecoder::decode_f64(&encoded).unwrap();
        assert_eq!(values.len(), decoded.len());
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn f64_nan_roundtrip() {
        let values = vec![f64::NAN; 100];
        let encoded = RleEncoder::encode_f64(&values).unwrap();
        let decoded = RleDecoder::decode_f64(&encoded).unwrap();
        assert_eq!(values.len(), decoded.len());
        for v in &decoded {
            assert!(v.is_nan());
        }
    }

    #[test]
    fn f64_mixed_special_values() {
        let values = vec![
            f64::INFINITY,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
            0.0,
            -0.0, // -0.0 has different bits than 0.0
        ];
        let encoded = RleEncoder::encode_f64(&values).unwrap();
        let decoded = RleDecoder::decode_f64(&encoded).unwrap();
        assert_eq!(values.len(), decoded.len());
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn f64_empty_error() {
        assert!(RleEncoder::encode_f64(&[]).is_err());
    }

    // ── bool ─────────────────────────────────────────────────────────

    #[test]
    fn bool_constant_roundtrip() {
        let values = vec![true; 500];
        let encoded = RleEncoder::encode_bool(&values).unwrap();
        assert!(encoded.len() < values.len());
        let decoded = RleDecoder::decode_bool(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn bool_alternating_roundtrip() {
        let values: Vec<bool> = (0..100).map(|i| i % 2 == 0).collect();
        let encoded = RleEncoder::encode_bool(&values).unwrap();
        let decoded = RleDecoder::decode_bool(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn bool_runs_roundtrip() {
        let mut values = Vec::new();
        values.extend(std::iter::repeat_n(true, 50));
        values.extend(std::iter::repeat_n(false, 100));
        values.extend(std::iter::repeat_n(true, 30));
        let encoded = RleEncoder::encode_bool(&values).unwrap();
        let decoded = RleDecoder::decode_bool(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn bool_empty_error() {
        assert!(RleEncoder::encode_bool(&[]).is_err());
    }

    // ── string ───────────────────────────────────────────────────────

    #[test]
    fn string_constant_roundtrip() {
        let values = vec!["us-east-1"; 200];
        let encoded = RleEncoder::encode_string(&values).unwrap();
        let decoded = RleDecoder::decode_string(&encoded).unwrap();
        let expected: Vec<String> = values.iter().map(|s| (*s).to_owned()).collect();
        assert_eq!(expected, decoded);
    }

    #[test]
    fn string_runs_roundtrip() {
        let values = vec!["a", "a", "a", "b", "b", "c", "c", "c", "c"];
        let encoded = RleEncoder::encode_string(&values).unwrap();
        let decoded = RleDecoder::decode_string(&encoded).unwrap();
        let expected: Vec<String> = values.iter().map(|s| (*s).to_owned()).collect();
        assert_eq!(expected, decoded);
    }

    #[test]
    fn string_varied_roundtrip() {
        let owned: Vec<String> = (0..50).map(|i| format!("val-{i}")).collect();
        let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
        let encoded = RleEncoder::encode_string(&refs).unwrap();
        let decoded = RleDecoder::decode_string(&encoded).unwrap();
        assert_eq!(owned, decoded);
    }

    #[test]
    fn string_empty_strings() {
        let values = vec![""; 100];
        let encoded = RleEncoder::encode_string(&values).unwrap();
        let decoded = RleDecoder::decode_string(&encoded).unwrap();
        let expected: Vec<String> = values.iter().map(|s| (*s).to_owned()).collect();
        assert_eq!(expected, decoded);
    }

    #[test]
    fn string_empty_error() {
        let empty: &[&str] = &[];
        assert!(RleEncoder::encode_string(empty).is_err());
    }

    // ── corruption / edge cases ──────────────────────────────────────

    #[test]
    fn decode_truncated_header() {
        assert!(RleDecoder::decode_i64(&[0, 1, 2, 3]).is_err());
    }

    #[test]
    fn decode_truncated_run() {
        // Valid header: type=i64, 1 value, 1 run, but missing run data
        let mut data = vec![VALUE_TYPE_I64];
        data.extend_from_slice(&1_u32.to_le_bytes());
        data.extend_from_slice(&1_u32.to_le_bytes());
        assert!(RleDecoder::decode_i64(&data).is_err());
    }

    #[test]
    fn decode_count_mismatch() {
        // Header says 999 values but data has 1 run of length 1
        let mut data = vec![VALUE_TYPE_I64];
        data.extend_from_slice(&999_u32.to_le_bytes()); // num_values = 999
        data.extend_from_slice(&1_u32.to_le_bytes()); // num_runs = 1
        data.extend_from_slice(&42_i64.to_le_bytes()); // value
        data.extend_from_slice(&1_u32.to_le_bytes()); // run_length = 1
        assert!(RleDecoder::decode_i64(&data).is_err());
    }

    // ── compression ratio ────────────────────────────────────────────

    #[test]
    fn constant_column_achieves_high_compression() {
        let values = vec![42_i64; 10_000];
        let encoded = RleEncoder::encode_i64(&values).unwrap();
        // 10k × 8 = 80_000 bytes raw; RLE should be 20 bytes
        let ratio = (values.len() * 8) as f64 / encoded.len() as f64;
        assert!(ratio > 100.0, "Expected >100× compression, got {ratio:.1}×");
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip_i64(values in proptest::collection::vec(any::<i64>(), 1..300)) {
            let encoded = RleEncoder::encode_i64(&values).unwrap();
            let decoded = RleDecoder::decode_i64(&encoded).unwrap();
            prop_assert_eq!(&values, &decoded);
        }

        #[test]
        fn roundtrip_u64(values in proptest::collection::vec(any::<u64>(), 1..300)) {
            let encoded = RleEncoder::encode_u64(&values).unwrap();
            let decoded = RleDecoder::decode_u64(&encoded).unwrap();
            prop_assert_eq!(&values, &decoded);
        }

        #[test]
        fn roundtrip_f64(values in proptest::collection::vec(any::<f64>(), 1..300)) {
            let encoded = RleEncoder::encode_f64(&values).unwrap();
            let decoded = RleDecoder::decode_f64(&encoded).unwrap();
            // Bitwise comparison for f64 (NaN == NaN)
            prop_assert_eq!(values.len(), decoded.len());
            for (a, b) in values.iter().zip(decoded.iter()) {
                prop_assert_eq!(a.to_bits(), b.to_bits());
            }
        }

        #[test]
        fn roundtrip_bool(values in proptest::collection::vec(any::<bool>(), 1..300)) {
            let encoded = RleEncoder::encode_bool(&values).unwrap();
            let decoded = RleDecoder::decode_bool(&encoded).unwrap();
            prop_assert_eq!(&values, &decoded);
        }

        #[test]
        fn roundtrip_string(values in proptest::collection::vec("[a-z]{0,8}", 1..100)) {
            let refs: Vec<&str> = values.iter().map(String::as_str).collect();
            let encoded = RleEncoder::encode_string(&refs).unwrap();
            let decoded = RleDecoder::decode_string(&encoded).unwrap();
            prop_assert_eq!(&values, &decoded);
        }
    }
}

#[cfg(test)]
mod bomb_tests {
    use super::*;

    /// A nine-byte header must not size an allocation.
    ///
    /// RLE was the last decoder to gain a ceiling: `read_header` fed
    /// `num_values` straight into `Vec::with_capacity`, so a nine-byte
    /// payload declaring `u32::MAX` reserved 34 GiB. This is the ALP finding
    /// in the codec the ALP fix did not cover, and it was reachable from a
    /// single flipped bit on the flash it decodes from.
    #[test]
    fn a_declared_value_count_cannot_size_an_allocation() {
        let mut payload = vec![VALUE_TYPE_I64];
        payload.extend_from_slice(&u32::MAX.to_le_bytes()); // num_values
        payload.extend_from_slice(&0u32.to_le_bytes()); // num_runs
        let err = RleDecoder::decode_i64(&payload).expect_err("must refuse the count");
        assert!(
            err.to_string().contains("ceiling"),
            "expected the decode ceiling to reject it, got: {err}"
        );
    }

    /// A single run must not expand past the declared count.
    ///
    /// Bounding `num_values` alone leaves the run lengths unbounded: they are
    /// independent `u32`s, and `verify_count` only ran *after* the expansion
    /// had already been materialised. Three declared values and one run of
    /// `u32::MAX` is 34 GiB of `repeat_n` before the mismatch is noticed — a
    /// check after the allocation is not a bound.
    #[test]
    fn a_run_length_cannot_expand_past_the_declared_count() {
        let mut payload = vec![VALUE_TYPE_I64];
        payload.extend_from_slice(&3u32.to_le_bytes()); // num_values
        payload.extend_from_slice(&1u32.to_le_bytes()); // num_runs
        payload.extend_from_slice(&7i64.to_le_bytes()); // value
        payload.extend_from_slice(&u32::MAX.to_le_bytes()); // run length
        let err = RleDecoder::decode_i64(&payload).expect_err("must refuse the run");
        assert!(
            err.to_string().contains("exceed the declared value count"),
            "expected the run guard to reject it, got: {err}"
        );
    }

    /// The same guard on the string form, where each element is 24 bytes.
    #[test]
    fn a_string_run_cannot_expand_past_the_declared_count() {
        let mut payload = vec![VALUE_TYPE_STRING];
        payload.extend_from_slice(&1u32.to_le_bytes()); // num_values
        payload.extend_from_slice(&1u32.to_le_bytes()); // num_runs
        payload.extend_from_slice(&1u32.to_le_bytes()); // string length
        payload.push(b'x');
        payload.extend_from_slice(&u32::MAX.to_le_bytes()); // run length
        let err = RleDecoder::decode_string(&payload).expect_err("must refuse the run");
        assert!(
            err.to_string().contains("exceed the declared value count"),
            "expected the run guard to reject it, got: {err}"
        );
    }

    /// A run count the payload cannot possibly hold is a corrupt header, and
    /// saying so beats iterating `num_runs` times to discover it.
    #[test]
    fn a_run_count_beyond_the_payload_is_rejected() {
        let mut payload = vec![VALUE_TYPE_BOOL];
        payload.extend_from_slice(&10u32.to_le_bytes()); // num_values
        payload.extend_from_slice(&1_000_000u32.to_le_bytes()); // num_runs
        let err = RleDecoder::decode_bool(&payload).expect_err("must refuse the run count");
        assert!(
            err.to_string().contains("more runs than the payload"),
            "expected the run-count guard to reject it, got: {err}"
        );
    }

    /// And a legitimate block still round-trips: the guards bound corruption,
    /// they do not narrow the format.
    #[test]
    fn legitimate_blocks_still_round_trip() {
        let values: Vec<i64> = std::iter::repeat_n(4, 1000)
            .chain(std::iter::repeat_n(9, 500))
            .collect();
        let encoded = RleEncoder::encode_i64(&values).expect("encode");
        assert_eq!(RleDecoder::decode_i64(&encoded).expect("decode"), values);
    }
}
