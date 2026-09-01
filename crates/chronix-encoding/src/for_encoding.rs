//! Frame-of-Reference (FOR) encoding for integer columns.
//!
//! FOR encoding stores each value as a small, fixed-width offset from
//! a per-block reference value (the minimum). This is optimal for columns
//! where all values fall within a **narrow range** — e.g. HTTP status codes,
//! sensor readings that hover around a baseline, or enum-style identifiers.
//!
//! Unlike delta encoding (which stores differences between *consecutive*
//! values), FOR stores differences from a *global* reference (the block
//! minimum). This makes FOR ideal for data with a tight value range but
//! no predictable ordering, while delta encoding excels at monotonic or
//! slowly-changing sequences.
//!
//! ## Wire format
//!
//! ```text
//! [count: u32 LE][reference: i64 LE][bit_width: u8][packed offsets…]
//! ```
//!
//! - **count** — number of values (max `u32::MAX`).
//! - **reference** — the minimum value in the block.
//! - **bit_width** — bits needed to represent the largest offset from
//!   reference (0–64). When 0, all values equal the reference.
//! - **packed offsets** — each offset packed at `bit_width` bits, MSB-first.

use crate::coding::{bits_needed, checked_count, checked_decode_count, pack_bits, unpack_bits};
use crate::error::{EncodingError, Result};

/// Header size: `count(4) + reference(8) + bit_width(1)` = 13 bytes.
const HEADER_SIZE: usize = 13;

/// Frame-of-Reference encoder for integer columns.
///
/// Stores each value as `value - reference` using the minimum number of
/// bits. Values must fit in `i64`.
#[derive(Debug, Clone, Copy)]
pub struct ForEncoder;

/// Frame-of-Reference decoder.
#[derive(Debug, Clone, Copy)]
pub struct ForDecoder;

impl ForEncoder {
    /// Encode a sequence of `i64` values using FOR.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode_i64(values: &[i64]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "FOR encoder (i64)",
            });
        }

        let count = checked_count(values.len())?;

        // Find the minimum value to use as reference.
        let reference = values.iter().copied().min().unwrap_or(0);

        // Compute offsets and max offset for bit-width calculation.
        // Subtracting the minimum from any value in the range gives a
        // non-negative result, but we use wrapping_sub to avoid panic
        // and reinterpret as u64 (safe because min ≤ v for all v).
        let offsets: Vec<u64> = values
            .iter()
            .map(|&v| v.wrapping_sub(reference) as u64)
            .collect();
        let max_offset = offsets.iter().copied().max().unwrap_or(0);
        let bit_width = bits_needed(max_offset);

        // Pre-allocate: header + packed bits
        let data_bits = offsets.len() as u64 * u64::from(bit_width);
        let data_bytes = data_bits.div_ceil(8) as usize;
        let mut buf = Vec::with_capacity(HEADER_SIZE + data_bytes);

        // Header
        buf.extend_from_slice(&count.to_le_bytes());
        buf.extend_from_slice(&reference.to_le_bytes());
        buf.push(bit_width);

        if bit_width == 0 {
            // All values equal — no data section needed.
            return Ok(buf);
        }

        // Pack offsets at bit_width bits each
        pack_bits(&offsets, bit_width, &mut buf);

        Ok(buf)
    }

    /// Encode a sequence of `u64` values using FOR.
    ///
    /// Uses native u64 arithmetic to avoid bit-width inflation at the
    /// i64 sign boundary (values near `i64::MAX` / `i64::MIN`).
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode_u64(values: &[u64]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "FOR encoder (u64)",
            });
        }

        let count = checked_count(values.len())?;

        let reference = values.iter().copied().min().unwrap_or(0);

        let offsets: Vec<u64> = values.iter().map(|&v| v - reference).collect();
        let max_offset = offsets.iter().copied().max().unwrap_or(0);
        let bit_width = bits_needed(max_offset);

        let data_bits = offsets.len() as u64 * u64::from(bit_width);
        let data_bytes = data_bits.div_ceil(8) as usize;
        let mut buf = Vec::with_capacity(HEADER_SIZE + data_bytes);

        // Store reference as i64 (reinterpret-cast preserves bits).
        buf.extend_from_slice(&count.to_le_bytes());
        buf.extend_from_slice(&(reference as i64).to_le_bytes());
        buf.push(bit_width);

        if bit_width == 0 {
            return Ok(buf);
        }

        pack_bits(&offsets, bit_width, &mut buf);

        Ok(buf)
    }
}

impl ForDecoder {
    /// Decode a FOR-encoded byte buffer back to `i64` values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is malformed.
    pub fn decode_i64(data: &[u8]) -> Result<Vec<i64>> {
        if data.len() < HEADER_SIZE {
            return Err(EncodingError::CorruptData {
                detail: "FOR data too short for header".to_string(),
            });
        }

        let count = checked_decode_count(
            u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize,
            "FOR",
        )?;
        let reference = i64::from_le_bytes([
            data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
        ]);
        let bit_width = data[12];

        if bit_width > 64 {
            return Err(EncodingError::CorruptData {
                detail: format!("invalid FOR bit_width {bit_width}, max is 64"),
            });
        }

        if count == 0 {
            return Ok(Vec::new());
        }

        // bit_width == 0 → all values equal the reference
        if bit_width == 0 {
            return Ok(vec![reference; count]);
        }

        // Unpack offsets
        let packed_data = &data[HEADER_SIZE..];
        let offsets = unpack_bits(packed_data, count, bit_width)?;

        // SIMD-friendly batch add scalar
        let mut values = Vec::new();
        crate::simd::batch_add_scalar_i64(reference, &offsets, &mut values);

        Ok(values)
    }

    /// Decode a FOR-encoded byte buffer back to `u64` values.
    ///
    /// Uses native u64 arithmetic, matching the encoder's native u64 path.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is malformed.
    pub fn decode_u64(data: &[u8]) -> Result<Vec<u64>> {
        if data.len() < HEADER_SIZE {
            return Err(EncodingError::CorruptData {
                detail: "FOR data too short for header".to_string(),
            });
        }

        let count = checked_decode_count(
            u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize,
            "FOR",
        )?;
        let reference_i64 = i64::from_le_bytes([
            data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
        ]);
        let reference = reference_i64 as u64;
        let bit_width = data[12];

        if bit_width > 64 {
            return Err(EncodingError::CorruptData {
                detail: format!("invalid FOR bit_width {bit_width}, max is 64"),
            });
        }

        if count == 0 {
            return Ok(Vec::new());
        }

        if bit_width == 0 {
            return Ok(vec![reference; count]);
        }

        let packed_data = &data[HEADER_SIZE..];
        let offsets = unpack_bits(packed_data, count, bit_width)?;

        // SIMD-friendly batch add scalar
        let mut values = Vec::new();
        crate::simd::batch_add_scalar_u64(reference, &offsets, &mut values);

        Ok(values)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_monotonic() {
        let values: Vec<i64> = (100..200).collect();
        let encoded = ForEncoder::encode_i64(&values).unwrap();
        let decoded = ForDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn roundtrip_constant() {
        let values = vec![42_i64; 1000];
        let encoded = ForEncoder::encode_i64(&values).unwrap();
        // Constant → bit_width == 0, so payload is just the header.
        assert_eq!(encoded.len(), 13);
        let decoded = ForDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn roundtrip_narrow_range() {
        // HTTP status codes: 200, 201, 204, 301, 302, 400, 404, 500, 502, 503
        let values = vec![200, 201, 204, 301, 302, 400, 404, 500, 502, 503];
        let encoded = ForEncoder::encode_i64(&values).unwrap();
        let decoded = ForDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);

        // range = 503 - 200 = 303 → 9 bits needed
        // 13 header + ceil(10 * 9 / 8) = 13 + 12 = 25 bytes
        // vs plain: 80 bytes → 3.2× compression
        assert!(encoded.len() < 80);
    }

    #[test]
    fn roundtrip_single_value() {
        let values = vec![i64::MAX];
        let encoded = ForEncoder::encode_i64(&values).unwrap();
        let decoded = ForDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn roundtrip_negative_values() {
        let values = vec![-100, -50, -10, -5, -1, 0, 5, 10, 50, 100];
        let encoded = ForEncoder::encode_i64(&values).unwrap();
        let decoded = ForDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn roundtrip_u64() {
        let values: Vec<u64> = (1000..1100).collect();
        let encoded = ForEncoder::encode_u64(&values).unwrap();
        let decoded = ForDecoder::decode_u64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn empty_input_error() {
        assert!(ForEncoder::encode_i64(&[]).is_err());
        assert!(ForEncoder::encode_u64(&[]).is_err());
    }

    #[test]
    fn corrupt_data_too_short() {
        assert!(ForDecoder::decode_i64(&[0; 5]).is_err());
    }

    #[test]
    fn corrupt_bit_width_too_large() {
        let mut data = vec![0u8; 13];
        // count = 1
        data[0] = 1;
        // bit_width = 65 (invalid)
        data[12] = 65;
        assert!(ForDecoder::decode_i64(&data).is_err());
    }

    #[test]
    fn compression_vs_plain_narrow_range() {
        // 1000 values in range [1000, 1015] → 4 bits each
        let values: Vec<i64> = (0..1000).map(|i| 1000 + (i % 16)).collect();
        let raw_size = values.len() * 8; // 8000 bytes
        let encoded = ForEncoder::encode_i64(&values).unwrap();
        // Expected: 13 + ceil(1000 * 4 / 8) = 13 + 500 = 513 bytes
        assert!(
            encoded.len() < raw_size / 10,
            "FOR should achieve >10× for 4-bit range"
        );
        let decoded = ForDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn compression_worse_for_wide_range() {
        // Values spanning the full i64 range — FOR cannot help
        let values = vec![i64::MIN, 0, i64::MAX];
        let encoded = ForEncoder::encode_i64(&values).unwrap();
        // 3 values × 64 bits each = 24 bytes data + 13 header = 37 bytes
        // vs plain: 24 bytes
        // FOR is worse here, which is correct — the adaptive selector
        // should not choose FOR for wide-range data.
        let decoded = ForDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn roundtrip_two_values() {
        let values = vec![10_i64, 20];
        let encoded = ForEncoder::encode_i64(&values).unwrap();
        let decoded = ForDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn roundtrip_all_same_negative() {
        let values = vec![-999_i64; 500];
        let encoded = ForEncoder::encode_i64(&values).unwrap();
        assert_eq!(encoded.len(), 13); // header only, bit_width = 0
        let decoded = ForDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn u64_sign_boundary_narrow_range() {
        // Values near the i64 sign boundary — reinterpret-as-i64 would
        // see min = i64::MAX and max = i64::MIN, inflating bit_width to 64.
        // Native u64 FOR correctly computes range = 2 → 2 bits.
        let mid = (1u64 << 63) - 1; // i64::MAX as u64
        let values = vec![mid, mid + 1, mid + 2];
        let encoded = ForEncoder::encode_u64(&values).unwrap();
        let decoded = ForDecoder::decode_u64(&encoded).unwrap();
        assert_eq!(values, decoded);

        // Range is 2 → 2 bits per value → 13 + ceil(3*2/8) = 14 bytes.
        assert!(
            encoded.len() <= 14,
            "expected ≤14 bytes, got {}",
            encoded.len()
        );
    }

    #[test]
    fn u64_large_values() {
        let values = vec![u64::MAX - 10, u64::MAX - 5, u64::MAX];
        let encoded = ForEncoder::encode_u64(&values).unwrap();
        let decoded = ForDecoder::decode_u64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip_i64(values in proptest::collection::vec(any::<i64>(), 1..500)) {
            let encoded = ForEncoder::encode_i64(&values).unwrap();
            let decoded = ForDecoder::decode_i64(&encoded).unwrap();
            prop_assert_eq!(&values, &decoded);
        }

        #[test]
        fn roundtrip_u64(values in proptest::collection::vec(any::<u64>(), 1..500)) {
            let encoded = ForEncoder::encode_u64(&values).unwrap();
            let decoded = ForDecoder::decode_u64(&encoded).unwrap();
            prop_assert_eq!(&values, &decoded);
        }
    }
}
