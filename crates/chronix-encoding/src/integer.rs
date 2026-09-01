//! Integer encoding using Delta + `ZigZag` + variable-length bit-packing.
//!
//! ## Encoders
//!
//! - [`IntegerEncoder`]: Delta + ZigZag + **fixed-width** bit-packing.
//!   Optimal for monotonic or slowly-changing sequences where all deltas
//!   have a similar magnitude.
//!
//! - [`VarintEncoder`]: Delta + ZigZag + **LEB128 varint** per delta.
//!   Optimal for sparse or highly variable data where most deltas are
//!   small but occasional outliers would inflate the fixed bit width.
//!
//! Both encoders compute deltas between consecutive values and ZigZag-encode
//! them, but differ in how the unsigned deltas are serialised.

use crate::coding::{
    bits_needed, checked_count, checked_decode_count, pack_bits, unpack_bits, varint_decode,
    varint_encode, zigzag_decode, zigzag_encode,
};
use crate::error::{EncodingError, Result};

/// Integer column encoder (Delta + `ZigZag` + bit-packing).
#[derive(Debug, Clone, Copy)]
pub struct IntegerEncoder;

/// Integer column decoder.
#[derive(Debug, Clone, Copy)]
pub struct IntegerDecoder;

impl IntegerEncoder {
    /// Encode a sequence of `i64` values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode_i64(values: &[i64]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "integer encoder (i64)",
            });
        }

        // Compute deltas and zigzag-encode them
        let mut zigzag_deltas = Vec::with_capacity(values.len().saturating_sub(1));
        let mut max_zz: u64 = 0;

        for i in 1..values.len() {
            let delta = values[i].wrapping_sub(values[i - 1]);
            let zz = zigzag_encode(delta);
            max_zz = max_zz.max(zz);
            zigzag_deltas.push(zz);
        }

        let bit_width = bits_needed(max_zz);

        // Header: [count: u32][first_value: i64][bit_width: u8]
        let count = checked_count(values.len())?;
        let header_size = 4 + 8 + 1;
        let data_bits = zigzag_deltas.len() as u64 * u64::from(bit_width);
        let data_bytes = data_bits.div_ceil(8) as usize;
        let mut buf = Vec::with_capacity(header_size + data_bytes);

        // Count
        buf.extend_from_slice(&count.to_le_bytes());
        // First value
        buf.extend_from_slice(&values[0].to_le_bytes());
        // Bit width
        buf.push(bit_width);

        if bit_width == 0 {
            // All deltas are 0 — constant sequence, no data needed
            return Ok(buf);
        }

        // Pack deltas at bit_width bits each
        pack_bits(&zigzag_deltas, bit_width, &mut buf);

        Ok(buf)
    }

    /// Encode a sequence of `u64` values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode_u64(values: &[u64]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "integer encoder (u64)",
            });
        }

        // Treat u64 as i64 for delta encoding (wrapping arithmetic works)
        let i64_values: Vec<i64> = values.iter().map(|&v| v as i64).collect();
        Self::encode_i64(&i64_values)
    }
}

impl IntegerDecoder {
    /// Decode an integer-encoded byte buffer back to `i64` values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is malformed.
    pub fn decode_i64(data: &[u8]) -> Result<Vec<i64>> {
        if data.len() < 13 {
            return Err(EncodingError::CorruptData {
                detail: "integer data too short for header".to_string(),
            });
        }

        let count = checked_decode_count(
            u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize,
            "integer",
        )?;
        let first_value = i64::from_le_bytes([
            data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
        ]);
        let bit_width = data[12];

        if bit_width > 64 {
            return Err(EncodingError::CorruptData {
                detail: format!("invalid bit_width {bit_width}, max is 64"),
            });
        }

        if count == 0 {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(count);
        result.push(first_value);

        if count == 1 || bit_width == 0 {
            // Constant sequence
            for _ in 1..count {
                result.push(first_value);
            }
            return Ok(result);
        }

        // Unpack deltas
        let packed_data = &data[13..];
        let deltas = unpack_bits(packed_data, count - 1, bit_width)?;

        // SIMD-friendly batch zigzag decode + prefix sum
        let mut decoded_deltas = Vec::new();
        crate::simd::batch_zigzag_decode(&deltas, &mut decoded_deltas);
        crate::simd::batch_prefix_sum_i64(first_value, &decoded_deltas, &mut result);

        Ok(result)
    }

    /// Decode an integer-encoded byte buffer back to `u64` values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is malformed.
    pub fn decode_u64(data: &[u8]) -> Result<Vec<u64>> {
        let i64_values = Self::decode_i64(data)?;
        Ok(i64_values.into_iter().map(|v| v as u64).collect())
    }
}

// ── Varint integer encoding ────────────────────────────────────────────

/// Integer encoder using Delta + ZigZag + LEB128 varint per delta.
///
/// Unlike [`IntegerEncoder`] which uses a uniform bit width for all deltas
/// (determined by the largest delta), this encoder uses LEB128 varint
/// encoding so each delta occupies only the bytes it needs.
///
/// This is more efficient for **sparse or highly variable** data where
/// most deltas are small but occasional large outliers would inflate
/// the fixed bit width.
///
/// ## Wire format
///
/// ```text
/// [count: u32 LE][first_value: i64 LE][zigzag(delta₀) as varint]…
/// ```
#[derive(Debug, Clone, Copy)]
pub struct VarintEncoder;

/// Decoder for [`VarintEncoder`]-encoded data.
#[derive(Debug, Clone, Copy)]
pub struct VarintDecoder;

impl VarintEncoder {
    /// Encode a sequence of `i64` values using delta + ZigZag + LEB128 varint.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode_i64(values: &[i64]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "varint encoder (i64)",
            });
        }

        let count = checked_count(values.len())?;

        // Header: [count: u32][first_value: i64]
        let mut buf = Vec::with_capacity(12 + values.len() * 2);
        buf.extend_from_slice(&count.to_le_bytes());
        buf.extend_from_slice(&values[0].to_le_bytes());

        for i in 1..values.len() {
            let delta = values[i].wrapping_sub(values[i - 1]);
            let zz = zigzag_encode(delta);
            varint_encode(zz, &mut buf);
        }

        Ok(buf)
    }

    /// Encode a sequence of `u64` values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode_u64(values: &[u64]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "varint encoder (u64)",
            });
        }
        let i64_values: Vec<i64> = values.iter().map(|&v| v as i64).collect();
        Self::encode_i64(&i64_values)
    }
}

impl VarintDecoder {
    /// Decode a varint-encoded byte buffer back to `i64` values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is malformed.
    pub fn decode_i64(data: &[u8]) -> Result<Vec<i64>> {
        if data.len() < 12 {
            return Err(EncodingError::CorruptData {
                detail: "varint data too short for header".to_string(),
            });
        }

        let count = checked_decode_count(
            u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize,
            "integer",
        )?;
        let first_value = i64::from_le_bytes([
            data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
        ]);

        if count == 0 {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(count);
        result.push(first_value);

        let mut pos = 12;
        for _ in 1..count {
            if pos >= data.len() {
                return Err(EncodingError::CorruptData {
                    detail: "unexpected end of varint-encoded data".to_string(),
                });
            }
            let (zz, consumed) = varint_decode(&data[pos..])?;
            pos += consumed;
            let delta = zigzag_decode(zz);
            let prev = result[result.len() - 1];
            result.push(prev.wrapping_add(delta));
        }

        Ok(result)
    }

    /// Decode a varint-encoded byte buffer back to `u64` values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is malformed.
    pub fn decode_u64(data: &[u8]) -> Result<Vec<u64>> {
        let i64_values = Self::decode_i64(data)?;
        Ok(i64_values.into_iter().map(|v| v as u64).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_monotonic_i64() {
        let values: Vec<i64> = (0..1000).collect();
        let encoded = IntegerEncoder::encode_i64(&values).unwrap();
        let decoded = IntegerDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn roundtrip_constant_i64() {
        let values = vec![42_i64; 100];
        let encoded = IntegerEncoder::encode_i64(&values).unwrap();
        let decoded = IntegerDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
        // Constant → bit_width=0 → header only
        assert_eq!(encoded.len(), 13);
    }

    #[test]
    fn roundtrip_negative_i64() {
        let values = vec![-100, -50, -10, 0, 10, 50, 100];
        let encoded = IntegerEncoder::encode_i64(&values).unwrap();
        let decoded = IntegerDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn roundtrip_edge_i64() {
        let values = vec![i64::MIN, 0, i64::MAX];
        let encoded = IntegerEncoder::encode_i64(&values).unwrap();
        let decoded = IntegerDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn roundtrip_single_i64() {
        let values = vec![42_i64];
        let encoded = IntegerEncoder::encode_i64(&values).unwrap();
        let decoded = IntegerDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn roundtrip_u64() {
        let values: Vec<u64> = vec![0, 1, 100, u64::MAX / 2, u64::MAX];
        let encoded = IntegerEncoder::encode_u64(&values).unwrap();
        let decoded = IntegerDecoder::decode_u64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn empty_error() {
        assert!(IntegerEncoder::encode_i64(&[]).is_err());
        assert!(IntegerEncoder::encode_u64(&[]).is_err());
    }

    #[test]
    fn compression_ratio_monotonic() {
        let values: Vec<i64> = (0..10_000).collect();
        let raw_size = values.len() * 8;
        let encoded = IntegerEncoder::encode_i64(&values).unwrap();
        #[allow(clippy::cast_precision_loss)]
        let ratio = raw_size as f64 / encoded.len() as f64;
        assert!(
            ratio > 5.0,
            "Expected ratio > 5x on monotonic data, got {ratio:.1}x"
        );
    }

    #[test]
    fn corrupt_bit_width() {
        // Build a valid header with invalid bit_width = 255
        let mut data = vec![0u8; 13];
        data[0..4].copy_from_slice(&2u32.to_le_bytes()); // count=2
        data[4..12].copy_from_slice(&42i64.to_le_bytes()); // first_value=42
        data[12] = 255; // invalid bit_width
        assert!(IntegerDecoder::decode_i64(&data).is_err());
    }

    // ── VarintEncoder / VarintDecoder tests ────────────────────────────

    #[test]
    fn varint_roundtrip_monotonic_i64() {
        let values: Vec<i64> = (0..1000).collect();
        let encoded = VarintEncoder::encode_i64(&values).unwrap();
        let decoded = VarintDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn varint_roundtrip_constant_i64() {
        let values = vec![42_i64; 100];
        let encoded = VarintEncoder::encode_i64(&values).unwrap();
        let decoded = VarintDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
        // Constant → all deltas are 0 → 1 byte (varint 0) each
        // Header (12) + 99 * 1 byte = 111
        assert_eq!(encoded.len(), 12 + 99);
    }

    #[test]
    fn varint_roundtrip_negative_i64() {
        let values = vec![-100, -50, -10, 0, 10, 50, 100];
        let encoded = VarintEncoder::encode_i64(&values).unwrap();
        let decoded = VarintDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn varint_roundtrip_edge_i64() {
        let values = vec![i64::MIN, 0, i64::MAX];
        let encoded = VarintEncoder::encode_i64(&values).unwrap();
        let decoded = VarintDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn varint_roundtrip_single_i64() {
        let values = vec![42_i64];
        let encoded = VarintEncoder::encode_i64(&values).unwrap();
        let decoded = VarintDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn varint_roundtrip_u64() {
        let values: Vec<u64> = vec![0, 1, 100, u64::MAX / 2, u64::MAX];
        let encoded = VarintEncoder::encode_u64(&values).unwrap();
        let decoded = VarintDecoder::decode_u64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn varint_empty_error() {
        assert!(VarintEncoder::encode_i64(&[]).is_err());
        assert!(VarintEncoder::encode_u64(&[]).is_err());
    }

    #[test]
    fn varint_better_for_sparse_data() {
        // Sparse data: mostly small deltas, some large outliers.
        // Varint should beat fixed-bit-width here because the fixed width
        // is driven by the max delta, wasting space on the small ones.
        let mut values = Vec::new();
        for i in 0..1000 {
            if i % 100 == 0 {
                values.push(i * 1_000_000); // large jump
            } else {
                values.push(values.last().copied().unwrap_or(0) + 1); // tiny delta
            }
        }
        let fixed = IntegerEncoder::encode_i64(&values).unwrap();
        let varint = VarintEncoder::encode_i64(&values).unwrap();
        assert!(
            varint.len() <= fixed.len(),
            "varint ({}) should be <= fixed-width ({}) for sparse data",
            varint.len(),
            fixed.len(),
        );
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip_arbitrary_i64(values in proptest::collection::vec(any::<i64>(), 1..500)) {
            let encoded = IntegerEncoder::encode_i64(&values).unwrap();
            let decoded = IntegerDecoder::decode_i64(&encoded).unwrap();
            prop_assert_eq!(&values, &decoded);
        }

        #[test]
        fn roundtrip_arbitrary_u64(values in proptest::collection::vec(any::<u64>(), 1..500)) {
            let encoded = IntegerEncoder::encode_u64(&values).unwrap();
            let decoded = IntegerDecoder::decode_u64(&encoded).unwrap();
            prop_assert_eq!(&values, &decoded);
        }

        #[test]
        fn varint_roundtrip_arbitrary_i64(values in proptest::collection::vec(any::<i64>(), 1..500)) {
            let encoded = VarintEncoder::encode_i64(&values).unwrap();
            let decoded = VarintDecoder::decode_i64(&encoded).unwrap();
            prop_assert_eq!(&values, &decoded);
        }

        #[test]
        fn varint_roundtrip_arbitrary_u64(values in proptest::collection::vec(any::<u64>(), 1..500)) {
            let encoded = VarintEncoder::encode_u64(&values).unwrap();
            let decoded = VarintDecoder::decode_u64(&encoded).unwrap();
            prop_assert_eq!(&values, &decoded);
        }
    }
}
