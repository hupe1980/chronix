//! Gorilla XOR float encoding (Facebook, 2015).
//!
//! Classic XOR-based compression where each value is `XORed` with its predecessor
//! and the meaningful bits of the XOR result are stored with a compact
//! leading/trailing zero prefix scheme. Used as fallback when Chimp's
//! bucket-based approach doesn't achieve sufficient compression.

use crate::coding::{checked_count, checked_decode_count};
use crate::delta::{BitReader, BitWriter};
use crate::error::{EncodingError, Result};
use crate::simd::{
    batch_f64_to_bits, batch_leading_zeros, batch_trailing_zeros, batch_xor_adjacent,
};

/// Gorilla float encoder.
#[derive(Debug, Clone, Copy)]
pub struct GorillaEncoder;

/// Reusable scratch buffers for Gorilla encoding.
///
/// Avoids allocating four intermediate `Vec`s per `encode()` call.
/// Create once and reuse across multiple encode calls (e.g., during
/// flush or compaction of many columns).
#[derive(Debug, Default)]
pub struct GorillaEncodeScratch {
    bits: Vec<u64>,
    xors: Vec<u64>,
    leading: Vec<u32>,
    trailing: Vec<u32>,
}

impl GorillaEncodeScratch {
    /// Create a new empty scratch buffer.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// Gorilla float decoder.
#[derive(Debug, Clone, Copy)]
pub struct GorillaDecoder;

impl GorillaEncoder {
    /// Encode a sequence of `f64` values using Gorilla XOR encoding.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode(values: &[f64]) -> Result<Vec<u8>> {
        let mut scratch = GorillaEncodeScratch::new();
        Self::encode_with_scratch(values, &mut scratch)
    }

    /// Encode using caller-provided scratch buffers.
    ///
    /// Reuses buffers across multiple calls to avoid
    /// repeated allocation during flush/compaction of many columns.
    pub fn encode_with_scratch(
        values: &[f64],
        scratch: &mut GorillaEncodeScratch,
    ) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "gorilla encoder",
            });
        }

        let mut writer = BitWriter::with_capacity(4 + values.len() * 2);

        // Value count (validated to fit in u32)
        let count = checked_count(values.len())?;
        writer.write_bits(u64::from(count), 32);

        // ---- SIMD-friendly batch pre-computation ----
        // Reuse scratch buffers — clear + refill avoids realloc
        // when capacity is already sufficient from a prior call.
        scratch.bits.clear();
        batch_f64_to_bits(values, &mut scratch.bits);

        scratch.xors.clear();
        batch_xor_adjacent(&scratch.bits, &mut scratch.xors);

        scratch.leading.clear();
        batch_leading_zeros(&scratch.xors, &mut scratch.leading);

        scratch.trailing.clear();
        batch_trailing_zeros(&scratch.xors, &mut scratch.trailing);

        // ---- Serial bit-packing (inherently sequential) ----
        // First value: raw 64-bit (safe access avoids potential index panic)
        let first_bits = *scratch.bits.first().ok_or(EncodingError::CorruptData {
            detail: "gorilla: empty bits buffer after non-empty check".to_string(),
        })?;
        writer.write_bits(first_bits, 64);

        let mut prev_leading: u32 = u32::MAX;
        let mut prev_trailing: u32 = 0;

        for i in 0..scratch.xors.len() {
            let xor = scratch.xors[i];

            if xor == 0 {
                // Case 0: identical → single 0 bit
                writer.write_bit(false);
            } else {
                let leading = scratch.leading[i];
                let trailing = scratch.trailing[i];

                // Check if current window fits within previous
                if prev_leading != u32::MAX && leading >= prev_leading && trailing >= prev_trailing
                {
                    // Case 1: reuse window → prefix 10
                    writer.write_bits(0b10, 2);
                    let meaningful = 64u32
                        .checked_sub(prev_leading)
                        .and_then(|v| v.checked_sub(prev_trailing))
                        .ok_or_else(|| EncodingError::CorruptData {
                            detail: "gorilla case-1 encode: meaningful-bits underflow".to_string(),
                        })?;
                    let shifted = xor >> prev_trailing;
                    writer.write_bits(shifted, meaningful as u8);
                } else {
                    // Case 2: new window → prefix 11
                    writer.write_bits(0b11, 2);
                    // 6-bit leading zeros (capped to 63)
                    writer.write_bits(u64::from(leading.min(63)), 6);
                    let meaningful = 64u32
                        .checked_sub(leading)
                        .and_then(|v| v.checked_sub(trailing))
                        .ok_or_else(|| EncodingError::CorruptData {
                            detail: format!(
                                "gorilla case-2 encode: meaningful-bits underflow \
                                 (leading={leading}, trailing={trailing})"
                            ),
                        })?;
                    // 6-bit meaningful length, stored as meaningful-1
                    // (so 1..=64 maps to 0..=63, fitting in 6 bits)
                    let meaningful_m1 =
                        meaningful
                            .checked_sub(1)
                            .ok_or_else(|| EncodingError::CorruptData {
                                detail: "gorilla case-2 encode: zero meaningful bits \
                                     for non-zero XOR"
                                    .to_string(),
                            })?;
                    writer.write_bits(u64::from(meaningful_m1), 6);
                    let shifted = xor >> trailing;
                    writer.write_bits(shifted, meaningful as u8);

                    prev_leading = leading;
                    prev_trailing = trailing;
                }
            }
        }

        Ok(writer.finish())
    }
}

impl GorillaDecoder {
    /// Decode a Gorilla-encoded byte buffer back to `f64` values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is malformed.
    pub fn decode(data: &[u8]) -> Result<Vec<f64>> {
        if data.len() < 4 {
            return Err(EncodingError::CorruptData {
                detail: "gorilla data too short for header".to_string(),
            });
        }

        let mut reader = BitReader::new(data);

        let count = checked_decode_count(reader.read_bits(32)? as usize, "Gorilla")?;
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(count);

        let first_bits = reader.read_bits(64)?;
        result.push(f64::from_bits(first_bits));

        let mut prev_bits = first_bits;
        let mut prev_leading: u32 = u32::MAX;
        let mut prev_trailing: u32 = 0;

        for _ in 1..count {
            if !reader.read_bit()? {
                // Case 0: identical
                result.push(f64::from_bits(prev_bits));
                continue;
            }

            if reader.read_bit()? {
                // Case 2: new window (prefix 11)
                let leading = reader.read_bits(6)? as u32;
                let meaningful = reader.read_bits(6)? as u32 + 1; // stored as m-1
                let trailing = 64u32
                    .checked_sub(leading)
                    .and_then(|v| v.checked_sub(meaningful))
                    .ok_or_else(|| EncodingError::CorruptData {
                        detail: format!(
                            "gorilla case-2: leading ({leading}) + meaningful \
                             ({meaningful}) exceeds 64"
                        ),
                    })?;
                let shifted = reader.read_bits(meaningful as u8)?;
                let xor = shifted << trailing;
                let bits = prev_bits ^ xor;
                result.push(f64::from_bits(bits));
                prev_bits = bits;

                prev_leading = leading;
                prev_trailing = trailing;
            } else {
                // Case 1: reuse window (prefix 10)
                if prev_leading == u32::MAX {
                    return Err(EncodingError::CorruptData {
                        detail: "gorilla case-1 encountered before any case-2 window".to_string(),
                    });
                }
                let meaningful = 64u32
                    .checked_sub(prev_leading)
                    .and_then(|v| v.checked_sub(prev_trailing))
                    .ok_or_else(|| EncodingError::CorruptData {
                        detail: format!(
                            "gorilla case-1: invalid window \
                             (leading={prev_leading}, trailing={prev_trailing})"
                        ),
                    })?;
                let shifted = reader.read_bits(meaningful as u8)?;
                let xor = shifted << prev_trailing;
                let bits = prev_bits ^ xor;
                result.push(f64::from_bits(bits));
                prev_bits = bits;
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_constant() {
        let values = vec![42.0_f64; 100];
        let encoded = GorillaEncoder::encode(&values).unwrap();
        let decoded = GorillaDecoder::decode(&encoded).unwrap();
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn roundtrip_special_values() {
        let values = vec![
            0.0,
            -0.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MIN,
            f64::MAX,
            f64::MIN_POSITIVE,
            f64::EPSILON,
        ];
        let encoded = GorillaEncoder::encode(&values).unwrap();
        let decoded = GorillaDecoder::decode(&encoded).unwrap();
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits(), "mismatch for value {a}");
        }
    }

    #[test]
    fn roundtrip_sinwave() {
        let values: Vec<f64> = (0..1000).map(|i| (i as f64 * 0.01).sin() * 100.0).collect();
        let encoded = GorillaEncoder::encode(&values).unwrap();
        let decoded = GorillaDecoder::decode(&encoded).unwrap();
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn empty_error() {
        assert!(GorillaEncoder::encode(&[]).is_err());
    }

    #[test]
    fn single_value() {
        let values = vec![99.99];
        let encoded = GorillaEncoder::encode(&values).unwrap();
        let decoded = GorillaDecoder::decode(&encoded).unwrap();
        assert_eq!(values[0].to_bits(), decoded[0].to_bits());
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip_arbitrary(values in proptest::collection::vec(any::<f64>(), 1..500)) {
            let encoded = GorillaEncoder::encode(&values).unwrap();
            let decoded = GorillaDecoder::decode(&encoded).unwrap();
            prop_assert_eq!(values.len(), decoded.len());
            for (a, b) in values.iter().zip(decoded.iter()) {
                prop_assert_eq!(a.to_bits(), b.to_bits());
            }
        }
    }
}
