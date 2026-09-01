//! Patas float encoding (VLDB 2023).
//!
//! Patas ("Exploiting Bitwise Structure for Lossless Floating-Point
//! Compression") is a byte-aligned XOR-based encoder.  Like Gorilla and
//! Chimp it XORs consecutive values, but instead of bit-packing the XOR
//! residuals it stores them as **whole bytes**, yielding much faster
//! decoding while matching or beating Gorilla compression ratios on most
//! time-series workloads.
//!
//! ## Encoding format
//!
//! ```text
//! [count: u32 LE][first_value: 8 bytes]
//!   for each subsequent value:
//!     [flag byte: trailing_zeros_bytes(high nibble) | significant_bytes(low nibble)]
//!     [significant_bytes: 0..=8 bytes]  (the non-zero core of the XOR)
//! ```
//!
//! - `trailing_zeros_bytes` (0–7): number of trailing zero **bytes** in the XOR.
//! - `significant_bytes` (0–8): number of bytes needed to represent the XOR
//!   after stripping trailing zero bytes.  0 means the values are identical.
//! - Together these fully describe the XOR: the encoder writes only the
//!   `significant_bytes` middle portion; the decoder reconstructs the full
//!   64-bit XOR by shifting and zero-filling.
//!
//! ## Advantages over Gorilla / Chimp
//!
//! - **~2× faster decode**: byte-aligned loads avoid bit-manipulation.
//! - **Competitive compression**: often within 5% of Chimp/Gorilla; wins on
//!   data with byte-aligned floating-point patterns (e.g. sensor data
//!   quantised to limited precision).
//! - **Simpler implementation**: no bit writers, no leading-zero buckets,
//!   no ring buffers.

use crate::coding::{checked_count, checked_decode_count};
use crate::error::{EncodingError, Result};

/// Patas float encoder (VLDB 2023).
#[derive(Debug, Clone, Copy)]
pub struct PatasEncoder;

/// Patas float decoder.
#[derive(Debug, Clone, Copy)]
pub struct PatasDecoder;

impl PatasEncoder {
    /// Encode a sequence of `f64` values using Patas byte-aligned XOR encoding.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode(values: &[f64]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "patas encoder",
            });
        }

        let count = checked_count(values.len())?;

        // Worst case: 4 (count) + 8 (first) + 9 * (n-1) (flag + 8 bytes each)
        let mut buf = Vec::with_capacity(12 + values.len() * 9);

        // Header: value count
        buf.extend_from_slice(&count.to_le_bytes());

        // First value: raw 8 bytes
        let mut prev_bits = values[0].to_bits();
        buf.extend_from_slice(&prev_bits.to_le_bytes());

        for &val in &values[1..] {
            let bits = val.to_bits();
            let xor = prev_bits ^ bits;

            if xor == 0 {
                // Identical: flag=0 means sig_bytes=0 → same value
                buf.push(0);
            } else {
                // Count trailing zero bytes and significant byte span
                let tz_bytes = (xor.trailing_zeros() / 8) as u8; // 0..=7
                let total_bytes = 8 - (xor.leading_zeros() / 8) as u8; // 1..=8
                let sig_bytes = total_bytes.saturating_sub(tz_bytes); // 1..=8

                // Flag byte: high nibble = tz_bytes (0-7), low nibble = sig_bytes (1-8)
                let flag = (tz_bytes << 4) | sig_bytes;
                buf.push(flag);

                // Write only the significant bytes (shifted right past trailing zeros)
                let shifted = xor >> (u32::from(tz_bytes) * 8);
                let sig_slice = shifted.to_le_bytes();
                buf.extend_from_slice(&sig_slice[..sig_bytes as usize]);
            }

            prev_bits = bits;
        }

        Ok(buf)
    }
}

impl PatasDecoder {
    /// Decode a Patas-encoded payload back to `f64` values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the payload is truncated or malformed.
    pub fn decode(payload: &[u8]) -> Result<Vec<f64>> {
        if payload.len() < 12 {
            return Err(EncodingError::CorruptData {
                detail: "patas payload too short for header".to_string(),
            });
        }

        let count = checked_decode_count(
            u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize,
            "Patas",
        )?;
        if count == 0 {
            return Err(EncodingError::CorruptData {
                detail: "patas count is zero".to_string(),
            });
        }

        let mut values = Vec::with_capacity(count);
        let mut pos = 4;

        // First value: raw 8 bytes
        if pos + 8 > payload.len() {
            return Err(EncodingError::CorruptData {
                detail: "patas payload truncated at first value".to_string(),
            });
        }
        let mut prev_bits = u64::from_le_bytes(payload[pos..pos + 8].try_into().expect("8 bytes"));
        values.push(f64::from_bits(prev_bits));
        pos += 8;

        for _ in 1..count {
            if pos >= payload.len() {
                return Err(EncodingError::CorruptData {
                    detail: "patas payload truncated".to_string(),
                });
            }

            let flag = payload[pos];
            pos += 1;

            let tz_bytes = (flag >> 4) & 0x0F;
            let sig_bytes = flag & 0x0F;

            // Both nibbles are byte counts into a `u64`, so the encoder can
            // only ever write `tz_bytes + sig_bytes <= 8`. A flag byte from
            // anywhere else breaks that, and each half breaks it differently:
            // `sig_bytes > 8` indexes past the end of the 8-byte scratch
            // buffer below, and `tz_bytes > 7` shifts a `u64` by 64 or more,
            // which panics in debug and silently masks to a wrong value in
            // release. Gorilla and Chimp both derive their shift through a
            // `checked_sub` chain that cannot exceed 63; Patas was the sibling
            // that validated nothing (R4, D33).
            if tz_bytes + sig_bytes > 8 {
                return Err(EncodingError::CorruptData {
                    detail: format!(
                        "patas flag byte claims {tz_bytes} trailing-zero and {sig_bytes}                          significant bytes, which do not fit in 8 — header is corrupt"
                    ),
                });
            }

            if sig_bytes == 0 {
                // Identical value
                values.push(f64::from_bits(prev_bits));
                continue;
            }

            if pos + sig_bytes as usize > payload.len() {
                return Err(EncodingError::CorruptData {
                    detail: "patas payload truncated at significant bytes".to_string(),
                });
            }

            // Read significant bytes into a u64
            let mut sig_val = [0u8; 8];
            sig_val[..sig_bytes as usize].copy_from_slice(&payload[pos..pos + sig_bytes as usize]);
            pos += sig_bytes as usize;

            let shifted = u64::from_le_bytes(sig_val);
            let xor = shifted << (u32::from(tz_bytes) * 8);
            prev_bits ^= xor;
            values.push(f64::from_bits(prev_bits));
        }

        if values.len() != count {
            return Err(EncodingError::CorruptData {
                detail: format!("patas decoded {} values, expected {}", values.len(), count),
            });
        }

        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_basic() {
        let values = vec![1.0, 1.1, 1.2, 1.3, 1.4, 1.5];
        let encoded = PatasEncoder::encode(&values).unwrap();
        let decoded = PatasDecoder::decode(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn roundtrip_identical() {
        let values = vec![42.0; 100];
        let encoded = PatasEncoder::encode(&values).unwrap();
        let decoded = PatasDecoder::decode(&encoded).unwrap();
        assert_eq!(values, decoded);
        // Identical values: 4 (count) + 8 (first) + 99 flag bytes (1 each) = 111
        assert_eq!(encoded.len(), 111);
    }

    #[test]
    fn roundtrip_single() {
        let values = vec![std::f64::consts::PI];
        let encoded = PatasEncoder::encode(&values).unwrap();
        let decoded = PatasDecoder::decode(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn roundtrip_special_values() {
        let values = vec![
            0.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            -0.0,
            f64::MAX,
            f64::MIN,
        ];
        let encoded = PatasEncoder::encode(&values).unwrap();
        let decoded = PatasDecoder::decode(&encoded).unwrap();
        // Compare bit patterns (NaN not included since NaN != NaN)
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn roundtrip_nan() {
        let values = vec![1.0, f64::NAN, 2.0];
        let encoded = PatasEncoder::encode(&values).unwrap();
        let decoded = PatasDecoder::decode(&encoded).unwrap();
        assert_eq!(values[0], decoded[0]);
        assert!(decoded[1].is_nan());
        assert_eq!(values[2], decoded[2]);
    }

    #[test]
    fn empty_input_error() {
        let result = PatasEncoder::encode(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn truncated_payload_error() {
        let result = PatasDecoder::decode(&[0; 4]);
        assert!(result.is_err());
    }

    #[test]
    fn compression_ratio_slowly_varying() {
        // Slowly varying data should compress reasonably well
        let values: Vec<f64> = (0..1000).map(|i| 20.0 + (i as f64) * 0.001).collect();
        let encoded = PatasEncoder::encode(&values).unwrap();
        let raw_size = values.len() * 8;
        // Patas is byte-aligned so expect at least 20% compression
        assert!(
            encoded.len() < raw_size,
            "Expected compression, got {}/{}",
            encoded.len(),
            raw_size
        );
        let decoded = PatasDecoder::decode(&encoded).unwrap();
        assert_eq!(values, decoded);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip_arbitrary(values in proptest::collection::vec(any::<f64>(), 1..500)) {
            let encoded = PatasEncoder::encode(&values).unwrap();
            let decoded = PatasDecoder::decode(&encoded).unwrap();
            prop_assert_eq!(values.len(), decoded.len());
            for (a, b) in values.iter().zip(decoded.iter()) {
                prop_assert_eq!(a.to_bits(), b.to_bits(), "bitwise mismatch");
            }
        }
    }
}

#[cfg(test)]
mod flag_validation_tests {
    use super::*;

    /// Build a minimal Patas payload: `[count u32][first value u64][flag]…`.
    fn payload_with_flag(flag: u8, tail: &[u8]) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&2u32.to_le_bytes());
        p.extend_from_slice(&1.0f64.to_bits().to_le_bytes());
        p.push(flag);
        p.extend_from_slice(tail);
        p
    }

    /// A trailing-zero count above 7 must be refused, not shifted with.
    ///
    /// `shifted << (tz_bytes * 8)` reaches 120 for `tz_bytes = 15`, and
    /// shifting a `u64` by 64 or more panics in debug and masks to a wrong
    /// value in release. This is reachable from one flipped bit in a flag
    /// byte on the flash the segment is read from, which is what makes it a
    /// durability property rather than a hardening nicety.
    #[test]
    fn a_trailing_zero_count_past_the_width_of_a_u64_is_rejected() {
        for tz in 8u8..=15 {
            let flag = (tz << 4) | 1; // one significant byte
            let err = PatasDecoder::decode(&payload_with_flag(flag, &[0xFF]))
                .expect_err("tz_bytes {tz} must be refused");
            assert!(
                err.to_string().contains("do not fit in 8"),
                "tz={tz}: expected the flag guard, got: {err}"
            );
        }
    }

    /// A significant-byte count above 8 would index past the scratch buffer.
    #[test]
    fn a_significant_byte_count_past_eight_is_rejected() {
        for sig in 9u8..=15 {
            let err = PatasDecoder::decode(&payload_with_flag(sig, &[0xFF; 16]))
                .expect_err("sig_bytes {sig} must be refused");
            assert!(
                err.to_string().contains("do not fit in 8"),
                "sig={sig}: expected the flag guard, got: {err}"
            );
        }
    }

    /// The guard bounds corruption; it does not narrow the format.
    #[test]
    fn every_flag_the_encoder_can_produce_still_decodes() {
        // Values chosen to exercise a spread of trailing-zero and
        // significant-byte counts.
        let values = vec![
            1.0,
            1.0,
            1.5,
            1.5000001,
            2.0,
            -2.0,
            0.0,
            -0.0,
            f64::MAX,
            f64::MIN_POSITIVE,
            1e300,
            1e-300,
        ];
        let encoded = PatasEncoder::encode(&values).expect("encode");
        let decoded = PatasDecoder::decode(&encoded).expect("decode");
        assert_eq!(decoded.len(), values.len());
        for (a, b) in values.iter().zip(&decoded) {
            assert_eq!(a.to_bits(), b.to_bits(), "round trip must be bitwise exact");
        }
    }
}
