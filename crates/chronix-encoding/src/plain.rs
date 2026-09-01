//! Plain (uncompressed) encoding for fallback use.
//!
//! Stores values directly in their natural binary representations when
//! specialised encoders cannot be applied or when the data is incompressible.
//!
//! ## Format
//!
//! ```text
//! [count: u32]
//! [raw values…]
//! ```
//!
//! - `f64`/`i64`/`u64`: 8 bytes little-endian each.
//! - `bool`: 1 byte each (`0x01` = true, `0x00` = false).
//! - `String`: `[len: u32][UTF-8 bytes…]` per entry.

use crate::coding::{checked_count, checked_decode_count};
use crate::error::{EncodingError, Result};

/// Plain (uncompressed) encoder.
#[derive(Debug, Clone, Copy)]
pub struct PlainEncoder;

/// Plain (uncompressed) decoder.
#[derive(Debug, Clone, Copy)]
pub struct PlainDecoder;

impl PlainEncoder {
    /// Encode `f64` values uncompressed.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode_f64(values: &[f64]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "plain f64",
            });
        }
        let count = checked_count(values.len())?;
        let mut buf = Vec::with_capacity(4 + values.len() * 8);
        buf.extend_from_slice(&count.to_le_bytes());
        for &v in values {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Ok(buf)
    }

    /// Encode `i64` values uncompressed.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode_i64(values: &[i64]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "plain i64",
            });
        }
        let count = checked_count(values.len())?;
        let mut buf = Vec::with_capacity(4 + values.len() * 8);
        buf.extend_from_slice(&count.to_le_bytes());
        for &v in values {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Ok(buf)
    }

    /// Encode `u64` values uncompressed.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode_u64(values: &[u64]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "plain u64",
            });
        }
        let count = checked_count(values.len())?;
        let mut buf = Vec::with_capacity(4 + values.len() * 8);
        buf.extend_from_slice(&count.to_le_bytes());
        for &v in values {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Ok(buf)
    }

    /// Encode boolean values uncompressed (1 byte per bool).
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode_bool(values: &[bool]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "plain bool",
            });
        }
        let count = checked_count(values.len())?;
        let mut buf = Vec::with_capacity(4 + values.len());
        buf.extend_from_slice(&count.to_le_bytes());
        for &v in values {
            buf.push(u8::from(v));
        }
        Ok(buf)
    }

    /// Encode string values uncompressed (length-prefixed).
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode_string(values: &[&str]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "plain string",
            });
        }
        let count = checked_count(values.len())?;
        let total_bytes: usize = values.iter().map(|s| 4 + s.len()).sum();
        let mut buf = Vec::with_capacity(4 + total_bytes);
        buf.extend_from_slice(&count.to_le_bytes());
        for &s in values {
            let len = u32::try_from(s.len()).map_err(|_| EncodingError::CorruptData {
                detail: format!("string length {} exceeds u32::MAX", s.len()),
            })?;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        Ok(buf)
    }
}

impl PlainDecoder {
    /// Decode `f64` values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is malformed.
    pub fn decode_f64(data: &[u8]) -> Result<Vec<f64>> {
        let count = Self::read_count(data, "f64")?;
        let expected = 4 + count * 8;
        Self::check_len(data, expected, "f64")?;

        let mut values = Vec::with_capacity(count);
        for i in 0..count {
            let offset = 4 + i * 8;
            let bytes: [u8; 8] =
                data[offset..offset + 8]
                    .try_into()
                    .map_err(|_| EncodingError::CorruptData {
                        detail: "plain f64 slice conversion failed".to_string(),
                    })?;
            values.push(f64::from_le_bytes(bytes));
        }
        Ok(values)
    }

    /// Decode `i64` values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is malformed.
    pub fn decode_i64(data: &[u8]) -> Result<Vec<i64>> {
        let count = Self::read_count(data, "i64")?;
        let expected = 4 + count * 8;
        Self::check_len(data, expected, "i64")?;

        let mut values = Vec::with_capacity(count);
        for i in 0..count {
            let offset = 4 + i * 8;
            let bytes: [u8; 8] =
                data[offset..offset + 8]
                    .try_into()
                    .map_err(|_| EncodingError::CorruptData {
                        detail: "plain i64 slice conversion failed".to_string(),
                    })?;
            values.push(i64::from_le_bytes(bytes));
        }
        Ok(values)
    }

    /// Decode `u64` values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is malformed.
    pub fn decode_u64(data: &[u8]) -> Result<Vec<u64>> {
        let count = Self::read_count(data, "u64")?;
        let expected = 4 + count * 8;
        Self::check_len(data, expected, "u64")?;

        let mut values = Vec::with_capacity(count);
        for i in 0..count {
            let offset = 4 + i * 8;
            let bytes: [u8; 8] =
                data[offset..offset + 8]
                    .try_into()
                    .map_err(|_| EncodingError::CorruptData {
                        detail: "plain u64 slice conversion failed".to_string(),
                    })?;
            values.push(u64::from_le_bytes(bytes));
        }
        Ok(values)
    }

    /// Decode boolean values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is malformed.
    pub fn decode_bool(data: &[u8]) -> Result<Vec<bool>> {
        let count = Self::read_count(data, "bool")?;
        let expected = 4 + count;
        Self::check_len(data, expected, "bool")?;

        let mut values = Vec::with_capacity(count);
        for i in 0..count {
            values.push(data[4 + i] != 0);
        }
        Ok(values)
    }

    /// Decode string values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is malformed.
    pub fn decode_string(data: &[u8]) -> Result<Vec<String>> {
        let count = Self::read_count(data, "string")?;
        let mut offset = 4;
        let mut values = Vec::with_capacity(count);

        for _ in 0..count {
            if offset + 4 > data.len() {
                return Err(EncodingError::CorruptData {
                    detail: "plain string data truncated at length prefix".to_string(),
                });
            }
            let len = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + len > data.len() {
                return Err(EncodingError::CorruptData {
                    detail: "plain string data truncated".to_string(),
                });
            }
            let s = std::str::from_utf8(&data[offset..offset + len]).map_err(|e| {
                EncodingError::CorruptData {
                    detail: format!("invalid UTF-8 in plain string: {e}"),
                }
            })?;
            values.push(s.to_owned());
            offset += len;
        }

        Ok(values)
    }

    fn read_count(data: &[u8], label: &str) -> Result<usize> {
        if data.len() < 4 {
            return Err(EncodingError::CorruptData {
                detail: format!("plain {label} data too short for header"),
            });
        }
        checked_decode_count(
            u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize,
            "plain",
        )
    }

    fn check_len(data: &[u8], expected: usize, label: &str) -> Result<()> {
        if data.len() < expected {
            return Err(EncodingError::CorruptData {
                detail: format!(
                    "plain {label} data truncated: expected {expected}, got {}",
                    data.len()
                ),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn f64_roundtrip() {
        let values = vec![1.0, -2.5, 0.0, f64::INFINITY, f64::NEG_INFINITY];
        let encoded = PlainEncoder::encode_f64(&values).unwrap();
        let decoded = PlainDecoder::decode_f64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn f64_nan_roundtrip() {
        let values = vec![f64::NAN];
        let encoded = PlainEncoder::encode_f64(&values).unwrap();
        let decoded = PlainDecoder::decode_f64(&encoded).unwrap();
        assert!(decoded[0].is_nan());
    }

    #[test]
    fn i64_roundtrip() {
        let values = vec![i64::MIN, -1, 0, 1, i64::MAX];
        let encoded = PlainEncoder::encode_i64(&values).unwrap();
        let decoded = PlainDecoder::decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn u64_roundtrip() {
        let values = vec![0, 1, u64::MAX, 42, 1_000_000];
        let encoded = PlainEncoder::encode_u64(&values).unwrap();
        let decoded = PlainDecoder::decode_u64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn bool_roundtrip() {
        let values = vec![true, false, true, true, false];
        let encoded = PlainEncoder::encode_bool(&values).unwrap();
        let decoded = PlainDecoder::decode_bool(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn string_roundtrip() {
        let values = vec!["hello", "world", "", "café", "日本語"];
        let encoded = PlainEncoder::encode_string(&values).unwrap();
        let decoded = PlainDecoder::decode_string(&encoded).unwrap();
        let expected: Vec<String> = values.iter().map(|s| (*s).to_owned()).collect();
        assert_eq!(expected, decoded);
    }

    #[test]
    fn empty_errors() {
        assert!(PlainEncoder::encode_f64(&[]).is_err());
        assert!(PlainEncoder::encode_i64(&[]).is_err());
        assert!(PlainEncoder::encode_u64(&[]).is_err());
        assert!(PlainEncoder::encode_bool(&[]).is_err());
        let empty: &[&str] = &[];
        assert!(PlainEncoder::encode_string(empty).is_err());
    }

    #[test]
    fn single_values() {
        let e = PlainEncoder::encode_f64(&[std::f64::consts::PI]).unwrap();
        assert_eq!(
            vec![std::f64::consts::PI],
            PlainDecoder::decode_f64(&e).unwrap()
        );

        let e = PlainEncoder::encode_i64(&[42]).unwrap();
        assert_eq!(vec![42i64], PlainDecoder::decode_i64(&e).unwrap());

        let e = PlainEncoder::encode_u64(&[99]).unwrap();
        assert_eq!(vec![99u64], PlainDecoder::decode_u64(&e).unwrap());

        let e = PlainEncoder::encode_bool(&[true]).unwrap();
        assert_eq!(vec![true], PlainDecoder::decode_bool(&e).unwrap());

        let e = PlainEncoder::encode_string(&["x"]).unwrap();
        assert_eq!(
            vec!["x".to_owned()],
            PlainDecoder::decode_string(&e).unwrap()
        );
    }

    #[test]
    fn corrupt_data_detected() {
        // Too short
        assert!(PlainDecoder::decode_f64(&[0, 0]).is_err());
        // Count says 2 but only 1 value present
        let mut bad = PlainEncoder::encode_f64(&[1.0, 2.0]).unwrap();
        bad.truncate(4 + 8); // Remove second value
        assert!(PlainDecoder::decode_f64(&bad).is_err());
    }

    #[test]
    fn no_compression_overhead() {
        // Plain encoding should have exactly 4-byte header + raw data
        let values: Vec<f64> = (0..1000).map(|i| i as f64).collect();
        let encoded = PlainEncoder::encode_f64(&values).unwrap();
        assert_eq!(encoded.len(), 4 + 1000 * 8);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip_f64(values in proptest::collection::vec(any::<f64>(), 1..500)) {
            let encoded = PlainEncoder::encode_f64(&values).unwrap();
            let decoded = PlainDecoder::decode_f64(&encoded).unwrap();
            prop_assert_eq!(values.len(), decoded.len());
            for (a, b) in values.iter().zip(decoded.iter()) {
                prop_assert_eq!(a.to_bits(), b.to_bits());
            }
        }

        #[test]
        fn roundtrip_i64(values in proptest::collection::vec(any::<i64>(), 1..500)) {
            let encoded = PlainEncoder::encode_i64(&values).unwrap();
            let decoded = PlainDecoder::decode_i64(&encoded).unwrap();
            prop_assert_eq!(&values, &decoded);
        }

        #[test]
        fn roundtrip_u64(values in proptest::collection::vec(any::<u64>(), 1..500)) {
            let encoded = PlainEncoder::encode_u64(&values).unwrap();
            let decoded = PlainDecoder::decode_u64(&encoded).unwrap();
            prop_assert_eq!(&values, &decoded);
        }

        #[test]
        fn roundtrip_bool(values in proptest::collection::vec(any::<bool>(), 1..500)) {
            let encoded = PlainEncoder::encode_bool(&values).unwrap();
            let decoded = PlainDecoder::decode_bool(&encoded).unwrap();
            prop_assert_eq!(&values, &decoded);
        }

        #[test]
        fn roundtrip_string(values in proptest::collection::vec("[a-z]{0,8}", 1..200)) {
            let refs: Vec<&str> = values.iter().map(String::as_str).collect();
            let encoded = PlainEncoder::encode_string(&refs).unwrap();
            let decoded = PlainDecoder::decode_string(&encoded).unwrap();
            prop_assert_eq!(&values, &decoded);
        }
    }
}
