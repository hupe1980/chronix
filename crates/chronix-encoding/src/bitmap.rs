//! Bitmap encoding for boolean columns.
//!
//! Packs boolean values at 1 bit each, padded to byte boundaries. A separate
//! null bitmap tracks null positions for nullable boolean columns.
//!
//! ## Format
//!
//! ```text
//! [value_count: u32]
//! [has_nulls: u8]  (0 = no null bitmap, 1 = null bitmap follows values)
//! [value bits: ceil(count/8) bytes]
//! [null bitmap: ceil(count/8) bytes]  (only if has_nulls == 1)
//! ```
//!
//! In the null bitmap, a `1` bit means the value is valid (non-null), and a `0`
//! bit means the value is null.

use crate::coding::{checked_count, checked_decode_count};
use crate::error::{EncodingError, Result};

/// Bitmap boolean encoder.
#[derive(Debug, Clone, Copy)]
pub struct BitmapEncoder;

/// Bitmap boolean decoder.
#[derive(Debug, Clone, Copy)]
pub struct BitmapDecoder;

impl BitmapEncoder {
    /// Encode a sequence of non-nullable boolean values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode(values: &[bool]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "bitmap encoder",
            });
        }

        let count = checked_count(values.len())?;
        let bitmap_bytes = values.len().div_ceil(8);
        let mut buf = Vec::with_capacity(4 + 1 + bitmap_bytes);

        // Count
        buf.extend_from_slice(&count.to_le_bytes());
        // No nulls
        buf.push(0);
        // Value bitmap
        buf.resize(4 + 1 + bitmap_bytes, 0);

        for (i, &val) in values.iter().enumerate() {
            if val {
                let byte_idx = i / 8;
                let bit_idx = 7 - (i % 8); // MSB first
                buf[5 + byte_idx] |= 1 << bit_idx;
            }
        }

        Ok(buf)
    }

    /// Encode a sequence of nullable boolean values.
    ///
    /// `None` values are represented in a separate null bitmap.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode_nullable(values: &[Option<bool>]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "bitmap encoder (nullable)",
            });
        }

        let has_nulls = values.iter().any(Option::is_none);
        let bitmap_bytes = values.len().div_ceil(8);

        let total_size = 4 + 1 + bitmap_bytes + if has_nulls { bitmap_bytes } else { 0 };
        let count = checked_count(values.len())?;
        let mut buf = vec![0u8; total_size];

        // Count
        buf[..4].copy_from_slice(&count.to_le_bytes());
        // Has nulls flag
        buf[4] = u8::from(has_nulls);

        // Value bitmap
        for (i, val) in values.iter().enumerate() {
            if val.unwrap_or(false) {
                let byte_idx = i / 8;
                let bit_idx = 7 - (i % 8);
                buf[5 + byte_idx] |= 1 << bit_idx;
            }
        }

        // Null bitmap (1 = valid, 0 = null)
        if has_nulls {
            let null_offset = 5 + bitmap_bytes;
            for (i, val) in values.iter().enumerate() {
                if val.is_some() {
                    let byte_idx = i / 8;
                    let bit_idx = 7 - (i % 8);
                    buf[null_offset + byte_idx] |= 1 << bit_idx;
                }
            }
        }

        Ok(buf)
    }
}

impl BitmapDecoder {
    /// Decode a bitmap-encoded byte buffer back to boolean values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is malformed.
    pub fn decode(data: &[u8]) -> Result<Vec<bool>> {
        if data.len() < 5 {
            return Err(EncodingError::CorruptData {
                detail: "bitmap data too short for header".to_string(),
            });
        }

        let count = checked_decode_count(
            u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize,
            "bitmap",
        )?;
        let has_nulls = data[4];
        if has_nulls != 0 {
            return Err(EncodingError::CorruptData {
                detail: "non-nullable decode called on nullable bitmap data".to_string(),
            });
        }

        let bitmap_bytes = count.div_ceil(8);
        if data.len() < 5 + bitmap_bytes {
            return Err(EncodingError::CorruptData {
                detail: "bitmap data truncated".to_string(),
            });
        }

        let mut result = Vec::with_capacity(count);
        for i in 0..count {
            let byte_idx = i / 8;
            let bit_idx = 7 - (i % 8);
            let val = (data[5 + byte_idx] >> bit_idx) & 1 == 1;
            result.push(val);
        }

        Ok(result)
    }

    /// Decode a nullable bitmap-encoded byte buffer.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is malformed.
    pub fn decode_nullable(data: &[u8]) -> Result<Vec<Option<bool>>> {
        if data.len() < 5 {
            return Err(EncodingError::CorruptData {
                detail: "bitmap data too short for header".to_string(),
            });
        }

        let count = checked_decode_count(
            u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize,
            "bitmap",
        )?;
        let has_nulls = data[4] != 0;

        let bitmap_bytes = count.div_ceil(8);
        let expected_len = 5 + bitmap_bytes + if has_nulls { bitmap_bytes } else { 0 };
        if data.len() < expected_len {
            return Err(EncodingError::CorruptData {
                detail: "nullable bitmap data truncated".to_string(),
            });
        }

        let mut result = Vec::with_capacity(count);
        let null_offset = 5 + bitmap_bytes;

        for i in 0..count {
            let byte_idx = i / 8;
            let bit_idx = 7 - (i % 8);

            let is_valid = if has_nulls {
                (data[null_offset + byte_idx] >> bit_idx) & 1 == 1
            } else {
                true
            };

            if is_valid {
                let val = (data[5 + byte_idx] >> bit_idx) & 1 == 1;
                result.push(Some(val));
            } else {
                result.push(None);
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_all_true() {
        let values = vec![true; 100];
        let encoded = BitmapEncoder::encode(&values).unwrap();
        let decoded = BitmapDecoder::decode(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn roundtrip_all_false() {
        let values = vec![false; 100];
        let encoded = BitmapEncoder::encode(&values).unwrap();
        let decoded = BitmapDecoder::decode(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn roundtrip_alternating() {
        let values: Vec<bool> = (0..100).map(|i| i % 2 == 0).collect();
        let encoded = BitmapEncoder::encode(&values).unwrap();
        let decoded = BitmapDecoder::decode(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn roundtrip_non_aligned() {
        // 13 values — not a multiple of 8
        let values = vec![
            true, false, true, true, false, false, true, false, true, false, true, true, false,
        ];
        let encoded = BitmapEncoder::encode(&values).unwrap();
        let decoded = BitmapDecoder::decode(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn roundtrip_nullable() {
        let values = vec![Some(true), Some(false), None, Some(true), None, Some(false)];
        let encoded = BitmapEncoder::encode_nullable(&values).unwrap();
        let decoded = BitmapDecoder::decode_nullable(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn roundtrip_nullable_no_nulls() {
        let values = vec![Some(true), Some(false), Some(true)];
        let encoded = BitmapEncoder::encode_nullable(&values).unwrap();
        let decoded = BitmapDecoder::decode_nullable(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn roundtrip_nullable_all_null() {
        let values = vec![None, None, None];
        let encoded = BitmapEncoder::encode_nullable(&values).unwrap();
        let decoded = BitmapDecoder::decode_nullable(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn empty_error() {
        assert!(BitmapEncoder::encode(&[]).is_err());
        let vals: Vec<Option<bool>> = vec![];
        assert!(BitmapEncoder::encode_nullable(&vals).is_err());
    }

    #[test]
    fn compression_ratio() {
        let values = vec![true; 10_000];
        let raw_size = values.len(); // 1 byte each as Rust bool
        let encoded = BitmapEncoder::encode(&values).unwrap();
        #[allow(clippy::cast_precision_loss)]
        let ratio = raw_size as f64 / encoded.len() as f64;
        // Should achieve ~8x (1 bit vs 1 byte) minus header
        assert!(ratio > 6.0, "Expected ratio > 6x, got {ratio:.1}x");
    }

    #[test]
    fn single_value() {
        let encoded = BitmapEncoder::encode(&[true]).unwrap();
        let decoded = BitmapDecoder::decode(&encoded).unwrap();
        assert_eq!(vec![true], decoded);

        let encoded = BitmapEncoder::encode(&[false]).unwrap();
        let decoded = BitmapDecoder::decode(&encoded).unwrap();
        assert_eq!(vec![false], decoded);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip_bool(values in proptest::collection::vec(any::<bool>(), 1..500)) {
            let encoded = BitmapEncoder::encode(&values).unwrap();
            let decoded = BitmapDecoder::decode(&encoded).unwrap();
            prop_assert_eq!(&values, &decoded);
        }

        #[test]
        fn roundtrip_nullable(values in proptest::collection::vec(any::<Option<bool>>(), 1..500)) {
            let encoded = BitmapEncoder::encode_nullable(&values).unwrap();
            let decoded = BitmapDecoder::decode_nullable(&encoded).unwrap();
            prop_assert_eq!(&values, &decoded);
        }
    }
}
