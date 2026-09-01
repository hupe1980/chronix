//! Dictionary encoding for string columns (tags and low-cardinality strings).
//!
//! Maps unique string values to integer indices and encodes the indices with
//! minimal bit-packing. The dictionary is stored inline in the encoded block.
//!
//! ## Format
//!
//! ```text
//! [value_count: u32]
//! [dict_size: u32]
//! [dict entry 0: len u16, bytes...]
//! [dict entry 1: len u16, bytes...]
//! ...
//! [bit_width: u8]
//! [packed indices at bit_width bits each]
//! ```

use std::collections::HashMap;

use crate::coding::{
    bits_needed, checked_count, checked_decode_count, checked_string_len, pack_bits, unpack_bits,
};
use crate::error::{EncodingError, Result};

/// Maximum number of dictionary entries (u32::MAX).
const MAX_DICT_SIZE: usize = u32::MAX as usize;

/// Dictionary-based string encoder.
#[derive(Debug, Clone, Copy)]
pub struct DictionaryEncoder;

/// Dictionary-based string decoder.
#[derive(Debug, Clone, Copy)]
pub struct DictionaryDecoder;

/// Threshold below which a linear scan over a Vec is faster than
/// HashMap lookup due to cache locality.
const LINEAR_SCAN_THRESHOLD: usize = 16;

impl DictionaryEncoder {
    /// Encode a sequence of string values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty, or
    /// [`EncodingError::DictionaryOverflow`] if there are more than 2^32−1
    /// unique values.
    pub fn encode(values: &[&str]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "dictionary encoder",
            });
        }

        // Build dictionary: unique value → index.
        // For low-cardinality columns (< LINEAR_SCAN_THRESHOLD unique values),
        // use Vec linear scan which is faster due to cache locality.
        let mut dict: Vec<&str> = Vec::new();
        let mut index_map: Option<HashMap<&str, usize>> = None;
        let mut indices = Vec::with_capacity(values.len());

        for &val in values {
            let idx = if let Some(ref map) = index_map {
                // HashMap path for high cardinality
                if let Some(&idx) = map.get(val) {
                    idx
                } else {
                    let idx = dict.len();
                    if idx >= MAX_DICT_SIZE {
                        return Err(EncodingError::DictionaryOverflow {
                            count: idx + 1,
                            limit: MAX_DICT_SIZE,
                        });
                    }
                    dict.push(val);
                    index_map
                        .as_mut()
                        .expect("inside Some arm")
                        .insert(val, idx);
                    idx
                }
            } else {
                // Vec linear scan path for low cardinality
                if let Some(pos) = dict.iter().position(|&v| v == val) {
                    pos
                } else {
                    let idx = dict.len();
                    if idx >= MAX_DICT_SIZE {
                        return Err(EncodingError::DictionaryOverflow {
                            count: idx + 1,
                            limit: MAX_DICT_SIZE,
                        });
                    }
                    dict.push(val);
                    // Promote to HashMap once we exceed the linear scan threshold
                    if dict.len() >= LINEAR_SCAN_THRESHOLD {
                        let map: HashMap<&str, usize> =
                            dict.iter().enumerate().map(|(i, &v)| (v, i)).collect();
                        index_map = Some(map);
                    }
                    idx
                }
            };
            indices.push(idx as u64);
        }

        // Bit width for indices
        let max_idx = dict.len().saturating_sub(1) as u64;
        let bit_width = if max_idx == 0 {
            0_u8 // single unique value
        } else {
            bits_needed(max_idx)
        };

        // Estimate buffer size
        let dict_bytes: usize = dict.iter().map(|s| 2 + s.len()).sum();
        let idx_bytes = (values.len() as u64 * u64::from(bit_width)).div_ceil(8) as usize;
        let mut buf = Vec::with_capacity(4 + 4 + dict_bytes + 1 + idx_bytes);

        // Value count
        let count = checked_count(values.len())?;
        buf.extend_from_slice(&count.to_le_bytes());

        // Dictionary size (u32)
        buf.extend_from_slice(&(dict.len() as u32).to_le_bytes());

        // Dictionary entries — validate string lengths fit in u16
        for &s in &dict {
            let len = checked_string_len(s.len())?;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }

        // Bit width
        buf.push(bit_width);

        // Packed indices
        if bit_width > 0 {
            pack_bits(&indices, bit_width, &mut buf);
        }

        Ok(buf)
    }
}

impl DictionaryDecoder {
    /// Decode a dictionary-encoded byte buffer back to owned strings.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is malformed.
    pub fn decode(data: &[u8]) -> Result<Vec<String>> {
        if data.len() < 8 {
            return Err(EncodingError::CorruptData {
                detail: "dictionary data too short for header".to_string(),
            });
        }

        let mut pos = 0;

        // Value count
        let count = checked_decode_count(
            u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize,
            "dictionary",
        )?;
        pos += 4;

        // Dictionary size (u32)
        let dict_size = checked_decode_count(
            u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize,
            "dictionary",
        )?;
        pos += 4;

        // Read dictionary entries
        let mut dict = Vec::with_capacity(dict_size);
        for _ in 0..dict_size {
            if pos + 2 > data.len() {
                return Err(EncodingError::CorruptData {
                    detail: "dictionary entry truncated".to_string(),
                });
            }
            let len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;
            if pos + len > data.len() {
                return Err(EncodingError::CorruptData {
                    detail: "dictionary string truncated".to_string(),
                });
            }
            let s = std::str::from_utf8(&data[pos..pos + len]).map_err(|_| {
                EncodingError::CorruptData {
                    detail: "invalid UTF-8 in dictionary".to_string(),
                }
            })?;
            dict.push(s.to_string());
            pos += len;
        }

        if pos >= data.len() {
            return Err(EncodingError::CorruptData {
                detail: "missing bit_width byte".to_string(),
            });
        }

        let bit_width = data[pos];
        pos += 1;

        // Unpack indices
        let mut result = Vec::with_capacity(count);
        if bit_width == 0 {
            // All values map to dict[0]
            let val = dict
                .first()
                .ok_or_else(|| EncodingError::CorruptData {
                    detail: "empty dictionary with zero bit_width".to_string(),
                })?
                .clone();
            for _ in 0..count {
                result.push(val.clone());
            }
        } else {
            let packed_data = &data[pos..];
            let indices = unpack_bits(packed_data, count, bit_width)?;
            for idx in indices {
                let idx = idx as usize;
                if idx >= dict.len() {
                    return Err(EncodingError::CorruptData {
                        detail: format!("index {idx} exceeds dictionary size {}", dict.len()),
                    });
                }
                result.push(dict[idx].clone());
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_low_cardinality() {
        let values = vec!["host1", "host2", "host1", "host3", "host2", "host1"];
        let encoded = DictionaryEncoder::encode(&values).unwrap();
        let decoded = DictionaryDecoder::decode(&encoded).unwrap();
        let expected: Vec<String> = values.into_iter().map(String::from).collect();
        assert_eq!(expected, decoded);
    }

    #[test]
    fn roundtrip_single_repeated() {
        let values = vec!["constant"; 100];
        let encoded = DictionaryEncoder::encode(&values).unwrap();
        let decoded = DictionaryDecoder::decode(&encoded).unwrap();
        assert!(decoded.iter().all(|s| s == "constant"));
        // Single unique value → bit_width=0 → very compact
    }

    #[test]
    fn roundtrip_all_unique() {
        let strs: Vec<String> = (0..100).map(|i| format!("val_{i}")).collect();
        let values: Vec<&str> = strs.iter().map(String::as_str).collect();
        let encoded = DictionaryEncoder::encode(&values).unwrap();
        let decoded = DictionaryDecoder::decode(&encoded).unwrap();
        assert_eq!(strs, decoded);
    }

    #[test]
    fn roundtrip_unicode() {
        let values = vec!["hello", "世界", "🦀", "café", "naïve"];
        let encoded = DictionaryEncoder::encode(&values).unwrap();
        let decoded = DictionaryDecoder::decode(&encoded).unwrap();
        let expected: Vec<String> = values.into_iter().map(String::from).collect();
        assert_eq!(expected, decoded);
    }

    #[test]
    fn roundtrip_empty_strings() {
        let values = vec!["", "a", "", "b", ""];
        let encoded = DictionaryEncoder::encode(&values).unwrap();
        let decoded = DictionaryDecoder::decode(&encoded).unwrap();
        let expected: Vec<String> = values.into_iter().map(String::from).collect();
        assert_eq!(expected, decoded);
    }

    #[test]
    fn roundtrip_single_value() {
        let values = vec!["only"];
        let encoded = DictionaryEncoder::encode(&values).unwrap();
        let decoded = DictionaryDecoder::decode(&encoded).unwrap();
        assert_eq!(vec!["only".to_string()], decoded);
    }

    #[test]
    fn empty_error() {
        let values: Vec<&str> = vec![];
        assert!(DictionaryEncoder::encode(&values).is_err());
    }

    #[test]
    fn compression_ratio_tags() {
        // Simulate 10K tag values from 5 hosts
        let hosts = ["host1", "host2", "host3", "host4", "host5"];
        let values: Vec<&str> = (0..10_000).map(|i| hosts[i % 5]).collect();
        let raw_size: usize = values.iter().map(|s| 8 + s.len()).sum(); // ptr + len + data
        let encoded = DictionaryEncoder::encode(&values).unwrap();
        #[allow(clippy::cast_precision_loss)]
        let ratio = raw_size as f64 / encoded.len() as f64;
        assert!(
            ratio > 3.0,
            "Expected ratio > 3x on tag data, got {ratio:.1}x"
        );
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip_string(values in proptest::collection::vec("[a-z]{0,8}", 1..200)) {
            let refs: Vec<&str> = values.iter().map(String::as_str).collect();
            let encoded = DictionaryEncoder::encode(&refs).unwrap();
            let decoded = DictionaryDecoder::decode(&encoded).unwrap();
            prop_assert_eq!(&values, &decoded);
        }
    }
}
