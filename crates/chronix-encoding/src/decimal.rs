//! Encoding for exact decimal columns — the `i128` mantissa stream.
//!
//! A decimal column stores `mantissa × 10⁻ˢᶜᵃˡᵉ` with the scale held once in
//! the column's metadata, so what reaches this codec is a column of plain
//! integers. That is the whole argument for the type: the README's
//! compression section already observes that most float columns are decimals
//! that went through an `f64`, and that ALP and pco spend their first stage
//! *recovering* the integer. A decimal column hands that integer over
//! directly — nothing to recover, nothing lost recovering it.
//!
//! # Two physical forms
//!
//! Almost every real decimal column is narrow. A quarter-hour meter register
//! in watt-hours at four decimal places needs 19 digits to reach 10¹⁴ kWh, so
//! its mantissas fit an `i64` with room to spare, and the interesting shapes
//! — a monotone counter, a slowly varying price, a constant tariff — are
//! exactly the shapes the `i64` stack is built for.
//!
//! ```text
//! [form: u8 = 0][inner i64 block: tag + payload]   narrow
//! [form: u8 = 1][count: u32][first: i128 LE][zigzag varint deltas…]  wide
//! ```
//!
//! The narrow form is not a second codec: it delegates to
//! [`ColumnEncoder::encode_i64`](crate::ColumnEncoder::encode_i64), so a
//! decimal column gets RLE on a constant register, frame-of-reference on a
//! narrow range, and pco on everything else — the same trial-and-keep-the-
//! smallest selection an `i64` field gets, for free and for ever.
//!
//! The wide form exists for the columns that genuinely need more than 63
//! bits of mantissa: 38-digit financial quantities, or a scale so large that
//! ordinary values overflow. Delta + `ZigZag` + LEB128 keeps a
//! slowly-varying wide column near the narrow one's size, and a random wide
//! column costs at most 19 bytes per value against the 16 it would take raw.

use crate::coding::{checked_count, checked_decode_count};
use crate::error::{EncodingError, Result};
use crate::unified::{ColumnDecoder, ColumnEncoder, DecodedColumn, EncodedBlock, EncodingType};

/// Physical form tag: mantissas that fit `i64`, delegated to the `i64` stack.
pub(crate) const FORM_NARROW: u8 = 0;
/// Physical form tag: full-width `i128` mantissas, delta + `ZigZag` + varint.
pub(crate) const FORM_WIDE: u8 = 1;

/// `ZigZag` encode a signed 128-bit integer to unsigned.
#[inline]
fn zigzag_encode_i128(n: i128) -> u128 {
    ((n << 1) ^ (n >> 127)) as u128
}

/// `ZigZag` decode an unsigned 128-bit integer back to signed.
#[inline]
fn zigzag_decode_i128(n: u128) -> i128 {
    ((n >> 1) as i128) ^ -((n & 1) as i128)
}

/// Append a LEB128 varint for a `u128`.
#[inline]
fn varint_encode_u128(mut value: u128, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Read one LEB128 varint, returning the value and the bytes consumed.
fn varint_decode_u128(bytes: &[u8]) -> Result<(u128, usize)> {
    let mut value: u128 = 0;
    let mut shift = 0u32;
    for (i, &byte) in bytes.iter().enumerate() {
        // 128 bits at 7 bits per byte is 19 bytes; the last one carries the
        // final bit. A longer run is corrupt, not a large number.
        if shift > 126 {
            return Err(EncodingError::CorruptData {
                detail: "decimal varint exceeds 128 bits".to_string(),
            });
        }
        value |= u128::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
        shift += 7;
    }
    Err(EncodingError::CorruptData {
        detail: "decimal varint truncated".to_string(),
    })
}

/// Encoder for a column of `i128` decimal mantissas.
#[derive(Debug, Clone, Copy)]
pub struct DecimalEncoder;

impl DecimalEncoder {
    /// Encode a mantissa column, choosing the narrow or wide form.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty, and
    /// propagates any error from the delegated `i64` encoder.
    pub fn encode(values: &[i128]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "decimal encoder",
            });
        }

        // The narrow form is the common one, so the check that selects it is
        // a single pass with no allocation on the wide path.
        let narrow = values.iter().all(|&v| i64::try_from(v).is_ok());

        if narrow {
            let as_i64: Vec<i64> = values.iter().map(|&v| v as i64).collect();
            let inner = ColumnEncoder::encode_i64(&as_i64)?;
            let mut out = Vec::with_capacity(2 + inner.payload.len());
            out.push(FORM_NARROW);
            out.push(inner.encoding.tag());
            out.extend_from_slice(&inner.payload);
            return Ok(out);
        }

        let count = checked_count(values.len())?;
        let mut out = Vec::with_capacity(1 + 4 + 16 + values.len() * 4);
        out.push(FORM_WIDE);
        out.extend_from_slice(&count.to_le_bytes());
        out.extend_from_slice(&values[0].to_le_bytes());
        for window in values.windows(2) {
            // Wrapping: the difference of two 128-bit mantissas can exceed
            // `i128`, and the decoder undoes it with the matching wrap.
            let delta = window[1].wrapping_sub(window[0]);
            varint_encode_u128(zigzag_encode_i128(delta), &mut out);
        }
        Ok(out)
    }
}

/// Decoder for a column of `i128` decimal mantissas.
#[derive(Debug, Clone, Copy)]
pub struct DecimalDecoder;

impl DecimalDecoder {
    /// Decode a mantissa column.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the form byte is unknown,
    /// the payload is truncated, or the narrow form's inner block does not
    /// decode to `i64` values.
    pub fn decode(payload: &[u8]) -> Result<Vec<i128>> {
        let (&form, rest) = payload
            .split_first()
            .ok_or_else(|| EncodingError::CorruptData {
                detail: "empty decimal block".to_string(),
            })?;

        match form {
            FORM_NARROW => Self::decode_narrow(rest),
            FORM_WIDE => Self::decode_wide(rest),
            other => Err(EncodingError::CorruptData {
                detail: format!("unknown decimal form: {other}"),
            }),
        }
    }

    /// Decode the narrow form by handing the inner block back to the `i64`
    /// stack.
    fn decode_narrow(rest: &[u8]) -> Result<Vec<i128>> {
        let (&tag, inner) = rest
            .split_first()
            .ok_or_else(|| EncodingError::CorruptData {
                detail: "narrow decimal block has no inner encoding tag".to_string(),
            })?;
        let encoding = EncodingType::from_tag(tag)?;
        // A corrupt tag naming this codec again would recurse until the
        // stack ran out, so the one encoding the inner block may never be is
        // this one. Checked before the decode, not after.
        if encoding == EncodingType::DecimalI128 {
            return Err(EncodingError::CorruptData {
                detail: "narrow decimal block nests a decimal block".to_string(),
            });
        }
        let block = EncodedBlock {
            encoding,
            payload: inner.to_vec(),
        };
        match ColumnDecoder::decode(&block)? {
            DecodedColumn::I64(values) => Ok(values.into_iter().map(i128::from).collect()),
            other => Err(EncodingError::CorruptData {
                detail: format!(
                    "narrow decimal block decoded to {} values, expected i64",
                    match other {
                        DecodedColumn::F64(_) => "f64",
                        DecodedColumn::U64(_) => "u64",
                        DecodedColumn::Bool(_) => "bool",
                        DecodedColumn::String(_) => "string",
                        _ => "nullable or wide",
                    }
                ),
            }),
        }
    }

    /// Decode the wide form: first value plus zigzag varint deltas.
    fn decode_wide(rest: &[u8]) -> Result<Vec<i128>> {
        if rest.len() < 4 + 16 {
            return Err(EncodingError::CorruptData {
                detail: "wide decimal block truncated".to_string(),
            });
        }
        let count = checked_decode_count(
            u32::from_le_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize,
            "decimal",
        )?;
        if count == 0 {
            return Err(EncodingError::CorruptData {
                detail: "wide decimal block declares zero values".to_string(),
            });
        }
        let mut first_bytes = [0u8; 16];
        first_bytes.copy_from_slice(&rest[4..20]);
        let mut current = i128::from_le_bytes(first_bytes);

        let mut values = Vec::with_capacity(count);
        values.push(current);
        let mut cursor = 20;
        for _ in 1..count {
            let (zz, used) = varint_decode_u128(&rest[cursor..])?;
            cursor += used;
            current = current.wrapping_add(zigzag_decode_i128(zz));
            values.push(current);
        }
        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(values: &[i128]) {
        let encoded = DecimalEncoder::encode(values).unwrap();
        let decoded = DecimalDecoder::decode(&encoded).unwrap();
        assert_eq!(decoded, values, "round trip");
    }

    #[test]
    fn narrow_round_trip() {
        round_trip(&[23_145]);
        round_trip(&[0, 1, 2, 3, 4, 5]);
        round_trip(&[-5, -4, -3, 0, 3, 4, 5]);
        round_trip(&[i64::MAX as i128, i64::MIN as i128]);
    }

    #[test]
    fn wide_round_trip() {
        round_trip(&[i128::from(i64::MAX) + 1]);
        round_trip(&[
            99_999_999_999_999_999_999_999_999_999_999_999_999,
            -99_999_999_999_999_999_999_999_999_999_999_999_999,
            0,
        ]);
    }

    #[test]
    fn form_is_narrow_when_every_mantissa_fits_i64() {
        let encoded = DecimalEncoder::encode(&[1, 2, 3]).unwrap();
        assert_eq!(encoded[0], FORM_NARROW);
        let encoded = DecimalEncoder::encode(&[i128::from(i64::MAX) + 1]).unwrap();
        assert_eq!(encoded[0], FORM_WIDE);
    }

    #[test]
    fn a_monotone_register_compresses() {
        // A quarter-hour meter register in Wh at scale 4: strictly rising by
        // roughly the same amount every step, which is what the i64 stack is
        // built for.
        let values: Vec<i128> = (0..4096).map(|i| 10_000_000_000 + i * 2_500_000).collect();
        let encoded = DecimalEncoder::encode(&values).unwrap();
        let raw = values.len() * 16;
        assert!(
            encoded.len() * 8 < raw,
            "expected >8x on a monotone register, got {} vs {raw}",
            encoded.len()
        );
        assert_eq!(DecimalDecoder::decode(&encoded).unwrap(), values);
    }

    #[test]
    fn a_constant_register_compresses_hard() {
        let values = vec![42_0000i128; 4096];
        let encoded = DecimalEncoder::encode(&values).unwrap();
        assert!(
            encoded.len() < 128,
            "constant column: {} bytes",
            encoded.len()
        );
        assert_eq!(DecimalDecoder::decode(&encoded).unwrap(), values);
    }

    #[test]
    fn empty_input_is_an_error() {
        assert!(matches!(
            DecimalEncoder::encode(&[]),
            Err(EncodingError::EmptyInput { .. })
        ));
    }

    #[test]
    fn corrupt_form_byte_is_an_error() {
        assert!(DecimalDecoder::decode(&[]).is_err());
        assert!(DecimalDecoder::decode(&[7]).is_err());
    }

    #[test]
    fn a_nested_decimal_tag_does_not_recurse() {
        // The inner tag naming this codec again is the one shape that would
        // recurse until the stack ran out.
        let nested = [FORM_NARROW, EncodingType::DecimalI128.tag(), 0, 0];
        assert!(DecimalDecoder::decode(&nested).is_err());
    }

    #[test]
    fn truncated_wide_block_is_an_error() {
        let encoded = DecimalEncoder::encode(&[i128::from(i64::MAX) + 1, 5, 9]).unwrap();
        for cut in 1..encoded.len() {
            // Never a panic: every truncation is either an error or a
            // shorter-but-valid prefix the length check rejects.
            let _ = DecimalDecoder::decode(&encoded[..cut]);
        }
    }

    #[test]
    fn a_corrupt_count_cannot_allocate_the_machine_away() {
        let mut encoded = DecimalEncoder::encode(&[i128::from(i64::MAX) + 1]).unwrap();
        encoded[1..5].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(DecimalDecoder::decode(&encoded).is_err());
    }

    #[test]
    fn varint_round_trip_at_the_boundaries() {
        for value in [0u128, 1, 127, 128, u128::from(u64::MAX), u128::MAX] {
            let mut buf = Vec::new();
            varint_encode_u128(value, &mut buf);
            let (decoded, used) = varint_decode_u128(&buf).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(used, buf.len());
        }
    }

    #[test]
    fn zigzag_round_trip_at_the_boundaries() {
        for value in [0i128, 1, -1, i128::MAX, i128::MIN, i128::from(i64::MIN)] {
            assert_eq!(zigzag_decode_i128(zigzag_encode_i128(value)), value);
        }
    }
}
