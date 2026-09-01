//! Shared encoding utilities: `ZigZag` coding and bit-packing.
//!
//! These primitives are used by multiple encoders (delta, integer, dictionary)
//! and are consolidated here to avoid duplication.

use crate::error::{EncodingError, Result};

/// `ZigZag` encode a signed integer to unsigned.
///
/// Maps negative values to odd positives and non-negative values to even
/// positives, so small-magnitude values produce small unsigned values
/// regardless of sign.
#[inline]
pub(crate) fn zigzag_encode(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

/// `ZigZag` decode an unsigned integer back to signed.
#[inline]
pub(crate) fn zigzag_decode(n: u64) -> i64 {
    ((n >> 1) as i64) ^ -((n & 1) as i64)
}

/// Compute the minimum number of bits needed to represent a value.
#[inline]
pub(crate) fn bits_needed(max_val: u64) -> u8 {
    if max_val == 0 {
        return 0;
    }
    64 - max_val.leading_zeros() as u8
}

/// Validate that a length fits in `u32`, returning an error on overflow.
///
/// This prevents silent truncation when encoding a `values.len()` as `u32`.
#[inline]
pub(crate) fn checked_count(len: usize) -> Result<u32> {
    u32::try_from(len).map_err(|_| EncodingError::CorruptData {
        detail: format!("value count {len} exceeds u32::MAX"),
    })
}

/// Largest element a decoder can produce, in bytes.
///
/// `Option<String>` and `String` are 24 bytes on every target this builds
/// for, and they are what [`MAX_BLOCK_VALUES`] has to be sized against. The
/// previous ceiling was reasoned about as `count × 8` because the codec that
/// motivated it decoded `f64`, which under-states the worst case by 3×.
pub(crate) const MAX_ELEMENT_BYTES: usize = 24;

/// Largest allocation a single corrupt block header may cause.
///
/// The number that matters is bytes, not values: the guard exists so that a
/// flipped bit on eMMC produces a decode error rather than a killed process,
/// and a process is killed by bytes. 32 MiB is small enough to survive on the
/// 512 MB gateway this engine targets and ~20× larger than the biggest block
/// the writer can legitimately produce.
pub(crate) const MAX_DECODED_BYTES: usize = 32 << 20;

/// Largest value count a decoder will honour from a block header.
///
/// **Derived, not chosen.** It is [`MAX_DECODED_BYTES`] divided by the widest
/// element a decoder can produce, so the byte bound holds by construction
/// rather than by a test somebody has to remember to update. That matters
/// because the previous ceiling was chosen directly — `2^24`, justified in a
/// comment by `2^24 × 8 B = 128 MiB` — and the `8` was the size of the `f64`
/// in the codec that motivated the guard, not the 24 bytes of the
/// `Option<String>` that a nullable block actually decodes to. The stated
/// 128 MiB bound was really 384 MiB, and the workspace's own decoder
/// robustness proptest was being OOM-killed by it.
///
/// The result is ~1.4M values, still ~21× the default row-group size
/// (`chronix_engine::segment::DEFAULT_ROW_GROUP_SIZE`, 65 536), which is the
/// most values any block this crate writes can hold.
pub(crate) const MAX_BLOCK_VALUES: usize = MAX_DECODED_BYTES / MAX_ELEMENT_BYTES;

/// The ceiling must never reject a legitimate block. A `.csx` row group holds
/// at most `chronix_engine::segment::DEFAULT_ROW_GROUP_SIZE` (65 536) rows, so
/// the widest column block is that many values; the constant is repeated here
/// rather than imported because `chronix-encoding` is standalone on purpose
///. Checked at compile time, so lowering `MAX_DECODED_BYTES` far enough
/// to break the format fails the build rather than a test run.
const _: () = assert!(
    MAX_BLOCK_VALUES >= 65_536 * 16,
    "MAX_DECODED_BYTES leaves no headroom above a full row group"
);

/// Validate a value count read from an untrusted block header.
///
/// Every decoder allocates `count` elements up front, so `count` is the one
/// header field that turns a corrupt byte into an out-of-memory abort. The
/// per-codec truncation checks do **not** cover it: a block whose bit width is
/// zero — a legitimate encoding for a constant run — has no payload at all, so
/// there is nothing for the payload length to bound `count` against. A 19-byte
/// ALP block declaring `u32::MAX` values decoded to a 34 GiB `Vec<f64>` before
/// this guard existed, which on the flash-constrained gateway that is the
/// design partner is a killed process rather than a decode error, and reaching
/// it needs a single flipped bit rather than an attacker.
#[inline]
pub(crate) fn checked_decode_count(count: usize, codec: &'static str) -> Result<usize> {
    if count > MAX_BLOCK_VALUES {
        return Err(EncodingError::CorruptData {
            detail: format!(
                "{codec} block declares {count} values, above the {MAX_BLOCK_VALUES} ceiling — \
                 header is corrupt"
            ),
        });
    }
    Ok(count)
}

/// Validate that a string length fits in `u16`, returning an error on overflow.
#[inline]
pub(crate) fn checked_string_len(len: usize) -> Result<u16> {
    u16::try_from(len).map_err(|_| EncodingError::CorruptData {
        detail: format!("string length {len} exceeds u16::MAX"),
    })
}

// ── Bit-packing ────────────────────────────────────────────────────────

/// Pack unsigned values at a fixed bit width into a byte buffer.
///
/// Values are packed MSB-first. The buffer is extended to hold all packed
/// bits, padded to byte boundaries.
///
/// Uses a u64 word accumulator for bulk packing — at most two word-level
/// operations per value instead of per-bit function calls.
pub(crate) fn pack_bits(values: &[u64], bit_width: u8, buf: &mut Vec<u8>) {
    if bit_width == 0 || values.is_empty() {
        return;
    }

    // u64 accumulator — left-aligned, MSB-first (same scheme as BitWriter)
    let mut accumulator: u64 = 0;
    let mut bits_in_acc: u8 = 0;

    for &val in values {
        let masked = if bit_width >= 64 {
            val
        } else {
            val & ((1u64 << bit_width) - 1)
        };

        let space = 64 - bits_in_acc;
        if bit_width <= space {
            accumulator |= masked << (space - bit_width);
            bits_in_acc += bit_width;
            if bits_in_acc == 64 {
                buf.extend_from_slice(&accumulator.to_be_bytes());
                accumulator = 0;
                bits_in_acc = 0;
            }
        } else {
            // Split across word boundary
            accumulator |= masked >> (bit_width - space);
            buf.extend_from_slice(&accumulator.to_be_bytes());
            let remaining = bit_width - space;
            accumulator = masked << (64 - remaining);
            bits_in_acc = remaining;
        }
    }

    // Flush remaining bits
    if bits_in_acc > 0 {
        let bytes_needed = bits_in_acc.div_ceil(8) as usize;
        let word_bytes = accumulator.to_be_bytes();
        buf.extend_from_slice(&word_bytes[..bytes_needed]);
    }
}

/// Unpack values from a bit-packed byte buffer.
///
/// Returns a vector of `count` unsigned values, each `bit_width` bits wide.
///
/// Reads a byte-at-a-time instead of bit-at-a-time for up to 8× fewer
/// loop iterations per value.
///
/// # Errors
///
/// Returns [`EncodingError::CorruptData`] if the buffer is too short or
/// `bit_width` exceeds 64.
pub(crate) fn unpack_bits(data: &[u8], count: usize, bit_width: u8) -> Result<Vec<u64>> {
    if bit_width > 64 {
        return Err(EncodingError::CorruptData {
            detail: format!("invalid bit_width {bit_width}, max is 64"),
        });
    }

    if bit_width == 0 {
        return Ok(vec![0; count]);
    }

    let mut result = Vec::with_capacity(count);
    let mut byte_pos: usize = 0;
    let mut bit_pos: u8 = 0;

    for _ in 0..count {
        let mut val: u64 = 0;
        let mut remaining = bit_width;

        while remaining > 0 {
            if byte_pos >= data.len() {
                return Err(EncodingError::CorruptData {
                    detail: "unexpected end of bit-packed data".to_string(),
                });
            }

            let bits_left_in_byte = 8 - bit_pos;
            let to_read = remaining.min(bits_left_in_byte);
            let shift = bits_left_in_byte - to_read;
            let mask: u8 = if to_read >= 8 {
                0xFF
            } else {
                (1u8 << to_read) - 1
            };
            let bits = (data[byte_pos] >> shift) & mask;

            val = (val << to_read) | u64::from(bits);
            remaining -= to_read;
            bit_pos += to_read;

            if bit_pos >= 8 {
                byte_pos += 1;
                bit_pos = 0;
            }
        }

        result.push(val);
    }

    Ok(result)
}

// ── LEB128 Varint ──────────────────────────────────────────────────────

/// Encode a `u64` value as LEB128 varint into a byte buffer.
///
/// Each byte uses 7 data bits and 1 continuation bit (MSB).
/// Small values (≤ 127) encode in a single byte; the maximum encoding
/// length is 10 bytes for `u64::MAX`.
pub(crate) fn varint_encode(mut val: u64, buf: &mut Vec<u8>) {
    loop {
        let mut byte = (val & 0x7F) as u8;
        val >>= 7;
        if val != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if val == 0 {
            break;
        }
    }
}

/// Decode a LEB128 varint from a byte slice.
///
/// Returns `(value, bytes_consumed)`.
///
/// # Errors
///
/// Returns [`EncodingError::CorruptData`] if the data is truncated or
/// the varint exceeds 10 bytes (would overflow `u64`).
pub(crate) fn varint_decode(data: &[u8]) -> Result<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    for (i, &byte) in data.iter().enumerate() {
        if shift >= 70 {
            return Err(EncodingError::CorruptData {
                detail: "varint exceeds 10 bytes".to_string(),
            });
        }
        result |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
    }
    Err(EncodingError::CorruptData {
        detail: "unexpected end of varint data".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zigzag_roundtrip() {
        for v in [0, 1, -1, 63, -63, 64, -64, 255, -255, i64::MAX, i64::MIN] {
            assert_eq!(zigzag_decode(zigzag_encode(v)), v, "failed for {v}");
        }
    }

    #[test]
    fn varint_roundtrip() {
        for val in [0u64, 1, 127, 128, 255, 16383, 16384, u64::MAX / 2, u64::MAX] {
            let mut buf = Vec::new();
            varint_encode(val, &mut buf);
            let (decoded, consumed) = varint_decode(&buf).unwrap();
            assert_eq!(decoded, val, "varint roundtrip failed for {val}");
            assert_eq!(consumed, buf.len());
        }
    }

    #[test]
    fn varint_size() {
        // Values ≤ 127 fit in 1 byte
        let mut buf = Vec::new();
        varint_encode(127, &mut buf);
        assert_eq!(buf.len(), 1);

        buf.clear();
        varint_encode(128, &mut buf);
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn bits_needed_values() {
        assert_eq!(bits_needed(0), 0);
        assert_eq!(bits_needed(1), 1);
        assert_eq!(bits_needed(2), 2);
        assert_eq!(bits_needed(255), 8);
        assert_eq!(bits_needed(256), 9);
        assert_eq!(bits_needed(u64::MAX), 64);
    }

    #[test]
    fn pack_unpack_roundtrip() {
        let values = vec![0, 1, 3, 7, 15, 31, 63];
        let mut buf = Vec::new();
        pack_bits(&values, 6, &mut buf);
        let unpacked = unpack_bits(&buf, values.len(), 6).unwrap();
        assert_eq!(values, unpacked);
    }

    #[test]
    fn pack_unpack_64bit() {
        let values = vec![u64::MAX, 0, u64::MAX / 2];
        let mut buf = Vec::new();
        pack_bits(&values, 64, &mut buf);
        let unpacked = unpack_bits(&buf, values.len(), 64).unwrap();
        assert_eq!(values, unpacked);
    }

    #[test]
    fn unpack_invalid_bit_width() {
        let result = unpack_bits(&[0], 1, 65);
        assert!(result.is_err());
    }

    #[test]
    fn checked_count_success() {
        assert_eq!(checked_count(0).unwrap(), 0);
        assert_eq!(checked_count(100).unwrap(), 100);
        assert_eq!(checked_count(u32::MAX as usize).unwrap(), u32::MAX);
    }

    #[test]
    fn checked_count_overflow() {
        assert!(checked_count(u32::MAX as usize + 1).is_err());
    }

    #[test]
    fn checked_string_len_success() {
        assert_eq!(checked_string_len(0).unwrap(), 0);
        assert_eq!(checked_string_len(65535).unwrap(), 65535);
    }

    #[test]
    fn checked_string_len_overflow() {
        assert!(checked_string_len(65536).is_err());
    }
}

#[cfg(test)]
mod ceiling_tests {
    use super::MAX_ELEMENT_BYTES;

    /// `MAX_BLOCK_VALUES` is only a byte bound if `MAX_ELEMENT_BYTES` really
    /// is the widest element a decoder produces. Enumerating them is what
    /// makes adding a wider `DecodedColumn` variant fail here rather than
    /// silently raise the true ceiling.
    #[test]
    fn max_element_bytes_covers_every_decoded_element() {
        let widths = [
            ("f64", std::mem::size_of::<f64>()),
            ("i64", std::mem::size_of::<i64>()),
            ("u64", std::mem::size_of::<u64>()),
            ("bool", std::mem::size_of::<bool>()),
            ("String", std::mem::size_of::<String>()),
            ("Option<f64>", std::mem::size_of::<Option<f64>>()),
            ("Option<i64>", std::mem::size_of::<Option<i64>>()),
            ("Option<u64>", std::mem::size_of::<Option<u64>>()),
            ("Option<bool>", std::mem::size_of::<Option<bool>>()),
            ("Option<String>", std::mem::size_of::<Option<String>>()),
        ];
        for (name, width) in widths {
            assert!(
                width <= MAX_ELEMENT_BYTES,
                "{name} is {width} B, above the {MAX_ELEMENT_BYTES} B \
                 MAX_ELEMENT_BYTES the decode ceiling is derived from"
            );
        }
    }
}
