//! Delta-of-delta timestamp encoding.
//!
//! Exploits the regularity of time-series timestamps where successive deltas
//! are often identical (constant sampling rate). The first value and first delta
//! are stored raw; subsequent deltas-of-deltas are variable-length bit-packed.
//!
//! ## When to use delta-of-delta encoding
//!
//! **Ideal for:** Monotonically increasing or nearly-monotonic sequences
//! with a regular cadence, such as timestamps at a fixed sampling rate.
//! In this case, the delta-of-delta is frequently zero, yielding
//! compression ratios of 30× or higher (≈1 bit per value).
//!
//! **Acceptable for:** Slowly changing integer sequences where consecutive
//! deltas are small (e.g., incrementing counters). Compression will still
//! be good as long as the delta-of-delta fits in the compact bit buckets.
//!
//! **Suboptimal for:** Non-monotonic or highly variable data such as
//! measured float values, gauge metrics, or random integers. Large
//! swings produce large deltas-of-deltas that fall into the 68-bit
//! fallback bucket, resulting in *worse* compression than raw storage
//! (68 bits vs. 64 bits per value, plus header overhead).
//!
//! **Prefer instead:**
//! - [`Gorilla`](crate::gorilla) / [`Chimp`](crate::chimp) XOR encoding
//!   for floating-point value columns — they exploit the bitwise
//!   similarity of consecutive IEEE 754 values.
//! - [`RLE`](crate::rle) for columns with many consecutive repeated
//!   values (e.g., status codes, boolean flags).
//! - [`IntegerEncoder`](crate::integer) (delta + ZigZag) for general
//!   integer columns that may not be monotonic.
//!
//! The [`AdaptiveSelector`](crate::adaptive::AdaptiveSelector) in the
//! unified encoder automatically samples data and picks the best
//! encoding, so callers using [`ColumnEncoder`](crate::unified::ColumnEncoder)
//! do not need to make this choice manually.
//!
//! ## Encoding scheme
//!
//! | Delta-of-delta value     | Prefix bits | Value bits | Total bits |
//! |--------------------------|-------------|------------|------------|
//! | 0                        | `0`         | 0          | 1          |
//! | −064..=63                 | `10`        | 7          | 9          |
//! | −256..=255               | `110`       | 9          | 12         |
//! | −2048..=2047             | `1110`      | 12         | 16         |
//! | anything else            | `1111`      | 64         | 68         |

use crate::coding::{checked_count, checked_decode_count, zigzag_decode, zigzag_encode};
use crate::error::{EncodingError, Result};

/// Encodes a sorted sequence of `i64` timestamps using delta-of-delta encoding.
#[derive(Debug, Clone, Copy)]
pub struct DeltaOfDeltaEncoder;

/// Decodes delta-of-delta encoded timestamps.
#[derive(Debug, Clone, Copy)]
pub struct DeltaOfDeltaDecoder;

impl DeltaOfDeltaEncoder {
    /// Encode a sequence of timestamps.
    ///
    /// # Overflow safety
    ///
    /// All delta and delta-of-delta computations use **wrapping arithmetic**
    /// (`wrapping_sub`).  This is intentional: the encoding scheme stores
    /// every delta-of-delta with a 64-bit raw fallback, so any `i64`
    /// difference — including values that wrap — is captured losslessly
    /// and decoded symmetrically via `wrapping_add`.  Plain `-` / `+`
    /// operators are avoided to prevent panics in debug builds.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `timestamps` is empty.
    pub fn encode(timestamps: &[i64]) -> Result<Vec<u8>> {
        if timestamps.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "delta-of-delta encoder",
            });
        }

        // Pre-allocate: worst case ~9 bytes/value, but typically ~1 bit/value
        let mut writer = BitWriter::with_capacity(8 + timestamps.len());

        // Store value count as u32
        let count = checked_count(timestamps.len())?;
        writer.write_bits(u64::from(count), 32);

        // First value: raw i64
        writer.write_bits(timestamps[0] as u64, 64);

        if timestamps.len() == 1 {
            return Ok(writer.finish());
        }

        // First delta: raw i64 (wrapping to handle extreme values)
        let first_delta = timestamps[1].wrapping_sub(timestamps[0]);
        writer.write_bits(first_delta as u64, 64);

        let mut prev_delta = first_delta;

        for i in 2..timestamps.len() {
            // wrapping arithmetic: any i64 difference is valid because the
            // 64-bit raw fallback bucket captures arbitrary wrapping results.
            let delta = timestamps[i].wrapping_sub(timestamps[i - 1]);
            let dod = delta.wrapping_sub(prev_delta);
            encode_dod(&mut writer, dod);
            prev_delta = delta;
        }

        Ok(writer.finish())
    }
}

fn encode_dod(writer: &mut BitWriter, dod: i64) {
    match dod {
        0 => {
            writer.write_bit(false);
        }
        -64..=63 => {
            writer.write_bits(0b10, 2);
            // ZigZag encode to fit in 7 bits unsigned
            // zigzag(-64)=127, zigzag(63)=126 — both fit in 7 bits
            writer.write_bits(zigzag_encode(dod), 7);
        }
        -256..=255 => {
            writer.write_bits(0b110, 3);
            // zigzag(-256)=511, zigzag(255)=510 — both fit in 9 bits
            writer.write_bits(zigzag_encode(dod), 9);
        }
        -2048..=2047 => {
            writer.write_bits(0b1110, 4);
            // zigzag(-2048)=4095, zigzag(2047)=4094 — both fit in 12 bits
            writer.write_bits(zigzag_encode(dod), 12);
        }
        _ => {
            writer.write_bits(0b1111, 4);
            writer.write_bits(dod as u64, 64);
        }
    }
}

impl DeltaOfDeltaDecoder {
    /// Decode a delta-of-delta encoded byte buffer back to timestamps.
    ///
    /// # Overflow safety
    ///
    /// Reconstruction uses **wrapping arithmetic** (`wrapping_add`) to
    /// mirror the `wrapping_sub` used during encoding.  This guarantees
    /// correct round-trip for any `i64` sequence and provides robustness
    /// against corrupt data — the decoder never panics on arithmetic.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is malformed.
    pub fn decode(data: &[u8]) -> Result<Vec<i64>> {
        if data.len() < 4 {
            return Err(EncodingError::CorruptData {
                detail: "delta-of-delta data too short for header".to_string(),
            });
        }

        let mut reader = BitReader::new(data);

        let count = checked_decode_count(reader.read_bits(32)? as u32 as usize, "delta")?;
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(count);

        // First value
        let first = reader.read_bits(64)? as i64;
        result.push(first);

        if count == 1 {
            return Ok(result);
        }

        // First delta (wrapping to handle extreme values)
        let first_delta = reader.read_bits(64)? as i64;
        result.push(first.wrapping_add(first_delta));

        let mut prev_delta = first_delta;

        for _ in 2..count {
            let dod = decode_dod(&mut reader)?;
            // wrapping: mirrors wrapping_sub used during encoding
            let delta = prev_delta.wrapping_add(dod);
            let value = result[result.len() - 1].wrapping_add(delta);
            result.push(value);
            prev_delta = delta;
        }

        Ok(result)
    }
}

fn decode_dod(reader: &mut BitReader) -> Result<i64> {
    if !reader.read_bit()? {
        // 0 prefix → dod = 0
        return Ok(0);
    }
    if !reader.read_bit()? {
        // 10 prefix → 7-bit zigzag
        let z = reader.read_bits(7)?;
        return Ok(zigzag_decode(z));
    }
    if !reader.read_bit()? {
        // 110 prefix → 9-bit zigzag
        let z = reader.read_bits(9)?;
        return Ok(zigzag_decode(z));
    }
    if !reader.read_bit()? {
        // 1110 prefix → 12-bit zigzag
        let z = reader.read_bits(12)?;
        return Ok(zigzag_decode(z));
    }
    // 1111 prefix → raw 64-bit
    let v = reader.read_bits(64)? as i64;
    Ok(v)
}

// ── Bit-level I/O ──────────────────────────────────────────────────────

/// A bit-level writer that packs bits MSB-first into a byte buffer.
///
/// Uses a `u64` word accumulator to batch bit operations — `write_bits()`
/// performs at most two word-level operations per call instead of per-bit
/// function calls. This eliminates the primary encoding bottleneck for
/// all variable-length encoders (delta-of-delta, Chimp, Gorilla).
///
/// Output is byte-identical to a naive per-bit writer (MSB-first within
/// each byte, big-endian word flushing).
pub(crate) struct BitWriter {
    buf: Vec<u8>,
    /// Accumulator where bits are left-aligned (MSB-first, starting at bit 63).
    accumulator: u64,
    /// Number of valid bits in the accumulator (0..64).
    bits_in_acc: u8,
}

impl BitWriter {
    /// Create a new `BitWriter` with the given byte capacity hint.
    pub(crate) fn with_capacity(bytes: usize) -> Self {
        Self {
            buf: Vec::with_capacity(bytes),
            accumulator: 0,
            bits_in_acc: 0,
        }
    }

    /// Flush the full 64-bit accumulator as 8 big-endian bytes.
    #[inline]
    fn flush_accumulator(&mut self) {
        self.buf.extend_from_slice(&self.accumulator.to_be_bytes());
        self.accumulator = 0;
        self.bits_in_acc = 0;
    }

    /// Write a single bit (MSB-first ordering).
    #[inline]
    pub(crate) fn write_bit(&mut self, bit: bool) {
        if bit {
            self.accumulator |= 1u64 << (63 - self.bits_in_acc);
        }
        self.bits_in_acc += 1;
        if self.bits_in_acc == 64 {
            self.flush_accumulator();
        }
    }

    /// Write `count` bits from the least-significant bits of `value`.
    ///
    /// Uses word-level operations to pack bits in at most two steps
    /// (fill current word + start next word), regardless of `count`.
    #[inline]
    pub(crate) fn write_bits(&mut self, value: u64, count: u8) {
        debug_assert!(count <= 64);
        if count == 0 {
            return;
        }

        // Mask to exactly `count` least-significant bits
        let masked = if count >= 64 {
            value
        } else {
            value & ((1u64 << count) - 1)
        };

        let space = 64 - self.bits_in_acc;
        if count <= space {
            // All bits fit in the current accumulator word
            self.accumulator |= masked << (space - count);
            self.bits_in_acc += count;
            if self.bits_in_acc == 64 {
                self.flush_accumulator();
            }
        } else {
            // Split across word boundary: fill current word, start next
            self.accumulator |= masked >> (count - space);
            self.flush_accumulator();
            let remaining = count - space;
            self.accumulator = masked << (64 - remaining);
            self.bits_in_acc = remaining;
        }
    }

    /// Flush remaining bits and return the completed byte buffer.
    pub(crate) fn finish(mut self) -> Vec<u8> {
        if self.bits_in_acc > 0 {
            let bytes_needed = self.bits_in_acc.div_ceil(8) as usize;
            let word_bytes = self.accumulator.to_be_bytes();
            self.buf.extend_from_slice(&word_bytes[..bytes_needed]);
        }
        self.buf
    }
}

/// A bit-level reader that reads bits MSB-first from a byte slice.
///
/// `read_bits()` processes a byte-at-a-time instead of bit-at-a-time,
/// reducing iterations from `count` to at most `ceil(count/8) + 1` per
/// call. For a 64-bit read this means ≤ 9 iterations instead of 64.
pub(crate) struct BitReader<'a> {
    data: &'a [u8],
    byte_pos: usize,
    bit_pos: u8, // 0..8
}

impl<'a> BitReader<'a> {
    /// Create a new `BitReader` over the given byte slice.
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            byte_pos: 0,
            bit_pos: 0,
        }
    }

    /// Read a single bit.
    #[inline]
    pub(crate) fn read_bit(&mut self) -> Result<bool> {
        if self.byte_pos >= self.data.len() {
            return Err(EncodingError::CorruptData {
                detail: "unexpected end of bit stream".to_string(),
            });
        }
        let bit = (self.data[self.byte_pos] >> (7 - self.bit_pos)) & 1 == 1;
        self.bit_pos += 1;
        if self.bit_pos == 8 {
            self.byte_pos += 1;
            self.bit_pos = 0;
        }
        Ok(bit)
    }

    /// Read `count` bits into the least-significant bits of a `u64`.
    ///
    /// Processes a full byte chunk per iteration instead of one bit at a
    /// time, yielding up to 8× fewer loop iterations for multi-bit reads.
    #[inline]
    pub(crate) fn read_bits(&mut self, count: u8) -> Result<u64> {
        debug_assert!(count <= 64);
        if count == 0 {
            return Ok(0);
        }

        let mut value: u64 = 0;
        let mut remaining = count;

        while remaining > 0 {
            if self.byte_pos >= self.data.len() {
                return Err(EncodingError::CorruptData {
                    detail: "unexpected end of bit stream".to_string(),
                });
            }

            let bits_left_in_byte = 8 - self.bit_pos;
            let to_read = remaining.min(bits_left_in_byte);

            // Extract `to_read` bits from the current byte at bit_pos
            let shift = bits_left_in_byte - to_read;
            let mask: u8 = if to_read >= 8 {
                0xFF
            } else {
                (1u8 << to_read) - 1
            };
            let bits = (self.data[self.byte_pos] >> shift) & mask;

            value = (value << to_read) | u64::from(bits);
            remaining -= to_read;
            self.bit_pos += to_read;

            if self.bit_pos >= 8 {
                self.byte_pos += 1;
                self.bit_pos = 0;
            }
        }

        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_monotonic_1s() {
        let ts: Vec<i64> = (0..1000)
            .map(|i| 1_000_000_000 + i * 1_000_000_000)
            .collect();
        let encoded = DeltaOfDeltaEncoder::encode(&ts).unwrap();
        let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
        assert_eq!(ts, decoded);
        // With constant delta, most dod=0 → ~1 bit each → very compact
        assert!(encoded.len() < ts.len()); // much better than 8 bytes/value
    }

    #[test]
    fn roundtrip_single_value() {
        let ts = vec![42_i64];
        let encoded = DeltaOfDeltaEncoder::encode(&ts).unwrap();
        let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
        assert_eq!(ts, decoded);
    }

    #[test]
    fn roundtrip_two_values() {
        let ts = vec![100_i64, 200];
        let encoded = DeltaOfDeltaEncoder::encode(&ts).unwrap();
        let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
        assert_eq!(ts, decoded);
    }

    #[test]
    fn roundtrip_constant_timestamps() {
        let ts = vec![42_i64; 100];
        let encoded = DeltaOfDeltaEncoder::encode(&ts).unwrap();
        let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
        assert_eq!(ts, decoded);
    }

    #[test]
    fn roundtrip_irregular_intervals() {
        let ts = vec![0, 1, 5, 10, 50, 100, 500, 1000, 2000];
        let encoded = DeltaOfDeltaEncoder::encode(&ts).unwrap();
        let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
        assert_eq!(ts, decoded);
    }

    #[test]
    fn roundtrip_negative_timestamps() {
        let ts = vec![-1000, -500, -200, -100, 0, 100, 200];
        let encoded = DeltaOfDeltaEncoder::encode(&ts).unwrap();
        let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
        assert_eq!(ts, decoded);
    }

    #[test]
    fn roundtrip_edge_i64() {
        let ts = vec![i64::MIN, i64::MIN / 2, 0, i64::MAX / 2, i64::MAX];
        let encoded = DeltaOfDeltaEncoder::encode(&ts).unwrap();
        let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
        assert_eq!(ts, decoded);
    }

    #[test]
    fn empty_input_error() {
        let result = DeltaOfDeltaEncoder::encode(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn compression_ratio_regular() {
        // 10_000 timestamps at 1s intervals
        let ts: Vec<i64> = (0..10_000).map(|i| i * 1_000_000_000).collect();
        let raw_size = ts.len() * 8;
        let encoded = DeltaOfDeltaEncoder::encode(&ts).unwrap();
        #[allow(clippy::cast_precision_loss)]
        let ratio = raw_size as f64 / encoded.len() as f64;
        // Should achieve excellent compression on regular data
        assert!(
            ratio > 10.0,
            "Expected ratio > 10x, got {ratio:.1}x ({} raw, {} encoded)",
            raw_size,
            encoded.len()
        );
    }

    /// Regression test for the `DoD` boundary bug: values at the exact
    /// `ZigZag` bit-width boundaries must roundtrip correctly.
    /// Previously, ranges were `-63..=64`, `-255..=256`, `-2047..=2048`
    /// which silently truncated the positive boundary values.
    #[test]
    fn roundtrip_dod_boundary_values() {
        // DoD of exactly 64 (was corrupted to 0)
        let ts_64 = vec![0_i64, 0, 64];
        let encoded = DeltaOfDeltaEncoder::encode(&ts_64).unwrap();
        let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
        assert_eq!(ts_64, decoded, "DoD=64 boundary failed");

        // DoD of exactly -64
        let ts_neg64 = vec![0_i64, 64, 0];
        let encoded = DeltaOfDeltaEncoder::encode(&ts_neg64).unwrap();
        let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
        assert_eq!(ts_neg64, decoded, "DoD=-64 boundary failed");

        // DoD of exactly 63 (last value that fits in 7-bit zigzag)
        let ts_63 = vec![0_i64, 0, 63];
        let encoded = DeltaOfDeltaEncoder::encode(&ts_63).unwrap();
        let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
        assert_eq!(ts_63, decoded, "DoD=63 boundary failed");

        // DoD of exactly 256 (was corrupted to 0)
        let ts_256 = vec![0_i64, 0, 256];
        let encoded = DeltaOfDeltaEncoder::encode(&ts_256).unwrap();
        let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
        assert_eq!(ts_256, decoded, "DoD=256 boundary failed");

        // DoD of exactly -256
        let ts_neg256 = vec![0_i64, 256, 0];
        let encoded = DeltaOfDeltaEncoder::encode(&ts_neg256).unwrap();
        let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
        assert_eq!(ts_neg256, decoded, "DoD=-256 boundary failed");

        // DoD of exactly 255
        let ts_255 = vec![0_i64, 0, 255];
        let encoded = DeltaOfDeltaEncoder::encode(&ts_255).unwrap();
        let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
        assert_eq!(ts_255, decoded, "DoD=255 boundary failed");

        // DoD of exactly 2048 (was corrupted to 0)
        let ts_2048 = vec![0_i64, 0, 2048];
        let encoded = DeltaOfDeltaEncoder::encode(&ts_2048).unwrap();
        let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
        assert_eq!(ts_2048, decoded, "DoD=2048 boundary failed");

        // DoD of exactly -2048
        let ts_neg2048 = vec![0_i64, 2048, 0];
        let encoded = DeltaOfDeltaEncoder::encode(&ts_neg2048).unwrap();
        let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
        assert_eq!(ts_neg2048, decoded, "DoD=-2048 boundary failed");

        // DoD of exactly 2047
        let ts_2047 = vec![0_i64, 0, 2047];
        let encoded = DeltaOfDeltaEncoder::encode(&ts_2047).unwrap();
        let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
        assert_eq!(ts_2047, decoded, "DoD=2047 boundary failed");
    }

    /// Verify that values just outside the compact encoding ranges also
    /// roundtrip correctly (they use the next-wider encoding bucket).
    #[test]
    fn roundtrip_dod_just_outside_boundaries() {
        // 65 → should use the 9-bit bucket
        let ts = vec![0_i64, 0, 65];
        let encoded = DeltaOfDeltaEncoder::encode(&ts).unwrap();
        let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
        assert_eq!(ts, decoded);

        // 257 → should use the 12-bit bucket
        let ts = vec![0_i64, 0, 257];
        let encoded = DeltaOfDeltaEncoder::encode(&ts).unwrap();
        let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
        assert_eq!(ts, decoded);

        // 2049 → should use the 64-bit raw bucket
        let ts = vec![0_i64, 0, 2049];
        let encoded = DeltaOfDeltaEncoder::encode(&ts).unwrap();
        let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
        assert_eq!(ts, decoded);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip_arbitrary_sorted(mut values in proptest::collection::vec(any::<i64>(), 1..500)) {
            values.sort_unstable();
            let encoded = DeltaOfDeltaEncoder::encode(&values).unwrap();
            let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
            prop_assert_eq!(&values, &decoded);
        }

        #[test]
        fn roundtrip_arbitrary_unsorted(values in proptest::collection::vec(any::<i64>(), 1..500)) {
            // Works on unsorted data too (just less compressible)
            let encoded = DeltaOfDeltaEncoder::encode(&values).unwrap();
            let decoded = DeltaOfDeltaDecoder::decode(&encoded).unwrap();
            prop_assert_eq!(&values, &decoded);
        }
    }
}
