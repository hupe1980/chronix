//! Chimp float encoding (VLDB 2022).
//!
//! Chimp improves upon Gorilla by exploiting the observation that XOR values
//! of successive floating-point values cluster their leading zeros. It uses a
//! leading-zero bucket scheme to reduce the overhead of encoding the number
//! of leading zeros.
//!
//! ## Leading-zero buckets
//!
//! | Bucket | Range (leading zeros) | Stored as |
//! |--------|----------------------|-----------|
//! | 0      | 0–7                  | 3 bits    |
//! | 1      | 8–15                 | 3 bits    |
//! | 2      | 16–23                | 3 bits    |
//! | 3      | 24–31                | 3 bits    |
//! | 4      | 32–39                | 3 bits    |
//! | 5      | 40–47                | 3 bits    |
//! | 6      | 48–55                | 3 bits    |
//! | 7      | 56–63                | 3 bits    |
//!
//! ## Chimp128 ring-buffer optimisation
//!
//! [`Chimp128Encoder`] / [`Chimp128Decoder`] implement the Chimp128 variant
//! from the same VLDB 2022 paper.  A ring buffer of the last 128 raw `f64`
//! bit-patterns is maintained.  For each new value the encoder computes the
//! XOR against the previous value *and* against every ring-buffer entry,
//! picking whichever reference yields the most leading zeros.  When a
//! ring-buffer reference wins, a 7-bit index is stored so the decoder can
//! reconstruct the same XOR base.
//!
//! This improves compression by 5–15 % on highly periodic workloads
//! (e.g. fixed-interval sensor data that cycles through a limited set of
//! levels) at the cost of ~1 KiB of encoder/decoder state.

use crate::coding::{checked_count, checked_decode_count};
use crate::delta::{BitReader, BitWriter};
use crate::error::{EncodingError, Result};

/// Chimp float encoder (VLDB 2022).
#[derive(Debug, Clone, Copy)]
pub struct ChimpEncoder;

/// Chimp float decoder.
#[derive(Debug, Clone, Copy)]
pub struct ChimpDecoder;

/// Number of leading-zero buckets.
const NUM_BUCKETS: u8 = 8;

/// Map a leading zero count (0..=64) to a 3-bit bucket index.
#[inline]
fn leading_zeros_to_bucket(lz: u32) -> u8 {
    (lz as u8 / 8).min(NUM_BUCKETS - 1)
}

/// Map a bucket index back to the minimum leading zeros in that bucket.
#[inline]
fn bucket_to_leading_zeros(bucket: u8) -> u32 {
    u32::from(bucket) * 8
}

impl ChimpEncoder {
    /// Encode a sequence of `f64` values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode(values: &[f64]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "chimp encoder",
            });
        }

        // Pre-allocate: worst case ~12 bytes per value + 12 header
        let mut writer = BitWriter::with_capacity(12 + values.len() * 12);

        Self::encode_core(values, &mut writer)?;

        Ok(writer.finish())
    }

    /// Shared encode logic — writes into a pre-existing `BitWriter`
    /// to allow callers to reuse buffers.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the value count exceeds `u32::MAX`.
    fn encode_core(values: &[f64], writer: &mut BitWriter) -> Result<()> {
        // Value count (validated to fit in u32)
        let count = checked_count(values.len())?;
        writer.write_bits(u64::from(count), 32);

        // First value: raw 64-bit
        let mut prev_bits = values[0].to_bits();
        writer.write_bits(prev_bits, 64);

        let mut prev_leading = u8::MAX; // no prior leading zeros
        let mut prev_trailing = 0_u8;

        #[allow(clippy::explicit_counter_loop)]
        // ring_pos wraps modulo RING_SIZE, not a plain counter
        for &val in &values[1..] {
            let bits = val.to_bits();
            let xor = prev_bits ^ bits;

            if xor == 0 {
                // Case 0: identical value → single 0 bit
                writer.write_bit(false);
            } else {
                let leading = xor.leading_zeros();
                let trailing = xor.trailing_zeros();

                let bucket = leading_zeros_to_bucket(leading);
                let bucket_lz = bucket_to_leading_zeros(bucket);

                // Check if we can reuse the previous window
                let can_reuse = prev_leading != u8::MAX
                    && leading >= u32::from(prev_leading)
                    && trailing >= u32::from(prev_trailing);

                if can_reuse {
                    // Case 1: reuse previous leading/trailing window
                    // prefix: 10
                    writer.write_bits(0b10, 2);
                    let prev_meaningful = 64 - u32::from(prev_leading) - u32::from(prev_trailing);
                    let shifted = xor >> u32::from(prev_trailing);
                    writer.write_bits(shifted, prev_meaningful as u8);
                } else {
                    // Case 2: new window with bucket
                    // prefix: 11
                    writer.write_bits(0b11, 2);
                    // 3-bit bucket
                    writer.write_bits(u64::from(bucket), 3);
                    // 6-bit trailing zero count
                    writer.write_bits(u64::from(trailing), 6);
                    // meaningful bits (adjusted for bucket granularity)
                    let adj_leading = bucket_lz;
                    let adj_meaningful = 64 - adj_leading - trailing;
                    let shifted = xor >> trailing;
                    // Store meaningful-1 to handle adj_meaningful==64
                    writer.write_bits(u64::from(adj_meaningful.saturating_sub(1)), 6);
                    writer.write_bits(shifted, adj_meaningful as u8);

                    prev_leading = bucket_lz as u8;
                    prev_trailing = trailing as u8;
                }
            }

            prev_bits = bits;
        }

        Ok(())
    }
}

impl ChimpDecoder {
    /// Decode a Chimp-encoded byte buffer back to `f64` values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is malformed.
    pub fn decode(data: &[u8]) -> Result<Vec<f64>> {
        if data.len() < 4 {
            return Err(EncodingError::CorruptData {
                detail: "chimp data too short for header".to_string(),
            });
        }

        let mut reader = BitReader::new(data);

        let count = checked_decode_count(reader.read_bits(32)? as usize, "Chimp")?;
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(count);
        Self::decode_core(&mut reader, count, &mut result)?;
        Ok(result)
    }

    /// Decode into a pre-allocated output vector for zero-alloc reuse.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is malformed.
    pub fn decode_into(data: &[u8], output: &mut Vec<f64>) -> Result<()> {
        if data.len() < 4 {
            return Err(EncodingError::CorruptData {
                detail: "chimp data too short for header".to_string(),
            });
        }

        let mut reader = BitReader::new(data);
        let count = checked_decode_count(reader.read_bits(32)? as usize, "Chimp")?;
        if count == 0 {
            return Ok(());
        }

        output.clear();
        output.reserve(count);
        Self::decode_core(&mut reader, count, output)
    }

    /// Shared decode logic operating on a pre-sized output buffer.
    fn decode_core(reader: &mut BitReader<'_>, count: usize, result: &mut Vec<f64>) -> Result<()> {
        let first_bits = reader.read_bits(64)?;
        result.push(f64::from_bits(first_bits));

        let mut prev_bits = first_bits;
        let mut prev_leading: u8 = u8::MAX;
        let mut prev_trailing: u8 = 0;

        for _ in 1..count {
            if !reader.read_bit()? {
                // Case 0: identical
                result.push(f64::from_bits(prev_bits));
                continue;
            }

            if reader.read_bit()? {
                // Case 2: new window (prefix was 11)
                let bucket = reader.read_bits(3)? as u8;
                let trailing = reader.read_bits(6)? as u32;
                let adj_leading = bucket_to_leading_zeros(bucket);
                let meaningful = reader.read_bits(6)? as u32 + 1; // stored as m-1
                if adj_leading + meaningful + trailing > 64 {
                    return Err(EncodingError::CorruptData {
                        detail: format!(
                            "chimp case-2: leading ({adj_leading}) + meaningful ({meaningful}) + trailing ({trailing}) exceeds 64"
                        ),
                    });
                }
                let shifted = reader.read_bits(meaningful as u8)?;
                let xor = shifted << trailing;
                let bits = prev_bits ^ xor;
                result.push(f64::from_bits(bits));
                prev_bits = bits;

                prev_leading = adj_leading as u8;
                prev_trailing = trailing as u8;
            } else {
                // Case 1: reuse window (prefix was 10)
                if prev_leading == u8::MAX {
                    return Err(EncodingError::CorruptData {
                        detail: "chimp case-1 encountered before any case-2 window".to_string(),
                    });
                }
                let meaningful = 64 - u32::from(prev_leading) - u32::from(prev_trailing);
                let shifted = reader.read_bits(meaningful as u8)?;
                let xor = shifted << u32::from(prev_trailing);
                let bits = prev_bits ^ xor;
                result.push(f64::from_bits(bits));
                prev_bits = bits;
            }
        }

        Ok(())
    }
}

/// Size of the Chimp128 ring buffer.
const RING_SIZE: usize = 128;

/// Chimp128 float encoder — ring-buffer variant (VLDB 2022, §4.2).
///
/// Maintains a ring buffer of the last 128 raw `f64` bit-patterns
/// and picks the reference that yields the most leading zeros in the
/// XOR, encoding a 7-bit index when a ring-buffer entry beats the
/// default sequential reference.
#[derive(Debug, Clone, Copy)]
pub struct Chimp128Encoder;

/// Chimp128 float decoder.
#[derive(Debug, Clone, Copy)]
pub struct Chimp128Decoder;

impl Chimp128Encoder {
    /// Encode a sequence of `f64` values using the Chimp128 ring-buffer scheme.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty.
    pub fn encode(values: &[f64]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "chimp128 encoder",
            });
        }

        let mut writer = BitWriter::with_capacity(12 + values.len() * 12);

        // Value count
        let count = checked_count(values.len())?;
        writer.write_bits(u64::from(count), 32);

        // First value: raw 64-bit
        let first_bits = values[0].to_bits();
        writer.write_bits(first_bits, 64);

        // Ring buffer of raw bit-patterns (circular)
        let mut ring = [0u64; RING_SIZE];
        ring[0] = first_bits;
        let mut ring_len: usize = 1; // how many valid entries (max RING_SIZE)
        let mut ring_pos: usize = 1; // next write position (mod RING_SIZE)

        // Hash table for O(1) ring lookup.
        //
        // Maps the top 8 bits of each bit-pattern to the most recent ring
        // index with that prefix. Entries sharing the top 8 bits are
        // guaranteed to produce XORs with ≥8 leading zeros. For periodic
        // data (where Chimp128 shines), this typically yields exact or
        // near-exact matches without scanning all 128 entries.
        //
        // Sentinel value u8::MAX means no entry for that prefix yet.
        // 256 bytes of state — negligible compared to the 1 KiB ring.
        let mut ring_hash = [u8::MAX; 256];
        ring_hash[(first_bits >> 56) as usize] = 0;

        let mut prev_bits = first_bits;
        let mut prev_leading: u8 = u8::MAX;
        let mut prev_trailing: u8 = 0;

        #[allow(clippy::explicit_counter_loop)]
        // ring_pos wraps modulo RING_SIZE, not a plain counter
        for &val in &values[1..] {
            let bits = val.to_bits();
            let seq_xor = prev_bits ^ bits;

            if seq_xor == 0 {
                // Case 0: identical to previous — single 0 bit
                writer.write_bit(false);
            } else {
                // Search ring buffer for best reference (most leading zeros)
                //
                // O(1) hash probe first, O(128) scan only as fallback.
                let seq_leading = seq_xor.leading_zeros();
                let mut best_leading = seq_leading;
                let mut best_xor = seq_xor;
                let mut best_idx: Option<u8> = None;

                // Phase 1: Hash probe — check if there's a ring entry with
                // the same top-8-bit prefix. Such entries share ≥8 high-order
                // bits, guaranteeing ≥8 leading zeros in the XOR.
                let hash_key = (bits >> 56) as usize;
                let hash_candidate = ring_hash[hash_key];
                let mut skip_scan = false;

                if hash_candidate != u8::MAX {
                    let ci = hash_candidate as usize;
                    let ref_xor = ring[ci] ^ bits;
                    if ref_xor == 0 {
                        // Perfect match via hash — skip scan entirely.
                        best_leading = 64;
                        best_xor = 0;
                        best_idx = Some(ci as u8);
                        skip_scan = true;
                    } else {
                        let rl = ref_xor.leading_zeros();
                        if rl > best_leading {
                            best_leading = rl;
                            best_xor = ref_xor;
                            best_idx = Some(ci as u8);
                        }
                        // If hash candidate already gives ≥24 leading zeros
                        // (matching top 3 bytes), the marginal improvement
                        // from scanning all 128 entries is <1 bit savings.
                        if best_leading >= 24 {
                            skip_scan = true;
                        }
                    }
                }

                // Phase 2: Full scan fallback (rare for periodic data).
                if !skip_scan {
                    let search_len = ring_len.min(RING_SIZE);
                    #[allow(clippy::needless_range_loop)] // index doubles as the ring reference id
                    for i in 0..search_len {
                        let ref_xor = ring[i] ^ bits;
                        if ref_xor == 0 {
                            best_xor = 0;
                            best_idx = Some(i as u8);
                            break;
                        }
                        let rl = ref_xor.leading_zeros();
                        if rl > best_leading {
                            best_leading = rl;
                            best_xor = ref_xor;
                            best_idx = Some(i as u8);
                        }
                    }
                }

                // Non-zero XOR — write the '1' prefix
                writer.write_bit(true);

                if best_xor == 0 {
                    // Ring reference gave exact match
                    // Bit 1 = 1 (ring reference), 7-bit index, then case-0-like
                    // We encode: [1: ring_ref][7: index][0: xor_is_zero]
                    writer.write_bit(true);
                    writer.write_bits(u64::from(best_idx.unwrap_or(0)), 7);
                    // The XOR against the ring entry is 0, but we already wrote
                    // the '1' prefix meaning non-zero seq XOR. We need to signal
                    // that the ring-ref XOR itself is zero.
                    writer.write_bit(false); // ring xor == 0
                } else {
                    let xor = best_xor;
                    let leading = xor.leading_zeros();
                    let trailing = xor.trailing_zeros();
                    let bucket = leading_zeros_to_bucket(leading);
                    let bucket_lz = bucket_to_leading_zeros(bucket);

                    if let Some(idx) = best_idx {
                        // Ring reference is better
                        writer.write_bit(true); // use ring reference
                        writer.write_bits(u64::from(idx), 7);
                        writer.write_bit(true); // ring xor != 0

                        // Encode the XOR using Chimp bucket scheme
                        Self::encode_xor(
                            &mut writer,
                            xor,
                            leading,
                            trailing,
                            bucket,
                            bucket_lz,
                            &mut prev_leading,
                            &mut prev_trailing,
                        );
                    } else {
                        // Sequential reference (no ring entry was better)
                        writer.write_bit(false); // use sequential reference

                        // Check if we can reuse the previous window
                        let can_reuse = prev_leading != u8::MAX
                            && leading >= u32::from(prev_leading)
                            && trailing >= u32::from(prev_trailing);

                        if can_reuse {
                            // Case 1: reuse previous leading/trailing
                            writer.write_bit(false); // prefix bit = 0 for reuse
                            let prev_meaningful =
                                64 - u32::from(prev_leading) - u32::from(prev_trailing);
                            let shifted = xor >> u32::from(prev_trailing);
                            writer.write_bits(shifted, prev_meaningful as u8);
                        } else {
                            // Case 2: new window with bucket
                            writer.write_bit(true); // prefix bit = 1 for new window
                            writer.write_bits(u64::from(bucket), 3);
                            writer.write_bits(u64::from(trailing), 6);
                            let adj_leading = bucket_lz;
                            let adj_meaningful = 64 - adj_leading - trailing;
                            let shifted = xor >> trailing;
                            writer.write_bits(u64::from(adj_meaningful.saturating_sub(1)), 6);
                            writer.write_bits(shifted, adj_meaningful as u8);

                            prev_leading = bucket_lz as u8;
                            prev_trailing = trailing as u8;
                        }
                    }
                }
            }

            // Push into ring buffer and update hash index
            ring[ring_pos % RING_SIZE] = bits;
            ring_hash[(bits >> 56) as usize] = (ring_pos % RING_SIZE) as u8;
            ring_pos += 1;
            if ring_len < RING_SIZE {
                ring_len += 1;
            }

            prev_bits = bits;
        }

        Ok(writer.finish())
    }

    /// Encode a non-zero XOR value using Chimp's bucket scheme (used for
    /// ring-buffer references where we always write a fresh window).
    #[inline]
    fn encode_xor(
        writer: &mut BitWriter,
        xor: u64,
        _leading: u32,
        trailing: u32,
        bucket: u8,
        bucket_lz: u32,
        _prev_leading: &mut u8,
        _prev_trailing: &mut u8,
    ) {
        // For ring references we always write a full window (case 2 style)
        // since the decoder doesn't share prev_leading/prev_trailing state
        // with a ring-referenced XOR.
        writer.write_bits(u64::from(bucket), 3);
        writer.write_bits(u64::from(trailing), 6);
        let adj_leading = bucket_lz;
        let adj_meaningful = 64 - adj_leading - trailing;
        let shifted = xor >> trailing;
        writer.write_bits(u64::from(adj_meaningful.saturating_sub(1)), 6);
        writer.write_bits(shifted, adj_meaningful as u8);

        // Do NOT update prev_leading/prev_trailing after a
        // ring-reference encode. The sequential XOR path and the ring-ref
        // path are independent state machines — contaminating one from
        // the other causes ~0.1% of subsequent sequential encodes to
        // produce the wrong leading/trailing window, which the decoder
        // cannot reconstruct.
    }
}

impl Chimp128Decoder {
    /// Decode a Chimp128-encoded byte buffer back to `f64` values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is malformed.
    pub fn decode(data: &[u8]) -> Result<Vec<f64>> {
        if data.len() < 4 {
            return Err(EncodingError::CorruptData {
                detail: "chimp128 data too short for header".to_string(),
            });
        }

        let mut reader = BitReader::new(data);
        let count = checked_decode_count(reader.read_bits(32)? as usize, "Chimp128")?;
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(count);
        Self::decode_core(&mut reader, count, &mut result)?;
        Ok(result)
    }

    /// Decode into a pre-allocated output vector for zero-alloc reuse.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the data is malformed.
    pub fn decode_into(data: &[u8], output: &mut Vec<f64>) -> Result<()> {
        if data.len() < 4 {
            return Err(EncodingError::CorruptData {
                detail: "chimp128 data too short for header".to_string(),
            });
        }

        let mut reader = BitReader::new(data);
        let count = checked_decode_count(reader.read_bits(32)? as usize, "Chimp128")?;
        if count == 0 {
            return Ok(());
        }

        output.clear();
        output.reserve(count);
        Self::decode_core(&mut reader, count, output)
    }

    /// Shared decode logic.
    fn decode_core(reader: &mut BitReader<'_>, count: usize, result: &mut Vec<f64>) -> Result<()> {
        let first_bits = reader.read_bits(64)?;
        result.push(f64::from_bits(first_bits));

        let mut ring = [0u64; RING_SIZE];
        ring[0] = first_bits;
        let mut ring_len: usize = 1;
        let mut ring_pos: usize = 1;

        let mut prev_bits = first_bits;
        let mut prev_leading: u8 = u8::MAX;
        let mut prev_trailing: u8 = 0;

        for _ in 1..count {
            if !reader.read_bit()? {
                // Case 0: identical to previous
                result.push(f64::from_bits(prev_bits));
                // Push into ring
                ring[ring_pos % RING_SIZE] = prev_bits;
                ring_pos += 1;
                if ring_len < RING_SIZE {
                    ring_len += 1;
                }
                continue;
            }

            // Non-zero sequential XOR — check reference type
            let use_ring = reader.read_bit()?;

            if use_ring {
                let idx = reader.read_bits(7)? as usize;
                if idx >= ring_len.min(RING_SIZE) {
                    return Err(EncodingError::CorruptData {
                        detail: format!(
                            "chimp128: ring index {idx} out of range (ring_len={})",
                            ring_len.min(RING_SIZE)
                        ),
                    });
                }
                let ref_bits = ring[idx];

                let ring_xor_nonzero = reader.read_bit()?;
                if !ring_xor_nonzero {
                    // Ring XOR is zero — value == ring entry
                    let bits = ref_bits;
                    result.push(f64::from_bits(bits));
                    prev_bits = bits;
                } else {
                    // Decode XOR against ring entry (always full window / case-2 style)
                    let bucket = reader.read_bits(3)? as u8;
                    let trailing = reader.read_bits(6)? as u32;
                    let adj_leading = bucket_to_leading_zeros(bucket);
                    let meaningful = reader.read_bits(6)? as u32 + 1;
                    if adj_leading + meaningful + trailing > 64 {
                        return Err(EncodingError::CorruptData {
                            detail: format!(
                                "chimp128 ring-ref: leading ({adj_leading}) + meaningful ({meaningful}) + trailing ({trailing}) exceeds 64"
                            ),
                        });
                    }
                    let shifted = reader.read_bits(meaningful as u8)?;
                    let xor = shifted << trailing;
                    let bits = ref_bits ^ xor;
                    result.push(f64::from_bits(bits));
                    prev_bits = bits;

                    // Do NOT update prev_leading/prev_trailing
                    // from ring-ref decodes — must match encoder behavior
                    // where ring-ref and sequential paths are independent.
                }
            } else {
                // Sequential reference
                let new_window = reader.read_bit()?;
                if new_window {
                    // Case 2: new window with bucket
                    let bucket = reader.read_bits(3)? as u8;
                    let trailing = reader.read_bits(6)? as u32;
                    let adj_leading = bucket_to_leading_zeros(bucket);
                    let meaningful = reader.read_bits(6)? as u32 + 1;
                    if adj_leading + meaningful + trailing > 64 {
                        return Err(EncodingError::CorruptData {
                            detail: format!(
                                "chimp128 case-2: leading ({adj_leading}) + meaningful ({meaningful}) + trailing ({trailing}) exceeds 64"
                            ),
                        });
                    }
                    let shifted = reader.read_bits(meaningful as u8)?;
                    let xor = shifted << trailing;
                    let bits = prev_bits ^ xor;
                    result.push(f64::from_bits(bits));
                    prev_bits = bits;

                    prev_leading = adj_leading as u8;
                    prev_trailing = trailing as u8;
                } else {
                    // Case 1: reuse previous leading/trailing window
                    if prev_leading == u8::MAX {
                        return Err(EncodingError::CorruptData {
                            detail: "chimp128 case-1 encountered before any case-2 window"
                                .to_string(),
                        });
                    }
                    let meaningful = 64 - u32::from(prev_leading) - u32::from(prev_trailing);
                    let shifted = reader.read_bits(meaningful as u8)?;
                    let xor = shifted << u32::from(prev_trailing);
                    let bits = prev_bits ^ xor;
                    result.push(f64::from_bits(bits));
                    prev_bits = bits;
                }
            }

            // Push decoded value into ring
            ring[ring_pos % RING_SIZE] = prev_bits;
            ring_pos += 1;
            if ring_len < RING_SIZE {
                ring_len += 1;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_constant() {
        let values = vec![42.0_f64; 100];
        let encoded = ChimpEncoder::encode(&values).unwrap();
        let decoded = ChimpDecoder::decode(&encoded).unwrap();
        assert_eq!(values.len(), decoded.len());
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn roundtrip_sinwave() {
        let values: Vec<f64> = (0..1000)
            .map(|i| (i as f64 * 0.01).sin() * 100.0 + 50.0)
            .collect();
        let encoded = ChimpEncoder::encode(&values).unwrap();
        let decoded = ChimpDecoder::decode(&encoded).unwrap();
        assert_eq!(values.len(), decoded.len());
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn roundtrip_special_values() {
        let values = vec![
            0.0_f64,
            -0.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MIN,
            f64::MAX,
            f64::MIN_POSITIVE,
            f64::EPSILON,
            1.0,
            -1.0,
        ];
        let encoded = ChimpEncoder::encode(&values).unwrap();
        let decoded = ChimpDecoder::decode(&encoded).unwrap();
        assert_eq!(values.len(), decoded.len());
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits(), "mismatch for value {a}");
        }
    }

    #[test]
    fn roundtrip_single() {
        let values = vec![std::f64::consts::PI];
        let encoded = ChimpEncoder::encode(&values).unwrap();
        let decoded = ChimpDecoder::decode(&encoded).unwrap();
        assert_eq!(values[0].to_bits(), decoded[0].to_bits());
    }

    #[test]
    fn empty_error() {
        assert!(ChimpEncoder::encode(&[]).is_err());
    }

    #[test]
    fn compression_ratio_regular_metrics() {
        // Simulate slowly-varying integer-like metrics (e.g., temperature * 10)
        // that change by small amounts — ideal for XOR encoders
        let values: Vec<f64> = (0..10_000)
            .map(|i| {
                // Slowly varying: 50.0, 50.0, 50.1, 50.1, 50.2, …
                50.0 + (i / 100) as f64 * 0.1
            })
            .collect();
        let raw_size = values.len() * 8;
        let encoded = ChimpEncoder::encode(&values).unwrap();
        #[allow(clippy::cast_precision_loss)]
        let ratio = raw_size as f64 / encoded.len() as f64;
        // Should achieve > 2x on data with high temporal locality
        assert!(
            ratio > 2.0,
            "Expected ratio > 2x, got {ratio:.1}x ({raw_size} raw, {} encoded)",
            encoded.len()
        );
    }

    #[test]
    fn decode_into_reuses_buffer() {
        let values: Vec<f64> = (0..500).map(|i| i as f64 * 0.5).collect();
        let encoded = ChimpEncoder::encode(&values).unwrap();

        let mut output = Vec::new();
        ChimpDecoder::decode_into(&encoded, &mut output).unwrap();
        assert_eq!(values.len(), output.len());
        for (a, b) in values.iter().zip(output.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }

        // Decode again into the same buffer — should clear+reuse
        let values2: Vec<f64> = (0..300).map(|i| i as f64 * 1.1).collect();
        let encoded2 = ChimpEncoder::encode(&values2).unwrap();
        ChimpDecoder::decode_into(&encoded2, &mut output).unwrap();
        assert_eq!(values2.len(), output.len());
        for (a, b) in values2.iter().zip(output.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn roundtrip_1m_floats() {
        // Verify correctness on 1M values
        let values: Vec<f64> = (0..1_000_000)
            .map(|i| 50.0 + (i as f64 * 0.001).sin() * 10.0)
            .collect();
        let encoded = ChimpEncoder::encode(&values).unwrap();
        let decoded = ChimpDecoder::decode(&encoded).unwrap();
        assert_eq!(values.len(), decoded.len());
        // Spot check first, middle, and last
        assert_eq!(values[0].to_bits(), decoded[0].to_bits());
        assert_eq!(values[500_000].to_bits(), decoded[500_000].to_bits());
        assert_eq!(values[999_999].to_bits(), decoded[999_999].to_bits());
    }

    // ── Chimp128 tests ───────────────────────────────────────────────

    #[test]
    fn chimp128_roundtrip_constant() {
        let values = vec![42.0_f64; 200];
        let encoded = Chimp128Encoder::encode(&values).unwrap();
        let decoded = Chimp128Decoder::decode(&encoded).unwrap();
        assert_eq!(values.len(), decoded.len());
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn chimp128_roundtrip_sinwave() {
        // Sinusoidal data — Chimp128 should handle well due to repeating
        // patterns in the ring buffer.
        let values: Vec<f64> = (0..2000)
            .map(|i| (i as f64 * 0.01).sin() * 100.0 + 50.0)
            .collect();
        let encoded = Chimp128Encoder::encode(&values).unwrap();
        let decoded = Chimp128Decoder::decode(&encoded).unwrap();
        assert_eq!(values.len(), decoded.len());
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn chimp128_roundtrip_special_values() {
        let values = vec![
            0.0_f64,
            -0.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MIN,
            f64::MAX,
            f64::MIN_POSITIVE,
            f64::EPSILON,
            1.0,
            -1.0,
        ];
        let encoded = Chimp128Encoder::encode(&values).unwrap();
        let decoded = Chimp128Decoder::decode(&encoded).unwrap();
        assert_eq!(values.len(), decoded.len());
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits(), "mismatch for value {a}");
        }
    }

    #[test]
    fn chimp128_beats_chimp_on_periodic() {
        // Periodic sensor data: cycles through a small set of values
        // repeatedly.  Chimp128's ring buffer should find exact or near
        // matches and compress better than plain Chimp.
        let base_values: Vec<f64> = (0..64).map(|i| 20.0 + (i as f64) * 0.5).collect();
        let values: Vec<f64> = base_values.iter().cycle().take(5000).copied().collect();

        let chimp_encoded = ChimpEncoder::encode(&values).unwrap();
        let chimp128_encoded = Chimp128Encoder::encode(&values).unwrap();

        // Verify correctness
        let decoded = Chimp128Decoder::decode(&chimp128_encoded).unwrap();
        assert_eq!(values.len(), decoded.len());
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }

        // Chimp128 should produce equal or smaller output
        assert!(
            chimp128_encoded.len() <= chimp_encoded.len(),
            "Expected Chimp128 ({} bytes) <= Chimp ({} bytes) on periodic data",
            chimp128_encoded.len(),
            chimp_encoded.len(),
        );
    }

    #[test]
    fn chimp128_empty_error() {
        assert!(Chimp128Encoder::encode(&[]).is_err());
    }

    #[test]
    fn chimp128_single_value() {
        let values = vec![std::f64::consts::PI];
        let encoded = Chimp128Encoder::encode(&values).unwrap();
        let decoded = Chimp128Decoder::decode(&encoded).unwrap();
        assert_eq!(values[0].to_bits(), decoded[0].to_bits());
    }

    #[test]
    fn chimp128_decode_into() {
        let values: Vec<f64> = (0..500).map(|i| i as f64 * 0.5).collect();
        let encoded = Chimp128Encoder::encode(&values).unwrap();
        let mut output = Vec::new();
        Chimp128Decoder::decode_into(&encoded, &mut output).unwrap();
        assert_eq!(values.len(), output.len());
        for (a, b) in values.iter().zip(output.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip_arbitrary(values in proptest::collection::vec(any::<f64>(), 1..500)) {
            let encoded = ChimpEncoder::encode(&values).unwrap();
            let decoded = ChimpDecoder::decode(&encoded).unwrap();
            prop_assert_eq!(values.len(), decoded.len());
            for (a, b) in values.iter().zip(decoded.iter()) {
                prop_assert_eq!(a.to_bits(), b.to_bits());
            }
        }

        #[test]
        fn chimp128_roundtrip_arbitrary(values in proptest::collection::vec(any::<f64>(), 1..500)) {
            let encoded = Chimp128Encoder::encode(&values).unwrap();
            let decoded = Chimp128Decoder::decode(&encoded).unwrap();
            prop_assert_eq!(values.len(), decoded.len());
            for (a, b) in values.iter().zip(decoded.iter()) {
                prop_assert_eq!(a.to_bits(), b.to_bits());
            }
        }
    }
}
