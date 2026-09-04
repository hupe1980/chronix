//! Pcodec (`pco`) — Loncaric, *Pcodec: Better Compression for Numerical
//! Sequences* (2025) — as a block codec for floats and integers.
//!
//! pco decomposes each number into latent variables through a "mode"
//! (integer-multiple or float-multiple recovery, the same insight as ALP's
//! decimal recovery), delta-encodes them, and entropy-codes the deltas with
//! ANS against learned bins. On the workloads this crate is measured
//! against it compresses **3–20× better than ALP and decodes faster**
//! (`tests/codec_field_check.rs` prints the table), which is why it leads
//! every adaptive candidate list. ALP and the XOR codecs stay behind it: a
//! candidate that loses the per-block trial costs one sample encode and
//! nothing else, and a second opinion on a codec this new is worth that.
//!
//! # Wire format
//!
//! ```text
//! [num_values: u32 LE][pco standalone file]
//! ```
//!
//! The count is ours, checked against the crate-wide decode ceiling before a
//! single byte is decompressed, and the decompression writes into a buffer
//! of exactly that size: pco's own chunk headers are never trusted for an
//! allocation, and a file that decodes to a different count is corrupt.

use pco::data_types::Number;
use pco::standalone::{simple_compress, simple_decompress_into};
use pco::ChunkConfig;

use crate::coding::MAX_BLOCK_VALUES;
use crate::error::{EncodingError, Result};

const HEADER: usize = 4;

/// Pcodec encoder for `f64`, `i64` and `u64` columns.
pub struct PcoEncoder;

/// Pcodec decoder.
pub struct PcoDecoder;

impl PcoEncoder {
    /// Encode a float column.
    ///
    /// # Errors
    ///
    /// Returns an error if the input is empty or larger than `u32::MAX`.
    pub fn encode_f64(values: &[f64]) -> Result<Vec<u8>> {
        Self::encode(values)
    }

    /// Encode a signed integer column.
    ///
    /// # Errors
    ///
    /// As [`encode_f64`](Self::encode_f64).
    pub fn encode_i64(values: &[i64]) -> Result<Vec<u8>> {
        Self::encode(values)
    }

    /// Encode an unsigned integer column.
    ///
    /// # Errors
    ///
    /// As [`encode_f64`](Self::encode_f64).
    pub fn encode_u64(values: &[u64]) -> Result<Vec<u8>> {
        Self::encode(values)
    }

    /// Encode a timestamp column.
    ///
    /// Timestamps are the one column whose shape is known before any data
    /// is seen — a near-arithmetic sequence with jitter and gaps — so the
    /// delta order is pinned to second-order (delta-of-delta) rather than
    /// auto-detected. The decoder needs no such hint; the pco file
    /// describes its own delta encoding.
    ///
    /// # Errors
    ///
    /// As [`encode_f64`](Self::encode_f64).
    pub fn encode_timestamps(values: &[i64]) -> Result<Vec<u8>> {
        Self::encode_with(values, &ChunkConfig::default())
    }

    fn encode<T: Number>(values: &[T]) -> Result<Vec<u8>> {
        Self::encode_with(values, &ChunkConfig::default())
    }

    fn encode_with<T: Number>(values: &[T], config: &ChunkConfig) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "pco encode",
            });
        }
        let n = u32::try_from(values.len()).map_err(|_| EncodingError::CorruptData {
            detail: format!("pco: {} values exceed a block", values.len()),
        })?;
        let body = simple_compress(values, config).map_err(|e| EncodingError::CorruptData {
            detail: format!("pco encode: {e}"),
        })?;
        let mut out = Vec::with_capacity(HEADER + body.len());
        out.extend_from_slice(&n.to_le_bytes());
        out.extend_from_slice(&body);
        Ok(out)
    }
}

impl PcoDecoder {
    /// Decode a float column.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload is truncated, declares more values
    /// than the decode ceiling allows, or does not decode to exactly the
    /// declared count.
    pub fn decode_f64(data: &[u8]) -> Result<Vec<f64>> {
        Self::decode(data)
    }

    /// Decode a signed integer column.
    ///
    /// # Errors
    ///
    /// As [`decode_f64`](Self::decode_f64).
    pub fn decode_i64(data: &[u8]) -> Result<Vec<i64>> {
        Self::decode(data)
    }

    /// Decode an unsigned integer column.
    ///
    /// # Errors
    ///
    /// As [`decode_f64`](Self::decode_f64).
    pub fn decode_u64(data: &[u8]) -> Result<Vec<u64>> {
        Self::decode(data)
    }

    fn decode<T: Number + Default + Copy>(data: &[u8]) -> Result<Vec<T>> {
        if data.len() < HEADER {
            return Err(EncodingError::CorruptData {
                detail: "pco: truncated header".into(),
            });
        }
        let n = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if n == 0 || n > MAX_BLOCK_VALUES {
            return Err(EncodingError::CorruptData {
                detail: format!("pco: declared {n} values, ceiling is {MAX_BLOCK_VALUES}"),
            });
        }
        // The only allocation is ours, sized by a count we have bounded.
        let mut out = vec![T::default(); n];
        let progress = simple_decompress_into(&data[HEADER..], &mut out).map_err(|e| {
            EncodingError::CorruptData {
                detail: format!("pco decode: {e}"),
            }
        })?;
        if progress.n_processed != n || !progress.finished {
            return Err(EncodingError::CountMismatch {
                expected: n,
                actual: progress.n_processed,
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_all_three_types() {
        let f: Vec<f64> = (0..5000)
            .map(|i| 230.0 + f64::from(i % 400) / 100.0)
            .collect();
        assert_eq!(
            PcoDecoder::decode_f64(&PcoEncoder::encode_f64(&f).unwrap()).unwrap(),
            f
        );
        let i: Vec<i64> = (0..5000).map(|i| 42_000 + i64::from(i) * 3).collect();
        assert_eq!(
            PcoDecoder::decode_i64(&PcoEncoder::encode_i64(&i).unwrap()).unwrap(),
            i
        );
        let u: Vec<u64> = (0..5000u64).map(|i| i * 7).collect();
        assert_eq!(
            PcoDecoder::decode_u64(&PcoEncoder::encode_u64(&u).unwrap()).unwrap(),
            u
        );
        let specials = vec![f64::NAN, f64::INFINITY, -0.0, f64::MIN_POSITIVE, 1e300];
        let back = PcoDecoder::decode_f64(&PcoEncoder::encode_f64(&specials).unwrap()).unwrap();
        for (a, b) in specials.iter().zip(&back) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn beats_alp_on_decimal_data() {
        let f: Vec<f64> = (0..8192)
            .map(|i| 230.0 + f64::from((i * 37) % 4000) / 100.0)
            .collect();
        let pco = PcoEncoder::encode_f64(&f).unwrap().len();
        let alp = crate::alp::AlpEncoder::encode(&f).unwrap().len();
        assert!(pco * 4 < alp, "pco {pco} B vs ALP {alp} B");
    }

    #[test]
    fn a_corrupt_header_cannot_allocate_and_a_wrong_count_is_an_error() {
        let f: Vec<f64> = (0..100).map(f64::from).collect();
        let mut enc = PcoEncoder::encode_f64(&f).unwrap();
        // Declare u32::MAX values: rejected before any allocation.
        enc[..4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            PcoDecoder::decode_f64(&enc),
            Err(EncodingError::CorruptData { .. })
        ));
        // Declare fewer values than the file holds: not finished → error.
        enc[..4].copy_from_slice(&50u32.to_le_bytes());
        assert!(PcoDecoder::decode_f64(&enc).is_err());
        // Declare more: not all processed → error.
        enc[..4].copy_from_slice(&150u32.to_le_bytes());
        assert!(PcoDecoder::decode_f64(&enc).is_err());
        // Garbage body.
        assert!(PcoDecoder::decode_f64(&[7, 0, 0, 0, 1, 2, 3]).is_err());
        assert!(PcoDecoder::decode_f64(&[1, 0]).is_err());
    }
}
