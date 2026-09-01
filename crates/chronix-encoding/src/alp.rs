//! ALP — Adaptive Lossless floating-Point compression (SIGMOD 2024).
//!
//! Afroozeh, Kuffo & Boncz, *"ALP: Adaptive Lossless floating-Point
//! Compression"*, Proc. ACM Manag. Data 2(1), SIGMOD 2024. The scheme DuckDB
//! adopted, and the current state of the art for lossless `f64` columns —
//! better ratios than Chimp/Patas/Gorilla on real data and an order of
//! magnitude faster to decode.
//!
//! # The idea
//!
//! XOR-based codecs (Gorilla, Chimp, Patas) treat a double as an opaque bit
//! pattern and hope consecutive values share a prefix. But most doubles in a
//! time-series database never had 15 digits of precision to begin with: they
//! are *decimals* that a sensor, meter or price feed produced with two or
//! three fractional digits and that IEEE-754 then stored approximately.
//! `231.45` is not a random 64-bit pattern — it is the integer `23145` with a
//! known scale.
//!
//! ALP recovers that integer. For a block it picks one exponent pair
//! `(e, f)` and encodes each value as
//!
//! ```text
//! i = round(v · 10^e · 10^-f)     decode: v = i · 10^f · 10^-e
//! ```
//!
//! The integers are then frame-of-reference coded and bit-packed, which is
//! where the compression comes from: a power meter reporting 200–250 W to two
//! decimals yields integers spanning 5000 values — 13 bits instead of 64.
//!
//! # Exactness
//!
//! The transform is a *guess*, so every value is verified by decoding it
//! again and comparing bit patterns. Values that do not round-trip — genuine
//! high-precision doubles, `NaN`, `±inf`, `-0.0`, anything outside `i64` —
//! are stored verbatim as **exceptions** alongside their positions. Both
//! sides call the same [`decode_value`], so encoder and decoder cannot
//! disagree about what a given integer means, and the codec is bitwise
//! lossless like every other encoder in this crate.
//!
//! # Scope
//!
//! Only the decimal scheme is implemented. The paper's second scheme,
//! `ALP_RD`, targets columns of genuinely non-decimal doubles (scientific
//! measurements, hashes) by splitting the bit pattern into a dictionary-coded
//! left part and a packed right part. Chronix already has three XOR codecs
//! that cover exactly that shape, and [`AdaptiveSelector`](crate::AdaptiveSelector)
//! picks between them by trial encoding — so ALP joins the competition and
//! loses gracefully on data it is not for, rather than duplicating a fallback
//! that already exists.
//!
//! # Wire format
//!
//! ```text
//! [count: u32 LE][e: u8][f: u8][bit_width: u8][reference: i64 LE]
//! [exception_count: u32 LE]
//! [packed offsets: count × bit_width bits, MSB-first]
//! [exceptions: exception_count × ([position: u32 LE][value: 8 bytes LE])]
//! ```

use crate::coding::{bits_needed, checked_count, checked_decode_count, pack_bits, unpack_bits};
use crate::error::{EncodingError, Result};

/// Header size: `count(4) + e(1) + f(1) + bit_width(1) + reference(8) +
/// exception_count(4)`.
const HEADER_SIZE: usize = 19;

/// Bytes per stored exception: position + raw value.
const EXCEPTION_SIZE: usize = 12;

/// Largest usable exponent: `10^18` is the biggest power of ten in `i64`.
const MAX_EXPONENT: u8 = 18;

/// Exact powers of ten. Every entry is exactly representable in `f64`
/// (integers below `2^53`), so `F10[i] == 10^i` with no rounding.
const F10: [f64; 19] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18,
];

/// Reciprocal powers of ten. These are *not* exact — `1e-1` is the nearest
/// double to 0.1 — which is fine because correctness comes from verifying the
/// round trip, not from the arithmetic being exact. What matters is that
/// encoder and decoder use the identical constants and the identical
/// multiplication order; [`decode_value`] is the single place that does.
const IF10: [f64; 19] = [
    1e-0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10, 1e-11, 1e-12, 1e-13, 1e-14,
    1e-15, 1e-16, 1e-17, 1e-18,
];

/// Values sampled per block when searching for the exponent pair.
const EXPONENT_SAMPLE: usize = 64;

/// Bits an exception costs relative to a packed value: 32-bit position plus
/// the 64-bit value. Used only to score candidate exponents.
const EXCEPTION_BITS: usize = 96;

/// ALP float encoder (SIGMOD 2024).
#[derive(Debug, Clone, Copy)]
pub struct AlpEncoder;

/// ALP float decoder.
#[derive(Debug, Clone, Copy)]
pub struct AlpDecoder;

/// Reconstruct the original value from its integer encoding.
///
/// The single definition of what `(i, e, f)` means. The encoder calls it to
/// verify each value round-trips and the decoder calls it to reconstruct, so
/// the two cannot drift apart.
#[inline]
#[must_use]
pub fn decode_value(encoded: i64, e: u8, f: u8) -> f64 {
    encoded as f64 * F10[f as usize] * IF10[e as usize]
}

/// Try to encode one value with the given exponents.
///
/// Returns `None` when the value cannot be recovered exactly, which makes it
/// an exception.
#[inline]
fn encode_value(v: f64, e: u8, f: u8) -> Option<i64> {
    let scaled = v * F10[e as usize] * IF10[f as usize];
    if !scaled.is_finite() {
        return None;
    }
    let rounded = scaled.round_ties_even();
    // `as` saturates rather than wrapping, so the range must be checked
    // before the cast or distinct values would collapse onto i64::MAX.
    if !(rounded >= -(2f64.powi(62)) && rounded <= 2f64.powi(62)) {
        return None;
    }
    let encoded = rounded as i64;
    // Bit comparison, not `==`: it is what distinguishes `-0.0` from `0.0`
    // and what makes the guarantee bitwise rather than numeric.
    (decode_value(encoded, e, f).to_bits() == v.to_bits()).then_some(encoded)
}

/// Pick the `(e, f)` pair that should compress this block best.
///
/// Scores every `0 <= f <= e <= 18` combination against a stratified sample by
/// the size it would produce — packed width for the values that round-trip,
/// plus a fixed cost for the ones that do not — and takes the cheapest. This
/// is the paper's two-level sampling reduced to one level: at Chronix's block
/// sizes the search is a few thousand multiplications and runs once per block,
/// so the second level buys nothing.
fn choose_exponents(values: &[f64]) -> (u8, u8) {
    let step = (values.len() / EXPONENT_SAMPLE).max(1);
    let sample: Vec<f64> = values.iter().copied().step_by(step).collect();
    let n = sample.len();

    let mut best = (0u8, 0u8);
    let mut best_bits = usize::MAX;

    for e in 0..=MAX_EXPONENT {
        for f in 0..=e {
            let mut min = i64::MAX;
            let mut max = i64::MIN;
            let mut ok = 0usize;
            for &v in &sample {
                if let Some(i) = encode_value(v, e, f) {
                    min = min.min(i);
                    max = max.max(i);
                    ok += 1;
                }
            }
            if ok == 0 {
                continue;
            }
            let width = usize::from(bits_needed(max.wrapping_sub(min) as u64));
            let bits = n * width + (n - ok) * EXCEPTION_BITS;
            // Ties go to the smaller exponent: it keeps the integers smaller,
            // which keeps the frame-of-reference offsets smaller on the parts
            // of the block the sample did not see.
            if bits < best_bits {
                best_bits = bits;
                best = (e, f);
            }
        }
    }

    best
}

impl AlpEncoder {
    /// Encode a sequence of `f64` values with ALP.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyInput`] if `values` is empty, or
    /// [`EncodingError::CorruptData`] if the block is longer than `u32::MAX`.
    pub fn encode(values: &[f64]) -> Result<Vec<u8>> {
        if values.is_empty() {
            return Err(EncodingError::EmptyInput {
                context: "ALP encoder",
            });
        }
        let count = checked_count(values.len())?;
        let (e, f) = choose_exponents(values);

        let mut encoded: Vec<i64> = Vec::with_capacity(values.len());
        let mut exceptions: Vec<(u32, f64)> = Vec::new();
        let mut filler: Option<i64> = None;

        for (idx, &v) in values.iter().enumerate() {
            match encode_value(v, e, f) {
                Some(i) => {
                    filler.get_or_insert(i);
                    encoded.push(i);
                }
                None => {
                    // Exception slots still occupy a packed position. Filling
                    // them with a value that actually occurs keeps them inside
                    // the frame of reference, so a single outlier cannot widen
                    // the whole block.
                    exceptions.push(u32::try_from(idx).map_or((u32::MAX, v), |i| (i, v)));
                    encoded.push(i64::MIN); // patched below
                }
            }
        }

        let filler = filler.unwrap_or(0);
        for (idx, _) in &exceptions {
            if let Some(slot) = encoded.get_mut(*idx as usize) {
                *slot = filler;
            }
        }

        let reference = encoded.iter().copied().min().unwrap_or(0);
        let offsets: Vec<u64> = encoded
            .iter()
            .map(|&v| v.wrapping_sub(reference) as u64)
            .collect();
        let bit_width = bits_needed(offsets.iter().copied().max().unwrap_or(0));

        let data_bytes = (offsets.len() as u64 * u64::from(bit_width)).div_ceil(8) as usize;
        let mut buf =
            Vec::with_capacity(HEADER_SIZE + data_bytes + exceptions.len() * EXCEPTION_SIZE);
        buf.extend_from_slice(&count.to_le_bytes());
        buf.push(e);
        buf.push(f);
        buf.push(bit_width);
        buf.extend_from_slice(&reference.to_le_bytes());
        buf.extend_from_slice(&checked_count(exceptions.len())?.to_le_bytes());

        pack_bits(&offsets, bit_width, &mut buf);

        for (idx, value) in &exceptions {
            buf.extend_from_slice(&idx.to_le_bytes());
            buf.extend_from_slice(&value.to_bits().to_le_bytes());
        }

        Ok(buf)
    }
}

impl AlpDecoder {
    /// Decode an ALP-encoded block.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CorruptData`] if the buffer is truncated, the
    /// exponents are out of range, or an exception position is out of bounds.
    pub fn decode(data: &[u8]) -> Result<Vec<f64>> {
        if data.len() < HEADER_SIZE {
            return Err(EncodingError::CorruptData {
                detail: format!(
                    "ALP block is {} bytes, need at least {HEADER_SIZE}",
                    data.len()
                ),
            });
        }

        let count = checked_decode_count(
            u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize,
            "ALP",
        )?;
        let e = data[4];
        let f = data[5];
        let bit_width = data[6];
        let reference = i64::from_le_bytes([
            data[7], data[8], data[9], data[10], data[11], data[12], data[13], data[14],
        ]);
        let exception_count = checked_decode_count(
            u32::from_le_bytes([data[15], data[16], data[17], data[18]]) as usize,
            "ALP exception",
        )?;

        if e > MAX_EXPONENT || f > e {
            return Err(EncodingError::CorruptData {
                detail: format!("ALP exponents out of range: e={e}, f={f}"),
            });
        }

        let data_bytes = (count as u64 * u64::from(bit_width)).div_ceil(8) as usize;
        let body = &data[HEADER_SIZE..];
        if body.len() < data_bytes + exception_count * EXCEPTION_SIZE {
            return Err(EncodingError::CorruptData {
                detail: "ALP block is truncated".to_string(),
            });
        }

        let offsets = unpack_bits(&body[..data_bytes], count, bit_width)?;
        let mut values: Vec<f64> = offsets
            .into_iter()
            .map(|o| decode_value(reference.wrapping_add(o as i64), e, f))
            .collect();

        let mut pos = data_bytes;
        for _ in 0..exception_count {
            let idx = u32::from_le_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]])
                as usize;
            let bits = u64::from_le_bytes([
                body[pos + 4],
                body[pos + 5],
                body[pos + 6],
                body[pos + 7],
                body[pos + 8],
                body[pos + 9],
                body[pos + 10],
                body[pos + 11],
            ]);
            let slot = values
                .get_mut(idx)
                .ok_or_else(|| EncodingError::CorruptData {
                    detail: format!(
                        "ALP exception position {idx} is outside a {count}-value block"
                    ),
                })?;
            *slot = f64::from_bits(bits);
            pos += EXCEPTION_SIZE;
        }

        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(values: &[f64]) -> Vec<f64> {
        let encoded = AlpEncoder::encode(values).expect("encode");
        AlpDecoder::decode(&encoded).expect("decode")
    }

    fn assert_bitwise(values: &[f64]) {
        let decoded = roundtrip(values);
        assert_eq!(decoded.len(), values.len());
        for (i, (a, b)) in values.iter().zip(&decoded).enumerate() {
            assert_eq!(
                a.to_bits(),
                b.to_bits(),
                "value {i} changed: {a} ({:#x}) -> {b} ({:#x})",
                a.to_bits(),
                b.to_bits()
            );
        }
    }

    #[test]
    fn two_decimal_sensor_readings_round_trip() {
        let values: Vec<f64> = (0..1024)
            .map(|i| 200.0 + f64::from(i % 500) / 100.0)
            .collect();
        assert_bitwise(&values);
    }

    /// The point of the codec: decimal data must beat the raw 8 bytes/value
    /// by a wide margin, and beat the XOR codecs it is meant to replace.
    #[test]
    fn decimal_data_compresses_far_better_than_xor_codecs() {
        let values: Vec<f64> = (0..4096)
            .map(|i| 230.0 + f64::from(i % 2000) / 100.0)
            .collect();
        let raw = values.len() * 8;
        let alp = AlpEncoder::encode(&values).unwrap().len();
        let chimp = crate::chimp::ChimpEncoder::encode(&values).unwrap().len();
        let gorilla = crate::gorilla::GorillaEncoder::encode(&values)
            .unwrap()
            .len();
        let patas = crate::patas::PatasEncoder::encode(&values).unwrap().len();

        assert!(
            alp * 4 < raw,
            "ALP should compress decimal data at least 4x: {alp} vs {raw}"
        );
        assert!(
            alp < chimp && alp < gorilla && alp < patas,
            "ALP {alp} did not beat chimp {chimp} / gorilla {gorilla} / patas {patas}"
        );
    }

    #[test]
    fn integers_round_trip() {
        let values: Vec<f64> = (0..500).map(f64::from).collect();
        assert_bitwise(&values);
    }

    #[test]
    fn constant_column_round_trips() {
        assert_bitwise(&[42.5; 300]);
    }

    #[test]
    fn single_value() {
        assert_bitwise(&[1.5]);
    }

    #[test]
    fn special_values_become_exceptions_and_survive() {
        let values = vec![
            0.0,
            -0.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MIN,
            f64::MAX,
            f64::MIN_POSITIVE,
            1.5,
            -2.25,
        ];
        let decoded = roundtrip(&values);
        for (i, (a, b)) in values.iter().zip(&decoded).enumerate() {
            assert_eq!(a.to_bits(), b.to_bits(), "special value {i} changed");
        }
    }

    /// High-precision doubles cannot be expressed as scaled integers, so they
    /// all become exceptions. The result must still be correct — just bigger,
    /// which is what makes the adaptive selector pick an XOR codec instead.
    #[test]
    fn non_decimal_doubles_still_round_trip() {
        let values: Vec<f64> = (0..256)
            .map(|i| f64::from(i).mul_add(0.000_000_000_123_456_789, std::f64::consts::PI))
            .collect();
        assert_bitwise(&values);
    }

    #[test]
    fn negative_and_mixed_signs() {
        let values: Vec<f64> = (0..1000).map(|i| (f64::from(i) - 500.0) / 8.0).collect();
        assert_bitwise(&values);
    }

    #[test]
    fn empty_input_is_rejected() {
        assert!(AlpEncoder::encode(&[]).is_err());
    }

    #[test]
    fn truncated_block_is_rejected() {
        let encoded = AlpEncoder::encode(&[1.0, 2.0, 3.0]).unwrap();
        assert!(AlpDecoder::decode(&encoded[..HEADER_SIZE - 1]).is_err());
        assert!(AlpDecoder::decode(&encoded[..encoded.len() - 1]).is_err());
    }

    #[test]
    fn corrupt_exponents_are_rejected() {
        let mut encoded = AlpEncoder::encode(&[1.0, 2.0, 3.0]).unwrap();
        encoded[4] = 99; // e out of range
        assert!(AlpDecoder::decode(&encoded).is_err());
    }

    /// The reciprocal table must match the division it stands in for,
    /// otherwise encoder-side verification and decoder-side reconstruction
    /// would be computing different things.
    #[test]
    fn reciprocal_table_matches_division() {
        for i in 0..=MAX_EXPONENT as usize {
            assert_eq!(
                IF10[i].to_bits(),
                (1.0 / F10[i]).to_bits(),
                "IF10[{i}] does not match 1/F10[{i}]"
            );
        }
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// Bitwise-exact round trip is the crate's contract (design principle
        /// 1), and ALP is the one codec that reaches its answer by guessing —
        /// so it is the one that most needs an arbitrary-input check.
        #[test]
        fn roundtrip_arbitrary_f64(values in prop::collection::vec(any::<f64>(), 1..300)) {
            let encoded = AlpEncoder::encode(&values).unwrap();
            let decoded = AlpDecoder::decode(&encoded).unwrap();
            prop_assert_eq!(decoded.len(), values.len());
            for (a, b) in values.iter().zip(&decoded) {
                prop_assert_eq!(a.to_bits(), b.to_bits());
            }
        }

        /// Decimal values with a bounded number of fractional digits are the
        /// shape ALP is for; these must round-trip too, and mostly avoid the
        /// exception path.
        #[test]
        fn roundtrip_decimals(
            scaled in prop::collection::vec(-1_000_000i64..1_000_000, 1..300),
            digits in 0u32..6,
        ) {
            let scale = 10f64.powi(digits as i32);
            let values: Vec<f64> = scaled.iter().map(|&v| v as f64 / scale).collect();
            let encoded = AlpEncoder::encode(&values).unwrap();
            let decoded = AlpDecoder::decode(&encoded).unwrap();
            for (a, b) in values.iter().zip(&decoded) {
                prop_assert_eq!(a.to_bits(), b.to_bits());
            }
        }

        /// A decoder must never panic on adversarial bytes, only error.
        #[test]
        fn decode_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
            let _ = AlpDecoder::decode(&bytes);
        }
    }
}
