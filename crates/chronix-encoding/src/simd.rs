//! SIMD-friendly batch operations for encoding acceleration.
//!
//! These functions provide batch versions of common encoding primitives —
//! XOR, leading-zero counting, trailing-zero counting, delta computation,
//! and `ZigZag` encoding. Written as tight loops over contiguous slices,
//! they are designed to **auto-vectorize** into native SIMD instructions
//! (SSE2/AVX2 on x86-64, NEON on `AArch64`) when compiled with
//! `opt-level >= 2`.
//!
//! # Architecture
//!
//! XOR-based float compression (Gorilla, Chimp) is inherently serial at the
//! *encoding decision* level because each value's encoding depends on the
//! previous value's bit window. However, the underlying math — XOR
//! computation, leading-zero counting, trailing-zero counting — can be
//! pre-computed in batch for better CPU pipeline utilization.
//!
//! The largest encoding speedup comes from the [`BitWriter`](crate::delta::BitWriter)
//! rewrite (u64 word accumulator vs. per-bit function calls) rather than
//! from explicit SIMD intrinsics on the serial XOR chain. These batch
//! functions provide infrastructure for future encoders and parallelizable
//! operations.
//!
//! # Compiler Hints
//!
//! For maximum auto-vectorization, compile with:
//! ```text
//! RUSTFLAGS="-C target-cpu=native"
//! ```
//! This enables AVX2, BMI1/BMI2, LZCNT, and TZCNT on modern x86-64 CPUs.

// Batch SIMD-friendly primitives used by Gorilla and Chimp encoders.

/// Compute XOR of consecutive `u64` pairs in batch.
///
/// Given `values` of length N, produces N−1 XOR results where
/// `result[i] = values[i] ^ values[i+1]`.
///
/// This is the core pre-computation for XOR-based float compression
/// (Gorilla, Chimp). Auto-vectorizes into SIMD XOR instructions.
#[inline]
pub(crate) fn batch_xor_adjacent(values: &[u64], out: &mut Vec<u64>) {
    let n = values.len().saturating_sub(1);
    out.clear();
    out.reserve(n);
    for pair in values.windows(2) {
        out.push(pair[0] ^ pair[1]);
    }
}

/// Compute leading zeros for each `u64` value in batch.
///
/// Compiles to LZCNT (BMI1) when targeting modern CPUs, or BSR + conversion
/// on baseline x86-64. Auto-vectorizes with AVX-512 CD (`VPLZCNTQ`).
#[inline]
pub(crate) fn batch_leading_zeros(values: &[u64], out: &mut Vec<u32>) {
    out.clear();
    out.reserve(values.len());
    for &v in values {
        out.push(v.leading_zeros());
    }
}

/// Compute trailing zeros for each `u64` value in batch.
///
/// Compiles to TZCNT (BMI1) or BSF on x86-64.
#[inline]
pub(crate) fn batch_trailing_zeros(values: &[u64], out: &mut Vec<u32>) {
    out.clear();
    out.reserve(values.len());
    for &v in values {
        out.push(v.trailing_zeros());
    }
}

/// Convert `f64` values to their `u64` IEEE 754 bit representations in batch.
///
/// This is a reinterpret cast (no arithmetic) that enables fused
/// `to_bits` + XOR vectorization when combined with [`batch_xor_adjacent`].
#[inline]
pub(crate) fn batch_f64_to_bits(values: &[f64], out: &mut Vec<u64>) {
    out.clear();
    out.reserve(values.len());
    for &v in values {
        out.push(v.to_bits());
    }
}

/// Convert `u64` IEEE 754 bit representations back to `f64` values in batch.
#[inline]
#[allow(dead_code)]
pub(crate) fn batch_bits_to_f64(bits: &[u64], out: &mut Vec<f64>) {
    out.clear();
    out.reserve(bits.len());
    for &b in bits {
        out.push(f64::from_bits(b));
    }
}

/// Compute the delta (difference) of consecutive `i64` values in batch.
///
/// Given `values` of length N, produces N−1 deltas where
/// `result[i] = values[i+1] - values[i]`. Uses wrapping arithmetic for
/// overflow safety. Auto-vectorizes into SIMD subtract instructions.
#[inline]
#[allow(dead_code)]
pub(crate) fn batch_delta(values: &[i64], out: &mut Vec<i64>) {
    let n = values.len().saturating_sub(1);
    out.clear();
    out.reserve(n);
    for pair in values.windows(2) {
        out.push(pair[1].wrapping_sub(pair[0]));
    }
}

/// Batch `ZigZag` encode: maps signed integers to unsigned.
///
/// `ZigZag` maps negative values to odd positives and non-negatives to even
/// positives, so small-magnitude values produce small unsigned values.
/// Auto-vectorizes into SIMD shift + XOR operations.
#[inline]
#[allow(dead_code)]
pub(crate) fn batch_zigzag_encode(values: &[i64], out: &mut Vec<u64>) {
    out.clear();
    out.reserve(values.len());
    for &v in values {
        out.push(((v << 1) ^ (v >> 63)) as u64);
    }
}

/// Batch `ZigZag` decode: maps unsigned integers back to signed.
#[inline]
pub(crate) fn batch_zigzag_decode(values: &[u64], out: &mut Vec<i64>) {
    out.clear();
    out.reserve(values.len());
    for &v in values {
        out.push(((v >> 1) as i64) ^ -((v & 1) as i64));
    }
}

// ── SIMD-friendly batch decode primitives ─────────────────

/// Compute the inclusive prefix sum of `i64` deltas in batch.
///
/// Given a `first` value and a slice of deltas, produces the reconstructed
/// sequence where `out[0] = first`, `out[i] = out[i-1] + deltas[i-1]`.
/// Uses wrapping arithmetic for overflow safety.
///
/// This is the inverse of [`batch_delta`] and is used by the integer and
/// delta-of-delta decoders to reconstruct values from delta-encoded data.
#[inline]
pub(crate) fn batch_prefix_sum_i64(first: i64, deltas: &[i64], out: &mut Vec<i64>) {
    out.clear();
    out.reserve(deltas.len() + 1);
    out.push(first);
    let mut acc = first;
    for &d in deltas {
        acc = acc.wrapping_add(d);
        out.push(acc);
    }
}

/// Add a scalar `reference` to each element of `offsets` in batch (i64).
///
/// Produces `out[i] = reference + offsets[i]`. Uses wrapping arithmetic.
/// This is the core decode step of Frame-of-Reference (FOR) encoding.
/// Auto-vectorizes into SIMD add instructions.
#[inline]
pub(crate) fn batch_add_scalar_i64(reference: i64, offsets: &[u64], out: &mut Vec<i64>) {
    out.clear();
    out.reserve(offsets.len());
    for &off in offsets {
        out.push(reference.wrapping_add(off as i64));
    }
}

/// Add a scalar `reference` to each element of `offsets` in batch (u64).
///
/// Produces `out[i] = reference + offsets[i]`. Uses wrapping arithmetic.
/// Auto-vectorizes into SIMD add instructions.
#[inline]
pub(crate) fn batch_add_scalar_u64(reference: u64, offsets: &[u64], out: &mut Vec<u64>) {
    out.clear();
    out.reserve(offsets.len());
    for &off in offsets {
        out.push(reference.wrapping_add(off));
    }
}

/// Batch XOR reconstruction: given a starting `base` value and a slice
/// of XOR deltas, reconstruct the original values.
///
/// `out[0] = base`, `out[i] = out[i-1] ^ xors[i-1]`.
/// This is the inverse of [`batch_xor_adjacent`] and is used by
/// Gorilla/Chimp decoders to reconstruct f64 bit patterns.
#[inline]
#[allow(dead_code)]
pub(crate) fn batch_xor_reconstruct(base: u64, xors: &[u64], out: &mut Vec<u64>) {
    out.clear();
    out.reserve(xors.len() + 1);
    out.push(base);
    let mut prev = base;
    for &x in xors {
        prev ^= x;
        out.push(prev);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xor_adjacent_basic() {
        let values = [0xFF00u64, 0xFF0F, 0x0000, 0xFFFF];
        let mut out = Vec::new();
        batch_xor_adjacent(&values, &mut out);
        assert_eq!(out, vec![0x000F, 0xFF0F, 0xFFFF]);
    }

    #[test]
    fn xor_adjacent_empty_and_single() {
        let mut out = Vec::new();
        batch_xor_adjacent(&[], &mut out);
        assert!(out.is_empty());
        batch_xor_adjacent(&[42], &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn xor_adjacent_identical() {
        let values = vec![123u64; 100];
        let mut out = Vec::new();
        batch_xor_adjacent(&values, &mut out);
        assert_eq!(out.len(), 99);
        assert!(out.iter().all(|&x| x == 0));
    }

    #[test]
    fn leading_zeros_basic() {
        let values = [0u64, 1, 0xFF, u64::MAX, 1u64 << 63];
        let mut out = Vec::new();
        batch_leading_zeros(&values, &mut out);
        assert_eq!(out, vec![64, 63, 56, 0, 0]);
    }

    #[test]
    fn trailing_zeros_basic() {
        let values = [0u64, 1, 0x100, 0x8000_0000_0000_0000, 6];
        let mut out = Vec::new();
        batch_trailing_zeros(&values, &mut out);
        assert_eq!(out, vec![64, 0, 8, 63, 1]);
    }

    #[test]
    fn f64_roundtrip() {
        let floats = [
            0.0,
            1.0,
            -1.0,
            std::f64::consts::PI,
            f64::NAN,
            f64::INFINITY,
        ];
        let mut bits = Vec::new();
        let mut recovered = Vec::new();
        batch_f64_to_bits(&floats, &mut bits);
        batch_bits_to_f64(&bits, &mut recovered);
        for (a, b) in floats.iter().zip(recovered.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn delta_basic() {
        let values = [100i64, 103, 107, 108, 108];
        let mut out = Vec::new();
        batch_delta(&values, &mut out);
        assert_eq!(out, vec![3, 4, 1, 0]);
    }

    #[test]
    fn delta_wrapping() {
        let values = [i64::MAX, i64::MIN];
        let mut out = Vec::new();
        batch_delta(&values, &mut out);
        assert_eq!(out, vec![1]); // wrapping: MIN - MAX = 1 in wrapping arithmetic
    }

    #[test]
    fn zigzag_roundtrip() {
        let signed = [0i64, 1, -1, 2, -2, i64::MAX, i64::MIN];
        let mut encoded = Vec::new();
        let mut decoded = Vec::new();
        batch_zigzag_encode(&signed, &mut encoded);
        batch_zigzag_decode(&encoded, &mut decoded);
        assert_eq!(&signed[..], &decoded[..]);
    }

    #[test]
    fn zigzag_known_values() {
        let mut out = Vec::new();
        batch_zigzag_encode(&[0, -1, 1, -2, 2], &mut out);
        assert_eq!(out, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn batch_xor_matches_scalar() {
        let values: Vec<u64> = (0..1000).map(|i| (i as f64 * 0.7).to_bits()).collect();
        let mut batch_result = Vec::new();
        batch_xor_adjacent(&values, &mut batch_result);

        let scalar: Vec<u64> = values.windows(2).map(|w| w[0] ^ w[1]).collect();
        assert_eq!(batch_result, scalar);
    }

    #[test]
    fn large_batch_leading_zeros() {
        let values: Vec<u64> = (0..10_000).map(|i| 1u64 << (i % 64)).collect();
        let mut out = Vec::new();
        batch_leading_zeros(&values, &mut out);
        for (i, &lz) in out.iter().enumerate() {
            assert_eq!(lz, values[i].leading_zeros(), "mismatch at index {i}");
        }
    }

    #[test]
    fn large_batch_trailing_zeros() {
        let values: Vec<u64> = (0..10_000).map(|i| 1u64 << (i % 64)).collect();
        let mut out = Vec::new();
        batch_trailing_zeros(&values, &mut out);
        for (i, &tz) in out.iter().enumerate() {
            assert_eq!(tz, values[i].trailing_zeros(), "mismatch at index {i}");
        }
    }

    #[test]
    fn prefix_sum_basic() {
        let deltas = [3i64, 4, 1, 0, -2];
        let mut out = Vec::new();
        batch_prefix_sum_i64(100, &deltas, &mut out);
        assert_eq!(out, vec![100, 103, 107, 108, 108, 106]);
    }

    #[test]
    fn prefix_sum_empty() {
        let mut out = Vec::new();
        batch_prefix_sum_i64(42, &[], &mut out);
        assert_eq!(out, vec![42]);
    }

    #[test]
    fn prefix_sum_wrapping() {
        let mut out = Vec::new();
        batch_prefix_sum_i64(i64::MAX, &[1], &mut out);
        assert_eq!(out, vec![i64::MAX, i64::MIN]);
    }

    #[test]
    fn add_scalar_i64_basic() {
        let offsets = [0u64, 1, 5, 100];
        let mut out = Vec::new();
        batch_add_scalar_i64(1000, &offsets, &mut out);
        assert_eq!(out, vec![1000, 1001, 1005, 1100]);
    }

    #[test]
    fn add_scalar_u64_basic() {
        let offsets = [0u64, 1, 5, 100];
        let mut out = Vec::new();
        batch_add_scalar_u64(1000, &offsets, &mut out);
        assert_eq!(out, vec![1000, 1001, 1005, 1100]);
    }

    #[test]
    fn xor_reconstruct_roundtrip() {
        let original = [0xABCDu64, 0xABCF, 0x0000, 0xFFFF];
        let mut xors = Vec::new();
        batch_xor_adjacent(&original, &mut xors);

        let mut reconstructed = Vec::new();
        batch_xor_reconstruct(original[0], &xors, &mut reconstructed);
        assert_eq!(&original[..], &reconstructed[..]);
    }

    #[test]
    fn delta_prefix_sum_roundtrip() {
        let original = [100i64, 103, 107, 108, 108, 50, -10];
        let mut deltas = Vec::new();
        batch_delta(&original, &mut deltas);

        let mut reconstructed = Vec::new();
        batch_prefix_sum_i64(original[0], &deltas, &mut reconstructed);
        assert_eq!(&original[..], &reconstructed[..]);
    }
}
