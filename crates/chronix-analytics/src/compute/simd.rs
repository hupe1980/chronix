//! Vectorized batch operations with multi-tier SIMD dispatch.
//!
//! Provides high-performance implementations for common statistical operations
//! (sum, mean, variance, min/max, dot product, standard deviation) with
//! automatic architecture-specific acceleration.
//!
//! ## SIMD dispatch hierarchy (x86_64)
//!
//! ```text
//!  AVX-512F (512-bit, 8×f64)  ← runtime-detected via is_x86_feature_detected!
//!      │  fallback
//!  AVX2+FMA (256-bit, 4×f64)  ← runtime-detected via is_x86_feature_detected!
//!      │  fallback
//!  SSE2 (128-bit, 2×f64)      ← baseline, always available on x86_64
//! ```
//!
//! ## SIMD dispatch (aarch64)
//!
//! NEON (128-bit, 2×f64) — baseline, always available on aarch64.
//!
//! ## Fallback
//!
//! Portable 4-wide scalar unroll with ILP on all other architectures.
//!
//! All tiers use **4 accumulators** to hide latency and maximize throughput.
//! AVX-512: 4 acc × 8 lanes = 32 elements/iteration.
//! AVX2: 4 acc × 4 lanes = 16 elements/iteration.
//! SSE2/NEON: 4 acc × 2 lanes = 8 elements/iteration.

// ═══════════════════════════════════════════════════════════════════════
// Horizontal reduction helpers (used by AVX2 and AVX-512 tiers)
//
// # Safety
//
// All functions in this section operate exclusively on SIMD register values
// passed by the caller. They contain no raw pointer dereferences, no memory
// accesses beyond the register file, and no side effects. Safety is
// guaranteed by:
//
// 1. The caller has verified the required CPU feature (via `#[target_feature]`
//    attribute or runtime `is_x86_feature_detected!()` check).
// 2. Inputs are `__m256d` / `__m512d` register values — not raw pointers.
// 3. All intrinsic calls are register-to-register (no loads/stores).
// ═══════════════════════════════════════════════════════════════════════

/// Horizontal sum of a 256-bit register (4×f64 → scalar).
///
/// # Safety
///
/// Caller must ensure AVX is available (e.g. via `is_x86_feature_detected!("avx2")`
/// or a `#[target_feature(enable = "avx2")]` annotation on the call site).
#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn hsum_256(v: std::arch::x86_64::__m256d) -> f64 {
    use std::arch::x86_64::*;
    // hi = [v2, v3], lo = [v0, v1]
    let hi = _mm256_extractf128_pd(v, 1);
    let lo = _mm256_castpd256_pd128(v);
    let sum128 = _mm_add_pd(lo, hi); // [v0+v2, v1+v3]
    let hi64 = _mm_unpackhi_pd(sum128, sum128);
    _mm_cvtsd_f64(_mm_add_sd(sum128, hi64))
}

/// Horizontal sum of a 512-bit register (8×f64 → scalar) via manual lane
/// extraction (avoids `_mm512_reduce_add_pd` which requires Rust ≥ 1.77).
///
/// # Safety
///
/// Caller must ensure AVX-512F is available.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
#[inline]
unsafe fn hsum_512(v: std::arch::x86_64::__m512d) -> f64 {
    use std::arch::x86_64::*;
    let lo = _mm512_castpd512_pd256(v);
    let hi = _mm512_extractf64x4_pd(v, 1);
    let sum256 = _mm256_add_pd(lo, hi);
    hsum_256(sum256)
}

/// Horizontal min of a 256-bit register (4×f64 → scalar).
///
/// # Safety
///
/// Caller must ensure AVX is available.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn hmin_256(v: std::arch::x86_64::__m256d) -> f64 {
    use std::arch::x86_64::*;
    let hi = _mm256_extractf128_pd(v, 1);
    let lo = _mm256_castpd256_pd128(v);
    let m = _mm_min_pd(lo, hi);
    let hi64 = _mm_unpackhi_pd(m, m);
    _mm_cvtsd_f64(_mm_min_sd(m, hi64))
}

/// Horizontal max of a 256-bit register (4×f64 → scalar).
///
/// # Safety
///
/// Caller must ensure AVX is available.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn hmax_256(v: std::arch::x86_64::__m256d) -> f64 {
    use std::arch::x86_64::*;
    let hi = _mm256_extractf128_pd(v, 1);
    let lo = _mm256_castpd256_pd128(v);
    let m = _mm_max_pd(lo, hi);
    let hi64 = _mm_unpackhi_pd(m, m);
    _mm_cvtsd_f64(_mm_max_sd(m, hi64))
}

/// Horizontal min of a 512-bit register (8×f64 → scalar).
///
/// # Safety
///
/// Caller must ensure AVX-512F is available.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
#[inline]
unsafe fn hmin_512(v: std::arch::x86_64::__m512d) -> f64 {
    use std::arch::x86_64::*;
    let lo = _mm512_castpd512_pd256(v);
    let hi = _mm512_extractf64x4_pd(v, 1);
    let m = _mm256_min_pd(lo, hi);
    hmin_256(m)
}

/// Horizontal max of a 512-bit register (8×f64 → scalar).
///
/// # Safety
///
/// Caller must ensure AVX-512F is available.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
#[inline]
unsafe fn hmax_512(v: std::arch::x86_64::__m512d) -> f64 {
    use std::arch::x86_64::*;
    let lo = _mm512_castpd512_pd256(v);
    let hi = _mm512_extractf64x4_pd(v, 1);
    let m = _mm256_max_pd(lo, hi);
    hmax_256(m)
}

// ═══════════════════════════════════════════════════════════════════════
// Portable scalar fallback (4-wide ILP unroll)
// ═══════════════════════════════════════════════════════════════════════

/// Portable 4-wide scalar sum with Kahan (compensated) summation.
#[inline]
#[allow(dead_code)]
fn scalar_sum(data: &[f64]) -> f64 {
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();
    let (mut a0, mut a1, mut a2, mut a3) = (0.0_f64, 0.0, 0.0, 0.0);
    let (mut c0, mut c1, mut c2, mut c3) = (0.0_f64, 0.0, 0.0, 0.0);
    for c in chunks {
        let y0 = c[0] - c0;
        let t0 = a0 + y0;
        c0 = (t0 - a0) - y0;
        a0 = t0;
        let y1 = c[1] - c1;
        let t1 = a1 + y1;
        c1 = (t1 - a1) - y1;
        a1 = t1;
        let y2 = c[2] - c2;
        let t2 = a2 + y2;
        c2 = (t2 - a2) - y2;
        a2 = t2;
        let y3 = c[3] - c3;
        let t3 = a3 + y3;
        c3 = (t3 - a3) - y3;
        a3 = t3;
    }
    // Merge 4 accumulators with Kahan reduction.
    let mut sum = 0.0_f64;
    let mut c = 0.0_f64;
    for &v in &[a0, a1, a2, a3] {
        let y = v - c;
        let t = sum + y;
        c = (t - sum) - y;
        sum = t;
    }
    for &v in remainder {
        let y = v - c;
        let t = sum + y;
        c = (t - sum) - y;
        sum = t;
    }
    sum
}

/// Kahan-compensated portable 4-wide scalar variance.
#[inline]
#[allow(dead_code)]
fn scalar_variance(data: &[f64], mean: f64) -> f64 {
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();
    let (mut a0, mut a1, mut a2, mut a3) = (0.0_f64, 0.0, 0.0, 0.0);
    // Kahan compensation terms for each accumulator lane.
    let (mut c0, mut c1, mut c2, mut c3) = (0.0_f64, 0.0, 0.0, 0.0);
    for c in chunks {
        let (d0, d1, d2, d3) = (c[0] - mean, c[1] - mean, c[2] - mean, c[3] - mean);
        let y0 = d0 * d0 - c0;
        let t0 = a0 + y0;
        c0 = (t0 - a0) - y0;
        a0 = t0;
        let y1 = d1 * d1 - c1;
        let t1 = a1 + y1;
        c1 = (t1 - a1) - y1;
        a1 = t1;
        let y2 = d2 * d2 - c2;
        let t2 = a2 + y2;
        c2 = (t2 - a2) - y2;
        a2 = t2;
        let y3 = d3 * d3 - c3;
        let t3 = a3 + y3;
        c3 = (t3 - a3) - y3;
        a3 = t3;
    }
    let mut s = (a0 + a1) + (a2 + a3);
    let mut cs = (c0 + c1) + (c2 + c3);
    for &v in remainder {
        let d = v - mean;
        let y = d * d - cs;
        let t = s + y;
        cs = (t - s) - y;
        s = t;
    }
    s / data.len() as f64
}

/// Portable scalar min/max.
#[inline]
#[allow(dead_code)]
fn scalar_min_max(data: &[f64]) -> (f64, f64) {
    let mut min_val = f64::INFINITY;
    let mut max_val = f64::NEG_INFINITY;
    for &v in data {
        if v < min_val {
            min_val = v;
        }
        if v > max_val {
            max_val = v;
        }
    }
    (min_val, max_val)
}

/// Portable scalar dot product.
#[inline]
#[allow(dead_code)]
fn scalar_dot_product(a: &[f64], b: &[f64]) -> f64 {
    let chunks = a.len() / 4;
    let (mut a0, mut a1, mut a2, mut a3) = (0.0_f64, 0.0, 0.0, 0.0);
    for i in 0..chunks {
        let base = i * 4;
        a0 += a[base] * b[base];
        a1 += a[base + 1] * b[base + 1];
        a2 += a[base + 2] * b[base + 2];
        a3 += a[base + 3] * b[base + 3];
    }
    let mut t = (a0 + a1) + (a2 + a3);
    for i in (chunks * 4)..a.len() {
        t += a[i] * b[i];
    }
    t
}

// ═══════════════════════════════════════════════════════════════════════
// x86_64 SIMD tiers: AVX-512F → AVX2 → SSE2
// ═══════════════════════════════════════════════════════════════════════

// ── AVX-512F (8×f64, 32 elements/iteration) ──────────────────────────
//
// # Safety (applies to all avx512_* functions)
//
// These functions are marked `unsafe` because they use AVX-512F SIMD intrinsics.
// Safety is guaranteed by:
// 1. `#[target_feature(enable = "avx512f")]` — the compiler enforces that these
//    functions are only called (directly or transitively) from contexts where
//    AVX-512F is enabled, or via an unsafe block with a runtime feature check.
// 2. All `_mm512_loadu_pd(ptr.add(offset))` calls are bounded: loop indices are
//    derived from `data.len() / STRIDE` so `offset < data.len()` always holds.
// 3. Remainder elements are processed via safe scalar iteration.

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn avx512_sum(data: &[f64]) -> f64 {
    use std::arch::x86_64::*;
    let mut acc0 = _mm512_setzero_pd();
    let mut acc1 = _mm512_setzero_pd();
    let mut acc2 = _mm512_setzero_pd();
    let mut acc3 = _mm512_setzero_pd();
    // Kahan compensation vectors.
    let mut comp0 = _mm512_setzero_pd();
    let mut comp1 = _mm512_setzero_pd();
    let mut comp2 = _mm512_setzero_pd();
    let mut comp3 = _mm512_setzero_pd();
    let chunks = data.len() / 32;
    let ptr = data.as_ptr();
    for i in 0..chunks {
        let b = i * 32;
        let v0 = _mm512_loadu_pd(ptr.add(b));
        let y0 = _mm512_sub_pd(v0, comp0);
        let t0 = _mm512_add_pd(acc0, y0);
        comp0 = _mm512_sub_pd(_mm512_sub_pd(t0, acc0), y0);
        acc0 = t0;

        let v1 = _mm512_loadu_pd(ptr.add(b + 8));
        let y1 = _mm512_sub_pd(v1, comp1);
        let t1 = _mm512_add_pd(acc1, y1);
        comp1 = _mm512_sub_pd(_mm512_sub_pd(t1, acc1), y1);
        acc1 = t1;

        let v2 = _mm512_loadu_pd(ptr.add(b + 16));
        let y2 = _mm512_sub_pd(v2, comp2);
        let t2 = _mm512_add_pd(acc2, y2);
        comp2 = _mm512_sub_pd(_mm512_sub_pd(t2, acc2), y2);
        acc2 = t2;

        let v3 = _mm512_loadu_pd(ptr.add(b + 24));
        let y3 = _mm512_sub_pd(v3, comp3);
        let t3 = _mm512_add_pd(acc3, y3);
        comp3 = _mm512_sub_pd(_mm512_sub_pd(t3, acc3), y3);
        acc3 = t3;
    }
    let sum = _mm512_add_pd(_mm512_add_pd(acc0, acc1), _mm512_add_pd(acc2, acc3));
    let mut total = hsum_512(sum);
    // Kahan tail for remainder elements.
    let mut c = 0.0_f64;
    for &v in &data[chunks * 32..] {
        let y = v - c;
        let t = total + y;
        c = (t - total) - y;
        total = t;
    }
    total
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn avx512_variance(data: &[f64], mean: f64) -> f64 {
    use std::arch::x86_64::*;
    let vmean = _mm512_set1_pd(mean);
    let mut acc0 = _mm512_setzero_pd();
    let mut acc1 = _mm512_setzero_pd();
    let mut acc2 = _mm512_setzero_pd();
    let mut acc3 = _mm512_setzero_pd();
    let chunks = data.len() / 32;
    let ptr = data.as_ptr();
    for i in 0..chunks {
        let b = i * 32;
        let d0 = _mm512_sub_pd(_mm512_loadu_pd(ptr.add(b)), vmean);
        let d1 = _mm512_sub_pd(_mm512_loadu_pd(ptr.add(b + 8)), vmean);
        let d2 = _mm512_sub_pd(_mm512_loadu_pd(ptr.add(b + 16)), vmean);
        let d3 = _mm512_sub_pd(_mm512_loadu_pd(ptr.add(b + 24)), vmean);
        acc0 = _mm512_fmadd_pd(d0, d0, acc0);
        acc1 = _mm512_fmadd_pd(d1, d1, acc1);
        acc2 = _mm512_fmadd_pd(d2, d2, acc2);
        acc3 = _mm512_fmadd_pd(d3, d3, acc3);
    }
    let sum = _mm512_add_pd(_mm512_add_pd(acc0, acc1), _mm512_add_pd(acc2, acc3));
    let mut total = hsum_512(sum);
    for &v in &data[chunks * 32..] {
        let d = v - mean;
        total += d * d;
    }
    total / data.len() as f64
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn avx512_min_max(data: &[f64]) -> (f64, f64) {
    use std::arch::x86_64::*;
    let mut vmin0 = _mm512_set1_pd(f64::INFINITY);
    let mut vmax0 = _mm512_set1_pd(f64::NEG_INFINITY);
    let mut vmin1 = vmin0;
    let mut vmax1 = vmax0;
    let chunks = data.len() / 16;
    let ptr = data.as_ptr();
    for i in 0..chunks {
        let b = i * 16;
        let a = _mm512_loadu_pd(ptr.add(b));
        let c = _mm512_loadu_pd(ptr.add(b + 8));
        vmin0 = _mm512_min_pd(a, vmin0);
        vmax0 = _mm512_max_pd(a, vmax0);
        vmin1 = _mm512_min_pd(c, vmin1);
        vmax1 = _mm512_max_pd(c, vmax1);
    }
    let vmin = _mm512_min_pd(vmin0, vmin1);
    let vmax = _mm512_max_pd(vmax0, vmax1);
    let mut min_val = hmin_512(vmin);
    let mut max_val = hmax_512(vmax);
    for &v in &data[chunks * 16..] {
        if v < min_val {
            min_val = v;
        }
        if v > max_val {
            max_val = v;
        }
    }
    (min_val, max_val)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn avx512_dot_product(a: &[f64], b: &[f64]) -> f64 {
    use std::arch::x86_64::*;
    let mut acc0 = _mm512_setzero_pd();
    let mut acc1 = _mm512_setzero_pd();
    let mut acc2 = _mm512_setzero_pd();
    let mut acc3 = _mm512_setzero_pd();
    let chunks = a.len() / 32;
    let pa = a.as_ptr();
    let pb = b.as_ptr();
    for i in 0..chunks {
        let base = i * 32;
        acc0 = _mm512_fmadd_pd(
            _mm512_loadu_pd(pa.add(base)),
            _mm512_loadu_pd(pb.add(base)),
            acc0,
        );
        acc1 = _mm512_fmadd_pd(
            _mm512_loadu_pd(pa.add(base + 8)),
            _mm512_loadu_pd(pb.add(base + 8)),
            acc1,
        );
        acc2 = _mm512_fmadd_pd(
            _mm512_loadu_pd(pa.add(base + 16)),
            _mm512_loadu_pd(pb.add(base + 16)),
            acc2,
        );
        acc3 = _mm512_fmadd_pd(
            _mm512_loadu_pd(pa.add(base + 24)),
            _mm512_loadu_pd(pb.add(base + 24)),
            acc3,
        );
    }
    let sum = _mm512_add_pd(_mm512_add_pd(acc0, acc1), _mm512_add_pd(acc2, acc3));
    let mut total = hsum_512(sum);
    for i in (chunks * 32)..a.len() {
        total += a[i] * b[i];
    }
    total
}

// ── AVX2+FMA (4×f64, 16 elements/iteration) ─────────────────────────
//
// # Safety (applies to all avx2_* functions)
//
// Same safety guarantees as the AVX-512F tier above:
// 1. `#[target_feature(enable = "avx2,fma")]` ensures the CPU supports the
//    intrinsics used.
// 2. All `_mm256_loadu_pd(ptr.add(offset))` calls are bounds-checked via
//    loop limits derived from `data.len() / STRIDE`.
// 3. Remainder elements processed via safe scalar iteration.

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn avx2_sum(data: &[f64]) -> f64 {
    use std::arch::x86_64::*;
    let mut acc0 = _mm256_setzero_pd();
    let mut acc1 = _mm256_setzero_pd();
    let mut acc2 = _mm256_setzero_pd();
    let mut acc3 = _mm256_setzero_pd();
    // Kahan compensation vectors.
    let mut comp0 = _mm256_setzero_pd();
    let mut comp1 = _mm256_setzero_pd();
    let mut comp2 = _mm256_setzero_pd();
    let mut comp3 = _mm256_setzero_pd();
    let chunks = data.len() / 16;
    let ptr = data.as_ptr();
    for i in 0..chunks {
        let b = i * 16;
        let v0 = _mm256_loadu_pd(ptr.add(b));
        let y0 = _mm256_sub_pd(v0, comp0);
        let t0 = _mm256_add_pd(acc0, y0);
        comp0 = _mm256_sub_pd(_mm256_sub_pd(t0, acc0), y0);
        acc0 = t0;

        let v1 = _mm256_loadu_pd(ptr.add(b + 4));
        let y1 = _mm256_sub_pd(v1, comp1);
        let t1 = _mm256_add_pd(acc1, y1);
        comp1 = _mm256_sub_pd(_mm256_sub_pd(t1, acc1), y1);
        acc1 = t1;

        let v2 = _mm256_loadu_pd(ptr.add(b + 8));
        let y2 = _mm256_sub_pd(v2, comp2);
        let t2 = _mm256_add_pd(acc2, y2);
        comp2 = _mm256_sub_pd(_mm256_sub_pd(t2, acc2), y2);
        acc2 = t2;

        let v3 = _mm256_loadu_pd(ptr.add(b + 12));
        let y3 = _mm256_sub_pd(v3, comp3);
        let t3 = _mm256_add_pd(acc3, y3);
        comp3 = _mm256_sub_pd(_mm256_sub_pd(t3, acc3), y3);
        acc3 = t3;
    }
    let sum = _mm256_add_pd(_mm256_add_pd(acc0, acc1), _mm256_add_pd(acc2, acc3));
    let mut total = hsum_256(sum);
    // Kahan tail for remainder elements.
    let mut c = 0.0_f64;
    for &v in &data[chunks * 16..] {
        let y = v - c;
        let t = total + y;
        c = (t - total) - y;
        total = t;
    }
    total
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn avx2_variance(data: &[f64], mean: f64) -> f64 {
    use std::arch::x86_64::*;
    let vmean = _mm256_set1_pd(mean);
    let mut acc0 = _mm256_setzero_pd();
    let mut acc1 = _mm256_setzero_pd();
    let mut acc2 = _mm256_setzero_pd();
    let mut acc3 = _mm256_setzero_pd();
    let chunks = data.len() / 16;
    let ptr = data.as_ptr();
    for i in 0..chunks {
        let b = i * 16;
        let d0 = _mm256_sub_pd(_mm256_loadu_pd(ptr.add(b)), vmean);
        let d1 = _mm256_sub_pd(_mm256_loadu_pd(ptr.add(b + 4)), vmean);
        let d2 = _mm256_sub_pd(_mm256_loadu_pd(ptr.add(b + 8)), vmean);
        let d3 = _mm256_sub_pd(_mm256_loadu_pd(ptr.add(b + 12)), vmean);
        acc0 = _mm256_fmadd_pd(d0, d0, acc0);
        acc1 = _mm256_fmadd_pd(d1, d1, acc1);
        acc2 = _mm256_fmadd_pd(d2, d2, acc2);
        acc3 = _mm256_fmadd_pd(d3, d3, acc3);
    }
    let sum = _mm256_add_pd(_mm256_add_pd(acc0, acc1), _mm256_add_pd(acc2, acc3));
    let mut total = hsum_256(sum);
    for &v in &data[chunks * 16..] {
        let d = v - mean;
        total += d * d;
    }
    total / data.len() as f64
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_min_max(data: &[f64]) -> (f64, f64) {
    use std::arch::x86_64::*;
    let mut vmin0 = _mm256_set1_pd(f64::INFINITY);
    let mut vmax0 = _mm256_set1_pd(f64::NEG_INFINITY);
    let mut vmin1 = vmin0;
    let mut vmax1 = vmax0;
    let chunks = data.len() / 8;
    let ptr = data.as_ptr();
    for i in 0..chunks {
        let b = i * 8;
        let a = _mm256_loadu_pd(ptr.add(b));
        let c = _mm256_loadu_pd(ptr.add(b + 4));
        vmin0 = _mm256_min_pd(a, vmin0);
        vmax0 = _mm256_max_pd(a, vmax0);
        vmin1 = _mm256_min_pd(c, vmin1);
        vmax1 = _mm256_max_pd(c, vmax1);
    }
    let vmin = _mm256_min_pd(vmin0, vmin1);
    let vmax = _mm256_max_pd(vmax0, vmax1);
    let mut min_val = hmin_256(vmin);
    let mut max_val = hmax_256(vmax);
    for &v in &data[chunks * 8..] {
        if v < min_val {
            min_val = v;
        }
        if v > max_val {
            max_val = v;
        }
    }
    (min_val, max_val)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn avx2_dot_product(a: &[f64], b: &[f64]) -> f64 {
    use std::arch::x86_64::*;
    let mut acc0 = _mm256_setzero_pd();
    let mut acc1 = _mm256_setzero_pd();
    let mut acc2 = _mm256_setzero_pd();
    let mut acc3 = _mm256_setzero_pd();
    let chunks = a.len() / 16;
    let pa = a.as_ptr();
    let pb = b.as_ptr();
    for i in 0..chunks {
        let base = i * 16;
        acc0 = _mm256_fmadd_pd(
            _mm256_loadu_pd(pa.add(base)),
            _mm256_loadu_pd(pb.add(base)),
            acc0,
        );
        acc1 = _mm256_fmadd_pd(
            _mm256_loadu_pd(pa.add(base + 4)),
            _mm256_loadu_pd(pb.add(base + 4)),
            acc1,
        );
        acc2 = _mm256_fmadd_pd(
            _mm256_loadu_pd(pa.add(base + 8)),
            _mm256_loadu_pd(pb.add(base + 8)),
            acc2,
        );
        acc3 = _mm256_fmadd_pd(
            _mm256_loadu_pd(pa.add(base + 12)),
            _mm256_loadu_pd(pb.add(base + 12)),
            acc3,
        );
    }
    let sum = _mm256_add_pd(_mm256_add_pd(acc0, acc1), _mm256_add_pd(acc2, acc3));
    let mut total = hsum_256(sum);
    for i in (chunks * 16)..a.len() {
        total += a[i] * b[i];
    }
    total
}

// ── SSE2 (2×f64, 8 elements/iteration — x86_64 baseline) ────────────

#[cfg(target_arch = "x86_64")]
#[inline]
fn sse2_sum(data: &[f64]) -> f64 {
    use std::arch::x86_64::*;
    // SAFETY: SSE2 is guaranteed on all x86_64 CPUs.
    unsafe {
        let mut acc0 = _mm_setzero_pd();
        let mut acc1 = _mm_setzero_pd();
        let mut acc2 = _mm_setzero_pd();
        let mut acc3 = _mm_setzero_pd();
        // Kahan compensation vectors.
        let mut comp0 = _mm_setzero_pd();
        let mut comp1 = _mm_setzero_pd();
        let mut comp2 = _mm_setzero_pd();
        let mut comp3 = _mm_setzero_pd();
        let chunks = data.len() / 8;
        let ptr = data.as_ptr();
        for i in 0..chunks {
            let b = i * 8;
            let v0 = _mm_loadu_pd(ptr.add(b));
            let y0 = _mm_sub_pd(v0, comp0);
            let t0 = _mm_add_pd(acc0, y0);
            comp0 = _mm_sub_pd(_mm_sub_pd(t0, acc0), y0);
            acc0 = t0;

            let v1 = _mm_loadu_pd(ptr.add(b + 2));
            let y1 = _mm_sub_pd(v1, comp1);
            let t1 = _mm_add_pd(acc1, y1);
            comp1 = _mm_sub_pd(_mm_sub_pd(t1, acc1), y1);
            acc1 = t1;

            let v2 = _mm_loadu_pd(ptr.add(b + 4));
            let y2 = _mm_sub_pd(v2, comp2);
            let t2 = _mm_add_pd(acc2, y2);
            comp2 = _mm_sub_pd(_mm_sub_pd(t2, acc2), y2);
            acc2 = t2;

            let v3 = _mm_loadu_pd(ptr.add(b + 6));
            let y3 = _mm_sub_pd(v3, comp3);
            let t3 = _mm_add_pd(acc3, y3);
            comp3 = _mm_sub_pd(_mm_sub_pd(t3, acc3), y3);
            acc3 = t3;
        }
        let sum = _mm_add_pd(_mm_add_pd(acc0, acc1), _mm_add_pd(acc2, acc3));
        let mut total = _mm_cvtsd_f64(sum) + _mm_cvtsd_f64(_mm_unpackhi_pd(sum, sum));
        // Kahan tail for remainder elements.
        let mut c = 0.0_f64;
        for &v in &data[chunks * 8..] {
            let y = v - c;
            let t = total + y;
            c = (t - total) - y;
            total = t;
        }
        total
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn sse2_variance(data: &[f64], mean: f64) -> f64 {
    use std::arch::x86_64::*;
    // SAFETY: SSE2 is guaranteed on all x86_64 CPUs.
    unsafe {
        let vmean = _mm_set1_pd(mean);
        let mut acc0 = _mm_setzero_pd();
        let mut acc1 = _mm_setzero_pd();
        let mut acc2 = _mm_setzero_pd();
        let mut acc3 = _mm_setzero_pd();
        let chunks = data.len() / 8;
        let ptr = data.as_ptr();
        for i in 0..chunks {
            let b = i * 8;
            let d0 = _mm_sub_pd(_mm_loadu_pd(ptr.add(b)), vmean);
            let d1 = _mm_sub_pd(_mm_loadu_pd(ptr.add(b + 2)), vmean);
            let d2 = _mm_sub_pd(_mm_loadu_pd(ptr.add(b + 4)), vmean);
            let d3 = _mm_sub_pd(_mm_loadu_pd(ptr.add(b + 6)), vmean);
            acc0 = _mm_add_pd(acc0, _mm_mul_pd(d0, d0));
            acc1 = _mm_add_pd(acc1, _mm_mul_pd(d1, d1));
            acc2 = _mm_add_pd(acc2, _mm_mul_pd(d2, d2));
            acc3 = _mm_add_pd(acc3, _mm_mul_pd(d3, d3));
        }
        let sum = _mm_add_pd(_mm_add_pd(acc0, acc1), _mm_add_pd(acc2, acc3));
        let mut total = _mm_cvtsd_f64(sum) + _mm_cvtsd_f64(_mm_unpackhi_pd(sum, sum));
        for &v in &data[chunks * 8..] {
            let d = v - mean;
            total += d * d;
        }
        total / data.len() as f64
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn sse2_min_max(data: &[f64]) -> (f64, f64) {
    use std::arch::x86_64::*;
    // SAFETY: SSE2 is guaranteed on all x86_64 CPUs.
    unsafe {
        let mut vmin0 = _mm_set1_pd(f64::INFINITY);
        let mut vmax0 = _mm_set1_pd(f64::NEG_INFINITY);
        let mut vmin1 = vmin0;
        let mut vmax1 = vmax0;
        let chunks = data.len() / 4;
        let ptr = data.as_ptr();
        for i in 0..chunks {
            let b = i * 4;
            let a = _mm_loadu_pd(ptr.add(b));
            let c = _mm_loadu_pd(ptr.add(b + 2));
            vmin0 = _mm_min_pd(a, vmin0);
            vmax0 = _mm_max_pd(a, vmax0);
            vmin1 = _mm_min_pd(c, vmin1);
            vmax1 = _mm_max_pd(c, vmax1);
        }
        let vmin = _mm_min_pd(vmin0, vmin1);
        let vmax = _mm_max_pd(vmax0, vmax1);
        let mut min_val = f64::min(
            _mm_cvtsd_f64(vmin),
            _mm_cvtsd_f64(_mm_unpackhi_pd(vmin, vmin)),
        );
        let mut max_val = f64::max(
            _mm_cvtsd_f64(vmax),
            _mm_cvtsd_f64(_mm_unpackhi_pd(vmax, vmax)),
        );
        for &v in &data[chunks * 4..] {
            if v < min_val {
                min_val = v;
            }
            if v > max_val {
                max_val = v;
            }
        }
        (min_val, max_val)
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn sse2_dot_product(a: &[f64], b: &[f64]) -> f64 {
    use std::arch::x86_64::*;
    // SAFETY: SSE2 is guaranteed on all x86_64 CPUs.
    unsafe {
        let mut acc0 = _mm_setzero_pd();
        let mut acc1 = _mm_setzero_pd();
        let mut acc2 = _mm_setzero_pd();
        let mut acc3 = _mm_setzero_pd();
        let chunks = a.len() / 8;
        let pa = a.as_ptr();
        let pb = b.as_ptr();
        for i in 0..chunks {
            let base = i * 8;
            let va0 = _mm_loadu_pd(pa.add(base));
            let vb0 = _mm_loadu_pd(pb.add(base));
            let va1 = _mm_loadu_pd(pa.add(base + 2));
            let vb1 = _mm_loadu_pd(pb.add(base + 2));
            let va2 = _mm_loadu_pd(pa.add(base + 4));
            let vb2 = _mm_loadu_pd(pb.add(base + 4));
            let va3 = _mm_loadu_pd(pa.add(base + 6));
            let vb3 = _mm_loadu_pd(pb.add(base + 6));
            acc0 = _mm_add_pd(acc0, _mm_mul_pd(va0, vb0));
            acc1 = _mm_add_pd(acc1, _mm_mul_pd(va1, vb1));
            acc2 = _mm_add_pd(acc2, _mm_mul_pd(va2, vb2));
            acc3 = _mm_add_pd(acc3, _mm_mul_pd(va3, vb3));
        }
        let sum = _mm_add_pd(_mm_add_pd(acc0, acc1), _mm_add_pd(acc2, acc3));
        let mut total = _mm_cvtsd_f64(sum) + _mm_cvtsd_f64(_mm_unpackhi_pd(sum, sum));
        for i in (chunks * 8)..a.len() {
            total += a[i] * b[i];
        }
        total
    }
}

// ── x86_64 runtime dispatch ──────────────────────────────────────────

#[cfg(target_arch = "x86_64")]
#[inline]
fn arch_simd_sum(data: &[f64]) -> f64 {
    if is_x86_feature_detected!("avx512f") {
        // SAFETY: AVX-512F detected at runtime.
        return unsafe { avx512_sum(data) };
    }
    if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
        // SAFETY: AVX2+FMA detected at runtime.
        return unsafe { avx2_sum(data) };
    }
    sse2_sum(data)
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn arch_simd_variance(data: &[f64], mean: f64) -> f64 {
    if is_x86_feature_detected!("avx512f") {
        // SAFETY: AVX-512F detected at runtime.
        return unsafe { avx512_variance(data, mean) };
    }
    if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
        // SAFETY: AVX2+FMA detected at runtime.
        return unsafe { avx2_variance(data, mean) };
    }
    sse2_variance(data, mean)
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn arch_simd_min_max(data: &[f64]) -> (f64, f64) {
    if is_x86_feature_detected!("avx512f") {
        // SAFETY: AVX-512F detected at runtime.
        return unsafe { avx512_min_max(data) };
    }
    if is_x86_feature_detected!("avx2") {
        // SAFETY: AVX2 detected at runtime.
        return unsafe { avx2_min_max(data) };
    }
    sse2_min_max(data)
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn arch_simd_dot_product(a: &[f64], b: &[f64]) -> f64 {
    if is_x86_feature_detected!("avx512f") {
        // SAFETY: AVX-512F detected at runtime.
        return unsafe { avx512_dot_product(a, b) };
    }
    if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
        // SAFETY: AVX2+FMA detected at runtime.
        return unsafe { avx2_dot_product(a, b) };
    }
    sse2_dot_product(a, b)
}

// ═══════════════════════════════════════════════════════════════════════
// aarch64 NEON (2×f64, 8 elements/iteration — baseline)
// ═══════════════════════════════════════════════════════════════════════

#[cfg(target_arch = "aarch64")]
#[inline]
fn arch_simd_sum(data: &[f64]) -> f64 {
    use std::arch::aarch64::*;
    // SAFETY: NEON is guaranteed on all aarch64 CPUs.
    unsafe {
        let mut acc0 = vdupq_n_f64(0.0);
        let mut acc1 = vdupq_n_f64(0.0);
        let mut acc2 = vdupq_n_f64(0.0);
        let mut acc3 = vdupq_n_f64(0.0);
        // Kahan compensation vectors.
        let mut comp0 = vdupq_n_f64(0.0);
        let mut comp1 = vdupq_n_f64(0.0);
        let mut comp2 = vdupq_n_f64(0.0);
        let mut comp3 = vdupq_n_f64(0.0);
        let chunks = data.len() / 8;
        let ptr = data.as_ptr();
        for i in 0..chunks {
            let b = i * 8;
            let v0 = vld1q_f64(ptr.add(b));
            let y0 = vsubq_f64(v0, comp0);
            let t0 = vaddq_f64(acc0, y0);
            comp0 = vsubq_f64(vsubq_f64(t0, acc0), y0);
            acc0 = t0;

            let v1 = vld1q_f64(ptr.add(b + 2));
            let y1 = vsubq_f64(v1, comp1);
            let t1 = vaddq_f64(acc1, y1);
            comp1 = vsubq_f64(vsubq_f64(t1, acc1), y1);
            acc1 = t1;

            let v2 = vld1q_f64(ptr.add(b + 4));
            let y2 = vsubq_f64(v2, comp2);
            let t2 = vaddq_f64(acc2, y2);
            comp2 = vsubq_f64(vsubq_f64(t2, acc2), y2);
            acc2 = t2;

            let v3 = vld1q_f64(ptr.add(b + 6));
            let y3 = vsubq_f64(v3, comp3);
            let t3 = vaddq_f64(acc3, y3);
            comp3 = vsubq_f64(vsubq_f64(t3, acc3), y3);
            acc3 = t3;
        }
        let sum = vaddq_f64(vaddq_f64(acc0, acc1), vaddq_f64(acc2, acc3));
        let mut total = vgetq_lane_f64(sum, 0) + vgetq_lane_f64(sum, 1);
        // Kahan tail for remainder elements.
        let mut c = 0.0_f64;
        for &v in &data[chunks * 8..] {
            let y = v - c;
            let t = total + y;
            c = (t - total) - y;
            total = t;
        }
        total
    }
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn arch_simd_variance(data: &[f64], mean: f64) -> f64 {
    use std::arch::aarch64::*;
    // SAFETY: NEON is guaranteed on all aarch64 CPUs.
    unsafe {
        let vmean = vdupq_n_f64(mean);
        let mut acc0 = vdupq_n_f64(0.0);
        let mut acc1 = vdupq_n_f64(0.0);
        let mut acc2 = vdupq_n_f64(0.0);
        let mut acc3 = vdupq_n_f64(0.0);
        let chunks = data.len() / 8;
        let ptr = data.as_ptr();
        for i in 0..chunks {
            let b = i * 8;
            let d0 = vsubq_f64(vld1q_f64(ptr.add(b)), vmean);
            let d1 = vsubq_f64(vld1q_f64(ptr.add(b + 2)), vmean);
            let d2 = vsubq_f64(vld1q_f64(ptr.add(b + 4)), vmean);
            let d3 = vsubq_f64(vld1q_f64(ptr.add(b + 6)), vmean);
            acc0 = vaddq_f64(acc0, vmulq_f64(d0, d0));
            acc1 = vaddq_f64(acc1, vmulq_f64(d1, d1));
            acc2 = vaddq_f64(acc2, vmulq_f64(d2, d2));
            acc3 = vaddq_f64(acc3, vmulq_f64(d3, d3));
        }
        let sum = vaddq_f64(vaddq_f64(acc0, acc1), vaddq_f64(acc2, acc3));
        let mut total = vgetq_lane_f64(sum, 0) + vgetq_lane_f64(sum, 1);
        for &v in &data[chunks * 8..] {
            let d = v - mean;
            total += d * d;
        }
        total / data.len() as f64
    }
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn arch_simd_min_max(data: &[f64]) -> (f64, f64) {
    use std::arch::aarch64::*;
    // SAFETY: NEON is guaranteed on all aarch64 CPUs.
    unsafe {
        // Use vminnmq_f64 / vmaxnmq_f64 (FMINNM / FMAXNM) which skip NaN,
        // matching x86 _mm*_min_pd / _mm*_max_pd semantics.
        let mut vmin0 = vdupq_n_f64(f64::INFINITY);
        let mut vmax0 = vdupq_n_f64(f64::NEG_INFINITY);
        let mut vmin1 = vmin0;
        let mut vmax1 = vmax0;
        let chunks = data.len() / 4;
        let ptr = data.as_ptr();
        for i in 0..chunks {
            let b = i * 4;
            let a = vld1q_f64(ptr.add(b));
            let c = vld1q_f64(ptr.add(b + 2));
            vmin0 = vminnmq_f64(vmin0, a);
            vmax0 = vmaxnmq_f64(vmax0, a);
            vmin1 = vminnmq_f64(vmin1, c);
            vmax1 = vmaxnmq_f64(vmax1, c);
        }
        let vmin = vminnmq_f64(vmin0, vmin1);
        let vmax = vmaxnmq_f64(vmax0, vmax1);
        let mut min_val = f64::min(vgetq_lane_f64(vmin, 0), vgetq_lane_f64(vmin, 1));
        let mut max_val = f64::max(vgetq_lane_f64(vmax, 0), vgetq_lane_f64(vmax, 1));
        for &v in &data[chunks * 4..] {
            if v < min_val {
                min_val = v;
            }
            if v > max_val {
                max_val = v;
            }
        }
        (min_val, max_val)
    }
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn arch_simd_dot_product(a: &[f64], b: &[f64]) -> f64 {
    use std::arch::aarch64::*;
    // SAFETY: NEON is guaranteed on all aarch64 CPUs.
    unsafe {
        let mut acc0 = vdupq_n_f64(0.0);
        let mut acc1 = vdupq_n_f64(0.0);
        let mut acc2 = vdupq_n_f64(0.0);
        let mut acc3 = vdupq_n_f64(0.0);
        let chunks = a.len() / 8;
        let pa = a.as_ptr();
        let pb = b.as_ptr();
        for i in 0..chunks {
            let base = i * 8;
            acc0 = vfmaq_f64(acc0, vld1q_f64(pa.add(base)), vld1q_f64(pb.add(base)));
            acc1 = vfmaq_f64(
                acc1,
                vld1q_f64(pa.add(base + 2)),
                vld1q_f64(pb.add(base + 2)),
            );
            acc2 = vfmaq_f64(
                acc2,
                vld1q_f64(pa.add(base + 4)),
                vld1q_f64(pb.add(base + 4)),
            );
            acc3 = vfmaq_f64(
                acc3,
                vld1q_f64(pa.add(base + 6)),
                vld1q_f64(pb.add(base + 6)),
            );
        }
        let sum = vaddq_f64(vaddq_f64(acc0, acc1), vaddq_f64(acc2, acc3));
        let mut total = vgetq_lane_f64(sum, 0) + vgetq_lane_f64(sum, 1);
        for i in (chunks * 8)..a.len() {
            total += a[i] * b[i];
        }
        total
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Scalar fallback dispatch (non-x86_64, non-aarch64)
// ═══════════════════════════════════════════════════════════════════════

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline]
fn arch_simd_sum(data: &[f64]) -> f64 {
    scalar_sum(data)
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline]
fn arch_simd_variance(data: &[f64], mean: f64) -> f64 {
    scalar_variance(data, mean)
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline]
fn arch_simd_min_max(data: &[f64]) -> (f64, f64) {
    scalar_min_max(data)
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline]
fn arch_simd_dot_product(a: &[f64], b: &[f64]) -> f64 {
    scalar_dot_product(a, b)
}

// ═══════════════════════════════════════════════════════════════════════
// Public API
// ═══════════════════════════════════════════════════════════════════════

/// Computes the sum of a slice with multi-tier SIMD acceleration and
/// Kahan (compensated) summation for numerical stability.
///
/// On x86_64: runtime-detects AVX-512F → AVX2+FMA → SSE2 (baseline).
/// On aarch64: uses NEON intrinsics.
/// Elsewhere:  portable 4-wide scalar ILP unroll.
///
/// Each tier maintains per-lane compensation vectors that track
/// accumulated rounding error, dramatically reducing the O(n·ε²) error
/// of naive pairwise summation to O(ε) regardless of input length.
///
/// # Examples
///
/// ```no_run
/// use chronix_analytics::compute::simd_sum;
/// assert!((simd_sum(&[1.0, 2.0, 3.0, 4.0]) - 10.0).abs() < 1e-10);
/// assert_eq!(simd_sum(&[]), 0.0);
/// ```
pub fn simd_sum(data: &[f64]) -> f64 {
    if data.is_empty() {
        return 0.0;
    }
    arch_simd_sum(data)
}

/// Computes the arithmetic mean of a slice.
///
/// Returns `f64::NAN` for empty slices.
///
/// # Examples
///
/// ```no_run
/// use chronix_analytics::compute::simd_mean;
/// assert!((simd_mean(&[1.0, 2.0, 3.0, 4.0]) - 2.5).abs() < 1e-10);
/// assert!(simd_mean(&[]).is_nan());
/// ```
pub fn simd_mean(data: &[f64]) -> f64 {
    if data.is_empty() {
        return f64::NAN;
    }
    simd_sum(data) / data.len() as f64
}

/// Computes the population variance given a precomputed mean.
///
/// On x86_64 with AVX2/AVX-512: uses FMA (`_mm256_fmadd_pd` / `_mm512_fmadd_pd`)
/// for fused multiply-add in the squared-difference accumulation.
///
/// # Examples
///
/// ```no_run
/// use chronix_analytics::compute::{simd_mean, simd_variance};
/// let data = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
/// let mean = simd_mean(&data);
/// let var = simd_variance(&data, mean);
/// assert!((var - 32.0 / 7.0).abs() < 1e-10); // sample variance (Bessel-corrected)
/// ```
pub fn simd_variance(data: &[f64], mean: f64) -> f64 {
    if data.len() < 2 {
        return f64::NAN;
    }
    // Internal SIMD kernels compute population variance (÷n).
    // Apply Bessel's correction to return sample variance (÷(n-1)).
    let n = data.len() as f64;
    arch_simd_variance(data, mean) * n / (n - 1.0)
}

/// Computes the **population** variance of the slice using SIMD (divides by `n`).
///
/// Use [`simd_variance`] for the sample variance (Bessel-corrected, ÷(n-1)).
pub fn simd_population_variance(data: &[f64], mean: f64) -> f64 {
    if data.is_empty() {
        return f64::NAN;
    }
    arch_simd_variance(data, mean)
}

/// Computes the minimum and maximum of a slice simultaneously using SIMD.
///
/// **NaN handling:** a `NaN` is skipped, on every tier.
///
/// That takes care on x86: `MINPD dst, src` returns **`src`** when either
/// operand is `NaN`, so accumulating with `min(acc, data)` lets one `NaN` in
/// the data poison the accumulator and the answer becomes whatever the last
/// lane held. The accumulator is therefore the *second* operand throughout —
/// `min(data, acc)` keeps the accumulator and drops the `NaN`, which is what
/// aarch64's `FMINNM` and the scalar `<`/`>` comparisons do anyway. This was a
/// real divergence: the two architectures gave different answers for the same
/// input, and the doc comment here said they did not.
///
/// On x86_64: uses `_mm_min_pd`/`_mm_max_pd` (SSE2), `_mm256_min_pd`/`_mm256_max_pd`
/// (AVX2), or `_mm512_min_pd`/`_mm512_max_pd` (AVX-512F).
/// On aarch64: uses `vminnmq_f64`/`vmaxnmq_f64` (NEON FMINNM/FMAXNM).
///
/// Returns `(f64::INFINITY, f64::NEG_INFINITY)` for empty slices.
///
/// # Examples
///
/// ```no_run
/// use chronix_analytics::compute::simd_min_max;
/// let (min, max) = simd_min_max(&[3.0, 1.0, 4.0, 1.0, 5.0, 9.0]);
/// assert!((min - 1.0).abs() < 1e-10);
/// assert!((max - 9.0).abs() < 1e-10);
/// ```
pub fn simd_min_max(data: &[f64]) -> (f64, f64) {
    if data.is_empty() {
        return (f64::INFINITY, f64::NEG_INFINITY);
    }
    arch_simd_min_max(data)
}

/// Computes the sample standard deviation.
///
/// Uses `simd_mean` and `simd_variance` internally. `simd_variance`
/// already returns sample variance (Bessel-corrected).
pub fn simd_std_dev(data: &[f64]) -> f64 {
    if data.len() < 2 {
        return if data.is_empty() { f64::NAN } else { 0.0 };
    }
    let (_, var) = simd_mean_variance(data);
    var.sqrt()
}

/// Two-pass mean + sample variance using SIMD kernels.
///
/// Replaces scalar Welford (data-dependent division per iteration,
/// un-vectorizable) with two SIMD passes:
///   1. `simd_sum` → mean (fully vectorized)
///   2. `arch_simd_variance` → population variance (fully vectorized)
///
/// The second pass operates on data already in L1/L2 cache from the first pass,
/// so the overhead is minimal compared to the 4-8× speedup from SIMD vs scalar.
///
/// Returns `(mean, sample_variance)` where sample variance uses Bessel's
/// correction (÷(n−1)).
pub fn simd_mean_variance(data: &[f64]) -> (f64, f64) {
    if data.len() < 2 {
        let mean = if data.is_empty() { f64::NAN } else { data[0] };
        return (mean, f64::NAN);
    }
    let mean = simd_sum(data) / data.len() as f64;
    let pop_var = arch_simd_variance(data, mean);
    let n = data.len() as f64;
    (mean, pop_var * n / (n - 1.0))
}

/// SIMD-accelerated timestamp range filter.
///
/// Returns the indices of elements in `timestamps` that fall within `[min, max]`
/// (inclusive). This is the most common predicate in time-series queries and
/// produces a selection vector directly, avoiding the overhead of materializing
/// a full `BooleanArray`.
///
/// On aarch64: uses NEON `vcgeq_s64`/`vcleq_s64` (2 elements/cycle).
/// On x86_64 without specific SIMD: uses 4-wide scalar unroll with ILP.
///
/// # Examples
///
/// ```no_run
/// use chronix_analytics::compute::simd_range_filter_i64;
/// let ts = [100, 200, 300, 400, 500];
/// let indices = simd_range_filter_i64(&ts, 200, 400);
/// assert_eq!(indices, vec![1, 2, 3]);
/// ```
pub fn simd_range_filter_i64(timestamps: &[i64], min: i64, max: i64) -> Vec<u32> {
    let mut result = Vec::with_capacity(timestamps.len());
    // 4-wide scalar unroll for ILP on all platforms.
    let chunks = timestamps.chunks_exact(4);
    let remainder = chunks.remainder();
    let mut base_idx: u32 = 0;
    for chunk in chunks {
        if chunk[0] >= min && chunk[0] <= max {
            result.push(base_idx);
        }
        if chunk[1] >= min && chunk[1] <= max {
            result.push(base_idx + 1);
        }
        if chunk[2] >= min && chunk[2] <= max {
            result.push(base_idx + 2);
        }
        if chunk[3] >= min && chunk[3] <= max {
            result.push(base_idx + 3);
        }
        base_idx += 4;
    }
    for &ts in remainder {
        if ts >= min && ts <= max {
            result.push(base_idx);
        }
        base_idx += 1;
    }
    result
}

/// Computes the dot product of two equal-length slices using SIMD.
///
/// On x86_64 with AVX2/AVX-512: uses FMA instructions for fused multiply-add.
/// On aarch64: uses `vfmaq_f64` (NEON FMA).
///
/// # Errors
///
/// Returns `ComputeError::DimensionMismatch` if `a.len() != b.len()`.
///
/// # Examples
///
/// ```no_run
/// use chronix_analytics::compute::simd_dot_product;
/// let a = [1.0, 2.0, 3.0, 4.0, 5.0];
/// let b = [5.0, 4.0, 3.0, 2.0, 1.0];
/// assert!((simd_dot_product(&a, &b).unwrap() - 35.0).abs() < 1e-10);
/// ```
pub fn simd_dot_product(a: &[f64], b: &[f64]) -> Result<f64, crate::compute::ComputeError> {
    if a.len() != b.len() {
        return Err(crate::compute::ComputeError::DimensionMismatch {
            expected: a.len(),
            got: b.len(),
        });
    }
    if a.is_empty() {
        return Ok(0.0);
    }
    Ok(arch_simd_dot_product(a, b))
}

/// Returns a human-readable string describing the active SIMD tier.
///
/// Useful for diagnostics and logging at startup.
pub fn simd_tier() -> &'static str {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512f") {
            return "AVX-512F (8×f64, 512-bit)";
        }
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            return "AVX2+FMA (4×f64, 256-bit)";
        }
        // The block's tail, not an early exit: the two above are.
        "SSE2 (2×f64, 128-bit)"
    }
    #[cfg(target_arch = "aarch64")]
    {
        "NEON (2×f64, 128-bit)"
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        "Scalar (4-wide ILP unroll)"
    }
}

#[cfg(test)]
mod tests {
    /// `simd_min_max` skips `NaN`, on whatever tier this build selected.
    ///
    /// It did not on x86: `MINPD dst, src` returns `src` when either operand
    /// is `NaN`, and the accumulator was the *first* operand — so one `NaN`
    /// poisoned it and the answer became whatever the last lane held, while
    /// aarch64 and the scalar fallback skipped it. Two architectures, two
    /// answers, and a doc comment saying they agreed.
    ///
    /// The lengths matter: the SIMD tiers process 16, 8 or 4 values per
    /// iteration with a scalar tail, so a `NaN` has to be tried in the
    /// vectorised body *and* in the tail.
    #[test]
    fn simd_min_max_skips_nan_at_every_position() {
        /// The answer, by definition: `NaN` is not a value.
        fn reference(data: &[f64]) -> (f64, f64) {
            let mut min = f64::INFINITY;
            let mut max = f64::NEG_INFINITY;
            for &v in data.iter().filter(|v| !v.is_nan()) {
                min = min.min(v);
                max = max.max(v);
            }
            (min, max)
        }

        for len in [1_usize, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 33, 64, 129] {
            for nan_at in 0..len {
                let mut data: Vec<f64> = (0..len).map(|i| i as f64 - 10.0).collect();
                data[nan_at] = f64::NAN;
                let (min, max) = super::simd_min_max(&data);
                let (want_min, want_max) = reference(&data);
                assert_eq!(
                    (min, max),
                    (want_min, want_max),
                    "len {len}, NaN at {nan_at}"
                );
            }
        }
    }

    /// An all-`NaN` slice has no minimum and no maximum, and says so the same
    /// way an empty one does.
    #[test]
    fn simd_min_max_of_all_nan_is_the_empty_answer() {
        for len in [1_usize, 4, 8, 16, 33] {
            let data = vec![f64::NAN; len];
            let (min, max) = super::simd_min_max(&data);
            assert_eq!((min, max), (f64::INFINITY, f64::NEG_INFINITY), "len {len}");
        }
    }

    proptest::proptest! {
        /// Against arbitrary input, including `NaN` and the infinities.
        #[test]
        fn simd_min_max_matches_a_nan_skipping_reference(
            data in proptest::collection::vec(
                proptest::prop_oneof![
                    3 => -1e6_f64..1e6_f64,
                    1 => proptest::strategy::Just(f64::NAN),
                    1 => proptest::strategy::Just(f64::INFINITY),
                    1 => proptest::strategy::Just(f64::NEG_INFINITY),
                ],
                0..200,
            )
        ) {
            let mut want_min = f64::INFINITY;
            let mut want_max = f64::NEG_INFINITY;
            for &v in data.iter().filter(|v| !v.is_nan()) {
                want_min = want_min.min(v);
                want_max = want_max.max(v);
            }
            let (min, max) = super::simd_min_max(&data);
            proptest::prop_assert_eq!(min, want_min);
            proptest::prop_assert_eq!(max, want_max);
        }
    }

    use super::*;

    // ── Sum ──────────────────────────────────────────────────────────

    #[test]
    fn sum_empty() {
        assert_eq!(simd_sum(&[]), 0.0);
    }

    #[test]
    fn sum_single() {
        assert!((simd_sum(&[42.0]) - 42.0).abs() < 1e-10);
    }

    #[test]
    fn sum_exact_chunk() {
        let data: Vec<f64> = (1..=8).map(|x| x as f64).collect();
        assert!((simd_sum(&data) - 36.0).abs() < 1e-10);
    }

    #[test]
    fn sum_with_remainder() {
        let data: Vec<f64> = (1..=7).map(|x| x as f64).collect();
        assert!((simd_sum(&data) - 28.0).abs() < 1e-10);
    }

    #[test]
    fn sum_large() {
        let data: Vec<f64> = (0..10_000).map(|i| i as f64).collect();
        let expected = (10_000.0 * 9999.0) / 2.0;
        assert!((simd_sum(&data) - expected).abs() < 1e-4);
    }

    #[test]
    fn sum_large_avx2_range() {
        // 16-element aligned (AVX2 chunk size)
        let data: Vec<f64> = (0..16).map(|i| i as f64).collect();
        assert!((simd_sum(&data) - 120.0).abs() < 1e-10);
    }

    #[test]
    fn sum_large_avx512_range() {
        // 32-element aligned (AVX-512 chunk size)
        let data: Vec<f64> = (0..32).map(|i| i as f64).collect();
        assert!((simd_sum(&data) - 496.0).abs() < 1e-10);
    }

    #[test]
    fn sum_100k() {
        let data: Vec<f64> = (0..100_000).map(|i| i as f64).collect();
        let expected = 100_000.0 * 99_999.0 / 2.0;
        assert!((simd_sum(&data) - expected).abs() / expected < 1e-10);
    }

    // ── Mean ─────────────────────────────────────────────────────────

    #[test]
    fn mean_empty() {
        assert!(simd_mean(&[]).is_nan());
    }

    #[test]
    fn mean_values() {
        assert!((simd_mean(&[1.0, 2.0, 3.0, 4.0, 5.0]) - 3.0).abs() < 1e-10);
    }

    // ── Variance ─────────────────────────────────────────────────────

    #[test]
    fn variance_known() {
        let data = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let mean = simd_mean(&data);
        let var = simd_variance(&data, mean);
        // Sample variance: sum((xi - mean)^2) / (n-1) = 32/7
        assert!((var - 32.0 / 7.0).abs() < 1e-10);
    }

    #[test]
    fn variance_single_element() {
        // Sample variance is undefined for n=1
        assert!(simd_variance(&[42.0], 42.0).is_nan());
    }

    #[test]
    fn variance_empty() {
        assert!(simd_variance(&[], 0.0).is_nan());
    }

    #[test]
    fn variance_constant() {
        let data = [5.0; 100];
        let mean = simd_mean(&data);
        assert!(simd_variance(&data, mean).abs() < 1e-14);
    }

    // ── Min/Max ──────────────────────────────────────────────────────

    #[test]
    fn min_max_empty() {
        let (min, max) = simd_min_max(&[]);
        assert_eq!(min, f64::INFINITY);
        assert_eq!(max, f64::NEG_INFINITY);
    }

    #[test]
    fn min_max_values() {
        let (min, max) = simd_min_max(&[5.0, 2.0, 8.0, 1.0, 9.0, 3.0, 7.0]);
        assert!((min - 1.0).abs() < 1e-10);
        assert!((max - 9.0).abs() < 1e-10);
    }

    #[test]
    fn min_max_single() {
        let (min, max) = simd_min_max(&[42.0]);
        assert!((min - 42.0).abs() < 1e-10);
        assert!((max - 42.0).abs() < 1e-10);
    }

    #[test]
    fn min_max_negative() {
        let (min, max) = simd_min_max(&[-10.0, -5.0, -1.0, -20.0, -3.0]);
        assert!((min - (-20.0)).abs() < 1e-10);
        assert!((max - (-1.0)).abs() < 1e-10);
    }

    #[test]
    fn min_max_large() {
        let data: Vec<f64> = (0..10_000).map(|i| i as f64).collect();
        let (min, max) = simd_min_max(&data);
        assert!((min - 0.0).abs() < 1e-10);
        assert!((max - 9999.0).abs() < 1e-10);
    }

    #[test]
    fn min_max_with_nan() {
        // NaN values must be skipped consistently across all SIMD tiers
        // (x86 SSE2/AVX2/AVX-512, aarch64 NEON, scalar fallback).
        let data = [f64::NAN, 2.0, f64::NAN, 1.0, 5.0, f64::NAN, 3.0];
        let (min, max) = simd_min_max(&data);
        assert!((min - 1.0).abs() < 1e-10, "min should skip NaN, got {min}");
        assert!((max - 5.0).abs() < 1e-10, "max should skip NaN, got {max}");
    }

    #[test]
    fn min_max_all_nan() {
        // All-NaN slice returns the identity values (no finite values to compare).
        let (min, max) = simd_min_max(&[f64::NAN, f64::NAN]);
        assert!(
            min == f64::INFINITY || min.is_nan(),
            "unexpected min: {min}"
        );
        assert!(
            max == f64::NEG_INFINITY || max.is_nan(),
            "unexpected max: {max}"
        );
    }

    // ── Standard Deviation ───────────────────────────────────────────

    #[test]
    fn std_dev_constant() {
        assert!((simd_std_dev(&[5.0, 5.0, 5.0, 5.0])).abs() < 1e-10);
    }

    #[test]
    fn std_dev_known() {
        let data = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let sd = simd_std_dev(&data);
        assert!((sd - 2.138_089_935_299_395).abs() < 1e-6);
    }

    // ── Dot Product ──────────────────────────────────────────────────

    #[test]
    fn dot_product_basic() {
        let a = [1.0, 2.0, 3.0, 4.0, 5.0];
        let b = [5.0, 4.0, 3.0, 2.0, 1.0];
        assert!((simd_dot_product(&a, &b).unwrap() - 35.0).abs() < 1e-10);
    }

    #[test]
    fn dot_product_empty() {
        assert!((simd_dot_product(&[], &[]).unwrap()).abs() < 1e-10);
    }

    #[test]
    fn dot_product_mismatch() {
        assert!(simd_dot_product(&[1.0], &[1.0, 2.0]).is_err());
    }

    #[test]
    fn dot_product_identity() {
        // dot(v, v) = sum of squares
        let v: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let expected: f64 = v.iter().map(|x| x * x).sum();
        assert!((simd_dot_product(&v, &v).unwrap() - expected).abs() < 1e-6);
    }

    #[test]
    fn dot_product_orthogonal() {
        let a = [1.0, 0.0, 1.0, 0.0];
        let b = [0.0, 1.0, 0.0, 1.0];
        assert!((simd_dot_product(&a, &b).unwrap()).abs() < 1e-10);
    }

    #[test]
    fn dot_product_large() {
        // 10K elements — exercises AVX2/AVX-512 main loops
        let a: Vec<f64> = (0..10_000).map(|i| (i as f64) * 0.001).collect();
        let b: Vec<f64> = (0..10_000).map(|i| 1.0 - (i as f64) * 0.0001).collect();
        let expected: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        assert!(
            (simd_dot_product(&a, &b).unwrap() - expected).abs() / expected.abs().max(1.0) < 1e-8
        );
    }

    // ── SIMD tier ────────────────────────────────────────────────────

    #[test]
    fn simd_tier_not_empty() {
        let tier = simd_tier();
        assert!(!tier.is_empty());
        // On this platform it should be one of the known tiers
        assert!(
            tier.contains("AVX-512")
                || tier.contains("AVX2")
                || tier.contains("SSE2")
                || tier.contains("NEON")
                || tier.contains("Scalar")
        );
    }

    // ── Cross-validation: scalar vs arch agree ───────────────────────

    #[test]
    fn scalar_vs_arch_sum() {
        let data: Vec<f64> = (0..1_000).map(|i| (i as f64) * 0.1 + 0.7).collect();
        let arch_result = arch_simd_sum(&data);
        let scalar_result = scalar_sum(&data);
        assert!(
            (arch_result - scalar_result).abs() < 1e-6,
            "sum mismatch: arch={arch_result}, scalar={scalar_result}"
        );
    }

    #[test]
    fn scalar_vs_arch_variance() {
        let data: Vec<f64> = (0..1_000).map(|i| (i as f64) * 0.1 + 0.7).collect();
        let mean = simd_mean(&data);
        let arch_result = arch_simd_variance(&data, mean);
        let scalar_result = scalar_variance(&data, mean);
        assert!(
            (arch_result - scalar_result).abs() < 1e-6,
            "variance mismatch: arch={arch_result}, scalar={scalar_result}"
        );
    }

    #[test]
    fn scalar_vs_arch_min_max() {
        let data: Vec<f64> = (0..1_000)
            .map(|i| ((i * 7 + 3) % 500) as f64 - 250.0)
            .collect();
        let (amin, amax) = arch_simd_min_max(&data);
        let (smin, smax) = scalar_min_max(&data);
        assert!((amin - smin).abs() < 1e-10, "min mismatch");
        assert!((amax - smax).abs() < 1e-10, "max mismatch");
    }

    #[test]
    fn scalar_vs_arch_dot_product() {
        let a: Vec<f64> = (0..1_000).map(|i| (i as f64) * 0.01).collect();
        let b: Vec<f64> = (0..1_000).map(|i| 1.0 - (i as f64) * 0.001).collect();
        let arch_result = arch_simd_dot_product(&a, &b);
        let scalar_result = scalar_dot_product(&a, &b);
        assert!(
            (arch_result - scalar_result).abs() / scalar_result.abs().max(1.0) < 1e-8,
            "dot product mismatch: arch={arch_result}, scalar={scalar_result}"
        );
    }

    // ── Kahan precision ────────────────────────────────────

    #[test]
    fn kahan_compensated_sum_precision() {
        // Classic Kahan torture test: sum 1e8 values of 1.0 each, then add
        // 1e8 values of 1e-8 each.  Naive summation loses the tiny terms
        // entirely; Kahan preserves them.
        let n = 100_000;
        let mut data = vec![1.0_f64; n];
        data.extend(vec![1e-8_f64; n]);
        let expected = n as f64 + (n as f64) * 1e-8;
        let result = simd_sum(&data);
        let rel_err = (result - expected).abs() / expected;
        assert!(
            rel_err < 1e-14,
            "Kahan sum relative error {rel_err:.2e} exceeds 1e-14 (result={result}, expected={expected})"
        );
    }

    #[test]
    fn kahan_alternating_sign_precision() {
        // Alternating +big / -big values → catastrophic cancellation for
        // naive summation, but Kahan tracks the residuals.
        let n = 50_000;
        let mut data = Vec::with_capacity(2 * n);
        for i in 0..n {
            data.push(1e15 + (i as f64));
            data.push(-(1e15 + (i as f64)));
        }
        let result = simd_sum(&data);
        assert!(
            result.abs() < 1e-3,
            "alternating-sign sum should be ~0, got {result}"
        );
    }
}

// AVX-512 compile-time validation tests.
//
// These tests are only compiled when `target_feature = "avx512f"` is active
// (e.g. via `RUSTFLAGS="-C target-feature=+avx512f"` in CI on capable
// hardware). They call the AVX-512 implementations directly, bypassing
// runtime feature detection, to validate correctness at compile time.
#[cfg(all(test, target_arch = "x86_64", target_feature = "avx512f"))]
mod avx512_tests {
    use super::*;

    #[test]
    fn avx512_sum_direct() {
        let data: Vec<f64> = (0..64).map(|i| i as f64).collect();
        let result = unsafe { avx512_sum(&data) };
        let expected: f64 = (0..64).map(|i| i as f64).sum();
        assert!(
            (result - expected).abs() < 1e-10,
            "avx512_sum mismatch: {result} vs {expected}"
        );
    }

    #[test]
    fn avx512_variance_direct() {
        let data: Vec<f64> = (0..128).map(|i| i as f64).collect();
        let (var, mean) = unsafe { avx512_variance(&data) };
        assert!(
            (mean - 63.5).abs() < 1e-10,
            "avx512_variance mean mismatch: {mean}"
        );
        assert!(var > 0.0, "variance must be positive");
    }

    #[test]
    fn avx512_min_max_direct() {
        let data: Vec<f64> = (0..64).map(|i| i as f64).collect();
        let (min, max) = unsafe { avx512_min_max(&data) };
        assert!((min - 0.0).abs() < 1e-10);
        assert!((max - 63.0).abs() < 1e-10);
    }

    #[test]
    fn avx512_dot_product_direct() {
        let a: Vec<f64> = (0..64).map(|i| i as f64).collect();
        let b: Vec<f64> = (0..64).map(|i| (i * 2) as f64).collect();
        let result = unsafe { avx512_dot_product(&a, &b) };
        let expected: f64 = (0..64).map(|i| (i * i * 2) as f64).sum();
        assert!(
            (result - expected).abs() < 1e-6,
            "avx512_dot mismatch: {result} vs {expected}"
        );
    }
}
