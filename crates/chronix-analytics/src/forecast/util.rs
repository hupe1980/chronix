//! Shared utilities for forecast models.

use crate::forecast::error::ForecastError;

/// Validate that all input values are finite (no NaN or Inf) and timestamps
/// match values in length.
#[inline]
pub fn validate_input(timestamps: &[i64], values: &[f64]) -> Result<(), ForecastError> {
    if timestamps.len() != values.len() {
        return Err(ForecastError::InvalidInput(format!(
            "timestamps length ({}) != values length ({})",
            timestamps.len(),
            values.len(),
        )));
    }
    if let Some(pos) = values.iter().position(|v| !v.is_finite()) {
        return Err(ForecastError::InvalidInput(format!(
            "values[{pos}] is not finite ({})",
            values[pos],
        )));
    }
    Ok(())
}

/// Compute the median interval between sorted timestamps using O(n) selection.
///
/// Returns at least 1 to prevent division-by-zero downstream when all
/// timestamps are identical (all deltas zero).
pub fn median_interval(timestamps: &[i64]) -> i64 {
    if timestamps.len() < 2 {
        return 1;
    }
    let mut deltas: Vec<i64> = timestamps.windows(2).map(|w| w[1] - w[0]).collect();
    let mid = deltas.len() / 2;
    let median = *deltas.select_nth_unstable(mid).1;
    median.max(1)
}

/// Standard-normal quantile function `Φ⁻¹(p)`, for `p ∈ (0, 1)`.
///
/// Acklam's rational approximation, whose relative error is below
/// `1.15 × 10⁻⁹` over the whole open interval — several orders of magnitude
/// finer than any prediction interval it is used to build. Returns `±∞` at the
/// closed boundaries and `NaN` outside them, which is what the function is.
#[must_use]
pub fn normal_quantile(p: f64) -> f64 {
    if !(0.0..=1.0).contains(&p) || p.is_nan() {
        return f64::NAN;
    }
    if p == 0.0 {
        return f64::NEG_INFINITY;
    }
    if p == 1.0 {
        return f64::INFINITY;
    }

    const A: [f64; 6] = [
        -3.969_683_028_665_376e1,
        2.209_460_984_245_205e2,
        -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_614_716e1,
        2.506_628_277_459_239e0,
    ];
    const B: [f64; 5] = [
        -5.447_609_879_822_406e1,
        1.615_858_368_580_409e2,
        -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1,
        -1.328_068_155_288_572e1,
    ];
    const C: [f64; 6] = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838e0,
        -2.549_732_539_343_734e0,
        4.374_664_141_464_968e0,
        2.938_163_982_698_783e0,
    ];
    const D: [f64; 4] = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996e0,
        3.754_408_661_907_416e0,
    ];
    // Breakpoints between the tail and central rational fits.
    const P_LOW: f64 = 0.024_25;
    const P_HIGH: f64 = 1.0 - P_LOW;

    if p < P_LOW {
        let q = (-2.0 * p.ln()).sqrt();
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    } else if p <= P_HIGH {
        let q = p - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    } else {
        let q = (-2.0 * (1.0 - p).ln()).sqrt();
        -(((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    }
}

/// The `z` multiplier of a two-sided prediction interval at `level`.
///
/// `z_for_confidence(0.95) ≈ 1.96`. Returns `None` outside `(0, 1)`.
#[must_use]
pub fn z_for_confidence(level: f64) -> Option<f64> {
    if !(level > 0.0 && level < 1.0) {
        return None;
    }
    Some(normal_quantile(0.5 + level / 2.0))
}
