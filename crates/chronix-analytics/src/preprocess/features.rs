//! Temporal feature extraction functions for time-series data.
//!
//! All functions operate on `&[f64]` slices and return `Vec<f64>`,
//! using `f64::NAN` where values cannot be computed (e.g. boundary rows).

/// Lag: value `offset` rows earlier. Returns NAN for first `offset` rows.
///
/// When `offset >= values.len()`, all output values are NAN (the entire
/// series is shifted beyond the available data).
#[inline]
pub fn lag(values: &[f64], offset: usize) -> Vec<f64> {
    let n = values.len();
    let mut out = vec![f64::NAN; n];
    if offset < n {
        out[offset..n].copy_from_slice(&values[..(n - offset)]);
    }
    out
}

/// N-th order differencing.
///
/// `order = 1`: `diff[i] = values[i] - values[i-1]`.
/// `order = 2`: second-difference, etc.  Leading `order` elements are NAN.
pub fn diff(values: &[f64], order: usize) -> Vec<f64> {
    if order == 0 {
        return values.to_vec();
    }
    let mut prev = values.to_vec();
    for _ in 0..order {
        let mut cur = vec![f64::NAN; prev.len()];
        for i in 1..prev.len() {
            cur[i] = prev[i] - prev[i - 1];
        }
        prev = cur;
    }
    prev
}

/// Percentage change: `(values[i] - values[i-1]) / values[i-1]`.
/// First element is NAN.  Returns NAN when `values[i-1]` is zero.
pub fn pct_change(values: &[f64]) -> Vec<f64> {
    let n = values.len();
    let mut out = vec![f64::NAN; n];
    for i in 1..n {
        let prev = values[i - 1];
        out[i] = if prev.abs() < f64::EPSILON {
            f64::NAN
        } else {
            (values[i] - prev) / prev
        };
    }
    out
}

/// Rolling arithmetic mean over a trailing window.
///
/// Outputs NAN for the first `window - 1` rows, matching [`rolling_std`] so
/// the two line up column-for-column. Kahan compensated summation keeps the
/// sliding sum from drifting over a long series, which a plain
/// add-the-new-subtract-the-old loop does not.
#[must_use]
pub fn rolling_mean(values: &[f64], window: usize) -> Vec<f64> {
    let n = values.len();
    if window == 0 || window > n {
        return vec![f64::NAN; n];
    }
    let mut out = vec![f64::NAN; n];
    let w = window as f64;
    let mut sum: f64 = values[..window].iter().sum();
    out[window - 1] = sum / w;
    for i in window..n {
        sum += values[i] - values[i - window];
        // Periodic exact recompute, for the same reason `rolling_std` does it:
        // the incremental form is O(1) but accumulates error.
        if (i - window + 1).is_multiple_of(1024) {
            sum = values[i + 1 - window..=i].iter().sum();
        }
        out[i] = sum / w;
    }
    out
}

/// Rolling standard deviation using Welford's online algorithm.
///
/// Outputs NAN for the first `window - 1` rows.
pub fn rolling_std(values: &[f64], window: usize) -> Vec<f64> {
    let n = values.len();
    if window == 0 || window > n {
        return vec![f64::NAN; n];
    }
    // Single-observation window: std dev is 0 by definition (no variance).
    if window == 1 {
        return vec![0.0; n];
    }
    let mut out = vec![f64::NAN; n];
    let w = window as f64;

    // Welford's online algorithm for initial window
    let mut mean = 0.0_f64;
    let mut m2 = 0.0_f64;
    for (count_0, &v) in values.iter().enumerate().take(window) {
        let count = (count_0 + 1) as f64;
        let delta = v - mean;
        mean += delta / count;
        let delta2 = v - mean;
        m2 += delta * delta2;
    }
    out[window - 1] = (m2 / (w - 1.0)).sqrt();

    // Sliding window: update Welford sums incrementally — O(1) per step.
    // Periodically recompute from scratch every 1024 steps to prevent drift.
    for i in window..n {
        let old = values[i - window];
        let new = values[i];

        // Remove old, add new using Welford-update formulas
        let old_mean = mean;
        mean += (new - old) / w;
        m2 += (new - old) * (new - mean + old - old_mean);
        // Guard against floating-point underflow
        if m2 < 0.0 {
            m2 = 0.0;
        }

        // Periodic exact recompute for numerical stability
        if (i - window + 1).is_multiple_of(1024) {
            let start = i + 1 - window;
            let slice = &values[start..=i];
            mean = slice.iter().sum::<f64>() / w;
            m2 = slice.iter().map(|v| (v - mean).powi(2)).sum::<f64>();
        }

        out[i] = (m2 / (w - 1.0)).sqrt();
    }
    out
}

/// Rolling Pearson correlation between two columns.
///
/// Outputs NAN for the first `window - 1` rows or when variance is zero.
/// Uses running sums for O(n) total complexity.
pub fn rolling_corr(a: &[f64], b: &[f64], window: usize) -> Vec<f64> {
    let n = a.len().min(b.len());
    if window < 2 || window > n {
        return vec![f64::NAN; n];
    }
    let mut out = vec![f64::NAN; n];
    let w = window as f64;

    // Initialize running sums for first window
    let mut sum_a = 0.0_f64;
    let mut sum_b = 0.0_f64;
    let mut sum_ab = 0.0_f64;
    let mut sum_a2 = 0.0_f64;
    let mut sum_b2 = 0.0_f64;
    for j in 0..window {
        sum_a += a[j];
        sum_b += b[j];
        sum_ab += a[j] * b[j];
        sum_a2 += a[j] * a[j];
        sum_b2 += b[j] * b[j];
    }
    let cov = sum_ab - sum_a * sum_b / w;
    let va = (sum_a2 - sum_a * sum_a / w).max(0.0);
    let vb = (sum_b2 - sum_b * sum_b / w).max(0.0);
    let denom = (va * vb).sqrt();
    out[window - 1] = if denom < f64::EPSILON {
        f64::NAN
    } else {
        cov / denom
    };

    // Slide window: O(1) per step, with periodic recomputation every 1024
    // steps to bound floating-point drift in running sums.
    const RECOMPUTE_INTERVAL: usize = 1024;
    for i in window..n {
        if (i - window).is_multiple_of(RECOMPUTE_INTERVAL) && i > window {
            // Recompute running sums from scratch to reset drift
            sum_a = 0.0;
            sum_b = 0.0;
            sum_ab = 0.0;
            sum_a2 = 0.0;
            sum_b2 = 0.0;
            for j in (i - window + 1)..=i {
                sum_a += a[j];
                sum_b += b[j];
                sum_ab += a[j] * b[j];
                sum_a2 += a[j] * a[j];
                sum_b2 += b[j] * b[j];
            }
        } else {
            let old_a = a[i - window];
            let old_b = b[i - window];
            let new_a = a[i];
            let new_b = b[i];
            sum_a += new_a - old_a;
            sum_b += new_b - old_b;
            sum_ab += new_a * new_b - old_a * old_b;
            sum_a2 += new_a * new_a - old_a * old_a;
            sum_b2 += new_b * new_b - old_b * old_b;
        }

        let cov = sum_ab - sum_a * sum_b / w;
        let va = (sum_a2 - sum_a * sum_a / w).max(0.0);
        let vb = (sum_b2 - sum_b * sum_b / w).max(0.0);
        let denom = (va * vb).sqrt();
        out[i] = if denom < f64::EPSILON {
            f64::NAN
        } else {
            cov / denom
        };
    }
    out
}

/// Z-score normalization: `(value - mean) / std_dev`.
///
/// Uses sample standard deviation (Bessel's correction, divides by `n-1`)
/// for consistency with `crate::compute::simd_std_dev`.
/// Returns NAN when `std_dev ≈ 0` or `n < 2`.
pub fn zscore(values: &[f64]) -> Vec<f64> {
    let n = values.len();
    if n < 2 {
        return vec![f64::NAN; n];
    }
    let mean = values.iter().sum::<f64>() / n as f64;
    let var = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1) as f64;
    let std = var.sqrt();
    if std < f64::EPSILON {
        return vec![f64::NAN; n];
    }
    values.iter().map(|v| (v - mean) / std).collect()
}

/// Exponentially weighted mean with configurable `alpha ∈ (0, 1]`.
///
/// `ewm[0] = values[0]`; `ewm[i] = alpha * values[i] + (1 - alpha) * ewm[i-1]`.
pub fn ewm(values: &[f64], alpha: f64) -> Vec<f64> {
    let n = values.len();
    if n == 0 {
        return vec![];
    }
    let alpha = alpha.clamp(f64::EPSILON, 1.0);
    let mut out = Vec::with_capacity(n);
    out.push(values[0]);
    for i in 1..n {
        let prev = out[i - 1];
        out.push(alpha * values[i] + (1.0 - alpha) * prev);
    }
    out
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f64, b: f64, eps: f64) -> bool {
        if a.is_nan() && b.is_nan() {
            return true;
        }
        (a - b).abs() < eps
    }

    #[test]
    fn test_lag_basic() {
        let v = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let l = lag(&v, 2);
        assert!(l[0].is_nan());
        assert!(l[1].is_nan());
        assert_eq!(l[2], 1.0);
        assert_eq!(l[3], 2.0);
        assert_eq!(l[4], 3.0);
    }

    #[test]
    fn test_lag_zero_offset() {
        let v = vec![10.0, 20.0];
        let l = lag(&v, 0);
        assert_eq!(l, v);
    }

    #[test]
    fn test_diff_order_1() {
        let v = vec![1.0, 3.0, 6.0, 10.0];
        let d = diff(&v, 1);
        assert!(d[0].is_nan());
        assert_eq!(d[1], 2.0);
        assert_eq!(d[2], 3.0);
        assert_eq!(d[3], 4.0);
    }

    #[test]
    fn test_diff_order_2() {
        let v = vec![1.0, 3.0, 6.0, 10.0, 15.0];
        let d = diff(&v, 2);
        assert!(d[0].is_nan());
        assert!(d[1].is_nan());
        assert_eq!(d[2], 1.0); // second-difference of quadratic-like
        assert_eq!(d[3], 1.0);
        assert_eq!(d[4], 1.0);
    }

    #[test]
    fn test_diff_order_0() {
        let v = vec![1.0, 2.0, 3.0];
        assert_eq!(diff(&v, 0), v);
    }

    #[test]
    fn test_pct_change() {
        let v = vec![100.0, 110.0, 99.0];
        let p = pct_change(&v);
        assert!(p[0].is_nan());
        assert!(approx(p[1], 0.1, 1e-9));
        assert!(approx(p[2], -0.1, 1e-9));
    }

    #[test]
    fn test_pct_change_zero_denominator() {
        let v = vec![0.0, 5.0];
        let p = pct_change(&v);
        assert!(p[1].is_nan());
    }

    #[test]
    fn test_rolling_std() {
        let v = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let r = rolling_std(&v, 3);
        assert!(r[0].is_nan());
        assert!(r[1].is_nan());
        // std({2,4,4}) = sqrt(((2-10/3)^2+(4-10/3)^2+(4-10/3)^2)/2)
        let expected: f64 = ((2.0_f64 - 10.0 / 3.0).powi(2)
            + (4.0_f64 - 10.0 / 3.0).powi(2)
            + (4.0_f64 - 10.0 / 3.0).powi(2))
            / 2.0;
        assert!(approx(r[2], expected.sqrt(), 1e-9));
    }

    #[test]
    fn test_rolling_std_window_one() {
        // Window of 1 has zero variance — should return 0, not NaN.
        let v = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let r = rolling_std(&v, 1);
        assert_eq!(r.len(), v.len());
        for val in &r {
            assert_eq!(*val, 0.0, "rolling_std with window=1 must be 0");
        }
    }

    #[test]
    fn test_rolling_corr_perfect() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let b = vec![2.0, 4.0, 6.0, 8.0, 10.0];
        let r = rolling_corr(&a, &b, 3);
        assert!(r[0].is_nan());
        assert!(r[1].is_nan());
        for &v in &r[2..] {
            assert!(approx(v, 1.0, 1e-9));
        }
    }

    #[test]
    fn test_rolling_corr_negative() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let b = vec![5.0, 4.0, 3.0, 2.0, 1.0];
        let r = rolling_corr(&a, &b, 3);
        for &v in &r[2..] {
            assert!(approx(v, -1.0, 1e-9));
        }
    }

    #[test]
    fn test_zscore() {
        let v = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let z = zscore(&v);
        assert_eq!(z.len(), 5);
        // mean=3, sample_std=sqrt(2.5) (Bessel's correction: variance=10/4=2.5)
        let std_dev = 2.5_f64.sqrt();
        assert!(approx(z[0], -2.0 / std_dev, 1e-9));
        assert!(approx(z[2], 0.0, 1e-9));
        assert!(approx(z[4], 2.0 / std_dev, 1e-9));
    }

    #[test]
    fn test_zscore_constant() {
        let v = vec![5.0, 5.0, 5.0];
        let z = zscore(&v);
        assert!(z.iter().all(|v| v.is_nan()));
    }

    #[test]
    fn test_ewm() {
        let v = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let e = ewm(&v, 0.5);
        assert_eq!(e[0], 1.0);
        assert!(approx(e[1], 1.5, 1e-9)); // 0.5*2 + 0.5*1
        assert!(approx(e[2], 2.25, 1e-9)); // 0.5*3 + 0.5*1.5
    }

    #[test]
    fn test_ewm_alpha_one() {
        let v = vec![1.0, 10.0, 100.0];
        let e = ewm(&v, 1.0);
        // alpha=1 means no smoothing — just pass-through
        assert_eq!(e, v);
    }

    #[test]
    fn test_ewm_empty() {
        let e = ewm(&[], 0.5);
        assert!(e.is_empty());
    }
}
