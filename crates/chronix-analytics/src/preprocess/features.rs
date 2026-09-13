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

/// Sliding first and second moments of the **non-NULL** values in a window,
/// maintained by Welford add/remove.
///
/// Welford rather than running sums of `x` and `x²`, because a metrics
/// database's commonest series is a counter: values near 1e9 with a variance
/// near 1 leave `Σx² − (Σx)²/n` with no significant digits at all. Welford
/// never forms either square.
///
/// `NULL` — a NaN in the caller's slice — is simply not an observation:
/// nothing is added for it and nothing removed. That is what makes the whole
/// family agree with `AVG(v) OVER (ROWS n PRECEDING)` sitting beside it in
/// the same `SELECT`, and it is what the previous incremental form could not
/// do. There, one NULL made `mean` and `m2` NaN, and every later row of the
/// partition came out NULL too — until an exact recompute every 1024 steps
/// silently healed it, so the damage ran to the next multiple of 1024 and a
/// NULL at row 5 and one at row 1500 broke a different number of rows.
#[derive(Debug, Default, Clone, Copy)]
struct Moments {
    n: usize,
    mean: f64,
    m2: f64,
}

impl Moments {
    fn add(&mut self, x: f64) {
        if !x.is_finite() {
            return;
        }
        self.n += 1;
        let d = x - self.mean;
        self.mean += d / self.n as f64;
        self.m2 += d * (x - self.mean);
    }

    fn remove(&mut self, x: f64) {
        if !x.is_finite() {
            return;
        }
        if self.n <= 1 {
            *self = Self::default();
            return;
        }
        let prev_mean = self.mean;
        self.n -= 1;
        self.mean -= (x - prev_mean) / self.n as f64;
        self.m2 -= (x - prev_mean) * (x - self.mean);
        // Cancellation can leave a sum of squares just below zero.
        if self.m2 < 0.0 {
            self.m2 = 0.0;
        }
    }

    fn from_slice(xs: &[f64]) -> Self {
        let mut m = Self::default();
        for &x in xs {
            m.add(x);
        }
        m
    }

    /// Sample standard deviation, `NaN` with fewer than two observations.
    fn sample_std(&self) -> f64 {
        if self.n < 2 {
            return f64::NAN;
        }
        (self.m2 / (self.n - 1) as f64).sqrt()
    }
}

/// How often the sliding moments are rebuilt from the window exactly.
///
/// Welford's add/remove pair is self-correcting for the mean but not for
/// `m2`, and a long series subtracts a great many times.
const RECOMPUTE_INTERVAL: usize = 1024;

/// Rolling arithmetic mean of the non-NULL values in a trailing window.
///
/// Outputs NAN for the first `window - 1` rows — there is no full window yet
/// — and thereafter the mean of whatever the window holds, which is NAN only
/// if the window holds nothing. `AVG(v) OVER (ROWS window - 1 PRECEDING)`
/// answers the same question with the same NULL rule.
#[must_use]
pub fn rolling_mean(values: &[f64], window: usize) -> Vec<f64> {
    let n = values.len();
    if window == 0 || window > n {
        return vec![f64::NAN; n];
    }
    let mut out = vec![f64::NAN; n];
    let mut m = Moments::from_slice(&values[..window]);
    out[window - 1] = if m.n == 0 { f64::NAN } else { m.mean };
    for i in window..n {
        m.remove(values[i - window]);
        m.add(values[i]);
        if (i - window + 1).is_multiple_of(RECOMPUTE_INTERVAL) {
            m = Moments::from_slice(&values[i + 1 - window..=i]);
        }
        out[i] = if m.n == 0 { f64::NAN } else { m.mean };
    }
    out
}

/// Rolling sample standard deviation of the non-NULL values in a trailing
/// window, by Welford's algorithm.
///
/// Outputs NAN for the first `window - 1` rows and wherever the window holds
/// fewer than two values — including `window == 1`, where a sample standard
/// deviation divides by zero. Returning `0.0` there, which this used to do,
/// asserts that a single reading has no spread; it has no *measurable*
/// spread, which is what NULL says.
#[must_use]
pub fn rolling_std(values: &[f64], window: usize) -> Vec<f64> {
    let n = values.len();
    if window == 0 || window > n {
        return vec![f64::NAN; n];
    }
    let mut out = vec![f64::NAN; n];
    let mut m = Moments::from_slice(&values[..window]);
    out[window - 1] = m.sample_std();
    for i in window..n {
        m.remove(values[i - window]);
        m.add(values[i]);
        if (i - window + 1).is_multiple_of(RECOMPUTE_INTERVAL) {
            m = Moments::from_slice(&values[i + 1 - window..=i]);
        }
        out[i] = m.sample_std();
    }
    out
}

/// Sliding co-moments of the pairs in a window where **both** sides are
/// non-NULL — pairwise-complete, as `PearsonCorrelation::compute` is over a
/// whole partition.
#[derive(Debug, Default, Clone, Copy)]
struct CoMoments {
    n: usize,
    mx: f64,
    my: f64,
    m2x: f64,
    m2y: f64,
    cxy: f64,
}

impl CoMoments {
    fn add(&mut self, x: f64, y: f64) {
        if !x.is_finite() || !y.is_finite() {
            return;
        }
        self.n += 1;
        let nf = self.n as f64;
        let dx = x - self.mx;
        let dy = y - self.my;
        self.mx += dx / nf;
        self.my += dy / nf;
        self.m2x += dx * (x - self.mx);
        self.m2y += dy * (y - self.my);
        self.cxy += dx * (y - self.my);
    }

    fn remove(&mut self, x: f64, y: f64) {
        if !x.is_finite() || !y.is_finite() {
            return;
        }
        if self.n <= 1 {
            *self = Self::default();
            return;
        }
        let (px, py) = (self.mx, self.my);
        self.n -= 1;
        let nf = self.n as f64;
        self.mx -= (x - px) / nf;
        self.my -= (y - py) / nf;
        self.m2x -= (x - px) * (x - self.mx);
        self.m2y -= (y - py) * (y - self.my);
        self.cxy -= (x - px) * (y - self.my);
        if self.m2x < 0.0 {
            self.m2x = 0.0;
        }
        if self.m2y < 0.0 {
            self.m2y = 0.0;
        }
    }

    fn from_slices(xs: &[f64], ys: &[f64]) -> Self {
        let mut c = Self::default();
        for (&x, &y) in xs.iter().zip(ys) {
            c.add(x, y);
        }
        c
    }

    /// Pearson correlation, `NaN` with fewer than two pairs or when either
    /// side has no spread — `0.0` there would assert "uncorrelated" where
    /// the truth is "there is nothing to correlate".
    fn correlation(&self) -> f64 {
        if self.n < 2 {
            return f64::NAN;
        }
        let denom = (self.m2x * self.m2y).sqrt();
        if denom < f64::EPSILON {
            return f64::NAN;
        }
        self.cxy / denom
    }
}

/// Rolling Pearson correlation over a trailing window, pairwise-complete.
///
/// Outputs NAN for the first `window - 1` rows and wherever the window holds
/// fewer than two complete pairs.
#[must_use]
pub fn rolling_corr(a: &[f64], b: &[f64], window: usize) -> Vec<f64> {
    let n = a.len().min(b.len());
    if window < 2 || window > n {
        return vec![f64::NAN; n];
    }
    let mut out = vec![f64::NAN; n];
    let mut c = CoMoments::from_slices(&a[..window], &b[..window]);
    out[window - 1] = c.correlation();
    for i in window..n {
        c.remove(a[i - window], b[i - window]);
        c.add(a[i], b[i]);
        if (i - window + 1).is_multiple_of(RECOMPUTE_INTERVAL) {
            c = CoMoments::from_slices(&a[i + 1 - window..=i], &b[i + 1 - window..=i]);
        }
        out[i] = c.correlation();
    }
    out
}

/// Z-score normalisation against the partition's own mean and sample
/// standard deviation.
///
/// The statistics are taken over the non-NULL values; a NULL row stays NULL
/// and does not make the rest of the partition NULL, which is what summing a
/// slice containing one NaN used to do. Returns all-NAN when fewer than two
/// values are present or the spread is zero.
///
/// Uses the sample standard deviation (Bessel's correction, `n-1`) for
/// consistency with `crate::compute::simd_std_dev`.
#[must_use]
pub fn zscore(values: &[f64]) -> Vec<f64> {
    let n = values.len();
    let m = Moments::from_slice(values);
    let std = m.sample_std();
    if !std.is_finite() || std < f64::EPSILON {
        return vec![f64::NAN; n];
    }
    values
        .iter()
        .map(|v| {
            if v.is_finite() {
                (v - m.mean) / std
            } else {
                f64::NAN
            }
        })
        .collect()
}

/// Exponentially weighted mean with smoothing factor `alpha ∈ (0, 1]`.
///
/// `ewm[i] = alpha * values[i] + (1 - alpha) * ewm[i-1]`, seeded with the
/// first non-NULL value — pandas' `ewm(alpha=…, adjust=False).mean()`.
///
/// A NULL contributes no observation: the state carries across it unchanged
/// and is what the row reports, exactly as `rolling_mean` still reports its
/// window's mean at a NULL row. The statistic is a property of the samples
/// seen so far, and a row with no sample does not undefine it. Folding the
/// NaN into the recurrence, which is what this used to do, made every row
/// after the first NULL NULL as well.
///
/// Matches pandas' `ewm(alpha=…, adjust=False, ignore_na=True).mean()`.
#[must_use]
pub fn ewm(values: &[f64], alpha: f64) -> Vec<f64> {
    let n = values.len();
    let alpha = alpha.clamp(f64::EPSILON, 1.0);
    let mut out = vec![f64::NAN; n];
    let mut state: Option<f64> = None;
    for i in 0..n {
        let v = values[i];
        if v.is_finite() {
            state = Some(match state {
                None => v,
                Some(prev) => alpha * v + (1.0 - alpha) * prev,
            });
        }
        // Before the first sample there is no state and the row is NULL;
        // after it, a NULL row reports the state unchanged.
        if let Some(cur) = state {
            out[i] = cur;
        }
    }
    out
}

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
        // A *sample* standard deviation of one observation divides by
        // `n - 1 = 0`: it is undefined, not zero. This used to return 0.0
        // under the comment "window of 1 has zero variance", which asserts
        // that a single reading has been observed not to vary. pandas'
        // `.rolling(1).std()` and `numpy.std(ddof=1)` both answer NaN, and
        // every other member of this family uses the sample form.
        let v = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let r = rolling_std(&v, 1);
        assert_eq!(r.len(), v.len());
        for val in &r {
            assert!(
                val.is_nan(),
                "rolling_std with window=1 is undefined, got {val}"
            );
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
