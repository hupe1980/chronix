//! Correlation analysis: Pearson, Spearman, Kendall, rolling, lag, cross-correlation.

use crate::compute::{simd_dot_product, simd_mean};

use crate::multivariate::error::MultivariateError;

/// Pairwise NaN deletion — keeps only indices where both x and y are finite.
fn filter_nan_pairs(x: &[f64], y: &[f64]) -> (Vec<f64>, Vec<f64>) {
    let (mut xf, mut yf) = (Vec::with_capacity(x.len()), Vec::with_capacity(y.len()));
    for (&xv, &yv) in x.iter().zip(y.iter()) {
        if xv.is_finite() && yv.is_finite() {
            xf.push(xv);
            yf.push(yv);
        }
    }
    (xf, yf)
}

/// Common trait for pairwise correlation methods.
pub trait CorrelationMethod: Send + Sync {
    /// Compute the correlation coefficient between two equal-length slices.
    fn compute(&self, x: &[f64], y: &[f64]) -> Result<f64, MultivariateError>;

    /// Whether this method supports incremental (online) updates.
    fn supports_incremental(&self) -> bool;
}

/// Pearson correlation coefficient.
pub struct PearsonCorrelation;

impl CorrelationMethod for PearsonCorrelation {
    fn compute(&self, x: &[f64], y: &[f64]) -> Result<f64, MultivariateError> {
        Self::compute(x, y)
    }
    fn supports_incremental(&self) -> bool {
        true
    }
}

impl PearsonCorrelation {
    /// Compute Pearson correlation between two equal-length slices.
    ///
    /// `NaN` pairs are deleted pairwise. When either series has **zero
    /// variance** the correlation is undefined and the result is `NaN`, not
    /// `0.0` — the convention `RollingCorrelation` and
    /// `rolling_corr`, applied here for the same reason: `0.0` asserts "these
    /// series are uncorrelated" where the truth is "there is nothing to
    /// correlate", and a caller acts on the first.
    ///
    /// # Errors
    ///
    /// [`MultivariateError::DimensionMismatch`] on unequal lengths, and
    /// [`MultivariateError::InsufficientData`] with fewer than two usable
    /// pairs.
    pub fn compute(x: &[f64], y: &[f64]) -> Result<f64, MultivariateError> {
        if x.len() != y.len() {
            return Err(MultivariateError::DimensionMismatch {
                expected: x.len(),
                got: y.len(),
            });
        }
        // Pairwise NaN deletion
        let (x, y) = filter_nan_pairs(x, y);
        if x.len() < 2 {
            return Err(MultivariateError::InsufficientData {
                min: 2,
                got: x.len(),
            });
        }
        let mx = simd_mean(&x);
        let my = simd_mean(&y);
        let dx: Vec<f64> = x.iter().map(|v| v - mx).collect();
        let dy: Vec<f64> = y.iter().map(|v| v - my).collect();
        let cov = simd_dot_product(&dx, &dy).map_err(|_| MultivariateError::InsufficientData {
            min: 2,
            got: dx.len(),
        })?;
        let sx = simd_dot_product(&dx, &dx)
            .map_err(|_| MultivariateError::InsufficientData {
                min: 2,
                got: dx.len(),
            })?
            .sqrt();
        let sy = simd_dot_product(&dy, &dy)
            .map_err(|_| MultivariateError::InsufficientData {
                min: 2,
                got: dy.len(),
            })?
            .sqrt();
        if sx < 1e-15 || sy < 1e-15 {
            return Ok(f64::NAN);
        }
        Ok(cov / (sx * sy))
    }
}

/// Spearman rank correlation.
pub struct SpearmanCorrelation;

impl CorrelationMethod for SpearmanCorrelation {
    fn compute(&self, x: &[f64], y: &[f64]) -> Result<f64, MultivariateError> {
        Self::compute(x, y)
    }
    fn supports_incremental(&self) -> bool {
        false
    }
}

impl SpearmanCorrelation {
    /// Compute Spearman correlation (rank-based).
    pub fn compute(x: &[f64], y: &[f64]) -> Result<f64, MultivariateError> {
        if x.len() != y.len() {
            return Err(MultivariateError::DimensionMismatch {
                expected: x.len(),
                got: y.len(),
            });
        }
        // Pairwise NaN deletion
        let (x, y) = filter_nan_pairs(x, y);
        if x.len() < 2 {
            return Err(MultivariateError::InsufficientData {
                min: 2,
                got: x.len(),
            });
        }
        let rx = rank(&x);
        let ry = rank(&y);
        PearsonCorrelation::compute(&rx, &ry)
    }
}

fn rank(data: &[f64]) -> Vec<f64> {
    let n = data.len();
    let mut indexed: Vec<(usize, f64)> = data.iter().copied().enumerate().collect();
    indexed.sort_by(|a, b| a.1.total_cmp(&b.1));
    let mut ranks = vec![0.0; n];
    let mut i = 0;
    while i < n {
        let mut j = i;
        while j < n && (indexed[j].1 - indexed[i].1).abs() < 1e-15 {
            j += 1;
        }
        let avg_rank = (i + j + 1) as f64 / 2.0;
        for item in &indexed[i..j] {
            ranks[item.0] = avg_rank;
        }
        i = j;
    }
    ranks
}

/// Kendall Tau-a rank correlation.
pub struct KendallTau;

impl CorrelationMethod for KendallTau {
    fn compute(&self, x: &[f64], y: &[f64]) -> Result<f64, MultivariateError> {
        Self::compute(x, y)
    }
    fn supports_incremental(&self) -> bool {
        false
    }
}

impl KendallTau {
    /// Compute Kendall Tau-b using O(n log n) merge-sort algorithm (Knight 2006).
    ///
    /// Counts discordant pairs via merge-sort-based inversion counting on the
    /// y-ranks after sorting by x-ranks.  Tau-b accounts for ties on both
    /// x and y through the denominator √((n₀−n₁)(n₀−n₂)).
    pub fn compute(x: &[f64], y: &[f64]) -> Result<f64, MultivariateError> {
        if x.len() != y.len() {
            return Err(MultivariateError::DimensionMismatch {
                expected: x.len(),
                got: y.len(),
            });
        }
        // Pairwise NaN deletion
        let (x, y) = filter_nan_pairs(x, y);
        let n = x.len();
        if n < 2 {
            return Err(MultivariateError::InsufficientData { min: 2, got: n });
        }

        // Sort pairs by x, breaking ties by y
        let mut pairs: Vec<(f64, f64)> = x.iter().copied().zip(y.iter().copied()).collect();
        pairs.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.total_cmp(&b.1)));

        // Extract y-values in x-sorted order; count inversions via merge sort
        let mut y_sorted: Vec<f64> = pairs.iter().map(|p| p.1).collect();
        let mut scratch = vec![0.0f64; n];
        let discordant = Self::merge_sort_count(&mut y_sorted, &mut scratch);

        let n0 = (n * (n - 1) / 2) as f64; // total pairs

        // Count tied pairs on x
        let mut ties_x: i64 = 0;
        let mut i = 0;
        while i < n {
            let mut j = i + 1;
            while j < n && (pairs[j].0 - pairs[i].0).abs() < 1e-15 {
                j += 1;
            }
            let run = (j - i) as i64;
            ties_x += run * (run - 1) / 2;
            i = j;
        }

        // Count tied pairs on y (from the sorted y values — re-sort by y)
        let mut y_vals: Vec<f64> = pairs.iter().map(|p| p.1).collect();
        y_vals.sort_by(f64::total_cmp);
        let mut ties_y: i64 = 0;
        let mut i = 0;
        while i < n {
            let mut j = i + 1;
            while j < n && (y_vals[j] - y_vals[i]).abs() < 1e-15 {
                j += 1;
            }
            let run = (j - i) as i64;
            ties_y += run * (run - 1) / 2;
            i = j;
        }

        // Count joint ties (tied on BOTH x and y).
        let mut ties_xy: i64 = 0;
        let mut i = 0;
        while i < n {
            let mut j = i + 1;
            while j < n
                && (pairs[j].0 - pairs[i].0).abs() < 1e-15
                && (pairs[j].1 - pairs[i].1).abs() < 1e-15
            {
                j += 1;
            }
            let run = (j - i) as i64;
            ties_xy += run * (run - 1) / 2;
            i = j;
        }

        // concordant = n0 - discordant - (ties_x - ties_xy) - (ties_y - ties_xy) - ties_xy
        //            = n0 - discordant - ties_x - ties_y + ties_xy
        let concordant = n0 as i64 - discordant as i64 - ties_x - ties_y + ties_xy;
        let denom = ((n0 - ties_x as f64) * (n0 - ties_y as f64)).sqrt();
        if denom < 1e-15 {
            return Ok(0.0);
        }
        let tau = (concordant - discordant as i64) as f64 / denom;
        Ok(tau.clamp(-1.0, 1.0))
    }

    /// Merge-sort-based inversion count — O(n log n).
    ///
    /// Uses `scratch` as a pre-allocated temporary buffer to avoid
    /// per-recursion allocations.  `scratch` must be at least as long
    /// as `arr`.
    fn merge_sort_count(arr: &mut [f64], scratch: &mut [f64]) -> usize {
        let n = arr.len();
        if n <= 1 {
            return 0;
        }
        let mid = n / 2;

        let mut count = Self::merge_sort_count(&mut arr[..mid], &mut scratch[..mid])
            + Self::merge_sort_count(&mut arr[mid..], &mut scratch[mid..n]);

        // Merge into scratch, then copy back
        let (mut i, mut j, mut k) = (0, 0, 0);
        while i < mid && j < n - mid {
            if arr[i] <= arr[mid + j] {
                scratch[k] = arr[i];
                i += 1;
            } else {
                scratch[k] = arr[mid + j];
                count += mid - i; // all remaining left elements are inversions
                j += 1;
            }
            k += 1;
        }
        while i < mid {
            scratch[k] = arr[i];
            i += 1;
            k += 1;
        }
        while j < n - mid {
            scratch[k] = arr[mid + j];
            j += 1;
            k += 1;
        }
        arr[..n].copy_from_slice(&scratch[..n]);
        count
    }
}

/// Rolling Pearson correlation with O(1) update per point.
pub struct RollingCorrelation {
    window: usize,
}

impl RollingCorrelation {
    /// Creates a new rolling correlator with the given window size.
    pub fn new(window: usize) -> Self {
        Self {
            window: window.max(1),
        }
    }

    /// Compute rolling Pearson correlation between two series.
    pub fn compute(&self, x: &[f64], y: &[f64]) -> Result<Vec<f64>, MultivariateError> {
        if x.len() != y.len() {
            return Err(MultivariateError::DimensionMismatch {
                expected: x.len(),
                got: y.len(),
            });
        }
        let n = x.len();
        if n < self.window {
            return Err(MultivariateError::InsufficientData {
                min: self.window,
                got: n,
            });
        }

        let mut result = Vec::with_capacity(n);
        let mut sx = 0.0;
        let mut sy = 0.0;
        let mut sxx = 0.0;
        let mut syy = 0.0;
        let mut sxy = 0.0;

        for i in 0..n {
            sx += x[i];
            sy += y[i];
            sxx += x[i] * x[i];
            syy += y[i] * y[i];
            sxy += x[i] * y[i];

            if i >= self.window {
                let old = i - self.window;
                sx -= x[old];
                sy -= y[old];
                sxx -= x[old] * x[old];
                syy -= y[old] * y[old];
                sxy -= x[old] * y[old];
            }

            // Periodic FP drift recomputation every 1024 steps.
            if i > 0 && i % 1024 == 0 {
                let start = i.saturating_sub(self.window.saturating_sub(1));
                sx = x[start..=i].iter().sum();
                sy = y[start..=i].iter().sum();
                sxx = x[start..=i].iter().map(|&v| v * v).sum();
                syy = y[start..=i].iter().map(|&v| v * v).sum();
                sxy = x[start..=i]
                    .iter()
                    .zip(&y[start..=i])
                    .map(|(&a, &b)| a * b)
                    .sum();
            }

            // Emit NaN until the window is full, and for an
            // undefined correlation.
            //
            // This function previously reported a value computed from a
            // *partial* window for the first `window - 1` rows, and `0.0`
            // when either series had zero variance. Both are wrong and they
            // disagreed with `preprocess::rolling_corr`, which returns NaN in
            // exactly those cases — two functions with the same name and
            // purpose gave different answers for the same input.
            //
            // NaN is also the semantically correct value: `0.0` claims "these
            // series are uncorrelated" when the truth is "correlation is
            // undefined here". This matches pandas
            // `rolling(window).corr()`, whose `min_periods` defaults to the
            // window size.
            if i + 1 < self.window {
                result.push(f64::NAN);
                continue;
            }

            let count = (i + 1).min(self.window) as f64;
            let mx = sx / count;
            let my = sy / count;
            let cov = sxy / count - mx * my;
            let vx = (sxx / count - mx * mx).max(0.0);
            let vy = (syy / count - my * my).max(0.0);
            let denom = (vx * vy).sqrt();
            if denom < 1e-15 {
                result.push(f64::NAN);
            } else {
                result.push(cov / denom);
            }
        }
        Ok(result)
    }
}

/// Lag correlation: Pearson at various time offsets.
pub struct LagCorrelation;

impl LagCorrelation {
    /// Compute Pearson correlation at each lag in `lags`.
    ///
    /// A lag whose correlation is **undefined** — the shift consumes the whole
    /// series, or either window has zero variance — yields `NaN`, not `0.0`.
    /// `0.0` is the worse answer of the two because it conflates "uncorrelated"
    /// with "there is not enough information to say", and it is *actionable*
    /// where a `NaN` is visibly absent (the same convention
    /// [`RollingCorrelation`] follows).
    ///
    /// Cost is O(n) per lag, so O(n·|lags|) overall — ask for the lags wanted
    /// rather than a range and then one of them.
    ///
    /// # Errors
    ///
    /// [`MultivariateError::DimensionMismatch`] if `x` and `y` differ in
    /// length.
    pub fn compute(
        x: &[f64],
        y: &[f64],
        lags: &[i32],
    ) -> Result<Vec<(i32, f64)>, MultivariateError> {
        if x.len() != y.len() {
            return Err(MultivariateError::DimensionMismatch {
                expected: x.len(),
                got: y.len(),
            });
        }
        let mut results = Vec::with_capacity(lags.len());
        for &lag in lags {
            let (xs, ys) = if lag >= 0 {
                let l = lag as usize;
                if l >= x.len() {
                    results.push((lag, f64::NAN));
                    continue;
                }
                (&x[l..], &y[..x.len() - l])
            } else {
                let l = (-lag) as usize;
                if l >= y.len() {
                    results.push((lag, f64::NAN));
                    continue;
                }
                (&x[..y.len() - l], &y[l..])
            };
            let r = PearsonCorrelation::compute(xs, ys).unwrap_or(f64::NAN);
            results.push((lag, r));
        }
        Ok(results)
    }
}

/// Full N×N pairwise Pearson correlation matrix.
pub struct CrossCorrelationMatrix;

impl CrossCorrelationMatrix {
    /// Compute pairwise Pearson correlation for all series.
    ///
    /// Returns a symmetric N×N matrix.
    pub fn compute(data: &[&[f64]]) -> Result<Vec<Vec<f64>>, MultivariateError> {
        let _start = std::time::Instant::now();
        let n = data.len();
        let mut matrix = vec![vec![0.0; n]; n];
        for i in 0..n {
            matrix[i][i] = 1.0;
            for j in (i + 1)..n {
                let r = PearsonCorrelation::compute(data[i], data[j]).unwrap_or(f64::NAN);
                matrix[i][j] = r;
                matrix[j][i] = r;
            }
        }
        metrics::histogram!("chronix_correlation_compute_duration_seconds")
            .record(_start.elapsed().as_secs_f64());
        Ok(matrix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pearson_perfectly_correlated() {
        let x: Vec<f64> = (0..100).map(|i| i as f64).collect();
        let y: Vec<f64> = (0..100).map(|i| i as f64 * 2.0 + 5.0).collect();
        let r = PearsonCorrelation::compute(&x, &y).unwrap();
        assert!((r - 1.0).abs() < 1e-6);
    }

    #[test]
    fn pearson_anti_correlated() {
        let x: Vec<f64> = (0..100).map(|i| i as f64).collect();
        let y: Vec<f64> = (0..100).map(|i| -(i as f64)).collect();
        let r = PearsonCorrelation::compute(&x, &y).unwrap();
        assert!((r - (-1.0)).abs() < 1e-6);
    }

    #[test]
    fn spearman_rank_correlation() {
        let x: Vec<f64> = (0..50).map(|i| i as f64).collect();
        let y: Vec<f64> = (0..50).map(|i| (i as f64).powi(2)).collect();
        let r = SpearmanCorrelation::compute(&x, &y).unwrap();
        // Monotonic relationship → Spearman = 1.0
        assert!((r - 1.0).abs() < 1e-6);
    }

    #[test]
    fn kendall_tau() {
        let x: Vec<f64> = (0..20).map(|i| i as f64).collect();
        let y: Vec<f64> = (0..20).map(|i| i as f64 * 3.0).collect();
        let r = KendallTau::compute(&x, &y).unwrap();
        assert!((r - 1.0).abs() < 1e-6);
    }

    /// `RollingCorrelation` and `preprocess::rolling_corr` compute
    /// the same statistic and must agree on conventions — partial windows and
    /// undefined correlations are NaN in both.
    #[test]
    fn rolling_correlation_conventions_match_preprocess() {
        let n = 60;
        let window = 10;
        let x: Vec<f64> = (0..n).map(|i| (i as f64 * 0.7).sin()).collect();
        let y: Vec<f64> = (0..n).map(|i| (i as f64 * 0.7).cos()).collect();

        let ours = RollingCorrelation::new(window).compute(&x, &y).unwrap();
        let theirs = crate::preprocess::rolling_corr(&x, &y, window);

        assert_eq!(ours.len(), theirs.len());
        for i in 0..n {
            assert_eq!(
                ours[i].is_nan(),
                theirs[i].is_nan(),
                "NaN convention differs at index {i}: {} vs {}",
                ours[i],
                theirs[i]
            );
            if !ours[i].is_nan() {
                assert!(
                    (ours[i] - theirs[i]).abs() < 1e-9,
                    "values differ at index {i}: {} vs {}",
                    ours[i],
                    theirs[i]
                );
            }
        }

        // And the convention is specifically: NaN until the window is full.
        assert!(ours[..window - 1].iter().all(|v| v.is_nan()));
        assert!(!ours[window - 1].is_nan());
    }

    /// A constant series has zero variance, so the correlation is undefined —
    /// not zero. Reporting `0.0` claimed "uncorrelated" for "unknown".
    #[test]
    fn zero_variance_is_nan_not_zero() {
        let x = vec![5.0; 40];
        let y: Vec<f64> = (0..40).map(|i| i as f64).collect();
        let r = RollingCorrelation::new(10).compute(&x, &y).unwrap();
        assert!(
            r[39].is_nan(),
            "zero-variance correlation should be NaN, got {}",
            r[39]
        );
    }

    #[test]
    fn rolling_matches_offline() {
        let n = 200;
        let x: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let y: Vec<f64> = (0..n).map(|i| (i as f64 * 0.5) + 3.0).collect();
        let rc = RollingCorrelation::new(50);
        let result = rc.compute(&x, &y).unwrap();
        // After the first window, should be close to 1.0
        assert!((result[n - 1] - 1.0).abs() < 1e-3);
    }

    #[test]
    fn cross_correlation_matrix() {
        let a: Vec<f64> = (0..100).map(|i| i as f64).collect();
        let b: Vec<f64> = (0..100).map(|i| i as f64 * 2.0).collect();
        let c: Vec<f64> = (0..100).map(|i| -(i as f64)).collect();
        let data: Vec<&[f64]> = vec![&a, &b, &c];
        let mat = CrossCorrelationMatrix::compute(&data).unwrap();
        assert_eq!(mat.len(), 3);
        assert!((mat[0][1] - 1.0).abs() < 1e-6); // a and b perfectly correlated
        assert!((mat[0][2] - (-1.0)).abs() < 1e-6); // a and c anti-correlated
        assert!((mat[0][0] - 1.0).abs() < 1e-15); // diagonal = 1
    }

    #[test]
    fn lag_correlation() {
        let x: Vec<f64> = (0..100).map(|i| i as f64).collect();
        let y: Vec<f64> = (0..100).map(|i| i as f64).collect();
        let results = LagCorrelation::compute(&x, &y, &[-1, 0, 1]).unwrap();
        assert_eq!(results.len(), 3);
        // At lag 0 should be ~1.0
        assert!((results[1].1 - 1.0).abs() < 1e-3);
    }

    /// An undefined lag correlation is `NaN`, never `0.0`. `0.0` asserts
    /// "these series are uncorrelated" where the truth is "there is not enough
    /// information to say", and a caller acts on the first and ignores the
    /// second.
    #[test]
    fn an_undefined_lag_correlation_is_nan() {
        let x = vec![1.0, 2.0, 3.0, 4.0];
        let y = vec![2.0, 4.0, 6.0, 8.0];

        // A shift longer than the series leaves nothing to correlate.
        let out = LagCorrelation::compute(&x, &y, &[10, -10]).unwrap();
        assert!(out.iter().all(|(_, r)| r.is_nan()), "{out:?}");

        // Zero variance in one window is undefined too.
        let flat = vec![7.0; 4];
        let out = LagCorrelation::compute(&flat, &y, &[0]).unwrap();
        assert!(out[0].1.is_nan(), "{out:?}");

        // A defined lag is still a number.
        let out = LagCorrelation::compute(&x, &y, &[0]).unwrap();
        assert!((out[0].1 - 1.0).abs() < 1e-9, "{out:?}");
    }

    #[test]
    fn correlation_method_trait_dispatch() {
        let x: Vec<f64> = (0..50).map(|i| i as f64).collect();
        let y: Vec<f64> = (0..50).map(|i| i as f64 * 2.0 + 1.0).collect();

        // Use trait objects for dynamic dispatch
        let methods: Vec<Box<dyn CorrelationMethod>> = vec![
            Box::new(PearsonCorrelation),
            Box::new(SpearmanCorrelation),
            Box::new(KendallTau),
        ];

        for method in &methods {
            let r = method.compute(&x, &y).unwrap();
            assert!(
                (r - 1.0).abs() < 1e-6,
                "{}: expected ~1.0, got {}",
                if method.supports_incremental() {
                    "Pearson"
                } else {
                    "other"
                },
                r
            );
        }

        // Verify supports_incremental flag
        assert!(PearsonCorrelation.supports_incremental());
        assert!(!SpearmanCorrelation.supports_incremental());
        assert!(!KendallTau.supports_incremental());
    }
}
