//! Granger causality test for VAR models.
//!
//! Tests whether one time-series "Granger-causes" another by comparing
//! the residual sum of squares (RSS) of restricted vs. unrestricted OLS
//! models.  An F-test determines statistical significance.
//!
//! ## Algorithm
//!
//! 1. **Unrestricted model:** Regress the effect series on all lagged
//!    predictors (including the candidate cause).
//! 2. **Restricted model:** Regress the effect series on all lagged
//!    predictors *excluding* the candidate cause.
//! 3. **F-statistic:**
//!    $$F = \frac{(RSS_r - RSS_u) / p}{RSS_u / (T - kp - 1)}$$
//!    where $p$ is the lag order, $k$ is the number of series, and $T$
//!    is the number of usable observations.
//! 4. Compare $F$ to the F-distribution with $(p, T - kp - 1)$ degrees
//!    of freedom.

use crate::compute::{ComputeEngine, CpuEngine};
use crate::multivariate::context::MultiSeriesContext;
use crate::multivariate::error::MultivariateError;

/// Result of a Granger causality test.
#[derive(Debug, Clone)]
pub struct GrangerResult {
    /// The computed F-statistic.
    pub f_statistic: f64,
    /// Approximate p-value from the F-distribution.
    pub p_value: f64,
    /// Whether causality is statistically significant at the given
    /// significance level.
    pub significant: bool,
    /// Lag order used in the test.
    pub lag_order: usize,
    /// Index of the candidate cause series.
    pub cause_idx: usize,
    /// Index of the effect series.
    pub effect_idx: usize,
}

/// Perform a Granger causality test.
///
/// Tests whether series `cause_idx` Granger-causes series `effect_idx`
/// in the context `ctx`, using `lag_order` lags.
///
/// This variant uses a standard F-test that assumes homoscedastic errors.
/// For heteroscedastic data (e.g. financial time series), use
/// [`granger_causality_test_robust`] instead, which applies an HC3
/// covariance estimator.
///
/// # Arguments
///
/// * `ctx` — aligned multi-series context
/// * `cause_idx` — column index of the candidate cause
/// * `effect_idx` — column index of the effect
/// * `lag_order` — number of lags (VAR order *p*)
/// * `significance` — significance level (e.g. 0.05)
///
/// # Errors
///
/// Returns `MultivariateError` if there is insufficient data, if indices
/// are out of range, or if cause and effect are the same series.
pub fn granger_causality_test(
    ctx: &MultiSeriesContext,
    cause_idx: usize,
    effect_idx: usize,
    lag_order: usize,
    significance: f64,
) -> Result<GrangerResult, MultivariateError> {
    let k = ctx.matrix.n_series();
    let n = ctx.matrix.n_timestamps();
    let p = lag_order;

    if cause_idx >= k {
        return Err(MultivariateError::SeriesNotFound(format!(
            "cause_idx {cause_idx} >= n_series {k}"
        )));
    }
    if effect_idx >= k {
        return Err(MultivariateError::SeriesNotFound(format!(
            "effect_idx {effect_idx} >= n_series {k}"
        )));
    }
    if cause_idx == effect_idx {
        return Err(MultivariateError::InvalidParameter(
            "cause and effect must be different series".into(),
        ));
    }
    if p == 0 {
        return Err(MultivariateError::InvalidParameter(
            "lag_order must be >= 1".into(),
        ));
    }

    let t = n.saturating_sub(p); // usable observations
    let dim_u = 1 + p * k; // unrestricted: intercept + p lags × k series
    let _dim_r = 1 + p * (k - 1); // restricted: exclude cause series lags
    let df_num = p as f64; // numerator df
    let df_den = t as f64 - dim_u as f64; // denominator df

    if t < dim_u + 1 {
        return Err(MultivariateError::InsufficientData {
            min: dim_u + p + 1,
            got: n,
        });
    }
    if df_den <= 0.0 {
        return Err(MultivariateError::InsufficientData {
            min: dim_u + p + 1,
            got: n,
        });
    }

    let engine = CpuEngine::default();

    // --- Unrestricted model (all predictors) ---
    let rss_u = ols_rss(ctx, effect_idx, None, p, &engine)?;

    // --- Restricted model (exclude cause_idx) ---
    let rss_r = ols_rss(ctx, effect_idx, Some(cause_idx), p, &engine)?;

    // F-statistic
    let f_stat = if rss_u.abs() < 1e-30 {
        // Perfect fit in unrestricted model — numerically F → ∞
        // but practically means the cause adds no information.
        0.0
    } else {
        ((rss_r - rss_u) / df_num) / (rss_u / df_den)
    };

    // Approximate p-value using the regularized incomplete beta function.
    let p_value = f_distribution_sf(f_stat.max(0.0), df_num, df_den);
    let significant = p_value < significance;

    Ok(GrangerResult {
        f_statistic: f_stat.max(0.0),
        p_value,
        significant,
        lag_order: p,
        cause_idx,
        effect_idx,
    })
}

/// Heteroscedasticity-consistent (HC3) Granger causality test.
///
/// Same hypothesis as [`granger_causality_test`] but uses the HC3 (MacKinnon &
/// White, 1985) covariance estimator, which is valid under arbitrary
/// conditional heteroscedasticity.  The test statistic is a Wald test:
///
/// $$W = (R\hat\beta)^\top \bigl(R\,\hat V_{\text{HC3}}\,R^\top\bigr)^{-1}
///       (R\hat\beta)$$
///
/// where $R$ selects the $p$ coefficients corresponding to the candidate
/// cause lags.  Under $H_0$, $W/p \sim F(p, T - kp - 1)$ approximately.
pub fn granger_causality_test_robust(
    ctx: &MultiSeriesContext,
    cause_idx: usize,
    effect_idx: usize,
    lag_order: usize,
    significance: f64,
) -> Result<GrangerResult, MultivariateError> {
    let k = ctx.matrix.n_series();
    let n = ctx.matrix.n_timestamps();
    let p = lag_order;

    if cause_idx >= k {
        return Err(MultivariateError::SeriesNotFound(format!(
            "cause_idx {cause_idx} >= n_series {k}"
        )));
    }
    if effect_idx >= k {
        return Err(MultivariateError::SeriesNotFound(format!(
            "effect_idx {effect_idx} >= n_series {k}"
        )));
    }
    if cause_idx == effect_idx {
        return Err(MultivariateError::InvalidParameter(
            "cause and effect must be different series".into(),
        ));
    }
    if p == 0 {
        return Err(MultivariateError::InvalidParameter(
            "lag_order must be >= 1".into(),
        ));
    }

    let t = n.saturating_sub(p);
    let dim = 1 + p * k; // unrestricted model dimension
    let df_den = t as f64 - dim as f64;

    if t < dim + 1 {
        return Err(MultivariateError::InsufficientData {
            min: dim + p + 1,
            got: n,
        });
    }
    if df_den <= 0.0 {
        return Err(MultivariateError::InsufficientData {
            min: dim + p + 1,
            got: n,
        });
    }

    let engine = CpuEngine::default();

    // Build X (t × dim) and y (t × 1).
    let mut x_mat = vec![0.0_f64; t * dim];
    let mut y_vec = vec![0.0_f64; t];

    for (row, obs) in (p..n).enumerate() {
        y_vec[row] = ctx.matrix.data[effect_idx][obs];
        x_mat[row * dim] = 1.0; // intercept
        let mut col = 1;
        for l in 1..=p {
            for s in 0..k {
                x_mat[row * dim + col] = ctx.matrix.data[s][obs - l];
                col += 1;
            }
        }
    }

    // X'X (dim × dim)
    let mut xtx = vec![0.0_f64; dim * dim];
    for row in 0..t {
        for i in 0..dim {
            let xi = x_mat[row * dim + i];
            for j in i..dim {
                let v = xi * x_mat[row * dim + j];
                xtx[i * dim + j] += v;
                if i != j {
                    xtx[j * dim + i] += v;
                }
            }
        }
    }

    // X'y (dim)
    let mut xty = vec![0.0_f64; dim];
    for row in 0..t {
        let y = y_vec[row];
        for i in 0..dim {
            xty[i] += x_mat[row * dim + i] * y;
        }
    }

    // Ridge regularization
    for i in 0..dim {
        xtx[i * dim + i] += 1e-8;
    }

    // β = (X'X)^{-1} X'y
    let beta = engine
        .batch_matrix_solve(&xtx, &xty, dim)
        .map_err(|e| MultivariateError::Compute(e.to_string()))?;

    // Residuals  e_i = y_i - x_i'β
    let mut residuals = vec![0.0_f64; t];
    for row in 0..t {
        let mut fitted = 0.0;
        for j in 0..dim {
            fitted += x_mat[row * dim + j] * beta[j];
        }
        residuals[row] = y_vec[row] - fitted;
    }

    // Compute (X'X)^{-1}  via solving dim identity columns.
    let mut xtx_inv = vec![0.0_f64; dim * dim];
    for col in 0..dim {
        let mut rhs = vec![0.0_f64; dim];
        rhs[col] = 1.0;
        let sol = engine
            .batch_matrix_solve(&xtx, &rhs, dim)
            .map_err(|e| MultivariateError::Compute(e.to_string()))?;
        for row in 0..dim {
            xtx_inv[row * dim + col] = sol[row];
        }
    }

    // Hat matrix diagonal: h_ii = x_i' (X'X)^{-1} x_i
    let mut hat_diag = vec![0.0_f64; t];
    for row in 0..t {
        let mut h = 0.0;
        for i in 0..dim {
            let mut inner = 0.0;
            for j in 0..dim {
                inner += xtx_inv[i * dim + j] * x_mat[row * dim + j];
            }
            h += x_mat[row * dim + i] * inner;
        }
        hat_diag[row] = h.clamp(0.0, 1.0 - 1e-12);
    }

    // HC3 "meat" matrix: M = Σ_i  x_i x_i' * e_i^2 / (1 - h_ii)^2
    let mut meat = vec![0.0_f64; dim * dim];
    for row in 0..t {
        let denom = (1.0 - hat_diag[row]).powi(2);
        let w = residuals[row].powi(2) / denom;
        for i in 0..dim {
            let xi_w = x_mat[row * dim + i] * w;
            for j in i..dim {
                let v = xi_w * x_mat[row * dim + j];
                meat[i * dim + j] += v;
                if i != j {
                    meat[j * dim + i] += v;
                }
            }
        }
    }

    // V_HC3 = (X'X)^{-1} M (X'X)^{-1}   (dim × dim)
    // First compute tmp = (X'X)^{-1} M
    let mut tmp = vec![0.0_f64; dim * dim];
    for i in 0..dim {
        for j in 0..dim {
            let mut s = 0.0;
            for c in 0..dim {
                s += xtx_inv[i * dim + c] * meat[c * dim + j];
            }
            tmp[i * dim + j] = s;
        }
    }
    // V_HC3 = tmp * (X'X)^{-1}
    let mut v_hc3 = vec![0.0_f64; dim * dim];
    for i in 0..dim {
        for j in 0..dim {
            let mut s = 0.0;
            for c in 0..dim {
                s += tmp[i * dim + c] * xtx_inv[c * dim + j];
            }
            v_hc3[i * dim + j] = s;
        }
    }

    // R selects the p coefficients of the cause series lags.
    // In the unrestricted model, columns are:
    //   [intercept, lag1_series0, lag1_series1, ..., lag2_series0, ...]
    // For lag l, the cause_idx column is at position 1 + (l-1)*k + cause_idx.
    let mut cause_cols = Vec::with_capacity(p);
    for l in 1..=p {
        cause_cols.push(1 + (l - 1) * k + cause_idx);
    }

    // Rβ (p × 1)
    let r_beta: Vec<f64> = cause_cols.iter().map(|&c| beta[c]).collect();

    // R V_HC3 R'  (p × p)
    let mut r_v_rt = vec![0.0_f64; p * p];
    for (i, &ci) in cause_cols.iter().enumerate() {
        for (j, &cj) in cause_cols.iter().enumerate() {
            r_v_rt[i * p + j] = v_hc3[ci * dim + cj];
        }
    }

    // Ridge regularize the small p×p matrix
    for i in 0..p {
        r_v_rt[i * p + i] += 1e-12;
    }

    // Wald = (Rβ)' (R V R')^{-1} (Rβ)
    let inv_r_beta = engine
        .batch_matrix_solve(&r_v_rt, &r_beta, p)
        .map_err(|e| MultivariateError::Compute(e.to_string()))?;

    let wald: f64 = r_beta
        .iter()
        .zip(inv_r_beta.iter())
        .map(|(a, b)| a * b)
        .sum();

    // F = W / p, compare with F(p, T - dim)
    let f_stat = (wald / p as f64).max(0.0);
    let p_value = f_distribution_sf(f_stat, p as f64, df_den);
    let significant = p_value < significance;

    Ok(GrangerResult {
        f_statistic: f_stat,
        p_value,
        significant,
        lag_order: p,
        cause_idx,
        effect_idx,
    })
}

/// Compute the RSS (residual sum of squares) from an OLS regression of
/// `effect_idx` on lagged predictors.  When `exclude_idx` is `Some(j)`,
/// the lags of series `j` are excluded (restricted model).
fn ols_rss(
    ctx: &MultiSeriesContext,
    effect_idx: usize,
    exclude_idx: Option<usize>,
    p: usize,
    engine: &CpuEngine,
) -> Result<f64, MultivariateError> {
    let k = ctx.matrix.n_series();
    let n = ctx.matrix.n_timestamps();
    let _t = n - p;

    let dim = if exclude_idx.is_some() {
        1 + p * (k - 1)
    } else {
        1 + p * k
    };

    let mut xtx = vec![0.0; dim * dim];
    let mut xty = vec![0.0; dim];
    let mut yty = 0.0;

    for obs in p..n {
        let y = ctx.matrix.data[effect_idx][obs];
        yty += y * y;

        let mut row = Vec::with_capacity(dim);
        row.push(1.0); // intercept
        for l in 1..=p {
            for s in 0..k {
                if exclude_idx == Some(s) {
                    continue;
                }
                row.push(ctx.matrix.data[s][obs - l]);
            }
        }
        debug_assert_eq!(row.len(), dim);

        for i in 0..dim {
            for j in 0..dim {
                xtx[i * dim + j] += row[i] * row[j];
            }
            xty[i] += row[i] * y;
        }
    }

    // Ridge regularization
    for i in 0..dim {
        xtx[i * dim + i] += 1e-8;
    }

    let beta = engine
        .batch_matrix_solve(&xtx, &xty, dim)
        .map_err(|e| MultivariateError::Compute(e.to_string()))?;

    // RSS = Y'Y - β'X'Y
    let beta_xty: f64 = beta.iter().zip(xty.iter()).map(|(b, xy)| b * xy).sum();
    let rss = (yty - beta_xty).max(0.0);

    Ok(rss)
}

// ── F-distribution survival function (1 - CDF) ──────────────────────

/// Compute P(F > x) for an F(d1, d2) distribution using the regularized
/// incomplete beta function.
///
/// Survival function: P(F > x) = I_{d2/(d2+d1*x)}(d2/2, d1/2)
/// where I_x(a,b) is the regularized incomplete beta function.
fn f_distribution_sf(x: f64, d1: f64, d2: f64) -> f64 {
    if x <= 0.0 || d1 <= 0.0 || d2 <= 0.0 {
        return 1.0;
    }
    let z = d2 / (d2 + d1 * x);
    regularized_incomplete_beta(z, d2 / 2.0, d1 / 2.0)
}

/// Regularized incomplete beta function I_x(a, b) using the continued
/// fraction expansion (Lentz's algorithm).
///
/// Reference: Press et al., "Numerical Recipes", §6.4.
fn regularized_incomplete_beta(x: f64, a: f64, b: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }

    // Use the symmetry relation when x > (a+1)/(a+b+2) for faster
    // convergence of the continued fraction.
    if x > (a + 1.0) / (a + b + 2.0) {
        return 1.0 - regularized_incomplete_beta(1.0 - x, b, a);
    }

    let ln_prefix = a * x.ln() + b * (1.0 - x).ln() - ln_beta(a, b) - a.ln();
    let prefix = ln_prefix.exp();

    // Lentz's continued fraction
    let max_iter = 200;
    let eps = 1e-14;
    let tiny = 1e-30;

    let mut c = 1.0;
    let mut d = 1.0 - (a + b) * x / (a + 1.0);
    if d.abs() < tiny {
        d = tiny;
    }
    d = 1.0 / d;
    let mut h = d;

    for m in 1..=max_iter {
        let m_f64 = m as f64;

        // Even step: d_{2m}
        let num_even = m_f64 * (b - m_f64) * x / ((a + 2.0 * m_f64 - 1.0) * (a + 2.0 * m_f64));
        d = 1.0 + num_even * d;
        if d.abs() < tiny {
            d = tiny;
        }
        c = 1.0 + num_even / c;
        if c.abs() < tiny {
            c = tiny;
        }
        d = 1.0 / d;
        h *= d * c;

        // Odd step: d_{2m+1}
        let num_odd =
            -((a + m_f64) * (a + b + m_f64)) * x / ((a + 2.0 * m_f64) * (a + 2.0 * m_f64 + 1.0));
        d = 1.0 + num_odd * d;
        if d.abs() < tiny {
            d = tiny;
        }
        c = 1.0 + num_odd / c;
        if c.abs() < tiny {
            c = tiny;
        }
        d = 1.0 / d;
        let delta = d * c;
        h *= delta;

        if (delta - 1.0).abs() < eps {
            break;
        }
    }

    prefix * h
}

/// Natural log of the Beta function: ln(B(a,b)) = ln(Γ(a)) + ln(Γ(b)) - ln(Γ(a+b)).
fn ln_beta(a: f64, b: f64) -> f64 {
    ln_gamma(a) + ln_gamma(b) - ln_gamma(a + b)
}

/// Lanczos approximation for ln(Γ(z)), z > 0.
///
/// Reference: Numerical Recipes §6.1.
fn ln_gamma(z: f64) -> f64 {
    const COEFFS: [f64; 7] = [
        1.000000000190015,
        76.18009172947146,
        -86.50532032941678,
        24.01409824083091,
        -1.231739572450155,
        0.1208650973866179e-2,
        -0.5395239384953e-5,
    ];

    let x = z;
    let tmp = x + 5.5;
    let tmp = (x + 0.5) * tmp.ln() - tmp;
    let mut ser = COEFFS[0];
    for (i, &c) in COEFFS[1..].iter().enumerate() {
        ser += c / (x + i as f64 + 1.0);
    }
    tmp + (2.5066282746310005_f64 * ser / x).ln()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multivariate::context::MultiSeriesContext;

    /// Build a causal dataset: y(t) = 0.8 * x(t-1) + noise.
    fn causal_ctx() -> MultiSeriesContext {
        let n = 200;
        let ts: Vec<i64> = (0..n as i64).map(|i| i * 1_000_000_000).collect();
        // x is a simple AR(1) process
        let mut x = vec![0.0; n];
        let mut rng_state: u64 = 42;
        for i in 1..n {
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let noise = ((rng_state >> 33) as f64 / (1u64 << 31) as f64 - 0.5) * 0.5;
            x[i] = 0.7 * x[i - 1] + noise;
        }
        // y is caused by x with lag 1
        let mut y = vec![0.0; n];
        for i in 1..n {
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let noise = ((rng_state >> 33) as f64 / (1u64 << 31) as f64 - 0.5) * 0.2;
            y[i] = 0.8 * x[i - 1] + noise;
        }
        MultiSeriesContext::build(vec![("x".into(), ts.clone(), x), ("y".into(), ts, y)], None)
            .unwrap()
    }

    /// Build an independent dataset: x and y are unrelated AR(1) processes.
    fn independent_ctx() -> MultiSeriesContext {
        let n = 200;
        let ts: Vec<i64> = (0..n as i64).map(|i| i * 1_000_000_000).collect();
        let mut x = vec![0.0; n];
        let mut y = vec![0.0; n];
        let mut rng_state: u64 = 12345;
        for i in 1..n {
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let noise_x = ((rng_state >> 33) as f64 / (1u64 << 31) as f64 - 0.5) * 0.5;
            x[i] = 0.5 * x[i - 1] + noise_x;
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let noise_y = ((rng_state >> 33) as f64 / (1u64 << 31) as f64 - 0.5) * 0.5;
            y[i] = 0.5 * y[i - 1] + noise_y;
        }
        MultiSeriesContext::build(vec![("x".into(), ts.clone(), x), ("y".into(), ts, y)], None)
            .unwrap()
    }

    #[test]
    fn granger_detects_causality() {
        let ctx = causal_ctx();
        // x Granger-causes y
        let result = granger_causality_test(&ctx, 0, 1, 1, 0.05).unwrap();
        assert!(
            result.significant,
            "x should Granger-cause y: F={:.2}, p={:.6}",
            result.f_statistic, result.p_value
        );
        assert!(result.f_statistic > 10.0, "F-stat should be large");
        assert!(result.p_value < 0.01, "p-value should be very small");
    }

    #[test]
    fn granger_no_reverse_causality() {
        let ctx = causal_ctx();
        // y should NOT Granger-cause x (reverse direction)
        let result = granger_causality_test(&ctx, 1, 0, 1, 0.05).unwrap();
        assert!(
            !result.significant,
            "y should not Granger-cause x: F={:.2}, p={:.6}",
            result.f_statistic, result.p_value
        );
    }

    #[test]
    fn granger_independent_series_not_significant() {
        let ctx = independent_ctx();
        let result = granger_causality_test(&ctx, 0, 1, 1, 0.05).unwrap();
        assert!(
            !result.significant,
            "independent series should not show Granger causality: F={:.2}, p={:.6}",
            result.f_statistic, result.p_value
        );
    }

    #[test]
    fn granger_rejects_same_series() {
        let ctx = causal_ctx();
        let err = granger_causality_test(&ctx, 0, 0, 1, 0.05).unwrap_err();
        assert!(
            matches!(err, MultivariateError::InvalidParameter(_)),
            "same cause and effect should be rejected"
        );
    }

    #[test]
    fn granger_rejects_zero_lag() {
        let ctx = causal_ctx();
        let err = granger_causality_test(&ctx, 0, 1, 0, 0.05).unwrap_err();
        assert!(matches!(err, MultivariateError::InvalidParameter(_)));
    }

    #[test]
    fn granger_rejects_out_of_range_index() {
        let ctx = causal_ctx();
        let err = granger_causality_test(&ctx, 5, 1, 1, 0.05).unwrap_err();
        assert!(matches!(err, MultivariateError::SeriesNotFound(_)));
    }

    #[test]
    fn granger_higher_lag_order() {
        let ctx = causal_ctx();
        // With lag=2, x should still Granger-cause y
        let result = granger_causality_test(&ctx, 0, 1, 2, 0.05).unwrap();
        assert!(
            result.significant,
            "x should Granger-cause y at lag 2: F={:.2}, p={:.6}",
            result.f_statistic, result.p_value
        );
    }

    #[test]
    fn f_distribution_sf_basic() {
        // F(1, 100) at x=3.94 → p ≈ 0.05
        let p = f_distribution_sf(3.94, 1.0, 100.0);
        assert!((p - 0.05).abs() < 0.01, "p={p} should be near 0.05");

        // x=0 → p=1
        assert!((f_distribution_sf(0.0, 1.0, 100.0) - 1.0).abs() < 1e-10);

        // Very large x → p ≈ 0
        let p = f_distribution_sf(100.0, 1.0, 100.0);
        assert!(p < 0.001, "large F should give p near 0: p={p}");
    }

    #[test]
    fn granger_robust_detects_causality() {
        let ctx = causal_ctx();
        let result = granger_causality_test_robust(&ctx, 0, 1, 1, 0.05).unwrap();
        assert!(
            result.significant,
            "robust: x should Granger-cause y: F={:.2}, p={:.6}",
            result.f_statistic, result.p_value
        );
    }

    #[test]
    fn granger_robust_independent_not_significant() {
        let ctx = independent_ctx();
        let result = granger_causality_test_robust(&ctx, 0, 1, 1, 0.05).unwrap();
        assert!(
            !result.significant,
            "robust: independent series should not show causality: F={:.2}, p={:.6}",
            result.f_statistic, result.p_value
        );
    }
}
