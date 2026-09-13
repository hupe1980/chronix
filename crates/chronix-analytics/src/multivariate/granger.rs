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

    /// `scipy.stats.f.sf` across both branches of the symmetry relation and
    /// both continued-fraction steps.
    ///
    /// Its sibling in `forecast::diagnostics` — the χ² survival function
    /// `ljung_box` reports p-values from — carried the partial numerators of
    /// *this* continued fraction and was 11 % wrong in the tail, under a
    /// two-example test that happened to exercise only its other branch. A
    /// p-value is read as a decision, so the table is wide rather than
    /// illustrative.
    #[test]
    fn f_distribution_sf_matches_scipy() {
        // `approx_constant` fires on the F(2, 1) row at x = 0.5, whose
        // survival really is 1/√2 — `sf(x; 2, 1) = 1/sqrt(1 + 2x)` and
        // `1 + 2(0.5) = 2`. It is a value scipy produced, not a constant
        // anybody spelled out, and rewriting it as `FRAC_1_SQRT_2` would
        // hide where it came from.
        #[allow(clippy::approx_constant)]
        #[rustfmt::skip]
        const REFERENCE: &[(f64, f64, f64, f64)] = &[
            (0.01_f64, 1.0_f64, 1.0_f64, 0.936548965138893),
            (0.5_f64, 1.0_f64, 1.0_f64, 0.6081734479693928),
            (1.0_f64, 1.0_f64, 1.0_f64, 0.5000000000000001),
            (2.0_f64, 1.0_f64, 1.0_f64, 0.39182655203060723),
            (3.94_f64, 1.0_f64, 1.0_f64, 0.29709592078583547),
            (10.0_f64, 1.0_f64, 1.0_f64, 0.19498222904213666),
            (50.0_f64, 1.0_f64, 1.0_f64, 0.08943852195031553),
            (200.0_f64, 1.0_f64, 1.0_f64, 0.0449410137265141),
            (0.01_f64, 1.0_f64, 5.0_f64, 0.9242301411546605),
            (0.5_f64, 1.0_f64, 5.0_f64, 0.5110840804302808),
            (1.0_f64, 1.0_f64, 5.0_f64, 0.3632174676491227),
            (2.0_f64, 1.0_f64, 5.0_f64, 0.21643722926968553),
            (3.94_f64, 1.0_f64, 5.0_f64, 0.10391936782688335),
            (10.0_f64, 1.0_f64, 5.0_f64, 0.025031015818452945),
            (50.0_f64, 1.0_f64, 5.0_f64, 0.0008750749201480802),
            (200.0_f64, 1.0_f64, 5.0_f64, 3.182292826705153e-05),
            (0.01_f64, 1.0_f64, 10.0_f64, 0.9223207185644082),
            (0.5_f64, 1.0_f64, 10.0_f64, 0.4956475043831199),
            (1.0_f64, 1.0_f64, 10.0_f64, 0.34089313230206),
            (2.0_f64, 1.0_f64, 10.0_f64, 0.18766987086960307),
            (3.94_f64, 1.0_f64, 10.0_f64, 0.07525127067974843),
            (10.0_f64, 1.0_f64, 10.0_f64, 0.01011955973543372),
            (50.0_f64, 1.0_f64, 10.0_f64, 3.4114010142784005e-05),
            (200.0_f64, 1.0_f64, 10.0_f64, 6.149001368036422e-08),
            (0.01_f64, 1.0_f64, 30.0_f64, 0.9210096117902712),
            (0.5_f64, 1.0_f64, 30.0_f64, 0.4849569686830381),
            (1.0_f64, 1.0_f64, 30.0_f64, 0.32530861542603),
            (2.0_f64, 1.0_f64, 30.0_f64, 0.167594108019346),
            (3.94_f64, 1.0_f64, 30.0_f64, 0.056360730753834305),
            (10.0_f64, 1.0_f64, 30.0_f64, 0.0035685233088176825),
            (50.0_f64, 1.0_f64, 30.0_f64, 7.319404864841293e-08),
            (200.0_f64, 1.0_f64, 30.0_f64, 8.298453462587933e-15),
            (0.01_f64, 1.0_f64, 100.0_f64, 0.9205445310958512),
            (0.5_f64, 1.0_f64, 100.0_f64, 0.48114467698573943),
            (1.0_f64, 1.0_f64, 100.0_f64, 0.3197241557841234),
            (2.0_f64, 1.0_f64, 100.0_f64, 0.1604051314856055),
            (3.94_f64, 1.0_f64, 100.0_f64, 0.049890019981428654),
            (10.0_f64, 1.0_f64, 100.0_f64, 0.0020728725808666602),
            (50.0_f64, 1.0_f64, 100.0_f64, 2.1218306803575447e-10),
            (200.0_f64, 1.0_f64, 100.0_f64, 1.3512423796021643e-25),
            (0.01_f64, 1.0_f64, 500.0_f64, 0.9203844075368762),
            (0.5_f64, 1.0_f64, 500.0_f64, 0.47982953965954955),
            (1.0_f64, 1.0_f64, 500.0_f64, 0.31779420726060853),
            (2.0_f64, 1.0_f64, 500.0_f64, 0.15792157401181237),
            (3.94_f64, 1.0_f64, 500.0_f64, 0.04769679159239106),
            (10.0_f64, 1.0_f64, 500.0_f64, 0.001660499524859762),
            (50.0_f64, 1.0_f64, 500.0_f64, 5.20599444507499e-12),
            (200.0_f64, 1.0_f64, 500.0_f64, 1.9504120091795658e-38),
            (0.01_f64, 2.0_f64, 1.0_f64, 0.9901475429766743),
            (0.5_f64, 2.0_f64, 1.0_f64, 0.7071067811865476),
            (1.0_f64, 2.0_f64, 1.0_f64, 0.5773502691896257),
            (2.0_f64, 2.0_f64, 1.0_f64, 0.4472135954999579),
            (3.94_f64, 2.0_f64, 1.0_f64, 0.33557802760701216),
            (10.0_f64, 2.0_f64, 1.0_f64, 0.21821789023599236),
            (50.0_f64, 2.0_f64, 1.0_f64, 0.09950371902099892),
            (200.0_f64, 2.0_f64, 1.0_f64, 0.04993761694389223),
            (0.01_f64, 2.0_f64, 5.0_f64, 0.9900695822980478),
            (0.5_f64, 2.0_f64, 5.0_f64, 0.6339381452606089),
            (1.0_f64, 2.0_f64, 5.0_f64, 0.43120115037169215),
            (2.0_f64, 2.0_f64, 5.0_f64, 0.2300481458333117),
            (3.94_f64, 2.0_f64, 5.0_f64, 0.09389346217368301),
            (10.0_f64, 2.0_f64, 5.0_f64, 0.01788854381999832),
            (50.0_f64, 2.0_f64, 5.0_f64, 0.0004948251479274203),
            (200.0_f64, 2.0_f64, 5.0_f64, 1.6935087808430286e-05),
            (0.01_f64, 2.0_f64, 10.0_f64, 0.9900597211159814),
            (0.5_f64, 2.0_f64, 10.0_f64, 0.6209213230591552),
            (1.0_f64, 2.0_f64, 10.0_f64, 0.4018775720164609),
            (2.0_f64, 2.0_f64, 10.0_f64, 0.18593443208187066),
            (3.94_f64, 2.0_f64, 10.0_f64, 0.05472205870970857),
            (10.0_f64, 2.0_f64, 10.0_f64, 0.004115226337448558),
            (50.0_f64, 2.0_f64, 10.0_f64, 6.209213230591552e-06),
            (200.0_f64, 2.0_f64, 10.0_f64, 8.63138952743669e-09),
            (0.01_f64, 2.0_f64, 30.0_f64, 0.9900531324547684),
            (0.5_f64, 2.0_f64, 30.0_f64, 0.6114957082084547),
            (1.0_f64, 2.0_f64, 30.0_f64, 0.3798124058152456),
            (2.0_f64, 2.0_f64, 30.0_f64, 0.15298014392033615),
            (3.94_f64, 2.0_f64, 30.0_f64, 0.030246091791237155),
            (10.0_f64, 2.0_f64, 30.0_f64, 0.0004701849845759996),
            (50.0_f64, 2.0_f64, 30.0_f64, 2.803293281617644e-10),
            (200.0_f64, 2.0_f64, 30.0_f64, 4.516395719298497e-18),
            (0.01_f64, 2.0_f64, 100.0_f64, 0.9900508236675098),
            (0.5_f64, 2.0_f64, 100.0_f64, 0.6080388246889497),
            (1.0_f64, 2.0_f64, 100.0_f64, 0.3715278821269618),
            (2.0_f64, 2.0_f64, 100.0_f64, 0.14071261533323967),
            (3.94_f64, 2.0_f64, 100.0_f64, 0.02253995844075508),
            (10.0_f64, 2.0_f64, 100.0_f64, 0.00010988481911717233),
            (50.0_f64, 2.0_f64, 100.0_f64, 8.881784197001252e-16),
            (200.0_f64, 2.0_f64, 100.0_f64, 1.1258999068426271e-35),
            (0.01_f64, 2.0_f64, 500.0_f64, 0.9900500317538745),
            (0.5_f64, 2.0_f64, 500.0_f64, 0.6068335969214584),
            (1.0_f64, 2.0_f64, 500.0_f64, 0.3686139762360143),
            (2.0_f64, 2.0_f64, 500.0_f64, 0.13641652194295442),
            (3.94_f64, 2.0_f64, 500.0_f64, 0.020055031785403108),
            (10.0_f64, 2.0_f64, 500.0_f64, 5.5165197239041185e-05),
            (50.0_f64, 2.0_f64, 500.0_f64, 1.602095822884927e-20),
            (200.0_f64, 2.0_f64, 500.0_f64, 1.520105478330734e-64),
            (0.01_f64, 3.0_f64, 1.0_f64, 0.997871600941586),
            (0.5_f64, 3.0_f64, 1.0_f64, 0.7477845036444961),
            (1.0_f64, 3.0_f64, 1.0_f64, 0.6089977810442293),
            (2.0_f64, 3.0_f64, 1.0_f64, 0.46952222906704294),
            (3.94_f64, 3.0_f64, 1.0_f64, 0.35092487524303984),
            (10.0_f64, 3.0_f64, 1.0_f64, 0.2274450888799132),
            (50.0_f64, 3.0_f64, 1.0_f64, 0.10350028571595933),
            (200.0_f64, 3.0_f64, 1.0_f64, 0.051922117925989546),
            (0.01_f64, 3.0_f64, 5.0_f64, 0.998444486547434),
            (0.5_f64, 3.0_f64, 5.0_f64, 0.6984526373049241),
            (1.0_f64, 3.0_f64, 5.0_f64, 0.4648547899936351),
            (2.0_f64, 3.0_f64, 5.0_f64, 0.2326239180000786),
            (3.94_f64, 3.0_f64, 5.0_f64, 0.08704010156908576),
            (10.0_f64, 3.0_f64, 5.0_f64, 0.014888525723791667),
            (50.0_f64, 3.0_f64, 5.0_f64, 0.0003763234251500881),
            (200.0_f64, 3.0_f64, 5.0_f64, 1.261190950980022e-05),
            (0.01_f64, 3.0_f64, 10.0_f64, 0.9985345070506094),
            (0.5_f64, 3.0_f64, 10.0_f64, 0.6906222455335577),
            (1.0_f64, 3.0_f64, 10.0_f64, 0.4323372030216968),
            (2.0_f64, 3.0_f64, 10.0_f64, 0.17800740737517545),
            (3.94_f64, 3.0_f64, 10.0_f64, 0.042971584124033214),
            (10.0_f64, 3.0_f64, 10.0_f64, 0.002351579333314937),
            (50.0_f64, 3.0_f64, 10.0_f64, 2.51347041838405e-06),
            (200.0_f64, 3.0_f64, 10.0_f64, 3.1831481745761687e-09),
            (0.01_f64, 3.0_f64, 30.0_f64, 0.9985977393494714),
            (0.5_f64, 3.0_f64, 30.0_f64, 0.685119541295208),
            (1.0_f64, 3.0_f64, 30.0_f64, 0.40635726687294865),
            (2.0_f64, 3.0_f64, 30.0_f64, 0.13519999888940365),
            (3.94_f64, 3.0_f64, 30.0_f64, 0.0175365391571832),
            (10.0_f64, 3.0_f64, 30.0_f64, 9.957793415499995e-05),
            (50.0_f64, 3.0_f64, 30.0_f64, 8.748902589397343e-12),
            (200.0_f64, 3.0_f64, 30.0_f64, 6.425629332909406e-20),
            (0.01_f64, 3.0_f64, 100.0_f64, 0.998620523137218),
            (0.5_f64, 3.0_f64, 100.0_f64, 0.6831325288938814),
            (1.0_f64, 3.0_f64, 100.0_f64, 0.39618624961800647),
            (2.0_f64, 3.0_f64, 100.0_f64, 0.11884246789399958),
            (3.94_f64, 3.0_f64, 100.0_f64, 0.010558992868236465),
            (10.0_f64, 3.0_f64, 100.0_f64, 8.001257542330507e-06),
            (50.0_f64, 3.0_f64, 100.0_f64, 7.944424679087579e-20),
            (200.0_f64, 3.0_f64, 100.0_f64, 4.144844518612392e-42),
            (0.01_f64, 3.0_f64, 500.0_f64, 0.9986284152834037),
            (0.5_f64, 3.0_f64, 500.0_f64, 0.6824432968133554),
            (1.0_f64, 3.0_f64, 500.0_f64, 0.39254764685850485),
            (2.0_f64, 3.0_f64, 500.0_f64, 0.11306718282556118),
            (3.94_f64, 3.0_f64, 500.0_f64, 0.008507992054928032),
            (10.0_f64, 3.0_f64, 500.0_f64, 2.0691615425823712e-06),
            (50.0_f64, 3.0_f64, 500.0_f64, 2.8228133170512915e-28),
            (200.0_f64, 3.0_f64, 500.0_f64, 3.2772069980690325e-85),
            (0.01_f64, 5.0_f64, 1.0_f64, 0.999829052424257),
            (0.5_f64, 5.0_f64, 1.0_f64, 0.7835627707303147),
            (1.0_f64, 5.0_f64, 1.0_f64, 0.6367825323508773),
            (2.0_f64, 5.0_f64, 1.0_f64, 0.48891591956971925),
            (3.94_f64, 5.0_f64, 1.0_f64, 0.36418583627503304),
            (10.0_f64, 5.0_f64, 1.0_f64, 0.23539522321120734),
            (50.0_f64, 5.0_f64, 1.0_f64, 0.10694156159315064),
            (200.0_f64, 5.0_f64, 1.0_f64, 0.0536308727633301),
            (0.01_f64, 5.0_f64, 5.0_f64, 0.9999475708664215),
            (0.5_f64, 5.0_f64, 5.0_f64, 0.7674886808696214),
            (1.0_f64, 5.0_f64, 5.0_f64, 0.5000000000000002),
            (2.0_f64, 5.0_f64, 5.0_f64, 0.23251131913037854),
            (3.94_f64, 5.0_f64, 5.0_f64, 0.07931444580945335),
            (10.0_f64, 5.0_f64, 5.0_f64, 0.012241916531069727),
            (50.0_f64, 5.0_f64, 5.0_f64, 0.00028634393165790495),
            (200.0_f64, 5.0_f64, 5.0_f64, 9.433866982533931e-06),
            (0.01_f64, 5.0_f64, 10.0_f64, 0.9999596193011073),
            (0.5_f64, 5.0_f64, 10.0_f64, 0.7700248806501016),
            (1.0_f64, 5.0_f64, 10.0_f64, 0.46511942653780036),
            (2.0_f64, 5.0_f64, 10.0_f64, 0.1641949508997389),
            (3.94_f64, 5.0_f64, 10.0_f64, 0.031024328474510615),
            (10.0_f64, 5.0_f64, 10.0_f64, 0.0012057806486995373),
            (50.0_f64, 5.0_f64, 10.0_f64, 9.402259788055297e-07),
            (200.0_f64, 5.0_f64, 10.0_f64, 1.1023299105708095e-09),
            (0.01_f64, 5.0_f64, 30.0_f64, 0.9999671712069669),
            (0.5_f64, 5.0_f64, 30.0_f64, 0.7737335937035947),
            (1.0_f64, 5.0_f64, 30.0_f64, 0.43464887633987337),
            (2.0_f64, 5.0_f64, 30.0_f64, 0.1073353181049588),
            (3.94_f64, 5.0_f64, 30.0_f64, 0.007268455477015853),
            (10.0_f64, 5.0_f64, 30.0_f64, 1.0494761682015432e-05),
            (50.0_f64, 5.0_f64, 30.0_f64, 1.1830748130608502e-13),
            (200.0_f64, 5.0_f64, 30.0_f64, 4.352520121551453e-22),
            (0.01_f64, 5.0_f64, 100.0_f64, 0.9999697160830983),
            (0.5_f64, 5.0_f64, 100.0_f64, 0.7755895192354819),
            (1.0_f64, 5.0_f64, 100.0_f64, 0.4218298943719735),
            (2.0_f64, 5.0_f64, 100.0_f64, 0.08507985027459153),
            (3.94_f64, 5.0_f64, 100.0_f64, 0.002653408596416724),
            (10.0_f64, 5.0_f64, 100.0_f64, 8.829291988294581e-08),
            (50.0_f64, 5.0_f64, 100.0_f64, 1.0553494582665948e-25),
            (200.0_f64, 5.0_f64, 100.0_f64, 2.0438314933293716e-50),
            (0.01_f64, 5.0_f64, 500.0_f64, 0.9999705763769625),
            (0.5_f64, 5.0_f64, 500.0_f64, 0.7763082744903824),
            (1.0_f64, 5.0_f64, 500.0_f64, 0.41709434013508173),
            (2.0_f64, 5.0_f64, 500.0_f64, 0.0772160206804),
            (3.94_f64, 5.0_f64, 500.0_f64, 0.0016330150886885548),
            (10.0_f64, 5.0_f64, 500.0_f64, 3.907927859640413e-09),
            (50.0_f64, 5.0_f64, 500.0_f64, 5.536126339695859e-42),
            (200.0_f64, 5.0_f64, 500.0_f64, 8.577660241289723e-117),
            (0.01_f64, 10.0_f64, 1.0_f64, 0.9999984104468244),
            (0.5_f64, 10.0_f64, 1.0_f64, 0.8123301291303968),
            (1.0_f64, 10.0_f64, 1.0_f64, 0.6591068676979401),
            (2.0_f64, 10.0_f64, 1.0_f64, 0.5043524956168801),
            (3.94_f64, 10.0_f64, 1.0_f64, 0.374680506094013),
            (10.0_f64, 10.0_f64, 1.0_f64, 0.24166846428882624),
            (50.0_f64, 10.0_f64, 1.0_f64, 0.1096544985800662),
            (200.0_f64, 10.0_f64, 1.0_f64, 0.05497784197229168),
            (0.01_f64, 10.0_f64, 5.0_f64, 0.999999966830924),
            (0.5_f64, 10.0_f64, 5.0_f64, 0.8358050491002611),
            (1.0_f64, 10.0_f64, 5.0_f64, 0.5348805734621996),
            (2.0_f64, 10.0_f64, 5.0_f64, 0.22997511934989848),
            (3.94_f64, 10.0_f64, 5.0_f64, 0.07168520205290538),
            (10.0_f64, 10.0_f64, 5.0_f64, 0.01011508946974278),
            (50.0_f64, 10.0_f64, 5.0_f64, 0.00022244594009466362),
            (200.0_f64, 10.0_f64, 5.0_f64, 7.234158868087465e-06),
            (0.01_f64, 10.0_f64, 10.0_f64, 0.9999999884021837),
            (0.5_f64, 10.0_f64, 10.0_f64, 0.8551541939744958),
            (1.0_f64, 10.0_f64, 10.0_f64, 0.5),
            (2.0_f64, 10.0_f64, 10.0_f64, 0.14484580602550423),
            (3.94_f64, 10.0_f64, 10.0_f64, 0.02060276864630747),
            (10.0_f64, 10.0_f64, 10.0_f64, 0.0005715525434020327),
            (50.0_f64, 10.0_f64, 10.0_f64, 3.419168704086705e-07),
            (200.0_f64, 10.0_f64, 10.0_f64, 3.777237977079334e-10),
            (0.01_f64, 10.0_f64, 30.0_f64, 0.9999999954728509),
            (0.5_f64, 10.0_f64, 30.0_f64, 0.8763612630739955),
            (1.0_f64, 10.0_f64, 30.0_f64, 0.46542429049441125),
            (2.0_f64, 10.0_f64, 30.0_f64, 0.06961370807638423),
            (3.94_f64, 10.0_f64, 30.0_f64, 0.0016868261961467488),
            (10.0_f64, 10.0_f64, 30.0_f64, 4.105278385297918e-07),
            (50.0_f64, 10.0_f64, 30.0_f64, 6.114705318904914e-16),
            (200.0_f64, 10.0_f64, 30.0_f64, 1.2838896913298983e-24),
            (0.01_f64, 10.0_f64, 100.0_f64, 0.9999999969790168),
            (0.5_f64, 10.0_f64, 100.0_f64, 0.886350391153074),
            (1.0_f64, 10.0_f64, 100.0_f64, 0.4488172795604992),
            (2.0_f64, 10.0_f64, 100.0_f64, 0.04098813977040327),
            (3.94_f64, 10.0_f64, 100.0_f64, 0.00015210131342646187),
            (10.0_f64, 10.0_f64, 100.0_f64, 1.9014845253906287e-11),
            (50.0_f64, 10.0_f64, 100.0_f64, 1.9168227637538351e-34),
            (200.0_f64, 10.0_f64, 100.0_f64, 2.0230874789256753e-61),
            (0.01_f64, 10.0_f64, 500.0_f64, 0.9999999974028768),
            (0.5_f64, 10.0_f64, 500.0_f64, 0.8901834974380083),
            (1.0_f64, 10.0_f64, 500.0_f64, 0.4422289394076462),
            (2.0_f64, 10.0_f64, 500.0_f64, 0.031541042308141296),
            (3.94_f64, 10.0_f64, 500.0_f64, 3.46693105201534e-05),
            (10.0_f64, 10.0_f64, 500.0_f64, 2.2707875150585556e-15),
            (50.0_f64, 10.0_f64, 500.0_f64, 5.944927399912203e-69),
            (200.0_f64, 10.0_f64, 500.0_f64, 1.2601089055667632e-167),
            (0.01_f64, 20.0_f64, 1.0_f64, 0.9999999968362182),
            (0.5_f64, 20.0_f64, 1.0_f64, 0.8273216960128282),
            (1.0_f64, 20.0_f64, 1.0_f64, 0.6707434228282909),
            (2.0_f64, 20.0_f64, 1.0_f64, 0.5123419049486245),
            (3.94_f64, 20.0_f64, 1.0_f64, 0.38008868552437247),
            (10.0_f64, 20.0_f64, 1.0_f64, 0.2448937224769333),
            (50.0_f64, 20.0_f64, 1.0_f64, 0.11104811307081802),
            (200.0_f64, 20.0_f64, 1.0_f64, 0.05566968855192704),
            (0.01_f64, 20.0_f64, 5.0_f64, 0.9999999999998095),
            (0.5_f64, 20.0_f64, 5.0_f64, 0.8774927553181573),
            (1.0_f64, 20.0_f64, 5.0_f64, 0.5569748153151197),
            (2.0_f64, 20.0_f64, 5.0_f64, 0.22739561420949508),
            (3.94_f64, 20.0_f64, 5.0_f64, 0.06710840040162436),
            (10.0_f64, 20.0_f64, 5.0_f64, 0.009008873742780133),
            (50.0_f64, 20.0_f64, 5.0_f64, 0.00019186568791248524),
            (200.0_f64, 20.0_f64, 5.0_f64, 6.198937506367042e-06),
            (0.01_f64, 20.0_f64, 10.0_f64, 0.9999999999999922),
            (0.5_f64, 20.0_f64, 10.0_f64, 0.91021728515625),
            (1.0_f64, 20.0_f64, 10.0_f64, 0.5244995315671084),
            (2.0_f64, 20.0_f64, 10.0_f64, 0.1298396258304001),
            (3.94_f64, 20.0_f64, 10.0_f64, 0.015109243840033613),
            (10.0_f64, 20.0_f64, 10.0_f64, 0.00034109735891310985),
            (50.0_f64, 20.0_f64, 10.0_f64, 1.7680920543628828e-07),
            (200.0_f64, 20.0_f64, 10.0_f64, 1.8950164426620433e-10),
            (0.01_f64, 20.0_f64, 30.0_f64, 0.9999999999999997),
            (0.5_f64, 20.0_f64, 30.0_f64, 0.94533507760557),
            (1.0_f64, 20.0_f64, 30.0_f64, 0.4890801931489531),
            (2.0_f64, 20.0_f64, 30.0_f64, 0.04176011226521399),
            (3.94_f64, 20.0_f64, 30.0_f64, 0.00036552127382908983),
            (10.0_f64, 20.0_f64, 30.0_f64, 2.181560318562557e-08),
            (50.0_f64, 20.0_f64, 30.0_f64, 9.387262400342459e-18),
            (200.0_f64, 20.0_f64, 30.0_f64, 1.46661271407789e-26),
            (0.01_f64, 20.0_f64, 100.0_f64, 0.9999999999999999),
            (0.5_f64, 20.0_f64, 100.0_f64, 0.9610073391512229),
            (1.0_f64, 20.0_f64, 100.0_f64, 0.4692089598227416),
            (2.0_f64, 20.0_f64, 100.0_f64, 0.013280421314494431),
            (3.94_f64, 20.0_f64, 100.0_f64, 2.4218334288111496e-06),
            (10.0_f64, 20.0_f64, 100.0_f64, 4.98784313382886e-16),
            (50.0_f64, 20.0_f64, 100.0_f64, 4.620956135500676e-43),
            (200.0_f64, 20.0_f64, 100.0_f64, 2.3195151914337875e-71),
            (0.01_f64, 20.0_f64, 500.0_f64, 1.0),
            (0.5_f64, 20.0_f64, 500.0_f64, 0.9667241763563608),
            (1.0_f64, 20.0_f64, 500.0_f64, 0.4603768778461135),
            (2.0_f64, 20.0_f64, 500.0_f64, 0.006357074065895011),
            (3.94_f64, 20.0_f64, 500.0_f64, 3.337294937580564e-08),
            (10.0_f64, 20.0_f64, 500.0_f64, 5.135630261578328e-26),
            (50.0_f64, 20.0_f64, 500.0_f64, 1.744228880458988e-105),
            (200.0_f64, 20.0_f64, 500.0_f64, 1.201843864451615e-223),
        ];

        for &(x, d1, d2, expected) in REFERENCE {
            let got = f_distribution_sf(x, d1, d2);
            let err = if expected > 1e-12 {
                (got - expected).abs() / expected
            } else {
                (got - expected).abs()
            };
            assert!(
                err < 1e-9,
                "sf({x}, {d1}, {d2}) = {got}, expected {expected} (relative error {err:.3e})"
            );
        }
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
