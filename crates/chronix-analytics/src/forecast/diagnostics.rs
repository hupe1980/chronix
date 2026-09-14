//! Model diagnostics for forecast quality assessment.
//!
//! Provides statistical tests and metrics to evaluate fitted forecast models:
//!
//! - **[`ljung_box`]** — Ljung-Box Q test for residual autocorrelation
//! - **[`residual_acf`]** — Sample autocorrelation function of residuals
//! - **[`bic`]** — Bayesian Information Criterion
//! - **[`aicc`]** — Corrected Akaike Information Criterion
//! - **Error metrics**: [`rmse`], [`mae`], [`mape`], [`smape`]
//! - **[`ModelDiagnostics`]** — All-in-one diagnostics report

use std::f64::consts::PI;

// ── Error Metrics ──────────────────────────────────────────────────

/// Root Mean Squared Error.
///
/// $$\text{RMSE} = \sqrt{\frac{1}{n}\sum_{i=1}^{n}(y_i - \hat{y}_i)^2}$$
///
/// # Errors
///
/// Returns `None` if `actual` and `predicted` have different lengths or are empty.
#[must_use]
pub fn rmse(actual: &[f64], predicted: &[f64]) -> Option<f64> {
    if actual.len() != predicted.len() || actual.is_empty() {
        return None;
    }
    let n = actual.len() as f64;
    let mse: f64 = actual
        .iter()
        .zip(predicted)
        .map(|(a, p)| (a - p).powi(2))
        .sum::<f64>()
        / n;
    Some(mse.sqrt())
}

/// Mean Absolute Error.
///
/// $$\text{MAE} = \frac{1}{n}\sum_{i=1}^{n}|y_i - \hat{y}_i|$$
#[must_use]
pub fn mae(actual: &[f64], predicted: &[f64]) -> Option<f64> {
    if actual.len() != predicted.len() || actual.is_empty() {
        return None;
    }
    let n = actual.len() as f64;
    let sum: f64 = actual
        .iter()
        .zip(predicted)
        .map(|(a, p)| (a - p).abs())
        .sum();
    Some(sum / n)
}

/// Mean Absolute Percentage Error.
///
/// $$\text{MAPE} = \frac{100}{n}\sum_{i=1}^{n}\left|\frac{y_i - \hat{y}_i}{y_i}\right|$$
///
/// Skips zero actual values to avoid division by zero.
#[must_use]
pub fn mape(actual: &[f64], predicted: &[f64]) -> Option<f64> {
    if actual.len() != predicted.len() || actual.is_empty() {
        return None;
    }
    let mut sum = 0.0;
    let mut count = 0usize;
    for (a, p) in actual.iter().zip(predicted) {
        if a.abs() > f64::EPSILON {
            sum += ((a - p) / a).abs();
            count += 1;
        }
    }
    if count == 0 {
        return None;
    }
    Some(100.0 * sum / count as f64)
}

/// Symmetric Mean Absolute Percentage Error.
///
/// $$\text{sMAPE} = \frac{200}{n}\sum_{i=1}^{n}\frac{|y_i - \hat{y}_i|}{|y_i| + |\hat{y}_i|}$$
///
/// More robust than MAPE when values are near zero.
#[must_use]
pub fn smape(actual: &[f64], predicted: &[f64]) -> Option<f64> {
    if actual.len() != predicted.len() || actual.is_empty() {
        return None;
    }
    let mut sum = 0.0;
    let mut count = 0usize;
    for (a, p) in actual.iter().zip(predicted) {
        let denom = a.abs() + p.abs();
        if denom > f64::EPSILON {
            sum += (a - p).abs() / denom;
            count += 1;
        }
    }
    if count == 0 {
        return None;
    }
    Some(200.0 * sum / count as f64)
}

// ── Residual ACF ───────────────────────────────────────────────────

/// Compute the sample autocorrelation function (ACF) of `residuals`
/// for lags `0..=max_lag`.
///
/// Returns a vector of length `max_lag + 1` where element `k` is the
/// autocorrelation at lag `k`.  Element 0 is always 1.0.
///
/// # Errors
///
/// Returns `None` if `residuals` has fewer than 2 elements or
/// `max_lag >= residuals.len()`.
#[must_use]
pub fn residual_acf(residuals: &[f64], max_lag: usize) -> Option<Vec<f64>> {
    let n = residuals.len();
    if n < 2 || max_lag >= n {
        return None;
    }

    let mean = residuals.iter().sum::<f64>() / n as f64;
    let var: f64 = residuals.iter().map(|r| (r - mean).powi(2)).sum();
    if var.abs() < f64::EPSILON {
        // All residuals identical — no autocorrelation.
        let mut acf = vec![0.0; max_lag + 1];
        acf[0] = 1.0;
        return Some(acf);
    }

    let mut acf = Vec::with_capacity(max_lag + 1);
    for lag in 0..=max_lag {
        let cov: f64 = residuals
            .iter()
            .skip(lag)
            .zip(residuals.iter())
            .map(|(r_t, r_t_k)| (r_t - mean) * (r_t_k - mean))
            .sum();
        acf.push(cov / var);
    }
    Some(acf)
}

// ── Ljung-Box Test ─────────────────────────────────────────────────

/// Result of the Ljung-Box Q test.
#[derive(Debug, Clone)]
pub struct LjungBoxResult {
    /// Test statistic Q.
    pub q_statistic: f64,
    /// Degrees of freedom (lags minus fitted AR/MA parameters).
    pub df: usize,
    /// Approximate p-value from chi-squared distribution.
    pub p_value: f64,
    /// Whether the null hypothesis (no autocorrelation) is rejected
    /// at the specified significance level.
    pub significant: bool,
}

/// Ljung-Box Q test for residual autocorrelation.
///
/// Tests the null hypothesis that the first `max_lag` autocorrelations
/// are jointly zero.
///
/// $$Q = n(n+2)\sum_{k=1}^{h}\frac{\hat{\rho}_k^2}{n-k}$$
///
/// # Arguments
///
/// - `residuals` — model residuals
/// - `max_lag`   — number of lags to include
/// - `n_params`  — number of fitted model parameters (subtracted from df)
/// - `alpha`     — significance level (e.g. 0.05)
///
/// # Errors
///
/// Returns `None` if the residuals are too short for the requested lags.
#[must_use]
pub fn ljung_box(
    residuals: &[f64],
    max_lag: usize,
    n_params: usize,
    alpha: f64,
) -> Option<LjungBoxResult> {
    let n = residuals.len();
    if max_lag == 0 || max_lag >= n || n_params >= max_lag {
        return None;
    }

    let acf = residual_acf(residuals, max_lag)?;

    let n_f = n as f64;
    let q: f64 = (1..=max_lag)
        .map(|k| acf[k].powi(2) / (n_f - k as f64))
        .sum::<f64>()
        * n_f
        * (n_f + 2.0);

    let df = max_lag - n_params;
    let p_value = chi_squared_survival(q, df);

    Some(LjungBoxResult {
        q_statistic: q,
        df,
        p_value,
        significant: p_value < alpha,
    })
}

// ── Information Criteria ───────────────────────────────────────────

/// Akaike Information Criterion.
///
/// $$\text{AIC} = n \ln(\text{RSS}/n) + 2k$$
///
/// where `rss` is the residual sum of squares, `n` is the sample size, and
/// `k` is the number of estimated parameters.
///
/// **Comparable only between models of the same data.** Two ARIMA orders that
/// difference the series a different number of times are fitted to different
/// series, of different lengths and on different scales, and their AIC values
/// do not rank anything — which is why
/// [`select_differencing_order`](crate::forecast::select_differencing_order)
/// exists.
#[must_use]
pub fn aic(rss: f64, n: usize, k: usize) -> Option<f64> {
    let v = mean_residual_variance(rss, n)?;
    Some(n as f64 * v.ln() + 2.0 * (k as f64))
}

/// Mean residual variance, floored so that a perfect fit still ranks.
///
/// A residual sum of squares of exactly zero makes the conditional
/// log-likelihood unbounded and every information criterion `-∞`. Returning
/// `None` there is worse than it sounds: a deterministic series — a synthetic
/// ramp, a constant, a counter that has not moved — then produces *no*
/// scorable model at all, and [`auto_arima`](crate::forecast::auto_arima)
/// fails with "nothing could be fitted" on the easiest input there is. The
/// variance is floored at the smallest positive double instead, so every
/// perfect fit scores the same enormous negative number and the parameter
/// penalty picks the simplest of them, which is the intended answer.
fn mean_residual_variance(rss: f64, n: usize) -> Option<f64> {
    if n == 0 || !rss.is_finite() || rss < 0.0 {
        return None;
    }
    Some((rss / n as f64).max(f64::MIN_POSITIVE))
}

/// Bayesian Information Criterion.
///
/// $$\text{BIC} = n \ln(\text{RSS}/n) + k \ln(n)$$
///
/// where `rss` is the residual sum of squares, `n` is the sample size,
/// and `k` is the number of estimated parameters.
#[must_use]
pub fn bic(rss: f64, n: usize, k: usize) -> Option<f64> {
    let v = mean_residual_variance(rss, n)?;
    let n_f = n as f64;
    Some(n_f * v.ln() + (k as f64) * n_f.ln())
}

/// Corrected Akaike Information Criterion (AICc).
///
/// $$\text{AICc} = n \ln(\text{RSS}/n) + 2k + \frac{2k(k+1)}{n - k - 1}$$
///
/// The correction term prevents overfitting for small samples.
#[must_use]
pub fn aicc(rss: f64, n: usize, k: usize) -> Option<f64> {
    if n <= k + 1 {
        return None;
    }
    let v = mean_residual_variance(rss, n)?;
    let n_f = n as f64;
    let k_f = k as f64;
    let base = n_f * v.ln() + 2.0 * k_f;
    let correction = 2.0 * k_f * (k_f + 1.0) / (n_f - k_f - 1.0);
    Some(base + correction)
}

// ── Composite Diagnostics ──────────────────────────────────────────

/// All-in-one diagnostics report for a fitted forecast model.
#[derive(Debug, Clone)]
pub struct ModelDiagnostics {
    /// RMSE of in-sample residuals.
    pub rmse: f64,
    /// MAE of in-sample residuals.
    pub mae: f64,
    /// MAPE (if computable — `None` when actuals contain zeros).
    pub mape: Option<f64>,
    /// sMAPE.
    pub smape: Option<f64>,
    /// Sample autocorrelation function of residuals.
    pub acf: Vec<f64>,
    /// Ljung-Box test result.
    pub ljung_box: Option<LjungBoxResult>,
    /// BIC.
    pub bic: Option<f64>,
    /// AICc.
    pub aicc: Option<f64>,
}

/// Compute comprehensive diagnostics from actual values, predictions,
/// and model metadata.
///
/// # Arguments
///
/// - `actual`    — observed values
/// - `predicted` — in-sample fitted values (same length as `actual`)
/// - `n_params`  — number of estimated model parameters
/// - `max_lag`   — max ACF / Ljung-Box lag (default: `min(20, n/5)`)
///
/// # Errors
///
/// Returns `None` if inputs are mismatched or too short.
#[must_use]
pub fn compute_diagnostics(
    actual: &[f64],
    predicted: &[f64],
    n_params: usize,
    max_lag: Option<usize>,
) -> Option<ModelDiagnostics> {
    if actual.len() != predicted.len() || actual.is_empty() {
        return None;
    }

    let n = actual.len();
    let residuals: Vec<f64> = actual.iter().zip(predicted).map(|(a, p)| a - p).collect();
    let rss: f64 = residuals.iter().map(|r| r * r).sum();

    let lag = max_lag.unwrap_or_else(|| 20.min(n / 5).max(1));

    Some(ModelDiagnostics {
        rmse: rmse(actual, predicted).unwrap_or(0.0),
        mae: mae(actual, predicted).unwrap_or(0.0),
        mape: mape(actual, predicted),
        smape: smape(actual, predicted),
        acf: residual_acf(&residuals, lag).unwrap_or_default(),
        ljung_box: ljung_box(&residuals, lag, n_params, 0.05),
        bic: bic(rss, n, n_params),
        aicc: aicc(rss, n, n_params),
    })
}

// ── Chi-squared survival (upper-tail) via regularized gamma ────────

/// Approximate chi-squared survival function P(X > x) for X ~ χ²(df).
///
/// Uses the regularized upper incomplete gamma function:
/// P(X > x) = Γ(df/2, x/2) / Γ(df/2)
fn chi_squared_survival(x: f64, df: usize) -> f64 {
    if df == 0 || x < 0.0 {
        return 1.0;
    }
    let a = df as f64 / 2.0;
    let z = x / 2.0;
    regularized_upper_gamma(a, z)
}

/// Regularized upper incomplete gamma function Q(a, z) = 1 - P(a, z).
///
/// Uses Legendre's continued fraction for Q(a, z) when z > a + 1,
/// and the series expansion for P(a, z) otherwise.
fn regularized_upper_gamma(a: f64, z: f64) -> f64 {
    if z < 0.0 {
        return 1.0;
    }
    if z < a + 1.0 {
        1.0 - gamma_series(a, z)
    } else {
        gamma_cf(a, z)
    }
}

/// P(a, z) via series expansion: P = e^{-z} z^a / Γ(a) * Σ z^n / (a)_n.
fn gamma_series(a: f64, z: f64) -> f64 {
    let ln_gamma_a = ln_gamma(a);
    let mut term = 1.0 / a;
    let mut sum = term;
    for n in 1..200 {
        term *= z / (a + n as f64);
        sum += term;
        if term.abs() < sum.abs() * 1e-14 {
            break;
        }
    }
    let log_val = a * z.ln() - z - ln_gamma_a + sum.ln();
    log_val.exp().min(1.0)
}

/// Q(a, z) via the modified Lentz evaluation of Legendre's continued
/// fraction, as in Numerical Recipes §6.2 `gcf`.
///
/// ```text
/// Q(a,z) = e^-z z^a / Γ(a) × 1/(z+1-a - 1(1-a)/(z+3-a - 2(2-a)/(z+5-a - …)))
/// ```
///
/// so the `n`-th partial numerator is `-n(n-a)` against the denominator
/// `z + (2n+1) - a`. What stood here paired those denominators with the
/// numerators of the continued fraction for the incomplete *beta* function —
/// `k(a-k)` and `-(a-1+k)k` on alternating terms — and the two do not belong
/// to the same fraction. `ljung_box` computed its Q statistic exactly and
/// then reported p = 0.0722 where the true value is 0.0648, an 11 % error in
/// the only number anybody reads: the series branch covers `z < a + 1`, so
/// every p-value in the tail came from here.
///
/// The guard is also a magnitude one. `x.max(tiny)` turns a legitimate
/// negative denominator into `+1e-30`, and Legendre's fraction has them.
fn gamma_cf(a: f64, z: f64) -> f64 {
    const TINY: f64 = 1e-300;
    let ln_gamma_a = ln_gamma(a);

    let mut b = z + 1.0 - a;
    let mut c = 1.0 / TINY;
    let mut d = 1.0 / if b.abs() < TINY { TINY } else { b };
    let mut h = d;

    for i in 1..300 {
        let an = -(i as f64) * (i as f64 - a);
        b += 2.0;
        d = an.mul_add(d, b);
        if d.abs() < TINY {
            d = TINY;
        }
        c = b + an / c;
        if c.abs() < TINY {
            c = TINY;
        }
        d = 1.0 / d;
        let delta = d * c;
        h *= delta;
        if (delta - 1.0).abs() < 1e-15 {
            break;
        }
    }

    let log_val = a.mul_add(z.ln(), -z) - ln_gamma_a + h.ln();
    log_val.exp().clamp(0.0, 1.0)
}

/// Lanczos approximation for ln(Γ(x)).
///
/// `pub(crate)` because it is the crate's **only** implementation. There were
/// two: this one, pinned by `ln_gamma_known_values`, and a copy in
/// `multivariate::mv_forecast` carrying the g = 7 coefficients with `t = x +
/// 6.5` — a g = 6 offset. Mismatched Lanczos parameters are not a precision
/// problem: that copy returned 0.928 for `ln Γ(1)`, which is 0, and it sat
/// under `ln_beta` → `regularized_incomplete_beta` → `f_distribution_sf`, so
/// **every Granger causality p-value in the crate was wrong**.
pub(crate) fn ln_gamma(x: f64) -> f64 {
    // Coefficients for g = 7, n = 9 (Lanczos).
    const COEFFS: [f64; 9] = [
        0.999_999_999_999_809_9,
        676.520_368_121_885_1,
        -1_259.139_216_722_402_9,
        771.323_428_777_653_1,
        -176.615_029_162_140_6,
        12.507_343_278_686_905,
        -0.138_571_095_265_720_12,
        9.984_369_578_019_572e-6,
        1.505_632_735_149_311_6e-7,
    ];

    if x < 0.5 {
        // Reflection formula
        let log_pi = PI.ln();
        return log_pi - (PI * x).sin().abs().ln() - ln_gamma(1.0 - x);
    }

    let xx = x - 1.0;
    let mut sum = COEFFS[0];
    for (i, &c) in COEFFS.iter().enumerate().skip(1) {
        sum += c / (xx + i as f64);
    }

    let t = xx + 7.5; // g + 0.5
    0.5 * (2.0 * PI).ln() + (xx + 0.5) * t.ln() - t + sum.ln()
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rmse_basic() {
        let actual = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let predicted = vec![1.1, 2.2, 2.8, 4.1, 4.9];
        let r = rmse(&actual, &predicted).unwrap();
        assert!((r - 0.1414).abs() < 0.01, "rmse = {r}");
    }

    #[test]
    fn mae_basic() {
        let actual = vec![1.0, 2.0, 3.0];
        let predicted = vec![1.5, 2.5, 2.5];
        let m = mae(&actual, &predicted).unwrap();
        assert!((m - 0.5).abs() < 1e-10, "mae = {m}");
    }

    #[test]
    fn mape_skips_zeros() {
        let actual = vec![0.0, 10.0, 20.0];
        let predicted = vec![1.0, 11.0, 18.0];
        let m = mape(&actual, &predicted).unwrap();
        // Only uses indices 1 and 2: (1/10 + 2/20)/2 * 100 = 10%
        assert!((m - 10.0).abs() < 1e-10, "mape = {m}");
    }

    #[test]
    fn smape_basic() {
        let actual = vec![10.0, 20.0];
        let predicted = vec![12.0, 18.0];
        // |10-12|/(10+12) + |20-18|/(20+18) = 2/22 + 2/38
        // smape = 200/2 * (2/22 + 2/38) = 100 * (0.0909 + 0.0526) = 14.35
        let s = smape(&actual, &predicted).unwrap();
        assert!(s > 14.0 && s < 15.0, "smape = {s}");
    }

    #[test]
    fn acf_lag0_is_one() {
        let residuals = vec![1.0, -0.5, 0.3, -0.1, 0.7, -0.4];
        let acf = residual_acf(&residuals, 3).unwrap();
        assert!((acf[0] - 1.0).abs() < 1e-10);
        assert_eq!(acf.len(), 4);
    }

    #[test]
    fn acf_white_noise_near_zero() {
        // Use a longer, more varied pseudo-random sequence.
        // Linear congruential generator (a=1103515245, c=12345, m=2^31).
        let mut state = 42u64;
        let residuals: Vec<f64> = (0..500)
            .map(|_| {
                state = state.wrapping_mul(1_103_515_245).wrapping_add(12345) & 0x7FFF_FFFF;
                (state as f64 / 0x7FFF_FFFF as f64) * 2.0 - 1.0
            })
            .collect();
        let acf = residual_acf(&residuals, 10).unwrap();
        for (lag, &r) in acf.iter().enumerate().skip(1) {
            assert!(
                r.abs() < 0.15,
                "acf at lag {lag} = {r} too large for LCG noise"
            );
        }
    }

    #[test]
    fn ljung_box_white_noise_not_significant() {
        // LCG pseudo-random data — should NOT reject null.
        let mut state = 7u64;
        let residuals: Vec<f64> = (0..500)
            .map(|_| {
                state = state.wrapping_mul(1_103_515_245).wrapping_add(12345) & 0x7FFF_FFFF;
                (state as f64 / 0x7FFF_FFFF as f64) * 2.0 - 1.0
            })
            .collect();
        let result = ljung_box(&residuals, 10, 2, 0.05).unwrap();
        assert!(
            !result.significant,
            "LCG noise should not be significant: p = {}",
            result.p_value
        );
    }

    #[test]
    fn ljung_box_autocorrelated_is_significant() {
        // Highly autocorrelated data.
        let mut residuals = vec![0.0; 200];
        residuals[0] = 1.0;
        for i in 1..200 {
            residuals[i] = 0.95 * residuals[i - 1] + 0.1 * ((i * 7 % 13) as f64 - 6.0);
        }
        let result = ljung_box(&residuals, 10, 0, 0.05).unwrap();
        assert!(
            result.significant,
            "AR(1) residuals should be significant: p = {}, Q = {}",
            result.p_value, result.q_statistic
        );
    }

    #[test]
    fn bic_basic() {
        let val = bic(100.0, 50, 3).unwrap();
        // BIC = 50*ln(2) + 3*ln(50) = 34.66 + 11.74 ≈ 46.4
        assert!(val > 45.0 && val < 48.0, "bic = {val}");
    }

    #[test]
    fn aicc_basic() {
        let val = aicc(100.0, 50, 3).unwrap();
        // AICc = 50*ln(2) + 6 + 24/46 ≈ 34.66 + 6 + 0.52 = 41.18
        assert!(val > 40.0 && val < 42.0, "aicc = {val}");
    }

    #[test]
    fn aicc_small_sample_returns_none() {
        // n <= k + 1 should return None.
        assert!(aicc(10.0, 3, 3).is_none());
    }

    #[test]
    fn compute_diagnostics_full() {
        let actual: Vec<f64> = (0..100).map(|i| i as f64 * 0.5 + 10.0).collect();
        let predicted: Vec<f64> = actual
            .iter()
            .enumerate()
            .map(|(i, &v)| v + 0.1 * ((i * 7 % 11) as f64 - 5.0))
            .collect();

        let diag = compute_diagnostics(&actual, &predicted, 2, None).unwrap();
        assert!(diag.rmse > 0.0);
        assert!(diag.mae > 0.0);
        assert!(diag.mape.is_some());
        assert!(diag.smape.is_some());
        assert!(!diag.acf.is_empty());
        assert!(diag.bic.is_some());
        assert!(diag.aicc.is_some());
    }

    #[test]
    fn empty_inputs_return_none() {
        assert!(rmse(&[], &[]).is_none());
        assert!(mae(&[], &[]).is_none());
        assert!(mape(&[], &[]).is_none());
        assert!(residual_acf(&[], 1).is_none());
        assert!(ljung_box(&[], 1, 0, 0.05).is_none());
        assert!(bic(0.0, 0, 0).is_none());
    }

    #[test]
    fn mismatched_lengths_return_none() {
        assert!(rmse(&[1.0], &[1.0, 2.0]).is_none());
        assert!(mae(&[1.0], &[1.0, 2.0]).is_none());
    }

    #[test]
    fn ln_gamma_known_values() {
        // Γ(1) = 1 → ln(Γ(1)) = 0
        assert!((ln_gamma(1.0)).abs() < 1e-10);
        // Γ(5) = 24 → ln(Γ(5)) ≈ 3.178
        assert!((ln_gamma(5.0) - 24.0_f64.ln()).abs() < 1e-6);
        // Γ(0.5) = √π → ln(Γ(0.5)) ≈ 0.5723
        assert!((ln_gamma(0.5) - (PI.sqrt().ln())).abs() < 1e-6);
    }

    #[test]
    fn chi_squared_survival_known() {
        // `scipy.stats.chi2.sf`, across both branches of
        // `regularized_upper_gamma` and well into the tail — which is the
        // half that was wrong. The series covers `z < a + 1`, i.e. small `x`
        // for large `df`, and everything else went through the continued
        // fraction; that one carried the partial numerators of the
        // incomplete *beta* function, so `ljung_box` reported p = 0.0722
        // where the answer is 0.0648.
        //
        // Two examples alone would not have caught it: the previous version
        // of this test used χ²(2) at x = 0 and x = 4, both of which take the
        // series branch, and it passed throughout.
        #[rustfmt::skip]
        const REFERENCE: &[(f64, usize, f64)] = &[
            (0.1_f64, 1, 0.7518296340458492),
            (0.5_f64, 1, 0.47950012218695337),
            (1.0_f64, 1, 0.31731050786291115),
            (2.0_f64, 1, 0.15729920705028105),
            (3.84_f64, 1, 0.050043521248705085),
            (5.0_f64, 1, 0.025347318677468252),
            (10.0_f64, 1, 0.0015654022580025482),
            (15.0_f64, 1, 0.00010751117672950056),
            (25.0_f64, 1, 5.733031437583878e-07),
            (40.0_f64, 1, 2.5396285894708634e-10),
            (60.0_f64, 1, 9.485737571073854e-15),
            (100.0_f64, 1, 1.5239706048320995e-23),
            (0.1_f64, 2, 0.951229424500714),
            (0.5_f64, 2, 0.7788007830714049),
            (1.0_f64, 2, 0.6065306597126334),
            (2.0_f64, 2, 0.36787944117144245),
            (3.84_f64, 2, 0.14660696213035015),
            (5.0_f64, 2, 0.0820849986238988),
            (10.0_f64, 2, 0.006737946999085468),
            (15.0_f64, 2, 0.0005530843701478337),
            (25.0_f64, 2, 3.7266531720786718e-06),
            (40.0_f64, 2, 2.0611536224385566e-09),
            (60.0_f64, 2, 9.357622968840163e-14),
            (100.0_f64, 2, 1.9287498479639183e-22),
            (0.1_f64, 3, 0.9918374237318764),
            (0.5_f64, 3, 0.9188914116546758),
            (1.0_f64, 3, 0.8012519569012009),
            (2.0_f64, 3, 0.5724067044708798),
            (3.84_f64, 3, 0.2792676171186097),
            (5.0_f64, 3, 0.17179714429673323),
            (10.0_f64, 3, 0.01856613546304323),
            (15.0_f64, 3, 0.0018166489665723223),
            (25.0_f64, 3, 1.544049829110137e-05),
            (40.0_f64, 3, 1.0655090334255846e-08),
            (60.0_f64, 3, 5.878230727906919e-13),
            (100.0_f64, 3, 1.5541594313896026e-21),
            (0.1_f64, 5, 0.9998376833880774),
            (0.5_f64, 5, 0.9921232932326296),
            (1.0_f64, 5, 0.9625657732472964),
            (2.0_f64, 5, 0.8491450360846096),
            (3.84_f64, 5, 0.5726744598320888),
            (5.0_f64, 5, 0.41588018699550783),
            (10.0_f64, 5, 0.07523524614651216),
            (15.0_f64, 5, 0.010362337915786436),
            (25.0_f64, 5, 0.0001393337911856263),
            (40.0_f64, 5, 1.493367900050396e-07),
            (60.0_f64, 5, 1.2154569777183006e-11),
            (100.0_f64, 5, 5.285148360943219e-20),
            (0.1_f64, 10, 0.9999999975020487),
            (0.5_f64, 10, 0.999993388289439),
            (1.0_f64, 10, 0.9998278843700441),
            (2.0_f64, 10, 0.9963401531726563),
            (3.84_f64, 10, 0.9542763043207358),
            (5.0_f64, 10, 0.8911780189141513),
            (10.0_f64, 10, 0.44049328506521246),
            (15.0_f64, 10, 0.13206185628772055),
            (25.0_f64, 10, 0.005345505487134069),
            (40.0_f64, 10, 1.694474393006737e-05),
            (60.0_f64, 10, 3.6243009520614924e-09),
            (100.0_f64, 10, 5.4497019829205215e-17),
            (0.1_f64, 20, 1.0),
            (0.5_f64, 20, 0.9999999999997906),
            (1.0_f64, 20, 0.999999999829033),
            (2.0_f64, 20, 0.9999998885745217),
            (3.84_f64, 20, 0.9999667953164147),
            (5.0_f64, 20, 0.9997226479053791),
            (10.0_f64, 20, 0.9681719426937951),
            (15.0_f64, 20, 0.7764076130197146),
            (25.0_f64, 20, 0.20143110494553587),
            (40.0_f64, 20, 0.0049954123083075785),
            (60.0_f64, 20, 7.12175086281558e-06),
            (100.0_f64, 20, 1.2596084591660936e-12),
            (0.1_f64, 50, 1.0),
            (0.5_f64, 50, 1.0),
            (1.0_f64, 50, 1.0),
            (2.0_f64, 50, 1.0),
            (3.84_f64, 50, 1.0),
            (5.0_f64, 50, 1.0),
            (10.0_f64, 50, 0.9999999998400414),
            (15.0_f64, 50, 0.999999625021075),
            (25.0_f64, 50, 0.9988075511517683),
            (40.0_f64, 50, 0.8432273781737623),
            (60.0_f64, 50, 0.1572420272383916),
            (100.0_f64, 50, 3.4549313829848465e-05),
        ];

        for &(x, df, expected) in REFERENCE {
            let got = chi_squared_survival(x, df);
            let err = if expected > 1e-12 {
                (got - expected).abs() / expected
            } else {
                (got - expected).abs()
            };
            assert!(
                err < 1e-10,
                "sf({x}, {df}) = {got}, expected {expected} (relative error {err:.3e})"
            );
        }

        // P(X > 0) = 1 for every df, and the function is monotone falling.
        for df in 1..40 {
            assert!((chi_squared_survival(0.0, df) - 1.0).abs() < 1e-12);
            let mut prev = 1.0;
            for k in 1..80 {
                let p = chi_squared_survival(f64::from(k), df);
                assert!(p <= prev + 1e-12, "sf is not monotone at x={k}, df={df}");
                assert!(
                    (0.0..=1.0).contains(&p),
                    "sf({k}, {df}) = {p} is not a probability"
                );
                prev = p;
            }
        }
    }
}
