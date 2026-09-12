//! ARIMA(p,d,q) and SARIMA(p,d,q)(P,D,Q)m forecast models.
//!
//! # Estimation
//!
//! Both models are estimated by **conditional sum of squares** (CSS), the
//! standard fast alternative to full maximum likelihood:
//!
//! 1. The series is differenced — seasonally first (`D` times at lag `m`),
//!    then regularly (`d` times).
//! 2. The autoregressive part is seeded by the **Burg** algorithm, which
//!    always returns a stable AR polynomial and behaves well on short series.
//! 3. The remaining parameters are refined by minimising the sum of squared
//!    one-step innovations. `ArimaModel` optimises the moving-average
//!    coefficients with the AR part held at its Burg estimate; `SarimaModel`
//!    optimises all four blocks (φ, Φ, θ, Θ) jointly, because the seasonal
//!    and non-seasonal AR parts interact multiplicatively and cannot be
//!    estimated independently.
//!
//! The first `max(deg φ*, deg θ*)` innovations are **conditioned away**: they
//! are computed from pre-sample history the model does not have, and for
//! `ARIMA(p,0,q)` the first of them is the raw first observation, so counting
//! them would make the residual variance measure the series' level rather than
//! its noise. That is what "conditional" in CSS means.
//!
//! # Seasonality
//!
//! `SarimaModel` expands the multiplicative form
//!
//! ```text
//! φ(B)·Φ(Bᵐ)·(1-B)ᵈ·(1-Bᵐ)ᴰ·xₜ = θ(B)·Θ(Bᵐ)·εₜ
//! ```
//!
//! into a single AR polynomial of degree `p + P·m` and a single MA polynomial
//! of degree `q + Q·m`, then runs the ordinary ARMA recursion over them. One
//! recursion and one forecast path, so the seasonal orders reach the forecast
//! by construction.

use crate::forecast::error::ForecastError;
use crate::forecast::result::{ForecastResult, ModelParams, ModelType};
use crate::forecast::traits::ForecastModel;
use crate::forecast::util::{median_interval, validate_input};

// ── Shared ARMA core ────────────────────────────────────────────────────
//
// One definition of each operation, used by both models. `ArimaModel` is the
// `P = D = Q = 0` case of `SarimaModel`; keeping the recursions here is what
// makes that true rather than merely claimed.

/// Difference a series `d` times at lag 1.
pub(crate) fn difference(values: &[f64], d: usize) -> Vec<f64> {
    let mut current = values.to_vec();
    for _ in 0..d {
        if current.len() < 2 {
            return Vec::new();
        }
        current = current.windows(2).map(|w| w[1] - w[0]).collect();
    }
    current
}

/// Difference a series `d` times at lag `m` (seasonal differencing).
pub(crate) fn seasonal_difference(values: &[f64], m: usize, d: usize) -> Vec<f64> {
    if m == 0 {
        return values.to_vec();
    }
    let mut current = values.to_vec();
    for _ in 0..d {
        if current.len() <= m {
            return Vec::new();
        }
        current = (m..current.len())
            .map(|i| current[i] - current[i - m])
            .collect();
    }
    current
}

/// Reconstruct the original scale from lag-`m` differenced predictions.
///
/// `anchor` is the tail of the *previous* difference level, and must hold at
/// least `m` values: prediction `h` adds back the value `m` steps earlier,
/// which for `h < m` comes from history and for `h >= m` from an already
/// reconstructed prediction.
///
/// Kahan compensated summation bounds the rounding error that otherwise
/// accumulates across a long horizon, where every value is the sum of every
/// prediction before it.
fn undifference_lag(predictions: &[f64], anchor: &[f64], m: usize) -> Vec<f64> {
    let mut buf: Vec<f64> = anchor.to_vec();
    let mut comp = vec![0.0_f64; m.max(1)];
    for (h, &v) in predictions.iter().enumerate() {
        let base = buf[buf.len() - m];
        let slot = h % m.max(1);
        let y = v - comp[slot];
        let t = base + y;
        comp[slot] = (t - base) - y;
        buf.push(t);
    }
    buf[anchor.len()..].to_vec()
}

/// Estimate AR coefficients with the Burg algorithm.
///
/// Burg minimises the sum of forward and backward prediction errors. Unlike
/// Yule–Walker it always produces a stable AR polynomial and behaves better on
/// short series, and it runs in O(N·p) through the Levinson recursion.
///
/// Returns `(coefficients, effective_order)`; the effective order is below `p`
/// when the recursion stops early on near-constant data.
///
/// # Errors
///
/// [`ForecastError::InsufficientData`] when fewer than `p + 1` observations
/// are supplied.
pub(crate) fn estimate_ar_burg(
    values: &[f64],
    p: usize,
) -> Result<(Vec<f64>, usize), ForecastError> {
    if values.len() < p + 1 {
        return Err(ForecastError::InsufficientData {
            min: p + 1,
            got: values.len(),
        });
    }
    if p == 0 {
        return Ok((Vec::new(), 0));
    }

    let n = values.len();
    let mut ef: Vec<f64> = values.to_vec();
    let mut eb: Vec<f64> = values.to_vec();
    let mut ar = vec![0.0; p];
    let mut effective_order = p;

    for m in 0..p {
        let mut num = 0.0;
        let mut den = 0.0;
        for t in (m + 1)..n {
            num += ef[t] * eb[t - 1];
            den += ef[t] * ef[t] + eb[t - 1] * eb[t - 1];
        }
        if den.abs() < 1e-15 {
            // Near-constant data: the reflection coefficient is 0/0. Stop and
            // report the order actually reached rather than dividing.
            effective_order = m;
            tracing::warn!(
                requested_order = p,
                effective_order = m,
                "Burg AR estimation terminated early due to numerical instability"
            );
            break;
        }
        let k = 2.0 * num / den;

        let mut ar_new = vec![0.0; p];
        for j in 0..m {
            ar_new[j] = ar[j] - k * ar[m - 1 - j];
        }
        ar_new[m] = k;
        ar = ar_new;

        let ef_prev = ef.clone();
        let eb_prev = eb.clone();
        for t in (m + 1)..n {
            ef[t] = ef_prev[t] - k * eb_prev[t - 1];
            eb[t] = eb_prev[t - 1] - k * ef_prev[t];
        }
    }

    Ok((ar, effective_order))
}

/// Multiply two polynomials given as coefficient vectors (index = power of B).
fn poly_mul(a: &[f64], b: &[f64]) -> Vec<f64> {
    let mut out = vec![0.0; a.len() + b.len() - 1];
    for (i, &ai) in a.iter().enumerate() {
        if ai == 0.0 {
            continue;
        }
        for (j, &bj) in b.iter().enumerate() {
            out[i + j] += ai * bj;
        }
    }
    out
}

/// Expand `φ(B)·Φ(Bᵐ)` into one AR coefficient vector of degree `p + P·m`.
///
/// Both inputs and the output use the recursion convention
/// `xₜ = Σ φᵢ·xₜ₋ᵢ + …`, i.e. the AR *polynomial* is `1 − Σ φᵢBⁱ`; the sign
/// flip happens here so no caller has to remember it.
pub(crate) fn expand_ar(phi: &[f64], seasonal_phi: &[f64], m: usize) -> Vec<f64> {
    if seasonal_phi.is_empty() || m == 0 {
        return phi.to_vec();
    }
    let mut a = vec![0.0; phi.len() + 1];
    a[0] = 1.0;
    for (i, &c) in phi.iter().enumerate() {
        a[i + 1] = -c;
    }
    let mut s = vec![0.0; seasonal_phi.len() * m + 1];
    s[0] = 1.0;
    for (k, &c) in seasonal_phi.iter().enumerate() {
        s[(k + 1) * m] = -c;
    }
    poly_mul(&a, &s)[1..].iter().map(|c| -c).collect()
}

/// Expand `θ(B)·Θ(Bᵐ)` into one MA coefficient vector of degree `q + Q·m`.
///
/// Convention `xₜ = … + Σ θⱼ·εₜ₋ⱼ + εₜ`, i.e. the MA polynomial is
/// `1 + Σ θⱼBʲ`, so no sign flip is needed — but stating it is what stops the
/// two conventions being mixed, which is the classic way a seasonal MA term
/// ends up with the wrong sign.
pub(crate) fn expand_ma(theta: &[f64], seasonal_theta: &[f64], m: usize) -> Vec<f64> {
    if seasonal_theta.is_empty() || m == 0 {
        return theta.to_vec();
    }
    let mut b = vec![0.0; theta.len() + 1];
    b[0] = 1.0;
    b[1..].copy_from_slice(theta);
    let mut t = vec![0.0; seasonal_theta.len() * m + 1];
    t[0] = 1.0;
    for (k, &c) in seasonal_theta.iter().enumerate() {
        t[(k + 1) * m] = c;
    }
    poly_mul(&b, &t)[1..].to_vec()
}

/// One-step innovations `εₜ` of `xₜ = Σφᵢxₜ₋ᵢ + Σθⱼεₜ₋ⱼ + εₜ`, conditioned on
/// zero pre-sample history.
///
/// The returned vector is aligned with `values`; entries before
/// [`warmup`](conditioning_warmup) are conditioned on history that does not
/// exist and must not be used as residuals.
pub(crate) fn innovations(values: &[f64], ar: &[f64], ma: &[f64]) -> Vec<f64> {
    let mut eps = vec![0.0; values.len()];
    for t in 0..values.len() {
        let mut pred = 0.0;
        for (j, &c) in ar.iter().enumerate() {
            if t > j {
                pred += c * values[t - j - 1];
            }
        }
        for (j, &c) in ma.iter().enumerate() {
            if t > j {
                pred += c * eps[t - j - 1];
            }
        }
        eps[t] = values[t] - pred;
    }
    eps
}

/// Whether the model carries a constant, by the Hyndman–Khandakar rule
/// `d + D <= 1`.
///
/// The constant is the mean of the *fully differenced* series, so what it
/// means depends on how much differencing precedes it: with no differencing
/// it is the **level** the series reverts to, and with one it is the **drift**
/// per step. Without it, `ARIMA(1,0,0)` on a series sitting at 100 has to
/// explain that level with a near-unit root, and `ARIMA(0,1,0)` on a trend
/// forecasts a flat line.
///
/// Above one difference a constant implies a polynomial trend that keeps
/// accelerating, which is essentially never what is wanted from a metric —
/// which is why `auto.arima` draws the line in the same place.
#[inline]
pub(crate) fn includes_constant(d: usize, seasonal_d: usize) -> bool {
    d + seasonal_d <= 1
}

/// Number of leading innovations that are conditioned on absent history.
#[inline]
pub(crate) fn conditioning_warmup(ar_len: usize, ma_len: usize) -> usize {
    ar_len.max(ma_len)
}

/// Conditional sum of squares: the CSS objective.
fn css(values: &[f64], ar: &[f64], ma: &[f64]) -> f64 {
    let warmup = conditioning_warmup(ar.len(), ma.len());
    if values.len() <= warmup {
        return f64::INFINITY;
    }
    let eps = innovations(values, ar, ma);
    eps[warmup..].iter().map(|e| e * e).sum()
}

/// ψ-weights of the MA(∞) representation `xₜ = Σⱼ ψⱼ·εₜ₋ⱼ`, `ψ₀ = 1`.
///
/// The forecast variance at horizon `h` is `σ²·Σⱼ₌₀^{h-1} ψⱼ²`. Recursion:
/// `ψⱼ = Σᵢ₌₁ᵖ φᵢ·ψⱼ₋ᵢ + θⱼ`, with `θⱼ = 0` for `j > q`.
pub(crate) fn psi_weights(ar: &[f64], ma: &[f64], horizon: usize) -> Vec<f64> {
    let mut psi = Vec::with_capacity(horizon.max(1));
    psi.push(1.0);
    for j in 1..horizon {
        let mut val = 0.0;
        for (i, &c) in ar.iter().enumerate() {
            if j > i {
                val += c * psi[j - i - 1];
            }
        }
        if j <= ma.len() {
            val += ma[j - 1];
        }
        psi.push(val);
    }
    psi
}

/// Extend a differenced series `horizon` steps under the ARMA recursion,
/// returning only the new values. Future innovations are zero, which is the
/// conditional expectation.
fn arma_forecast(
    values: &[f64],
    residuals: &[f64],
    ar: &[f64],
    ma: &[f64],
    horizon: usize,
) -> Vec<f64> {
    let n = values.len();
    let mut extended = values.to_vec();
    let mut eps = residuals.to_vec();
    // Align the innovation history with the value history: a shorter residual
    // vector (after streaming updates trimmed it) must not shift the MA lags.
    // Both adjustments happen at the *front*: `truncate` dropped the newest
    // innovations, which are exactly the ones the MA lags read, so an
    // over-long buffer shifted every lag by the excess.
    if eps.len() < n {
        let mut padded = vec![0.0; n - eps.len()];
        padded.extend_from_slice(&eps);
        eps = padded;
    } else if eps.len() > n {
        eps.drain(..eps.len() - n);
    }

    for _ in 0..horizon {
        let idx = extended.len();
        let mut pred = 0.0;
        for (j, &c) in ar.iter().enumerate() {
            if idx > j {
                pred += c * extended[idx - j - 1];
            }
        }
        for (j, &c) in ma.iter().enumerate() {
            if idx > j {
                pred += c * eps[idx - j - 1];
            }
        }
        extended.push(pred);
        eps.push(0.0);
    }
    extended[n..].to_vec()
}

/// Residual standard deviation over the innovations that are not conditioned
/// on absent history, with a degrees-of-freedom correction for the estimated
/// parameters.
fn residual_std(residuals: &[f64], warmup: usize, n_params: usize) -> f64 {
    let usable = residuals.get(warmup..).unwrap_or(&[]);
    if usable.len() < 2 {
        return 0.0;
    }
    let mean: f64 = usable.iter().sum::<f64>() / usable.len() as f64;
    let dof = usable.len().saturating_sub(n_params).max(1);
    let var: f64 = usable.iter().map(|&r| (r - mean).powi(2)).sum::<f64>() / dof as f64;
    var.sqrt()
}

/// The **full** autoregressive operator of an ARIMA/SARIMA model,
/// `φ(B)·Φ(Bᵐ)·(1-B)ᵈ·(1-Bᵐ)ᴰ`, expanded into a single coefficient vector in
/// the same convention as `ar` (`xₜ = Σᵢ cᵢ·xₜ₋ᵢ + …`).
///
/// # Why the differencing has to be in here
///
/// The ψ-weights of the *stationary* ARMA part describe the error of the
/// differenced series. The forecast is issued on the original scale, and
/// integration accumulates those errors: for `ARIMA(0,1,0)` every ψⱼ is 1, so
/// `Var[e_h] = h·σ²` and the interval widens as `√h`. Taking ψ from the ARMA
/// part alone gives `ψ = [1]` and a **constant-width** interval for a random
/// walk — the textbook example of an interval that must widen.
pub(crate) fn integrated_ar(ar: &[f64], d: usize, seasonal_d: usize, m: usize) -> Vec<f64> {
    // φ(B) = 1 - φ₁B - … - φₚBᵖ
    let mut poly = Vec::with_capacity(ar.len() + 1);
    poly.push(1.0);
    poly.extend(ar.iter().map(|c| -c));
    for _ in 0..d {
        poly = poly_mul(&poly, &[1.0, -1.0]);
    }
    if m > 0 && seasonal_d > 0 {
        let mut sd = vec![0.0; m + 1];
        sd[0] = 1.0;
        sd[m] = -1.0;
        for _ in 0..seasonal_d {
            poly = poly_mul(&poly, &sd);
        }
    }
    poly[1..].iter().map(|c| -c).collect()
}

/// Build symmetric 95 % prediction intervals from the ψ-weights of the
/// **integrated** model — see [`integrated_ar`].
fn prediction_intervals(
    values: &[f64],
    ar: &[f64],
    ma: &[f64],
    sigma: f64,
    horizon: usize,
    d: usize,
    seasonal_d: usize,
    m: usize,
) -> (Vec<f64>, Vec<f64>) {
    const Z95: f64 = 1.959_963_984_540_054;
    let phi_star = integrated_ar(ar, d, seasonal_d, m);
    let psi = psi_weights(&phi_star, ma, horizon);
    let mut lower = Vec::with_capacity(horizon);
    let mut upper = Vec::with_capacity(horizon);
    for h in 1..=horizon {
        let scale = psi[..h].iter().map(|w| w * w).sum::<f64>().sqrt();
        let width = Z95 * sigma * scale;
        lower.push(values[h - 1] - width);
        upper.push(values[h - 1] + width);
    }
    (lower, upper)
}

/// Forecast timestamps at `last_ts + h·interval`, saturating rather than
/// wrapping on overflow.
fn forecast_timestamps(last_ts: i64, interval_ns: i64, horizon: usize) -> Vec<i64> {
    (1..=horizon as i64)
        .map(|h| last_ts.saturating_add(h.saturating_mul(interval_ns)))
        .collect()
}

// ── ARIMA ───────────────────────────────────────────────────────────────

/// ARIMA(p,d,q) — non-seasonal, estimated by Burg + conditional sum of
/// squares.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct ArimaModel {
    p: usize,
    d: usize,
    q: usize,
    params: ModelParams,
    history: Vec<f64>,
    residuals: Vec<f64>,
    /// Mean of the differenced series; the level for `d = 0`, the drift for
    /// `d = 1`, and zero above that.
    constant: f64,
    /// Leading innovations conditioned on absent history; excluded from every
    /// residual statistic.
    warmup: usize,
    last_ts: i64,
    interval_ns: i64,
    fitted: bool,
}

impl ArimaModel {
    /// Creates a new ARIMA model with the given orders.
    #[must_use]
    pub fn new(p: usize, d: usize, q: usize) -> Self {
        Self {
            p,
            d,
            q,
            params: ModelParams::Arima {
                p,
                d,
                q,
                ar_coeffs: Vec::new(),
                ma_coeffs: Vec::new(),
                constant: 0.0,
                residual_std: 0.0,
                effective_ar_order: None,
            },
            history: Vec::new(),
            residuals: Vec::new(),
            constant: 0.0,
            warmup: 0,
            last_ts: 0,
            interval_ns: 0,
            fitted: false,
        }
    }

    /// Estimate MA coefficients by conditional sum of squares, with the AR
    /// part held at its Burg estimate.
    ///
    /// Brent's method for `q == 1`, Nelder–Mead above it. The objective skips
    /// the conditioning warm-up, so the first observations — which for `d = 0`
    /// carry the *level* of the series rather than its noise — do not steer
    /// the optimiser.
    fn estimate_ma(values: &[f64], ar: &[f64], q: usize) -> Vec<f64> {
        if q == 0 {
            return Vec::new();
        }
        let objective = |theta: &[f64]| css(values, ar, theta);
        if q == 1 {
            let opt = crate::forecast::optimizer::minimize_brent(
                |x| objective(&[x]),
                -0.99,
                0.99,
                1e-8,
                200,
            );
            vec![opt.params[0]]
        } else {
            let bounds: Vec<crate::forecast::optimizer::Bound> = (0..q)
                .map(|_| crate::forecast::optimizer::Bound::new(-0.99, 0.99))
                .collect();
            let x0 = vec![0.0; q];
            crate::forecast::optimizer::minimize_nelder_mead(objective, &x0, &bounds, 500, 1e-8)
                .params
        }
    }

    /// Free parameters, counting the innovation variance and the constant if
    /// the model carries one. This is the `k` of every information criterion
    /// below; counting a constant that is not there is a quiet way to make
    /// two model families incomparable.
    fn n_params(&self) -> usize {
        self.p + self.q + usize::from(includes_constant(self.d, 0)) + 1
    }

    /// Residual sum of squares over the innovations that are not conditioned
    /// on absent history, and how many there are.
    fn usable_rss(&self) -> Option<(f64, usize)> {
        let warmup = self.warmup.min(self.residuals.len());
        let usable = &self.residuals[warmup..];
        if usable.is_empty() {
            return None;
        }
        Some((usable.iter().map(|r| r * r).sum(), usable.len()))
    }

    /// Akaike Information Criterion, `n·ln(RSS/n) + 2k`.
    ///
    /// `n` counts only the innovations the conditional likelihood is defined
    /// over, which is what makes the value comparable between two models of
    /// the same series at the same differencing order. It is *not* comparable
    /// across differencing orders — a differenced series is a different
    /// series — which is why [`auto_arima`] chooses `d` by a unit-root test
    /// rather than by this number.
    #[must_use]
    pub fn aic(&self) -> Option<f64> {
        if !self.fitted {
            return None;
        }
        let (rss, n) = self.usable_rss()?;
        crate::forecast::diagnostics::aic(rss, n, self.n_params())
    }

    /// Bayesian Information Criterion, `n·ln(RSS/n) + k·ln(n)`.
    #[must_use]
    pub fn bic(&self) -> Option<f64> {
        if !self.fitted {
            return None;
        }
        let (rss, n) = self.usable_rss()?;
        crate::forecast::diagnostics::bic(rss, n, self.n_params())
    }

    /// Corrected Akaike Information Criterion (AICc).
    #[must_use]
    pub fn aicc(&self) -> Option<f64> {
        if !self.fitted {
            return None;
        }
        let (rss, n) = self.usable_rss()?;
        crate::forecast::diagnostics::aicc(rss, n, self.n_params())
    }

    /// Comprehensive model diagnostics.
    ///
    /// Requires both the original training values and the in-sample
    /// predictions (fitted values) for the model.
    #[must_use]
    pub fn diagnostics(
        &self,
        actual: &[f64],
        predicted: &[f64],
    ) -> Option<crate::forecast::diagnostics::ModelDiagnostics> {
        if !self.fitted {
            return None;
        }
        crate::forecast::diagnostics::compute_diagnostics(actual, predicted, self.n_params(), None)
    }
}

impl ForecastModel for ArimaModel {
    #[tracing::instrument(skip_all, level = "debug")]
    fn fit(&mut self, timestamps: &[i64], values: &[f64]) -> Result<(), ForecastError> {
        let _start = std::time::Instant::now();
        validate_input(timestamps, values)?;
        let min_required = self.p.max(self.q) + 2 * self.d + 10;
        if values.len() < min_required {
            return Err(ForecastError::InsufficientData {
                min: min_required,
                got: values.len(),
            });
        }

        self.history = values.to_vec();
        let diffed = difference(values, self.d);
        if diffed.is_empty() {
            return Err(ForecastError::InsufficientData {
                min: min_required,
                got: values.len(),
            });
        }
        self.constant = if includes_constant(self.d, 0) {
            diffed.iter().sum::<f64>() / diffed.len() as f64
        } else {
            0.0
        };
        let centered: Vec<f64> = diffed.iter().map(|v| v - self.constant).collect();

        let (ar_coeffs, effective_ar_order) = estimate_ar_burg(&centered, self.p)?;
        let ma_coeffs = Self::estimate_ma(&centered, &ar_coeffs, self.q);
        let residuals = innovations(&centered, &ar_coeffs, &ma_coeffs);

        // Emit the effective order so a dashboard can see silent degradation.
        metrics::gauge!("chronix_forecast_arima_effective_order",
            "requested" => format!("{}", self.p))
        .set(effective_ar_order as f64);

        self.warmup = conditioning_warmup(ar_coeffs.len(), ma_coeffs.len());
        let sigma = residual_std(&residuals, self.warmup, self.n_params());

        self.residuals = residuals;
        self.interval_ns = median_interval(timestamps);
        self.last_ts = timestamps[timestamps.len() - 1];

        self.params = ModelParams::Arima {
            p: self.p,
            d: self.d,
            q: self.q,
            ar_coeffs,
            ma_coeffs,
            constant: self.constant,
            residual_std: sigma,
            effective_ar_order: (effective_ar_order < self.p).then_some(effective_ar_order),
        };
        self.fitted = true;
        metrics::histogram!("chronix_forecast_fit_duration_seconds", "model_type" => "arima")
            .record(_start.elapsed().as_secs_f64());
        Ok(())
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn predict(&self, horizon: usize) -> Result<ForecastResult, ForecastError> {
        let _start = std::time::Instant::now();
        if !self.fitted {
            return Err(ForecastError::NotFitted);
        }
        let ModelParams::Arima {
            ar_coeffs,
            ma_coeffs,
            constant,
            residual_std: sigma,
            ..
        } = &self.params
        else {
            return Err(ForecastError::InvalidInput(
                "unexpected model params variant".into(),
            ));
        };

        let diffed = difference(&self.history, self.d);
        let centered: Vec<f64> = diffed.iter().map(|v| v - constant).collect();
        let diff_preds: Vec<f64> =
            arma_forecast(&centered, &self.residuals, ar_coeffs, ma_coeffs, horizon)
                .into_iter()
                .map(|v| v + constant)
                .collect();

        // Undifference outward: anchor `i` is the last value of the series
        // differenced `i` times, so index 0 is the original scale.
        let mut anchors = Vec::with_capacity(self.d);
        let mut level = self.history.clone();
        for _ in 0..self.d {
            anchors.push(
                *level
                    .last()
                    .ok_or(ForecastError::InsufficientData { min: 1, got: 0 })?,
            );
            level = difference(&level, 1);
        }
        let mut values = diff_preds;
        for anchor in anchors.into_iter().rev() {
            values = undifference_lag(&values, &[anchor], 1);
        }

        let (lower, upper) =
            prediction_intervals(&values, ar_coeffs, ma_coeffs, *sigma, horizon, self.d, 0, 0);

        metrics::histogram!("chronix_forecast_predict_duration_seconds", "model_type" => "arima")
            .record(_start.elapsed().as_secs_f64());
        Ok(ForecastResult {
            values,
            timestamps: forecast_timestamps(self.last_ts, self.interval_ns, horizon),
            confidence_lower: lower,
            confidence_upper: upper,
            confidence_level: 0.95,
        })
    }

    fn update(&mut self, timestamp: i64, value: f64) -> Result<(), ForecastError> {
        if !self.fitted {
            return Err(ForecastError::NotFitted);
        }
        self.history.push(value);
        self.last_ts = timestamp;

        let ModelParams::Arima {
            ar_coeffs,
            ma_coeffs,
            constant,
            ..
        } = &self.params
        else {
            return Err(ForecastError::InvalidInput(
                "unexpected model params variant".into(),
            ));
        };
        let diffed: Vec<f64> = difference(&self.history, self.d)
            .into_iter()
            .map(|v| v - constant)
            .collect();
        let Some(idx) = diffed.len().checked_sub(1) else {
            return Ok(());
        };
        let mut pred = 0.0;
        for (j, &c) in ar_coeffs.iter().enumerate() {
            if idx > j {
                pred += c * diffed[idx - j - 1];
            }
        }
        for (j, &c) in ma_coeffs.iter().enumerate() {
            if let Some(e) = idx.checked_sub(j + 1).and_then(|k| self.residuals.get(k)) {
                pred += c * e;
            }
        }
        self.residuals.push(diffed[idx] - pred);

        // Bound the buffers so a long-running stream does not grow without
        // limit, keeping enough context for the model orders.
        //
        // `residuals` is aligned one-for-one with `difference(history, d)`,
        // which is the only reason `predict` may read them at the same index.
        // Trimming the two to *independent* caps broke that alignment by `d`
        // after the first trim, silently shifting every MA lag from then on.
        // One cap, and the residual buffer follows the history buffer.
        let max_history = (self.p + self.d + self.q + 1).max(64) * 4;
        if self.history.len() > max_history {
            self.history.drain(..self.history.len() - max_history);
        }
        let want_residuals = self.history.len().saturating_sub(self.d);
        if self.residuals.len() > want_residuals {
            self.residuals
                .drain(..self.residuals.len() - want_residuals);
        }
        Ok(())
    }

    fn model_type(&self) -> ModelType {
        ModelType::Arima
    }

    fn params(&self) -> &ModelParams {
        &self.params
    }
}

// ── SARIMA ──────────────────────────────────────────────────────────────

/// Configuration for constructing a [`SarimaModel`].
///
/// A config struct rather than a seven-parameter constructor: `(1, 1, 1, 0,
/// 1, 1, 24)` is unreadable at a call site, and swapping two of those numbers
/// changes the model silently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SarimaConfig {
    /// Non-seasonal autoregressive order `p`.
    pub p: usize,
    /// Non-seasonal differencing order `d`.
    pub d: usize,
    /// Non-seasonal moving-average order `q`.
    pub q: usize,
    /// Seasonal autoregressive order `P`.
    pub sp: usize,
    /// Seasonal differencing order `D`.
    pub sd: usize,
    /// Seasonal moving-average order `Q`.
    pub sq: usize,
    /// Seasonal period `m` (e.g. 24 for hourly data with daily seasonality).
    pub m: usize,
}

/// SARIMA(p,d,q)(P,D,Q)m.
///
/// The multiplicative form is expanded into a single AR polynomial of degree
/// `p + P·m` and a single MA polynomial of degree `q + Q·m`, so the seasonal
/// orders participate in the forecast recursion rather than only in the
/// stored parameters. All four coefficient blocks are estimated jointly by
/// conditional sum of squares, seeded from a Burg fit of the non-seasonal AR
/// part — the seasonal and non-seasonal AR factors multiply, so estimating
/// one while pretending the other is absent biases both.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct SarimaModel {
    cfg_p: usize,
    cfg_d: usize,
    cfg_q: usize,
    sp: usize,
    sd: usize,
    sq: usize,
    m: usize,
    params: ModelParams,
    /// Original-scale training values.
    history: Vec<f64>,
    /// Innovations aligned with the fully differenced series.
    residuals: Vec<f64>,
    /// Expanded AR polynomial `φ*` of degree `p + P·m`.
    ar_expanded: Vec<f64>,
    /// Expanded MA polynomial `θ*` of degree `q + Q·m`.
    ma_expanded: Vec<f64>,
    /// Mean of the fully differenced series; see [`includes_constant`].
    constant: f64,
    warmup: usize,
    last_ts: i64,
    interval_ns: i64,
    fitted: bool,
}

impl SarimaModel {
    /// Creates a new SARIMA model from a [`SarimaConfig`].
    #[must_use]
    pub fn from_config(cfg: SarimaConfig) -> Self {
        Self::new(cfg.p, cfg.d, cfg.q, cfg.sp, cfg.sd, cfg.sq, cfg.m)
    }

    /// Creates a new SARIMA model with explicit order parameters.
    ///
    /// A period `m` of 0 or 1 leaves no seasonal structure to model; the
    /// seasonal orders are then ignored and the model degenerates to
    /// ARIMA(p,d,q), which is what those periods mean.
    #[must_use]
    pub fn new(p: usize, d: usize, q: usize, sp: usize, sd: usize, sq: usize, m: usize) -> Self {
        let seasonal = m > 1;
        let (sp, sd, sq) = if seasonal { (sp, sd, sq) } else { (0, 0, 0) };
        Self {
            cfg_p: p,
            cfg_d: d,
            cfg_q: q,
            sp,
            sd,
            sq,
            m,
            params: ModelParams::Sarima {
                p,
                d,
                q,
                sp,
                sd,
                sq,
                m,
                ar_coeffs: Vec::new(),
                ma_coeffs: Vec::new(),
                sar_coeffs: Vec::new(),
                sma_coeffs: Vec::new(),
                constant: 0.0,
                residual_std: 0.0,
            },
            history: Vec::new(),
            residuals: Vec::new(),
            ar_expanded: Vec::new(),
            ma_expanded: Vec::new(),
            constant: 0.0,
            warmup: 0,
            last_ts: 0,
            interval_ns: 0,
            fitted: false,
        }
    }

    /// Fully difference the series: seasonally `D` times at lag `m`, then
    /// regularly `d` times.
    fn transform(&self, values: &[f64]) -> Vec<f64> {
        difference(&seasonal_difference(values, self.m, self.sd), self.cfg_d)
    }

    /// The fully differenced series with the constant removed — the scale the
    /// ARMA recursion is defined on.
    fn centered(&self, values: &[f64]) -> Vec<f64> {
        self.transform(values)
            .into_iter()
            .map(|v| v - self.constant)
            .collect()
    }

    /// Free parameters, counting the innovation variance and the constant.
    fn n_free(&self) -> usize {
        self.cfg_p
            + self.sp
            + self.cfg_q
            + self.sq
            + usize::from(includes_constant(self.cfg_d, self.sd))
            + 1
    }

    /// The expanded polynomials implied by a packed parameter vector
    /// `[φ₁…φ_p, Φ₁…Φ_P, θ₁…θ_q, Θ₁…Θ_Q]`.
    fn expand(&self, x: &[f64]) -> (Vec<f64>, Vec<f64>) {
        let (p, sp, q) = (self.cfg_p, self.sp, self.cfg_q);
        let ar = expand_ar(&x[..p], &x[p..p + sp], self.m);
        let ma = expand_ma(&x[p + sp..p + sp + q], &x[p + sp + q..], self.m);
        (ar, ma)
    }
}

impl ForecastModel for SarimaModel {
    #[tracing::instrument(skip_all, level = "debug")]
    fn fit(&mut self, timestamps: &[i64], values: &[f64]) -> Result<(), ForecastError> {
        let _start = std::time::Instant::now();
        validate_input(timestamps, values)?;

        // The expanded polynomials reach back `p + P·m` steps, and each round
        // of differencing costs observations up front. Ten usable innovations
        // is the floor below which the CSS objective is noise.
        let reach = (self.cfg_p + self.sp * self.m).max(self.cfg_q + self.sq * self.m);
        let min_required = reach + 2 * self.cfg_d + 2 * self.sd * self.m + 10;
        if values.len() < min_required {
            return Err(ForecastError::InsufficientData {
                min: min_required,
                got: values.len(),
            });
        }

        self.history = values.to_vec();
        let differenced = self.transform(values);
        if differenced.is_empty() {
            return Err(ForecastError::InsufficientData {
                min: min_required,
                got: values.len(),
            });
        }
        self.constant = if includes_constant(self.cfg_d, self.sd) {
            differenced.iter().sum::<f64>() / differenced.len() as f64
        } else {
            0.0
        };
        let w: Vec<f64> = differenced.iter().map(|v| v - self.constant).collect();

        let n_params = self.cfg_p + self.sp + self.cfg_q + self.sq;
        let mut x0 = vec![0.0; n_params];
        if self.cfg_p > 0 {
            let (phi, _) = estimate_ar_burg(&w, self.cfg_p)?;
            x0[..self.cfg_p].copy_from_slice(&phi);
        }

        let best = if n_params == 0 {
            x0
        } else {
            let objective = |x: &[f64]| {
                let (ar, ma) = self.expand(x);
                css(&w, &ar, &ma)
            };
            if n_params == 1 {
                let opt = crate::forecast::optimizer::minimize_brent(
                    |v| objective(&[v]),
                    -0.99,
                    0.99,
                    1e-8,
                    200,
                );
                vec![opt.params[0]]
            } else {
                let bounds: Vec<crate::forecast::optimizer::Bound> = (0..n_params)
                    .map(|_| crate::forecast::optimizer::Bound::new(-0.99, 0.99))
                    .collect();
                crate::forecast::optimizer::minimize_nelder_mead(
                    objective, &x0, &bounds, 1000, 1e-9,
                )
                .params
            }
        };

        let (ar_expanded, ma_expanded) = self.expand(&best);
        let residuals = innovations(&w, &ar_expanded, &ma_expanded);
        self.warmup = conditioning_warmup(ar_expanded.len(), ma_expanded.len());
        let sigma = residual_std(&residuals, self.warmup, self.n_free());

        let (p, sp, q) = (self.cfg_p, self.sp, self.cfg_q);
        self.params = ModelParams::Sarima {
            p,
            d: self.cfg_d,
            q,
            sp,
            sd: self.sd,
            sq: self.sq,
            m: self.m,
            ar_coeffs: best[..p].to_vec(),
            ma_coeffs: best[p + sp..p + sp + q].to_vec(),
            sar_coeffs: best[p..p + sp].to_vec(),
            sma_coeffs: best[p + sp + q..].to_vec(),
            constant: self.constant,
            residual_std: sigma,
        };
        self.ar_expanded = ar_expanded;
        self.ma_expanded = ma_expanded;
        self.residuals = residuals;
        self.interval_ns = median_interval(timestamps);
        self.last_ts = timestamps[timestamps.len() - 1];
        self.fitted = true;

        metrics::histogram!("chronix_forecast_fit_duration_seconds", "model_type" => "sarima")
            .record(_start.elapsed().as_secs_f64());
        Ok(())
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn predict(&self, horizon: usize) -> Result<ForecastResult, ForecastError> {
        let _start = std::time::Instant::now();
        if !self.fitted {
            return Err(ForecastError::NotFitted);
        }
        let ModelParams::Sarima {
            residual_std: sigma,
            ..
        } = &self.params
        else {
            return Err(ForecastError::InvalidInput(
                "unexpected model params variant".into(),
            ));
        };

        let w = self.centered(&self.history);
        let mut values: Vec<f64> = arma_forecast(
            &w,
            &self.residuals,
            &self.ar_expanded,
            &self.ma_expanded,
            horizon,
        )
        .into_iter()
        .map(|v| v + self.constant)
        .collect();

        // Undifference in the reverse of the order differencing was applied:
        // regular first (innermost), then seasonal. Each level's anchor is the
        // tail of the series differenced one step less than the predictions.
        let seasonal_base = seasonal_difference(&self.history, self.m, self.sd);
        let mut regular_levels = Vec::with_capacity(self.cfg_d);
        let mut level = seasonal_base.clone();
        for _ in 0..self.cfg_d {
            regular_levels.push(level.clone());
            level = difference(&level, 1);
        }
        for hist in regular_levels.into_iter().rev() {
            let anchor = *hist
                .last()
                .ok_or(ForecastError::InsufficientData { min: 1, got: 0 })?;
            values = undifference_lag(&values, &[anchor], 1);
        }

        let mut seasonal_levels = Vec::with_capacity(self.sd);
        let mut level = self.history.clone();
        for _ in 0..self.sd {
            seasonal_levels.push(level.clone());
            level = seasonal_difference(&level, self.m, 1);
        }
        for hist in seasonal_levels.into_iter().rev() {
            if hist.len() < self.m {
                return Err(ForecastError::InsufficientData {
                    min: self.m,
                    got: hist.len(),
                });
            }
            let anchor = &hist[hist.len() - self.m..];
            values = undifference_lag(&values, anchor, self.m);
        }

        // The interval is computed from the ψ-weights of the *integrated*
        // operator: differencing has unit lead coefficient, so the one-step
        // error is the same on both scales, but beyond one step integration
        // accumulates the earlier errors and the interval on the original
        // scale is strictly wider than the one on the differenced scale.
        let (lower, upper) = prediction_intervals(
            &values,
            &self.ar_expanded,
            &self.ma_expanded,
            *sigma,
            horizon,
            self.cfg_d,
            self.sd,
            self.m,
        );

        metrics::histogram!("chronix_forecast_predict_duration_seconds", "model_type" => "sarima")
            .record(_start.elapsed().as_secs_f64());
        Ok(ForecastResult {
            values,
            timestamps: forecast_timestamps(self.last_ts, self.interval_ns, horizon),
            confidence_lower: lower,
            confidence_upper: upper,
            confidence_level: 0.95,
        })
    }

    fn update(&mut self, timestamp: i64, value: f64) -> Result<(), ForecastError> {
        if !self.fitted {
            return Err(ForecastError::NotFitted);
        }
        self.history.push(value);
        self.last_ts = timestamp;

        let w = self.centered(&self.history);
        let Some(idx) = w.len().checked_sub(1) else {
            return Ok(());
        };
        let mut pred = 0.0;
        for (j, &c) in self.ar_expanded.iter().enumerate() {
            if idx > j {
                pred += c * w[idx - j - 1];
            }
        }
        for (j, &c) in self.ma_expanded.iter().enumerate() {
            if let Some(e) = idx.checked_sub(j + 1).and_then(|k| self.residuals.get(k)) {
                pred += c * e;
            }
        }
        self.residuals.push(w[idx] - pred);

        // Keep enough original-scale history to re-derive the differenced
        // series the recursion reads, plus a margin — and keep `residuals`
        // aligned one-for-one with that differenced series, which is the
        // invariant `predict` depends on. See `ArimaModel::update`.
        let reach = self.ar_expanded.len().max(self.ma_expanded.len());
        let max_history = (reach + self.cfg_d + self.sd * self.m + 1).max(64) * 4;
        if self.history.len() > max_history {
            self.history.drain(..self.history.len() - max_history);
        }
        let want_residuals = self.centered(&self.history).len();
        if self.residuals.len() > want_residuals {
            self.residuals
                .drain(..self.residuals.len() - want_residuals);
        }
        Ok(())
    }

    fn model_type(&self) -> ModelType {
        ModelType::Sarima
    }

    fn params(&self) -> &ModelParams {
        &self.params
    }
}

// ── Unit-root testing: how many times to difference ─────────────────────

/// Critical value of the KPSS level-stationarity statistic at 5 %.
///
/// Kwiatkowski, Phillips, Schmidt & Shin (1992), Table 1, level-stationary
/// case: 10 % → 0.347, **5 % → 0.463**, 2.5 % → 0.574, 1 % → 0.739.
pub const KPSS_CRITICAL_5PCT: f64 = 0.463;

/// KPSS test statistic for level stationarity.
///
/// The null hypothesis is that the series is stationary around a constant, so
/// a statistic **above** the critical value is evidence that it is not and
/// that a difference is called for. That direction is the opposite of an
/// augmented Dickey–Fuller test and is the usual way this test is misread.
///
/// The long-run variance uses a Bartlett kernel with the Schwert short
/// truncation lag `⌊4·(n/100)^¼⌋`, which is the default in both R's
/// `tseries::kpss.test` and `urca::ur.kpss`.
///
/// Returns 0 for a series with fewer than three observations or no variance,
/// both of which are stationary by any reading.
#[must_use]
pub fn kpss_statistic(values: &[f64]) -> f64 {
    let n = values.len();
    if n < 3 {
        return 0.0;
    }
    let n_f = n as f64;
    let mean = values.iter().sum::<f64>() / n_f;

    // Partial sums of the demeaned series.
    let mut partial = 0.0;
    let mut sum_sq_partial = 0.0;
    let mut gamma0 = 0.0;
    let resid: Vec<f64> = values.iter().map(|v| v - mean).collect();
    for &e in &resid {
        partial += e;
        sum_sq_partial += partial * partial;
        gamma0 += e * e;
    }
    gamma0 /= n_f;
    if gamma0 <= 0.0 {
        return 0.0;
    }

    // Bartlett-weighted long-run variance.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let lag = (4.0 * (n_f / 100.0).powf(0.25)).floor() as usize;
    let mut long_run = gamma0;
    for j in 1..=lag.min(n - 1) {
        let gamma_j: f64 = resid[j..]
            .iter()
            .zip(&resid[..n - j])
            .map(|(a, b)| a * b)
            .sum::<f64>()
            / n_f;
        long_run += 2.0 * (1.0 - j as f64 / (lag + 1) as f64) * gamma_j;
    }
    if long_run <= 0.0 {
        return 0.0;
    }

    sum_sq_partial / (n_f * n_f * long_run)
}

/// Choose the differencing order `d` by successive KPSS tests.
///
/// This is the Hyndman–Khandakar rule used by R's `auto.arima`: difference
/// while the series still rejects level stationarity, up to `max_d`.
///
/// It exists because **AIC cannot choose `d`**. Differencing changes the
/// series the likelihood is computed over — a different number of
/// observations, on a different scale — so the value is not comparable across
/// orders, and comparing it anyway systematically over-differences: on
/// stationary noise the `d = 1` fits win every time, and the resulting model
/// forecasts a random walk.
#[must_use]
pub fn select_differencing_order(values: &[f64], max_d: usize) -> usize {
    let mut current = values.to_vec();
    let mut d = 0;
    while d < max_d {
        if current.len() < 4 || kpss_statistic(&current) <= KPSS_CRITICAL_5PCT {
            break;
        }
        current = difference(&current, 1);
        d += 1;
    }
    d
}

/// Seasonal strength above which a series is treated as having a seasonal
/// unit root, from Wang, Smith & Hyndman (2006).
///
/// R's `forecast::nsdiffs` uses this exact threshold as its **default** test,
/// with the comment "Threshold chosen based on seasonal M3 auto.arima
/// accuracy" — it was fitted by minimising MASE over the M3 and M4
/// collections rather than derived, which is why it is a bare number and why
/// copying it is better than inventing one.
pub const SEASONAL_STRENGTH_THRESHOLD: f64 = 0.64;

/// How many seasonal differences a series needs, from its seasonal strength.
///
/// The measure is `max(0, min(1, 1 − Var(remainder) / Var(remainder +
/// seasonal)))` over an STL decomposition: the share of the seasonal-plus-noise
/// variance that the seasonal component explains. One seasonal difference is
/// taken when it exceeds [`SEASONAL_STRENGTH_THRESHOLD`].
///
/// This is the default test in R's `forecast::nsdiffs`, in preference to OCSB
/// — which that package keeps as an option rather than a default. Reading the
/// two guards out of its source mattered as much as the formula: a **constant**
/// series needs no differencing however the variance ratio comes out (both
/// variances are zero), and a series shorter than two periods cannot be
/// decomposed at all, let alone differenced.
///
/// # Why this is not cosmetic
///
/// The alternative is what stood here: a fixed `D = 1`. Every seasonal
/// candidate was seasonally differenced, so a series with strong but
/// *stationary* seasonality — a daily load curve that repeats rather than
/// drifting — was always over-differenced, and the model that fits it,
/// `SARIMA(p,d,q)(P,0,Q)[m]`, could not be proposed at all. Over-differencing
/// does not fail; it inflates the forecast variance and widens every interval
/// built on it.
#[must_use]
pub fn select_seasonal_differencing_order(values: &[f64], period: usize, max_d: usize) -> usize {
    if period < 2 || max_d == 0 {
        return 0;
    }
    // Fewer than two full periods cannot be decomposed, and `nsdiffs`
    // refuses for the same reason.
    if values.len() < 2 * period {
        return 0;
    }
    // A constant series has zero variance in both terms, so the ratio is
    // meaningless; R checks this before the heuristic and so does this.
    let first = values.first().copied().unwrap_or(0.0);
    if values.iter().all(|v| (v - first).abs() < f64::EPSILON) {
        return 0;
    }

    let mut current = values.to_vec();
    let mut d = 0usize;
    while d < max_d {
        let Some(strength) = seasonal_strength(&current, period) else {
            break;
        };
        if strength <= SEASONAL_STRENGTH_THRESHOLD {
            break;
        }
        current = seasonal_difference(&current, period, 1);
        d += 1;
        if current.len() < 2 * period {
            break;
        }
    }
    d
}

/// `max(0, min(1, 1 − Var(remainder) / Var(remainder + seasonal)))`, or
/// `None` if the series cannot be decomposed.
fn seasonal_strength(values: &[f64], period: usize) -> Option<f64> {
    let decomposition = crate::preprocess::decomposition::stl_decompose(
        values,
        &crate::preprocess::decomposition::StlConfig::new(period),
    )
    .ok()?;
    let remainder_var = variance(&decomposition.residual)?;
    let combined: Vec<f64> = decomposition
        .residual
        .iter()
        .zip(&decomposition.seasonal)
        .map(|(r, s)| r + s)
        .collect();
    let combined_var = variance(&combined)?;
    if combined_var <= 0.0 {
        return Some(0.0);
    }
    Some((1.0 - remainder_var / combined_var).clamp(0.0, 1.0))
}

/// Sample variance, or `None` for fewer than two observations.
fn variance(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    Some(values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1.0))
}

// ── auto_arima ──────────────────────────────────────────────────────────

/// Result of automatic ARIMA model selection.
#[derive(Debug, Clone)]
pub struct AutoArimaResult {
    /// Best `(p, d, q)` order.
    pub order: (usize, usize, usize),
    /// AICc score of the best model.
    pub aic: f64,
    /// Every evaluated candidate, sorted by score ascending.
    pub candidates: Vec<(usize, usize, usize, f64)>,
    /// Number of model fits performed.
    pub evals: usize,
    /// Differencing order chosen by the unit-root test, before `max_d` was
    /// applied. Exposed so a caller can see that the cap bound the answer.
    pub kpss_order: usize,
}

/// Search strategy for [`auto_arima`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SearchStrategy {
    /// Exhaustive grid over `p ∈ [0, max_p]`, `q ∈ [0, max_q]`.
    #[default]
    Exhaustive,
    /// Hyndman–Khandakar stepwise search — O(P+Q) fits instead of O(P×Q), at
    /// the risk of a local optimum.
    Stepwise,
}

/// Budget controls for [`auto_arima`].
#[derive(Debug, Clone, Copy, Default)]
pub struct AutoArimaOptions {
    /// Hard cap on the number of model fits. `None` means unlimited.
    pub max_evals: Option<usize>,
    /// Search strategy.
    pub strategy: SearchStrategy,
}

/// Automatically select an ARIMA(p,d,q) order.
///
/// # How the order is chosen
///
/// `d` comes from a **KPSS unit-root test**
/// ([`select_differencing_order`]), capped at `max_d`; `p` and `q` are then
/// chosen by **AICc** at that fixed `d`. Splitting the decision this way is
/// not a refinement — it is what makes the information criterion meaningful,
/// because AIC/AICc compare models of the *same* data and two differencing
/// orders are two different datasets.
///
/// AICc rather than AIC: the correction matters at exactly the series lengths
/// a gateway has, and it converges to AIC as `n` grows.
///
/// # Strategies
///
/// - [`SearchStrategy::Exhaustive`] (default) fits every `(p, q)` in the grid
///   in parallel and returns the global AICc minimum within it. With `d`
///   fixed the grid is `(max_p+1)·(max_q+1)`, which is small enough that this
///   is usually the right choice.
/// - [`SearchStrategy::Stepwise`] follows Hyndman–Khandakar: seed with
///   `(2,q=2)`, `(0,0)`, `(1,0)`, `(0,1)`, then walk to improving neighbours.
///
/// Use [`AutoArimaOptions::max_evals`] to cap the cost of either.
///
/// # Errors
///
/// [`ForecastError::InsufficientData`] below 30 observations, and
/// [`ForecastError::NumericalInstability`] when no candidate fits.
pub fn auto_arima(
    timestamps: &[i64],
    values: &[f64],
    max_p: usize,
    max_d: usize,
    max_q: usize,
    options: Option<&AutoArimaOptions>,
) -> Result<AutoArimaResult, ForecastError> {
    validate_input(timestamps, values)?;
    if values.len() < 30 {
        return Err(ForecastError::InsufficientData {
            min: 30,
            got: values.len(),
        });
    }

    let kpss_order = select_differencing_order(values, max_d.max(2));
    let d = kpss_order.min(max_d);
    let opts = options.copied().unwrap_or_default();
    let max_evals = opts.max_evals.unwrap_or(usize::MAX);

    let score = |p: usize, q: usize| -> Option<f64> {
        let min_required = p.max(q) + 2 * d + 10;
        if values.len() < min_required {
            return None;
        }
        let mut model = ArimaModel::new(p, d, q);
        model.fit(timestamps, values).ok()?;
        model.aicc().filter(|a| a.is_finite())
    };

    let (order, best, mut candidates, evals) = match opts.strategy {
        SearchStrategy::Exhaustive => {
            use rayon::prelude::*;
            let grid: Vec<(usize, usize)> = (0..=max_p)
                .flat_map(|p| (0..=max_q).map(move |q| (p, q)))
                .filter(|&(p, q)| p + q > 0)
                .take(max_evals)
                .collect();
            let found: Vec<(usize, usize, usize, f64)> = grid
                .par_iter()
                .filter_map(|&(p, q)| score(p, q).map(|a| (p, d, q, a)))
                .collect();
            let evals = found.len();
            let best = found
                .iter()
                .copied()
                .min_by(|a, b| a.3.total_cmp(&b.3))
                .ok_or_else(|| {
                    ForecastError::NumericalInstability(
                        "no ARIMA model in the grid could be fitted".into(),
                    )
                })?;
            ((best.0, best.1, best.2), best.3, found, evals)
        }
        SearchStrategy::Stepwise => stepwise(max_p, max_q, d, max_evals, &score)?,
    };

    candidates.sort_by(|a, b| a.3.total_cmp(&b.3));
    Ok(AutoArimaResult {
        order,
        aic: best,
        candidates,
        evals,
        kpss_order,
    })
}

/// Hyndman–Khandakar stepwise walk over `(p, q)` at a fixed `d`.
#[allow(clippy::type_complexity)]
fn stepwise(
    max_p: usize,
    max_q: usize,
    d: usize,
    max_evals: usize,
    score: &dyn Fn(usize, usize) -> Option<f64>,
) -> Result<
    (
        (usize, usize, usize),
        f64,
        Vec<(usize, usize, usize, f64)>,
        usize,
    ),
    ForecastError,
> {
    let mut candidates: Vec<(usize, usize, usize, f64)> = Vec::new();
    let mut visited: std::collections::HashSet<(usize, usize)> = std::collections::HashSet::new();
    let mut evals = 0usize;

    let mut best_score = f64::INFINITY;
    let mut best = (0usize, 0usize);

    let mut try_model = |p: usize,
                         q: usize,
                         candidates: &mut Vec<(usize, usize, usize, f64)>,
                         evals: &mut usize|
     -> Option<f64> {
        if p > max_p || q > max_q || (p == 0 && q == 0) {
            return None;
        }
        if !visited.insert((p, q)) {
            return candidates
                .iter()
                .find(|c| c.0 == p && c.2 == q)
                .map(|c| c.3);
        }
        if *evals >= max_evals {
            return None;
        }
        let s = score(p, q)?;
        *evals += 1;
        candidates.push((p, d, q, s));
        Some(s)
    };

    for &(p, q) in &[(2, 2), (0, 0), (1, 0), (0, 1)] {
        let (p, q) = (p.min(max_p), q.min(max_q));
        if let Some(s) = try_model(p, q, &mut candidates, &mut evals) {
            if s < best_score {
                best_score = s;
                best = (p, q);
            }
        }
    }
    if best_score.is_infinite() {
        return Err(ForecastError::NumericalInstability(
            "no ARIMA model in the stepwise seed set could be fitted".into(),
        ));
    }

    const DELTAS: [(i8, i8); 8] = [
        (1, 0),
        (-1, 0),
        (0, 1),
        (0, -1),
        (1, -1),
        (-1, 1),
        (1, 1),
        (-1, -1),
    ];
    loop {
        let (cp, cq) = best;
        let mut improved = false;
        for (dp, dq) in DELTAS {
            let (Ok(np), Ok(nq)) = (
                usize::try_from(cp as i64 + i64::from(dp)),
                usize::try_from(cq as i64 + i64::from(dq)),
            ) else {
                continue;
            };
            if let Some(s) = try_model(np, nq, &mut candidates, &mut evals) {
                if s < best_score {
                    best_score = s;
                    best = (np, nq);
                    improved = true;
                }
            }
        }
        if !improved || evals >= max_evals {
            break;
        }
    }

    Ok(((best.0, d, best.1), best_score, candidates, evals))
}

impl crate::forecast::storage::ModelStore for ArimaModel {}
impl crate::forecast::storage::ModelStore for SarimaModel {}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Deterministic uniform noise on `[-amp, amp]`, so every test in this
    /// module is reproducible without a RNG dependency.
    fn noise(seed: u64, n: usize, amp: f64) -> Vec<f64> {
        let mut s = seed;
        (0..n)
            .map(|_| {
                s = s
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                ((s >> 33) as f64 / (1u64 << 31) as f64 - 0.5) * 2.0 * amp
            })
            .collect()
    }

    fn ts(n: usize) -> Vec<i64> {
        (0..n as i64).map(|i| i * 1_000_000_000).collect()
    }

    // ── Core recursions ─────────────────────────────────────────────

    #[test]
    fn difference_and_undifference_round_trip() {
        let series: Vec<f64> = (0..40).map(|i| (i as f64).powi(2) + 3.0).collect();
        for d in 1..=3 {
            let diffed = difference(&series, d);
            // Undifference the tail back to the original scale.
            let split = 30;
            let mut preds = diffed[diffed.len() - (40 - split)..].to_vec();
            let mut anchors = Vec::new();
            let mut level = series[..split].to_vec();
            for _ in 0..d {
                anchors.push(*level.last().unwrap());
                level = difference(&level, 1);
            }
            for a in anchors.into_iter().rev() {
                preds = undifference_lag(&preds, &[a], 1);
            }
            for (i, v) in preds.iter().enumerate() {
                assert!(
                    (v - series[split + i]).abs() < 1e-6,
                    "d={d} i={i}: {v} vs {}",
                    series[split + i]
                );
            }
        }
    }

    #[test]
    fn seasonal_difference_and_undifference_round_trip() {
        let m = 4;
        let series: Vec<f64> = (0..40)
            .map(|i| (i % m) as f64 * 2.0 + i as f64 * 0.1)
            .collect();
        let diffed = seasonal_difference(&series, m, 1);
        let split = 30;
        let preds = diffed[diffed.len() - (40 - split)..].to_vec();
        let anchor = &series[split - m..split];
        let restored = undifference_lag(&preds, anchor, m);
        for (i, v) in restored.iter().enumerate() {
            assert!((v - series[split + i]).abs() < 1e-9, "i={i}: {v}");
        }
    }

    #[test]
    fn expanded_polynomials_match_the_multiplicative_form() {
        // (1 - 0.5B)(1 - 0.3B⁴) = 1 - 0.5B - 0.3B⁴ + 0.15B⁵
        // In the "plus" convention that is φ* = [0.5, 0, 0, 0.3, -0.15].
        let ar = expand_ar(&[0.5], &[0.3], 4);
        let expected = [0.5, 0.0, 0.0, 0.3, -0.15];
        assert_eq!(ar.len(), expected.len());
        for (a, b) in ar.iter().zip(expected) {
            assert!((a - b).abs() < 1e-12, "{ar:?}");
        }
        // (1 + 0.4B)(1 + 0.2B³) = 1 + 0.4B + 0.2B³ + 0.08B⁴
        let ma = expand_ma(&[0.4], &[0.2], 3);
        let expected = [0.4, 0.0, 0.2, 0.08];
        for (a, b) in ma.iter().zip(expected) {
            assert!((a - b).abs() < 1e-12, "{ma:?}");
        }
    }

    #[test]
    fn expanded_polynomials_degenerate_without_seasonal_terms() {
        assert_eq!(expand_ar(&[0.5, -0.2], &[], 12), vec![0.5, -0.2]);
        assert_eq!(expand_ma(&[0.4], &[], 12), vec![0.4]);
    }

    #[test]
    fn psi_weights_degenerate_to_sqrt_h() {
        let psi = psi_weights(&[], &[], 5);
        assert_eq!(psi.len(), 5);
        assert!((psi[0] - 1.0).abs() < 1e-12);
        for (j, w) in psi.iter().enumerate().skip(1) {
            assert!(w.abs() < 1e-12, "ψ_{j} should be 0 for pure noise");
        }

        let psi_ar = psi_weights(&[0.5], &[], 4);
        for (j, expected) in [1.0, 0.5, 0.25, 0.125].into_iter().enumerate() {
            assert!((psi_ar[j] - expected).abs() < 1e-12);
        }

        let psi_ma = psi_weights(&[], &[0.8], 4);
        assert!((psi_ma[0] - 1.0).abs() < 1e-12);
        assert!((psi_ma[1] - 0.8).abs() < 1e-12);
        assert!(psi_ma[2].abs() < 1e-12);
        assert!(psi_ma[3].abs() < 1e-12);
    }

    #[test]
    fn burg_recovers_a_known_ar2_and_stays_stable() {
        let n = 1000;
        let eps = noise(12345, n, 0.25);
        let mut vals = vec![0.0; n];
        for t in 2..n {
            vals[t] = 0.75 * vals[t - 1] - 0.5 * vals[t - 2] + eps[t];
        }
        let (coeffs, _) = estimate_ar_burg(&vals[200..], 2).unwrap();
        assert!((coeffs[0] - 0.75).abs() < 0.15, "{coeffs:?}");
        assert!((coeffs[1] + 0.5).abs() < 0.15, "{coeffs:?}");
        // Roots of z² − φ₁z − φ₂ must lie inside the unit circle.
        let (b, c) = (-coeffs[0], -coeffs[1]);
        let disc = b * b - 4.0 * c;
        if disc >= 0.0 {
            let r1 = (-b + disc.sqrt()) / 2.0;
            let r2 = (-b - disc.sqrt()) / 2.0;
            assert!(
                r1.abs() < 1.0 && r2.abs() < 1.0,
                "unstable roots {r1}, {r2}"
            );
        }
    }

    // ── ARIMA ───────────────────────────────────────────────────────

    /// A random walk is `ARIMA(0,1,0)`: every ψⱼ is 1, so `Var[e_h] = h·σ²`
    /// and the variance ratio is **exactly** `h`. Before the integration term
    /// entered the ψ-weights this was a constant-width interval.
    #[test]
    fn random_walk_interval_variance_grows_exactly_linearly() {
        let e = noise(7, 200, 1.0);
        let mut vals = vec![100.0];
        for v in e.iter().take(199) {
            vals.push(vals.last().unwrap() + v);
        }
        let mut model = ArimaModel::new(0, 1, 0);
        model.fit(&ts(200), &vals).unwrap();
        let r = model.predict(12).unwrap();
        let half = |h: usize| r.confidence_upper[h] - r.values[h];
        assert!(half(0) > 0.0);
        for h in 1..=12 {
            let ratio = (half(h - 1) / half(0)).powi(2);
            assert!(
                (ratio - h as f64).abs() < 1e-9,
                "var({h})/var(1) = {ratio}, want {h}"
            );
        }
    }

    /// `ARIMA(1,1,0)`: `φ*(B) = (1-φB)(1-B)`, so
    /// `ψⱼ = 1 + φ + … + φʲ = (1-φ^{j+1})/(1-φ)`.
    #[test]
    fn arima_110_interval_matches_the_psi_closed_form() {
        let e = noise(11, 240, 1.0);
        let mut vals = vec![50.0];
        let mut prev_d = 0.0;
        for v in e.iter().take(239) {
            let d = 0.6 * prev_d + v;
            vals.push(vals.last().unwrap() + d);
            prev_d = d;
        }
        let mut model = ArimaModel::new(1, 1, 0);
        model.fit(&ts(240), &vals).unwrap();
        let ModelParams::Arima {
            ar_coeffs,
            residual_std,
            ..
        } = model.params()
        else {
            panic!()
        };
        let phi = ar_coeffs[0];
        assert!(phi.abs() > 0.1, "need a non-trivial φ, got {phi}");
        let r = model.predict(10).unwrap();
        for h in 1..=10 {
            let factor: f64 = (0..h)
                .map(|j| {
                    let psi = (1.0 - phi.powi(j as i32 + 1)) / (1.0 - phi);
                    psi * psi
                })
                .sum::<f64>();
            let expected = 1.959_963_984_540_054 * residual_std * factor.sqrt();
            let half = r.confidence_upper[h - 1] - r.values[h - 1];
            assert!(
                (half - expected).abs() < 1e-9,
                "h={h}: half-width {half} != {expected}"
            );
        }
    }

    /// `integrated_ar` must expand `φ(B)(1-B)^d(1-B^m)^D` exactly.
    #[test]
    fn integrated_ar_expands_the_differencing_operator() {
        // (1-B) → xₜ = xₜ₋₁
        assert_eq!(integrated_ar(&[], 1, 0, 0), vec![1.0]);
        // (1-B)² = 1 - 2B + B² → xₜ = 2xₜ₋₁ - xₜ₋₂
        let d2 = integrated_ar(&[], 2, 0, 0);
        assert!((d2[0] - 2.0).abs() < 1e-12 && (d2[1] + 1.0).abs() < 1e-12);
        // (1-0.5B)(1-B) = 1 - 1.5B + 0.5B²
        let m = integrated_ar(&[0.5], 1, 0, 0);
        assert!((m[0] - 1.5).abs() < 1e-12 && (m[1] + 0.5).abs() < 1e-12);
        // (1-B⁴): coefficient 1 at lag 4, zero elsewhere.
        let sd = integrated_ar(&[], 0, 1, 4);
        assert_eq!(sd.len(), 4);
        assert!((sd[3] - 1.0).abs() < 1e-12);
        assert!(sd[..3].iter().all(|c| c.abs() < 1e-12));
        // No differencing → unchanged.
        assert_eq!(integrated_ar(&[0.3, -0.2], 0, 0, 12), vec![0.3, -0.2]);
    }

    /// `arma_forecast` must read the **newest** innovations. With a residual
    /// buffer longer than the value buffer, `θ₁ = 0.5` and last innovation
    /// 5.0, the one-step forecast is exactly 2.5; truncating from the tail
    /// gave 1.5.
    #[test]
    fn arma_forecast_reads_the_newest_innovations() {
        let values = [0.0, 0.0, 0.0];
        let residuals = [1.0, 2.0, 3.0, 4.0, 5.0];
        let out = arma_forecast(&values, &residuals, &[], &[0.5], 1);
        assert!((out[0] - 2.5).abs() < 1e-12, "got {}", out[0]);
    }

    /// `predict` reads `residuals[i]` alongside `difference(history, d)[i]`,
    /// so after the streaming buffers are trimmed the two lengths must still
    /// differ by exactly `d`.
    #[test]
    fn update_keeps_history_and_residuals_aligned() {
        let vals: Vec<f64> = (0..100)
            .map(|i| 10.0 + i as f64 * 0.3 + ((i * 17) % 7) as f64 * 0.4)
            .collect();
        let mut model = ArimaModel::new(1, 1, 1);
        model.fit(&ts(100), &vals).unwrap();
        for i in 0..300 {
            let v = 40.0 + ((i * 13) % 11) as f64 * 0.5;
            model.update((100 + i) as i64 * 1_000_000_000, v).unwrap();
        }
        assert!(model.history.len() < 300, "history was never trimmed");
        assert_eq!(
            model.residuals.len(),
            model.history.len() - model.d,
            "residuals must stay aligned with difference(history, d)"
        );

        // …and the MA term of the one-step forecast must use the newest
        // innovation: ŷ = last + c + θ₁·e_last for ARIMA(p,1,1) with p's
        // contribution written out from the differenced history.
        let ModelParams::Arima {
            ar_coeffs,
            ma_coeffs,
            constant,
            ..
        } = model.params().clone()
        else {
            panic!()
        };
        let diffed: Vec<f64> = difference(&model.history, 1)
            .into_iter()
            .map(|v| v - constant)
            .collect();
        let n = diffed.len();
        let expected_diff = constant
            + ar_coeffs[0] * diffed[n - 1]
            + ma_coeffs[0] * model.residuals[model.residuals.len() - 1];
        let expected = model.history[model.history.len() - 1] + expected_diff;
        let got = model.predict(1).unwrap().values[0];
        assert!((got - expected).abs() < 1e-9, "{got} != {expected}");
    }

    /// The same alignment invariant for SARIMA.
    #[test]
    fn sarima_update_keeps_history_and_residuals_aligned() {
        let vals = {
            (0..120)
                .map(|i| 50.0 + 5.0 * (std::f64::consts::TAU * (i % 12) as f64 / 12.0).sin())
                .collect::<Vec<f64>>()
        };
        let mut model = SarimaModel::new(1, 1, 1, 0, 1, 0, 12);
        model.fit(&ts(120), &vals).unwrap();
        for i in 0..400 {
            let v = 50.0 + 5.0 * (std::f64::consts::TAU * (i % 12) as f64 / 12.0).sin();
            model.update((120 + i) as i64 * 1_000_000_000, v).unwrap();
        }
        assert!(model.history.len() < 400, "history was never trimmed");
        assert_eq!(model.residuals.len(), model.centered(&model.history).len());
    }

    #[test]
    fn arima_random_walk() {
        let e = noise(42, 200, 1.0);
        let mut vals = vec![100.0];
        for v in e.iter().take(199) {
            vals.push(vals.last().unwrap() + v);
        }
        let mut model = ArimaModel::new(1, 1, 0);
        model.fit(&ts(200), &vals).unwrap();
        let result = model.predict(10).unwrap();
        assert_eq!(result.values.len(), 10);
        assert!((result.values[0] - vals[199]).abs() < 10.0);
    }

    #[test]
    fn arima_linear_trend() {
        let vals: Vec<f64> = (0..100).map(|i| f64::from(i) * 2.0 + 10.0).collect();
        let mut model = ArimaModel::new(0, 1, 0);
        model.fit(&ts(100), &vals).unwrap();
        let result = model.predict(5).unwrap();
        // d = 1, so the model carries a drift: the mean first difference is
        // exactly 2, and the forecast continues the line.
        for (i, v) in result.values.iter().enumerate() {
            let expected = 208.0 + (i + 1) as f64 * 2.0;
            assert!((v - expected).abs() < 1e-6, "h={i}: {v} vs {expected}");
        }
    }

    /// Above one difference there is no constant, so `ARIMA(0,2,0)` is the
    /// pure `2x_{t-1} - x_{t-2}` extrapolation — exact on a line, and short of
    /// a quadratic by its (unmodelled) curvature. Both are the right answer
    /// for the model that was asked for.
    #[test]
    fn arima_d2_extrapolates_a_line_exactly() {
        let n = 60usize;
        let vals: Vec<f64> = (0..n).map(|i| 3.0 * i as f64 - 7.0).collect();
        let mut model = ArimaModel::new(0, 2, 0);
        model.fit(&ts(n), &vals).unwrap();
        let result = model.predict(5).unwrap();
        for (h, v) in result.values.iter().enumerate() {
            let expected = 3.0 * (n + h) as f64 - 7.0;
            assert!((v - expected).abs() < 1e-6, "h={h}: {v} vs {expected}");
        }
    }

    #[test]
    fn arima_d2_undifference_correctness() {
        let n = 100usize;
        let vals: Vec<f64> = (0..n).map(|i| (i as f64).powi(2)).collect();
        let mut model = ArimaModel::new(0, 2, 0);
        model.fit(&ts(n), &vals).unwrap();
        let result = model.predict(5).unwrap();
        // No constant at d = 2, so the forecast is the linear continuation of
        // the last first difference: x_{n+h} = x_{n-1} + (h+1)·(x_{n-1} −
        // x_{n-2}). It falls short of the true quadratic by the curvature the
        // model does not carry, and it must do so *exactly*.
        let slope = vals[n - 1] - vals[n - 2];
        for (h, v) in result.values.iter().enumerate() {
            let expected = vals[n - 1] + (h + 1) as f64 * slope;
            assert!((v - expected).abs() < 1e-6, "h={h}: {v} vs {expected}");
        }
    }

    #[test]
    fn arima_confidence_intervals_bracket_the_forecast() {
        let vals: Vec<f64> = (0..100)
            .map(|i| (f64::from(i)).sin() * 5.0 + 50.0)
            .collect();
        let mut model = ArimaModel::new(2, 0, 1);
        model.fit(&ts(100), &vals).unwrap();
        let result = model.predict(10).unwrap();
        for i in 0..10 {
            assert!(result.confidence_lower[i] <= result.values[i]);
            assert!(result.confidence_upper[i] >= result.values[i]);
        }
    }

    /// The conditioning warm-up is the whole reason ARIMA(p,0,q) intervals are
    /// usable: without it the first innovation is the raw first observation,
    /// so a series sitting at 100 with noise of 0.1 gets an interval two
    /// orders of magnitude too wide.
    #[test]
    fn arima_intervals_track_the_noise_not_the_level() {
        let n = 200;
        let e = noise(7, n, 0.1);
        let mut vals = Vec::with_capacity(n);
        let mut x = 100.0_f64;
        for v in &e {
            x = 100.0 + 0.5 * (x - 100.0) + v;
            vals.push(x);
        }
        let mut model = ArimaModel::new(1, 0, 0);
        model.fit(&ts(n), &vals).unwrap();
        let r = model.predict(3).unwrap();
        let half_width = r.confidence_upper[0] - r.values[0];
        assert!(
            half_width < 0.5,
            "one-step 95% half-width {half_width} exceeds the noise scale (0.1)"
        );
        assert!(
            half_width > 0.05,
            "interval collapsed to nothing: {half_width}"
        );
    }

    #[test]
    fn arima_insufficient_data() {
        let mut model = ArimaModel::new(2, 1, 1);
        assert!(model.fit(&[0, 1, 2], &[1.0, 2.0, 3.0]).is_err());
    }

    #[test]
    fn arima_update() {
        let vals: Vec<f64> = (0..50).map(f64::from).collect();
        let mut model = ArimaModel::new(1, 1, 0);
        model.fit(&ts(50), &vals).unwrap();
        assert!(model.update(50_000_000_000, 50.0).is_ok());
        assert!(model.predict(3).unwrap().values.len() == 3);
    }

    #[test]
    fn css_recovers_a_known_arma11() {
        let n = 1000;
        let eps = noise(99999, n, 0.2);
        let mut vals = vec![0.0; n];
        for t in 1..n {
            vals[t] = 0.6 * vals[t - 1] + eps[t] + 0.4 * eps[t - 1];
        }
        let vals = &vals[200..];
        let mut model = ArimaModel::new(1, 0, 1);
        model.fit(&ts(vals.len()), vals).unwrap();
        let ModelParams::Arima {
            ma_coeffs,
            ar_coeffs,
            ..
        } = model.params()
        else {
            panic!("wrong variant");
        };
        assert!((ma_coeffs[0] - 0.4).abs() < 0.3, "θ = {}", ma_coeffs[0]);
        assert!((ar_coeffs[0] - 0.6).abs() < 0.2, "φ = {}", ar_coeffs[0]);
    }

    // ── KPSS and differencing order ─────────────────────────────────

    #[test]
    fn kpss_accepts_stationary_and_rejects_a_random_walk() {
        let white = noise(3, 300, 1.0);
        assert!(
            kpss_statistic(&white) < KPSS_CRITICAL_5PCT,
            "white noise rejected: {}",
            kpss_statistic(&white)
        );

        let mut walk = vec![0.0];
        for v in white.iter().take(299) {
            walk.push(walk.last().unwrap() + v);
        }
        assert!(
            kpss_statistic(&walk) > KPSS_CRITICAL_5PCT,
            "random walk accepted: {}",
            kpss_statistic(&walk)
        );
        assert_eq!(select_differencing_order(&white, 2), 0);
        assert_eq!(select_differencing_order(&walk, 2), 1);
    }

    #[test]
    fn kpss_is_scale_invariant() {
        let v = noise(11, 200, 1.0);
        let scaled: Vec<f64> = v.iter().map(|x| x * 1000.0 + 5.0).collect();
        let a = kpss_statistic(&v);
        let b = kpss_statistic(&scaled);
        assert!((a - b).abs() < 1e-9, "{a} vs {b}");
    }

    #[test]
    fn kpss_handles_degenerate_input() {
        assert_eq!(kpss_statistic(&[]), 0.0);
        assert_eq!(kpss_statistic(&[1.0, 1.0]), 0.0);
        assert_eq!(kpss_statistic(&[7.0; 50]), 0.0);
        assert_eq!(select_differencing_order(&[7.0; 50], 2), 0);
    }

    /// Strong seasonality takes a seasonal difference.
    ///
    /// This is the M3/M4-tuned behaviour of the measure, and it is worth
    /// being explicit that it is a **strength** test, not a unit-root test:
    /// a perfectly repeating pattern around a stable level scores near 1 and
    /// is differenced. R's `nsdiffs` does the same, deliberately — the
    /// threshold was fitted by minimising forecast error, not derived from
    /// stationarity theory.
    #[test]
    fn strong_seasonality_takes_a_seasonal_difference() {
        let m = 24usize;
        let values: Vec<f64> = (0..(m * 14))
            .map(|i| {
                let phase = (i % m) as f64 / m as f64 * std::f64::consts::TAU;
                100.0 + 20.0 * phase.sin() + ((i * 37) % 11) as f64 * 0.05
            })
            .collect();
        assert_eq!(select_seasonal_differencing_order(&values, m, 1), 1);
    }

    /// Faint seasonality buried in noise takes none — and this is the case
    /// the fixed `D = 1` got wrong.
    ///
    /// `detect_period` will happily return a period for a series whose
    /// seasonal term explains almost none of its variance. Differencing it
    /// anyway spends `m` observations, adds a moving-average term the data
    /// does not support, and widens every interval built on the result.
    #[test]
    fn weak_seasonality_takes_no_seasonal_difference() {
        let m = 24usize;
        let values: Vec<f64> = (0..(m * 14))
            .map(|i| {
                let phase = (i % m) as f64 / m as f64 * std::f64::consts::TAU;
                let noise = (((i * 2_654_435_761usize) % 1000) as f64 / 1000.0 - 0.5) * 40.0;
                100.0 + 0.5 * phase.sin() + noise
            })
            .collect();
        assert_eq!(select_seasonal_differencing_order(&values, m, 1), 0);
    }

    #[test]
    fn pure_noise_takes_no_seasonal_difference() {
        let m = 24usize;
        let values: Vec<f64> = (0..(m * 14))
            .map(|i| 100.0 + (((i * 2_654_435_761usize) % 1000) as f64 / 1000.0 - 0.5) * 40.0)
            .collect();
        assert_eq!(select_seasonal_differencing_order(&values, m, 0), 0);
        assert_eq!(select_seasonal_differencing_order(&values, m, 1), 0);
    }

    /// The measure's blind spot, pinned rather than left to be rediscovered.
    ///
    /// A **seasonal random walk** — each season's level drifts from cycle to
    /// cycle — is the textbook case for a seasonal difference, and the
    /// strength heuristic scores it *low*: STL's cycle-subseries smoother
    /// cannot fit a seasonal shape that keeps moving, so the variation lands
    /// in the remainder and the ratio collapses. This is exactly what a real
    /// seasonal unit-root test (OCSB, Canova–Hansen) is for, and why R keeps
    /// them as options beside this default. Recorded as a test so the limit
    /// is a known quantity rather than a surprise.
    /// Smallest |root| of `1 + θ₁z + … + θ_qz^q`, by Durand–Kerner.
    ///
    /// The MA polynomial is invertible iff every root lies strictly outside
    /// the unit circle, so the answer is compared against 1.
    fn min_ma_root_modulus(theta: &[f64]) -> f64 {
        let q = theta.len();
        if q == 0 {
            return f64::INFINITY;
        }
        let mut c: Vec<(f64, f64)> = std::iter::once(1.0)
            .chain(theta.iter().copied())
            .map(|v| (v, 0.0))
            .collect();
        let lead = c[q].0;
        if lead.abs() < 1e-12 {
            return min_ma_root_modulus(&theta[..q - 1]);
        }
        for v in &mut c {
            v.0 /= lead;
        }
        let mul = |a: (f64, f64), b: (f64, f64)| (a.0 * b.0 - a.1 * b.1, a.0 * b.1 + a.1 * b.0);
        let sub = |a: (f64, f64), b: (f64, f64)| (a.0 - b.0, a.1 - b.1);
        let div = |a: (f64, f64), b: (f64, f64)| {
            let d = b.0 * b.0 + b.1 * b.1;
            ((a.0 * b.0 + a.1 * b.1) / d, (a.1 * b.0 - a.0 * b.1) / d)
        };
        let eval = |z: (f64, f64)| {
            let mut acc = (0.0, 0.0);
            for k in (0..=q).rev() {
                acc = mul(acc, z);
                acc = (acc.0 + c[k].0, acc.1 + c[k].1);
            }
            acc
        };
        let mut roots: Vec<(f64, f64)> = (0..q)
            .map(|k| {
                let a = 0.4 + 0.9 * k as f64;
                (a.cos() * 0.9, a.sin() * 0.9)
            })
            .collect();
        for _ in 0..500 {
            let mut moved = 0.0f64;
            for i in 0..q {
                let mut denom = (1.0, 0.0);
                for j in 0..q {
                    if i != j {
                        denom = mul(denom, sub(roots[i], roots[j]));
                    }
                }
                if denom.0.abs() + denom.1.abs() < 1e-300 {
                    continue;
                }
                let delta = div(eval(roots[i]), denom);
                roots[i] = sub(roots[i], delta);
                moved = moved.max(delta.0.abs() + delta.1.abs());
            }
            if moved < 1e-14 {
                break;
            }
        }
        roots
            .iter()
            .map(|r| (r.0 * r.0 + r.1 * r.1).sqrt())
            .fold(f64::INFINITY, f64::min)
    }

    #[test]
    fn the_root_finder_agrees_with_the_quadratic_formula() {
        // `1 + 0.99z - 0.99z²` has a root at ≈ -0.6225: inside the unit
        // circle, so this θ is non-invertible *and* inside the ±0.99 box
        // every coefficient is bounded to. The box is not the constraint.
        let m = min_ma_root_modulus(&[0.99, -0.99]);
        assert!((m - 0.6225).abs() < 1e-3, "min |root| was {m}");
        // An invertible one, for the other direction.
        assert!(min_ma_root_modulus(&[0.5]) > 1.0);
    }

    #[test]
    fn a_fitted_ma_polynomial_is_invertible() {
        // `estimate_ma` bounds each coefficient to ±0.99, which does not
        // imply invertibility for q > 1 — see the test above. What keeps
        // fits inside the invertible region is the **objective**: the CSS
        // error recursion diverges outside it, so the optimiser has no
        // reason to go there. That is a property of the estimator, not of
        // the bounds, so it is worth checking rather than assuming — if the
        // estimator is ever replaced (an exact likelihood, say), this is the
        // guarantee that quietly changes.
        let mut state = 7u64;
        let mut next = move || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            ((state >> 33) as f64 / f64::from(u32::MAX >> 1)) - 1.0
        };

        let mut checked = 0usize;
        let mut worst = f64::INFINITY;
        for q in 2..=3usize {
            for trial in 0..40 {
                let n = 160usize;
                let mut v = Vec::with_capacity(n);
                let (mut prev0, mut prev1, mut walk) = (0.0f64, 0.0f64, 0.0f64);
                for i in 0..n {
                    let e = next();
                    v.push(match trial % 4 {
                        0 => e,
                        1 => e + 0.95 * prev0 + 0.9 * prev1,
                        2 => {
                            walk += e;
                            walk
                        }
                        _ => i as f64 * 0.05 + e,
                    });
                    prev1 = prev0;
                    prev0 = e;
                }
                let ts: Vec<i64> = (0..n as i64).collect();
                let mut model = ArimaModel::new(1, 0, q);
                if model.fit(&ts, &v).is_err() {
                    continue;
                }
                let ModelParams::Arima { ma_coeffs, .. } = model.params() else {
                    continue;
                };
                let r = min_ma_root_modulus(ma_coeffs);
                worst = worst.min(r);
                assert!(
                    r > 1.0,
                    "q={q} trial={trial}: fitted θ={ma_coeffs:?} is non-invertible \
                     (min |root| = {r})"
                );
                checked += 1;
            }
        }
        assert!(checked >= 60, "only {checked} fits were checked");
        // Over-differenced noise is the textbook way to manufacture a
        // non-invertible MA(1), so it belongs in the sample rather than
        // beside it.
        let raw: Vec<f64> = (0..121).map(|_| next()).collect();
        let over: Vec<f64> = raw.windows(2).map(|w| w[1] - w[0]).collect();
        let ts: Vec<i64> = (0..over.len() as i64).collect();
        let mut model = ArimaModel::new(1, 0, 2);
        if model.fit(&ts, &over).is_ok() {
            if let ModelParams::Arima { ma_coeffs, .. } = model.params() {
                let r = min_ma_root_modulus(ma_coeffs);
                assert!(r > 1.0, "over-differenced fit is non-invertible: {r}");
            }
        }
    }

    #[test]
    fn the_strength_measure_is_blind_to_a_stochastic_seasonal_level() {
        let m = 24usize;
        let mut values = vec![0.0f64; 12 * m];
        let mut season_level = vec![0.0f64; m];
        let mut rng_state = 42u64;
        let mut next = || {
            rng_state = rng_state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            ((rng_state >> 33) as f64 / f64::from(u32::MAX >> 1)) - 1.0
        };
        for cycle in 0..12 {
            for season in 0..m {
                season_level[season] += next() * 3.0;
                values[cycle * m + season] = season_level[season] + next() * 0.1;
            }
        }
        assert_eq!(
            select_seasonal_differencing_order(&values, m, 1),
            0,
            "if this starts returning 1 the measure has changed — check which \
             test is being used before assuming it is an improvement",
        );
    }

    #[test]
    fn a_constant_series_is_never_seasonally_differenced() {
        // Both variances are zero, so the ratio says nothing; the guard has
        // to come first. R's `nsdiffs` checks `is.constant` before the
        // heuristic for exactly this reason.
        let values = vec![7.0f64; 200];
        assert_eq!(select_seasonal_differencing_order(&values, 24, 1), 0);
    }

    #[test]
    fn a_series_shorter_than_two_periods_is_never_seasonally_differenced() {
        let values: Vec<f64> = (0..30).map(|i| f64::from(i % 24)).collect();
        assert_eq!(
            select_seasonal_differencing_order(&values, 24, 1),
            0,
            "STL needs two full periods; there is nothing to measure",
        );
    }

    #[test]
    fn a_period_below_two_is_not_seasonal() {
        let values: Vec<f64> = (0..100).map(f64::from).collect();
        assert_eq!(select_seasonal_differencing_order(&values, 1, 1), 0);
        assert_eq!(select_seasonal_differencing_order(&values, 0, 1), 0);
    }

    #[test]
    fn select_differencing_order_respects_the_cap() {
        let mut quad: Vec<f64> = (0..200).map(|i| (f64::from(i)).powi(2)).collect();
        quad.iter_mut()
            .zip(noise(5, 200, 1.0))
            .for_each(|(v, e)| *v += e);
        assert_eq!(select_differencing_order(&quad, 1), 1);
        assert!(select_differencing_order(&quad, 3) >= 2);
    }

    // ── auto_arima ──────────────────────────────────────────────────

    #[test]
    fn auto_arima_differences_a_trend() {
        let vals: Vec<f64> = (0..100).map(|i| f64::from(i) * 2.0 + 10.0).collect();
        let result = auto_arima(&ts(100), &vals, 2, 2, 2, None).unwrap();
        assert!(result.order.1 >= 1, "d should be >= 1 for trending data");
        assert!(!result.candidates.is_empty());
        assert!(result.aic.is_finite());
        assert!(result.evals > 0);
    }

    /// Why `d` comes from KPSS: scoring it by an information criterion picks
    /// `d = 1` on stationary noise, because differencing shrinks the residual
    /// sum of squares the criterion is comparing.
    #[test]
    fn auto_arima_does_not_overdifference_stationary_noise() {
        let vals: Vec<f64> = noise(2024, 200, 0.5).iter().map(|v| v + 10.0).collect();
        let result = auto_arima(&ts(200), &vals, 2, 2, 2, None).unwrap();
        assert_eq!(result.order.1, 0, "stationary series was differenced");
        assert_eq!(result.kpss_order, 0);
    }

    #[test]
    fn auto_arima_reports_when_max_d_bound_the_answer() {
        let vals: Vec<f64> = (0..200)
            .map(|i| (f64::from(i)).powi(2))
            .zip(noise(6, 200, 1.0))
            .map(|(a, b)| a + b)
            .collect();
        let result = auto_arima(&ts(200), &vals, 1, 1, 1, None).unwrap();
        assert_eq!(result.order.1, 1, "capped at max_d");
        assert!(
            result.kpss_order >= 2,
            "kpss wanted more: {}",
            result.kpss_order
        );
    }

    #[test]
    fn auto_arima_returns_sorted_candidates_at_one_differencing_order() {
        let vals: Vec<f64> = (0..80)
            .map(|i| (f64::from(i) * 0.1).sin() * 5.0 + 50.0)
            .collect();
        let result = auto_arima(&ts(80), &vals, 2, 1, 2, None).unwrap();
        for w in result.candidates.windows(2) {
            assert!(w[0].3 <= w[1].3, "candidates not sorted");
        }
        assert!(
            result.candidates.iter().all(|c| c.1 == result.order.1),
            "candidates must share one differencing order to be comparable"
        );
    }

    #[test]
    fn auto_arima_insufficient_data() {
        let vals: Vec<f64> = (0..10).map(f64::from).collect();
        assert!(auto_arima(&ts(10), &vals, 3, 2, 3, None).is_err());
    }

    #[test]
    fn auto_arima_max_evals_budget() {
        let vals: Vec<f64> = (0..100).map(|i| f64::from(i) * 2.0 + 10.0).collect();
        let opts = AutoArimaOptions {
            max_evals: Some(3),
            ..Default::default()
        };
        let result = auto_arima(&ts(100), &vals, 3, 2, 3, Some(&opts)).unwrap();
        assert!(result.evals <= 3, "got {} evals", result.evals);
        assert!(result.aic.is_finite());
    }

    #[test]
    fn auto_arima_stepwise_agrees_with_exhaustive_on_a_small_grid() {
        let vals: Vec<f64> = (0..100).map(|i| f64::from(i) * 2.0 + 10.0).collect();
        let exhaustive = auto_arima(&ts(100), &vals, 2, 1, 2, None).unwrap();
        let opts = AutoArimaOptions {
            strategy: SearchStrategy::Stepwise,
            ..Default::default()
        };
        let stepwise = auto_arima(&ts(100), &vals, 2, 1, 2, Some(&opts)).unwrap();
        let denom = exhaustive.aic.abs().max(1e-6);
        assert!(
            (stepwise.aic - exhaustive.aic).abs() / denom < 0.10,
            "stepwise {} vs exhaustive {}",
            stepwise.aic,
            exhaustive.aic
        );
    }

    #[test]
    fn auto_arima_stepwise_uses_fewer_fits() {
        // Noisy, or every candidate fits exactly, RSS is zero and no
        // information criterion is defined for any of them.
        let vals: Vec<f64> = (0..150)
            .map(|i| f64::from(i) * 2.0 + 10.0)
            .zip(noise(31, 150, 1.0))
            .map(|(a, b)| a + b)
            .collect();
        let exhaustive = auto_arima(&ts(150), &vals, 5, 2, 5, None).unwrap();
        assert!(
            exhaustive.evals > 10,
            "grid collapsed: {}",
            exhaustive.evals
        );
        let opts = AutoArimaOptions {
            strategy: SearchStrategy::Stepwise,
            ..Default::default()
        };
        let stepwise = auto_arima(&ts(150), &vals, 5, 2, 5, Some(&opts)).unwrap();
        assert!(
            stepwise.evals < exhaustive.evals,
            "stepwise {} vs exhaustive {}",
            stepwise.evals,
            exhaustive.evals
        );
    }

    // ── SARIMA ──────────────────────────────────────────────────────

    fn seasonal(n: usize, m: usize, amp: f64, level: f64) -> Vec<f64> {
        (0..n)
            .map(|i| level + amp * (2.0 * std::f64::consts::PI * (i % m) as f64 / m as f64).sin())
            .collect()
    }

    #[test]
    fn sarima_seasonal_orders_change_the_forecast() {
        let vals = seasonal(240, 12, 5.0, 50.0);
        let mut plain = SarimaModel::new(1, 0, 0, 0, 0, 0, 12);
        plain.fit(&ts(240), &vals).unwrap();
        let mut seas = SarimaModel::new(1, 0, 0, 1, 0, 1, 12);
        seas.fit(&ts(240), &vals).unwrap();
        let a = plain.predict(12).unwrap();
        let b = seas.predict(12).unwrap();
        assert_ne!(
            a.values, b.values,
            "seasonal orders must reach the forecast recursion"
        );
    }

    /// A seasonal model must beat a non-seasonal one on seasonal data — the
    /// property that fails the moment the seasonal coefficients stop reaching
    /// the forecast recursion.
    #[test]
    fn sarima_forecasts_the_season() {
        let m = 12;
        let n = 240;
        let vals = seasonal(n, m, 5.0, 50.0);
        let truth = seasonal(n + m, m, 5.0, 50.0);
        let expected = &truth[n..];

        let mut seas = SarimaModel::new(0, 0, 0, 1, 0, 1, m);
        seas.fit(&ts(n), &vals).unwrap();
        let got = seas.predict(m).unwrap();

        let mut plain = ArimaModel::new(1, 0, 0);
        plain.fit(&ts(n), &vals).unwrap();
        let baseline = plain.predict(m).unwrap();

        let err = |p: &[f64]| -> f64 {
            p.iter()
                .zip(expected)
                .map(|(a, b)| (a - b).abs())
                .sum::<f64>()
                / m as f64
        };
        let seasonal_err = err(&got.values);
        let baseline_err = err(&baseline.values);
        assert!(
            seasonal_err < baseline_err * 0.5,
            "SARIMA MAE {seasonal_err:.4} should beat ARIMA MAE {baseline_err:.4}"
        );
    }

    #[test]
    fn sarima_seasonal_differencing_round_trips_the_level() {
        // A pure repeating pattern with a linear drift: (0,0,0)(0,1,0)m
        // forecasts x_{t} = x_{t-m} + drift, which reproduces the pattern.
        let m = 12;
        let n = 240;
        let vals: Vec<f64> = (0..n)
            .map(|i| 50.0 + 5.0 * ((i % m) as f64) + 0.25 * i as f64)
            .collect();
        let mut model = SarimaModel::new(0, 0, 0, 0, 1, 0, m);
        model.fit(&ts(n), &vals).unwrap();
        let r = model.predict(m).unwrap();
        for (h, v) in r.values.iter().enumerate() {
            let expected = 50.0 + 5.0 * ((n + h) % m) as f64 + 0.25 * (n + h) as f64;
            assert!((v - expected).abs() < 1.0, "h={h}: {v} vs {expected}");
        }
    }

    #[test]
    fn sarima_ignores_seasonal_orders_when_the_period_is_degenerate() {
        let vals: Vec<f64> = noise(8, 120, 1.0).iter().map(|v| v + 5.0).collect();
        let mut model = SarimaModel::new(1, 0, 0, 2, 1, 2, 1);
        model.fit(&ts(120), &vals).unwrap();
        let ModelParams::Sarima { sp, sd, sq, .. } = model.params() else {
            panic!("wrong variant");
        };
        assert_eq!((*sp, *sd, *sq), (0, 0, 0));
    }

    #[test]
    fn sarima_insufficient_data() {
        let vals = seasonal(20, 12, 5.0, 50.0);
        let mut model = SarimaModel::new(1, 0, 0, 1, 1, 0, 12);
        assert!(model.fit(&ts(20), &vals).is_err());
    }

    #[test]
    fn sarima_update_advances_the_forecast() {
        let vals = seasonal(240, 12, 5.0, 50.0);
        let mut model = SarimaModel::new(1, 0, 0, 1, 0, 0, 12);
        model.fit(&ts(240), &vals).unwrap();
        let before = model.predict(1).unwrap();
        model.update(240_000_000_000, 60.0).unwrap();
        let after = model.predict(1).unwrap();
        assert_ne!(before.values[0], after.values[0]);
        assert!(after.timestamps[0] > before.timestamps[0]);
    }

    #[test]
    fn sarima_intervals_widen_with_horizon() {
        let vals = seasonal(240, 12, 5.0, 50.0);
        let mut model = SarimaModel::new(1, 0, 1, 1, 0, 1, 12);
        model.fit(&ts(240), &vals).unwrap();
        let r = model.predict(24).unwrap();
        let width = |h: usize| r.confidence_upper[h] - r.confidence_lower[h];
        // Strict growth, with the ψ-weight closed form: `>=` also passes for
        // a constant-width interval, which is exactly the bug it missed.
        for h in 1..24 {
            assert!(
                width(h) > width(h - 1),
                "width must grow strictly: h={h} {} vs {}",
                width(h),
                width(h - 1)
            );
        }
        let ModelParams::Sarima { residual_std, .. } = model.params() else {
            panic!()
        };
        let phi_star = integrated_ar(&model.ar_expanded, 0, 0, 12);
        let psi = psi_weights(&phi_star, &model.ma_expanded, 24);
        for h in 1..=24 {
            let scale: f64 = psi[..h].iter().map(|w| w * w).sum::<f64>().sqrt();
            let expected = 2.0 * 1.959_963_984_540_054 * residual_std * scale;
            assert!(
                (width(h - 1) - expected).abs() < 1e-9,
                "h={h}: width {} != {expected}",
                width(h - 1)
            );
        }
        for h in 0..24 {
            assert!(r.confidence_lower[h] <= r.values[h]);
            assert!(r.confidence_upper[h] >= r.values[h]);
        }
    }
}
