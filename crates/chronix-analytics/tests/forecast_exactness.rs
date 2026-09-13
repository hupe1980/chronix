#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Analytic invariants the forecast models must satisfy exactly.
//!
//! Statistical code is easy to get subtly wrong and hard to notice: an
//! off-by-one in a seasonal index or a trend term that is applied once too
//! often still produces plausible-looking numbers. These tests use inputs
//! whose correct forecast is known in closed form, so any such error shows up
//! as a hard failure rather than a slightly worse MAPE.

use chronix_analytics::forecast::{
    ForecastModel, HoltLinearModel, HoltWintersModel, LinearRegressionModel, SesModel,
};

const SEC: i64 = 1_000_000_000;

fn timestamps(n: usize) -> Vec<i64> {
    (0..n as i64).map(|i| i * SEC).collect()
}

fn assert_close(actual: f64, expected: f64, tol: f64, what: &str) {
    assert!(
        (actual - expected).abs() <= tol,
        "{what}: got {actual}, expected {expected} (tolerance {tol})"
    );
}

// ── Simple exponential smoothing ───────────────────────────────────────

/// With `alpha = 1` SES has no memory: the forecast is the last observation,
/// flat across the whole horizon.
#[test]
fn ses_alpha_one_forecasts_the_last_observation() {
    let values: Vec<f64> = vec![5.0, 9.0, 2.0, 7.0, 42.0];
    let mut m = SesModel::new(Some(1.0));
    m.fit(&timestamps(values.len()), &values).unwrap();
    let f = m.predict(3).unwrap();
    for (i, v) in f.values.iter().enumerate() {
        assert_close(*v, 42.0, 1e-9, &format!("ses alpha=1 step {i}"));
    }
}

/// On a constant series every smoothing constant gives the same constant.
#[test]
fn ses_on_a_constant_series_is_that_constant() {
    let values = vec![7.5; 40];
    for alpha in [0.1, 0.5, 0.9] {
        let mut m = SesModel::new(Some(alpha));
        m.fit(&timestamps(values.len()), &values).unwrap();
        let f = m.predict(4).unwrap();
        for v in &f.values {
            assert_close(*v, 7.5, 1e-9, &format!("ses alpha={alpha}"));
        }
    }
}

// ── Holt's linear trend ────────────────────────────────────────────────

/// A perfect straight line must be extrapolated exactly: forecast at step `h`
/// is `last + h * slope`. This is the invariant that catches a trend term
/// applied the wrong number of times.
#[test]
fn holt_extrapolates_a_perfect_line_exactly() {
    let slope = 3.0;
    let intercept = 10.0;
    let n = 60;
    let values: Vec<f64> = (0..n).map(|i| intercept + slope * f64::from(i)).collect();

    let mut m = HoltLinearModel::new(Some(0.8), Some(0.8), 1.0);
    m.fit(&timestamps(values.len()), &values).unwrap();
    let f = m.predict(5).unwrap();

    let last = intercept + slope * f64::from(n - 1);
    for (i, v) in f.values.iter().enumerate() {
        let expected = last + slope * (i as f64 + 1.0);
        assert_close(*v, expected, 1e-6, &format!("holt step {}", i + 1));
    }
}

/// A damped model must not overshoot the undamped one on a rising line, and
/// must still be increasing.
#[test]
fn damped_holt_stays_below_the_undamped_forecast() {
    let values: Vec<f64> = (0..60).map(|i| 10.0 + 3.0 * f64::from(i)).collect();
    let ts = timestamps(values.len());

    let mut undamped = HoltLinearModel::new(Some(0.8), Some(0.8), 1.0);
    undamped.fit(&ts, &values).unwrap();
    let mut damped = HoltLinearModel::new(Some(0.8), Some(0.8), 0.8);
    damped.fit(&ts, &values).unwrap();

    let u = undamped.predict(10).unwrap();
    let d = damped.predict(10).unwrap();
    for h in 0..10 {
        assert!(
            d.values[h] <= u.values[h] + 1e-9,
            "damped step {h} ({}) exceeded undamped ({})",
            d.values[h],
            u.values[h]
        );
    }
    assert!(
        d.values[9] > d.values[0],
        "damped forecast should still rise on a rising series"
    );
}

// ── Holt-Winters ───────────────────────────────────────────────────────

/// Additive Holt-Winters on a pure seasonal pattern (no trend, no noise) must
/// reproduce the pattern exactly, in phase. Phase errors are the classic
/// Holt-Winters bug and are invisible to an aggregate error metric.
#[test]
fn holt_winters_additive_reproduces_a_pure_season_in_phase() {
    let period = 4;
    let season = [10.0, 20.0, 15.0, 5.0];
    let n = period * 20;
    let values: Vec<f64> = (0..n).map(|i| season[i % period]).collect();

    let mut m = HoltWintersModel::new(Some(0.3), Some(0.05), Some(0.3), Some(period), false);
    m.fit(&timestamps(values.len()), &values).unwrap();
    let f = m.predict(period * 2).unwrap();

    for (h, v) in f.values.iter().enumerate() {
        let expected = season[(n + h) % period];
        assert_close(*v, expected, 0.5, &format!("HW additive step {}", h + 1));
    }
}

/// The same with a linear trend on top: the forecast must carry both the
/// season and the trend.
#[test]
fn holt_winters_additive_handles_season_plus_trend() {
    let period = 4;
    let season = [0.0, 10.0, 5.0, -5.0];
    let slope = 2.0;
    let n = period * 25;
    let values: Vec<f64> = (0..n)
        .map(|i| 100.0 + slope * i as f64 + season[i % period])
        .collect();

    let mut m = HoltWintersModel::new(Some(0.3), Some(0.1), Some(0.3), Some(period), false);
    m.fit(&timestamps(values.len()), &values).unwrap();
    let f = m.predict(period).unwrap();

    for (h, v) in f.values.iter().enumerate() {
        let i = n + h;
        let expected = 100.0 + slope * i as f64 + season[i % period];
        assert_close(
            *v,
            expected,
            2.0,
            &format!("HW season+trend step {}", h + 1),
        );
    }
}

/// Multiplicative Holt-Winters on a multiplicative pattern.
#[test]
fn holt_winters_multiplicative_reproduces_a_scaled_season() {
    let period = 4;
    let factors = [1.0, 1.5, 0.8, 0.7];
    let n = period * 25;
    let values: Vec<f64> = (0..n).map(|i| 100.0 * factors[i % period]).collect();

    let mut m = HoltWintersModel::new(Some(0.3), Some(0.05), Some(0.3), Some(period), true);
    m.fit(&timestamps(values.len()), &values).unwrap();
    let f = m.predict(period).unwrap();

    for (h, v) in f.values.iter().enumerate() {
        let expected = 100.0 * factors[(n + h) % period];
        assert_close(*v, expected, 5.0, &format!("HW mult step {}", h + 1));
    }
}

// ── Linear regression ──────────────────────────────────────────────────

/// Ordinary least squares on exactly collinear points must recover the line
/// to floating-point precision.
#[test]
fn linear_regression_recovers_an_exact_line() {
    let slope = -1.25;
    let intercept = 40.0;
    let n = 50;
    let values: Vec<f64> = (0..n).map(|i| intercept + slope * f64::from(i)).collect();

    let mut m = LinearRegressionModel::new();
    m.fit(&timestamps(values.len()), &values).unwrap();
    let f = m.predict(3).unwrap();

    for (h, v) in f.values.iter().enumerate() {
        let x = f64::from(n) + h as f64;
        let expected = intercept + slope * x;
        assert_close(*v, expected, 1e-6, &format!("linreg step {}", h + 1));
    }
}

// ── Online update ──────────────────────────────────────────────────────

/// `update()` is documented as an O(1) equivalent of refitting with the extra
/// point appended. For SES that equivalence is exact, so it is checkable.
#[test]
fn ses_online_update_matches_a_full_refit() {
    let mut values: Vec<f64> = (0..40)
        .map(|i| 50.0 + (f64::from(i) * 0.7).sin() * 5.0)
        .collect();

    let mut online = SesModel::new(Some(0.4));
    online.fit(&timestamps(values.len()), &values).unwrap();

    let new_point = 61.0;
    online.update(values.len() as i64 * SEC, new_point).unwrap();

    values.push(new_point);
    let mut refit = SesModel::new(Some(0.4));
    refit.fit(&timestamps(values.len()), &values).unwrap();

    let a = online.predict(1).unwrap().values[0];
    let b = refit.predict(1).unwrap().values[0];
    assert_close(a, b, 1e-9, "SES online update vs refit");
}

// ── Degenerate inputs ──────────────────────────────────────────────────

/// Models that cannot be fitted from a single point must say so.
#[test]
fn models_reject_inputs_that_are_too_short() {
    let ts = timestamps(1);
    let v = vec![1.0];
    assert!(HoltLinearModel::new(None, None, 1.0).fit(&ts, &v).is_err());
    assert!(LinearRegressionModel::new().fit(&ts, &v).is_err());
}

/// Holt-Winters instead degrades: too little data for a seasonal
/// decomposition drops it to a level+trend model rather than failing, so a
/// freshly-created series still forecasts. The forecast must be finite and,
/// from a single point, flat at that point.
#[test]
fn holt_winters_degrades_to_level_and_trend_on_short_input() {
    let mut m = HoltWintersModel::new(None, None, None, Some(4), false);
    m.fit(&timestamps(1), &[3.5]).unwrap();
    let f = m.predict(3).unwrap();
    for v in &f.values {
        assert!(v.is_finite(), "degraded fit produced {v}");
        assert_close(*v, 3.5, 1e-9, "HW from a single point");
    }
}

/// Residual spread must follow the same degrees-of-freedom convention across
/// models. Dividing by the raw residual count instead narrows every
/// interval derived from it, so a model fitted on the same data must not
/// report a visibly tighter interval just because of which model it is.
#[test]
fn prediction_intervals_use_consistent_degrees_of_freedom() {
    // A trendless noisy series both models can fit.
    let values: Vec<f64> = (0..40)
        .map(|i| 100.0 + ((i * 7919) % 13) as f64 - 6.0)
        .collect();
    let ts = timestamps(values.len());

    let mut holt = HoltLinearModel::new(Some(0.3), Some(0.05), 1.0);
    holt.fit(&ts, &values).unwrap();
    let mut hw = HoltWintersModel::new(Some(0.3), Some(0.05), None, Some(1), false);
    hw.fit(&ts, &values).unwrap();

    let h = holt.predict(1).unwrap();
    let w = hw.predict(1).unwrap();
    let h_width = h.confidence_upper[0] - h.confidence_lower[0];
    let w_width = w.confidence_upper[0] - w.confidence_lower[0];

    // Same data, same smoothing constants, near-identical models: the
    // interval widths should be within a factor of two of each other.
    assert!(
        w_width > 0.0 && h_width > 0.0,
        "degenerate intervals: holt {h_width}, hw {w_width}"
    );
    let ratio = w_width / h_width;
    assert!(
        (0.5..=2.0).contains(&ratio),
        "Holt-Winters interval width {w_width} vs Holt {h_width} (ratio {ratio:.2}) — \
         the two models disagree on residual degrees of freedom"
    );
}

/// Mismatched timestamp/value lengths must be an error, not a silent zip.
#[test]
fn mismatched_input_lengths_are_rejected() {
    let ts = timestamps(10);
    let v = vec![1.0; 5];
    assert!(SesModel::new(None).fit(&ts, &v).is_err());
    assert!(HoltLinearModel::new(None, None, 1.0).fit(&ts, &v).is_err());
    assert!(LinearRegressionModel::new().fit(&ts, &v).is_err());
}

// ── ARIMA estimation ───────────────────────────────────────────────────
//
// The orders that interact are the ones nobody checks by hand. A pure AR
// and a pure MA were recovered correctly for as long as this file existed;
// a *mixed* ARMA was not, because `ArimaModel` held its AR block at the
// Burg seed and optimised only the MA against it. A Burg AR fitted to mixed
// data is biased — the moving-average term drags the lag-1 autocorrelation
// away from φ — so the MA search then fits the residue of the wrong model.

use chronix_analytics::forecast::{ArimaModel, ModelParams};

/// Box–Muller over an LCG, so the series is identical on every platform.
fn gauss(seed: u64, n: usize) -> Vec<f64> {
    let mut s = seed;
    let mut u = move || {
        s = s.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        ((s >> 11) as f64 / (1u64 << 53) as f64).clamp(1e-12, 1.0 - 1e-12)
    };
    (0..n)
        .map(|_| {
            let (a, b) = (u(), u());
            (-2.0 * a.ln()).sqrt() * (std::f64::consts::TAU * b).cos()
        })
        .collect()
}

/// Schur–Cohn. `1 - Σφⱼzʲ` has every root strictly outside the unit circle
/// iff the inverse Levinson–Durbin peel stays inside `(-1, 1)`. Exact, and
/// without root-finding — which matters, because the interesting fits sit
/// within 1e-4 of the boundary and a grid search cannot tell the sides apart.
fn is_stable(coeffs: &[f64]) -> bool {
    let mut cur = coeffs.to_vec();
    for k in (0..cur.len()).rev() {
        let r = cur[k];
        if r.abs() >= 1.0 {
            return false;
        }
        if k == 0 {
            break;
        }
        let denom = 1.0 - r * r;
        let prev: Vec<f64> = (0..k)
            .map(|j| r.mul_add(cur[k - 1 - j], cur[j]) / denom)
            .collect();
        cur[..k].copy_from_slice(&prev);
    }
    true
}

/// Coefficients of a known process come back, mixed orders included.
///
/// The expected values are `statsmodels`' maximum likelihood on the same
/// series, not the generating parameters: at n = 2 000 a realisation's own
/// φ genuinely differs from the φ it was generated with, and asserting the
/// generator would be asserting that the estimator is *biased in the way
/// this sample happens to be*. Agreeing with the reference to four decimals
/// is the statement worth making.
#[test]
fn arima_recovers_known_coefficients() {
    const N: usize = 2000;
    let ts = timestamps(N);

    // AR(1), φ = 0.7.
    let e = gauss(11, N);
    let mut x = vec![0.0; N];
    for t in 1..N {
        x[t] = 0.7 * x[t - 1] + e[t];
    }
    let mut m = ArimaModel::new(1, 0, 0);
    m.fit(&ts, &x).unwrap();
    let ModelParams::Arima { ar_coeffs, .. } = m.params() else {
        panic!("wrong params variant")
    };
    assert_close(ar_coeffs[0], 0.7028, 1e-3, "AR(1) phi");

    // MA(1), θ = 0.5.
    let e = gauss(13, N);
    let mut x = vec![0.0; N];
    for t in 1..N {
        x[t] = e[t] + 0.5 * e[t - 1];
    }
    let mut m = ArimaModel::new(0, 0, 1);
    m.fit(&ts, &x).unwrap();
    let ModelParams::Arima { ma_coeffs, .. } = m.params() else {
        panic!("wrong params variant")
    };
    assert_close(ma_coeffs[0], 0.5134, 1e-3, "MA(1) theta");

    // ARMA(1,1), φ = 0.6, θ = -0.4. statsmodels MLE: 0.5707 / -0.3675.
    // This is the one that was wrong: 0.2194 / -0.0191.
    let e = gauss(17, N);
    let mut x = vec![0.0; N];
    for t in 1..N {
        x[t] = 0.6 * x[t - 1] + e[t] - 0.4 * e[t - 1];
    }
    let mut m = ArimaModel::new(1, 0, 1);
    m.fit(&ts, &x).unwrap();
    let ModelParams::Arima {
        ar_coeffs,
        ma_coeffs,
        ..
    } = m.params()
    else {
        panic!("wrong params variant")
    };
    assert_close(ar_coeffs[0], 0.5707, 1e-3, "ARMA(1,1) phi");
    assert_close(ma_coeffs[0], -0.3675, 1e-3, "ARMA(1,1) theta");

    // ARIMA(1,1,1). statsmodels MLE with drift: 0.4400 / -0.2175.
    let e = gauss(23, N);
    let mut d = vec![0.0; N];
    for t in 1..N {
        d[t] = 0.5 * d[t - 1] + e[t] - 0.3 * e[t - 1];
    }
    let mut x = vec![0.0; N];
    for t in 1..N {
        x[t] = x[t - 1] + d[t];
    }
    let mut m = ArimaModel::new(1, 1, 1);
    m.fit(&ts, &x).unwrap();
    let ModelParams::Arima {
        ar_coeffs,
        ma_coeffs,
        ..
    } = m.params()
    else {
        panic!("wrong params variant")
    };
    assert_close(ar_coeffs[0], 0.4400, 1e-3, "ARIMA(1,1,1) phi");
    assert_close(ma_coeffs[0], -0.2175, 1e-3, "ARIMA(1,1,1) theta");

    // A random walk with drift: the constant is the drift per step.
    let e = gauss(19, N);
    let mut x = vec![0.0; N];
    for t in 1..N {
        x[t] = x[t - 1] + 0.25 + e[t];
    }
    let mut m = ArimaModel::new(0, 1, 0);
    m.fit(&ts, &x).unwrap();
    let ModelParams::Arima { constant, .. } = m.params() else {
        panic!("wrong params variant")
    };
    assert_close(*constant, 0.25, 0.05, "random-walk drift");
}

/// Every fitted polynomial is stationary and invertible, by construction.
///
/// Not a bound, a **reparameterisation**: the optimiser searches unconstrained
/// reals, and `tanh` plus the Levinson–Durbin recursion turns them into
/// partial autocorrelations inside `(-1, 1)` and thence into a polynomial
/// whose roots are outside the unit circle for any order — Jones (1980),
/// which is what `statsmodels`' `enforce_stationarity` does.
///
/// The box it replaces was neither sufficient nor necessary. Not sufficient:
/// `φ = (0.99, 0.012)` is inside ±0.99 and has a root at 0.9985, so the
/// forecast recursion diverges — 21 of 80 fits on a random walk landed
/// there. Not necessary: `φ = (1.2, -0.4)` is an ordinary stationary AR(2)
/// that the box could not represent at all.
#[test]
fn every_fitted_polynomial_is_stable() {
    assert!(is_stable(&[0.5]), "the criterion accepts a stable AR(1)");
    assert!(
        is_stable(&[1.2, -0.4]),
        "the criterion accepts an AR(2) the old box could not reach"
    );
    assert!(
        !is_stable(&[0.99, 0.011_8]),
        "the criterion rejects what the old box produced"
    );
    assert!(!is_stable(&[1.0]), "a unit root is not stable");

    let mut state = 7u64;
    let mut next = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        ((state >> 33) as f64 / f64::from(u32::MAX >> 1)) - 1.0
    };
    let mut checked = 0usize;
    for p in 1..=3usize {
        for trial in 0..40 {
            let n = 160usize;
            let (mut prev0, mut prev1, mut walk) = (0.0f64, 0.0f64, 0.0f64);
            let v: Vec<f64> = (0..n)
                .map(|i| {
                    let e = next();
                    // Noise, a strongly correlated MA, a random walk and a
                    // trend: the last two are what push a fit to the edge.
                    let out = match trial % 4 {
                        0 => e,
                        1 => 0.9f64.mul_add(prev1, 0.95f64.mul_add(prev0, e)),
                        2 => {
                            walk += e;
                            walk
                        }
                        _ => (i as f64).mul_add(0.05, e),
                    };
                    prev1 = prev0;
                    prev0 = e;
                    out
                })
                .collect();
            let ts: Vec<i64> = (0..n as i64).collect();
            for q in 0..=2usize {
                let mut m = ArimaModel::new(p, 0, q);
                if m.fit(&ts, &v).is_err() {
                    continue;
                }
                let ModelParams::Arima {
                    ar_coeffs,
                    ma_coeffs,
                    ..
                } = m.params()
                else {
                    continue;
                };
                checked += 1;
                assert!(
                    is_stable(ar_coeffs),
                    "p={p} q={q} trial={trial}: non-stationary AR {ar_coeffs:?}"
                );
                // The MA convention here is `1 + Σθⱼzʲ`, so the stability
                // criterion applies to the negated coefficients.
                let negated: Vec<f64> = ma_coeffs.iter().map(|v| -v).collect();
                assert!(
                    is_stable(&negated),
                    "p={p} q={q} trial={trial}: non-invertible MA {ma_coeffs:?}"
                );
            }
        }
    }
    assert!(checked >= 300, "only {checked} fits were checked");
}
