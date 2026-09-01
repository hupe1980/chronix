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
