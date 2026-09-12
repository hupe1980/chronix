//! Does searching seasonal `(P, Q)` pay for the extra fits?
//!
//! `select_model` proposes two seasonal shapes — `(1,D,0)` and `(0,D,1)` —
//! on the argument that the answer is nearly always one of them. This tests
//! that on data generated with a true `(1,0,1)`: the one shape the candidate
//! set cannot express.
//!
//! Run with `cargo run -p chronix-analytics --example seasonal_pq_search`.
use chronix_analytics::forecast::{auto_forecast, AutoForecastOptions, ForecastModel, SarimaModel};

/// Simulate `x_t = Φ·x_{t−m} + e_t + Θ·e_{t−m}` — seasonal ARMA(1,1).
fn seasonal_arma11(n: usize, m: usize, phi: f64, theta: f64, seed: u64) -> Vec<f64> {
    let mut state = seed;
    let mut next = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        ((state >> 33) as f64 / f64::from(u32::MAX >> 1)) - 1.0
    };
    let mut x = vec![0.0f64; n + m];
    let mut e = vec![0.0f64; n + m];
    for t in m..n + m {
        e[t] = next();
        x[t] = phi * x[t - m] + e[t] + theta * e[t - m];
    }
    x[m..].to_vec()
}

fn mase(actual: &[f64], pred: &[f64], train: &[f64], m: usize) -> f64 {
    let scale: f64 = train
        .windows(m + 1)
        .map(|w| (w[m] - w[0]).abs())
        .sum::<f64>()
        / (train.len() - m) as f64;
    let mae: f64 = actual
        .iter()
        .zip(pred)
        .map(|(a, p)| (a - p).abs())
        .sum::<f64>()
        / actual.len() as f64;
    mae / scale
}

fn main() {
    let (m, horizon) = (12usize, 12usize);

    const FOLDS: [usize; 4] = [3, 5, 8, 12];
    let (mut sum_auto, mut sum_best, mut n) = (0.0f64, 0.0f64, 0usize);
    let mut sum_by_folds = [0.0f64; FOLDS.len()];
    let mut n_by_folds = [0usize; FOLDS.len()];

    for seed in 1u64..=12 {
        let series = seasonal_arma11(24 * m, m, 0.7, 0.6, seed);
        let split = series.len() - horizon;
        let (train, test) = series.split_at(split);
        let ts: Vec<i64> = (0..train.len() as i64).collect();

        // The two shapes the selector can express, fit directly.
        let mut fixed = [f64::NAN; 2];
        for (i, (sp, sq)) in [(1usize, 0usize), (0, 1)].into_iter().enumerate() {
            let mut mdl = SarimaModel::new(0, 0, 0, sp, 0, sq, m);
            if mdl.fit(&ts, train).is_ok() {
                if let Ok(f) = mdl.predict(horizon) {
                    fixed[i] = mase(test, &f.values, train, m);
                }
            }
        }
        // The shape it cannot: the true (1,0,1).
        let mut truth = f64::NAN;
        let mut mdl = SarimaModel::new(0, 0, 0, 1, 0, 1, m);
        if mdl.fit(&ts, train).is_ok() {
            if let Ok(f) = mdl.predict(horizon) {
                truth = mase(test, &f.values, train, m);
            }
        }

        for (fi, folds) in FOLDS.iter().enumerate() {
            let opts = AutoForecastOptions {
                period: Some(m),
                folds: *folds,
                ..Default::default()
            };
            if let Ok(o) = auto_forecast(&ts, train, horizon, &opts) {
                let e = mase(test, &o.result.values, train, m);
                if e.is_finite() {
                    sum_by_folds[fi] += e;
                    n_by_folds[fi] += 1;
                }
            }
        }
        if truth.is_finite() {
            sum_best += truth;
            n += 1;
        }
        let _ = (&mut sum_auto, fixed);
    }
    println!(
        "\noracle — the true (1,0,1) shape: {:.4}",
        sum_best / n as f64
    );
    for (i, f) in FOLDS.iter().enumerate() {
        println!(
            "auto_forecast, folds={f:>2}: {:.4}  ({} series)",
            sum_by_folds[i] / n_by_folds[i] as f64,
            n_by_folds[i]
        );
    }
}
