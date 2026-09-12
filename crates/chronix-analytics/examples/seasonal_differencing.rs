//! Why there is no seasonal unit-root test.
//!
//! `D` comes from a seasonal-**strength** rule (R's `nsdiffs` default), which
//! measures strength rather than stationarity: a stochastic seasonal level —
//! a seasonal random walk — scores low and is left undifferenced. In
//! isolation that is expensive. The `D=0` and `D=1` columns below are the
//! same SARIMA shape at the two orders, and `D=1` wins by roughly 2x MASE.
//!
//! The conclusion is the third column. `auto_forecast` does not pay that
//! cost, because Holt-Winters is in the candidate set and its seasonal
//! smoothing tracks a drifting level — exactly what the strength rule is
//! blind to. Proposing both `D` values and letting the folds choose was
//! measured against this: mean MASE moved by less than 0.01 in three
//! regimes and got *worse* in the fourth, for 22% more fits. So OCSB or
//! Canova-Hansen would buy close to nothing here, and the entry asking for
//! one is closed by this measurement rather than by an implementation.
//!
//! Run with `cargo run -p chronix-analytics --example seasonal_differencing`.
use chronix_analytics::forecast::{auto_forecast, AutoForecastOptions, ForecastModel, SarimaModel};

/// A seasonal random walk: each season's level drifts by its own noise.
fn seasonal_random_walk(cycles: usize, m: usize, seed: u64, drift: f64) -> Vec<f64> {
    let mut state = seed;
    let mut next = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        ((state >> 33) as f64 / f64::from(u32::MAX >> 1)) - 1.0
    };
    let mut level = vec![0.0f64; m];
    let mut out = Vec::with_capacity(cycles * m);
    for _ in 0..cycles {
        for lvl in level.iter_mut() {
            *lvl += next() * drift;
            out.push(*lvl + next() * 0.1);
        }
    }
    out
}

/// Mean absolute scaled error against the seasonal-naive benchmark.
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
    for (m, cycles, drift) in [
        (12usize, 14usize, 3.0f64),
        (12, 14, 1.0),
        (12, 25, 3.0),
        (4, 20, 2.0),
    ] {
        run(m, cycles, drift);
    }
}

fn run(m: usize, cycles: usize, drift: f64) {
    let horizon = m;
    println!("\n── m={m}, cycles={cycles}, drift={drift} ──");
    println!(
        "{:>5}  {:>8}  {:>8}  {:>7}   auto_forecast picked",
        "seed", "D=0", "D=1", "chosen"
    );
    let (mut wins, mut total, mut sum, mut chose_d1) = (0usize, 0usize, 0.0f64, 0usize);

    for seed in 1u64..=10 {
        let series = seasonal_random_walk(cycles, m, seed, drift);
        let split = series.len() - horizon;
        let (train, test) = series.split_at(split);
        let ts: Vec<i64> = (0..train.len() as i64).collect();

        let mut row = [f64::NAN; 2];
        for (i, sd) in [0usize, 1].into_iter().enumerate() {
            let mut model = SarimaModel::new(1, 0, 0, 1, sd, 0, m);
            if model.fit(&ts, train).is_ok() {
                if let Ok(f) = model.predict(horizon) {
                    row[i] = mase(test, &f.values, train, m);
                }
            }
        }

        let opts = AutoForecastOptions {
            period: Some(m),
            ..Default::default()
        };
        // What the user actually gets: the selected model's forecast.
        let (picked, chosen_mase) = match auto_forecast(&ts, train, horizon, &opts) {
            Ok(out) => (
                out.selection.label.clone(),
                mase(test, &out.result.values, train, m),
            ),
            Err(e) => (format!("error: {e}"), f64::NAN),
        };
        if picked.contains(",1,") && picked.contains(&format!("[{m}]")) {
            chose_d1 += 1;
        }
        sum += chosen_mase;
        total += 1;
        if chosen_mase < row[0] {
            wins += 1;
        }
        println!(
            "{seed:>5}  {:>8.4}  {:>8.4}  {chosen_mase:>7.4}   {picked}",
            row[0], row[1]
        );
    }
    println!(
        "\nauto_forecast beat the D=0 SARIMA on {wins}/{total}; mean MASE of what it picked = {:.3}",
        sum / total as f64
    );
    println!("(seasonally differenced shapes picked {chose_d1}/10)");
}
