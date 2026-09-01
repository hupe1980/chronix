#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Quantile Forecasting
//!
//! Prediction intervals built from the **empirical distribution of
//! walk-forward residuals**, bucketed per horizon step — rather than the
//! Gaussian `ŷ ± 1.96σ` the point models produce.
//!
//! This matters for the workloads Chronix targets. A household PV series is
//! heavily right-skewed and its error scale grows with the horizon; a
//! symmetric fixed-σ band under-covers the long tail while wasting width on
//! the short one. Empirical quantiles learn the actual shape.
//!
//! Residual quantiles are additive offsets, so they do not by themselves
//! respect a physical domain — `lower_bound`/`upper_bound` state that
//! separately.
//!
//! ```sh
//! cargo run -p chronix --example quantile_forecast
//! ```

use chronix::chronix_analytics::forecast::{
    CalibrationStrategy, HoltWintersModel, QuantileConfig, QuantileForecaster,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── A deliberately skewed, seasonal series ─────────────────────────
    //
    // Simulated PV generation: a daily bell clipped at zero overnight, plus
    // multiplicative cloud noise. Errors here are asymmetric by construction.
    let period = 24usize;
    let n = 480usize; // 20 days
    let interval_ns = 3_600_000_000_000_i64; // 1 hour
    let base_ts = 1_700_000_000_000_000_000_i64;

    let mut seed = 0x5EED_u64;
    let mut rand = move || {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        (seed >> 33) as f64 / f64::from(u32::MAX)
    };

    let mut timestamps = Vec::with_capacity(n);
    let mut values = Vec::with_capacity(n);
    for i in 0..n {
        let hour = (i % period) as f64;
        // Daylight bell between hours 6 and 18, zero otherwise.
        let solar = if (6.0..=18.0).contains(&hour) {
            let x = (hour - 12.0) / 6.0;
            (1.0 - x * x).max(0.0) * 4_000.0
        } else {
            0.0
        };
        // Clouds cut generation but never add to it — the skew.
        let cloud = 1.0 - rand() * 0.6;
        timestamps.push(base_ts + i as i64 * interval_ns);
        values.push(solar * cloud);
    }
    println!(
        "✅ Simulated {n} hourly PV observations ({} days)\n",
        n / 24
    );

    // ── Calibrate ──────────────────────────────────────────────────────
    let config = QuantileConfig {
        levels: vec![0.1, 0.5, 0.9],
        horizon: 24,
        initial_window: 240, // 10 days of training
        step: 6,             // roll the origin every 6 hours
        strategy: CalibrationStrategy::OnlineUpdate,
        conformal: true,
        // PV generation cannot be negative and cannot exceed the inverter's
        // nameplate rating. Residual quantiles are additive offsets, so the
        // domain has to be stated rather than learned.
        lower_bound: Some(0.0),
        upper_bound: Some(5_000.0),
    };

    let mut forecaster = QuantileForecaster::new(
        || {
            Box::new(HoltWintersModel::new(
                Some(0.3),
                Some(0.05),
                Some(0.4),
                Some(24),
                false,
            ))
        },
        config,
    );
    forecaster.fit(&timestamps, &values)?;
    println!(
        "✅ Calibrated on walk-forward residuals \
         ({} residuals at h=1, {} at h=24)\n",
        forecaster.calibration_size(1),
        forecaster.calibration_size(24),
    );

    // ── Forecast ───────────────────────────────────────────────────────
    let forecast = forecaster.predict(24)?;
    let p10 = forecast.level(0.1).expect("configured level");
    let p50 = forecast.level(0.5).expect("configured level");
    let p90 = forecast.level(0.9).expect("configured level");

    println!("  h   point      p10       p50       p90     width   n");
    println!("  ──────────────────────────────────────────────────────");
    for h in 0..24 {
        println!(
            "  {:2}  {:8.1}  {:8.1}  {:8.1}  {:8.1}  {:7.1}  {:3}",
            h + 1,
            forecast.point[h],
            p10[h],
            p50[h],
            p90[h],
            p90[h] - p10[h],
            forecast.calibration_counts[h],
        );
    }

    // ── Why this beats a symmetric interval ────────────────────────────
    //
    // The empirical interval is asymmetric around the point forecast
    // wherever the residual distribution is. Show that directly.
    let mut asymmetric = 0;
    for h in 0..24 {
        let below = forecast.point[h] - p10[h];
        let above = p90[h] - forecast.point[h];
        if (below - above).abs() > 0.15 * (below + above).max(1.0) {
            asymmetric += 1;
        }
    }
    println!(
        "\n📐 {asymmetric}/24 horizons have a visibly asymmetric interval — \n\
           a Gaussian ±z·σ band cannot represent any of them.\n\
           Bounds keep every quantile within [0, 5000] W."
    );

    // The `predict_interval` helper returns the familiar `ForecastResult`
    // shape, so existing code paths get calibrated bounds for free.
    let central = forecaster.predict_interval(12, 0.8)?;
    println!(
        "\n📊 80% central interval at h=1: [{:.1}, {:.1}] (point {:.1})",
        central.confidence_lower[0], central.confidence_upper[0], central.values[0],
    );

    Ok(())
}
