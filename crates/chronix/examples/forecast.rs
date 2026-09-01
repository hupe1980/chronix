#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Forecasting — All Models
//!
//! Demonstrates every forecasting model available in Chronix — Linear
//! Regression, SES, Holt Linear, Holt-Winters, ARIMA and SARIMA — then hands
//! the same series to `auto_forecast`, which picks one by cross-validation and
//! shows its working.
//!
//! ```sh
//! cargo run -p chronix --example forecast
//! ```

use std::sync::Arc;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix, ForecastConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let config = ChronixConfig::builder().data_dir(dir.path()).build()?;
    let db = Arc::new(Chronix::open(config)?);

    // ── Seed: 300 points with trend + seasonality (5-minute intervals) ──
    let base_ts = 1_700_000_000_000_000_000_i64;
    let interval_ns = 300_000_000_000_i64; // 5 minutes

    let key = SeriesKey::new("energy_usage", tags! { "building" => "HQ", "floor" => "3" })?;

    let n = 300;
    let period = 24; // daily cycle (24 × 5 min = 2 hours simulated)
    let mut points = Vec::with_capacity(n);

    for i in 0..n {
        let t = i as f64;
        // trend + seasonal pattern + slight noise
        let value = 200.0 + t * 0.5 + (t * 2.0 * std::f64::consts::PI / period as f64).sin() * 40.0;
        points.push(Point::new(
            key.clone(),
            fields! { "kwh" => value },
            base_ts + (i as i64) * interval_ns,
        )?);
    }
    assert!(
        db.insert_batch(&points)?.is_complete(),
        "insert was partial"
    );
    println!("✅ Seeded {n} points (trend + seasonal, period={period})\n");

    let start = base_ts;
    let end = base_ts + (n as i64) * interval_ns;
    let tags: &[(&str, &str)] = &[("building", "HQ"), ("floor", "3")];
    let horizon = 12;

    // ── 1. Linear Regression ───────────────────────────────────
    println!("─── 1. Linear Regression (horizon={horizon}) ───");
    let result = db.forecast(
        "energy_usage",
        "kwh",
        tags,
        start,
        end,
        horizon,
        Some(ForecastConfig {
            model: Some("linear_regression".into()),
            confidence: 0.95,
            ..Default::default()
        }),
    )?;
    print_forecast("Linear Regression", &result);

    // ── 2. Simple Exponential Smoothing ────────────────────────
    println!("─── 2. SES (horizon={horizon}) ───");
    let result = db.forecast(
        "energy_usage",
        "kwh",
        tags,
        start,
        end,
        horizon,
        Some(ForecastConfig {
            model: Some("ses".into()),
            confidence: 0.90,
            ..Default::default()
        }),
    )?;
    print_forecast("SES", &result);

    // ── 3. Holt Linear (double exponential smoothing) ──────────
    println!("─── 3. Holt Linear (horizon={horizon}) ───");
    let result = db.forecast(
        "energy_usage",
        "kwh",
        tags,
        start,
        end,
        horizon,
        Some(ForecastConfig {
            model: Some("holt".into()),
            confidence: 0.95,
            ..Default::default()
        }),
    )?;
    print_forecast("Holt Linear", &result);

    // ── 4. Holt-Winters (triple exponential smoothing) ─────────
    println!("─── 4. Holt-Winters (period={period}, horizon={horizon}) ───");
    let result = db.forecast(
        "energy_usage",
        "kwh",
        tags,
        start,
        end,
        horizon,
        Some(ForecastConfig {
            model: Some("holt_winters".into()),
            confidence: 0.95,
            period: Some(period),
            ..Default::default()
        }),
    )?;
    print_forecast("Holt-Winters", &result);

    // ── 5. ARIMA(1,1,1) ───────────────────────────────────────
    println!("─── 5. ARIMA(1,1,1) (horizon={horizon}) ───");
    let result = db.forecast(
        "energy_usage",
        "kwh",
        tags,
        start,
        end,
        horizon,
        Some(ForecastConfig {
            model: Some("arima".into()),
            confidence: 0.95,
            arima_order: Some((1, 1, 1)),
            ..Default::default()
        }),
    )?;
    print_forecast("ARIMA(1,1,1)", &result);

    // ── 6. SARIMA(1,1,1)(1,1,1,24) ────────────────────────────
    println!("─── 6. SARIMA(1,1,1)(1,1,1,{period}) (horizon={horizon}) ───");
    let result = db.forecast(
        "energy_usage",
        "kwh",
        tags,
        start,
        end,
        horizon,
        Some(ForecastConfig {
            model: Some("sarima".into()),
            confidence: 0.95,
            period: Some(period),
            arima_order: Some((1, 1, 1)),
            sarima_order: Some((1, 1, 1, period)),
        }),
    )?;
    print_forecast("SARIMA", &result);

    // ── 7. Default model (no config → SES) ─────────────────────
    println!("─── 7. Default model (no config: simple exponential smoothing) ───");
    let result = db.forecast("energy_usage", "kwh", tags, start, end, horizon, None)?;
    print_forecast("Default (SES)", &result);

    // ── 8. Automatic selection ─────────────────────────────────
    //
    // Every candidate above, scored by rolling-origin cross-validation at the
    // horizon actually wanted, and the winner refitted on everything.
    println!("─── 8. auto_forecast (model chosen by cross-validation) ───");
    let chosen = db.auto_forecast("energy_usage", "kwh", tags, start, end, horizon, None)?;
    let s = &chosen.selection;
    println!("   Winner: {}", s.label);
    println!(
        "   Chosen by: {} over {} rolling origins, {} training points in the first fold",
        s.metric, s.folds, s.min_train_size,
    );
    println!(
        "   Detected period: {}   KPSS differencing order: {}",
        s.period.map_or("none".to_string(), |m| m.to_string()),
        s.differencing_order,
    );
    println!("   Candidates:");
    for c in &s.candidates {
        println!("     {:<34} {} {:>10.3}", c.label, s.metric, c.score);
    }
    if !s.rejected.is_empty() {
        println!("   Not considered:");
        for (label, reason) in &s.rejected {
            println!("     {label:<34} {reason}");
        }
    }
    print_forecast(&s.label, &chosen.result);

    // ── 9. A different confidence level ────────────────────────
    //
    // The models compute a 95% interval; asking for another level rescales it,
    // which is exact for a symmetric normal interval.
    println!("─── 9. The same forecast at 99% confidence ───");
    let wide = db.forecast(
        "energy_usage",
        "kwh",
        tags,
        start,
        end,
        horizon,
        Some(ForecastConfig {
            confidence: 0.99,
            ..Default::default()
        }),
    )?;
    print_forecast("Default (SES) @ 99%", &wide);

    db.close()?;
    println!("✅ Done");
    Ok(())
}

fn print_forecast(label: &str, result: &chronix::chronix_analytics::forecast::ForecastResult) {
    println!("   Model: {label}");
    println!("   Confidence: {:.0}%", result.confidence_level * 100.0);
    println!("   Steps: {}", result.values.len());
    for (i, v) in result.values.iter().enumerate() {
        let lo = result.confidence_lower[i];
        let hi = result.confidence_upper[i];
        println!("   [{i:>2}] {v:>8.2}   CI: [{lo:>8.2}, {hi:>8.2}]");
    }
    println!();
}
