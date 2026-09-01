#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Anomaly Detection — All Methods
//!
//! Demonstrates every anomaly detection method: Z-Score, Modified Z-Score,
//! IQR, Dynamic Threshold, Moving Average Residual, and Forecast Residual.
//!
//! ```sh
//! cargo run -p chronix --example anomaly_detection
//! ```

use std::sync::Arc;

use chronix::prelude::*;
use chronix::{fields, tags, AnomalyConfig, Chronix};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let config = ChronixConfig::builder().data_dir(dir.path()).build()?;
    let db = Arc::new(Chronix::open(config)?);

    // ── Seed: stable baseline + injected anomalies ─────────────
    let base_ts = 1_700_000_000_000_000_000_i64;
    let interval_ns = 10_000_000_000_i64; // 10-second intervals

    let key = SeriesKey::new(
        "temperature",
        tags! { "sensor" => "tank-1", "zone" => "east" },
    )?;

    let n = 200;
    let mut points = Vec::with_capacity(n + 5);

    // Normal readings: ~22°C with small random-like variation
    for i in 0..n {
        let t = i as f64;
        let value = 22.0 + (t * 0.3).sin() * 1.5 + (t * 0.7).cos() * 0.8;
        points.push(Point::new(
            key.clone(),
            fields! { "value" => value },
            base_ts + (i as i64) * interval_ns,
        )?);
    }

    // Inject 3 clear anomalies
    let anomaly_offsets = [n, n + 1, n + 2];
    let anomaly_values = [55.0, -10.0, 80.0]; // Way outside normal range
    for (j, &val) in anomaly_values.iter().enumerate() {
        points.push(Point::new(
            key.clone(),
            fields! { "value" => val },
            base_ts + anomaly_offsets[j] as i64 * interval_ns,
        )?);
    }

    // Two more normal points after anomalies
    for k in 0..2 {
        let i = n + 3 + k;
        points.push(Point::new(
            key.clone(),
            fields! { "value" => 22.5 },
            base_ts + (i as i64) * interval_ns,
        )?);
    }

    assert!(
        db.insert_batch(&points)?.is_complete(),
        "insert was partial"
    );
    let total = points.len();
    println!("✅ Seeded {total} points ({n} normal + 3 spikes + 2 recovery)\n");

    let start = base_ts;
    let end = base_ts + (total as i64) * interval_ns;
    let tag_pairs: &[(&str, &str)] = &[("sensor", "tank-1"), ("zone", "east")];

    // All methods to test, with appropriate thresholds
    let methods: &[(&str, f64, Option<usize>)] = &[
        ("zscore", 2.5, None),
        ("modified_zscore", 2.5, None),
        ("iqr", 1.5, None),
        ("dynamic_threshold", 3.0, Some(30)),
        ("moving_average", 3.0, Some(20)),
        ("forecast_residual", 3.0, None),
    ];

    for (i, &(method, threshold, window)) in methods.iter().enumerate() {
        println!(
            "─── {}. {} (threshold={threshold}{}) ───",
            i + 1,
            method,
            window.map_or(String::new(), |w| format!(", window={w}"))
        );

        let scores = db.detect_anomalies(
            "temperature",
            "value",
            tag_pairs,
            start,
            end,
            Some(AnomalyConfig {
                method: Some(method.into()),
                threshold,
                window_size: window,
            }),
        )?;

        let anomalies: Vec<_> = scores.iter().filter(|s| s.is_anomaly).collect();
        println!("   Scored points : {}", scores.len());
        println!("   Anomalies     : {}", anomalies.len());

        for a in &anomalies {
            println!(
                "   ⚠️  ts={} value={:>7.2} score={:.3} method={:?} | {}",
                a.timestamp, a.value, a.score, a.method, a.details
            );
        }
        println!();
    }

    db.close()?;
    println!("✅ Done");
    Ok(())
}
