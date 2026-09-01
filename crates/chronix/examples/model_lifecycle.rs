#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Model Lifecycle Management
//!
//! Demonstrates the model registry, A/B testing, drift detection,
//! and accuracy tracking for ML model management.
//!
//! ```bash
//! cargo run --example model_lifecycle
//! ```

use chronix_analytics::lifecycle::{
    ABTestConfig, ABTestEvaluator, AccuracyMetrics, AccuracyTracker, AccuracyTrackerConfig,
    DriftDetector, DriftMonitor, ModelRegistry, ModelTag, PromotionCriteria,
};
use std::collections::HashMap;
use std::time::Duration;

fn main() {
    println!("=== Chronix Model Lifecycle Management ===\n");

    // ── 1. Model Registry ─────────────────────────────────────────
    println!("--- Model Registry ---");
    let registry = ModelRegistry::new();

    // Register version 1: Linear Regression
    let mut hyperparams_v1 = HashMap::new();
    hyperparams_v1.insert("regularization".to_string(), "0.01".to_string());
    let v1 = registry.register(
        "cpu_forecast",
        "primary",
        "LinearRegression",
        hyperparams_v1,
        (1_000_000, 2_000_000),
        b"model_weights_v1".to_vec(),
    );
    println!(
        "Registered v{} ({}) — tag: {:?}",
        v1.version, v1.algorithm, v1.tag
    );

    // Register version 2: ARIMA (becomes Challenger)
    let mut hyperparams_v2 = HashMap::new();
    hyperparams_v2.insert("p".to_string(), "1".to_string());
    hyperparams_v2.insert("d".to_string(), "1".to_string());
    hyperparams_v2.insert("q".to_string(), "1".to_string());
    let v2 = registry.register(
        "cpu_forecast",
        "primary",
        "ARIMA(1,1,1)",
        hyperparams_v2,
        (1_500_000, 2_500_000),
        b"model_weights_v2".to_vec(),
    );
    println!(
        "Registered v{} ({}) — tag: {:?}",
        v2.version, v2.algorithm, v2.tag
    );

    // List versions
    let versions = registry.list_versions("cpu_forecast", "primary");
    println!("\nAll versions for cpu_forecast/primary:");
    for v in &versions {
        println!(
            "  v{}: {} (tag={:?}, trained={:?})",
            v.version, v.algorithm, v.tag, v.training_range
        );
    }

    // Get current champion
    if let Some(champ) = registry.get_champion("cpu_forecast", "primary") {
        println!("Current champion: v{} ({})", champ.version, champ.algorithm);
    }

    // Add metrics to v1
    let metrics_v1 = AccuracyMetrics {
        mape: 0.08,
        rmse: 2.5,
        mae: 1.8,
        r_squared: 0.92,
    };
    registry.update_metrics("cpu_forecast", "primary", 1, metrics_v1);

    // Tag v2 as champion
    registry.set_tag("cpu_forecast", "primary", 2, ModelTag::Champion);
    registry.set_tag("cpu_forecast", "primary", 1, ModelTag::Retired);
    println!("\nAfter promotion:");
    for v in registry.list_versions("cpu_forecast", "primary") {
        println!("  v{}: {} (tag={:?})", v.version, v.algorithm, v.tag);
    }

    // ── 2. A/B Testing ────────────────────────────────────────────
    println!("\n--- A/B Testing ---");
    let config = ABTestConfig {
        challenger_traffic_pct: 0.3,
        evaluation_windows: 5,
        criteria: PromotionCriteria::LowerMape,
    };
    let ab_test = ABTestEvaluator::new(config);
    println!("A/B test configured: 30% challenger traffic, evaluate every 5 windows");

    // Simulate predictions — challenger is slightly better
    let actuals = vec![
        100.0, 102.0, 98.0, 105.0, 101.0, 99.0, 103.0, 97.0, 104.0, 100.0, 102.0, 98.0, 105.0,
        101.0, 99.0,
    ];
    let champion_preds = vec![
        103.0, 105.0, 95.0, 110.0, 104.0, 96.0, 107.0, 94.0, 108.0, 103.0, 105.0, 95.0, 108.0,
        104.0, 96.0,
    ];
    let challenger_preds = vec![
        101.0, 103.0, 97.0, 106.0, 102.0, 98.5, 104.0, 96.5, 105.0, 101.0, 103.0, 97.5, 106.0,
        102.0, 98.0,
    ];

    for i in 0..actuals.len() {
        ab_test.log_prediction(actuals[i], champion_preds[i], challenger_preds[i]);
    }

    let result = ab_test.evaluate();
    println!(
        "A/B Test result: {:?} ({} observations)",
        result,
        ab_test.record_count()
    );

    if let Some(champ) = ab_test.champion_metrics() {
        println!(
            "  Champion  → MAPE: {:.4}, RMSE: {:.4}",
            champ.mape, champ.rmse
        );
    }
    if let Some(chal) = ab_test.challenger_metrics() {
        println!(
            "  Challenger → MAPE: {:.4}, RMSE: {:.4}",
            chal.mape, chal.rmse
        );
    }

    // ── 3. Drift Detection ────────────────────────────────────────
    println!("\n--- Drift Detection ---");

    // Training distribution (normal operation)
    let training_data: Vec<f64> = (0..500)
        .map(|i| 50.0 + (i as f64 * 0.01).sin() * 5.0)
        .collect();

    // Current distribution (drifted — higher mean, different variance)
    let drifted_data: Vec<f64> = (0..500)
        .map(|i| 70.0 + (i as f64 * 0.02).cos() * 15.0)
        .collect();

    // Stable distribution (no drift)
    let stable_data: Vec<f64> = (0..500)
        .map(|i| 50.5 + (i as f64 * 0.01).sin() * 5.2)
        .collect();

    let detectors = [
        ("PSI", DriftDetector::Psi { threshold: 0.25 }),
        ("KS", DriftDetector::KolmogorovSmirnov { p_value: 0.05 }),
        ("ADWIN", DriftDetector::Adwin { delta: 0.002 }),
    ];

    for (name, detector) in &detectors {
        let report = detector.detect(&training_data, &drifted_data);
        println!(
            "  {name:<6} (drifted):  score={:.4}, drift={}, action={:?}",
            report.score, report.drift_detected, report.action
        );

        let stable_report = detector.detect(&training_data, &stable_data);
        println!(
            "  {name:<6} (stable):   score={:.4}, drift={}, action={:?}",
            stable_report.score, stable_report.drift_detected, stable_report.action
        );
    }

    // ── 4. Drift Monitor ──────────────────────────────────────────
    println!("\n--- Drift Monitor ---");
    let monitor = DriftMonitor::new(
        Duration::from_secs(60),
        Box::new(|measurement, report| {
            if report.drift_detected {
                println!(
                    "  ⚠ DRIFT ALERT for {measurement}: score={:.4}, action={:?}",
                    report.score, report.action
                );
            }
        }),
    );

    monitor.register(
        "cpu_forecast",
        DriftDetector::Psi { threshold: 0.25 },
        training_data.clone(),
    );
    monitor.update_current("cpu_forecast", drifted_data);

    let results = monitor.check_all();
    println!("Monitor checked {} measurements:", results.len());
    for (name, report) in &results {
        println!(
            "  {name}: drift={}, score={:.4}",
            report.drift_detected, report.score
        );
    }

    // ── 5. Accuracy Tracker ───────────────────────────────────────
    println!("\n--- Accuracy Tracker ---");
    let tracker = AccuracyTracker::new(AccuracyTrackerConfig {
        window_size: 10,
        refit_mape_threshold: 0.05,
        refit_window_count: 3,
        max_model_age_secs: 86400 * 7, // 7 days
    });

    tracker.register_model("cpu_forecast", "arima_v2", 1_700_000_000);

    // Feed observations with intentionally worsening predictions
    for i in 0..15 {
        let actual = 100.0 + (i as f64) * 0.5;
        let predicted = actual + (i as f64) * 0.8; // increasingly bad
        tracker.add_observation("cpu_forecast", "arima_v2", actual, predicted);
    }

    if let Some(m) = tracker.metrics("cpu_forecast", "arima_v2") {
        println!(
            "  Current metrics: MAPE={:.4}, RMSE={:.4}, MAE={:.4}",
            m.mape, m.rmse, m.mae
        );
    }

    let needs_refit = tracker.needs_refit("cpu_forecast", "arima_v2");
    println!("  Needs refit: {needs_refit}");

    let is_stale = tracker.is_stale("cpu_forecast", "arima_v2", 1_700_000_000 + 86400 * 8);
    println!("  Is stale (after 8 days): {is_stale}");

    // ── 6. Compute Accuracy Metrics ───────────────────────────────
    println!("\n--- AccuracyMetrics::compute ---");
    let actuals_check: Vec<f64> = (0..20).map(|i| 100.0 + i as f64).collect();
    let preds_check: Vec<f64> = actuals_check.iter().map(|v| v + 1.5).collect();
    let metrics = AccuracyMetrics::compute(&actuals_check, &preds_check);
    println!(
        "  MAE={:.4}, RMSE={:.4}, MAPE={:.4}, R²={:.4}",
        metrics.mae, metrics.rmse, metrics.mape, metrics.r_squared
    );

    println!("\n✓ Model lifecycle management complete");
}
