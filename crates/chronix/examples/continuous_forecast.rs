#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Continuous Forecast
//!
//! Demonstrates the continuous forecasting engine, forecast caching,
//! accuracy tracking, and anomaly precision feedback loops.
//!
//! ```bash
//! cargo run --example continuous_forecast
//! ```

use chronix::chronix_analytics::forecast::ForecastResult;
use chronix::chronix_analytics::forecast::ModelType;
use chronix::chronix_analytics::{
    compute_accuracy, AnomalyFeedback, AnomalyPrecisionTracker, ContinuousForecastConfig,
    ContinuousForecastEngine, FeedbackLabel, ForecastAccuracyTracker, ForecastCache,
};
use chronix::chronix_core::FieldValue;
use std::collections::BTreeMap;
use std::time::Duration;

fn main() {
    println!("=== Chronix Continuous Forecasting ===\n");

    // ── 1. Continuous Forecast Engine ─────────────────────────────
    println!("--- Continuous Forecast Engine ---");

    let engine = ContinuousForecastEngine::new();

    // Configure continuous forecasting for cpu_usage
    let config = ContinuousForecastConfig::new("cpu_usage", ModelType::Ses)
        .with_field("value")
        .with_update_interval(20)
        .with_min_fit_points(15)
        .with_max_buffer_size(1000);

    engine.enable(config).expect("Failed to enable");
    println!("Enabled continuous forecast for cpu_usage (update every 20 points)");
    println!("  is_enabled: {}", engine.is_enabled("cpu_usage"));

    // Feed points to the engine
    let tags = BTreeMap::new();
    let base_ts = 1_700_000_000_000_000_000i64;

    println!("\nFeeding 50 data points...");
    for i in 0..50 {
        let ts = base_ts + i * 1_000_000_000;
        let value = 50.0 + (i as f64 * 0.1).sin() * 20.0 + (i as f64) * 0.3;
        let mut fields = BTreeMap::new();
        fields.insert("value".to_string(), FieldValue::F64(value));

        let update = engine.process_point("cpu_usage", &tags, &fields, ts);
        if let Some(ref u) = update {
            println!("  Point {i}: update={u:?}");
        }
    }

    // Disable
    engine.disable("cpu_usage");
    println!("Disabled continuous forecast for cpu_usage");
    println!("  is_enabled: {}", engine.is_enabled("cpu_usage"));

    // ── 2. Forecast Cache ─────────────────────────────────────────
    println!("\n--- Forecast Cache ---");

    let cache = ForecastCache::new(Duration::from_secs(3600)); // 1 hour max age
    println!("Created forecast cache (max_age=1h)");

    // Store a forecast result
    let forecast_result = ForecastResult {
        values: vec![60.0, 62.0, 65.0, 68.0, 70.0],
        timestamps: vec![
            base_ts + 50_000_000_000,
            base_ts + 51_000_000_000,
            base_ts + 52_000_000_000,
            base_ts + 53_000_000_000,
            base_ts + 54_000_000_000,
        ],
        confidence_lower: vec![55.0, 57.0, 59.0, 62.0, 64.0],
        confidence_upper: vec![65.0, 67.0, 71.0, 74.0, 76.0],
        confidence_level: 0.95,
    };

    let tags_cpu: BTreeMap<String, String> = [("host".to_string(), "web-01".to_string())].into();

    cache.store("cpu_usage", &tags_cpu, forecast_result, 5);
    println!("Stored forecast for cpu_usage {{host=web-01}}");
    println!("  Cache size: {}", cache.len());

    // Retrieve
    if let Some(materialized) = cache.get("cpu_usage", &tags_cpu) {
        println!("  Retrieved forecast:");
        println!("    Horizon: {}", materialized.horizon);
        println!("    Values: {:?}", materialized.result.values);
        println!(
            "    Fresh: {}",
            materialized.is_fresh(Duration::from_secs(3600))
        );
    }

    // Store another forecast
    let tags_mem: BTreeMap<String, String> = [("host".to_string(), "web-02".to_string())].into();
    cache.store(
        "memory_usage",
        &tags_mem,
        ForecastResult {
            values: vec![80.0, 82.0, 85.0],
            timestamps: vec![
                base_ts + 50_000_000_000,
                base_ts + 51_000_000_000,
                base_ts + 52_000_000_000,
            ],
            confidence_lower: vec![75.0, 77.0, 80.0],
            confidence_upper: vec![85.0, 87.0, 90.0],
            confidence_level: 0.95,
        },
        3,
    );
    println!("  Cache size after 2nd store: {}", cache.len());

    // Invalidate one
    cache.invalidate("memory_usage", &tags_mem);
    println!("  Cache size after invalidation: {}", cache.len());

    // Evict stale entries
    let evicted = cache.evict_stale();
    println!("  Evicted stale: {evicted}");

    // ── 3. Forecast Accuracy Tracking ─────────────────────────────
    println!("\n--- Forecast Accuracy Tracking ---");

    let tracker = ForecastAccuracyTracker::new().with_max_entries(100);

    // Evaluate model accuracy
    let forecasted = vec![100.0, 105.0, 110.0, 115.0, 120.0];
    let actual = vec![101.0, 104.0, 112.0, 113.0, 121.0];

    let metrics = tracker
        .evaluate(&forecasted, &actual, "ARIMA", "cpu_usage", "v1")
        .expect("Evaluate failed");
    println!("Accuracy for ARIMA/cpu_usage/v1:");
    println!(
        "  MAE={:.4}, RMSE={:.4}, MAPE={:.4}",
        metrics.mae, metrics.rmse, metrics.mape
    );

    // Evaluate another model
    let forecasted2 = vec![102.0, 106.0, 109.0, 116.0, 119.0];
    let metrics2 = tracker
        .evaluate(&forecasted2, &actual, "HoltWinters", "cpu_usage", "v1")
        .expect("Evaluate failed");
    println!("Accuracy for HoltWinters/cpu_usage/v1:");
    println!(
        "  MAE={:.4}, RMSE={:.4}, MAPE={:.4}",
        metrics2.mae, metrics2.rmse, metrics2.mape
    );

    // Query all results
    let all = tracker.all();
    println!("\nAll tracked metrics: {} entries", all.len());

    let cpu_metrics = tracker.for_measurement("cpu_usage");
    println!("  cpu_usage metrics: {} entries", cpu_metrics.len());

    if let Some(latest) = tracker.latest("cpu_usage") {
        println!(
            "  Latest: model={}, MAPE={:.4}",
            latest.model_type, latest.mape
        );
    }

    // Check for stale measurements
    let stale = tracker.stale_measurements(&["cpu_usage", "disk_io", "network"]);
    println!("  Stale measurements (no data): {:?}", stale);

    // ── 4. Compute Accuracy (Standalone) ──────────────────────────
    println!("\n--- compute_accuracy() ---");
    let pred = vec![10.0, 20.0, 30.0, 40.0, 50.0];
    let act = vec![11.0, 19.0, 31.0, 39.0, 51.0];

    let acc = compute_accuracy(&pred, &act, "LinearRegression", "test_metric", "v2")
        .expect("compute_accuracy failed");
    println!(
        "  model={}, measurement={}, MAE={:.4}, RMSE={:.4}, MAPE={:.4}",
        acc.model_type, acc.measurement, acc.mae, acc.rmse, acc.mape
    );

    // ── 5. Anomaly Precision Tracker ──────────────────────────────
    println!("\n--- Anomaly Precision Tracker ---");

    let precision_tracker = AnomalyPrecisionTracker::new(0.8); // min 80% precision

    // Submit feedback for detected anomalies
    let feedback_items = vec![
        ("anomaly-001", FeedbackLabel::TruePositive, "alice"),
        ("anomaly-002", FeedbackLabel::TruePositive, "bob"),
        ("anomaly-003", FeedbackLabel::FalsePositive, "alice"),
        ("anomaly-004", FeedbackLabel::TruePositive, "carol"),
        ("anomaly-005", FeedbackLabel::FalsePositive, "bob"),
        ("anomaly-006", FeedbackLabel::TruePositive, "alice"),
        ("anomaly-007", FeedbackLabel::TruePositive, "carol"),
        ("anomaly-008", FeedbackLabel::Unknown, "dave"),
    ];

    for (id, label, principal) in feedback_items {
        precision_tracker.submit_feedback(AnomalyFeedback {
            anomaly_id: id.to_string(),
            measurement: "cpu_usage".to_string(),
            tags: BTreeMap::new(),
            timestamp: base_ts,
            label,
            feedback_at: base_ts + 3_600_000_000_000,
            principal: principal.to_string(),
        });
    }

    let stats = precision_tracker.precision();
    println!("Overall precision stats:");
    println!("  Total labeled: {}", stats.total_labeled);
    println!("  True positives: {}", stats.true_positives);
    println!("  False positives: {}", stats.false_positives);
    println!("  Precision: {:?}", stats.precision);
    println!("  Below threshold (80%): {}", stats.below_threshold);

    let cpu_stats = precision_tracker.precision_for_measurement("cpu_usage");
    println!("\ncpu_usage precision: {:?}", cpu_stats.precision);

    let fps = precision_tracker.false_positives();
    println!("False positive anomalies ({}):", fps.len());
    for fp in &fps {
        println!("  {} (reported by {})", fp.anomaly_id, fp.principal);
    }

    println!("\nTotal feedback entries: {}", precision_tracker.count());

    println!("\n✓ Continuous forecasting complete");
}
