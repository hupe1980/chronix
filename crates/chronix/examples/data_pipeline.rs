#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Data Pipeline
//!
//! Demonstrates the Chronix pipeline engine: CDC-driven real-time
//! processing with triggers, alerts, streaming anomaly detection,
//! and continuous forecasting.
//!
//! ```sh
//! cargo run -p chronix --example data_pipeline
//! ```

use std::sync::Arc;
use std::time::Duration;

use chronix::chronix_analytics::forecast::ModelType;
use chronix::chronix_analytics::ContinuousForecastConfig;
use chronix::prelude::*;
use chronix::{fields, tags, Chronix, Pipeline, PipelineConfig};
use chronix_security::audit::{AuditAction, AuditDecision, AuditEvent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;

    let config = ChronixConfig::builder().data_dir(dir.path()).build()?;

    let db = Arc::new(Chronix::open(config)?);

    // ── 1. Create and configure the pipeline ───────────────────
    let pipeline_config = PipelineConfig {
        signal_store_capacity: 10_000,
        enable_log_delivery: true,
        enable_metric_delivery: true,
        audit_memory_capacity: 1_000,
        forecast_horizon: 5,

        ..PipelineConfig::default()
    };

    let pipeline = Arc::new(Pipeline::with_config(pipeline_config));
    println!("✅ Pipeline created");

    // ── 2. Set up a trigger via Signal SQL ──────────────────────
    println!("\n─── 2. Creating triggers via Signal SQL ───");

    let create_result = pipeline
        .execute_signal_sql("CREATE TRIGGER high_cpu ON cpu_usage WHEN value > 90.0 FOR 1m");
    println!("   CREATE TRIGGER result: {create_result:?}");

    let show_result = pipeline.execute_signal_sql("SHOW TRIGGERS");
    println!("   SHOW TRIGGERS result: {show_result:?}");

    // ── 2b. Enable continuous forecasting ───────────────────────
    //
    // The forecast cache is filled by the CDC path, and only for a
    // measurement that has a model enabled — without this the pipeline is
    // wired correctly and the cache stays empty, which is what this example
    // used to demonstrate.
    pipeline.forecast_engine().enable(
        ContinuousForecastConfig::new("cpu_usage", ModelType::Ses)
            .with_field("value")
            .with_min_fit_points(20)
            .with_update_interval(20),
    )?;
    println!("   Continuous forecast enabled for cpu_usage (SES, refit every 20 points)");

    // ── 3. Spawn CDC listener (connects pipeline to DB events) ──
    let listener_handle = pipeline.spawn_cdc_listener(db.event_bus());
    println!("\n✅ CDC listener spawned");

    // ── 4. Write data that should trigger anomalies ────────────
    let base_ts = 1_700_000_000_000_000_000_i64;

    println!("\n─── 4. Writing normal + anomalous data ───");

    // Normal readings
    let key = SeriesKey::new("cpu_usage", tags! { "host" => "prod-1", "dc" => "us-east" })?;

    for i in 0..50 {
        let value = 45.0 + (i as f64 * 0.1).sin() * 10.0; // 35-55 range
        let point = Point::new(
            key.clone(),
            fields! { "value" => value },
            base_ts + i * 1_000_000_000,
        )?;
        db.insert(&point)?;
    }
    println!("   Wrote 50 normal points (35-55 range)");

    // Anomalous spike
    for i in 50..55 {
        let point = Point::new(
            key.clone(),
            fields! { "value" => 98.0 + (i - 50) as f64 }, // way above normal
            base_ts + i * 1_000_000_000,
        )?;
        db.insert(&point)?;
    }
    println!("   Wrote 5 anomalous points (98-103 range)");

    // Give the CDC listener a moment to process events
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ── 5. Check pipeline state ────────────────────────────────
    println!("\n─── 5. Pipeline state ───");
    println!("   Signals in store: {}", pipeline.signal_store().len());
    println!(
        "   Trigger engine rules: {}",
        pipeline.trigger_engine().trigger_count()
    );

    // Check the forecast cache
    let forecast_cache = pipeline.forecast_cache();
    println!("   Forecasts cached: {}", forecast_cache.len());

    // ── 6. Query audit log ─────────────────────────────────────
    //
    // The pipeline does not audit on your behalf: `Pipeline::audit` is where
    // an application records the decisions *it* made, so that the trail
    // reflects the application's semantics rather than the database's.
    println!("\n─── 6. Audit trail ───");
    for signal in pipeline.signal_store().all() {
        pipeline.audit(
            AuditEvent::new(
                "pipeline",
                AuditAction::DetectAnomalies,
                format!("cpu_usage/{}", signal.trigger_name),
                AuditDecision::Allow,
            )
            .with_metadata("value", format!("{:.1}", signal.value)),
        );
    }
    if let Some(audit_sink) = pipeline.audit_sink() {
        let events = audit_sink.events();
        println!("   Audit events: {}", events.len());
        for event in events.iter().take(3) {
            println!("   - {:?}", event);
        }
    }

    // ── 7. Cleanup ─────────────────────────────────────────────
    listener_handle.abort();
    db.close()?;
    println!("\n✅ Done");

    Ok(())
}
