#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Alerting and Streaming Anomaly Detection
//!
//! Demonstrates the `StreamingAnomalyEngine` for real-time per-point
//! anomaly scoring and the `AlertEngine` for threshold-based alerts
//! with cooldown and action dispatch.
//!
//! ```sh
//! cargo run -p chronix --example alerting
//! ```

use std::collections::BTreeMap;
use std::time::Duration;

use chronix::chronix_analytics::anomaly::DetectorType;
use chronix::chronix_analytics::{
    AlertAction, AlertConfig, AlertEngine, StreamingAnomalyConfig, StreamingAnomalyEngine,
};
use chronix::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── 1. Configure streaming anomaly detector ────────────────
    println!("─── 1. Streaming Anomaly Engine ───");
    let anomaly_engine = StreamingAnomalyEngine::new();

    anomaly_engine.enable(
        StreamingAnomalyConfig::new("cpu", DetectorType::ZScore, 2.5)
            .with_field("usage")
            .with_min_fit_points(20)
            .with_max_buffer_size(5000),
    )?;
    println!("   Enabled Z-Score detector for cpu.usage (threshold=2.5)");

    // ── 2. Configure alert engine ──────────────────────────────
    println!("\n─── 2. Alert Engine ───");
    let alert_engine = AlertEngine::new();

    alert_engine.register(
        AlertConfig::new("cpu_critical", "cpu", 0.8)
            .with_cooldown(Duration::from_secs(60))
            .with_actions(vec![AlertAction::Log, AlertAction::Metric]),
    );

    alert_engine.register(
        AlertConfig::new("cpu_warning", "cpu", 0.5)
            .with_cooldown(Duration::from_secs(30))
            .with_action(AlertAction::Log),
    );

    println!("   Registered {} alert rules", alert_engine.alert_count());

    // ── 3. Simulate normal + anomalous writes ──────────────────
    println!("\n─── 3. Processing writes ───");
    let base_ts = 1_700_000_000_000_000_000_i64;

    let tags: BTreeMap<String, String> = [("host".into(), "web-1".into())].into_iter().collect();

    // Feed 40 normal points to build baseline
    println!("   Phase 1: 40 normal points (20-30 range)...");
    for i in 0..40 {
        let value = 25.0 + (i as f64 * 0.5).sin() * 5.0;
        let mut fields = BTreeMap::new();
        fields.insert("usage".into(), FieldValue::from(value));

        let scored =
            anomaly_engine.process_write("cpu", &tags, &fields, base_ts + i * 1_000_000_000);

        if let Some(anomaly) = scored {
            if anomaly.score.is_anomaly {
                println!(
                    "     ⚠️  ts={} value={:.1} score={:.3}",
                    anomaly.score.timestamp, anomaly.score.value, anomaly.score.score
                );
            }
        }
    }

    // Inject spikes
    println!("   Phase 2: 5 anomalous spikes (90-110 range)...");
    let mut total_alerts = Vec::new();

    for j in 0..5 {
        let i = 40 + j;
        let spike_value = 95.0 + j as f64 * 4.0;
        let mut fields = BTreeMap::new();
        fields.insert("usage".into(), FieldValue::from(spike_value));
        let ts = base_ts + i * 1_000_000_000;

        let scored = anomaly_engine.process_write("cpu", &tags, &fields, ts);

        if let Some(ref anomaly) = scored {
            let icon = if anomaly.score.is_anomaly {
                "⚠️ "
            } else {
                "  "
            };
            println!(
                "     {icon}ts={ts} value={spike_value:.1} score={:.3} anomaly={}",
                anomaly.score.score, anomaly.score.is_anomaly
            );

            // Feed anomaly score to alert engine
            let fired = alert_engine.evaluate("cpu", &tags, anomaly.score.score, ts);
            total_alerts.extend(fired);
        }
    }

    // Recovery
    println!("   Phase 3: 5 recovery points (25 range)...");
    for k in 0..5 {
        let i = 45 + k;
        let mut fields = BTreeMap::new();
        fields.insert("usage".into(), FieldValue::from(25.0));

        anomaly_engine.process_write("cpu", &tags, &fields, base_ts + i * 1_000_000_000);
    }

    // ── 4. Alert results ───────────────────────────────────────
    println!("\n─── 4. Fired alerts ───");
    println!("   Total alerts fired: {}", total_alerts.len());
    for alert in &total_alerts {
        println!(
            "   🔔 rule={} measurement={} score={:.3} threshold={:.2} ts={}",
            alert.alert_id, alert.measurement, alert.score, alert.threshold, alert.timestamp
        );
    }

    // ── 5. Alert history ───────────────────────────────────────
    println!("\n─── 5. Alert history ───");
    let history = alert_engine.history();
    println!("   History entries: {}", history.len());

    // ── 6. Disable / re-enable detector ────────────────────────
    println!("\n─── 6. Engine management ───");
    println!(
        "   cpu detector enabled: {}",
        anomaly_engine.is_enabled("cpu")
    );
    anomaly_engine.disable("cpu");
    println!("   After disable: {}", anomaly_engine.is_enabled("cpu"));

    alert_engine.unregister("cpu_warning");
    println!(
        "   Alert rules after unregister: {}",
        alert_engine.alert_count()
    );

    println!("\n✅ Done");
    Ok(())
}
