#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Signal Triggers
//!
//! Demonstrates the trigger engine with all condition types, SQL-based
//! trigger management, delivery routing, and dead-letter queues.
//!
//! ```bash
//! cargo run --example signal_triggers
//! ```

use chronix::chronix_core::FieldValue;
use chronix::chronix_signal::{
    anomaly_threshold_trigger, execute_trigger_sql, forecast_deviation_trigger,
    ma_crossover_trigger, parse_trigger_sql, rate_of_change_trigger, DeadLetter, DeadLetterQueue,
    DeliveryRouter, EventTrigger, LogChannel, MetricChannel, ThresholdOp, TriggerCatalog,
    TriggerCondition, TriggerEngine,
};
use chronix::chronix_stream::CdcEvent;
use std::collections::BTreeMap;
use std::time::Duration;

fn main() {
    println!("=== Chronix Signal Triggers ===\n");

    // ── 1. Template Triggers ──────────────────────────────────────
    println!("--- Template Triggers ---");
    let engine = TriggerEngine::new();

    // Anomaly score threshold trigger
    let t1 = anomaly_threshold_trigger("anomaly-cpu", "CPU Anomaly Alert", "cpu_usage", 2.5);
    engine.register(t1).expect("Register anomaly trigger");
    println!("  Registered: anomaly-cpu (threshold > 2.5)");

    // Forecast deviation trigger
    let t2 = forecast_deviation_trigger(
        "forecast-mem",
        "Memory Forecast Deviation",
        "memory_usage",
        15.0,
    );
    engine.register(t2).expect("Register forecast trigger");
    println!("  Registered: forecast-mem (deviation > 15%)");

    // Moving average crossover trigger
    let t3 = ma_crossover_trigger(
        "ma-crossover",
        "MA Crossover Signal",
        "cpu_usage",
        5,  // short window
        20, // long window
    );
    engine.register(t3).expect("Register MA crossover trigger");
    println!("  Registered: ma-crossover (5/20 MA crossover)");

    // Rate of change trigger
    let t4 = rate_of_change_trigger(
        "roc-alert",
        "Rate of Change Alert",
        "network_throughput",
        50.0, // 50% change threshold
        10,   // window size
    );
    engine.register(t4).expect("Register RoC trigger");
    println!("  Registered: roc-alert (50% change over 10 points)");

    // ── 2. Custom Trigger with FieldThreshold ─────────────────────
    println!("\n--- Custom Field Threshold Trigger ---");
    let custom = EventTrigger::new(
        "high-temp",
        "High Temperature Alert",
        "temperature",
        TriggerCondition::FieldThreshold {
            field: "value".to_string(),
            op: ThresholdOp::Gt,
            value: 85.0,
        },
    )
    .with_cooldown(Duration::from_secs(60))
    .with_signal_type("temperature_alert")
    .with_severity_thresholds(80.0, 90.0)
    .with_delivery_targets(vec!["log".to_string(), "webhook".to_string()]);

    engine.register(custom).expect("Register custom trigger");
    println!("  Registered: high-temp (value > 85.0)");
    println!(
        "  Total triggers: {} ({:?})",
        engine.trigger_count(),
        engine.trigger_ids()
    );

    // ── 3. Process Events ─────────────────────────────────────────
    println!("\n--- Processing Events ---");

    // Simulate CDC events for temperature measurement
    let events = [
        make_cdc_event("temperature", 1_000, 70.0),
        make_cdc_event("temperature", 2_000, 75.0),
        make_cdc_event("temperature", 3_000, 88.0), // fires: 88 > 85
        // Also over the threshold, but inside `high-temp`'s 60 s cooldown —
        // one alert per incident is the point of the cooldown, so this one is
        // suppressed rather than duplicated.
        make_cdc_event("temperature", 4_000, 92.0),
        make_cdc_event("temperature", 5_000, 60.0), // normal
    ];

    // `process_event_collect` hands the signals back instead of pushing them
    // into the engine's sink, so the caller owns delivery. Accumulate them
    // here — asking the sink afterwards would report zero no matter how many
    // fired, which is what this example used to print one line after showing
    // a signal firing.
    let mut all_signals = Vec::new();
    for (i, event) in events.iter().enumerate() {
        let signals = engine.process_event_collect(event);
        if signals.is_empty() {
            println!("  Event {i}: no signals fired");
        } else {
            for signal in &signals {
                println!(
                    "  Event {i}: SIGNAL '{}' severity={:?} value={:.1}",
                    signal.trigger_name, signal.severity, signal.value
                );
            }
        }
        all_signals.extend(signals);
    }

    println!("\nTotal signals fired: {}", all_signals.len());
    assert_eq!(
        all_signals.len(),
        1,
        "one signal, the second suppressed by cooldown"
    );

    // ── 4. Enable/Disable Triggers ────────────────────────────────
    println!("\n--- Enable/Disable ---");
    engine
        .set_enabled("high-temp", false)
        .expect("Disable failed");
    println!("  Disabled: high-temp");

    if let Some(t) = engine.get_trigger("high-temp") {
        println!("  high-temp enabled: {}", t.enabled);
    }

    engine
        .set_enabled("high-temp", true)
        .expect("Enable failed");
    println!("  Re-enabled: high-temp");

    // Unregister
    engine.unregister("roc-alert").expect("Unregister failed");
    println!("  Unregistered: roc-alert");
    println!("  Remaining triggers: {}", engine.trigger_count());

    // ── 5. SQL Trigger Management ─────────────────────────────────
    println!("\n--- SQL Trigger Management ---");

    let sql_engine = TriggerEngine::new();

    // CREATE TRIGGER via SQL
    let create_sql =
        "CREATE TRIGGER disk_alert ON disk_usage WHEN value > 90 DELIVER log COOLDOWN INTERVAL '120s'";
    let stmt = parse_trigger_sql(create_sql).expect("Parse CREATE TRIGGER failed");
    let result = execute_trigger_sql(&sql_engine, &stmt).expect("Execute CREATE failed");
    println!("  SQL: {create_sql}");
    println!("  Result: {result:?}");

    // SHOW TRIGGERS
    let show_sql = "SHOW TRIGGERS";
    let show_stmt = parse_trigger_sql(show_sql).expect("Parse SHOW failed");
    let show_result = execute_trigger_sql(&sql_engine, &show_stmt).expect("Execute SHOW failed");
    println!("\n  SQL: {show_sql}");
    println!("  Result: {show_result:?}");

    // ALTER TRIGGER
    let alter_sql = "ALTER TRIGGER disk_alert DISABLE";
    let alter_stmt = parse_trigger_sql(alter_sql).expect("Parse ALTER failed");
    let alter_result = execute_trigger_sql(&sql_engine, &alter_stmt).expect("Execute ALTER failed");
    println!("\n  SQL: {alter_sql}");
    println!("  Result: {alter_result:?}");

    // DROP TRIGGER
    let drop_sql = "DROP TRIGGER disk_alert";
    let drop_stmt = parse_trigger_sql(drop_sql).expect("Parse DROP failed");
    let drop_result = execute_trigger_sql(&sql_engine, &drop_stmt).expect("Execute DROP failed");
    println!("\n  SQL: {drop_sql}");
    println!("  Result: {drop_result:?}");

    // ── 6. Trigger Catalog ────────────────────────────────────────
    println!("\n--- Trigger Catalog (Persistent) ---");
    let catalog = TriggerCatalog::new();

    catalog.insert(
        "cpu_alert",
        "CREATE TRIGGER cpu_alert ON cpu_usage WHEN value > 95 DELIVER TO log",
    );
    catalog.insert(
        "mem_alert",
        "CREATE TRIGGER mem_alert ON memory_usage WHEN value > 90 DELIVER TO log",
    );
    catalog.insert(
        "disk_alert",
        "CREATE TRIGGER disk_alert ON disk_usage WHEN value > 85 DELIVER TO log",
    );
    println!("  Catalog entries: {}", catalog.len());

    // Serialize to JSON
    let json = catalog.to_json().expect("Catalog to_json failed");
    println!("  Serialized catalog: {} bytes", json.len());

    // Deserialize
    let restored = TriggerCatalog::from_json(&json).expect("Catalog from_json failed");
    println!("  Restored catalog entries: {}", restored.len());

    for entry in restored.entries() {
        println!("    {}: {}", entry.name, entry.sql);
    }

    // Remove and verify
    catalog.remove("disk_alert");
    println!("  After removal: {} entries", catalog.len());

    // ── 7. Delivery Router ────────────────────────────────────────
    println!("\n--- Delivery Router ---");
    let router = DeliveryRouter::new();
    router.add_channel(Box::new(LogChannel));
    router.add_channel(Box::new(MetricChannel));
    println!("  Router configured with 2 channels");

    // Create a sample signal event and deliver
    if !all_signals.is_empty() {
        let delivered = router.deliver(&all_signals[0]);
        println!("  Delivered signal to {delivered} channel(s)");
    }

    // ── 8. Dead Letter Queue ──────────────────────────────────────
    println!("\n--- Dead Letter Queue ---");
    let dlq = DeadLetterQueue::new(100);
    println!("  DLQ created (max_size=100)");

    // Simulate a failed delivery
    if !all_signals.is_empty() {
        dlq.push(DeadLetter {
            event: all_signals[0].clone(),
            channel: "webhook".to_string(),
            error: "Connection refused".to_string(),
            attempts: 3,
        });
        println!("  Pushed 1 dead letter (DLQ len={})", dlq.len());

        let dead_letters = dlq.drain();
        println!("  Drained {} dead letters:", dead_letters.len());
        for dl in &dead_letters {
            println!(
                "    channel={}, error='{}', attempts={}",
                dl.channel, dl.error, dl.attempts
            );
        }
    }

    println!("\n✓ Signal triggers complete");
}

/// Helper to create a CDC event with a single float field.
fn make_cdc_event(measurement: &str, ts: i64, value: f64) -> CdcEvent {
    let mut fields = BTreeMap::new();
    fields.insert("value".to_string(), FieldValue::F64(value));
    CdcEvent::PointWritten {
        measurement: measurement.to_string(),
        tags: BTreeMap::new(),
        fields,
        timestamp: ts,
        seq: ts as u64,
    }
}
