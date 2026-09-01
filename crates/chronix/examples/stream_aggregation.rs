#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # CDC Stream and Continuous Aggregation
//!
//! Demonstrates Chronix's Change Data Capture (CDC) event bus and
//! the continuous aggregation engine that materialises real-time
//! downsampled views from the write stream.
//!
//! ```sh
//! cargo run -p chronix --example stream_aggregation
//! ```

use std::sync::Arc;
use std::time::Duration;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};
use chronix_streaming::cdc::{
    AggFunction, ContinuousAggregationConfig, ContinuousAggregationEngine, SubscriptionFilter,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;

    let config = ChronixConfig::builder().data_dir(dir.path()).build()?;

    let db = Arc::new(Chronix::open(config)?);

    // ── 1. Subscribe to CDC events with a filter ───────────────
    let mut subscription = db.subscribe(SubscriptionFilter::all().measurement("temperature"));
    println!("✅ Subscribed to CDC events for 'temperature'");

    // ── 2. Set up continuous aggregation ───────────────────────
    // Aggregate temperature into 10-second buckets
    let agg_config = ContinuousAggregationConfig {
        name: "temp_10s".into(),
        source_measurement: "temperature".into(),
        target_measurement: "temperature_10s_agg".into(),
        source_field: "value".into(),
        interval: Duration::from_secs(10),
        functions: vec![AggFunction::Mean, AggFunction::Min, AggFunction::Max],
        late_arrival_window: Duration::from_secs(5),
    };

    let mut agg_engine = ContinuousAggregationEngine::new(agg_config.clone())?;
    println!(
        "✅ Continuous aggregation configured: {} → {} (10s buckets)",
        agg_config.source_measurement, agg_config.target_measurement
    );

    // ── 3. Write data and process events ───────────────────────
    let base_ts = 1_700_000_000_000_000_000_i64;

    // Simulate 3 sensors writing at different timestamps
    let sensors = [
        ("sensor-A", 20.0_f64),
        ("sensor-B", 22.5),
        ("sensor-C", 18.0),
    ];

    println!("\n─── Writing 30 points (3 sensors × 10 readings) ───");
    for i in 0..10_i64 {
        for (sensor, base_temp) in &sensors {
            let key = SeriesKey::new(
                "temperature",
                tags! { "sensor" => *sensor, "room" => "lab-1" },
            )?;
            let temp = base_temp + (i as f64 * 0.5).sin() * 2.0;
            let point = Point::new(
                key,
                fields! { "value" => temp },
                base_ts + i * 2_000_000_000, // 2-second intervals
            )?;
            db.insert(&point)?;
        }
    }

    // ── 4. Drain CDC events and feed to aggregation engine ─────
    let mut events_received = 0;
    let mut bucket_results = Vec::new();

    // Process all available events (non-blocking)
    while let Ok(Some(event)) =
        tokio::time::timeout(Duration::from_millis(100), subscription.recv()).await
    {
        events_received += 1;
        // Feed each event to the aggregation engine
        let results = agg_engine.process_event(&event);
        bucket_results.extend(results);
    }

    println!("   CDC events received: {events_received}");

    // Flush remaining partial buckets
    let flushed = agg_engine.flush_all();
    bucket_results.extend(flushed);

    // ── 5. Display aggregated results ──────────────────────────
    println!("\n─── Continuous aggregation results ───");
    println!(
        "   Active buckets before flush: {}",
        agg_engine.active_bucket_count()
    );
    println!("   Finalized buckets: {}", bucket_results.len());

    for result in &bucket_results {
        println!(
            "\n   Bucket: {} ({}ns → {}ns)",
            result.target_measurement, result.bucket_start_ns, result.bucket_end_ns
        );
        println!("   Tags: {:?}", result.tags);
        println!("   Aggregation: {}", result.aggregation_name);
        for (func, value) in &result.values {
            println!("     {func:?}: {value:.2}");
        }
    }

    // ── 6. Write aggregated results back to Chronix ────────────
    println!("\n─── Writing aggregated results back to Chronix ───");
    for result in &bucket_results {
        // Convert HashMap tags to BTreeMap for SeriesKey
        let tag_map: std::collections::BTreeMap<String, String> = result
            .tags
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let key = SeriesKey::new(&result.target_measurement, tag_map)?;
        let mut field_map = std::collections::BTreeMap::new();
        for (func, value) in &result.values {
            field_map.insert(format!("{:?}", func), FieldValue::from(*value));
        }
        if !field_map.is_empty() {
            let point = Point::new(key, field_map, result.bucket_start_ns)?;
            db.insert(&point)?;
        }
    }

    // Verify with a query
    let plan = db
        .query()
        .measurement("temperature_10s_agg")
        .range(base_ts, base_ts + 30_000_000_000)
        .build()?;

    let batch = db.execute(&plan)?;
    println!(
        "   Aggregated rows written and queryable: {}",
        batch.num_rows()
    );
    if batch.num_rows() > 0 {
        arrow::util::pretty::print_batches(&[batch])?;
    }

    db.close()?;
    println!("\n✅ Done");

    Ok(())
}
