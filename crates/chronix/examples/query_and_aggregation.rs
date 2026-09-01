#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Queries and Aggregations
//!
//! Demonstrates the fluent QueryBuilder with aggregation, downsampling,
//! and pruning statistics.
//!
//! ```sh
//! cargo run -p chronix --example query_and_aggregation
//! ```

use std::time::Duration;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;

    let config = ChronixConfig::builder().data_dir(dir.path()).build()?;

    let db = Chronix::open(config)?;

    // ── Seed data: 3 hosts × 60 points (1 minute at 1s resolution) ──
    let base_ts = 1_700_000_000_000_000_000_i64;

    let hosts = [
        ("web-01", "us-east"),
        ("web-02", "us-east"),
        ("db-01", "eu-west"),
    ];

    for (host, region) in &hosts {
        let key = SeriesKey::new(
            "http_requests",
            tags! { "host" => *host, "region" => *region },
        )?;

        let points: Vec<Point> = (0..60)
            .map(|i| {
                let value = 100.0 + (i as f64).sin() * 50.0;
                Point::new(
                    key.clone(),
                    fields! { "count" => value, "latency_ms" => value * 0.1 },
                    base_ts + i * 1_000_000_000,
                )
                .unwrap()
            })
            .collect();

        assert!(
            db.insert_batch(&points)?.is_complete(),
            "insert was partial"
        );
    }
    println!("✅ Seeded 180 points (3 hosts × 60 samples)");

    // ── 1. Simple scan with tag filter ─────────────────────────
    println!("\n─── 1. Scan: http_requests where host=web-01 ───");
    let plan = db
        .query()
        .measurement("http_requests")
        .tag("host", "web-01")
        .field("count")
        .range(base_ts, base_ts + 10_000_000_000)
        .build()?;

    let batch = db.execute(&plan)?;
    println!("   {} rows", batch.num_rows());
    arrow::util::pretty::print_batches(&[batch])?;

    // ── 2. Aggregation: avg, min, max ──────────────────────────
    println!("\n─── 2. Aggregate: avg/min/max of count for web-01 ───");
    let plan = db
        .query()
        .measurement("http_requests")
        .tag("host", "web-01")
        .field("count")
        .range(base_ts, base_ts + 60_000_000_000)
        .aggregate(AggFn::Avg)
        .aggregate(AggFn::Min)
        .aggregate(AggFn::Max)
        .build()?;

    let batch = db.execute(&plan)?;
    arrow::util::pretty::print_batches(&[batch])?;

    // ── 3. Downsampling: 10-second buckets ─────────────────────
    println!("\n─── 3. Downsample: 10s buckets, avg(count) for db-01 ───");
    let plan = db
        .query()
        .measurement("http_requests")
        .tag("host", "db-01")
        .field("count")
        .range(base_ts, base_ts + 60_000_000_000)
        .downsample(Duration::from_secs(10), AggFn::Avg)
        .build()?;

    let batch = db.execute(&plan)?;
    println!("   {} buckets", batch.num_rows());
    arrow::util::pretty::print_batches(&[batch])?;

    // ── 4. Pruning stats ───────────────────────────────────────
    //
    // Pruning happens over *segments*, so a database whose data is still in
    // the memtable prunes nothing and reports zeros — which is what this
    // section used to print, making the whole pipeline look inert.
    //
    // Flush what we have, then write a second batch a day later and flush
    // again: two segments in different time windows is the smallest shape in
    // which "pruned by time" can be anything but zero.
    db.flush()?;

    let day_later = base_ts + 86_400_000_000_000;
    let key = SeriesKey::new(
        "http_requests",
        tags! { "host" => "web-01", "region" => "us-east" },
    )?;
    let later: Vec<Point> = (0..60)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "count" => 42.0, "latency_ms" => 4.2 },
                day_later + i * 1_000_000_000,
            )
            .unwrap()
        })
        .collect();
    assert!(db.insert_batch(&later)?.is_complete());
    db.flush()?;

    println!("\n─── 4. Execute with pruning statistics ───");
    let plan = db
        .query()
        .measurement("http_requests")
        .tag("host", "web-02")
        .range(base_ts, base_ts + 30_000_000_000)
        .build()?;

    let (batch, stats) = db.execute_with_stats(&plan)?;
    println!("   Rows returned      : {}", batch.num_rows());
    println!("   Segments total     : {}", stats.segments_total);
    println!("   Pruned by time     : {}", stats.pruned_by_time);
    println!("   Pruned by bloom    : {}", stats.pruned_by_bloom);
    println!("   Pruned by stats    : {}", stats.pruned_by_stats);
    println!("   Segments remaining : {}", stats.segments_remaining);
    println!("   Total pruned       : {}", stats.total_pruned());

    db.close()?;
    println!("\n✅ Done");

    Ok(())
}
