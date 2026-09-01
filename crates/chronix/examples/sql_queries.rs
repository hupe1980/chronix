#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # SQL Queries
//!
//! Demonstrates running SQL queries against Chronix using DataFusion.
//! Supports full SQL including `GROUP BY`, window functions, `ORDER BY`,
//! `LIMIT`, and built-in UDFs like `time_bucket()`, `rate()`, `first()`, `last()`.
//!
//! ```sh
//! cargo run -p chronix --example sql_queries
//! ```

use std::sync::Arc;

use chronix::prelude::*;
use chronix::sql::create_session_context;
use chronix::{fields, tags, Chronix};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;

    let config = ChronixConfig::builder().data_dir(dir.path()).build()?;

    let db = Arc::new(Chronix::open(config)?);

    // ── Seed realistic server metrics ──────────────────────────
    let base_ts = 1_700_000_000_000_000_000_i64;

    for (host, region) in [
        ("web-1", "us-east"),
        ("web-2", "eu-west"),
        ("db-1", "us-east"),
    ] {
        let key = SeriesKey::new(
            "system_metrics",
            tags! { "host" => host, "region" => region },
        )?;

        let points: Vec<Point> = (0..120)
            .map(|i| {
                let cpu = 30.0 + (i as f64 * 0.05).sin() * 20.0 + (i as f64) * 0.1;
                let mem = 60.0 + (i as f64 * 0.03).cos() * 10.0;
                Point::new(
                    key.clone(),
                    fields! {
                        "cpu_usage"    => cpu.min(100.0),
                        "memory_usage" => mem.min(100.0),
                        "disk_iops"    => 200.0 + (i as f64) * 3.0,
                    },
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
    println!("✅ Seeded 360 points (3 hosts × 120 samples at 1s)");

    // Create a DataFusion SessionContext wired to Chronix
    let ctx = create_session_context(db.clone());

    // ── 1. Simple SELECT ───────────────────────────────────────
    println!("\n─── 1. SELECT * LIMIT 5 ───");
    ctx.sql(
        "SELECT _time, host, cpu_usage, memory_usage
         FROM system_metrics
         ORDER BY _time
         LIMIT 5",
    )
    .await?
    .show()
    .await?;

    // ── 2. Filtering with WHERE ────────────────────────────────
    println!("\n─── 2. WHERE host = 'db-1' AND cpu_usage > 35 ───");
    ctx.sql(
        "SELECT _time, host, cpu_usage
         FROM system_metrics
         WHERE host = 'db-1' AND cpu_usage > 35.0
         ORDER BY _time
         LIMIT 10",
    )
    .await?
    .show()
    .await?;

    // ── 3. Aggregation ─────────────────────────────────────────
    println!("\n─── 3. Per-host aggregates ───");
    ctx.sql(
        "SELECT host,
                COUNT(*) AS total_points,
                ROUND(AVG(cpu_usage), 2)  AS avg_cpu,
                ROUND(MAX(cpu_usage), 2)  AS peak_cpu,
                ROUND(AVG(memory_usage), 2) AS avg_mem
         FROM system_metrics
         GROUP BY host
         ORDER BY avg_cpu DESC",
    )
    .await?
    .show()
    .await?;

    // ── 4. Time-bucketed downsampling ──────────────────────────
    println!("\n─── 4. 30-second time buckets (avg CPU) ───");
    ctx.sql(
        "SELECT time_bucket('30s', _time) AS bucket,
                host,
                ROUND(AVG(cpu_usage), 2) AS avg_cpu
         FROM system_metrics
         GROUP BY bucket, host
         ORDER BY bucket, host
         LIMIT 12",
    )
    .await?
    .show()
    .await?;

    // ── 5. Window function: running average ────────────────────
    println!("\n─── 5. Running average (window function) ───");
    ctx.sql(
        "SELECT _time, host, cpu_usage,
                ROUND(AVG(cpu_usage) OVER (
                    PARTITION BY host
                    ORDER BY _time
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ), 2) AS rolling_avg_5
         FROM system_metrics
         WHERE host = 'web-1'
         ORDER BY _time
         LIMIT 10",
    )
    .await?
    .show()
    .await?;

    // ── 6. Cross-host comparison ───────────────────────────────
    println!("\n─── 6. Region-level summary ───");
    ctx.sql(
        "SELECT region,
                COUNT(DISTINCT host) AS hosts,
                ROUND(AVG(cpu_usage), 2) AS avg_cpu,
                ROUND(AVG(disk_iops), 1) AS avg_iops
         FROM system_metrics
         GROUP BY region
         ORDER BY region",
    )
    .await?
    .show()
    .await?;

    db.close()?;
    println!("\n✅ Done");

    Ok(())
}
