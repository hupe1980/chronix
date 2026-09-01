#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Basic Usage
//!
//! Shows how to open a Chronix database, insert points, and read them back.
//!
//! ```sh
//! cargo run -p chronix --example basic_usage
//! ```

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    println!("📂 Data directory: {}", dir.path().display());

    // ── 1. Open the database ───────────────────────────────────
    let config = ChronixConfig::builder().data_dir(dir.path()).build()?;

    let db = Chronix::open(config)?;
    println!("✅ Database opened");

    // ── 2. Insert individual points ────────────────────────────
    let now = 1_700_000_000_000_000_000_i64; // 2023-11-14T22:13:20Z

    let key = SeriesKey::new(
        "cpu",
        tags! {
            "host"   => "server-01",
            "region" => "us-east",
        },
    )?;

    for i in 0..5 {
        let point = Point::new(
            key.clone(),
            fields! {
                "usage_idle"   => 95.5 - (i as f64) * 2.0,
                "usage_system" => 1.2 + (i as f64) * 0.5,
            },
            now + i * 1_000_000_000, // 1-second intervals
        )?;
        db.insert(&point)?;
    }
    println!("✅ Inserted 5 CPU points for server-01");

    // ── 3. Batch insert (more efficient) ───────────────────────
    let key2 = SeriesKey::new(
        "cpu",
        tags! {
            "host"   => "server-02",
            "region" => "eu-west",
        },
    )?;

    let batch: Vec<Point> = (0..5)
        .map(|i| {
            Point::new(
                key2.clone(),
                fields! {
                    "usage_idle"   => 80.0 + (i as f64),
                    "usage_system" => 3.0 - (i as f64) * 0.2,
                },
                now + i * 1_000_000_000,
            )
            .unwrap()
        })
        .collect();

    assert!(db.insert_batch(&batch)?.is_complete(), "insert was partial");
    println!("✅ Batch-inserted 5 CPU points for server-02");

    // ── 4. Read the schema ─────────────────────────────────────
    if let Some(schema) = db.schema("cpu") {
        println!("\n📊 Schema for 'cpu':");
        for col in schema.columns() {
            println!("   {} ({:?}, {:?})", col.name, col.role, col.column_type);
        }
    }

    // ── 5. Query with the fluent QueryBuilder ──────────────────
    let plan = db
        .query()
        .measurement("cpu")
        .tag("host", "server-01")
        .field("usage_idle")
        .range(now, now + 10_000_000_000)
        .build()?;

    let batch = db.execute(&plan)?;
    println!(
        "\n🔍 Query result for server-01/usage_idle: {} rows",
        batch.num_rows()
    );
    arrow::util::pretty::print_batches(&[batch])?;

    // ── 6. last_value — fast latest-point lookup ───────────────
    let tags_map = tags! { "host" => "server-02", "region" => "eu-west" };
    if let Some(latest) = db.last_value("cpu", &tags_map)? {
        println!(
            "\n⏱  Last value for server-02: ts={}, fields={:?}",
            latest.timestamp(),
            latest.fields()
        );
    }

    // ── 7. Clean shutdown ──────────────────────────────────────
    db.close()?;
    println!("\n✅ Database closed cleanly");

    Ok(())
}
