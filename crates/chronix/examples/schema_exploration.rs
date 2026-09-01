#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Schema Exploration
//!
//! Demonstrates schema introspection: listing measurements, tag keys,
//! field keys, column definitions, and the schema registry.
//!
//! ```sh
//! cargo run -p chronix --example schema_exploration
//! ```

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let config = ChronixConfig::builder().data_dir(dir.path()).build()?;
    let db = Chronix::open(config)?;

    // ── Seed several measurements with different schemas ───────
    let ts = 1_700_000_000_000_000_000_i64;

    // Measurement 1: cpu
    let key = SeriesKey::new("cpu", tags! { "host" => "web-1", "region" => "us-east" })?;
    db.insert(&Point::new(
        key.clone(),
        fields! { "usage_user" => 45.2, "usage_system" => 12.1, "usage_idle" => 42.7 },
        ts,
    )?)?;

    // Measurement 2: memory
    let key = SeriesKey::new("memory", tags! { "host" => "web-1", "type" => "physical" })?;
    db.insert(&Point::new(
        key.clone(),
        fields! { "total_bytes" => 17179869184.0, "used_bytes" => 8589934592.0, "cached_bytes" => 2147483648.0 },
        ts,
    )?)?;

    // Measurement 3: disk
    let key = SeriesKey::new(
        "disk",
        tags! { "host" => "web-1", "device" => "sda1", "fstype" => "ext4" },
    )?;
    db.insert(&Point::new(
        key,
        fields! { "read_iops" => 1200.0, "write_iops" => 340.0 },
        ts,
    )?)?;

    println!("✅ Seeded 3 measurements: cpu, memory, disk\n");

    // ── 1. List all measurements ───────────────────────────────
    println!("─── 1. All measurements ───");
    let names = db.schema_registry().measurement_names();
    println!("   Count: {}", names.len());
    for name in &names {
        println!("   • {name}");
    }

    // ── 2. Per-measurement schema details ──────────────────────
    println!("\n─── 2. Schema details ───");
    for name in &names {
        if let Some(schema) = db.schema(name) {
            println!("\n   📊 {name}");
            println!(
                "      Tags   ({}): {:?}",
                schema.tag_count(),
                schema.tag_names()
            );
            println!(
                "      Fields ({}): {:?}",
                schema.field_count(),
                schema.field_names()
            );
            println!("      Columns:");
            for col in schema.columns() {
                println!(
                    "         {:<20} role={:<12?} type={:?}",
                    col.name, col.role, col.column_type
                );
            }
        }
    }

    // ── 3. Look up a specific column ───────────────────────────
    println!("\n─── 3. Column lookup: cpu.usage_user ───");
    if let Some(schema) = db.schema("cpu") {
        match schema.column("usage_user") {
            Some(col) => println!(
                "   Found: {} ({:?}, {:?})",
                col.name, col.role, col.column_type
            ),
            None => println!("   Not found"),
        }
        // Non-existent column
        match schema.column("does_not_exist") {
            Some(_) => println!("   Unexpected!"),
            None => println!("   'does_not_exist' → None (correct)"),
        }
    }

    // ── 4. Schema registry stats ─────────────────────────────
    println!("\n─── 4. Registry stats ───");
    println!(
        "   Total measurements: {}",
        db.schema_registry().measurement_count()
    );

    // ── 5. Tag index cardinality ──────────────────────────────
    println!("\n─── 5. Adding more series and checking cardinality ───");
    for i in 0..5 {
        let key = SeriesKey::new(
            "cpu",
            tags! { "host" => format!("web-{i}"), "region" => "us-east" },
        )?;
        db.insert(&Point::new(
            key,
            fields! { "usage_user" => 50.0 + i as f64 },
            ts + i * 1_000_000_000,
        )?)?;
    }

    let plan = db
        .query()
        .measurement("cpu")
        .range(ts, ts + 10_000_000_000)
        .build()?;

    let batch = db.execute(&plan)?;
    println!("   cpu series across all hosts: {} rows", batch.num_rows());
    arrow::util::pretty::print_batches(&[batch])?;

    db.close()?;
    println!("\n✅ Done");
    Ok(())
}
