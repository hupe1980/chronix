#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Delete Operations
//!
//! Demonstrates time-series deletion: single-series tombstones,
//! predicate-based deletes, re-creating a deleted series, and measurement
//! drops.
//!
//! The row counts are asserted rather than printed. An example that only
//! prints cannot fail, and this one used to print a plausible number while a
//! ranged delete removed the whole series.
//!
//! ```sh
//! cargo run -p chronix --example delete_operations
//! ```

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;

    let config = ChronixConfig::builder().data_dir(dir.path()).build()?;

    let db = Chronix::open(config)?;

    // ── Seed data ──────────────────────────────────────────────
    let base_ts = 1_700_000_000_000_000_000_i64;

    let hosts = ["web-1", "web-2", "web-3", "db-1", "db-2"];
    for host in &hosts {
        let key = SeriesKey::new("requests", tags! { "host" => *host, "env" => "production" })?;
        let points: Vec<Point> = (0..20)
            .map(|i| {
                Point::new(
                    key.clone(),
                    fields! { "count" => (i as f64) * 100.0 },
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
    println!("✅ Seeded 100 points (5 hosts × 20 samples)");

    // Count before deletes
    let plan = db
        .query()
        .measurement("requests")
        .range(base_ts, base_ts + 100_000_000_000)
        .build()?;
    let batch = db.execute(&plan)?;
    let before = batch.num_rows();
    assert_eq!(before, 100, "5 hosts x 20 samples");
    println!("   Total rows before: {before}\n");

    // ── 1. Delete a single series by exact tags ────────────────
    println!("─── 1. Delete series: requests{{host=web-3}} ───");
    let tags_map = tags! { "host" => "web-3", "env" => "production" };
    db.delete_series("requests", &tags_map)?;
    println!("   Deleted");

    let batch = db.execute(&plan)?;
    assert_eq!(batch.num_rows(), 80, "one whole series (20 points) removed");
    println!("   Rows after: {}\n", batch.num_rows());

    // ── 2. Predicate-based delete with time range ──────────────
    println!("─── 2. Predicate delete: host=db-1, first 10 seconds ───");
    let delete_req = db
        .delete_builder()
        .measurement("requests")
        .tag("host", "db-1")
        .range(base_ts, base_ts + 10_000_000_000)
        .build()?;

    let count = db.execute_delete(&delete_req)?;
    assert!(count.is_complete(), "every matching segment was scanned");
    println!("   Series tombstoned: {}", count.series_tombstoned);

    let batch = db.execute(&plan)?;
    // The window [base, base+10s] is inclusive at both ends, so it covers the
    // samples at offsets 0..=10 — eleven of db-1's twenty points. The other
    // nine survive: a ranged delete deletes its range and nothing else.
    assert_eq!(batch.num_rows(), 69, "only db-1's first 11 points are gone");
    println!("   Rows after: {}\n", batch.num_rows());

    // ── 3. Delete by tag only (all time) ───────────────────────
    println!("─── 3. Delete all db-2 series ───");
    let delete_req = db
        .delete_builder()
        .measurement("requests")
        .tag("host", "db-2")
        .build()?;

    let count = db.execute_delete(&delete_req)?;
    println!("   Series tombstoned: {}", count.series_tombstoned);

    let batch = db.execute(&plan)?;
    assert_eq!(batch.num_rows(), 49, "db-2's 20 points are gone");
    println!("   Rows after: {}\n", batch.num_rows());

    // ── 4. Re-create a deleted series ──────────────────────────
    //
    // An unbounded delete removes the data that exists; it is not a standing
    // order against the identifier. Writing to web-3 again brings it back,
    // which is what re-provisioning a device under a recycled name looks like.
    println!("─── 4. Write to the deleted series web-3 again ───");
    let key = SeriesKey::new(
        "requests",
        tags! { "host" => "web-3", "env" => "production" },
    )?;
    db.insert(&Point::new(
        key,
        fields! { "count" => 999.0_f64 },
        base_ts + 50_000_000_000,
    )?)?;

    let plan_wide = db
        .query()
        .measurement("requests")
        .range(base_ts, base_ts + 100_000_000_000)
        .build()?;
    let batch = db.execute(&plan_wide)?;
    assert_eq!(
        batch.num_rows(),
        50,
        "the new point is visible — the delete covered the old data, not the name"
    );
    println!("   Rows after re-write: {}\n", batch.num_rows());

    // ── 5. Drop entire measurement ─────────────────────────────
    // First create a temporary measurement
    let key = SeriesKey::new("temp_metrics", tags! { "test" => "yes" })?;
    db.insert(&Point::new(key, fields! { "value" => 42.0_f64 }, base_ts)?)?;

    println!("─── 5. Drop measurement 'temp_metrics' ───");
    let has_before = db.schema("temp_metrics").is_some();
    db.drop_measurement("temp_metrics")?;
    let has_after = db.schema("temp_metrics").is_some();
    assert!(has_before && !has_after, "the drop removed the schema");
    println!("   Schema existed before: {has_before}, after: {has_after}");

    db.close()?;
    println!("\n✅ Done");

    Ok(())
}
