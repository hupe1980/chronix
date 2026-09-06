#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Compaction, Retention, and Rollups
//!
//! Demonstrates storage lifecycle management: manual flush, compaction,
//! rollup materialisation, and retention enforcement.
//!
//! ```sh
//! cargo run -p chronix --example storage_lifecycle
//! ```

use std::sync::Arc;
use std::time::Duration;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix, ParquetExportConfig, RollupAggFn, RollupBuilder};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;

    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .shard_duration(Duration::from_secs(3600)) // 1-hour shards
        .retention(Some(Duration::from_secs(86400 * 7))) // 7-day retention
        .compression(CompressionCodec::Lz4)
        .build()?;

    let db = Arc::new(Chronix::open(config)?);

    // ── 1. Seed data across multiple shards ────────────────────
    //
    // Timestamps are relative to the wall clock, not hardcoded: retention is
    // evaluated against "now", so a fixed epoch would silently age past the
    // retention window and make this example delete all of its own data.
    let hour_ns = 3_600_000_000_000_i64;
    let now_ns = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos(),
    )
    .unwrap_or(i64::MAX);

    // Three hourly shards: two old ones (5 h and 4 h ago) and the current
    // one. Points must land within the engine's out-of-order tolerance
    // (±2 shards of the newest write), so they are seeded oldest-first —
    // and the gap is deliberate: once "now" is written, the two old shards
    // are outside the window, which is what makes their rollup buckets
    // final and lets retention drop them.
    let shard_offsets = [-5, -4, 0];
    let recent_start = now_ns - 5 * hour_ns;

    let key = SeriesKey::new(
        "network_traffic",
        tags! { "interface" => "eth0", "host" => "router-1" },
    )?;

    println!("─── 1. Seeding data across 3 hourly shards ───");
    for offset in shard_offsets {
        let points: Vec<Point> = (0..100)
            .map(|i| {
                let ts = now_ns + offset * hour_ns + i * 1_000_000_000;
                Point::new(
                    key.clone(),
                    fields! {
                        "bytes_in"  => 1000.0 + (i as f64) * 10.0,
                        "bytes_out" => 500.0 + (i as f64) * 5.0,
                    },
                    ts,
                )
                .unwrap()
            })
            .collect();
        // A batch insert returns `Ok` even when individual points are
        // rejected (e.g. too far out of order) — always check the result.
        let res = db.insert_batch(&points)?;
        assert!(
            res.is_complete(),
            "seed insert was partial: {:?}",
            res.rejected
        );
    }
    println!("   300 points across 3 shards");

    // ── 2. Manual flush: memtable → segments ───────────────────
    println!("\n─── 2. Flushing memtable to durable segments ───");
    let flush_results = db.flush()?;
    println!("   Flushed {} results", flush_results.len());
    for res in &flush_results {
        println!(
            "   Measurement '{}': {} points flushed",
            res.measurement, res.points_flushed
        );
    }

    // ── 3. Rollups: materialised downsampled tiers ──────────────
    println!("\n─── 3. Creating rollup: 1-minute aggregation ───");
    let rollup_config = RollupBuilder::new()
        .name("net_1m")
        .source("network_traffic")
        .target("network_traffic_1m")
        .every("1m") // 1 minute
        .aggregation(RollupAggFn::Avg)
        .aggregation(RollupAggFn::Max)
        .aggregation(RollupAggFn::Sum)
        .group_by("host")
        .group_by("interface")
        .build()?;

    db.create_rollup(rollup_config)?;

    let rollups = db.list_rollups()?;
    for r in &rollups {
        println!(
            "   Rollup '{}': {} → {} (every {}, {:?})",
            r.name, r.source_measurement, r.target_measurement, r.bucket, r.aggregations
        );
    }

    // ── 4. Compaction — and rollup materialisation ─────────────
    // A rollup bucket is aggregated exactly once, over every row that can
    // ever reach it, as soon as the out-of-order window has closed over
    // it. `compact()` materialises after its pass (so does retention, and
    // `materialise_rollups()` is the explicit call). The two old shards
    // are final; the current one is not, so its buckets wait.
    println!("\n─── 4. Running compaction (materialises final rollup buckets) ───");
    let compaction_tasks = db.compact()?;
    println!("   Compaction tasks: {compaction_tasks}");
    let rollup_rows = db.sql(
        "SELECT count(*) AS buckets, min(bytes_in_avg) AS lo, max(bytes_in_avg) AS hi \
         FROM network_traffic_1m",
    )?;
    println!("   Materialised 1-minute buckets (two final shards):");
    println!(
        "{}",
        arrow::util::pretty::pretty_format_batches(&rollup_rows)?
    );

    // ── 5. Retention enforcement ───────────────────────────────
    println!("\n─── 5. Retention enforcement ───");
    // A 3-hour window: the two old shards fall outside it. They are
    // dropped only because their rollup is materialised past them — raw
    // data that still feeds an unmaterialised bucket is preserved.
    // (Retention is evaluated against the wall clock, which is why the
    // data above is seeded relative to `now`.)
    let retention_result = db.enforce_retention(Duration::from_secs(3 * 3600))?;
    println!("   Shards dropped   : {}", retention_result.shards_dropped);
    println!(
        "   Segments deleted  : {}",
        retention_result.segments_deleted
    );
    println!("   Bytes freed       : {}", retention_result.bytes_freed);

    // ── 6. Warm tier migration ─────────────────────────────────
    println!("\n─── 6. Exporting query results to Parquet ───");
    let plan = db
        .query()
        .measurement("network_traffic")
        .field("bytes_in")
        .range(recent_start, now_ns + hour_ns)
        .build()?;

    let export_path = dir.path().join("export.parquet");
    let export = db.export_parquet(
        &plan,
        &export_path,
        &ParquetExportConfig {
            compression: chronix::export::ParquetCompression::Zstd,
            row_group_size: 1024,
            // Tag columns are low-cardinality; dictionary encoding is the
            // dominant win on export size.
            dictionary_tags: true,
            // Cap the export so a fleet upload cannot blow a flash budget.
            max_bytes: Some(8 * 1024 * 1024),
        },
    )?;
    println!(
        "   Exported {} rows ({} bytes{}) to {}",
        export.rows_written,
        export.bytes_written,
        if export.truncated { ", TRUNCATED" } else { "" },
        export_path.display()
    );

    // ── 8. GC soft-deleted segments ────────────────────────────
    println!("\n─── 7. Garbage collecting soft-deleted segments ───");
    let gc_count = db.gc_with_grace(0)?; // 0ms grace for demo
    println!("   Segments cleaned: {gc_count}");

    // ── 9. Database stats ──────────────────────────────────────
    println!("\n─── 8. Database introspection ───");
    println!("   WAL sequence: {}", db.wal_sequence());
    println!("   Data dir    : {}", db.data_dir().display());

    if let Some(schema) = db.schema("network_traffic") {
        println!(
            "   Schema 'network_traffic': {} columns",
            schema.columns().len()
        );
    }

    db.close()?;
    println!("\n✅ Done");

    Ok(())
}
