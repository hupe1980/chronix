#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Parquet cold archive — the archive is not a walled garden
//!
//! Chronix keeps hot data in its own `.csx` format because time-series
//! encodings, row-group zone maps and series blooms measurably beat general
//! Parquet on this workload. That advantage is real — and it is irrelevant to
//! data nobody queries hot. What matters about an *archive* is that something
//! other than chronix can read it.
//!
//! So archiving re-encodes segments to **Parquet**, uploads them, verifies
//! each object, and then removes them from the hot database. This example
//! shows all three consequences:
//!
//! 1. the archived rows leave the hot tier;
//! 2. the resulting object reads with a plain Parquet reader — no chronix
//!    involved, which is what DuckDB, Polars, Spark or pandas would be doing;
//! 3. the same prefix registers back with chronix's SQL engine, so the archive
//!    stays queryable from here too.
//!
//! ```sh
//! cargo run -p chronix --features object-store --example cold_tier
//! ```
//!
//! The equivalent in other tools, against the directory this example prints:
//!
//! ```sql
//! -- DuckDB
//! SELECT host, avg(watts) FROM read_parquet('<dir>/**/*.parquet') GROUP BY host;
//! ```
//!
//! ```python
//! # Polars
//! import polars as pl
//! pl.scan_parquet("<dir>/**/*.parquet").group_by("host").agg(pl.col("watts").mean()).collect()
//! ```

use std::sync::Arc;
use std::time::Duration;

use chronix::cold_archive::ArchiveConfig;
use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempfile::tempdir()?;
    let archive_dir = tmp.path().join("archive");
    std::fs::create_dir_all(&archive_dir)?;

    let config = ChronixConfig::builder()
        .data_dir(tmp.path().join("hot"))
        .build()?;
    let db = Arc::new(Chronix::open(config)?);

    // ── 1. Write meter readings, old and recent ─────────────────────────
    //
    // Decimals a sensor produced, not opaque bit patterns — the shape the
    // encodings are tuned for.
    let now_ns = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos(),
    )?;
    let year_ago = now_ns - 365 * 86_400 * 1_000_000_000;

    for (label, base) in [("old", year_ago), ("recent", now_ns - 60_000_000_000)] {
        let points: Vec<Point> = (0..2_000)
            .map(|i| {
                let host = if i % 2 == 0 { "meter-a" } else { "meter-b" };
                let key = SeriesKey::new("power", tags! { "host" => host }).unwrap();
                Point::new(
                    key,
                    fields! { "watts" => 200.0 + f64::from(i % 50) * 0.25 },
                    base + i64::from(i),
                )
                .unwrap()
            })
            .collect();
        assert!(db.insert_batch(&points)?.is_complete());
        db.flush()?; // one segment per batch
        println!("wrote 2000 {label} readings");
    }

    let hot_rows = |db: &Chronix| -> usize {
        let plan = db.query().measurement("power").build().unwrap();
        db.execute_iter(&plan)
            .unwrap()
            .map(|b| b.unwrap().num_rows())
            .sum()
    };
    println!("hot tier     : {} rows", hot_rows(&db));

    // ── 2. Archive everything older than 30 days ────────────────────────
    //
    // Each (measurement, shard) is read back through the *read path* —
    // deduplicated, tombstones applied — encoded as one Parquet object,
    // uploaded, verified, and only then dropped. In that order, so a failure
    // at any step leaves the data hot rather than leaving the catalog
    // pointing at a file that is gone.
    let archive_url = format!("file://{}/", archive_dir.display());
    let outcome = db
        .archive_cold_segments(&ArchiveConfig {
            cold_after: Duration::from_secs(30 * 86_400),
            remote_url: archive_url.clone(),
            ..Default::default()
        })
        .await?;

    println!(
        "archived     : {} object(s) from {} segment(s), {} rows, {} bytes, {} left hot",
        outcome.objects, outcome.segments, outcome.rows, outcome.bytes, outcome.failed
    );
    println!(
        "hot tier     : {} rows (archived rows have left)",
        hot_rows(&db)
    );

    // ── 3. Read the archive with a stock Parquet reader ─────────────────
    //
    // Nothing below this line knows what chronix is.
    let cold_object = find_one_parquet(&archive_dir).expect("a .parquet object was written");
    let bytes = std::fs::read(&cold_object)?;
    println!(
        "cold object  : {} ({} bytes, magic {:?})",
        cold_object.file_name().unwrap().to_string_lossy(),
        bytes.len(),
        std::str::from_utf8(&bytes[..4]).unwrap_or("??"),
    );

    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(&cold_object)?,
    )?
    .build()?;

    let mut rows = 0usize;
    let mut columns = Vec::new();
    for batch in reader {
        let batch = batch?;
        if columns.is_empty() {
            columns = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
        }
        rows += batch.num_rows();
    }
    println!("stock reader : {rows} rows, columns {columns:?}");
    assert_eq!(rows, 2_000, "every archived row survives the re-encode");

    // ── 4. Query the archive back through chronix SQL ───────────────────
    //
    // A separately named table rather than a transparent extension of the hot
    // measurement: a cold object has no series bloom and no tag index, so a
    // query that silently crossed the boundary would change cost class with
    // nothing in the plan saying so.
    let ctx = datafusion::prelude::SessionContext::new();
    chronix::sql::cold_tier::register_cold_tier(&ctx, &archive_url, "power", "power_archive")
        .await?;

    println!("\nSELECT host, count(*), avg(watts) FROM power_archive GROUP BY host:");
    ctx.sql(
        "SELECT host, count(*) AS n, round(avg(watts), 2) AS avg_w \
         FROM power_archive GROUP BY host ORDER BY host",
    )
    .await?
    .show()
    .await?;

    println!("\narchive dir  : {}", archive_dir.display());
    println!("\nPoint DuckDB or Polars at that directory — it is ordinary Parquet.");

    db.close()?;
    Ok(())
}

/// First `.parquet` object anywhere under `dir`.
fn find_one_parquet(dir: &std::path::Path) -> Option<std::path::PathBuf> {
    for entry in std::fs::read_dir(dir).ok()? {
        let path = entry.ok()?.path();
        if path.is_dir() {
            if let Some(found) = find_one_parquet(&path) {
                return Some(found);
            }
        } else if path.extension().is_some_and(|e| e == "parquet") {
            return Some(path);
        }
    }
    None
}
