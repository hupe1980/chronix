#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Quickstart
//!
//! Open a database, write a point, read it back — with the builder and with
//! SQL. This is the example on the front page of the docs; CI compiles and
//! runs it on every change.
//!
//! ```sh
//! cargo run -p chronix --example quickstart
//! ```

use chronix::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;

    // A data directory is the whole deployment. It is file-locked, so a
    // second process cannot open it by accident. `open_small` is the
    // gateway preset; `Chronix::open(ChronixConfig::builder()…)` is the
    // general form.
    let db = Chronix::open_small(dir.path())?;

    // A point is a series key (measurement + tags), some fields, and a
    // timestamp in **nanoseconds**.
    let key = SeriesKey::new("cpu", tags! { "host" => "web-01" })?;
    let point = Point::new(
        key,
        fields! { "usage_idle" => 95.5 },
        1_700_000_000_000_000_000,
    )?;
    db.insert(&point)?;

    // Query with the builder — an Arrow RecordBatch back.
    let plan = db.query().measurement("cpu").build()?;
    let batch = db.execute(&plan)?;
    println!("wrote 1 point to `cpu`, read it back with the builder:\n");
    println!("{}", arrow::util::pretty::pretty_format_batches(&[batch])?);

    // …or with SQL. Every measurement is a table, `_time` is the timestamp.
    let batches = db.sql("SELECT _time, host, usage_idle FROM cpu")?;
    println!("\nand with SQL:\n");
    println!("{}", arrow::util::pretty::pretty_format_batches(&batches)?);

    // `close()` flushes and records the WAL floor, so the next open replays
    // nothing. Dropping the last handle does the same.
    db.close()?;
    Ok(())
}
