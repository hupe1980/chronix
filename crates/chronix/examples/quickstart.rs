#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Quickstart
//!
//! The program from the Getting Started page, kept here so CI compiles and
//! runs it. A documentation snippet that nothing executes is a snippet that
//! drifts: this crate's docs previously showed ports the server does not
//! listen on and crates that no longer exist.
//!
//! ```sh
//! cargo run -p chronix --example quickstart
//! ```

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The page uses a fixed path; a test needs a disposable one.
    let dir = tempfile::tempdir()?;

    // A data directory is the whole deployment. It is file-locked, so a
    // second process cannot open it by accident.
    let config = ChronixConfig::builder().data_dir(dir.path()).build()?;
    let db = Chronix::open(config)?;

    // A point is a series key (measurement + tags), some fields, and a
    // timestamp in **nanoseconds**.
    let key = SeriesKey::new("cpu", tags! { "host" => "web-01" })?;
    let point = Point::new(
        key,
        fields! { "usage_idle" => 95.5 },
        1_700_000_000_000_000_000,
    )?;
    db.insert(&point)?;

    // Query with the builder. Without `.range()` the plan covers all time.
    let plan = db.query().measurement("cpu").build()?;
    let batch = db.execute(&plan)?; // an Arrow RecordBatch
    assert_eq!(
        batch.num_rows(),
        1,
        "the point just written must be readable"
    );

    // Show the result rather than its row count: this is the first program
    // anybody runs, and "1 rows" is not evidence that a time-series database
    // works.
    println!("wrote 1 point to `cpu`, read it back:\n");
    arrow::util::pretty::print_batches(&[batch])?;

    // `close()` flushes and truncates the WAL. Dropping without it is safe —
    // recovery replays the log — but closing makes the next open faster.
    db.close()?;
    Ok(())
}
