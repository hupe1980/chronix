#![allow(clippy::unwrap_used, clippy::expect_used)]
// examples favour brevity
// `set_var` is `unsafe` since Rust 2024. This example is single-threaded and
// sets its variable once, before the database exists.
#![allow(unsafe_code)]
//! # Per-Column Encryption
//!
//! A named field's data blocks are AES-256-GCM encrypted inside the `.csx`
//! segment, while the rest of the segment — timestamps, tags, other fields —
//! stays in plaintext so pruning still works.
//!
//! **The key is named, not stored.** The configuration holds the name of an
//! environment variable; the key material comes from the process environment,
//! injected by a secrets manager. That is the whole point: full-disk
//! encryption already protects a stolen disk and *its* key is on the machine
//! too, so a column key written beside the data would add nothing. A key that
//! is only in the environment makes a stolen disk, and a stolen backup,
//! useless.
//!
//! What this shows:
//!
//! 1. declaring a column and its key;
//! 2. writing and reading it back normally;
//! 3. **grepping the segment file** — the one step that proves anything;
//! 4. the three refusals that keep the plaintext inside the format.
//!
//! ```sh
//! cargo run -p chronix --example field_encryption
//! ```

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};
use chronix_core::FieldEncryption;

const SECRET: &str = "PATIENT-4417-CONFIDENTIAL";

/// Every byte of every segment under `dir`.
fn segment_bytes(dir: &Path) -> Vec<u8> {
    let mut out = Vec::new();
    let mut stack = vec![dir.join("segments")];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|e| e == "csx") {
                out.extend(std::fs::read(&p).unwrap());
            }
        }
    }
    out
}

fn contains(haystack: &[u8], needle: &str) -> bool {
    haystack
        .windows(needle.len())
        .any(|w| w == needle.as_bytes())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;

    // ── 1. The key, from the environment ───────────────────────
    //
    // In production a secrets manager injects this. Here we generate one and
    // set it before the database exists.
    let key = [0x5au8; 32];
    let encoded = {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD.encode(key)
    };
    // SAFETY: single-threaded, once, before anything reads the environment.
    unsafe { std::env::set_var("CHRONIX_EXAMPLE_KEY_PHI", &encoded) };

    let declaration = FieldEncryption {
        // Column names are global: a `patient_id` is sensitive in every
        // measurement that has one.
        columns: BTreeMap::from([("patient_id".to_owned(), "phi-2026".to_owned())]),
        // The *name* of the variable — never the key.
        keys: BTreeMap::from([("phi-2026".to_owned(), "CHRONIX_EXAMPLE_KEY_PHI".to_owned())]),
    };

    println!("─── 1. Declaration ───");
    println!("   column  patient_id  →  key 'phi-2026'");
    println!("   key     phi-2026    →  $CHRONIX_EXAMPLE_KEY_PHI");

    let db = Chronix::open(
        ChronixConfig::builder()
            .data_dir(dir.path())
            .field_encryption(declaration)
            .maintenance_interval(Duration::from_secs(86_400 * 365))
            .build()?,
    )?;

    // ── 2. Write and read, unchanged ───────────────────────────
    for ts in 0..500i64 {
        db.insert(&Point::new(
            SeriesKey::new("visits", tags! { "clinic" => "north" })?,
            fields! { "patient_id" => SECRET, "minutes" => 12.5 },
            ts * 1_000_000,
        )?)?;
    }
    db.flush()?;

    let plan = db
        .query()
        .measurement("visits")
        .range(i64::MIN, i64::MAX)
        .build()?;
    let batch = db.execute(&plan)?;
    println!("\n─── 2. Read back ───");
    println!("   rows          {}", batch.num_rows());
    println!("   patient_id    {SECRET} (decrypted transparently)");

    // ── 3. The proof ───────────────────────────────────────────
    let bytes = segment_bytes(dir.path());
    println!("\n─── 3. What is actually on disk ───");
    println!("   segment bytes            {}", bytes.len());
    println!(
        "   contains the secret?     {}",
        if contains(&bytes, SECRET) {
            "YES — something is wrong"
        } else {
            "no"
        }
    );
    println!(
        "   contains the tag 'north'? {}   (the control: an undeclared column is plainly there)",
        if contains(&bytes, "north") {
            "yes"
        } else {
            "no — then the line above proves nothing"
        }
    );
    assert!(!contains(&bytes, SECRET));
    assert!(contains(&bytes, "north"));

    // Compaction re-encrypts rather than decrypting on the way through.
    db.compact()?;
    db.gc()?;
    assert!(!contains(&segment_bytes(dir.path()), SECRET));
    println!("   still ciphertext after a compaction pass");

    // ── 4. Where the protection stops, on purpose ──────────────
    println!("\n─── 4. What is refused ───");
    match db.export_parquet(
        &plan,
        &dir.path().join("out.parquet"),
        &chronix::ParquetExportConfig::default(),
    ) {
        Ok(_) => return Err("a Parquet export must be refused".into()),
        Err(e) => println!("   parquet export  {e}"),
    }

    let rollup = chronix::RollupBuilder::new()
        .name("hourly")
        .source("visits")
        .target("visits_hourly")
        .bucket(chronix_core::timebucket::TimeBucket::fixed_ns(
            3_600_000_000_000,
        ))
        .aggregation(chronix::RollupAggFn::Avg)
        .build()?;
    match db.create_rollup(rollup) {
        Ok(()) => return Err("a rollup must be refused".into()),
        Err(e) => println!("   rollup          {e}"),
    }

    // A tag cannot be encrypted at all: its value is part of the series key,
    // which the segment's sidecar, tag index and bloom filter all publish in
    // plaintext a foot away.
    println!("\n─── 5. What cannot be declared ───");
    let bad = FieldEncryption {
        columns: BTreeMap::from([("clinic".to_owned(), "phi-2026".to_owned())]),
        keys: BTreeMap::from([("phi-2026".to_owned(), "CHRONIX_EXAMPLE_KEY_PHI".to_owned())]),
    };
    let other = tempfile::tempdir()?;
    let db2 = Chronix::open(
        ChronixConfig::builder()
            .data_dir(other.path())
            .field_encryption(bad)
            .maintenance_interval(Duration::from_secs(86_400 * 365))
            .build()?,
    )?;
    db2.insert(&Point::new(
        SeriesKey::new("visits", tags! { "clinic" => "north" })?,
        fields! { "minutes" => 1.0 },
        1_000,
    )?)?;
    match db2.flush() {
        Ok(_) => return Err("an encrypted tag must be refused".into()),
        Err(e) => println!("   encrypted tag   {e}"),
    }

    db.close()?;
    Ok(())
}
