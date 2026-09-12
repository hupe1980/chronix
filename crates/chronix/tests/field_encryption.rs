//! Per-column encryption, end to end.
//!
//! The declaration names columns and **environment variables**; the key
//! material never appears in a configuration file, which is the property
//! that makes this worth having. Full-disk encryption already protects a
//! stolen disk and its key is on the machine too, so a column key stored
//! beside the data would add nothing. A key injected into the process
//! environment makes a stolen disk *and* a stolen backup useless.
//!
//! **The test that decides whether any of this works is
//! `the_plaintext_is_not_in_the_segment_file`** — everything else checks
//! that the feature is usable, and that one checks that it is a feature.

// The whole file is about a feature-gated capability; without it there is
// nothing here to test and `FieldEncryption` does not reach the engine.
#![cfg(feature = "field-encryption")]
#![allow(clippy::unwrap_used, clippy::expect_used)]
// test code may unwrap
// `std::env::set_var` is `unsafe` since Rust 2024 because it mutates process
// state other threads may be reading. Every variable this file uses is set
// **once**, before any test body runs, and none is ever changed or removed —
// so there is no mutation to race with. A test that needs a *different* key
// for the same key id points it at a different variable; one that needs a
// missing key names a variable nobody sets.
#![allow(unsafe_code)]

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use arrow::array::Array as _;
use base64::Engine as _;
use chronix::prelude::*;
use chronix::{fields, tags, Chronix};
use chronix_core::FieldEncryption;
use tempfile::TempDir;

/// Every key this file uses, set once before any test body runs.
///
/// `Once` rather than per-test `set_var`: the tests in a binary run in
/// parallel, and mutating the environment beside a thread that reads it is
/// exactly the race `set_var` was made `unsafe` for.
fn keys() {
    static SET: std::sync::Once = std::sync::Once::new();
    SET.call_once(|| {
        for (var, seed) in [
            ("CHRONIX_TEST_KEY_PHI", 7u8),
            ("CHRONIX_TEST_KEY_RIGHT", 3),
            ("CHRONIX_TEST_KEY_WRONG", 9),
            ("CHRONIX_TEST_KEY_COMPACT", 5),
            ("CHRONIX_TEST_KEY_TAG", 11),
            ("CHRONIX_TEST_KEY_BACKUP", 13),
            ("CHRONIX_TEST_KEY_EXPORT", 17),
        ] {
            let encoded = base64::engine::general_purpose::STANDARD.encode([seed; 32]);
            // SAFETY: inside `Once`, before any test body has run, and
            // nothing in this file ever changes or removes a variable.
            unsafe { std::env::set_var(var, encoded) };
        }
    });
}

fn settings(column: &str, key_id: &str, var: &str) -> FieldEncryption {
    FieldEncryption {
        columns: BTreeMap::from([(column.to_owned(), key_id.to_owned())]),
        keys: BTreeMap::from([(key_id.to_owned(), var.to_owned())]),
    }
}

fn open_with(dir: &Path, enc: FieldEncryption) -> Result<Chronix, chronix::DbError> {
    Chronix::open(
        ChronixConfig::builder()
            .data_dir(dir)
            .field_encryption(enc)
            // Nothing may flush or compact behind these tests.
            .maintenance_interval(Duration::from_secs(86_400 * 365))
            .build()
            .unwrap(),
    )
}

fn point(secret: &str, ts: i64) -> Point {
    Point::new(
        SeriesKey::new("visits", tags! { "clinic" => "north" }).unwrap(),
        fields! { "patient_id" => secret, "minutes" => 12.0 },
        ts,
    )
    .unwrap()
}

fn read_back(db: &Chronix) -> Vec<String> {
    let plan = db
        .query()
        .measurement("visits")
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    let col = batch.column_by_name("patient_id").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    (0..arr.len()).map(|i| arr.value(i).to_owned()).collect()
}

/// Every byte of every segment file under `dir`.
fn all_segment_bytes(dir: &Path) -> Vec<u8> {
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

/// **The one that matters.** The declared column's value is not in the file.
///
/// A round trip proves the plumbing; it does not prove the bytes changed. An
/// encryption that reads back correctly and stores plaintext is exactly what
/// a wiring mistake produces, and it is indistinguishable from success at
/// every other level.
#[test]
fn the_plaintext_is_not_in_the_segment_file() {
    keys();
    let dir = TempDir::new().unwrap();
    let secret = "PATIENT-0000-SECRET-VALUE";

    let db = open_with(
        dir.path(),
        settings("patient_id", "phi", "CHRONIX_TEST_KEY_PHI"),
    )
    .unwrap();
    for ts in 0..200i64 {
        db.insert(&point(secret, ts * 1_000_000)).unwrap();
    }
    db.flush().unwrap();
    assert_eq!(read_back(&db)[0], secret, "it still reads back");
    db.close().unwrap();

    let bytes = all_segment_bytes(dir.path());
    assert!(!bytes.is_empty(), "there has to be a segment to look at");
    assert!(
        !bytes.windows(secret.len()).any(|w| w == secret.as_bytes()),
        "the declared column's plaintext is in the segment file"
    );

    // The control: a column that is *not* declared is plainly there, so the
    // assertion above is detecting encryption rather than compression.
    assert!(
        bytes.windows(5).any(|w| w == b"north"),
        "an undeclared tag must be findable — otherwise the test above proves nothing"
    );
}

/// The same database without the key cannot read the column.
#[test]
fn a_missing_key_is_a_startup_error() {
    keys();
    let dir = TempDir::new().unwrap();
    let err = open_with(
        dir.path(),
        settings("patient_id", "a", "CHRONIX_TEST_KEY_NOBODY_SETS_THIS"),
    )
    .expect_err("a declared key that does not resolve must stop the open");
    let message = err.to_string();
    assert!(
        message.contains("CHRONIX_TEST_KEY_NOBODY_SETS_THIS"),
        "{message}"
    );
    assert!(
        !message.contains("AAAA"),
        "the message must never carry key material: {message}"
    );
}

/// A different key cannot read what the first one wrote.
#[test]
fn the_wrong_key_cannot_read_the_column() {
    keys();
    let dir = TempDir::new().unwrap();
    let db = open_with(
        dir.path(),
        settings("patient_id", "k", "CHRONIX_TEST_KEY_RIGHT"),
    )
    .unwrap();
    db.insert(&point("secret", 1_000)).unwrap();
    db.flush().unwrap();
    db.close().unwrap();

    // Same key id, different material — what a restored-from-the-wrong-vault
    // deployment looks like.
    let db = open_with(
        dir.path(),
        settings("patient_id", "k", "CHRONIX_TEST_KEY_WRONG"),
    )
    .unwrap();
    let plan = db
        .query()
        .measurement("visits")
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    assert!(
        db.execute(&plan).is_err(),
        "a wrong key must fail loudly, not return ciphertext as a string"
    );
    db.close().unwrap();
}

/// Compaction re-encrypts rather than decrypting on the way through.
///
/// The failure this pins is the quiet one: a compaction that reads with the
/// key and writes without it leaves the column in plaintext hours after the
/// write that asked for it, and every read still works.
#[test]
fn compaction_keeps_the_column_encrypted() {
    keys();
    let dir = TempDir::new().unwrap();
    let secret = "SECRET-SURVIVES-COMPACTION";
    let db = open_with(
        dir.path(),
        settings("patient_id", "c", "CHRONIX_TEST_KEY_COMPACT"),
    )
    .unwrap();

    // Several segments, so there is something to merge.
    for batch in 0..6i64 {
        for ts in 0..50i64 {
            db.insert(&point(secret, (batch * 50 + ts) * 1_000_000))
                .unwrap();
        }
        db.flush().unwrap();
    }
    db.compact().unwrap();
    db.gc().unwrap();

    assert_eq!(read_back(&db)[0], secret, "still readable after compaction");
    db.close().unwrap();

    let bytes = all_segment_bytes(dir.path());
    assert!(
        !bytes.windows(secret.len()).any(|w| w == secret.as_bytes()),
        "compaction wrote the column back in plaintext"
    );
}

/// A tag cannot be encrypted, and saying so is better than pretending.
///
/// A tag's value is part of the series key, which is written in plaintext
/// beside the segment — in the `.series` sidecar, the inverted tag index and
/// the bloom filter. An "encrypted" tag column would publish the value it
/// claims to hide, a foot away.
#[test]
fn a_tag_column_cannot_be_encrypted() {
    keys();
    let dir = TempDir::new().unwrap();
    let db = open_with(dir.path(), settings("clinic", "t", "CHRONIX_TEST_KEY_TAG")).unwrap();
    db.insert(&point("x", 1_000)).unwrap();

    let err = db
        .flush()
        .expect_err("a segment naming an encrypted tag must not be written");
    let message = err.to_string();
    assert!(message.contains("clinic"), "{message}");
    assert!(message.contains("tag"), "{message}");
}

/// A backup of an encrypted database is still encrypted.
///
/// It is hard-linked or copied `.csx` bytes, so this is true by
/// construction — which is exactly why it is worth a test: the backup is the
/// copy most likely to leave the machine.
#[test]
fn a_backup_of_an_encrypted_database_is_ciphertext() {
    keys();
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("db");
    let backup = dir.path().join("backup");
    let secret = "SECRET-IN-A-BACKUP";

    let db = open_with(
        &data,
        settings("patient_id", "b", "CHRONIX_TEST_KEY_BACKUP"),
    )
    .unwrap();
    for ts in 0..200i64 {
        db.insert(&point(secret, ts * 1_000_000)).unwrap();
    }
    db.backup(&backup).unwrap();
    db.close().unwrap();

    let bytes = all_segment_bytes(&backup);
    assert!(!bytes.is_empty());
    assert!(
        !bytes.windows(secret.len()).any(|w| w == secret.as_bytes()),
        "the backup carries the plaintext"
    );
}

/// An encrypted column does not leave the segment format.
#[test]
fn an_export_of_an_encrypted_column_is_refused() {
    keys();
    let dir = TempDir::new().unwrap();
    let db = open_with(
        dir.path(),
        settings("patient_id", "e", "CHRONIX_TEST_KEY_EXPORT"),
    )
    .unwrap();
    db.insert(&point("secret", 1_000)).unwrap();
    db.flush().unwrap();

    let plan = db
        .query()
        .measurement("visits")
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    let err = db
        .export_parquet(
            &plan,
            &dir.path().join("out.parquet"),
            &chronix::ParquetExportConfig::default(),
        )
        .expect_err("a Parquet export would write the plaintext");
    assert!(err.to_string().contains("patient_id"), "{err}");

    // And a rollup over the same measurement, for the same reason.
    let rollup = chronix::RollupBuilder::new()
        .name("hourly")
        .source("visits")
        .target("visits_hourly")
        .bucket(chronix_core::timebucket::TimeBucket::fixed_ns(
            3_600_000_000_000,
        ))
        .aggregation(chronix::RollupAggFn::Avg)
        .build()
        .unwrap();
    let err = db
        .create_rollup(rollup)
        .expect_err("a rollup would write the aggregate in plaintext");
    assert!(err.to_string().contains("patient_id"), "{err}");
    db.close().unwrap();
}

/// A declaration that names a key nobody declared is refused at build time.
#[test]
fn an_undeclared_key_is_a_configuration_error() {
    let enc = FieldEncryption {
        columns: BTreeMap::from([("patient_id".to_owned(), "missing".to_owned())]),
        keys: BTreeMap::new(),
    };
    let err = ChronixConfig::builder()
        .data_dir("/tmp/chronix-unused")
        .field_encryption(enc)
        .build()
        .expect_err("a column naming an undeclared key must not build");
    assert!(err.to_string().contains("missing"), "{err}");
}

/// Restoring an encrypted backup without the key fails *intelligibly*.
///
/// The realistic disaster: the backup travels, the environment does not. The
/// database opens — a checkpoint is just segments, and `open()` only resolves
/// the keys the *restored* configuration declares — and then every query over
/// the encrypted column fails. That is correct and fail-closed; what matters
/// is whether the operator can tell it from data corruption, because those
/// two have very different next steps.
#[test]
fn a_restore_without_the_key_says_what_is_wrong() {
    keys();
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("db");
    let backup = dir.path().join("backup");
    let restored = dir.path().join("restored");

    let db = open_with(
        &data,
        settings("patient_id", "b", "CHRONIX_TEST_KEY_BACKUP"),
    )
    .unwrap();
    for ts in 0..200i64 {
        db.insert(&point("secret", ts * 1_000_000)).unwrap();
    }
    db.backup(&backup).unwrap();
    db.close().unwrap();

    // The new machine knows nothing about field encryption.
    Chronix::restore(&backup, &restored).unwrap();
    let plain = Chronix::open(
        ChronixConfig::builder()
            .data_dir(&restored)
            .maintenance_interval(Duration::from_secs(86_400 * 365))
            .build()
            .unwrap(),
    )
    .expect("the database still opens — the segments are just bytes");

    // The unencrypted columns are readable, so this is not a broken database.
    let plan = plain
        .query()
        .measurement("visits")
        .range(i64::MIN, i64::MAX)
        .field("minutes")
        .build()
        .unwrap();
    assert_eq!(
        plain.execute(&plan).unwrap().num_rows(),
        200,
        "a column that was never encrypted must still read"
    );

    // The encrypted one fails, and the message has to name the column and say
    // the word 'key' — an operator reading `corrupt file` reaches for the
    // wrong tool entirely.
    let plan = plain
        .query()
        .measurement("visits")
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    let err = plain
        .execute(&plan)
        .expect_err("an encrypted column with no key must not be readable");
    let message = err.to_string();
    assert!(
        message.contains("patient_id"),
        "the message must name the column: {message}"
    );
    assert!(
        message.contains("key"),
        "the message must say this is a key problem, not corruption: {message}"
    );
    assert!(
        !message.contains("corrupt"),
        "a missing key is not corruption, and the two have different fixes: {message}"
    );
    plain.close().unwrap();
}
