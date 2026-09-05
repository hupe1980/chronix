#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap
//! A scan batch says what each of its columns *is*.
//!
//! A tag and a string field are both `Utf8`, so a schema built from column
//! types alone cannot be classified afterwards. The role was known at write
//! time, dropped when the Arrow schema was built, and guessed again by four
//! consumers — with three different guesses, and none of them right for every
//! input:
//!
//! - the HTTP and gRPC query APIs guessed "anything not marked tag is a
//!   field", and nothing was marked, so **every tag came back as a field**
//!   and each row's `tags` was empty;
//! - the series-key extractor guessed "any `Utf8` that is not the timestamp
//!   is a tag", which puts a string **field** into a `SeriesKey`.
//!
//! The role is now stamped once, from the schema registry that owns it.

use std::sync::Arc;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

const T: i64 = 1_700_000_000_000_000_000;

fn db_with_a_string_field(dir: &tempfile::TempDir) -> Arc<Chronix> {
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());
    db.insert(
        &Point::new(
            SeriesKey::new("cpu", tags! { "host" => "a" }).unwrap(),
            // `note` is a *field* of string type — the case that makes a
            // type-based guess impossible.
            fields! { "usage" => 1.0, "note" => "boot".to_string() },
            T,
        )
        .unwrap(),
    )
    .unwrap();
    db
}

fn roles(batch: &arrow::record_batch::RecordBatch) -> Vec<(String, String)> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|f| {
            (
                f.name().clone(),
                f.metadata()
                    .get(chronix::db::ROLE_KEY)
                    .cloned()
                    .unwrap_or_else(|| "<none>".to_string()),
            )
        })
        .collect()
}

#[test]
fn every_scanned_column_carries_its_role_from_the_memtable_and_from_a_segment() {
    let dir = tempfile::tempdir().unwrap();
    let db = db_with_a_string_field(&dir);
    let plan = db.query().measurement("cpu").build().unwrap();

    let expected = vec![
        (
            chronix_core::TIME_COLUMN.to_string(),
            "timestamp".to_string(),
        ),
        ("host".to_string(), "tag".to_string()),
        ("note".to_string(), "field".to_string()),
        ("usage".to_string(), "field".to_string()),
    ];

    // Straight from the memtable…
    let batch = db.execute(&plan).unwrap();
    assert_eq!(roles(&batch), expected, "memtable");

    // …and after a flush, from the segment reader, which builds its schema
    // from stored column metadata rather than from the registry.
    db.flush().unwrap();
    let batch = db.execute(&plan).unwrap();
    assert_eq!(roles(&batch), expected, "segment");

    // And through the streaming path, which is what the server queries drive.
    for b in db.execute_iter(&plan).unwrap() {
        assert_eq!(roles(&b.unwrap()), expected, "execute_iter");
    }
}

#[test]
fn a_string_field_is_not_part_of_the_series_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = db_with_a_string_field(&dir);
    db.flush().unwrap();

    // `note` is a field; only `host` identifies the series. If a string field
    // were taken for a tag, this measurement would report two tags — and a
    // second `note` value would look like a second series.
    let schema = db.schema("cpu").unwrap();
    assert_eq!(schema.tag_names(), vec!["host"]);

    db.insert(
        &Point::new(
            SeriesKey::new("cpu", tags! { "host" => "a" }).unwrap(),
            fields! { "usage" => 2.0, "note" => "reload".to_string() },
            T + 1,
        )
        .unwrap(),
    )
    .unwrap();
    db.flush().unwrap();
    assert_eq!(
        db.statistics().series_count,
        1,
        "a second value for a string field is not a second series"
    );
}
