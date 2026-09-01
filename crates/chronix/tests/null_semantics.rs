#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! D-NULL regression tests: absent fields must read back as SQL `NULL`.
//!
//! Before `.csx` v2 an absent field was stored as a type sentinel (`0`, `""`,
//! `false`) with only an aggregate `null_count` surviving, so `IS NULL` never
//! matched, `= 0` matched rows that had no value, and `AVG`/`MIN`/`COUNT`
//! folded sentinels into their results.

use arrow::array::Array;
use chronix::prelude::*;
use chronix::{fields, tags, Chronix};
use std::sync::Arc;

fn open(dir: &tempfile::TempDir) -> Arc<Chronix> {
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    Arc::new(Chronix::open(config).unwrap())
}

async fn rows(ctx: &datafusion::prelude::SessionContext, q: &str) -> usize {
    ctx.sql(q)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum()
}

async fn one_f64(ctx: &datafusion::prelude::SessionContext, q: &str) -> Option<f64> {
    let b = ctx.sql(q).await.unwrap().collect().await.unwrap();
    let col = b[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    col.is_valid(0).then(|| col.value(0))
}

#[tokio::test]
async fn absent_numeric_field_is_null_not_zero() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();
    db.insert(&Point::new(key.clone(), fields! { "power" => 10.0 }, 1000).unwrap())
        .unwrap();
    db.insert(&Point::new(key.clone(), fields! { "other" => 1.0 }, 2000).unwrap())
        .unwrap();
    db.flush().unwrap();

    let ctx = chronix::sql::create_session_context(db);

    assert_eq!(rows(&ctx, "SELECT * FROM m WHERE power IS NULL").await, 1);
    assert_eq!(
        rows(&ctx, "SELECT * FROM m WHERE power IS NOT NULL").await,
        1
    );
    assert_eq!(
        rows(&ctx, "SELECT * FROM m WHERE power = 0").await,
        0,
        "an absent field must not compare equal to 0"
    );

    // Aggregates must ignore the absent row entirely.
    assert_eq!(one_f64(&ctx, "SELECT avg(power) FROM m").await, Some(10.0));
    assert_eq!(one_f64(&ctx, "SELECT min(power) FROM m").await, Some(10.0));
    assert_eq!(one_f64(&ctx, "SELECT sum(power) FROM m").await, Some(10.0));
    assert_eq!(rows(&ctx, "SELECT power FROM m WHERE power > 5").await, 1);

    let counts = ctx
        .sql("SELECT count(power) FROM m")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let c = counts[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(c, 1, "count() must skip the absent row");
}

#[tokio::test]
async fn absent_string_bool_and_int_fields_are_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();
    db.insert(
        &Point::new(
            key.clone(),
            fields! { "s" => "hello", "b" => true, "i" => 7_i64 },
            1000,
        )
        .unwrap(),
    )
    .unwrap();
    db.insert(&Point::new(key.clone(), fields! { "z" => 1.0 }, 2000).unwrap())
        .unwrap();
    db.flush().unwrap();

    let ctx = chronix::sql::create_session_context(db);
    for col in ["s", "b", "i"] {
        assert_eq!(
            rows(&ctx, &format!("SELECT * FROM m WHERE {col} IS NULL")).await,
            1,
            "absent {col} must be NULL"
        );
    }
    assert_eq!(rows(&ctx, "SELECT * FROM m WHERE s = ''").await, 0);
    assert_eq!(rows(&ctx, "SELECT * FROM m WHERE b = false").await, 0);
    assert_eq!(rows(&ctx, "SELECT * FROM m WHERE i = 0").await, 0);
}

/// Dense data must be unaffected — and must store no bitmap at all.
#[tokio::test]
async fn dense_data_has_no_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();
    for i in 0..100i64 {
        db.insert(&Point::new(key.clone(), fields! { "v" => i as f64 }, i * 1000).unwrap())
            .unwrap();
    }
    db.flush().unwrap();

    let ctx = chronix::sql::create_session_context(db);
    assert_eq!(rows(&ctx, "SELECT * FROM m WHERE v IS NULL").await, 0);
    assert_eq!(rows(&ctx, "SELECT * FROM m WHERE v IS NOT NULL").await, 100);
    assert_eq!(one_f64(&ctx, "SELECT sum(v) FROM m").await, Some(4950.0));
}

/// The memtable path always built proper Arrow null buffers while the segment
/// path stored sentinels, so the *same* query returned different answers
/// before and after a flush. `.csx` v2 makes the two agree.
#[tokio::test]
async fn unflushed_and_flushed_data_agree_on_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();
    db.insert(&Point::new(key.clone(), fields! { "power" => 10.0 }, 1000).unwrap())
        .unwrap();
    db.insert(&Point::new(key.clone(), fields! { "other" => 1.0 }, 2000).unwrap())
        .unwrap();

    let ctx = chronix::sql::create_session_context(db.clone());

    // Read from the memtable (nothing flushed yet).
    let before_null = rows(&ctx, "SELECT * FROM m WHERE power IS NULL").await;
    let before_avg = one_f64(&ctx, "SELECT avg(power) FROM m").await;

    db.flush().unwrap();

    // Same queries, now served from a segment.
    let after_null = rows(&ctx, "SELECT * FROM m WHERE power IS NULL").await;
    let after_avg = one_f64(&ctx, "SELECT avg(power) FROM m").await;

    assert_eq!(before_null, after_null, "IS NULL changed across flush");
    assert_eq!(before_avg, after_avg, "avg() changed across flush");
    assert_eq!(after_null, 1);
    assert_eq!(after_avg, Some(10.0));
}

/// Compaction rewrites segments through the batch encode path. Nulls must
/// survive that round-trip, not silently revert to sentinels.
#[tokio::test]
async fn nulls_survive_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();

    // Several small segments, each alternating which field is present.
    for i in 0..8i64 {
        let p = if i % 2 == 0 {
            Point::new(key.clone(), fields! { "power" => i as f64 }, i * 1000).unwrap()
        } else {
            Point::new(key.clone(), fields! { "other" => i as f64 }, i * 1000).unwrap()
        };
        db.insert(&p).unwrap();
        db.flush().unwrap();
    }

    let ctx = chronix::sql::create_session_context(db.clone());
    let before = rows(&ctx, "SELECT * FROM m WHERE power IS NULL").await;
    let before_sum = one_f64(&ctx, "SELECT sum(power) FROM m").await;
    assert_eq!(before, 4, "4 rows have no power field");

    db.compact().unwrap();

    assert_eq!(
        rows(&ctx, "SELECT * FROM m WHERE power IS NULL").await,
        before,
        "compaction lost null information"
    );
    assert_eq!(
        one_f64(&ctx, "SELECT sum(power) FROM m").await,
        before_sum,
        "compaction changed sum(power)"
    );
    assert_eq!(rows(&ctx, "SELECT * FROM m WHERE power = 0").await, 1);
}
