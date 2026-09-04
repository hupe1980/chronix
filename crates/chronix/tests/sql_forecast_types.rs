//! A forecast over a non-`Float64` column must error, not panic.
//!
//! `f64_values` downcast with `as_primitive::<Float64Type>()`, which **panics**
//! on any other type — and an integer counter is exactly what somebody
//! forecasts or rates. The panic happened inside a `spawn_blocking`, so it
//! surfaced as an opaque 500 with a poisoned task rather than as an error
//! naming the column.
//!
//! Casting is the answer rather than refusing: every numeric column this
//! engine stores has an `f64` image, and `forecast(request_count, …)` is an
//! ordinary thing to ask. A non-numeric column is refused by name.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap

use std::sync::Arc;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

fn db_with_int_column(dir: &tempfile::TempDir) -> Arc<Chronix> {
    let db = Arc::new(
        Chronix::open(
            ChronixConfig::builder()
                .data_dir(dir.path())
                .build()
                .unwrap(),
        )
        .unwrap(),
    );
    let key = SeriesKey::new("m", tags! { "host" => "h" }).unwrap();
    let points: Vec<Point> = (0..40)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "count" => i, "value" => i as f64 },
                i * 1_000_000_000,
            )
            .unwrap()
        })
        .collect();
    assert!(db.insert_batch(&points).unwrap().is_complete());
    db.flush().unwrap();
    db
}

/// Every sample-buffering aggregate, over an integer column, must answer.
#[tokio::test]
async fn the_aggregates_answer_over_an_integer_column() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = chronix::sql::create_session_context(db_with_int_column(&dir));

    for sql in [
        "SELECT forecast(count, _time, 5) AS r FROM m",
        "SELECT auto_forecast(count, _time, 5) AS r FROM m",
        "SELECT rate(count, _time) AS r FROM m",
        "SELECT irate(count, _time) AS r FROM m",
    ] {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql} must plan: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("{sql} must run: {e}"));
        assert_eq!(batches[0].num_rows(), 1, "{sql} must answer");
    }

    // `count` climbs by exactly 1 per second, so the rate is 1.0/s whatever
    // the column's storage type is.
    let batches = ctx
        .sql("SELECT rate(count, _time) AS r FROM m")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let r = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    assert!(
        (r - 1.0).abs() < 1e-9,
        "an integer counter must rate the same as a float one, got {r}"
    );
}

/// A column that is not numeric at all is refused by name, not cast.
#[tokio::test]
async fn a_non_numeric_column_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = chronix::sql::create_session_context(db_with_int_column(&dir));
    let result = match ctx.sql("SELECT rate(host, _time) AS r FROM m").await {
        Ok(df) => df.collect().await.map(|_| ()),
        Err(e) => Err(e),
    };
    let err = result.expect_err("a tag column has no rate");
    let msg = err.to_string();
    assert!(
        !msg.contains("panicked") && !msg.contains("JoinError"),
        "must fail cleanly, got: {msg}"
    );
}

/// The `Float64` column still works, so the guard did not break the feature.
#[tokio::test]
async fn a_float_column_still_forecasts() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = chronix::sql::create_session_context(db_with_int_column(&dir));
    let batches = ctx
        .sql("SELECT forecast(value, _time, 5) AS f FROM m")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 1);
}
