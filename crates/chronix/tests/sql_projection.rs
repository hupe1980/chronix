#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Projected scans must return the column the user
//! asked for.
//!
//! The storage layer emits record batches in Chronix's canonical column order
//! (timestamp, tags sorted, fields sorted). The DataFusion table schema used
//! to follow schema-*registration* order instead. When a measurement gained
//! its fields across separate writes in non-alphabetical order the two
//! disagreed, and the index-based projection in `ChronixExec` silently
//! returned a different column's data under the requested name.

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

async fn f64s(ctx: &datafusion::prelude::SessionContext, q: &str) -> Vec<f64> {
    let batches = ctx.sql(q).await.unwrap().collect().await.unwrap();
    batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

/// `power` is registered before `other`, so registration order
/// (`power, other`) differs from canonical order (`other, power`).
#[tokio::test]
async fn projection_returns_the_requested_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();
    db.insert(&Point::new(key.clone(), fields! { "power" => 10.0 }, 1000).unwrap())
        .unwrap();
    db.insert(&Point::new(key.clone(), fields! { "other" => 1.0 }, 2000).unwrap())
        .unwrap();
    db.flush().unwrap();

    let ctx = chronix::sql::create_session_context(db);

    // The row without the field reads back as NULL (see `null_semantics`),
    // so only the present value survives — but it must be *that column's*
    // value, which is what this test is about.
    assert_eq!(
        f64s(&ctx, "SELECT power FROM m WHERE power IS NOT NULL").await,
        vec![10.0],
        "SELECT power must return power's data"
    );
    assert_eq!(
        f64s(&ctx, "SELECT other FROM m WHERE other IS NOT NULL").await,
        vec![1.0],
        "SELECT other must return other's data"
    );
    assert_eq!(f64s(&ctx, "SELECT avg(power) FROM m").await, vec![10.0]);
    assert_eq!(f64s(&ctx, "SELECT avg(other) FROM m").await, vec![1.0]);
    assert_eq!(f64s(&ctx, "SELECT min(power) FROM m").await, vec![10.0]);
}

/// The SQL column order must be a function of the schema alone, never of the
/// order in which fields happened to be written.
#[tokio::test]
async fn column_order_is_independent_of_write_history() {
    let names = |ctx: &datafusion::prelude::SessionContext| {
        let ctx = ctx.clone();
        async move {
            ctx.sql("SELECT * FROM m")
                .await
                .unwrap()
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        }
    };

    // Write `zeta` first, then `alpha`.
    let d1 = tempfile::tempdir().unwrap();
    let db1 = open(&d1);
    let k1 = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();
    db1.insert(&Point::new(k1.clone(), fields! { "zeta" => 1.0 }, 1).unwrap())
        .unwrap();
    db1.insert(&Point::new(k1, fields! { "alpha" => 2.0 }, 2).unwrap())
        .unwrap();
    db1.flush().unwrap();
    let n1 = names(&chronix::sql::create_session_context(db1)).await;

    // Write `alpha` first, then `zeta`.
    let d2 = tempfile::tempdir().unwrap();
    let db2 = open(&d2);
    let k2 = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();
    db2.insert(&Point::new(k2.clone(), fields! { "alpha" => 2.0 }, 1).unwrap())
        .unwrap();
    db2.insert(&Point::new(k2, fields! { "zeta" => 1.0 }, 2).unwrap())
        .unwrap();
    db2.flush().unwrap();
    let n2 = names(&chronix::sql::create_session_context(db2)).await;

    assert_eq!(n1, n2, "column order must not depend on write order");
    assert_eq!(n1, vec!["_time", "h", "alpha", "zeta"]);
}

/// `SELECT count(*)` must work.
///
/// DataFusion pushes an **empty** projection down for an aggregate that reads
/// no columns — it needs the row count and nothing else. The scan then built a
/// `RecordBatch` with zero columns, and Arrow refuses one of those unless the
/// row count is given explicitly, so the most ordinary SQL query there is
/// failed with `500 INTERNAL_ERROR: must either specify a row count or at
/// least one column`.
///
/// Found by running the Python SDK against a live server rather than against a
/// mock. No unit test covered it because the hot-tier SQL tests all project at
/// least one column; the two `count(*)` tests that existed were for the
/// **cold** tier, which DataFusion reads through its own Parquet scan.
#[tokio::test]
async fn count_star_over_the_hot_tier_returns_the_row_count() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();
    for i in 0..5 {
        db.insert(&Point::new(key.clone(), fields! { "power" => 1.0 }, 1000 + i).unwrap())
            .unwrap();
    }
    db.flush().unwrap();

    let ctx = chronix::sql::create_session_context(db);
    let batches = ctx
        .sql("SELECT count(*) AS n FROM m")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 5, "count(*) must see every row");

    // `count(1)` takes the same empty-projection path.
    let batches = ctx
        .sql("SELECT count(1) AS n FROM m")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 5, "count(1) reads no column either");
}
