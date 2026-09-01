#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! `per_query_memory_limit` must bind on **every** query path.
//!
//! A memory budget that holds for `execute()` and not for `execute_stream()`
//! is not a memory budget — and `execute_stream` is the path that matters,
//! because SQL, HTTP `/query`, gRPC, PromQL, Prometheus remote read and Flight
//! SQL all go through it (R1: two implementations of one semantic diverge
//! silently, and the wrong one is always the one users get).
//!
//! The streaming `Aggregate(Scan)` branch folds each batch into a
//! `StreamingAggregator` whose state is `O(groups × fields)`. A group-by on a
//! high-cardinality tag therefore grows without bound. The budget is what
//! turns that from an OOM-killed process into a query error the caller can
//! handle, which on an embedded gateway is the difference between one failed
//! request and a dead host process.

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};
use tempfile::TempDir;

/// A budget small enough that a few thousand groups cannot fit, but large
/// enough that ordinary bookkeeping does not trip it.
const TINY_BUDGET: usize = 16 * 1024;

fn db_with_budget(dir: &TempDir, budget: usize) -> Chronix {
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .per_query_memory_limit(budget)
        .max_series_cardinality(200_000)
        .build()
        .unwrap();
    Chronix::open(config).unwrap()
}

/// Insert `n` distinct series, so a group-by on `host` produces `n` groups.
fn seed(db: &Chronix, n: usize) {
    let points: Vec<Point> = (0..n)
        .map(|i| {
            let key = SeriesKey::new("m", tags! { "host" => format!("h{i}").as_str() }).unwrap();
            Point::new(key, fields! { "v" => i as f64 }, 1_000 + i as i64).unwrap()
        })
        .collect();
    db.insert_batch(&points).unwrap().into_complete().unwrap();
}

fn grouped_plan(db: &Chronix) -> QueryPlan {
    db.query()
        .measurement("m")
        .range(0, i64::MAX)
        .group_by(&["host"])
        .aggregate(AggFn::Avg)
        .build()
        .unwrap()
}

/// The headline case: a high-cardinality group-by through `execute_stream`
/// must be refused by the budget, not allowed to consume the process.
#[test]
fn execute_stream_enforces_the_per_query_memory_limit() {
    let tmp = TempDir::new().unwrap();
    let db = db_with_budget(&tmp, TINY_BUDGET);
    seed(&db, 20_000);

    let plan = grouped_plan(&db);
    let err = db
        .execute_stream(&plan)
        .expect_err("a 20 000-group aggregate must not fit in a 16 KiB budget");

    let msg = err.to_string();
    assert!(
        msg.contains("memory") || msg.contains("budget"),
        "the error must name the budget, got: {msg}"
    );
}

/// `execute()` and `execute_stream()` must agree about the budget. They are
/// two implementations of one semantic, which is this tree's most productive
/// bug class.
#[test]
fn both_query_paths_agree_that_the_budget_binds() {
    let tmp = TempDir::new().unwrap();
    let db = db_with_budget(&tmp, TINY_BUDGET);
    seed(&db, 20_000);

    let plan = grouped_plan(&db);
    assert!(
        db.execute(&plan).is_err(),
        "execute() must refuse the query"
    );
    assert!(
        db.execute_stream(&plan).is_err(),
        "execute_stream() must refuse the same query"
    );
}

/// A guard that rejects ordinary queries is worse than the bug it fixes. A
/// small group-by well inside the budget must still answer.
#[test]
fn a_modest_group_by_still_succeeds_under_the_budget() {
    let tmp = TempDir::new().unwrap();
    let db = db_with_budget(&tmp, TINY_BUDGET);
    seed(&db, 8);

    let plan = grouped_plan(&db);
    let batches = db
        .execute_stream(&plan)
        .expect("eight groups must fit comfortably");
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 8, "one row per group");
}

/// A budget of zero means "unlimited", and that must keep working — it is the
/// default for embedded callers who manage their own memory.
#[test]
fn a_zero_budget_means_unlimited() {
    let tmp = TempDir::new().unwrap();
    let db = db_with_budget(&tmp, 0);
    seed(&db, 5_000);

    let plan = grouped_plan(&db);
    let batches = db
        .execute_stream(&plan)
        .expect("a zero budget must not impose a limit");
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 5_000);
}
