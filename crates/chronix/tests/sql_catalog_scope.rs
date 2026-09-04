#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap
//! The SQL catalog a session sees is its own namespace's.
//!
//! Rows were always scoped — a cross-tenant `SELECT` returned nothing. But
//! `table_names()` and `table_exist()` answered from the process-wide schema
//! registry, so `SELECT * FROM another_tenants_measurement` **succeeded**
//! with zero rows where a name that genuinely does not exist errors. That
//! difference is an oracle: a tenant could enumerate every other tenant's
//! measurement names by probing, one name at a time.
//!
//! It also kept `information_schema` switched off, because those views
//! enumerate the catalog — so nobody could run `SHOW TABLES`, which is the
//! first thing anyone types in a SQL shell.

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};
use std::sync::Arc;

const NOW: i64 = 1_700_000_000_000_000_000;

fn open(dir: &tempfile::TempDir) -> Arc<Chronix> {
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    Arc::new(Chronix::open(config).unwrap())
}

/// A point carrying the namespace tag the server stamps.
fn point(measurement: &str, namespace: &str, ts: i64) -> Point {
    Point::new(
        SeriesKey::new(
            measurement,
            tags! { "host" => "h", "__namespace__" => namespace },
        )
        .unwrap(),
        fields! { "v" => 1.0 },
        ts,
    )
    .unwrap()
}

/// Two tenants, each with one measurement of its own.
fn two_tenants(dir: &tempfile::TempDir) -> Arc<Chronix> {
    let db = open(dir);
    db.insert(&point("payroll", "tenant-a", NOW)).unwrap();
    db.insert(&point("metrics", "tenant-b", NOW)).unwrap();
    db
}

fn query(
    db: &Arc<Chronix>,
    namespace: &str,
    sql: &str,
) -> Result<Vec<arrow::array::RecordBatch>, String> {
    let ctx = chronix::sql::create_namespaced_session_context(db.clone(), namespace);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        chronix::sql::sql_read_only(&ctx, sql)
            .await
            .map_err(|e| e.to_string())?
            .collect()
            .await
            .map_err(|e| e.to_string())
    })
}

/// Table names in the result of a `SHOW TABLES`-shaped query.
fn table_names(batches: &[arrow::array::RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            b.column_by_name("table_name")
                .and_then(|c| {
                    c.as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .map(|a| {
                            (0..arrow::array::Array::len(a))
                                .map(|i| a.value(i).to_string())
                                .collect::<Vec<_>>()
                        })
                })
                .unwrap_or_default()
        })
        .collect()
}

#[test]
fn another_tenants_measurement_does_not_resolve() {
    let dir = tempfile::tempdir().unwrap();
    let db = two_tenants(&dir);

    let own = query(&db, "tenant-b", "SELECT * FROM metrics").expect("its own table");
    assert_eq!(
        own.iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );

    let other = query(&db, "tenant-b", "SELECT * FROM payroll");
    let missing = query(&db, "tenant-b", "SELECT * FROM never_written");
    assert!(
        other.is_err(),
        "another tenant's measurement must not resolve"
    );
    assert!(missing.is_err());

    // Indistinguishable: the whole point is that probing tells you nothing.
    let normalise = |e: String| e.replace("payroll", "X").replace("never_written", "X");
    assert_eq!(
        normalise(other.unwrap_err()),
        normalise(missing.unwrap_err()),
        "a name that exists elsewhere must fail exactly as one that does not"
    );
}

#[test]
fn show_tables_lists_only_the_sessions_own_measurements() {
    let dir = tempfile::tempdir().unwrap();
    let db = two_tenants(&dir);

    let names = table_names(&query(&db, "tenant-b", "SHOW TABLES").expect("SHOW TABLES"));
    assert!(
        names.contains(&"metrics".to_string()),
        "its own table must be listed: {names:?}"
    );
    assert!(
        !names.contains(&"payroll".to_string()),
        "another tenant's must not be: {names:?}"
    );

    let names = table_names(
        &query(
            &db,
            "tenant-a",
            "SELECT table_name FROM information_schema.tables",
        )
        .expect("information_schema"),
    );
    assert!(names.contains(&"payroll".to_string()), "{names:?}");
    assert!(!names.contains(&"metrics".to_string()), "{names:?}");
}

/// The catalog views are what `SHOW TABLES` and `SHOW COLUMNS` read, and
/// they were unreachable while `information_schema` was off.
#[test]
fn the_catalog_views_are_reachable() {
    let dir = tempfile::tempdir().unwrap();
    let db = two_tenants(&dir);

    let columns = query(&db, "tenant-b", "SHOW COLUMNS FROM metrics").expect("SHOW COLUMNS");
    assert_eq!(
        columns
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum::<usize>(),
        3,
        "_time, host and v"
    );
}

/// An unscoped session — the embedded API, and a single-tenant server — sees
/// every measurement, because there is only one tenant.
#[test]
fn an_unscoped_session_sees_everything() {
    let dir = tempfile::tempdir().unwrap();
    let db = two_tenants(&dir);

    assert_eq!(
        db.sql("SELECT * FROM payroll")
            .unwrap()
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    let names = table_names(&db.sql("SHOW TABLES").unwrap());
    assert!(names.contains(&"payroll".to_string()), "{names:?}");
    assert!(names.contains(&"metrics".to_string()), "{names:?}");
}

/// The index is derived from the series set, which is rebuilt from the
/// segment sidecars at open — so it has to survive a restart, or every
/// tenant's tables vanish the first time the process is bounced.
#[test]
fn the_namespace_index_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = two_tenants(&dir);
        db.flush().unwrap();
        db.close().unwrap();
    }
    let db = open(&dir);

    assert_eq!(
        db.measurement_names_in(Some("tenant-a")),
        vec!["payroll".to_string()]
    );
    assert_eq!(
        db.measurement_names_in(Some("tenant-b")),
        vec!["metrics".to_string()]
    );
    assert!(db.has_measurement_in(Some("tenant-b"), "metrics"));
    assert!(!db.has_measurement_in(Some("tenant-b"), "payroll"));
}

/// A series still in the memtable at close is replayed from the WAL rather
/// than read from a sidecar, and the index is built from the same set — so
/// an unflushed measurement must come back too.
#[test]
fn an_unflushed_measurement_is_indexed_after_replay() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = two_tenants(&dir);
        // No flush, no close: the WAL is the only record.
        drop(db);
    }
    let db = open(&dir);
    assert!(
        db.has_measurement_in(Some("tenant-a"), "payroll"),
        "indexed from WAL replay: {:?}",
        db.measurement_names_in(Some("tenant-a"))
    );
}
