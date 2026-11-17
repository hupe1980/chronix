//! Namespace limits are admitted by the engine, exactly, on every write.
//!
//! A tenant's series and measurement quotas used to be configuration only:
//! the check existed, was tested, and had no caller, so a namespace grew past
//! its limit on every real write while its usage counter never moved. These
//! tests drive the public API — the only path a write takes — and compare
//! usage against a recount of what the database actually holds.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chronix::prelude::*;
use chronix::{Chronix, DbError, NamespaceLimits, QuotaResource, fields};
use chronix_core::NAMESPACE_TAG;
use proptest::prelude::*;
use tempfile::TempDir;

const HOUR: i64 = 3_600_000_000_000;

fn tags_for(ns: Option<&str>, host: &str) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::new();
    tags.insert("host".to_string(), host.to_string());
    if let Some(ns) = ns {
        tags.insert(NAMESPACE_TAG.to_string(), ns.to_string());
    }
    tags
}

fn point(measurement: &str, ns: Option<&str>, host: &str, ts: i64) -> Point {
    Point::new(
        SeriesKey::new(measurement, tags_for(ns, host)).unwrap(),
        fields! { "v" => 1.0 },
        ts,
    )
    .unwrap()
}

/// The distinct series of `namespace` the database answers a query with —
/// an independent recount of what admission should be tracking.
fn series_held(db: &Chronix, namespace: &str) -> u64 {
    use arrow::array::{Array, StringArray};
    let plan = db
        .query()
        .measurement("m")
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    let column = |name: &str| {
        batch
            .column_by_name(name)
            .map(|c| c.as_any().downcast_ref::<StringArray>().unwrap().clone())
    };
    let (Some(ns), Some(host)) = (column(NAMESPACE_TAG), column("host")) else {
        return 0;
    };
    let mut hosts = std::collections::BTreeSet::new();
    for row in 0..batch.num_rows() {
        if !ns.is_null(row) && ns.value(row) == namespace {
            hosts.insert(host.value(row).to_string());
        }
    }
    hosts.len() as u64
}

fn open(dir: &TempDir, budget: usize) -> Chronix {
    Chronix::open(
        ChronixConfig::builder()
            .data_dir(dir.path())
            .max_series_cardinality(budget)
            .build()
            .unwrap(),
    )
    .unwrap()
}

fn limits(series: u64, measurements: u32) -> NamespaceLimits {
    NamespaceLimits {
        max_series: series,
        max_measurements: measurements,
    }
}

fn quota_error(err: &DbError) -> (String, QuotaResource, u64, u64) {
    match err {
        DbError::NamespaceQuotaExceeded {
            namespace,
            resource,
            current,
            limit,
        } => (namespace.clone(), *resource, *current, *limit),
        other => panic!("expected NamespaceQuotaExceeded, got {other:?}"),
    }
}

/// (a) One new series over the limit refuses the whole batch, registers
/// nothing, and says which namespace, which limit and what to do.
#[test]
fn a_batch_over_the_series_quota_is_refused_whole() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir, 1_000);
    db.set_namespace_limits("tenant-a", limits(2, 10));
    db.insert_batch(&[
        point("m", Some("tenant-a"), "h1", 1),
        point("m", Some("tenant-a"), "h2", 1),
    ])
    .unwrap()
    .into_complete()
    .unwrap();

    let err = db
        .insert_batch(&[
            point("m", Some("tenant-a"), "h1", 2),
            point("m", Some("tenant-a"), "h3", 2),
        ])
        .unwrap_err();
    assert_eq!(
        quota_error(&err),
        ("tenant-a".to_string(), QuotaResource::SeriesCount, 2, 2)
    );
    let msg = err.to_string();
    assert!(
        msg.contains("tenant-a") && msg.contains("max_series_count"),
        "{msg}"
    );

    // Nothing from the refused batch was admitted — not even the point on a
    // known series, and the new series is not counted.
    assert_eq!(db.namespace_usage("tenant-a").series, 2);
    assert_eq!(db.statistics().series_count, 2);
}

/// (b) At the quota, writes to series the namespace already has go through:
/// the limit is on distinct series, not on points.
#[test]
fn existing_series_stay_writable_at_the_quota() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir, 1_000);
    db.set_namespace_limits("tenant-a", limits(1, 10));
    for ts in 0..50 {
        db.insert(&point("m", Some("tenant-a"), "h1", ts)).unwrap();
    }
    assert_eq!(db.namespace_usage("tenant-a").series, 1);
}

/// (c) A whole-series delete and a retention pass release headroom, with
/// no reopen in between.
#[test]
fn delete_and_retention_release_the_quota() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir, 1_000);
    db.set_namespace_limits("tenant-a", limits(2, 10));
    db.insert(&point("m", Some("tenant-a"), "old", HOUR))
        .unwrap();
    db.insert(&point("m", Some("tenant-a"), "gone", HOUR))
        .unwrap();
    db.flush().unwrap();
    assert!(
        db.insert(&point("m", Some("tenant-a"), "new", HOUR))
            .is_err()
    );

    db.delete_series("m", &tags_for(Some("tenant-a"), "gone"))
        .unwrap();
    assert_eq!(db.namespace_usage("tenant-a").series, 1);
    db.insert(&point("m", Some("tenant-a"), "new", 1000 * HOUR))
        .unwrap();
    db.flush().unwrap();
    assert_eq!(db.namespace_usage("tenant-a").series, 2);

    // `old` lives only in the first segment, which retention drops.
    db.enforce_retention(Duration::from_secs(10 * 3600))
        .unwrap();
    assert_eq!(db.namespace_usage("tenant-a").series, 1);
    db.insert(&point("m", Some("tenant-a"), "after", 1000 * HOUR))
        .unwrap();
}

/// (d) and (e) Another namespace is unaffected, and a series with no
/// namespace tag is never limited.
#[test]
fn limits_are_per_namespace_and_untagged_series_are_unlimited() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir, 1_000);
    db.set_namespace_limits("tenant-a", limits(1, 10));
    db.insert(&point("m", Some("tenant-a"), "h1", 1)).unwrap();
    assert!(db.insert(&point("m", Some("tenant-a"), "h2", 1)).is_err());
    for i in 0..20 {
        db.insert(&point("m", Some("tenant-b"), &format!("h{i}"), 1))
            .unwrap();
        db.insert(&point("m", None, &format!("h{i}"), 1)).unwrap();
    }
    assert_eq!(db.namespace_usage("tenant-b").series, 20);
}

/// (f) The measurement quota refuses a new measurement and not a new series
/// of a measurement the namespace already has.
#[test]
fn the_measurement_quota_counts_measurements_not_series() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir, 1_000);
    db.set_namespace_limits("tenant-a", limits(100, 1));
    db.insert(&point("cpu", Some("tenant-a"), "h1", 1)).unwrap();
    db.insert(&point("cpu", Some("tenant-a"), "h2", 1)).unwrap();
    let err = db
        .insert(&point("mem", Some("tenant-a"), "h1", 1))
        .unwrap_err();
    assert_eq!(
        quota_error(&err),
        ("tenant-a".to_string(), QuotaResource::Measurements, 1, 1)
    );
    assert!(err.to_string().contains("max_measurements"));
    assert_eq!(db.namespace_usage("tenant-a").measurements, 1);
}

/// (g) A limit lowered below usage keeps every existing series writable,
/// refuses new ones, and reports usage above the limit honestly.
#[test]
fn a_lowered_limit_keeps_existing_series() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir, 1_000);
    for i in 0..5 {
        db.insert(&point("m", Some("tenant-a"), &format!("h{i}"), 1))
            .unwrap();
    }
    db.set_namespace_limits("tenant-a", limits(2, 10));
    db.insert(&point("m", Some("tenant-a"), "h4", 2)).unwrap();
    let err = db
        .insert(&point("m", Some("tenant-a"), "h9", 2))
        .unwrap_err();
    assert_eq!(quota_error(&err).2, 5);
    assert_eq!(db.namespace_usage("tenant-a").series, 5);

    db.clear_namespace_limits("tenant-a");
    db.insert(&point("m", Some("tenant-a"), "h9", 2)).unwrap();
}

/// (h) A batch mixing two namespaces where only one is over is refused
/// whole — a batch is one write, and half of one is a surprise.
#[test]
fn a_mixed_batch_is_refused_whole() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir, 1_000);
    db.set_namespace_limits("tenant-a", limits(1, 10));
    db.insert(&point("m", Some("tenant-a"), "h1", 1)).unwrap();
    let err = db
        .insert_batch(&[
            point("m", Some("tenant-b"), "h1", 1),
            point("m", Some("tenant-a"), "h2", 1),
        ])
        .unwrap_err();
    assert_eq!(quota_error(&err).0, "tenant-a");
    assert_eq!(db.namespace_usage("tenant-b").series, 0);
}

/// (i) Over both the namespace quota and the database budget, the refusal
/// names the namespace quota — the limit the writer can act on.
#[test]
fn the_namespace_quota_is_checked_before_the_database_budget() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir, 2);
    db.set_namespace_limits("tenant-a", limits(2, 10));
    db.insert_batch(&[
        point("m", Some("tenant-a"), "h1", 1),
        point("m", Some("tenant-a"), "h2", 1),
    ])
    .unwrap()
    .into_complete()
    .unwrap();
    let err = db
        .insert(&point("m", Some("tenant-a"), "h3", 1))
        .unwrap_err();
    assert_eq!(quota_error(&err).1, QuotaResource::SeriesCount);
}

/// Usage survives a crash: it is derived from the series set, which the
/// database rebuilds on open — WAL replay included — and limits are the
/// host's to push again.
#[test]
fn usage_is_rebuilt_on_open_including_replayed_series() {
    if let Ok(dir) = std::env::var("CHRONIX_QUOTA_CRASH_DIR") {
        let db = Chronix::open(ChronixConfig::builder().data_dir(&dir).build().unwrap()).unwrap();
        db.insert(&point("m", Some("tenant-a"), "flushed", 1))
            .unwrap();
        db.flush().unwrap();
        db.insert(&point("m", Some("tenant-a"), "in-wal", 1))
            .unwrap();
        std::process::abort(); // the second series survives only in the WAL
    }
    let dir = TempDir::new().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "usage_is_rebuilt_on_open_including_replayed_series",
            "--exact",
            "--nocapture",
        ])
        .env("CHRONIX_QUOTA_CRASH_DIR", dir.path())
        .status()
        .unwrap();
    assert!(!status.success(), "the child must abort, not exit cleanly");

    let db = open(&dir, 1_000);
    assert_eq!(db.namespace_usage("tenant-a").series, 2);
    db.set_namespace_limits("tenant-a", limits(2, 10));
    db.insert(&point("m", Some("tenant-a"), "in-wal", 2))
        .unwrap();
    assert!(
        db.insert(&point("m", Some("tenant-a"), "third", 2))
            .is_err()
    );
}

/// Concurrent writers cannot overshoot either limit. Before admission took
/// a lock, two batches could each see room and both insert.
#[test]
fn concurrent_admission_never_overshoots() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(open(&dir, 120));
    db.set_namespace_limits("tenant-a", limits(50, 10));
    let handles: Vec<_> = (0..8)
        .map(|t| {
            let db = Arc::clone(&db);
            std::thread::spawn(move || {
                for i in 0..40 {
                    let ns = if i % 2 == 0 { Some("tenant-a") } else { None };
                    let _ = db.insert(&point("m", ns, &format!("t{t}-{i}"), 1));
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert!(db.namespace_usage("tenant-a").series <= 50);
    assert!(db.statistics().series_count <= 120);
    assert_eq!(
        db.namespace_usage("tenant-a").series,
        50,
        "the quota fills exactly"
    );
}

#[derive(Debug, Clone)]
enum Op {
    Write { ns: u8, host: u8 },
    DeleteSeries { ns: u8, host: u8 },
    Retain,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (0u8..3, 0u8..12).prop_map(|(ns, host)| Op::Write { ns, host }),
        2 => (0u8..3, 0u8..12).prop_map(|(ns, host)| Op::DeleteSeries { ns, host }),
        1 => Just(Op::Retain),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    /// Whatever writes, deletes and retention passes happen, a namespace's
    /// usage is exactly the number of series it holds.
    #[test]
    fn usage_is_always_a_recount(ops in proptest::collection::vec(op(), 1..40)) {
        let dir = TempDir::new().unwrap();
        let db = open(&dir, 10_000);
        let mut clock = 0i64;
        for op in ops {
            clock += HOUR;
            match op {
                Op::Write { ns, host } => {
                    let ns = format!("ns{ns}");
                    db.insert(&point("m", Some(&ns), &format!("h{host}"), clock)).unwrap();
                }
                Op::DeleteSeries { ns, host } => {
                    let ns = format!("ns{ns}");
                    db.delete_series("m", &tags_for(Some(&ns), &format!("h{host}"))).unwrap();
                }
                Op::Retain => {
                    db.flush().unwrap();
                    db.enforce_retention(Duration::from_secs(5 * 3600)).unwrap();
                }
            }
            for ns in 0..3 {
                let ns = format!("ns{ns}");
                let recount = series_held(&db, &ns);
                prop_assert_eq!(db.namespace_usage(&ns).series, recount, "namespace {}", ns);
            }
        }
    }
}
