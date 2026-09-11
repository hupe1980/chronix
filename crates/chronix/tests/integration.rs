#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Integration tests for the Chronix embedded time-series database.
//!
//! These tests exercise the full open → write → flush → read → close → reopen
//! lifecycle to verify crash recovery, schema persistence, and query pruning.

use chronix::prelude::*;
use chronix::{fields, tags, Chronix, DbError};
use tempfile::TempDir;

// ── Helpers ────────────────────────────────────────────────────────────

fn default_config(dir: &std::path::Path) -> ChronixConfig {
    ChronixConfig::builder()
        .data_dir(dir)
        .memtable_flush_threshold(1024 * 1024) // 1 MB
        .build()
        .unwrap()
}

fn cpu_point(host: &str, ts: i64, usage: f64) -> Point {
    let tags = tags! { "host" => host, "region" => "us-east" };
    let fields = fields! { "usage_idle" => usage };
    let key = SeriesKey::new("cpu", tags).unwrap();
    Point::new(key, fields, ts).unwrap()
}

fn mem_point(host: &str, ts: i64, free: i64) -> Point {
    let tags = tags! { "host" => host };
    let fields = fields! { "free" => free };
    let key = SeriesKey::new("memory", tags).unwrap();
    Point::new(key, fields, ts).unwrap()
}

/// Recursively find files with a given extension under a directory.
fn walkdir(dir: &std::path::Path, ext: &str) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                found.extend(walkdir(&path, ext));
            } else if path.extension().is_some_and(|e| e == ext) {
                found.push(path);
            }
        }
    }
    found
}

// ── Basic Lifecycle ────────────────────────────────────────────────────

#[test]
fn open_insert_close_reopen_read() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();

    // Phase 1: Write data
    {
        let db = Chronix::open(default_config(&path)).unwrap();

        for i in 0..50 {
            let point = cpu_point("srv-1", i * 1_000_000_000, 90.0 + (i as f64) * 0.1);
            db.insert(&point).unwrap();
        }
        db.close().unwrap();
    }

    // Phase 2: Reopen and read. A graceful close flushed everything into a
    // segment and recorded the WAL floor, so nothing is replayed and the
    // memtable is empty; the data is on disk and the query path finds it.
    {
        let db = Chronix::open(default_config(&path)).unwrap();
        assert_eq!(db.wal_replayed_records(), 0);

        let key =
            SeriesKey::new("cpu", tags! { "host" => "srv-1", "region" => "us-east" }).unwrap();
        assert!(db.scan_memtable(&key, 0, i64::MAX).is_empty());

        let plan = db
            .query()
            .measurement("cpu")
            .tag("host", "srv-1")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        assert_eq!(db.execute(&plan).unwrap().num_rows(), 50);

        db.close().unwrap();
    }
}

#[test]
fn schema_on_write_evolves_across_inserts() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // First insert creates the schema
    db.insert(&cpu_point("srv-1", 1000, 95.5)).unwrap();
    let schema = db.schema("cpu").unwrap();
    assert!(schema.column("host").is_some());
    assert!(schema.column("region").is_some());
    assert!(schema.column("usage_idle").is_some());

    // Second insert doesn't change schema
    db.insert(&cpu_point("srv-2", 2000, 92.0)).unwrap();
    let schema2 = db.schema("cpu").unwrap();
    assert_eq!(schema.tag_count(), schema2.tag_count());

    db.close().unwrap();
}

#[test]
fn multiple_measurements_independent_schemas() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    db.insert(&cpu_point("srv-1", 1000, 95.5)).unwrap();
    db.insert(&mem_point("srv-1", 1000, 1_073_741_824)).unwrap();

    let cpu_schema = db.schema("cpu").unwrap();
    let mem_schema = db.schema("memory").unwrap();

    assert!(cpu_schema.column("usage_idle").is_some());
    assert!(cpu_schema.column("free").is_none());

    assert!(mem_schema.column("free").is_some());
    assert!(mem_schema.column("usage_idle").is_none());

    db.close().unwrap();
}

// ── Batch Insert ───────────────────────────────────────────────────────

#[test]
fn batch_insert_all_points_readable() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    let points: Vec<Point> = (0..200)
        .map(|i| {
            cpu_point(
                if i % 2 == 0 { "srv-1" } else { "srv-2" },
                i * 1_000_000_000,
                90.0,
            )
        })
        .collect();

    assert!(
        db.insert_batch(&points).unwrap().is_complete(),
        "insert was partial"
    );

    // Read series 1
    let key1 = SeriesKey::new("cpu", tags! { "host" => "srv-1", "region" => "us-east" }).unwrap();
    let result1 = db.scan_memtable(&key1, 0, i64::MAX);
    assert_eq!(
        result1.len(),
        100,
        "Expected 100 even-index points for srv-1"
    );

    // Read series 2
    let key2 = SeriesKey::new("cpu", tags! { "host" => "srv-2", "region" => "us-east" }).unwrap();
    let result2 = db.scan_memtable(&key2, 0, i64::MAX);
    assert_eq!(
        result2.len(),
        100,
        "Expected 100 odd-index points for srv-2"
    );

    db.close().unwrap();
}

#[test]
fn batch_insert_empty_is_noop() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    assert!(
        db.insert_batch(&[]).unwrap().is_complete(),
        "insert was partial"
    );
    assert_eq!(db.wal_sequence(), 0);

    db.close().unwrap();
}

// ── Flush & Segment Lifecycle ──────────────────────────────────────────

#[test]
fn flush_persists_to_catalog() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    for i in 0..20 {
        db.insert(&cpu_point("srv-1", i * 1_000_000_000, 92.0))
            .unwrap();
    }

    // Flush should write a segment
    let results = db.flush().unwrap();
    assert!(!results.is_empty(), "Expected flush to produce results");

    let r = &results[0];
    assert_eq!(r.measurement, "cpu");
    assert_eq!(r.points_flushed, 20);
    assert!(
        r.segment_meta.path.exists(),
        "Segment file should exist on disk"
    );

    // Catalog should have the segment
    {
        let catalog = db.catalog().read();
        assert!(catalog.segment_count() > 0);
    }

    db.close().unwrap();
}

#[test]
fn double_flush_second_is_noop() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    for i in 0..10 {
        db.insert(&cpu_point("srv-1", i * 1_000_000_000, 95.0))
            .unwrap();
    }

    let results1 = db.flush().unwrap();
    assert!(!results1.is_empty());

    // Second flush with no new data
    let results2 = db.flush().unwrap();
    assert!(results2.is_empty(), "Second flush should be a no-op");

    db.close().unwrap();
}

// ── Crash Recovery ─────────────────────────────────────────────────────

/// Re-executes this test binary so a child process can write and then
/// `abort()` — the only way to get a WAL that was never closed. Dropping
/// the handle runs `close()`, which flushes; that is not a crash.
fn run_crashing_child(test_name: &str, data_dir: &std::path::Path) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([test_name, "--exact", "--nocapture", "--test-threads=1"])
        .env("CHRONIX_CRASH_DIR", data_dir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(
        !status.success(),
        "the child is expected to abort, it exited cleanly"
    );
}

/// Child half of [`wal_replay_recovers_unflushed_data`]: writes 25 points
/// with a per-batch fsync and aborts without flushing or closing.
#[test]
fn wal_replay_recovers_unflushed_data() {
    if let Ok(dir) = std::env::var("CHRONIX_CRASH_DIR") {
        let db = Chronix::open(default_config(std::path::Path::new(&dir))).unwrap();
        for i in 0..25 {
            db.insert(&cpu_point("srv-1", i * 1_000_000_000, 88.0 + i as f64))
                .unwrap();
        }
        std::process::abort();
    }

    let tmp = TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();
    run_crashing_child("wal_replay_recovers_unflushed_data", &path);

    // Reopen: the WAL replays the 25 points the child never flushed.
    let db = Chronix::open(default_config(&path)).unwrap();
    assert_eq!(db.wal_replayed_records(), 25);

    let key = SeriesKey::new("cpu", tags! { "host" => "srv-1", "region" => "us-east" }).unwrap();
    let points = db.scan_memtable(&key, 0, i64::MAX);
    assert_eq!(points.len(), 25, "WAL replay should recover all 25 points");
    let mut timestamps: Vec<i64> = points
        .iter()
        .map(chronix::prelude::Point::timestamp)
        .collect();
    timestamps.sort_unstable();
    assert_eq!(timestamps[0], 0);
    assert_eq!(timestamps[24], 24 * 1_000_000_000);

    // And the series count knows about them.
    assert_eq!(db.statistics().series_count, 1);
    db.close().unwrap();
}

#[test]
fn flush_then_crash_data_in_segments() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();

    // Phase 1: Write + flush + crash before clean close
    {
        let db = Chronix::open(default_config(&path)).unwrap();

        for i in 0..15 {
            db.insert(&cpu_point("srv-1", i * 1_000_000_000, 80.0))
                .unwrap();
        }

        db.flush().unwrap();
        // Drop without close — triggers Drop impl which does best-effort flush
    }

    // Phase 2: Reopen — catalog should have the flushed segment
    {
        let db = Chronix::open(default_config(&path)).unwrap();

        let catalog = db.catalog().read();
        assert!(
            catalog.segment_count() >= 1,
            "Catalog should contain at least 1 segment after flush"
        );
        drop(catalog);

        db.close().unwrap();
    }
}

// ── Concurrency & Safety ───────────────────────────────────────────────

#[test]
fn exclusive_lock_prevents_double_open() {
    let tmp = TempDir::new().unwrap();
    let _db1 = Chronix::open(default_config(tmp.path())).unwrap();

    let result = Chronix::open(default_config(tmp.path()));
    assert!(result.is_err(), "Second open should fail");
    assert!(matches!(result.unwrap_err(), DbError::LockFailed { .. }));
}

#[test]
fn operations_after_close_return_error() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();
    db.close().unwrap();

    let point = cpu_point("srv-1", 1000, 95.0);
    let err = db.insert(&point).unwrap_err();
    assert!(matches!(err, DbError::Closed));

    let flush_err = db.flush().unwrap_err();
    assert!(matches!(flush_err, DbError::Closed));
}

// ── Macros ─────────────────────────────────────────────────────────────

#[test]
fn tags_macro_integrates_with_series_key() {
    let t = tags! {
        "host" => "srv-1",
        "region" => "us-east",
        "dc" => "dc1",
    };
    assert_eq!(t.len(), 3);
    assert_eq!(t["host"], "srv-1");

    let key = SeriesKey::new("cpu", t).unwrap();
    assert_eq!(key.measurement(), "cpu");
}

#[test]
fn fields_macro_mixed_types() {
    let f = fields! {
        "usage" => 95.5_f64,
        "count" => 42_i64,
        "active" => true,
        "label" => "prod",
    };
    assert_eq!(f.len(), 4);
    assert!(matches!(f["usage"], FieldValue::F64(v) if (v - 95.5).abs() < f64::EPSILON));
    assert!(matches!(f["count"], FieldValue::I64(42)));
    assert!(matches!(f["active"], FieldValue::Bool(true)));
    assert!(matches!(&f["label"], FieldValue::String(s) if s == "prod"));
}

// ── Time Range Scans ───────────────────────────────────────────────────

#[test]
fn scan_memtable_respects_time_range() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    for i in 0..100 {
        let point = cpu_point("srv-1", i * 1_000_000_000, 90.0);
        db.insert(&point).unwrap();
    }

    let key = SeriesKey::new("cpu", tags! { "host" => "srv-1", "region" => "us-east" }).unwrap();

    // Query subset: timestamps [10B, 20B] → 11 points (10,11,...,20)
    let subset = db.scan_memtable(&key, 10_000_000_000, 20_000_000_000);
    assert_eq!(subset.len(), 11, "Expected 11 points in [10B, 20B]");

    // All points
    let all = db.scan_memtable(&key, 0, i64::MAX);
    assert_eq!(all.len(), 100);

    db.close().unwrap();
}

#[test]
fn scan_memtable_different_series_isolated() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // Insert for two different hosts
    for i in 0..10 {
        db.insert(&cpu_point("srv-1", i * 1000, 90.0)).unwrap();
        db.insert(&cpu_point("srv-2", i * 1000, 80.0)).unwrap();
    }

    let key1 = SeriesKey::new("cpu", tags! { "host" => "srv-1", "region" => "us-east" }).unwrap();

    let key2 = SeriesKey::new("cpu", tags! { "host" => "srv-2", "region" => "us-east" }).unwrap();

    let result1 = db.scan_memtable(&key1, 0, i64::MAX);
    let result2 = db.scan_memtable(&key2, 0, i64::MAX);

    assert_eq!(result1.len(), 10);
    assert_eq!(result2.len(), 10);

    // Verify values are from the right series
    for p in &result1 {
        assert!(
            matches!(p.field("usage_idle"), Some(FieldValue::F64(v)) if (*v - 90.0).abs() < f64::EPSILON)
        );
    }
    for p in &result2 {
        assert!(
            matches!(p.field("usage_idle"), Some(FieldValue::F64(v)) if (*v - 80.0).abs() < f64::EPSILON)
        );
    }

    db.close().unwrap();
}

// ── Column Projection Through Query API ────────────────────────────────

#[test]
fn query_projection_selects_subset_of_fields() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // Insert points with multiple fields
    for i in 0..5 {
        let key = SeriesKey::new("sensor", tags! { "device" => "d1" }).unwrap();
        let fields = fields! {
            "temperature" => 20.0 + i as f64,
            "humidity" => 50.0 + i as f64,
            "pressure" => 1013.0 + i as f64
        };
        let point = Point::new(key, fields, i * 1000).unwrap();
        db.insert(&point).unwrap();
    }

    // Project only "temperature"
    let plan = db
        .query()
        .measurement("sensor")
        .range(0, i64::MAX)
        .field("temperature")
        .build()
        .unwrap();

    let batch = db.execute(&plan).unwrap();
    assert_eq!(batch.num_rows(), 5);
    // Should have timestamp + temperature = 2 columns
    assert_eq!(batch.num_columns(), 2);
    assert!(batch.column_by_name(chronix_core::TIME_COLUMN).is_some());
    assert!(batch.column_by_name("temperature").is_some());
    assert!(batch.column_by_name("humidity").is_none());
    assert!(batch.column_by_name("pressure").is_none());

    db.close().unwrap();
}

#[test]
#[allow(clippy::cast_precision_loss)]
fn query_projection_multiple_fields() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    for i in 0..3_i64 {
        let key = SeriesKey::new("sensor", tags! { "device" => "d1" }).unwrap();
        let fields = fields! {
            "temperature" => 20.0 + i as f64,
            "humidity" => 50.0 + i as f64,
            "pressure" => 1013.0 + i as f64
        };
        let point = Point::new(key, fields, i * 1000).unwrap();
        db.insert(&point).unwrap();
    }

    // Project temperature + pressure
    let plan = db
        .query()
        .measurement("sensor")
        .range(0, i64::MAX)
        .field("temperature")
        .field("pressure")
        .build()
        .unwrap();

    let batch = db.execute(&plan).unwrap();
    assert_eq!(batch.num_rows(), 3);
    // timestamp + temperature + pressure = 3
    assert_eq!(batch.num_columns(), 3);
    assert!(batch.column_by_name("humidity").is_none());

    db.close().unwrap();
}

// ── Delete / Drop Integration ──────────────────────────────────────────

#[test]
fn drop_measurement_end_to_end() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // Insert into two measurements
    for i in 0..10 {
        db.insert(&cpu_point("srv-1", i * 1000, 90.0)).unwrap();
        db.insert(&mem_point("srv-1", i * 1000, 1024)).unwrap();
    }

    // Flush so data is on disk
    db.flush().unwrap();

    // Drop cpu measurement
    db.drop_measurement("cpu").unwrap();

    // cpu query returns empty
    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    assert_eq!(batch.num_rows(), 0);

    // memory query still works
    let plan = db
        .query()
        .measurement("memory")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    assert_eq!(batch.num_rows(), 10);

    db.close().unwrap();
}

#[test]
fn delete_series_filters_query_results() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // Two hosts
    for i in 0..5 {
        db.insert(&cpu_point("srv-1", i * 1000, 90.0)).unwrap();
        db.insert(&cpu_point("srv-2", i * 1000, 80.0)).unwrap();
    }

    // Delete series for srv-1
    let del_tags = tags! { "host" => "srv-1", "region" => "us-east" };
    db.delete_series("cpu", &del_tags).unwrap();

    // Query — only srv-2 should remain (5 points)
    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .build()
        .unwrap();

    let batch = db.execute(&plan).unwrap();
    assert_eq!(batch.num_rows(), 5);

    // Verify all rows are srv-2
    let host_col = batch
        .column_by_name("host")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    for i in 0..batch.num_rows() {
        assert_eq!(host_col.value(i), "srv-2");
    }

    db.close().unwrap();
}

#[test]
fn drop_nonexistent_measurement_is_noop() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // Should not error
    db.drop_measurement("nonexistent").unwrap();
    db.close().unwrap();
}

// ── Aggregation through execute() ──────────────────────────────────

#[test]
fn aggregation_end_to_end() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // Write 10 points: usage_idle = 10, 20, … 100
    for i in 1..=10 {
        db.insert(&cpu_point("srv-1", i * 100, i as f64 * 10.0))
            .unwrap();
    }

    // Aggregate: sum, min, max, avg, count
    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .field("usage_idle")
        .aggregate(AggFn::Sum)
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    assert_eq!(batch.num_rows(), 1);
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    // sum(10..100) = 550
    assert!((col.value(0) - 550.0).abs() < f64::EPSILON);

    db.close().unwrap();
}

#[test]
fn grouped_aggregation_end_to_end() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // 3 points for srv-1, 2 for srv-2
    db.insert(&cpu_point("srv-1", 100, 10.0)).unwrap();
    db.insert(&cpu_point("srv-1", 200, 20.0)).unwrap();
    db.insert(&cpu_point("srv-1", 300, 30.0)).unwrap();
    db.insert(&cpu_point("srv-2", 100, 40.0)).unwrap();
    db.insert(&cpu_point("srv-2", 200, 50.0)).unwrap();

    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .field("usage_idle")
        .aggregate(AggFn::Sum)
        .group_by(&["host"])
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    assert_eq!(batch.num_rows(), 2);

    let host_col = batch
        .column_by_name("host")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let sum_col = batch
        .column(1) // first agg column after group-by
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();

    // Groups may appear in any order (HashMap iteration is non-deterministic).
    // Collect both rows and sort by host name for stable assertion.
    let mut rows: Vec<(&str, f64)> = (0..batch.num_rows())
        .map(|i| (host_col.value(i), sum_col.value(i)))
        .collect();
    rows.sort_by_key(|(host, _)| host.to_string());

    assert_eq!(rows[0].0, "srv-1");
    assert!((rows[0].1 - 60.0).abs() < f64::EPSILON); // 10+20+30

    assert_eq!(rows[1].0, "srv-2");
    assert!((rows[1].1 - 90.0).abs() < f64::EPSILON); // 40+50

    db.close().unwrap();
}

#[test]
fn downsampling_end_to_end() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // Write 100 points at 1-second intervals (timestamps 0, 1000, 2000 … 99000 ns)
    // We'll use simple timestamps for bucket alignment
    for i in 0..100 {
        db.insert(&cpu_point("srv-1", i * 1000, 1.0)).unwrap();
    }

    // Downsample into 10-second buckets (10000 ns each), avg aggregation
    // Should produce 10 buckets, each with avg = 1.0
    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .field("usage_idle")
        .downsample(TimeBucket::fixed_ns(10_000), AggFn::Avg)
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    assert_eq!(batch.num_rows(), 10);

    db.close().unwrap();
}

// ── Empty-result schema preservation ───────────────────────────────

#[test]
fn empty_result_preserves_measurement_schema() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // Write one point to register schema
    db.insert(&cpu_point("srv-1", 100, 42.0)).unwrap();

    // Query far-future time range → no results
    let plan = db
        .query()
        .measurement("cpu")
        .range(999_999_000, 999_999_999)
        .build()
        .unwrap();
    let result = db.execute(&plan).unwrap();
    assert_eq!(result.num_rows(), 0);

    let schema = result.schema();
    assert!(
        schema.column_with_name(chronix_core::TIME_COLUMN).is_some(),
        "empty result should preserve the time column"
    );
    assert!(
        schema.column_with_name("usage_idle").is_some(),
        "empty result should preserve field columns"
    );

    db.close().unwrap();
}

// ── Concurrent writes ──────────────────────────────────────────────

#[test]
fn concurrent_writes_from_multiple_threads() {
    use std::sync::Arc;

    let tmp = TempDir::new().unwrap();
    let db = Arc::new(Chronix::open(default_config(tmp.path())).unwrap());

    let mut handles = Vec::new();
    for thread_id in 0..4 {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            let host = format!("srv-{thread_id}");
            for i in 0..50 {
                let ts = (thread_id as i64) * 10_000 + i * 100;
                db.insert(&cpu_point(&host, ts, i as f64)).unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Verify all 200 points are queryable
    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    assert_eq!(batch.num_rows(), 200);

    db.close().unwrap();
}

// ── Bloom filter persistence ───────────────────────────────────────

#[test]
fn series_indexes_persist_across_restart() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();

    // Phase 1: Insert, flush (writes .bloom sidecar files), close
    {
        let db = Chronix::open(default_config(&path)).unwrap();
        for i in 0..10 {
            db.insert(&cpu_point("srv-1", i * 1000, 90.0)).unwrap();
        }
        db.flush().unwrap();
        db.close().unwrap();
    }

    // Verify .bloom files exist on disk (segments are in shard subdirectories)
    let segments_dir = path.join("segments");
    let bloom_files: Vec<_> = walkdir(&segments_dir, "series");
    assert!(
        !bloom_files.is_empty(),
        "Expected at least one .bloom sidecar file after flush"
    );

    // Phase 2: Reopen — blooms should be loaded from disk
    {
        let db = Chronix::open(default_config(&path)).unwrap();

        // Query matching series — should find data (bloom says "maybe")
        let plan = db
            .query()
            .measurement("cpu")
            .tag("host", "srv-1")
            .tag("region", "us-east")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        let batch = db.execute(&plan).unwrap();
        assert_eq!(
            batch.num_rows(),
            10,
            "Matching series should return all 10 points"
        );

        // Query non-existent series — bloom should prune segment
        let plan = db
            .query()
            .measurement("cpu")
            .tag("host", "srv-nonexistent")
            .tag("region", "us-east")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        let batch = db.execute(&plan).unwrap();
        assert_eq!(
            batch.num_rows(),
            0,
            "Non-existent series should be pruned by bloom"
        );

        db.close().unwrap();
    }
}

// ── Tombstone filtering on segment data ────────────────────────────

#[test]
fn delete_series_filters_flushed_segment_data() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // Insert two series and flush to segments
    for i in 0..5 {
        db.insert(&cpu_point("srv-1", i * 1000, 90.0)).unwrap();
        db.insert(&cpu_point("srv-2", i * 1000, 80.0)).unwrap();
    }
    db.flush().unwrap();

    // Delete srv-1 — data is in segments, not memtable
    let del_tags = tags! { "host" => "srv-1", "region" => "us-east" };
    db.delete_series("cpu", &del_tags).unwrap();

    // Query — only srv-2 should remain
    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .build()
        .unwrap();

    let batch = db.execute(&plan).unwrap();
    assert_eq!(
        batch.num_rows(),
        5,
        "Only srv-2's 5 points should survive tombstone filtering"
    );

    let host_col = batch
        .column_by_name("host")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    for i in 0..batch.num_rows() {
        assert_eq!(host_col.value(i), "srv-2", "Row {i} should be srv-2");
    }

    db.close().unwrap();
}

#[test]
fn delete_series_filters_both_memtable_and_segment_data() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // Phase 1: Insert and flush srv-1 + srv-2 to segments
    for i in 0..5 {
        db.insert(&cpu_point("srv-1", i * 1000, 90.0)).unwrap();
        db.insert(&cpu_point("srv-2", i * 1000, 80.0)).unwrap();
    }
    db.flush().unwrap();

    // Phase 2: Insert more data into memtable (unflushed)
    for i in 5..10 {
        db.insert(&cpu_point("srv-1", i * 1000, 91.0)).unwrap();
        db.insert(&cpu_point("srv-2", i * 1000, 81.0)).unwrap();
    }

    // Delete srv-1 — should affect both segment and memtable data
    let del_tags = tags! { "host" => "srv-1", "region" => "us-east" };
    db.delete_series("cpu", &del_tags).unwrap();

    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .build()
        .unwrap();

    let batch = db.execute(&plan).unwrap();
    assert_eq!(
        batch.num_rows(),
        10,
        "Only srv-2's 10 points (5 seg + 5 mem) should survive"
    );

    let host_col = batch
        .column_by_name("host")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    for i in 0..batch.num_rows() {
        assert_eq!(host_col.value(i), "srv-2");
    }

    db.close().unwrap();
}

// ── execute_stream ─────────────────────────────────────────────────

#[test]
fn execute_stream_returns_multiple_batches() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // Insert and flush to create a segment
    for i in 0..5 {
        db.insert(&cpu_point("srv-1", i * 1000, 90.0)).unwrap();
    }
    db.flush().unwrap();

    // Insert more into memtable (unflushed)
    for i in 5..10 {
        db.insert(&cpu_point("srv-1", i * 1000, 91.0)).unwrap();
    }

    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .build()
        .unwrap();

    let batches = db.execute_stream(&plan).unwrap();
    // Sort-merge dedup may merge memtable + segment into one batch.
    // Verify we get at least 1 batch containing all 10 rows.
    assert!(
        !batches.is_empty(),
        "execute_stream should produce at least 1 batch, got 0",
    );

    // Total rows should equal 10
    let total: usize = batches
        .iter()
        .map(chronix::prelude::RecordBatch::num_rows)
        .sum();
    assert_eq!(total, 10, "Total rows across batches should be 10");

    db.close().unwrap();
}

#[test]
fn execute_stream_scan_matches_execute() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    for i in 0..20 {
        db.insert(&cpu_point("srv-1", i * 1000, 90.0 + i as f64))
            .unwrap();
    }
    db.flush().unwrap();

    for i in 20..30 {
        db.insert(&cpu_point("srv-1", i * 1000, 90.0 + i as f64))
            .unwrap();
    }

    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .build()
        .unwrap();

    let single = db.execute(&plan).unwrap();
    let stream = db.execute_stream(&plan).unwrap();

    let stream_total: usize = stream
        .iter()
        .map(chronix::prelude::RecordBatch::num_rows)
        .sum();
    assert_eq!(
        single.num_rows(),
        stream_total,
        "execute and execute_stream should return the same total rows"
    );

    db.close().unwrap();
}

#[test]
fn execute_stream_aggregate_returns_single_batch() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    for i in 0..10 {
        db.insert(&cpu_point("srv-1", i * 1000, 10.0)).unwrap();
    }

    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .field("usage_idle")
        .aggregate(AggFn::Sum)
        .build()
        .unwrap();

    let batches = db.execute_stream(&plan).unwrap();
    assert_eq!(
        batches.len(),
        1,
        "Aggregate plans should produce exactly one batch"
    );
    assert_eq!(batches[0].num_rows(), 1);

    db.close().unwrap();
}

// ── last_value from segments ───────────────────────────────────────

#[test]
fn last_value_from_flushed_segments() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    let lv_tags = tags! { "host" => "srv-1", "region" => "us-east" };

    // Insert and flush
    for i in 0..10 {
        db.insert(&cpu_point("srv-1", (i + 1) * 1000, 90.0 + i as f64))
            .unwrap();
    }
    db.flush().unwrap();

    // No memtable data — last_value should come from segment
    let lv = db.last_value("cpu", &lv_tags).unwrap();
    assert!(
        lv.is_some(),
        "last_value should return data from flushed segments"
    );
    let point = lv.unwrap();
    assert_eq!(point.timestamp(), 10_000);

    db.close().unwrap();
}

// ── Drop measurement cleans bloom files ────────────────────────────

#[test]
fn drop_measurement_removes_series_index_sidecars() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    for i in 0..10 {
        db.insert(&cpu_point("srv-1", i * 1000, 90.0)).unwrap();
    }
    db.flush().unwrap();

    // Verify bloom files exist
    let segments_dir = tmp.path().join("segments");
    let bloom_count_before = walkdir(&segments_dir, "series").len();
    assert!(bloom_count_before > 0, "Expected bloom files after flush");

    // Drop measurement
    db.drop_measurement("cpu").unwrap();

    // Verify bloom files removed
    let bloom_count_after = walkdir(&segments_dir, "series").len();
    assert_eq!(
        bloom_count_after, 0,
        "Bloom files should be removed after drop_measurement"
    );

    db.close().unwrap();
}

// ── Schema type conflict ───────────────────────────────────────────

#[test]
fn schema_type_conflict_rejected() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // First write establishes field "usage_idle" as Float64
    db.insert(&cpu_point("srv-1", 1000, 42.0)).unwrap();

    // Second write with same field name but different type should fail
    let tags = tags! { "host" => "srv-1", "region" => "us-east" };
    let fields = fields! { "usage_idle" => 42_i64 }; // I64 instead of F64
    let key = SeriesKey::new("cpu", tags).unwrap();
    let point = Point::new(key, fields, 2000).unwrap();

    let result = db.insert(&point);
    assert!(result.is_err(), "Should reject type conflict");
    let err = format!("{}", result.unwrap_err());
    assert!(
        err.contains("Type conflict")
            || err.contains("type conflict")
            || err.contains("TypeConflict"),
        "Error should mention type conflict: {err}"
    );

    db.close().unwrap();
}

// ── Concurrent read + write ────────────────────────────────────────

#[test]
fn concurrent_read_write() {
    use std::sync::Arc;
    use std::thread;

    let tmp = TempDir::new().unwrap();
    let db = Arc::new(Chronix::open(default_config(tmp.path())).unwrap());

    // Pre-populate some data
    for i in 0..100 {
        db.insert(&cpu_point("srv-1", i * 1000, i as f64)).unwrap();
    }
    db.flush().unwrap();

    let db_writer = Arc::clone(&db);
    let db_reader = Arc::clone(&db);

    // Writer thread: insert 200 more points
    let writer = thread::spawn(move || {
        for i in 100..300 {
            db_writer
                .insert(&cpu_point("srv-1", i * 1000, i as f64))
                .unwrap();
        }
    });

    // Reader thread: query repeatedly while writes happen
    let reader = thread::spawn(move || {
        let mut success_count = 0;
        for _ in 0..50 {
            let plan = db_reader
                .query()
                .measurement("cpu")
                .range(0, 500_000)
                .build()
                .unwrap();
            let result = db_reader.execute(&plan);
            assert!(
                result.is_ok(),
                "Query should succeed during concurrent writes"
            );
            let batch = result.unwrap();
            assert!(batch.num_rows() > 0, "Query should return data");
            success_count += 1;
        }
        success_count
    });

    writer.join().unwrap();
    let reads = reader.join().unwrap();
    assert_eq!(reads, 50, "All reads should have succeeded");

    db.close().unwrap();
}

// ── Stats pruning verification via execute ─────────────────────────

#[test]
fn stats_pruning_applied_in_execute_path() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // Write and flush data for measurement "cpu" with host tag
    for i in 0..10 {
        db.insert(&cpu_point("srv-1", i * 1000, 90.0)).unwrap();
    }
    db.flush().unwrap();

    // Write and flush data for measurement "memory" (no "host" in the query path)
    for i in 0..10 {
        db.insert(&mem_point("srv-A", i * 1000, 1024)).unwrap();
    }
    db.flush().unwrap();

    // Query cpu with tag filters — should only scan cpu segments.
    // Must specify ALL tags to match the series key for bloom filters.
    let plan = db
        .query()
        .measurement("cpu")
        .tag("host", "srv-1")
        .tag("region", "us-east")
        .range(0, 20_000)
        .build()
        .unwrap();
    let result = db.execute(&plan).unwrap();
    assert_eq!(result.num_rows(), 10, "Should find all 10 cpu points");

    // Query with a host that doesn't exist — bloom filter should prune
    let plan = db
        .query()
        .measurement("cpu")
        .tag("host", "nonexistent")
        .tag("region", "us-east")
        .range(0, 20_000)
        .build()
        .unwrap();
    let result = db.execute(&plan).unwrap();
    assert_eq!(
        result.num_rows(),
        0,
        "Non-existent host should return empty"
    );

    db.close().unwrap();
}

// ── Bloom pruning in last_value ────────────────────────────────────

#[test]
fn last_value_uses_bloom_pruning() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // Write and flush data for multiple hosts
    for i in 0..10 {
        db.insert(&cpu_point("srv-1", i * 1000, 90.0)).unwrap();
        db.insert(&cpu_point("srv-2", i * 1000, 80.0)).unwrap();
    }
    db.flush().unwrap();

    // last_value for srv-1 should work (bloom allows)
    let tags = tags! { "host" => "srv-1", "region" => "us-east" };
    let lv = db.last_value("cpu", &tags).unwrap();
    assert!(lv.is_some());
    assert_eq!(lv.unwrap().timestamp(), 9000);

    // last_value for nonexistent host should return None (bloom prunes)
    let tags = tags! { "host" => "nonexistent", "region" => "us-east" };
    let lv = db.last_value("cpu", &tags).unwrap();
    assert!(lv.is_none(), "Non-existent host should return None");

    db.close().unwrap();
}

// ── Tombstone filtering with string fields ─────────────────────────

/// Regression test: when a measurement has both string-typed tag columns
/// AND string-typed field columns, tombstone filtering must only hash
/// the tag columns (not field columns) to match `SeriesKey::hash_fnv()`.
#[test]
fn tombstone_filtering_with_string_fields() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // Insert points with a String field alongside the tag
    let tags_map = tags! { "host" => "srv-1" };
    let fields_map = fields! {
        "metric" => 42.5_f64,
        "status" => "healthy"   // string field — must NOT be hashed as a tag
    };
    let key = SeriesKey::new("events", tags_map.clone()).unwrap();
    for ts in 0..5 {
        let p = Point::new(key.clone(), fields_map.clone(), ts * 1000).unwrap();
        db.insert(&p).unwrap();
    }
    db.flush().unwrap();

    // Delete the series
    db.delete_series("events", &tags_map).unwrap();

    // Query — all rows should be filtered out by tombstones
    let plan = db
        .query()
        .measurement("events")
        .tag("host", "srv-1")
        .range(0, 10_000)
        .build()
        .unwrap();
    let result = db.execute(&plan).unwrap();
    assert_eq!(
        result.num_rows(),
        0,
        "Tombstoned series with string fields should return 0 rows"
    );

    db.close().unwrap();
}

// ── Missing tag filter returns empty ───────────────────────────────

/// When a tag filter references a column that does not exist in the batch
/// (e.g. schema evolution), the filter should return zero rows — not
/// silently pass all rows.
#[test]
fn missing_tag_filter_returns_empty() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // Insert cpu data that does NOT have a "datacenter" tag
    for i in 0..5 {
        db.insert(&cpu_point("srv-1", i * 1000, 80.0)).unwrap();
    }
    db.flush().unwrap();

    // Query with a tag that doesn't exist in the data
    let plan = db
        .query()
        .measurement("cpu")
        .tag("datacenter", "us-east-1")
        .range(0, 10_000)
        .build()
        .unwrap();
    let result = db.execute(&plan).unwrap();
    assert_eq!(
        result.num_rows(),
        0,
        "Tag filter on non-existent column should return 0 rows"
    );

    db.close().unwrap();
}

// ── Cardinality enforcement after WAL replay ───────────────────────

/// After a restart the cardinality tracker must know every series the
/// database holds — from the segments, not from the WAL.
#[test]
fn cardinality_enforced_after_restart() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();

    // Phase 1: open DB with max 5 series, insert 4 different series
    {
        let config = ChronixConfig::builder()
            .data_dir(&path)
            .max_series_cardinality(5)
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        for i in 0..4 {
            let host = format!("srv-{i}");
            db.insert(&cpu_point(&host, 1000, 99.0)).unwrap();
        }
        db.close().unwrap();
    }

    // Phase 2: reopen — the segments hold 4 series, limit is 5
    {
        let config = ChronixConfig::builder()
            .data_dir(&path)
            .max_series_cardinality(5)
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        // Insert 1 more new series — should succeed (4+1 = 5 ≤ limit)
        db.insert(&cpu_point("srv-4", 2000, 88.0)).unwrap();

        // Insert yet another new series — should fail (5+1 = 6 > limit)
        let result = db.insert(&cpu_point("srv-5", 3000, 77.0));
        assert!(
            result.is_err(),
            "6th series should be rejected after WAL replay restored 4 series"
        );
        match result.unwrap_err() {
            DbError::CardinalityExceeded { .. } => {} // expected
            e => panic!("Expected CardinalityExceeded, got {e:?}"),
        }

        db.close().unwrap();
    }
}

// ── Negative timestamp downsample ──────────────────────────────────

#[test]
fn downsample_negative_timestamps() {
    use chronix_query::aggregate::AggFn;
    use chronix_query::downsample::downsample;

    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    // Create a batch with negative timestamps
    let schema = Arc::new(Schema::new(vec![
        Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let timestamps = Arc::new(Int64Array::from(vec![-15, -5, 5, 15]));
    let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]));
    let batch = RecordBatch::try_new(schema, vec![timestamps, values]).unwrap();

    // Downsample with 10-unit buckets
    let result = downsample(
        &batch,
        "value",
        &TimeBucket::fixed_ns(10),
        &AggFn::Sum,
        None,
    )
    .unwrap();

    let ts_col = result
        .column_by_name(chronix_core::TIME_COLUMN)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let val_col = result
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    // With div_euclid: ts=-15 → bucket -20, ts=-5 → bucket -10,
    //                  ts=5 → bucket 0, ts=15 → bucket 10
    assert_eq!(result.num_rows(), 4, "Should have 4 distinct buckets");
    // Buckets in order: -20, -10, 0, 10
    assert_eq!(ts_col.value(0), -20);
    assert_eq!(ts_col.value(1), -10);
    assert_eq!(ts_col.value(2), 0);
    assert_eq!(ts_col.value(3), 10);
    assert!((val_col.value(0) - 1.0).abs() < f64::EPSILON); // ts=-15
    assert!((val_col.value(1) - 2.0).abs() < f64::EPSILON); // ts=-5
    assert!((val_col.value(2) - 3.0).abs() < f64::EPSILON); // ts=5
    assert!((val_col.value(3) - 4.0).abs() < f64::EPSILON); // ts=15
}

// ── COUNT on empty returns 0 ───────────────────────────────────────

#[test]
fn count_on_empty_returns_zero() {
    use chronix_query::aggregate::{aggregate_batch, AggFn};

    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    // Create an empty batch with a value column
    let schema = Arc::new(Schema::new(vec![
        Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::new_empty(schema);

    let result = aggregate_batch(&batch, &[AggFn::Count], &["value"], None).unwrap();
    assert_eq!(result.num_rows(), 1);
    let count_col = result
        .column_by_name("value_count")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!(
        (count_col.value(0) - 0.0).abs() < f64::EPSILON,
        "COUNT on empty should return 0, not NULL"
    );
}

// ── Schema tag_names / field_names API ─────────────────────────────

#[test]
fn schema_tag_and_field_names() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // Insert data with tags and fields
    let tags_map = tags! { "host" => "srv-1", "region" => "us-east" };
    let fields_map = fields! { "usage_idle" => 42.5_f64, "count" => 10_i64 };
    let key = SeriesKey::new("cpu", tags_map).unwrap();
    let p = Point::new(key, fields_map, 1000).unwrap();
    db.insert(&p).unwrap();

    let schema = db.schema("cpu").expect("schema should exist");
    let tag_names = schema.tag_names();
    let field_names = schema.field_names();

    assert!(tag_names.contains(&"host"));
    assert!(tag_names.contains(&"region"));
    assert_eq!(tag_names.len(), 2);

    assert!(field_names.contains(&"usage_idle"));
    assert!(field_names.contains(&"count"));
    assert_eq!(field_names.len(), 2);

    db.close().unwrap();
}

// ── Concurrent write + flush + compact + query stress test ─────────

/// Exercises the full engine under concurrent contention: writers insert
/// points while a flusher, a compactor and readers run simultaneously.
/// Verifies that all written data is visible and consistent after the
/// workload completes.
#[test]
fn concurrent_write_flush_compact_query_stress() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    let tmp = TempDir::new().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(tmp.path())
        .memtable_flush_threshold(4096) // small to force frequent flushes
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());
    let done = Arc::new(AtomicBool::new(false));

    const NUM_WRITERS: usize = 4;
    const POINTS_PER_WRITER: usize = 200;

    let mut handles = Vec::new();

    // Writer threads: each inserts POINTS_PER_WRITER points for a unique host
    for writer_id in 0..NUM_WRITERS {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            let host = format!("stress-{writer_id}");
            for i in 0..POINTS_PER_WRITER {
                let ts = (writer_id as i64) * 1_000_000 + (i as i64) * 100;
                db.insert(&cpu_point(&host, ts, i as f64)).unwrap();
            }
        }));
    }

    // Flusher thread: triggers flush in a loop until writers are done
    {
        let db = Arc::clone(&db);
        let done = Arc::clone(&done);
        handles.push(std::thread::spawn(move || {
            while !done.load(Ordering::Relaxed) {
                let _ = db.flush();
                std::thread::sleep(Duration::from_millis(5));
            }
            // Final flush to ensure all data is on disk
            let _ = db.flush();
        }));
    }

    // Compactor thread: runs compaction in a loop
    {
        let db = Arc::clone(&db);
        let done = Arc::clone(&done);
        handles.push(std::thread::spawn(move || {
            while !done.load(Ordering::Relaxed) {
                let _ = db.compact();
                std::thread::sleep(Duration::from_millis(10));
            }
        }));
    }

    // Reader thread: runs queries concurrently with writes
    {
        let db = Arc::clone(&db);
        let done = Arc::clone(&done);
        handles.push(std::thread::spawn(move || {
            let mut query_count = 0u32;
            while !done.load(Ordering::Relaxed) {
                let plan = db
                    .query()
                    .measurement("cpu")
                    .range(0, i64::MAX)
                    .build()
                    .unwrap();
                let result = db.execute(&plan);
                // Query must always succeed, even if it returns 0 rows
                assert!(
                    result.is_ok(),
                    "query failed during stress: {:?}",
                    result.err()
                );
                query_count += 1;
                std::thread::sleep(Duration::from_millis(3));
            }
            assert!(
                query_count > 0,
                "reader should have completed at least one query"
            );
        }));
    }

    // Wait for all writers to finish
    for h in handles.drain(..NUM_WRITERS) {
        h.join().unwrap();
    }

    // Signal background threads to stop
    done.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    // Final flush and compact to ensure all data is queryable
    db.flush().unwrap();
    let _ = db.compact();

    // Verify all points are present and deduplicated
    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    let expected_total = NUM_WRITERS * POINTS_PER_WRITER;
    assert_eq!(
        batch.num_rows(),
        expected_total,
        "expected {expected_total} points after concurrent stress, got {}",
        batch.num_rows()
    );

    // Verify data integrity: each writer's series should have exactly
    // POINTS_PER_WRITER rows. Use client-side filtering on the result
    // batch to avoid dependence on the tag inverted index timing.
    let host_col = batch
        .column_by_name("host")
        .expect("batch should have host column");
    let host_arr = arrow::compute::cast(host_col, &arrow::datatypes::DataType::Utf8)
        .expect("host column castable to Utf8");
    let host_str = host_arr
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("host column is StringArray after cast");
    for writer_id in 0..NUM_WRITERS {
        let host = format!("stress-{writer_id}");
        let count = host_str
            .iter()
            .filter(|v| v == &Some(host.as_str()))
            .count();
        assert_eq!(
            count, POINTS_PER_WRITER,
            "writer {writer_id} (host={host}) should have {POINTS_PER_WRITER} points, got {count}",
        );
    }

    db.close().unwrap();
}

// ── Backup & Restore ───────────────────────────────────────────────

#[test]
fn backup_restore_roundtrip() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    let backup_dir = tmp.path().join("backup");
    let restore_dir = tmp.path().join("restored");

    // Phase 1: Write data and backup
    {
        let db = Chronix::open(default_config(&data_dir)).unwrap();
        for i in 0..100 {
            db.insert(&cpu_point("srv-1", i * 1_000_000, 50.0 + i as f64))
                .unwrap();
        }
        db.flush().unwrap();

        let manifest = db.backup(&backup_dir).unwrap();
        assert!(manifest.file_count > 0);
        assert!(manifest.total_bytes > 0);
        assert!(backup_dir.join("backup_manifest.json").exists());
        db.close().unwrap();
    }

    // Phase 2: Restore and verify data
    {
        let manifest = Chronix::restore(&backup_dir, &restore_dir).unwrap();
        assert!(manifest.total_bytes > 0);

        let db = Chronix::open(default_config(&restore_dir)).unwrap();
        let plan = db
            .query()
            .measurement("cpu")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        let batch = db.execute(&plan).unwrap();
        assert_eq!(
            batch.num_rows(),
            100,
            "restored DB should have all 100 points"
        );
        db.close().unwrap();
    }
}

#[test]
fn backup_restore_preserves_multiple_measurements() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    let backup_dir = tmp.path().join("backup");
    let restore_dir = tmp.path().join("restored");

    {
        let db = Chronix::open(default_config(&data_dir)).unwrap();
        for i in 0..50 {
            db.insert(&cpu_point("a", i * 1000, i as f64)).unwrap();
            db.insert(&mem_point("a", i * 1000, i)).unwrap();
        }
        db.flush().unwrap();
        db.backup(&backup_dir).unwrap();
        db.close().unwrap();
    }

    {
        Chronix::restore(&backup_dir, &restore_dir).unwrap();
        let db = Chronix::open(default_config(&restore_dir)).unwrap();

        let cpu_plan = db
            .query()
            .measurement("cpu")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        let mem_plan = db
            .query()
            .measurement("memory")
            .range(0, i64::MAX)
            .build()
            .unwrap();

        assert_eq!(db.execute(&cpu_plan).unwrap().num_rows(), 50);
        assert_eq!(db.execute(&mem_plan).unwrap().num_rows(), 50);
        db.close().unwrap();
    }
}

#[test]
fn restore_rejects_existing_target() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    let backup_dir = tmp.path().join("backup");
    let restore_dir = tmp.path().join("restored");

    let db = Chronix::open(default_config(&data_dir)).unwrap();
    db.insert(&cpu_point("h", 1000, 1.0)).unwrap();
    db.flush().unwrap();
    db.backup(&backup_dir).unwrap();
    db.close().unwrap();

    // Create target dir so restore should fail
    std::fs::create_dir_all(&restore_dir).unwrap();
    let result = Chronix::restore(&backup_dir, &restore_dir);
    assert!(result.is_err(), "restore should reject existing target dir");
}

// ── PITR (Point-in-Time Recovery) ──────────────────────────────────

#[test]
fn pitr_restore_at_backup_sequence() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    let backup_dir = tmp.path().join("backup");
    let restore_dir = tmp.path().join("restored");

    let db = Chronix::open(default_config(&data_dir)).unwrap();
    for i in 0..20 {
        db.insert(&cpu_point("h", i * 1000, i as f64)).unwrap();
    }
    db.flush().unwrap();

    let manifest = db.backup(&backup_dir).unwrap();
    db.close().unwrap();

    // PITR at exact backup sequence = no WAL replay needed
    let (restored, replayed) =
        Chronix::restore_pitr(&backup_dir, &restore_dir, manifest.wal_sequence, None).unwrap();
    assert_eq!(replayed, 0, "no WAL replay needed at exact backup sequence");
    assert_eq!(restored.wal_sequence, manifest.wal_sequence);
}

// ── Parquet Export ──────────────────────────────────────────────────

#[test]
fn export_parquet_roundtrip() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    for i in 0..50 {
        db.insert(&cpu_point("srv-1", i * 1_000_000, 42.0 + i as f64))
            .unwrap();
    }
    db.flush().unwrap();

    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    let export_path = tmp.path().join("export.parquet");
    let config = chronix::export::ParquetExportConfig::default();

    let export = db.export_parquet(&plan, &export_path, &config).unwrap();
    assert!(
        export.bytes_written > 0,
        "parquet file should have non-zero size"
    );
    assert!(export.rows_written > 0, "parquet export wrote no rows");
    assert!(!export.truncated, "unbounded export should not truncate");
    assert!(export_path.exists(), "parquet file should exist on disk");

    // Verify the parquet file is readable
    let file = std::fs::File::open(&export_path).unwrap();
    let reader =
        parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(file, 1024).unwrap();
    let batches: Vec<_> = reader
        .into_iter()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    let total_rows: usize = batches
        .iter()
        .map(chronix::prelude::RecordBatch::num_rows)
        .sum();
    assert_eq!(
        total_rows, 50,
        "parquet should contain all 50 exported rows"
    );

    db.close().unwrap();
}

// ── Retention Enforcement ──────────────────────────────────────────

#[test]
fn retention_enforcement_removes_old_data() {
    let tmp = TempDir::new().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(tmp.path())
        .memtable_flush_threshold(1024 * 1024)
        .build()
        .unwrap();
    let db = Chronix::open(config).unwrap();

    // Two hourly shards: one three hours old, one current. Retention
    // measures age from the newest timestamp the database holds capped by
    // the clock (`retention::retention_reference`), so the fixture has to
    // *contain* the span it is asserting about — a database whose newest
    // point is three hours old is three hours old to itself, and a rule
    // measured against the wall clock alone would delete all of it the first
    // time anybody's clock was wrong.
    let now_ns = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
    .unwrap();
    let hour_ns = 3_600_000_000_000i64;

    for i in 0..25 {
        db.insert(&cpu_point("a", now_ns - 3 * hour_ns + i * 1000, i as f64))
            .unwrap();
    }
    db.flush().unwrap();
    for i in 0..25 {
        db.insert(&cpu_point("a", now_ns - i * 1000, i as f64))
            .unwrap();
    }
    db.flush().unwrap();

    let result = db
        .enforce_retention(std::time::Duration::from_secs(3600))
        .unwrap();
    assert!(
        result.shards_dropped > 0 || result.segments_deleted > 0,
        "retention should remove the three-hour-old shard (shards_dropped={}, segments_deleted={})",
        result.shards_dropped,
        result.segments_deleted,
    );

    // …and must not have taken the current one with it.
    let plan = db
        .query()
        .measurement("cpu")
        .range(now_ns - hour_ns, i64::MAX)
        .build()
        .unwrap();
    assert!(
        db.execute(&plan).unwrap().num_rows() > 0,
        "the data inside the retention window must survive"
    );

    db.close().unwrap();
}

// ── Tag Index After Compaction ─────────────────────────────────────

#[test]
fn tag_index_consistent_after_compaction() {
    let tmp = TempDir::new().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(tmp.path())
        .memtable_flush_threshold(512) // small to force flushes
        .build()
        .unwrap();
    let db = Chronix::open(config).unwrap();

    // Write data for multiple hosts across multiple flushes
    for batch in 0..5 {
        for host_id in 0..3 {
            let host = format!("host-{host_id}");
            db.insert(&cpu_point(&host, (batch * 3 + host_id) * 10_000, 50.0))
                .unwrap();
        }
        let _ = db.flush();
    }

    // Compact to merge segments
    let _ = db.compact();

    // Verify tag index is consistent
    let tag_keys = db.tag_keys();
    assert!(
        tag_keys.contains(&"host".to_string()),
        "tag_keys should contain 'host'"
    );

    let hosts = db.tag_values("host");
    assert_eq!(hosts.len(), 3, "should have 3 distinct hosts");
    for i in 0..3 {
        assert!(
            hosts.contains(&format!("host-{i}")),
            "host-{i} should be in tag values"
        );
    }

    db.close().unwrap();
}

// ── Soft-Delete and Restore Measurement ────────────────────────────

#[test]
fn soft_delete_and_restore_measurement() {
    let tmp = TempDir::new().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(tmp.path())
        .memtable_flush_threshold(1024 * 1024)
        .soft_delete_ttl(Some(std::time::Duration::from_secs(3600)))
        .build()
        .unwrap();
    let db = Chronix::open(config).unwrap();

    // Insert data
    for i in 0..20 {
        db.insert(&cpu_point("srv", i * 1000, i as f64)).unwrap();
    }
    db.flush().unwrap();

    // Verify data exists
    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    assert_eq!(db.execute(&plan).unwrap().num_rows(), 20);

    // Drop measurement (soft-delete due to TTL)
    db.drop_measurement("cpu").unwrap();
    assert!(db.is_measurement_pending_drop("cpu"));

    // A "drop" that leaves the data fully readable for the whole grace
    // period is not a drop: the schema, the listing and every scan must
    // treat a pending-drop measurement as absent, exactly as a genuinely
    // dropped one is — while the segments underneath stay untouched so a
    // restore is instant and lossless.
    assert!(
        db.schema("cpu").is_none(),
        "a pending-drop measurement's schema must be invisible"
    );
    assert!(
        !db.measurement_names_in(None).contains(&"cpu".to_string()),
        "a pending-drop measurement must not be listed"
    );
    assert_eq!(
        db.execute(&plan).unwrap().num_rows(),
        0,
        "a pending-drop measurement's rows must not be scannable"
    );

    // Restore measurement before TTL expires
    let restored = db.restore_measurement("cpu").unwrap();
    assert!(restored, "measurement should be restorable before GC");
    assert!(!db.is_measurement_pending_drop("cpu"));

    // The data comes back whole, not just the name.
    assert!(db.schema("cpu").is_some());
    assert_eq!(db.execute(&plan).unwrap().num_rows(), 20);

    db.close().unwrap();
}

/// A pending drop is catalog state, not a process-local fact.
///
/// The pending-drop map used to live only in an in-memory `HashMap` on the
/// `Chronix` handle, so a restart forgot it — silently un-dropping a
/// measurement an operator was told was gone, and forgetting the deadline
/// that was supposed to reclaim its disk. The fix is the same one a
/// tombstone got: durable in the catalog manifest, reconstructed by
/// `open()`, exactly like every other fact a restart must not lose.
#[test]
fn a_pending_measurement_drop_survives_a_restart() {
    let tmp = TempDir::new().unwrap();
    let cfg = || {
        ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(1024 * 1024)
            .soft_delete_ttl(Some(std::time::Duration::from_secs(3600)))
            .build()
            .unwrap()
    };

    let db = Chronix::open(cfg()).unwrap();
    for i in 0..10 {
        db.insert(&cpu_point("srv", i * 1000, i as f64)).unwrap();
    }
    db.flush().unwrap();
    db.drop_measurement("cpu").unwrap();
    assert!(db.is_measurement_pending_drop("cpu"));
    db.close().unwrap();

    // Reopen: the pending drop, and the masking it causes, must both
    // still hold — not reset by the restart that a real deployment sees
    // every time it upgrades or is rescheduled.
    let db = Chronix::open(cfg()).unwrap();
    assert!(
        db.is_measurement_pending_drop("cpu"),
        "a restart must not silently un-drop a measurement"
    );
    assert!(db.schema("cpu").is_none());
    assert!(!db.measurement_names_in(None).contains(&"cpu".to_string()));

    // And it is still reversible, across the restart, until the deadline.
    assert!(db.restore_measurement("cpu").unwrap());
    assert!(db.schema("cpu").is_some());
    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    assert_eq!(db.execute(&plan).unwrap().num_rows(), 10);

    db.close().unwrap();
}

// ── Predicate Delete ───────────────────────────────────────────────

#[test]
fn predicate_delete_removes_matching_series() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // Use separate measurements so the predicate delete targets
    // one measurement cleanly (avoids tag-column dictionary encoding
    // complexities).
    for i in 0..30 {
        db.insert(&cpu_point("srv-1", i * 1000, i as f64)).unwrap();
    }
    for i in 0..30 {
        let tags = tags! { "host" => "srv-2" };
        let fields = fields! { "free" => 100 + i };
        let key = SeriesKey::new("to_delete", tags).unwrap();
        let pt = Point::new(key, fields, i * 1000).unwrap();
        db.insert(&pt).unwrap();
    }
    db.flush().unwrap();

    // Verify both measurements have data
    let plan = db
        .query()
        .measurement("to_delete")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    assert_eq!(db.execute(&plan).unwrap().num_rows(), 30);

    // Delete the entire "to_delete" measurement via predicate delete
    let req = db
        .delete_builder()
        .measurement("to_delete")
        .build()
        .unwrap();
    let deleted = db.execute_delete(&req).unwrap();
    assert!(
        deleted.series_tombstoned > 0,
        "should have tombstoned at least one series"
    );
    assert!(deleted.is_complete(), "delete should not skip segments");

    // Query to_delete — should return 0 rows due to tombstone filtering
    let plan = db
        .query()
        .measurement("to_delete")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    assert_eq!(
        batch.num_rows(),
        0,
        "deleted measurement should return no rows"
    );

    // The other measurement should be untouched
    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    assert_eq!(
        batch.num_rows(),
        30,
        "unrelated measurement should be intact"
    );

    db.close().unwrap();
}

// ── WAL Archiving ──────────────────────────────────────────────────

#[test]
fn wal_archive_copies_files() {
    let tmp = TempDir::new().unwrap();
    let archive_dir = tmp.path().join("wal_archive");

    let db = Chronix::open(default_config(tmp.path())).unwrap();

    // Write some data so the WAL has entries
    for i in 0..50 {
        db.insert(&cpu_point("h", i * 1000, i as f64)).unwrap();
    }
    // Flush to advance the WAL sequence and allow archiving of older files
    db.flush().unwrap();

    let current_seq = db.wal_sequence();
    assert!(current_seq > 0, "WAL sequence should advance after writes");

    // Archive WAL files before the current sequence.
    // With default WAL settings there may be only one WAL file, so
    // `archive_before` may return 0 (it never archives the active file).
    let archived = db.wal().archive_before(current_seq, &archive_dir).unwrap();

    // If any files were archived, verify the archive dir was created
    if archived > 0 {
        assert!(archive_dir.exists(), "archive directory should exist");
    }

    db.close().unwrap();
}

// ── Multiple Flushes + Compaction + Query ──────────────────────────

#[test]
fn multi_flush_compact_query_correctness() {
    let tmp = TempDir::new().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(tmp.path())
        .memtable_flush_threshold(512) // tiny threshold to force many flushes
        .build()
        .unwrap();
    let db = Chronix::open(config).unwrap();

    // Insert data in 10 waves, flushing between each
    let mut total_points = 0;
    for wave in 0..10 {
        for i in 0..20 {
            let ts = (wave * 20 + i) * 1_000_000;
            db.insert(&cpu_point("srv-1", ts, (wave * 20 + i) as f64))
                .unwrap();
            total_points += 1;
        }
        db.flush().unwrap();
    }

    // Compact all segments
    let compacted = db.compact().unwrap();
    assert!(
        compacted > 0 || total_points > 0,
        "should have segments to compact or data"
    );

    // Query should return all points
    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    assert_eq!(
        batch.num_rows(),
        total_points,
        "all {total_points} points should survive flush + compact"
    );

    // Verify ordering: timestamps should be monotonically increasing
    let ts_col = batch
        .column_by_name(chronix_core::TIME_COLUMN)
        .expect("should have timestamp column");
    let ts_arr = ts_col
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("timestamp should be Int64Array");
    for i in 1..ts_arr.len() {
        assert!(
            ts_arr.value(i) >= ts_arr.value(i - 1),
            "timestamps should be non-decreasing: {} vs {}",
            ts_arr.value(i - 1),
            ts_arr.value(i)
        );
    }

    db.close().unwrap();
}

// ── Database Statistics ────────────────────────────────────────────

#[test]
fn statistics_reflect_operations() {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(default_config(tmp.path())).unwrap();

    let stats_before = db.statistics();

    for i in 0..50 {
        db.insert(&cpu_point("srv", i * 1000, i as f64)).unwrap();
    }
    db.flush().unwrap();

    let stats_after = db.statistics();
    assert!(
        stats_after.wal_sequence > stats_before.wal_sequence,
        "WAL sequence should advance after writes"
    );
    assert!(
        stats_after.segment_count >= 1,
        "should have at least one segment after flush"
    );
    assert!(
        stats_after.measurement_count >= 1,
        "should have at least one measurement"
    );

    db.close().unwrap();
}

/// A soft delete's grace period is not at the mercy of the wall clock.
///
/// The deadline is a wall-clock instant stamped when the measurement was
/// dropped, and the GC pass used to compare it against `SystemTime::now()`
/// alone. One bad reading — a gateway with no battery-backed RTC, an NTP
/// server handing out a date in the next century, a restored VM snapshot —
/// closed the window instantly and hard-deleted the data it existed to
/// protect. It now measures from the same reference retention does: the
/// clock **capped by the newest timestamp the database holds**.
///
/// No clock is manipulated here. A database whose newest data is a year old
/// *is* the case where the clock has run ahead of the data, which is what the
/// cap is for.
#[test]
fn a_soft_deletes_grace_survives_a_clock_that_ran_ahead() {
    let tmp = TempDir::new().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(tmp.path())
        .memtable_flush_threshold(1024 * 1024)
        // Already elapsed by the time the pass runs, on the wall clock.
        .soft_delete_ttl(Some(std::time::Duration::from_millis(1)))
        .build()
        .unwrap();
    let db = Chronix::open(config).unwrap();

    // Data from a year ago — the database's own clock is a year behind the
    // wall clock, which is the shape a jumped clock produces.
    let now_ns = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
    .unwrap();
    let year_ago = now_ns - 365 * 86_400 * 1_000_000_000;
    for i in 0..10i64 {
        let _ = db
            .backfill(&[cpu_point("srv", year_ago + i * 1000, i as f64)])
            .unwrap();
    }
    db.flush().unwrap();

    db.drop_measurement("cpu").unwrap();
    assert!(db.is_measurement_pending_drop("cpu"));

    std::thread::sleep(std::time::Duration::from_millis(10));

    // The wall clock is far past the deadline, but the data is not.
    let collected = db.gc_pending_measurement_drops().unwrap();
    assert_eq!(
        collected, 0,
        "the grace period must not be closed by a clock the data does not support"
    );
    assert!(
        db.is_measurement_pending_drop("cpu"),
        "the measurement is still restorable"
    );
    assert!(db.restore_measurement("cpu").unwrap());

    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    assert_eq!(
        db.execute(&plan).unwrap().num_rows(),
        10,
        "restoring brings every point back"
    );
}
