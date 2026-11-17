#![allow(clippy::unwrap_used)] // benches may unwrap
//! End-to-end benchmarks for the Chronix database.
//!
//! Covers the full insert → flush → query lifecycle using the public API.

use std::collections::BTreeMap;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use tempfile::TempDir;

use chronix::prelude::*;
use chronix::{Chronix, tags};

// ── Helpers ────────────────────────────────────────────────────────────

fn bench_config(dir: &std::path::Path) -> ChronixConfig {
    ChronixConfig::builder()
        .data_dir(dir)
        .memtable_flush_threshold(64 * 1024 * 1024) // 64 MB — avoid mid-bench flushes
        .build()
        .unwrap()
}

fn make_points(n: usize) -> Vec<Point> {
    (0..n)
        .map(|i| {
            let tags: BTreeMap<String, String> = [
                ("host".to_string(), format!("host-{}", i % 10)),
                ("region".to_string(), "us-east".to_string()),
            ]
            .into_iter()
            .collect();
            let key = SeriesKey::new("cpu", tags).unwrap();
            let fields: BTreeMap<String, FieldValue> = [
                (
                    "usage_idle".to_string(),
                    FieldValue::F64(42.0 + (i as f64) * 0.01),
                ),
                ("temp".to_string(), FieldValue::I64(60 + (i as i64) % 20)),
            ]
            .into_iter()
            .collect();
            let ts = 1_700_000_000_000_000_000_i64 + (i as i64) * 1_000_000_000;
            Point::new(key, fields, ts).unwrap()
        })
        .collect()
}

/// Create points with 5 fields (mixed types) for multi-field benchmarks.
fn make_multi_field_points(n: usize) -> Vec<Point> {
    (0..n)
        .map(|i| {
            let tags: BTreeMap<String, String> = [
                ("host".to_string(), format!("host-{}", i % 10)),
                ("region".to_string(), "us-east".to_string()),
            ]
            .into_iter()
            .collect();
            let key = SeriesKey::new("metrics", tags).unwrap();
            let fields: BTreeMap<String, FieldValue> = [
                (
                    "cpu_usage".to_string(),
                    FieldValue::F64(42.0 + (i as f64) * 0.01),
                ),
                (
                    "mem_free".to_string(),
                    FieldValue::I64(1_073_741_824 + (i as i64) % 1000),
                ),
                (
                    "disk_io".to_string(),
                    FieldValue::F64(100.0 + (i as f64) * 0.5),
                ),
                ("net_bytes".to_string(), FieldValue::I64((i as i64) * 1024)),
                ("healthy".to_string(), FieldValue::Bool(i % 7 != 0)),
            ]
            .into_iter()
            .collect();
            let ts = 1_700_000_000_000_000_000_i64 + (i as i64) * 1_000_000_000;
            Point::new(key, fields, ts).unwrap()
        })
        .collect()
}

/// Open a DB, insert `n` points, flush, and return `(db, _tmp_dir)`.
fn seeded_db(n: usize) -> (Chronix, TempDir) {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(bench_config(tmp.path())).unwrap();
    let points = make_points(n);
    let _ = db.insert_batch(&points).unwrap();
    db.flush().unwrap();
    (db, tmp)
}

// ── Write Benchmarks ──────────────────────────────────────────────────
//
// Every routine below returns its `(Chronix, TempDir)` rather than dropping
// it. Criterion drops a routine's return value outside the timed region, so
// returning the pair keeps teardown — `Chronix::close`, which flushes and
// encodes every memtable, plus the temp-dir delete — out of the ingest number.
// Persist cost is measured separately, by `flush_10K` and `batch_commit`.

fn bench_insert_single(c: &mut Criterion) {
    let point = make_points(1).into_iter().next().unwrap();

    c.bench_function("insert_single", |b| {
        b.iter_with_setup(
            || {
                let tmp = TempDir::new().unwrap();
                let db = Chronix::open(bench_config(tmp.path())).unwrap();
                (db, tmp)
            },
            |(db, tmp)| {
                db.insert(black_box(&point)).unwrap();
                // Returned, not dropped: see `teardown` note above.
                (db, tmp)
            },
        );
    });
}

fn bench_insert_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_batch");
    for size in [100, 1_000, 10_000] {
        let points = make_points(size);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &points, |b, pts| {
            b.iter_with_setup(
                || {
                    let tmp = TempDir::new().unwrap();
                    let db = Chronix::open(bench_config(tmp.path())).unwrap();
                    (db, tmp)
                },
                |(db, tmp)| {
                    let _ = db.insert_batch(black_box(pts)).unwrap();
                    (db, tmp)
                },
            );
        });
    }
    group.finish();
}

/// The steady-state write — every series already known — with and without
/// a namespace tag and namespace limits in force.
///
/// Namespace admission works on *new* series only, so a write whose series
/// are all known must cost what it cost before quotas were enforced: the
/// containment check it always did, and no lock. The two cases must agree
/// within noise; a gap means admission work leaked onto the fast path.
fn bench_insert_batch_namespaced(c: &mut Criterion) {
    const N: usize = 10_000;
    let shift = |points: &[Point], by: i64, namespace: Option<&str>| -> Vec<Point> {
        points
            .iter()
            .map(|p| {
                let mut tags: BTreeMap<String, String> = p
                    .series_key()
                    .tags()
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect();
                if let Some(ns) = namespace {
                    tags.insert(
                        chronix::chronix_core::NAMESPACE_TAG.to_string(),
                        ns.to_string(),
                    );
                }
                let key = SeriesKey::new(p.series_key().measurement(), tags).unwrap();
                let fields: BTreeMap<String, FieldValue> = p
                    .fields()
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.clone()))
                    .collect();
                Point::new(key, fields, p.timestamp() + by).unwrap()
            })
            .collect()
    };
    let base = make_points(N);
    let mut group = c.benchmark_group("insert_batch_namespaced");
    group.throughput(Throughput::Elements(N as u64));
    // `tenant-b` carries the same tag with no limits set, which separates
    // the cost of the extra tag (a longer key, one more column) from the cost
    // of the quota itself.
    for (label, namespace) in [
        ("untagged", None),
        ("namespaced_unlimited", Some("tenant-b")),
        ("namespaced", Some("tenant-a")),
    ] {
        let warm = shift(&base, 0, namespace);
        let next = shift(&base, 1, namespace);
        group.bench_function(BenchmarkId::new("existing_series", label), |b| {
            b.iter_with_setup(
                || {
                    let tmp = TempDir::new().unwrap();
                    let db = Chronix::open(bench_config(tmp.path())).unwrap();
                    db.set_namespace_limits(
                        "tenant-a",
                        chronix::NamespaceLimits {
                            max_series: 1_000_000,
                            max_measurements: 1_000,
                        },
                    );
                    let _ = db.insert_batch(&warm).unwrap();
                    (db, tmp)
                },
                |(db, tmp)| {
                    let _ = db.insert_batch(black_box(&next)).unwrap();
                    (db, tmp)
                },
            );
        });
    }
    group.finish();
}

/// `insert_batch` of 10 000 points with and without the field encryption
/// that seals `usage_idle` in the WAL — the cost of keeping an encrypted
/// column's plaintext out of the log.
#[cfg(feature = "field-encryption")]
fn bench_insert_batch_sealed(c: &mut Criterion) {
    use base64::Engine as _;
    // SAFETY: set once, before any benchmark runs, and never changed.
    #[allow(unsafe_code)]
    unsafe {
        std::env::set_var(
            "CHRONIX_BENCH_SEAL_KEY",
            base64::engine::general_purpose::STANDARD.encode([7u8; 32]),
        );
    }
    let mut group = c.benchmark_group("insert_batch_sealed");
    let points = make_points(10_000);
    group.throughput(Throughput::Elements(10_000));
    for sealed in [false, true] {
        let name = if sealed { "encrypted_field" } else { "plain" };
        group.bench_with_input(BenchmarkId::from_parameter(name), &points, |b, pts| {
            b.iter_with_setup(
                || {
                    let tmp = TempDir::new().unwrap();
                    let mut builder = ChronixConfig::builder()
                        .data_dir(tmp.path())
                        .memtable_flush_threshold(64 * 1024 * 1024);
                    if sealed {
                        builder = builder.field_encryption(chronix_core::FieldEncryption {
                            columns: [("usage_idle".to_string(), "k".to_string())].into(),
                            keys: [("k".to_string(), "CHRONIX_BENCH_SEAL_KEY".to_string())].into(),
                        });
                    }
                    let db = Chronix::open(builder.build().unwrap()).unwrap();
                    (db, tmp)
                },
                |(db, tmp)| {
                    let _ = db.insert_batch(black_box(pts)).unwrap();
                    (db, tmp)
                },
            );
        });
    }
    group.finish();
}

#[cfg(not(feature = "field-encryption"))]
fn bench_insert_batch_sealed(_: &mut Criterion) {}

// ── Flush Benchmark ───────────────────────────────────────────────────

fn bench_flush(c: &mut Criterion) {
    let points = make_points(10_000);

    c.bench_function("flush_10K", |b| {
        b.iter_with_setup(
            || {
                let tmp = TempDir::new().unwrap();
                let db = Chronix::open(bench_config(tmp.path())).unwrap();
                let _ = db.insert_batch(&points).unwrap();
                (db, tmp)
            },
            |(db, tmp)| {
                black_box(db.flush().unwrap());
                (db, tmp)
            },
        );
    });
}

// ── Query Benchmarks ──────────────────────────────────────────────────

fn bench_range_query(c: &mut Criterion) {
    let (db, _tmp) = seeded_db(10_000);

    let start = 1_700_000_000_000_000_000_i64;
    let end = start + 5_000 * 1_000_000_000; // first 5K points

    c.bench_function("range_query_5K_of_10K", |b| {
        b.iter(|| {
            let plan = db
                .query()
                .measurement("cpu")
                .range(start, end)
                .build()
                .unwrap();
            black_box(db.execute(&plan).unwrap());
        });
    });
}

fn bench_filtered_query(c: &mut Criterion) {
    let (db, _tmp) = seeded_db(10_000);

    c.bench_function("filtered_query_single_host", |b| {
        b.iter(|| {
            let plan = db
                .query()
                .measurement("cpu")
                .tag("host", "host-0")
                .build()
                .unwrap();
            black_box(db.execute(&plan).unwrap());
        });
    });
}

fn bench_projected_query(c: &mut Criterion) {
    let (db, _tmp) = seeded_db(10_000);

    c.bench_function("projected_query_one_field", |b| {
        b.iter(|| {
            let plan = db
                .query()
                .measurement("cpu")
                .field("usage_idle")
                .build()
                .unwrap();
            black_box(db.execute(&plan).unwrap());
        });
    });
}

fn bench_aggregation_query(c: &mut Criterion) {
    let (db, _tmp) = seeded_db(10_000);

    c.bench_function("aggregation_sum_10K", |b| {
        b.iter(|| {
            let plan = db
                .query()
                .measurement("cpu")
                .aggregate(AggFn::Sum)
                .build()
                .unwrap();
            black_box(db.execute(&plan).unwrap());
        });
    });
}

// The last-value cache is opt-in, so a benchmark built on `bench_config`
// measures the fallback memtable scan no matter what it is called. Both paths
// get a number, each named for what it runs.
fn bench_last_value(c: &mut Criterion) {
    let tag_map = tags! { "host" => "host-0", "region" => "us-east" };

    // Cache hit: what `enable_last_value_cache(true)` buys.
    let tmp_cached = TempDir::new().unwrap();
    let cached_db = Chronix::open(
        ChronixConfig::builder()
            .data_dir(tmp_cached.path())
            .memtable_flush_threshold(64 * 1024 * 1024)
            .enable_last_value_cache(true)
            .build()
            .unwrap(),
    )
    .unwrap();
    let _ = cached_db.insert_batch(&make_points(10_000)).unwrap();
    cached_db.flush().unwrap();
    c.bench_function("last_value_cached", |b| {
        b.iter(|| {
            black_box(cached_db.last_value("cpu", black_box(&tag_map)).unwrap());
        });
    });

    // Cache miss: the default configuration, which scans the memtable and
    // then walks segments newest-first.
    let (db, _tmp) = seeded_db(10_000);
    c.bench_function("last_value_uncached", |b| {
        b.iter(|| {
            black_box(db.last_value("cpu", black_box(&tag_map)).unwrap());
        });
    });
}

// ── Multi-field insert benchmark ───────────────────────────────────

fn bench_multi_field_insert(c: &mut Criterion) {
    let points = make_multi_field_points(10_000);

    c.bench_function("multi_field_insert_10K_5fields", |b| {
        b.iter_with_setup(
            || {
                let tmp = TempDir::new().unwrap();
                let db = Chronix::open(bench_config(tmp.path())).unwrap();
                (db, tmp)
            },
            |(db, tmp)| {
                let _ = db.insert_batch(black_box(&points)).unwrap();
                (db, tmp)
            },
        );
    });
}

// ── Batch commit (< 10ms target) benchmark ────────────────────────

fn bench_batch_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_commit");
    for size in [1_000, 10_000] {
        let points = make_points(size);
        group.bench_with_input(BenchmarkId::from_parameter(size), &points, |b, pts| {
            b.iter_with_setup(
                || {
                    let tmp = TempDir::new().unwrap();
                    let db = Chronix::open(bench_config(tmp.path())).unwrap();
                    (db, tmp)
                },
                |(db, tmp)| {
                    let _ = db.insert_batch(black_box(pts)).unwrap();
                    black_box(db.flush().unwrap());
                    (db, tmp)
                },
            );
        });
    }
    group.finish();
}

// ── execute_stream benchmark ───────────────────────────────────────

fn bench_execute_stream(c: &mut Criterion) {
    let (db, _tmp) = seeded_db(10_000);

    c.bench_function("execute_stream_10K", |b| {
        b.iter(|| {
            let plan = db
                .query()
                .measurement("cpu")
                .range(0, i64::MAX)
                .build()
                .unwrap();
            let batches = db.execute_stream(&plan).unwrap();
            let total: usize = batches
                .iter()
                .map(chronix::prelude::RecordBatch::num_rows)
                .sum();
            black_box(total);
        });
    });
}

// ── Performance Validation ─────────────────────────────────────────

/// Sustained ingestion: 1M points in 100 batches, one writer.
///
/// Inserts 10K batches of 100 points (1M total) including WAL append,
/// memtable insert, and LVC update.
fn bench_sustained_ingestion_1m(c: &mut Criterion) {
    let mut group = c.benchmark_group("sustained_ingestion");
    group.sample_size(10); // fewer samples — this is a heavy benchmark
    group.throughput(Throughput::Elements(1_000_000));

    // Pre-generate 100 batches of 10K points (1M total)
    let batches: Vec<Vec<Point>> = (0..100)
        .map(|batch_idx| {
            (0..10_000)
                .map(|i| {
                    let idx = batch_idx * 10_000 + i;
                    let tags: BTreeMap<String, String> = [
                        ("host".to_string(), format!("host-{}", idx % 100)),
                        ("region".to_string(), format!("region-{}", idx % 5)),
                        ("dc".to_string(), format!("dc-{}", idx % 3)),
                    ]
                    .into_iter()
                    .collect();
                    let key = SeriesKey::new("cpu", tags).unwrap();
                    let fields: BTreeMap<String, FieldValue> = [
                        (
                            "usage_idle".to_string(),
                            FieldValue::F64(42.0 + (idx as f64) * 0.001),
                        ),
                        (
                            "usage_system".to_string(),
                            FieldValue::F64(1.0 + (idx as f64) * 0.0001),
                        ),
                    ]
                    .into_iter()
                    .collect();
                    let ts = 1_700_000_000_000_000_000_i64 + (idx as i64) * 1_000_000_000;
                    Point::new(key, fields, ts).unwrap()
                })
                .collect()
        })
        .collect();

    group.bench_function("1M_points_100_batches", |b| {
        b.iter_with_setup(
            || {
                let tmp = TempDir::new().unwrap();
                let db = Chronix::open(bench_config(tmp.path())).unwrap();
                (db, tmp)
            },
            |(db, tmp)| {
                for batch in &batches {
                    let _ = db.insert_batch(black_box(batch)).unwrap();
                }
                (db, tmp)
            },
        );
    });
    group.finish();
}

/// Sustained ingestion from several writer threads at once.
///
/// The single-writer number above is bounded by one thread's WAL encode and
/// memtable insert; it is not the engine's ceiling. The memtable is a
/// lock-free skip list behind a shard router and the WAL coalesces concurrent
/// appends into one group sync, so the interesting question — the one a
/// "points/sec" figure in a document is usually taken to answer — is what
/// several writers reach together. Nothing measured it, so no answer was
/// grounded in anything.
fn bench_concurrent_ingestion(c: &mut Criterion) {
    use std::thread;

    let mut group = c.benchmark_group("concurrent_ingestion");
    group.sample_size(10);

    const PER_WRITER: usize = 100_000;

    for writers in [2usize, 4, 8] {
        group.throughput(Throughput::Elements((writers * PER_WRITER) as u64));
        // Each writer owns a disjoint slice of the series space, which is how
        // a real multi-source ingest looks and keeps writers off each other's
        // skip-list nodes.
        let per_writer: Vec<Vec<Point>> = (0..writers)
            .map(|w| {
                (0..PER_WRITER)
                    .map(|i| {
                        let tags: BTreeMap<String, String> = [
                            ("host".to_string(), format!("host-{}", w * 100 + i % 100)),
                            ("region".to_string(), format!("region-{}", i % 5)),
                        ]
                        .into_iter()
                        .collect();
                        let key = SeriesKey::new("cpu", tags).unwrap();
                        let fields: BTreeMap<String, FieldValue> = [(
                            "usage_idle".to_string(),
                            FieldValue::F64(42.0 + (i as f64) * 0.001),
                        )]
                        .into_iter()
                        .collect();
                        let ts = 1_700_000_000_000_000_000_i64 + (i as i64) * 1_000_000_000;
                        Point::new(key, fields, ts).unwrap()
                    })
                    .collect()
            })
            .collect();
        let per_writer = Arc::new(per_writer);

        group.bench_with_input(
            BenchmarkId::from_parameter(writers),
            &per_writer,
            |b, batches| {
                b.iter_with_setup(
                    || {
                        let tmp = TempDir::new().unwrap();
                        let db = Arc::new(Chronix::open(bench_config(tmp.path())).unwrap());
                        (db, tmp)
                    },
                    |(db, tmp)| {
                        thread::scope(|scope| {
                            for chunk in batches.iter() {
                                let db = Arc::clone(&db);
                                scope.spawn(move || {
                                    for batch in chunk.chunks(10_000) {
                                        let _ = db.insert_batch(black_box(batch)).unwrap();
                                    }
                                });
                            }
                        });
                        (db, tmp)
                    },
                );
            },
        );
    }
    group.finish();
}

/// Compression ratio benchmark.
///
/// Writes known data patterns and measures segment size vs raw data size.
fn bench_compression_ratio(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression_ratio");
    group.sample_size(10);

    // Regular metrics: monotonically increasing with small noise
    let regular_points: Vec<Point> = (0..100_000)
        .map(|i| {
            let tags: BTreeMap<String, String> = [("host".to_string(), format!("host-{}", i % 10))]
                .into_iter()
                .collect();
            let key = SeriesKey::new("cpu", tags).unwrap();
            let v = 50.0 + (i as f64) * 0.001 + ((i as f64) * 0.1).sin() * 0.5;
            let fields: BTreeMap<String, FieldValue> = [("value".to_string(), FieldValue::F64(v))]
                .into_iter()
                .collect();
            let ts = 1_700_000_000_000_000_000_i64 + (i as i64) * 10_000_000_000;
            Point::new(key, fields, ts).unwrap()
        })
        .collect();

    group.bench_function("regular_metrics_100K_flush", |b| {
        b.iter_with_setup(
            || {
                let tmp = TempDir::new().unwrap();
                let db = Chronix::open(bench_config(tmp.path())).unwrap();
                (db, tmp)
            },
            |(db, _tmp)| {
                let _ = db.insert_batch(black_box(&regular_points)).unwrap();
                db.flush().unwrap();
            },
        );
    });
    group.finish();
}

/// Query latency, against 100K points across 1K series.
///
/// Preloads a database with data across many series and measures
/// single-series and multi-series query latency.
fn bench_query_latency(c: &mut Criterion) {
    // Preload with 100K points across 1K series
    let tmp = TempDir::new().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(tmp.path())
        .memtable_flush_threshold(128 * 1024 * 1024)
        // `lvc_lookup` below is where the last-value-cache figure is read
        // from. The cache is opt-in, so without this the benchmark measures
        // the fallback scan instead.
        .enable_last_value_cache(true)
        .build()
        .unwrap();
    let db = Chronix::open(config).unwrap();

    let points: Vec<Point> = (0..100_000)
        .map(|i| {
            let tags: BTreeMap<String, String> = [
                ("host".to_string(), format!("host-{}", i % 1000)),
                ("region".to_string(), format!("region-{}", i % 5)),
            ]
            .into_iter()
            .collect();
            let key = SeriesKey::new("metrics", tags).unwrap();
            let fields: BTreeMap<String, FieldValue> =
                [("value".to_string(), FieldValue::F64(i as f64))]
                    .into_iter()
                    .collect();
            let ts = 1_700_000_000_000_000_000_i64 + (i as i64) * 1_000_000_000;
            Point::new(key, fields, ts).unwrap()
        })
        .collect();

    let _ = db.insert_batch(&points).unwrap();
    db.flush().unwrap();

    let tag_map: BTreeMap<String, String> = [
        ("host".to_string(), "host-0".to_string()),
        ("region".to_string(), "region-0".to_string()),
    ]
    .into_iter()
    .collect();

    let mut group = c.benchmark_group("query_latency");

    // 1 series, narrow range
    group.bench_function("single_series_1hr", |b| {
        b.iter(|| {
            let start = 1_700_000_000_000_000_000_i64;
            let end = start + 3600 * 1_000_000_000;
            let plan = db
                .query()
                .measurement("metrics")
                .tag("host", "host-0")
                .tag("region", "region-0")
                .range(start, end)
                .build()
                .unwrap();
            black_box(db.execute(&plan).unwrap());
        });
    });

    // LVC lookup
    group.bench_function("lvc_lookup", |b| {
        b.iter(|| {
            black_box(db.last_value("metrics", black_box(&tag_map)).unwrap());
        });
    });

    // 100 series, 24 hour range
    group.bench_function("wide_query_100_series_24hr", |b| {
        b.iter(|| {
            let start = 1_700_000_000_000_000_000_i64;
            let end = start + 24 * 3600 * 1_000_000_000; // 24 hours
            let plan = db
                .query()
                .measurement("metrics")
                .tag("region", "region-0") // ~200 series (1000 hosts / 5 regions)
                .range(start, end)
                .build()
                .unwrap();
            black_box(db.execute(&plan).unwrap());
        });
    });

    // Wide query: no tag filter
    group.bench_function("wide_query_all_series", |b| {
        b.iter(|| {
            let plan = db
                .query()
                .measurement("metrics")
                .field("value")
                .range(0, i64::MAX)
                .build()
                .unwrap();
            black_box(db.execute(&plan).unwrap());
        });
    });

    group.finish();
    db.close().unwrap();
}

/// The parallelism budget on the read it changes: one time range covered by
/// eight overlapping segments, read with `Serial` and with `Auto`.
fn bench_parallelism(c: &mut Criterion) {
    let tmp = TempDir::new().unwrap();
    let mut config = ChronixConfig::builder()
        .data_dir(tmp.path())
        .maintenance_interval(std::time::Duration::ZERO)
        .build()
        .unwrap();
    config.shard_duration = std::time::Duration::from_secs(30 * 86_400);
    let db = Chronix::open(config.clone()).unwrap();
    let base = 1_700_000_000_000_000_000_i64;
    for batch in 0..8 {
        let points: Vec<Point> = (0..50_000)
            .map(|i| {
                let tags: BTreeMap<String, String> =
                    [("host".to_string(), format!("host-{batch}-{}", i % 50))]
                        .into_iter()
                        .collect();
                let key = SeriesKey::new("metrics", tags).unwrap();
                let fields: BTreeMap<String, FieldValue> =
                    [("value".to_string(), FieldValue::F64(f64::from(i)))]
                        .into_iter()
                        .collect();
                Point::new(key, fields, base + i64::from(i) * 1_000_000_000).unwrap()
            })
            .collect();
        let _ = db.backfill(&points).unwrap();
        db.flush().unwrap();
    }
    db.close().unwrap();

    let mut group = c.benchmark_group("parallelism");
    for (name, parallelism) in [
        ("overlapping_8_segments/serial", Parallelism::Serial),
        ("overlapping_8_segments/auto", Parallelism::Auto),
    ] {
        let mut config = config.clone();
        config.parallelism = parallelism;
        let db = Chronix::open(config).unwrap();
        group.bench_function(name, |b| {
            b.iter(|| {
                let plan = db
                    .query()
                    .measurement("metrics")
                    .field("value")
                    .range(0, i64::MAX)
                    .build()
                    .unwrap();
                black_box(db.execute(&plan).unwrap());
            });
        });
        db.close().unwrap();
    }
    group.finish();
}

/// Compaction benchmark — measure merge-sort compaction throughput.
fn bench_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction");
    group.sample_size(10);

    group.bench_function("compact_5_segments", |b| {
        b.iter_with_setup(
            || {
                let tmp = TempDir::new().unwrap();
                let config = ChronixConfig::builder()
                    .data_dir(tmp.path())
                    .memtable_flush_threshold(256)
                    .build()
                    .unwrap();
                let db = Chronix::open(config).unwrap();
                for batch in 0..5 {
                    for i in 0..100 {
                        let ts = batch * 10000 + i;
                        let tags: BTreeMap<String, String> =
                            [("host".to_string(), format!("host-{}", i % 10))]
                                .into_iter()
                                .collect();
                        let key = SeriesKey::new("cpu", tags).unwrap();
                        let fields: BTreeMap<String, FieldValue> =
                            [("value".to_string(), FieldValue::F64(i as f64))]
                                .into_iter()
                                .collect();
                        let point = Point::new(key, fields, ts).unwrap();
                        db.insert(&point).unwrap();
                    }
                    db.flush().unwrap();
                }
                (db, tmp)
            },
            |(db, _tmp)| {
                black_box(db.compact().unwrap());
            },
        );
    });
    group.finish();
}

// ── SQL Analytics Benchmarks ───────────────────────────────────────

/// SQL forecast on a single series: target < 50ms.
fn bench_sql_forecast(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_forecast");
    group.sample_size(10);

    // Seed with 168 points (7 days × 24h) for one host
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(bench_config(tmp.path())).unwrap();
    let n = 7 * 24;
    let points: Vec<Point> = (0..n)
        .map(|i| {
            let tags: BTreeMap<String, String> = [
                ("host".to_string(), "host-0".to_string()),
                ("region".to_string(), "us-east".to_string()),
            ]
            .into_iter()
            .collect();
            let key = SeriesKey::new("cpu", tags).unwrap();
            let fields: BTreeMap<String, FieldValue> = [
                (
                    "usage_idle".to_string(),
                    FieldValue::F64(42.0 + (i as f64) * 0.01),
                ),
                ("temp".to_string(), FieldValue::I64(60 + (i as i64) % 20)),
            ]
            .into_iter()
            .collect();
            let ts = 1_700_000_000_000_000_000_i64 + (i as i64) * 1_000_000_000;
            Point::new(key, fields, ts).unwrap()
        })
        .collect();
    let _ = db.insert_batch(&points).unwrap();
    db.flush().unwrap();
    let db = Arc::new(db);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let ctx = create_session_context(db);

    let sql = "SELECT forecast(usage_idle, 24) FROM cpu";

    group.bench_function("forecast_168pts_24h", |b| {
        b.iter(|| {
            rt.block_on(async {
                let df = ctx.sql(black_box(sql)).await.unwrap();
                let batches = df.collect().await.unwrap();
                black_box(batches);
            });
        });
    });
    group.finish();
}

/// SQL anomaly_score on a single series: target < 100ms.
fn bench_sql_anomaly(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_anomaly");
    group.sample_size(10);

    // Seed with 720 points (30 days × 24h) for one host
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(bench_config(tmp.path())).unwrap();
    let n = 30 * 24;
    let points: Vec<Point> = (0..n)
        .map(|i| {
            let tags: BTreeMap<String, String> = [
                ("host".to_string(), "host-0".to_string()),
                ("region".to_string(), "us-east".to_string()),
            ]
            .into_iter()
            .collect();
            let key = SeriesKey::new("cpu", tags).unwrap();
            let fields: BTreeMap<String, FieldValue> = [
                (
                    "usage_idle".to_string(),
                    FieldValue::F64(42.0 + (i as f64) * 0.01),
                ),
                ("temp".to_string(), FieldValue::I64(60 + (i as i64) % 20)),
            ]
            .into_iter()
            .collect();
            let ts = 1_700_000_000_000_000_000_i64 + (i as i64) * 1_000_000_000;
            Point::new(key, fields, ts).unwrap()
        })
        .collect();
    let _ = db.insert_batch(&points).unwrap();
    db.flush().unwrap();
    let db = Arc::new(db);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let ctx = create_session_context(db);

    let sql = "SELECT anomaly_score(usage_idle, 3.0) FROM cpu";

    group.bench_function("anomaly_score_720pts", |b| {
        b.iter(|| {
            rt.block_on(async {
                let df = ctx.sql(black_box(sql)).await.unwrap();
                let batches = df.collect().await.unwrap();
                black_box(batches);
            });
        });
    });
    group.finish();
}

/// 10 concurrent FORECAST queries in parallel: target < 200ms total.
fn bench_sql_parallel_forecast(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_parallel_forecast");
    group.sample_size(10);

    // Seed with 168 points for one host
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(bench_config(tmp.path())).unwrap();
    let n = 7 * 24;
    let points: Vec<Point> = (0..n)
        .map(|i| {
            let tags: BTreeMap<String, String> = [
                ("host".to_string(), "host-0".to_string()),
                ("region".to_string(), "us-east".to_string()),
            ]
            .into_iter()
            .collect();
            let key = SeriesKey::new("cpu", tags).unwrap();
            let fields: BTreeMap<String, FieldValue> = [
                (
                    "usage_idle".to_string(),
                    FieldValue::F64(42.0 + (i as f64) * 0.01),
                ),
                ("temp".to_string(), FieldValue::I64(60 + (i as i64) % 20)),
            ]
            .into_iter()
            .collect();
            let ts = 1_700_000_000_000_000_000_i64 + (i as i64) * 1_000_000_000;
            Point::new(key, fields, ts).unwrap()
        })
        .collect();
    let _ = db.insert_batch(&points).unwrap();
    db.flush().unwrap();
    let db = Arc::new(db);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let ctx = create_session_context(db);

    let sql = "SELECT forecast(usage_idle, 24) FROM cpu";

    group.bench_function("10_concurrent_forecast", |b| {
        b.iter(|| {
            let rt_handle = rt.handle();
            let handles: Vec<_> = (0..10)
                .map(|_| {
                    let ctx = ctx.clone();
                    let sql = sql.to_string();
                    rt_handle.spawn(async move {
                        let df = ctx.sql(&sql).await.unwrap();
                        df.collect().await.unwrap()
                    })
                })
                .collect();
            for h in handles {
                rt.block_on(h).unwrap();
            }
        });
    });
    group.finish();
}

/// One maintenance pass of a declared detector at the design partner's
/// shape: 50 series, one new minute at 1 Hz each (3 000 points), scored
/// and written as verdicts. Only the pass is timed, not the writes it judges.
fn bench_detector_pass(c: &mut Criterion) {
    use chronix::detector::DetectorDeclaration;
    const MINUTE: i64 = 60_000_000_000;
    const BASE: i64 = 1_672_531_200_000_000_000; // 2023-01-01, settled
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(
        ChronixConfig::builder()
            .data_dir(tmp.path())
            .maintenance_interval(std::time::Duration::ZERO)
            .memtable_flush_threshold(64 * 1024 * 1024)
            .build()
            .unwrap(),
    )
    .unwrap();
    db.declare_detector(
        DetectorDeclaration::builder("bench")
            .source("pv", "power")
            .zscore(4.0)
            .cusum(0.5, 5.0)
            .settle(std::time::Duration::ZERO)
            .build()
            .unwrap(),
    )
    .unwrap();
    let mut minute = 0i64;
    let write_minute = |m: i64| {
        let points: Vec<Point> = (0..50)
            .flat_map(|s| {
                (0..60).map(move |sec| {
                    let key =
                        SeriesKey::new("pv", tags! { "inverter" => format!("inv-{s}") }).unwrap();
                    let v = 1_000.0 + ((m * 60 + sec) % 7) as f64;
                    Point::new(
                        key,
                        chronix::fields! { "power" => v },
                        BASE + m * MINUTE + sec * 1_000_000_000,
                    )
                    .unwrap()
                })
            })
            .collect();
        db.backfill(&points).unwrap().into_complete().unwrap();
    };
    // Warm every baseline before measuring.
    write_minute(minute);
    db.advance_detectors().unwrap();
    minute += 1;

    let mut group = c.benchmark_group("detector_pass");
    group.throughput(Throughput::Elements(3_000));
    group.bench_function("50_series_x_60_points", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                write_minute(minute);
                minute += 1;
                let t = std::time::Instant::now();
                black_box(db.advance_detectors().unwrap());
                total += t.elapsed();
            }
            total
        });
    });
    group.finish();
}

// ── Groups ────────────────────────────────────────────────────────────

/// `Chronix::cardinality` over 1 M live series: one pass over the known
/// series, top 10 of each list. The target is under a second.
fn bench_cardinality_stats(c: &mut Criterion) {
    const SERIES: usize = 1_000_000;
    let dir = TempDir::new().unwrap();
    let db = Chronix::open(
        ChronixConfig::builder()
            .data_dir(dir.path())
            .max_series_cardinality(SERIES + 1)
            .build()
            .unwrap(),
    )
    .unwrap();
    for chunk in (0..SERIES).collect::<Vec<_>>().chunks(50_000) {
        let points: Vec<Point> = chunk
            .iter()
            .map(|&i| {
                let tags: BTreeMap<String, String> = [
                    ("device".to_string(), format!("dev-{i}")),
                    ("region".to_string(), format!("region-{}", i % 20)),
                    ("model".to_string(), format!("model-{}", i % 300)),
                ]
                .into_iter()
                .collect();
                let key = SeriesKey::new(format!("metric_{}", i % 50), tags).unwrap();
                Point::new(
                    key,
                    chronix::fields! { "v" => 1.0 },
                    1_700_000_000_000_000_000,
                )
                .unwrap()
            })
            .collect();
        let _ = db.insert_batch(&points).unwrap();
        db.flush().unwrap();
    }
    let mut group = c.benchmark_group("cardinality_stats");
    group.sample_size(10);
    group.bench_function("1m_series_limit_10", |b| {
        b.iter(|| db.cardinality(10, None).unwrap());
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_cardinality_stats,
    bench_insert_single,
    bench_insert_batch_namespaced,
    bench_insert_batch,
    bench_insert_batch_sealed,
    bench_flush,
    bench_range_query,
    bench_filtered_query,
    bench_projected_query,
    bench_aggregation_query,
    bench_last_value,
    bench_multi_field_insert,
    bench_batch_commit,
    bench_execute_stream,
    bench_sustained_ingestion_1m,
    bench_concurrent_ingestion,
    bench_compression_ratio,
    bench_query_latency,
    bench_parallelism,
    bench_compaction,
    bench_sql_select_with_time_range,
    bench_sql_group_by_time_bucket,
    bench_sql_order_limit,
    bench_latest_n,
    bench_sql_forecast,
    bench_sql_anomaly,
    bench_sql_parallel_forecast,
    bench_promql_instant_rate,
    bench_promql_range_query,
    bench_promql_aggregation,
    bench_detector_pass,
    bench_inspect,
);
criterion_main!(benches);

// ── SQL Query Engine Benchmarks ────────────────────────────────────

use chronix::promql::eval::QueryParams;
use chronix::promql::{PromQLEvaluator, parse};
use chronix::sql::create_session_context;

/// Create a seeded DB with `n` points and return it wrapped in Arc, plus a
/// tokio runtime for async DataFusion queries.
fn seeded_sql_env(n: usize) -> (Arc<Chronix>, tokio::runtime::Runtime, TempDir) {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(bench_config(tmp.path())).unwrap();
    let points = make_points(n);
    let _ = db.insert_batch(&points).unwrap();
    db.flush().unwrap();
    let db = Arc::new(db);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    (db, rt, tmp)
}

/// Simple SELECT with a time range: target < 5ms p99.
fn bench_sql_select_with_time_range(c: &mut Criterion) {
    let (db, rt, _tmp) = seeded_sql_env(100_000);
    let ctx = create_session_context(db);

    let sql =
        "SELECT * FROM cpu WHERE _time >= 1700000000000000000 AND _time < 1700000050000000000";

    c.bench_function("sql_select_time_range_100K", |b| {
        b.iter(|| {
            rt.block_on(async {
                let df = ctx.sql(black_box(sql)).await.unwrap();
                let batches = df.collect().await.unwrap();
                black_box(batches);
            });
        });
    });
}

/// GROUP BY time_bucket with aggregation: target < 50ms p99 on 1M rows.
fn bench_sql_group_by_time_bucket(c: &mut Criterion) {
    let (db, rt, _tmp) = seeded_sql_env(100_000);
    let ctx = create_session_context(db);

    let sql = "SELECT time_bucket(300000000000, _time) AS bucket, \
               AVG(usage_idle) AS avg_idle, COUNT(*) AS cnt \
               FROM cpu \
               GROUP BY bucket \
               ORDER BY bucket";

    c.bench_function("sql_group_by_time_bucket_100K", |b| {
        b.iter(|| {
            rt.block_on(async {
                let df = ctx.sql(black_box(sql)).await.unwrap();
                let batches = df.collect().await.unwrap();
                black_box(batches);
            });
        });
    });
}

/// ORDER BY _time DESC LIMIT 100: target < 2ms p99.
fn bench_sql_order_limit(c: &mut Criterion) {
    let (db, rt, _tmp) = seeded_sql_env(100_000);
    let ctx = create_session_context(db);

    let sql = "SELECT * FROM cpu ORDER BY _time DESC LIMIT 100";

    c.bench_function("sql_order_limit_100K", |b| {
        b.iter(|| {
            rt.block_on(async {
                let df = ctx.sql(black_box(sql)).await.unwrap();
                let batches = df.collect().await.unwrap();
                black_box(batches);
            });
        });
    });
}

/// Seed `series` series at one point per minute for `days` days, flushing at
/// the end of each simulated day so the segments are time-disjoint — the
/// layout a gateway that has run for a month actually has on disk.
fn seeded_latest_n_env(
    series: usize,
    days: usize,
) -> (Arc<Chronix>, tokio::runtime::Runtime, TempDir) {
    let tmp = TempDir::new().unwrap();
    let db = Chronix::open(bench_config(tmp.path())).unwrap();
    let start = 1_700_000_000_000_000_000_i64;
    let minute = 60_000_000_000_i64;
    let keys: Vec<SeriesKey> = (0..series)
        .map(|s| SeriesKey::new("meter", tags! { "device" => format!("d-{s}") }).unwrap())
        .collect();
    for day in 0..days {
        let mut points = Vec::with_capacity(series * 1440);
        for m in 0..1440_i64 {
            let ts = start + (day as i64 * 1440 + m) * minute;
            for (s, key) in keys.iter().enumerate() {
                let v = (s as f64) + (m as f64) * 0.01;
                points
                    .push(Point::new(key.clone(), chronix::fields! { "value" => v }, ts).unwrap());
            }
        }
        let _ = db.insert_batch(&points).unwrap();
        db.flush().unwrap();
    }
    let db = Arc::new(db);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    (db, rt, tmp)
}

/// `ORDER BY _time {ASC,DESC} LIMIT 10` over a month of a gateway and three
/// days of a server: whether "the latest N" stops early or reads everything.
fn bench_latest_n(c: &mut Criterion) {
    let mut group = c.benchmark_group("latest_n");
    group.sample_size(10);
    for (name, series, days) in [("gateway_30d", 50, 30), ("server_3d", 500, 3)] {
        let (db, rt, _tmp) = seeded_latest_n_env(series, days);
        let ctx = create_session_context(db);
        for (dir, sql) in [
            ("asc", "SELECT * FROM meter ORDER BY _time ASC LIMIT 10"),
            ("desc", "SELECT * FROM meter ORDER BY _time DESC LIMIT 10"),
        ] {
            group.bench_function(format!("{name}/{dir}"), |b| {
                b.iter(|| {
                    rt.block_on(async {
                        let df = ctx.sql(black_box(sql)).await.unwrap();
                        black_box(df.collect().await.unwrap());
                    });
                });
            });
        }
    }
    group.finish();
}

// ── PromQL Query Engine Benchmarks ──────────────────────────────────

/// Seed a DB with points that look like Prometheus metrics
/// (measurement = metric name, tags = labels).
fn seeded_promql_env(
    metric: &str,
    n_series: usize,
    n_points_per_series: usize,
) -> (Arc<Chronix>, TempDir) {
    let tmp = TempDir::new().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(tmp.path())
        .memtable_flush_threshold(128 * 1024 * 1024)
        .build()
        .unwrap();
    let db = Chronix::open(config).unwrap();

    let mut points = Vec::with_capacity(n_series * n_points_per_series);
    for s in 0..n_series {
        for p in 0..n_points_per_series {
            let tags: BTreeMap<String, String> = [
                ("host".to_string(), format!("host-{s}")),
                ("region".to_string(), format!("region-{}", s % 5)),
            ]
            .into_iter()
            .collect();
            let key = SeriesKey::new(metric, tags).unwrap();
            // Monotonically increasing counter (for rate())
            let val = (s * n_points_per_series + p) as f64;
            let fields: BTreeMap<String, FieldValue> =
                [("value".to_string(), FieldValue::F64(val))]
                    .into_iter()
                    .collect();
            // 15s intervals
            let ts = 1_700_000_000_000_000_000_i64 + (p as i64) * 15_000_000_000;
            points.push(Point::new(key, fields, ts).unwrap());
        }
    }
    let _ = db.insert_batch(&points).unwrap();
    db.flush().unwrap();
    (Arc::new(db), tmp)
}

/// Instant query (`rate(metric[5m])`): target < 10ms p99.
fn bench_promql_instant_rate(c: &mut Criterion) {
    let (db, _tmp) = seeded_promql_env("http_requests_total", 100, 100);
    let evaluator = PromQLEvaluator::new(db);

    let expr = parse("rate(http_requests_total[5m])").unwrap();

    // Evaluate at the last data point
    let eval_time = 1_700_000_000_000_000_000_i64 + 99 * 15_000_000_000;

    c.bench_function("promql_instant_rate_100series", |b| {
        b.iter(|| {
            let params = QueryParams {
                time: eval_time,
                ..Default::default()
            };
            let result = evaluator.instant_query(black_box(&expr), &params);
            black_box(result.unwrap());
        });
    });
}

/// Range query (1h range, 15s step): target < 100ms p99.
fn bench_promql_range_query(c: &mut Criterion) {
    let (db, _tmp) = seeded_promql_env("node_cpu_seconds", 10, 240);
    let evaluator = PromQLEvaluator::new(db);

    let expr = parse("rate(node_cpu_seconds[5m])").unwrap();

    let start = 1_700_000_000_000_000_000_i64 + 300_000_000_000; // after 5m lookback
    let end = start + 3_600_000_000_000; // 1 hour
    let step = 15_000_000_000_i64; // 15s

    let mut group = c.benchmark_group("promql_range");
    group.sample_size(10); // fewer samples for expensive benchmark

    group.bench_function("rate_1h_15s_step_10series", |b| {
        b.iter(|| {
            let params = QueryParams {
                time: end,
                start: Some(start),
                end: Some(end),
                step: Some(step),
                ..Default::default()
            };
            let result = evaluator.range_query(black_box(&expr), &params);
            black_box(result.unwrap());
        });
    });
    group.finish();
}

/// Aggregation (`sum by (host) (rate(metric[5m]))`): target < 50ms p99.
fn bench_promql_aggregation(c: &mut Criterion) {
    let (db, _tmp) = seeded_promql_env("app_requests_total", 100, 100);
    let evaluator = PromQLEvaluator::new(db);

    let expr = parse("sum by (host) (rate(app_requests_total[5m]))").unwrap();

    let eval_time = 1_700_000_000_000_000_000_i64 + 99 * 15_000_000_000;

    c.bench_function("promql_sum_by_host_rate_100series", |b| {
        b.iter(|| {
            let params = QueryParams {
                time: eval_time,
                ..Default::default()
            };
            let result = evaluator.instant_query(black_box(&expr), &params);
            black_box(result.unwrap());
        });
    });
}

// ── Offline inspection ─────────────────────────────────────────────

/// `inspect_10k_segments`: the report a support thread asks for, on a data
/// directory of ten thousand segments. Target: under one second.
fn bench_inspect(c: &mut Criterion) {
    let tmp = TempDir::new().unwrap();
    synthetic_dir(tmp.path(), 10_000);
    let mut group = c.benchmark_group("inspect");
    group.sample_size(10);
    group.bench_function("inspect_10k_segments", |b| {
        b.iter(|| {
            black_box(
                chronix::inspect::inspect(tmp.path(), chronix::inspect::InspectOptions::default())
                    .unwrap(),
            )
        });
    });
    group.finish();
}

/// A synthetic data directory of `n` segments: real catalog entries, segment
/// files that are a valid header and footer around an empty body, and real
/// `.series` sidecars — everything `inspect` reads, without `n` flushes.
fn synthetic_dir(root: &std::path::Path, n: u64) {
    use chronix::chronix_core::{SegmentFile, SegmentId, SegmentState, ShardId};
    use chronix::chronix_engine::index::{SegmentCatalog, SegmentCatalogEntry, series_index};
    use chronix::chronix_engine::segment::header::{SegmentFooter, SegmentHeader, VERSION};

    let seg_dir = root.join("segments");
    let mut cat = SegmentCatalog::new(root.join("catalog")).unwrap();
    cat.in_one_sync(|cat| {
        for id in 1..=n {
            let measurement = format!("m{}", id % 6);
            let file = SegmentFile::new(ShardId(0), &format!("seg_{id}.csx")).unwrap();
            let path = file.resolve(&seg_dir);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let header = SegmentHeader {
                version: VERSION,
                flags: 0,
                created_at: id as i64,
                min_timestamp: id as i64 * 1_000,
                max_timestamp: id as i64 * 1_000 + 999,
                row_count: 100,
                column_count: 3,
                series_count: 1,
                compression: 0,
                sort_order: 1,
            };
            let footer = SegmentFooter {
                metadata_offset: 48,
                row_group_count: 1,
                checksum: 0,
                metadata_checksum: 0,
            };
            let mut bytes = header.to_bytes();
            bytes.extend_from_slice(&footer.to_bytes());
            std::fs::write(&path, &bytes).unwrap();
            let tags: std::collections::BTreeMap<String, String> =
                [("unit".to_owned(), format!("u{}", id % 50))].into();
            let key = chronix::chronix_core::SeriesKey::new(measurement.clone(), tags).unwrap();
            series_index::write(&path, &[key], series_index::SegmentStamp::of(&header)).unwrap();
            cat.add_segment(SegmentCatalogEntry {
                segment_id: SegmentId(id),
                shard_id: ShardId(0),
                measurement,
                file,
                min_timestamp: header.min_timestamp,
                max_timestamp: header.max_timestamp,
                row_count: 100,
                series_count: 1,
                byte_size: bytes.len() as u64,
                row_group_count: 1,
                column_count: 3,
                column_stats: Vec::new(),
                state: SegmentState::Active,
            })?;
        }
        Ok(())
    })
    .unwrap();
    cat.force_snapshot().unwrap();
}
