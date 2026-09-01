#![allow(clippy::unwrap_used)] // benches may unwrap
//! Benchmarks for the chronix-memtable crate.

use std::collections::BTreeMap;
use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};

use chronix_core::types::{FieldValue, Point, SeriesKey};
use chronix_engine::memtable::Memtable;

fn make_point(host_id: usize, ts: i64, value: f64) -> Point {
    let tags: BTreeMap<String, String> = [("host".to_string(), format!("host-{host_id}"))]
        .into_iter()
        .collect();
    let series_key = SeriesKey::new("cpu".to_string(), tags).unwrap();
    let fields: BTreeMap<String, FieldValue> = [("value".to_string(), FieldValue::F64(value))]
        .into_iter()
        .collect();
    Point::new(series_key, fields, ts).unwrap()
}

fn bench_insert_sequential(c: &mut Criterion) {
    let points: Vec<Point> = (0..100_000)
        .map(|i| make_point(i % 10, i as i64 * 1_000_000_000, i as f64))
        .collect();

    c.bench_function("memtable_insert_100K_seq", |b| {
        b.iter_with_setup(Memtable::new, |mt| {
            for p in &points {
                mt.insert(black_box(p)).unwrap();
            }
            black_box(&mt);
        });
    });
}

fn bench_insert_concurrent(c: &mut Criterion) {
    // Pre-generate points for 4 threads, 25K each
    let thread_points: Vec<Vec<Point>> = (0..4)
        .map(|t| {
            (0..25_000)
                .map(|i| {
                    make_point(
                        t * 10 + (i % 10),
                        (t * 100_000 + i) as i64 * 1_000_000_000,
                        i as f64,
                    )
                })
                .collect()
        })
        .collect();

    c.bench_function("memtable_insert_100K_4threads", |b| {
        b.iter_with_setup(
            || Arc::new(Memtable::new()),
            |mt| {
                std::thread::scope(|s| {
                    for points in &thread_points {
                        let mt = Arc::clone(&mt);
                        s.spawn(move || {
                            for p in points {
                                mt.insert(black_box(p)).unwrap();
                            }
                        });
                    }
                });
                black_box(&mt);
            },
        );
    });
}

fn bench_scan_all(c: &mut Criterion) {
    let mt = Memtable::new();
    for i in 0..10_000 {
        mt.insert(&make_point(i % 10, i as i64 * 1_000_000_000, i as f64))
            .unwrap();
    }

    c.bench_function("memtable_scan_all_10K", |b| {
        b.iter(|| {
            black_box(mt.scan_all());
        });
    });
}

fn bench_scan_series(c: &mut Criterion) {
    let mt = Memtable::new();
    for i in 0..10_000 {
        mt.insert(&make_point(i % 10, i as i64 * 1_000_000_000, i as f64))
            .unwrap();
    }

    let key = SeriesKey::new(
        "cpu".to_string(),
        [("host".to_string(), "host-0".to_string())]
            .into_iter()
            .collect(),
    )
    .unwrap();

    c.bench_function("memtable_scan_series_1K_of_10K", |b| {
        b.iter(|| {
            black_box(mt.scan(black_box(&key), 0, i64::MAX));
        });
    });
}

criterion_group!(
    benches,
    bench_insert_sequential,
    bench_insert_concurrent,
    bench_scan_all,
    bench_scan_series,
);
criterion_main!(benches);
