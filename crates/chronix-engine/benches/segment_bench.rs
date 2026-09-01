#![allow(clippy::unwrap_used)] // benches may unwrap
//! Benchmarks for the chronix-segment crate.

use std::collections::BTreeMap;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tempfile::TempDir;

use chronix_core::types::{FieldValue, Point, SeriesKey};
use chronix_engine::segment::reader::SegmentReader;
use chronix_engine::segment::writer::{SegmentWriter, SegmentWriterConfig};

fn generate_points(n: usize) -> Vec<Point> {
    (0..n)
        .map(|i| {
            let tags: BTreeMap<String, String> = [("host".to_string(), format!("host-{}", i % 10))]
                .into_iter()
                .collect();
            let series_key = SeriesKey::new("cpu".to_string(), tags).unwrap();
            let fields: BTreeMap<String, FieldValue> = [
                (
                    "usage".to_string(),
                    FieldValue::F64(20.0 + (i as f64) * 0.01),
                ),
                ("count".to_string(), FieldValue::I64(i as i64)),
            ]
            .into_iter()
            .collect();
            let ts = 1_700_000_000_000_000_000_i64 + (i as i64) * 1_000_000_000;
            Point::new(series_key, fields, ts).unwrap()
        })
        .collect()
}

fn bench_segment_write(c: &mut Criterion) {
    let points = generate_points(10_000);

    c.bench_function("segment_write_10K", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                let path = dir.path().join("bench.csx");
                (dir, path)
            },
            |(_dir, path)| {
                let config = SegmentWriterConfig {
                    compress: true,
                    ..Default::default()
                };
                let mut writer = SegmentWriter::new(&path, config).unwrap();
                writer.write_rows(black_box(&points)).unwrap();
                black_box(writer.finalize().unwrap());
            },
        );
    });
}

fn bench_segment_read(c: &mut Criterion) {
    let points = generate_points(10_000);
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bench_read.csx");

    let config = SegmentWriterConfig {
        compress: true,
        ..Default::default()
    };
    let mut writer = SegmentWriter::new(&path, config).unwrap();
    writer.write_rows(&points).unwrap();
    writer.finalize().unwrap();

    c.bench_function("segment_read_all_10K", |b| {
        b.iter(|| {
            let reader = SegmentReader::open(black_box(&path)).unwrap();
            black_box(reader.read_all().unwrap());
        });
    });

    c.bench_function("segment_read_column_10K", |b| {
        b.iter(|| {
            let reader = SegmentReader::open(black_box(&path)).unwrap();
            black_box(reader.read_columns(&["usage"], 0).unwrap());
        });
    });
}

fn bench_segment_write_uncompressed(c: &mut Criterion) {
    let points = generate_points(10_000);

    c.bench_function("segment_write_uncompressed_10K", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                let path = dir.path().join("bench.csx");
                (dir, path)
            },
            |(_dir, path)| {
                let config = SegmentWriterConfig {
                    compress: false,
                    ..Default::default()
                };
                let mut writer = SegmentWriter::new(&path, config).unwrap();
                writer.write_rows(black_box(&points)).unwrap();
                black_box(writer.finalize().unwrap());
            },
        );
    });
}

criterion_group!(
    benches,
    bench_segment_write,
    bench_segment_read,
    bench_segment_write_uncompressed,
);
criterion_main!(benches);
