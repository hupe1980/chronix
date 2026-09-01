#![allow(clippy::unwrap_used)] // benches may unwrap
//! Criterion benchmarks for chronix-wal.
//!
//! Run with: `cargo bench -p chronix-wal`

use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};

use chronix_core::{FsyncPolicy, WalConfig};
use chronix_engine::wal::WalWriter;

/// Helper: build a WalConfig with no fsync (measure pure write perf).
fn bench_config() -> WalConfig {
    WalConfig {
        max_file_size: 256 * 1024 * 1024, // 256 MiB — avoid rotation
        max_unflushed_wals: 1024,
        fsync_policy: FsyncPolicy::Periodic(Duration::from_secs(3600)), // effectively disabled
        compress: true,
        ..WalConfig::default()
    }
}

fn bench_single_append(c: &mut Criterion) {
    let payload_sizes: &[usize] = &[64, 256, 1024, 4096];
    let mut group = c.benchmark_group("wal_append_single");

    for &size in payload_sizes {
        let payload = vec![0xABu8; size];

        group.bench_with_input(BenchmarkId::from_parameter(size), &payload, |b, payload| {
            // Each iteration gets a fresh temp dir + writer.
            b.iter_batched(
                || {
                    let dir = tempfile::tempdir().expect("tempdir");
                    let writer =
                        WalWriter::open(dir.path(), bench_config()).expect("open WAL writer");
                    (dir, writer)
                },
                |(_dir, writer)| {
                    let seq = writer.append(black_box(payload)).expect("append");
                    black_box(seq);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_append_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_append_throughput");
    let payload = vec![0xABu8; 128]; // typical point-like payload

    group.throughput(criterion::Throughput::Elements(1000));
    group.bench_function("1000_appends_128B", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().expect("tempdir");
                let writer = WalWriter::open(dir.path(), bench_config()).expect("open WAL writer");
                (dir, writer)
            },
            |(_dir, writer)| {
                for _ in 0..1000 {
                    let _ = writer.append(black_box(&payload));
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_batch_append(c: &mut Criterion) {
    let batch_sizes: &[usize] = &[10, 100, 1000];
    let mut group = c.benchmark_group("wal_append_batch");

    for &batch_size in batch_sizes {
        let payload = vec![0xABu8; 128];
        let payloads: Vec<&[u8]> = (0..batch_size).map(|_| payload.as_slice()).collect();

        group.throughput(criterion::Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &payloads,
            |b, payloads| {
                b.iter_batched(
                    || {
                        let dir = tempfile::tempdir().expect("tempdir");
                        let writer =
                            WalWriter::open(dir.path(), bench_config()).expect("open WAL writer");
                        (dir, writer)
                    },
                    |(_dir, writer)| {
                        let last = writer.append_batch(black_box(payloads)).expect("batch");
                        black_box(last);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_replay(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_replay");
    let record_counts: &[usize] = &[100, 1000, 10_000];

    for &count in record_counts {
        group.throughput(criterion::Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter_batched(
                || {
                    // Setup: write `count` records into a temporary WAL dir.
                    let dir = tempfile::tempdir().expect("tempdir");
                    let writer =
                        WalWriter::open(dir.path(), bench_config()).expect("open WAL writer");
                    let payload = vec![0xABu8; 128];
                    for _ in 0..count {
                        writer.append(&payload).expect("append");
                    }
                    writer.sync().expect("sync");
                    dir
                },
                |dir| {
                    let records = chronix_engine::wal::replay_all(dir.path()).expect("replay_all");
                    black_box(records);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_sync");

    group.bench_function("sync_after_100_appends", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().expect("tempdir");
                let writer = WalWriter::open(dir.path(), bench_config()).expect("open WAL writer");
                let payload = vec![0xABu8; 128];
                for _ in 0..100 {
                    writer.append(&payload).expect("append");
                }
                (dir, writer)
            },
            |(_dir, writer)| {
                writer.sync().expect("sync");
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_append,
    bench_append_throughput,
    bench_batch_append,
    bench_replay,
    bench_sync,
);
criterion_main!(benches);
