#![allow(clippy::unwrap_used)] // benches may unwrap
//! Performance benchmarks for preprocessing and feature engineering.
//!
//! Key target: feature functions < 10ms on 1M rows.

use chronix_analytics::preprocess::decomposition::{stl_decompose, StlConfig};
use chronix_analytics::preprocess::{diff, ewm, pct_change, rolling_std, zscore};
use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;

fn gen_data(n: usize) -> Vec<f64> {
    (0..n)
        .map(|i| 50.0 + (i as f64 * 0.01).sin() * 10.0)
        .collect()
}

fn bench_diff_1m(c: &mut Criterion) {
    let vals = gen_data(1_000_000);
    c.bench_function("diff_order1_1m", |b| b.iter(|| diff(black_box(&vals), 1)));
}

fn bench_pct_change_1m(c: &mut Criterion) {
    let vals = gen_data(1_000_000);
    c.bench_function("pct_change_1m", |b| b.iter(|| pct_change(black_box(&vals))));
}

fn bench_rolling_std_1m(c: &mut Criterion) {
    let vals = gen_data(1_000_000);
    c.bench_function("rolling_std_1m_w60", |b| {
        b.iter(|| rolling_std(black_box(&vals), 60))
    });
}

fn bench_zscore_1m(c: &mut Criterion) {
    let vals = gen_data(1_000_000);
    c.bench_function("zscore_1m", |b| b.iter(|| zscore(black_box(&vals))));
}

fn bench_ewm_1m(c: &mut Criterion) {
    let vals = gen_data(1_000_000);
    c.bench_function("ewm_1m_alpha03", |b| b.iter(|| ewm(black_box(&vals), 0.3)));
}

fn bench_stl_10k_period24(c: &mut Criterion) {
    let n = 10_000;
    let period = 24;
    let vals: Vec<f64> = (0..n)
        .map(|i| {
            50.0 + 10.0 * (2.0 * std::f64::consts::PI * i as f64 / period as f64).sin()
                + 0.01 * i as f64
        })
        .collect();

    let config = StlConfig::new(period);
    c.bench_function("stl_decompose_10k_period24", |b| {
        b.iter(|| stl_decompose(black_box(&vals), black_box(&config)).unwrap())
    });
}

criterion_group!(
    benches,
    bench_diff_1m,
    bench_pct_change_1m,
    bench_rolling_std_1m,
    bench_zscore_1m,
    bench_ewm_1m,
    bench_stl_10k_period24,
);
criterion_main!(benches);
