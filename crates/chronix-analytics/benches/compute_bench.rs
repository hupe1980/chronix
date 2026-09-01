#![allow(clippy::unwrap_used)] // benches may unwrap
//! Benchmarks for chronix-compute SIMD, CPU, and GPU engine operations.

use chronix_analytics::compute::{
    simd_mean, simd_min_max, simd_sum, simd_variance, ComputeEngine, CpuEngine,
};
use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;

fn bench_simd_sum(c: &mut Criterion) {
    let data: Vec<f64> = (0..1_000_000).map(|i| i as f64 * 0.001).collect();
    c.bench_function("simd_sum_1M", |b| b.iter(|| simd_sum(black_box(&data))));
}

fn bench_simd_mean(c: &mut Criterion) {
    let data: Vec<f64> = (0..1_000_000).map(|i| i as f64 * 0.001).collect();
    c.bench_function("simd_mean_1M", |b| b.iter(|| simd_mean(black_box(&data))));
}

fn bench_simd_variance(c: &mut Criterion) {
    let data: Vec<f64> = (0..1_000_000).map(|i| i as f64 * 0.001).collect();
    let mean = simd_mean(&data);
    c.bench_function("simd_variance_1M", |b| {
        b.iter(|| simd_variance(black_box(&data), mean))
    });
}

fn bench_simd_min_max(c: &mut Criterion) {
    let data: Vec<f64> = (0..1_000_000).map(|i| (i as f64).sin()).collect();
    c.bench_function("simd_min_max_1M", |b| {
        b.iter(|| simd_min_max(black_box(&data)))
    });
}

fn bench_dot_product(c: &mut Criterion) {
    let a: Vec<f64> = (0..1_000_000).map(|i| i as f64 * 0.001).collect();
    let b: Vec<f64> = (0..1_000_000).map(|i| (i as f64 * 0.002).sin()).collect();
    let engine = CpuEngine::default();
    c.bench_function("cpu_dot_product_1M", |bench| {
        bench.iter(|| engine.batch_dot_product(black_box(&a), black_box(&b)))
    });
}

fn bench_cpu_difference(c: &mut Criterion) {
    let data: Vec<f64> = (0..100_000).map(|i| (i as f64).sin()).collect();
    let engine = CpuEngine::default();
    c.bench_function("cpu_difference_100K", |bench| {
        bench.iter(|| engine.batch_difference(black_box(&data), 1))
    });
}

fn bench_cpu_autocorrelation(c: &mut Criterion) {
    let data: Vec<f64> = (0..10_000).map(|i| (i as f64 * 0.1).sin()).collect();
    let engine = CpuEngine::default();
    c.bench_function("cpu_autocorrelation_10K_lag50", |bench| {
        bench.iter(|| engine.batch_autocorrelation(black_box(&data), 50))
    });
}

fn bench_cpu_exp_smooth(c: &mut Criterion) {
    let data: Vec<f64> = (0..100_000).map(|i| (i as f64 * 0.01).sin()).collect();
    let engine = CpuEngine::default();
    c.bench_function("cpu_exp_smooth_100K", |bench| {
        bench.iter(|| engine.batch_exponential_smooth(black_box(&data), 0.3))
    });
}

fn bench_cpu_z_score(c: &mut Criterion) {
    let engine = CpuEngine::default();
    let data: Vec<f64> = (0..100_000).map(|i| (i as f64 * 0.01).sin()).collect();
    c.bench_function("cpu_z_score_100K", |bench| {
        bench.iter(|| engine.batch_z_score(black_box(&data)))
    });
}

// ---------------------------------------------------------------------------
// Scale benchmarks (CPU baseline: 10K series × 10K points)
// ---------------------------------------------------------------------------

/// Simulate batch SES over many series (10K series × 10K points each).
fn bench_scale_cpu_exp_smooth(c: &mut Criterion) {
    let series: Vec<Vec<f64>> = (0..10_000)
        .map(|s| {
            (0..10_000)
                .map(|i| ((s * 10_000 + i) as f64 * 0.01).sin())
                .collect()
        })
        .collect();
    let engine = CpuEngine::default();
    c.bench_function("scale_cpu_exp_smooth_10Kx10K", |bench| {
        bench.iter(|| {
            for s in &series {
                let _ = engine.batch_exponential_smooth(black_box(s), 0.3);
            }
        })
    });
}

/// Simulate batch Z-Score over many series (10K series × 10K points each).
fn bench_scale_cpu_z_score(c: &mut Criterion) {
    let series: Vec<Vec<f64>> = (0..10_000)
        .map(|s| {
            (0..10_000)
                .map(|i| ((s * 10_000 + i) as f64 * 0.01).sin())
                .collect()
        })
        .collect();
    let engine = CpuEngine::default();
    c.bench_function("scale_cpu_z_score_10Kx10K", |bench| {
        bench.iter(|| {
            for s in &series {
                let _ = engine.batch_z_score(black_box(s));
            }
        })
    });
}

criterion_group!(
    benches,
    bench_simd_sum,
    bench_simd_mean,
    bench_simd_variance,
    bench_simd_min_max,
    bench_dot_product,
    bench_cpu_difference,
    bench_cpu_autocorrelation,
    bench_cpu_exp_smooth,
    bench_cpu_z_score,
    bench_scale_cpu_exp_smooth,
    bench_scale_cpu_z_score,
);

criterion_main!(benches);
