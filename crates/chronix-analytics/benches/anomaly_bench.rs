#![allow(clippy::unwrap_used)] // benches may unwrap
//! Benchmarks for anomaly detectors.

use chronix_analytics::anomaly::{
    AnomalyDetector, DynamicThresholdDetector, IqrDetector, ModifiedZScoreDetector,
    MovingAverageResidualDetector, ZScoreDetector,
};
use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;

fn gen_data(n: usize) -> (Vec<i64>, Vec<f64>) {
    let ts: Vec<i64> = (0..n as i64).map(|i| i * 1_000_000_000).collect();
    let vals: Vec<f64> = (0..n)
        .map(|i| (i as f64 * 0.01).sin() * 10.0 + 50.0)
        .collect();
    (ts, vals)
}

fn bench_zscore_detect_1m(c: &mut Criterion) {
    let (ts, vals) = gen_data(1_000_000);
    let mut det = ZScoreDetector::new(None);
    det.fit(&ts, &vals).unwrap();

    c.bench_function("zscore_detect_1m", |b| {
        b.iter(|| det.detect(black_box(&ts), black_box(&vals)).unwrap())
    });
}

fn bench_iqr_detect_1m(c: &mut Criterion) {
    let (ts, vals) = gen_data(1_000_000);
    let mut det = IqrDetector::new(None);
    det.fit(&ts, &vals).unwrap();

    c.bench_function("iqr_detect_1m", |b| {
        b.iter(|| det.detect(black_box(&ts), black_box(&vals)).unwrap())
    });
}

fn bench_modified_zscore_detect_1m(c: &mut Criterion) {
    let (ts, vals) = gen_data(1_000_000);
    let mut det = ModifiedZScoreDetector::new(None);
    det.fit(&ts, &vals).unwrap();

    c.bench_function("modified_zscore_detect_1m", |b| {
        b.iter(|| det.detect(black_box(&ts), black_box(&vals)).unwrap())
    });
}

fn bench_dynamic_threshold_detect_1m(c: &mut Criterion) {
    let (ts, vals) = gen_data(1_000_000);
    let mut det = DynamicThresholdDetector::new(Some(100), None);
    det.fit(&ts, &vals).unwrap();

    c.bench_function("dynamic_threshold_detect_1m", |b| {
        b.iter(|| det.detect(black_box(&ts), black_box(&vals)).unwrap())
    });
}

fn bench_moving_avg_detect_streaming(c: &mut Criterion) {
    let (ts, vals) = gen_data(100_000);
    let mut det = MovingAverageResidualDetector::new(Some(24), None);
    det.fit(&ts, &vals).unwrap();

    c.bench_function("moving_avg_detect_point_100k", |b| {
        b.iter(|| {
            for i in 0..vals.len() {
                let _ = det.detect_point(black_box(ts[i]), black_box(vals[i]));
            }
        })
    });
}

criterion_group!(
    benches,
    bench_zscore_detect_1m,
    bench_iqr_detect_1m,
    bench_modified_zscore_detect_1m,
    bench_dynamic_threshold_detect_1m,
    bench_moving_avg_detect_streaming,
);
criterion_main!(benches);
