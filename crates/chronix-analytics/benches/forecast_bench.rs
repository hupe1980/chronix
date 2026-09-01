#![allow(clippy::unwrap_used)] // benches may unwrap
//! Performance benchmarks for forecast models.
//!
//! Targets from BACKLOG7:
//! - SES fit (10K) < 5ms, predict (1K) < 100µs
//! - Holt-Winters fit (10K, period=24) < 20ms
//! - ARIMA(1,1,1) fit (10K) < 100ms
//! - Linear Regression fit (10K) < 2ms
//! - Parallel fit (1K series × 10K, SES) < 5s on 8-core
//! - SES predict throughput ≥ 1M pts/sec

use chronix_analytics::forecast::{
    ArimaModel, ForecastModel, HoltLinearModel, HoltWintersModel, LinearRegressionModel, SesModel,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn gen_data(n: usize) -> (Vec<i64>, Vec<f64>) {
    let ts: Vec<i64> = (0..n as i64).map(|i| i * 1_000_000_000).collect();
    let vals: Vec<f64> = (0..n)
        .map(|i| 50.0 + (i as f64 * 0.01).sin() * 10.0 + 0.001 * i as f64)
        .collect();
    (ts, vals)
}

#[allow(dead_code)]
fn gen_seasonal_data(n: usize, period: usize) -> (Vec<i64>, Vec<f64>) {
    let ts: Vec<i64> = (0..n as i64).map(|i| i * 3_600_000_000_000).collect();
    let vals: Vec<f64> = (0..n)
        .map(|i| {
            50.0 + 10.0 * (2.0 * std::f64::consts::PI * i as f64 / period as f64).sin()
                + 0.002 * i as f64
        })
        .collect();
    (ts, vals)
}

fn bench_ses_fit_10k(c: &mut Criterion) {
    let (ts, vals) = gen_data(10_000);
    c.bench_function("ses_fit_10k", |b| {
        b.iter(|| {
            let mut model = SesModel::new(None);
            model.fit(black_box(&ts), black_box(&vals)).unwrap();
        })
    });
}

fn bench_ses_predict_1k(c: &mut Criterion) {
    let (ts, vals) = gen_data(10_000);
    let mut model = SesModel::new(None);
    model.fit(&ts, &vals).unwrap();

    c.bench_function("ses_predict_1k", |b| {
        b.iter(|| model.predict(black_box(1000)).unwrap())
    });
}

fn bench_holt_linear_fit_10k(c: &mut Criterion) {
    let (ts, vals) = gen_data(10_000);
    c.bench_function("holt_linear_fit_10k", |b| {
        b.iter(|| {
            let mut model = HoltLinearModel::new(None, None, 1.0);
            model.fit(black_box(&ts), black_box(&vals)).unwrap();
        })
    });
}

fn bench_holt_winters_fit_10k(c: &mut Criterion) {
    let (ts, vals) = gen_seasonal_data(10_000, 24);
    c.bench_function("holt_winters_fit_10k_period24", |b| {
        b.iter(|| {
            let mut model = HoltWintersModel::new(None, None, None, Some(24), false);
            model.fit(black_box(&ts), black_box(&vals)).unwrap();
        })
    });
}

fn bench_arima_fit_10k(c: &mut Criterion) {
    let (ts, vals) = gen_data(10_000);
    c.bench_function("arima_111_fit_10k", |b| {
        b.iter(|| {
            let mut model = ArimaModel::new(1, 1, 1);
            model.fit(black_box(&ts), black_box(&vals)).unwrap();
        })
    });
}

fn bench_linear_regression_fit_10k(c: &mut Criterion) {
    let (ts, vals) = gen_data(10_000);
    c.bench_function("linear_regression_fit_10k", |b| {
        b.iter(|| {
            let mut model = LinearRegressionModel::new();
            model.fit(black_box(&ts), black_box(&vals)).unwrap();
        })
    });
}

fn bench_parallel_fit_1k_series(c: &mut Criterion) {
    let n_series = 1000;
    let n_points = 10_000;
    let datasets: Vec<(Vec<i64>, Vec<f64>)> = (0..n_series)
        .map(|s| {
            let ts: Vec<i64> = (0..n_points as i64).map(|i| i * 1_000_000_000).collect();
            let vals: Vec<f64> = (0..n_points)
                .map(|i| 50.0 + (i as f64 * 0.01 + s as f64).sin() * 10.0)
                .collect();
            (ts, vals)
        })
        .collect();

    c.bench_function("parallel_fit_1k_series_ses", |b| {
        b.iter(|| {
            let mut models: Vec<SesModel> = (0..n_series).map(|_| SesModel::new(None)).collect();
            let data_refs: Vec<(&[i64], &[f64])> = datasets
                .iter()
                .map(|(t, v)| (t.as_slice(), v.as_slice()))
                .collect();
            chronix_analytics::forecast::parallel_fit(black_box(&mut models), black_box(&data_refs))
        })
    });
}

criterion_group!(
    benches,
    bench_ses_fit_10k,
    bench_ses_predict_1k,
    bench_holt_linear_fit_10k,
    bench_holt_winters_fit_10k,
    bench_arima_fit_10k,
    bench_linear_regression_fit_10k,
    bench_parallel_fit_1k_series,
);
criterion_main!(benches);
