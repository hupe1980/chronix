#![allow(clippy::unwrap_used)] // benches may unwrap
//! Benchmarks for multivariate analysis (Epic 10 / Story 11.8).

use chronix_analytics::multivariate::{
    AnalyticsResults, ColumnarMatrix, CompositeSignalEngine, CompositeSignalRule,
    CrossCorrelationMatrix, DerivedSeriesDefinition, DerivedSeriesEngine, MahalanobisDetector,
    MultiSeriesContext, MultivariateAnomalyDetector, MultivariateForecastModel, RollingCorrelation,
    VarModel,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn make_context(n: usize, k: usize) -> MultiSeriesContext {
    let data: Vec<Vec<f64>> = (0..k)
        .map(|s| {
            (0..n)
                .map(|i| (i as f64 * 0.01 + s as f64).sin() * 10.0 + 50.0)
                .collect()
        })
        .collect();
    let ts: Vec<i64> = (0..n as i64).map(|i| i * 1_000_000_000).collect();
    let ids: Vec<String> = (0..k).map(|s| format!("s{s}")).collect();
    MultiSeriesContext {
        matrix: ColumnarMatrix {
            data,
            timestamps: ts,
            series_ids: ids,
        },
    }
}

fn bench_rolling_pearson_1m(c: &mut Criterion) {
    let n = 1_000_000;
    let x: Vec<f64> = (0..n).map(|i| (i as f64 * 0.01).sin()).collect();
    let y: Vec<f64> = (0..n).map(|i| (i as f64 * 0.01).cos()).collect();
    let rc = RollingCorrelation::new(100);

    c.bench_function("rolling_pearson_1m", |b| {
        b.iter(|| rc.compute(black_box(&x), black_box(&y)).unwrap())
    });
}

fn bench_cross_correlation_100(c: &mut Criterion) {
    let n = 10_000;
    let k = 100;
    let series: Vec<Vec<f64>> = (0..k)
        .map(|s| (0..n).map(|i| (i as f64 * 0.01 + s as f64).sin()).collect())
        .collect();
    let refs: Vec<&[f64]> = series.iter().map(std::vec::Vec::as_slice).collect();

    c.bench_function("cross_correlation_100x10k", |b| {
        b.iter(|| CrossCorrelationMatrix::compute(black_box(&refs)).unwrap())
    });
}

fn bench_mahalanobis_50x10k(c: &mut Criterion) {
    let ctx = make_context(10_000, 50);
    let mut det = MahalanobisDetector::new(None, None);
    det.fit(&ctx).unwrap();

    c.bench_function("mahalanobis_50x10k", |b| {
        b.iter(|| det.detect(black_box(&ctx)).unwrap())
    });
}

fn bench_var_10x1k(c: &mut Criterion) {
    let ctx = make_context(1_000, 10);
    let mut model = VarModel::new(Some(10));
    model.fit(&ctx, 0).unwrap();

    c.bench_function("var_10x1k_predict", |b| {
        b.iter(|| model.predict(black_box(24)).unwrap())
    });
}

fn bench_derived_series_dag_5_100k(c: &mut Criterion) {
    use chronix_analytics::multivariate::ArithOp;
    use chronix_analytics::multivariate::ArithmeticExpr;

    c.bench_function("derived_series_dag_5_100k", |b| {
        b.iter(|| {
            let mut ctx_clone = make_context(100_000, 3);
            ctx_clone.matrix.series_ids = vec!["a".into(), "b".into(), "c".into()];
            let engine = DerivedSeriesEngine::new(vec![
                DerivedSeriesDefinition {
                    name: "ab_sum".into(),
                    expression: Box::new(ArithmeticExpr {
                        left: "a".into(),
                        right: "b".into(),
                        op: ArithOp::Add,
                    }),
                    dependencies: vec!["a".into(), "b".into()],
                },
                DerivedSeriesDefinition {
                    name: "bc_diff".into(),
                    expression: Box::new(ArithmeticExpr {
                        left: "b".into(),
                        right: "c".into(),
                        op: ArithOp::Sub,
                    }),
                    dependencies: vec!["b".into(), "c".into()],
                },
                DerivedSeriesDefinition {
                    name: "ab_bc_product".into(),
                    expression: Box::new(ArithmeticExpr {
                        left: "ab_sum".into(),
                        right: "bc_diff".into(),
                        op: ArithOp::Mul,
                    }),
                    dependencies: vec!["ab_sum".into(), "bc_diff".into()],
                },
                DerivedSeriesDefinition {
                    name: "a_times_c".into(),
                    expression: Box::new(ArithmeticExpr {
                        left: "a".into(),
                        right: "c".into(),
                        op: ArithOp::Mul,
                    }),
                    dependencies: vec!["a".into(), "c".into()],
                },
                DerivedSeriesDefinition {
                    name: "final_ratio".into(),
                    expression: Box::new(ArithmeticExpr {
                        left: "ab_bc_product".into(),
                        right: "a_times_c".into(),
                        op: ArithOp::Div,
                    }),
                    dependencies: vec!["ab_bc_product".into(), "a_times_c".into()],
                },
            ]);
            engine.evaluate(black_box(&mut ctx_clone)).unwrap()
        })
    });
}

fn bench_composite_signal_10_100k(c: &mut Criterion) {
    let ctx = make_context(100_000, 5);
    let analytics = AnalyticsResults::default();

    let rules: Vec<CompositeSignalRule> = (0..10)
        .map(|i| CompositeSignalRule {
            name: format!("rule_{i}"),
            signal_type: format!("alert_{i}"),
            contributing_series: vec!["s0".into(), "s1".into()],
            cooldown_ns: 0,
            condition: Box::new(
                move |ctx: &MultiSeriesContext, _analytics: &AnalyticsResults| {
                    let data = &ctx.matrix.data;
                    if data.is_empty() || data[0].is_empty() {
                        return (false, 0.0);
                    }
                    let last = data[0][data[0].len() - 1];
                    (last > 40.0 + i as f64, 0.9)
                },
            ),
        })
        .collect();

    c.bench_function("composite_signal_10_rules_100k", |b| {
        b.iter(|| {
            let mut engine = CompositeSignalEngine::new();
            engine.evaluate(black_box(&rules), black_box(&ctx), black_box(&analytics))
        })
    });
}

criterion_group!(
    benches,
    bench_rolling_pearson_1m,
    bench_cross_correlation_100,
    bench_mahalanobis_50x10k,
    bench_var_10x1k,
    bench_derived_series_dag_5_100k,
    bench_composite_signal_10_100k,
);
criterion_main!(benches);
