#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Multivariate Analysis
//!
//! Demonstrates cross-series correlation, multivariate anomaly detection,
//! multivariate forecasting, and derived series computation.
//!
//! ```bash
//! cargo run --example multivariate_analysis
//! ```

use chronix::chronix_analytics::multivariate::{
    ArithOp, ArithmeticExpr, CrossCorrelationMatrix, IsolationForestDetector, KendallTau,
    LagCorrelation, MahalanobisDetector, MultiLinearRegression, MultiSeriesContext,
    MultivariateAnomalyDetector, MultivariateForecastModel, PcaAnomalyDetector, PearsonCorrelation,
    RollingCorrelation, SpearmanCorrelation, VarModel,
};

fn main() {
    println!("=== Chronix Multivariate Analysis ===\n");

    // ── Generate three correlated time series ─────────────────────
    let n = 200;
    let timestamps: Vec<i64> = (0..n).map(|i| 1_000_000_000 + i * 1_000_000_000).collect();

    // Series A: base trend
    let series_a: Vec<f64> = (0..n)
        .map(|i| 100.0 + (i as f64) * 0.5 + (i as f64 * 0.1).sin() * 10.0)
        .collect();

    // Series B: positively correlated with A (+ noise)
    let series_b: Vec<f64> = series_a
        .iter()
        .enumerate()
        .map(|(i, &v)| v * 1.2 + 5.0 + (i as f64 * 0.3).cos() * 3.0)
        .collect();

    // Series C: negatively correlated with A
    let series_c: Vec<f64> = series_a
        .iter()
        .enumerate()
        .map(|(i, &v)| 300.0 - v * 0.8 + (i as f64 * 0.2).sin() * 5.0)
        .collect();

    // Build the multivariate context
    let ctx = MultiSeriesContext::build(
        vec![
            (
                "cpu_usage".to_string(),
                timestamps.clone(),
                series_a.clone(),
            ),
            (
                "memory_usage".to_string(),
                timestamps.clone(),
                series_b.clone(),
            ),
            ("disk_io".to_string(), timestamps.clone(), series_c.clone()),
        ],
        None,
    )
    .expect("Failed to build MultiSeriesContext");

    println!(
        "Built context: {} series × {} timestamps\n",
        ctx.matrix.n_series(),
        ctx.matrix.n_timestamps()
    );

    // ── 1. Pairwise Correlations ──────────────────────────────────
    println!("--- Pairwise Correlation ---");

    let pearson_ab = PearsonCorrelation::compute(&series_a, &series_b).expect("Pearson A-B failed");
    let pearson_ac = PearsonCorrelation::compute(&series_a, &series_c).expect("Pearson A-C failed");
    println!("Pearson  (cpu ↔ memory): {pearson_ab:.4}  (expected: strong positive)");
    println!("Pearson  (cpu ↔ disk):   {pearson_ac:.4}  (expected: strong negative)");

    let spearman_ab =
        SpearmanCorrelation::compute(&series_a, &series_b).expect("Spearman A-B failed");
    println!("Spearman (cpu ↔ memory): {spearman_ab:.4}");

    let kendall_ab = KendallTau::compute(&series_a, &series_b).expect("Kendall A-B failed");
    println!("Kendall  (cpu ↔ memory): {kendall_ab:.4}");

    // ── 2. Rolling & Lag Correlation ──────────────────────────────
    println!("\n--- Rolling Correlation (window=30) ---");
    let rolling = RollingCorrelation::new(30);
    let rolling_vals = rolling
        .compute(&series_a, &series_b)
        .expect("Rolling correlation failed");
    // The first `window - 1` values are NaN by construction: a 30-point
    // correlation has nothing to say about point 3.
    let settled = &rolling_vals[29.min(rolling_vals.len())..];
    println!(
        "Rolling corr: 29 NaN warm-up, then {:?}",
        &settled[..5.min(settled.len())]
    );
    println!(
        "Rolling corr last 5: {:?}",
        &rolling_vals[rolling_vals.len().saturating_sub(5)..]
    );

    println!("\n--- Lag Cross-Correlation ---");
    let lags = vec![-3, -2, -1, 0, 1, 2, 3];
    let lag_corr =
        LagCorrelation::compute(&series_a, &series_b, &lags).expect("Lag correlation failed");
    for (lag, corr) in &lag_corr {
        println!("  lag {lag:+}: {corr:.4}");
    }

    // ── 3. Cross-Correlation Matrix ───────────────────────────────
    println!("\n--- Cross-Correlation Matrix (NxN) ---");
    let slices: Vec<&[f64]> = vec![&series_a, &series_b, &series_c];
    let matrix = CrossCorrelationMatrix::compute(&slices).expect("CCM failed");
    let labels = ["cpu", "mem", "disk"];
    print!("{:>8}", "");
    for l in &labels {
        print!("{l:>8}");
    }
    println!();
    for (i, row) in matrix.iter().enumerate() {
        print!("{:>8}", labels[i]);
        for val in row {
            print!("{val:>8.3}");
        }
        println!();
    }

    // ── 4. Multivariate Anomaly Detection ─────────────────────────
    println!("\n--- Multivariate Anomaly Detection ---");

    // Inject anomalies: create a modified context with spikes
    let mut series_a_anom = series_a.clone();
    let mut series_b_anom = series_b.clone();
    series_a_anom[50] += 80.0; // spike at index 50
    series_b_anom[50] -= 60.0; // opposite spike → multivariate anomaly
    series_a_anom[120] += 70.0;
    series_b_anom[120] -= 50.0;

    let ctx_anom = MultiSeriesContext::build(
        vec![
            ("cpu_usage".to_string(), timestamps.clone(), series_a_anom),
            (
                "memory_usage".to_string(),
                timestamps.clone(),
                series_b_anom,
            ),
            ("disk_io".to_string(), timestamps.clone(), series_c.clone()),
        ],
        None,
    )
    .expect("Failed to build anomalous context");

    // Mahalanobis detector
    let mut mahal = MahalanobisDetector::new(Some(3.0), None);
    mahal.fit(&ctx_anom).expect("Mahalanobis fit failed");
    let mahal_results = mahal.detect(&ctx_anom).expect("Mahalanobis detect failed");
    let mahal_anomalies: Vec<_> = mahal_results.iter().filter(|s| s.is_anomaly).collect();
    println!(
        "Mahalanobis: detected {} anomalies out of {} points",
        mahal_anomalies.len(),
        mahal_results.len()
    );
    for a in mahal_anomalies.iter().take(5) {
        println!(
            "  score={:.3}, contributions={:?}",
            a.score, a.contributions
        );
    }

    // Isolation Forest detector
    let mut iforest = IsolationForestDetector::new(Some(100), Some(128), Some(0.6));
    iforest.fit(&ctx_anom).expect("IsolationForest fit failed");
    let iforest_results = iforest
        .detect(&ctx_anom)
        .expect("IsolationForest detect failed");
    let iforest_anomalies: Vec<_> = iforest_results.iter().filter(|s| s.is_anomaly).collect();
    println!(
        "\nIsolation Forest: detected {} anomalies out of {} points",
        iforest_anomalies.len(),
        iforest_results.len()
    );

    // PCA detector
    let mut pca = PcaAnomalyDetector::new(None, None);
    pca.fit(&ctx_anom).expect("PCA fit failed");
    let pca_results = pca.detect(&ctx_anom).expect("PCA detect failed");
    let pca_anomalies: Vec<_> = pca_results.iter().filter(|s| s.is_anomaly).collect();
    println!(
        "PCA: detected {} anomalies out of {} points",
        pca_anomalies.len(),
        pca_results.len()
    );

    // ── 5. Multivariate Forecasting ───────────────────────────────
    println!("\n--- Multivariate Forecasting ---");

    // Multi-Linear Regression: predict series 0 from all others
    let mut mlr = MultiLinearRegression::new();
    mlr.fit(&ctx, 0).expect("MLR fit failed");
    let mlr_result = mlr.predict(10).expect("MLR predict failed");
    println!(
        "Multi-Linear Regression: predicted {} future timestamps",
        mlr_result.timestamps.len()
    );
    println!("  Predictor importance:");
    for (name, imp) in &mlr_result.predictor_importance {
        println!("    {name}: {imp:.4}");
    }
    println!(
        "  First 5 predictions: {:?}",
        &mlr_result.predictions[0][..5.min(mlr_result.predictions[0].len())]
    );

    // VAR Model
    let mut var = VarModel::new(Some(2));
    var.fit(&ctx, 0).expect("VAR fit failed");
    let var_result = var.predict(10).expect("VAR predict failed");
    println!(
        "\nVAR(2) Model: predicted {} future timestamps",
        var_result.timestamps.len()
    );

    // ── 6. Derived Series ─────────────────────────────────────────
    println!("\n--- Derived Series ---");
    use chronix::chronix_analytics::multivariate::DerivedSeriesExpr;
    let spread = ArithmeticExpr {
        left: "cpu_usage".to_string(),
        right: "disk_io".to_string(),
        op: ArithOp::Sub,
    };
    let spread_vals = spread.evaluate(&ctx).expect("Spread eval failed");
    println!(
        "Spread (cpu - disk) first 5: {:?}",
        &spread_vals[..5.min(spread_vals.len())]
    );

    let ratio = ArithmeticExpr {
        left: "cpu_usage".to_string(),
        right: "memory_usage".to_string(),
        op: ArithOp::Div,
    };
    let ratio_vals = ratio.evaluate(&ctx).expect("Ratio eval failed");
    println!(
        "Ratio  (cpu / memory) first 5: {:?}",
        &ratio_vals[..5.min(ratio_vals.len())]
    );

    println!("\n✓ Multivariate analysis complete");
}
