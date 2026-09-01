#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Data Preprocessing
//!
//! Demonstrates gap detection, interpolation, smoothing, STL decomposition,
//! resampling, feature engineering, and the full preprocessing pipeline.
//!
//! ```bash
//! cargo run --example preprocessing
//! ```

use chronix::chronix_analytics::preprocess::auto_features::auto_features;
use chronix::chronix_analytics::preprocess::{
    detect_period, diff, ewm, lag, pct_change, rolling_std, stl_decompose, AutoFeatureConfig,
    ExponentialSmoother, GapDetector, Interpolator, MovingAverageSmoother, PreprocessConfig,
    PreprocessPipeline, ResampleConfig, Resampler, Smoother, StlConfig, WeightedMovingAverage,
};

fn main() {
    println!("=== Chronix Data Preprocessing ===\n");

    let interval_ns: i64 = 1_000_000_000; // 1 second

    // ── Generate data with deliberate gaps ────────────────────────
    let mut timestamps: Vec<i64> = Vec::new();
    let mut values: Vec<f64> = Vec::new();
    let base_ts = 1_700_000_000_000_000_000i64;

    for i in 0..200 {
        // Skip indices 50–54 and 120–122 to create gaps
        if (50..=54).contains(&i) || (120..=122).contains(&i) {
            continue;
        }
        timestamps.push(base_ts + i * interval_ns);
        let t = i as f64;
        // Seasonal pattern + trend + noise
        values.push(50.0 + t * 0.1 + (t * 2.0 * std::f64::consts::PI / 24.0).sin() * 15.0);
    }

    println!(
        "Generated {} points with gaps (expected 200, missing {})\n",
        timestamps.len(),
        200 - timestamps.len()
    );

    // ── 1. Gap Detection ──────────────────────────────────────────
    println!("--- Gap Detection ---");
    let detector = GapDetector::new(interval_ns, 1.5).expect("Failed to create GapDetector");
    let gaps = detector.detect(&timestamps);
    println!("Found {} gaps:", gaps.len());
    for gap in &gaps {
        println!(
            "  Gap at index {}: {} missing points (ts {} → {})",
            gap.before_idx, gap.missing_count, gap.before_ts, gap.after_ts
        );
    }

    // ── 2. Interpolation ──────────────────────────────────────────
    println!("\n--- Interpolation ---");
    let methods = [
        ("Linear", Interpolator::Linear),
        ("Nearest", Interpolator::Nearest),
        ("CatmullRom", Interpolator::CatmullRom),
        ("Forward", Interpolator::Forward),
        ("Zero", Interpolator::Zero),
    ];

    for (name, method) in &methods {
        let (filled_ts, filled_vals, gaps_filled, _gaps_skipped) = method
            .fill(&timestamps, &values, interval_ns)
            .expect("interpolation failed");
        println!(
            "  {name:>12}: filled {gaps_filled} gaps → {} total points",
            filled_ts.len()
        );
        let _ = (filled_ts, filled_vals); // use bindings
    }

    // Use linear for subsequent steps
    let (full_ts, full_vals, _, _) = Interpolator::Linear
        .fill(&timestamps, &values, interval_ns)
        .expect("interpolation failed");

    // ── 3. Smoothing ──────────────────────────────────────────────
    println!("\n--- Smoothing ---");

    let ema = ExponentialSmoother::new(0.3);
    let ema_vals = ema.smooth(&full_vals);
    println!(
        "  EMA(α=0.3): first 5 = {:?}",
        &ema_vals[..5.min(ema_vals.len())]
    );

    let ma = MovingAverageSmoother::new(5);
    let ma_vals = ma.smooth(&full_vals);
    println!(
        "  MA(w=5):    first 5 = {:?}",
        &ma_vals[..5.min(ma_vals.len())]
    );

    let wma = WeightedMovingAverage::new(vec![0.1, 0.2, 0.3, 0.4]);
    let wma_vals = wma.smooth(&full_vals);
    println!(
        "  WMA:        first 5 = {:?}",
        &wma_vals[..5.min(wma_vals.len())]
    );

    // Enum-based smoother
    let smoother = Smoother::Exponential { alpha: 0.5 };
    let smoothed = smoother.smooth(&full_vals);
    println!(
        "  Enum-EMA:   first 5 = {:?}",
        &smoothed[..5.min(smoothed.len())]
    );

    // ── 4. Resampling ─────────────────────────────────────────────
    println!("\n--- Resampling ---");
    let resample_config = ResampleConfig {
        target_interval_ns: 5 * interval_ns, // downsample 5x
        aggregation: chronix::chronix_analytics::preprocess::AggregationFn::Mean,
        interpolation: Interpolator::Linear,
    };
    let (resampled_ts, resampled_vals) =
        Resampler::resample(&full_ts, &full_vals, &resample_config);
    println!(
        "  {} points → {} points (5x downsample)",
        full_ts.len(),
        resampled_ts.len()
    );
    println!(
        "  First 5 resampled: {:?}",
        &resampled_vals[..5.min(resampled_vals.len())]
    );

    // ── 5. STL Decomposition ──────────────────────────────────────
    println!("\n--- STL Decomposition ---");

    // Auto-detect period
    let period = detect_period(&full_vals, 50);
    println!("  Auto-detected period: {:?}", period);

    let stl_period = period.unwrap_or(24);
    let config = StlConfig::new(stl_period).with_iterations(3);
    let decomp = stl_decompose(&full_vals, &config).expect("STL decomposition failed");
    println!(
        "  Trend component:    first 5 = {:?}",
        &decomp.trend[..5.min(decomp.trend.len())]
    );
    println!(
        "  Seasonal component: first 5 = {:?}",
        &decomp.seasonal[..5.min(decomp.seasonal.len())]
    );
    println!(
        "  Residual component: first 5 = {:?}",
        &decomp.residual[..5.min(decomp.residual.len())]
    );

    // ── 6. Feature Engineering ────────────────────────────────────
    println!("\n--- Feature Engineering ---");

    let lagged = lag(&full_vals, 3);
    println!("  lag(3):      len={}", lagged.len());

    let diffed = diff(&full_vals, 1);
    println!(
        "  diff(1):     first 5 = {:?}",
        &diffed[..5.min(diffed.len())]
    );

    let pct = pct_change(&full_vals);
    println!("  pct_change:  first 5 = {:?}", &pct[..5.min(pct.len())]);

    // A rolling window has no value until it is full, so the first `w - 1`
    // entries are NaN by construction. Print from where the window closes —
    // five NaNs say nothing about the function.
    let w = 10;
    let rstd = rolling_std(&full_vals, w);
    let settled = &rstd[(w - 1).min(rstd.len())..];
    println!(
        "  rolling_std: {} NaN warm-up, then {:?}",
        w - 1,
        &settled[..5.min(settled.len())]
    );

    let ewm_vals = ewm(&full_vals, 0.3);
    println!(
        "  ewm(0.3):    first 5 = {:?}",
        &ewm_vals[..5.min(ewm_vals.len())]
    );

    // ── 7. Auto Feature Extraction ────────────────────────────────
    println!("\n--- Auto Feature Extraction ---");
    let feature_config = AutoFeatureConfig::default()
        .with_period(24)
        .with_windows(vec![5, 10, 20]);
    let feature_matrix = auto_features(&full_ts, &full_vals, &feature_config);
    println!(
        "  Generated {} features × {} rows",
        feature_matrix.n_features(),
        feature_matrix.n_rows
    );
    println!("  Feature names: {:?}", feature_matrix.names);

    // ── 8. Full Preprocessing Pipeline ────────────────────────────
    println!("\n--- Full Preprocessing Pipeline ---");
    let pipe_config = PreprocessConfig {
        expected_interval_ns: interval_ns,
        gap_tolerance: 1.5,
        interpolation: Some(Interpolator::Linear),
        smoothing: Some(Smoother::MovingAverage { window: 5 }),
        resample: None,
        ..Default::default()
    };

    let result =
        PreprocessPipeline::run(&pipe_config, &timestamps, &values).expect("Pipeline failed");
    println!(
        "  Pipeline output: {} points, {} gaps filled",
        result.values.len(),
        result.gaps_filled
    );
    println!(
        "  First 5 values: {:?}",
        &result.values[..5.min(result.values.len())]
    );

    println!("\n✓ Preprocessing complete");
}
