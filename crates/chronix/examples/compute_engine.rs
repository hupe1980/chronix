#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Compute Engine
//!
//! Demonstrates SIMD-accelerated computation, hardware tier detection,
//! batch operations, and buffer pool management.
//!
//! ```bash
//! cargo run --example compute_engine
//! ```

use chronix::chronix_analytics::compute::{
    simd_dot_product, simd_mean, simd_min_max, simd_std_dev, simd_sum, simd_tier, simd_variance,
    BufferPool, ComputeEngine, CpuEngine,
};

fn main() {
    println!("=== Chronix Compute Engine ===\n");

    // ── 1. SIMD Hardware Tier ─────────────────────────────────────
    println!("--- SIMD Hardware Tier ---");
    let tier = simd_tier();
    println!("Active SIMD tier: {tier}\n");

    // ── 2. SIMD-Accelerated Statistics ────────────────────────────
    println!("--- SIMD Statistics ---");

    // Generate a dataset
    let data: Vec<f64> = (0..10_000)
        .map(|i| (i as f64 * 0.01).sin() * 100.0 + 50.0)
        .collect();

    let sum = simd_sum(&data);
    let mean = simd_mean(&data);
    let variance = simd_variance(&data, mean);
    let std_dev = simd_std_dev(&data);
    let (min, max) = simd_min_max(&data);

    println!("  Dataset: {} points", data.len());
    println!("  Sum:      {sum:.4}");
    println!("  Mean:     {mean:.4}");
    println!("  Variance: {variance:.4}");
    println!("  Std Dev:  {std_dev:.4}");
    println!("  Min:      {min:.4}");
    println!("  Max:      {max:.4}");

    // ── 3. SIMD Dot Product ───────────────────────────────────────
    println!("\n--- SIMD Dot Product ---");
    let a: Vec<f64> = (0..1000).map(|i| i as f64 * 0.1).collect();
    let b: Vec<f64> = (0..1000).map(|i| (i as f64 * 0.05).cos()).collect();

    let dot = simd_dot_product(&a, &b).expect("dot product failed");
    println!("  dot(a, b) over 1000 elements = {dot:.4}");

    // ── 4. CpuEngine Batch Operations ─────────────────────────────
    println!("\n--- CpuEngine Batch Operations ---");
    let engine = CpuEngine::default();

    // Batch dot product
    let batch_dot = engine
        .batch_dot_product(&a, &b)
        .expect("batch_dot_product failed");
    println!("  batch_dot_product: {batch_dot:.4}");

    // Batch exponential smoothing
    let values: Vec<f64> = (0..100)
        .map(|i| 50.0 + (i as f64 * 0.1).sin() * 20.0)
        .collect();
    let smoothed = engine
        .batch_exponential_smooth(&values, 0.3)
        .expect("batch_exponential_smooth failed");
    println!(
        "  batch_exponential_smooth(α=0.3): first 5 = {:?}",
        &smoothed[..5]
    );

    // Batch differencing
    let diffed = engine
        .batch_difference(&values, 1)
        .expect("batch_difference failed");
    println!(
        "  batch_difference(d=1): first 5 = {:?}",
        &diffed[..5.min(diffed.len())]
    );

    // Batch autocorrelation
    let acf = engine
        .batch_autocorrelation(&values, 10)
        .expect("batch_autocorrelation failed");
    println!("  batch_autocorrelation(max_lag=10):");
    for (lag, &corr) in acf.iter().enumerate() {
        println!("    lag {lag:>2}: {corr:.4}");
    }

    // Batch least squares: fit y = a*x + b
    // A = [[x1, 1], [x2, 1], ...]
    let m = 50;
    let n = 2;
    let x_vals: Vec<f64> = (0..m).map(|i| i as f64).collect();
    let y_vals: Vec<f64> = x_vals.iter().map(|&x| 2.5 * x + 10.0 + 0.1).collect();
    let a_flat: Vec<f64> = x_vals.iter().flat_map(|&x| vec![x, 1.0]).collect();
    let coeffs = engine
        .batch_least_squares(&a_flat, &y_vals, m, n)
        .expect("batch_least_squares failed");
    println!(
        "\n  batch_least_squares (y=ax+b): a={:.4}, b={:.4} (expected: ~2.5, ~10.1)",
        coeffs[0], coeffs[1]
    );

    // Batch matrix solve: solve Ax = b for a 3x3 system
    let a_matrix: Vec<f64> = vec![
        2.0, 1.0, -1.0, // row 1
        -3.0, -1.0, 2.0, // row 2
        -2.0, 1.0, 2.0, // row 3
    ];
    let b_vec = vec![8.0, -11.0, -3.0];
    let solution = engine
        .batch_matrix_solve(&a_matrix, &b_vec, 3)
        .expect("batch_matrix_solve failed");
    println!(
        "  batch_matrix_solve (3×3): x={:.2}, y={:.2}, z={:.2} (expected: 2, 3, -1)",
        solution[0], solution[1], solution[2]
    );

    // ── 5. Buffer Pool ────────────────────────────────────────────
    println!("\n--- Buffer Pool ---");
    let pool: BufferPool<f64> = BufferPool::new(4);
    println!("  Pool created with max_size=4");

    // Get buffers, fill them, return them
    let mut buf1 = pool.get(100);
    buf1.extend_from_slice(&[1.0, 2.0, 3.0]);
    println!("  Got buffer with capacity >= 100, filled with 3 elements");

    let mut buf2 = pool.get(200);
    buf2.extend_from_slice(&[4.0, 5.0]);
    println!("  Got buffer with capacity >= 200, filled with 2 elements");

    println!("  Available in pool before return: {}", pool.available());
    pool.put(buf1);
    pool.put(buf2);
    println!("  Available in pool after return:  {}", pool.available());

    // Reuse a pooled buffer
    let reused = pool.get(50);
    println!(
        "  Reused buffer capacity: {} (may be larger than requested 50)",
        reused.capacity()
    );
    println!("  Available after reuse: {}", pool.available());

    println!("\n✓ Compute engine showcase complete");
}
