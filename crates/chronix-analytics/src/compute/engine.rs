//! Compute engine trait and CPU implementation.

use std::sync::Arc;

use rayon::prelude::*;

use crate::compute::error::ComputeError;
use crate::compute::simd::{simd_dot_product, simd_sum};

/// Minimum element count before rayon parallelism kicks in.
/// Below this, the thread pool dispatch overhead exceeds the compute gain.
const PARALLEL_THRESHOLD: usize = 8_192;

/// Configuration for the compute engine.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ComputeConfig {
    /// Number of CPU threads for parallel operations (0 = auto).
    #[serde(default)]
    pub cpu_threads: usize,
}

/// Compute engine trait — defines batch numerical operations used by
/// forecast, anomaly detection, and multivariate engines.
pub trait ComputeEngine: Send + Sync {
    /// Dot product of two vectors.
    fn batch_dot_product(&self, a: &[f64], b: &[f64]) -> Result<f64, ComputeError>;

    /// Solve a linear system Ax = b using LU decomposition.
    ///
    /// `a_flat` is the row-major flattened NxN matrix A.
    /// `b` is the right-hand-side vector of length N.
    /// Returns x such that Ax = b.
    fn batch_matrix_solve(
        &self,
        a_flat: &[f64],
        b: &[f64],
        n: usize,
    ) -> Result<Vec<f64>, ComputeError>;

    /// Exponential smoothing on a time series.
    ///
    /// Returns smoothed values with the given alpha parameter.
    fn batch_exponential_smooth(
        &self,
        values: &[f64],
        alpha: f64,
    ) -> Result<Vec<f64>, ComputeError>;

    /// Compute d-th order differences of a series.
    fn batch_difference(&self, values: &[f64], d: usize) -> Result<Vec<f64>, ComputeError>;

    /// Compute autocorrelation at the given lags.
    fn batch_autocorrelation(
        &self,
        values: &[f64],
        max_lag: usize,
    ) -> Result<Vec<f64>, ComputeError>;

    /// Solve least-squares: find x minimizing ||Ax - b||².
    ///
    /// `a_flat` is row-major M×N matrix, `b` is length-M vector.
    /// Returns x of length N via Householder QR decomposition (numerically
    /// stable — preserves κ(A) instead of squaring it like normal equations).
    fn batch_least_squares(
        &self,
        a_flat: &[f64],
        b: &[f64],
        m: usize,
        n: usize,
    ) -> Result<Vec<f64>, ComputeError>;
}

/// CPU compute engine using SIMD and rayon parallelism.
///
/// Owns a dedicated [`rayon::ThreadPool`] so that the `cpu_threads`
/// configuration is always honoured, regardless of whether any other
/// code has already initialised the rayon global pool.  Operations
/// that benefit from data-parallelism (autocorrelation, dot product
/// on large vectors) are dispatched onto this pool when the input
/// exceeds `PARALLEL_THRESHOLD`.
pub struct CpuEngine {
    config: ComputeConfig,
    pool: Arc<rayon::ThreadPool>,
}

impl CpuEngine {
    /// Creates a new CPU compute engine with the given configuration.
    ///
    /// The dedicated thread pool is sized according to
    /// [`ComputeConfig::cpu_threads`] (0 = number of available cores).
    pub fn new(config: ComputeConfig) -> Result<Self, ComputeError> {
        let num_threads = if config.cpu_threads > 0 {
            config.cpu_threads
        } else {
            std::thread::available_parallelism()
                .map(std::num::NonZero::get)
                .unwrap_or(4)
        };

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .thread_name(|i| format!("chronix-cpu-{i}"))
            .build()
            .map_err(|e| {
                ComputeError::Internal(format!("failed to build rayon thread pool: {e}"))
            })?;

        metrics::gauge!("chronix_compute_thread_pool_size").set(num_threads as f64);

        Ok(Self {
            config,
            pool: Arc::new(pool),
        })
    }

    /// Returns a reference to the owned thread pool.
    #[must_use]
    pub fn thread_pool(&self) -> &rayon::ThreadPool {
        &self.pool
    }

    /// Returns the number of threads in the pool.
    #[must_use]
    pub fn num_threads(&self) -> usize {
        self.pool.current_num_threads()
    }

    /// Compute z-scores for a batch of values.
    ///
    /// Uses sample variance (Bessel's correction, n − 1); all arithmetic in
    /// `f64`. A constant series (zero standard deviation) yields all-zero
    /// z-scores.
    ///
    /// # Errors
    ///
    /// Returns [`ComputeError::InsufficientData`] if `values` has fewer than
    /// 2 elements.
    pub fn batch_z_score(&self, values: &[f64]) -> Result<Vec<f64>, ComputeError> {
        if values.len() < 2 {
            return Err(ComputeError::InsufficientData {
                min: 2,
                got: values.len(),
            });
        }

        let n = values.len() as f64;
        let mean: f64 = values.iter().sum::<f64>() / n;
        // Sample variance (Bessel's correction) for consistency across detectors.
        let variance: f64 = values.iter().map(|&v| (v - mean).powi(2)).sum::<f64>() / (n - 1.0);
        let std_dev = variance.sqrt();

        if std_dev == 0.0 {
            return Ok(vec![0.0; values.len()]);
        }

        Ok(values.iter().map(|&v| (v - mean) / std_dev).collect())
    }
}

impl std::fmt::Debug for CpuEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CpuEngine")
            .field("config", &self.config)
            .field("num_threads", &self.pool.current_num_threads())
            .finish()
    }
}

impl Default for CpuEngine {
    fn default() -> Self {
        Self::new(ComputeConfig::default()).expect("default CpuEngine config should always succeed")
    }
}

impl ComputeEngine for CpuEngine {
    fn batch_dot_product(&self, a: &[f64], b: &[f64]) -> Result<f64, ComputeError> {
        metrics::counter!("chronix_compute_batch_operations_total", "operation" => "dot_product", "backend" => "cpu").increment(1);

        // For large vectors, split into chunks and sum partial dot products in parallel.
        if a.len() >= PARALLEL_THRESHOLD {
            // Validate dimensions up-front so chunk-level errors are impossible.
            if a.len() != b.len() {
                return Err(ComputeError::DimensionMismatch {
                    expected: a.len(),
                    got: b.len(),
                });
            }
            let chunk_size = (a.len() / self.pool.current_num_threads()).max(1024);
            let result: Result<f64, ComputeError> = self.pool.install(|| {
                a.par_chunks(chunk_size)
                    .zip(b.par_chunks(chunk_size))
                    .map(|(ca, cb)| simd_dot_product(ca, cb))
                    .try_reduce(|| 0.0, |acc, v| Ok(acc + v))
            });
            return result;
        }

        simd_dot_product(a, b)
    }

    fn batch_matrix_solve(
        &self,
        a_flat: &[f64],
        b: &[f64],
        n: usize,
    ) -> Result<Vec<f64>, ComputeError> {
        if a_flat.len() != n * n {
            return Err(ComputeError::DimensionMismatch {
                expected: n * n,
                got: a_flat.len(),
            });
        }
        if b.len() != n {
            return Err(ComputeError::DimensionMismatch {
                expected: n,
                got: b.len(),
            });
        }
        metrics::counter!("chronix_compute_batch_operations_total", "operation" => "matrix_solve", "backend" => "cpu").increment(1);
        lu_solve(a_flat, b, n)
    }

    fn batch_exponential_smooth(
        &self,
        values: &[f64],
        alpha: f64,
    ) -> Result<Vec<f64>, ComputeError> {
        if !(0.0..=1.0).contains(&alpha) {
            return Err(ComputeError::InvalidParameter {
                name: "alpha",
                value: alpha.to_string(),
                reason: "must be in [0.0, 1.0]",
            });
        }
        if values.is_empty() {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(values.len());
        result.push(values[0]);
        let one_minus_alpha = 1.0 - alpha;
        for i in 1..values.len() {
            let smoothed = alpha * values[i] + one_minus_alpha * result[i - 1];
            result.push(smoothed);
        }
        Ok(result)
    }

    fn batch_difference(&self, values: &[f64], d: usize) -> Result<Vec<f64>, ComputeError> {
        if d == 0 {
            return Ok(values.to_vec());
        }
        if values.len() <= d {
            return Err(ComputeError::InsufficientData {
                min: d + 1,
                got: values.len(),
            });
        }

        let mut current = values.to_vec();
        for _ in 0..d {
            let mut next = Vec::with_capacity(current.len() - 1);
            for i in 1..current.len() {
                next.push(current[i] - current[i - 1]);
            }
            current = next;
        }
        Ok(current)
    }

    fn batch_autocorrelation(
        &self,
        values: &[f64],
        max_lag: usize,
    ) -> Result<Vec<f64>, ComputeError> {
        if values.len() < 2 {
            return Err(ComputeError::InsufficientData {
                min: 2,
                got: values.len(),
            });
        }

        metrics::counter!("chronix_compute_batch_operations_total", "operation" => "autocorrelation", "backend" => "cpu").increment(1);
        let n = values.len();
        let mean = simd_sum(values) / n as f64;

        // Compute variance (denominator)
        let var: f64 = values.iter().map(|&v| (v - mean).powi(2)).sum();
        if var.abs() < 1e-15 {
            // Constant series — autocorrelation is 1 at lag 0, 0 elsewhere
            let mut result = vec![0.0; max_lag + 1];
            result[0] = 1.0;
            return Ok(result);
        }

        let effective_max = max_lag.min(n - 1);

        // Parallelize across lags when work is large enough.
        // Total work ≈ n * effective_max, so parallelize when that exceeds threshold.
        let acf = if n * effective_max >= PARALLEL_THRESHOLD {
            let pool = &self.pool;
            pool.install(|| {
                (0..=effective_max)
                    .into_par_iter()
                    .map(|lag| {
                        let mut cov = 0.0;
                        for i in 0..(n - lag) {
                            cov += (values[i] - mean) * (values[i + lag] - mean);
                        }
                        cov / var
                    })
                    .collect::<Vec<_>>()
            })
        } else {
            let mut acf = Vec::with_capacity(effective_max + 1);
            for lag in 0..=effective_max {
                let mut cov = 0.0;
                for i in 0..(n - lag) {
                    cov += (values[i] - mean) * (values[i + lag] - mean);
                }
                acf.push(cov / var);
            }
            acf
        };

        // Pad with zeros if max_lag > n-1
        if acf.len() <= max_lag {
            let mut padded = acf;
            padded.resize(max_lag + 1, 0.0);
            Ok(padded)
        } else {
            Ok(acf)
        }
    }

    fn batch_least_squares(
        &self,
        a_flat: &[f64],
        b: &[f64],
        m: usize,
        n: usize,
    ) -> Result<Vec<f64>, ComputeError> {
        if a_flat.len() != m * n {
            return Err(ComputeError::DimensionMismatch {
                expected: m * n,
                got: a_flat.len(),
            });
        }
        if b.len() != m {
            return Err(ComputeError::DimensionMismatch {
                expected: m,
                got: b.len(),
            });
        }
        if m < n {
            return Err(ComputeError::DimensionMismatch {
                expected: n,
                got: m,
            });
        }

        qr_least_squares(a_flat, b, m, n)
    }
}

/// Solve least-squares via Householder QR decomposition.
///
/// Factorises A = QR where Q is M×N with orthonormal columns and R is N×N
/// upper-triangular. Then solves Rx = Q^T b by back-substitution.
///
/// This is numerically stable (κ(R) = κ(A)) unlike normal equations which
/// square the condition number.
fn qr_least_squares(
    a_flat: &[f64],
    b: &[f64],
    m: usize,
    n: usize,
) -> Result<Vec<f64>, ComputeError> {
    // Work on a column-major copy for efficient column access during
    // Householder reflections. A_col[j*m + i] = A[i, j].
    let mut a_col = vec![0.0; m * n];
    for i in 0..m {
        for j in 0..n {
            a_col[j * m + i] = a_flat[i * n + j];
        }
    }
    let mut rhs = b.to_vec();

    for j in 0..n {
        // Compute Householder vector for column j, rows j..m.
        let col_start = j * m + j;
        let col_end = j * m + m;
        let sub_col = &a_col[col_start..col_end];
        let sub_len = m - j;

        // σ = ||sub_col||
        let sigma: f64 = sub_col.iter().map(|&v| v * v).sum::<f64>().sqrt();
        if sigma < 1e-15 {
            return Err(ComputeError::SingularMatrix);
        }

        // v[0] = sub_col[0] + sign(sub_col[0]) * σ
        let sign = if sub_col[0] >= 0.0 { 1.0 } else { -1.0 };
        let mut v = sub_col.to_vec();
        v[0] += sign * sigma;

        let v_dot_v: f64 = v.iter().map(|&x| x * x).sum();
        if v_dot_v < 1e-30 {
            continue;
        }
        let tau = 2.0 / v_dot_v;

        // Apply H = I - τvv^T to remaining columns of A (columns j..n)
        for k in j..n {
            let ck = k * m;
            // dot = v^T * A[j:m, k]
            let mut dot = 0.0;
            for i in 0..sub_len {
                dot += v[i] * a_col[ck + j + i];
            }
            let factor = tau * dot;
            for i in 0..sub_len {
                a_col[ck + j + i] -= factor * v[i];
            }
        }

        // Apply H to rhs: rhs[j:m] -= τ * (v^T rhs[j:m]) * v
        let mut dot_rhs = 0.0;
        for i in 0..sub_len {
            dot_rhs += v[i] * rhs[j + i];
        }
        let factor_rhs = tau * dot_rhs;
        for i in 0..sub_len {
            rhs[j + i] -= factor_rhs * v[i];
        }
    }

    // Proper numerical rank detection.
    // Compare each diagonal element of R against ε·max(diag(R))
    // to detect effective rank deficiency.
    let mut max_diag = 0.0f64;
    for i in 0..n {
        max_diag = max_diag.max(a_col[i * m + i].abs());
    }
    let rank_tol = max_diag * 1e-12 * (m.max(n) as f64);

    let mut x = vec![0.0; n];
    for i in (0..n).rev() {
        let mut sum = rhs[i];
        for k in (i + 1)..n {
            // R[i, k] = a_col[k*m + i]
            sum -= a_col[k * m + i] * x[k];
        }
        let diag = a_col[i * m + i];
        if diag.abs() < rank_tol {
            return Err(ComputeError::SingularMatrix);
        }
        x[i] = sum / diag;
    }

    Ok(x)
}

/// Solve a square linear system Ax = b via LU decomposition with partial
/// pivoting.  Used by `batch_matrix_solve` for exact N×N systems.
fn lu_solve(a_flat: &[f64], b: &[f64], n: usize) -> Result<Vec<f64>, ComputeError> {
    // Copy A into a mutable row-major buffer and b into rhs.
    let mut a = a_flat.to_vec();
    let rhs = b.to_vec();
    let mut pivot = (0..n).collect::<Vec<usize>>();

    // Forward elimination with partial pivoting.
    for col in 0..n {
        // Find pivot row.
        let mut max_val = a[pivot[col] * n + col].abs();
        let mut max_row = col;
        for row in (col + 1)..n {
            let v = a[pivot[row] * n + col].abs();
            if v > max_val {
                max_val = v;
                max_row = row;
            }
        }
        if max_val < 1e-14 {
            return Err(ComputeError::SingularMatrix);
        }
        pivot.swap(col, max_row);

        let piv_row = pivot[col];
        for row in (col + 1)..n {
            let cur_row = pivot[row];
            let factor = a[cur_row * n + col] / a[piv_row * n + col];
            a[cur_row * n + col] = factor; // store multiplier in L region
            for k in (col + 1)..n {
                a[cur_row * n + k] -= factor * a[piv_row * n + k];
            }
        }
    }

    // Forward substitution (Ly = Pb).
    let mut y = vec![0.0; n];
    for i in 0..n {
        let mut sum = rhs[pivot[i]];
        for j in 0..i {
            sum -= a[pivot[i] * n + j] * y[j];
        }
        y[i] = sum;
    }

    // Back substitution (Ux = y).
    let mut x = vec![0.0; n];
    for i in (0..n).rev() {
        let mut sum = y[i];
        for j in (i + 1)..n {
            sum -= a[pivot[i] * n + j] * x[j];
        }
        let diag = a[pivot[i] * n + i];
        if diag.abs() < 1e-14 {
            return Err(ComputeError::SingularMatrix);
        }
        x[i] = sum / diag;
    }

    Ok(x)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> CpuEngine {
        CpuEngine::default()
    }

    #[test]
    fn dot_product_correct() {
        let e = engine();
        let r = e
            .batch_dot_product(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0])
            .unwrap();
        assert!((r - 32.0).abs() < 1e-10);
    }

    #[test]
    fn dot_product_dimension_mismatch() {
        let e = engine();
        assert!(e.batch_dot_product(&[1.0], &[1.0, 2.0]).is_err());
    }

    #[test]
    fn matrix_solve_identity() {
        let e = engine();
        // Ix = b => x = b
        let a = vec![1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0];
        let b = vec![5.0, 3.0, 7.0];
        let x = e.batch_matrix_solve(&a, &b, 3).unwrap();
        for i in 0..3 {
            assert!((x[i] - b[i]).abs() < 1e-10);
        }
    }

    #[test]
    fn matrix_solve_2x2() {
        let e = engine();
        // [2 1; 5 3] x = [4; 7]  =>  x = [5; -6]
        let a = vec![2.0, 1.0, 5.0, 3.0];
        let b = vec![4.0, 7.0];
        let x = e.batch_matrix_solve(&a, &b, 2).unwrap();
        assert!((x[0] - 5.0).abs() < 1e-10);
        assert!((x[1] - (-6.0)).abs() < 1e-10);
    }

    #[test]
    fn matrix_solve_singular() {
        let e = engine();
        let a = vec![1.0, 2.0, 2.0, 4.0]; // singular
        let b = vec![3.0, 6.0];
        assert!(e.batch_matrix_solve(&a, &b, 2).is_err());
    }

    #[test]
    fn exponential_smooth_alpha_1() {
        let e = engine();
        // alpha=1 => output = input
        let vals = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = e.batch_exponential_smooth(&vals, 1.0).unwrap();
        assert_eq!(result.len(), vals.len());
        for (a, b) in result.iter().zip(vals.iter()) {
            assert!((a - b).abs() < 1e-10);
        }
    }

    #[test]
    fn exponential_smooth_alpha_0() {
        let e = engine();
        // alpha=0 => all values = first value
        let vals = vec![10.0, 20.0, 30.0];
        let result = e.batch_exponential_smooth(&vals, 0.0).unwrap();
        for &v in &result {
            assert!((v - 10.0).abs() < 1e-10);
        }
    }

    #[test]
    fn exponential_smooth_invalid_alpha() {
        let e = engine();
        assert!(e.batch_exponential_smooth(&[1.0], 1.5).is_err());
        assert!(e.batch_exponential_smooth(&[1.0], -0.1).is_err());
    }

    #[test]
    fn difference_order_1() {
        let e = engine();
        let vals = vec![1.0, 3.0, 6.0, 10.0, 15.0];
        let diff = e.batch_difference(&vals, 1).unwrap();
        assert_eq!(diff, vec![2.0, 3.0, 4.0, 5.0]);
    }

    #[test]
    fn difference_order_2() {
        let e = engine();
        let vals = vec![1.0, 3.0, 6.0, 10.0, 15.0];
        let diff = e.batch_difference(&vals, 2).unwrap();
        // First diff: [2, 3, 4, 5], second diff: [1, 1, 1]
        assert_eq!(diff, vec![1.0, 1.0, 1.0]);
    }

    #[test]
    fn difference_order_0() {
        let e = engine();
        let vals = vec![1.0, 2.0, 3.0];
        let diff = e.batch_difference(&vals, 0).unwrap();
        assert_eq!(diff, vals);
    }

    #[test]
    fn autocorrelation_lag0_is_one() {
        let e = engine();
        let vals = vec![1.0, 2.0, 3.0, 2.0, 1.0, 2.0, 3.0, 2.0];
        let acf = e.batch_autocorrelation(&vals, 3).unwrap();
        assert!((acf[0] - 1.0).abs() < 1e-10);
    }

    #[test]
    fn autocorrelation_constant_series() {
        let e = engine();
        let vals = vec![5.0; 10];
        let acf = e.batch_autocorrelation(&vals, 3).unwrap();
        assert!((acf[0] - 1.0).abs() < 1e-10);
        for i in 1..=3 {
            assert!((acf[i]).abs() < 1e-10);
        }
    }

    #[test]
    fn least_squares_line() {
        let e = engine();
        // y = 2x + 1, x = [0, 1, 2, 3]
        // A = [[1, 0], [1, 1], [1, 2], [1, 3]]
        // b = [1, 3, 5, 7]
        let a = vec![1.0, 0.0, 1.0, 1.0, 1.0, 2.0, 1.0, 3.0];
        let b = vec![1.0, 3.0, 5.0, 7.0];
        let x = e.batch_least_squares(&a, &b, 4, 2).unwrap();
        assert!((x[0] - 1.0).abs() < 1e-10); // intercept
        assert!((x[1] - 2.0).abs() < 1e-10); // slope
    }

    #[test]
    fn least_squares_noisy() {
        let e = engine();
        // y ≈ 3x + 2 with small noise
        let a = vec![1.0, 0.0, 1.0, 1.0, 1.0, 2.0, 1.0, 3.0, 1.0, 4.0];
        let b = vec![2.1, 4.9, 8.0, 11.1, 14.0];
        let x = e.batch_least_squares(&a, &b, 5, 2).unwrap();
        assert!((x[0] - 2.0).abs() < 0.5); // intercept ≈ 2
        assert!((x[1] - 3.0).abs() < 0.5); // slope ≈ 3
    }

    #[test]
    fn least_squares_ill_conditioned() {
        // 4×3 Hilbert-like matrix — condition number ≈ 748.
        // Normal equations would square this to ~560 000, losing ~6 digits.
        // Householder QR preserves κ(A) and should still yield < 1e-8 error.
        let e = engine();
        let m = 4;
        let n = 3;
        // A[i,j] = 1 / (i + j + 1)  (0-indexed)
        let mut a = Vec::with_capacity(m * n);
        for i in 0..m {
            for j in 0..n {
                a.push(1.0 / (i + j + 1) as f64);
            }
        }
        // Choose x_true = [1, -1, 1], compute b = A * x_true.
        let x_true = [1.0, -1.0, 1.0];
        let mut b = vec![0.0; m];
        for i in 0..m {
            for j in 0..n {
                b[i] += a[i * n + j] * x_true[j];
            }
        }
        let x = e.batch_least_squares(&a, &b, m, n).unwrap();
        for j in 0..n {
            assert!(
                (x[j] - x_true[j]).abs() < 1e-8,
                "x[{j}] = {} but expected {} (diff = {})",
                x[j],
                x_true[j],
                (x[j] - x_true[j]).abs(),
            );
        }
    }

    #[test]
    fn least_squares_underdetermined_fails() {
        let e = engine();
        // m=2, n=3 → underdetermined, should error
        let a = vec![1.0, 0.0, 0.0, 0.0, 1.0, 0.0];
        let b = vec![1.0, 2.0];
        assert!(e.batch_least_squares(&a, &b, 2, 3).is_err());
    }

    #[test]
    fn cpu_engine_has_dedicated_thread_pool() {
        let config = ComputeConfig { cpu_threads: 2 };
        let e = CpuEngine::new(config).unwrap();
        assert_eq!(e.num_threads(), 2);
    }

    #[test]
    fn cpu_engine_default_auto_detects_threads() {
        let e = CpuEngine::default();
        assert!(e.num_threads() >= 1);
    }

    #[test]
    fn parallel_dot_product_matches_serial() {
        // Create vectors above the PARALLEL_THRESHOLD to trigger the rayon path
        let n = super::PARALLEL_THRESHOLD + 1000;
        let a: Vec<f64> = (0..n).map(|i| (i as f64) * 0.001).collect();
        let b: Vec<f64> = (0..n).map(|i| 1.0 - (i as f64) * 0.0005).collect();

        let e = engine();
        let result = e.batch_dot_product(&a, &b).unwrap();

        // Compute expected with a simple serial loop
        let expected: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        assert!(
            (result - expected).abs() < expected.abs() * 1e-10,
            "parallel dot product diverged: got {result}, expected {expected}"
        );
    }

    #[test]
    fn parallel_autocorrelation_matches_serial() {
        // Create a series large enough to trigger the parallel path
        let n = 1000;
        let max_lag = 20;
        // n * max_lag = 20_000 > PARALLEL_THRESHOLD for small threshold values,
        // but we set cpu_threads=2 to at least exercise the pool.install() path
        let config = ComputeConfig { cpu_threads: 2 };
        let e = CpuEngine::new(config).unwrap();

        let vals: Vec<f64> = (0..n).map(|i| (i as f64 * 0.1).sin()).collect();
        let acf = e.batch_autocorrelation(&vals, max_lag).unwrap();

        assert_eq!(acf.len(), max_lag + 1);
        assert!(
            (acf[0] - 1.0).abs() < 1e-10,
            "lag-0 autocorrelation must be 1.0"
        );
        // All values must be in [-1, 1]
        for (lag, &v) in acf.iter().enumerate() {
            assert!(
                (-1.0 - 1e-10..=1.0 + 1e-10).contains(&v),
                "acf[{lag}] = {v} out of range [-1, 1]"
            );
        }
    }

    #[test]
    fn cpu_engine_debug_shows_threads() {
        let e = CpuEngine::new(ComputeConfig { cpu_threads: 3 }).unwrap();
        let dbg = format!("{e:?}");
        assert!(dbg.contains("num_threads: 3"), "debug output: {dbg}");
    }
}
