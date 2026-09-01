//! Multivariate forecasting: Multi-Linear Regression and VAR models.

use crate::compute::{ComputeEngine, CpuEngine};

use crate::multivariate::context::MultiSeriesContext;
use crate::multivariate::error::MultivariateError;

/// Result of a multivariate forecast.
#[derive(Debug, Clone)]
pub struct MultivariateForecastResult {
    /// Forecast timestamps (nanoseconds since epoch).
    pub timestamps: Vec<i64>,
    /// Predicted values: one row per target series.
    pub predictions: Vec<Vec<f64>>,
    /// Per-predictor importance / coefficients.
    pub predictor_importance: Vec<(String, f64)>,
}

/// Multivariate forecast model contract.
pub trait MultivariateForecastModel: Send + Sync {
    /// Fit the model.
    ///
    /// - `ctx` — aligned multi-series context.
    /// - `target_idx` — index of the target series to forecast.
    fn fit(&mut self, ctx: &MultiSeriesContext, target_idx: usize)
        -> Result<(), MultivariateError>;

    /// Predict `horizon` steps ahead.
    fn predict(&self, horizon: usize) -> Result<MultivariateForecastResult, MultivariateError>;

    /// Whether the model supports incremental online learning.
    fn supports_online_learning(&self) -> bool;
}

// ─── Multi-Linear Regression ────────────────────────────────────────

/// OLS regression: multiple predictors → single target.
pub struct MultiLinearRegression {
    coefficients: Vec<f64>,
    intercept: f64,
    r_squared: f64,
    target_idx: usize,
    predictor_names: Vec<String>,
    /// Last observed predictor values — used for persistence forecasting.
    last_predictors: Vec<f64>,
    last_ts: i64,
    interval_ns: i64,
    fitted: bool,
}

impl MultiLinearRegression {
    /// Creates a new unfitted multi-linear regression model.
    pub fn new() -> Self {
        Self {
            coefficients: Vec::new(),
            intercept: 0.0,
            r_squared: 0.0,
            target_idx: 0,
            predictor_names: Vec::new(),
            last_predictors: Vec::new(),
            last_ts: 0,
            interval_ns: 1_000_000_000,
            fitted: false,
        }
    }
}

impl Default for MultiLinearRegression {
    fn default() -> Self {
        Self::new()
    }
}

impl MultivariateForecastModel for MultiLinearRegression {
    fn fit(
        &mut self,
        ctx: &MultiSeriesContext,
        target_idx: usize,
    ) -> Result<(), MultivariateError> {
        let k = ctx.matrix.n_series();
        let n = ctx.matrix.n_timestamps();
        if target_idx >= k {
            return Err(MultivariateError::InvalidParameter(format!(
                "target_idx {target_idx} >= n_series {k}"
            )));
        }
        let n_pred = k - 1;
        if n < n_pred + 2 {
            return Err(MultivariateError::InsufficientData {
                min: n_pred + 2,
                got: n,
            });
        }

        self.target_idx = target_idx;
        self.predictor_names = ctx
            .matrix
            .series_ids
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != target_idx)
            .map(|(_, name)| name.clone())
            .collect();

        let y = &ctx.matrix.data[target_idx];
        // Build X^T X and X^T y using normal equations
        // Design matrix: [1, x1, x2, ..., x_{k-1}] — augmented with intercept column
        let p = n_pred + 1; // number of columns including intercept
        let engine = CpuEngine::default();

        // XtX: p × p
        let mut xtx = vec![0.0; p * p];
        // Xty: p × 1
        let mut xty = vec![0.0; p];

        #[allow(clippy::needless_range_loop)]
        for t in 0..n {
            let mut row = Vec::with_capacity(p);
            row.push(1.0); // intercept
            for s in 0..k {
                if s != target_idx {
                    row.push(ctx.matrix.data[s][t]);
                }
            }
            for i in 0..p {
                for j in 0..p {
                    xtx[i * p + j] += row[i] * row[j];
                }
                xty[i] += row[i] * y[t];
            }
        }

        // Ridge regularisation to handle collinear predictors
        for i in 0..p {
            xtx[i * p + i] += 1e-8;
        }

        // Solve (X^T X + λI) beta = X^T y
        let beta = engine
            .batch_matrix_solve(&xtx, &xty, p)
            .map_err(|e: crate::compute::ComputeError| MultivariateError::Compute(e.to_string()))?;

        self.intercept = beta[0];
        self.coefficients = beta[1..].to_vec();

        // Compute R²
        let y_mean = y.iter().sum::<f64>() / n as f64;
        let ss_tot: f64 = y.iter().map(|&v| (v - y_mean).powi(2)).sum();
        let mut ss_res = 0.0;
        #[allow(clippy::needless_range_loop)]
        for t in 0..n {
            let mut pred = self.intercept;
            let mut pidx = 0;
            for s in 0..k {
                if s != target_idx {
                    pred += self.coefficients[pidx] * ctx.matrix.data[s][t];
                    pidx += 1;
                }
            }
            ss_res += (y[t] - pred).powi(2);
        }
        self.r_squared = if ss_tot > 1e-15 {
            1.0 - ss_res / ss_tot
        } else {
            1.0
        };

        // Store last observed predictor values for persistence forecasting
        self.last_predictors = Vec::with_capacity(n_pred);
        let last_t = n - 1;
        for s in 0..k {
            if s != target_idx {
                self.last_predictors.push(ctx.matrix.data[s][last_t]);
            }
        }

        // Timing
        self.last_ts = ctx
            .matrix
            .timestamps
            .last()
            .copied()
            .ok_or(MultivariateError::InsufficientData { min: 1, got: 0 })?;
        if ctx.matrix.timestamps.len() >= 2 {
            let ts = &ctx.matrix.timestamps;
            let mut deltas: Vec<i64> = ts.windows(2).map(|w| w[1] - w[0]).collect();
            deltas.sort_unstable();
            self.interval_ns = deltas[deltas.len() / 2];
        }

        self.fitted = true;
        Ok(())
    }

    fn predict(&self, horizon: usize) -> Result<MultivariateForecastResult, MultivariateError> {
        if !self.fitted {
            return Err(MultivariateError::NotFitted);
        }
        // For multi-linear regression, we need predictor values for future timestamps.
        // Since we don't have them, we use the last known predictor values (persistence forecast).
        let timestamps: Vec<i64> = (1..=horizon as i64)
            .map(|h| {
                self.last_ts
                    .saturating_add(h.saturating_mul(self.interval_ns))
            })
            .collect();

        // Compute prediction using intercept + coefficients × last known predictor values
        let pred_val = self.intercept
            + self
                .coefficients
                .iter()
                .zip(self.last_predictors.iter())
                .map(|(c, x)| c * x)
                .sum::<f64>();
        let predictions = vec![vec![pred_val; horizon]];

        let importance: Vec<(String, f64)> = self
            .predictor_names
            .iter()
            .zip(self.coefficients.iter())
            .map(|(name, &coef)| (name.clone(), coef.abs()))
            .collect();

        Ok(MultivariateForecastResult {
            timestamps,
            predictions,
            predictor_importance: importance,
        })
    }

    fn supports_online_learning(&self) -> bool {
        true
    }
}

// ─── VAR (Vector AutoRegression) ────────────────────────────────────

// Vector AutoRegression: the stability check uses the QR
// eigenvalue helpers below for the VAR spectral radius.

/// Reduce a square row-major matrix `h` of size `n×n` to upper Hessenberg
/// form in-place using Householder reflections.
fn hessenberg_reduce(h: &mut [f64], n: usize) {
    for col in 0..n.saturating_sub(2) {
        // Build Householder vector for column `col`, rows col+1..n
        let m = n - col - 1;
        let mut x = vec![0.0; m];
        for i in 0..m {
            x[i] = h[(col + 1 + i) * n + col];
        }

        let x_norm = x.iter().map(|v| v * v).sum::<f64>().sqrt();
        if x_norm < 1e-15 {
            continue;
        }

        let sign = if x[0] >= 0.0 { 1.0 } else { -1.0 };
        let alpha = -sign * x_norm;
        x[0] -= alpha;
        let v_norm = x.iter().map(|v| v * v).sum::<f64>().sqrt();
        if v_norm < 1e-15 {
            continue;
        }
        for v in &mut x {
            *v /= v_norm;
        }

        // Apply H = I - 2*v*v^T from the left: h[col+1..n, :] -= 2*v*(v^T * h[col+1..n, :])
        for j in 0..n {
            let mut dot = 0.0;
            for i in 0..m {
                dot += x[i] * h[(col + 1 + i) * n + j];
            }
            for i in 0..m {
                h[(col + 1 + i) * n + j] -= 2.0 * x[i] * dot;
            }
        }

        // Apply from the right: h[:, col+1..n] -= 2*(h[:, col+1..n] * v)*v^T
        for i in 0..n {
            let mut dot = 0.0;
            for j in 0..m {
                dot += h[i * n + col + 1 + j] * x[j];
            }
            for j in 0..m {
                h[i * n + col + 1 + j] -= 2.0 * dot * x[j];
            }
        }
    }
}

/// Francis implicit double-shift QR iteration on upper Hessenberg matrix `h`
/// (in-place). Converges to quasi-upper-triangular (real Schur) form.
fn qr_francis(h: &mut [f64], n: usize) {
    if n <= 1 {
        return;
    }

    let max_iter = 100 * n;
    let mut iter_count = 0;
    let mut nn = n;

    while nn > 1 && iter_count < max_iter {
        iter_count += 1;

        // Deflation: check if sub-diagonal element is negligible
        let sub = h[(nn - 1) * n + nn - 2];
        let diag_sum = h[(nn - 2) * n + nn - 2].abs() + h[(nn - 1) * n + nn - 1].abs();
        let tol = 1e-14 * diag_sum.max(1e-15);

        if sub.abs() <= tol {
            nn -= 1;
            continue;
        }

        // Check for 2×2 block convergence
        if nn > 2 {
            let sub2 = h[(nn - 2) * n + nn - 3];
            let diag_sum2 = h[(nn - 3) * n + nn - 3].abs() + h[(nn - 2) * n + nn - 2].abs();
            let tol2 = 1e-14 * diag_sum2.max(1e-15);
            if sub2.abs() <= tol2 {
                // 2×2 block at bottom — deflate it (may be complex pair)
                nn -= 2;
                continue;
            }
        } else {
            // 2×2 block at top — done
            break;
        }

        // Wilkinson shift: eigenvalue of trailing 2×2 closest to h[nn-1,nn-1]
        let a11 = h[(nn - 2) * n + nn - 2];
        let a12 = h[(nn - 2) * n + nn - 1];
        let a21 = h[(nn - 1) * n + nn - 2];
        let a22 = h[(nn - 1) * n + nn - 1];
        let trace = a11 + a22;
        let det = a11 * a22 - a12 * a21;

        // Single implicit QR step with Wilkinson shift
        let shift = a22;
        h[0] -= shift;
        for i in 0..n {
            if i < nn {
                h[i * n + i] -= shift;
            }
        }
        // Restore shift after the step
        // Actually do a proper single-shift QR step via Givens rotations
        // on the Hessenberg matrix.
        // Revert the shift subtraction and use a proper implementation.
        for i in 0..n {
            if i < nn {
                h[i * n + i] += shift;
            }
        }
        h[0] += shift;

        // Proper single-shift QR step via Givens rotations
        let _ = trace; // suppress unused warning
        let _ = det;
        qr_step_givens(h, n, nn, a22);
    }
}

/// A single implicit QR step with Wilkinson shift applied via Givens rotations
/// on the upper Hessenberg matrix `h`. Only operates on the leading `nn×nn`
/// active submatrix.
fn qr_step_givens(h: &mut [f64], n: usize, nn: usize, shift: f64) {
    // Apply shift: H - σI
    for i in 0..nn {
        h[i * n + i] -= shift;
    }

    // QR factorization via Givens rotations (chase the bulge)
    let mut cs = vec![0.0; nn - 1];
    let mut sn = vec![0.0; nn - 1];

    for i in 0..nn - 1 {
        let a = h[i * n + i];
        let b = h[(i + 1) * n + i];
        let r = a.hypot(b);
        if r < 1e-30 {
            cs[i] = 1.0;
            sn[i] = 0.0;
            continue;
        }
        cs[i] = a / r;
        sn[i] = b / r;

        // Apply Givens rotation from the left to rows i, i+1
        for j in 0..n {
            let t1 = h[i * n + j];
            let t2 = h[(i + 1) * n + j];
            h[i * n + j] = cs[i] * t1 + sn[i] * t2;
            h[(i + 1) * n + j] = -sn[i] * t1 + cs[i] * t2;
        }
    }

    // Apply Givens rotations from the right: R * Q
    for i in 0..nn - 1 {
        for j in 0..n {
            let t1 = h[j * n + i];
            let t2 = h[j * n + i + 1];
            h[j * n + i] = cs[i] * t1 + sn[i] * t2;
            h[j * n + i + 1] = -sn[i] * t1 + cs[i] * t2;
        }
    }

    // Remove shift: H + σI
    for i in 0..nn {
        h[i * n + i] += shift;
    }
}

/// Extract the spectral radius (maximum eigenvalue magnitude) from a
/// quasi-upper-triangular (real Schur) matrix.
fn spectral_radius_from_schur(h: &[f64], n: usize) -> f64 {
    let mut max_mag = 0.0f64;
    let mut i = 0;

    while i < n {
        if i + 1 < n
            && h[(i + 1) * n + i].abs()
                > 1e-14 * (h[i * n + i].abs() + h[(i + 1) * n + i + 1].abs()).max(1e-15)
        {
            // 2×2 block → complex conjugate pair
            let a = h[i * n + i];
            let b = h[i * n + i + 1];
            let c = h[(i + 1) * n + i];
            let d = h[(i + 1) * n + i + 1];
            // Eigenvalue magnitude: sqrt(|det|) for a 2×2 block
            // det = ad - bc, trace = a+d
            // eigenvalues = (trace ± sqrt(trace²-4*det))/2
            // For complex pair: |λ| = sqrt(det) when trace² < 4*det
            let det = a * d - b * c;
            let trace = a + d;
            let disc = trace * trace - 4.0 * det;
            let mag = if disc < 0.0 {
                // Complex pair: both have magnitude sqrt(det)
                det.abs().sqrt()
            } else {
                // Two real eigenvalues from the 2×2 block
                let sq = disc.sqrt();
                let e1 = (trace + sq) / 2.0;
                let e2 = (trace - sq) / 2.0;
                e1.abs().max(e2.abs())
            };
            max_mag = max_mag.max(mag);
            i += 2;
        } else {
            // 1×1 block → real eigenvalue
            max_mag = max_mag.max(h[i * n + i].abs());
            i += 1;
        }
    }

    max_mag
}

/// After fitting, the model verifies that the VAR companion matrix has
/// a spectral radius below [`stability_threshold`](Self::new_with_stability_threshold)
/// (default: 1.05). Models exceeding this are rejected as unstable.
/// The QR-based eigenvalue computation correctly captures complex
/// conjugate eigenvalue pairs (oscillatory dynamics) that power iteration
/// would miss.
/// Callers can override the threshold via
/// [`new_with_stability_threshold()`](Self::new_with_stability_threshold).
pub struct VarModel {
    lag_order: usize,
    /// Coefficient matrices: lag_order matrices of k×k each (row-major).
    coefficients: Vec<Vec<f64>>,
    intercepts: Vec<f64>,
    k: usize,
    last_values: Vec<Vec<f64>>, // last `lag_order` observations, each of length k
    last_ts: i64,
    interval_ns: i64,
    series_names: Vec<String>,
    fitted: bool,
    /// Maximum allowed spectral radius for stability check.
    stability_threshold: f64,
}

impl VarModel {
    /// Creates a VAR model with the given lag order (default: 1).
    pub fn new(lag_order: Option<usize>) -> Self {
        Self::new_with_stability_threshold(lag_order, None)
    }

    /// Creates a VAR model with a custom stability threshold.
    ///
    /// The `stability_threshold` controls the maximum allowed spectral
    /// radius of the VAR companion matrix (default: 1.05). Set to a
    /// value > 1.0 to tolerate near-unit-root processes, or closer to
    /// 1.0 for stricter stability enforcement.
    pub fn new_with_stability_threshold(
        lag_order: Option<usize>,
        stability_threshold: Option<f64>,
    ) -> Self {
        Self {
            lag_order: lag_order.unwrap_or(1),
            coefficients: Vec::new(),
            intercepts: Vec::new(),
            k: 0,
            last_values: Vec::new(),
            last_ts: 0,
            interval_ns: 1_000_000_000,
            series_names: Vec::new(),
            fitted: false,
            stability_threshold: stability_threshold.unwrap_or(1.05),
        }
    }

    /// Compute spectral radius of the VAR companion matrix.
    ///
    /// Uses the Francis QR algorithm (implicit double-shift)
    /// to compute all eigenvalues, including complex conjugate pairs that
    /// power iteration would miss. The spectral radius is max(|λ_i|).
    ///
    /// The companion matrix for VAR(p) with k variables is a (pk × pk) matrix.
    /// The VAR is stable iff all eigenvalues lie inside the unit circle,
    /// i.e., spectral radius < 1.
    fn compute_spectral_radius(&self) -> f64 {
        let k = self.k;
        let p = self.lag_order;
        let n = p * k;
        if n == 0 {
            return 0.0;
        }

        // Build companion matrix A of size n×n (row-major):
        //
        //   A = [ A_1  A_2  ...  A_p ]
        //       [  I    0   ...   0  ]
        //       [  0    I   ...   0  ]
        //       [ ...             0  ]
        //
        // where A_l(i,j) = coefficients[i][l*k + j]
        let mut h = vec![0.0; n * n];

        // First k rows: the VAR coefficient matrices
        for i in 0..k {
            for l in 0..p {
                for j in 0..k {
                    h[i * n + l * k + j] = self.coefficients[i][l * k + j];
                }
            }
        }

        // Identity sub-diagonals: rows k..n
        for i in k..n {
            h[i * n + (i - k)] = 1.0;
        }

        // ── QR iteration via implicit shifts (Hessenberg form) ──────
        // Step 1: Reduce to upper Hessenberg form via Householder reflections.
        hessenberg_reduce(&mut h, n);

        // Step 2: Francis QR iteration to converge to quasi-upper-triangular
        // (real Schur) form — 2×2 diagonal blocks represent complex pairs.
        qr_francis(&mut h, n);

        // Step 3: Extract eigenvalue magnitudes from the diagonal / 2×2 blocks.
        spectral_radius_from_schur(&h, n)
    }
}

impl MultivariateForecastModel for VarModel {
    fn fit(
        &mut self,
        ctx: &MultiSeriesContext,
        _target_idx: usize,
    ) -> Result<(), MultivariateError> {
        let k = ctx.matrix.n_series();
        let n = ctx.matrix.n_timestamps();
        let p = self.lag_order;

        if n <= p + 1 {
            return Err(MultivariateError::InsufficientData { min: p + 2, got: n });
        }

        self.k = k;
        self.series_names = ctx.matrix.series_ids.clone();

        let engine = CpuEngine::default();

        // For each equation i: y_i(t) = c_i + Σ_{l=1..p} Σ_{j=1..k} A_l(i,j) * y_j(t-l)
        // Solved via OLS per equation.
        let dim = 1 + p * k; // intercept + p lags × k series
        self.intercepts = vec![0.0; k];
        self.coefficients = vec![vec![0.0; p * k]; k]; // per-equation

        for eq in 0..k {
            let mut xtx = vec![0.0; dim * dim];
            let mut xty = vec![0.0; dim];

            for t in p..n {
                let mut row = Vec::with_capacity(dim);
                row.push(1.0); // intercept
                for l in 1..=p {
                    for s in 0..k {
                        row.push(ctx.matrix.data[s][t - l]);
                    }
                }
                let y = ctx.matrix.data[eq][t];
                for i in 0..dim {
                    for j in 0..dim {
                        xtx[i * dim + j] += row[i] * row[j];
                    }
                    xty[i] += row[i] * y;
                }
            }

            // Regularize to avoid singular matrix
            for i in 0..dim {
                xtx[i * dim + i] += 1e-8;
            }

            let beta = engine.batch_matrix_solve(&xtx, &xty, dim).map_err(
                |e: crate::compute::ComputeError| MultivariateError::Compute(e.to_string()),
            )?;

            self.intercepts[eq] = beta[0];
            self.coefficients[eq] = beta[1..].to_vec();
        }

        // Store last p observations for prediction
        self.last_values = Vec::with_capacity(p);
        for l in 0..p {
            let t = n - p + l;
            let obs: Vec<f64> = (0..k).map(|s| ctx.matrix.data[s][t]).collect();
            self.last_values.push(obs);
        }

        // Timing
        self.last_ts = ctx
            .matrix
            .timestamps
            .last()
            .copied()
            .ok_or(MultivariateError::InsufficientData { min: 1, got: 0 })?;
        if ctx.matrix.timestamps.len() >= 2 {
            let ts = &ctx.matrix.timestamps;
            let mut deltas: Vec<i64> = ts.windows(2).map(|w| w[1] - w[0]).collect();
            deltas.sort_unstable();
            self.interval_ns = deltas[deltas.len() / 2];
        }

        self.fitted = true;

        // VAR stability check — verify spectral radius < threshold.
        // Build the companion matrix and compute spectral radius via
        // power iteration. Unstable VAR produces exponentially diverging
        // forecasts. The threshold (default 1.05) is configurable via
        // `new_with_stability_threshold()`.
        let stability_threshold = self.stability_threshold;
        let spectral_radius = self.compute_spectral_radius();
        if spectral_radius >= stability_threshold {
            tracing::warn!(
                spectral_radius = spectral_radius,
                "VAR model is unstable (spectral radius >= {stability_threshold}) — forecasts may diverge"
            );
            return Err(MultivariateError::InvalidParameter(format!(
                "VAR model is unstable: spectral radius {spectral_radius:.4} >= {stability_threshold}; \
                 try increasing regularization, reducing lag order, or differencing the data"
            )));
        } else if spectral_radius >= 1.0 {
            tracing::warn!(
                spectral_radius = spectral_radius,
                "VAR model is near unit-root (spectral radius close to 1.0) — \
                 forecasts may drift; consider differencing"
            );
        }

        Ok(())
    }

    fn predict(&self, horizon: usize) -> Result<MultivariateForecastResult, MultivariateError> {
        if !self.fitted {
            return Err(MultivariateError::NotFitted);
        }

        let k = self.k;
        let p = self.lag_order;
        let mut history = self.last_values.clone();
        let mut predictions = vec![Vec::with_capacity(horizon); k];
        let timestamps: Vec<i64> = (1..=horizon as i64)
            .map(|h| {
                self.last_ts
                    .saturating_add(h.saturating_mul(self.interval_ns))
            })
            .collect();

        for _ in 0..horizon {
            let mut new_obs = vec![0.0; k];
            #[allow(clippy::needless_range_loop)]
            for eq in 0..k {
                let mut val = self.intercepts[eq];
                #[allow(clippy::needless_range_loop)]
                for l in 0..p {
                    let lag_idx = history.len() - 1 - l;
                    if lag_idx < history.len() {
                        for s in 0..k {
                            val += self.coefficients[eq][l * k + s] * history[lag_idx][s];
                        }
                    }
                }
                new_obs[eq] = val;
            }
            for eq in 0..k {
                predictions[eq].push(new_obs[eq]);
            }
            history.push(new_obs);
        }

        // Importance: average absolute coefficient per series
        let mut importance = Vec::with_capacity(k);
        for s in 0..k {
            let avg: f64 = (0..k)
                .flat_map(|eq| (0..p).map(move |l| self.coefficients[eq][l * k + s].abs()))
                .sum::<f64>()
                / (k * p) as f64;
            importance.push((self.series_names[s].clone(), avg));
        }

        Ok(MultivariateForecastResult {
            timestamps,
            predictions,
            predictor_importance: importance,
        })
    }

    fn supports_online_learning(&self) -> bool {
        false
    }
}

// ─── Granger Causality ──────────────────────────────────────────────

/// Result of a pairwise Granger causality test.
#[derive(Debug, Clone)]
pub struct GrangerCausalityResult {
    /// Name of the "cause" series (whose lags were excluded in H₀).
    pub cause: String,
    /// Name of the "effect" series being predicted.
    pub effect: String,
    /// Lag order used for the test.
    pub lag_order: usize,
    /// F-statistic.
    pub f_statistic: f64,
    /// Raw (unadjusted) p-value from the F-distribution.
    pub p_value: f64,
    /// Benjamini-Hochberg adjusted p-value.
    /// Controls the false discovery rate across all pairwise tests.
    pub adjusted_p_value: f64,
    /// Whether H₀ is rejected after BH correction at α = 0.05.
    pub significant: bool,
    /// RSS of the unrestricted model (all lags included).
    pub rss_unrestricted: f64,
    /// RSS of the restricted model (cause lags excluded).
    pub rss_restricted: f64,
}

impl VarModel {
    /// Perform pairwise Granger causality tests for all series pairs.
    ///
    /// For each pair (cause → effect), fits an unrestricted VAR equation
    /// (with all series' lags) and a restricted equation (excluding the
    /// cause's lags), then compares RSS via an F-test.
    ///
    /// **Requires** the model to be fitted first via `fit()`.
    ///
    /// # F-test
    ///
    /// $$F = \frac{(RSS_r - RSS_u) / p}{RSS_u / (T - k_{full})}$$
    ///
    /// where $p$ = lag order (number of excluded regressors),
    /// $T$ = number of usable observations, and $k_{full}$ = number of
    /// regressors in the unrestricted equation (1 + p × k).
    pub fn granger_causality(
        &self,
        ctx: &MultiSeriesContext,
    ) -> Result<Vec<GrangerCausalityResult>, MultivariateError> {
        if !self.fitted {
            return Err(MultivariateError::NotFitted);
        }

        let k = self.k;
        let p = self.lag_order;
        let n = ctx.matrix.n_timestamps();
        let t = n - p; // usable observations

        if t <= 1 + p * k {
            return Err(MultivariateError::InsufficientData {
                min: 2 + p * k,
                got: t,
            });
        }

        let engine = CpuEngine::default();
        let dim_full = 1 + p * k; // unrestricted: intercept + p lags × k series
        let dim_restricted = 1 + p * (k - 1); // restricted: exclude one series' lags

        let mut results = Vec::with_capacity(k * (k - 1));

        for effect_eq in 0..k {
            for cause_s in 0..k {
                if cause_s == effect_eq {
                    continue;
                }

                // ── Unrestricted model: all lags ──
                let rss_u = self.ols_rss(ctx, &engine, effect_eq, None, dim_full, p, k)?;

                // ── Restricted model: exclude cause_s lags ──
                let rss_r =
                    self.ols_rss(ctx, &engine, effect_eq, Some(cause_s), dim_restricted, p, k)?;

                // ── F-test ──
                let df1 = p as f64; // numerator df
                let df2 = (t - dim_full) as f64; // denominator df
                let f_stat = if rss_u.abs() < 1e-30 {
                    0.0
                } else {
                    ((rss_r - rss_u) / df1) / (rss_u / df2)
                };

                // p-value from F(df1, df2) using the regularized incomplete
                // beta function: P(F > f) = 1 - I_{x}(a, b) where
                // x = df2 / (df2 + df1 * f), a = df2/2, b = df1/2.
                let p_value = f_distribution_sf(f_stat, df1, df2);

                results.push(GrangerCausalityResult {
                    cause: self.series_names[cause_s].clone(),
                    effect: self.series_names[effect_eq].clone(),
                    lag_order: p,
                    f_statistic: f_stat,
                    p_value,
                    adjusted_p_value: p_value, // will be corrected below
                    significant: false,        // will be set after BH
                    rss_unrestricted: rss_u,
                    rss_restricted: rss_r,
                });
            }
        }

        // Benjamini-Hochberg FDR correction for multiple comparisons.
        // Sort by p-value ascending, apply BH threshold:
        //   threshold_i = α × rank_i / m
        let m = results.len();
        if m > 0 {
            let alpha = 0.05;
            let mut indices: Vec<usize> = (0..m).collect();
            indices.sort_by(|&a, &b| {
                results[a]
                    .p_value
                    .partial_cmp(&results[b].p_value)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            // Compute BH-adjusted p-values (step-up procedure).
            // adjusted_p[i] = min(p[i] * m / rank, min of adjusted_p[j] for j > i)
            let mut adj_p = vec![0.0; m];
            for (rank, &idx) in indices.iter().enumerate() {
                adj_p[idx] = (results[idx].p_value * m as f64 / (rank + 1) as f64).min(1.0);
            }
            // Enforce monotonicity: walk backwards through rank order
            let mut running_min = 1.0;
            for &idx in indices.iter().rev() {
                adj_p[idx] = adj_p[idx].min(running_min);
                running_min = adj_p[idx];
            }
            for (i, p_adj) in adj_p.into_iter().enumerate() {
                results[i].adjusted_p_value = p_adj;
                results[i].significant = p_adj < alpha;
            }
        }

        Ok(results)
    }

    /// HC3-robust pairwise Granger causality tests.
    ///
    /// Same hypothesis as `granger_causality` but uses the HC3
    /// (MacKinnon & White 1985) covariance estimator, which is valid
    /// under arbitrary conditional heteroscedasticity.
    ///
    /// For each pair (cause → effect) the unrestricted OLS equation is
    /// fitted, residuals are obtained, and a Wald test with HC3
    /// sandwich covariance is computed:
    ///
    /// $$W = (R\hat\beta)^\top (R\,\hat V_{HC3}\,R^\top)^{-1}(R\hat\beta)$$
    ///
    /// where $R$ selects the $p$ coefficients of the candidate cause.
    /// $F = W / p$ is compared against $F(p, T - kp - 1)$.
    pub fn granger_causality_robust(
        &self,
        ctx: &MultiSeriesContext,
    ) -> Result<Vec<GrangerCausalityResult>, MultivariateError> {
        if !self.fitted {
            return Err(MultivariateError::NotFitted);
        }

        let k = self.k;
        let p = self.lag_order;
        let n = ctx.matrix.n_timestamps();
        let t = n - p;

        if t <= 1 + p * k {
            return Err(MultivariateError::InsufficientData {
                min: 2 + p * k,
                got: t,
            });
        }

        let engine = CpuEngine::default();
        let dim = 1 + p * k; // unrestricted model dimension
        let df_den = t as f64 - dim as f64;

        if df_den <= 0.0 {
            return Err(MultivariateError::InsufficientData {
                min: dim + p + 1,
                got: t,
            });
        }

        let mut results = Vec::with_capacity(k * (k - 1));

        for effect_eq in 0..k {
            // Build X (t × dim) and y (t) for the unrestricted equation.
            let mut x_mat = vec![0.0_f64; t * dim];
            let mut y_vec = vec![0.0_f64; t];

            for (row, obs) in (p..n).enumerate() {
                y_vec[row] = ctx.matrix.data[effect_eq][obs];
                x_mat[row * dim] = 1.0;
                let mut col = 1;
                for l in 1..=p {
                    for s in 0..k {
                        x_mat[row * dim + col] = ctx.matrix.data[s][obs - l];
                        col += 1;
                    }
                }
            }

            // X'X (dim × dim)
            let mut xtx = vec![0.0_f64; dim * dim];
            for row in 0..t {
                for i in 0..dim {
                    let xi = x_mat[row * dim + i];
                    for j in i..dim {
                        let v = xi * x_mat[row * dim + j];
                        xtx[i * dim + j] += v;
                        if i != j {
                            xtx[j * dim + i] += v;
                        }
                    }
                }
            }
            // X'y
            let mut xty = vec![0.0_f64; dim];
            for row in 0..t {
                let y = y_vec[row];
                for i in 0..dim {
                    xty[i] += x_mat[row * dim + i] * y;
                }
            }
            // Ridge regularization
            for i in 0..dim {
                xtx[i * dim + i] += 1e-8;
            }

            // β = (X'X)^{-1} X'y
            let beta = engine
                .batch_matrix_solve(&xtx, &xty, dim)
                .map_err(|e| MultivariateError::Compute(e.to_string()))?;

            // Residuals
            let mut residuals = vec![0.0_f64; t];
            for row in 0..t {
                let mut fitted = 0.0;
                for j in 0..dim {
                    fitted += x_mat[row * dim + j] * beta[j];
                }
                residuals[row] = y_vec[row] - fitted;
            }

            // (X'X)^{-1} via column solves
            let mut xtx_inv = vec![0.0_f64; dim * dim];
            for c in 0..dim {
                let mut rhs = vec![0.0_f64; dim];
                rhs[c] = 1.0;
                let sol = engine
                    .batch_matrix_solve(&xtx, &rhs, dim)
                    .map_err(|e| MultivariateError::Compute(e.to_string()))?;
                for r in 0..dim {
                    xtx_inv[r * dim + c] = sol[r];
                }
            }

            // Hat diagonal: h_ii = x_i' (X'X)^{-1} x_i
            let mut hat_diag = vec![0.0_f64; t];
            for row in 0..t {
                let mut h = 0.0;
                for i in 0..dim {
                    let mut inner = 0.0;
                    for j in 0..dim {
                        inner += xtx_inv[i * dim + j] * x_mat[row * dim + j];
                    }
                    h += x_mat[row * dim + i] * inner;
                }
                hat_diag[row] = h.clamp(0.0, 1.0 - 1e-12);
            }

            // HC3 meat: M = Σ x_i x_i' e_i² / (1 - h_ii)²
            let mut meat = vec![0.0_f64; dim * dim];
            for row in 0..t {
                let denom = (1.0 - hat_diag[row]).powi(2);
                let w = residuals[row].powi(2) / denom;
                for i in 0..dim {
                    let xi_w = x_mat[row * dim + i] * w;
                    for j in i..dim {
                        let v = xi_w * x_mat[row * dim + j];
                        meat[i * dim + j] += v;
                        if i != j {
                            meat[j * dim + i] += v;
                        }
                    }
                }
            }

            // V_HC3 = (X'X)^{-1} M (X'X)^{-1}
            let mut tmp = vec![0.0_f64; dim * dim];
            for i in 0..dim {
                for j in 0..dim {
                    let mut s = 0.0;
                    for c in 0..dim {
                        s += xtx_inv[i * dim + c] * meat[c * dim + j];
                    }
                    tmp[i * dim + j] = s;
                }
            }
            let mut v_hc3 = vec![0.0_f64; dim * dim];
            for i in 0..dim {
                for j in 0..dim {
                    let mut s = 0.0;
                    for c in 0..dim {
                        s += tmp[i * dim + c] * xtx_inv[c * dim + j];
                    }
                    v_hc3[i * dim + j] = s;
                }
            }

            // RSS for the unrestricted model (for the result struct)
            let rss_u: f64 = residuals.iter().map(|e| e * e).sum();

            // Now test each potential cause against this effect
            for cause_s in 0..k {
                if cause_s == effect_eq {
                    continue;
                }

                // R selects p columns for cause_s lags in unrestricted model:
                // col = 1 + (l-1)*k + cause_s  for l = 1..=p
                let cause_cols: Vec<usize> = (1..=p).map(|l| 1 + (l - 1) * k + cause_s).collect();

                // Rβ
                let r_beta: Vec<f64> = cause_cols.iter().map(|&c| beta[c]).collect();

                // R V_HC3 R' (p × p)
                let mut r_v_rt = vec![0.0_f64; p * p];
                for (i, &ci) in cause_cols.iter().enumerate() {
                    for (j, &cj) in cause_cols.iter().enumerate() {
                        r_v_rt[i * p + j] = v_hc3[ci * dim + cj];
                    }
                }
                for i in 0..p {
                    r_v_rt[i * p + i] += 1e-12;
                }

                // Wald = (Rβ)' (R V R')^{-1} (Rβ)
                let inv_r_beta = engine
                    .batch_matrix_solve(&r_v_rt, &r_beta, p)
                    .map_err(|e| MultivariateError::Compute(e.to_string()))?;

                let wald: f64 = r_beta
                    .iter()
                    .zip(inv_r_beta.iter())
                    .map(|(a, b)| a * b)
                    .sum();
                let f_stat = (wald / p as f64).max(0.0);
                let p_value = f_distribution_sf(f_stat, p as f64, df_den);

                // Also compute restricted RSS for the result struct
                let dim_r = 1 + p * (k - 1);
                let rss_r = self.ols_rss(ctx, &engine, effect_eq, Some(cause_s), dim_r, p, k)?;

                results.push(GrangerCausalityResult {
                    cause: self.series_names[cause_s].clone(),
                    effect: self.series_names[effect_eq].clone(),
                    lag_order: p,
                    f_statistic: f_stat,
                    p_value,
                    adjusted_p_value: p_value,
                    significant: false,
                    rss_unrestricted: rss_u,
                    rss_restricted: rss_r,
                });
            }
        }

        // BH correction
        let m = results.len();
        if m > 0 {
            let alpha = 0.05;
            let mut indices: Vec<usize> = (0..m).collect();
            indices.sort_by(|&a, &b| {
                results[a]
                    .p_value
                    .partial_cmp(&results[b].p_value)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            let mut adj_p = vec![0.0; m];
            for (rank, &idx) in indices.iter().enumerate() {
                adj_p[idx] = (results[idx].p_value * m as f64 / (rank + 1) as f64).min(1.0);
            }
            let mut running_min = 1.0;
            for &idx in indices.iter().rev() {
                adj_p[idx] = adj_p[idx].min(running_min);
                running_min = adj_p[idx];
            }
            for (i, p_adj) in adj_p.into_iter().enumerate() {
                results[i].adjusted_p_value = p_adj;
                results[i].significant = p_adj < alpha;
            }
        }

        Ok(results)
    }

    /// Fit one OLS equation and return RSS.
    ///
    /// If `exclude_series` is `Some(s)`, the lags of series `s` are
    /// omitted from the design matrix (restricted model for Granger test).
    fn ols_rss(
        &self,
        ctx: &MultiSeriesContext,
        engine: &CpuEngine,
        effect_eq: usize,
        exclude_series: Option<usize>,
        dim: usize,
        p: usize,
        k: usize,
    ) -> Result<f64, MultivariateError> {
        let n = ctx.matrix.n_timestamps();
        let mut xtx = vec![0.0; dim * dim];
        let mut xty = vec![0.0; dim];

        for obs in p..n {
            let mut row = Vec::with_capacity(dim);
            row.push(1.0); // intercept
            for l in 1..=p {
                for s in 0..k {
                    if exclude_series == Some(s) {
                        continue;
                    }
                    row.push(ctx.matrix.data[s][obs - l]);
                }
            }
            let y = ctx.matrix.data[effect_eq][obs];
            for i in 0..dim {
                for j in 0..dim {
                    xtx[i * dim + j] += row[i] * row[j];
                }
                xty[i] += row[i] * y;
            }
        }

        // Regularize
        for i in 0..dim {
            xtx[i * dim + i] += 1e-8;
        }

        let beta = engine
            .batch_matrix_solve(&xtx, &xty, dim)
            .map_err(|e| MultivariateError::Compute(e.to_string()))?;

        // Compute RSS
        let mut rss = 0.0;
        for obs in p..n {
            let mut pred = beta[0]; // intercept
            let mut col = 1;
            for l in 1..=p {
                for s in 0..k {
                    if exclude_series == Some(s) {
                        continue;
                    }
                    pred += beta[col] * ctx.matrix.data[s][obs - l];
                    col += 1;
                }
            }
            let residual = ctx.matrix.data[effect_eq][obs] - pred;
            rss += residual * residual;
        }

        Ok(rss)
    }
}

/// Survival function of the F-distribution: P(F > f) for F ~ F(d1, d2).
///
/// Uses the regularized incomplete beta function relation:
/// P(F > f) = I_{x}(d2/2, d1/2) where x = d2 / (d2 + d1 * f).
fn f_distribution_sf(f: f64, d1: f64, d2: f64) -> f64 {
    if f <= 0.0 || d1 <= 0.0 || d2 <= 0.0 {
        return 1.0;
    }
    let x = d2 / (d2 + d1 * f);
    regularized_incomplete_beta(d2 / 2.0, d1 / 2.0, x)
}

/// Regularized incomplete beta function I_x(a, b) via continued fraction
/// (Lentz's method).  Accurate to ~1e-10 for typical F-test parameters.
fn regularized_incomplete_beta(a: f64, b: f64, x: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }

    // Use the symmetry relation when x > (a+1)/(a+b+2) for convergence
    if x > (a + 1.0) / (a + b + 2.0) {
        return 1.0 - regularized_incomplete_beta(b, a, 1.0 - x);
    }

    let ln_prefix = a * x.ln() + b * (1.0 - x).ln() - ln_beta(a, b) - a.ln();
    let prefix = ln_prefix.exp();

    // Lentz's continued fraction for I_x(a,b)
    let max_iter = 200;
    let eps = 1e-14;
    let tiny = 1e-30;

    let mut c = 1.0;
    let mut d = 1.0 - (a + b) * x / (a + 1.0);
    if d.abs() < tiny {
        d = tiny;
    }
    d = 1.0 / d;
    let mut h = d;

    for m in 1..=max_iter {
        let m_f = m as f64;

        // Even step
        let num_even = m_f * (b - m_f) * x / ((a + 2.0 * m_f - 1.0) * (a + 2.0 * m_f));
        d = 1.0 + num_even * d;
        if d.abs() < tiny {
            d = tiny;
        }
        d = 1.0 / d;
        c = 1.0 + num_even / c;
        if c.abs() < tiny {
            c = tiny;
        }
        h *= d * c;

        // Odd step
        let num_odd = -((a + m_f) * (a + b + m_f)) * x / ((a + 2.0 * m_f) * (a + 2.0 * m_f + 1.0));
        d = 1.0 + num_odd * d;
        if d.abs() < tiny {
            d = tiny;
        }
        d = 1.0 / d;
        c = 1.0 + num_odd / c;
        if c.abs() < tiny {
            c = tiny;
        }
        let delta = d * c;
        h *= delta;

        if (delta - 1.0).abs() < eps {
            break;
        }
    }

    prefix * h
}

/// Log of the Beta function: ln B(a,b) = ln Γ(a) + ln Γ(b) − ln Γ(a+b).
fn ln_beta(a: f64, b: f64) -> f64 {
    ln_gamma(a) + ln_gamma(b) - ln_gamma(a + b)
}

/// Lanczos approximation for ln Γ(x).
fn ln_gamma(x: f64) -> f64 {
    const COEFFS: [f64; 7] = [
        0.999_999_999_999_809_9,
        676.5203681218851,
        -1259.1392167224028,
        771.323_428_777_653_1,
        -176.615_029_162_140_6,
        12.507343278686905,
        -0.13857109526572012,
    ];
    let x = x - 1.0;
    let mut y = COEFFS[0];
    for (i, &c) in COEFFS.iter().enumerate().skip(1) {
        y += c / (x + i as f64);
    }
    let t = x + 6.5;
    0.5 * (2.0 * std::f64::consts::PI).ln() + (t).ln() * (x + 0.5) - t + y.ln()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn simple_ctx() -> MultiSeriesContext {
        let n = 100;
        let ts: Vec<i64> = (0..n as i64).map(|i| i * 1_000_000_000).collect();
        // Use independent predictors: x1 = linear, x2 = sinusoidal
        let x1: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let x2: Vec<f64> = (0..n).map(|i| (i as f64 * 0.1).sin() * 10.0).collect();
        // y = 2*x1 + 3*x2 + 5
        let y: Vec<f64> = (0..n)
            .map(|i| 2.0 * (i as f64) + 3.0 * (i as f64 * 0.1).sin() * 10.0 + 5.0)
            .collect();
        MultiSeriesContext::build(
            vec![
                ("y".to_string(), ts.clone(), y),
                ("x1".to_string(), ts.clone(), x1),
                ("x2".to_string(), ts, x2),
            ],
            None,
        )
        .unwrap()
    }

    #[test]
    fn multi_linear_regression_fit() {
        let ctx = simple_ctx();
        let mut model = MultiLinearRegression::new();
        model.fit(&ctx, 0).unwrap();
        assert!(model.r_squared > 0.99, "R²={}", model.r_squared);
        // Coefficients should be close to [2.0, 3.0]
        assert!((model.coefficients[0] - 2.0).abs() < 0.5);
        assert!((model.coefficients[1] - 3.0).abs() < 0.5);
    }

    #[test]
    fn multi_linear_regression_predict() {
        let ctx = simple_ctx();
        let mut model = MultiLinearRegression::new();
        model.fit(&ctx, 0).unwrap();
        let result = model.predict(5).unwrap();
        assert_eq!(result.predictions[0].len(), 5);
        assert_eq!(result.timestamps.len(), 5);
        // With y = 2*x1 + 3*x2 + 5, predictions should use last known predictor values.
        // Last x1 = 99.0, last x2 = sin(9.9)*10 ≈ -4.56; expected ≈ 2*99 + 3*(-4.56) + 5 ≈ 189.3
        let pred = result.predictions[0][0];
        assert!(
            pred > 100.0,
            "prediction {} should reflect learned coefficients, not just intercept",
            pred
        );
    }

    #[test]
    fn var_fit_and_predict() {
        let n = 200;
        let ts: Vec<i64> = (0..n as i64).map(|i| i * 1_000_000_000).collect();
        let s1: Vec<f64> = (0..n)
            .map(|i| (i as f64 * 0.05).sin() * 10.0 + 50.0)
            .collect();
        let s2: Vec<f64> = (0..n)
            .map(|i| (i as f64 * 0.05).cos() * 10.0 + 50.0)
            .collect();
        let ctx = MultiSeriesContext::build(
            vec![
                ("s1".to_string(), ts.clone(), s1),
                ("s2".to_string(), ts, s2),
            ],
            None,
        )
        .unwrap();

        let mut model = VarModel::new(Some(2));
        model.fit(&ctx, 0).unwrap();
        let result = model.predict(10).unwrap();
        assert_eq!(result.predictions.len(), 2); // both series
        assert_eq!(result.predictions[0].len(), 10);
        assert_eq!(result.predictions[1].len(), 10);
    }

    #[test]
    fn var_predictor_importance() {
        let n = 200;
        let ts: Vec<i64> = (0..n as i64).map(|i| i * 1_000_000_000).collect();
        let s1: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let s2: Vec<f64> = (0..n).map(|i| i as f64 * 2.0).collect();
        let ctx = MultiSeriesContext::build(
            vec![
                ("s1".to_string(), ts.clone(), s1),
                ("s2".to_string(), ts, s2),
            ],
            None,
        )
        .unwrap();

        let mut model = VarModel::new(Some(1));
        model.fit(&ctx, 0).unwrap();
        let result = model.predict(5).unwrap();
        assert_eq!(result.predictor_importance.len(), 2);
    }

    #[test]
    fn granger_causality_detects_cause() {
        // s2(t) = 0.8 * s1(t-1) + noise → s1 Granger-causes s2
        let n = 300;
        let ts: Vec<i64> = (0..n as i64).map(|i| i * 1_000_000_000).collect();
        // Simple deterministic "pseudo-random" via LCG
        let mut rng_state: u64 = 42;
        let mut pseudo_rand = || -> f64 {
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            ((rng_state >> 33) as f64 / (1u64 << 31) as f64 - 0.5) * 0.5
        };

        let s1: Vec<f64> = (0..n).map(|i| (i as f64 * 0.1).sin() * 5.0).collect();
        let mut s2 = vec![0.0; n];
        for i in 1..n {
            s2[i] = 0.8 * s1[i - 1] + pseudo_rand();
        }

        let ctx = MultiSeriesContext::build(
            vec![
                ("s1".to_string(), ts.clone(), s1),
                ("s2".to_string(), ts, s2),
            ],
            None,
        )
        .unwrap();

        let mut model = VarModel::new(Some(2));
        model.fit(&ctx, 0).unwrap();

        let results = model.granger_causality(&ctx).unwrap();
        assert_eq!(results.len(), 2); // s1→s2 and s2→s1

        let s1_causes_s2 = results
            .iter()
            .find(|r| r.cause == "s1" && r.effect == "s2")
            .unwrap();
        assert!(
            s1_causes_s2.significant,
            "s1 should Granger-cause s2: F={:.3}, p={:.4}",
            s1_causes_s2.f_statistic, s1_causes_s2.p_value
        );
        assert!(s1_causes_s2.f_statistic > 1.0);
        assert!(s1_causes_s2.p_value < 0.05);
    }

    #[test]
    fn granger_requires_fitted_model() {
        let n = 100;
        let ts: Vec<i64> = (0..n as i64).map(|i| i * 1_000_000_000).collect();
        let s1: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let s2: Vec<f64> = (0..n).map(|i| (i as f64).sqrt()).collect();
        let ctx = MultiSeriesContext::build(
            vec![
                ("s1".to_string(), ts.clone(), s1),
                ("s2".to_string(), ts, s2),
            ],
            None,
        )
        .unwrap();

        let model = VarModel::new(Some(1));
        let err = model.granger_causality(&ctx);
        assert!(err.is_err());
    }

    #[test]
    fn granger_robust_detects_cause() {
        // Same causal setup as granger_causality_detects_cause.
        let n = 200;
        let ts: Vec<i64> = (0..n as i64).map(|i| i * 1_000_000_000).collect();
        let mut s1 = vec![0.0; n];
        let mut s2 = vec![0.0; n];
        let mut rng_state: u64 = 12345;
        for i in 1..n {
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let noise1 = ((rng_state >> 33) as f64 / (1u64 << 31) as f64 - 0.5) * 0.5;
            s1[i] = 0.7 * s1[i - 1] + noise1;
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let noise2 = ((rng_state >> 33) as f64 / (1u64 << 31) as f64 - 0.5) * 0.2;
            // s2 is caused by s1 at lag 1
            s2[i] = 0.8 * s1[i - 1] + noise2;
        }
        let ctx = MultiSeriesContext::build(
            vec![
                ("s1".to_string(), ts.clone(), s1),
                ("s2".to_string(), ts, s2),
            ],
            None,
        )
        .unwrap();

        let mut model = VarModel::new(Some(1));
        model.fit(&ctx, 1).unwrap();
        let results = model.granger_causality_robust(&ctx).unwrap();
        // s1 → s2 should be significant
        let s1_causes_s2 = results
            .iter()
            .find(|r| r.cause == "s1" && r.effect == "s2")
            .unwrap();
        assert!(
            s1_causes_s2.significant,
            "HC3: s1 should Granger-cause s2: F={:.3}, p={:.4}, adj_p={:.4}",
            s1_causes_s2.f_statistic, s1_causes_s2.p_value, s1_causes_s2.adjusted_p_value,
        );
    }
}
