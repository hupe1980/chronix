//! Multivariate forecasting: Multi-Linear Regression and VAR models.

use crate::compute::{ComputeEngine, CpuEngine};

use crate::multivariate::context::MultiSeriesContext;
use crate::multivariate::error::MultivariateError;

/// Result of a multivariate forecast.
#[derive(Debug, Clone)]
pub struct MultivariateForecastResult {
    /// Forecast timestamps (nanoseconds since epoch).
    pub timestamps: Vec<i64>,
    /// The series each row of `predictions` belongs to, in the same order.
    ///
    /// This exists because the two implementors of
    /// [`MultivariateForecastModel`] return **different numbers of rows** for
    /// the same call, and nothing used to say so.
    /// [`MultiLinearRegression`] is many-predictors-to-one-target and returns
    /// one row; [`VarModel`] fits every equation of the system and returns one
    /// row per series. A caller reaching for `predictions[0]` therefore got
    /// the requested series from one and series index 0 from the other — a
    /// plausible number belonging to a different series, with nothing in the
    /// type to catch it.
    pub series: Vec<String>,
    /// Predicted values, one row per entry of [`series`](Self::series).
    pub predictions: Vec<Vec<f64>>,
    /// The series named by `target_idx` at [`fit`](MultivariateForecastModel::fit).
    ///
    /// Prefer [`target_forecast`](Self::target_forecast) to indexing
    /// `predictions` directly.
    pub target: String,
    /// Per-predictor importance / coefficients.
    pub predictor_importance: Vec<(String, f64)>,
}

impl MultivariateForecastResult {
    /// The forecast for one named series, if this model produced it.
    #[must_use]
    pub fn for_series(&self, name: &str) -> Option<&[f64]> {
        let i = self.series.iter().position(|s| s == name)?;
        self.predictions.get(i).map(Vec::as_slice)
    }

    /// The forecast for the series the caller asked `fit` for.
    ///
    /// Correct for every implementor, which is the point: it is what
    /// `predictions[0]` was reached for and only sometimes was.
    #[must_use]
    pub fn target_forecast(&self) -> Option<&[f64]> {
        self.for_series(&self.target)
    }
}

/// Multivariate forecast model contract.
pub trait MultivariateForecastModel: Send + Sync {
    /// Fit the model.
    ///
    /// - `ctx` — aligned multi-series context.
    /// - `target_idx` — the series the caller wants forecast.
    ///
    /// A model that forecasts the **whole system** — [`VarModel`] fits one
    /// equation per series — still records `target_idx` and still returns
    /// every series it fitted. It is recorded in
    /// [`MultivariateForecastResult::target`] so that
    /// [`target_forecast`](MultivariateForecastResult::target_forecast)
    /// answers correctly whichever model produced the result. `target_idx`
    /// selects what the caller asked for; it does not promise to be the only
    /// row that comes back.
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
    /// Name of the target series, so the result can name its one row.
    target_name: String,
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
            target_name: String::new(),
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
        self.target_name = ctx.matrix.series_ids[target_idx].clone();
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
            series: vec![self.target_name.clone()],
            predictions,
            target: self.target_name.clone(),
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
    /// The series the caller asked `fit` for. A VAR fits every equation, so
    /// this selects what the caller wanted out of what the model produced —
    /// it was `_target_idx` and thrown away, which made `predictions[0]`
    /// silently mean series 0 rather than the requested one.
    target_name: String,
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
            target_name: String::new(),
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
        target_idx: usize,
    ) -> Result<(), MultivariateError> {
        let k = ctx.matrix.n_series();
        let n = ctx.matrix.n_timestamps();
        let p = self.lag_order;

        if target_idx >= k {
            return Err(MultivariateError::InvalidParameter(format!(
                "target_idx {target_idx} >= n_series {k}"
            )));
        }
        self.target_name = ctx.matrix.series_ids[target_idx].clone();

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
            series: self.series_names.clone(),
            predictions,
            target: self.target_name.clone(),
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
    let z = d2 / (d2 + d1 * f);
    regularized_incomplete_beta(z, d2 / 2.0, d1 / 2.0)
}

/// Regularized incomplete beta function I_x(a, b) via continued fraction
/// (Lentz's method).  Accurate to ~1e-10 for typical F-test parameters.
fn regularized_incomplete_beta(x: f64, a: f64, b: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }

    // Use the symmetry relation when x > (a+1)/(a+b+2) for faster
    // convergence of the continued fraction.
    if x > (a + 1.0) / (a + b + 2.0) {
        return 1.0 - regularized_incomplete_beta(1.0 - x, b, a);
    }

    let ln_prefix = a * x.ln() + b * (1.0 - x).ln() - ln_beta(a, b) - a.ln();
    let prefix = ln_prefix.exp();

    // Lentz's continued fraction
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
        let m_f64 = m as f64;

        // Even step: d_{2m}
        let num_even = m_f64 * (b - m_f64) * x / ((a + 2.0 * m_f64 - 1.0) * (a + 2.0 * m_f64));
        d = 1.0 + num_even * d;
        if d.abs() < tiny {
            d = tiny;
        }
        c = 1.0 + num_even / c;
        if c.abs() < tiny {
            c = tiny;
        }
        d = 1.0 / d;
        h *= d * c;

        // Odd step: d_{2m+1}
        let num_odd =
            -((a + m_f64) * (a + b + m_f64)) * x / ((a + 2.0 * m_f64) * (a + 2.0 * m_f64 + 1.0));
        d = 1.0 + num_odd * d;
        if d.abs() < tiny {
            d = tiny;
        }
        c = 1.0 + num_odd / c;
        if c.abs() < tiny {
            c = tiny;
        }
        d = 1.0 / d;
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
    use crate::forecast::diagnostics::ln_gamma;
    ln_gamma(a) + ln_gamma(b) - ln_gamma(a + b)
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

    // ── The F-distribution survival function, against ground truth ──
    //
    // `f_distribution_sf` decides every Granger p-value, and the copy in this
    // module was **wrong**: `F(2,1)` at 0.01 returned 0.996593 where the truth
    // is 0.990148. It went unnoticed because the only comparison ever made
    // between this module and its now-deleted sibling compared *F-statistics*,
    // which agreed — and both p-values were ~0 in the deep tail, so the
    // comparison was blind to the defect it was meant to find.
    //
    // These assert against **closed forms**, not against another
    // implementation. That is stronger than a table copied from scipy: for
    // these parameter pairs the survival function is an elementary
    // expression, so the assertion is ground truth and reproduces with no
    // third-party install. Comparing against the regularized incomplete beta
    // would be circular — that is what the code computes.

    /// `F(2, d₂)`: `sf(x) = (1 + 2x/d₂)^(−d₂/2)`, from `I_z(a,1) = z^a`.
    #[test]
    fn f_distribution_sf_matches_the_closed_form_for_d1_eq_2() {
        for d2 in [1.0_f64, 2.0, 3.0, 5.0, 10.0, 30.0, 100.0, 500.0] {
            for x in [0.01_f64, 0.1, 0.5, 1.0, 2.0, 3.5, 5.0, 10.0, 50.0, 1000.0] {
                let expected = (1.0 + 2.0 * x / d2).powf(-d2 / 2.0);
                let got = f_distribution_sf(x, 2.0, d2);
                assert!(
                    (got - expected).abs() < 1e-9,
                    "F(2, {d2}) at {x}: got {got:.12}, closed form {expected:.12}"
                );
            }
        }
    }

    /// `F(d₁, 2)`: `sf(x) = 1 − (d₁x/(2 + d₁x))^(d₁/2)`, from `I_z(1,b) = 1−(1−z)^b`.
    #[test]
    fn f_distribution_sf_matches_the_closed_form_for_d2_eq_2() {
        for d1 in [1.0_f64, 2.0, 3.0, 5.0, 10.0, 30.0, 100.0] {
            for x in [0.01_f64, 0.1, 0.5, 1.0, 2.0, 3.5, 5.0, 10.0, 50.0, 1000.0] {
                let expected = 1.0 - (d1 * x / (2.0 + d1 * x)).powf(d1 / 2.0);
                let got = f_distribution_sf(x, d1, 2.0);
                assert!(
                    (got - expected).abs() < 1e-9,
                    "F({d1}, 2) at {x}: got {got:.12}, closed form {expected:.12}"
                );
            }
        }
    }

    /// `F(1,1)` is the square of a standard Cauchy: `sf(x) = 1 − (2/π)·atan(√x)`.
    #[test]
    fn f_distribution_sf_matches_the_closed_form_for_one_and_one() {
        for x in [0.001_f64, 0.01, 0.1, 0.5, 1.0, 2.0, 7.0, 100.0, 10_000.0] {
            let expected = 1.0 - 2.0 / std::f64::consts::PI * x.sqrt().atan();
            let got = f_distribution_sf(x, 1.0, 1.0);
            assert!(
                (got - expected).abs() < 1e-9,
                "F(1, 1) at {x}: got {got:.12}, closed form {expected:.12}"
            );
        }
    }

    /// `sf_{F(d₁,d₂)}(x) = 1 − sf_{F(d₂,d₁)}(1/x)`.
    ///
    /// The reciprocal of an `F(d₁,d₂)` variate is `F(d₂,d₁)`. This holds for
    /// every parameter pair, including those with no elementary form, so it
    /// covers the region the closed forms cannot reach — and it exercises the
    /// argument-swapping branch of the continued fraction, which is where the
    /// sibling `gamma_cf` defect lived.
    #[test]
    fn f_distribution_sf_is_consistent_under_reciprocal() {
        for d1 in [1.0_f64, 3.0, 7.0, 12.0, 40.0] {
            for d2 in [1.0_f64, 3.0, 7.0, 12.0, 40.0] {
                for x in [0.05_f64, 0.3, 1.0, 2.5, 9.0, 60.0] {
                    let a = f_distribution_sf(x, d1, d2);
                    let b = 1.0 - f_distribution_sf(1.0 / x, d2, d1);
                    assert!(
                        (a - b).abs() < 1e-9,
                        "F({d1},{d2}) at {x}: {a:.12} vs reciprocal identity {b:.12}"
                    );
                }
            }
        }
    }

    /// A survival function is in `[0, 1]` and non-increasing.
    #[test]
    fn f_distribution_sf_is_a_survival_function() {
        for d1 in [1.0_f64, 2.0, 5.0, 17.0] {
            for d2 in [1.0_f64, 2.0, 5.0, 17.0] {
                let mut previous = f64::INFINITY;
                for k in 0..200 {
                    let x = f64::from(k) * 0.25 + 0.01;
                    let v = f_distribution_sf(x, d1, d2);
                    assert!((0.0..=1.0).contains(&v), "F({d1},{d2}) at {x}: {v}");
                    assert!(v <= previous + 1e-12, "F({d1},{d2}) increased at {x}");
                    previous = v;
                }
            }
        }
    }

    #[test]
    fn f_distribution_sf_matches_scipy() {
        // `scipy.stats.f.sf`, generated with the venv in this repository.
        // Ground-truth closed forms cover d1=2, d2=2 and (1,1); this table
        // covers the parameter pairs that have no elementary form, which is
        // where the continued fraction actually earns its place.
        const SCIPY: &[(f64, f64, f64, f64)] = &[
            (0.05_f64, 1.0, 1.0, 8.599_513_039_068_979e-1),
            (0.5_f64, 1.0, 1.0, 6.081_734_479_693_929e-1),
            (1.0_f64, 1.0, 1.0, 5.000_000_000_000_001e-1),
            (2.5_f64, 1.0, 1.0, 3.590_170_359_713_762e-1),
            (5.0_f64, 1.0, 1.0, 2.677_204_728_012_301e-1),
            (20.0_f64, 1.0, 1.0, 1.400_486_960_931_020_5e-1),
            (200.0_f64, 1.0, 1.0, 4.494_101_372_651_411e-2),
            (0.05_f64, 1.0, 3.0, 8.374_248_972_889_173e-1),
            (0.5_f64, 1.0, 3.0, 5.304_777_709_329_569e-1),
            (1.0_f64, 1.0, 3.0, 3.910_022_189_557_707e-1),
            (2.5_f64, 1.0, 3.0, 2.119_854_426_726_486_8e-1),
            (5.0_f64, 1.0, 3.0, 1.113_671_547_140_838_7e-1),
            (20.0_f64, 1.0, 3.0, 2.083_515_119_618_485e-2),
            (200.0_f64, 1.0, 3.0, 7.658_843_583_400_378e-4),
            (0.05_f64, 1.0, 5.0, 8.319_122_479_866_876e-1),
            (0.5_f64, 1.0, 5.0, 5.110_840_804_302_803e-1),
            (1.0_f64, 1.0, 5.0, 3.632_174_676_491_225_5e-1),
            (2.5_f64, 1.0, 5.0, 1.746_878_142_641_194_2e-1),
            (5.0_f64, 1.0, 5.0, 7.558_681_842_161_245e-2),
            (20.0_f64, 1.0, 5.0, 6.566_271_827_563_008e-3),
            (200.0_f64, 1.0, 5.0, 3.182_292_826_705_155e-5),
            (0.05_f64, 1.0, 7.0, 8.294_489_446_380_614e-1),
            (0.5_f64, 1.0, 7.0, 5.023_540_170_799_178e-1),
            (1.0_f64, 1.0, 7.0, 3.506_166_628_202_073_7e-1),
            (2.5_f64, 1.0, 7.0, 1.578_593_227_958_298_7e-1),
            (5.0_f64, 1.0, 7.0, 6.042_742_018_442_884e-2),
            (20.0_f64, 1.0, 7.0, 2.893_495_120_607_208e-3),
            (200.0_f64, 1.0, 7.0, 2.097_359_711_842_887e-6),
            (0.05_f64, 1.0, 10.0, 8.275_651_592_009_66e-1),
            (0.5_f64, 1.0, 10.0, 4.956_475_043_831_195_5e-1),
            (1.0_f64, 1.0, 10.0, 3.408_931_323_020_597_5e-1),
            (2.5_f64, 1.0, 10.0, 1.449_276_054_040_804_8e-1),
            (5.0_f64, 1.0, 10.0, 4.933_219_563_992_176_4e-2),
            (20.0_f64, 1.0, 10.0, 1.193_466_830_020_324_4e-3),
            (200.0_f64, 1.0, 10.0, 6.149_001_368_036_42e-8),
            (0.05_f64, 1.0, 30.0, 8.245_790_072_177_894e-1),
            (0.5_f64, 1.0, 30.0, 4.849_569_686_830_377e-1),
            (1.0_f64, 1.0, 30.0, 3.253_086_154_260_302_3e-1),
            (2.5_f64, 1.0, 30.0, 1.243_334_200_465_653_3e-1),
            (5.0_f64, 1.0, 30.0, 3.293_630_592_563_78e-2),
            (20.0_f64, 1.0, 30.0, 1.029_210_178_637_816_9e-4),
            (200.0_f64, 1.0, 30.0, 8.298_453_462_587_944e-15),
            (0.05_f64, 1.0, 100.0, 8.235_194_441_364_071e-1),
            (0.5_f64, 1.0, 100.0, 4.811_446_769_857_403_7e-1),
            (1.0_f64, 1.0, 100.0, 3.197_241_557_841_232e-1),
            (2.5_f64, 1.0, 100.0, 1.170_041_896_091_983_8e-1),
            (5.0_f64, 1.0, 100.0, 2.756_960_066_604_534_4e-2),
            (20.0_f64, 1.0, 100.0, 2.049_634_555_794_899_3e-5),
            (200.0_f64, 1.0, 100.0, 1.351_242_379_602_169_4e-25),
            (0.05_f64, 1.0, 500.0, 8.231_546_038_971_818e-1),
            (0.5_f64, 1.0, 500.0, 4.798_295_396_595_345_7e-1),
            (1.0_f64, 1.0, 500.0, 3.177_942_072_606_256e-1),
            (2.5_f64, 1.0, 500.0, 1.144_786_392_733_747_5e-1),
            (5.0_f64, 1.0, 500.0, 2.578_771_199_599_521_8e-2),
            (20.0_f64, 1.0, 500.0, 9.592_901_087_300_635e-6),
            (200.0_f64, 1.0, 500.0, 1.950_412_009_179_663e-38),
            (0.05_f64, 3.0, 1.0, 9.791_648_488_038_152e-1),
            (0.5_f64, 3.0, 1.0, 7.477_845_036_444_957e-1),
            (1.0_f64, 3.0, 1.0, 6.089_977_810_442_295e-1),
            (2.5_f64, 3.0, 1.0, 4.279_966_192_993_59e-1),
            (5.0_f64, 3.0, 1.0, 3.149_623_575_257_075e-1),
            (20.0_f64, 3.0, 1.0, 1.625_751_027_110_831_4e-1),
            (200.0_f64, 3.0, 1.0, 5.192_211_792_598_956e-2),
            (0.05_f64, 3.0, 3.0, 9.826_133_295_298_128e-1),
            (0.5_f64, 3.0, 3.0, 7.082_085_942_090_715e-1),
            (1.0_f64, 3.0, 3.0, 5.000_000_000_000_001e-1),
            (2.5_f64, 3.0, 3.0, 2.357_618_226_542_65e-1),
            (5.0_f64, 3.0, 3.0, 1.095_510_187_085_240_3e-1),
            (20.0_f64, 3.0, 3.0, 1.738_667_047_018_715_6e-2),
            (200.0_f64, 3.0, 3.0, 5.948_473_159_638_333e-4),
            (0.05_f64, 3.0, 5.0, 9.835_627_319_613_537e-1),
            (0.5_f64, 3.0, 5.0, 6.984_526_373_049_242e-1),
            (1.0_f64, 3.0, 5.0, 4.648_547_899_936_353_3e-1),
            (2.5_f64, 3.0, 5.0, 1.739_276_579_365_1e-1),
            (5.0_f64, 3.0, 5.0, 5.766_888_562_243_733e-2),
            (20.0_f64, 3.0, 5.0, 3.250_005_065_191_810_5e-3),
            (200.0_f64, 3.0, 5.0, 1.261_190_950_980_022_2e-5),
            (0.05_f64, 3.0, 7.0, 9.840_063_156_479_888e-1),
            (0.5_f64, 3.0, 7.0, 6.940_363_875_688_136e-1),
            (1.0_f64, 3.0, 7.0, 4.470_796_134_684_835e-1),
            (2.5_f64, 3.0, 7.0, 1.435_094_562_789_392_7e-1),
            (5.0_f64, 3.0, 7.0, 3.667_335_421_818_645e-2),
            (20.0_f64, 3.0, 7.0, 8.225_792_466_075_144e-4),
            (200.0_f64, 3.0, 7.0, 3.817_227_678_744_054_4e-7),
            (0.05_f64, 3.0, 10.0, 9.843_549_357_040_79e-1),
            (0.5_f64, 3.0, 10.0, 6.906_222_455_335_574e-1),
            (1.0_f64, 3.0, 10.0, 4.323_372_030_216_968e-1),
            (2.5_f64, 3.0, 10.0, 1.190_395_626_582_781_6e-1),
            (5.0_f64, 3.0, 10.0, 2.261_392_275_109_628_4e-2),
            (20.0_f64, 3.0, 10.0, 1.511_650_265_860_084_4e-4),
            (200.0_f64, 3.0, 10.0, 3.183_148_174_576_168_7e-9),
            (0.05_f64, 3.0, 30.0, 9.849_263_153_922_542e-1),
            (0.5_f64, 3.0, 30.0, 6.851_195_412_952_075e-1),
            (1.0_f64, 3.0, 30.0, 4.063_572_668_729_493e-1),
            (2.5_f64, 3.0, 30.0, 7.847_395_791_463_87e-2),
            (5.0_f64, 3.0, 30.0, 6.254_870_601_873_435e-3),
            (20.0_f64, 3.0, 30.0, 2.587_600_271_284_227e-7),
            (200.0_f64, 3.0, 30.0, 6.425_629_332_909_392e-20),
            (0.05_f64, 3.0, 100.0, 9.851_351_172_793_991e-1),
            (0.5_f64, 3.0, 100.0, 6.831_325_288_938_818e-1),
            (1.0_f64, 3.0, 100.0, 3.961_862_496_180_044e-1),
            (2.5_f64, 3.0, 100.0, 6.383_295_997_908_654e-2),
            (5.0_f64, 3.0, 100.0, 2.844_573_100_886_653e-3),
            (20.0_f64, 3.0, 100.0, 3.112_606_226_774_384_6e-10),
            (200.0_f64, 3.0, 100.0, 4.144_844_518_612_426e-42),
            (0.05_f64, 3.0, 500.0, 9.852_077_993_840_09e-1),
            (0.5_f64, 3.0, 500.0, 6.824_432_968_133_542e-1),
            (1.0_f64, 3.0, 500.0, 3.925_476_468_585_115_7e-1),
            (2.5_f64, 3.0, 500.0, 5.881_163_820_983_985e-2),
            (5.0_f64, 3.0, 500.0, 2.001_302_671_949_856e-3),
            (20.0_f64, 3.0, 500.0, 2.948_423_715_311_517_2e-12),
            (200.0_f64, 3.0, 500.0, 3.277_206_998_069_135e-85),
            (0.05_f64, 5.0, 1.0, 9.934_337_281_724_37e-1),
            (0.5_f64, 5.0, 1.0, 7.835_627_707_303_144e-1),
            (1.0_f64, 5.0, 1.0, 6.367_825_323_508_775e-1),
            (2.5_f64, 5.0, 1.0, 4.451_217_169_436_3e-1),
            (5.0_f64, 5.0, 1.0, 3.265_715_644_624_462_3e-1),
            (20.0_f64, 5.0, 1.0, 1.680_877_520_133_123e-1),
            (200.0_f64, 5.0, 1.0, 5.363_087_276_333_012e-2),
            (0.05_f64, 5.0, 3.0, 9.967_499_949_348_082e-1),
            (0.5_f64, 5.0, 3.0, 7.673_760_819_999_214e-1),
            (1.0_f64, 5.0, 3.0, 5.351_452_100_063_651e-1),
            (2.5_f64, 5.0, 3.0, 2.405_360_338_406_623_6e-1),
            (5.0_f64, 5.0, 3.0, 1.078_163_782_679_175_8e-1),
            (20.0_f64, 5.0, 3.0, 1.643_726_803_864_62e-2),
            (200.0_f64, 5.0, 3.0, 5.539_101_145_966_495e-4),
            (0.05_f64, 5.0, 5.0, 9.974_477_392_801_275e-1),
            (0.5_f64, 5.0, 5.0, 7.674_886_808_696_213e-1),
            (1.0_f64, 5.0, 5.0, 5.000_000_000_000_001e-1),
            (2.5_f64, 5.0, 5.0, 1.686_841_555_429_12e-1),
            (5.0_f64, 5.0, 5.0, 5.096_973_941_492_917_4e-2),
            (20.0_f64, 5.0, 5.0, 2.552_260_719_872_432_3e-3),
            (200.0_f64, 5.0, 5.0, 9.433_866_982_533_933e-6),
            (0.05_f64, 5.0, 7.0, 9.977_457_456_693_496e-1),
            (0.5_f64, 5.0, 7.0, 7.685_843_297_279_157e-1),
            (1.0_f64, 5.0, 7.0, 4.812_939_129_582_055e-1),
            (2.5_f64, 5.0, 7.0, 1.320_062_236_078_406_7e-1),
            (5.0_f64, 5.0, 7.0, 2.874_617_655_405_799e-2),
            (20.0_f64, 5.0, 7.0, 5.139_310_327_531_792e-4),
            (200.0_f64, 5.0, 7.0, 2.155_804_614_637_414e-7),
            (0.05_f64, 5.0, 10.0, 9.979_679_819_314_153e-1),
            (0.5_f64, 5.0, 10.0, 7.700_248_806_501_017e-1),
            (1.0_f64, 5.0, 10.0, 4.651_194_265_378_001_4e-1),
            (2.5_f64, 5.0, 10.0, 1.020_022_766_442_697_7e-1),
            (5.0_f64, 5.0, 10.0, 1.486_880_040_811_298_2e-2),
            (20.0_f64, 5.0, 10.0, 6.472_349_610_750_509e-5),
            (200.0_f64, 5.0, 10.0, 1.102_329_910_570_809e-9),
            (0.05_f64, 5.0, 30.0, 9.983_102_236_198_341e-1),
            (0.5_f64, 5.0, 30.0, 7.737_335_937_035_952e-1),
            (1.0_f64, 5.0, 30.0, 4.346_488_763_398_733e-1),
            (2.5_f64, 5.0, 30.0, 5.244_410_472_445_027_6e-2),
            (5.0_f64, 5.0, 30.0, 1.896_967_469_088_041_7e-3),
            (20.0_f64, 5.0, 30.0, 9.581_174_502_890_943e-9),
            (200.0_f64, 5.0, 30.0, 4.352_520_121_551_445e-22),
            (0.05_f64, 5.0, 100.0, 9.984_286_736_809_271e-1),
            (0.5_f64, 5.0, 100.0, 7.755_895_192_354_815e-1),
            (1.0_f64, 5.0, 100.0, 4.218_298_943_719_714_4e-1),
            (2.5_f64, 5.0, 100.0, 3.544_824_945_021_782e-2),
            (5.0_f64, 5.0, 100.0, 3.953_080_102_324_142_3e-4),
            (20.0_f64, 5.0, 100.0, 8.923_890_603_183_405e-14),
            (200.0_f64, 5.0, 100.0, 2.043_831_493_329_358_6e-50),
            (0.05_f64, 5.0, 500.0, 9.984_690_929_254_346e-1),
            (0.5_f64, 5.0, 500.0, 7.763_082_744_903_42e-1),
            (1.0_f64, 5.0, 500.0, 4.170_943_401_351_524e-1),
            (2.5_f64, 5.0, 500.0, 2.989_775_590_717_396_3e-2),
            (5.0_f64, 5.0, 500.0, 1.764_704_885_541_849_8e-4),
            (20.0_f64, 5.0, 500.0, 3.364_299_050_353_227e-18),
            (200.0_f64, 5.0, 500.0, 8.577_660_241_290_851e-117),
            (0.05_f64, 7.0, 1.0, 9.971_065_048_793_928e-1),
            (0.5_f64, 7.0, 1.0, 7.997_999_258_337_599e-1),
            (1.0_f64, 7.0, 1.0, 6.493_833_371_797_926e-1),
            (2.5_f64, 7.0, 1.0, 4.528_161_585_563_629e-1),
            (5.0_f64, 7.0, 1.0, 3.317_689_599_284_96e-1),
            (20.0_f64, 7.0, 1.0, 1.705_510_553_619_384_6e-1),
            (200.0_f64, 7.0, 1.0, 5.439_421_527_116_384e-2),
            (0.05_f64, 7.0, 3.0, 9.991_774_207_533_924e-1),
            (0.5_f64, 7.0, 3.0, 7.973_063_575_133_491e-1),
            (1.0_f64, 7.0, 3.0, 5.529_203_865_315_162e-1),
            (2.5_f64, 7.0, 3.0, 2.424_975_282_447_001e-1),
            (5.0_f64, 7.0, 3.0, 1.068_320_443_375_101_9e-1),
            (20.0_f64, 7.0, 3.0, 1.599_368_435_201_107_8e-2),
            (200.0_f64, 7.0, 3.0, 5.354_274_210_490_03e-4),
            (0.05_f64, 7.0, 5.0, 9.994_860_689_672_468e-1),
            (0.5_f64, 7.0, 5.0, 8.043_267_551_331_769e-1),
            (1.0_f64, 7.0, 5.0, 5.187_060_870_417_941e-1),
            (2.5_f64, 7.0, 5.0, 1.651_665_724_962_968_2e-1),
            (5.0_f64, 7.0, 5.0, 4.758_354_977_857_251_5e-2),
            (20.0_f64, 7.0, 5.0, 2.254_254_330_650_468e-3),
            (200.0_f64, 7.0, 5.0, 8.156_429_888_424_49e-6),
            (0.05_f64, 7.0, 7.0, 9.996_005_787_903_773e-1),
            (0.5_f64, 7.0, 7.0, 8.096_434_091_615_682e-1),
            (1.0_f64, 7.0, 7.0, 4.999_999_999_999_998_3e-1),
            (2.5_f64, 7.0, 7.0, 1.248_783_321_232_528_3e-1),
            (5.0_f64, 7.0, 7.0, 2.493_361_528_444_257_3e-2),
            (20.0_f64, 7.0, 7.0, 3.994_212_096_226_762_7e-4),
            (200.0_f64, 7.0, 7.0, 1.602_201_087_428_974_8e-7),
            (0.05_f64, 7.0, 10.0, 9.996_785_729_851_23e-1),
            (0.5_f64, 7.0, 10.0, 8.150_431_465_936_292e-1),
            (1.0_f64, 7.0, 10.0, 4.834_025_108_823_465e-1),
            (2.5_f64, 7.0, 10.0, 9.167_374_844_440_98e-2),
            (5.0_f64, 7.0, 10.0, 1.146_120_704_541_107_4e-2),
            (20.0_f64, 7.0, 10.0, 4.017_926_806_906_255e-5),
            (200.0_f64, 7.0, 10.0, 6.221_655_538_495_649e-10),
            (0.05_f64, 7.0, 30.0, 9.997_853_615_351_379e-1),
            (0.5_f64, 7.0, 30.0, 8.269_609_583_211_951e-1),
            (1.0_f64, 7.0, 30.0, 4.505_706_879_613_486e-1),
            (2.5_f64, 7.0, 30.0, 3.777_766_128_993_187_4e-2),
            (5.0_f64, 7.0, 30.0, 7.711_805_851_010_107e-4),
            (20.0_f64, 7.0, 30.0, 1.100_187_450_229_647_2e-9),
            (200.0_f64, 7.0, 30.0, 2.202_044_443_436_217_8e-23),
            (0.05_f64, 7.0, 100.0, 9.998_183_096_039_113e-1),
            (0.5_f64, 7.0, 100.0, 8.325_416_801_853_718e-1),
            (1.0_f64, 7.0, 100.0, 4.359_080_275_908_213_7e-1),
            (2.5_f64, 7.0, 100.0, 2.075_541_485_050_993e-2),
            (5.0_f64, 7.0, 100.0, 6.953_028_285_653_315e-5),
            (20.0_f64, 7.0, 100.0, 1.522_620_759_402_403_3e-16),
            (200.0_f64, 7.0, 100.0, 7.676_461_532_132_527e-56),
            (0.05_f64, 7.0, 500.0, 9.998_290_563_503_065e-1),
            (0.5_f64, 7.0, 500.0, 8.346_733_652_780_742e-1),
            (1.0_f64, 7.0, 500.0, 4.303_267_111_651_259e-1),
            (2.5_f64, 7.0, 500.0, 1.564_022_931_445_676_8e-2),
            (5.0_f64, 7.0, 500.0, 1.724_493_926_108_725_2e-5),
            (20.0_f64, 7.0, 500.0, 1.105_913_154_993_826_2e-23),
            (200.0_f64, 7.0, 500.0, 1.602_972_532_619_707_2e-140),
            (0.05_f64, 10.0, 1.0, 9.988_065_331_699_797e-1),
            (0.5_f64, 10.0, 1.0, 8.123_301_291_303_971e-1),
            (1.0_f64, 10.0, 1.0, 6.591_068_676_979_402e-1),
            (2.5_f64, 10.0, 1.0, 4.587_204_829_251_881e-1),
            (5.0_f64, 10.0, 1.0, 3.357_485_282_688_647_5e-1),
            (20.0_f64, 10.0, 1.0, 1.724_348_407_990_325e-1),
            (200.0_f64, 10.0, 1.0, 5.497_784_197_229_169e-2),
            (0.05_f64, 10.0, 3.0, 9.998_488_349_734_14e-1),
            (0.5_f64, 10.0, 3.0, 8.219_925_926_248_246e-1),
            (1.0_f64, 10.0, 3.0, 5.676_627_969_783_027e-1),
            (2.5_f64, 10.0, 3.0, 2.439_174_274_603_792_3e-1),
            (5.0_f64, 10.0, 3.0, 1.059_818_885_792_791_2e-1),
            (20.0_f64, 10.0, 3.0, 1.564_506_429_592_112_5e-2),
            (200.0_f64, 10.0, 3.0, 5.211_599_742_799_054e-4),
            (0.05_f64, 10.0, 5.0, 9.999_352_765_038_925e-1),
            (0.5_f64, 10.0, 5.0, 8.358_050_491_002_612e-1),
            (1.0_f64, 10.0, 5.0, 5.348_805_734_621_997e-1),
            (2.5_f64, 10.0, 5.0, 1.618_347_415_219_576_2e-1),
            (5.0_f64, 10.0, 5.0, 4.480_822_975_357_040_5e-2),
            (20.0_f64, 10.0, 5.0, 2.032_018_068_584_613e-3),
            (200.0_f64, 10.0, 5.0, 7.234_158_868_087_465e-6),
            (0.05_f64, 10.0, 7.0, 9.999_598_207_319_309e-1),
            (0.5_f64, 10.0, 7.0, 8.454_966_997_285_922e-1),
            (1.0_f64, 10.0, 7.0, 5.165_974_891_176_535e-1),
            (2.5_f64, 10.0, 7.0, 1.183_060_618_644_493_1e-1),
            (5.0_f64, 10.0, 7.0, 2.191_557_852_489_411_6e-2),
            (20.0_f64, 10.0, 7.0, 3.214_270_148_770_766e-4),
            (200.0_f64, 10.0, 7.0, 1.246_086_026_245_263_5e-7),
            (0.05_f64, 10.0, 10.0, 9.999_737_541_172_163e-1),
            (0.5_f64, 10.0, 10.0, 8.551_541_939_744_957e-1),
            (1.0_f64, 10.0, 10.0, 5.000_000_000_000_001e-1),
            (2.5_f64, 10.0, 10.0, 8.225_366_322_272_007e-2),
            (5.0_f64, 10.0, 10.0, 8.950_061_601_381_901e-3),
            (20.0_f64, 10.0, 10.0, 2.624_588_278_370_414_6e-5),
            (200.0_f64, 10.0, 10.0, 3.777_237_977_079_334e-10),
            (0.05_f64, 10.0, 30.0, 9.999_886_383_739_246e-1),
            (0.5_f64, 10.0, 30.0, 8.763_612_630_739_956e-1),
            (1.0_f64, 10.0, 30.0, 4.654_242_904_944_112e-1),
            (2.5_f64, 10.0, 30.0, 2.556_200_682_455_132e-2),
            (5.0_f64, 10.0, 30.0, 2.814_713_926_752_866e-4),
            (20.0_f64, 10.0, 30.0, 1.238_523_030_227_409_6e-10),
            (200.0_f64, 10.0, 30.0, 1.283_889_691_329_897_6e-24),
            (0.05_f64, 10.0, 100.0, 9.999_921_314_059_984e-1),
            (0.5_f64, 10.0, 100.0, 8.863_503_911_530_737e-1),
            (1.0_f64, 10.0, 100.0, 4.488_172_795_604_992e-1),
            (2.5_f64, 10.0, 100.0, 1.009_520_838_004_780_5e-2),
            (5.0_f64, 10.0, 100.0, 7.203_294_285_246_187e-6),
            (20.0_f64, 10.0, 100.0, 9.052_974_736_110_621e-20),
            (200.0_f64, 10.0, 100.0, 2.023_087_478_925_676_3e-61),
            (0.05_f64, 10.0, 500.0, 9.999_931_479_117_107e-1),
            (0.5_f64, 10.0, 500.0, 8.901_834_974_380_145e-1),
            (1.0_f64, 10.0, 500.0, 4.422_289_394_076_182_6e-1),
            (2.5_f64, 10.0, 500.0, 6.181_310_892_451_748e-3),
            (5.0_f64, 10.0, 500.0, 6.122_346_115_150_638e-7),
            (20.0_f64, 10.0, 500.0, 3.451_499_099_408_449_6e-31),
            (200.0_f64, 10.0, 500.0, 1.260_108_905_566_712_7e-167),
            (0.05_f64, 20.0, 1.0, 9.997_665_525_652_514e-1),
            (0.5_f64, 20.0, 1.0, 8.273_216_960_128_282e-1),
            (1.0_f64, 20.0, 1.0, 6.707_434_228_282_909e-1),
            (2.5_f64, 20.0, 1.0, 4.657_462_675_288_914e-1),
            (5.0_f64, 20.0, 1.0, 3.404_734_867_967_272_3e-1),
            (20.0_f64, 20.0, 1.0, 1.746_685_268_748_851_8e-1),
            (200.0_f64, 20.0, 1.0, 5.566_968_855_192_703e-2),
            (0.05_f64, 20.0, 3.0, 9.999_968_983_502_592e-1),
            (0.5_f64, 20.0, 3.0, 8.535_611_969_133_787e-1),
            (1.0_f64, 20.0, 3.0, 5.867_480_859_375_398e-1),
            (2.5_f64, 20.0, 3.0, 2.455_033_268_956_120_8e-1),
            (5.0_f64, 20.0, 3.0, 1.048_551_121_300_250_3e-1),
            (20.0_f64, 20.0, 3.0, 1.521_997_452_120_014_3e-2),
            (200.0_f64, 20.0, 3.0, 5.040_490_534_002_49e-4),
            (0.05_f64, 20.0, 5.0, 9.999_996_332_912_71e-1),
            (0.5_f64, 20.0, 5.0, 8.774_927_553_181_574e-1),
            (1.0_f64, 20.0, 5.0, 5.569_748_153_151_206e-1),
            (2.5_f64, 20.0, 5.0, 1.569_969_392_108_339e-1),
            (5.0_f64, 20.0, 5.0, 4.128_835_849_042_164e-2),
            (20.0_f64, 20.0, 5.0, 1.774_850_437_874_341_7e-3),
            (200.0_f64, 20.0, 5.0, 6.198_937_506_367_039e-6),
            (0.05_f64, 20.0, 7.0, 9.999_999_022_681_04e-1),
            (0.5_f64, 20.0, 7.0, 8.939_292_004_628_931e-1),
            (1.0_f64, 20.0, 7.0, 5.401_531_821_063_319e-1),
            (2.5_f64, 20.0, 7.0, 1.089_237_236_633_195_4e-1),
            (5.0_f64, 20.0, 7.0, 1.823_902_806_509_197_8e-2),
            (20.0_f64, 20.0, 7.0, 2.396_075_955_539_662_8e-4),
            (200.0_f64, 20.0, 7.0, 8.920_495_843_834_883e-8),
            (0.05_f64, 20.0, 10.0, 9.999_999_726_576_813e-1),
            (0.5_f64, 20.0, 10.0, 9.102_172_851_562_5e-1),
            (1.0_f64, 20.0, 10.0, 5.244_995_315_671_086e-1),
            (2.5_f64, 20.0, 10.0, 6.897_514_678_237_858e-2),
            (5.0_f64, 20.0, 10.0, 6.161_513_123_423_422e-3),
            (20.0_f64, 20.0, 10.0, 1.437_064_754_200_605_2e-5),
            (200.0_f64, 20.0, 10.0, 1.895_016_442_662_043_3e-10),
            (0.05_f64, 20.0, 30.0, 9.999_999_984_214_671e-1),
            (0.5_f64, 20.0, 30.0, 9.453_350_776_055_7e-1),
            (1.0_f64, 20.0, 30.0, 4.890_801_931_489_531e-1),
            (2.5_f64, 20.0, 30.0, 1.132_846_370_347_176_2e-2),
            (5.0_f64, 20.0, 30.0, 4.132_575_555_705_573e-5),
            (20.0_f64, 20.0, 30.0, 3.214_720_610_769_998e-12),
            (200.0_f64, 20.0, 30.0, 1.466_612_714_077_889e-26),
            (0.05_f64, 20.0, 100.0, 9.999_999_996_344_979e-1),
            (0.5_f64, 20.0, 100.0, 9.610_073_391_512_228e-1),
            (1.0_f64, 20.0, 100.0, 4.692_089_598_227_433e-1),
            (2.5_f64, 20.0, 100.0, 1.499_776_663_571_476_4e-3),
            (5.0_f64, 20.0, 100.0, 2.632_416_051_112_641_8e-8),
            (20.0_f64, 20.0, 100.0, 1.985_975_732_417_538e-26),
            (200.0_f64, 20.0, 100.0, 2.319_515_191_433_788e-71),
            (0.05_f64, 20.0, 500.0, 9.999_999_997_993_518e-1),
            (0.5_f64, 20.0, 500.0, 9.667_241_763_563_634e-1),
            (1.0_f64, 20.0, 500.0, 4.603_768_778_460_684_6e-1),
            (2.5_f64, 20.0, 500.0, 3.614_471_679_365_32e-4),
            (5.0_f64, 20.0, 500.0, 2.419_896_115_024_529_5e-11),
            (20.0_f64, 20.0, 500.0, 1.351_873_364_707_244e-51),
            (200.0_f64, 20.0, 500.0, 1.201_843_864_451_444_3e-223),
            (0.05_f64, 50.0, 1.0, 9.999_553_080_801_258e-1),
            (0.5_f64, 50.0, 1.0, 8.365_038_281_044_941e-1),
            (1.0_f64, 50.0, 1.0, 6.778_743_548_997_557e-1),
            (2.5_f64, 50.0, 1.0, 4.700_290_002_323_09e-1),
            (5.0_f64, 50.0, 1.0, 3.433_477_708_108_625_4e-1),
            (20.0_f64, 50.0, 1.0, 1.760_256_107_265_665_6e-1),
            (200.0_f64, 50.0, 1.0, 5.608_990_729_137_773e-2),
            (0.05_f64, 50.0, 3.0, 9.999_999_881_840_26e-1),
            (0.5_f64, 50.0, 3.0, 8.740_579_417_973_806e-1),
            (1.0_f64, 50.0, 3.0, 5.993_767_746_957_155e-1),
            (2.5_f64, 50.0, 3.0, 2.464_112_141_782_734e-1),
            (5.0_f64, 50.0, 3.0, 1.041_029_960_001_185_8e-1),
            (20.0_f64, 50.0, 3.0, 1.495_494_664_972_292_7e-2),
            (200.0_f64, 50.0, 3.0, 4.935_306_370_114_788e-4),
            (0.05_f64, 50.0, 5.0, 9.999_999_999_331_19e-1),
            (0.5_f64, 50.0, 5.0, 9.052_470_979_699_002e-1),
            (1.0_f64, 50.0, 5.0, 5.725_055_335_833_61e-1),
            (2.5_f64, 50.0, 5.0, 1.534_912_727_528_461_3e-1),
            (5.0_f64, 50.0, 5.0, 3.901_818_940_991_427e-2),
            (20.0_f64, 50.0, 5.0, 1.621_991_784_701_261_1e-3),
            (200.0_f64, 50.0, 5.0, 5.599_778_288_513_24e-6),
            (0.05_f64, 50.0, 7.0, 9.999_999_999_982_813e-1),
            (0.5_f64, 50.0, 7.0, 9.263_825_817_270_023e-1),
            (1.0_f64, 50.0, 7.0, 5.575_369_842_173_838e-1),
            (2.5_f64, 50.0, 7.0, 1.021_853_712_134_265_7e-1),
            (5.0_f64, 50.0, 7.0, 1.596_566_743_101_228_8e-2),
            (20.0_f64, 50.0, 7.0, 1.955_206_109_766_565e-4),
            (200.0_f64, 50.0, 7.0, 7.102_024_313_914_767e-8),
            (0.05_f64, 50.0, 10.0, 9.999_999_999_999_66e-1),
            (0.5_f64, 50.0, 10.0, 9.468_123_117_221_907e-1),
            (1.0_f64, 50.0, 10.0, 5.436_430_945_196_307e-1),
            (2.5_f64, 50.0, 10.0, 5.958_672_931_842_764e-2),
            (5.0_f64, 50.0, 10.0, 4.615_101_165_139_315e-3),
            (20.0_f64, 50.0, 10.0, 9.266_790_041_201_036e-6),
            (200.0_f64, 50.0, 10.0, 1.158_252_031_677_620_7e-10),
            (0.05_f64, 50.0, 30.0, 9.999_999_999_999_999e-1),
            (0.5_f64, 50.0, 30.0, 9.852_667_345_044_902e-1),
            (1.0_f64, 50.0, 30.0, 5.108_974_983_279_239e-1),
            (2.5_f64, 50.0, 30.0, 4.393_558_041_015_697_4e-3),
            (5.0_f64, 50.0, 30.0, 5.660_692_594_295_936e-6),
            (20.0_f64, 50.0, 30.0, 1.192_477_741_574_441_5e-13),
            (200.0_f64, 50.0, 30.0, 3.224_186_055_438_517_6e-28),
            (0.05_f64, 50.0, 100.0, 9.999_999_999_999_999e-1),
            (0.5_f64, 50.0, 100.0, 9.961_734_860_330_544e-1),
            (1.0_f64, 50.0, 100.0, 4.891_205_078_079_293e-1),
            (2.5_f64, 50.0, 100.0, 5.068_369_209_338_273_5e-5),
            (5.0_f64, 50.0, 100.0, 4.192_942_714_416_357e-12),
            (20.0_f64, 50.0, 100.0, 1.590_676_925_542_226_2e-34),
            (200.0_f64, 50.0, 100.0, 8.434_051_290_315_383e-82),
            (0.05_f64, 50.0, 500.0, 9.999_999_999_999_999e-1),
            (0.5_f64, 50.0, 500.0, 9.984_090_605_193_361e-1),
            (1.0_f64, 50.0, 500.0, 4.771_617_689_801_394e-1),
            (2.5_f64, 50.0, 500.0, 2.900_216_183_887_641_3e-7),
            (5.0_f64, 50.0, 500.0, 7.588_419_680_838_374e-22),
            (20.0_f64, 50.0, 500.0, 5.990_923_934_343_106e-90),
            (200.0_f64, 50.0, 500.0, 1.590_323_134_507_893_6e-297),
        ];
        for &(f, d1, d2, expected) in SCIPY {
            let got = f_distribution_sf(f, d1, d2);
            let tol = 1e-11_f64.max(expected.abs() * 1e-9);
            assert!(
                (got - expected).abs() < tol,
                "F({d1},{d2}) sf({f}): got {got:.17e}, scipy {expected:.17e}"
            );
        }
    }
}
