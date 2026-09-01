//! Multivariate anomaly detection: Mahalanobis distance, Isolation Forest,
//! and PCA reconstruction error.

use crate::compute::simd_dot_product;
use rand::Rng;

use crate::multivariate::context::MultiSeriesContext;
use crate::multivariate::error::MultivariateError;

/// Score for a multivariate anomaly at a given timestamp.
#[derive(Debug, Clone)]
pub struct MultivariateAnomalyScore {
    /// Timestamp (nanoseconds since epoch) of the observation.
    pub timestamp: i64,
    /// Anomaly score (higher = more anomalous).
    pub score: f64,
    /// Whether this point exceeds the anomaly threshold.
    pub is_anomaly: bool,
    /// Per-series contribution to the score (explainability).
    pub contributions: Vec<(String, f64)>,
}

/// Multivariate anomaly detection contract.
pub trait MultivariateAnomalyDetector: Send + Sync {
    /// Fit the detector to the multi-series context.
    fn fit(&mut self, ctx: &MultiSeriesContext) -> Result<(), MultivariateError>;
    /// Detect anomalies and return per-timestamp scores.
    fn detect(
        &self,
        ctx: &MultiSeriesContext,
    ) -> Result<Vec<MultivariateAnomalyScore>, MultivariateError>;
}

// ─── Mahalanobis ────────────────────────────────────────────────────

/// Multivariate anomaly detection via Mahalanobis distance.
///
/// Points with distance > `threshold` are flagged.
pub struct MahalanobisDetector {
    threshold: f64,
    epsilon: f64,
    means: Vec<f64>,
    /// Inverse covariance matrix (k × k flattened row-major).
    inv_cov: Vec<f64>,
    k: usize,
    fitted: bool,
}

impl MahalanobisDetector {
    /// Creates a new detector with the given threshold and regularisation epsilon.
    pub fn new(threshold: Option<f64>, epsilon: Option<f64>) -> Self {
        Self {
            threshold: threshold.unwrap_or(3.0),
            epsilon: epsilon.unwrap_or(1e-6),
            means: Vec::new(),
            inv_cov: Vec::new(),
            k: 0,
            fitted: false,
        }
    }

    fn compute_covariance(data: &[Vec<f64>], means: &[f64]) -> Vec<f64> {
        let k = data.len();
        let n = data[0].len();
        let divisor = if n > 1 { (n - 1) as f64 } else { 1.0 };

        // Pre-compute mean-centred vectors for SIMD dot products.
        let centered: Vec<Vec<f64>> = data
            .iter()
            .zip(means.iter())
            .map(|(col, &m)| col.iter().map(|&v| v - m).collect())
            .collect();

        let mut cov = vec![0.0; k * k];
        for i in 0..k {
            for j in i..k {
                // Use SIMD-accelerated dot product instead of
                // scalar loop for the inner O(n) summation.
                // Both slices have identical length `n`, so the call cannot fail.
                let s = simd_dot_product(&centered[i], &centered[j])
                    .expect("centered vectors have equal length");
                cov[i * k + j] = s / divisor;
                cov[j * k + i] = cov[i * k + j];
            }
        }
        cov
    }

    /// Invert the k×k covariance matrix using LU decomposition.
    ///
    /// Single LU factorisation O(k³) + k back-substitutions
    /// O(k²) each = O(k³) total, replacing the previous O(k⁴) approach
    /// that called batch_matrix_solve k separate times.
    fn invert_matrix(cov: &[f64], k: usize) -> Result<Vec<f64>, MultivariateError> {
        // LU decomposition with partial pivoting (in-place on a copy).
        let mut lu = cov.to_vec();
        let mut piv: Vec<usize> = (0..k).collect();

        for col in 0..k {
            // Find pivot.
            let mut max_val = lu[piv[col] * k + col].abs();
            let mut max_row = col;
            for row in (col + 1)..k {
                let v = lu[piv[row] * k + col].abs();
                if v > max_val {
                    max_val = v;
                    max_row = row;
                }
            }
            if max_val < 1e-15 {
                return Err(MultivariateError::Compute(
                    "covariance matrix is singular".into(),
                ));
            }
            piv.swap(col, max_row);

            let pivot_row = piv[col];
            for row in (col + 1)..k {
                let target_row = piv[row];
                let factor = lu[target_row * k + col] / lu[pivot_row * k + col];
                lu[target_row * k + col] = factor; // Store L factor in lower part.
                for j in (col + 1)..k {
                    lu[target_row * k + j] -= factor * lu[pivot_row * k + j];
                }
            }
        }

        // Solve for each column of the identity matrix.
        let mut inv = vec![0.0; k * k];
        for col in 0..k {
            // Forward substitution (Ly = Pb).
            let mut y = vec![0.0; k];
            for i in 0..k {
                let pi = piv[i];
                let b_val = if pi == col { 1.0 } else { 0.0 };
                let mut sum = b_val;
                for j in 0..i {
                    sum -= lu[pi * k + j] * y[j];
                }
                // L diagonal is implicitly 1 — no division needed.
                y[i] = sum;
            }

            // Back substitution (Ux = y).
            for i in (0..k).rev() {
                let pi = piv[i];
                let mut sum = y[i];
                for j in (i + 1)..k {
                    sum -= lu[pi * k + j] * inv[j * k + col];
                }
                inv[i * k + col] = sum / lu[pi * k + i];
            }
        }
        Ok(inv)
    }

    fn mahalanobis_distance(&self, point: &[f64]) -> f64 {
        let k = self.k;
        // d² = (x-μ)ᵀ Σ⁻¹ (x-μ)
        let diff: Vec<f64> = point
            .iter()
            .zip(self.means.iter())
            .map(|(a, b)| a - b)
            .collect();
        let mut d2 = 0.0;
        for i in 0..k {
            let mut row_sum = 0.0;
            #[allow(clippy::needless_range_loop)]
            for j in 0..k {
                row_sum += self.inv_cov[i * k + j] * diff[j];
            }
            d2 += diff[i] * row_sum;
        }
        d2.max(0.0).sqrt()
    }
}

impl MultivariateAnomalyDetector for MahalanobisDetector {
    fn fit(&mut self, ctx: &MultiSeriesContext) -> Result<(), MultivariateError> {
        let k = ctx.matrix.n_series();
        let n = ctx.matrix.n_timestamps();
        if n < k + 1 {
            return Err(MultivariateError::InsufficientData { min: k + 1, got: n });
        }

        self.k = k;
        self.means = ctx
            .matrix
            .data
            .iter()
            .map(|s| s.iter().sum::<f64>() / s.len() as f64)
            .collect();

        let mut cov = Self::compute_covariance(&ctx.matrix.data, &self.means);
        // Regularize
        for i in 0..k {
            cov[i * k + i] += self.epsilon;
        }
        self.inv_cov = Self::invert_matrix(&cov, k)?;
        self.fitted = true;
        Ok(())
    }

    fn detect(
        &self,
        ctx: &MultiSeriesContext,
    ) -> Result<Vec<MultivariateAnomalyScore>, MultivariateError> {
        if !self.fitted {
            return Err(MultivariateError::NotFitted);
        }
        let _start = std::time::Instant::now();
        let k = ctx.matrix.n_series();
        if k != self.k {
            return Err(MultivariateError::InsufficientData {
                min: self.k,
                got: k,
            });
        }
        let n = ctx.matrix.n_timestamps();
        let mut scores = Vec::with_capacity(n);

        for t in 0..n {
            let point: Vec<f64> = (0..k).map(|s| ctx.matrix.data[s][t]).collect();
            let dist = self.mahalanobis_distance(&point);
            let is_anomaly = dist > self.threshold;

            // Per-series contribution: |diff_i| / dist
            let contributions: Vec<(String, f64)> = (0..k)
                .map(|s| {
                    let diff = (point[s] - self.means[s]).abs();
                    let contribution = if dist > 1e-15 { diff / dist } else { 0.0 };
                    (ctx.matrix.series_ids[s].clone(), contribution)
                })
                .collect();

            scores.push(MultivariateAnomalyScore {
                timestamp: ctx.matrix.timestamps[t],
                score: dist,
                is_anomaly,
                contributions,
            });
        }
        metrics::histogram!("chronix_multivariate_anomaly_detect_duration_seconds")
            .record(_start.elapsed().as_secs_f64());
        Ok(scores)
    }
}

// ─── Isolation Forest ───────────────────────────────────────────────

/// A single isolation tree node.
enum IsolationNode {
    External {
        size: usize,
    },
    Internal {
        split_feature: usize,
        split_value: f64,
        left: Box<IsolationNode>,
        right: Box<IsolationNode>,
    },
}

/// Isolation Forest multivariate anomaly detector.
///
/// Recursively partitions data along random features and split values.
/// Anomalous points are isolated in fewer splits (shorter path length).
/// Score is normalized to [0, 1] where values close to 1 indicate anomalies.
///
/// # Deterministic Seeding
///
/// The forest uses a **fixed base seed** (42) combined with the tree
/// index to derive per-tree PRNG streams. This makes results fully
/// deterministic for a given dataset, which is valuable for
/// reproducibility and testing.
///
/// The seed does **not** incorporate data shape (n_series, n_timestamps)
/// because the subsample selection and split decisions are already
/// conditioned on the actual data values. Adding shape to the seed
/// would change results when the same logical dataset is padded or
/// truncated, without improving detection quality.
///
/// If caller-controlled seeding is needed (e.g. for ensemble
/// diversity), a `seed` parameter can be added to the constructor
/// in a future release.
pub struct IsolationForestDetector {
    n_trees: usize,
    subsample_size: usize,
    /// Actual subsample size used during fit (may be smaller than `subsample_size`
    /// when the dataset has fewer points).
    actual_subsample: usize,
    threshold: f64,
    seed: u64,
    trees: Vec<IsolationNode>,
    k: usize,
    fitted: bool,
}

impl IsolationForestDetector {
    /// Creates a new Isolation Forest detector with optional parameters.
    pub fn new(
        n_trees: Option<usize>,
        subsample_size: Option<usize>,
        threshold: Option<f64>,
    ) -> Self {
        Self {
            n_trees: n_trees.unwrap_or(100).max(1),
            subsample_size: subsample_size.unwrap_or(256).max(1),
            actual_subsample: 0,
            threshold: threshold.unwrap_or(0.6),
            seed: 42,
            trees: Vec::new(),
            k: 0,
            fitted: false,
        }
    }

    /// Sets the seed for deterministic PRNG. Default is 42.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Average path length of unsuccessful search in BST of n elements.
    fn avg_path_length(n: usize) -> f64 {
        if n <= 1 {
            return 0.0;
        }
        let n = n as f64;
        2.0 * (n.ln() + 0.5772156649) - 2.0 * (n - 1.0) / n
    }

    /// Simple deterministic pseudo-random from seed.
    fn splitmix64(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }

    fn build_tree(
        data: &[Vec<f64>],
        indices: &[usize],
        k: usize,
        depth: usize,
        max_depth: usize,
        rng: &mut u64,
    ) -> IsolationNode {
        if indices.len() <= 1 || depth >= max_depth {
            return IsolationNode::External {
                size: indices.len(),
            };
        }

        // Pick random feature
        let feature = (Self::splitmix64(rng) as usize) % k;

        // Find min/max along feature
        let mut fmin = f64::INFINITY;
        let mut fmax = f64::NEG_INFINITY;
        for &idx in indices {
            let v = data[feature][idx];
            if v < fmin {
                fmin = v;
            }
            if v > fmax {
                fmax = v;
            }
        }

        if (fmax - fmin).abs() < 1e-15 {
            return IsolationNode::External {
                size: indices.len(),
            };
        }

        // Random split value in [fmin, fmax)
        let r = (Self::splitmix64(rng) as f64) / (u64::MAX as f64);
        let split_value = fmin + r * (fmax - fmin);

        let mut left_idx = Vec::new();
        let mut right_idx = Vec::new();
        for &idx in indices {
            if data[feature][idx] < split_value {
                left_idx.push(idx);
            } else {
                right_idx.push(idx);
            }
        }

        // Guard against degenerate splits
        if left_idx.is_empty() || right_idx.is_empty() {
            return IsolationNode::External {
                size: indices.len(),
            };
        }

        IsolationNode::Internal {
            split_feature: feature,
            split_value,
            left: Box::new(Self::build_tree(
                data,
                &left_idx,
                k,
                depth + 1,
                max_depth,
                rng,
            )),
            right: Box::new(Self::build_tree(
                data,
                &right_idx,
                k,
                depth + 1,
                max_depth,
                rng,
            )),
        }
    }

    fn path_length(node: &IsolationNode, point: &[f64], depth: usize) -> f64 {
        match node {
            IsolationNode::External { size } => depth as f64 + Self::avg_path_length(*size),
            IsolationNode::Internal {
                split_feature,
                split_value,
                left,
                right,
            } => {
                if point[*split_feature] < *split_value {
                    Self::path_length(left, point, depth + 1)
                } else {
                    Self::path_length(right, point, depth + 1)
                }
            }
        }
    }

    /// Traverse the isolation tree and accumulate per-feature split
    /// counts weighted by depth.  Splits closer to the root (lower
    /// depth) are more influential in isolating the point, so each
    /// split contributes `1 / (1 + depth)` to its feature.  This
    /// mirrors the Mean Decrease in Impurity concept from Random
    /// Forests adapted for Isolation Forest path structure.
    fn path_feature_importance(
        node: &IsolationNode,
        point: &[f64],
        depth: usize,
        importance: &mut [f64],
    ) {
        match node {
            IsolationNode::External { .. } => {}
            IsolationNode::Internal {
                split_feature,
                split_value,
                left,
                right,
            } => {
                // Depth-weighted contribution: early splits matter more
                importance[*split_feature] += 1.0 / (1 + depth) as f64;
                if point[*split_feature] < *split_value {
                    Self::path_feature_importance(left, point, depth + 1, importance);
                } else {
                    Self::path_feature_importance(right, point, depth + 1, importance);
                }
            }
        }
    }

    fn anomaly_score_from_path(&self, avg_path: f64) -> f64 {
        let c = Self::avg_path_length(self.actual_subsample);
        if c <= 0.0 {
            return 0.5;
        }
        2.0_f64.powf(-avg_path / c)
    }
}

impl MultivariateAnomalyDetector for IsolationForestDetector {
    fn fit(&mut self, ctx: &MultiSeriesContext) -> Result<(), MultivariateError> {
        let k = ctx.matrix.n_series();
        let n = ctx.matrix.n_timestamps();
        if n < 2 {
            return Err(MultivariateError::InsufficientData { min: 2, got: n });
        }

        self.k = k;
        let sub = self.subsample_size.min(n);
        self.actual_subsample = sub;
        // Cap max_depth to prevent stack overflow on pathological inputs.
        const ABSOLUTE_MAX_DEPTH: usize = 64;
        let max_depth = ((sub as f64).log2().ceil() as usize).min(ABSOLUTE_MAX_DEPTH);

        self.trees.clear();
        self.trees.reserve(self.n_trees);
        for tree_idx in 0..self.n_trees {
            // Derive per-tree seed from a base seed mixed with the tree
            // index so that each tree gets a unique PRNG stream while the
            // overall forest remains deterministic for a given base seed.
            let mut rng: u64 = self
                .seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407)
                .wrapping_add(tree_idx as u64)
                .wrapping_mul(6364136223846793005);

            // Subsample without allocating the full 0..n index vector.
            // Use rejection sampling into a HashSet when sample_size << n.
            let sample_size = sub.min(n);
            let indices: Vec<usize> = if sample_size >= n / 2 {
                // If sampling more than half, Fisher-Yates is more efficient.
                let mut all: Vec<usize> = (0..n).collect();
                for i in 0..sample_size {
                    let j = i + (Self::splitmix64(&mut rng) as usize) % (n - i);
                    all.swap(i, j);
                }
                all.truncate(sample_size);
                all
            } else {
                // Rejection sampling — O(sample_size) expected time.
                let mut selected = std::collections::HashSet::with_capacity(sample_size);
                while selected.len() < sample_size {
                    let idx = (Self::splitmix64(&mut rng) as usize) % n;
                    selected.insert(idx);
                }
                selected.into_iter().collect()
            };

            let tree = Self::build_tree(&ctx.matrix.data, &indices, k, 0, max_depth, &mut rng);
            self.trees.push(tree);
        }

        self.fitted = true;
        Ok(())
    }

    fn detect(
        &self,
        ctx: &MultiSeriesContext,
    ) -> Result<Vec<MultivariateAnomalyScore>, MultivariateError> {
        if !self.fitted {
            return Err(MultivariateError::NotFitted);
        }
        let _start = std::time::Instant::now();
        let k = ctx.matrix.n_series();
        if k != self.k {
            return Err(MultivariateError::InsufficientData {
                min: self.k,
                got: k,
            });
        }
        let n = ctx.matrix.n_timestamps();
        let mut scores = Vec::with_capacity(n);

        for t in 0..n {
            let point: Vec<f64> = (0..k).map(|s| ctx.matrix.data[s][t]).collect();

            // Average path length across all trees
            let avg_path: f64 = self
                .trees
                .iter()
                .map(|tree| Self::path_length(tree, &point, 0))
                .sum::<f64>()
                / self.trees.len() as f64;

            let score = self.anomaly_score_from_path(avg_path);
            let is_anomaly = score > self.threshold;

            // Per-series contribution via depth-weighted split frequency
            // across all trees.  Features split more often (especially
            // near the root) are more responsible for isolating the point.
            let mut importance = vec![0.0; k];
            for tree in &self.trees {
                Self::path_feature_importance(tree, &point, 0, &mut importance);
            }
            // Normalize to sum to 1.0 (relative contribution)
            let total: f64 = importance.iter().sum();
            let contributions: Vec<(String, f64)> = (0..k)
                .map(|s| {
                    let contrib = if total > 1e-15 {
                        importance[s] / total
                    } else {
                        1.0 / k as f64
                    };
                    (ctx.matrix.series_ids[s].clone(), contrib)
                })
                .collect();

            scores.push(MultivariateAnomalyScore {
                timestamp: ctx.matrix.timestamps[t],
                score,
                is_anomaly,
                contributions,
            });
        }
        metrics::histogram!("chronix_multivariate_anomaly_detect_duration_seconds")
            .record(_start.elapsed().as_secs_f64());
        Ok(scores)
    }
}

// ─── PCA Reconstruction Error ───────────────────────────────────────

/// PCA-based multivariate anomaly detection.
///
/// Fits principal components retaining `variance_threshold` of total
/// variance, then flags points with high reconstruction error.
pub struct PcaAnomalyDetector {
    variance_threshold: f64,
    anomaly_threshold: f64,
    means: Vec<f64>,
    /// Principal component vectors (n_components × k, row-major).
    components: Vec<Vec<f64>>,
    k: usize,
    fitted: bool,
}

impl PcaAnomalyDetector {
    /// Creates a new PCA anomaly detector with optional thresholds.
    pub fn new(variance_threshold: Option<f64>, anomaly_threshold: Option<f64>) -> Self {
        Self {
            variance_threshold: variance_threshold.unwrap_or(0.95),
            anomaly_threshold: anomaly_threshold.unwrap_or(3.0),
            means: Vec::new(),
            components: Vec::new(),
            k: 0,
            fitted: false,
        }
    }

    /// Extract top eigenvectors via Randomized SVD (Halko-Martinsson-Tropp).
    ///
    /// For a k×k symmetric positive semi-definite matrix A:
    /// 1. Draw a k × (target+oversampling) Gaussian random matrix Ω
    /// 2. Form Y = A · Ω  (project into random subspace)
    /// 3. QR decomposition: Y = Q·R  (orthonormal basis for range)
    /// 4. Form B = Qᵀ · A · Q  (small matrix)
    /// 5. Eigen-decompose B = V·Λ·Vᵀ  (via Jacobi rotations)
    /// 6. Recover eigenvectors: U = Q · V
    ///
    /// Runs in O(k²·r) where r = target rank, vs O(k²·r·iter) for
    /// iterated power method.  More robust with near-degenerate gaps.
    fn randomized_eigen(
        matrix: &[f64],
        k: usize,
        n_components: usize,
        n_oversampling: usize,
    ) -> (Vec<f64>, Vec<Vec<f64>>) {
        let r = (n_components + n_oversampling).min(k);

        // Step 1: Generate pseudo-random Gaussian matrix Ω (k × r)
        // Use rand crate (ChaCha12 by default) instead of
        // correlated LCG. Box-Muller on cryptographic-quality uniform draws
        // produces independent standard-normal samples.
        let mut omega = vec![0.0; k * r];
        let mut rng = rand::rng();
        for v in omega.iter_mut() {
            // Box-Muller: two uniform draws → one standard normal
            let u1: f64 = rng.random_range(1e-15_f64..1.0_f64); // avoid log(0)
            let u2: f64 = rng.random::<f64>();
            *v = (-2.0_f64 * u1.ln()).sqrt() * (2.0_f64 * std::f64::consts::PI * u2).cos();
        }

        // Step 2: Y = A · Ω  (k × r)
        let mut y = vec![0.0; k * r];
        for i in 0..k {
            for j in 0..r {
                let mut sum = 0.0;
                for l in 0..k {
                    sum += matrix[i * k + l] * omega[l * r + j];
                }
                y[i * r + j] = sum;
            }
        }

        // Power iterations to improve range approximation
        // Y ← A · Y — helps with slowly decaying spectrum
        // Halko et al. (2011) recommend 2-3 iterations
        let n_power_iterations = 2;
        for _ in 0..n_power_iterations {
            let mut tmp = vec![0.0; k * r];
            for i in 0..k {
                for j in 0..r {
                    let mut sum = 0.0;
                    for l in 0..k {
                        sum += matrix[i * k + l] * y[l * r + j];
                    }
                    tmp[i * r + j] = sum;
                }
            }
            y = tmp;
        }

        // Step 3: QR decomposition via modified Gram-Schmidt
        let mut q = vec![0.0; k * r];
        q.copy_from_slice(&y);

        for j in 0..r {
            // Normalize column j
            let mut norm = 0.0;
            for i in 0..k {
                norm += q[i * r + j] * q[i * r + j];
            }
            norm = norm.sqrt();
            if norm < 1e-14 {
                // Degenerate column — zero it out
                for i in 0..k {
                    q[i * r + j] = 0.0;
                }
                continue;
            }
            for i in 0..k {
                q[i * r + j] /= norm;
            }

            // Orthogonalize subsequent columns against column j
            for j2 in (j + 1)..r {
                let mut dot = 0.0;
                for i in 0..k {
                    dot += q[i * r + j] * q[i * r + j2];
                }
                for i in 0..k {
                    q[i * r + j2] -= dot * q[i * r + j];
                }
            }
        }

        // Step 4: B = Qᵀ · A · Q  (r × r symmetric matrix)
        // Validate Q columns for NaN before proceeding to avoid
        // silent NaN propagation through the Jacobi eigensolver.
        if q.iter().any(|v| v.is_nan()) {
            tracing::warn!("PCA: NaN detected in randomized Q matrix — input may be degenerate");
            // Return zero eigenvalues and identity-like eigenvectors for graceful degradation.
            return (vec![0.0; n_components], vec![vec![0.0; k]; n_components]);
        }

        // First compute AQ = A · Q (k × r)
        let mut aq = vec![0.0; k * r];
        for i in 0..k {
            for j in 0..r {
                let mut sum = 0.0;
                for l in 0..k {
                    sum += matrix[i * k + l] * q[l * r + j];
                }
                aq[i * r + j] = sum;
            }
        }
        // Then B = Qᵀ · AQ (r × r)
        let mut b = vec![0.0; r * r];
        for i in 0..r {
            for j in 0..r {
                let mut sum = 0.0;
                for l in 0..k {
                    sum += q[l * r + i] * aq[l * r + j];
                }
                b[i * r + j] = sum;
            }
        }

        // Step 5: Eigen-decompose B via Jacobi eigenvalue algorithm.
        // B is symmetric, so Jacobi rotations converge to diagonal form.
        // Convergence is typically quadratic for well-conditioned matrices;
        // 1000 iterations suffices for matrices up to several hundred in
        // dimension. If the loop exhausts all iterations without reaching
        // the 1e-12 tolerance, a tracing warning is emitted.
        const MAX_JACOBI_ITERATIONS: usize = 1000;

        let mut eigvecs = vec![0.0; r * r]; // identity
        for i in 0..r {
            eigvecs[i * r + i] = 1.0;
        }

        let mut final_max_offdiag = 0.0f64;
        let mut jacobi_iters_used: usize = 0;
        for _ in 0..MAX_JACOBI_ITERATIONS {
            jacobi_iters_used += 1;
            // Find largest off-diagonal element
            let mut max_val = 0.0f64;
            let mut p_idx = 0;
            let mut q_idx = 1;
            for i in 0..r {
                for j in (i + 1)..r {
                    if b[i * r + j].abs() > max_val {
                        max_val = b[i * r + j].abs();
                        p_idx = i;
                        q_idx = j;
                    }
                }
            }
            if max_val < 1e-12 {
                break;
            }
            final_max_offdiag = max_val;

            // Compute Jacobi rotation angle
            let bpp = b[p_idx * r + p_idx];
            let bqq = b[q_idx * r + q_idx];
            let bpq = b[p_idx * r + q_idx];

            let theta = if (bpp - bqq).abs() < 1e-15 {
                std::f64::consts::FRAC_PI_4
            } else {
                0.5 * (2.0 * bpq / (bpp - bqq)).atan()
            };
            let c = theta.cos();
            let s = theta.sin();

            // Apply rotation B ← Gᵀ · B · G
            // Update rows p, q for all columns
            for j in 0..r {
                let bp = b[p_idx * r + j];
                let bq = b[q_idx * r + j];
                b[p_idx * r + j] = c * bp + s * bq;
                b[q_idx * r + j] = -s * bp + c * bq;
            }
            // Update columns p, q for all rows
            for i in 0..r {
                let bp = b[i * r + p_idx];
                let bq = b[i * r + q_idx];
                b[i * r + p_idx] = c * bp + s * bq;
                b[i * r + q_idx] = -s * bp + c * bq;
            }

            // Accumulate eigenvectors
            for i in 0..r {
                let vp = eigvecs[i * r + p_idx];
                let vq = eigvecs[i * r + q_idx];
                eigvecs[i * r + p_idx] = c * vp + s * vq;
                eigvecs[i * r + q_idx] = -s * vp + c * vq;
            }
        }

        // Warn if Jacobi iteration did not converge
        if final_max_offdiag >= 1e-12 {
            tracing::warn!(
                max_offdiag = final_max_offdiag,
                iterations_used = jacobi_iters_used,
                max_iterations = MAX_JACOBI_ITERATIONS,
                matrix_size = r,
                "PCA Jacobi eigenvalue did not converge within {MAX_JACOBI_ITERATIONS} iterations \
                 (remaining off-diag magnitude: {final_max_offdiag:.2e}) — results may be inaccurate"
            );
        }

        // Extract eigenvalues (diagonal of B) and sort descending
        let mut eigen_pairs: Vec<(f64, usize)> = (0..r).map(|i| (b[i * r + i], i)).collect();
        eigen_pairs.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        // Step 6: Recover eigenvectors U = Q · V for top n_components
        let n_out = n_components.min(eigen_pairs.len());
        let mut eigenvalues = Vec::with_capacity(n_out);
        let mut eigenvectors = Vec::with_capacity(n_out);

        for &(eval, idx) in eigen_pairs.iter().take(n_out) {
            if eval < 1e-12 {
                break;
            }
            eigenvalues.push(eval);

            // u = Q · v_idx  (k-dimensional vector)
            let mut u = vec![0.0; k];
            for i in 0..k {
                let mut sum = 0.0;
                for j in 0..r {
                    sum += q[i * r + j] * eigvecs[j * r + idx];
                }
                u[i] = sum;
            }
            // Normalize
            let norm: f64 = u.iter().map(|x| x * x).sum::<f64>().sqrt();
            if norm > 1e-15 {
                for x in &mut u {
                    *x /= norm;
                }
            }
            eigenvectors.push(u);
        }

        (eigenvalues, eigenvectors)
    }

    fn reconstruction_error(&self, point: &[f64]) -> f64 {
        let centered: Vec<f64> = point
            .iter()
            .zip(self.means.iter())
            .map(|(a, b)| a - b)
            .collect();
        // Project onto components and reconstruct
        let mut reconstructed = vec![0.0; self.k];
        for comp in &self.components {
            let proj: f64 = centered.iter().zip(comp.iter()).map(|(a, b)| a * b).sum();
            for (i, &c) in comp.iter().enumerate() {
                reconstructed[i] += proj * c;
            }
        }
        // Reconstruction error = ||centered - reconstructed||
        centered
            .iter()
            .zip(reconstructed.iter())
            .map(|(a, b)| (a - b).powi(2))
            .sum::<f64>()
            .sqrt()
    }
}

impl MultivariateAnomalyDetector for PcaAnomalyDetector {
    fn fit(&mut self, ctx: &MultiSeriesContext) -> Result<(), MultivariateError> {
        let k = ctx.matrix.n_series();
        let n = ctx.matrix.n_timestamps();
        if n < k + 1 {
            return Err(MultivariateError::InsufficientData { min: k + 1, got: n });
        }

        self.k = k;
        self.means = ctx
            .matrix
            .data
            .iter()
            .map(|s| s.iter().sum::<f64>() / s.len() as f64)
            .collect();

        let cov = MahalanobisDetector::compute_covariance(&ctx.matrix.data, &self.means);
        let total_var: f64 = (0..k).map(|i| cov[i * k + i]).sum();

        // Extract top components via Randomized SVD (Halko-Martinsson-Tropp).
        // Request up to k components with 2 extra for oversampling.
        let (eigenvalues, eigenvectors) = Self::randomized_eigen(&cov, k, k, 2);

        self.components.clear();
        let mut explained = 0.0;
        for (i, &eval) in eigenvalues.iter().enumerate() {
            explained += eval;
            self.components.push(eigenvectors[i].clone());
            if explained / total_var >= self.variance_threshold {
                break;
            }
        }

        self.fitted = true;
        Ok(())
    }

    fn detect(
        &self,
        ctx: &MultiSeriesContext,
    ) -> Result<Vec<MultivariateAnomalyScore>, MultivariateError> {
        if !self.fitted {
            return Err(MultivariateError::NotFitted);
        }
        let _start = std::time::Instant::now();
        let k = ctx.matrix.n_series();
        if k != self.k {
            return Err(MultivariateError::InsufficientData {
                min: self.k,
                got: k,
            });
        }
        let n = ctx.matrix.n_timestamps();

        // Compute all reconstruction errors for threshold calibration
        let mut errors = Vec::with_capacity(n);
        let mut points = Vec::with_capacity(n);
        for t in 0..n {
            let point: Vec<f64> = (0..k).map(|s| ctx.matrix.data[s][t]).collect();
            errors.push(self.reconstruction_error(&point));
            points.push(point);
        }

        let mean_err = errors.iter().sum::<f64>() / n as f64;
        // Use Bessel's correction (n-1) for consistency with simd_std_dev
        // and features::zscore across the codebase.
        let denom = if n > 1 { (n - 1) as f64 } else { n as f64 };
        let std_err = (errors.iter().map(|&e| (e - mean_err).powi(2)).sum::<f64>() / denom).sqrt();

        let mut scores = Vec::with_capacity(n);
        for t in 0..n {
            let z = if std_err > 1e-15 {
                (errors[t] - mean_err) / std_err
            } else {
                0.0
            };
            let is_anomaly = z > self.anomaly_threshold;

            let contributions: Vec<(String, f64)> = (0..k)
                .map(|s| {
                    let diff = (points[t][s] - self.means[s]).abs();
                    (ctx.matrix.series_ids[s].clone(), diff)
                })
                .collect();

            scores.push(MultivariateAnomalyScore {
                timestamp: ctx.matrix.timestamps[t],
                score: errors[t],
                is_anomaly,
                contributions,
            });
        }
        metrics::histogram!("chronix_multivariate_anomaly_detect_duration_seconds")
            .record(_start.elapsed().as_secs_f64());
        Ok(scores)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn normal_ctx(n: usize) -> MultiSeriesContext {
        let ts: Vec<i64> = (0..n as i64).map(|i| i * 1_000_000_000).collect();
        let s1: Vec<f64> = (0..n)
            .map(|i| (i as f64 * 0.1).sin() * 5.0 + 50.0)
            .collect();
        let s2: Vec<f64> = (0..n)
            .map(|i| (i as f64 * 0.1).cos() * 5.0 + 50.0)
            .collect();
        MultiSeriesContext::build(
            vec![
                ("s1".to_string(), ts.clone(), s1),
                ("s2".to_string(), ts, s2),
            ],
            None,
        )
        .unwrap()
    }

    fn anomalous_ctx() -> MultiSeriesContext {
        let n = 100;
        let ts: Vec<i64> = (0..n as i64).map(|i| i * 1_000_000_000).collect();
        let mut s1: Vec<f64> = (0..n)
            .map(|i| (i as f64 * 0.1).sin() * 5.0 + 50.0)
            .collect();
        let mut s2: Vec<f64> = (0..n)
            .map(|i| (i as f64 * 0.1).cos() * 5.0 + 50.0)
            .collect();
        // Inject a multivariate anomaly at t=50 (individually plausible, jointly abnormal)
        s1[50] = 70.0;
        s2[50] = 70.0;
        MultiSeriesContext::build(
            vec![
                ("s1".to_string(), ts.clone(), s1),
                ("s2".to_string(), ts, s2),
            ],
            None,
        )
        .unwrap()
    }

    #[test]
    fn mahalanobis_fit_and_detect() {
        let ctx = normal_ctx(200);
        let mut det = MahalanobisDetector::new(None, None);
        det.fit(&ctx).unwrap();
        let scores = det.detect(&ctx).unwrap();
        assert_eq!(scores.len(), ctx.matrix.n_timestamps());
        // Most should not be anomalous
        let anomaly_count = scores.iter().filter(|s| s.is_anomaly).count();
        assert!(anomaly_count < scores.len() / 2);
    }

    #[test]
    fn mahalanobis_detects_joint_anomaly() {
        let ctx = anomalous_ctx();
        let mut det = MahalanobisDetector::new(Some(2.0), None);
        det.fit(&ctx).unwrap();
        let scores = det.detect(&ctx).unwrap();
        // The injected anomaly at t=50 should have a high score
        let t50 = &scores[50];
        assert!(
            t50.score > 1.5,
            "score at anomaly should be high: {}",
            t50.score
        );
    }

    #[test]
    fn mahalanobis_contributions() {
        let ctx = anomalous_ctx();
        let mut det = MahalanobisDetector::new(None, None);
        det.fit(&ctx).unwrap();
        let scores = det.detect(&ctx).unwrap();
        assert_eq!(scores[50].contributions.len(), 2);
    }

    #[test]
    fn pca_fit_and_detect() {
        let ctx = normal_ctx(200);
        let mut det = PcaAnomalyDetector::new(None, None);
        det.fit(&ctx).unwrap();
        let scores = det.detect(&ctx).unwrap();
        assert_eq!(scores.len(), ctx.matrix.n_timestamps());
    }

    #[test]
    fn pca_components_retained() {
        let ctx = normal_ctx(200);
        let mut det = PcaAnomalyDetector::new(Some(0.95), None);
        det.fit(&ctx).unwrap();
        // With 2 series, should retain 1-2 components
        assert!(!det.components.is_empty());
        assert!(det.components.len() <= 2);
    }

    #[test]
    fn randomized_svd_recovers_known_eigenvalues() {
        // Construct a 4×4 covariance matrix with known eigenvalues:
        // λ = [10, 5, 1, 0.1]
        // Use Q·Λ·Qᵀ where Q is an orthogonal rotation.
        let eigenvalues = [10.0, 5.0, 1.0, 0.1];
        let k = 4usize;

        // Simple orthogonal matrix (Hadamard-like, normalized)
        let q = [
            0.5, 0.5, 0.5, 0.5, 0.5, -0.5, 0.5, -0.5, 0.5, 0.5, -0.5, -0.5, 0.5, -0.5, -0.5, 0.5,
        ];

        // Construct A = Q · diag(λ) · Qᵀ
        let mut a = vec![0.0; k * k];
        for i in 0..k {
            for j in 0..k {
                let mut sum = 0.0;
                for l in 0..k {
                    sum += q[i * k + l] * eigenvalues[l] * q[j * k + l];
                }
                a[i * k + j] = sum;
            }
        }

        let (evals, evecs) = PcaAnomalyDetector::randomized_eigen(&a, k, k, 2);
        assert_eq!(evals.len(), k);

        // Eigenvalues should be close to the true ones (sorted descending)
        for (i, &expected) in eigenvalues.iter().enumerate() {
            assert!(
                (evals[i] - expected).abs() < 0.5,
                "eigenvalue[{i}]: got {}, expected {expected}",
                evals[i]
            );
        }

        // Eigenvectors should be orthonormal
        for i in 0..evals.len() {
            let norm: f64 = evecs[i].iter().map(|x| x * x).sum::<f64>().sqrt();
            assert!(
                (norm - 1.0).abs() < 1e-10,
                "eigenvector[{i}] not unit: norm = {norm}"
            );
            for j in (i + 1)..evals.len() {
                let dot: f64 = evecs[i]
                    .iter()
                    .zip(evecs[j].iter())
                    .map(|(a, b)| a * b)
                    .sum();
                assert!(
                    dot.abs() < 1e-8,
                    "eigenvectors [{i}] and [{j}] not orthogonal: dot = {dot}"
                );
            }
        }
    }

    // ── Isolation Forest ────────────────────────────────────────────

    #[test]
    fn isolation_forest_fit_and_detect() {
        let ctx = normal_ctx(200);
        let mut det = IsolationForestDetector::new(Some(50), Some(128), None);
        det.fit(&ctx).unwrap();
        let scores = det.detect(&ctx).unwrap();
        assert_eq!(scores.len(), 200);
        // All scores should be in [0, 1]
        for s in &scores {
            assert!(
                s.score >= 0.0 && s.score <= 1.0,
                "score out of range: {}",
                s.score
            );
        }
    }

    #[test]
    fn isolation_forest_detects_anomaly() {
        let ctx = anomalous_ctx();
        let mut det = IsolationForestDetector::new(Some(100), Some(256), Some(0.55));
        det.fit(&ctx).unwrap();
        let scores = det.detect(&ctx).unwrap();
        // The injected anomaly at t=50 should have a higher score than median
        let median_score = {
            let mut sorted: Vec<f64> = scores.iter().map(|s| s.score).collect();
            sorted.sort_by(f64::total_cmp);
            sorted[sorted.len() / 2]
        };
        assert!(
            scores[50].score > median_score,
            "anomaly score {} should exceed median {}",
            scores[50].score,
            median_score
        );
    }

    #[test]
    fn isolation_forest_contributions() {
        let ctx = anomalous_ctx();
        let mut det = IsolationForestDetector::new(Some(50), None, None);
        det.fit(&ctx).unwrap();
        let scores = det.detect(&ctx).unwrap();
        assert_eq!(scores[50].contributions.len(), 2);
    }

    #[test]
    fn isolation_forest_insufficient_data() {
        let ts = vec![0i64];
        let s1 = vec![1.0];
        let ctx = MultiSeriesContext::build(vec![("s1".to_string(), ts, s1)], None).unwrap();
        let mut det = IsolationForestDetector::new(None, None, None);
        assert!(det.fit(&ctx).is_err());
    }

    #[test]
    fn jacobi_converges_for_small_identity_matrix() {
        // A 3×3 identity matrix is already diagonal — Jacobi should
        // converge immediately without emitting a warning.
        let k = 3;
        let mut identity = vec![0.0; k * k];
        for i in 0..k {
            identity[i * k + i] = 1.0;
        }
        // Run the randomized eigen path with n_components = k (no rank reduction)
        let (evals, evecs) = PcaAnomalyDetector::randomized_eigen(&identity, k, k, 0);
        assert_eq!(evals.len(), k);
        for &ev in &evals {
            assert!(
                (ev - 1.0).abs() < 0.1,
                "eigenvalue should be ≈1.0, got {ev}"
            );
        }
        // Eigenvectors should be orthonormal
        for i in 0..evals.len() {
            let norm: f64 = evecs[i].iter().map(|x| x * x).sum::<f64>().sqrt();
            assert!(
                (norm - 1.0).abs() < 1e-8,
                "eigenvector[{i}] not unit: norm = {norm}"
            );
        }
    }
}
