+++
title = "Principal Component Analysis (PCA)"
description = "A system producing metrics generates observations in a 50-dimensional space. Visualizing, understanding, and detecting anomalies in 50D is intractable. But the metrics are often highly correlated…."
weight = 290
+++

## The Dimensionality Problem

A system producing $p = 50$ metrics generates observations in a
50-dimensional space. Visualizing, understanding, and detecting anomalies
in 50D is intractable. But the metrics are often **highly correlated** —
CPU, memory, and disk I/O move together, network in correlates with
network out, etc.

PCA finds a new coordinate system where most of the variance is
concentrated in the first few dimensions.

## Mathematical Foundation

### Covariance Matrix

Given centered data $\mathbf{X} \in \mathbb{R}^{n \times p}$ (each
column has zero mean), the sample covariance matrix is:

$$
\mathbf{C} = \frac{1}{n-1} \mathbf{X}^T \mathbf{X}
$$

### Eigendecomposition

PCA decomposes $\mathbf{C}$ into eigenvalues and eigenvectors:

$$
\mathbf{C} = \mathbf{V} \mathbf{\Lambda} \mathbf{V}^T
$$

where:
- $\mathbf{V} = [\mathbf{v}_1, \ldots, \mathbf{v}_p]$ — orthogonal
  eigenvectors (principal components / directions)
- $\mathbf{\Lambda} = \text{diag}(\lambda_1, \ldots, \lambda_p)$ —
  eigenvalues in decreasing order ($\lambda_1 \geq \lambda_2 \geq \cdots$)

### Projection

The projection onto the first $k$ principal components:

$$
\mathbf{Z} = \mathbf{X} \mathbf{V}_k
$$

where $\mathbf{V}_k = [\mathbf{v}_1, \ldots, \mathbf{v}_k]$.

### Variance Explained

The fraction of total variance captured by the first $k$ components:

$$
\text{Var}_{k} = \frac{\sum_{i=1}^{k} \lambda_i}{\sum_{i=1}^{p} \lambda_i}
$$

A common rule: choose $k$ such that $\text{Var}_k \geq 0.95$ (95%).

## PCA for Anomaly Detection

### Reconstruction Error

The key insight: if the first $k$ components capture normal behavior, then
**anomalies will have large reconstruction errors** when projected and
reconstructed:

$$
\hat{\mathbf{x}} = \mathbf{V}_k \mathbf{V}_k^T \mathbf{x}
$$

$$
\text{RE}(\mathbf{x}) = \|\mathbf{x} - \hat{\mathbf{x}}\|^2
$$

The reconstruction error is the squared distance from the point to its
projection onto the principal subspace:

```text
                PC1
                 ↗
    ●          ↗    ← anomaly (far from PC subspace)
    │        ↗
    │ RE   ↗
    │↓   ↗
    ┼──●────────── PC1 (reconstructed point)
    │ ↗ ●●●●●
    ↗  ●●●●●●●    ← normal points (close to PC subspace)
  ↗   ●●●●●●●
```

### Threshold

Under normality, the reconstruction error follows a scaled chi-squared
distribution:

$$
\frac{\text{RE}(\mathbf{x})}{\hat{\sigma}^2_{\text{RE}}} \sim \chi^2_{p-k}
$$

Points with RE exceeding the $\chi^2_{p-k, 1-\alpha}$ quantile are
flagged as anomalous.

## Choosing *k* in Chronix

Chronix supports three strategies:

| Strategy | Method | Default |
|----------|--------|---------|
| Variance threshold | Keep components until 95% variance explained | ✓ |
| Fixed *k* | User-specified number of components | |
| Scree heuristic | Find "elbow" in eigenvalue plot | |

### Kaiser's Rule

An alternative: retain only components with $\lambda_i > 1$ (for
standardized data). Components with eigenvalue < 1 explain less variance
than a single original variable.

## Incremental PCA

For streaming time-series, recomputing PCA from scratch is expensive.
Chronix uses **incremental PCA** (Ross et al. 2008) that updates the
eigendecomposition as new observations arrive:

$$
\mathbf{C}_{t+1} = \frac{n-1}{n} \mathbf{C}_t + \frac{1}{n} (\mathbf{x}_{t+1} - \bar{\mathbf{x}}_t)(\mathbf{x}_{t+1} - \bar{\mathbf{x}}_t)^T
$$

The eigenvectors are updated via rank-1 perturbation formulas, giving
O(p²) updates instead of O(p³) full decomposition.

## Complexity

| Operation | Cost |
|-----------|------|
| Fit (full SVD) | O(n·p² + p³) |
| Fit (incremental) | O(p²) per point |
| Transform | O(p·k) per point |
| Reconstruction error | O(p·k) per point |

For typical monitoring ($p < 100$), PCA is extremely fast.

## Limitations

| Limitation | Mitigation |
|------------|------------|
| Assumes linear correlations | Use kernel PCA for non-linear |
| Sensitive to scaling | Always standardize first |
| Static model | Incremental PCA + periodic refit |
| Orthogonality constraint | May miss non-orthogonal structure |
