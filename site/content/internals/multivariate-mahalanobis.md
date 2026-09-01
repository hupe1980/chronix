+++
title = "Mahalanobis Distance"
description = "Euclidean distance treats all dimensions equally and ignores correlations. For multivariate anomaly detection, this produces misleading results."
weight = 270
+++

## Beyond Euclidean Distance

Euclidean distance treats all dimensions equally and ignores correlations.
For multivariate anomaly detection, this produces misleading results:

```text
Euclidean:                    Mahalanobis:
  y₂                            y₂
  │         ●                    │         ●  ← anomalous
  │       ╱    ╲                 │       ╱    ╲
  │     ╱   ●    ╲               │     ╱ ●      ╲
  │   ╱   ●●●●     ╲            │   ╱  ●●●●      ╲
  │   ╲   ●●●●●●  ╱             │   ╲  ●●●●●●   ╱
  │     ╲  ●●●  ╱               │     ╲ ●●●   ╱
  │       ╲  ╱                   │       ╲  ╱
  └──────────── y₁               └──────────── y₁
  Circle (ignores              Ellipse (follows
   correlation)                 data shape)
```

The point marked ● is equidistant from the center in both views, but
Mahalanobis distance correctly identifies it as anomalous because it
lies outside the data's natural ellipsoidal shape.

## Definition

The Mahalanobis distance from a point $\mathbf{x}$ to a distribution
with mean $\boldsymbol{\mu}$ and covariance matrix $\mathbf{\Sigma}$ is:

$$
D_M(\mathbf{x}) = \sqrt{(\mathbf{x} - \boldsymbol{\mu})^T \mathbf{\Sigma}^{-1} (\mathbf{x} - \boldsymbol{\mu})}
$$

### Properties

- **Scale-invariant**: Standardizes each dimension by its variance
- **Correlation-aware**: Accounts for inter-variable correlations
- **Reduces to Z-score**: In one dimension, $D_M = |z|$
- **Reduces to Euclidean**: When $\mathbf{\Sigma} = \mathbf{I}$

## Statistical Distribution

If $\mathbf{x} \sim N(\boldsymbol{\mu}, \mathbf{\Sigma})$, then:

$$
D_M^2 \sim \chi^2_p
$$

where $p$ is the number of variables. This gives a principled threshold:

| Dimensions *p* | $\chi^2_{p, 0.99}$ | $D_M$ threshold (99%) |
|----------------|--------------------|-----------------------|
| 2 | 9.21 | 3.03 |
| 5 | 15.09 | 3.88 |
| 10 | 23.21 | 4.82 |
| 20 | 37.57 | 6.13 |

A point is flagged anomalous if $D_M^2 > \chi^2_{p, 1-\alpha}$.

## Covariance Estimation

The quality of Mahalanobis distance depends on the covariance estimate.

### Sample Covariance

$$
\hat{\mathbf{\Sigma}} = \frac{1}{n-1} \sum_{t=1}^{n} (\mathbf{y}_t - \bar{\mathbf{y}})(\mathbf{y}_t - \bar{\mathbf{y}})^T
$$

Requires $n \gg p$ for stability. Breaks down when $n < p$ (singular matrix).

### Robust Covariance (MCD)

The **Minimum Covariance Determinant** estimator (Rousseeuw 1984) finds
the subset of $h$ observations (out of $n$) whose covariance matrix has
the smallest determinant:

$$
\hat{\mathbf{\Sigma}}_{\text{MCD}} = \arg\min_{|S|=h} \det(\text{Cov}(S))
$$

with $h \approx \lfloor (n + p + 1) / 2 \rfloor$.

MCD has a 50% breakdown point — it tolerates up to 50% outliers in
the training data. Chronix uses MCD when the data may contain anomalies
during the baseline training period.

### Shrinkage Estimation

When $p$ is close to $n$, Ledoit-Wolf shrinkage regularizes the covariance:

$$
\hat{\mathbf{\Sigma}}_{\text{shrunk}} = (1-\alpha)\hat{\mathbf{\Sigma}} + \alpha \cdot \text{tr}(\hat{\mathbf{\Sigma}}) / p \cdot \mathbf{I}
$$

The shrinkage coefficient $\alpha$ is determined analytically. This
ensures the covariance is always invertible.

## Sliding Window Computation

For streaming anomaly detection, Chronix maintains a running covariance
matrix using the **rank-1 update** formula:

$$
\mathbf{\Sigma}_{t+1} = \mathbf{\Sigma}_t + \frac{1}{n}(\mathbf{y}_{t+1} - \bar{\mathbf{y}})(\mathbf{y}_{t+1} - \bar{\mathbf{y}})^T
$$

The matrix inverse can be updated in O(p²) using the
Sherman-Morrison-Woodbury formula, avoiding an O(p³) inversion at each step.

## Computational Complexity

| Operation | Cost | Notes |
|-----------|------|-------|
| Covariance estimation | O(n·p²) | One-time |
| Matrix inversion | O(p³) | One-time |
| Distance per point | O(p²) | Matrix-vector multiply |
| Sherman-Morrison update | O(p²) | Sliding window |
