+++
title = "Correlation Analysis"
description = "Correlation measures the strength and direction of the relationship between two variables. For time-series monitoring, correlation analysis answers questions like: 'When CPU goes up, does latency…."
weight = 260
+++

Correlation measures the strength and direction of the relationship between
two variables. For time-series monitoring, correlation analysis answers
questions like: "When CPU goes up, does latency also increase?"

## Pearson Correlation

### Definition

The Pearson correlation coefficient measures **linear** relationships:

$$
r_{xy} = \frac{\sum_{t=1}^{n}(x_t - \bar{x})(y_t - \bar{y})}
              {\sqrt{\sum_{t=1}^{n}(x_t - \bar{x})^2 \cdot \sum_{t=1}^{n}(y_t - \bar{y})^2}}
$$

$r_{xy} \in [-1, +1]$:

| Value | Interpretation |
|-------|---------------|
| +1.0 | Perfect positive linear |
| +0.7 to +1.0 | Strong positive |
| +0.3 to +0.7 | Moderate positive |
| −0.3 to +0.3 | Weak or none |
| −1.0 | Perfect negative linear |

### Limitations

- Only detects **linear** relationships ($r = 0$ for $y = x^2$)
- Sensitive to outliers (a single extreme point can dominate)
- Does not imply causation

---

## Spearman Rank Correlation

### Definition

Spearman correlation operates on **ranks** rather than raw values,
detecting any **monotonic** relationship:

$$
\rho_s = 1 - \frac{6 \sum_{t=1}^{n} d_t^2}{n(n^2 - 1)}
$$

where $d_t = \text{rank}(x_t) - \text{rank}(y_t)$.

### Comparison with Pearson

| Scenario | Pearson *r* | Spearman *ρ* |
|----------|-------------|--------------|
| $y = 2x + 1$ | 1.0 | 1.0 |
| $y = e^x$ | < 1.0 | 1.0 (monotonic) |
| $y = x^2$ (positive domain) | < 1.0 | 1.0 |
| Outlier-contaminated linear | Distorted | Robust |

Spearman is the **default** in Chronix because infrastructure metrics
often have non-linear but monotonic relationships.

---

## Kendall Tau

### Definition

Kendall's τ counts **concordant** and **discordant** pairs:

$$
\tau = \frac{C - D}{\binom{n}{2}}
$$

where:
- $C$ = number of pairs $(i, j)$ with $i < j$ where both $x_i < x_j$
  and $y_i < y_j$ (or both reversed)
- $D$ = number of discordant pairs

### When to Use Kendall

- More robust than Spearman for small samples
- Better statistical properties (variance is well-known)
- Computationally O(n log n) with merge-sort algorithm

---

## Correlation Matrix

For $p$ variables, the **correlation matrix** is a $p \times p$ symmetric
matrix:

$$
\mathbf{R} = \begin{bmatrix}
1 & r_{12} & \cdots & r_{1p} \\
r_{12} & 1 & \cdots & r_{2p} \\
\vdots & \vdots & \ddots & \vdots \\
r_{1p} & r_{2p} & \cdots & 1
\end{bmatrix}
$$

Chronix computes this matrix over sliding windows to detect **correlation
changes** — a sudden decorrelation between normally correlated metrics
is itself an anomaly signal.

## Lag Correlation (Cross-Correlation)

Metrics may be correlated with a **time lag** (e.g. increased incoming
requests → higher CPU 30 seconds later). Cross-correlation at lag *k*:

$$
r_{xy}(k) = \text{Pearson}(x_{t}, y_{t+k})
$$

Chronix scans lags $k \in [-k_{\max}, +k_{\max}]$ and reports the lag
with the highest absolute correlation, along with its significance.

## Computational Notes

| Method | Complexity | Notes |
|--------|-----------|-------|
| Pearson | O(n) | Incremental via Welford's algorithm |
| Spearman | O(n log n) | Requires sorting for ranks |
| Kendall | O(n log n) | Knight's merge-sort algorithm (pre-allocated scratch buffer) |
| Lag correlation | O(n · L) | One Pearson pass per requested lag; ask for the lags you want, not a range |

## Missing-Value Handling

All pairwise correlation methods (Pearson, Spearman, Kendall) apply
**pairwise NaN deletion** before computation: if either $x_t$ or $y_t$
is NaN or ±∞, the pair $(x_t, y_t)$ is excluded from the analysis.

This is the standard approach used by pandas, R, and NumPy and avoids
silent NaN propagation through arithmetic.  If fewer than 2 valid pairs
remain after filtering, an `InsufficientData` error is returned.
