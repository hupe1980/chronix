+++
title = "Multivariate Analysis"
description = "Real systems produce many correlated time-series simultaneously — CPU, memory, disk I/O, network throughput, request latency. Analyzing each series independently misses cross-variable…."
weight = 250
+++

Real systems produce **many correlated time-series** simultaneously — CPU,
memory, disk I/O, network throughput, request latency. Analyzing each
series independently misses **cross-variable relationships** that are
often the key to understanding system behavior.

Multivariate analysis examines multiple time-series jointly.

## Why Multivariate?

### Example: The Hidden Correlation

Consider detecting a memory leak:

| Metric | Behavior | Univariate Detection |
|--------|----------|---------------------|
| Memory usage | Slowly increasing | Possibly flagged (if strong enough) |
| Garbage collection | Increasing frequency | Possibly flagged |
| Response latency | Stable (so far) | No alert |
| **Joint pattern** | Memory ↑, GC ↑, latency stable → **impending failure** | Requires multivariate analysis |

A univariate detector might miss the early warning because no individual
metric has crossed a threshold. The **joint pattern** — correlated growth
in two metrics — is the actual signal.

## Methods Implemented in Chronix

| Method | Type | Use Case | Section |
|--------|------|----------|---------|
| Pearson / Spearman / Kendall | Correlation | Detect linear/monotonic relationships | [Correlation](@/internals/multivariate-correlation.md) |
| Mahalanobis Distance | Distance | Multivariate outlier detection | [Mahalanobis](@/internals/multivariate-mahalanobis.md) |
| Isolation Forest | Tree-based | High-dimensional anomaly detection | [Isolation Forest](@/internals/multivariate-isolation-forest.md) |
| PCA | Decomposition | Dimensionality reduction, anomaly detection | [PCA](@/internals/multivariate-pca.md) |
| VAR | Autoregressive | Multi-series forecasting | [VAR](@/internals/multivariate-var.md) |

## Data Representation

Multivariate time-series data is represented as a matrix $\mathbf{Y}$:

$$
\mathbf{Y} = \begin{bmatrix}
y_{1,1} & y_{1,2} & \cdots & y_{1,p} \\
y_{2,1} & y_{2,2} & \cdots & y_{2,p} \\
\vdots  & \vdots  & \ddots & \vdots  \\
y_{n,1} & y_{n,2} & \cdots & y_{n,p}
\end{bmatrix}
$$

where $n$ is the number of time steps and $p$ is the number of variables
(metrics). Each row is a **multivariate observation** — a snapshot of
all metrics at one instant.

## Preprocessing for Multivariate Analysis

### Alignment

All series must be aligned to the same time grid. Missing values are
handled through:
1. **Forward fill** — carry last known value
2. **Linear interpolation** — interpolate between neighbors
3. **Exclusion** — drop time steps with any missing value

### Standardization

Because metrics have different scales (CPU in %, memory in GB, latency
in ms), all multivariate methods require **standardization**:

$$
z_{t,j} = \frac{y_{t,j} - \bar{y}_j}{s_j}
$$

This centers each variable at zero with unit variance, ensuring no single
variable dominates distance calculations or covariance matrices.

## Curse of Dimensionality

As the number of variables $p$ grows, several problems emerge:

| Issue | Threshold | Mitigation |
|-------|-----------|------------|
| Covariance matrix estimation | $p > n/5$ | Regularization (shrinkage) |
| Distance concentration | $p > 20$ | PCA dimensionality reduction |
| Computational cost | $p > 100$ | Feature selection |

Chronix addresses high dimensionality through PCA-based dimensionality
reduction before applying distance-based or tree-based detectors.
