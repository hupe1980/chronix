+++
title = "Isolation Forest"
description = "Most anomaly detection methods build a model of normal data and then flag points that deviate from it. Isolation Forest flips this: it directly isolates anomalies by exploiting their two key…."
weight = 280
+++

## Intuition

Most anomaly detection methods build a model of **normal** data and then
flag points that deviate from it. Isolation Forest flips this: it
directly isolates **anomalies** by exploiting their two key properties:

1. **Few**: Anomalies are a small minority of observations
2. **Different**: Their attribute values differ markedly from the majority

Because anomalies are few and different, they are **easier to isolate** —
they require fewer random splits in a tree structure.

## Algorithm

### Building an Isolation Tree

An isolation tree (iTree) is built by recursively partitioning the data:

1. Select a random feature $j \in \{1, \ldots, p\}$
2. Select a random split value $s$ uniformly in $[\min(x_j), \max(x_j)]$
3. Partition data into left ($x_j < s$) and right ($x_j \geq s$)
4. Recurse until:
   - The node contains a single point, or
   - The tree reaches maximum depth $\lceil \log_2 n \rceil$

### Path Length as Anomaly Score

The **path length** $h(\mathbf{x})$ is the number of edges from the root
to the node where point $\mathbf{x}$ terminates.

- **Anomalies**: Short paths (isolated quickly)
- **Normal points**: Long paths (buried deep in the tree)

```text
                    Root
                   ╱    ╲
              ← anomaly   ╲
              (depth 1)     ╲
                           ╱  ╲
                          ╱    ╲
                         ╱      ╲
                        ╱  ╲   ╱  ╲
                       …    … …    …
                       normal points (depth 4-8)
```

### Ensemble: Isolation Forest

A single tree is noisy. The Isolation Forest builds an ensemble of
$T$ trees (typically $T = 100$) and averages the path lengths:

$$
E[h(\mathbf{x})] = \frac{1}{T} \sum_{t=1}^{T} h_t(\mathbf{x})
$$

### Anomaly Score

The anomaly score is normalized to $[0, 1]$:

$$
s(\mathbf{x}, n) = 2^{-\frac{E[h(\mathbf{x})]}{c(n)}}
$$

where $c(n)$ is the **average path length of unsuccessful search in a BST**:

$$
c(n) = 2H(n-1) - \frac{2(n-1)}{n}
$$

and $H(k) = \ln(k) + \gamma$ is the harmonic number ($\gamma \approx 0.5772$
is the Euler-Mascheroni constant).

| Score | Interpretation |
|-------|---------------|
| → 1.0 | Definite anomaly |
| ≈ 0.5 | No clear signal |
| → 0.0 | Definitely normal |

Points with $s > 0.6$ are typically flagged as anomalous (threshold
is configurable).

## Advantages for Time-Series

| Property | Benefit |
|----------|---------|
| No distribution assumption | Works on any data shape |
| Handles high dimensions | No curse of dimensionality |
| Sub-sampling | Training on 256 samples is sufficient |
| Linear time | O(T · n · log n) training |
| No distance computation | Avoids O(p²) covariance estimation |

### Sub-Sampling

Liu et al. (2008) showed that Isolation Forest works best with **small
sub-samples** (ψ = 256 is recommended). This is because:

1. Swamping: Large samples cause normal points to interfere with anomaly
   isolation
2. Masking: Dense clusters of anomalies become harder to isolate in large
   samples

In Chronix, each tree is built on a random sub-sample of ψ = 256 points
from the training window.

## Complexity Analysis

| Phase | Time | Space |
|-------|------|-------|
| Training | O(T · ψ · log ψ) | O(T · ψ) |
| Scoring (per point) | O(T · log ψ) | O(1) |

With $T = 100$ and $ψ = 256$:
- Training: ~200K operations (sub-millisecond)
- Scoring: ~800 operations per point

## Limitations

| Limitation | Notes |
|------------|-------|
| Axis-aligned splits | May miss anomalies aligned with the axes |
| Uniform split values | Sensitive to feature scaling |
| No temporal awareness | Treats each time step independently |

Chronix mitigates the temporal limitation by constructing **feature
vectors** from sliding windows: each observation includes lagged values,
rolling statistics, and cross-variable features before being passed to
the Isolation Forest.

## Feature Importance

When an anomaly is detected, understanding *which* series contributed
most is critical for root-cause analysis. Chronix computes
**depth-weighted split frequency importance** for each feature:

1. For each tree in the ensemble, traverse the path from root to the
   leaf where the point terminates.
2. At each internal node, record which feature was used for the split.
3. Weight the contribution by $\frac{1}{1 + \text{depth}}$ — splits
   closer to the root are more discriminative.
4. Aggregate across all trees and normalize so contributions sum to 1.0.

This is more meaningful than naive distance-to-mean because it reflects
the actual isolation structure: features that repeatedly split out an
anomaly early contribute more to its anomaly score.

## Reproducibility

The Isolation Forest uses per-tree RNG seeding for deterministic results.
By default the seed is `42`, producing identical forests across runs.
Use `.with_seed(seed)` to control the random state:

```rust
let detector = IsolationForestDetector::new(n_trees, max_samples)
    .with_seed(12345);
```

Setting different seeds is useful for ensemble diversity analysis or
integration tests that require specific behaviour.
