+++
title = "Multivariate analysis"
description = "Correlation, VAR, Mahalanobis distance, PCA and isolation forests in Chronix: detecting anomalies across several series at once."
weight = 130
aliases = ["/internals/multivariate/", "/internals/multivariate-correlation/", "/internals/multivariate-var/", "/internals/multivariate-mahalanobis/", "/internals/multivariate-pca/", "/internals/multivariate-isolation-forest/"]
+++

Some failures only show across series: memory rising while garbage collection
speeds up, with latency still flat. Chronix's multivariate module looks at
several aligned series at once — it measures how they move together, forecasts
them jointly, and flags an observation that is unusual as a *combination* even
when every series is normal on its own. Everything here lives in
`chronix_analytics::multivariate` and runs on series the caller has aligned to
one time grid.

| Question | Method | Type |
|---|---|---|
| Do two series move together? | Pearson, Spearman, Kendall τ; rolling and lagged | `PearsonCorrelation`, `SpearmanCorrelation`, `KendallTau`, `RollingCorrelation`, `LagCorrelation`, `CrossCorrelationMatrix` |
| What happens next, given all of them? | Vector autoregression, Granger causality | `VarModel` |
| Is this combination of values unusual? | Mahalanobis distance | `MahalanobisDetector` |
| Does it break the usual structure? | PCA reconstruction error | `PcaAnomalyDetector` |
| Is it easy to isolate? | Isolation forest | `IsolationForestDetector` |

## Correlation

| Measure | Detects | Cost | Use when |
|---|---|---|---|
| Pearson *r* | linear relationships | O(n) | the relationship is roughly linear and clean |
| Spearman ρ | any monotonic relationship | O(n log n) | outliers or non-linear but monotonic, the common case for infrastructure metrics |
| Kendall τ-a | concordant minus discordant pairs | O(n log n) | small samples |

- **Missing values** are deleted pairwise: a time step where either value is
  NaN or infinite is left out, and fewer than two valid pairs is an
  `InsufficientData` error rather than a NaN.
- **Lagged correlation** computes Pearson at the lags you ask for — "requests
  lead CPU by 30 s". Ask for specific lags; each costs one pass.
- **Rolling correlation** over a sliding window shows when two normally coupled
  series decouple, which is often the anomaly itself.

From SQL, `correlation(a, b)`, `cross_correlation(a, b, lag)` and
`rolling_corr(a, b, window)` are window functions over a partition.

## Vector autoregression (VAR)

A VAR(*p*) predicts every series from the last *p* values of **all** of them:

$$
\mathbf{y}_t = \mathbf{c} + \mathbf{A}_1 \mathbf{y}_{t-1} + \cdots + \mathbf{A}_p \mathbf{y}_{t-p} + \boldsymbol{\varepsilon}_t
$$

The off-diagonal coefficients are the cross-effects. Coefficients are
estimated by least squares, equation by equation. The lag order defaults to 1
and is the caller's choice; a fit whose companion matrix has a spectral radius
above 1.05 (an explosive process) is refused rather than forecast. Parameters
grow as *k²p*, so VAR suits a handful of series, not dozens.

**Granger causality** asks whether the past of *X* improves the forecast of *Y*
beyond *Y*'s own past — a way to find leading indicators:

```rust
let mut model = VarModel::new(Some(2));
model.fit(&ctx, 0)?;
for r in model.granger_causality(&ctx)? {
    if r.significant {
        println!("{} → {} (F = {:.2}, p = {:.4})", r.cause, r.effect, r.f_statistic, r.p_value);
    }
}
```

Each result carries the F statistic, its p-value and `significant` at α = 0.05.
Granger causality is predictive, not causal.

## Mahalanobis distance

The distance of an observation from the mean, measured in the shape of the
data's covariance:

$$
D_M(\mathbf{x}) = \sqrt{(\mathbf{x} - \boldsymbol{\mu})^\top \mathbf{\Sigma}^{-1} (\mathbf{x} - \boldsymbol{\mu})}
$$

Two metrics that always rise together make a narrow ellipse; a point where one
rose alone is far in Mahalanobis terms even if each value is ordinary. In one
dimension it is the absolute z-score.

- **Default threshold:** 3. Under normality *D²* follows χ² with *p* degrees
  of freedom; for a 99 % threshold use √χ²<sub>p,0.99</sub> — 3.03 for two
  series, 3.88 for five, 4.82 for ten.
- **Regularisation:** a small ridge (1e-6) is added to the covariance so nearly
  collinear series do not make it singular.
- **Explanation:** each anomaly reports how much every series contributed.
- **Needs** many more observations than series; the covariance is estimated
  from the training window, so anomalies in that window widen it.

## PCA reconstruction error

Correlated metrics live near a lower-dimensional subspace. PCA finds it; an
observation far from it — high **reconstruction error** — breaks the usual
structure.

- Components are kept until they explain 95 % of the variance (configurable).
- The top components come from a **randomized SVD** (Halko–Martinsson–Tropp)
  with a fixed seed: the same data always gives the same components and scores.
- An observation is flagged when its reconstruction error is more than 3
  standard deviations above the training errors (configurable).
- Series must be on comparable scales; standardise first, or a series in bytes
  dominates one in percent.

## Isolation forest

Anomalies are few and different, so random splits isolate them quickly. The
forest builds 100 trees, each on a random subsample of 256 observations, and
scores a point by its average path length, normalised to [0, 1]: near 1 is an
anomaly, about 0.5 is no signal. The default threshold is 0.6.

- No distribution assumption and no covariance, so it copes with more series
  than Mahalanobis.
- Each anomaly reports **feature importance**: which series split it out, with
  splits near the root weighted more.
- Deterministic: the seed defaults to 42 (`with_seed` changes it).
- Each time step is scored on its own; give it lagged or rolling features if
  temporal context matters.

## Choosing a detector

| Situation | Start with |
|---|---|
| Few series, roughly Gaussian, strongly correlated | Mahalanobis |
| Many correlated series | PCA |
| Many series, odd distributions, mixed scales | Isolation forest |
| "Did they stop moving together?" | Rolling correlation |

## Related

- [Anomaly detection](@/explanation/anomaly-detection.md) — single-series detectors
- [Forecasting](@/explanation/forecasting.md)
- [SQL reference](@/reference/sql.md)
