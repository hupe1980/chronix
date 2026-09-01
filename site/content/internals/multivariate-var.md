+++
title = "Vector Autoregression (VAR)"
description = "ARIMA models forecast each series independently. But in systems monitoring, metrics are causally linked — a disk I/O spike causes latency to increase, which causes queue depth to grow. VAR models…."
weight = 300
+++

## From Univariate to Multivariate Forecasting

ARIMA models forecast each series independently. But in systems monitoring,
metrics are causally linked — a disk I/O spike causes latency to increase,
which causes queue depth to grow. **VAR** models these cross-dependencies
explicitly.

## The VAR(p) Model

A VAR model of order $p$ for $k$ time-series:

$$
\mathbf{y}_t = \mathbf{c} + \mathbf{A}_1 \mathbf{y}_{t-1} + \mathbf{A}_2 \mathbf{y}_{t-2} + \cdots + \mathbf{A}_p \mathbf{y}_{t-p} + \boldsymbol{\varepsilon}_t
$$

where:
- $\mathbf{y}_t \in \mathbb{R}^k$ — vector of $k$ variables at time $t$
- $\mathbf{c} \in \mathbb{R}^k$ — intercept vector
- $\mathbf{A}_i \in \mathbb{R}^{k \times k}$ — coefficient matrices
- $\boldsymbol{\varepsilon}_t \sim N(\mathbf{0}, \mathbf{\Sigma}_\varepsilon)$ — white noise vector

### Example: VAR(1) with 3 metrics

$$
\begin{bmatrix} \text{cpu}_t \\ \text{mem}_t \\ \text{lat}_t \end{bmatrix}
=
\begin{bmatrix} c_1 \\ c_2 \\ c_3 \end{bmatrix}
+
\begin{bmatrix}
a_{11} & a_{12} & a_{13} \\
a_{21} & a_{22} & a_{23} \\
a_{31} & a_{32} & a_{33}
\end{bmatrix}
\begin{bmatrix} \text{cpu}_{t-1} \\ \text{mem}_{t-1} \\ \text{lat}_{t-1} \end{bmatrix}
+
\begin{bmatrix} \varepsilon_{1,t} \\ \varepsilon_{2,t} \\ \varepsilon_{3,t} \end{bmatrix}
$$

The off-diagonal elements ($a_{12}$, $a_{13}$, etc.) capture the
**cross-dependencies** between variables. If $a_{31}$ is large, CPU at
time $t-1$ is a strong predictor of latency at time $t$.

## Estimation

### OLS (Equation-by-Equation)

Each equation in the VAR can be estimated separately by OLS:

$$
\hat{\mathbf{A}} = \left(\sum_{t} \mathbf{y}_{t-1:t-p} \mathbf{y}_{t-1:t-p}^T\right)^{-1} \left(\sum_{t} \mathbf{y}_{t-1:t-p} \mathbf{y}_t^T\right)
$$

This is equivalent to multivariate OLS and is asymptotically efficient
when the errors are i.i.d. Gaussian.

### Parameter Count

A VAR(p) with $k$ variables has $k + k^2 \cdot p$ parameters per equation,
and $k \cdot (k + k^2 \cdot p)$ total:

| Variables *k* | Lag Order *p* | Parameters |
|---------------|---------------|------------|
| 3 | 1 | 12 |
| 3 | 4 | 39 |
| 10 | 1 | 110 |
| 10 | 4 | 410 |

The parameter count grows as $O(k^2 p)$, which limits practical
application to moderate $k$ (typically $k < 20$).

## Lag Order Selection

The lag order $p$ is selected by minimizing an information criterion:

$$
\text{AIC}(p) = \ln \det(\hat{\mathbf{\Sigma}}_\varepsilon(p)) + \frac{2pk^2}{n}
$$

$$
\text{BIC}(p) = \ln \det(\hat{\mathbf{\Sigma}}_\varepsilon(p)) + \frac{pk^2 \ln n}{n}
$$

BIC tends to select smaller models (stronger complexity penalty) and is
preferred in Chronix's automatic model selection.

## Granger Causality

VAR enables **Granger causality testing**: variable $X$ Granger-causes
variable $Y$ if past values of $X$ improve the forecast of $Y$ beyond
what $Y$'s own past provides.

The test compares:
- Restricted model: $Y_t = f(Y_{t-1}, \ldots, Y_{t-p})$
- Unrestricted model: $Y_t = f(Y_{t-1}, \ldots, Y_{t-p}, X_{t-1}, \ldots, X_{t-p})$

An F-test on the residual sum of squares determines significance.

**Operational use**: Granger causality reveals which metrics are **leading
indicators** — early warning signals that predict future issues.

### API

```rust
let mut model = VarModel::new(Some(2)); // lag order 2
model.fit(&ctx, 0)?;

// Test all pairs
let results = model.granger_causality(&ctx)?;
for r in &results {
    if r.significant {
        println!("{} Granger-causes {} (F={:.2}, p={:.4})",
            r.cause, r.effect, r.f_statistic, r.p_value);
    }
}
```

Returns a `GrangerCausalityResult` per pair with `f_statistic`,
`p_value`, `significant` (at α = 0.05), and both RSS values.

## Impulse Response Functions (IRF)

IRFs trace the effect of a one-unit shock to one variable on all other
variables over time:

$$
\mathbf{y}_{t+h} = \mathbf{\Phi}_h \boldsymbol{\varepsilon}_t
$$

where $\mathbf{\Phi}_h$ are the MA(∞) coefficient matrices derived from
the VAR coefficients.

**Operational use**: "If disk I/O spikes, how does latency respond over
the next 30 minutes?"

## Stationarity

VAR requires all series to be **jointly stationary**. The stationarity
condition is that all eigenvalues of the companion matrix lie inside the
unit circle:

$$
\det\left(\mathbf{I}_{kp} - \mathbf{A}_{\text{companion}} z\right) \neq 0 \quad \text{for } |z| \leq 1
$$

Non-stationary series are differenced before fitting (VAR in differences)
or modeled with a **Vector Error Correction Model** (VECM) if cointegration
exists.

## Complexity

| Operation | Cost |
|-----------|------|
| Estimation (OLS) | O(n · k² · p) |
| Lag selection (max P lags) | O(P · n · k² · p) |
| Forecasting (h steps) | O(h · k² · p) |
| Granger test | O(n · k² · p) per pair |

For real-time applications, Chronix caches trained VAR models and
retrains periodically (e.g. every hour) or when a distribution shift
is detected.
