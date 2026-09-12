+++
title = "Model Selection"
description = "Choosing the right forecasting model is as important as tuning it. This page describes the automated selection strategy used by Chronix when the user does not explicitly specify a model."
weight = 170
+++

Choosing the right forecasting model is as important as tuning it.
`select_model` and `auto_forecast` in `chronix-analytics::forecast` do it
automatically, and report their working.

## The rule

**Every eligible model is fitted and scored by rolling-origin cross-validation
at the horizon actually asked for; the lowest error wins.**

## Why not an information criterion

An AIC or AICc race across model families is not a valid comparison. AIC is a
statement about *one* likelihood, and these candidates do not share one:

- The exponential smoothers (SES, Holt, Holt-Winters) are fitted by minimising
  a sum of squared one-step errors, not by maximum likelihood. There is no
  \(\hat{L}\) to put in the formula.
- An ARIMA with \(d = 1\) is fitted to the **differenced** series — one
  observation shorter, on a different scale. Its residual sum of squares is not
  measuring the same quantity as an undifferenced model's.

Ranking those by AIC produces a number for every candidate and an ordering that
means nothing. Within one family at one differencing order the criterion *is*
valid, and that is exactly where `auto_arima` uses AICc — to choose
\((p, q)\) once \(d\) is fixed.

Walk-forward error asks the only question that transfers across families:
*given the data up to here, how wrong was this model \(h\) steps later?*

## The pipeline

```text
1. Seasonal period
   └── detect_period(): detrend, then find a local maximum of the ACF
       above the significance threshold

2. Differencing order
   └── select_differencing_order(): difference while KPSS rejects level
       stationarity, up to max_d

2b. Seasonal differencing order
   └── select_seasonal_differencing_order(): STL seasonal strength
       1 - Var(remainder)/Var(remainder + seasonal) against 0.64

3. Candidate set
   └── pruned by series length before anything is fitted

4. ARIMA order
   └── auto_arima() on the FIRST FOLD'S TRAINING WINDOW only, so the
       order search cannot see the data it will be scored against

5. Score
   └── CrossValidator (expanding window, `folds` origins, horizon = the
       requested horizon) → mean RMSE / MAE / sMAPE

6. Refit the winner on the whole series
```

## Selection Criteria

### 1. Series length

| Training window | Eligible candidates |
|-----------------|---------------------|
| any | SES |
| ≥ 10 observations | + Holt linear, damped Holt (φ = 0.9), linear regression |
| ≥ 30 observations in the first fold | + ARIMA at the `auto_arima` order |
| a period *m* is found and ≥ 2 seasons fit in a fold | + Holt-Winters additive |
| as above, and every value is strictly positive | + Holt-Winters multiplicative |
| as above, with room for the expanded polynomial | + SARIMA(p,d,q)(1,1,0)ₘ and (0,1,1)ₘ |

Multiplicative Holt-Winters is excluded on a series that touches zero or goes
negative, because the seasonal factors divide.

### 2. Seasonality

`detect_period` **detrends** the series, then looks for a **local maximum** of
the autocorrelation above a threshold (0.3 by default).

Both halves matter. On a raw trended series the ACF is dominated by the trend
and decays from lag 1, so the largest value is always at the smallest lag — a
statement about smoothness rather than seasonality. The biased ACF estimator
(dividing by the total variance rather than by \(n - k\)) tapers at long lags,
which keeps a harmonic multiple 2*P* from outscoring the fundamental *P*.

### 3. Stationarity

The **KPSS** test, whose null hypothesis is that the series is stationary around
a constant — so a statistic **above** the critical value is evidence that it is
*not*, and that a difference is called for. That direction is the opposite of an
augmented Dickey–Fuller test and is the usual way this test is misread.

$$
\eta = \frac{1}{n^2 \hat{\sigma}^2_{LR}} \sum_{t=1}^{n} S_t^2,
\qquad S_t = \sum_{i=1}^{t} (y_i - \bar{y})
$$

with \(\hat{\sigma}^2_{LR}\) a Bartlett-kernel long-run variance at the
Schwert short truncation lag \(\lfloor 4(n/100)^{1/4} \rfloor\). The 5 %
critical value for the level-stationary case is **0.463**.

### 4. Metric

`SelectionMetric::Rmse` (default), `Mae` or `Smape`. RMSE penalises large
misses, which is usually what a capacity or threshold decision cares about;
sMAPE compares series of different magnitudes. Every candidate carries all
three regardless of which one decided.

## The report

`ModelSelection` names the winner, lists every candidate with its score sorted
best-first, and lists every candidate that was **not** scored with the reason:
*"SARIMA was not considered"* and *"SARIMA lost"* are different answers to
*"why is my forecast not seasonal?"*.

```text
Winner: SARIMA(2,1,0)(0,1,1)[24]
Chosen by: RMSE over 3 rolling origins, 264 training points in the first fold
Detected period: 24   KPSS differencing order: 1
Candidates:
  SARIMA(2,1,0)(0,1,1)[24]           RMSE      0.000
  SARIMA(2,1,0)(1,1,0)[24]           RMSE      0.000
  Holt-Winters additive[24]          RMSE      0.305
  ARIMA(2,1,0)                       RMSE      1.027
  Holt damped (φ=0.9)                RMSE     16.215
  Holt-Winters multiplicative[24]    RMSE     23.374
  Linear regression                  RMSE     30.710
  Holt linear                        RMSE     37.150
  SES                                RMSE     39.045
```

That output is from `cargo run -p chronix --example forecast`.

## Cost

`candidates × folds` fits, which is why the candidate set is pruned by series
length first and why `folds` defaults to 3 — enough that one lucky window
cannot decide, few enough that the whole search is a handful of fits. The ARIMA
order search runs once, on the first training window, rather than once per fold.

## Why the candidate set is small

A wider search is not a better one. Every extra candidate is another chance for
one to win the folds by luck and lose out of sample, and that cost is
measurable:

| Change | Effect on mean MASE |
|---|---|
| Offer both seasonal differencing orders and let the folds pick | < 0.01 over four regimes; **worse** on quarterly data |
| Offer the seasonal `(1,1)` shape, on data generated from a true `(1,0,1)` | **worse** — 0.8925 → 0.8976 |
| Raise `folds` from 3 to 12 | none — 0.8925 → 0.8980 |

The residual gap to an oracle handed the true model (0.8264 above) is
selection variance, not missing candidates. This is also why there is no
seasonal unit-root test: the seasonal-strength rule is blind to a *stochastic*
seasonal level, but Holt-Winters is in the set and its seasonal smoothing
tracks one, so the case is already covered.
`chronix-analytics/examples/seasonal_differencing.rs` and
`seasonal_pq_search.rs` reproduce the table.

## Cross-Validation

`CrossValidator` is the machinery underneath, and is usable on its own for
evaluating a single model:

```text
Fold 1:  [train₁ ───────── ] [test₁]
Fold 2:  [train₂ ──────────── ] [test₂]
Fold 3:  [train₃ ─────────────── ] [test₃]
```

Each fold fits a **fresh** model — the factory is called once per fold, so no
state carries over — on all data up to a cutoff, and scores it on the next *h*
observations. `CrossValidationMode::ExpandingWindow` anchors every training set
at index 0; `SlidingWindow` keeps it a fixed size.

```rust
use chronix_analytics::forecast::cross_validation::{CrossValidationMode, CrossValidator};
use chronix_analytics::forecast::SesModel;

let cv = CrossValidator::new(CrossValidationMode::ExpandingWindow, 30, 10, 5);
let results = cv.evaluate(&timestamps, &values, || Box::new(SesModel::new(None)))?;
println!("{:.3}", results.mean_rmse);
```

## Error Metrics

`CrossValidationResult` carries all four; `SelectionMetric` picks which one
decides.

| Metric | Formula | Notes |
|--------|---------|-------|
| MAE | $\frac{1}{n}\sum|y_t - \hat{y}_t|$ | Same units as the series |
| RMSE | $\sqrt{\frac{1}{n}\sum(y_t - \hat{y}_t)^2}$ | **The default.** Penalises large misses |
| MAPE | $\frac{100}{n}\sum\left|\frac{y_t - \hat{y}_t}{y_t}\right|$ | Scale-free, undefined at $y_t=0$ — reported as `None` there rather than as infinity |
| sMAPE | $\frac{100}{n}\sum\frac{|y_t - \hat{y}_t|}{(|y_t| + |\hat{y}_t|)/2}$ | Symmetric, bounded, defined at zero unless both are zero |

MASE is not computed. It needs a naïve-baseline MAE over the *training* window
to normalise against, which the fold result does not carry.

## Choosing `D`

The seasonal differencing order comes from the **seasonal strength** measure of
Wang, Smith & Hyndman over the STL decomposition:

```text
Fs = max(0, min(1, 1 - Var(remainder) / Var(remainder + seasonal)))
D  = 1 if Fs > 0.64 else 0
```

Both the measure and the threshold are R's `forecast::nsdiffs` default; the
0.64 was fitted by minimising MASE across M3 and M4. Two guards come with it:
a **constant** series has zero variance in both terms, so the ratio says
nothing, and a series shorter than two periods cannot be decomposed.

`D = 0` matters when a period is detected but the seasonal term explains
almost none of the variance: differencing there spends `m` observations, adds
a moving-average term the data does not support, and widens the intervals.

**This is a strength test, not a unit-root test.** A *stochastic* seasonal
level — each season drifting from cycle to cycle — scores **low** and is not
differenced, because STL's cycle-subseries smoother cannot fit a shape that
keeps moving and the variation lands in the remainder. OCSB and Canova–Hansen
are the tests for that case; R keeps them as options beside this default.
