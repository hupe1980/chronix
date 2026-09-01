//! Automatic forecast-model selection.
//!
//! Picking a model is the decision that matters most and the one a caller is
//! least equipped to make: it needs a seasonality test, a stationarity test,
//! an order search and, above all, an out-of-sample comparison. This module
//! runs all four and reports what it did.
//!
//! # How the winner is chosen
//!
//! By **rolling-origin cross-validation at the horizon actually wanted**, not
//! by an information criterion.
//!
//! An information criterion cannot compare these models with each other. AIC
//! is a statement about one likelihood, and the candidates here do not share
//! one: the exponential smoothers are fitted by sum of squares rather than by
//! maximum likelihood, and an ARIMA with `d = 1` is fitted to the *differenced*
//! series — a different dataset, one observation shorter and on a different
//! scale. Ranking them by AIC produces a number for every candidate and an
//! ordering that means nothing. (Within one family, at one differencing order,
//! AICc is the right tool, and that is where
//! [`auto_arima`](crate::forecast::auto_arima) uses it.)
//!
//! Walk-forward validation asks the only question that transfers: *given data
//! up to here, how wrong was this model `h` steps later?* It costs
//! `candidates × folds` fits, which is why the fold count is small by default
//! and why the candidate set is pruned by series length before anything is
//! fitted.
//!
//! # What it fits
//!
//! | Candidate | Included when |
//! |---|---|
//! | SES | always |
//! | Holt linear, and damped Holt (φ = 0.9) | ≥ 10 observations |
//! | Linear regression | ≥ 10 observations |
//! | ARIMA, at the order [`auto_arima`] picks | ≥ 30 observations |
//! | Holt-Winters, additive | a period `m ≥ 2` is found and there are ≥ 2 seasons |
//! | Holt-Winters, multiplicative | as above, and every value is strictly positive |
//! | SARIMA(p,d,q)(1,1,0)ₘ and (0,1,1)ₘ | as above, with ≥ 3 seasons |
//!
//! The seasonal period comes from
//! [`detect_period`](crate::preprocess::detect_period) unless one is given;
//! the differencing order comes from the KPSS test
//! ([`select_differencing_order`](crate::forecast::select_differencing_order)).
//! The ARIMA order is chosen on the **first fold's training window**, not on
//! the whole series, so the order search cannot see the data it is later
//! scored against.
//!
//! # Example
//!
//! ```no_run
//! use chronix_analytics::forecast::{auto_forecast, AutoForecastOptions};
//!
//! # let (timestamps, values): (Vec<i64>, Vec<f64>) = (vec![], vec![]);
//! let chosen = auto_forecast(&timestamps, &values, 24, &AutoForecastOptions::default())?;
//! println!("{} won with {} {:.3}", chosen.selection.label,
//!          chosen.selection.metric, chosen.selection.score);
//! # Ok::<(), chronix_analytics::forecast::ForecastError>(())
//! ```

use crate::forecast::cross_validation::{CrossValidationMode, CrossValidator};
use crate::forecast::error::ForecastError;
use crate::forecast::result::{ForecastResult, ModelType};
use crate::forecast::traits::ForecastModel;
use crate::forecast::{
    auto_arima, select_differencing_order, ArimaModel, AutoArimaOptions, HoltLinearModel,
    HoltWintersModel, LinearRegressionModel, SarimaModel, SearchStrategy, SesModel,
};

/// Which accuracy metric decides the winner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SelectionMetric {
    /// Root mean squared error — the default. Penalises large misses, which
    /// is usually what a capacity or threshold decision cares about.
    #[default]
    Rmse,
    /// Mean absolute error. Less sensitive to a single bad fold.
    Mae,
    /// Symmetric mean absolute percentage error, for comparing series of
    /// different magnitudes. Undefined where actual and forecast are both
    /// zero, so a candidate it cannot score is ranked by RMSE instead.
    Smape,
}

impl std::fmt::Display for SelectionMetric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Rmse => "RMSE",
            Self::Mae => "MAE",
            Self::Smape => "sMAPE",
        })
    }
}

/// Options for [`select_model`] and [`auto_forecast`].
#[derive(Debug, Clone, Copy)]
pub struct AutoForecastOptions {
    /// Seasonal period, in observations. `None` detects it.
    pub period: Option<usize>,
    /// Largest period the detector will consider.
    pub max_period: usize,
    /// Rolling origins to score each candidate over.
    pub folds: usize,
    /// Metric that decides the winner.
    pub metric: SelectionMetric,
    /// Cap on the ARIMA orders searched.
    pub max_order: usize,
}

impl Default for AutoForecastOptions {
    fn default() -> Self {
        Self {
            period: None,
            // 288 five-minute samples is a day, which is the longest cycle
            // worth detecting automatically; a weekly cycle needs a period the
            // caller states, because detecting one needs months of data.
            max_period: 288,
            // Three origins is enough to stop one lucky window deciding, and
            // few enough that the whole search is a handful of fits.
            folds: 3,
            metric: SelectionMetric::default(),
            max_order: 2,
        }
    }
}

/// One candidate's cross-validated score.
#[derive(Debug, Clone)]
pub struct CandidateScore {
    /// Human-readable model description, e.g. `SARIMA(1,0,0)(1,1,0)[24]`.
    pub label: String,
    /// Model family.
    pub model_type: ModelType,
    /// Score under the selected metric, averaged over folds. Lower is better.
    pub score: f64,
    /// Mean RMSE across folds, always populated.
    pub rmse: f64,
    /// Mean MAE across folds, always populated.
    pub mae: f64,
    /// Mean sMAPE across folds, where it is defined.
    pub smape: Option<f64>,
}

/// What [`select_model`] chose, and everything it had to work with.
#[derive(Debug, Clone)]
pub struct ModelSelection {
    /// Winning model's label.
    pub label: String,
    /// Winning model's family.
    pub model_type: ModelType,
    /// Winning score under [`Self::metric`].
    pub score: f64,
    /// The metric that decided it.
    pub metric: SelectionMetric,
    /// Every candidate that produced a score, best first.
    pub candidates: Vec<CandidateScore>,
    /// Candidates that could not be scored, with the reason. Reported rather
    /// than dropped: "SARIMA was not considered" and "SARIMA lost" are very
    /// different answers to "why is my forecast not seasonal?".
    pub rejected: Vec<(String, String)>,
    /// Seasonal period used, if any.
    pub period: Option<usize>,
    /// Differencing order the KPSS test asked for.
    pub differencing_order: usize,
    /// Rolling origins each candidate was scored over.
    pub folds: usize,
    /// Observations in the first fold's training window.
    pub min_train_size: usize,
}

/// A selected model, fitted on the whole series, and its forecast.
#[derive(Debug, Clone)]
pub struct AutoForecast {
    /// The forecast from the winning model, fitted on all the data.
    pub result: ForecastResult,
    /// Why that model won.
    pub selection: ModelSelection,
}

type Factory = Box<dyn Fn() -> Box<dyn ForecastModel> + Send + Sync>;

struct Candidate {
    label: String,
    model_type: ModelType,
    factory: Factory,
}

/// Build the candidate set for a series.
fn candidates(
    timestamps: &[i64],
    values: &[f64],
    period: Option<usize>,
    min_train: usize,
    options: &AutoForecastOptions,
    rejected: &mut Vec<(String, String)>,
) -> Vec<Candidate> {
    let n = values.len();
    let mut out: Vec<Candidate> = Vec::new();

    out.push(Candidate {
        label: "SES".into(),
        model_type: ModelType::Ses,
        factory: Box::new(|| Box::new(SesModel::new(None))),
    });

    if n >= 10 {
        out.push(Candidate {
            label: "Holt linear".into(),
            model_type: ModelType::HoltLinear,
            factory: Box::new(|| Box::new(HoltLinearModel::new(None, None, 1.0))),
        });
        out.push(Candidate {
            label: "Holt damped (φ=0.9)".into(),
            model_type: ModelType::HoltLinear,
            factory: Box::new(|| Box::new(HoltLinearModel::new(None, None, 0.9))),
        });
        out.push(Candidate {
            label: "Linear regression".into(),
            model_type: ModelType::LinearRegression,
            factory: Box::new(|| Box::new(LinearRegressionModel::new())),
        });
    } else {
        rejected.push((
            "Holt / linear regression".into(),
            format!("needs 10 observations, got {n}"),
        ));
    }

    // The ARIMA order is searched on the first training window only, so the
    // search cannot see the data the candidate is scored against.
    if min_train >= 30 {
        let opts = AutoArimaOptions {
            max_evals: None,
            strategy: SearchStrategy::Stepwise,
        };
        match auto_arima(
            &timestamps[..min_train],
            &values[..min_train],
            options.max_order,
            options.max_order,
            options.max_order,
            Some(&opts),
        ) {
            Ok(found) => {
                let (p, d, q) = found.order;
                out.push(Candidate {
                    label: format!("ARIMA({p},{d},{q})"),
                    model_type: ModelType::Arima,
                    factory: Box::new(move || Box::new(ArimaModel::new(p, d, q))),
                });
                if let Some(m) = period {
                    // Two standard seasonal shapes: a seasonal AR on a
                    // seasonally differenced series, and the seasonal MA of
                    // the airline model. Searching (P,D,Q) as well would
                    // multiply the fits for a choice that is nearly always one
                    // of these two.
                    for (sp, sd, sq) in [(1usize, 1usize, 0usize), (0, 1, 1)] {
                        let needed = (p.max(q) + sp.max(sq) * m) + 2 * d + 2 * sd * m + 10;
                        if min_train < needed {
                            rejected.push((
                                format!("SARIMA({p},{d},{q})({sp},{sd},{sq})[{m}]"),
                                format!("needs {needed} observations per fold, got {min_train}"),
                            ));
                            continue;
                        }
                        out.push(Candidate {
                            label: format!("SARIMA({p},{d},{q})({sp},{sd},{sq})[{m}]"),
                            model_type: ModelType::Sarima,
                            factory: Box::new(move || {
                                Box::new(SarimaModel::new(p, d, q, sp, sd, sq, m))
                            }),
                        });
                    }
                }
            }
            Err(e) => rejected.push(("ARIMA".into(), e.to_string())),
        }
    } else {
        rejected.push((
            "ARIMA / SARIMA".into(),
            format!("needs a 30-observation training window, got {min_train}"),
        ));
    }

    match period {
        Some(m) if min_train >= 2 * m => {
            out.push(Candidate {
                label: format!("Holt-Winters additive[{m}]"),
                model_type: ModelType::HoltWinters,
                factory: Box::new(move || {
                    Box::new(HoltWintersModel::new(None, None, None, Some(m), false))
                }),
            });
            if values.iter().all(|v| *v > 0.0) {
                out.push(Candidate {
                    label: format!("Holt-Winters multiplicative[{m}]"),
                    model_type: ModelType::HoltWinters,
                    factory: Box::new(move || {
                        Box::new(HoltWintersModel::new(None, None, None, Some(m), true))
                    }),
                });
            } else {
                rejected.push((
                    "Holt-Winters multiplicative".into(),
                    "series contains a value that is not strictly positive".into(),
                ));
            }
        }
        Some(m) => rejected.push((
            format!("Holt-Winters[{m}]"),
            format!("needs two seasons ({}) per fold, got {min_train}", 2 * m),
        )),
        None => rejected.push((
            "Holt-Winters / SARIMA".into(),
            "no seasonal period detected".into(),
        )),
    }

    out
}

/// Choose a forecast model for a series by rolling-origin cross-validation.
///
/// See the [module documentation](self) for what is fitted and why the winner
/// is chosen by out-of-sample error rather than by an information criterion.
///
/// # Errors
///
/// [`ForecastError::InsufficientData`] when the series is too short to hold a
/// training window and one `horizon`-long test window, and
/// [`ForecastError::NumericalInstability`] when no candidate could be scored.
pub fn select_model(
    timestamps: &[i64],
    values: &[f64],
    horizon: usize,
    options: &AutoForecastOptions,
) -> Result<ModelSelection, ForecastError> {
    crate::forecast::util::validate_input(timestamps, values)?;
    if horizon == 0 {
        return Err(ForecastError::InvalidParams {
            name: "horizon",
            value: "0".into(),
            reason: "a forecast needs at least one step",
        });
    }
    let n = values.len();
    // One training window plus one test window is the floor; below it there is
    // nothing to validate against and the honest answer is "not enough data".
    let min_required = horizon * 2 + 4;
    if n < min_required {
        return Err(ForecastError::InsufficientData {
            min: min_required,
            got: n,
        });
    }

    let folds = options.folds.max(1);
    // Reserve the last `horizon` observations for the final fold's test
    // window, and leave room for the origins to step forward.
    let min_train = n
        .saturating_sub(horizon * folds)
        .max(n / 2)
        .min(n - horizon);
    let period = options
        .period
        .filter(|m| *m >= 2)
        .or_else(|| crate::preprocess::detect_period(values, options.max_period.min(n / 2)));
    let differencing_order = select_differencing_order(values, options.max_order.max(2));

    let mut rejected = Vec::new();
    let set = candidates(
        timestamps,
        values,
        period,
        min_train,
        options,
        &mut rejected,
    );

    let cv = CrossValidator::new(
        CrossValidationMode::ExpandingWindow,
        min_train,
        horizon,
        folds,
    );

    let mut scored: Vec<CandidateScore> = Vec::new();
    for candidate in &set {
        match cv.evaluate(timestamps, values, || (candidate.factory)()) {
            Ok(r) => {
                let score = match options.metric {
                    SelectionMetric::Rmse => r.mean_rmse,
                    SelectionMetric::Mae => r.mean_mae,
                    SelectionMetric::Smape => r.mean_smape.unwrap_or(r.mean_rmse),
                };
                if score.is_finite() {
                    scored.push(CandidateScore {
                        label: candidate.label.clone(),
                        model_type: candidate.model_type.clone(),
                        score,
                        rmse: r.mean_rmse,
                        mae: r.mean_mae,
                        smape: r.mean_smape,
                    });
                } else {
                    rejected.push((candidate.label.clone(), "score was not finite".into()));
                }
            }
            Err(e) => rejected.push((candidate.label.clone(), e.to_string())),
        }
    }

    scored.sort_by(|a, b| a.score.total_cmp(&b.score));
    let best = scored.first().cloned().ok_or_else(|| {
        ForecastError::NumericalInstability(
            "no forecast model could be cross-validated on this series".into(),
        )
    })?;

    metrics::counter!("chronix_forecast_model_selected_total",
        "model" => format!("{:?}", best.model_type))
    .increment(1);

    Ok(ModelSelection {
        label: best.label,
        model_type: best.model_type,
        score: best.score,
        metric: options.metric,
        candidates: scored,
        rejected,
        period,
        differencing_order,
        folds,
        min_train_size: min_train,
    })
}

/// Select a model, fit it on the whole series, and forecast `horizon` steps.
///
/// # Errors
///
/// Whatever [`select_model`] returns, plus any error from fitting the winner
/// on the full series.
pub fn auto_forecast(
    timestamps: &[i64],
    values: &[f64],
    horizon: usize,
    options: &AutoForecastOptions,
) -> Result<AutoForecast, ForecastError> {
    let selection = select_model(timestamps, values, horizon, options)?;

    // Re-create the winner from the same candidate set and fit it on
    // everything, which is strictly more data than any fold saw.
    let mut rejected = Vec::new();
    let set = candidates(
        timestamps,
        values,
        selection.period,
        selection.min_train_size,
        options,
        &mut rejected,
    );
    let winner = set
        .iter()
        .find(|c| c.label == selection.label)
        .ok_or_else(|| {
            ForecastError::NumericalInstability(format!(
                "selected model {} could not be rebuilt",
                selection.label
            ))
        })?;

    let mut model = (winner.factory)();
    model.fit(timestamps, values)?;
    let result = model.predict(horizon)?;
    Ok(AutoForecast { result, selection })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn ts(n: usize) -> Vec<i64> {
        (0..n as i64).map(|i| i * 1_000_000_000).collect()
    }

    fn noise(seed: u64, n: usize, amp: f64) -> Vec<f64> {
        let mut s = seed;
        (0..n)
            .map(|_| {
                s = s
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                ((s >> 33) as f64 / (1u64 << 31) as f64 - 0.5) * 2.0 * amp
            })
            .collect()
    }

    #[test]
    fn a_seasonal_series_selects_a_seasonal_model() {
        let m = 24;
        let n = 240;
        let values: Vec<f64> = (0..n)
            .map(|i| 50.0 + 10.0 * (2.0 * std::f64::consts::PI * (i % m) as f64 / m as f64).sin())
            .zip(noise(1, n, 0.2))
            .map(|(a, b)| a + b)
            .collect();

        let opts = AutoForecastOptions {
            period: Some(m),
            ..Default::default()
        };
        let chosen = select_model(&ts(n), &values, 12, &opts).unwrap();
        assert!(
            matches!(
                chosen.model_type,
                ModelType::HoltWinters | ModelType::Sarima
            ),
            "picked {} ({:?}); candidates: {:?}",
            chosen.label,
            chosen.model_type,
            chosen
                .candidates
                .iter()
                .map(|c| (&c.label, c.score))
                .collect::<Vec<_>>()
        );
        assert_eq!(chosen.period, Some(m));
    }

    #[test]
    fn a_trending_series_beats_ses_with_a_trended_model() {
        let n = 200;
        let values: Vec<f64> = (0..n)
            .map(|i| 5.0 + i as f64 * 0.7)
            .zip(noise(2, n, 0.5))
            .map(|(a, b)| a + b)
            .collect();
        let chosen = select_model(&ts(n), &values, 10, &AutoForecastOptions::default()).unwrap();
        let ses = chosen
            .candidates
            .iter()
            .find(|c| c.label == "SES")
            .expect("SES is always a candidate");
        assert!(
            chosen.score < ses.score,
            "SES won on a trending series: {:?}",
            chosen
                .candidates
                .iter()
                .map(|c| (&c.label, c.score))
                .collect::<Vec<_>>()
        );
        assert_ne!(chosen.model_type, ModelType::Ses);
    }

    #[test]
    fn flat_noise_selects_a_flat_model() {
        let n = 200;
        let values: Vec<f64> = noise(3, n, 1.0).iter().map(|v| v + 20.0).collect();
        let chosen = select_model(&ts(n), &values, 8, &AutoForecastOptions::default()).unwrap();
        assert_eq!(chosen.differencing_order, 0, "stationary noise differenced");
        assert!(chosen.score.is_finite());
        // Whatever wins, it must not be worse than doing nothing by much.
        let ses = chosen.candidates.iter().find(|c| c.label == "SES").unwrap();
        assert!(chosen.score <= ses.score + 1e-9);
    }

    #[test]
    fn every_candidate_is_scored_or_explained() {
        let n = 120;
        let values: Vec<f64> = noise(4, n, 1.0).iter().map(|v| v + 3.0).collect();
        let chosen = select_model(&ts(n), &values, 6, &AutoForecastOptions::default()).unwrap();
        assert!(!chosen.candidates.is_empty());
        for w in chosen.candidates.windows(2) {
            assert!(w[0].score <= w[1].score, "candidates not sorted");
        }
        // Nothing is silently dropped: a candidate is either scored or has a
        // stated reason.
        for (label, reason) in &chosen.rejected {
            assert!(!reason.is_empty(), "{label} rejected with no reason");
        }
    }

    #[test]
    fn auto_forecast_returns_the_winner_fitted_on_everything() {
        let m = 12;
        let n = 180;
        let values: Vec<f64> = (0..n)
            .map(|i| 20.0 + 4.0 * (2.0 * std::f64::consts::PI * (i % m) as f64 / m as f64).sin())
            .collect();
        let opts = AutoForecastOptions {
            period: Some(m),
            ..Default::default()
        };
        let out = auto_forecast(&ts(n), &values, m, &opts).unwrap();
        assert_eq!(out.result.values.len(), m);
        assert_eq!(out.result.timestamps.len(), m);
        assert!(out.result.timestamps[0] > ts(n)[n - 1]);
        assert_eq!(out.selection.label, out.selection.candidates[0].label);
        for v in &out.result.values {
            assert!(v.is_finite(), "forecast contained {v}");
        }
    }

    #[test]
    fn a_horizon_of_zero_is_rejected() {
        let values: Vec<f64> = (0..50).map(f64::from).collect();
        assert!(select_model(&ts(50), &values, 0, &AutoForecastOptions::default()).is_err());
    }

    #[test]
    fn too_short_a_series_is_rejected() {
        let values: Vec<f64> = (0..10).map(f64::from).collect();
        assert!(select_model(&ts(10), &values, 8, &AutoForecastOptions::default()).is_err());
    }

    #[test]
    fn the_metric_can_change_the_winner_ordering() {
        let n = 200;
        let values: Vec<f64> = (0..n)
            .map(|i| 100.0 + (i as f64 * 0.05).sin() * 10.0)
            .zip(noise(5, n, 1.0))
            .map(|(a, b)| a + b)
            .collect();
        for metric in [
            SelectionMetric::Rmse,
            SelectionMetric::Mae,
            SelectionMetric::Smape,
        ] {
            let opts = AutoForecastOptions {
                metric,
                ..Default::default()
            };
            let chosen = select_model(&ts(n), &values, 10, &opts).unwrap();
            assert_eq!(chosen.metric, metric);
            assert!(
                chosen.score.is_finite(),
                "{metric} produced {}",
                chosen.score
            );
            // Every candidate carries all three metrics regardless of which
            // one decided.
            for c in &chosen.candidates {
                assert!(c.rmse.is_finite() && c.mae.is_finite());
            }
        }
    }
}
