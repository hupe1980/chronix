//! What does one call to `MultivariateForecastModel` promise, across every
//! implementor?
//!
//! The trait has two implementors that return a **different number of rows**
//! for the same call: `MultiLinearRegression` is many-predictors-to-one-target
//! and returns one, `VarModel` fits every equation of the system and returns
//! one per series. Neither the type nor the documentation said so, and
//! `VarModel::fit` bound the parameter as `_target_idx` and discarded it — so
//! generic code over `Box<dyn MultivariateForecastModel>` reading
//! `predictions[0]` got the requested series from one implementor and series
//! index 0 from the other. A plausible number belonging to a different series.
//!
//! These tests take the implementor list from this file rather than from
//! memory, so a third implementor has to answer the same questions.

use chronix_analytics::multivariate::{
    MultiLinearRegression, MultiSeriesContext, MultivariateForecastModel, VarModel,
};

/// Three series at wildly different levels, so "which row is this?" is
/// answerable from the value alone.
fn ctx() -> MultiSeriesContext {
    let n = 200usize;
    let ts: Vec<i64> = (0..n as i64).map(|i| i * 1_000_000_000).collect();
    let mk = |base: f64| -> Vec<f64> { (0..n).map(|i| base + (i as f64) * 0.01).collect() };
    MultiSeriesContext::build(
        vec![
            ("s0".into(), ts.clone(), mk(10.0)),
            ("s1".into(), ts.clone(), mk(1_000.0)),
            ("s2".into(), ts, mk(50_000.0)),
        ],
        None,
    )
    .expect("context")
}

fn implementors() -> Vec<(&'static str, Box<dyn MultivariateForecastModel>)> {
    vec![
        (
            "MultiLinearRegression",
            Box::new(MultiLinearRegression::new()),
        ),
        ("VarModel", Box::new(VarModel::new(Some(1)))),
    ]
}

#[test]
fn every_model_answers_for_the_series_the_caller_asked_for() {
    const TARGET: usize = 2; // "s2", around 50 000
    for (name, mut model) in implementors() {
        model.fit(&ctx(), TARGET).expect("fit");
        let r = model.predict(3).expect("predict");

        assert_eq!(r.target, "s2", "{name} lost the requested target");
        let got = r
            .target_forecast()
            .unwrap_or_else(|| panic!("{name}: target_forecast() found no row for its own target"));
        assert!(
            (got[0] - 50_000.0).abs() < 100.0,
            "{name}: target_forecast() answered {:.2}, which is not s2 (~50000). \
             This is the defect: the caller asked for series {TARGET} and got \
             a plausible value belonging to a different series.",
            got[0]
        );
    }
}

#[test]
fn every_row_of_a_forecast_says_which_series_it_is() {
    for (name, mut model) in implementors() {
        model.fit(&ctx(), 2).expect("fit");
        let r = model.predict(3).expect("predict");
        assert_eq!(
            r.series.len(),
            r.predictions.len(),
            "{name}: {} rows of predictions but {} names for them",
            r.predictions.len(),
            r.series.len()
        );
        assert!(
            r.series
                .iter()
                .all(|s| ["s0", "s1", "s2"].contains(&s.as_str())),
            "{name} named a row after a series that was not in the input: {:?}",
            r.series
        );
        for s in &r.series {
            assert!(
                r.for_series(s).is_some(),
                "{name}: `{s}` is named and absent"
            );
        }
    }
}

#[test]
fn a_target_index_out_of_range_is_refused_by_every_model() {
    // `VarModel` used to accept any index, because it ignored the parameter.
    for (name, mut model) in implementors() {
        assert!(
            model.fit(&ctx(), 99).is_err(),
            "{name} accepted target_idx 99 for a three-series context"
        );
    }
}
