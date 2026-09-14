//! What every forecasting model does with the series a user actually has.
//!
//! `clippy::indexing_slicing` is `allow` at workspace level with 1 400+ sites,
//! "mostly the numeric kernels" — so the kernels are the one place where an
//! out-of-bounds index is not a compile-time-visible risk. This asks what they
//! do when handed an empty series, one point, all-NaN, a constant, or a
//! seasonal period longer than the data.
//!
//! It found one: `HoltWintersModel::fit(&[], &[])` indexed `values[0]` and
//! **panicked**, on a method that returns `Result` and had an error channel
//! going spare. In an embedded database a panic is the host process going
//! down — `hemsd` runs one process on a gateway nobody visits.
//!
//! The other models survived an empty series by luck rather than by checking:
//! their fitting loops are no-ops over it, and they then answered `predict()`
//! from an uninitialised level. So the fix went into `validate_input`, the one
//! function all of them call, rather than into the model that happened to
//! crash — and this file exists so the next model added gets the same
//! question asked of it.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use chronix_analytics::forecast::{
    ArimaModel, ForecastModel, HoltLinearModel, HoltWintersModel, LinearRegressionModel, SesModel,
};

/// Every `ForecastModel` implementor, boxed.
///
/// Taken as a list here rather than from a registry because the trait has no
/// registry; the guard against it going stale is that a new model with no
/// entry is a model nobody asked these questions of.
fn models() -> Vec<(&'static str, Box<dyn ForecastModel>)> {
    vec![
        ("SesModel", Box::new(SesModel::new(Some(0.3)))),
        (
            "HoltLinearModel",
            Box::new(HoltLinearModel::new(Some(0.3), Some(0.1), 1.0)),
        ),
        (
            "HoltWintersModel",
            Box::new(HoltWintersModel::new(
                Some(0.3),
                Some(0.1),
                Some(0.1),
                Some(4),
                false,
            )),
        ),
        (
            "HoltWintersModel/multiplicative",
            Box::new(HoltWintersModel::new(
                Some(0.3),
                Some(0.1),
                Some(0.1),
                Some(4),
                true,
            )),
        ),
        (
            "LinearRegressionModel",
            Box::new(LinearRegressionModel::new()),
        ),
        ("ArimaModel", Box::new(ArimaModel::new(1, 0, 1))),
    ]
}

fn ts(n: usize) -> Vec<i64> {
    (0..n as i64).map(|i| i * 1_000_000_000).collect()
}

/// Series a user really has, each with a reason it is here.
fn degenerate() -> Vec<(&'static str, Vec<f64>)> {
    vec![
        ("empty", vec![]),                                // a measurement with no data yet
        ("one", vec![1.0]),                               // the first scrape
        ("two", vec![1.0, 2.0]),                          // fewer points than any period
        ("constant", vec![5.0; 50]),                      // an idle sensor
        ("all-nan", vec![f64::NAN; 8]),                   // a probe that never succeeded
        ("infinite", vec![f64::INFINITY, 0.0, 1.0, 2.0]), // a divide-by-zero upstream
        (
            "extreme",
            vec![f64::MAX, f64::MIN, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
        ),
        ("subnormal", vec![f64::MIN_POSITIVE; 12]),
    ]
}

#[test]
fn no_model_panics_on_a_degenerate_series() {
    let mut panicked = Vec::new();
    for (series_name, values) in degenerate() {
        for (model_name, _) in models() {
            let v = values.clone();
            let t = ts(v.len());
            let label = format!("{model_name} on {series_name}");
            // Rebuild inside the closure: `Box<dyn ForecastModel>` is not
            // unwind-safe and the point is the arithmetic, not the box.
            let m = model_name.to_string();
            let result = std::panic::catch_unwind(move || {
                let mut model = models()
                    .into_iter()
                    .find(|(n, _)| *n == m)
                    .expect("model")
                    .1;
                if model.fit(&t, &v).is_ok() {
                    let _ = model.predict(5);
                }
            });
            if result.is_err() {
                panicked.push(label);
            }
        }
    }
    assert!(
        panicked.is_empty(),
        "a forecasting model panicked instead of returning its `Result`. In an \
         embedded database that is the host process going down: {panicked:#?}"
    );
}

#[test]
fn fitting_an_empty_series_is_an_error_for_every_model() {
    // Not "does not panic" — an error. A model fitted on no observations
    // would otherwise answer `predict()` from an uninitialised level, which
    // is a number with nothing behind it.
    for (name, mut model) in models() {
        let err = model.fit(&[], &[]).expect_err(&format!(
            "{name} accepted an empty series; `predict` would then answer from nothing"
        ));
        let msg = err.to_string();
        assert!(
            msg.contains("empty"),
            "{name}'s refusal should say what was wrong with the input: {msg}"
        );
    }
}

#[test]
fn a_series_shorter_than_its_period_does_not_panic() {
    // The seasonal models take a period; a user who declares `period = 24`
    // and has six points is the ordinary first-day-of-a-deployment case.
    for period in [2usize, 7, 24, 100, 10_000] {
        for multiplicative in [false, true] {
            let values: Vec<f64> = (0..6).map(|i| f64::from(i) + 1.0).collect();
            let t = ts(values.len());
            let r = std::panic::catch_unwind(move || {
                let mut m = HoltWintersModel::new(
                    Some(0.3),
                    Some(0.1),
                    Some(0.1),
                    Some(period),
                    multiplicative,
                );
                if m.fit(&t, &values).is_ok() {
                    let _ = m.predict(3);
                }
            });
            assert!(
                r.is_ok(),
                "Holt-Winters panicked with period={period} \
                 multiplicative={multiplicative} on six points"
            );
        }
    }
}

// ── The anomaly detectors, asked the same questions ──────────────────

use chronix_analytics::anomaly::{
    AnomalyDetector, CusumDetector, DynamicThresholdDetector, ForecastResidualDetector,
    IqrDetector, ModifiedZScoreDetector, MovingAverageResidualDetector, ZScoreDetector,
};

fn detectors() -> Vec<(&'static str, Box<dyn AnomalyDetector>)> {
    vec![
        ("ZScoreDetector", Box::new(ZScoreDetector::new(None))),
        (
            "ModifiedZScoreDetector",
            Box::new(ModifiedZScoreDetector::new(None)),
        ),
        ("IqrDetector", Box::new(IqrDetector::new(None))),
        ("CusumDetector", Box::new(CusumDetector::new(None, None))),
        (
            "MovingAverageResidualDetector",
            Box::new(MovingAverageResidualDetector::new(None, None)),
        ),
        (
            "ForecastResidualDetector",
            Box::new(ForecastResidualDetector::new(None, None)),
        ),
        (
            "DynamicThresholdDetector",
            Box::new(DynamicThresholdDetector::new(None, None)),
        ),
    ]
}

#[test]
fn no_detector_panics_on_a_degenerate_series() {
    let mut panicked = Vec::new();
    for (series_name, values) in degenerate() {
        for (detector_name, _) in detectors() {
            let v = values.clone();
            let t = ts(v.len());
            let label = format!("{detector_name} on {series_name}");
            let d = detector_name.to_string();
            let result = std::panic::catch_unwind(move || {
                let mut det = detectors()
                    .into_iter()
                    .find(|(n, _)| *n == d)
                    .expect("detector")
                    .1;
                if det.fit(&t, &v).is_ok() {
                    // Score the training data back, and values it never saw.
                    let _ = det.detect(&t, &v);
                    for x in v.iter().copied().chain([0.0, f64::MAX, -1.0]) {
                        let _ = det.detect_point(0, x);
                    }
                }
            });
            if result.is_err() {
                panicked.push(label);
            }
        }
    }
    assert!(
        panicked.is_empty(),
        "an anomaly detector panicked instead of returning its `Result`: {panicked:#?}"
    );
}

#[test]
fn scoring_before_fitting_does_not_panic() {
    // The order a caller gets wrong first. Every detector must answer or
    // refuse — `score` on an unfitted detector reads uninitialised
    // parameters, which are zeroes, and a z-score with a zero standard
    // deviation is a division this must not perform blindly.
    // `detect_point` is the streaming entry point, so it is the one a
    // trigger evaluation reaches first.
    for (name, mut det) in detectors() {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = det.detect_point(0, 42.0);
        }));
        assert!(r.is_ok(), "{name} panicked when scored before being fitted");
    }
}
