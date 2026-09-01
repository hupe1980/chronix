#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Automatic model selection through the embedded API.
//!
//! Driven through `Chronix::auto_forecast` rather than through
//! `select_model`, because the selection is only useful if the window it is
//! given comes from a real read.

use std::sync::Arc;

use chronix::chronix_analytics::forecast::{AutoForecastOptions, ModelType, SelectionMetric};
use chronix::prelude::*;
use chronix::{fields, tags, Chronix, ForecastConfig};

const SEC: i64 = 1_000_000_000;

fn db_with(values: &[f64]) -> (tempfile::TempDir, Arc<Chronix>) {
    let dir = tempfile::tempdir().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    let points: Vec<Point> = values
        .iter()
        .enumerate()
        .map(|(i, v)| Point::new(key.clone(), fields! { "v" => *v }, i as i64 * SEC).unwrap())
        .collect();
    let _ = db.insert_batch(&points).unwrap();
    db.flush().unwrap();
    (dir, db)
}

fn seasonal(n: usize, m: usize) -> Vec<f64> {
    (0..n)
        .map(|i| 50.0 + 10.0 * (2.0 * std::f64::consts::PI * (i % m) as f64 / m as f64).sin())
        .collect()
}

#[test]
fn auto_forecast_picks_a_seasonal_model_for_a_seasonal_series() {
    let m = 24;
    let n = 240;
    let (_dir, db) = db_with(&seasonal(n, m));

    let options = AutoForecastOptions {
        period: Some(m),
        ..Default::default()
    };
    let out = db
        .auto_forecast(
            "m",
            "v",
            &[("host", "a")],
            0,
            n as i64 * SEC,
            12,
            Some(options),
        )
        .unwrap();

    assert_eq!(out.result.values.len(), 12);
    assert!(
        matches!(
            out.selection.model_type,
            ModelType::HoltWinters | ModelType::Sarima
        ),
        "picked {}",
        out.selection.label
    );
    assert_eq!(out.selection.period, Some(m));

    // The winner must actually track the season: the next 12 points continue
    // the sine, so a flat forecast at the mean would be roughly 6.4 off on
    // average.
    let truth = seasonal(n + 12, m);
    let mae: f64 = out
        .result
        .values
        .iter()
        .zip(&truth[n..])
        .map(|(a, b)| (a - b).abs())
        .sum::<f64>()
        / 12.0;
    assert!(mae < 3.0, "seasonal forecast MAE {mae:.3}");
}

#[test]
fn the_selection_report_explains_itself() {
    let (_dir, db) = db_with(&seasonal(200, 12));
    let out = db
        .auto_forecast("m", "v", &[("host", "a")], 0, 200 * SEC, 6, None)
        .unwrap();

    let s = &out.selection;
    assert!(!s.candidates.is_empty(), "no candidate was scored");
    assert_eq!(
        s.label, s.candidates[0].label,
        "winner is not the best score"
    );
    for w in s.candidates.windows(2) {
        assert!(w[0].score <= w[1].score, "candidates are not sorted");
    }
    assert_eq!(s.metric, SelectionMetric::Rmse);
    assert!(s.folds >= 1 && s.min_train_size > 0);
    // Anything not scored says why.
    for (label, reason) in &s.rejected {
        assert!(!reason.is_empty(), "{label} was dropped without a reason");
    }
}

#[test]
fn confidence_is_honoured_rather_than_ignored() {
    let (_dir, db) = db_with(
        &(0..120)
            .map(|i| 20.0 + f64::from(i) * 0.1)
            .collect::<Vec<_>>(),
    );
    let window = (0, 120 * SEC);

    let narrow = db
        .forecast(
            "m",
            "v",
            &[("host", "a")],
            window.0,
            window.1,
            5,
            Some(ForecastConfig {
                confidence: 0.80,
                ..Default::default()
            }),
        )
        .unwrap();
    let wide = db
        .forecast(
            "m",
            "v",
            &[("host", "a")],
            window.0,
            window.1,
            5,
            Some(ForecastConfig {
                confidence: 0.99,
                ..Default::default()
            }),
        )
        .unwrap();

    assert!((narrow.confidence_level - 0.80).abs() < 1e-12);
    assert!((wide.confidence_level - 0.99).abs() < 1e-12);
    for h in 0..5 {
        assert!(
            (narrow.values[h] - wide.values[h]).abs() < 1e-9,
            "point forecast moved"
        );
        let narrow_w = narrow.confidence_upper[h] - narrow.confidence_lower[h];
        let wide_w = wide.confidence_upper[h] - wide.confidence_lower[h];
        assert!(
            wide_w > narrow_w * 1.5,
            "99% interval ({wide_w}) is not meaningfully wider than 80% ({narrow_w})"
        );
    }
}

#[test]
fn an_unknown_model_name_is_an_error_not_a_silent_fallback() {
    let (_dir, db) = db_with(&(0..80).map(f64::from).collect::<Vec<_>>());
    let err = db.forecast(
        "m",
        "v",
        &[("host", "a")],
        0,
        80 * SEC,
        5,
        Some(ForecastConfig {
            // A plausible typo for "holt_winters".
            model: Some("holtwinters".into()),
            ..Default::default()
        }),
    );
    let message = err.expect_err("unknown model was accepted").to_string();
    assert!(
        message.contains("unknown model"),
        "unhelpful error: {message}"
    );
}

#[test]
fn too_short_a_window_is_reported_rather_than_guessed() {
    let (_dir, db) = db_with(&(0..12).map(f64::from).collect::<Vec<_>>());
    assert!(db
        .auto_forecast("m", "v", &[("host", "a")], 0, 12 * SEC, 10, None)
        .is_err());
}
