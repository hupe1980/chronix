#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap
//! `[analytics]` is configuration that is read.
//!
//! The whole section — `default_forecast_model`, `default_anomaly_method`,
//! `max_forecast_horizon`, `max_training_points` and the per-measurement
//! overrides — was accepted, validated, documented in the configuration table
//! and **read nowhere**: `ChronixConfig::effective_analytics` had no callers
//! outside the file that defines it. Two of those settings are bounds, and a
//! bound nothing enforces is not a bound: `SELECT forecast(v, _time, 2000000)`
//! was answered against a configured limit of 8 760, returning a
//! two-million-element list from one cell.

use std::sync::Arc;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

const SEC: i64 = 1_000_000_000;
const T: i64 = 1_700_000_000 * SEC;

fn db_with(dir: &tempfile::TempDir, analytics: chronix_core::AnalyticsConfig) -> Arc<Chronix> {
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .analytics(analytics)
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());
    let points: Vec<Point> = (0..200i64)
        .map(|i| {
            Point::new(
                SeriesKey::new("m", tags! { "h" => "a" }).unwrap(),
                fields! { "v" => i as f64 },
                T + i * SEC,
            )
            .unwrap()
        })
        .collect();
    db.insert_batch(&points).unwrap().into_complete().unwrap();
    db
}

#[test]
fn a_horizon_over_the_configured_limit_is_refused_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let analytics = chronix_core::AnalyticsConfig {
        max_forecast_horizon: 24,
        ..Default::default()
    };
    let db = db_with(&dir, analytics);

    // At the limit: answered.
    let ok = db.sql("SELECT forecast(v, _time, 24) AS f FROM m").unwrap();
    assert_eq!(ok.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

    // Over it: refused, and the message names the setting to change.
    for sql in [
        "SELECT forecast(v, _time, 25) AS f FROM m",
        "SELECT auto_forecast(v, _time, 25) AS f FROM m",
    ] {
        let err = db.sql(sql).expect_err(sql).to_string();
        assert!(
            err.contains("max_forecast_horizon") && err.contains("25"),
            "{sql}: {err}"
        );
    }
}

/// The default is the documented one, and it binds.
#[test]
fn the_default_horizon_limit_binds() {
    let dir = tempfile::tempdir().unwrap();
    let db = db_with(&dir, chronix_core::AnalyticsConfig::default());
    assert_eq!(db.config().analytics.max_forecast_horizon, 8760);
    let err = db
        .sql("SELECT forecast(v, _time, 2000000) AS f FROM m")
        .expect_err("an unbounded horizon must be refused")
        .to_string();
    assert!(err.contains("max_forecast_horizon"), "{err}");
}
