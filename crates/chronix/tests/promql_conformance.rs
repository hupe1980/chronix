#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap
#![allow(clippy::needless_pass_by_value)]
//! End-to-end PromQL conformance against data in a real database.
//!
//! The unit tests in `promql::eval::function` call the compute helpers
//! directly, which is how the `rate()` defect survived: its extrapolation branch
//! was only ever reached with `range_ns: None`, and the one test that did pass
//! a range asserted `> 0 && is_finite`. These go through parse → select →
//! evaluate against stored samples, the same path Grafana drives.

use chronix::prelude::*;
use chronix::promql::{self, PromQLEvaluator};
use chronix::{fields, tags, Chronix};
use std::sync::Arc;

const SEC: i64 = 1_000_000_000;

fn db_with(points: &[Point]) -> Arc<Chronix> {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .wal_fsync_policy(chronix_core::FsyncPolicy::Periodic(
            std::time::Duration::from_secs(1),
        ))
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());
    db.insert_batch(points).unwrap().into_complete().unwrap();
    db
}

fn instant(db: &Arc<Chronix>, query: &str, at: i64) -> Vec<(Vec<(String, String)>, f64)> {
    let expr = promql::parse(query).unwrap();
    let ev = PromQLEvaluator::new(db.clone());
    let params = promql::eval::QueryParams {
        time: at,
        ..Default::default()
    };
    match ev.instant_query(&expr, &params).unwrap() {
        promql::PromQLValue::Vector(series) => series
            .into_iter()
            .map(|s| {
                let v = s.samples.first().map_or(f64::NAN, |sm| sm.value);
                (s.labels, v)
            })
            .collect(),
        other => panic!("expected an instant vector, got {other:?}"),
    }
}

fn one_value(db: &Arc<Chronix>, query: &str, at: i64) -> f64 {
    let r = instant(db, query, at);
    assert_eq!(r.len(), 1, "{query}: expected one series, got {}", r.len());
    r[0].1
}

/// A counter scraped every 15 s, queried with `rate(...[1m])`, must
/// return the true per-second slope. Prometheus extrapolates the observed
/// increase up to the range window and then divides by the range; the two
/// operations cancel for an evenly scraped counter.
#[test]
fn rate_over_a_range_matches_the_true_slope() {
    let key = SeriesKey::new("http_requests_total", tags! { "job" => "api" }).unwrap();
    // Scrapes at 15, 30, 45, 60 s; +10 per scrape.
    let points: Vec<Point> = (1..=4)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "value" => 100.0 + f64::from(i - 1) * 10.0 },
                i64::from(i) * 15 * SEC,
            )
            .unwrap()
        })
        .collect();
    let db = db_with(&points);

    let rate = one_value(&db, "rate(http_requests_total[1m])", 60 * SEC);
    assert!(
        (rate - 10.0 / 15.0).abs() < 1e-6,
        "rate = {rate}, expected {} (Prometheus semantics)",
        10.0 / 15.0
    );

    // increase over the same window scales the observed +30 to the minute.
    let inc = one_value(&db, "increase(http_requests_total[1m])", 60 * SEC);
    assert!((inc - 40.0).abs() < 1e-6, "increase = {inc}, expected 40");
}

/// A counter reset must be corrected, not read as a large negative delta.
#[test]
fn rate_corrects_a_counter_reset() {
    let key = SeriesKey::new("c", tags! { "job" => "a" }).unwrap();
    // 100, 110, then a restart to 5, then 15 — a true increase of 10 per step.
    let values = [100.0, 110.0, 5.0, 15.0];
    let points: Vec<Point> = values
        .iter()
        .enumerate()
        .map(|(i, v)| {
            Point::new(
                key.clone(),
                fields! { "value" => *v },
                (i as i64 + 1) * 15 * SEC,
            )
            .unwrap()
        })
        .collect();
    let db = db_with(&points);

    let inc = one_value(&db, "increase(c[1m])", 60 * SEC);
    // Observed increase across the window: 10 + 5 (reset, counts as the new
    // value) + 10 = 25, extrapolated over 60/45.
    assert!(inc > 0.0, "reset produced a non-positive increase: {inc}");
    assert!(
        (inc - 25.0 * 60.0 / 45.0).abs() < 1e-6,
        "increase after reset = {inc}"
    );
}

/// `delta()` uses the same extrapolation but no reset correction.
#[test]
fn delta_extrapolates_to_the_range() {
    let key = SeriesKey::new("g", tags! { "job" => "a" }).unwrap();
    let points: Vec<Point> = (1..=4)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "value" => f64::from(i) * 10.0 },
                i64::from(i) * 15 * SEC,
            )
            .unwrap()
        })
        .collect();
    let db = db_with(&points);

    // Observed delta 30 over 45 s, extrapolated to 60 s → 40.
    let d = one_value(&db, "delta(g[1m])", 60 * SEC);
    assert!((d - 40.0).abs() < 1e-6, "delta = {d}, expected 40");
}

/// `histogram_quantile` must interpolate inside the containing bucket and
/// report the highest finite bound when the quantile lands in `+Inf`.
#[test]
fn histogram_quantile_interpolates_and_caps_at_the_last_finite_bound() {
    // Cumulative buckets: le=1 → 2, le=2 → 6, le=+Inf → 8.
    let buckets = [("1", 2.0), ("2", 6.0), ("+Inf", 8.0)];
    let points: Vec<Point> = buckets
        .iter()
        .map(|(le, count)| {
            let key = SeriesKey::new("h_bucket", tags! { "le" => *le }).unwrap();
            Point::new(key, fields! { "value" => *count }, 10 * SEC).unwrap()
        })
        .collect();
    let db = db_with(&points);

    // q=0.5 → rank 4, which falls in the (1, 2] bucket holding 4 observations
    // starting at cumulative 2: 1 + (2-1) * (4-2)/4 = 1.5.
    let q50 = one_value(&db, "histogram_quantile(0.5, h_bucket)", 10 * SEC);
    assert!((q50 - 1.5).abs() < 1e-9, "p50 = {q50}, expected 1.5");

    // q=0.9 → rank 7.2, which falls in the +Inf bucket → highest finite bound.
    let q90 = one_value(&db, "histogram_quantile(0.9, h_bucket)", 10 * SEC);
    assert!((q90 - 2.0).abs() < 1e-9, "p90 = {q90}, expected 2.0");

    // Out-of-range quantiles are ±Inf per the spec.
    assert!(one_value(&db, "histogram_quantile(1.5, h_bucket)", 10 * SEC).is_infinite());
    assert!(one_value(&db, "histogram_quantile(-0.5, h_bucket)", 10 * SEC) < 0.0);
}

/// A histogram with a single bucket has no interval to interpolate in, so
/// Prometheus returns NaN rather than a fabricated bound.
#[test]
fn histogram_quantile_needs_at_least_two_buckets() {
    let key = SeriesKey::new("h1_bucket", tags! { "le" => "+Inf" }).unwrap();
    let db = db_with(&[Point::new(key, fields! { "value" => 5.0 }, 10 * SEC).unwrap()]);

    let v = one_value(&db, "histogram_quantile(0.5, h1_bucket)", 10 * SEC);
    assert!(v.is_nan(), "single-bucket histogram gave {v}, expected NaN");
}

/// Instant selectors respect the lookback window rather than returning the
/// newest sample regardless of age.
#[test]
fn instant_selector_respects_the_lookback_window() {
    let key = SeriesKey::new("stale", tags! { "job" => "a" }).unwrap();
    let db = db_with(&[Point::new(key, fields! { "value" => 1.0 }, 10 * SEC).unwrap()]);

    // Within the default 5 m lookback the sample is visible …
    assert_eq!(instant(&db, "stale", 100 * SEC).len(), 1);
    // … an hour later it is not.
    assert!(
        instant(&db, "stale", 3_600 * SEC).is_empty(),
        "a sample older than the lookback delta must not be returned"
    );
}

// ── Range-query shape ──────────────────────────────────────────────────

/// Evaluate a range query and return one series' `(timestamp, value)` points.
fn range(db: &Arc<Chronix>, query: &str, start: i64, end: i64, step: i64) -> Vec<Vec<(i64, f64)>> {
    let expr = promql::parse(query).unwrap();
    let ev = PromQLEvaluator::new(db.clone());
    let params = promql::eval::QueryParams {
        time: start,
        start: Some(start),
        end: Some(end),
        step: Some(step),
        ..Default::default()
    };
    match ev.range_query(&expr, &params).unwrap() {
        promql::PromQLValue::Matrix(series) => series
            .into_iter()
            .map(|s| {
                s.samples
                    .into_iter()
                    .map(|sm| (sm.timestamp, sm.value))
                    .collect()
            })
            .collect(),
        other => panic!("expected a matrix, got {other:?}"),
    }
}

/// Every point a range query returns must land on a step boundary, and no
/// timestamp may repeat.
///
/// This is the shape bug, not a value bug, and it was invisible to a suite
/// that only checked values. `avg_over_time` and friends stamped the *last
/// input sample's* timestamp on their output, so with a step finer than the
/// scrape interval two adjacent steps saw the same newest sample and emitted
/// it twice. The result is a matrix that is neither step-aligned nor strictly
/// increasing — which the Prometheus HTTP API forbids and Grafana renders as
/// a flat spot rather than an error.
#[test]
fn a_range_query_returns_one_point_per_step() {
    let key = SeriesKey::new("temp", tags! { "room" => "hall" }).unwrap();
    // Samples every 60 s; the query steps every 20 s, so most steps see no
    // new sample and reuse the previous one via lookback.
    let points: Vec<Point> = (0..5)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "value" => 20.0 + f64::from(i) },
                i64::from(i) * 60 * SEC,
            )
            .unwrap()
        })
        .collect();
    let db = db_with(&points);

    for query in [
        "temp",
        "avg_over_time(temp[2m])",
        "max_over_time(temp[2m])",
        "changes(temp[2m])",
        "deriv(temp[2m])",
        "irate(temp[5m])",
        "predict_linear(temp[5m], 60)",
        "rate(temp[2m])",
    ] {
        let series = range(&db, query, 60 * SEC, 240 * SEC, 20 * SEC);
        assert_eq!(series.len(), 1, "{query}: expected one series");
        let points = &series[0];

        for (ts, _) in points {
            assert_eq!(
                (ts - 60 * SEC) % (20 * SEC),
                0,
                "{query}: point at {ts} is not on a step boundary"
            );
        }
        for w in points.windows(2) {
            assert!(
                w[1].0 > w[0].0,
                "{query}: timestamps must strictly increase, got {} then {}",
                w[0].0,
                w[1].0
            );
        }
    }
}

/// `count_over_time` counts samples, and `present_over_time` reports presence.
///
/// Both used to run through a blanket "skip NaN" filter shared by the whole
/// `*_over_time` family, so a window holding NaN samples was under-counted and
/// a window holding *only* NaN samples reported nothing at all — from the one
/// function whose entire job is to say whether anything was there.
///
/// The engine refuses to *store* a non-finite value, so the NaN is
/// manufactured the only way it can occur: `0 / 0` inside a subquery, which is
/// also how it reaches these functions in practice.
#[test]
fn nan_samples_are_counted_and_present() {
    let key = SeriesKey::new("z", tags! { "id" => "1" }).unwrap();
    let points: Vec<Point> = (1..=4)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "value" => 0.0 },
                i64::from(i) * 10 * SEC,
            )
            .unwrap()
        })
        .collect();
    let db = db_with(&points);

    // Four subquery steps at 10, 20, 30 and 40 s, each evaluating 0/0 = NaN.
    let count = one_value(&db, "count_over_time((z / z)[35s:10s])", 40 * SEC);
    assert!(
        (count - 4.0).abs() < 1e-9,
        "count_over_time must count NaN samples too, got {count}"
    );

    // present_over_time answers presence, not numerosity.
    let present = one_value(&db, "present_over_time((z / z)[35s:10s])", 40 * SEC);
    assert!(
        (present - 1.0).abs() < 1e-9,
        "present_over_time must report an all-NaN window, got {present}"
    );

    // sum/avg see the raw values, so NaN propagates.
    let sum = one_value(&db, "sum_over_time((z / z)[35s:10s])", 40 * SEC);
    assert!(sum.is_nan(), "sum_over_time must propagate NaN, got {sum}");

    // max_over_time is the other half of the rule: it skips NaN, and an
    // all-NaN window is NaN rather than a missing series.
    let max = one_value(&db, "max_over_time((z / z)[35s:10s])", 40 * SEC);
    assert!(max.is_nan(), "an all-NaN window has no maximum, got {max}");
}

/// `topk` and `sort_desc` rank `NaN` last, not first.
///
/// `f64::total_cmp` gives NaN a fixed slot in the total order; reversing it to
/// sort descending moved NaN to the *top*, so `topk(1, …)` over a group with
/// one broken series returned the broken one and hid the real maximum.
#[test]
fn nan_ranks_last_in_topk_and_sort_desc() {
    // `load / on(host) d` yields NaN for host a (0/0), 5 for b and 9 for c.
    let mk = |m: &str, host: &str, v: f64| {
        Point::new(
            SeriesKey::new(m, tags! { "host" => host }).unwrap(),
            fields! { "value" => v },
            10 * SEC,
        )
        .unwrap()
    };
    let db = db_with(&[
        mk("load", "a", 0.0),
        mk("load", "b", 5.0),
        mk("load", "c", 9.0),
        mk("d", "a", 0.0),
        mk("d", "b", 1.0),
        mk("d", "c", 1.0),
    ]);

    let ratio = "load / on(host) d";

    let top = instant(&db, &format!("topk(1, {ratio})"), 10 * SEC);
    assert_eq!(top.len(), 1);
    assert!(
        (top[0].1 - 9.0).abs() < 1e-9,
        "topk(1) must return the largest real value, got {}",
        top[0].1
    );

    let sorted = instant(&db, &format!("sort_desc({ratio})"), 10 * SEC);
    assert_eq!(sorted.len(), 3);
    assert!(
        sorted.last().unwrap().1.is_nan(),
        "sort_desc must place NaN last, got {:?}",
        sorted.iter().map(|s| s.1).collect::<Vec<_>>()
    );
    assert!(
        (sorted[0].1 - 9.0).abs() < 1e-9,
        "sort_desc must still order the real values, got {:?}",
        sorted.iter().map(|s| s.1).collect::<Vec<_>>()
    );
}

/// A range selector is left-open: a sample exactly on the older boundary
/// belongs to the previous window.
///
/// Prometheus 3.0 made this change so that evenly spaced samples yield a
/// constant count — with 10 s samples, `[30s]` selects three of them whether
/// or not one lands on the boundary. Left-closed selection returns four at the
/// aligned instant and three everywhere else, which shows up as a periodic
/// step in `count_over_time` and a periodic dip in `rate`.
#[test]
fn a_range_selector_excludes_its_left_boundary() {
    let key = SeriesKey::new("ticks", tags! { "id" => "1" }).unwrap();
    let points: Vec<Point> = (0..=6)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "value" => f64::from(i) },
                i64::from(i) * 10 * SEC,
            )
            .unwrap()
        })
        .collect();
    let db = db_with(&points);

    // Window (30s, 60s] holds the samples at 40, 50 and 60 s — not the one
    // sitting exactly on 30 s.
    let count = one_value(&db, "count_over_time(ticks[30s])", 60 * SEC);
    assert!(
        (count - 3.0).abs() < 1e-9,
        "a left-open [30s] over 10 s samples selects 3, got {count}"
    );
}

/// `group_left` keeps the many side's labels; `on()` prunes only for
/// one-to-one matching.
///
/// Pruning under `group_left` collapsed output series that differed only in a
/// label `on()` did not mention — two distinct results merging into one, which
/// is a wrong answer rather than a cosmetic one.
#[test]
fn group_left_keeps_the_many_sides_labels() {
    let db = db_with(&[
        Point::new(
            SeriesKey::new("errors", tags! { "job" => "api", "path" => "/a" }).unwrap(),
            fields! { "value" => 2.0 },
            10 * SEC,
        )
        .unwrap(),
        Point::new(
            SeriesKey::new("errors", tags! { "job" => "api", "path" => "/b" }).unwrap(),
            fields! { "value" => 4.0 },
            10 * SEC,
        )
        .unwrap(),
        Point::new(
            SeriesKey::new("budget", tags! { "job" => "api" }).unwrap(),
            fields! { "value" => 2.0 },
            10 * SEC,
        )
        .unwrap(),
    ]);

    let result = instant(&db, "errors / on(job) group_left budget", 10 * SEC);
    assert_eq!(
        result.len(),
        2,
        "group_left must keep both paths as distinct series, got {result:?}"
    );
    for (labels, _) in &result {
        assert!(
            labels.iter().any(|(k, _)| k == "path"),
            "the many side's `path` label must survive on(job), got {labels:?}"
        );
    }
}

/// One-to-one matching with an ambiguous match group is an error, not a
/// silently truncated result.
#[test]
fn ambiguous_one_to_one_matching_is_rejected() {
    let db = db_with(&[
        Point::new(
            SeriesKey::new("errors", tags! { "job" => "api", "path" => "/a" }).unwrap(),
            fields! { "value" => 2.0 },
            10 * SEC,
        )
        .unwrap(),
        Point::new(
            SeriesKey::new("errors", tags! { "job" => "api", "path" => "/b" }).unwrap(),
            fields! { "value" => 4.0 },
            10 * SEC,
        )
        .unwrap(),
        Point::new(
            SeriesKey::new("budget", tags! { "job" => "api" }).unwrap(),
            fields! { "value" => 2.0 },
            10 * SEC,
        )
        .unwrap(),
    ]);

    let expr = promql::parse("errors / on(job) budget").unwrap();
    let ev = PromQLEvaluator::new(db.clone());
    let params = promql::eval::QueryParams {
        time: 10 * SEC,
        ..Default::default()
    };
    let err = ev
        .instant_query(&expr, &params)
        .expect_err("ambiguous one-to-one matching must be an error");
    assert!(
        err.to_string()
            .contains("many-to-one matching must be explicit"),
        "expected Prometheus's diagnostic, got: {err}"
    );
}

/// `histogram_quantile` needs a `+Inf` bucket to know the observation total.
///
/// Without one the total is a partial count and every quantile derived from it
/// is a plausible, wrong number. Prometheus returns NaN; so must a `NaN`
/// quantile argument, which previously fell through every edge-case branch and
/// came back as a finite bucket bound.
#[test]
fn histogram_quantile_refuses_an_unbounded_histogram() {
    let mk = |le: &str, count: f64| {
        Point::new(
            SeriesKey::new("lat_bucket", tags! { "le" => le }).unwrap(),
            fields! { "value" => count },
            10 * SEC,
        )
        .unwrap()
    };
    let db = db_with(&[mk("0.1", 1.0), mk("0.5", 3.0), mk("1", 5.0)]);
    let v = one_value(&db, "histogram_quantile(0.9, lat_bucket)", 10 * SEC);
    assert!(v.is_nan(), "no +Inf bucket ⇒ NaN, got {v}");

    let db = db_with(&[mk("0.1", 1.0), mk("0.5", 3.0), mk("+Inf", 5.0)]);
    let v = one_value(&db, "histogram_quantile(0.9, lat_bucket)", 10 * SEC);
    assert!(
        v.is_finite(),
        "a conforming histogram still answers, got {v}"
    );
    let v = one_value(&db, "histogram_quantile(NaN, lat_bucket)", 10 * SEC);
    assert!(v.is_nan(), "a NaN quantile ⇒ NaN, got {v}");
}

/// A range query reads its window once, not once per step.
///
/// `range_fetch_start`/`range_fetch_end` widen every selector to the whole
/// query window. Without a cache behind them that is not an optimisation but
/// its opposite: it converts O(steps) *narrow* scans into O(steps)
/// *full-range* scans, so a Grafana panel with 200 steps over six hours read
/// six hours of data two hundred times. The comment above the field claimed
/// "O(steps) to O(1)" for several releases while the code had no cache at all
/// — so the claim is asserted here rather than described there.
#[test]
fn a_range_query_reads_its_window_once() {
    let key = SeriesKey::new("cpu", tags! { "host" => "a" }).unwrap();
    let points: Vec<Point> = (0..60)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "value" => f64::from(i) },
                i64::from(i) * 10 * SEC,
            )
            .unwrap()
        })
        .collect();
    let db = db_with(&points);

    // `[1m]` is inside the 5 min lookback; `[10m]` reaches further back than
    // it. The second is the case that breaks a widened window computed per
    // step — its lower bound moves with the step, so every step misses.
    for query in [
        "cpu",
        "rate(cpu[1m])",
        "rate(cpu[10m])",
        "avg_over_time(cpu[10m])",
    ] {
        let expr = promql::parse(query).unwrap();
        let ev = PromQLEvaluator::new(db.clone());
        let params = promql::eval::QueryParams {
            time: 0,
            start: Some(60 * SEC),
            end: Some(600 * SEC),
            step: Some(10 * SEC),
            ..Default::default()
        };
        ev.range_query(&expr, &params).unwrap();

        let stats = ev.scan_stats();
        assert_eq!(
            stats.scans, 1,
            "{query}: 55 steps over one selector must read storage once, not {} times",
            stats.scans
        );
        assert!(
            stats.cache_hits >= 50,
            "{query}: every later step must be served from the cache, got {} hits",
            stats.cache_hits
        );
    }
}

// ── Prometheus 3.x functions ────────────────────────────────────────────
//
// Each of these is checked against a value computed independently of the
// implementation — the upstream recurrence transcribed by hand, or a median
// worked out on paper — rather than against "did it return a finite number".
// A numeric result checked only for sign or finiteness is uncovered (that is
// how `rate()` shipped 25 % low for twenty-five passes).

/// `double_exponential_smoothing` must follow Prometheus's recurrence, which
/// is not the textbook Holt linear method: the previous smoothed value starts
/// at zero and the trend update is suppressed on the first iteration.
///
/// Expected value derived by hand from upstream's loop for
/// `sf = 0.5, tf = 0.5` over `[1, 2, 3, 4]`:
///
/// ```text
/// s0=0, s1=1, b=1
/// i=1: x=1.0,  b=1 (first iteration passes the seed through)
///      y=0.5*(1+1)=1.0     → s0=1,   s1=2.0
/// i=2: x=1.5,  b=0.5*(2-1)+0.5*1 = 1.0
///      y=0.5*(2+1)=1.5     → s0=2,   s1=3.0
/// i=3: x=2.0,  b=0.5*(3-2)+0.5*1 = 1.0
///      y=0.5*(3+1)=2.0     → s1=4.0
/// ```
#[test]
fn double_exponential_smoothing_matches_the_upstream_recurrence() {
    let key = SeriesKey::new("g", tags! { "job" => "a" }).unwrap();
    let points: Vec<Point> = [1.0, 2.0, 3.0, 4.0]
        .iter()
        .enumerate()
        .map(|(i, v)| {
            Point::new(
                key.clone(),
                fields! { "value" => *v },
                (i as i64 + 1) * 15 * SEC,
            )
            .unwrap()
        })
        .collect();
    let db = db_with(&points);

    let got = one_value(
        &db,
        "double_exponential_smoothing(g[1m], 0.5, 0.5)",
        60 * SEC,
    );
    assert!(
        (got - 4.0).abs() < 1e-9,
        "double_exponential_smoothing = {got}, expected 4.0"
    );
}

/// Both factors must be strictly inside `(0, 1)`; a bound is an error, not a
/// silently different answer.
#[test]
fn double_exponential_smoothing_rejects_out_of_range_factors() {
    let key = SeriesKey::new("g", tags! { "job" => "a" }).unwrap();
    let db = db_with(&[
        Point::new(key.clone(), fields! { "value" => 1.0 }, 15 * SEC).unwrap(),
        Point::new(key, fields! { "value" => 2.0 }, 30 * SEC).unwrap(),
    ]);

    for q in [
        "double_exponential_smoothing(g[1m], 0, 0.5)",
        "double_exponential_smoothing(g[1m], 1, 0.5)",
        "double_exponential_smoothing(g[1m], 0.5, 0)",
        "double_exponential_smoothing(g[1m], 0.5, 1)",
    ] {
        let expr = promql::parse(q).unwrap();
        let ev = PromQLEvaluator::new(db.clone());
        let params = promql::eval::QueryParams {
            time: 60 * SEC,
            ..Default::default()
        };
        assert!(
            ev.instant_query(&expr, &params).is_err(),
            "{q} must be rejected"
        );
    }
}

/// `mad_over_time` is the median of the absolute deviations from the median.
///
/// For `[1, 1, 2, 2, 4, 6, 9]` the median is 2 and the deviations are
/// `[1, 1, 0, 0, 2, 4, 7]`, whose median is 1.
#[test]
fn mad_over_time_computes_the_median_absolute_deviation() {
    let key = SeriesKey::new("g", tags! { "job" => "a" }).unwrap();
    let values = [1.0, 1.0, 2.0, 2.0, 4.0, 6.0, 9.0];
    let points: Vec<Point> = values
        .iter()
        .enumerate()
        .map(|(i, v)| {
            Point::new(
                key.clone(),
                fields! { "value" => *v },
                (i as i64 + 1) * 5 * SEC,
            )
            .unwrap()
        })
        .collect();
    let db = db_with(&points);

    let got = one_value(&db, "mad_over_time(g[1m])", 35 * SEC);
    assert!(
        (got - 1.0).abs() < 1e-9,
        "mad_over_time = {got}, expected 1"
    );
}

/// `sort_by_label` orders by *natural* comparison, so `pod-2` precedes
/// `pod-10`. Sorting the label values as opaque strings gives the reverse,
/// and looks entirely plausible.
#[test]
fn sort_by_label_uses_natural_ordering() {
    let points: Vec<Point> = ["pod-10", "pod-2", "pod-1"]
        .iter()
        .map(|pod| {
            let key = SeriesKey::new("g", tags! { "pod" => *pod }).unwrap();
            Point::new(key, fields! { "value" => 1.0 }, 10 * SEC).unwrap()
        })
        .collect();
    let db = db_with(&points);

    let pods: Vec<String> = instant(&db, r#"sort_by_label(g, "pod")"#, 10 * SEC)
        .into_iter()
        .map(|(labels, _)| {
            labels
                .iter()
                .find(|(k, _)| k == "pod")
                .map(|(_, v)| v.clone())
                .unwrap_or_default()
        })
        .collect();
    assert_eq!(pods, vec!["pod-1", "pod-2", "pod-10"]);

    let desc: Vec<String> = instant(&db, r#"sort_by_label_desc(g, "pod")"#, 10 * SEC)
        .into_iter()
        .map(|(labels, _)| {
            labels
                .iter()
                .find(|(k, _)| k == "pod")
                .map(|(_, v)| v.clone())
                .unwrap_or_default()
        })
        .collect();
    assert_eq!(desc, vec!["pod-10", "pod-2", "pod-1"]);
}

// ── Date functions ──────────────────────────────────────────────────────

/// `2024-02-29T13:47:11Z` — a leap day, so `days_in_month` and `day_of_year`
/// both have something to get wrong, at a time of day that distinguishes UTC
/// from every common local zone.
const LEAP_DAY: i64 = 1_709_214_431 * SEC;

fn date_db() -> Arc<Chronix> {
    // One sample whose *value* is a Unix timestamp: the date functions read
    // the value, not the sample's own time, which is the part that surprises
    // people.
    let key = SeriesKey::new("epoch", tags! { "job" => "clock" }).unwrap();
    db_with(&[Point::new(key, fields! { "value" => 1_709_214_431.0_f64 }, LEAP_DAY).unwrap()])
}

#[test]
fn date_functions_default_to_the_evaluation_time() {
    let db = date_db();
    for (query, expected) in [
        ("year()", 2024.0),
        ("month()", 2.0),
        ("day_of_month()", 29.0),
        // 2024-02-29 was a Thursday; Prometheus counts Sunday as 0.
        ("day_of_week()", 4.0),
        ("day_of_year()", 60.0),
        ("days_in_month()", 29.0),
        ("hour()", 13.0),
        ("minute()", 47.0),
    ] {
        assert_eq!(one_value(&db, query, LEAP_DAY), expected, "{query}");
    }
}

#[test]
fn date_functions_read_the_sample_value_not_its_timestamp() {
    let db = date_db();
    // Evaluate a day later: the sample is still in the lookback window and
    // still carries the leap-day epoch as its value, so the answer must not
    // move with the evaluation time.
    let later = LEAP_DAY + 240 * SEC;
    assert_eq!(one_value(&db, "day_of_month(epoch)", later), 29.0);
    assert_eq!(one_value(&db, "hour(epoch)", later), 13.0);
    // The bare form does follow the evaluation time.
    assert_eq!(one_value(&db, "minute()", later), 51.0);
}

#[test]
fn date_functions_drop_the_metric_name() {
    let db = date_db();
    let result = instant(&db, "year(epoch)", LEAP_DAY);
    assert_eq!(result.len(), 1);
    assert!(
        result[0].0.iter().all(|(k, _)| k != "__name__"),
        "year() kept __name__: {:?}",
        result[0].0
    );
    assert!(
        result[0].0.iter().any(|(k, v)| k == "job" && v == "clock"),
        "year() dropped the other labels: {:?}",
        result[0].0
    );
}

#[test]
fn days_in_month_handles_the_century_leap_rule() {
    let db = date_db();
    for (epoch, expected) in [
        // 1900-02-15 — divisible by 100 but not 400, so not a leap year.
        (-2_204_496_000_i64, 28.0),
        // 2000-02-15 — divisible by 400, so a leap year.
        (950_572_800, 29.0),
        // 2023-02-15 — an ordinary year.
        (1_676_419_200, 28.0),
        // 2023-01-15 and 2023-04-15, for the 31/30 split.
        (1_673_740_800, 31.0),
        (1_681_516_800, 30.0),
    ] {
        let q = format!("days_in_month(vector({epoch}))");
        assert_eq!(one_value(&db, &q, LEAP_DAY), expected, "{q}");
    }
}

#[test]
fn date_functions_reject_extra_arguments() {
    let db = date_db();
    let expr = promql::parse("hour(epoch, epoch)").unwrap();
    let ev = PromQLEvaluator::new(db.clone());
    let params = promql::eval::QueryParams {
        time: LEAP_DAY,
        ..Default::default()
    };
    assert!(ev.instant_query(&expr, &params).is_err());
}

// ── Range-query step arithmetic ─────────────────────────────────────────
//
// The step loop was `t += step` on an `i64`. A range whose `end` sits near
// `i64::MAX` overflows it: in a debug build that panics, and in a **release**
// build it wraps to a large negative number, `t <= end` stays true, and the
// loop never terminates. The evaluation runs inside `spawn_blocking`, so the
// caller's timeout returns to the client while the blocking thread spins for
// ever — a handful of such queries retires every blocking thread the server
// has.
//
// The server's point-count guard does not catch it. A *large* step keeps the
// point count small: `end = i64::MAX` with a step of `i64::MAX / 2` is three
// evaluation points, which every limit admits.

/// A range whose last step would overflow must terminate.
#[test]
fn a_range_query_near_the_end_of_time_terminates() {
    let key = SeriesKey::new("up", tags! { "job" => "j" }).unwrap();
    let db = db_with(&[Point::new(key, fields! { "value" => 1.0 }, 0).unwrap()]);
    let expr = promql::parse("up").unwrap();
    let evaluator = PromQLEvaluator::new(db);

    let params = promql::eval::QueryParams {
        time: i64::MAX,
        start: Some(0),
        end: Some(i64::MAX),
        step: Some(i64::MAX / 2),
        ..Default::default()
    };

    // The property is termination. A wrong answer here would be a different
    // bug; an answer at all is the one being asserted.
    let result = evaluator.range_query(&expr, &params);
    assert!(
        result.is_ok(),
        "a range query at the end of the i64 domain must answer, got {result:?}"
    );
}

/// A step count beyond the budget is refused, rather than evaluated.
#[test]
fn an_unbounded_step_count_is_refused() {
    let key = SeriesKey::new("up", tags! { "job" => "j" }).unwrap();
    let db = db_with(&[Point::new(key, fields! { "value" => 1.0 }, 0).unwrap()]);
    let expr = promql::parse("up").unwrap();
    let evaluator = PromQLEvaluator::new(db);

    let params = promql::eval::QueryParams {
        time: 0,
        start: Some(0),
        end: Some(1_000_000_000_000_000),
        step: Some(1), // one nanosecond: 10^15 steps
        ..Default::default()
    };

    let err = evaluator
        .range_query(&expr, &params)
        .expect_err("10^15 evaluation points must be refused, not attempted");
    assert!(
        err.to_string().contains("evaluation points"),
        "the error must name the limit, got: {err}"
    );
}

// ══ Deviations from Prometheus, each derived from the upstream source ══
//
// Every test below states the upstream rule it encodes and where it comes
// from in `prometheus/promql`. A backlog entry is not evidence: one of these
// claims turned out to describe behaviour this evaluator already had right.

/// `scalar < vector` keeps the **vector element's** value, not the scalar's.
///
/// `engine.go`, `VectorscalarBinop`: "Catch cases where the scalar is the LHS
/// in a scalar-vector comparison operation. We want to always keep the vector
/// element value as the output value, even if it's on the RHS."
#[test]
fn a_scalar_on_the_left_of_a_comparison_keeps_the_vectors_value() {
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    let db = db_with(&[Point::new(key, fields! { "value" => 42.0 }, 10 * SEC).unwrap()]);

    // The vector is on the right: 2 < 42 is true, and the result is 42.
    assert_eq!(one_value(&db, "2 < m", 10 * SEC), 42.0);
    // And on the left, for the same reason.
    assert_eq!(one_value(&db, "m > 2", 10 * SEC), 42.0);
    // A false comparison drops the sample from either side.
    assert!(instant(&db, "100 < m", 10 * SEC).is_empty());
    assert!(instant(&db, "m > 100", 10 * SEC).is_empty());

    // With `bool`, the value is 1 or 0 whichever side the scalar is on.
    assert_eq!(one_value(&db, "2 < bool m", 10 * SEC), 1.0);
    assert_eq!(one_value(&db, "100 < bool m", 10 * SEC), 0.0);
    assert_eq!(one_value(&db, "m > bool 2", 10 * SEC), 1.0);
}

/// Arithmetic with a scalar on the left applies in the written order.
#[test]
fn a_scalar_on_the_left_of_arithmetic_is_not_commuted() {
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    let db = db_with(&[Point::new(key, fields! { "value" => 4.0 }, 10 * SEC).unwrap()]);
    assert_eq!(one_value(&db, "10 - m", 10 * SEC), 6.0);
    assert_eq!(one_value(&db, "m - 10", 10 * SEC), -6.0);
    assert_eq!(one_value(&db, "10 / m", 10 * SEC), 2.5);
}

/// A duplicate on the "one" side of a `group_left` is an error.
///
/// `engine.go`, `VectorBinop`: "found duplicate series for the match group
/// … on the right hand-side of the operation". Silently doing many-to-many
/// instead produces a cross product, which is a wrong answer rather than a
/// missing feature.
#[test]
fn a_duplicate_on_the_one_side_of_group_left_is_rejected() {
    let many = |host: &str| {
        SeriesKey::new("http_requests", tags! { "host" => host, "code" => "200" }).unwrap()
    };
    // Two `info` series that collapse to the same label set on `host` — the
    // duplicate the "one" side must not have.
    let one_a = SeriesKey::new("info", tags! { "host" => "a", "dc" => "east" }).unwrap();
    let one_b = SeriesKey::new("info", tags! { "host" => "a", "dc" => "west" }).unwrap();

    let db = db_with(&[
        Point::new(many("a"), fields! { "value" => 1.0 }, 10 * SEC).unwrap(),
        Point::new(one_a, fields! { "value" => 1.0 }, 10 * SEC).unwrap(),
        Point::new(one_b, fields! { "value" => 1.0 }, 10 * SEC).unwrap(),
    ]);

    let expr = promql::parse("http_requests * on(host) group_left(dc) info").unwrap();
    let ev = PromQLEvaluator::new(db);
    let params = promql::eval::QueryParams {
        time: 10 * SEC,
        ..Default::default()
    };
    let err = ev
        .instant_query(&expr, &params)
        .expect_err("a duplicate on the `one` side must be an error, not a cross product");
    assert!(
        err.to_string().contains("duplicate"),
        "the error must name the problem, got: {err}"
    );
}

/// `deriv` and `idelta` drop a series they cannot compute, rather than
/// emitting NaN.
///
/// `functions.go`: `funcDeriv` returns `enh.Out` unchanged when
/// `len(samples.Floats) < 2`, and `funcIdelta` likewise — an absent sample,
/// not a NaN one. A NaN reaches a dashboard as a gap *with* a series in the
/// legend, which reads as "broken" rather than "no data".
#[test]
fn deriv_and_idelta_drop_a_series_they_cannot_compute() {
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    // One sample in the range: neither function has two points to work with.
    let db = db_with(&[Point::new(key, fields! { "value" => 1.0 }, 100 * SEC).unwrap()]);

    for q in ["deriv(m[30s])", "idelta(m[30s])"] {
        let r = instant(&db, q, 100 * SEC);
        assert!(r.is_empty(), "{q} must drop the series, got {r:?}");
    }
}

/// `last_over_time` keeps `__name__`; every other `_over_time` drops it.
///
/// `functions.go`: the `_over_time` family calls `dropSeriesName` through
/// `aggrOverTime`, but `funcLastOverTime` is registered without it — it is
/// the one that returns an actual sample of the original series.
#[test]
fn last_over_time_keeps_the_metric_name_and_the_others_drop_it() {
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    let db = db_with(&[
        Point::new(key.clone(), fields! { "value" => 1.0 }, 10 * SEC).unwrap(),
        Point::new(key, fields! { "value" => 2.0 }, 20 * SEC).unwrap(),
    ]);

    let has_name = |q: &str| {
        instant(&db, q, 30 * SEC)
            .first()
            .is_some_and(|(labels, _)| labels.iter().any(|(k, _)| k == "__name__"))
    };

    assert!(
        has_name("last_over_time(m[30s])"),
        "last_over_time keeps it"
    );
    for q in [
        "avg_over_time(m[30s])",
        "max_over_time(m[30s])",
        "sum_over_time(m[30s])",
        "count_over_time(m[30s])",
    ] {
        assert!(!has_name(q), "{q} must drop __name__");
    }
}

/// `timestamp()` drops `__name__`.
///
/// `functions.go`, `funcTimestamp`: it builds `enh.DropMetricName(el.Metric)`.
/// The value is a timestamp, not a measurement of the original metric, so
/// keeping the name would let `timestamp(m)` and `m` collide in a binary op.
#[test]
fn timestamp_drops_the_metric_name() {
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    let db = db_with(&[Point::new(key, fields! { "value" => 5.0 }, 10 * SEC).unwrap()]);
    let r = instant(&db, "timestamp(m)", 10 * SEC);
    assert_eq!(r.len(), 1);
    assert!(
        !r[0].0.iter().any(|(k, _)| k == "__name__"),
        "timestamp() must drop __name__, got {:?}",
        r[0].0
    );
    assert_eq!(r[0].1, 10.0, "the value is the sample's time in seconds");
}

/// `offset` after anything but a selector is a parse error.
///
/// Prometheus's grammar attaches `offset` to a `VectorSelector`,
/// `MatrixSelector` or `SubqueryExpr` only; `sum(m) offset 5m` fails with
/// "offset modifier must be preceded by an instant vector selector or range
/// vector selector or a subquery". Parsing and discarding it is worse than
/// refusing: the query looks like it shifted and did not.
#[test]
fn an_offset_after_a_non_selector_is_a_parse_error() {
    for q in [
        "sum(m) offset 5m",
        "(m + 1) offset 5m",
        "rate(m[5m]) offset 5m",
    ] {
        assert!(
            promql::parse(q).is_err(),
            "{q} must be refused: offset only follows a selector or a subquery"
        );
    }
    // Where it is allowed, it still parses.
    for q in ["m offset 5m", "rate(m[5m] offset 5m)", "m[5m:1m] offset 5m"] {
        assert!(promql::parse(q).is_ok(), "{q} must parse");
    }
}

/// A negative offset shifts forward in time.
///
/// Prometheus 2.26+ with `--enable-feature=promql-negative-offset`, and
/// unconditional in 3.x.
#[test]
fn a_negative_offset_shifts_forward() {
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    let db = db_with(&[
        Point::new(key.clone(), fields! { "value" => 1.0 }, 10 * SEC).unwrap(),
        Point::new(key, fields! { "value" => 2.0 }, 20 * SEC).unwrap(),
    ]);
    // At t=10s, `offset -10s` looks at t=20s.
    assert_eq!(one_value(&db, "m offset -10s", 10 * SEC), 2.0);
    assert_eq!(one_value(&db, "m offset 10s", 20 * SEC), 1.0);
}

/// `@` pins an evaluation to an absolute timestamp.
///
/// Prometheus 2.25+; `@ start()` and `@ end()` resolve to the range query's
/// bounds. The point of it is that a query can compare a live value against a
/// fixed one.
#[test]
fn the_at_modifier_pins_an_evaluation_time() {
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    let db = db_with(&[
        Point::new(key.clone(), fields! { "value" => 1.0 }, 10 * SEC).unwrap(),
        Point::new(key, fields! { "value" => 2.0 }, 20 * SEC).unwrap(),
    ]);
    // Whatever the evaluation time, `@ 10` reads the sample at t=10s.
    assert_eq!(one_value(&db, "m @ 10", 20 * SEC), 1.0);
    assert_eq!(one_value(&db, "m @ 20", 10 * SEC), 2.0);
}

/// A subquery with no step uses the 1-minute default, not the outer step.
///
/// `parser`: `SubqueryExpr.Step == 0` means "use the default evaluation
/// interval", which the engine fills from its own configuration (1m in every
/// standard deployment). Resolving it against the *outer* step makes the same
/// query mean different things at different resolutions.
#[test]
fn a_step_less_subquery_uses_the_default_step() {
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    // One sample per 30s over 10 minutes.
    let points: Vec<Point> = (0..20)
        .map(|i| Point::new(key.clone(), fields! { "value" => 1.0 }, i * 30 * SEC).unwrap())
        .collect();
    let db = db_with(&points);

    // `m[5m:]` over a 5-minute window at a 1-minute default step is 5 or 6
    // points, never the ~300 an outer step of 1s would give.
    let n = one_value(&db, "count_over_time(m[5m:])", 600 * SEC);
    assert!(
        (5.0..=6.0).contains(&n),
        "a step-less subquery must use the 1m default; got {n} points"
    );
}

// ══ Subqueries ══════════════════════════════════════════════════════════
//
// A subquery `expr[range:step]` evaluates `expr` as an instant query at each
// point of an absolute step grid and hands the results up as a range vector.
// Four properties of that follow from `engine.go`'s `evalSubquery` and the
// `*parser.SubqueryExpr` case of `eval`, and each was wrong here.

/// Every sample of a subquery carries its **step** timestamp.
///
/// The inner expression is evaluated *at* each grid point, so that is the
/// instant its result belongs to. Chronix pushed the inner sample through
/// unchanged, which carries the raw scrape time — so two adjacent steps that
/// saw the same newest sample emitted the same timestamp twice. A range vector
/// with duplicate timestamps is not one, and `irate` over it sees `dt == 0`.
#[test]
fn subquery_samples_carry_step_timestamps() {
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    // Samples every 60s, so a 10s subquery step re-reads the same sample six
    // times in a row — the case that produced duplicates.
    let points: Vec<Point> = (0..6)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "value" => (i * 10) as f64 },
                i * 60 * SEC,
            )
            .unwrap()
        })
        .collect();
    let db = db_with(&points);

    let expr = promql::parse("m[100s:10s]").unwrap();
    let ev = PromQLEvaluator::new(db);
    let params = promql::eval::QueryParams {
        time: 300 * SEC,
        ..Default::default()
    };
    let promql::PromQLValue::Matrix(series) = ev.instant_query(&expr, &params).unwrap() else {
        panic!("a subquery is a range vector");
    };
    assert_eq!(series.len(), 1);
    let ts: Vec<i64> = series[0].samples.iter().map(|s| s.timestamp).collect();

    let mut sorted = ts.clone();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        ts.len(),
        "a range vector must not have duplicate timestamps, got {ts:?}"
    );
    assert!(
        ts.windows(2).all(|w| w[1] - w[0] == 10 * SEC),
        "samples must land on the 10s step grid, got {ts:?}"
    );
}

/// `irate` over a subquery still works, which it cannot with duplicate
/// timestamps: the last two samples would be `dt == 0` apart.
#[test]
fn irate_over_a_subquery_answers() {
    let key = SeriesKey::new("c", tags! { "host" => "a" }).unwrap();
    // A counter climbing by 1 per second, scraped every 15s.
    let points: Vec<Point> = (0..40)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "value" => (i * 15) as f64 },
                i * 15 * SEC,
            )
            .unwrap()
        })
        .collect();
    let db = db_with(&points);

    let v = one_value(&db, "irate(c[5m:30s])", 500 * SEC);
    assert!(
        (v - 1.0).abs() < 1e-6,
        "irate over a subquery of a 1/s counter must be 1.0, got {v}"
    );
}

/// `rate` over a subquery extrapolates, exactly as over a range selector.
///
/// The range comes from the subquery's own `[range:...]`. Chronix's
/// `extract_range_ns` matched only `MatrixSelector`, so a subquery got no
/// range and therefore no extrapolation — the same query over `[5m]` and
/// `[5m:15s]` gave different answers for the same data.
#[test]
fn rate_over_a_subquery_extrapolates_like_a_range_selector() {
    let key = SeriesKey::new("c", tags! { "host" => "a" }).unwrap();
    let points: Vec<Point> = (0..60)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "value" => (i * 15) as f64 },
                i * 15 * SEC,
            )
            .unwrap()
        })
        .collect();
    let db = db_with(&points);

    let direct = one_value(&db, "rate(c[5m])", 600 * SEC);
    let sub = one_value(&db, "rate(c[5m:15s])", 600 * SEC);
    assert!(
        (direct - 1.0).abs() < 1e-6,
        "the range-selector form must be 1.0/s, got {direct}"
    );
    assert!(
        (sub - direct).abs() < 0.05,
        "the subquery form must agree with the range-selector form: {sub} vs {direct}"
    );
}

/// The subquery window is **left-open** on the step grid.
///
/// `engine.go`: the start is advanced by one interval when it lands exactly on
/// `start - offset - range`, so a point on the older boundary belongs to the
/// previous window. Left-closed instead makes an evenly-sampled series produce
/// n+1 points in some windows and n in others.
#[test]
fn a_subquery_window_excludes_its_left_boundary() {
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    let points: Vec<Point> = (0..40)
        .map(|i| Point::new(key.clone(), fields! { "value" => 1.0 }, i * 10 * SEC).unwrap())
        .collect();
    let db = db_with(&points);

    // A 100s window on a 10s grid ending at 300s covers (200s, 300s] — ten
    // points, not eleven.
    let n = one_value(&db, "count_over_time(m[100s:10s])", 300 * SEC);
    assert_eq!(
        n, 10.0,
        "a 100s window at a 10s step is 10 points, not 11 — the left edge is open"
    );
}

/// In a **range** query, a step-less subquery still uses the 1-minute default
/// rather than the outer step.
///
/// `noStepSubqueryIntervalFn` is the engine's own evaluation interval, which
/// has nothing to do with the resolution the caller asked for. Resolving it
/// against the outer step makes the same dashboard panel mean different things
/// at different zoom levels — and at a 1-second step it is 300 inner
/// evaluations per outer step instead of 5.
#[test]
fn a_step_less_subquery_in_a_range_query_uses_the_default_step() {
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    let points: Vec<Point> = (0..40)
        .map(|i| Point::new(key.clone(), fields! { "value" => 1.0 }, i * 30 * SEC).unwrap())
        .collect();
    let db = db_with(&points);

    // A range query at a 30s step. If the subquery inherited it, a 5m window
    // would hold ~10 points; at the 1m default it holds 5.
    let series = range(
        &db,
        "count_over_time(m[5m:])",
        900 * SEC,
        1_000 * SEC,
        30 * SEC,
    );
    assert_eq!(series.len(), 1);
    for (_, v) in &series[0] {
        assert!(
            (5.0..=6.0).contains(v),
            "the subquery must use the 1m default step, not the outer 30s one; got {v}"
        );
    }
}

/// A range query containing a subquery reads storage once, not once per step.
///
/// The subquery derived its inner selectors' prefetch window from its **own**
/// per-step window, which moves with the outer step — so every outer step
/// missed the scan cache, and the eight-entry cap meant the misses could not
/// even accumulate into hits. `max_over_time(rate(x[5m])[1h:1m])` over 200
/// outer steps did 200 widening scans of the same data, each of them the whole
/// hour. The window the *outer* query already prefetches covers every inner
/// step, and is the same at all of them.
#[test]
fn a_range_query_with_a_subquery_reads_its_window_once() {
    let key = SeriesKey::new("cpu", tags! { "host" => "a" }).unwrap();
    let points: Vec<Point> = (0..400)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "value" => f64::from(i) },
                i64::from(i) * 15 * SEC,
            )
            .unwrap()
        })
        .collect();
    let db = db_with(&points);

    for query in [
        "max_over_time(cpu[10m:1m])",
        "max_over_time(rate(cpu[5m])[10m:1m])",
    ] {
        let expr = promql::parse(query).unwrap();
        let ev = PromQLEvaluator::new(db.clone());
        let params = promql::eval::QueryParams {
            time: 0,
            start: Some(3_000 * SEC),
            end: Some(4_200 * SEC),
            step: Some(60 * SEC),
            ..Default::default()
        };
        ev.range_query(&expr, &params).unwrap();

        let stats = ev.scan_stats();
        assert_eq!(
            stats.scans, 1,
            "{query}: the whole range query must read storage once, not {} times",
            stats.scans
        );
    }
}

/// `sort` and `sort_desc` both put NaN **last**.
///
/// `functions.go` comments it twice — "NaN should sort to the bottom" — and
/// implements it by sorting NaN-first and reversing, in both directions. The
/// backlog claimed Prometheus sorts NaN first; it does not, and this
/// evaluator already agreed with the real behaviour. Pinned so the claim
/// cannot come back.
#[test]
fn both_sorts_put_nan_last() {
    // The write path refuses a non-finite field, so the NaN is produced the
    // way a query produces one: `0 / 0` for host b.
    let mk = |m: &str, host: &str, v: f64| {
        Point::new(
            SeriesKey::new(m, tags! { "host" => host }).unwrap(),
            fields! { "value" => v },
            10 * SEC,
        )
        .unwrap()
    };
    let db = db_with(&[
        mk("load", "a", 2.0),
        mk("load", "b", 0.0),
        mk("load", "c", 1.0),
        mk("d", "a", 1.0),
        mk("d", "b", 0.0),
        mk("d", "c", 1.0),
    ]);
    let m = "load / on(host) d";

    for query in [format!("sort({m})"), format!("sort_desc({m})")] {
        let query = query.as_str();
        let r = instant(&db, query, 10 * SEC);
        assert_eq!(r.len(), 3, "{query}");
        assert!(
            r.last().unwrap().1.is_nan(),
            "{query}: NaN must sort last, got {:?}",
            r.iter().map(|(_, v)| *v).collect::<Vec<_>>()
        );
    }
    // …and the finite values are ordered in each direction.
    let asc: Vec<f64> = instant(&db, &format!("sort({m})"), 10 * SEC)
        .iter()
        .map(|(_, v)| *v)
        .take(2)
        .collect();
    assert_eq!(asc, vec![1.0, 2.0]);
    let desc: Vec<f64> = instant(&db, &format!("sort_desc({m})"), 10 * SEC)
        .iter()
        .map(|(_, v)| *v)
        .take(2)
        .collect();
    assert_eq!(desc, vec![2.0, 1.0]);
}

/// IEEE-754 arithmetic reaches the result, unchanged.
///
/// Prometheus does no special-casing here: `1/0` is `+Inf`, `0/0` is NaN, and
/// `%` follows Go's `math.Mod`. A database that "helpfully" errors or zeroes
/// these disagrees with every recording rule written against Prometheus.
#[test]
fn division_and_modulo_follow_ieee_754() {
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    let db = db_with(&[Point::new(key, fields! { "value" => 1.0 }, 10 * SEC).unwrap()]);

    assert_eq!(one_value(&db, "m / 0", 10 * SEC), f64::INFINITY);
    assert_eq!(one_value(&db, "-m / 0", 10 * SEC), f64::NEG_INFINITY);
    assert!(one_value(&db, "(m - m) / 0", 10 * SEC).is_nan());
    assert!(one_value(&db, "m % 0", 10 * SEC).is_nan());
    assert_eq!(one_value(&db, "m ^ 0", 10 * SEC), 1.0);
}

/// `clamp` family: NaN input stays NaN, and `clamp` with min > max is empty.
///
/// `functions.go`, `funcClamp`: "if min > max, return an empty vector".
#[test]
fn clamp_follows_the_upstream_edge_cases() {
    let mk = |m: &str, host: &str, v: f64| {
        Point::new(
            SeriesKey::new(m, tags! { "host" => host }).unwrap(),
            fields! { "value" => v },
            10 * SEC,
        )
        .unwrap()
    };
    // host a is 5, host b is 0/0 = NaN.
    let db = db_with(&[
        mk("load", "a", 5.0),
        mk("load", "b", 0.0),
        mk("d", "a", 1.0),
        mk("d", "b", 0.0),
    ]);
    let m = "load / on(host) d";

    let clamped = instant(&db, &format!("clamp({m}, 0, 3)"), 10 * SEC);
    assert_eq!(clamped.len(), 2);
    let finite: Vec<f64> = clamped
        .iter()
        .map(|(_, v)| *v)
        .filter(|v| !v.is_nan())
        .collect();
    assert_eq!(finite, vec![3.0], "5 clamps to the maximum");
    assert!(
        clamped.iter().any(|(_, v)| v.is_nan()),
        "a NaN sample stays NaN rather than clamping to a bound"
    );

    assert!(
        instant(&db, &format!("clamp({m}, 10, 3)"), 10 * SEC).is_empty(),
        "min > max is an empty vector"
    );
}

/// `round` rounds ties **up**, and a zero step yields NaN.
///
/// `functions.go`, `funcRound`, computes `Floor(f/toNearest + 0.5) *
/// toNearest`, so a `toNearest` of zero is `Floor(Inf)/Inf` — NaN. That is a
/// quirk of the arithmetic rather than a decision, and it is pinned here
/// because "fall back to 1" is the plausible-looking answer that would
/// disagree with every Prometheus deployment.
#[test]
fn round_rounds_ties_up_and_a_zero_step_is_nan() {
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    let db = db_with(&[Point::new(key, fields! { "value" => 2.5 }, 10 * SEC).unwrap()]);
    // Ties go toward +Inf, not away from zero: 2.5 rounds to 3, not to 2.
    assert_eq!(one_value(&db, "round(m)", 10 * SEC), 3.0);
    assert_eq!(one_value(&db, "round(m, 0.5)", 10 * SEC), 2.5);
    assert!(one_value(&db, "round(m, 0)", 10 * SEC).is_nan());
}

/// `quantile` outside `[0, 1]` yields ±Inf, not an error.
///
/// `aggregations.go`: `q < 0` gives `-Inf`, `q > 1` gives `+Inf`, both with a
/// warning annotation. Erroring instead breaks a dashboard whose variable
/// briefly holds a bad value.
#[test]
fn an_out_of_range_quantile_is_infinite_not_an_error() {
    let s = |host: &str| SeriesKey::new("m", tags! { "host" => host }).unwrap();
    let db = db_with(&[
        Point::new(s("a"), fields! { "value" => 1.0 }, 10 * SEC).unwrap(),
        Point::new(s("b"), fields! { "value" => 2.0 }, 10 * SEC).unwrap(),
    ]);
    assert_eq!(one_value(&db, "quantile(1.5, m)", 10 * SEC), f64::INFINITY);
    assert_eq!(
        one_value(&db, "quantile(-0.5, m)", 10 * SEC),
        f64::NEG_INFINITY
    );
    assert_eq!(one_value(&db, "quantile(0.5, m)", 10 * SEC), 1.5);
}

/// `label_replace` with an invalid destination label is an error; a
/// non-matching regex leaves the series untouched.
#[test]
fn label_replace_validates_its_destination() {
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    let db = db_with(&[Point::new(key, fields! { "value" => 1.0 }, 10 * SEC).unwrap()]);

    let expr = promql::parse(r#"label_replace(m, "1bad", "$1", "host", "(.*)")"#).unwrap();
    let ev = PromQLEvaluator::new(db.clone());
    let params = promql::eval::QueryParams {
        time: 10 * SEC,
        ..Default::default()
    };
    assert!(
        ev.instant_query(&expr, &params).is_err(),
        "an invalid label name must be refused"
    );

    // A regex that does not match leaves the series as it was.
    let r = instant(
        &db,
        r#"label_replace(m, "dc", "$1", "host", "zzz(.*)")"#,
        10 * SEC,
    );
    assert_eq!(r.len(), 1);
    assert!(
        !r[0].0.iter().any(|(k, _)| k == "dc"),
        "a non-matching regex must not add the destination label"
    );
}

/// `@ start()` and `@ end()` resolve to the **range query's** bounds at every
/// step, not to the step's own time.
///
/// That is the entire point of them: `rate(x[5m]) / rate(x[5m] @ start())`
/// compares each step against one fixed baseline, and a `start()` that
/// resolved per step would compare every step against itself and return 1.
#[test]
fn at_start_and_end_resolve_to_the_range_querys_bounds() {
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    // A ramp: value == t in seconds.
    let points: Vec<Point> = (0..30)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "value" => (i * 10) as f64 },
                i * 10 * SEC,
            )
            .unwrap()
        })
        .collect();
    let db = db_with(&points);

    // `m @ start()` must be the same value at every step — the sample at the
    // query's start — while `m` itself climbs.
    let pinned = range(&db, "m @ start()", 100 * SEC, 200 * SEC, 20 * SEC);
    assert_eq!(pinned.len(), 1);
    let values: Vec<f64> = pinned[0].iter().map(|(_, v)| *v).collect();
    assert!(
        values
            .windows(2)
            .all(|w| (w[0] - w[1]).abs() < f64::EPSILON),
        "`@ start()` must be constant across steps, got {values:?}"
    );
    assert!(
        (values[0] - 100.0).abs() < f64::EPSILON,
        "`@ start()` must read the sample at the query start (100), got {}",
        values[0]
    );

    // `@ end()` likewise, at the query's end.
    let pinned = range(&db, "m @ end()", 100 * SEC, 200 * SEC, 20 * SEC);
    let values: Vec<f64> = pinned[0].iter().map(|(_, v)| *v).collect();
    assert!(
        values.iter().all(|v| (v - 200.0).abs() < f64::EPSILON),
        "`@ end()` must read the sample at the query end (200), got {values:?}"
    );

    // Without the modifier the same selector climbs, which is what makes the
    // assertion above mean something.
    let moving = range(&db, "m", 100 * SEC, 200 * SEC, 20 * SEC);
    let values: Vec<f64> = moving[0].iter().map(|(_, v)| *v).collect();
    assert_eq!(values, vec![100.0, 120.0, 140.0, 160.0, 180.0, 200.0]);
}
