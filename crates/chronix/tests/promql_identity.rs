#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap
//! What a PromQL series is *called*, and whether that name can be typed back
//! in.
//!
//! A metric is one `(measurement, field)` pair. Before this was decided
//! anywhere, the evaluator named a series after its measurement when the
//! measurement held one field and after `measurement_field` when it held more,
//! and a `__name__` matcher only ever resolved a *measurement*. Three
//! properties were broken at once, and each of them is a test here:
//!
//! - a name a query returns is a selector that returns the same series;
//! - a name is a function of its own pair, so writing a second field does not
//!   rename the first one's history;
//! - a vector holds no duplicate label sets, which PromQL cannot represent.

use std::collections::HashSet;
use std::sync::Arc;

use chronix::prelude::*;
use chronix::promql::{self, PromQLEvaluator};
use chronix::{fields, tags, Chronix};

const SEC: i64 = 1_000_000_000;
const T: i64 = 1_700_000_000 * SEC;

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

fn instant(db: &Arc<Chronix>, query: &str) -> Vec<(Vec<(String, String)>, f64)> {
    let expr = promql::parse(query).unwrap();
    let ev = PromQLEvaluator::new(db.clone());
    let params = promql::eval::QueryParams {
        time: T,
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

fn name_of(labels: &[(String, String)]) -> String {
    labels
        .iter()
        .find(|(k, _)| k == "__name__")
        .map(|(_, v)| v.clone())
        .expect("a selector's result carries __name__")
}

/// `cpu{host}` with two fields, plus a Prometheus-shaped `up{job}` whose
/// value lives in a field called `value`.
fn two_field_db() -> Arc<Chronix> {
    let mut points = Vec::new();
    for i in 0..5i64 {
        points.push(
            Point::new(
                SeriesKey::new("cpu", tags! { "host" => "a" }).unwrap(),
                fields! { "usage" => i as f64, "load" => i as f64 * 0.5 },
                T - (5 - i) * SEC,
            )
            .unwrap(),
        );
        points.push(
            Point::new(
                SeriesKey::new("up", tags! { "job" => "api" }).unwrap(),
                fields! { "value" => 1.0 },
                T - (5 - i) * SEC,
            )
            .unwrap(),
        );
    }
    db_with(&points)
}

#[test]
fn a_name_a_query_returns_is_a_selector_that_returns_it() {
    let db = two_field_db();

    // Every metric the database knows.
    let metrics = promql::all_metrics(db.schema_registry());
    let names: Vec<&str> = metrics.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, ["cpu_load", "cpu_usage", "up"], "{metrics:?}");

    for metric in &metrics {
        let result = instant(&db, &metric.name);
        assert!(
            !result.is_empty(),
            "{} is a metric name the database reports but no selector answers",
            metric.name
        );
        for (labels, _) in &result {
            assert_eq!(
                name_of(labels),
                metric.name,
                "a selector returned a series under a different name"
            );
        }
    }
}

#[test]
fn a_measurement_is_not_a_metric_unless_it_holds_a_value_field() {
    let db = two_field_db();
    // `cpu` names no metric: its fields are `usage` and `load`.
    assert!(instant(&db, "cpu").is_empty());
    // `up` does, because its field is `value` — which is how the Prometheus
    // remote-write and OTLP paths store a sample, so the round trip holds.
    assert_eq!(instant(&db, "up").len(), 1);
}

#[test]
fn writing_a_second_field_does_not_rename_the_first() {
    let mut points = Vec::new();
    for i in 0..5i64 {
        points.push(
            Point::new(
                SeriesKey::new("mem", tags! { "host" => "a" }).unwrap(),
                fields! { "used" => i as f64 },
                T - (10 - i) * SEC,
            )
            .unwrap(),
        );
    }
    let db = db_with(&points);
    let before = instant(&db, "mem_used");
    assert_eq!(before.len(), 1, "{before:?}");

    // A single point carrying a *new* field. Under the scheme this replaces,
    // it renamed every `mem` series in the database — including the history
    // above — to `mem_used`, and `mem_used` was not selectable, so the
    // measurement became unreachable by name.
    db.insert_batch(&[Point::new(
        SeriesKey::new("mem", tags! { "host" => "a" }).unwrap(),
        fields! { "used" => 9.0, "free" => 1.0 },
        T - SEC,
    )
    .unwrap()])
        .unwrap()
        .into_complete()
        .unwrap();

    let after = instant(&db, "mem_used");
    assert_eq!(after.len(), 1, "{after:?}");
    assert_eq!(name_of(&after[0].0), "mem_used");
    assert_eq!(after[0].1, 9.0);

    // And the new field is its own metric rather than a renaming of the old.
    let free = instant(&db, "mem_free");
    assert_eq!(free.len(), 1, "{free:?}");
    assert_eq!(free[0].1, 1.0);
}

#[test]
fn a_row_without_the_field_is_not_a_sample_of_that_metric() {
    // `free` appears only on the newest point; the older rows carry a null in
    // that column and are not `mem_free` samples.
    let db = db_with(&[
        Point::new(
            SeriesKey::new("mem", tags! { "host" => "a" }).unwrap(),
            fields! { "used" => 1.0 },
            T - 3 * SEC,
        )
        .unwrap(),
        Point::new(
            SeriesKey::new("mem", tags! { "host" => "a" }).unwrap(),
            fields! { "used" => 2.0, "free" => 7.0 },
            T - 2 * SEC,
        )
        .unwrap(),
    ]);

    let expr = promql::parse("count_over_time(mem_free[1m])").unwrap();
    let ev = PromQLEvaluator::new(db.clone());
    let params = promql::eval::QueryParams {
        time: T,
        ..Default::default()
    };
    let promql::PromQLValue::Vector(v) = ev.instant_query(&expr, &params).unwrap() else {
        panic!("expected a vector");
    };
    assert_eq!(v.len(), 1);
    assert_eq!(
        v[0].samples[0].value, 1.0,
        "a null field column must not count as a sample"
    );
}

#[test]
fn a_selector_reaching_one_metric_never_produces_duplicate_label_sets() {
    let db = two_field_db();
    // `rate` drops `__name__`. When one selector reached *both* fields of a
    // measurement — which is what a bare measurement selector used to do —
    // that left two series with identical labels in one vector, and
    // `sum(cpu)` added a percentage to a load average.
    for query in ["rate(cpu_usage[1m])", "avg_over_time(up[1m])"] {
        let result = instant(&db, query);
        let mut seen = HashSet::new();
        for (labels, _) in &result {
            assert!(
                seen.insert(labels.clone()),
                "{query} returned two series with the labels {labels:?}"
            );
        }
    }
}

#[test]
fn a_vector_with_duplicate_label_sets_is_an_execution_error() {
    let db = two_field_db();
    // Two metrics, one label set once `__name__` is dropped. Prometheus
    // rejects the vector rather than answering with one of them, and the
    // message is matched on by clients.
    let expr = promql::parse("rate({__name__=~\"cpu.+\"}[1m])").unwrap();
    let ev = PromQLEvaluator::new(db);
    let params = promql::eval::QueryParams {
        time: T,
        ..Default::default()
    };
    let err = ev.instant_query(&expr, &params).unwrap_err();
    assert!(
        err.to_string()
            .contains("vector cannot contain metrics with the same labelset"),
        "{err}"
    );
}

#[test]
fn a_name_regex_matches_the_metric_name_not_the_measurement() {
    let db = two_field_db();
    let mut names: Vec<String> = instant(&db, "{__name__=~\"cpu.+\"}")
        .iter()
        .map(|(l, _)| name_of(l))
        .collect();
    names.sort();
    assert_eq!(names, ["cpu_load", "cpu_usage"]);

    // The measurement name is not a metric name, so anchoring on it finds
    // nothing — the regex is matched against what a result is called.
    assert!(instant(&db, "{__name__=~\"cpu\"}").is_empty());
}

#[test]
fn a_selector_without_a_name_selects_across_metrics() {
    let db = two_field_db();
    // Prometheus's rule is about *matchers*, not about the name: a selector
    // needs one matcher that does not match the empty string. `{host="a"}` is
    // a legal query upstream and reaches every metric carrying that label.
    let mut names: Vec<String> = instant(&db, "{host=\"a\"}")
        .iter()
        .map(|(l, _)| name_of(l))
        .collect();
    names.sort();
    assert_eq!(names, ["cpu_load", "cpu_usage"]);
}

#[test]
fn a_selector_that_matches_everything_is_refused() {
    // In the parser, as upstream does it, so the client sees a 400 with
    // `errorType: bad_data` rather than a 422 execution error.
    for query in [
        "{}",
        "{host=~\".*\"}",
        "{host!=\"\"} ",
        "{job=~\".*\",host=~\".*\"}",
    ] {
        let err = promql::parse(query.trim())
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        if query.contains("!=") {
            // `{host!=""}` is *not* satisfied by an absent label, so it is a
            // legal selector — the rule is about the matcher, not the syntax.
            assert!(err.is_empty(), "{query} must parse: {err}");
        } else {
            assert!(
                err.contains("at least one non-empty matcher"),
                "{query}: {err}"
            );
        }
    }
}

#[test]
fn an_unknown_function_is_a_parse_error() {
    // Upstream reports it from the parser, so it is a 400 with
    // `errorType: bad_data` rather than a 422 execution error — and a client
    // branches on that.
    let err = promql::parse("nope(cpu_usage)").unwrap_err();
    assert!(
        err.to_string().contains("unknown function with name"),
        "{err}"
    );
    // A real one still parses.
    assert!(promql::parse("rate(cpu_usage[5m])").is_ok());
}
