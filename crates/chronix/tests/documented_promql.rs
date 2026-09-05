#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap
//! Every PromQL query the documentation shows must **return something**.
//!
//! Parsing is not the bar here, because none of these queries was a syntax
//! error: they named metrics that do not exist. A PromQL metric is
//! `<measurement>_<field>`, and the documentation had been written as though
//! it were the measurement — so the landing page's
//! `rate(power[5m])` against `power,meter=main watts=…`, the getting-started
//! page's `rate(cpu[5m])` against `cpu,host=web-01 usage_idle=…` and the
//! README's own two examples all answered `{"result":[]}`. A reader following
//! any of them saw an empty graph and no error.
//!
//! So the assertion is that each query, evaluated against a fixture holding
//! the measurements those pages write, returns a non-empty result. A doc
//! example naming something outside the fixture fails, which is the point:
//! either the example is wrong, or the fixture should grow with it.

use std::sync::Arc;

use chronix::prelude::*;
use chronix::promql::{self, PromQLEvaluator, PromQLValue};
use chronix::{fields, tags, Chronix};

const NOW: i64 = 1_700_000_000_000_000_000;

/// The measurements the documented pages write, with the fields they write.
fn fixture(dir: &tempfile::TempDir) -> Arc<Chronix> {
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());

    let mut points = Vec::new();
    for i in 0..20i64 {
        let ts = NOW - (20 - i) * 15_000_000_000;
        // getting-started and the README: `cpu,host=… usage_idle=…`,
        // `usage=…`, `usage_system=…`.
        points.push(
            Point::new(
                SeriesKey::new("cpu", tags! { "host" => "server-01" }).unwrap(),
                fields! {
                    "usage_idle" => 95.5 + i as f64,
                    "usage" => 72.5 + i as f64,
                    "usage_system" => 1.0 + i as f64
                },
                ts,
            )
            .unwrap(),
        );
        // The README's write example: `cpu,host=srv1 usage=72.5`.
        points.push(
            Point::new(
                SeriesKey::new("cpu", tags! { "host" => "srv1" }).unwrap(),
                fields! { "usage" => 72.5 + i as f64 },
                ts,
            )
            .unwrap(),
        );
        // The landing page: `power,meter=main watts=…`.
        points.push(
            Point::new(
                SeriesKey::new("power", tags! { "meter" => "main" }).unwrap(),
                fields! { "watts" => 231.45 + i as f64 },
                ts,
            )
            .unwrap(),
        );
        // The Grafana page's alerting and latency examples. The two chronix
        // metrics stand in for a Prometheus that has scraped this server —
        // and they must be named the way the server actually emits them,
        // which `chronixd --test documented_metrics` is what enforces. This
        // fixture previously invented `chronix_active_series_total` and
        // `chronix_ingested_points_total`, so it made the documentation pass
        // by agreeing with its mistake.
        points.push(
            Point::new(
                SeriesKey::new("query_duration_seconds_bucket", tags! { "le" => "0.5" }).unwrap(),
                fields! { "value" => i as f64 },
                ts,
            )
            .unwrap(),
        );
        points.push(
            Point::new(
                SeriesKey::new("query_duration_seconds_bucket", tags! { "le" => "+Inf" }).unwrap(),
                fields! { "value" => 2.0 * i as f64 },
                ts,
            )
            .unwrap(),
        );
        points.push(
            Point::new(
                SeriesKey::new("chronix_series_count", tags! {}).unwrap(),
                fields! { "value" => i as f64 },
                ts,
            )
            .unwrap(),
        );
        points.push(
            Point::new(
                SeriesKey::new("chronix_points_written_total", tags! {}).unwrap(),
                fields! { "value" => i as f64 },
                ts,
            )
            .unwrap(),
        );
    }
    db.insert_batch(&points).unwrap().into_complete().unwrap();
    db
}

/// Every PromQL query the published documentation shows, with where it came
/// from: ```promql fences, and the `query=` parameter of a `curl` example.
fn documented_queries() -> Vec<(String, String)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("repo root");

    let mut files = vec![root.join("README.md")];
    let mut stack = vec![root.join("site").join("content")];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("readable docs directory") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "md") {
                files.push(path);
            }
        }
    }

    let mut out = Vec::new();
    for path in files {
        let text = std::fs::read_to_string(&path).expect("readable markdown");
        let name = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .display()
            .to_string();
        for (i, q) in fenced_promql(&text)
            .into_iter()
            .chain(curl_queries(&text))
            .enumerate()
        {
            out.push((format!("{name} [{i}]"), q));
        }
    }
    out.sort();
    out
}

/// Each non-comment line of every ```promql fence. The fences hold one query
/// per line, separated by blank lines and `#` comments.
fn fenced_promql(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut inside = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("```promql") {
            inside = true;
        } else if inside && trimmed == "```" {
            inside = false;
        } else if inside && !trimmed.is_empty() && !trimmed.starts_with('#') {
            out.push(trimmed.to_string());
        }
    }
    out
}

/// The `query=…` parameter of every `curl` example.
///
/// The query ends at the quote that opened the URL, not at the first quote:
/// `curl 'http://…?query=cpu_usage{host="srv1"}&time=…'` is one argument, and
/// cutting at the inner `"` yields a fragment that cannot parse.
fn curl_queries(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in text.lines() {
        let Some(at) = line.find("query=") else {
            continue;
        };
        let opener = line[..at]
            .chars()
            .rev()
            .find(|c| *c == '\'' || *c == '"' || *c == '`');
        let rest = &line[at + "query=".len()..];
        let end = rest
            .find(|c: char| opener.is_some_and(|o| c == o) || c == '&')
            .unwrap_or(rest.len());
        let q = rest[..end].trim();
        if !q.is_empty() && q.contains(|c: char| c.is_ascii_alphabetic()) {
            out.push(q.to_string());
        }
    }
    out
}

/// An alerting expression's *threshold* is allowed to filter everything out —
/// that is what a threshold is for — so the check applies to the vector it
/// compares. Without this the fixture would have to be shaped to satisfy every
/// documented alert condition, which tests the fixture rather than the docs.
fn under_threshold(expr: &promql::Expr) -> &promql::Expr {
    use promql::{BinaryOp, Expr};
    if let Expr::BinaryExpr { op, lhs, rhs, .. } = expr {
        if matches!(
            op,
            BinaryOp::Gtr
                | BinaryOp::Lss
                | BinaryOp::Gte
                | BinaryOp::Lte
                | BinaryOp::Eql
                | BinaryOp::Neq
        ) {
            if matches!(**rhs, Expr::NumberLiteral(_)) {
                return under_threshold(lhs);
            }
            if matches!(**lhs, Expr::NumberLiteral(_)) {
                return under_threshold(rhs);
            }
        }
    }
    expr
}

#[test]
fn every_documented_promql_query_returns_something() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let ev = PromQLEvaluator::new(db);

    let queries = documented_queries();
    assert!(
        queries.len() >= 6,
        "the extractor found only {} queries, so it is not working",
        queries.len()
    );

    let mut failures = Vec::new();
    for (origin, query) in &queries {
        let params = promql::eval::QueryParams {
            time: NOW,
            ..Default::default()
        };
        match promql::parse(query) {
            Err(e) => failures.push(format!("{origin}: {query}\n    parse error: {e}")),
            Ok(expr) => match ev.instant_query(under_threshold(&expr), &params) {
                Err(e) => failures.push(format!("{origin}: {query}\n    {e}")),
                Ok(PromQLValue::Vector(v)) if v.is_empty() => failures.push(format!(
                    "{origin}: {query}\n    returned no series — the metric it names does not exist"
                )),
                Ok(PromQLValue::Matrix(m)) if m.is_empty() => failures.push(format!(
                    "{origin}: {query}\n    returned no series — the metric it names does not exist"
                )),
                Ok(_) => {}
            },
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} documented PromQL queries do not answer:\n\n{}",
        failures.len(),
        queries.len(),
        failures.join("\n")
    );
}
