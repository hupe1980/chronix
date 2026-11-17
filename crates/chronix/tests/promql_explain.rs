//! `Chronix::promql_explain`: a query as a tree of sub-expressions, each one
//! evaluated on its own with `analyze`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap

use std::collections::BTreeMap;

use chronix::fields;
use chronix::prelude::*;
use chronix::promql::{ExplainNode, parse};

const T0: i64 = 1_700_000_000_000_000_000;
const SEC: i64 = 1_000_000_000;

/// Printing an expression and parsing it again gives the same tree, so the
/// text an explain shows is a query the user can paste.
#[test]
fn print_round_trips() {
    for q in [
        "m",
        r#"m{job="api", code=~"5..", path!="/x\"y"}"#,
        r#"{__name__=~"cpu_.*"}"#,
        "rate(m[5m])",
        "rate(m[1h30m] offset 2d)",
        "m[5m] anchored",
        "m[500ms]",
        "m smoothed",
        "m @ 1609746000",
        "rate(m[5m] @ start())",
        "sum by (host, dc) (rate(m[5m]))",
        "topk(3, m)",
        "count_values(\"v\", m)",
        "a + on (host) group_left (dc) b",
        "a / ignoring (code) b",
        "a > bool 3",
        "a and b or c unless d",
        "-m",
        "(a + b) * c",
        "max_over_time(rate(m[5m])[1h:1m])",
        "m[5m * 2]",
        "m offset (1h / 2)",
        "histogram_quantile(0.9, sum by (le) (rate(h_bucket[5m])))",
        "2 ^ 3 ^ 2",
        "NaN",
        "-Inf",
    ] {
        let first = parse(q).unwrap_or_else(|e| panic!("{q}: {e}"));
        let printed = first.to_string();
        let second = parse(&printed).unwrap_or_else(|e| panic!("{q} printed as {printed}: {e}"));
        // `Debug`, not `==`: `NaN` is a literal that equals nothing.
        assert_eq!(
            format!("{first:?}"),
            format!("{second:?}"),
            "{q} printed as {printed}"
        );
    }
}

fn db_with_two_hosts(dir: &std::path::Path) -> Chronix {
    let db = Chronix::open(ChronixConfig::builder().data_dir(dir).build().unwrap()).unwrap();
    for host in ["a", "b"] {
        for i in 0..40 {
            let tags: BTreeMap<String, String> = [("host".to_owned(), host.to_owned())].into();
            let key = SeriesKey::new("cpu", tags).unwrap();
            db.insert(
                &Point::new(key, fields! { "usage" => i as f64 }, T0 + i * 15 * SEC).unwrap(),
            )
            .unwrap();
        }
    }
    db
}

fn search<'a>(node: &'a ExplainNode, name: &str) -> Option<&'a ExplainNode> {
    if node.node == name {
        return Some(node);
    }
    node.children.iter().find_map(|c| search(c, name))
}

fn find<'a>(node: &'a ExplainNode, name: &str) -> &'a ExplainNode {
    search(node, name).unwrap_or_else(|| panic!("no {name} node under {}", node.text))
}

#[test]
fn promql_explain_counts_series_per_node() {
    let tmp = tempfile::tempdir().unwrap();
    let db = db_with_two_hosts(tmp.path());
    let at = T0 + 600 * SEC;

    let plan = db
        .promql_explain("sum(rate(cpu_usage[5m]))", at, at, None, true)
        .unwrap();
    let root = &plan.tree;
    assert_eq!(root.node, "sum");
    assert_eq!(root.text, "sum (rate(cpu_usage[5m]))");
    assert_eq!(root.series, Some(1), "two hosts summed into one");

    let rate = find(root, "rate");
    assert_eq!(rate.series, Some(2), "one rate per host");
    let range = find(root, "range selector");
    assert_eq!(range.text, "cpu_usage[5m]");
    assert_eq!(range.series, Some(2));
    // (300 s, 600 s] at 15 s; the last sample is at 585 s.
    assert_eq!(range.samples, Some(2 * 19));
}

#[test]
fn promql_explain_names_the_node_that_lost_the_series() {
    let tmp = tempfile::tempdir().unwrap();
    let db = db_with_two_hosts(tmp.path());
    let at = T0 + 600 * SEC;

    let plan = db
        .promql_explain(r#"sum(cpu_usage{host="c"})"#, at, at, None, true)
        .unwrap();
    let selector = find(&plan.tree, "selector");
    assert_eq!(selector.series, Some(0), "host c has no data");
}

#[test]
fn promql_explain_without_analyze_evaluates_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let db = db_with_two_hosts(tmp.path());
    let plan = db
        .promql_explain("rate(cpu_usage[5m])", T0, T0, None, false)
        .unwrap();
    assert_eq!(plan.tree.series, None);
    assert_eq!(plan.tree.children[0].series, None);
}

#[test]
fn promql_explain_over_a_range_counts_every_step() {
    let tmp = tempfile::tempdir().unwrap();
    let db = db_with_two_hosts(tmp.path());
    let (start, end) = (T0 + 300 * SEC, T0 + 600 * SEC);
    let plan = db
        .promql_explain("rate(cpu_usage[5m])", start, end, Some(60 * SEC), true)
        .unwrap();
    assert_eq!(plan.tree.series, Some(2));
    assert_eq!(plan.tree.samples, Some(2 * 6), "six steps per host");
    // The range selector has no value per step; it is read at the end.
    assert_eq!(plan.tree.children[0].series, Some(2));
}

#[test]
fn promql_explain_refuses_a_query_that_does_not_parse() {
    let tmp = tempfile::tempdir().unwrap();
    let db = db_with_two_hosts(tmp.path());
    assert!(db.promql_explain("sum(", T0, T0, None, true).is_err());
}
