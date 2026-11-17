#![cfg(feature = "sql")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Errors that say what was meant, in the style of DuckDB's
//! `did_you_mean.test`: for every kind of name a query can get wrong, one
//! typo with a close match, one with none, and one with several — and the
//! span must cover exactly the name as typed.

use chronix::prelude::*;
use chronix::promql::AggregationOp;
use chronix::{Chronix, fields, tags};

const T: i64 = 1_700_000_000_000_000_000;

fn db(dir: &std::path::Path) -> Chronix {
    let db = Chronix::open(
        ChronixConfig::builder()
            .data_dir(dir)
            .maintenance_interval(std::time::Duration::ZERO)
            .build()
            .unwrap(),
    )
    .unwrap();
    for (m, f) in [
        ("cpu_usage", "usage"),
        ("cpu_used", "used"),
        ("cpu_user", "user"),
        ("mem", "free"),
    ] {
        let key = SeriesKey::new(m, tags! { "host" => "a" }).unwrap();
        db.insert(&Point::new(key, fields! { f => 1.0 }, T).unwrap())
            .unwrap();
    }
    db
}

/// One case: the query, the message it must produce, the hint, and the text
/// the span must cover (`None`: no span expected).
struct Case {
    query: &'static str,
    message: &'static str,
    hint: Option<&'static str>,
    spans: Option<&'static str>,
}

fn check_sql(db: &Chronix, case: &Case) {
    let err = db.sql(case.query).expect_err(case.query);
    let d = db.diagnose(&err, case.query, None);
    assert_eq!(d.message.as_deref(), Some(case.message), "{}", case.query);
    assert_eq!(d.hint.as_deref(), case.hint, "{}", case.query);
    let covered = d.span.map(|s| &case.query[s.start..s.end]);
    assert_eq!(covered, case.spans, "{}", case.query);
}

#[test]
fn a_misspelt_table_is_corrected() {
    let dir = tempfile::tempdir().unwrap();
    let db = db(dir.path());
    for case in [
        Case {
            query: "SELECT * FROM cpu_usge",
            message: r#"unknown table "cpu_usge" — did you mean "cpu_usage"?"#,
            hint: Some(r#"did you mean "cpu_usage"?"#),
            spans: Some("cpu_usge"),
        },
        Case {
            query: "SELECT * FROM zzzzzzzz",
            message: r#"unknown table "zzzzzzzz"; the tables are "cpu_usage", "cpu_used", "cpu_user" and "mem""#,
            hint: None,
            spans: Some("zzzzzzzz"),
        },
        Case {
            // An alias after the name, and a tie.
            query: "SELECT * FROM cpu_usex AS c",
            message: r#"unknown table "cpu_usex" — did you mean "cpu_used" or "cpu_user"?"#,
            hint: Some(r#"did you mean "cpu_used" or "cpu_user"?"#),
            spans: Some("cpu_usex"),
        },
    ] {
        check_sql(&db, &case);
    }
}

#[test]
fn a_misspelt_column_is_corrected() {
    let dir = tempfile::tempdir().unwrap();
    let db = db(dir.path());
    for case in [
        Case {
            query: "SELECT usgae FROM cpu_usage",
            message: r#"unknown column "usgae" — did you mean "usage"?"#,
            hint: Some(r#"did you mean "usage"?"#),
            spans: Some("usgae"),
        },
        Case {
            query: "SELECT zzzzzz FROM cpu_usage",
            message: r#"unknown column "zzzzzz"; the columns are "_time", "host" and "usage""#,
            hint: None,
            spans: Some("zzzzzz"),
        },
        Case {
            // Non-ASCII before the error must not move the span.
            query: "SELECT 'ü' AS x, hots FROM cpu_usage",
            message: r#"unknown column "hots" — did you mean "host"?"#,
            hint: Some(r#"did you mean "host"?"#),
            spans: Some("hots"),
        },
    ] {
        check_sql(&db, &case);
    }
}

#[test]
fn a_misspelt_sql_function_is_corrected() {
    let dir = tempfile::tempdir().unwrap();
    let db = db(dir.path());
    for case in [
        Case {
            query: "SELECT forcast(usage, _time, 3) FROM cpu_usage",
            message: r#"unknown function "forcast" — did you mean "forecast"?"#,
            hint: Some(r#"did you mean "forecast"?"#),
            spans: Some("forcast"),
        },
        Case {
            // DataFusion always proposes *something*; a distant proposal is
            // not passed on.
            query: "SELECT qqqqzzzz(usage) FROM cpu_usage",
            message: r#"unknown function "qqqqzzzz""#,
            hint: None,
            spans: Some("qqqqzzzz"),
        },
    ] {
        check_sql(&db, &case);
    }
}

#[test]
fn a_misspelt_promql_function_is_corrected() {
    let dir = tempfile::tempdir().unwrap();
    let db = db(dir.path());
    for (query, message, hint, spans) in [
        (
            "rat(cpu_usage_usage[5m])",
            r#"PromQL error: parse error: unknown function with name "rat" — did you mean "rate"?"#,
            Some(r#"did you mean "rate"?"#),
            "rat",
        ),
        (
            "1 + qqqqzzzz(cpu_usage_usage)",
            r#"PromQL error: parse error: unknown function with name "qqqqzzzz""#,
            None,
            "qqqqzzzz",
        ),
        (
            // Aggregation operators are candidates too.
            "summ(cpu_usage_usage)",
            r#"PromQL error: parse error: unknown function with name "summ" — did you mean "sum"?"#,
            Some(r#"did you mean "sum"?"#),
            "summ",
        ),
    ] {
        let err = db.promql(query, T).expect_err(query);
        assert_eq!(err.to_string(), message, "{query}");
        let d = db.diagnose(&err, query, None);
        assert_eq!(d.hint.as_deref(), hint, "{query}");
        let s = d.span.expect(query);
        assert_eq!(&query[s.start..s.end], spans, "{query}");
    }
}

#[test]
fn a_promql_syntax_error_points_at_the_token() {
    let dir = tempfile::tempdir().unwrap();
    let db = db(dir.path());
    let query = r#"rate(m{x="ü"}[5m]]"#;
    let err = db.promql(query, T).expect_err(query);
    let s = err.diagnosis(query).span.unwrap();
    assert_eq!(&query[s.start..s.end], "]");
    // The column Prometheus prints: characters, not bytes.
    assert_eq!(s.column(query), query.chars().count());
}

#[test]
fn every_aggregation_name_is_an_aggregation() {
    for name in AggregationOp::NAMES {
        assert!(AggregationOp::parse_op(name).is_some(), "{name}");
    }
}
