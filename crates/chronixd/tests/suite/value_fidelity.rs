#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap
//! Every Arrow type a query can produce reaches the client as data.
//!
//! Each surface used to carry its own `match` over `DataType`, and each
//! covered a different subset. A type nobody had enumerated came back as the
//! literal string `"<unsupported: Date32>"` inside a column whose declared
//! `data_type` was `Date32`, under `200 OK` — indistinguishable from a value.
//! `SELECT CAST(_time AS DATE) … GROUP BY 1`, an ordinary group-by-day, was
//! one such query. Flight SQL, which hands Arrow through untouched, answered
//! the same query correctly, so the surfaces disagreed about the same result.
//!
//! The structural guard is in `chronixd::wire::value`: its match has no `_`
//! arm, so an Arrow release that adds a variant breaks the build. These tests
//! are the behavioural half — they pin the *encoding* each type gets, which a
//! compile-time check cannot.

use serde_json::{json, Value};

use super::integration::{client, start_test_server};

/// Run one SQL query, returning `(data_type, first cell)` of column 0.
async fn cell(base: &str, sql: &str) -> (String, Value) {
    let resp = client()
        .post(format!("{base}/api/v1/chronix/sql"))
        .json(&json!({ "query": sql }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body: Value = resp.json().await.unwrap();
    assert!(status.is_success(), "`{sql}` failed: {body}");
    let dt = body["columns"][0]["data_type"]
        .as_str()
        .unwrap()
        .to_string();
    (dt, body["rows"][0][0].clone())
}

#[tokio::test]
async fn every_arrow_type_reaches_json_as_a_value_not_a_placeholder() {
    let (base, _tmp) = start_test_server().await;

    // `arrow_cast` is the only way to name most of these from SQL, which is
    // also how a user reaches them: a cast, a struct, an interval.
    let cases: &[(&str, &str, Value)] = &[
        ("SELECT arrow_cast(1, 'Int8')", "Int8", json!(1)),
        ("SELECT arrow_cast(1, 'Int32')", "Int32", json!(1)),
        ("SELECT arrow_cast(1, 'UInt32')", "UInt32", json!(1)),
        ("SELECT arrow_cast(1.5, 'Float32')", "Float32", json!(1.5)),
        // Every decimal width, as digits — a JSON number would be parsed
        // back through an `f64` by every client on the planet.
        (
            "SELECT arrow_cast(123, 'Decimal32(9, 2)')",
            "Decimal32(9, 2)",
            json!("123.00"),
        ),
        (
            "SELECT arrow_cast(123, 'Decimal64(15, 2)')",
            "Decimal64(15, 2)",
            json!("123.00"),
        ),
        (
            "SELECT arrow_cast(1, 'Decimal256(40, 2)')",
            "Decimal256(40, 2)",
            json!("1.00"),
        ),
        // A *negative* scale is legal in Arrow. The hand-rolled converter
        // read the scale into a `u8`, so this reached the client as `null` —
        // silent loss, in the one column type that exists to be exact.
        (
            "SELECT arrow_cast(12345, 'Decimal128(10, -2)')",
            "Decimal128(10, -2)",
            json!("12300"),
        ),
        // Temporal values are integers in the unit `data_type` names.
        (
            "SELECT arrow_cast('2025-09-11', 'Date32')",
            "Date32",
            json!(20_342),
        ),
        (
            "SELECT arrow_cast('12:34:56', 'Time64(Nanosecond)')",
            "Time64(ns)",
            json!(45_296_000_000_000_i64),
        ),
        ("SELECT arrow_cast('x', 'Utf8View')", "Utf8View", json!("x")),
        // Bytes are base64; "abc" is "YWJj".
        (
            "SELECT arrow_cast('abc', 'Binary')",
            "Binary",
            json!("YWJj"),
        ),
        ("SELECT NULL", "Null", Value::Null),
        // An interval is three independent parts, because a month is not a
        // span of anything. An object keeps all three.
        (
            "SELECT INTERVAL '1 day'",
            "Interval(MonthDayNano)",
            json!({"months": 0, "days": 1, "nanoseconds": 0}),
        ),
        (
            "SELECT struct(1, 2)",
            "Struct(\"c0\": Int64, \"c1\": Int64)",
            json!({"c0": 1, "c1": 2}),
        ),
        (
            "SELECT make_array(1, 2, 3)",
            "List(Int64)",
            json!([1, 2, 3]),
        ),
    ];

    for (sql, want_type, want_value) in cases {
        let (dt, v) = cell(&base, sql).await;
        assert_eq!(&dt, want_type, "`{sql}` reported an unexpected type");
        assert_eq!(&v, want_value, "`{sql}` ({dt}) encoded wrongly");
        // The shape of the old defect, stated directly: a placeholder is a
        // string that describes a type instead of carrying a value.
        assert!(
            !v.as_str().is_some_and(|s| s.starts_with("<unsupported")),
            "`{sql}` answered a placeholder: {v}"
        );
    }
}

#[tokio::test]
async fn grouping_by_day_returns_the_day() {
    // The query that found all of this: `CAST(_time AS DATE)` is how anyone
    // groups by day, and its column came back as `"<unsupported: Date32>"`.
    let (base, _tmp) = start_test_server().await;

    let resp = client()
        .post(format!("{base}/api/v2/write"))
        .body("day,host=a v=1 1757548800000000000")
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "write failed");

    let (dt, v) = cell(
        &base,
        "SELECT CAST(_time AS DATE) AS d, count(*) FROM day GROUP BY 1",
    )
    .await;
    assert_eq!(dt, "Date32");
    // 2025-09-11, as days since the epoch.
    assert_eq!(v, json!(20_342));
}

#[tokio::test]
async fn a_non_finite_float_is_null_rather_than_a_string() {
    // JSON has no `NaN` literal. A string in a column declared `Float64`
    // would be the same poison this module exists to remove, so the encoding
    // is `null` — documented, and the reason Flight SQL exists for callers
    // who need the distinction.
    let (base, _tmp) = start_test_server().await;
    let (dt, v) = cell(&base, "SELECT arrow_cast('NaN', 'Float64')").await;
    assert_eq!(dt, "Float64");
    assert_eq!(v, Value::Null);
}

// ── Error classification ───────────────────────────────────────────────

/// The `code` and message a failing query answers with.
async fn sql_error(base: &str, sql: &str) -> (u16, String) {
    let resp = client()
        .post(format!("{base}/api/v1/chronix/sql"))
        .json(&json!({ "query": sql }))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let body: Value = resp.json().await.unwrap();
    (
        status,
        body["error"].as_str().unwrap_or_default().to_string(),
    )
}

#[tokio::test]
async fn a_mistake_in_a_query_says_what_it_was() {
    // Six of these ten answered `500 … an internal error occurred`, because
    // the classifier ended in a `_` arm. A caller could not tell their own
    // typo from a server fault, and the message that named the problem was
    // redacted on the way out.
    let (base, _tmp) = start_test_server().await;

    let cases: &[(&str, &str)] = &[
        ("SELECT 1/0", "Divide by zero"),
        ("SELECT 0.0/0.0", "Divide by zero"),
        ("SELECT CAST('abc' AS INT)", "Cast error"),
        ("SELECT to_timestamp('not-a-date')", "not-a-date"),
        ("SELECT date_trunc('fortnight', now())", "fortnight"),
        ("SELECT 'a' AT TIME ZONE 'Mars/Phobos'", "Mars/Phobos"),
        ("SELECT unknownfn(1)", "unknownfn"),
        ("SELECT * FROM nosuchtable", "nosuchtable"),
    ];

    for (sql, want) in cases {
        let (status, msg) = sql_error(&base, sql).await;
        assert_eq!(status, 400, "`{sql}` should be the caller's fault: {msg}");
        assert!(
            msg.contains(want),
            "`{sql}` should say what was wrong; got {msg}"
        );
    }
}

#[tokio::test]
async fn integer_overflow_wraps_which_is_a_recorded_deviation() {
    // Pinning current behaviour, not endorsing it. DataFusion's
    // `BinaryExpr::evaluate_with_resolved_args` calls `add_wrapping` /
    // `sub_wrapping` / `mul_wrapping` unconditionally, and
    // `enable_ansi_mode` does not reach them. PostgreSQL and DuckDB raise
    // here. The deviation is recorded in the backlog and stated in the
    // data-model documentation; this test is what makes a DataFusion upgrade
    // that fixes it *visible* rather than silent — the same reason each
    // PromQL deviation has one.
    let (base, _tmp) = start_test_server().await;

    let resp = client()
        .post(format!("{base}/api/v2/write"))
        .body("ovf,h=a v=9223372036854775807i 1757548800000000000")
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "write failed");

    let (_dt, v) = cell(&base, "SELECT v + 1 AS wrapped FROM ovf").await;
    assert_eq!(
        v,
        json!(i64::MIN),
        "if this now errors or saturates, DataFusion changed: \
         drop the recorded deviation from the backlog and the docs"
    );
}
