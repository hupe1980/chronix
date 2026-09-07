#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap
//! An exact decimal survives every wire format the server speaks.
//!
//! The failure this file exists to catch is not a wrong number — it is a
//! *plausible* one. `1234.5678` through a JSON `double` and back prints as
//! `1234.5678`; the digit that goes missing is the eighteenth, and no
//! eyeball review finds it. So every assertion below compares digits, and
//! the values are chosen to be ones binary floating point cannot represent.
//!
//! It also covers the two silent-drop paths that existed before decimals
//! did: the HTTP and gRPC row encoders both ended in a catch-all that
//! logged at `debug` and left the column out of the response entirely — a
//! query that returned rows with the one column the caller asked for
//! missing.

use reqwest::StatusCode;
use serde_json::{json, Value};

use super::integration::{client, start_test_server};

/// The digits a decimal field came back as, from the point-shaped query API.
fn decimal_field(row: &Value, name: &str) -> String {
    row["fields"][name]["decimal"]
        .as_str()
        .unwrap_or_else(|| {
            panic!(
                "expected {{\"decimal\": \"…\"}} for '{name}', got {}",
                row["fields"]
            )
        })
        .to_string()
}

async fn query_rows(base: &str, measurement: &str) -> Vec<Value> {
    let resp = client()
        .post(format!("{base}/api/v1/chronix/query"))
        .json(&json!({ "measurement": measurement }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "query failed");
    resp.json::<Vec<Value>>().await.unwrap()
}

#[tokio::test]
async fn a_decimal_written_as_json_reads_back_with_every_digit() {
    let (base, _tmp) = start_test_server().await;

    let resp = client()
        .post(format!("{base}/api/v1/write"))
        .json(&json!([{
            "measurement": "meter",
            "tags": {"device": "main"},
            // Seventeen significant digits: past what a double can hold.
            "fields": {"z1nb_q": {"decimal": "0.30000000000000004"}},
            "timestamp": 1_700_000_000_000_000_000_i64,
        }]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT, "write failed");

    let rows = query_rows(&base, "meter").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(decimal_field(&rows[0], "z1nb_q"), "0.30000000000000004");
}

#[tokio::test]
async fn a_decimal_read_back_is_a_decimal_that_can_be_written_back() {
    // The read shape and the write shape are the same object, so a point
    // that round-trips through a client does not degrade on the way.
    let (base, _tmp) = start_test_server().await;
    client()
        .post(format!("{base}/api/v1/write"))
        .json(&json!([{
            "measurement": "meter",
            "fields": {"v": {"decimal": "1234.5678"}},
            "timestamp": 1_000_i64,
        }]))
        .send()
        .await
        .unwrap();

    let rows = query_rows(&base, "meter").await;
    let echoed = rows[0]["fields"]["v"].clone();

    let resp = client()
        .post(format!("{base}/api/v1/write"))
        .json(&json!([{
            "measurement": "meter",
            "fields": {"v": echoed},
            "timestamp": 2_000_i64,
        }]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let rows = query_rows(&base, "meter").await;
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(decimal_field(row, "v"), "1234.5678");
    }
}

#[tokio::test]
async fn a_decimal_written_as_a_json_number_is_refused() {
    // Accepting it would mean accepting whatever double the parser had
    // already made of the digits — the loss the type exists to prevent,
    // arriving at the last possible moment.
    let (base, _tmp) = start_test_server().await;
    let resp = client()
        .post(format!("{base}/api/v1/write"))
        .json(&json!([{
            "measurement": "meter",
            "fields": {"v": {"decimal": 1234.5678}},
            "timestamp": 1_000_i64,
        }]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await.unwrap();
    assert!(
        body["error"].as_str().unwrap().contains("string of digits"),
        "{body}"
    );
}

#[tokio::test]
async fn line_protocol_carries_a_decimal_with_the_d_suffix() {
    let (base, _tmp) = start_test_server().await;
    let resp = client()
        .post(format!("{base}/write"))
        .header("Content-Type", "text/plain")
        .body("meter,device=main z1nb_q=1234.5678d 1700000000000000000")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT, "line protocol write");

    let rows = query_rows(&base, "meter").await;
    assert_eq!(decimal_field(&rows[0], "z1nb_q"), "1234.5678");
}

#[tokio::test]
async fn line_protocol_without_the_suffix_is_still_a_float() {
    // The extension must not capture the ordinary case.
    let (base, _tmp) = start_test_server().await;
    client()
        .post(format!("{base}/write"))
        .header("Content-Type", "text/plain")
        .body("cpu usage=72.5 1700000000000000000")
        .send()
        .await
        .unwrap();
    let rows = query_rows(&base, "cpu").await;
    assert!(
        rows[0]["fields"]["usage"].is_number(),
        "a float field must stay a JSON number: {}",
        rows[0]["fields"]
    );
}

#[tokio::test]
async fn a_decimal_cell_in_a_sql_result_is_a_string_of_digits() {
    let (base, _tmp) = start_test_server().await;
    client()
        .post(format!("{base}/api/v1/write"))
        .json(&json!([
            {"measurement": "meter", "fields": {"v": {"decimal": "0.1"}}, "timestamp": 1},
            {"measurement": "meter", "fields": {"v": {"decimal": "0.2"}}, "timestamp": 2},
        ]))
        .send()
        .await
        .unwrap();

    let resp = client()
        .post(format!("{base}/api/v1/chronix/sql"))
        .json(&json!({"query": "SELECT sum(v) AS total FROM meter"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let cell = &body["rows"][0][0];
    // 0.1 + 0.2 is 0.30000000000000004 in a double, and it is not that here.
    assert_eq!(cell.as_str(), Some("0.3"), "{body}");
}

#[tokio::test]
async fn the_schema_endpoint_reports_the_decimal_type_with_its_scale() {
    let (base, _tmp) = start_test_server().await;
    client()
        .post(format!("{base}/api/v1/write"))
        .json(&json!([{
            "measurement": "meter",
            "fields": {"v": {"decimal": "1.5000"}},
            "timestamp": 1,
        }]))
        .send()
        .await
        .unwrap();

    let resp = client()
        .get(format!("{base}/api/v1/measurements/meter/schema"))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let column = body["columns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "v")
        .unwrap_or_else(|| panic!("no column 'v' in {body}"));
    // The scale is part of the type, so it has to be part of the name: a
    // client told only "decimal" cannot tell what the column stores.
    assert_eq!(column["data_type"], "decimal(38, 4)");
}

#[tokio::test]
async fn declaring_a_decimal_field_fixes_its_scale_before_the_first_write() {
    let (base, _tmp) = start_test_server().await;

    let resp = client()
        .post(format!("{base}/api/v1/measurements/meter/schema/fields"))
        .json(&json!({"name": "z1nb_q", "type": "decimal", "scale": 4}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "declare failed");

    // A narrower value is widened to the declared scale…
    client()
        .post(format!("{base}/api/v1/write"))
        .json(&json!([{
            "measurement": "meter",
            "fields": {"z1nb_q": {"decimal": "1.5"}},
            "timestamp": 1,
        }]))
        .send()
        .await
        .unwrap();
    let rows = query_rows(&base, "meter").await;
    assert_eq!(decimal_field(&rows[0], "z1nb_q"), "1.5000");

    // …and one that would have to be rounded is refused, not rounded.
    let resp = client()
        .post(format!("{base}/api/v1/write"))
        .json(&json!([{
            "measurement": "meter",
            "fields": {"z1nb_q": {"decimal": "1.00005"}},
            "timestamp": 2,
        }]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("scale"), "{body}");
}

#[tokio::test]
async fn declaring_a_scale_on_a_non_decimal_column_is_refused() {
    // Ignoring it would let `{"type": "int64", "scale": 4}` look honoured.
    let (base, _tmp) = start_test_server().await;
    let resp = client()
        .post(format!("{base}/api/v1/measurements/meter/schema/fields"))
        .json(&json!({"name": "n", "type": "int64", "scale": 4}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn declaring_a_conflicting_type_is_refused() {
    let (base, _tmp) = start_test_server().await;
    let declare = |body: Value| {
        let base = base.clone();
        async move {
            client()
                .post(format!("{base}/api/v1/measurements/meter/schema/fields"))
                .json(&body)
                .send()
                .await
                .unwrap()
                .status()
        }
    };
    assert_eq!(
        declare(json!({"name": "v", "type": "decimal", "scale": 4})).await,
        StatusCode::OK
    );
    // Same type twice is a no-op, not an error.
    assert_eq!(
        declare(json!({"name": "v", "type": "decimal(38, 4)"})).await,
        StatusCode::OK
    );
    // A different scale is a different type, and a type change is not a
    // thing Chronix has.
    assert_eq!(
        declare(json!({"name": "v", "type": "decimal", "scale": 6})).await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        declare(json!({"name": "v", "type": "float64"})).await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        declare(json!({"name": "v", "type": "not-a-type"})).await,
        StatusCode::BAD_REQUEST
    );
}
