#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! What the real clients send.
//!
//! Every test here sends the bytes a stock Telegraf, OpenTelemetry Collector
//! or Prometheus produces with its **default** configuration — the route it
//! derives, the encoding it applies, the parameters it sets. A conformance
//! suite that builds its own requests cannot find the defects these catch:
//! it never posts to `/write`, never gzips, and never sets `precision`.

use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;

use serde_json::Value;
use tempfile::TempDir;
use tokio::net::TcpListener;

use chronix::prelude::*;
use chronix::Chronix;
use chronixd::http::{AppState, SharedState};
use chronixd::server::build_router;

async fn server() -> (String, TempDir, Arc<Chronix>) {
    let tmp = TempDir::new().expect("tempdir");
    let config = ChronixConfigBuilder::default()
        .data_dir(tmp.path().to_path_buf())
        .build()
        .expect("chronix config");
    let db = Arc::new(Chronix::open(config).expect("open db"));
    let sql_contexts = chronixd::namespace::SqlContexts::new(db.clone());
    let state: AppState = Arc::new(SharedState {
        db: db.clone(),
        start_time: std::time::Instant::now(),
        connector_manager: None,
        sql_contexts,
        auth_state: None,
        #[cfg(feature = "cluster")]
        meta_client: None,
        namespace_registry: std::sync::Arc::new(chronix_security::tenant::NamespaceRegistry::new()),
        model_catalog: Arc::new(parking_lot::RwLock::new(
            chronix::chronix_analytics::forecast::ModelCatalog::new(),
        )),
        authz_engine: None,
        audit_logger: None,
        config: chronixd::config::ServerConfig::default(),
        namespace_rate_limiter: chronixd::rate_limit::NamespaceRateLimiter::new(),
        sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
        write_dedup_cache: None,
        write_timeout: std::time::Duration::ZERO,
        pipeline: None,
        openapi_json: std::sync::OnceLock::new(),
    });
    let metrics_handle = chronixd::server::prometheus_builder()
        .expect("bucket config")
        .build_recorder()
        .handle();
    let app = build_router(state, "/metrics", metrics_handle, 10 * 1024 * 1024, None);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let base = format!("http://{addr}");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (base, tmp, db)
}

/// A client, with the rustls provider installed — these suites never call
/// `server::run`, so nothing else installs it, and `reqwest` panics rather
/// than returning an error when it is missing.
fn client() -> reqwest::Client {
    chronixd::tls::ensure_crypto_provider();
    reqwest::Client::new()
}

fn gzip(body: &str) -> Vec<u8> {
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(body.as_bytes()).unwrap();
    e.finish().unwrap()
}

/// How many rows `measurement` holds.
fn rows(db: &Chronix, measurement: &str) -> usize {
    let plan = db
        .query()
        .measurement(measurement)
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    db.execute(&plan).unwrap().num_rows()
}

/// The exact request `[[outputs.influxdb]]` makes: `POST /write?db=…`, with
/// a gzipped body. Neither the route nor the encoding was accepted before,
/// so Telegraf could not write to this server at all.
#[tokio::test]
async fn telegraf_v1_output_writes() {
    let (base, _tmp, db) = server().await;
    let c = client();

    let resp = c
        .post(format!("{base}/write?db=telegraf"))
        .header("Content-Type", "text/plain; charset=utf-8")
        .header("Content-Encoding", "gzip")
        .body(gzip("cpu,host=a usage=1.5 1700000000000000000\n"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NO_CONTENT,
        "body: {}",
        resp.text().await.unwrap()
    );
    assert_eq!(rows(&db, "cpu"), 1);
}

/// `[[outputs.influxdb_v2]]` posts to `/api/v2/write?bucket=…&org=…`.
#[tokio::test]
async fn telegraf_v2_output_writes() {
    let (base, _tmp, db) = server().await;
    let resp = client()
        .post(format!("{base}/api/v2/write?bucket=metrics&org=acme"))
        .header("Content-Type", "text/plain; charset=utf-8")
        .body("mem,host=a used=42i 1700000000000000000\n")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    assert_eq!(rows(&db, "mem"), 1);
}

/// `precision=ms` means the timestamps are milliseconds. Ignoring it put a
/// millisecond client's data in January 1970.
#[tokio::test]
async fn a_precision_parameter_scales_the_timestamps() {
    let (base, _tmp, db) = server().await;
    let c = client();

    // 1700000000000 ms = 2023-11-14T22:13:20Z. Read as nanoseconds it would
    // be 1970-01-01T00:28:20Z.
    let resp = c
        .post(format!("{base}/write?db=x&precision=ms"))
        .body("cpu,host=a usage=1 1700000000000\n")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);

    let plan = db
        .query()
        .measurement("cpu")
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    let ts = batch
        .column_by_name(chronix_core::TIME_COLUMN)
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(
        ts.value(0),
        1_700_000_000_000_000_000,
        "a millisecond timestamp landed in 1970"
    );

    // Seconds too, and an unknown unit is a clear error rather than silence.
    let resp = c
        .post(format!("{base}/write?db=x&precision=s"))
        .body("disk,host=a used=1 1700000000\n")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    let resp = c
        .post(format!("{base}/write?db=x&precision=fortnights"))
        .body("disk,host=a used=1 1\n")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
}

/// One bad line does not discard the good ones, and the response says so in
/// words Telegraf treats as permanent. Failing the whole batch made it retry
/// that batch for ever, so the good lines never landed.
#[tokio::test]
async fn a_bad_line_does_not_discard_the_good_ones() {
    let (base, _tmp, db) = server().await;

    let resp = client()
        .post(format!("{base}/write?db=x"))
        .body(
            "cpu,host=a usage=1 1700000000000000000\n\
             this is not line protocol\n\
             cpu,host=b usage=2 1700000000000000000\n",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await.unwrap();
    let message = body["error"].as_str().unwrap();
    assert!(
        message.starts_with("partial write:"),
        "Telegraf only treats a failure as permanent when it recognises the \
         message; got {message}"
    );
    assert_eq!(body["written"], 2);
    assert_eq!(body["rejected"], 1);

    assert_eq!(rows(&db, "cpu"), 2, "the good lines must have been stored");
}

/// A body that is entirely unparseable is a plain 400 naming the failure.
#[tokio::test]
async fn a_wholly_bad_body_is_a_bad_request() {
    let (base, _tmp, _db) = server().await;
    let resp = client()
        .post(format!("{base}/write?db=x"))
        .body("nonsense\nmore nonsense\n")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body = resp.text().await.unwrap();
    assert!(body.contains("unable to parse"), "got {body}");
}

// ─── Prometheus remote read ───────────────────────────────────────────

use prost::Message;

/// Build a snappy-compressed `ReadRequest` for one query.
fn read_request(matchers: Vec<chronixd::prom_proto::LabelMatcher>) -> Vec<u8> {
    let req = chronixd::prom_proto::ReadRequest {
        queries: vec![chronixd::prom_proto::Query {
            start_timestamp_ms: 0,
            end_timestamp_ms: 4_102_444_800_000,
            matchers,
        }],
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .unwrap()
}

fn matcher(kind: i32, name: &str, value: &str) -> chronixd::prom_proto::LabelMatcher {
    chronixd::prom_proto::LabelMatcher {
        r#type: kind,
        name: name.to_string(),
        value: value.to_string(),
    }
}

/// Read back the series a response holds, as label maps.
async fn read_series(base: &str, body: Vec<u8>) -> Vec<std::collections::BTreeMap<String, String>> {
    let resp = client()
        .post(format!("{base}/api/v1/prom/read"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .body(body)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "status {}", resp.status());
    let bytes = resp.bytes().await.unwrap();
    let raw = snap::raw::Decoder::new().decompress_vec(&bytes).unwrap();
    let read = chronixd::prom_proto::ReadResponse::decode(raw.as_slice()).unwrap();
    read.results
        .into_iter()
        .flat_map(|r| r.timeseries)
        .map(|ts| {
            ts.labels
                .into_iter()
                .map(|l| (l.name, l.value))
                .collect::<std::collections::BTreeMap<_, _>>()
        })
        .collect()
}

/// Remote read applies **every** matcher type. Only `EQ` was collected, so
/// `job!="a"` returned every job and a `__name__` regex was a flat 400 —
/// which is one of the most ordinary queries a remote-read client sends.
#[tokio::test]
async fn remote_read_applies_every_matcher_type() {
    const EQ: i32 = 0;
    const NEQ: i32 = 1;
    const RE: i32 = 2;
    const NRE: i32 = 3;

    let (base, _tmp, db) = server().await;
    for (measurement, job) in [("cpu", "api"), ("cpu", "web"), ("mem", "api")] {
        db.insert(
            &Point::new(
                SeriesKey::new(measurement, [("job".to_string(), job.to_string())].into()).unwrap(),
                [("value".to_string(), FieldValue::F64(1.0))].into(),
                1_700_000_000_000_000_000,
            )
            .unwrap(),
        )
        .unwrap();
    }
    db.flush().unwrap();

    // Baseline: both `cpu` series.
    let series = read_series(&base, read_request(vec![matcher(EQ, "__name__", "cpu")])).await;
    assert_eq!(series.len(), 2);

    // `job != "api"` must exclude it, not be ignored.
    let series = read_series(
        &base,
        read_request(vec![
            matcher(EQ, "__name__", "cpu"),
            matcher(NEQ, "job", "api"),
        ]),
    )
    .await;
    assert_eq!(series.len(), 1, "NEQ was ignored: {series:?}");
    assert_eq!(series[0]["job"], "web");

    // A regex on a label.
    let series = read_series(
        &base,
        read_request(vec![
            matcher(EQ, "__name__", "cpu"),
            matcher(RE, "job", "a.*"),
        ]),
    )
    .await;
    assert_eq!(series.len(), 1);
    assert_eq!(series[0]["job"], "api");

    // A negated regex.
    let series = read_series(
        &base,
        read_request(vec![
            matcher(EQ, "__name__", "cpu"),
            matcher(NRE, "job", "a.*"),
        ]),
    )
    .await;
    assert_eq!(series.len(), 1);
    assert_eq!(series[0]["job"], "web");

    // A `__name__` regex selects measurements — it used to be a 400.
    let series = read_series(
        &base,
        read_request(vec![matcher(RE, "__name__", "cpu|mem")]),
    )
    .await;
    assert_eq!(
        series.len(),
        3,
        "a __name__ regex must select both: {series:?}"
    );

    // The server's own namespace tag is never returned as a label.
    assert!(series.iter().all(|s| !s.contains_key("__namespace__")));
}

/// Remote read addresses **metrics**, not measurements.
///
/// It resolved `__name__` to a measurement and then took "the first non-string
/// column" as the value, so a federating Prometheus asking for `disk_read`
/// got nothing, asking for `disk` got whichever field sorted first under the
/// measurement's name, and the other field did not exist as far as the wire
/// was concerned.
#[tokio::test]
async fn remote_read_reads_one_field_per_metric() {
    const EQ: i32 = 0;
    const RE: i32 = 2;

    let (base, _tmp, db) = server().await;
    db.insert(
        &Point::new(
            SeriesKey::new("disk", [("dev".to_string(), "sda".to_string())].into()).unwrap(),
            [
                ("read".to_string(), FieldValue::F64(7.0)),
                ("write".to_string(), FieldValue::F64(11.0)),
            ]
            .into(),
            1_700_000_000_000_000_000,
        )
        .unwrap(),
    )
    .unwrap();
    db.flush().unwrap();

    // The measurement is not a metric: its fields carry names of their own.
    let series = read_series(&base, read_request(vec![matcher(EQ, "__name__", "disk")])).await;
    assert!(series.is_empty(), "{series:?}");

    let series = read_series(
        &base,
        read_request(vec![matcher(EQ, "__name__", "disk_read")]),
    )
    .await;
    assert_eq!(series.len(), 1, "{series:?}");
    assert_eq!(series[0]["__name__"], "disk_read");

    // A regex matches the metric name, and both fields answer separately.
    let mut names: Vec<String> =
        read_series(&base, read_request(vec![matcher(RE, "__name__", "disk.+")]))
            .await
            .into_iter()
            .map(|s| s["__name__"].clone())
            .collect();
    names.sort();
    assert_eq!(names, ["disk_read", "disk_write"]);
}
