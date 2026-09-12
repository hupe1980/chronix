#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! When a bound cuts the answer short, the client is told.
//!
//! Every read surface here has a ceiling, and not one of them said so. A SQL
//! query whose answer was 6 000 rows returned the first 5 with
//! `row_count: 5`; `/api/v1/series` returned two of three label sets;
//! `/api/v1/label/host/values` returned two of three values. The server logged
//! a `warn!` for each, which is not where the person reading the answer is
//! looking, and an aggregate computed over a truncated scan is not a partial
//! answer — it is a wrong one that no client can distinguish from a right one.
//!
//! Beside that, the `limit` parameter every Prometheus client may send was
//! parsed by nobody: `?limit=1` returned everything. An ignored bound and a
//! silent one are the same defect from opposite sides.
//!
//! So each test drives the real router with a ceiling low enough to bite, and
//! asserts on what the *response* says — never on a log line.

use std::net::SocketAddr;
use std::sync::Arc;

use serde_json::Value;
use tempfile::TempDir;
use tokio::net::TcpListener;

use chronix::prelude::*;
use chronix::Chronix;
use chronixd::config::{ServerConfig, ServerSettings};
use chronixd::http::{AppState, SharedState};
use chronixd::server::build_router;

/// A server holding three series of one measurement, under the given caps.
async fn server(sql_max_rows: usize, prom_series_limit: usize) -> (String, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let cfg = ChronixConfigBuilder::default()
        .data_dir(tmp.path().to_path_buf())
        .build()
        .expect("chronix config");
    let db = Arc::new(Chronix::open(cfg).expect("open db"));

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos() as i64;
    let mut points = Vec::new();
    for i in 0..10i64 {
        for host in ["a", "b", "c"] {
            points.push(
                Point::new(
                    SeriesKey::new("cpu", chronix::tags! { "host" => host }).expect("series key"),
                    chronix::fields! { "value" => i as f64 },
                    now - (10 - i) * 1_000_000_000,
                )
                .expect("point"),
            );
        }
    }
    db.insert_batch(&points)
        .expect("insert")
        .into_complete()
        .expect("all points accepted");
    db.flush().expect("flush");

    let config = ServerConfig {
        server: ServerSettings {
            sql_max_rows,
            prom_series_limit,
            ..Default::default()
        },
        ..Default::default()
    };

    let sql_contexts = chronixd::namespace::SqlContexts::new(db.clone());
    let state: AppState = Arc::new(SharedState {
        db,
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
        config,
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
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (base, tmp)
}

async fn get_json(url: &str) -> Value {
    reqwest::get(url)
        .await
        .expect("request")
        .json()
        .await
        .expect("json")
}

/// A SQL result cut by `sql_max_rows` says so.
#[tokio::test]
async fn a_truncated_sql_result_says_it_is_truncated() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server(5, 10_000).await;
    let c = reqwest::Client::new();

    let body: Value = c
        .post(format!("{base}/api/v1/chronix/sql"))
        .json(&serde_json::json!({"query": "SELECT * FROM cpu"}))
        .send()
        .await
        .expect("request")
        .json()
        .await
        .expect("json");

    assert_eq!(body["row_count"], 5, "the cap still binds");
    assert_eq!(
        body["truncated"], true,
        "a result the server cut short must say so: {body}"
    );

    // …and one that fits says the opposite, so the flag carries information.
    let full: Value = c
        .post(format!("{base}/api/v1/chronix/sql"))
        .json(&serde_json::json!({"query": "SELECT count(*) FROM cpu"}))
        .send()
        .await
        .expect("request")
        .json()
        .await
        .expect("json");
    assert_eq!(full["truncated"], false, "{full}");
}

/// `/api/v1/series` past the server's cardinality cap carries the warning
/// Prometheus uses.
#[tokio::test]
async fn a_truncated_series_list_carries_a_warning() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server(100_000, 2).await;

    let body = get_json(&format!("{base}/api/v1/series?match%5B%5D=cpu")).await;
    assert_eq!(body["data"].as_array().expect("array").len(), 2);
    assert_eq!(
        body["warnings"],
        serde_json::json!(["results truncated due to limit"]),
        "{body}"
    );
}

/// A complete answer carries no `warnings` key at all, as upstream does.
#[tokio::test]
async fn a_complete_answer_has_no_warnings_key() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server(100_000, 10_000).await;

    let body = get_json(&format!("{base}/api/v1/series?match%5B%5D=cpu")).await;
    assert_eq!(body["data"].as_array().expect("array").len(), 3);
    assert!(
        body.get("warnings").is_none(),
        "an untruncated response must not carry an empty warnings array: {body}"
    );
}

/// A truncated list is the sorted **prefix**, at every limit.
///
/// It was whatever the scan met first: the cap broke out of the enumeration
/// and the `BTreeSet` then sorted the survivors, so `limit=1` answered
/// `host="b"` where `limit=2` answered `host="a"`, `host="b"` — not even
/// prefixes of each other, and moving with the data's physical layout. The
/// comment above the enumeration claimed the opposite.
#[tokio::test]
async fn a_truncated_list_is_the_sorted_prefix() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server(100_000, 10_000).await;

    let mut previous: Vec<Value> = Vec::new();
    for n in 1..=3usize {
        let body = get_json(&format!("{base}/api/v1/series?match%5B%5D=cpu&limit={n}")).await;
        let data = body["data"].as_array().expect("array").clone();
        assert_eq!(data.len(), n, "limit={n}: {body}");
        assert!(
            data.starts_with(&previous),
            "limit={n} must extend limit={}: {data:?} does not start with {previous:?}",
            n - 1
        );
        previous = data;
    }
    // …and it is the *sorted* prefix, not merely a stable one.
    assert_eq!(previous[0]["host"], "a");
    assert_eq!(previous[2]["host"], "c");
}

/// The `limit` parameter is applied on every endpoint that accepts it
/// upstream — and was applied on none of them.
#[tokio::test]
async fn the_limit_parameter_is_applied() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server(100_000, 10_000).await;

    for (what, url, expected) in [
        (
            "/series",
            format!("{base}/api/v1/series?match%5B%5D=cpu&limit=1"),
            1,
        ),
        (
            "/labels",
            format!("{base}/api/v1/labels?match%5B%5D=cpu&limit=1"),
            1,
        ),
        (
            "/label/host/values",
            format!("{base}/api/v1/label/host/values?limit=2"),
            2,
        ),
    ] {
        let body = get_json(&url).await;
        assert_eq!(
            body["data"].as_array().expect("array").len(),
            expected,
            "{what} ignored ?limit=: {body}"
        );
        assert_eq!(
            body["warnings"],
            serde_json::json!(["results truncated due to limit"]),
            "{what} applied ?limit= without saying so: {body}"
        );
    }
}

/// `limit=0` means *no* limit, which is Prometheus's rule and the opposite of
/// the obvious reading.
#[tokio::test]
async fn limit_zero_means_unlimited() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server(100_000, 10_000).await;

    let body = get_json(&format!("{base}/api/v1/series?match%5B%5D=cpu&limit=0")).await;
    assert_eq!(body["data"].as_array().expect("array").len(), 3, "{body}");
    assert!(body.get("warnings").is_none(), "{body}");
}

/// A `limit` that is not a non-negative integer is a 400, as upstream's
/// `parseLimitParam` answers.
#[tokio::test]
async fn a_malformed_limit_is_rejected() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server(100_000, 10_000).await;

    let resp = reqwest::get(format!("{base}/api/v1/series?match%5B%5D=cpu&limit=-1"))
        .await
        .expect("request");
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.expect("json");
    assert_eq!(body["errorType"], "bad_data", "{body}");
}

/// `/query` applies `limit` to the number of series, and says so.
#[tokio::test]
async fn an_instant_query_applies_limit_to_series() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server(100_000, 10_000).await;

    let body = get_json(&format!("{base}/api/v1/query?query=cpu&limit=1")).await;
    assert_eq!(
        body["data"]["result"].as_array().expect("array").len(),
        1,
        "{body}"
    );
    assert_eq!(
        body["warnings"],
        serde_json::json!(["results truncated due to limit"]),
        "{body}"
    );
}

/// The structured query endpoint refuses an answer it cannot bound.
///
/// Its response is a bare JSON array with nowhere to put a flag, so it says
/// no rather than returning a prefix. It had no ceiling at all: a
/// `{"measurement":"cpu"}` with no `limit` materialised every row of the
/// measurement into memory and then into JSON.
#[tokio::test]
async fn an_unbounded_structured_query_is_refused() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server(5, 10_000).await;
    let c = reqwest::Client::new();

    let resp = c
        .post(format!("{base}/api/v1/chronix/query"))
        .json(&serde_json::json!({"measurement": "cpu"}))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.expect("json");
    assert!(
        body["error"]
            .as_str()
            .expect("error")
            .contains("sql_max_rows"),
        "the refusal must name the setting and the way out: {body}"
    );

    // An explicit `limit` within the ceiling is the caller saying what they
    // want, and is honoured.
    let rows: Value = c
        .post(format!("{base}/api/v1/chronix/query"))
        .json(&serde_json::json!({"measurement": "cpu", "limit": 3}))
        .send()
        .await
        .expect("request")
        .json()
        .await
        .expect("json");
    assert_eq!(rows.as_array().expect("array").len(), 3);

    // …and a `limit` **above** it is refused before the scan, rather than
    // answered with the ceiling's worth. Silently returning fewer rows than
    // the caller asked for is the defect this endpoint exists not to have.
    let resp = c
        .post(format!("{base}/api/v1/chronix/query"))
        .json(&serde_json::json!({"measurement": "cpu", "limit": 50}))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.expect("json");
    assert!(
        body["error"]
            .as_str()
            .expect("error")
            .contains("sql_max_rows"),
        "{body}"
    );
}

// ── Backfill over the network ───────────────────────────────────────────

/// A point older than the out-of-order window is refused, and the refusal
/// says what to do.
///
/// The message named an internal shard identifier — `write to shard 496833
/// rejected: outside tolerance window [496836..=496840]` — which tells a
/// Telegraf agent neither how late its point was nor that a remedy exists.
#[tokio::test]
async fn a_late_write_is_refused_with_an_actionable_message() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server(100_000, 10_000).await;
    let c = reqwest::Client::new();

    let old = 1_600_000_000_000_000_000i64; // 2020
    let resp = c
        .post(format!("{base}/api/v1/write"))
        .json(&serde_json::json!({
            "measurement": "cpu",
            "tags": {"host": "a"},
            "fields": {"value": 1.0},
            "timestamp": old,
        }))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.expect("json");
    let msg = body["error"].as_str().expect("error");
    assert!(
        msg.contains(&old.to_string()),
        "the refusal must name the timestamp it refused: {msg}"
    );
    assert!(
        msg.contains("backfill"),
        "the refusal must name the way out: {msg}"
    );
}

/// `?backfill=true` writes history, on every protocol that carries points.
///
/// `backfill()` was an embedded-API call with no network surface at all, so
/// the documentation's only remedy for a late write did not exist for anybody
/// running `chronixd`.
#[tokio::test]
async fn backfill_accepts_a_write_outside_the_window() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server(100_000, 10_000).await;
    let c = reqwest::Client::new();

    let old = 1_600_000_000_000_000_000i64;
    let resp = c
        .post(format!("{base}/api/v1/write?backfill=true"))
        .json(&serde_json::json!({
            "measurement": "cpu",
            "tags": {"host": "a"},
            "fields": {"value": 7.0},
            "timestamp": old,
        }))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 204, "backfill must accept a historic write");

    // And it is readable, which is the half a 204 does not prove.
    let rows: Value = c
        .post(format!("{base}/api/v1/chronix/query"))
        .json(&serde_json::json!({
            "measurement": "cpu",
            "range": {"start": old - 1, "end": old + 1},
        }))
        .send()
        .await
        .expect("request")
        .json()
        .await
        .expect("json");
    let rows = rows.as_array().expect("array");
    assert_eq!(
        rows.len(),
        1,
        "the backfilled point must be queryable: {rows:?}"
    );
    assert_eq!(rows[0]["fields"]["value"], 7.0);
}

/// Line protocol carries the flag too — this is Telegraf's path.
#[tokio::test]
async fn line_protocol_accepts_a_backfill() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server(100_000, 10_000).await;
    let c = reqwest::Client::new();

    let old = 1_600_000_000_000_000_000i64;
    let resp = c
        .post(format!("{base}/write?backfill=true"))
        .body(format!("cpu,host=z value=3 {old}"))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 204, "{:?}", resp.text().await);
}

/// A malformed write body names the field, the position and the mistake.
///
/// `#[serde(untagged)]` discarded the variants' errors, so one mistyped key in
/// a batch of fifty thousand answered `data did not match any variant of
/// untagged enum WriteBody`.
#[tokio::test]
async fn a_malformed_write_body_names_what_is_wrong() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server(100_000, 10_000).await;
    let c = reqwest::Client::new();

    let resp = c
        .post(format!("{base}/api/v1/write"))
        .json(&serde_json::json!([
            {"measurement": "cpu", "fields": {"value": 1.0}},
            {"measurment": "cpu", "fields": {"value": 1.0}},
        ]))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.expect("json");
    let msg = body["error"].as_str().expect("error");
    assert!(
        msg.contains("measurment"),
        "the error must name the offending field: {msg}"
    );
    assert!(
        msg.contains('1'),
        "the error must locate it in the batch: {msg}"
    );
}

/// A query parameter chronix does not read must not fail a write.
///
/// The `?backfill=` extractor is deliberately lenient about *unknown* keys:
/// a query string is decorated by things that are not the sender — a proxy,
/// an agent appending its own label — and a remote-write sender treats a
/// `400` as permanent, so rejecting one would drop the batch for ever.
#[tokio::test]
async fn an_unknown_query_parameter_does_not_fail_a_write() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server(100_000, 10_000).await;
    let c = reqwest::Client::new();

    let resp = c
        .post(format!("{base}/api/v1/write?extra_label=team%3Dcore"))
        .json(&serde_json::json!({
            "measurement": "cpu", "tags": {"host": "a"}, "fields": {"value": 1.0}
        }))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 204, "{:?}", resp.text().await);
}

/// …but a `backfill` value that is not a boolean is a 400, not a silent
/// `false`. That is the mistake somebody actually makes.
#[tokio::test]
async fn a_non_boolean_backfill_value_is_refused() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server(100_000, 10_000).await;
    let c = reqwest::Client::new();

    let resp = c
        .post(format!("{base}/api/v1/write?backfill=yes"))
        .json(&serde_json::json!({
            "measurement": "cpu", "tags": {"host": "a"}, "fields": {"value": 1.0}
        }))
        .send()
        .await
        .expect("request");
    assert!(
        resp.status().is_client_error(),
        "?backfill=yes must not be read as false: {}",
        resp.status()
    );
}

// ── Readiness ───────────────────────────────────────────────────────────

/// `/ready` answers on **writability**, not on whether a read succeeds.
///
/// It called `measurement_count()` — an in-memory registry lookup that
/// succeeds while every write is being refused — under a doc comment already
/// claiming it checked that the database was "accepting writes". A Kubernetes
/// deployment kept routing traffic to a node that could not store a point.
#[tokio::test]
async fn ready_reports_writability_and_says_why_not() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server(100_000, 10_000).await;

    let body = get_json(&format!("{base}/ready")).await;
    assert_eq!(body["ready"], true, "{body}");

    // The unready answer carries a reason, so an operator reading a probe
    // failure learns something. `/health` stays up either way: the process is
    // alive and can still serve reads.
    let health = get_json(&format!("{base}/health")).await;
    assert_eq!(health["status"], "ok");
}
