#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! When chronix stops, what is the caller told — and what keeps running?
//!
//! Two halves of one hole, and both were live in a tree whose Critical list
//! was empty.
//!
//! **The engine knows more than the client is told.** `DbError` distinguishes
//! a transient overload (a full memtable, resolved by the flush already
//! signalled), a persistent one (a WAL poisoned by a failed `fsync`, which
//! needs an operator), a query that ran out of time, and a schema mistake.
//! The server's mapping had four arms and a `_ => 500 DATABASE_ERROR: an
//! internal error occurred`, so the three conditions an operator most needs to
//! tell apart — *back off*, *come and look*, *your query is too big* — were
//! one opaque 500. The `503 BACKPRESSURE` the API reference documents was
//! produced by nothing.
//!
//! **Nothing that is stopped actually stops.** A write deadline was a
//! `tokio::time::timeout` around a `JoinHandle` from `spawn_blocking`.
//! Dropping that handle cancels nothing — a blocking task cannot be
//! cancelled — so the write completed and the caller was told `504`, under
//! documentation reading *"the write did not complete within
//! `write_timeout`"*. That is the pass-42 defect (a record the caller was
//! told had failed was written anyway) one level up, in the server.
//!
//! Each test drives the real router and asserts on the **response**, never on
//! the log line the server was already writing.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tempfile::TempDir;
use tokio::net::TcpListener;

use chronix::prelude::*;
use chronix::Chronix;
use chronixd::config::{DatabaseConfig, ServerConfig, ServerSettings};
use chronixd::http::{AppState, SharedState};
use chronixd::server::build_router;

/// A running server over a fresh database, with the knobs a test needs.
struct Harness {
    base: String,
    db: Arc<Chronix>,
    _tmp: TempDir,
}

async fn harness(
    tune_db: impl FnOnce(&mut ChronixConfigBuilder),
    tune_server: impl FnOnce(&mut ServerSettings),
    write_timeout: Duration,
) -> Harness {
    let tmp = TempDir::new().expect("tempdir");
    let mut builder = ChronixConfig::builder();
    builder = builder.data_dir(tmp.path().to_path_buf());
    tune_db(&mut builder);
    let cfg = builder.build().expect("chronix config");
    let db = Arc::new(Chronix::open(cfg).expect("open db"));

    let mut server = ServerSettings::default();
    tune_server(&mut server);
    let config = ServerConfig {
        server,
        database: DatabaseConfig {
            data_dir: tmp.path().to_path_buf(),
            ..Default::default()
        },
        ..Default::default()
    };

    let sql_contexts = chronixd::namespace::SqlContexts::new(db.clone());
    let state: AppState = Arc::new(SharedState {
        db: db.clone(),
        start_time: std::time::Instant::now(),
        connector_manager: None,
        sql_contexts,
        auth_state: None,
        #[cfg(feature = "cluster")]
        meta_client: None,
        namespace_registry: None,
        model_catalog: Arc::new(parking_lot::RwLock::new(
            chronix::chronix_analytics::forecast::ModelCatalog::new(),
        )),
        authz_engine: None,
        audit_logger: None,
        config,
        namespace_rate_limiter: chronixd::rate_limit::NamespaceRateLimiter::new(),
        sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
        write_dedup_cache: chronixd::http::WriteDedupCache::new(60, 1024),
        write_timeout,
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
    tokio::time::sleep(Duration::from_millis(50)).await;
    Harness {
        base,
        db,
        _tmp: tmp,
    }
}

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("epoch nanos fit i64")
}

/// A JSON write body of `count` points, each its own series.
fn write_body(measurement: &str, start_series: usize, count: usize) -> Value {
    let now = now_ns();
    let points: Vec<Value> = (0..count)
        .map(|i| {
            serde_json::json!({
                "measurement": measurement,
                "tags": { "host": format!("h{}", start_series + i) },
                "fields": { "value": 1.5 },
                "timestamp": now + i as i64,
            })
        })
        .collect();
    Value::Array(points)
}

// ── The engine knows; the client is told ───────────────────────────────

/// A full memtable is back-pressure, and the client is told to back off.
///
/// This is the ordinary condition on a gateway whose flash is slower than its
/// ingest — the engine names it `TransientOverload`, logs *"retry after
/// backoff"*, and the client received
/// `500 {"error":"DATABASE_ERROR: an internal error occurred"}`. A `500` is
/// what a client reports as *our* bug; a `503` is what it retries.
#[tokio::test]
async fn a_full_memtable_is_a_503_that_says_to_retry() {
    chronixd::tls::ensure_crypto_provider();
    // Small enough that a few hundred series fill it, and equal to the flush
    // threshold so the flush that resolves it is the one already signalled.
    let h = harness(
        |b| {
            *b = std::mem::take(b)
                .max_memtable_memory(128 * 1024)
                .memtable_flush_threshold(128 * 1024);
        },
        |_| {},
        Duration::ZERO,
    )
    .await;

    let client = reqwest::Client::new();
    let mut saw = None;
    for round in 0..40 {
        let resp = client
            .post(format!("{}/api/v1/write", h.base))
            .json(&write_body("cpu", round * 500, 500))
            .send()
            .await
            .expect("request");
        if resp.status() != reqwest::StatusCode::NO_CONTENT {
            saw = Some((resp.status(), resp.json::<Value>().await.expect("json")));
            break;
        }
    }

    let (status, body) = saw.expect("the memtable must fill within 20 000 points");
    assert_eq!(
        status,
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "a full memtable is back-pressure, not a server fault: {body}"
    );
    assert_eq!(body["code"], "BACKPRESSURE", "{body}");
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("memtable"),
        "the reason is safe to show and is what an operator acts on: {body}"
    );
    assert!(
        !body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("an internal error occurred"),
        "a condition the engine names must not be redacted: {body}"
    );
}

/// A `503` from back-pressure carries `Retry-After`.
///
/// The engine has already signalled the flush that resolves it, so the wait
/// is short and knowable. Without the header every client invents its own.
#[tokio::test]
async fn back_pressure_says_how_long_to_wait() {
    chronixd::tls::ensure_crypto_provider();
    let h = harness(
        |b| {
            *b = std::mem::take(b)
                .max_memtable_memory(128 * 1024)
                .memtable_flush_threshold(128 * 1024);
        },
        |_| {},
        Duration::ZERO,
    )
    .await;

    let client = reqwest::Client::new();
    for round in 0..40 {
        let resp = client
            .post(format!("{}/api/v1/write", h.base))
            .json(&write_body("cpu", round * 500, 500))
            .send()
            .await
            .expect("request");
        if resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            assert!(
                resp.headers().contains_key("retry-after"),
                "a 503 a client should retry says when: {:?}",
                resp.headers()
            );
            return;
        }
    }
    panic!("the memtable must fill within 20 000 points");
}

/// A query that runs out of time is a `504`, and says which setting bound it.
///
/// `DbError::QueryTimeout` fell into the `_ => 500 DATABASE_ERROR` arm, so
/// the one person who can act on it — the one who wrote the query — was told
/// only that something happened.
#[tokio::test]
async fn a_query_timeout_is_a_504_that_names_itself() {
    chronixd::tls::ensure_crypto_provider();
    // A deadline of one nanosecond expires before the first batch.
    let h = harness(
        |b| *b = std::mem::take(b).query_timeout(Duration::from_nanos(1)),
        |_| {},
        Duration::ZERO,
    )
    .await;

    let now = now_ns();
    let points: Vec<Point> = (0..200)
        .map(|i| {
            Point::new(
                SeriesKey::new("cpu", chronix::tags! { "host" => "a" }).expect("key"),
                chronix::fields! { "value" => f64::from(i) },
                now - i64::from(200 - i) * 1_000_000,
            )
            .expect("point")
        })
        .collect();
    h.db.insert_batch(&points)
        .expect("insert")
        .into_complete()
        .expect("all points accepted");

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/v1/chronix/query", h.base))
        .json(&serde_json::json!({ "measurement": "cpu" }))
        .send()
        .await
        .expect("request");
    let status = resp.status();
    let body: Value = resp.json().await.expect("json");

    assert_eq!(
        status,
        reqwest::StatusCode::GATEWAY_TIMEOUT,
        "a query that ran out of time is not an internal error: {body}"
    );
    assert_eq!(body["code"], "QUERY_TIMEOUT", "{body}");
    assert!(
        !body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("an internal error occurred"),
        "the caller can act on this one: {body}"
    );
}

/// The SQL endpoint's own deadline answers like every other surface's.
///
/// Flight SQL and gRPC SQL answer `DEADLINE_EXCEEDED` and PromQL answers
/// `errorType: "timeout"`; the REST SQL endpoint raised
/// `ServerError::Internal`, which is redacted to
/// `INTERNAL_ERROR: an internal error occurred`. One promise, four surfaces,
/// one of them disagreeing.
// A **multi-threaded** runtime on purpose. `#[tokio::test]` is
// `current_thread`, and a CPU-bound DataFusion plan holds that one thread for
// the whole query — so the timer never fires and the deadline appears not to
// work when it does. The first version of this test read a 160-second `200`
// and looked exactly like a product defect.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_sql_endpoint_reports_its_deadline_like_its_siblings() {
    chronixd::tls::ensure_crypto_provider();
    let h = harness(
        |_| {},
        |s| {
            // One second, and a statement that cannot finish inside it.
            s.sql_query_timeout_secs = 1;
        },
        Duration::ZERO,
    )
    .await;

    let now = now_ns();
    let points: Vec<Point> = (0..6_000)
        .map(|i| {
            Point::new(
                SeriesKey::new("cpu", chronix::tags! { "host" => "a" }).expect("key"),
                chronix::fields! { "value" => f64::from(i) },
                now - i64::from(6_000 - i) * 1_000_000,
            )
            .expect("point")
        })
        .collect();
    h.db.insert_batch(&points)
        .expect("insert")
        .into_complete()
        .expect("all points accepted");

    // A self-join with an inequality is quadratic: 6 000 rows is 1.8·10⁷ pairs,
    // comfortably past a one-second budget in a debug build and small enough
    // that the abandoned work finishes soon after the test's assertions —
    // dropping the stream stops the *next* poll, not the one in progress.
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/v1/chronix/sql", h.base))
        .json(&serde_json::json!({
            "query": "SELECT count(*) FROM cpu a JOIN cpu b ON a.value < b.value"
        }))
        .send()
        .await
        .expect("request");
    let status = resp.status();
    let body: Value = resp.json().await.expect("json");

    assert_eq!(
        status,
        reqwest::StatusCode::GATEWAY_TIMEOUT,
        "a SQL deadline is a timeout, not an internal error: {body}"
    );
    assert_eq!(body["code"], "QUERY_TIMEOUT", "{body}");
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("sql_query_timeout_secs"),
        "the message names the setting that bound it: {body}"
    );
}

// ── Nothing that is stopped actually stops ─────────────────────────────

/// A write that outruns its deadline does not claim it did not happen.
///
/// `tokio::time::timeout` around a `spawn_blocking` `JoinHandle` cancels the
/// *wait*, never the task: the write lands. The API reference said *"the
/// write did not complete within `write_timeout`"*, which is the one thing
/// the server does not know.
/// How a write is made to outrun its deadline, deterministically.
///
/// **Not by shrinking the deadline.** `tokio::time::timeout` polls the inner
/// future *first*, so a `spawn_blocking` write that finishes before that first
/// poll returns `Ok` and never times out, however small the budget. A
/// one-nanosecond deadline therefore raced — and raced differently on two CI
/// runners than on the development machine: one job saw `204 No Content` (no
/// body to parse), another saw the idempotency key committed, and both passed
/// locally.
///
/// So the *write* is made slow rather than the deadline small, and slow in
/// **CPU** rather than in I/O: an fsync is milliseconds on one filesystem and
/// microseconds on another, while parsing and inserting ten thousand points
/// costs the same order everywhere. Measured at ~200 ms in a debug build
/// against a 1 ms budget. For this to race, a machine would have to ingest ten
/// million points a second through JSON — sixteen times the *release* build's
/// measured single-writer rate.
const SLOW_WRITE_POINTS: usize = 10_000;

/// A budget no write of [`SLOW_WRITE_POINTS`] can meet.
const TIGHT_WRITE_TIMEOUT: Duration = Duration::from_millis(1);

#[tokio::test]
async fn a_write_that_outruns_its_deadline_says_the_outcome_is_unknown() {
    chronixd::tls::ensure_crypto_provider();
    let h = harness(|_| {}, |_| {}, TIGHT_WRITE_TIMEOUT).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/v1/write", h.base))
        .json(&write_body("late", 0, SLOW_WRITE_POINTS))
        .send()
        .await
        .expect("request");
    let status = resp.status();
    assert_eq!(
        status,
        reqwest::StatusCode::GATEWAY_TIMEOUT,
        "the premise: a {SLOW_WRITE_POINTS}-point write must not fit in \
         {TIGHT_WRITE_TIMEOUT:?}"
    );
    let body: Value = resp.json().await.expect("json");

    assert_eq!(body["code"], "WRITE_TIMEOUT", "{body}");
    let message = body["error"].as_str().unwrap_or_default();
    assert!(
        message.contains("may still"),
        "the server cannot cancel a blocking write, so it must not claim the \
         write did not happen: {body}"
    );

    // And it did happen — which is exactly why the message must not deny it.
    let mut landed = false;
    for _ in 0..100 {
        if h.db.schema("late").is_some() {
            landed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        landed,
        "the point the caller was told about must in fact have been written — \
         if this ever stops being true the message above can be tightened"
    );
}

/// The idempotency key of a write with an unknown outcome is still free.
///
/// A retry has to be possible, because the caller cannot tell whether the
/// first attempt landed. A point is identified by its series and timestamp,
/// so re-sending it is a no-op — which is what makes releasing the claim the
/// safe choice, and worth pinning.
///
/// The premise is asserted, not assumed: a write that *succeeds* commits its
/// key, and a `409` on the retry would then be correct. See
/// [`SLOW_WRITE_POINTS`] for why the timeout is deterministic.
#[tokio::test]
async fn a_timed_out_write_leaves_its_idempotency_key_reusable() {
    chronixd::tls::ensure_crypto_provider();
    let h = harness(|_| {}, |_| {}, TIGHT_WRITE_TIMEOUT).await;

    let client = reqwest::Client::new();
    let body = write_body("dedup", 0, SLOW_WRITE_POINTS);
    for attempt in 0..2 {
        let resp = client
            .post(format!("{}/api/v1/write", h.base))
            .header("idempotency-key", "k1")
            .json(&body)
            .send()
            .await
            .expect("request");
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::GATEWAY_TIMEOUT,
            "attempt {attempt}: the outcome must be unknown for this test to \
             say anything — a write that succeeded would commit its key, and a \
             409 on the retry would then be right"
        );
    }
}
