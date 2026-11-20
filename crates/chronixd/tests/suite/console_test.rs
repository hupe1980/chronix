//! The console at `/console`: what it serves, to whom, and what it may call.
//!
//! The page itself runs in a browser and is smoke-tested there in CI. What
//! these tests hold is the server's half and the page's contract: the shell is
//! the same bytes for every caller and carries no data, it is served under a
//! strict policy, it only calls routes other clients use, it stores nothing
//! but the theme, it never builds markup from strings, and the code it offers
//! to copy is code that compiles and requests that return the same rows.

use std::collections::BTreeSet;
use std::io::Read;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use chronix::Chronix;
use chronix::prelude::*;
use chronixd::config::ServerConfig;
use chronixd::console;
use chronixd::demo;
use chronixd::http::SharedState;
use serde_json::Value;

const KEY_A: &str = "console-key-a-0123456789";
const KEY_B: &str = "console-key-b-0123456789";

fn spawn(state: chronixd::http::AppState, auth: Option<chronixd::auth::AuthState>) -> SocketAddr {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let handle = chronixd::server::prometheus_builder()
                .unwrap()
                .build_recorder()
                .handle();
            let app =
                chronixd::server::build_router(state, "/metrics", handle, 10 * 1024 * 1024, auth);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            tx.send(listener.local_addr().unwrap()).unwrap();
            axum::serve(listener, app).await.unwrap();
        });
    });
    rx.recv().unwrap()
}

fn demo_db(dir: &Path) -> Arc<Chronix> {
    let db = Chronix::open(
        ChronixConfig::builder()
            .data_dir(dir.to_path_buf())
            .build()
            .unwrap(),
    )
    .unwrap();
    let now = i64::try_from(jiff::Timestamp::now().as_nanosecond()).unwrap();
    demo::ingest(&db, demo::window_end(now)).unwrap();
    Arc::new(db)
}

/// The demo week behind an unauthenticated server, once per binary.
fn demo_server() -> SocketAddr {
    static SERVER: OnceLock<(tempfile::TempDir, SocketAddr)> = OnceLock::new();
    SERVER
        .get_or_init(|| {
            let dir = tempfile::tempdir().unwrap();
            let db = demo_db(&dir.path().join("data"));
            let addr = spawn(SharedState::standalone(db, ServerConfig::default()), None);
            (dir, addr)
        })
        .1
}

/// A server that requires a key, with two keys bound to two namespaces.
fn authenticated_server(console_enabled: bool) -> (tempfile::TempDir, SocketAddr) {
    let dir = tempfile::tempdir().unwrap();
    let db = demo_db(&dir.path().join("data"));
    let mut config = ServerConfig::default();
    config.console.enabled = console_enabled;
    let auth_config = chronixd::config::AuthConfig {
        api_keys: vec![
            chronixd::config::ApiKeyEntry {
                name: "a".into(),
                key: KEY_A.into(),
                namespaces: vec!["default".into()],
                admin: false,
                roles: Vec::new(),
            },
            chronixd::config::ApiKeyEntry {
                name: "b".into(),
                key: KEY_B.into(),
                namespaces: vec!["other".into()],
                admin: false,
                roles: Vec::new(),
            },
        ],
        jwt: None,
        exempt_paths: vec!["/health".into()],
    };
    let auth = chronixd::auth::AuthState::from_config(&auth_config).unwrap();
    let state = SharedState::standalone(db, config);
    let mut inner = Arc::try_unwrap(state).ok().expect("fresh state");
    inner.auth_state = Some(auth.clone());
    let addr = spawn(Arc::new(inner), Some(auth));
    (dir, addr)
}

fn http() -> reqwest::Client {
    chronixd::tls::ensure_crypto_provider();
    reqwest::Client::builder()
        .no_gzip()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

fn asset(name: &str) -> &'static str {
    std::str::from_utf8(console::asset_bytes(name).expect(name)).unwrap()
}

#[tokio::test]
async fn console_shell_is_served_with_csp() {
    let base = format!("http://{}", demo_server());
    let resp = http().get(format!("{base}/console")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let h = resp.headers().clone();
    assert_eq!(h["content-security-policy"], console::CSP);
    assert_eq!(h["x-content-type-options"], "nosniff");
    assert!(h["content-type"].to_str().unwrap().starts_with("text/html"));
    let etag = h["etag"].to_str().unwrap().to_owned();
    assert_eq!(
        resp.bytes().await.unwrap().as_ref(),
        console::asset_bytes("index.html").unwrap()
    );

    // Unchanged: 304, no body.
    let resp = http()
        .get(format!("{base}/console"))
        .header("if-none-match", &etag)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 304);

    // Compressed when asked, and the bytes are the same once inflated.
    for name in console::asset_names() {
        let resp = http()
            .get(format!("{base}/console/{name}"))
            .header("accept-encoding", "gzip")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "{name}");
        assert_eq!(resp.headers()["content-encoding"], "gzip", "{name}");
        assert_eq!(resp.headers()["content-security-policy"], console::CSP);
        let body = resp.bytes().await.unwrap();
        let mut out = Vec::new();
        flate2::read::GzDecoder::new(body.as_ref())
            .read_to_end(&mut out)
            .unwrap();
        assert_eq!(out, console::asset_bytes(name).unwrap(), "{name}");
    }

    // The trailing-slash form redirects, so relative asset paths resolve.
    let resp = http().get(format!("{base}/console/")).send().await.unwrap();
    assert_eq!(resp.status(), 308);
    assert_eq!(resp.headers()["location"], "../console");
}

#[tokio::test]
async fn unknown_console_asset_is_404() {
    let base = format!("http://{}", demo_server());
    for path in [
        "/console/nope.js",
        "/console/..%2Fsecret",
        "/console/app.js.map",
    ] {
        let resp = http().get(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(resp.status(), 404, "{path}");
    }
}

/// The shell loads without a key, and is the same bytes whoever asks; the
/// requests the page makes are not exempt.
#[tokio::test]
async fn console_shell_is_data_free_and_identical_for_every_caller() {
    let (_dir, addr) = authenticated_server(true);
    let base = format!("http://{addr}");
    let mut bodies = BTreeSet::new();
    for (key, ns) in [
        (None, None),
        (Some(KEY_A), Some("default")),
        (Some(KEY_B), Some("other")),
    ] {
        for path in ["/console", "/console/app.js"] {
            let mut req = http().get(format!("{base}{path}"));
            if let Some(k) = key {
                req = req.bearer_auth(k);
            }
            if let Some(n) = ns {
                req = req.header("x-namespace", n);
            }
            let resp = req.send().await.unwrap();
            assert_eq!(resp.status(), 200, "{path} with {key:?}");
            bodies.insert((path, resp.bytes().await.unwrap().to_vec()));
        }
    }
    assert_eq!(bodies.len(), 2, "every caller gets the same two files");

    // What the page calls is gated as usual.
    let resp = http()
        .get(format!("{base}/api/v1/label/__name__/values"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    // A write cannot ride the exemption.
    let resp = http().post(format!("{base}/console")).send().await.unwrap();
    assert!(
        resp.status() == 401 || resp.status() == 405,
        "{}",
        resp.status()
    );
}

#[tokio::test]
async fn console_disabled_serves_nothing() {
    let (_dir, addr) = authenticated_server(false);
    let resp = http()
        .get(format!("http://{addr}/console"))
        .bearer_auth(KEY_A)
        .send()
        .await
        .unwrap();
    // Refused before routing (403) or not routed (404) — either way, no page.
    let status = resp.status();
    assert!(status == 403 || status == 404, "{status}");
    let body = resp.text().await.unwrap();
    assert!(
        !body.contains("<html"),
        "the shell was served with the console off"
    );
}

/// Everything the page fetches is a route another client also uses.
#[test]
fn every_route_the_page_calls_is_in_the_contract() {
    const CONTRACT: &[&str] = &[
        "api/v1/query",
        "api/v1/query_range",
        "api/v1/labels",
        "api/v1/label/__name__/values",
        "api/v1/label/",
        "api/v1/chronix/sql",
        "api/v1/measurements",
        "api/v1/status/tsdb",
        "api/v1/chronix/promql/explain",
    ];
    let js = asset("app.js");
    let mut called = BTreeSet::new();
    for (i, _) in js.match_indices("api/v1/") {
        let tail: String = js[i..]
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '_'))
            .collect();
        called.insert(tail);
    }
    assert!(called.len() >= 7, "the scan is broken: {called:?}");
    for route in &called {
        assert!(
            CONTRACT.contains(&route.as_str()),
            "the page calls {route}, which is not in the contract"
        );
    }
    // The one route outside `/api/v1`: the MCP endpoint agents use.
    assert!(js.contains(r#"api("mcp""#));
}

/// No HTML is ever parsed from a string, and no code is built from one.
#[test]
fn the_page_never_builds_markup_from_strings() {
    let js = asset("app.js");
    for sink in [
        "innerHTML",
        "outerHTML",
        "insertAdjacentHTML",
        "document.write",
        "eval(",
        "new Function",
        "setAttribute(\"on",
    ] {
        assert!(!js.contains(sink), "app.js uses {sink}");
    }
    // Inline handlers in the shell would be blocked by the policy anyway.
    let html = asset("index.html");
    assert!(!html.contains(" onclick=") && !html.contains("<script>") && !html.contains("style="));
}

/// The only stored value is the theme; the key lives in memory.
#[test]
fn the_page_stores_nothing_but_the_theme() {
    let js = asset("app.js");
    assert!(
        !js.contains("sessionStorage")
            && !js.contains("document.cookie")
            && !js.contains("indexedDB")
    );
    for (i, _) in js.match_indices("localStorage.") {
        let call = &js[i..i + 40.min(js.len() - i)];
        assert!(
            call.contains("\"chronix.theme\""),
            "localStorage used for something else: {call}"
        );
    }
}

/// The whole console, compressed, stays small.
#[test]
fn the_console_fits_its_budget() {
    let mut total = 0;
    for name in console::asset_names() {
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        std::io::Write::write_all(&mut enc, console::asset_bytes(name).unwrap()).unwrap();
        total += enc.finish().unwrap().len();
    }
    assert!(
        total <= 300 * 1024,
        "the console is {total} bytes compressed"
    );
}

/// Render a template the way the page does: `{{name}}` from `vars`.
fn render(template: &str, vars: &[(&str, &str)]) -> String {
    let mut out = template.to_owned();
    for (k, v) in vars {
        out = out.replace(&format!("{{{{{k}}}}}"), v);
    }
    assert!(!out.contains("{{"), "unrendered placeholder in {out}");
    out
}

fn snippets() -> Value {
    serde_json::from_str(asset("snippets.json")).unwrap()
}

/// The Rust the page offers is a program that compiles: it is checked in as
/// an example, which `cargo test` builds.
#[test]
fn copy_as_rust_matches_a_compiled_example() {
    let t = snippets()["rust"]["promql"].as_str().unwrap().to_owned();
    let rendered = render(
        &t,
        &[
            ("query_rust", "\"pv_power_w\""),
            (
                "range",
                "2026-11-17T01:00:00Z to 2026-11-17T07:00:00Z, one point every 30s",
            ),
            ("start_ns", "1_794_877_200_000_000_000"),
            ("end_ns", "1_794_898_800_000_000_000"),
            ("step_ns", "30_000_000_000"),
        ],
    );
    let example = include_str!("../../examples/console_snippet.rs");
    assert_eq!(
        rendered, example,
        "the Rust template and examples/console_snippet.rs differ"
    );
    let sql = snippets()["rust"]["sql"].as_str().unwrap().to_owned();
    assert!(render(&sql, &[("query_rust", "\"SELECT 1\"")]).contains("db.sql(\"SELECT 1\")?"));
}

/// Undo a single-quoted shell word.
fn unquote(word: &str) -> String {
    let inner = word
        .strip_prefix('\'')
        .and_then(|w| w.strip_suffix('\''))
        .expect("quoted");
    inner.replace("'\\''", "'")
}
fn shq(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// The curl the page offers sends the request the page sent: same rows.
#[tokio::test]
async fn copy_as_curl_returns_the_same_rows() {
    let base = format!("http://{}", demo_server());
    let query = "SELECT device, avg(power_w) AS avg_w FROM pv GROUP BY device ORDER BY device";
    let body = serde_json::json!({ "query": query }).to_string();
    let curl = render(
        snippets()["curl"]["sql"].as_str().unwrap(),
        &[
            ("base_url", &base),
            ("auth_curl", ""),
            ("sql_body_shell", &shq(&body)),
        ],
    );
    let url = curl.split_whitespace().nth(2).unwrap().to_owned();
    let data = curl.split("-d ").nth(1).unwrap().trim();
    assert_eq!(url, format!("{base}/api/v1/chronix/sql"));
    let from_curl: Value = http()
        .post(&url)
        .header("content-type", "application/json")
        .body(unquote(data))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let from_page: Value = http()
        .post(format!("{base}/api/v1/chronix/sql"))
        .json(&serde_json::json!({ "query": query }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(from_page["rows"].as_array().is_some_and(|r| !r.is_empty()));
    assert_eq!(from_curl["rows"], from_page["rows"]);

    // A query with a quote in it survives the shell quoting.
    let tricky = "SELECT 'it''s' AS s";
    let body = serde_json::json!({ "query": tricky }).to_string();
    let curl = render(
        snippets()["curl"]["sql"].as_str().unwrap(),
        &[
            ("base_url", &base),
            ("auth_curl", ""),
            ("sql_body_shell", &shq(&body)),
        ],
    );
    let data = curl.split("-d ").nth(1).unwrap().trim();
    let v: Value = serde_json::from_str(&unquote(data)).unwrap();
    assert_eq!(v["query"], tricky);
}
