//! The MCP server, driven by the official SDK's own client.
//!
//! The server's side of the protocol is rmcp's; what these tests hold is
//! chronix's side: every tool answers on real data, every tool only reads,
//! results are bounded and say so, errors carry the same hint the HTTP API
//! gives, `/mcp` is scoped and gated like every route, and stdio mode answers
//! from a directory without changing it.

#![cfg(feature = "mcp")]
#![allow(clippy::panic)] // test code may panic

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use rmcp::model::{CallToolRequestParams, CallToolResult, ClientConfig, ProtocolVersion};
use rmcp::service::{RoleClient, RunningService};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::{ClientHandler, ServiceExt};
use serde_json::{Value, json};

use chronix::Chronix;
use chronix::prelude::*;
use chronixd::config::ServerConfig;
use chronixd::demo;
use chronixd::http::SharedState;

/// A client that asks for one protocol revision.
#[derive(Clone)]
struct Pinned(ProtocolVersion);

impl ClientHandler for Pinned {
    fn get_info(&self) -> ClientConfig {
        let mut info = ClientConfig::default();
        info.protocol_version = self.0.clone();
        info
    }
}

type Client = RunningService<RoleClient, Pinned>;

/// A server over `state`, on its own runtime for the life of the test binary.
fn spawn(state: chronixd::http::AppState) -> SocketAddr {
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
                chronixd::server::build_router(state, "/metrics", handle, 10 * 1024 * 1024, None);
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
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    demo::ingest(&db, demo::window_end(now)).unwrap();
    Arc::new(db)
}

/// The demo week, served once for the whole binary.
///
/// Every test in this binary shares it, in parallel, so it gets more query
/// slots than the tests can hold at once: the default (cores − 1) is 2 on a
/// 3-core CI runner, where a queue of debug-build forecasts outlasts the 30 s
/// slot wait. The cap itself is tested by `one_query_slot_serves_concurrent_tools`.
fn demo_server() -> SocketAddr {
    static SERVER: OnceLock<(tempfile::TempDir, SocketAddr)> = OnceLock::new();
    SERVER
        .get_or_init(|| {
            let dir = tempfile::tempdir().unwrap();
            let db = demo_db(&dir.path().join("data"));
            let mut config = ServerConfig::default();
            config.server.max_concurrent_queries = 64;
            let addr = spawn(SharedState::standalone(db, config));
            (dir, addr)
        })
        .1
}

async fn connect(addr: SocketAddr, version: ProtocolVersion, headers: &[(&str, &str)]) -> Client {
    chronixd::tls::ensure_crypto_provider();
    let mut config = StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}/mcp"));
    for (k, v) in headers {
        config
            .custom_headers
            .insert(k.parse().unwrap(), v.parse().unwrap());
    }
    let transport = StreamableHttpClientTransport::from_config(config);
    // 2026-07-28 has no `initialize`: a client discovers and then sends each
    // request self-contained. Older revisions shake hands.
    if version == ProtocolVersion::V_2026_07_28 {
        rmcp::serve_client_with_lifecycle(
            Pinned(version.clone()),
            transport,
            rmcp::ClientLifecycleMode::Discover {
                preferred_versions: vec![version],
            },
        )
        .await
        .unwrap()
    } else {
        Pinned(version).serve(transport).await.unwrap()
    }
}

async fn client() -> Client {
    connect(demo_server(), ProtocolVersion::LATEST, &[]).await
}

async fn call(client: &Client, tool: &str, args: Value) -> CallToolResult {
    let Value::Object(args) = args else {
        panic!("arguments must be an object")
    };
    client
        .call_tool(CallToolRequestParams::new(tool.to_owned()).with_arguments(args))
        .await
        .unwrap_or_else(|e| panic!("{tool}: {e}"))
}

fn ok(result: &CallToolResult, tool: &str) -> Value {
    assert_ne!(
        result.is_error,
        Some(true),
        "{tool} failed: {:?}",
        result.content
    );
    result
        .structured_content
        .clone()
        .unwrap_or_else(|| panic!("{tool} returned no structured content"))
}

fn text(result: &CallToolResult) -> String {
    serde_json::to_value(&result.content).unwrap().to_string()
}

#[tokio::test]
async fn every_tool_answers_on_the_demo() {
    let c = client().await;

    let tools: Vec<String> = c
        .list_all_tools()
        .await
        .unwrap()
        .into_iter()
        .map(|t| t.name.to_string())
        .collect();
    assert_eq!(tools, chronixd::mcp::MCP_TOOLS, "every tool, in order");

    let metrics = ok(
        &call(&c, "list_metrics", json!({ "match": "^pv_" })).await,
        "list_metrics",
    );
    let names: Vec<&str> = metrics["metrics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"pv_power_w"), "{names:?}");
    assert!(names.iter().all(|n| n.starts_with("pv_")), "{names:?}");

    let devices = ok(
        &call(
            &c,
            "label_values",
            json!({ "label": "device", "metric": "pv_power_w" }),
        )
        .await,
        "label_values",
    );
    let device = devices["values"][0].as_str().unwrap().to_owned();

    let promql = ok(
        &call(&c, "query_promql", json!({ "query": "pv_power_w" })).await,
        "query_promql",
    );
    assert!(!promql["series"].as_array().unwrap().is_empty());
    let range = ok(
        &call(
            &c,
            "query_promql",
            json!({ "query": "pv_power_w", "start": "now-1h", "end": "now" }),
        )
        .await,
        "query_promql range",
    );
    assert_eq!(range["resultType"], "matrix");

    let sql = ok(
        &call(&c, "query_sql", json!({ "query": demo::queries::SQL })).await,
        "query_sql",
    );
    assert!(!sql["rows"].as_array().unwrap().is_empty());

    let tree = ok(
        &call(
            &c,
            "explain",
            json!({ "query": "sum(pv_power_w)", "language": "promql", "analyze": true }),
        )
        .await,
        "explain promql",
    );
    assert_eq!(tree["tree"]["series"], 1);
    let plan = ok(
        &call(
            &c,
            "explain",
            json!({ "query": "SELECT * FROM pv", "language": "sql" }),
        )
        .await,
        "explain sql",
    );
    assert!(!plan["plan"].as_str().unwrap().is_empty());

    let card = ok(&call(&c, "cardinality", json!({})).await, "cardinality");
    assert!(card["headStats"]["numSeries"].as_u64().unwrap() > 0);

    let fc = ok(
        &call(
            &c,
            "forecast",
            json!({ "metric": "pv_power_w", "labels": { "device": device }, "horizon": 60 }),
        )
        .await,
        "forecast",
    );
    assert_eq!(fc["points"].as_array().unwrap().len(), 60);
    assert!(fc["model"].as_str().is_some_and(|m| !m.is_empty()));

    let anomalies = ok(
        &call(
            &c,
            "detect_anomalies",
            json!({ "metric": "heat_pump_power_w" }),
        )
        .await,
        "detect_anomalies",
    );
    assert!(anomalies["total"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn detect_anomalies_finds_the_planted_spike() {
    let c = client().await;
    let result = call(
        &c,
        "detect_anomalies",
        json!({ "metric": "heat_pump_power_w" }),
    )
    .await;
    let found = ok(&result, "detect_anomalies");
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let planted = demo::Planted::for_window(demo::window_end(now));
    let strongest = found["anomalies"][0]["t"].as_str().unwrap();
    let t = chrono::DateTime::parse_from_rfc3339(strongest)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap();
    // The window ends at a 10 s step; a second's slack either side.
    let (from, to) = planted.heat_pump_spike;
    assert!(
        (from - 1_000_000_000..to + 1_000_000_000).contains(&t),
        "strongest anomaly at {strongest}, planted {from}..{to}"
    );
    assert!(text(&result).contains("strongest at"), "{}", text(&result));
}

#[tokio::test]
async fn forecast_with_a_step_fits_at_that_step() {
    let c = client().await;
    let labels = json!({ "device": "pv" });
    // A week at 15m is 672 buckets: the forecast comes back at that spacing.
    let fc = ok(
        &call(
            &c,
            "forecast",
            json!({ "metric": "pv_power_w", "labels": labels, "start": "now-7d",
                    "step": "15m", "horizon": 96 }),
        )
        .await,
        "forecast at a step",
    );
    assert_eq!(fc["points"].as_array().unwrap().len(), 96);
    assert!(
        (fc["step_s"].as_f64().unwrap() - 900.0).abs() < 1e-9,
        "{fc}"
    );

    // A step that would cut the window past the cap is refused, with a way out.
    let too_fine = call(
        &c,
        "forecast",
        json!({ "metric": "pv_power_w", "labels": labels, "start": "now-7d",
                "step": "10s", "horizon": 5 }),
    )
    .await;
    assert_eq!(too_fine.is_error, Some(true));
    assert!(
        text(&too_fine).contains("larger step"),
        "{}",
        text(&too_fine)
    );
}

#[tokio::test]
async fn forecast_min_clamps_and_is_reported() {
    let c = client().await;
    let args = |min: Option<f64>| {
        let mut a = json!({ "metric": "pv_power_w", "labels": { "device": "pv" },
                            "start": "now-7d", "step": "15m", "horizon": 96 });
        if let Some(m) = min {
            a["min"] = json!(m);
        }
        a
    };
    let floor_of = |fc: &serde_json::Value| {
        fc["points"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|p| [p["value"].as_f64().unwrap(), p["lower"].as_f64().unwrap()])
            .fold(f64::INFINITY, f64::min)
    };

    // Unclamped, PV's symmetric night-time interval reaches below zero: the
    // case the floor exists for. If it ever stops doing so the test still
    // checks the reporting half.
    let free = ok(&call(&c, "forecast", args(None)).await, "forecast");
    assert!(free["clamped_below"].is_null(), "{free}");

    let clamped = ok(
        &call(&c, "forecast", args(Some(0.0))).await,
        "forecast min 0",
    );
    assert_eq!(clamped["clamped_below"].as_f64(), Some(0.0), "{clamped}");
    assert!(floor_of(&clamped) >= 0.0, "{clamped}");
    if floor_of(&free) < 0.0 {
        assert!(floor_of(&clamped) == 0.0);
    }

    let bad = call(&c, "forecast", {
        let mut a = args(None);
        a["min"] = json!("zero");
        a
    })
    .await;
    assert_eq!(bad.is_error, Some(true));
}

#[tokio::test]
async fn a_metric_with_several_series_must_be_narrowed() {
    let c = client().await;
    let result = call(
        &c,
        "forecast",
        json!({ "metric": "pv_power_w", "horizon": 5 }),
    )
    .await;
    let devices = ok(
        &call(
            &c,
            "label_values",
            json!({ "label": "device", "metric": "pv_power_w" }),
        )
        .await,
        "label_values",
    );
    if devices["values"].as_array().unwrap().len() > 1 {
        assert_eq!(result.is_error, Some(true));
        assert!(text(&result).contains("series"), "{}", text(&result));
    }
    // A selector that matches nothing says so, and how to fix it.
    let none = call(
        &c,
        "forecast",
        json!({ "metric": "pv_power_w", "labels": { "device": "no-such-device" }, "horizon": 5 }),
    )
    .await;
    assert_eq!(none.is_error, Some(true));
    assert!(text(&none).contains("no samples"), "{}", text(&none));
}

#[tokio::test]
async fn tools_are_read_only() {
    let c = client().await;
    for tool in c.list_all_tools().await.unwrap() {
        let read_only = tool.annotations.as_ref().and_then(|a| a.read_only_hint);
        assert_eq!(
            read_only,
            Some(true),
            "{} must say it only reads",
            tool.name
        );
    }
    for statement in [
        "INSERT INTO pv (power_w) VALUES (1)",
        "CREATE TABLE x (a INT)",
        "DROP TABLE pv",
    ] {
        let r = call(&c, "query_sql", json!({ "query": statement })).await;
        assert_eq!(r.is_error, Some(true), "{statement} must be refused");
    }
}

#[tokio::test]
async fn results_are_bounded_and_say_so() {
    let c = client().await;
    let r = call(&c, "query_sql", json!({ "query": "SELECT * FROM pv" })).await;
    let out = ok(&r, "query_sql");
    assert_eq!(out["truncated"], true);
    assert!(text(&r).contains("truncated"), "{}", text(&r));
    let bytes = serde_json::to_vec(&out).unwrap().len();
    assert!(bytes < 300 * 1024, "{bytes} bytes");
}

#[tokio::test]
async fn errors_carry_hints() {
    let c = client().await;
    let r = call(&c, "query_sql", json!({ "query": "SELECT * FROM pvv" })).await;
    assert_eq!(r.is_error, Some(true));
    assert!(text(&r).contains(r#"\"pv\""#), "{}", text(&r));

    let r = call(&c, "query_promql", json!({ "query": "pv_powr_w" })).await;
    let out = ok(&r, "query_promql");
    assert!(
        out["warnings"].to_string().contains("pv_power_w"),
        "an empty result says what was meant: {out}"
    );
}

#[tokio::test]
async fn negotiates_2026_07_28_and_2025_06_18() {
    for version in [ProtocolVersion::V_2026_07_28, ProtocolVersion::V_2025_06_18] {
        let c = connect(demo_server(), version.clone(), &[]).await;
        let negotiated = c.peer().peer_info().map(|i| i.protocol_version.clone());
        assert_eq!(negotiated, Some(version.clone()));
        let r = call(&c, "list_metrics", json!({})).await;
        ok(&r, "list_metrics");
    }
}

#[tokio::test]
async fn concurrent_tool_calls_are_independent() {
    let c = Arc::new(client().await);
    let calls = (0..8).map(|i| {
        let c = Arc::clone(&c);
        tokio::spawn(async move {
            let q = if i % 2 == 0 {
                "pv_power_w"
            } else {
                "heat_pump_power_w"
            };
            let r = call(&c, "query_promql", json!({ "query": q })).await;
            ok(&r, "query_promql")["series"].as_array().unwrap().len()
        })
    });
    for n in futures::future::join_all(calls).await {
        assert!(n.unwrap() > 0);
    }
}

#[tokio::test]
async fn the_tool_allowlist_is_enforced_at_list_and_call() {
    let dir = tempfile::tempdir().unwrap();
    let db = demo_db(&dir.path().join("data"));
    let mut config = ServerConfig::default();
    config.mcp.tools = vec!["query_promql".into()];
    let addr = spawn(SharedState::standalone(db, config));
    let c = connect(addr, ProtocolVersion::LATEST, &[]).await;

    let names: Vec<String> = c
        .list_all_tools()
        .await
        .unwrap()
        .into_iter()
        .map(|t| t.name.to_string())
        .collect();
    assert_eq!(names, ["query_promql"]);
    let refused = c
        .call_tool(
            CallToolRequestParams::new("forecast").with_arguments(
                json!({ "metric": "pv_power_w", "horizon": 5 })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await;
    assert!(refused.is_err(), "a tool left out is not callable by name");
}

#[test]
fn every_tool_maps_to_a_cedar_action() {
    use axum::http::Method;
    use chronix_security::authz::ChronixAction;
    // `/mcp` is one route whatever the method; every tool reads.
    for method in [Method::POST, Method::GET, Method::DELETE] {
        assert_eq!(
            chronixd::namespace::action_for_route("/mcp", &method),
            Some(ChronixAction::Read),
            "{method} /mcp"
        );
    }
    assert_eq!(chronixd::mcp::MCP_TOOLS.len(), 8);
}

#[tokio::test]
async fn a_namespace_sees_only_its_own_series() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Chronix::open(
            ChronixConfig::builder()
                .data_dir(dir.path().to_path_buf())
                .build()
                .unwrap(),
        )
        .unwrap(),
    );
    let mut config = ServerConfig::default();
    config.server.multi_tenancy = true;
    let state = SharedState::standalone(db, config);
    for ns in ["tenant-a", "tenant-b"] {
        state
            .namespace_registry
            .create_namespace(
                chronix_core::NamespaceId::new(ns).unwrap(),
                "test",
                "test",
                chronix_core::NamespaceQuota::default(),
            )
            .unwrap();
    }
    let addr = spawn(state);
    chronixd::tls::ensure_crypto_provider();
    let http = reqwest::Client::new();
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    for (ns, measurement) in [("tenant-a", "alpha"), ("tenant-b", "beta")] {
        for i in 0..20 {
            let resp = http
                .post(format!("http://{addr}/api/v1/write"))
                .header("X-Namespace", ns)
                .json(&json!({
                    "measurement": measurement,
                    "tags": { "host": "h" },
                    "fields": { "load": f64::from(i) },
                    "timestamp": now - i64::from(20 - i) * 60_000_000_000,
                }))
                .send()
                .await
                .unwrap();
            assert!(resp.status().is_success(), "write to {ns}");
        }
    }

    let c = connect(
        addr,
        ProtocolVersion::LATEST,
        &[("x-namespace", "tenant-a")],
    )
    .await;
    let listed = ok(&call(&c, "list_metrics", json!({})).await, "list_metrics").to_string();
    assert!(listed.contains("alpha_load"), "{listed}");
    assert!(
        !listed.contains("beta"),
        "tenant-b's metric leaked: {listed}"
    );

    let sql = call(
        &c,
        "query_sql",
        json!({ "query": "SELECT count(*) FROM beta" }),
    )
    .await;
    let rows = sql
        .structured_content
        .as_ref()
        .map(|v| v["rows"].to_string())
        .unwrap_or_default();
    assert!(
        sql.is_error == Some(true) || rows.contains("[0]") || rows == "[]",
        "tenant-a read tenant-b's rows: {rows}"
    );

    let fc = call(
        &c,
        "forecast",
        json!({ "metric": "beta_load", "horizon": 3 }),
    )
    .await;
    assert_eq!(
        fc.is_error,
        Some(true),
        "another tenant's series cannot be forecast"
    );
}

// ── stdio ────────────────────────────────────────────────────────────────

const BIN: &str = env!("CARGO_BIN_EXE_chronixd");

/// Every file under `dir` with its size and modification time.
fn snapshot(dir: &Path) -> BTreeMap<PathBuf, (u64, std::time::SystemTime)> {
    let mut out = BTreeMap::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let e = e.unwrap();
            let md = e.metadata().unwrap();
            if md.is_dir() {
                stack.push(e.path());
            }
            out.insert(e.path(), (md.len(), md.modified().unwrap()));
        }
    }
    out
}

async fn stdio_client(dir: &Path) -> Result<RunningService<RoleClient, ()>, String> {
    let mut cmd = tokio::process::Command::new(BIN);
    cmd.arg("mcp").arg("--data-dir").arg(dir);
    let transport = rmcp::transport::TokioChildProcess::new(cmd).map_err(|e| e.to_string())?;
    ().serve(transport).await.map_err(|e| e.to_string())
}

#[tokio::test]
async fn stdio_mode_answers_and_never_writes() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let db = demo_db(&data);
    db.close().unwrap();
    drop(db);
    let before = snapshot(&data);

    let c = stdio_client(&data).await.unwrap();
    let names: Vec<String> = c
        .list_all_tools()
        .await
        .unwrap()
        .into_iter()
        .map(|t| t.name.to_string())
        .collect();
    assert_eq!(names, chronixd::mcp::MCP_TOOLS);
    let r = c
        .call_tool(
            CallToolRequestParams::new("query_sql").with_arguments(
                json!({ "query": demo::queries::SQL })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
    assert_ne!(r.is_error, Some(true), "{:?}", r.content);
    c.cancel().await.unwrap();

    let after = snapshot(&data);
    let changed: Vec<String> = before
        .keys()
        .chain(after.keys())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .filter(|p| before.get(*p) != after.get(*p))
        .map(|p| format!("{} {:?} -> {:?}", p.display(), before.get(p), after.get(p)))
        .collect();
    assert!(
        changed.is_empty(),
        "stdio mode wrote to the directory: {changed:#?}"
    );
}

#[tokio::test]
async fn stdio_refuses_a_live_directory() {
    let dir = tempfile::tempdir().unwrap();
    let live = Chronix::open(
        ChronixConfig::builder()
            .data_dir(dir.path().to_path_buf())
            .build()
            .unwrap(),
    )
    .unwrap();
    // The holder record is written at open; a refusal needs nothing else.
    let out = std::process::Command::new(BIN)
        .arg("mcp")
        .arg("--data-dir")
        .arg(dir.path())
        .stdin(std::process::Stdio::null())
        .output()
        .unwrap();
    assert!(!out.status.success(), "a live directory must be refused");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("in use by process"), "{stderr}");
    assert!(out.stdout.is_empty(), "nothing on the protocol channel");
    drop(live);
}

/// With a single query slot, a tool takes exactly one slot per query — never
/// one for the tool and another for a handler it calls — so concurrent tool
/// calls queue behind each other and all complete.
#[tokio::test(flavor = "multi_thread")]
async fn one_query_slot_serves_concurrent_tools() {
    let dir = tempfile::tempdir().unwrap();
    let db = demo_db(&dir.path().join("data"));
    let mut config = ServerConfig::default();
    config.server.max_concurrent_queries = 1;
    let addr = spawn(SharedState::standalone(db, config));

    let (a, b, c) = tokio::join!(
        connect(addr, ProtocolVersion::LATEST, &[]),
        connect(addr, ProtocolVersion::LATEST, &[]),
        connect(addr, ProtocolVersion::LATEST, &[]),
    );
    let forecast = json!({ "metric": "pv_power_w", "labels": { "device": "pv" },
                           "start": "now-1d", "step": "15m", "horizon": 8 });
    let detect = json!({ "metric": "heat_pump_power_w", "start": "now-1d" });
    let query = json!({ "query": "pv_power_w", "start": "now-1h", "end": "now", "step": "1m" });
    let (f, d, q) = tokio::join!(
        call(&a, "forecast", forecast),
        call(&b, "detect_anomalies", detect),
        call(&c, "query_promql", query),
    );
    ok(&f, "forecast");
    ok(&d, "detect_anomalies");
    ok(&q, "query_promql");
}
