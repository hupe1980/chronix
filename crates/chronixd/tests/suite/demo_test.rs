//! `chronixd --demo`: the data it generates, the queries its banner prints,
//! and the binary's refusals.
//!
//! The banner is a promise that each printed command answers; these tests
//! run the same strings, from `chronixd::demo::queries`, against the same data.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use arrow::array::{Array, TimestampNanosecondArray};
use arrow::record_batch::RecordBatch;
use chronix::Chronix;
use chronix::prelude::*;
use chronixd::demo::{self, Planted, STEP_NS, queries};

const BIN: &str = env!("CARGO_BIN_EXE_chronixd");
/// 2025-11-17 12:00:00 UTC, a fixed end so the assertions name fixed times.
const END: i64 = 1_763_380_800_000_000_000;

/// One demo database for the whole module: generating a week is the
/// expensive part, and every test reads it.
fn demo_db() -> &'static Chronix {
    static DB: OnceLock<(tempfile::TempDir, Chronix)> = OnceLock::new();
    &DB.get_or_init(|| {
        let dir = tempfile::tempdir().unwrap();
        let db = Chronix::open(
            ChronixConfig::builder()
                .data_dir(dir.path().join("data"))
                .build()
                .unwrap(),
        )
        .unwrap();
        demo::ingest(&db, END).unwrap();
        (dir, db)
    })
    .1
}

fn times(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            let col = b
                .column_by_name(chronix::chronix_core::TIME_COLUMN)
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            (0..col.len()).map(|i| col.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

#[test]
fn demo_generator_is_deterministic() {
    let a = demo::generate(END);
    let b = demo::generate(END);
    assert_eq!(a[..1000], b[..1000]);
    assert_eq!(a, b);
}

#[test]
fn demo_anomalies_rank_top_five() {
    let db = demo_db();
    let planted = Planted::for_window(END);

    // The heat pump's spike: every one of the five strongest scores.
    let (a, b) = planted.heat_pump_spike;
    let top = times(&db.sql(queries::ANOMALIES).unwrap());
    assert_eq!(top.len(), 5, "{top:?}");
    assert!(
        top.iter().all(|t| (a..b).contains(t)),
        "{top:?} outside {a}..{b}"
    );

    // The PV dropout: its two edges are the two largest step changes.
    let (a, b) = planted.pv_dropout;
    let top = times(&db.sql(queries::PV_DROPOUT).unwrap());
    let mut edges = top[..2].to_vec();
    edges.sort_unstable();
    assert_eq!(edges, vec![a, b], "top step changes: {top:?}");
}

#[test]
fn demo_banner_commands_answer() {
    let db = demo_db();
    let vector = db.promql(queries::PROMQL, END).unwrap();
    assert!(
        format!("{vector:?}").contains("pv_power_w"),
        "PromQL answered {vector:?}"
    );
    for (name, sql) in [
        ("SQL", queries::SQL),
        ("forecast", queries::FORECAST),
        ("anomalies", queries::ANOMALIES),
    ] {
        let answered = rows(&db.sql(sql).unwrap_or_else(|e| panic!("{name}: {e}")));
        assert!(answered > 0, "{name} returned no rows");
    }
    // The forecast is the next hour: 360 ten-second steps.
    assert_eq!(rows(&db.sql(queries::FORECAST).unwrap()), 360);
}

#[test]
fn demo_generates_and_ingests_quickly() {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open(
        ChronixConfig::builder()
            .data_dir(dir.path().join("data"))
            .build()
            .unwrap(),
    )
    .unwrap();
    let started = Instant::now();
    let written = demo::ingest(&db, END).unwrap();
    let took = started.elapsed();
    assert_eq!(written as i64, 4 * (7 * 86_400_000_000_000 / STEP_NS + 1));
    // The 5-second budget is for a release build; a debug build only reports.
    if !cfg!(debug_assertions) {
        assert!(took < Duration::from_secs(5), "demo ingest took {took:?}");
    }
    eprintln!("demo ingest: {written} points in {took:?}");
}

#[test]
fn demo_refuses_real_directories() {
    for args in [
        &["--demo", "--data-dir", "/tmp/should-not-exist"][..],
        &["--demo", "--config", "/tmp/should-not-exist.toml"][..],
    ] {
        let out = Command::new(BIN)
            .args(args)
            .env_remove("CHRONIXD_DATA_DIR")
            .env_remove("CHRONIXD_CONFIG")
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(2), "{args:?}");
        assert_eq!(
            String::from_utf8_lossy(&out.stderr).trim(),
            "error: --demo never writes into a real directory; \
             drop --data-dir/--config, or run without --demo"
        );
    }
}

#[test]
fn demo_is_not_a_subcommand_flag() {
    let out = Command::new(BIN)
        .args(["--demo", "inspect", "/tmp"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}

/// The real binary: the banner appears only once the port answers, and the
/// address it names is the one serving.
#[test]
fn demo_serves_on_the_address_it_prints() {
    chronixd::tls::ensure_crypto_provider();
    let mut child = Command::new(BIN)
        .args(["--demo", "--bind", "127.0.0.1:0"])
        .env_remove("CHRONIXD_DATA_DIR")
        .env_remove("CHRONIXD_CONFIG")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stderr = BufReader::new(child.stderr.take().unwrap());
    let mut addr = None;
    for line in stderr.lines() {
        let line = line.unwrap();
        if let Some(rest) = line.strip_prefix("No authentication. Bound to ") {
            addr = rest.strip_suffix(" only.").map(str::to_owned);
        }
        if line.starts_with("Planted anomalies:") {
            break;
        }
    }
    let addr = addr.expect("the banner names its address");
    let body = reqwest::blocking::get(format!(
        "http://{addr}/api/v1/query?query={}",
        queries::PROMQL
    ))
    .unwrap()
    .text()
    .unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(body.contains("\"pv_power_w\""), "{body}");
}

#[test]
fn demo_reports_a_busy_port() {
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = taken.local_addr().unwrap();
    let out = Command::new(BIN)
        .args(["--demo", "--bind", &addr.to_string()])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(
        String::from_utf8_lossy(&out.stderr).trim(),
        format!("error: {addr} is in use; try --demo --bind 127.0.0.1:0")
    );
}
