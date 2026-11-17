//! `chronixd inspect <DIR>` through the real binary.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

use chronix::fields;
use chronix::prelude::*;

const BIN: &str = env!("CARGO_BIN_EXE_chronixd");
const T0: i64 = 1_700_000_000_000_000_000;

/// Six measurements, fifty series, closed cleanly, in `<tmp>/data`.
fn gateway_dir(tmp: &Path) {
    let db = Chronix::open(
        ChronixConfig::builder()
            .data_dir(tmp.join("data"))
            .build()
            .unwrap(),
    )
    .unwrap();
    let names = [
        "pv",
        "battery",
        "grid",
        "heat_pump",
        "ev_charger",
        "household_load",
    ];
    let mut series = 0;
    for (i, m) in names.iter().enumerate() {
        let per = if i < 2 { 9 } else { 8 }; // 9+9+8+8+8+8 = 50
        for unit in 0..per {
            let tags: BTreeMap<String, String> = [("unit".to_owned(), format!("u{unit}"))].into();
            let key = SeriesKey::new(*m, tags).unwrap();
            for t in 0..4 {
                db.insert(
                    &Point::new(
                        key.clone(),
                        fields! { "watts" => 1.0 },
                        T0 + t * 1_000_000_000,
                    )
                    .unwrap(),
                )
                .unwrap();
            }
            series += 1;
        }
    }
    assert_eq!(series, 50);
    db.close().unwrap();
}

fn inspect(cwd: &Path, args: &[&str]) -> std::process::Output {
    Command::new(BIN)
        .current_dir(cwd)
        .arg("inspect")
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn human_output_fits_80_columns() {
    let tmp = tempfile::tempdir().unwrap();
    gateway_dir(tmp.path());
    let out = inspect(tmp.path(), &["data"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(
        text.starts_with("data  closed cleanly · generation "),
        "{text}"
    );
    assert!(text.contains("series    50 · budget"), "{text}");
    for line in text.lines() {
        assert!(
            line.chars().count() <= 80,
            "{} columns: {line}",
            line.chars().count()
        );
    }
}

#[test]
fn inspect_json_schema_is_stable() {
    let tmp = tempfile::tempdir().unwrap();
    gateway_dir(tmp.path());
    let out = inspect(tmp.path(), &["data", "--json"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let doc: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    // `schema` first, so a reader can dispatch on the first key.
    let keys: Vec<&str> = doc
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        [
            "schema",
            "path",
            "state",
            "formats",
            "series_total",
            "series_budget",
            "measurements",
            "rollups",
            "detectors",
            "wal",
            "problems",
        ]
    );
    assert_eq!(doc["schema"], "chronix.inspect/1");
    assert_eq!(doc["state"]["kind"], "closed_cleanly");
    assert_eq!(doc["series_total"], 50);
    let m = &doc["measurements"][0];
    for key in [
        "name",
        "segments",
        "series",
        "oldest_ns",
        "newest_ns",
        "oldest",
        "newest",
        "bytes",
        "pending_drop",
    ] {
        assert!(m.get(key).is_some(), "measurement lacks {key}: {m}");
    }
}

#[test]
fn inspect_refuses_a_non_chronix_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let out = inspect(tmp.path(), &["."]);
    assert_eq!(out.status.code(), Some(1));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("error: . is not a chronix data directory (no catalog/ or segments/)"),
        "{err}"
    );
    assert!(out.stdout.is_empty());
}

#[test]
fn bare_chronixd_still_parses_server_flags() {
    // `--check-config` resolves the flags and exits without binding a port
    // or opening the data directory: the server's own command line is
    // unchanged by the subcommand.
    let tmp = tempfile::tempdir().unwrap();
    let out = Command::new(BIN)
        .current_dir(tmp.path())
        .args([
            "--data-dir",
            "data",
            "--bind",
            "127.0.0.1:0",
            "--check-config",
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !tmp.path().join("data").exists(),
        "--check-config opened the data directory"
    );
}
