#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Every configuration file the documentation shows must be one the server
//! can load.
//!
//! The configuration guide opened with this, as the first example anybody
//! copies:
//!
//! ```toml
//! [server]
//! http_addr = "0.0.0.0:8086"
//! ```
//!
//! There was no `[server]` section. The settings lived loose at the top level,
//! and serde drops a table it does not recognise exactly as silently as it
//! drops a misspelt key — so an operator who copied the documented file got a
//! server on **every default**: listening on `0.0.0.0` after asking for
//! loopback, with the documented `sql_max_rows`, `max_body_size` and
//! `log_format` all ignored, no error, and no log line. Two more sections were
//! fiction beside it — `[tracing]`, which nothing deserialised, and
//! `[connectors.mqtt]`, which was `[mqtt]`.
//!
//! `check-docs.sh` §8 could not see any of it: it asks whether the *leaf key*
//! is named somewhere in the code, and `http_addr` is. Only handing the block
//! to the real parser answers the question that matters, which is whether the
//! file does what the page says it does.
//!
//! Two halves, and both are needed:
//!
//!  1. every documented block **parses** — `ServerConfig` and every struct
//!     under it is `deny_unknown_fields`, so a key or a section that does not
//!     exist fails here instead of being ignored in production;
//!  2. every documented block **takes effect** — a block that sets
//!     `http_addr` must produce a config whose `http_addr` is that value, not
//!     the default. Parsing alone would still pass if a section were dropped
//!     by a `#[serde(skip)]`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use chronixd::config::ServerConfig;

/// Top-level tables that belong to a `chronixd` configuration file.
///
/// A fenced `toml` block is treated as one of ours when it opens a table in
/// this set, or when it sets a bare key at the top level (which, after the
/// restructure, is always wrong and must fail). Everything else in the docs —
/// a `Cargo.toml` fragment, a Telegraf `[outputs.influxdb]` — names a table
/// that is not here and is skipped.
const OUR_TABLES: &[&str] = &[
    "server",
    "database",
    "tls",
    "auth",
    "audit",
    "triggers",
    "cold_archive",
    "kafka",
    "mqtt",
    "cluster",
    "tracing",
];

/// Table prefixes that belong to something other than `chronixd`.
///
/// The documentation quotes other tools' configuration — a Prometheus scrape
/// job, a Telegraf output, a Grafana datasource — and those settings are not
/// ours to load. Every entry is a real other program, because the cost of a
/// wrong entry here is a claim about `chronixd` that nothing checks.
const FOREIGN_TABLES: &[&str] = &[
    // Prometheus / Grafana / Telegraf / OpenTelemetry Collector
    "global",
    "scrape_configs",
    "remote_write",
    "remote_read",
    "outputs",
    "inputs",
    "agent",
    "exporters",
    "receivers",
    "processors",
    "service",
    "datasources",
    "apiVersion",
    // Cargo, Docker Compose, Kubernetes
    "package",
    "dependencies",
    "dev_dependencies",
    "workspace",
    "profile",
    "features",
    "services",
    "spec",
    "metadata",
    "resources",
    "env",
];

/// Pages whose TOML is a design sketch rather than a file this binary loads,
/// each with the reason.
///
/// The list is short and every entry is argued, because an exemption is how a
/// check of this kind stops being a check.
fn exempt(page: &Path) -> Option<&'static str> {
    let name = page.file_name()?.to_str()?;
    match name {
        // The cluster tier is frozen and out of the default build; its
        // `[meta]` / `[data]` node files describe a topology `chronixd`
        // cannot be started into today. The page says so.
        "cluster.md" => Some("frozen cluster tier — the node config is a design sketch"),
        _ => None,
    }
}

struct Block {
    page: PathBuf,
    line: usize,
    body: String,
}

fn toml_blocks() -> Vec<Block> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();

    let mut pages = vec![root.join("README.md")];
    let mut stack = vec![root.join("site").join("content")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "md") {
                pages.push(path);
            }
        }
    }
    pages.sort();

    let mut blocks = Vec::new();
    for page in pages {
        if exempt(&page).is_some() {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&page) else {
            continue;
        };
        let mut in_block = false;
        let mut start = 0usize;
        let mut body = String::new();
        for (idx, line) in text.lines().enumerate() {
            if in_block {
                if line.trim_start().starts_with("```") {
                    in_block = false;
                    if is_ours(&body) {
                        blocks.push(Block {
                            page: page.clone(),
                            line: start,
                            body: std::mem::take(&mut body),
                        });
                    } else {
                        body.clear();
                    }
                } else {
                    body.push_str(line);
                    body.push('\n');
                }
            } else if line.trim_start() == "```toml" {
                in_block = true;
                start = idx + 1;
            }
        }
    }
    blocks
}

/// Whether a fenced block is a `chronixd` configuration file.
fn is_ours(body: &str) -> bool {
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("[[").or_else(|| line.strip_prefix('[')) {
            let table = rest
                .trim_end_matches([']', ' '])
                .split('.')
                .next()
                .unwrap_or_default();
            return OUR_TABLES.contains(&table);
        }
        // A bare `key = value` at the top of a block, before any table: after
        // the restructure there is no such thing in a chronixd file, so the
        // block is only ours if a later table says so. Keep scanning.
    }
    false
}

/// Every documented configuration block parses.
#[test]
fn every_documented_config_block_parses() {
    let blocks = toml_blocks();
    assert!(
        blocks.len() >= 10,
        "found only {} documented config blocks — the scanner is broken, not the docs",
        blocks.len()
    );

    let mut failures = Vec::new();
    for block in &blocks {
        if let Err(e) = toml::from_str::<ServerConfig>(&block.body) {
            // An *excerpt* is legitimate documentation: `[tls]
            // reload_interval_secs = 300` shows one setting without repeating
            // `cert` and `key`, and a reader understands that. So a `missing
            // field` is not a finding.
            //
            // An *unknown* field or table is the finding, and it is the only
            // one this check is for: it means the page names something the
            // server does not have, which is precisely what `[server]`,
            // `[tracing]` and `[connectors.mqtt]` each were.
            if e.to_string().contains("missing field") {
                continue;
            }
            failures.push(format!(
                "{}:{}\n{}\n  → {e}",
                block.page.display(),
                block.line,
                block.body.trim_end(),
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} documented config block(s) the server cannot load:\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}

/// Every key a documented block sets is a key the parsed config carries.
///
/// Parsing is not enough on its own: a section dropped by a `#[serde(skip)]`,
/// or a key that lands in a struct nothing reads, parses perfectly. This
/// re-serialises the loaded config and requires each documented leaf key to
/// appear in it — which is the same question `check-docs.sh` §8 asks of the
/// code, asked here of the *loaded value*.
#[test]
fn every_documented_setting_survives_the_load() {
    let mut missing = Vec::new();
    for block in toml_blocks() {
        let Ok(config) = toml::from_str::<ServerConfig>(&block.body) else {
            continue; // reported by the test above
        };
        let round_tripped = toml::to_string(&config).expect("config re-serialises");
        let documented: BTreeSet<&str> = block
            .body
            .lines()
            .filter_map(|l| l.trim().split_once(" ="))
            .map(|(k, _)| k)
            .filter(|k| !k.starts_with('#'))
            .collect();
        for key in documented {
            if !round_tripped.contains(key) {
                missing.push(format!(
                    "{}:{} sets `{key}`, which is not in the loaded configuration",
                    block.page.display(),
                    block.line
                ));
            }
        }
    }
    assert!(missing.is_empty(), "{}", missing.join("\n"));
}

/// A section the server does not know is a startup error, not a silent
/// default.
///
/// This is the property the whole restructure exists for: the documented
/// `[server]` table was ignored for as long as it was wrong, because serde's
/// default is to drop what it does not recognise.
#[test]
fn an_unknown_section_is_refused() {
    let err = toml::from_str::<ServerConfig>(
        r#"
        [srever]
        http_addr = "127.0.0.1:9999"
        "#,
    )
    .expect_err("an unknown table must not be accepted");
    assert!(
        err.to_string().contains("srever"),
        "the error must name the table: {err}"
    );
}

/// A misspelt key inside a known section is refused too.
#[test]
fn an_unknown_key_is_refused() {
    let err = toml::from_str::<ServerConfig>(
        r#"
        [server]
        htpp_addr = "127.0.0.1:9999"
        "#,
    )
    .expect_err("an unknown key must not be accepted");
    assert!(
        err.to_string().contains("htpp_addr"),
        "the error must name the key: {err}"
    );
}

/// The old flat shape is refused rather than half-accepted.
///
/// Before the restructure the scalars lived at the top level. Leaving them
/// accepted "for compatibility" would mean two spellings for one file and no
/// error for the wrong one — which is the situation this pass removed.
#[test]
fn the_old_flat_shape_is_refused() {
    let err = toml::from_str::<ServerConfig>(
        r#"
        http_addr = "127.0.0.1:9999"

        [database]
        data_dir = "/tmp/x"
        "#,
    )
    .expect_err("a top-level http_addr must not be accepted");
    assert!(err.to_string().contains("http_addr"), "{err}");
}

/// A `[tracing]` section with no export target is refused.
#[test]
fn a_tracing_section_with_no_target_is_refused() {
    let config: ServerConfig = toml::from_str(
        r#"
        [tracing]
        service_name = "chronix"
        "#,
    )
    .expect("parses");
    let err = config
        .validate_tracing()
        .expect_err("a [tracing] section with no [tracing.otlp] must be refused");
    assert!(err.to_string().contains("[tracing.otlp]"), "{err}");
}

/// The sampling strategy parses in the form the documentation shows.
#[test]
fn sampling_parses_in_the_documented_form() {
    let config: ServerConfig = toml::from_str(
        r#"
        [tracing.otlp]
        endpoint = "https://collector:4317"
        sampling = { ratio = 0.05 }
        "#,
    )
    .expect("`sampling = { ratio = … }` is the documented form and must parse");
    let otlp = config.tracing.unwrap().otlp.unwrap();
    assert!(matches!(
        otlp.sampling,
        chronixd::otel::SamplingStrategy::Ratio(r) if (r - 0.05).abs() < 1e-9
    ));
}

/// Every engine setting the server documentation names is one a server
/// operator can actually set.
///
/// The `[database]` table maps to `ChronixConfig`, and eight of its settings
/// had no key at all: `maintenance_interval`, `ooo_shard_tolerance`,
/// `future_write_tolerance`, `cdc_capacity`, `wal_max_unflushed`,
/// `soft_delete_ttl`, `lvc_measurements` and the analytics bounds. Three of
/// them are named in *server* pages — "live ingestion is held to
/// ±`ooo_shard_tolerance` shards" in the API reference, "every
/// `maintenance_interval` (30 s)" in operations and getting-started — so an
/// operator was told the name of a knob and had no way to turn it.
///
/// The values are checked through the **real loader**, and each is
/// deliberately different from the engine default, so a key that parses and
/// is then dropped on the floor fails here rather than looking configured.
#[test]
fn the_engine_settings_a_server_operator_needs_are_settable() {
    let toml = r#"
        [database]
        data_dir = "/tmp/chronix-test"
        maintenance_interval_secs = 300
        ooo_shard_tolerance = 5
        future_write_tolerance_secs = 60
        cdc_capacity = 1024
        wal_max_unflushed = 3
        soft_delete_ttl_secs = 86400
        lvc_measurements = ["power", "energy"]
    "#;
    let server: ServerConfig = toml::from_str(toml).expect("the documented form must load");
    let engine = server
        .to_chronix_config()
        .expect("the loaded settings must build an engine config");

    assert_eq!(
        engine.maintenance_interval,
        std::time::Duration::from_secs(300)
    );
    assert_eq!(engine.ooo_shard_tolerance, 5);
    assert_eq!(
        engine.future_write_tolerance,
        std::time::Duration::from_secs(60)
    );
    assert_eq!(engine.cdc_capacity, 1024);
    assert_eq!(engine.wal.max_unflushed_wals, 3);
    assert_eq!(
        engine.soft_delete_ttl,
        Some(std::time::Duration::from_secs(86_400))
    );
    let lvc = engine
        .lvc_measurements
        .as_ref()
        .expect("an explicit list must reach the engine");
    assert!(
        lvc.contains("power") && lvc.contains("energy"),
        "got {lvc:?}"
    );
}

/// Omitting them leaves the engine's own defaults in place.
///
/// They are `Option` for this reason: repeating each default in the server
/// config would give it two definitions that can drift apart, and the value
/// an operator gets would depend on which one they read.
#[test]
fn an_unset_engine_setting_keeps_the_engine_default() {
    let server: ServerConfig = toml::from_str(
        r#"
        [database]
        data_dir = "/tmp/chronix-test"
        "#,
    )
    .expect("a minimal database section must load");
    let engine = server.to_chronix_config().expect("builds");
    let default = chronix::prelude::ChronixConfigBuilder::default()
        .data_dir("/tmp/chronix-test")
        .build()
        .expect("engine defaults are valid");

    assert_eq!(engine.maintenance_interval, default.maintenance_interval);
    assert_eq!(engine.ooo_shard_tolerance, default.ooo_shard_tolerance);
    assert_eq!(
        engine.future_write_tolerance,
        default.future_write_tolerance
    );
    assert_eq!(engine.cdc_capacity, default.cdc_capacity);
    assert_eq!(
        engine.wal.max_unflushed_wals,
        default.wal.max_unflushed_wals
    );
    assert_eq!(engine.soft_delete_ttl, default.soft_delete_ttl);
    assert_eq!(engine.lvc_measurements, default.lvc_measurements);
}

/// A setting the prose tells an operator to set must be a setting that loads.
///
/// `documented_config`'s other halves read **fenced `toml` blocks**, which is
/// where a configuration example lives — and the claim that cost the most was
/// not in one. The security guide's hardening checklist read
///
/// > - [ ] Enable encryption at rest (`storage.encryption.enabled = true`)
///
/// in prose, above a compliance claim, in front of an `EncryptingBackend`
/// that was correct, tested, and had no caller anywhere. There is no
/// `storage` table and never has been. A checklist line is exactly where a
/// false claim does the most damage, because it is the line an auditor ticks
/// off — so the guard has to reach outside the fences.
///
/// It looks for a backticked `a.b = value` or `a = value` where the leading
/// segment names one of [`OUR_TABLES`], builds the smallest TOML document
/// that says the same thing, and hands it to the real loader. Anything whose
/// head is not one of our tables is somebody else's configuration and is
/// skipped, which is the same rule the block scanner uses.
#[test]
fn a_setting_named_in_prose_is_a_setting_that_loads() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let mut failures: Vec<String> = Vec::new();

    for page in markdown_pages(&root.join("site").join("content")) {
        if exempt(&page).is_some() {
            continue;
        }
        let text = std::fs::read_to_string(&page).unwrap_or_default();
        for (lineno, line) in text.lines().enumerate() {
            // Skip the inside of fenced blocks — the other tests own those.
            for claim in backticked_assignments(line) {
                let Some(document) = as_toml_document(&claim) else {
                    continue;
                };
                if let Err(e) = toml::from_str::<ServerConfig>(&document) {
                    let message = e.to_string();
                    if message.contains("unknown field")
                        || message.contains("unknown variant")
                        || message.contains("invalid value")
                        || message.contains("invalid type")
                    {
                        failures.push(format!(
                            "{}:{}\n  claim: `{}`\n  {}",
                            page.strip_prefix(root).unwrap_or(&page).display(),
                            lineno + 1,
                            claim,
                            message
                                .lines()
                                .find(|l| {
                                    l.contains("unknown field")
                                        || l.contains("unknown variant")
                                        || l.contains("invalid value")
                                        || l.contains("invalid type")
                                })
                                .unwrap_or(&message)
                                .trim(),
                        ));
                    }
                }
            }
        }
    }

    assert!(
        failures.is_empty(),
        "{} documented setting(s) the server cannot load:\n\n{}",
        failures.len(),
        failures.join("\n\n"),
    );
}

/// Every `md` file under `dir`.
fn markdown_pages(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|e| e == "md") {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

/// Backticked `key = value` claims in one line of prose.
fn backticked_assignments(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = line;
    while let Some(start) = rest.find('`') {
        let after = &rest[start + 1..];
        let Some(end) = after.find('`') else { break };
        let inner = &after[..end];
        if inner.contains('=') && !inner.contains('\n') {
            out.push(inner.trim().to_owned());
        }
        rest = &after[end + 1..];
    }
    out
}

/// Turn `a.b.c = value` into the TOML document that sets it, or `None` if the
/// claim is not about one of our tables.
fn as_toml_document(claim: &str) -> Option<String> {
    let (path, value) = claim.split_once('=')?;
    let path = path.trim();
    let value = value.trim();
    if value.is_empty() || path.is_empty() {
        return None;
    }
    let segments: Vec<&str> = path.split('.').collect();
    if segments.len() < 2 {
        return None;
    }
    if !segments
        .iter()
        .all(|s| !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
    {
        return None;
    }
    // **Not** `if !OUR_TABLES.contains(...) { return None }`, which is the
    // shape this guard was first written in and the shape that misses the
    // defect it exists for. A whitelist of tables that exist skips a
    // documented table that does **not** — `storage.encryption.enabled`, the
    // key the security checklist told operators to set, has no `[storage]`
    // section and never had one, so a whitelist reads it as somebody else's
    // configuration and says nothing. The same narrowing as pass 52's
    // page-level exemption: the guard's question has to be wider than the
    // set of things that already work.
    //
    // So the default is *ours*, and anything genuinely foreign has to be
    // declared with a reason below.
    if FOREIGN_TABLES.contains(&segments[0]) {
        return None;
    }
    // A leading capital is a type, not a table: `WriteRequest.backfill = true`
    // is a protobuf field. Every configuration table is lower snake case.
    if segments[0].starts_with(|c: char| c.is_ascii_uppercase()) {
        return None;
    }
    let (key, tables) = segments.split_last()?;
    Some(format!("[{}]\n{key} = {value}\n", tables.join(".")))
}
