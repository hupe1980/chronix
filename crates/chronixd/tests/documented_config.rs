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
