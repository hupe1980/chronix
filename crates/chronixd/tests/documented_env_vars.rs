#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Every environment variable the documentation promises must be read.
//!
//! The configuration guide carried a table of four `CHRONIX_*` overrides —
//! `CHRONIX_DATA_DIR`, `CHRONIX_LOG_LEVEL`, `CHRONIX_HTTP_ADDR` and
//! `CHRONIX_JWT_SECRET` — and **not one of them was read anywhere in the
//! tree**. A container setting `CHRONIX_DATA_DIR` wrote to `./chronix-data`
//! instead, which is the worst possible way to find out: nothing fails, the
//! data is simply somewhere else, and it is discovered when the volume turns
//! out to be empty.
//!
//! Two more claims of the same shape sat elsewhere: a
//! `CHRONIXD_MAX_RANGE_QUERY_POINTS` override in a doc comment, and a
//! `${ENV_VAR}` syntax for a webhook `auth_header` setting that does not
//! exist. Both are corrected; this test is what stops the class coming back.
//!
//! Same rule as `documented_sql` and `suite::dashboards`: prose that claims
//! something checkable is checked.

use std::collections::BTreeSet;

/// Every `CHRONIX*` identifier the documentation presents as a **built-in
/// override** — a bare name in prose or a table, rather than a `$VAR`
/// reference or a quoted value.
///
/// A `$CHRONIX_INGEST_KEY` inside a config example is the *generic* env
/// reference, resolved by `config::resolve_env_reference` for any
/// secret-bearing field; the variable name there is the reader's to choose, so
/// requiring the code to mention it would be wrong. A bare name in a table of
/// overrides is a promise about a specific variable, and that is what this
/// checks.
fn documented() -> BTreeSet<String> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let re = regex::Regex::new(r"\bCHRONIXD?_[A-Z0-9_]{3,}\b").unwrap();
    // Two forms name a variable without promising it exists as a built-in
    // override, and both are dropped before matching:
    //
    //  - `$VAR` / `${VAR}` — the *generic* reference syntax, resolved for any
    //    secret-bearing field, where the name is the reader's to choose;
    //  - `"VAR"` as a value — `hmac_key_env = "CHRONIX_AUDIT_KEY"` is a
    //    setting whose value happens to be a variable name.
    //
    // What is left is a bare name in prose or a table, which is a promise
    // about that specific variable.
    let generic =
        regex::Regex::new(r#"\$\{?CHRONIXD?_[A-Z0-9_]+\}?|"CHRONIXD?_[A-Z0-9_]+""#).unwrap();

    let mut found = BTreeSet::new();
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
                let Ok(text) = std::fs::read_to_string(&path) else {
                    continue;
                };
                let text = generic.replace_all(&text, "");
                for m in re.find_iter(&text) {
                    found.insert(m.as_str().to_string());
                }
            }
        }
    }
    found
}

/// Every `CHRONIX*` identifier the crate sources actually mention.
fn implemented() -> BTreeSet<String> {
    let crates = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap();
    let re = regex::Regex::new(r#""(CHRONIXD?_[A-Z0-9_]{3,})""#).unwrap();

    let mut found = BTreeSet::new();
    let mut stack = vec![crates.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n == "target") {
                    continue;
                }
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let Ok(text) = std::fs::read_to_string(&path) else {
                    continue;
                };
                for c in re.captures_iter(&text) {
                    found.insert(c[1].to_string());
                }
            }
        }
    }
    found
}

#[test]
fn every_documented_environment_variable_is_read_by_the_code() {
    let documented = documented();
    assert!(
        !documented.is_empty(),
        "the documentation should name at least one environment variable — \
         if the table was removed, remove this assertion too"
    );

    let implemented = implemented();
    let missing: Vec<&String> = documented.difference(&implemented).collect();
    assert!(
        missing.is_empty(),
        "these environment variables are documented but nothing reads them:\n  {}\n\
         Either implement them or take them out of the documentation — a promised \
         override that is ignored fails silently, with the default in place.",
        missing
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}

/// A lookup standing in for the process environment.
fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
    let map: std::collections::HashMap<String, String> = pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    move |name: &str| map.get(name).cloned()
}

/// Each override must actually change the value it names.
#[test]
fn the_overrides_change_what_they_say_they_change() {
    let mut config = chronixd::config::ServerConfig::default();
    let original_dir = config.database.data_dir.clone();

    config
        .apply_overrides_from(env_of(&[
            ("CHRONIX_DATA_DIR", "/tmp/chronix-env-test"),
            ("CHRONIX_LOG_LEVEL", "trace"),
            ("CHRONIX_HTTP_ADDR", "127.0.0.1:9999"),
        ]))
        .unwrap();

    assert_ne!(config.database.data_dir, original_dir);
    assert_eq!(
        config.database.data_dir,
        std::path::PathBuf::from("/tmp/chronix-env-test")
    );
    assert_eq!(config.log_level, "trace");
    assert_eq!(config.http_addr.to_string(), "127.0.0.1:9999");
}

/// An unset variable leaves the configured value alone.
#[test]
fn an_unset_variable_changes_nothing() {
    let mut config = chronixd::config::ServerConfig::default();
    let before = config.clone();
    config.apply_overrides_from(env_of(&[])).unwrap();
    assert_eq!(config.log_level, before.log_level);
    assert_eq!(config.http_addr, before.http_addr);
    assert_eq!(config.database.data_dir, before.database.data_dir);
}

/// An unparseable value must stop the server, not be ignored.
#[test]
fn a_malformed_override_is_a_startup_error() {
    let mut config = chronixd::config::ServerConfig::default();
    let err = config
        .apply_overrides_from(env_of(&[("CHRONIX_HTTP_ADDR", "not-an-address")]))
        .expect_err("a malformed address must refuse to start rather than fall back");
    assert!(err.to_string().contains("CHRONIX_HTTP_ADDR"), "got: {err}");
}

/// `CHRONIX_JWT_SECRET` overrides a JWT config; it does not conjure one.
#[test]
fn the_jwt_secret_override_needs_a_jwt_section() {
    let mut config = chronixd::config::ServerConfig::default();
    let err = config
        .apply_overrides_from(env_of(&[("CHRONIX_JWT_SECRET", "s3cret")]))
        .expect_err("no [auth.jwt] section means this cannot be honoured");
    assert!(
        err.to_string().contains("auth.jwt"),
        "the error must say what is missing, got: {err}"
    );
}

// ── Configuration that the binary cannot honour ─────────────────────────

/// `[cold_archive]` without the `object-store` feature is a startup error.
///
/// A configured section that the binary silently ignores is the defect class
/// the environment-variable table above belonged to: nothing fails, and the
/// operator finds out when the disk fills. With the feature on it is accepted;
/// without it, refused by name.
#[test]
fn a_cold_archive_section_needs_the_object_store_feature() {
    let toml = r#"
http_addr = "127.0.0.1:8080"
grpc_addr = "127.0.0.1:8081"
flight_addr = "127.0.0.1:8082"

[database]
data_dir = "/tmp/chronix-cold-archive-test"

[cold_archive]
remote_url = "file:///tmp/chronix-archive"
"#;
    let config: chronixd::config::ServerConfig =
        toml::from_str(toml).expect("the section must parse whatever the feature set");
    let cold = config
        .cold_archive
        .expect("[cold_archive] must deserialize into the config");
    assert_eq!(cold.remote_url, "file:///tmp/chronix-archive");
    // The defaults are the ones the documentation states.
    assert_eq!(cold.cold_after_secs, 30 * 86_400);
    assert_eq!(cold.interval_secs, 3_600);
    assert_eq!(cold.max_objects_per_run, 8);
}
