//! The supported public surface, pinned.
//!
//! An item becomes public by accident more easily than it becomes private on
//! purpose: a `pub` written for one call site inside the crate is
//! indistinguishable, from the outside, from a promise. Sealing the facade is
//! a pre-1.0 job (`CONTRIBUTING.md`); pinning it now is what makes that
//! possible then, because a widening shows up here as a diff.
//!
//! **To change the surface deliberately**, edit `SURFACE` below in the same
//! commit. The list is the crate root's re-exports and its public modules —
//! tier 1 of the stability note in `lib.rs`.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap

/// Every name `use chronix::*;` brings in, plus the public modules.
///
/// Sorted, one per line. Feature-gated items are listed under the feature
/// that provides them.
const SURFACE: &[&str] = &[
    // ── Modules ────────────────────────────────────────────────────
    "mod db",
    "mod delete",
    "mod error",
    "mod export",
    "mod macros",
    "mod prelude",
    "mod promql",
    "mod retention",
    "mod rollup",
    // ── Re-exported engine crates (tier 3) ─────────────────────────
    "mod chronix_core",
    "mod chronix_encoding",
    "mod chronix_engine",
    "mod chronix_query",
    // ── Types and macros at the crate root (tier 1) ────────────────
    "BackupManifest",
    "Chronix",
    "DatabaseStatistics",
    "DbError",
    "DeleteBuilder",
    "DeleteOutcome",
    "DeleteRequest",
    "InsertResult",
    "ParquetCompression",
    "ParquetExportConfig",
    "SegmentInfo",
    "RollupAggFn",
    "RollupBuilder",
    "RollupConfig",
    "RollupRegistry",
    "RollupState",
    "fields",
    "tags",
];

/// Feature-gated additions, keyed by the feature that provides them.
const FEATURE_SURFACE: &[(&str, &[&str])] = &[
    // On by default. `default-features = false` drops the largest sub-crate
    // in the workspace — forecasting, anomaly detection, preprocessing and
    // the model registry — for a consumer that only stores and queries. The
    // design partner is exactly that consumer and carried all of it until
    // somebody asked why the largest sub-crate was the only engine crate a
    // consumer could not opt out of.
    (
        "analytics",
        &[
            "mod analytics",
            "mod chronix_analytics",
            "AnomalyConfig",
            "ForecastConfig",
        ],
    ),
    ("object-store", &["mod cold_archive"]),
    // On by default; `default-features = false` drops DataFusion and with it
    // `db.sql()`, `session_context()` and the analytics SQL functions. The
    // rest of the surface — writes, the native query API, PromQL, rollups,
    // retention, triggers — is unchanged.
    ("sql", &["mod sql"]),
    // Off by default, so an embedded build carries neither the CDC bus nor
    // the security crate — 129 packages, a JWT library and a TLS stack among
    // them. `chronixd` enables all three at its dependency and asserts at
    // compile time that it has them.
    ("streaming", &["mod chronix_streaming"]),
    ("security", &["mod chronix_security"]),
    ("pipeline", &["mod pipeline", "Pipeline", "PipelineConfig"]),
];

/// Parse `lib.rs` for what it declares public.
fn declared_surface() -> (Vec<String>, Vec<(String, String)>) {
    let full = include_str!("../src/lib.rs");
    // The prelude's own re-exports are not crate-root items; they are pinned
    // separately by `the_prelude_is_the_one_that_was_reviewed`.
    let src = full
        .split_once("pub mod prelude {")
        .map_or(full, |(head, _)| head);
    let mut always = Vec::new();
    let mut gated: Vec<(String, String)> = Vec::new();
    let mut pending_feature: Option<String> = None;

    for line in src.lines() {
        let trimmed = line.trim();

        // `#[cfg(feature = "x")]` applies to the next item.
        if let Some(rest) = trimmed.strip_prefix("#[cfg(feature = \"") {
            if let Some(name) = rest.split('"').next() {
                pending_feature = Some(name.to_string());
            }
            continue;
        }
        if trimmed.starts_with("//") || trimmed.is_empty() {
            continue;
        }

        let mut names: Vec<String> = Vec::new();
        if let Some(rest) = trimmed.strip_prefix("pub mod ") {
            let name = rest.trim_end_matches(';').trim_end_matches('{').trim();
            names.push(format!("mod {name}"));
        } else if let Some(rest) = trimmed.strip_prefix("pub use ") {
            let rest = rest.trim_end_matches(';');
            // `pub use chronix_core;` re-exports a crate as a module.
            if !rest.contains("::") {
                names.push(format!("mod {rest}"));
            } else {
                let (_path, tail) = rest.rsplit_once("::").unwrap_or(("", rest));
                if let Some(group) = tail.strip_prefix('{') {
                    for item in group.trim_end_matches('}').split(',') {
                        let item = item.trim();
                        if !item.is_empty() {
                            names.push(item.to_string());
                        }
                    }
                } else {
                    names.push(tail.to_string());
                }
            }
        }

        // One `pub use` may export several names, and the `cfg` above it
        // applies to **all** of them. This used to `take()` the pending
        // feature on the first name, so `pub use pipeline::{Pipeline,
        // PipelineConfig}` recorded the first as gated and the second as
        // always-present — a surface entry that would survive its own
        // feature being turned off.
        let feature = pending_feature.take();
        for name in names {
            match &feature {
                Some(f) => gated.push((f.clone(), name)),
                None => always.push(name),
            }
        }
        // An attribute line that was not a cfg still applies to the next item.
        if !trimmed.starts_with('#') {
            pending_feature = None;
        }
    }
    (always, gated)
}

/// Macros are `#[macro_export]`ed, so they live at the crate root regardless
/// of which module defines them.
fn exported_macros() -> Vec<String> {
    let src = include_str!("../src/macros.rs");
    let mut out = Vec::new();
    let mut armed = false;
    for line in src.lines() {
        let t = line.trim();
        if t.starts_with("#[macro_export]") {
            armed = true;
            continue;
        }
        if armed {
            if let Some(rest) = t.strip_prefix("macro_rules! ") {
                out.push(rest.trim_end_matches(" {").trim().to_string());
                armed = false;
            } else if !t.starts_with('#') && !t.is_empty() && !t.starts_with("//") {
                armed = false;
            }
        }
    }
    out
}

#[test]
fn the_public_surface_is_the_one_that_was_reviewed() {
    let (always, _gated) = declared_surface();

    let mut actual: Vec<String> = always;
    actual.extend(exported_macros());
    // The `prelude` module is declared inline rather than with `pub mod`.
    if include_str!("../src/lib.rs").contains("pub mod prelude {") {
        actual.push("mod prelude".to_string());
    }
    actual.sort();
    actual.dedup();

    let mut expected: Vec<String> = SURFACE.iter().map(|s| (*s).to_string()).collect();
    expected.sort();
    expected.dedup();

    let added: Vec<&String> = actual.iter().filter(|a| !expected.contains(a)).collect();
    let removed: Vec<&String> = expected.iter().filter(|e| !actual.contains(e)).collect();

    assert!(
        added.is_empty() && removed.is_empty(),
        "the public surface changed.\n  added:   {added:?}\n  removed: {removed:?}\n\
         If this was deliberate, update SURFACE in this file in the same commit; \
         if it was not, the item probably wants `pub(crate)`."
    );
}

#[test]
fn feature_gated_items_are_declared_under_their_feature() {
    let (_, gated) = declared_surface();
    for (feature, expected) in FEATURE_SURFACE {
        let found: Vec<&String> = gated
            .iter()
            .filter(|(f, _)| f == feature)
            .map(|(_, n)| n)
            .collect();
        for want in *expected {
            assert!(
                found.iter().any(|f| f.as_str() == *want),
                "`{want}` should be gated behind `{feature}`, found {found:?}"
            );
        }
    }
}

/// The prelude is what a first program types, so its contents are pinned too.
#[test]
fn the_prelude_is_the_one_that_was_reviewed() {
    const PRELUDE: &[&str] = &[
        "AggFn",
        // The width of a calendar bucket, beside `TimeBucket` below.
        "BucketWidth",
        "ChronixConfig",
        "ChronixConfigBuilder",
        "ChronixError",
        "Chronix",
        "ColumnDef",
        "ColumnRole",
        "ColumnType",
        "CompressionCodec",
        "DbError",
        // Exact fixed-point. In the prelude because writing one is the
        // point: `FieldValue::Decimal("1234.5678".parse()?)` needs the type
        // in scope, and a settlement quantity that reaches an `f64` because
        // the exact type was one import away is the failure it exists to
        // prevent.
        "Decimal",
        "FieldValue",
        "FloatEncoding",
        "FsyncPolicy",
        "InsertResult",
        "MeasurementSchema",
        "Point",
        "QueryBuilder",
        "QueryPlan",
        "RecordBatch",
        "RollupAggFn",
        "RollupBuilder",
        "SeriesKey",
        // The one answer to "what is a day". In the prelude for the same
        // reason `Decimal` is: `RollupBuilder::bucket()` and
        // `QueryBuilder::downsample()` both take one, and a `1d` that meant
        // "86400 seconds" because the calendar type was one import away is
        // exactly the defect it exists to prevent.
        "TimeBucket",
        "Timestamp",
        "fields",
        "tags",
    ];

    let src = include_str!("../src/lib.rs");
    let body = src
        .split_once("pub mod prelude {")
        .expect("a prelude module")
        .1;
    let body = &body[..body.find("\n}").expect("a closing brace")];

    let mut actual: Vec<String> = Vec::new();
    for chunk in body.split("pub use ").skip(1) {
        let stmt = chunk.split(';').next().unwrap_or_default();
        let tail = stmt.rsplit_once("::").map_or(stmt, |(_, t)| t);
        if let Some(group) = tail.trim().strip_prefix('{') {
            for item in group.trim_end_matches('}').split(',') {
                let item = item.trim();
                if !item.is_empty() {
                    actual.push(item.to_string());
                }
            }
        } else {
            actual.push(tail.trim().to_string());
        }
    }
    actual.sort();
    actual.dedup();

    let mut expected: Vec<String> = PRELUDE.iter().map(|s| (*s).to_string()).collect();
    expected.sort();

    assert_eq!(
        actual, expected,
        "the prelude changed; update PRELUDE in this file in the same commit"
    );
}

// ── The handle's own surface ────────────────────────────────────────
//
// `SURFACE` above pins the crate root: which modules and which names
// `use chronix::*` brings in. It never pinned the methods on `Chronix`,
// which is the surface almost every caller actually touches — and that is
// how the handle reached **eighty** methods, six of which returned the
// engine's internal `RwLock`s.
//
// Those six were not a documentation problem. `CatalogLock` lives in a
// `pub(crate)` module, so a caller could call `db.catalog()` and could not
// name what came back; the lock hierarchy is documented as "an invariant of
// this crate's own implementation, not a contract with callers" and was then
// enforced on callers by a debug-build panic naming levels they had no way
// to read; and in release the assertion compiles away, so the same two reads
// deadlock against the maintenance thread instead. An external probe doing
// two ordinary reads in the wrong order panicked on the second one.
//
// This test is the guard that would have caught it. It is deliberately a
// *list*, not a rule: a new method is a deliberate edit here, in the same
// commit, which is the only mechanism that has ever worked for a surface
// that grows one convenience at a time.

/// Every inherent method on `Chronix`, sorted.
const HANDLE_SURFACE: &[&str] = &[
    // lifecycle
    "close",
    "open",
    "open_small",
    // write
    "backfill",
    "declare_field",
    "insert",
    "insert_batch",
    // read
    "execute",
    "execute_iter",
    "execute_stream",
    "execute_with_stats",
    "last_value",
    "promql",
    "promql_range",
    "query",
    "sql",
    "sql_async",
    // schema and catalog inspection
    "config",
    "data_dir",
    "has_measurement_in",
    "measurement_names_in",
    "schema",
    "schema_registry",
    "segment_count",
    "segments",
    "segments_of",
    "statistics",
    "disk_usage_bytes",
    "tag_keys",
    "tag_values",
    // delete
    "delete_builder",
    "delete_series",
    "drop_measurement",
    "execute_delete",
    "is_measurement_pending_drop",
    "restore_measurement",
    // maintenance
    "check_writable",
    "compact",
    "enforce_configured_retention",
    "enforce_retention",
    "flush",
    "flush_shard",
    "gc",
    "gc_pending_measurement_drops",
    "gc_tombstones",
    // rollups
    "create_rollup",
    "delete_rollup",
    "list_rollups",
    "materialise_rollups",
    "refresh_rollup",
    "rollup",
    "rollup_state",
    "rollup_where",
    // analytics
    "auto_forecast",
    "detect_anomalies",
    "fetch_series_data",
    "forecast",
    "preprocess",
    // backup
    "backup",
    "restore",
    "verify_backup",
    // interop and extension points
    "custom_udafs",
    "custom_udfs",
    "event_bus",
    "export_parquet",
    "register_udaf",
    "register_udf",
    "session_context",
    "subscribe",
    // WAL and memtable observability. `scan_memtable` answers "is this
    // point still unflushed?", which is what a durability test asks; it
    // returns owned points and holds no lock.
    "scan_memtable",
    "wal_replayed_records",
    "wal_sequence",
];

/// Read every `pub fn` / `pub async fn` declared in an `impl Chronix` block.
///
/// Scoped to those blocks on purpose: `db/stream.rs` also defines
/// `impl BatchStream`, whose methods are that type's surface rather than the
/// handle's, and the first draft of this scanner conflated the two.
fn handle_methods() -> Vec<String> {
    let mut out = Vec::new();
    for src in [
        include_str!("../src/db/accessors.rs"),
        include_str!("../src/db/analytics_api.rs"),
        include_str!("../src/db/backup.rs"),
        include_str!("../src/db/delete.rs"),
        include_str!("../src/db/lifecycle.rs"),
        include_str!("../src/db/mod.rs"),
        include_str!("../src/db/query.rs"),
        include_str!("../src/db/rollup.rs"),
        include_str!("../src/db/stream.rs"),
        include_str!("../src/db/write.rs"),
    ] {
        let mut in_chronix_impl = false;
        for line in src.lines() {
            // Blocks start at column 0, so a line with no indentation that
            // opens an `impl` ends the previous block and may open ours.
            if !line.starts_with(char::is_whitespace) && line.contains("impl ") {
                let head = line.split_once("impl").map_or("", |(_, t)| t);
                // `impl Chronix {` and `impl super::Chronix {` — half the
                // blocks spell it the second way — but not `impl BatchStream`,
                // `impl SegmentInfo`, or a trait impl like `impl Clone for
                // Chronix`, whose methods are the trait's surface.
                let head = head.trim_start().trim_start_matches("super::");
                in_chronix_impl = head.starts_with("Chronix") && !head.contains(" for ");
                continue;
            }
            if !in_chronix_impl {
                continue;
            }
            let t = line.trim();
            let Some(rest) = t
                .strip_prefix("pub async fn ")
                .or_else(|| t.strip_prefix("pub fn "))
            else {
                continue;
            };
            let name: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() {
                out.push(name);
            }
        }
    }
    out.sort();
    out.dedup();
    assert!(
        out.len() > 40,
        "the scanner found only {} methods, which means it stopped matching \
         the source layout rather than that the surface shrank",
        out.len()
    );
    out
}

#[test]
fn the_handle_surface_is_the_one_that_was_reviewed() {
    let actual = handle_methods();
    let mut expected: Vec<String> = HANDLE_SURFACE.iter().map(|s| (*s).to_string()).collect();
    expected.sort();
    expected.dedup();

    let added: Vec<_> = actual.iter().filter(|m| !expected.contains(m)).collect();
    let removed: Vec<_> = expected.iter().filter(|m| !actual.contains(m)).collect();

    assert!(
        added.is_empty() && removed.is_empty(),
        "the `Chronix` handle's surface changed.\n\
         \n  added (new public methods, not in HANDLE_SURFACE): {added:?}\
         \n  removed (in HANDLE_SURFACE, no longer public):     {removed:?}\n\
         \nIf the change is deliberate, edit `HANDLE_SURFACE` in this file in\n\
         the same commit. Before adding one, ask the question this guard exists\n\
         for: can a caller outside this crate *name* every type in the\n\
         signature, and can they hold the result across another call without\n\
         deadlocking the maintenance thread? Six methods that failed both were\n\
         deleted rather than documented."
    );
}

#[test]
fn the_handle_hands_out_no_internal_locks() {
    // The narrower question, asked structurally so it survives a rename:
    // no public method on `Chronix` may return one of the engine's ordered
    // locks. They are the crate's own hierarchy (`lock_order`), a
    // `pub(crate)` module — so such a return type is unnameable from
    // outside *and* lets a caller violate an ordering the compiler cannot
    // check for them.
    for (file, src) in [
        ("accessors.rs", include_str!("../src/db/accessors.rs")),
        ("lifecycle.rs", include_str!("../src/db/lifecycle.rs")),
        ("mod.rs", include_str!("../src/db/mod.rs")),
        ("query.rs", include_str!("../src/db/query.rs")),
        ("rollup.rs", include_str!("../src/db/rollup.rs")),
        ("stream.rs", include_str!("../src/db/stream.rs")),
        ("write.rs", include_str!("../src/db/write.rs")),
    ] {
        for (i, line) in src.lines().enumerate() {
            let t = line.trim();
            if !t.starts_with("pub fn ") && !t.starts_with("pub async fn ") {
                continue;
            }
            let Some((_, ret)) = t.split_once("->") else {
                continue;
            };
            for lock in [
                "CatalogLock",
                "BloomsLock",
                "TombstonesLock",
                "RollupRegistryLock",
            ] {
                assert!(
                    !ret.contains(lock),
                    "{file}:{} returns `{lock}`, one of this crate's ordered locks.\n\
                     A caller outside the crate cannot name it (the module is\n\
                     `pub(crate)`) and can deadlock the maintenance thread with it.\n\
                     Return an owned snapshot instead — see `Chronix::segments`.",
                    i + 1
                );
            }
        }
    }
}
