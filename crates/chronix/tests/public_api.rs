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
    "mod analytics",
    "mod db",
    "mod delete",
    "mod error",
    "mod export",
    "mod macros",
    "mod pipeline",
    "mod prelude",
    "mod promql",
    "mod retention",
    "mod rollup",
    // ── Re-exported engine crates (tier 3) ─────────────────────────
    "mod chronix_analytics",
    "mod chronix_core",
    "mod chronix_encoding",
    "mod chronix_engine",
    "mod chronix_query",
    "mod chronix_security",
    "mod chronix_streaming",
    // ── Types and macros at the crate root (tier 1) ────────────────
    "AnomalyConfig",
    "BackupManifest",
    "Chronix",
    "DatabaseStatistics",
    "DbError",
    "DeleteBuilder",
    "DeleteOutcome",
    "DeleteRequest",
    "ForecastConfig",
    "InsertResult",
    "ParquetCompression",
    "ParquetExportConfig",
    "Pipeline",
    "PipelineConfig",
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
    ("object-store", &["mod cold_archive"]),
    // On by default; `default-features = false` drops DataFusion and with it
    // `db.sql()`, `session_context()` and the analytics SQL functions. The
    // rest of the surface — writes, the native query API, PromQL, rollups,
    // retention, triggers — is unchanged.
    ("sql", &["mod sql"]),
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

        for name in names {
            match pending_feature.take() {
                Some(f) => gated.push((f, name)),
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
