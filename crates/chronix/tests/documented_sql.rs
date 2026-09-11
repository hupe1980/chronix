#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap
//! Every SQL statement the documentation shows must plan.
//!
//! Documentation drifts silently: nothing runs it, so a query keeps its
//! place on the page long after the column, the function or the whole
//! syntax it uses has gone. This tree had six such blocks at once — a
//! `time` column that is spelled `_time`, an `ORDER BY timestamp`, a
//! `_segments` table that never existed, `FILL`/`RESAMPLE`/`SMOOTH`
//! functions that were never written, and a trigger DSL with `ACTION` and
//! `SEVERITY` clauses the parser does not accept.
//!
//! Planning is the right bar. It catches an unknown column, function or
//! table without the test having to assert what the query *means* — which
//! would make every doc example a second copy of a semantics test.

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};
use std::sync::Arc;

/// Measurements and columns the documented examples use.
///
/// A doc example naming something outside this set fails, which is the
/// point: either the example is wrong, or the fixture should grow with it.
fn fixture(dir: &tempfile::TempDir) -> Arc<Chronix> {
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());

    // The docs' canonical exact-decimal measurement: a meter register whose
    // value a bill is computed from. Declared rather than inferred, so its
    // scale is the one the documentation says it is.
    db.declare_field(
        "meter",
        "reading",
        chronix_core::ColumnType::Decimal { scale: 4 },
    )
    .unwrap();

    let now = 1_700_000_000_000_000_000i64;
    let insert = |measurement: &str, tags, fields, ts: i64| {
        db.insert(&Point::new(SeriesKey::new(measurement, tags).unwrap(), fields, ts).unwrap())
            .unwrap();
    };

    for i in 0..3i64 {
        let ts = now + i * 60_000_000_000;
        insert(
            "cpu",
            tags! { "host" => "web-01" },
            fields! { "usage" => 1.0, "usage_idle" => 99.0, "value" => 1.0 },
            ts,
        );
        insert(
            "cpu_usage",
            tags! { "host" => "web-01" },
            fields! { "usage" => 1.0, "value" => 1.0 },
            ts,
        );
        insert(
            "metrics",
            tags! { "metric_name" => "cpu" },
            fields! { "value" => 1.0 },
            ts,
        );
        insert(
            "temp",
            tags! { "site" => "a" },
            fields! { "value" => 1.0 },
            ts,
        );
        insert(
            "meter",
            tags! { "device" => "main" },
            fields! { "reading" => "1234.5678".parse::<chronix_core::Decimal>().unwrap() },
            ts,
        );
    }
    db
}

/// Every ```sql fenced block in the published documentation.
fn documented_blocks() -> Vec<(String, String)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("repo root");

    let mut blocks = Vec::new();
    let mut stack = vec![root.join("site").join("content")];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("readable docs directory") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().is_none_or(|e| e != "md") {
                continue;
            }
            let text = std::fs::read_to_string(&path).expect("readable markdown");
            let name = path
                .strip_prefix(&root)
                .unwrap_or(&path)
                .display()
                .to_string();
            for (i, block) in fenced_sql(&text).into_iter().enumerate() {
                blocks.push((format!("{name} [{i}]"), block));
            }
        }
    }
    blocks.sort();
    blocks
}

/// The contents of every ```sql fence in `text`.
fn fenced_sql(text: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut current: Option<String> = None;
    for line in text.lines() {
        match current {
            None if line.trim_start().starts_with("```sql") => current = Some(String::new()),
            None => {}
            Some(_) if line.trim_start() == "```" => {
                blocks.push(current.take().unwrap_or_default());
            }
            Some(ref mut body) => {
                body.push_str(line);
                body.push('\n');
            }
        }
    }
    blocks
}

/// Split a block into statements, dropping comments and empty ones.
///
/// Comments go **before** the split, because a `;` inside one is not a
/// statement terminator — a prose sentence in a `--` line otherwise cuts the
/// query that follows it in half.
fn statements(block: &str) -> Vec<String> {
    let without_comments: String = block
        .lines()
        .filter(|l| !l.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");

    // A `;` inside a string literal is not a terminator either.
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    for c in without_comments.chars() {
        match c {
            '\'' => {
                in_string = !in_string;
                current.push(c);
            }
            ';' if !in_string => {
                statements.push(std::mem::take(&mut current));
            }
            _ => current.push(c),
        }
    }
    statements.push(current);

    statements
        .into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Whether a statement belongs to the trigger DSL rather than to DataFusion.
fn is_trigger_dsl(sql: &str) -> bool {
    let head = sql.trim_start().to_ascii_uppercase();
    head.starts_with("CREATE TRIGGER")
        || head.starts_with("DROP TRIGGER")
        || head.starts_with("ALTER TRIGGER")
        || head.starts_with("SHOW TRIGGERS")
}

#[test]
fn every_documented_sql_statement_plans() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let ctx = chronix::sql::create_session_context(db.clone());
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let blocks = documented_blocks();
    assert!(
        blocks.len() >= 10,
        "the extractor found only {} blocks, so it is not working",
        blocks.len()
    );

    let mut failures: Vec<String> = Vec::new();
    let mut checked = 0usize;
    for (origin, block) in blocks {
        for sql in statements(&block) {
            checked += 1;
            let result = if is_trigger_dsl(&sql) {
                chronix::chronix_streaming::signal::parse_trigger_sql(&sql)
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            } else {
                rt.block_on(chronix::sql::plan_read_only(&ctx, &sql))
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            };
            if let Err(e) = result {
                let reason = e.lines().last().unwrap_or(&e).trim().to_string();
                failures.push(format!(
                    "{origin}\n    {}\n    -> {reason}",
                    sql.replace('\n', "\n    ")
                ));
            }
        }
    }

    assert!(
        checked >= 15,
        "only {checked} statements were checked, so the splitter is not working"
    );
    assert!(
        failures.is_empty(),
        "documented SQL that does not plan — either the example is wrong or \
         the fixture must grow with it:\n\n{}",
        failures.join("\n\n")
    );
}
