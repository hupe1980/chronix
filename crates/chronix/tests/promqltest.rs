#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code
#![allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
//! Prometheus's own `promqltest` cases, run against chronix.
//!
//! The files under `tests/promqltest/` are vendored from upstream (see
//! `UPSTREAM.md`); the expected values are Prometheus's, not ours. This is a
//! deliberately small interpreter for the subset of the format those files
//! use: `load`, `eval instant`, `eval range`, `clear`, and the `expect`
//! lines that matter for a result.
//!
//! A case that fails must be listed in [`SKIPPED`] with a reason, and a listed
//! case that passes fails the test — so the list only ever shrinks.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use chronix::prelude::*;
use chronix::promql::PromQLValue;
use chronix::{Chronix, fields};

/// `(file, line, reason)`. Each entry is an `eval` line that does not pass.
const SKIPPED: &[(&str, u32, &str)] = &[
    (
        "extended_vectors.test",
        468,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        471,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        475,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        478,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        484,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        487,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        493,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        497,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        501,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        504,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        510,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        515,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        520,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        526,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        530,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        534,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        545,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        570,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        573,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        576,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        579,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        588,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        592,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        598,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        607,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        610,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        622,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        631,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        642,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        653,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        664,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        676,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        715,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        742,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        748,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        751,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        754,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        757,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        760,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        763,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        769,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        772,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        775,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "extended_vectors.test",
        778,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "first_over_time.test",
        27,
        "block loads NaN samples, which chronix refuses at write",
    ),
    (
        "first_over_time.test",
        39,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "first_over_time.test",
        47,
        "block loads NaN samples, which chronix refuses at write",
    ),
    (
        "first_over_time.test",
        59,
        "native-histogram series are not loaded by the runner",
    ),
    (
        "first_over_time.test",
        67,
        "expects native-histogram values",
    ),
    (
        "first_over_time.test",
        79,
        "expects native-histogram values; float semantics pinned in promql_conformance.rs",
    ),
    (
        "first_over_time.test",
        91,
        "counts NaN and native-histogram samples the runner cannot load",
    ),
];

const FILES: &[&str] = &[
    "duration_expression.test",
    "extended_vectors.test",
    "first_over_time.test",
];

const MS: i64 = 1_000_000;

type Labels = Vec<(String, String)>;
/// One value per load step or expected step; `None` is a gap.
type Values = Vec<Option<f64>>;

// ── Parsing the test format ──────────────────────────────────────────────

/// A duration or timestamp such as `1m30s`, `20s500ms`, `1000s`, `0m`, or a
/// bare number of seconds. Returns nanoseconds.
fn parse_time(s: &str) -> i64 {
    let s = s.trim();
    if let Ok(secs) = s.parse::<f64>() {
        return (secs * 1e9) as i64;
    }
    let mut total_ms: i64 = 0;
    let mut num = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c.is_ascii_digit() {
            num.push(c);
            continue;
        }
        let n: i64 = num.parse().unwrap_or_else(|_| panic!("bad duration {s:?}"));
        num.clear();
        let unit_ms = match c {
            'm' if chars.peek() == Some(&'s') => {
                chars.next();
                1
            }
            's' => 1_000,
            'm' => 60_000,
            'h' => 3_600_000,
            'd' => 86_400_000,
            'w' => 604_800_000,
            'y' => 31_536_000_000,
            _ => panic!("bad duration unit in {s:?}"),
        };
        total_ms += n * unit_ms;
    }
    assert!(num.is_empty(), "duration {s:?} ends without a unit");
    total_ms * MS
}

fn parse_value(s: &str) -> Option<f64> {
    match s {
        "_" | "stale" => None,
        "NaN" => Some(f64::NAN),
        "Inf" | "+Inf" => Some(f64::INFINITY),
        "-Inf" => Some(f64::NEG_INFINITY),
        _ => Some(s.parse().unwrap_or_else(|_| panic!("bad value {s:?}"))),
    }
}

/// Expand one load token: `1`, `_`, `1+1x4`, `99-1x10`, `0x5`, `_x3`.
fn expand_token(tok: &str) -> Vec<Option<f64>> {
    let Some((head, n)) = tok.rsplit_once('x') else {
        return vec![parse_value(tok)];
    };
    let Ok(n) = n.parse::<usize>() else {
        return vec![parse_value(tok)];
    };
    // `a+b` or `a-b`, where `a` may itself carry a sign.
    let split = head
        .char_indices()
        .skip(1)
        .find(|(_, c)| *c == '+' || *c == '-')
        .map(|(i, _)| i);
    match split {
        None => vec![parse_value(head); n + 1],
        Some(i) => {
            let a = parse_value(&head[..i]).unwrap();
            let b = parse_value(&head[i..]).unwrap();
            (0..=n).map(|k| Some(a + b * k as f64)).collect()
        }
    }
}

/// `name{a="b",c="d"}`, `{a="b"}` or `name` → (name, labels).
fn parse_series(s: &str) -> (Option<String>, Vec<(String, String)>) {
    let s = s.trim();
    let (name, rest) = match s.find('{') {
        Some(i) => (&s[..i], &s[i..]),
        None => (s, ""),
    };
    let name = (!name.is_empty()).then(|| name.to_string());
    let mut labels = Vec::new();
    let inner = rest.trim_start_matches('{').trim_end_matches('}');
    for pair in inner.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap();
        labels.push((k.trim().to_string(), v.trim().trim_matches('"').to_string()));
    }
    (name, labels)
}

/// Split a load or expectation line into its series descriptor and the rest.
fn split_descriptor(line: &str) -> (&str, &str) {
    let line = line.trim();
    if let Some(close) = line.find('}') {
        let (d, rest) = line.split_at(close + 1);
        return (d, rest.trim());
    }
    match line.split_once(char::is_whitespace) {
        Some((d, rest)) => (d, rest.trim()),
        None => (line, ""),
    }
}

#[derive(Debug, Default)]
struct Expect {
    fail: Option<String>,
    /// An expected value is a native histogram, which the runner does not model.
    histogram: bool,
    /// `expect range vector from A to B step S` on an instant query.
    range_vector: Option<(i64, i64, i64)>,
    /// `None` labels = a scalar result.
    series: Vec<(Option<Labels>, Values)>,
}

#[derive(Debug)]
enum Eval {
    Instant { at: i64 },
    Range { from: i64, to: i64, step: i64 },
}

#[derive(Debug)]
enum Command {
    Load {
        interval: i64,
        series: Vec<(String, Labels, Values)>,
        /// The block loads native-histogram series, which the runner skips.
        histograms: bool,
    },
    Eval {
        line: u32,
        eval: Eval,
        expr: String,
        expect: Expect,
    },
    Clear,
}

fn parse_file(text: &str) -> Vec<Command> {
    let lines: Vec<&str> = text.lines().collect();
    let mut cmds = Vec::new();
    let mut i = 0;
    let indented = |l: &str| l.starts_with(' ') || l.starts_with('\t');
    while i < lines.len() {
        let line = lines[i].trim_end();
        let lineno = (i + 1) as u32;
        i += 1;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        // The block belonging to this command: following indented lines.
        let mut block = Vec::new();
        while i < lines.len() && (indented(lines[i]) || lines[i].trim().is_empty()) {
            if !lines[i].trim().is_empty() && !lines[i].trim().starts_with('#') {
                block.push(lines[i].trim());
            }
            if lines[i].trim().is_empty() {
                // A blank line ends a block unless more indented lines follow
                // it directly (upstream allows trailing whitespace lines).
                let next_indented = lines
                    .get(i + 1)
                    .is_some_and(|l| indented(l) && !l.trim().is_empty());
                if !next_indented {
                    i += 1;
                    break;
                }
            }
            i += 1;
        }
        if trimmed == "clear" {
            cmds.push(Command::Clear);
        } else if let Some(rest) = trimmed.strip_prefix("load ") {
            let interval = parse_time(rest);
            let mut series = Vec::new();
            let mut histograms = false;
            for b in &block {
                if b.contains("{{") {
                    histograms = true;
                    continue;
                }
                let (desc, values) = split_descriptor(b);
                let (name, labels) = parse_series(desc);
                let vals = values.split_whitespace().flat_map(expand_token).collect();
                series.push((name.unwrap(), labels, vals));
            }
            cmds.push(Command::Load {
                interval,
                series,
                histograms,
            });
        } else if let Some(rest) = trimmed.strip_prefix("eval ") {
            let (eval, expr) = if let Some(r) = rest.strip_prefix("instant at ") {
                let (at, expr) = r.split_once(' ').unwrap();
                (Eval::Instant { at: parse_time(at) }, expr)
            } else if let Some(r) = rest.strip_prefix("range from ") {
                let mut it = r.splitn(6, ' ');
                let from = parse_time(it.next().unwrap());
                assert_eq!(it.next(), Some("to"));
                let to = parse_time(it.next().unwrap());
                assert_eq!(it.next(), Some("step"));
                let step = parse_time(it.next().unwrap());
                (Eval::Range { from, to, step }, it.next().unwrap())
            } else {
                panic!("line {lineno}: unsupported eval form: {trimmed}");
            };
            let mut expect = Expect::default();
            for b in &block {
                if let Some(e) = b.strip_prefix("expect ") {
                    if let Some(msg) = e.strip_prefix("fail msg: ") {
                        expect.fail = Some(msg.to_string());
                    } else if e == "fail" {
                        expect.fail = Some(String::new());
                    } else if let Some(r) = e.strip_prefix("range vector from ") {
                        let parts: Vec<&str> = r.split_whitespace().collect();
                        expect.range_vector = Some((
                            parse_time(parts[0]),
                            parse_time(parts[2]),
                            parse_time(parts[4]),
                        ));
                    }
                    // `warn`, `info`, `no_info`: annotations are not modelled.
                    continue;
                }
                if b.contains("{{") {
                    expect.histogram = true;
                    continue;
                }
                let first = b.split_whitespace().next().unwrap();
                if parse_scalar(first).is_some() && !b.contains('{') {
                    expect.series.push((None, vec![parse_value(first)]));
                    continue;
                }
                let (desc, values) = split_descriptor(b);
                let (name, mut labels) = parse_series(desc);
                if let Some(n) = name {
                    labels.push(("__name__".into(), n));
                }
                let vals = values.split_whitespace().flat_map(expand_token).collect();
                expect.series.push((Some(labels), vals));
            }
            cmds.push(Command::Eval {
                line: lineno,
                eval,
                expr: expr.to_string(),
                expect,
            });
        } else {
            panic!("line {lineno}: unsupported command: {trimmed}");
        }
    }
    cmds
}

fn parse_scalar(s: &str) -> Option<f64> {
    matches!(s, "NaN" | "Inf" | "+Inf" | "-Inf")
        .then(|| parse_value(s).unwrap())
        .or_else(|| s.parse().ok())
}

// ── Running ──────────────────────────────────────────────────────────────

fn almost_equal(a: f64, b: f64) -> bool {
    if a.is_nan() && b.is_nan() {
        return true;
    }
    if a == b {
        return true;
    }
    let diff = (a - b).abs();
    diff <= 1e-6 * a.abs().max(b.abs()) || diff < 1e-12
}

fn sorted(mut labels: Vec<(String, String)>) -> Vec<(String, String)> {
    labels.sort();
    labels
}

/// One result series: labels → (timestamp → value).
type Got = BTreeMap<Vec<(String, String)>, BTreeMap<i64, f64>>;

fn collect(value: PromQLValue) -> Result<Got, String> {
    let series = match value {
        PromQLValue::Vector(s) | PromQLValue::Matrix(s) => s,
        PromQLValue::Scalar(v) => {
            let mut g = Got::new();
            g.entry(Vec::new()).or_default().insert(0, v);
            return Ok(g);
        }
        PromQLValue::String(s) => return Err(format!("unexpected string result {s:?}")),
    };
    let mut got = Got::new();
    for s in series {
        if s.is_histogram() {
            return Err("histogram result (not modelled by the runner)".into());
        }
        let points = got.entry(sorted(s.labels)).or_default();
        for sample in s.samples {
            points.insert(sample.timestamp, sample.value);
        }
    }
    Ok(got)
}

/// `Ok(())` when the result matches the expectation.
fn check(db: &Chronix, eval: &Eval, expr: &str, expect: &Expect) -> Result<(), String> {
    let result = match eval {
        Eval::Instant { at } => db.promql(expr, *at),
        Eval::Range { from, to, step } => db.promql_range(expr, *from, *to, *step),
    };
    if let Some(msg) = &expect.fail {
        return match result {
            Err(e) if e.to_string().contains(msg.as_str()) => Ok(()),
            Err(e) => Err(format!("failed with the wrong message: {e}")),
            Ok(v) => Err(format!("expected failure {msg:?}, got {v:?}")),
        };
    }
    let value = result.map_err(|e| format!("error: {e}"))?;
    if expect.histogram {
        return Err("expects a native-histogram value (not modelled by the runner)".into());
    }
    let got = collect(value)?;

    // The timestamps each expected value belongs to.
    let span = match (eval, expect.range_vector) {
        (_, Some(span)) => Some(span),
        (Eval::Range { from, to, step }, None) => Some((*from, *to, *step)),
        (Eval::Instant { .. }, None) => None,
    };
    let stamps: Vec<i64> = match span {
        Some((from, to, step)) => {
            let mut v = Vec::new();
            let mut t = from;
            while t <= to {
                v.push(t);
                t += step;
            }
            v
        }
        None => Vec::new(),
    };

    let mut want: BTreeMap<Vec<(String, String)>, Vec<Option<f64>>> = BTreeMap::new();
    for (labels, values) in &expect.series {
        let labels = labels.clone().map(sorted).unwrap_or_default();
        want.insert(labels, values.clone());
    }
    let got_keys: Vec<_> = got
        .iter()
        .filter(|(_, pts)| !pts.is_empty())
        .map(|(k, _)| k.clone())
        .collect();
    let want_keys: Vec<_> = want
        .iter()
        .filter(|(_, v)| v.iter().any(Option::is_some))
        .map(|(k, _)| k.clone())
        .collect();
    if got_keys != want_keys {
        return Err(format!(
            "series differ: got {got_keys:?}, want {want_keys:?}\n  got: {got:?}"
        ));
    }
    for (labels, values) in &want {
        let Some(points) = got.get(labels) else {
            continue;
        };
        if stamps.is_empty() {
            // An instant vector: exactly one value per series.
            let v = values[0].unwrap();
            let g: Vec<f64> = points.values().copied().collect();
            if g.len() != 1 || !almost_equal(g[0], v) {
                return Err(format!("{labels:?}: got {g:?}, want {v}"));
            }
            continue;
        }
        let mut msg = String::new();
        for (t, want_v) in stamps.iter().zip(values) {
            let got_v = points.get(t).copied();
            let ok = match (got_v, want_v) {
                (None, None) => true,
                (Some(g), Some(w)) => almost_equal(g, *w),
                _ => false,
            };
            if !ok {
                let _ = write!(
                    msg,
                    " t={}s got {got_v:?} want {want_v:?};",
                    t / 1_000_000_000
                );
            }
        }
        if points.len() != values.iter().filter(|v| v.is_some()).count() {
            let _ = write!(
                msg,
                " {} points returned for {} expected",
                points.len(),
                values.len()
            );
        }
        if !msg.is_empty() {
            return Err(format!("{labels:?}:{msg}"));
        }
    }
    Ok(())
}

fn fresh_db() -> (tempfile::TempDir, Chronix) {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open_small(dir.path()).unwrap();
    (dir, db)
}

#[test]
fn upstream_promqltest_cases() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/promqltest");
    let mut failures: Vec<(String, u32, String)> = Vec::new();
    let mut passed = 0usize;
    let mut skipped_and_failing = Vec::new();

    let mut per_file = Vec::new();
    for file in FILES {
        let (passed_before, skipped_before) = (passed, skipped_and_failing.len());
        let failed_before = failures.len();
        let text = std::fs::read_to_string(root.join(file)).unwrap();
        let (mut dir, mut db) = fresh_db();
        let mut loaded_histograms = false;
        let mut loaded_nonfinite = false;
        for cmd in parse_file(&text) {
            match cmd {
                Command::Clear => {
                    loaded_histograms = false;
                    loaded_nonfinite = false;
                    db.close().unwrap();
                    drop(dir);
                    (dir, db) = fresh_db();
                }
                Command::Load {
                    interval,
                    series,
                    histograms,
                } => {
                    loaded_histograms |= histograms;
                    let mut points = Vec::new();
                    for (name, labels, values) in series {
                        let key = SeriesKey::new(&name, labels.into_iter().collect()).unwrap();
                        for (i, v) in values.into_iter().enumerate() {
                            // chronix refuses non-finite floats at write; such
                            // a sample cannot be loaded, and the block is noted.
                            if let Some(v) = v.filter(|v| !v.is_finite()) {
                                let _ = v;
                                loaded_nonfinite = true;
                                continue;
                            }
                            if let Some(v) = v {
                                points.push(
                                    Point::new(
                                        key.clone(),
                                        fields! { "value" => v },
                                        i as i64 * interval,
                                    )
                                    .unwrap(),
                                );
                            }
                        }
                    }
                    if !points.is_empty() {
                        db.insert_batch(&points).unwrap().into_complete().unwrap();
                    }
                }
                Command::Eval {
                    line,
                    eval,
                    expr,
                    expect,
                } => {
                    let skipped = SKIPPED.iter().find(|(f, l, _)| f == file && *l == line);
                    match (check(&db, &eval, &expr, &expect), skipped) {
                        (Ok(()), None) => passed += 1,
                        (Ok(()), Some(_)) => failures.push((
                            (*file).to_string(),
                            line,
                            format!("passes now — remove it from SKIPPED: {expr}"),
                        )),
                        (Err(e), None) => {
                            let note = match (loaded_histograms, loaded_nonfinite) {
                                (true, _) => " [block loads native histograms]",
                                (false, true) => " [block loads NaN/Inf, which chronix refuses]",
                                _ => "",
                            };
                            failures.push((
                                (*file).to_string(),
                                line,
                                format!("{expr}{note}\n    {e}"),
                            ));
                        }
                        (Err(_), Some(_)) => skipped_and_failing.push(((*file).to_string(), line)),
                    }
                }
            }
        }
        db.close().unwrap();
        drop(dir);
        per_file.push(format!(
            "{file}: {} passed, {} skipped (listed), {} failed",
            passed - passed_before,
            skipped_and_failing.len() - skipped_before,
            failures.len() - failed_before
        ));
    }
    for line in &per_file {
        eprintln!("{line}");
    }

    for (_, _, reason) in SKIPPED {
        assert!(!reason.is_empty(), "every SKIPPED entry names its reason");
    }
    if !failures.is_empty() {
        let mut report = String::new();
        for (f, l, e) in &failures {
            let _ = writeln!(report, "{f}:{l}: {e}");
        }
        panic!(
            "{} upstream case(s) do not match ({passed} pass, {} skipped):\n{report}",
            failures.len(),
            skipped_and_failing.len()
        );
    }
    eprintln!(
        "{passed} upstream cases pass, {} skipped",
        skipped_and_failing.len()
    );
}
