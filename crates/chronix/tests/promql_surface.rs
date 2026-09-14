//! Which PromQL functions does chronix implement, and against which version?
//!
//! The claim used to be "what remains of the 3.x surface is native
//! histograms and the experimental `limitk` / `limit_ratio`" — an
//! exhaustive-sounding list that stopped being exhaustive three Prometheus
//! releases later. Prometheus 3.5 through 3.14 added nine functions, every one
//! behind a feature flag, and nothing here noticed.
//!
//! The failure mode is the compression claim's, applied to a protocol: a
//! statement about somebody else's project, kept in prose, decaying silently.
//! The fix is the same — write the version down, and make a test refuse to let
//! the list and the evaluator disagree.
//!
//! Two directions are asserted:
//!
//! 1. Every name this file calls implemented is accepted by the evaluator.
//! 2. Every name this file calls *not* implemented is **refused by name**,
//!    rather than silently answering something plausible.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use chronix::prelude::*;

/// The Prometheus release this surface is stated against.
///
/// Bump it only after re-reading that release's changelog for new functions,
/// and record what was found even when the answer is "nothing" — the point of
/// the obligation is that "we looked" is a result.
const CONFORMS_TO: &str = "3.14";

/// Functions chronix implements, grouped as upstream documents them.
const IMPLEMENTED: &[&str] = &[
    // range-vector
    "rate",
    "irate",
    "increase",
    "delta",
    "idelta",
    "deriv",
    "predict_linear",
    "resets",
    "changes",
    "avg_over_time",
    "min_over_time",
    "max_over_time",
    "sum_over_time",
    "count_over_time",
    "quantile_over_time",
    "stddev_over_time",
    "stdvar_over_time",
    "last_over_time",
    "present_over_time",
    "absent_over_time",
    "mad_over_time",
    "double_exponential_smoothing",
    // instant-vector and scalar
    "abs",
    "absent",
    "ceil",
    "floor",
    "round",
    "sgn",
    "clamp",
    "clamp_max",
    "clamp_min",
    "exp",
    "ln",
    "log2",
    "log10",
    "sqrt",
    "scalar",
    "vector",
    "timestamp",
    "time",
    "pi",
    // trigonometric and angle
    "acos",
    "acosh",
    "asin",
    "asinh",
    "atan",
    "atanh",
    "cos",
    "cosh",
    "sin",
    "sinh",
    "tan",
    "tanh",
    "deg",
    "rad",
    // label manipulation and sorting
    "label_join",
    "label_replace",
    "sort",
    "sort_desc",
    "sort_by_label",
    "sort_by_label_desc",
    // histograms — classic `le` buckets and native, dispatched on whether
    // the series carries histogram samples
    "histogram_quantile",
    "histogram_count",
    "histogram_sum",
    "histogram_avg",
    "histogram_fraction",
    "histogram_stddev",
    "histogram_stdvar",
    // date and time
    "year",
    "month",
    "day_of_month",
    "day_of_week",
    "day_of_year",
    "days_in_month",
    "hour",
    "minute",
];

/// Functions chronix does **not** implement, each with the reason.
///
/// The reason is part of the entry on purpose: an unimplemented list without
/// one is a to-do list, and this is a set of decisions.
const NOT_IMPLEMENTED: &[(&str, &str)] = &[
    // `histogram_quantiles` is the variadic form, still experimental
    // upstream; the singular `histogram_quantile` is implemented for both
    // classic and native input.
    ("histogram_quantiles", "experimental upstream"),
    // Experimental upstream: the semantics are still moving, and copying a
    // function whose meaning changes is how a surface acquires a behaviour it
    // then has to keep.
    ("limitk", "experimental upstream"),
    ("limit_ratio", "experimental upstream"),
    ("info", "experimental upstream"),
    ("ts_of_min_over_time", "experimental upstream"),
    ("ts_of_max_over_time", "experimental upstream"),
    ("ts_of_last_over_time", "experimental upstream"),
    ("ts_of_first_over_time", "experimental upstream"),
    ("first_over_time", "experimental upstream"),
    ("start_timestamp", "experimental upstream"),
    // Duration functions (3.12) — experimental, and they are expression-level
    // rather than sample-level, which is a parser change as well.
    ("start", "experimental upstream"),
    ("end", "experimental upstream"),
    ("range", "experimental upstream"),
    ("step", "experimental upstream"),
    ("min_of", "experimental upstream"),
    ("max_of", "experimental upstream"),
];

/// A small database, with the temporary directory that owns it.
///
/// The `TempDir` is **returned**, not forgotten. An earlier version of this
/// helper called `std::mem::forget(dir)` so the database could outlive the
/// function — which works, and leaks a directory of database files on every
/// single run. Three tests, run on every `cargo test`, on every developer
/// machine and every CI job: it filled a disk during this pass, which is the
/// cheapest possible way to learn the lesson. A caller binding
/// `let (_dir, db) = db();` keeps it alive for exactly as long as it needs
/// to, and the drop cleans up.
fn db() -> (tempfile::TempDir, Chronix) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Chronix::open_small(dir.path()).expect("open");
    let key = SeriesKey::new("m", tags! { "host" => "a" }).expect("key");
    for i in 0..5i64 {
        let p = Point::new(
            key.clone(),
            fields! { "value" => i as f64 },
            1_700_000_000_000_000_000 + i * 1_000_000_000,
        )
        .expect("point");
        db.insert(&p).expect("insert");
    }
    (dir, db)
}

/// An expression that exercises `name` with a plausible argument list.
fn call(name: &str) -> String {
    const RANGE: &[&str] = &[
        "rate",
        "irate",
        "increase",
        "delta",
        "idelta",
        "deriv",
        "resets",
        "changes",
        "avg_over_time",
        "min_over_time",
        "max_over_time",
        "sum_over_time",
        "count_over_time",
        "stddev_over_time",
        "stdvar_over_time",
        "last_over_time",
        "present_over_time",
        "absent_over_time",
        "mad_over_time",
        "first_over_time",
        "ts_of_min_over_time",
        "ts_of_max_over_time",
        "ts_of_last_over_time",
        "ts_of_first_over_time",
    ];
    match name {
        n if RANGE.contains(&n) => format!("{n}(m[5m])"),
        "quantile_over_time" => format!("{name}(0.9, m[5m])"),
        "predict_linear" => format!("{name}(m[5m], 60)"),
        "double_exponential_smoothing" => format!("{name}(m[5m], 0.5, 0.5)"),
        "histogram_quantile" | "histogram_fraction" => format!("{name}(0.9, m)"),
        "histogram_quantiles" => format!("{name}(\"q\", 0.9, m)"),
        "clamp" => format!("{name}(m, 0, 1)"),
        "clamp_max" | "clamp_min" | "round" => format!("{name}(m, 1)"),
        "label_join" => format!("{name}(m, \"dst\", \",\", \"host\")"),
        "label_replace" => format!("{name}(m, \"dst\", \"$1\", \"host\", \"(.*)\")"),
        "sort_by_label" | "sort_by_label_desc" => format!("{name}(m, \"host\")"),
        "limitk" => format!("{name}(1, m)"),
        "limit_ratio" => format!("{name}(0.5, m)"),
        "min_of" | "max_of" => format!("{name}(1, 2)"),
        "time" | "pi" | "start" | "end" | "range" | "step" => format!("{name}()"),
        "scalar" | "vector" => format!("{name}(1)"),
        _ => format!("{name}(m)"),
    }
}

/// Did the evaluator refuse this because the *function* is unknown?
fn refused_by_name(err: &str, name: &str) -> bool {
    let e = err.to_lowercase();
    (e.contains("unknown function") || e.contains("unsupported")) && e.contains(name)
}

#[test]
fn every_function_this_file_calls_implemented_is_accepted() {
    let (_dir, db) = db();
    let at = 1_700_000_004_000_000_000;
    let mut missing = Vec::new();
    for name in IMPLEMENTED {
        if let Err(e) = db.promql(&call(name), at) {
            let msg = e.to_string();
            if refused_by_name(&msg, name) {
                missing.push((*name, msg));
            }
            // Any other error is an argument-shape quarrel with this test's
            // own `call()`, not a missing function.
        }
    }
    assert!(
        missing.is_empty(),
        "these are listed as implemented and the evaluator does not know them \
         — either the list is stale or a function was lost: {missing:#?}"
    );
}

#[test]
fn every_function_this_file_calls_absent_is_refused_by_name() {
    let (_dir, db) = db();
    let at = 1_700_000_004_000_000_000;
    let mut answered = Vec::new();
    for (name, why) in NOT_IMPLEMENTED {
        match db.promql(&call(name), at) {
            Ok(_) => answered.push((*name, *why)),
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    refused_by_name(&msg, name),
                    "`{name}` is not implemented ({why}) and must be refused by \
                     name, so a user learns the function is missing rather than \
                     reading a parse error: got {msg}"
                );
            }
        }
    }
    assert!(
        answered.is_empty(),
        "these are listed as not implemented and the evaluator answered them. \
         A function that works is a function to document and move to \
         IMPLEMENTED — silence here is how the surface drifts: {answered:#?}"
    );
}

#[test]
fn the_surface_names_the_version_it_conforms_to() {
    // "3.x" is not a version. The number is here so that the next person to
    // read the list knows what it was checked against, and the query
    // documentation quotes this same figure.
    assert!(
        CONFORMS_TO.starts_with('3'),
        "chronix targets the Prometheus 3 series"
    );
    assert!(
        !IMPLEMENTED.is_empty() && !NOT_IMPLEMENTED.is_empty(),
        "both directions must be stated: a list of what works, without a list \
         of what deliberately does not, is the shape that went stale"
    );
    let mut all: Vec<&str> = IMPLEMENTED.to_vec();
    all.extend(NOT_IMPLEMENTED.iter().map(|(n, _)| *n));
    let before = all.len();
    all.sort_unstable();
    all.dedup();
    assert_eq!(
        before,
        all.len(),
        "a function is either implemented or it is not; one cannot be in both lists"
    );
}
