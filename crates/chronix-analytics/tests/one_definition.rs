//! A numeric primitive has exactly one implementation in this crate.
//!
//! "Two implementations of one semantic diverge silently" is this project's
//! most productive bug class, and it has now cost twice in the special
//! functions specifically.
//!
//! Once already: `chi_squared_survival`'s continued fraction carried the incomplete
//! **beta** function's numerators against the incomplete **gamma**'s
//! denominators, and the correct algorithm was sitting in a sibling module.
//! The p-value came out 11 % wrong.
//!
//! And again, in the module that fix pointed at: `multivariate::mv_forecast`
//! kept its own `ln_gamma` with the g = 7 Lanczos coefficients and a `t = x +
//! 6.5` offset — g = 6. Mismatched parameters are not a precision problem.
//! That copy returned **0.928** for `ln Γ(1)`, which is 0, and it sat under
//! `ln_beta` → `regularized_incomplete_beta` → `f_distribution_sf`, so every
//! Granger causality p-value the crate produced was wrong. The correct
//! `ln_gamma` was in `forecast::diagnostics`, pinned by a known-values test,
//! one module away.
//!
//! Neither was found by reading. The first needed the two read against each
//! other; the second needed a closed form. What finds them cheaply is asking
//! how many definitions there are.

/// Functions whose value is a mathematical constant of their inputs, so a
/// second implementation can only agree or be wrong.
const SINGLE_DEFINITION: &[&str] = &[
    "ln_gamma",
    "ln_beta",
    "regularized_incomplete_beta",
    "regularized_upper_gamma",
    "chi_squared_survival",
    "f_distribution_sf",
    "normal_quantile",
    "erf",
    "erfc",
];

fn crate_sources() -> Vec<(std::path::PathBuf, String)> {
    fn walk(dir: &std::path::Path, out: &mut Vec<(std::path::PathBuf, String)>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|x| x == "rs")
                && let Ok(t) = std::fs::read_to_string(&p)
            {
                out.push((p, t));
            }
        }
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut out = Vec::new();
    walk(&root, &mut out);
    assert!(!out.is_empty(), "found no sources under {}", root.display());
    out
}

#[test]
fn every_numeric_primitive_has_exactly_one_definition() {
    let sources = crate_sources();
    let mut offenders = Vec::new();

    for name in SINGLE_DEFINITION {
        let mut sites = Vec::new();
        for (path, text) in &sources {
            for (i, line) in text.lines().enumerate() {
                let t = line.trim_start();
                // A definition, not a call: `fn name(` at the start of the
                // item, with or without visibility. Test modules count —
                // a copy in a test is still a second implementation to
                // maintain, and is how one of these started.
                for prefix in ["fn ", "pub fn ", "pub(crate) fn ", "pub(super) fn "] {
                    if let Some(rest) = t.strip_prefix(prefix)
                        && (rest.starts_with(&format!("{name}("))
                            || rest.starts_with(&format!("{name}<")))
                    {
                        sites.push(format!("{}:{}", path.display(), i + 1));
                    }
                }
            }
        }
        if sites.len() > 1 {
            offenders.push(format!(
                "`{name}` is defined {} times:\n      {}",
                sites.len(),
                sites.join("\n      ")
            ));
        }
    }

    assert!(
        offenders.is_empty(),
        "a numeric primitive has more than one implementation in this crate. \
         Two of these have already shipped wrong, each with a correct sibling \
         one module away — a second implementation of a mathematical constant \
         can only agree or be wrong, and nothing makes it agree:\n\n  {}",
        offenders.join("\n  ")
    );
}

#[test]
fn the_scan_can_see_a_definition() {
    // Rule 29: prove the guard can fail. If the source layout changes so that
    // this scanner matches nothing, the test above passes vacuously.
    let sources = crate_sources();
    let found = sources.iter().any(|(_, t)| {
        t.lines().any(|l| {
            let t = l.trim_start();
            t.starts_with("fn ln_gamma(") || t.starts_with("pub(crate) fn ln_gamma(")
        })
    });
    assert!(
        found,
        "the scanner found no `ln_gamma` definition at all — it has stopped \
         matching the source layout rather than proving anything"
    );
}
