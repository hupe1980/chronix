//! The router's own route table, read out of `server.rs`.
//!
//! axum does not expose the routes it mounts, and a list maintained beside
//! the router is a second inventory that drifts from the first. Two test
//! binaries need it — one checks every route is documented, the other that
//! every route is behind an authorization gate — so it lives here and both
//! `#[path]`-include it rather than keeping a copy each.
//!
//! The scan is deliberately strict: a `.route(` whose path is not a literal
//! will not be seen, so it must not exist.

use std::collections::BTreeSet;

const SERVER_SRC: &str = include_str!("../../src/server.rs");

/// Paths mounted by `build_router`, with `nest` prefixes applied.
pub fn mounted_paths() -> BTreeSet<String> {
    let body = {
        let start = SERVER_SRC
            .find("pub fn build_router(")
            .expect("build_router");
        &SERVER_SRC[start..]
    };

    let mut out = BTreeSet::new();
    // (prefix, brace depth at which the nest block opened)
    let mut nests: Vec<(String, i32)> = Vec::new();
    let mut depth = 0i32;
    for (idx, ch) in body.char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                while nests.last().is_some_and(|(_, d)| *d > depth) {
                    nests.pop();
                }
                if depth <= 0 {
                    break;
                }
            }
            '.' => {
                let rest = &body[idx..];
                if let Some(path) = literal_after(rest, ".route(") {
                    let prefix: String = nests.iter().map(|(p, _)| p.as_str()).collect();
                    let full = if path == "/" && !prefix.is_empty() {
                        prefix
                    } else {
                        format!("{prefix}{path}")
                    };
                    out.insert(full);
                } else if let Some(prefix) = literal_after(rest, ".nest(") {
                    // The block opens on the next `{`; record the depth it
                    // will be at so the prefix pops with it.
                    nests.push((prefix.to_string(), depth + 1));
                }
            }
            _ => {}
        }
    }
    out
}

/// The string literal that follows `call` at the start of `rest`, if any.
fn literal_after<'a>(rest: &'a str, call: &str) -> Option<&'a str> {
    let after = rest.strip_prefix(call)?;
    let after = after.trim_start();
    let after = after.strip_prefix('"')?;
    let end = after.find('"')?;
    Some(&after[..end])
}
