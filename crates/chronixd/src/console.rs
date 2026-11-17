//! The console: one static page compiled into the binary.
//!
//! `GET /console` serves the shell and `GET /console/{file}` its assets, all
//! from the table below. The page holds no data — every number it shows comes
//! from a route other clients use, through the same authentication, namespace
//! and policy checks — so the shell is identical for every caller and needs no
//! credential to load. A browser navigation cannot send a bearer token anyway.
//!
//! Each response carries a strict Content-Security-Policy (no inline script or
//! style, same-origin connections only), `nosniff`, and an `ETag`. Assets are
//! gzip-compressed once, on first request, when the client accepts it.

use std::sync::OnceLock;

use axum::extract::Path;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::error::ServerError;

/// The path the shell is served at, printed by `chronixd --demo`.
pub const PATH: &str = "/console";

/// The policy every console response carries.
pub const CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self'; \
img-src 'self' data:; connect-src 'self'; base-uri 'none'; form-action 'none'; \
frame-ancestors 'none'";

struct Asset {
    name: &'static str,
    content_type: &'static str,
    bytes: &'static [u8],
}

const ASSETS: &[Asset] = &[
    Asset {
        name: "index.html",
        content_type: "text/html; charset=utf-8",
        bytes: include_bytes!("../console/index.html"),
    },
    Asset {
        name: "app.css",
        content_type: "text/css; charset=utf-8",
        bytes: include_bytes!("../console/app.css"),
    },
    Asset {
        name: "app.js",
        content_type: "text/javascript; charset=utf-8",
        bytes: include_bytes!("../console/app.js"),
    },
    Asset {
        name: "uplot.min.js",
        content_type: "text/javascript; charset=utf-8",
        bytes: include_bytes!("../console/uplot.min.js"),
    },
    Asset {
        name: "uplot.min.css",
        content_type: "text/css; charset=utf-8",
        bytes: include_bytes!("../console/uplot.min.css"),
    },
    Asset {
        name: "snippets.json",
        content_type: "application/json",
        bytes: include_bytes!("../console/snippets.json"),
    },
    Asset {
        name: "LICENSE-uPlot",
        content_type: "text/plain; charset=utf-8",
        bytes: include_bytes!("../console/LICENSE-uPlot"),
    },
];

/// Per-asset values computed once: the entity tag and the gzip body.
struct Prepared {
    etag: String,
    gzip: OnceLock<Vec<u8>>,
}

fn prepared() -> &'static [Prepared] {
    static PREPARED: OnceLock<Vec<Prepared>> = OnceLock::new();
    PREPARED.get_or_init(|| {
        ASSETS
            .iter()
            .map(|a| Prepared {
                etag: format!("\"{:016x}\"", fnv1a(a.bytes)),
                gzip: OnceLock::new(),
            })
            .collect()
    })
}

/// FNV-1a, 64 bit: a stable content hash for the `ETag`, not a security
/// primitive.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3)
    })
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
    // Writing to a `Vec` cannot fail.
    let _ = enc.write_all(bytes);
    enc.finish().unwrap_or_default()
}

fn accepts_gzip(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::ACCEPT_ENCODING)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|enc| {
            let mut parts = enc.split(';');
            let name = parts.next().unwrap_or("").trim();
            let refused = parts.any(|p| {
                p.trim()
                    .strip_prefix("q=")
                    .is_some_and(|q| q.trim().parse::<f32>().is_ok_and(|q| q == 0.0))
            });
            name.eq_ignore_ascii_case("gzip") && !refused
        })
}

/// Every embedded asset name, for tests and the size budget.
#[must_use]
pub fn asset_names() -> Vec<&'static str> {
    ASSETS.iter().map(|a| a.name).collect()
}

/// The bytes of an embedded asset, for tests.
#[must_use]
pub fn asset_bytes(name: &str) -> Option<&'static [u8]> {
    ASSETS.iter().find(|a| a.name == name).map(|a| a.bytes)
}

fn serve(name: &str, headers: &HeaderMap) -> Response {
    let Some(idx) = ASSETS.iter().position(|a| a.name == name) else {
        return ServerError::NotFound(format!("the console has no file named \"{name}\""))
            .into_response();
    };
    let asset = &ASSETS[idx];
    let prep = &prepared()[idx];

    let mut out = HeaderMap::new();
    out.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(asset.content_type),
    );
    out.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    out.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CSP),
    );
    out.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    out.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    out.insert(header::VARY, HeaderValue::from_static("accept-encoding"));
    if let Ok(etag) = HeaderValue::from_str(&prep.etag) {
        out.insert(header::ETAG, etag);
    }

    let matches = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(',')
                .any(|t| t.trim() == prep.etag || t.trim() == "*")
        });
    if matches {
        return (StatusCode::NOT_MODIFIED, out).into_response();
    }

    if accepts_gzip(headers) {
        let body = prep.gzip.get_or_init(|| gzip(asset.bytes)).clone();
        out.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        return (StatusCode::OK, out, body).into_response();
    }
    (StatusCode::OK, out, asset.bytes).into_response()
}

/// `GET /console` — the shell.
pub async fn shell(headers: HeaderMap) -> Response {
    serve("index.html", &headers)
}

/// `GET /console/` — the shell lives at `/console`; its asset paths are
/// relative to that, so the trailing-slash form redirects rather than
/// serving a page whose stylesheet would not resolve.
pub async fn slash() -> Response {
    (
        StatusCode::PERMANENT_REDIRECT,
        [(header::LOCATION, HeaderValue::from_static("../console"))],
    )
        .into_response()
}

/// `GET /console/{file}` — an asset of the shell.
pub async fn asset(Path(file): Path<String>, headers: HeaderMap) -> Response {
    serve(&file, &headers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gzip_is_accepted_unless_refused() {
        let h = |v: &'static str| {
            let mut m = HeaderMap::new();
            m.insert(header::ACCEPT_ENCODING, HeaderValue::from_static(v));
            m
        };
        assert!(accepts_gzip(&h("gzip, deflate, br")));
        assert!(accepts_gzip(&h("br;q=1.0, gzip;q=0.8")));
        assert!(!accepts_gzip(&h("gzip;q=0")));
        assert!(!accepts_gzip(&h("br")));
        assert!(!accepts_gzip(&HeaderMap::new()));
    }

    #[test]
    fn every_asset_is_named_once() {
        let mut names = asset_names();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), ASSETS.len());
    }
}
