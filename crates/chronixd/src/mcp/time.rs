//! Time arguments as an agent writes them.
//!
//! `now`, `now-7d` and `now-90m` because an agent composes relative ranges;
//! RFC 3339 and Unix seconds because those are what it copies out of a
//! result. One parser, so every tool accepts the same forms.

use crate::error::ServerError;
use crate::http::{parse_duration_ns, parse_time_ns};

/// Nanoseconds since the epoch for `spec`, relative to `now_ns`.
///
/// # Errors
///
/// A value in none of the accepted forms; the message names them.
pub fn parse(key: &str, spec: &str, now_ns: i64) -> Result<i64, ServerError> {
    let spec = spec.trim();
    if spec == "now" {
        return Ok(now_ns);
    }
    if let Some(rest) = spec.strip_prefix("now-") {
        let back = parse_duration_ns(key, rest).map_err(|_| invalid(key, spec))?;
        return Ok(now_ns.saturating_sub(back));
    }
    parse_time_ns(key, spec).map_err(|_| invalid(key, spec))
}

fn invalid(key: &str, spec: &str) -> ServerError {
    ServerError::BadRequest(format!(
        "{key} {spec:?} is not a time; use \"now\", \"now-7d\", RFC 3339 \
         (\"2026-11-17T08:00:00Z\") or Unix seconds"
    ))
}

#[cfg(test)]
mod tests {
    use super::parse;

    const NOW: i64 = 1_800_000_000_000_000_000;
    const SEC: i64 = 1_000_000_000;

    #[test]
    fn accepts_every_documented_form() {
        assert_eq!(parse("t", "now", NOW).unwrap(), NOW);
        assert_eq!(parse("t", "now-7d", NOW).unwrap(), NOW - 7 * 86_400 * SEC);
        assert_eq!(parse("t", "now-90m", NOW).unwrap(), NOW - 90 * 60 * SEC);
        assert_eq!(parse("t", "1700000000", NOW).unwrap(), 1_700_000_000 * SEC);
        assert_eq!(
            parse("t", "2023-11-14T22:13:20Z", NOW).unwrap(),
            1_700_000_000 * SEC
        );
    }

    #[test]
    fn refuses_anything_else_by_naming_the_forms() {
        let err = parse("start", "yesterday", NOW).unwrap_err().to_string();
        assert!(err.contains("now-7d") && err.contains("RFC 3339"), "{err}");
        assert!(parse("start", "now-", NOW).is_err());
    }
}
