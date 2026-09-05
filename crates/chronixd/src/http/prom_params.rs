//! Parsing the Prometheus HTTP API's parameters the way its clients send
//! them.
//!
//! Two things about this API are easy to get wrong from the specification
//! alone, and chronix got both wrong:
//!
//! 1. **Grafana POSTs.** Its Prometheus datasource defaults to
//!    `httpMethod: POST` and sends `application/x-www-form-urlencoded`, so a
//!    handler that only reads the query string rejects every panel with a
//!    400 before any of this code runs. [`PromParams`] is an extractor that
//!    accepts either.
//! 2. **`time`, `start` and `end` are "RFC 3339 **or** a Unix timestamp"**,
//!    and `step` is "a duration **or** a number of seconds". Prometheus's own
//!    `parseTime`/`parseDuration` accept both spellings, and clients use both
//!    — Grafana sends floats, humans and `curl` examples send RFC 3339 and
//!    `15s`.
//!
//! Timestamps are rounded to milliseconds, as Prometheus's `parseTime` does.
//! Multiplying a float by 1e9 and truncating loses the last digits: the
//! instant `1725360000.123` became `…122999808` ns, which misses a sample
//! stored at exactly `…123000000`.

use std::collections::HashMap;

use axum::extract::{FromRequest, Request};
use axum::http::Method;

use crate::error::ServerError;

/// The raw key/value parameters of a Prometheus API request, from the query
/// string on a `GET` or the form body on a `POST`.
#[derive(Debug, Default)]
pub struct PromParams(HashMap<String, Vec<String>>);

impl PromParams {
    /// The first value of `key`, if present.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.first()).map(String::as_str)
    }

    /// Every value of `key` — `match[]` is repeatable, and the repeats are a
    /// union rather than an override.
    #[must_use]
    pub fn get_all(&self, key: &str) -> Vec<&str> {
        self.0
            .get(key)
            .map(|v| v.iter().map(String::as_str).collect())
            .unwrap_or_default()
    }

    /// A required parameter.
    ///
    /// # Errors
    ///
    /// [`ServerError::BadRequest`] when it is absent.
    pub fn require(&self, key: &str) -> Result<&str, ServerError> {
        self.get(key)
            .ok_or_else(|| ServerError::BadRequest(format!("missing required parameter: {key}")))
    }

    /// An instant, in nanoseconds, from RFC 3339 or a Unix timestamp.
    ///
    /// # Errors
    ///
    /// [`ServerError::BadRequest`] when the value parses as neither.
    pub fn time(&self, key: &str) -> Result<Option<i64>, ServerError> {
        self.get(key).map(|v| parse_time_ns(key, v)).transpose()
    }

    /// The `limit` parameter, as Prometheus defines it: a non-negative
    /// integer bounding the number of results, where `0` means no limit.
    ///
    /// Every discovery endpoint and both query endpoints accept it upstream,
    /// and chronix accepted it from every client and **ignored it** — a
    /// `limit=1` returned everything. An ignored bound is the same failure as
    /// a bound applied without saying so, from the other side.
    ///
    /// # Errors
    ///
    /// [`ServerError::BadRequest`] when the value is not a non-negative
    /// integer, which is what Prometheus's `parseLimitParam` answers.
    pub fn limit(&self) -> Result<Option<usize>, ServerError> {
        let Some(raw) = self.get("limit") else {
            return Ok(None);
        };
        let n: usize = raw.parse().map_err(|_| {
            ServerError::BadRequest(format!(
                "cannot parse limit as a non-negative integer: {raw}"
            ))
        })?;
        Ok((n > 0).then_some(n))
    }

    /// A duration, in nanoseconds, from a Prometheus duration or a number of
    /// seconds.
    ///
    /// # Errors
    ///
    /// [`ServerError::BadRequest`] when the value parses as neither, or is
    /// not positive.
    pub fn duration(&self, key: &str) -> Result<Option<i64>, ServerError> {
        self.get(key).map(|v| parse_duration_ns(key, v)).transpose()
    }
}

/// Parse `time`, `start` or `end`: RFC 3339, or a Unix timestamp in seconds
/// with an optional fraction.
///
/// # Errors
///
/// [`ServerError::BadRequest`] when the value is neither, or is not finite.
pub fn parse_time_ns(key: &str, value: &str) -> Result<i64, ServerError> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
        return dt.timestamp_nanos_opt().ok_or_else(|| {
            ServerError::BadRequest(format!("{key}: timestamp out of range: {value}"))
        });
    }
    let secs: f64 = value.parse().map_err(|_| {
        ServerError::BadRequest(format!("cannot parse {key} as a timestamp: {value}"))
    })?;
    if !secs.is_finite() {
        // `time=NaN` used to reach the evaluator as epoch 0 and answer with
        // whatever happened to be stored in 1970.
        return Err(ServerError::BadRequest(format!(
            "{key} must be a finite timestamp, got {value}"
        )));
    }
    // Round to milliseconds first, as Prometheus's `parseTime` does, so a
    // float that names a millisecond instant names it exactly.
    let millis = (secs * 1_000.0).round();
    if millis >= (i64::MAX / 1_000_000) as f64 || millis <= (i64::MIN / 1_000_000) as f64 {
        return Err(ServerError::BadRequest(format!(
            "{key}: timestamp out of range: {value}"
        )));
    }
    #[allow(clippy::cast_possible_truncation)]
    Ok(millis as i64 * 1_000_000)
}

/// Parse a Prometheus duration (`5m`, `1h30m`, `100ms`, `1w`) or a bare
/// number of seconds.
///
/// # Errors
///
/// [`ServerError::BadRequest`] when the value is neither, or is not positive.
pub fn parse_duration_ns(key: &str, value: &str) -> Result<i64, ServerError> {
    let invalid = || ServerError::BadRequest(format!("cannot parse {key} as a duration: {value}"));

    // A bare number is seconds, which is what Grafana sends for `step`.
    if let Ok(secs) = value.parse::<f64>() {
        if !secs.is_finite() || secs <= 0.0 {
            return Err(ServerError::BadRequest(format!(
                "{key} must be a positive duration, got {value}"
            )));
        }
        #[allow(clippy::cast_possible_truncation)]
        return Ok((secs * 1e9) as i64);
    }

    // Otherwise a unit-suffixed duration, possibly compound: `1h30m`.
    let mut total: i64 = 0;
    let mut digits = String::new();
    let mut chars = value.chars().peekable();
    let mut saw_unit = false;
    while let Some(c) = chars.next() {
        if c.is_ascii_digit() {
            digits.push(c);
            continue;
        }
        if digits.is_empty() {
            return Err(invalid());
        }
        // `ms` is the only two-character unit; `m` alone is minutes.
        let unit = if c == 'm' && chars.peek() == Some(&'s') {
            chars.next();
            "ms"
        } else {
            match c {
                'y' | 'w' | 'd' | 'h' | 'm' | 's' => {
                    if c == 's' {
                        "s"
                    } else {
                        match c {
                            'y' => "y",
                            'w' => "w",
                            'd' => "d",
                            'h' => "h",
                            _ => "m",
                        }
                    }
                }
                _ => return Err(invalid()),
            }
        };
        let n: i64 = digits.parse().map_err(|_| invalid())?;
        digits.clear();
        saw_unit = true;
        let scale = match unit {
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60 * 1_000_000_000,
            "h" => 3_600 * 1_000_000_000,
            "d" => 86_400 * 1_000_000_000,
            "w" => 7 * 86_400 * 1_000_000_000,
            _ => 365 * 86_400 * 1_000_000_000_i64,
        };
        total = total
            .checked_add(n.checked_mul(scale).ok_or_else(invalid)?)
            .ok_or_else(invalid)?;
    }
    if !saw_unit || !digits.is_empty() || total <= 0 {
        return Err(invalid());
    }
    Ok(total)
}

impl<S: Send + Sync> FromRequest<S> for PromParams {
    type Rejection = ServerError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let mut params: HashMap<String, Vec<String>> = HashMap::new();
        if let Some(query) = req.uri().query() {
            for (k, v) in form_urlencoded::parse(query.as_bytes()) {
                params
                    .entry(k.into_owned())
                    .or_default()
                    .push(v.into_owned());
            }
        }
        // A POST carries its parameters in the body, which is what Grafana's
        // Prometheus datasource does by default. Query string and body are
        // merged rather than one overriding the other, exactly as
        // Prometheus's `r.ParseForm` does.
        if req.method() == Method::POST {
            let bytes = axum::body::Bytes::from_request(req, state)
                .await
                .map_err(|e| ServerError::BadRequest(format!("reading the form body: {e}")))?;
            for (k, v) in form_urlencoded::parse(&bytes) {
                params
                    .entry(k.into_owned())
                    .or_default()
                    .push(v.into_owned());
            }
        }
        Ok(Self(params))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_timestamp_may_be_rfc3339_or_a_unix_float() {
        // Grafana sends floats; humans and curl examples send RFC 3339.
        assert_eq!(
            parse_time_ns("time", "2024-09-03T12:00:00Z").unwrap(),
            1_725_364_800_000_000_000
        );
        assert_eq!(
            parse_time_ns("time", "2024-09-03T14:00:00+02:00").unwrap(),
            1_725_364_800_000_000_000
        );
        assert_eq!(
            parse_time_ns("time", "1725364800").unwrap(),
            1_725_364_800_000_000_000
        );

        // Rounded to milliseconds, so a float naming a millisecond instant
        // names it exactly. `secs * 1e9` truncated gave …122999808 here, and
        // a sample stored at exactly …123000000 was outside the range.
        assert_eq!(
            parse_time_ns("time", "1725364800.123").unwrap(),
            1_725_364_800_123_000_000
        );

        // Not a timestamp at all, and the values that used to become 1970.
        assert!(parse_time_ns("time", "yesterday").is_err());
        assert!(parse_time_ns("time", "NaN").is_err());
        assert!(parse_time_ns("time", "inf").is_err());
    }

    #[test]
    fn a_duration_may_be_a_unit_string_or_bare_seconds() {
        assert_eq!(parse_duration_ns("step", "15").unwrap(), 15_000_000_000);
        assert_eq!(parse_duration_ns("step", "0.5").unwrap(), 500_000_000);
        assert_eq!(parse_duration_ns("step", "15s").unwrap(), 15_000_000_000);
        assert_eq!(parse_duration_ns("step", "1m").unwrap(), 60_000_000_000);
        assert_eq!(parse_duration_ns("step", "100ms").unwrap(), 100_000_000);
        assert_eq!(parse_duration_ns("step", "1h").unwrap(), 3_600_000_000_000);
        assert_eq!(parse_duration_ns("step", "1d").unwrap(), 86_400_000_000_000);
        assert_eq!(
            parse_duration_ns("step", "1w").unwrap(),
            604_800_000_000_000
        );
        // Compound, as Prometheus allows.
        assert_eq!(
            parse_duration_ns("step", "1h30m").unwrap(),
            5_400_000_000_000
        );

        assert!(parse_duration_ns("step", "0").is_err());
        assert!(parse_duration_ns("step", "-1").is_err());
        assert!(parse_duration_ns("step", "5x").is_err());
        assert!(parse_duration_ns("step", "m").is_err());
        assert!(parse_duration_ns("step", "5m3").is_err());
    }
}
