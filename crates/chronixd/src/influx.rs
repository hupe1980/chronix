//! InfluxDB Line Protocol parser.
//!
//! Parses the InfluxDB v1/v2 line protocol text format:
//! ```text
//! measurement,tag1=val1,tag2=val2 field1=1.0,field2="hello" 1609459200000000000
//! ```
//!
//! Supports:
//! - Multiple lines (one point per line)
//! - Float, integer (`42i`), boolean, and string (`"hello"`) field values
//! - Optional nanosecond timestamps (defaults to current time)
//! - Comment lines starting with `#`

use std::collections::BTreeMap;

use chronix_core::{FieldValue, Point, SeriesKey};

use crate::error::ServerError;

/// Parse an InfluxDB Line Protocol payload into Chronix [`Point`]s.
///
/// # Errors
///
/// Returns [`ServerError::BadRequest`] if any line fails to parse.
pub fn parse_line_protocol(input: &str) -> Result<Vec<Point>, ServerError> {
    let parsed = parse_line_protocol_partial(input, Precision::Nanoseconds);
    if let Some(first) = parsed.errors.first() {
        return Err(ServerError::BadRequest(first.clone()));
    }
    Ok(parsed.points)
}

/// The timestamp unit a client says its line protocol is in.
///
/// InfluxDB's write API takes `precision=ns|us|ms|s`, and Telegraf sets it —
/// its default output is `precision = "1ns"` but a configured `ms` is
/// common, and every client that writes seconds sets it. Ignoring the
/// parameter, as this server did, put a millisecond client's data in
/// January 1970: `1700000000000` read as nanoseconds is 1970-01-01T00:28:20Z.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Precision {
    /// Nanoseconds — the Line Protocol default.
    #[default]
    Nanoseconds,
    /// Microseconds.
    Microseconds,
    /// Milliseconds.
    Milliseconds,
    /// Seconds.
    Seconds,
}

impl Precision {
    /// Parse the `precision` query parameter. InfluxDB accepts both the bare
    /// unit (`ms`) and the v2 duration spelling (`1ms`).
    ///
    /// # Errors
    ///
    /// [`ServerError::BadRequest`] for an unknown unit.
    pub fn parse(value: &str) -> Result<Self, ServerError> {
        match value.trim_start_matches('1') {
            "ns" | "" => Ok(Self::Nanoseconds),
            "us" | "µs" | "u" => Ok(Self::Microseconds),
            "ms" => Ok(Self::Milliseconds),
            "s" => Ok(Self::Seconds),
            other => Err(ServerError::BadRequest(format!(
                "unsupported precision: {other} (expected ns, us, ms or s)"
            ))),
        }
    }

    /// Nanoseconds per unit.
    #[must_use]
    pub fn scale(self) -> i64 {
        match self {
            Self::Nanoseconds => 1,
            Self::Microseconds => 1_000,
            Self::Milliseconds => 1_000_000,
            Self::Seconds => 1_000_000_000,
        }
    }
}

/// What a batch of line protocol parsed into: the good lines, and one
/// message per bad one.
#[derive(Debug, Default)]
pub struct ParsedBatch {
    /// Points from the lines that parsed.
    pub points: Vec<Point>,
    /// One message per line that did not, naming the line number.
    pub errors: Vec<String>,
}

/// Parse a batch with InfluxDB's **partial write** semantics: a bad line
/// does not discard the good ones.
///
/// This is not leniency, it is what the protocol's clients require. Telegraf
/// retries a batch unless the response marks the failure permanent, and it
/// recognises "partial write" and "unable to parse" — so failing the whole
/// batch on one malformed line made it retry that batch for ever, and the
/// good lines in it never landed at all.
#[must_use]
pub fn parse_line_protocol_partial(input: &str, precision: Precision) -> ParsedBatch {
    let mut out = ParsedBatch::default();
    for (line_no, line) in input.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        match parse_line(line, line_no + 1) {
            Ok(point) => match scale_timestamp(point, precision) {
                Ok(p) => out.points.push(p),
                Err(e) => out.errors.push(e),
            },
            Err(e) => out.errors.push(e.to_string()),
        }
    }
    out
}

/// Re-stamp a point whose timestamp was read in `precision` units.
fn scale_timestamp(point: Point, precision: Precision) -> Result<Point, String> {
    if precision == Precision::Nanoseconds {
        return Ok(point);
    }
    let scaled = point
        .timestamp()
        .checked_mul(precision.scale())
        .ok_or_else(|| {
            format!(
                "timestamp {} overflows when scaled from {precision:?}",
                point.timestamp()
            )
        })?;
    Point::new(
        point.series_key().clone(),
        point
            .fields()
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
        scaled,
    )
    .map_err(|e| e.to_string())
}

/// Parse a single line of InfluxDB Line Protocol.
fn parse_line(line: &str, line_no: usize) -> Result<Point, ServerError> {
    // Format: measurement[,tag1=val1,tag2=val2] field1=val1[,field2=val2] [timestamp]
    //
    // Split strategy:
    // 1. First unescaped space separates measurement+tags from fields+timestamp
    // 2. Second unescaped space separates fields from timestamp

    let (parts, part_count) = split_line_parts(line);
    if part_count < 2 {
        return Err(ServerError::BadRequest(format!(
            "line {line_no}: expected at least measurement and fields"
        )));
    }

    // Parse measurement and tags
    let (measurement, tags) = parse_measurement_tags(parts[0], line_no)?;

    // Parse fields
    let fields = parse_fields(parts[1], line_no)?;
    if fields.is_empty() {
        return Err(ServerError::BadRequest(format!(
            "line {line_no}: at least one field is required"
        )));
    }

    // Parse optional timestamp
    let timestamp = if part_count >= 3 {
        parts[2].parse::<i64>().map_err(|_| {
            ServerError::BadRequest(format!("line {line_no}: invalid timestamp: {}", parts[2]))
        })?
    } else {
        crate::util::now_nanos()?
    };

    let key = SeriesKey::new(&measurement, tags)
        .map_err(|e| ServerError::BadRequest(format!("line {line_no}: {e}")))?;

    Point::new(key, fields, timestamp)
        .map_err(|e| ServerError::BadRequest(format!("line {line_no}: {e}")))
}

/// Whether the byte at `i` is escaped, i.e. preceded by an **odd** number of
/// consecutive backslashes.
///
/// A naive `bytes[i - 1] != b'\\'` check gets this wrong whenever the
/// preceding backslash is itself escaped. `path="C:\\"` ends with an escaped
/// backslash followed by a *real* closing quote; treating that quote as
/// escaped left the parser inside a string for the rest of the line, so the
/// timestamp was never split off.
fn is_escaped(bytes: &[u8], i: usize) -> bool {
    let mut backslashes = 0usize;
    let mut j = i;
    while j > 0 && bytes[j - 1] == b'\\' {
        backslashes += 1;
        j -= 1;
    }
    backslashes % 2 == 1
}

/// Split `s` at unescaped occurrences of `delim`.
///
/// When `respect_quotes` is set, delimiters inside a double-quoted run are
/// ignored — that is only correct for the *field* section, where `"` delimits
/// string values. In the measurement/tag section `"` is an ordinary character
/// (the LP spec lists only `,`, `=` and space as escapable there), so treating
/// it as a quote made any tag value containing a double quote break the whole
/// line.
fn split_unescaped(s: &str, delim: u8, respect_quotes: bool) -> Vec<&str> {
    let bytes = s.as_bytes();
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut in_quotes = false;

    for i in 0..bytes.len() {
        let b = bytes[i];
        if respect_quotes && b == b'"' && !is_escaped(bytes, i) {
            in_quotes = !in_quotes;
        }
        if b == delim && !in_quotes && !is_escaped(bytes, i) {
            parts.push(&s[start..i]);
            start = i + 1;
        }
    }
    parts.push(&s[start..]);
    parts
}

/// Split `s` at the first unescaped `delim`.
fn split_once_unescaped(s: &str, delim: u8) -> Option<(&str, &str)> {
    let bytes = s.as_bytes();
    for i in 0..bytes.len() {
        if bytes[i] == delim && !is_escaped(bytes, i) {
            return Some((&s[..i], &s[i + 1..]));
        }
    }
    None
}

/// Split a line into its three sections at unescaped spaces.
///
/// Returns up to 3 parts (measurement+tags, fields, timestamp) in a
/// fixed-size array to avoid heap allocation on the hot ingest path.
///
/// Quote tracking starts only at the field section. Scanning the whole line
/// with quote tracking meant a `"` anywhere in a tag value flipped the parser
/// into "inside a string" for the rest of the line, so the field and
/// timestamp sections were never split off.
fn split_line_parts(line: &str) -> ([&str; 3], usize) {
    let mut parts = [""; 3];
    let mut count = 0usize;
    let bytes = line.as_bytes();

    // Section 1: measurement + tags, ending at the first unescaped space.
    // No quote handling here — `"` is a literal in this section.
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b' ' && !is_escaped(bytes, i) {
            break;
        }
        i += 1;
    }
    if i > 0 {
        parts[0] = &line[..i];
        count = 1;
    }
    if i >= bytes.len() {
        return (parts, count);
    }

    // Section 2: fields, ending at the next unescaped space outside quotes.
    let field_start = i + 1;
    let mut in_quotes = false;
    let mut j = field_start;
    while j < bytes.len() {
        let b = bytes[j];
        if b == b'"' && !is_escaped(bytes, j) {
            in_quotes = !in_quotes;
        }
        if b == b' ' && !in_quotes && !is_escaped(bytes, j) {
            break;
        }
        j += 1;
    }
    if j > field_start {
        parts[count] = &line[field_start..j];
        count += 1;
    }

    // Section 3: timestamp — the remainder.
    if j < bytes.len() {
        let ts = line[j + 1..].trim();
        if !ts.is_empty() {
            parts[count] = ts;
            count += 1;
        }
    }

    (parts, count)
}

/// Parse `measurement[,tag1=val1,tag2=val2]` into measurement name and tags.
fn parse_measurement_tags(
    s: &str,
    line_no: usize,
) -> Result<(String, BTreeMap<String, String>), ServerError> {
    let mut tags = BTreeMap::new();

    // Split on the first *unescaped* comma: a measurement name may contain an
    // escaped one.
    let (measurement_raw, tag_section) = match split_once_unescaped(s, b',') {
        Some((m, rest)) => (m, Some(rest)),
        None => (s, None),
    };
    let measurement = unescape(measurement_raw);

    if measurement.is_empty() {
        return Err(ServerError::BadRequest(format!(
            "line {line_no}: empty measurement name"
        )));
    }

    if let Some(tag_section) = tag_section {
        for tag_pair in split_unescaped(tag_section, b',', false) {
            let Some((k, v)) = split_once_unescaped(tag_pair, b'=') else {
                return Err(ServerError::BadRequest(format!(
                    "line {line_no}: invalid tag: {tag_pair}"
                )));
            };
            tags.insert(unescape(k), unescape(v));
        }
    }

    Ok((measurement, tags))
}

/// Parse `field1=val1,field2=val2` into fields.
fn parse_fields(s: &str, line_no: usize) -> Result<BTreeMap<String, FieldValue>, ServerError> {
    let mut fields = BTreeMap::new();

    for field_pair in split_unescaped(s, b',', true) {
        let Some((k, v)) = split_once_unescaped(field_pair, b'=') else {
            return Err(ServerError::BadRequest(format!(
                "line {line_no}: invalid field: {field_pair}"
            )));
        };
        let key = unescape(k);
        let value = parse_field_value(v, line_no)?;
        fields.insert(key, value);
    }

    Ok(fields)
}

/// Parse a single field value.
fn parse_field_value(s: &str, line_no: usize) -> Result<FieldValue, ServerError> {
    // String: "..."
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        return Ok(FieldValue::String(unescape(&s[1..s.len() - 1])));
    }

    // Boolean
    match s {
        "t" | "T" | "true" | "True" | "TRUE" => return Ok(FieldValue::Bool(true)),
        "f" | "F" | "false" | "False" | "FALSE" => return Ok(FieldValue::Bool(false)),
        _ => {}
    }

    // Integer: 42i
    if let Some(num) = s.strip_suffix('i') {
        return num
            .parse::<i64>()
            .map(FieldValue::I64)
            .map_err(|_| ServerError::BadRequest(format!("line {line_no}: invalid integer: {s}")));
    }

    // Unsigned: 42u
    if let Some(num) = s.strip_suffix('u') {
        return num.parse::<u64>().map(FieldValue::U64).map_err(|_| {
            ServerError::BadRequest(format!("line {line_no}: invalid unsigned integer: {s}"))
        });
    }

    // Exact decimal: 231.45d
    //
    // A Chronix extension to line protocol, beside `i` and `u`. Influx has
    // no exact type, so there is no suffix to borrow and no compatibility to
    // break: a line written for Influx never carries a `d`. The digits are
    // parsed as digits — a decimal field never passes through an `f64`, on
    // this path or any other.
    if let Some(num) = s.strip_suffix('d') {
        return num
            .parse::<chronix_core::Decimal>()
            .map(FieldValue::Decimal)
            .map_err(|e| {
                ServerError::BadRequest(format!("line {line_no}: invalid decimal {s}: {e}"))
            });
    }

    // Float (default for numbers without suffix)
    s.parse::<f64>()
        .map(FieldValue::F64)
        .map_err(|_| ServerError::BadRequest(format!("line {line_no}: invalid field value: {s}")))
}

/// Unescape Line Protocol escapes in a single left-to-right pass.
///
/// The previous implementation chained `str::replace` calls, which
/// cannot process escapes correctly. Chained replacement has no notion of
/// "this backslash was already consumed", so `\\,` (a literal backslash
/// followed by a separator) was indistinguishable from `\,` (an escaped
/// comma), and `\\` was never turned back into a single backslash at all.
///
/// A single pass consumes `\` plus the character after it, so each escape is
/// resolved exactly once. Recognised escapes are `\ `, `\,`, `\=`, `\"`
/// and `\\`; a backslash before anything else is preserved verbatim, which
/// is what InfluxDB does.
fn unescape(s: &str) -> String {
    if !s.contains('\\') {
        // Overwhelmingly the common case — avoid the allocation dance.
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some(next @ (' ' | ',' | '=' | '"' | '\\')) => out.push(next),
            // Not a recognised escape: keep both characters.
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            // Trailing lone backslash.
            None => out.push('\\'),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_line() {
        let input = r#"cpu,host=server01,region=us-east usage_idle=95.5,usage_system=1.2 1609459200000000000"#;
        let points = parse_line_protocol(input).unwrap();
        assert_eq!(points.len(), 1);

        let p = &points[0];
        assert_eq!(p.series_key().measurement(), "cpu");
        assert_eq!(p.tag("host").unwrap(), "server01");
        assert_eq!(p.tag("region").unwrap(), "us-east");
        assert_eq!(p.timestamp(), 1_609_459_200_000_000_000);

        match p.field("usage_idle") {
            Some(FieldValue::F64(v)) => assert!((v - 95.5).abs() < f64::EPSILON),
            other => panic!("expected Float64(95.5), got {other:?}"),
        }
    }

    #[test]
    fn parse_integer_field() {
        let input = "mem,host=srv1 used=1024i 1000000000";
        let points = parse_line_protocol(input).unwrap();
        assert_eq!(points.len(), 1);
        match points[0].field("used") {
            Some(FieldValue::I64(1024)) => {}
            other => panic!("expected Int64(1024), got {other:?}"),
        }
    }

    #[test]
    fn parse_unsigned_field() {
        let input = "disk used_bytes=42u 1000000000";
        let points = parse_line_protocol(input).unwrap();
        match points[0].field("used_bytes") {
            Some(FieldValue::U64(42)) => {}
            other => panic!("expected UInt64(42), got {other:?}"),
        }
    }

    #[test]
    fn parse_decimal_field() {
        let input = "meter,dev=z1 z1nb=1234.5678d 1000000000";
        let points = parse_line_protocol(input).unwrap();
        match points[0].field("z1nb") {
            Some(FieldValue::Decimal(d)) => {
                assert_eq!(d.mantissa(), 12_345_678);
                assert_eq!(d.scale(), 4);
                assert_eq!(d.to_string(), "1234.5678");
            }
            other => panic!("expected Decimal(1234.5678), got {other:?}"),
        }
    }

    #[test]
    fn a_decimal_field_keeps_digits_a_float_would_lose() {
        // Written as a float, this line protocol value comes back as
        // 0.30000000000000004; written as a decimal it comes back as itself.
        let input = "meter price=0.3d,approx=0.3 1000000000";
        let points = parse_line_protocol(input).unwrap();
        match points[0].field("price") {
            Some(FieldValue::Decimal(d)) => assert_eq!(d.to_string(), "0.3"),
            other => panic!("expected Decimal, got {other:?}"),
        }
        assert!(matches!(
            points[0].field("approx"),
            Some(FieldValue::F64(_))
        ));
    }

    #[test]
    fn an_invalid_decimal_is_rejected_not_rounded() {
        let input = "meter z=1.2.3d 1000000000";
        assert!(parse_line_protocol(input).is_err());
    }

    #[test]
    fn a_decimal_field_round_trips_through_display() {
        // `FieldValue`'s Display is the line-protocol rendering, so what a
        // decimal prints must be what the parser reads back.
        let value = FieldValue::Decimal("1234.5678".parse().unwrap());
        assert_eq!(value.to_string(), "1234.5678d");
        assert_eq!(parse_field_value("1234.5678d", 1).unwrap(), value);
    }

    #[test]
    fn parse_boolean_field() {
        let input = "status,host=srv1 alive=true 1000000000";
        let points = parse_line_protocol(input).unwrap();
        match points[0].field("alive") {
            Some(FieldValue::Bool(true)) => {}
            other => panic!("expected Bool(true), got {other:?}"),
        }
    }

    #[test]
    fn parse_string_field() {
        let input = r#"log,host=srv1 message="hello world" 1000000000"#;
        let points = parse_line_protocol(input).unwrap();
        match points[0].field("message") {
            Some(FieldValue::String(s)) => assert_eq!(s, "hello world"),
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn parse_no_tags() {
        let input = "cpu usage=42.0 1000000000";
        let points = parse_line_protocol(input).unwrap();
        assert_eq!(points.len(), 1);
        assert!(points[0].series_key().tags().is_empty());
    }

    #[test]
    fn parse_no_timestamp() {
        let input = "cpu,host=srv1 usage=42.0";
        let points = parse_line_protocol(input).unwrap();
        assert_eq!(points.len(), 1);
        assert!(points[0].timestamp() > 0);
    }

    #[test]
    fn parse_multi_line() {
        let input = "cpu,host=a usage=1.0 1000\ncpu,host=b usage=2.0 2000\n";
        let points = parse_line_protocol(input).unwrap();
        assert_eq!(points.len(), 2);
    }

    #[test]
    fn skip_comments_and_blanks() {
        let input = "# this is a comment\n\ncpu usage=1.0 1000\n\n";
        let points = parse_line_protocol(input).unwrap();
        assert_eq!(points.len(), 1);
    }

    #[test]
    fn empty_measurement_is_error() {
        let input = ",host=srv1 usage=1.0 1000";
        let err = parse_line_protocol(input).unwrap_err();
        assert!(err.to_string().contains("measurement"), "{err}");
    }

    #[test]
    fn no_fields_is_error() {
        let input = "cpu,host=srv1";
        let err = parse_line_protocol(input).unwrap_err();
        assert!(err.to_string().contains("fields"), "{err}");
    }

    #[test]
    fn invalid_timestamp_is_error() {
        let input = "cpu usage=1.0 notanumber";
        let err = parse_line_protocol(input).unwrap_err();
        assert!(err.to_string().contains("timestamp"), "{err}");
    }

    #[test]
    fn multiple_fields() {
        let input =
            r#"weather,location=us temp=72.5,humidity=45i,description="partly cloudy" 1000"#;
        let points = parse_line_protocol(input).unwrap();
        assert_eq!(points[0].fields().len(), 3);
    }
}

#[cfg(test)]
mod escape_tests {
    use super::*;

    /// A string field value ending in a literal backslash: the closing quote
    /// is preceded by `\`, but that `\` is itself escaped, so the quote is
    /// real and must terminate the string.
    #[test]
    fn trailing_escaped_backslash_closes_the_string() {
        // Wire form: path="C:\\"  → value is `C:\`
        let input = r#"m path="C:\\" 1000000000"#;
        let points = parse_line_protocol(input).expect("should parse");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp(), 1_000_000_000);
        match points[0].field("path") {
            Some(FieldValue::String(v)) => assert_eq!(v, r"C:\"),
            other => panic!("expected string field, got {other:?}"),
        }
    }

    /// `\\` is a literal backslash and must be unescaped as one.
    #[test]
    fn escaped_backslash_is_unescaped() {
        let input = r#"m v="a\\b" 1000000000"#;
        let points = parse_line_protocol(input).expect("should parse");
        match points[0].field("v") {
            Some(FieldValue::String(s)) => assert_eq!(s, r"a\b"),
            other => panic!("expected string field, got {other:?}"),
        }
    }

    /// An escaped quote inside a value must not terminate it.
    #[test]
    fn escaped_quote_does_not_terminate() {
        let input = r#"m v="say \"hi\"" 1000000000"#;
        let points = parse_line_protocol(input).expect("should parse");
        assert_eq!(points[0].timestamp(), 1_000_000_000);
        match points[0].field("v") {
            Some(FieldValue::String(s)) => assert_eq!(s, r#"say "hi""#),
            other => panic!("expected string field, got {other:?}"),
        }
    }

    /// A tag value ending in a literal backslash, followed by a real
    /// separator.
    #[test]
    fn escaped_backslash_in_tag_value() {
        let input = r#"m,dir=C:\\,host=a v=1 1000000000"#;
        let points = parse_line_protocol(input).expect("should parse");
        let key = points[0].series_key();
        assert_eq!(key.tag("dir"), Some(r"C:\"));
        assert_eq!(key.tag("host"), Some("a"));
    }
}

#[cfg(test)]
mod escape_proptests {
    use super::*;
    use proptest::prelude::*;

    /// Escape a value the way an InfluxDB client is required to.
    fn escape_tag(v: &str) -> String {
        let mut out = String::with_capacity(v.len());
        for c in v.chars() {
            if matches!(c, '\\' | ',' | ' ' | '=') {
                out.push('\\');
            }
            out.push(c);
        }
        out
    }

    /// String field values escape only `"` and `\`.
    fn escape_str_field(v: &str) -> String {
        let mut out = String::with_capacity(v.len());
        for c in v.chars() {
            if matches!(c, '\\' | '"') {
                out.push('\\');
            }
            out.push(c);
        }
        out
    }

    proptest! {
        /// Anything a conforming client escapes must parse back identically.
        /// This is the property the hand-rolled escape handling violated.
        #[test]
        fn tag_values_roundtrip(
            v in r#"[a-zA-Z0-9 ,=\\"'/:\.\-]{1,24}"#
        ) {
            let line = format!("m,t={} f=1 1000000000", escape_tag(&v));
            let points = parse_line_protocol(&line)
                .map_err(|e| TestCaseError::fail(format!("parse failed for {v:?}: {e}")))?;
            prop_assert_eq!(points.len(), 1);
            prop_assert_eq!(points[0].series_key().tag("t"), Some(v.as_str()));
            prop_assert_eq!(points[0].timestamp(), 1_000_000_000);
        }

        #[test]
        fn string_field_values_roundtrip(
            v in r#"[a-zA-Z0-9 ,=\\"'/:\.\-]{0,24}"#
        ) {
            let line = format!(r#"m f="{}" 1000000000"#, escape_str_field(&v));
            let points = parse_line_protocol(&line)
                .map_err(|e| TestCaseError::fail(format!("parse failed for {v:?}: {e}")))?;
            prop_assert_eq!(points.len(), 1);
            match points[0].field("f") {
                Some(FieldValue::String(s)) => prop_assert_eq!(s.as_str(), v.as_str()),
                other => return Err(TestCaseError::fail(format!("expected string, got {other:?}"))),
            }
            prop_assert_eq!(points[0].timestamp(), 1_000_000_000);
        }
    }
}
