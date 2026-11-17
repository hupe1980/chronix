//! Diagnostics: what a query got wrong, what it probably meant, and where.
//!
//! A diagnostic is one complete sentence — the error text a log line or a
//! Grafana banner shows on its own — plus two optional machine-readable
//! parts: a [`hint`](Diagnosis::hint) (`did you mean "cpu_usage"?`) and a
//! [`span`](Diagnosis::span), the bytes of the query the error is about.
//!
//! ```
//! use chronix::diagnostics::suggest;
//!
//! let s = suggest("cpu_usge", ["cpu_usage", "mem_used", "disk_io"]);
//! assert_eq!(s.hint().as_deref(), Some(r#"did you mean "cpu_usage"?"#));
//! ```

use serde::{Deserialize, Serialize};

/// Half-open byte range into the query string as it was sent.
///
/// `&query[span.start..span.end]` is the text the diagnostic is about, which
/// is how a client draws a caret under it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Span {
    /// First byte, inclusive.
    pub start: usize,
    /// Last byte, exclusive.
    pub end: usize,
}

impl Span {
    /// The 1-based line and column of the span's start, the column counted
    /// in characters — what Prometheus prints as `<line>:<column>:` before a
    /// parse error.
    #[must_use]
    pub fn line_column(&self, query: &str) -> (usize, usize) {
        let prefix = query.get(..self.start).unwrap_or(query);
        let line = prefix.matches('\n').count() + 1;
        let line_start = prefix.rfind('\n').map_or(0, |i| i + 1);
        (line, prefix[line_start..].chars().count() + 1)
    }

    /// The 1-based column of the span's start on its line, in characters.
    #[must_use]
    pub fn column(&self, query: &str) -> usize {
        self.line_column(query).1
    }
}

/// The parts of a diagnostic beyond its message.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Diagnosis {
    /// A rewritten one-line message, when chronix can say it better than the
    /// error it was raised as (`unknown table "x" — did you mean "y"?`).
    pub message: Option<String>,
    /// What the query probably meant: `did you mean "x"?`.
    pub hint: Option<String>,
    /// The bytes of the query the error is about.
    pub span: Option<Span>,
}

impl Diagnosis {
    /// `true` when there is nothing beyond the error's own text.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.message.is_none() && self.hint.is_none() && self.span.is_none()
    }
}

/// What kind of name a query used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameKind {
    /// A SQL table (a measurement).
    Table,
    /// A SQL column.
    Column,
    /// A SQL function.
    SqlFunction,
    /// A PromQL function.
    PromqlFunction,
    /// A PromQL metric name.
    Metric,
    /// A PromQL label key.
    LabelKey,
}

impl NameKind {
    /// The noun used in a message: `unknown table "x"`.
    #[must_use]
    pub const fn noun(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Column => "column",
            Self::SqlFunction | Self::PromqlFunction => "function",
            Self::Metric => "metric",
            Self::LabelKey => "label",
        }
    }
}

/// The names closest to what was typed — up to three when they tie.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Suggestion {
    /// The candidates, equally close, in alphabetical order.
    pub candidates: Vec<String>,
}

impl Suggestion {
    /// `did you mean "a"?`, `"a" or "b"`, `"a", "b" or "c"`; `None` when
    /// nothing was close enough.
    #[must_use]
    pub fn hint(&self) -> Option<String> {
        if self.candidates.is_empty() {
            None
        } else {
            Some(format!(
                "did you mean {}?",
                quoted_list(&self.candidates, "or")
            ))
        }
    }
}

/// Names within typing distance of `typed`, by optimal string alignment.
///
/// Case is ignored when measuring — `CPU_usage` is a suggestion for
/// `cpu_usage` — but the identical name is never suggested. How far a
/// candidate may be scales with what was typed: nothing under three
/// characters, one edit up to four, two up to eight, three beyond.
pub fn suggest<'a>(typed: &str, candidates: impl IntoIterator<Item = &'a str>) -> Suggestion {
    let len = typed.chars().count();
    let limit = match len {
        0..=2 => return Suggestion::default(),
        3..=4 => 1,
        5..=8 => 2,
        _ => 3,
    };
    let typed_lower = typed.to_lowercase();
    // Rank by distance, then prefer a name that one of the two is a prefix
    // of — a dropped or extra trailing letter is the likelier typo, so `rat`
    // means `rate` rather than `rad`. Only the best rank is offered: three
    // names when one is plainly meant is noise.
    let mut hits: Vec<((usize, bool), &str)> = candidates
        .into_iter()
        .filter(|c| *c != typed)
        .filter_map(|c| {
            let lower = c.to_lowercase();
            let d = osa_distance(&typed_lower, &lower);
            let not_prefix = !(lower.starts_with(&typed_lower) || typed_lower.starts_with(&lower));
            (d <= limit).then_some(((d, not_prefix), c))
        })
        .collect();
    hits.sort_unstable();
    hits.dedup_by(|a, b| a.1 == b.1);
    let best = hits.first().map(|h| h.0);
    Suggestion {
        candidates: hits
            .into_iter()
            .take_while(|h| Some(h.0) == best)
            .take(3)
            .map(|(_, c)| c.to_string())
            .collect(),
    }
}

/// Optimal string alignment distance: Levenshtein plus one-step adjacent
/// transpositions, so `usgae` is one edit from `usage`, not two.
#[must_use]
pub fn osa_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let (n, m) = (a.len(), b.len());
    // Three rows: the one before the previous is what a transposition reads.
    let mut prev2 = vec![0usize; m + 1];
    let mut prev: Vec<usize> = (0..=m).collect();
    let mut cur = vec![0usize; m + 1];
    for i in 1..=n {
        cur[0] = i;
        for j in 1..=m {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            let mut d = (prev[j] + 1).min(cur[j - 1] + 1).min(prev[j - 1] + cost);
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                d = d.min(prev2[j - 2] + 1);
            }
            cur[j] = d;
        }
        std::mem::swap(&mut prev2, &mut prev);
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[m]
}

/// `"a"`, `"a" or "b"`, `"a", "b" or "c"` — with `and` or `or` as the joiner.
#[must_use]
pub fn quoted_list(names: &[String], joiner: &str) -> String {
    let q: Vec<String> = names.iter().map(|n| format!("\"{n}\"")).collect();
    match q.len() {
        0 => String::new(),
        1 => q[0].clone(),
        n => format!("{} {joiner} {}", q[..n - 1].join(", "), q[n - 1]),
    }
}

/// What a query can choose from, listed for a message: up to eight names,
/// then how many more.
#[must_use]
pub(crate) fn available(mut names: Vec<String>) -> String {
    const SHOWN: usize = 8;
    names.sort_unstable();
    if names.len() <= SHOWN {
        quoted_list(&names, "and")
    } else {
        let more = names.len() - SHOWN;
        names.truncate(SHOWN);
        let q: Vec<String> = names.iter().map(|n| format!("\"{n}\"")).collect();
        format!("{} and {more} more", q.join(", "))
    }
}

/// The message for a name the query used and the database does not have.
///
/// With a close match: `unknown table "cpu_usge" — did you mean "cpu_usage"?`.
/// Without one, the names that exist, so the fix is visible:
/// `unknown column "foo"; the columns are "_time", "host" and "usage"`.
#[must_use]
pub fn unknown_name_message(
    kind: NameKind,
    typed: &str,
    all: &[String],
) -> (String, Option<String>) {
    let suggestion = suggest(typed, all.iter().map(String::as_str));
    let noun = kind.noun();
    if let Some(hint) = suggestion.hint() {
        return (format!("unknown {noun} \"{typed}\" — {hint}"), Some(hint));
    }
    let message = if all.is_empty() {
        match kind {
            NameKind::Table => format!("unknown table \"{typed}\"; no table has data yet"),
            _ => format!("unknown {noun} \"{typed}\""),
        }
    } else {
        format!(
            "unknown {noun} \"{typed}\"; the {noun}s are {}",
            available(all.to_vec())
        )
    };
    (message, None)
}

/// Convert a 1-based `(line, column)` position — columns counted in
/// characters, as `DataFusion` reports them — to a byte offset into `query`.
#[must_use]
pub fn byte_offset(query: &str, line: u64, column: u64) -> Option<usize> {
    if line == 0 || column == 0 {
        return None;
    }
    let mut offset = 0usize;
    for (n, text) in query.split_inclusive('\n').enumerate() {
        if n as u64 + 1 == line {
            let col = usize::try_from(column - 1).ok()?;
            return match text.char_indices().nth(col) {
                Some((b, _)) => Some(offset + b),
                // One past the last character of the line is a valid end.
                None if text.trim_end_matches('\n').chars().count() == col => {
                    Some(offset + text.trim_end_matches('\n').len())
                }
                None => None,
            };
        }
        offset += text.len();
    }
    // An empty query, or a position just past the end.
    (line == 1 && column == 1).then_some(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transpositions_are_one_edit() {
        assert_eq!(osa_distance("usgae", "usage"), 1);
        assert_eq!(osa_distance("requets", "requests"), 1);
        assert_eq!(osa_distance("kitten", "sitting"), 3);
        assert_eq!(osa_distance("", "abc"), 3);
        assert_eq!(osa_distance("abc", "abc"), 0);
    }

    #[test]
    fn the_threshold_scales_with_what_was_typed() {
        // Under three characters: never.
        assert!(suggest("ab", ["abc"]).candidates.is_empty());
        // Three to four: one edit.
        assert_eq!(suggest("rat", ["rate"]).candidates, ["rate"]);
        assert!(suggest("rt", ["rate"]).candidates.is_empty());
        assert!(suggest("rxtx", ["rate"]).candidates.is_empty());
        // Five to eight: two.
        assert_eq!(suggest("cpu_usge", ["cpu_usage"]).candidates, ["cpu_usage"]);
        // Nine and more: three.
        assert_eq!(
            suggest("http_requets_totl", ["http_requests_total"]).candidates,
            ["http_requests_total"]
        );
    }

    #[test]
    fn case_only_differences_are_suggested_and_identity_is_not() {
        assert_eq!(
            suggest("CPU_usage", ["cpu_usage"]).candidates,
            ["cpu_usage"]
        );
        assert!(suggest("cpu_usage", ["cpu_usage"]).candidates.is_empty());
    }

    #[test]
    fn only_the_closest_are_offered_and_ties_are_capped_at_three() {
        let s = suggest("cpu", ["cpx", "cpa", "cpb", "cpc", "cpz"]);
        assert_eq!(s.candidates, ["cpa", "cpb", "cpc"]);
        // One edit beats two, whatever the alphabet says.
        let s = suggest("cpu_usge", ["cpu_used", "cpu_usage", "cpu_user"]);
        assert_eq!(s.candidates, ["cpu_usage"]);
        // At equal distance, a dropped trailing letter beats a substitution.
        let s = suggest("rat", ["rad", "rate"]);
        assert_eq!(s.candidates, ["rate"]);
        // A genuine tie is offered whole.
        let s = suggest("cpu_usex", ["cpu_used", "cpu_user"]);
        assert_eq!(s.candidates, ["cpu_used", "cpu_user"]);
    }

    #[test]
    fn the_hint_reads_as_a_sentence() {
        let one = Suggestion {
            candidates: vec!["a".into()],
        };
        let two = Suggestion {
            candidates: vec!["a".into(), "b".into()],
        };
        let three = Suggestion {
            candidates: vec!["a".into(), "b".into(), "c".into()],
        };
        assert_eq!(one.hint().unwrap(), r#"did you mean "a"?"#);
        assert_eq!(two.hint().unwrap(), r#"did you mean "a" or "b"?"#);
        assert_eq!(three.hint().unwrap(), r#"did you mean "a", "b" or "c"?"#);
        assert_eq!(Suggestion::default().hint(), None);
    }

    #[test]
    fn an_unknown_name_without_a_match_lists_what_exists() {
        let cols = vec!["usage".to_string(), "_time".into(), "host".into()];
        let (msg, hint) = unknown_name_message(NameKind::Column, "zzzzzz", &cols);
        assert_eq!(
            msg,
            r#"unknown column "zzzzzz"; the columns are "_time", "host" and "usage""#
        );
        assert_eq!(hint, None);
        let (msg, hint) = unknown_name_message(NameKind::Column, "usgae", &cols);
        assert_eq!(msg, r#"unknown column "usgae" — did you mean "usage"?"#);
        assert_eq!(hint.as_deref(), Some(r#"did you mean "usage"?"#));
        let (msg, _) = unknown_name_message(NameKind::Table, "cpu", &[]);
        assert_eq!(msg, r#"unknown table "cpu"; no table has data yet"#);
        let many: Vec<String> = (0..11).map(|i| format!("m{i:02}")).collect();
        let (msg, _) = unknown_name_message(NameKind::Table, "zzzzzz", &many);
        assert!(msg.ends_with(r#""m07" and 3 more"#), "{msg}");
    }

    #[test]
    fn line_and_column_become_bytes_past_non_ascii_text() {
        // `ü` is two bytes and one column.
        let q = "SELECT 'ü' AS x, usgae FROM cpu";
        let off = byte_offset(q, 1, 18).unwrap();
        assert_eq!(&q[off..off + 5], "usgae");
        let q = "SELECT a\nFROM cpu_usge";
        let off = byte_offset(q, 2, 6).unwrap();
        assert_eq!(&q[off..], "cpu_usge");
        assert_eq!(byte_offset(q, 0, 1), None);
        assert_eq!(byte_offset(q, 9, 1), None);
    }

    #[test]
    fn a_position_on_a_later_line_counts_from_that_line() {
        let q = "sum(\n  rate(x[5m]]";
        let start = q.rfind(']').unwrap();
        let span = Span {
            start,
            end: start + 1,
        };
        // `  rate(x[5m]]`: the second `]` is the thirteenth character of line 2.
        assert_eq!(span.line_column(q), (2, 13));
    }

    #[test]
    fn the_column_is_counted_in_characters() {
        let q = "ü{x=\"y\"}]";
        let span = Span {
            start: q.find(']').unwrap(),
            end: q.len(),
        };
        assert_eq!(span.column(q), 9);
    }
}
