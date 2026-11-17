//! SQL diagnostics: `DataFusion`'s structured errors, phrased like every
//! other diagnostic chronix gives.
//!
//! `DataFusion` already knows *where* an unresolved name is (with
//! `sql_parser.collect_spans` on) and, for columns, *which* names were
//! available. This module turns that into one sentence, a hint and a byte
//! span — the same shape PromQL errors have — and adds the one suggestion
//! `DataFusion` does not make: tables.

use datafusion::common::{Diagnostic, SchemaError};
use datafusion::error::DataFusionError;

use crate::diagnostics::{Diagnosis, NameKind, Span, byte_offset, suggest, unknown_name_message};

/// Diagnose a SQL error raised for `query`.
///
/// `tables` is the set of tables the caller may be offered; `None` means
/// "do not suggest tables" (the caller's visibility is not known here).
pub(crate) fn sql_diagnosis(
    err: &DataFusionError,
    query: &str,
    tables: Option<&[String]>,
) -> Diagnosis {
    let Some(diag) = err.diagnostic() else {
        return Diagnosis::default();
    };
    let span = diag.span.and_then(|s| {
        let start = byte_offset(query, s.start.line, s.start.column)?;
        let end = byte_offset(query, s.end.line, s.end.column)?;
        (start <= end && end <= query.len()).then_some(Span { start, end })
    });
    let Some(kind) = kind_of(diag) else {
        return Diagnosis {
            message: None,
            hint: diag.helps.first().map(|h| h.message.clone()),
            span,
        };
    };
    let Some((typed, span)) = typed_name(query, span, kind, diag) else {
        return Diagnosis {
            message: None,
            hint: None,
            span,
        };
    };
    let candidates: Vec<String> = match kind {
        NameKind::Table => match tables {
            Some(t) => t.to_vec(),
            None => {
                return Diagnosis {
                    message: Some(format!("unknown table \"{typed}\"")),
                    hint: None,
                    span,
                };
            }
        },
        NameKind::Column => valid_columns(err),
        _ => possible_functions(diag),
    };
    if kind == NameKind::SqlFunction {
        // `DataFusion` always names *some* function, however far it is; hold
        // it to the same distance as every other suggestion, and do not list
        // the hundreds of functions there are when nothing is close.
        let hint = suggest(&typed, candidates.iter().map(String::as_str)).hint();
        let message = match &hint {
            Some(h) => format!("unknown function \"{typed}\" — {h}"),
            None => format!("unknown function \"{typed}\""),
        };
        return Diagnosis {
            message: Some(message),
            hint,
            span,
        };
    }
    let (message, hint) = unknown_name_message(kind, &typed, &candidates);
    Diagnosis {
        message: Some(message),
        hint,
        span,
    }
}

/// Which kind of name a `DataFusion` diagnostic is about, from the shape of
/// the message `DataFusion` builds for each.
fn kind_of(diag: &Diagnostic) -> Option<NameKind> {
    let m = diag.message.as_str();
    if m.starts_with("table '") {
        Some(NameKind::Table)
    } else if m.starts_with("column '") {
        Some(NameKind::Column)
    } else if m.starts_with("Invalid function '") {
        Some(NameKind::SqlFunction)
    } else {
        None
    }
}

/// The name as the query wrote it, and the span narrowed to exactly it.
///
/// A table's span can include its alias and a function's the whole call, so
/// the name is the leading identifier of the spanned text — its last dotted
/// part, without quotes. Without a span, the name in the diagnostic's message.
fn typed_name(
    query: &str,
    span: Option<Span>,
    kind: NameKind,
    diag: &Diagnostic,
) -> Option<(String, Option<Span>)> {
    if let Some(s) = span
        && let Some(text) = query.get(s.start..s.end)
    {
        let ident_len = text
            .char_indices()
            .find(|(_, c)| !(c.is_alphanumeric() || matches!(c, '_' | '.' | '"' | '`')))
            .map_or(text.len(), |(i, _)| i);
        let ident = &text[..ident_len];
        let last_start = ident.rfind('.').map_or(0, |i| i + 1);
        let last = &ident[last_start..];
        let name = last.trim_matches(|c| c == '"' || c == '`');
        if !name.is_empty() {
            let quote = last.len() - last.trim_start_matches(['"', '`']).len();
            let start = s.start + last_start + quote;
            return Some((
                name.to_string(),
                Some(Span {
                    start,
                    end: start + name.len(),
                }),
            ));
        }
    }
    // `table 'x' not found`, `column 'x' not found`, `Invalid function 'x'`.
    let quoted = diag.message.split('\'').nth(1)?;
    let name = quoted.rsplit('.').next()?.to_string();
    let _ = kind;
    Some((name, span))
}

/// The columns the failed reference could have meant, from the structured
/// error `DataFusion` raised under its diagnostic.
fn valid_columns(err: &DataFusionError) -> Vec<String> {
    let mut cur = Some(err);
    while let Some(e) = cur {
        if let DataFusionError::SchemaError(se, _) = e
            && let SchemaError::FieldNotFound { valid_fields, .. } = se.as_ref()
        {
            let mut names: Vec<String> = valid_fields.iter().map(|c| c.name.clone()).collect();
            names.sort_unstable();
            names.dedup();
            return names;
        }
        cur = match e {
            DataFusionError::Diagnostic(_, inner) | DataFusionError::Context(_, inner) => {
                Some(inner.as_ref())
            }
            DataFusionError::Shared(inner) => Some(inner.as_ref()),
            other => {
                std::error::Error::source(other).and_then(|s| s.downcast_ref::<DataFusionError>())
            }
        };
    }
    Vec::new()
}

/// The function `DataFusion` proposed, from its `Possible function 'x'` note.
fn possible_functions(diag: &Diagnostic) -> Vec<String> {
    diag.notes
        .iter()
        .filter_map(|n| {
            n.message
                .strip_prefix("Possible function '")
                .and_then(|rest| rest.strip_suffix('\''))
                .map(str::to_string)
        })
        .collect()
}
