//! `chronixd inspect <DIR>`: what a data directory holds, without opening it.
//!
//! The report leads with what decides the next step — is it in use, was it
//! closed cleanly, which format generation — then the contents, then a `next`
//! line when something needs doing. Human output stays within 80 columns for
//! measurement names up to 24 characters; `--json` prints the facade's
//! [`Inspection`] as one document.

use std::fmt::Write as _;
use std::path::PathBuf;

use clap::Args;

use chronix::inspect::{DirState, InspectOptions, Inspection, ProblemKind, inspect};

use crate::config::ServerConfig;

/// Arguments of `chronixd inspect`.
#[derive(Args, Debug)]
pub struct InspectArgs {
    /// The data directory to inspect.
    pub dir: PathBuf,

    /// Print one JSON document instead of the report.
    #[arg(long)]
    pub json: bool,

    /// Skip the series sidecars: faster, without series counts.
    #[arg(long)]
    pub fast: bool,

    /// Server configuration to take the cardinality budget from.
    #[arg(long, short = 'c')]
    pub config: Option<PathBuf>,
}

impl InspectArgs {
    /// Run the subcommand: print the report, or an error.
    ///
    /// Exit status 0 when a report was printed (problems included), 1 when
    /// the directory is not a chronix data directory or the configuration
    /// cannot be read.
    pub fn run(&self) -> std::process::ExitCode {
        let (budget, from_config) = match &self.config {
            Some(path) => match ServerConfig::from_toml(path) {
                Ok(c) => (c.database.max_series_cardinality as u64, true),
                Err(e) => {
                    eprintln!("error: {e}");
                    return std::process::ExitCode::FAILURE;
                }
            },
            None => (
                ServerConfig::default().database.max_series_cardinality as u64,
                false,
            ),
        };
        let opts = InspectOptions::default()
            .fast(self.fast)
            .series_budget(Some(budget));
        let report = match inspect(&self.dir, opts) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("error: {e}");
                return std::process::ExitCode::FAILURE;
            }
        };
        if self.json {
            match serde_json::to_string_pretty(&report) {
                Ok(s) => println!("{s}"),
                Err(e) => {
                    eprintln!("error: {e}");
                    return std::process::ExitCode::FAILURE;
                }
            }
        } else {
            let now = i64::try_from(jiff::Timestamp::now().as_nanosecond()).unwrap_or(i64::MAX);
            print!("{}", render(&report, from_config, now));
        }
        std::process::ExitCode::SUCCESS
    }
}

const LABEL: usize = 10;
const WIDTH: usize = 80;

/// The human report. `now_ns` makes relative times reproducible in tests.
#[must_use]
pub fn render(r: &Inspection, budget_from_config: bool, now_ns: i64) -> String {
    let mut out = String::new();
    let generation = r
        .formats
        .iter()
        .map(|f| f.supported)
        .max()
        .unwrap_or_default();
    let readable = r.formats.iter().all(|f| f.readable);

    // The line that decides what to do next.
    let state = match &r.state {
        DirState::Live { .. } => "in use",
        DirState::ClosedCleanly => "closed cleanly",
        DirState::NotClosedCleanly { .. } => "not closed cleanly",
        _ => "state unknown",
    };
    let gen_note = if readable {
        format!("generation {generation}")
    } else {
        "another format generation".to_owned()
    };
    let _ = writeln!(out, "{}  {state} · {gen_note}", r.path);
    out.push('\n');

    // ── Who holds it ─────────────────────────────────────────────────────
    let holder_line = match &r.state {
        DirState::Live { holder } => Some(format!(
            "pid {} on {} · chronix {} · opened {}",
            holder.pid,
            holder.host,
            holder.version,
            ago_rfc3339(&holder.opened, now_ns)
        )),
        DirState::NotClosedCleanly { holder: Some(h) } => Some(format!(
            "pid {} on {} · chronix {} · opened {}",
            h.pid,
            h.host,
            h.version,
            ago_rfc3339(&h.opened, now_ns)
        )),
        DirState::NotClosedCleanly { holder: None } => {
            Some("the WAL holds records no close flushed".to_owned())
        }
        DirState::Unknown { reason, .. } => Some(reason.clone()),
        _ => None,
    };
    if let Some(line) = holder_line {
        field(&mut out, "holder", &line);
    }

    // ── Formats ──────────────────────────────────────────────────────────
    let formats = r
        .formats
        .iter()
        .map(|f| match (f.readable, f.found) {
            (true, _) => format!("{} ✓", f.what),
            (false, Some(v)) => format!("{} ✗ v{v}", f.what),
            (false, None) => format!("{} ✗", f.what),
        })
        .collect::<Vec<_>>()
        .join("  ");
    field(&mut out, "formats", &formats);

    // ── Series ───────────────────────────────────────────────────────────
    let budget = r.series_budget.map(|b| {
        format!(
            "budget {}{}",
            group(b),
            if budget_from_config { "" } else { " (default)" }
        )
    });
    let series = match (r.series_total, budget) {
        (Some(n), Some(b)) => format!("{} · {b}", group(n)),
        (Some(n), None) => group(n),
        (None, Some(b)) => format!("not counted (--fast) · {b}"),
        (None, None) => "not counted".to_owned(),
    };
    field(&mut out, "series", &series);

    // ── WAL ──────────────────────────────────────────────────────────────
    let w = &r.wal;
    let wal = if w.records_above_floor == 0 {
        format!("{} · nothing to replay", plural(w.files, "file"))
    } else {
        let seq = match (w.seq_first, w.seq_last) {
            (Some(a), Some(b)) if a == b => format!(" · seq {}", group(a)),
            (Some(a), Some(b)) => format!(" · seq {}–{}", group(a), group(b)),
            _ => String::new(),
        };
        format!(
            "{} · {} to replay ({}){seq}",
            plural(w.files, "file"),
            plural(w.records_above_floor, "record"),
            bytes(w.bytes_above_floor)
        )
    };
    field(&mut out, "wal", &wal);

    // ── Declared objects ─────────────────────────────────────────────────
    for (i, ru) in r.rollups.iter().enumerate() {
        let until = ru.materialised_until_ns.map_or_else(
            || "nothing materialised yet".to_owned(),
            |t| format!("materialised to {}", ago(t, now_ns)),
        );
        let invalid = if ru.invalid_ranges == 0 {
            String::new()
        } else {
            format!(" · {} to recompute", plural(ru.invalid_ranges, "range"))
        };
        field(
            &mut out,
            if i == 0 { "rollups" } else { "" },
            &format!("{} {until}{invalid}", ru.name),
        );
    }
    if !r.detectors.is_empty() {
        field(&mut out, "detectors", &r.detectors.join(", "));
    }

    // ── Measurements ─────────────────────────────────────────────────────
    out.push('\n');
    if r.measurements.is_empty() {
        out.push_str("no measurements in segments\n");
    } else {
        let name_w = r
            .measurements
            .iter()
            .map(|m| m.name.chars().count())
            .max()
            .unwrap_or(0)
            .clamp("measurement".len(), 24);
        let _ = writeln!(
            out,
            "{:<name_w$}  {:>8}  {:>6}  {:>9}  {:<10}  newest",
            "measurement", "segments", "series", "size", "oldest"
        );
        for m in &r.measurements {
            let name = clip(&m.name, name_w);
            let series = m.series.map_or_else(|| "–".to_owned(), group);
            let flag = if m.pending_drop { "  (dropped)" } else { "" };
            let _ = writeln!(
                out,
                "{name:<name_w$}  {:>8}  {:>6}  {:>9}  {:<10}  {}{flag}",
                group(m.segments),
                series,
                bytes(m.bytes),
                m.oldest.get(..10).unwrap_or(&m.oldest),
                ago(m.newest_ns, now_ns),
            );
        }
    }

    // ── Problems, then what to do ────────────────────────────────────────
    out.push('\n');
    if r.problems.is_empty() {
        field(&mut out, "problems", "none");
    } else {
        for (i, p) in r.problems.iter().enumerate() {
            field(
                &mut out,
                if i == 0 { "problems" } else { "" },
                &format!("✗ {}: {}", p.file, p.message),
            );
        }
    }
    for hint in next_steps(r) {
        field(&mut out, "next", &hint);
    }
    out
}

/// What the reader should do, most urgent first.
fn next_steps(r: &Inspection) -> Vec<String> {
    let mut out = Vec::new();
    let has = |k: ProblemKind| r.problems.iter().any(|p| p.kind == k);
    if has(ProblemKind::ForeignGeneration) {
        out.push(
            "a file is from another format generation: open the directory with the \
             chronix that wrote it, export, and re-ingest"
                .to_owned(),
        );
    }
    if has(ProblemKind::Corrupt) || has(ProblemKind::Unreadable) {
        out.push(
            "a file is damaged or unreadable: copy the directory before opening it, and \
             restore from a backup if the open refuses"
                .to_owned(),
        );
    }
    match &r.state {
        DirState::Live { holder } => out.push(format!(
            "in use by pid {}: inspecting is safe; stop it before copying the directory",
            holder.pid
        )),
        DirState::NotClosedCleanly { .. } => out.push(format!(
            "the next open replays {} ({}); nothing acknowledged is lost",
            plural(r.wal.records_above_floor, "WAL record"),
            bytes(r.wal.bytes_above_floor)
        )),
        _ => {}
    }
    if has(ProblemKind::MissingSidecar) {
        out.push("the next open rebuilds the missing `.series` sidecars".to_owned());
    }
    out
}

/// One labelled line, wrapped at [`WIDTH`] with the label's indent.
fn field(out: &mut String, label: &str, value: &str) {
    let indent = " ".repeat(LABEL);
    let mut line = format!("{label:<LABEL$}");
    let mut used = LABEL;
    let mut first = true;
    for word in value.split(' ') {
        let len = word.chars().count();
        if !first && used + 1 + len > WIDTH {
            out.push_str(line.trim_end());
            out.push('\n');
            line = indent.clone();
            used = LABEL;
            first = true;
        }
        if !first {
            line.push(' ');
            used += 1;
        }
        line.push_str(word);
        used += len;
        first = false;
    }
    out.push_str(line.trim_end());
    out.push('\n');
}

fn clip(s: &str, w: usize) -> String {
    if s.chars().count() <= w {
        s.to_owned()
    } else {
        let mut t: String = s.chars().take(w - 1).collect();
        t.push('…');
        t
    }
}

/// Digits grouped by three with a space: `1 912`.
fn group(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(' ');
        }
        out.push(c);
    }
    out
}

fn plural(n: u64, what: &str) -> String {
    if n == 1 {
        format!("1 {what}")
    } else {
        format!("{} {what}s", group(n))
    }
}

#[allow(clippy::cast_precision_loss)] // a displayed size, one decimal place
fn bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    if n < 1024 {
        return format!("{n} B");
    }
    let mut v = n as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit < UNITS.len() - 1 {
        v /= 1024.0;
        unit += 1;
    }
    format!("{v:.1} {}", UNITS[unit])
}

/// `3 min ago`, `2 h ago`, `5 days ago`, or the date beyond 90 days.
fn ago(ts_ns: i64, now_ns: i64) -> String {
    let secs = now_ns.saturating_sub(ts_ns) / 1_000_000_000;
    if secs < 0 {
        return match -secs {
            0..60 => "just now".to_owned(),
            s @ 60..3_600 => format!("in {} min", s / 60),
            s @ 3_600..172_800 => format!("in {} h", s / 3_600),
            s => format!("in {} days", s / 86_400),
        };
    }
    match secs {
        0..60 => "just now".to_owned(),
        60..3_600 => format!("{} min ago", secs / 60),
        3_600..172_800 => format!("{} h ago", secs / 3_600),
        172_800..7_776_000 => format!("{} days ago", secs / 86_400),
        _ => jiff::Timestamp::from_nanosecond(i128::from(ts_ns)).map_or_else(
            |_| ts_ns.to_string(),
            |t| t.strftime("%Y-%m-%d").to_string(),
        ),
    }
}

fn ago_rfc3339(s: &str, now_ns: i64) -> String {
    s.parse::<jiff::Timestamp>()
        .ok()
        .and_then(|t| i64::try_from(t.as_nanosecond()).ok())
        .map_or_else(|| s.to_owned(), |ns| ago(ns, now_ns))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_and_units_read_like_a_person_wrote_them() {
        assert_eq!(group(1_912), "1 912");
        assert_eq!(group(1_000_000), "1 000 000");
        assert_eq!(group(12), "12");
        assert_eq!(bytes(512), "512 B");
        assert_eq!(bytes(3 * 1024 * 1024 + 400 * 1024), "3.4 MiB");
        assert_eq!(plural(1, "file"), "1 file");
        assert_eq!(plural(3, "file"), "3 files");
        let now = 1_000_000 * 1_000_000_000;
        assert_eq!(ago(now - 30 * 1_000_000_000, now), "just now");
        assert_eq!(ago(now - 180 * 1_000_000_000, now), "3 min ago");
        assert_eq!(ago(now - 7_200 * 1_000_000_000, now), "2 h ago");
        assert_eq!(ago(now - 5 * 86_400 * 1_000_000_000, now), "5 days ago");
        assert_eq!(ago(now + 720 * 1_000_000_000, now), "in 12 min");
    }

    #[test]
    fn a_long_value_wraps_under_its_label() {
        let mut out = String::new();
        field(&mut out, "next", &"word ".repeat(30));
        for line in out.lines() {
            assert!(line.chars().count() <= WIDTH, "{line}");
        }
        assert!(out.lines().nth(1).unwrap().starts_with(&" ".repeat(LABEL)));
    }
}
