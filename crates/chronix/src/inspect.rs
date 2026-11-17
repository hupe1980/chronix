//! Read a data directory without opening it.
//!
//! [`inspect`] answers the first questions about a data directory — is a
//! process using it, was it closed cleanly, which format generation is it,
//! what does it hold, how much would the next open replay — from metadata
//! alone. It never writes, creates, locks, truncates or replays anything, so
//! it is safe on a directory a live process holds, on a read-only copy and on
//! one written by another chronix version.
//!
//! ```rust,no_run
//! use chronix::inspect::{inspect, InspectOptions};
//!
//! let report = inspect("/var/lib/chronix", InspectOptions::default())?;
//! println!("{:?}: {} measurements", report.state, report.measurements.len());
//! for problem in &report.problems {
//!     eprintln!("{}: {}", problem.file, problem.message);
//! }
//! # Ok::<(), chronix::error::DbError>(())
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::Serialize;

use chronix_engine::index::SegmentCatalog;
use chronix_engine::index::series_index;
use chronix_engine::segment::{self, SegmentError};

use crate::db::{catalog_dir_of, segments_dir_of, wal_dir_of};
use crate::error::{DbError, Result};

/// The JSON document's schema identifier.
pub const SCHEMA: &str = "chronix.inspect/1";

/// What to read.
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub struct InspectOptions {
    /// Skip the `.series` sidecars: no series counts, but one read per
    /// segment fewer.
    pub fast: bool,
    /// The cardinality budget to report beside the series count, usually
    /// taken from the server's configuration.
    pub series_budget: Option<u64>,
}

impl InspectOptions {
    /// Skip the series sidecars (see [`fast`](Self::fast)).
    #[must_use]
    pub fn fast(mut self, fast: bool) -> Self {
        self.fast = fast;
        self
    }

    /// Report this cardinality budget beside the series count.
    #[must_use]
    pub fn series_budget(mut self, budget: Option<u64>) -> Self {
        self.series_budget = budget;
        self
    }
}

/// The process recorded in a data directory's `LOCK` file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct LockHolder {
    /// Process id.
    pub pid: u32,
    /// Host name.
    pub host: String,
    /// When it opened the directory, RFC 3339 UTC.
    pub opened: String,
    /// The chronix version it ran.
    pub version: String,
}

/// Whether a process is using the directory, and how the last one left it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum DirState {
    /// A running process on this host has the directory open.
    Live {
        /// The process.
        holder: LockHolder,
    },
    /// The last process closed it: the next open replays nothing.
    ClosedCleanly,
    /// The last process stopped without closing it: the next open replays
    /// the WAL records in [`Inspection::wal`].
    NotClosedCleanly {
        /// The process, when the `LOCK` file still names it.
        holder: Option<LockHolder>,
    },
    /// Cannot be told from here.
    Unknown {
        /// The process the `LOCK` file names, if any.
        holder: Option<LockHolder>,
        /// Why not.
        reason: String,
    },
}

/// One on-disk format and the generation found for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct FormatVersion {
    /// `"segment"`, `"wal"`, `"catalog"` or `"series"`.
    pub what: &'static str,
    /// The version found, or `None` when no file of this kind exists or its
    /// version could not be read.
    pub found: Option<u64>,
    /// The version this build reads.
    pub supported: u64,
    /// Whether every file of this kind is readable by this build.
    pub readable: bool,
}

/// One measurement's footprint on disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct MeasurementSummary {
    /// The measurement.
    pub name: String,
    /// Live segment files.
    pub segments: u64,
    /// Distinct series in those segments; `None` with
    /// [`InspectOptions::fast`] or when a sidecar could not be read.
    pub series: Option<u64>,
    /// Oldest timestamp, nanoseconds since the epoch.
    pub oldest_ns: i64,
    /// Newest timestamp, nanoseconds since the epoch.
    pub newest_ns: i64,
    /// [`oldest_ns`](Self::oldest_ns) as RFC 3339 UTC.
    pub oldest: String,
    /// [`newest_ns`](Self::newest_ns) as RFC 3339 UTC.
    pub newest: String,
    /// Bytes on disk.
    pub bytes: u64,
    /// Dropped, and waiting out the grace period before its files go.
    pub pending_drop: bool,
}

/// A declared rollup and how far it has been materialised.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct RollupSummary {
    /// The rollup.
    pub name: String,
    /// Exclusive end of the newest materialised bucket.
    pub materialised_until_ns: Option<i64>,
    /// Ranges below that point waiting to be recomputed.
    pub invalid_ranges: u64,
}

/// What the next open would replay.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct WalSummary {
    /// WAL files in the directory.
    pub files: u64,
    /// The sequence number every record at or below is already in a segment.
    pub floor: u64,
    /// Records above the floor: what the next open replays.
    pub records_above_floor: u64,
    /// Bytes those records occupy.
    pub bytes_above_floor: u64,
    /// The first sequence number above the floor.
    pub seq_first: Option<u64>,
    /// The last sequence number above the floor.
    pub seq_last: Option<u64>,
}

/// What a problem is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ProblemKind {
    /// The file is damaged.
    Corrupt,
    /// A segment has no `.series` sidecar.
    MissingSidecar,
    /// The file was written by another format generation.
    ForeignGeneration,
    /// The file could not be read.
    Unreadable,
}

/// One file that could not be read as expected. Problems never stop the
/// report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct Problem {
    /// The file, relative to the data directory.
    pub file: String,
    /// What is wrong.
    pub kind: ProblemKind,
    /// The reader's message.
    pub message: String,
}

/// Everything [`inspect`] found.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct Inspection {
    /// Always [`SCHEMA`].
    pub schema: &'static str,
    /// The directory, as given.
    pub path: String,
    /// Whether a process is using it.
    pub state: DirState,
    /// The format generation of each on-disk format.
    pub formats: Vec<FormatVersion>,
    /// Distinct series across all segments; `None` with
    /// [`InspectOptions::fast`] or when a sidecar could not be read.
    pub series_total: Option<u64>,
    /// The budget from [`InspectOptions::series_budget`].
    pub series_budget: Option<u64>,
    /// Measurements, by name.
    pub measurements: Vec<MeasurementSummary>,
    /// Declared rollups, by name.
    pub rollups: Vec<RollupSummary>,
    /// Declared detectors, by name.
    pub detectors: Vec<String>,
    /// The write-ahead log.
    pub wal: WalSummary,
    /// Files that could not be read as expected.
    pub problems: Vec<Problem>,
}

/// Inspect the data directory at `path` without opening it.
///
/// Reads the catalog, each segment's header and footer, the `.series`
/// sidecars (unless [`InspectOptions::fast`]), the WAL record headers and the
/// `LOCK` file. Writes nothing and takes no lock, so a process opening the
/// directory at the same moment is unaffected.
///
/// # Errors
///
/// [`DbError::InvalidRequest`] if `path` holds neither `catalog/` nor
/// `segments/`. Everything else — a damaged file, a foreign format
/// generation — is reported in [`Inspection::problems`].
pub fn inspect(path: impl AsRef<Path>, opts: InspectOptions) -> Result<Inspection> {
    let dir = path.as_ref();
    let catalog_dir = catalog_dir_of(dir);
    let segments_dir = segments_dir_of(dir);
    if !catalog_dir.is_dir() && !segments_dir.is_dir() {
        return Err(DbError::InvalidRequest(format!(
            "{} is not a chronix data directory (no catalog/ or segments/)",
            dir.display()
        )));
    }

    let mut problems = Vec::new();
    let rel = |p: &Path| p.strip_prefix(dir).unwrap_or(p).display().to_string();

    // ── Catalog ────────────────────────────────────────────────────────
    let catalog = match SegmentCatalog::open_read_only(&catalog_dir) {
        Ok(c) => Some(c),
        Err(e) => {
            let kind = match e {
                chronix_engine::index::IndexError::UnsupportedVersion { .. } => {
                    ProblemKind::ForeignGeneration
                }
                chronix_engine::index::IndexError::Io(_) => ProblemKind::Unreadable,
                _ => ProblemKind::Corrupt,
            };
            problems.push(Problem {
                file: rel(&catalog_dir),
                kind,
                message: e.to_string(),
            });
            None
        }
    };
    let catalog_supported = u64::from(chronix_engine::format::CATALOG_FORMAT_VERSION);
    let mut formats = vec![FormatVersion {
        what: "catalog",
        found: catalog.as_ref().map(|_| catalog_supported),
        supported: catalog_supported,
        readable: catalog.is_some(),
    }];

    // ── Segments and sidecars ──────────────────────────────────────────
    let segment_supported = u64::from(segment::header::VERSION);
    let series_supported = u64::from(chronix_engine::format::SERIES_INDEX_FORMAT_VERSION);
    let mut segment_found: Option<u64> = None;
    let mut segment_readable = true;
    let mut series_found: Option<u64> = None;
    let mut series_readable = true;

    #[derive(Default)]
    struct Acc {
        segments: u64,
        oldest: Option<i64>,
        newest: Option<i64>,
        bytes: u64,
        series: Option<BTreeSet<String>>,
    }
    let mut by_measurement: BTreeMap<String, Acc> = BTreeMap::new();
    let mut all_series: Option<BTreeSet<String>> = (!opts.fast).then(BTreeSet::new);

    if let Some(cat) = &catalog {
        for entry in cat.all_segments() {
            if !matches!(entry.state, chronix_core::SegmentState::Active) {
                continue;
            }
            let acc = by_measurement
                .entry(entry.measurement.clone())
                .or_insert_with(|| Acc {
                    series: (!opts.fast).then(BTreeSet::new),
                    ..Acc::default()
                });
            acc.segments += 1;
            acc.oldest = Some(
                acc.oldest
                    .map_or(entry.min_timestamp, |o| o.min(entry.min_timestamp)),
            );
            acc.newest = Some(
                acc.newest
                    .map_or(entry.max_timestamp, |n| n.max(entry.max_timestamp)),
            );

            let seg_path = entry.file.resolve(&segments_dir);
            let meta = match segment::read_meta(&seg_path) {
                Ok(m) => {
                    segment_found = Some(u64::from(m.header.version));
                    acc.bytes += m.byte_size;
                    Some(m)
                }
                Err(e) => {
                    acc.bytes += entry.byte_size;
                    segment_readable = false;
                    let kind = match &e {
                        SegmentError::UnsupportedVersion { version, .. } => {
                            segment_found = Some(u64::from(*version));
                            ProblemKind::ForeignGeneration
                        }
                        SegmentError::Io(_) => ProblemKind::Unreadable,
                        _ => ProblemKind::Corrupt,
                    };
                    problems.push(Problem {
                        file: rel(&seg_path),
                        kind,
                        message: e.to_string(),
                    });
                    None
                }
            };

            if opts.fast {
                continue;
            }
            let sidecar = series_index::sidecar_path(&seg_path);
            if series_found.is_none()
                && let Some(v) = sidecar_version(&sidecar)
            {
                series_found = Some(v);
            }
            let keys = match &meta {
                None => None,
                Some(_) if !sidecar.exists() => {
                    problems.push(Problem {
                        file: rel(&sidecar),
                        kind: ProblemKind::MissingSidecar,
                        message: "no `.series` sidecar; the next open rebuilds it from the segment"
                            .to_owned(),
                    });
                    None
                }
                Some(m) => {
                    match series_index::read(&seg_path, series_index::SegmentStamp::of(&m.header)) {
                        Ok(keys) => Some(keys),
                        Err(e) => {
                            series_readable = false;
                            let kind = match e {
                                chronix_engine::index::IndexError::UnsupportedVersion {
                                    ..
                                } => ProblemKind::ForeignGeneration,
                                chronix_engine::index::IndexError::Io(_) => ProblemKind::Unreadable,
                                _ => ProblemKind::Corrupt,
                            };
                            problems.push(Problem {
                                file: rel(&sidecar),
                                kind,
                                message: e.to_string(),
                            });
                            None
                        }
                    }
                }
            };
            match keys {
                Some(keys) => {
                    for k in keys {
                        let canonical = k.canonical_form().to_owned();
                        if let Some(set) = acc.series.as_mut() {
                            set.insert(canonical.clone());
                        }
                        if let Some(all) = all_series.as_mut() {
                            all.insert(canonical);
                        }
                    }
                }
                None => {
                    // An unreadable sidecar makes the counts it feeds unknown,
                    // not smaller: a wrong number is worse than none.
                    acc.series = None;
                    all_series = None;
                }
            }
        }
    }
    formats.push(FormatVersion {
        what: "segment",
        found: segment_found,
        supported: segment_supported,
        readable: segment_readable,
    });

    let measurements = by_measurement
        .into_iter()
        .map(|(name, acc)| {
            let oldest_ns = acc.oldest.unwrap_or(0);
            let newest_ns = acc.newest.unwrap_or(0);
            MeasurementSummary {
                pending_drop: catalog
                    .as_ref()
                    .is_some_and(|c| c.is_measurement_pending_drop(&name)),
                name,
                segments: acc.segments,
                series: acc.series.map(|s| s.len() as u64),
                oldest_ns,
                newest_ns,
                oldest: rfc3339(oldest_ns),
                newest: rfc3339(newest_ns),
                bytes: acc.bytes,
            }
        })
        .collect();

    // ── Declared objects ───────────────────────────────────────────────
    let mut rollups = Vec::new();
    let mut detectors = Vec::new();
    if let Some(cat) = &catalog {
        for (name, record) in cat.rollups() {
            let state: crate::rollup::RollupState =
                postcard::from_bytes(&record.state).unwrap_or_default();
            rollups.push(RollupSummary {
                name: name.to_owned(),
                materialised_until_ns: state.materialised_until,
                invalid_ranges: state.invalid.len() as u64,
            });
        }
        detectors = cat.detectors().map(|(n, _)| n.to_owned()).collect();
    }
    rollups.sort_by(|a, b| a.name.cmp(&b.name));
    detectors.sort();

    // ── WAL ────────────────────────────────────────────────────────────
    let wal_dir = wal_dir_of(dir);
    let wal_supported = u64::from(chronix_engine::wal::WAL_VERSION);
    let mut wal_found: Option<u64> = None;
    let mut wal_readable = true;
    let mut wal = WalSummary {
        floor: catalog.as_ref().map_or(0, SegmentCatalog::wal_floor),
        ..WalSummary::default()
    };
    match chronix_engine::wal::list_wal_files(&wal_dir) {
        Ok(files) => {
            wal.files = files.len() as u64;
            for (_, file) in files {
                match chronix_engine::wal::scan_headers(&file, wal.floor) {
                    Ok(scan) => {
                        wal_found = Some(u64::from(scan.version));
                        if u64::from(scan.version) != wal_supported {
                            wal_readable = false;
                            problems.push(Problem {
                                file: rel(&file),
                                kind: ProblemKind::ForeignGeneration,
                                message: chronix_engine::format::version_mismatch(
                                    "this write-ahead log",
                                    u64::from(scan.version),
                                    wal_supported,
                                ),
                            });
                            continue;
                        }
                        wal.records_above_floor += scan.records_above_floor;
                        wal.bytes_above_floor += scan.bytes_above_floor;
                        if wal.seq_first.is_none() {
                            wal.seq_first = scan.seq_first;
                        }
                        if scan.seq_last.is_some() {
                            wal.seq_last = scan.seq_last;
                        }
                        if scan.torn_tail {
                            problems.push(Problem {
                                file: rel(&file),
                                kind: ProblemKind::Corrupt,
                                message: "ends inside a record (a write a crash interrupted); \
                                          the next open skips it"
                                    .to_owned(),
                            });
                        }
                    }
                    Err(e) => {
                        wal_readable = false;
                        problems.push(Problem {
                            file: rel(&file),
                            kind: ProblemKind::Corrupt,
                            message: e.to_string(),
                        });
                    }
                }
            }
        }
        Err(e) => {
            wal_readable = false;
            problems.push(Problem {
                file: rel(&wal_dir),
                kind: ProblemKind::Unreadable,
                message: e.to_string(),
            });
        }
    }
    formats.push(FormatVersion {
        what: "wal",
        found: wal_found,
        supported: wal_supported,
        readable: wal_readable,
    });
    formats.push(FormatVersion {
        what: "series",
        found: series_found,
        supported: series_supported,
        readable: series_readable,
    });

    // ── Who holds it ───────────────────────────────────────────────────
    let state = dir_state(&dir.join("LOCK"), wal.records_above_floor);

    Ok(Inspection {
        schema: SCHEMA,
        path: dir.display().to_string(),
        state,
        formats,
        series_total: all_series.map(|s| s.len() as u64),
        series_budget: opts.series_budget,
        measurements,
        rollups,
        detectors,
        wal,
        problems,
    })
}

/// The version byte of a `.series` sidecar, if the file exists.
fn sidecar_version(path: &Path) -> Option<u64> {
    use std::io::Read;
    let mut buf = [0u8; 5];
    std::fs::File::open(path).ok()?.read_exact(&mut buf).ok()?;
    Some(u64::from(buf[4]))
}

fn rfc3339(ns: i64) -> String {
    chrono::DateTime::from_timestamp_nanos(ns).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// The running process on this host that holds `data_dir`, if any.
///
/// The same reading [`inspect`] reports as [`DirState::Live`]: a holder
/// record naming this host and a process that is alive.
pub(crate) fn live_holder(data_dir: &Path) -> Option<LockHolder> {
    match dir_state(&data_dir.join("LOCK"), 0) {
        DirState::Live { holder } => Some(holder),
        _ => None,
    }
}

/// Read `LOCK` and decide whether its holder is still running.
fn dir_state(lock: &Path, records_above_floor: u64) -> DirState {
    let text = match std::fs::read_to_string(lock) {
        Ok(t) => t,
        Err(_) => {
            return DirState::Unknown {
                holder: None,
                reason: "no LOCK file".to_owned(),
            };
        }
    };
    let Some(holder) = LockHolder::parse(&text) else {
        // Empty: the last close emptied it. A WAL with records above the
        // floor contradicts that — a holder record lost to a power cut — and
        // the WAL is the one that decides what the next open does.
        return if records_above_floor > 0 {
            DirState::NotClosedCleanly { holder: None }
        } else {
            DirState::ClosedCleanly
        };
    };
    if holder.host != local_host() {
        return DirState::Unknown {
            reason: format!(
                "opened on {} — live there, or a copy taken while it ran",
                holder.host
            ),
            holder: Some(holder),
        };
    }
    match process_alive(holder.pid) {
        Some(true) => DirState::Live { holder },
        Some(false) => DirState::NotClosedCleanly {
            holder: Some(holder),
        },
        None => DirState::Unknown {
            holder: Some(holder),
            reason: "this platform cannot check a process id".to_owned(),
        },
    }
}

impl LockHolder {
    /// The record this process writes into `LOCK` when it opens a directory.
    pub(crate) fn current() -> Self {
        Self {
            pid: std::process::id(),
            host: local_host(),
            opened: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            version: env!("CARGO_PKG_VERSION").to_owned(),
        }
    }

    /// `key=value` lines; not a durable format — rewritten on every open.
    pub(crate) fn render(&self) -> String {
        format!(
            "pid={}\nhost={}\nopened={}\nversion={}\n",
            self.pid, self.host, self.opened, self.version
        )
    }

    fn parse(text: &str) -> Option<Self> {
        let mut fields: BTreeMap<&str, &str> = BTreeMap::new();
        for line in text.lines() {
            if let Some((k, v)) = line.split_once('=') {
                fields.insert(k.trim(), v.trim());
            }
        }
        Some(Self {
            pid: fields.get("pid")?.parse().ok()?,
            host: (*fields.get("host")?).to_owned(),
            opened: fields
                .get("opened")
                .map_or_else(String::new, |s| (*s).to_owned()),
            version: fields
                .get("version")
                .map_or_else(String::new, |s| (*s).to_owned()),
        })
    }
}

#[cfg(unix)]
fn local_host() -> String {
    rustix::system::uname()
        .nodename()
        .to_string_lossy()
        .into_owned()
}

#[cfg(not(unix))]
fn local_host() -> String {
    std::env::var("COMPUTERNAME").unwrap_or_default()
}

#[cfg(unix)]
fn process_alive(pid: u32) -> Option<bool> {
    let pid = rustix::process::Pid::from_raw(i32::try_from(pid).ok()?)?;
    match rustix::process::test_kill_process(pid) {
        Ok(()) => Some(true),
        // Exists, owned by another user.
        Err(e) if e == rustix::io::Errno::PERM => Some(true),
        Err(e) if e == rustix::io::Errno::SRCH => Some(false),
        Err(_) => None,
    }
}

#[cfg(not(unix))]
fn process_alive(_pid: u32) -> Option<bool> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn holder_record_round_trips_and_empty_means_none() {
        let h = LockHolder::current();
        assert_eq!(LockHolder::parse(&h.render()), Some(h));
        assert_eq!(LockHolder::parse(""), None);
        assert_eq!(LockHolder::parse("garbage"), None);
    }

    #[test]
    fn this_process_is_alive() {
        #[cfg(unix)]
        assert_eq!(process_alive(std::process::id()), Some(true));
    }
}
