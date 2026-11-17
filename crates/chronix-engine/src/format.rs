//! The on-disk format versions, and the one rule for checking them.
//!
//! Chronix writes four durable formats — `.csx` segments, the WAL, the catalog
//! snapshot and the `.series` sidecar — and checks all four the same way.
//!
//! # The rule
//!
//! **A format version is checked for equality, in both directions, and a
//! mismatch is its own error — never corruption.** Corruption means the bytes
//! are damaged and the answer is a restore; a version mismatch means the
//! software and the directory disagree about the layout, and the answer is to
//! match them up. There is no reader for another generation and no migration:
//! a directory from another generation is refused by name, with the remedy in
//! the message.
//!
//! # Generations
//!
//! The four versions move together, so a directory belongs to one
//! *generation*:
//!
//! | Generation | Written by |
//! |---|---|
//! | 1 | chronix 0.7 and earlier |
//! | 2 | chronix 0.8 and later — **frozen** |
//!
//! Generation 2 is frozen: a minor release does nothing to a data directory.
//! A version moves only when the bytes change, all four together, and the
//! change is stated in `CHANGELOG.md`.

/// `.csx` segment files.
pub const SEGMENT_FORMAT_VERSION: u16 = 2;

/// Write-ahead log files.
pub const WAL_FORMAT_VERSION: u16 = 2;

/// The catalog manifest snapshot.
pub const CATALOG_FORMAT_VERSION: u32 = 2;

/// The per-segment `.series` sidecar.
pub const SERIES_INDEX_FORMAT_VERSION: u8 = 2;

/// What to tell somebody whose directory this build cannot read.
///
/// It names the thing, both generations and what to do, because it is the
/// first message an operator sees after an upgrade.
#[must_use]
pub fn version_mismatch(what: &str, found: u64, expected: u64) -> String {
    format!(
        "{what} is format generation {found} ({}), and this build of chronix \
         reads generation {expected} ({}). Chronix has no on-disk migration: a \
         data directory belongs to one generation. Either run the chronix that \
         wrote it, or start a new data directory and re-ingest into it.",
        written_by(found),
        written_by(expected),
    )
}

/// Which releases write a generation, in words an operator can act on.
#[must_use]
pub fn written_by(generation: u64) -> &'static str {
    match generation {
        1 => "chronix 0.7 and earlier",
        2 => "chronix 0.8 and later",
        _ => "a chronix newer than this one",
    }
}

/// Check a format version, returning the message to report on a mismatch.
///
/// Equality in both directions: see the module documentation for why an
/// older file is refused as firmly as a newer one.
#[must_use]
pub fn check(what: &str, found: u64, expected: u64) -> Option<String> {
    (found != expected).then(|| version_mismatch(what, found, expected))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_exact_match_passes() {
        assert!(check("a segment", 2, 2).is_none());
    }

    #[test]
    fn an_older_version_is_refused_as_firmly_as_a_newer_one() {
        // Both directions are a mismatch: there is no reader for an older one.
        assert!(check("a catalog", 1, 2).is_some());
        assert!(check("a catalog", 3, 2).is_some());
    }

    #[test]
    fn the_message_names_the_remedy_not_just_the_number() {
        let m = check("the WAL", 1, 2).expect("mismatch");
        assert!(m.contains("generation 1"), "{m}");
        assert!(m.contains("generation 2"), "{m}");
        assert!(
            m.contains("chronix 0.7 and earlier"),
            "names who wrote it: {m}"
        );
        assert!(m.contains("chronix 0.8 and later"), "{m}");
        assert!(
            m.contains("re-ingest") || m.contains("new data directory"),
            "a version mismatch is the first error an operator meets; it has \
             to say what to do: {m}"
        );
    }

    #[test]
    fn every_durable_format_is_on_the_same_generation() {
        // Keeping the four in step is not required by anything — it is a
        // convention that makes "which generation is this directory?" one
        // question instead of four, and it is why the format-changing work is
        // batched into one break rather than trickled.
        let g = u64::from(SEGMENT_FORMAT_VERSION);
        assert_eq!(u64::from(WAL_FORMAT_VERSION), g);
        assert_eq!(u64::from(CATALOG_FORMAT_VERSION), g);
        assert_eq!(u64::from(SERIES_INDEX_FORMAT_VERSION), g);
    }
}

#[cfg(test)]
mod policy_tests {
    /// Every durable reader must refuse a foreign version through a variant
    /// that is *not* a corruption variant, and must build its message here.
    ///
    /// This is a source check rather than a behavioural one because the
    /// alternative is writing four files at three versions each. The thing
    /// it prevents is the one that was there: the `.series` sidecar reported
    /// a version mismatch as `IndexError::Corrupt`, so an operator who had
    /// upgraded chronix went looking for a failing disk.
    #[test]
    fn no_durable_reader_reports_a_version_mismatch_as_corruption() {
        for (name, src) in [
            ("wal/reader.rs", include_str!("wal/reader.rs")),
            (
                "index/series_index.rs",
                include_str!("index/series_index.rs"),
            ),
            ("index/catalog.rs", include_str!("index/catalog.rs")),
            ("segment/header.rs", include_str!("segment/header.rs")),
        ] {
            // Find the version check and read what it returns just after it.
            let Some(at) = src.find("format::check").or_else(|| src.find("!= VERSION")) else {
                panic!("{name}: no format-version check found — did the reader stop checking?");
            };
            // Only as far as the `return Err(..)` this check guards. A
            // wider window ran into the *next* function's doc comment,
            // which mentions `WalError::Corruption` — and a guard whose
            // false positives are noise is a guard people learn to ignore.
            let tail = &src[at..];
            let end = tail
                .find("return Err(")
                .and_then(|i| tail[i..].find(';').map(|j| i + j + 1))
                .unwrap_or(tail.len());
            // Comments stripped: the window otherwise matched this very
            // rule's own justification ("deliberately not `Corrupt`"), which
            // is a guard tripping over prose about itself.
            let window: String = tail[..end]
                .lines()
                .map(|l| l.split("//").next().unwrap_or(""))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                !window.contains("Corrupt"),
                "{name} reports a format-version mismatch as corruption. The \
                 bytes are intact and the reader is from another generation; \
                 saying `Corrupt` sends an operator to a restore when the fix \
                 is to match the versions up."
            );
        }
    }
}
