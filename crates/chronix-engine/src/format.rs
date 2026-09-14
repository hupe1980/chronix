//! The on-disk format versions, and the one rule for checking them.
//!
//! Chronix writes four durable formats and every one of them used to carry
//! its own version constant, its own comparison and its own error text:
//!
//! - the `.csx` segment header refused anything but an exact match, and said
//!   so through a dedicated `UnsupportedVersion` variant;
//! - the WAL file header refused anything but an exact match, as an
//!   `InvalidHeader`;
//! - the `.series` sidecar refused anything but an exact match and reported
//!   it as **`Corrupt`** — which points an operator at a failing disk when
//!   what actually happened is that they upgraded chronix;
//! - and the catalog snapshot refused only a **newer** version, silently
//!   accepting an older one it has no code to interpret.
//!
//! That last one is an asymmetry nobody decided. The three others agree that
//! a version is an identity, not an ordering: there is no partial reader for
//! a superseded layout, because the layouts were superseded before anyone
//! could have data under them.
//!
//! # The rule
//!
//! **A format version is checked for equality, in both directions, and a
//! mismatch is its own error — never corruption.** The two are different
//! facts with different remedies: corruption means the bytes are damaged and
//! the answer is a restore; a version mismatch means the software and the
//! directory disagree about the layout and the answer is to match them up.
//!
//! # Changing a format
//!
//! Bump the constant here **and** state the break in `CHANGELOG.md`. There is
//! deliberately no migration: chronix reads exactly the generation it writes,
//! and a directory from another generation is refused by name with the
//! remedy in the message. That is a decision with an expiry date — it holds
//! only while the project is willing to tell a consumer to recreate their
//! data directory, which is a promise 1.0 will have to replace.
//!
//! **Bump it when the bytes change, not before.** The version exists to make
//! a foreign directory *legible*: without a bump, an old directory has
//! matching magic and a matching version, so the reader proceeds into a
//! layout it does not understand and the failure arrives as a CRC error, a
//! bounds check or a decode error — which is exactly the "report a version
//! mismatch as corruption" confusion this module exists to remove. A bump
//! with no layout change is the mirror mistake: it invalidates every
//! directory in exchange for nothing. All four are still at **1** because
//! nothing has changed the bytes yet; the format-changing work is batched so
//! they move together, once.

/// `.csx` segment files.
pub const SEGMENT_FORMAT_VERSION: u16 = 1;

/// Write-ahead log files.
pub const WAL_FORMAT_VERSION: u16 = 1;

/// The catalog manifest snapshot.
pub const CATALOG_FORMAT_VERSION: u32 = 1;

/// The per-segment `.series` sidecar.
pub const SERIES_INDEX_FORMAT_VERSION: u8 = 1;

/// What to tell somebody whose directory this build cannot read.
///
/// The message an operator meets before they have any other information, so
/// it names the thing, both versions, and what to do — rather than a bare
/// number, which is what three of the four used to print.
#[must_use]
pub fn version_mismatch(what: &str, found: u64, expected: u64) -> String {
    format!(
        "{what} is format version {found}, and this build of chronix reads \
         version {expected}. Chronix has no on-disk migration: a data \
         directory belongs to one format generation. Either run the version \
         of chronix that wrote it, or start a new data directory and re-ingest \
         — see the release notes for the version that changed it."
    )
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
        // The catalog used to accept an older snapshot, having no code to
        // interpret one. Both directions are a mismatch.
        assert!(check("a catalog", 1, 2).is_some());
        assert!(check("a catalog", 3, 2).is_some());
    }

    #[test]
    fn the_message_names_the_remedy_not_just_the_number() {
        let m = check("the WAL", 1, 2).expect("mismatch");
        assert!(m.contains("version 1"), "{m}");
        assert!(m.contains("version 2"), "{m}");
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
