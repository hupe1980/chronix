#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! A data directory written by chronix 0.7 is refused **by name** when 0.8
//! opens it: the first error an upgrading operator meets says which
//! generation the directory is, which this build reads, and what to do.
//!
//! `tests/fixtures/gen1_dir` was written by the last generation-1 build and
//! is never regenerated.

use chronix::Chronix;
use chronix::prelude::*;

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap().flatten() {
        let path = entry.path();
        let dest = to.join(entry.file_name());
        if path.is_dir() {
            copy_dir(&path, &dest);
        } else {
            std::fs::copy(&path, &dest).unwrap();
        }
    }
}

#[test]
fn opening_a_0_7_directory_names_the_generation_and_the_remedy() {
    let tmp = tempfile::tempdir().unwrap();
    copy_dir(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/gen1_dir"),
        tmp.path(),
    );
    let err = match Chronix::open(
        ChronixConfig::builder()
            .data_dir(tmp.path())
            .build()
            .unwrap(),
    ) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("a generation-1 directory was opened"),
    };
    for needle in [
        "generation 1",
        "chronix 0.7 and earlier",
        "generation 2",
        "re-ingest",
    ] {
        assert!(err.contains(needle), "the refusal lacks {needle:?}: {err}");
    }
    let lower = err.to_lowercase();
    for wrong in ["crc", "checksum", "corrupt"] {
        assert!(
            !lower.contains(wrong),
            "a version mismatch reported as {wrong}: {err}"
        );
    }
}
