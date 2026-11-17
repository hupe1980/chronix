#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap
//! Files written by generation 1 (chronix 0.7 and earlier) are refused by
//! every reader **as a version mismatch** — never as a CRC error, a decode
//! error or corruption, which would send an operator who has just upgraded
//! looking for a failing disk.
//!
//! The fixtures in `tests/fixtures/gen1/` were written by the last
//! generation-1 build and are never regenerated: they are the evidence.

use std::path::{Path, PathBuf};

use chronix_core::WalError;
use chronix_engine::index::IndexError;
use chronix_engine::segment::SegmentError;

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/gen1")
        .join(name)
}

fn says_generations(message: &str) {
    assert!(
        message.contains("generation 1") && message.contains("generation 2"),
        "the refusal must name both generations: {message}"
    );
    assert!(
        message.contains("re-ingest"),
        "the refusal must say what to do: {message}"
    );
}

#[test]
fn a_generation_1_segment_is_a_version_mismatch() {
    match chronix_engine::segment::reader::SegmentReader::open(fixture("segment.csx")) {
        Err(SegmentError::UnsupportedVersion { version, detail }) => {
            assert_eq!(version, 1);
            says_generations(&detail);
        }
        Err(other) => panic!("refused, but as the wrong error: {other:?}"),
        Ok(_) => panic!("a generation-1 segment was opened"),
    }
}

#[test]
fn a_generation_1_wal_is_a_version_mismatch() {
    match chronix_engine::wal::WalReader::open(fixture("wal_00000000000000000001.cxwl")) {
        Err(WalError::UnsupportedVersion { detail, .. }) => says_generations(&detail),
        Err(other) => panic!("refused, but as the wrong error: {other:?}"),
        Ok(_) => panic!("a generation-1 WAL was opened"),
    }
}

#[test]
fn a_generation_1_catalog_snapshot_is_a_version_mismatch() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::copy(
        fixture("manifest.snapshot.bin"),
        tmp.path().join("manifest.snapshot.bin"),
    )
    .unwrap();
    match chronix_engine::index::catalog::SegmentCatalog::open(tmp.path()) {
        Err(IndexError::UnsupportedVersion { detail }) => says_generations(&detail),
        Err(other) => panic!("refused, but as the wrong error: {other:?}"),
        Ok(_) => panic!("a generation-1 catalog was opened"),
    }
}

#[test]
fn a_generation_1_series_sidecar_is_a_version_mismatch() {
    let tmp = tempfile::tempdir().unwrap();
    let seg = tmp.path().join("segment.csx");
    std::fs::copy(fixture("segment.csx"), &seg).unwrap();
    std::fs::copy(fixture("segment.series"), tmp.path().join("segment.series")).unwrap();
    // Reading the stamp from a generation-1 header is itself a mismatch…
    match chronix_engine::index::series_index::SegmentStamp::read(&seg) {
        Err(IndexError::UnsupportedVersion { detail }) => says_generations(&detail),
        other => panic!("the stamp of a generation-1 segment: {other:?}"),
    }
    // …and so is the sidecar, whatever stamp it is asked for.
    let any = chronix_engine::index::series_index::SegmentStamp {
        created_at: 0,
        row_count: 0,
        series_count: 0,
    };
    match chronix_engine::index::series_index::read(&seg, any) {
        Err(IndexError::UnsupportedVersion { detail }) => says_generations(&detail),
        other => panic!("a generation-1 sidecar: {other:?}"),
    }
}
