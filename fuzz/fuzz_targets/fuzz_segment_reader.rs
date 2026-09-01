#![no_main]
//! Fuzz target for `SegmentReader::open`.
//!
//! Writes arbitrary bytes to a temporary `.csx` file and tries to open it
//! with `SegmentReader`.  The reader must never panic or cause UB — only
//! clean error returns are acceptable.

use libfuzzer_sys::fuzz_target;

use chronix_engine::segment::SegmentReader;
use std::io::Write;

fuzz_target!(|data: &[u8]| {
    // Create a temp file with the fuzzed bytes.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("fuzz.csx");
    {
        let mut f = std::fs::File::create(&path).expect("create");
        f.write_all(data).expect("write");
    }

    // Try to open — errors are fine, panics are bugs.
    let _ = SegmentReader::open(&path);
});
