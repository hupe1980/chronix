#![no_main]
//! Fuzz target for `WalReader::open` and `WalReader::read_next`.
//!
//! Writes arbitrary bytes to a temporary WAL file and iterates through
//! all records.  The reader must gracefully return errors on corrupt
//! data rather than panicking.

use libfuzzer_sys::fuzz_target;

use chronix_engine::wal::WalReader;
use std::io::Write;

fuzz_target!(|data: &[u8]| {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("fuzz.wal");
    {
        let mut f = std::fs::File::create(&path).expect("create");
        f.write_all(data).expect("write");
    }

    // Open the WAL file; if it succeeds, drain all records.
    if let Ok(reader) = WalReader::open(&path) {
        for _record in reader {
            // Consume; errors are expected on corrupt input.
        }
    }
});
