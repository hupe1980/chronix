#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
#![allow(unsafe_code)] // a global allocator is the point of this test
//! A corrupt pco block must not be able to ask for a large allocation.
//!
//! Every other codec in this crate reads its value count from a header this
//! crate wrote, so `coding::checked_decode_count` is the whole guard. pco is
//! different: the pco *file* carries its own chunk metadata, including counts
//! and page sizes, and that metadata is inside the payload — the part a
//! flipped bit on eMMC lands in. pco 1.0.3 fixed a large-allocation issue on
//! antagonistic input, and relying on that fix would make our safety property
//! a property of a dependency's patch level. So `PcoDecoder` sizes the one
//! allocation it owns from *our* 4-byte count, bounded by the crate ceiling,
//! and hands pco a fixed-size slice.
//!
//! This test watches the allocator to prove it, rather than asserting that an
//! error was returned — an error is what you get *after* the allocation
//! succeeds, and on the 512 MB gateway the allocation is the thing that kills
//! the process.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    /// Largest single allocation requested on this thread since the last reset.
    static MAX_ALLOC: Cell<usize> = const { Cell::new(0) };
}

struct Watching;

// SAFETY-equivalent reasoning: this forwards every call to `System` unchanged
// and only records sizes. `try_with` is used because the thread-local may
// already be destroyed during thread teardown.
unsafe impl GlobalAlloc for Watching {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = MAX_ALLOC.try_with(|m| m.set(m.get().max(layout.size())));
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let _ = MAX_ALLOC.try_with(|m| m.set(m.get().max(new_size)));
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOC: Watching = Watching;

fn largest_alloc_during<T>(f: impl FnOnce() -> T) -> usize {
    MAX_ALLOC.with(|m| m.set(0));
    let out = f();
    drop(out);
    MAX_ALLOC.with(Cell::get)
}

/// The crate ceiling is 32 MiB; nothing a corrupt block does may approach it
/// when the block itself is a few hundred bytes.
const TOLERANCE: usize = 4 << 20;

use chronix_encoding::{PcoDecoder, PcoEncoder};

fn sample_block() -> Vec<u8> {
    let values: Vec<f64> = (0..1000)
        .map(|i| 230.0 + f64::from(i % 400) / 100.0)
        .collect();
    PcoEncoder::encode_f64(&values).unwrap()
}

#[test]
fn our_count_header_bounds_the_allocation() {
    let mut block = sample_block();
    // A count far above the ceiling is refused before anything is allocated.
    block[..4].copy_from_slice(&u32::MAX.to_le_bytes());
    let peak = largest_alloc_during(|| {
        assert!(PcoDecoder::decode_f64(&block).is_err());
    });
    assert!(
        peak < TOLERANCE,
        "a u32::MAX count allocated {peak} bytes before being rejected"
    );

    // A count just under the ceiling is *accepted* as a count, so the
    // decoder does allocate for it — but bounded by the ceiling, not by
    // u32::MAX. This is the allocation the ceiling exists to cap.
    block[..4].copy_from_slice(&1_300_000u32.to_le_bytes());
    let peak = largest_alloc_during(|| {
        assert!(PcoDecoder::decode_f64(&block).is_err());
    });
    assert!(
        peak <= 32 << 20,
        "a count at the ceiling allocated {peak} bytes, above the 32 MiB bound"
    );
}

/// The pco file's *own* metadata must not drive an allocation.
///
/// Every byte of a valid block is corrupted in turn, which reaches pco's
/// chunk headers — the counts and page sizes we deliberately do not trust.
#[test]
fn corrupting_pcos_own_metadata_cannot_allocate() {
    let block = sample_block();
    let mut worst = 0usize;
    for i in 4..block.len().min(256) {
        for patch in [0xFF_u8, 0x7F, 0x80, 0x00] {
            let mut corrupt = block.clone();
            corrupt[i] = patch;
            let peak = largest_alloc_during(|| {
                // Either outcome is fine; the allocation is what is on trial.
                let _ = PcoDecoder::decode_f64(&corrupt);
                let _ = PcoDecoder::decode_i64(&corrupt);
                let _ = PcoDecoder::decode_u64(&corrupt);
            });
            worst = worst.max(peak);
            assert!(
                peak < TOLERANCE,
                "byte {i} set to {patch:#04x} caused a {peak}-byte allocation"
            );
        }
    }
    println!("largest allocation across all single-byte corruptions: {worst} B");
}

/// Truncation is the other half: a short payload must not be read as a
/// promise of a long one.
#[test]
fn truncated_pco_blocks_cannot_allocate() {
    let block = sample_block();
    for cut in [4, 5, 8, 16, 32, 64, block.len() / 2, block.len() - 1] {
        let peak = largest_alloc_during(|| {
            let _ = PcoDecoder::decode_f64(&block[..cut]);
        });
        assert!(
            peak < TOLERANCE,
            "truncating to {cut} allocated {peak} bytes"
        );
    }
}
