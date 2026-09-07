//! What the engine's resident heap is made of, and what it costs to open.
//!
//! Two properties, both invisible to a test that only checks answers:
//! `DatabaseStatistics` reports every term of the resident heap and they sum;
//! and opening a database allocates kilobytes, because the CDC broadcast ring
//! — 7 MiB at the default capacity — is built on the first subscription
//! rather than at `open`.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use chronix::prelude::*;
use chronix::{fields, tags};

/// Counts live heap bytes, so the number is the program's rather than the
/// allocator's caching policy.
struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);

// SAFETY: delegates every operation to the system allocator unchanged and
// only adds atomic bookkeeping around it.
#[allow(unsafe_code)]
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn live() -> usize {
    LIVE.load(Ordering::Relaxed)
}

/// The allocator counter is process-global and the harness is threaded, so a
/// test that measures a delta has to be the only one measuring. Without this
/// the tests read each other's allocations: a 1 024-event ring measured 7 MiB
/// because another test built a 65 536-event one at the same moment.
static MEASURING: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn measuring() -> std::sync::MutexGuard<'static, ()> {
    MEASURING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Opening a database allocates kilobytes, not megabytes.
///
/// An eagerly built CDC ring makes this 7 MiB — a quarter of a gateway's
/// whole budget, spent before the first point is written, for a feature the
/// deployment may never use.
#[test]
fn opening_a_database_does_not_allocate_megabytes() {
    let _guard = measuring();
    // The **default** config, whose CDC capacity is a server's 65 536 — an
    // eagerly allocated ring of that size is 7 MiB, so this is the
    // configuration that distinguishes lazy from eager. (`small()` caps the
    // capacity, which would mask the regression.)
    let dir = tempfile::tempdir().unwrap();
    let cfg = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    assert_eq!(cfg.cdc_capacity, 65_536, "the default is a server's ring");

    let before = live();
    let db = Chronix::open(cfg).unwrap();
    let at_open = live().saturating_sub(before);

    // Measured: 1.1 MiB lazy (mostly the segment cache's frequency sketch,
    // which scales with the 512 MB default cache) against 8.1 MiB when the
    // ring is allocated eagerly. The bound sits between the two.
    assert!(
        at_open < 2 * 1024 * 1024,
        "opening allocated {:.1} MiB; the CDC ring is the usual cause — it must \
         stay lazy",
        at_open as f64 / (1024.0 * 1024.0)
    );
    db.close().unwrap();
}

/// The ring is allocated when something subscribes, and not before.
#[test]
fn the_cdc_ring_is_allocated_on_first_subscription() {
    let _guard = measuring();
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open(
        ChronixConfig::builder()
            .data_dir(dir.path())
            .cdc_capacity(65_536)
            .build()
            .unwrap(),
    )
    .unwrap();

    let before = live();
    let sub = db.event_bus().subscribe();
    let ring = live().saturating_sub(before);

    assert!(
        ring > 1024 * 1024,
        "a 65 536-event ring is megabytes; got {ring} bytes — is the capacity \
         being honoured?"
    );
    drop(sub);
    db.close().unwrap();
}

/// A smaller capacity is a smaller ring — the knob does something.
#[test]
fn the_cdc_capacity_bounds_the_ring() {
    let _guard = measuring();
    let mut sizes = Vec::new();
    for capacity in [1_024, 16_384] {
        let dir = tempfile::tempdir().unwrap();
        let db = Chronix::open(
            ChronixConfig::builder()
                .data_dir(dir.path())
                .cdc_capacity(capacity)
                .build()
                .unwrap(),
        )
        .unwrap();
        let before = live();
        let sub = db.event_bus().subscribe();
        sizes.push(live().saturating_sub(before));
        drop(sub);
        db.close().unwrap();
    }
    assert!(
        sizes[1] > sizes[0] * 4,
        "a 16x capacity should be a far larger ring: {sizes:?}"
    );
}

/// The gateway preset picks a capacity sized for a gateway.
#[test]
fn the_small_preset_sizes_the_bus_for_a_gateway() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = ChronixConfig::small(dir.path());
    assert!(
        cfg.cdc_capacity <= 8_192,
        "the small preset must not carry a server's ring: {}",
        cfg.cdc_capacity
    );
}

/// Every term of the resident heap is reported, and they sum.
#[test]
fn the_resident_heap_is_reported_term_by_term() {
    let _guard = measuring();
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open(ChronixConfig::small(dir.path())).unwrap();

    let key = SeriesKey::new("cpu", tags! { "host" => "h1" }).unwrap();
    let points: Vec<Point> = (0..5_000)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "usage" => f64::from(i) },
                i64::from(i) * 1_000_000_000,
            )
            .unwrap()
        })
        .collect();
    assert!(db.insert_batch(&points).unwrap().is_complete());

    let s = db.statistics();
    assert_eq!(
        s.resident_memory_bytes(),
        s.memtable_memory_bytes
            + s.interner_memory_bytes
            + s.wal_buffer_bytes
            + s.catalog_memory_bytes
            + s.metadata_cache_bytes,
        "the sum must be the sum"
    );

    // Each term is a real measurement, not a placeholder.
    assert!(s.memtable_memory_bytes > 0, "rows are in the memtable");
    assert!(
        s.interner_memory_bytes > 0,
        "the interner holds `cpu`, `host` and `h1`"
    );
    assert!(s.wal_buffer_bytes > 0, "the WAL writer has a buffer");

    db.flush().unwrap();
    let s = db.statistics();
    assert!(
        s.catalog_memory_bytes > 0,
        "a flushed segment has a catalog entry"
    );
    db.close().unwrap();
}
