#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # What SQL costs on a small machine
//!
//! Answers the question a `sql` feature gate would be decided on: how much
//! peak heap does DataFusion add over the engine's own read path, for the
//! *same* answer?
//!
//! Both halves compute the same aggregate over the same data — one through
//! `db.sql(…)`, one through `db.query()…aggregate(…)` — and the peak live
//! heap is measured around each, from a counting allocator rather than from
//! the OS's high-water mark. The session context is built before the SQL
//! measurement so what is reported is the cost of *running* a query, not of
//! constructing the engine once; the construction cost is reported separately,
//! because on a gateway that is paid once and kept.
//!
//! ```sh
//! cargo run --release -p chronix --example sql_footprint
//! ```

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use chronix::prelude::*;

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

// SAFETY: delegates every operation to the system allocator unchanged and
// only adds atomic bookkeeping around it.
#[allow(unsafe_code)]
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
        PEAK.fetch_max(live, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

const MIB: f64 = 1024.0 * 1024.0;

fn live_mib() -> f64 {
    LIVE.load(Ordering::Relaxed) as f64 / MIB
}

/// Peak live heap while `f` runs, over the baseline it started at.
fn peak_over_baseline<T>(f: impl FnOnce() -> T) -> (T, f64) {
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let out = f();
    let peak = PEAK.load(Ordering::Relaxed);
    (out, (peak.saturating_sub(baseline)) as f64 / MIB)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let db = Chronix::open_small(dir.path())?;

    // The design partner's shape: 50 series at 1 s for an hour.
    let base = 1_700_000_000_000_000_000i64;
    let mut points = Vec::with_capacity(50 * 3_600);
    for s in 0..50 {
        let key = SeriesKey::new(
            "power",
            chronix::tags! { "meter" => format!("m{s}").as_str() },
        )?;
        for t in 0..3_600i64 {
            points.push(Point::new(
                key.clone(),
                chronix::fields! { "w" => 200.0 + f64::from(s) + (t % 97) as f64 },
                base + t * 1_000_000_000,
            )?);
        }
    }
    for chunk in points.chunks(10_000) {
        db.insert_batch(chunk)?.into_complete()?;
    }
    db.flush()?;
    let after_ingest = live_mib();
    println!("180 000 points written, {after_ingest:.2} MiB live");

    // ── The engine's own read path ─────────────────────────────────────
    let plan = db
        .query()
        .measurement("power")
        .range(base, base + 3_600 * 1_000_000_000)
        .aggregate(chronix::chronix_query::aggregate::AggFn::Avg)
        .group_by(&["meter"])
        .build()?;
    let (native_rows, native_peak) = peak_over_baseline(|| db.execute(&plan).map(|b| b.num_rows()));
    let native_rows = native_rows?;

    // ── The same answer through SQL ────────────────────────────────────
    // The session context is built on first use and cached, so measuring it
    // alone separates the once-per-process cost from the per-query one. On a
    // gateway the first is paid at startup and kept.
    let (_ctx, ctx_peak) = peak_over_baseline(|| db.session_context());
    let sql = "SELECT meter, avg(w) FROM power GROUP BY meter";
    // Warm the planner: the first statement of a process builds catalogs and
    // function registries every later one reuses, and charging those to "a
    // query" would overstate the steady-state cost.
    let _ = db.sql(sql)?;
    let (sql_rows, sql_peak) = peak_over_baseline(|| {
        db.sql(sql).map(|bs| {
            bs.iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum::<usize>()
        })
    });
    let sql_rows = sql_rows?;

    assert_eq!(native_rows, sql_rows, "both paths answer the same question");

    println!(
        "\n{:<34}{:>10}\n{:<34}{:>9.2} MiB\n{:<34}{:>9.2} MiB\n{:<34}{:>9.2} MiB",
        "rows (both paths)",
        native_rows,
        "native aggregate, peak",
        native_peak,
        "SQL aggregate, peak",
        sql_peak,
        "SessionContext, once at startup",
        ctx_peak,
    );
    println!(
        "\nSQL costs {:.2} MiB more per query and {:.2} MiB once.",
        (sql_peak - native_peak).max(0.0),
        ctx_peak
    );
    db.close()?;
    Ok(())
}
