#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Gateway footprint
//!
//! Opens the small preset, writes the design partner's workload — 50 series
//! at 1 s for an hour, a 1 s → 1 min → 15 min rollup cascade — runs the SQL
//! and PromQL a dashboard would run, and prints the process's resident set
//! at each step. "≈48 MB" is the sum of the configured budgets; this is the
//! number.
//!
//! ```sh
//! cargo run --release -p chronix --example gateway_footprint
//! ```

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use chronix::prelude::*;

/// Counts live heap bytes, so the number is the program's, not the
/// allocator's caching policy or the OS's high-water mark.
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

fn heap_mib() -> f64 {
    LIVE.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0)
}

/// Resident set size in MiB, from the OS. Linux reads `/proc/self/statm`;
/// everywhere else asks `ps`.
fn rss_mib() -> f64 {
    if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
        let pages: f64 = statm
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        return pages * 4096.0 / (1024.0 * 1024.0);
    }
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok();
    out.and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse::<f64>().ok())
        .map_or(f64::NAN, |kib| kib / 1024.0)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    println!(
        "{:<40} {:>7} {:>8} {:>9} {:>8} {:>6} {:>8}",
        "step", "RSS", "heap", "memtable", "interner", "WAL", "catalog"
    );
    let mib = |b: usize| b as f64 / (1024.0 * 1024.0);
    let row = |step: &str, db: Option<&Chronix>| {
        let s = db.map(Chronix::statistics);
        let get =
            |f: fn(&chronix::DatabaseStatistics) -> usize| s.as_ref().map_or(0.0, |s| mib(f(s)));
        println!(
            "{:<40} {:>7.1} {:>8.1} {:>9.1} {:>8.2} {:>6.2} {:>8.2}",
            step,
            rss_mib(),
            heap_mib(),
            get(|s| s.memtable_memory_bytes),
            get(|s| s.interner_memory_bytes),
            get(|s| s.wal_buffer_bytes),
            get(|s| s.catalog_memory_bytes),
        );
    };
    row("process start", None);

    let db = Chronix::open_small(dir.path())?;
    row("open_small", Some(&db));

    for (name, source, target, interval) in [
        ("raw_1m", "power", "power_1m", 60_000_000_000i64),
        ("1m_15m", "power_1m", "power_15m", 900_000_000_000),
    ] {
        db.create_rollup(
            RollupBuilder::new()
                .name(name)
                .source(source)
                .target(target)
                .interval_ns(interval)
                .aggregation(RollupAggFn::Avg)
                .aggregation(RollupAggFn::First)
                .aggregation(RollupAggFn::Last)
                .group_by("meter")
                .build()?,
        )?;
    }

    // One hour of 50 series at 1 s, two-decimal watts, written a second at
    // a time as a gateway would.
    let base = 1_700_000_000_000_000_000i64;
    let keys: Vec<SeriesKey> = (0..50)
        .map(|m| SeriesKey::new("power", tags! { "meter" => format!("m{m:02}") }).unwrap())
        .collect();
    for s in 0..3600i64 {
        let ts = base + s * 1_000_000_000;
        let batch: Vec<Point> = keys
            .iter()
            .enumerate()
            .map(|(m, k)| {
                let w = 200.0 + (m as f64) * 3.0 + ((s as f64) * 0.05).sin() * 25.0;
                Point::new(
                    k.clone(),
                    fields! { "w" => (w * 100.0).round() / 100.0 },
                    ts,
                )
                .unwrap()
            })
            .collect();
        db.insert_batch(&batch)?.into_complete()?;
    }
    row("180 000 points written", Some(&db));

    db.flush()?;
    row("flushed", Some(&db));

    // Close the window so the hour is final, then materialise the cascade.
    let later = keys[0].clone();
    db.insert(&Point::new(
        later,
        fields! { "w" => 0.0 },
        base + 4 * 3_600_000_000_000,
    )?)?;
    let written = db.materialise_rollups()?;
    row(&format!("{written} rollup points materialised"), Some(&db));

    let rows = db.sql(
        "SELECT meter, avg(w) AS avg_w, max(w) AS peak FROM power GROUP BY meter ORDER BY meter",
    )?;
    row(
        &format!(
            "SQL group-by ({} rows)",
            rows.iter().map(RecordBatch::num_rows).sum::<usize>()
        ),
        Some(&db),
    );

    let view = db.rollup("1m_15m", base, base + 3_600_000_000_000)?;
    row(
        &format!("15-minute view ({} rows)", view.num_rows()),
        Some(&db),
    );

    let _ = db.promql(r#"avg(rate(w{meter="m00"}[5m]))"#, base + 3_599_000_000_000)?;
    row("PromQL rate over 5m", Some(&db));

    let stats = db.statistics();
    println!(
        "\n{} series, {} segments; peak heap {:.1} MiB",
        stats.series_count,
        stats.segment_count,
        PEAK.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0)
    );
    println!(
        "engine resident {:.1} MiB = memtable {:.1} + interner {:.2} + WAL {:.2} + catalog {:.2}",
        mib(stats.resident_memory_bytes()),
        mib(stats.memtable_memory_bytes),
        mib(stats.interner_memory_bytes),
        mib(stats.wal_buffer_bytes),
        mib(stats.catalog_memory_bytes),
    );
    println!(
        "unaccounted (live heap − engine resident): {:.1} MiB — DataFusion's own \
         allocation once a SQL query has run, plus allocator slack",
        heap_mib() - mib(stats.resident_memory_bytes())
    );
    db.close()?;
    Ok(())
}
