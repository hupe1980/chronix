#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! End-to-end compression on the shape of data the design partner stores.
//!
//! The codec-level ratios in `chronix-encoding` prove ALP works on a column;
//! this proves the win survives the segment writer — headers, row groups,
//! zone maps, blooms and all — which is what actually lands on a gateway's
//! flash.

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

/// A day of 1 Hz meter readings for a handful of series, two decimal places —
/// the hems workload in miniature.
#[test]
fn decimal_metrics_reach_the_documented_compression_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .wal_fsync_policy(chronix_core::FsyncPolicy::Periodic(
            std::time::Duration::from_secs(1),
        ))
        .build()
        .unwrap();
    let db = Chronix::open(config).unwrap();

    const SERIES: usize = 4;
    const POINTS: usize = 4_000;

    let keys: Vec<SeriesKey> = (0..SERIES)
        .map(|s| SeriesKey::new("power", tags! { "meter" => &format!("m{s}") }).unwrap())
        .collect();

    // Timestamp-major: writes advance monotonically through the shard window,
    // the way an ingesting gateway produces them.
    let mut batch = Vec::with_capacity(SERIES * POINTS);
    for i in 0..POINTS {
        let ts = i as i64 * 1_000_000_000;
        for (s, key) in keys.iter().enumerate() {
            // Watts to two decimals, the way a meter reports them.
            let watts = 200.0 + f64::from(u32::try_from((i * 7 + s) % 5000).unwrap()) / 100.0;
            batch.push(Point::new(key.clone(), fields! { "watts" => watts }, ts).unwrap());
        }
    }
    db.insert_batch(&batch).unwrap().into_complete().unwrap();
    db.flush().unwrap();

    let mut segment_bytes = 0u64;
    for entry in std::fs::read_dir(dir.path()).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            for f in walk(&path) {
                if f.extension().is_some_and(|e| e == "csx") {
                    segment_bytes += std::fs::metadata(&f).map(|m| m.len()).unwrap_or(0);
                }
            }
        }
    }

    let rows = (SERIES * POINTS) as u64;
    assert!(segment_bytes > 0, "no .csx segments were written");

    // 8 bytes for the value + 8 for the timestamp is the floor for an
    // uncompressed columnar layout, and understates the raw form because it
    // ignores the tag column the segment also stores.
    let raw = rows * 16;
    let ratio = raw as f64 / segment_bytes as f64;
    println!("{rows} rows, {segment_bytes} bytes on disk, {ratio:.1}x vs {raw} raw");

    // Two things have to hold for this number: ALP has to win the codec
    // competition on the values, and segment metadata has to stay
    // proportional to the data. Before either, this measured 1.8x.
    assert!(
        ratio >= 5.0,
        "on-disk compression regressed to {ratio:.1}x ({segment_bytes} bytes for {rows} rows)"
    );

    // The data must still read back exactly.
    let plan = db
        .query()
        .measurement("power")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    let total: usize = db
        .execute_iter(&plan)
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum();
    assert_eq!(total as u64, rows);
}

fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(walk(&p));
        } else {
            out.push(p);
        }
    }
    out
}
