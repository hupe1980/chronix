#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! `execute_iter` must read one time-disjoint bucket at a time, and must agree
//! with `execute_stream` on the rows it produces.
//!
//! The memory claim in `db::stream` rests on two properties that are checked
//! here directly: segments spanning K non-overlapping time ranges produce K
//! buckets, and pulling one batch materialises exactly one of them.

use arrow::array::{Array, Float64Array, Int64Array};
use arrow::record_batch::RecordBatch;
use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

const HOUR_NS: i64 = 3_600_000_000_000;

fn open(dir: &tempfile::TempDir) -> Chronix {
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    Chronix::open(config).unwrap()
}

/// Write `hours` separate shards, flushing each so it becomes its own segment.
fn seed_hourly_shards(db: &Chronix, hours: i64, per_hour: i64) {
    let key = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();
    for hour in 0..hours {
        for i in 0..per_hour {
            let ts = hour * HOUR_NS + i * 1_000_000_000;
            let v = (hour * per_hour + i) as f64;
            db.insert(&Point::new(key.clone(), fields! { "v" => v }, ts).unwrap())
                .unwrap();
        }
        db.flush().unwrap();
    }
}

fn all_rows(batches: &[RecordBatch]) -> Vec<(i64, f64)> {
    let mut out = Vec::new();
    for b in batches {
        let ts = b
            .column(b.schema().index_of(chronix_core::TIME_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let v = b
            .column(b.schema().index_of("v").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push((
                ts.value(i),
                if v.is_valid(i) { v.value(i) } else { f64::NAN },
            ));
        }
    }
    out
}

#[test]
fn disjoint_shards_become_separate_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed_hourly_shards(&db, 4, 50);

    let plan = db
        .query()
        .measurement("m")
        .range(0, i64::MAX)
        .build()
        .unwrap();

    let mut stream = db.execute_iter(&plan).unwrap();
    assert_eq!(
        stream.buckets_remaining(),
        4,
        "four non-overlapping hourly segments must sweep into four buckets"
    );

    // Pulling one batch must materialise exactly one bucket — this is the
    // property the memory bound rests on.
    let first = stream.next().expect("a batch").unwrap();
    assert!(first.num_rows() > 0);
    assert_eq!(
        stream.buckets_remaining(),
        3,
        "one batch must not have read more than one bucket"
    );
}

#[test]
fn iter_and_stream_agree() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed_hourly_shards(&db, 4, 50);
    // Leave some rows unflushed so the memtable participates in the merge.
    let key = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();
    for i in 0..10 {
        let ts = 4 * HOUR_NS + i * 1_000_000_000;
        db.insert(&Point::new(key.clone(), fields! { "v" => 1000.0 + i as f64 }, ts).unwrap())
            .unwrap();
    }

    let plan = db
        .query()
        .measurement("m")
        .range(0, i64::MAX)
        .build()
        .unwrap();

    let streamed = all_rows(&db.execute_stream(&plan).unwrap());
    let iterated = all_rows(
        &db.execute_iter(&plan)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
    );

    assert_eq!(streamed.len(), 210);
    assert_eq!(iterated, streamed);

    // Output must still be globally ascending by timestamp across buckets.
    assert!(
        iterated.windows(2).all(|w| w[0].0 <= w[1].0),
        "bucketed output is not globally time-ordered"
    );
}

/// Overlapping segments must land in one bucket — correctness must not depend
/// on segments being time-partitioned.
#[test]
fn overlapping_segments_share_a_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();

    // Two flushes covering the same range, second overwriting the first.
    for i in 0..20 {
        db.insert(&Point::new(key.clone(), fields! { "v" => 1.0 }, i * 1_000_000_000).unwrap())
            .unwrap();
    }
    db.flush().unwrap();
    for i in 0..20 {
        db.insert(&Point::new(key.clone(), fields! { "v" => 2.0 }, i * 1_000_000_000).unwrap())
            .unwrap();
    }
    db.flush().unwrap();

    let plan = db
        .query()
        .measurement("m")
        .range(0, i64::MAX)
        .build()
        .unwrap();

    let stream = db.execute_iter(&plan).unwrap();
    assert_eq!(
        stream.buckets_remaining(),
        1,
        "overlapping segments must be merged together, not split"
    );

    let rows = all_rows(&stream.collect::<Result<Vec<_>, _>>().unwrap());
    assert_eq!(rows.len(), 20, "dedup must collapse the overwrite");
    assert!(
        rows.iter().all(|(_, v)| (*v - 2.0).abs() < f64::EPSILON),
        "the later segment must win: {rows:?}"
    );
}

/// A Parquet export of a multi-shard window must round-trip every row while
/// driving the query lazily.
#[test]
fn streaming_parquet_export_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed_hourly_shards(&db, 6, 100);

    let plan = db
        .query()
        .measurement("m")
        .range(0, i64::MAX)
        .build()
        .unwrap();

    let path = dir.path().join("export.parquet");
    let result = db
        .export_parquet(&plan, &path, &chronix::ParquetExportConfig::default())
        .unwrap();

    assert_eq!(result.rows_written, 600);
    assert!(!result.truncated);

    let file = std::fs::File::open(&path).unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let read: usize = reader.map(|b| b.unwrap().num_rows()).sum();
    assert_eq!(read, 600);
}

/// Aggregate plans cannot stream; `execute_iter` must say so rather than
/// quietly returning raw scan rows.
#[test]
fn execute_iter_rejects_non_scan_plans() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed_hourly_shards(&db, 1, 10);

    let plan = db
        .query()
        .measurement("m")
        .range(0, i64::MAX)
        .aggregate(chronix_query::aggregate::AggFn::Sum)
        .build()
        .unwrap();

    assert!(db.execute_iter(&plan).is_err());
}
