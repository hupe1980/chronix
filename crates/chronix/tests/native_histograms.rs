//! A native histogram survives the write path.
//!
//! This is the test that says the type is *reachable*, not merely correct:
//! a value written through the public API comes back from the memtable, from
//! a flushed segment, and after a restart — which are the three places a new
//! column type is usually forgotten.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use arrow::array::Array;
use chronix::chronix_core::histogram::{Bucket, CUSTOM_BUCKETS_SCHEMA, Histogram, ResetHint};
use chronix::prelude::*;
use std::collections::BTreeMap;

fn latency_histogram(scale: f64) -> Histogram {
    let mut h = Histogram::empty(3);
    h.zero_threshold = 1e-6;
    h.reset_hint = ResetHint::NoReset;
    for i in 1..=200 {
        h.observe(f64::from(i) * scale / 200.0);
    }
    h
}

fn point_at(ts: i64, h: Histogram) -> Point {
    let key = SeriesKey::new("http", tags! { "route" => "/api" }).unwrap();
    let mut fields: BTreeMap<String, FieldValue> = BTreeMap::new();
    fields.insert("latency".into(), FieldValue::Histogram(Box::new(h)));
    Point::new(key, fields, ts).unwrap()
}

fn read_back(db: &Chronix) -> Vec<Histogram> {
    let plan = db
        .query()
        .measurement("http")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    let batches = db.execute_stream(&plan).unwrap();
    let mut out = Vec::new();
    for b in &batches {
        let idx = b
            .schema()
            .index_of("latency")
            .expect("the column is present");
        let col = b.column(idx);
        let bin = col
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .expect("a histogram column reads back as Binary");
        for i in 0..bin.len() {
            if bin.is_null(i) {
                continue;
            }
            out.push(postcard::from_bytes::<Histogram>(bin.value(i)).expect("decode"));
        }
    }
    out
}

#[test]
fn a_histogram_survives_the_memtable_a_flush_and_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let original = latency_histogram(2.0);

    {
        let db = Chronix::open_small(dir.path()).unwrap();
        db.insert(&point_at(1_700_000_000_000_000_000, original.clone()))
            .unwrap();

        // 1. From the memtable, before anything is durable.
        let from_memtable = read_back(&db);
        assert_eq!(from_memtable.len(), 1, "the unflushed point is queryable");
        assert_eq!(from_memtable[0], original, "unchanged in the memtable");

        // 2. From a segment.
        db.flush().unwrap();
        let from_segment = read_back(&db);
        assert_eq!(from_segment.len(), 1, "the flushed point is queryable");
        assert_eq!(from_segment[0], original, "unchanged through a segment");
        db.close().unwrap();
    }

    // 3. After a restart — the WAL and catalog paths.
    let db = Chronix::open_small(dir.path()).unwrap();
    let after_restart = read_back(&db);
    assert_eq!(after_restart.len(), 1, "the point survived a reopen");
    assert_eq!(after_restart[0], original, "unchanged across a restart");
    assert!(
        after_restart[0].validate().is_ok(),
        "and it is still a valid histogram"
    );
    db.close().unwrap();
}

#[test]
fn the_schema_reports_a_histogram_column_as_one() {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open_small(dir.path()).unwrap();
    db.insert(&point_at(1_700_000_000_000_000_000, latency_histogram(1.0)))
        .unwrap();
    let schema = db.schema("http").expect("the measurement exists");
    let col = schema
        .columns()
        .iter()
        .find(|c| c.name == "latency")
        .expect("the field is in the schema");
    assert_eq!(
        col.column_type,
        chronix::chronix_core::ColumnType::Histogram,
        "a histogram field is typed as one, not inferred as a blob"
    );
    db.close().unwrap();
}

#[test]
fn a_custom_bucket_histogram_survives_a_flush() {
    // The NHCB shape — what a classic `le`-bucketed histogram converts into —
    // carries its boundaries with it, and those must survive too.
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open_small(dir.path()).unwrap();
    let mut h = Histogram::empty(CUSTOM_BUCKETS_SCHEMA);
    h.custom_values = vec![
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ];
    for v in [0.003, 0.02, 0.4, 3.0, 20.0] {
        h.observe(v);
    }
    db.insert(&point_at(1_700_000_000_000_000_000, h.clone()))
        .unwrap();
    db.flush().unwrap();
    let back = read_back(&db);
    assert_eq!(back.len(), 1);
    assert_eq!(
        back[0].custom_values, h.custom_values,
        "boundaries survived"
    );
    assert_eq!(back[0], h);
    db.close().unwrap();
}

#[test]
fn two_histograms_in_one_measurement_keep_their_own_buckets() {
    // The failure a shared-column-metadata design would produce: one
    // histogram's buckets leaking into another's. They are independent
    // values, not a column-level schema.
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open_small(dir.path()).unwrap();
    let a = latency_histogram(1.0);
    let b = latency_histogram(1000.0);
    db.insert(&point_at(1_700_000_000_000_000_000, a.clone()))
        .unwrap();
    db.insert(&point_at(1_700_000_001_000_000_000, b.clone()))
        .unwrap();
    db.flush().unwrap();
    let back = read_back(&db);
    assert_eq!(back.len(), 2);
    assert!(
        back.contains(&a) && back.contains(&b),
        "both survived intact"
    );
    assert_ne!(
        back[0].positive, back[1].positive,
        "two different distributions keep two different bucket sets"
    );
    db.close().unwrap();
}

#[test]
fn a_histogram_and_a_float_can_share_a_measurement() {
    // A composite column beside a scalar one — the case where a schema that
    // assumed every field is numeric would fall over.
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open_small(dir.path()).unwrap();
    let key = SeriesKey::new("http", tags! { "route" => "/api" }).unwrap();
    let mut fields: BTreeMap<String, FieldValue> = BTreeMap::new();
    fields.insert(
        "latency".into(),
        FieldValue::Histogram(Box::new(latency_histogram(1.0))),
    );
    fields.insert("inflight".into(), FieldValue::F64(7.0));
    db.insert(&Point::new(key, fields, 1_700_000_000_000_000_000).unwrap())
        .unwrap();
    db.flush().unwrap();

    let plan = db
        .query()
        .measurement("http")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    let batches = db.execute_stream(&plan).unwrap();
    let b = &batches[0];
    assert!(b.schema().index_of("latency").is_ok());
    assert!(b.schema().index_of("inflight").is_ok());
    assert_eq!(read_back(&db).len(), 1);
    db.close().unwrap();
}

#[test]
fn an_invalid_histogram_is_refused_at_the_write_call() {
    // Bad buckets produce a wrong quantile rather than an error, so they have
    // to be refused where they enter — not discovered at read time.
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open_small(dir.path()).unwrap();
    let mut bad = Histogram::empty(0);
    bad.count = 1.0;
    bad.positive = vec![
        Bucket {
            index: 5,
            count: 1.0,
        },
        Bucket {
            index: 2,
            count: 1.0,
        }, // out of order
    ];
    // Refused at `Point::new`, which is earlier than `insert` and is the
    // right place: a malformed histogram interpolates to a plausible wrong
    // quantile, so it must not become a `Point` at all.
    let key = SeriesKey::new("http", tags! { "route" => "/api" }).unwrap();
    let mut fields: BTreeMap<String, FieldValue> = BTreeMap::new();
    fields.insert("latency".into(), FieldValue::Histogram(Box::new(bad)));
    let err = Point::new(key, fields, 1_700_000_000_000_000_000)
        .expect_err("an unsorted bucket list is not a histogram");
    let msg = err.to_string();
    assert!(
        msg.contains("ascending") || msg.contains("histogram"),
        "the refusal should name what is wrong: {msg}"
    );
    db.close().unwrap();
}

// ── Rollups ──────────────────────────────────────────────────────────

/// A rollup over a histogram column must produce a **histogram**, not a
/// number derived from one — and not silently nothing.
///
/// Skipping a column type the rollup could not handle is a failure this
/// engine has had twice already (integer columns, then decimal ones): no
/// rollup, no error, and then a retention pass that dropped the raw rows
/// anyway. A histogram would have been the third, and it is the worst of the
/// three, because merging *is* defined for it.
#[test]
fn a_rollup_over_a_histogram_column_merges_rather_than_dropping_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open_small(dir.path()).unwrap();

    db.create_rollup(
        chronix::RollupBuilder::new()
            .name("http_1m")
            .source("http")
            .target("http_1m")
            .every("1m")
            .aggregation(chronix::RollupAggFn::Sum)
            .build()
            .unwrap(),
    )
    .unwrap();

    // Three samples in one minute, each a distribution.
    let base = 1_700_000_000_000_000_000;
    let mut expected = Histogram::empty(3);
    for k in 0..3i64 {
        let mut h = Histogram::empty(3);
        h.zero_threshold = 1e-6;
        for i in 1..=50 {
            h.observe(f64::from(i) / 25.0 + k as f64);
        }
        expected.merge(&h).unwrap();
        db.insert(&point_at(base + k * 1_000_000_000, h)).unwrap();
    }
    // A much later sample, so the bucket above falls outside the live
    // out-of-order window and becomes final. Four hours, because the default
    // shard is an hour and the tolerance is two shards — a rollup will not
    // aggregate a bucket a live write could still land in, which is the
    // engine being right and this test's first version being wrong.
    db.insert(&point_at(
        base + 4 * 3_600_000_000_000,
        latency_histogram(1.0),
    ))
    .unwrap();
    db.flush().unwrap();
    let n = db.materialise_rollups().unwrap();
    assert!(n > 0, "the rollup pass materialised nothing at all");

    let plan = db
        .query()
        .measurement("http_1m")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    let batches = db.execute_stream(&plan).unwrap();
    let mut rolled: Vec<Histogram> = Vec::new();
    for b in &batches {
        let Ok(idx) = b.schema().index_of("latency_sum") else {
            continue;
        };
        let bin = b
            .column(idx)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .expect("a rolled-up histogram is still a histogram column");
        for i in 0..bin.len() {
            if !bin.is_null(i) {
                rolled.push(postcard::from_bytes(bin.value(i)).expect("decode"));
            }
        }
    }

    assert!(
        !rolled.is_empty(),
        "the rollup produced no histogram column — a tier that silently drops \
         the column it exists to summarise is the failure this test is for"
    );
    let first = &rolled[0];
    assert_eq!(
        first.count, expected.count,
        "the merged histogram must hold every observation of the bucket"
    );
    assert!(
        (first.sum - expected.sum).abs() < 1e-6,
        "and their sum: got {}, expected {}",
        first.sum,
        expected.sum
    );
    assert_eq!(
        first.positive, expected.positive,
        "bucket for bucket, the rollup equals merging the samples by hand"
    );
    assert!(first.validate().is_ok(), "and it is a valid histogram");
    db.close().unwrap();
}

#[test]
fn a_rolled_up_histogram_still_answers_quantiles() {
    // The point of merging rather than averaging: the tier a dashboard reads
    // must answer the question a histogram is kept for.
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open_small(dir.path()).unwrap();
    let mut a = Histogram::empty(4);
    let mut b = Histogram::empty(4);
    for i in 1..=100 {
        a.observe(f64::from(i) / 100.0);
        b.observe(f64::from(i) / 100.0);
    }
    let mut merged = a.clone();
    merged.merge(&b).unwrap();

    assert!(
        (merged.quantile(0.5) - a.quantile(0.5)).abs() < 1e-9,
        "merging two identical distributions must not move the median: \
         {} vs {}",
        merged.quantile(0.5),
        a.quantile(0.5)
    );
    assert_eq!(merged.count, a.count + b.count);
    db.close().unwrap();
}

// ── PromQL ───────────────────────────────────────────────────────────

fn one_value(db: &Chronix, q: &str, at: i64) -> f64 {
    match db.promql(q, at).unwrap_or_else(|e| panic!("{q}: {e}")) {
        chronix::promql::ast::PromQLValue::Vector(v) => {
            assert_eq!(v.len(), 1, "{q} returned {} series", v.len());
            v[0].samples[0].value
        }
        other => panic!("{q} returned {other:?}"),
    }
}

/// The whole vertical: a stored histogram answers the `histogram_*` family.
#[test]
fn promql_reads_a_stored_native_histogram() {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open_small(dir.path()).unwrap();
    let at = 1_700_000_000_000_000_000;

    // A known distribution: 200 observations of 0.005 … 1.0 seconds.
    let mut h = Histogram::empty(5);
    h.zero_threshold = 1e-9;
    for i in 1..=200 {
        h.observe(f64::from(i) / 200.0);
    }
    let (count, sum, avg) = (h.count, h.sum, h.avg());
    let (q50, q99) = (h.quantile(0.5), h.quantile(0.99));
    let stddev = h.stddev();
    db.insert(&point_at(at, h)).unwrap();
    db.flush().unwrap();

    assert!(
        (one_value(&db, "histogram_count(http_latency)", at) - count).abs() < 1e-9,
        "histogram_count must be the observation count"
    );
    assert!(
        (one_value(&db, "histogram_sum(http_latency)", at) - sum).abs() < 1e-9,
        "histogram_sum must be the observation sum"
    );
    assert!(
        (one_value(&db, "histogram_avg(http_latency)", at) - avg).abs() < 1e-9,
        "histogram_avg must be sum/count"
    );
    assert!(
        (one_value(&db, "histogram_stddev(http_latency)", at) - stddev).abs() < 1e-9,
        "histogram_stddev must match the type's own answer"
    );
    assert!(
        (one_value(&db, "histogram_quantile(0.5, http_latency)", at) - q50).abs() < 1e-9,
        "histogram_quantile must read the native buckets, not look for `le` labels"
    );
    assert!((one_value(&db, "histogram_quantile(0.99, http_latency)", at) - q99).abs() < 1e-9);

    // The quantile must actually be near the truth, not merely self-consistent.
    let p50 = one_value(&db, "histogram_quantile(0.5, http_latency)", at);
    assert!(
        (p50 - 0.5).abs() < 0.02,
        "the median of a uniform [0.005, 1.0] distribution should be ~0.5, got {p50}"
    );

    // `histogram_fraction` over the whole range is everything.
    let all = one_value(&db, "histogram_fraction(-Inf, +Inf, http_latency)", at);
    assert!(
        (all - 1.0).abs() < 1e-6,
        "fraction over everything is 1, got {all}"
    );
    let half = one_value(&db, "histogram_fraction(-Inf, 0.5, http_latency)", at);
    assert!(
        (half - 0.5).abs() < 0.02,
        "half the uniform distribution is below 0.5, got {half}"
    );

    db.close().unwrap();
}

#[test]
fn the_histogram_family_refuses_a_float_series_by_name() {
    // `histogram_count` of a gauge is not the gauge. Upstream errors rather
    // than guessing, and a plausible number would be worse than a refusal.
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open_small(dir.path()).unwrap();
    let at = 1_700_000_000_000_000_000;
    let key = SeriesKey::new("cpu", tags! { "host" => "a" }).unwrap();
    let mut fields: BTreeMap<String, FieldValue> = BTreeMap::new();
    fields.insert("value".into(), FieldValue::F64(42.0));
    db.insert(&Point::new(key, fields, at).unwrap()).unwrap();
    db.flush().unwrap();

    for q in [
        "histogram_count(cpu)",
        "histogram_sum(cpu)",
        "histogram_avg(cpu)",
        "histogram_stddev(cpu)",
        "histogram_stdvar(cpu)",
        "histogram_fraction(0, 1, cpu)",
    ] {
        let err = db
            .promql(q, at)
            .expect_err(&format!("{q} must refuse a float series"));
        let msg = err.to_string();
        assert!(
            msg.contains("histogram"),
            "{q} should say what it wanted: {msg}"
        );
    }
    db.close().unwrap();
}

#[test]
fn a_classic_histogram_still_answers_histogram_quantile() {
    // The dispatch must not have broken the `le`-bucketed path: a classic
    // histogram has no native samples and takes the original route.
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open_small(dir.path()).unwrap();
    let at = 1_700_000_000_000_000_000;
    for (le, count) in [("0.1", 1.0), ("0.5", 3.0), ("1", 5.0), ("+Inf", 5.0)] {
        let key = SeriesKey::new("lat", tags! { "le" => le }).unwrap();
        let mut fields: BTreeMap<String, FieldValue> = BTreeMap::new();
        fields.insert("value".into(), FieldValue::F64(count));
        db.insert(&Point::new(key, fields, at).unwrap()).unwrap();
    }
    db.flush().unwrap();
    let v = one_value(&db, "histogram_quantile(0.5, lat)", at);
    assert!(
        v > 0.1 && v < 1.0,
        "the classic path must still work: got {v}"
    );
    db.close().unwrap();
}

// ── The classic `le` exposition ──────────────────────────────────────

/// Every series a query returned, as `(labels, value)` pairs.
fn vector(db: &Chronix, q: &str, at: i64) -> Vec<(BTreeMap<String, String>, f64)> {
    match db.promql(q, at).unwrap_or_else(|e| panic!("{q}: {e}")) {
        chronix::promql::ast::PromQLValue::Vector(v) => v
            .into_iter()
            .map(|s| {
                let labels: BTreeMap<String, String> = s.labels.into_iter().collect();
                (labels, s.samples[0].value)
            })
            .collect(),
        other => panic!("{q} returned {other:?}"),
    }
}

/// A database holding one histogram with hand-chosen buckets.
///
/// Schema −53, so the boundaries are *listed* rather than implied and the
/// expected `le` labels can be written down rather than computed:
/// `(-inf, 0.1] (0.1, 0.5] (0.5, 1] (1, +inf)` with counts 1, 2, 3, 4.
fn classic_fixture(dir: &tempfile::TempDir) -> (Chronix, i64) {
    let db = Chronix::open_small(dir.path()).unwrap();
    let at = 1_700_000_000_000_000_000;

    let mut h = Histogram::empty(CUSTOM_BUCKETS_SCHEMA);
    h.custom_values = vec![0.1, 0.5, 1.0];
    h.positive = vec![
        Bucket {
            index: 0,
            count: 1.0,
        },
        Bucket {
            index: 1,
            count: 2.0,
        },
        Bucket {
            index: 2,
            count: 3.0,
        },
        Bucket {
            index: 3,
            count: 4.0,
        },
    ];
    h.count = 10.0;
    h.sum = 6.5;
    h.validate().unwrap();

    let key = SeriesKey::new("lat", tags! { "job" => "api" }).unwrap();
    let mut fields: BTreeMap<String, FieldValue> = BTreeMap::new();
    fields.insert("value".into(), FieldValue::Histogram(Box::new(h)));
    db.insert(&Point::new(key, fields, at).unwrap()).unwrap();
    db.flush().unwrap();
    (db, at)
}

/// `foo_bucket` over a stored native histogram is the classic exposition.
///
/// The expectation is the *definition* of a classic histogram — one series per
/// boundary carrying the count of observations at or below it, ending at
/// `+Inf` — written from the fixture's four buckets rather than read back off
/// the implementation.
#[test]
fn a_native_histogram_answers_the_classic_bucket_name() {
    let dir = tempfile::tempdir().unwrap();
    let (db, at) = classic_fixture(&dir);

    let series = vector(&db, "lat_bucket", at);
    let mut got: Vec<(String, f64)> = series.iter().map(|(l, v)| (l["le"].clone(), *v)).collect();
    got.sort_by(|a, b| a.0.cmp(&b.0));

    let mut expected = vec![
        ("0.1".to_string(), 1.0),
        ("0.5".to_string(), 3.0), // 1 + 2
        ("1".to_string(), 6.0),   // 1 + 2 + 3
        ("+Inf".to_string(), 10.0),
    ];
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        got, expected,
        "counts are cumulative and the last boundary is `+Inf`, spelled the \
         way a dashboard written years ago spells it"
    );

    // The name a client sees is the name it typed.
    for (labels, _) in &series {
        assert_eq!(labels["__name__"], "lat_bucket");
        assert_eq!(labels["job"], "api", "the original tags are kept");
    }

    db.close().unwrap();
}

/// `foo_count` and `foo_sum` read the same column.
#[test]
fn a_native_histogram_answers_the_classic_count_and_sum_names() {
    let dir = tempfile::tempdir().unwrap();
    let (db, at) = classic_fixture(&dir);
    assert_eq!(one_value(&db, "lat_count", at), 10.0);
    assert_eq!(one_value(&db, "lat_sum", at), 6.5);
    // And they agree with the native family over the same data.
    assert_eq!(
        one_value(&db, "lat_count", at),
        one_value(&db, "histogram_count(lat)", at)
    );
    assert_eq!(
        one_value(&db, "lat_sum", at),
        one_value(&db, "histogram_sum(lat)", at)
    );
    db.close().unwrap();
}

/// An `le` matcher selects one boundary.
///
/// This is the case a filter applied too early gets wrong: `le` does not exist
/// on the stored row, so a post-filter run before the bucket view adds it sees
/// an absent label — which reads as the empty string, and matches nothing.
#[test]
fn an_le_matcher_selects_one_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let (db, at) = classic_fixture(&dir);
    assert_eq!(one_value(&db, r#"lat_bucket{le="0.5"}"#, at), 3.0);
    assert_eq!(one_value(&db, r#"lat_bucket{le="+Inf"}"#, at), 10.0);
    assert!(
        vector(&db, r#"lat_bucket{le="0.25"}"#, at).is_empty(),
        "a boundary this histogram does not have selects nothing"
    );
    db.close().unwrap();
}

/// The classic view and the native one give the same quantile.
///
/// Not exactly — the classic exposition throws away the sub-bucket detail by
/// construction, and an NHCB histogram interpolates linearly inside a bucket
/// either way — but they must land in the same bucket, or one of the two
/// readings of the same stored bytes is wrong.
#[test]
fn the_classic_view_and_the_native_one_agree_on_the_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let (db, at) = classic_fixture(&dir);
    let native = one_value(&db, "histogram_quantile(0.5, lat)", at);
    let classic = one_value(&db, "histogram_quantile(0.5, lat_bucket)", at);
    assert!(
        (native - classic).abs() < 1e-9,
        "the same bytes read two ways: native {native}, classic {classic}"
    );
    db.close().unwrap();
}

/// The classic names are resolvable but not listed.
///
/// Listing them would put three extra entries per histogram into
/// `/api/v1/label/__name__/values`, and — worse — make `{__name__=~".+"}`
/// return the same observations three times over.
#[test]
fn the_classic_names_are_not_offered_by_discovery() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _at) = classic_fixture(&dir);
    let names: Vec<String> = chronix::promql::all_metrics(db.schema_registry())
        .into_iter()
        .map(|m| m.name)
        .collect();
    assert!(names.contains(&"lat".to_string()));
    for suffix in ["_bucket", "_count", "_sum"] {
        let name = format!("lat{suffix}");
        assert!(
            !names.contains(&name),
            "{name} is a view, not a stored metric, and listing it would make \
             `{{__name__=~\".+\"}}` count the same observations twice: {names:?}"
        );
    }
    db.close().unwrap();
}

/// A real float field called `count` is not shadowed by the view.
#[test]
fn a_genuine_count_field_still_resolves_to_itself() {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open_small(dir.path()).unwrap();
    let at = 1_700_000_000_000_000_000;

    // `jobs` has an ordinary float field `count`, so `jobs_count` is that
    // field — not a classic view of anything.
    let key = SeriesKey::new("jobs", tags! { "queue" => "a" }).unwrap();
    let mut fields: BTreeMap<String, FieldValue> = BTreeMap::new();
    fields.insert("count".into(), FieldValue::F64(7.0));
    db.insert(&Point::new(key, fields, at).unwrap()).unwrap();
    db.flush().unwrap();

    assert_eq!(one_value(&db, "jobs_count", at), 7.0);
    db.close().unwrap();
}

/// A classic name over a float metric resolves to nothing, not to a guess.
#[test]
fn a_float_metric_has_no_classic_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open_small(dir.path()).unwrap();
    let at = 1_700_000_000_000_000_000;
    let key = SeriesKey::new("cpu", tags! { "host" => "a" }).unwrap();
    let mut fields: BTreeMap<String, FieldValue> = BTreeMap::new();
    fields.insert("value".into(), FieldValue::F64(42.0));
    db.insert(&Point::new(key, fields, at).unwrap()).unwrap();
    db.flush().unwrap();

    assert!(
        vector(&db, "cpu_bucket", at).is_empty(),
        "a gauge has no buckets, and inventing one is worse than an empty result"
    );
    db.close().unwrap();
}

/// The `le` values are the boundaries the histogram *has*, not the ones asked for.
///
/// This is the caveat the data-model page spends a table on, and it is the one
/// way the classic exposition differs in kind between the two schema families:
///
/// - a **custom-bucket** histogram carries the boundaries the instrumentation
///   chose, so a dashboard's `le="0.5"` literal matches;
/// - an **exponential** one carries powers of `2^(2^-schema)`, so there is no
///   bucket ending at exactly 0.5 and the matcher selects nothing.
///
/// Returning the nearest bucket instead would be the dangerous answer: a p99
/// read off a boundary nobody asked for is wrong in a way no error reports.
#[test]
fn the_le_values_are_the_schemas_boundaries_not_the_ones_asked_for() {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open_small(dir.path()).unwrap();
    let at = 1_700_000_000_000_000_000;

    // Schema 0: base 2, so index i covers (2^(i-1), 2^i] and `le` is 2^i.
    let mut h = Histogram::empty(0);
    h.positive = vec![
        Bucket {
            index: 1,
            count: 2.0,
        },
        Bucket {
            index: 2,
            count: 3.0,
        },
    ];
    h.count = 5.0;
    h.sum = 12.0;
    h.validate().unwrap();

    let key = SeriesKey::new("exp", tags! {}).unwrap();
    let mut fields: BTreeMap<String, FieldValue> = BTreeMap::new();
    fields.insert("value".into(), FieldValue::Histogram(Box::new(h)));
    db.insert(&Point::new(key, fields, at).unwrap()).unwrap();
    db.flush().unwrap();

    let mut les: Vec<String> = vector(&db, "exp_bucket", at)
        .into_iter()
        .map(|(l, _)| l["le"].clone())
        .collect();
    les.sort();
    let mut expected = vec![
        "2".to_string(), // 2^1
        "4".to_string(), // 2^2
        "+Inf".to_string(),
    ];
    expected.sort();
    assert_eq!(
        les, expected,
        "the boundaries are the schema's, computed from `(2^(2^-0))^i`"
    );

    assert!(
        vector(&db, r#"exp_bucket{le="3"}"#, at).is_empty(),
        "an exponential histogram has no bucket ending at 3, and answering \
         with the nearest one would be a quantile nobody asked for"
    );

    // The whole series is still readable, which is what a heatmap panel and
    // `histogram_quantile` over the classic name both need.
    let q = one_value(&db, "histogram_quantile(0.5, exp_bucket)", at);
    assert!(
        (2.0..=4.0).contains(&q),
        "the median of counts 2 then 3 over (…,2] (2,4] is in the second \
         bucket, got {q}"
    );
    db.close().unwrap();
}
