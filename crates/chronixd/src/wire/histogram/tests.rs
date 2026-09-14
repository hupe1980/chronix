//! Tests for the wire ↔ storage histogram conversions.
//!
//! Every expectation here comes from the protocol specifications or from a
//! path that was tested against them separately — never from running the
//! decoder and writing down what it said. A span decoder that is wrong by one
//! is perfectly self-consistent, so self-consistency proves nothing.

use super::*;
use chronix_core::histogram::Histogram;

/// Build a `WireHistogram` from the pieces a test cares about.
///
/// Written as a helper rather than a `Default` impl because the struct
/// borrows, and because a test that says which fields it sets is a test that
/// says what it is about.
fn wire<'a>(
    schema: i32,
    count: u64,
    sum: f64,
    positive_spans: Vec<(i32, u32)>,
    positive_deltas: &'a [i64],
) -> WireHistogram<'a> {
    WireHistogram {
        count_int: Some(count),
        count_float: None,
        sum,
        schema,
        zero_threshold: 0.0,
        zero_count_int: Some(0),
        zero_count_float: None,
        negative_spans: Vec::new(),
        negative_deltas: &[],
        negative_counts: &[],
        positive_spans,
        positive_deltas,
        positive_counts: &[],
        reset_hint: 0,
        timestamp: 0,
        custom_values: &[],
    }
}

/// The worked example from `prompb/types.proto`'s own commentary.
///
/// Two spans — `{offset: 0, length: 2}` and `{offset: 1, length: 2}` — with
/// deltas `[1, 1, -1, 0]`. A span's offset is a gap measured from *one past*
/// the previous span's last bucket, so the second span starts at index 3, not
/// index 2 and not index 4. Those two off-by-ones are the whole reason this
/// decoder exists in one place.
#[test]
fn spans_and_deltas_decode_to_the_documented_indices() {
    let w = wire(0, 5, 0.0, vec![(0, 2), (1, 2)], &[1, 1, -1, 0]);
    let h = w.to_histogram().expect("valid");
    assert_eq!(
        h.positive,
        vec![
            Bucket {
                index: 0,
                count: 1.0
            },
            Bucket {
                index: 1,
                count: 2.0
            },
            Bucket {
                index: 3,
                count: 1.0
            },
            Bucket {
                index: 4,
                count: 1.0
            },
        ],
        "deltas are cumulative and the second span's offset is a gap, not an index"
    );
}

/// A zero-length span contributes its offset and no bucket.
///
/// Legal on the wire: a producer may split a long gap across spans rather than
/// emit one enormous offset. A decoder that treats length 0 as "emit one
/// bucket" reads a count that belongs to the next bucket.
#[test]
fn a_zero_length_span_moves_the_index_without_emitting_a_bucket() {
    let w = wire(0, 2, 0.0, vec![(2, 1), (3, 0), (4, 1)], &[1, 0]);
    let h = w.to_histogram().expect("valid");
    // First span: index 2. Second span: length 0, so nothing, but the index
    // advances past it. Third span: 2 + 1 + 3 + 4 = 10.
    assert_eq!(h.positive.len(), 2);
    assert_eq!(h.positive[0].index, 2);
    assert_eq!(h.positive[1].index, 10);
}

/// Counts are cumulative, so a delta may be negative — the running total may not.
#[test]
fn deltas_that_run_negative_are_refused() {
    let w = wire(0, 1, 0.0, vec![(0, 2)], &[1, -5]);
    assert!(matches!(
        w.to_histogram(),
        Err(WireHistogramError::NegativeRunningCount { .. })
    ));
}

/// Spans that promise more buckets than the array holds are refused, not truncated.
///
/// Truncating would store a histogram missing its tail while reporting the
/// full `count`, which then fails validation for an unrelated-looking reason —
/// or worse, passes, because `count` is only required to be an upper bound.
#[test]
fn spans_longer_than_the_count_array_are_refused() {
    let w = wire(0, 3, 0.0, vec![(0, 4)], &[1, 1]);
    assert!(matches!(
        w.to_histogram(),
        Err(WireHistogramError::SpanOverrun {
            declared: 4,
            supplied: 2
        })
    ));
}

/// A float histogram sends absolute counts; the `count` oneof is what says so.
#[test]
fn the_float_variant_reads_absolute_counts_not_deltas() {
    let counts = [2.5, 1.5];
    let w = WireHistogram {
        count_int: None,
        count_float: Some(4.0),
        sum: 10.0,
        schema: 0,
        zero_threshold: 0.0,
        zero_count_int: None,
        zero_count_float: Some(0.0),
        negative_spans: Vec::new(),
        negative_deltas: &[],
        negative_counts: &[],
        positive_spans: vec![(0, 2)],
        // Present and wrong on purpose: a float histogram must ignore them.
        positive_deltas: &[99, 99],
        positive_counts: &counts,
        reset_hint: 0,
        timestamp: 0,
        custom_values: &[],
    };
    let h = w.to_histogram().expect("valid");
    assert_eq!(h.positive[0].count, 2.5);
    assert_eq!(
        h.positive[1].count, 1.5,
        "absolute, not cumulative: 1.5 rather than 4.0"
    );
}

/// A schema outside `i8` is refused before anything tries to use it.
#[test]
fn an_impossible_schema_is_refused_by_name() {
    let w = wire(9999, 0, 0.0, vec![], &[]);
    let e = w.to_histogram().expect_err("9999 is not a resolution");
    assert!(
        e.to_string().contains("9999"),
        "the message must name the value it refused: {e}"
    );
}

/// A histogram whose buckets outweigh its `count` cannot be interpolated.
#[test]
fn buckets_exceeding_the_declared_count_are_refused() {
    let w = wire(0, 1, 0.0, vec![(0, 2)], &[5, 5]);
    assert!(matches!(
        w.to_histogram(),
        Err(WireHistogramError::Invalid(_))
    ));
}

/// Prometheus's staleness marker is a specific NaN payload in `sum`.
#[test]
fn the_staleness_marker_is_recognised_and_an_ordinary_nan_is_not() {
    let stale = wire(0, 0, f64::from_bits(0x7ff0_0000_0000_0002), vec![], &[]);
    assert!(stale.is_stale_marker());

    // A histogram that observed a NaN has a NaN sum and is real data.
    let ordinary = wire(0, 1, f64::NAN, vec![], &[]);
    assert!(
        !ordinary.is_stale_marker(),
        "an ordinary NaN sum means a NaN was observed, which is a sample"
    );
}

/// Encode → decode returns the same buckets.
///
/// The round trip is asserted over a shape with gaps of every interesting
/// width — one, two, three and far — because the encoder chooses between
/// filling a gap with zero counts and opening a new span, and either choice
/// must decode back to the same indices.
#[test]
fn encode_then_decode_is_the_identity_over_gaps_of_every_width() {
    let mut original = Histogram::empty(2);
    original.positive = vec![
        Bucket {
            index: -3,
            count: 1.0,
        },
        Bucket {
            index: -1,
            count: 2.0,
        }, // gap of 1
        Bucket {
            index: 2,
            count: 3.0,
        }, // gap of 2
        Bucket {
            index: 6,
            count: 4.0,
        }, // gap of 3
        Bucket {
            index: 40,
            count: 5.0,
        }, // far
    ];
    original.negative = vec![Bucket {
        index: 1,
        count: 6.0,
    }];
    original.zero_count = 7.0;
    original.count = 28.0;
    original.sum = 123.5;
    original.validate().expect("the fixture itself is valid");

    let e = encode(&original, 1_700_000_000_000);
    let w = WireHistogram {
        count_int: None,
        count_float: Some(e.count),
        sum: e.sum,
        schema: e.schema,
        zero_threshold: e.zero_threshold,
        zero_count_int: None,
        zero_count_float: Some(e.zero_count),
        negative_spans: e.negative_spans.clone(),
        negative_deltas: &[],
        negative_counts: &e.negative_counts,
        positive_spans: e.positive_spans.clone(),
        positive_deltas: &[],
        positive_counts: &e.positive_counts,
        reset_hint: e.reset_hint,
        timestamp: e.timestamp,
        custom_values: &e.custom_values,
    };
    let back = w.to_histogram().expect("valid");
    assert_eq!(back.positive, original.positive);
    assert_eq!(back.negative, original.negative);
    assert_eq!(back.zero_count, original.zero_count);
    assert_eq!(back.count, original.count);
    assert_eq!(back.sum, original.sum);
}

/// The reset hint survives the round trip, including the gauge case.
#[test]
fn the_reset_hint_survives_the_round_trip() {
    for hint in [
        ResetHint::Unknown,
        ResetHint::Reset,
        ResetHint::NoReset,
        ResetHint::Gauge,
    ] {
        let mut h = Histogram::empty(0);
        h.reset_hint = hint;
        let e = encode(&h, 0);
        assert_eq!(reset_hint_from_proto(e.reset_hint), hint);
    }
}

/// OTLP's index is one lower than Prometheus's for the same bucket.
///
/// The expectation is built the long way round: three values are *observed*
/// into a chronix histogram (whose bucket assignment is tested against the
/// `(2^(2^-n))^i` formula in `chronix-core`), and separately the OTLP indices
/// for those same values are computed from OTLP's own definition —
/// `bucket i covers (base^i, base^(i+1)]`, so `index = floor(log_base v)`.
/// Converting the OTLP form must reproduce the observed form exactly.
#[test]
fn otlp_bucket_indices_are_shifted_by_one_to_prometheus_convention() {
    const SCALE: i32 = 2;
    let values = [1.5f64, 3.0, 7.0];

    let mut observed = Histogram::empty(SCALE as i8);
    for v in values {
        observed.observe(v);
    }
    observed.canonicalise();

    // OTLP indices, straight from the OTLP formula.
    let base = 2f64.powf(2f64.powi(-SCALE));
    let otlp_indices: Vec<i32> = values
        .iter()
        .map(|v| (v.ln() / base.ln()).floor() as i32)
        .collect();
    assert_eq!(
        otlp_indices,
        vec![2, 6, 11],
        "sanity: these are the OTLP indices for 1.5, 3.0 and 7.0 at scale 2"
    );

    // Lay them out as OTLP does: one contiguous run from the lowest index.
    let lo = *otlp_indices.iter().min().expect("non-empty");
    let hi = *otlp_indices.iter().max().expect("non-empty");
    let mut counts = vec![0u64; (hi - lo + 1) as usize];
    for i in &otlp_indices {
        counts[(i - lo) as usize] += 1;
    }

    let converted = from_otlp_exponential(
        SCALE,
        0.0,
        0,
        values.len() as u64,
        Some(values.iter().sum()),
        Some(OtlpBuckets {
            offset: lo,
            counts: &counts,
        }),
        None,
    )
    .expect("valid");

    assert_eq!(
        converted.positive, observed.positive,
        "the OTLP conversion must land in the same buckets as observing the \
         values directly; a missing +1 puts every one of them a factor of \
         2^(2^-2) too low"
    );
    assert_eq!(converted.count, observed.count);
    assert!((converted.sum - observed.sum).abs() < 1e-12);
}

/// The negative side takes the same shift, mapped by absolute value.
#[test]
fn otlp_negative_buckets_convert_by_absolute_value() {
    let mut observed = Histogram::empty(0);
    for v in [-1.5f64, -5.0] {
        observed.observe(v);
    }
    observed.canonicalise();

    // |−1.5| → OTLP index floor(log2 1.5) = 0; |−5| → floor(log2 5) = 2.
    let converted = from_otlp_exponential(
        0,
        0.0,
        0,
        2,
        Some(-6.5),
        None,
        Some(OtlpBuckets {
            offset: 0,
            counts: &[1, 0, 1],
        }),
    )
    .expect("valid");

    assert_eq!(converted.negative, observed.negative);
}

/// An unset OTLP sum becomes NaN rather than zero.
///
/// OTLP leaves `sum` unset for a histogram that recorded negative events. A
/// decoder without the presence bit reads zero, and a zero sum over a hundred
/// observations makes `histogram_avg` report exactly 0.0 — a number, wrong,
/// and indistinguishable from a real one.
#[test]
fn an_absent_otlp_sum_is_not_a_sum_of_zero() {
    let h = from_otlp_exponential(
        0,
        0.0,
        0,
        4,
        None,
        Some(OtlpBuckets {
            offset: 0,
            counts: &[4],
        }),
        None,
    )
    .expect("valid");
    assert!(h.sum.is_nan(), "an unset sum is unknown, not zero");
    assert!(h.avg().is_nan());
}

/// The OTLP zero region maps onto chronix's zero bucket.
#[test]
fn the_otlp_zero_region_becomes_the_zero_bucket() {
    let h = from_otlp_exponential(0, 0.001, 7, 7, Some(0.0), None, None).expect("valid");
    assert_eq!(h.zero_count, 7.0);
    assert_eq!(h.zero_threshold, 0.001);
    assert_eq!(h.count, 7.0);
}

/// Explicit OTLP bounds become an NHCB histogram that answers quantiles.
///
/// `bucket_counts` has one more entry than `explicit_bounds`: the last is the
/// `+Inf` overflow. The expectations are the classic-histogram definition —
/// `histogram_quantile` over buckets `(-Inf,1] (1,2] (2,5] (5,+Inf)` with
/// counts 1, 2, 3, 4 — not whatever the code returns.
#[test]
fn explicit_otlp_bounds_become_custom_buckets() {
    let h = from_otlp_explicit(&[1.0, 2.0, 5.0], &[1, 2, 3, 4], 10, Some(30.0)).expect("valid");

    assert_eq!(h.schema, CUSTOM_BUCKETS_SCHEMA);
    assert_eq!(h.custom_values, vec![1.0, 2.0, 5.0]);
    assert_eq!(h.count, 10.0);
    assert_eq!(h.positive.len(), 4);

    // Bucket 0 is (-Inf, 1]; bucket 3 is (5, +Inf).
    assert_eq!(h.bucket_bounds(0, false), (f64::NEG_INFINITY, 1.0));
    assert_eq!(h.bucket_bounds(2, false), (2.0, 5.0));
    assert_eq!(h.bucket_bounds(3, false).1, f64::INFINITY);

    // Every observation is in some bucket.
    assert!((h.fraction(f64::NEG_INFINITY, f64::INFINITY) - 1.0).abs() < 1e-12);

    // The 30th percentile falls in (1, 2]: 1 observation below, 3 through the
    // end of that bucket.
    let q = h.quantile(0.3);
    assert!(
        (1.0..=2.0).contains(&q),
        "q(0.30) of counts 1,2,3,4 over (-inf,1] (1,2] (2,5] (5,inf) is in \
         the second bucket, got {q}"
    );
}

/// An empty explicit histogram is still a histogram.
#[test]
fn an_explicit_histogram_with_no_observations_is_accepted() {
    let h = from_otlp_explicit(&[1.0, 2.0], &[0, 0, 0], 0, Some(0.0)).expect("valid");
    assert_eq!(h.count, 0.0);
    assert!(h.positive.is_empty(), "canonicalise drops empty buckets");
}

/// Custom boundaries that are not ascending are refused.
#[test]
fn unordered_explicit_bounds_are_refused() {
    assert!(from_otlp_explicit(&[2.0, 1.0], &[1, 1, 1], 3, Some(6.0)).is_err());
}
