//! Ground truth for the native histogram model.
//!
//! Every assertion here comes from the **specification's own formula** or from
//! a property that holds independently of how the code works — not from
//! reading the implementation back to itself. That distinction is the one that
//! cost this project a wrong `ln_gamma`, an 11 %-wrong `ljung_box` and a 22 %
//! amplitude loss in STL: a statistical routine's output looks correct while
//! being wrong, and a test written from the code cannot see it.

use super::*;

/// `(2^(2^-n))^i` — the specification's bucket upper bound, written the way
/// the specification writes it rather than the way the code computes it.
fn spec_upper(schema: i8, index: i32) -> f64 {
    let base: f64 = (2.0_f64).powf((2.0_f64).powi(-i32::from(schema)));
    base.powi(index)
}

fn close(a: f64, b: f64, tol: f64) -> bool {
    (a - b).abs() <= tol * a.abs().max(b.abs()).max(1.0)
}

// ── The bucket geometry ──────────────────────────────────────────────

#[test]
fn bucket_bounds_follow_the_specification_formula() {
    for schema in MIN_SCHEMA..=MAX_SCHEMA {
        let h = Histogram::empty(schema);
        for index in -20..=20 {
            let (lower, upper) = h.bucket_bounds(index, false);
            assert!(
                close(upper, spec_upper(schema, index), 1e-12),
                "schema {schema} bucket {index}: upper {upper} vs spec {}",
                spec_upper(schema, index)
            );
            assert!(
                close(lower, spec_upper(schema, index - 1), 1e-12),
                "schema {schema} bucket {index}: lower {lower} vs spec {}",
                spec_upper(schema, index - 1)
            );
        }
    }
}

#[test]
fn schema_zero_buckets_are_the_powers_of_two() {
    // The anchoring case a reader can check by eye: at schema 0 the bounds
    // are 1, 2, 4, 8 … and bucket 0 ends at 1.
    let h = Histogram::empty(0);
    assert_eq!(h.bucket_bounds(0, false), (0.5, 1.0));
    assert_eq!(h.bucket_bounds(1, false), (1.0, 2.0));
    assert_eq!(h.bucket_bounds(2, false), (2.0, 4.0));
    assert_eq!(h.bucket_bounds(-1, false), (0.25, 0.5));
}

#[test]
fn each_schema_has_twice_the_resolution_of_the_one_below() {
    // "Schema n has half the resolution of schema n+1" — so one bucket at
    // schema n spans exactly two at schema n+1.
    for schema in MIN_SCHEMA..MAX_SCHEMA {
        let coarse = Histogram::empty(schema);
        let fine = Histogram::empty(schema + 1);
        let (lo, hi) = coarse.bucket_bounds(1, false);
        let (flo, _) = fine.bucket_bounds(1, false);
        let (_, fhi) = fine.bucket_bounds(2, false);
        assert!(
            close(lo, flo, 1e-12) && close(hi, fhi, 1e-12),
            "schema {schema} bucket 1 ({lo}, {hi}] should be schema {} buckets 1..2 ({flo}, {fhi}]",
            schema + 1
        );
    }
}

#[test]
fn a_value_lands_in_the_bucket_whose_bounds_contain_it() {
    // The inverse property, checked against the bounds rather than against
    // the index formula: whatever index we compute, the value must sit in
    // `(lower, upper]`.
    for schema in MIN_SCHEMA..=MAX_SCHEMA {
        let h = Histogram::empty(schema);
        for v in [
            1e-9, 0.001, 0.5, 0.999, 1.0, 1.0001, 1.5, 2.0, 3.0, 10.0, 1024.0, 1e6, 1e12,
        ] {
            let i = Histogram::index_for(schema, v);
            let (lo, hi) = h.bucket_bounds(i, false);
            assert!(
                v > lo * (1.0 - 1e-9) && v <= hi * (1.0 + 1e-9),
                "schema {schema}: {v} got index {i}, whose bounds are ({lo}, {hi}]"
            );
        }
    }
}

#[test]
fn an_exact_bucket_bound_falls_in_the_bucket_it_closes() {
    // Upper bounds are *inclusive*, so `2.0` at schema 0 belongs to bucket 1,
    // not bucket 2. Off-by-one here moves every observation that is exactly a
    // power of two — which, for latencies in seconds, is most of them.
    for schema in MIN_SCHEMA..=MAX_SCHEMA {
        for index in -8..=8 {
            let bound = spec_upper(schema, index);
            if !bound.is_finite() || bound <= 0.0 {
                continue;
            }
            assert_eq!(
                Histogram::index_for(schema, bound),
                index,
                "schema {schema}: the bound {bound} closes bucket {index}"
            );
        }
    }
}

// ── Observation routing ──────────────────────────────────────────────

#[test]
fn observations_are_counted_once_and_summed() {
    let mut h = Histogram::empty(2);
    let values = [0.5, 1.0, 1.5, 2.0, 8.0, 100.0];
    for v in values {
        h.observe(v);
    }
    assert_eq!(h.count, values.len() as f64);
    assert!(close(h.sum, values.iter().sum::<f64>(), 1e-12));
    let bucketed: f64 = h.positive.iter().map(|b| b.count).sum::<f64>() + h.zero_count;
    assert_eq!(bucketed, h.count, "every observation landed somewhere");
    h.validate()
        .expect("a histogram built by observing is valid");
}

#[test]
fn the_zero_bucket_is_closed_on_both_sides() {
    let mut h = Histogram::empty(0);
    h.zero_threshold = 0.5;
    for v in [-0.5, -0.1, 0.0, 0.1, 0.5] {
        h.observe(v);
    }
    assert_eq!(h.zero_count, 5.0, "[-t, +t] is inclusive at both ends");
    h.observe(0.5000001);
    assert_eq!(h.zero_count, 5.0, "just outside is not in it");
    assert_eq!(h.positive.len(), 1);
}

#[test]
fn negative_observations_go_to_the_negative_side() {
    let mut h = Histogram::empty(0);
    h.observe(-2.0);
    h.observe(2.0);
    assert_eq!(h.negative.len(), 1);
    assert_eq!(h.positive.len(), 1);
    assert_eq!(
        h.negative[0].index, h.positive[0].index,
        "the sides mirror: -2 and 2 have the same magnitude index"
    );
    let (lo, hi) = h.bucket_bounds(h.negative[0].index, true);
    assert!(lo <= -2.0 && -2.0 < hi, "-2 sits in [{lo}, {hi})");
}

// ── Quantiles ────────────────────────────────────────────────────────

#[test]
fn a_quantile_of_a_single_bucket_stays_inside_it() {
    let mut h = Histogram::empty(0);
    for _ in 0..100 {
        h.observe(1.5); // bucket (1, 2]
    }
    for q in [0.0, 0.01, 0.25, 0.5, 0.75, 0.99, 1.0] {
        let v = h.quantile(q);
        assert!(
            (1.0..=2.0).contains(&v),
            "q={q} gave {v}, outside the only populated bucket (1, 2]"
        );
    }
}

#[test]
fn quantiles_are_monotone_in_q() {
    let mut h = Histogram::empty(3);
    for i in 1..=1000 {
        h.observe(f64::from(i) / 10.0);
    }
    let mut previous = f64::NEG_INFINITY;
    for k in 0..=100 {
        let q = f64::from(k) / 100.0;
        let v = h.quantile(q);
        assert!(
            v >= previous - 1e-9,
            "quantile decreased between q={} and q={q}: {previous} then {v}",
            f64::from(k - 1) / 100.0
        );
        previous = v;
    }
}

#[test]
fn the_median_of_a_symmetric_distribution_is_near_zero() {
    let mut h = Histogram::empty(2);
    h.zero_threshold = 0.001;
    for i in 1..=500 {
        let v = f64::from(i) / 100.0;
        h.observe(v);
        h.observe(-v);
    }
    let m = h.quantile(0.5);
    assert!(
        m.abs() < 0.2,
        "a distribution symmetric about zero should have a median near zero, got {m}"
    );
}

#[test]
fn quantile_interpolates_exponentially_not_linearly() {
    // The detail a naive implementation gets wrong, and the reason this test
    // exists: inside one wide bucket (1, 1024] the halfway point by
    // *population* is at the geometric middle, 32 — not the arithmetic
    // middle, 512. Schema -3 gives base 2^8 = 256... use schema -4 (base
    // 65536) so a single bucket spans a wide range.
    let mut h = Histogram::empty(-3);
    let (lo, hi) = h.bucket_bounds(2, false); // (256, 65536] at schema -3
    for _ in 0..100 {
        h.observe((lo * hi).sqrt()); // everything in that one bucket
    }
    let mid = h.quantile(0.5);
    let geometric = (lo * hi).sqrt();
    let arithmetic = (lo + hi) / 2.0;
    assert!(
        (mid - geometric).abs() < (mid - arithmetic).abs(),
        "q=0.5 inside ({lo}, {hi}] gave {mid}; the geometric middle is \
         {geometric} and the arithmetic middle {arithmetic} — exponential \
         buckets interpolate on a log scale"
    );
}

#[test]
fn quantile_edge_cases_match_the_specification() {
    let mut h = Histogram::empty(0);
    h.observe(1.0);
    assert_eq!(h.quantile(-0.1), f64::NEG_INFINITY);
    assert_eq!(h.quantile(1.1), f64::INFINITY);
    assert!(h.quantile(f64::NAN).is_nan());
    assert!(Histogram::empty(0).quantile(0.5).is_nan(), "empty is NaN");
}

// ── Fractions ────────────────────────────────────────────────────────

#[test]
fn the_fraction_over_everything_is_one() {
    let mut h = Histogram::empty(1);
    for i in 1..=200 {
        h.observe(f64::from(i));
    }
    let all = h.fraction(f64::NEG_INFINITY, f64::INFINITY);
    assert!(close(all, 1.0, 1e-9), "fraction over (-inf, inf) was {all}");
}

#[test]
fn fraction_and_quantile_are_inverses() {
    // The strongest property available without a reference implementation:
    // the fraction below the q-th quantile is q.
    let mut h = Histogram::empty(3);
    for i in 1..=1000 {
        h.observe(f64::from(i) / 7.0);
    }
    for q in [0.1, 0.25, 0.5, 0.75, 0.9, 0.95] {
        let v = h.quantile(q);
        let f = h.fraction(f64::NEG_INFINITY, v);
        assert!(
            (f - q).abs() < 1e-6,
            "quantile({q}) = {v}, but the fraction below {v} is {f}"
        );
    }
}

#[test]
fn fraction_of_an_inverted_or_empty_interval_is_zero() {
    let mut h = Histogram::empty(0);
    h.observe(1.0);
    assert_eq!(h.fraction(5.0, 1.0), 0.0);
    assert_eq!(h.fraction(2.0, 2.0), 0.0);
}

// ── Moments ──────────────────────────────────────────────────────────

#[test]
fn a_constant_distribution_has_no_spread() {
    let mut h = Histogram::empty(8); // finest resolution
    for _ in 0..1000 {
        h.observe(42.0);
    }
    assert!(close(h.avg(), 42.0, 1e-9), "mean was {}", h.avg());
    assert!(
        h.stddev() < 0.2,
        "a thousand identical observations should have near-zero spread, got {}",
        h.stddev()
    );
}

#[test]
fn variance_uses_the_geometric_representative() {
    // A bucket's representative is √(lower·upper), not (lower+upper)/2. With
    // every observation in one bucket, the variance is the squared distance
    // between the true mean and that representative — so using the
    // arithmetic midpoint would give a visibly larger answer.
    let mut h = Histogram::empty(-2); // base 16, wide buckets
    let (lo, hi) = h.bucket_bounds(1, false);
    let g = (lo * hi).sqrt();
    for _ in 0..100 {
        h.observe(g);
    }
    assert!(
        h.variance() < 1e-6,
        "observations at the geometric representative should have ~zero \
         variance under a geometric representative; got {}",
        h.variance()
    );
}

// ── The classic view ─────────────────────────────────────────────────

#[test]
fn the_classic_view_is_cumulative_and_ends_at_inf() {
    let mut h = Histogram::empty(1);
    for v in [0.3, 0.7, 1.5, 3.0, 9.0] {
        h.observe(v);
    }
    let classic = h.classic_buckets();
    assert_eq!(
        classic.last().expect("at least +Inf").0,
        f64::INFINITY,
        "a classic exposition always ends at +Inf"
    );
    assert_eq!(
        classic.last().expect("at least +Inf").1,
        h.count,
        "the +Inf bucket holds every observation"
    );
    let mut previous = (f64::NEG_INFINITY, 0.0);
    for (le, cumulative) in classic {
        assert!(le > previous.0, "boundaries ascend");
        assert!(cumulative >= previous.1, "counts are cumulative");
        previous = (le, cumulative);
    }
}

#[test]
fn the_classic_view_agrees_with_fraction() {
    // The view is a *view*: the count it reports below a boundary must be the
    // count the histogram itself reports below that boundary.
    let mut h = Histogram::empty(2);
    for i in 1..=300 {
        h.observe(f64::from(i) / 3.0);
    }
    for (le, cumulative) in h.classic_buckets() {
        if !le.is_finite() {
            continue;
        }
        let direct = h.fraction(f64::NEG_INFINITY, le) * h.count;
        assert!(
            (direct - cumulative).abs() < 1e-6,
            "classic view says {cumulative} at or below {le}; the histogram says {direct}"
        );
    }
}

// ── Custom buckets (NHCB) ────────────────────────────────────────────

fn classic_style() -> Histogram {
    let mut h = Histogram::empty(CUSTOM_BUCKETS_SCHEMA);
    h.custom_values = vec![0.1, 0.5, 1.0, 5.0];
    h
}

#[test]
fn custom_buckets_use_the_listed_boundaries() {
    let h = classic_style();
    assert_eq!(h.bucket_bounds(0, false), (f64::NEG_INFINITY, 0.1));
    assert_eq!(h.bucket_bounds(1, false), (0.1, 0.5));
    assert_eq!(h.bucket_bounds(3, false), (1.0, 5.0));
    assert_eq!(h.bucket_bounds(4, false), (5.0, f64::INFINITY));
}

#[test]
fn a_custom_bucket_observation_lands_by_upper_inclusive_bound() {
    let mut h = classic_style();
    for v in [0.05, 0.1, 0.3, 0.5, 0.9, 5.0, 100.0] {
        h.observe(v);
    }
    // 0.05 and 0.1 → bucket 0; 0.3 and 0.5 → bucket 1; 0.9 → 2; 5.0 → 3; 100 → 4
    let counts: Vec<(i32, f64)> = h.positive.iter().map(|b| (b.index, b.count)).collect();
    assert_eq!(
        counts,
        vec![(0, 2.0), (1, 2.0), (2, 1.0), (3, 1.0), (4, 1.0)]
    );
    h.validate().expect("valid");
}

#[test]
fn custom_buckets_interpolate_linearly() {
    // No exponential geometry to respect, so the midpoint by population of a
    // uniformly-filled custom bucket is its arithmetic middle.
    let mut h = Histogram::empty(CUSTOM_BUCKETS_SCHEMA);
    h.custom_values = vec![0.0, 100.0];
    h.count = 100.0;
    h.sum = 5000.0;
    h.positive = vec![Bucket {
        index: 1,
        count: 100.0,
    }];
    let m = h.quantile(0.5);
    assert!(
        close(m, 50.0, 1e-9),
        "linear interpolation gives 50, got {m}"
    );
}

// ── Validation ───────────────────────────────────────────────────────

#[test]
fn validate_refuses_a_schema_outside_the_defined_range() {
    for bad in [i8::MIN, -60, -5, 9, 100] {
        let h = Histogram::empty(bad);
        assert!(
            matches!(h.validate(), Err(HistogramError::InvalidSchema(_))),
            "schema {bad} should be refused"
        );
    }
    for good in MIN_SCHEMA..=MAX_SCHEMA {
        Histogram::empty(good).validate().expect("valid schema");
    }
}

#[test]
fn validate_refuses_unordered_or_duplicated_buckets() {
    let mut h = Histogram::empty(0);
    h.count = 3.0;
    h.positive = vec![
        Bucket {
            index: 5,
            count: 1.0,
        },
        Bucket {
            index: 2,
            count: 1.0,
        },
    ];
    assert!(matches!(
        h.validate(),
        Err(HistogramError::UnorderedBuckets { .. })
    ));
    h.positive = vec![
        Bucket {
            index: 2,
            count: 1.0,
        },
        Bucket {
            index: 2,
            count: 1.0,
        },
    ];
    assert!(
        matches!(h.validate(), Err(HistogramError::UnorderedBuckets { .. })),
        "a repeated index is not strictly ascending"
    );
}

#[test]
fn validate_refuses_parts_that_exceed_the_whole() {
    let mut h = Histogram::empty(0);
    h.count = 1.0;
    h.positive = vec![Bucket {
        index: 1,
        count: 5.0,
    }];
    assert!(matches!(
        h.validate(),
        Err(HistogramError::CountMismatch { .. })
    ));
}

#[test]
fn validate_allows_a_count_above_the_buckets_for_nan_observations() {
    // A NaN observation is counted and falls in no bucket, so `count` may
    // legitimately exceed the bucket total. Refusing that would refuse real
    // Prometheus traffic.
    let mut h = Histogram::empty(0);
    h.count = 10.0;
    h.sum = f64::NAN;
    h.positive = vec![Bucket {
        index: 1,
        count: 9.0,
    }];
    h.validate()
        .expect("NaN observations leave count above the bucket total");
}

#[test]
fn validate_refuses_a_count_beyond_exact_representation() {
    let mut h = Histogram::empty(0);
    h.count = MAX_EXACT_COUNT * 2.0;
    assert!(matches!(
        h.validate(),
        Err(HistogramError::CountNotExact { .. })
    ));
}

#[test]
fn validate_refuses_custom_values_on_an_exponential_schema_and_vice_versa() {
    let mut h = Histogram::empty(0);
    h.custom_values = vec![1.0];
    assert!(matches!(h.validate(), Err(HistogramError::CustomValues(_))));

    let h = Histogram::empty(CUSTOM_BUCKETS_SCHEMA);
    assert!(
        matches!(h.validate(), Err(HistogramError::CustomValues(_))),
        "schema -53 with no boundaries has no buckets at all"
    );
}

#[test]
fn validate_refuses_a_negative_or_infinite_count() {
    let mut h = Histogram::empty(0);
    h.count = -1.0;
    assert!(matches!(
        h.validate(),
        Err(HistogramError::InvalidCount { .. })
    ));
    h.count = f64::INFINITY;
    assert!(matches!(
        h.validate(),
        Err(HistogramError::InvalidCount { .. })
    ));
}

// ── Canonical form ───────────────────────────────────────────────────

#[test]
fn canonicalise_sorts_merges_and_drops_empties() {
    let mut h = Histogram::empty(0);
    h.count = 6.0;
    h.positive = vec![
        Bucket {
            index: 3,
            count: 1.0,
        },
        Bucket {
            index: 1,
            count: 2.0,
        },
        Bucket {
            index: 3,
            count: 3.0,
        },
        Bucket {
            index: 7,
            count: 0.0,
        },
    ];
    h.canonicalise();
    assert_eq!(
        h.positive,
        vec![
            Bucket {
                index: 1,
                count: 2.0
            },
            Bucket {
                index: 3,
                count: 4.0
            },
        ],
        "sorted, duplicates summed, empty dropped"
    );
    h.validate().expect("canonical form is valid");
}

#[test]
fn canonicalise_is_idempotent() {
    let mut h = Histogram::empty(0);
    h.count = 10.0;
    h.positive = vec![
        Bucket {
            index: 2,
            count: 1.0,
        },
        Bucket {
            index: 2,
            count: 1.0,
        },
        Bucket {
            index: 1,
            count: 0.0,
        },
    ];
    h.canonicalise();
    let once = h.clone();
    h.canonicalise();
    assert_eq!(h, once, "a canonical form is a fixed point");
}

// ── Merge ────────────────────────────────────────────────────────────

#[test]
fn merging_adds_every_total_and_bucket() {
    let mut a = Histogram::empty(1);
    let mut b = Histogram::empty(1);
    for v in [1.0, 2.0, 3.0] {
        a.observe(v);
    }
    for v in [2.0, 3.0, 4.0] {
        b.observe(v);
    }
    let (ca, cb, sa, sb) = (a.count, b.count, a.sum, b.sum);
    a.merge(&b).expect("same schema");
    assert_eq!(a.count, ca + cb);
    assert!(close(a.sum, sa + sb, 1e-12));
    a.validate().expect("a merged histogram is valid");
    let bucketed: f64 = a.positive.iter().map(|x| x.count).sum::<f64>() + a.zero_count;
    assert_eq!(bucketed, a.count);
}

#[test]
fn merging_is_the_same_as_observing_everything_once() {
    // The property that makes a rollup correct: aggregating two histograms
    // must equal a histogram of the combined observations.
    let values_a = [0.5, 1.5, 2.5, 10.0];
    let values_b = [1.0, 3.0, 30.0];
    let mut a = Histogram::empty(2);
    let mut b = Histogram::empty(2);
    let mut both = Histogram::empty(2);
    for v in values_a {
        a.observe(v);
        both.observe(v);
    }
    for v in values_b {
        b.observe(v);
        both.observe(v);
    }
    a.merge(&b).expect("merge");
    a.canonicalise();
    both.canonicalise();
    assert_eq!(a.count, both.count);
    assert!(close(a.sum, both.sum, 1e-12));
    assert_eq!(a.positive, both.positive, "bucket for bucket");
}

#[test]
fn merging_different_schemas_is_refused_rather_than_guessed() {
    let mut a = Histogram::empty(1);
    let b = Histogram::empty(2);
    assert!(
        a.merge(&b).is_err(),
        "two resolutions cannot be added without re-bucketing, and silently \
         picking one would change what the data means"
    );
}

#[test]
fn merging_different_zero_thresholds_is_refused() {
    let mut a = Histogram::empty(0);
    a.observe(1.0);
    let mut b = Histogram::empty(0);
    b.zero_threshold = 0.5;
    b.observe(1.0);
    assert!(
        a.merge(&b).is_err(),
        "the zero buckets cover different intervals"
    );
}

#[test]
fn merging_into_an_empty_histogram_adopts_its_zero_threshold() {
    let mut a = Histogram::empty(0);
    let mut b = Histogram::empty(0);
    b.zero_threshold = 0.25;
    b.observe(0.1);
    a.merge(&b)
        .expect("an empty accumulator takes the first shape it sees");
    assert_eq!(a.zero_threshold, 0.25);
    assert_eq!(a.zero_count, 1.0);
}

// ── Round trip ───────────────────────────────────────────────────────

#[test]
fn a_histogram_round_trips_through_postcard() {
    // `postcard` is what every persisted format in this tree uses, so this is
    // the serialisation the WAL and the catalog will perform.
    let mut h = Histogram::empty(3);
    h.zero_threshold = 0.001;
    h.reset_hint = ResetHint::NoReset;
    for i in 1..=50 {
        h.observe(f64::from(i) / 11.0);
        h.observe(-f64::from(i) / 13.0);
    }
    let bytes = postcard::to_allocvec(&h).expect("encode");
    let back: Histogram = postcard::from_bytes(&bytes).expect("decode");
    assert_eq!(back, h);
    back.validate().expect("valid after a round trip");
}

#[test]
fn a_custom_bucket_histogram_round_trips() {
    let mut h = classic_style();
    for v in [0.05, 0.3, 2.0, 50.0] {
        h.observe(v);
    }
    let bytes = postcard::to_allocvec(&h).expect("encode");
    assert_eq!(
        postcard::from_bytes::<Histogram>(&bytes).expect("decode"),
        h
    );
}

// ── Does it recover a distribution we know the answer for? ───────────

#[test]
fn quantiles_of_a_uniform_distribution_are_recovered_within_bucket_resolution() {
    // The acid test: a histogram is a lossy summary, and the loss is bounded
    // by the bucket width. At schema 8 there are 256 buckets per octave, so a
    // quantile should come back within a fraction of a percent — and a
    // systematic error (a wrong interpolation, an off-by-one bucket) shows up
    // as a bias far larger than that.
    let mut h = Histogram::empty(8);
    let n = 100_000;
    for i in 1..=n {
        h.observe(1.0 + 999.0 * (f64::from(i) - 0.5) / f64::from(n));
    }
    for q in [0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99] {
        let truth = 1.0 + 999.0 * q;
        let got = h.quantile(q);
        let relative = (got - truth).abs() / truth;
        assert!(
            relative < 0.01,
            "uniform[1,1000] q={q}: true {truth:.4}, histogram {got:.4} \
             ({:.3}% off — schema 8 buckets are 0.27% wide, so this is a \
             systematic error rather than resolution)",
            relative * 100.0
        );
    }
}

#[test]
fn a_coarse_schema_is_less_accurate_but_not_biased() {
    // Resolution should cost accuracy symmetrically. A one-sided error means
    // the interpolation is wrong, not that the buckets are wide.
    let mut signed_error = 0.0;
    let mut samples = 0;
    for schema in [0_i8, 2, 4] {
        let mut h = Histogram::empty(schema);
        let n = 20_000;
        for i in 1..=n {
            h.observe(1.0 + 999.0 * (f64::from(i) - 0.5) / f64::from(n));
        }
        for q in [0.1, 0.3, 0.5, 0.7, 0.9] {
            let truth = 1.0 + 999.0 * q;
            signed_error += (h.quantile(q) - truth) / truth;
            samples += 1;
        }
    }
    let bias = signed_error / f64::from(samples);
    assert!(
        bias.abs() < 0.05,
        "quantiles are biased by {:.2}% across schemas — a coarse bucket \
         should lose precision symmetrically, so a one-sided error means the \
         interpolation is wrong",
        bias * 100.0
    );
}

#[test]
fn the_sum_is_the_sum_regardless_of_resolution() {
    // `count` and `sum` are exact whatever the schema: they are not bucketed.
    // If a coarse schema changed them, the summary would be lossy in a
    // dimension it is not supposed to be.
    let values: Vec<f64> = (1..=500).map(|i| f64::from(i) * 1.37).collect();
    let mut reference: Option<(f64, f64)> = None;
    for schema in MIN_SCHEMA..=MAX_SCHEMA {
        let mut h = Histogram::empty(schema);
        for &v in &values {
            h.observe(v);
        }
        match reference {
            None => reference = Some((h.count, h.sum)),
            Some((c, s)) => {
                assert_eq!(h.count, c, "schema {schema} changed the count");
                assert!(close(h.sum, s, 1e-12), "schema {schema} changed the sum");
            }
        }
    }
}

// ── Bucket bounds are exact, not merely close ────────────────────────

/// Every bound that *is* a power of two is that power of two exactly.
///
/// `2^(index / 2^schema)` is an exact power of two whenever `index` is a
/// multiple of `2^schema`, at every schema. Computed with `powf` it was not:
/// schema 0 index 0 came back as `0.4999999999999999`, and under Miri — which
/// perturbs `powf` by a ULP on purpose, because its last bit is unspecified —
/// it moved from run to run. A bucket boundary decides which bucket an
/// observation belongs to, so it cannot be allowed to depend on the machine.
#[test]
fn bounds_that_are_powers_of_two_are_exact_at_every_schema() {
    /// `2^k`, by exact doubling — a reference that owes nothing to the code
    /// under test and nothing to `powi`, whose last bit is unspecified.
    fn reference(k: i32) -> f64 {
        let mut v = 1.0_f64;
        for _ in 0..k.abs() {
            if k > 0 {
                v *= 2.0;
            } else {
                v /= 2.0;
            }
        }
        v
    }

    for schema in MIN_SCHEMA..=MAX_SCHEMA {
        // bound(schema, index) = 2^(index / 2^schema). It is an exact power of
        // two whenever that exponent is an integer: for schema <= 0 that is
        // every index, and for schema > 0 the multiples of 2^schema.
        for power in -6..=6i32 {
            let (index, exponent) = if schema <= 0 {
                (power, power * (1 << -i32::from(schema)))
            } else {
                (power * (1 << schema), power)
            };
            assert_eq!(
                Histogram::bound(schema, index),
                reference(exponent),
                "schema {schema}, index {index}: 2^{exponent} must be exact"
            );
        }
    }
}

/// Bounds ascend strictly, so no observation can fall in two buckets.
#[test]
fn bounds_are_strictly_increasing_in_the_index() {
    for schema in MIN_SCHEMA..=MAX_SCHEMA {
        let mut previous = f64::NEG_INFINITY;
        for index in -40..=40 {
            let b = Histogram::bound(schema, index);
            assert!(
                b > previous,
                "schema {schema}: bound({index}) = {b} did not exceed {previous}"
            );
            previous = b;
        }
    }
}

/// `base()` is the ratio between consecutive bounds, at every schema.
#[test]
fn base_is_the_ratio_between_consecutive_bounds() {
    for schema in MIN_SCHEMA..=MAX_SCHEMA {
        let base = Histogram::base(schema);
        let ratio = Histogram::bound(schema, 1) / Histogram::bound(schema, 0);
        assert!(
            (ratio - base).abs() <= 4.0 * f64::EPSILON * base,
            "schema {schema}: base is {base} but consecutive bounds differ by {ratio}"
        );
    }
}

/// A quantile never leaves the bucket it was located in.
///
/// The interpolation runs through `log2` and `powf`, neither of which is
/// correctly rounded, so `2^log2(x)` need not be `x`: `q = 1` over the bucket
/// `(1, 2]` returned `2.000000000000003`. A quantile outside its own bucket
/// is wrong by definition, however small the excess.
#[test]
fn a_quantile_never_leaves_its_bucket_at_any_schema() {
    for schema in MIN_SCHEMA..=MAX_SCHEMA {
        for index in [-3i32, 0, 1, 5] {
            let mut h = Histogram::empty(schema);
            h.positive = vec![Bucket { index, count: 10.0 }];
            h.count = 10.0;
            h.sum = 10.0;
            h.validate().expect("fixture is valid");

            let (lo, hi) = h.bucket_bounds(index, false);
            for step in 0..=20 {
                let q = f64::from(step) / 20.0;
                let v = h.quantile(q);
                assert!(
                    v >= lo && v <= hi,
                    "schema {schema}, index {index}, q={q}: {v} is outside ({lo}, {hi}]"
                );
            }
        }
    }
}

/// The same holds on the negative side, where the mirror is easy to get wrong.
#[test]
fn a_negative_quantile_never_leaves_its_bucket() {
    for schema in MIN_SCHEMA..=MAX_SCHEMA {
        let mut h = Histogram::empty(schema);
        h.negative = vec![Bucket {
            index: 2,
            count: 8.0,
        }];
        h.count = 8.0;
        h.sum = -8.0;
        h.validate().expect("fixture is valid");

        let (lo, hi) = h.bucket_bounds(2, true);
        for step in 0..=20 {
            let q = f64::from(step) / 20.0;
            let v = h.quantile(q);
            assert!(
                v >= lo && v <= hi,
                "schema {schema}, q={q}: {v} is outside [{lo}, {hi})"
            );
        }
    }
}

/// `exp2i` is the exponent field, so it agrees with the literal powers.
#[test]
fn exp2i_matches_the_literal_powers_of_two() {
    assert_eq!(exp2i(0), 1.0);
    assert_eq!(exp2i(1), 2.0);
    assert_eq!(exp2i(-1), 0.5);
    assert_eq!(exp2i(-2), 0.25);
    assert_eq!(exp2i(10), 1024.0);
    assert_eq!(exp2i(-1022), f64::MIN_POSITIVE);
    // The smallest subnormal, and one step past it.
    assert_eq!(exp2i(-1074), 5e-324);
    assert_eq!(exp2i(-1075), 0.0);
    // The largest power of two, and one step past it.
    assert_eq!(exp2i(1023), 8.988_465_674_311_58e307);
    assert_eq!(exp2i(1024), f64::INFINITY);
}

/// `root_of_two(n)` squared `n` times returns to 2.
#[test]
fn repeated_square_roots_of_two_invert_by_squaring() {
    for n in 0..=8u32 {
        let mut v = root_of_two(n);
        for _ in 0..n {
            v *= v;
        }
        assert!(
            (v - 2.0).abs() < 1e-12,
            "{n} roots of two squared back gave {v}"
        );
    }
}

/// The endpoint guards must not swallow a `NaN` φ.
///
/// `quantile` screens `NaN` before it reaches `interpolate`, so this asserts
/// the interpolation's *own* behaviour rather than relying on that screen —
/// the two guards that return `lower` at φ ≤ 0 and `upper` at φ ≥ 1 are one
/// spelling away from turning a `NaN` into a plausible number. Written
/// `!(fraction > 0.0)`, the first of them did exactly that.
#[test]
fn interpolation_propagates_a_nan_fraction_rather_than_returning_a_bound() {
    let h = Histogram::empty(0);
    assert!(
        h.interpolate(1.0, 2.0, f64::NAN).is_nan(),
        "a NaN fraction must stay NaN, not become the bucket's lower bound"
    );
    // And the guards themselves still do their job.
    assert_eq!(h.interpolate(1.0, 2.0, 0.0), 1.0);
    assert_eq!(h.interpolate(1.0, 2.0, 1.0), 2.0);
    assert_eq!(h.interpolate(1.0, 2.0, -0.5), 1.0);
    assert_eq!(h.interpolate(1.0, 2.0, 1.5), 2.0);
}
