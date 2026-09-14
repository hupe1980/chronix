//! Native histograms on the wire, in both directions and both protocols.
//!
//! Three encodings describe the same distribution, and this module is the only
//! place that translates between them and
//! [`chronix_core::histogram::Histogram`]:
//!
//! - **Prometheus remote write** 1.0 and 2.0 — buckets as *spans* (runs of
//!   consecutive populated buckets) plus a flat array of integer deltas or
//!   absolute float counts.
//! - **OTLP exponential histograms** — one contiguous run per sign with an
//!   offset.
//!
//! ## The index convention
//!
//! Prometheus bucket *i* covers `(base^(i-1), base^i]`; OTLP bucket *i* covers
//! `(base^i, base^(i+1)]`. They are off by one, both call the field an index,
//! and a copied index produces a histogram that validates, stores cleanly, and
//! answers every quantile a factor of `base` wrong. [`from_otlp_exponential`]
//! adds the one; nothing else may.

use chronix_core::histogram::{Bucket, CUSTOM_BUCKETS_SCHEMA, Histogram, ResetHint};

/// Everything the two remote-write versions say about one histogram.
///
/// The v1 and v2 protobuf messages are field-for-field identical and generate
/// two unrelated Rust types. Rather than duplicating the decoder, each is
/// borrowed into this shape first. Spans are copied because they are a handful
/// of integers; the bucket arrays, which are not, stay borrowed.
pub(crate) struct WireHistogram<'a> {
    /// Total observations. `None` distinguishes an integer histogram from a
    /// float one — see [`WireHistogram::is_float`].
    pub count_int: Option<u64>,
    /// Total observations, when the producer sent the float variant.
    pub count_float: Option<f64>,
    /// Sum of observations.
    pub sum: f64,
    /// `-4..=8`, or `-53` for custom boundaries.
    pub schema: i32,
    /// Half-width of the zero bucket.
    pub zero_threshold: f64,
    /// Zero-bucket count, integer variant.
    pub zero_count_int: Option<u64>,
    /// Zero-bucket count, float variant.
    pub zero_count_float: Option<f64>,
    /// Negative-side spans, as `(offset, length)`.
    pub negative_spans: Vec<(i32, u32)>,
    /// Negative-side counts as deltas (integer histograms).
    pub negative_deltas: &'a [i64],
    /// Negative-side counts, absolute (float histograms).
    pub negative_counts: &'a [f64],
    /// Positive-side spans, as `(offset, length)`.
    pub positive_spans: Vec<(i32, u32)>,
    /// Positive-side counts as deltas (integer histograms).
    pub positive_deltas: &'a [i64],
    /// Positive-side counts, absolute (float histograms).
    pub positive_counts: &'a [f64],
    /// Counter-reset hint, as the protobuf enum's discriminant.
    pub reset_hint: i32,
    /// Sample time, milliseconds since epoch.
    pub timestamp: i64,
    /// Upper-inclusive boundaries when `schema == -53`.
    pub custom_values: &'a [f64],
}

/// Why a histogram on the wire could not be stored.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub(crate) enum WireHistogramError {
    /// A span's length ran past the end of the count array.
    #[error(
        "histogram spans describe {declared} buckets but only {supplied} counts were sent: \
         the spans and the bucket array disagree, and guessing which is right would \
         silently move every boundary"
    )]
    SpanOverrun {
        /// Buckets the spans account for.
        declared: usize,
        /// Counts actually present.
        supplied: usize,
    },
    /// Running the deltas produced a negative count.
    #[error(
        "histogram bucket deltas run negative at index {index}: deltas are differences \
         from the previous bucket and a bucket cannot hold fewer than no observations"
    )]
    NegativeRunningCount {
        /// The bucket index at which it went negative.
        index: i32,
    },
    /// A bucket index did not fit in `i32`.
    #[error("histogram bucket index overflowed at span offset {offset}")]
    IndexOverflow {
        /// The offset that pushed it over.
        offset: i32,
    },
    /// The schema did not fit chronix's `i8`, before it was even range-checked.
    #[error(
        "histogram schema {0} is not a bucket resolution: expected -4..=8 for \
         exponential buckets, or -53 for custom boundaries"
    )]
    SchemaOutOfRange(i32),
    /// The decoded histogram was not self-consistent.
    #[error("histogram is not valid: {0}")]
    Invalid(#[from] chronix_core::histogram::HistogramError),
}

impl WireHistogram<'_> {
    /// Did the producer send float counts?
    ///
    /// The `count` field is the discriminator, which is how Prometheus itself
    /// decides. Upstream tests `GetCountFloat() > 0`, which misreads an empty
    /// float histogram as an integer one; the protobuf presence bit says what
    /// the producer actually sent, so that is what is used here.
    fn is_float(&self) -> bool {
        self.count_float.is_some() || self.zero_count_float.is_some()
    }

    /// Is this a staleness marker rather than an observation?
    ///
    /// Prometheus marks a histogram series stale by sending a histogram whose
    /// sum carries the staleness NaN payload. It is not data, and storing it
    /// would put a NaN-sum histogram in the middle of a series.
    pub(crate) fn is_stale_marker(&self) -> bool {
        self.sum.is_nan() && self.sum.to_bits() == 0x7ff0_0000_0000_0002
    }

    /// Decode into chronix's histogram, or say why not.
    ///
    /// The result is canonicalised (empty buckets dropped, buckets ordered)
    /// and validated, so a caller that stores it is storing something that
    /// reads back.
    pub(crate) fn to_histogram(&self) -> Result<Histogram, WireHistogramError> {
        let schema = i8::try_from(self.schema)
            .map_err(|_| WireHistogramError::SchemaOutOfRange(self.schema))?;

        let float = self.is_float();
        let count = self
            .count_float
            .or_else(|| self.count_int.map(|v| v as f64))
            .unwrap_or(0.0);
        let zero_count = self
            .zero_count_float
            .or_else(|| self.zero_count_int.map(|v| v as f64))
            .unwrap_or(0.0);

        let positive = if float {
            decode_absolute(&self.positive_spans, self.positive_counts)?
        } else {
            decode_deltas(&self.positive_spans, self.positive_deltas)?
        };
        let negative = if float {
            decode_absolute(&self.negative_spans, self.negative_counts)?
        } else {
            decode_deltas(&self.negative_spans, self.negative_deltas)?
        };

        // Field assignment rather than a struct literal: `Histogram` is
        // `#[non_exhaustive]`, so a new field arriving upstream is a compile
        // error here only if it is *required*, which is the intent.
        let mut h = Histogram::empty(schema);
        h.zero_threshold = self.zero_threshold;
        h.zero_count = zero_count;
        h.count = count;
        h.sum = self.sum;
        h.positive = positive;
        h.negative = negative;
        h.custom_values = self.custom_values.to_vec();
        h.reset_hint = reset_hint_from_proto(self.reset_hint);
        h.canonicalise();
        h.validate()?;
        Ok(h)
    }
}

/// Map the protobuf `ResetHint` discriminant.
///
/// An unknown discriminant is `Unknown`, which is the honest answer: a newer
/// producer telling us something about resets we do not understand leaves us
/// exactly where we would be if it had said nothing.
fn reset_hint_from_proto(v: i32) -> ResetHint {
    match v {
        1 => ResetHint::Reset,
        2 => ResetHint::NoReset,
        3 => ResetHint::Gauge,
        _ => ResetHint::Unknown,
    }
}

/// The protobuf `ResetHint` discriminant for a chronix hint.
fn reset_hint_to_proto(h: ResetHint) -> i32 {
    match h {
        ResetHint::Reset => 1,
        ResetHint::NoReset => 2,
        ResetHint::Gauge => 3,
        _ => 0,
    }
}

/// Walk spans, handing each populated bucket's index to `emit`.
///
/// The index arithmetic is Prometheus's: the first span's offset is the first
/// bucket's index, and every later span's offset is a gap measured from one
/// past the previous span's last bucket. A zero-length span is legal and
/// contributes only its offset — a producer is allowed to split a gap.
fn walk_spans(
    spans: &[(i32, u32)],
    supplied: usize,
    mut emit: impl FnMut(usize, i32) -> Result<(), WireHistogramError>,
) -> Result<(), WireHistogramError> {
    let declared: usize = spans.iter().map(|(_, len)| *len as usize).sum();
    if declared > supplied {
        return Err(WireHistogramError::SpanOverrun { declared, supplied });
    }

    let mut index: i32 = 0;
    let mut k: usize = 0;
    for (n, &(offset, length)) in spans.iter().enumerate() {
        index = if n == 0 {
            offset
        } else {
            index
                .checked_add(offset)
                .ok_or(WireHistogramError::IndexOverflow { offset })?
        };
        for _ in 0..length {
            emit(k, index)?;
            k += 1;
            index = index
                .checked_add(1)
                .ok_or(WireHistogramError::IndexOverflow { offset })?;
        }
    }
    Ok(())
}

/// Spans plus integer deltas → buckets.
fn decode_deltas(spans: &[(i32, u32)], deltas: &[i64]) -> Result<Vec<Bucket>, WireHistogramError> {
    let mut out = Vec::with_capacity(deltas.len());
    let mut running: i64 = 0;
    walk_spans(spans, deltas.len(), |k, index| {
        running = running.saturating_add(deltas[k]);
        if running < 0 {
            return Err(WireHistogramError::NegativeRunningCount { index });
        }
        out.push(Bucket {
            index,
            count: running as f64,
        });
        Ok(())
    })?;
    Ok(out)
}

/// Spans plus absolute float counts → buckets.
fn decode_absolute(
    spans: &[(i32, u32)],
    counts: &[f64],
) -> Result<Vec<Bucket>, WireHistogramError> {
    let mut out = Vec::with_capacity(counts.len());
    walk_spans(spans, counts.len(), |k, index| {
        out.push(Bucket {
            index,
            count: counts[k],
        });
        Ok(())
    })?;
    Ok(out)
}

/// Buckets → spans plus absolute float counts.
///
/// The other direction, for remote read. Chronix stores counts as `f64` and
/// makes no attempt to re-derive whether they were once integers: emitting the
/// float form is always correct, and claiming a histogram is an integer one
/// when a rollup has averaged it would be a lie the reader cannot detect.
///
/// A gap of one empty bucket is cheaper to encode as a zero count inside a
/// span than as a new span (a span costs two varints), which is the rule
/// Prometheus's own encoder uses.
fn encode_spans(buckets: &[Bucket]) -> (Vec<(i32, u32)>, Vec<f64>) {
    let mut spans: Vec<(i32, u32)> = Vec::new();
    let mut counts: Vec<f64> = Vec::new();
    let mut previous: Option<i32> = None;

    for b in buckets {
        match previous {
            // A gap of at most two is filled with zero counts rather than
            // starting a span; beyond that the span pays for itself.
            Some(p) if b.index - p <= 3 && b.index > p => {
                let filled = (b.index - p - 1) as usize;
                counts.extend(std::iter::repeat_n(0.0, filled));
                counts.push(b.count);
                if let Some(last) = spans.last_mut() {
                    last.1 += filled as u32 + 1;
                }
            }
            Some(p) => {
                // `- 1` because a span's offset is measured from one past the
                // previous span's last bucket.
                spans.push((b.index - p - 1, 1));
                counts.push(b.count);
            }
            None => {
                spans.push((b.index, 1));
                counts.push(b.count);
            }
        }
        previous = Some(b.index);
    }
    (spans, counts)
}

/// A chronix histogram in the shape remote read has to send.
///
/// Returned as plain data so the two protobuf versions can each build their
/// own message from it without this module depending on either.
pub(crate) struct EncodedHistogram {
    /// Total observations.
    pub count: f64,
    /// Sum of observations.
    pub sum: f64,
    /// Bucket resolution.
    pub schema: i32,
    /// Half-width of the zero bucket.
    pub zero_threshold: f64,
    /// Zero-bucket count.
    pub zero_count: f64,
    /// Negative-side spans.
    pub negative_spans: Vec<(i32, u32)>,
    /// Negative-side absolute counts.
    pub negative_counts: Vec<f64>,
    /// Positive-side spans.
    pub positive_spans: Vec<(i32, u32)>,
    /// Positive-side absolute counts.
    pub positive_counts: Vec<f64>,
    /// Counter-reset hint discriminant.
    pub reset_hint: i32,
    /// Sample time, milliseconds since epoch.
    pub timestamp: i64,
    /// Custom boundaries, when the schema is `-53`.
    pub custom_values: Vec<f64>,
}

/// Encode a stored histogram for the wire.
pub(crate) fn encode(h: &Histogram, timestamp_ms: i64) -> EncodedHistogram {
    let (positive_spans, positive_counts) = encode_spans(&h.positive);
    let (negative_spans, negative_counts) = encode_spans(&h.negative);
    EncodedHistogram {
        count: h.count,
        sum: h.sum,
        schema: i32::from(h.schema),
        zero_threshold: h.zero_threshold,
        zero_count: h.zero_count,
        negative_spans,
        negative_counts,
        positive_spans,
        positive_counts,
        reset_hint: reset_hint_to_proto(h.reset_hint),
        timestamp: timestamp_ms,
        custom_values: h.custom_values.clone(),
    }
}

/// One OTLP contiguous bucket run.
pub(crate) struct OtlpBuckets<'a> {
    /// Index of the first count.
    pub offset: i32,
    /// Counts, for consecutive indices from `offset`.
    pub counts: &'a [u64],
}

/// An OTLP exponential histogram → chronix's, shifting each index by **+1**.
///
/// See the module documentation for why the shift lives only here.
///
/// `sum` is an `Option` because OTLP leaves it unset for a histogram that
/// recorded negative events, and an unset sum is not a sum of zero. An absent
/// sum becomes `NaN`, which `avg()` already returns for a histogram it cannot
/// average.
pub(crate) fn from_otlp_exponential(
    scale: i32,
    zero_threshold: f64,
    zero_count: u64,
    count: u64,
    sum: Option<f64>,
    positive: Option<OtlpBuckets<'_>>,
    negative: Option<OtlpBuckets<'_>>,
) -> Result<Histogram, WireHistogramError> {
    let schema = i8::try_from(scale).map_err(|_| WireHistogramError::SchemaOutOfRange(scale))?;

    let convert = |b: Option<OtlpBuckets<'_>>| -> Result<Vec<Bucket>, WireHistogramError> {
        let Some(b) = b else { return Ok(Vec::new()) };
        let mut out = Vec::with_capacity(b.counts.len());
        for (i, &c) in b.counts.iter().enumerate() {
            let i = i32::try_from(i)
                .map_err(|_| WireHistogramError::IndexOverflow { offset: b.offset })?;
            let index = b
                .offset
                .checked_add(i)
                .and_then(|n| n.checked_add(1))
                .ok_or(WireHistogramError::IndexOverflow { offset: b.offset })?;
            out.push(Bucket {
                index,
                count: c as f64,
            });
        }
        Ok(out)
    };

    let mut h = Histogram::empty(schema);
    h.zero_threshold = zero_threshold;
    h.zero_count = zero_count as f64;
    h.count = count as f64;
    h.sum = sum.unwrap_or(f64::NAN);
    h.positive = convert(positive)?;
    h.negative = convert(negative)?;
    h.canonicalise();
    h.validate()?;
    Ok(h)
}

/// An OTLP explicit-bucket histogram → a chronix custom-bucket histogram.
///
/// This is the NHCB (native histogram with custom buckets) form: schema `-53`,
/// with the boundaries listed rather than computed. It is the same data the
/// `bucket_<bound>` field expansion carries, in one column instead of one per
/// boundary, and it answers `histogram_quantile` — the field expansion never
/// did, which is how a dashboard full of histogram panels came to render as
/// summaries.
///
/// OTLP sends `bucket_counts` with one more entry than `explicit_bounds`: the
/// last is the `+Inf` overflow. Chronix's index `n` (one past the last
/// boundary) is that same overflow bucket, so the arrays line up without a
/// special case.
pub(crate) fn from_otlp_explicit(
    bounds: &[f64],
    bucket_counts: &[u64],
    count: u64,
    sum: Option<f64>,
) -> Result<Histogram, WireHistogramError> {
    let positive = bucket_counts
        .iter()
        .enumerate()
        .filter(|&(_, &c)| c != 0)
        .map(|(i, &c)| {
            Ok(Bucket {
                index: i32::try_from(i)
                    .map_err(|_| WireHistogramError::IndexOverflow { offset: 0 })?,
                count: c as f64,
            })
        })
        .collect::<Result<Vec<_>, WireHistogramError>>()?;

    let mut h = Histogram::empty(CUSTOM_BUCKETS_SCHEMA);
    h.count = count as f64;
    h.sum = sum.unwrap_or(f64::NAN);
    h.positive = positive;
    h.custom_values = bounds.to_vec();
    h.canonicalise();
    h.validate()?;
    Ok(h)
}

#[cfg(test)]
mod tests;
