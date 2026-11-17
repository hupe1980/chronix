//! Extended range selectors: `anchored` and `smoothed`.
//!
//! A port of Prometheus 3.16's `extendedRate`, `extendFloats` and the
//! smoothed instant selector (`promql/functions.go`, `promql/engine.go`) for
//! float samples. chronix carries no start timestamps in this path, and with
//! every start timestamp unset upstream's start-timestamp branches never fire,
//! so they are left out rather than ported as dead code.
//!
//! Every function takes samples sorted by timestamp and timestamps in
//! nanoseconds. The windows are upstream's: a selector with a modifier reads
//! `(range_start − lookback, range_end]` (`anchored`) or
//! `(range_start − lookback, range_end + lookback]` (`smoothed`).

use crate::promql::ast::Sample;

/// Linear interpolation between two samples at `t`.
///
/// For a counter that reset between them, the counter is modelled as
/// restarting from zero.
fn interpolate(p1: Sample, p2: Sample, t: i64, is_counter: bool) -> f64 {
    let y1 = if is_counter && p2.value < p1.value {
        0.0
    } else {
        p1.value
    };
    let y2 = p2.value;
    y1 + (y2 - y1) * (t - p1.timestamp) as f64 / (p2.timestamp - p1.timestamp) as f64
}

/// The first index in `[0, n)` for which `pred` holds, or `n` — Go's `sort.Search`.
fn search(n: usize, pred: impl Fn(usize) -> bool) -> usize {
    (0..n).find(|&i| pred(i)).unwrap_or(n)
}

/// The value at the left boundary. `first` is the newest sample at or before
/// `range_start`, or the first sample when none is.
fn pick_left(
    f: &[Sample],
    first: usize,
    range_start: i64,
    smoothed: bool,
    is_counter: bool,
) -> f64 {
    let first_dp = f[first];
    if first_dp.timestamp >= range_start {
        return first_dp.value;
    }
    if smoothed {
        return interpolate(first_dp, f[first + 1], range_start, is_counter);
    }
    first_dp.value
}

/// The value at the right boundary. `last` is the first sample at or after
/// `range_end` (smoothed) or the last sample in the window.
fn pick_right(f: &[Sample], last: usize, range_end: i64, smoothed: bool, is_counter: bool) -> f64 {
    let last_dp = f[last];
    if last_dp.timestamp <= range_end || last == 0 {
        return last_dp.value;
    }
    if smoothed {
        return interpolate(f[last - 1], last_dp, range_end, is_counter);
    }
    last_dp.value
}

/// Upstream's `correctForCounterResets`: the sum of the values before every
/// decrease, from `left` through `points` to `right`.
fn counter_reset_correction(left: f64, right: f64, points: &[Sample]) -> f64 {
    let mut correction = 0.0;
    let mut prev = left;
    for p in points {
        if p.value < prev {
            correction += prev;
        }
        prev = p.value;
    }
    if right < prev {
        correction += prev;
    }
    correction
}

/// `rate`, `increase` or `delta` over an anchored or smoothed range.
///
/// `None` when the window holds nothing to measure — no sample after the
/// range start, or (smoothed) no sample before the range end.
#[must_use]
pub(crate) fn extended_rate(
    f: &[Sample],
    range_start: i64,
    range_end: i64,
    range_secs: f64,
    smoothed: bool,
    is_counter: bool,
    is_rate: bool,
) -> Option<f64> {
    if f.is_empty() {
        return None;
    }
    let mut last = f.len() - 1;
    let mut first = search(last, |i| f[i].timestamp > range_start).saturating_sub(1);
    if smoothed {
        last = search(last, |i| f[i].timestamp >= range_end);
    }
    if f[last].timestamp <= range_start {
        return None;
    }
    if smoothed && f[first].timestamp > range_end {
        return None;
    }

    let left = pick_left(f, first, range_start, smoothed, is_counter);
    let right = pick_right(f, last, range_end, smoothed, is_counter);
    let mut result = right - left;

    if is_counter {
        // Only samples strictly inside the range: the boundaries already
        // account for resets at the edges.
        if f[first].timestamp <= range_start {
            first += 1;
        }
        if f[last].timestamp >= range_end {
            last = last.wrapping_sub(1);
        }
        let inner = if last != usize::MAX && first <= last {
            &f[first..=last]
        } else {
            &[][..]
        };
        result += counter_reset_correction(left, right, inner);
    }
    if is_rate {
        result /= range_secs;
    }
    Some(result)
}

/// Upstream's `extendFloats`: a bare `m[5m] anchored` or `m[5m] smoothed`.
///
/// Returns the samples strictly inside the range framed by synthetic
/// boundary samples at `mint` and `maxt`, or nothing when the window does not
/// reach past `mint`.
#[must_use]
pub(crate) fn extend_floats(f: &[Sample], mint: i64, maxt: i64, smoothed: bool) -> Vec<Sample> {
    if f.is_empty() {
        return Vec::new();
    }
    let mut last = f.len() - 1;
    let mut first = search(last, |i| f[i].timestamp > mint).saturating_sub(1);
    if smoothed {
        last = search(last, |i| f[i].timestamp >= maxt);
    }
    if f[last].timestamp <= mint {
        return Vec::new();
    }
    let left = pick_left(f, first, mint, smoothed, false);
    let right = pick_right(f, last, maxt, smoothed, false);

    if f[first].timestamp <= mint {
        first += 1;
    }
    if f[last].timestamp >= maxt {
        last = last.wrapping_sub(1);
    }
    let mut out = Vec::with_capacity(f.len() + 2);
    out.push(Sample {
        timestamp: mint,
        value: left,
    });
    if last != usize::MAX && first <= last {
        out.extend_from_slice(&f[first..=last]);
    }
    out.push(Sample {
        timestamp: maxt,
        value: right,
    });
    out
}

/// Where an anchored `resets` or `changes` starts: the newest sample at or
/// before `range_start` (the anchor), or the first sample when there is none.
///
/// `None` when no sample lies strictly after `range_start` — there is nothing
/// to measure.
#[must_use]
pub(crate) fn anchored_first(f: &[Sample], range_start: i64) -> Option<usize> {
    let after = f.iter().position(|s| s.timestamp > range_start)?;
    Some(after.saturating_sub(1))
}

/// A smoothed instant selector: the value at `t`, interpolated between the
/// samples either side, the sample itself on an exact match, or the previous
/// sample carried forward when nothing follows.
#[must_use]
pub(crate) fn smoothed_at(f: &[Sample], t: i64) -> Option<f64> {
    let i = f.iter().position(|s| s.timestamp >= t).unwrap_or(f.len());
    match (f.get(i), i) {
        (Some(s), _) if s.timestamp == t => Some(s.value),
        (Some(&next), i) if i > 0 => Some(interpolate(f[i - 1], next, t, false)),
        (None, i) if i > 0 => Some(f[i - 1].value),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: i64 = 1_000_000_000;

    fn series(points: &[(i64, f64)]) -> Vec<Sample> {
        points
            .iter()
            .map(|&(t, v)| Sample {
                timestamp: t * S,
                value: v,
            })
            .collect()
    }

    /// Upstream's "interval is left-open" case: samples at 0, 1, 3, 4 min;
    /// `increase(m[1m] smoothed)` at 2 min interpolates the right boundary.
    #[test]
    fn smoothed_increase_interpolates_the_right_boundary() {
        let f = series(&[(0, 1.0), (60, 2.0), (180, 4.0), (240, 5.0)]);
        let v = extended_rate(&f, 60 * S, 120 * S, 60.0, true, true, false);
        assert_eq!(v, Some(1.0));
    }

    #[test]
    fn anchored_increase_needs_a_sample_after_the_range_start() {
        let f = series(&[(0, 1.0), (60, 2.0)]);
        assert_eq!(
            extended_rate(&f, 60 * S, 120 * S, 60.0, false, true, false),
            None
        );
    }

    #[test]
    fn anchored_increase_corrects_a_reset_inside_the_range() {
        // 5, 10, 3 at 0, 30, 60 s; anchored over (0, 60]: 3 − 5 + 10.
        let f = series(&[(0, 5.0), (30, 10.0), (60, 3.0)]);
        assert_eq!(
            extended_rate(&f, 0, 60 * S, 60.0, false, true, false),
            Some(8.0)
        );
    }

    #[test]
    fn extend_floats_frames_the_window_with_its_boundaries() {
        let f = series(&[(0, 1.0), (60, 2.0), (120, 3.0), (180, 4.0)]);
        let out = extend_floats(&f, 60 * S, 180 * S, false);
        let got: Vec<(i64, f64)> = out.iter().map(|s| (s.timestamp / S, s.value)).collect();
        assert_eq!(got, vec![(60, 2.0), (120, 3.0), (180, 4.0)]);
    }

    #[test]
    fn smoothed_at_interpolates_and_carries_forward() {
        let f = series(&[(0, 1.0), (10, 2.0)]);
        assert_eq!(smoothed_at(&f, 5 * S), Some(1.5));
        assert_eq!(smoothed_at(&f, 10 * S), Some(2.0));
        assert_eq!(smoothed_at(&f, 20 * S), Some(2.0));
        assert_eq!(smoothed_at(&f, -S), None);
    }
}
