//! A seasonal baseline whose whole state is a few numbers per slot.
//!
//! This is the state behind a *declared* detector: a monitor the store keeps
//! and advances across restarts, rather than an object a caller drives. Its
//! requirement is the opposite of the detectors elsewhere in this module —
//! not "fit on a window, then score", but "score one point, fold it in, and
//! be serialisable at any moment" — so it is a small value type with
//! `serde`, not an [`AnomalyDetector`](super::AnomalyDetector).
//!
//! Two pieces:
//!
//! - [`Welford`] — a running mean and variance (Welford's algorithm), with
//!   optional **exponential forgetting** so a baseline follows slow,
//!   legitimate change (a roof that is a year older) without a window of
//!   samples. One per slot of the season.
//! - [`Cusum`] — a two-sided tabular CUSUM on standardised residuals, the
//!   textbook detector for a *persistent* shift: a roof that delivers 15 %
//!   less every day trips it within days, while one cloudy day does not.
//!
//! The scoring rule a caller applies is in [`SeasonalBaseline::score`]: a
//! point is standardised against its own slot, an outlier is not folded into
//! the baseline (one fault must not widen the band that should catch the
//! next), and nothing is scored until the slot has seen `min_samples`.

use serde::{Deserialize, Serialize};

use super::scale_floor;

/// A running mean and variance with optional exponential forgetting.
///
/// `weight` is the effective sample count. Without forgetting it is the
/// number of samples folded in; with forgetting, every update first decays
/// the existing weight and second moment by the same factor, which makes the
/// mean and variance exponentially weighted — the standard incremental form.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Welford {
    /// Effective number of samples.
    pub weight: f64,
    /// Running mean.
    pub mean: f64,
    /// Running sum of squared deviations from the mean.
    pub m2: f64,
}

impl Welford {
    /// Fold one value in, after decaying the existing state by `decay`
    /// (`1.0` = no forgetting).
    pub fn update(&mut self, x: f64, decay: f64) {
        self.weight = self.weight * decay + 1.0;
        self.m2 *= decay;
        let delta = x - self.mean;
        self.mean += delta / self.weight;
        self.m2 += delta * (x - self.mean);
    }

    /// Sample variance (`m2 / (weight − 1)`), or `0` below two samples.
    #[must_use]
    pub fn variance(&self) -> f64 {
        if self.weight > 1.0 {
            (self.m2 / (self.weight - 1.0)).max(0.0)
        } else {
            0.0
        }
    }

    /// Sample standard deviation.
    #[must_use]
    pub fn stddev(&self) -> f64 {
        self.variance().sqrt()
    }
}

/// Which side of a two-sided CUSUM raised an alarm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CusumAlarm {
    /// A sustained shift upwards.
    High,
    /// A sustained shift downwards.
    Low,
}

/// A two-sided tabular CUSUM on standardised residuals.
///
/// `k` is the allowance (half the shift to detect, in standard deviations)
/// and `h` the decision interval. The side that alarms is reset, which is
/// the standard restart after a signal.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Cusum {
    /// Upper cumulative sum.
    pub pos: f64,
    /// Lower cumulative sum.
    pub neg: f64,
}

impl Cusum {
    /// Fold one standardised residual `z` in; returns the alarm, if any.
    pub fn step(&mut self, z: f64, k: f64, h: f64) -> Option<CusumAlarm> {
        self.pos = (self.pos + z - k).max(0.0);
        self.neg = (self.neg - z - k).max(0.0);
        if self.pos > h {
            self.pos = 0.0;
            Some(CusumAlarm::High)
        } else if self.neg > h {
            self.neg = 0.0;
            Some(CusumAlarm::Low)
        } else {
            None
        }
    }
}

/// How a declared detector scores a point against its slot.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoringRule {
    /// Flag a point whose |z| exceeds this. `None` disables the z-score test
    /// (outliers are then never excluded from the baseline).
    pub zscore: Option<f64>,
    /// CUSUM `(k, h)` in units of the slot's standard deviation.
    pub cusum: Option<(f64, f64)>,
    /// Effective samples a slot needs before anything in it is scored.
    pub min_samples: f64,
}

/// The outcome of scoring one point.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Verdict {
    /// The slot's mean before the point was folded in.
    pub baseline: f64,
    /// The slot's standard deviation before the point was folded in.
    pub stddev: f64,
    /// The standardised residual.
    pub z: f64,
    /// The CUSUM sums after this point.
    pub cusum: Cusum,
    /// Whether the z-score test flagged the point.
    pub zscore_flag: bool,
    /// Whether, and which way, the CUSUM alarmed.
    pub cusum_alarm: Option<CusumAlarm>,
}

impl Verdict {
    /// Whether anything flagged this point.
    #[must_use]
    pub fn is_anomaly(&self) -> bool {
        self.zscore_flag || self.cusum_alarm.is_some()
    }

    /// A short machine-readable reason: `"zscore"`, `"cusum_low"`,
    /// `"cusum_high"`, or `""`.
    #[must_use]
    pub fn reason(&self) -> &'static str {
        match (self.zscore_flag, self.cusum_alarm) {
            (true, _) => "zscore",
            (false, Some(CusumAlarm::Low)) => "cusum_low",
            (false, Some(CusumAlarm::High)) => "cusum_high",
            (false, None) => "",
        }
    }
}

/// One series' seasonal baseline: a [`Welford`] per slot and one [`Cusum`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SeasonalBaseline {
    /// One running estimate per slot of the season, grown on demand.
    pub slots: Vec<Welford>,
    /// The period each slot was last updated in, for forgetting.
    pub last_period: Vec<i64>,
    /// The CUSUM accumulator, shared across slots: it sums *standardised*
    /// residuals, which are comparable whichever slot produced them.
    pub cusum: Cusum,
}

impl SeasonalBaseline {
    /// Score `x` in `slot` during `period`, then fold it into the baseline.
    ///
    /// `half_life` is in periods: the weight of what a slot learned halves
    /// every `half_life` periods it is revisited. Returns `None` while the
    /// slot is warming up — fewer than `rule.min_samples` effective samples —
    /// in which case the point is still folded in.
    pub fn score(
        &mut self,
        slot: usize,
        period: i64,
        x: f64,
        rule: &ScoringRule,
        half_life: Option<u32>,
    ) -> Option<Verdict> {
        if self.slots.len() <= slot {
            self.slots.resize(slot + 1, Welford::default());
            self.last_period.resize(slot + 1, i64::MIN);
        }
        let decay = match (half_life, self.last_period[slot]) {
            (Some(h), last) if h > 0 && last != i64::MIN && period > last => {
                #[allow(clippy::cast_precision_loss)]
                let elapsed = (period - last) as f64;
                0.5_f64.powf(elapsed / f64::from(h))
            }
            _ => 1.0,
        };
        self.last_period[slot] = period;
        let state = self.slots[slot];

        if state.weight < rule.min_samples {
            self.slots[slot].update(x, decay);
            return None;
        }

        let baseline = state.mean;
        let stddev = state.stddev();
        let z = (x - baseline) / scale_floor(stddev, baseline);
        let zscore_flag = rule.zscore.is_some_and(|t| z.abs() > t);
        let cusum_alarm = rule.cusum.and_then(|(k, h)| self.cusum.step(z, k, h));

        // An outlier does not move its slot: one fault must not widen the
        // band that should catch the next one.
        if !zscore_flag {
            self.slots[slot].update(x, decay);
        }

        Some(Verdict {
            baseline,
            stddev,
            z,
            cusum: self.cusum,
            zscore_flag,
            cusum_alarm,
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::float_cmp)]

    use super::*;

    fn rule() -> ScoringRule {
        ScoringRule {
            zscore: Some(3.0),
            cusum: Some((0.5, 5.0)),
            min_samples: 3.0,
        }
    }

    #[test]
    fn welford_matches_a_two_pass_computation() {
        let xs = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0, 1.5, 3.25];
        let mut w = Welford::default();
        for &x in &xs {
            w.update(x, 1.0);
        }
        let n = xs.len() as f64;
        let mean = xs.iter().sum::<f64>() / n;
        let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
        assert!((w.mean - mean).abs() < 1e-12);
        assert!((w.variance() - var).abs() < 1e-12);
        assert_eq!(w.weight, n);
    }

    #[test]
    fn forgetting_halves_the_weight_after_one_half_life() {
        let mut b = SeasonalBaseline::default();
        let r = ScoringRule {
            zscore: None,
            cusum: None,
            min_samples: 1e9,
        };
        // Ten samples in period 0, then one in period 4 with a half-life of 4.
        for _ in 0..10 {
            b.score(0, 0, 1.0, &r, Some(4));
        }
        assert_eq!(b.slots[0].weight, 10.0);
        b.score(0, 4, 1.0, &r, Some(4));
        assert!((b.slots[0].weight - (10.0 * 0.5 + 1.0)).abs() < 1e-12);
    }

    #[test]
    fn nothing_is_scored_while_a_slot_warms_up() {
        let mut b = SeasonalBaseline::default();
        assert!(b.score(0, 0, 10.0, &rule(), None).is_none());
        assert!(b.score(0, 1, 11.0, &rule(), None).is_none());
        assert!(b.score(0, 2, 9.0, &rule(), None).is_none());
        assert!(b.score(0, 3, 10.0, &rule(), None).is_some());
    }

    #[test]
    fn an_outlier_does_not_move_its_slot() {
        let mut b = SeasonalBaseline::default();
        for (p, x) in [10.0, 11.0, 9.0, 10.5, 9.5].into_iter().enumerate() {
            b.score(0, p as i64, x, &rule(), None);
        }
        let before = b.slots[0];
        let v = b.score(0, 9, 1_000.0, &rule(), None).unwrap();
        assert!(v.zscore_flag);
        assert_eq!(v.reason(), "zscore");
        assert_eq!(
            b.slots[0], before,
            "the outlier was folded into the baseline"
        );
    }

    #[test]
    fn cusum_catches_a_sustained_one_sigma_drop_and_resets() {
        // k = 0.5, h = 5 on a 1σ shift: the textbook ARL₁ is ~10 samples.
        let mut c = Cusum::default();
        let mut alarm_at = None;
        for i in 1..=40 {
            if let Some(a) = c.step(-1.0, 0.5, 5.0) {
                assert_eq!(a, CusumAlarm::Low);
                alarm_at = Some(i);
                break;
            }
        }
        let n = alarm_at.expect("a sustained 1σ shift must alarm");
        assert!((8..=14).contains(&n), "alarmed after {n} samples");
        assert_eq!(c.neg, 0.0, "the alarming side resets");
        // In control, it stays quiet.
        let mut quiet = Cusum::default();
        for i in 0..1_000 {
            let z = if i % 2 == 0 { 0.4 } else { -0.4 };
            assert!(quiet.step(z, 0.5, 5.0).is_none());
        }
    }

    #[test]
    fn slots_are_independent() {
        let mut b = SeasonalBaseline::default();
        // Slot 0 lives near 0, slot 1 near 100.
        for p in 0..6 {
            b.score(0, p, 0.0 + (p as f64) * 0.1, &rule(), None);
            b.score(1, p, 100.0 + (p as f64) * 0.1, &rule(), None);
        }
        let v = b.score(1, 7, 100.2, &rule(), None).unwrap();
        assert!(!v.is_anomaly(), "100 is normal for slot 1: {v:?}");
        let v = b.score(0, 7, 100.2, &rule(), None).unwrap();
        assert!(v.zscore_flag, "100 is not normal for slot 0");
    }

    #[test]
    fn postcard_round_trip_is_lossless() {
        let mut b = SeasonalBaseline::default();
        for p in 0..5 {
            b.score(2, p, 3.0 + p as f64, &rule(), Some(3));
        }
        let bytes = postcard::to_stdvec(&b).unwrap();
        let back: SeasonalBaseline = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back, b);
    }
}
