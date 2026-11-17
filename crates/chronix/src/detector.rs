//! Declared detectors: anomaly monitors the store keeps.
//!
//! A [`create_detector`](crate::analytics) object is something a caller
//! drives — it owns the state, the schedule and the persistence, which is
//! exactly the hard part on a box that reboots. A *declared* detector is the
//! other shape, the one a rollup has: declared once on a source measurement,
//! persisted in the catalog, advanced by the maintenance pass, and resumed —
//! not relearned — after a restart.
//!
//! ```no_run
//! # #[cfg(feature = "analytics")] {
//! use chronix::detector::{DetectorDeclaration, Season};
//! use chronix_core::timebucket::TimeBucket;
//! # fn demo(db: &chronix::Chronix) -> chronix::error::Result<()> {
//! let declaration = DetectorDeclaration::builder("roof_health")
//!     .source("pv_ratio", "value")
//!     .season(Season::new(
//!         TimeBucket::parse("1mo", Some("Europe/Berlin")).unwrap(),
//!         TimeBucket::parse("1y", Some("Europe/Berlin")).unwrap(),
//!     )?)
//!     .zscore(3.0)
//!     .cusum(0.5, 5.0)
//!     .build()?;
//! db.declare_detector(declaration)?; // idempotent: declare it on every start
//! # Ok(()) }
//! # }
//! ```
//!
//! Each point after a slot's warm-up produces one **verdict** row in the
//! output measurement (`<name>_verdicts` by default), tagged with the source
//! series' tags and `detector=<name>`, with the fields `value`, `baseline`,
//! `stddev`, `z`, `cusum_pos`, `cusum_neg`, `anomaly` and `reason`. Verdicts
//! are ordinary points: query them, subscribe to them, alert on them.

use std::time::Duration;

use chronix_core::timebucket::TimeBucket;
use serde::{Deserialize, Serialize};

use crate::error::{DbError, Result};

/// The most slots one season may have.
pub const MAX_SLOTS_PER_PERIOD: usize = 2_048;

/// The calendar a baseline is learned against: `slot`-wide buckets within a
/// `period`.
///
/// Fifteen-minute slots in a Europe/Berlin day give each quarter hour its own
/// normal; monthly slots in a year give each month its own. Both are
/// [`TimeBucket`]s, so a 23- or 25-hour day and a 28-day February are read
/// against the calendar rather than divided out of a nanosecond count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Season {
    slot: TimeBucket,
    period: TimeBucket,
}

/// Instants the slot tiling is checked at: an ordinary day, both European
/// transition days of 2026, and midsummer — every shape a day can have.
const TILING_PROBES: [i64; 4] = [
    1_768_478_400_000_000_000,
    1_774_785_600_000_000_000,
    1_781_524_800_000_000_000,
    1_792_929_600_000_000_000,
];

impl Season {
    /// A season of `slot`-wide slots within each `period`.
    ///
    /// # Errors
    ///
    /// [`DbError::InvalidRequest`] when the slots do not tile the period —
    /// a period boundary that is not a slot boundary, so a slot would
    /// straddle two periods — or when a period holds more than
    /// [`MAX_SLOTS_PER_PERIOD`] slots.
    pub fn new(slot: TimeBucket, period: TimeBucket) -> Result<Self> {
        let season = Self { slot, period };
        for probe in TILING_PROBES {
            let start = period.start_of(probe);
            let end = period.next(start);
            if slot.start_of(start) != start {
                return Err(DbError::InvalidRequest(format!(
                    "a {slot} slot does not start where a {period} period does; \
                     choose a slot that divides the period (15m in a 1d, 1mo in a 1y) \
                     and the same time zone for both"
                )));
            }
            let mut s = start;
            let mut n = 0usize;
            while s < end {
                s = slot.next(s);
                n += 1;
                if n > MAX_SLOTS_PER_PERIOD {
                    return Err(DbError::InvalidRequest(format!(
                        "a {period} period holds more than {MAX_SLOTS_PER_PERIOD} {slot} \
                         slots; choose a wider slot"
                    )));
                }
            }
            if s != end {
                return Err(DbError::InvalidRequest(format!(
                    "{slot} slots do not tile a {period} period exactly; \
                     choose a slot that divides it"
                )));
            }
        }
        Ok(season)
    }

    /// The slot width.
    #[must_use]
    pub fn slot(&self) -> TimeBucket {
        self.slot
    }

    /// The period width.
    #[must_use]
    pub fn period(&self) -> TimeBucket {
        self.period
    }

    /// `(slot index, period start)` of `ts`.
    ///
    /// Counted by stepping the slot calendar from the period's start, so the
    /// 02:00 slot of a 23-hour day is index 8 in quarter hours and the 03:00
    /// one is index 8 too — the hour that does not exist has no slot.
    #[must_use]
    pub fn slot_of(&self, ts: i64) -> (usize, i64) {
        let period_start = self.period.start_of(ts);
        let slot_start = self.slot.start_of(ts).max(period_start);
        let mut s = period_start;
        let mut i = 0usize;
        while s < slot_start && i < MAX_SLOTS_PER_PERIOD {
            s = self.slot.next(s);
            i += 1;
        }
        (i, period_start)
    }
}

/// A declared detector's definition.
///
/// Built with [`DetectorDeclaration::builder`]; persisted in the catalog as
/// written, so a field added later is a new declaration kind's business and
/// never reinterprets this one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DetectorDeclaration {
    /// The detector's name, unique among detectors.
    pub name: String,
    /// The measurement it watches.
    pub source_measurement: String,
    /// The numeric field it scores.
    pub field: String,
    /// Only rows whose tags equal these are judged — one site's meters, or
    /// one tenant's namespace. Empty = every row of the source.
    pub where_tags: Vec<(String, String)>,
    /// The tags one baseline is kept per combination of. `None` = every tag,
    /// which is one baseline per series.
    pub group_by_tags: Option<Vec<String>>,
    /// The calendar the baseline is learned against; `None` = one slot.
    pub season: Option<Season>,
    /// Flag a point whose |z| against its slot exceeds this.
    pub zscore: Option<f64>,
    /// CUSUM `(k, h)` on the slot-standardised residual.
    pub cusum: Option<(f64, f64)>,
    /// Samples a slot needs before anything in it is scored.
    pub min_samples: u32,
    /// Half-life of what a slot has learned, in periods; `None` = never
    /// forget.
    pub forgetting: Option<u32>,
    /// Where verdicts are written.
    pub output_measurement: String,
    /// How far behind the wall clock the detector judges, in nanoseconds —
    /// so ordinary delivery jitter is not "late". `None` = twice the
    /// database's maintenance interval.
    pub settle_ns: Option<i64>,
    /// The most source points one maintenance pass consumes.
    pub max_points_per_pass: u32,
}

impl DetectorDeclaration {
    /// Start a declaration named `name`.
    pub fn builder(name: &str) -> DetectorBuilder {
        DetectorBuilder {
            name: name.to_string(),
            source: None,
            where_tags: Vec::new(),
            group_by_tags: None,
            season: None,
            zscore: None,
            cusum: None,
            min_samples: 3,
            forgetting: None,
            output: None,
            settle: None,
            max_points_per_pass: 100_000,
        }
    }

    /// Check every rule a declaration must satisfy on its own.
    ///
    /// # Errors
    ///
    /// [`DbError::InvalidRequest`] naming the rule and what to write instead.
    pub fn validate(&self) -> Result<()> {
        let bad = |m: String| Err(DbError::InvalidRequest(m));
        // `namespace/name` is how a server keeps tenants' declarations apart,
        // as it does for rollups; the part after the last `/` is the name.
        let local = self.name.rsplit('/').next().unwrap_or_default();
        if local.is_empty()
            || !local
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        {
            return bad(format!(
                "detector name {:?} must be lower-case letters, digits and '_'",
                self.name
            ));
        }
        if self.source_measurement.is_empty() || self.field.is_empty() {
            return bad("a detector needs a source: .source(\"measurement\", \"field\")".into());
        }
        if self.zscore.is_none() && self.cusum.is_none() {
            return bad(
                "a detector needs a test: .zscore(threshold), .cusum(k, h), or both".into(),
            );
        }
        if let Some(t) = self.zscore
            && !(t.is_finite() && t > 0.0)
        {
            return bad(format!("zscore threshold must be positive, got {t}"));
        }
        if let Some((k, h)) = self.cusum
            && !(k.is_finite() && k >= 0.0 && h.is_finite() && h > 0.0)
        {
            return bad(format!("cusum needs k ≥ 0 and h > 0, got k={k}, h={h}"));
        }
        if self.min_samples == 0 {
            return bad("min_samples must be at least 1".into());
        }
        if self.forgetting == Some(0) {
            return bad("forgetting is a half-life in periods and must be at least 1".into());
        }
        if self.forgetting.is_some() && self.season.is_none() {
            return bad(
                "forgetting is measured in periods, so it needs a .season(…) to count them".into(),
            );
        }
        if self.output_measurement == self.source_measurement {
            return bad("a detector cannot write its verdicts into its own source".into());
        }
        if self.settle_ns.is_some_and(|n| n < 0) {
            return bad("settle must not be negative".into());
        }
        if self.max_points_per_pass == 0 {
            return bad("max_points_per_pass must be at least 1".into());
        }
        Ok(())
    }
}

/// Builder for a [`DetectorDeclaration`].
#[derive(Debug, Clone)]
#[must_use]
pub struct DetectorBuilder {
    name: String,
    source: Option<(String, String)>,
    where_tags: Vec<(String, String)>,
    group_by_tags: Option<Vec<String>>,
    season: Option<Season>,
    zscore: Option<f64>,
    cusum: Option<(f64, f64)>,
    min_samples: u32,
    forgetting: Option<u32>,
    output: Option<String>,
    settle: Option<Duration>,
    max_points_per_pass: u32,
}

impl DetectorBuilder {
    /// The measurement and numeric field to watch.
    pub fn source(mut self, measurement: &str, field: &str) -> Self {
        self.source = Some((measurement.to_string(), field.to_string()));
        self
    }

    /// Judge only rows whose tag `key` equals `value`. Repeatable; every
    /// condition must hold.
    pub fn where_tag(mut self, key: &str, value: &str) -> Self {
        self.where_tags.push((key.to_string(), value.to_string()));
        self
    }

    /// Keep one baseline per combination of these tags instead of one per
    /// series.
    pub fn group_by<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.group_by_tags = Some(tags.into_iter().map(Into::into).collect());
        self
    }

    /// Learn the baseline per slot of a season.
    pub fn season(mut self, season: Season) -> Self {
        self.season = Some(season);
        self
    }

    /// Flag a point more than `threshold` standard deviations from its
    /// slot's mean.
    pub fn zscore(mut self, threshold: f64) -> Self {
        self.zscore = Some(threshold);
        self
    }

    /// Flag a persistent shift: a two-sided CUSUM with allowance `k` and
    /// decision interval `h`, in standard deviations. `(0.5, 5.0)` catches a
    /// sustained one-sigma shift in about ten samples.
    pub fn cusum(mut self, k: f64, h: f64) -> Self {
        self.cusum = Some((k, h));
        self
    }

    /// Samples a slot needs before it is scored (default 3).
    pub fn min_samples(mut self, n: u32) -> Self {
        self.min_samples = n;
        self
    }

    /// Let the baseline follow slow change: what a slot learned halves in
    /// weight every `half_life_periods` periods.
    pub fn forgetting(mut self, half_life_periods: u32) -> Self {
        self.forgetting = Some(half_life_periods);
        self
    }

    /// Write verdicts here instead of `<name>_verdicts`.
    pub fn output(mut self, measurement: &str) -> Self {
        self.output = Some(measurement.to_string());
        self
    }

    /// Judge this far behind the wall clock (default: twice the maintenance
    /// interval, decided when the declaration is made).
    pub fn settle(mut self, settle: Duration) -> Self {
        self.settle = Some(settle);
        self
    }

    /// Cap the source points one maintenance pass consumes (default 100 000),
    /// so a week offline catches up over several passes rather than in one
    /// long one.
    pub fn max_points_per_pass(mut self, n: u32) -> Self {
        self.max_points_per_pass = n;
        self
    }

    /// Validate and build.
    ///
    /// # Errors
    ///
    /// [`DbError::InvalidRequest`] naming the rule that failed.
    pub fn build(self) -> Result<DetectorDeclaration> {
        let (source_measurement, field) = self.source.unwrap_or_default();
        let output_measurement = self.output.unwrap_or_else(|| {
            let local = self.name.rsplit('/').next().unwrap_or_default();
            format!("{local}_verdicts")
        });
        let settle_ns = self
            .settle
            .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX));
        let decl = DetectorDeclaration {
            name: self.name,
            source_measurement,
            field,
            where_tags: self.where_tags,
            group_by_tags: self.group_by_tags,
            season: self.season,
            zscore: self.zscore,
            cusum: self.cusum,
            min_samples: self.min_samples,
            forgetting: self.forgetting,
            output_measurement,
            settle_ns,
            max_points_per_pass: self.max_points_per_pass,
        };
        decl.validate()?;
        Ok(decl)
    }
}

/// Whether a declared detector is running.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum DetectorStatus {
    /// Advanced by every maintenance pass.
    Active,
    /// Kept, and not advanced — the reason says why.
    Inactive {
        /// Why it is not running.
        reason: String,
    },
    /// Running for some series and not others — the state budget is spent.
    Degraded {
        /// What was not admitted.
        reason: String,
    },
}

/// A declared detector as the store reports it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DetectorInfo {
    /// The definition, as declared.
    pub declaration: DetectorDeclaration,
    /// The newest source timestamp judged, nanoseconds; `None` before the
    /// first pass.
    pub watermark_ns: Option<i64>,
    /// Series (or tag groups) with a baseline.
    pub series: usize,
    /// Points that arrived at or below the watermark and were not judged.
    pub late_points: u64,
    /// Whether it is running.
    pub status: DetectorStatus,
}

/// A declared detector's persisted state.
///
/// **Field order is load-bearing.** postcard is not self-describing, so a
/// build without the `analytics` feature — which has no type for a baseline
/// — reads the leading fields as [`DetectorStateHead`] and never decodes, nor
/// rewrites, the rest. The declaration and its state survive such a build
/// byte for byte.
#[cfg(feature = "analytics")]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct DetectorState {
    pub(crate) watermark_ns: Option<i64>,
    pub(crate) late_points: u64,
    pub(crate) degraded: Option<String>,
    pub(crate) series_count: u64,
    /// Keyed by the group's canonical tag string — exact, so two groups can
    /// never share a baseline the way two hashes can.
    pub(crate) series:
        std::collections::BTreeMap<String, chronix_analytics::anomaly::SeasonalBaseline>,
}

/// The leading fields of a persisted [`DetectorState`], readable in every
/// build.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub(crate) struct DetectorStateHead {
    pub(crate) watermark_ns: Option<i64>,
    pub(crate) late_points: u64,
    pub(crate) degraded: Option<String>,
    pub(crate) series_count: u64,
}

impl DetectorStateHead {
    /// Read the head of a persisted state; an empty state is the default.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Ok(Self::default());
        }
        postcard::take_from_bytes::<Self>(bytes)
            .map(|(head, _rest)| head)
            .map_err(|e| DbError::Internal(format!("unreadable detector state: {e}")))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn berlin(w: &str) -> TimeBucket {
        TimeBucket::parse(w, Some("Europe/Berlin")).unwrap()
    }

    /// Every quarter hour of a local day has its own slot, including the
    /// 23-hour day in March and the 25-hour day in October.
    #[test]
    fn quarter_hours_of_a_berlin_day_on_both_transitions() {
        let season = Season::new(berlin("15m"), berlin("1d")).unwrap();
        // 2026-03-29 00:00 Berlin (CET, +01:00) = 2026-03-28T23:00Z.
        let march = 1_774_738_800_000_000_000_i64;
        // 2026-10-25 00:00 Berlin (CEST, +02:00) = 2026-10-24T22:00Z.
        let october = 1_792_879_200_000_000_000_i64;
        for (day_start, quarters) in [(march, 92usize), (october, 100usize)] {
            let mut seen = Vec::new();
            for q in 0..quarters {
                let ts = day_start + i64::try_from(q).unwrap() * 900_000_000_000 + 1;
                let (slot, period) = season.slot_of(ts);
                assert_eq!(period, day_start, "quarter {q} left its day");
                seen.push(slot);
            }
            let mut sorted = seen.clone();
            sorted.dedup();
            assert_eq!(
                sorted.len(),
                quarters,
                "two quarters shared a slot: {seen:?}"
            );
            assert!(
                seen.windows(2).all(|w| w[0] < w[1]),
                "slots out of order: {seen:?}"
            );
        }
    }

    #[test]
    fn months_of_a_year() {
        let season = Season::new(berlin("1mo"), berlin("1y")).unwrap();
        // 2026-06-15 12:00Z is in June: slot 5.
        assert_eq!(season.slot_of(1_781_524_800_000_000_000).0, 5);
        // 2026-01-15 is January: slot 0.
        assert_eq!(season.slot_of(1_768_478_400_000_000_000).0, 0);
    }

    #[test]
    fn too_many_slots_and_misaligned_slots_are_refused() {
        let utc = |w| TimeBucket::parse(w, None).unwrap();
        assert!(Season::new(utc("1s"), utc("1d")).is_err(), "86 400 slots");
        assert!(
            Season::new(utc("1w"), utc("1mo")).is_err(),
            "weeks do not tile months"
        );
        assert!(
            Season::new(utc("7m"), utc("1h")).is_err(),
            "7 minutes do not divide an hour"
        );
        assert!(Season::new(utc("15m"), utc("1d")).is_ok());
    }

    #[test]
    fn a_declaration_needs_a_source_and_a_test() {
        assert!(
            DetectorDeclaration::builder("x")
                .zscore(3.0)
                .build()
                .is_err()
        );
        assert!(
            DetectorDeclaration::builder("x")
                .source("m", "f")
                .build()
                .is_err()
        );
        assert!(
            DetectorDeclaration::builder("Bad Name")
                .source("m", "f")
                .zscore(3.0)
                .build()
                .is_err()
        );
        let d = DetectorDeclaration::builder("ok")
            .source("m", "f")
            .cusum(0.5, 5.0)
            .build()
            .unwrap();
        assert_eq!(d.output_measurement, "ok_verdicts");
        assert!(
            DetectorDeclaration::builder("x")
                .source("m", "f")
                .zscore(3.0)
                .output("m")
                .build()
                .is_err(),
            "verdicts into the source"
        );
        assert!(
            DetectorDeclaration::builder("x")
                .source("m", "f")
                .zscore(3.0)
                .forgetting(4)
                .build()
                .is_err(),
            "forgetting without a season"
        );
    }

    /// A build without `analytics` reads the head of a full state and
    /// leaves the rest alone — which is what keeps a declaration intact
    /// across such a build.
    #[cfg(feature = "analytics")]
    #[test]
    fn the_state_head_is_readable_without_the_baselines() {
        let mut state = DetectorState {
            watermark_ns: Some(42),
            late_points: 7,
            degraded: Some("budget".into()),
            series_count: 2,
            ..DetectorState::default()
        };
        state.series.insert("host=a".into(), Default::default());
        state.series.insert("host=b".into(), Default::default());
        let bytes = postcard::to_stdvec(&state).unwrap();
        let head = DetectorStateHead::decode(&bytes).unwrap();
        assert_eq!(head.watermark_ns, Some(42));
        assert_eq!(head.late_points, 7);
        assert_eq!(head.degraded.as_deref(), Some("budget"));
        assert_eq!(head.series_count, 2);
    }

    #[test]
    fn a_declaration_round_trips_through_postcard() {
        let d = DetectorDeclaration::builder("roof")
            .source("pv_ratio", "value")
            .season(Season::new(berlin("1mo"), berlin("1y")).unwrap())
            .zscore(3.0)
            .cusum(0.5, 5.0)
            .forgetting(3)
            .group_by(["array"])
            .build()
            .unwrap();
        let bytes = postcard::to_stdvec(&d).unwrap();
        let back: DetectorDeclaration = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back, d);
    }
}
