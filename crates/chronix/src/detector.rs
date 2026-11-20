//! Declared detectors: anomaly monitors the store keeps.
//!
//! A [`create_detector`](crate::analytics) object is something a caller
//! drives — it owns the state, the schedule and the persistence, which is
//! exactly the hard part on a box that reboots. A *declared* detector is the
//! other shape, the one a rollup has: declared once on a source measurement,
//! persisted in the catalog, advanced by the maintenance pass, and resumed —
//! not relearned — after a restart. Declaring and advancing need the
//! `analytics` feature (on by default; name it with `default-features =
//! false`).
//!
//! The source is any measurement, including one the application writes
//! itself — a performance ratio `actual / modelled` per quarter hour is the
//! intended use, since raw production flags every cloudy day:
//!
//! ```no_run
//! # #[cfg(feature = "analytics")] {
//! use chronix::detector::{DetectorDeclaration, Season};
//! use chronix_core::timebucket::TimeBucket;
//! # fn demo(db: &chronix::Chronix) -> chronix::error::Result<()> {
//! let berlin = |w| TimeBucket::parse(w, Some("Europe/Berlin")).unwrap();
//! let declaration = DetectorDeclaration::builder("roof_health")
//!     .source("pv_ratio", "value")
//!     .season(Season::new(berlin("15m"), berlin("1d"))?) // 14:00 vs 14:00
//!     .zscore(4.0)
//!     .cusum(0.5, 5.0)
//!     .forgetting(14) // follow the seasons: a slot's past halves every 14 days
//!     .build()?;
//! db.declare_detector(declaration)?; // idempotent: declare it on every start
//! # Ok(()) }
//! # }
//! ```
//!
//! Each point after its slot's warm-up produces one **verdict** row in the
//! output measurement (`<name>_verdicts` by default), tagged with the source
//! series' tags and `detector=<name>`, with the fields `value`, `baseline`,
//! `stddev`, `z`, `cusum_pos`, `cusum_neg`, `anomaly`, `reason` and `slot`.
//! Verdicts are ordinary points: query them, subscribe to them, alert on them.

use std::time::Duration;

use chronix_core::timebucket::{BucketWidth, TimeBucket};
use serde::{Deserialize, Serialize};

use crate::error::{DbError, Result};

/// The most slots one season may have.
pub const MAX_SLOTS_PER_PERIOD: usize = 2_048;

const DAY_NS: i64 = 86_400_000_000_000;

/// The calendar a baseline is learned against: `slot`-wide slots within a
/// `period`, read in one time zone.
///
/// **A slot is a wall-clock position.** Fifteen-minute slots in a
/// Europe/Berlin day make 14:00 slot 56 on every day of the year — on the
/// 23-hour day in March, the 25-hour day in October, and every other — so a
/// quarter hour is always judged against the same quarter hour. The repeated
/// hour of an autumn night folds into the slots of 02:00–02:59; the hour a
/// spring night skips leaves its four slots empty for that day. Monthly slots
/// in a year make June slot 5 in every year, and a period's index counts
/// calendar periods, so forgetting halves exactly once per period.
///
/// Pick the zone for what drives the signal: the local zone for anything
/// people and tariffs shape (consumption, charging), UTC for anything the sun
/// shapes (shading, dawn on a PV array), since solar time does not move when
/// the clocks do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "SeasonRepr", into = "SeasonRepr")]
pub struct Season {
    slot: TimeBucket,
    period: TimeBucket,
    /// [`Season::slots`], computed once.
    slots: usize,
}

/// The persisted form: the two buckets. Read back through [`Season::new`],
/// so a stored season obeys the same rules as a new one.
#[derive(Serialize, Deserialize)]
struct SeasonRepr {
    slot: TimeBucket,
    period: TimeBucket,
}

impl TryFrom<SeasonRepr> for Season {
    type Error = DbError;
    fn try_from(r: SeasonRepr) -> Result<Self> {
        Self::new(r.slot, r.period)
    }
}

impl From<Season> for SeasonRepr {
    fn from(s: Season) -> Self {
        Self {
            slot: s.slot,
            period: s.period,
        }
    }
}

impl Season {
    /// A season of `slot`-wide slots within each `period`.
    ///
    /// # Errors
    ///
    /// [`DbError::InvalidRequest`] when the slots do not tile the period —
    /// the two are read in different zones, the slot carries an origin (the
    /// period's start places every slot), the slot does not divide the period
    /// (15m in a 1d, 1d in a 1w or 1mo, 1mo in a 1y), or the period starts
    /// where no slot does — or when a period holds more than
    /// [`MAX_SLOTS_PER_PERIOD`] slots.
    pub fn new(slot: TimeBucket, period: TimeBucket) -> Result<Self> {
        let bad = |m: String| Err(DbError::InvalidRequest(m));
        if slot.timezone() != period.timezone() {
            return bad(format!(
                "a season's slot and period must be read in one time zone; got {} and {}",
                slot.timezone().unwrap_or("UTC"),
                period.timezone().unwrap_or("UTC")
            ));
        }
        if slot.origin().is_some() {
            return bad(
                "a season's slot cannot carry an origin: the period's start places every \
                 slot — set the origin on the period"
                    .into(),
            );
        }
        use BucketWidth::{Days, Fixed, Months};
        let tiles = match (slot.width(), period.width()) {
            (Fixed(s), Fixed(p)) => s > 0 && s < p && p % s == 0,
            (Fixed(s), Days(_) | Months(_)) => s > 0 && DAY_NS % s == 0,
            (Days(s), Days(p)) => s < p && p % s == 0,
            (Days(1), Months(_)) => true,
            (Months(s), Months(p)) => s < p && p % s == 0,
            _ => false,
        };
        // Calendar slots keep their own boundaries (days at midnight), so a
        // period starting mid-slot — a month from the 15th at 06:00 — would
        // cut its first slot in two.
        let aligned = slot
            .width()
            .is_calendar()
            .then(|| period.origin().map(|o| slot.start_of(o) == o))
            .flatten()
            .unwrap_or(true);
        if !tiles || !aligned {
            return bad(format!(
                "{} slots do not tile a {} period; choose a slot that divides it \
                 (15m in a 1d, 1d in a 1w or 1mo, 1mo in a 1y)",
                slot.width(),
                period.width()
            ));
        }
        let slots = Self::count_slots(slot.width(), period.width());
        if slots > MAX_SLOTS_PER_PERIOD {
            return bad(format!(
                "a {} period holds up to {slots} {} slots, more than {MAX_SLOTS_PER_PERIOD}; \
                 choose a wider slot",
                period.width(),
                slot.width()
            ));
        }
        Ok(Self {
            slot,
            period,
            slots,
        })
    }

    /// The most slots any one period holds on the wall clock: 96 quarter
    /// hours in a day, 31 days in a month, 366 days in a year, 12 months.
    fn count_slots(slot: BucketWidth, period: BucketWidth) -> usize {
        let slot_ns = match slot {
            BucketWidth::Fixed(s) => s,
            BucketWidth::Days(d) => i64::from(d) * DAY_NS,
            BucketWidth::Months(s) => {
                let BucketWidth::Months(p) = period else {
                    return 1;
                };
                return (p / s) as usize;
            }
        };
        let period_ns = match period {
            BucketWidth::Fixed(p) => p,
            BucketWidth::Days(d) => i64::from(d) * DAY_NS,
            BucketWidth::Months(m) => longest_months_days(m) * DAY_NS,
        };
        usize::try_from(period_ns / slot_ns.max(1)).unwrap_or(usize::MAX)
    }

    /// The slot width.
    #[must_use]
    pub fn slot(&self) -> &TimeBucket {
        &self.slot
    }

    /// The period width.
    #[must_use]
    pub fn period(&self) -> &TimeBucket {
        &self.period
    }

    /// The most slots one period holds on the wall clock — 96 for quarter
    /// hours in a day. Slots that only some periods have (the 31st of a
    /// month, 29 February) count.
    #[must_use]
    pub fn slots(&self) -> usize {
        self.slots
    }

    /// `(slot, period index)` of `ts`.
    ///
    /// The slot is the wall-clock position in the period (see [`Season`]);
    /// the period index counts calendar periods, consecutive periods being
    /// consecutive integers.
    #[must_use]
    pub fn slot_of(&self, ts: i64) -> (usize, i64) {
        let period_start = self.period.start_of(ts);
        let slot = match self.slot.width() {
            BucketWidth::Fixed(s) => {
                // Measured from the period's *nominal* local start. A period
                // that opens in a gap — Santiago springs forward at midnight —
                // starts at 01:00 on the wall clock, and 14:00 must still be
                // fourteen hours in: one nanosecond earlier the clock read
                // just before the nominal start.
                let start = self
                    .slot
                    .wall_clock_ns(period_start)
                    .min(self.slot.wall_clock_ns(period_start - 1) + 1);
                (self.slot.wall_clock_ns(ts) - start).div_euclid(s.max(1))
            }
            _ => self.slot.index_of(ts) - self.slot.index_of(period_start),
        };
        let slot = usize::try_from(slot.max(0)).unwrap_or(0);
        (slot.min(self.slots - 1), self.period.index_of(ts))
    }
}

/// The most days `m` consecutive calendar months hold: 31 for one, 92 for a
/// quarter, 366 for a year. Counted over a 400-year cycle, which holds every
/// arrangement of leap days the calendar has.
fn longest_months_days(m: u32) -> i64 {
    let month = TimeBucket::utc(BucketWidth::Months(1));
    // 2000-01-01T00:00Z.
    let mut starts = vec![946_684_800_000_000_000_i64];
    for _ in 0..4_800 + m {
        starts.push(month.next(*starts.last().unwrap_or(&0)));
    }
    let m = m as usize;
    starts
        .windows(m + 1)
        .map(|w| (w[m] - w[0]) / DAY_NS)
        .max()
        .unwrap_or(31)
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
    /// Slot baselines still warming up: (series, slot) pairs that have seen
    /// data but fewer than `min_samples` points of it, so judge nothing yet.
    /// A slot no data falls in (night, for a PV ratio) is not counted. Zero
    /// when every slot in use can judge; `None` in a build without
    /// `analytics`, which cannot read a baseline.
    pub warming_slots: Option<u64>,
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

    /// How many slots a period holds, on the wall clock, and what is
    /// refused: a cap, a slot that does not divide, two zones, a slot origin.
    #[test]
    fn slot_counts_and_refusals() {
        let utc = |w| TimeBucket::parse(w, None).unwrap();
        for (slot, period, n) in [
            ("15m", "1d", 96),
            ("1h", "1w", 168),
            ("1d", "1w", 7),
            ("1d", "1mo", 31),
            ("1d", "3mo", 92),
            ("1d", "1y", 366),
            ("1mo", "1y", 12),
            ("1h", "1mo", 744),
            ("5m", "1h", 12),
        ] {
            assert_eq!(
                Season::new(utc(slot), utc(period)).unwrap().slots(),
                n,
                "{slot} in {period}"
            );
        }
        assert!(Season::new(utc("1m"), utc("1w")).is_err(), "10 080 slots");
        assert!(
            Season::new(utc("1w"), utc("1mo")).is_err(),
            "weeks do not tile months"
        );
        assert!(
            Season::new(utc("7m"), utc("1d")).is_err(),
            "7 minutes do not divide a day"
        );
        assert!(
            Season::new(utc("1d"), utc("1d")).is_err(),
            "one slot is not a season"
        );
        assert!(Season::new(utc("15m"), berlin("1d")).is_err(), "two zones");
        let slot_origin = utc("15m").with_origin(60_000_000_000).unwrap();
        assert!(
            Season::new(slot_origin, utc("1d")).is_err(),
            "a slot origin"
        );
        // A month period from the 15th at 06:00 cannot be tiled by days
        // counted from midnight; one from the 15th at midnight can.
        let p = |o: &str| utc("1mo").with_origin_str(o).unwrap();
        assert!(Season::new(utc("1d"), p("2024-01-15 06:00:00")).is_err());
        assert!(Season::new(utc("1d"), p("2024-01-15")).is_ok());
    }

    /// America/Santiago springs forward at midnight: the day starts at 01:00
    /// on the wall clock, and 14:00 is still slot 56.
    #[test]
    fn a_day_that_starts_in_a_gap_keeps_wall_clock_slots() {
        let santiago = |w| TimeBucket::parse(w, Some("America/Santiago")).unwrap();
        let season = Season::new(santiago("15m"), santiago("1d")).unwrap();
        // 2024-09-08: 00:00 -04 → 01:00 -03. 14:00 -03 = 17:00Z.
        let two_pm = 1_725_814_800_000_000_000;
        assert_eq!(season.slot_of(two_pm).0, 56);
        // 01:00 -03 (04:00Z) — the first instant of the day — is slot 4.
        assert_eq!(season.slot_of(1_725_768_000_000_000_000).0, 4);
    }

    #[test]
    fn days_of_a_month() {
        let season = Season::new(berlin("1d"), berlin("1mo")).unwrap();
        // 2024-02-29 12:00 CET is the 29th: slot 28.
        assert_eq!(season.slot_of(1_709_204_400_000_000_000).0, 28);
    }

    /// A stored season is read back through the same rules.
    #[test]
    fn a_season_round_trips_through_postcard() {
        let season = Season::new(berlin("15m"), berlin("1d")).unwrap();
        let bytes = postcard::to_stdvec(&season).unwrap();
        let back: Season = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back, season);
        assert_eq!(back.slots(), 96);
    }

    /// 2026 Europe/Berlin, local wall-clock time → epoch nanoseconds.
    /// CET is +01:00, CEST +02:00; the transitions are 29 March 02:00 → 03:00
    /// and 25 October 03:00 → 02:00.
    fn berlin_at(y: i32, mo: u32, d: u32, h: u32, mi: u32, cest: bool) -> i64 {
        let days = {
            // Days from 1970-01-01 (proleptic Gregorian), no library needed.
            let (y, mo) = if mo <= 2 {
                (y - 1, mo + 9)
            } else {
                (y, mo - 3)
            };
            let era = y.div_euclid(400);
            let yoe = y - era * 400;
            let doy = (153 * mo as i32 + 2) / 5 + d as i32 - 1;
            let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
            i64::from(era * 146_097 + doe - 719_468)
        };
        let local = days * 86_400 + i64::from(h) * 3_600 + i64::from(mi) * 60;
        (local - if cest { 7_200 } else { 3_600 }) * 1_000_000_000
    }

    /// The slot is the wall-clock position: 14:00 is 14:00 on every day,
    /// including both transition days, and a day never has more than 96.
    #[test]
    fn a_quarter_hour_keeps_its_slot_across_both_transitions() {
        let season = Season::new(berlin("15m"), berlin("1d")).unwrap();
        // 14:00 on an ordinary day, on the 23-hour day, on the 25-hour day.
        for at in [
            berlin_at(2026, 3, 28, 14, 0, false),
            berlin_at(2026, 3, 29, 14, 0, true),
            berlin_at(2026, 10, 25, 14, 0, false),
        ] {
            assert_eq!(season.slot_of(at).0, 56, "14:00 must be slot 56");
        }
        // Both passes of 02:30 on 25 October are 02:30 on the wall clock.
        let first = berlin_at(2026, 10, 25, 2, 30, true);
        let second = berlin_at(2026, 10, 25, 2, 30, false);
        assert_eq!(first + 3_600_000_000_000, second);
        assert_eq!(season.slot_of(first).0, 10);
        assert_eq!(season.slot_of(second).0, 10);
        // 23:45 is the last slot on all three days.
        for (d, cest) in [(28, false), (29, true)] {
            assert_eq!(season.slot_of(berlin_at(2026, 3, d, 23, 45, cest)).0, 95);
        }
        assert_eq!(season.slot_of(berlin_at(2026, 10, 25, 23, 45, false)).0, 95);
    }

    /// Forgetting counts periods: consecutive years and months are
    /// consecutive integers, whatever their length.
    #[test]
    fn the_period_index_steps_by_one_per_calendar_period() {
        let yearly = Season::new(berlin("1mo"), berlin("1y")).unwrap();
        let years: Vec<i64> = (2019..=2030)
            .map(|y| yearly.slot_of(berlin_at(y, 1, 1, 12, 0, false)).1)
            .collect();
        assert!(
            years.windows(2).all(|w| w[1] == w[0] + 1),
            "years: {years:?}"
        );
        let monthly = Season::new(berlin("1d"), berlin("1mo")).unwrap();
        let months: Vec<i64> = (1..=12)
            .map(|m| {
                monthly
                    .slot_of(berlin_at(2024, m, 15, 12, 0, (4..=10).contains(&m)))
                    .1
            })
            .collect();
        assert!(
            months.windows(2).all(|w| w[1] == w[0] + 1),
            "months: {months:?}"
        );
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
