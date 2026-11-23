//! Time buckets — the one answer to "what is a day?".
//!
//! Everything that divides a stream of instants into buckets goes through
//! [`TimeBucket`]: the `time_bucket()` SQL function and every rollup tier.
//!
//! ## What a bucket width means
//!
//! The **unit** decides, not the magnitude. That is the whole rule:
//!
//! - **Sub-day units** (`ns`, `us`, `ms`, `s`, `m`/`min`, `h`) are
//!   [`BucketWidth::Fixed`]: a uniform span that never varies. An hour bucket
//!   is always an hour. In a zone they align to the zone's **standard**
//!   (non-summer) offset, so boundaries stay evenly spaced across a
//!   daylight-saving transition — truncating the *local* clock instead would
//!   merge the repeated hour of an autumn fall-back into one bucket.
//! - **Super-day units** (`d`, `w`, `mo`, `y`) follow the **local calendar**:
//!   local midnight to local midnight, which is 23 or 25 hours on a
//!   transition day, and a month of 28, 29, 30 or 31 days. A fixed span
//!   cannot be either.
//!
//! ## Why a type and not an `i64`
//!
//! `start + width` is not the next bucket when the width is a month, nor on a
//! transition day when it is a day. Callers ask [`TimeBucket::next`] instead,
//! and cannot reach a width to add — which is the point: that arithmetic is
//! correct for every value an `i64` can hold and wrong for the quantity.
//!
//! ## Anchors
//!
//! A width of one needs none: every local midnight is a day boundary and
//! every local 1st a month boundary. Wider ones are anchored on **Monday
//! 5 January 1970** (so `1w` starts on a Monday, not on the Thursday the
//! epoch happens to be) and on **January 1970** (so `3mo` is a calendar
//! quarter and `1y` a calendar year).

use std::fmt;
use std::str::FromStr;
use std::sync::LazyLock;

use jiff::Timestamp;
use jiff::tz::{AmbiguousOffset, TimeZone, TimeZoneDatabase};
use serde::{Deserialize, Serialize};

const NS: i64 = 1;
const US: i64 = 1_000;
const MS: i64 = 1_000_000;
const SEC: i64 = 1_000_000_000;
const MIN: i64 = 60 * SEC;
const HOUR: i64 = 60 * MIN;
const DAY: i64 = 24 * HOUR;

/// Days from 1970-01-01 to Monday 1970-01-05.
const DAY_ANCHOR: i64 = 4;

/// The IANA time-zone database compiled into this build — never the host's.
///
/// A bucket boundary is a function of the data and the build: a gateway's
/// directory read on a laptop buckets as it did on the gateway, an image with
/// no `/usr/share/zoneinfo` still knows `Europe/Berlin`, and a zone name from
/// a caller is looked up in memory, never on the filesystem. `TZ` and
/// `TZDIR` change nothing.
static ZONES: LazyLock<TimeZoneDatabase> = LazyLock::new(TimeZoneDatabase::bundled);

/// Resolve an IANA zone name (case-insensitively) against [`ZONES`].
fn zone(name: &str) -> Result<TimeZone, BucketParseError> {
    let unknown = || {
        BucketParseError(format!(
            "unknown time zone '{name}' — use an IANA name like Europe/Berlin"
        ))
    };
    let tz = ZONES.get(name.trim()).map_err(|_| unknown())?;
    // `Etc/Unknown` resolves to a zone that behaves as UTC: accepting it
    // would turn a typo-shaped name into a silent UTC.
    if tz
        .iana_name()
        .is_none_or(|n| n.eq_ignore_ascii_case("Etc/Unknown"))
    {
        return Err(unknown());
    }
    Ok(tz)
}

/// How wide a bucket is, and therefore whether it varies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BucketWidth {
    /// A fixed span of nanoseconds. Never varies.
    Fixed(i64),
    /// A whole number of calendar days. Varies across a daylight-saving
    /// transition when the bucket is zoned.
    Days(u32),
    /// A whole number of calendar months. `3` is a quarter, `12` a year.
    Months(u32),
}

/// Error parsing a bucket specification.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct BucketParseError(pub String);

impl BucketWidth {
    /// Roughly how long this bucket is, for a caller that needs one number.
    ///
    /// **An estimate for calendar widths**, and only ever used where an
    /// estimate is correct — sizing a scan, ordering two tiers by coarseness.
    /// A month is 30.44 days here and never that in reality, so anything that
    /// must be *right* uses [`TimeBucket::next`].
    #[must_use]
    pub fn nominal_ns(self) -> i64 {
        match self {
            Self::Fixed(n) => n,
            Self::Days(n) => i64::from(n).saturating_mul(24 * HOUR),
            // The mean Gregorian month: 146 097 days per 400 years / 4 800
            // months, to the nanosecond.
            Self::Months(n) => i64::from(n).saturating_mul(2_629_746 * SEC),
        }
    }

    /// Whether this width follows a calendar rather than a fixed span.
    #[must_use]
    pub fn is_calendar(self) -> bool {
        !matches!(self, Self::Fixed(_))
    }
}

impl fmt::Display for BucketWidth {
    /// The canonical spelling, which parses back to the same width.
    ///
    /// Round-tripping matters: a rollup reports the width it was created
    /// with, and an operator has to be able to type that back in.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Fixed(n) => {
                for (unit, suffix) in [(HOUR, "h"), (MIN, "m"), (SEC, "s"), (MS, "ms"), (US, "us")]
                {
                    if n % unit == 0 {
                        return write!(f, "{}{suffix}", n / unit);
                    }
                }
                write!(f, "{n}ns")
            }
            Self::Days(n) if n % 7 == 0 => write!(f, "{}w", n / 7),
            Self::Days(n) => write!(f, "{n}d"),
            Self::Months(n) if n % 12 == 0 => write!(f, "{}y", n / 12),
            Self::Months(n) => write!(f, "{n}mo"),
        }
    }
}

impl FromStr for BucketWidth {
    type Err = BucketParseError;

    /// Parse `15m`, `1h`, `1d`, `1w`, `1mo`, `1y`.
    ///
    /// `mo` is the month, never `M`: a case-sensitive distinction between
    /// minute and month is the kind that is wrong once and then wrong for
    /// three years of stored data. InfluxDB spells it `mo` for the same
    /// reason.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.is_empty() {
            return Err(BucketParseError("empty bucket width".into()));
        }
        let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
        let (num_str, suffix) = s.split_at(split);
        let n: i64 = num_str.parse().map_err(|_| {
            BucketParseError(format!(
                "'{s}' does not start with a number — write a width like 15m, 1h, 1d, 1mo"
            ))
        })?;
        if n <= 0 {
            return Err(BucketParseError(format!(
                "a bucket width must be positive, got '{s}'"
            )));
        }
        let suffix = suffix.trim();
        let mul = |unit: i64| -> Result<Self, BucketParseError> {
            n.checked_mul(unit).map(Self::Fixed).ok_or_else(|| {
                BucketParseError(format!("bucket width '{s}' overflows the nanosecond range"))
            })
        };
        let count = |per: i64| -> Result<u32, BucketParseError> {
            u32::try_from(n.saturating_mul(per))
                .map_err(|_| BucketParseError(format!("bucket width '{s}' is too large")))
        };
        // The long spellings are accepted beside the short ones, because
        // `time_bucket('1 hour', _time)` is what somebody coming from
        // TimescaleDB or PostgreSQL writes, and the parser already tolerates
        // the space. What is deliberately *not* accepted is a bare `M` for
        // the month: `m` is the minute here, and a unit that means one thing
        // in one case and 43 200 times that in the other is a trap. The
        // month is `mo` or `month`, always.
        match suffix {
            "ns" | "nanosecond" | "nanoseconds" => mul(NS),
            "us" | "µs" | "microsecond" | "microseconds" => mul(US),
            "ms" | "millisecond" | "milliseconds" => mul(MS),
            "s" | "sec" | "secs" | "second" | "seconds" => mul(SEC),
            "m" | "min" | "mins" | "minute" | "minutes" => mul(MIN),
            "h" | "hr" | "hrs" | "hour" | "hours" => mul(HOUR),
            "d" | "day" | "days" => count(1).map(Self::Days),
            "w" | "week" | "weeks" => count(7).map(Self::Days),
            "mo" | "month" | "months" => count(1).map(Self::Months),
            "y" | "yr" | "yrs" | "year" | "years" => count(12).map(Self::Months),
            "" => Err(BucketParseError(format!(
                "'{s}' has no unit — write a width like 15m, 1h, 1d, 1mo"
            ))),
            other => Err(BucketParseError(format!(
                "unknown bucket unit '{other}'. Use ns, us, ms, s, m/min, h, d, w, mo, y — \
                 or their long forms (second, minute, hour, day, week, month, year) \
                 (the month is 'mo'; 'm' is always the minute)"
            ))),
        }
    }
}

/// A bucket width together with the calendar it is read against.
///
/// See the [module documentation](self) for what each width means.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeBucket {
    width: BucketWidth,
    tz: Option<TimeZone>,
    /// The instant bucket boundaries are aligned to, if not the default
    /// anchor. See [`TimeBucket::with_origin`].
    origin: Option<i64>,
}

/// The on-disk and on-the-wire form.
///
/// The zone travels as its IANA name, so a rollup's definition survives a
/// change of time library and can be read by anything.
///
/// **No `skip_serializing_if`, deliberately.** The rollup catalog is
/// `postcard`, which is not self-describing: a field the serialiser omits is
/// a field the deserialiser still expects, and every following byte is read
/// as the wrong thing. An `Option` costs one tag byte and is always written.
#[derive(Serialize, Deserialize)]
struct TimeBucketRepr {
    width: BucketWidth,
    timezone: Option<String>,
    /// Always written, for the same reason `timezone` is.
    origin: Option<i64>,
}

impl Serialize for TimeBucket {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        TimeBucketRepr {
            width: self.width,
            timezone: self.timezone().map(str::to_owned),
            origin: self.origin,
        }
        .serialize(s)
    }
}

impl<'de> Deserialize<'de> for TimeBucket {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let repr = TimeBucketRepr::deserialize(d)?;
        let tz = match repr.timezone {
            None => None,
            Some(name) => Some(zone(&name).map_err(serde::de::Error::custom)?),
        };
        let bucket = Self {
            width: repr.width,
            tz,
            origin: None,
        };
        // Through `with_origin`, not past it: a deserialiser is a
        // constructor, and the day-of-month rule has to hold for a bucket
        // read back from the catalog exactly as it does for one just built.
        match repr.origin {
            None => Ok(bucket),
            Some(origin) => bucket.with_origin(origin).map_err(serde::de::Error::custom),
        }
    }
}

impl TimeBucket {
    /// A bucket of `width`, read against UTC.
    #[must_use]
    pub const fn utc(width: BucketWidth) -> Self {
        Self {
            width,
            tz: None,
            origin: None,
        }
    }

    /// A fixed span of nanoseconds against UTC.
    #[must_use]
    pub const fn fixed_ns(ns: i64) -> Self {
        Self::utc(BucketWidth::Fixed(ns))
    }

    /// A fixed span, from a [`Duration`](std::time::Duration).
    ///
    /// A `Duration` *is* a fixed span, so this conversion loses nothing —
    /// which is exactly why it cannot produce a calendar bucket. For a day in
    /// a zone or a calendar month, say so by name: [`parse`](Self::parse)
    /// with `"1d"` or `"1mo"`.
    ///
    /// Saturates at [`i64::MAX`] nanoseconds, about 292 years.
    #[must_use]
    pub fn fixed(span: std::time::Duration) -> Self {
        Self::fixed_ns(i64::try_from(span.as_nanos()).unwrap_or(i64::MAX))
    }

    /// Read this bucket against an IANA time zone, such as `Europe/Berlin`.
    ///
    /// # Errors
    ///
    /// [`BucketParseError`] if the zone is unknown, or if an origin already
    /// set falls on a day of month [`with_origin`](Self::with_origin) refuses
    /// in the new zone.
    pub fn in_zone(mut self, zone_name: &str) -> Result<Self, BucketParseError> {
        self.tz = Some(zone(zone_name)?);
        match self.origin.take() {
            None => Ok(self),
            Some(origin) => self.with_origin(origin),
        }
    }

    /// Align bucket boundaries to `origin` instead of the default anchor.
    ///
    /// The origin is an instant in epoch nanoseconds; what is taken from it
    /// is its **local** position in this bucket's zone:
    ///
    /// - a fixed width takes the whole instant, and buckets tile outwards
    ///   from it in both directions;
    /// - a day or week width takes the origin's local **time of day**, so a
    ///   `1d` bucket runs 06:00 → 06:00 for a shift that starts at six;
    /// - a month or year width takes its local **day of month** and time of
    ///   day, so a `1mo` bucket runs from the 15th to the 15th for a billing
    ///   period that starts then.
    ///
    /// Only where the origin falls in the cycle matters, not how far away it
    /// is. For a **multi-unit** width it also selects which of the `n` units
    /// opens a bucket: `3mo` anchored on January gives calendar quarters,
    /// anchored on February gives February–April. It replaces the default
    /// anchor entirely, so a `1w` bucket with an origin starts on the
    /// origin's weekday.
    ///
    /// # Errors
    ///
    /// [`BucketParseError`] if the origin's local day of month is 29, 30 or
    /// 31 for a month or year width: those days are missing from some months,
    /// so such a boundary is not a monthly one.
    pub fn with_origin(mut self, origin: i64) -> Result<Self, BucketParseError> {
        if matches!(self.width, BucketWidth::Months(_)) {
            let (_, _, day) = civil_from_days(self.local_day_and_time(origin).0);
            if day > 28 {
                return Err(BucketParseError(format!(
                    "a monthly bucket cannot start on day {day}: that day is missing from some months, so it is not a monthly boundary. Use 1 to 28."
                )));
            }
        }
        self.origin = Some(origin);
        Ok(self)
    }

    /// Align boundaries to an origin written as text.
    ///
    /// Accepts an RFC 3339 instant (`2024-01-15T06:00:00+01:00`), or a local
    /// wall-clock time read **in this bucket's zone**: `2024-01-15`,
    /// `2024-01-15 06:00:00`, or `2024-01-15T06:00:00`. A bare date is local
    /// midnight — "the billing month starts on the 15th" means the local 15th.
    ///
    /// # Errors
    ///
    /// [`BucketParseError`] if the text is not one of those forms, names a
    /// local time inside a daylight-saving gap, is outside the representable
    /// range, or if [`Self::with_origin`] refuses the resulting day of month.
    pub fn with_origin_str(self, text: &str) -> Result<Self, BucketParseError> {
        let text = text.trim();
        let out_of_range = || {
            BucketParseError(format!(
                "origin '{text}' is outside the representable range"
            ))
        };
        if let Ok(instant) = text.parse::<Timestamp>() {
            let ns = i64::try_from(instant.as_nanosecond()).map_err(|_| out_of_range())?;
            return self.with_origin(ns);
        }
        let local = text
            .parse::<jiff::civil::DateTime>()
            .or_else(|_| {
                text.parse::<jiff::civil::Date>()
                    .map(|d| d.to_datetime(jiff::civil::Time::midnight()))
            })
            .map_err(|_| {
                BucketParseError(format!(
                    "could not read '{text}' as an origin — write it as '2024-01-15', \
                     '2024-01-15 06:00:00', or an RFC 3339 instant"
                ))
            })?;
        let offset = match &self.tz {
            None => jiff::tz::Offset::UTC,
            Some(tz) => match tz.to_ambiguous_timestamp(local).offset() {
                AmbiguousOffset::Unambiguous { offset } => offset,
                // Happens twice: the earlier, as a bucket boundary does.
                AmbiguousOffset::Fold { before, .. } => before,
                AmbiguousOffset::Gap { .. } => {
                    return Err(BucketParseError(format!(
                        "the local time '{text}' does not exist in {} — \
                         it falls in a daylight-saving gap",
                        tz.iana_name().unwrap_or("this zone")
                    )));
                }
            },
        };
        let instant = offset.to_timestamp(local).map_err(|_| out_of_range())?;
        let ns = i64::try_from(instant.as_nanosecond()).map_err(|_| out_of_range())?;
        self.with_origin(ns)
    }

    /// The instant this bucket's boundaries are aligned to, if any.
    #[must_use]
    pub const fn origin(&self) -> Option<i64> {
        self.origin
    }

    /// Parse a width, and optionally a zone.
    ///
    /// # Errors
    /// [`BucketParseError`] if the width or the zone name is not understood.
    pub fn parse(width: &str, timezone: Option<&str>) -> Result<Self, BucketParseError> {
        let width: BucketWidth = width.parse()?;
        let tz = match timezone.map(str::trim).filter(|s| !s.is_empty()) {
            None => None,
            Some(name) => Some(zone(name)?),
        };
        Ok(Self {
            width,
            tz,
            origin: None,
        })
    }

    /// This bucket's width.
    #[must_use]
    pub const fn width(&self) -> BucketWidth {
        self.width
    }

    /// The IANA zone this bucket is read against, if any, in its canonical
    /// spelling (`Europe/Berlin` however it was typed).
    #[must_use]
    pub fn timezone(&self) -> Option<&str> {
        self.tz.as_ref().and_then(TimeZone::iana_name)
    }

    /// Roughly how long a bucket is — see [`BucketWidth::nominal_ns`].
    #[must_use]
    pub fn nominal_ns(&self) -> i64 {
        self.width.nominal_ns()
    }

    /// Whether a bucket's length can vary between one bucket and the next.
    ///
    /// True for a calendar width in *any* zone (February is short in UTC
    /// too), and for a day only where a zone has transitions.
    #[must_use]
    pub fn is_calendar(&self) -> bool {
        self.width.is_calendar()
    }

    /// The start of the bucket containing `ts`.
    #[must_use]
    pub fn start_of(&self, ts: i64) -> i64 {
        self.locate(ts, 0).1
    }

    /// The ordinal of the bucket containing `ts`, counted from this width's
    /// anchor: consecutive buckets have consecutive ordinals, whatever their
    /// length, and two instants share an ordinal exactly when they share a
    /// bucket.
    ///
    /// This is how "how many periods apart?" is answered on a calendar —
    /// dividing by [`nominal_ns`](Self::nominal_ns) is not: a year in
    /// Europe/Berlin divided by 365.2425 days makes 2023 and 2024 the same
    /// year.
    #[must_use]
    pub fn index_of(&self, ts: i64) -> i64 {
        self.locate(ts, 0).0
    }

    /// Locate a bucket relative to the one containing `ts`. Advance the
    /// ordinal before clipping the boundary to the representable timeline.
    fn locate(&self, ts: i64, ahead: i64) -> (i64, i64) {
        match self.width {
            BucketWidth::Fixed(n) => {
                let n = i128::from(n.max(1));
                // With an origin, buckets tile outwards from that instant.
                // Without one they align to the zone's *standard* clock, not
                // to whatever offset is in force at `ts`: an alignment that
                // moved with summer time would make one bucket an hour long
                // and its neighbour two. Standard local midnight is the
                // instant `-offset`, so in Asia/Kolkata (+05:30) hour buckets
                // land at half past UTC — the local clock's hour.
                let anchor = self.origin.unwrap_or_else(|| -self.standard_offset_ns(ts));
                let since = i128::from(ts) - i128::from(anchor);
                let ordinal = since.div_euclid(n) + i128::from(ahead);
                (
                    clamp_i64(ordinal),
                    clamp_i64(i128::from(anchor) + ordinal * n),
                )
            }
            BucketWidth::Days(n) => {
                let n = i64::from(n.max(1));
                let (anchor_day, tod) = self.day_phase();
                // An instant earlier in the day than the boundary belongs to
                // the bucket that opened the previous day.
                let (day, time) = self.local_day_and_time(ts);
                // A repeated 02:15 can follow the first 02:30. A boundary
                // is the earlier instant when the local clock repeats.
                let day = day - i64::from(time < tod && ts < self.instant_at(day, tod));
                let ordinal = (day - anchor_day).div_euclid(n) + ahead;
                let start_day = ordinal * n + anchor_day;
                (ordinal, self.instant_at(start_day, tod))
            }
            BucketWidth::Months(n) => {
                let n = i64::from(n.max(1));
                let (anchor_month, dom, tod) = self.month_phase();
                let (day, time) = self.local_day_and_time(ts);
                let (y, m, d) = civil_from_days(day);
                let mut months = (y - 1970) * 12 + i64::from(m - 1);
                // Before this month's boundary day (or at it but earlier in
                // the day) is still the previous month's bucket.
                if d < dom || (d == dom && time < tod && ts < self.instant_at(day, tod)) {
                    months -= 1;
                }
                let ordinal = (months - anchor_month).div_euclid(n) + ahead;
                let start = self.month_boundary(ordinal * n + anchor_month, dom, tod);
                (ordinal, start)
            }
        }
    }

    /// The start of the bucket after the one starting at `bucket_start`.
    ///
    /// **Never `bucket_start + width`.** A calendar bucket's length depends
    /// on which bucket it is: February is three days shorter than March, and
    /// a local day is 23 or 25 hours on a transition. Code that adds is code
    /// that has assumed the calendar away.
    #[must_use]
    pub fn next(&self, bucket_start: i64) -> i64 {
        if let BucketWidth::Fixed(n) = self.width
            && bucket_start != i64::MIN
        {
            return bucket_start.saturating_add(n.max(1));
        }
        self.locate(bucket_start, 1).1
    }

    /// The wall clock at `ts` in this bucket's zone, as nanoseconds since
    /// local 1970-01-01 00:00 — "civil" time, in which every day is 24 hours
    /// and 14:00 is `14 h` past a day's start on every day of the year.
    ///
    /// Differences of civil times are wall-clock distances: what a season's
    /// slot is measured in.
    #[must_use]
    pub fn wall_clock_ns(&self, ts: i64) -> i64 {
        ts.saturating_add(self.local_offset_ns(ts))
    }

    fn local_offset_ns(&self, ts: i64) -> i64 {
        self.tz
            .as_ref()
            .map_or(0, |tz| offset_ns(tz.to_offset(timestamp(ts))))
    }

    // Apply the offset to the time of day so a local civil time just outside
    // the timestamp range can still resolve to its correct UTC boundary.
    fn local_day_and_time(&self, ts: i64) -> (i64, i64) {
        let time = ts.rem_euclid(DAY) + self.local_offset_ns(ts);
        (
            ts.div_euclid(DAY) + time.div_euclid(DAY),
            time.rem_euclid(DAY),
        )
    }

    /// The instant the wall clock reaches `tod` on `day`.
    ///
    /// A time that happens twice (autumn fall-back) is its earlier instant,
    /// which is what a boundary means. A time that does not happen (spring
    /// forward — at midnight in America/Santiago and Asia/Beirut) is the
    /// instant the clock resumed.
    fn instant_at(&self, day: i64, tod: i64) -> i64 {
        let civil = i128::from(day) * i128::from(DAY) + i128::from(tod);
        let Some(tz) = &self.tz else {
            return clamp_i64(civil);
        };
        let Some(dt) = civil_datetime(day, tod) else {
            return clamp_i64(civil);
        };
        let instant = |offset| clamp_i64(civil - i128::from(offset_ns(offset)));
        match tz.to_ambiguous_timestamp(dt).offset() {
            AmbiguousOffset::Unambiguous { offset } => instant(offset),
            AmbiguousOffset::Fold { before, .. } => instant(before),
            AmbiguousOffset::Gap { before, .. } => {
                // `civil - before` lies inside the gap's instants, so the
                // transition just before it is the instant the clock resumed.
                let inside = instant(before);
                tz.preceding(timestamp(inside.saturating_add(1)))
                    .next()
                    .map_or(inside, |t| clamp_i64(t.timestamp().as_nanosecond()))
            }
        }
    }

    /// The instant local `tod` on day `dom` of the month `months` after
    /// January 1970 happens at.
    fn month_boundary(&self, months: i64, dom: u32, tod: i64) -> i64 {
        let day = days_from_civil(
            1970 + months.div_euclid(12),
            months.rem_euclid(12) as u32 + 1,
            dom,
        );
        self.instant_at(day, tod)
    }

    /// The day anchor and the local time of day boundaries fall on.
    ///
    /// Without an origin: Monday 1970-01-05 at local midnight, so `1w`
    /// starts on a Monday rather than on the Thursday the epoch happens to
    /// be.
    fn day_phase(&self) -> (i64, i64) {
        match self.origin {
            Some(o) => self.local_day_and_time(o),
            None => (DAY_ANCHOR, 0),
        }
    }

    /// The month anchor, the day of month, and the local time of day.
    ///
    /// Without an origin: January 1970, the 1st, at local midnight — so
    /// `3mo` is a calendar quarter and `1y` a calendar year. The day of month
    /// is 1–28 by construction: [`TimeBucket::with_origin`] is the only way
    /// in, including from a deserialiser, and refuses the rest.
    fn month_phase(&self) -> (i64, u32, i64) {
        match self.origin {
            Some(o) => {
                let (day, time) = self.local_day_and_time(o);
                let (y, m, d) = civil_from_days(day);
                ((y - 1970) * 12 + i64::from(m - 1), d.min(28), time)
            }
            None => (0, 1, 0),
        }
    }

    /// The zone's standard (non-summer) UTC offset at `ts`, in nanoseconds.
    ///
    /// A zone's *base* offset does change, on the scale of decades —
    /// Europe/Moscow moved twice — so this is read at `ts` rather than
    /// assumed constant.
    fn standard_offset_ns(&self, ts: i64) -> i64 {
        let Some(tz) = &self.tz else { return 0 };
        let at = timestamp(ts);
        let info = tz.to_offset_info(at);
        if !info.dst().is_dst() {
            return offset_ns(info.offset());
        }
        tz.preceding(at)
            .find(|t| !t.dst().is_dst())
            .map_or_else(|| offset_ns(info.offset()), |t| offset_ns(t.offset()))
    }
}

impl fmt::Display for TimeBucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.timezone() {
            None => write!(f, "{}", self.width),
            Some(tz) => write!(f, "{} {tz}", self.width),
        }
    }
}

fn timestamp(ns: i64) -> Timestamp {
    // Every `i64` of nanoseconds (±292 years) is inside jiff's ±9999 years.
    Timestamp::from_nanosecond(i128::from(ns)).unwrap_or(Timestamp::UNIX_EPOCH)
}

fn offset_ns(offset: jiff::tz::Offset) -> i64 {
    i64::from(offset.seconds()) * SEC
}

fn clamp_i64(v: i128) -> i64 {
    i64::try_from(v).unwrap_or(if v < 0 { i64::MIN } else { i64::MAX })
}

/// Civil day and time of day → a jiff civil date-time, for zone lookups.
fn civil_datetime(day: i64, tod: i64) -> Option<jiff::civil::DateTime> {
    let (y, m, d) = civil_from_days(day);
    jiff::civil::DateTime::new(
        i16::try_from(y).ok()?,
        i8::try_from(m).ok()?,
        i8::try_from(d).ok()?,
        i8::try_from(tod / HOUR).ok()?,
        i8::try_from(tod % HOUR / MIN).ok()?,
        i8::try_from(tod % MIN / SEC).ok()?,
        i32::try_from(tod % SEC).ok()?,
    )
    .ok()
}

/// Days from 1970-01-01 to the proleptic Gregorian date `y-m-d`.
///
/// Howard Hinnant's `days_from_civil`: exact for every date, no table, no
/// failure — which is what lets bucket arithmetic stay in integers.
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = i64::from((m + 9) % 12);
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The proleptic Gregorian `(year, month, day)` of `days` after 1970-01-01.
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn ts(s: &str) -> i64 {
        i64::try_from(s.parse::<Timestamp>().unwrap().as_nanosecond()).unwrap()
    }

    fn berlin() -> &'static str {
        "Europe/Berlin"
    }

    fn weekday(ns: i64) -> jiff::civil::Weekday {
        TimeZone::UTC.to_datetime(timestamp(ns)).weekday()
    }

    // ── Parsing ────────────────────────────────────────────────────────

    #[test]
    fn the_unit_decides_whether_a_width_is_a_calendar_one() {
        use BucketWidth::{Days, Fixed, Months};
        let cases = [
            ("30s", Fixed(30 * SEC)),
            ("15m", Fixed(15 * MIN)),
            ("15min", Fixed(15 * MIN)),
            // A sub-day unit stays fixed however large the number: 36 hours
            // is thirty-six hours, not a day and a half of calendar.
            ("36h", Fixed(36 * HOUR)),
            ("1d", Days(1)),
            ("1w", Days(7)),
            ("2w", Days(14)),
            ("1mo", Months(1)),
            ("3mo", Months(3)),
            ("1y", Months(12)),
        ];
        for (text, want) in cases {
            assert_eq!(text.parse::<BucketWidth>().unwrap(), want, "{text}");
        }
    }

    /// Every width round-trips through its own spelling.
    ///
    /// A rollup reports the width it was created with and an operator has to
    /// be able to type that back in (the invertible-name rule).
    #[test]
    fn a_widths_spelling_parses_back_to_the_same_width() {
        for text in [
            "500ns", "5us", "250ms", "30s", "15m", "6h", "1d", "1w", "3mo", "1y",
        ] {
            let width: BucketWidth = text.parse().unwrap();
            let spelled = width.to_string();
            assert_eq!(
                spelled.parse::<BucketWidth>().unwrap(),
                width,
                "{text} spelled as {spelled}"
            );
        }
    }

    #[test]
    fn the_month_is_mo_and_m_is_always_the_minute() {
        assert_eq!(
            "1m".parse::<BucketWidth>().unwrap(),
            BucketWidth::Fixed(MIN)
        );
        assert_eq!(
            "1mo".parse::<BucketWidth>().unwrap(),
            BucketWidth::Months(1)
        );
        // `M` is refused rather than guessed at: a case-sensitive difference
        // between a minute and a month is wrong once and then wrong for
        // three years of stored data.
        let err = "1M".parse::<BucketWidth>().unwrap_err();
        assert!(err.0.contains("'mo'"), "{err}");
    }

    #[test]
    fn the_long_unit_names_mean_the_same_as_the_short_ones() {
        // `time_bucket('1 hour', _time)` is what somebody coming from
        // TimescaleDB writes, and the parser already tolerated the space —
        // so the only thing standing between that query and an answer was
        // the vocabulary.
        for (long, short) in [
            ("1 second", "1s"),
            ("30 seconds", "30s"),
            ("15 minutes", "15m"),
            ("1 minute", "1m"),
            ("1 hour", "1h"),
            ("24 hours", "24h"),
            ("1 day", "1d"),
            ("2 weeks", "2w"),
            ("1 month", "1mo"),
            ("3 months", "3mo"),
            ("1 year", "1y"),
        ] {
            assert_eq!(
                long.parse::<BucketWidth>().unwrap(),
                short.parse::<BucketWidth>().unwrap(),
                "'{long}' must mean '{short}'"
            );
        }
        // And the long forms work without the space too.
        assert_eq!(
            "1hour".parse::<BucketWidth>().unwrap(),
            BucketWidth::Fixed(HOUR)
        );
    }

    #[test]
    fn no_long_form_reopens_the_minute_month_trap() {
        // `month` is unambiguous; `M`, `Min` and `MO` are not spellings this
        // parser accepts, because a case-sensitive difference between a
        // minute and a month is wrong once and then wrong for three years of
        // stored data.
        assert_eq!(
            "1month".parse::<BucketWidth>().unwrap(),
            BucketWidth::Months(1)
        );
        assert_eq!(
            "1minute".parse::<BucketWidth>().unwrap(),
            BucketWidth::Fixed(MIN)
        );
        for ambiguous in ["1M", "1MO", "1Month", "1Min"] {
            assert!(
                ambiguous.parse::<BucketWidth>().is_err(),
                "'{ambiguous}' must be refused rather than guessed at"
            );
        }
    }

    #[test]
    fn a_width_without_a_unit_or_with_a_bad_one_is_refused() {
        for bad in ["", "5", "0h", "-1h", "1fortnight", "h"] {
            assert!(
                bad.parse::<BucketWidth>().is_err(),
                "'{bad}' must be refused"
            );
        }
    }

    #[test]
    fn an_unknown_zone_is_refused_by_name() {
        let err = TimeBucket::parse("1d", Some("Europe/Atlantis")).unwrap_err();
        assert!(err.0.contains("Europe/Atlantis"), "{err}");
        assert!(err.0.contains("IANA"), "{err}");
    }

    // ── Fixed widths stay uniform ──────────────────────────────────────

    /// The defect this type exists for: two instants an hour apart cannot
    /// share a one-hour bucket, even where the local clock says they can.
    #[test]
    fn a_fixed_bucket_does_not_merge_the_repeated_hour() {
        let b = TimeBucket::utc(BucketWidth::Fixed(HOUR))
            .in_zone(berlin())
            .unwrap();
        // 2026-10-25T01:00:00Z is the transition: 03:00 CEST → 02:00 CET.
        // Both of these read 02:30 on the local clock.
        let first = b.start_of(ts("2026-10-25T00:30:00Z"));
        let second = b.start_of(ts("2026-10-25T01:30:00Z"));
        assert_eq!(second - first, HOUR, "hour buckets stay an hour apart");
        assert_eq!(first, ts("2026-10-25T00:00:00Z"));
        assert_eq!(second, ts("2026-10-25T01:00:00Z"));
    }

    /// …so a 25-hour local day holds 25 hourly buckets, evenly spaced.
    #[test]
    fn a_fall_back_day_holds_twenty_five_hourly_buckets() {
        let b = TimeBucket::utc(BucketWidth::Fixed(HOUR))
            .in_zone(berlin())
            .unwrap();
        let day_start = ts("2026-10-24T22:00:00Z"); // local midnight, CEST
        let buckets: std::collections::BTreeSet<i64> = (0..25 * 60)
            .map(|m| b.start_of(day_start + m * MIN))
            .collect();
        assert_eq!(buckets.len(), 25);
        let mut prev: Option<i64> = None;
        for start in buckets {
            if let Some(p) = prev {
                assert_eq!(start - p, HOUR, "evenly spaced");
            }
            prev = Some(start);
        }
    }

    /// A half-hour zone buckets on its own clock, which is what makes the
    /// standard-offset alignment observable at all.
    #[test]
    fn a_half_hour_zone_buckets_on_its_own_clock() {
        let kolkata = "Asia/Kolkata"; // UTC+05:30, no DST
        let b = TimeBucket::utc(BucketWidth::Fixed(HOUR))
            .in_zone(kolkata)
            .unwrap();
        // 00:10 UTC is 05:40 local; the hour bucket is 05:00 local = 23:30Z
        // the previous day.
        assert_eq!(
            b.start_of(ts("2024-03-15T00:10:00Z")),
            ts("2024-03-14T23:30:00Z")
        );
    }

    /// A fixed width wider than the offset starts at the zone's standard
    /// midnight, not at twice the offset past it: six-hour buckets in Berlin
    /// are 00:00, 06:00, 12:00 and 18:00 CET.
    #[test]
    fn a_wide_fixed_bucket_starts_on_the_standard_clock() {
        let b = TimeBucket::parse("6h", Some("Europe/Berlin")).unwrap();
        // 12:00Z is 13:00 CET: the bucket opened at 12:00 CET = 11:00Z.
        assert_eq!(
            b.start_of(ts("2024-01-10T12:00:00Z")),
            ts("2024-01-10T11:00:00Z")
        );
        // 2024-01-10T12:00Z is 17:30 in Kolkata: 12:00–18:00 local = 06:30Z.
        let k = TimeBucket::parse("6h", Some("Asia/Kolkata")).unwrap();
        assert_eq!(
            k.start_of(ts("2024-01-10T12:00:00Z")),
            ts("2024-01-10T06:30:00Z")
        );
    }

    #[test]
    fn an_unzoned_fixed_bucket_truncates_utc() {
        let b = TimeBucket::fixed_ns(HOUR);
        assert_eq!(
            b.start_of(ts("2024-03-15T02:34:56Z")),
            ts("2024-03-15T02:00:00Z")
        );
        assert_eq!(
            b.next(ts("2024-03-15T02:00:00Z")),
            ts("2024-03-15T03:00:00Z")
        );
    }

    // ── Calendar widths follow the calendar ────────────────────────────

    #[test]
    fn a_zoned_day_is_local_midnight_to_local_midnight() {
        let ny = "America/New_York";
        let b = TimeBucket::utc(BucketWidth::Days(1)).in_zone(ny).unwrap();
        // 02:00 UTC on the 15th is 22:00 on the *14th* in New York.
        assert_eq!(
            b.start_of(ts("2024-03-15T02:00:00Z")),
            ts("2024-03-14T04:00:00Z")
        );
    }

    /// A transition day is 23 or 25 hours, and `next` says so.
    #[test]
    fn a_transition_day_is_not_twenty_four_hours() {
        let ny = "America/New_York";
        let day = TimeBucket::utc(BucketWidth::Days(1)).in_zone(ny).unwrap();

        // Spring forward: 2024-03-10 is 23 hours in New York.
        let start = day.start_of(ts("2024-03-10T20:00:00Z"));
        assert_eq!(start, ts("2024-03-10T05:00:00Z"));
        assert_eq!(day.next(start) - start, 23 * HOUR);

        // Fall back: 2024-11-03 is 25 hours.
        let start = day.start_of(ts("2024-11-03T20:00:00Z"));
        assert_eq!(start, ts("2024-11-03T04:00:00Z"));
        assert_eq!(day.next(start) - start, 25 * HOUR);
    }

    /// A month is a month — and `start + width` would never have been one.
    #[test]
    fn a_month_bucket_is_a_calendar_month() {
        let b = TimeBucket::utc(BucketWidth::Months(1))
            .in_zone(berlin())
            .unwrap();
        let feb = b.start_of(ts("2024-02-17T12:00:00Z"));
        assert_eq!(feb, ts("2024-01-31T23:00:00Z"), "1 Feb 2024, Berlin (CET)");
        // 2024 is a leap year: February is 29 days.
        assert_eq!(b.next(feb) - feb, 29 * 24 * HOUR);
        // …and 2023's was 28.
        let feb23 = b.start_of(ts("2023-02-17T12:00:00Z"));
        assert_eq!(b.next(feb23) - feb23, 28 * 24 * HOUR);
        // March crosses the spring transition, so it is an hour short of 31
        // days.
        let mar = b.start_of(ts("2024-03-17T12:00:00Z"));
        assert_eq!(b.next(mar) - mar, 31 * 24 * HOUR - HOUR);
    }

    #[test]
    fn a_year_bucket_is_a_calendar_year() {
        let b = TimeBucket::utc(BucketWidth::Months(12));
        let y = b.start_of(ts("2024-07-04T00:00:00Z"));
        assert_eq!(y, ts("2024-01-01T00:00:00Z"));
        assert_eq!(b.next(y), ts("2025-01-01T00:00:00Z"));
    }

    #[test]
    fn a_quarter_is_three_calendar_months_from_january() {
        let b = TimeBucket::utc(BucketWidth::Months(3));
        assert_eq!(
            b.start_of(ts("2024-05-17T00:00:00Z")),
            ts("2024-04-01T00:00:00Z")
        );
        assert_eq!(
            b.start_of(ts("2024-12-31T23:59:59Z")),
            ts("2024-10-01T00:00:00Z")
        );
    }

    /// The epoch is day zero, and it is a Thursday.
    ///
    /// Pinned because an off-by-one is invisible everywhere except in the
    /// weekday a `1w` bucket starts on.
    #[test]
    fn the_epoch_is_day_zero_and_a_thursday() {
        use jiff::civil::Weekday;
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(weekday(0), Weekday::Thursday);
        assert_eq!(
            weekday(DAY_ANCHOR * DAY),
            Weekday::Monday,
            "the multi-day anchor is a Monday"
        );
    }

    /// The civil-day conversion is exact over the whole range a bucket can
    /// meet, checked against jiff's own calendar: leap years, centuries, and
    /// both ends of the `i64` nanosecond range.
    #[test]
    fn civil_days_agree_with_the_calendar() {
        for days in (-106_751..=106_750)
            .step_by(if cfg!(miri) { 7_919 } else { 37 })
            .chain([-106_751, -1, 0, 59, 60, 10_957, 106_750])
        {
            let (y, m, d) = civil_from_days(days);
            let want = TimeZone::UTC.to_datetime(timestamp(days * DAY)).date();
            assert_eq!(
                (y, m, d),
                (
                    i64::from(want.year()),
                    u32::try_from(want.month()).unwrap(),
                    u32::try_from(want.day()).unwrap()
                ),
                "day {days}"
            );
            assert_eq!(days_from_civil(y, m, d), days);
        }
    }

    /// `index_of` counts buckets: consecutive buckets have consecutive
    /// ordinals, and two instants share one exactly when they share a bucket
    /// — for every width, zoned or not, with and without an origin.
    #[test]
    fn index_of_counts_buckets_on_the_calendar() {
        let mut cases = Vec::new();
        for w in ["15m", "6h", "1d", "1w", "1mo", "3mo", "1y"] {
            for z in [
                None,
                Some("Europe/Berlin"),
                Some("America/Santiago"),
                Some("Asia/Kolkata"),
            ] {
                let b = TimeBucket::parse(w, z).unwrap();
                cases.push(b.clone());
                cases.push(b.with_origin(ts("2021-02-14T06:30:00Z")).unwrap());
            }
        }
        let from = ts("2023-01-01T00:00:00Z");
        let to = ts("2029-01-01T00:00:00Z");
        for b in cases {
            // Walk bucket starts; probe each bucket at its start, middle
            // and last nanosecond.
            let mut start = b.start_of(from);
            let mut prev = b.index_of(start) - 1;
            let mut steps = 0;
            // Under Miri a few buckets per shape: it looks for UB, not
            // calendar coverage.
            let limit = if cfg!(miri) { 3 } else { 4_000 };
            while start < to && steps < limit {
                let next = b.next(start);
                let i = b.index_of(start);
                assert_eq!(i, prev + 1, "{b}: bucket at {start} skipped or repeated");
                assert_eq!(b.index_of(start + (next - start) / 2), i, "{b}");
                assert_eq!(b.index_of(next - 1), i, "{b}");
                prev = i;
                start = next;
                steps += 1;
            }
        }
    }

    /// The defect this exists for: a year in Berlin divided by its nominal
    /// width makes 2023 and 2024 one year.
    #[test]
    fn consecutive_berlin_years_and_months_are_consecutive_ordinals() {
        let y = TimeBucket::parse("1y", Some("Europe/Berlin")).unwrap();
        let years: Vec<i64> = (2019..=2030)
            .map(|yr| y.index_of(ts(&format!("{yr}-01-01T00:30:00+01:00"))))
            .collect();
        assert!(years.windows(2).all(|w| w[1] == w[0] + 1), "{years:?}");
        assert_eq!(years[0], 49, "1970 is ordinal 0");
        let m = TimeBucket::parse("1mo", Some("Europe/Berlin")).unwrap();
        let months: Vec<i64> = (1..=12)
            .map(|mo| m.index_of(ts(&format!("2024-{mo:02}-15T12:00:00+01:00"))))
            .collect();
        assert!(months.windows(2).all(|w| w[1] == w[0] + 1), "{months:?}");
    }

    /// Zone names resolve only against the compiled-in database: a path is
    /// not a zone, and `Etc/Unknown` is not a silent UTC.
    #[test]
    fn a_zone_name_is_never_a_path() {
        for bad in [
            "../../etc/passwd",
            "/etc/localtime",
            "Europe/../Berlin",
            "Etc/Unknown",
            "posixrules/../UTC",
        ] {
            assert!(TimeBucket::parse("1d", Some(bad)).is_err(), "{bad}");
        }
        // Case does not matter; the canonical spelling is kept.
        let b = TimeBucket::parse("1d", Some("europe/berlin")).unwrap();
        assert_eq!(b.timezone(), Some("Europe/Berlin"));
    }

    /// A week starts on a Monday, not on the Thursday the epoch happens to
    /// be.
    #[test]
    fn a_week_starts_on_monday() {
        let b = TimeBucket::utc(BucketWidth::Days(7));
        // 2024-03-15 is a Friday; its week began Monday 2024-03-11.
        let start = b.start_of(ts("2024-03-15T12:00:00Z"));
        assert_eq!(start, ts("2024-03-11T00:00:00Z"));
        assert_eq!(weekday(start), jiff::civil::Weekday::Monday);
    }

    #[test]
    fn an_anchored_calendar_bucket_stays_open_when_the_clock_repeats() {
        for width in ["1d", "1mo"] {
            let bucket = TimeBucket::parse(width, Some("Europe/Berlin"))
                .unwrap()
                .with_origin_str("2020-01-25T02:30:00")
                .unwrap();
            let first_0230 = ts("2026-10-25T00:30:00Z");
            assert_eq!(bucket.start_of(first_0230), first_0230);
            assert_eq!(bucket.start_of(ts("2026-10-25T01:15:00Z")), first_0230);
        }
    }

    #[test]
    fn buckets_tile_the_first_and_last_representable_instants() {
        for width in ["15m", "1w", "3mo"] {
            for zone in [None, Some("Europe/Berlin"), Some("America/New_York")] {
                let bucket = TimeBucket::parse(width, zone).unwrap();
                let first = bucket.start_of(i64::MIN);
                assert_eq!(first, i64::MIN, "{width} {zone:?}");
                let second = bucket.next(first);
                assert_eq!(bucket.start_of(second - 1), first, "{width} {zone:?}");
                assert_eq!(bucket.start_of(second), second, "{width} {zone:?}");
                assert_eq!(
                    bucket.next(bucket.start_of(i64::MAX)),
                    i64::MAX,
                    "{width} {zone:?}"
                );
            }
        }
        // MAX is 2262-04-11 23:47 UTC, already April 12 in Berlin.
        let day = TimeBucket::parse("1d", Some("Europe/Berlin")).unwrap();
        assert_eq!(day.start_of(i64::MAX), ts("2262-04-11T22:00:00Z"));
        assert!(day.clone().with_origin_str("1677-09-21T00:13:00").is_err());
        assert_eq!(
            day.with_origin_str("2262-04-12T00:00:00").unwrap().origin(),
            Some(ts("2262-04-11T22:00:00Z"))
        );
    }

    /// Every bucket is half-open and they tile the line with no gap and no
    /// overlap — the property every consumer of `start_of`/`next` assumes.
    #[test]
    fn buckets_tile_the_line() {
        let specs = [
            TimeBucket::fixed_ns(HOUR),
            TimeBucket::utc(BucketWidth::Fixed(15 * MIN))
                .in_zone(berlin())
                .unwrap(),
            TimeBucket::utc(BucketWidth::Days(1))
                .in_zone(berlin())
                .unwrap(),
            TimeBucket::utc(BucketWidth::Months(1))
                .in_zone(berlin())
                .unwrap(),
            TimeBucket::utc(BucketWidth::Days(7)),
            // An origin must not break the tiling — it only moves the phase.
            TimeBucket::fixed_ns(HOUR)
                .with_origin(ts("2020-01-01T00:17:00Z"))
                .unwrap(),
            TimeBucket::utc(BucketWidth::Days(1))
                .in_zone(berlin())
                .unwrap()
                .with_origin(ts("2020-01-01T05:00:00Z"))
                .unwrap(),
            TimeBucket::utc(BucketWidth::Months(1))
                .in_zone(berlin())
                .unwrap()
                .with_origin(ts("2020-01-15T00:00:00Z"))
                .unwrap(),
            TimeBucket::utc(BucketWidth::Days(7))
                .with_origin(ts("2020-01-01T00:00:00Z"))
                .unwrap(),
            // The hard one: a boundary at local 02:30, which **does not
            // exist** on Berlin's spring-forward day. The bucket that day has
            // to start somewhere, and the tiling must still hold across it.
            TimeBucket::utc(BucketWidth::Days(1))
                .in_zone(berlin())
                .unwrap()
                .with_origin(ts("2020-01-15T01:30:00Z"))
                .unwrap(),
        ];
        // A span that crosses both of Berlin's 2026 transitions and a
        // February.
        let from = ts("2026-01-15T00:00:00Z");
        let to = ts("2027-01-15T00:00:00Z");
        for b in specs {
            let mut cursor = b.start_of(from);
            let mut steps = 0;
            while cursor < to {
                let next = b.next(cursor);
                assert!(next > cursor, "{b} did not advance at {cursor}");
                // Every instant inside [cursor, next) belongs to this bucket.
                for probe in [cursor, cursor + (next - cursor) / 2, next - 1] {
                    assert_eq!(b.start_of(probe), cursor, "{b} at {probe}");
                }
                // …and the first instant of the next one does not.
                assert_eq!(b.start_of(next), next, "{b} boundary at {next}");
                cursor = next;
                steps += 1;
                assert!(steps < 40_000, "{b} is not advancing");
            }
        }
    }

    // ── Serialisation ──────────────────────────────────────────────────

    /// A rollup's definition survives a restart, zone and all — through the
    /// format the catalog actually uses.
    ///
    /// `postcard` is not self-describing, so a `skip_serializing_if` on the
    /// zone made every rollup in the catalog unreadable after the first one
    /// without a zone: the omitted field was still expected, and the bytes
    /// after it were read as the wrong thing. JSON hid it completely.
    #[test]
    fn a_bucket_round_trips_through_the_catalog_format() {
        for b in [
            TimeBucket::fixed_ns(15 * MIN),
            TimeBucket::utc(BucketWidth::Days(1)),
            TimeBucket::utc(BucketWidth::Days(1))
                .in_zone(berlin())
                .unwrap(),
            TimeBucket::utc(BucketWidth::Months(3))
                .in_zone(berlin())
                .unwrap(),
        ] {
            let bytes = postcard::to_allocvec(&b).unwrap();
            assert_eq!(postcard::from_bytes::<TimeBucket>(&bytes).unwrap(), b);
        }
        // …and a sequence of them, which is what the catalog stores: a
        // truncated field only shows up once something follows it.
        let seq = vec![
            TimeBucket::fixed_ns(15 * MIN),
            TimeBucket::utc(BucketWidth::Months(1))
                .in_zone(berlin())
                .unwrap(),
            TimeBucket::utc(BucketWidth::Days(7)),
        ];
        let bytes = postcard::to_allocvec(&seq).unwrap();
        assert_eq!(
            postcard::from_bytes::<Vec<TimeBucket>>(&bytes).unwrap(),
            seq
        );
    }

    /// A rollup's definition survives a restart, zone and all.
    #[test]
    fn a_bucket_round_trips_through_json_by_zone_name() {
        for b in [
            TimeBucket::fixed_ns(15 * MIN),
            TimeBucket::utc(BucketWidth::Days(1))
                .in_zone(berlin())
                .unwrap(),
            TimeBucket::utc(BucketWidth::Months(3))
                .in_zone(berlin())
                .unwrap(),
        ] {
            let json = serde_json::to_string(&b).unwrap();
            assert_eq!(
                serde_json::from_str::<TimeBucket>(&json).unwrap(),
                b,
                "{json}"
            );
            if let Some(name) = b.timezone() {
                assert!(json.contains(name), "the zone travels by name: {json}");
            }
        }
    }

    #[test]
    fn display_round_trips_through_parse() {
        for (width, zone) in [("15m", None), ("1d", Some("Europe/Berlin")), ("1mo", None)] {
            let b = TimeBucket::parse(width, zone).unwrap();
            assert_eq!(b.width().to_string(), width);
            assert_eq!(b.timezone(), zone);
        }
    }

    // ── Origin ─────────────────────────────────────────────────────────

    /// A shift that starts at 06:00 gets days that start at 06:00.
    #[test]
    fn a_day_bucket_takes_its_boundary_from_the_origin() {
        let b = TimeBucket::utc(BucketWidth::Days(1))
            .with_origin(ts("2020-01-01T06:00:00Z"))
            .unwrap();
        // 05:00 is still the previous shift.
        assert_eq!(
            b.start_of(ts("2024-03-15T05:00:00Z")),
            ts("2024-03-14T06:00:00Z")
        );
        // 06:00 opens the new one.
        assert_eq!(
            b.start_of(ts("2024-03-15T06:00:00Z")),
            ts("2024-03-15T06:00:00Z")
        );
        assert_eq!(
            b.next(ts("2024-03-15T06:00:00Z")),
            ts("2024-03-16T06:00:00Z")
        );
    }

    /// A billing period that runs from the 15th to the 15th.
    #[test]
    fn a_month_bucket_takes_its_day_from_the_origin() {
        let b = TimeBucket::utc(BucketWidth::Months(1))
            .with_origin(ts("2020-01-15T00:00:00Z"))
            .unwrap();
        assert_eq!(
            b.start_of(ts("2024-03-14T23:59:59Z")),
            ts("2024-02-15T00:00:00Z")
        );
        assert_eq!(
            b.start_of(ts("2024-03-15T00:00:00Z")),
            ts("2024-03-15T00:00:00Z")
        );
        // February is short, and the boundary is still the 15th.
        assert_eq!(
            b.next(ts("2024-01-15T00:00:00Z")),
            ts("2024-02-15T00:00:00Z")
        );
    }

    /// A day of month that does not exist in every month is refused rather
    /// than clamped: clamping makes the boundary drift or stop being a fixed
    /// day, and both are wrong answers delivered silently.
    #[test]
    fn a_monthly_origin_after_the_twenty_eighth_is_refused() {
        for day in ["2020-01-29", "2020-01-30", "2020-01-31"] {
            let err = TimeBucket::utc(BucketWidth::Months(1))
                .with_origin(ts(&format!("{day}T00:00:00Z")))
                .unwrap_err();
            assert!(err.0.contains("missing from some months"), "{}", err.0);
        }
        // 28 is fine — every month has one.
        assert!(
            TimeBucket::utc(BucketWidth::Months(1))
                .with_origin(ts("2020-01-28T00:00:00Z"))
                .is_ok()
        );
        // And a day width has no such restriction.
        assert!(
            TimeBucket::utc(BucketWidth::Days(1))
                .with_origin(ts("2020-01-31T00:00:00Z"))
                .is_ok()
        );
    }

    /// For a multi-unit width the origin picks *which* unit opens a bucket,
    /// not only the phase inside it.
    ///
    /// The documentation says so because the one-unit case ("any date with
    /// the right day will do") is the intuition, and it is wrong here: two
    /// origins that differ only in month give different quarters.
    #[test]
    fn a_multi_unit_origin_selects_which_unit_opens_a_bucket() {
        let jan = TimeBucket::utc(BucketWidth::Months(3))
            .with_origin(ts("2024-01-15T00:00:00Z"))
            .unwrap();
        let feb = TimeBucket::utc(BucketWidth::Months(3))
            .with_origin(ts("2024-02-15T00:00:00Z"))
            .unwrap();
        let probe = ts("2024-05-20T00:00:00Z");
        assert_eq!(jan.start_of(probe), ts("2024-04-15T00:00:00Z"));
        assert_eq!(feb.start_of(probe), ts("2024-05-15T00:00:00Z"));

        // A one-unit width has no such choice: only the phase is left, so
        // two origins a year apart agree.
        let a = TimeBucket::utc(BucketWidth::Months(1))
            .with_origin(ts("2024-01-15T00:00:00Z"))
            .unwrap();
        let b = TimeBucket::utc(BucketWidth::Months(1))
            .with_origin(ts("2020-07-15T00:00:00Z"))
            .unwrap();
        assert_eq!(a.start_of(probe), b.start_of(probe));
    }

    /// The origin is read in the bucket's own zone, so the same instant
    /// gives a different local boundary in a different zone.
    #[test]
    fn the_origin_is_read_in_the_buckets_zone() {
        // 04:00Z is 05:00 in Berlin (winter).
        let utc = TimeBucket::utc(BucketWidth::Days(1))
            .with_origin(ts("2024-01-01T04:00:00Z"))
            .unwrap();
        let berlin = TimeBucket::utc(BucketWidth::Days(1))
            .in_zone(berlin())
            .unwrap()
            .with_origin(ts("2024-01-01T04:00:00Z"))
            .unwrap();
        assert_eq!(
            utc.start_of(ts("2024-01-10T12:00:00Z")),
            ts("2024-01-10T04:00:00Z")
        );
        // Local 05:00 in Berlin is 04:00Z in winter.
        assert_eq!(
            berlin.start_of(ts("2024-01-10T12:00:00Z")),
            ts("2024-01-10T04:00:00Z")
        );
        // …and 03:00Z in summer, because the boundary is a local time.
        assert_eq!(
            berlin.start_of(ts("2024-07-10T12:00:00Z")),
            ts("2024-07-10T03:00:00Z")
        );
    }

    /// A fixed width tiles outwards from the origin in both directions.
    #[test]
    fn a_fixed_bucket_tiles_outwards_from_its_origin() {
        let b = TimeBucket::fixed_ns(HOUR)
            .with_origin(ts("2024-01-01T00:30:00Z"))
            .unwrap();
        assert_eq!(
            b.start_of(ts("2024-01-01T01:00:00Z")),
            ts("2024-01-01T00:30:00Z")
        );
        // Before the origin, too.
        assert_eq!(
            b.start_of(ts("2023-12-31T23:00:00Z")),
            ts("2023-12-31T22:30:00Z")
        );
    }

    /// The origin survives the format the rollup catalog actually uses.
    #[test]
    fn an_origin_round_trips_through_the_catalog_format() {
        let b = TimeBucket::parse("1mo", Some("Europe/Berlin"))
            .unwrap()
            .with_origin(ts("2020-01-15T00:00:00Z"))
            .unwrap();
        // In a sequence, so a short read shows up as a corrupted neighbour.
        let seq = vec![b, TimeBucket::fixed_ns(HOUR)];
        let bytes = postcard::to_stdvec(&seq).unwrap();
        let back: Vec<TimeBucket> = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back, seq);
        assert_eq!(back[0].origin(), Some(ts("2020-01-15T00:00:00Z")));
    }

    /// A catalog entry carrying an impossible origin is refused on the way
    /// in, not clamped.
    ///
    /// The deserialiser is a constructor: nothing else guards a bucket read
    /// back from the rollup catalog, and a silently clamped boundary is a
    /// tier that aggregates the wrong window for as long as it exists.
    #[test]
    fn a_deserialised_origin_goes_through_the_same_rule() {
        // Hand-built JSON, the way a corrupted or edited catalog would be.
        let json = r#"{"width":{"Months":1},"timezone":null,"origin":1706659200000000000}"#;
        let err = serde_json::from_str::<TimeBucket>(json).unwrap_err();
        assert!(
            err.to_string().contains("missing from some months"),
            "{err}"
        );
        // 2024-01-31 is day 31; the 28th is fine.
        let ok = r#"{"width":{"Months":1},"timezone":null,"origin":1706400000000000000}"#;
        assert!(serde_json::from_str::<TimeBucket>(ok).is_ok());
    }

    /// A zero or negative width is refused where the width is *named*, which
    /// is why nothing downstream has to re-check it.
    #[test]
    fn a_width_of_zero_is_refused() {
        for spec in ["0s", "0d", "0mo", "-1h"] {
            assert!(
                spec.parse::<BucketWidth>().is_err(),
                "'{spec}' should be refused"
            );
        }
    }
}
