+++
title = "Your first gateway monitor"
description = "Build a solar-roof health monitor with Chronix: a declared detector that judges each quarter hour on the wall clock and resumes after a reboot."
weight = 30
+++

A gateway reboots, loses power and runs for years without anyone watching.
This tutorial declares a detector that judges a PV array quarter hour by
quarter hour against its own baseline, inside the database, and keeps judging
across restarts and clock changes.
The complete program is
[`examples/declared_detector.rs`](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/declared_detector.rs):

```bash
cargo run -p chronix --example declared_detector
```

## Declare the detector

A detector from `create_detector` is an object your program drives: it owns
the state, the schedule and the persistence. That is the hard part on a box
that reboots. A **declared** detector is the other shape — the one a rollup
has. You declare it once on a source measurement; the store persists it,
advances it on every maintenance pass, writes its verdicts as ordinary rows,
and after a restart **resumes** rather than relearns.

```rust
use chronix::detector::{DetectorDeclaration, Season};
use chronix_core::timebucket::TimeBucket;

let berlin = |w| TimeBucket::parse(w, Some("Europe/Berlin"));
db.declare_detector(
    DetectorDeclaration::builder("roof_health")
        .source("pv_ratio", "value")                         // a series you write
        .season(Season::new(berlin("15m")?, berlin("1d")?)?) // 14:00 vs 14:00
        .forgetting(7)     // a slot's past halves every week: follow the year
        .min_samples(7)    // a week of points before a slot judges
        .zscore(5.0)       // one quarter hour far outside its band
        .cusum(0.5, 8.0)   // or a persistent drop
        .build()?,
)?; // idempotent: declare it on every start
```

Declared detectors are part of the `analytics` feature, which is on by
default; a build with `default-features = false` names it —
`features = ["analytics"]` — and still compiles without DataFusion.

Every point after a slot's warm-up becomes one row in `roof_health_verdicts`,
tagged with the source series' tags and `detector=roof_health`:

| Field | Meaning |
|---|---|
| `value` | The point that was judged |
| `baseline`, `stddev` | This slot's normal before the point was folded in |
| `z` | `(value − baseline) / stddev` |
| `cusum_pos`, `cusum_neg` | The CUSUM sums after the point |
| `anomaly` | Whether anything flagged it |
| `reason` | `zscore`, `cusum_low`, `cusum_high`, or empty |
| `slot` | The slot that judged it — `56` is 14:00 in a quarter-hour day |

```sql
SELECT _time, slot, value, baseline, z, reason
FROM roof_health_verdicts WHERE anomaly ORDER BY _time DESC LIMIT 20
```

## The seasonal baseline

A `Season` is `slot`-wide slots within each `period`, both
[`TimeBucket`s](@/reference/data-model.md) in one time zone: fifteen-minute
slots in a local day, days in a week or a month, months in a year. Each slot
keeps its own running mean and variance, so 03:00 is compared with 03:00 and
December with December — a z-score against one flat mean would flag every
winter.

**A slot is a position on the wall clock.** In Europe/Berlin, 14:00 is slot
56 on every day of the year, including the 23-hour day in March and the
25-hour day in October. The autumn night's repeated hour folds into the
slots of 02:00–02:59; the spring night's skipped hour leaves its four slots
empty that day. June is slot 5 of a year in every year. Slots must tile the
period (15 m in a day, a day in a week or month, a month in a year); a season
with more than 2 048 slots is refused.

**Choose the zone for what drives the signal.** The local zone suits
anything people and tariffs shape: consumption, charging, a heat pump's
schedule. For anything the sun shapes — shading, dawn, a PV array's daily
curve — declare the season in UTC: solar time does not move when the clocks
do, so a chimney's shadow stays in the same UTC slots all year.

- **Warm-up.** A slot scores nothing until it has seen `min_samples`
  (default 3). Nothing is flagged on thin evidence. `warming_slots` in
  `detectors()` counts the slots that have data and are not there yet — the
  answer to "why are there no verdicts?".
- **Outliers do not teach.** A point the z-score test flags is not folded
  into its slot, so one fault cannot widen the band that should catch the
  next one.
- **Slow change.** `.forgetting(n)` halves the weight of what a slot learned
  every `n` periods — exactly once per calendar period — so a baseline
  follows the year. A slot fed once a period then holds about `1.44·n + 0.5`
  points, so keep `min_samples` below that or it never judges.
- **One baseline per series**, or per combination of `.group_by([...])`
  tags. `.where_tag(k, v)` confines a detector to some rows of its source.

## A derived signal: materialise it first

What you usually want to watch is not a stored column. A PV array's raw
output flags every cloudy day; what says "the roof is degrading" is the
**performance ratio**, actual over what a model of a healthy array predicts.
Write that ratio as its own measurement, one point per quarter hour, and
declare the detector on it — the source of a detector is any measurement,
including one the application computes. The runnable
[`declared_detector`](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/declared_detector.rs)
example does exactly this over a simulated year of Europe/Berlin quarter
hours: no false alarm across both clock changes, and a 15 % degradation
caught within hours.

## Restarts, late points and backlogs

- **Restart.** The baseline, the CUSUM sums and the watermark are persisted
  in the catalog after the verdicts they describe are durable — so a crash
  between the two re-judges a few points (identically) and never skips one.
  Reopening resumes where the last pass stopped.
- **Late points.** The detector judges points older than `settle` (default:
  twice `maintenance_interval`) once, in order. A point that arrives at or
  below the newest timestamp already judged is **not** judged and is counted
  — `late_points` in `detectors()`, and
  `chronix_detector_late_points_total{detector}`. A verdict is a judgement
  somebody may already have acted on; rewriting it is worse than missing a
  late point. If the counter climbs, raise `settle` above the source's
  delivery delay.
- **Backlogs.** A gateway back online after a week judges its backlog over
  several passes, at most `max_points_per_pass` (default 100 000) each.
- **Budget.** A detector's state is capped at 1 MiB. A series that would
  exceed it is not admitted, and the detector reports `Degraded` with the
  reason; narrow `group_by` or widen the slot.

`db.detectors()` lists every declaration with its watermark, series count,
late points, warming slots and status (`Active`, `Inactive` with a reason — no data yet, or
a build without the `analytics` feature, which keeps declarations intact —
or `Degraded`). `db.drop_detector(name)` removes a declaration and its state;
its verdicts stay. On `chronixd` the same lives at
[`/api/v1/detectors`](@/reference/http-api.md#declared-detectors).

## Next

- [Rollups and retention](@/guides/rollups-and-retention.md) — keep raw data for days and summaries for years
- [Run on a gateway](@/guides/run-on-a-gateway.md) — memory, flash and the small preset
- [Anomaly detection](@/explanation/anomaly-detection.md) — how each detector works
