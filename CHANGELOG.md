# Changelog

Notable changes to Chronix. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/). Chronix is pre-1.0:
per [CONTRIBUTING.md](CONTRIBUTING.md), a breaking change bumps the minor
version and a fix bumps the patch — there is no stability promise before 1.0,
and no migration tooling for the on-disk format.

## [0.7.0] - 2026-09-14

**The on-disk format is unchanged — existing data directories open normally.**
This release is breaking for the *API*, not for the bytes. The format-changing
work is deliberately batched into one later break, so that it happens once and
the format then freezes at 1.0.

### Changed

- **One policy for the four on-disk format versions**, in
  `chronix_engine::format`: `.csx` segments, the write-ahead log, the catalog
  snapshot and the `.series` sidecar. All four stay at version **1** — nothing
  here changes the bytes — but they are now checked the same way and reported
  the same way.

  The refusal itself was the reason to do this now. The four formats had four
  version constants, three different comparisons and four different error
  texts. The catalog compared with `>`, so an **older** snapshot was silently
  accepted by a reader with no code to interpret one. The `.series` sidecar
  reported a version mismatch as `IndexError::Corrupt`, which sends an
  operator who has just upgraded chronix looking for a failing disk — the
  bytes are intact and the remedy is to match the versions up. And three of
  the four printed a bare number, which is the first error somebody meets
  before they have any other information.

  There is now one policy in `chronix_engine::format`: equality in both
  directions, a dedicated `UnsupportedVersion` on each error enum — never a
  corruption variant — and one message naming both versions and what to do.
  Guarded by `no_durable_reader_reports_a_version_mismatch_as_corruption`,
  which reads the code rather than the comments, after its first version
  tripped over its own justification for the rule.

  **The versions move when the bytes move, and not before.** A bump with no
  layout change invalidates every directory in exchange for nothing; a layout
  change with no bump is worse, because an old directory then has matching
  magic and a matching version, so the reader walks into a layout it does not
  understand and the failure arrives as a CRC error or a decode error — the
  very confusion this policy removes.

- **BREAKING — `chronix-analytics` is behind the facade's `analytics`
  feature**, on by default. `default-features = false` now drops the largest
  crate in the workspace — forecasting, anomaly detection, preprocessing, the
  model registry — along with `sha2`, for a consumer that only stores and
  queries. `sql` implies it, because the SQL window functions and forecast
  aggregates are wrappers over those kernels.

  It was unconditional while `chronix-security` and `chronix-streaming` were
  both optional, and the design partner asked the question nobody had: why is
  the biggest sub-crate the only engine crate I cannot turn off? `rayon` is
  **not** removed by this — the scan path uses it to read segments in parallel
  above a threshold — which is worth knowing before anyone expects otherwise.

- **BREAKING — the six accessors that handed out the engine's internal locks
  are gone:** `Chronix::catalog`, `bloom_filters`, `rollup_registry`, `wal`,
  `shards` and `tag_index`. Five had no caller anywhere in this repository.

  They were not merely untidy. `CatalogLock` lives in a `pub(crate)` module,
  so a caller could invoke the method and could **not name what came back** —
  no struct field, no signature, no annotated `let`. The lock hierarchy is
  documented as an invariant of this crate's implementation rather than a
  contract with callers, and was then enforced on callers by a debug-build
  panic naming levels they had no way to read: an external program doing two
  ordinary reads in the wrong order panicked on the second. In release the
  assertion compiles away and the same two reads deadlock against the
  maintenance thread.

  **Replacement:** `Chronix::segment_count()`, `segments()` and
  `segments_of(measurement)`, returning owned `SegmentInfo` values taken under
  the lock and handed back after it is released. Every call site in this
  repository got shorter.

- **BREAKING — `MultivariateForecastResult` names its rows.** New fields
  `series` (one name per row of `predictions`) and `target` (the series
  `fit` was asked for), and new methods `for_series()` and
  `target_forecast()`.

  `MultivariateForecastModel::fit(ctx, target_idx)` documents `target_idx` as
  the series to forecast. `MultiLinearRegression` honoured it and returned one
  row; `VarModel` bound it `_target_idx`, discarded it, and returned one row
  per series — so generic code reading `predictions[0]` got the requested
  series from one implementor and series index 0 from the other. Asking for a
  series whose values sit around 50 000 returned **12.00**, which is a
  plausible number belonging to a different series. `VarModel` now also
  validates the index it used to ignore.

- **BREAKING — duplicate public type names disambiguated.**
  `preprocess::DriftReport` → `ClockDriftReport` (a sensor's clock drifting),
  `lifecycle::DriftReport` → `ModelDriftReport` (a model's accuracy drifting),
  and `lifecycle::registry::AccuracyMetrics` → `VersionAccuracy` (distinct
  from the crate-root `AccuracyMetrics`, which is a forecast scorecard).

- **BREAKING — `chronix_analytics::multivariate::granger` is deleted.** It was
  a second implementation of Granger causality in the same crate, with a
  second result type, and **no consumer** — nothing in the facade, the server,
  the examples or the tests referenced it. Run against
  `VarModel::granger_causality` on the same data at three lag orders the two
  agreed to 5×10⁻⁴, so this was a duplicate rather than a divergence. Use
  `VarModel::granger_causality` / `granger_causality_robust`.

- **The workspace is on Rust edition 2024.** MSRV is unchanged at 1.94. The
  migration paid for itself immediately: `std::env::set_var` is `unsafe` in
  2024 because it mutates state every thread shares, and six calls sat in
  `cargo test`'s parallel harness, racing any concurrently running test that
  reads the environment. They are gone rather than annotated — the variable
  substitution takes its lookup as an argument, so the tests touch no global
  state.

### Added

- **Native histograms.** A whole distribution as one sample, end to end:
  `FieldValue::Histogram`, `ColumnType::Histogram` and
  `chronix_core::histogram::Histogram` with the Prometheus model — schemas −4
  to 8 plus −53 for custom boundaries, exponentially-interpolated quantiles,
  fractions, moments and merge.

  - **Storage.** Survives the memtable, a flush and a restart; keeps custom
    boundaries; coexists with scalar columns. A histogram with unsorted
    buckets is refused at `Point::new` rather than interpolating to a
    plausible wrong quantile later.
  - **Rollups.** A tier over a histogram column **merges** it: `sum` is the
    merge, `avg` scales by 1/n, `min`/`max` are NULL, and two schemas in one
    bucket are refused rather than silently re-bucketed.
  - **PromQL.** `histogram_count`, `histogram_sum`, `histogram_avg`,
    `histogram_stddev`, `histogram_stdvar` and `histogram_fraction`;
    `histogram_quantile` dispatches between native and classic `le` input.
    The rest refuse a float series **by name** — `histogram_count` of a gauge
    is not the gauge.
  - **Ingest.** Remote write **1.0 and 2.0**, selected by the `Content-Type`
    `proto=` parameter (`prometheus.WriteRequest` or
    `io.prometheus.write.v2.Request`); anything else is `415` naming both. A
    2.0 write answers `X-Prometheus-Remote-Write-Samples-Written`,
    `-Histograms-Written` and `-Exemplars-Written` — the last always `0`,
    since Chronix stores no exemplars. OTLP **exponential** and
    **explicit-bucket** histograms both land as histogram columns.
  - **Remote read** returns histograms, so a federating Prometheus reading
    Chronix as long-term storage gets the distribution back.
  - **The classic names.** `foo_bucket`, `foo_count` and `foo_sum` read a
    stored histogram as a read-time view — nothing stored twice — so an
    existing panel needs no change. They are resolvable but not *listed*, so
    `{__name__=~".+"}` cannot return the same observations three times. A
    measurement with a real float field called `count` keeps it.

    The `le` values are the boundaries the histogram **has**. Custom-bucket
    data (OTLP explicit-bucket, or a classic histogram) carries the
    instrumentation's own boundaries, so an existing `le="0.5"` matches; an
    exponential schema carries powers of `2^(2^-n)`, so it does not, and the
    query returns empty rather than the nearest bucket.

- **`EncodingType::Bytes`** — a length-prefixed framing for columns whose
  values are composite objects with their own encoding. Deliberately not a
  clever codec: a histogram is already packed, and a second-guessing layer
  over an opaque blob would compress nothing while adding a decoder to audit.
  It joins the fuzz corpus and the adversarial proptests by construction,
  because both are generated from the encoding enum.

- **`Chronix::segments()`, `segments_of()` and `segment_count()`**, returning
  `SegmentInfo` — measurement, absolute path, rows, series, time bounds and
  size on disk. Sorted, so two calls on an unchanged database compare equal.
- **`chronix_engine::format`** — the four format versions and the single rule
  for checking them, with the policy written down where the next person
  changing a format will read it.
- **The changelog travels with the published crate.** `cargo package` includes
  nothing from outside a package directory, so a root-level `CHANGELOG.md` is
  invisible on crates.io and docs.rs — and the reader who most needs it is the
  one whose `cargo update` just moved them a minor version. The design partner
  proposed `include = ["../../CHANGELOG.md"]`; that does **not** work, and the
  way it fails is the problem — cargo drops a path outside the package root
  with no error and no warning. `scripts/publish-crate.sh` now copies the root
  changelog into the crate directory for the duration of the publish and
  removes it after, so the tarball carries it and the repository still has
  exactly one.
- **`scripts/check-references.sh` resolves the evidence citations too.** The
  architecture notes carry a table whose premise is that every measured claim
  names the test pinning it — and nothing checked that the named test exists.
  Two did not: one had been renamed, and one had never existed while the
  property it claimed was real and pinned elsewhere. A dangling citation is
  worse than an absent one, because an absent one reads as a gap and a
  dangling one reads as evidence.

- **`scripts/check-docs.sh` gained two checks.** §13 refuses two public types
  with the same name inside one crate — three pairs existed, each locally
  sensible and each invisible until a caller holds both. And the
  internal-reference check now covers `CHANGELOG.md`, because the changelog
  ships inside the crate as of this release and a reader with the tarball
  cannot open a gitignored note.

  The changelog joins *that* check and not the others, which is the
  interesting half: most checks in that script ask "is this still true?", and
  a changelog exists to record states that are deliberately no longer true.
  Adding it to the general list immediately flagged an entry describing the
  release that **fixed** a wrong port, for naming the wrong port.

- **`crates/chronix/tests/public_api.rs` pins the handle's surface.** It
  pinned the crate root's re-exports and public modules and never the methods
  on `Chronix`, which is what a caller touches — and which had reached eighty.
  Two new tests: a list every addition must join deliberately, and a
  structural check that no public method returns one of the engine's ordered
  locks.
- **`crates/chronix/tests/promql_surface.rs`** states the PromQL surface
  against a **named** Prometheus version — 3.14 — and asserts it in both
  directions: every function listed as implemented is accepted by the
  evaluator, and every function listed as absent is refused **by name** rather
  than answering something plausible. A third test refuses to let a name
  appear in both lists.

  The documentation used to say "what remains of the 3.x surface is native
  histograms and the experimental `limitk` / `limit_ratio`" — an
  exhaustive-sounding list that stopped being exhaustive three Prometheus
  releases later, when 3.5 through 3.14 added nine functions. "3.x" is not a
  version, and a claim about somebody else's project kept in prose decays
  silently. Each absent entry now carries its reason, because an unimplemented
  list without one is a to-do list rather than a set of decisions.

- **`crates/chronix-analytics/tests/multivariate_contract.rs`** — what one
  call to `MultivariateForecastModel` promises, asked of every implementor:
  that `target_forecast()` answers for the series the caller named, that every
  row says which series it is, and that an out-of-range target index is
  refused.

### Changed — BREAKING

- **An OTLP explicit-bucket histogram is now one histogram column**, not
  `count`, `sum` and a `bucket_<bound>` field per boundary. It lands as schema
  −53 in the metric's own `value` column and answers the whole `histogram_*`
  family — the field expansion stored the data and answered nothing, because
  `histogram_quantile` had no bucketed series to read.

  **Migration:** `bucket_<bound>` fields are gone. Use
  `histogram_quantile(0.99, your_metric)`, or the classic names —
  `your_metric_bucket{le="0.5"}`, `your_metric_count`, `your_metric_sum`. Note
  the spelling: the old expansion produced `your_metric_bucket_0.5` as a
  *metric name*; the exposition produces `your_metric_bucket` with an `le`
  label, which is what Prometheus has always meant by a bucket series.

- **An unset OTLP `sum` is now absent rather than zero.** `HistogramDataPoint`,
  `SummaryDataPoint` and `ExponentialHistogramDataPoint` declare `sum` as
  `optional`, as upstream does. Without the presence bit an unset sum decoded
  as `0.0` — and OTLP leaves it unset precisely when the instrument recorded
  negative events, so `histogram_avg` reported a number that was wrong and
  indistinguishable from a real one.

### Added — server

- **`ServerError::UnsupportedMediaType`**, answering `415`. Distinct from
  `BadRequest` because a sender acts on them differently: `400` says "your data
  is wrong, do not retry", `415` says "I do not speak this dialect, send
  another".

### Fixed — testing

- **Two test helpers leaked a temporary directory on every run.**
  `promql_surface.rs` and `chronixd`'s `auth.rs` both ended in
  `std::mem::forget(dir)` so a database could outlive the helper — which works,
  and leaves a directory of database files behind on every run, on every
  machine. Between them they filled a disk during this pass. Both now return
  the directory beside the handle, and `scripts/check-docs.sh` §14 refuses the
  shortcut — `into_path()` and `keep()` included, since they disarm the same
  cleanup by another name.

### Fixed

- **Every Granger causality p-value was wrong.** `chronix-analytics` carried
  two `ln_gamma` implementations, and the one in `multivariate::mv_forecast`
  had the g = 7 Lanczos coefficients with a `t = x + 6.5` offset — g = 6.
  Mismatched parameters are not a precision problem: that copy returned
  **0.928** for `ln Γ(1)`, which is 0. It sits under `ln_beta` →
  `regularized_incomplete_beta` → `f_distribution_sf`, so
  `VarModel::granger_causality` and its HC3-robust sibling reported
  significance from a broken survival function — `F(2,1)` at 0.01 answered
  0.9966 where the truth is 0.9901.

  The correct `ln_gamma`, with a passing known-values test, was one module
  away in `forecast::diagnostics`. There is now one, `pub(crate)`, and
  `chronix-analytics --test one_definition` counts the definitions of every
  special function in the crate so a second copy cannot reappear.

  `f_distribution_sf` is pinned two ways: five **closed forms** — `F(2,d₂)`,
  `F(d₁,2)`, `F(1,1)`, the reciprocal identity `sf_{F(d₁,d₂)}(x) = 1 −
  sf_{F(d₂,d₁)}(1/x)`, and survival-function bounds — and a **392-point**
  generated `scipy.stats.f.sf` table for the parameter pairs with no
  elementary form. A closed form is ground truth rather than a second opinion,
  and reproduces with nothing installed.

- **`HoltWintersModel::fit` panicked on an empty series.** It indexed
  `values[0]` on a method that returns `Result`, so an empty measurement took
  down the host process — which, in an embedded database, is the caller's
  daemon. The check went into `validate_input`, the one function every
  forecasting model calls, because the other five survived the same input only
  by luck: their fitting loops are no-ops over an empty slice, and they then
  answered `predict()` from an uninitialised level. Fitting an empty series is
  now an error for all of them, with a message naming the problem.

  Found by pushing eight degenerate series — empty, one point, all-NaN,
  constant, infinite, extreme, subnormal, and a seasonal period longer than the
  data — through the public surface. `chronix-analytics --test
  degenerate_input` keeps the question asked, of every model *and* every
  anomaly detector; the detectors were clean.

- **`WalError::UnsupportedVersion` and `IndexError::UnsupportedVersion`** are
  new variants, distinct from `InvalidHeader` and `Corrupt`. A log or sidecar
  from another format generation is well-formed data the reader cannot
  interpret, not damage.

## [0.6.0] - 2026-09-13

### Added

- **`scripts/check-docs.sh` checks that every `pub` field of a config struct
  is read.** The existing check asks the question from the documentation's
  side — of the keys a TOML block shows, which does no code read? — and a
  field can be absent from the documentation and still be accepted from a
  file, which is exactly how the four settings above survived. The
  discriminator is field *access*, so a setting translated into an engine
  value inside its own config module still counts as read.
- **`Chronix::rollup_where(name, start, end, tags)`** — the rollup view
  restricted to the series matching `tags`, asked for by the `hems` design
  partner: without it a caller reads every tag group and discards all but one,
  which is bounded for a handful of measurement points and a whole-group-set
  materialisation for anyone with real cardinality. `rollup()` is unchanged
  and delegates to it.

  Every key must be one of the rollup's `group_by_tags`, and anything else is
  refused by name. The target measurement carries only the tags the rollup
  grouped by, so a filter on any other key would narrow the live half — read
  from the source, which still has every tag — and match nothing in the
  materialised half, giving an answer short on one side of the watermark and
  whole on the other.
- **Robust STL** — `StlConfig::robust()`, and an optional `robust` flag on
  `stl_trend`, `stl_seasonal`, `stl_residual` and `stl_decompose` in SQL. It
  runs Cleveland et al. (1990) §4.3's outer loop, reweighting each point by a
  bisquare of its residual so an outlier stops pulling on the fit. The case
  for it is ordinary in a metrics database: a restart, a scrape backfilled as
  one enormous sample, a sensor returning its error sentinel. Without it one
  such point is spread across the *whole* seasonal component — the
  cycle-subseries smoother sees it in the same season of one cycle — and took
  the seasonal amplitude of a 10-unit cycle to 33.7. Default off, as in R's
  `stl` and statsmodels' `STL`; it costs about eleven times the iterations.
  `StlConfig::low_pass_window` and `with_outer_iterations` are new beside it.
- **`chronix/tests/analytics_null_semantics.rs`** — every analytics window
  function driven with a gap mid-partition, with the list taken from the
  session's own registry, so a new kernel cannot be added without saying what
  it answers across one.

### Fixed

- **STL decomposition lost 11–22 % of the seasonal amplitude, and invented a
  seasonal component for a straight line.** Three defects compounded.
  `loess_smooth` returned the plain mean whenever its window covered the data
  — a degree-1 local regression silently becoming degree-0. The seasonal
  window defaulted to `max(7, period)`, reading a quantity measured in
  **cycles** as if it were measured in samples: the cycle-subseries smoother
  runs along one point per cycle, so seven days of hourly data with a daily
  cycle asked for a 25-point smoother over a 7-point subseries. And the
  low-pass filter padded by copying the first and last cycle, which is correct
  only for a series that is already periodic. Consequences: any series with
  fewer cycles than its period lost amplitude into the trend, and the straight
  line `1000 + 37i` — whose seasonal component is exactly zero — came out with
  a seasonal swing of 278, which the seasonal-strength measure read as 0.81
  against a 0.64 threshold, so `auto_forecast` **seasonally differenced every
  counter it was shown**.

  Now Cleveland et al. (1990) as the paper specifies it: the cycle-subseries
  smoother is extended one cycle beyond either end by evaluating its own loess
  at `−1` and at `len`, and the low-pass is `MA(p) → MA(p) → MA(3) →
  loess(n_l)` in valid mode, which consumes exactly the `2p` points the
  extension added. `StlConfig::seasonal_window` is in **cycles** and defaults
  to 7, matching R's `stl` and statsmodels' `STL`; `low_pass_window` is new.
  Components match statsmodels' `STL` to three decimals.
- **A NULL nulled the rest of its partition in every rolling analytics
  function.** `rolling_mean`, `rolling_std`, `rolling_corr` and `ewm` folded
  the missing value into a sliding accumulator, so one gap made every later
  row NULL — and an exact recompute every 1024 steps then silently healed it,
  which made the damage *length* depend on where the gap fell. `zscore`
  nulled the whole partition from one gap. The `stl_*` kernels *removed* the
  row, shifting every row after it into the previous season. And
  `multivariate_anomaly` scored a row with no observation `0.0` — the
  distribution's exact centre, the most normal answer it has.

  One rule now: a NULL is a missing **sample**, not a missing row. It is
  excluded from every statistic, and a row still gets an answer wherever one
  can be computed from samples that exist — so `rolling_*`, `ewm`,
  `correlation` and `cross_correlation` answer at a gap row from the samples
  around it, exactly as `AVG(v) OVER (ROWS n PRECEDING)` does, while `diff`,
  `pct_change`, `zscore`, `anomaly_score`, `multivariate_anomaly` and the
  `stl_*` family are NULL there. All seven rolling and smoothing functions now
  match pandas exactly on input containing NULLs.
- **`ljung_box` reported a p-value 11 % wrong.** The Q statistic was exact;
  the χ² survival function under it was not. `gamma_cf` carried the partial
  numerators of the continued fraction for the incomplete *beta* function
  against the incomplete *gamma*'s denominators, and guarded with
  `x.max(tiny)`, which turns a legitimate negative denominator into `+1e-30`.
  Only the tail branch was affected — the half where p-values live. Now
  Numerical Recipes' `gcf` with magnitude guards, pinned against
  `scipy.stats.chi2.sf` at 84 points across both branches;
  `f_distribution_sf` is pinned at 288 against `scipy.stats.f.sf` beside it.
- **The nightly fuzz job had never run.** `fuzz/` is its own build — nightly,
  sanitizer instrumentation, its own `RUSTFLAGS` — but its manifest declared
  no `[workspace]` and the root listed it in neither `members` nor `exclude`,
  which cargo refuses outright. Every scheduled run failed before compiling a
  line. The three targets now build and run; `preflight.sh` builds them when
  a nightly toolchain and `cargo-fuzz` are present, so the next such breakage
  is caught before the schedule.
- **ARIMA estimated mixed models wrongly.** `ArimaModel` held its
  autoregressive block at the Burg seed and optimised only the moving-average
  coefficients against it — and a Burg AR fitted to mixed ARMA data is biased,
  because the MA term drags the lag-1 autocorrelation away from φ. On 2 000
  points of `x = 0.6x[t-1] + e - 0.4e[t-1]` it returned φ = 0.22, θ = -0.02
  where the maximum likelihood is 0.571 and -0.367. Pure AR and pure MA were
  unaffected, which is why it went unnoticed: those are the two orders with
  nothing to interact with, and `auto_forecast` searches the mixed ones.

  Every block is now refined together, by the same `minimise_css` that
  `SarimaModel` already used. Estimates match `statsmodels`' maximum
  likelihood to four decimals on AR(1), MA(1), ARMA(1,1) and ARIMA(1,1,1).
- **A fitted ARIMA could be non-stationary, and the forecast then diverged.**
  What kept the AR polynomial stable was the Burg seed, not the ±0.99 box on
  each coefficient — so once the optimiser could move that block, 21 of 80
  fits on a random walk landed outside the stable region, `φ = (0.99, 0.012)`
  with a root at 0.9985. The box was excluding legitimate models at the same
  time: `φ = (1.2, -0.4)` is an ordinary stationary AR(2) it cannot represent.

  Stationarity and invertibility now hold **by construction**: the optimiser
  searches unconstrained reals and the Jones (1980) reparameterisation maps
  them through partial autocorrelations to a polynomial whose roots lie
  outside the unit circle for any order — as `statsmodels`'
  `enforce_stationarity` does. Estimates are unchanged where the box was not
  binding.
- **Naming a rollup wrong was a `500`.** `create_rollup` collapsed every
  `RollupError` into `DbError::Internal`, so `AlreadyExists` arrived as
  `Internal("rollup registration failed: rollup 'x' already exists")` — a
  redacted `500` on the wire, and a string match for anyone trying to tell
  "already there" from "failed". Declaring a tier on every start is idempotent
  by nature, because the registry is persisted and every run after the first
  meets its own rollup, so the conflict is a *normal* path. Re-derived from
  the report, the mirror case was the same: a rollup name that does not exist
  was also `Internal`. `DbError::NotFound` and `DbError::Conflict` are new and
  render `404` and `409`. Reported by the `hems` design partner.
- **A malformed series key and an unknown model or detector were `500`s too.**
  Found by asking the same question of the rest of the facade: `invalid series
  key` on the query and delete paths, and `forecast: unknown model` /
  `detect_anomalies: unknown detector`, were all `DbError::Internal`. They are
  the caller's input and are now `InvalidRequest` — `400`, with the message
  intact rather than redacted.
- **An unknown anomaly detector name silently became a z-score.**
  `detect_anomalies()` matched five detector names and fell through to
  `ZScoreDetector` for anything else, so `method: Some("modified-zscore")` — a
  plausible typo for the *median/MAD* detector, chosen precisely because it is
  robust to the outliers a mean and standard deviation are not — returned
  mean-based scores labelled `method: ZScore`, with no error. `forecast()`,
  one function above, already refused an unknown model by name and listed the
  valid ones; two implementations of one promise, and only one was checked.
  Now both refuse.

  The fallback was load-bearing, which is why it survived: `"zscore"` is the
  first name in `AnomalyConfig::method`'s documented list and had no branch of
  its own — it reached its detector only through the same arm that swallowed
  the typos. `preprocess()` was already immune, taking enums rather than
  strings.
- **`rolling_std(v, 1)` returned `0.0`.** A *sample* standard deviation of one
  observation divides by `n - 1 = 0` and is undefined; `0.0` asserts that a
  single reading has been observed not to vary. Now NULL, as pandas'
  `.rolling(1).std()` and `numpy.std(ddof=1)` both answer.

### Removed

- **Four `[analytics]` settings that were accepted from a config file and read
  by nothing**: `default_forecast_model`, `default_anomaly_method`,
  `default_confidence_level` and `default_anomaly_threshold`. Each had a serde
  default and a validated type, so `default_confidence_level = 0.99` loaded,
  validated and produced 0.95 for ever.

  There was no surface for them to reach. The analytics API is the embedded
  Rust one — `ForecastConfig { model, confidence, … }` and
  `AnomalyConfig { method, threshold, … }`, passed per call — and `chronixd`
  serves no forecasting or anomaly endpoint at all, while SQL splits the
  choice across two named functions (`forecast()` is SES, `auto_forecast()`
  chooses) and returns point values rather than intervals. A deployment-wide
  default had nothing to default. Under `deny_unknown_fields` a file setting
  one is now refused rather than silently ignored. **Breaking** for any config
  file that sets them. `[analytics]` is `max_forecast_horizon` and
  `max_training_points`, both enforced.

## [0.5.0] - 2026-09-12

### Added

- **`chronix-encoding`'s two untested codec modules have property tests.**
  The crate's first stated principle is that every encoder guarantees a
  bitwise-exact roundtrip, and every module backed it with a `proptest!`
  except `decimal` and `coding` — the two that most needed one. `decimal`'s
  wide form is a hand-written 128-bit LEB128 over `wrapping_sub` /
  `wrapping_add` deltas, and `coding`'s `pack_bits` / `unpack_bits` — the
  primitive under RLE, frame-of-reference and delta — accepts 64 bit widths
  and was pinned at two of them. Eleven new properties: roundtrip at every
  width, the packed size, the full `u128` varint and `i128` `ZigZag` ranges,
  `bits_needed` minimality, and two "decode never panics on adversarial
  bytes". Both modules turned out to be correct; what changed is that the
  principle is now checked rather than asserted.
- **CI runs the Kafka and MQTT integration suites against real brokers**, and
  `ignored_suites_run.rs` keeps it that way. All eleven tests that drive the
  connectors end to end are `#[ignore]`d because they need Docker, so the two
  jobs that exist to test the connectors printed `ok. 0 passed; 5 ignored`
  and `0 passed; 6 ignored` — green, executing nothing. `testcontainers`
  starts the brokers from inside the test, so the whole gap was a missing
  `-- --ignored`. The new test refuses any suite under `crates/chronixd/tests`
  that ignores its tests and is not named by a workflow line passing
  `--ignored`. Running them for the first time found what never-executed
  code always has: MQTT passed 6/6, and **4 of the 5 Kafka tests failed** —
  each produced to a topic nothing had created, and a KRaft broker resolves
  the metadata request that triggers auto-creation *after* it answers the
  produce. They now create their topics through `AdminClient::create_topics`.
- **A TSBS load benchmark, run by hand** — `.github/workflows/tsbs.yml`,
  `workflow_dispatch`. It generates one dataset with the industry's suite and
  loads it into chronix *and* InfluxDB 1.8 on the same runner in the same
  job, because a shared runner's absolute throughput is not comparable to
  figures published from dedicated instances — the ratio is the result. Load
  only: `tsbs_run_queries_influx` speaks InfluxQL, which chronix does not.
- **`scripts/preflight.sh` runs what CI gates on in one command** — lints,
  rustdoc, the guards and both the default and feature test builds
  (`--quick`), plus the examples and the frozen tier without it. CONTRIBUTING
  points at it.
- **Feature documentation names the dependencies a feature pulls in rather
  than counting them.** A package count is not a property of the code: the
  same tree resolves 63 packages for `chronix --features streaming` on macOS,
  62 in a Linux container and 88 on GitHub's runners. The docs now name what
  arrives — a JWT library, Argon2, Cedar and a TLS stack for `security`; an
  HTTP client and a TLS stack for `streaming` — and point at
  `cargo tree -e normal` for anyone wanting a figure for their own build.
- **The release's publish-order check validates that the steps *work*, not
  that they match one particular ordering.** It compared them against
  `publish-order.sh`'s own output, which is one linearisation of several
  correct ones, so a release failed when removing the
  `chronix-engine` → `chronix-security` edge freed those two crates to swap.
  It now checks what crates.io enforces — every crate published after its
  dependencies, none missing, none duplicated — and runs on every preflight
  rather than only on release day.
- **`scripts/check-features.sh` now reads the workflows too**, in both
  directions: a workflow may not name a feature its crate does not declare
  (renaming `flight` to `arrow` left one such step behind, and it failed only
  minutes into CI when that job ran), and a crate may not declare an optional
  feature no workflow names. The second found `rust_decimal`, whose three
  conversion tests had never run in CI — only `cargo doc --all-features`
  reached the module, and that does not build test code. Both crates now have
  a `--features rust_decimal` test step.
- **`scripts/check-features.sh` checks every feature dependency against the
  code it unlocks**, and runs in CI. `cargo machete` cannot read the
  `[features]` table and `cargo udeps` needs nightly and a full build, so a
  `dep:` entry that nothing names is invisible to both on a pull request —
  which is how the `flight` feature above carried 32 unused packages. It
  also refuses a `[package.metadata.cargo-machete] ignored` list in a crate
  with no `build.rs`: generated code is the only thing a source scanner
  cannot see, and the ignore list that had been written for `flight` gave a
  reason that was true in general and false about those three crates.
- **The Cedar authorization model is compiled in as a schema, and every
  policy is validated against it.**
  [`chronix.cedarschema`](crates/chronix-security/src/authz/chronix.cedarschema)
  ships in the binary; `chronixd` refuses to start on a policy naming an
  action or an entity type it never asks about. Without it Cedar accepts
  anything well-formed, so a `permit` that permits nothing looks like a
  lockout and a `forbid` that forbids nothing looks like protection — which
  is what every policy example in the security guide was.
- **Granular administrative capabilities are asked for.** Each administrative
  route group requires its own — `ManageKeys`, `ManageBackups`,
  `ManageConfig`, `ManageModels`, `ManageNamespaces`, `ManageNodes`,
  `ManageRegions`, `ManageCluster`, `ViewCluster` — so a backup credential
  cannot mint API keys. `Admin` is now an action **group**:
  `action in Chronix::Action::"Admin"` grants all of them at once.
- **API keys carry Cedar roles.** `roles = [...]` on `[[auth.api_keys]]`, and
  `roles` in the body of `POST /api/v1/admin/auth/keys`. Roles are resolved
  once at authentication, from whichever credential was used, and live on
  `AuthContext`.
- **`cargo machete` runs in CI**, and found an unused `chrono-tz` in the
  facade on its first run. An unused dependency is not a warning, so nothing
  here could see one — which is how `chronix-engine` carried
  `chronix-security` with no reference to it, and why a consumer found that
  instead of us.
- **`every_data_route_resolves_a_namespace`** — the second property asserted
  of every mounted route, after the deny-all authorization walk. Authorization
  and scoping are different questions and only the first is a middleware's:
  the gate says *may you touch this tenant*, the handler decides *which rows
  come back*. A route added tomorrow is covered the day it is mounted.
- **`chronix_engine::durable::DurableFile`** — one fault-injection seam for
  every durable path, so a test can make a write fail where a filesystem will
  not. The WAL's `WalSink` is now its seekable specialisation, and the
  catalog uses it too, which is what found the three defects above.
  `chronix_catalog_snapshot_failures_total` and
  `chronix_catalog_tail_records_skipped_total`, published at zero.
- **`ServerConfig::validate_authz`** refuses `authz_policy_dir` without an
  `[auth]` section: a policy names a principal, and every request would be
  anonymous.
- **`PUT /api/v1/namespaces/{name}/quota`.** `NamespaceRegistry::update_quota`
  existed, validated its bounds and persisted — and no route reached it, so a
  tenant's quota was whatever it was created with and growing one meant
  deleting the namespace. The new route applies the change to the live rate
  limiter as well as to the record.
- **Every audit category the server declares is one it emits.** `AuditAction`
  had twenty-five variants and the server constructed **four**;
  `namespace_delete`, `data_export`, `policy_load`, `api_key_create`,
  `schema_change`, `trigger_create`/`trigger_drop`, `create_rollup`/
  `drop_rollup`, `quota_change` and `signal_fired` are all recorded now, the
  Cedar policy set is sealed into the chain at startup with every policy id,
  and **every refusal is recorded, on every protocol** — administrative and
  data-plane, HTTP, gRPC and Flight SQL. That is the event the trail exists
  for and it was in none of it: a policy denial left a `warn!` in the process
  log, which does not survive a restart and cannot be shown to be unedited.
  `chronix_authz_denied_total` counts them. `every_audit_action_has_a_producer`
  walks the enum. Dropped with no producer and no operation behind them:
  `login_success` (a database authenticates every request; recording the
  successes buries what the trail is for), `token_refresh`,
  `permission_change`, `forecast`, `detect_anomalies`, `subscribe`, and
  `key_rotation`, which stood in for the two precise key events.
- **The crate's front page links to this file.** `cargo package` includes
  nothing from outside a package directory, so a root-level changelog is
  invisible on crates.io and docs.rs and a consumer had to diff the
  repository to find a breaking change. There is still exactly one changelog,
  at the repository root; the README — which *is* the published crate's front
  page — now carries an absolute link to it, and `check-docs.sh` fails if the
  link goes or a second changelog appears.

- **Webhook signing secrets rotate.** `triggers.webhook_signing_secrets` is
  a list, newest first, and every delivery carries one `v1,<sig>` per secret
  in the space-delimited `webhook-signature` header Standard Webhooks defines
  for this. A receiver holding any one of them verifies, so sender and
  receivers can be updated in either order; with a single secret every
  in-flight delivery failed the moment either side changed.
  `CHRONIX_WEBHOOK_SIGNING_SECRET` takes the list comma-separated.
- **Scheduled checkpoints.** `[database.checkpoints]` — an interval, a
  directory and a `keep` count — and the maintenance thread takes them,
  beside the flush, compaction, rollup and retention passes it already runs.
  Off by default. The first runs at startup rather than one interval later,
  pruning happens after a successful run, and an unfinished run is removed on
  sight. A backup was the one maintenance task that still needed a scheduler
  the embedded deployment does not have.
- **Per-column encryption is configurable.** `[database.field_encryption]`
  names a column and an **environment variable** — never a key — so the key
  is not on the disk it protects and a stolen backup stays unreadable.
  AES-256-GCM per block, each bound to its column name and the segment's
  creation timestamp. Compaction re-encrypts rather than decrypting on the
  way through, and a Parquet export, the cold-tier archive and any rollup
  over the measurement are **refused**, naming the column, because each
  would write the plaintext somewhere the segment's protection does not
  reach. Only a field may be encrypted: a tag is part of the series key and
  is written in plaintext in the segment's sidecar, tag index and bloom
  filter, so declaring one is a write error. Rotation is a second key id;
  every declared key must resolve at startup. The format capability existed
  and was reachable from nothing — no configuration turned it on, the read
  paths were not key-aware, and compaction would have silently decrypted.
- **`backup()` is a checkpoint.** It is driven by the catalog rather than by a
  directory walk, takes a segment lease so nothing it is copying can be
  unlinked under it, and **hard-links** segment files when the target shares a
  filesystem — so a checkpoint of a large database is near instant and costs
  no space until those segments are compacted away. `BackupManifest` gains
  `segments`, the number its catalog names.
- **A restore hard-links the backup's segments** and copies only `catalog/`
  and `wal/` — linkable if and only if immutable, so restoring a large backup
  costs a directory entry per segment rather than its bytes.
- **`Chronix::verify_backup()` and `POST /api/v1/admin/backup/verify`** check
  a backup without restoring it — the same verification a restore runs, on
  its own, so the backups you are keeping can be checked before you need them.
- **`chronix_backups_total`, `chronix_backup_failures_total`,
  `chronix_backup_bytes_total` and `chronix_backup_duration_seconds`**, with
  the two counters published at zero so an alert on a nightly checkpoint that
  stopped running can fire from the first scrape. Panels in
  `dashboards/storage.json`.
- **`restore()` verifies before it copies**: every segment the backup's
  catalog names must be present at its recorded size, and the count must match
  the manifest. An incomplete backup is refused rather than restored into a
  database that fails at its first query. `Chronix::open()` asks the same
  question of any data directory.
- Backing up repeatedly into one directory is a rolling checkpoint: files the
  new one does not name are removed, and the previous manifest is deleted
  first, so a re-checkpoint that fails part-way leaves nothing restorable.

- **`time_bucket()` takes an `origin`**, so a bucket boundary need not be
  midnight on the 1st: `time_bucket('1mo', _time, '', '2024-01-15')` is a
  billing month that runs from the 15th, and
  `time_bucket('1d', _time, 'Europe/Berlin', '2024-01-01 06:00:00')` is a
  shift day that starts at six. Only the origin's phase matters. A monthly
  origin after day 28 is refused rather than clamped — that day is missing
  from some months, so it is not a monthly boundary.
- **Arrow Flight `DoPut` can backfill.** Put `{"backfill": true}` in
  `FlightData.app_metadata`; it was the one write surface that could not
  import history. An unrecognised key is refused rather than ignored.
- `TimeBucket::fixed(Duration)`, because a `Duration` *is* a fixed span —
  which is also why it cannot produce a calendar bucket, and says so.
- **Rollups take an `origin` too**, on the HTTP API and on `RollupBuilder`,
  and the rollup listing now reports it — so what the listing returns can be
  typed straight back into a create request, which is what its documentation
  already promised.

### Changed

- **`chronix-streaming`'s `flight` feature is now `arrow`, and no longer
  pulls the gRPC stack.** It declared `arrow-flight`, `tonic` and `prost` —
  **32 packages** — for a module that names none of the three: it converts
  CDC events to Arrow `RecordBatch`es and stops there, while its own
  documentation claimed it "implements the Arrow Flight `DoExchange` RPC".
  The feature *is* the encoding, for an embedded consumer that already
  speaks Arrow; over the network `chronixd` streams CDC as JSON over SSE at
  `/api/v1/cdc/stream`. Enabling `arrow`
  on `chronix-streaming` now resolves **244 packages instead of 276**.
  `cdc::flight` is now `cdc::arrow_batch` and `CdcFlightExporter` is
  `CdcBatchExporter`.
- **`chronix-core` no longer makes its consumers depend on `serde_json`,
  and `chronix-engine`'s `object-store` feature no longer pulls
  `tempfile`.** Both were used only under `#[cfg(test)]` and declared as
  ordinary dependencies, so every consumer inherited them.
- **`chronix`'s `security` and `streaming` features are off by default**, and
  `chronix-streaming` / `chronix-security` are optional dependencies. An
  embedded build resolves **175 packages instead of 304** — no JWT library,
  no Cedar, no Argon2, no `aes-gcm`, no HTTP client, no TLS stack. If you use
  `db.subscribe()`, `chronix::chronix_security`, or `Pipeline`, add
  `features = ["streaming"]`, `["security"]` or `["pipeline"]`. `chronixd`
  takes all three at its dependency and does not compile without them.
- **A data request's action comes from the route, not the HTTP method.** The
  method mapping was wrong in both directions, and a live server found it.
  `POST /api/v1/chronix/sql`, `POST /api/v1/chronix/query` and
  `POST /api/v1/query` are **reads** — the last is how Grafana sends PromQL by
  default — so a read-only policy could not read, and granting a datasource
  the least privilege it needed meant granting `Write`, which also grants
  ingest. In the other direction `POST /api/v1/delete` and `/delete_batch`
  are **deletes**, so `forbid(principal, action == Chronix::Action::"Delete", …)`
  — the example this guide shows — forbade nothing, because any principal
  that could write could delete. Every data route is now classified
  explicitly against its matched route pattern, an unclassified one is
  refused rather than guessed, and `every_data_route_is_classified` fails on
  a route nobody has classified.
- **gRPC and Flight SQL are behind the data-plane authorization gate.** They
  do not pass through an axum middleware, which is where the gate lived, so
  they had none: a principal a policy denied `Read` on a namespace could read
  it by pointing any Flight SQL or gRPC client at the same server. Every RPC
  now names the action it performs, through one function, the way every write
  surface already went through one write function.
- **`SharedState::namespace_registry` is no longer an `Option`.** The gate
  returned early when it was `None` — a branch `run()` could not produce,
  because it always opens a registry, and one that **every test and bench in
  the tree took**, since they all set `None`. So the branch the whole suite
  exercised was the one that skips the gate. Nothing was wrong in the
  product; nothing could have caught it if something had been.
- **An administrative request needs the capability *and* the policy.** A
  configured policy engine used to replace the credential's `admin` flag
  rather than join it, so pointing `authz_policy_dir` at a permissive file
  widened what every ordinary key could reach. Adding policies can now only
  narrow.
- **Health, readiness, the metrics scrape, the OpenAPI document and every
  administrative route are outside the data gate**, so a policy file cannot
  take a liveness probe down and an administrative request is not also a
  request *in* a namespace. That last one was a live trap: `validate_tenancy`
  requires every key to name its namespaces under multi-tenancy,
  administrative keys included, so a key bound to `tenant-a` was refused on
  every administrative endpoint unless it also listed `default` — a namespace
  an administrative request does not have and nothing told anyone to add.
- `AuthzEngine` has two entry points — `authorize_namespace` and
  `authorize_system` — replacing `authorize` / `authorize_admin`.

- **Breaking:** the catalog records a segment's path **relative** to the
  `segments/` directory, as a `SegmentFile`. `SegmentCatalogEntry.path:
  PathBuf` is now `SegmentCatalogEntry.file: SegmentFile`, and
  `SegmentFile::resolve(segments_dir)` is the only way to an openable path.
  A data directory is relocatable as a result — see *Fixed*.
- **Breaking:** `triggers.webhook_signing_secret` is now
  `triggers.webhook_signing_secrets`, a list;
  `PipelineConfig::webhook_signing_secret` is now `webhook_signing_secrets`;
  and `WebhookConfig::signing_secret` is now `signing_secrets`, with
  `WebhookConfig::with_secrets` beside `new`.
- **Breaking:** `CompactionTask` carries `segments_dir` and a relative
  `output`, with `output_path()` resolving the two; `PrunedSegment` loses its
  `path` field, which nothing read.
- **Breaking:** point-in-time recovery is removed —
  `Chronix::restore_pitr`, `POST /api/v1/admin/restore/pitr` and
  `WalWriter::archive_before`. The endpoint did nothing (see *Removed*).
- **Breaking:** `chronix_engine::storage::{LocalFsBackend, EncryptingBackend}`
  are removed. Neither had a caller.
- **Breaking:** `QueryBuilder::downsample()` takes a `TimeBucket` instead of
  a `Duration`, and `QueryPlan::Downsample` carries one. The native plan can
  now express a calendar day or month, and — more to the point — it now means
  the same thing as `time_bucket()` in SQL and as a rollup tier. A `1d` there
  used to be 86 400 seconds of UTC while `1d` in SQL was the zone's day, so
  the two surfaces disagreed on every daylight-saving transition.
- **Breaking:** `TimeBucket` and `BucketWidth` moved from `chronix::timebucket`
  to `chronix_core::timebucket`, and are in `chronix::prelude`. They are part
  of the data model, and every crate that buckets time now shares one
  definition.
- **Breaking:** the native query API's `EXPLAIN` reports a `Downsample` node's
  bucket as its width, timezone and whether it is a calendar bucket, replacing
  `interval_ms` — which had to invent a length for a month.

- **Breaking (gRPC):** `SqlValue` and `FieldValue` gain a `json` variant,
  carrying the JSON encoding of a value with no scalar protobuf field — a
  list, struct, map, interval or binary cell. These previously arrived as the
  literal text `"<unsupported: List(Float64)>"` in the `string` field, which
  no client could tell from a string.

### Fixed

- **`LogReturnExpr`, `RollingStatExpr` and `RollingStat` are reachable.**
  All three implement or support `chronix_analytics`'s own
  `DerivedSeriesExpr` trait and were missing from the `pub use` list that
  makes the private `derived` module's contents nameable, so a consumer
  could not construct a log-return or rolling-statistic derived series —
  while a comment elsewhere in the crate cited `RollingStatExpr` "for
  consistency across the codebase". A module-level `#![allow(dead_code)]`,
  annotated "Public API types — used by external consumers", kept the
  compiler quiet; removing it produces exactly those three warnings and no
  others.
- **A `[kafka]` or `[mqtt]` section in a binary built without that feature is
  now a startup error** naming the flag to rebuild with, as `[cold_archive]`
  already was. It previously registered a connector that logged "enable the
  feature", then reported `Idle` — healthy, with `chronix_connector_up 1` —
  while ingesting nothing.
- **A connector that could not reach its broker reported `Running` for
  ever.** Three layers, each hiding the one below. (1) Both connectors gave
  up on their first setup error — `error!(…); return;` — and the commonest
  such error is transient: a Kafka broker that has just started has no
  `__consumer_offsets` topic, so the first `subscribe` gets
  `CoordinatorNotAvailable`. That is chronixd and its broker coming up
  together in a compose file or a rollout, and the connector was then dead
  for the lifetime of the process. Setup now retries with backoff, for ever.
  (2) `status()` was computed from the `running` flag that `start()` had just
  set, so it reported the caller's *intent*: `ConnectorStatus` declares five
  variants and `Idle`, `Reconnecting` and `Failed(String)` were constructed
  nowhere in the tree. MQTT's loop logged "connection error, reconnecting…"
  while `status()` answered `Running`. It now reports what the task is
  doing. (3) `is_healthy()`'s documentation claimed it gated `/ready`; it did
  not, and `all_healthy()` had no caller outside its own test.
  `chronix_connector_up{connector,type}` replaces it, refreshed on scrape.
  Found by running the broker suites for the first time.

- **An API key's creation was audited against the key it created.** The
  record named the *new* key as the principal rather than the caller, which
  answers "who did this?" with the thing that was done.
- **Two concurrent audited requests made the tamper-evident log report
  tampering.** `AuditLogger::log` sealed the event into the hash chain under a
  lock and released it *before* writing, so two callers could seal in one
  order and write in the other — leaving a file in which `B` precedes `A`
  while `B.prev_hash` names `A`, which is exactly what `verify_hash_chain`
  reports as tampering. No write failed and nothing was logged. A trail whose
  purpose is to be checkable cannot have a benign reason to fail its own
  check, because then no failure of it means anything. Sealing and emitting
  are now one step.
- **An audit event that reached no durable sink still advanced the chain**,
  so a full disk broke every later verification of that file, permanently,
  and reported it as tampering. The chain now continues from the last event
  that landed; the gap is counted by `chronix_audit_chain_gaps_total` and
  logged at `error`.
- **A half-written audit line swallowed the next event.** `writeln!` that
  failed part-way left a line with no terminator, and the following event was
  appended to it — one line holding a fragment and a whole event, parsing as
  neither. The line is now one `write_all` with a rewind on failure, so a
  failed write loses only itself.
- **The trigger catalog fsynced the directory but not the data.** That makes
  the *rename* durable while the bytes it points at may not be, so a crash
  could leave the file empty or half-written with every trigger in it gone —
  the ordering that looks careful and protects the wrong half. It also
  discarded the directory fsync's error (`let _ =`) and left its temp file
  behind on failure. Both lessons had been learned by the two sibling atomic
  writers in this tree, at different times, and not carried across.
- **Revoking an API key was undone by a restart.** `[[auth.api_keys]]` is a
  declaration and is re-read on every start, and the revocation lived in an
  in-memory store — so an operator revoking a leaked credential during an
  incident was told `204`, and the next restart handed the key back. Nothing
  said so. The mirror half was the same: a key minted through
  `POST /api/v1/admin/auth/keys` is shown **once** and cannot be recovered,
  and it stopped working at the next restart. Both are now recorded in
  `<data_dir>/auth/api_keys.json` — Argon2 hashes for created keys, names for
  revoked ones, applied after the config so a revocation wins over a
  declaration — written before the operation is acknowledged and rolled back
  if the write fails.
- **Creating a namespace was acknowledged before it was durable.** The
  registry snapshotted *best effort* on a background thread and returned
  `Ok` regardless, so an operator could provision a tenant, be told it
  exists, hand out a credential bound to it, and find after the next restart
  that every request from that tenant answers `400 namespace not found`.
  Deleting one was already synchronous, with a comment saying destructive
  operations must be durable before returning — the asymmetry was the tell.
  Creates and quota changes are synchronous now, the in-memory entry is
  rolled back if the write fails, and the background-snapshot thread (with
  the rename race it needed serialising against) is gone because nothing
  used it any more.
- **Every namespace-registry failure reached the caller as `400`.** A full
  disk was reported as *invalid namespace config*, which sends an operator to
  re-read their request body while the problem is the volume. Persistence
  failures have their own `TenantError::Persist` and map to a server error.
- **gRPC `GetSchema` leaked another tenant's schema.** It answered from the
  process-wide schema registry with no namespace scope at all, so one tenant
  could name another's measurement and be told its column names — while the
  HTTP sibling `GET /api/v1/measurements/{name}/schema` had scoped since the
  tenancy pass. The gRPC tenancy suite could not see it: it walks the RPCs
  that *take* a scope, and this one did not take one.
- **The Python SDK's defaults and documented calls.** `ChronixClient` used
  `localhost:5555` and Flight SQL `5557`; the server listens on 8086 and
  8817. Three files showed `client.query(..., tag_filters={...})`, which
  raises `TypeError`. The namespace header was `X-Chronix-Namespace`, which
  `chronixd` does not read — so a client configured for a tenant was served
  `default`, and on a deployment whose credential is bound to a tenant, every
  request was refused for a namespace nobody asked for.
- **`scripts/check-docs.sh` scanned `site/content` and the top-level README
  only.** It has a check for exactly the port drift above, written after it
  happened once; `sdks/python/README.md` is on PyPI and was outside its
  scope. It now covers every published document.

- **A restored backup is the database that was backed up.** The catalog stored
  absolute segment paths, so a restore onto a fresh directory produced an
  **empty** database — `open()`'s orphan sweep compared the restored files
  against paths naming the original directory, matched none, and deleted every
  one — and a restore *beside* the original silently read the original's
  segments until the copy's first compaction unlinked them. The existing
  round-trip test never flushed, so it exercised a backup with no segments in
  it.
- **A backup taken while the database is working is complete.** `backup()`
  walked `wal/`, `segments/` and `catalog/` with `read_dir`: a WAL file
  truncated by the next flush vanished mid-copy and failed the backup with a
  bare `ENOENT`; a catalog snapshot landing mid-copy could pair the old
  snapshot with the log that snapshot had truncated, losing every transition
  between them; and a segment flushed between two directory walks was named by
  the copied catalog and absent from the copy.
- **`Chronix::tag_keys()` and `tag_values()` see unflushed series.** They read
  the inverted tag index, which is built at flush — so a freshly started
  database reported no labels at all, and a tag appearing only in recent data
  was invisible. `chronixd`'s `/labels` was always correct, because it scans;
  the two now agree.
- **`last_value()` finds a pre-1970 point.** It scanned the memtable from `0`
  rather than `i64::MIN` and returned `None` for a series a query returns rows
  for.
- **Every `histogram!` now reaches a Prometheus scrape as a histogram.** The
  exporter was left unconfigured, and `metrics-exporter-prometheus` renders
  histograms as *summaries* unless buckets are set — so no `_bucket` series
  existed, `histogram_quantile()` had nothing to read, and 24 of 45 panel
  targets in the bundled dashboards were permanently empty. A summary's
  quantiles also cannot be aggregated across replicas, and the estimator was
  independently wrong for non-latency distributions
  (`chronix_batch_size{quantile="0.5"}` read `0.9998` for observed values of
  1 and 4320).
- **`dashboards/query-performance.json` queried metrics only a
  `--features cluster` build emits**, so all four panels were empty on every
  standard `chronixd`. Rewritten against the metrics a default build exports:
  query and write latency percentiles, throughput, batch size, SQL plan-cache
  and PromQL scan-cache hit ratios, and segment pruning.
- Write-path counters are published at **zero** from startup, so an
  ingestion-error panel reads `0` rather than "No data" and an alert on it can
  fire from the first scrape.
- A soft delete's grace period no longer depends on the raw wall clock. The
  GC pass compared the deadline against `SystemTime::now()`, so one bad
  reading — a gateway with no battery-backed RTC, an NTP server handing out a
  date in the next century — closed the recovery window instantly and
  hard-deleted the data it existed to protect. It now measures from the same
  reference retention uses: the clock capped by the newest timestamp held.
- `RollupBuilder::bucket()` dropped the bucket's origin. It decomposed the
  `TimeBucket` into width and timezone strings and re-parsed them at
  `build()`, so a tier declared through any protocol handler bucketed from
  midnight on the 1st however the caller had asked.
- Every Arrow type a query can produce now reaches the client as a value.
  Each wire surface carried its own conversion table over a different subset
  of Arrow's types, so an unenumerated type came back as the string
  `"<unsupported: Date32>"` inside a column declared `Date32`, under
  `200 OK` — `SELECT CAST(_time AS DATE) … GROUP BY 1`, an ordinary
  group-by-day, was such a query — or was dropped from the row entirely. One
  encoding now serves every surface, and its match is exhaustive over
  `DataType`, so coverage is a build error rather than a promise. Arrow Flight
  SQL was unaffected throughout: it streams the batch unmodified.
- A decimal column with a **negative scale** (legal in Arrow, produced by a
  cast) reached the client as `null` rather than its digits.
- A SQL mistake now answers `400` with the reason. `SELECT 1/0`,
  `CAST('abc' AS INT)`, `to_timestamp('not-a-date')`,
  `date_trunc('fortnight', …)` and an unknown time zone answered
  `500 … an internal error occurred`, with the message that named the problem
  redacted on the way out. A chronix error raised under a SQL query — a full
  memtable, a query deadline — again keeps its own status instead of being
  flattened to `500`.
- Converting a query result back into points dropped any field column whose
  Arrow type it did not enumerate, `Decimal128` among them. That path serves
  distributed reads **and Raft region snapshots**, so replicating a region
  silently discarded every exact-decimal field it held. It is now one shared
  conversion, and a type it cannot represent fails the snapshot rather than
  disappearing from it.

### Removed

- **`Chronix::Measurement` as an authorization resource, with its tag
  constraints.** The unit of authorization is the namespace, which is the
  unit storage, SQL scoping, quotas and credentials all already use. A second
  finer unit understood only by the policy layer would have to be remembered
  by each of ~40 routes, and the ones that forgot would read as protected.
  `ChronixResource` is gone; `ChronixSystem` replaces the synthetic
  `Measurement::"__system__"` the administrative gate used.
- **Policy versioning, rollback and dry-run** (`PolicyVersion`,
  `current_version`, `policy_versions`, `revert_to_version`,
  `dryrun_load_policies`). Documented with a worked example and reachable
  from no endpoint; the policy files are the source of truth.
- **`load_cross_tenant_isolation_policy` and `warn_if_no_namespace_policies`.**
  Neither had a caller, and the built-in policy compared a
  `principal.namespace` attribute nothing ever set — an isolation guarantee
  that existed as a string constant. Cross-tenant isolation is a property of
  the credential and is enforced before Cedar is consulted.
- **`ChronixAction::{Forecast, DetectAnomalies, Subscribe, CreateRollup}`**,
  and `Admin` as a requestable action — no request path asked for any of
  them. (The identically named `AuditAction::CreateRollup` is unrelated and
  stays: rollup creation *is* recorded. The two enums answer different
  questions — what a policy decides about, and what a trail is searched by —
  and `namespace::audit_action_for` is the one place they meet.)
- **`chronix-engine`'s dependency on `chronix-security`** — one line in a
  manifest, referenced from nowhere in the crate, and the edge that made
  gating the facade alone insufficient (reported by the hems gateway).

- **Point-in-time recovery.** `Chronix::restore_pitr` and
  `POST /api/v1/admin/restore/pitr` did nothing: the target sequence had to be
  at or after the backup's own, the backup's WAL ends there, so the replay
  window was always empty and every target sequence produced a byte-identical
  database. The other half — `WalWriter::archive_before`, which copies WAL
  files aside before truncation — had no caller, no configuration and no
  documentation, so there was no archive to recover from. It was also the one
  admin endpoint that wrote no audit record.
- **`WalEntry::Delete`.** A delete wrote and fsynced a WAL record carrying its
  resolved tombstones, and nothing could read it: `execute_delete` flushes
  first, so every point a tombstone covers is already in a segment and below
  the WAL floor, which replay never reads. Its stated purpose was
  point-in-time restore. What is left is one `fsync` per delete instead of
  two — the catalog manifest, which was always the durable record. WAL
  discriminant `0x01` is retired and not reused.
- **`EncryptingBackend` and `LocalFsBackend`.** Neither had a caller anywhere,
  and `EncryptingBackend` could only ever have encrypted cold-tier objects,
  because the cold tier is the only implementor of the trait it wraps.
  Chronix does not encrypt its own data directory and no setting made it: the
  security guide's `storage.encryption.enabled` was a key that never parsed,
  and its claims of WAL encryption and an HMAC manifest check were both
  phantom (the manifest is CRC-32C). The documentation now says what is
  actually offered — filesystem or volume encryption for the data directory,
  the bucket's own for the cold tier, and the `.csx` format's per-column
  AES-256-GCM, which fails closed and which no configuration enables.
- `chronixd snapshot` / `chronixd restore` / `chronixd bench` from the docs.
  `chronixd` takes no subcommands.

## [0.4.0]

### Changed

- **Breaking:** webhook delivery now sends a [CloudEvents](https://cloudevents.io)
  1.0 envelope, signed per the [Standard Webhooks](https://www.standardwebhooks.com)
  `v1` scheme (`webhook-id` / `webhook-timestamp` / `webhook-signature`),
  replacing the `X-Chronix-Signature: sha256=<hex>` header. Signing the
  timestamp alongside the body lets a receiver reject a replayed request,
  which the old body-only signature could not express.

### Fixed

- A measurement dropped with `soft_delete_ttl` configured is now actually
  invisible — to SQL, PromQL, the native query API, gRPC and Flight SQL — for
  its whole grace period, and the pending state survives a restart. It was
  previously tracked in memory only and consulted by nothing on the read
  path, so the data stayed fully readable until the background pass
  eventually deleted it.
- HTTP, gRPC and Flight SQL measurement listing, and PromQL discovery, now
  resolve "what measurements exist" through the same accessors the query path
  uses, so a pending-drop measurement can no longer appear in one listing and
  not another.

## [0.3.0] and earlier

Predate this file. See the `v0.1.0` / `v0.2.0` / `v0.3.0` git tags.
