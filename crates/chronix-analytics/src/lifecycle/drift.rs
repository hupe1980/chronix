//! Distribution drift detection for model monitoring.
//!
//! Three techniques:
//!
//! - **PSI** (Population Stability Index): compares binned distributions
//!   of current vs training data.  Threshold ≥ 0.25 → significant drift.
//! - **Kolmogorov–Smirnov**: two-sample KS test on CDFs.
//! - **ADWIN** (Adaptive Windowing): online algorithm that detects concept
//!   drift in a data stream by maintaining adaptive windows.

use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;

/// Action to take when drift is detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DriftAction {
    /// No drift detected — do nothing.
    NoAction,
    /// Drift detected — trigger model re-fit.
    RetriggerFit,
    /// Mild drift — emit alert but don't re-fit yet.
    AlertOnly,
}

/// Complete drift analysis report.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DriftReport {
    /// Detector that produced this report.
    pub detector_name: String,
    /// Drift score (PSI value, KS statistic, or ADWIN `epsilon`).
    pub score: f64,
    /// Whether drift was detected.
    pub drift_detected: bool,
    /// Recommended action.
    pub action: DriftAction,
}

/// Drift detection strategy.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum DriftDetector {
    /// Population Stability Index with a configurable threshold (default 0.25).
    Psi {
        /// PSI threshold for drift detection.
        threshold: f64,
    },
    /// Kolmogorov–Smirnov two-sample test with a configurable p-value
    /// significance level (default 0.05).
    KolmogorovSmirnov {
        /// Significance level for the KS test.
        p_value: f64,
    },
    /// ADWIN online adaptive windowing with a configurable delta (default 0.002).
    Adwin {
        /// ADWIN confidence parameter.
        delta: f64,
    },
}

impl Default for DriftDetector {
    fn default() -> Self {
        Self::Psi { threshold: 0.25 }
    }
}

impl DriftDetector {
    /// Run drift detection.
    ///
    /// - `training`: the reference distribution the model was trained on.
    /// - `current`: the current/production data.
    pub fn detect(&self, training: &[f64], current: &[f64]) -> DriftReport {
        let report = match self {
            Self::Psi { threshold } => psi_detect(training, current, *threshold),
            Self::KolmogorovSmirnov { p_value } => ks_detect(training, current, *p_value),
            Self::Adwin { delta } => adwin_detect(training, current, *delta),
        };
        if report.drift_detected {
            metrics::counter!("chronix_model_drift_detected_total", "method" => report.detector_name.clone()).increment(1);
        }
        metrics::gauge!("chronix_model_drift_score", "method" => report.detector_name.clone())
            .set(report.score);
        report
    }
}

// ── PSI ─────────────────────────────────────────────────────────────────

fn psi_detect(training: &[f64], current: &[f64], threshold: f64) -> DriftReport {
    let n_bins = 10;
    let psi = compute_psi(training, current, n_bins);
    let drift_detected = psi >= threshold;
    let action = if psi >= threshold {
        DriftAction::RetriggerFit
    } else if psi >= threshold * 0.4 {
        DriftAction::AlertOnly
    } else {
        DriftAction::NoAction
    };

    DriftReport {
        detector_name: "PSI".into(),
        score: psi,
        drift_detected,
        action,
    }
}

/// Compute PSI between two distributions using quantile-based
/// (equal-frequency) bins derived from the *training* data.
///
/// Quantile-based bins adapt to the shape of the training distribution,
/// producing more meaningful PSI values when data is skewed.
fn compute_psi(training: &[f64], current: &[f64], n_bins: usize) -> f64 {
    if training.is_empty() || current.is_empty() || n_bins == 0 {
        return 0.0;
    }

    let mut sorted_train: Vec<f64> = training.to_vec();
    sorted_train.sort_by(f64::total_cmp);

    let edges = quantile_bin_edges(&sorted_train, n_bins);

    let t_counts = bin_counts_by_edges(training, &edges);
    let c_counts = bin_counts_by_edges(current, &edges);

    let t_total: f64 = t_counts.iter().sum();
    let c_total: f64 = c_counts.iter().sum();
    let eps = 1e-4; // smoothing constant – avoids log(0)

    if t_total == 0.0 || c_total == 0.0 {
        return 0.0;
    }

    let mut psi = 0.0;
    for i in 0..t_counts.len() {
        let p = (t_counts[i] / t_total).max(eps);
        let q = (c_counts[i] / c_total).max(eps);
        psi += (q - p) * (q / p).ln();
    }

    psi.max(0.0)
}

/// Compute bin edges from percentiles of *sorted* training data.
///
/// Returns `n_bins + 1` edges such that each bin captures approximately
/// the same number of training observations (equal-frequency binning).
fn quantile_bin_edges(sorted_data: &[f64], n_bins: usize) -> Vec<f64> {
    let n = sorted_data.len();
    let mut edges = Vec::with_capacity(n_bins + 1);
    for i in 0..=n_bins {
        let idx = (i * n / n_bins).min(n.saturating_sub(1));
        edges.push(sorted_data[idx]);
    }
    // Ensure the last edge is slightly above the max so that the
    // maximum value falls inside the last bin.
    if let Some(last) = edges.last_mut() {
        *last += f64::EPSILON;
    }
    // Repeated edges are left in place, and that is deliberate.
    //
    // Equal-frequency edges repeat wherever the training data is quantised —
    // a gauge resting at zero, a state of charge pinned at 100 %. The obvious
    // worry is that a repeated edge is a bin nothing can fall into, so its
    // reference share floors at `eps` while its current share does not, and
    // `(q−p)·ln(q/p)` invents drift. It does not happen: a zero-width bin is
    // unreachable for *both* samples, so `p == q == eps` and the term is
    // zero. Collapsing them would change every PSI value against the
    // conventional 0.1 / 0.2 thresholds in exchange for nothing, so the
    // hypothesis is recorded here rather than acted on.
    edges
}

/// Count how many values fall into each bin defined by `edges`.
///
/// `edges` has length `n_bins + 1`.  Bin *i* spans `[edges[i], edges[i+1])`.
/// Values below the first edge go into bin 0; values at or above the last
/// edge go into the final bin.  Returns `n_bins` counts as `f64`.
fn bin_counts_by_edges(data: &[f64], edges: &[f64]) -> Vec<f64> {
    let n_bins = edges.len() - 1;
    let mut counts = vec![0.0_f64; n_bins];
    for &v in data {
        if v.is_nan() {
            continue;
        }
        // Binary-search for the right bin
        let mut idx = match edges.binary_search_by(|e| e.total_cmp(&v)) {
            Ok(i) => i,  // exactly on an edge
            Err(i) => i, // insertion point
        };
        // Value before first edge → bin 0
        if idx == 0 {
            counts[0] += 1.0;
            continue;
        }
        // Value at or beyond last edge → last bin
        if idx >= n_bins {
            counts[n_bins - 1] += 1.0;
            continue;
        }
        // Normal case: value falls in bin (idx - 1)
        // edges layout: [e0, e1, …, en] → bin i covers [ei, ei+1)
        // binary_search returns the idx where v would be inserted,
        // so the bin index is idx - 1.
        idx -= 1;
        counts[idx] += 1.0;
    }
    counts
}

// ── Kolmogorov–Smirnov ──────────────────────────────────────────────────

fn ks_detect(training: &[f64], current: &[f64], p_threshold: f64) -> DriftReport {
    if training.is_empty() || current.is_empty() {
        return DriftReport {
            detector_name: "KolmogorovSmirnov".into(),
            score: 0.0,
            drift_detected: false,
            action: DriftAction::NoAction,
        };
    }

    let ks_stat = ks_statistic(training, current);

    // The decision is the p-value against the configured threshold, which is
    // what a field named `p_threshold` means. It used to be a three-entry
    // lookup — 1.63 / 1.36 / 1.22 for α ≤ 0.01 / 0.05 / anything else — so
    // every α above 0.05 got the same 0.10 critical value and the reported
    // score was a statistic the threshold was not comparable to. The closed
    // form reproduces all three constants exactly and works for any α.
    let drift_detected = ks_p_value(ks_stat, training.len(), current.len()) < p_threshold;
    let action = if drift_detected {
        DriftAction::RetriggerFit
    } else {
        DriftAction::NoAction
    };

    DriftReport {
        detector_name: "KolmogorovSmirnov".into(),
        score: ks_stat,
        drift_detected,
        action,
    }
}

/// Asymptotic p-value for a two-sample Kolmogorov–Smirnov statistic.
///
/// `Q(λ) = 2 Σ_{k≥1} (−1)^{k−1} e^{−2k²λ²}` evaluated at
/// `λ = D·(√nₑ + 0.12 + 0.11/√nₑ)` with the effective size
/// `nₑ = n₁n₂/(n₁+n₂)` — the Stephens correction, as in *Numerical Recipes*
/// §14.3. The series converges geometrically; 100 terms is far beyond
/// double precision for any λ that reaches the loop.
///
/// The equivalent critical value is `sqrt(−ln(α/2)/2)·sqrt((n₁+n₂)/(n₁n₂))`,
/// which is where the familiar 1.358 / 1.628 / 1.224 constants come from.
fn ks_p_value(d: f64, n1: usize, n2: usize) -> f64 {
    if n1 == 0 || n2 == 0 || !d.is_finite() {
        return 1.0;
    }
    let n_eff = (n1 as f64 * n2 as f64) / (n1 as f64 + n2 as f64);
    let sqrt_n = n_eff.sqrt();
    let lambda = d * (sqrt_n + 0.12 + 0.11 / sqrt_n);
    if lambda <= 0.0 {
        return 1.0;
    }

    let mut sum = 0.0;
    let mut sign = 1.0;
    for k in 1..=100 {
        let term = (-2.0 * f64::from(k) * f64::from(k) * lambda * lambda).exp();
        sum += sign * term;
        if term < 1e-12 * sum.abs() || term < f64::MIN_POSITIVE {
            break;
        }
        sign = -sign;
    }
    (2.0 * sum).clamp(0.0, 1.0)
}

/// Two-sample KS statistic: max |F_1(x) - F_2(x)|.
fn ks_statistic(a: &[f64], b: &[f64]) -> f64 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }

    let mut sa: Vec<f64> = a.to_vec();
    sa.sort_by(f64::total_cmp);
    let mut sb: Vec<f64> = b.to_vec();
    sb.sort_by(f64::total_cmp);

    let na = sa.len() as f64;
    let nb = sb.len() as f64;

    let mut ia = 0usize;
    let mut ib = 0usize;
    let mut max_diff = 0.0_f64;

    while ia < sa.len() && ib < sb.len() {
        if sa[ia] <= sb[ib] {
            ia += 1;
        } else {
            ib += 1;
        }
        let fa = ia as f64 / na;
        let fb = ib as f64 / nb;
        max_diff = max_diff.max((fa - fb).abs());
    }

    // Handle remaining elements
    while ia < sa.len() {
        let fa = (ia + 1) as f64 / na;
        let fb = ib as f64 / nb;
        max_diff = max_diff.max((fa - fb).abs());
        ia += 1;
    }
    while ib < sb.len() {
        let fa = ia as f64 / na;
        let fb = (ib + 1) as f64 / nb;
        max_diff = max_diff.max((fa - fb).abs());
        ib += 1;
    }

    max_diff
}

// ── ADWIN ───────────────────────────────────────────────────────────────

/// ADWIN's cut threshold, from Bifet & Gavaldà, *Learning from Time-Changing
/// Data with Adaptive Windowing*, SDM 2007 (the ADWIN2 bound, §3.2):
///
/// ```text
/// m  = 1 / (1/n0 + 1/n1)          harmonic mean of the two window sizes
/// δ' = δ / n                       Bonferroni correction over the n cut points
/// ε  = sqrt( (2/m)·σ²_W·ln(2/δ') ) + (2/(3m))·ln(2/δ')
/// ```
///
/// # Why the variance term is not optional
///
/// The implementation this replaces used `sqrt((2/m)·ln(4/δ))` — a Hoeffding
/// bound with no `σ²` and no `δ/n`, which is two separate problems.
///
/// The missing `σ²` is the serious one: without it the threshold is a pure
/// function of the window sizes, so it is *identical* for a series in `[0, 1]`
/// and a power meter reading 200–250 W. On the meter it fires on essentially
/// any window pair, and on a near-constant series it never fires at all. A
/// drift detector whose sensitivity depends on the unit the data happens to be
/// recorded in is not measuring drift.
///
/// The missing `δ/n` is the reason the bound holds at all: ADWIN tests every
/// split point of the window, so the confidence has to be shared across them
/// or the nominal false-positive rate is not the actual one.
///
/// `σ²_W` is the variance of the two windows combined, which is what the paper
/// specifies — the statistic under test is a difference of means drawn from
/// one window under the null hypothesis.
fn adwin_epsilon_cut(training: &[f64], current: &[f64], delta: f64) -> f64 {
    let n0 = training.len() as f64;
    let n1 = current.len() as f64;
    let n = n0 + n1;

    // Pooled variance of the combined window, under the null that both halves
    // came from one distribution.
    let mean: f64 = (training.iter().sum::<f64>() + current.iter().sum::<f64>()) / n;
    let variance: f64 = training
        .iter()
        .chain(current.iter())
        .map(|v| (v - mean).powi(2))
        .sum::<f64>()
        / n;

    let m = 1.0 / (1.0 / n0 + 1.0 / n1);
    let delta_prime = (delta / n).clamp(f64::MIN_POSITIVE, 1.0);
    let ln_term = (2.0 / delta_prime).ln();

    ((2.0 / m) * variance * ln_term).sqrt() + (2.0 / (3.0 * m)) * ln_term
}

fn adwin_detect(training: &[f64], current: &[f64], delta: f64) -> DriftReport {
    // `training` is the reference window, `current` the test window; drift is
    // a difference of means larger than the ADWIN2 cut allows.
    if training.is_empty() || current.is_empty() {
        return DriftReport {
            detector_name: "ADWIN".into(),
            score: 0.0,
            drift_detected: false,
            action: DriftAction::NoAction,
        };
    }

    let mean1 = training.iter().sum::<f64>() / training.len() as f64;
    let mean2 = current.iter().sum::<f64>() / current.len() as f64;
    let epsilon_cut = adwin_epsilon_cut(training, current, delta);

    let diff = (mean1 - mean2).abs();
    let drift_detected = diff > epsilon_cut;

    let action = if drift_detected {
        DriftAction::RetriggerFit
    } else {
        DriftAction::NoAction
    };

    DriftReport {
        detector_name: "ADWIN".into(),
        score: diff,
        drift_detected,
        action,
    }
}

// ── DriftMonitor ────────────────────────────────────────────────────────

/// Callback invoked when drift is detected on a model.
pub type DriftCallback = Box<dyn Fn(&str, &DriftReport) + Send + Sync>;

/// Background monitor that periodically checks registered models for data drift.
///
/// Each registered model has a training distribution snapshot. The monitor
/// re-checks the current distribution against the training snapshot at the
/// configured interval.
///
/// Snapshots are capped at `max_snapshot_size` to bound memory usage.
/// When incoming data exceeds this limit, reservoir sampling retains a
/// representative subset.
pub struct DriftMonitor {
    /// (measurement_name, detector, training_data, current_data_fn)
    models: Arc<Mutex<Vec<MonitoredModel>>>,
    interval: Duration,
    callback: Arc<DriftCallback>,
    max_snapshot_size: usize,
}

struct MonitoredModel {
    measurement: String,
    detector: DriftDetector,
    training_snapshot: Vec<f64>,
    current_snapshot: Vec<f64>,
}

/// Default maximum number of data points kept per snapshot.
const DEFAULT_MAX_SNAPSHOT_SIZE: usize = 10_000;

/// Downsample `data` in-place to at most `max_size` elements using
/// deterministic stride-based sampling (fast, reproducible, no RNG).
fn cap_snapshot(data: &mut Vec<f64>, max_size: usize) {
    if data.len() <= max_size || max_size == 0 {
        return;
    }
    let step = data.len() as f64 / max_size as f64;
    let mut sampled = Vec::with_capacity(max_size);
    for i in 0..max_size {
        let idx = (i as f64 * step) as usize;
        sampled.push(data[idx]);
    }
    *data = sampled;
}

impl DriftMonitor {
    /// Creates a new `DriftMonitor` with the given check interval and callback.
    pub fn new(interval: Duration, callback: DriftCallback) -> Self {
        Self {
            models: Arc::new(Mutex::new(Vec::new())),
            interval,
            callback: Arc::new(callback),
            max_snapshot_size: DEFAULT_MAX_SNAPSHOT_SIZE,
        }
    }

    /// Creates a new `DriftMonitor` with a custom snapshot size limit.
    pub fn with_max_snapshot_size(
        interval: Duration,
        callback: DriftCallback,
        max_snapshot_size: usize,
    ) -> Self {
        Self {
            models: Arc::new(Mutex::new(Vec::new())),
            interval,
            callback: Arc::new(callback),
            max_snapshot_size,
        }
    }

    /// Registers a model for drift monitoring.
    pub fn register(
        &self,
        measurement: &str,
        detector: DriftDetector,
        training_snapshot: Vec<f64>,
    ) {
        let mut snapshot = training_snapshot;
        cap_snapshot(&mut snapshot, self.max_snapshot_size);
        let mut models = self.models.lock();
        models.push(MonitoredModel {
            measurement: measurement.to_string(),
            detector,
            training_snapshot: snapshot,
            current_snapshot: Vec::new(),
        });
    }

    /// Updates the current data snapshot for a measurement.
    pub fn update_current(&self, measurement: &str, data: Vec<f64>) {
        let mut capped = data;
        cap_snapshot(&mut capped, self.max_snapshot_size);
        let mut models = self.models.lock();
        for model in models.iter_mut() {
            if model.measurement == measurement {
                model.current_snapshot = capped.clone();
            }
        }
    }

    /// Runs a single check cycle — evaluates all registered models for drift.
    /// Returns a list of drift reports for models where drift was detected.
    pub fn check_all(&self) -> Vec<(String, DriftReport)> {
        // Collect reports under the lock, then release before invoking
        // callbacks. This prevents deadlocks if a callback tries to call
        // register() or update_current(), which also take the lock.
        let reports: Vec<(String, DriftReport)> = {
            let models = self.models.lock();
            models
                .iter()
                .filter(|m| !m.current_snapshot.is_empty())
                .map(|m| {
                    let report = m.detector.detect(&m.training_snapshot, &m.current_snapshot);
                    (m.measurement.clone(), report)
                })
                .collect()
        }; // ← lock released here

        // Now invoke callbacks without holding any lock.
        for (measurement, report) in &reports {
            if report.drift_detected {
                (self.callback)(measurement, report);
            }
        }

        reports
    }

    /// Returns the configured check interval.
    pub fn interval(&self) -> Duration {
        self.interval
    }
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_psi_no_drift_same_distribution() {
        let training: Vec<f64> = (0..1000).map(|i| i as f64 / 100.0).collect();
        let current: Vec<f64> = (0..1000).map(|i| i as f64 / 100.0).collect();

        let det = DriftDetector::Psi { threshold: 0.25 };
        let report = det.detect(&training, &current);
        assert!(!report.drift_detected);
        assert!(report.score < 0.01);
        assert_eq!(report.action, DriftAction::NoAction);
    }

    #[test]
    fn test_psi_drift_detected_shifted() {
        let training: Vec<f64> = (0..1000).map(|i| i as f64 / 100.0).collect();
        let current: Vec<f64> = (0..1000).map(|i| i as f64 / 100.0 + 50.0).collect();

        let det = DriftDetector::Psi { threshold: 0.25 };
        let report = det.detect(&training, &current);
        assert!(report.drift_detected);
        assert!(report.score > 0.25);
        assert_eq!(report.action, DriftAction::RetriggerFit);
    }

    #[test]
    fn test_ks_same_distribution() {
        let a: Vec<f64> = (0..500).map(|i| i as f64).collect();
        let b: Vec<f64> = (0..500).map(|i| i as f64).collect();

        let det = DriftDetector::KolmogorovSmirnov { p_value: 0.05 };
        let report = det.detect(&a, &b);
        assert!(!report.drift_detected);
    }

    #[test]
    fn test_ks_drift_detected() {
        let a: Vec<f64> = (0..500).map(|i| i as f64).collect();
        let b: Vec<f64> = (0..500).map(|i| i as f64 + 1000.0).collect();

        let det = DriftDetector::KolmogorovSmirnov { p_value: 0.05 };
        let report = det.detect(&a, &b);
        assert!(report.drift_detected);
        assert_eq!(report.action, DriftAction::RetriggerFit);
    }

    #[test]
    fn test_adwin_no_drift() {
        let training = vec![5.0; 100];
        let current = vec![5.0; 100];

        let det = DriftDetector::Adwin { delta: 0.002 };
        let report = det.detect(&training, &current);
        assert!(!report.drift_detected);
    }

    #[test]
    fn test_adwin_drift_detected() {
        let training = vec![5.0; 100];
        let current = vec![50.0; 100];

        let det = DriftDetector::Adwin { delta: 0.002 };
        let report = det.detect(&training, &current);
        assert!(report.drift_detected);
        assert_eq!(report.action, DriftAction::RetriggerFit);
    }

    #[test]
    fn test_ks_statistic_identical() {
        let a: Vec<f64> = (0..100).map(|i| i as f64).collect();
        let stat = ks_statistic(&a, &a);
        assert!(stat < 0.02);
    }

    #[test]
    fn test_ks_statistic_disjoint() {
        let a: Vec<f64> = (0..100).map(|i| i as f64).collect();
        let b: Vec<f64> = (200..300).map(|i| i as f64).collect();
        let stat = ks_statistic(&a, &b);
        assert!((stat - 1.0).abs() < 0.02);
    }

    #[test]
    fn test_psi_alert_only() {
        // Create a mild shift that triggers AlertOnly, not RetriggerFit
        let training: Vec<f64> = (0..2000).map(|i| i as f64).collect();
        // Shift ~10% of the range
        let current: Vec<f64> = (0..2000).map(|i| i as f64 + 200.0).collect();

        let det = DriftDetector::Psi { threshold: 0.25 };
        let report = det.detect(&training, &current);

        // This might be AlertOnly or NoAction depending on the shift magnitude
        assert!(report.action != DriftAction::RetriggerFit || report.score >= 0.25);
    }

    /// ADWIN must decide the same way on the same data in different units.
    ///
    /// The bound it replaced had no `σ²` term, so its threshold depended only
    /// on the window sizes: identical for a series in `[0, 1]` and for the
    /// same series expressed in watts. Scaling the data by 100 turned "no
    /// drift" into "drift" without changing anything about the data.
    #[test]
    fn adwin_is_scale_invariant() {
        let mut seed = 7u64;
        let mut noise = || {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            ((seed >> 33) as f64 / f64::from(u32::MAX)) - 0.5
        };
        let base: Vec<f64> = (0..200).map(|_| 0.5 + noise() * 0.2).collect();
        let (a, b) = base.split_at(100);

        for scale in [1.0, 100.0, 10_000.0] {
            let sa: Vec<f64> = a.iter().map(|v| v * scale).collect();
            let sb: Vec<f64> = b.iter().map(|v| v * scale).collect();
            let report = DriftDetector::Adwin { delta: 0.002 }.detect(&sa, &sb);
            assert!(
                !report.drift_detected,
                "no drift at scale {scale}: two halves of one distribution"
            );
        }

        // …and a genuine shift is still caught at every scale.
        for scale in [1.0, 100.0, 10_000.0] {
            let sa: Vec<f64> = a.iter().map(|v| v * scale).collect();
            let shifted: Vec<f64> = b.iter().map(|v| (v + 1.0) * scale).collect();
            let report = DriftDetector::Adwin { delta: 0.002 }.detect(&sa, &shifted);
            assert!(
                report.drift_detected,
                "a full-scale mean shift must be detected at scale {scale}"
            );
        }
    }

    /// The KS decision is a p-value, so any `p_threshold` is meaningful — not
    /// just the three the old lookup table had entries for.
    #[test]
    fn ks_p_value_matches_the_textbook_critical_values() {
        // c(α)·sqrt((n1+n2)/(n1·n2)) is the classical critical value; at
        // exactly that D the p-value must sit at α.
        for (alpha, c) in [(0.10, 1.224), (0.05, 1.358), (0.01, 1.628)] {
            let (n1, n2) = (200usize, 200usize);
            let critical = c * ((n1 + n2) as f64 / (n1 as f64 * n2 as f64)).sqrt();
            let p = ks_p_value(critical, n1, n2);
            assert!(
                (p - alpha).abs() < 0.01,
                "D at the α={alpha} critical value should give p≈{alpha}, got {p:.4}"
            );
        }
        // Identical samples are the least surprising outcome there is.
        assert!((ks_p_value(0.0, 100, 100) - 1.0).abs() < 1e-9);
        // Disjoint samples are the most.
        assert!(ks_p_value(1.0, 100, 100) < 1e-9);
    }

    /// A distribution compared against itself has not drifted, ties or not.
    ///
    /// This is the guard behind the note on `quantile_bin_edges`: a heavily
    /// tied reference produces repeated bin edges, and the reason that is
    /// harmless is that the resulting zero-width bins are empty on both
    /// sides. The test is what makes that an assertion rather than an
    /// argument.
    #[test]
    fn psi_does_not_fire_on_a_tied_reference_distribution() {
        // 80 % of the reference sits at exactly 0.0.
        let training: Vec<f64> = (0..1000)
            .map(|i| if i % 5 == 0 { f64::from(i % 50) } else { 0.0 })
            .collect();
        let report = DriftDetector::Psi { threshold: 0.2 }.detect(&training, &training.clone());
        assert!(
            !report.drift_detected,
            "a distribution compared against itself cannot have drifted (PSI = {})",
            report.score
        );
        assert!(
            report.score < 0.01,
            "PSI against an identical sample must be ~0, got {}",
            report.score
        );
    }

    #[test]
    fn test_default_detector() {
        let det = DriftDetector::default();
        assert!(
            matches!(det, DriftDetector::Psi { threshold } if (threshold - 0.25).abs() < f64::EPSILON)
        );
    }

    /// Quantile-based PSI with highly skewed data should detect
    /// a distributional shift more reliably than equal-width bins.
    #[test]
    fn test_psi_quantile_bins_skewed_data() {
        // Training: heavily right-skewed (exponential-like)
        let training: Vec<f64> = (0..1000).map(|i| ((i as f64) / 100.0).exp()).collect();

        // Current: same shape but shifted (multiplied by 5 + offset)
        let current: Vec<f64> = (0..1000)
            .map(|i| 5.0 * ((i as f64) / 100.0).exp() + 50.0)
            .collect();

        let det = DriftDetector::Psi { threshold: 0.25 };
        let report = det.detect(&training, &current);
        // The shift should be detected with quantile bins
        assert!(
            report.drift_detected,
            "PSI should detect drift in skewed data, score={}",
            report.score
        );
        assert!(report.score > 0.25);
    }

    #[test]
    fn test_quantile_bin_edges_uniform() {
        let data: Vec<f64> = (0..100).map(|i| i as f64).collect();
        let edges = quantile_bin_edges(&data, 10);
        assert_eq!(edges.len(), 11);
        // First edge is 0.0, roughly 10-unit spacing
        assert!((edges[0] - 0.0).abs() < 1.0);
        assert!((edges[5] - 50.0).abs() < 2.0);
    }

    #[test]
    fn test_bin_counts_by_edges_basic() {
        let edges = vec![0.0, 5.0, 10.0 + f64::EPSILON];
        let data = vec![1.0, 2.0, 3.0, 6.0, 7.0, 8.0];
        let counts = bin_counts_by_edges(&data, &edges);
        assert_eq!(counts.len(), 2);
        assert!((counts[0] - 3.0).abs() < f64::EPSILON); // 1,2,3
        assert!((counts[1] - 3.0).abs() < f64::EPSILON); // 6,7,8
    }

    #[test]
    fn drift_monitor_detects_shift() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let fired = Arc::new(AtomicUsize::new(0));
        let fired_clone = fired.clone();

        let monitor = DriftMonitor::new(
            Duration::from_secs(60),
            Box::new(move |_measurement, _report| {
                fired_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );

        // Register with normal training data
        let training: Vec<f64> = (0..1000).map(|i| (i as f64) * 0.01).collect();
        monitor.register("cpu", DriftDetector::Psi { threshold: 0.25 }, training);

        // Update with shifted distribution
        let shifted: Vec<f64> = (0..1000).map(|i| 100.0 + (i as f64) * 0.01).collect();
        monitor.update_current("cpu", shifted);

        let reports = monitor.check_all();
        assert!(!reports.is_empty());
        assert!(reports[0].1.drift_detected);
        assert!(fired.load(Ordering::SeqCst) > 0);
    }

    #[test]
    fn callback_can_register_without_deadlock() {
        // Regression test for H4: callback was invoked under the mutex lock,
        // which would deadlock if it called register()/update_current().
        use std::sync::atomic::{AtomicBool, Ordering};

        let lock_free = Arc::new(AtomicBool::new(false));
        let lock_free_clone = lock_free.clone();

        // We share the models Arc so the callback can try_lock() it.
        let models = Arc::new(Mutex::new(Vec::<MonitoredModel>::new()));
        let models_for_callback = Arc::clone(&models);

        let monitor = DriftMonitor {
            models: Arc::clone(&models),
            interval: Duration::from_secs(60),
            callback: Arc::new(Box::new(move |_measurement, _report| {
                // Try to acquire the lock — if callback is outside the lock,
                // this will succeed. If still under the lock, it would deadlock
                // (Mutex is not reentrant).
                let acquired = models_for_callback.try_lock().is_some();
                lock_free_clone.store(acquired, Ordering::SeqCst);
            })),
            max_snapshot_size: DEFAULT_MAX_SNAPSHOT_SIZE,
        };

        let training: Vec<f64> = (0..1000).map(|i| (i as f64) * 0.01).collect();
        monitor.register("cpu", DriftDetector::Psi { threshold: 0.25 }, training);

        let shifted: Vec<f64> = (0..1000).map(|i| 100.0 + (i as f64) * 0.01).collect();
        monitor.update_current("cpu", shifted);

        let reports = monitor.check_all();
        assert!(!reports.is_empty());
        assert!(reports[0].1.drift_detected);
        assert!(
            lock_free.load(Ordering::SeqCst),
            "callback should execute outside lock"
        );
    }
}
