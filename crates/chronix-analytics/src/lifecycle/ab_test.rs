//! Champion/Challenger A/B testing for model evaluation.
//!
//! During prediction, traffic is probabilistically routed between the
//! champion and a challenger model.  Both predictions are logged against
//! actuals, and an evaluator determines when to promote the challenger.

use std::collections::VecDeque;

use parking_lot::Mutex;

use crate::lifecycle::registry::AccuracyMetrics;

/// Relative+absolute tolerance comparison for float metrics.
///
/// Uses `1e-9` relative tolerance with a `1e-12` absolute floor,
/// correctly handling values across all magnitudes (MAPE 0.01–100+,
/// RMSE 0.001–1000+).
#[inline]
fn approx_eq(a: f64, b: f64) -> bool {
    let diff = (a - b).abs();
    let magnitude = a.abs().max(b.abs());
    diff <= 1e-9 * magnitude.max(1e-3)
}

/// Criterion for promoting a challenger to champion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PromotionCriteria {
    /// Promote when challenger's MAPE is lower.
    LowerMape,
    /// Promote when challenger's RMSE is lower.
    LowerRmse,
    /// Promote when both MAPE and RMSE are lower.
    LowerBoth,
}

/// Configuration for an A/B test.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ABTestConfig {
    /// Fraction of traffic routed to challenger `[0.0, 1.0]`. Default: `0.10`.
    pub challenger_traffic_pct: f64,
    /// Number of evaluation windows before a decision is made. Default: 10.
    pub evaluation_windows: usize,
    /// Promotion criterion.
    pub criteria: PromotionCriteria,
}

impl Default for ABTestConfig {
    fn default() -> Self {
        Self {
            challenger_traffic_pct: 0.10,
            evaluation_windows: 10,
            criteria: PromotionCriteria::LowerMape,
        }
    }
}

/// Result of an A/B test evaluation round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ABTestResult {
    /// Promote the challenger to champion.
    Promote,
    /// Keep the current champion; retire the challenger.
    Keep,
    /// Not enough data for a decision yet.
    Inconclusive,
}

/// Logged prediction record.
#[derive(Debug, Clone)]
struct PredictionRecord {
    actual: f64,
    champion_prediction: f64,
    challenger_prediction: f64,
}

/// Evaluator that collects prediction pairs and judges the test.
///
/// Thread-safe — mutable state is wrapped in `Mutex`.
#[derive(Debug)]
pub struct ABTestEvaluator {
    config: ABTestConfig,
    inner: Mutex<ABTestInner>,
}

#[derive(Debug)]
struct ABTestInner {
    records: VecDeque<PredictionRecord>,
    consecutive_wins: usize,
    last_eval_count: usize,
}

impl ABTestEvaluator {
    /// Creates a new A/B test evaluator with the given configuration.
    pub fn new(config: ABTestConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(ABTestInner {
                records: VecDeque::new(),
                consecutive_wins: 0,
                last_eval_count: 0,
            }),
        }
    }

    /// Decide whether a specific request should be routed to the challenger.
    ///
    /// Uses a simple deterministic hash on the `request_id` for consistent
    /// routing (no randomness in tight loops).
    pub fn should_use_challenger(&self, request_id: u64) -> bool {
        let frac = (request_id % 10000) as f64 / 10000.0;
        frac < self.config.challenger_traffic_pct
    }

    /// Log a prediction pair (both champion and challenger predictions along
    /// with the actual observed value).
    pub fn log_prediction(&self, actual: f64, champion_pred: f64, challenger_pred: f64) {
        let mut inner = self.inner.lock();
        inner.records.push_back(PredictionRecord {
            actual,
            champion_prediction: champion_pred,
            challenger_prediction: challenger_pred,
        });

        // Keep bounded to 10× evaluation_windows
        let max_records = self.config.evaluation_windows * 100;
        while inner.records.len() > max_records {
            inner.records.pop_front();
        }
    }

    /// Evaluate the A/B test based on logged records.
    ///
    /// Computes metrics over the most recent `evaluation_windows` records
    /// to avoid stale early predictions diluting accuracy.
    pub fn evaluate(&self) -> ABTestResult {
        let mut inner = self.inner.lock();
        if inner.records.len() < self.config.evaluation_windows {
            return ABTestResult::Inconclusive;
        }

        // Only count as a new evaluation window when fresh data has arrived.
        let current_count = inner.records.len();
        if current_count == inner.last_eval_count {
            // No new data — return current state without inflating counters.
            return if inner.consecutive_wins >= 3 {
                ABTestResult::Promote
            } else if current_count >= self.config.evaluation_windows * 5
                && inner.consecutive_wins == 0
            {
                ABTestResult::Keep
            } else {
                ABTestResult::Inconclusive
            };
        }
        inner.last_eval_count = current_count;

        // Use only the most recent window of records for metric computation
        let window = self.config.evaluation_windows;
        let recent = inner.records.iter().rev().take(window);
        let actuals: Vec<f64> = recent.clone().map(|r| r.actual).collect();
        let champ_preds: Vec<f64> = recent.clone().map(|r| r.champion_prediction).collect();
        let chal_preds: Vec<f64> = recent.map(|r| r.challenger_prediction).collect();

        let champ_metrics = AccuracyMetrics::compute(&actuals, &champ_preds);
        let chal_metrics = AccuracyMetrics::compute(&actuals, &chal_preds);

        metrics::gauge!("chronix_model_ab_test_champion_mape").set(champ_metrics.mape);
        metrics::gauge!("chronix_model_ab_test_challenger_mape").set(chal_metrics.mape);

        let challenger_wins = match self.config.criteria {
            PromotionCriteria::LowerMape => {
                !chal_metrics.mape.is_nan()
                    && !champ_metrics.mape.is_nan()
                    && chal_metrics.mape < champ_metrics.mape
            }
            PromotionCriteria::LowerRmse => {
                !chal_metrics.rmse.is_nan()
                    && !champ_metrics.rmse.is_nan()
                    && chal_metrics.rmse < champ_metrics.rmse
            }
            PromotionCriteria::LowerBoth => {
                chal_metrics.rmse < champ_metrics.rmse
                    && !chal_metrics.mape.is_nan()
                    && !champ_metrics.mape.is_nan()
                    && chal_metrics.mape < champ_metrics.mape
            }
        };

        // Tie detection: when challenger and champion produce exactly
        // equal metrics under the active criterion. On a tie the
        // `consecutive_wins` counter is left unchanged — the streak is
        // neither extended nor broken.  This prevents a lucky sequence
        // of ties from inflating the counter, while also avoiding
        // penalising the challenger for matching the champion exactly.
        // Use relative tolerance (1e-9) for tie detection
        // instead of f64::EPSILON (~2.2e-16) which is meaningless for
        // MAPE/RMSE values in the range 0.01-100+.
        let is_tie = match self.config.criteria {
            PromotionCriteria::LowerMape => {
                !chal_metrics.mape.is_nan()
                    && !champ_metrics.mape.is_nan()
                    && approx_eq(chal_metrics.mape, champ_metrics.mape)
            }
            PromotionCriteria::LowerRmse => {
                !chal_metrics.rmse.is_nan()
                    && !champ_metrics.rmse.is_nan()
                    && approx_eq(chal_metrics.rmse, champ_metrics.rmse)
            }
            PromotionCriteria::LowerBoth => {
                !chal_metrics.mape.is_nan()
                    && !champ_metrics.mape.is_nan()
                    && approx_eq(chal_metrics.mape, champ_metrics.mape)
                    && !chal_metrics.rmse.is_nan()
                    && !champ_metrics.rmse.is_nan()
                    && approx_eq(chal_metrics.rmse, champ_metrics.rmse)
            }
        };

        if challenger_wins {
            inner.consecutive_wins += 1;
        } else if is_tie {
            // Tie: preserve existing streak — the challenger matched
            // the champion exactly, so no reason to penalise it.
        } else {
            inner.consecutive_wins = 0;
        }

        // Require sustained wins across multiple evaluations
        if inner.consecutive_wins >= 3 {
            ABTestResult::Promote
        } else if inner.records.len() >= self.config.evaluation_windows * 5
            && inner.consecutive_wins == 0
        {
            ABTestResult::Keep
        } else {
            ABTestResult::Inconclusive
        }
    }

    /// Champion accuracy from logged data.
    pub fn champion_metrics(&self) -> Option<AccuracyMetrics> {
        let inner = self.inner.lock();
        if inner.records.is_empty() {
            return None;
        }
        let actuals: Vec<f64> = inner.records.iter().map(|r| r.actual).collect();
        let preds: Vec<f64> = inner
            .records
            .iter()
            .map(|r| r.champion_prediction)
            .collect();
        Some(AccuracyMetrics::compute(&actuals, &preds))
    }

    /// Challenger accuracy from logged data.
    pub fn challenger_metrics(&self) -> Option<AccuracyMetrics> {
        let inner = self.inner.lock();
        if inner.records.is_empty() {
            return None;
        }
        let actuals: Vec<f64> = inner.records.iter().map(|r| r.actual).collect();
        let preds: Vec<f64> = inner
            .records
            .iter()
            .map(|r| r.challenger_prediction)
            .collect();
        Some(AccuracyMetrics::compute(&actuals, &preds))
    }

    /// Number of logged records.
    pub fn record_count(&self) -> usize {
        self.inner.lock().records.len()
    }
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_routing_fraction() {
        let eval = ABTestEvaluator::new(ABTestConfig {
            challenger_traffic_pct: 0.10,
            ..Default::default()
        });

        let mut challenger_count = 0;
        for id in 0..10000 {
            if eval.should_use_challenger(id) {
                challenger_count += 1;
            }
        }
        // ~10% of 10000 = ~1000
        assert!((900..=1100).contains(&challenger_count));
    }

    #[test]
    fn test_inconclusive_with_few_records() {
        let eval = ABTestEvaluator::new(ABTestConfig {
            evaluation_windows: 10,
            ..Default::default()
        });

        for i in 0..5 {
            eval.log_prediction(100.0, 110.0 + i as f64, 105.0 + i as f64);
        }

        assert_eq!(eval.evaluate(), ABTestResult::Inconclusive);
    }

    #[test]
    fn test_promote_when_challenger_consistently_better() {
        let eval = ABTestEvaluator::new(ABTestConfig {
            evaluation_windows: 5,
            criteria: PromotionCriteria::LowerMape,
            ..Default::default()
        });

        // Challenger is consistently better
        for i in 0..50 {
            let actual = 100.0 + (i as f64 * 0.1).sin();
            let champ_pred = actual + 10.0; // champion off by 10
            let chal_pred = actual + 1.0; // challenger off by 1
            eval.log_prediction(actual, champ_pred, chal_pred);
        }

        // Need 3 consecutive evaluations with challenger winning.
        // Each evaluation requires new data to avoid no-op re-evaluation.
        for batch in 0..3 {
            for i in 0..5 {
                let actual = 100.0 + ((50 + batch * 5 + i) as f64 * 0.1).sin();
                let champ_pred = actual + 10.0;
                let chal_pred = actual + 1.0;
                eval.log_prediction(actual, champ_pred, chal_pred);
            }
            eval.evaluate();
        }
        let result = eval.evaluate();
        assert_eq!(result, ABTestResult::Promote);
    }

    #[test]
    fn test_keep_when_champion_wins() {
        let eval = ABTestEvaluator::new(ABTestConfig {
            evaluation_windows: 5,
            criteria: PromotionCriteria::LowerRmse,
            ..Default::default()
        });

        // Champion is better
        for i in 0..50 {
            let actual = 100.0 + (i as f64 * 0.1).sin();
            let champ_pred = actual + 0.5;
            let chal_pred = actual + 20.0;
            eval.log_prediction(actual, champ_pred, chal_pred);
        }

        let result = eval.evaluate();
        assert_eq!(result, ABTestResult::Keep);
    }

    #[test]
    fn test_metrics_available() {
        let eval = ABTestEvaluator::new(Default::default());
        eval.log_prediction(100.0, 110.0, 105.0);
        eval.log_prediction(200.0, 190.0, 195.0);

        let cm = eval.champion_metrics().unwrap();
        let clm = eval.challenger_metrics().unwrap();
        assert!(cm.mae > 0.0);
        assert!(clm.mae > 0.0);
        assert!(clm.mae < cm.mae); // challenger closer
    }

    #[test]
    fn test_record_count() {
        let eval = ABTestEvaluator::new(Default::default());
        assert_eq!(eval.record_count(), 0);
        eval.log_prediction(1.0, 1.0, 1.0);
        assert_eq!(eval.record_count(), 1);
    }

    /// When challenger MAPE exactly equals champion MAPE (tie),
    /// `consecutive_wins` must be preserved — not reset to zero.
    #[test]
    fn test_ab_test_tie_preserves_streak() {
        let eval = ABTestEvaluator::new(ABTestConfig {
            evaluation_windows: 5,
            criteria: PromotionCriteria::LowerMape,
            ..Default::default()
        });

        // ── Round 1: challenger wins (lower MAPE) ──────────────
        for _ in 0..10 {
            let actual = 100.0;
            let champ_pred = 110.0; // 10% error
            let chal_pred = 101.0; // 1% error
            eval.log_prediction(actual, champ_pred, chal_pred);
        }
        let r1 = eval.evaluate();
        assert_eq!(r1, ABTestResult::Inconclusive); // 1 consecutive win

        // ── Round 2: challenger wins again ─────────────────────
        for _ in 0..5 {
            eval.log_prediction(100.0, 110.0, 101.0);
        }
        let r2 = eval.evaluate();
        assert_eq!(r2, ABTestResult::Inconclusive); // 2 consecutive wins

        // ── Round 3: TIE — identical predictions ──────────────
        // Both champion and challenger predict with the same error.
        for _ in 0..5 {
            eval.log_prediction(100.0, 105.0, 105.0);
        }
        let r3 = eval.evaluate();
        // Streak should NOT be reset; still at 2 consecutive wins
        assert_eq!(r3, ABTestResult::Inconclusive);

        // ── Round 4: challenger wins again ─────────────────────
        for _ in 0..5 {
            eval.log_prediction(100.0, 110.0, 101.0);
        }
        let r4 = eval.evaluate();
        // This should be the 3rd win (2 before tie + 1 now) → Promote
        assert_eq!(r4, ABTestResult::Promote);
    }
}
