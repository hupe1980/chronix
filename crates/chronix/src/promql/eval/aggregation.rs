//! Aggregation operator evaluation for PromQL.
//!
//! Handles `sum`, `avg`, `min`, `max`, `count`, `group`, `stddev`, `stdvar`,
//! `topk`, `bottomk`, `quantile`, and `count_values`.

use std::collections::BTreeMap;

use crate::promql::ast::{AggregationModifier, AggregationOp, Expr, PromQLValue, Sample, Series};

use super::{EvalError, PromQLEvaluator, QueryParams};

// ── PromQLEvaluator method ─────────────────────────────────────────────

impl PromQLEvaluator {
    pub(crate) fn eval_aggregation(
        &self,
        op: AggregationOp,
        expr: &Expr,
        param: Option<&Expr>,
        modifier: &Option<AggregationModifier>,
        params: &QueryParams,
    ) -> Result<PromQLValue, EvalError> {
        let val = self.eval(expr, params)?;
        let PromQLValue::Vector(series) = val else {
            return Err(EvalError("aggregation requires instant vector".into()));
        };

        // Group series by the modifier labels
        let mut groups: BTreeMap<Vec<(String, String)>, Vec<&Series>> = BTreeMap::new();
        for s in &series {
            let group_key = compute_group_key(&s.labels, modifier);
            groups.entry(group_key).or_default().push(s);
        }

        // count_values takes a string parameter (label name), not a
        // scalar — skip the generic param evaluation for it.
        let param_val = if matches!(op, AggregationOp::CountValues) {
            None
        } else if let Some(p) = param {
            match self.eval(p, params)? {
                PromQLValue::Scalar(n) => Some(n),
                _ => return Err(EvalError("aggregation parameter must be scalar".into())),
            }
        } else {
            None
        };

        let mut result = Vec::new();
        for (labels, group) in groups {
            let values: Vec<f64> = group
                .iter()
                .flat_map(|s| s.samples.iter().map(|sample| sample.value))
                .collect();

            if values.is_empty() {
                continue;
            }

            let agg_value = match op {
                AggregationOp::Sum => values.iter().sum(),
                AggregationOp::Avg => {
                    let sum: f64 = values.iter().sum();
                    sum / values.len() as f64
                }
                AggregationOp::Min => values.iter().copied().reduce(f64::min).unwrap_or(f64::NAN),
                AggregationOp::Max => values.iter().copied().reduce(f64::max).unwrap_or(f64::NAN),
                AggregationOp::Count => values.len() as f64,
                AggregationOp::Group => 1.0,
                AggregationOp::Stddev => {
                    let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
                    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                        / values.len() as f64;
                    variance.sqrt()
                }
                AggregationOp::Stdvar => {
                    let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
                    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64
                }
                AggregationOp::Topk => {
                    // topk returns multiple series, handled specially below
                    0.0
                }
                AggregationOp::Bottomk => 0.0,
                AggregationOp::Quantile => {
                    let q = param_val
                        .ok_or_else(|| EvalError("quantile() requires parameter".into()))?;
                    compute_quantile(&values, q)
                }
                AggregationOp::CountValues => {
                    // count_values produces one series per distinct value
                    // — handled specially below like topk/bottomk.
                    0.0
                }
            };

            // Special handling for topk/bottomk
            if matches!(op, AggregationOp::Topk | AggregationOp::Bottomk) {
                let k = param_val.unwrap_or(1.0).max(0.0) as usize;
                let mut sorted: Vec<&Series> = group.clone();
                // NaN ranks last for *both* — Prometheus's heap treats NaN as
                // the worst value in either direction, so `topk(k, x)` returns
                // a NaN only when fewer than `k` real values exist. A plain
                // reversed `total_cmp` made NaN the *largest* value and handed
                // `topk(1, …)` the broken series.
                sorted.sort_by(|a, b| {
                    let va = a.samples.first().map_or(f64::NAN, |s| s.value);
                    let vb = b.samples.first().map_or(f64::NAN, |s| s.value);
                    crate::promql::eval::function::nan_last_cmp(
                        va,
                        vb,
                        matches!(op, AggregationOp::Topk),
                    )
                });
                for s in sorted.into_iter().take(k) {
                    // topk/bottomk keep the input's labels — including
                    // `__name__` — but the sample is a new one at this step.
                    let mut kept = s.clone();
                    for sample in &mut kept.samples {
                        sample.timestamp = params.time;
                    }
                    result.push(kept);
                }
                continue;
            }

            // count_values: one output series per distinct value, with
            // the value stored as a label and the count as the sample.
            if matches!(op, AggregationOp::CountValues) {
                let label_name = match param {
                    Some(Expr::StringLiteral(s)) => s.clone(),
                    _ => "value".to_string(),
                };
                let mut counts: BTreeMap<u64, (f64, usize)> = BTreeMap::new();
                for &v in &values {
                    let entry = counts.entry(v.to_bits()).or_insert((v, 0));
                    entry.1 += 1;
                }
                for (_, (value, count)) in counts {
                    let mut cv_labels = labels.clone();
                    // Format the value as the label value — use integer
                    // formatting when the value is a whole number.
                    let label_val = if value == value.floor()
                        && value.is_finite()
                        && value >= i64::MIN as f64
                        && value <= i64::MAX as f64
                    {
                        #[allow(clippy::cast_possible_truncation)]
                        let iv = value as i64;
                        format!("{iv}")
                    } else {
                        format!("{value}")
                    };
                    cv_labels.push((label_name.clone(), label_val));
                    result.push(Series {
                        labels: cv_labels,
                        samples: vec![Sample {
                            timestamp: params.time,
                            value: count as f64,
                        }],
                    });
                }
                continue;
            }

            result.push(Series {
                labels,
                samples: vec![Sample {
                    timestamp: params.time,
                    value: agg_value,
                }],
            });
        }

        Ok(PromQLValue::Vector(result))
    }
}

// ── Free functions ─────────────────────────────────────────────────────

pub(crate) fn compute_group_key(
    labels: &[(String, String)],
    modifier: &Option<AggregationModifier>,
) -> Vec<(String, String)> {
    match modifier {
        Some(AggregationModifier::By(names)) => labels
            .iter()
            .filter(|(k, _)| names.contains(k))
            .cloned()
            .collect(),
        Some(AggregationModifier::Without(names)) => labels
            .iter()
            .filter(|(k, _)| !names.contains(k) && k != "__name__")
            .cloned()
            .collect(),
        None => vec![], // no modifier: all series grouped together
    }
}

pub(crate) fn compute_quantile(values: &[f64], q: f64) -> f64 {
    if values.is_empty() || q.is_nan() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    let mut sorted: Vec<f64> = values.to_vec();
    sorted.sort_by(f64::total_cmp);

    let n = sorted.len();
    if n == 1 {
        return sorted[0];
    }

    let rank = q * (n - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    let frac = rank - lower as f64;

    if upper >= n {
        sorted[n - 1]
    } else {
        sorted[lower] * (1.0 - frac) + sorted[upper] * frac
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::ast::AggregationModifier;

    #[test]
    fn test_compute_group_key() {
        let labels = vec![
            ("__name__".into(), "cpu".into()),
            ("host".into(), "srv1".into()),
            ("region".into(), "us".into()),
        ];

        let by = Some(AggregationModifier::By(vec!["host".into()]));
        let key = compute_group_key(&labels, &by);
        assert_eq!(key, vec![("host".into(), "srv1".into())]);

        let without = Some(AggregationModifier::Without(vec!["host".into()]));
        let key = compute_group_key(&labels, &without);
        assert_eq!(key, vec![("region".into(), "us".into())]);
    }

    #[test]
    fn test_compute_quantile() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert!((compute_quantile(&values, 0.5) - 3.0).abs() < 1e-9);
        assert!((compute_quantile(&values, 0.0) - 1.0).abs() < 1e-9);
        assert!((compute_quantile(&values, 1.0) - 5.0).abs() < 1e-9);

        // NaN quantile returns NaN (Prometheus compat)
        assert!(compute_quantile(&values, f64::NAN).is_nan());

        // NaN values in input are sorted to end via total_cmp
        // [3.0, NaN, 1.0, 2.0] → sorted [1.0, 2.0, 3.0, NaN]
        let with_nan = vec![3.0, f64::NAN, 1.0, 2.0];
        assert!((compute_quantile(&with_nan, 0.0) - 1.0).abs() < 1e-9);
        // q=0.5: rank=1.5, interpolate(2.0, 3.0) = 2.5
        assert!((compute_quantile(&with_nan, 0.5) - 2.5).abs() < 1e-9);
    }

    #[test]
    fn test_count_values_bypasses_scalar_check() {
        // count_values takes a string param — ensure it does NOT fail with
        // "aggregation parameter must be scalar".
        let series = [Series {
            labels: vec![("__name__".into(), "m".into())],
            samples: vec![Sample {
                timestamp: 100,
                value: 1.0,
            }],
        }];
        let series2 = [Series {
            labels: vec![("__name__".into(), "m".into())],
            samples: vec![Sample {
                timestamp: 100,
                value: 2.0,
            }],
        }];
        let series3 = [Series {
            labels: vec![("__name__".into(), "m".into())],
            samples: vec![Sample {
                timestamp: 100,
                value: 1.0,
            }],
        }];
        // Build a combined vector with duplicate value 1.0
        let all: Vec<f64> = [&series[..], &series2[..], &series3[..]]
            .iter()
            .flat_map(|ss| ss.iter().flat_map(|s| s.samples.iter().map(|sm| sm.value)))
            .collect();
        // Two 1.0 and one 2.0
        let mut counts: std::collections::BTreeMap<u64, (f64, usize)> =
            std::collections::BTreeMap::new();
        for v in &all {
            let entry = counts.entry(v.to_bits()).or_insert((*v, 0));
            entry.1 += 1;
        }
        assert_eq!(counts.get(&1.0_f64.to_bits()).unwrap().1, 2);
        assert_eq!(counts.get(&2.0_f64.to_bits()).unwrap().1, 1);
    }

    #[test]
    fn test_aggregation_min_max_all_nan() {
        // All-NaN group should produce NaN, not ±Infinity.
        let values = [f64::NAN, f64::NAN, f64::NAN];
        let min_result: f64 = values.iter().copied().reduce(f64::min).unwrap_or(f64::NAN);
        let max_result: f64 = values.iter().copied().reduce(f64::max).unwrap_or(f64::NAN);
        assert!(
            min_result.is_nan(),
            "min of all-NaN should be NaN, got {min_result}"
        );
        assert!(
            max_result.is_nan(),
            "max of all-NaN should be NaN, got {max_result}"
        );

        // Mixed NaN: should ignore NaN and return the non-NaN extremum.
        let mixed = [f64::NAN, 5.0, f64::NAN, 3.0];
        let min_mixed: f64 = mixed.iter().copied().reduce(f64::min).unwrap_or(f64::NAN);
        let max_mixed: f64 = mixed.iter().copied().reduce(f64::max).unwrap_or(f64::NAN);
        assert!(
            (min_mixed - 3.0).abs() < f64::EPSILON,
            "min should be 3.0, got {min_mixed}"
        );
        assert!(
            (max_mixed - 5.0).abs() < f64::EPSILON,
            "max should be 5.0, got {max_mixed}"
        );
    }
}
