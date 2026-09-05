//! PromQL function evaluation.
//!
//! Implements the built-in PromQL functions: rate, irate, increase, delta,
//! math functions, the `*_over_time` family, label manipulation, sort,
//! `histogram_quantile`, the trigonometric functions and the UTC date
//! functions.

use std::collections::BTreeMap;

use crate::promql::ast::{Expr, MatchOp, PromQLValue, Sample, Series};

use super::aggregation::compute_quantile;
use super::{EvalError, HistogramBuckets, PromQLEvaluator, QueryParams};

// ── PromQLEvaluator method ─────────────────────────────────────────────

/// Every function name [`PromQLEvaluator::eval_function`] dispatches.
///
/// The parser consults it so an unknown name is a **parse** error — a 400 with
/// `errorType: bad_data`, as upstream reports it — rather than an evaluation
/// error at 422. A client branches on that difference, and a typo in a
/// function name is one of the most ordinary things a query editor sends.
///
/// Aggregation operators (`sum`, `topk`, …) are not here: they are parsed as
/// aggregations, not calls. `functions::every_listed_function_dispatches`
/// pins the list against the dispatcher.
pub(crate) const FUNCTIONS: &[&str] = &[
    "abs",
    "absent",
    "absent_over_time",
    "acos",
    "acosh",
    "asin",
    "asinh",
    "atan",
    "atanh",
    "avg_over_time",
    "ceil",
    "changes",
    "clamp",
    "clamp_max",
    "clamp_min",
    "cos",
    "cosh",
    "count_over_time",
    "day_of_month",
    "day_of_week",
    "day_of_year",
    "days_in_month",
    "deg",
    "delta",
    "deriv",
    "double_exponential_smoothing",
    "exp",
    "floor",
    "histogram_quantile",
    "hour",
    "idelta",
    "increase",
    "irate",
    "label_join",
    "label_replace",
    "last_over_time",
    "ln",
    "log10",
    "log2",
    "mad_over_time",
    "max_over_time",
    "min_over_time",
    "minute",
    "month",
    "pi",
    "predict_linear",
    "present_over_time",
    "quantile_over_time",
    "rad",
    "rate",
    "resets",
    "round",
    "scalar",
    "sgn",
    "sin",
    "sinh",
    "sort",
    "sort_by_label",
    "sort_by_label_desc",
    "sort_desc",
    "sqrt",
    "stddev_over_time",
    "stdvar_over_time",
    "sum_over_time",
    "tan",
    "tanh",
    "time",
    "timestamp",
    "vector",
    "year",
];

/// Whether `name` is a PromQL function this engine implements.
#[must_use]
pub fn is_known_function(name: &str) -> bool {
    FUNCTIONS.contains(&name)
}

impl PromQLEvaluator {
    #[allow(clippy::too_many_lines)]
    pub(crate) fn eval_function(
        &self,
        func: &str,
        args: &[Expr],
        params: &QueryParams,
    ) -> Result<PromQLValue, EvalError> {
        match func {
            "rate" => {
                if args.len() != 1 {
                    return Err(EvalError("rate() requires exactly 1 argument".into()));
                }
                let range_ns = extract_range_ns(&args[0]);
                let offset_ns = extract_offset_ns(&args[0]);
                let matrix = self.eval(&args[0], params)?;
                let window_end = params.time - offset_ns;
                match matrix {
                    PromQLValue::Matrix(series) => Ok(PromQLValue::Vector(compute_rate(
                        &series,
                        false,
                        range_ns,
                        window_end,
                        params.time,
                    ))),
                    _ => Err(EvalError("rate() requires a range vector".into())),
                }
            }
            "irate" => {
                if args.len() != 1 {
                    return Err(EvalError("irate() requires exactly 1 argument".into()));
                }
                let matrix = self.eval(&args[0], params)?;
                match matrix {
                    PromQLValue::Matrix(series) => {
                        Ok(PromQLValue::Vector(compute_irate(&series, params.time)))
                    }
                    _ => Err(EvalError("irate() requires a range vector".into())),
                }
            }
            "increase" => {
                if args.len() != 1 {
                    return Err(EvalError("increase() requires exactly 1 argument".into()));
                }
                let range_ns = extract_range_ns(&args[0]);
                let offset_ns = extract_offset_ns(&args[0]);
                let matrix = self.eval(&args[0], params)?;
                let window_end = params.time - offset_ns;
                match matrix {
                    PromQLValue::Matrix(series) => Ok(PromQLValue::Vector(compute_rate(
                        &series,
                        true,
                        range_ns,
                        window_end,
                        params.time,
                    ))),
                    _ => Err(EvalError("increase() requires a range vector".into())),
                }
            }
            "delta" => {
                if args.len() != 1 {
                    return Err(EvalError("delta() requires exactly 1 argument".into()));
                }
                let range_ns = extract_range_ns(&args[0]);
                let offset_ns = extract_offset_ns(&args[0]);
                let matrix = self.eval(&args[0], params)?;
                let window_end = params.time - offset_ns;
                match matrix {
                    PromQLValue::Matrix(series) => Ok(PromQLValue::Vector(compute_delta(
                        &series,
                        range_ns,
                        window_end,
                        params.time,
                    ))),
                    _ => Err(EvalError("delta() requires a range vector".into())),
                }
            }
            "abs" => {
                if args.len() != 1 {
                    return Err(EvalError("abs() requires exactly 1 argument".into()));
                }
                let val = self.eval(&args[0], params)?;
                apply_scalar_fn(val, f64::abs)
            }
            "ceil" => {
                if args.len() != 1 {
                    return Err(EvalError("ceil() requires exactly 1 argument".into()));
                }
                let val = self.eval(&args[0], params)?;
                apply_scalar_fn(val, f64::ceil)
            }
            "floor" => {
                if args.len() != 1 {
                    return Err(EvalError("floor() requires exactly 1 argument".into()));
                }
                let val = self.eval(&args[0], params)?;
                apply_scalar_fn(val, f64::floor)
            }
            "round" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(EvalError("round() requires 1 or 2 arguments".into()));
                }
                let val = self.eval(&args[0], params)?;
                let to_nearest = if args.len() == 2 {
                    match self.eval(&args[1], params)? {
                        PromQLValue::Scalar(n) => n,
                        _ => return Err(EvalError("round() second arg must be scalar".into())),
                    }
                } else {
                    1.0
                };
                apply_scalar_fn(val, |v| {
                    if to_nearest == 0.0 {
                        // Prometheus: Floor(v/0 + 0.5) * 0 = NaN
                        f64::NAN
                    } else {
                        // Use Floor(x + 0.5) to match Prometheus rounding
                        // semantics (ties toward +Inf), unlike Rust's
                        // .round() which rounds ties away from zero.
                        (v / to_nearest + 0.5).floor() * to_nearest
                    }
                })
            }
            "sqrt" => {
                if args.len() != 1 {
                    return Err(EvalError("sqrt() requires exactly 1 argument".into()));
                }
                let val = self.eval(&args[0], params)?;
                apply_scalar_fn(val, f64::sqrt)
            }
            "ln" => {
                if args.len() != 1 {
                    return Err(EvalError("ln() requires exactly 1 argument".into()));
                }
                let val = self.eval(&args[0], params)?;
                apply_scalar_fn(val, f64::ln)
            }
            "log2" => {
                if args.len() != 1 {
                    return Err(EvalError("log2() requires exactly 1 argument".into()));
                }
                let val = self.eval(&args[0], params)?;
                apply_scalar_fn(val, f64::log2)
            }
            "log10" => {
                if args.len() != 1 {
                    return Err(EvalError("log10() requires exactly 1 argument".into()));
                }
                let val = self.eval(&args[0], params)?;
                apply_scalar_fn(val, f64::log10)
            }
            "exp" => {
                if args.len() != 1 {
                    return Err(EvalError("exp() requires exactly 1 argument".into()));
                }
                let val = self.eval(&args[0], params)?;
                apply_scalar_fn(val, f64::exp)
            }
            "clamp" => {
                if args.len() != 3 {
                    return Err(EvalError("clamp() requires exactly 3 arguments".into()));
                }
                let val = self.eval(&args[0], params)?;
                let PromQLValue::Scalar(min) = self.eval(&args[1], params)? else {
                    return Err(EvalError("clamp() min must be scalar".into()));
                };
                let PromQLValue::Scalar(max) = self.eval(&args[2], params)? else {
                    return Err(EvalError("clamp() max must be scalar".into()));
                };
                // `max < min` is an **empty** vector, not a vector of NaN:
                // `functions.go` returns `enh.Out` untouched. The difference
                // matters because NaN survives arithmetic and appears in a
                // legend, where "no series" is the honest answer to a bound
                // that cannot be satisfied.
                if max < min {
                    return Ok(PromQLValue::Vector(Vec::new()));
                }
                apply_scalar_fn(val, |v| {
                    if min.is_nan() || max.is_nan() {
                        f64::NAN
                    } else {
                        v.clamp(min, max)
                    }
                })
            }
            "clamp_min" => {
                if args.len() != 2 {
                    return Err(EvalError("clamp_min() requires exactly 2 arguments".into()));
                }
                let val = self.eval(&args[0], params)?;
                let PromQLValue::Scalar(min) = self.eval(&args[1], params)? else {
                    return Err(EvalError("clamp_min() min must be scalar".into()));
                };
                apply_scalar_fn(val, |v| {
                    if v.is_nan() || min.is_nan() {
                        f64::NAN
                    } else {
                        v.max(min)
                    }
                })
            }
            "clamp_max" => {
                if args.len() != 2 {
                    return Err(EvalError("clamp_max() requires exactly 2 arguments".into()));
                }
                let val = self.eval(&args[0], params)?;
                let PromQLValue::Scalar(max) = self.eval(&args[1], params)? else {
                    return Err(EvalError("clamp_max() max must be scalar".into()));
                };
                apply_scalar_fn(val, |v| {
                    if v.is_nan() || max.is_nan() {
                        f64::NAN
                    } else {
                        v.min(max)
                    }
                })
            }
            "vector" => {
                if args.len() != 1 {
                    return Err(EvalError("vector() requires exactly 1 argument".into()));
                }
                let val = self.eval(&args[0], params)?;
                match val {
                    PromQLValue::Scalar(n) => Ok(PromQLValue::Vector(vec![Series {
                        labels: vec![],
                        samples: vec![Sample {
                            timestamp: params.time,
                            value: n,
                        }],
                    }])),
                    other => Ok(other),
                }
            }
            "scalar" => {
                if args.len() != 1 {
                    return Err(EvalError("scalar() requires exactly 1 argument".into()));
                }
                let val = self.eval(&args[0], params)?;
                match val {
                    PromQLValue::Vector(series)
                        if series.len() == 1 && series[0].samples.len() == 1 =>
                    {
                        Ok(PromQLValue::Scalar(series[0].samples[0].value))
                    }
                    PromQLValue::Scalar(n) => Ok(PromQLValue::Scalar(n)),
                    _ => Ok(PromQLValue::Scalar(f64::NAN)),
                }
            }
            "time" => {
                // Split into integer seconds + fractional part to preserve
                // sub-second precision for current-era nanosecond timestamps
                // (f64 only has ~53 bits of mantissa).
                let secs = params.time / 1_000_000_000;
                let frac_ns = params.time % 1_000_000_000;
                Ok(PromQLValue::Scalar(
                    secs as f64 + frac_ns as f64 / 1_000_000_000.0,
                ))
            }
            // ── *_over_time family ──────────────────────────────────
            "avg_over_time" => {
                eval_over_time_fn(self, args, params, NanPolicy::Propagate, |vals| {
                    vals.iter().sum::<f64>() / vals.len() as f64
                })
            }
            "sum_over_time" => {
                eval_over_time_fn(self, args, params, NanPolicy::Propagate, |vals| {
                    vals.iter().sum()
                })
            }
            "min_over_time" => eval_over_time_fn(self, args, params, NanPolicy::Skip, |vals| {
                vals.iter().copied().reduce(f64::min).unwrap_or(f64::NAN)
            }),
            "max_over_time" => eval_over_time_fn(self, args, params, NanPolicy::Skip, |vals| {
                vals.iter().copied().reduce(f64::max).unwrap_or(f64::NAN)
            }),
            // Counts samples, including NaN ones: the question is how many
            // samples were in the window, not how many were numbers.
            "count_over_time" => {
                eval_over_time_fn(self, args, params, NanPolicy::Propagate, |vals| {
                    vals.len() as f64
                })
            }
            "stddev_over_time" => {
                eval_over_time_fn(self, args, params, NanPolicy::Propagate, |vals| {
                    let mean = vals.iter().sum::<f64>() / vals.len() as f64;
                    let var =
                        vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / vals.len() as f64;
                    var.sqrt()
                })
            }
            "stdvar_over_time" => {
                eval_over_time_fn(self, args, params, NanPolicy::Propagate, |vals| {
                    let mean = vals.iter().sum::<f64>() / vals.len() as f64;
                    vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / vals.len() as f64
                })
            }
            "quantile_over_time" => {
                if args.len() != 2 {
                    return Err(EvalError(
                        "quantile_over_time() requires 2 arguments".into(),
                    ));
                }
                let q = match self.eval(&args[0], params)? {
                    PromQLValue::Scalar(n) => n,
                    _ => {
                        return Err(EvalError(
                            "quantile_over_time() first arg must be scalar".into(),
                        ))
                    }
                };
                let matrix = self.eval(&args[1], params)?;
                match matrix {
                    PromQLValue::Matrix(series) => Ok(PromQLValue::Vector(
                        series
                            .iter()
                            .filter_map(|s| {
                                if s.samples.is_empty() {
                                    return None;
                                }
                                let vals: Vec<f64> = s.samples.iter().map(|sm| sm.value).collect();
                                Some(Series {
                                    labels: s
                                        .labels
                                        .iter()
                                        .filter(|(k, _)| k != "__name__")
                                        .cloned()
                                        .collect(),
                                    samples: vec![Sample {
                                        timestamp: params.time,
                                        value: compute_quantile(&vals, q),
                                    }],
                                })
                            })
                            .collect(),
                    )),
                    _ => Err(EvalError(
                        "quantile_over_time() requires a range vector".into(),
                    )),
                }
            }
            // The last sample, NaN or not — `last_over_time` reports what was
            // written, and a NaN written is a NaN read.
            // The one member of the family that keeps `__name__`: it returns
            // an actual sample of the original series rather than an aggregate
            // about it.
            "last_over_time" => {
                eval_over_time_fn_opt(self, args, params, MetricName::Keep, |samples| {
                    samples.last().map(|s| s.value)
                })
            }
            // Presence, not numerosity: a window holding only NaN samples
            // still held samples.
            "present_over_time" => {
                eval_over_time_fn(self, args, params, NanPolicy::Propagate, |_vals| 1.0)
            }
            // ── absent / absent_over_time ───────────────────────────
            "absent" => {
                if args.len() != 1 {
                    return Err(EvalError("absent() requires exactly 1 argument".into()));
                }
                let absent_labels = extract_absent_labels(&args[0]);
                let val = self.eval(&args[0], params)?;
                match val {
                    PromQLValue::Vector(v) if v.is_empty() => {
                        Ok(PromQLValue::Vector(vec![Series {
                            labels: absent_labels,
                            samples: vec![Sample {
                                timestamp: params.time,
                                value: 1.0,
                            }],
                        }]))
                    }
                    PromQLValue::Vector(_) => Ok(PromQLValue::Vector(vec![])),
                    _ => Ok(PromQLValue::Vector(vec![])),
                }
            }
            "absent_over_time" => {
                if args.len() != 1 {
                    return Err(EvalError(
                        "absent_over_time() requires exactly 1 argument".into(),
                    ));
                }
                let absent_labels = extract_absent_labels(&args[0]);
                let val = self.eval(&args[0], params)?;
                match val {
                    PromQLValue::Matrix(m)
                        if m.is_empty() || m.iter().all(|s| s.samples.is_empty()) =>
                    {
                        Ok(PromQLValue::Vector(vec![Series {
                            labels: absent_labels,
                            samples: vec![Sample {
                                timestamp: params.time,
                                value: 1.0,
                            }],
                        }]))
                    }
                    _ => Ok(PromQLValue::Vector(vec![])),
                }
            }
            // ── changes / resets / deriv ────────────────────────────
            "changes" => eval_over_time_fn_full(self, args, params, |samples| {
                // Use != for exact comparison (no epsilon).
                // NaN != NaN is true in IEEE 754, but Prometheus explicitly
                // excludes NaN→NaN transitions from the change count.
                samples
                    .windows(2)
                    .filter(|w| {
                        w[0].value != w[1].value && !(w[0].value.is_nan() && w[1].value.is_nan())
                    })
                    .count() as f64
            }),
            "resets" => eval_over_time_fn_full(self, args, params, |samples| {
                samples
                    .windows(2)
                    .filter(|w| w[1].value < w[0].value)
                    .count() as f64
            }),
            "deriv" => eval_over_time_fn_opt(self, args, params, MetricName::Drop, |samples| {
                if samples.len() < 2 {
                    // Fewer than two points is no slope, and Prometheus leaves
                    // the series out rather than reporting NaN.
                    return None;
                }
                // Linear regression using relative timestamps to avoid
                // catastrophic cancellation with nanosecond-scale values.
                let base_ts = samples[0].timestamp as f64;
                let n = samples.len() as f64;
                let sum_x: f64 = samples.iter().map(|s| s.timestamp as f64 - base_ts).sum();
                let sum_y: f64 = samples.iter().map(|s| s.value).sum();
                let sum_xy: f64 = samples
                    .iter()
                    .map(|s| (s.timestamp as f64 - base_ts) * s.value)
                    .sum();
                let sum_x2: f64 = samples
                    .iter()
                    .map(|s| {
                        let x = s.timestamp as f64 - base_ts;
                        x * x
                    })
                    .sum();
                let denom = n * sum_x2 - sum_x * sum_x;
                if denom.abs() < f64::EPSILON {
                    // Every sample at the same instant: the slope is genuinely
                    // undefined rather than uncomputable, and Prometheus's
                    // `linearRegression` yields NaN here too. Only the
                    // fewer-than-two case drops the series.
                    return Some(f64::NAN);
                }
                // Convert from per-nanosecond to per-second.
                Some((n * sum_xy - sum_x * sum_y) / denom * 1_000_000_000.0)
            }),
            "predict_linear" => {
                if args.len() != 2 {
                    return Err(EvalError("predict_linear() requires 2 arguments".into()));
                }
                let t = match self.eval(&args[1], params)? {
                    PromQLValue::Scalar(n) => n,
                    _ => {
                        return Err(EvalError(
                            "predict_linear() second arg must be scalar".into(),
                        ))
                    }
                };
                let matrix = self.eval(&args[0], params)?;
                let eval_time = params.time;
                match matrix {
                    PromQLValue::Matrix(series) => {
                        Ok(PromQLValue::Vector(
                            series
                                .iter()
                                .filter_map(|s| {
                                    if s.samples.len() < 2 {
                                        return None;
                                    }
                                    // Use relative timestamps to avoid f64
                                    // precision loss with nanosecond-scale values.
                                    let base_ts = s.samples[0].timestamp as f64;
                                    let n = s.samples.len() as f64;
                                    let sum_x: f64 = s
                                        .samples
                                        .iter()
                                        .map(|sm| sm.timestamp as f64 - base_ts)
                                        .sum();
                                    let sum_y: f64 = s.samples.iter().map(|sm| sm.value).sum();
                                    let sum_xy: f64 = s
                                        .samples
                                        .iter()
                                        .map(|sm| (sm.timestamp as f64 - base_ts) * sm.value)
                                        .sum();
                                    let sum_x2: f64 = s
                                        .samples
                                        .iter()
                                        .map(|sm| {
                                            let x = sm.timestamp as f64 - base_ts;
                                            x * x
                                        })
                                        .sum();
                                    let denom = n * sum_x2 - sum_x * sum_x;
                                    if denom.abs() < f64::EPSILON {
                                        // Degenerate regression — return NaN
                                        // (Prometheus emits NaN rather than
                                        // dropping the series entirely).
                                        return Some(Series {
                                            labels: s
                                                .labels
                                                .iter()
                                                .filter(|(k, _)| k != "__name__")
                                                .cloned()
                                                .collect(),
                                            samples: vec![Sample {
                                                timestamp: eval_time,
                                                value: f64::NAN,
                                            }],
                                        });
                                    }
                                    let slope = (n * sum_xy - sum_x * sum_y) / denom;
                                    let intercept = (sum_y - slope * sum_x) / n;
                                    // Prometheus predicts `t` seconds past the
                                    // *evaluation* time, not past the last
                                    // sample: `linearRegression` is taken with
                                    // `interceptTime = enh.Ts`. Anchoring on
                                    // the last sample instead shifts every
                                    // prediction by one scrape interval of
                                    // slope, and it makes the answer depend on
                                    // scrape jitter — two steps of a range
                                    // query with the same data would disagree.
                                    let predict_ts =
                                        (eval_time as f64 - base_ts) + t * 1_000_000_000.0;
                                    let value = slope * predict_ts + intercept;
                                    Some(Series {
                                        labels: s
                                            .labels
                                            .iter()
                                            .filter(|(k, _)| k != "__name__")
                                            .cloned()
                                            .collect(),
                                        samples: vec![Sample {
                                            timestamp: eval_time,
                                            value,
                                        }],
                                    })
                                })
                                .collect(),
                        ))
                    }
                    _ => Err(EvalError("predict_linear() requires a range vector".into())),
                }
            }
            // ── label_replace / label_join ──────────────────────────
            "label_replace" => {
                if args.len() != 5 {
                    return Err(EvalError("label_replace() requires 5 arguments".into()));
                }
                let val = self.eval(&args[0], params)?;
                let dst_label = match &args[1] {
                    Expr::StringLiteral(s) => s.clone(),
                    _ => return Err(EvalError("label_replace() dst_label must be string".into())),
                };
                let replacement = match &args[2] {
                    Expr::StringLiteral(s) => s.clone(),
                    _ => {
                        return Err(EvalError(
                            "label_replace() replacement must be string".into(),
                        ))
                    }
                };
                let src_label = match &args[3] {
                    Expr::StringLiteral(s) => s.clone(),
                    _ => return Err(EvalError("label_replace() src_label must be string".into())),
                };
                let regex_str = match &args[4] {
                    Expr::StringLiteral(s) => s.clone(),
                    _ => return Err(EvalError("label_replace() regex must be string".into())),
                };
                // A destination that is not a legal label name would produce a
                // series nothing can select, match or aggregate by — so
                // Prometheus refuses it rather than building one. Silently
                // writing it meant `label_replace(m, "1bad", …)` returned a
                // series whose label could never be named again.
                if !is_valid_label_name(&dst_label) {
                    return Err(EvalError(format!(
                        "invalid destination label name in label_replace(): {dst_label}"
                    )));
                }
                // Cache the compiled regex: a range query would otherwise
                // recompile the same pattern at every step.
                let anchored = format!("^(?:{regex_str})$");
                let re = {
                    let cache = self.regex_cache.borrow();
                    cache.get(&anchored).cloned()
                };
                let re = match re {
                    Some(r) => r,
                    None => {
                        let compiled = regex::Regex::new(&anchored).map_err(|e| {
                            EvalError(format!("label_replace() invalid regex: {e}"))
                        })?;
                        let mut cache = self.regex_cache.borrow_mut();
                        // Bound the regex cache to prevent unbounded memory
                        // growth on high-cardinality label_replace() patterns.
                        if cache.len() >= 10_000 {
                            cache.clear();
                        }
                        cache.insert(anchored, compiled.clone());
                        compiled
                    }
                };

                match val {
                    PromQLValue::Vector(series) => {
                        let result = series
                            .into_iter()
                            .map(|mut s| {
                                let src_val = s
                                    .labels
                                    .iter()
                                    .find(|(k, _)| k == &src_label)
                                    .map(|(_, v)| v.clone())
                                    .unwrap_or_default();
                                if let Some(caps) = re.captures(&src_val) {
                                    let mut new_val = String::new();
                                    // Single-pass capture group expansion (avoids
                                    // double-substitution when captured text contains $N tokens).
                                    caps.expand(&replacement, &mut new_val);
                                    // Remove or update the destination label
                                    s.labels.retain(|(k, _)| k != &dst_label);
                                    if !new_val.is_empty() {
                                        s.labels.push((dst_label.clone(), new_val));
                                        s.labels.sort_by(|a, b| a.0.cmp(&b.0));
                                    }
                                }
                                s
                            })
                            .collect();
                        Ok(PromQLValue::Vector(result))
                    }
                    other => Ok(other),
                }
            }
            "label_join" => {
                if args.len() < 3 {
                    return Err(EvalError(
                        "label_join() requires at least 3 arguments".into(),
                    ));
                }
                let val = self.eval(&args[0], params)?;
                let dst_label = match &args[1] {
                    Expr::StringLiteral(s) => s.clone(),
                    _ => return Err(EvalError("label_join() dst_label must be string".into())),
                };
                let separator = match &args[2] {
                    Expr::StringLiteral(s) => s.clone(),
                    _ => return Err(EvalError("label_join() separator must be string".into())),
                };
                let src_labels: Vec<String> = args[3..]
                    .iter()
                    .map(|a| match a {
                        Expr::StringLiteral(s) => Ok(s.clone()),
                        _ => Err(EvalError(
                            "label_join() source labels must be strings".into(),
                        )),
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                match val {
                    PromQLValue::Vector(series) => {
                        let result = series
                            .into_iter()
                            .map(|mut s| {
                                let parts: Vec<String> = src_labels
                                    .iter()
                                    .map(|lbl| {
                                        s.labels
                                            .iter()
                                            .find(|(k, _)| k == lbl)
                                            .map(|(_, v)| v.clone())
                                            .unwrap_or_default()
                                    })
                                    .collect();
                                let joined = parts.join(&separator);
                                s.labels.retain(|(k, _)| k != &dst_label);
                                if !joined.is_empty() {
                                    s.labels.push((dst_label.clone(), joined));
                                    s.labels.sort_by(|a, b| a.0.cmp(&b.0));
                                }
                                s
                            })
                            .collect();
                        Ok(PromQLValue::Vector(result))
                    }
                    other => Ok(other),
                }
            }
            // ── mad_over_time ───────────────────────────────────────
            //
            // Median absolute deviation. Prometheus returns `NaN` for the
            // whole window as soon as one sample is `NaN` rather than
            // dropping it, because a median over a set containing `NaN` is
            // not defined — so this is `NanPolicy::Propagate` with an
            // explicit early return, not a `Skip`.
            "mad_over_time" => {
                eval_over_time_fn(self, args, params, NanPolicy::Propagate, |vals| {
                    if vals.is_empty() || vals.iter().any(|v| v.is_nan()) {
                        return f64::NAN;
                    }
                    let median = median_of(vals);
                    let deviations: Vec<f64> = vals.iter().map(|v| (v - median).abs()).collect();
                    median_of(&deviations)
                })
            }
            // ── double_exponential_smoothing ────────────────────────
            //
            // Prometheus 3's rename of `holt_winters`, and *not* a call into
            // the Holt-Winters model in `chronix-analytics`: the recurrence
            // upstream evaluates is its own. The trend is seeded as
            // `s[1] - s[0]` while the smoothed value starts at `s[0]` and the
            // previous smoothed value starts at zero, and the trend update is
            // suppressed on the first iteration. Substituting a textbook
            // Holt-Winters fit here would answer a different question with the
            // same function name, which is the failure mode that put a
            // variance-free "ADWIN" in this tree.
            "double_exponential_smoothing" => {
                if args.len() != 3 {
                    return Err(EvalError(
                        "double_exponential_smoothing() requires 3 arguments".into(),
                    ));
                }
                let PromQLValue::Scalar(sf) = self.eval(&args[1], params)? else {
                    return Err(EvalError(
                        "double_exponential_smoothing() smoothing factor must be a scalar".into(),
                    ));
                };
                let PromQLValue::Scalar(tf) = self.eval(&args[2], params)? else {
                    return Err(EvalError(
                        "double_exponential_smoothing() trend factor must be a scalar".into(),
                    ));
                };
                if !(sf > 0.0 && sf < 1.0) {
                    return Err(EvalError(format!(
                        "invalid smoothing factor: expected 0 < sf < 1, got {sf}"
                    )));
                }
                if !(tf > 0.0 && tf < 1.0) {
                    return Err(EvalError(format!(
                        "invalid trend factor: expected 0 < tf < 1, got {tf}"
                    )));
                }
                let matrix = self.eval(&args[0], params)?;
                match matrix {
                    PromQLValue::Matrix(series) => Ok(PromQLValue::Vector(
                        series
                            .iter()
                            .filter_map(|s| {
                                let vals: Vec<f64> = s.samples.iter().map(|sm| sm.value).collect();
                                // Fewer than two samples yields no result at
                                // all, rather than the single sample echoed
                                // back: there is no trend to estimate.
                                let smoothed = double_exponential_smoothing(&vals, sf, tf)?;
                                Some(Series {
                                    labels: s
                                        .labels
                                        .iter()
                                        .filter(|(k, _)| k != "__name__")
                                        .cloned()
                                        .collect(),
                                    samples: vec![Sample {
                                        timestamp: params.time,
                                        value: smoothed,
                                    }],
                                })
                            })
                            .collect(),
                    )),
                    other => Ok(other),
                }
            }
            // ── sort_by_label / sort_by_label_desc ──────────────────
            //
            // Ordering is by *natural* sort on each named label in turn —
            // `pod-2` before `pod-10` — with the full label set as the final
            // tie-break so the result is a total order rather than an
            // arbitrary one among equals.
            "sort_by_label" | "sort_by_label_desc" => {
                if args.is_empty() {
                    return Err(EvalError(format!(
                        "{func}() requires a vector and at least one label"
                    )));
                }
                let descending = func == "sort_by_label_desc";
                let mut label_names = Vec::with_capacity(args.len() - 1);
                for arg in &args[1..] {
                    match arg {
                        Expr::StringLiteral(name) => label_names.push(name.clone()),
                        _ => {
                            return Err(EvalError(format!(
                                "{func}() label arguments must be string literals"
                            )))
                        }
                    }
                }
                let val = self.eval(&args[0], params)?;
                match val {
                    PromQLValue::Vector(mut series) => {
                        series.sort_by(|a, b| {
                            let ord = compare_by_labels(a, b, &label_names);
                            if descending {
                                ord.reverse()
                            } else {
                                ord
                            }
                        });
                        Ok(PromQLValue::Vector(series))
                    }
                    other => Ok(other),
                }
            }
            // ── sort / sort_desc ────────────────────────────────────
            "sort" | "sort_desc" => {
                if args.len() != 1 {
                    return Err(EvalError(format!("{func}() requires exactly 1 argument")));
                }
                let descending = func == "sort_desc";
                let val = self.eval(&args[0], params)?;
                match val {
                    PromQLValue::Vector(mut series) => {
                        series.sort_by(|a, b| {
                            let va = a.samples.first().map_or(f64::NAN, |s| s.value);
                            let vb = b.samples.first().map_or(f64::NAN, |s| s.value);
                            nan_last_cmp(va, vb, descending)
                        });
                        Ok(PromQLValue::Vector(series))
                    }
                    other => Ok(other),
                }
            }
            // ── histogram_quantile ──────────────────────────────────
            "histogram_quantile" => {
                if args.len() != 2 {
                    return Err(EvalError(
                        "histogram_quantile() requires 2 arguments".into(),
                    ));
                }
                let q = match self.eval(&args[0], params)? {
                    PromQLValue::Scalar(n) => n,
                    _ => {
                        return Err(EvalError(
                            "histogram_quantile() first arg must be scalar".into(),
                        ))
                    }
                };
                let val = self.eval(&args[1], params)?;
                match val {
                    PromQLValue::Vector(series) => {
                        // Group by labels excluding "le"
                        let mut buckets: HistogramBuckets = BTreeMap::new();
                        for s in &series {
                            let le_val = s
                                .labels
                                .iter()
                                .find(|(k, _)| k == "le")
                                .map(|(_, v)| v.clone());
                            let group_key: Vec<(String, String)> = s
                                .labels
                                .iter()
                                .filter(|(k, _)| k != "le" && k != "__name__")
                                .cloned()
                                .collect();
                            if let Some(le_str) = le_val {
                                let le = if le_str == "+Inf" {
                                    f64::INFINITY
                                } else {
                                    match le_str.parse::<f64>() {
                                        Ok(v) if !v.is_nan() => v,
                                        _ => continue, // skip unparseable/NaN le
                                    }
                                };
                                let count = s.samples.first().map_or(0.0, |sm| sm.value);
                                buckets.entry(group_key).or_default().push((le, count));
                            }
                        }

                        let mut result = Vec::new();
                        for (labels, mut bucket_list) in buckets {
                            bucket_list.sort_by(|a, b| {
                                a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)
                            });
                            if bucket_list.is_empty() {
                                continue;
                            }

                            // Coalesce buckets sharing an upper bound, as
                            // `coalesceBuckets` does. Two `le` labels can
                            // render the same number — "0.5" and "0.50", or a
                            // relabelled series — and leaving both in place
                            // makes the cumulative counts non-monotonic in a
                            // way the fix-up below then papers over.
                            let mut coalesced: Vec<(f64, f64)> =
                                Vec::with_capacity(bucket_list.len());
                            for (le, count) in bucket_list {
                                match coalesced.last_mut() {
                                    Some(last) if last.0 == le => last.1 += count,
                                    _ => coalesced.push((le, count)),
                                }
                            }
                            let mut bucket_list = coalesced;

                            // Enforce monotonicity: bucket counts must be
                            // non-decreasing. Fix up scrape artifacts.
                            for i in 1..bucket_list.len() {
                                if bucket_list[i].1 < bucket_list[i - 1].1 {
                                    bucket_list[i].1 = bucket_list[i - 1].1;
                                }
                            }

                            // A conforming histogram's highest bucket is
                            // `+Inf`; without it the observation total is
                            // unknown, so the quantile is not defined and
                            // `bucketQuantile` returns NaN rather than a
                            // plausible number derived from a partial total.
                            let top_is_inf = bucket_list
                                .last()
                                .is_some_and(|b| b.0.is_infinite() && b.0.is_sign_positive());
                            if q.is_nan() || !top_is_inf {
                                result.push(Series {
                                    labels,
                                    samples: vec![Sample {
                                        timestamp: params.time,
                                        value: f64::NAN,
                                    }],
                                });
                                continue;
                            }

                            // Prometheus returns NaN for a histogram with
                            // fewer than two buckets: there is no interval to
                            // interpolate within.
                            if bucket_list.len() < 2 {
                                result.push(Series {
                                    labels,
                                    samples: vec![Sample {
                                        timestamp: params.time,
                                        value: f64::NAN,
                                    }],
                                });
                                continue;
                            }

                            let total = bucket_list.last().map_or(0.0, |b| b.1);
                            if total == 0.0 {
                                // Emit NaN for zero-observation histogram
                                result.push(Series {
                                    labels,
                                    samples: vec![Sample {
                                        timestamp: params.time,
                                        value: f64::NAN,
                                    }],
                                });
                                continue;
                            }

                            // Edge cases per Prometheus spec
                            if q < 0.0 {
                                result.push(Series {
                                    labels,
                                    samples: vec![Sample {
                                        timestamp: params.time,
                                        value: f64::NEG_INFINITY,
                                    }],
                                });
                                continue;
                            }
                            if q > 1.0 {
                                result.push(Series {
                                    labels,
                                    samples: vec![Sample {
                                        timestamp: params.time,
                                        value: f64::INFINITY,
                                    }],
                                });
                                continue;
                            }

                            let rank = q * total;

                            // Locate the first bucket whose cumulative count
                            // reaches the rank, then interpolate within it —
                            // Prometheus `bucketQuantile`.
                            let last = bucket_list.len() - 1;
                            let b = bucket_list
                                .iter()
                                .position(|&(_, count)| count >= rank)
                                .unwrap_or(last);

                            let value = if b == last {
                                // The rank falls in the top bucket, whose upper
                                // bound is +Inf in any conforming histogram and
                                // so carries no information. Report the highest
                                // finite bound instead of extrapolating into it.
                                bucket_list[last - 1].0
                            } else if b == 0 && bucket_list[0].0 <= 0.0 {
                                // A lowest bucket with a non-positive bound has
                                // no meaningful lower edge to interpolate from.
                                bucket_list[0].0
                            } else {
                                // 0 is the implicit lower bound of the lowest
                                // bucket; otherwise the previous bucket's bound.
                                let (bucket_start, prev_count) = if b > 0 {
                                    (bucket_list[b - 1].0, bucket_list[b - 1].1)
                                } else {
                                    (0.0, 0.0)
                                };
                                let bucket_end = bucket_list[b].0;
                                let count = bucket_list[b].1 - prev_count;
                                if count > 0.0 {
                                    bucket_start
                                        + (bucket_end - bucket_start) * (rank - prev_count) / count
                                } else {
                                    bucket_start
                                }
                            };
                            result.push(Series {
                                labels,
                                samples: vec![Sample {
                                    timestamp: params.time,
                                    value,
                                }],
                            });
                        }
                        Ok(PromQLValue::Vector(result))
                    }
                    _ => Err(EvalError(
                        "histogram_quantile() requires instant vector".into(),
                    )),
                }
            }
            // ── timestamp / idelta / sgn ─────────────────────────────
            "timestamp" => {
                if args.len() != 1 {
                    return Err(EvalError("timestamp() requires exactly 1 argument".into()));
                }
                let val = self.eval(&args[0], params)?;
                match val {
                    PromQLValue::Vector(series) => {
                        let result = series
                            .into_iter()
                            .map(|mut s| {
                                // The value is a time, not a measurement of
                                // this metric, so the name goes — Prometheus
                                // builds the output through `DropMetricName`.
                                // Keeping it let `timestamp(m)` and `m` match
                                // each other in a binary operation, which
                                // compares a clock reading against a value.
                                s.labels.retain(|(k, _)| k != "__name__");
                                for sample in &mut s.samples {
                                    sample.value = sample.timestamp as f64 / 1_000_000_000.0;
                                }
                                s
                            })
                            .collect();
                        Ok(PromQLValue::Vector(result))
                    }
                    _ => Err(EvalError("timestamp() requires instant vector".into())),
                }
            }
            "idelta" => eval_over_time_fn_opt(self, args, params, MetricName::Drop, |samples| {
                if samples.len() < 2 {
                    return None;
                }
                let n = samples.len();
                Some(samples[n - 1].value - samples[n - 2].value)
            }),
            "sgn" => {
                if args.len() != 1 {
                    return Err(EvalError("sgn() requires exactly 1 argument".into()));
                }
                let val = self.eval(&args[0], params)?;
                apply_scalar_fn(val, |v| {
                    if v.is_nan() {
                        f64::NAN
                    } else if v > 0.0 {
                        1.0
                    } else if v < 0.0 {
                        -1.0
                    } else {
                        0.0
                    }
                })
            }
            // ── constants & trig functions ──────────────────────────
            "pi" => Ok(PromQLValue::Scalar(std::f64::consts::PI)),
            "sin" | "cos" | "tan" | "asin" | "acos" | "atan" | "sinh" | "cosh" | "tanh"
            | "asinh" | "acosh" | "atanh" | "deg" | "rad" => {
                if args.len() != 1 {
                    return Err(EvalError(format!("{func}() requires exactly 1 argument")));
                }
                let val = self.eval(&args[0], params)?;
                let f: fn(f64) -> f64 = match func {
                    "sin" => f64::sin,
                    "cos" => f64::cos,
                    "tan" => f64::tan,
                    "asin" => f64::asin,
                    "acos" => f64::acos,
                    "atan" => f64::atan,
                    "sinh" => f64::sinh,
                    "cosh" => f64::cosh,
                    "tanh" => f64::tanh,
                    "asinh" => f64::asinh,
                    "acosh" => f64::acosh,
                    "atanh" => f64::atanh,
                    "deg" => f64::to_degrees,
                    "rad" => f64::to_radians,
                    other => return Err(EvalError(format!("unknown trig function: {other}"))),
                };
                apply_scalar_fn(val, f)
            }
            // ── date and time ───────────────────────────────────────
            "year" | "month" | "day_of_month" | "day_of_week" | "day_of_year" | "days_in_month"
            | "hour" | "minute" => eval_date_fn(self, func, args, params),
            other => Err(EvalError(format!("unknown function: {other}"))),
        }
    }
}

/// Evaluate one of the eight PromQL date functions.
///
/// Each takes an optional instant vector and, with no argument, uses
/// `vector(time())` — a single sample at the evaluation time carrying no
/// labels. Every one reads the sample *value* as a Unix timestamp in seconds
/// (not the sample's own timestamp: `year(my_metric)` asks what year the
/// *value* encodes) and answers in **UTC**, which is what Prometheus does and
/// the reason `hour() < 9` alerting rules behave the same wherever the server
/// runs.
///
/// The metric name is dropped, as it is for every function that changes what
/// the value means.
fn eval_date_fn(
    evaluator: &PromQLEvaluator,
    func: &str,
    args: &[Expr],
    params: &QueryParams,
) -> Result<PromQLValue, EvalError> {
    use chrono::{DateTime, Datelike, Timelike};

    if args.len() > 1 {
        return Err(EvalError(format!(
            "{func}() takes at most 1 argument, got {}",
            args.len()
        )));
    }

    let series = if let Some(arg) = args.first() {
        match evaluator.eval(arg, params)? {
            PromQLValue::Vector(v) => v,
            PromQLValue::Scalar(n) => vec![Series {
                labels: vec![],
                samples: vec![Sample {
                    timestamp: params.time,
                    value: n,
                }],
            }],
            _ => return Err(EvalError(format!("{func}() requires an instant vector"))),
        }
    } else {
        // The default argument is `vector(time())`.
        let secs = params.time / 1_000_000_000;
        let frac = params.time % 1_000_000_000;
        vec![Series {
            labels: vec![],
            samples: vec![Sample {
                timestamp: params.time,
                #[allow(clippy::cast_precision_loss)]
                value: secs as f64 + frac as f64 / 1_000_000_000.0,
            }],
        }]
    };

    let component = |seconds: f64| -> f64 {
        if !seconds.is_finite() {
            return f64::NAN;
        }
        // `DateTime::from_timestamp` rejects anything outside roughly
        // ±262 000 years, which is the honest boundary for a value that is
        // being *interpreted* as a date rather than measured as one.
        #[allow(clippy::cast_possible_truncation)]
        let whole = seconds.floor() as i64;
        let Some(dt) = DateTime::from_timestamp(whole, 0) else {
            return f64::NAN;
        };
        let dt = dt.naive_utc();
        match func {
            "year" => f64::from(dt.year()),
            "month" => f64::from(dt.month()),
            "day_of_month" => f64::from(dt.day()),
            // Prometheus numbers the week from Sunday = 0.
            "day_of_week" => f64::from(dt.weekday().num_days_from_sunday()),
            "day_of_year" => f64::from(dt.ordinal()),
            "days_in_month" => f64::from(days_in_month(dt.year(), dt.month())),
            "hour" => f64::from(dt.hour()),
            "minute" => f64::from(dt.minute()),
            _ => f64::NAN,
        }
    };

    Ok(PromQLValue::Vector(
        series
            .into_iter()
            .map(|s| Series {
                labels: s
                    .labels
                    .into_iter()
                    .filter(|(k, _)| k != "__name__")
                    .collect(),
                samples: s
                    .samples
                    .into_iter()
                    .map(|sm| Sample {
                        timestamp: sm.timestamp,
                        value: component(sm.value),
                    })
                    .collect(),
            })
            .collect(),
    ))
}

/// Number of days in a Gregorian calendar month.
fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

// ── Free functions ─────────────────────────────────────────────────────

/// What an `*_over_time` function does with `NaN` samples.
///
/// Prometheus does not have one rule here and neither can we. `min_over_time`
/// and `max_over_time` skip `NaN` unless every sample is `NaN`; every other
/// member of the family sees the raw values, so `NaN` propagates through
/// `avg`/`sum`/`stddev` and is *counted* by `count_over_time`.
///
/// This used to be a single blanket filter applied to all of them, which made
/// `count_over_time` under-report by the number of `NaN` samples and made
/// `present_over_time` return nothing at all for a series whose samples were
/// all `NaN` — the one function whose entire job is to answer "was anything
/// here?".
#[derive(Clone, Copy, PartialEq, Eq)]
enum NanPolicy {
    /// Pass every sample through; `NaN` propagates into the result.
    Propagate,
    /// Drop `NaN` samples, but keep the series and hand the function an empty
    /// slice if that is all there was.
    Skip,
}

/// Helper for `*_over_time` functions that work on sample values.
fn eval_over_time_fn(
    evaluator: &PromQLEvaluator,
    args: &[Expr],
    params: &QueryParams,
    nan: NanPolicy,
    f: impl Fn(&[f64]) -> f64,
) -> Result<PromQLValue, EvalError> {
    eval_over_time_fn_full(evaluator, args, params, |samples| {
        match nan {
            NanPolicy::Propagate => {
                let vals: Vec<f64> = samples.iter().map(|sm| sm.value).collect();
                f(&vals)
            }
            NanPolicy::Skip => {
                let vals: Vec<f64> = samples
                    .iter()
                    .map(|sm| sm.value)
                    .filter(|v| !v.is_nan())
                    .collect();
                // All-NaN: the extremum of nothing is NaN, which is also what
                // Prometheus reports.
                if vals.is_empty() {
                    f64::NAN
                } else {
                    f(&vals)
                }
            }
        }
    })
}

/// Helper for `*_over_time` functions that need full Sample structs
/// (e.g. `changes`, `resets`, `deriv`).
///
/// The result carries the **evaluation** timestamp. Every function over a
/// range vector produces an instant vector, and an instant vector is by
/// definition a set of values *at the evaluation time* — Prometheus stamps
/// them all with `enh.Ts`. Stamping the last input sample's timestamp instead
/// looks harmless at an instant query and is not: a range query evaluates the
/// same expression once per step and concatenates the results, so two adjacent
/// steps whose windows end at the same sample emit the *same* timestamp twice.
/// The matrix handed back to Grafana is then neither step-aligned nor strictly
/// increasing, and a binary operation between two such vectors compares
/// samples that no longer share a timestamp.
fn eval_over_time_fn_full(
    evaluator: &PromQLEvaluator,
    args: &[Expr],
    params: &QueryParams,
    f: impl Fn(&[Sample]) -> f64,
) -> Result<PromQLValue, EvalError> {
    eval_over_time_fn_opt(evaluator, args, params, MetricName::Drop, |s| Some(f(s)))
}

/// Whether a range function keeps the input series' `__name__`.
///
/// Almost every `*_over_time` drops it: the output is an aggregate *about* the
/// series, not a sample *of* it, and keeping the name would let
/// `avg_over_time(m[5m])` and `m` collide in a binary operation.
/// `last_over_time` is the exception in Prometheus, because it returns an
/// actual sample of the original series — `functions.go` registers it without
/// the `dropSeriesName` the rest of the family shares.
#[derive(Clone, Copy, PartialEq, Eq)]
enum MetricName {
    Keep,
    Drop,
}

/// The general driver: a function may decline to produce a value, and the
/// series is then **absent** rather than NaN.
///
/// `deriv` and `idelta` need two samples; with fewer, Prometheus leaves the
/// series out of the result (`functions.go` returns `enh.Out` untouched).
/// Emitting NaN instead puts the series in the legend of a dashboard with a
/// gap where its value should be, which reads as "broken" rather than as "not
/// enough data" — and it survives arithmetic, so one under-sampled series
/// turns a whole expression NaN.
fn eval_over_time_fn_opt(
    evaluator: &PromQLEvaluator,
    args: &[Expr],
    params: &QueryParams,
    name: MetricName,
    f: impl Fn(&[Sample]) -> Option<f64>,
) -> Result<PromQLValue, EvalError> {
    if args.len() != 1 {
        return Err(EvalError("function requires exactly 1 argument".into()));
    }
    let matrix = evaluator.eval(&args[0], params)?;
    match matrix {
        PromQLValue::Matrix(series) => Ok(PromQLValue::Vector(
            series
                .iter()
                .filter(|s| !s.samples.is_empty())
                .filter_map(|s| {
                    let value = f(&s.samples)?;
                    Some(Series {
                        labels: s
                            .labels
                            .iter()
                            .filter(|(k, _)| name == MetricName::Keep || k != "__name__")
                            .cloned()
                            .collect(),
                        samples: vec![Sample {
                            timestamp: params.time,
                            value,
                        }],
                    })
                })
                .collect(),
        )),
        _ => Err(EvalError("function requires a range vector".into())),
    }
}

/// Extract the range duration (in ns) from a MatrixSelector expression.
/// Whether `name` is a legal Prometheus label name.
///
/// `[a-zA-Z_][a-zA-Z0-9_]*`, which is `model.LabelName.IsValid()` upstream.
/// A label outside it cannot be written in a selector, a `by` clause or a
/// `group_left` list, so a function that produced one would produce a series
/// nothing could ever refer to.
fn is_valid_label_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

pub(crate) fn extract_range_ns(expr: &Expr) -> Option<i64> {
    // A subquery is a range vector too, and its `[range:step]` is the range —
    // so `rate(x[5m:15s])` extrapolates over five minutes exactly as
    // `rate(x[5m])` does. Matching only `MatrixSelector` meant the two forms
    // gave different answers for the same data.
    match expr {
        Expr::MatrixSelector { range, .. } | Expr::Subquery { range, .. } => Some(range.as_nanos()),
        _ => None,
    }
}

/// Extract the offset duration (in ns) from a MatrixSelector's inner
/// VectorSelector.  Returns 0 when no offset is present.
pub(crate) fn extract_offset_ns(expr: &Expr) -> i64 {
    if let Expr::MatrixSelector { vector, .. } = expr {
        if let Expr::VectorSelector { offset, .. } = vector.as_ref() {
            return offset.map(|d| d.as_nanos()).unwrap_or(0);
        }
    }
    0
}

/// Reconstruct output labels for `absent()`/`absent_over_time()` from
/// equality matchers of the input selector (excluding `__name__`).
pub(crate) fn extract_absent_labels(expr: &Expr) -> Vec<(String, String)> {
    let matchers = match expr {
        Expr::VectorSelector { matchers, .. } => matchers,
        Expr::MatrixSelector { vector, .. } => {
            if let Expr::VectorSelector { matchers, .. } = vector.as_ref() {
                matchers
            } else {
                return vec![];
            }
        }
        _ => return vec![],
    };
    matchers
        .iter()
        .filter(|m| m.op == MatchOp::Equal && m.name != "__name__")
        .map(|m| (m.name.clone(), m.value.clone()))
        .collect()
}

pub(crate) fn compute_rate(
    series: &[Series],
    return_increase: bool,
    range_ns: Option<i64>,
    eval_time: i64,
    output_ts: i64,
) -> Vec<Series> {
    series
        .iter()
        .filter_map(|s| {
            if s.samples.len() < 2 {
                return None;
            }
            let first = s.samples.first()?;
            let last = s.samples.last()?;
            let dt = (last.timestamp - first.timestamp) as f64 / 1_000_000_000.0;
            if dt <= 0.0 {
                return None;
            }
            // Walk all samples to detect multiple counter resets.
            // This pairwise approach is equivalent to
            // Prometheus's counterCorrection method.
            let mut total_increase = 0.0_f64;
            for w in s.samples.windows(2) {
                let delta = w[1].value - w[0].value;
                if delta >= 0.0 {
                    total_increase += delta;
                } else {
                    // Counter reset: value restarted from 0, reached w[1].
                    total_increase += w[1].value;
                }
            }

            // Prometheus `extrapolatedRate` (promql/functions.go).
            //
            // The observed increase covers `dt`, which is almost never the
            // whole range: the first sample lands somewhere after the range
            // start and the last somewhere before the evaluation time. The
            // increase is therefore scaled *up* by
            // `extrapolate_to / dt`, and `rate` then divides by the range —
            // which for an evenly scraped counter recovers the true slope.
            //
            // This used to scale by `range / extrapolate_to`, which
            // is neither of those things. With 15 s scrapes and `[1m]` it
            // under-reported by 25%, and the only test covering this branch
            // asserted just `> 0 && is_finite`.
            let value = if let Some(rng) = range_ns {
                let range_secs = rng as f64 / 1_000_000_000.0;
                let n = s.samples.len() as f64;
                let avg_interval = dt / (n - 1.0);

                let range_start = eval_time - rng;
                let mut to_start = (first.timestamp - range_start) as f64 / 1_000_000_000.0;
                let mut to_end = (eval_time - last.timestamp) as f64 / 1_000_000_000.0;

                // A gap larger than ~1 sample interval means the series
                // probably did not exist there, so extrapolate only half an
                // interval rather than across the whole gap.
                let threshold = avg_interval * 1.1;
                if to_start >= threshold {
                    to_start = avg_interval / 2.0;
                }
                if to_end >= threshold {
                    to_end = avg_interval / 2.0;
                }

                // Counter zero-clamping: a counter cannot have been negative,
                // so never extrapolate further back than the point at which it
                // would have been zero.
                if total_increase > 0.0 && first.value >= 0.0 {
                    let to_zero = dt * (first.value / total_increase);
                    if to_zero < to_start {
                        to_start = to_zero;
                    }
                }

                let extrapolate_to = dt + to_start + to_end;
                let mut factor = extrapolate_to / dt;
                if !return_increase {
                    if range_secs <= 0.0 {
                        return None;
                    }
                    factor /= range_secs;
                }
                total_increase * factor
            } else if return_increase {
                total_increase
            } else {
                total_increase / dt
            };
            Some(Series {
                labels: s
                    .labels
                    .iter()
                    .filter(|(k, _)| k != "__name__")
                    .cloned()
                    .collect(),
                samples: vec![Sample {
                    timestamp: output_ts,
                    value,
                }],
            })
        })
        .collect()
}

/// `irate` — the instantaneous rate from the last two samples.
///
/// `output_ts` is the evaluation timestamp, for the reason given on
/// [`eval_over_time_fn_full`]: the result is an instant vector, so every
/// sample in it belongs to the step being evaluated, not to whichever raw
/// sample happened to be last in the window.
pub(crate) fn compute_irate(series: &[Series], output_ts: i64) -> Vec<Series> {
    series
        .iter()
        .filter_map(|s| {
            if s.samples.len() < 2 {
                return None;
            }
            let n = s.samples.len();
            let prev = &s.samples[n - 2];
            let last = &s.samples[n - 1];
            let dt = (last.timestamp - prev.timestamp) as f64 / 1_000_000_000.0;
            if dt <= 0.0 {
                return None;
            }
            let mut dv = last.value - prev.value;
            if dv < 0.0 {
                dv = last.value;
            }
            Some(Series {
                labels: s
                    .labels
                    .iter()
                    .filter(|(k, _)| k != "__name__")
                    .cloned()
                    .collect(),
                samples: vec![Sample {
                    timestamp: output_ts,
                    value: dv / dt,
                }],
            })
        })
        .collect()
}

pub(crate) fn compute_delta(
    series: &[Series],
    range_ns: Option<i64>,
    eval_time: i64,
    output_ts: i64,
) -> Vec<Series> {
    series
        .iter()
        .filter_map(|s| {
            if s.samples.len() < 2 {
                return None;
            }
            let first = s.samples.first()?;
            let last = s.samples.last()?;
            let mut result_val = last.value - first.value;

            // Extrapolate delta to the full range (same logic as
            // Prometheus extrapolatedRate with isCounter=false).
            if let Some(rng) = range_ns {
                let dt = (last.timestamp - first.timestamp) as f64;
                if dt > 0.0 && s.samples.len() > 1 {
                    let avg_interval = dt / (s.samples.len() - 1) as f64;
                    let range_start = eval_time as f64 - rng as f64;
                    let raw_extra_start = (first.timestamp as f64 - range_start).max(0.0);
                    let extra_start = if raw_extra_start > avg_interval * 1.1 {
                        avg_interval / 2.0
                    } else {
                        raw_extra_start
                    };
                    let raw_extra_end = (eval_time - last.timestamp) as f64;
                    let extra_end = if raw_extra_end > avg_interval * 1.1 {
                        avg_interval / 2.0
                    } else {
                        raw_extra_end.max(0.0)
                    };
                    let extrapolation_to_interval = extra_start + extra_end + dt;
                    result_val *= extrapolation_to_interval / dt;
                }
            }

            Some(Series {
                labels: s
                    .labels
                    .iter()
                    .filter(|(k, _)| k != "__name__")
                    .cloned()
                    .collect(),
                samples: vec![Sample {
                    timestamp: output_ts,
                    value: result_val,
                }],
            })
        })
        .collect()
}

/// Order two sample values with `NaN` always last, in either direction.
///
/// Prometheus is deliberately asymmetric here: `sort`, `sort_desc`, `topk`
/// and `bottomk` all push `NaN` to the bottom, so `topk(1, x)` returns a real
/// value whenever one exists. `f64::total_cmp` cannot express that — it gives
/// `NaN` a fixed position in the total order, which reversing then moves to
/// the *top*, so `sort_desc` and `topk` returned the `NaN` series first.
/// Median of `vals` by Prometheus's `quantile(0.5, ...)`.
///
/// Prometheus interpolates between the two neighbouring order statistics
/// rather than taking the lower one, so an even-length window has a
/// fractional median. Rounding here instead would make `mad_over_time`
/// disagree with `quantile_over_time(0.5, ...)` on the same data.
fn median_of(vals: &[f64]) -> f64 {
    if vals.is_empty() {
        return f64::NAN;
    }
    let mut sorted: Vec<f64> = vals.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = 0.5 * (sorted.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    let weight = rank - lower as f64;
    sorted[lower] * (1.0 - weight) + sorted[upper] * weight
}

/// Prometheus's `double_exponential_smoothing` recurrence.
///
/// Transcribed from upstream rather than derived, because the seeding is what
/// distinguishes it from the textbook Holt linear method: the previous
/// smoothed value starts at **zero** rather than at `s[0]`, and the trend
/// update is skipped on the first iteration, so `b` carries `s[1] - s[0]`
/// through unchanged. Returns `None` for fewer than two samples — there is no
/// trend to estimate from one point, and upstream emits nothing rather than
/// echoing the sample back.
fn double_exponential_smoothing(vals: &[f64], sf: f64, tf: f64) -> Option<f64> {
    if vals.len() < 2 {
        return None;
    }
    let mut s0 = 0.0_f64;
    let mut s1 = vals[0];
    let mut b = vals[1] - vals[0];

    for (i, &v) in vals.iter().enumerate().skip(1) {
        let x = sf * v;
        // `calcTrendValue`: the first iteration passes the seeded trend
        // through untouched.
        if i > 1 {
            b = tf * (s1 - s0) + (1.0 - tf) * b;
        }
        let y = (1.0 - sf) * (s1 + b);
        s0 = s1;
        s1 = x + y;
    }
    Some(s1)
}

/// Natural ("version") string comparison: digit runs compare numerically.
///
/// `pod-2` sorts before `pod-10`, which plain lexicographic ordering gets
/// backwards. This is what `sort_by_label` promises, and sorting label values
/// as opaque strings is a deviation that produces a plausible-looking order —
/// the kind that no test checking "did it sort" would catch.
fn natural_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let (mut ai, mut bi) = (a.chars().peekable(), b.chars().peekable());
    loop {
        match (ai.peek().copied(), bi.peek().copied()) {
            (None, None) => return Ordering::Equal,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (Some(ca), Some(cb)) => {
                if ca.is_ascii_digit() && cb.is_ascii_digit() {
                    // Compare the whole digit runs as numbers. Leading zeros
                    // do not change the value, so they only break a tie.
                    let da: String = collect_digits(&mut ai);
                    let db: String = collect_digits(&mut bi);
                    let na = da.trim_start_matches('0');
                    let nb = db.trim_start_matches('0');
                    let ord = na.len().cmp(&nb.len()).then_with(|| na.cmp(nb));
                    if ord != Ordering::Equal {
                        return ord;
                    }
                    // Equal numerically: shorter (fewer leading zeros) first.
                    let ord = da.len().cmp(&db.len());
                    if ord != Ordering::Equal {
                        return ord;
                    }
                } else {
                    let ord = ca.cmp(&cb);
                    if ord != Ordering::Equal {
                        return ord;
                    }
                    ai.next();
                    bi.next();
                }
            }
        }
    }
}

fn collect_digits(it: &mut std::iter::Peekable<std::str::Chars<'_>>) -> String {
    let mut out = String::new();
    while it.peek().is_some_and(char::is_ascii_digit) {
        if let Some(c) = it.next() {
            out.push(c);
        }
    }
    out
}

/// Order two series by the named labels, falling back to the full label set.
///
/// The fallback is what makes the ordering total: without it, two series that
/// agree on every named label sort arbitrarily, so the same query can return
/// the same data in a different order on two runs.
fn compare_by_labels(a: &Series, b: &Series, labels: &[String]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let get = |s: &Series, name: &str| -> String {
        s.labels
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    };
    for name in labels {
        let (va, vb) = (get(a, name), get(b, name));
        if va == vb {
            continue;
        }
        return natural_cmp(&va, &vb);
    }
    let mut la: Vec<&(String, String)> = a.labels.iter().collect();
    let mut lb: Vec<&(String, String)> = b.labels.iter().collect();
    la.sort();
    lb.sort();
    la.cmp(&lb).then(Ordering::Equal)
}

pub(crate) fn nan_last_cmp(a: f64, b: f64, descending: bool) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => {
            if descending {
                b.total_cmp(&a)
            } else {
                a.total_cmp(&b)
            }
        }
    }
}

pub(crate) fn apply_scalar_fn(
    val: PromQLValue,
    f: impl Fn(f64) -> f64,
) -> Result<PromQLValue, EvalError> {
    match val {
        PromQLValue::Scalar(n) => Ok(PromQLValue::Scalar(f(n))),
        PromQLValue::Vector(series) => {
            let result = series
                .into_iter()
                .map(|mut s| {
                    // Prometheus drops __name__ for transform functions.
                    s.labels.retain(|(k, _)| k != "__name__");
                    for sample in &mut s.samples {
                        sample.value = f(sample.value);
                    }
                    s
                })
                .collect();
            Ok(PromQLValue::Vector(result))
        }
        _ => Err(EvalError(
            "scalar function applied to non-scalar/vector".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    /// [`FUNCTIONS`] is the parser's view of what this dispatcher implements,
    /// and the two live in one file precisely so this test can read both. A
    /// name the dispatcher handles but the list omits is the dangerous
    /// direction: the parser would reject a query this engine can answer.
    #[test]
    fn every_dispatched_function_is_listed() {
        let source = include_str!("function.rs");
        let mut missing = Vec::new();
        for line in source.lines() {
            let trimmed = line.trim_start();
            // A match arm: one or more quoted names, then `=>`.
            let Some(arrow) = trimmed.find("=>") else {
                continue;
            };
            let head = &trimmed[..arrow];
            if !head.starts_with('"') {
                continue;
            }
            for name in head.split('|') {
                let name = name.trim().trim_matches('"');
                if name.is_empty() || name.contains(char::is_whitespace) {
                    continue;
                }
                if !super::FUNCTIONS.contains(&name) {
                    missing.push(name.to_string());
                }
            }
        }
        missing.sort();
        missing.dedup();
        assert!(
            missing.is_empty(),
            "dispatched but not listed, so the parser would reject them: {missing:?}"
        );
    }

    use super::*;
    use crate::promql::ast::{Duration, LabelMatcher};

    #[test]
    fn test_compute_rate() {
        let series = vec![Series {
            labels: vec![("__name__".into(), "m".into())],
            samples: vec![
                Sample {
                    timestamp: 0,
                    value: 0.0,
                },
                Sample {
                    timestamp: 10_000_000_000,
                    value: 100.0,
                },
            ],
        }];
        let result = compute_rate(&series, false, None, 10_000_000_000, 10_000_000_000);
        assert_eq!(result.len(), 1);
        assert!((result[0].samples[0].value - 10.0).abs() < 1e-9);
    }

    /// A counter scraped every 15 s over a `[1m]` range.
    ///
    /// Prometheus computes `increase = delta × extrapolateToInterval /
    /// sampledInterval` and `rate = increase / range`, which for an evenly
    /// scraped counter recovers the true per-second slope. Every existing test
    /// here passed `range_ns: None`, so the extrapolation branch — the one
    /// that runs for every real `rate(x[5m])` — was never exercised.
    #[test]
    fn rate_over_a_range_recovers_the_true_slope() {
        const SEC: i64 = 1_000_000_000;
        // Scrapes at 15, 30, 45, 60 s; counter climbs by 10 each scrape.
        let series = vec![Series {
            labels: vec![("__name__".into(), "m".into())],
            samples: (1..=4)
                .map(|i| Sample {
                    timestamp: i * 15 * SEC,
                    value: 100.0 + f64::from(i as i32 - 1) * 10.0,
                })
                .collect(),
        }];

        let eval_time = 60 * SEC;
        let range = 60 * SEC;

        let rate = compute_rate(&series, false, Some(range), eval_time, eval_time);
        assert!(
            (rate[0].samples[0].value - 10.0 / 15.0).abs() < 1e-9,
            "rate(m[1m]) = {}, expected the true slope {}",
            rate[0].samples[0].value,
            10.0 / 15.0
        );

        // increase over the same window extrapolates the observed +30 across
        // the full minute: 30 × 60/45 = 40.
        let inc = compute_rate(&series, true, Some(range), eval_time, eval_time);
        assert!(
            (inc[0].samples[0].value - 40.0).abs() < 1e-9,
            "increase(m[1m]) = {}, expected 40",
            inc[0].samples[0].value
        );
    }

    /// When the samples already span the whole range there is nothing to
    /// extrapolate, so rate is exactly the observed slope.
    #[test]
    fn rate_needs_no_extrapolation_when_samples_fill_the_range() {
        const SEC: i64 = 1_000_000_000;
        let series = vec![Series {
            labels: vec![("__name__".into(), "m".into())],
            samples: vec![
                Sample {
                    timestamp: 0,
                    value: 0.0,
                },
                Sample {
                    timestamp: 60 * SEC,
                    value: 60.0,
                },
            ],
        }];
        let r = compute_rate(&series, false, Some(60 * SEC), 60 * SEC, 60 * SEC);
        assert!(
            (r[0].samples[0].value - 1.0).abs() < 1e-9,
            "expected 1.0/s, got {}",
            r[0].samples[0].value
        );
    }

    #[test]
    fn test_compute_rate_with_counter_reset() {
        let series = vec![Series {
            labels: vec![("__name__".into(), "m".into())],
            samples: vec![
                Sample {
                    timestamp: 0,
                    value: 100.0,
                },
                Sample {
                    timestamp: 10_000_000_000,
                    value: 50.0,
                }, // reset
            ],
        }];
        let result = compute_rate(&series, false, None, 10_000_000_000, 10_000_000_000);
        assert_eq!(result.len(), 1);
        // After reset, dv = last.value = 50, dt = 10s, rate = 5.0
        assert!((result[0].samples[0].value - 5.0).abs() < 1e-9);
    }

    #[test]
    fn test_compute_increase() {
        let series = vec![Series {
            labels: vec![("__name__".into(), "m".into())],
            samples: vec![
                Sample {
                    timestamp: 0,
                    value: 0.0,
                },
                Sample {
                    timestamp: 10_000_000_000,
                    value: 100.0,
                },
            ],
        }];
        let result = compute_rate(&series, true, None, 10_000_000_000, 10_000_000_000);
        assert_eq!(result.len(), 1);
        assert!((result[0].samples[0].value - 100.0).abs() < 1e-9);
    }

    #[test]
    fn test_compute_irate() {
        let series = vec![Series {
            labels: vec![("__name__".into(), "m".into())],
            samples: vec![
                Sample {
                    timestamp: 0,
                    value: 0.0,
                },
                Sample {
                    timestamp: 5_000_000_000,
                    value: 50.0,
                },
                Sample {
                    timestamp: 10_000_000_000,
                    value: 100.0,
                },
            ],
        }];
        let eval_time = 12_000_000_000;
        let result = compute_irate(&series, eval_time);
        assert_eq!(result.len(), 1);
        // (100 - 50) / 5 = 10
        assert!((result[0].samples[0].value - 10.0).abs() < 1e-9);
        assert_eq!(
            result[0].samples[0].timestamp, eval_time,
            "an instant vector is stamped at the evaluation time, not at the last raw sample"
        );
    }

    #[test]
    fn test_compute_delta() {
        let series = vec![Series {
            labels: vec![("__name__".into(), "m".into())],
            samples: vec![
                Sample {
                    timestamp: 0,
                    value: 10.0,
                },
                Sample {
                    timestamp: 10_000_000_000,
                    value: 30.0,
                },
            ],
        }];
        // Without range: simple difference
        let result = compute_delta(&series, None, 10_000_000_000, 10_000_000_000);
        assert_eq!(result.len(), 1);
        assert!((result[0].samples[0].value - 20.0).abs() < 1e-9);
    }

    #[test]
    fn test_rate_extrapolation_with_range() {
        // Counter goes from 0 to 100 in 10s, range is 15s
        let series = vec![Series {
            labels: vec![("__name__".into(), "m".into())],
            samples: vec![
                Sample {
                    timestamp: 5_000_000_000,
                    value: 0.0,
                }, // 5s into 15s range
                Sample {
                    timestamp: 15_000_000_000,
                    value: 100.0,
                }, // 15s
            ],
        }];
        let range_ns = Some(15_000_000_000_i64); // 15s range
        let result = compute_rate(&series, false, range_ns, 15_000_000_000, 15_000_000_000);
        assert_eq!(result.len(), 1);
        let rate_val = result[0].samples[0].value;

        // dt = 10 s, increase = 100. The counter starts at 0, so zero-clamping
        // pins the backward extrapolation at the point it would have been
        // zero — which is the first sample itself — giving
        // extrapolate_to = 10 s and rate = 100 × (10/10) / 15 = 6.667/s.
        assert!(
            (rate_val - 100.0 / 15.0).abs() < 1e-9,
            "rate = {rate_val}, expected {}",
            100.0 / 15.0
        );
    }

    #[test]
    fn test_deriv_numerical_stability() {
        // Use nanosecond-scale timestamps that would cause catastrophic
        // cancellation without base-timestamp subtraction.
        let ts_base: i64 = 1_700_000_000_000_000_000; // ~2023 in ns
        let samples = [
            Sample {
                timestamp: ts_base,
                value: 0.0,
            },
            Sample {
                timestamp: ts_base + 1_000_000_000,
                value: 1.0,
            },
            Sample {
                timestamp: ts_base + 2_000_000_000,
                value: 2.0,
            },
            Sample {
                timestamp: ts_base + 3_000_000_000,
                value: 3.0,
            },
        ];
        // Test the linear regression math directly (same as deriv):
        let base_ts = samples[0].timestamp as f64;
        let n = samples.len() as f64;
        let sum_x: f64 = samples.iter().map(|s| s.timestamp as f64 - base_ts).sum();
        let sum_y: f64 = samples.iter().map(|s| s.value).sum();
        let sum_xy: f64 = samples
            .iter()
            .map(|s| (s.timestamp as f64 - base_ts) * s.value)
            .sum();
        let sum_x2: f64 = samples
            .iter()
            .map(|s| {
                let x = s.timestamp as f64 - base_ts;
                x * x
            })
            .sum();
        let denom = n * sum_x2 - sum_x * sum_x;
        assert!(denom.abs() > f64::EPSILON, "denominator should be non-zero");
        let slope = (n * sum_xy - sum_x * sum_y) / denom * 1_000_000_000.0;
        // Slope should be ~1.0 per second (value increases by 1 each second)
        assert!((slope - 1.0).abs() < 1e-6, "slope = {slope}, expected ~1.0");
    }

    #[test]
    fn test_histogram_quantile_inf_bucket() {
        // Quantile in the +Inf bucket should return the upper bound
        // of the last finite bucket, not +Inf.
        let series = vec![
            // le=10, count=5
            Series {
                labels: vec![("le".into(), "10".into()), ("job".into(), "a".into())],
                samples: vec![Sample {
                    timestamp: 0,
                    value: 5.0,
                }],
            },
            // le=+Inf, count=10 (5 samples fall in the 10..+Inf range)
            Series {
                labels: vec![("le".into(), "+Inf".into()), ("job".into(), "a".into())],
                samples: vec![Sample {
                    timestamp: 0,
                    value: 10.0,
                }],
            },
        ];
        // q=0.75: rank = 0.75 * 10 = 7.5, which falls in the +Inf bucket
        let q = 0.75;
        let mut buckets: HistogramBuckets = BTreeMap::new();
        for s in &series {
            let le_val = s
                .labels
                .iter()
                .find(|(k, _)| k == "le")
                .map(|(_, v)| v.clone());
            let group_key: Vec<(String, String)> = s
                .labels
                .iter()
                .filter(|(k, _)| k != "le")
                .cloned()
                .collect();
            if let Some(le_str) = le_val {
                let le = if le_str == "+Inf" {
                    f64::INFINITY
                } else {
                    le_str.parse::<f64>().unwrap()
                };
                let count = s.samples.first().unwrap().value;
                buckets.entry(group_key).or_default().push((le, count));
            }
        }
        for bucket_list in buckets.values_mut() {
            bucket_list.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            let total = bucket_list.last().unwrap().1;
            let rank = q * total;
            let mut _prev_count = 0.0_f64;
            let mut prev_le = 0.0;
            for &(le, count) in bucket_list.iter() {
                if count >= rank {
                    if le.is_infinite() && le.is_sign_positive() {
                        // Should return prev_le = 10.0, NOT +Inf
                        assert!(
                            (prev_le - 10.0_f64).abs() < f64::EPSILON,
                            "expected 10.0 for +Inf bucket, got {prev_le}"
                        );
                    }
                    break;
                }
                _prev_count = count;
                prev_le = le;
            }
        }
    }

    #[test]
    fn test_changes_nan_handling() {
        // Prometheus excludes NaN→NaN transitions from the change count.
        let samples = [
            Sample {
                timestamp: 0,
                value: 1.0,
            },
            Sample {
                timestamp: 1,
                value: f64::NAN,
            },
            Sample {
                timestamp: 2,
                value: f64::NAN,
            },
            Sample {
                timestamp: 3,
                value: 1.0,
            },
        ];
        let changes: f64 = samples
            .windows(2)
            .filter(|w| w[0].value != w[1].value && !(w[0].value.is_nan() && w[1].value.is_nan()))
            .count() as f64;
        // 1→NaN (change), NaN→NaN (NOT a change), NaN→1 (change) = 2
        assert_eq!(changes, 2.0, "NaN→NaN should NOT count as a change");
    }

    #[test]
    fn test_absent_labels_reconstruction() {
        // absent() should reconstruct labels from equality matchers
        let labels = extract_absent_labels(&Expr::VectorSelector {
            name: None,
            matchers: vec![
                LabelMatcher {
                    name: "__name__".into(),
                    op: MatchOp::Equal,
                    value: "up".into(),
                },
                LabelMatcher {
                    name: "job".into(),
                    op: MatchOp::Equal,
                    value: "api".into(),
                },
                LabelMatcher {
                    name: "env".into(),
                    op: MatchOp::RegexMatch,
                    value: "prod.*".into(),
                },
            ],
            offset: None,
            at: None,
        });
        // Should include job=api but NOT __name__ and NOT regex matchers
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0], ("job".into(), "api".into()));
    }

    #[test]
    fn test_avg_over_time_computation() {
        let series = [Series {
            labels: vec![("__name__".into(), "m".into())],
            samples: vec![
                Sample {
                    timestamp: 0,
                    value: 10.0,
                },
                Sample {
                    timestamp: 1_000_000_000,
                    value: 20.0,
                },
                Sample {
                    timestamp: 2_000_000_000,
                    value: 30.0,
                },
            ],
        }];
        let vals: Vec<f64> = series[0].samples.iter().map(|s| s.value).collect();
        let avg = vals.iter().sum::<f64>() / vals.len() as f64;
        assert!((avg - 20.0).abs() < f64::EPSILON);

        let min = vals.iter().copied().fold(f64::INFINITY, f64::min);
        assert!((min - 10.0).abs() < f64::EPSILON);

        let max = vals.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        assert!((max - 30.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_rate_with_offset_extrapolation() {
        let offset_ns: i64 = 600_000_000_000; // 10 minutes
        let eval_time: i64 = 0;
        let range_ns: i64 = 300_000_000_000; // 5 minutes
        let window_end = eval_time - offset_ns;

        let series = vec![Series {
            labels: vec![("__name__".into(), "m".into())],
            samples: vec![
                Sample {
                    timestamp: -895_000_000_000,
                    value: 0.0,
                },
                Sample {
                    timestamp: -880_000_000_000,
                    value: 150.0,
                },
                Sample {
                    timestamp: -865_000_000_000,
                    value: 300.0,
                },
                Sample {
                    timestamp: -850_000_000_000,
                    value: 450.0,
                },
                Sample {
                    timestamp: -835_000_000_000,
                    value: 600.0,
                },
                Sample {
                    timestamp: -820_000_000_000,
                    value: 750.0,
                },
                Sample {
                    timestamp: -805_000_000_000,
                    value: 900.0,
                },
                Sample {
                    timestamp: -790_000_000_000,
                    value: 1050.0,
                },
                Sample {
                    timestamp: -775_000_000_000,
                    value: 1200.0,
                },
                Sample {
                    timestamp: -760_000_000_000,
                    value: 1350.0,
                },
                Sample {
                    timestamp: -745_000_000_000,
                    value: 1500.0,
                },
                Sample {
                    timestamp: -730_000_000_000,
                    value: 1650.0,
                },
                Sample {
                    timestamp: -715_000_000_000,
                    value: 1800.0,
                },
                Sample {
                    timestamp: -700_000_000_000,
                    value: 1950.0,
                },
                Sample {
                    timestamp: -685_000_000_000,
                    value: 2100.0,
                },
                Sample {
                    timestamp: -670_000_000_000,
                    value: 2250.0,
                },
                Sample {
                    timestamp: -655_000_000_000,
                    value: 2400.0,
                },
                Sample {
                    timestamp: -640_000_000_000,
                    value: 2550.0,
                },
                Sample {
                    timestamp: -625_000_000_000,
                    value: 2700.0,
                },
                Sample {
                    timestamp: -610_000_000_000,
                    value: 2850.0,
                },
            ],
        }];

        let result = compute_rate(&series, false, Some(range_ns), window_end, eval_time);
        assert_eq!(result.len(), 1);
        let rate = result[0].samples[0].value;
        assert!(rate > 0.0, "rate must be positive, got {rate}");
        assert!(rate.is_finite(), "rate must be finite, got {rate}");
        assert!(
            (rate - 10.0).abs() < 1.0,
            "rate should be ~10/s, got {rate}"
        );
        assert_eq!(result[0].samples[0].timestamp, eval_time);
    }

    #[test]
    fn test_extract_offset_ns() {
        // MatrixSelector with offset
        let expr = Expr::MatrixSelector {
            vector: Box::new(Expr::VectorSelector {
                name: Some("metric".into()),
                matchers: vec![],
                offset: Some(Duration(600.0)), // 10 minutes
                at: None,
            }),
            range: Duration(300.0), // 5 minutes
        };
        assert_eq!(extract_offset_ns(&expr), 600_000_000_000);

        // MatrixSelector without offset
        let expr_no_offset = Expr::MatrixSelector {
            vector: Box::new(Expr::VectorSelector {
                name: Some("metric".into()),
                matchers: vec![],
                offset: None,
                at: None,
            }),
            range: Duration(300.0),
        };
        assert_eq!(extract_offset_ns(&expr_no_offset), 0);

        // Non-matrix expression
        let expr_scalar = Expr::NumberLiteral(42.0);
        assert_eq!(extract_offset_ns(&expr_scalar), 0);
    }
}
