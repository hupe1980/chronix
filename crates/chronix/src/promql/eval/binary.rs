//! Binary expression evaluation for PromQL.
//!
//! Handles arithmetic, comparison, and set operations between scalars
//! and instant vectors, including `on()`/`ignoring()` label matching
//! and `group_left`/`group_right` many-to-one/one-to-many cardinality.

use std::collections::{HashMap, HashSet};

use crate::promql::ast::{
    BinaryOp, Expr, PromQLValue, Series, VectorMatching, VectorMatchingCardinality,
};

use super::{EvalError, PromQLEvaluator, QueryParams};

// ── PromQLEvaluator method ─────────────────────────────────────────────

impl PromQLEvaluator {
    pub(crate) fn eval_binary(
        &self,
        op: BinaryOp,
        lhs: &Expr,
        rhs: &Expr,
        bool_mod: bool,
        matching: Option<&VectorMatching>,
        params: &QueryParams,
    ) -> Result<PromQLValue, EvalError> {
        let lval = self.eval(lhs, params)?;
        let rval = self.eval(rhs, params)?;

        match (lval, rval) {
            (PromQLValue::Scalar(a), PromQLValue::Scalar(b)) => {
                let (result, _) = apply_binary_op(op, a, b, bool_mod);
                Ok(PromQLValue::Scalar(result))
            }
            (PromQLValue::Vector(series), PromQLValue::Scalar(s)) => {
                // Prometheus drops __name__ for arithmetic and comparison-with-bool
                let drop_name = op.is_arithmetic() || (op.is_comparison() && bool_mod);
                let result: Vec<_> = series
                    .into_iter()
                    .map(|mut ser| {
                        if drop_name {
                            ser.labels.retain(|(k, _)| k != "__name__");
                        }
                        ser.samples.retain_mut(|sample| {
                            let (val, keep) = apply_binary_op(op, sample.value, s, bool_mod);
                            sample.value = val;
                            keep
                        });
                        ser
                    })
                    // Drop series where all samples were filtered out by comparison
                    .filter(|ser| !ser.samples.is_empty())
                    .collect();
                Ok(PromQLValue::Vector(result))
            }
            (PromQLValue::Scalar(s), PromQLValue::Vector(series)) => {
                let drop_name = op.is_arithmetic() || (op.is_comparison() && bool_mod);
                let result: Vec<_> = series
                    .into_iter()
                    .map(|mut ser| {
                        if drop_name {
                            ser.labels.retain(|(k, _)| k != "__name__");
                        }
                        ser.samples.retain_mut(|sample| {
                            let (val, keep) = apply_binary_op(op, s, sample.value, bool_mod);
                            sample.value = val;
                            keep
                        });
                        ser
                    })
                    .filter(|ser| !ser.samples.is_empty())
                    .collect();
                Ok(PromQLValue::Vector(result))
            }
            (PromQLValue::Vector(left), PromQLValue::Vector(right)) => {
                // Set operators need special handling
                if op.is_set_operator() {
                    return Ok(PromQLValue::Vector(apply_set_op(
                        op, &left, &right, matching,
                    )));
                }

                let card = matching.map(|m| m.card);
                let is_one_to_many = card == Some(VectorMatchingCardinality::OneToMany);

                let mut result = Vec::new();
                let mut used_right: HashSet<usize> = HashSet::new();
                let drop_name = op.is_arithmetic() || (op.is_comparison() && bool_mod);

                if is_one_to_many {
                    // group_right: iterate RIGHT as many-side, LEFT as one-side.
                    // Hash-based O(N+M) matching rather than O(N×M).
                    let left_index = build_matching_index(&left, matching);
                    for rs in &right {
                        let sig = matching_signature(&rs.labels, matching);
                        let left_matches = match left_index.get(&sig) {
                            Some(indices) => indices.as_slice(),
                            None => continue,
                        };
                        let mut matched_left: Option<usize> = None;
                        for &li in left_matches {
                            let ls = &left[li];
                            if let Some(prev) = matched_left {
                                if prev != li {
                                    return Err(EvalError(
                                        "many-to-many matching not allowed: matching labels must be unique on one side".into(),
                                    ));
                                }
                            }
                            matched_left = Some(li);
                            let mut merged = rs.clone();
                            if drop_name {
                                merged.labels.retain(|(k, _)| k != "__name__");
                            }
                            // No on()/ignoring() pruning here: with
                            // group_right the "many" side keeps its full label
                            // set (see `prune_matching_labels`).
                            let lv = ls.samples.first().map_or(f64::NAN, |s| s.value);
                            merged.samples.retain_mut(|sample| {
                                let (val, keep) = apply_binary_op(op, lv, sample.value, bool_mod);
                                sample.value = val;
                                keep
                            });

                            // Copy extra labels from the "one" side (left)
                            if let Some(m) = matching {
                                for inc in &m.include {
                                    if let Some((_, v)) = ls.labels.iter().find(|(k, _)| k == inc) {
                                        if let Some(pos) =
                                            merged.labels.iter().position(|(k, _)| k == inc)
                                        {
                                            merged.labels[pos].1 = v.clone();
                                        } else {
                                            merged.labels.push((inc.clone(), v.clone()));
                                        }
                                    }
                                }
                            }

                            if merged.samples.is_empty() {
                                // one-to-many: don't consume left
                                continue;
                            }
                            result.push(merged);
                            // Don't break — many right can match one left
                        }
                    }
                } else {
                    // group_left or one-to-one: iterate LEFT as base.
                    // Hash-based O(N+M) matching rather than O(N×M).
                    let is_many = card == Some(VectorMatchingCardinality::ManyToOne);
                    let right_index = build_matching_index(&right, matching);
                    if !is_many {
                        // One-to-one requires the match group to identify at
                        // most one series on each side. Silently taking the
                        // first and dropping the rest turns a query the user
                        // wrote wrong into a plausible answer — Prometheus
                        // makes it an error and names the side at fault.
                        if let Some(sig) = right_index
                            .iter()
                            .find(|(_, idx)| idx.len() > 1)
                            .map(|(sig, _)| sig.clone())
                        {
                            return Err(EvalError(format!(
                                "found duplicate series on the right hand side of the operation: \
                                 many-to-one matching must be explicit (group_left/group_right); \
                                 match group {}",
                                describe_signature(&sig)
                            )));
                        }
                        let left_index = build_matching_index(&left, matching);
                        if let Some(sig) = left_index
                            .iter()
                            .find(|(sig, idx)| idx.len() > 1 && right_index.contains_key(*sig))
                            .map(|(sig, _)| sig.clone())
                        {
                            return Err(EvalError(format!(
                                "found duplicate series on the left hand side of the operation: \
                                 many-to-one matching must be explicit (group_left/group_right); \
                                 match group {}",
                                describe_signature(&sig)
                            )));
                        }
                    }
                    for ls in &left {
                        let sig = matching_signature(&ls.labels, matching);
                        let right_matches = match right_index.get(&sig) {
                            Some(indices) => indices.as_slice(),
                            None => continue,
                        };
                        for &ri in right_matches {
                            if !is_many && used_right.contains(&ri) {
                                continue;
                            }
                            let rs = &right[ri];
                            let mut merged = ls.clone();
                            if drop_name {
                                merged.labels.retain(|(k, _)| k != "__name__");
                            }
                            if !is_many {
                                prune_matching_labels(&mut merged.labels, matching);
                            }
                            let rv = rs.samples.first().map_or(f64::NAN, |s| s.value);
                            merged.samples.retain_mut(|sample| {
                                let (val, keep) = apply_binary_op(op, sample.value, rv, bool_mod);
                                sample.value = val;
                                keep
                            });

                            // For group_left, copy extra labels from the "one" side (right)
                            if let Some(m) = matching {
                                if m.card == VectorMatchingCardinality::ManyToOne {
                                    for inc in &m.include {
                                        if let Some((_, v)) =
                                            rs.labels.iter().find(|(k, _)| k == inc)
                                        {
                                            if let Some(pos) =
                                                merged.labels.iter().position(|(k, _)| k == inc)
                                            {
                                                merged.labels[pos].1 = v.clone();
                                            } else {
                                                merged.labels.push((inc.clone(), v.clone()));
                                            }
                                        }
                                    }
                                }
                            }

                            if merged.samples.is_empty() {
                                used_right.insert(ri);
                                if !is_many {
                                    break;
                                }
                                continue;
                            }
                            result.push(merged);
                            used_right.insert(ri);
                            if !is_many {
                                break; // one-to-one: only first match
                            }
                        }
                    }
                }
                Ok(PromQLValue::Vector(result))
            }
            _ => Err(EvalError("unsupported binary operation types".into())),
        }
    }
}

// ── Free functions ─────────────────────────────────────────────────────

/// Applies a binary operation and returns `(result_value, keep)`.
///
/// For comparison operators without `bool` modifier, `keep` indicates whether
/// the comparison passed. This avoids conflating NaN-as-sentinel with genuine
/// NaN input values (matching Prometheus's `vectorElemBinop` which returns
/// `(float64, bool)`).
///
/// For arithmetic operators and comparison operators with `bool` modifier,
/// `keep` is always `true` (every sample is kept).
pub(crate) fn apply_binary_op(op: BinaryOp, a: f64, b: f64, bool_mod: bool) -> (f64, bool) {
    match op {
        BinaryOp::Add => (a + b, true),
        BinaryOp::Sub => (a - b, true),
        BinaryOp::Mul => (a * b, true),
        BinaryOp::Div => (a / b, true),
        BinaryOp::Mod => (a % b, true),
        BinaryOp::Pow => (a.powf(b), true),
        BinaryOp::Eql => {
            let cmp = a == b;
            if bool_mod {
                (if cmp { 1.0 } else { 0.0 }, true)
            } else {
                (a, cmp)
            }
        }
        BinaryOp::Neq => {
            let cmp = a != b;
            if bool_mod {
                (if cmp { 1.0 } else { 0.0 }, true)
            } else {
                (a, cmp)
            }
        }
        BinaryOp::Lss => {
            let cmp = a < b;
            if bool_mod {
                (if cmp { 1.0 } else { 0.0 }, true)
            } else {
                (a, cmp)
            }
        }
        BinaryOp::Gtr => {
            let cmp = a > b;
            if bool_mod {
                (if cmp { 1.0 } else { 0.0 }, true)
            } else {
                (a, cmp)
            }
        }
        BinaryOp::Lte => {
            let cmp = a <= b;
            if bool_mod {
                (if cmp { 1.0 } else { 0.0 }, true)
            } else {
                (a, cmp)
            }
        }
        BinaryOp::Gte => {
            let cmp = a >= b;
            if bool_mod {
                (if cmp { 1.0 } else { 0.0 }, true)
            } else {
                (a, cmp)
            }
        }
        BinaryOp::And | BinaryOp::Or | BinaryOp::Unless => {
            // Set operations on scalars: not well-defined, return a
            (a, true)
        }
    }
}

#[cfg(test)]
pub(crate) fn labels_match(a: &[(String, String)], b: &[(String, String)]) -> bool {
    // Match on all labels except __name__
    let a_filtered: Vec<_> = a.iter().filter(|(k, _)| k != "__name__").collect();
    let b_filtered: Vec<_> = b.iter().filter(|(k, _)| k != "__name__").collect();
    a_filtered == b_filtered
}

#[cfg(test)]
pub(crate) fn labels_match_with_matching(
    a: &[(String, String)],
    b: &[(String, String)],
    matching: Option<&VectorMatching>,
) -> bool {
    matching_signature(a, matching) == matching_signature(b, matching)
}

/// Apply `on(...)` / `ignoring(...)` to a **one-to-one** result's labels.
///
/// Prometheus prunes here only for `CardOneToOne`: with `group_left` or
/// `group_right` the many side keeps its full label set, because that side is
/// what identifies the output series. Pruning there as well meant
/// `sum by (a, b) (x) / on(a) group_left(c) y` dropped `b` from the result —
/// collapsing distinct output series into one, which is a wrong answer rather
/// than a cosmetic one.
fn prune_matching_labels(labels: &mut Vec<(String, String)>, matching: Option<&VectorMatching>) {
    let Some(m) = matching else { return };
    if m.on {
        labels.retain(|(k, _)| m.labels.contains(k));
    } else if !m.labels.is_empty() {
        labels.retain(|(k, _)| !m.labels.contains(k));
    }
}

/// Render a matching signature back into `{k="v", …}` for an error message.
fn describe_signature(sig: &str) -> String {
    let mut out = String::from("{");
    for (i, pair) in sig.split('\u{2}').filter(|p| !p.is_empty()).enumerate() {
        let (k, v) = pair.split_once('\u{1}').unwrap_or((pair, ""));
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(k);
        out.push_str("=\"");
        out.push_str(v);
        out.push('"');
    }
    out.push('}');
    out
}

/// Compute the vector-matching signature for a label set.
///
/// With `on(...)` only the listed labels participate; with `ignoring(...)`
/// all labels except the listed ones (and `__name__`) participate; without a
/// matching clause all labels except `__name__` participate. Labels are
/// sorted so the signature is order-independent.
fn matching_signature(labels: &[(String, String)], matching: Option<&VectorMatching>) -> String {
    let mut sig: Vec<(String, String)> = match matching {
        Some(m) if m.on => labels
            .iter()
            .filter(|(k, _)| m.labels.contains(k))
            .cloned()
            .collect(),
        Some(m) if !m.labels.is_empty() => labels
            .iter()
            .filter(|(k, _)| k != "__name__" && !m.labels.contains(k))
            .cloned()
            .collect(),
        _ => labels
            .iter()
            .filter(|(k, _)| k != "__name__")
            .cloned()
            .collect(),
    };
    sig.sort();
    let mut out = String::new();
    for (k, v) in &sig {
        out.push_str(k);
        out.push('\u{1}');
        out.push_str(v);
        out.push('\u{2}');
    }
    out
}

/// Build a hash index from matching signature → series indices for O(N+M)
/// vector matching.
fn build_matching_index(
    series: &[Series],
    matching: Option<&VectorMatching>,
) -> HashMap<String, Vec<usize>> {
    let mut index: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, s) in series.iter().enumerate() {
        index
            .entry(matching_signature(&s.labels, matching))
            .or_default()
            .push(i);
    }
    index
}

/// Evaluate PromQL set operators (`and`, `or`, `unless`) on two instant
/// vectors.
fn apply_set_op(
    op: BinaryOp,
    left: &[Series],
    right: &[Series],
    matching: Option<&VectorMatching>,
) -> Vec<Series> {
    let right_map = build_matching_index(right, matching);
    match op {
        BinaryOp::And => left
            .iter()
            .filter(|ls| {
                let sig = matching_signature(&ls.labels, matching);
                right_map.contains_key(&sig)
            })
            .cloned()
            .collect(),
        BinaryOp::Or => {
            let left_sigs: HashSet<String> = left
                .iter()
                .map(|ls| matching_signature(&ls.labels, matching))
                .collect();
            let mut result: Vec<Series> = left.to_vec();
            for rs in right {
                let sig = matching_signature(&rs.labels, matching);
                if !left_sigs.contains(&sig) {
                    result.push(rs.clone());
                }
            }
            result
        }
        BinaryOp::Unless => left
            .iter()
            .filter(|ls| {
                let sig = matching_signature(&ls.labels, matching);
                !right_map.contains_key(&sig)
            })
            .cloned()
            .collect(),
        _ => left.to_vec(), // unreachable for set ops
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::ast::Sample;
    use crate::promql::ast::VectorMatchingCardinality;

    #[test]
    fn test_apply_binary_op() {
        assert_eq!(apply_binary_op(BinaryOp::Add, 1.0, 2.0, false), (3.0, true));
        assert_eq!(apply_binary_op(BinaryOp::Sub, 5.0, 3.0, false), (2.0, true));
        assert_eq!(
            apply_binary_op(BinaryOp::Mul, 4.0, 3.0, false),
            (12.0, true)
        );
        assert_eq!(
            apply_binary_op(BinaryOp::Div, 10.0, 4.0, false),
            (2.5, true)
        );
        assert_eq!(apply_binary_op(BinaryOp::Pow, 2.0, 3.0, false), (8.0, true));
    }

    #[test]
    fn test_comparison_bool_modifier() {
        assert_eq!(apply_binary_op(BinaryOp::Gtr, 5.0, 3.0, true), (1.0, true));
        assert_eq!(apply_binary_op(BinaryOp::Gtr, 1.0, 3.0, true), (0.0, true));
        assert_eq!(apply_binary_op(BinaryOp::Eql, 5.0, 5.0, true), (1.0, true));
        assert_eq!(apply_binary_op(BinaryOp::Eql, 5.0, 3.0, true), (0.0, true));
    }

    #[test]
    fn test_comparison_filter_mode() {
        // Filter mode (no bool modifier): keep=true when comparison passes, keep=false when it fails
        let (val, keep) = apply_binary_op(BinaryOp::Gtr, 5.0, 3.0, false);
        assert_eq!(val, 5.0);
        assert!(keep);

        let (val, keep) = apply_binary_op(BinaryOp::Gtr, 1.0, 3.0, false);
        assert_eq!(val, 1.0);
        assert!(!keep);

        // NaN input: comparison still returns keep based on IEEE semantics
        let (val, keep) = apply_binary_op(BinaryOp::Neq, f64::NAN, 5.0, false);
        assert!(val.is_nan());
        assert!(keep); // NaN != 5.0 is true in IEEE 754
    }

    #[test]
    fn test_labels_match() {
        let a = vec![
            ("__name__".into(), "a".into()),
            ("host".into(), "srv1".into()),
        ];
        let b = vec![
            ("__name__".into(), "b".into()),
            ("host".into(), "srv1".into()),
        ];
        assert!(labels_match(&a, &b)); // __name__ excluded

        let c = vec![
            ("__name__".into(), "c".into()),
            ("host".into(), "srv2".into()),
        ];
        assert!(!labels_match(&a, &c));
    }

    #[test]
    fn test_labels_match_with_on_modifier() {
        let a = vec![
            ("__name__".into(), "cpu".into()),
            ("host".into(), "srv1".into()),
            ("region".into(), "us".into()),
        ];
        let b = vec![
            ("__name__".into(), "mem".into()),
            ("host".into(), "srv1".into()),
            ("region".into(), "eu".into()),
        ];
        // Without matching: different "region" → no match
        assert!(!labels_match(&a, &b));

        // With on(host): only compare "host" → match
        let matching = VectorMatching {
            card: VectorMatchingCardinality::OneToOne,
            labels: vec!["host".into()],
            on: true,
            include: vec![],
        };
        assert!(labels_match_with_matching(&a, &b, Some(&matching)));

        // With ignoring(region): ignore "region" → compare only "host" → match
        let matching_ign = VectorMatching {
            card: VectorMatchingCardinality::OneToOne,
            labels: vec!["region".into()],
            on: false,
            include: vec![],
        };
        assert!(labels_match_with_matching(&a, &b, Some(&matching_ign)));
    }

    #[test]
    fn test_labels_match_with_on_no_match() {
        let a = vec![
            ("host".into(), "srv1".into()),
            ("region".into(), "us".into()),
        ];
        let b = vec![
            ("host".into(), "srv2".into()),
            ("region".into(), "us".into()),
        ];
        let matching = VectorMatching {
            card: VectorMatchingCardinality::OneToOne,
            labels: vec!["host".into()],
            on: true,
            include: vec![],
        };
        // Different "host" → no match even with on(host)
        assert!(!labels_match_with_matching(&a, &b, Some(&matching)));
    }

    #[test]
    fn test_binary_op_drops_name() {
        assert!(BinaryOp::Add.is_arithmetic());
        assert!(BinaryOp::Sub.is_arithmetic());
        assert!(BinaryOp::Mul.is_arithmetic());
        assert!(BinaryOp::Div.is_arithmetic());
        assert!(BinaryOp::Mod.is_arithmetic());
        assert!(BinaryOp::Pow.is_arithmetic());
        assert!(!BinaryOp::Eql.is_arithmetic());
        assert!(!BinaryOp::And.is_arithmetic());
    }

    #[test]
    fn test_apply_set_op_with_on_matching() {
        let left = vec![
            Series {
                labels: vec![("host".into(), "a".into()), ("region".into(), "us".into())],
                samples: vec![Sample {
                    timestamp: 0,
                    value: 1.0,
                }],
            },
            Series {
                labels: vec![("host".into(), "b".into()), ("region".into(), "eu".into())],
                samples: vec![Sample {
                    timestamp: 0,
                    value: 2.0,
                }],
            },
        ];
        let right = vec![Series {
            labels: vec![("host".into(), "a".into()), ("region".into(), "eu".into())],
            samples: vec![Sample {
                timestamp: 0,
                value: 3.0,
            }],
        }];
        // Without matching: "a,us" ≠ "a,eu" → AND returns empty
        let result_no_match = apply_set_op(BinaryOp::And, &left, &right, None);
        assert!(result_no_match.is_empty());

        // With on(host): match only on "host" → "a" matches "a"
        let matching = VectorMatching {
            card: VectorMatchingCardinality::OneToOne,
            labels: vec!["host".into()],
            on: true,
            include: vec![],
        };
        let result_on = apply_set_op(BinaryOp::And, &left, &right, Some(&matching));
        assert_eq!(result_on.len(), 1);
        assert_eq!(
            result_on[0]
                .labels
                .iter()
                .find(|(k, _)| k == "host")
                .unwrap()
                .1,
            "a"
        );
    }
}
