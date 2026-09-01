//! Zone map evaluator — predicate pushdown using per-row-group column stats.
//!
//! Each row group stores `ColumnStats { min_value, max_value, null_count }`.
//! The zone map evaluator checks predicates against these stats to decide
//! whether a row group can be entirely skipped.

/// Comparison operators supported by zone map evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZoneMapOp {
    /// `column == value`
    Eq,
    /// `column != value`
    Ne,
    /// `column > value`
    Gt,
    /// `column >= value`
    Ge,
    /// `column < value`
    Lt,
    /// `column <= value`
    Le,
}

/// A numeric predicate to evaluate against column zone maps.
#[derive(Debug, Clone, Copy)]
pub struct ZoneMapPredicate {
    /// The comparison operator.
    pub op: ZoneMapOp,
    /// The value to compare against (as f64 bits — supports i64 and f64).
    pub value: f64,
}

/// Result of evaluating a predicate against zone map stats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZoneMapResult {
    /// All rows in the zone satisfy the predicate (can skip decoding values).
    AllMatch,
    /// No rows in the zone can satisfy the predicate (skip the zone entirely).
    NoneMatch,
    /// Some rows may match — must decode and evaluate per row.
    MaybeMatch,
}

impl ZoneMapPredicate {
    /// Evaluate this predicate against a row group's column min/max stats.
    ///
    /// `min` and `max` are the column's zone map boundaries for a row group.
    ///
    /// # Returns
    ///
    /// - `NoneMatch` if no row in the group can satisfy the predicate
    /// - `AllMatch` if every row must satisfy the predicate
    /// - `MaybeMatch` if some rows might match
    #[must_use]
    pub fn evaluate(&self, min: f64, max: f64) -> ZoneMapResult {
        // NaN guard: if any operand is NaN, we cannot safely prune or
        // claim all-match — fall back to MaybeMatch (scan required).
        if self.value.is_nan() || min.is_nan() || max.is_nan() {
            return ZoneMapResult::MaybeMatch;
        }

        match self.op {
            ZoneMapOp::Eq => {
                if self.value < min || self.value > max {
                    ZoneMapResult::NoneMatch
                } else if min.to_bits() == max.to_bits() && min.to_bits() == self.value.to_bits() {
                    ZoneMapResult::AllMatch
                } else {
                    ZoneMapResult::MaybeMatch
                }
            }
            ZoneMapOp::Ne => {
                if min.to_bits() == max.to_bits() && min.to_bits() == self.value.to_bits() {
                    ZoneMapResult::NoneMatch
                } else if self.value < min || self.value > max {
                    ZoneMapResult::AllMatch
                } else {
                    ZoneMapResult::MaybeMatch
                }
            }
            ZoneMapOp::Gt => {
                if min > self.value {
                    ZoneMapResult::AllMatch
                } else if max <= self.value {
                    ZoneMapResult::NoneMatch
                } else {
                    ZoneMapResult::MaybeMatch
                }
            }
            ZoneMapOp::Ge => {
                if min >= self.value {
                    ZoneMapResult::AllMatch
                } else if max < self.value {
                    ZoneMapResult::NoneMatch
                } else {
                    ZoneMapResult::MaybeMatch
                }
            }
            ZoneMapOp::Lt => {
                if max < self.value {
                    ZoneMapResult::AllMatch
                } else if min >= self.value {
                    ZoneMapResult::NoneMatch
                } else {
                    ZoneMapResult::MaybeMatch
                }
            }
            ZoneMapOp::Le => {
                if max <= self.value {
                    ZoneMapResult::AllMatch
                } else if min > self.value {
                    ZoneMapResult::NoneMatch
                } else {
                    ZoneMapResult::MaybeMatch
                }
            }
        }
    }
}

/// Evaluate a time range predicate against a segment's timestamp bounds.
///
/// Returns `true` if the segment may contain data in `[range_start, range_end]`.
#[must_use]
pub fn segment_overlaps_range(
    seg_min_ts: i64,
    seg_max_ts: i64,
    range_start: i64,
    range_end: i64,
) -> bool {
    seg_max_ts >= range_start && seg_min_ts <= range_end
}

/// Evaluate row-group-level column stats against a predicate to determine
/// which row groups can be pruned.
///
/// Returns a `Vec<bool>` of length `row_group_count` indicating which row
/// groups should be read.
#[must_use]
pub fn prune_row_groups(
    row_group_stats: &[(f64, f64)], // (min, max) per row group
    predicate: &ZoneMapPredicate,
) -> Vec<bool> {
    row_group_stats
        .iter()
        .map(|&(min, max)| predicate.evaluate(min, max) != ZoneMapResult::NoneMatch)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eq_inside_range() {
        let pred = ZoneMapPredicate {
            op: ZoneMapOp::Eq,
            value: 50.0,
        };
        assert_eq!(pred.evaluate(0.0, 100.0), ZoneMapResult::MaybeMatch);
    }

    #[test]
    fn eq_outside_range() {
        let pred = ZoneMapPredicate {
            op: ZoneMapOp::Eq,
            value: 200.0,
        };
        assert_eq!(pred.evaluate(0.0, 100.0), ZoneMapResult::NoneMatch);
    }

    #[test]
    fn eq_constant_column() {
        let pred = ZoneMapPredicate {
            op: ZoneMapOp::Eq,
            value: 42.0,
        };
        assert_eq!(pred.evaluate(42.0, 42.0), ZoneMapResult::AllMatch);
    }

    #[test]
    fn gt_entire_range_above() {
        let pred = ZoneMapPredicate {
            op: ZoneMapOp::Gt,
            value: 10.0,
        };
        assert_eq!(pred.evaluate(50.0, 100.0), ZoneMapResult::AllMatch);
    }

    #[test]
    fn gt_entire_range_below() {
        let pred = ZoneMapPredicate {
            op: ZoneMapOp::Gt,
            value: 100.0,
        };
        assert_eq!(pred.evaluate(50.0, 100.0), ZoneMapResult::NoneMatch);
    }

    #[test]
    fn lt_entire_range_below() {
        let pred = ZoneMapPredicate {
            op: ZoneMapOp::Lt,
            value: 200.0,
        };
        assert_eq!(pred.evaluate(50.0, 100.0), ZoneMapResult::AllMatch);
    }

    #[test]
    fn lt_entire_range_above() {
        let pred = ZoneMapPredicate {
            op: ZoneMapOp::Lt,
            value: 10.0,
        };
        assert_eq!(pred.evaluate(50.0, 100.0), ZoneMapResult::NoneMatch);
    }

    #[test]
    fn ne_constant_column_matches_value() {
        let pred = ZoneMapPredicate {
            op: ZoneMapOp::Ne,
            value: 42.0,
        };
        assert_eq!(pred.evaluate(42.0, 42.0), ZoneMapResult::NoneMatch);
    }

    #[test]
    fn ne_value_outside_range() {
        let pred = ZoneMapPredicate {
            op: ZoneMapOp::Ne,
            value: 200.0,
        };
        assert_eq!(pred.evaluate(0.0, 100.0), ZoneMapResult::AllMatch);
    }

    #[test]
    fn ge_at_boundary() {
        let pred = ZoneMapPredicate {
            op: ZoneMapOp::Ge,
            value: 50.0,
        };
        assert_eq!(pred.evaluate(50.0, 100.0), ZoneMapResult::AllMatch);
    }

    #[test]
    fn le_at_boundary() {
        let pred = ZoneMapPredicate {
            op: ZoneMapOp::Le,
            value: 100.0,
        };
        assert_eq!(pred.evaluate(50.0, 100.0), ZoneMapResult::AllMatch);
    }

    #[test]
    fn prune_row_groups_mixed() {
        // 5 row groups with known ranges
        let stats = vec![
            (0.0, 10.0),    // should match
            (20.0, 30.0),   // should match
            (50.0, 60.0),   // should NOT match
            (5.0, 15.0),    // should match (maybe)
            (100.0, 200.0), // should NOT match
        ];
        let pred = ZoneMapPredicate {
            op: ZoneMapOp::Lt,
            value: 40.0,
        };
        let result = prune_row_groups(&stats, &pred);
        assert_eq!(result, vec![true, true, false, true, false]);
    }

    #[test]
    fn segment_overlap_basic() {
        assert!(segment_overlaps_range(100, 200, 150, 250));
        assert!(segment_overlaps_range(100, 200, 50, 150));
        assert!(!segment_overlaps_range(100, 200, 300, 400));
        assert!(!segment_overlaps_range(100, 200, 0, 50));
        assert!(segment_overlaps_range(100, 200, 100, 200)); // exact
    }

    // ── NaN safety tests ────────────────────────────────────────────

    #[test]
    fn nan_value_returns_maybe_match() {
        let pred = ZoneMapPredicate {
            op: ZoneMapOp::Eq,
            value: f64::NAN,
        };
        assert_eq!(pred.evaluate(0.0, 100.0), ZoneMapResult::MaybeMatch);
    }

    #[test]
    fn nan_min_returns_maybe_match() {
        let pred = ZoneMapPredicate {
            op: ZoneMapOp::Gt,
            value: 50.0,
        };
        assert_eq!(pred.evaluate(f64::NAN, 100.0), ZoneMapResult::MaybeMatch);
    }

    #[test]
    fn nan_max_returns_maybe_match() {
        let pred = ZoneMapPredicate {
            op: ZoneMapOp::Lt,
            value: 50.0,
        };
        assert_eq!(pred.evaluate(0.0, f64::NAN), ZoneMapResult::MaybeMatch);
    }

    #[test]
    fn nan_all_returns_maybe_match() {
        for op in [
            ZoneMapOp::Eq,
            ZoneMapOp::Ne,
            ZoneMapOp::Gt,
            ZoneMapOp::Ge,
            ZoneMapOp::Lt,
            ZoneMapOp::Le,
        ] {
            let pred = ZoneMapPredicate {
                op,
                value: f64::NAN,
            };
            assert_eq!(
                pred.evaluate(f64::NAN, f64::NAN),
                ZoneMapResult::MaybeMatch,
                "op={op:?} with all-NaN should be MaybeMatch"
            );
        }
    }

    #[test]
    fn nan_in_prune_row_groups_keeps_nan_groups() {
        let stats = vec![
            (0.0, 100.0),     // normal
            (f64::NAN, 50.0), // NaN min → MaybeMatch → kept
            (10.0, f64::NAN), // NaN max → MaybeMatch → kept
        ];
        let pred = ZoneMapPredicate {
            op: ZoneMapOp::Gt,
            value: 200.0,
        };
        let result = prune_row_groups(&stats, &pred);
        // Normal group: NoneMatch (max=100 <= 200). NaN groups: MaybeMatch → kept.
        assert_eq!(result, vec![false, true, true]);
    }
}
