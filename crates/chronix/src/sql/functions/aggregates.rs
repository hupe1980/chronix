use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type, TimeUnit};
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};

use super::helpers::extract_timestamps;

// ── first(value, timestamp) ─────────────────────────────────────────────

/// `first(value, timestamp)` — return the value at the earliest timestamp.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct FirstUdaf {
    signature: Signature,
}

impl FirstUdaf {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for FirstUdaf {
    fn name(&self) -> &'static str {
        "first"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(FirstAccumulator {
            value: ScalarValue::Null,
            timestamp: i64::MAX,
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                "value",
                args.return_field.data_type().clone(),
                true,
            )),
            Arc::new(Field::new("timestamp", DataType::Int64, true)),
        ])
    }
}

#[derive(Debug)]
struct FirstAccumulator {
    value: ScalarValue,
    timestamp: i64,
}

impl Accumulator for FirstAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let ts_array = extract_timestamps(&values[1])?;
        let val_array = &values[0];

        for i in 0..ts_array.len() {
            if ts_array.is_null(i) {
                continue;
            }
            let ts = ts_array.value(i);
            if ts < self.timestamp {
                self.timestamp = ts;
                self.value = ScalarValue::try_from_array(val_array, i)?;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(self.value.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.value.size()
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![
            self.value.clone(),
            ScalarValue::Int64(Some(self.timestamp)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let val_array = &states[0];
        let ts_array = states[1]
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Internal("expected Int64 state".into())
            })?;

        for i in 0..ts_array.len() {
            if ts_array.is_null(i) {
                continue;
            }
            let ts = ts_array.value(i);
            if ts < self.timestamp {
                self.timestamp = ts;
                self.value = ScalarValue::try_from_array(val_array, i)?;
            }
        }
        Ok(())
    }
}

// ── last(value, timestamp) ──────────────────────────────────────────────

/// `last(value, timestamp)` — return the value at the latest timestamp.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct LastUdaf {
    signature: Signature,
}

impl LastUdaf {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for LastUdaf {
    fn name(&self) -> &'static str {
        "last"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(LastAccumulator {
            value: ScalarValue::Null,
            timestamp: i64::MIN,
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                "value",
                args.return_field.data_type().clone(),
                true,
            )),
            Arc::new(Field::new("timestamp", DataType::Int64, true)),
        ])
    }
}

#[derive(Debug)]
struct LastAccumulator {
    value: ScalarValue,
    timestamp: i64,
}

impl Accumulator for LastAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let ts_array = extract_timestamps(&values[1])?;
        let val_array = &values[0];

        for i in 0..ts_array.len() {
            if ts_array.is_null(i) {
                continue;
            }
            let ts = ts_array.value(i);
            if ts > self.timestamp {
                self.timestamp = ts;
                self.value = ScalarValue::try_from_array(val_array, i)?;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(self.value.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.value.size()
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![
            self.value.clone(),
            ScalarValue::Int64(Some(self.timestamp)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let val_array = &states[0];
        let ts_array = states[1]
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Internal("expected Int64 state".into())
            })?;

        for i in 0..ts_array.len() {
            if ts_array.is_null(i) {
                continue;
            }
            let ts = ts_array.value(i);
            if ts > self.timestamp {
                self.timestamp = ts;
                self.value = ScalarValue::try_from_array(val_array, i)?;
            }
        }
        Ok(())
    }
}

// ── rate(value, timestamp) ──────────────────────────────────────────────

/// `rate(value, timestamp)` — per-second rate of change over the group.
///
/// Computes `total_increase / (last_time - first_time)` in seconds, where
/// `total_increase` sums the per-sample deltas with Prometheus's counter-reset
/// rule: a drop means the counter restarted, so the new value *is* the
/// increase since the reset.
///
/// Unlike PromQL's `rate()`, there is no extrapolation to a range window —
/// a SQL group has no window beyond the samples in it, so the observed span
/// *is* the denominator.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct RateUdaf {
    signature: Signature,
}

impl RateUdaf {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::Float64,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for RateUdaf {
    fn name(&self) -> &'static str {
        "rate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(RateAccumulator::default()))
    }

    fn state_fields(&self, _: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        // O(1) streaming state instead of O(N) sample lists.
        Ok(vec![
            Arc::new(Field::new("first_time", DataType::Int64, true)),
            Arc::new(Field::new("first_val", DataType::Float64, true)),
            Arc::new(Field::new("last_time", DataType::Int64, true)),
            Arc::new(Field::new("last_val", DataType::Float64, true)),
            Arc::new(Field::new("prev_val", DataType::Float64, true)),
            Arc::new(Field::new("total_increase", DataType::Float64, true)),
            Arc::new(Field::new("count", DataType::Int64, true)),
        ])
    }
}

/// One contiguous run of samples, reduced to what `rate` needs from it.
///
/// A partial is closed under concatenation: two adjacent runs combine into one
/// by adding their increases plus the increase across the boundary between
/// them. That is the whole merge, and expressing it as a value rather than as
/// mutation of the accumulator is what makes the merge order-independent.
#[derive(Debug, Clone, Copy)]
struct RatePartial {
    first_time: i64,
    first_val: f64,
    last_time: i64,
    last_val: f64,
    /// Counter-reset-corrected increase *within* this run.
    increase: f64,
    count: u64,
}

impl RatePartial {
    /// Increase from `prev`'s last value to `next`'s first, with Prometheus's
    /// counter-reset rule: a drop means the counter restarted, so the new
    /// value *is* the increase.
    fn boundary_increase(prev_last: f64, next_first: f64) -> f64 {
        let delta = next_first - prev_last;
        if delta >= 0.0 {
            delta
        } else {
            next_first
        }
    }

    /// Append a run that starts at or after this one ends.
    fn concat(self, next: Self) -> Self {
        Self {
            first_time: self.first_time,
            first_val: self.first_val,
            last_time: next.last_time,
            last_val: next.last_val,
            increase: self.increase
                + next.increase
                + Self::boundary_increase(self.last_val, next.first_val),
            count: self.count + next.count,
        }
    }
}

/// Streaming rate accumulator.
///
/// `update_batch` folds samples into a single partial in O(1); `merge_batch`
/// keeps the partials it is handed and they are folded on demand. Memory is
/// therefore O(partial states), which is the number of partitions rather than
/// the number of rows, which is what the O(1) claim was about — while the fold
/// no longer depends on the order the states arrive in.
///
/// **Why the partials are kept rather than folded on arrival.** The previous
/// version folded each incoming state into the accumulator immediately and
/// added the cross-partition increase only when `i > 0` *within a single
/// call*. DataFusion calls `merge_batch` once per batch of partial states, so
/// with more than one such batch the boundary between the last partial of one
/// call and the first of the next was silently skipped, and `rate()` came back
/// low by exactly that increase. Sorting inside one call also cannot order
/// states across calls, so no in-place fold can be correct.
#[derive(Debug, Default)]
struct RateAccumulator {
    /// Folded from `update_batch`.
    own: Option<RatePartial>,
    /// Received from `merge_batch`, folded on demand.
    received: Vec<RatePartial>,
}

impl RateAccumulator {
    /// Fold every partial into one, in timestamp order.
    ///
    /// Runs are ordered by `first_time` and concatenated, so the result is the
    /// same whatever order the partials were produced or received in.
    fn combined(&self) -> Option<RatePartial> {
        let mut parts: Vec<RatePartial> = self
            .own
            .iter()
            .copied()
            .chain(self.received.iter().copied())
            .filter(|p| p.count > 0)
            .collect();
        if parts.is_empty() {
            return None;
        }
        parts.sort_by_key(|p| (p.first_time, p.last_time));
        let mut folded = parts[0];
        for next in &parts[1..] {
            folded = folded.concat(*next);
        }
        Some(folded)
    }
}

impl Accumulator for RateAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let vals = values[0].as_primitive::<Float64Type>();
        let times = extract_timestamps(&values[1])?;

        for i in 0..vals.len() {
            if vals.is_null(i) || times.is_null(i) {
                continue;
            }
            let t = times.value(i);
            let v = vals.value(i);

            match &mut self.own {
                None => {
                    self.own = Some(RatePartial {
                        first_time: t,
                        first_val: v,
                        last_time: t,
                        last_val: v,
                        increase: 0.0,
                        count: 1,
                    });
                }
                Some(p) => {
                    // Counter-reset-corrected increase against the previous
                    // sample, in arrival order — which is timestamp order for
                    // every scan this engine produces.
                    p.increase += RatePartial::boundary_increase(p.last_val, v);
                    p.count += 1;
                    if t < p.first_time {
                        p.first_time = t;
                        p.first_val = v;
                    }
                    if t >= p.last_time {
                        p.last_time = t;
                    }
                    p.last_val = v;
                }
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let Some(p) = self.combined() else {
            return Ok(ScalarValue::Float64(None));
        };
        if p.count < 2 || p.last_time <= p.first_time {
            return Ok(ScalarValue::Float64(None));
        }
        let dt_secs = (p.last_time - p.first_time) as f64 / 1e9;
        Ok(ScalarValue::Float64(Some(p.increase / dt_secs)))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.received.capacity() * std::mem::size_of::<RatePartial>()
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let p = self.combined();
        Ok(vec![
            ScalarValue::Int64(p.map(|p| p.first_time)),
            ScalarValue::Float64(p.map(|p| p.first_val)),
            ScalarValue::Int64(p.map(|p| p.last_time)),
            ScalarValue::Float64(p.map(|p| p.last_val)),
            // `prev_val` is the run's last value; kept in the state layout so
            // the field list is unchanged.
            ScalarValue::Float64(p.map(|p| p.last_val)),
            ScalarValue::Float64(p.map(|p| p.increase)),
            ScalarValue::Int64(p.map(|p| p.count as i64)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let int64 = |idx: usize| -> DFResult<&arrow::array::Int64Array> {
            states[idx]
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Internal(
                        "RateAccumulator: expected Int64Array".to_string(),
                    )
                })
        };
        let first_times = int64(0)?;
        let first_vals = states[1].as_primitive::<Float64Type>();
        let last_times = int64(2)?;
        let last_vals = states[3].as_primitive::<Float64Type>();
        let increases = states[5].as_primitive::<Float64Type>();
        let counts = int64(6)?;

        for i in 0..first_times.len() {
            if first_times.is_null(i) || counts.is_null(i) || counts.value(i) == 0 {
                continue;
            }
            self.received.push(RatePartial {
                first_time: first_times.value(i),
                first_val: first_vals.value(i),
                last_time: last_times.value(i),
                last_val: last_vals.value(i),
                increase: increases.value(i),
                count: counts.value(i) as u64,
            });
        }
        Ok(())
    }
}

// ── irate(value, timestamp) ─────────────────────────────────────────────

/// `irate(value, timestamp)` — instantaneous per-second rate from last two points.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct IRateUdaf {
    signature: Signature,
}

impl IRateUdaf {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::Float64,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for IRateUdaf {
    fn name(&self) -> &'static str {
        "irate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(IRateAccumulator::default()))
    }

    fn state_fields(&self, _: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new("second_last_value", DataType::Float64, true)),
            Arc::new(Field::new("second_last_time", DataType::Int64, true)),
            Arc::new(Field::new("last_value", DataType::Float64, true)),
            Arc::new(Field::new("last_time", DataType::Int64, true)),
        ])
    }
}

#[derive(Debug, Default)]
struct IRateAccumulator {
    /// Second-to-last (value, time)
    second_last: Option<(f64, i64)>,
    /// Last (value, time) — the most recent
    last: Option<(f64, i64)>,
}

impl Accumulator for IRateAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let vals = values[0].as_primitive::<Float64Type>();
        let times = extract_timestamps(&values[1])?;

        for i in 0..vals.len() {
            if vals.is_null(i) || times.is_null(i) {
                continue;
            }
            let v = vals.value(i);
            let t = times.value(i);

            match self.last {
                None => {
                    self.last = Some((v, t));
                }
                Some((_, lt)) => {
                    if t > lt {
                        self.second_last = self.last;
                        self.last = Some((v, t));
                    } else if t < lt {
                        match self.second_last {
                            None => self.second_last = Some((v, t)),
                            Some((_, slt)) if t > slt => self.second_last = Some((v, t)),
                            _ => {}
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        match (self.second_last, self.last) {
            (Some((sv, st)), Some((lv, lt))) if lt > st => {
                let dt_secs = (lt - st) as f64 / 1e9;
                let dv = if lv >= sv { lv - sv } else { lv }; // counter reset
                Ok(ScalarValue::Float64(Some(dv / dt_secs)))
            }
            _ => Ok(ScalarValue::Float64(None)),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let (slv, slt) = self.second_last.unzip();
        let (lv, lt) = self.last.unzip();
        Ok(vec![
            ScalarValue::Float64(slv),
            ScalarValue::Int64(slt),
            ScalarValue::Float64(lv),
            ScalarValue::Int64(lt),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let slv = states[0].as_primitive::<Float64Type>();
        let slt = states[1]
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "IRateAccumulator: expected Int64Array for second_last_time".to_string(),
                )
            })?;
        let lv = states[2].as_primitive::<Float64Type>();
        let lt = states[3]
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "IRateAccumulator: expected Int64Array for last_time".to_string(),
                )
            })?;

        for i in 0..lv.len() {
            if !lt.is_null(i) {
                let v = lv.value(i);
                let t = lt.value(i);
                self.insert_point(v, t);
            }
            if !slt.is_null(i) {
                let v = slv.value(i);
                let t = slt.value(i);
                self.insert_point(v, t);
            }
        }
        Ok(())
    }
}

impl IRateAccumulator {
    fn insert_point(&mut self, v: f64, t: i64) {
        match self.last {
            None => {
                self.last = Some((v, t));
            }
            Some((_, lt)) => {
                if t > lt {
                    self.second_last = self.last;
                    self.last = Some((v, t));
                } else if t < lt {
                    match self.second_last {
                        None => self.second_last = Some((v, t)),
                        Some((_, slt)) if t > slt => self.second_last = Some((v, t)),
                        _ => {}
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, TimestampNanosecondArray};
    use std::sync::Arc;

    #[test]
    fn rate_accumulator_basic() {
        let mut acc = RateAccumulator::default();
        let vals = Arc::new(Float64Array::from(vec![0.0, 10.0])) as ArrayRef;
        let ts = Arc::new(TimestampNanosecondArray::from(vec![
            1_000_000_000,
            2_000_000_000,
        ])) as ArrayRef;
        acc.update_batch(&[vals, ts]).unwrap();
        let result = acc.evaluate().unwrap();
        // 10.0 / 1.0s = 10.0
        assert_eq!(result, ScalarValue::Float64(Some(10.0)));
    }

    #[test]
    fn irate_accumulator_basic() {
        let mut acc = IRateAccumulator::default();
        let vals = Arc::new(Float64Array::from(vec![0.0, 5.0, 15.0])) as ArrayRef;
        let ts = Arc::new(TimestampNanosecondArray::from(vec![
            1_000_000_000,
            2_000_000_000,
            3_000_000_000,
        ])) as ArrayRef;
        acc.update_batch(&[vals, ts]).unwrap();
        let result = acc.evaluate().unwrap();
        // (15.0 - 5.0) / 1.0s = 10.0
        assert_eq!(result, ScalarValue::Float64(Some(10.0)));
    }

    #[test]
    fn first_last_accumulators() {
        // First
        let mut first = FirstAccumulator {
            value: ScalarValue::Null,
            timestamp: i64::MAX,
        };
        let vals = Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])) as ArrayRef;
        let ts = Arc::new(TimestampNanosecondArray::from(vec![
            3_000_000_000i64,
            1_000_000_000,
            2_000_000_000,
        ])) as ArrayRef;
        first.update_batch(&[vals.clone(), ts.clone()]).unwrap();
        assert_eq!(first.evaluate().unwrap(), ScalarValue::Float64(Some(20.0)));

        // Last
        let mut last = LastAccumulator {
            value: ScalarValue::Null,
            timestamp: i64::MIN,
        };
        last.update_batch(&[vals, ts]).unwrap();
        assert_eq!(last.evaluate().unwrap(), ScalarValue::Float64(Some(10.0)));
    }

    /// Build one partial-state row set for `RateAccumulator::merge_batch`.
    fn rate_state(
        first_time: i64,
        first_val: f64,
        last_time: i64,
        last_val: f64,
        increase: f64,
        count: i64,
    ) -> Vec<ArrayRef> {
        vec![
            Arc::new(arrow::array::Int64Array::from(vec![first_time])) as ArrayRef,
            Arc::new(Float64Array::from(vec![first_val])),
            Arc::new(arrow::array::Int64Array::from(vec![last_time])),
            Arc::new(Float64Array::from(vec![last_val])),
            Arc::new(Float64Array::from(vec![last_val])),
            Arc::new(Float64Array::from(vec![increase])),
            Arc::new(arrow::array::Int64Array::from(vec![count])),
        ]
    }

    /// Partial states arriving in separate `merge_batch` calls must combine
    /// exactly as if they had arrived together.
    ///
    /// DataFusion calls `merge_batch` once per batch of partial states. The
    /// previous fold added the cross-partition increase only for states after
    /// the first *within one call*, so with two calls the boundary between
    /// them was skipped and `rate()` came back low by exactly that increase.
    /// Nothing errored; the number was simply smaller.
    #[test]
    fn rate_merges_across_separate_batches() {
        const SEC: i64 = 1_000_000_000;

        // Three runs of a counter climbing by 10/s, split at 10 s and 20 s.
        // Together they span 0..30 s and increase by 300.
        let runs = [
            rate_state(0, 0.0, 10 * SEC, 100.0, 100.0, 11),
            rate_state(10 * SEC, 100.0, 20 * SEC, 200.0, 100.0, 11),
            rate_state(20 * SEC, 200.0, 30 * SEC, 300.0, 100.0, 11),
        ];

        // One call with all three.
        let mut together = RateAccumulator::default();
        for r in &runs {
            together.merge_batch(r).unwrap();
        }
        let expected = together.evaluate().unwrap();

        // The same three, delivered in a different order.
        let mut shuffled = RateAccumulator::default();
        for i in [2usize, 0, 1] {
            shuffled.merge_batch(&runs[i]).unwrap();
        }
        assert_eq!(
            shuffled.evaluate().unwrap(),
            expected,
            "the merge must not depend on the order partial states arrive in"
        );

        // …and the value is the true slope: 300 over 30 s.
        match expected {
            ScalarValue::Float64(Some(v)) => assert!(
                (v - 10.0).abs() < 1e-9,
                "rate across three merged runs should be 10/s, got {v}"
            ),
            other => panic!("expected a rate, got {other:?}"),
        }
    }

    #[test]
    fn rate_accumulator_multiple_counter_resets() {
        let mut acc = RateAccumulator::default();
        let vals =
            Arc::new(Float64Array::from(vec![100.0, 200.0, 0.0, 50.0, 0.0, 10.0])) as ArrayRef;
        let ts = Arc::new(TimestampNanosecondArray::from(vec![
            1_000_000_000i64,
            2_000_000_000,
            3_000_000_000,
            4_000_000_000,
            5_000_000_000,
            6_000_000_000,
        ])) as ArrayRef;
        acc.update_batch(&[vals, ts]).unwrap();
        let result = acc.evaluate().unwrap();
        // total_increase = 100 + 0 + 50 + 0 + 10 = 160
        // dt = 5s → rate = 32.0
        assert_eq!(result, ScalarValue::Float64(Some(32.0)));
    }
}

// ── forecast(value, timestamp, horizon) ─────────────────────────────────

/// `forecast(value, timestamp, horizon)` — `horizon` predicted values, as a
/// `LIST(DOUBLE)`.
///
/// # Why an aggregate, and why a list
///
/// A forecast is not a property of a row: it is `horizon` *new* values after
/// the last one, so there is no existing row to put them on. An aggregate
/// consumes a group and returns one value, and a list is how a group returns
/// several numbers:
///
/// ```sql
/// SELECT host, unnest(forecast(usage, _time, 12)) AS predicted
/// FROM cpu
/// GROUP BY host
/// ```
///
/// The `timestamp` argument is not decoration: aggregates see rows in whatever
/// order the plan delivers them, so the accumulator sorts by it. Passing a
/// constant, or a column that is not time, produces a forecast of a shuffled
/// series.
///
/// The model is simple exponential smoothing, which is the right default for a
/// metric with no seasonality declared. For seasonal or trending series, fit
/// [`HoltWintersModel`](chronix_analytics::forecast::HoltWintersModel) or
/// [`SarimaModel`](chronix_analytics::forecast::SarimaModel) through the
/// embedded API, where the model is a choice rather than an assumption.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct ForecastUdaf {
    signature: Signature,
}

impl ForecastUdaf {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Volatile),
        }
    }

    fn list_field() -> FieldRef {
        Arc::new(Field::new("item", DataType::Float64, true))
    }
}

impl AggregateUDFImpl for ForecastUdaf {
    fn name(&self) -> &'static str {
        "forecast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(Self::list_field()))
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(SeriesAccumulator::new(ForecastKind::Univariate)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        Ok(SeriesAccumulator::state_fields())
    }
}

// ── multivariate_forecast(target, predictor, timestamp, horizon) ────────

/// `multivariate_forecast(target, predictor, timestamp, horizon)` — `horizon`
/// predicted values of `target` from a multiple linear regression on
/// `predictor`, as a `LIST(DOUBLE)`.
///
/// Same shape and the same ordering contract as
/// [`ForecastUdaf`] — see there for why a forecast is an aggregate.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct MultivariateForecastUdaf {
    signature: Signature,
}

impl MultivariateForecastUdaf {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(4), Volatility::Volatile),
        }
    }
}

impl AggregateUDFImpl for MultivariateForecastUdaf {
    fn name(&self) -> &'static str {
        "multivariate_forecast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(ForecastUdaf::list_field()))
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(SeriesAccumulator::new(ForecastKind::Multivariate)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        Ok(SeriesAccumulator::state_fields())
    }
}

/// Which model the accumulated series is fed to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ForecastKind {
    Univariate,
    Multivariate,
}

/// Collects `(timestamp, value, covariate)` triples so the model can be fitted
/// on a time-ordered series.
///
/// The covariate column is unused for [`ForecastKind::Univariate`]; carrying
/// one accumulator for both keeps the partial/final merge logic in one place,
/// which is where an aggregate normally goes wrong.
#[derive(Debug)]
struct SeriesAccumulator {
    kind: ForecastKind,
    timestamps: Vec<i64>,
    values: Vec<f64>,
    covariates: Vec<f64>,
    horizon: usize,
}

impl SeriesAccumulator {
    fn new(kind: ForecastKind) -> Self {
        Self {
            kind,
            timestamps: Vec::new(),
            values: Vec::new(),
            covariates: Vec::new(),
            horizon: 0,
        }
    }

    fn state_fields() -> Vec<FieldRef> {
        let item = |name: &str| Arc::new(Field::new(name, DataType::Float64, true));
        vec![
            Arc::new(Field::new(
                "timestamps",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                false,
            )),
            Arc::new(Field::new("values", DataType::List(item("item")), false)),
            Arc::new(Field::new(
                "covariates",
                DataType::List(item("item")),
                false,
            )),
            Arc::new(Field::new("horizon", DataType::Int64, true)),
        ]
    }

    /// Column index of the horizon argument.
    fn horizon_index(&self) -> usize {
        match self.kind {
            ForecastKind::Univariate => 2,
            ForecastKind::Multivariate => 3,
        }
    }

    fn read_horizon(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let idx = self.horizon_index();
        let arr = values.get(idx).ok_or_else(|| {
            datafusion::common::DataFusionError::Internal("forecast: missing horizon".into())
        })?;
        let i = arr
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Plan(
                    "forecast: horizon must be an integer".into(),
                )
            })?;
        if i.is_empty() || i.is_null(0) {
            return Ok(());
        }
        let raw = i.value(0);
        self.horizon = usize::try_from(raw)
            .ok()
            .filter(|v| *v > 0)
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Plan(format!(
                    "forecast: horizon must be positive, got {raw}"
                ))
            })?;
        Ok(())
    }

    /// The series, sorted by timestamp with duplicates keeping the last value.
    fn ordered(&self) -> (Vec<i64>, Vec<f64>, Vec<f64>) {
        let mut idx: Vec<usize> = (0..self.timestamps.len()).collect();
        idx.sort_by_key(|&i| self.timestamps[i]);
        let mut ts = Vec::with_capacity(idx.len());
        let mut v = Vec::with_capacity(idx.len());
        let mut c = Vec::with_capacity(idx.len());
        for i in idx {
            if ts.last() == Some(&self.timestamps[i]) {
                // Last write wins, matching the read path's dedup.
                let n = v.len() - 1;
                v[n] = self.values[i];
                c[n] = self.covariates.get(i).copied().unwrap_or(f64::NAN);
                continue;
            }
            ts.push(self.timestamps[i]);
            v.push(self.values[i]);
            c.push(self.covariates.get(i).copied().unwrap_or(f64::NAN));
        }
        (ts, v, c)
    }

    fn forecast(&self) -> DFResult<Vec<f64>> {
        if self.horizon == 0 {
            return Err(datafusion::common::DataFusionError::Plan(
                "forecast: horizon must be positive".into(),
            ));
        }
        let (ts, values, covariates) = self.ordered();
        match self.kind {
            ForecastKind::Univariate => {
                use chronix_analytics::forecast::{ForecastModel, SesModel};
                if values.len() < 2 {
                    return Err(datafusion::common::DataFusionError::Plan(
                        "forecast: need at least 2 non-null rows".into(),
                    ));
                }
                let mut model = SesModel::new(None);
                model
                    .fit(&ts, &values)
                    .map_err(|e| datafusion::common::DataFusionError::Execution(e.to_string()))?;
                Ok(model
                    .predict(self.horizon)
                    .map_err(|e| datafusion::common::DataFusionError::Execution(e.to_string()))?
                    .values)
            }
            ForecastKind::Multivariate => {
                use chronix_analytics::multivariate::{
                    ColumnarMatrix, MultiLinearRegression, MultiSeriesContext,
                    MultivariateForecastModel,
                };
                if values.len() < 4 {
                    return Err(datafusion::common::DataFusionError::Plan(
                        "multivariate_forecast: need at least 4 non-null rows".into(),
                    ));
                }
                let ctx = MultiSeriesContext {
                    matrix: ColumnarMatrix {
                        data: vec![values, covariates],
                        timestamps: ts,
                        series_ids: vec!["target".into(), "predictor".into()],
                    },
                };
                let mut model = MultiLinearRegression::new();
                model
                    .fit(&ctx, 0)
                    .map_err(|e| datafusion::common::DataFusionError::Execution(e.to_string()))?;
                let result = model
                    .predict(self.horizon)
                    .map_err(|e| datafusion::common::DataFusionError::Execution(e.to_string()))?;
                Ok(result
                    .predictions
                    .first()
                    .cloned()
                    .unwrap_or_else(|| vec![f64::NAN; self.horizon]))
            }
        }
    }
}

/// Read a `Float64` column into a dense `Vec`, mapping NULL to `NaN`.
fn f64_values(arr: &ArrayRef) -> Vec<f64> {
    let f = arr.as_primitive::<Float64Type>();
    (0..f.len())
        .map(|i| if f.is_null(i) { f64::NAN } else { f.value(i) })
        .collect()
}

/// Flatten a `ListArray` state column back into a `Vec`.
fn flatten_list<T: arrow::array::ArrowPrimitiveType>(arr: &ArrayRef) -> Vec<T::Native> {
    let mut out = Vec::new();
    if let Some(list) = arr.as_any().downcast_ref::<arrow::array::ListArray>() {
        for i in 0..list.len() {
            if list.is_null(i) {
                continue;
            }
            let child = list.value(i);
            let prim = child.as_primitive::<T>();
            out.extend(prim.values().iter().copied());
        }
    }
    out
}

impl Accumulator for SeriesAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        if values.is_empty() {
            return Ok(());
        }
        self.read_horizon(values)?;

        let ts_index = self.horizon_index() - 1;
        let ts = extract_timestamps(&values[ts_index])?;
        let vals = f64_values(&values[0]);
        let covs = if self.kind == ForecastKind::Multivariate {
            f64_values(&values[1])
        } else {
            vec![f64::NAN; vals.len()]
        };

        for i in 0..ts.len() {
            if ts.is_null(i) || !vals[i].is_finite() {
                continue;
            }
            self.timestamps.push(ts.value(i));
            self.values.push(vals[i]);
            self.covariates.push(covs[i]);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let values = self.forecast()?;
        Ok(ScalarValue::List(ScalarValue::new_list_nullable(
            &values
                .into_iter()
                .map(ScalarValue::from)
                .collect::<Vec<_>>(),
            &DataType::Float64,
        )))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + (self.timestamps.len() + self.values.len() + self.covariates.len())
                * std::mem::size_of::<f64>()
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let ts = ScalarValue::new_list_nullable(
            &self
                .timestamps
                .iter()
                .map(|v| ScalarValue::Int64(Some(*v)))
                .collect::<Vec<_>>(),
            &DataType::Int64,
        );
        let make = |src: &[f64]| {
            ScalarValue::new_list_nullable(
                &src.iter()
                    .copied()
                    .map(ScalarValue::from)
                    .collect::<Vec<_>>(),
                &DataType::Float64,
            )
        };
        Ok(vec![
            ScalarValue::List(ts),
            ScalarValue::List(make(&self.values)),
            ScalarValue::List(make(&self.covariates)),
            #[allow(clippy::cast_possible_wrap)]
            ScalarValue::Int64(Some(self.horizon as i64)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        if states.len() < 4 {
            return Err(datafusion::common::DataFusionError::Internal(
                "forecast: malformed aggregate state".into(),
            ));
        }
        self.timestamps
            .extend(flatten_list::<arrow::datatypes::Int64Type>(&states[0]));
        self.values.extend(flatten_list::<Float64Type>(&states[1]));
        self.covariates
            .extend(flatten_list::<Float64Type>(&states[2]));
        if let Some(h) = states[3]
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
        {
            for i in 0..h.len() {
                if !h.is_null(i) {
                    self.horizon = self.horizon.max(usize::try_from(h.value(i)).unwrap_or(0));
                }
            }
        }
        Ok(())
    }
}
