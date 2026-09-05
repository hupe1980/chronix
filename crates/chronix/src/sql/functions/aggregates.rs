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
        Ok(RateAccumulator::state_fields())
    }
}

/// Streaming rate accumulator.
///
/// # Why this buffers instead of folding
///
/// `rate` is **order-dependent**: the counter-reset rule ("a drop means the
/// counter restarted, so the new value *is* the increase") is defined over
/// *adjacent* samples in time. Two earlier designs tried to summarise a
/// partition into a fixed-size state and both were wrong:
///
/// - folding each incoming state on arrival skipped the boundary between the
///   last partial of one `merge_batch` call and the first of the next, so
///   `rate()` came back low by exactly that increase;
/// - keeping the partials and concatenating them in `first_time` order is
///   correct only if the runs are **disjoint**. They are not. DataFusion puts
///   a `RoundRobinBatch(N)` repartition below the partial aggregate, so with
///   more than `N` scan batches a partition receives batches 0, N, 2N… — its
///   run spans the whole range with holes in it, and every other partition's
///   run overlaps it. Concatenating two interleaved runs reads the second
///   run's first value as a counter reset. Measured on a perfect 1 sample/s
///   counter over 40 batches: `rate()` returned **13.5** where the answer is
///   1.0.
///
/// No fixed-size summary can be right, because reconstructing adjacency needs
/// the samples. So the samples are kept and sorted once, at `evaluate` —
/// which is what the forecast aggregates in this module already do, and is
/// why `size()` reports the buffer to DataFusion's memory pool.
#[derive(Debug, Default)]
struct RateAccumulator {
    timestamps: Vec<i64>,
    values: Vec<f64>,
}

impl RateAccumulator {
    /// The state layout `rate` and any other sample-buffering aggregate share.
    fn state_fields() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new(
                "timestamps",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                false,
            )),
            Arc::new(Field::new(
                "values",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                false,
            )),
        ]
    }

    /// Samples in timestamp order, duplicates resolved last-write-wins to
    /// match the read path's dedup.
    fn ordered(&self) -> (Vec<i64>, Vec<f64>) {
        let mut idx: Vec<usize> = (0..self.timestamps.len()).collect();
        idx.sort_unstable_by_key(|&i| self.timestamps[i]);
        let mut ts = Vec::with_capacity(idx.len());
        let mut v = Vec::with_capacity(idx.len());
        for i in idx {
            if ts.last() == Some(&self.timestamps[i]) {
                let n = v.len() - 1;
                v[n] = self.values[i];
                continue;
            }
            ts.push(self.timestamps[i]);
            v.push(self.values[i]);
        }
        (ts, v)
    }

    /// Counter-reset-corrected total increase across the ordered samples.
    ///
    /// Prometheus's rule: a drop means the counter restarted, so the new value
    /// *is* the increase since the restart.
    fn increase(values: &[f64]) -> f64 {
        values
            .windows(2)
            .map(|w| {
                let delta = w[1] - w[0];
                if delta >= 0.0 {
                    delta
                } else {
                    w[1]
                }
            })
            .sum()
    }
}

impl Accumulator for RateAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let vals = as_f64_array(&values[0])?;
        let times = extract_timestamps(&values[1])?;

        self.timestamps.reserve(vals.len());
        self.values.reserve(vals.len());
        for i in 0..vals.len() {
            if vals.is_null(i) || times.is_null(i) {
                continue;
            }
            self.timestamps.push(times.value(i));
            self.values.push(vals.value(i));
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let (ts, values) = self.ordered();
        if values.len() < 2 {
            return Ok(ScalarValue::Float64(None));
        }
        let (Some(first), Some(last)) = (ts.first(), ts.last()) else {
            return Ok(ScalarValue::Float64(None));
        };
        if last <= first {
            return Ok(ScalarValue::Float64(None));
        }
        let dt_secs = (last - first) as f64 / 1e9;
        Ok(ScalarValue::Float64(Some(
            Self::increase(&values) / dt_secs,
        )))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.timestamps.capacity() * std::mem::size_of::<i64>()
            + self.values.capacity() * std::mem::size_of::<f64>()
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::List(ScalarValue::new_list_nullable(
                &self
                    .timestamps
                    .iter()
                    .map(|v| ScalarValue::Int64(Some(*v)))
                    .collect::<Vec<_>>(),
                &DataType::Int64,
            )),
            ScalarValue::List(ScalarValue::new_list_nullable(
                &self
                    .values
                    .iter()
                    .copied()
                    .map(ScalarValue::from)
                    .collect::<Vec<_>>(),
                &DataType::Float64,
            )),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        if states.len() < 2 {
            return Err(datafusion::error::DataFusionError::Internal(
                "rate: malformed aggregate state".to_string(),
            ));
        }
        self.timestamps
            .extend(flatten_list::<arrow::datatypes::Int64Type>(&states[0]));
        self.values.extend(flatten_list::<Float64Type>(&states[1]));
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
        let vals = as_f64_array(&values[0])?;
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
    /// `max_training_points` bounds the buffer a forecast model is fed, and
    /// keeps the **newest** points — the ones a forecast is about.
    ///
    /// Tested here rather than through a query because simple exponential
    /// smoothing converges to the recent level whatever history it is shown,
    /// so an end-to-end assertion on the forecast's *value* cannot tell a
    /// capped model from an uncapped one. The cap is a property of the
    /// buffer; this is where the buffer is.
    #[test]
    fn ordered_keeps_only_the_newest_training_points() {
        let limits = crate::sql::functions::ForecastLimits {
            max_horizon: 8760,
            max_training_points: 3,
        };
        let mut acc = super::SeriesAccumulator::new(super::ForecastKind::Univariate, limits);
        for i in 0..10i64 {
            acc.timestamps.push(i);
            acc.values.push(i as f64);
            acc.covariates.push(f64::NAN);
        }
        let (ts, v, _) = acc.ordered();
        assert_eq!(ts, vec![7, 8, 9]);
        assert_eq!(v, vec![7.0, 8.0, 9.0]);
    }

    /// A cap of zero means "no cap", so a configuration that does not set one
    /// does not silently truncate to nothing.
    #[test]
    fn a_zero_cap_keeps_everything() {
        let limits = crate::sql::functions::ForecastLimits {
            max_horizon: 8760,
            max_training_points: 0,
        };
        let mut acc = super::SeriesAccumulator::new(super::ForecastKind::Univariate, limits);
        for i in 0..5i64 {
            acc.timestamps.push(i);
            acc.values.push(i as f64);
            acc.covariates.push(f64::NAN);
        }
        assert_eq!(acc.ordered().0.len(), 5);
    }

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

    /// Build one partial-state row set for `RateAccumulator::merge_batch`:
    /// the samples of one run, as the list state the accumulator emits.
    fn rate_state(samples: &[(i64, f64)]) -> Vec<ArrayRef> {
        let mut acc = RateAccumulator::default();
        let ts = Arc::new(TimestampNanosecondArray::from(
            samples.iter().map(|(t, _)| *t).collect::<Vec<_>>(),
        )) as ArrayRef;
        let vals = Arc::new(Float64Array::from(
            samples.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
        )) as ArrayRef;
        acc.update_batch(&[vals, ts]).unwrap();
        acc.state()
            .unwrap()
            .into_iter()
            .map(|sv| sv.to_array().unwrap())
            .collect()
    }

    /// Partial states must combine to the same answer whatever order they
    /// arrive in, and whether or not their time ranges **overlap**.
    ///
    /// Overlap is the case that matters and the one two earlier designs got
    /// wrong. DataFusion puts a `RoundRobinBatch(N)` repartition below the
    /// partial aggregate, so a partition receives batches 0, N, 2N… — a run
    /// spanning the whole range with holes in it, overlapping every other
    /// partition's run. A summary state cannot be merged across that, because
    /// the counter-reset rule is defined over *adjacent* samples and adjacency
    /// is exactly what the interleaving destroys.
    #[test]
    fn rate_merges_across_separate_batches() {
        const SEC: i64 = 1_000_000_000;

        // A counter climbing by 10/s over 0..30 s: increase 300, rate 10/s.
        let all: Vec<(i64, f64)> = (0..=30).map(|i| (i * SEC, (i * 10) as f64)).collect();

        // Split three ways *round-robin*, so every run overlaps every other —
        // the shape the repartition actually produces.
        let interleaved: Vec<Vec<(i64, f64)>> = (0..3)
            .map(|p| {
                all.iter()
                    .enumerate()
                    .filter(|(i, _)| i % 3 == p)
                    .map(|(_, s)| *s)
                    .collect()
            })
            .collect();
        let runs: Vec<Vec<ArrayRef>> = interleaved.iter().map(|r| rate_state(r)).collect();

        let evaluate = |order: &[usize]| {
            let mut acc = RateAccumulator::default();
            for &i in order {
                acc.merge_batch(&runs[i]).unwrap();
            }
            acc.evaluate().unwrap()
        };

        let expected = evaluate(&[0, 1, 2]);
        assert_eq!(
            evaluate(&[2, 0, 1]),
            expected,
            "the merge must not depend on the order partial states arrive in"
        );

        match expected {
            ScalarValue::Float64(Some(v)) => assert!(
                (v - 10.0).abs() < 1e-9,
                "rate across three interleaved runs should be 10/s, got {v}"
            ),
            other => panic!("expected a rate, got {other:?}"),
        }

        // Contiguous, non-overlapping runs must still work — that is the
        // single-partition shape.
        let split: Vec<Vec<ArrayRef>> = all.chunks(11).map(|c| rate_state(c)).collect();
        let mut acc = RateAccumulator::default();
        for r in &split {
            acc.merge_batch(r).unwrap();
        }
        match acc.evaluate().unwrap() {
            ScalarValue::Float64(Some(v)) => assert!((v - 10.0).abs() < 1e-9, "got {v}"),
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
    limits: super::ForecastLimits,
}

impl ForecastUdaf {
    pub(super) fn new(limits: super::ForecastLimits) -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Volatile),
            limits,
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
        Ok(Box::new(SeriesAccumulator::new(
            ForecastKind::Univariate,
            self.limits,
        )))
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
    limits: super::ForecastLimits,
}

impl MultivariateForecastUdaf {
    pub(super) fn new(limits: super::ForecastLimits) -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(4), Volatility::Volatile),
            limits,
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
        Ok(Box::new(SeriesAccumulator::new(
            ForecastKind::Multivariate,
            self.limits,
        )))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        Ok(SeriesAccumulator::state_fields())
    }
}

// ── auto_forecast(value, timestamp, horizon) ────────────────────────────

/// `auto_forecast(value, timestamp, horizon)` — `horizon` predicted values
/// from the model that wins rolling-origin cross-validation at that
/// horizon, as a `LIST(DOUBLE)`.
///
/// Same shape as [`ForecastUdaf`]; where `forecast` assumes simple
/// exponential smoothing, this one races SES, Holt, damped Holt, linear
/// regression, ARIMA, Holt-Winters and SARIMA on walk-forward error and
/// fits the winner on everything — the same selection the embedded
/// `Chronix::auto_forecast` runs. It costs `candidates × folds` fits, so it
/// is the right call for a scheduled report and the wrong one for a
/// dashboard refresh.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct AutoForecastUdaf {
    signature: Signature,
    limits: super::ForecastLimits,
}

impl AutoForecastUdaf {
    pub(super) fn new(limits: super::ForecastLimits) -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Volatile),
            limits,
        }
    }
}

impl AggregateUDFImpl for AutoForecastUdaf {
    fn name(&self) -> &'static str {
        "auto_forecast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(ForecastUdaf::list_field()))
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(SeriesAccumulator::new(
            ForecastKind::Auto,
            self.limits,
        )))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        Ok(SeriesAccumulator::state_fields())
    }
}

/// Which model the accumulated series is fed to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ForecastKind {
    Univariate,
    Auto,
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
    limits: super::ForecastLimits,
}

impl SeriesAccumulator {
    fn new(kind: ForecastKind, limits: super::ForecastLimits) -> Self {
        Self {
            kind,
            timestamps: Vec::new(),
            values: Vec::new(),
            covariates: Vec::new(),
            horizon: 0,
            limits,
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
            ForecastKind::Univariate | ForecastKind::Auto => 2,
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
        let horizon = usize::try_from(raw)
            .ok()
            .filter(|v| *v > 0)
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Plan(format!(
                    "forecast: horizon must be positive, got {raw}"
                ))
            })?;
        // `analytics.max_forecast_horizon` was configuration nothing read, so
        // one cell could be asked for millions of values. Refused rather than
        // clamped: a silently shortened forecast is a wrong answer, and the
        // caller asked for a number.
        if horizon > self.limits.max_horizon {
            return Err(datafusion::common::DataFusionError::Plan(format!(
                "forecast: horizon {horizon} exceeds analytics.max_forecast_horizon \
                 ({}); raise it in the configuration or ask for fewer points",
                self.limits.max_horizon
            )));
        }
        self.horizon = horizon;
        Ok(())
    }

    /// The series, sorted by timestamp with duplicates keeping the last value,
    /// and truncated to the most recent `max_training_points`.
    ///
    /// The accumulator buffers its whole input — it has to, because an
    /// aggregate sees rows in the plan's order and a forecast needs them in
    /// time order — so an unbounded group is an unbounded allocation. The
    /// **newest** points are the ones a forecast wants, so the truncation
    /// takes the tail rather than refusing the query.
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
        let cap = self.limits.max_training_points;
        if cap > 0 && ts.len() > cap {
            let drop = ts.len() - cap;
            ts.drain(..drop);
            v.drain(..drop);
            c.drain(..drop);
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
            ForecastKind::Auto => {
                use chronix_analytics::forecast::{auto_forecast, AutoForecastOptions};
                if values.len() < 8 {
                    return Err(datafusion::common::DataFusionError::Plan(
                        "auto_forecast: need at least 8 non-null rows".into(),
                    ));
                }
                let chosen =
                    auto_forecast(&ts, &values, self.horizon, &AutoForecastOptions::default())
                        .map_err(|e| {
                            datafusion::common::DataFusionError::Execution(e.to_string())
                        })?;
                Ok(chosen.result.values)
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

/// A numeric column as `Float64`, casting when it is not already.
///
/// `as_primitive::<Float64Type>()` **panics** on any other type, and an
/// integer is exactly what somebody forecasts or rates — a counter. The panic
/// happened inside a `spawn_blocking`, so it surfaced as an opaque 500 with a
/// poisoned task rather than as an error naming the column.
///
/// Casting rather than refusing is the right answer here: every numeric column
/// this engine stores has an exact or near-exact `f64` image, and a forecast
/// over an integer counter is an ordinary thing to ask for. A non-numeric
/// column is refused by name.
///
/// # Errors
///
/// Returns a plan error if `arr` is not numeric, or execution error if the
/// cast fails.
fn as_f64_array(arr: &ArrayRef) -> DFResult<arrow::array::Float64Array> {
    use arrow::datatypes::DataType;

    if let Some(f) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
        return Ok(f.clone());
    }
    if !matches!(
        arr.data_type(),
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
    ) {
        return Err(datafusion::error::DataFusionError::Plan(format!(
            "expected a numeric column, found {:?}",
            arr.data_type()
        )));
    }
    let cast = arrow::compute::cast(arr, &DataType::Float64)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
    Ok(cast
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("cast to Float64 did not".into())
        })?
        .clone())
}

/// Read a numeric column into a dense `Vec`, mapping NULL to `NaN`.
fn f64_values(arr: &ArrayRef) -> DFResult<Vec<f64>> {
    let f = as_f64_array(arr)?;
    Ok((0..f.len())
        .map(|i| if f.is_null(i) { f64::NAN } else { f.value(i) })
        .collect())
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
        let vals = f64_values(&values[0])?;
        let covs = if self.kind == ForecastKind::Multivariate {
            f64_values(&values[1])?
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
