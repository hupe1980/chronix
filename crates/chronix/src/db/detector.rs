//! Declared detectors: declare, drop, list and advance.
//!
//! The shape is a rollup's. A declaration and its state live in the catalog
//! as a [`DeclaredKind::Detector`] record; the maintenance pass advances
//! every declaration after the rollups; verdicts are ordinary points written
//! through the normal write path; and the state is persisted **after** the
//! verdicts are durable, never before — a state that outlives its verdicts
//! would skip them for ever, exactly as a rollup watermark that outlives its
//! rows leaves a permanent hole.

use chronix_engine::index::catalog::DeclaredKind;

use crate::detector::{DetectorDeclaration, DetectorInfo, DetectorStateHead, DetectorStatus};
use crate::error::{DbError, Result};

impl super::Chronix {
    /// Declare a detector the store keeps and advances.
    ///
    /// Idempotent for an identical declaration, so a program can declare its
    /// monitors on every start. The source measurement does not have to
    /// exist yet: a gateway declares before its first write, and the
    /// detector reports itself inactive until data arrives.
    ///
    /// # Errors
    ///
    /// - [`DbError::InvalidRequest`] if the declaration breaks a rule, or the
    ///   source field is declared encrypted (a verdict carries the value).
    /// - [`DbError::Conflict`] if a *different* detector already has this
    ///   name — drop it first; a declaration is never silently redefined.
    #[cfg(feature = "analytics")]
    pub fn declare_detector(&self, declaration: DetectorDeclaration) -> Result<()> {
        self.ensure_writable("declaring a detector")?;
        self.check_open()?;
        declaration.validate()?;
        #[cfg(feature = "field-encryption")]
        self.refuse_encrypted_columns(
            [declaration.field.as_str()],
            &format!("declaring detector '{}'", declaration.name),
        )?;
        let bytes = postcard::to_stdvec(&declaration)
            .map_err(|e| DbError::Internal(format!("encoding detector: {e}")))?;

        let _pass = self.detector_pass.lock();
        let mut catalog = self.catalog.write();
        if let Some((_, existing)) = catalog
            .detectors()
            .find(|(name, _)| *name == declaration.name)
        {
            if existing.definition == bytes {
                return Ok(());
            }
            return Err(DbError::Conflict {
                kind: "detector",
                name: declaration.name,
            });
        }
        catalog
            .set_declared(DeclaredKind::Detector, &declaration.name, bytes)
            .map_err(|e| DbError::Internal(format!("persisting detector: {e}")))?;
        drop(catalog);
        self.detector_marks
            .entry(declaration.source_measurement.clone())
            .or_default()
            .push((declaration.name.clone(), None));
        tracing::info!(
            detector = %declaration.name,
            source = %declaration.source_measurement,
            "detector declared"
        );
        Ok(())
    }

    /// Remove a declared detector and its state. Its verdicts stay: they
    /// are data, and deleting data is a different operation.
    ///
    /// # Errors
    ///
    /// [`DbError::NotFound`] if no detector has this name.
    pub fn drop_detector(&self, name: &str) -> Result<()> {
        self.ensure_writable("dropping a detector")?;
        self.check_open()?;
        let _pass = self.detector_pass.lock();
        let removed = self
            .catalog
            .write()
            .remove_declared(DeclaredKind::Detector, name)
            .map_err(|e| DbError::Internal(format!("removing detector: {e}")))?;
        if !removed {
            return Err(DbError::NotFound {
                kind: "detector",
                name: name.to_string(),
            });
        }
        for mut marks in self.detector_marks.iter_mut() {
            marks.retain(|(n, _)| n != name);
        }
        self.detector_marks.retain(|_, marks| !marks.is_empty());
        Ok(())
    }

    /// Every declared detector, with its watermark and status.
    ///
    /// Available in every build. Without the `analytics` feature each one is
    /// [`DetectorStatus::Inactive`] — kept, listed, and untouched.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed or a record is unreadable.
    pub fn detectors(&self) -> Result<Vec<DetectorInfo>> {
        self.check_open()?;
        let records: Vec<(String, Vec<u8>, Vec<u8>)> = self
            .catalog
            .read()
            .detectors()
            .map(|(name, r)| (name.to_string(), r.definition.clone(), r.state.clone()))
            .collect();
        let mut out = Vec::with_capacity(records.len());
        for (name, definition, state) in records {
            let declaration: DetectorDeclaration = postcard::from_bytes(&definition)
                .map_err(|e| DbError::Internal(format!("unreadable detector {name}: {e}")))?;
            let head = DetectorStateHead::decode(&state)?;
            let status = self.detector_status(&declaration, &head);
            let warming_slots = Self::warming_slots(&declaration, &state);
            out.push(DetectorInfo {
                watermark_ns: head.watermark_ns,
                series: usize::try_from(head.series_count).unwrap_or(usize::MAX),
                late_points: head.late_points,
                warming_slots,
                status,
                declaration,
            });
        }
        Ok(out)
    }

    /// (series, slot) baselines that have seen data and not yet
    /// `min_samples` of it. A slot no data falls in — night, for a PV
    /// ratio — is unused, not warming.
    #[cfg(feature = "analytics")]
    fn warming_slots(declaration: &DetectorDeclaration, state: &[u8]) -> Option<u64> {
        if state.is_empty() {
            return Some(0);
        }
        let state: crate::detector::DetectorState = postcard::from_bytes(state).ok()?;
        let min = f64::from(declaration.min_samples);
        let warming = state
            .series
            .values()
            .flat_map(|b| &b.slots)
            .filter(|w| w.weight > 0.0 && w.weight < min)
            .count();
        Some(warming as u64)
    }

    #[cfg(not(feature = "analytics"))]
    fn warming_slots(_: &DetectorDeclaration, _: &[u8]) -> Option<u64> {
        None
    }

    fn detector_status(
        &self,
        declaration: &DetectorDeclaration,
        head: &DetectorStateHead,
    ) -> DetectorStatus {
        if !cfg!(feature = "analytics") {
            return DetectorStatus::Inactive {
                reason: "built without the analytics feature".into(),
            };
        }
        if self
            .schema
            .lookup(&declaration.source_measurement)
            .is_none()
        {
            return DetectorStatus::Inactive {
                reason: format!(
                    "source measurement '{}' has no data yet",
                    declaration.source_measurement
                ),
            };
        }
        match &head.degraded {
            Some(reason) => DetectorStatus::Degraded {
                reason: reason.clone(),
            },
            None => DetectorStatus::Active,
        }
    }

    /// Load the in-memory late-point marks from the catalog, at open.
    pub(super) fn load_detector_marks(&self) {
        let catalog = self.catalog.read();
        for (name, record) in catalog.detectors() {
            let Ok(declaration) = postcard::from_bytes::<DetectorDeclaration>(&record.definition)
            else {
                continue;
            };
            let watermark = DetectorStateHead::decode(&record.state)
                .ok()
                .and_then(|h| h.watermark_ns);
            self.detector_marks
                .entry(declaration.source_measurement)
                .or_default()
                .push((name.to_string(), watermark));
        }
    }

    /// Count admitted points that landed at or below a detector's watermark:
    /// they will never be judged, and saying so is the difference between a
    /// monitor that is quiet and one that did not look.
    pub(super) fn count_late_detector_points(&self, points: &[&chronix_core::Point]) {
        if self.detector_marks.is_empty() {
            return;
        }
        for point in points {
            let Some(marks) = self.detector_marks.get(point.series_key().measurement()) else {
                continue;
            };
            for (name, watermark) in marks.iter() {
                if watermark.is_some_and(|w| point.timestamp() <= w) {
                    metrics::counter!("chronix_detector_late_points_total", "detector" => name.clone())
                        .increment(1);
                    self.detector_late
                        .entry(name.clone())
                        .and_modify(|n| *n += 1)
                        .or_insert(1);
                }
            }
        }
    }

    /// Advance every declared detector over the source data that has
    /// settled since its watermark. Returns the number of verdicts written.
    ///
    /// Called by the maintenance thread after the rollups; callers that run
    /// without it (`maintenance_interval = 0`) call it themselves.
    ///
    /// # Errors
    ///
    /// Returns the first error; a detector that fails keeps its watermark,
    /// so nothing is skipped, and the others still advance.
    #[cfg(feature = "analytics")]
    pub fn advance_detectors(&self) -> Result<usize> {
        self.ensure_writable("advancing detectors")?;
        self.check_open()?;
        let _pass = self.detector_pass.lock();
        let records: Vec<(String, Vec<u8>, Vec<u8>)> = self
            .catalog
            .read()
            .detectors()
            .map(|(name, r)| (name.to_string(), r.definition.clone(), r.state.clone()))
            .collect();
        let mut written = 0usize;
        let mut first_error = None;
        for (name, definition, state) in records {
            match self.advance_one(&definition, &state) {
                Ok(n) => written += n,
                Err(e) => {
                    tracing::warn!(detector = %name, error = %e, "detector pass failed");
                    metrics::counter!("chronix_detector_failures_total").increment(1);
                    first_error.get_or_insert(e);
                }
            }
        }
        match first_error {
            Some(e) if written == 0 => Err(e),
            _ => Ok(written),
        }
    }

    #[cfg(feature = "analytics")]
    fn advance_one(&self, definition: &[u8], state: &[u8]) -> Result<usize> {
        use arrow::array::{Array, Float64Array, Int64Array, StringArray};
        use chronix_analytics::anomaly::ScoringRule;

        use crate::detector::DetectorState;

        let decl: DetectorDeclaration = postcard::from_bytes(definition)
            .map_err(|e| DbError::Internal(format!("unreadable detector: {e}")))?;
        let mut state: DetectorState = if state.is_empty() {
            DetectorState::default()
        } else {
            postcard::from_bytes(state)
                .map_err(|e| DbError::Internal(format!("unreadable detector state: {e}")))?
        };
        if self.schema.lookup(&decl.source_measurement).is_none() {
            return Ok(0);
        }
        #[cfg(feature = "field-encryption")]
        self.refuse_encrypted_columns(
            [decl.field.as_str()],
            &format!("advancing detector '{}'", decl.name),
        )?;

        let settle = decl.settle_ns.unwrap_or_else(|| {
            i64::try_from(self.config.maintenance_interval.as_nanos())
                .unwrap_or(i64::MAX)
                .saturating_mul(2)
        });
        let upper = Self::now_ns().saturating_sub(settle);
        let lower = state.watermark_ns.map_or(i64::MIN, |w| w.saturating_add(1));
        if lower > upper {
            return Ok(0);
        }

        let mut builder = self
            .query()
            .measurement(&decl.source_measurement)
            .range(lower, upper);
        for (k, v) in &decl.where_tags {
            builder = builder.tag(k.clone(), v.clone());
        }
        let plan = builder
            .build()
            .map_err(|e| DbError::Internal(format!("detector scan plan: {e}")))?;

        let rule = ScoringRule {
            zscore: decl.zscore,
            cusum: decl.cusum,
            min_samples: f64::from(decl.min_samples),
        };
        let budget = chronix_engine::index::catalog::MAX_DECLARED_STATE_BYTES;
        let cap = decl.max_points_per_pass as usize;

        let mut verdicts: Vec<chronix_core::Point> = Vec::new();
        // The verdict series of each group, built once per pass rather than
        // once per row: a `SeriesKey` validates and sorts its tags.
        let mut output_keys: std::collections::HashMap<String, chronix_core::SeriesKey> =
            std::collections::HashMap::new();
        let mut consumed = 0usize;
        let mut last_ts: Option<i64> = None;
        // Rough per-group cost, to keep the encoded state under the catalog
        // cap without encoding it after every new group.
        let mut approx_bytes = postcard::to_stdvec(&state).map(|b| b.len()).unwrap_or(0);

        'batches: for batch in self.execute_iter(&plan)?.without_deadline() {
            let batch = batch?;
            let schema = batch.schema();
            let Ok(ts_idx) = schema.index_of(chronix_core::TIME_COLUMN) else {
                continue;
            };
            let Some(ts_col) = batch.column(ts_idx).as_any().downcast_ref::<Int64Array>() else {
                continue;
            };
            let Ok(field_idx) = schema.index_of(&decl.field) else {
                continue;
            };
            let values = arrow::compute::cast(
                batch.column(field_idx),
                &arrow::datatypes::DataType::Float64,
            )
            .map_err(|e| {
                DbError::InvalidRequest(format!(
                    "detector '{}': field '{}' is not numeric: {e}",
                    decl.name, decl.field
                ))
            })?;
            let Some(values) = values.as_any().downcast_ref::<Float64Array>() else {
                continue;
            };
            let mut tag_cols: Vec<(String, &StringArray)> = schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| f.metadata().get("role").map(String::as_str) == Some("tag"))
                .filter(|(_, f)| {
                    decl.group_by_tags
                        .as_ref()
                        .is_none_or(|g| g.iter().any(|t| t == f.name()))
                })
                .filter_map(|(i, f)| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map(|a| (f.name().clone(), a))
                })
                .collect();
            // Sorted by name, so a group's key is canonical whatever order
            // the schema grew its tag columns in — otherwise a tag added
            // later would split one series' baseline in two.
            tag_cols.sort_by(|a, b| a.0.cmp(&b.0));

            for row in 0..batch.num_rows() {
                let ts = ts_col.value(row);
                // Stop only between timestamps, so every series at the
                // watermark's instant has been judged and none is left
                // behind as "late".
                if consumed >= cap && last_ts.is_some_and(|l| ts > l) {
                    break 'batches;
                }
                last_ts = Some(ts);
                consumed += 1;
                if values.is_null(row) {
                    continue;
                }
                let x = values.value(row);
                if !x.is_finite() {
                    continue;
                }
                let mut key = String::new();
                for (name, col) in &tag_cols {
                    if !col.is_null(row) && !col.value(row).is_empty() {
                        if !key.is_empty() {
                            key.push('\u{1f}');
                        }
                        key.push_str(name);
                        key.push('=');
                        key.push_str(col.value(row));
                    }
                }
                let baseline = match state.series.get_mut(&key) {
                    Some(b) => b,
                    None => {
                        // A new group costs its slots as it learns; refuse
                        // it up front when the state is near the cap.
                        let slots = decl
                            .season
                            .as_ref()
                            .map_or(1, crate::detector::Season::slots);
                        let estimate = key.len() + 40 + slots * 32;
                        if approx_bytes + estimate > budget {
                            state.degraded = Some(format!(
                                "state budget ({budget} bytes) spent: series beyond it are not \
                                 scored — narrow group_by or use a wider slot"
                            ));
                            continue;
                        }
                        approx_bytes += estimate;
                        state.series.entry(key.clone()).or_default()
                    }
                };
                let (slot, period_index) = decl.season.as_ref().map_or((0, 0), |s| s.slot_of(ts));
                let Some(v) = baseline.score(slot, period_index, x, &rule, decl.forgetting) else {
                    continue;
                };
                let series_key = match output_keys.get(&key) {
                    Some(k) => k.clone(),
                    None => {
                        let mut tags = std::collections::BTreeMap::new();
                        for (name, col) in &tag_cols {
                            if !col.is_null(row) && !col.value(row).is_empty() {
                                tags.insert(name.clone(), col.value(row).to_string());
                            }
                        }
                        tags.insert("detector".to_string(), decl.name.clone());
                        let k = chronix_core::SeriesKey::new(decl.output_measurement.clone(), tags)
                            .map_err(|e| DbError::Internal(format!("verdict series: {e}")))?;
                        output_keys.insert(key.clone(), k.clone());
                        k
                    }
                };
                let mut fields = std::collections::BTreeMap::new();
                use chronix_core::FieldValue as F;
                fields.insert("value".to_string(), F::F64(x));
                fields.insert("baseline".to_string(), F::F64(v.baseline));
                fields.insert("stddev".to_string(), F::F64(v.stddev));
                fields.insert(
                    "z".to_string(),
                    F::F64(if v.z.is_finite() { v.z } else { 0.0 }),
                );
                fields.insert("cusum_pos".to_string(), F::F64(v.cusum.pos));
                fields.insert("cusum_neg".to_string(), F::F64(v.cusum.neg));
                fields.insert("anomaly".to_string(), F::Bool(v.is_anomaly()));
                fields.insert("reason".to_string(), F::String(v.reason().to_string()));
                // Which baseline judged it: a surprising verdict is traced to
                // its slot, and `slot = 56` reads as 14:00 in a quarter-hour day.
                fields.insert(
                    "slot".to_string(),
                    F::I64(i64::try_from(slot).unwrap_or(i64::MAX)),
                );
                let point = chronix_core::Point::new(series_key, fields, ts)
                    .map_err(|e| DbError::Internal(format!("verdict point: {e}")))?;
                verdicts.push(point);
            }
        }

        let written = verdicts.len();
        if !verdicts.is_empty() {
            for chunk in verdicts.chunks(8_192) {
                self.backfill(chunk)?.into_complete()?;
            }
        }
        // The watermark is the newest timestamp *judged* — anchored on the
        // data, not on the clock. Moving it to the settled horizon when the
        // source was idle would make a device's buffered week, written when
        // it comes back online, entirely "late": never judged. An idle source
        // costs one empty range read per pass instead.
        if last_ts.is_some() {
            state.watermark_ns = last_ts;
        }
        state.series_count = state.series.len() as u64;
        if let Some((_, late)) = self.detector_late.remove(&decl.name) {
            state.late_points = state.late_points.saturating_add(late);
        }
        let bytes = postcard::to_stdvec(&state)
            .map_err(|e| DbError::Internal(format!("encoding detector state: {e}")))?;
        // After the verdicts: a state that outlived its verdicts would skip
        // them for ever.
        self.catalog
            .write()
            .set_declared_state(DeclaredKind::Detector, &decl.name, bytes)
            .map_err(|e| DbError::Internal(format!("persisting detector state: {e}")))?;
        if let Some(mut marks) = self.detector_marks.get_mut(&decl.source_measurement) {
            for (n, w) in marks.iter_mut() {
                if *n == decl.name {
                    *w = state.watermark_ns;
                }
            }
        }
        metrics::counter!("chronix_detector_verdicts_total", "detector" => decl.name.clone())
            .increment(written as u64);
        Ok(written)
    }
}
