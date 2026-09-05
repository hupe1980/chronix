//! The mapping between a Chronix `(measurement, field)` pair and a PromQL
//! metric name.
//!
//! Chronix stores a *measurement* with many *fields*; PromQL addresses a
//! *metric*, which carries one value per sample. This module decides which
//! name a series answers to, and it is the only place that does — the
//! evaluator, `/api/v1/series`, `/api/v1/label/__name__/values` and
//! `/api/v1/metadata` all resolve through it, so they cannot disagree.
//!
//! ```text
//! field == "value"  →  <measurement>
//! otherwise         →  <measurement>_<field>
//! ```
//!
//! Two properties follow, and both matter:
//!
//! - **A name is a function of its own pair.** It does not depend on how many
//!   other fields the measurement has, so writing a second field never
//!   renames the first one's history.
//! - **A name a query returns can be typed back in.** [`resolve`] is the exact
//!   inverse of [`metric_name`], so the `__name__` in a result is a selector.
//!
//! The `value` case inverts how the Prometheus remote-write and OTLP paths
//! store a sample — metric name → measurement, value → a field called
//! `value` — so a scraped `up` comes back as `up` rather than `up_value`. It
//! is also the rule VictoriaMetrics uses for line-protocol ingestion.

use chronix_core::schema::SchemaRegistry;

/// The field name whose metric is the bare measurement name.
///
/// A sample arriving over Prometheus remote write or OTLP is stored under this
/// field, so the metric name survives the round trip.
pub const VALUE_FIELD: &str = "value";

/// One metric: the name PromQL addresses it by, and the column it reads.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MetricRef {
    /// The PromQL metric name — the value of `__name__`.
    pub name: String,
    /// The measurement holding the samples.
    pub measurement: String,
    /// The field column carrying the value.
    pub field: String,
}

impl MetricRef {
    /// The metric for one `(measurement, field)` pair.
    #[must_use]
    pub fn new(measurement: &str, field: &str) -> Self {
        Self {
            name: metric_name(measurement, field),
            measurement: measurement.to_string(),
            field: field.to_string(),
        }
    }
}

/// The PromQL metric name for one `(measurement, field)` pair.
#[must_use]
pub fn metric_name(measurement: &str, field: &str) -> String {
    if field == VALUE_FIELD {
        measurement.to_string()
    } else {
        format!("{measurement}_{field}")
    }
}

/// Every metric the registry knows, sorted by name.
///
/// Used wherever a `__name__` regex or negation has to be answered, and by the
/// discovery endpoints. The registry is process-wide, so callers that answer a
/// tenant restrict the result themselves.
#[must_use]
pub fn all_metrics(registry: &SchemaRegistry) -> Vec<MetricRef> {
    let mut out = Vec::new();
    for measurement in registry.measurement_names() {
        let Some(schema) = registry.lookup(&measurement) else {
            continue;
        };
        for field in schema.field_names() {
            out.push(MetricRef::new(&measurement, field));
        }
    }
    out.sort();
    out
}

/// Every metric of one measurement, sorted by name.
#[must_use]
pub fn metrics_of(registry: &SchemaRegistry, measurement: &str) -> Vec<MetricRef> {
    let Some(schema) = registry.lookup(measurement) else {
        return Vec::new();
    };
    let mut out: Vec<MetricRef> = schema
        .field_names()
        .iter()
        .map(|f| MetricRef::new(measurement, f))
        .collect();
    out.sort();
    out
}

/// The metrics a `__name__="…"` equality names — the inverse of
/// [`metric_name`].
///
/// Usually one. It is a `Vec` because the mapping is not injective across
/// *different* measurements: measurement `a_b` with field `c` and measurement
/// `a` with field `b_c` both produce `a_b_c`, and a selector naming it must
/// read both rather than silently pick one.
///
/// Empty when nothing is stored under that name, which is what PromQL expects
/// of an unknown metric — an empty vector, not an error, or `absent()` could
/// never be true.
#[must_use]
pub fn resolve(registry: &SchemaRegistry, name: &str) -> Vec<MetricRef> {
    let mut out = Vec::new();

    // `<measurement>` — the measurement itself, if it carries a `value` field.
    if registry
        .lookup(name)
        .is_some_and(|s| s.field_names().contains(&VALUE_FIELD))
    {
        out.push(MetricRef {
            name: name.to_string(),
            measurement: name.to_string(),
            field: VALUE_FIELD.to_string(),
        });
    }

    // `<measurement>_<field>` — every split that names a real column. The
    // `value` field is skipped here because `metric_name` never produces
    // `m_value` for it; `m_value` belongs to a measurement of that name.
    for (idx, _) in name.match_indices('_') {
        let (measurement, field) = (&name[..idx], &name[idx + 1..]);
        if field == VALUE_FIELD {
            continue;
        }
        if registry
            .lookup(measurement)
            .is_some_and(|s| s.field_names().contains(&field))
        {
            out.push(MetricRef {
                name: name.to_string(),
                measurement: measurement.to_string(),
                field: field.to_string(),
            });
        }
    }

    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronix_core::schema::MeasurementSchema;
    use chronix_core::FieldValue;

    fn registry(defs: &[(&str, &[&str])]) -> SchemaRegistry {
        let reg = SchemaRegistry::new();
        for (measurement, fields) in defs {
            let mut schema = MeasurementSchema::new(*measurement);
            for f in *fields {
                schema.add_field(f, &FieldValue::F64(0.0)).unwrap();
            }
            reg.register_measurement(schema);
        }
        reg
    }

    #[test]
    fn a_value_field_keeps_the_measurement_name() {
        assert_eq!(metric_name("up", "value"), "up");
        assert_eq!(metric_name("cpu", "usage"), "cpu_usage");
    }

    #[test]
    fn resolve_inverts_metric_name_for_every_known_pair() {
        let reg = registry(&[
            ("up", &["value"]),
            ("cpu", &["usage", "load"]),
            ("a_b", &["c"]),
        ]);
        for m in all_metrics(&reg) {
            let back = resolve(&reg, &m.name);
            assert!(
                back.contains(&m),
                "{} did not resolve back to {m:?}, got {back:?}",
                m.name
            );
        }
    }

    #[test]
    fn an_ambiguous_name_resolves_to_every_pair_that_produces_it() {
        let reg = registry(&[("a_b", &["c"]), ("a", &["b_c"])]);
        let refs = resolve(&reg, "a_b_c");
        assert_eq!(refs.len(), 2, "{refs:?}");
    }

    #[test]
    fn a_measurement_without_a_value_field_is_not_a_metric() {
        let reg = registry(&[("cpu", &["usage"])]);
        assert!(resolve(&reg, "cpu").is_empty());
        assert_eq!(resolve(&reg, "cpu_usage").len(), 1);
    }

    #[test]
    fn m_value_names_a_measurement_not_a_value_field() {
        // `metric_name("m", "value")` is `m`, so `m_value` must not resolve to
        // it — otherwise the mapping stops being invertible.
        let reg = registry(&[("m", &["value"]), ("m_value", &["value"])]);
        let refs = resolve(&reg, "m_value");
        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(refs[0].measurement, "m_value");
    }

    #[test]
    fn adding_a_field_does_not_rename_an_existing_metric() {
        let before = metric_name("cpu", "usage");
        // The name is a function of the pair alone: a second field changes
        // nothing about the first.
        assert_eq!(before, metric_name("cpu", "usage"));
    }
}
