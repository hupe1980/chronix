//! Shared utility functions for the analytics crate.

use std::collections::BTreeMap;

use chronix_core::FieldValue;

/// Extract the first numeric (f64) value from a field map.
///
/// Tries `field_name` first; if `None`, uses the first numeric field found.
/// Converts `I64` and `U64` to `f64`. Rejects `NaN` and `Infinity`.
pub(crate) fn extract_numeric_value(
    fields: &BTreeMap<String, FieldValue>,
    field_name: Option<&str>,
) -> Option<f64> {
    let v = if let Some(name) = field_name {
        fields.get(name).and_then(field_to_f64)
    } else {
        // Use the first numeric field
        fields.values().find_map(field_to_f64)
    };
    v.filter(|v| v.is_finite())
}

/// Convert a `FieldValue` to `f64` if numeric.
///
/// A decimal comes through, and comes through lossily: every model in this
/// crate — forecasting, anomaly detection, seasonality — is defined over
/// floating point and produces an estimate, so converting is what an answer
/// here *means*. The exactness that matters is on the storage and
/// aggregation paths, which never come this way.
pub(crate) fn field_to_f64(fv: &FieldValue) -> Option<f64> {
    fv.as_f64_lossy()
}

/// Build a deterministic, collision-free canonical key for a tags map.
///
/// Because `BTreeMap` iterates in sorted order the output is stable.
/// Uses a `key=value,` format — no hashing, no collisions.
pub(crate) fn compute_tags_key(tags: &BTreeMap<String, String>) -> String {
    use std::fmt::Write;
    let mut key = String::new();
    for (k, v) in tags {
        let _ = write!(key, "{k}={v},");
    }
    key
}
