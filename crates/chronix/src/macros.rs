//! Convenience macros for building tags and fields.

/// Build a `BTreeMap<String, String>` of tags.
///
/// # Example
///
/// ```no_run
/// use chronix::tags;
///
/// let tags = tags! {
///     "host" => "server-01",
///     "region" => "us-east",
/// };
/// assert_eq!(tags.len(), 2);
/// ```
#[macro_export]
macro_rules! tags {
    () => {{
        ::std::collections::BTreeMap::<String, String>::new()
    }};
    ($($key:expr => $val:expr),+ $(,)?) => {{
        let mut map = ::std::collections::BTreeMap::<String, String>::new();
        $(
            map.insert($key.into(), $val.into());
        )+
        map
    }};
}

/// Build a `BTreeMap<String, FieldValue>` of fields.
///
/// Automatically infers the `FieldValue` variant from the literal type:
/// - `f64` literals → `FieldValue::F64`
/// - `i64` literals → `FieldValue::I64`
/// - `u64` literals → `FieldValue::U64`
/// - `bool` → `FieldValue::Bool`
/// - `&str` / `String` → `FieldValue::Str`
///
/// # Example
///
/// ```no_run
/// use chronix::fields;
/// use chronix::prelude::FieldValue;
///
/// let fields = fields! {
///     "usage_idle" => 95.5_f64,
///     "count" => 42_i64,
/// };
/// assert_eq!(fields.len(), 2);
/// ```
#[macro_export]
macro_rules! fields {
    () => {{
        ::std::collections::BTreeMap::<String, $crate::prelude::FieldValue>::new()
    }};
    ($($key:expr => $val:expr),+ $(,)?) => {{
        let mut map = ::std::collections::BTreeMap::<String, $crate::prelude::FieldValue>::new();
        $(
            map.insert($key.into(), $crate::prelude::FieldValue::from($val));
        )+
        map
    }};
}

#[cfg(test)]
mod tests {
    use crate::prelude::FieldValue;

    #[test]
    fn tags_macro_builds_map() {
        let tags = tags! {
            "host" => "srv-1",
            "region" => "us-east",
        };
        assert_eq!(tags.len(), 2);
        assert_eq!(tags.get("host").unwrap(), "srv-1");
    }

    #[test]
    fn tags_macro_empty() {
        let tags = tags! {};
        assert!(tags.is_empty());
    }

    #[test]
    fn fields_macro_f64() {
        let fields = fields! {
            "cpu" => 95.5_f64,
        };
        assert_eq!(fields.len(), 1);
        assert!(
            matches!(fields.get("cpu").unwrap(), FieldValue::F64(v) if (*v - 95.5).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn fields_macro_i64() {
        let fields = fields! {
            "count" => 42_i64,
        };
        assert!(matches!(fields.get("count").unwrap(), FieldValue::I64(42)));
    }

    #[test]
    fn fields_macro_mixed() {
        let fields = fields! {
            "cpu" => 95.5_f64,
            "count" => 42_i64,
        };
        assert_eq!(fields.len(), 2);
    }

    #[test]
    fn fields_macro_empty() {
        let fields = fields! {};
        assert!(fields.is_empty());
    }
}
