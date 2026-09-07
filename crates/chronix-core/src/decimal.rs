//! Exact fixed-point decimals.
//!
//! [`Decimal`] is `mantissa × 10⁻ˢᶜᵃˡᵉ` — an `i128` mantissa and a `u8`
//! scale, which is `rust_decimal`'s representation and PostgreSQL
//! `NUMERIC`'s, so a round trip through either is lossless and needs no
//! dependency on either.
//!
//! # Why a database of metrics needs one
//!
//! Every other field type in Chronix answers *how much*, approximately: a
//! temperature, a load average, a byte count. [`FieldValue::F64`](crate::FieldValue::F64) is the
//! right shape for those, and the compression stack is built around the
//! observation that most of them started life as decimals.
//!
//! A quarter-hour meter register is a different kind of quantity. It is a
//! *legal* one: a German household settlement under BK 618-25-02 (MiSpeL) is
//! computed from `Z1NB¼`, `Z2V¼` and their siblings, and a settlement that
//! went through a `double` is a settlement nobody can reproduce. The same is
//! true of a price, an invoice line, a tariff step and a regulatory report.
//! For those, "close enough" is not a weaker answer — it is the wrong one.
//!
//! So the exactness is structural, not best-effort:
//!
//! - **No `f64` anywhere on the path.** There is no `From<f64>`. The only
//!   way in is [`Decimal::new`] from an integer mantissa or [`FromStr`] from
//!   the digits themselves; the only lossy way out is
//!   [`to_f64_lossy`](Decimal::to_f64_lossy), which says so in its name.
//! - **Rescaling is lossless or refused.** [`rescale`](Decimal::rescale)
//!   widens (`1.5` → `1.500`) and narrows only when the digits it drops are
//!   zeros; otherwise it is an error, never a rounding.
//! - **One scale per column.** A decimal field's scale is fixed when the
//!   column is created and every later write is rescaled to it — see
//!   [`ColumnType::Decimal`](crate::ColumnType::Decimal). A register's scale
//!   is a property of the series, not of a point.
//!
//! # Limits
//!
//! Scale ≤ [`MAX_DECIMAL_SCALE`] and |mantissa| ≤ [`MAX_DECIMAL_MANTISSA`],
//! i.e. 38 significant digits. Both come from Arrow `Decimal128(38, s)`,
//! which is what a decimal column becomes in a query result and therefore
//! what DataFusion aggregates exactly. A value that does not fit is rejected
//! at ingest rather than truncated.

use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Largest scale a [`Decimal`] may carry.
///
/// 38 is Arrow `Decimal128`'s maximum precision: a column is read back as
/// `Decimal128(38, scale)`, and a scale above the precision cannot be
/// represented there.
pub const MAX_DECIMAL_SCALE: u8 = 38;

/// The precision every decimal column declares in Arrow: `Decimal128(38, s)`.
///
/// Fixed rather than derived from the data, because the precision is part of
/// the Arrow schema and two segments of the same column must produce the
/// same schema for their batches to concatenate.
pub const DECIMAL_PRECISION: u8 = 38;

/// Largest absolute mantissa a [`Decimal`] may carry: `10³⁸ − 1`.
///
/// One less than `10^DECIMAL_PRECISION`, i.e. 38 significant digits. Arrow
/// rejects a `Decimal128(38, s)` value outside this range, so accepting one
/// at ingest would produce a point that can be written and never read.
pub const MAX_DECIMAL_MANTISSA: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

/// Powers of ten up to `10³⁸`, for scaling without repeated multiplication.
const POW10: [i128; 39] = {
    let mut table = [1i128; 39];
    let mut i = 1;
    while i < 39 {
        table[i] = table[i - 1] * 10;
        i += 1;
    }
    table
};

/// Ten raised to `exp`, or `None` when `exp > 38`.
///
/// Public because rescaling a mantissa is not only this module's job: the
/// query layer aligns two decimal columns, and the segment writer aligns a
/// value to its column. One table, so they cannot disagree.
#[inline]
#[must_use]
pub fn pow10(exp: u32) -> Option<i128> {
    POW10.get(exp as usize).copied()
}

/// Why a [`Decimal`] could not be constructed, parsed or rescaled.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecimalError {
    /// The scale exceeds [`MAX_DECIMAL_SCALE`].
    #[error("decimal scale {scale} exceeds the maximum of {max}")]
    ScaleTooLarge {
        /// The requested scale.
        scale: u32,
        /// [`MAX_DECIMAL_SCALE`].
        max: u8,
    },

    /// The mantissa needs more than [`DECIMAL_PRECISION`] significant digits.
    #[error("decimal mantissa {mantissa} exceeds {digits} significant digits")]
    PrecisionOverflow {
        /// The offending mantissa.
        mantissa: i128,
        /// [`DECIMAL_PRECISION`].
        digits: u8,
    },

    /// Narrowing the scale would have dropped a non-zero digit.
    #[error("cannot rescale {value} from scale {from} to scale {to} without losing digits")]
    InexactRescale {
        /// The value that could not be rescaled, rendered exactly.
        value: String,
        /// Its current scale.
        from: u8,
        /// The requested scale.
        to: u8,
    },

    /// An arithmetic result does not fit in 38 significant digits.
    #[error("decimal arithmetic overflowed 38 significant digits")]
    Overflow,

    /// The text is not a decimal literal.
    #[error("invalid decimal literal \"{input}\": {reason}")]
    Parse {
        /// The text that failed to parse.
        input: String,
        /// What was wrong with it.
        reason: &'static str,
    },
}

/// An exact fixed-point decimal: `mantissa × 10⁻ˢᶜᵃˡᵉ`.
///
/// See the [module documentation](self) for why the type exists and what it
/// guarantees. Construct with [`Decimal::new`] or by parsing the digits:
///
/// ```
/// use chronix_core::Decimal;
///
/// let register: Decimal = "1234.5678".parse().unwrap();
/// assert_eq!(register.mantissa(), 12_345_678);
/// assert_eq!(register.scale(), 4);
/// assert_eq!(register.to_string(), "1234.5678");
///
/// // Widening is exact; narrowing past a non-zero digit is refused.
/// assert_eq!(register.rescale(6).unwrap().to_string(), "1234.567800");
/// assert!(register.rescale(2).is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal {
    /// The unscaled integer value.
    mantissa: i128,
    /// The number of digits after the decimal point.
    scale: u8,
}

impl Decimal {
    /// Construct from an unscaled mantissa and a scale.
    ///
    /// # Errors
    ///
    /// [`DecimalError::ScaleTooLarge`] if `scale > MAX_DECIMAL_SCALE`, and
    /// [`DecimalError::PrecisionOverflow`] if the mantissa needs more than
    /// 38 significant digits.
    pub const fn new(mantissa: i128, scale: u8) -> Result<Self, DecimalError> {
        if scale > MAX_DECIMAL_SCALE {
            return Err(DecimalError::ScaleTooLarge {
                scale: scale as u32,
                max: MAX_DECIMAL_SCALE,
            });
        }
        if mantissa > MAX_DECIMAL_MANTISSA || mantissa < -MAX_DECIMAL_MANTISSA {
            return Err(DecimalError::PrecisionOverflow {
                mantissa,
                digits: DECIMAL_PRECISION,
            });
        }
        Ok(Self { mantissa, scale })
    }

    /// Zero at the given scale.
    ///
    /// # Errors
    ///
    /// [`DecimalError::ScaleTooLarge`] if `scale > MAX_DECIMAL_SCALE`.
    pub const fn zero(scale: u8) -> Result<Self, DecimalError> {
        Self::new(0, scale)
    }

    /// The unscaled integer value.
    #[inline]
    #[must_use]
    pub const fn mantissa(&self) -> i128 {
        self.mantissa
    }

    /// The number of digits after the decimal point.
    #[inline]
    #[must_use]
    pub const fn scale(&self) -> u8 {
        self.scale
    }

    /// `true` when the value is exactly zero, at any scale.
    #[inline]
    #[must_use]
    pub const fn is_zero(&self) -> bool {
        self.mantissa == 0
    }

    /// `-1`, `0` or `1` — the sign of the value.
    #[inline]
    #[must_use]
    pub const fn signum(&self) -> i8 {
        if self.mantissa > 0 {
            1
        } else if self.mantissa < 0 {
            -1
        } else {
            0
        }
    }

    /// The absolute value, at the same scale.
    ///
    /// Total: `|mantissa|` cannot overflow because the mantissa is bounded
    /// by [`MAX_DECIMAL_MANTISSA`], well inside `i128::MIN`.
    #[inline]
    #[must_use]
    pub const fn abs(&self) -> Self {
        Self {
            mantissa: self.mantissa.abs(),
            scale: self.scale,
        }
    }

    /// The negation, at the same scale.
    #[inline]
    #[must_use]
    pub const fn neg(&self) -> Self {
        Self {
            mantissa: -self.mantissa,
            scale: self.scale,
        }
    }

    /// Return the same value expressed at `target` scale.
    ///
    /// Widening multiplies by a power of ten and is always exact. Narrowing
    /// succeeds only when every digit it would drop is a zero — `1.500` to
    /// scale 1 is `1.5`, `1.55` to scale 1 is an error, never `1.6` and
    /// never `1.5`.
    ///
    /// # Errors
    ///
    /// - [`DecimalError::ScaleTooLarge`] if `target > MAX_DECIMAL_SCALE`.
    /// - [`DecimalError::InexactRescale`] if narrowing would drop a
    ///   non-zero digit.
    /// - [`DecimalError::PrecisionOverflow`] if widening pushes the mantissa
    ///   past 38 significant digits.
    pub fn rescale(&self, target: u8) -> Result<Self, DecimalError> {
        if target > MAX_DECIMAL_SCALE {
            return Err(DecimalError::ScaleTooLarge {
                scale: u32::from(target),
                max: MAX_DECIMAL_SCALE,
            });
        }
        match target.cmp(&self.scale) {
            Ordering::Equal => Ok(*self),
            Ordering::Greater => {
                let factor = pow10(u32::from(target - self.scale)).ok_or(DecimalError::Overflow)?;
                let mantissa = self
                    .mantissa
                    .checked_mul(factor)
                    .ok_or(DecimalError::Overflow)?;
                Self::new(mantissa, target)
            }
            Ordering::Less => {
                let factor = pow10(u32::from(self.scale - target)).ok_or(DecimalError::Overflow)?;
                if self.mantissa % factor != 0 {
                    return Err(DecimalError::InexactRescale {
                        value: self.to_string(),
                        from: self.scale,
                        to: target,
                    });
                }
                Self::new(self.mantissa / factor, target)
            }
        }
    }

    /// Drop trailing fractional zeros, down to `min_scale`.
    ///
    /// `1.2500` normalised to a minimum scale of 0 is `1.25`; to a minimum
    /// of 3 it is `1.250`. Never changes the value.
    #[must_use]
    pub fn normalized(&self, min_scale: u8) -> Self {
        let mut out = *self;
        while out.scale > min_scale && out.mantissa % 10 == 0 && out.mantissa != 0 {
            out.mantissa /= 10;
            out.scale -= 1;
        }
        // A zero mantissa carries no digits to strip; take it straight down.
        if out.mantissa == 0 && out.scale > min_scale {
            out.scale = min_scale;
        }
        out
    }

    /// Exact sum, at the wider of the two scales.
    ///
    /// # Errors
    ///
    /// [`DecimalError::Overflow`] if the result needs more than 38
    /// significant digits.
    pub fn checked_add(&self, other: &Self) -> Result<Self, DecimalError> {
        let scale = self.scale.max(other.scale);
        let a = self.rescale(scale)?;
        let b = other.rescale(scale)?;
        let mantissa = a
            .mantissa
            .checked_add(b.mantissa)
            .ok_or(DecimalError::Overflow)?;
        Self::new(mantissa, scale).map_err(|_| DecimalError::Overflow)
    }

    /// Exact difference, at the wider of the two scales.
    ///
    /// # Errors
    ///
    /// [`DecimalError::Overflow`] if the result needs more than 38
    /// significant digits.
    pub fn checked_sub(&self, other: &Self) -> Result<Self, DecimalError> {
        self.checked_add(&other.neg())
    }

    /// The value as an `f64` — **lossy**, and named so at every call site.
    ///
    /// The only conversion to binary floating point this type offers. A
    /// settlement quantity must not travel this way; a chart of one may.
    #[inline]
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn to_f64_lossy(&self) -> f64 {
        self.mantissa as f64 / pow10(u32::from(self.scale)).unwrap_or(1) as f64
    }

    /// Compare against another decimal of any scale, exactly.
    ///
    /// Aligning two mantissas at different scales can overflow `i128` —
    /// `10³⁷` at scale 0 against anything at scale 38 does. An overflow is
    /// not an error here but an answer: the side that cannot be represented
    /// at the common scale is the side with the larger magnitude, because
    /// the other one *is* representable there.
    #[must_use]
    pub fn cmp_exact(&self, other: &Self) -> Ordering {
        if self.scale == other.scale {
            return self.mantissa.cmp(&other.mantissa);
        }
        // Different signs (or a zero) decide it without any scaling.
        let sign = self.signum().cmp(&other.signum());
        if sign != Ordering::Equal {
            return sign;
        }
        let scale = self.scale.max(other.scale);
        match (self.rescale(scale), other.rescale(scale)) {
            (Ok(a), Ok(b)) => a.mantissa.cmp(&b.mantissa),
            // `self` overflowed the common scale and `other` did not, so
            // |self| > |other|; the sign then decides the direction.
            (Err(_), Ok(_)) => {
                if self.signum() >= 0 {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (Ok(_), Err(_)) => {
                if self.signum() >= 0 {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            // Both overflow: compare at the narrower scale instead, where at
            // least one of them is exact by construction.
            (Err(_), Err(_)) => {
                let narrow = self.scale.min(other.scale);
                let a = self.mantissa / pow10(u32::from(self.scale - narrow)).unwrap_or(1);
                let b = other.mantissa / pow10(u32::from(other.scale - narrow)).unwrap_or(1);
                a.cmp(&b)
            }
        }
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cmp_exact(other)
    }
}

impl fmt::Display for Decimal {
    /// Render the exact digits: no exponent, no rounding, no `f64`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.scale == 0 {
            return write!(f, "{}", self.mantissa);
        }
        let negative = self.mantissa < 0;
        // `unsigned_abs` rather than `abs`: the mantissa is bounded well
        // inside `i128::MIN`, but the unsigned form makes that irrelevant.
        let digits = self.mantissa.unsigned_abs().to_string();
        let scale = usize::from(self.scale);
        let (int_part, frac_part) = if digits.len() > scale {
            let split = digits.len() - scale;
            (digits[..split].to_string(), digits[split..].to_string())
        } else {
            ("0".to_string(), format!("{:0>scale$}", digits))
        };
        if negative {
            write!(f, "-")?;
        }
        write!(f, "{int_part}.{frac_part}")
    }
}

impl FromStr for Decimal {
    type Err = DecimalError;

    /// Parse the digits themselves — never via `f64`.
    ///
    /// Accepts an optional sign, digits with an optional fractional part,
    /// and an optional decimal exponent (`1.5e3`). The scale is the number
    /// of fractional digits less the exponent; a literal that lands on a
    /// scale above [`MAX_DECIMAL_SCALE`] is accepted only if the excess
    /// digits are trailing zeros that can be dropped without changing the
    /// value.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = |reason: &'static str| DecimalError::Parse {
            input: s.to_string(),
            reason,
        };
        if s.is_empty() {
            return Err(err("empty"));
        }

        let (mantissa_str, exponent) = match s.find(['e', 'E']) {
            Some(idx) => {
                let exp = s[idx + 1..]
                    .parse::<i32>()
                    .map_err(|_| err("malformed exponent"))?;
                if !(-1_000..=1_000).contains(&exp) {
                    return Err(err("exponent out of range"));
                }
                (&s[..idx], exp)
            }
            None => (s, 0),
        };

        let (negative, digits) = match mantissa_str.as_bytes().first() {
            Some(b'-') => (true, &mantissa_str[1..]),
            Some(b'+') => (false, &mantissa_str[1..]),
            _ => (false, mantissa_str),
        };
        if digits.is_empty() {
            return Err(err("no digits"));
        }

        let (int_digits, mut frac_digits) = match digits.find('.') {
            Some(idx) => {
                let frac = &digits[idx + 1..];
                if frac.contains('.') {
                    return Err(err("more than one decimal point"));
                }
                (&digits[..idx], frac)
            }
            None => (digits, ""),
        };
        if int_digits.is_empty() && frac_digits.is_empty() {
            return Err(err("no digits"));
        }

        // A literal with more fractional digits than the maximum scale is
        // not automatically out of range: `0.10000…0` carries no information
        // in its tail. Drop trailing zeros — and only zeros, and only as far
        // as the maximum scale — before the mantissa is accumulated, so a
        // representable value is not refused for an overflow it never had.
        while i64::try_from(frac_digits.len()).map_err(|_| err("too many fractional digits"))?
            - i64::from(exponent)
            > i64::from(MAX_DECIMAL_SCALE)
            && frac_digits.ends_with('0')
        {
            frac_digits = &frac_digits[..frac_digits.len() - 1];
        }

        let mut mantissa: i128 = 0;
        for part in [int_digits, frac_digits] {
            for byte in part.bytes() {
                let digit = match byte {
                    b'0'..=b'9' => i128::from(byte - b'0'),
                    _ => return Err(err("non-digit character")),
                };
                mantissa = mantissa
                    .checked_mul(10)
                    .and_then(|m| m.checked_add(digit))
                    .ok_or(DecimalError::PrecisionOverflow {
                        mantissa: i128::MAX,
                        digits: DECIMAL_PRECISION,
                    })?;
            }
        }
        if negative {
            mantissa = -mantissa;
        }

        // The literal's scale before any normalisation. `frac_digits.len()`
        // fits an i32 for any input a caller can hold in memory, but the
        // subtraction is checked anyway so a pathological literal is a
        // parse error rather than a panic.
        let raw_scale = i64::try_from(frac_digits.len())
            .map_err(|_| err("too many fractional digits"))?
            - i64::from(exponent);

        if raw_scale < 0 {
            // A negative scale means the value is an integer with trailing
            // zeros: fold them into the mantissa and land on scale 0.
            let shift = u32::try_from(-raw_scale).map_err(|_| err("exponent out of range"))?;
            let factor = pow10(shift).ok_or(DecimalError::Overflow)?;
            let mantissa = mantissa.checked_mul(factor).ok_or(DecimalError::Overflow)?;
            return Self::new(mantissa, 0);
        }

        if raw_scale > i64::from(MAX_DECIMAL_SCALE) {
            // Every trailing zero that could have been dropped already was,
            // so what is left is a digit that would have to be rounded away.
            return Err(DecimalError::ScaleTooLarge {
                scale: u32::try_from(raw_scale).unwrap_or(u32::MAX),
                max: MAX_DECIMAL_SCALE,
            });
        }

        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        Self::new(mantissa, raw_scale as u8)
    }
}

impl Serialize for Decimal {
    /// Exact in both directions: the digits as a string for JSON and any
    /// other human-readable format, the `(mantissa, scale)` pair for
    /// postcard and the rest of the binary ones.
    ///
    /// A decimal must never reach JSON as a JSON number: `serde_json`
    /// parses one back through `f64`, which is the loss this type exists to
    /// prevent.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            let repr = DecimalRepr {
                mantissa: self.mantissa,
                scale: self.scale,
            };
            repr.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for Decimal {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            let text = String::deserialize(deserializer)?;
            text.parse().map_err(serde::de::Error::custom)
        } else {
            let repr = DecimalRepr::deserialize(deserializer)?;
            Self::new(repr.mantissa, repr.scale).map_err(serde::de::Error::custom)
        }
    }
}

/// The binary serde shape: exactly the two fields, nothing derived.
#[derive(Serialize, Deserialize)]
struct DecimalRepr {
    mantissa: i128,
    scale: u8,
}

/// Conversions to and from `rust_decimal::Decimal`.
///
/// Enabled by the `rust_decimal` feature. Both directions are exact
/// wherever they are possible at all:
///
/// - **In**: `rust_decimal` is a 96-bit mantissa with scale `0..=28`, which
///   fits inside this type's 128 bits and scale `0..=38` with room to
///   spare. It cannot fail, and the `TryFrom` bound is there only because
///   the two ranges are not the same type.
/// - **Out**: a value with more than 96 bits of mantissa, or a scale above
///   28, has no `rust_decimal` representation. That is a genuine failure and
///   is reported rather than rounded.
///
/// Without the feature the conversion is two lines, because the
/// representation is the same on both sides:
///
/// ```ignore
/// let chronix = chronix_core::Decimal::new(d.mantissa(), d.scale() as u8)?;
/// let back = rust_decimal::Decimal::try_from_i128_with_scale(
///     chronix.mantissa(), u32::from(chronix.scale()))?;
/// ```
#[cfg(feature = "rust_decimal")]
mod rust_decimal_interop {
    use super::{Decimal, DecimalError, DECIMAL_PRECISION, MAX_DECIMAL_SCALE};

    impl TryFrom<rust_decimal::Decimal> for Decimal {
        type Error = DecimalError;

        fn try_from(value: rust_decimal::Decimal) -> Result<Self, Self::Error> {
            let scale = u8::try_from(value.scale()).map_err(|_| DecimalError::ScaleTooLarge {
                scale: value.scale(),
                max: MAX_DECIMAL_SCALE,
            })?;
            Self::new(value.mantissa(), scale)
        }
    }

    impl TryFrom<Decimal> for rust_decimal::Decimal {
        type Error = DecimalError;

        fn try_from(value: Decimal) -> Result<Self, Self::Error> {
            Self::try_from_i128_with_scale(value.mantissa(), u32::from(value.scale())).map_err(
                |_| DecimalError::PrecisionOverflow {
                    mantissa: value.mantissa(),
                    digits: DECIMAL_PRECISION,
                },
            )
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::str::FromStr;

        #[test]
        fn round_trip_through_rust_decimal() {
            for text in ["0", "231.45", "-231.45", "1234.5678", "0.0000000001"] {
                let theirs = rust_decimal::Decimal::from_str(text).unwrap();
                let ours = Decimal::try_from(theirs).unwrap();
                assert_eq!(ours.to_string(), text);
                let back = rust_decimal::Decimal::try_from(ours).unwrap();
                assert_eq!(back, theirs);
            }
        }

        #[test]
        fn a_value_too_wide_for_rust_decimal_is_refused_not_rounded() {
            // 38 digits: representable here, not in a 96-bit mantissa.
            let ours: Decimal = "99999999999999999999999999999999999999".parse().unwrap();
            assert!(rust_decimal::Decimal::try_from(ours).is_err());
        }

        #[test]
        fn a_scale_above_28_is_refused() {
            let ours = Decimal::new(1, 30).unwrap();
            assert!(rust_decimal::Decimal::try_from(ours).is_err());
        }
    }
}

impl TryFrom<i64> for Decimal {
    type Error = DecimalError;

    /// An integer is a decimal of scale 0.
    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Self::new(i128::from(value), 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_render_round_trip() {
        for text in [
            "0",
            "0.0",
            "231.45",
            "-231.45",
            "1234.5678",
            "-0.001",
            "0.00000000000000000001",
            "99999999999999999999999999999999999999",
        ] {
            let d: Decimal = text.parse().unwrap();
            assert_eq!(d.to_string(), text, "round trip of {text}");
        }
    }

    #[test]
    fn parse_keeps_every_digit_an_f64_would_lose() {
        // 0.1 + 0.2 in binary floating point is 0.30000000000000004; the
        // whole point of this type is that these digits survive.
        let d: Decimal = "0.30000000000000004".parse().unwrap();
        assert_eq!(d.mantissa(), 30_000_000_000_000_004);
        assert_eq!(d.scale(), 17);
    }

    #[test]
    fn parse_scale_and_sign() {
        let d: Decimal = "231.45".parse().unwrap();
        assert_eq!(d.mantissa(), 23_145);
        assert_eq!(d.scale(), 2);
        let d: Decimal = "-0.05".parse().unwrap();
        assert_eq!(d.mantissa(), -5);
        assert_eq!(d.scale(), 2);
        let d: Decimal = "+7".parse().unwrap();
        assert_eq!(d.mantissa(), 7);
        assert_eq!(d.scale(), 0);
    }

    #[test]
    fn parse_exponent() {
        assert_eq!("1.5e3".parse::<Decimal>().unwrap().to_string(), "1500");
        assert_eq!("15e-3".parse::<Decimal>().unwrap().to_string(), "0.015");
        assert_eq!("1E2".parse::<Decimal>().unwrap().to_string(), "100");
    }

    #[test]
    fn parse_rejects_junk() {
        for bad in [
            "", "-", ".", "1.2.3", "1,5", "abc", "1.2x", "1e", "1e999999",
        ] {
            assert!(bad.parse::<Decimal>().is_err(), "{bad} should not parse");
        }
    }

    #[test]
    fn parse_bare_fraction() {
        let d: Decimal = ".5".parse().unwrap();
        assert_eq!(d.to_string(), "0.5");
    }

    #[test]
    fn parse_overflow_is_rejected_not_wrapped() {
        let too_big = "1".repeat(40);
        assert!(too_big.parse::<Decimal>().is_err());
    }

    #[test]
    fn parse_strips_representable_trailing_zeros_beyond_max_scale() {
        // 40 fractional digits whose tail is zeros: representable at 38.
        let text = format!("0.{}", "1".to_string() + &"0".repeat(39));
        let d: Decimal = text.parse().unwrap();
        assert_eq!(d.scale(), MAX_DECIMAL_SCALE);
        assert_eq!(d.mantissa(), 10_i128.pow(37));
        // 41 fractional digits with a non-zero tail: not representable.
        let text = format!("0.{}1", "0".repeat(40));
        assert!(text.parse::<Decimal>().is_err());
    }

    #[test]
    fn rescale_widens_exactly_and_refuses_to_round() {
        let d: Decimal = "1.5".parse().unwrap();
        assert_eq!(d.rescale(4).unwrap().to_string(), "1.5000");
        assert_eq!(d.rescale(4).unwrap().rescale(1).unwrap(), d);
        let d: Decimal = "1.55".parse().unwrap();
        assert!(matches!(
            d.rescale(1),
            Err(DecimalError::InexactRescale { .. })
        ));
    }

    #[test]
    fn rescale_rejects_scale_above_max() {
        let d: Decimal = "1".parse().unwrap();
        assert!(matches!(
            d.rescale(MAX_DECIMAL_SCALE + 1),
            Err(DecimalError::ScaleTooLarge { .. })
        ));
    }

    #[test]
    fn new_rejects_out_of_range() {
        assert!(matches!(
            Decimal::new(1, MAX_DECIMAL_SCALE + 1),
            Err(DecimalError::ScaleTooLarge { .. })
        ));
        assert!(matches!(
            Decimal::new(MAX_DECIMAL_MANTISSA + 1, 0),
            Err(DecimalError::PrecisionOverflow { .. })
        ));
        assert!(matches!(
            Decimal::new(-MAX_DECIMAL_MANTISSA - 1, 0),
            Err(DecimalError::PrecisionOverflow { .. })
        ));
    }

    #[test]
    fn arithmetic_is_exact() {
        let a: Decimal = "0.1".parse().unwrap();
        let b: Decimal = "0.2".parse().unwrap();
        assert_eq!(a.checked_add(&b).unwrap().to_string(), "0.3");
        let a: Decimal = "1.5".parse().unwrap();
        let b: Decimal = "0.250".parse().unwrap();
        assert_eq!(a.checked_add(&b).unwrap().to_string(), "1.750");
        assert_eq!(a.checked_sub(&b).unwrap().to_string(), "1.250");
    }

    #[test]
    fn addition_overflow_is_an_error() {
        let a = Decimal::new(MAX_DECIMAL_MANTISSA, 0).unwrap();
        assert!(matches!(a.checked_add(&a), Err(DecimalError::Overflow)));
    }

    #[test]
    fn ordering_across_scales() {
        let a: Decimal = "1.5".parse().unwrap();
        let b: Decimal = "1.50".parse().unwrap();
        assert_eq!(a.cmp_exact(&b), Ordering::Equal);
        let c: Decimal = "1.51".parse().unwrap();
        assert_eq!(a.cmp_exact(&c), Ordering::Less);
        let neg: Decimal = "-1.5".parse().unwrap();
        assert_eq!(neg.cmp_exact(&a), Ordering::Less);
        assert_eq!(neg.cmp_exact(&neg), Ordering::Equal);
    }

    #[test]
    fn ordering_when_alignment_overflows() {
        // 10³⁷ at scale 0 cannot be expressed at scale 38, but it is still
        // the larger of the two.
        let huge = Decimal::new(POW10[37], 0).unwrap();
        let tiny = Decimal::new(1, MAX_DECIMAL_SCALE).unwrap();
        assert_eq!(huge.cmp_exact(&tiny), Ordering::Greater);
        assert_eq!(tiny.cmp_exact(&huge), Ordering::Less);
        let huge_neg = huge.neg();
        assert_eq!(huge_neg.cmp_exact(&tiny), Ordering::Less);
        assert_eq!(tiny.cmp_exact(&huge_neg), Ordering::Greater);
    }

    #[test]
    fn ordering_is_a_total_order_on_a_mixed_scale_sample() {
        let mut values: Vec<Decimal> = [
            "-2", "-1.5", "-1.50", "0", "0.000", "0.5", "1", "1.0000", "1.5", "2",
        ]
        .iter()
        .map(|s| s.parse().unwrap())
        .collect();
        values.sort();
        let rendered: Vec<String> = values.iter().map(ToString::to_string).collect();
        assert_eq!(
            rendered,
            ["-2", "-1.5", "-1.50", "0", "0.000", "0.5", "1", "1.0000", "1.5", "2"]
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn normalized_strips_only_trailing_zeros() {
        let d: Decimal = "1.2500".parse().unwrap();
        assert_eq!(d.normalized(0).to_string(), "1.25");
        assert_eq!(d.normalized(3).to_string(), "1.250");
        let z: Decimal = "0.0000".parse().unwrap();
        assert_eq!(z.normalized(0).to_string(), "0");
    }

    #[test]
    fn json_is_a_string_and_survives_the_round_trip() {
        let d: Decimal = "1234.5678".parse().unwrap();
        let json = serde_json::to_string(&d).unwrap();
        assert_eq!(json, "\"1234.5678\"");
        let back: Decimal = serde_json::from_str(&json).unwrap();
        assert_eq!(back, d);
    }

    #[test]
    fn postcard_round_trip_is_the_mantissa_and_scale() {
        let d: Decimal = "1234.5678".parse().unwrap();
        let bytes = postcard::to_stdvec(&d).unwrap();
        let back: Decimal = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back, d);
        assert_eq!(back.scale(), 4);
    }

    #[test]
    fn to_f64_lossy_is_the_only_way_out() {
        let d: Decimal = "231.45".parse().unwrap();
        assert!((d.to_f64_lossy() - 231.45).abs() < 1e-9);
    }

    #[test]
    fn signum_and_abs() {
        let d: Decimal = "-1.5".parse().unwrap();
        assert_eq!(d.signum(), -1);
        assert_eq!(d.abs().to_string(), "1.5");
        let z: Decimal = "0.00".parse().unwrap();
        assert_eq!(z.signum(), 0);
        assert!(z.is_zero());
    }
}
