use super::{MappingError, invalid};
use serde::{Deserialize, Serialize};

/// Decimal coefficient and display scale. Leading coefficient zeros and negative
/// zero are normalized; trailing fractional zeros are deliberately retained.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Numeric(Repr);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Repr {
    Finite {
        negative: bool,
        digits: String,
        scale: u32,
    },
    NaN,
    PositiveInfinity,
    NegativeInfinity,
}

impl Numeric {
    pub const NAN: Self = Self(Repr::NaN);
    pub const POSITIVE_INFINITY: Self = Self(Repr::PositiveInfinity);
    pub const NEGATIVE_INFINITY: Self = Self(Repr::NegativeInfinity);

    /// Sign, normalized coefficient digits and preserved display scale.
    pub fn finite_parts(&self) -> Option<(bool, &str, u32)> {
        match &self.0 {
            Repr::Finite {
                negative,
                digits,
                scale,
            } => Some((*negative, digits, *scale)),
            _ => None,
        }
    }

    pub fn finite(negative: bool, digits: &str, scale: u32) -> Result<Self, MappingError> {
        if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_digit()) {
            return Err(invalid("numeric coefficient"));
        }
        let digits = digits.trim_start_matches('0');
        let digits = if digits.is_empty() { "0" } else { digits };
        let result = Self(Repr::Finite {
            negative: negative && digits != "0",
            digits: digits.into(),
            scale,
        });
        result.validate()?;
        Ok(result)
    }

    pub fn parse(text: &str) -> Result<Self, MappingError> {
        let text = text.trim();
        if text.eq_ignore_ascii_case("nan") {
            return Ok(Self(Repr::NaN));
        }
        if text.eq_ignore_ascii_case("infinity") || text.eq_ignore_ascii_case("+infinity") {
            return Ok(Self(Repr::PositiveInfinity));
        }
        if text.eq_ignore_ascii_case("-infinity") {
            return Ok(Self(Repr::NegativeInfinity));
        }
        let (negative, body) = match text.as_bytes().first() {
            Some(b'-') => (true, &text[1..]),
            Some(b'+') => (false, &text[1..]),
            _ => (false, text),
        };
        let mut exponent = body.split(['e', 'E']);
        let body = exponent.next().unwrap_or_default();
        let exp: i32 = exponent.next().map_or(Ok(0), |s| {
            s.parse().map_err(|_| invalid("numeric exponent"))
        })?;
        if exponent.next().is_some() {
            return Err(invalid("numeric exponent"));
        }
        let mut parts = body.split('.');
        let whole = parts.next().unwrap_or_default();
        let fraction = parts.next().unwrap_or_default();
        if parts.next().is_some()
            || whole.is_empty() && fraction.is_empty()
            || !whole
                .bytes()
                .chain(fraction.bytes())
                .all(|c| c.is_ascii_digit())
        {
            return Err(invalid("numeric digits"));
        }
        let scale =
            i64::try_from(fraction.len()).map_err(|_| invalid("numeric size"))? - i64::from(exp);
        // PG numeric bounds also prevent hostile exponent-sized allocations.
        if !(-131_072..=16_383).contains(&scale) {
            return Err(invalid("numeric scale"));
        }
        let mut digits = format!("{whole}{fraction}");
        if scale < 0 {
            digits.extend(std::iter::repeat_n('0', (-scale) as usize));
        }
        Self::finite(negative, &digits, scale.max(0) as u32)
    }

    pub fn scale(&self) -> Option<u32> {
        match self.0 {
            Repr::Finite { scale, .. } => Some(scale),
            _ => None,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), MappingError> {
        if let Repr::Finite {
            negative,
            digits,
            scale,
        } = &self.0
            && (digits.is_empty()
                || !digits.bytes().all(|c| c.is_ascii_digit())
                || digits.len() > 1 && digits.starts_with('0')
                || *negative && digits == "0"
                || *scale > 16_383
                || digits.len().saturating_sub(*scale as usize) > 131_072)
        {
            return Err(invalid("noncanonical or out-of-range numeric"));
        }
        Ok(())
    }

    pub(crate) fn normalized_json(&self) -> Self {
        let mut result = self.clone();
        if let Repr::Finite { digits, scale, .. } = &mut result.0 {
            while *scale > 0 && digits.ends_with('0') && digits != "0" {
                digits.pop();
                *scale -= 1;
            }
            if digits == "0" {
                *scale = 0;
            }
        }
        result
    }
}

impl std::fmt::Display for Numeric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Repr::NaN => f.write_str("NaN"),
            Repr::PositiveInfinity => f.write_str("Infinity"),
            Repr::NegativeInfinity => f.write_str("-Infinity"),
            Repr::Finite {
                negative,
                digits,
                scale,
            } => {
                if *negative {
                    f.write_str("-")?;
                }
                let scale = *scale as usize;
                if scale == 0 {
                    return f.write_str(digits);
                }
                if digits.len() > scale {
                    let cut = digits.len() - scale;
                    write!(f, "{}.{}", &digits[..cut], &digits[cut..])
                } else {
                    f.write_str("0.")?;
                    for _ in 0..scale - digits.len() {
                        f.write_str("0")?;
                    }
                    f.write_str(digits)
                }
            }
        }
    }
}
