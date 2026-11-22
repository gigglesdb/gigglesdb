//! PostgreSQL operator arithmetic, separate from existing numeric function APIs.
use crate::{Error, Numeric, Repr, pow};
use num_traits::Zero;

impl Numeric {
    /// Exact a - trunc(a / b) * b with PostgreSQL remainder scale and specials.
    pub fn checked_rem_pg(&self, rhs: &Self) -> Result<Self, Error> {
        match (&self.0, &rhs.0) {
            (Repr::NaN, _) | (_, Repr::NaN) => Ok(Self(Repr::NaN)),
            (_, Repr::Finite { coefficient, .. }) if coefficient.is_zero() => {
                Err(Error::DivisionByZero)
            }
            (Repr::Infinity | Repr::NegInfinity, _) => Ok(Self(Repr::NaN)),
            (Repr::Finite { .. }, Repr::Infinity | Repr::NegInfinity) => Ok(self.clone()),
            (
                Repr::Finite {
                    coefficient: left,
                    scale: left_scale,
                },
                Repr::Finite {
                    coefficient: right,
                    scale: right_scale,
                },
            ) => {
                let scale = (*left_scale).max(*right_scale);
                let left = left * pow(scale - left_scale);
                let right = right * pow(scale - right_scale);
                // BigInt remainder truncates toward zero. No rounded division
                // result or bounded intermediate Numeric is constructed.
                Self::finite(left % right, scale)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn numeric(text: &str) -> Numeric {
        Numeric::parse(text).unwrap()
    }

    #[test]
    fn division_scale_uses_base_10000_first_digit() {
        for (left, right, expected) in [
            ("7", "2", "3.5000000000000000"),
            ("1", "2", "0.50000000000000000000"),
            ("10000", "2", "5000.0000000000000000"),
            ("2", "2", "1.00000000000000000000"),
            ("10000", "20000", "0.50000000000000000000"),
            ("-7", "2", "-3.5000000000000000"),
            (
                "1.0000000000000000000000000",
                "2",
                "0.5000000000000000000000000",
            ),
        ] {
            assert_eq!(
                numeric(left)
                    .checked_div(&numeric(right))
                    .unwrap()
                    .to_string(),
                expected
            );
        }
        assert_eq!(
            numeric("1e-1001")
                .checked_div(&numeric("1"))
                .unwrap()
                .scale(),
            Some(1000)
        );
        assert_eq!(
            numeric("1e100").checked_div(&numeric("1")).unwrap().scale(),
            Some(0)
        );
    }

    #[test]
    fn remainder_is_exact_scaled_and_truncates_toward_zero() {
        for (left, right, expected) in [
            ("-7.00", "3", "-1.00"),
            ("7", "-3.000", "1.000"),
            ("-7", "-3", "-1"),
            (
                "1.999999999999999999999999999999",
                "1",
                "0.999999999999999999999999999999",
            ),
            ("9999999999999999999999999999999999999999", "3", "0"),
        ] {
            assert_eq!(
                numeric(left)
                    .checked_rem_pg(&numeric(right))
                    .unwrap()
                    .to_string(),
                expected
            );
        }
    }

    #[test]
    fn arithmetic_special_values_and_zero() {
        let zero = numeric("0.00");
        for left in ["1", "Infinity", "-Infinity"] {
            assert_eq!(numeric(left).checked_div(&zero), Err(Error::DivisionByZero));
            assert_eq!(
                numeric(left).checked_rem_pg(&zero),
                Err(Error::DivisionByZero)
            );
        }
        assert_eq!(
            numeric("NaN").checked_rem_pg(&zero).unwrap().to_string(),
            "NaN"
        );
        assert_eq!(
            numeric("NaN").checked_div(&zero).unwrap().to_string(),
            "NaN"
        );
        assert_eq!(
            numeric("Infinity")
                .checked_rem_pg(&numeric("2"))
                .unwrap()
                .to_string(),
            "NaN"
        );
        assert_eq!(
            numeric("1.00")
                .checked_rem_pg(&numeric("Infinity"))
                .unwrap()
                .to_string(),
            "1.00"
        );
        assert_eq!(
            numeric("1.00")
                .checked_div(&numeric("Infinity"))
                .unwrap()
                .to_string(),
            "0"
        );
        assert_eq!(
            numeric("Infinity")
                .checked_div(&numeric("-2"))
                .unwrap()
                .to_string(),
            "-Infinity"
        );
        assert_eq!(
            numeric("Infinity")
                .checked_div(&numeric("Infinity"))
                .unwrap()
                .to_string(),
            "NaN"
        );
    }
}

/// A width_bucket operand can borrow a regular value or use a wider SQL literal.
pub enum BucketOperand<'a> {
    Numeric(&'a Numeric),
    Literal(&'a str),
}

#[derive(Debug)]
pub enum BucketError {
    Input(Error),
    InvalidCount,
    Nan,
    InfiniteBound,
    EqualBounds,
    BucketOverflow,
}

/// Compute floor(count * distance / span) with unbounded intermediates.
/// All inputs are bounded before expansion and the result fits in an integer.
pub fn numeric_width_bucket(
    [x, low, high]: [BucketOperand<'_>; 3],
    count: i32,
) -> Result<i32, BucketError> {
    use std::borrow::Cow;
    let parse = |value| match value {
        BucketOperand::Numeric(value) => Ok(Cow::Borrowed(value)),
        BucketOperand::Literal(value) => Numeric::parse_with_width(value, Some(131_072))
            .map(Cow::Owned)
            .map_err(BucketError::Input),
    };
    let (x, low, high) = (parse(x)?, parse(low)?, parse(high)?);
    if count <= 0 {
        return Err(BucketError::InvalidCount);
    }
    if [&x, &low, &high].iter().any(|v| matches!(v.0, Repr::NaN)) {
        return Err(BucketError::Nan);
    }
    let (Some(low_scale), Some(high_scale)) = (low.scale(), high.scale()) else {
        return Err(BucketError::InfiniteBound);
    };
    if low == high {
        return Err(BucketError::EqualBounds);
    }
    let descending = low > high;
    if (!descending && x < low) || (descending && x > low) {
        return Ok(0);
    }
    if (!descending && x >= high) || (descending && x <= high) {
        return count.checked_add(1).ok_or(BucketError::BucketOverflow);
    }
    let scale = low_scale.max(high_scale).max(x.scale().expect("finite in range"));
    let coefficient = |v: &Numeric| {
        let Repr::Finite { coefficient, scale: own } = &v.0 else {
            unreachable!("bounds and an in-range operand are finite");
        };
        if *own == scale {
            coefficient.clone()
        } else {
            coefficient * super::pow(scale - own)
        }
    };
    let (x, low, high) = (coefficient(&x), coefficient(&low), coefficient(&high));
    // In a descending range both differences are negative and cancel.
    let index = ((x - &low) * count) / (high - low);
    use num_traits::ToPrimitive;
    Ok(index.to_i32().expect("in-range bucket is below count") + 1)
}

#[cfg(test)]
#[test]
fn wide_bucket_literals_do_not_expand_the_numeric_storage_domain() {
    use BucketOperand::{Literal, Numeric as Number};
    let low = "-9e131071";
    let high = "9e131071";
    let one = Numeric::parse("1").unwrap();
    assert!(matches!(Numeric::parse(low), Err(Error::Limit)));
    assert!(matches!(Numeric::parse(high), Err(Error::Limit)));
    assert_eq!(numeric_width_bucket([Number(&one), Literal(low), Literal(high)], 4).unwrap(), 3);
    assert_eq!(numeric_width_bucket([Number(&one), Literal(high), Literal(low)], 4).unwrap(), 2);
    assert!(matches!(
        numeric_width_bucket([Number(&one), Literal(low), Literal("9e131072")], 4),
        Err(BucketError::Input(Error::Limit))
    ));
}
