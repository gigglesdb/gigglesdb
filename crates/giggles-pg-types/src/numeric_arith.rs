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
