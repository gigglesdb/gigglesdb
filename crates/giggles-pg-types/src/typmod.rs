//! PostgreSQL numeric modifiers (`numerictypmodin` and `apply_typmod`).
use crate::{Numeric, Repr};
use num_traits::{Signed, Zero};
use std::fmt;

pub const VARHDRSZ: i32 = 4;
pub const NUMERIC_MAX_PRECISION: i32 = 1000;
pub const NUMERIC_MIN_SCALE: i32 = -1000;
pub const NUMERIC_MAX_SCALE: i32 = 1000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TypmodError {
    NumericPrecision(i32),
    NumericScale(i32),
    InvalidModifier,
    CharacterLengthTooSmall { type_name: &'static str },
    CharacterLengthTooLarge { type_name: &'static str, max: i32 },
}
impl TypmodError {
    pub const fn sqlstate(&self) -> &'static str {
        "22023"
    }
    pub fn message(&self) -> String {
        self.to_string()
    }
}
impl fmt::Display for TypmodError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NumericPrecision(p) => {
                write!(f, "NUMERIC precision {p} must be between 1 and 1000")
            }
            Self::NumericScale(s) => write!(f, "NUMERIC scale {s} must be between -1000 and 1000"),
            Self::InvalidModifier => f.write_str("invalid NUMERIC type modifier"),
            Self::CharacterLengthTooSmall { type_name } => {
                write!(f, "length for type {type_name} must be at least 1")
            }
            Self::CharacterLengthTooLarge { type_name, max } => {
                write!(f, "length for type {type_name} cannot exceed {max}")
            }
        }
    }
}
impl std::error::Error for TypmodError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct NumericTypmod {
    precision: i32,
    scale: i32,
}
impl NumericTypmod {
    /// Validates in PostgreSQL `numerictypmodin` order: precision, then scale.
    pub fn new(precision: i32, scale: i32) -> Result<Self, TypmodError> {
        if !(1..=NUMERIC_MAX_PRECISION).contains(&precision) {
            return Err(TypmodError::NumericPrecision(precision));
        }
        if !(NUMERIC_MIN_SCALE..=NUMERIC_MAX_SCALE).contains(&scale) {
            return Err(TypmodError::NumericScale(scale));
        }
        Ok(Self { precision, scale })
    }
    pub const fn precision(self) -> i32 {
        self.precision
    }
    pub const fn scale(self) -> i32 {
        self.scale
    }
    pub const fn to_typmod(self) -> i32 {
        ((self.precision << 16) | (self.scale & 0x7ff)) + VARHDRSZ
    }
    /// Decodes PostgreSQL's raw packed fields without applying input validation.
    pub const fn from_typmod(raw: i32) -> Option<Self> {
        if raw < VARHDRSZ {
            return None;
        }
        let packed = raw - VARHDRSZ;
        Some(Self {
            precision: (packed >> 16) & 0xffff,
            scale: ((packed & 0x7ff) ^ 1024) - 1024,
        })
    }
}
impl fmt::Display for NumericTypmod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({},{})", self.precision, self.scale)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NumericOverflow {
    precision: i32,
    scale: i32,
    infinite: bool,
}
impl NumericOverflow {
    pub const fn sqlstate(&self) -> &'static str {
        "22003"
    }
    pub const fn message(&self) -> &'static str {
        "numeric field overflow"
    }
    pub fn detail(&self) -> String {
        if self.infinite {
            format!(
                "A field with precision {}, scale {} cannot hold an infinite value.",
                self.precision, self.scale
            )
        } else {
            let maxdigits = self.precision - self.scale;
            let bound = if maxdigits != 0 {
                format!("10^{maxdigits}")
            } else {
                "1".to_owned()
            };
            format!(
                "A field with precision {}, scale {} must round to an absolute value less than {bound}.",
                self.precision, self.scale
            )
        }
    }
}
impl fmt::Display for NumericOverflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}
impl std::error::Error for NumericOverflow {}

impl Numeric {
    /// PostgreSQL `apply_typmod`: round first, then enforce the strict magnitude bound.
    pub fn apply_typmod(&self, modifier: NumericTypmod) -> Result<Self, NumericOverflow> {
        let overflow = NumericOverflow {
            precision: modifier.precision,
            scale: modifier.scale,
            infinite: false,
        };
        match &self.0 {
            Repr::NaN => return Ok(self.clone()),
            Repr::Infinity | Repr::NegInfinity => {
                return Err(NumericOverflow {
                    infinite: true,
                    ..overflow
                });
            }
            _ => {}
        }
        let rounded = self.round(modifier.scale).map_err(|_| overflow.clone())?;
        if let Repr::Finite { coefficient, scale } = &rounded.0
            && !coefficient.is_zero()
        {
            // |coefficient| * 10^-scale < 10^(precision - modifier.scale).
            let exponent = modifier.precision - modifier.scale + *scale as i32;
            if exponent <= 0 || coefficient.abs().to_str_radix(10).len() as i32 > exponent {
                return Err(overflow);
            }
        }
        Ok(rounded)
    }
}
