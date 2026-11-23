//! PostgreSQL type modifier validation and numeric modifier application.
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
    FloatPrecisionTooSmall,
    FloatPrecisionTooLarge,
    InvalidModifier,
    InvalidTypeModifier,
    InvalidIntervalModifier,
    NegativeTemporalPrecision { kind: TemporalType, precision: i32 },
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
            Self::FloatPrecisionTooSmall => f.write_str("precision for type float must be at least 1 bit"),
            Self::FloatPrecisionTooLarge => f.write_str("precision for type float must be less than 54 bits"),
            Self::InvalidModifier => f.write_str("invalid NUMERIC type modifier"),
            Self::InvalidTypeModifier => f.write_str("invalid type modifier"),
            Self::InvalidIntervalModifier => f.write_str("invalid INTERVAL type modifier"),
            Self::NegativeTemporalPrecision { kind, precision } => {
                let (name, zone) = match kind {
                    TemporalType::Timestamp => ("TIMESTAMP", ""),
                    TemporalType::TimestampTz => ("TIMESTAMP", " WITH TIME ZONE"),
                    TemporalType::Time => ("TIME", ""),
                    TemporalType::TimeTz => ("TIME", " WITH TIME ZONE"),
                    TemporalType::Interval => ("INTERVAL", ""),
                };
                write!(f, "{name}({precision}){zone} precision must not be negative")
            }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TemporalType {
    Timestamp,
    TimestampTz,
    Time,
    TimeTz,
    Interval,
}

impl TemporalType {
    /// PostgreSQL accepts precision above six and reduces it to six.
    pub fn precision(self, precision: i32) -> Result<i32, TypmodError> {
        if precision < 0 {
            return Err(TypmodError::NegativeTemporalPrecision { kind: self, precision });
        }
        Ok(precision.min(6))
    }
}

pub const INTERVAL_FULL_RANGE: i32 = 0x7fff;

/// Validate the field mask and optional precision given to intervaltypmodin.
pub fn interval_typmod(range: i32, precision: Option<i32>) -> Result<(), TypmodError> {
    if !matches!(range,
        INTERVAL_FULL_RANGE | 2 | 4 | 6 | 8 | 1024 | 1032 | 2048 | 3072 | 3080
        | 4096 | 6144 | 7168 | 7176
    ) {
        return Err(TypmodError::InvalidIntervalModifier);
    }
    if let Some(precision) = precision {
        TemporalType::Interval.precision(precision)?;
    }
    Ok(())
}

/// Validate FLOAT(p) and choose the four-byte or eight-byte type.
pub fn float_precision_is_real(precision: u32) -> Result<bool, TypmodError> {
    match precision {
        0 => Err(TypmodError::FloatPrecisionTooSmall),
        1..=24 => Ok(true),
        25..=53 => Ok(false),
        _ => Err(TypmodError::FloatPrecisionTooLarge),
    }
}

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
