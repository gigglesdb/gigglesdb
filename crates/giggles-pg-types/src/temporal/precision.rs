use super::*;
// PG18 AdjustTimestampForTypmod/AdjustTimeForTypmod: ties away from zero.
fn rounded(v: i64, precision: u32) -> Result<i64, Error> {
    if precision > 6 {
        return Err(Error::Range);
    }
    let scale = 10i128.pow(6 - precision);
    let value = i128::from(v);
    i64::try_from(value.signum() * ((value.abs() + scale / 2) / scale) * scale)
        .map_err(|_| Error::Range)
}
impl Time {
    pub fn with_precision(self, precision: u32) -> Result<Self, Error> {
        Self::from_microseconds(rounded(self.0, precision)?)
    }
}
impl Timestamp {
    pub fn with_precision(self, precision: u32) -> Result<Self, Error> {
        if precision > 6 {
            return Err(Error::Range);
        }
        if self.0 == i64::MIN || self.0 == i64::MAX {
            return Ok(self);
        }
        Self::from_microseconds(rounded(self.0, precision)?)
    }
}
impl TimestampTz {
    pub fn with_precision(self, precision: u32) -> Result<Self, Error> {
        Timestamp(self.0)
            .with_precision(precision)
            .and_then(|v| Self::from_microseconds(v.0))
    }
}
impl TimeTz {
    pub fn with_precision(self, precision: u32) -> Result<Self, Error> {
        Self::new(self.time.with_precision(precision)?, self.offset_east)
    }
}
impl Interval {
    pub fn with_precision(self, precision: u32) -> Result<Self, Error> {
        if precision > 6 {
            return Err(Error::Range);
        }
        if self.same_components(Self::INFINITY) || self.same_components(Self::NEG_INFINITY) {
            return Ok(self);
        }
        Ok(Self::new(
            self.months,
            self.days,
            rounded(self.microseconds, precision)?,
        ))
    }
}
