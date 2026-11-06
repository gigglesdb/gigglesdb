//! Checked signed-64-bit arithmetic shared by engine expression evaluators.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IntegerError {
    Overflow,
    DivisionByZero,
}

pub(crate) fn negate(value: i64) -> Result<i64, IntegerError> {
    value.checked_neg().ok_or(IntegerError::Overflow)
}

pub(crate) fn abs(value: i64) -> Result<i64, IntegerError> {
    value.checked_abs().ok_or(IntegerError::Overflow)
}

pub(crate) fn add(left: i64, right: i64) -> Result<i64, IntegerError> {
    left.checked_add(right).ok_or(IntegerError::Overflow)
}

pub(crate) fn subtract(left: i64, right: i64) -> Result<i64, IntegerError> {
    left.checked_sub(right).ok_or(IntegerError::Overflow)
}

pub(crate) fn multiply(left: i64, right: i64) -> Result<i64, IntegerError> {
    left.checked_mul(right).ok_or(IntegerError::Overflow)
}

pub(crate) fn divide(left: i64, right: i64) -> Result<i64, IntegerError> {
    if right == 0 {
        return Err(IntegerError::DivisionByZero);
    }
    left.checked_div(right).ok_or(IntegerError::Overflow)
}

pub(crate) fn remainder(left: i64, right: i64) -> Result<i64, IntegerError> {
    if right == 0 {
        return Err(IntegerError::DivisionByZero);
    }
    // The mathematical remainder is representable even when MIN / -1 is not.
    if right == -1 {
        return Ok(0);
    }
    left.checked_rem(right).ok_or(IntegerError::Overflow)
}
