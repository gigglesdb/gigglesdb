//! Bounded PostgreSQL numeric and bit-preserving floating representations.
use num_bigint::BigInt;
use num_traits::{Signed, Zero};
use std::{cmp::Ordering, fmt};
pub mod bytea;
mod float;
pub mod integer;
mod numeric_aggregate;
mod numeric_math;
pub mod temporal;
pub use float::{Float4, Float8};

/// Deliberate resource ceiling, smaller than PostgreSQL's maximum integral width.
pub const MAX_DIGITS: usize = 16_384;
pub const MAX_SCALE: u32 = 16_383;
pub const MAX_TEXT: usize = 32_800;
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Syntax,
    Limit,
    Binary,
    DivisionByZero,
    Unimplemented,
    Range,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Clone, Debug)]
enum Repr {
    Finite { coefficient: BigInt, scale: u32 },
    NegInfinity,
    Infinity,
    NaN,
}
#[derive(Clone, Debug)]
pub struct Numeric(Repr);
// All callers use validated scales; scratch exponent is at most twice MAX_SCALE.
fn pow(n: u32) -> BigInt {
    assert!(n <= 2 * MAX_SCALE + 4);
    BigInt::from(10u8).pow(n)
}
impl Numeric {
    fn finite(coefficient: BigInt, scale: u32) -> Result<Self, Error> {
        if scale > MAX_SCALE
            || coefficient.to_str_radix(10).trim_start_matches('-').len() > MAX_DIGITS
        {
            return Err(Error::Limit);
        }
        Ok(Self(Repr::Finite { coefficient, scale }))
    }
    pub fn parse(text: &str) -> Result<Self, Error> {
        if text.len() > MAX_TEXT {
            return Err(Error::Limit);
        }
        let s = text.trim_matches(|c: char| c.is_ascii_whitespace());
        match s.to_ascii_lowercase().as_str() {
            "nan" => return Ok(Self(Repr::NaN)),
            "infinity" | "+infinity" | "inf" | "+inf" => return Ok(Self(Repr::Infinity)),
            "-infinity" | "-inf" => return Ok(Self(Repr::NegInfinity)),
            _ => {}
        }
        let (s, neg) = if let Some(s) = s.strip_prefix('-') {
            (s, true)
        } else {
            (s.strip_prefix('+').unwrap_or(s), false)
        };
        // Nondecimal separators may appear immediately after the prefix.
        if s.len() >= 2 && s.as_bytes()[0] == b'0' {
            let base = match s.as_bytes()[1] {
                b'x' | b'X' => Some(16),
                b'o' | b'O' => Some(8),
                b'b' | b'B' => Some(2),
                _ => None,
            };
            if let Some(base) = base {
                let body = &s[2..];
                let mut digits = String::with_capacity(body.len());
                for (i, c) in body.char_indices() {
                    if c == '_' {
                        if !body
                            .as_bytes()
                            .get(i + 1)
                            .is_some_and(|b| char::from(*b).is_digit(base))
                        {
                            return Err(Error::Syntax);
                        }
                    } else if c.is_ascii() && c.is_digit(base) {
                        digits.push(c);
                    } else {
                        return Err(Error::Syntax);
                    }
                }
                if digits.is_empty() {
                    return Err(Error::Syntax);
                }
                let digits = digits.trim_start_matches('0');
                // Integer lower bound for log10(base), excluding the first digit.
                let log = match base {
                    16 => 120411usize,
                    8 => 90308,
                    _ => 30102,
                };
                if digits.len().saturating_sub(1) * log / 100000 >= MAX_DIGITS {
                    return Err(Error::Limit);
                }
                let c = if digits.is_empty() {
                    BigInt::zero()
                } else {
                    BigInt::parse_bytes(digits.as_bytes(), base).ok_or(Error::Syntax)?
                };
                return Self::finite(if neg { -c } else { c }, 0);
            }
        }
        let mut cleaned = String::with_capacity(s.len());
        for (i, c) in s.bytes().enumerate() {
            if c == b'_' {
                if i == 0
                    || i + 1 == s.len()
                    || !s.as_bytes()[i - 1].is_ascii_digit()
                    || !s.as_bytes()[i + 1].is_ascii_digit()
                {
                    return Err(Error::Syntax);
                }
            } else {
                cleaned.push(char::from(c));
            }
        }
        let s = cleaned.as_str();
        let mut parts = s.split(['e', 'E']);
        let mant = parts.next().ok_or(Error::Syntax)?;
        let exp = parts
            .next()
            .map(|s| s.parse::<i32>().map_err(|_| Error::Limit))
            .transpose()?
            .unwrap_or(0);
        if parts.next().is_some() {
            return Err(Error::Syntax);
        }
        let mut digits = String::new();
        let mut dot = false;
        let mut frac = 0i64;
        for c in mant.chars() {
            if c == '.' && !dot {
                dot = true;
            } else if c.is_ascii_digit() {
                digits.push(c);
                if dot {
                    frac += 1
                }
            } else {
                return Err(Error::Syntax);
            }
        }
        if digits.is_empty() {
            return Err(Error::Syntax);
        }
        let digits = digits.trim_start_matches('0');
        let scale = frac - i64::from(exp);
        if scale > i64::from(MAX_SCALE) || scale < -(MAX_DIGITS as i64) || digits.len() > MAX_DIGITS
        {
            return Err(Error::Limit);
        }
        let mut c = if digits.is_empty() {
            BigInt::zero()
        } else {
            BigInt::parse_bytes(digits.as_bytes(), 10).ok_or(Error::Syntax)?
        };
        if neg {
            c = -c
        }
        if scale < 0 {
            if digits.len() + (-scale) as usize > MAX_DIGITS {
                return Err(Error::Limit);
            }
            if !c.is_zero() {
                c *= pow((-scale) as u32);
            }
        }
        Self::finite(c, scale.max(0) as u32)
    }
    /// PostgreSQL REL_18_0 select_div_scale: base-10000 weight estimate,
    /// at least 16 significant digits, capped at 1000 fractional digits.
    pub fn checked_div(&self, b: &Self) -> Result<Self, Error> {
        if self.scale().is_none() || b.scale().is_none() {
            return self.div_at_scale(b, 0);
        }
        let header = |n: &Self| {
            let bytes = n.to_binary();
            let w = i16::from_be_bytes([bytes[2], bytes[3]]) as i32;
            let first = if bytes.len() > 8 {
                u16::from_be_bytes([bytes[8], bytes[9]])
            } else {
                0
            };
            (w, first)
        };
        let (wa, a) = header(self);
        let (wb, bb) = header(b);
        let qweight = wa - wb - i32::from(a <= bb);
        let scale = (16 - qweight * 4)
            .max(self.scale().unwrap() as i32)
            .max(b.scale().unwrap() as i32)
            .clamp(0, 1000);
        self.div_at_scale(b, scale as u32)
    }
    fn sign(&self) -> i8 {
        match &self.0 {
            Repr::NegInfinity => -1,
            Repr::Infinity => 1,
            Repr::Finite { coefficient, .. } => {
                if coefficient.is_zero() {
                    0
                } else if coefficient.is_negative() {
                    -1
                } else {
                    1
                }
            }
            Repr::NaN => 0,
        }
    }
    fn infinity(negative: bool) -> Self {
        Self(if negative {
            Repr::NegInfinity
        } else {
            Repr::Infinity
        })
    }
    /// Empty input returns None, matching SQL SUM NULL presence without owning SQL NULL.
    pub fn checked_sum<'a>(
        values: impl IntoIterator<Item = &'a Self>,
    ) -> Result<Option<Self>, Error> {
        let mut result: Option<Self> = None;
        for value in values {
            result = Some(match result {
                None => value.clone(),
                Some(acc) => acc.checked_add(value)?,
            })
        }
        Ok(result)
    }
    /// Allocation-free upper bound for canonical display bytes. The validated
    /// coefficient and scale bound all intermediate integer arithmetic here.
    pub fn display_byte_bound(&self) -> usize {
        match &self.0 {
            Repr::Finite { coefficient, scale } => {
                // 30103/100000 strictly exceeds log10(2).
                let digits = (coefficient.bits() as usize * 30103 / 100000 + 1).max(1);
                digits.max(*scale as usize + 1)
                    + usize::from(*scale > 0)
                    + usize::from(coefficient.is_negative())
            }
            Repr::NaN => 3,
            Repr::Infinity => 8,
            Repr::NegInfinity => 9,
        }
    }
    pub fn scale(&self) -> Option<u32> {
        match &self.0 {
            Repr::Finite { scale, .. } => Some(*scale),
            _ => None,
        }
    }
    pub fn checked_add(&self, b: &Self) -> Result<Self, Error> {
        match (&self.0, &b.0) {
            (
                Repr::Finite {
                    coefficient: a,
                    scale: x,
                },
                Repr::Finite {
                    coefficient: b,
                    scale: y,
                },
            ) => {
                let s = (*x).max(*y);
                Self::finite(a * pow(s - x) + b * pow(s - y), s)
            }
            (Repr::NaN, _) | (_, Repr::NaN) => Ok(Self(Repr::NaN)),
            (Repr::Infinity, Repr::NegInfinity) | (Repr::NegInfinity, Repr::Infinity) => {
                Ok(Self(Repr::NaN))
            }
            (Repr::Infinity, _) | (_, Repr::Infinity) => Ok(Self(Repr::Infinity)),
            _ => Ok(Self(Repr::NegInfinity)),
        }
    }
    pub fn checked_sub(&self, b: &Self) -> Result<Self, Error> {
        let mut b = b.clone();
        match &mut b.0 {
            Repr::Finite { coefficient, .. } => *coefficient = -coefficient.clone(),
            Repr::Infinity => b.0 = Repr::NegInfinity,
            Repr::NegInfinity => b.0 = Repr::Infinity,
            Repr::NaN => {}
        }
        self.checked_add(&b)
    }
    pub fn checked_mul(&self, b: &Self) -> Result<Self, Error> {
        match (&self.0, &b.0) {
            (
                Repr::Finite {
                    coefficient: a,
                    scale: x,
                },
                Repr::Finite {
                    coefficient: b,
                    scale: y,
                },
            ) => {
                if x + y > MAX_SCALE {
                    return Err(Error::Limit);
                }
                if !a.is_zero()
                    && !b.is_zero()
                    && a.abs().to_str_radix(10).len() + b.abs().to_str_radix(10).len() - 1
                        > MAX_DIGITS
                {
                    return Err(Error::Limit);
                }
                Self::finite(a * b, x + y)
            }
            (Repr::NaN, _) | (_, Repr::NaN) => Ok(Self(Repr::NaN)),
            _ if self.sign() == 0 || b.sign() == 0 => Ok(Self(Repr::NaN)),
            _ => Ok(Self::infinity(self.sign() != b.sign())),
        }
    }
    /// Explicit output scale, ties away from zero. Specials follow SQL division.
    pub fn div_at_scale(&self, b: &Self, scale: u32) -> Result<Self, Error> {
        if scale > MAX_SCALE {
            return Err(Error::Limit);
        }
        match (&self.0, &b.0) {
            (
                Repr::Finite {
                    coefficient: a,
                    scale: x,
                },
                Repr::Finite {
                    coefficient: b,
                    scale: y,
                },
            ) => {
                if b.is_zero() {
                    return Err(Error::DivisionByZero);
                }
                let e = i64::from(scale) + i64::from(*y) - i64::from(*x);
                if a.is_zero() {
                    return Self::finite(BigInt::zero(), scale);
                }
                // Lower bound on quotient width, checked before power/clone.
                if a.abs().to_str_radix(10).len() as i64 + e - b.abs().to_str_radix(10).len() as i64
                    > MAX_DIGITS as i64
                {
                    return Err(Error::Limit);
                }
                let (n, d) = if e >= 0 {
                    (a * pow(e as u32), b.clone())
                } else {
                    (a.clone(), b * pow((-e) as u32))
                };
                Self::finite(round_quotient(n, d), scale)
            }
            (Repr::NaN, _) | (_, Repr::NaN) => Ok(Self(Repr::NaN)),
            _ if b.sign() == 0 => Err(Error::DivisionByZero),
            (Repr::Finite { .. }, _) => Self::finite(BigInt::zero(), 0),
            (_, Repr::Finite { .. }) => Ok(Self::infinity(self.sign() != b.sign())),
            _ => Ok(Self(Repr::NaN)),
        }
    }
    pub fn round(&self, scale: i32) -> Result<Self, Error> {
        if scale.unsigned_abs() > MAX_SCALE {
            return Err(Error::Limit);
        }
        match &self.0 {
            Repr::Finite {
                coefficient,
                scale: old,
            } => {
                let delta = i64::from(*old) - i64::from(scale);
                if coefficient.is_zero() {
                    return Self::finite(BigInt::zero(), scale.max(0) as u32);
                }
                if delta < 0
                    && coefficient.abs().to_str_radix(10).len() as i64 - delta > MAX_DIGITS as i64
                {
                    return Err(Error::Limit);
                }
                let c = if delta > 0 {
                    round_quotient(coefficient.clone(), pow(delta as u32))
                } else {
                    coefficient * pow((-delta) as u32)
                };
                if scale < 0 {
                    Self::finite(c * pow(scale.unsigned_abs()), 0)
                } else {
                    Self::finite(c, scale as u32)
                }
            }
            _ => Ok(self.clone()),
        }
    }
    /// Canonical PostgreSQL base-10000 wire encoding. Also the stable standalone serialization.
    pub fn to_binary(&self) -> Vec<u8> {
        let mut weight = 0i16;
        let mut sign = 0u16;
        let mut scale = 0u16;
        let mut ds = Vec::<u16>::new();
        match &self.0 {
            Repr::NaN => sign = 0xc000,
            Repr::Infinity => {
                sign = 0xd000;
                scale = 32
            }
            Repr::NegInfinity => {
                sign = 0xf000;
                scale = 32
            }
            Repr::Finite {
                coefficient,
                scale: s,
            } => {
                scale = *s as u16;
                if coefficient.is_negative() {
                    sign = 0x4000
                }
                if !coefficient.is_zero() {
                    let raw = coefficient.abs().to_str_radix(10);
                    let frac = (*s as usize).div_ceil(4);
                    let pad = frac * 4 - *s as usize;
                    let mut raw = raw + &"0".repeat(pad);
                    let left = (4 - raw.len() % 4) % 4;
                    raw = "0".repeat(left) + &raw;
                    ds = raw
                        .as_bytes()
                        .chunks(4)
                        .map(|c| std::str::from_utf8(c).unwrap().parse().unwrap())
                        .collect();
                    weight = (ds.len() as i32 - frac as i32 - 1) as i16;
                    while ds.first() == Some(&0) {
                        ds.remove(0);
                        weight -= 1
                    }
                    while ds.last() == Some(&0) {
                        ds.pop();
                    }
                } else {
                    sign = 0
                }
            }
        }
        let mut out = Vec::with_capacity(8 + 2 * ds.len());
        for x in [ds.len() as u16, weight as u16, sign, scale] {
            out.extend(x.to_be_bytes())
        }
        for d in ds {
            out.extend(d.to_be_bytes())
        }
        out
    }
    pub fn from_binary(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < 8 || bytes.len() > 8 + MAX_DIGITS / 4 * 2 + 2 {
            return Err(Error::Binary);
        }
        let word = |i| u16::from_be_bytes([bytes[i], bytes[i + 1]]);
        let n = word(0) as usize;
        let w = word(2) as i16;
        let sign = word(4);
        let s = word(6) as u32;
        if n > MAX_DIGITS / 4 + 1 || bytes.len() != 8 + 2 * n || s > MAX_SCALE {
            return Err(Error::Binary);
        }
        if !matches!(sign, 0 | 0x4000 | 0xc000 | 0xd000 | 0xf000) {
            return Err(Error::Binary);
        }
        for i in 0..n {
            if word(8 + i * 2) > 9999 {
                return Err(Error::Binary);
            }
        }
        if sign >= 0xc000 {
            return Ok(Self(match sign {
                0xc000 => Repr::NaN,
                0xd000 => Repr::Infinity,
                _ => Repr::NegInfinity,
            }));
        }
        // Normalize before weight/resource checks: receive allows redundant zero
        // groups and arbitrary zero weight. Never exponentiate an untrusted weight.
        let first = (0..n).find(|i| word(8 + 2 * i) != 0);
        let Some(first) = first else {
            return Self::finite(BigInt::zero(), s);
        };
        let weight = i32::from(w) - first as i32;
        let top = word(8 + 2 * first);
        let top_width = top.ilog10() as i32 + 1;
        let decimal_width = weight * 4 + top_width + s as i32;
        if decimal_width > MAX_DIGITS as i32 {
            return Err(Error::Limit);
        }
        if decimal_width <= 0 {
            return Self::finite(BigInt::zero(), s);
        }
        let mut digits = top.to_string();
        for i in first + 1..n {
            use std::fmt::Write;
            write!(digits, "{:04}", word(8 + 2 * i)).unwrap();
        }
        let width = decimal_width as usize;
        if digits.len() > width {
            digits.truncate(width);
        } else {
            digits.extend(std::iter::repeat_n('0', width - digits.len()));
        }
        let mut c = BigInt::parse_bytes(digits.as_bytes(), 10).ok_or(Error::Binary)?;
        if sign == 0x4000 {
            c = -c;
        }
        Self::finite(c, s)
    }
}
fn round_quotient(n: BigInt, d: BigInt) -> BigInt {
    let mut q = &n / &d;
    let r = &n % &d;
    if r.abs() * 2 >= d.abs() {
        q += if n.sign() == d.sign() { 1 } else { -1 }
    }
    q
}
impl fmt::Display for Numeric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Repr::NaN => f.write_str("NaN"),
            Repr::Infinity => f.write_str("Infinity"),
            Repr::NegInfinity => f.write_str("-Infinity"),
            Repr::Finite { coefficient, scale } => {
                let mut s = coefficient.abs().to_str_radix(10);
                let scale = *scale as usize;
                if s.len() <= scale {
                    s = "0".repeat(scale + 1 - s.len()) + &s
                }
                if scale > 0 {
                    s.insert(s.len() - scale, '.')
                }
                if coefficient.is_negative() {
                    f.write_str("-")?
                }
                f.write_str(&s)
            }
        }
    }
}
impl Ord for Numeric {
    fn cmp(&self, b: &Self) -> Ordering {
        fn rank(r: &Repr) -> u8 {
            match r {
                Repr::NegInfinity => 0,
                Repr::Finite { .. } => 1,
                Repr::Infinity => 2,
                Repr::NaN => 3,
            }
        }
        match (&self.0, &b.0) {
            (
                Repr::Finite {
                    coefficient: a,
                    scale: x,
                },
                Repr::Finite {
                    coefficient: b,
                    scale: y,
                },
            ) => {
                let sign = a.sign().cmp(&b.sign());
                if sign != Ordering::Equal {
                    return sign;
                }
                if a.is_zero() {
                    return Ordering::Equal;
                }
                let aa = a.abs().to_str_radix(10);
                let bb = b.abs().to_str_radix(10);
                let magnitude = (aa.len() as i64 - i64::from(*x))
                    .cmp(&(bb.len() as i64 - i64::from(*y)))
                    .then_with(|| {
                        aa.bytes()
                            .chain(std::iter::repeat(b'0'))
                            .take(aa.len().max(bb.len()))
                            .cmp(
                                bb.bytes()
                                    .chain(std::iter::repeat(b'0'))
                                    .take(aa.len().max(bb.len())),
                            )
                    });
                if a.is_negative() {
                    magnitude.reverse()
                } else {
                    magnitude
                }
            }
            _ => rank(&self.0).cmp(&rank(&b.0)),
        }
    }
}
impl PartialOrd for Numeric {
    fn partial_cmp(&self, b: &Self) -> Option<Ordering> {
        Some(self.cmp(b))
    }
}
impl PartialEq for Numeric {
    fn eq(&self, b: &Self) -> bool {
        self.cmp(b) == Ordering::Equal
    }
}
impl Eq for Numeric {}

#[cfg(test)]
mod display_bound_tests {
    use super::*;
    #[test]
    fn display_bound_covers_zero_sign_scale_specials_and_digit_edges() {
        for text in [
            "0",
            "-0.000",
            "1.00",
            "-999999999.001",
            "NaN",
            "Infinity",
            "-Infinity",
            "1e-16383",
        ] {
            let n = Numeric::parse(text).unwrap();
            assert!(n.to_string().len() <= n.display_byte_bound(), "{text}");
        }
        for digits in [1, 2, 9, 31, 100, 1000, MAX_DIGITS] {
            for sign in ["", "-"] {
                let n = Numeric::parse(&format!("{sign}{}", "9".repeat(digits))).unwrap();
                assert!(n.to_string().len() <= n.display_byte_bound());
            }
        }
        for value in -2048..2048 {
            let n = Numeric::parse(&format!("{value}.00000")).unwrap();
            assert!(n.to_string().len() <= n.display_byte_bound());
        }
    }
}
