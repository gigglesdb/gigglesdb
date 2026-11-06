use crate::{Error, MAX_TEXT};
use num_bigint::BigUint;
use num_traits::{ToPrimitive, Zero};
use std::cmp::Ordering;
macro_rules! float_type {
    ($name:ident,$float:ty,$bits:ty,$size:expr,$threshold:expr) => {
        #[derive(Clone, Copy, Debug)]
        pub struct $name($bits);
        impl $name {
            pub const fn from_bits(bits: $bits) -> Self {
                Self(bits)
            }
            pub const fn to_bits(self) -> $bits {
                self.0
            }
            pub fn value(self) -> $float {
                <$float>::from_bits(self.0)
            }
            pub fn from_binary(b: &[u8]) -> Result<Self, Error> {
                let a: [u8; $size] = b.try_into().map_err(|_| Error::Binary)?;
                Ok(Self(<$bits>::from_be_bytes(a)))
            }
            /// Stable serialization preserves every NaN payload and signed zero.
            pub fn to_binary(self) -> [u8; $size] {
                self.0.to_be_bytes()
            }
            pub fn parse(s: &str) -> Result<Self, Error> {
                if s.len() > MAX_TEXT {
                    return Err(Error::Limit);
                }
                let s = s.trim_matches(|c: char| matches!(c, '\t'..='\r' | ' '));
                let negative = s.starts_with('-');
                let raw = s.strip_prefix(['+', '-']).unwrap_or(s);
                let lower = raw.to_ascii_lowercase();
                if lower == "nan" {
                    let v = if negative {
                        -<$float>::NAN
                    } else {
                        <$float>::NAN
                    };
                    return Ok(Self(v.to_bits()));
                }
                if let Some(payload) = lower.strip_prefix("nan(").and_then(|s| s.strip_suffix(')'))
                {
                    if !payload
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_')
                    {
                        return Err(Error::Syntax);
                    }
                    let (digits, radix) = if let Some(x) = payload.strip_prefix("0x") {
                        (x, 16)
                    } else if payload.starts_with('0') {
                        (payload, 8)
                    } else {
                        (payload, 10)
                    };
                    let payload = if digits.is_empty() {
                        0
                    } else {
                        match u64::from_str_radix(digits, radix) {
                            Ok(n) => n,
                            Err(e) if matches!(e.kind(), std::num::IntErrorKind::PosOverflow) => {
                                u64::MAX
                            }
                            Err(_) => 0,
                        }
                    };
                    let mask = (1u64 << (<$float>::MANTISSA_DIGITS - 1)) - 1;
                    let sign = if negative {
                        (1 as $bits) << ($size * 8 - 1)
                    } else {
                        0
                    };
                    return Ok(Self(
                        <$float>::NAN.to_bits() | (payload & mask) as $bits | sign,
                    ));
                }
                if lower == "inf" || lower == "infinity" {
                    let v = if negative {
                        <$float>::NEG_INFINITY
                    } else {
                        <$float>::INFINITY
                    };
                    return Ok(Self(v.to_bits()));
                }
                // PG checks strtof/strtod ERANGE before checking trailing junk.
                // Preserve that error precedence for a valid numeric prefix.
                let prefix = numeric_prefix(&lower);
                if prefix.len() < lower.len() {
                    if let Some(hex) = prefix.strip_prefix("0x") {
                        if parse_hex(
                            hex,
                            <$float>::MANTISSA_DIGITS,
                            <$float>::MIN_EXP,
                            <$float>::MAX_EXP,
                        ) == Err(Error::Range)
                        {
                            return Err(Error::Range);
                        }
                    } else if let Ok(v) = prefix.parse::<$float>() {
                        let mant = prefix.split('e').next().unwrap_or("");
                        if v.is_infinite()
                            || (v == 0.0 && mant.bytes().any(|b| matches!(b, b'1'..=b'9')))
                        {
                            return Err(Error::Range);
                        }
                    }
                }
                if let Some(hex) = lower.strip_prefix("0x") {
                    let bits = parse_hex(
                        hex,
                        <$float>::MANTISSA_DIGITS,
                        <$float>::MIN_EXP,
                        <$float>::MAX_EXP,
                    )?;
                    let sign = if negative {
                        (1 as $bits) << ($size * 8 - 1)
                    } else {
                        0
                    };
                    return Ok(Self(bits as $bits | sign));
                }
                // Validate decimal grammar through Numeric; do not accept Rust-only spellings.
                // Float exponents can exceed Numeric limits, so grammar is checked separately.
                let raw = s.strip_prefix(['+', '-']).unwrap_or(s);
                let mut p = raw.split(['e', 'E']);
                let m = p.next().ok_or(Error::Syntax)?;
                let mut dots = 0;
                let mut digits = 0;
                for c in m.bytes() {
                    if c == b'.' {
                        dots += 1
                    } else if c.is_ascii_digit() {
                        digits += 1
                    } else {
                        return Err(Error::Syntax);
                    }
                }
                if dots > 1 || digits == 0 {
                    return Err(Error::Syntax);
                }
                if let Some(e) = p.next() {
                    let e = e.strip_prefix(['+', '-']).unwrap_or(e);
                    if e.is_empty() || !e.bytes().all(|c| c.is_ascii_digit()) {
                        return Err(Error::Syntax);
                    }
                }
                if p.next().is_some() {
                    return Err(Error::Syntax);
                }
                let v = s.parse::<$float>().map_err(|_| Error::Syntax)?;
                if v.is_infinite() || (v == 0.0 && m.bytes().any(|c| matches!(c, b'1'..=b'9'))) {
                    return Err(Error::Range);
                }
                Ok(Self(v.to_bits()))
            }
            /// `extra_float_digits` accepts PostgreSQL's -15..=3 range.
            /// Positive modes use shortest output; nonpositive modes use
            /// max(1, FLT_DIG/DBL_DIG + extra) significant decimal digits.
            pub fn to_text_with_extra_float_digits(self, extra: i32) -> Result<String, Error> {
                if !(-15..=3).contains(&extra) {
                    return Err(Error::Range);
                }
                if extra > 0 || !self.value().is_finite() || self.value() == 0.0 {
                    return Ok(self.to_text());
                }
                Ok(legacy(
                    self.0 as u64,
                    <$float>::MANTISSA_DIGITS,
                    <$float>::MAX_EXP,
                    ($threshold + extra).max(1),
                ))
            }
            /// PostgreSQL 18 default (`extra_float_digits > 0`) shortest output.
            /// Decimal candidates strictly exclude binary rounding midpoints.
            pub fn to_text(self) -> String {
                let v = self.value();
                if v.is_nan() {
                    "NaN".into()
                } else if v == <$float>::INFINITY {
                    "Infinity".into()
                } else if v == <$float>::NEG_INFINITY {
                    "-Infinity".into()
                } else if v == 0.0 {
                    if v.is_sign_negative() {
                        "-0".into()
                    } else {
                        "0".into()
                    }
                } else {
                    shortest(
                        self.0 as u64,
                        <$float>::MANTISSA_DIGITS,
                        <$float>::MAX_EXP,
                        $threshold,
                    )
                }
            }
        }
        impl Ord for $name {
            fn cmp(&self, b: &Self) -> Ordering {
                let a = self.value();
                let b = b.value();
                match (a.is_nan(), b.is_nan()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Greater,
                    (false, true) => Ordering::Less,
                    _ => a.partial_cmp(&b).unwrap(),
                }
            }
        }
        impl PartialOrd for $name {
            fn partial_cmp(&self, b: &Self) -> Option<Ordering> {
                Some(self.cmp(b))
            }
        }
        impl PartialEq for $name {
            fn eq(&self, b: &Self) -> bool {
                self.cmp(b) == Ordering::Equal
            }
        }
        impl Eq for $name {}
    };
}
float_type!(Float4, f32, u32, 4, 6);
float_type!(Float8, f64, u64, 8, 15);

// Exact-arithmetic implementation of the legal interval and nearest decimal
// selection in PostgreSQL REL_18_0 src/common/{f2s,d2s}.c. Unlike that Ryu
// implementation this uses bigint division rather than cached-power tables.
// The open interval (acceptBounds=false) is essential for PG midpoint avoidance.
fn ratio(m: u64, e: i32) -> (BigUint, BigUint) {
    if e >= 0 {
        (BigUint::from(m) << e as usize, BigUint::from(1u8))
    } else {
        (BigUint::from(m), BigUint::from(1u8) << (-e) as usize)
    }
}
fn scaled(n: &BigUint, d: &BigUint, e: i32) -> (BigUint, BigUint) {
    let ten = BigUint::from(10u8).pow(e.unsigned_abs());
    if e >= 0 {
        (n.clone(), d * ten)
    } else {
        (n * ten, d.clone())
    }
}
fn nearest(n: &BigUint, d: &BigUint) -> BigUint {
    let q = n / d;
    let twice = (n % d) << 1;
    if twice > *d || (twice == *d && q.bit(0)) {
        q + 1u8
    } else {
        q
    }
}
fn shortest(bits: u64, precision: u32, max_exp: i32, threshold: i32) -> String {
    let fraction_bits = precision - 1;
    let exponent_bits = if precision == 24 { 8 } else { 11 };
    let negative = bits >> (fraction_bits + exponent_bits) != 0;
    let fraction = bits & ((1u64 << fraction_bits) - 1);
    let exponent = ((bits >> fraction_bits) & ((1 << exponent_bits) - 1)) as i32;
    let m = fraction | if exponent == 0 { 0 } else { 1 << fraction_bits };
    let e = exponent.max(1) - (max_exp - 1) - fraction_bits as i32 - 2;
    let (n, d) = ratio(4 * m, e);
    let (low, _) = ratio(4 * m - 1 - u64::from(fraction != 0 || exponent <= 1), e);
    let (high, _) = ratio(4 * m + 2, e);
    // Determine floor(log10(value)) with exact comparisons, no floating estimate.
    let mut decade = n.to_str_radix(10).len() as i32 - d.to_str_radix(10).len() as i32;
    let (sn, sd) = scaled(&n, &d, decade);
    if sn < sd {
        decade -= 1;
    }
    for digits in 1..=precision {
        let e10 = decade - digits as i32 + 1;
        let (ln, ld) = scaled(&low, &d, e10);
        let (hn, hd) = scaled(&high, &d, e10);
        let min = ln / ld + 1u8;
        let max = (hn - 1u8) / hd;
        if min > max {
            continue;
        }
        let (vn, vd) = scaled(&n, &d, e10);
        let coefficient = nearest(&vn, &vd).max(min).min(max);
        return layout(coefficient.to_str_radix(10), e10, negative, threshold);
    }
    unreachable!("IEEE decimal round-trip bound")
}
fn layout(mut digits: String, mut e: i32, negative: bool, threshold: i32) -> String {
    while digits.ends_with('0') {
        digits.pop();
        e += 1;
    }
    let exp = e + digits.len() as i32 - 1;
    let body = if !(-4..threshold).contains(&exp) {
        let tail = if digits.len() == 1 {
            String::new()
        } else {
            format!(".{}", &digits[1..])
        };
        format!("{}{tail}e{exp:+03}", &digits[..1])
    } else {
        let point = digits.len() as i32 + e;
        if point <= 0 {
            format!("0.{}{}", "0".repeat((-point) as usize), digits)
        } else if point >= digits.len() as i32 {
            format!(
                "{}{}",
                digits,
                "0".repeat((point - digits.len() as i32) as usize)
            )
        } else {
            format!(
                "{}.{}",
                &digits[..point as usize],
                &digits[point as usize..]
            )
        }
    };
    if negative { format!("-{body}") } else { body }
}
// C99 hexadecimal grammar, rounded once directly to the destination precision.
fn parse_hex(s: &str, precision: u32, min_exp: i32, max_exp: i32) -> Result<u64, Error> {
    let mut parts = s.split('p');
    let mant = parts.next().ok_or(Error::Syntax)?;
    let exp = if let Some(s) = parts.next() {
        let raw = s.strip_prefix(['+', '-']).unwrap_or(s);
        if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
            return Err(Error::Syntax);
        }
        s.parse::<i64>().unwrap_or(if s.starts_with('-') {
            i64::MIN / 2
        } else {
            i64::MAX / 2
        })
    } else {
        0
    };
    if parts.next().is_some() {
        return Err(Error::Syntax);
    }
    let mut digits = String::new();
    let mut point = false;
    let mut fractional = 0i64;
    for b in mant.bytes() {
        if b == b'.' && !point {
            point = true;
        } else if b.is_ascii_hexdigit() {
            digits.push(b as char);
            if point {
                fractional += 1;
            }
        } else {
            return Err(Error::Syntax);
        }
    }
    if digits.is_empty() {
        return Err(Error::Syntax);
    }
    let n = BigUint::parse_bytes(digits.as_bytes(), 16).ok_or(Error::Syntax)?;
    if n.is_zero() {
        return Ok(0);
    }
    let e = exp.saturating_sub(4 * fractional);
    let top = (n.bits() as i64 - 1).saturating_add(e);
    if top >= max_exp as i64 || top < (min_exp - precision as i32 - 1) as i64 {
        return Err(Error::Range);
    }
    let quantum = (top - precision as i64 + 1).max((min_exp - precision as i32) as i64);
    let shift = quantum - e;
    let rounded = if shift > 0 {
        nearest(&n, &(BigUint::from(1u8) << shift as usize))
    } else {
        n << (-shift) as usize
    };
    let mut m = rounded.to_u64().ok_or(Error::Range)?;
    if m == 0 {
        return Err(Error::Range);
    }
    let mut q = quantum;
    if m >= (1 << precision) {
        m >>= 1;
        q += 1;
    }
    let exponent = q + precision as i64 - 1;
    if exponent >= max_exp as i64 {
        return Err(Error::Range);
    }
    if m < 1 << (precision - 1) {
        Ok(m)
    } else {
        Ok(((exponent + max_exp as i64 - 1) as u64) << (precision - 1)
            | (m - (1 << (precision - 1))))
    }
}

fn legacy(bits: u64, precision: u32, max_exp: i32, digits: i32) -> String {
    let fb = precision - 1;
    let eb = if precision == 24 { 8 } else { 11 };
    let exponent = ((bits >> fb) & ((1 << eb) - 1)) as i32;
    let fraction = bits & ((1 << fb) - 1);
    let m = fraction | if exponent == 0 { 0 } else { 1 << fb };
    let (n, d) = ratio(m, exponent.max(1) - (max_exp - 1) - fb as i32);
    let mut decade = n.to_str_radix(10).len() as i32 - d.to_str_radix(10).len() as i32;
    let (sn, sd) = scaled(&n, &d, decade);
    if sn < sd {
        decade -= 1;
    }
    let e = decade - digits + 1;
    let (sn, sd) = scaled(&n, &d, e);
    layout(
        nearest(&sn, &sd).to_str_radix(10),
        e,
        bits >> (fb + eb) != 0,
        digits,
    )
}

fn numeric_prefix(s: &str) -> &str {
    let b = s.as_bytes();
    let hex = s.starts_with("0x");
    let mut i = if hex { 2 } else { 0 };
    let start = i;
    let mut point = false;
    while i < b.len() {
        if if hex {
            b[i].is_ascii_hexdigit()
        } else {
            b[i].is_ascii_digit()
        } {
            i += 1;
        } else if b[i] == b'.' && !point {
            point = true;
            i += 1;
        } else {
            break;
        }
    }
    if i == start {
        return "";
    }
    if i < b.len() && b[i] == if hex { b'p' } else { b'e' } {
        let mark = i;
        i += 1;
        if i < b.len() && matches!(b[i], b'+' | b'-') {
            i += 1;
        }
        let first = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == first {
            i = mark;
        }
    }
    &s[..i]
}
