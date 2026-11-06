//! Bounded PostgreSQL 18 integer text input, independent of SQL token grammar.
//!
//! Adapted from PostgreSQL REL_18_0 src/backend/utils/adt/numutils.c.
//! See INTEGER-PG-NOTICE for the upstream copyright and license.
//! PostgreSQL's C-string input excludes embedded NUL. This API never silently
//! truncates a borrowed Rust string at NUL: syntax rejection applies unless
//! an overflowing prefix has already produced Range. NUL has no PG parity claim.

/// Explicit API resource ceiling. Inputs above this size are not classified
/// as PostgreSQL syntax or range errors, regardless of their contents.
pub const MAX_INPUT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntegerError {
    Syntax,
    Range,
    Limit,
}

impl IntegerError {
    /// PostgreSQL SQLSTATE where the error has a PostgreSQL counterpart.
    pub const fn sqlstate(self) -> Option<&'static str> {
        match self {
            Self::Syntax => Some("22P02"),
            Self::Range => Some("22003"),
            Self::Limit => None,
        }
    }
}

impl std::fmt::Display for IntegerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Syntax => "invalid integer text syntax",
            Self::Range => "integer text out of range",
            Self::Limit => "integer text input limit exceeded",
        })
    }
}
impl std::error::Error for IntegerError {}

pub fn parse_i16(text: &str) -> Result<i16, IntegerError> {
    scan(text, 16).map(|n| n as i16)
}
pub fn parse_i32(text: &str) -> Result<i32, IntegerError> {
    scan(text, 32).map(|n| n as i32)
}
pub fn parse_i64(text: &str) -> Result<i64, IntegerError> {
    scan(text, 64)
}

fn space(b: u8) -> bool {
    b == b' ' || (b'\t'..=b'\r').contains(&b)
}
fn digit(b: u8, base: u64) -> Option<u64> {
    let n = match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => return None,
    };
    (u64::from(n) < base).then_some(u64::from(n))
}

fn scan(text: &str, bits: u32) -> Result<i64, IntegerError> {
    if text.len() > MAX_INPUT_BYTES {
        return Err(IntegerError::Limit);
    }
    let bytes = text.as_bytes();
    let mut pos = 0;
    while bytes.get(pos).is_some_and(|b| space(*b)) {
        pos += 1;
    }
    let negative = bytes.get(pos) == Some(&b'-');
    if negative || bytes.get(pos) == Some(&b'+') {
        pos += 1;
    }
    let mut base = 10;
    if bytes.get(pos) == Some(&b'0') {
        base = match bytes.get(pos + 1) {
            Some(b'x' | b'X') => 16,
            Some(b'o' | b'O') => 8,
            Some(b'b' | b'B') => 2,
            _ => 10,
        };
        if base != 10 {
            pos += 2;
        }
    }
    let first = pos;
    let minimum_magnitude = 1u64 << (bits - 1);
    let threshold = minimum_magnitude / base;
    let mut magnitude = 0u64;
    loop {
        match bytes.get(pos).copied() {
            Some(b) if digit(b, base).is_some() => {
                // Preserve PostgreSQL's *pre-multiply* overflow precedence.
                // In particular, a merely final-width overflow followed by junk
                // is Syntax, while an overflowing prefix is already Range.
                if magnitude > threshold {
                    return Err(IntegerError::Range);
                }
                // threshold * base + (base-1) <= 2^63 + 15: no u64 overflow.
                magnitude = magnitude * base + digit(b, base).unwrap();
                pos += 1;
            }
            Some(b'_') => {
                if (base == 10 && pos == first)
                    || !bytes
                        .get(pos + 1)
                        .is_some_and(|b| digit(*b, base).is_some())
                {
                    return Err(IntegerError::Syntax);
                }
                pos += 1;
            }
            _ => break,
        }
    }
    if pos == first {
        return Err(IntegerError::Syntax);
    }
    while bytes.get(pos).is_some_and(|b| space(*b)) {
        pos += 1;
    }
    if pos != bytes.len() {
        return Err(IntegerError::Syntax);
    }
    if magnitude > minimum_magnitude || (!negative && magnitude == minimum_magnitude) {
        return Err(IntegerError::Range);
    }
    if negative {
        // Wrapping operations represent i64::MIN without a signed overflow.
        Ok((magnitude as i64).wrapping_neg())
    } else {
        Ok(magnitude as i64)
    }
}
