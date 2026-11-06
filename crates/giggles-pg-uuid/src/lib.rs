//! PostgreSQL 18 UUID value foundation. No ambient clock or entropy.
pub use giggles_pg_types::temporal::TimestampTz;
use std::{fmt, str::FromStr};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Text,
    Binary,
}
impl Error {
    pub const fn sqlstate(self) -> &'static str {
        match self {
            Self::Text => "22P02",
            Self::Binary => "22P03",
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Text => "invalid input syntax for type uuid",
            Self::Binary => "invalid uuid binary length",
        })
    }
}
impl std::error::Error for Error {}
/// Exact 16-byte value. Ordering is PostgreSQL's unsigned bytewise ordering.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Uuid([u8; 16]);
impl Uuid {
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
    pub const fn to_binary(self) -> [u8; 16] {
        self.0
    }
    pub fn from_binary(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self(bytes.try_into().map_err(|_| Error::Binary)?))
    }
    /// Caller supplies all entropy; this is not SQL uuidv4().
    pub const fn v4_from_entropy(mut entropy: [u8; 16]) -> Self {
        entropy[6] = (entropy[6] & 15) | 64;
        entropy[8] = (entropy[8] & 63) | 128;
        Self(entropy)
    }
    /// SQL smallint result; all version nibbles (including 0 and 15) are returned.
    pub const fn extract_version(self) -> Option<i16> {
        if self.0[8] & 192 == 128 {
            Some((self.0[6] >> 4) as i16)
        } else {
            None
        }
    }
    /// SQL timestamptz result. v7 deliberately discards its fractional bits.
    pub fn extract_timestamp(self) -> Option<TimestampTz> {
        let b = self.0;
        let us = match self.extract_version()? {
            1 => {
                let ticks = u64::from(u32::from_be_bytes(b[..4].try_into().unwrap()))
                    | (u64::from(u16::from_be_bytes([b[4], b[5]])) << 32)
                    | (u64::from(u16::from_be_bytes([b[6] & 15, b[7]])) << 48);
                (ticks / 10) as i64 - 13_165_977_600_000_000
            }
            7 => {
                let ms = u64::from_be_bytes([0, 0, b[0], b[1], b[2], b[3], b[4], b[5]]);
                (ms * 1000) as i64 - 946_684_800_000_000
            }
            _ => return None,
        };
        // All v1/v7 encoded timestamps are within PostgreSQL's finite range.
        Some(TimestampTz::from_microseconds(us).expect("UUID timestamp range"))
    }
    /// PostgreSQL hash_bytes for a 16-byte key on little-endian servers.
    pub fn pg_hash_le(self) -> i32 {
        hash(self.0, 0).2 as i32
    }
    /// PostgreSQL hash_bytes_extended for a 16-byte key on little-endian servers.
    pub fn pg_hash_extended_le(self, seed: u64) -> i64 {
        let (_, b, c) = hash(self.0, seed);
        ((u64::from(b) << 32) | u64::from(c)) as i64
    }
}
impl FromStr for Uuid {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        if !(32..=41).contains(&s.len()) {
            return Err(Error::Text);
        }
        let mut input = s.as_bytes();
        if input.first() == Some(&b'{') {
            input = input
                .strip_prefix(b"{")
                .and_then(|v| v.strip_suffix(b"}"))
                .ok_or(Error::Text)?;
        }
        let mut out = [0; 16];
        fn hex(c: u8) -> Result<u8, Error> {
            match c {
                b'0'..=b'9' => Ok(c - b'0'),
                b'a'..=b'f' => Ok(c - b'a' + 10),
                b'A'..=b'F' => Ok(c - b'A' + 10),
                _ => Err(Error::Text),
            }
        }
        for (i, v) in out.iter_mut().enumerate() {
            if input.len() < 2 {
                return Err(Error::Text);
            }
            *v = hex(input[0])? * 16 + hex(input[1])?;
            input = &input[2..];
            if i % 2 == 1 && i < 15 && input.first() == Some(&b'-') {
                input = &input[1..];
            }
        }
        if !input.is_empty() {
            return Err(Error::Text);
        }
        Ok(Self(out))
    }
}
impl fmt::Display for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, b) in self.0.iter().enumerate() {
            if [4, 6, 8, 10].contains(&i) {
                f.write_str("-")?;
            }
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}
// Specialized from PostgreSQL REL_18_0 hashfn.c (reference/COPYRIGHT).
fn mix(mut a: u32, mut b: u32, mut c: u32) -> (u32, u32, u32) {
    a = a.wrapping_sub(c);
    a ^= c.rotate_left(4);
    c = c.wrapping_add(b);
    b = b.wrapping_sub(a);
    b ^= a.rotate_left(6);
    a = a.wrapping_add(c);
    c = c.wrapping_sub(b);
    c ^= b.rotate_left(8);
    b = b.wrapping_add(a);
    a = a.wrapping_sub(c);
    a ^= c.rotate_left(16);
    c = c.wrapping_add(b);
    b = b.wrapping_sub(a);
    b ^= a.rotate_left(19);
    a = a.wrapping_add(c);
    c = c.wrapping_sub(b);
    c ^= b.rotate_left(4);
    b = b.wrapping_add(a);
    (a, b, c)
}
fn hash(bytes: [u8; 16], seed: u64) -> (u32, u32, u32) {
    let mut a = 0x9e3779b9u32.wrapping_add(16).wrapping_add(3923095);
    let mut b = a;
    let mut c = a;
    if seed != 0 {
        a = a.wrapping_add((seed >> 32) as u32);
        b = b.wrapping_add(seed as u32);
        (a, b, c) = mix(a, b, c);
    }
    let w = |i| u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap());
    (a, b, c) = mix(
        a.wrapping_add(w(0)),
        b.wrapping_add(w(4)),
        c.wrapping_add(w(8)),
    );
    a = a.wrapping_add(w(12));
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(14));
    a ^= c;
    a = a.wrapping_sub(c.rotate_left(11));
    b ^= a;
    b = b.wrapping_sub(a.rotate_left(25));
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(16));
    a ^= c;
    a = a.wrapping_sub(c.rotate_left(4));
    b ^= a;
    b = b.wrapping_sub(a.rotate_left(14));
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(24));
    (a, b, c)
}
