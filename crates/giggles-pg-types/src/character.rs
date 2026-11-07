//! PostgreSQL character typmods and Unicode text coercion.
//!
//! Semantics follow PostgreSQL REL_18_6 `varchar_input`, `bpchar_input`, and
//! `bcTruelen` in `src/backend/utils/adt/varchar.c`.

use std::{borrow::Cow, cmp::Ordering, fmt};

use crate::typmod::{TypmodError, VARHDRSZ};

/// PostgreSQL's maximum declared character length.
pub const MAX_CHAR_LENGTH: i32 = 10_485_760;

/// The PostgreSQL variable or blank-padded character type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CharKind {
    Varchar,
    Bpchar,
}

impl fmt::Display for CharKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Varchar => "character varying",
            Self::Bpchar => "character",
        })
    }
}

/// Character type modifier, with a length measured in Unicode scalar values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CharTypmod {
    kind: CharKind,
    length: i32,
}

impl CharTypmod {
    /// Validates a declared length, as in `varchartypmodin`/`bpchartypmodin`.
    pub fn new(kind: CharKind, length: i32) -> Result<Self, TypmodError> {
        let type_name = match kind {
            CharKind::Varchar => "varchar",
            CharKind::Bpchar => "char",
        };
        if length < 1 {
            return Err(TypmodError::CharacterLengthTooSmall { type_name });
        }
        if length > MAX_CHAR_LENGTH {
            return Err(TypmodError::CharacterLengthTooLarge {
                type_name,
                max: MAX_CHAR_LENGTH,
            });
        }
        Ok(Self { kind, length })
    }

    pub const fn kind(self) -> CharKind {
        self.kind
    }

    pub const fn length(self) -> i32 {
        self.length
    }

    pub const fn to_typmod(self) -> i32 {
        self.length + VARHDRSZ
    }

    /// Decodes the stored modifier without validating a SQL declaration.
    /// As in PostgreSQL, values below `VARHDRSZ` mean no length modifier.
    pub const fn from_typmod(kind: CharKind, typmod: i32) -> Option<Self> {
        if typmod < VARHDRSZ {
            None
        } else {
            Some(Self {
                kind,
                length: typmod - VARHDRSZ,
            })
        }
    }
}

impl fmt::Display for CharTypmod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({})", self.length)
    }
}

/// Assignment would discard a character other than a trailing ASCII space.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StringTooLong {
    message: String,
}

impl StringTooLong {
    fn new(typmod: CharTypmod) -> Self {
        Self {
            message: format!("value too long for type {}{}", typmod.kind, typmod),
        }
    }

    pub const fn sqlstate(&self) -> &'static str {
        "22001"
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for StringTooLong {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for StringTooLong {}

/// Coerces assignment input following `varchar_input`/`bpchar_input`.
/// Only excess U+0020 spaces may be discarded implicitly.
pub fn coerce_assignment(input: &str, typmod: CharTypmod) -> Result<Cow<'_, str>, StringTooLong> {
    let limit = typmod.length as usize;
    if let Some((boundary, _)) = input.char_indices().nth(limit) {
        if !input[boundary..].bytes().all(|byte| byte == b' ') {
            return Err(StringTooLong::new(typmod));
        }
        return Ok(Cow::Borrowed(&input[..boundary]));
    }
    Ok(pad(input, typmod, limit))
}

/// Explicit character casts silently discard excess Unicode scalar values.
pub fn coerce_explicit(input: &str, typmod: CharTypmod) -> Cow<'_, str> {
    let limit = typmod.length as usize;
    if let Some((boundary, _)) = input.char_indices().nth(limit) {
        Cow::Borrowed(&input[..boundary])
    } else {
        pad(input, typmod, limit)
    }
}

fn pad(input: &str, typmod: CharTypmod, limit: usize) -> Cow<'_, str> {
    if typmod.kind == CharKind::Bpchar {
        let count = input.chars().count();
        if count < limit {
            let mut result = input.to_owned();
            result.extend(std::iter::repeat_n(' ', limit - count));
            return Cow::Owned(result);
        }
    }
    Cow::Borrowed(input)
}

/// Removes trailing U+0020 spaces, matching PostgreSQL `bcTruelen`.
pub fn bpchar_trim(input: &str) -> &str {
    input.trim_end_matches(' ')
}

/// Blank-padded character equality ignores trailing U+0020 spaces.
pub fn bpchar_eq(left: &str, right: &str) -> bool {
    bpchar_trim(left) == bpchar_trim(right)
}

/// Compares trimmed UTF-8 bytes, without applying a locale collation.
pub fn bpchar_cmp(left: &str, right: &str) -> Ordering {
    bpchar_trim(left)
        .as_bytes()
        .cmp(bpchar_trim(right).as_bytes())
}

/// Counts Unicode scalar values after `bcTruelen`-style trimming.
pub fn bpchar_char_length(input: &str) -> usize {
    bpchar_trim(input).chars().count()
}
