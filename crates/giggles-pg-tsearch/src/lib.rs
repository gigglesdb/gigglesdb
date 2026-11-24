//! PostgreSQL 18.6 full-text search for UTF-8 with the en_US locale.
//!
//! `TsVector` and `TsQuery` implement text input via `FromStr`, canonical text
//! output via `Display`, and PostgreSQL's binary format via `send` and `recv`.
//! The APIs take concrete arguments. The SQL caller is responsible for strict
//! NULL propagation, and supplies the configuration for default-config overloads.
//! Errors are returned as `TsError`. Functions that produce PostgreSQL notices
//! return them alongside the value in `SearchResult`.
//!
//! Use `Config::lookup` and `Dictionary::lookup` for names in SQL expressions.
//! `to_tsvector` and the four query builders normalize document or query text.
//! Vector methods cover weights, concatenation, stripping, deletion, filtering,
//! position rows and array conversion. Query methods cover Boolean and phrase
//! operators, containment, node count, and the indexable query tree.
//! `ts_match` evaluates a vector/query pair. The two text helpers perform the
//! corresponding SQL casts using the supplied configuration.
//! `ts_rank` and `ts_rank_cd` take optional one-dimensional weights in D, C, B,
//! A order and a normalization bitmask. `ts_headline` takes PostgreSQL's option
//! string. `ts_parse`, `ts_token_type`, and `ts_debug` expose parser rows.
//!
//! A stripped lexeme has an empty positions slice. Its SQL unnest row has
//! NULL positions and weights. Text input merges duplicate vector lexemes.
//! Binary input retains the duplicate entries and positions admitted by
//! PostgreSQL's receive function.

mod config;
mod english;
mod headline;
mod locale;
mod matching;
mod parser;
mod pg_sort;
mod query;
mod rank;
mod vector;

pub use config::{Config, DebugToken, Dictionary, ts_debug, ts_lexize};
pub use headline::ts_headline;
pub use matching::{ts_match, ts_match_text, ts_match_text_text};
pub use parser::{Parser, Token, TokenType, ts_parse, ts_token_type};
pub use query::{TsQuery, phraseto_tsquery, plainto_tsquery, to_tsquery, websearch_to_tsquery};
pub use rank::{ts_rank, ts_rank_cd};
pub use vector::{Lexeme, Position, TsVector, Weight, to_tsvector};

/// A PostgreSQL error or notice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TsError {
    pub sqlstate: &'static str,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

impl TsError {
    pub(crate) fn new(sqlstate: &'static str, message: impl Into<String>) -> Self {
        Self {
            sqlstate,
            message: message.into(),
            detail: None,
            hint: None,
        }
    }

    pub(crate) fn syntax(kind: &str, input: &str) -> Self {
        Self::new("42601", format!("syntax error in {kind}: \"{input}\""))
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::new("XX000", message)
    }

    pub(crate) fn long_token() -> Self {
        Self {
            sqlstate: "54000",
            message: "word is too long to be indexed".into(),
            detail: Some("Words longer than 2047 characters are ignored.".into()),
            hint: None,
        }
    }
}

impl std::fmt::Display for TsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TsError {}

/// A result whose construction can also emit PostgreSQL notices.
#[derive(Clone, Debug)]
pub struct SearchResult<T> {
    pub value: T,
    pub notices: Vec<TsError>,
}

pub(crate) fn quote(f: &mut std::fmt::Formatter<'_>, word: &str) -> std::fmt::Result {
    use std::fmt::Write;
    f.write_char('\'')?;
    for c in word.chars() {
        if matches!(c, '\'' | '\\') {
            f.write_char(c)?;
        }
        f.write_char(c)?;
    }
    f.write_char('\'')
}

/// An invalid full-text search storage payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Corrupt;

impl std::fmt::Display for Corrupt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid text search storage payload")
    }
}

impl std::error::Error for Corrupt {}

pub(crate) struct Wire<'a>(pub &'a [u8]);

impl<'a> Wire<'a> {
    pub fn read<const N: usize>(&mut self) -> Result<[u8; N], TsError> {
        let (head, rest) = self
            .0
            .split_first_chunk::<N>()
            .ok_or_else(|| TsError::new("08P01", "insufficient data left in message"))?;
        self.0 = rest;
        Ok(*head)
    }

    pub fn string(&mut self) -> Result<&'a str, TsError> {
        let end = self
            .0
            .iter()
            .position(|&c| c == 0)
            .ok_or_else(|| TsError::new("08P01", "invalid string in message"))?;
        let bytes = &self.0[..end];
        let text = std::str::from_utf8(bytes).map_err(|err| {
            let invalid = &bytes[err.valid_up_to()..];
            let length = match invalid[0] {
                0xc0..=0xdf => 2,
                0xe0..=0xef => 3,
                0xf0..=0xf7 => 4,
                _ => 1,
            };
            let hex = invalid
                .iter()
                .take(length)
                .map(|b| format!("0x{b:02x}"))
                .collect::<Vec<_>>()
                .join(" ");
            TsError::new(
                "22021",
                format!("invalid byte sequence for encoding \"UTF8\": {hex}"),
            )
        })?;
        self.0 = &self.0[end + 1..];
        Ok(text)
    }

    pub fn finish(self) -> Result<(), TsError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(TsError::new("22P03", "incorrect binary data format"))
        }
    }
}
