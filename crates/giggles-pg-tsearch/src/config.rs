// Based on PostgreSQL 18.6 dict_simple.c, dict_snowball.c, ts_parse.c,
// snowball.sql.in and the built-in pg_ts_config_map data.
// Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group.
use std::{collections::HashSet, sync::LazyLock};

use crate::{Parser, Token, TsError, english, locale, vector::MAX_POS};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Config {
    Simple,
    English,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Dictionary {
    Simple,
    EnglishStem,
}

type StopWords = LazyLock<HashSet<&'static str>>;

static ENGLISH_STOP: StopWords =
    LazyLock::new(|| include_str!("english.stop").split_whitespace().collect());

struct DictionarySpec {
    id: Dictionary,
    name: &'static str,
    oid: u32,
    stem: Option<fn(String) -> String>,
    stop: Option<&'static StopWords>,
}

static DICTIONARIES: &[DictionarySpec] = &[
    DictionarySpec {
        id: Dictionary::Simple,
        name: "simple",
        oid: 3765,
        stem: None,
        stop: None,
    },
    DictionarySpec {
        id: Dictionary::EnglishStem,
        name: "english_stem",
        oid: 13247,
        stem: Some(english::stem),
        stop: Some(&ENGLISH_STOP),
    },
];

struct ConfigSpec {
    id: Config,
    name: &'static str,
    oid: u32,
    letters: Dictionary,
}

static CONFIGS: &[ConfigSpec] = &[
    ConfigSpec {
        id: Config::Simple,
        name: "simple",
        oid: 3748,
        letters: Dictionary::Simple,
    },
    ConfigSpec {
        id: Config::English,
        name: "english",
        oid: 13248,
        letters: Dictionary::EnglishStem,
    },
];

impl Dictionary {
    pub fn oid(self) -> u32 {
        DICTIONARIES[self as usize].oid
    }

    pub fn from_oid(oid: u32) -> Option<Self> {
        DICTIONARIES.iter().find(|spec| spec.oid == oid).map(|spec| spec.id)
    }

    pub fn name(self) -> &'static str {
        DICTIONARIES[self as usize].name
    }

    pub fn lookup(name: &str) -> Result<Self, TsError> {
        if let Some(oid) = numeric_oid(name)
            && let Some(spec) = DICTIONARIES.iter().find(|spec| spec.oid == oid)
        {
            return Ok(spec.id);
        }
        resolve(name, "dictionary", |word| {
            DICTIONARIES
                .iter()
                .find(|spec| spec.name == word)
                .map(|spec| spec.id)
        })
    }

    pub(crate) fn lexize(self, word: &str) -> Option<String> {
        let lower = locale::lower(word);
        if lower.is_empty() {
            return None;
        }
        let spec = &DICTIONARIES[self as usize];
        if spec.stem.is_some() && word.len() > 1000 {
            return Some(lower);
        }
        if spec.stop.is_some_and(|list| list.contains(lower.as_str())) {
            return None;
        }
        Some(if let Some(stem) = spec.stem {
            stem(lower)
        } else {
            lower
        })
    }
}

impl Config {
    pub fn oid(self) -> u32 {
        CONFIGS[self as usize].oid
    }

    pub fn from_oid(oid: u32) -> Option<Self> {
        CONFIGS.iter().find(|spec| spec.oid == oid).map(|spec| spec.id)
    }

    pub fn name(self) -> &'static str {
        CONFIGS[self as usize].name
    }

    /// Whether a canonical name refers to a built-in PostgreSQL 18.6 configuration.
    pub fn is_builtin(name: &str) -> bool {
        CONFIGS.iter().any(|spec| spec.name == name)
            || matches!(
                name,
                "arabic" | "armenian" | "basque" | "catalan" | "danish" | "dutch"
                    | "estonian" | "finnish" | "french" | "german" | "greek" | "hindi"
                    | "hungarian" | "indonesian" | "irish" | "italian" | "lithuanian"
                    | "nepali" | "norwegian" | "portuguese" | "romanian" | "russian"
                    | "serbian" | "spanish" | "swedish" | "tamil" | "turkish" | "yiddish"
            )
    }

    pub fn lookup(name: &str) -> Result<Self, TsError> {
        if let Some(oid) = numeric_oid(name)
            && let Some(config) = Self::from_oid(oid)
        {
            return Ok(config);
        }
        resolve(name, "configuration", |word| {
            if let Some(spec) = CONFIGS.iter().find(|spec| spec.name == word) {
                Some(Ok(spec.id))
            } else if Self::is_builtin(word) {
                Some(Err(TsError::new(
                    "0A000",
                    format!("text search configuration \"{word}\" is not supported"),
                )))
            } else {
                None
            }
        })?
    }

    pub(crate) fn dictionary(self, token_id: u8) -> Option<Dictionary> {
        match token_id {
            12..=14 | 23 => None,
            1 | 2 | 10 | 11 | 16 | 17 => Some(CONFIGS[self as usize].letters),
            _ => Some(Dictionary::Simple),
        }
    }
}

pub(crate) fn lookup_parser(name: &str) -> Result<(), TsError> {
    resolve(name, "parser", |word| (word == "default").then_some(()))
}

fn numeric_oid(input: &str) -> Option<u32> {
    let number = input;
    if !number.as_bytes().first().is_some_and(u8::is_ascii_digit) {
        return None;
    }
    number.parse().ok()
}

fn resolve<T>(name: &str, object: &str, get: impl FnOnce(&str) -> Option<T>) -> Result<T, TsError> {
    let names = qualified_name(name)?;
    let display = names.join(".");
    let missing = || {
        TsError::new(
            "42704",
            format!("text search {object} \"{display}\" does not exist"),
        )
    };
    match names.as_slice() {
        [word] => get(word).ok_or_else(missing),
        [schema, word] if schema == "pg_catalog" => get(word).ok_or_else(missing),
        [schema, _] if schema == "public" => Err(missing()),
        [schema, _] => Err(TsError::new(
            "3F000",
            format!("schema \"{schema}\" does not exist"),
        )),
        [_, _, _] => Err(TsError::new(
            "0A000",
            format!("cross-database references are not implemented: {display}"),
        )),
        _ => Err(TsError::new(
            "42601",
            format!("improper qualified name (too many dotted names): {display}"),
        )),
    }
}

fn qualified_name(mut input: &str) -> Result<Vec<String>, TsError> {
    let invalid = || TsError::new("42602", "invalid name syntax");
    let mut names = Vec::new();
    loop {
        input = input.trim_start_matches(locale::ascii_space);
        if let Some(mut quoted) = input.strip_prefix('"') {
            let mut word = String::new();
            loop {
                let end = quoted.find('"').ok_or_else(invalid)?;
                word.push_str(&quoted[..end]);
                quoted = &quoted[end + 1..];
                if let Some(rest) = quoted.strip_prefix('"') {
                    word.push('"');
                    quoted = rest;
                } else {
                    break;
                }
            }
            if word.is_empty() {
                return Err(invalid());
            }
            names.push(word);
            input = quoted;
        } else {
            let end = input
                .find(|c| locale::ascii_space(c) || c == '.')
                .unwrap_or(input.len());
            if end == 0 {
                return Err(invalid());
            }
            names.push(input[..end].to_ascii_lowercase());
            input = &input[end..];
        }
        input = input.trim_start_matches(locale::ascii_space);
        if input.is_empty() {
            break;
        }
        input = input.strip_prefix('.').ok_or_else(invalid)?;
    }
    for word in &mut names {
        let mut end = 63.min(word.len());
        while !word.is_char_boundary(end) {
            end -= 1;
        }
        word.truncate(end);
    }
    Ok(names)
}

pub fn ts_lexize(dict: Dictionary, word: &str) -> Vec<String> {
    dict.lexize(word).into_iter().collect()
}

/// One row of `ts_debug`. An empty lexeme list denotes a stop word.
#[derive(Debug, Eq, PartialEq)]
pub struct DebugToken<'a> {
    pub token: Token<'a>,
    pub dictionary: Option<Dictionary>,
    pub lexemes: Option<Vec<String>>,
}

pub fn ts_debug(config: Config, text: &str) -> impl Iterator<Item = DebugToken<'_>> {
    Parser::new(text).map(move |token| {
        let dictionary = config.dictionary(token.kind.id);
        let lexemes = dictionary.map(|dict| ts_lexize(dict, token.text));
        DebugToken {
            token,
            dictionary,
            lexemes,
        }
    })
}

pub(crate) fn lexemes(
    config: Config,
    text: &str,
    notices: &mut Vec<TsError>,
) -> Vec<(usize, String)> {
    let mut current = 0;
    let mut words = Vec::new();
    for token in Parser::new(text) {
        if token.text.len() >= 2047 {
            notices.push(TsError::long_token());
            continue;
        }
        if let Some(dict) = config.dictionary(token.kind.id) {
            current += 1;
            if let Some(word) = dict.lexize(token.text) {
                if word.len() > 2047 {
                    notices.push(TsError::long_token());
                } else {
                    words.push((current.min(usize::from(MAX_POS)), word));
                }
            }
        }
    }
    words
}
