//! Ordered JSON documents with exact numeric text and data-free diagnostics.

use crate::error::{NyCode, Refusal};
use std::collections::HashSet;
use std::fmt;

/// PostgreSQL and Spanner JSON value codecs.
pub mod codec;
/// Policy-directed JSON path filtering.
pub mod filter;
/// Concrete PostgreSQL extraction paths and policy rebasing.
pub mod extract;

/// An owned JSON value preserving member order and duplicate keys.
#[derive(Clone, PartialEq, Eq)]
pub enum JsonValue {
    /// JSON null.
    Null,
    /// JSON boolean.
    Bool(bool),
    /// Validated RFC 8259 number in its exact source spelling.
    Number(String),
    /// Decoded JSON string.
    String(String),
    /// Ordered array elements.
    Array(Vec<JsonValue>),
    /// Ordered object members, including duplicates.
    Object(Vec<(String, JsonValue)>),
}

// Also bound stack use when a partially parsed or caller-built deep tree is
// discarded. Empty each container before its ordinary field destructors run.
impl Drop for JsonValue {
    fn drop(&mut self) {
        fn children(value: &mut JsonValue, pending: &mut Vec<JsonValue>) {
            match value {
                JsonValue::Array(elements) => pending.append(elements),
                JsonValue::Object(members) => {
                    pending.extend(std::mem::take(members).into_iter().map(|(_, v)| v));
                }
                JsonValue::Null
                | JsonValue::Bool(_)
                | JsonValue::Number(_)
                | JsonValue::String(_) => {}
            }
        }
        let mut pending = Vec::new();
        children(self, &mut pending);
        while let Some(mut value) = pending.pop() {
            children(&mut value, &mut pending);
        }
    }
}

impl fmt::Debug for JsonValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("JsonValue::Null"),
            Self::Bool(_) => f.write_str("JsonValue::Bool"),
            Self::Number(_) => f.write_str("JsonValue::Number"),
            Self::String(_) => f.write_str("JsonValue::String"),
            Self::Array(v) => write!(f, "JsonValue::Array({} elements)", v.len()),
            Self::Object(v) => write!(f, "JsonValue::Object({} members)", v.len()),
        }
    }
}

/// Resource limits applied before accepting a JSON document.
#[derive(Debug, Clone, Copy)]
pub struct JsonLimits {
    /// Maximum container nesting, with a root container at depth one.
    pub max_depth: usize,
    /// Maximum input size in bytes.
    pub max_bytes: usize,
}

impl Default for JsonLimits {
    fn default() -> Self {
        Self {
            max_depth: 64,
            max_bytes: 16 * 1024 * 1024,
        }
    }
}

fn refuse() -> Refusal {
    Refusal::new(NyCode::NotAnalysable, "invalid or excessive JSON document")
}

/// Detect duplicate decoded keys in a single object.
pub fn duplicate_keys(members: &[(String, JsonValue)]) -> bool {
    let mut seen = HashSet::new();
    members.iter().any(|(key, _)| !seen.insert(key.as_str()))
}

enum Frame {
    Array(Vec<JsonValue>),
    Object(Vec<(String, JsonValue)>, String),
}

struct Parser<'a> {
    text: &'a str,
    pos: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<u8> {
        self.text.as_bytes().get(self.pos).copied()
    }

    fn whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn take(&mut self, byte: u8) -> Result<(), Refusal> {
        if self.peek() != Some(byte) {
            return Err(refuse());
        }
        self.pos += 1;
        Ok(())
    }

    fn hex4(&mut self) -> Result<u32, Refusal> {
        let mut value = 0;
        for _ in 0..4 {
            let digit = match self.peek() {
                Some(b'0'..=b'9') => u32::from(self.text.as_bytes()[self.pos] - b'0'),
                Some(b'a'..=b'f') => u32::from(self.text.as_bytes()[self.pos] - b'a' + 10),
                Some(b'A'..=b'F') => u32::from(self.text.as_bytes()[self.pos] - b'A' + 10),
                _ => return Err(refuse()),
            };
            value = value * 16 + digit;
            self.pos += 1;
        }
        Ok(value)
    }

    fn string(&mut self) -> Result<String, Refusal> {
        self.take(b'"')?;
        let mut result = String::new();
        loop {
            match self.peek() {
                Some(b'"') => {
                    self.pos += 1;
                    return Ok(result);
                }
                Some(b'\\') => {
                    self.pos += 1;
                    let escape = self.peek().ok_or_else(refuse)?;
                    self.pos += 1;
                    let ch = match escape {
                        b'"' => '"',
                        b'\\' => '\\',
                        b'/' => '/',
                        b'b' => '\u{0008}',
                        b'f' => '\u{000c}',
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        b'u' => {
                            let first = self.hex4()?;
                            let scalar = if (0xd800..=0xdbff).contains(&first) {
                                self.take(b'\\')?;
                                self.take(b'u')?;
                                let second = self.hex4()?;
                                if !(0xdc00..=0xdfff).contains(&second) {
                                    return Err(refuse());
                                }
                                0x10000 + ((first - 0xd800) << 10) + second - 0xdc00
                            } else {
                                first
                            };
                            char::from_u32(scalar).ok_or_else(refuse)?
                        }
                        _ => return Err(refuse()),
                    };
                    result.push(ch);
                }
                Some(0..=0x1f) | None => return Err(refuse()),
                Some(_) => {
                    let ch = self.text[self.pos..].chars().next().ok_or_else(refuse)?;
                    result.push(ch);
                    self.pos += ch.len_utf8();
                }
            }
        }
    }

    fn number(&mut self) -> Result<JsonValue, Refusal> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        match self.peek() {
            Some(b'0') => self.pos += 1,
            Some(b'1'..=b'9') => {
                self.pos += 1;
                while matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.pos += 1;
                }
            }
            _ => return Err(refuse()),
        }
        if self.peek() == Some(b'.') {
            self.pos += 1;
            self.digits()?;
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            self.digits()?;
        }
        Ok(JsonValue::Number(self.text[start..self.pos].to_owned()))
    }

    fn digits(&mut self) -> Result<(), Refusal> {
        let start = self.pos;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.pos += 1;
        }
        if self.pos == start {
            Err(refuse())
        } else {
            Ok(())
        }
    }

    fn literal(&mut self, literal: &str, value: JsonValue) -> Result<JsonValue, Refusal> {
        if !self.text[self.pos..].starts_with(literal) {
            return Err(refuse());
        }
        self.pos += literal.len();
        Ok(value)
    }
}

/// Parse exactly one RFC 8259 value using an explicit container stack.
pub fn parse(bytes: &[u8], limits: &JsonLimits) -> Result<JsonValue, Refusal> {
    if bytes.len() > limits.max_bytes {
        return Err(refuse());
    }
    let mut p = Parser {
        text: std::str::from_utf8(bytes).map_err(|_| refuse())?,
        pos: 0,
    };
    let mut stack = Vec::new();
    loop {
        p.whitespace();
        let mut value = match p.peek() {
            Some(b'[' | b'{') => {
                if stack.len() >= limits.max_depth {
                    return Err(refuse());
                }
                let object = p.peek() == Some(b'{');
                p.pos += 1;
                p.whitespace();
                if object {
                    if p.peek() == Some(b'}') {
                        p.pos += 1;
                        JsonValue::Object(Vec::new())
                    } else {
                        let key = p.string()?;
                        p.whitespace();
                        p.take(b':')?;
                        stack.push(Frame::Object(Vec::new(), key));
                        continue;
                    }
                } else if p.peek() == Some(b']') {
                    p.pos += 1;
                    JsonValue::Array(Vec::new())
                } else {
                    stack.push(Frame::Array(Vec::new()));
                    continue;
                }
            }
            Some(b'"') => JsonValue::String(p.string()?),
            Some(b'-' | b'0'..=b'9') => p.number()?,
            Some(b'n') => p.literal("null", JsonValue::Null)?,
            Some(b't') => p.literal("true", JsonValue::Bool(true))?,
            Some(b'f') => p.literal("false", JsonValue::Bool(false))?,
            _ => return Err(refuse()),
        };
        loop {
            p.whitespace();
            let Some(frame) = stack.pop() else {
                return if p.peek().is_none() {
                    Ok(value)
                } else {
                    Err(refuse())
                };
            };
            match frame {
                Frame::Array(mut elements) => {
                    elements.push(value);
                    match p.peek() {
                        Some(b']') => {
                            p.pos += 1;
                            value = JsonValue::Array(elements);
                        }
                        Some(b',') => {
                            p.pos += 1;
                            stack.push(Frame::Array(elements));
                            break;
                        }
                        _ => return Err(refuse()),
                    }
                }
                Frame::Object(mut members, key) => {
                    members.push((key, value));
                    match p.peek() {
                        Some(b'}') => {
                            p.pos += 1;
                            value = JsonValue::Object(members);
                        }
                        Some(b',') => {
                            p.pos += 1;
                            p.whitespace();
                            let key = p.string()?;
                            p.whitespace();
                            p.take(b':')?;
                            stack.push(Frame::Object(members, key));
                            break;
                        }
                        _ => return Err(refuse()),
                    }
                }
            }
        }
    }
}

fn quote(text: &str, output: &mut String) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    output.push('"');
    for ch in text.chars() {
        match ch {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\u{0008}' => output.push_str("\\b"),
            '\u{000c}' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{0000}'..='\u{001f}' => {
                output.push_str("\\u00");
                output.push(char::from(HEX[(ch as usize) >> 4]));
                output.push(char::from(HEX[(ch as usize) & 15]));
            }
            _ => output.push(ch),
        }
    }
    output.push('"');
}

enum Render<'a> {
    Value(&'a JsonValue),
    Text(&'static str),
    Key(&'a str),
}

fn render(value: &JsonValue, spaced: bool) -> String {
    let mut output = String::new();
    let mut pending = vec![Render::Value(value)];
    let comma = if spaced { ", " } else { "," };
    let colon = if spaced { ": " } else { ":" };
    while let Some(item) = pending.pop() {
        match item {
            Render::Text(text) => output.push_str(text),
            Render::Key(key) => quote(key, &mut output),
            Render::Value(v) => match v {
                JsonValue::Null => output.push_str("null"),
                JsonValue::Bool(b) => output.push_str(if *b { "true" } else { "false" }),
                JsonValue::Number(n) => output.push_str(n),
                JsonValue::String(s) => quote(s, &mut output),
                JsonValue::Array(elements) => {
                    output.push('[');
                    pending.push(Render::Text("]"));
                    for (i, v) in elements.iter().enumerate().rev() {
                        pending.push(Render::Value(v));
                        if i != 0 {
                            pending.push(Render::Text(comma));
                        }
                    }
                }
                JsonValue::Object(members) => {
                    output.push('{');
                    pending.push(Render::Text("}"));
                    for (i, (key, v)) in members.iter().enumerate().rev() {
                        pending.push(Render::Value(v));
                        pending.push(Render::Text(colon));
                        pending.push(Render::Key(key));
                        if i != 0 {
                            pending.push(Render::Text(comma));
                        }
                    }
                }
            },
        }
    }
    output
}

/// Render compact JSON, preserving member order and numeric text.
pub fn render_compact(v: &JsonValue) -> String {
    render(v, false)
}

/// Render JSON with PostgreSQL jsonb text separator spacing.
pub fn render_pg_jsonb(v: &JsonValue) -> String {
    render(v, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(text: &str) -> JsonValue {
        parse(text.as_bytes(), &JsonLimits::default()).unwrap()
    }

    #[test]
    fn value_kinds_and_exact_numbers() {
        for text in [
            "null",
            "true",
            "false",
            "1.0",
            "-0",
            "1e400",
            "12345678901234567890",
            "-12.50E+03",
            "\"héllo🌍\"",
            "[]",
            "{}",
            "[true,null,1,\"x\",{},[]]",
        ] {
            assert_eq!(render_compact(&read(text)), text);
        }
        assert_eq!(read(" \t\r\ntrue \n"), JsonValue::Bool(true));
    }

    #[test]
    fn string_escapes_and_surrogates() {
        let v = read(r#""\"\\\/\b\f\n\r\t\u0000\u001F\u0061\uD83D\uDE00""#);
        assert_eq!(
            v,
            JsonValue::String("\"\\/\u{8}\u{c}\n\r\t\0\u{1f}a😀".to_owned())
        );
        assert_eq!(render_compact(&v), r#""\"\\/\b\f\n\r\t\u0000\u001fa😀""#);
        assert_eq!(read(&render_compact(&v)), v);
    }

    #[test]
    fn malformed_documents_refused() {
        for text in [
            "",
            " ",
            r#""\uD800""#,
            r#""\uDC00""#,
            r#""\uD800\u0061""#,
            r#""\uZZZZ""#,
            r#""\u123""#,
            r#""\v""#,
            "\"\n\"",
            "\"\u{1f}\"",
            "\"unterminated",
            "01",
            "-01",
            "+1",
            "NaN",
            "Infinity",
            "1.",
            "1e",
            "1e+",
            "-",
            ".1",
            "[1,]",
            "{\"a\":1,}",
            "{\"a\" 1}",
            "[1 2]",
            "{1:2}",
            "true false",
            "nullx",
            "[",
            "{",
            "[1",
            "{\"a\":1",
            "\u{b}null",
            "null\u{c}",
        ] {
            assert!(parse(text.as_bytes(), &JsonLimits::default()).is_err());
        }
        assert!(parse(&[b'"', 0xff, b'"'], &JsonLimits::default()).is_err());
    }

    #[test]
    fn depth_and_size_limits() {
        let limits = JsonLimits {
            max_depth: 2,
            max_bytes: 100,
        };
        assert!(parse(br#"[{"a":1}]"#, &limits).is_ok());
        assert!(parse(br#"[{"a":[]}]"#, &limits).is_err());
        assert!(parse(b"[[[]]]", &limits).is_err());
        assert!(
            parse(
                b"[]",
                &JsonLimits {
                    max_depth: 0,
                    max_bytes: 2
                }
            )
            .is_err()
        );
        assert!(
            parse(
                b"0",
                &JsonLimits {
                    max_depth: 0,
                    max_bytes: 1
                }
            )
            .is_ok()
        );
        assert!(
            parse(
                b"null",
                &JsonLimits {
                    max_depth: 0,
                    max_bytes: 4
                }
            )
            .is_ok()
        );
        assert!(
            parse(
                b"null",
                &JsonLimits {
                    max_depth: 0,
                    max_bytes: 3
                }
            )
            .is_err()
        );
        assert!(parse(&vec![b'['; 1_000_000], &JsonLimits::default()).is_err());
    }

    #[test]
    fn duplicates_kept_in_order() {
        let v = read(r#"{"a":1,"b":2,"\u0061":3}"#);
        let JsonValue::Object(members) = &v else {
            panic!("expected object")
        };
        assert!(duplicate_keys(members));
        assert!(!duplicate_keys(&members[..2]));
        assert_eq!(
            members.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            ["a", "b", "a"]
        );
        assert_eq!(render_compact(&v), r#"{"a":1,"b":2,"a":3}"#);
        assert_eq!(render_pg_jsonb(&v), r#"{"a": 1, "b": 2, "a": 3}"#);
    }

    #[test]
    fn debug_and_refusal_are_data_free() {
        let marker = "distinctive_secret_marker";
        for v in [
            read(r#"{"distinctive_secret_marker":"distinctive_secret_marker"}"#),
            JsonValue::String(marker.to_owned()),
            JsonValue::Number(marker.to_owned()),
            JsonValue::Array(vec![JsonValue::String(marker.to_owned())]),
        ] {
            assert!(!format!("{v:?}").contains(marker));
        }
        let err = parse(
            br#"{"distinctive_secret_marker":"distinctive_secret_marker",}"#,
            &JsonLimits::default(),
        )
        .unwrap_err();
        assert_eq!(err.code, NyCode::NotAnalysable);
        assert!(!format!("{err:?} {err}").contains(marker));
    }

    #[test]
    fn rendering_is_iterative() {
        // Deliberately exceed the normal depth limit on a small thread stack.
        std::thread::Builder::new()
            .stack_size(64 * 1024)
            .spawn(|| {
                let depth = 2_000;
                let text = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
                let v = parse(
                    text.as_bytes(),
                    &JsonLimits {
                        max_depth: depth,
                        max_bytes: text.len(),
                    },
                )
                .unwrap();
                assert_eq!(render_compact(&v), text);
                assert_eq!(render_pg_jsonb(&v), text);
                drop(v);
                // Trailing garbage discards the completed deep tree inside parse.
                assert!(
                    parse(
                        format!("{text}x").as_bytes(),
                        &JsonLimits {
                            max_depth: depth,
                            max_bytes: text.len() + 1,
                        }
                    )
                    .is_err()
                );
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
