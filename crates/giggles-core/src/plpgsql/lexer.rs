//! Source-spanned SQL tokens. The relational lexer discards source positions.
use super::PlCompileError;

#[derive(Debug, PartialEq, Eq)]
enum Kind {
    Word(String),
    Quoted(String),
    String,
    EscapeString,
    DollarString(usize),
    Other,
}

#[derive(Debug)]
pub(super) struct Token<'a> {
    pub start: usize,
    pub raw: &'a str,
    kind: Kind,
}

impl Token<'_> {
    pub fn end(&self) -> usize {
        self.start + self.raw.len()
    }

    pub fn word(&self, keyword: &str) -> bool {
        matches!(&self.kind, Kind::Word(word) if word == keyword)
    }

    pub fn identifier(&self) -> Option<&str> {
        match &self.kind {
            Kind::Word(name) | Kind::Quoted(name) => Some(name),
            _ => None,
        }
    }

    pub fn quoted(&self) -> bool {
        matches!(self.kind, Kind::Quoted(_))
    }

    pub fn string(&self) -> Result<Option<String>, PlCompileError> {
        match self.kind {
            Kind::String => Ok(Some(self.raw[1..self.raw.len() - 1].replace("''", "'"))),
            Kind::EscapeString => crate::relational::plpgsql_string(self.raw)
                .map(Some)
                .map_err(|_| syntax_token(self)),
            Kind::DollarString(tag) => Ok(Some(self.raw[tag..self.raw.len() - tag].into())),
            _ => Ok(None),
        }
    }
}

pub(super) fn error(
    sqlstate: &'static str,
    message: impl Into<String>,
    position: Option<usize>,
) -> PlCompileError {
    PlCompileError {
        sqlstate,
        message: message.into(),
        position,
        hint: None,
    }
}

pub(super) fn syntax_token(token: &Token<'_>) -> PlCompileError {
    error(
        "42601",
        format!("syntax error at or near \"{}\"", token.raw),
        Some(token.start),
    )
}

pub(super) fn eof(position: usize) -> PlCompileError {
    error("42601", "syntax error at end of input", Some(position))
}

fn identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || !byte.is_ascii()
}

fn identifier_continue(byte: u8) -> bool {
    identifier_start(byte) || byte.is_ascii_digit() || byte == b'$'
}

fn quoted_end(bytes: &[u8], start: usize, quote: u8, escapes: bool) -> Option<usize> {
    let mut i = start + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' if escapes => i += 2,
            byte if byte == quote => {
                if bytes.get(i + 1) != Some(&quote) {
                    return Some(i + 1);
                }
                i += 2;
            }
            _ => i += 1,
        }
    }
    None
}

fn dollar_tag(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = start + 1;
    if bytes.get(i) != Some(&b'$') {
        if !bytes.get(i).is_some_and(|b| identifier_start(*b)) {
            return None;
        }
        i += 1;
        while bytes
            .get(i)
            .is_some_and(|b| *b != b'$' && identifier_continue(*b))
        {
            i += 1;
        }
    }
    (bytes.get(i) == Some(&b'$')).then_some(i + 1)
}

pub(super) fn lex(src: &str) -> Result<Vec<Token<'_>>, PlCompileError> {
    let bytes = src.as_bytes();
    let mut i = 0;
    let mut tokens = Vec::new();
    while i < bytes.len() {
        let start = i;
        let rest = &bytes[i..];
        if rest[0].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if rest.starts_with(b"--") {
            i += rest
                .iter()
                .position(|b| matches!(b, b'\n' | b'\r'))
                .unwrap_or(rest.len());
            continue;
        }
        if rest.starts_with(b"/*") {
            i += 2;
            let mut depth = 1;
            while i < bytes.len() && depth != 0 {
                if bytes[i..].starts_with(b"/*") {
                    depth += 1;
                    i += 2;
                } else if bytes[i..].starts_with(b"*/") {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            if depth != 0 {
                return Err(error("42601", "unterminated /* comment", Some(start)));
            }
            continue;
        }
        let kind = match rest[0] {
            b'E' | b'e' if rest.get(1) == Some(&b'\'') => {
                i = quoted_end(bytes, i + 1, b'\'', true)
                    .ok_or_else(|| error("42601", "unterminated quoted string", Some(start)))?;
                Kind::EscapeString
            }
            b'U' | b'u' if rest.get(1..3).is_some_and(|b| b == b"&'" || b == b"&\"") => {
                i = quoted_end(bytes, i + 2, rest[2], false).ok_or_else(|| {
                    error("42601", "unterminated Unicode-quoted token", Some(start))
                })?;
                if rest[2] == b'\'' {
                    Kind::EscapeString
                } else {
                    Kind::Other
                }
            }
            b'B' | b'b' | b'X' | b'x' | b'N' | b'n' if rest.get(1) == Some(&b'\'') => {
                i = quoted_end(bytes, i + 1, b'\'', false)
                    .ok_or_else(|| error("42601", "unterminated quoted string", Some(start)))?;
                Kind::Other
            }
            b'\'' => {
                i = quoted_end(bytes, i, b'\'', false)
                    .ok_or_else(|| error("42601", "unterminated quoted string", Some(start)))?;
                Kind::String
            }
            b'"' => {
                i = quoted_end(bytes, i, b'"', false)
                    .ok_or_else(|| error("42601", "unterminated quoted identifier", Some(start)))?;
                Kind::Quoted(src[start + 1..i - 1].replace("\"\"", "\""))
            }
            b'$' if dollar_tag(bytes, i).is_some() => {
                let end = dollar_tag(bytes, i).expect("dollar tag");
                let tag = &src[i..end];
                i = src[end..]
                    .find(tag)
                    .map(|offset| end + offset + tag.len())
                    .ok_or_else(|| {
                        error("42601", "unterminated dollar-quoted string", Some(start))
                    })?;
                Kind::DollarString(tag.len())
            }
            b'$' if rest.get(1).is_some_and(u8::is_ascii_digit) => {
                i += 2;
                while bytes.get(i).is_some_and(u8::is_ascii_digit) {
                    i += 1;
                }
                Kind::Other
            }
            byte if identifier_start(byte) => {
                i += 1;
                while bytes.get(i).is_some_and(|b| identifier_continue(*b)) {
                    i += 1;
                }
                Kind::Word(src[start..i].to_ascii_lowercase())
            }
            b'0'..=b'9' => {
                i += 1;
                while bytes
                    .get(i)
                    .is_some_and(|b| b.is_ascii_digit() || *b == b'_')
                {
                    i += 1;
                }
                // PL integer FOR uses '..', including when adjacent to a bound.
                if bytes.get(i) == Some(&b'.') && bytes.get(i + 1) != Some(&b'.') {
                    i += 1;
                    while bytes
                        .get(i)
                        .is_some_and(|b| b.is_ascii_digit() || *b == b'_')
                    {
                        i += 1;
                    }
                }
                if matches!(bytes.get(i), Some(b'e' | b'E')) {
                    let mut end = i + 1;
                    if matches!(bytes.get(end), Some(b'+' | b'-')) {
                        end += 1;
                    }
                    if bytes.get(end).is_some_and(u8::is_ascii_digit) {
                        i = end + 1;
                        while bytes
                            .get(i)
                            .is_some_and(|b| b.is_ascii_digit() || *b == b'_')
                        {
                            i += 1;
                        }
                    }
                }
                Kind::Other
            }
            _ => {
                i += if rest.get(..2).is_some_and(|pair| {
                    matches!(
                        pair,
                        b":="
                            | b"::"
                            | b".."
                            | b"<<"
                            | b">>"
                            | b"=>"
                            | b"<="
                            | b">="
                            | b"<>"
                            | b"!="
                            | b"||"
                            | b"->"
                            | b"#>"
                    )
                }) {
                    2
                } else {
                    1
                };
                Kind::Other
            }
        };
        tokens.push(Token {
            start,
            raw: &src[start..i],
            kind,
        });
    }
    Ok(tokens)
}
