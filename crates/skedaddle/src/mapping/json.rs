use super::{MappingError, Numeric, invalid};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// UTF-8 JSONB in semantic normal form: sorted object keys, last duplicate wins,
/// exact decimal numbers normalized without float conversion, minimal whitespace.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Jsonb(String);

impl Jsonb {
    pub fn parse(text: &str) -> Result<Self, MappingError> {
        Ok(Self(parse(text, true)?))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub(crate) fn validate(&self) -> Result<(), MappingError> {
        if parse(&self.0, true)? != self.0 {
            return Err(invalid("noncanonical jsonb"));
        }
        Ok(())
    }
}

pub(crate) fn validate_json(text: &str) -> Result<(), MappingError> {
    parse(text, false).map(|_| ())
}

fn parse(text: &str, jsonb: bool) -> Result<String, MappingError> {
    let mut p = Parser {
        text,
        pos: 0,
        jsonb,
    };
    let out = p.value(0)?;
    p.ws();
    if p.pos != text.len() {
        return Err(invalid("JSON trailing text"));
    }
    Ok(out)
}

struct Parser<'a> {
    text: &'a str,
    pos: usize,
    jsonb: bool,
}
impl Parser<'_> {
    fn ws(&mut self) {
        while self
            .peek()
            .is_some_and(|b| matches!(b, b' ' | b'\n' | b'\r' | b'\t'))
        {
            self.pos += 1;
        }
    }
    fn peek(&self) -> Option<u8> {
        self.text.as_bytes().get(self.pos).copied()
    }
    fn take(&mut self, c: u8) -> Result<(), MappingError> {
        self.ws();
        if self.peek() != Some(c) {
            return Err(invalid("JSON punctuation"));
        }
        self.pos += 1;
        Ok(())
    }
    fn string(&mut self) -> Result<String, MappingError> {
        self.ws();
        let start = self.pos;
        self.take(b'"')?;
        loop {
            match self
                .peek()
                .ok_or_else(|| invalid("unterminated JSON string"))?
            {
                b'"' => {
                    self.pos += 1;
                    break;
                }
                0..=31 => return Err(invalid("JSON control byte")),
                b'\\' => {
                    self.pos += 1;
                    match self.peek().ok_or_else(|| invalid("JSON escape"))? {
                        b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => self.pos += 1,
                        b'u' => {
                            self.pos += 1;
                            for _ in 0..4 {
                                if !self.peek().is_some_and(|b| b.is_ascii_hexdigit()) {
                                    return Err(invalid("JSON Unicode escape"));
                                }
                                self.pos += 1;
                            }
                        }
                        _ => return Err(invalid("JSON escape")),
                    }
                }
                _ => self.pos += 1,
            }
        }
        let token = &self.text[start..self.pos];
        if !self.jsonb {
            return Ok(token.into());
        }
        let s: String = serde_json::from_str(token).map_err(|e| invalid(e.to_string()))?;
        if s.contains('\0') {
            return Err(invalid("jsonb NUL"));
        }
        Ok(s)
    }
    fn value(&mut self, depth: usize) -> Result<String, MappingError> {
        if depth > 64 {
            return Err(invalid("JSON nesting exceeds 64"));
        }
        self.ws();
        match self.peek() {
            Some(b'"') => {
                let s = self.string()?;
                if self.jsonb {
                    serde_json::to_string(&s).map_err(|e| invalid(e.to_string()))
                } else {
                    Ok(s)
                }
            }
            Some(b'{') => {
                self.pos += 1;
                self.ws();
                let mut fields = BTreeMap::new();
                if self.peek() != Some(b'}') {
                    loop {
                        let key = self.string()?;
                        self.take(b':')?;
                        let value = self.value(depth + 1)?;
                        fields.insert(key, value);
                        self.ws();
                        if self.peek() != Some(b',') {
                            break;
                        }
                        self.pos += 1;
                    }
                }
                self.take(b'}')?;
                let mut out = String::from("{");
                for (i, (key, value)) in fields.into_iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    if self.jsonb {
                        out.push_str(
                            &serde_json::to_string(&key).map_err(|e| invalid(e.to_string()))?,
                        );
                    } else {
                        out.push_str(&key);
                    }
                    out.push(':');
                    out.push_str(&value);
                }
                out.push('}');
                Ok(out)
            }
            Some(b'[') => {
                self.pos += 1;
                self.ws();
                let mut values = Vec::new();
                if self.peek() != Some(b']') {
                    loop {
                        values.push(self.value(depth + 1)?);
                        self.ws();
                        if self.peek() != Some(b',') {
                            break;
                        }
                        self.pos += 1;
                    }
                }
                self.take(b']')?;
                Ok(format!("[{}]", values.join(",")))
            }
            Some(b'n' | b't' | b'f') => {
                for literal in ["null", "true", "false"] {
                    if self.text[self.pos..].starts_with(literal) {
                        self.pos += literal.len();
                        return Ok(literal.into());
                    }
                }
                Err(invalid("JSON literal"))
            }
            Some(b'-' | b'0'..=b'9') => self.number(),
            _ => Err(invalid("JSON value")),
        }
    }
    fn digits(&mut self) -> Result<(), MappingError> {
        let start = self.pos;
        while self.peek().is_some_and(|b| b.is_ascii_digit()) {
            self.pos += 1;
        }
        if start == self.pos {
            return Err(invalid("JSON number digits"));
        }
        Ok(())
    }
    fn number(&mut self) -> Result<String, MappingError> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        if self.peek() == Some(b'0') {
            self.pos += 1;
        } else {
            self.digits()?;
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
        let token = &self.text[start..self.pos];
        if self.jsonb {
            Ok(Numeric::parse(token)?.normalized_json().to_string())
        } else {
            Ok(token.into())
        }
    }
}
