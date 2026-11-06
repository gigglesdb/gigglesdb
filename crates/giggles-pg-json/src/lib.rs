//! Standalone, bounded UTF-8 PostgreSQL JSON foundations. No database integration.
use giggles_pg_types::Numeric;
use std::ops::Range;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Syntax,
    Encoding,
    Unicode,
    Limit,
    Shape,
    BinaryVersion,
    NullPath,
    Unsupported,
}
pub type Result<T> = std::result::Result<T, Error>;
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub input_bytes: usize,
    pub depth: usize,
    pub nodes: usize,
    pub output_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            input_bytes: 1_048_576,
            depth: 64,
            nodes: 100_000,
            output_bytes: 1_048_576,
        }
    }
}
#[derive(Clone, Debug)]
enum Kind {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<Node>),
    Object(Vec<(String, Node)>),
}
#[derive(Clone, Debug)]
struct Node {
    span: Range<usize>,
    kind: Kind,
}
impl Node {
    fn typeof_(&self) -> &'static str {
        match self.kind {
            Kind::Null => "null",
            Kind::Bool(_) => "boolean",
            Kind::Number(_) => "number",
            Kind::String(_) => "string",
            Kind::Array(_) => "array",
            Kind::Object(_) => "object",
        }
    }
    fn field(&self, key: &str) -> Option<&Node> {
        match &self.kind {
            Kind::Object(v) => v.iter().rev().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }
    fn element(&self, i: i32) -> Option<&Node> {
        match &self.kind {
            Kind::Array(v) => {
                let j = if i < 0 {
                    v.len() as i64 + i as i64
                } else {
                    i as i64
                };
                usize::try_from(j).ok().and_then(|j| v.get(j))
            }
            _ => None,
        }
    }
}
struct Parser<'a> {
    s: &'a str,
    p: usize,
    count: usize,
    l: Limits,
    decode: bool,
    b: bool,
}
impl Parser<'_> {
    fn peek(&self) -> u8 {
        self.s.as_bytes().get(self.p).copied().unwrap_or(0)
    }
    fn ws(&mut self) {
        while matches!(self.peek(), b' ' | b'\n' | b'\r' | b'\t') {
            self.p += 1;
        }
    }
    fn eat(&mut self, b: u8) -> Result<()> {
        if self.peek() != b {
            return Err(Error::Syntax);
        }
        self.p += 1;
        Ok(())
    }
    fn hex(&mut self) -> Result<u16> {
        let end = self.p.checked_add(4).ok_or(Error::Limit)?;
        let s = self.s.get(self.p..end).ok_or(Error::Syntax)?;
        if !s.bytes().all(|c| c.is_ascii_hexdigit()) {
            return Err(Error::Syntax);
        }
        self.p = end;
        u16::from_str_radix(s, 16).map_err(|_| Error::Syntax)
    }
    fn string(&mut self) -> Result<String> {
        self.eat(b'"')?;
        let mut out = String::new();
        loop {
            let c = self.peek();
            match c {
                0..=31 => return Err(Error::Syntax),
                b'"' => {
                    self.p += 1;
                    return Ok(out);
                }
                b'\\' => {
                    self.p += 1;
                    let e = self.peek();
                    self.p += 1;
                    let ch = match e {
                        b'"' => '"',
                        b'\\' => '\\',
                        b'/' => '/',
                        b'b' => '\u{8}',
                        b'f' => '\u{c}',
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        b'u' => {
                            let h = self.hex()?;
                            if !self.decode {
                                continue;
                            }
                            let cp = if (0xd800..=0xdbff).contains(&h) {
                                self.eat(b'\\').map_err(|_| Error::Syntax)?;
                                self.eat(b'u').map_err(|_| Error::Syntax)?;
                                let low = self.hex()?;
                                if !(0xdc00..=0xdfff).contains(&low) {
                                    return Err(Error::Syntax);
                                }
                                0x10000 + ((h as u32 - 0xd800) << 10) + (low as u32 - 0xdc00)
                            } else {
                                h as u32
                            };
                            if cp == 0 {
                                return Err(Error::Unicode);
                            }
                            char::from_u32(cp).ok_or(Error::Syntax)?
                        }
                        _ => return Err(Error::Syntax),
                    };
                    if self.decode {
                        out.push(ch)
                    }
                }
                _ => {
                    let ch = self.s[self.p..].chars().next().ok_or(Error::Syntax)?;
                    self.p += ch.len_utf8();
                    if self.decode {
                        out.push(ch)
                    }
                }
            }
        }
    }
    fn value(&mut self, d: usize) -> Result<Node> {
        self.ws();
        if d > self.l.depth.min(128) || self.count >= self.l.nodes {
            return Err(Error::Limit);
        }
        self.count += 1;
        let start = self.p;
        let kind = match self.peek() {
            b'"' => Kind::String(self.string()?),
            b'[' => {
                self.p += 1;
                self.ws();
                let mut v = Vec::new();
                if self.peek() != b']' {
                    loop {
                        v.push(self.value(d + 1)?);
                        self.ws();
                        if self.peek() != b',' {
                            break;
                        }
                        self.p += 1;
                    }
                }
                self.eat(b']')?;
                Kind::Array(v)
            }
            b'{' => {
                self.p += 1;
                self.ws();
                let mut v = Vec::new();
                if self.peek() != b'}' {
                    loop {
                        self.ws();
                        if self.count >= self.l.nodes {
                            return Err(Error::Limit);
                        }
                        self.count += 1;
                        let k = self.string()?;
                        self.ws();
                        self.eat(b':')?;
                        let val = self.value(d + 1)?;
                        v.push((k, val));
                        self.ws();
                        if self.peek() != b',' {
                            break;
                        }
                        self.p += 1;
                    }
                }
                self.eat(b'}')?;
                if self.b {
                    // Stable sort keeps the last input occurrence of equal decoded keys.
                    v.sort_by(|a, b| {
                        a.0.len()
                            .cmp(&b.0.len())
                            .then(a.0.as_bytes().cmp(b.0.as_bytes()))
                    });
                    let mut unique: Vec<(String, Node)> = Vec::new();
                    for pair in v {
                        if unique.last().is_some_and(|p| p.0 == pair.0) {
                            unique.pop();
                        }
                        unique.push(pair);
                    }
                    v = unique;
                }
                Kind::Object(v)
            }
            b'n' | b't' | b'f' => {
                let (token, k) = match self.peek() {
                    b'n' => ("null", Kind::Null),
                    b't' => ("true", Kind::Bool(true)),
                    _ => ("false", Kind::Bool(false)),
                };
                if !self.s[self.p..].starts_with(token) {
                    return Err(Error::Syntax);
                }
                self.p += token.len();
                k
            }
            b'-' | b'0'..=b'9' => {
                if self.peek() == b'-' {
                    self.p += 1;
                }
                if self.peek() == b'0' {
                    self.p += 1;
                } else {
                    if !matches!(self.peek(), b'1'..=b'9') {
                        return Err(Error::Syntax);
                    }
                    while self.peek().is_ascii_digit() {
                        self.p += 1;
                    }
                }
                if self.peek() == b'.' {
                    self.p += 1;
                    let p = self.p;
                    while self.peek().is_ascii_digit() {
                        self.p += 1;
                    }
                    if p == self.p {
                        return Err(Error::Syntax);
                    }
                }
                if matches!(self.peek(), b'e' | b'E') {
                    self.p += 1;
                    if matches!(self.peek(), b'+' | b'-') {
                        self.p += 1;
                    }
                    let p = self.p;
                    while self.peek().is_ascii_digit() {
                        self.p += 1;
                    }
                    if p == self.p {
                        return Err(Error::Syntax);
                    }
                }
                let raw = &self.s[start..self.p];
                let n = if self.b {
                    Numeric::parse(raw).map_err(|_| Error::Limit)?.to_string()
                } else {
                    raw.to_owned()
                };
                Kind::Number(n)
            }
            _ => return Err(Error::Syntax),
        };
        Ok(Node {
            span: start..self.p,
            kind,
        })
    }
}
fn parse(s: &str, l: Limits, decode: bool, b: bool) -> Result<Node> {
    if s.as_bytes().contains(&0) {
        return Err(Error::Encoding);
    }
    if s.len() > l.input_bytes {
        return Err(Error::Limit);
    }
    let mut p = Parser {
        s,
        p: 0,
        count: 0,
        l,
        decode,
        b,
    };
    let n = p.value(0)?;
    p.ws();
    if p.p != s.len() {
        return Err(Error::Syntax);
    }
    Ok(n)
}
fn push(out: &mut String, s: &str, l: Limits) -> Result<()> {
    if s.len() > l.output_bytes.saturating_sub(out.len()) {
        return Err(Error::Limit);
    }
    out.push_str(s);
    Ok(())
}
fn quote(out: &mut String, s: &str, l: Limits) -> Result<()> {
    push(out, "\"", l)?;
    for c in s.chars() {
        match c {
            '"' => push(out, "\\\"", l)?,
            '\\' => push(out, "\\\\", l)?,
            '\n' => push(out, "\\n", l)?,
            '\r' => push(out, "\\r", l)?,
            '\t' => push(out, "\\t", l)?,
            '\u{8}' => push(out, "\\b", l)?,
            '\u{c}' => push(out, "\\f", l)?,
            c if c < ' ' => push(out, &format!("\\u{:04x}", c as u32), l)?,
            c => {
                let mut b = [0; 4];
                push(out, c.encode_utf8(&mut b), l)?;
            }
        }
    }
    push(out, "\"", l)
}
fn render(n: &Node, out: &mut String, l: Limits) -> Result<()> {
    match &n.kind {
        Kind::Null => push(out, "null", l),
        Kind::Bool(v) => push(out, if *v { "true" } else { "false" }, l),
        Kind::Number(v) => push(out, v, l),
        Kind::String(v) => quote(out, v, l),
        Kind::Array(v) => {
            push(out, "[", l)?;
            for (i, n) in v.iter().enumerate() {
                if i > 0 {
                    push(out, ", ", l)?
                }
                render(n, out, l)?;
            }
            push(out, "]", l)
        }
        Kind::Object(v) => {
            push(out, "{", l)?;
            for (i, (k, n)) in v.iter().enumerate() {
                if i > 0 {
                    push(out, ", ", l)?
                }
                quote(out, k, l)?;
                push(out, ": ", l)?;
                render(n, out, l)?;
            }
            push(out, "}", l)
        }
    }
}
/// JSON preserves the exact input bytes, including duplicate keys and whitespace.
#[derive(Clone, Debug)]
pub struct Json {
    raw: String,
    node: Node,
    limits: Limits,
}
/// JSONB retains normalized numeric display scale and PostgreSQL key order.
#[derive(Clone, Debug)]
pub struct Jsonb {
    node: Node,
    limits: Limits,
}
impl Json {
    pub fn parse(s: &str, l: Limits) -> Result<Self> {
        let node = parse(s, l, false, false)?;
        Ok(Self {
            raw: s.to_owned(),
            node,
            limits: l,
        })
    }
    pub fn receive(bytes: &[u8], l: Limits) -> Result<Self> {
        if bytes.len() > l.input_bytes {
            return Err(Error::Limit);
        }
        Self::parse(std::str::from_utf8(bytes).map_err(|_| Error::Encoding)?, l)
    }
    pub fn text(&self) -> Result<&str> {
        if self.raw.len() > self.limits.output_bytes {
            return Err(Error::Limit);
        }
        Ok(&self.raw)
    }
    pub fn send(&self) -> Result<Vec<u8>> {
        Ok(self.text()?.as_bytes().to_vec())
    }
    pub fn typeof_(&self) -> &'static str {
        self.node.typeof_()
    }
    pub fn array_length(&self) -> Result<usize> {
        match &self.node.kind {
            Kind::Array(v) => Ok(v.len()),
            _ => Err(Error::Shape),
        }
    }
    fn decoded(&self) -> Result<Node> {
        parse(&self.raw, self.limits, true, false)
    }
    pub fn object_keys(&self) -> Result<Vec<String>> {
        keys(&self.decoded()?, self.limits)
    }
    pub fn field(&self, k: &str) -> Result<Option<Self>> {
        self.selected(self.decoded()?.field(k))
    }
    pub fn element(&self, i: i32) -> Result<Option<Self>> {
        self.selected(self.decoded()?.element(i))
    }
    fn selected(&self, n: Option<&Node>) -> Result<Option<Self>> {
        n.map(|n| {
            let slice = &self.raw[n.span.clone()];
            if slice.len() > self.limits.output_bytes {
                return Err(Error::Limit);
            }
            Self::parse(slice, self.limits)
        })
        .transpose()
    }
    pub fn field_text(&self, k: &str) -> Result<Option<String>> {
        self.text_selected(self.decoded()?.field(k))
    }
    pub fn element_text(&self, i: i32) -> Result<Option<String>> {
        self.text_selected(self.decoded()?.element(i))
    }
    fn text_selected(&self, n: Option<&Node>) -> Result<Option<String>> {
        match n {
            None
            | Some(Node {
                kind: Kind::Null, ..
            }) => Ok(None),
            Some(Node {
                kind: Kind::String(s),
                ..
            }) => bounded_string(s, self.limits).map(Some),
            Some(n) => bounded_string(&self.raw[n.span.clone()], self.limits).map(Some),
        }
    }
}
fn bounded_string(s: &str, l: Limits) -> Result<String> {
    if s.len() > l.output_bytes {
        return Err(Error::Limit);
    }
    Ok(s.to_owned())
}
fn keys(n: &Node, l: Limits) -> Result<Vec<String>> {
    match &n.kind {
        Kind::Object(v) => {
            let mut total = 0usize;
            for (k, _) in v {
                total = total.checked_add(k.len()).ok_or(Error::Limit)?;
                if total > l.output_bytes {
                    return Err(Error::Limit);
                }
            }
            Ok(v.iter().map(|(k, _)| k.clone()).collect())
        }
        _ => Err(Error::Shape),
    }
}
impl Jsonb {
    pub fn parse(s: &str, l: Limits) -> Result<Self> {
        Ok(Self {
            node: parse(s, l, true, true)?,
            limits: l,
        })
    }
    pub fn receive(bytes: &[u8], l: Limits) -> Result<Self> {
        if bytes.first() != Some(&1) {
            return Err(Error::BinaryVersion);
        }
        if bytes.len() - 1 > l.input_bytes {
            return Err(Error::Limit);
        }
        Self::parse(
            std::str::from_utf8(&bytes[1..]).map_err(|_| Error::Encoding)?,
            l,
        )
    }
    pub fn text(&self) -> Result<String> {
        let mut out = String::new();
        render(&self.node, &mut out, self.limits)?;
        Ok(out)
    }
    pub fn send(&self) -> Result<Vec<u8>> {
        let text = self.text()?;
        if text.len() >= self.limits.output_bytes {
            return Err(Error::Limit);
        }
        let mut out = Vec::with_capacity(text.len() + 1);
        out.push(1);
        out.extend_from_slice(text.as_bytes());
        Ok(out)
    }
    pub fn typeof_(&self) -> &'static str {
        self.node.typeof_()
    }
    pub fn array_length(&self) -> Result<usize> {
        match &self.node.kind {
            Kind::Array(v) => Ok(v.len()),
            _ => Err(Error::Shape),
        }
    }
    pub fn object_keys(&self) -> Result<Vec<String>> {
        keys(&self.node, self.limits)
    }
    fn array_element(&self, i: i32) -> Option<&Node> {
        match &self.node.kind {
            Kind::Array(_) => self.node.element(i),
            Kind::Object(_) => None,
            _ if i == 0 || i == -1 => Some(&self.node),
            _ => None,
        }
    }
    pub fn field(&self, k: &str) -> Option<JsonbRef<'_>> {
        self.node.field(k).map(|node| JsonbRef {
            node,
            limits: self.limits,
        })
    }
    pub fn element(&self, i: i32) -> Option<JsonbRef<'_>> {
        self.array_element(i).map(|node| JsonbRef {
            node,
            limits: self.limits,
        })
    }
    pub fn field_text(&self, k: &str) -> Result<Option<String>> {
        self.selected_text(self.node.field(k))
    }
    pub fn element_text(&self, i: i32) -> Result<Option<String>> {
        self.selected_text(self.array_element(i))
    }
    fn selected_text(&self, n: Option<&Node>) -> Result<Option<String>> {
        match n {
            None
            | Some(Node {
                kind: Kind::Null, ..
            }) => Ok(None),
            Some(Node {
                kind: Kind::String(s),
                ..
            }) => bounded_string(s, self.limits).map(Some),
            Some(n) => {
                let mut out = String::new();
                render(n, &mut out, self.limits)?;
                Ok(Some(out))
            }
        }
    }
}

/// Borrowed JSONB subtree; selection allocates nothing.
pub struct JsonbRef<'a> {
    node: &'a Node,
    limits: Limits,
}
impl JsonbRef<'_> {
    pub fn text(&self) -> Result<String> {
        let mut out = String::new();
        render(self.node, &mut out, self.limits)?;
        Ok(out)
    }
    pub fn typeof_(&self) -> &'static str {
        self.node.typeof_()
    }
}
impl Error {
    /// PostgreSQL SQLSTATE where the foundation has an exact category.
    pub fn sqlstate(self) -> Option<&'static str> {
        match self {
            Self::NullPath => Some("22004"),
            Self::Syntax => Some("22P02"),
            Self::Encoding => Some("22021"),
            Self::Unicode => Some("22P05"),
            Self::Shape => Some("22023"),
            _ => None,
        }
    }
}
impl Jsonb {
    /// Exact scalar numeric including display scale; nonnumeric values return None.
    pub fn numeric(&self) -> Result<Option<Numeric>> {
        match &self.node.kind {
            Kind::Number(s) => Numeric::parse(s).map(Some).map_err(|_| Error::Limit),
            _ => Ok(None),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unicode_modes_and_limits() {
        let l = Limits::default();
        for s in [r#""\u0000""#, r#""\ud800""#] {
            assert!(Json::parse(s, l).is_ok());
            assert!(Jsonb::parse(s, l).is_err());
        }
        assert_eq!(Jsonb::parse(r#""\ud800""#, l).unwrap_err(), Error::Syntax);
        assert_eq!(Jsonb::parse(r#""\u0000""#, l).unwrap_err(), Error::Unicode);
        let n = Jsonb::parse("{\"a\":1}", l).unwrap();
        assert_eq!(n.field("a").unwrap().typeof_(), "number");
        assert!(Json::parse(" [ ] ", Limits { depth: 0, ..l }).is_ok());
        assert!(Json::parse("[0]", Limits { depth: 0, ..l }).is_err());
    }
    #[test]
    fn deterministic_generated_properties() {
        let l = Limits::default();
        let mut seed = 0x12545678u64;
        for _ in 0..1000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let i = seed as i64;
            let s = format!("{{\"b\":{i}.00, \"a\":[null,true,\"x\\n\"],\"b\":{i}.000}}");
            let b = Jsonb::parse(&s, l).unwrap();
            let canonical = b.text().unwrap();
            assert_eq!(
                Jsonb::parse(&canonical, l).unwrap().text().unwrap(),
                canonical
            );
            assert_eq!(
                Jsonb::receive(&b.send().unwrap(), l)
                    .unwrap()
                    .text()
                    .unwrap(),
                canonical
            );
            let raw = Json::parse(&s, l).unwrap();
            assert_eq!(raw.text().unwrap(), s);
            assert_eq!(
                raw.field("b").unwrap().unwrap().text().unwrap(),
                format!("{i}.000")
            );
            assert_eq!(b.object_keys().unwrap(), ["a", "b"]);
        }
    }
}
pub mod jsonpath;
mod mutation;
mod operators;
