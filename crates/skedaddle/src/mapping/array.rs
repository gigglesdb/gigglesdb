use super::{Array, ArrayDimension, MappingError, PgType, Value, invalid};

pub(crate) fn parse(
    element_type: &PgType,
    text: &[u8],
    depth: usize,
) -> Result<Value, MappingError> {
    let mut p = Parser {
        bytes: text,
        pos: 0,
        element_type,
        elements: Vec::new(),
        depth,
    };
    p.ws();
    let mut declared = Vec::new();
    while p.peek() == Some(b'[') {
        p.pos += 1;
        let lo = p.bound(b':')?;
        let hi = p.bound(b']')?;
        let length = u32::try_from(i64::from(hi) - i64::from(lo) + 1)
            .map_err(|_| invalid("array bounds"))?;
        if length == 0 || declared.len() >= 6 {
            return Err(invalid("array dimensions"));
        }
        declared.push(ArrayDimension {
            length,
            lower_bound: lo,
        });
        p.ws();
    }
    if !declared.is_empty() {
        p.take(b'=')?;
    }
    let shape = p.level(0)?;
    p.ws();
    if p.pos != text.len() {
        return Err(invalid("array trailing text"));
    }
    let dimensions = if declared.is_empty() {
        shape
            .into_iter()
            .map(|length| ArrayDimension {
                length,
                lower_bound: 1,
            })
            .collect()
    } else {
        if declared.iter().map(|d| d.length).collect::<Vec<_>>() != shape {
            return Err(invalid("declared array shape mismatch"));
        }
        declared
    };
    let value = Value::Array(Array {
        element_type: Box::new(element_type.clone()),
        dimensions,
        elements: p.elements,
    });
    value.validate(depth)?;
    Ok(value)
}

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
    element_type: &'a PgType,
    elements: Vec<Value>,
    depth: usize,
}
impl Parser<'_> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }
    fn ws(&mut self) {
        while self.peek().is_some_and(|b| b.is_ascii_whitespace()) {
            self.pos += 1;
        }
    }
    fn take(&mut self, b: u8) -> Result<(), MappingError> {
        self.ws();
        if self.peek() != Some(b) {
            return Err(invalid("array punctuation"));
        }
        self.pos += 1;
        Ok(())
    }
    fn bound(&mut self, end: u8) -> Result<i32, MappingError> {
        let start = self.pos;
        while self.peek().is_some_and(|b| b != end) {
            self.pos += 1;
        }
        let n = std::str::from_utf8(&self.bytes[start..self.pos])
            .map_err(|_| invalid("array bound"))?
            .parse()
            .map_err(|_| invalid("array bound"))?;
        self.take(end)?;
        Ok(n)
    }
    fn level(&mut self, level: usize) -> Result<Vec<u32>, MappingError> {
        if level >= 6 || self.depth + level > 32 {
            return Err(invalid("array nesting"));
        }
        self.take(b'{')?;
        self.ws();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            if level > 0 {
                return Err(invalid("nested empty array"));
            }
            return Ok(Vec::new());
        }
        let nested = self.peek() == Some(b'{');
        let mut child_shape = None;
        let mut count = 0_u32;
        loop {
            self.ws();
            if (self.peek() == Some(b'{')) != nested {
                return Err(invalid("mixed array dimensions"));
            }
            if nested {
                let shape = self.level(level + 1)?;
                if child_shape.as_ref().is_some_and(|s| s != &shape) {
                    return Err(invalid("ragged array"));
                }
                child_shape = Some(shape);
            } else {
                let token = self.token()?;
                self.elements.push(super::text::parse(
                    self.element_type,
                    token.as_deref(),
                    self.depth + 1,
                )?);
            }
            count = count.checked_add(1).ok_or_else(|| invalid("array size"))?;
            self.ws();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b'}') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err(invalid("array delimiter")),
            }
        }
        let mut shape = vec![count];
        shape.extend(child_shape.unwrap_or_default());
        Ok(shape)
    }
    fn token(&mut self) -> Result<Option<Vec<u8>>, MappingError> {
        self.ws();
        let quoted = self.peek() == Some(b'"');
        if quoted {
            self.pos += 1;
        }
        let mut out = Vec::new();
        let mut escaped = false;
        loop {
            match self
                .peek()
                .ok_or_else(|| invalid("unterminated array element"))?
            {
                b'"' if quoted => {
                    self.pos += 1;
                    break;
                }
                b',' | b'}' if !quoted => break,
                b'\\' => {
                    escaped = true;
                    self.pos += 1;
                    out.push(self.peek().ok_or_else(|| invalid("array escape"))?);
                    self.pos += 1;
                }
                b'"' | b'{' if !quoted => return Err(invalid("unquoted array metacharacter")),
                c => {
                    out.push(c);
                    self.pos += 1;
                }
            }
        }
        if !quoted {
            while out.last().is_some_and(|b| b.is_ascii_whitespace()) {
                out.pop();
            }
            if out.is_empty() {
                return Err(invalid("empty unquoted array element"));
            }
        }
        if !quoted && !escaped && out.eq_ignore_ascii_case(b"NULL") {
            Ok(None)
        } else {
            Ok(Some(out))
        }
    }
}

pub(crate) fn format(a: &Array, depth: usize) -> Result<Vec<u8>, MappingError> {
    let mut out = Vec::new();
    if a.dimensions.iter().any(|d| d.lower_bound != 1) {
        for d in &a.dimensions {
            out.extend(
                format!(
                    "[{}:{}]",
                    d.lower_bound,
                    i64::from(d.lower_bound) + i64::from(d.length) - 1
                )
                .bytes(),
            );
        }
        out.push(b'=');
    }
    let mut cursor = 0;
    level(a, depth, 0, &mut cursor, &mut out)?;
    Ok(out)
}
fn level(
    a: &Array,
    depth: usize,
    dim: usize,
    cursor: &mut usize,
    out: &mut Vec<u8>,
) -> Result<(), MappingError> {
    out.push(b'{');
    if let Some(d) = a.dimensions.get(dim) {
        for i in 0..d.length {
            if i > 0 {
                out.push(b',');
            }
            if dim + 1 < a.dimensions.len() {
                level(a, depth, dim + 1, cursor, out)?;
            } else {
                let encoded =
                    super::text::format(&a.elements[*cursor], &a.element_type, depth + 1)?;
                *cursor += 1;
                if let Some(bytes) = encoded {
                    out.push(b'"');
                    for c in bytes {
                        if c == b'"' || c == b'\\' {
                            out.push(b'\\');
                        }
                        out.push(c);
                    }
                    out.push(b'"');
                } else {
                    out.extend(b"NULL");
                }
            }
        }
    }
    out.push(b'}');
    Ok(())
}
