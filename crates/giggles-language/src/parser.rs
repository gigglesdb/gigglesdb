use crate::*;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Token {
    Word(String),
    String(String),
    Number(String),
    Param(String),
    Symbol(char),
    Op(Compare),
}
pub(crate) fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.as_bytes()[0].is_ascii_alphabetic()
        && s.bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
        && !reserved(s)
}
fn reserved(s: &str) -> bool {
    [
        "where",
        "order",
        "by",
        "limit",
        "select",
        "insert",
        "into",
        "update",
        "delete",
        "many",
        "claim",
        "when",
        "set",
        "at",
        "most",
        "and",
        "or",
        "not",
        "is",
        "null",
        "true",
        "false",
        "asc",
        "desc",
        "once",
        "atomic",
        "create",
        "alter",
        "drop",
        "show",
        "call",
        "begin",
        "commit",
        "rollback",
        "join",
        "group",
        "having",
        "as",
        "budget",
        "returning",
    ]
    .contains(&s)
}
fn lex(s: &str) -> Result<Vec<(Token, usize)>> {
    if s.len() > MAX_INPUT {
        return Err(Error::Limit);
    }
    let b = s.as_bytes();
    let mut i = 0;
    let mut out = vec![];
    while i < b.len() {
        let start = i;
        if b[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if b[i..].starts_with(b"--") {
            while i < b.len() && b[i] != b'\n' {
                i += 1
            }
            continue;
        }
        let t = match b[i] {
            b'"' => {
                i += 1;
                let mut closed = false;
                while i < b.len() {
                    if b[i] == b'\\' {
                        i += 2;
                    } else if b[i] == b'"' {
                        i += 1;
                        closed = true;
                        break;
                    } else {
                        i += 1
                    }
                }
                if !closed || i > b.len() {
                    return Err(Error::Syntax { offset: start });
                }
                Token::String(
                    serde_json::from_str(&s[start..i])
                        .map_err(|_| Error::Syntax { offset: start })?,
                )
            }
            b'$' => {
                i += 1;
                let from = i;
                if i >= b.len() || !(b[i].is_ascii_alphabetic() || b[i] == b'_') {
                    return Err(Error::Syntax { offset: start });
                }
                i += 1;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                    i += 1
                }
                Token::Param(s[from..i].to_ascii_lowercase())
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                i += 1;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                    i += 1
                }
                Token::Word(s[start..i].to_ascii_lowercase())
            }
            c if c.is_ascii_digit() || c == b'-' => {
                i += 1;
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1
                }
                Token::Number(s[start..i].into())
            }
            b'=' => {
                i += 1;
                Token::Op(Compare::Eq)
            }
            b'!' => {
                if b.get(i + 1) != Some(&b'=') {
                    return Err(Error::Syntax { offset: i });
                }
                i += 2;
                Token::Op(Compare::Ne)
            }
            b'<' | b'>' => {
                let less = b[i] == b'<';
                i += 1;
                let eq = b.get(i) == Some(&b'=');
                if eq {
                    i += 1
                }
                Token::Op(match (less, eq) {
                    (true, true) => Compare::Le,
                    (true, false) => Compare::Lt,
                    (false, true) => Compare::Ge,
                    (false, false) => Compare::Gt,
                })
            }
            b',' | b'(' | b')' | b';' | b'*' => {
                i += 1;
                Token::Symbol(b[start] as char)
            }
            _ => return Err(Error::Syntax { offset: start }),
        };
        out.push((t, start));
        if out.len() > 4096 {
            return Err(Error::Limit);
        }
    }
    Ok(out)
}
struct Parser {
    tokens: Vec<(Token, usize)>,
    pos: usize,
    end: usize,
}
impl Parser {
    fn error(&self) -> Error {
        Error::Syntax {
            offset: self.tokens.get(self.pos).map_or(self.end, |t| t.1),
        }
    }
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos).map(|t| &t.0)
    }
    fn word(&mut self, w: &str) -> bool {
        if self.peek() == Some(&Token::Word(w.into())) {
            self.pos += 1;
            true
        } else {
            false
        }
    }
    fn need(&mut self, w: &str) -> Result<()> {
        if self.word(w) {
            Ok(())
        } else {
            Err(self.error())
        }
    }
    fn symbol(&mut self, c: char) -> bool {
        if self.peek() == Some(&Token::Symbol(c)) {
            self.pos += 1;
            true
        } else {
            false
        }
    }
    fn name(&mut self) -> Result<String> {
        match self.peek().cloned() {
            Some(Token::Word(w)) if identifier(&w) => {
                self.pos += 1;
                Ok(w)
            }
            _ => Err(self.error()),
        }
    }
    fn value(&mut self) -> Result<ValueExpr> {
        let v = match self.peek().cloned() {
            Some(Token::String(s)) => ValueExpr::Literal(Value::Text(s)),
            Some(Token::Number(s)) => {
                ValueExpr::Literal(Value::Int(s.parse().map_err(|_| self.error())?))
            }
            Some(Token::Param(s)) => ValueExpr::Parameter(s),
            Some(Token::Word(s)) if s == "null" => ValueExpr::Literal(Value::Null),
            Some(Token::Word(s)) if s == "true" || s == "false" => {
                ValueExpr::Literal(Value::Bool(s == "true"))
            }
            _ => return Err(self.error()),
        };
        self.pos += 1;
        Ok(v)
    }
    fn expr(&mut self, depth: usize) -> Result<Expr> {
        self.or(depth)
    }
    fn or(&mut self, d: usize) -> Result<Expr> {
        let mut e = self.and(d)?;
        while self.word("or") {
            e = Expr::Or(Box::new(e), Box::new(self.and(d + 1)?))
        }
        Ok(e)
    }
    fn and(&mut self, d: usize) -> Result<Expr> {
        let mut e = self.not(d)?;
        while self.word("and") {
            e = Expr::And(Box::new(e), Box::new(self.not(d + 1)?))
        }
        Ok(e)
    }
    fn not(&mut self, d: usize) -> Result<Expr> {
        if d > MAX_DEPTH {
            return Err(Error::Limit);
        }
        if self.word("not") {
            return Ok(Expr::Not(Box::new(self.not(d + 1)?)));
        }
        let a = self.atom(d)?;
        if let Some(Token::Op(op)) = self.peek().cloned() {
            self.pos += 1;
            return Ok(Expr::Compare(Box::new(a), op, Box::new(self.atom(d + 1)?)));
        }
        if self.word("is") {
            let neg = self.word("not");
            self.need("null")?;
            return Ok(Expr::IsNull(Box::new(a), neg));
        }
        Ok(a)
    }
    fn atom(&mut self, d: usize) -> Result<Expr> {
        if d > MAX_DEPTH {
            return Err(Error::Limit);
        }
        if self.symbol('(') {
            let e = self.expr(d + 1)?;
            if !self.symbol(')') {
                return Err(self.error());
            }
            return Ok(e);
        }
        if let Some(Token::Word(w)) = self.peek()
            && !reserved(w)
        {
            return Ok(Expr::Field(self.name()?));
        }
        Ok(Expr::Value(self.value()?))
    }
    fn filter(&mut self) -> Result<Option<Expr>> {
        if self.word("where") {
            Ok(Some(self.expr(0)?))
        } else {
            Ok(None)
        }
    }
    fn sorts(&mut self) -> Result<Vec<Sort>> {
        let mut v = vec![];
        if self.word("order") {
            self.need("by")?;
            loop {
                let field = self.name()?;
                let descending = self.word("desc");
                if !descending {
                    self.word("asc");
                }
                v.push(Sort { field, descending });
                if !self.symbol(',') {
                    break;
                }
            }
        }
        Ok(v)
    }
    fn assignments(&mut self) -> Result<Vec<(String, ValueExpr)>> {
        self.need("set")?;
        let mut v = vec![];
        loop {
            let n = self.name()?;
            if self.peek() != Some(&Token::Op(Compare::Eq)) {
                return Err(self.error());
            }
            self.pos += 1;
            v.push((n, self.value()?));
            if !self.symbol(',') {
                break;
            }
        }
        Ok(v)
    }
    fn cap(&mut self) -> Result<ValueExpr> {
        self.need("at")?;
        self.need("most")?;
        self.value()
    }
    fn statement(&mut self) -> Result<Statement> {
        if self.word("insert") {
            self.word("into");
            let table = self.name()?;
            return Ok(Statement::Insert {
                table,
                values: self.assignments()?,
            });
        }
        if self.word("update") {
            let many = self.word("many");
            let table = self.name()?;
            self.need("where")?;
            let filter = self.expr(0)?;
            let guard = if self.word("when") {
                Some(self.expr(0)?)
            } else {
                None
            };
            let values = self.assignments()?;
            let intent = if many {
                Intent::Many { cap: self.cap()? }
            } else {
                Intent::One
            };
            return Ok(Statement::Update {
                table,
                filter,
                guard,
                values,
                intent,
            });
        }
        if self.word("delete") {
            let many = self.word("many");
            let table = self.name()?;
            self.need("where")?;
            let filter = self.expr(0)?;
            let guard = if self.word("when") {
                Some(self.expr(0)?)
            } else {
                None
            };
            let cap = if many { Some(self.cap()?) } else { None };
            return Ok(Statement::Delete {
                table,
                filter,
                guard,
                cap,
            });
        }
        if self.word("claim") {
            let table = self.name()?;
            self.need("where")?;
            let filter = self.expr(0)?;
            let order = self.sorts()?;
            if order.is_empty() {
                return Err(self.error());
            }
            let values = self.assignments()?;
            return Ok(Statement::Update {
                table,
                filter,
                guard: None,
                values,
                intent: Intent::Claim { order },
            });
        }
        let table = self.name()?;
        let filter = self.filter()?;
        let order = self.sorts()?;
        let limit = if self.word("limit") {
            Some(self.value()?)
        } else {
            None
        };
        let mut projection = vec![];
        if self.word("select") && !self.symbol('*') {
            loop {
                projection.push(self.name()?);
                if !self.symbol(',') {
                    break;
                }
            }
        }
        Ok(Statement::Read {
            table,
            filter,
            order,
            limit,
            projection,
        })
    }
}
/// Parse exactly one bounded native statement. No execution occurs.
pub fn parse(s: &str) -> Result<Statement> {
    let mut p = Parser {
        tokens: lex(s)?,
        pos: 0,
        end: s.len(),
    };
    let stmt = p.statement()?;
    p.symbol(';');
    if p.peek().is_some() {
        return Err(p.error());
    }
    execute::validate_depth(&stmt)?;
    Ok(stmt)
}
