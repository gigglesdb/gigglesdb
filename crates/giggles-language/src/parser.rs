use crate::*;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Token {
    Word(String),
    QuotedIdentifier(String),
    String(String),
    DefaultText(String),
    Number(String),
    Param(String),
    Symbol(char),
    Op(Compare),
    Duration(i64),
}

fn duration_microseconds(text: &str) -> Result<i64> {
    let digits = text.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 {
        return Err(Error::Type);
    }
    let multiplier = match &text[digits..] {
        "us" => 1,
        "ms" => 1_000,
        "s" => 1_000_000,
        "m" => 60 * 1_000_000,
        "h" => 60 * 60 * 1_000_000,
        "d" => 24 * 60 * 60 * 1_000_000,
        _ => return Err(Error::Unsupported),
    };
    text[..digits]
        .parse::<i64>()
        .map_err(|_| Error::Overflow)?
        .checked_mul(multiplier)
        .ok_or(Error::Overflow)
}
pub(crate) fn identifier(s: &str) -> bool {
    canonical_identifier(s) && !reserved(s)
}
fn canonical_identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.as_bytes()[0].is_ascii_alphabetic()
        && s.bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
}
pub(crate) fn semantic_relation(name: &str, version: LanguageVersion) -> bool {
    if let Some((schema, table)) = name.split_once('.') {
        version.supports_schema_names()
            && semantic_identifier(schema, version) && semantic_identifier(table, version)
    } else {
        semantic_identifier(name, version)
    }
}
pub(crate) fn semantic_identifier(s: &str, version: LanguageVersion) -> bool {
    if version.supports_quoted_identifiers() {
        canonical_identifier(s)
    } else {
        identifier(s)
    }
}
fn reserved_for(s: &str, version: LanguageVersion) -> bool {
    reserved(s) || (version.supports_case() && matches!(s, "case" | "then" | "else" | "end"))
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
/// Whether text is an INSERT, which SQL also bounds by bytes and flat tuples.
fn insert_text(s: &str) -> bool {
    let t = s.trim_start().as_bytes();
    t.len() > 6 && t[..6].eq_ignore_ascii_case(b"insert") && t[6].is_ascii_whitespace()
}
fn lex(s: &str, version: LanguageVersion) -> Result<Vec<(Token, usize)>> {
    let insert = insert_text(s);
    let (max_input, max_tokens) = if insert {
        (MAX_INSERT_INPUT.max(MAX_INPUT), MAX_INSERT_TOKENS)
    } else {
        (MAX_INPUT, MAX_TOKENS)
    };
    if s.len() > max_input {
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
            b'`' if version.supports_quoted_identifiers() => {
                i += 1;
                let from = i;
                while i < b.len() && b[i] != b'`' {
                    i += 1;
                }
                if i == b.len() {
                    return Err(Error::Syntax { offset: start });
                }
                let name = &s[from..i];
                i += 1;
                if name.len() > 63 {
                    return Err(Error::Limit);
                }
                if !canonical_identifier(name) || b.get(i) == Some(&b'`') {
                    return Err(Error::Unsupported);
                }
                Token::QuotedIdentifier(name.to_owned())
            }
            b'\'' if version.supports_r2_wave3() => {
                i += 1;
                let mut text = String::new();
                let mut from = i;
                loop {
                    if i == b.len() { return Err(Error::Syntax { offset: start }); }
                    if b[i] == b'\'' {
                        text.push_str(&s[from..i]); i += 1;
                        if b.get(i) != Some(&b'\'') { break; }
                        text.push('\''); i += 1; from = i;
                    } else { i += 1; }
                }
                Token::DefaultText(text)
            }
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
            c if c.is_ascii_digit() || (c == b'-' && version == LanguageVersion::Embedded01) => {
                i += 1;
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1
                }
                let mut end = i;
                if version.supports_once() {
                    while end < b.len() && (b[end].is_ascii_alphanumeric() || b[end] == b'_') {
                        end += 1;
                    }
                }
                let unit = &s[i..end];
                if version.supports_once()
                    && ["us", "ms", "s", "m", "h", "d"]
                        .iter()
                        .any(|expected| unit.eq_ignore_ascii_case(expected))
                {
                    // Units are case-sensitive exceptions to keyword folding.
                    // Other adjacent words (e.g. `1and`) keep the old lexer path.
                    let duration = duration_microseconds(&s[start..end])?;
                    i = end;
                    Token::Duration(duration)
                } else {
                    Token::Number(s[start..i].into())
                }
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
            b'+' | b'-' | b'/' | b'%' if version != LanguageVersion::Embedded01 => {
                i += 1;
                Token::Symbol(b[start] as char)
            }
            b'@' | b'.' | b'{' | b'}' if version.supports_transactions() => {
                i += 1;
                Token::Symbol(b[start] as char)
            }
            _ => return Err(Error::Syntax { offset: start }),
        };
        out.push((t, start));
        if out.len() > max_tokens {
            return Err(Error::Limit);
        }
    }
    Ok(out)
}
struct Parser {
    tokens: Vec<(Token, usize)>,
    pos: usize,
    end: usize,
    version: LanguageVersion,
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
            Some(Token::Word(w)) if canonical_identifier(&w) && !reserved_for(&w, self.version) => {
                self.pos += 1;
                Ok(w)
            }
            Some(Token::QuotedIdentifier(w)) if self.version.supports_quoted_identifiers() => {
                self.pos += 1;
                Ok(w)
            }
            _ => Err(self.error()),
        }
    }
    fn relation_name(&mut self) -> Result<String> {
        let first = self.name()?;
        if self.version.supports_schema_names() && self.symbol('.') {
            Ok(format!("{first}.{}", self.name()?))
        } else {
            Ok(first)
        }
    }
    fn variable_name(&mut self) -> Result<String> {
        if !self.version.supports_case() {
            return self.name();
        }
        match self.peek().cloned() {
            Some(Token::Word(name)) if canonical_identifier(&name) => {
                self.pos += 1;
                Ok(name)
            }
            _ => Err(self.error()),
        }
    }
    fn value(&mut self) -> Result<ValueExpr> {
        // Value-only clauses still accept signed integer literals in 0.2+;
        // they do not gain computed expressions or unary parameter operators.
        if self.version != LanguageVersion::Embedded01 && self.symbol('-') {
            let Some(Token::Number(s)) = self.peek() else {
                return Err(self.error());
            };
            let value = format!("-{s}").parse().map_err(|_| self.error())?;
            self.pos += 1;
            return Ok(ValueExpr::Literal(Value::Int(value)));
        }
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
        let a = self.comparison_operand(d)?;
        if let Some(Token::Op(op)) = self.peek().cloned() {
            self.pos += 1;
            return Ok(Expr::Compare(
                Box::new(a),
                op,
                Box::new(self.comparison_operand(d + 1)?),
            ));
        }
        if self.word("is") {
            let neg = self.word("not");
            self.need("null")?;
            return Ok(Expr::IsNull(Box::new(a), neg));
        }
        Ok(a)
    }
    fn comparison_operand(&mut self, d: usize) -> Result<Expr> {
        match self.version {
            LanguageVersion::Embedded01 => self.atom(d),
            LanguageVersion::Embedded02
            | LanguageVersion::Embedded03
            | LanguageVersion::Embedded04
            | LanguageVersion::Embedded05
            | LanguageVersion::Embedded06
            | LanguageVersion::Embedded07
            | LanguageVersion::Embedded08
            | LanguageVersion::Embedded09
            | LanguageVersion::Embedded010
            | LanguageVersion::Embedded011
            | LanguageVersion::Embedded012
            | LanguageVersion::Embedded013
            | LanguageVersion::Embedded014
            | LanguageVersion::Embedded015 | LanguageVersion::Embedded016 | LanguageVersion::Embedded017 | LanguageVersion::Embedded018 | LanguageVersion::Embedded019 | LanguageVersion::Embedded020 | LanguageVersion::Embedded021 => self.sum(d),
        }
    }
    fn sum(&mut self, d: usize) -> Result<Expr> {
        let mut e = self.product(d)?;
        loop {
            let op = if self.symbol('+') {
                Arithmetic::Add
            } else if self.symbol('-') {
                Arithmetic::Subtract
            } else {
                break;
            };
            e = Expr::Arithmetic(Box::new(e), op, Box::new(self.product(d + 1)?));
        }
        Ok(e)
    }
    fn product(&mut self, d: usize) -> Result<Expr> {
        let mut e = self.unary(d)?;
        loop {
            let op = if self.symbol('*') {
                Arithmetic::Multiply
            } else if self.symbol('/') {
                Arithmetic::Divide
            } else if self.symbol('%') {
                Arithmetic::Remainder
            } else {
                break;
            };
            e = Expr::Arithmetic(Box::new(e), op, Box::new(self.unary(d + 1)?));
        }
        Ok(e)
    }
    fn unary(&mut self, d: usize) -> Result<Expr> {
        if d > MAX_DEPTH {
            return Err(Error::Limit);
        }
        if self.symbol('-') {
            // The positive magnitude of MIN is outside Value::Int. Accept it
            // only as this signed literal, without wrapping any other literal.
            if matches!(self.peek(), Some(Token::Number(s)) if s.parse::<u64>() == Ok(1u64 << 63)) {
                self.pos += 1;
                return Ok(Expr::Value(ValueExpr::Literal(Value::Int(i64::MIN))));
            }
            return Ok(Expr::Negate(Box::new(self.unary(d + 1)?)));
        }
        self.atom(d)
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
        if self.version.supports_case() && self.word("case") {
            let operand = if matches!(self.peek(), Some(Token::Word(word)) if word == "when") {
                None
            } else {
                Some(Box::new(self.expr(d + 1)?))
            };
            let mut arms = Vec::new();
            while self.word("when") {
                if arms.len() >= MAX_CASE_ARMS {
                    return Err(Error::Limit);
                }
                let condition = self.expr(d + 1)?;
                self.need("then")?;
                let result = self.expr(d + 1)?;
                arms.push((condition, result));
            }
            execute::validate_case(arms.len())?;
            let else_expr = if self.word("else") {
                Some(Box::new(self.expr(d + 1)?))
            } else {
                None
            };
            self.need("end")?;
            return Ok(Expr::Case {
                operand,
                arms,
                else_expr,
            });
        }
        if self.symbol('@') {
            let local = self.variable_name()?;
            if !self.symbol('.') {
                return Err(self.error());
            }
            let reference = if self.word("row") {
                if !self.symbol('.') {
                    return Err(self.error());
                }
                LocalRef::Field {
                    local,
                    field: self.name()?,
                }
            } else if self.word("status") {
                LocalRef::Status { local }
            } else if self.word("changed") {
                LocalRef::Changed { local }
            } else if self.word("count") {
                LocalRef::Count { local }
            } else {
                return Err(self.error());
            };
            return Ok(Expr::Local(reference));
        }
        if matches!(self.peek(), Some(Token::QuotedIdentifier(_))) {
            return Ok(Expr::Field(self.name()?));
        }
        if self.version.supports_upsert() {
            let source = match &self.tokens[self.pos..] {
                [(Token::Word(name), _), (Token::Symbol('.'), _), ..] => match name.as_str() {
                    "existing" => Some(ConflictSource::Existing),
                    "incoming" => Some(ConflictSource::Incoming),
                    _ => None,
                },
                _ => None,
            };
            if let Some(source) = source {
                self.pos += 2;
                return Ok(Expr::ConflictField {
                    source,
                    field: self.name()?,
                });
            }
        }
        let function = if self.version.supports_scalar_functions() {
            match &self.tokens[self.pos..] {
                [(Token::Word(name), _), (Token::Symbol('('), _), ..] => match name.as_str() {
                    "coalesce" => Some(ScalarFunction::Coalesce),
                    "abs" => Some(ScalarFunction::Abs),
                    "length" => Some(ScalarFunction::Length),
                    _ => None,
                },
                _ => None,
            }
        } else {
            None
        };
        if let Some(function) = function {
            self.pos += 2;
            let mut arguments = Vec::new();
            if !self.symbol(')') {
                loop {
                    if arguments.len() >= MAX_FUNCTION_ARGUMENTS {
                        return Err(Error::Limit);
                    }
                    arguments.push(self.expr(d + 1)?);
                    if !self.symbol(',') {
                        if !self.symbol(')') {
                            return Err(self.error());
                        }
                        break;
                    }
                }
            }
            execute::validate_call(function, arguments.len())?;
            return Ok(Expr::Call {
                function,
                arguments,
            });
        }
        if let Some(Token::Word(w)) = self.peek()
            && !reserved_for(w, self.version)
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
    fn update_assignments(&mut self) -> Result<Vec<(String, Expr)>> {
        if !self.version.supports_computed() {
            return Ok(self
                .assignments()?
                .into_iter()
                .map(|(name, value)| (name, Expr::Value(value)))
                .collect());
        }
        self.need("set")?;
        let mut values = Vec::new();
        loop {
            let name = self.name()?;
            if self.peek() != Some(&Token::Op(Compare::Eq)) {
                return Err(self.error());
            }
            self.pos += 1;
            // Preserve the legacy signed-literal/parameter AST at assignment
            // boundaries. Actual arithmetic goes through the expression parser
            // and is never constant-folded here or during preparation.
            let start = self.pos;
            let literal = self.value().ok().filter(|_| {
                matches!(self.peek(), None | Some(Token::Symbol(',' | ';')))
                    || (self.version.supports_transactions()
                        && matches!(self.peek(), Some(Token::Symbol(')' | '}'))))
                    || self.peek() == Some(&Token::Word("at".into()))
                    || (self.version.supports_return()
                        && self.peek() == Some(&Token::Word("return".into())))
                    || (self.version.supports_once()
                        && self.peek() == Some(&Token::Word("once".into())))
            });
            let expression = if let Some(value) = literal {
                Expr::Value(value)
            } else {
                self.pos = start;
                self.expr(0)?
            };
            values.push((name, expression));
            if !self.symbol(',') {
                break;
            }
        }
        Ok(values)
    }
    fn update(
        table: String,
        filter: Expr,
        guard: Option<Expr>,
        values: Vec<(String, Expr)>,
        intent: Intent,
    ) -> Statement {
        if values
            .iter()
            .all(|(_, expr)| matches!(expr, Expr::Value(_)))
        {
            Statement::Update {
                table,
                filter,
                guard,
                values: values
                    .into_iter()
                    .map(|(name, expr)| match expr {
                        Expr::Value(value) => (name, value),
                        _ => unreachable!("value-only assignments"),
                    })
                    .collect(),
                intent,
            }
        } else {
            Statement::UpdateExpressions {
                table,
                filter,
                guard,
                values,
                intent,
            }
        }
    }
    fn cap(&mut self) -> Result<ValueExpr> {
        self.need("at")?;
        self.need("most")?;
        self.value()
    }
    fn projection(&mut self) -> Result<Vec<Projection>> {
        if self.symbol('*') {
            return Ok(Vec::new());
        }
        let mut projection = Vec::new();
        loop {
            let expression = if self.version.supports_computed() {
                self.expr(0)?
            } else {
                Expr::Field(self.name()?)
            };
            let alias = if self.version.supports_computed() && self.word("as") {
                Some(self.name()?)
            } else {
                None
            };
            if alias.is_none() && !matches!(expression, Expr::Field(_)) {
                return Err(self.error());
            }
            projection.push(Projection { expression, alias });
            if !self.symbol(',') {
                break;
            }
        }
        Ok(projection)
    }
    fn statement(&mut self) -> Result<Statement> {
        let statement = self.base_statement()?;
        self.return_clause(statement)
    }
    fn return_clause(&mut self, statement: Statement) -> Result<Statement> {
        if self.version.supports_return() && self.word("return") {
            if !matches!(
                statement,
                Statement::Insert { .. }
                    | Statement::InsertExpressions { .. }
                    | Statement::InsertMany { .. }
                    | Statement::OnConflict { .. }
                    | Statement::Update { .. }
                    | Statement::UpdateExpressions { .. }
                    | Statement::Delete { .. }
            ) {
                return Err(self.error());
            }
            return Ok(Statement::Returning {
                statement: Box::new(statement),
                projection: self.projection()?,
            });
        }
        Ok(statement)
    }
    fn once_key(&mut self) -> Result<ValueExpr> {
        if matches!(self.peek(), Some(Token::String(text)) if text.len() > giggles_core::operations::once::MAX_ONCE_KEY_BYTES)
        {
            return Err(Error::Limit);
        }
        self.value()
    }
    fn retention(&mut self) -> Result<DurationLiteral> {
        let microseconds = match self.peek() {
            Some(Token::Duration(microseconds)) => {
                let microseconds = *microseconds;
                self.pos += 1;
                microseconds
            }
            Some(Token::Word(word)) if word == "duration" => {
                self.pos += 1;
                if !self.symbol('(') {
                    return Err(self.error());
                }
                let Some(Token::String(text)) = self.peek() else {
                    return Err(self.error());
                };
                let microseconds = duration_microseconds(text)?;
                self.pos += 1;
                if !self.symbol(')') {
                    return Err(self.error());
                }
                microseconds
            }
            Some(Token::Param(_)) => return Err(Error::Unsupported),
            _ => return Err(Error::Type),
        };
        let duration = DurationLiteral { microseconds };
        once::retention_seconds(&duration)?;
        Ok(duration)
    }
    fn once_header(&mut self) -> Result<(ValueExpr, DurationLiteral)> {
        self.need("once")?;
        let key = self.once_key()?;
        self.need("for")?;
        Ok((key, self.retention()?))
    }
    fn statement_input(&mut self) -> Result<ParsedInput> {
        let statement = self.base_statement()?;
        if self.version.supports_once() && self.peek() == Some(&Token::Word("once".into())) {
            let (key, retention) = self.once_header()?;
            let body = OnceBody::Statement(self.return_clause(statement)?);
            let envelope = OnceEnvelope {
                language_version: self.version,
                key,
                retention,
                body,
            };
            once::validate_once(&envelope, self.version)?;
            Ok(ParsedInput::Once(envelope))
        } else {
            let statement = self.return_clause(statement)?;
            execute::validate_depth(&statement)?;
            Ok(ParsedInput::Statement(statement))
        }
    }
    fn outcome_prefix(&self) -> bool {
        if !self.version.supports_once() || self.peek() != Some(&Token::Word("outcome".into())) {
            return false;
        }
        matches!(
            self.tokens.get(self.pos + 1).map(|token| &token.0),
            Some(Token::String(_) | Token::Param(_) | Token::Number(_) | Token::Symbol('-'))
        ) || matches!(self.tokens.get(self.pos + 1), Some((Token::Word(word), _)) if matches!(word.as_str(), "null" | "true" | "false"))
    }
    fn outcome(&mut self) -> Result<OutcomeQuery> {
        self.need("outcome")?;
        let query = OutcomeQuery {
            language_version: self.version,
            key: self.once_key()?,
        };
        once::validate_outcome(&query, self.version)?;
        Ok(query)
    }
    fn conflict_clause(&mut self) -> Result<Option<(Vec<String>, ConflictAction)>> {
        if !self.version.supports_upsert() || !self.word("on") {
            return Ok(None);
        }
        self.need("conflict")?;
        if !self.symbol('(') {
            return Err(self.error());
        }
        let mut target = vec![self.name()?];
        while self.symbol(',') {
            if target.len() >= giggles_core::relational::MAX_COLUMNS {
                return Err(Error::Limit);
            }
            target.push(self.name()?);
        }
        if !self.symbol(')') {
            return Err(self.error());
        }
        self.need("do")?;
        let action = if self.word("nothing") {
            ConflictAction::Nothing
        } else {
            self.need("update")?;
            let guard = if self.word("when") {
                Some(self.expr(0)?)
            } else {
                None
            };
            let values = self.update_assignments()?;
            ConflictAction::Update { guard, values }
        };
        Ok(Some((target, action)))
    }
    fn base_statement(&mut self) -> Result<Statement> {
        if self.word("insert") {
            let many = self.version.supports_insert_many() && self.word("many");
            let into = if many { self.need("into")?; true } else { self.word("into") };
            let table = self.relation_name()?;
            if !many && self.version.supports_r2_native() {
                let start = self.pos;
                let mut columns = Vec::new();
                if self.symbol('(') {
                    columns.push(self.name()?);
                    while self.symbol(',') { columns.push(self.name()?); }
                    if !self.symbol(')') { return Err(self.error()); }
                }
                if self.word("from") {
                    if !into { return Err(self.error()); }
                    let source = Box::new(self.query()?);
                    if !self.query_end() { return Err(self.error()); }
                    return Ok(Statement::InsertSelect { table, columns, source });
                }
                self.pos = start;
            }
            if self.version.supports_transactions() && self.symbol('(') {
                let mut names = vec![self.name()?];
                while self.symbol(',') {
                    if many && names.len() >= giggles_core::relational::MAX_COLUMNS {
                        return Err(Error::Limit);
                    }
                    names.push(self.name()?);
                }
                if !self.symbol(')') {
                    return Err(self.error());
                }
                self.need("values")?;
                let mut rows = Vec::new();
                loop {
                    if !self.symbol('(') {
                        return Err(self.error());
                    }
                    let mut expressions = vec![self.expr(0)?];
                    while self.symbol(',') {
                        if many && expressions.len() >= giggles_core::relational::MAX_COLUMNS {
                            return Err(Error::Limit);
                        }
                        expressions.push(self.expr(0)?);
                    }
                    if !self.symbol(')') || names.len() != expressions.len() {
                        return Err(self.error());
                    }
                    rows.push(expressions);
                    if !many || !self.symbol(',') {
                        break;
                    }
                    // Flat tuples, as SQL VALUES; engines admit row counts.
                    if rows.len() >= MAX_INSERT_TOKENS {
                        return Err(Error::Limit);
                    }
                }
                let conflict = self.conflict_clause()?;
                let statement = if many {
                    Statement::InsertMany {
                        table,
                        columns: names,
                        rows,
                        cap: self.cap()?,
                    }
                } else {
                    Statement::InsertExpressions {
                        table,
                        values: names
                            .into_iter()
                            .zip(rows.pop().expect("one VALUES tuple"))
                            .collect(),
                    }
                };
                return Ok(if let Some((target, action)) = conflict {
                    Statement::OnConflict {
                        statement: Box::new(statement),
                        target,
                        action,
                    }
                } else {
                    statement
                });
            }
            if many {
                return Err(self.error());
            }
            return Ok(Statement::Insert {
                table,
                values: self.assignments()?,
            });
        }
        if self.word("update") {
            let many = self.word("many");
            let table = self.relation_name()?;
            self.need("where")?;
            let filter = self.expr(0)?;
            let guard = if self.word("when") {
                Some(self.expr(0)?)
            } else {
                None
            };
            let values = self.update_assignments()?;
            let intent = if many {
                Intent::Many { cap: self.cap()? }
            } else {
                Intent::One
            };
            return Ok(Self::update(table, filter, guard, values, intent));
        }
        if self.word("delete") {
            let many = self.word("many");
            let table = self.relation_name()?;
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
            let table = self.relation_name()?;
            self.need("where")?;
            let filter = self.expr(0)?;
            let order = self.sorts()?;
            if order.is_empty() {
                return Err(self.error());
            }
            let values = self.update_assignments()?;
            return Ok(Self::update(
                table,
                filter,
                None,
                values,
                Intent::Claim { order },
            ));
        }
        if self.version.supports_r2_native() {
            let start = self.pos;
            if let Ok(read) = self.legacy_read() && self.query_end() { return Ok(read); }
            self.pos = start;
            let query = self.query()?;
            if !self.query_end() { return Err(self.error()); }
            return Ok(Statement::Query(Box::new(query)));
        }
        self.legacy_read()
    }
    fn query_end(&self) -> bool {
        matches!(self.peek(), None | Some(Token::Symbol(';' | '}' | ')')))
    }
    fn legacy_read(&mut self) -> Result<Statement> {
        self.page_read(false).map(|(read, _)| read)
    }
    fn page_read(&mut self, page: bool) -> Result<(Statement, Option<Vec<ValueExpr>>)> {
        // A read clause disambiguates SHOW only when the profile permits its identifier.
        let table = if semantic_identifier("show", self.version)
            && matches!(self.peek(), Some(Token::Word(word)) if word == "show")
            && matches!(self.tokens.get(self.pos + 1), Some((Token::Word(next), _)) if matches!(next.as_str(), "where" | "order" | "limit" | "select"))
        {
            self.pos += 1;
            "show".into()
        } else {
            self.relation_name()?
        };
        let filter = self.filter()?;
        let order = self.sorts()?;
        let limit = if self.word("limit") {
            Some(self.value()?)
        } else {
            None
        };
        let after = if page && self.word("after") {
            if !self.symbol('(') { return Err(self.error()); }
            let mut values = vec![self.value()?];
            while self.symbol(',') { values.push(self.value()?); }
            if !self.symbol(')') { return Err(self.error()); }
            Some(values)
        } else { None };
        let projection = if self.word("select") { self.projection()? } else { Vec::new() };
        if page && projection.iter().any(|p| p.alias.is_some()) { return Err(Error::Unsupported); }
        if projection.iter().any(|p| p.alias.is_some()) {
            return Ok((Statement::ReadExpressions {
                table, filter, order, limit, projection,
            }, after));
        }
        Ok((Statement::Read {
            table,
            filter,
            order,
            limit,
            projection: projection
                .into_iter()
                .map(|p| match p.expression {
                    Expr::Field(name) => name,
                    _ => unreachable!("unaliased field projections"),
                })
                .collect(),
        }, after))
    }
    fn query_source(&mut self) -> Result<QuerySource> {
        let table = self.relation_name()?;
        let alias = if matches!(self.peek(), Some(Token::Word(w)) if identifier(w) && !matches!(w.as_str(), "inner" | "left" | "on" | "having")) { Some(self.name()?) } else { None };
        Ok(QuerySource { table, alias })
    }
    fn query_expr(&mut self) -> Result<QueryExpr> {
        let start = self.pos;
        if let Ok(value) = self.value() { return Ok(QueryExpr::Value(value)); }
        self.pos = start;
        let name = self.name()?;
        if self.symbol('(') {
            let function = match name.as_str() {
                "count" => AggregateFunction::Count, "sum" => AggregateFunction::Sum,
                "min" => AggregateFunction::Min, "max" => AggregateFunction::Max,
                "avg" => AggregateFunction::Avg, _ => return Err(self.error()),
            };
            let (source, column) = if function == AggregateFunction::Count && self.symbol('*') { (None, None) }
                else { let (source, column) = self.query_column()?; (source, Some(column)) };
            if !self.symbol(')') { return Err(self.error()); }
            return Ok(QueryExpr::Aggregate { function, source, column });
        }
        let (source, column) = if self.symbol('.') { (Some(name), self.name()?) } else { (None, name) };
        Ok(QueryExpr::Column { source, column })
    }
    fn query_column(&mut self) -> Result<(Option<String>, String)> {
        let name = self.name()?;
        Ok(if self.symbol('.') { (Some(name), self.name()?) } else { (None, name) })
    }
    fn query_predicate(&mut self, d: usize, level: u8) -> Result<QueryPredicate> {
        if d > MAX_DEPTH { return Err(Error::Limit); }
        if level < 2 {
            let mut predicate = self.query_predicate(d, level + 1)?;
            while self.word(if level == 0 { "or" } else { "and" }) {
                let right = self.query_predicate(d + 1, level + 1)?;
                predicate = if level == 0 { QueryPredicate::Or(Box::new(predicate), Box::new(right)) }
                    else { QueryPredicate::And(Box::new(predicate), Box::new(right)) };
            }
            return Ok(predicate);
        }
        if self.word("exists") { return Ok(QueryPredicate::Exists { negated: false, subquery: Box::new(self.subquery(false, d + 1)?) }); }
        if self.word("not") {
            return if self.word("exists") { Ok(QueryPredicate::Exists { negated: true, subquery: Box::new(self.subquery(false, d + 1)?) }) }
                else { Ok(QueryPredicate::Not(Box::new(self.query_predicate(d + 1, 2)?))) };
        }
        if self.symbol('(') {
            let predicate = self.query_predicate(d + 1, 0)?;
            if !self.symbol(')') { return Err(self.error()); }
            return Ok(predicate);
        }
        let left = self.query_expr()?;
        if let Some(Token::Op(op)) = self.peek().cloned() {
            self.pos += 1;
            return Ok(QueryPredicate::Compare(left, op, self.query_expr()?));
        }
        let negated = self.word("not");
        if self.word("in") { return Ok(QueryPredicate::In { expr: left, negated, subquery: Box::new(self.subquery(true, d + 1)?) }); }
        if negated { return Err(self.error()); }
        self.need("is")?;
        let negated = self.word("not"); self.need("null")?;
        Ok(QueryPredicate::IsNull(left, negated))
    }
    fn subquery(&mut self, single: bool, d: usize) -> Result<Query> {
        if !self.symbol('(') { return Err(self.error()); }
        if d > MAX_DEPTH { return Err(Error::Limit); }
        let source = QuerySource { table: self.relation_name()?, alias: None };
        let filter = if self.word("where") { Some(self.query_predicate(d, 0)?) } else { None };
        let mut projection = Vec::new();
        if self.word("select") { loop {
            let expression = self.query_expr()?;
            let alias = if self.word("as") { Some(self.name()?) } else { None };
            projection.push(QueryProjection { expression, alias });
            if !self.symbol(',') { break; }
        } }
        if !self.symbol(')') { return Err(self.error()); }
        if single && projection.len() != 1 { return Err(Error::Unsupported); }
        Ok(Query { source, join: None, filter, group_by: Vec::new(), having: None,
            order: Vec::new(), limit: None, projection })
    }
    fn query(&mut self) -> Result<Query> {
        let source = self.query_source()?;
        let kind = if self.word("inner") { self.need("join")?; Some(JoinKind::Inner) }
            else if self.word("left") { self.need("join")?; Some(JoinKind::Left) }
            else if self.word("join") { Some(JoinKind::Inner) } else { None };
        let join = if let Some(kind) = kind {
            let source = self.query_source()?; self.need("on")?;
            Some(QueryJoin { kind, source, on: self.query_predicate(0, 0)? })
        } else { None };
        let filter = if self.word("where") { Some(self.query_predicate(0, 0)?) } else { None };
        let mut group_by = Vec::new();
        if self.word("group") { self.need("by")?; loop { group_by.push(self.query_expr()?); if !self.symbol(',') { break; } } }
        let having = if self.word("having") { Some(self.query_predicate(0, 0)?) } else { None };
        let mut order = Vec::new();
        if self.word("order") { self.need("by")?; loop {
            let expression = self.query_expr()?; let descending = self.word("desc");
            if !descending { self.word("asc"); } order.push(QueryOrder { expression, descending });
            if !self.symbol(',') { break; }
        } }
        let limit = if self.word("limit") { Some(self.value()?) } else { None };
        let mut projection = Vec::new();
        if self.word("select") { loop {
            let expression = self.query_expr()?;
            let alias = if self.word("as") { Some(self.name()?) } else { None };
            projection.push(QueryProjection { expression, alias }); if !self.symbol(',') { break; }
        } }
        Ok(Query { source, join, filter, group_by, having, order, limit, projection })
    }
    fn inspection_prefix(&self) -> bool {
        let tokens = &self.tokens[self.pos..];
        matches!(tokens, [(Token::Word(word), _), (Token::Word(target), _), ..]
            if word == "show" && matches!(target.as_str(), "tables" | "capabilities" | "limits"))
            || matches!(tokens, [(Token::Word(describe), _), (Token::Word(table), _), ..] if describe == "describe" && table == "table")
    }
    fn inspection(&mut self) -> Result<InspectionStatement> {
        if !self.version.supports_inspection() {
            return Err(Error::Unsupported);
        }
        if self.word("show") {
            if self.word("capabilities") {
                Ok(InspectionStatement::ShowCapabilities)
            } else if self.word("limits") {
                Ok(InspectionStatement::ShowLimits)
            } else if self.word("tables") {
                Ok(InspectionStatement::ShowTables)
            } else {
                Err(self.error())
            }
        } else {
            self.need("describe")?;
            self.need("table")?;
            Ok(InspectionStatement::DescribeTable {
                table: self.name()?,
            })
        }
    }
    fn catalog_column(&mut self) -> Result<ColumnSpec> {
        let name = self.name()?;
        let nullable = self.word("nullable");
        let data_type = if self.word("bool") {
            CatalogType::Bool
        } else if self.word("int32") {
            CatalogType::Int32
        } else if self.word("int64") {
            CatalogType::Int64
        } else if self.word("text") {
            CatalogType::Text
        } else if matches!(self.version, LanguageVersion::Embedded016 | LanguageVersion::Embedded017 | LanguageVersion::Embedded018 | LanguageVersion::Embedded019 | LanguageVersion::Embedded020 | LanguageVersion::Embedded021) && self.word("numeric") {
            CatalogType::Numeric
        } else if self.word("bytes") {
            CatalogType::Bytes
        } else {
            return Err(Error::Unsupported);
        };
        Ok(ColumnSpec { name, data_type, nullable })
    }
    fn table_column(&mut self) -> Result<TableColumn> {
        if !self.version.supports_r2_wave3() {
            return Ok(TableColumn { column: self.catalog_column()?, default: None, generated: None });
        }
        let start = self.pos;
        let name = self.name()?;
        let nullable = self.word("nullable");
        let serial = if self.word("serial") { Some(CatalogType::Int32) }
            else if self.word("bigserial") { Some(CatalogType::Int64) } else { None };
        let column = if let Some(data_type) = serial { ColumnSpec { name, nullable, data_type } }
            else { self.pos = start; self.catalog_column()? };
        let mut generated = serial.map(|_| ColumnGenerated::Serial);
        let default = if self.word("default") {
            Some(if self.word("nextval") {
                if !self.symbol('(') { return Err(self.error()); }
                let relation = self.relation_name()?;
                if !self.symbol(')') { return Err(self.error()); }
                ColumnDefaultSpec::Nextval(relation)
            } else {
                let start = self.pos;
                let negative = self.symbol('-');
                let number = if let Some(Token::Number(n)) = self.peek().cloned() {
                    self.pos += 1;
                    if self.symbol('.') {
                        let Some(Token::Number(fraction)) = self.peek().cloned() else { return Err(self.error()); };
                        self.pos += 1;
                        Some(format!("{}{n}.{fraction}", if negative { "-" } else { "" }))
                    } else { None }
                } else { None };
                let value = if let Some(number) = number {
                    Value::Numeric(giggles_core::relational::Numeric::parse(&number).map_err(|_| self.error())?)
                } else {
                    self.pos = start;
                    if let Some(Token::DefaultText(text)) = self.peek().cloned() {
                        self.pos += 1; Value::Text(text)
                    } else { match self.value()? { ValueExpr::Literal(v) => v, ValueExpr::Parameter(_) => return Err(Error::Unsupported) } }
                };
                ColumnDefaultSpec::Value(value)
            })
        } else { None };
        if self.word("generated") {
            if generated.is_some() { return Err(self.error()); }
            generated = Some(if self.word("always") { ColumnGenerated::IdentityAlways }
                else { self.need("by")?; self.need("default")?; ColumnGenerated::IdentityByDefault });
            self.need("as")?; self.need("identity")?;
        }
        if column.nullable && generated.is_some() { return Err(self.error()); }
        Ok(TableColumn { column, default, generated })
    }
    fn catalog_prefix(&self) -> bool {
        matches!(&self.tokens[self.pos..], [(Token::Word(create), _), (Token::Word(table), _), ..]
            if ((create == "alter" && table == "table" && self.version.supports_r2_native()) || create == "create" || (create == "drop" && matches!(self.version, LanguageVersion::Embedded017 | LanguageVersion::Embedded018 | LanguageVersion::Embedded019 | LanguageVersion::Embedded020 | LanguageVersion::Embedded021)))
                && (table == "table" || table == "index" || (table == "view" && self.version.supports_r2_wave2())
                    || (table == "sequence" && create != "alter" && self.version.supports_r2_wave3())))
    }
    fn catalog(&mut self) -> Result<CatalogStatement> {
        if !self.version.supports_catalog() {
            return Err(Error::Unsupported);
        }
        if matches!(self.version, LanguageVersion::Embedded017 | LanguageVersion::Embedded018 | LanguageVersion::Embedded019 | LanguageVersion::Embedded020 | LanguageVersion::Embedded021) && self.word("drop") {
            if self.version.supports_r2_wave3() && self.word("sequence") {
                let if_exists = self.word("if");
                if if_exists { self.need("exists")?; }
                let name = self.relation_name()?;
                let cascade = self.word("cascade");
                if !cascade { self.word("restrict"); }
                return Ok(CatalogStatement::DropSequence { name, if_exists, cascade });
            }
            if self.version.supports_r2_wave2() && self.word("view") {
                let if_exists = self.word("if");
                if if_exists { self.need("exists")?; }
                let name = self.relation_name()?;
                let cascade = self.word("cascade");
                if !cascade { self.word("restrict"); }
                return Ok(CatalogStatement::DropView { name, if_exists, cascade });
            }
            let index = if self.word("index") { true } else { self.need("table")?; false };
            let name = self.relation_name()?;
            let cascade = self.word("cascade");
            if !cascade { self.word("restrict"); }
            return Ok(if index {
                CatalogStatement::DropIndex { name, cascade }
            } else {
                CatalogStatement::DropTable { table: name, cascade }
            });
        }
        if self.version.supports_r2_native() && self.word("alter") {
            self.need("table")?;
            let table = self.relation_name()?;
            let action = if self.word("add") {
                let explicit_column = self.word("column");
                if self.version.supports_r2_wave2() && !explicit_column
                    && matches!(self.tokens.get(self.pos), Some((Token::Word(w), _)) if matches!(w.as_str(), "constraint" | "unique" | "check" | "foreign")) {
                    let name = if self.word("constraint") { Some(self.name()?) } else { None };
                    AlterTableAction::AddConstraint { name, constraint: self.catalog_constraint()? }
                } else { AlterTableAction::AddColumn(self.catalog_column()?) }
            } else if self.word("drop") {
                if self.version.supports_r2_wave2() && self.word("constraint") {
                    let if_exists = self.word("if");
                    if if_exists { self.need("exists")?; }
                    let name = self.name()?;
                    let cascade = self.word("cascade");
                    if !cascade { self.word("restrict"); }
                    return Ok(CatalogStatement::AlterTable { table, action: AlterTableAction::DropConstraint { name, if_exists, cascade } });
                }
                self.word("column");
                let name = self.name()?;
                let cascade = self.word("cascade");
                if !cascade { self.word("restrict"); }
                AlterTableAction::DropColumn { name, cascade }
            } else {
                self.need("rename")?;
                if self.word("to") { AlterTableAction::RenameTable { to: self.name()? } }
                else {
                    self.word("column");
                    let from = self.name()?;
                    self.need("to")?;
                    AlterTableAction::RenameColumn { from, to: self.name()? }
                }
            };
            return Ok(CatalogStatement::AlterTable { table, action });
        }
        self.need("create")?;
        if self.version.supports_r2_wave3() && self.word("sequence") {
            let if_not_exists = self.word("if");
            if if_not_exists { self.need("not")?; self.need("exists")?; }
            return Ok(CatalogStatement::CreateSequence { name: self.relation_name()?, if_not_exists });
        }
        if self.version.supports_r2_wave2() && self.word("view") {
            let name = self.relation_name()?;
            self.need("as")?;
            return Ok(CatalogStatement::CreateView { name, query: Box::new(self.query()?) });
        }
        if self.word("index") {
            if !self.version.supports_indexes() {
                return Err(Error::Unsupported);
            }
            let name = self.name()?;
            self.need("on")?;
            let table = self.relation_name()?;
            if !self.symbol('(') {
                return Err(self.error());
            }
            let mut columns = vec![self.name()?];
            while self.symbol(',') {
                if columns.len() >= giggles_core::relational::MAX_COLUMNS {
                    return Err(Error::Limit);
                }
                columns.push(self.name()?);
            }
            if !self.symbol(')') {
                return Err(self.error());
            }
            return Ok(CatalogStatement::CreateIndex {
                table,
                name,
                columns,
            });
        }
        self.need("table")?;
        let table = self.relation_name()?;
        if !self.symbol('(') {
            return Err(self.error());
        }
        let mut columns = Vec::new();
        let mut primary_key = None;
        let mut constraints = Vec::new();
        loop {
            let key_clause = matches!(&self.tokens[self.pos..],
                [(Token::Word(primary), _), (Token::Word(key), _), ..]
                    if primary == "primary" && key == "key");
            if self.version.supports_r2_wave3() && self.table_foreign_key_ahead() {
                if constraints.len() >= giggles_core::relational::MAX_COLUMNS { return Err(Error::Limit); }
                let name = if self.word("constraint") { Some(self.name()?) } else { None };
                constraints.push(TableConstraint { name, constraint: self.catalog_constraint()? });
            } else if key_clause {
                if primary_key.is_some() {
                    return Err(self.error());
                }
                self.pos += 2;
                if !self.symbol('(') {
                    return Err(self.error());
                }
                let mut keys = vec![self.name()?];
                while self.symbol(',') {
                    if keys.len() >= giggles_core::relational::MAX_COLUMNS {
                        return Err(Error::Limit);
                    }
                    keys.push(self.name()?);
                }
                if !self.symbol(')') {
                    return Err(self.error());
                }
                primary_key = Some(keys);
            } else {
                if columns.len() >= giggles_core::relational::MAX_COLUMNS {
                    return Err(Error::Limit);
                }
                let column = self.table_column()?;
                // Column REFERENCES: `[CONSTRAINT n] REFERENCES p [(c)] [ON ...]`, repeatable.
                while self.version.supports_r2_wave3() && self.column_reference_ahead() {
                    if constraints.len() >= giggles_core::relational::MAX_COLUMNS { return Err(Error::Limit); }
                    let name = if self.word("constraint") { Some(self.name()?) } else { None };
                    self.need("references")?;
                    constraints.push(TableConstraint { name, constraint: self.references_tail(vec![column.column.name.clone()])? });
                }
                columns.push(column);
            }
            if !self.symbol(',') {
                break;
            }
        }
        if !self.symbol(')') {
            return Err(self.error());
        }
        let primary_key = primary_key.ok_or_else(|| self.error())?;
        if !constraints.is_empty() || columns.iter().any(|c| c.default.is_some() || c.generated.is_some()) {
            Ok(CatalogStatement::CreateTableWith { table, columns, primary_key, constraints })
        } else {
            Ok(CatalogStatement::CreateTable { table, columns: columns.into_iter().map(|c| c.column).collect(), primary_key })
        }
    }
    /// `FOREIGN KEY` or `CONSTRAINT n FOREIGN` starts a CREATE TABLE element.
    fn table_foreign_key_ahead(&self) -> bool {
        let word = |i: usize, w: &str| matches!(self.tokens.get(self.pos + i), Some((Token::Word(x), _)) if x == w);
        (word(0, "foreign") && word(1, "key")) || (word(0, "constraint") && word(2, "foreign"))
    }
    /// `REFERENCES` or `CONSTRAINT n REFERENCES` follows a CREATE TABLE column.
    fn column_reference_ahead(&self) -> bool {
        let word = |i: usize, w: &str| matches!(self.tokens.get(self.pos + i), Some((Token::Word(x), _)) if x == w);
        word(0, "references") || (word(0, "constraint") && word(2, "references"))
    }
    fn constraint_columns(&mut self) -> Result<Vec<String>> {
        if !self.symbol('(') { return Err(self.error()); }
        let mut columns = vec![self.name()?];
        while self.symbol(',') {
            if columns.len() >= giggles_core::relational::MAX_COLUMNS { return Err(Error::Limit); }
            columns.push(self.name()?);
        }
        if !self.symbol(')') { return Err(self.error()); }
        Ok(columns)
    }
    fn foreign_key_action(&mut self) -> Result<ForeignKeyAction> {
        Ok(if self.word("no") { self.need("action")?; ForeignKeyAction::NoAction }
        else if self.word("restrict") { ForeignKeyAction::Restrict }
        else if self.word("cascade") { ForeignKeyAction::Cascade }
        else { self.need("set")?;
            if self.word("null") { ForeignKeyAction::SetNull }
            else { self.need("default")?; ForeignKeyAction::SetDefault }
        })
    }
    fn catalog_constraint(&mut self) -> Result<ConstraintSpec> {
        if self.word("unique") { return Ok(ConstraintSpec::Unique { columns: self.constraint_columns()? }); }
        if self.word("check") {
            if !self.symbol('(') { return Err(self.error()); }
            let expression = self.expr(0)?;
            if !self.symbol(')') { return Err(self.error()); }
            return Ok(ConstraintSpec::Check(expression));
        }
        self.need("foreign")?; self.need("key")?;
        let columns = self.constraint_columns()?;
        self.need("references")?;
        self.references_tail(columns)
    }
    /// `p [(c, ...)] [ON DELETE a] [ON UPDATE a]` after REFERENCES (ALTER TABLE and CREATE TABLE).
    fn references_tail(&mut self, columns: Vec<String>) -> Result<ConstraintSpec> {
        let ref_table = self.relation_name()?;
        let ref_columns = if matches!(self.tokens.get(self.pos), Some((Token::Symbol('('), _))) { Some(self.constraint_columns()?) } else { None };
        let (mut on_delete, mut on_update) = (None, None);
        while self.word("on") {
            let target = if self.word("delete") { &mut on_delete } else { self.need("update")?; &mut on_update };
            if target.is_some() { return Err(self.error()); }
            *target = Some(self.foreign_key_action()?);
        }
        Ok(ConstraintSpec::ForeignKey { columns, ref_table, ref_columns,
            on_delete: on_delete.unwrap_or(ForeignKeyAction::NoAction), on_update: on_update.unwrap_or(ForeignKeyAction::NoAction) })
    }
    fn transaction_prefix(&self) -> bool {
        let tokens = &self.tokens[self.pos..];
        matches!(tokens, [(Token::Word(word), _), (Token::Symbol('{'), _), ..] if word == "transaction")
            || matches!(tokens, [(Token::Word(word), _), (Token::Word(read), _), (Token::Word(only), _), (Token::Symbol('{'), _), ..] if word == "transaction" && read == "read" && only == "only")
            || (self.version.supports_once()
                && (matches!(tokens, [(Token::Word(word), _), (Token::Word(once), _), ..] if word == "transaction" && once == "once")
                    || matches!(tokens, [(Token::Word(word), _), (Token::Word(read), _), (Token::Word(only), _), (Token::Word(once), _), ..] if word == "transaction" && read == "read" && only == "only" && once == "once")))
    }
    fn item_word(&mut self, word: &str) -> bool {
        if self.version.supports_r2_native()
            && matches!(self.tokens.get(self.pos + 1), Some((Token::Word(next), _)) if matches!(next.as_str(), "group" | "having" | "join" | "inner" | "left"))
            && matches!(self.tokens.get(self.pos + 2), Some((token, _)) if !matches!(token, Token::Op(Compare::Eq) | Token::Symbol(';' | '}' | ')'))) { return false; }
        // A read clause disambiguates keyword-named tables inside blocks too.
        if matches!(self.tokens.get(self.pos + 1), Some((Token::Word(next), _)) if matches!(next.as_str(), "where" | "order" | "limit" | "select"))
        {
            return false;
        }
        self.word(word)
    }
    fn emit_expression(&mut self) -> Result<EmitExpr> {
        // Whole rows/collections are emit values, never scalar operands.
        let start = self.pos;
        if self.symbol('@') {
            let local = self.variable_name()?;
            if self.symbol('.') {
                let row = self.word("row");
                let rows = !row && self.word("rows");
                if (row || rows) && matches!(self.peek(), Some(Token::Symbol(',' | ';' | '}'))) {
                    return Ok(if row {
                        EmitExpr::Row(local)
                    } else {
                        EmitExpr::Rows(local)
                    });
                }
            }
            self.pos = start;
        }
        Ok(EmitExpr::Scalar(self.expr(0)?))
    }
    fn transaction(&mut self) -> Result<Transaction> {
        let read_only = self.transaction_header()?;
        self.transaction_body(read_only)
    }
    fn transaction_header(&mut self) -> Result<bool> {
        if !self.version.supports_transactions() {
            return Err(Error::Unsupported);
        }
        self.need("transaction")?;
        let read_only = self.word("read");
        if read_only {
            self.need("only")?;
        }
        Ok(read_only)
    }
    fn transaction_input(&mut self) -> Result<ParsedInput> {
        let read_only = self.transaction_header()?;
        let header =
            if self.version.supports_once() && self.peek() == Some(&Token::Word("once".into())) {
                Some(self.once_header()?)
            } else {
                None
            };
        let transaction = self.transaction_body(read_only)?;
        if let Some((key, retention)) = header {
            let envelope = OnceEnvelope {
                language_version: self.version,
                key,
                retention,
                body: OnceBody::Transaction(transaction),
            };
            once::validate_once(&envelope, self.version)?;
            Ok(ParsedInput::Once(envelope))
        } else {
            execute::validate_transaction(&transaction, self.version)?;
            Ok(ParsedInput::Transaction(transaction))
        }
    }
    /// Profile 0.21: `nextval(` / `currval(` / `setval(` at `offset` tokens ahead.
    fn sequence_prefix(&self, offset: usize) -> bool {
        self.version.supports_r2_wave3() && matches!(&self.tokens[(self.pos + offset).min(self.tokens.len())..],
            [(Token::Word(f), _), (Token::Symbol('('), _), ..] if matches!(f.as_str(), "nextval" | "currval" | "setval"))
    }
    fn sequence_call(&mut self) -> Result<SequenceStatement> {
        let call = if self.word("nextval") { None } else if self.word("currval") { Some(false) } else { self.need("setval")?; Some(true) };
        if !self.symbol('(') { return Err(self.error()); }
        let name = self.relation_name()?;
        let statement = match call {
            None => SequenceStatement::Nextval(name),
            Some(false) => SequenceStatement::Currval(name),
            Some(true) => {
                if !self.symbol(',') { return Err(self.error()); }
                let value = self.value()?;
                let is_called = if self.symbol(',') { Some(self.value()?) } else { None };
                SequenceStatement::Setval { name, value, is_called }
            }
        };
        if !self.symbol(')') { return Err(self.error()); }
        Ok(statement)
    }
    fn savepoint_name(&mut self) -> Result<String> {
        match self.peek().cloned() {
            Some(Token::Word(name)) if canonical_identifier(&name) && !reserved_for(&name, self.version) => {
                self.pos += 1;
                Ok(name)
            }
            _ => Err(self.error()),
        }
    }
    fn transaction_body(&mut self, read_only: bool) -> Result<Transaction> {
        if !self.symbol('{') {
            return Err(self.error());
        }
        let mut steps = Vec::new();
        let mut emit = None;
        while !self.symbol('}') {
            if steps.len() + usize::from(emit.is_some()) >= MAX_TRANSACTION_ITEMS {
                return Err(Error::Limit);
            }
            if self.item_word("emit") {
                let mut values = vec![self.emit_expression()?];
                while self.symbol(',') {
                    if values.len() >= MAX_EMIT_VALUES {
                        return Err(Error::Limit);
                    }
                    values.push(self.emit_expression()?);
                }
                emit = Some(values);
                self.symbol(';');
                if !self.symbol('}') {
                    return Err(self.error());
                }
                break;
            }
            let step = if self.version.supports_r2_native() && self.item_word("savepoint") {
                TransactionStep::Savepoint(self.savepoint_name()?)
            } else if self.version.supports_r2_native() && self.item_word("rollback") {
                if !self.word("to") { return Err(self.error()); }
                self.word("savepoint");
                TransactionStep::RollbackTo(self.savepoint_name()?)
            } else if self.version.supports_r2_native() && self.item_word("release") {
                self.word("savepoint");
                TransactionStep::Release(self.savepoint_name()?)
            } else if self.sequence_prefix(0) {
                TransactionStep::Sequence { name: None, call: self.sequence_call()? }
            } else if self.item_word("let") {
                let name = self.variable_name()?;
                if self.peek() != Some(&Token::Op(Compare::Eq)) {
                    return Err(self.error());
                }
                self.pos += 1;
                if self.sequence_prefix(0) {
                    TransactionStep::Sequence { name: Some(name), call: self.sequence_call()? }
                } else {
                if !self.symbol('(') {
                    return Err(self.error());
                }
                let statement = self.statement()?;
                if !self.symbol(')') {
                    return Err(self.error());
                }
                TransactionStep::Let { name, statement }
                }
            } else if self.item_word("require") {
                TransactionStep::Require(self.expr(0)?)
            } else {
                TransactionStep::Execute(self.statement()?)
            };
            steps.push(step);
            if self.symbol('}') {
                break;
            }
            if !self.symbol(';') {
                return Err(self.error());
            }
        }
        Ok(Transaction {
            read_only,
            steps,
            emit,
        })
    }
}
/// Parse exactly one bounded native statement. No execution occurs.
pub fn parse(s: &str) -> Result<Statement> {
    parse_with_version(s, LanguageVersion::Embedded01)
}

/// Parse exactly one statement in an explicitly selected finite language.
pub fn parse_with_version(s: &str, version: LanguageVersion) -> Result<Statement> {
    let mut p = Parser {
        tokens: lex(s, version)?,
        pos: 0,
        end: s.len(),
        version,
    };
    let stmt = p.statement()?;
    p.symbol(';');
    if p.peek().is_some() {
        return Err(p.error());
    }
    execute::validate_depth(&stmt)?;
    Ok(stmt)
}

/// Parse one complete native transaction in its explicitly selected profile.
pub fn parse_transaction_with_version(s: &str, version: LanguageVersion) -> Result<Transaction> {
    if !version.supports_transactions() {
        return Err(Error::Unsupported);
    }
    let mut p = Parser {
        tokens: lex(s, version)?,
        pos: 0,
        end: s.len(),
        version,
    };
    let transaction = p.transaction()?;
    p.symbol(';');
    if p.peek().is_some() {
        return Err(p.error());
    }
    execute::validate_transaction(&transaction, version)?;
    Ok(transaction)
}

/// Parse one standalone catalog operation in its explicitly selected profile.
pub fn parse_catalog_with_version(s: &str, version: LanguageVersion) -> Result<CatalogStatement> {
    if !version.supports_catalog() {
        return Err(Error::Unsupported);
    }
    let mut p = Parser {
        tokens: lex(s, version)?,
        pos: 0,
        end: s.len(),
        version,
    };
    let statement = p.catalog()?;
    p.symbol(';');
    if p.peek().is_some() {
        return Err(p.error());
    }
    execute::validate_catalog(&statement, version)?;
    Ok(statement)
}

/// Parse one standalone discovery operation in its selected finite profile.
pub fn parse_inspection_with_version(
    s: &str,
    version: LanguageVersion,
) -> Result<InspectionStatement> {
    if !version.supports_inspection() {
        return Err(Error::Unsupported);
    }
    let mut p = Parser {
        tokens: lex(s, version)?,
        pos: 0,
        end: s.len(),
        version,
    };
    let statement = p.inspection()?;
    p.symbol(';');
    if p.peek().is_some() {
        return Err(p.error());
    }
    inspection::validate(&statement, version)?;
    Ok(statement)
}

/// Parse a complete keyed write or outer transaction, without executing it.
pub fn parse_once_with_version(s: &str, version: LanguageVersion) -> Result<OnceEnvelope> {
    if !version.supports_once() {
        return Err(Error::Unsupported);
    }
    match parse_input_with_version(s, version)? {
        ParsedInput::Once(envelope) => Ok(envelope),
        _ => Err(Error::Unsupported),
    }
}

/// Parse an outcome lookup. The original database scope is supplied separately.
pub fn parse_outcome_with_version(s: &str, version: LanguageVersion) -> Result<OutcomeQuery> {
    if !version.supports_once() {
        return Err(Error::Unsupported);
    }
    match parse_input_with_version(s, version)? {
        ParsedInput::Outcome(query) => Ok(query),
        _ => Err(Error::Unsupported),
    }
}

/// Dispatch complete tokenized input without treating a transaction-named table as a block.
pub fn parse_input_with_version(s: &str, version: LanguageVersion) -> Result<ParsedInput> {
    let mut p = Parser {
        tokens: lex(s, version)?,
        pos: 0,
        end: s.len(),
        version,
    };
    let input = if version.supports_explain() && p.word("explain") {
        if matches!(p.peek(), Some(Token::Word(word)) if matches!(word.as_str(), "explain" | "analyze" | "show" | "describe" | "transaction" | "once"))
        {
            return Err(Error::Unsupported);
        }
        let statement = ExplainStatement {
            read: p.statement()?,
        };
        if !(version.supports_r2_wave2() && matches!(statement.read, Statement::Query(_))) {
            explain::lower(&statement, version)?;
        }
        ParsedInput::Explain(statement)
    } else if version.supports_inspection() && p.inspection_prefix() {
        let statement = p.inspection()?;
        inspection::validate(&statement, version)?;
        ParsedInput::Inspection(statement)
    } else if version.supports_catalog() && p.catalog_prefix() {
        let statement = p.catalog()?;
        execute::validate_catalog(&statement, version)?;
        ParsedInput::Catalog(statement)
    } else if p.outcome_prefix() {
        ParsedInput::Outcome(p.outcome()?)
    } else if version.supports_transactions() && p.transaction_prefix() {
        p.transaction_input()?
    } else {
        p.statement_input()?
    };
    p.symbol(';');
    if p.peek().is_some() {
        return Err(p.error());
    }
    Ok(input)
}

/// Parse one standalone bounded static explanation in explicit 0.13.
pub fn parse_explain_with_version(s: &str, version: LanguageVersion) -> Result<ExplainStatement> {
    if !version.supports_explain() {
        return Err(Error::Unsupported);
    }
    match parse_input_with_version(s, version)? {
        ParsedInput::Explain(statement) => Ok(statement),
        _ => Err(Error::Unsupported),
    }
}

/// Parse a standalone simple read page in explicit 0.20.
/// Profile 0.21: `nextval(s)`, `currval(s)` or `setval(s, value [, is_called])`.
pub fn parse_sequence_with_version(s: &str, version: LanguageVersion) -> Result<SequenceStatement> {
    if !version.supports_r2_wave3() { return Err(Error::Unsupported); }
    let mut p = Parser { tokens: lex(s, version)?, pos: 0, end: s.len(), version };
    if !p.sequence_prefix(0) { return Err(p.error()); }
    let statement = p.sequence_call()?;
    p.symbol(';');
    if p.peek().is_some() { return Err(p.error()); }
    execute::validate_sequence(&statement, version)?;
    Ok(statement)
}
pub fn parse_page_with_version(s: &str, version: LanguageVersion) -> Result<PageStatement> {
    if !version.supports_r2_wave2() { return Err(Error::Unsupported); }
    let mut p = Parser { tokens: lex(s, version)?, pos: 0, end: s.len(), version };
    p.need("page")?;
    let (read, after) = p.page_read(true)?;
    p.symbol(';');
    if p.peek().is_some() { return Err(p.error()); }
    let statement = PageStatement { read, after };
    execute::lower_checked(&statement.read, version)?;
    execute::validate_size(&statement)?;
    Ok(statement)
}
