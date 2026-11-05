//! Bounded trusted relational subset. This module grants no task authority.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_SQL: usize = 64 * 1024;
pub const MAX_ROWS: usize = 1024;
pub const MAX_ROW_BYTES: usize = 64 * 1024;
pub const MAX_COLUMNS: usize = 64;
pub const MAX_PARAMS: usize = 256;
pub const MAX_TABLES: usize = 128;
pub const MAX_RESULT_BYTES: usize = 1024 * 1024;
pub const MAX_TRANSACTIONS: usize = 8;
fn value_bytes(v: &Value) -> usize {
    match v {
        Value::Text(s) => s.len(),
        Value::Bytes(b) => b.len(),
        _ => 9,
    }
}
fn payload_bound<'a>(values: impl Iterator<Item = &'a Value>) -> R<()> {
    let mut bytes = 0usize;
    for v in values {
        bytes = bytes.checked_add(value_bytes(v)).ok_or(SqlError::Limit)?;
        if bytes > MAX_ROW_BYTES {
            return Err(SqlError::Limit);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlError {
    Unsupported,
    Syntax,
    Reserved,
    UnknownTable,
    UnknownColumn,
    Duplicate,
    Type,
    Null,
    Parameters,
    Limit,
    Unbounded,
    Version,
    Conflict,
    StaleSchema,
    Transaction,
    Aborted,
    Overflow,
}
type R<T> = Result<T, SqlError>;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Text(String),
    Bytes(Vec<u8>),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    SmallInt,
    Integer,
    BigInt,
    Text,
    Bytes,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub id: u64,
    pub name: String,
    pub data_type: DataType,
    pub not_null: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDef {
    pub id: u64,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<usize>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlRow {
    pub values: Vec<Value>,
    pub version: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SqlResult {
    pub columns: Vec<String>,
    pub rows: Vec<SqlRow>,
    pub affected: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Relational {
    format: u32,
    pub generation: u64,
    pub epoch: u64,
    next_id: u64,
    next_version: u64,
    pub tables: BTreeMap<String, TableDef>,
    rows: BTreeMap<u64, BTreeMap<Vec<u8>, SqlRow>>,
}
impl Default for Relational {
    fn default() -> Self {
        Self {
            format: 1,
            generation: 0,
            epoch: 0,
            next_id: 1,
            next_version: 1,
            tables: BTreeMap::new(),
            rows: BTreeMap::new(),
        }
    }
}
fn allocate(n: &mut u64) -> R<u64> {
    let id = *n;
    *n = n.checked_add(1).ok_or(SqlError::Overflow)?;
    Ok(id)
}
fn ident(s: &str) -> R<()> {
    if s.is_empty()
        || s.len() > 63
        || !s.as_bytes()[0].is_ascii_alphabetic()
        || !s.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
    {
        return Err(SqlError::Syntax);
    }
    if [
        "deliveries",
        "giggles",
        "system",
        "information_schema",
        "pg_catalog",
        "repair",
        "internal",
        "tasks",
        "operations",
        "approvals",
        "receipts",
    ]
    .contains(&s)
        || s.starts_with("giggles_")
        || s.starts_with("system_")
        || s.starts_with("pg_")
        || s.starts_with("repair_")
        || s.starts_with("internal_")
    {
        return Err(SqlError::Reserved);
    }
    Ok(())
}
fn check(v: &Value, c: &ColumnDef) -> R<()> {
    if *v == Value::Null {
        return if c.not_null {
            Err(SqlError::Null)
        } else {
            Ok(())
        };
    }
    let yes = match (&c.data_type, v) {
        (DataType::Boolean, Value::Bool(_))
        | (DataType::BigInt, Value::Int(_))
        | (DataType::Text, Value::Text(_))
        | (DataType::Bytes, Value::Bytes(_)) => true,
        (DataType::SmallInt, Value::Int(n)) => i16::try_from(*n).is_ok(),
        (DataType::Integer, Value::Int(n)) => i32::try_from(*n).is_ok(),
        _ => false,
    };
    if yes { Ok(()) } else { Err(SqlError::Type) }
}
fn row_check(t: &TableDef, values: &[Value]) -> R<()> {
    if values.len() != t.columns.len() {
        return Err(SqlError::Type);
    }
    let mut size = 0usize;
    for (v, c) in values.iter().zip(&t.columns) {
        check(v, c)?;
        size = size
            .checked_add(match v {
                Value::Text(s) => s.len(),
                Value::Bytes(b) => b.len(),
                _ => 9,
            })
            .ok_or(SqlError::Limit)?;
    }
    if size > MAX_ROW_BYTES {
        return Err(SqlError::Limit);
    }
    Ok(())
}
/// Canonical memcomparable encoding v1: binary collation; NULL sorts first.
pub fn encode_key(values: &[Value]) -> R<Vec<u8>> {
    if values.len() > MAX_COLUMNS {
        return Err(SqlError::Limit);
    }
    payload_bound(values.iter())?;
    let mut out = Vec::new();
    for v in values {
        match v {
            Value::Null => out.push(0),
            Value::Bool(b) => {
                out.push(1);
                out.push(u8::from(*b));
            }
            Value::Int(n) => {
                out.push(2);
                out.extend(((*n as u64) ^ (1 << 63)).to_be_bytes());
            }
            Value::Text(s) => {
                out.push(3);
                escaped(s.as_bytes(), &mut out);
            }
            Value::Bytes(b) => {
                out.push(4);
                escaped(b, &mut out);
            }
        }
        if out.len() > MAX_ROW_BYTES * 2 + MAX_COLUMNS * 3 {
            return Err(SqlError::Limit);
        }
    }
    Ok(out)
}
fn escaped(b: &[u8], out: &mut Vec<u8>) {
    for c in b {
        if *c == 0 {
            out.extend([0, 255]);
        } else {
            out.push(*c);
        }
    }
    out.extend([0, 0]);
}
fn pk(t: &TableDef, v: &[Value]) -> R<Vec<u8>> {
    encode_key(
        &t.primary_key
            .iter()
            .map(|i| v[*i].clone())
            .collect::<Vec<_>>(),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Word(String),
    String(String),
    Number(String),
    Param(usize),
    Symbol(char),
    Op(String),
}
fn lex(s: &str) -> R<Vec<Token>> {
    if s.len() > MAX_SQL {
        return Err(SqlError::Limit);
    }
    let b = s.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    while i < b.len() {
        if b[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        let c = b[i];
        if c == b'\'' {
            i += 1;
            let mut v = Vec::new();
            let mut closed = false;
            while i < b.len() {
                if b[i] == b'\'' {
                    if b.get(i + 1) == Some(&b'\'') {
                        v.push(b'\'');
                        i += 2;
                    } else {
                        i += 1;
                        closed = true;
                        break;
                    }
                } else {
                    v.push(b[i]);
                    i += 1;
                }
            }
            if !closed {
                return Err(SqlError::Syntax);
            }
            out.push(Token::String(
                String::from_utf8(v).map_err(|_| SqlError::Syntax)?,
            ));
        } else if c.is_ascii_alphabetic() || c == b'_' {
            let start = i;
            i += 1;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                i += 1;
            }
            out.push(Token::Word(s[start..i].to_ascii_lowercase()));
        } else if c.is_ascii_digit() || (c == b'-' && b.get(i + 1).is_some_and(u8::is_ascii_digit))
        {
            let start = i;
            i += 1;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            out.push(Token::Number(s[start..i].into()));
        } else if c == b'$' {
            i += 1;
            let start = i;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            let n = s[start..i]
                .parse::<usize>()
                .map_err(|_| SqlError::Parameters)?;
            if n == 0 || n > MAX_PARAMS {
                return Err(SqlError::Parameters);
            }
            out.push(Token::Param(n));
        } else if b"(),*;".contains(&c) {
            out.push(Token::Symbol(c as char));
            i += 1;
        } else if b"=<>".contains(&c) {
            let start = i;
            i += 1;
            if b.get(i) == Some(&b'=') {
                i += 1;
            }
            out.push(Token::Op(s[start..i].into()));
        } else {
            return Err(SqlError::Unsupported);
        }
        if out.len() > 4096 {
            return Err(SqlError::Limit);
        }
    }
    Ok(out)
}
#[derive(Debug, Clone)]
enum Expr {
    Literal(Value),
    Param(usize),
}
impl Expr {
    fn value(&self, p: &[Value]) -> R<Value> {
        match self {
            Self::Literal(v) => Ok(v.clone()),
            Self::Param(n) => p.get(n - 1).cloned().ok_or(SqlError::Parameters),
        }
    }
}
#[derive(Debug, Clone)]
struct Predicate {
    column: String,
    op: String,
    value: Expr,
}
#[derive(Debug, Clone)]
enum Statement {
    Create {
        name: String,
        columns: Vec<ColumnDef>,
        keys: Vec<String>,
    },
    Insert {
        name: String,
        columns: Vec<String>,
        values: Vec<Expr>,
    },
    Select {
        name: String,
        projection: Vec<String>,
        predicates: Vec<Predicate>,
        order: Vec<(String, bool)>,
        limit: Option<Expr>,
    },
    Update {
        name: String,
        sets: Vec<(String, Expr)>,
        predicates: Vec<Predicate>,
    },
    Delete {
        name: String,
        predicates: Vec<Predicate>,
    },
    Begin,
    Commit,
    Rollback,
}
struct Parser {
    tokens: Vec<Token>,
    at: usize,
    params: BTreeSet<usize>,
}
impl Parser {
    fn word(&mut self, s: &str) -> bool {
        if self.tokens.get(self.at) == Some(&Token::Word(s.into())) {
            self.at += 1;
            true
        } else {
            false
        }
    }
    fn sym(&mut self, c: char) -> bool {
        if self.tokens.get(self.at) == Some(&Token::Symbol(c)) {
            self.at += 1;
            true
        } else {
            false
        }
    }
    fn need(&mut self, s: &str) -> R<()> {
        if self.word(s) {
            Ok(())
        } else {
            Err(SqlError::Syntax)
        }
    }
    fn punct(&mut self, c: char) -> R<()> {
        if self.sym(c) {
            Ok(())
        } else {
            Err(SqlError::Syntax)
        }
    }
    fn name(&mut self) -> R<String> {
        if let Some(Token::Word(s)) = self.tokens.get(self.at) {
            let s = s.clone();
            ident(&s)?;
            self.at += 1;
            Ok(s)
        } else {
            Err(SqlError::Syntax)
        }
    }
    fn expr(&mut self) -> R<Expr> {
        let t = self.tokens.get(self.at).cloned().ok_or(SqlError::Syntax)?;
        self.at += 1;
        Ok(match t {
            Token::Param(n) => {
                self.params.insert(n);
                Expr::Param(n)
            }
            Token::String(s) => Expr::Literal(Value::Text(s)),
            Token::Number(n) => Expr::Literal(Value::Int(n.parse().map_err(|_| SqlError::Type)?)),
            Token::Word(s) if s == "true" || s == "false" => {
                Expr::Literal(Value::Bool(s == "true"))
            }
            Token::Word(s) if s == "null" => Expr::Literal(Value::Null),
            _ => return Err(SqlError::Unsupported),
        })
    }
    fn names(&mut self) -> R<Vec<String>> {
        self.punct('(')?;
        let mut v = vec![self.name()?];
        while self.sym(',') {
            v.push(self.name()?);
        }
        self.punct(')')?;
        Ok(v)
    }
    fn predicates(&mut self) -> R<Vec<Predicate>> {
        let mut out = vec![];
        if !self.word("where") {
            return Ok(out);
        }
        loop {
            let column = self.name()?;
            let Some(Token::Op(op)) = self.tokens.get(self.at).cloned() else {
                return Err(SqlError::Syntax);
            };
            self.at += 1;
            if !["=", "<", "<=", ">", ">="].contains(&op.as_str()) {
                return Err(SqlError::Unsupported);
            }
            let value = self.expr()?;
            out.push(Predicate { column, op, value });
            if !self.word("and") {
                break;
            }
        }
        Ok(out)
    }
    fn parse(&mut self) -> R<Statement> {
        let st = if self.word("create") {
            self.need("table")?;
            let name = self.name()?;
            self.punct('(')?;
            let mut columns = vec![];
            let mut keys = vec![];
            loop {
                if self.word("primary") {
                    self.need("key")?;
                    if !keys.is_empty() {
                        return Err(SqlError::Duplicate);
                    }
                    keys = self.names()?;
                } else {
                    let name = self.name()?;
                    let data_type = if self.word("boolean") || self.word("bool") {
                        DataType::Boolean
                    } else if self.word("smallint") {
                        DataType::SmallInt
                    } else if self.word("integer") || self.word("int") {
                        DataType::Integer
                    } else if self.word("bigint") {
                        DataType::BigInt
                    } else if self.word("text") {
                        DataType::Text
                    } else if self.word("bytea") || self.word("bytes") {
                        DataType::Bytes
                    } else {
                        return Err(SqlError::Unsupported);
                    };
                    let mut not_null = false;
                    if self.word("not") {
                        self.need("null")?;
                        not_null = true;
                    } else {
                        self.word("null");
                    }
                    if self.word("primary") {
                        self.need("key")?;
                        if !keys.is_empty() {
                            return Err(SqlError::Duplicate);
                        }
                        keys.push(name.clone());
                    }
                    columns.push(ColumnDef {
                        id: 0,
                        name,
                        data_type,
                        not_null,
                    });
                }
                if !self.sym(',') {
                    break;
                }
            }
            self.punct(')')?;
            Statement::Create {
                name,
                columns,
                keys,
            }
        } else if self.word("insert") {
            self.need("into")?;
            let name = self.name()?;
            let columns = if self.tokens.get(self.at) == Some(&Token::Symbol('(')) {
                self.names()?
            } else {
                vec![]
            };
            self.need("values")?;
            self.punct('(')?;
            let mut values = vec![self.expr()?];
            while self.sym(',') {
                values.push(self.expr()?);
            }
            self.punct(')')?;
            Statement::Insert {
                name,
                columns,
                values,
            }
        } else if self.word("select") {
            let projection = if self.sym('*') {
                vec![]
            } else {
                let mut v = vec![self.name()?];
                while self.sym(',') {
                    v.push(self.name()?);
                }
                v
            };
            self.need("from")?;
            let name = self.name()?;
            let predicates = self.predicates()?;
            let mut order = vec![];
            if self.word("order") {
                self.need("by")?;
                loop {
                    let c = self.name()?;
                    let desc = self.word("desc");
                    if !desc {
                        self.word("asc");
                    }
                    order.push((c, desc));
                    if !self.sym(',') {
                        break;
                    }
                }
            }
            let limit = if self.word("limit") {
                Some(self.expr()?)
            } else {
                None
            };
            Statement::Select {
                name,
                projection,
                predicates,
                order,
                limit,
            }
        } else if self.word("update") {
            let name = self.name()?;
            self.need("set")?;
            let mut sets = vec![];
            loop {
                let c = self.name()?;
                if self.tokens.get(self.at) != Some(&Token::Op("=".into())) {
                    return Err(SqlError::Syntax);
                }
                self.at += 1;
                sets.push((c, self.expr()?));
                if !self.sym(',') {
                    break;
                }
            }
            let predicates = self.predicates()?;
            Statement::Update {
                name,
                sets,
                predicates,
            }
        } else if self.word("delete") {
            self.need("from")?;
            let name = self.name()?;
            let predicates = self.predicates()?;
            Statement::Delete { name, predicates }
        } else if self.word("begin") {
            Statement::Begin
        } else if self.word("commit") {
            Statement::Commit
        } else if self.word("rollback") {
            Statement::Rollback
        } else {
            return Err(SqlError::Unsupported);
        };
        self.sym(';');
        if self.at != self.tokens.len() {
            return Err(SqlError::Unsupported);
        }
        Ok(st)
    }
}
#[derive(Debug, Clone)]
pub struct Prepared {
    pub(crate) domain: u128,
    pub(crate) epoch: u64,
    pub(crate) transaction: Option<u128>,
    statement: Statement,
    params: usize,
}
/// Native ordered access compiled through the same binder and transaction path.
#[derive(Debug, Clone)]
pub struct Scan {
    pub table: String,
    pub projection: Vec<String>,
    pub prefix: Vec<Value>,
    pub lower: Option<(Value, bool)>,
    pub upper: Option<(Value, bool)>,
    pub reverse: bool,
    pub limit: usize,
}
impl Prepared {
    pub(crate) fn scan(scan: Scan, domain: u128, state: &Relational) -> R<Self> {
        ident(&scan.table)?;
        let t = state
            .tables
            .get(&scan.table)
            .ok_or(SqlError::UnknownTable)?;
        if scan.prefix.len() > t.primary_key.len()
            || scan.limit > MAX_ROWS
            || scan.projection.len() > MAX_COLUMNS
        {
            return Err(SqlError::Limit);
        }
        payload_bound(
            scan.prefix
                .iter()
                .chain(scan.lower.iter().map(|(v, _)| v))
                .chain(scan.upper.iter().map(|(v, _)| v)),
        )?;
        let mut predicates = vec![];
        for (i, v) in scan.prefix.into_iter().enumerate() {
            predicates.push(Predicate {
                column: t.columns[t.primary_key[i]].name.clone(),
                op: "=".into(),
                value: Expr::Literal(v),
            });
        }
        let used = predicates.len();
        for (bound, lower) in [(scan.lower, true), (scan.upper, false)] {
            if let Some((v, inclusive)) = bound {
                let i = *t.primary_key.get(used).ok_or(SqlError::Unbounded)?;
                predicates.push(Predicate {
                    column: t.columns[i].name.clone(),
                    op: match (lower, inclusive) {
                        (true, true) => ">=",
                        (true, false) => ">",
                        (false, true) => "<=",
                        (false, false) => "<",
                    }
                    .into(),
                    value: Expr::Literal(v),
                });
            }
        }
        Ok(Self {
            domain,
            epoch: state.epoch,
            transaction: None,
            params: 0,
            statement: Statement::Select {
                name: scan.table,
                projection: scan.projection,
                predicates,
                order: t
                    .primary_key
                    .iter()
                    .map(|i| (t.columns[*i].name.clone(), scan.reverse))
                    .collect(),
                limit: Some(Expr::Literal(Value::Int(scan.limit as i64))),
            },
        })
    }
    pub fn parameter_count(&self) -> usize {
        self.params
    }
    pub(crate) fn parse(sql: &str, domain: u128, epoch: u64) -> R<Self> {
        let mut p = Parser {
            tokens: lex(sql)?,
            at: 0,
            params: BTreeSet::new(),
        };
        let statement = p.parse()?;
        let params = p.params.last().copied().unwrap_or(0);
        if p.params.len() != params {
            return Err(SqlError::Parameters);
        }
        Ok(Self {
            domain,
            epoch,
            transaction: None,
            statement,
            params,
        })
    }
    pub(crate) fn control(&self) -> Option<&'static str> {
        match self.statement {
            Statement::Begin => Some("begin"),
            Statement::Commit => Some("commit"),
            Statement::Rollback => Some("rollback"),
            _ => None,
        }
    }
    pub(crate) fn is_read(&self) -> bool {
        matches!(self.statement, Statement::Select { .. })
    }
}
fn col(t: &TableDef, name: &str) -> R<usize> {
    t.columns
        .iter()
        .position(|c| c.name == name)
        .ok_or(SqlError::UnknownColumn)
}
fn unique<T: Ord>(v: impl IntoIterator<Item = T>) -> R<()> {
    let mut set = BTreeSet::new();
    for item in v {
        if !set.insert(item) {
            return Err(SqlError::Duplicate);
        }
    }
    Ok(())
}
struct Bounds {
    low: Vec<u8>,
    high: Vec<u8>,
    low_inclusive: bool,
    high_inclusive: bool,
    point: bool,
}
fn successor(mut v: Vec<u8>) -> Option<Vec<u8>> {
    while let Some(b) = v.pop() {
        if b < 255 {
            v.push(b + 1);
            return Some(v);
        }
    }
    None
}
fn bounds(t: &TableDef, preds: &[Predicate], params: &[Value]) -> R<Bounds> {
    let mut by: BTreeMap<usize, Vec<(&str, Value)>> = BTreeMap::new();
    for p in preds {
        let i = col(t, &p.column)?;
        if !t.primary_key.contains(&i) {
            return Err(SqlError::Unbounded);
        }
        let v = p.value.value(params)?;
        check(&v, &t.columns[i])?;
        by.entry(i).or_default().push((&p.op, v));
    }
    let mut prefix = vec![];
    let mut consumed = 0;
    for i in &t.primary_key {
        let Some(ps) = by.get(i) else { break };
        if ps.len() == 1 && ps[0].0 == "=" {
            prefix.extend(encode_key(&[ps[0].1.clone()])?);
            consumed += 1;
        } else {
            break;
        }
    }
    if consumed == t.primary_key.len() {
        if by.len() != consumed {
            return Err(SqlError::Unbounded);
        }
        return Ok(Bounds {
            low: prefix.clone(),
            high: prefix,
            low_inclusive: true,
            high_inclusive: true,
            point: true,
        });
    }
    let mut low = prefix.clone();
    let mut high = successor(prefix.clone()).unwrap_or_else(|| vec![255]);
    let mut low_inclusive = true;
    let mut high_inclusive = false;
    if let Some(ps) = by.get(&t.primary_key[consumed]) {
        let mut has_low = false;
        let mut has_high = false;
        for (op, v) in ps {
            let mut b = prefix.clone();
            b.extend(encode_key(std::slice::from_ref(v))?);
            match *op {
                ">" | ">=" => {
                    if has_low {
                        return Err(SqlError::Unsupported);
                    }
                    has_low = true;
                    if *op == ">" {
                        low = successor(b).ok_or(SqlError::Unbounded)?;
                    } else {
                        low = b;
                    }
                    low_inclusive = true;
                }
                "<" | "<=" => {
                    if has_high {
                        return Err(SqlError::Unsupported);
                    }
                    has_high = true;
                    if *op == "<=" {
                        high = successor(b).ok_or(SqlError::Unbounded)?;
                    } else {
                        high = b;
                    }
                    high_inclusive = false;
                }
                _ => return Err(SqlError::Unsupported),
            }
        }
        consumed += 1;
    }
    if by.len() != consumed {
        return Err(SqlError::Unbounded);
    }
    Ok(Bounds {
        low,
        high,
        low_inclusive,
        high_inclusive,
        point: false,
    })
}
impl Relational {
    pub(crate) fn validate(&self) -> R<()> {
        if self.format != 1 {
            return Err(SqlError::Unsupported);
        }
        if self.tables.len() > MAX_TABLES || self.rows.len() != self.tables.len() {
            return Err(SqlError::Type);
        }
        let mut ids = BTreeSet::new();
        let mut versions = BTreeSet::new();
        for (name, t) in &self.tables {
            ident(name)?;
            if name != &t.name
                || t.id == 0
                || t.id >= self.next_id
                || !ids.insert(t.id)
                || t.columns.is_empty()
                || t.columns.len() > MAX_COLUMNS
                || t.primary_key.is_empty()
            {
                return Err(SqlError::Type);
            }
            unique(t.columns.iter().map(|c| &c.name))?;
            unique(t.primary_key.iter())?;
            for c in &t.columns {
                ident(&c.name)?;
                if c.id == 0 || c.id >= self.next_id || !ids.insert(c.id) {
                    return Err(SqlError::Type);
                }
            }
            for i in &t.primary_key {
                if *i >= t.columns.len() || !t.columns[*i].not_null {
                    return Err(SqlError::Null);
                }
            }
            for (k, r) in self.rows.get(&t.id).ok_or(SqlError::Type)? {
                row_check(t, &r.values)?;
                if pk(t, &r.values)? != *k
                    || r.version == 0
                    || r.version >= self.next_version
                    || !versions.insert(r.version)
                {
                    return Err(SqlError::Version);
                }
            }
        }
        Ok(())
    }
    pub(crate) fn execute(
        &mut self,
        prepared: &Prepared,
        params: &[Value],
        expected: Option<u64>,
    ) -> R<SqlResult> {
        if prepared.epoch != self.epoch {
            return Err(SqlError::StaleSchema);
        }
        if params.len() != prepared.params || params.len() > MAX_PARAMS {
            return Err(SqlError::Parameters);
        }
        // Bound parameters even if a statement affects no rows.
        payload_bound(params.iter())?;
        let mut result = SqlResult::default();
        match &prepared.statement {
            Statement::Create {
                name,
                columns,
                keys,
            } => {
                if expected.is_some() {
                    return Err(SqlError::Unsupported);
                }
                if self.tables.contains_key(name) {
                    return Err(SqlError::Duplicate);
                }
                if self.tables.len() >= MAX_TABLES
                    || columns.is_empty()
                    || columns.len() > MAX_COLUMNS
                {
                    return Err(SqlError::Limit);
                }
                if keys.is_empty() {
                    return Err(SqlError::Unbounded);
                }
                unique(columns.iter().map(|c| &c.name))?;
                unique(keys)?;
                let mut t = TableDef {
                    id: allocate(&mut self.next_id)?,
                    name: name.clone(),
                    columns: columns.clone(),
                    primary_key: vec![],
                };
                for key in keys {
                    let i = col(&t, key)?;
                    t.primary_key.push(i);
                    t.columns[i].not_null = true;
                }
                for c in &mut t.columns {
                    c.id = allocate(&mut self.next_id)?;
                }
                self.epoch = self.epoch.checked_add(1).ok_or(SqlError::Overflow)?;
                self.rows.insert(t.id, BTreeMap::new());
                self.tables.insert(name.clone(), t);
            }
            Statement::Insert {
                name,
                columns,
                values,
            } => {
                if expected.is_some() {
                    return Err(SqlError::Unsupported);
                }
                let t = self.tables.get(name).ok_or(SqlError::UnknownTable)?;
                let indexes = if columns.is_empty() {
                    (0..t.columns.len()).collect::<Vec<_>>()
                } else {
                    columns.iter().map(|c| col(t, c)).collect::<R<Vec<_>>>()?
                };
                unique(&indexes)?;
                if indexes.len() != values.len() {
                    return Err(SqlError::Type);
                }
                let mut row = vec![Value::Null; t.columns.len()];
                for (i, v) in indexes.iter().zip(values) {
                    row[*i] = v.value(params)?;
                }
                row_check(t, &row)?;
                let key = pk(t, &row)?;
                let rows = self.rows.get_mut(&t.id).ok_or(SqlError::UnknownTable)?;
                if rows.contains_key(&key) {
                    return Err(SqlError::Duplicate);
                }
                let version = allocate(&mut self.next_version)?;
                rows.insert(
                    key,
                    SqlRow {
                        values: row,
                        version,
                    },
                );
                result.affected = 1;
            }
            Statement::Select {
                name,
                projection,
                predicates,
                order,
                limit,
            } => {
                if projection.len() > MAX_COLUMNS {
                    return Err(SqlError::Limit);
                }
                if expected.is_some() {
                    return Err(SqlError::Unsupported);
                }
                let t = self.tables.get(name).ok_or(SqlError::UnknownTable)?;
                let b = bounds(t, predicates, params)?;
                let limit = match limit {
                    Some(e) => match e.value(params)? {
                        Value::Int(n) if n >= 0 && (n as u64) <= MAX_ROWS as u64 => n as usize,
                        _ => return Err(SqlError::Limit),
                    },
                    None if b.point => 1,
                    None => return Err(SqlError::Unbounded),
                };
                let indexes = if projection.is_empty() {
                    (0..t.columns.len()).collect::<Vec<_>>()
                } else {
                    projection
                        .iter()
                        .map(|c| col(t, c))
                        .collect::<R<Vec<_>>>()?
                };
                let desc = order.first().is_some_and(|v| v.1);
                if !order.is_empty() {
                    let ordered = order
                        .iter()
                        .map(|(c, d)| {
                            if *d != desc {
                                return Err(SqlError::Unsupported);
                            }
                            col(t, c)
                        })
                        .collect::<R<Vec<_>>>()?;
                    let eq = predicates.iter().filter(|p| p.op == "=").count();
                    let suffix = &t.primary_key[eq.min(t.primary_key.len())..];
                    if ordered != t.primary_key && ordered != suffix {
                        return Err(SqlError::Unsupported);
                    }
                }
                result.columns = indexes.iter().map(|i| t.columns[*i].name.clone()).collect();
                let rows = self.rows.get(&t.id).ok_or(SqlError::UnknownTable)?;
                if b.low > b.high || (b.low == b.high && !b.point) {
                    return Ok(result);
                }
                use std::ops::Bound::{Excluded, Included};
                let lo = if b.low_inclusive {
                    Included(b.low)
                } else {
                    Excluded(b.low)
                };
                let hi = if b.high_inclusive {
                    Included(b.high)
                } else {
                    Excluded(b.high)
                };
                let iter = rows.range((lo, hi));
                let iter: Box<dyn Iterator<Item = (&Vec<u8>, &SqlRow)>> = if desc {
                    Box::new(iter.rev())
                } else {
                    Box::new(iter)
                };
                let mut bytes = 0usize;
                for (_, r) in iter.take(limit) {
                    bytes += indexes
                        .iter()
                        .map(|i| value_bytes(&r.values[*i]))
                        .sum::<usize>();
                    if bytes > MAX_RESULT_BYTES {
                        return Err(SqlError::Limit);
                    }
                    let values: Vec<Value> = indexes.iter().map(|i| r.values[*i].clone()).collect();
                    result.rows.push(SqlRow {
                        values,
                        version: r.version,
                    });
                }
            }
            Statement::Update {
                name,
                sets,
                predicates,
            } => {
                let t = self.tables.get(name).ok_or(SqlError::UnknownTable)?;
                let b = bounds(t, predicates, params)?;
                if !b.point {
                    return Err(SqlError::Unbounded);
                }
                let indexes = sets
                    .iter()
                    .map(|(c, v)| {
                        let i = col(t, c)?;
                        if t.primary_key.contains(&i) {
                            return Err(SqlError::Unsupported);
                        }
                        let value = v.value(params)?;
                        check(&value, &t.columns[i])?;
                        Ok((i, value))
                    })
                    .collect::<R<Vec<_>>>()?;
                unique(indexes.iter().map(|(i, _)| i))?;
                let rows = self.rows.get_mut(&t.id).ok_or(SqlError::UnknownTable)?;
                let row = rows.get_mut(&b.low);
                if let Some(want) = expected
                    && row.as_ref().is_none_or(|r| r.version != want)
                {
                    return Err(SqlError::Version);
                }
                if let Some(row) = row {
                    for (i, v) in indexes {
                        row.values[i] = v;
                    }
                    row_check(t, &row.values)?;
                    row.version = allocate(&mut self.next_version)?;
                    result.affected = 1;
                }
            }
            Statement::Delete { name, predicates } => {
                let t = self.tables.get(name).ok_or(SqlError::UnknownTable)?;
                let b = bounds(t, predicates, params)?;
                if !b.point {
                    return Err(SqlError::Unbounded);
                }
                let rows = self.rows.get_mut(&t.id).ok_or(SqlError::UnknownTable)?;
                if let Some(want) = expected
                    && rows.get(&b.low).is_none_or(|r| r.version != want)
                {
                    return Err(SqlError::Version);
                }
                result.affected = u64::from(rows.remove(&b.low).is_some());
            }
            _ => return Err(SqlError::Transaction),
        }
        Ok(result)
    }
}
