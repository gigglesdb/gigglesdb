//! COPY's SQL envelope. Data decoding belongs to the transport; mutation is INSERT.
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    Text,
    Binary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Copy {
    pub table: String,
    pub columns: Vec<String>,
    pub format: CopyFormat,
    pub delimiter: u8,
    pub null: String,
}
impl Copy {
    /// Recognize COPY even when the remainder cannot be lexed, for authority gates.
    pub fn is_copy(sql: &str) -> bool {
        let mut s = sql.trim_start();
        loop {
            if let Some(rest) = s.strip_prefix("--") {
                let Some(end) = rest.find(['\n', '\r']) else {
                    return false;
                };
                s = rest[end..].trim_start();
            } else if s.starts_with("/*") {
                let bytes = s.as_bytes();
                let (mut at, mut depth) = (2, 1usize);
                while at + 1 < bytes.len() && depth != 0 {
                    if &bytes[at..at + 2] == b"/*" {
                        depth += 1;
                        at += 2;
                    } else if &bytes[at..at + 2] == b"*/" {
                        depth -= 1;
                        at += 2;
                    } else {
                        at += 1;
                    }
                }
                if depth != 0 {
                    return false;
                }
                s = s[at..].trim_start();
            } else {
                break;
            }
        }
        s.get(..4)
            .is_some_and(|head| head.eq_ignore_ascii_case("copy"))
            && s.as_bytes()
                .get(4)
                .is_none_or(|b| !b.is_ascii_alphanumeric() && *b != b'_')
    }
    /// Extract only the target, before unsupported options or malformed body classification.
    pub fn target(sql: &str) -> R<Option<String>> {
        if !Self::is_copy(sql) {
            return Ok(None);
        }
        fn next(s: &str, at: &mut usize) -> R<Option<(usize, usize)>> {
            let b = s.as_bytes();
            loop {
                while b.get(*at).is_some_and(u8::is_ascii_whitespace) {
                    *at += 1;
                }
                if b.get(*at..*at + 2) == Some(b"--") {
                    *at += 2;
                    while b.get(*at).is_some_and(|c| !matches!(c, b'\n' | b'\r')) {
                        *at += 1;
                    }
                } else if b.get(*at..*at + 2) == Some(b"/*") {
                    *at += 2;
                    let mut depth = 1;
                    while *at + 1 < b.len() && depth > 0 {
                        if &b[*at..*at + 2] == b"/*" {
                            depth += 1;
                            *at += 2;
                        } else if &b[*at..*at + 2] == b"*/" {
                            depth -= 1;
                            *at += 2;
                        } else {
                            *at += 1;
                        }
                    }
                    if depth > 0 {
                        return Err(SqlError::Syntax);
                    }
                } else {
                    break;
                }
            }
            if *at == b.len() {
                return Ok(None);
            }
            let start = *at;
            if b[*at] == b'"' {
                *at += 1;
                loop {
                    let Some(c) = b.get(*at) else {
                        return Err(SqlError::Syntax);
                    };
                    *at += 1;
                    if *c == b'"' {
                        if b.get(*at) == Some(&b'"') {
                            *at += 1;
                        } else {
                            break;
                        }
                    }
                }
            } else if b[*at].is_ascii_alphabetic() || b[*at] == b'_' {
                *at += 1;
                while b
                    .get(*at)
                    .is_some_and(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'$'))
                {
                    *at += 1;
                }
            } else {
                *at += s[*at..].chars().next().ok_or(SqlError::Syntax)?.len_utf8();
            }
            Ok(Some((start, *at)))
        }
        if sql.len() > MAX_SQL {
            return Err(SqlError::Limit);
        }
        let mut at = 0;
        next(sql, &mut at)?.ok_or(SqlError::Syntax)?;
        let (_, mut end) = next(sql, &mut at)?.ok_or(SqlError::Syntax)?;
        if let Some((start, _)) = next(sql, &mut at)?
            && sql.as_bytes().get(start..at) == Some(b".")
        {
            end = next(sql, &mut at)?.ok_or(SqlError::Syntax)?.1;
        }
        let mut p = Parser {
            tokens: lex(&sql[..end])?,
            at: 0,
            params: BTreeSet::new(),
        };
        p.need("copy")?;
        p.table_name().map(Some)
    }
    pub fn parse(sql: &str) -> R<Self> {
        let mut p = Parser {
            tokens: lex(sql)?,
            at: 0,
            params: BTreeSet::new(),
        };
        p.need("copy")?;
        if p.tokens.get(p.at) == Some(&Token::Symbol('(')) {
            return Err(SqlError::Unsupported);
        }
        let table = p.table_name()?;
        let mut columns = vec![];
        if p.sym('(') {
            columns.push(p.name()?);
            while p.sym(',') {
                columns.push(p.name()?);
            }
            p.punct(')')?;
        }
        if p.word("to") {
            return Err(SqlError::Unsupported);
        }
        p.need("from")?;
        if !p.word("stdin") {
            return Err(SqlError::Unsupported);
        }
        let mut copy = Self {
            table,
            columns,
            format: CopyFormat::Text,
            delimiter: b'\t',
            null: "\\N".into(),
        };
        let mut seen = BTreeSet::new();
        if p.word("binary") {
            copy.format = CopyFormat::Binary;
            seen.insert("format");
        } else {
            let with = p.word("with");
            if p.sym('(') {
                loop {
                    let name = p.name()?;
                    match name.as_str() {
                        "format" => {
                            if !seen.insert("format") {
                                return Err(SqlError::Syntax);
                            }
                            copy.format = if p.word("text") {
                                CopyFormat::Text
                            } else if p.word("binary") {
                                CopyFormat::Binary
                            } else {
                                return Err(SqlError::Unsupported);
                            };
                        }
                        "delimiter" | "null" => {
                            let key = if name == "delimiter" {
                                "delimiter"
                            } else {
                                "null"
                            };
                            if !seen.insert(key) {
                                return Err(SqlError::Syntax);
                            }
                            let Some(Token::String(value)) = p.tokens.get(p.at) else {
                                return Err(SqlError::Syntax);
                            };
                            let value = value.clone();
                            p.at += 1;
                            if name == "delimiter" {
                                if value.len() != 1
                                    || matches!(value.as_bytes()[0], 0 | b'\n' | b'\r' | b'\\')
                                {
                                    return Err(SqlError::InvalidParameterValue);
                                }
                                copy.delimiter = value.as_bytes()[0];
                            } else {
                                if value.contains(['\0', '\n', '\r']) {
                                    return Err(SqlError::InvalidParameterValue);
                                }
                                copy.null = value;
                            }
                        }
                        _ => return Err(SqlError::Unsupported),
                    }
                    if !p.sym(',') {
                        break;
                    }
                }
                p.punct(')')?;
            } else if with {
                return Err(SqlError::Unsupported);
            }
        }
        p.sym(';');
        if p.at != p.tokens.len() {
            return Err(SqlError::Unsupported);
        }
        if copy.format == CopyFormat::Binary
            && (seen.contains("delimiter") || seen.contains("null"))
        {
            return Err(SqlError::Unsupported);
        }
        if copy.null.as_bytes().contains(&copy.delimiter) {
            return Err(SqlError::InvalidParameterValue);
        }
        Ok(copy)
    }
    pub(crate) fn columns_in(&self, state: &Relational) -> R<Vec<ColumnDef>> {
        let table = state
            .tables
            .get(&self.table)
            .ok_or(SqlError::UnknownTable)?;
        if self.columns.is_empty() {
            return Ok(table.logical_columns().cloned().collect());
        }
        if self.columns.len() > MAX_COLUMNS {
            return Err(SqlError::Limit);
        }
        let indexes = self
            .columns
            .iter()
            .map(|name| col(table, name))
            .collect::<R<Vec<_>>>()?;
        unique(&indexes)?;
        Ok(indexes
            .into_iter()
            .map(|i| table.columns[i].clone())
            .collect())
    }
    pub(crate) fn prepared(&self, state: &Relational, domain: u128) -> R<Prepared> {
        let columns = self.columns_in(state)?;
        let width = columns.len();
        let types = columns.into_iter().map(|c| c.data_type).collect();
        Ok(Prepared {
            domain,
            epoch: state.epoch,
            transaction: None,
            params: 0,
            parameter_types: vec![],
            statement: Statement::InsertMulti(sql_insert::Insert::copy(
                self.table.clone(),
                self.columns.clone(),
                types,
                vec![vec![Value::Null; width]],
            )),
        })
    }
}
impl Prepared {
    pub(crate) fn copy_rows(
        &self,
        rows: Vec<Vec<Value>>,
        clock: Option<&clock::ExecutionClock>,
    ) -> R<Self> {
        let mut prepared = self.clone();
        let Statement::InsertMulti(insert) = &mut prepared.statement else {
            return Err(SqlError::Unsupported);
        };
        insert.replace_copy_rows(rows, clock)?;
        Ok(prepared)
    }
}

impl Relational {
    pub(crate) fn normalize_copy(
        &self,
        prepared: &Prepared,
        rows: Vec<Vec<Value>>,
        clock: Option<&clock::ExecutionClock>,
    ) -> R<(Prepared, Vec<Vec<Value>>)> {
        sql_insert::normalize_copy(self, prepared, rows, clock)
    }
}

/// Replace with query::assignment::literal when DD-0068 F18 lands; COPY text input equals unknown-literal assignment.
pub(super) fn copy_text_field(
    text: String,
    ty: &DataType,
    clock: Option<&clock::ExecutionClock>,
) -> R<Value> {
    query::coerce_clocked(Value::Text(text), ty, clock).map_err(|error| {
        if *ty == DataType::Boolean && error == SqlError::Type {
            SqlError::InvalidTextRepresentation
        } else {
            error
        }
    })
}
