//! COPY's SQL envelope. Data decoding belongs to the transport; mutation is INSERT.
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    Text,
    Csv,
    Binary,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CopyHeader {
    #[default]
    Off,
    On,
    Match,
}
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CopyColumns {
    pub all: bool,
    pub names: Vec<String>,
}
impl CopyColumns {
    pub fn contains(&self, name: &str) -> bool {
        self.all || self.names.iter().any(|n| n == name)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CopyLogVerbosity { #[default] Default, Verbose, Silent }

/// Runtime-only COPY options, shared by both SQL envelopes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyOptions {
    pub freeze: bool,
    pub format: CopyFormat,
    pub encoding: Option<CopyEncoding>,
    pub delimiter: u8,
    pub null: String,
    pub quote: u8,
    pub escape: u8,
    pub header: CopyHeader,
    pub force_quote: CopyColumns,
    pub force_not_null: CopyColumns,
    pub force_null: CopyColumns,
    pub default: Option<String>,
    pub on_error_ignore: bool,
    pub reject_limit: Option<u64>,
    pub log_verbosity: CopyLogVerbosity,
}
impl Default for CopyOptions {
    fn default() -> Self {
        Self {
            freeze: false,
            format: CopyFormat::Text,
            encoding: None,
            delimiter: b'\t',
            null: "\\N".into(),
            quote: b'"',
            escape: b'"',
            header: CopyHeader::Off,
            force_quote: CopyColumns::default(),
            force_not_null: CopyColumns::default(),
            force_null: CopyColumns::default(),
            default: None,
            on_error_ignore: false,
            reject_limit: None,
            log_verbosity: CopyLogVerbosity::Default,
        }
    }
}
fn copy_error<T>(error: SqlError, message: impl Into<String>) -> R<T> {
    user_types::view_replace_error(message.into());
    Err(error)
}
impl CopyOptions {
    pub fn parse(sql: &str, from: bool) -> R<Self> {
        let mut p = Parser {
            privilege_casts: false,
            privilege_seen: false,
            tokens: lex(sql)?,
            at: 0,
            params: BTreeSet::new(),
            relations: Vec::new(),
        };
        Self::parse_tokens(&mut p, from)
    }
    fn parse_tokens(p: &mut Parser, from: bool) -> R<Self> {
        let mut o = Self::default();
        let mut values = BTreeMap::new();
        let mut seen = BTreeSet::new();
        p.word("with");
        let parens = p.sym('(');
        while p.at < p.tokens.len() && !matches!(p.tokens.get(p.at), Some(Token::Symbol(')' | ';')))
        {
            if !parens && p.word("where") { return Err(SqlError::Unsupported); }
            let mut name = p.name()?;
            // OIDS was removed from the legacy COPY grammar; it is not a generic option.
            if !parens && name == "oids" {
                return copy_error(SqlError::Syntax, "syntax error at or near \"OIDS\"");
            }
            if !parens && name == "force" {
                name = if p.word("quote") {
                    "force_quote"
                } else if p.word("not") {
                    p.need("null")?;
                    "force_not_null"
                } else {
                    p.need("null")?;
                    "force_null"
                }
                .into();
            }
            if !parens && matches!(name.as_str(), "csv" | "binary") {
                if !seen.insert("format".to_owned()) {
                    return copy_error(SqlError::Syntax, "conflicting or redundant options");
                }
                values.insert("format".to_owned(), name);
            } else {
                if !seen.insert(name.clone()) {
                    return copy_error(SqlError::Syntax, "conflicting or redundant options");
                }
                match name.as_str() {
                    "force_quote" | "force_not_null" | "force_null" => {
                        let cols = match name.as_str() {
                            "force_quote" => &mut o.force_quote,
                            "force_not_null" => &mut o.force_not_null,
                            _ => &mut o.force_null,
                        };
                        if p.sym('*') {
                            cols.all = true;
                        } else {
                            if parens {
                                p.punct('(')?;
                            }
                            cols.names.push(p.column_name()?);
                            while p.sym(',') {
                                cols.names.push(p.column_name()?);
                            }
                            if parens {
                                p.punct(')')?;
                            }
                        }
                    }
                    "format" | "delimiter" | "null" | "default" | "quote" | "escape"
                    | "encoding" | "header" | "freeze" | "on_error" | "log_verbosity"
                    | "reject_limit" => {
                        if !parens {
                            p.word("as");
                        }
                        let bare = matches!(name.as_str(), "header" | "freeze")
                            && (matches!(
                                p.tokens.get(p.at),
                                None | Some(Token::Symbol(',' | ')' | ';'))
                            ) || (!parens
                                && matches!(p.tokens.get(p.at), Some(Token::Word(w)) if !matches!(w.as_str(), "true" | "false" | "on" | "off" | "match"))));
                        let value = if bare {
                            "true".into()
                        } else {
                            let value = match p.tokens.get(p.at) {
                                Some(Token::Word(v) | Token::String(v) | Token::Number(v)) => {
                                    v.clone()
                                }
                                _ => return Err(SqlError::Syntax),
                            };
                            p.at += 1;
                            value
                        };
                        values.insert(name, value);
                    }
                    _ => {
                        return copy_error(
                            SqlError::Syntax,
                            format!("option \"{name}\" not recognized"),
                        );
                    }
                }
            }
            if parens && !p.sym(',') {
                break;
            }
        }
        if parens {
            p.punct(')')?;
        }
        p.sym(';');
        if p.at != p.tokens.len() {
            return Err(SqlError::Syntax);
        }
        // Process the option values before the cross-option checks.
        if let Some(v) = values.get("format") {
            o.format = match v.to_ascii_lowercase().as_str() {
                "text" => CopyFormat::Text,
                "csv" => CopyFormat::Csv,
                "binary" => CopyFormat::Binary,
                _ => {
                    return copy_error(
                        SqlError::InvalidParameterValue,
                        format!("COPY format \"{v}\" not recognized"),
                    );
                }
            };
        }
        if let Some(v) = values.get("header") {
            o.header = match v.to_ascii_lowercase().as_str() {
                "true" | "on" | "1" => CopyHeader::On,
                "false" | "off" | "0" => CopyHeader::Off,
                "match" => CopyHeader::Match,
                _ => {
                    return copy_error(
                        SqlError::Syntax,
                        "header requires a Boolean value or \"match\"",
                    );
                }
            };
        }
        if !from && o.header == CopyHeader::Match {
            return copy_error(
                SqlError::Unsupported,
                "cannot use \"match\" with HEADER in COPY TO",
            );
        }
        if let Some(v) = values.get("encoding") {
            o.encoding = Some(CopyEncoding::lookup(v).ok_or_else(|| {
                user_types::view_replace_error("argument to option \"encoding\" must be a valid encoding name".into());
                SqlError::InvalidParameterValue
            })?);
        }
        let freeze = if let Some(v) = values.get("freeze") {
            match v.to_ascii_lowercase().as_str() {
                "true" | "on" | "1" => true, "false" | "off" | "0" => false,
                _ => return copy_error(SqlError::Syntax, "freeze requires a Boolean value"),
            }
        } else { false };
        let ignore = if let Some(v) = values.get("on_error") {
            if !from {
                return copy_error(
                    SqlError::InvalidParameterValue,
                    "COPY ON_ERROR cannot be used with COPY TO",
                );
            }
            match v.to_ascii_lowercase().as_str() {
                "stop" => false,
                "ignore" => true,
                _ => {
                    return copy_error(
                        SqlError::InvalidParameterValue,
                        format!("COPY ON_ERROR \"{v}\" not recognized"),
                    );
                }
            }
        } else {
            false
        };
        if let Some(v) = values.get("log_verbosity")
            && !matches!(
                v.to_ascii_lowercase().as_str(),
                "default" | "verbose" | "silent"
            ) {
                return copy_error(
                    SqlError::InvalidParameterValue,
                    format!("COPY LOG_VERBOSITY \"{v}\" not recognized"),
                );
        }
        if let Some(v) = values.get("reject_limit") {
            let n: i64 = v.parse().map_err(|_| SqlError::InvalidParameterValue)?;
            if n <= 0 {
                return copy_error(
                    SqlError::InvalidParameterValue,
                    format!("REJECT_LIMIT ({n}) must be greater than zero"),
                );
            }
        }
        let csv = o.format == CopyFormat::Csv;
        let binary = o.format == CopyFormat::Binary;
        if csv {
            o.delimiter = b',';
            o.null.clear();
        }
        for name in ["delimiter", "null", "default"] {
            if binary && seen.contains(name) {
                return copy_error(
                    SqlError::Syntax,
                    format!("cannot specify {} in BINARY mode", name.to_uppercase()),
                );
            }
        }
        if let Some(v) = values.get("delimiter") {
            if v.len() != 1 {
                return copy_error(
                    SqlError::Unsupported,
                    "COPY delimiter must be a single one-byte character",
                );
            }
            o.delimiter = v.as_bytes()[0];
        }
        if matches!(o.delimiter, b'\n' | b'\r') {
            return copy_error(
                SqlError::InvalidParameterValue,
                "COPY delimiter cannot be newline or carriage return",
            );
        }
        if let Some(v) = values.get("null") {
            o.null = v.clone();
        }
        if o.null.contains(['\n', '\r']) {
            return copy_error(
                SqlError::InvalidParameterValue,
                "COPY null representation cannot use newline or carriage return",
            );
        }
        if values.get("default").is_some_and(|v| v.contains(['\n', '\r'])) {
            return copy_error(SqlError::InvalidParameterValue, "COPY default representation cannot use newline or carriage return");
        }
        if !csv
            && (o.delimiter == b'\\'
                || o.delimiter == b'.'
                || o.delimiter.is_ascii_lowercase()
                || o.delimiter.is_ascii_digit())
        {
            return copy_error(
                SqlError::InvalidParameterValue,
                format!("COPY delimiter cannot be \"{}\"", o.delimiter as char),
            );
        }
        if binary && o.header != CopyHeader::Off {
            return copy_error(SqlError::Unsupported, "cannot specify HEADER in BINARY mode");
        }
        for name in [
            "quote",
            "escape",
            "force_quote",
            "force_not_null",
            "force_null",
        ] {
            if seen.contains(name) && !csv {
                return copy_error(
                    SqlError::Unsupported,
                    format!("COPY {} requires CSV mode", name.to_uppercase()),
                );
            }
            if let Some(v) = values.get(name) {
                if v.len() != 1 {
                    return copy_error(
                        SqlError::Unsupported,
                        format!("COPY {name} must be a single one-byte character"),
                    );
                }
                if name == "quote" {
                    o.quote = v.as_bytes()[0];
                } else {
                    o.escape = v.as_bytes()[0];
                }
            }
            if name == "quote" && csv && o.delimiter == o.quote {
                return copy_error(SqlError::InvalidParameterValue, "COPY delimiter and quote must be different");
            }
            if name == "force_quote" && from && seen.contains(name) {
                return copy_error(SqlError::Unsupported, "COPY FORCE_QUOTE cannot be used with COPY FROM");
            }
            if matches!(name, "force_not_null" | "force_null") && !from && seen.contains(name) {
                return copy_error(SqlError::InvalidParameterValue, format!("COPY {} cannot be used with COPY TO", name.to_uppercase()));
            }
        }
        if !seen.contains("escape") {
            o.escape = o.quote;
        }
        if o.null.as_bytes().contains(&o.delimiter) {
            return copy_error(
                SqlError::InvalidParameterValue,
                "COPY delimiter character must not appear in the NULL specification",
            );
        }
        if csv && o.null.as_bytes().contains(&o.quote) {
            return copy_error(
                SqlError::InvalidParameterValue,
                "CSV quote character must not appear in the NULL specification",
            );
        }
        if freeze && !from {
            return copy_error(SqlError::InvalidParameterValue, "COPY FREEZE cannot be used with COPY TO");
        }
        if let Some(v) = values.get("default") {
            if !from {
                return copy_error(
                    SqlError::Unsupported,
                    "COPY DEFAULT cannot be used with COPY TO",
                );
            }
            if v.as_bytes().contains(&o.delimiter) {
                return copy_error(
                    SqlError::Unsupported,
                    "COPY delimiter character must not appear in the DEFAULT specification",
                );
            }
            if csv && v.as_bytes().contains(&o.quote) {
                return copy_error(
                    SqlError::Unsupported,
                    "CSV quote character must not appear in the DEFAULT specification",
                );
            }
            if v == &o.null {
                return copy_error(
                    SqlError::Unsupported,
                    "NULL specification and DEFAULT specification cannot be the same",
                );
            }
        }
        if binary && ignore {
            return copy_error(
                SqlError::Syntax,
                "only ON_ERROR STOP is allowed in BINARY mode",
            );
        }
        if seen.contains("reject_limit") && !ignore {
            return copy_error(
                SqlError::InvalidParameterValue,
                "COPY REJECT_LIMIT requires ON_ERROR to be set to IGNORE",
            );
        }
        o.default = values.get("default").cloned();
        o.on_error_ignore = ignore;
        o.reject_limit = values.get("reject_limit").map(|v| v.parse().unwrap());
        o.log_verbosity = match values.get("log_verbosity").map(|v| v.to_ascii_lowercase()).as_deref() {
            Some("verbose") => CopyLogVerbosity::Verbose,
            Some("silent") => CopyLogVerbosity::Silent,
            _ => CopyLogVerbosity::Default,
        };
        o.freeze = freeze;
        if o.encoding.is_some_and(|e| !e.supported()) {
            return Err(SqlError::Unsupported);
        }
        Ok(o)
    }
    pub fn validate_columns(&self, table: &str, all: &[String], selected: &[String]) -> R<()> {
        for (name, cols) in [
            ("FORCE_QUOTE", &self.force_quote),
            ("FORCE_NOT_NULL", &self.force_not_null),
            ("FORCE_NULL", &self.force_null),
        ] {
            for c in &cols.names {
                if !all.contains(c) {
                    return copy_error(
                        SqlError::UnknownColumn,
                        format!("column \"{c}\" of relation \"{table}\" does not exist"),
                    );
                }
                if !selected.contains(c) {
                    return copy_error(
                        SqlError::InvalidColumnReference,
                        format!("{name} column \"{c}\" not referenced by COPY"),
                    );
                }
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopySource { Stdin, File(String), Program(String) }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Copy {
    pub target_is_view: bool,
    pub source: CopySource,
    pub table: String,
    relation: schemas::Reference,
    pub columns: Vec<String>,
    pub format: CopyFormat,
    pub delimiter: u8,
    pub null: String,
    pub options: CopyOptions,
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
        Self::target_reference(sql)
            .map(|name| name.map(|name| relation_key(split_key(&name).0, split_key(&name).1)))
    }
    #[doc(hidden)]
    pub fn target_reference(sql: &str) -> R<Option<String>> {
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
            privilege_casts: false,
            privilege_seen: false,
            tokens: lex(&sql[..end])?,
            at: 0,
            params: BTreeSet::new(),
            relations: Vec::new(),
        };
        p.need("copy")?;
        p.table_name().map(Some)
    }
    pub fn parse(sql: &str) -> R<Self> {
        let mut p = Parser {
            privilege_casts: false,
            privilege_seen: false,
            tokens: lex(sql)?,
            at: 0,
            params: BTreeSet::new(),
            relations: Vec::new(),
        };
        p.need("copy")?;
        if p.tokens.get(p.at) == Some(&Token::Symbol('(')) {
            return Err(SqlError::Unsupported);
        }
        let table = p.table_name()?;
        let table = relation_key(split_key(&table).0, split_key(&table).1);
        let mut columns = vec![];
        if p.sym('(') {
            columns.push(p.column_name()?);
            while p.sym(',') {
                columns.push(p.column_name()?);
            }
            p.punct(')')?;
        }
        if p.word("to") {
            return Err(SqlError::Unsupported);
        }
        p.need("from")?;
        let source = if p.word("stdin") { CopySource::Stdin } else {
            let program = p.word("program");
            let Some(Token::String(value)) = p.tokens.get(p.at) else { return Err(SqlError::Syntax); };
            let value = value.clone(); p.at += 1;
            if program { CopySource::Program(value) } else { CopySource::File(value) }
        };
        let mut copy = Self {
            source,
            relation: p.relations[0].clone(),
            table,
            columns,
            format: CopyFormat::Text,
            delimiter: b'\t',
            null: "\\N".into(),
            target_is_view: false,
            options: CopyOptions::default(),
        };
        copy.options = CopyOptions::parse_tokens(&mut p, true)?;
        copy.format = copy.options.format;
        copy.delimiter = copy.options.delimiter;
        copy.null = copy.options.null.clone();
        Ok(copy)
    }
    pub(crate) fn columns_in(&self, state: &Relational) -> R<Vec<ColumnDef>> {
        let table = state.tables.get(&self.table).ok_or(SqlError::UnknownTable)?;
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
    pub(crate) fn schema_key(&self, state: &Relational, path: &[String]) -> String {
        let name = self.relation.schema.as_ref().map_or_else(
            || self.relation.name.clone(),
            |schema| format!("{schema}\0{}", self.relation.name),
        );
        state.resolve_relation_key(&name, path)
    }
    pub(crate) fn prepared(&self, state: &Relational, domain: u128) -> R<Prepared> {
        let mut copy = self.clone();
        if self.table.contains('\0') {
            copy.table = relation_key(split_key(&self.table).0, split_key(&self.table).1);
        }
        let columns = copy.columns_in(state)?;
        let table = &state.tables[&copy.table];
        copy.options.validate_columns(split_key(&copy.table).1,
            &table.logical_columns().map(|c| c.name.clone()).collect::<Vec<_>>(),
            &columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>())?;
        let width = columns.len();
        let types = columns.into_iter().map(|c| c.data_type).collect();
        Ok(Prepared {
            domain,
            epoch: state.epoch,
            transaction: None,
            engine_generation: None,
            params: 0,
            parameter_types: vec![],
            declared_enum_parameters: None,
            relations: vec![self.relation.clone()],
            search_path: vec!["public".into()],
            schema_user: "giggles".into(),
            extension_owner: std::sync::Arc::new(std::sync::atomic::AtomicU32::new(10)),
            native_delete: None,
            native_insert_many: false,
            unbound_statement: None,
            literal_date_order: clock::LiteralDateOrder::current(),
            warnings: vec![],
            authorization_has_views: false,
            authorization_binding: None,
            privilege_inquiry: false,
            statement: prepared_syntax::Syntax::new(Statement::InsertMulti(Box::new(sql_insert::Insert::copy(
                format!(
                    "{}\0{}",
                    if copy.target_is_view { split_key(&copy.table).0 } else { state.table_schema(table.id) },
                    split_key(&copy.table).1
                ),
                self.columns.clone(),
                types,
                vec![vec![Value::Null; width]],
            )))),
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
        prepared.unbound_statement = None;
        // COPY rows belong to the target bound by the already accepted header.
        prepared.relations.clear();
        let Statement::InsertMulti(insert) = prepared.statement.get_mut() else {
            return Err(SqlError::Unsupported);
        };
        insert.replace_copy_rows(rows, clock)?;
        Ok(prepared)
    }
}

/// Runtime COPY cells: `None` denotes DEFAULT; SQL NULL is `Some(Value::Null)`.
#[derive(Debug, Clone)]
pub struct CopyInput {
    pub rows: Vec<Vec<Option<Value>>>,
    pub options: CopyOptions,
    pub first_line: u64,
    pub skipped: u64,
}
impl Relational {
    pub(crate) fn normalize_copy(
        &self, prepared: &Prepared, input: CopyInput,
        clock: Option<&clock::ExecutionClock>,
    ) -> R<(Prepared, Vec<Vec<Value>>, u64)> {
        sql_insert::normalize_copy(self, prepared, input, clock)
    }
}

/// COPY text input equals unknown-literal assignment (DD-0068 F18).
pub(super) fn copy_text_field(
    text: String,
    ty: &DataType,
    clock: Option<&clock::ExecutionClock>,
) -> R<Value> {
    query::assignment::literal(Value::Text(text), ty, clock)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn runtime_default_and_ignore_options() {
        let options = CopyOptions::parse("(DEFAULT 'DEF', ON_ERROR ignore, REJECT_LIMIT 2, LOG_VERBOSITY verbose)", true).unwrap();
        assert_eq!(options.format, CopyFormat::Text);
        for verbosity in ["verbose", "silent"] { assert!(CopyOptions::parse(&format!("(LOG_VERBOSITY {verbosity})"), true).is_ok()); }
    }
    #[test]
    fn shared_copy_options() {
        for sql in ["COPY t FROM STDIN CSV HEADER QUOTE AS '~' FORCE NOT NULL a FORCE NULL b", "COPY t FROM STDIN WITH (FORMAT csv, HEADER true, QUOTE '~', FORCE_NOT_NULL (a), FORCE_NULL (b))"] {
            let c = Copy::parse(sql).unwrap();
            assert_eq!(c.options.header, CopyHeader::On);
            assert_eq!(c.options.escape, b'~');
            assert!(c.options.force_not_null.contains("a"));
            assert!(c.options.force_null.contains("b"));
        }
        assert_eq!(CopyOptions::parse("(HEADER MATCH)", true).unwrap().header, CopyHeader::Match);
    }
    #[test]
    fn option_error_messages() {
        for (sql, from, error, message) in [
            ("OIDS", false, SqlError::Syntax, "syntax error at or near \"OIDS\""),
            ("(HEADER 2)", true, SqlError::Syntax, "header requires a Boolean value or \"match\""),
            ("(FORMAT xml)", true, SqlError::InvalidParameterValue, "COPY format \"xml\" not recognized"),
            ("(HEADER, HEADER false)", true, SqlError::Syntax, "conflicting or redundant options"),
            ("(bogus 1)", true, SqlError::Syntax, "option \"bogus\" not recognized"),
            ("(FORMAT binary, HEADER)", false, SqlError::Unsupported, "cannot specify HEADER in BINARY mode"),
            ("(FORMAT csv, QUOTE '~~')", true, SqlError::Unsupported, "COPY quote must be a single one-byte character"),
            ("(FORMAT csv, FORCE_NULL *)", false, SqlError::InvalidParameterValue, "COPY FORCE_NULL cannot be used with COPY TO"),
            ("(FORMAT binary, DEFAULT 'x')", true, SqlError::Syntax, "cannot specify DEFAULT in BINARY mode"),
            ("(REJECT_LIMIT 1)", true, SqlError::InvalidParameterValue, "COPY REJECT_LIMIT requires ON_ERROR to be set to IGNORE"),
        ] {
            assert_eq!(CopyOptions::parse(sql, from), Err(error), "{sql}");
            assert_eq!(user_types::take_sql_error_message().as_deref(), Some(message), "{sql}");
        }
    }
}
