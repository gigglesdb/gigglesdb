//! PostgreSQL-listener GUC grammar; deliberately separate from the agent parser.
use super::{Result, Setting, Show};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Token {
    Word(String),
    Ident(String),
    Text(String),
    Comma,
    Equals,
}
pub(crate) fn lex(sql: &str) -> Result<Vec<Token>> {
    let mut chars = sql
        .trim()
        .strip_suffix(';')
        .unwrap_or(sql.trim())
        .trim()
        .chars()
        .peekable();
    let mut tokens = Vec::new();
    while let Some(c) = chars.next() {
        if c.is_whitespace() {
            continue;
        }
        tokens.push(match c {
            ',' => Token::Comma,
            '=' => Token::Equals,
            '\'' | '"' => {
                let mut value = String::new();
                loop {
                    match chars.next() {
                        Some(x) if x == c => {
                            if chars.peek() == Some(&c) {
                                chars.next();
                                value.push(c);
                            } else {
                                break;
                            }
                        }
                        Some(x) => value.push(x),
                        None => return Err("42601"),
                    }
                }
                if c == '\'' {
                    Token::Text(value)
                } else {
                    Token::Ident(value)
                }
            }
            c if c.is_alphabetic() || c == '_' => {
                let mut value = String::from(c);
                while chars
                    .peek()
                    .is_some_and(|c| c.is_alphanumeric() || matches!(c, '_' | '$'))
                {
                    value.push(chars.next().unwrap());
                }
                Token::Word(value)
            }
            c if c.is_ascii_digit() || matches!(c, '-' | '+' | '.') => {
                let mut value = String::from(c);
                while chars
                    .peek()
                    .is_some_and(|c| c.is_ascii_digit() || matches!(c, '-' | '+' | '.' | 'e' | 'E'))
                {
                    value.push(chars.next().unwrap());
                }
                if value.parse::<f64>().is_err() {
                    return Err("42601");
                }
                Token::Word(value)
            }
            _ => return Err("42601"),
        });
    }
    Ok(tokens)
}
pub(crate) struct Parser {
    pub tokens: Vec<Token>,
    pub at: usize,
}
impl Parser {
    pub fn new(sql: &str) -> Result<Self> {
        Ok(Self {
            tokens: lex(sql)?,
            at: 0,
        })
    }
    pub fn kw(&mut self, word: &str) -> bool {
        if matches!(self.tokens.get(self.at), Some(Token::Word(w)) if w.eq_ignore_ascii_case(word))
        {
            self.at += 1;
            true
        } else {
            false
        }
    }
    pub fn need(&mut self, word: &str) -> Result<()> {
        if self.kw(word) { Ok(()) } else { Err("42601") }
    }
    pub fn end(&self) -> Result<()> {
        if self.at == self.tokens.len() {
            Ok(())
        } else {
            Err("42601")
        }
    }
    pub fn comma(&mut self) -> bool {
        if self.tokens.get(self.at) == Some(&Token::Comma) {
            self.at += 1;
            true
        } else {
            false
        }
    }
    fn name(&mut self) -> Result<Show> {
        if self.kw("TIME") {
            self.need("ZONE")?;
            return Ok(Show::TimeZone);
        }
        let name = match self.tokens.get(self.at) {
            Some(Token::Word(s) | Token::Ident(s)) => s.to_ascii_lowercase(),
            _ => return Err("42601"),
        };
        self.at += 1;
        lookup_name(&name)
    }
    fn values(&mut self) -> Result<Vec<Token>> {
        let mut values = Vec::new();
        loop {
            match self.tokens.get(self.at) {
                Some(t @ (Token::Word(_) | Token::Ident(_) | Token::Text(_))) => {
                    values.push(t.clone())
                }
                _ => return Err("42601"),
            }
            self.at += 1;
            if !self.comma() {
                break;
            }
        }
        self.end()?;
        Ok(values)
    }
}
pub(crate) fn lookup_name(name: &str) -> Result<Show> {
    match name.to_ascii_lowercase().as_str() {
        "application_name" => Ok(Show::ApplicationName),
        "extra_float_digits" => Ok(Show::ExtraFloatDigits),
        "timezone" => Ok(Show::TimeZone),
        "datestyle" => Ok(Show::DateStyle),
        "client_encoding" => Ok(Show::ClientEncoding),
        "standard_conforming_strings" => Ok(Show::StandardConformingStrings),
        "search_path" => Ok(Show::SearchPath),
        "intervalstyle" => Ok(Show::IntervalStyle),
        "bytea_output" => Ok(Show::ByteaOutput),
        "statement_timeout" => Ok(Show::StatementTimeout),
        "lock_timeout" => Ok(Show::LockTimeout),
        "idle_in_transaction_session_timeout" => Ok(Show::IdleInTransactionSessionTimeout),
        "transaction_isolation" => Ok(Show::TransactionIsolation),
        "default_transaction_isolation" => Ok(Show::DefaultTransactionIsolation),
        "transaction_read_only" => Ok(Show::TransactionReadOnly),
        "default_transaction_read_only" => Ok(Show::DefaultTransactionReadOnly),
        "transaction_timeout" => Ok(Show::TransactionTimeout),
        "client_min_messages" => Ok(Show::ClientMinMessages),
        "row_security" => Ok(Show::RowSecurity),
        "check_function_bodies" => Ok(Show::CheckFunctionBodies),
        "synchronize_seqscans" => Ok(Show::SynchronizeSeqscans),
        "restrict_nonsystem_relation_kind" => Ok(Show::RestrictNonsystemRelationKind),
        "xmloption" => Ok(Show::XmlOption),
        "default_tablespace" => Ok(Show::DefaultTablespace),
        "default_table_access_method" => Ok(Show::DefaultTableAccessMethod),
        "server_version" => Ok(Show::ServerVersion),
        "server_version_num" => Ok(Show::ServerVersionNum),
        _ => Err("42704"),
    }
}
fn first(sql: &str, keyword: &str) -> bool {
    sql.trim_start()
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .next()
        .is_some_and(|s| s.eq_ignore_ascii_case(keyword))
}
pub(crate) fn show_database(sql: &str) -> Result<Option<Show>> {
    if !first(sql, "SHOW") {
        return Ok(None);
    }
    let mut p = Parser::new(sql)?;
    p.need("SHOW")?;
    let name = if p.kw("TRANSACTION") {
        p.need("ISOLATION")?;
        p.need("LEVEL")?;
        Show::TransactionIsolation
    } else {
        p.name()?
    };
    p.end()?;
    Ok(Some(name))
}
pub(crate) fn parse_database(sql: &str) -> Result<Option<Vec<Setting>>> {
    if !first(sql, "SET") && !first(sql, "RESET") {
        return Ok(None);
    }
    let mut p = Parser::new(sql)?;
    let reset = p.kw("RESET");
    if reset && p.kw("ALL") {
        p.end()?;
        return Ok(Some(vec![Setting::ResetAll]));
    }
    let local = if !reset {
        p.need("SET")?;
        if p.kw("SESSION") {
            false
        } else {
            p.kw("LOCAL")
        }
    } else {
        false
    };
    let time_zone =
        matches!(p.tokens.get(p.at), Some(Token::Word(w)) if w.eq_ignore_ascii_case("TIME"));
    let name = p.name()?;
    if matches!(
        name,
        Show::ServerVersion
            | Show::ServerVersionNum
            | Show::TransactionIsolation
            | Show::TransactionReadOnly
    ) {
        return Err("22023");
    }
    let value = if reset {
        p.end()?;
        None
    } else {
        if !time_zone {
            if p.tokens.get(p.at) == Some(&Token::Equals) {
                p.at += 1;
            } else {
                p.need("TO")?;
            }
        }
        if p.kw("DEFAULT") || (name == Show::TimeZone && p.kw("LOCAL")) {
            p.end()?;
            None
        } else {
            Some(validate(name, p.values()?)?)
        }
    };
    Ok(Some(vec![Setting::Database {
        name,
        value,
        local,
        reset,
    }]))
}
fn text(token: &Token) -> &str {
    match token {
        Token::Word(s) | Token::Ident(s) | Token::Text(s) => s,
        _ => unreachable!(),
    }
}
fn validate(name: Show, values: Vec<Token>) -> Result<String> {
    if name == Show::SearchPath {
        return search_path(values);
    }
    let value = if name == Show::DateStyle {
        values.iter().map(text).collect::<Vec<_>>().join(", ")
    } else {
        if values.len() != 1 {
            return Err("22023");
        }
        text(&values[0]).to_owned()
    };
    let lower = value.to_ascii_lowercase();
    match name {
        Show::DefaultTransactionReadOnly => match lower.as_str() {
            "on" | "true" | "yes" | "1" => Ok("on".into()),
            "off" | "false" | "no" | "0" => Ok("off".into()),
            _ => Err("22023"),
        },
        Show::DefaultTransactionIsolation => crate::pg_transaction::IsolationLevel::parse(&lower)
            .map(|requested| {
                crate::pg_transaction::effective_isolation(requested)
                    .name()
                    .into()
            }),
        Show::ApplicationName if value.len() <= 128 => Ok(value),
        Show::ExtraFloatDigits => value
            .parse::<i32>()
            .ok()
            .filter(|n| (-15..=3).contains(n))
            .map(|n| n.to_string())
            .ok_or("22023"),
        Show::TimeZone
            if !value.is_empty()
                && value.len() <= 128
                && value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"/_+-:.".contains(&b)) =>
        {
            Ok(value)
        }
        Show::DateStyle => {
            let parts: Vec<_> = lower.split(',').map(str::trim).collect();
            match parts.as_slice() {
                ["iso"] | ["iso", "mdy"] => Ok("ISO, MDY".into()),
                ["iso", "dmy"] => Ok("ISO, DMY".into()),
                ["iso", "ymd"] => Ok("ISO, YMD".into()),
                _ => Err("22023"),
            }
        }
        Show::ClientEncoding if matches!(lower.as_str(), "utf8" | "utf-8" | "unicode") => {
            Ok("UTF8".into())
        }
        Show::StandardConformingStrings if lower == "on" => Ok("on".into()),
        Show::IntervalStyle if lower == "postgres" => Ok("postgres".into()),
        Show::ByteaOutput if lower == "hex" => Ok("hex".into()),
        Show::ClientMinMessages
            if matches!(
                lower.as_str(),
                "debug5"
                    | "debug4"
                    | "debug3"
                    | "debug2"
                    | "debug1"
                    | "log"
                    | "notice"
                    | "warning"
                    | "error"
            ) =>
        {
            Ok(lower)
        }
        Show::CheckFunctionBodies | Show::SynchronizeSeqscans => {
            boolean(&lower).map(|v| if v { "on" } else { "off" }.into())
        }
        Show::RowSecurity => {
            if boolean(&lower)? {
                Err("0A000")
            } else {
                Ok("off".into())
            }
        }
        Show::RestrictNonsystemRelationKind
            if value.trim().is_empty()
                || lower
                    .split(',')
                    .all(|part| matches!(part.trim(), "view" | "foreign-table")) =>
        {
            Ok(value)
        }
        Show::XmlOption if matches!(lower.as_str(), "content" | "document") => Ok(lower),
        Show::DefaultTablespace if value.is_empty() => Ok(value),
        Show::DefaultTableAccessMethod if lower == "heap" => Ok(lower),
        Show::TransactionTimeout
        | Show::StatementTimeout
        | Show::LockTimeout
        | Show::IdleInTransactionSessionTimeout => {
            if value.parse::<u64>() == Ok(0) {
                Ok("0".into())
            } else {
                Err("22023")
            }
        }
        _ => Err("22023"),
    }
}
fn search_path(values: Vec<Token>) -> Result<String> {
    // A string literal can contain an entire identifier list, including quotes.
    let values = if let [Token::Text(value)] = values.as_slice() {
        let mut p = Parser::new(value).map_err(|_| "22023")?;
        if p.tokens.is_empty() {
            return Ok("\"\"".into());
        }
        p.values().map_err(|_| "22023")?
    } else {
        values
    };
    let mut schemas = Vec::new();
    for token in values {
        let name = match token {
            Token::Word(s) => s.to_lowercase(),
            Token::Ident(s) | Token::Text(s) => s,
            _ => return Err("22023"),
        };
        if name.is_empty() {
            schemas.push("\"\"".into());
            continue;
        }
        if name.len() > 63 {
            return Err("22023");
        }
        let simple = name.bytes().enumerate().all(|(i, b)| {
            b.is_ascii_lowercase() || b == b'_' || (i > 0 && (b.is_ascii_digit() || b == b'$'))
        });
        let reserved = matches!(
            name.as_str(),
            "select"
                | "from"
                | "where"
                | "table"
                | "user"
                | "current_user"
                | "session_user"
                | "order"
                | "group"
                | "default"
                | "all"
                | "null"
                | "true"
                | "false"
                | "public"
        );
        if simple && (!reserved || name == "public") {
            schemas.push(name);
        } else {
            schemas.push(format!("\"{}\"", name.replace('"', "\"\"")));
        }
    }
    Ok(schemas.join(", "))
}

fn boolean(value: &str) -> Result<bool> {
    match value {
        "on" | "true" | "yes" | "1" => Ok(true),
        "off" | "false" | "no" | "0" => Ok(false),
        _ => Err("22023"),
    }
}
pub(super) fn function_assignment(name: &str, value: &str, local: bool) -> Result<(Show, Setting)> {
    let name = lookup_name(name)?;
    if matches!(
        name,
        Show::ServerVersion
            | Show::ServerVersionNum
            | Show::TransactionIsolation
            | Show::TransactionReadOnly
    ) {
        return Err("22023");
    }
    let validated = validate(name, vec![Token::Text(value.into())])?;
    // set_config passes a raw list string to PostgreSQL's GUC check hook; it
    // bypasses SQL SET's GUC_LIST_QUOTE flattening, including for empty strings.
    let value = if name == Show::SearchPath {
        value.into()
    } else {
        validated
    };
    Ok((
        name,
        Setting::Database {
            name,
            value: Some(value),
            local,
            reset: false,
        },
    ))
}
