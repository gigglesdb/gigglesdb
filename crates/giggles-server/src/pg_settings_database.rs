//! PostgreSQL-listener GUC grammar; deliberately separate from the agent parser.
use super::{RegistryName, Result, Setting, SettingName, Show, registry};
use std::ops::Range;

use giggles_pg_types::{
    bytea::ByteaOutput,
    temporal::{DateOrder, DateStyle, DateStyleSettings},
};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DateStyleError {
    List,
    Conflict,
    Keyword(String),
}

impl DateStyleError {
    pub(crate) fn detail(&self) -> String {
        match self {
            Self::List => "List syntax is invalid.".into(),
            Self::Conflict => "Conflicting \"DateStyle\" specifications.".into(),
            Self::Keyword(value) => format!("Unrecognized key word: \"{value}\"."),
        }
    }
}

fn date_style_keywords(value: &str) -> std::result::Result<Vec<String>, DateStyleError> {
    let mut rest = value.trim();
    let mut words = Vec::new();
    if rest.is_empty() {
        return Ok(words);
    }
    loop {
        let word = if let Some(after_quote) = rest.strip_prefix('"') {
            rest = after_quote;
            let mut word = String::new();
            loop {
                let (text, after) = rest.split_once('"').ok_or(DateStyleError::List)?;
                word.push_str(text);
                if let Some(after_escape) = after.strip_prefix('"') {
                    word.push('"');
                    rest = after_escape;
                } else {
                    rest = after;
                    break;
                }
            }
            word
        } else {
            let end = rest.find(|c: char| c == ',' || c.is_whitespace()).unwrap_or(rest.len());
            let (word, after) = rest.split_at(end);
            rest = after;
            if word.is_empty() {
                return Err(DateStyleError::List);
            }
            word.to_ascii_lowercase()
        };
        words.push(word);
        rest = rest.trim_start();
        if rest.is_empty() {
            return Ok(words);
        }
        rest = rest.strip_prefix(',').ok_or(DateStyleError::List)?.trim_start();
        if rest.is_empty() {
            return Err(DateStyleError::List);
        }
    }
}

pub(crate) fn date_style(
    value: &str,
    current: DateStyleSettings,
) -> std::result::Result<DateStyleSettings, DateStyleError> {
    use DateOrder::*;
    use DateStyle::*;
    let mut style = None;
    let mut order = None;
    let mut use_default = false;
    for keyword in date_style_keywords(value)? {
        let (new_style, new_order) = match keyword.to_ascii_lowercase().as_str() {
            "iso" => (Some(Iso), None),
            "sql" => (Some(Sql), None),
            "postgres" => (Some(Postgres), None),
            "german" => (Some(German), None),
            "ymd" => (None, Some(Ymd)),
            "dmy" | "euro" | "european" => (None, Some(Dmy)),
            "mdy" | "us" | "noneuro" | "noneuropean" => (None, Some(Mdy)),
            "default" => {
                use_default = true;
                continue;
            }
            _ => return Err(DateStyleError::Keyword(keyword)),
        };
        if new_style.is_some_and(|next| style.is_some_and(|old| old != next))
            || new_order.is_some_and(|next| order.is_some_and(|old| old != next))
        {
            return Err(DateStyleError::Conflict);
        }
        style = new_style.or(style);
        order = new_order.or(order);
    }
    let fallback = if use_default { DateStyleSettings::default() } else { current };
    Ok(DateStyleSettings {
        style: style.unwrap_or(fallback.style),
        order: order.unwrap_or(if style == Some(German) { Dmy } else { fallback.order }),
    })
}

pub(crate) fn bytea_output(value: &str) -> Result<ByteaOutput> {
    if value.eq_ignore_ascii_case("hex") {
        Ok(ByteaOutput::Hex)
    } else if value.eq_ignore_ascii_case("escape") {
        Ok(ByteaOutput::Escape)
    } else {
        Err("22023")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Token {
    Word(String),
    Ident(String),
    Text(String),
    Comma,
    Equals,
    Dot,
    NumericJunk(String),
    Invalid,
}
pub(crate) fn lex(sql: &str) -> Result<Vec<Token>> {
    setting_tokens(sql, false)
        .map(|(token, _)| if token == Token::Invalid { Err("42601") } else { Ok(token) })
        .collect()
}

fn setting_tokens(sql: &str, for_show: bool) -> impl Iterator<Item = (Token, Range<usize>)> {
    let sql = sql.trim_end();
    let sql = sql.strip_suffix(';').unwrap_or(sql);
    let mut chars = sql.char_indices().peekable();
    std::iter::from_fn(move || {
        let (start, c) = chars.by_ref().find(|(_, c)| !c.is_whitespace())?;
        let token = (|| {
            Some(match c {
                ',' => Token::Comma,
                '=' if !for_show => Token::Equals,
                '.' if for_show && chars.peek().is_some_and(|(_, c)| *c == '.') => {
                    chars.next();
                    Token::Invalid
                }
                '.' if !chars.peek().is_some_and(|(_, c)| c.is_ascii_digit()) => Token::Dot,
                '\'' | '"' => {
                    let mut value = String::new();
                    loop {
                        let (_, x) = chars.next()?;
                        if x == c {
                            if chars.peek().is_some_and(|(_, next)| *next == c) {
                                chars.next();
                                value.push(c);
                            } else {
                                break;
                            }
                        } else {
                            value.push(x);
                        }
                    }
                    if c == '\'' { Token::Text(value) } else { Token::Ident(value) }
                }
                c if c.is_alphabetic() || c == '_' => {
                    while chars.peek().is_some_and(|(_, c)| {
                        c.is_alphanumeric() || matches!(c, '_' | '$') || (!for_show && *c == '.')
                    }) {
                        chars.next();
                    }
                    let end = chars.peek().map_or(sql.len(), |(at, _)| *at);
                    Token::Word(sql[start..end].to_owned())
                }
                c if for_show && "+-*/<>=~!@#%^&|`?:".contains(c) => {
                    while chars.peek().is_some_and(|(_, c)| "*/<>=~!@#%^&|`?:".contains(*c)) {
                        chars.next();
                    }
                    Token::Invalid
                }
                '$' if for_show => {
                    while chars.peek().is_some_and(|(_, c)| c.is_ascii_digit()) {
                        chars.next();
                    }
                    Token::Invalid
                }
                c if c.is_ascii_digit() || matches!(c, '-' | '+' | '.') => {
                    let mut value = String::from(c);
                    while chars.peek().is_some_and(|(_, c)| {
                        c.is_ascii_digit() || matches!(c, '-' | '+' | '.' | 'e' | 'E')
                    }) {
                        value.push(chars.next().unwrap().1);
                    }
                    if value.parse::<f64>().is_err() {
                        return None;
                    }
                    if chars.peek().is_some_and(|(_, c)| c.is_alphabetic() || *c == '_') {
                        while chars.peek().is_some_and(|(_, c)| c.is_alphanumeric() || *c == '_') {
                            value.push(chars.next().unwrap().1);
                        }
                        Token::NumericJunk(value)
                    } else if for_show {
                        Token::Invalid
                    } else {
                        Token::Word(value)
                    }
                }
                _ => Token::Invalid,
            })
        })().unwrap_or(Token::Invalid);
        let end = chars.peek().map_or(sql.len(), |(at, _)| *at);
        Some((token, start..end))
    })
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
    fn keywords(&mut self, words: &[&str]) -> bool {
        let start = self.at;
        if words.iter().all(|word| self.kw(word)) {
            true
        } else {
            self.at = start;
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
    fn name(&mut self) -> Result<String> {
        if self.kw("TIME") {
            self.need("ZONE")?;
            return Ok("timezone".into());
        }
        let mut name = self.name_part()?;
        loop {
            if self.tokens.get(self.at) == Some(&Token::Dot) {
                self.at += 1;
                name.push('.');
            } else if !(name.ends_with('.') && matches!(self.tokens.get(self.at), Some(Token::Ident(_)))) {
                break;
            }
            name.push_str(&self.name_part()?);
        }
        Ok(name)
    }
    fn name_part(&mut self) -> Result<String> {
        let name = match self.tokens.get(self.at) {
            Some(Token::Word(s)) => s.to_ascii_lowercase(),
            Some(Token::Ident(s)) => s.clone(),
            _ => return Err("42601"),
        };
        self.at += 1;
        Ok(name)
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
pub(crate) fn lookup_native(name: &str) -> Result<Show> {
    match name.to_ascii_lowercase().as_str() {
        "role" => Ok(Show::Role),
        "session_authorization" => Ok(Show::SessionAuthorization),
        "application_name" => Ok(Show::ApplicationName),
        "extra_float_digits" => Ok(Show::ExtraFloatDigits),
        "gigglesdb.max_statement_tokens" => Ok(Show::MaxStatementTokens),
        "gigglesdb.max_value_bytes" => Ok(Show::MaxValueBytes),
        "timezone" => Ok(Show::TimeZone),
        "datestyle" => Ok(Show::DateStyle),
        "client_encoding" => Ok(Show::ClientEncoding),
        "standard_conforming_strings" => Ok(Show::StandardConformingStrings),
        "search_path" => Ok(Show::SearchPath),
        "intervalstyle" => Ok(Show::IntervalStyle),
        "session_replication_role" => Ok(Show::SessionReplicationRole),
        "bytea_output" => Ok(Show::ByteaOutput),
        "statement_timeout" => Ok(Show::StatementTimeout),
        "lock_timeout" => Ok(Show::LockTimeout),
        "idle_in_transaction_session_timeout" => Ok(Show::IdleInTransactionSessionTimeout),
        "transaction_isolation" => Ok(Show::TransactionIsolation),
        "default_transaction_isolation" => Ok(Show::DefaultTransactionIsolation),
        "transaction_read_only" => Ok(Show::TransactionReadOnly),
        "transaction_deferrable" => Ok(Show::TransactionDeferrable),
        "default_transaction_deferrable" => Ok(Show::DefaultTransactionDeferrable),
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
        "server_encoding" => Ok(Show::ServerEncoding),
        "giggles_capabilities" => Ok(Show::GigglesCapabilities),
        "server_version_num" => Ok(Show::ServerVersionNum),
        "max_identifier_length" => Ok(Show::MaxIdentifierLength),
        "block_size" => Ok(Show::BlockSize),
        "integer_datetimes" => Ok(Show::IntegerDatetimes),
        "max_index_keys" => Ok(Show::MaxIndexKeys),
        _ => Err("42704"),
    }
}
pub(crate) fn lookup_name(name: &str) -> std::result::Result<SettingName, registry::Error> {
    if let Ok(show) = lookup_native(name) {
        return Ok(SettingName::Native(show));
    }
    let lower = name.to_ascii_lowercase();
    let canonical = match lower.as_str() {
        "sort_mem" => "work_mem",
        "vacuum_mem" => "maintenance_work_mem",
        "ssl_ecdh_curve" => "ssl_groups",
        _ => lower.as_str(),
    };
    if let Some(entry) = registry::find(canonical) {
        return Ok(SettingName::Registry(RegistryName::Registered(entry)));
    }
    if lower.contains('.') && lower.split('.').all(|part| {
        let mut chars = part.chars();
        chars.next().is_some_and(|c| c == '_' || c.is_alphabetic())
            && chars.all(|c| matches!(c, '_' | '$') || c.is_alphanumeric())
    }) {
        return Ok(SettingName::Registry(RegistryName::Custom { key: lower, spelling: name.into() }));
    }
    Err(registry::Error::unknown(name))
}

pub(crate) fn lookup_assignment(name: &str) -> std::result::Result<SettingName, registry::Error> {
    lookup_name(name).map_err(|error| {
        if name.contains('.') {
            let mut error = registry::Error::new("42602", format!("invalid configuration parameter name \"{name}\""));
            error.detail = Some("Custom parameter names must be two or more simple identifiers separated by dots.");
            error
        } else {
            error
        }
    })
}

fn capture<T>(
    result: std::result::Result<T, registry::Error>,
    diagnostic: &mut Option<registry::Error>,
) -> Result<T> {
    result.map_err(|error| {
        let code = error.code;
        *diagnostic = Some(error);
        code
    })
}

fn first(sql: &str, keyword: &str) -> bool {
    sql.trim_start()
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .next()
        .is_some_and(|s| s.eq_ignore_ascii_case(keyword))
}
pub(crate) fn show_database(
    sql: &str,
    syntax_at: &mut Option<Range<usize>>,
    diagnostic: &mut Option<registry::Error>,
) -> Result<Option<SettingName>> {
    if !first(sql, "SHOW") {
        return Ok(None);
    }
    // Keep each token's original span for the wire diagnostic.
    let (tokens, spans): (Vec<_>, Vec<_>) = setting_tokens(sql, true).unzip();
    let mut p = Parser { tokens, at: 0 };
    let name = (|| -> Result<_> {
        p.need("SHOW")?;
        let name = if p.kw("ALL") {
            None
        } else if p.keywords(&["TIME", "ZONE"]) {
            Some("timezone".into())
        } else if p.keywords(&["TRANSACTION", "ISOLATION"]) {
            p.need("LEVEL")?;
            Some("transaction_isolation".into())
        } else if p.keywords(&["SESSION", "AUTHORIZATION"]) {
            Some("session_authorization".into())
        } else {
            let mut name = p.name_part()?;
            while p.tokens.get(p.at) == Some(&Token::Dot) {
                p.at += 1;
                name.push('.');
                name.push_str(&p.name_part()?);
            }
            Some(name)
        };
        p.end()?;
        Ok(name)
    })().inspect_err(|_| {
        *syntax_at = Some(spans.get(p.at).cloned().unwrap_or(sql.len()..sql.len()));
    })?;
    let name = match name {
        Some(name) => capture(lookup_name(&name), diagnostic)?,
        None => SettingName::All,
    };
    Ok(Some(name))
}
pub(crate) fn parse_database(
    sql: &str,
    syntax_at: &mut Option<String>,
    diagnostic: &mut Option<registry::Error>,
) -> Result<Option<Vec<Setting>>> {
    if !first(sql, "SET") && !first(sql, "RESET") {
        return Ok(None);
    }
    // The two role GUCs accept PostgreSQL's special SET grammar without TO.
    let mut role_parser = Parser::new(sql)?;
    let reset_role = role_parser.kw("RESET");
    if !reset_role { role_parser.need("SET")?; }
    let mut local_role = false;
    let session_prefix = role_parser.kw("SESSION");
    if !session_prefix { local_role = role_parser.kw("LOCAL"); }
    let role_name = if session_prefix && role_parser.kw("AUTHORIZATION") { Some(Show::SessionAuthorization) }
        else if role_parser.kw("ROLE") { Some(Show::Role) }
        else if role_parser.kw("SESSION_AUTHORIZATION") { Some(Show::SessionAuthorization) }
        else { None };
    if let Some(name) = role_name {
        let value = if reset_role { None } else {
            if role_parser.tokens.get(role_parser.at) == Some(&Token::Equals) { role_parser.at += 1; }
            else { role_parser.kw("TO"); }
            if role_parser.kw("DEFAULT") || (name == Show::Role && role_parser.kw("NONE")) { None }
            else {
                let token = role_parser.tokens.get(role_parser.at).ok_or("42601")?;
                let name = match token { Token::Word(s) => s.to_lowercase(), Token::Ident(s) | Token::Text(s) => s.clone(), _ => return Err("42601") };
                role_parser.at += 1;
                Some(name)
            }
        };
        role_parser.end()?;
        return Ok(Some(vec![Setting::Database { name, value, local: local_role, reset: reset_role }]));
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
    let raw_name = p.name()?;
    let name = match capture(lookup_assignment(&raw_name), diagnostic)? {
        SettingName::Native(name) => name,
        SettingName::Registry(name) => {
            let value = parse_registry_value(&mut p, &raw_name, &name, reset, syntax_at, diagnostic)?;
            return Ok(Some(vec![Setting::Registry { name, value, local, reset }]));
        }
        SettingName::All => unreachable!(),
    };
    if matches!(name, Show::MaxIdentifierLength | Show::BlockSize | Show::IntegerDatetimes) {
        return Err("55P02");
    }
    if matches!(
        name,
        Show::ServerVersion
            | Show::ServerEncoding
            | Show::GigglesCapabilities
            | Show::ServerVersionNum
            | Show::MaxIndexKeys
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
            if p.tokens.get(p.at) == Some(&Token::Comma) {
                *syntax_at = Some(",".into());
            }
            p.end()?;
            None
        } else if name == Show::TimeZone && p.kw("INTERVAL") {
            let interval = match p.tokens.get(p.at) {
                Some(Token::Text(s)) => s.clone(),
                _ => return Err("42601"),
            };
            p.at += 1;
            if p.kw("HOUR") { p.need("TO")?; p.need("MINUTE")?; }
            p.end()?;
            let interval = giggles_pg_types::temporal::Interval::parse(&interval).map_err(|_| "22023")?;
            let (months, days, micros) = (interval.months(), interval.days(), interval.microseconds());
            if months != 0 || days != 0 { return Err("22023"); }
            let seconds = i32::try_from(micros / 1_000_000).map_err(|_| "22023")?;
            Some(giggles_pg_types::temporal::SessionZone::interval_setting(seconds)
                .map_err(|_| "22023")?.1)
        } else {
            let values = p.values()?;
            if let Some(word) = values.iter().find_map(|value| match value {
                Token::Word(word) if word.eq_ignore_ascii_case("default") => Some(word),
                _ => None,
            }) {
                *syntax_at = Some(word.clone());
                return Err("42601");
            }
            if matches!(name, Show::TransactionDeferrable | Show::DefaultTransactionDeferrable)
                && let [value] = values.as_slice()
            {
                let entry = registry::find(&raw_name.to_ascii_lowercase()).expect("registered setting");
                Some(capture(entry.parse(text(value)), diagnostic)?)
            } else {
                Some(validate(name, values)?)
            }
        }
    };
    if name == Show::TransactionDeferrable && value.is_none() {
        return capture(Err(transaction_deferrable_reset()), diagnostic);
    }
    Ok(Some(vec![Setting::Database {
        name,
        value,
        local,
        reset,
    }]))
}
pub(super) fn transaction_deferrable_reset() -> registry::Error {
    registry::Error::new("0A000", "parameter \"transaction_deferrable\" cannot be reset".into())
}

fn parse_registry_value(
    p: &mut Parser,
    raw_name: &str,
    name: &RegistryName,
    reset: bool,
    syntax_at: &mut Option<String>,
    diagnostic: &mut Option<registry::Error>,
) -> Result<Option<String>> {
    if reset {
        p.end()?;
        return Ok(None);
    }
    if p.tokens.get(p.at) == Some(&Token::Equals) {
        p.at += 1;
    } else {
        p.need("TO")?;
    }
    if p.kw("DEFAULT") {
        if p.tokens.get(p.at) == Some(&Token::Comma) {
            *syntax_at = Some(",".into());
        }
        p.end()?;
        return Ok(None);
    }
    if let Some(Token::NumericJunk(token)) = p.tokens.get(p.at) {
        return capture(Err(registry::Error::new(
            "42601", format!("trailing junk after numeric literal at or near \"{token}\""),
        )), diagnostic);
    }
    let values = p.values()?;
    if let Some(word) = values.iter().find_map(|token| match token {
        Token::Word(word) if word.eq_ignore_ascii_case("default") => Some(word),
        _ => None,
    }) {
        *syntax_at = Some(word.clone());
        return Err("42601");
    }
    if name.name() == "temp_tablespaces" {
        let quoted: Vec<_> = values.iter().map(|token| {
            let value = match token {
                Token::Word(word) => word.to_ascii_lowercase(),
                _ => text(token).into(),
            };
            if matches!(value.as_str(), "pg_default" | "pg_global") {
                value
            } else {
                format!("\"{}\"", value.replace('"', "\"\""))
            }
        }).collect();
        return Ok(Some(quoted.join(", ")));
    }
    if let [value] = values.as_slice() {
        return Ok(Some(match value {
            Token::Word(word) => word.parse::<i32>().map_or_else(
                |_| word.to_ascii_lowercase(),
                |integer| integer.to_string(),
            ),
            _ => text(value).into(),
        }));
    }
    capture(Err(registry::Error::new("22023", format!("SET {raw_name} takes only one argument"))), diagnostic)
}

pub(super) fn temp_tablespaces(input: &str) -> std::result::Result<String, registry::Error> {
    let invalid = || registry::Error::new("22023", "invalid list syntax in parameter \"temp_tablespaces\"".into());
    let mut parser = Parser::new(input).map_err(|_| invalid())?;
    if parser.tokens.is_empty() {
        return Ok(String::new());
    }
    let values = parser.values().map_err(|_| invalid())?;
    for value in values {
        let name = match value {
            Token::Word(word) => word.to_ascii_lowercase(),
            Token::Ident(name) | Token::Text(name) => name,
            _ => unreachable!(),
        };
        if !matches!(name.as_str(), "" | "pg_default" | "pg_global") {
            return Err(registry::Error::new("42704", format!("tablespace \"{name}\" does not exist")));
        }
    }
    Ok(input.into())
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
        Show::TransactionDeferrable | Show::DefaultTransactionDeferrable => registry::parse_bool(&value)
            .map(|on| registry::bool_text(on).into()).ok_or("22023"),
        Show::DefaultTransactionReadOnly => match lower.as_str() {
            "on" | "true" | "yes" | "1" => Ok("on".into()),
            "off" | "false" | "no" | "0" => Ok("off".into()),
            _ => Err("22023"),
        },
        Show::DefaultTransactionIsolation => crate::pg_transaction::IsolationLevel::parse(&lower)
            .map(|requested| requested.name().into()),
        Show::ApplicationName if value.len() <= 128 => Ok(value),
        Show::MaxStatementTokens => value.parse::<i64>().ok().filter(|n| (0..=i64::from(i32::MAX)).contains(n)).map(|n| n.to_string()).ok_or("22023"),
        Show::MaxValueBytes => value.parse::<usize>().ok().filter(|n| (65536..=1073741819).contains(n)).map(|n| n.to_string()).ok_or("22023"),
        Show::ExtraFloatDigits => value
            .parse::<i32>()
            .ok()
            .filter(|n| (-15..=3).contains(n))
            .map(|n| n.to_string())
            .ok_or("22023"),
        Show::TimeZone => giggles_pg_types::temporal::SessionZone::setting(&value)
            .map(|(_, label)| label).map_err(|_| "22023"),
        // These accept raw input here. The execution-time check has the
        // current DateStyle and can attach the PostgreSQL diagnostic fields.
        Show::DateStyle | Show::ByteaOutput => Ok(value),
        Show::ClientEncoding if matches!(lower.as_str(), "utf8" | "utf-8" | "unicode") => {
            Ok("UTF8".into())
        }
        Show::StandardConformingStrings if lower == "on" => Ok("on".into()),
        Show::IntervalStyle => interval_style(&value),
        Show::SessionReplicationRole => replication_role(&value),
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
        Show::StatementTimeout | Show::LockTimeout => statement_timeout_ms(&lower).map(|n| n.to_string()),
        Show::TransactionTimeout
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
pub(crate) fn statement_timeout_ms(value: &str) -> Result<u64> {
            let (number, unit) = value.trim().find(|c: char| !c.is_ascii_digit()).map_or((value.trim(), ""), |at| value.trim().split_at(at));
            let factor = match unit.trim() { "" | "ms" => 1, "s" => 1000, "min" => 60000, "h" => 3600000, "d" => 86400000, _ => return Err("22023") };
            number.parse::<u64>().ok().and_then(|n| n.checked_mul(factor)).filter(|n| *n <= i32::MAX as u64).ok_or("22023")
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

pub(crate) fn replication_role(value: &str) -> Result<String> {
    match value.to_ascii_lowercase().as_str() {
        "origin" | "replica" | "local" => Ok(value.to_ascii_lowercase()),
        _ => Err("22023"),
    }
}

pub(crate) fn interval_style(value: &str) -> Result<String> {
    match value.to_ascii_lowercase().as_str() {
        "postgres" | "postgres_verbose" | "sql_standard" | "iso_8601" => {
            Ok(value.to_ascii_lowercase())
        }
        _ => Err("22023"),
    }
}

fn boolean(value: &str) -> Result<bool> {
    match value {
        "on" | "true" | "yes" | "1" => Ok(true),
        "off" | "false" | "no" | "0" => Ok(false),
        _ => Err("22023"),
    }
}
pub(crate) fn function_assignment(name: &str, value: &str, local: bool) -> Result<(Show, Setting)> {
    let name = lookup_native(name)?;
    if matches!(name, Show::MaxIdentifierLength | Show::BlockSize | Show::IntegerDatetimes) {
        return Err("55P02");
    }
    if matches!(
        name,
        Show::ServerVersion
            | Show::ServerEncoding
            | Show::GigglesCapabilities
            | Show::ServerVersionNum
            | Show::MaxIndexKeys
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
