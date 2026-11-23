//! Standalone configuration SELECTs and the PostgreSQL settings inventory.
use super::{RegistryName, Result, SessionSettings, Setting, SettingName, Show, database, registry};
use giggles_core::relational::{DataType, Value};

#[derive(Clone)]
pub(crate) enum Function {
    Inventory {
        fields: Vec<(String, DataType)>,
        projection: Vec<(usize, bool)>,
        filter: Option<Vec<String>>,
        count: bool,
        order: bool,
    },
    Advisory { name: String, key: (bool, i64), column: String },
    Set {
        additional: Vec<Function>,
        name: String,
        value: String,
        local: bool,
        from_settings: bool,
        column: String,
    },
    Current {
        additional: Vec<Function>,
        name: String,
        missing_ok: bool,
        column: String,
    },
}

pub(crate) enum FunctionError {
    Native(&'static str),
    Registry(registry::Error),
}
impl From<registry::Error> for FunctionError {
    fn from(error: registry::Error) -> Self {
        Self::Registry(error)
    }
}
impl From<&'static str> for FunctionError {
    fn from(code: &'static str) -> Self {
        Self::Native(code)
    }
}

impl Function {
    pub fn current_columns(&self) -> Option<Vec<&Function>> {
        match self {
            Self::Current { additional, .. } | Self::Set { additional, .. } => {
                let mut columns = vec![self];
                columns.extend(additional);
                Some(columns)
            }
            _ => None,
        }
    }

    pub fn column(&self) -> &str {
        match self {
            Self::Inventory { .. } => "setting",
            Self::Set { column, .. } | Self::Current { column, .. } | Self::Advisory { column, .. } => column,
        }
    }

    /// The catalog filter compares names case-sensitively before calling set_config.
    pub fn has_row(&self, privileged: bool) -> bool {
        match self {
            Self::Set { name, from_settings: true, .. } => {
                let lower = name.to_ascii_lowercase();
                match registry::find(&lower) {
                    Some(entry) => entry.name == name && (!entry.restricted || privileged),
                    None => database::lookup_native(name)
                        .is_ok_and(|show| show.value(&super::Settings::default()).0 == name),
                }
            }
            _ => true,
        }
    }

    /// Called within a real SQL transaction, including a statement's implicit one.
    pub fn evaluate_with_permissions(
        &self,
        settings: &mut SessionSettings,
        superuser: bool,
        privileged: bool,
        database: Option<&str>,
    ) -> std::result::Result<Option<String>, FunctionError> {
        match self {
            Self::Inventory { .. } | Self::Advisory { .. } => unreachable!("wire session function"),
            Self::Set { name, value, local, .. } => {
                let name = database::lookup_assignment(name)?;
                let assignment = match &name {
                    SettingName::Native(show) => {
                        match show {
                            Show::DateStyle => {
                                database::date_style(value, settings.current.datestyle).map_err(|_| "22023")?;
                            }
                            Show::ByteaOutput => { database::bytea_output(value)?; }
                            Show::TransactionDeferrable | Show::DefaultTransactionDeferrable => {
                                registry::find(name.name(settings)).expect("registered setting").parse(value)?;
                            }
                            _ => {}
                        }
                        let (_, assignment) = database::function_assignment(name.name(settings), value, *local)?;
                        assignment
                    }
                    SettingName::Registry(name) => Setting::Registry {
                        value: name.normalize(Some(value), superuser, database)?,
                        name: name.clone(),
                        local: *local,
                        reset: false,
                    },
                    SettingName::All => unreachable!(),
                };
                settings.apply(&assignment, true);
                Ok(Some(name.read(settings, privileged)?))
            }
            Self::Current { name, missing_ok, .. } => {
                match database::lookup_name(name).and_then(|name| name.read(settings, privileged)) {
                    Ok(value) => Ok(Some(value)),
                    Err(error) if error.code == "42704" && *missing_ok => Ok(None),
                    Err(error) => Err(error.into()),
                }
            }
        }
    }

    #[cfg(test)]
    pub fn evaluate(&self, settings: &mut SessionSettings) -> Result<Option<String>> {
        self.evaluate_with_permissions(settings, true, true, None).map_err(|error| match error {
            FunctionError::Native(code) => code,
            FunctionError::Registry(error) => error.code,
        })
    }
}

const INVENTORY_COLUMNS: &[&str] = &[
    "name", "setting", "unit", "category", "short_desc", "extra_desc", "context", "vartype",
    "source", "min_val", "max_val", "enumvals", "boot_val", "reset_val", "sourcefile",
    "sourceline", "pending_restart",
];

pub(crate) fn inventory_rows(
    function: &Function,
    settings: &SessionSettings,
    privileged: bool,
) -> Result<Vec<Vec<Value>>> {
    let Function::Inventory { projection, filter, count, order, .. } = function else {
        unreachable!("settings inventory");
    };
    let mut entries: Vec<_> = registry::ENTRIES.iter().filter(|entry| {
        (!entry.restricted || privileged)
            && filter.as_ref().is_none_or(|names| names.iter().any(|name| name == entry.name))
    }).map(|entry| (entry.name, Some(entry))).collect();
    // Native-only entries were directly queryable by name before the registry.
    // Preserve those lookups while enumeration uses the PostgreSQL inventory.
    if let Some(names) = filter {
        for name in names {
            let Ok(show) = database::lookup_native(name) else { continue };
            let canonical = show.value(&settings.current).0;
            if canonical == name
                && registry::find(&name.to_ascii_lowercase()).is_none()
                && !entries.iter().any(|(listed, _)| *listed == canonical)
            {
                entries.push((canonical, None));
            }
        }
    }
    if *count {
        return Ok(vec![vec![Value::Int(entries.len() as i64)]]);
    }
    if *order {
        entries.sort_by_key(|(name, _)| *name);
    }
    entries.into_iter().map(|(name, entry)| {
        let (current, reset, source) = if let Ok(show) = database::lookup_native(name) {
            let (current, reset) = match show {
                Show::StatementTimeout => (
                    settings.current.statement_timeout.to_string(), settings.defaults.statement_timeout.to_string(),
                ),
                Show::LockTimeout => (
                    settings.current.lock_timeout.to_string(), settings.defaults.lock_timeout.to_string(),
                ),
                _ => (show.value(&settings.current).1, show.value(&settings.defaults).1),
            };
            let source = settings.current.assigned.contains(&show) || current != reset;
            (current, reset, source)
        } else {
            let entry = entry.expect("registry entry");
            (
                entry.display(settings.registry_base(entry), false),
                entry.default.into(),
                settings.current.registered.contains_key(entry.name),
            )
        };
        let values = [
            Some(name), Some(current.as_str()), entry.and_then(|e| e.unit), entry.map(|e| e.category),
            entry.map(|e| e.short_desc), entry.and_then(|e| e.extra_desc), entry.map(|e| e.context_name),
            entry.map(|e| e.kind.name()), Some(if source { "session" } else { "default" }),
            entry.and_then(|e| e.min), entry.and_then(|e| e.max), None, entry.and_then(|e| e.boot),
            Some(reset.as_str()), None, None, None,
        ];
        projection.iter().map(|&(index, numeric)| {
            if index == 16 {
                return Ok(Value::Bool(false));
            }
            if index == 11 && let Some(entry) = entry && !entry.enum_values.is_empty() {
                let members: Vec<_> = entry.enum_values.iter()
                    .map(|value| format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\"")))
                    .collect();
                let array = Value::Text(format!("{{{}}}", members.join(",")));
                return giggles_core::relational::logical_types::coerce(array, &DataType::TextArray)
                    .map_err(|_| "XX000");
            }
            match values[index] {
                Some(text) if numeric => giggles_pg_types::Numeric::parse(text)
                    .map(Value::Numeric).map_err(|_| "22P02"),
                Some(text) => Ok(Value::Text(text.into())),
                None => Ok(Value::Null),
            }
        }).collect()
    }).collect()
}

pub(crate) fn show_all(settings: &SessionSettings, privileged: bool) -> Vec<Vec<Value>> {
    registry::ENTRIES.iter().filter(|entry| !entry.restricted || privileged).map(|entry| {
        let value = match database::lookup_name(entry.name).expect("registered name") {
            SettingName::Native(show) => show.value(&settings.current).1,
            SettingName::Registry(RegistryName::Registered(entry)) => entry.display(settings.registry_base(entry), true),
            _ => unreachable!("registered name"),
        };
        vec![Value::Text(entry.name.into()), Value::Text(value), Value::Text(entry.short_desc.into())]
    }).collect()
}
#[derive(Clone, PartialEq)]
enum Token {
    Word(String),
    Ident(String),
    Text(String),
    Left,
    Right,
    Dot,
    Comma,
    Equals,
    Number(i64),
    Cast,
    Star,
}
fn lex(sql: &str) -> Option<Vec<Token>> {
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
            ':' if chars.next()? == ':' => Token::Cast,
            '(' => Token::Left,
            ')' => Token::Right,
            '.' => Token::Dot,
            ',' => Token::Comma,
            '=' => Token::Equals,
            '*' => Token::Star,
            '\'' | '"' => {
                let mut value = String::new();
                loop {
                    match chars.next()? {
                        x if x == c => {
                            if chars.peek() == Some(&c) {
                                chars.next();
                                value.push(c);
                            } else {
                                break;
                            }
                        }
                        x => value.push(x),
                    }
                }
                if c == '\'' {
                    Token::Text(value)
                } else {
                    Token::Ident(value)
                }
            }
            c if c.is_ascii_digit() || matches!(c, '-' | '+') => {
                let mut value = String::from(c);
                while chars.peek().is_some_and(char::is_ascii_digit) { value.push(chars.next()?); }
                Token::Number(value.parse().ok()?)
            }
            c if c.is_alphabetic() || c == '_' => {
                let mut word = String::from(c);
                while chars
                    .peek()
                    .is_some_and(|c| c.is_alphanumeric() || matches!(c, '_' | '$'))
                {
                    word.push(chars.next()?);
                }
                Token::Word(word.to_ascii_lowercase())
            }
            _ => return None, // Includes $n parameters, operators and larger SQL expressions.
        });
    }
    Some(tokens)
}
struct Parser {
    tokens: Vec<Token>,
    at: usize,
}
impl Parser {
    fn token(&mut self, token: Token) -> Option<()> {
        if self.tokens.get(self.at) != Some(&token) {
            return None;
        }
        self.at += 1;
        Some(())
    }
    fn word(&mut self) -> Option<String> {
        let Some(Token::Word(word)) = self.tokens.get(self.at) else {
            return None;
        };
        self.at += 1;
        Some(word.clone())
    }
    fn text(&mut self) -> Option<String> {
        let Some(Token::Text(value)) = self.tokens.get(self.at) else {
            return None;
        };
        self.at += 1;
        Some(value.clone())
    }
    fn boolean(&mut self) -> Option<bool> {
        match self.word()?.as_str() {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        }
    }
}
// This is the existing bounded inventory SELECT, with rows supplied at execution.
fn inventory(tokens: Vec<Token>) -> Option<Function> {
    let mut p = Parser { tokens, at: 0 };
    p.token(Token::Word("select".into()))?;
    let mut fields = Vec::new();
    let mut projection = Vec::new();
    let mut count = false;
    if p.token(Token::Star).is_some() {
        for (index, name) in INVENTORY_COLUMNS.iter().enumerate() {
            fields.push(((*name).into(), inventory_type(index)));
            projection.push((index, false));
        }
    } else {
        loop {
            let name = p.word()?;
            if name == "count" && fields.is_empty() {
                p.token(Token::Left)?;
                p.token(Token::Star)?;
                p.token(Token::Right)?;
                let alias = if p.token(Token::Word("as".into())).is_some() { p.word()? } else { name };
                fields.push((alias, DataType::BigInt));
                count = true;
                break;
            }
            let index = INVENTORY_COLUMNS.iter().position(|&n| n == name)?;
            let numeric = p.token(Token::Cast).is_some();
            if numeric && (p.word()?.as_str() != "numeric" || !matches!(index, 1 | 9 | 10 | 12 | 13)) {
                return None;
            }
            let alias = if p.token(Token::Word("as".into())).is_some() { p.word()? } else { name };
            fields.push((alias, if numeric { DataType::Numeric } else { inventory_type(index) }));
            projection.push((index, numeric));
            if fields.len() > 128 {
                return None;
            }
            if p.token(Token::Comma).is_none() {
                break;
            }
        }
    }
    p.token(Token::Word("from".into()))?;
    let mut relation = p.word()?;
    if relation == "pg_catalog" {
        p.token(Token::Dot)?;
        relation = p.word()?;
    }
    if relation == "pg_show_all_settings" {
        p.token(Token::Left)?;
        p.token(Token::Right)?;
    } else if relation != "pg_settings" {
        return None;
    }
    let filter = if p.token(Token::Word("where".into())).is_some() {
        p.token(Token::Word("name".into()))?;
        if p.token(Token::Equals).is_some() {
            Some(vec![p.text()?])
        } else {
            p.token(Token::Word("in".into()))?;
            p.token(Token::Left)?;
            let mut names = Vec::new();
            loop {
                names.push(p.text()?);
                if names.len() > 399 {
                    return None;
                }
                if p.token(Token::Comma).is_none() {
                    break;
                }
            }
            p.token(Token::Right)?;
            Some(names)
        }
    } else {
        None
    };
    let order = p.at != p.tokens.len();
    if order {
        p.token(Token::Word("order".into()))?;
        p.token(Token::Word("by".into()))?;
        if p.token(Token::Word("name".into())).is_some() {
            if (count && fields.first()?.0 != "name")
                || fields.iter().zip(&projection).any(|((alias, _), (index, _))| {
                    alias == "name" && *index != 0
                })
            {
                return None;
            }
        } else {
            p.token(Token::Number(1))?;
            if fields.first()?.0 != "name" || (!count && projection.first()?.0 != 0) {
                return None;
            }
        }
    }
    (p.at == p.tokens.len()).then_some(Function::Inventory { fields, projection, filter, count, order })
}

fn inventory_type(index: usize) -> DataType {
    match index {
        11 => DataType::TextArray,
        15 => DataType::Integer,
        16 => DataType::Boolean,
        _ => DataType::Text,
    }
}

fn parse(sql: &str) -> Option<Function> {
    let tokens = lex(sql)?;
    let mut depth = 0;
    let mut start = 1;
    let mut parts = Vec::new();
    if tokens.first() != Some(&Token::Word("select".into())) { return None; }
    for (at, token) in tokens.iter().enumerate().skip(1) {
        match token {
            Token::Left => depth += 1,
            Token::Right => depth -= 1,
            Token::Comma if depth == 0 => { parts.push(tokens[start..at].to_vec()); start = at + 1; }
            _ => {}
        }
    }
    if !parts.is_empty() {
        parts.push(tokens[start..].to_vec());
        let mut calls = Vec::new();
        for mut part in parts {
            part.insert(0, Token::Word("select".into()));
            let call = parse_tokens(part)?;
            if !matches!(call, Function::Current { .. } | Function::Set { from_settings: false, .. }) { return None; }
            calls.push(call);
        }
        let mut first = calls.remove(0);
        match &mut first {
            Function::Current { additional, .. } | Function::Set { additional, .. } => *additional = calls,
            _ => unreachable!(),
        }
        return Some(first);
    }
    parse_tokens(tokens)
}
fn parse_tokens(tokens: Vec<Token>) -> Option<Function> {
    let mut p = Parser { tokens, at: 0 };
    p.token(Token::Word("select".into()))?;
    let mut function = p.word()?;
    if function == "pg_catalog" {
        p.token(Token::Dot)?;
        function = p.word()?;
    }
    if matches!(function.as_str(), "pg_advisory_lock" | "pg_try_advisory_lock" | "pg_advisory_unlock" | "pg_advisory_unlock_all" | "pg_advisory_xact_lock" | "pg_try_advisory_xact_lock") {
        p.token(Token::Left)?;
        let mut key = (false, 0);
        if function != "pg_advisory_unlock_all" {
            let Token::Number(first) = *p.tokens.get(p.at)? else { return None; }; p.at += 1;
            key.1 = first;
            if p.tokens.get(p.at) == Some(&Token::Comma) {
                p.at += 1;
                let Token::Number(second) = *p.tokens.get(p.at)? else { return None; }; p.at += 1;
                key = (true, (i64::from(i32::try_from(first).ok()?) << 32) | i64::from(i32::try_from(second).ok()? as u32));
            }
        }
        p.token(Token::Right)?;
        let column = if p.tokens.get(p.at) == Some(&Token::Word("as".into())) {
            p.at += 1;
            let column = match p.tokens.get(p.at)? { Token::Word(s) | Token::Ident(s) => s.clone(), _ => return None }; p.at += 1; column
        } else { function.clone() };
        return (p.at == p.tokens.len()).then_some(Function::Advisory { name: function, key, column });
    }
    if !matches!(function.as_str(), "set_config" | "current_setting") {
        return None;
    }
    p.token(Token::Left)?;
    let from_settings =
        function == "set_config" && p.tokens.get(p.at) == Some(&Token::Word("name".into()));
    let name = if from_settings {
        p.at += 1;
        String::new()
    } else {
        p.text()?
    };
    let mut call = if function == "set_config" {
        p.token(Token::Comma)?;
        let value = p.text()?;
        p.token(Token::Comma)?;
        let local = p.boolean()?;
        Function::Set {
            additional: vec![],
            name,
            value,
            local,
            from_settings,
            column: function,
        }
    } else {
        let missing_ok = if p.tokens.get(p.at) == Some(&Token::Comma) {
            p.at += 1;
            p.boolean()?
        } else {
            false
        };
        Function::Current {
            additional: vec![],
            name,
            missing_ok,
            column: function,
        }
    };
    p.token(Token::Right)?;
    if p.tokens.get(p.at) == Some(&Token::Word("as".into())) {
        p.token(Token::Word("as".into()))?;
        let column = match p.tokens.get(p.at)? {
            Token::Word(s) | Token::Ident(s) => s.clone(),
            _ => return None,
        };
        p.at += 1;
        match &mut call {
            Function::Set { column: c, .. } | Function::Current { column: c, .. } | Function::Advisory { column: c, .. } => *c = column,
            Function::Inventory { .. } => unreachable!("separate inventory grammar"),
        }
    }
    if from_settings || p.tokens.get(p.at) == Some(&Token::Word("from".into())) {
        p.token(Token::Word("from".into()))?;
        let mut relation = p.word()?;
        if relation == "pg_catalog" {
            p.token(Token::Dot)?;
            relation = p.word()?;
        }
        match relation.as_str() {
            "pg_settings" => {}
            "pg_show_all_settings" => {
                p.token(Token::Left)?;
                p.token(Token::Right)?;
            }
            _ => return None,
        }
        p.token(Token::Word("where".into()))?;
        p.token(Token::Word("name".into()))?;
        p.token(Token::Equals)?;
        let filter = p.text()?;
        if let Function::Set {
            name,
            from_settings: source,
            ..
        } = &mut call
        {
            // Literal arguments need not equal the source row's name. Leave
            // those forms to general SQL rather than silently changing them.
            if !from_settings && *name != filter {
                return None;
            }
            *name = filter;
            *source = true;
        } else {
            return None;
        }
    }
    (p.at == p.tokens.len()).then_some(call)
}
pub(crate) fn parse_function(sql: &str) -> Result<Option<Function>> {
    if sql.len() <= 65536 && let Some(call) = lex(sql).and_then(inventory) { return Ok(Some(call)); }
    let Some(call) = parse(sql) else {
        return Ok(None);
    };
    if call.current_columns().unwrap_or_else(|| vec![&call]).iter().any(|c| c.column().len() > 63 || c.column().is_empty()) {
        return Err("42601");
    }
    Ok(Some(call))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pgadmin_settings_source_and_case_sensitive_filter() {
        let mut settings = SessionSettings::default();
        for source in [
            "pg_show_all_settings()",
            "pg_catalog.pg_show_all_settings()",
            "pg_settings",
        ] {
            let sql = format!(
                "SELECT set_config('bytea_output','hex',false) FROM {source} WHERE name = 'bytea_output'"
            );
            let call = parse_function(&sql).unwrap().unwrap();
            assert!(call.has_row(true));
            assert_eq!(call.evaluate(&mut settings), Ok(Some("hex".into())));
        }
        let call = parse_function("SELECT set_config(name,'hex',false) FROM pg_show_all_settings() WHERE name='BYTEA_OUTPUT'").unwrap().unwrap();
        assert!(!call.has_row(true));
        assert!(parse_function("SELECT set_config('bytea_output','hex',false) FROM pg_show_all_settings() WHERE name='other'").unwrap().is_none());
    }

    #[test]
    fn numeric_inventory_defaults_are_bounded_and_case_sensitive() {
        let names = "'autovacuum_analyze_scale_factor','autovacuum_analyze_threshold','autovacuum_freeze_max_age','autovacuum_vacuum_cost_delay','autovacuum_vacuum_cost_limit','autovacuum_vacuum_scale_factor','autovacuum_vacuum_threshold','vacuum_freeze_min_age','vacuum_freeze_table_age'";
        let sql = format!("SELECT name,setting::numeric AS setting FROM pg_show_all_settings() WHERE name IN ({names}) ORDER BY name");
        let inventory = parse_function(&sql).unwrap().unwrap();
        let rows = inventory_rows(&inventory, &SessionSettings::default(), true).unwrap();
        let expected = ["0.1","50","200000000","2","-1","0.2","50","50000000","150000000"];
        assert_eq!(rows.iter().map(|r| r[1].clone()).collect::<Vec<_>>(), expected.into_iter().map(|s| Value::Numeric(giggles_pg_types::Numeric::parse(s).unwrap())).collect::<Vec<_>>());
        assert!(parse_function(&(sql.clone() + "; DELETE FROM x")).unwrap().is_none());
        assert!(parse_function(&sql.replace("ORDER BY name", "ORDER BY setting")).unwrap().is_none());
    }

    #[test]
    fn jdbc_max_index_keys_is_read_only() {
        let settings = SessionSettings::default();
        let call = parse_function(
            "SELECT setting FROM pg_catalog.pg_settings WHERE name='max_index_keys'",
        )
        .unwrap()
        .unwrap();
        assert!(call.has_row(true));
        assert_eq!(inventory_rows(&call, &settings, true), Ok(vec![vec![Value::Text("32".into())]]));
        assert_eq!(
            database::function_assignment("max_index_keys", "32", false).err(),
            Some("22023")
        );
    }
}
