//! Literal configuration SELECTs and the bounded pg_dump pg_settings lookup.
use super::{Result, SessionSettings, database};

#[derive(Clone)]
pub(crate) enum Function {
    Inventory(giggles_pg_catalog::ResultSet),
    Advisory { name: String, key: (bool, i64), column: String },
    Set {
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
        from_settings: bool,
        column: String,
    },
}
impl Function {
    pub fn current_columns(&self) -> Option<Vec<&Function>> {
        if let Self::Current { additional, .. } = self {
            let mut columns = vec![self];
            columns.extend(additional);
            Some(columns)
        } else { None }
    }
    pub fn column(&self) -> &str {
        match self {
            Self::Inventory(_) => "setting",
            Self::Set { column, .. } | Self::Current { column, .. } | Self::Advisory { column, .. } => column,
        }
    }
    /// pg_settings.name comparisons are case-sensitive; absent rows never run set_config.
    pub fn has_row(&self, settings: &SessionSettings) -> bool {
        match self {
            Self::Set {
                name,
                from_settings: true,
                ..
            }
            | Self::Current {
                name,
                from_settings: true,
                ..
            } => database::lookup_name(name).is_ok_and(|setting| {
                setting.value(&settings.current).0.to_ascii_lowercase() == *name
            }),
            _ => true,
        }
    }
    /// Called within a real SQL transaction, including a statement's implicit one.
    /// Thus set_config(..., true) returns the local value even in autocommit;
    /// the wire dispatcher restores it at that transaction's end, without WARNING.
    pub fn evaluate(&self, settings: &mut SessionSettings) -> Result<Option<String>> {
        match self {
            Self::Inventory(_) => unreachable!("settings inventory"),
            Self::Advisory { .. } => unreachable!("wire session function"),
            Self::Set {
                name, value, local, ..
            } => {
                let (name, assignment) = database::function_assignment(name, value, *local)?;
                settings.apply(&assignment, true);
                Ok(Some(name.value(&settings.current).1))
            }
            Self::Current {
                name, missing_ok, ..
            } => match database::lookup_name(name) {
                Ok(name) => Ok(Some(name.value(&settings.current).1)),
                Err("42704") if *missing_ok => Ok(None),
                Err(code) => Err(code),
            },
        }
    }
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
// Read-only compatibility defaults, not evidence of an autovacuum worker.
// Keep the finite inventory explicit; unknown settings have no row.
fn inventory(tokens: Vec<Token>) -> Option<Function> {
    use giggles_pg_catalog::{Field, PgType, ResultSet, Value};
    let mut p = Parser { tokens, at: 0 };
    p.token(Token::Word("select".into()))?;
    let names = ["name","setting","unit","min_val","max_val","vartype","context","source","boot_val","reset_val","pending_restart"];
    let mut columns = Vec::new();
    loop {
        let name = p.word()?;
        let index = names.iter().position(|n| *n == name)?;
        let numeric = if p.tokens.get(p.at) == Some(&Token::Cast) {
            p.at += 1;
            if p.word()? != "numeric" || !matches!(index, 1 | 3 | 4 | 8 | 9) { return None; }
            true
        } else { false };
        let alias = if p.tokens.get(p.at) == Some(&Token::Word("as".into())) { p.at += 1; p.word()? } else { name };
        columns.push((index,numeric,alias));
        if columns.len() > 128 { return None; }
        if p.tokens.get(p.at) != Some(&Token::Comma) { break; }
        p.at += 1;
    }
    p.token(Token::Word("from".into()))?;
    let mut relation = p.word()?;
    if relation == "pg_catalog" { p.token(Token::Dot)?; relation = p.word()?; }
    if relation == "pg_show_all_settings" { p.token(Token::Left)?; p.token(Token::Right)?; }
    else if relation != "pg_settings" { return None; }
    p.token(Token::Word("where".into()))?;
    p.token(Token::Word("name".into()))?;
    p.token(Token::Word("in".into()))?;
    p.token(Token::Left)?;
    let mut filter = Vec::new();
    loop {
        filter.push(p.text()?);
        if filter.len() > 256 { return None; }
        if p.tokens.get(p.at) != Some(&Token::Comma) { break; }
        p.at += 1;
    }
    p.token(Token::Right)?;
    if p.at != p.tokens.len() {
        for word in ["order","by","name"] { p.token(Token::Word(word.into()))?; }
    }
    if p.at != p.tokens.len() { return None; }
    let mut rows = Vec::new();
    for (name,value,unit,min,max,kind,context) in [
        ("autovacuum_analyze_scale_factor","0.1",None,"0","100","real","sighup"),
        ("autovacuum_analyze_threshold","50",None,"0","2147483647","integer","sighup"),
        ("autovacuum_freeze_max_age","200000000",None,"100000","2000000000","integer","postmaster"),
        ("autovacuum_vacuum_cost_delay","2",Some("ms"),"-1","100","real","sighup"),
        ("autovacuum_vacuum_cost_limit","-1",None,"-1","10000","integer","sighup"),
        ("autovacuum_vacuum_scale_factor","0.2",None,"0","100","real","sighup"),
        ("autovacuum_vacuum_threshold","50",None,"0","2147483647","integer","sighup"),
        ("vacuum_freeze_min_age","50000000",None,"0","1000000000","integer","user"),
        ("vacuum_freeze_table_age","150000000",None,"0","2000000000","integer","user"),
    ] {
        if !filter.iter().any(|n| n == name) { continue; }
        let values = [Some(name),Some(value),unit,Some(min),Some(max),Some(kind),Some(context),Some("default"),Some(value),Some(value),None];
        rows.push(columns.iter().map(|(i,numeric,_)| {
            if *i == 10 { Value::Bool(false) }
            else if *numeric { Value::Numeric(giggles_pg_types::Numeric::parse(values[*i].unwrap()).expect("numeric defaults")) }
            else { values[*i].map(|s| Value::Text(s.into())).unwrap_or(Value::Null) }
        }).collect());
    }
    let fields = columns.into_iter().map(|(i,numeric,name)| Field { name, data_type: if numeric { PgType::Numeric } else if i == 10 { PgType::Bool } else { PgType::Text } }).collect();
    Some(Function::Inventory(ResultSet { fields, rows }))
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
            if !matches!(call, Function::Current { from_settings: false, .. }) { return None; }
            calls.push(call);
        }
        let mut first = calls.remove(0);
        if let Function::Current { additional, .. } = &mut first { *additional = calls; }
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
    // Bounded, case-sensitive pg_settings read using the same session values as SHOW.
    if function == "setting" {
        p.token(Token::Word("from".into()))?;
        let mut relation = p.word()?;
        if relation == "pg_catalog" {
            p.token(Token::Dot)?;
            relation = p.word()?;
        }
        if relation != "pg_settings" {
            return None;
        }
        p.token(Token::Word("where".into()))?;
        p.token(Token::Word("name".into()))?;
        p.token(Token::Equals)?;
        let name = p.text()?;
        return (p.at == p.tokens.len()).then_some(Function::Current {
            additional: vec![],
            name,
            missing_ok: true,
            from_settings: true,
            column: "setting".into(),
        });
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
            from_settings: false,
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
            Function::Inventory(_) => unreachable!("separate inventory grammar"),
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
            assert!(call.has_row(&settings));
            assert_eq!(call.evaluate(&mut settings), Ok(Some("hex".into())));
        }
        let call = parse_function("SELECT set_config(name,'hex',false) FROM pg_show_all_settings() WHERE name='BYTEA_OUTPUT'").unwrap().unwrap();
        assert!(!call.has_row(&settings));
        assert!(parse_function("SELECT set_config('bytea_output','hex',false) FROM pg_show_all_settings() WHERE name='other'").unwrap().is_none());
    }

    #[test]
    fn numeric_inventory_defaults_are_bounded_and_case_sensitive() {
        let names = "'autovacuum_analyze_scale_factor','autovacuum_analyze_threshold','autovacuum_freeze_max_age','autovacuum_vacuum_cost_delay','autovacuum_vacuum_cost_limit','autovacuum_vacuum_scale_factor','autovacuum_vacuum_threshold','vacuum_freeze_min_age','vacuum_freeze_table_age'";
        let sql = format!("SELECT name,setting::numeric AS setting FROM pg_show_all_settings() WHERE name IN ({names}) ORDER BY name");
        let Function::Inventory(result) = parse_function(&sql).unwrap().unwrap() else { panic!("inventory") };
        let expected = ["0.1","50","200000000","2","-1","0.2","50","50000000","150000000"];
        assert_eq!(result.rows.iter().map(|r| r[1].clone()).collect::<Vec<_>>(), expected.into_iter().map(|s| giggles_pg_catalog::Value::Numeric(giggles_pg_types::Numeric::parse(s).unwrap())).collect::<Vec<_>>());
        assert!(parse_function(&(sql.clone() + "; DELETE FROM x")).unwrap().is_none());
        assert!(parse_function(&sql.replace("ORDER BY name", "ORDER BY setting")).unwrap().is_none());
    }

    #[test]
    fn jdbc_max_index_keys_is_read_only() {
        let mut settings = SessionSettings::default();
        let call = parse_function(
            "SELECT setting FROM pg_catalog.pg_settings WHERE name='max_index_keys'",
        )
        .unwrap()
        .unwrap();
        assert!(call.has_row(&settings));
        assert_eq!(call.evaluate(&mut settings), Ok(Some("32".into())));
        assert_eq!(
            database::function_assignment("max_index_keys", "32", false).err(),
            Some("22023")
        );
    }
}
