//! Standalone, literal-only configuration SELECTs. Larger SQL stays on its old path.
use super::{Result, SessionSettings, database};

#[derive(Clone)]
pub(crate) enum Function {
    Set {
        name: String,
        value: String,
        local: bool,
        column: String,
    },
    Current {
        name: String,
        missing_ok: bool,
        column: String,
    },
}
impl Function {
    pub fn column(&self) -> &str {
        match self {
            Self::Set { column, .. } | Self::Current { column, .. } => column,
        }
    }
    /// Called within a real SQL transaction, including a statement's implicit one.
    /// Thus set_config(..., true) returns the local value even in autocommit;
    /// the wire dispatcher restores it at that transaction's end, without WARNING.
    pub fn evaluate(&self, settings: &mut SessionSettings) -> Result<Option<String>> {
        match self {
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
#[derive(PartialEq)]
enum Token {
    Word(String),
    Ident(String),
    Text(String),
    Left,
    Right,
    Dot,
    Comma,
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
            '(' => Token::Left,
            ')' => Token::Right,
            '.' => Token::Dot,
            ',' => Token::Comma,
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
fn parse(sql: &str) -> Option<Function> {
    let mut p = Parser {
        tokens: lex(sql)?,
        at: 0,
    };
    p.token(Token::Word("select".into()))?;
    let mut function = p.word()?;
    if function == "pg_catalog" {
        p.token(Token::Dot)?;
        function = p.word()?;
    }
    if !matches!(function.as_str(), "set_config" | "current_setting") {
        return None;
    }
    p.token(Token::Left)?;
    let name = p.text()?;
    let mut call = if function == "set_config" {
        p.token(Token::Comma)?;
        let value = p.text()?;
        p.token(Token::Comma)?;
        let local = p.boolean()?;
        Function::Set {
            name,
            value,
            local,
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
            name,
            missing_ok,
            column: function,
        }
    };
    p.token(Token::Right)?;
    if p.at < p.tokens.len() {
        p.token(Token::Word("as".into()))?;
        let column = match p.tokens.get(p.at)? {
            Token::Word(s) | Token::Ident(s) => s.clone(),
            _ => return None,
        };
        p.at += 1;
        match &mut call {
            Function::Set { column: c, .. } | Function::Current { column: c, .. } => *c = column,
        }
    }
    (p.at == p.tokens.len()).then_some(call)
}
pub(crate) fn parse_function(sql: &str) -> Result<Option<Function>> {
    let Some(call) = parse(sql) else {
        return Ok(None);
    };
    if call.column().len() > 63 || call.column().is_empty() {
        return Err("42601");
    }
    Ok(Some(call))
}
