// Port of PostgreSQL 18.6 src/backend/tsearch/wparser_def.c.
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group.
mod actions;

use crate::{TsError, locale};
use actions::{ACTIONS, LEX_DESCR, State, TOK_ALIAS};

const BINGO: u8 = 1;
const POP: u8 = 2;
const PUSH: u8 = 4;
const RERUN: u8 = 8;
const CLEAR: u8 = 16;
const MERGE: u8 = 32;
const CLEAR_ALL: u8 = 64;

#[derive(Clone, Copy)]
enum Condition {
    Always,
    Eof,
    Eq,
    Ignore,
    AsciiLetter,
    Alpha,
    Digit,
    Alnum,
    NotAlnum,
    Special,
    Space,
    Hex,
    StopHost,
    Host,
    UrlPath,
    UrlChar,
}

#[derive(Clone, Copy)]
enum Special {
    None,
    Tags,
    FullUrl,
    Hyphen,
    Version,
}

#[derive(Clone, Copy)]
struct Action {
    condition: Condition,
    literal: u8,
    flags: u8,
    target: State,
    token: u8,
    special: Special,
}

impl Action {
    const fn new(
        condition: Condition,
        literal: u8,
        flags: u8,
        target: State,
        token: u8,
        special: Special,
    ) -> Self {
        Self {
            condition,
            literal,
            flags,
            target,
            token,
            special,
        }
    }
}

#[derive(Clone, Copy)]
struct Position {
    offset: usize,
    len: usize,
    state: State,
    next_action: usize,
}

/// One row of `ts_token_type`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TokenType {
    pub id: u8,
    pub alias: &'static str,
    pub description: &'static str,
}

/// A parser token and its starting byte offset in the document.
/// Hyphenated words and URLs can overlap their component tokens.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Token<'a> {
    pub kind: TokenType,
    pub text: &'a str,
    pub offset: usize,
}

fn kind(id: u8) -> TokenType {
    TokenType {
        id,
        alias: TOK_ALIAS[id as usize],
        description: LEX_DESCR[id as usize],
    }
}

/// Iterator implementing the PostgreSQL default parser.
pub struct Parser<'a> {
    text: &'a str,
    pos: Position,
    saved: Vec<Position>,
    ignore: bool,
    want_host: bool,
}

impl<'a> Parser<'a> {
    pub fn new(text: &'a str) -> Self {
        Self {
            text,
            pos: Position {
                offset: 0,
                len: 0,
                state: State::Base,
                next_action: 0,
            },
            saved: Vec::new(),
            ignore: false,
            want_host: false,
        }
    }

    fn matches(&mut self, action: Action, character: Option<char>, char_len: &mut usize) -> bool {
        let c = character.unwrap_or('\0');
        match action.condition {
            Condition::Always => true,
            Condition::Eof => character.is_none(),
            Condition::Eq => character == Some(char::from(action.literal)),
            Condition::Ignore => self.ignore,
            Condition::AsciiLetter => c.is_ascii_alphabetic(),
            Condition::Alpha => locale::alpha(c),
            Condition::Digit => c.is_ascii_digit(),
            Condition::Alnum => c.is_ascii_digit() || locale::alpha(c),
            Condition::NotAlnum => !(c.is_ascii_digit() || locale::alpha(c)),
            Condition::Special => locale::special(c),
            Condition::Space => locale::space(c),
            Condition::Hex => c.is_ascii_hexdigit(),
            Condition::StopHost => std::mem::take(&mut self.want_host),
            Condition::UrlChar => {
                c.is_ascii_graphic()
                    && !matches!(c, '"' | '<' | '>' | '\\' | '^' | '`' | '{' | '|' | '}')
            }
            Condition::Host | Condition::UrlPath => {
                let mut lookahead = Self::new(&self.text[self.pos.offset..]);
                let target = if matches!(action.condition, Condition::Host) {
                    lookahead.want_host = true;
                    6
                } else {
                    lookahead.saved.push(lookahead.pos);
                    lookahead.pos.state = State::InURLPathFirst;
                    18
                };
                let Some(token) = lookahead.next().filter(|token| token.kind.id == target) else {
                    return false;
                };
                self.pos.offset += token.text.len();
                self.pos.len += token.text.len();
                *char_len = lookahead.text[lookahead.pos.offset..]
                    .chars()
                    .next()
                    .map_or(0, char::len_utf8);
                true
            }
        }
    }
}

impl<'a> Iterator for Parser<'a> {
    type Item = Token<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let start = self.pos.offset;
        if start >= self.text.len() {
            return None;
        }
        self.pos.next_action = 0;
        loop {
            let character = self.text[self.pos.offset..].chars().next();
            let mut char_len = character.map_or(0, char::len_utf8);
            let transitions = ACTIONS[self.pos.state as usize];
            let resume = std::mem::take(&mut self.pos.next_action);
            let (index, action) = transitions
                .iter()
                .copied()
                .enumerate()
                .skip(resume)
                .find(|&(_, action)| self.matches(action, character, &mut char_len))
                .expect("each parser state has a default transition");

            match action.special {
                Special::None => (),
                Special::Tags => {
                    let name = &self.text[start..self.pos.offset];
                    if name.eq_ignore_ascii_case("<script") || name.eq_ignore_ascii_case("<style") {
                        self.ignore = true;
                    } else if name.eq_ignore_ascii_case("</script")
                        || name.eq_ignore_ascii_case("</style")
                    {
                        self.ignore = false;
                    }
                }
                Special::FullUrl => {
                    self.want_host = true;
                    self.pos.offset -= self.pos.len;
                }
                Special::Hyphen => self.pos.offset -= self.pos.len,
                Special::Version => {
                    self.pos.offset -= self.pos.len;
                    self.pos.len = 0;
                }
            }

            let token = if action.flags & BINGO != 0 {
                let len = std::mem::take(&mut self.pos.len);
                Some(Token {
                    kind: kind(action.token),
                    text: &self.text[start..start + len],
                    offset: start,
                })
            } else {
                None
            };

            if action.flags & POP != 0 {
                self.pos = self.saved.pop().expect("parser POP follows PUSH");
            } else if action.flags & PUSH != 0 {
                self.saved.push(Position {
                    next_action: index + 1,
                    ..self.pos
                });
            } else if action.flags & CLEAR != 0 {
                self.saved.pop().expect("parser CLEAR follows PUSH");
            } else if action.flags & CLEAR_ALL != 0 {
                self.saved.clear();
            } else if action.flags & MERGE != 0 {
                let saved = self.saved.pop().expect("parser MERGE follows PUSH");
                self.pos.state = saved.state;
                self.pos.next_action = saved.next_action;
            }

            if !matches!(action.target, State::Null) {
                self.pos.state = action.target;
            }

            if token.is_some() || (self.pos.offset == self.text.len() && action.flags & RERUN == 0)
            {
                return token;
            }
            if action.flags & (RERUN | POP) == 0 {
                self.pos.offset += char_len;
                self.pos.len += char_len;
            }
        }
    }
}

pub fn ts_token_type(name: &str) -> Result<impl Iterator<Item = TokenType>, TsError> {
    crate::config::lookup_parser(name)?;
    Ok((1..=23).map(kind))
}

pub fn ts_parse<'a>(name: &str, text: &'a str) -> Result<Parser<'a>, TsError> {
    crate::config::lookup_parser(name)?;
    Ok(Parser::new(text))
}
