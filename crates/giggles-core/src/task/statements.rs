//! Task SQL statements (GD-0061 R3, design-1 section 5): the parser half.
//!
//! `parse` recognises CREATE TASK, REVOKE TASK, SHOW TASK(S) and
//! GRANT/REVOKE { UNDO | INSPECT } ON TASK. It classifies on the statement
//! head before lexing, so every other statement returns `None` and keeps its
//! existing path. Predicates are captured as the exact text between their
//! balanced parentheses and checked with the relational lexer.
use super::definition::{
    AllowedWrite, DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText, TaskPrivileges,
};
use crate::relational::Value;
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskStatement {
    /// A task statement that failed to parse; the SQLSTATE.
    Error(&'static str),
    Create(Box<NewTask>),
    Revoke(String),
    /// `None` is SHOW TASKS.
    Show(Option<String>),
    Grant {
        grant: bool,
        bits: TaskPrivileges,
        task: String,
        roles: Vec<String>,
    },
}

pub fn command_tag(statement: &TaskStatement) -> &'static str {
    match statement {
        TaskStatement::Create(_) => "CREATE TASK",
        TaskStatement::Revoke(_) => "REVOKE TASK",
        TaskStatement::Show(_) => "SHOW",
        TaskStatement::Grant { grant: true, .. } => "GRANT",
        TaskStatement::Grant { grant: false, .. } => "REVOKE",
        // An error never completes; report by its head.
        TaskStatement::Error(_) => "",
    }
}

const SYNTAX: &str = "42601";
const UNSUPPORTED: &str = "0A000";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Head {
    Create,
    RevokeTask,
    Show,
    Privileges,
}

/// Lower-cased head words, split on whitespace and commas, without lexing.
fn head_words(sql: &str, max: usize) -> Vec<String> {
    sql.split(|c: char| c.is_ascii_whitespace() || c == ',' || c == ';')
        .filter(|w| !w.is_empty())
        .take(max)
        .map(|w| w.to_ascii_lowercase())
        .collect()
}

fn classify(sql: &str) -> Option<Head> {
    let words = head_words(sql, 16);
    let first = words.first()?.as_str();
    let second = words.get(1).map(String::as_str);
    match (first, second) {
        ("create", Some("task")) => Some(Head::Create),
        // `REVOKE task FROM r` revokes membership in a role named task.
        ("revoke", Some("task")) if !head_words(sql, usize::MAX).iter().any(|w| w == "from") => {
            Some(Head::RevokeTask)
        }
        ("show", Some("task" | "tasks")) => Some(Head::Show),
        ("grant" | "revoke", Some(_)) => {
            // grant|revoke <task privilege words> on task
            let mut i = 1;
            let mut any = false;
            while let Some(w) = words.get(i) {
                match w.as_str() {
                    "undo" | "inspect" | "approve" => any = true,
                    "on" => break,
                    _ => return None,
                }
                i += 1;
            }
            (any && words.get(i).map(String::as_str) == Some("on")
                && words.get(i + 1).map(String::as_str) == Some("task"))
            .then_some(Head::Privileges)
        }
        _ => None,
    }
}

pub fn parse(sql: &str) -> Option<TaskStatement> {
    let head = classify(sql)?;
    let tokens = match tokenize(sql) {
        Ok(t) => t,
        Err(code) => return Some(TaskStatement::Error(code)),
    };
    let mut p = Parser {
        sql,
        tokens,
        pos: 0,
    };
    let result = match head {
        Head::Create => p.create().map(|t| TaskStatement::Create(Box::new(t))),
        Head::RevokeTask => p.revoke_task(),
        Head::Show => p.show(),
        Head::Privileges => p.privileges(),
    }
    .and_then(|stmt| p.end().map(|()| stmt));
    Some(result.unwrap_or_else(TaskStatement::Error))
}

// ---------------------------------------------------------------------------
// Spanned tokens. The relational lexer has no spans, so the statement shape is
// tokenized here and each predicate slice is checked with `relational::lex`.

#[derive(Clone, Debug, PartialEq, Eq)]
enum Tok {
    /// Unquoted word, lower-cased.
    Word(String),
    QuotedIdent(String),
    Str(String),
    Number(String),
    Symbol(char),
}

#[derive(Clone, Debug)]
struct Spanned {
    tok: Tok,
    start: usize,
    end: usize,
}

fn tokenize(sql: &str) -> Result<Vec<Spanned>, &'static str> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
        } else if c == b'-' && b.get(i + 1) == Some(&b'-') {
            while i < b.len() && b[i] != b'\n' && b[i] != b'\r' {
                i += 1;
            }
        } else if c == b'/' && b.get(i + 1) == Some(&b'*') {
            i += 2;
            let mut depth = 1usize;
            while depth > 0 {
                if i + 1 >= b.len() {
                    return Err(SYNTAX);
                }
                if b[i] == b'/' && b[i + 1] == b'*' {
                    depth += 1;
                    i += 2;
                } else if b[i] == b'*' && b[i + 1] == b'/' {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
        } else if c == b'\'' || c == b'"' {
            let start = i;
            let (text, next) = quoted(sql, i, c)?;
            let tok = if c == b'\'' {
                Tok::Str(text)
            } else {
                if text.is_empty() {
                    return Err(SYNTAX);
                }
                Tok::QuotedIdent(text)
            };
            out.push(Spanned {
                tok,
                start,
                end: next,
            });
            i = next;
        } else if c.is_ascii_digit() {
            let start = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'.' || b[i] == b'_') {
                i += 1;
            }
            out.push(Spanned {
                tok: Tok::Number(sql[start..i].to_string()),
                start,
                end: i,
            });
        } else if c.is_ascii_alphabetic() || c == b'_' || c >= 0x80 {
            let start = i;
            while i < b.len()
                && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$' || b[i] >= 0x80)
            {
                i += 1;
            }
            out.push(Spanned {
                tok: Tok::Word(sql[start..i].to_lowercase()),
                start,
                end: i,
            });
        } else {
            out.push(Spanned {
                tok: Tok::Symbol(c as char),
                start: i,
                end: i + 1,
            });
            i += 1;
        }
    }
    Ok(out)
}

/// A quoted string or identifier starting at `i`; a doubled quote escapes.
fn quoted(sql: &str, i: usize, q: u8) -> Result<(String, usize), &'static str> {
    let b = sql.as_bytes();
    let mut j = i + 1;
    let mut text = String::new();
    let mut run = j;
    loop {
        if j >= b.len() {
            return Err(SYNTAX);
        }
        if b[j] == q {
            text.push_str(&sql[run..j]);
            if b.get(j + 1) == Some(&q) {
                text.push(q as char);
                j += 2;
                run = j;
                continue;
            }
            return Ok((text, j + 1));
        }
        j += 1;
    }
}

// ---------------------------------------------------------------------------

type P<T> = Result<T, &'static str>;

struct Parser<'a> {
    sql: &'a str,
    tokens: Vec<Spanned>,
    pos: usize,
}

const LIMIT_NAMES: [&str; 7] = [
    "statement_ms",
    "rows_scanned",
    "concurrent_statements",
    "lock_wait_ms",
    "pending_write_bytes",
    "stored_previews",
    "preview_ttl_s",
];

#[derive(Default)]
struct Seen(BTreeSet<&'static str>);

impl Seen {
    fn once(&mut self, clause: &'static str) -> P<()> {
        if self.0.insert(clause) {
            Ok(())
        } else {
            Err(SYNTAX)
        }
    }
    fn has(&self, clause: &'static str) -> bool {
        self.0.contains(clause)
    }
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Tok> {
        self.tokens.get(self.pos).map(|t| &t.tok)
    }

    fn peek_word(&self, w: &str) -> bool {
        matches!(self.peek(), Some(Tok::Word(x)) if x == w)
    }

    fn eat_word(&mut self, w: &str) -> bool {
        if self.peek_word(w) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn word(&mut self, w: &str) -> P<()> {
        if self.eat_word(w) {
            Ok(())
        } else {
            Err(SYNTAX)
        }
    }

    fn words(&mut self, ws: &[&str]) -> P<()> {
        ws.iter().try_for_each(|w| self.word(w))
    }

    fn eat_symbol(&mut self, c: char) -> bool {
        if self.peek() == Some(&Tok::Symbol(c)) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn symbol(&mut self, c: char) -> P<()> {
        if self.eat_symbol(c) {
            Ok(())
        } else {
            Err(SYNTAX)
        }
    }

    /// Identifier: unquoted words fold to lower case, quoted ones are exact.
    fn ident(&mut self) -> P<String> {
        let name = match self.peek() {
            Some(Tok::Word(w)) => w.clone(),
            Some(Tok::QuotedIdent(w)) => w.clone(),
            _ => return Err(SYNTAX),
        };
        self.pos += 1;
        Ok(name)
    }

    fn number(&mut self) -> P<u64> {
        let n = match self.peek() {
            Some(Tok::Number(n)) => n.parse::<u64>().map_err(|_| SYNTAX)?,
            _ => return Err(SYNTAX),
        };
        self.pos += 1;
        Ok(n)
    }

    /// Optional `;`, then nothing.
    fn end(&mut self) -> P<()> {
        self.eat_symbol(';');
        if self.pos == self.tokens.len() {
            Ok(())
        } else {
            Err(SYNTAX)
        }
    }

    fn ident_list(&mut self) -> P<Vec<String>> {
        self.symbol('(')?;
        let mut out = vec![self.ident()?];
        while self.eat_symbol(',') {
            out.push(self.ident()?);
        }
        self.symbol(')')?;
        Ok(out)
    }

    /// `( predicate )`: the exact text between balanced parentheses.
    fn predicate(&mut self) -> P<PredicateText> {
        let open = self.tokens.get(self.pos).ok_or(SYNTAX)?;
        if open.tok != Tok::Symbol('(') {
            return Err(SYNTAX);
        }
        let inner_start = open.end;
        let mut depth = 0usize;
        let mut j = self.pos;
        loop {
            let t = self.tokens.get(j).ok_or(SYNTAX)?;
            match t.tok {
                Tok::Symbol('(') => depth += 1,
                Tok::Symbol(')') => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                Tok::Symbol(';') => return Err(SYNTAX),
                _ => {}
            }
            j += 1;
        }
        let close = &self.tokens[j];
        if j == self.pos + 1 {
            return Err(SYNTAX);
        }
        let text = self.sql[inner_start..close.start].trim().to_string();
        crate::relational::lex_tokens(&text).map_err(|_| SYNTAX)?;
        self.pos = j + 1;
        Ok(PredicateText(text))
    }

    fn masked_constant(&mut self) -> P<Value> {
        let negative = self.eat_symbol('-');
        let value = match self.peek().cloned() {
            Some(Tok::Number(n)) => {
                let digits = if negative { format!("-{n}") } else { n };
                Value::Int(digits.parse::<i64>().map_err(|_| SYNTAX)?)
            }
            Some(Tok::Str(s)) if !negative => Value::Text(s),
            Some(Tok::Word(w)) if !negative && w == "true" => Value::Bool(true),
            Some(Tok::Word(w)) if !negative && w == "false" => Value::Bool(false),
            _ => return Err(SYNTAX),
        };
        self.pos += 1;
        Ok(value)
    }

    fn create(&mut self) -> P<NewTask> {
        self.words(&["create", "task"])?;
        let name = self.ident()?;
        let mut seen = Seen::default();
        let mut executor = None;
        let mut approver = None;
        let mut target = None;
        let mut visible_rows = None;
        let mut readable = Vec::new();
        let mut masked = Vec::new();
        let mut untrusted_text = Vec::new();
        let mut write: Option<(AllowedWrite, MutationCaps, Option<u64>)> = None;
        let mut disclosure = None;
        let mut limits = LoadLimits::default();
        let mut expires = None;
        while let Some(Tok::Word(w)) = self.peek().cloned() {
            match w.as_str() {
                "executor" => {
                    seen.once("executor")?;
                    self.pos += 1;
                    executor = Some(self.ident()?);
                }
                "approver" => {
                    seen.once("approver")?;
                    self.pos += 1;
                    approver = Some(self.ident()?);
                }
                "on" => {
                    seen.once("on")?;
                    self.pos += 1;
                    let first = self.ident()?;
                    target = Some(if self.eat_symbol('.') {
                        (Some(first), self.ident()?)
                    } else {
                        (None, first)
                    });
                }
                "visible" => {
                    seen.once("visible")?;
                    self.pos += 1;
                    self.words(&["rows", "where"])?;
                    visible_rows = Some(self.predicate()?);
                }
                "readable" => {
                    seen.once("readable")?;
                    self.pos += 1;
                    readable = self.ident_list()?;
                }
                "masked" => {
                    seen.once("masked")?;
                    self.pos += 1;
                    self.symbol('(')?;
                    loop {
                        let col = self.ident()?;
                        let constant = if self.eat_symbol('=') {
                            Some(self.masked_constant()?)
                        } else {
                            None
                        };
                        masked.push((col, constant));
                        if !self.eat_symbol(',') {
                            break;
                        }
                    }
                    self.symbol(')')?;
                }
                "untrusted" => {
                    seen.once("untrusted")?;
                    self.pos += 1;
                    self.word("text")?;
                    untrusted_text = self.ident_list()?;
                }
                "allow" => {
                    seen.once("allow")?;
                    self.pos += 1;
                    self.word("update")?;
                    let columns = self.ident_list()?.into_iter().collect();
                    self.words(&["old", "row", "where"])?;
                    let old_row = self.predicate()?;
                    self.words(&["new", "row", "where"])?;
                    let new_row = self.predicate()?;
                    self.words(&["at", "most"])?;
                    let distinct_rows = self.number()?;
                    self.words(&["distinct", "rows", "and"])?;
                    let transitions = self.number()?;
                    self.word("transitions")?;
                    let threshold = if self.eat_word("approve") {
                        self.word("above")?;
                        Some(self.number()?)
                    } else {
                        None
                    };
                    write = Some((
                        AllowedWrite {
                            columns,
                            old_row,
                            new_row,
                        },
                        MutationCaps {
                            distinct_rows,
                            transitions,
                        },
                        threshold,
                    ));
                }
                "disclose" => {
                    seen.once("disclose")?;
                    self.pos += 1;
                    self.words(&["at", "most"])?;
                    let rows_read = self.number()?;
                    self.words(&["rows", "read"])?;
                    self.symbol(',')?;
                    let n = self.number()?;
                    let (rows_returned, bytes_returned) = if self.eat_word("rows") {
                        self.word("returned")?;
                        self.symbol(',')?;
                        let bytes = self.number()?;
                        (n, bytes)
                    } else {
                        (rows_read, n)
                    };
                    self.words(&["bytes", "returned"])?;
                    disclosure = Some(DisclosureCaps {
                        rows_read,
                        rows_returned,
                        bytes_returned,
                    });
                }
                "with" => {
                    seen.once("with")?;
                    self.pos += 1;
                    self.symbol('(')?;
                    let mut names = BTreeSet::new();
                    loop {
                        let limit = match self.peek() {
                            Some(Tok::Word(w)) => w.clone(),
                            _ => return Err(SYNTAX),
                        };
                        self.pos += 1;
                        if !LIMIT_NAMES.contains(&limit.as_str()) || !names.insert(limit.clone()) {
                            return Err(SYNTAX);
                        }
                        self.symbol('=')?;
                        let n = self.number()?;
                        let small = || u32::try_from(n).map_err(|_| SYNTAX);
                        match limit.as_str() {
                            "statement_ms" => limits.statement_ms = n,
                            "rows_scanned" => limits.rows_scanned = n,
                            "concurrent_statements" => limits.concurrent_statements = small()?,
                            "lock_wait_ms" => limits.lock_wait_ms = n,
                            "pending_write_bytes" => limits.pending_write_bytes = n,
                            "stored_previews" => limits.stored_previews = small()?,
                            _ => limits.preview_ttl_s = n,
                        }
                        if !self.eat_symbol(',') {
                            break;
                        }
                    }
                    self.symbol(')')?;
                }
                "expires" => {
                    seen.once("expires")?;
                    self.pos += 1;
                    match self.peek().cloned() {
                        Some(Tok::Str(s)) => {
                            self.pos += 1;
                            expires = Some(s);
                        }
                        _ => return Err(SYNTAX),
                    }
                }
                _ => break,
            }
        }
        for mandatory in ["executor", "on", "visible", "disclose", "expires"] {
            if !seen.has(mandatory) {
                return Err(SYNTAX);
            }
        }
        let (schema, table) = target.ok_or(SYNTAX)?;
        let (allowed_write, mutation_caps, approval_threshold) = match write {
            Some((w, caps, threshold)) => {
                let threshold = threshold.unwrap_or(caps.distinct_rows);
                (Some(w), caps, threshold)
            }
            None => (None, MutationCaps::default(), 0),
        };
        Ok(NewTask {
            name,
            executor: executor.ok_or(SYNTAX)?,
            approver,
            schema,
            table,
            visible_rows: visible_rows.ok_or(SYNTAX)?,
            readable,
            masked,
            untrusted_text,
            allowed_write,
            mutation_caps,
            disclosure_caps: disclosure.ok_or(SYNTAX)?,
            limits,
            approval_threshold,
            expires: expires.ok_or(SYNTAX)?,
        })
    }

    fn revoke_task(&mut self) -> P<TaskStatement> {
        self.words(&["revoke", "task"])?;
        Ok(TaskStatement::Revoke(self.ident()?))
    }

    fn show(&mut self) -> P<TaskStatement> {
        self.word("show")?;
        if self.eat_word("tasks") {
            return Ok(TaskStatement::Show(None));
        }
        self.word("task")?;
        Ok(TaskStatement::Show(Some(self.ident()?)))
    }

    fn privileges(&mut self) -> P<TaskStatement> {
        let grant = if self.eat_word("grant") {
            true
        } else {
            self.word("revoke")?;
            false
        };
        let mut bits = TaskPrivileges::NONE;
        loop {
            match self.peek() {
                Some(Tok::Word(w)) if w == "undo" => bits |= TaskPrivileges::UNDO,
                Some(Tok::Word(w)) if w == "inspect" => bits |= TaskPrivileges::INSPECT,
                Some(Tok::Word(w)) if w == "approve" => return Err(UNSUPPORTED),
                _ => return Err(SYNTAX),
            }
            self.pos += 1;
            if !self.eat_symbol(',') {
                break;
            }
        }
        self.words(&["on", "task"])?;
        let task = self.ident()?;
        self.word(if grant { "to" } else { "from" })?;
        let mut roles = vec![self.ident()?];
        while self.eat_symbol(',') {
            roles.push(self.ident()?);
        }
        Ok(TaskStatement::Grant {
            grant,
            bits,
            task,
            roles,
        })
    }
}
