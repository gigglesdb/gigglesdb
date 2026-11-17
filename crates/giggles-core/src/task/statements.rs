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
use super::ledger::OperationId;
use super::receipt::{Receipt, DECISION_COMMITTED, DECISION_UNDO_COMPLETE, DECISION_UNDO_PARTIAL};
use super::undo::{UndoMode, UndoSummary};
use crate::relational::{SqlError, SqlResult, SqlRow, TimestampTz, Value};
use crate::CoreError;
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskStatement {
    /// A task statement that failed to parse; the SQLSTATE.
    Error(&'static str),
    Create(Box<NewTask>),
    Revoke(String),
    /// `None` is SHOW TASKS.
    Show(Option<String>),
    /// `SHOW TASK READS <task> [AFTER <read_seq>] [LIMIT <n>]` (R3-309);
    /// executed by `task::readlog::show_reads`.
    ShowReads {
        task: String,
        after: Option<u64>,
        limit: Option<u64>,
    },
    /// `SHOW TASK EXPOSURE <task>` (R3-501); executed by
    /// `task::exposure::show_exposure`.
    ShowExposure(String),
    /// `SHOW TASK EXPOSURE FOR CREATE TASK ...` (R3-502): the report of an
    /// unsaved draft; executed by `task::exposure::draft_exposure`.
    ShowExposureDraft(Box<NewTask>),
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
        TaskStatement::Show(_)
        | TaskStatement::ShowReads { .. }
        | TaskStatement::ShowExposure(_)
        | TaskStatement::ShowExposureDraft(_) => "SHOW",
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
        // `SHOW TASK reads` shows a task named reads; READS followed by a
        // name is the read log.
        let named = matches!(
            self.tokens.get(self.pos + 1).map(|t| &t.tok),
            Some(Tok::Word(_) | Tok::QuotedIdent(_))
        );
        if named && self.eat_word("reads") {
            let task = self.ident()?;
            let after = if self.eat_word("after") { Some(self.number()?) } else { None };
            let limit = if self.eat_word("limit") { Some(self.number()?) } else { None };
            return Ok(TaskStatement::ShowReads { task, after, limit });
        }
        // Likewise `SHOW TASK exposure` shows a task named exposure.
        if named && self.eat_word("exposure") {
            // `FOR CREATE TASK ...` is a draft; `SHOW TASK EXPOSURE for`
            // shows a task named for.
            let word = |at: usize, want: &str| {
                matches!(self.tokens.get(at).map(|t| &t.tok), Some(Tok::Word(w)) if w == want)
            };
            if word(self.pos, "for") && word(self.pos + 1, "create") {
                self.pos += 1;
                return Ok(TaskStatement::ShowExposureDraft(Box::new(self.create()?)));
            }
            return Ok(TaskStatement::ShowExposure(self.ident()?));
        }
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

// ---------------------------------------------------------------------------
// R3-707: the preview and approval statements (design-7 D7.1).

/// One of the six SQL forms of D7.1. `PREVIEW` and `APPLY CHANGE` are the
/// executor's (agent listener; the server adds the operation-ID prefix of
/// ruling R2 to `APPLY`); the other four are the approver's, on the
/// PostgreSQL listener only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChangeStatement {
    /// `PREVIEW <one UPDATE>`: the UPDATE's text, a suffix of the statement,
    /// so its `$n` numbering is unchanged.
    Preview { update: String },
    /// `APPLY CHANGE <id> DIGEST '<64 hex>'`.
    Apply { change: u64, digest: [u8; 32] },
    /// `SHOW CHANGES OF TASK <name>`.
    ShowChanges { task: String },
    /// `SHOW CHANGE <id> OF TASK <name>`.
    ShowChange { change: u64, task: String },
    /// `APPROVE CHANGE <id> OF TASK <name> DIGEST '<64 hex>'`.
    Approve {
        change: u64,
        task: String,
        digest: [u8; 32],
    },
    /// `REVOKE APPROVAL OF CHANGE <id> OF TASK <name>`.
    RevokeApproval { change: u64, task: String },
    /// R3-804: `PREVIEW UNDO OF TASK <name> [OPERATION '<op id>']`.
    PreviewUndo {
        task: String,
        operation: Option<OperationId>,
    },
    /// R3-804: `SHOW UNDO <id> OF TASK <name>`.
    ShowUndo { undo: u64, task: String },
    /// R3-804: `APPROVE UNDO <id> OF TASK <name> DIGEST '<64 hex>' MODE
    /// {COMPLETE | PARTIAL}`. The mode keyword is required.
    ApproveUndo {
        undo: u64,
        task: String,
        digest: [u8; 32],
        mode: UndoMode,
    },
}

impl ChangeStatement {
    /// The approver forms, and the undo forms of R3-804: never admitted in a
    /// task session, and on any other session refused unless it passes the
    /// PostgreSQL-listener test.
    pub fn is_approver_form(&self) -> bool {
        !matches!(self, ChangeStatement::Preview { .. } | ChangeStatement::Apply { .. })
    }

    /// The three undo forms (R3-804).
    pub fn is_undo_form(&self) -> bool {
        matches!(
            self,
            ChangeStatement::PreviewUndo { .. }
                | ChangeStatement::ShowUndo { .. }
                | ChangeStatement::ApproveUndo { .. }
        )
    }
}

/// `PREVIEW` result columns (D7.1; ruling R2 names the id `change_id`).
pub const PREVIEW_COLUMNS: [&str; 4] = ["change_id", "digest", "row_count", "expires_at"];

#[derive(Clone, Copy, PartialEq, Eq)]
enum ChangeHead {
    Preview,
    Apply,
    Show,
    Approve,
    RevokeApproval,
    PreviewUndo,
    ShowUndo,
    ApproveUndo,
}

fn change_head(sql: &str) -> Option<ChangeHead> {
    let words = head_words(sql, 3);
    let word = |i: usize| words.get(i).map(String::as_str);
    match (word(0)?, word(1)) {
        ("preview", Some("update")) => Some(ChangeHead::Preview),
        ("preview", Some("undo")) => Some(ChangeHead::PreviewUndo),
        ("show", Some("undo")) => Some(ChangeHead::ShowUndo),
        ("approve", Some("undo")) => Some(ChangeHead::ApproveUndo),
        ("apply", Some("change")) => Some(ChangeHead::Apply),
        ("show", Some("changes" | "change")) => Some(ChangeHead::Show),
        ("approve", Some("change")) => Some(ChangeHead::Approve),
        // `REVOKE approval FROM r` revokes membership in a role named approval.
        ("revoke", Some("approval")) if word(2) == Some("of") => Some(ChangeHead::RevokeApproval),
        _ => None,
    }
}

/// Parses the six forms of D7.1 and the three undo forms (R3-804). `None` is
/// any other statement, which keeps
/// its existing path; a malformed form is 42601 (`Syntax`), and a digest that
/// is not exactly 64 hex digits is 22023 (`InvalidParameterValue`). Task
/// names are identifiers (`"inc-436"` must be quoted).
pub fn parse_change_statement(sql: &str) -> Option<Result<ChangeStatement, SqlError>> {
    let head = change_head(sql)?;
    let tokens = match tokenize(sql) {
        Ok(tokens) => tokens,
        Err(_) => return Some(Err(SqlError::Syntax)),
    };
    if head == ChangeHead::Preview {
        // `PREVIEW` is consumed; the UPDATE is everything after it.
        return match tokens.get(1) {
            Some(update) => Some(Ok(ChangeStatement::Preview {
                update: sql[update.start..].to_owned(),
            })),
            None => Some(Err(SqlError::Syntax)),
        };
    }
    let mut p = Parser {
        sql,
        tokens,
        pos: 0,
    };
    Some(match head {
        ChangeHead::Preview | ChangeHead::Apply => p.apply_change(),
        ChangeHead::Show => p.show_change(),
        ChangeHead::Approve => p.approve_change(),
        ChangeHead::RevokeApproval => p.revoke_approval(),
        ChangeHead::PreviewUndo => p.preview_undo(),
        ChangeHead::ShowUndo => p.show_undo(),
        ChangeHead::ApproveUndo => p.approve_undo(),
    })
}

/// 64 hex digits (either case) as 32 bytes.
pub fn parse_digest(text: &str) -> Option<[u8; 32]> {
    let b = text.as_bytes();
    if b.len() != 64 {
        return None;
    }
    let nibble = |c: u8| (c as char).to_digit(16).and_then(|d| u8::try_from(d).ok());
    let mut out = [0u8; 32];
    for (slot, [high, low]) in out.iter_mut().zip(b.as_chunks::<2>().0) {
        *slot = (nibble(*high)? << 4) | nibble(*low)?;
    }
    Some(out)
}

impl Parser<'_> {
    /// Runs `f`, then requires the end of the statement; any shape error is
    /// 42601.
    fn change_form<T>(&mut self, f: impl FnOnce(&mut Self) -> P<T>) -> Result<T, SqlError> {
        f(self)
            .and_then(|value| self.end().map(|()| value))
            .map_err(|_| SqlError::Syntax)
    }

    /// `DIGEST '<text>'`: a string literal whose content is checked once the
    /// whole statement has parsed.
    fn digest_literal(&mut self) -> P<String> {
        self.word("digest")?;
        match self.peek() {
            Some(Tok::Str(text)) => {
                let text = text.clone();
                self.pos += 1;
                Ok(text)
            }
            _ => Err(SYNTAX),
        }
    }

    fn of_task(&mut self) -> P<String> {
        self.words(&["of", "task"])?;
        self.ident()
    }

    fn apply_change(&mut self) -> Result<ChangeStatement, SqlError> {
        let (change, text) = self.change_form(|p| {
            p.words(&["apply", "change"])?;
            let change = p.number()?;
            Ok((change, p.digest_literal()?))
        })?;
        let digest = parse_digest(&text).ok_or(SqlError::InvalidParameterValue)?;
        Ok(ChangeStatement::Apply { change, digest })
    }

    fn show_change(&mut self) -> Result<ChangeStatement, SqlError> {
        self.change_form(|p| {
            p.word("show")?;
            if p.eat_word("changes") {
                return Ok(ChangeStatement::ShowChanges { task: p.of_task()? });
            }
            p.word("change")?;
            let change = p.number()?;
            Ok(ChangeStatement::ShowChange {
                change,
                task: p.of_task()?,
            })
        })
    }

    fn approve_change(&mut self) -> Result<ChangeStatement, SqlError> {
        let (change, task, text) = self.change_form(|p| {
            p.words(&["approve", "change"])?;
            let change = p.number()?;
            let task = p.of_task()?;
            Ok((change, task, p.digest_literal()?))
        })?;
        let digest = parse_digest(&text).ok_or(SqlError::InvalidParameterValue)?;
        Ok(ChangeStatement::Approve {
            change,
            task,
            digest,
        })
    }

    /// `PREVIEW UNDO OF TASK <name> [OPERATION '<op id>']`; an operation ID
    /// that is empty or longer than 128 bytes is 42601, as on the agent
    /// listener.
    fn preview_undo(&mut self) -> Result<ChangeStatement, SqlError> {
        self.change_form(|p| {
            p.words(&["preview", "undo"])?;
            let task = p.of_task()?;
            let operation = if p.eat_word("operation") {
                let op = match p.peek() {
                    Some(Tok::Str(text)) => OperationId::new(text.as_bytes()).ok_or(SYNTAX)?,
                    _ => return Err(SYNTAX),
                };
                p.pos += 1;
                Some(op)
            } else {
                None
            };
            Ok(ChangeStatement::PreviewUndo { task, operation })
        })
    }

    /// `SHOW UNDO <id> OF TASK <name>`.
    fn show_undo(&mut self) -> Result<ChangeStatement, SqlError> {
        self.change_form(|p| {
            p.words(&["show", "undo"])?;
            let undo = p.number()?;
            Ok(ChangeStatement::ShowUndo {
                undo,
                task: p.of_task()?,
            })
        })
    }

    /// `APPROVE UNDO <id> OF TASK <name> DIGEST '<hex>' MODE {COMPLETE |
    /// PARTIAL}`. A missing or unknown mode is 42601; the digest is checked
    /// (22023) once the whole form has parsed.
    fn approve_undo(&mut self) -> Result<ChangeStatement, SqlError> {
        let (undo, task, text, mode) = self.change_form(|p| {
            p.words(&["approve", "undo"])?;
            let undo = p.number()?;
            let task = p.of_task()?;
            let text = p.digest_literal()?;
            p.word("mode")?;
            let mode = if p.eat_word("complete") {
                UndoMode::Complete
            } else if p.eat_word("partial") {
                UndoMode::Partial
            } else {
                return Err(SYNTAX);
            };
            Ok((undo, task, text, mode))
        })?;
        let digest = parse_digest(&text).ok_or(SqlError::InvalidParameterValue)?;
        Ok(ChangeStatement::ApproveUndo {
            undo,
            task,
            digest,
            mode,
        })
    }

    fn revoke_approval(&mut self) -> Result<ChangeStatement, SqlError> {
        self.change_form(|p| {
            p.words(&["revoke", "approval", "of", "change"])?;
            let change = p.number()?;
            Ok(ChangeStatement::RevokeApproval {
                change,
                task: p.of_task()?,
            })
        })
    }
}

/// The `PREVIEW` result row `(change_id, digest, row_count, expires_at)`
/// (D7.1). It carries no row values.
pub fn preview_result(summary: &super::preview::PreviewSummary) -> Result<SqlResult, CoreError> {
    let overflow = || CoreError::Sql(SqlError::Overflow);
    let change_id = i64::try_from(summary.id.0).map_err(|_| overflow())?;
    let expires_us = summary
        .expires_at
        .0
        .checked_sub(super::catalog::PG_EPOCH_UNIX_MS)
        .and_then(|ms| ms.checked_mul(1000))
        .ok_or_else(overflow)?;
    let expires_at = TimestampTz::from_microseconds(expires_us).map_err(|_| overflow())?;
    Ok(SqlResult {
        columns: PREVIEW_COLUMNS.map(String::from).to_vec(),
        rows: vec![SqlRow {
            values: vec![
                Value::Int(change_id),
                Value::Text(super::preview::digest_hex(&summary.digest)),
                Value::Int(i64::from(summary.rows)),
                Value::TimestampTz(expires_at),
            ],
            version: 0,
        }],
        affected: 1,
    })
}

// ---------------------------------------------------------------------------
// R3-804: the result rows of the undo forms (design-7 D7.1, D8).

/// `PREVIEW UNDO` result columns: the stored undo preview's id (the id
/// `SHOW CHANGES` lists with kind `undo`), its digest and the four counts.
pub const UNDO_PREVIEW_COLUMNS: [&str; 7] = [
    "change_id",
    "digest",
    "restorable",
    "conflicts",
    "superseded",
    "settled",
    "expires_at",
];

/// `SHOW UNDO` columns (plan v5 R3-804): one line per restored column, and
/// one per conflict or superseded write.
pub const UNDO_COLUMNS: [&str; 6] = [
    "class",
    "key",
    "column_name",
    "current_value",
    "restored_value",
    "reason",
];

/// `APPROVE UNDO` result columns: the full receipt of the undo commit.
pub const UNDO_RECEIPT_COLUMNS: [&str; 20] = [
    "operation_id",
    "decision",
    "receipt_seq",
    "change_id",
    "mode",
    "restored",
    "conflicts",
    "superseded",
    "row_count",
    "distinct_charged",
    "transitions_charged",
    "distinct_after",
    "transitions_after",
    "effects_digest",
    "request_fp",
    "issuer",
    "executor",
    "undone_by",
    "def_version",
    "decided_at",
];

fn int_value(n: u64) -> Result<Value, CoreError> {
    i64::try_from(n)
        .map(Value::Int)
        .map_err(|_| CoreError::Sql(SqlError::Overflow))
}

/// Unix milliseconds as a timestamptz value.
fn ms_timestamptz(ms: u64) -> Result<Value, CoreError> {
    let overflow = || CoreError::Sql(SqlError::Overflow);
    let us = i64::try_from(ms)
        .ok()
        .and_then(|ms| ms.checked_sub(super::catalog::PG_EPOCH_UNIX_MS))
        .and_then(|ms| ms.checked_mul(1000))
        .ok_or_else(overflow)?;
    Ok(Value::TimestampTz(
        TimestampTz::from_microseconds(us).map_err(|_| overflow())?,
    ))
}

fn one_row(columns: &[&str], values: Vec<Value>) -> SqlResult {
    SqlResult {
        columns: columns.iter().map(|c| (*c).to_owned()).collect(),
        rows: vec![SqlRow { values, version: 0 }],
        affected: 1,
    }
}

/// The `PREVIEW UNDO` row `(change_id, digest, restorable, conflicts,
/// superseded, settled, expires_at)`. It carries no row values.
pub fn undo_preview_result(summary: &UndoSummary) -> Result<SqlResult, CoreError> {
    Ok(one_row(
        &UNDO_PREVIEW_COLUMNS,
        vec![
            int_value(summary.id.0)?,
            Value::Text(super::preview::digest_hex(&summary.digest)),
            int_value(u64::from(summary.restorable))?,
            int_value(u64::from(summary.conflicts))?,
            int_value(u64::from(summary.superseded))?,
            int_value(u64::from(summary.settled))?,
            ms_timestamptz(summary.expires_at)?,
        ],
    ))
}

/// The `APPROVE UNDO` row: every field of the full receipt
/// ([`UNDO_RECEIPT_COLUMNS`]). `decision` is `committed`, `undo_complete`
/// or `undo_partial`; `mode` is `complete` or `partial` (NULL for a receipt
/// that is not an undo's); `undone_by` is the receipt's approver, the undo
/// role. A minimal receipt is refused (`Unsupported`): the undo role always
/// gets the full view.
pub fn undo_receipt_result(receipt: &Receipt) -> Result<SqlResult, CoreError> {
    let record = receipt.record().ok_or(CoreError::Unsupported)?;
    let decision = match record.decision {
        DECISION_COMMITTED => "committed",
        DECISION_UNDO_COMPLETE => "undo_complete",
        DECISION_UNDO_PARTIAL => "undo_partial",
        _ => return Err(CoreError::Corrupt("receipt decision".into())),
    };
    let mode = if record.undo_mode == UndoMode::Complete as u8 {
        Value::Text("complete".into())
    } else if record.undo_mode == UndoMode::Partial as u8 {
        Value::Text("partial".into())
    } else {
        Value::Null
    };
    let preview = receipt.preview().map_or(Ok(Value::Null), int_value)?;
    Ok(one_row(
        &UNDO_RECEIPT_COLUMNS,
        vec![
            Value::Text(String::from_utf8_lossy(&record.op_id).into_owned()),
            Value::Text(decision.into()),
            int_value(record.seq)?,
            preview,
            mode,
            int_value(u64::from(record.restored))?,
            int_value(u64::from(record.conflicts))?,
            int_value(u64::from(record.superseded))?,
            int_value(u64::from(record.rows_affected))?,
            int_value(u64::from(record.distinct_charged))?,
            int_value(u64::from(record.transitions_charged))?,
            int_value(record.distinct_after)?,
            int_value(record.transitions_after)?,
            Value::Text(super::preview::digest_hex(&record.effects_digest)),
            Value::Text(super::preview::digest_hex(&record.request_fp)),
            Value::Text(record.issuer.clone()),
            Value::Text(record.executor.clone()),
            record.approver.clone().map_or(Value::Null, Value::Text),
            int_value(record.def_version)?,
            ms_timestamptz(record.decided_ms)?,
        ],
    ))
}
