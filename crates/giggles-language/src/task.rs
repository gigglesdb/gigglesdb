//! Profile 0.22 native task statements (GD-0061 R3-112, design-1 section 9).
//!
//! The forms lower to the same `giggles_core::task::statements::TaskStatement`
//! that the SQL parser produces. Native expressions are rendered to SQL
//! predicate text; core canonicalises that text when the task is created, so
//! both languages store identical bytes. Dispatch is on the head words only:
//! the general statement parser never sees these words.
use crate::parser::{Parser, Token, lex};
use crate::{
    Arithmetic, Bindings, Compare, Error, Expr, LanguageVersion, Result, ScalarFunction, Value, ValueExpr,
};
use giggles_core::task::definition::{
    AllowedWrite, DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText, TaskPrivileges,
};
use giggles_core::task::statements::TaskStatement;
use std::collections::{BTreeMap, BTreeSet};

// R3-502: `show task exposure` and `show task reads`.
#[path = "task_exposure.rs"]
mod exposure;
pub use exposure::show_task;

const LIMIT_NAMES: [&str; 7] = [
    "statement_ms",
    "rows_scanned",
    "concurrent_statements",
    "lock_wait_ms",
    "pending_write_bytes",
    "stored_previews",
    "preview_ttl_s",
];

/// Parse one native task statement in an explicitly selected profile.
///
/// Profiles before 0.22 return [`Error::Unsupported`]. Text whose head words
/// are not a task statement is a syntax error at its first token.
pub fn parse_task_with_version(text: &str, version: LanguageVersion) -> Result<TaskStatement> {
    if !version.supports_tasks() {
        return Err(Error::Unsupported);
    }
    let (tokens, prefixes) = fold(lex(text, version)?);
    let mut t = TaskParser {
        p: Parser {
            tokens,
            pos: 0,
            end: text.len(),
            version,
        },
        prefixes,
    };
    let statement = t.statement()?;
    t.p.symbol(';');
    if t.p.peek().is_some() {
        return Err(t.p.error());
    }
    Ok(statement)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Prefix {
    Old,
    New,
}

/// Fold `timestamp("..")` into its string and `old.c` / `new.c` into a quoted
/// identifier, remembering each prefix by its token offset. `on old.t` keeps
/// its schema-qualified meaning.
fn fold(tokens: Vec<(Token, usize)>) -> (Vec<(Token, usize)>, BTreeMap<usize, Prefix>) {
    let mut out: Vec<(Token, usize)> = Vec::with_capacity(tokens.len());
    let mut prefixes = BTreeMap::new();
    let mut i = 0;
    while i < tokens.len() {
        let (token, offset) = &tokens[i];
        match (token, tokens.get(i + 1), tokens.get(i + 2), tokens.get(i + 3)) {
            (
                Token::Word(w),
                Some((Token::Symbol('('), _)),
                Some((Token::String(s), _)),
                Some((Token::Symbol(')'), _)),
            ) if w == "timestamp" => {
                out.push((Token::String(s.clone()), *offset));
                i += 4;
                continue;
            }
            (
                Token::Word(w),
                Some((Token::Symbol('.'), _)),
                Some((Token::Word(c) | Token::QuotedIdentifier(c), _)),
                _,
            ) if (w == "old" || w == "new")
                && !matches!(out.last(), Some((Token::Word(on), _)) if on == "on") =>
            {
                prefixes.insert(*offset, if w == "old" { Prefix::Old } else { Prefix::New });
                out.push((Token::QuotedIdentifier(c.clone()), *offset));
                i += 3;
                continue;
            }
            _ => {}
        }
        out.push((token.clone(), *offset));
        i += 1;
    }
    (out, prefixes)
}

struct TaskParser {
    p: Parser,
    prefixes: BTreeMap<usize, Prefix>,
}

impl TaskParser {
    fn peek_word(&self, offset: usize) -> Option<&str> {
        match self.p.tokens.get(self.p.pos + offset) {
            Some((Token::Word(w), _)) => Some(w),
            _ => None,
        }
    }

    fn statement(&mut self) -> Result<TaskStatement> {
        match (self.peek_word(0), self.peek_word(1)) {
            (Some("create"), Some("task")) => Ok(TaskStatement::Create(Box::new(self.create()?))),
            (Some("revoke"), Some("task")) => {
                self.p.pos += 2;
                Ok(TaskStatement::Revoke(self.ident()?))
            }
            (Some("show"), Some("tasks")) => {
                self.p.pos += 2;
                Ok(TaskStatement::Show(None))
            }
            (Some("show"), Some("task")) => {
                self.p.pos += 2;
                self.show_task()
            }
            (Some("grant" | "revoke"), Some("undo" | "inspect" | "approve")) => self.privileges(),
            _ => Err(self.p.error()),
        }
    }

    /// A task or role name: a native string, a word or a quoted identifier.
    fn ident(&mut self) -> Result<String> {
        if let Some(Token::String(s)) = self.p.peek().cloned() {
            self.p.pos += 1;
            return Ok(s);
        }
        self.p.name()
    }

    fn ident_list(&mut self) -> Result<Vec<String>> {
        if !self.p.symbol('(') {
            return Err(self.p.error());
        }
        let mut out = vec![self.p.name()?];
        while self.p.symbol(',') {
            out.push(self.p.name()?);
        }
        if !self.p.symbol(')') {
            return Err(self.p.error());
        }
        Ok(out)
    }

    fn number(&mut self) -> Result<u64> {
        match self.p.peek().cloned() {
            Some(Token::Number(n)) => {
                let n = n.parse::<u64>().map_err(|_| self.p.error())?;
                self.p.pos += 1;
                Ok(n)
            }
            _ => Err(self.p.error()),
        }
    }

    fn words(&mut self, words: &[&str]) -> Result<()> {
        words.iter().try_for_each(|w| self.p.need(w))
    }

    fn once(&self, seen: &mut BTreeSet<&'static str>, clause: &'static str) -> Result<()> {
        if seen.insert(clause) {
            Ok(())
        } else {
            Err(self.p.error())
        }
    }

    /// One native expression rendered to SQL predicate text. `allowed` is the
    /// only row prefix the clause accepts; any other prefix is Unsupported.
    fn predicate(&mut self, allowed: Option<Prefix>) -> Result<PredicateText> {
        let start = self.p.pos;
        let expr = self.p.expr(0)?;
        for (_, offset) in &self.p.tokens[start..self.p.pos] {
            if let Some(prefix) = self.prefixes.get(offset)
                && Some(*prefix) != allowed
            {
                return Err(Error::Unsupported);
            }
        }
        let mut sql = String::new();
        render(&expr, 0, &mut sql, &mut Params::constant())?;
        Ok(PredicateText(sql))
    }

    fn masked_constant(&mut self) -> Result<Value> {
        match self.p.value()? {
            ValueExpr::Literal(value @ (Value::Int(_) | Value::Text(_) | Value::Bool(_))) => Ok(value),
            ValueExpr::Parameter(_) => Err(Error::Unsupported),
            ValueExpr::Literal(_) => Err(self.p.error()),
        }
    }

    fn create(&mut self) -> Result<NewTask> {
        self.words(&["create", "task"])?;
        let name = self.ident()?;
        let mut seen = BTreeSet::new();
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
        while let Some(clause) = self.peek_word(0).map(str::to_owned) {
            match clause.as_str() {
                "for" => {
                    self.once(&mut seen, "for")?;
                    self.words(&["for", "role"])?;
                    executor = Some(self.ident()?);
                }
                "approver" => {
                    self.once(&mut seen, "approver")?;
                    self.p.pos += 1;
                    approver = Some(self.ident()?);
                }
                "on" => {
                    self.once(&mut seen, "on")?;
                    self.p.pos += 1;
                    let first = self.p.name()?;
                    target = Some(if self.p.symbol('.') {
                        (Some(first), self.p.name()?)
                    } else {
                        (None, first)
                    });
                }
                "visible" => {
                    self.once(&mut seen, "visible")?;
                    self.words(&["visible", "when"])?;
                    visible_rows = Some(self.predicate(None)?);
                }
                "readable" => {
                    self.once(&mut seen, "readable")?;
                    self.p.pos += 1;
                    readable = self.ident_list()?;
                }
                "masked" => {
                    self.once(&mut seen, "masked")?;
                    self.p.pos += 1;
                    if !self.p.symbol('(') {
                        return Err(self.p.error());
                    }
                    loop {
                        let column = self.p.name()?;
                        let constant = if matches!(self.p.peek(), Some(Token::Op(Compare::Eq))) {
                            self.p.pos += 1;
                            Some(self.masked_constant()?)
                        } else {
                            None
                        };
                        masked.push((column, constant));
                        if !self.p.symbol(',') {
                            break;
                        }
                    }
                    if !self.p.symbol(')') {
                        return Err(self.p.error());
                    }
                }
                "untrusted" => {
                    self.once(&mut seen, "untrusted")?;
                    self.words(&["untrusted", "text"])?;
                    untrusted_text = self.ident_list()?;
                }
                "allow" => {
                    self.once(&mut seen, "allow")?;
                    self.words(&["allow", "update"])?;
                    let columns = self.ident_list()?.into_iter().collect();
                    self.p.need("from")?;
                    let old_row = self.predicate(Some(Prefix::Old))?;
                    self.p.need("to")?;
                    let new_row = self.predicate(Some(Prefix::New))?;
                    self.words(&["at", "most"])?;
                    let distinct_rows = self.number()?;
                    self.words(&["distinct", "rows", "and"])?;
                    let transitions = self.number()?;
                    self.p.need("transitions")?;
                    let threshold = if self.p.word("approve") {
                        self.p.need("above")?;
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
                    self.once(&mut seen, "disclose")?;
                    self.words(&["disclose", "at", "most"])?;
                    let rows_read = self.number()?;
                    self.words(&["rows", "read", "and"])?;
                    let n = self.number()?;
                    let (rows_returned, bytes_returned) = if self.p.word("rows") {
                        self.words(&["returned", "and"])?;
                        (n, self.number()?)
                    } else {
                        (rows_read, n)
                    };
                    self.p.need("bytes")?;
                    disclosure = Some(DisclosureCaps {
                        rows_read,
                        rows_returned,
                        bytes_returned,
                    });
                }
                "limits" => {
                    self.once(&mut seen, "limits")?;
                    self.p.pos += 1;
                    if !self.p.symbol('(') {
                        return Err(self.p.error());
                    }
                    let mut names = BTreeSet::new();
                    loop {
                        let limit = match self.p.peek() {
                            Some(Token::Word(w)) if LIMIT_NAMES.contains(&w.as_str()) => w.clone(),
                            _ => return Err(self.p.error()),
                        };
                        if !names.insert(limit.clone()) {
                            return Err(self.p.error());
                        }
                        self.p.pos += 1;
                        if !matches!(self.p.peek(), Some(Token::Op(Compare::Eq))) {
                            return Err(self.p.error());
                        }
                        self.p.pos += 1;
                        let n = self.number()?;
                        let small = || u32::try_from(n).map_err(|_| Error::Limit);
                        match limit.as_str() {
                            "statement_ms" => limits.statement_ms = n,
                            "rows_scanned" => limits.rows_scanned = n,
                            "concurrent_statements" => limits.concurrent_statements = small()?,
                            "lock_wait_ms" => limits.lock_wait_ms = n,
                            "pending_write_bytes" => limits.pending_write_bytes = n,
                            "stored_previews" => limits.stored_previews = small()?,
                            _ => limits.preview_ttl_s = n,
                        }
                        if !self.p.symbol(',') {
                            break;
                        }
                    }
                    if !self.p.symbol(')') {
                        return Err(self.p.error());
                    }
                }
                "expires" => {
                    self.once(&mut seen, "expires")?;
                    self.p.pos += 1;
                    match self.p.peek().cloned() {
                        Some(Token::String(s)) => {
                            self.p.pos += 1;
                            expires = Some(s);
                        }
                        _ => return Err(self.p.error()),
                    }
                }
                _ => break,
            }
        }
        let (Some(executor), Some((schema, table)), Some(visible_rows), Some(disclosure_caps), Some(expires)) =
            (executor, target, visible_rows, disclosure, expires)
        else {
            return Err(self.p.error());
        };
        // Same defaults as the SQL parser.
        let (allowed_write, mutation_caps, approval_threshold) = match write {
            Some((w, caps, threshold)) => {
                let threshold = threshold.unwrap_or(caps.distinct_rows);
                (Some(w), caps, threshold)
            }
            None => (None, MutationCaps::default(), 0),
        };
        Ok(NewTask {
            name,
            executor,
            approver,
            schema,
            table,
            visible_rows,
            readable,
            masked,
            untrusted_text,
            allowed_write,
            mutation_caps,
            disclosure_caps,
            limits,
            approval_threshold,
            expires,
        })
    }

    fn privileges(&mut self) -> Result<TaskStatement> {
        let grant = self.p.word("grant");
        if !grant {
            self.p.need("revoke")?;
        }
        let mut bits = TaskPrivileges::NONE;
        loop {
            match self.peek_word(0) {
                Some("undo") => bits |= TaskPrivileges::UNDO,
                Some("inspect") => bits |= TaskPrivileges::INSPECT,
                // As in SQL: approval is the task's approver, not a grant.
                Some("approve") => return Err(Error::Unsupported),
                _ => return Err(self.p.error()),
            }
            self.p.pos += 1;
            if !self.p.symbol(',') {
                break;
            }
        }
        self.words(&["on", "task"])?;
        let task = self.ident()?;
        self.p.need(if grant { "to" } else { "from" })?;
        let mut roles = vec![self.ident()?];
        while self.p.symbol(',') {
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
// SQL rendering. Precedence levels: OR 1, AND 2, NOT 3, comparison 4,
// additive 5, multiplicative 6, unary minus 7, atoms 8. A child below its
// required level is parenthesised.

fn level(expr: &Expr) -> u8 {
    match expr {
        Expr::Or(..) => 1,
        Expr::And(..) => 2,
        Expr::Not(_) => 3,
        Expr::Compare(..) | Expr::IsNull(..) => 4,
        Expr::Arithmetic(_, Arithmetic::Add | Arithmetic::Subtract, _) => 5,
        Expr::Arithmetic(..) => 6,
        Expr::Negate(_) => 7,
        _ => 8,
    }
}

/// The parameters of rendered text. A task predicate is a stored constant
/// with no bindings; a previewed UPDATE (R3-708) renders each native `$name`
/// as a positional `$n`, numbered by first use, with its bound value.
pub(crate) struct Params<'a> {
    bindings: Option<&'a Bindings>,
    names: Vec<String>,
    pub(crate) values: Vec<Value>,
}

impl<'a> Params<'a> {
    pub(crate) fn constant() -> Self {
        Params { bindings: None, names: Vec::new(), values: Vec::new() }
    }

    pub(crate) fn bound(bindings: &'a Bindings) -> Self {
        Params { bindings: Some(bindings), names: Vec::new(), values: Vec::new() }
    }

    fn placeholder(&mut self, name: &str, out: &mut String) -> Result<()> {
        let bindings = self.bindings.ok_or(Error::Unsupported)?;
        let position = match self.names.iter().position(|seen| seen == name) {
            Some(position) => position,
            None => {
                let value = bindings.get(name).ok_or_else(|| Error::Bind(name.to_owned()))?;
                self.names.push(name.to_owned());
                self.values.push(value.clone());
                self.names.len() - 1
            }
        };
        out.push_str(&format!("${}", position + 1));
        Ok(())
    }
}

pub(crate) fn render(expr: &Expr, min: u8, out: &mut String, params: &mut Params) -> Result<()> {
    let wrap = level(expr) < min;
    if wrap {
        out.push('(');
    }
    match expr {
        Expr::Value(value) => literal(value, out, params)?,
        Expr::Field(name) => identifier(name, out),
        Expr::Or(a, b) => binary(a, 1, " OR ", b, 2, out, params)?,
        Expr::And(a, b) => binary(a, 2, " AND ", b, 3, out, params)?,
        Expr::Not(e) => {
            out.push_str("NOT ");
            render(e, 3, out, params)?;
        }
        Expr::Compare(a, op, b) => {
            let op = match op {
                Compare::Eq => " = ",
                Compare::Ne => " <> ",
                Compare::Lt => " < ",
                Compare::Le => " <= ",
                Compare::Gt => " > ",
                Compare::Ge => " >= ",
            };
            binary(a, 5, op, b, 5, out, params)?;
        }
        Expr::IsNull(e, negated) => {
            render(e, 5, out, params)?;
            out.push_str(if *negated { " IS NOT NULL" } else { " IS NULL" });
        }
        Expr::Arithmetic(a, op, b) => {
            let (text, l) = match op {
                Arithmetic::Add => (" + ", 5),
                Arithmetic::Subtract => (" - ", 5),
                Arithmetic::Multiply => (" * ", 6),
                Arithmetic::Divide => (" / ", 6),
                Arithmetic::Remainder => (" % ", 6),
            };
            binary(a, l, text, b, l + 1, out, params)?;
        }
        Expr::Negate(e) => {
            out.push('-');
            render(e, 8, out, params)?;
        }
        Expr::Call {
            function,
            arguments,
        } => {
            out.push_str(match function {
                ScalarFunction::Coalesce => "coalesce(",
                ScalarFunction::Abs => "abs(",
                ScalarFunction::Length => "length(",
            });
            for (i, argument) in arguments.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                render(argument, 1, out, params)?;
            }
            out.push(')');
        }
        Expr::Case {
            operand,
            arms,
            else_expr,
        } => {
            out.push_str("CASE");
            if let Some(operand) = operand {
                out.push(' ');
                render(operand, 1, out, params)?;
            }
            for (condition, result) in arms {
                out.push_str(" WHEN ");
                render(condition, 1, out, params)?;
                out.push_str(" THEN ");
                render(result, 1, out, params)?;
            }
            if let Some(else_expr) = else_expr {
                out.push_str(" ELSE ");
                render(else_expr, 1, out, params)?;
            }
            out.push_str(" END");
        }
        // Block locals and upsert sources have no meaning in a task predicate.
        Expr::Local(_) | Expr::ConflictField { .. } => return Err(Error::Unsupported),
    }
    if wrap {
        out.push(')');
    }
    Ok(())
}

fn binary(
    a: &Expr,
    la: u8,
    op: &str,
    b: &Expr,
    lb: u8,
    out: &mut String,
    params: &mut Params,
) -> Result<()> {
    render(a, la, out, params)?;
    out.push_str(op);
    render(b, lb, out, params)
}

fn literal(value: &ValueExpr, out: &mut String, params: &mut Params) -> Result<()> {
    match value {
        // A task definition is a stored constant; it has no bindings.
        ValueExpr::Parameter(name) => params.placeholder(name, out)?,
        ValueExpr::Literal(Value::Null) => out.push_str("NULL"),
        ValueExpr::Literal(Value::Bool(b)) => out.push_str(if *b { "TRUE" } else { "FALSE" }),
        // Parenthesised so `a - -1` never renders as a `--` comment.
        ValueExpr::Literal(Value::Int(i)) if *i < 0 => out.push_str(&format!("({i})")),
        ValueExpr::Literal(Value::Int(i)) => out.push_str(&i.to_string()),
        ValueExpr::Literal(Value::Text(s)) => {
            out.push('\'');
            out.push_str(&s.replace('\'', "''"));
            out.push('\'');
        }
        ValueExpr::Literal(_) => return Err(Error::Unsupported),
    }
    Ok(())
}

/// Native field names are already lower-case `[a-z][a-z0-9_]*`. SQL keywords
/// are double-quoted; core re-renders canonical text, so over-quoting is
/// harmless while under-quoting would change the predicate.
pub(crate) fn identifier(name: &str, out: &mut String) {
    const KEYWORDS: &[&str] = &[
        "all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric", "authorization",
        "between", "bigint", "binary", "bit", "boolean", "both", "case", "cast", "char", "character",
        "check", "coalesce", "collate", "collation", "column", "concurrently", "constraint", "create",
        "cross", "current_catalog", "current_date", "current_role", "current_schema", "current_time",
        "current_timestamp", "current_user", "dec", "decimal", "default", "deferrable", "desc",
        "distinct", "do", "else", "end", "except", "exists", "extract", "false", "fetch", "float",
        "for", "foreign", "freeze", "from", "full", "grant", "greatest", "group", "grouping", "having",
        "ilike", "in", "initially", "inner", "inout", "int", "integer", "intersect", "interval", "into",
        "is", "isnull", "join", "lateral", "leading", "least", "left", "like", "limit", "localtime",
        "localtimestamp", "national", "natural", "nchar", "none", "normalize", "not", "notnull", "null",
        "nullif", "numeric", "offset", "on", "only", "or", "order", "out", "outer", "overlaps",
        "overlay", "placing", "position", "precision", "primary", "real", "references", "returning",
        "right", "row", "select", "session_user", "setof", "similar", "smallint", "some", "substring",
        "symmetric", "system_user", "table", "tablesample", "then", "time", "timestamp", "to",
        "trailing", "treat", "trim", "true", "union", "unique", "user", "using", "values", "varchar",
        "variadic", "verbose", "when", "where", "window", "with", "xmlattributes", "xmlconcat",
        "xmlelement", "xmlexists", "xmlforest", "xmlnamespaces", "xmlparse", "xmlpi", "xmlroot",
        "xmlserialize", "xmltable",
    ];
    let plain = name.bytes().next().is_some_and(|b| b.is_ascii_lowercase() || b == b'_')
        && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && !KEYWORDS.contains(&name);
    if plain {
        out.push_str(name);
    } else {
        out.push('"');
        out.push_str(&name.replace('"', "\"\""));
        out.push('"');
    }
}
