//! Profile 0.22 native preview and apply (GD-0061 R3-708, design-7 D7.1).
//!
//! - `preview { <update> } using task <name>` lowers the native `update` to
//!   one SQL UPDATE and previews it through `task::preview` with
//!   `Surface::Native`. Core has no native parser, so the stored text is that
//!   SQL lowering; apply re-runs it as SQL.
//! - `apply change <id> digest <hex> once <op> for <duration>` is one
//!   protected commit of the stored preview (`ProtectedRequest::Apply`) under
//!   operation ID `<op>`. The duration is validated, then ignored.
//! - `approve change ..` parses and is 0A000: the PostgreSQL listener has no
//!   native-text entry, so there are no native approver forms in R3.
//!
//! Task errors carry the SQL surface's SQLSTATE (`TaskError::sqlstate`).
//! Dispatch is on the head words only, as for `crate::task`.
use crate::parser::{Parser, Token, lex};
use crate::task::{Params, identifier, render};
use crate::{
    Bindings, DurationLiteral, Error, Expr, Intent, LanguageVersion, Result, Statement, Value, ValueExpr,
};
use giggles_core::SqlSession;
use giggles_core::relational::StatementClock;
use giggles_core::task::context::TaskContext;
use giggles_core::task::statements::parse_digest;
use giggles_core::task::{
    Actor, DenyRule, OUTCOME_RETENTION_MS, OperationId, PreviewId, PreviewSummary, ProtectedRequest,
    Receipt, Surface, TaskError, preview, protected_commit,
};

/// One native change statement of profile 0.22.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChangeStatement {
    /// `preview { <update> } using task <name>`. `task` is a text literal (a
    /// native string, word or quoted identifier) or a parameter.
    Preview { update: Statement, task: ValueExpr },
    /// `apply change <id> digest <hex> once <op> for <duration>`.
    Apply {
        change: ValueExpr,
        digest: ValueExpr,
        once: ValueExpr,
        retention: DurationLiteral,
    },
    /// `approve change <id> [of task <name>] digest <hex>`: 0A000 when run.
    Approve {
        change: ValueExpr,
        task: Option<ValueExpr>,
        digest: ValueExpr,
    },
}

/// What a native change statement returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChangeOutcome {
    /// `(change_id, digest, row_count, expires_at)`.
    Preview(PreviewSummary),
    /// The protected commit's receipt.
    Applied(Receipt),
}

/// The SQL UPDATE a previewed native `update` lowers to, with its
/// positional parameters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoweredUpdate {
    pub sql: String,
    pub params: Vec<Value>,
}

/// Parse one native change statement in an explicitly selected profile.
/// Profiles before 0.22 return [`Error::Unsupported`].
pub fn parse_change_with_version(text: &str, version: LanguageVersion) -> Result<ChangeStatement> {
    if !version.supports_tasks() {
        return Err(Error::Unsupported);
    }
    let mut p = Parser {
        tokens: lex(text, version)?,
        pos: 0,
        end: text.len(),
        version,
    };
    let statement = if p.word("preview") {
        preview_block(&mut p)?
    } else if p.word("apply") {
        p.need("change")?;
        let change = p.value()?;
        p.need("digest")?;
        let digest = p.value()?;
        p.need("once")?;
        let once = p.value()?;
        p.need("for")?;
        let retention = p.retention()?;
        let seconds = u64::try_from(retention.microseconds / 1_000_000).map_err(|_| Error::Type)?;
        if seconds.saturating_mul(1000) > OUTCOME_RETENTION_MS {
            return Err(Error::Limit);
        }
        ChangeStatement::Apply {
            change,
            digest,
            once,
            retention,
        }
    } else if p.word("approve") {
        p.need("change")?;
        let change = p.value()?;
        let task = if p.word("of") {
            p.need("task")?;
            Some(task_name(&mut p)?)
        } else {
            None
        };
        p.need("digest")?;
        ChangeStatement::Approve {
            change,
            task,
            digest: p.value()?,
        }
    } else {
        return Err(p.error());
    };
    p.symbol(';');
    if p.peek().is_some() {
        return Err(p.error());
    }
    Ok(statement)
}

fn preview_block(p: &mut Parser) -> Result<ChangeStatement> {
    if !p.symbol('{') {
        return Err(p.error());
    }
    if p.peek() != Some(&Token::Word("update".into())) {
        return Err(Error::Unsupported);
    }
    let update = p.base_statement()?;
    match &update {
        Statement::Update { guard: None, intent, .. }
        | Statement::UpdateExpressions { guard: None, intent, .. }
            if !matches!(intent, Intent::Claim { .. }) => {}
        _ => return Err(Error::Unsupported),
    }
    crate::execute::validate_depth(&update, p.version)?;
    if !p.symbol('}') {
        return Err(p.error());
    }
    p.need("using")?;
    p.need("task")?;
    Ok(ChangeStatement::Preview {
        update,
        task: task_name(p)?,
    })
}

/// A task name: a native string, a word, a quoted identifier or a parameter.
fn task_name(p: &mut Parser) -> Result<ValueExpr> {
    match p.peek().cloned() {
        Some(Token::String(name)) => {
            p.pos += 1;
            Ok(ValueExpr::Literal(Value::Text(name)))
        }
        Some(Token::Param(name)) => {
            p.pos += 1;
            Ok(ValueExpr::Parameter(name))
        }
        _ => Ok(ValueExpr::Literal(Value::Text(p.name()?))),
    }
}

/// Lowers a previewed native `update` to `UPDATE t SET .. WHERE ..`. Each
/// `$name` becomes a positional `$n` with its bound value.
pub fn lower_preview_update(update: &Statement, bindings: &Bindings) -> Result<LoweredUpdate> {
    let (table, filter, values): (&str, &Expr, Vec<(&str, Expr)>) = match update {
        Statement::Update {
            table,
            filter,
            guard: None,
            values,
            intent: Intent::One | Intent::Many { .. },
        } => (
            table,
            filter,
            values
                .iter()
                .map(|(name, value)| (name.as_str(), Expr::Value(value.clone())))
                .collect(),
        ),
        Statement::UpdateExpressions {
            table,
            filter,
            guard: None,
            values,
            intent: Intent::One | Intent::Many { .. },
        } => (
            table,
            filter,
            values.iter().map(|(name, value)| (name.as_str(), value.clone())).collect(),
        ),
        _ => return Err(Error::Unsupported),
    };
    let mut params = Params::bound(bindings);
    let mut sql = String::from("UPDATE ");
    match table.split_once('.') {
        Some((schema, name)) => {
            identifier(schema, &mut sql);
            sql.push('.');
            identifier(name, &mut sql);
        }
        None => identifier(table, &mut sql),
    }
    sql.push_str(" SET ");
    for (i, (column, value)) in values.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        identifier(column, &mut sql);
        sql.push_str(" = ");
        render(value, 1, &mut sql, &mut params)?;
    }
    sql.push_str(" WHERE ");
    render(filter, 0, &mut sql, &mut params)?;
    Ok(LoweredUpdate {
        sql,
        params: params.values,
    })
}

/// Parses and runs one native change statement in the task session
/// `session`, bound to `ctx`, under the statement clock `clock`.
pub fn execute_change(
    session: &mut SqlSession,
    ctx: &TaskContext,
    text: &str,
    version: LanguageVersion,
    bindings: &Bindings,
    clock: &StatementClock,
) -> Result<ChangeOutcome> {
    match parse_change_with_version(text, version)? {
        ChangeStatement::Preview { update, task } => {
            if text_value(&task, bindings)? != ctx.name.as_str() {
                return Err(task_error(TaskError::Denied(DenyRule::RolePrivilege)));
            }
            let lowered = lower_preview_update(&update, bindings)?;
            let summary = preview(session, ctx, Surface::Native, &lowered.sql, &lowered.params, clock)
                .map_err(task_error)?;
            intent_bound(&update, bindings, summary.rows)?;
            Ok(ChangeOutcome::Preview(summary))
        }
        ChangeStatement::Apply {
            change,
            digest,
            once,
            ..
        } => {
            let change = match value(&change, bindings)? {
                Value::Int(id) => u64::try_from(id).map_err(|_| Error::Type)?,
                _ => return Err(Error::Type),
            };
            let digest = parse_digest(&text_value(&digest, bindings)?).ok_or_else(|| Error::Sql {
                sqlstate: "22023".into(),
            })?;
            let op = OperationId::new(text_value(&once, bindings)?.into_bytes()).ok_or(Error::Limit)?;
            let request = ProtectedRequest::Apply {
                preview: PreviewId(change),
                digest,
            };
            protected_commit(session, Actor::Executor(ctx), &op, request)
                .map(ChangeOutcome::Applied)
                .map_err(task_error)
        }
        ChangeStatement::Approve { .. } => Err(Error::Sql {
            sqlstate: "0A000".into(),
        }),
    }
}

/// The SQL surface's code for a task error.
pub fn task_error(error: TaskError) -> Error {
    Error::Sql {
        sqlstate: error.sqlstate().to_owned(),
    }
}

/// The native intent, checked on the preview's row count: a single-row
/// `update` matches exactly one row, `update many .. at most n` at most `n`.
/// Apply re-runs only the exact previewed row set, so the bound also holds
/// for what apply writes. A preview that breaks it is stored but its id and
/// digest are not returned.
fn intent_bound(update: &Statement, bindings: &Bindings, rows: u32) -> Result<()> {
    let intent = match update {
        Statement::Update { intent, .. } | Statement::UpdateExpressions { intent, .. } => intent,
        _ => return Err(Error::Unsupported),
    };
    match intent {
        Intent::One if rows == 0 => Err(Error::NotFound),
        Intent::One if rows > 1 => Err(Error::AmbiguousMatch),
        Intent::Many { cap } => match value(cap, bindings)? {
            Value::Int(cap) if cap >= 0 => {
                if i64::from(rows) > cap {
                    Err(Error::TooManyMatches)
                } else {
                    Ok(())
                }
            }
            _ => Err(Error::Type),
        },
        _ => Ok(()),
    }
}

fn value(expr: &ValueExpr, bindings: &Bindings) -> Result<Value> {
    match expr {
        ValueExpr::Literal(value) => Ok(value.clone()),
        ValueExpr::Parameter(name) => bindings
            .get(name)
            .cloned()
            .ok_or_else(|| Error::Bind(name.clone())),
    }
}

fn text_value(expr: &ValueExpr, bindings: &Bindings) -> Result<String> {
    match value(expr, bindings)? {
        Value::Text(text) => Ok(text),
        _ => Err(Error::Type),
    }
}
