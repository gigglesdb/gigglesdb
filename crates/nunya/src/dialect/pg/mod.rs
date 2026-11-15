//! Fail-closed PostgreSQL statement lowering.
use crate::{
    dialect::Lowering,
    error::{NyCode, Refusal},
    ir::*,
    policy::{Policy, Treatment},
    schema::{ColumnRef, RelationKind, Snapshot},
    value::ColumnType,
};
use pg_query::{NodeEnum, protobuf as p};
use std::collections::{BTreeMap, BTreeSet};
mod dml;
mod expr;
mod functions;
mod lexer;
mod scope;
mod select;
#[cfg(test)]
mod tests;
/// PostgreSQL lowering entry point.
pub struct PgLowering;
type Result<T> = std::result::Result<T, Refusal>;
fn refuse() -> Refusal {
    Refusal::new(NyCode::NotAnalysable, "unsupported PostgreSQL syntax")
}
fn node(n: &p::Node) -> Result<&NodeEnum> {
    n.node.as_ref().ok_or_else(refuse)
}
fn strings(ns: &[p::Node]) -> Result<Vec<String>> {
    ns.iter()
        .map(|n| match node(n)? {
            NodeEnum::String(s) => Ok(s.sval.clone()),
            _ => Err(refuse()),
        })
        .collect()
}
fn empty(span: Span) -> ResolvedStatement {
    ResolvedStatement {
        kind: StatementKind::Select,
        outputs: vec![],
        uses: vec![],
        writes: vec![],
        param_count: 0,
        plain_params: vec![],
        source_span: span,
    }
}

fn setting_string(n: &p::Node) -> Option<&str> {
    match n.node.as_ref()? {
        NodeEnum::AConst(c) if !c.isnull => match c.val.as_ref()? {
            p::a_const::Val::Sval(s) => Some(&s.sval),
            _ => None,
        },
        _ => None,
    }
}
fn setting_integer(n: &p::Node) -> Option<i32> {
    match n.node.as_ref()? {
        NodeEnum::AConst(c) if !c.isnull => match c.val.as_ref()? {
            p::a_const::Val::Ival(i) => Some(i.ival),
            _ => None,
        },
        _ => None,
    }
}
fn session_setting(s: &p::VariableSetStmt) -> StatementKind {
    let denied = StatementKind::Refused(NyCode::StatementNotAllowed);
    let name = s.name.to_ascii_lowercase();
    if s.kind == p::VariableSetKind::VarSetMulti as i32 && name == "transaction" {
        let valid = !s.args.is_empty()
            && s.args.iter().all(|n| match n.node.as_ref() {
                Some(NodeEnum::DefElem(d)) => match (d.defname.as_str(), d.arg.as_deref()) {
                    ("transaction_isolation", Some(arg)) => matches!(
                        setting_string(arg),
                        Some(
                            "serializable"
                                | "repeatable read"
                                | "read committed"
                                | "read uncommitted"
                        )
                    ),
                    ("transaction_read_only", Some(arg)) => {
                        matches!(setting_integer(arg), Some(0 | 1))
                    }
                    _ => false,
                },
                _ => false,
            });
        // Reuse the non-recovery transaction path without Begin's status transition.
        return if valid {
            StatementKind::Savepoint
        } else {
            denied
        };
    }
    if s.kind != p::VariableSetKind::VarSetValue as i32 {
        return denied;
    }
    let values = s
        .args
        .iter()
        .map(setting_string)
        .collect::<Option<Vec<_>>>();
    let normalized = values.as_ref().map(|v| {
        v.iter()
            .map(|s| s.trim().to_ascii_lowercase())
            .collect::<Vec<_>>()
    });
    let one = normalized.as_ref().and_then(|v| {
        if v.len() == 1 {
            Some(v[0].as_str())
        } else {
            None
        }
    });
    let allowed = match name.as_str() {
        "extra_float_digits" => {
            one == Some("3") || (s.args.len() == 1 && setting_integer(&s.args[0]) == Some(3))
        }
        "application_name" => matches!(values.as_deref(), Some([value]) if value.len() <= 64),
        "client_encoding" => matches!(one, Some("utf8" | "utf-8" | "unicode")),
        "timezone" => matches!(one, Some("utc" | "etc/utc")),
        "datestyle" => {
            matches!(one, Some("iso, mdy" | "iso"))
                || matches!(normalized.as_deref(), Some([a, b]) if a == "iso" && b == "mdy")
        }
        "intervalstyle" => one == Some("postgres"),
        "standard_conforming_strings" => one == Some("on"),
        _ => false,
    };
    if allowed {
        StatementKind::SessionNoop { tag: "SET" }
    } else {
        denied
    }
}

struct State<'a> {
    sql: String,
    snapshot: &'a Snapshot,
    policy: &'a Policy,
    statement: ResolvedStatement,
    plain: BTreeSet<u32>,
    ctes: BTreeMap<String, Vec<scope::Output>>,
    outers: Vec<scope::Scope>,
    depth: usize,
    view_depth: usize,
    view_body: bool,
}
impl State<'_> {
    fn label(&self, col: &ColumnRef) -> Label {
        match self.policy.treatment(col) {
            Some(Treatment::Blind { domain }) => Label::Blind(domain.clone()),
            Some(Treatment::Masked { .. }) => Label::Masked,
            None => Label::Plain,
        }
    }
    fn root(
        &mut self,
        n: &p::Node,
        scope: &scope::Scope,
        context: Context,
        kind: UseKind,
    ) -> Result<expr::Expr> {
        let e = self.expression(n, scope, &context)?;
        if e.bare || e.source.is_some() || matches!(kind, UseKind::Order | UseKind::GroupBy) {
            self.record(&e, kind, &context);
        }
        self.plain.extend(e.params.iter().copied());
        Ok(e)
    }
    fn record(&mut self, e: &expr::Expr, kind: UseKind, context: &Context) {
        for (column, label) in &e.inputs {
            self.statement.uses.push(ColumnUse {
                column: column.clone(),
                label: if e.label == Label::Tainted {
                    Label::Tainted
                } else {
                    label.clone()
                },
                kind: kind.clone(),
                context: context.clone(),
            });
        }
    }
}
impl Lowering for PgLowering {
    fn lower(
        &self,
        sql: &str,
        snapshot: &Snapshot,
        policy: &Policy,
        _param_types: &[Option<ColumnType>],
    ) -> Result<Vec<ResolvedStatement>> {
        let parsed = pg_query::parse(sql, 0).map_err(|_| refuse())?;
        let mut statements = Vec::new();
        for raw in parsed.protobuf.stmts {
            let start = usize::try_from(raw.stmt_location).map_err(|_| refuse())?;
            let end = if raw.stmt_len == 0 {
                sql.len()
            } else {
                start
                    .checked_add(usize::try_from(raw.stmt_len).map_err(|_| refuse())?)
                    .ok_or_else(refuse)?
            };
            if end > sql.len() || start > end {
                return Err(refuse());
            }
            let mut state = State {
                sql: sql.into(),
                snapshot,
                policy,
                statement: empty(Span { start, end }),
                plain: BTreeSet::new(),
                ctes: BTreeMap::new(),
                outers: vec![],
                depth: 0,
                view_depth: 0,
                view_body: false,
            };
            match node(raw.stmt.as_deref().ok_or_else(refuse)?)? {
                NodeEnum::SelectStmt(s) => {
                    if s.into_clause.is_some() {
                        state.statement.kind = StatementKind::Refused(NyCode::StatementNotAllowed);
                    } else if !state.tokenize(s)? {
                        match state.select(s) {
                            Ok(outputs) => {
                                state.statement.outputs =
                                    outputs.into_iter().map(scope::Output::into_ir).collect()
                            }
                            Err(e) if e.code == NyCode::StatementNotAllowed => {
                                state.plain.clear();
                                state.statement = empty(state.statement.source_span.clone());
                                state.statement.kind =
                                    StatementKind::Refused(NyCode::StatementNotAllowed);
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
                NodeEnum::VariableSetStmt(s) => state.statement.kind = session_setting(s),
                NodeEnum::TransactionStmt(t) => {
                    state.statement.kind = match p::TransactionStmtKind::try_from(t.kind)
                        .map_err(|_| refuse())?
                    {
                        p::TransactionStmtKind::TransStmtBegin
                        | p::TransactionStmtKind::TransStmtStart => StatementKind::Begin,
                        p::TransactionStmtKind::TransStmtCommit => StatementKind::Commit,
                        p::TransactionStmtKind::TransStmtRollback => StatementKind::Rollback,
                        p::TransactionStmtKind::TransStmtSavepoint => StatementKind::Savepoint,
                        p::TransactionStmtKind::TransStmtRelease => StatementKind::ReleaseSavepoint,
                        p::TransactionStmtKind::TransStmtRollbackTo => {
                            StatementKind::RollbackToSavepoint
                        }
                        _ => StatementKind::Refused(NyCode::StatementNotAllowed),
                    };
                }
                n @ (NodeEnum::InsertStmt(_)
                | NodeEnum::UpdateStmt(_)
                | NodeEnum::DeleteStmt(_)) => match state.lower_dml(n) {
                    Ok(()) => {}
                    Err(e) if e.code == NyCode::StatementNotAllowed => {
                        state.plain.clear();
                        state.statement = empty(state.statement.source_span.clone());
                        state.statement.kind = StatementKind::Refused(NyCode::StatementNotAllowed);
                    }
                    Err(e) => return Err(e),
                },
                _ => state.statement.kind = StatementKind::Refused(NyCode::StatementNotAllowed),
            }
            state.statement.plain_params = state.plain.into_iter().collect();
            statements.push(state.statement);
        }
        Ok(statements)
    }
}
