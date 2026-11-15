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
