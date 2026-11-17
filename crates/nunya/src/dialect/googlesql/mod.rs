//! Fail-closed GoogleSQL lowering. Literals are never decrypted: use named parameters for tokens.
use crate::{
    dialect::Lowering,
    error::{NyCode, Refusal},
    ir::*,
    policy::{Policy, Treatment},
    schema::{ColumnRef, RelationKind, Snapshot},
    value::ColumnType,
};
use sqlparser::{
    ast as a,
    dialect::BigQueryDialect,
    keywords::Keyword,
    parser::Parser,
    tokenizer::{Token, Tokenizer},
};
use std::collections::{BTreeMap, BTreeSet};
mod expr;
mod query;
#[cfg(test)]
mod tests;
/// GoogleSQL lowering entry point.
pub struct GoogleSqlLowering;
type Result<T> = std::result::Result<T, Refusal>;
fn refuse() -> Refusal {
    Refusal::new(NyCode::NotAnalysable, "unsupported GoogleSQL syntax")
}
#[derive(Clone)]
struct Output {
    ir: OutputColumn,
    inputs: Vec<(ColumnRef, Label)>,
}
#[derive(Clone)]
struct Bound {
    schema: String,
    table: String,
    output: Output,
    visible: bool,
    qualified: bool,
    ordinal: usize,
}
type Scope = Vec<Bound>;
struct State<'a> {
    snapshot: &'a Snapshot,
    policy: &'a Policy,
    stmt: ResolvedStatement,
    names: Vec<String>,
    plain: BTreeSet<u32>,
    ctes: BTreeMap<String, Vec<Output>>,
    outers: Vec<Scope>,
    depth: usize,
    view_depth: usize,
    sql_has_backslash: bool,
}
/// Lower exactly one GoogleSQL statement, returning parameter names in textual occurrence order.
pub fn lower_gsql(
    sql: &str,
    snapshot: &Snapshot,
    policy: &Policy,
) -> Result<(ResolvedStatement, Vec<String>)> {
    let parsed = match Parser::parse_sql(&BigQueryDialect {}, sql) {
        Ok(parsed) => parsed,
        Err(_) => {
            let mut tokens = Tokenizer::new(&BigQueryDialect {}, sql)
                .tokenize()
                .map_err(|_| refuse())?;
            tokens.retain(|token| !matches!(token, Token::Whitespace(_)));
            while matches!(tokens.last(), Some(Token::SemiColon)) {
                tokens.pop();
            }
            if tokens.iter().any(|token| matches!(token, Token::SemiColon)) {
                return Err(refuse());
            }
            // Classifying unparsable Spanner DDL/DCL as refused never accepts input.
            match tokens.first() {
                Some(Token::Word(word))
                    if matches!(
                        word.keyword,
                        Keyword::CREATE
                            | Keyword::ALTER
                            | Keyword::DROP
                            | Keyword::GRANT
                            | Keyword::REVOKE
                            | Keyword::RENAME
                            | Keyword::TRUNCATE
                            | Keyword::ANALYZE
                    ) =>
                {
                    return Ok((
                        ResolvedStatement {
                            canonical_sql: None,
                            kind: StatementKind::Refused(NyCode::StatementNotAllowed),
                            outputs: vec![],
                            uses: vec![],
                            writes: vec![],
                            param_count: 0,
                            plain_params: vec![],
                            source_span: Span {
                                start: 0,
                                end: sql.len(),
                            },
                        },
                        vec![],
                    ));
                }
                _ => return Err(refuse()),
            }
        }
    };
    if parsed.len() != 1 {
        return Err(refuse());
    }
    let tokens = Tokenizer::new(&BigQueryDialect {}, sql)
        .tokenize()
        .map_err(|_| refuse())?;
    let mut names = vec![];
    let mut ts = tokens
        .iter()
        .filter(|t| !matches!(t, Token::Whitespace(_)))
        .peekable();
    while let Some(t) = ts.next() {
        if matches!(t, Token::AtSign) {
            match ts.next() {
                Some(Token::Word(w)) => {
                    if !names.contains(&w.value) {
                        names.push(w.value.clone())
                    }
                }
                _ => return Err(refuse()),
            }
        }
    }
    let mut s = State {
        snapshot,
        policy,
        stmt: ResolvedStatement {
            canonical_sql: None,
            kind: StatementKind::Select,
            outputs: vec![],
            uses: vec![],
            writes: vec![],
            param_count: u32::try_from(names.len()).map_err(|_| refuse())?,
            plain_params: vec![],
            source_span: Span {
                start: 0,
                end: sql.len(),
            },
        },
        names,
        plain: BTreeSet::new(),
        ctes: BTreeMap::new(),
        outers: vec![],
        depth: 0,
        view_depth: 0,
        sql_has_backslash: sql.contains('\\'),
    };
    s.statement(&parsed[0])?;
    s.stmt.plain_params = s.plain.into_iter().collect();
    Ok((s.stmt, s.names))
}
impl Lowering for GoogleSqlLowering {
    fn lower(
        &self,
        sql: &str,
        snapshot: &Snapshot,
        policy: &Policy,
        _param_types: &[Option<ColumnType>],
    ) -> Result<Vec<ResolvedStatement>> {
        Ok(vec![lower_gsql(sql, snapshot, policy)?.0])
    }
}
impl State<'_> {
    fn label(&self, c: &ColumnRef) -> Label {
        match self.policy.treatment(c) {
            Some(Treatment::Blind { domain }) => Label::Blind(domain.clone()),
            Some(Treatment::Masked { .. }) => Label::Masked,
            None => Label::Plain,
        }
    }
    fn record(&mut self, e: &expr::Expr, kind: UseKind, context: &Context) {
        for (c, l) in &e.inputs {
            self.stmt.uses.push(ColumnUse {
                column: c.clone(),
                label: if e.label == Label::Tainted {
                    Label::Tainted
                } else {
                    l.clone()
                },
                kind: kind.clone(),
                context: context.clone(),
            })
        }
    }
    fn root(
        &mut self,
        n: &a::Expr,
        scope: &Scope,
        context: Context,
        kind: UseKind,
    ) -> Result<expr::Expr> {
        let e = self.expression(n, scope, &context)?;
        if e.bare || matches!(kind, UseKind::Order | UseKind::GroupBy) {
            self.record(&e, kind, &context)
        }
        self.plain.extend(&e.params);
        Ok(e)
    }
    fn resolve(&self, names: &[a::Ident], scope: &Scope) -> Result<Output> {
        for scope in std::iter::once(scope).chain(self.outers.iter().rev()) {
            let mut found = scope.iter().filter(|b| match names {
                [c] => b.visible && b.output.ir.name.to_lowercase() == c.value.to_lowercase(),
                [t, c] => {
                    b.qualified
                        && b.table.to_lowercase() == t.value.to_lowercase()
                        && b.output.ir.name.to_lowercase() == c.value.to_lowercase()
                }
                [s, t, c] => {
                    b.qualified
                        && b.schema.to_lowercase() == s.value.to_lowercase()
                        && b.table.to_lowercase() == t.value.to_lowercase()
                        && b.output.ir.name.to_lowercase() == c.value.to_lowercase()
                }
                _ => false,
            });
            if let Some(b) = found.next() {
                if found.next().is_some() {
                    return Err(refuse());
                }
                return Ok(b.output.clone());
            }
            if names.len() > 1
                && scope
                    .iter()
                    .any(|b| b.table.to_lowercase() == names[names.len() - 2].value.to_lowercase())
            {
                return Err(refuse());
            }
        }
        Err(refuse())
    }
    fn relation(&self, name: &a::ObjectName) -> Result<crate::schema::Relation> {
        let (schema, table) = match name.0.as_slice() {
            [t] => ("", t.value.as_str()),
            [s, t] => (s.value.as_str(), t.value.as_str()),
            _ => return Err(refuse()),
        };
        let mut rs = self.snapshot.relations.iter().filter(|r| {
            r.schema.to_lowercase() == schema.to_lowercase()
                && r.name.to_lowercase() == table.to_lowercase()
        });
        let r = rs.next().ok_or_else(refuse)?;
        if rs.next().is_some() {
            return Err(refuse());
        }
        Ok(r.clone())
    }
    fn bind(
        outputs: Vec<Output>,
        schema: String,
        table: String,
        alias: Option<&a::TableAlias>,
    ) -> Result<Scope> {
        let mut outputs = outputs;
        if let Some(al) = alias {
            if al.columns.len() > outputs.len() || al.columns.iter().any(|c| c.data_type.is_some())
            {
                return Err(refuse());
            }
            for (o, n) in outputs.iter_mut().zip(&al.columns) {
                o.ir.name = n.name.value.clone()
            }
        }
        Ok(outputs
            .into_iter()
            .enumerate()
            .map(|(ordinal, output)| Bound {
                schema: if alias.is_some() {
                    String::new()
                } else {
                    schema.clone()
                },
                table: alias.map_or_else(|| table.clone(), |a| a.name.value.clone()),
                output,
                visible: true,
                qualified: true,
                ordinal,
            })
            .collect())
    }
    fn table_outputs(&self, r: &crate::schema::Relation) -> Vec<Output> {
        r.columns
            .iter()
            .map(|c| {
                let col = ColumnRef {
                    schema: r.schema.clone(),
                    table: r.name.clone(),
                    column: c.name.clone(),
                };
                let label = self.label(&col);
                Output {
                    inputs: if label == Label::Plain {
                        vec![]
                    } else {
                        vec![(col.clone(), label.clone())]
                    },
                    ir: OutputColumn {
                        name: c.name.clone(),
                        label,
                        source: Some(col),
                        ty: Some(c.ty.clone()),
                    },
                }
            })
            .collect()
    }
}
