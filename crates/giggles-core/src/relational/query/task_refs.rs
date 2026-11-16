//! Syntactic column-reference walk of a query for task statements (GD-0061).
//! Read-only over the unbound AST; nothing here binds, evaluates or refuses.
use super::*;
use crate::task::columns::{Position, StatementShape};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// The statement's own SELECT: bare items are `Output`.
    Top,
    /// A subquery or derived table: bare items are `Expression`.
    Nested,
    /// The synthetic WHERE target of a searched UPDATE: its star is not read.
    Search,
    /// One `SET col = <expression>` value.
    Assign,
    /// A RETURNING list.
    Returning,
}

/// Position of a column nested below an expression node that sits at `p`.
fn nested(p: Position) -> Position {
    match p {
        Position::Aggregate => Position::Aggregate,
        Position::AssignSource => Position::AssignSource,
        Position::Returning | Position::ReturningExpr => Position::ReturningExpr,
        _ => Position::Expression,
    }
}

fn scalar(s: &Scalar, at: Position, out: &mut StatementShape) {
    let inner = nested(at);
    match s {
        Scalar::Column(c) => out.add_ref(&c.name, at),
        Scalar::Literal(_) | Scalar::Param(_) => {}
        Scalar::Call(name, args) if name == aggregate_filter::CALL => {
            // `agg(x) FILTER (WHERE f)`: the wrapped call and the encoded filter
            // both belong to the aggregate.
            for a in args {
                scalar(a, Position::Aggregate, out);
            }
        }
        Scalar::Call(name, args) => {
            let aggregate = Aggregate::from_name(name).is_some();
            if name == aggregate_filter::COUNT_STAR {
                out.add_function("count");
            } else if !name.starts_with("\0aggregate-filter:") {
                out.add_function(name);
            }
            let at = if aggregate || name == aggregate_filter::COUNT_STAR {
                Position::Aggregate
            } else {
                inner
            };
            for a in args {
                scalar(a, at, out);
            }
        }
        Scalar::Domain(a, ..) => {
            out.casts_user_type = true;
            scalar(a, inner, out);
        }
        Scalar::Cast(a, _) | Scalar::Unary(_, a) => scalar(a, inner, out),
        Scalar::Binary(_, a, b) => {
            scalar(a, inner, out);
            scalar(b, inner, out);
        }
        Scalar::SubstringOrder(args) => {
            for a in args {
                scalar(a, inner, out);
            }
        }
        Scalar::Predicate(f) => filter(f, inner, out),
        Scalar::Case(c) => {
            for (f, a) in &c.arms {
                filter(f, inner, out);
                scalar(a, inner, out);
            }
            if let Some(a) = &c.otherwise {
                scalar(a, inner, out);
            }
        }
    }
}

fn filter(f: &Filter, at: Position, out: &mut StatementShape) {
    match f {
        Filter::Clauses(c) => clauses(c, out),
        Filter::Scalar(s) | Filter::Null(s, _) => scalar(s, at, out),
        Filter::Compare(a, _, b) => {
            scalar(a, at, out);
            scalar(b, at, out);
        }
        Filter::InSubquery { value, query, .. } => {
            scalar(value, at, out);
            subquery(query, out);
        }
        Filter::Exists { query, .. } => subquery(query, out),
        Filter::Like {
            value,
            pattern,
            escape,
            ..
        } => {
            scalar(value, at, out);
            scalar(pattern, at, out);
            if let Some(e) = escape {
                scalar(e, at, out);
            }
        }
        Filter::Not(f) | Filter::Is(f, _, _) => filter(f, at, out),
        Filter::Logic(a, _, b) => {
            filter(a, at, out);
            filter(b, at, out);
        }
    }
}

/// WHERE, GROUP BY and HAVING of the root clause bundle. The derived query is
/// walked by `Query::walk`, which also counts it.
fn clauses(c: &QueryClauses, out: &mut StatementShape) {
    for name in &c.view_reads {
        out.add_table(name);
    }
    if let Some(f) = &c.filter {
        filter(f, Position::Filter, out);
    }
    for s in &c.group {
        scalar(s, Position::Group, out);
    }
    if let Some(f) = &c.having {
        filter(f, Position::Filter, out);
    }
}

fn subquery(q: &Query, out: &mut StatementShape) {
    out.subqueries += 1;
    q.walk(out, Mode::Nested);
}

impl Query {
    /// Record this query's column references, call names, user-type casts,
    /// star flags, sources, joins, subqueries and EXPLAIN into `out`. `top` is
    /// true for the statement's own SELECT and false for a nested query.
    pub(in crate::relational) fn task_refs(&self, out: &mut StatementShape, top: bool) {
        self.walk(out, if top { Mode::Top } else { Mode::Nested });
    }

    /// The WHERE target of a searched UPDATE (`Query::mutation_target`).
    pub(in crate::relational) fn task_search_refs(&self, out: &mut StatementShape) {
        self.walk(out, Mode::Search);
    }

    /// One assignment value (`Query::assignment`): every column is a source.
    pub(in crate::relational) fn task_assign_refs(&self, out: &mut StatementShape) {
        self.walk(out, Mode::Assign);
    }

    /// A RETURNING list (`Query::returning`).
    pub(in crate::relational) fn task_returning_refs(&self, out: &mut StatementShape) {
        self.walk(out, Mode::Returning);
    }

    fn walk(&self, out: &mut StatementShape, mode: Mode) {
        out.explain |= self.explain;
        for s in &self.sources {
            out.add_table(&s.name);
        }
        out.joins += self.joins.len().max(self.sources.len().saturating_sub(1));
        if let Some(d) = self.derived() {
            out.subqueries += 1;
            d.walk(out, Mode::Nested);
        }
        let item_at = match mode {
            Mode::Top => Some(Position::Output),
            Mode::Nested => Some(Position::Expression),
            Mode::Assign => Some(Position::AssignSource),
            Mode::Returning => Some(Position::Returning),
            Mode::Search => None,
        };
        if let Some(at) = item_at {
            for (item, _) in &self.items {
                match item {
                    Item::Star | Item::QualifiedStar { .. } => {
                        if mode == Mode::Returning {
                            out.returning_star = true;
                        } else {
                            out.star = true;
                        }
                    }
                    Item::Scalar(s) => scalar(s, at, out),
                    Item::Aggregate(a, arg) => {
                        out.add_function(&a.call_name());
                        if let Some(s) = arg {
                            scalar(s, Position::Aggregate, out);
                        }
                    }
                }
            }
        }
        for (c, _, _) in &self.predicates {
            out.add_ref(&c.name, Position::Filter);
        }
        if let Some(f) = &self.filter {
            filter(f, Position::Filter, out);
        }
        for j in &self.joins {
            filter(&j.on, Position::Join, out);
        }
        for (s, _, _) in &self.order {
            scalar(s, Position::Sort, out);
        }
    }
}

impl Query {
    /// Replace every `*` and `t.*` select item with explicit column items, in
    /// the given order (R3-303). Called before bind, only in a task session.
    pub(in crate::relational) fn task_expand_star(&mut self, columns: &[String]) {
        if !self
            .items
            .iter()
            .any(|(i, _)| matches!(i, Item::Star | Item::QualifiedStar { .. }))
        {
            return;
        }
        let mut items = Vec::with_capacity(self.items.len() + columns.len());
        for (item, alias) in std::mem::take(&mut self.items) {
            match item {
                Item::Star | Item::QualifiedStar { .. } => {
                    items.extend(columns.iter().map(|c| {
                        (
                            Item::Scalar(Scalar::Column(ColumnRef {
                                qualifier: None,
                                name: c.clone(),
                            })),
                            None,
                        )
                    }));
                }
                other => items.push((other, alias)),
            }
        }
        self.items = items;
    }

    /// True when a `*` or `t.*` select item remains.
    pub(in crate::relational) fn task_has_star(&self) -> bool {
        self.items
            .iter()
            .any(|(i, _)| matches!(i, Item::Star | Item::QualifiedStar { .. }))
    }

    /// True when the query reads no relation in its FROM list (`SELECT 1`,
    /// `SELECT now()`). Unlike `is_table_free`, calls do not count: the task
    /// column rules judge them (design 3.2).
    pub(in crate::relational) fn task_has_no_sources(&self) -> bool {
        self.sources.is_empty()
    }

    /// Test hook: whether any call in this query is volatile.
    #[cfg(test)]
    pub(in crate::relational) fn task_has_volatile_call(&self) -> bool {
        self.has_volatile_call()
    }
}
