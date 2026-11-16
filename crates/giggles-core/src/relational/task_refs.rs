//! Statement shape of a prepared SQL statement for task sessions (GD-0061,
//! design section 3.2). Syntactic only: it reads the statement as parsed and
//! never binds, so it runs before any row or catalog value is touched.
use super::*;
use crate::task::columns::{Position, StatementShape};

impl Prepared {
    /// Collect the column references and shape facts of this statement.
    ///
    /// `Query`, `Select`, `Update` and `UpdateExpressions` are walked; every
    /// other statement yields its class and nothing else.
    #[allow(dead_code, reason = "R3-303 wires the column rules to this collector")]
    pub(crate) fn task_shape(&self) -> R<StatementShape> {
        let mut out = StatementShape::new(self.task_class());
        match &self.statement {
            Statement::Query(q) => q.task_refs(&mut out, true),
            Statement::CompositeSelect(_) => out.casts_user_type = true,
            Statement::Select {
                name,
                projection,
                predicates,
                order,
                ..
            } => {
                out.add_table(name);
                out.star = projection.is_empty();
                for column in projection {
                    out.add_ref(column, Position::Output);
                }
                for p in predicates {
                    out.add_ref(&p.column, Position::Filter);
                }
                for (column, _) in order {
                    out.add_ref(column, Position::Sort);
                }
            }
            Statement::Update {
                name,
                sets,
                predicates,
                search,
                returning,
            } => {
                out.add_table(name);
                for (column, _) in sets {
                    out.add_ref(column, Position::AssignTarget);
                }
                mutation_refs(&mut out, predicates, search.as_deref(), returning.as_ref());
            }
            Statement::UpdateExpressions {
                name,
                sets,
                predicates,
                search,
                returning,
            } => {
                out.add_table(name);
                for (column, value) in sets {
                    out.add_ref(column, Position::AssignTarget);
                    match value {
                        assign::Value::Scalar(q) => q.task_assign_refs(&mut out),
                        assign::Value::Plain(_) | assign::Value::Default => {}
                    }
                }
                mutation_refs(&mut out, predicates, search.as_deref(), returning.as_ref());
            }
            _ => {}
        }
        Ok(out)
    }

    /// Rewrite `SELECT *` (and `t.*`) to `columns`, and `RETURNING *` to
    /// `returning`, before bind (R3-303). Statements without a star are
    /// returned unchanged, so the rewrite is idempotent.
    pub(crate) fn task_expand_star(&self, columns: &[String], returning: &[String]) -> Prepared {
        let mut out = self.clone();
        let expand_returning = |r: &mut Option<Returning>| {
            if let Some(r) = r {
                r.query_mut().task_expand_star(returning);
            }
        };
        match &mut out.statement {
            Statement::Query(q) => q.task_expand_star(columns),
            Statement::Select { projection, .. } if projection.is_empty() => {
                projection.extend(columns.iter().cloned());
            }
            Statement::Update { returning: r, .. }
            | Statement::UpdateExpressions { returning: r, .. } => expand_returning(r),
            _ => {}
        }
        out
    }

    /// True when a select-list star survives in this statement.
    pub(crate) fn task_has_star(&self) -> bool {
        match &self.statement {
            Statement::Query(q) => q.task_has_star(),
            Statement::Select { projection, .. } => projection.is_empty(),
            _ => false,
        }
    }

    /// Test hook: whether a query statement calls a volatile routine.
    #[cfg(test)]
    pub(crate) fn task_has_volatile_call(&self) -> bool {
        match &self.statement {
            Statement::Query(q) => q.task_has_volatile_call(),
            _ => false,
        }
    }

}

fn mutation_refs(
    out: &mut StatementShape,
    predicates: &[Predicate],
    search: Option<&query::Query>,
    returning: Option<&Returning>,
) {
    for p in predicates {
        out.add_ref(&p.column, Position::Filter);
    }
    if let Some(q) = search {
        q.task_search_refs(out);
    }
    if let Some(r) = returning {
        r.query().task_returning_refs(out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::columns::{Reference, StatementClass};

    // The worked example's `deliveries` is a reserved legacy name
    // (`ident.rs`), so the same columns are used under `shipments`:
    // shipments(id bigint primary key, merchant_id bigint, status text,
    // failed_at timestamptz, attempt_count int, customer_email text,
    // card_token text, note text)
    fn shape(sql: &str) -> StatementShape {
        Prepared::parse(sql, 1, 0)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
            .task_shape()
            .unwrap()
    }
    fn at(s: &StatementShape, column: &str) -> Vec<Position> {
        s.refs
            .iter()
            .filter(|r| r.column == column)
            .map(|r| r.position)
            .collect()
    }
    fn has(s: &StatementShape, column: &str, p: Position) -> bool {
        s.refs.contains(&Reference {
            column: column.into(),
            position: p,
        })
    }

    #[test]
    fn bare_output_vs_expression() {
        let s = shape(
            "SELECT id, status AS s, lower(customer_email), attempt_count + 1, \
             failed_at::date, CASE WHEN note IS NULL THEN 'x' ELSE card_token END \
             FROM shipments",
        );
        assert_eq!(s.class, StatementClass::Select);
        assert_eq!(s.tables, ["shipments"]);
        assert_eq!(at(&s, "id"), [Position::Output]);
        assert_eq!(at(&s, "status"), [Position::Output]);
        assert_eq!(at(&s, "customer_email"), [Position::Expression]);
        assert_eq!(at(&s, "attempt_count"), [Position::Expression]);
        assert_eq!(at(&s, "failed_at"), [Position::Expression]);
        assert_eq!(at(&s, "note"), [Position::Expression]);
        assert_eq!(at(&s, "card_token"), [Position::Expression]);
        assert_eq!(s.functions, ["lower"]);
        assert!(!s.star && !s.explain && !s.casts_user_type);
        assert_eq!((s.subqueries, s.joins), (0, 0));
        let d = shape("SELECT note::short_note FROM shipments");
        assert!(d.casts_user_type);
        assert_eq!(at(&d, "note"), [Position::Expression]);
    }

    #[test]
    fn filter_sort_group_aggregate_positions() {
        let s = shape(
            "SELECT merchant_id, count(*), sum(attempt_count), max(abs(id)) FROM shipments \
             WHERE status = 'failed' AND failed_at > $1 \
             GROUP BY merchant_id HAVING count(note) > 1 ORDER BY merchant_id, upper(status)",
        );
        assert_eq!(s.class, StatementClass::Select);
        assert!(has(&s, "merchant_id", Position::Output));
        assert!(has(&s, "merchant_id", Position::Group));
        assert!(has(&s, "merchant_id", Position::Sort));
        assert_eq!(at(&s, "status"), [Position::Filter, Position::Expression]);
        assert_eq!(at(&s, "failed_at"), [Position::Filter]);
        assert_eq!(at(&s, "attempt_count"), [Position::Aggregate]);
        // A call below an aggregate keeps its argument an aggregate input.
        assert_eq!(at(&s, "id"), [Position::Aggregate]);
        assert_eq!(at(&s, "note"), [Position::Aggregate]);
        for f in ["count", "sum", "max", "abs", "upper"] {
            assert!(s.functions.iter().any(|n| n == f), "{f}: {:?}", s.functions);
        }
        let d = shape("SELECT count(DISTINCT status) FROM shipments");
        assert_eq!(at(&d, "status"), [Position::Aggregate]);
        assert!(d.functions.iter().all(|f| f.ends_with("count")), "{:?}", d.functions);
        let f = shape("SELECT count(*) FILTER (WHERE attempt_count > 2) FROM shipments");
        assert_eq!(at(&f, "attempt_count"), [Position::Aggregate]);
        assert_eq!(f.functions, ["count"]);
    }

    #[test]
    fn update_targets_sources_returning() {
        let s = shape(
            "UPDATE shipments SET status = 'retry', attempt_count = attempt_count + 1, \
             note = lower(status) WHERE id = $1 AND failed_at < now() \
             RETURNING id, upper(status)",
        );
        assert_eq!(s.class, StatementClass::Update);
        assert_eq!(s.tables, ["shipments"]);
        assert!(has(&s, "status", Position::AssignTarget));
        assert!(has(&s, "attempt_count", Position::AssignTarget));
        assert!(has(&s, "note", Position::AssignTarget));
        assert!(has(&s, "attempt_count", Position::AssignSource));
        assert!(has(&s, "status", Position::AssignSource));
        assert!(has(&s, "id", Position::Filter));
        assert!(has(&s, "failed_at", Position::Filter));
        assert!(has(&s, "id", Position::Returning));
        assert!(has(&s, "status", Position::ReturningExpr));
        assert!(!at(&s, "id").contains(&Position::Output));
        assert!(!s.star && !s.returning_star);
        assert!(s.functions.iter().any(|f| f == "lower"));
        assert!(s.functions.iter().any(|f| f == "now"));
        assert!(s.functions.iter().any(|f| f == "upper"));
    }

    #[test]
    fn star_and_qualified_star_flag() {
        let s = shape("SELECT * FROM shipments");
        assert!(s.star && s.refs.is_empty());
        let q = shape("SELECT d.* FROM shipments d WHERE d.id = 1");
        assert!(q.star);
        assert!(has(&q, "id", Position::Filter));
        let n = shape("SELECT id FROM shipments");
        assert!(!n.star);
        let r = shape("UPDATE shipments SET status = 'x' WHERE id = 1 RETURNING *");
        assert!(r.returning_star && !r.star);
        let r = shape("UPDATE shipments SET status = 'x' WHERE id = 1 RETURNING shipments.*");
        assert!(r.returning_star && !r.star);
    }

    #[test]
    fn subquery_join_derived_counted() {
        let s = shape(
            "SELECT id FROM shipments WHERE merchant_id IN (SELECT id FROM merchants WHERE \
             region = 'eu') AND EXISTS (SELECT 1 FROM refunds)",
        );
        assert_eq!(s.subqueries, 2);
        assert_eq!(s.tables, ["shipments", "merchants", "refunds"]);
        assert!(has(&s, "merchant_id", Position::Filter));
        // A subquery's own select item is not an output of the statement.
        assert!(has(&s, "id", Position::Expression));
        assert!(has(&s, "region", Position::Filter));

        let j = shape(
            "SELECT d.id, m.name FROM shipments d JOIN merchants m ON d.merchant_id = m.id",
        );
        assert_eq!(j.joins, 1);
        assert_eq!(j.tables, ["shipments", "merchants"]);
        assert!(has(&j, "merchant_id", Position::Join));
        let l = shape(
            "SELECT s.id FROM shipments s LEFT JOIN merchants m ON m.id = s.merchant_id \
             JOIN refunds r ON r.shipment_id = s.id",
        );
        assert_eq!(l.joins, 2);
        assert_eq!(l.tables, ["shipments", "merchants", "refunds"]);

        let d = shape("SELECT x.status FROM (SELECT status FROM shipments) AS x");
        assert_eq!(d.subqueries, 1);
        assert_eq!(d.tables, ["shipments"]);
        assert!(has(&d, "status", Position::Output));

        let u = shape(
            "UPDATE shipments SET status = 'x' WHERE merchant_id IN (SELECT id FROM merchants)",
        );
        assert_eq!(u.subqueries, 1);
        assert_eq!(u.tables, ["shipments", "merchants"]);
    }

    #[test]
    fn legacy_select_and_update_arms() {
        let mut p = Prepared::parse("SELECT 1", 1, 0).unwrap();
        p.statement = Statement::Select {
            name: "shipments".into(),
            projection: vec!["id".into(), "status".into()],
            predicates: vec![Predicate {
                column: "merchant_id".into(),
                op: "=".into(),
                value: Expr::Literal(Value::Int(7)),
            }],
            order: vec![("id".into(), false)],
            limit: Some(Expr::Literal(Value::Int(10))),
        };
        let s = p.task_shape().unwrap();
        assert_eq!(s.class, StatementClass::Select);
        assert_eq!(s.tables, ["shipments"]);
        assert_eq!(at(&s, "id"), [Position::Output, Position::Sort]);
        assert_eq!(at(&s, "status"), [Position::Output]);
        assert_eq!(at(&s, "merchant_id"), [Position::Filter]);
        assert!(!s.star);
        if let Statement::Select { projection, .. } = &mut p.statement {
            projection.clear();
        }
        assert!(p.task_shape().unwrap().star);

        let u = shape("UPDATE shipments SET status = 'retry', attempt_count = $2 WHERE id = $1");
        assert_eq!(u.class, StatementClass::Update);
        assert_eq!(at(&u, "status"), [Position::AssignTarget]);
        assert_eq!(at(&u, "attempt_count"), [Position::AssignTarget]);
        assert_eq!(at(&u, "id"), [Position::Filter]);

        for (sql, class) in [
            ("INSERT INTO shipments (id) VALUES (1)", StatementClass::Insert),
            ("DELETE FROM shipments WHERE id = 1", StatementClass::Delete),
            ("BEGIN", StatementClass::TransactionControl),
            ("SAVEPOINT a", StatementClass::Savepoint),
            ("DROP TABLE shipments", StatementClass::Ddl),
            ("EXPLAIN SELECT id FROM shipments", StatementClass::Explain),
        ] {
            let s = shape(sql);
            assert_eq!(s.class, class, "{sql}");
            if class == StatementClass::Explain {
                assert!(s.explain);
            } else {
                assert!(s.refs.is_empty() && s.tables.is_empty(), "{sql}");
            }
        }
    }

    #[test]
    fn function_names_collected_nested_case() {
        let s = shape(
            "SELECT CASE WHEN length(btrim(note)) > 3 THEN coalesce(status, upper(card_token)) \
             ELSE nextval('s') END FROM shipments WHERE starts_with(customer_email, 'a')",
        );
        for f in ["length", "btrim", "coalesce", "upper", "nextval", "starts_with"] {
            assert!(s.functions.iter().any(|n| n == f), "{f}: {:?}", s.functions);
        }
        assert_eq!(at(&s, "note"), [Position::Expression]);
        assert_eq!(at(&s, "status"), [Position::Expression]);
        assert_eq!(at(&s, "card_token"), [Position::Expression]);
        assert_eq!(at(&s, "customer_email"), [Position::Expression]);
        let t = shape("SELECT now()");
        assert!(t.tables.is_empty());
        assert_eq!(t.functions, ["now"]);
    }
}
