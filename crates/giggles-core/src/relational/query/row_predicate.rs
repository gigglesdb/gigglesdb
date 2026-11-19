//! A task's visible-rows predicate, bound once for row-source filtering
//! (GD-0061 R3-305, design-3 section 3).
//!
//! Ruling R1: the predicate rules are R3-104's (`task_predicate`). `bind`
//! validates through `task_predicate::canonical` and then binds the canonical
//! text once, so `accepts` evaluates a bound filter per row without parsing.
//! On top of R3-104 it refuses calls outside the task function allowlist and
//! volatile calls, and it derives `pushdown()`: the top-level AND of
//! `column op literal` comparisons a secondary index can serve.
use super::*;

/// A bound visible-rows predicate over one table. `Send + Sync`, so one bound
/// copy is shared through the per-database cache.
pub(crate) struct RowPredicate {
    table: u64,
    filter: BoundFilter,
    pushdown: Vec<(usize, &'static str, Value)>,
    key_range: Option<scan::KeyRange>,
}

const _: fn() = || {
    fn shared<T: Send + Sync>() {}
    shared::<RowPredicate>();
};

impl std::fmt::Debug for RowPredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowPredicate")
            .field("table", &self.table)
            .field("pushdown", &self.pushdown)
            .finish_non_exhaustive()
    }
}

impl RowPredicate {
    /// Validates `text` as a task predicate on `table` (R3-104 rules) and binds
    /// it. Refuses parameters, subqueries, calls outside the task allowlist and
    /// volatile calls.
    pub(crate) fn bind(state: &Relational, table: &TableDef, text: &str) -> R<Self> {
        let canonical = super::super::task_predicate::canonical(state, table, text)?;
        let (parsed, filter) = constraints::constraint_bind(state, table, &lex(&canonical)?)?;
        if parsed.has_volatile_call() {
            return Err(SqlError::Unsupported);
        }
        parsed.visit_subqueries(&mut |_, _| Err(SqlError::Unsupported))?;
        parsed.walk_scalars(&mut |scalar| match scalar {
            Scalar::Param(_) => Err(SqlError::Unsupported),
            Scalar::Call(name, _)
                if crate::task::columns::TASK_FUNCTIONS
                    .binary_search(&name.as_str())
                    .is_err() =>
            {
                Err(SqlError::Unsupported)
            }
            Scalar::Predicate(nested) => {
                nested.visit_subqueries(&mut |_, _| Err(SqlError::Unsupported))
            }
            _ => Ok(()),
        })?;
        let mut pushdown = Vec::new();
        conjuncts(table, &parsed, &mut pushdown)?;
        let key_range = prefix_range(state, table, &pushdown);
        Ok(Self {
            key_range,
            table: table.id,
            filter,
            pushdown,
        })
    }

    /// Primary-key prefix implied by the predicate, cached with the binding.
    pub(crate) fn key_range(&self) -> Option<&scan::KeyRange> {
        self.key_range.as_ref()
    }

    /// The table this predicate was bound on.
    pub(crate) fn table(&self) -> u64 {
        self.table
    }

    /// True only when the predicate is true on `row`; NULL is false.
    #[cfg_attr(not(test), allow(dead_code, reason = "value-slice form for R3-306's native filter"))]
    pub(crate) fn accepts(&self, row: &[Value]) -> R<bool> {
        self.accepts_row(&SqlRow {
            values: row.to_vec(),
            version: 0,
        })
    }

    /// As [`RowPredicate::accepts`], on a stored row (keeps its version).
    pub(crate) fn accepts_row(&self, row: &SqlRow) -> R<bool> {
        let tuple = Tuple {
            source_versions: vec![],
            values: vec![row.values.clone()],
            version: row.version,
        };
        Ok(self.filter.eval(&tuple, &[], &mut 0)? == Some(true))
    }

    /// Top-level AND of `column op literal` (op one of `=`, `<`, `<=`, `>`,
    /// `>=`) with the literal already typed for the column, in the form
    /// `RowSource::index_scan` and `index_input` take. Every listed comparison
    /// is implied by the predicate, so a source may narrow its input with them;
    /// rows are still checked with `accepts`.
    pub(crate) fn pushdown(&self) -> &[(usize, &'static str, Value)] {
        &self.pushdown
    }
}

/// Reuse the planner's typed key bounds. Extended key types and collations
/// need their own encoding rules, so retain the full scan for those tables.
fn prefix_range(
    state: &Relational,
    table: &TableDef,
    pushdown: &[(usize, &'static str, Value)],
) -> Option<scan::KeyRange> {
    if table.primary_key.iter().any(|i| {
        let id = table.columns[*i].id;
        state.ext.column_collations.contains_key(&id)
            || state.ext.columns.get(&id).is_some_and(|c| c.type_id.is_some())
    }) {
        return None;
    }
    let mut predicates = Vec::new();
    for i in &table.primary_key {
        let Some((_, _, value)) = pushdown.iter().find(|(col, op, _)| col == i && *op == "=") else {
            break;
        };
        predicates.push(Predicate {
            column: table.columns[*i].name.clone(),
            op: "=".into(),
            value: Expr::Literal(value.clone()),
        });
    }
    if predicates.is_empty() { return None; }
    bounds(table, &predicates, &[]).ok().map(scan::KeyRange::from_bounds)
}

fn conjuncts(
    table: &TableDef,
    filter: &Filter,
    out: &mut Vec<(usize, &'static str, Value)>,
) -> R<()> {
    match filter {
        Filter::Logic(a, true, b) => {
            conjuncts(table, a, out)?;
            conjuncts(table, b, out)
        }
        Filter::Scalar(Scalar::Predicate(inner)) => conjuncts(table, inner, out),
        Filter::Compare(left, op, right) => {
            let (column, op, literal) = match (left, right) {
                (Scalar::Column(c), Scalar::Literal(v)) => (c, op_name(op), v),
                (Scalar::Literal(v), Scalar::Column(c)) => (c, flipped(op), v),
                _ => return Ok(()),
            };
            let Some(op) = op else { return Ok(()) };
            if column.qualifier.is_some() {
                return Ok(());
            }
            let offset = col(table, &column.name)?;
            if let Some(value) = typed(&table.columns[offset], literal)? {
                out.push((offset, op, value));
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn op_name(op: &str) -> Option<&'static str> {
    Some(match op {
        "=" => "=",
        "<" => "<",
        "<=" => "<=",
        ">" => ">",
        ">=" => ">=",
        _ => return None,
    })
}

fn flipped(op: &str) -> Option<&'static str> {
    Some(match op {
        "=" => "=",
        "<" => ">",
        "<=" => ">=",
        ">" => "<",
        ">=" => "<=",
        _ => return None,
    })
}

/// The literal typed for an index range on `column`, under the same conditions
/// the SELECT planner uses for its own `index_scan` predicates; `None` when the
/// comparison cannot be served exactly by the index.
fn typed(column: &ColumnDef, literal: &Value) -> R<Option<Value>> {
    let mut value = literal.clone();
    if let Some(typed) = unknown_comparison_literal(literal, &column.data_type)? {
        let Some(typed) = typed else { return Ok(None) };
        value = typed;
    }
    let indexable = matches!(
        column.data_type,
        DataType::Boolean
            | DataType::SmallInt
            | DataType::Integer
            | DataType::BigInt
            | DataType::Text
            | DataType::Varchar
            | DataType::Bytes
            | DataType::Numeric
            | DataType::Real
            | DataType::DoublePrecision
    );
    Ok((indexable
        && value != Value::Null
        && check(&value, column).is_ok()
        && !comparison_requires_filter(&value, &column.data_type)?)
    .then_some(value))
}

impl Prepared {
    /// A table read that runs through a `RowSource` (GD-0061 R3-305): task
    /// sessions route these through the scoped source on the buffered path.
    pub(crate) fn task_source_read(&self) -> bool {
        matches!(self.statement, Statement::Query(_) | Statement::Select { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> (Relational, TableDef) {
        let mut state = Relational::default();
        for sql in [
            "CREATE TABLE shipments(id INT PRIMARY KEY, merchant_id BIGINT, status TEXT, \
             attempt_count INT, note TEXT)",
            "CREATE SEQUENCE s",
        ] {
            let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
            state.execute(&prepared, &[], None).unwrap();
        }
        let table = state.tables["shipments"].clone();
        (state, table)
    }

    fn row(merchant: Value, status: &str) -> Vec<Value> {
        vec![
            Value::Int(1),
            merchant,
            Value::Text(status.into()),
            Value::Int(0),
            Value::Null,
        ]
    }

    #[test]
    fn key_prefix_bounds_match_planner_and_fall_back_safely() {
        let mut state = Relational::default();
        let sql = "CREATE TABLE items (merchant BIGINT, id INT, note TEXT, PRIMARY KEY (merchant, id))";
        let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
        state.execute(&prepared, &[], None).unwrap();
        let table = &state.tables["items"];
        for (text, values, full) in [
            ("merchant = 42", vec![Value::Int(42)], false),
            ("merchant = 42 AND id = 3", vec![Value::Int(42), Value::Int(3)], true),
            ("42 = merchant AND note = 'x'", vec![Value::Int(42)], false),
        ] {
            let p = RowPredicate::bind(&state, table, text).unwrap();
            let key = encode_key(&values).unwrap();
            assert_eq!(p.key_range(), Some(&scan::KeyRange {
                lo: std::ops::Bound::Included(key.clone()),
                hi: if full { std::ops::Bound::Included(key) } else {
                    std::ops::Bound::Excluded(successor(key).unwrap())
                },
            }));
        }
        for text in ["id = 3", "merchant = 4.5", "merchant = 42 OR id = 3", "NOT (merchant = 42)"] {
            assert!(RowPredicate::bind(&state, table, text).unwrap().key_range().is_none(), "{text}");
        }
    }

    #[test]
    fn accepts_true_only() {
        let (state, table) = state();
        let p = RowPredicate::bind(&state, &table, "merchant_id = 42 AND status <> 'done'").unwrap();
        assert_eq!(p.table(), table.id);
        assert!(p.accepts(&row(Value::Int(42), "failed")).unwrap());
        assert!(!p.accepts(&row(Value::Int(42), "done")).unwrap());
        assert!(!p.accepts(&row(Value::Int(7), "failed")).unwrap());
        // NULL is false, not an error.
        assert!(!p.accepts(&row(Value::Null, "failed")).unwrap());
    }

    #[test]
    fn bind_refusals() {
        let (state, table) = state();
        for text in [
            "merchant_id = $1",
            "merchant_id IN (SELECT 1)",
            "EXISTS (SELECT 1)",
            "random() > 0.5",
            "nextval('s') > 0",
            "md5(status) = 'x'",
            "now() > '2020-01-01'",
            "no_such_column = 1",
            "shipments.merchant_id = 42",
        ] {
            assert!(RowPredicate::bind(&state, &table, text).is_err(), "{text}");
        }
        // Allowlisted calls bind.
        RowPredicate::bind(&state, &table, "lower(status) = 'failed'").unwrap();
        RowPredicate::bind(&state, &table, "abs(attempt_count) < 3").unwrap();
    }

    #[test]
    fn pushdown_is_top_level_and_of_column_literal() {
        let (state, table) = state();
        let p = RowPredicate::bind(
            &state,
            &table,
            "merchant_id = 42 AND (status = 'failed' AND 3 > attempt_count)",
        )
        .unwrap();
        assert_eq!(
            p.pushdown(),
            &[
                (1, "=", Value::Int(42)),
                (2, "=", Value::Text("failed".into())),
                (3, "<", Value::Int(3)),
            ]
        );
        let between = RowPredicate::bind(&state, &table, "merchant_id BETWEEN 1 AND 9").unwrap();
        assert_eq!(
            between.pushdown(),
            &[(1, ">=", Value::Int(1)), (1, "<=", Value::Int(9))]
        );
        for text in [
            "merchant_id = 42 OR status = 'x'",
            "NOT (merchant_id = 42)",
            "merchant_id <> 42",
            "merchant_id + 1 = 43",
            "status IS NULL",
            "merchant_id = 4.5",
        ] {
            let p = RowPredicate::bind(&state, &table, text).unwrap();
            assert!(p.pushdown().is_empty(), "{text}: {:?}", p.pushdown());
        }
    }

    #[test]
    fn pushdown_implies_predicate() {
        let (state, table) = state();
        let p = RowPredicate::bind(&state, &table, "merchant_id = 42 AND status LIKE 'f%'").unwrap();
        assert_eq!(p.pushdown(), &[(1, "=", Value::Int(42))]);
        assert!(!p.accepts(&row(Value::Int(42), "queued")).unwrap());
        assert!(p.accepts(&row(Value::Int(42), "failed")).unwrap());
    }
}
