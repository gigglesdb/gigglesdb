//! Shared operation context and immutable snapshot execution. No publication authority.
use super::*;
use crate::relational::{Prepared, SqlResult};
use crate::storage::State;

pub(super) trait Context {
    type Input;
    fn max_read_rows(&self) -> usize { MAX_ROWS }
    fn coerce_column(&self, column: &ColumnDef, value: Value) -> Result<Value> {
        if matches!(column.data_type, DataType::Varchar | DataType::Bpchar) {
            return Err(Error::Unsupported);
        }
        assign_number(value, column)
    }
    fn checkpoint(&self) -> std::result::Result<(), CoreError>;
    fn resolve_table_engine(&mut self, name: &str) -> std::result::Result<TableDef, CoreError>;
    /// A view a read may name instead of a table; snapshot contexts have none.
    fn resolve_view_engine(&mut self, _name: &str) -> std::result::Result<Option<TableDef>, CoreError> {
        Ok(None)
    }
    fn consume_complete_engine(
        &mut self,
        input: Self::Input,
        table: &TableDef,
    ) -> std::result::Result<Vec<SqlRow>, CoreError>;
    fn session(&mut self) -> Result<&mut SqlSession> {
        Err(Error::Unsupported)
    }
    fn insert_engine(
        &mut self,
        table: &TableDef,
        values: Vec<Value>,
    ) -> std::result::Result<SqlResult, CoreError>;
    fn update_engine(
        &mut self,
        table: &TableDef,
        key: Vec<Value>,
        sets: Vec<(usize, Value)>,
        expected: u64,
    ) -> std::result::Result<SqlResult, CoreError>;
    fn delete_engine(
        &mut self,
        table: &TableDef,
        key: Vec<Value>,
        expected: u64,
    ) -> std::result::Result<SqlResult, CoreError>;
    fn row_version_engine(
        &mut self,
        table: &TableDef,
        key: &[Value],
    ) -> std::result::Result<u64, CoreError>;
    fn scan(&mut self, scan: Scan) -> std::result::Result<SqlResult, CoreError>;
}
impl Context for SqlSession {
    type Input = CompleteRelation;
    fn max_read_rows(&self) -> usize {
        if self.is_v2_engine() { usize::MAX } else { MAX_ROWS }
    }
    fn coerce_column(&self, column: &ColumnDef, value: Value) -> Result<Value> {
        self.coerce_column_engine(column, assign_number(value, column)?).map_err(core)
    }
    fn checkpoint(&self) -> std::result::Result<(), CoreError> {
        crate::cancellation::check()
    }
    fn session(&mut self) -> Result<&mut SqlSession> {
        Ok(self)
    }
    fn resolve_table_engine(&mut self, name: &str) -> std::result::Result<TableDef, CoreError> {
        SqlSession::native_table_engine(self, name)
    }
    fn resolve_view_engine(&mut self, name: &str) -> std::result::Result<Option<TableDef>, CoreError> {
        SqlSession::native_view_engine(self, name)
    }
    fn consume_complete_engine(
        &mut self,
        input: Self::Input,
        table: &TableDef,
    ) -> std::result::Result<Vec<SqlRow>, CoreError> {
        SqlSession::consume_complete_engine(self, input, table)
    }
    fn insert_engine(
        &mut self,
        table: &TableDef,
        values: Vec<Value>,
    ) -> std::result::Result<SqlResult, CoreError> {
        SqlSession::insert_engine(self, table, values)
    }
    fn update_engine(
        &mut self,
        table: &TableDef,
        key: Vec<Value>,
        sets: Vec<(usize, Value)>,
        expected: u64,
    ) -> std::result::Result<SqlResult, CoreError> {
        SqlSession::update_engine(self, table, key, sets, expected)
    }
    fn delete_engine(
        &mut self,
        table: &TableDef,
        key: Vec<Value>,
        expected: u64,
    ) -> std::result::Result<SqlResult, CoreError> {
        SqlSession::delete_engine(self, table, key, expected)
    }
    fn row_version_engine(
        &mut self,
        table: &TableDef,
        key: &[Value],
    ) -> std::result::Result<u64, CoreError> {
        SqlSession::row_version_engine(self, table, key)
    }
    fn scan(&mut self, scan: Scan) -> std::result::Result<SqlResult, CoreError> {
        SqlSession::scan(self, scan)
    }
}
struct SnapshotContext {
    state: State,
}
impl Context for SnapshotContext {
    type Input = Vec<SqlRow>;
    fn coerce_column(&self, column: &ColumnDef, value: Value) -> Result<Value> {
        crate::relational::coerce_to_column(&self.state.sql, column, assign_number(value, column)?)
            .map_err(|error| core(CoreError::Sql(error)))
    }
    fn checkpoint(&self) -> std::result::Result<(), CoreError> {
        Ok(())
    }
    fn resolve_table_engine(&mut self, name: &str) -> std::result::Result<TableDef, CoreError> {
        let key = self.state.sql.resolve_relation_key(name, &["public".into()]);
        self.state.sql.tables.get(&key).cloned()
            .ok_or(CoreError::Sql(SqlError::UnknownTable))
    }
    fn consume_complete_engine(
        &mut self,
        rows: Self::Input,
        table: &TableDef,
    ) -> std::result::Result<Vec<SqlRow>, CoreError> {
        if self.state.sql.tables.get(&self.state.sql.table_key(table)) != Some(table) {
            return Err(CoreError::Sql(SqlError::StaleSchema));
        }
        Ok(rows)
    }
    fn insert_engine(
        &mut self,
        _: &TableDef,
        _: Vec<Value>,
    ) -> std::result::Result<SqlResult, CoreError> {
        Err(CoreError::Unsupported)
    }
    fn delete_engine(
        &mut self,
        _: &TableDef,
        _: Vec<Value>,
        _: u64,
    ) -> std::result::Result<SqlResult, CoreError> {
        Err(CoreError::Unsupported)
    }
    fn update_engine(
        &mut self,
        table: &TableDef,
        key: Vec<Value>,
        sets: Vec<(usize, Value)>,
        expected: u64,
    ) -> std::result::Result<SqlResult, CoreError> {
        let prepared =
            Prepared::update_engine(table, key, sets, self.state.view.domain, &self.state.sql)
                .map_err(CoreError::Sql)?;
        self.state
            .sql
            .execute_with_cancel(&prepared, &[], Some(expected), || false)
            .map_err(CoreError::Sql)
    }
    fn row_version_engine(
        &mut self,
        table: &TableDef,
        key: &[Value],
    ) -> std::result::Result<u64, CoreError> {
        self.state
            .sql
            .row_version_engine(table, key)
            .map_err(CoreError::Sql)
    }
    fn scan(&mut self, scan: Scan) -> std::result::Result<SqlResult, CoreError> {
        let prepared = Prepared::scan(scan, self.state.view.domain, &self.state.sql)
            .map_err(CoreError::Sql)?;
        self.state
            .sql
            .execute(&prepared, &[], None)
            .map_err(CoreError::Sql)
    }
}

// Only equality conjunctions containing every primary-key field exactly once.
fn point_filter(expression: &Expr, table: &TableDef, seen: &mut BTreeSet<usize>) -> Result<()> {
    if table.hidden_rowid().is_some() {
        return Err(Error::Unsupported);
    }
    match expression {
        Expr::And(left, right) => {
            point_filter(left, table, seen)?;
            point_filter(right, table, seen)
        }
        Expr::Compare(left, Compare::Eq, right) => {
            let (Expr::Field(name), Expr::Value(ValueExpr::Literal(_))) = (&**left, &**right)
            else {
                return Err(Error::Unsupported);
            };
            let index = table
                .columns
                .iter()
                .position(|column| &column.name == name)
                .ok_or_else(|| Error::Bind("column".into()))?;
            if !table.primary_key.contains(&index) || !seen.insert(index) {
                return Err(Error::Unsupported);
            }
            Ok(())
        }
        _ => Err(Error::Unsupported),
    }
}
fn literal_guard(expression: &Expr) -> bool {
    match expression {
        Expr::Compare(left, _, right) => matches!(
            (&**left, &**right),
            (Expr::Field(_), Expr::Value(ValueExpr::Literal(_)))
        ),
        Expr::And(left, right) => literal_guard(left) && literal_guard(right),
        _ => false,
    }
}

/// Bind and execute against one borrowed snapshot. The returned clone is unpublished;
/// every failure leaves the caller's snapshot untouched. Admission/authority and
/// immutable replay records remain the application owner's responsibility.
#[allow(dead_code)]
pub(crate) fn execute(
    state: &State,
    request: &Request,
    bindings: &Bindings,
) -> Result<(State, Outcome)> {
    execute_inner(state, request, bindings, None)
}

#[allow(dead_code)]
pub(crate) fn execute_fenced(
    state: &State,
    request: &Request,
    bindings: &Bindings,
    description: &BoundDescription,
    expected_version: Option<u64>,
) -> Result<(State, Outcome)> {
    execute_inner(
        state,
        request,
        bindings,
        Some((description, expected_version)),
    )
}
/// Check the snapshot fence and request shape before binding any values.
pub(crate) fn admit_fenced(
    state: &State,
    request: &Request,
    bindings: &Bindings,
    description: &BoundDescription,
) -> Result<()> {
    admit_inner(state, request, bindings, Some(description))
}
fn admit_inner(
    state: &State,
    request: &Request,
    bindings: &Bindings,
    description: Option<&BoundDescription>,
) -> Result<()> {
    if let Some(description) = description
        && (description.contract_version != ENGINE_CONTRACT_VERSION
            || state.view.domain != description.database_domain
            || state.sql.epoch != description.catalog_epoch
            || state
                .sql
                .tables
                .get(&state.sql.table_key(&description.table))
                != Some(&description.table)
            || state.sql.resolve_relation_key(&native_relation_key(request.table()?), &["public".into()])
                != state.sql.table_key(&description.table))
    {
        return Err(core(CoreError::Sql(SqlError::StaleSchema)));
    }
    validate_request(request)?;
    validate_bindings(bindings)?;
    let filter = match request {
        Request::Read {
            filter: Some(filter),
            order,
            limit,
            ..
        } if order.is_empty() && matches!(limit, None | Some(ValueExpr::Literal(_))) => filter,
        Request::Update {
            filter,
            guard: Some(guard),
            values,
            intent: Intent::One,
            ..
        } if literal_guard(guard)
            && values
                .iter()
                .all(|(_, value)| matches!(value, ValueExpr::Literal(_))) =>
        {
            filter
        }
        _ => return Err(Error::Unsupported),
    };
    let key = state.sql.resolve_relation_key(&native_relation_key(request.table()?), &["public".into()]);
    let table = state.sql.tables.get(&key)
        .ok_or_else(|| core(CoreError::Sql(SqlError::UnknownTable)))?;
    let mut seen = BTreeSet::new();
    point_filter(filter, table, &mut seen)?;
    if seen.len() != table.primary_key.len() {
        return Err(Error::Unsupported);
    }
    Ok(())
}
fn execute_inner(
    state: &State,
    request: &Request,
    bindings: &Bindings,
    fence: Option<(&BoundDescription, Option<u64>)>,
) -> Result<(State, Outcome)> {
    admit_inner(state, request, bindings, fence.map(|(description, _)| description))?;
    let mut context = SnapshotContext {
        state: state.clone(),
    };
    let (table, operation) = bind_request(&mut context, request, bindings)?;
    operation.check_input_cap()?;
    // Preserve the existing engine's full-relation cap, including N+1 proof.
    let rows = context
        .scan(Scan {
            table: state.sql.table_key(&table),
            projection: vec![],
            prefix: vec![],
            lower: None,
            upper: None,
            reverse: false,
            limit: MAX_ROWS,
        })
        .map_err(core)?
        .rows;
    if rows.len() == MAX_ROWS {
        let maximum = context
            .scan(Scan {
                table: state.sql.table_key(&table),
                projection: table
                    .primary_key
                    .iter()
                    .map(|i| table.columns[*i].name.clone())
                    .collect(),
                prefix: vec![],
                lower: None,
                upper: None,
                reverse: true,
                limit: 1,
            })
            .map_err(core)?;
        let last_key: Vec<_> = table
            .primary_key
            .iter()
            .map(|i| rows[MAX_ROWS - 1].values[*i].clone())
            .collect();
        if maximum.rows.len() != 1 || maximum.rows[0].values != last_key {
            return Err(Error::Limit);
        }
    }
    if let Some((description, expected)) = fence {
        if operation.columns(&table) != description.columns {
            return Err(core(CoreError::Sql(SqlError::StaleSchema)));
        }
        match (&operation, expected) {
            (Operation::Read { .. }, None) => {}
            (Operation::Mutate { filter, .. }, Some(expected)) if expected != 0 => {
                let matched = matching_rows(&context, rows.clone(), filter)?;
                if matched.len() != 1 || matched[0].version != expected {
                    return Err(Error::Conflict);
                }
            }
            _ => return Err(Error::Unsupported),
        }
    }
    let outcome = run(&mut context, &table, operation, rows, None)?;
    Ok((context.state, outcome))
}
