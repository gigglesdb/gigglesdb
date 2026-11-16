//! One-request blocks over the common bounded operation engine.
use super::*;

pub const MAX_TRANSACTION_ITEMS: usize = 64;
pub const MAX_TRANSACTION_LOCALS: usize = 64;
pub const MAX_EMIT_VALUES: usize = 64;
pub const MAX_RETAINED_LOCAL_BYTES: usize = crate::relational::MAX_RESULT_BYTES;
pub const MAX_EMIT_BYTES: usize = crate::relational::MAX_RESULT_BYTES;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LocalRef {
    Field { local: String, field: String },
    Status { local: String },
    Changed { local: String },
    Count { local: String },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transaction {
    pub read_only: bool,
    pub steps: Vec<TransactionStep>,
    pub emit: Option<Vec<EmitExpr>>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionStep {
    Execute(Request),
    Let { name: String, request: Request },
    Require(Expr),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmitExpr {
    Scalar(Expr),
    Row(String),
    Rows(String),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepSummary {
    pub item_index: usize,
    pub matched: usize,
    pub affected: usize,
    pub changed: usize,
    pub complete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict_results: Option<Vec<ConflictTupleOutcome>>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmittedValue {
    Scalar(Value),
    Row {
        columns: Vec<ColumnDef>,
        row: SqlRow,
    },
    Rows {
        columns: Vec<ColumnDef>,
        rows: Vec<SqlRow>,
        complete: bool,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionOutcome {
    pub summaries: Vec<StepSummary>,
    pub emitted: Vec<EmittedValue>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EmitDescription {
    /// None describes an untyped NULL, not a persisted scalar type.
    Scalar {
        data_type: Option<DataType>,
        nullable: bool,
    },
    Row {
        columns: Vec<ColumnDef>,
    },
    Rows {
        columns: Vec<ColumnDef>,
    },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundTransactionDescription {
    pub contract_version: u32,
    pub database_domain: u128,
    pub catalog_epoch: u64,
    pub read_only: bool,
    pub tables: Vec<TableDef>,
    pub emitted: Vec<EmitDescription>,
}
/// An immutable process-local plan. It contains frozen values, not authority.
#[derive(Clone)]
pub struct BoundTransaction {
    description: BoundTransactionDescription,
    steps: Vec<BoundStep>,
    emit: Vec<BoundEmit>,
    locals: Vec<LocalShape>,
    remaining_value_bytes: usize,
}
impl BoundTransaction {
    pub fn description(&self) -> &BoundTransactionDescription {
        &self.description
    }
}
impl std::fmt::Debug for BoundTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundTransaction")
            .field("description", &self.description)
            .finish_non_exhaustive()
    }
}
#[derive(Clone)]
enum BoundStep {
    Execute {
        table: usize,
        operation: Operation,
        retain: bool,
    },
    Require(Bound),
}
#[derive(Clone)]
enum BoundEmit {
    Scalar(Bound),
    Row(usize),
    Rows(usize),
}
#[derive(Clone, Copy)]
enum LocalKind {
    Read,
    Write { singular: bool },
}
#[derive(Clone)]
pub(super) struct LocalShape {
    name: String,
    kind: LocalKind,
    columns: Vec<ColumnDef>,
}
#[derive(Clone)]
enum LocalMember {
    Field(usize),
    Status { singular: bool },
    Changed,
    Count,
}
#[derive(Clone)]
pub(super) struct BoundLocal {
    slot: usize,
    member: LocalMember,
    data_type: DataType,
    pub(super) nullable: bool,
}
impl BoundLocal {
    pub(super) fn ty(&self) -> Ty {
        match self.data_type {
            DataType::Boolean => Ty::Bool,
            DataType::SmallInt | DataType::Integer | DataType::BigInt => Ty::Int,
            DataType::RegType
            | DataType::Uuid
            | DataType::Jsonb
            | DataType::IntArray
            | DataType::BigIntArray
            | DataType::TextArray
            | DataType::Text
            | DataType::Varchar => Ty::Text,
            DataType::Bpchar => Ty::Bpchar,
            DataType::Bytes => Ty::Bytes,
            DataType::Numeric => Ty::Numeric,
            DataType::Real => Ty::Real,
            DataType::DoublePrecision => Ty::DoublePrecision,
            DataType::Date => Ty::Date,
            DataType::Time => Ty::Time,
            DataType::TimeTz => Ty::TimeTz,
            DataType::Timestamp => Ty::Timestamp,
            DataType::TimestampTz => Ty::TimestampTz,
            DataType::Interval => Ty::Interval,
        }
    }
    fn value<'a>(&self, locals: &'a [Outcome]) -> Result<Cow<'a, Value>> {
        let local = locals
            .get(self.slot)
            .ok_or_else(|| Error::Bind("local slot".into()))?;
        Ok(match self.member {
            LocalMember::Field(index) => Cow::Borrowed(
                local
                    .rows
                    .first()
                    .and_then(|row| row.values.get(index))
                    .ok_or_else(|| Error::Bind("local row".into()))?,
            ),
            LocalMember::Status { singular } => {
                let skipped = singular
                    && local
                        .conflict_results
                        .as_ref()
                        .and_then(|results| results.first())
                        .is_some_and(|result| result.status == ConflictStatus::Skipped);
                Cow::Owned(Value::Text(
                    if skipped { "skipped" } else { "success" }.into(),
                ))
            }
            LocalMember::Changed => Cow::Owned(Value::Int(local.changed as i64)),
            LocalMember::Count => Cow::Owned(Value::Int(local.rows.len() as i64)),
        })
    }
}
fn local_slot(locals: &[LocalShape], name: &str) -> Result<usize> {
    if !identifier(name) {
        return Err(Error::Bind("local name".into()));
    }
    locals
        .iter()
        .position(|local| local.name == name)
        .ok_or_else(|| Error::Bind("local missing".into()))
}
pub(super) fn bind_local(locals: &[LocalShape], reference: &LocalRef) -> Result<BoundLocal> {
    let name = match reference {
        LocalRef::Field { local, .. }
        | LocalRef::Status { local }
        | LocalRef::Changed { local }
        | LocalRef::Count { local } => local,
    };
    let slot = local_slot(locals, name)?;
    let local = &locals[slot];
    let (member, data_type, nullable) = match reference {
        LocalRef::Field { field, .. } => {
            if !matches!(local.kind, LocalKind::Write { singular: true }) || !identifier(field) {
                return Err(Error::Bind("local row".into()));
            }
            let index = local
                .columns
                .iter()
                .position(|column| column.name == *field)
                .ok_or_else(|| Error::Bind("local column".into()))?;
            let column = &local.columns[index];
            (
                LocalMember::Field(index),
                column.data_type.clone(),
                !column.not_null,
            )
        }
        LocalRef::Status { .. } if matches!(local.kind, LocalKind::Write { .. }) => (
            LocalMember::Status {
                singular: matches!(local.kind, LocalKind::Write { singular: true }),
            },
            DataType::Text,
            false,
        ),
        LocalRef::Changed { .. } if matches!(local.kind, LocalKind::Write { .. }) => {
            (LocalMember::Changed, DataType::BigInt, false)
        }
        LocalRef::Count { .. } if matches!(local.kind, LocalKind::Read) => {
            (LocalMember::Count, DataType::BigInt, false)
        }
        _ => return Err(Error::Bind("local shape".into())),
    };
    Ok(BoundLocal {
        slot,
        member,
        data_type,
        nullable,
    })
}
fn request_kind(request: &Request) -> Result<LocalKind> {
    let (request, _) = request.split_returning()?;
    Ok(match request {
        Request::Read { .. } | Request::ReadExpressions { .. } => LocalKind::Read,
        Request::InsertMany { .. } => LocalKind::Write { singular: false },
        Request::Insert { .. } | Request::InsertExpressions { .. } => {
            LocalKind::Write { singular: true }
        }
        Request::Update { intent, .. } | Request::UpdateExpressions { intent, .. } => {
            if matches!(intent, Intent::Claim { .. }) {
                return Err(Error::Unsupported);
            }
            LocalKind::Write {
                singular: matches!(intent, Intent::One),
            }
        }
        Request::Delete { cap, .. } => LocalKind::Write {
            singular: cap.is_none(),
        },
        Request::Returning { .. } | Request::OnConflict { .. } => return Err(Error::Unsupported),
    })
}
/// See `Admission`. Outside Base, each INSERT MANY step has its own
/// MAX_INSERT_REQUEST_BYTES bound and the rest of the block keeps MAX_INPUT;
/// Base blocks keep MAX_INPUT as a whole.
pub(super) fn validate_transaction(transaction: &Transaction, admission: Admission) -> Result<()> {
    let v2 = admission == Admission::V2;
    let count = transaction
        .steps
        .len()
        .checked_add(usize::from(transaction.emit.is_some()))
        .ok_or(Error::Limit)?;
    if count > MAX_TRANSACTION_ITEMS {
        return Err(Error::Limit);
    }
    if count == 0 {
        return Err(Error::Bind("transaction items".into()));
    }
    if let Some(emit) = &transaction.emit {
        if emit.is_empty() {
            return Err(Error::Bind("emit".into()));
        }
        if emit.len() > MAX_EMIT_VALUES {
            return Err(Error::Limit);
        }
    }
    let mut nodes = 0;
    for step in &transaction.steps {
        match step {
            TransactionStep::Execute(request) | TransactionStep::Let { request, .. } => {
                let kind = request_kind(request)?;
                if transaction.read_only && !matches!(kind, LocalKind::Read) {
                    return Err(Error::Unsupported);
                }
                validate_request_nodes(request, &mut nodes, v2)?;
                if !relation_identifier(request.table()?) {
                    return Err(Error::Bind("identifier".into()));
                }
            }
            TransactionStep::Require(expression) => {
                validate_expression_nodes(vec![(expression, 0)], &mut nodes)?
            }
        }
    }
    if let Some(emit) = &transaction.emit {
        for item in emit {
            if let EmitExpr::Scalar(expression) = item {
                validate_expression_nodes(vec![(expression, 0)], &mut nodes)?;
            }
        }
    }
    if admission == Admission::Base {
        return validate_size(transaction);
    }
    // Each INSERT MANY step has a single request's tree bound (SQL bounds
    // each statement the same way); its bytes do not raise other steps' bound.
    let mut inserts = 0usize;
    for step in &transaction.steps {
        if let TransactionStep::Execute(request) | TransactionStep::Let { request, .. } = step
            && matches!(request.parts()?.body, Request::InsertMany { .. })
        {
            inserts = inserts.saturating_add(validate_size_within(step, MAX_INSERT_REQUEST_BYTES)?);
        }
    }
    validate_size_within(transaction, MAX_INPUT.saturating_add(inserts)).map(drop)
}
fn scalar_description(bound: &Bound, ty: Option<Ty>) -> EmitDescription {
    let data_type = match bound {
        Bound::Local(local) => Some(local.data_type.clone()),
        _ => ty.map(|ty| match ty {
            Ty::Bool => DataType::Boolean,
            Ty::Int => DataType::BigInt,
            Ty::Text => DataType::Text,
            Ty::Bpchar => DataType::Bpchar,
            Ty::Bytes => DataType::Bytes,
            Ty::Numeric => DataType::Numeric,
            Ty::Real => DataType::Real,
            Ty::DoublePrecision => DataType::DoublePrecision,
            Ty::Date => DataType::Date,
            Ty::Time => DataType::Time,
            Ty::TimeTz => DataType::TimeTz,
            Ty::Timestamp => DataType::Timestamp,
            Ty::TimestampTz => DataType::TimestampTz,
            Ty::Interval => DataType::Interval,
        }),
    };
    EmitDescription::Scalar {
        data_type,
        nullable: bound.nullable(None),
    }
}
pub(super) fn bind_transaction(
    session: &mut SqlSession,
    transaction: &Transaction,
    bindings: &Bindings,
) -> Result<BoundTransaction> {
    let (database_domain, catalog_epoch) = session.snapshot_identity_engine().map_err(core)?;
    let mut tables = Vec::new();
    let mut table_indices = BTreeMap::new();
    let mut locals: Vec<LocalShape> = Vec::new();
    let mut steps = Vec::new();
    let mut used = BTreeSet::new();
    let mut remaining_value_bytes = MAX_BOUND_VALUE_BYTES;
    for step in &transaction.steps {
        crate::cancellation::check().map_err(core)?;
        let bound_table;
        let mut binder = Binder {
            table: None,
            allow_fields: false,
            allow_conflict_fields: false,
            locals: Some(&locals),
            bindings,
            used,
            remaining_value_bytes,
            row_limit: MAX_ROWS,
        };
        let bound = match step {
            TransactionStep::Require(expression) => {
                BoundStep::Require(binder.predicate(expression)?)
            }
            TransactionStep::Execute(request) | TransactionStep::Let { request, .. } => {
                let name = request.table()?;
                bound_table = resolve_relation(session, &native_relation_key(name), request)?;
                let table = &bound_table;
                if table.hidden_rowid().is_some() {
                    return Err(Error::Unsupported);
                }
                binder.table = Some(table);
                binder.allow_fields = true;
                let operation = binder.bind(request, session.max_read_rows())?;
                let table_index = *table_indices.entry(table.id).or_insert_with(|| {
                    tables.push(table.clone());
                    tables.len() - 1
                });
                BoundStep::Execute {
                    table: table_index,
                    operation,
                    retain: matches!(step, TransactionStep::Let { .. }),
                }
            }
        };
        used = binder.used;
        remaining_value_bytes = binder.remaining_value_bytes;
        if let TransactionStep::Let { name, request } = step {
            if !identifier(name) {
                return Err(Error::Bind("local name".into()));
            }
            if locals.iter().any(|local| local.name == *name) {
                return Err(Error::Bind("duplicate local".into()));
            }
            if locals.len() == MAX_TRANSACTION_LOCALS {
                return Err(Error::Limit);
            }
            if let BoundStep::Execute {
                table, operation, ..
            } = &bound
            {
                locals.push(LocalShape {
                    name: name.clone(),
                    kind: request_kind(request)?,
                    columns: operation.columns(&tables[*table]),
                });
            }
        }
        steps.push(bound);
    }
    let mut emit = Vec::new();
    let mut descriptions = Vec::new();
    let mut binder = Binder {
        table: None,
        allow_fields: false,
        allow_conflict_fields: false,
        locals: Some(&locals),
        bindings,
        used,
        remaining_value_bytes,
        row_limit: MAX_ROWS,
    };
    if let Some(items) = &transaction.emit {
        for item in items {
            match item {
                EmitExpr::Scalar(expression) => {
                    let (expression, ty) = binder.expr(expression, 0)?;
                    descriptions.push(scalar_description(&expression, ty));
                    emit.push(BoundEmit::Scalar(expression));
                }
                EmitExpr::Row(name) | EmitExpr::Rows(name) => {
                    let slot = local_slot(&locals, name)?;
                    let local = &locals[slot];
                    let columns = local.columns.clone();
                    if matches!(item, EmitExpr::Row(_)) {
                        if !matches!(local.kind, LocalKind::Write { singular: true })
                            || columns.is_empty()
                        {
                            return Err(Error::Bind("local row".into()));
                        }
                        emit.push(BoundEmit::Row(slot));
                        descriptions.push(EmitDescription::Row { columns });
                    } else {
                        if !matches!(local.kind, LocalKind::Read) {
                            return Err(Error::Bind("local rows".into()));
                        }
                        emit.push(BoundEmit::Rows(slot));
                        descriptions.push(EmitDescription::Rows { columns });
                    }
                }
            }
        }
    }
    if binder.used.len() != bindings.len() {
        return Err(Error::Bind("parameters".into()));
    }
    remaining_value_bytes = binder.remaining_value_bytes;
    Ok(BoundTransaction {
        description: BoundTransactionDescription {
            contract_version: ENGINE_CONTRACT_VERSION,
            database_domain,
            catalog_epoch,
            read_only: transaction.read_only,
            tables,
            emitted: descriptions,
        },
        steps,
        emit,
        locals,
        remaining_value_bytes,
    })
}
impl Engine {
    pub fn execute_transaction(
        &self,
        transaction: &Transaction,
        bindings: &Bindings,
    ) -> Result<TransactionOutcome> {
        validate_transaction(transaction, Admission::engine(self.admin.engine() == crate::Engine::V2))?;
        validate_bindings(bindings)?;
        self.execute_transaction_plan(|session| bind_transaction(session, transaction, bindings))
    }
    pub fn prepare_transaction_bound(
        &self,
        transaction: &Transaction,
        bindings: &Bindings,
    ) -> Result<BoundTransaction> {
        validate_transaction(transaction, Admission::engine(self.admin.engine() == crate::Engine::V2))?;
        validate_bindings(bindings)?;
        let mut session = self.admin.sql_session();
        session.begin_engine().map_err(core)?;
        let result = bind_transaction(&mut session, transaction, bindings);
        session.rollback();
        result
    }
    pub fn execute_transaction_bound(
        &self,
        prepared: &BoundTransaction,
    ) -> Result<TransactionOutcome> {
        self.execute_transaction_plan(|session| {
            let description = &prepared.description;
            let (domain, epoch) = session.snapshot_identity_engine().map_err(core)?;
            let (_, tables) = session.catalog_engine().map_err(core)?;
            let mut stale = description.contract_version != ENGINE_CONTRACT_VERSION
                || domain != description.database_domain
                || epoch != description.catalog_epoch;
            for expected in &description.tables {
                // Only reads bind a view; its descriptor must still be current.
                stale = stale || (!tables.iter().any(|table| table == expected)
                    && !(expected.primary_key.is_empty()
                        && session.native_view_engine_current(expected).map_err(core)?));
            }
            if stale {
                return Err(core(CoreError::Sql(SqlError::StaleSchema)));
            }
            Ok(prepared.clone())
        })
    }
    fn execute_transaction_plan(
        &self,
        bind: impl FnOnce(&mut SqlSession) -> Result<BoundTransaction>,
    ) -> Result<TransactionOutcome> {
        let mut session = self.admin.sql_session();
        session.begin_engine().map_err(core)?;
        let staged = (|| {
            let prepared = bind(&mut session)?;
            let result = run_transaction(&mut session, &prepared)?;
            crate::cancellation::check().map_err(core)?;
            Ok((result, prepared.description.read_only))
        })();
        let (result, read_only) = match staged {
            Ok(result) => result,
            Err(error) => {
                session.rollback();
                return Err(error);
            }
        };
        if read_only {
            session.rollback();
            return Ok(result);
        }
        match session.commit_engine() {
            Ok(()) => Ok(result),
            Err(error) => {
                session.rollback();
                Err(match error {
                    CoreError::Storage(_) => Error::Unknown,
                    other => core(other),
                })
            }
        }
    }
}
impl Bound {
    fn resolve_locals(&mut self, locals: &[Outcome], remaining: &mut usize) -> Result<()> {
        match self {
            Self::Local(local) => {
                let value = local.value(locals)?;
                charge_value(remaining, &value)?;
                *self = Self::Value(value.into_owned());
            }
            Self::Case {
                operand,
                arms,
                else_expr,
            } => {
                // Admission visits all explicit occurrences before execution,
                // including CASE branches whose scalar computation is skipped.
                if let Some(expression) = operand {
                    expression.resolve_locals(locals, remaining)?;
                }
                for (when, then) in arms {
                    when.resolve_locals(locals, remaining)?;
                    then.resolve_locals(locals, remaining)?;
                }
                if let Some(expression) = else_expr {
                    expression.resolve_locals(locals, remaining)?;
                }
            }
            Self::Call { arguments, .. } => {
                // Local occurrence admission is eager even when COALESCE's
                // scalar evaluation will skip an argument.
                for argument in arguments {
                    argument.resolve_locals(locals, remaining)?;
                }
            }
            Self::Compare(a, _, b)
            | Self::Arithmetic(a, _, b)
            | Self::And(a, b)
            | Self::Or(a, b) => {
                a.resolve_locals(locals, remaining)?;
                b.resolve_locals(locals, remaining)?;
            }
            Self::IsNull(expression, _) | Self::Not(expression) | Self::Negate(expression) => {
                expression.resolve_locals(locals, remaining)?
            }
            Self::Value(_) | Self::Field(_) | Self::ConflictField { .. } => (),
        }
        Ok(())
    }
}
impl Operation {
    fn resolve_locals(&mut self, locals: &[Outcome], remaining: &mut usize) -> Result<()> {
        let returning = match self {
            Self::Upsert {
                rows,
                action,
                returning,
                ..
            } => {
                for row in rows {
                    for value in row {
                        value.resolve_locals(locals, remaining)?;
                    }
                }
                if let BoundConflict::Update { guard, values } = action {
                    guard.resolve_locals(locals, remaining)?;
                    for (_, value) in values {
                        value.resolve_locals(locals, remaining)?;
                    }
                }
                returning
            }
            Self::Read { filter, .. } => {
                filter.resolve_locals(locals, remaining)?;
                return Ok(());
            }
            Self::ReadExpressions {
                filter, projection, ..
            } => {
                filter.resolve_locals(locals, remaining)?;
                for item in projection {
                    item.expression.resolve_locals(locals, remaining)?;
                }
                return Ok(());
            }
            Self::InsertMany {
                rows, returning, ..
            } => {
                for row in rows {
                    for value in row {
                        value.resolve_locals(locals, remaining)?;
                    }
                }
                returning
            }
            Self::Insert { returning, .. } => returning,
            Self::InsertExpressions { values, returning } => {
                for value in values {
                    value.resolve_locals(locals, remaining)?;
                }
                returning
            }
            Self::Mutate {
                filter,
                guard,
                sets,
                returning,
                ..
            } => {
                filter.resolve_locals(locals, remaining)?;
                guard.resolve_locals(locals, remaining)?;
                if let Some(Assignments::Expressions(values)) = sets {
                    for (_, value) in values {
                        value.resolve_locals(locals, remaining)?;
                    }
                }
                returning
            }
        };
        if let Some(returning) = returning {
            for projection in returning {
                projection.expression.resolve_locals(locals, remaining)?;
            }
        }
        Ok(())
    }
}
pub(super) fn run_transaction(
    session: &mut SqlSession,
    plan: &BoundTransaction,
) -> Result<TransactionOutcome> {
    let mut locals = Vec::new();
    let mut summaries = Vec::new();
    let mut remaining_values = plan.remaining_value_bytes;
    let mut retained = MAX_RETAINED_LOCAL_BYTES;
    for (item_index, step) in plan.steps.iter().enumerate() {
        #[cfg(test)]
        tests::before_step(item_index);
        #[cfg(all(test, feature = "engine-rocksdb"))]
        block_boundary_tests::before_step(item_index);
        crate::cancellation::check().map_err(core)?;
        session.begin_native_step().map_err(core)?;
        match step {
            BoundStep::Require(expression) => {
                let mut expression = expression.clone();
                expression.resolve_locals(&locals, &mut remaining_values)?;
                if *expression.eval(&[])? != Value::Bool(true) {
                    return Err(Error::RequirementFailed);
                }
            }
            BoundStep::Execute {
                table,
                operation,
                retain,
            } => {
                operation.check_input_cap()?;
                let mut operation = operation.clone();
                operation.resolve_locals(&locals, &mut remaining_values)?;
                let table = &plan.description.tables[*table];
                let outcome = run_complete(
                    session,
                    table,
                    operation,
                    if *retain { Some(&mut retained) } else { None },
                )?;
                summaries.push(StepSummary {
                    item_index,
                    matched: outcome.matched,
                    affected: outcome.affected,
                    changed: outcome.changed,
                    complete: outcome.complete,
                    conflict_results: outcome.conflict_results.clone(),
                });
                if *retain {
                    locals.push(outcome);
                }
            }
        }
    }
    // Resolve scalar local leaves before evaluating emit, exactly once per
    // occurrence. Row/rows emissions have the separate result payload budget.
    let mut emit = plan.emit.clone();
    for item in &mut emit {
        if let BoundEmit::Scalar(expression) = item {
            expression.resolve_locals(&locals, &mut remaining_values)?;
        }
    }
    enum Pending<'a> {
        Scalar(Cow<'a, Value>),
        Row(usize, &'a SqlRow),
        Rows(usize, &'a Outcome),
    }
    let mut remaining = MAX_EMIT_BYTES;
    let mut pending = Vec::new();
    for item in &emit {
        crate::cancellation::check().map_err(core)?;
        pending.push(match item {
            BoundEmit::Scalar(expression) => {
                let value = expression.eval(&[])?;
                charge_value(&mut remaining, &value)?;
                Pending::Scalar(value)
            }
            BoundEmit::Row(slot) => {
                let row = locals[*slot]
                    .rows
                    .first()
                    .ok_or_else(|| Error::Bind("local row".into()))?;
                for value in &row.values {
                    charge_value(&mut remaining, value)?;
                }
                Pending::Row(*slot, row)
            }
            BoundEmit::Rows(slot) => {
                let local = &locals[*slot];
                for row in &local.rows {
                    for value in &row.values {
                        charge_value(&mut remaining, value)?;
                    }
                }
                Pending::Rows(*slot, local)
            }
        });
    }
    // No output payload is copied until all emitted occurrences fit.
    let emitted = pending
        .into_iter()
        .map(|item| match item {
            Pending::Scalar(value) => EmittedValue::Scalar(value.into_owned()),
            Pending::Row(slot, row) => EmittedValue::Row {
                columns: plan.locals[slot].columns.clone(),
                row: row.clone(),
            },
            Pending::Rows(slot, local) => EmittedValue::Rows {
                columns: plan.locals[slot].columns.clone(),
                rows: local.rows.clone(),
                complete: local.complete,
            },
        })
        .collect();
    Ok(TransactionOutcome { summaries, emitted })
}

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
mod block_boundary_tests {
    use super::*;
    use std::cell::RefCell;
    type Hook = Box<dyn FnMut(usize)>;
    thread_local! { static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) }; }
    pub(super) fn before_step(index: usize) {
        HOOK.with_borrow_mut(|hook| {
            if let Some(hook) = hook {
                hook(index);
            }
        });
    }
    fn hooked<T>(hook: impl FnMut(usize) + 'static, run: impl FnOnce() -> T) -> T {
        struct Clear;
        impl Drop for Clear {
            fn drop(&mut self) {
                HOOK.with_borrow_mut(|hook| *hook = None);
            }
        }
        HOOK.with_borrow_mut(|slot| *slot = Some(Box::new(hook)));
        let _clear = Clear;
        run()
    }
    fn fixture(engine: crate::Engine) -> (std::path::PathBuf, Admin) {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!(
            "block-boundary-{:032x}",
            u128::from_le_bytes(nonce)
        ));
        let admin = Admin::create_engine(&path, crate::Limits::default(), engine).unwrap();
        admin
            .sql_session()
            .execute(
                "CREATE TABLE items(id BIGINT PRIMARY KEY, value BIGINT)",
                &[],
                None,
            )
            .unwrap();
        (path, admin)
    }
    fn read() -> Request {
        Request::Read {
            table: "items".into(),
            filter: None,
            order: vec![],
            limit: Some(ValueExpr::Literal(Value::Int(10))),
            projection: vec!["value".into()],
        }
    }
    fn insert(id: i64) -> TransactionStep {
        TransactionStep::Execute(Request::Insert {
            table: "items".into(),
            values: vec![
                ("id".into(), ValueExpr::Literal(Value::Int(id))),
                ("value".into(), ValueExpr::Literal(Value::Int(7))),
            ],
        })
    }
    fn block(steps: Vec<TransactionStep>) -> Transaction {
        Transaction {
            read_only: false,
            steps,
            emit: None,
        }
    }
    fn rows(admin: &Admin) -> Vec<SqlRow> {
        admin
            .sql_session()
            .execute("SELECT * FROM items ORDER BY id", &[], None)
            .unwrap()
            .rows
    }
    #[test]
    fn rc_steps_refresh_only_v2_and_keep_earlier_results() {
        for engine in [crate::Engine::Legacy, crate::Engine::V2] {
            let (path, admin) = fixture(engine);
            admin
                .sql_session()
                .execute("INSERT INTO items VALUES(1,0)", &[], None)
                .unwrap();
            let outside = admin.clone();
            let plan = Transaction {
                read_only: true,
                steps: vec![
                    TransactionStep::Let {
                        name: "first".into(),
                        request: read(),
                    },
                    TransactionStep::Let {
                        name: "second".into(),
                        request: read(),
                    },
                ],
                emit: Some(vec![
                    EmitExpr::Rows("first".into()),
                    EmitExpr::Rows("second".into()),
                ]),
            };
            let result = hooked(
                move |index| {
                    if index == 1 {
                        std::thread::scope(|scope| {
                            scope
                                .spawn(|| {
                                    outside
                                        .sql_session()
                                        .execute("UPDATE items SET value=1 WHERE id=1", &[], None)
                                        .unwrap();
                                })
                                .join()
                                .unwrap()
                        });
                    }
                },
                || Engine::new(admin.clone()).execute_transaction(&plan, &Bindings::new()),
            )
            .unwrap();
            for (index, value) in result.emitted.iter().enumerate() {
                let EmittedValue::Rows { rows, .. } = value else {
                    panic!("rows");
                };
                let expected = if engine == crate::Engine::V2 && index == 1 {
                    1
                } else {
                    0
                };
                assert_eq!(rows[0].values, vec![Value::Int(expected)]);
            }
            drop(admin);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
    #[test]
    fn own_writes_require_failure_and_outer_sql_commit_or_rollback() {
        let (path, admin) = fixture(crate::Engine::V2);
        let mut session = admin.sql_session();
        for commit in [false, true] {
            session.begin_native().unwrap();
            session
                .execute("INSERT INTO items VALUES(1,3)", &[], None)
                .unwrap();
            let mut plan = block(vec![
                insert(2),
                TransactionStep::Let {
                    name: "saved".into(),
                    request: read(),
                },
            ]);
            plan.emit = Some(vec![EmitExpr::Rows("saved".into())]);
            let result = session
                .execute_native_transaction(&plan, &Bindings::new())
                .unwrap();
            let EmittedValue::Rows { rows, .. } = &result.emitted[0] else {
                panic!("rows");
            };
            assert_eq!(rows.len(), 2);
            assert_eq!(result.summaries[0].affected, 1);
            assert!(self::rows(&admin).is_empty());
            if commit {
                session.commit_native().unwrap();
            } else {
                session.rollback_native().unwrap();
            }
            assert_eq!(self::rows(&admin).len(), if commit { 2 } else { 0 });
        }
        let plan = block(vec![
            insert(3),
            TransactionStep::Require(Expr::Value(ValueExpr::Literal(Value::Bool(false)))),
        ]);
        assert_eq!(
            session.execute_native_transaction(&plan, &Bindings::new()),
            Err(Error::RequirementFailed)
        );
        assert_eq!(rows(&admin).len(), 2);
        session.begin_native().unwrap();
        session
            .execute("INSERT INTO items VALUES(4,3)", &[], None)
            .unwrap();
        assert_eq!(
            session.execute_native_transaction(&plan, &Bindings::new()),
            Err(Error::RequirementFailed)
        );
        assert!(session.is_aborted());
        session.rollback_native().unwrap();
        assert_eq!(rows(&admin).len(), 2);
        drop(session);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn cancellation_between_steps_rolls_back_staged_write() {
        let (path, admin) = fixture(crate::Engine::V2);
        let cancellation = crate::Cancellation::default();
        let cancel = cancellation.clone();
        let result = cancellation.scope(|| {
            hooked(
                move |index| {
                    if index == 1 {
                        assert!(cancel.cancel());
                    }
                },
                || {
                    Engine::new(admin.clone())
                        .execute_transaction(&block(vec![insert(1), insert(2)]), &Bindings::new())
                },
            )
        });
        assert!(result.is_err());
        assert!(rows(&admin).is_empty());
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}
