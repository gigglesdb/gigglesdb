//! Explicit structural intent codec V1. These tags and equivalence rules are
//! permanent; future contracts need another encoder, not edits to these tags.
//!
//! Values replace parameter references, but every supplied occurrence remains.
//! Names, optional-clause presence and sequence order remain significant. Only
//! conflict targets are sets. Neither serde layout nor an execution plan is an
//! input to this codec. Identity scope and catalog epoch are separate fences.

use super::*;
use sha2::{Digest, Sha256};

const CODEC_V1: u16 = 1;
const BUDGET_V1: u16 = 1;
const SUBMISSION_DOMAIN: &[u8] = b"GD-ONCE-SUBMISSION\0";
const ACCEPTED_DOMAIN: &[u8] = b"GD-ONCE-ACCEPTED\0";

pub(super) enum AcceptedOutput<'a> {
    Operation(&'a [ColumnDef]),
    Transaction(&'a [EmitDescription]),
}

/// Catalog-free identity. Admission and exact value occurrence accounting run
/// before the hashing pass; no canonical byte buffer is allocated in either.
pub(super) fn submission_identity(
    request: &OnceRequest,
    bindings: &Bindings,
) -> OnceResult<[u8; 32]> {
    if request.contract_version != CODEC_V1 {
        return Err(Error::Unsupported.into());
    }
    match &request.body {
        OnceBody::Operation(body) => {
            validate_request(body)?;
            if matches!(
                body.parts()?.body,
                Request::Read { .. } | Request::ReadExpressions { .. }
            ) {
                return Err(Error::Unsupported.into());
            }
        }
        OnceBody::Transaction(body) => {
            transaction::validate_transaction(body)?;
            if body.read_only {
                return Err(Error::Unsupported.into());
            }
        }
    }
    validate_bindings(bindings)?;
    Encoder::new(bindings, false).submission(request)?;
    Encoder::new(bindings, true).submission(request)
}

/// Bind the submission to the exact catalog/output descriptors used by its
/// accepting transaction. Current schema is never consulted by a replay.
pub(super) fn accepted_identity(
    submitted: &[u8; 32],
    tables: &[TableDef],
    outputs: AcceptedOutput<'_>,
) -> OnceResult<[u8; 32]> {
    if tables.len() > crate::relational::MAX_TABLES {
        return Err(Error::Limit.into());
    }
    // The collection has already passed a constant bound before allocation.
    let mut tables = tables.iter().collect::<Vec<_>>();
    tables.sort_unstable_by_key(|table| table.id);
    if tables.windows(2).any(|pair| pair[0].id == pair[1].id) {
        return Err(Error::Bind("accepted table identity".into()).into());
    }
    let encode = |writer: &mut Writer| -> OnceResult<()> {
        writer.bytes(ACCEPTED_DOMAIN)?;
        writer.u16(CODEC_V1)?;
        writer.bytes(submitted)?;
        writer.count(tables.len())?;
        for table in &tables {
            writer.u64(table.id)?;
            writer.name(&table.name)?;
            writer.columns(&table.columns)?;
            if table.primary_key.len() > crate::relational::MAX_COLUMNS {
                return Err(Error::Limit.into());
            }
            writer.count(table.primary_key.len())?;
            for index in &table.primary_key {
                let column = table
                    .columns
                    .get(*index)
                    .ok_or_else(|| Error::Bind("accepted primary key".into()))?;
                writer.u64(column.id)?;
            }
        }
        match &outputs {
            AcceptedOutput::Operation(columns) => {
                writer.tag(0)?;
                writer.columns(columns)?;
            }
            AcceptedOutput::Transaction(outputs) => {
                if outputs.len() > MAX_EMIT_VALUES {
                    return Err(Error::Limit.into());
                }
                writer.tag(1)?;
                writer.count(outputs.len())?;
                for output in *outputs {
                    match output {
                        EmitDescription::Scalar {
                            data_type,
                            nullable,
                        } => {
                            writer.tag(0)?;
                            writer.flag(data_type.is_some())?;
                            if let Some(data_type) = data_type {
                                writer.data_type(data_type)?;
                            }
                            writer.flag(*nullable)?;
                        }
                        EmitDescription::Row { columns } => {
                            writer.tag(1)?;
                            writer.columns(columns)?;
                        }
                        EmitDescription::Rows { columns } => {
                            writer.tag(2)?;
                            writer.columns(columns)?;
                        }
                    }
                }
            }
        }
        Ok(())
    };
    encode(&mut Writer::new(false))?;
    let mut writer = Writer::new(true);
    encode(&mut writer)?;
    Ok(writer.finish())
}

struct Writer {
    bytes: usize,
    digest: Option<Sha256>,
}
impl Writer {
    fn new(hash: bool) -> Self {
        Self {
            bytes: 0,
            digest: hash.then(Sha256::new),
        }
    }
    fn bytes(&mut self, bytes: &[u8]) -> OnceResult<()> {
        self.bytes = self.bytes.checked_add(bytes.len()).ok_or(Error::Limit)?;
        if self.bytes > MAX_ONCE_INTENT_BYTES {
            return Err(Error::Limit.into());
        }
        if let Some(digest) = &mut self.digest {
            digest.update(bytes);
        }
        Ok(())
    }
    fn tag(&mut self, value: u8) -> OnceResult<()> {
        self.bytes(&[value])
    }
    fn flag(&mut self, value: bool) -> OnceResult<()> {
        self.tag(u8::from(value))
    }
    fn u16(&mut self, value: u16) -> OnceResult<()> {
        self.bytes(&value.to_be_bytes())
    }
    fn u64(&mut self, value: u64) -> OnceResult<()> {
        self.bytes(&value.to_be_bytes())
    }
    fn count(&mut self, value: usize) -> OnceResult<()> {
        let value = u32::try_from(value).map_err(|_| Error::Limit)?;
        self.bytes(&value.to_be_bytes())
    }
    fn blob(&mut self, value: &[u8]) -> OnceResult<()> {
        self.count(value.len())?;
        self.bytes(value)
    }
    fn name(&mut self, name: &str) -> OnceResult<()> {
        if !identifier(name) {
            return Err(Error::Bind("identifier".into()).into());
        }
        self.blob(name.as_bytes())
    }
    fn data_type(&mut self, value: &DataType) -> OnceResult<()> {
        self.tag(match value {
            DataType::Boolean => 0,
            DataType::SmallInt => 1,
            DataType::Integer => 2,
            DataType::BigInt => 3,
            DataType::Uuid
            | DataType::Jsonb
            | DataType::IntArray
            | DataType::BigIntArray
            | DataType::TextArray
            | DataType::Text
            | DataType::Varchar
            | DataType::Bpchar => 4,
            DataType::Bytes => 5,
            DataType::Numeric => 6,
            DataType::Real => 7,
            DataType::DoublePrecision => 8,
            DataType::Date => 9,
            DataType::Time => 10,
            DataType::TimeTz => 11,
            DataType::Timestamp => 12,
            DataType::TimestampTz => 13,
            DataType::Interval => 14,
        })
    }
    fn columns(&mut self, columns: &[ColumnDef]) -> OnceResult<()> {
        if columns.len() > crate::relational::MAX_COLUMNS {
            return Err(Error::Limit.into());
        }
        self.count(columns.len())?;
        for column in columns {
            self.u64(column.id)?;
            self.name(&column.name)?;
            self.data_type(&column.data_type)?;
            self.flag(column.not_null)?;
        }
        Ok(())
    }
    fn value(&mut self, value: &Value) -> OnceResult<()> {
        match value {
            Value::Date(n) => {
                self.tag(8)?;
                self.bytes(&n.to_binary())
            }
            Value::Time(n) => {
                self.tag(9)?;
                self.bytes(&n.to_binary())
            }
            Value::TimeTz(n) => {
                self.tag(10)?;
                self.bytes(&n.to_binary())
            }
            Value::Timestamp(n) => {
                self.tag(11)?;
                self.bytes(&n.to_binary())
            }
            Value::TimestampTz(n) => {
                self.tag(12)?;
                self.bytes(&n.to_binary())
            }
            Value::Interval(n) => {
                self.tag(13)?;
                self.bytes(&n.to_binary())
            }
            Value::Numeric(n) => {
                self.tag(5)?;
                self.blob(n.to_string().as_bytes())
            }
            Value::Float4(n) => {
                self.tag(6)?;
                self.bytes(&n.to_bits().to_be_bytes())
            }
            Value::Float8(n) => {
                self.tag(7)?;
                self.bytes(&n.to_bits().to_be_bytes())
            }
            Value::Null => self.tag(0),
            Value::Bool(value) => {
                self.tag(1)?;
                self.flag(*value)
            }
            Value::Int(value) => {
                self.tag(2)?;
                self.bytes(&value.to_be_bytes())
            }
            Value::Text(value) => {
                self.tag(3)?;
                self.blob(value.as_bytes())
            }
            Value::Bytes(value) => {
                self.tag(4)?;
                self.blob(value)
            }
        }
    }
    fn finish(self) -> [u8; 32] {
        self.digest
            .map(|digest| digest.finalize().into())
            .unwrap_or([0; 32])
    }
}

struct Encoder<'a> {
    bindings: &'a Bindings,
    used: BTreeSet<&'a str>,
    remaining_values: usize,
    writer: Writer,
}
impl<'a> Encoder<'a> {
    fn new(bindings: &'a Bindings, hash: bool) -> Self {
        Self {
            bindings,
            used: BTreeSet::new(),
            remaining_values: MAX_BOUND_VALUE_BYTES,
            writer: Writer::new(hash),
        }
    }
    fn submission(mut self, request: &'a OnceRequest) -> OnceResult<[u8; 32]> {
        self.writer.bytes(SUBMISSION_DOMAIN)?;
        self.writer.u16(CODEC_V1)?;
        self.writer.u16(BUDGET_V1)?;
        self.writer.u64(request.retention_seconds)?;
        match &request.body {
            OnceBody::Operation(request) => {
                self.writer.tag(0)?;
                self.request(request)?;
            }
            OnceBody::Transaction(transaction) => {
                self.writer.tag(1)?;
                self.writer.flag(transaction.read_only)?;
                self.writer.count(transaction.steps.len())?;
                for step in &transaction.steps {
                    match step {
                        TransactionStep::Execute(request) => {
                            self.writer.tag(0)?;
                            self.request(request)?;
                        }
                        TransactionStep::Let { name, request } => {
                            self.writer.tag(1)?;
                            self.writer.name(name)?;
                            self.request(request)?;
                        }
                        TransactionStep::Require(expression) => {
                            self.writer.tag(2)?;
                            self.expression(expression)?;
                        }
                    }
                }
                self.writer.flag(transaction.emit.is_some())?;
                if let Some(emit) = &transaction.emit {
                    self.writer.count(emit.len())?;
                    for item in emit {
                        match item {
                            EmitExpr::Scalar(expression) => {
                                self.writer.tag(0)?;
                                self.expression(expression)?;
                            }
                            EmitExpr::Row(name) => {
                                self.writer.tag(1)?;
                                self.writer.name(name)?;
                            }
                            EmitExpr::Rows(name) => {
                                self.writer.tag(2)?;
                                self.writer.name(name)?;
                            }
                        }
                    }
                }
            }
        }
        if self.used.len() != self.bindings.len() {
            return Err(Error::Bind("parameters".into()).into());
        }
        Ok(self.writer.finish())
    }
    fn value(&mut self, expression: &'a ValueExpr) -> OnceResult<()> {
        let value = match expression {
            ValueExpr::Literal(value) => value,
            ValueExpr::Parameter(name) => {
                if name.is_empty()
                    || name.len() > 63
                    || !(name.as_bytes()[0].is_ascii_alphabetic() || name.starts_with('_'))
                    || !name.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'
                    })
                {
                    return Err(Error::Bind("parameter name".into()).into());
                }
                let value = self
                    .bindings
                    .get(name)
                    .ok_or_else(|| Error::Bind("parameter missing".into()))?;
                self.used.insert(name);
                value
            }
        };
        self.remaining_values = self
            .remaining_values
            .checked_sub(value_bytes(value))
            .ok_or(Error::Limit)?;
        self.writer.value(value)
    }
    fn optional_value(&mut self, value: &'a Option<ValueExpr>) -> OnceResult<()> {
        self.writer.flag(value.is_some())?;
        if let Some(value) = value {
            self.value(value)?;
        }
        Ok(())
    }
    fn optional_expression(&mut self, expression: &'a Option<Expr>) -> OnceResult<()> {
        self.writer.flag(expression.is_some())?;
        if let Some(expression) = expression {
            self.expression(expression)?;
        }
        Ok(())
    }
    fn names(&mut self, names: &[String]) -> OnceResult<()> {
        self.writer.count(names.len())?;
        for name in names {
            self.writer.name(name)?;
        }
        Ok(())
    }
    fn sorts(&mut self, sorts: &[Sort]) -> OnceResult<()> {
        self.writer.count(sorts.len())?;
        for sort in sorts {
            self.writer.name(&sort.field)?;
            self.writer.flag(sort.descending)?;
        }
        Ok(())
    }
    fn assignments(&mut self, values: &'a [(String, ValueExpr)]) -> OnceResult<()> {
        self.writer.count(values.len())?;
        for (name, value) in values {
            self.writer.name(name)?;
            self.value(value)?;
        }
        Ok(())
    }
    fn expression_assignments(&mut self, values: &'a [(String, Expr)]) -> OnceResult<()> {
        self.writer.count(values.len())?;
        for (name, value) in values {
            self.writer.name(name)?;
            self.expression(value)?;
        }
        Ok(())
    }
    fn projections(&mut self, projection: &'a [Projection]) -> OnceResult<()> {
        self.writer.count(projection.len())?;
        for item in projection {
            self.expression(&item.expression)?;
            self.writer.flag(item.alias.is_some())?;
            if let Some(alias) = &item.alias {
                self.writer.name(alias)?;
            }
        }
        Ok(())
    }
    fn intent(&mut self, intent: &'a Intent) -> OnceResult<()> {
        match intent {
            Intent::One => self.writer.tag(0),
            Intent::Many { cap } => {
                self.writer.tag(1)?;
                self.value(cap)
            }
            Intent::Claim { order } => {
                self.writer.tag(2)?;
                self.sorts(order)
            }
        }
    }
    fn request(&mut self, request: &'a Request) -> OnceResult<()> {
        match request {
            Request::Read {
                table,
                filter,
                order,
                limit,
                projection,
            } => {
                self.writer.tag(0)?;
                self.writer.name(table)?;
                self.optional_expression(filter)?;
                self.sorts(order)?;
                self.optional_value(limit)?;
                self.names(projection)
            }
            Request::Insert { table, values } => {
                self.writer.tag(1)?;
                self.writer.name(table)?;
                self.assignments(values)
            }
            Request::Update {
                table,
                filter,
                guard,
                values,
                intent,
            } => {
                self.writer.tag(2)?;
                self.writer.name(table)?;
                self.expression(filter)?;
                self.optional_expression(guard)?;
                self.assignments(values)?;
                self.intent(intent)
            }
            Request::Delete {
                table,
                filter,
                guard,
                cap,
            } => {
                self.writer.tag(3)?;
                self.writer.name(table)?;
                self.expression(filter)?;
                self.optional_expression(guard)?;
                self.optional_value(cap)
            }
            Request::UpdateExpressions {
                table,
                filter,
                guard,
                values,
                intent,
            } => {
                self.writer.tag(4)?;
                self.writer.name(table)?;
                self.expression(filter)?;
                self.optional_expression(guard)?;
                self.expression_assignments(values)?;
                self.intent(intent)
            }
            Request::ReadExpressions {
                table,
                filter,
                order,
                limit,
                projection,
            } => {
                self.writer.tag(5)?;
                self.writer.name(table)?;
                self.optional_expression(filter)?;
                self.sorts(order)?;
                self.optional_value(limit)?;
                self.projections(projection)
            }
            Request::Returning {
                request,
                projection,
            } => {
                self.writer.tag(6)?;
                self.request(request)?;
                self.projections(projection)
            }
            Request::InsertExpressions { table, values } => {
                self.writer.tag(7)?;
                self.writer.name(table)?;
                self.expression_assignments(values)
            }
            Request::InsertMany {
                table,
                columns,
                rows,
                cap,
            } => {
                self.writer.tag(8)?;
                self.writer.name(table)?;
                self.names(columns)?;
                self.writer.count(rows.len())?;
                for row in rows {
                    self.writer.count(row.len())?;
                    for value in row {
                        self.expression(value)?;
                    }
                }
                self.value(cap)
            }
            Request::OnConflict {
                request,
                target,
                action,
            } => {
                self.writer.tag(9)?;
                self.request(request)?;
                // parts() bounded target count before this small sort allocation.
                let mut target = target.iter().map(String::as_str).collect::<Vec<_>>();
                target.sort_unstable();
                if target.windows(2).any(|pair| pair[0] == pair[1]) {
                    return Err(Error::Bind("conflict target".into()).into());
                }
                self.writer.count(target.len())?;
                for name in target {
                    self.writer.name(name)?;
                }
                match action {
                    ConflictAction::Nothing => self.writer.tag(0),
                    ConflictAction::Update { guard, values } => {
                        self.writer.tag(1)?;
                        self.optional_expression(guard)?;
                        self.expression_assignments(values)
                    }
                }
            }
        }
    }
    fn expression(&mut self, expression: &'a Expr) -> OnceResult<()> {
        match expression {
            Expr::Value(value) => {
                self.writer.tag(0)?;
                self.value(value)
            }
            Expr::Field(name) => {
                self.writer.tag(1)?;
                self.writer.name(name)
            }
            Expr::Compare(left, compare, right) => {
                self.writer.tag(2)?;
                self.writer.tag(match compare {
                    Compare::Eq => 0,
                    Compare::Ne => 1,
                    Compare::Lt => 2,
                    Compare::Le => 3,
                    Compare::Gt => 4,
                    Compare::Ge => 5,
                })?;
                self.expression(left)?;
                self.expression(right)
            }
            Expr::IsNull(value, negate) => {
                self.writer.tag(3)?;
                self.expression(value)?;
                self.writer.flag(*negate)
            }
            Expr::Not(value) => {
                self.writer.tag(4)?;
                self.expression(value)
            }
            Expr::And(left, right) => {
                self.writer.tag(5)?;
                self.expression(left)?;
                self.expression(right)
            }
            Expr::Or(left, right) => {
                self.writer.tag(6)?;
                self.expression(left)?;
                self.expression(right)
            }
            Expr::Negate(value) => {
                self.writer.tag(7)?;
                self.expression(value)
            }
            Expr::Arithmetic(left, op, right) => {
                self.writer.tag(8)?;
                self.writer.tag(match op {
                    Arithmetic::Add => 0,
                    Arithmetic::Subtract => 1,
                    Arithmetic::Multiply => 2,
                    Arithmetic::Divide => 3,
                    Arithmetic::Remainder => 4,
                })?;
                self.expression(left)?;
                self.expression(right)
            }
            Expr::Local(reference) => {
                self.writer.tag(9)?;
                match reference {
                    LocalRef::Field { local, field } => {
                        self.writer.tag(0)?;
                        self.writer.name(local)?;
                        self.writer.name(field)
                    }
                    LocalRef::Status { local } => {
                        self.writer.tag(1)?;
                        self.writer.name(local)
                    }
                    LocalRef::Changed { local } => {
                        self.writer.tag(2)?;
                        self.writer.name(local)
                    }
                    LocalRef::Count { local } => {
                        self.writer.tag(3)?;
                        self.writer.name(local)
                    }
                }
            }
            Expr::Call {
                function,
                arguments,
            } => {
                self.writer.tag(10)?;
                self.writer.tag(match function {
                    ScalarFunction::Coalesce => 0,
                    ScalarFunction::Abs => 1,
                    ScalarFunction::Length => 2,
                })?;
                self.writer.count(arguments.len())?;
                for argument in arguments {
                    self.expression(argument)?;
                }
                Ok(())
            }
            Expr::Case {
                operand,
                arms,
                else_expr,
            } => {
                self.writer.tag(11)?;
                self.writer.flag(operand.is_some())?;
                if let Some(operand) = operand {
                    self.expression(operand)?;
                }
                self.writer.count(arms.len())?;
                for (when, then) in arms {
                    self.expression(when)?;
                    self.expression(then)?;
                }
                self.writer.flag(else_expr.is_some())?;
                if let Some(other) = else_expr {
                    self.expression(other)?;
                }
                Ok(())
            }
            Expr::ConflictField { source, field } => {
                self.writer.tag(12)?;
                self.writer.tag(match source {
                    ConflictSource::Existing => 0,
                    ConflictSource::Incoming => 1,
                })?;
                self.writer.name(field)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(body: OnceBody) -> OnceRequest {
        OnceRequest {
            contract_version: 1,
            identity: OnceIdentity {
                database_domain: 17,
                tenant: 23,
                namespace: "application".into(),
                key: b"original-request".to_vec(),
            },
            retention_seconds: 60,
            body,
        }
    }

    fn insert(value: ValueExpr) -> Request {
        Request::Insert {
            table: "items".into(),
            values: vec![("id".into(), value)],
        }
    }

    fn literal(value: i64) -> ValueExpr {
        ValueExpr::Literal(Value::Int(value))
    }

    fn hash(body: Request) -> [u8; 32] {
        submission_identity(&envelope(OnceBody::Operation(body)), &Bindings::new()).unwrap()
    }

    fn expression_request(expression: Expr) -> OnceRequest {
        envelope(OnceBody::Operation(Request::InsertExpressions {
            table: "items".into(),
            values: vec![("payload".into(), expression)],
        }))
    }

    fn operation_error(error: Error) -> OnceError {
        OnceError::Operation(error)
    }

    #[test]
    fn submission_v1_has_an_independent_golden_vector() {
        // This 61-byte vector was assembled independently from the V1 wire
        // contract: domain, codec/budget versions, retention, body/request tags,
        // table, one assignment and the signed integer 7, all lengths big-endian.
        let digest = hash(insert(literal(7)));
        let hexadecimal = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(
            hexadecimal,
            "cf3ead3361e8ad6f91239a4c60f4d80d1d1612e190eab56a109151810c15ff30"
        );
        // Altering these inherited admission rules needs an explicit codec
        // compatibility decision, even if the structural tags stay unchanged.
        assert_eq!(
            (MAX_INPUT, MAX_DEPTH, MAX_EXPRESSION_NODES),
            (65_536, 32, 4096)
        );
        assert_eq!(
            (MAX_BINDINGS, MAX_BINDING_BYTES, MAX_BOUND_VALUE_BYTES),
            (256, 65_536, 65_536)
        );
        assert_eq!(MAX_ONCE_INTENT_BYTES, 262_144);
    }

    #[test]
    fn parameters_resolve_to_typed_values_but_identity_scope_is_external() {
        let expected = hash(insert(literal(7)));
        for name in ["number", "renamed", "_value"] {
            let request = envelope(OnceBody::Operation(insert(ValueExpr::Parameter(
                name.into(),
            ))));
            assert_eq!(
                submission_identity(&request, &Bindings::from([(name.into(), Value::Int(7))]))
                    .unwrap(),
                expected
            );
        }
        let mut request = envelope(OnceBody::Operation(insert(literal(7))));
        request.identity = OnceIdentity {
            database_domain: 999,
            tenant: 1000,
            namespace: "different".into(),
            key: b"other-key".to_vec(),
        };
        assert_eq!(
            submission_identity(&request, &Bindings::new()).unwrap(),
            expected
        );
        request.retention_seconds += 1;
        assert_ne!(
            submission_identity(&request, &Bindings::new()).unwrap(),
            expected
        );
        request.contract_version = 2;
        assert_eq!(
            submission_identity(&request, &Bindings::new()),
            Err(operation_error(Error::Unsupported))
        );

        let values = [
            Value::Null,
            Value::Bool(false),
            Value::Int(0),
            Value::Text("0".into()),
            Value::Bytes(b"0".to_vec()),
        ];
        let identities = values
            .into_iter()
            .map(|value| hash(insert(ValueExpr::Literal(value))))
            .collect::<BTreeSet<_>>();
        assert_eq!(identities.len(), 5);
    }

    #[test]
    fn omitted_defaults_orders_and_aliases_remain_observable() {
        let delete = Request::Delete {
            table: "items".into(),
            filter: Expr::Field("enabled".into()),
            guard: None,
            cap: None,
        };
        let mut guarded = delete.clone();
        if let Request::Delete { guard, .. } = &mut guarded {
            *guard = Some(Expr::Value(ValueExpr::Literal(Value::Bool(true))));
        }
        assert_ne!(hash(delete), hash(guarded));

        let first = Request::Insert {
            table: "items".into(),
            values: vec![
                ("id".into(), literal(1)),
                ("payload".into(), ValueExpr::Literal(Value::Null)),
            ],
        };
        assert_ne!(hash(insert(literal(1))), hash(first.clone()));
        let mut reordered = first.clone();
        if let Request::Insert { values, .. } = &mut reordered {
            values.reverse();
        }
        assert_ne!(hash(first.clone()), hash(reordered));
        let returning = |alias| Request::Returning {
            request: Box::new(first.clone()),
            projection: vec![Projection {
                expression: Expr::Field("id".into()),
                alias,
            }],
        };
        assert_ne!(hash(returning(None)), hash(returning(Some("id".into()))));
        assert_ne!(
            hash(returning(Some("id".into()))),
            hash(returning(Some("renamed".into())))
        );

        let read_in_transaction = |limit| {
            envelope(OnceBody::Transaction(Transaction {
                read_only: false,
                steps: vec![
                    TransactionStep::Execute(insert(literal(1))),
                    TransactionStep::Execute(Request::Read {
                        table: "items".into(),
                        filter: None,
                        order: vec![],
                        limit,
                        projection: vec![],
                    }),
                ],
                emit: None,
            }))
        };
        assert_ne!(
            submission_identity(&read_in_transaction(None), &Bindings::new()).unwrap(),
            submission_identity(&read_in_transaction(Some(literal(1024))), &Bindings::new())
                .unwrap()
        );
    }

    #[test]
    fn conflict_targets_are_sets_but_input_tuples_are_ordered() {
        let upsert = |target: Vec<&str>| Request::OnConflict {
            request: Box::new(insert(literal(1))),
            target: target.into_iter().map(str::to_owned).collect(),
            action: ConflictAction::Nothing,
        };
        assert_eq!(
            hash(upsert(vec!["tenant", "id"])),
            hash(upsert(vec!["id", "tenant"]))
        );
        assert_eq!(
            submission_identity(
                &envelope(OnceBody::Operation(upsert(vec!["id", "id"]))),
                &Bindings::new()
            ),
            Err(operation_error(Error::Bind("conflict target".into())))
        );
        let many = |ids: [i64; 2]| Request::InsertMany {
            table: "items".into(),
            columns: vec!["id".into()],
            rows: ids
                .into_iter()
                .map(|id| vec![Expr::Value(literal(id))])
                .collect(),
            cap: literal(2),
        };
        assert_ne!(hash(many([1, 2])), hash(many([2, 1])));
    }

    #[test]
    fn local_names_steps_and_emit_kinds_are_structural() {
        let transaction = |local: &str| {
            envelope(OnceBody::Transaction(Transaction {
                read_only: false,
                steps: vec![TransactionStep::Let {
                    name: local.into(),
                    request: insert(literal(1)),
                }],
                emit: Some(vec![EmitExpr::Scalar(Expr::Local(LocalRef::Status {
                    local: local.into(),
                }))]),
            }))
        };
        assert_ne!(
            submission_identity(&transaction("created"), &Bindings::new()).unwrap(),
            submission_identity(&transaction("renamed"), &Bindings::new()).unwrap()
        );
        let ordered = |reverse: bool| {
            let mut steps = vec![
                TransactionStep::Execute(insert(literal(1))),
                TransactionStep::Execute(insert(literal(2))),
            ];
            if reverse {
                steps.reverse();
            }
            envelope(OnceBody::Transaction(Transaction {
                read_only: false,
                steps,
                emit: None,
            }))
        };
        assert_ne!(
            submission_identity(&ordered(false), &Bindings::new()).unwrap(),
            submission_identity(&ordered(true), &Bindings::new()).unwrap()
        );
        let row_or_rows = |emit| {
            envelope(OnceBody::Transaction(Transaction {
                read_only: false,
                steps: vec![TransactionStep::Let {
                    name: "created".into(),
                    request: insert(literal(1)),
                }],
                emit: Some(vec![emit]),
            }))
        };
        // Catalog-free identity preserves even distinctions that the subsequent
        // binder may reject for a particular local's shape.
        assert_ne!(
            submission_identity(
                &row_or_rows(EmitExpr::Row("created".into())),
                &Bindings::new()
            )
            .unwrap(),
            submission_identity(
                &row_or_rows(EmitExpr::Rows("created".into())),
                &Bindings::new()
            )
            .unwrap()
        );
    }

    #[test]
    fn raw_and_hidden_binding_admission_precedes_hashing() {
        let request = envelope(OnceBody::Operation(insert(literal(7))));
        assert_eq!(
            submission_identity(
                &request,
                &Bindings::from([(
                    "unused".into(),
                    Value::Text("x".repeat(MAX_BINDING_BYTES + 1))
                )])
            ),
            Err(operation_error(Error::Limit))
        );
        assert_eq!(
            submission_identity(
                &request,
                &Bindings::from([("unused".into(), Value::Int(1))])
            ),
            Err(operation_error(Error::Bind("parameters".into())))
        );
        let too_many = (0..=MAX_BINDINGS)
            .map(|index| (format!("p{index}"), Value::Null))
            .collect();
        assert_eq!(
            submission_identity(&request, &too_many),
            Err(operation_error(Error::Limit))
        );
        let hidden = expression_request(Expr::Call {
            function: ScalarFunction::Coalesce,
            arguments: vec![
                Expr::Value(literal(1)),
                Expr::Value(ValueExpr::Parameter("hidden".into())),
            ],
        });
        assert_eq!(
            submission_identity(&hidden, &Bindings::new()),
            Err(operation_error(Error::Bind("parameter missing".into())))
        );
        assert_eq!(
            submission_identity(
                &hidden,
                &Bindings::from([("hidden".into(), Value::Text("x".repeat(MAX_BINDING_BYTES)))])
            ),
            Err(operation_error(Error::Limit))
        );
        let bad_name = envelope(OnceBody::Operation(insert(ValueExpr::Parameter(
            "UPPER".into(),
        ))));
        assert_eq!(
            submission_identity(
                &bad_name,
                &Bindings::from([("UPPER".into(), Value::Int(1))])
            ),
            Err(operation_error(Error::Bind("parameter name".into())))
        );
    }

    #[test]
    fn resolved_occurrences_share_an_exact_budget_across_lazy_branches_and_steps() {
        let parameter = || Expr::Value(ValueExpr::Parameter("payload".into()));
        let request = |count| {
            expression_request(Expr::Call {
                function: ScalarFunction::Coalesce,
                arguments: (0..count).map(|_| parameter()).collect(),
            })
        };
        let bindings = Bindings::from([(
            "payload".into(),
            Value::Text("x".repeat(MAX_BOUND_VALUE_BYTES / 2)),
        )]);
        assert!(submission_identity(&request(2), &bindings).is_ok());
        assert_eq!(
            submission_identity(&request(3), &bindings),
            Err(operation_error(Error::Limit))
        );

        let transaction = |count| {
            envelope(OnceBody::Transaction(Transaction {
                read_only: false,
                steps: (0..count)
                    .map(|_| {
                        TransactionStep::Execute(insert(ValueExpr::Parameter("payload".into())))
                    })
                    .collect(),
                emit: None,
            }))
        };
        assert!(submission_identity(&transaction(2), &bindings).is_ok());
        assert_eq!(
            submission_identity(&transaction(3), &bindings),
            Err(operation_error(Error::Limit))
        );
    }

    #[test]
    fn malformed_wrappers_and_deep_expressions_are_rejected_before_recursion() {
        let nested = Request::Returning {
            request: Box::new(Request::Returning {
                request: Box::new(insert(ValueExpr::Literal(Value::Text(
                    "x".repeat(MAX_INPUT + 1),
                )))),
                projection: vec![],
            }),
            projection: vec![],
        };
        assert_eq!(
            submission_identity(&envelope(OnceBody::Operation(nested)), &Bindings::new()),
            Err(operation_error(Error::Unsupported))
        );
        let mut expression = Expr::Value(literal(1));
        for _ in 0..=MAX_DEPTH {
            expression = Expr::Negate(Box::new(expression));
        }
        assert_eq!(
            submission_identity(&expression_request(expression), &Bindings::new()),
            Err(operation_error(Error::Limit))
        );

        let read = Request::Read {
            table: "items".into(),
            filter: None,
            order: vec![],
            limit: None,
            projection: vec![],
        };
        assert_eq!(
            submission_identity(
                &envelope(OnceBody::Operation(read.clone())),
                &Bindings::new()
            ),
            Err(operation_error(Error::Unsupported))
        );
        let read_only = Transaction {
            read_only: true,
            steps: vec![TransactionStep::Execute(read)],
            emit: None,
        };
        assert_eq!(
            submission_identity(
                &envelope(OnceBody::Transaction(read_only)),
                &Bindings::new()
            ),
            Err(operation_error(Error::Unsupported))
        );
    }

    fn table(id: u64, name: &str) -> TableDef {
        TableDef {
            id,
            name: name.into(),
            columns: vec![ColumnDef {
                id: id * 10,
                name: "id".into(),
                data_type: DataType::BigInt,
                not_null: true,
            }],
            primary_key: vec![0],
        }
    }

    #[test]
    fn accepted_identity_binds_stable_objects_and_output_types() {
        let submitted = hash(insert(literal(7)));
        let a = table(1, "items");
        let b = table(2, "other");
        let accepted = |tables: &[TableDef], columns: &[ColumnDef]| {
            accepted_identity(&submitted, tables, AcceptedOutput::Operation(columns)).unwrap()
        };
        assert_eq!(
            accepted(&[a.clone(), b.clone()], &a.columns),
            accepted(&[b, a.clone()], &a.columns)
        );
        let baseline = accepted(std::slice::from_ref(&a), &a.columns);
        for mutation in 0..6 {
            let mut changed = a.clone();
            match mutation {
                0 => changed.id += 1,
                1 => changed.name = "renamed".into(),
                2 => changed.columns[0].id += 1,
                3 => changed.columns[0].data_type = DataType::Integer,
                4 => changed.columns[0].not_null = false,
                5 => changed.columns[0].name = "renamed".into(),
                _ => unreachable!(),
            }
            assert_ne!(
                accepted(std::slice::from_ref(&changed), &a.columns),
                baseline
            );
        }
        let mut alias = a.columns.clone();
        alias[0].name = "result".into();
        assert_ne!(accepted(std::slice::from_ref(&a), &alias), baseline);
        assert_eq!(
            accepted_identity(
                &submitted,
                &[a.clone(), a.clone()],
                AcceptedOutput::Operation(&[])
            ),
            Err(operation_error(Error::Bind(
                "accepted table identity".into()
            )))
        );

        let emit_identity = |output| {
            accepted_identity(
                &submitted,
                std::slice::from_ref(&a),
                AcceptedOutput::Transaction(&[output]),
            )
            .unwrap()
        };
        assert_ne!(
            emit_identity(EmitDescription::Scalar {
                data_type: None,
                nullable: true
            }),
            emit_identity(EmitDescription::Scalar {
                data_type: Some(DataType::BigInt),
                nullable: true
            })
        );
        assert_ne!(
            emit_identity(EmitDescription::Row {
                columns: a.columns.clone()
            }),
            emit_identity(EmitDescription::Rows {
                columns: a.columns.clone()
            })
        );
    }

    #[test]
    fn canonical_size_is_bounded_before_hashing_and_after_metadata_expansion() {
        for hashing in [false, true] {
            let mut writer = Writer::new(hashing);
            writer.bytes(&vec![0; MAX_ONCE_INTENT_BYTES]).unwrap();
            assert_eq!(writer.tag(0), Err(operation_error(Error::Limit)));
        }
        let columns = (0..crate::relational::MAX_COLUMNS)
            .map(|index| ColumnDef {
                id: index as u64 + 1,
                name: format!("c{index:02}{}", "x".repeat(60)),
                data_type: DataType::Text,
                not_null: true,
            })
            .collect::<Vec<_>>();
        let tables = (0..crate::relational::MAX_TABLES)
            .map(|index| TableDef {
                id: index as u64 + 1,
                name: format!("t{index}"),
                columns: columns.clone(),
                primary_key: vec![0],
            })
            .collect::<Vec<_>>();
        assert_eq!(
            accepted_identity(&[0; 32], &tables, AcceptedOutput::Operation(&[])),
            Err(operation_error(Error::Limit))
        );
    }
}
