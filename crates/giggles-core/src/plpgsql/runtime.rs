use super::*;
use crate::relational::output_text;
use std::collections::HashMap;

type Result<T> = std::result::Result<T, PlRuntimeError>;

pub(super) fn exec_trigger(
    f: &CompiledFunction,
    ctx: &TriggerData,
    spi: &mut dyn Spi,
) -> Result<TriggerReturn> {
    if f.signature.kind != FunctionKind::Trigger {
        return Err(error("42809", "function is not a trigger function"));
    }
    let mut state = State {
        ctx,
        spi,
        scopes: vec![trigger_scope(ctx)],
        notices: Vec::new(),
        row_count: 0,
        function: &f.signature.name,
    };
    let result = match state.block(&f.block) {
        Ok(Flow::Return(value)) => Ok(value),
        Ok(_) => {
            let mut err = error(
                "2F005",
                "control reached end of trigger procedure without RETURN",
            );
            err.context
                .push(format!("PL/pgSQL function {}()", state.function));
            Err(err)
        }
        Err(err) => Err(err),
    };
    if !state.notices.is_empty() {
        state.spi.notices(state.notices);
    }
    result
}

enum Flow {
    Next,
    Exit(Option<String>),
    Continue(Option<String>),
    Return(TriggerReturn),
}

enum Datum {
    Scalar(SpiParam),
    Record(Record),
}

#[derive(Clone)]
struct Record {
    columns: Option<Arc<[SpiColumn]>>,
    values: Option<Vec<Value>>,
    fixed_shape: bool,
}

struct Variable {
    datum: Datum,
    constant: bool,
    not_null: bool,
    ty: Option<ScalarType>,
}

type Scope = HashMap<String, Variable>;

struct State<'a> {
    ctx: &'a TriggerData,
    spi: &'a mut dyn Spi,
    scopes: Vec<Scope>,
    notices: Vec<PlNotice>,
    row_count: u64,
    function: &'a str,
}

fn error(sqlstate: &str, message: impl Into<Box<str>>) -> PlRuntimeError {
    PlRuntimeError {
        sqlstate: sqlstate.into(),
        message: message.into(),
        detail: None,
        hint: None,
        query: None,
        context: Vec::new(),
        spi_error: None,
    }
}

impl PlRuntimeError {
    fn from_spi(cause: SpiError, query: &str) -> Self {
        let mut error = error(crate::sqlstate(&cause.error), cause.message.as_str());
        error.detail = cause.detail.as_deref().map(Into::into);
        error.hint = cause.hint.as_deref().map(Into::into);
        error.query = Some(query.into());
        error.spi_error = Some(Box::new(cause));
        error
    }

    fn assignment_expression(mut self, query: &str) -> Self {
        self.query = None;
        self.context
            .push(format!("PL/pgSQL assignment \"{query}\""));
        self
    }

    fn with_query(mut self, query: &str) -> Self {
        self.query = Some(query.into());
        self
    }
}

fn param(data_type: DataType, value: Value) -> SpiParam {
    SpiParam {
        data_type,
        value,
        type_oid: None,
    }
}

fn text(value: impl Into<String>) -> SpiParam {
    param(DataType::Text, Value::Text(value.into()))
}

fn scalar(value: SpiParam) -> Variable {
    Variable {
        datum: Datum::Scalar(value),
        constant: false,
        not_null: false,
        ty: None,
    }
}

fn record(columns: Option<Arc<[SpiColumn]>>, values: Option<Vec<Value>>) -> Variable {
    let fixed_shape = columns.is_some();
    Variable {
        datum: Datum::Record(Record {
            columns,
            values,
            fixed_shape,
        }),
        constant: false,
        not_null: false,
        ty: None,
    }
}

fn trigger_scope(ctx: &TriggerData) -> Scope {
    let named = |name: &str| SpiParam {
        type_oid: Some(19),
        ..text(name)
    };
    let when = match ctx.when {
        TriggerWhen::Before => "BEFORE",
        TriggerWhen::After => "AFTER",
        TriggerWhen::InsteadOf => "INSTEAD OF",
    };
    let level = match ctx.level {
        TriggerLevel::Row => "ROW",
        TriggerLevel::Statement => "STATEMENT",
    };
    let op = match ctx.op {
        TriggerOp::Insert => "INSERT",
        TriggerOp::Update => "UPDATE",
        TriggerOp::Delete => "DELETE",
        TriggerOp::Truncate => "TRUNCATE",
    };
    let oid = SpiParam {
        type_oid: Some(26),
        ..param(DataType::BigInt, Value::Int(i64::from(ctx.relation_oid)))
    };
    let columns: Arc<[_]> = ctx.columns.clone().into();
    HashMap::from([
        ("new".into(), record(Some(columns.clone()), ctx.new.clone())),
        ("old".into(), record(Some(columns), ctx.old.clone())),
        (
            "found".into(),
            scalar(param(DataType::Boolean, Value::Bool(false))),
        ),
        ("tg_name".into(), scalar(named(&ctx.name))),
        ("tg_when".into(), scalar(text(when))),
        ("tg_level".into(), scalar(text(level))),
        ("tg_op".into(), scalar(text(op))),
        ("tg_relid".into(), scalar(oid)),
        ("tg_relname".into(), scalar(named(&ctx.relation_name))),
        ("tg_table_name".into(), scalar(named(&ctx.relation_name))),
        ("tg_table_schema".into(), scalar(named(&ctx.schema_name))),
        (
            "tg_nargs".into(),
            scalar(param(
                DataType::Integer,
                Value::Int(ctx.arguments.len() as i64),
            )),
        ),
    ])
}

impl State<'_> {
    fn block(&mut self, block: &Block) -> Result<Flow> {
        self.scopes.push(HashMap::new());
        let result = self.block_body(block);
        self.scopes.pop();
        match result? {
            Flow::Exit(Some(label)) if block.label.as_ref() == Some(&label) => Ok(Flow::Next),
            result => Ok(result),
        }
    }

    fn block_body(&mut self, block: &Block) -> Result<Flow> {
        for declaration in &block.declarations {
            self.declare(declaration).map_err(|mut err| {
                err.context.push(format!(
                    "PL/pgSQL function {}() line {} during statement block local variable initialization",
                    self.function, declaration.line
                ));
                err
            })?;
        }
        self.statements(&block.statements)
    }

    fn declare(&mut self, decl: &Declaration) -> Result<()> {
        let mut variable = match &decl.ty {
            Some(ty) => scalar(param(ty.data_type.clone(), Value::Null)),
            None => record(None, None),
        };
        variable.ty = decl.ty.clone();
        variable.not_null = decl.not_null;
        match &decl.default {
            Some(ValueExpr::Scalar(default)) => {
                let value = self.eval(default)?;
                Self::check_null(decl.not_null, &value.value, &decl.name)?;
                if let Some(ty) = &decl.ty {
                    variable.datum =
                        Datum::Scalar(self.cast_to_decl(value, ty, &default.original)?);
                } else if value.value != Value::Null {
                    return Err(error(
                        "42804",
                        "cannot assign non-composite value to a record variable",
                    ));
                }
            }
            Some(ValueExpr::Record(name)) => {
                let mut record = self.record(name)?.clone();
                record.fixed_shape = false;
                if decl.ty.is_some() {
                    return Err(error(
                        "42804",
                        "cannot assign a record to a scalar variable",
                    ));
                }
                Self::check_record_null(decl.not_null, &record, &decl.name)?;
                variable.datum = Datum::Record(record);
            }
            None => {}
        }
        variable.constant = decl.constant;
        self.scopes
            .last_mut()
            .unwrap()
            .insert(decl.name.clone(), variable);
        Ok(())
    }

    fn statements(&mut self, statements: &[Statement]) -> Result<Flow> {
        for statement in statements {
            let result = self.statement(statement);
            match result? {
                Flow::Next => {}
                other => return Ok(other),
            }
        }
        Ok(Flow::Next)
    }

    fn statement(&mut self, stmt: &Statement) -> Result<Flow> {
        // Attach the current statement to errors raised here. Errors coming out
        // of nested statements already carry their innermost location.
        self.run_statement(&stmt.kind).map_err(|mut err| {
            if err.context.is_empty() || matches!(&stmt.kind, Stmt::Assign(..)) {
                err.context.push(format!(
                    "PL/pgSQL function {}() line {} at {}",
                    self.function,
                    stmt.line,
                    stmt.kind.action()
                ));
            }
            err
        })
    }

    fn run_statement(&mut self, stmt: &Stmt) -> Result<Flow> {
        match stmt {
            Stmt::Block(block) => return self.block(block),
            Stmt::Assign(target, ValueExpr::Scalar(expr), source) => {
                if target.field.is_some() {
                    self.destination(target)
                        .map_err(|err| err.assignment_expression(source))?;
                }
                let value = self
                    .eval(expr)
                    .map_err(|err| err.assignment_expression(source))?;
                self.assign(target, value, source)?;
            }
            Stmt::Assign(target, ValueExpr::Record(name), _) => {
                let record = self.record(name)?.clone();
                self.assign_record(target, record.columns, record.values)?;
            }
            Stmt::If(branches, otherwise) => {
                for (condition, branch) in branches {
                    if self.condition(condition)? {
                        return self.statements(branch);
                    }
                }
                return self.statements(otherwise);
            }
            Stmt::Case {
                value,
                arms,
                otherwise,
            } => {
                let value = value.as_ref().map(|sql| self.eval(sql)).transpose()?;
                for (conditions, branch) in arms {
                    for condition in conditions {
                        let matches = match &value {
                            Some(value) => self.case_matches(value, condition)?,
                            None => self.condition(condition)?,
                        };
                        if matches {
                            return self.statements(branch);
                        }
                    }
                }
                if let Some(otherwise) = otherwise {
                    return self.statements(otherwise);
                }
                let mut err = error("20000", "case not found");
                err.hint = Some("CASE statement is missing ELSE part.".into());
                return Err(err);
            }
            Stmt::Loop {
                label,
                kind,
                statements,
            } => {
                return self.run_loop(label.as_deref(), kind, statements);
            }
            Stmt::LoopControl {
                exit,
                label,
                condition,
            } => {
                if let Some(condition) = condition
                    && !self.condition(condition)?
                {
                    return Ok(Flow::Next);
                }
                return Ok(if *exit {
                    Flow::Exit(label.clone())
                } else {
                    Flow::Continue(label.clone())
                });
            }
            Stmt::Return(value) => return self.return_value(value).map(Flow::Return),
            Stmt::Null => {}
            Stmt::Perform(sql) => {
                let result = self.run_sql(sql)?;
                self.row_count = result.row_count;
                self.set_found(!result.rows.is_empty() || result.row_count != 0);
            }
            Stmt::Raise(raise) => self.raise(raise)?,
            Stmt::Sql {
                sql,
                into,
                modifies,
            } => self.statement_sql(sql, into.as_ref(), *modifies)?,
            Stmt::Diagnostics(target) => {
                let count = i64::try_from(self.row_count)
                    .map_err(|_| error("22003", "ROW_COUNT is out of range for bigint"))?;
                self.assign(
                    target,
                    param(DataType::BigInt, Value::Int(count)),
                    "GET DIAGNOSTICS",
                )?;
            }
        }
        Ok(Flow::Next)
    }

    fn return_value(&mut self, value: &ReturnValue) -> Result<TriggerReturn> {
        match value {
            ReturnValue::Null => Ok(TriggerReturn::Null),
            ReturnValue::Value(ValueExpr::Record(name)) => {
                let record = self.record(name)?;
                self.return_row(
                    record.columns.as_deref().unwrap_or(&[]),
                    record.values.clone(),
                )
            }
            ReturnValue::Value(ValueExpr::Scalar(sql)) | ReturnValue::Row(sql) => {
                let result = self.run_sql(sql)?;
                if result.rows.len() > 1 {
                    return Err(error("21000", "query returned more than one row"));
                }
                let row = result.rows.into_iter().next();
                if !matches!(value, ReturnValue::Row(_)) && result.columns.len() == 1 {
                    if matches!(row.as_deref(), None | Some([Value::Null])) {
                        return Ok(TriggerReturn::Null);
                    }
                    return Err(error(
                        "42804",
                        "returned scalar value in function returning a composite type",
                    ));
                }
                self.return_row(&result.columns, row)
            }
        }
    }

    fn return_row(&self, columns: &[SpiColumn], row: Option<Vec<Value>>) -> Result<TriggerReturn> {
        let Some(row) = row else {
            return Ok(TriggerReturn::Null);
        };
        if self.ctx.level == TriggerLevel::Statement {
            return Ok(TriggerReturn::Null);
        }
        if row.len() != self.ctx.columns.len()
            || columns.len() != row.len()
            || columns
                .iter()
                .zip(&self.ctx.columns)
                .any(|(actual, expected)| actual.data_type != expected.data_type)
        {
            return Err(error(
                "42804",
                "returned row structure does not match the structure of the triggering table",
            ));
        }
        Ok(TriggerReturn::Row(row))
    }

    fn variable(&self, name: &str) -> Result<&Variable> {
        self.scopes
            .iter()
            .rev()
            .find_map(|scope| scope.get(name))
            .ok_or_else(|| error("42703", format!("variable \"{name}\" does not exist")))
    }

    fn variable_mut(&mut self, name: &str) -> &mut Variable {
        self.scopes
            .iter_mut()
            .rev()
            .find_map(|scope| scope.get_mut(name))
            .expect("validated PL/pgSQL variable")
    }

    fn record(&self, name: &str) -> Result<&Record> {
        match &self.variable(name)?.datum {
            Datum::Record(record) => Ok(record),
            Datum::Scalar(_) => Err(error(
                "42804",
                format!("variable \"{name}\" is not a record"),
            )),
        }
    }

    fn field(&self, name: &str, field: &str) -> Result<(&Record, usize, &SpiColumn)> {
        let record = self.record(name)?;
        let columns = record.columns.as_deref().ok_or_else(|| {
            let mut err = error("55000", format!("record \"{name}\" is not assigned yet"));
            err.detail =
                Some("The tuple structure of a not-yet-assigned record is indeterminate.".into());
            err
        })?;
        columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.name == field)
            .map(|(i, column)| (record, i, column))
            .ok_or_else(|| {
                error(
                    "42703",
                    format!("record \"{name}\" has no field \"{field}\""),
                )
            })
    }

    fn reference(&mut self, reference: &Reference) -> Result<SpiParam> {
        match reference {
            Reference::Scalar(name) => match &self.variable(name)?.datum {
                Datum::Scalar(value) => Ok(value.clone()),
                Datum::Record(_) => Err(error("42804", "record variable used as a scalar")),
            },
            Reference::Field(name, field) => {
                let (record, i, column) = self.field(name, field)?;
                let value = record
                    .values
                    .as_ref()
                    .and_then(|row| row.get(i))
                    .cloned()
                    .unwrap_or(Value::Null);
                Ok(param(column.data_type.clone(), value))
            }
            Reference::Arg(index) => {
                let index = match index {
                    ArgIndex::Literal(index) => Some(*index),
                    ArgIndex::Expression(sql) => self.integer(sql)?,
                };
                let value = index
                    .and_then(|i| usize::try_from(i).ok())
                    .and_then(|i| self.ctx.arguments.get(i))
                    .cloned()
                    .map_or(Value::Null, Value::Text);
                Ok(param(DataType::Text, value))
            }
            Reference::RecordNull { name, negated } => {
                let values = self.record(name)?.values.as_deref();
                let is_null = if *negated {
                    values.is_some_and(|row| row.iter().all(|value| *value != Value::Null))
                } else {
                    values.is_none_or(|row| row.iter().all(|value| *value == Value::Null))
                };
                Ok(param(DataType::Boolean, Value::Bool(is_null)))
            }
        }
    }

    fn run_sql(&mut self, sql: &Sql) -> Result<SpiResult> {
        let params = self.params(sql)?;
        self.execute(&sql.text, &params, &sql.original)
    }

    fn params(&mut self, sql: &Sql) -> Result<Vec<SpiParam>> {
        sql.params
            .iter()
            .map(|reference| {
                self.reference(reference).map_err(|err| {
                    if err.query.is_some() {
                        err
                    } else {
                        err.with_query(&sql.original)
                    }
                })
            })
            .collect()
    }

    fn execute(&mut self, sql: &str, params: &[SpiParam], original: &str) -> Result<SpiResult> {
        self.spi
            .execute(sql, params)
            .map_err(|err| PlRuntimeError::from_spi(err, original))
    }

    fn eval(&mut self, sql: &Sql) -> Result<SpiParam> {
        scalar_result(self.run_sql(sql)?).map_err(|err| err.with_query(&sql.original))
    }

    fn condition(&mut self, sql: &Sql) -> Result<bool> {
        let value = self.eval(sql)?;
        self.truth(value, &sql.original)
    }

    fn truth(&mut self, value: SpiParam, query: &str) -> Result<bool> {
        let value = self.cast(value, &DataType::Boolean, "boolean", false, query)?;
        match value.value {
            Value::Bool(value) => Ok(value),
            Value::Null => Ok(false),
            _ => Err(error("XX000", "SPI boolean result has an unexpected type")),
        }
    }

    fn integer(&mut self, sql: &Sql) -> Result<Option<i64>> {
        let value = self.eval(sql)?;
        let value = self.cast(value, &DataType::Integer, "integer", false, &sql.original)?;
        match value.value {
            Value::Int(value) => Ok(Some(value)),
            Value::Null => Ok(None),
            _ => Err(error("XX000", "SPI integer result has an unexpected type")),
        }
    }

    fn case_matches(&mut self, value: &SpiParam, condition: &Sql) -> Result<bool> {
        // Evaluate the selector once. Keep the WHEN expression in the same SQL
        // comparison so PostgreSQL's coercion rules apply to its literals.
        let mut params = self.params(condition)?;
        params.push(value.clone());
        let result = self.execute(&condition.text, &params, &condition.original)?;
        self.truth(scalar_result(result)?, &condition.original)
    }

    fn statement_sql(
        &mut self,
        sql: &Sql,
        into: Option<&SelectInto>,
        modifies: bool,
    ) -> Result<()> {
        let mut result = self.run_sql(sql)?;
        self.row_count = result.row_count;
        if let Some(into) = into {
            self.set_found(!result.rows.is_empty());
            if into.strict && result.rows.is_empty() {
                return Err(error("P0002", "query returned no rows"));
            }
            if (into.strict || modifies) && result.rows.len() > 1 {
                return Err(error("P0003", "query returned more than one row"));
            }
            let row = result.rows.drain(..).next();
            if let [target] = into.targets.as_slice()
                && target.field.is_none()
                && matches!(self.variable(&target.name)?.datum, Datum::Record(_))
            {
                return self.assign_record(target, Some(result.columns.into()), row);
            }
            let mut row = row.unwrap_or_default().into_iter();
            let mut columns = result.columns.into_iter();
            for target in &into.targets {
                let value = row.next().unwrap_or(Value::Null);
                let ty = columns.next().map_or(DataType::Text, |col| col.data_type);
                self.assign(target, param(ty, value), &sql.original)?;
            }
        } else {
            if modifies {
                self.set_found(result.row_count != 0);
            }
            if !result.columns.is_empty() || !result.rows.is_empty() {
                let mut err = error("42601", "query has no destination for result data");
                if !modifies {
                    err.hint = Some(
                        "If you want to discard the results of a SELECT, use PERFORM instead."
                            .into(),
                    );
                }
                return Err(err);
            }
        }
        Ok(())
    }

    fn assign_record(
        &mut self,
        target: &Target,
        columns: Option<Arc<[SpiColumn]>>,
        row: Option<Vec<Value>>,
    ) -> Result<()> {
        let variable = self.variable_mut(&target.name);
        if variable.constant {
            return Err(error(
                "22005",
                format!("variable \"{}\" is declared CONSTANT", target.name),
            ));
        }
        if target.field.is_some() || !matches!(variable.datum, Datum::Record(_)) {
            return Err(error(
                "42804",
                "cannot assign a record to a scalar variable",
            ));
        }
        let Datum::Record(record) = &mut variable.datum else {
            unreachable!()
        };
        if record.fixed_shape
            && let Some((columns, expected)) = columns.as_deref().zip(record.columns.as_deref())
            && (columns.len() != expected.len()
                || columns
                    .iter()
                    .zip(expected)
                    .any(|(actual, expected)| actual.data_type != expected.data_type))
        {
            return Err(error(
                "42804",
                "query result does not match the record structure",
            ));
        }
        if variable.not_null && row.is_none() {
            Self::check_null(true, &Value::Null, &target.name)?;
        }
        if !record.fixed_shape {
            record.columns = columns;
        }
        record.values = row;
        Ok(())
    }

    fn check_record_null(not_null: bool, record: &Record, name: &str) -> Result<()> {
        if record.values.is_none() {
            Self::check_null(not_null, &Value::Null, name)?;
        }
        Ok(())
    }

    fn destination(&self, target: &Target) -> Result<(Option<usize>, ScalarType, bool)> {
        let variable = self.variable(&target.name)?;
        if variable.constant {
            return Err(error(
                "22005",
                format!("variable \"{}\" is declared CONSTANT", target.name),
            ));
        }
        if let Some(field) = &target.field {
            let (_, index, column) = self.field(&target.name, field)?;
            return Ok((Some(index), field_type(column.data_type.clone()), false));
        }
        let Datum::Scalar(value) = &variable.datum else {
            return Err(error(
                "42804",
                "cannot assign non-composite value to a record variable",
            ));
        };
        Ok((
            None,
            variable
                .ty
                .clone()
                .unwrap_or_else(|| field_type(value.data_type.clone())),
            variable.not_null,
        ))
    }

    fn assign(&mut self, target: &Target, value: SpiParam, query: &str) -> Result<()> {
        if target.field.is_none()
            && value.value == Value::Null
            && matches!(self.variable(&target.name)?.datum, Datum::Record(_))
        {
            let variable = self.variable_mut(&target.name);
            if variable.constant {
                return Err(error(
                    "22005",
                    format!("variable \"{}\" is declared CONSTANT", target.name),
                ));
            }
            Self::check_null(variable.not_null, &value.value, &target.name)?;
            let Datum::Record(record) = &mut variable.datum else {
                unreachable!()
            };
            record.values = None;
            return Ok(());
        }
        let (index, ty, not_null) = self.destination(target)?;
        let value = self.cast_to_decl(value, &ty, query)?;
        Self::check_null(not_null, &value.value, &target.name)?;
        let variable = self.variable_mut(&target.name);
        match &mut variable.datum {
            Datum::Scalar(current) => {
                *current = SpiParam {
                    type_oid: current.type_oid,
                    ..value
                }
            }
            Datum::Record(record) => {
                let columns = record
                    .columns
                    .as_ref()
                    .expect("validated record descriptor");
                let row = record
                    .values
                    .get_or_insert_with(|| vec![Value::Null; columns.len()]);
                let cell = row
                    .get_mut(index.expect("validated record field"))
                    .ok_or_else(|| error("XX000", "trigger record has the wrong width"))?;
                *cell = value.value;
            }
        }
        Ok(())
    }

    fn check_null(not_null: bool, value: &Value, name: &str) -> Result<()> {
        if not_null && *value == Value::Null {
            Err(error(
                "22004",
                format!("null value cannot be assigned to variable \"{name}\" declared NOT NULL"),
            ))
        } else {
            Ok(())
        }
    }

    fn cast_to_decl(&mut self, value: SpiParam, ty: &ScalarType, query: &str) -> Result<SpiParam> {
        self.cast(value, &ty.data_type, &ty.spelling, ty.has_modifier, query)
    }

    fn cast(
        &mut self,
        value: SpiParam,
        ty: &DataType,
        spelling: &str,
        has_modifier: bool,
        query: &str,
    ) -> Result<SpiParam> {
        if value.value == Value::Null || (value.data_type == *ty && !has_modifier) {
            return Ok(param(ty.clone(), value.value));
        }
        let sql = format!("SELECT CAST($1 AS {spelling})");
        let result = match self.execute(&sql, std::slice::from_ref(&value), query) {
            Err(err) if err.sqlstate.as_ref() == "42846" && value.data_type != DataType::Text => {
                let output = format_value(&value.value)?.unwrap_or_default();
                self.execute(&sql, &[text(output)], query)?
            }
            result => result?,
        };
        let converted = scalar_result(result)?;
        Ok(param(ty.clone(), converted.value))
    }

    fn set_found(&mut self, found: bool) {
        self.scopes[0]
            .get_mut("found")
            .expect("FOUND variable")
            .datum = Datum::Scalar(param(DataType::Boolean, Value::Bool(found)));
    }

    fn run_loop(
        &mut self,
        label: Option<&str>,
        kind: &LoopKind,
        statements: &[Statement],
    ) -> Result<Flow> {
        match kind {
            LoopKind::Integer(for_loop) => self.integer_for(label, for_loop, statements),
            LoopKind::Plain | LoopKind::While(_) => loop {
                check_cancelled()?;
                if let LoopKind::While(sql) = kind
                    && !self.condition(sql)?
                {
                    return Ok(Flow::Next);
                }
                if let Some(flow) = loop_flow(self.statements(statements)?, label) {
                    return Ok(flow);
                }
            },
        }
    }

    fn integer_for(
        &mut self,
        label: Option<&str>,
        cfg: &IntegerFor,
        body: &[Statement],
    ) -> Result<Flow> {
        let mut index = self
            .integer(&cfg.lower)?
            .ok_or_else(|| error("22004", "lower bound of FOR loop cannot be null"))?;
        let stop = self
            .integer(&cfg.upper)?
            .ok_or_else(|| error("22004", "upper bound of FOR loop cannot be null"))?;
        let step = match &cfg.step {
            Some(sql) => self
                .integer(sql)?
                .ok_or_else(|| error("22004", "BY value of FOR loop cannot be null"))?,
            None => 1,
        };
        if step <= 0 {
            return Err(error(
                "22023",
                "BY value of FOR loop must be greater than zero",
            ));
        }
        let step = if cfg.reverse { -step } else { step };
        let mut found = false;
        self.scopes.push(HashMap::from([(
            cfg.name.clone(),
            scalar(param(DataType::Integer, Value::Null)),
        )]));
        let result = (|| {
            while if cfg.reverse {
                index >= stop
            } else {
                index <= stop
            } {
                check_cancelled()?;
                found = true;
                self.variable_mut(&cfg.name).datum =
                    Datum::Scalar(param(DataType::Integer, Value::Int(index)));
                if let Some(flow) = loop_flow(self.statements(body)?, label) {
                    return Ok(flow);
                }
                index += step;
            }
            Ok(Flow::Next)
        })();
        self.scopes.pop();
        self.set_found(found);
        result
    }

    fn raise(&mut self, raise: &Raise) -> Result<()> {
        let mut chars = raise.format.chars().peekable();
        let mut args = raise.args.iter();
        let mut message = String::new();
        while let Some(ch) = chars.next() {
            if ch != '%' || chars.next_if_eq(&'%').is_some() {
                message.push(ch);
            } else {
                let arg = self.eval(args.next().expect("validated RAISE arguments"))?;
                message.push_str(format_value(&arg.value)?.as_deref().unwrap_or("<NULL>"));
            }
        }
        let mut code = match raise.level {
            RaiseLevel::Exception => "P0001",
            RaiseLevel::Warning => "01000",
            _ => "00000",
        }
        .into();
        let mut detail = None;
        let mut hint = None;
        for (option, sql) in &raise.options {
            let value = self.eval(sql)?;
            let value = format_value(&value.value)?
                .ok_or_else(|| error("22004", "RAISE statement option cannot be null"))?;
            match option {
                RaiseOption::Code => {
                    if value.len() != 5
                        || !value
                            .bytes()
                            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
                    {
                        return Err(error(
                            "42704",
                            format!("unrecognized exception condition \"{value}\""),
                        ));
                    }
                    code = value;
                }
                RaiseOption::Message => message = value,
                RaiseOption::Detail => detail = Some(value.into_boxed_str()),
                RaiseOption::Hint => hint = Some(value.into_boxed_str()),
            }
        }
        if raise.level == RaiseLevel::Exception {
            let mut err = error(&code, message);
            err.detail = detail;
            err.hint = hint;
            return Err(err);
        }
        self.notices.push(PlNotice {
            level: raise.level,
            sqlstate: code.into(),
            message: message.into(),
            detail,
            hint,
        });
        Ok(())
    }
}

fn check_cancelled() -> Result<()> {
    if let Some(cancel) = crate::Cancellation::current()
        && cancel.check().is_err()
    {
        Err(error("57014", "canceling statement due to user request"))
    } else {
        Ok(())
    }
}

fn loop_flow(flow: Flow, label: Option<&str>) -> Option<Flow> {
    match flow {
        Flow::Next => None,
        Flow::Continue(target) if target.is_none() || target.as_deref() == label => None,
        Flow::Exit(target) if target.is_none() || target.as_deref() == label => Some(Flow::Next),
        other => Some(other),
    }
}

fn scalar_result(mut result: SpiResult) -> Result<SpiParam> {
    if result.columns.len() != 1 {
        return Err(error(
            "42601",
            format!("query returned {} columns", result.columns.len()),
        ));
    }
    if result.rows.len() > 1 {
        return Err(error("21000", "query returned more than one row"));
    }
    let value = match result.rows.pop() {
        None => Value::Null,
        Some(mut row) if row.len() == 1 => row.pop().unwrap(),
        Some(_) => return Err(error("XX000", "SPI result has the wrong width")),
    };
    Ok(param(result.columns.pop().unwrap().data_type, value))
}

fn format_value(value: &Value) -> Result<Option<String>> {
    output_text(value).map_err(|error_code| {
        error(
            crate::sqlstate(&CoreError::Sql(error_code)),
            "could not format PL/pgSQL value",
        )
    })
}

fn field_type(data_type: DataType) -> ScalarType {
    let spelling = match data_type {
        DataType::Boolean => "boolean",
        DataType::SmallInt => "smallint",
        DataType::Integer => "integer",
        DataType::BigInt => "bigint",
        DataType::Text => "text",
        DataType::Bytes => "bytea",
        DataType::Numeric => "numeric",
        DataType::Real => "real",
        DataType::DoublePrecision => "double precision",
        DataType::Date => "date",
        DataType::Time => "time",
        DataType::TimeTz => "timetz",
        DataType::Timestamp => "timestamp",
        DataType::TimestampTz => "timestamptz",
        DataType::Interval => "interval",
        DataType::Varchar => "varchar",
        DataType::Bpchar => "bpchar",
        DataType::Uuid => "uuid",
        DataType::Jsonb => "jsonb",
        DataType::IntArray => "integer[]",
        DataType::BigIntArray => "bigint[]",
        DataType::TextArray => "text[]",
        DataType::RegType => "regtype",
        DataType::ExpressionArray(array) => array.metadata().2,
        DataType::UuidArray => "uuid[]",
        DataType::TsVector => "tsvector",
        DataType::TsQuery => "tsquery",
        DataType::RegConfig => "regconfig",
        DataType::RegDictionary => "regdictionary",
        DataType::Oid => "oid",
        DataType::PgChar => "\"char\"",
        DataType::RealArray => "real[]",
        DataType::PgCharArray => "\"char\"[]",
        DataType::SmallIntArray => "smallint[]",
        DataType::RegDictionaryArray => "regdictionary[]",
    };
    ScalarType {
        data_type,
        spelling: spelling.into(),
        has_modifier: false,
    }
}

impl Stmt {
    fn action(&self) -> &'static str {
        match self {
            Self::Block(_) => "statement block",
            Self::Assign(..) => "assignment",
            Self::If(..) => "IF",
            Self::Case { .. } => "CASE",
            Self::Loop {
                kind: LoopKind::While(_),
                ..
            } => "WHILE",
            Self::Loop {
                kind: LoopKind::Integer(_),
                ..
            } => "FOR with integer loop variable",
            Self::Loop { .. } => "LOOP",
            Self::LoopControl { exit: true, .. } => "EXIT",
            Self::LoopControl { .. } => "CONTINUE",
            Self::Return(_) => "RETURN",
            Self::Null => "NULL",
            Self::Perform(_) => "PERFORM",
            Self::Raise(_) => "RAISE",
            Self::Sql { .. } => "SQL statement",
            Self::Diagnostics(_) => "GET DIAGNOSTICS",
        }
    }
}
