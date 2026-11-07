//! Bounded SQL multi-row INSERT and ON CONFLICT frontend.
use super::*;
use crate::operations::{self, ConflictAction, ConflictSource};

#[derive(Debug, Clone)]
pub(super) struct Insert {
    pub table: String,
    columns: Vec<String>,
    rows: Vec<Vec<Expr>>,
    conflict: Option<Conflict>,
    pub(super) returning: Option<returning::Returning>,
}
#[derive(Debug, Clone)]
struct Conflict {
    columns: Option<Vec<String>>,
    constraint: Option<String>,
    update: Option<Update>,
}
#[derive(Debug, Clone)]
struct Update {
    sets: Vec<(String, Operand)>,
    guard: Option<Condition>,
}
#[derive(Debug, Clone)]
enum Operand {
    Value(Expr),
    Field(ConflictSource, String),
}
#[derive(Debug, Clone)]
enum Condition {
    Operand(Operand),
    Compare(Operand, operations::Compare, Operand),
    IsNull(Operand, bool),
    Not(Box<Condition>),
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
}

/// Claim only multiple VALUES tuples or a top-level ON CONFLICT clause.
pub(super) fn claims(tokens: &[Token]) -> bool {
    let mut depth = 0usize;
    let mut values_at = None;
    for (index, token) in tokens.iter().enumerate() {
        match token {
            Token::Word(word) if depth == 0 && word == "on" => {
                if matches!(tokens.get(index + 1), Some(Token::Word(word)) if word == "conflict") {
                    return true;
                }
            }
            // INSERT INTO target occupies the first three tokens. The target
            // itself may legally be named values.
            Token::Word(word) if depth == 0 && index >= 3 && word == "values" => {
                values_at.get_or_insert(index);
            }
            Token::Symbol('(') => depth += 1,
            Token::Symbol(')') => depth = depth.saturating_sub(1),
            Token::Symbol(';') if depth == 0 => break,
            _ => {}
        }
    }
    let Some(mut at) = values_at.map(|index| index + 1) else {
        return false;
    };
    let mut tuples = 0usize;
    while tokens.get(at) == Some(&Token::Symbol('(')) {
        tuples += 1;
        if tuples >= 2 {
            return true;
        }
        depth = 1;
        at += 1;
        while depth != 0 {
            match tokens.get(at) {
                Some(Token::Symbol('(')) => depth += 1,
                Some(Token::Symbol(')')) => depth -= 1,
                Some(_) => {}
                None => return false,
            }
            at += 1;
        }
        if tokens.get(at) != Some(&Token::Symbol(',')) {
            break;
        }
        at += 1;
    }
    false
}

fn names(p: &mut Parser) -> R<Vec<String>> {
    p.punct('(')?;
    let mut names = vec![p.name()?];
    while p.sym(',') {
        names.push(p.name()?);
    }
    p.punct(')')?;
    Ok(names)
}
fn value(p: &mut Parser) -> R<Expr> {
    if matches!(
        p.tokens.get(p.at),
        None | Some(Token::Symbol(')' | ',' | ';'))
    ) {
        return Err(SqlError::Syntax);
    }
    p.expr()
}
fn unsupported_or_syntax(p: &Parser) -> SqlError {
    if matches!(p.tokens.get(p.at), None | Some(Token::Symbol(';' | ')'))) {
        SqlError::Syntax
    } else {
        SqlError::Unsupported
    }
}
pub(super) fn parse(p: &mut Parser) -> R<Insert> {
    p.need("insert")?;
    p.need("into")?;
    let table = p.table_name()?;
    let columns = if p.tokens.get(p.at) == Some(&Token::Symbol('(')) {
        names(p)?
    } else {
        vec![]
    };
    if !p.word("values") {
        return Err(unsupported_or_syntax(p));
    }
    let mut rows = Vec::new();
    loop {
        p.punct('(')?;
        let mut row = vec![value(p)?];
        while p.sym(',') {
            row.push(value(p)?);
        }
        if !p.sym(')') {
            return Err(unsupported_or_syntax(p));
        }
        rows.push(row);
        if rows.len() > MAX_ROWS {
            return Err(SqlError::Limit);
        }
        if !p.sym(',') {
            break;
        }
    }
    let conflict = if p.word("on") {
        p.need("conflict")?;
        let mut columns = None;
        let mut constraint = None;
        if p.tokens.get(p.at) == Some(&Token::Symbol('(')) {
            columns = Some(names(p)?);
        } else if p.word("on") {
            p.need("constraint")?;
            constraint = Some(p.name()?);
        }
        p.need("do")?;
        let update = if p.word("nothing") {
            None
        } else {
            p.need("update")?;
            if columns.is_none() && constraint.is_none() {
                return Err(SqlError::Syntax);
            }
            p.need("set")?;
            let mut sets = Vec::new();
            loop {
                let column = p.name()?;
                if p.tokens.get(p.at) != Some(&Token::Op("=".into())) {
                    return Err(SqlError::Syntax);
                }
                p.at += 1;
                sets.push((column, operand(p, &table)?));
                if !p.sym(',') {
                    break;
                }
            }
            let guard = if p.word("where") {
                Some(condition(p, &table, 0)?)
            } else {
                None
            };
            Some(Update { sets, guard })
        };
        Some(Conflict {
            columns,
            constraint,
            update,
        })
    } else {
        None
    };
    let returning = returning::Returning::parse(p, &table)?;
    p.sym(';');
    if p.at != p.tokens.len() {
        return Err(unsupported_or_syntax(p));
    }
    Ok(Insert {
        table,
        columns,
        rows,
        conflict,
        returning,
    })
}
fn operand(p: &mut Parser, table: &str) -> R<Operand> {
    if matches!(p.tokens.get(p.at), Some(Token::Word(word)) if word == "null" || word == "true" || word == "false")
        || matches!(
            p.tokens.get(p.at),
            Some(Token::String(_) | Token::Number(_) | Token::Param(_))
        )
        || (matches!(p.tokens.get(p.at), Some(Token::Op(sign)) if sign == "+" || sign == "-")
            && matches!(p.tokens.get(p.at + 1), Some(Token::Number(_))))
    {
        return value(p).map(Operand::Value);
    }
    if p.word("default") {
        return Err(SqlError::Unsupported);
    }
    let (relation, field) = p.qualified_name()?;
    let source = match relation.as_deref() {
        Some("excluded") => ConflictSource::Incoming,
        Some(name) if name == table => ConflictSource::Existing,
        Some(_) => return Err(SqlError::Unsupported),
        None => ConflictSource::Existing,
    };
    Ok(Operand::Field(source, field))
}
fn condition(p: &mut Parser, table: &str, depth: usize) -> R<Condition> {
    let mut left = conjunction(p, table, depth)?;
    while p.word("or") {
        left = Condition::Or(Box::new(left), Box::new(conjunction(p, table, depth)?));
    }
    Ok(left)
}
fn conjunction(p: &mut Parser, table: &str, depth: usize) -> R<Condition> {
    let mut left = predicate(p, table, depth)?;
    while p.word("and") {
        left = Condition::And(Box::new(left), Box::new(predicate(p, table, depth)?));
    }
    Ok(left)
}
fn predicate(p: &mut Parser, table: &str, depth: usize) -> R<Condition> {
    if depth > operations::MAX_DEPTH {
        return Err(SqlError::Limit);
    }
    if p.word("not") {
        return Ok(Condition::Not(Box::new(predicate(p, table, depth + 1)?)));
    }
    if p.sym('(') {
        let expression = condition(p, table, depth + 1)?;
        p.punct(')')?;
        return Ok(expression);
    }
    let left = operand(p, table)?;
    if p.word("is") {
        let not = p.word("not");
        p.need("null")?;
        return Ok(Condition::IsNull(left, not));
    }
    if let Some(Token::Op(operator)) = p.tokens.get(p.at) {
        let mut operator = operator.clone();
        p.at += 1;
        if operator == "<" && p.tokens.get(p.at) == Some(&Token::Op(">".into())) {
            operator = "<>".into();
            p.at += 1;
        }
        let operator = match operator.as_str() {
            "=" => operations::Compare::Eq,
            "<>" => operations::Compare::Ne,
            "<" => operations::Compare::Lt,
            "<=" => operations::Compare::Le,
            ">" => operations::Compare::Gt,
            ">=" => operations::Compare::Ge,
            _ => return Err(SqlError::Unsupported),
        };
        return Ok(Condition::Compare(left, operator, operand(p, table)?));
    }
    Ok(Condition::Operand(left))
}

fn indexes(insert: &Insert, table: &TableDef) -> R<Vec<usize>> {
    let indexes = if insert.columns.is_empty() {
        table.logical_offsets()
    } else {
        insert
            .columns
            .iter()
            .map(|name| col(table, name))
            .collect::<R<Vec<_>>>()?
    };
    unique(&indexes)?;
    if insert.rows.iter().any(|row| row.len() != indexes.len()) {
        return Err(SqlError::Type);
    }
    Ok(indexes)
}
fn infer_value(expression: &Expr, ty: &DataType, parameters: &mut [Option<DataType>]) -> R<()> {
    if let Expr::Param(number) = expression {
        let slot = parameters.get_mut(number - 1).ok_or(SqlError::Parameters)?;
        if slot.as_ref().is_some_and(|old| old != ty) {
            return Err(SqlError::Type);
        }
        *slot = Some(ty.clone());
    }
    Ok(())
}
fn operand_type(operand: &Operand, table: &TableDef) -> R<Option<DataType>> {
    match operand {
        Operand::Field(_, name) => Ok(Some(table.columns[col(table, name)?].data_type.clone())),
        Operand::Value(_) => Ok(None),
    }
}
fn infer_operand(
    operand: &Operand,
    expected: Option<&DataType>,
    table: &TableDef,
    parameters: &mut [Option<DataType>],
) -> R<()> {
    match operand {
        Operand::Value(expression) => {
            if let Some(expected) = expected {
                infer_value(expression, expected, parameters)?;
            }
        }
        Operand::Field(_, name) => {
            col(table, name)?;
        }
    }
    Ok(())
}
fn infer_condition(
    condition: &Condition,
    table: &TableDef,
    parameters: &mut [Option<DataType>],
) -> R<()> {
    match condition {
        Condition::Compare(left, _, right) => {
            let left_type = operand_type(left, table)?;
            let right_type = operand_type(right, table)?;
            infer_operand(left, right_type.as_ref(), table, parameters)?;
            infer_operand(right, left_type.as_ref(), table, parameters)?;
        }
        Condition::Operand(operand) => {
            infer_operand(operand, Some(&DataType::Boolean), table, parameters)?
        }
        Condition::IsNull(operand, _) => infer_operand(operand, None, table, parameters)?,
        Condition::Not(inner) => infer_condition(inner, table, parameters)?,
        Condition::And(left, right) | Condition::Or(left, right) => {
            infer_condition(left, table, parameters)?;
            infer_condition(right, table, parameters)?;
        }
    }
    Ok(())
}
impl Insert {
    pub(super) fn describe(&self, state: &Relational, count: usize) -> R<Description> {
        let table = state
            .tables
            .get(&self.table)
            .ok_or(SqlError::UnknownTable)?;
        let indexes = indexes(self, table)?;
        let mut parameters = vec![None; count];
        for row in &self.rows {
            for (expression, index) in row.iter().zip(&indexes) {
                infer_value(
                    expression,
                    &table.columns[*index].data_type,
                    &mut parameters,
                )?;
            }
        }
        if let Some(Conflict {
            update: Some(update),
            ..
        }) = &self.conflict
        {
            let mut seen = BTreeSet::new();
            for (name, operand) in &update.sets {
                let index = col(table, name)?;
                if !seen.insert(index) {
                    return Err(SqlError::Duplicate);
                }
                if table.primary_key.contains(&index) {
                    return Err(SqlError::Unsupported);
                }
                infer_operand(
                    operand,
                    Some(&table.columns[index].data_type),
                    table,
                    &mut parameters,
                )?;
                if let Operand::Value(expression @ Expr::Literal(_)) = operand {
                    let value = resolved_value(expression, &[], &table.columns[index].data_type)?;
                    let mut column = table.columns[index].clone();
                    column.not_null = false;
                    check(&value, &column)?;
                }
                if let Some(ty) = operand_type(operand, table)?
                    && !same_family(&ty, &table.columns[index].data_type)
                {
                    return Err(SqlError::Type);
                }
            }
            if let Some(guard) = &update.guard {
                infer_condition(guard, table, &mut parameters)?;
            }
        }
        if let Some(returning) = &self.returning {
            let (columns, parameters) = returning.describe(state, parameters)?;
            return Ok(Description {
                columns,
                parameters,
            });
        }
        Ok(Description {
            parameters: parameters
                .into_iter()
                .map(|ty| ty.ok_or(SqlError::Parameters))
                .collect::<R<Vec<_>>>()?,
            columns: vec![],
        })
    }
}
fn same_family(left: &DataType, right: &DataType) -> bool {
    let integer = |ty: &DataType| {
        matches!(
            ty,
            DataType::SmallInt | DataType::Integer | DataType::BigInt
        )
    };
    left == right || integer(left) && integer(right)
}
fn resolved_value(expression: &Expr, params: &[Value], ty: &DataType) -> R<Value> {
    let value = expression.value(params)?;
    if matches!(expression, Expr::Literal(_))
        && (matches!(
            ty,
            DataType::Numeric | DataType::Real | DataType::DoublePrecision
        ) || ty.is_temporal())
    {
        query::coerce(value, ty)
    } else {
        Ok(value)
    }
}
fn lower_operand(
    operand: &Operand,
    table: &TableDef,
    params: &[Value],
    expected: Option<&DataType>,
) -> R<operations::Expr> {
    Ok(match operand {
        Operand::Field(source, field) => {
            col(table, field)?;
            operations::Expr::ConflictField {
                source: *source,
                field: field.clone(),
            }
        }
        Operand::Value(expression) => {
            let value = match expected {
                Some(ty) => resolved_value(expression, params, ty)?,
                None => expression.value(params)?,
            };
            operations::Expr::Value(operations::ValueExpr::Literal(value))
        }
    })
}
fn lower_condition(
    condition: &Condition,
    table: &TableDef,
    params: &[Value],
) -> R<operations::Expr> {
    Ok(match condition {
        Condition::Operand(operand) => {
            lower_operand(operand, table, params, Some(&DataType::Boolean))?
        }
        Condition::Compare(left, operator, right) => operations::Expr::Compare(
            Box::new(lower_operand(
                left,
                table,
                params,
                operand_type(right, table)?.as_ref(),
            )?),
            *operator,
            Box::new(lower_operand(
                right,
                table,
                params,
                operand_type(left, table)?.as_ref(),
            )?),
        ),
        Condition::IsNull(operand, not) => {
            operations::Expr::IsNull(Box::new(lower_operand(operand, table, params, None)?), *not)
        }
        Condition::Not(inner) => {
            operations::Expr::Not(Box::new(lower_condition(inner, table, params)?))
        }
        Condition::And(left, right) => operations::Expr::And(
            Box::new(lower_condition(left, table, params)?),
            Box::new(lower_condition(right, table, params)?),
        ),
        Condition::Or(left, right) => operations::Expr::Or(
            Box::new(lower_condition(left, table, params)?),
            Box::new(lower_condition(right, table, params)?),
        ),
    })
}
/// Allocation-free raw byte admission with secondary_admit_values work accounting.
/// Rows have separate byte budgets; the whole input shares byte/work budgets.
#[derive(Default)]
struct InputAdmission {
    total: usize,
    row_bytes: usize,
    work: usize,
}
impl InputAdmission {
    fn row(&mut self) {
        self.row_bytes = 0;
    }
    fn value(&mut self, value: &Value) -> R<()> {
        let raw = match value {
            Value::Text(value) => value.len(),
            Value::Bytes(value) => value.len(),
            Value::Numeric(value) => value.display_byte_bound(),
            Value::Interval(_) => 16,
            Value::TimeTz(_) => 12,
            _ => 9,
        };
        self.total = self.total.checked_add(raw).ok_or(SqlError::Limit)?;
        self.row_bytes = self.row_bytes.checked_add(raw).ok_or(SqlError::Limit)?;
        self.work = self.work.checked_add(raw).ok_or(SqlError::Limit)?;
        if matches!(value, Value::Numeric(_)) {
            let numeric_work = raw
                .checked_mul(raw)
                .and_then(|work| work.checked_mul(8))
                .ok_or(SqlError::Limit)?;
            self.work = self.work.checked_add(numeric_work).ok_or(SqlError::Limit)?;
        }
        if self.row_bytes > MAX_ROW_BYTES
            || self.total > operations::MAX_INSERT_BYTES
            || self.work > secondary_index::MAX_WORK
        {
            return Err(SqlError::Limit);
        }
        Ok(())
    }
}
fn borrowed_value<'a>(expression: &'a Expr, params: &'a [Value]) -> R<&'a Value> {
    match expression {
        Expr::Literal(value) => Ok(value),
        Expr::Param(number) => params.get(number - 1).ok_or(SqlError::Parameters),
    }
}
fn admit_operand(operand: &Operand, params: &[Value], admission: &mut InputAdmission) -> R<()> {
    if let Operand::Value(expression) = operand {
        admission.value(borrowed_value(expression, params)?)?;
    }
    Ok(())
}
fn admit_condition(
    condition: &Condition,
    params: &[Value],
    admission: &mut InputAdmission,
    depth: usize,
) -> R<()> {
    if depth > operations::MAX_DEPTH {
        return Err(SqlError::Limit);
    }
    match condition {
        Condition::Operand(operand) | Condition::IsNull(operand, _) => {
            admit_operand(operand, params, admission)?
        }
        Condition::Compare(left, _, right) => {
            admit_operand(left, params, admission)?;
            admit_operand(right, params, admission)?;
        }
        Condition::Not(inner) => admit_condition(inner, params, admission, depth + 1)?,
        Condition::And(left, right) | Condition::Or(left, right) => {
            admit_condition(left, params, admission, depth + 1)?;
            admit_condition(right, params, admission, depth + 1)?;
        }
    }
    Ok(())
}
fn admit_statement(insert: &Insert, params: &[Value]) -> R<()> {
    if insert.rows.len() > MAX_ROWS {
        return Err(SqlError::Limit);
    }
    if insert.columns.len() > MAX_COLUMNS {
        return Err(SqlError::Type);
    }
    let mut admission = InputAdmission::default();
    for row in &insert.rows {
        if row.len() > MAX_COLUMNS
            || !insert.columns.is_empty() && row.len() != insert.columns.len()
        {
            return Err(SqlError::Type);
        }
        admission.row();
        for expression in row {
            admission.value(borrowed_value(expression, params)?)?;
        }
    }
    if let Some(Conflict {
        update: Some(update),
        ..
    }) = &insert.conflict
    {
        if update.sets.len() > MAX_COLUMNS {
            return Err(SqlError::Limit);
        }
        admission.row();
        for (_, operand) in &update.sets {
            admit_operand(operand, params, &mut admission)?;
        }
        if let Some(condition) = &update.guard {
            admit_condition(condition, params, &mut admission, 0)?;
        }
    }
    Ok(())
}
fn admit_engine_expression(
    expression: &operations::Expr,
    admission: &mut InputAdmission,
    depth: usize,
) -> R<()> {
    if depth > operations::MAX_DEPTH {
        return Err(SqlError::Limit);
    }
    match expression {
        operations::Expr::Value(operations::ValueExpr::Literal(value)) => admission.value(value)?,
        operations::Expr::Value(operations::ValueExpr::Parameter(_)) => {
            return Err(SqlError::Parameters);
        }
        operations::Expr::Field(_) | operations::Expr::ConflictField { .. } => {}
        operations::Expr::Compare(left, _, right)
        | operations::Expr::And(left, right)
        | operations::Expr::Or(left, right) => {
            admit_engine_expression(left, admission, depth + 1)?;
            admit_engine_expression(right, admission, depth + 1)?;
        }
        operations::Expr::IsNull(inner, _) | operations::Expr::Not(inner) => {
            admit_engine_expression(inner, admission, depth + 1)?
        }
        _ => return Err(SqlError::Unsupported),
    }
    Ok(())
}
impl Relational {
    /// Re-admit the full owned proposals before the immutable planner snapshot.
    pub(crate) fn sql_admit_conflict_input(
        table: &TableDef,
        incoming: &[Vec<Value>],
        action: &ConflictAction,
    ) -> R<()> {
        if incoming.len() > MAX_ROWS {
            return Err(SqlError::Limit);
        }
        let mut admission = InputAdmission::default();
        let logical_width = table.logical_columns().count();
        for row in incoming {
            if logical_width > MAX_COLUMNS || row.len() != table.columns.len() {
                return Err(SqlError::Type);
            }
            admission.row();
            for value in row {
                admission.value(value)?;
            }
        }
        if let ConflictAction::Update { guard, values } = action {
            if values.len() > MAX_COLUMNS {
                return Err(SqlError::Limit);
            }
            admission.row();
            for (_, expression) in values {
                admit_engine_expression(expression, &mut admission, 0)?;
            }
            if let Some(expression) = guard {
                admit_engine_expression(expression, &mut admission, 0)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
thread_local! {
    static ADMISSION_EVENTS: std::cell::Cell<[usize; 5]> = const { std::cell::Cell::new([0; 5]) };
}
#[cfg(test)]
impl Relational {
    pub(crate) fn sql_insert_test_reset() {
        ADMISSION_EVENTS.set([0; 5]);
    }
    /// Table clone, incoming materialization, state clone, planner, publisher.
    pub(crate) fn sql_insert_test_counts() -> [usize; 5] {
        ADMISSION_EVENTS.get()
    }
    pub(crate) fn sql_insert_test_event(event: usize) {
        ADMISSION_EVENTS.with(|events| {
            let mut counts = events.get();
            counts[event] += 1;
            events.set(counts);
        });
    }
}

pub(super) fn execute(
    state: &mut Relational,
    insert: &Insert,
    params: &[Value],
    expected: Option<u64>,
    returning: &Option<returning::Plan<'_>>,
    cancel: &mut dyn FnMut() -> bool,
) -> R<SqlResult> {
    if cancel() {
        return Err(SqlError::Aborted);
    }
    admit_statement(insert, params)?;
    if cancel() {
        return Err(SqlError::Aborted);
    }
    if expected.is_some() {
        return Err(SqlError::Unsupported);
    }
    let table = state
        .tables
        .get(&insert.table)
        .ok_or(SqlError::UnknownTable)?;
    // With no column list, actual table width can only be checked after the
    // borrowed raw admission; this still precedes every input-sized copy.
    let logical_width = table.logical_columns().count();
    if insert.columns.is_empty()
        && insert
            .rows
            .iter()
            .any(|row| row.len() != logical_width)
    {
        return Err(SqlError::Type);
    }
    #[cfg(test)]
    Relational::sql_insert_test_event(0);
    let table = table.clone();
    let hidden_rowid = table.hidden_rowid();
    let indexes = indexes(insert, &table)?;
    #[cfg(test)]
    Relational::sql_insert_test_event(1);
    let mut incoming = Vec::with_capacity(insert.rows.len());
    let mut total = 0usize;
    for values in &insert.rows {
        if cancel() {
            return Err(SqlError::Aborted);
        }
        let mut row = vec![Value::Null; table.columns.len()];
        for (expression, index) in values.iter().zip(&indexes) {
            row[*index] = resolved_value(expression, params, &table.columns[*index].data_type)?;
        }
        Relational::secondary_admit_values(row.iter())?;
        if let Some(hidden) = hidden_rowid {
            // The NULL placeholder has the same fixed byte budget as its future Int.
            // Skip only its check until a version supplies the hidden rowid.
            let mut size = 0usize;
            for (index, (value, column)) in row.iter().zip(&table.columns).enumerate() {
                if index != hidden {
                    check(value, column)?;
                }
                size = size.checked_add(value_bytes(value)).ok_or(SqlError::Limit)?;
            }
            if size > MAX_ROW_BYTES {
                return Err(SqlError::Limit);
            }
        } else {
            row_check(&table, &row)?;
        }
        for value in &row {
            total = total
                .checked_add(value_bytes(value))
                .ok_or(SqlError::Limit)?;
        }
        if total > MAX_RESULT_BYTES {
            return Err(SqlError::Limit);
        }
        incoming.push(row);
    }
    if let Some(conflict) = &insert.conflict {
        let action = match &conflict.update {
            None => ConflictAction::Nothing,
            Some(update) => ConflictAction::Update {
                guard: update
                    .guard
                    .as_ref()
                    .map(|guard| lower_condition(guard, &table, params))
                    .transpose()?,
                values: update
                    .sets
                    .iter()
                    .map(|(name, operand)| {
                        let index = col(&table, name)?;
                        Ok((
                            name.clone(),
                            lower_operand(
                                operand,
                                &table,
                                params,
                                Some(&table.columns[index].data_type),
                            )?,
                        ))
                    })
                    .collect::<R<Vec<_>>>()?,
            },
        };
        let mut result = if returning.is_some() {
            state.sql_conflict_engine_projected(
                &table,
                (conflict.columns.as_deref(), conflict.constraint.as_deref()),
                &incoming,
                &action,
                Some(&mut |source, rows| returning::project(returning, source, params, rows)),
                cancel,
            )?
        } else {
            state.sql_conflict_engine(
                &table,
                (conflict.columns.as_deref(), conflict.constraint.as_deref()),
                &incoming,
                &action,
                cancel,
            )?
        };
        let rows = std::mem::take(&mut result.rows);
        returning::attach(returning, &mut result, rows);
        return Ok(result);
    }
    let mut next_version = state.next_version_engine();
    // Heap tables take their hidden row id from a version allocated in VALUES order.
    let mut proposed = if let Some(hidden) = hidden_rowid {
        incoming
            .into_iter()
            .enumerate()
            .map(|(input, mut values)| {
                let version = allocate(&mut next_version)?;
                values[hidden] = Value::Int(i64::try_from(version).map_err(|_| SqlError::Overflow)?);
                Ok((pk(&table, &values)?, SqlRow { values, version }, input))
            })
            .collect::<R<Vec<_>>>()?
    } else {
        incoming
            .into_iter()
            .enumerate()
            .map(|(input, values)| Ok((pk(&table, &values)?, SqlRow { values, version: 0 }, input)))
            .collect::<R<Vec<_>>>()?
    };
    // Project RETURNING in VALUES order, after hidden row ids exist and before the key sort.
    let mut returned = if returning.is_some() {
        returning::project(
            returning,
            state,
            params,
            &proposed.iter().map(|(_, row, _)| row).collect::<Vec<_>>(),
        )?
    } else {
        vec![]
    };
    proposed.sort_by(|left, right| left.0.cmp(&right.0));
    if hidden_rowid.is_none() {
        for (_, row, _) in &mut proposed {
            row.version = allocate(&mut next_version)?;
        }
    }
    for (_, row, input) in &proposed {
        if let Some(projected) = returned.get_mut(*input) {
            projected.version = row.version;
        }
    }
    let changes = proposed
        .iter()
        .map(|(key, row, _)| BatchRowChange {
            table: table.id,
            old: None,
            new: Some((key, row)),
        })
        .collect::<Vec<_>>();
    #[cfg(test)]
    Relational::sql_insert_test_event(4);
    let mut result =
        state.secondary_apply_batch_engine(state.epoch, &changes, next_version, cancel)?;
    returning::attach(returning, &mut result, returned);
    Ok(result)
}

#[cfg(test)]
#[path = "sql_insert_tests.rs"]
mod sql_insert_tests;
