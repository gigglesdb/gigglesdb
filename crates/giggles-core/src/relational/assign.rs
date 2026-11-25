//! Point UPDATE assignments bind once and evaluate against the unchanged OLD row.
use super::*;

#[derive(Debug, Clone)]
pub(super) enum Value {
    Default,
    Rows(std::sync::Arc<BTreeMap<Vec<u8>, super::Value>>),
    Plain(Expr),
    Scalar(Box<query::Query>),
}

impl Value {
    pub(super) fn parse(p: &mut Parser, table: &str) -> R<Self> {
        if p.word("default") {
            return Ok(Self::Default);
        }
        let start = p.at;
        let parameters = p.params.clone();
        if let Ok(value) = p.expr()
            && (matches!(p.tokens.get(p.at), None | Some(Token::Symbol(',' | ';')))
                || matches!(p.tokens.get(p.at), Some(Token::Word(w)) if w == "where" || w == "returning"))
        {
            return Ok(Self::Plain(value));
        }
        p.at = start;
        p.params = parameters;
        query::Query::assignment(p, table).map(|value| Self::Scalar(Box::new(value.normalize_assignment_alias())))
    }
}

pub(super) fn duplicate_names<'a>(names: impl IntoIterator<Item = &'a String>) -> bool {
    let mut seen = BTreeSet::new();
    names.into_iter().any(|name| !seen.insert(name))
}

pub(super) fn has_duplicates(prepared: &Prepared) -> bool {
    match prepared.statement.get() {
        Statement::Update { sets, .. } => duplicate_names(sets.iter().map(|(name, _)| name)),
        Statement::UpdateExpressions { sets, .. } => {
            duplicate_names(sets.iter().map(|(name, _)| name))
        }
        _ => false,
    }
}

// PG18.6 analyzes SQL before rejecting duplicate targets, before binding values.
pub(super) fn check_duplicate_literals(prepared: &Prepared, state: &Relational) -> R<()> {
    let table = state
        .tables
        .get(prepared.table().ok_or(SqlError::UnknownTable)?)
        .ok_or(SqlError::UnknownTable)?;
    let values: Vec<(&String, Option<&Expr>)> = match prepared.statement.get() {
        Statement::Update { sets, .. } => sets
            .iter()
            .map(|(name, value)| (name, Some(value)))
            .collect(),
        Statement::UpdateExpressions { sets, .. } => sets
            .iter()
            .map(|(name, value)| {
                (
                    name,
                    match value {
                        Value::Plain(value) => Some(value),
                        _ => None,
                    },
                )
            })
            .collect(),
        _ => return Ok(()),
    };
    for (name, value) in values {
        let mut column = table.columns[col(table, name)?].clone();
        column.not_null = false;
        if let Some(value @ Expr::Literal(_)) = value {
            plain_value(state, value, &column, &prepared.parameter_types, &[], None)?;
        }
    }
    Ok(())
}

pub(super) fn indexes(
    sets: &[(String, Value)],
    table: &TableDef,
    state: &Relational,
) -> R<Vec<usize>> {
    if sets.len() > MAX_TABLE_COLUMNS {
        return Err(SqlError::Limit);
    }
    sets.iter()
        .map(|(name, value)| {
            let index = col(table, name)?;
            if !matches!(value, Value::Default | Value::Rows(_)) {
                state.check_generated_explicit(&table.columns[index], true)?;
                defaults::check_explicit_identity(state, &table.columns[index])?;
            }
            Ok(index)
        })
        .collect::<R<Vec<_>>>()
}

pub(super) fn plain_value(
    state: &Relational,
    value: &Expr,
    column: &ColumnDef,
    declared_types: &[Option<DataType>],
    params: &[super::Value],
    clock: Option<&clock::ExecutionClock>,
) -> R<super::Value> {
    let mut bound = value.value(params)?;
    // Without a clock (the duplicate-literal pre-check) keep the text: a UTC
    // parse could reject a local value that is in range in the session zone.
    if matches!(value, Expr::Literal(_))
        && !(clock.is_none()
            && matches!(value, Expr::Literal(super::Value::Text(_)))
            && column.data_type == DataType::TimestampTz)
    {
        bound = query::assignment::literal(bound, &column.data_type, clock)?;
    }
    if let Expr::Param(n) = value
        && let Some(Some(declared)) = declared_types.get(n - 1)
    {
        check(
            &bound,
            &ColumnDef {
                id: 0,
                name: String::new(),
                data_type: declared.clone(),
                not_null: false,
            },
        )?;
        bound = query::assignment::convert(bound, declared, &column.data_type, clock)?;
    }
    // UPDATE builds constant assignments before visiting rows. Domain checks
    // belong to evaluation of an actual destination row, not this preparation.
    bound = coerce_base_to_column(state, column, bound)?;
    if matches!(state.column_type(column.id), Some(TypeDef::Enum { .. })) {
        user_types::coerce(state, column, &bound)?;
    }
    check(&bound, column)?;
    Ok(bound)
}

pub(super) fn describe(
    name: &str,
    sets: &[(String, Value)],
    predicates: &[Predicate],
    returning: Option<&Returning>,
    state: &Relational,
    count: usize,
    declared: &[Option<DataType>],
) -> R<Description> {
    let table = state.tables.get(name).ok_or(SqlError::UnknownTable)?;
    let indexes = indexes(sets, table, state)?;
    let mut parameters = vec![None; count];
    if declared.len() > parameters.len() {
        return Err(SqlError::Parameters);
    }
    parameters[..declared.len()].clone_from_slice(declared);
    for ((_, value), index) in sets.iter().zip(indexes) {
        let target = &table.columns[index].data_type;
        match value {
            Value::Default | Value::Rows(_) => {}
            Value::Plain(value) => {
                enum_parameters::infer_column(value, &table.columns[index], state, &mut parameters)?;
                infer_mutation_parameter(value, target, &mut parameters, declared, true)?
            }
            Value::Scalar(value) => value.infer_assignment(
                state,
                &{
                    let mut target = table.clone();
                    target.name = table.name.rsplit('\0').next().unwrap_or(&table.name).into();
                    target
                },
                target,
                &mut parameters,
            )?,
        }
    }
    for predicate in predicates {
        infer_mutation_parameter(
            &predicate.value,
            &table.columns[col(table, &predicate.column)?].data_type,
            &mut parameters,
            declared,
            false,
        )?;
    }
    let (columns, parameters, _typmods) = if let Some(returning) = returning {
        returning.describe(state, parameters)?
    } else {
        (
            vec![],
            parameters
                .into_iter()
                .map(|ty| ty.ok_or(SqlError::Parameters))
                .collect::<R<Vec<_>>>()?,
            vec![],
        )
    };
    Ok(Description {
        type_oids: vec![],
        typmods: columns
            .iter()
            .map(|c| result_typmod(state, TypmodSource::Column(c)))
            .collect(),
        columns,
        parameters,
    })
}

pub(super) fn admit_literals(sets: &[(String, Value)]) -> R<()> {
    let mut bytes = 0;
    for (_, value) in sets {
        match value {
            Value::Default => {}
            Value::Rows(values) => { for value in values.values() { Relational::secondary_admit_values(std::iter::once(value))?; payload_bound(std::iter::once(value))?; } }
            Value::Plain(Expr::Literal(value)) => {
                Relational::secondary_admit_values(std::iter::once(value))?;
                payload_bound(std::iter::once(value))?;
                bytes += value_bytes(value);
            }
            Value::Scalar(value) => value.admit_assignment_literals(&mut bytes)?,
            Value::Plain(Expr::Param(_)) => {}
        }
        if bytes > crate::relational::max_value_bytes() {
            return Err(SqlError::Limit);
        }
    }
    Ok(())
}

enum BoundValue<'a> {
    Rows(&'a BTreeMap<Vec<u8>, super::Value>),
    Default(Option<String>, Option<clock::ExecutionClock>),
    Plain(super::Value),
    Scalar(Box<query::BoundQuery<'a>>),
}
pub(super) struct Plan<'a> {
    state: &'a Relational,
    values: Vec<(usize, BoundValue<'a>)>,
    scalar: bool,
}
impl<'a> Plan<'a> {
    pub(super) fn written_offsets(&self) -> impl Iterator<Item = usize> + '_ {
        self.values.iter().map(|(offset, _)| *offset)
    }

    pub(super) fn new(
        table: &TableDef,
        state: &'a Relational,
        params: &[super::Value],
        clock: Option<&super::clock::ExecutionClock>,
        description: &'a StatementDescriptionInput<'_>,
    ) -> R<Self> {
        let prepared = description.prepared();
        // Plain SQL type checks precede row lookup; NOT NULL checks apply to
        // matched postimages. Scalar inputs need a shared schema
        // for SET, WHERE and RETURNING before any parameter is evaluated.
        if let Statement::Update { sets, .. } = prepared.statement.get() {
            let values = sets
                .iter()
                .map(|(name, value)| {
                    let index = col(table, name)?;
                    state.check_generated_explicit(&table.columns[index], true)?;
                    defaults::check_explicit_identity(state, &table.columns[index])?;
                    let mut column = table.columns[index].clone();
                    column.not_null = false;
                    let value = plain_value(
                        state,
                        value,
                        &column,
                        &prepared.parameter_types,
                        params,
                        clock,
                    )?;
                    Ok((index, BoundValue::Plain(value)))
                })
                .collect::<R<Vec<_>>>()?;
            unique(values.iter().map(|(index, _)| index))?;
            return Ok(Self {
                state,
                values,
                scalar: false,
            });
        }
        let Statement::UpdateExpressions { sets, .. } = prepared.statement.get() else {
            return Err(SqlError::Unsupported);
        };
        let indexes = indexes(sets, table, state)?;
        let parameters: Vec<Option<DataType>> = {
            let description = description.describe(state)?;
            for (value, ty) in params.iter().zip(&description.parameters) {
                check(
                    value,
                    &ColumnDef {
                        id: 0,
                        name: String::new(),
                        data_type: ty.clone(),
                        not_null: false,
                    },
                )?;
            }
            description.parameters.iter().cloned().map(Some).collect()
        };
        let values = sets
            .iter()
            .zip(indexes)
            .map(|((_, value), index)| {
                let value = match value {
                    Value::Rows(values) => BoundValue::Rows(values),
                    Value::Default => BoundValue::Default(
                        defaults::default_sql(state, &table.columns[index]),
                        clock.cloned(),
                    ),
                    Value::Plain(value) => {
                        let mut column = table.columns[index].clone();
                        column.not_null = false;
                        let value = plain_value(
                            state,
                            value,
                            &column,
                            &prepared.parameter_types,
                            params,
                            clock,
                        )?;
                        BoundValue::Plain(value)
                    }
                    Value::Scalar(value) => BoundValue::Scalar(Box::new(
                        value
                            .bind_assignment(
                                state,
                                parameters.clone(),
                                &table.columns[index].data_type,
                            )?
                            .with_clock(clock),
                    )),
                };
                Ok((index, value))
            })
            .collect::<R<Vec<_>>>()?;
        Ok(Self {
            state,
            values,
            scalar: true,
        })
    }

    pub(super) fn evaluate(
        &self,
        table: &TableDef,
        old: &SqlRow,
        params: &[super::Value],
        cancel: &mut impl FnMut() -> bool,
    ) -> R<Vec<(usize, super::Value)>> {
        if !self.scalar {
            return self
                .values
                .iter()
                .map(|(index, value)| {
                    let BoundValue::Plain(value) = value else {
                        unreachable!()
                    };
                    Ok((
                        *index,
                        coerce_to_column(self.state, &table.columns[*index], value.clone())?,
                    ))
                })
                .collect();
        }
        let mut work = 0;
        let mut bytes = 0;
        let mut values = Vec::with_capacity(self.values.len());
        for (index, value) in &self.values {
            let index = *index;
            if cancel() {
                return Err(SqlError::Aborted);
            }
            let value = match value {
                BoundValue::Rows(values) => values.get(&pk(table,&old.values)?).cloned().ok_or(SqlError::UnknownColumn)?,
                BoundValue::Default(sql, clock) => match sql {
                    Some(sql) => defaults::evaluate_sql(
                        sql,
                        &table.columns[index].data_type,
                        clock.as_ref(),
                    )?,
                    None => super::Value::Null,
                },
                BoundValue::Plain(value) => value.clone(),
                BoundValue::Scalar(value) => value
                    .project_row(&old.values, old.version, params, &mut work, &mut bytes)?
                    .pop()
                    .ok_or(SqlError::Type)?,
            };
            let value = coerce_to_column(self.state, &table.columns[index], value)?;
            // Column NOT NULL belongs to the complete postimage, after every
            // SET expression and assignment conversion has succeeded.
            if value != super::Value::Null {
                check(&value, &table.columns[index])?;
            }
            values.push((index, value));
        }
        Relational::secondary_admit_values(values.iter().map(|(_, value)| value))?;
        payload_bound(values.iter().map(|(_, value)| value))?;
        Ok(values)
    }
}

/// Native numeric assignment uses SQL casts only between different value families.
/// Integer destinations retain native range admission and its Type error.
pub(crate) fn assign_number_engine(value: super::Value, column: &ColumnDef) -> R<super::Value> {
    let source = match &value {
        super::Value::Int(n) if i32::try_from(*n).is_ok() => DataType::Integer,
        super::Value::Int(_) => DataType::BigInt,
        super::Value::Numeric(_) => DataType::Numeric,
        super::Value::Float4(_) => DataType::Real,
        super::Value::Float8(_) => DataType::DoublePrecision,
        _ => return Ok(value),
    };
    let target = &column.data_type;
    if !matches!(target, DataType::SmallInt | DataType::Integer | DataType::BigInt
        | DataType::Numeric | DataType::Real | DataType::DoublePrecision)
        || source == *target
        || matches!(value, super::Value::Int(_))
            && matches!(target, DataType::SmallInt | DataType::Integer | DataType::BigInt)
    {
        return Ok(value);
    }
    query::assignment::convert(value, &source, target, None)
}
