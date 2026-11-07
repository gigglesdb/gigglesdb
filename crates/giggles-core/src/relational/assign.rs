//! Point UPDATE assignments bind once and evaluate against the unchanged OLD row.
use super::*;

#[derive(Debug, Clone)]
pub(super) enum Value {
    Plain(Expr),
    Scalar(Box<query::Query>),
}

impl Value {
    pub(super) fn parse(p: &mut Parser, table: &str) -> R<Self> {
        if p.word("default") {
            return Err(SqlError::Unsupported);
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
        query::Query::assignment(p, table).map(|value| Self::Scalar(Box::new(value)))
    }
}

fn indexes(sets: &[(String, Value)], table: &TableDef) -> R<Vec<usize>> {
    if sets.len() > MAX_COLUMNS {
        return Err(SqlError::Limit);
    }
    let indexes = sets
        .iter()
        .map(|(name, _)| {
            let index = col(table, name)?;
            if table.primary_key.contains(&index) {
                return Err(SqlError::Unsupported);
            }
            Ok(index)
        })
        .collect::<R<Vec<_>>>()?;
    unique(indexes.iter())?;
    Ok(indexes)
}

fn infer_plain(value: &Expr, ty: &DataType, parameters: &mut [Option<DataType>]) -> R<()> {
    if let Expr::Param(n) = value {
        let slot = parameters.get_mut(n - 1).ok_or(SqlError::Parameters)?;
        if slot.as_ref().is_some_and(|old| old != ty) {
            return Err(SqlError::Type);
        }
        *slot = Some(ty.clone());
    }
    Ok(())
}

pub(super) fn describe(
    name: &str,
    sets: &[(String, Value)],
    predicates: &[Predicate],
    returning: Option<&Returning>,
    state: &Relational,
    count: usize,
) -> R<Description> {
    let table = state.tables.get(name).ok_or(SqlError::UnknownTable)?;
    let indexes = indexes(sets, table)?;
    let mut parameters = vec![None; count];
    for ((_, value), index) in sets.iter().zip(indexes) {
        let target = &table.columns[index].data_type;
        match value {
            Value::Plain(value) => infer_plain(value, target, &mut parameters)?,
            Value::Scalar(value) => value.infer_assignment(table, target, &mut parameters)?,
        }
    }
    for predicate in predicates {
        infer_plain(
            &predicate.value,
            &table.columns[col(table, &predicate.column)?].data_type,
            &mut parameters,
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
            Value::Plain(Expr::Literal(value)) => {
                Relational::secondary_admit_values(std::iter::once(value))?;
                payload_bound(std::iter::once(value))?;
                bytes += value_bytes(value);
            }
            Value::Scalar(value) => value.admit_assignment_literals(&mut bytes)?,
            Value::Plain(Expr::Param(_)) => {}
        }
        if bytes > MAX_ROW_BYTES {
            return Err(SqlError::Limit);
        }
    }
    Ok(())
}

enum BoundValue<'a> {
    Plain(super::Value),
    Scalar(Box<query::BoundQuery<'a>>),
}
pub(super) struct Plan<'a> {
    values: Vec<(usize, BoundValue<'a>)>,
    scalar: bool,
}
impl<'a> Plan<'a> {
    pub(super) fn new(
        table: &TableDef,
        state: &'a Relational,
        prepared: &'a Prepared,
        params: &[super::Value],
    ) -> R<Self> {
        // Preserve the native and plain SQL assignment path exactly, including
        // value checks before finding a row. Scalar inputs need a shared schema
        // for SET, WHERE and RETURNING before any parameter is evaluated.
        if let Statement::Update { sets, .. } = &prepared.statement {
            let values = sets
                .iter()
                .map(|(name, value)| {
                    let index = col(table, name)?;
                    if table.primary_key.contains(&index) {
                        return Err(SqlError::Unsupported);
                    }
                    let value = value.value(params)?;
                    check(&value, &table.columns[index])?;
                    Ok((index, BoundValue::Plain(value)))
                })
                .collect::<R<Vec<_>>>()?;
            unique(values.iter().map(|(index, _)| index))?;
            return Ok(Self {
                values,
                scalar: false,
            });
        }
        let Statement::UpdateExpressions { sets, .. } = &prepared.statement else {
            return Err(SqlError::Unsupported);
        };
        let indexes = indexes(sets, table)?;
        let parameters: Vec<Option<DataType>> = {
            let description = prepared.describe(state)?;
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
            description.parameters.into_iter().map(Some).collect()
        };
        let values = sets
            .iter()
            .zip(indexes)
            .map(|((_, value), index)| {
                let value = match value {
                    Value::Plain(value) => {
                        let value = value.value(params)?;
                        check(&value, &table.columns[index])?;
                        BoundValue::Plain(value)
                    }
                    Value::Scalar(value) => BoundValue::Scalar(Box::new(value.bind_assignment(
                        state,
                        parameters.clone(),
                        &table.columns[index].data_type,
                    )?)),
                };
                Ok((index, value))
            })
            .collect::<R<Vec<_>>>()?;
        Ok(Self {
            values,
            scalar: true,
        })
    }

    pub(super) fn evaluate(
        self,
        table: &TableDef,
        old: &SqlRow,
        params: &[super::Value],
        cancel: &mut impl FnMut() -> bool,
    ) -> R<Vec<(usize, super::Value)>> {
        if !self.scalar {
            return Ok(self
                .values
                .into_iter()
                .map(|(index, value)| {
                    let BoundValue::Plain(value) = value else {
                        unreachable!()
                    };
                    (index, value)
                })
                .collect());
        }
        let mut work = 0;
        let mut bytes = 0;
        let mut values = Vec::with_capacity(self.values.len());
        for (index, value) in self.values {
            if cancel() {
                return Err(SqlError::Aborted);
            }
            let value = match value {
                BoundValue::Plain(value) => value,
                BoundValue::Scalar(value) => value
                    .project_row(&old.values, old.version, params, &mut work, &mut bytes)?
                    .pop()
                    .ok_or(SqlError::Type)?,
            };
            check(&value, &table.columns[index])?;
            values.push((index, value));
        }
        Relational::secondary_admit_values(values.iter().map(|(_, value)| value))?;
        payload_bound(values.iter().map(|(_, value)| value))?;
        Ok(values)
    }
}
