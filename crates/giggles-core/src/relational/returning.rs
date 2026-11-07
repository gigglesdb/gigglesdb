//! Mutation projections use the SELECT parser, binder, and scalar evaluator.
use super::*;

#[derive(Debug, Clone)]
pub(super) struct Returning {
    query: query::Query,
}

impl Returning {
    pub(super) fn parse(p: &mut Parser, table: &str) -> R<Option<Self>> {
        if !p.word("returning") {
            return Ok(None);
        }
        Ok(Some(Self {
            query: query::Query::returning(p, table)?,
        }))
    }

    pub(super) fn describe(
        &self,
        state: &Relational,
        seed: Vec<Option<DataType>>,
    ) -> R<(Vec<ColumnDef>, Vec<DataType>, Vec<i32>)> {
        let description = self
            .query
            .bind_projection_seeded(state, seed)?
            .description(state);
        Ok((
            description.columns,
            description.parameters,
            description.typmods,
        ))
    }

    pub(super) fn project(
        &self,
        state: &Relational,
        params: &[Value],
        rows: &[&[Value]],
    ) -> R<Vec<SqlRow>> {
        self.project_seeded(state, params, rows, vec![None; params.len()])
    }

    fn project_seeded(
        &self,
        state: &Relational,
        params: &[Value],
        rows: &[&[Value]],
        seed: Vec<Option<DataType>>,
    ) -> R<Vec<SqlRow>> {
        let bound = self.query.bind_seeded(state, seed)?;
        let mut bytes = 0usize;
        let mut work = 0usize;
        if rows.len() > MAX_ROWS {
            return Err(SqlError::Limit);
        }
        let mut output = Vec::with_capacity(rows.len());
        for row in rows {
            let values = bound.project_row(row, 0, params, &mut work, &mut bytes)?;
            output.push(SqlRow { values, version: 0 });
        }
        Ok(output)
    }
}

impl Prepared {
    pub(super) fn returning(&self) -> Option<&Returning> {
        match &self.statement {
            Statement::Insert { returning, .. }
            | Statement::Update { returning, .. }
            | Statement::UpdateExpressions { returning, .. }
            | Statement::Delete { returning, .. } => returning.as_ref(),
            Statement::InsertExpressions(values) => values.returning.as_ref(),
            Statement::InsertMulti(insert) => insert.returning.as_ref(),
            _ => None,
        }
    }
}

pub(super) fn describe_insert(
    values: &query::InsertExpressions,
    state: &Relational,
    count: usize,
) -> R<Description> {
    let Some(map) = &values.parameter_map else {
        return values.describe(state, count);
    };
    let body = values.describe(state, map.len())?;
    let mut seed = vec![None; count];
    for (original, ty) in map.iter().zip(body.parameters) {
        *seed.get_mut(original - 1).ok_or(SqlError::Parameters)? = Some(ty);
    }
    let (columns, parameters, typmods) = values
        .returning
        .as_ref()
        .ok_or(SqlError::Unsupported)?
        .describe(state, seed)?;
    Ok(Description {
        typmods,
        columns,
        parameters,
    })
}

pub(super) fn validate_insert_constants(
    values: &query::InsertExpressions,
    state: &Relational,
    count: usize,
) -> R<()> {
    values.validate_constants(state, values.parameter_map.as_ref().map_or(count, Vec::len))
}

pub(super) fn evaluate_insert(
    values: &query::InsertExpressions,
    state: &Relational,
    count: usize,
    params: &[Value],
    cancel: &mut impl FnMut() -> bool,
) -> R<Vec<Expr>> {
    if let Some(map) = &values.parameter_map {
        let inputs = map
            .iter()
            .map(|n| params.get(n - 1).cloned().ok_or(SqlError::Parameters))
            .collect::<R<Vec<_>>>()?;
        values.evaluate(state, inputs.len(), &inputs, cancel)
    } else {
        values.evaluate(state, count, params, cancel)
    }
}

pub(super) struct Plan<'a> {
    returning: &'a Returning,
    description: Description,
}
impl<'a> Plan<'a> {
    pub(super) fn new(
        prepared: &'a Prepared,
        state: &Relational,
        params: &[Value],
    ) -> R<Option<Self>> {
        let Some(returning) = prepared.returning() else {
            return Ok(None);
        };
        let description = prepared.describe(state)?;
        for (value, data_type) in params.iter().zip(&description.parameters) {
            check(
                value,
                &ColumnDef {
                    id: 0,
                    name: String::new(),
                    data_type: data_type.clone(),
                    not_null: false,
                },
            )?;
        }
        Ok(Some(Self {
            returning,
            description,
        }))
    }
}

pub(super) fn project(
    plan: &Option<Plan<'_>>,
    state: &Relational,
    params: &[Value],
    rows: &[&SqlRow],
) -> R<Vec<SqlRow>> {
    let Some(plan) = plan else {
        return Ok(vec![]);
    };
    let inputs = rows
        .iter()
        .map(|row| row.values.as_slice())
        .collect::<Vec<_>>();
    let seed = plan
        .description
        .parameters
        .iter()
        .cloned()
        .map(Some)
        .collect();
    let mut output = if params.is_empty() {
        plan.returning.project(state, params, &inputs)?
    } else {
        plan.returning
            .project_seeded(state, params, &inputs, seed)?
    };
    for (projected, source) in output.iter_mut().zip(rows) {
        projected.version = source.version;
    }
    Ok(output)
}

pub(super) fn attach(plan: &Option<Plan<'_>>, result: &mut SqlResult, rows: Vec<SqlRow>) {
    if let Some(plan) = plan {
        result.columns = plan
            .description
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect();
        result.rows = rows;
    }
}

#[cfg(test)]
#[path = "returning_tests.rs"]
mod tests;
