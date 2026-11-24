//! Mutation projections use the SELECT parser, binder, and scalar evaluator.
use super::*;

type MaterializedRows = (Description, BTreeMap<Vec<u8>, SqlRow>);

#[derive(Debug, Clone)]
pub(super) struct Returning {
    query: query::Query,
    images: bool,
    materialized: Option<Box<MaterializedRows>>,
}

impl Returning {
    pub(super) fn materialized(
        table: &str,
        mut description: Description,
        rows: BTreeMap<Vec<u8>, SqlRow>,
    ) -> Self {
        description.parameters.clear();
        Self {
            query: query::Query::materialized_returning(table, &description.columns),
            images: false,
            materialized: Some(Box::new((description, rows))),
        }
    }
    pub(super) fn bind_schema_names(
        &mut self,
        bind: &impl Fn(&mut String),
        state: &Relational,
        path: &[String],
    ) {
        self.query.bind_schema_names(bind, state, path);
    }

    pub(super) fn query(&self) -> &query::Query {
        &self.query
    }

    pub(super) fn query_mut(&mut self) -> &mut query::Query {
        &mut self.query
    }

    pub(super) fn apply_type_oids(
        &self,
        state: &Relational,
        description: &mut Description,
    ) -> R<()> {
        if let Some((d, _)) = self.materialized.as_deref() {
            description.type_oids.clone_from(&d.type_oids);
        }
        if self.images {
            let oid = type_oid(state.tables[self.query.target()].id)?
                .checked_add(3)
                .ok_or(SqlError::Limit)?;
            for (slot, whole) in description
                .type_oids
                .iter_mut()
                .zip(self.query.image_whole_columns(state)?)
            {
                if whole {
                    *slot = oid;
                }
            }
        }
        Ok(())
    }

    pub(super) fn reads_columns(&self) -> bool {
        self.query.projection_reads_columns()
    }

    pub(super) fn sequence_dependencies(&self) -> Vec<SequenceDependency> {
        self.query.sequence_dependencies()
    }
    pub(super) fn parse(p: &mut Parser, table: &str) -> R<Option<Self>> {
        if !p.word("returning") {
            return Ok(None);
        }
        let mut aliases = [("old".to_owned(), "old"), ("new".to_owned(), "new")];
        if p.word("with") {
            p.punct('(')?;
            loop {
                let image = if p.word("old") {
                    0
                } else {
                    p.need("new")?;
                    1
                };
                p.need("as")?;
                aliases[image].0 = p.name()?;
                if !p.sym(',') {
                    break;
                }
            }
            p.punct(')')?;
        }
        let mut query = query::Query::returning(p, table)?;
        let images = query.rewrite_returning_images(&aliases)?;
        Ok(Some(Self {
            query,
            images,
            materialized: None,
        }))
    }

    fn image_catalog(&self, state: &Relational) -> R<Relational> {
        let mut catalog = state.clone();
        let table = std::sync::Arc::make_mut(&mut catalog.tables).get_mut(self.query.target())
            .ok_or(SqlError::UnknownTable)?;
        let columns = table.logical_columns().cloned().collect::<Vec<_>>();
        for image in ["old", "new"] {
            for column in &columns {
                let mut column = column.clone();
                column.name = format!("\0returning_{image}_{}", column.name);
                column.not_null = false;
                table.columns.push(column);
            }
            table.columns.push(ColumnDef {
                id: 0,
                name: format!("\0returning_{image}"),
                data_type: DataType::Text,
                not_null: false,
            });
        }
        Ok(catalog)
    }

    pub(super) fn describe(
        &self,
        state: &Relational,
        seed: Vec<Option<DataType>>,
    ) -> R<(Vec<ColumnDef>, Vec<DataType>, Vec<i32>)> {
        if let Some((d, _)) = self.materialized.as_deref() {
            return Ok((d.columns.clone(), d.parameters.clone(), d.typmods.clone()));
        }
        let catalog = self.images.then(|| self.image_catalog(state)).transpose()?;
        let binding = catalog.as_ref().unwrap_or(state);
        let description = self
            .query
            .image_projection(self.images, state)?
            .bind_projection_seeded(binding, seed)?
            .description(binding);
        Ok((
            description.columns,
            description.parameters,
            description.typmods,
        ))
    }

    #[cfg(test)]
    pub(super) fn project(
        &self,
        state: &Relational,
        params: &[Value],
        rows: &[&[Value]],
    ) -> R<Vec<SqlRow>> {
        self.project_seeded(state, params, rows, vec![None; params.len()], None)
    }

    fn project_seeded(
        &self,
        state: &Relational,
        params: &[Value],
        rows: &[&[Value]],
        seed: Vec<Option<DataType>>,
        clock: Option<&super::clock::ExecutionClock>,
    ) -> R<Vec<SqlRow>> {
        let catalog = self.images.then(|| self.image_catalog(state)).transpose()?;
        let projection = self.query.image_projection(self.images, state)?;
        let bound = projection
            .bind_seeded(catalog.as_ref().unwrap_or(state), seed)?
            .with_clock(clock);
        let mut bytes = 0usize;
        let mut work = 0usize;
        let unbounded = spill::returning_is_unbounded();
        if !unbounded && rows.len() > MAX_ROWS {
            return Err(SqlError::Limit);
        }
        let mut output = Vec::with_capacity(rows.len());
        for row in rows {
            if unbounded {
                bytes = 0;
                work = 0;
            }
            let values = bound.project_row(row, 0, params, &mut work, &mut bytes)?;
            output.push(SqlRow { values, version: 0 });
        }
        Ok(output)
    }
}

impl Prepared {
    pub(super) fn returning_mut(&mut self) -> Option<&mut Returning> {
        match self.statement.get_mut() {
            Statement::Insert { returning, .. }
            | Statement::Update { returning, .. }
            | Statement::UpdateExpressions { returning, .. }
            | Statement::Delete { returning, .. } => returning.as_mut(),
            Statement::InsertExpressions(values) => values.returning.as_mut(),
            Statement::InsertMulti(insert) => insert.returning.as_mut(),
            _ => None,
        }
    }

    pub(super) fn returning(&self) -> Option<&Returning> {
        match self.statement.get() {
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
    declared: &[Option<DataType>],
) -> R<Description> {
    let Some(map) = &values.parameter_map else {
        return values.describe(state, count, declared);
    };
    let inputs = map
        .iter()
        .map(|n| declared.get(n - 1).cloned().flatten())
        .collect::<Vec<_>>();
    let body = values.describe(state, map.len(), &inputs)?;
    let mut seed = vec![None; count];
    if declared.len() > count {
        return Err(SqlError::Parameters);
    }
    seed[..declared.len()].clone_from_slice(declared);
    for (original, ty) in map.iter().zip(body.parameters) {
        *seed.get_mut(original - 1).ok_or(SqlError::Parameters)? = Some(ty);
    }
    let (columns, parameters, typmods) = values
        .returning
        .as_ref()
        .ok_or(SqlError::Unsupported)?
        .describe(state, seed)?;
    Ok(Description {
        type_oids: vec![],
        typmods,
        columns,
        parameters,
    })
}

pub(super) fn validate_insert_constants(
    values: &query::InsertExpressions,
    state: &Relational,
    count: usize,
    declared: &[Option<DataType>],
) -> R<()> {
    if let Some(map) = &values.parameter_map {
        let inputs = map
            .iter()
            .map(|n| declared.get(n - 1).cloned().flatten())
            .collect::<Vec<_>>();
        values.validate_constants(state, map.len(), &inputs)
    } else {
        values.validate_constants(state, count, declared)
    }
}

pub(super) fn evaluate_insert(
    values: &query::InsertExpressions,
    state: &Relational,
    count: usize,
    declared: &[Option<DataType>],
    params: &[Value],
    cancel: &mut impl FnMut() -> bool,
    clock: Option<&super::clock::ExecutionClock>,
) -> R<Vec<Expr>> {
    if let Some(map) = &values.parameter_map {
        let inputs = map
            .iter()
            .map(|n| params.get(n - 1).cloned().ok_or(SqlError::Parameters))
            .collect::<R<Vec<_>>>()?;
        let types = map
            .iter()
            .map(|n| declared.get(n - 1).cloned().flatten())
            .collect::<Vec<_>>();
        values.evaluate_clocked(state, inputs.len(), &types, &inputs, cancel, clock)
    } else {
        values.evaluate_clocked(state, count, declared, params, cancel, clock)
    }
}

pub(super) struct Plan<'a> {
    returning: &'a Returning,
    description: &'a Description,
    clock: Option<super::clock::ExecutionClock>,
    command: &'static str,
}
impl<'a> Plan<'a> {
    pub(super) fn new(
        prepared: &'a Prepared,
        state: &Relational,
        params: &[Value],
        clock: Option<&super::clock::ExecutionClock>,
        description: &'a StatementDescription<'_>,
    ) -> R<Option<Self>> {
        let Some(returning) = prepared.returning() else {
            return Ok(None);
        };
        let description = description.describe(prepared, state)?;
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
            clock: clock.cloned(),
            command: prepared.command(),
        }))
    }
}

pub(super) fn project(
    plan: &Option<Plan<'_>>,
    state: &Relational,
    params: &[Value],
    rows: &[&SqlRow],
) -> R<Vec<SqlRow>> {
    let Some(bound) = plan else {
        return Ok(vec![]);
    };
    let pairs = rows
        .iter()
        .map(|row| match bound.command {
            "DELETE" => (Some(*row), None),
            "INSERT" => (None, Some(*row)),
            _ => (Some(*row), Some(*row)),
        })
        .collect::<Vec<_>>();
    project_pairs(plan, state, params, &pairs)
}

pub(super) fn project_pairs(
    plan: &Option<Plan<'_>>,
    state: &Relational,
    params: &[Value],
    rows: &[(Option<&SqlRow>, Option<&SqlRow>)],
) -> R<Vec<SqlRow>> {
    let Some(plan) = plan else {
        return Ok(vec![]);
    };
    let table = &state.tables[plan.returning.query.target()];
    if let Some((_, materialized)) = plan.returning.materialized.as_deref() {
        return rows
            .iter()
            .map(|(old, new)| {
                let source = old.or(*new).ok_or(SqlError::UnknownColumn)?;
                let mut row = materialized
                    .get(&pk(table, &source.values)?)
                    .cloned()
                    .ok_or(SqlError::UnknownColumn)?;
                row.version = new.or(*old).map_or(0, |r| r.version);
                Ok(row)
            })
            .collect();
    }
    let mut owned = vec![];
    for (old, new) in rows {
        let primary = new.or(*old).ok_or(SqlError::UnknownColumn)?;
        let mut values = primary.values.clone();
        if plan.returning.images {
            for image in [old, new] {
                let logical = table
                    .logical_offsets()
                    .into_iter()
                    .map(|i| image.map_or(Value::Null, |r| r.values[i].clone()))
                    .collect::<Vec<_>>();
                values.extend(logical.iter().cloned());
                values.push(if image.is_some() {
                    Value::Text(query::plan::record(&logical)?)
                } else {
                    Value::Null
                });
            }
        }
        owned.push(values);
    }
    let inputs = owned.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let seed = plan
        .description
        .parameters
        .iter()
        .cloned()
        .map(Some)
        .collect();
    let mut output =
        plan.returning
            .project_seeded(state, params, &inputs, seed, plan.clock.as_ref())?;
    for (projected, source) in output.iter_mut().zip(rows) {
        projected.version = source.1.or(source.0).map_or(0, |r| r.version);
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
