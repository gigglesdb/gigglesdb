//! CREATE TABLE AS syntax, output metadata and population.
use super::*;
use select_source::SelectSource;

#[derive(Debug, Clone)]
pub(super) struct CreateAs {
    pub source: SelectSource,
    names: Vec<String>,
    no_data: bool,
}

pub(super) fn parse(p: &mut Parser, name: &str, optional: bool) -> R<Option<Statement>> {
    let start = p.at;
    let names = if p.word("as") {
        vec![]
    } else if let Ok(names) = p.names()
        && p.word("as")
    {
        names
    } else {
        p.at = start;
        return Ok(None);
    };

    let mut end = p.tokens.len();
    if p.tokens.last() == Some(&Token::Symbol(';')) {
        end -= 1;
    }
    let word =
        |at: usize, expected: &str| matches!(&p.tokens[at], Token::Word(word) if word == expected);
    let mut no_data = false;
    if end >= p.at + 2 && word(end - 1, "data") {
        if word(end - 2, "with") {
            end -= 2;
        } else if end >= p.at + 3 && word(end - 2, "no") && word(end - 3, "with") {
            end -= 3;
            no_data = true;
        }
    }
    let source = SelectSource::parse(p, end)?;
    p.at = p.tokens.len();

    Ok(Some(Statement::Create {
        name: name.to_owned(),
        optional,
        columns: vec![],
        typmods: vec![],
        user_types: vec![],
        collations: vec![],
        defaults: vec![],
        generated: vec![],
        sequences: vec![],
        keys: vec![],
        constraints: vec![],
        primary_name: None,
        as_query: Some(Box::new(CreateAs {
            source,
            names,
            no_data,
        })),
    }))
}

pub(super) struct Definition {
    pub columns: Vec<ColumnDef>,
    pub typmods: Vec<Option<i32>>,
    pub collations: Vec<Option<String>>,
    pub type_ids: Vec<Option<u64>>,
    pub rows: Vec<SqlRow>,
}

impl CreateAs {
    pub(super) fn completion_command(&self) -> &'static str {
        if self.no_data {
            "CREATE TABLE AS WITH NO DATA"
        } else {
            "CREATE TABLE AS"
        }
    }

    pub(super) fn describe(&self, state: &Relational, prepared: &Prepared) -> R<Description> {
        let mut description = match &self.source {
            SelectSource::Query(query) => query
                .bind_create_as(state, prepared.params, &prepared.parameter_types)?
                .description(state),
            SelectSource::Plan(plan, _) => {
                plan.describe(state, prepared.params, &prepared.parameter_types)?
            }
        };
        // CTAS has input parameters and never returns the source's rows.
        description.columns.clear();
        description.typmods.clear();
        description.type_oids.clear();
        Ok(description)
    }

    pub(super) fn skip(state: &Relational, name: &str, optional: bool) -> R<bool> {
        if sql_index::skip_existing_relation(state, name, optional)? {
            return Ok(true);
        }
        if state.type_by_name(split_key(name).1).is_some() {
            return Err(SqlError::DuplicateObject);
        }
        Ok(false)
    }

    pub(super) fn bind_and_read(
        &self,
        state: &Relational,
        prepared: &Prepared,
        params: &[Value],
        clock: Option<&clock::ExecutionClock>,
        cancel: &mut impl FnMut() -> bool,
        injected_source: Option<&dyn scan::RowSource>,
    ) -> R<Definition> {
        let legacy = scan::LegacyRows(state);
        let rows = injected_source.unwrap_or(&legacy);
        let declared = &prepared.parameter_types;
        match &self.source {
            SelectSource::Query(query) => {
                let bound = query
                    .bind_create_as(state, prepared.params, declared)?
                    .with_clock(clock);
                let mut definition =
                    self.definition(state, bound.description(state), bound.output_collations()?)?;
                if !self.no_data {
                    definition.rows = bound
                        .execute_declared(state, rows, params, cancel, declared)?
                        .rows;
                }
                Ok(definition)
            }
            SelectSource::Plan(plan, _) => {
                let (description, collations) =
                    plan.describe_create_as(state, prepared.params, declared)?;
                let mut definition = self.definition(state, description, collations)?;
                if !self.no_data {
                    definition.rows = plan
                        .execute(
                            state,
                            rows,
                            prepared.params,
                            params,
                            declared,
                            clock,
                            cancel,
                        )?
                        .rows;
                }
                Ok(definition)
            }
        }
    }

    fn definition(
        &self,
        state: &Relational,
        description: Description,
        collations: Vec<Option<String>>,
    ) -> R<Definition> {
        let Description {
            mut columns,
            typmods,
            type_oids,
            ..
        } = description;
        if self.names.len() > columns.len() {
            user_types::view_replace_error("too many column names were specified".into());
            return Err(SqlError::Syntax);
        }
        for (column, name) in columns.iter_mut().zip(&self.names) {
            column.name.clone_from(name);
        }
        let mut names = BTreeSet::new();
        if columns
            .iter()
            .any(|column| column.name == "ctid" || !names.insert(&column.name))
        {
            return Err(SqlError::DuplicateColumn);
        }
        // JSON has a wire type and no durable column representation.
        if type_oids.contains(&114) || columns.iter().any(|c| matches!(c.data_type, DataType::ExpressionArray(_))) {
            return Err(SqlError::Unsupported);
        }
        let type_ids = columns
            .iter_mut()
            .enumerate()
            .map(|(i, column)| {
                let attached = state
                    .ext
                    .columns
                    .get(&column.id)
                    .and_then(|column| column.type_id);
                let oid = type_oids.get(i).copied().unwrap_or(0);
                let user_type = oid
                    .checked_sub(user_types::USER_TYPE_OID_BASE)
                    .map(|offset| u64::from(offset / user_types::USER_TYPE_OID_STRIDE))
                    .filter(|id| state.ext.types.contains_key(id));
                column.id = 0;
                column.not_null = false;
                attached.or(user_type)
            })
            .collect();
        let typmods = typmods.into_iter().map(|n| (n >= 0).then_some(n)).collect();
        let collations = collations
            .into_iter()
            .map(|name| name.map(|name| name.split('\0').next().unwrap_or(&name).to_owned()))
            .collect();
        Ok(Definition {
            columns,
            typmods,
            collations,
            type_ids,
            rows: vec![],
        })
    }

    pub(super) fn population(
        prepared: &Prepared,
        name: &str,
        epoch: u64,
        width: usize,
    ) -> R<Prepared> {
        let quote = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
        let (schema, leaf) = split_key(name);
        let placeholders = (1..=width)
            .map(|i| format!("${i}"))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "INSERT INTO {}.{} VALUES ({placeholders})",
            quote(schema),
            quote(leaf)
        );
        let mut insert = Prepared::parse(&sql, prepared.domain, epoch)?;
        insert.search_path.clone_from(&prepared.search_path);
        Ok(insert)
    }
}
