//! Definition and execution of PostgreSQL generated columns.
use super::*;

#[derive(Clone, Debug)]
pub(super) struct Definition {
    pub tokens: Vec<Token>,
    pub locations: Vec<usize>,
    pub stored: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeneratedColumn {
    pub sql: String,
    pub stored: bool,
    pub not_null: bool,
    pub references: Vec<u64>,
}

impl GeneratedColumn {
    pub fn pretty_expression(&self) -> String {
        query::pretty_generated_expression(&self.sql).unwrap_or_else(|_| self.sql.clone())
    }
}

thread_local! {
    static LOCATIONS: std::cell::RefCell<Vec<usize>> = const { std::cell::RefCell::new(Vec::new()) };
}

pub(super) struct Locations(Vec<usize>);

impl Locations {
    pub(super) fn enter(sql: &str, tokens: &[Token]) -> R<Option<Self>> {
        if !matches!(tokens.get(..2), Some([Token::Word(action), Token::Word(table)])
            if matches!(action.as_str(), "create" | "alter") && table == "table")
            || !tokens.iter().any(|token| matches!(token, Token::Word(word) if word == "generated" || word == "expression"))
        {
            return Ok(None);
        }
        let mut spans = Vec::new();
        lex_with_spans(sql, &mut Vec::new(), false, Some(&mut spans))?;
        let mut positions: Vec<_> = spans.into_iter().map(|span| span.start).collect();
        let mut next = sql.char_indices().enumerate();
        for position in &mut positions {
            if let Some((index, _)) = next.find(|(_, (offset, _))| offset == position) {
                *position = index + 1;
            }
        }
        Ok(Some(Self(LOCATIONS.with(|value| value.replace(positions)))))
    }
}

impl Drop for Locations {
    fn drop(&mut self) {
        LOCATIONS.with(|value| value.replace(std::mem::take(&mut self.0)));
    }
}

pub(super) fn error_at(token: usize) {
    if let Some(position) = LOCATIONS.with(|value| value.borrow().get(token).copied()) {
        user_types::sql_error_position(position);
    }
}

pub(super) fn expression_tokens(p: &mut Parser) -> R<(Vec<Token>, Vec<usize>)> {
    p.punct('(')?;
    let start = p.at;
    let mut depth = 1;
    while let Some(token) = p.tokens.get(p.at) {
        match token {
            Token::Symbol('(') => depth += 1,
            Token::Symbol(')') => {
                depth -= 1;
                if depth == 0 {
                    let tokens = p.tokens[start..p.at].to_vec();
                    let locations = LOCATIONS
                        .with(|value| value.borrow().get(start..p.at).unwrap_or_default().to_vec());
                    p.at += 1;
                    return if tokens.is_empty() {
                        Err(SqlError::Syntax)
                    } else {
                        Ok((tokens, locations))
                    };
                }
            }
            _ => {}
        }
        p.at += 1;
    }
    Err(SqlError::Syntax)
}

pub(super) fn starts_expression(p: &Parser) -> bool {
    matches!(
        p.tokens.get(p.at..p.at + 3),
        Some([Token::Word(always), Token::Word(as_word), Token::Symbol('(')])
            if always == "always" && as_word == "as"
    )
}

pub(super) fn parse(p: &mut Parser) -> R<Definition> {
    p.need("always")?;
    p.need("as")?;
    let (tokens, locations) = expression_tokens(p)?;
    let stored = p.word("stored");
    if !stored {
        p.word("virtual");
    }
    Ok(Definition {
        tokens,
        locations,
        stored,
    })
}

pub(super) fn conflicting_default(table: &str, column: &str) -> SqlError {
    user_types::view_replace_error(format!(
        "both default and generation expression specified for column \"{column}\" of table \"{}\"",
        split_key(table).1
    ));
    SqlError::Syntax
}

pub(super) fn bind_create(
    table: &TableDef,
    definitions: &[Option<Definition>],
) -> R<BTreeMap<u64, GeneratedColumn>> {
    let generated: BTreeSet<_> = table
        .columns
        .iter()
        .zip(definitions)
        .filter_map(|(column, definition)| definition.as_ref().map(|_| column.id))
        .collect();
    table
        .columns
        .iter()
        .zip(definitions)
        .filter_map(|(column, definition)| {
            let definition = definition.as_ref()?;
            Some(
                query::bind_generated_definition(table, column, definition, &generated)
                    .map(|g| (column.id, g)),
            )
        })
        .collect()
}

pub(super) fn explicit(column: &ColumnDef, update: bool) -> SqlError {
    user_types::view_replace_error(if update {
        format!("column \"{}\" can only be updated to DEFAULT", column.name)
    } else {
        format!(
            "cannot insert a non-DEFAULT value into column \"{}\"",
            column.name
        )
    });
    user_types::sql_error_detail(format!("Column \"{}\" is a generated column.", column.name));
    SqlError::GeneratedAlways
}

pub(super) fn install(
    state: &mut Relational,
    table: &TableDef,
    column: &ColumnDef,
    definition: &Definition,
) -> R<()> {
    let mut generated: BTreeSet<_> = state.ext.generated_columns.keys().copied().collect();
    generated.insert(column.id);
    let definition = query::bind_generated_definition(table, column, definition, &generated)?;
    Arc::make_mut(&mut state.ext)
        .generated_columns
        .insert(column.id, definition);
    Ok(())
}

pub(super) fn dependent_columns<'a>(
    state: &Relational,
    table: &'a TableDef,
    referenced: u64,
) -> Vec<&'a ColumnDef> {
    table
        .columns
        .iter()
        .filter(|column| {
            state
                .generated_column(column.id)
                .is_some_and(|generation| generation.references.contains(&referenced))
        })
        .collect()
}

pub(super) fn check_referenced_type(
    state: &Relational,
    table: &TableDef,
    column: &ColumnDef,
) -> R<()> {
    if let Some(dependent) = dependent_columns(state, table, column.id).first() {
        user_types::view_replace_error(
            "cannot alter type of a column used by a generated column".into(),
        );
        user_types::sql_error_detail(format!(
            "Column \"{}\" is used by generated column \"{}\".",
            column.name, dependent.name
        ));
        return Err(SqlError::Unsupported);
    }
    Ok(())
}

pub(super) fn rename_reference(
    state: &mut Relational,
    table: &TableDef,
    column: &ColumnDef,
    new: &str,
) -> R<()> {
    let dependents = dependent_columns(state, table, column.id);
    if !dependents.is_empty() {
        let generations = &mut Arc::make_mut(&mut state.ext).generated_columns;
        for dependent in dependents {
            let definition = generations.get_mut(&dependent.id).ok_or(SqlError::Type)?;
            definition.sql = query::rename_generated_reference(&definition.sql, &column.name, new)?;
        }
    }
    Ok(())
}

pub(super) fn alter(
    state: &mut Relational,
    table: &TableDef,
    column: &ColumnDef,
    expression: Option<&Definition>,
    if_exists: bool,
    cancel: &mut impl FnMut() -> bool,
) -> R<()> {
    let Some(generation) = state.generated_column(column.id) else {
        let message = format!(
            "column \"{}\" of relation \"{}\" is not a generated column",
            column.name, table.name
        );
        if if_exists {
            user_types::sql_notice(format!("{message}, skipping"), "00000");
            return Ok(());
        }
        user_types::view_replace_error(message);
        return Err(SqlError::ObjectNotInPrerequisiteState);
    };
    let Some(expression) = expression else {
        if !generation.stored {
            user_types::view_replace_error(
                "ALTER TABLE / DROP EXPRESSION is not supported for virtual generated columns"
                    .into(),
            );
            user_types::sql_error_detail(format!(
                "Column \"{}\" of relation \"{}\" is a virtual generated column.",
                column.name, table.name
            ));
            return Err(SqlError::Unsupported);
        }
        Arc::make_mut(&mut state.ext)
            .generated_columns
            .remove(&column.id);
        return Ok(());
    };
    let stored = generation.stored;
    let mut column = column.clone();
    column.not_null |= generation.not_null;
    install(
        state,
        table,
        &column,
        &Definition {
            stored,
            ..expression.clone()
        },
    )?;
    if stored {
        let mut rewritten = BTreeMap::new();
        for (_, mut row) in state.rows.remove(&table.id).unwrap_or_default() {
            if cancel() {
                return Err(SqlError::Aborted);
            }
            state.materialize_generated(table, &mut row.values)?;
            row_check(table, &row.values)?;
            state.check_insert_candidate(table, &row.values)?;
            let key = pk(table, &row.values)?;
            let std::collections::btree_map::Entry::Vacant(slot) = rewritten.entry(key) else {
                return Err(state.primary_duplicate(table, &row.values));
            };
            slot.insert(row);
        }
        state.rows.insert(table.id, rewritten);
        let indexes = state.secondary_descriptors_engine().to_vec();
        state.secondary = state.secondary_build_complete_engine(&indexes, cancel)?;
    } else if column.not_null {
        let generation = state.generated_column(column.id).ok_or(SqlError::Type)?;
        for row in state.rows.get(&table.id).ok_or(SqlError::Type)?.values() {
            if cancel() {
                return Err(SqlError::Aborted);
            }
            if query::generated_expression_value(table, generation, &column, &row.values)?
                == Value::Null
            {
                user_types::view_replace_error(format!(
                    "column \"{}\" of relation \"{}\" contains null values",
                    column.name, table.name
                ));
                user_types::sql_error_names(state, table, Some(&column.name), None);
                return Err(SqlError::Null);
            }
        }
    }
    Ok(())
}

impl Relational {
    pub(super) fn generated_column(&self, column: u64) -> Option<&GeneratedColumn> {
        self.ext.generated_columns.get(&column)
    }

    pub(crate) fn materialize_generated(&self, table: &TableDef, values: &mut [Value]) -> R<()> {
        if self.ext.generated_columns.is_empty() {
            return Ok(());
        }
        let mut generated = false;
        for (offset, column) in table.columns.iter().enumerate() {
            if let Some(generation) = self.generated_column(column.id) {
                generated = true;
                if generation.stored {
                    let value =
                        query::generated_expression_value(table, generation, column, values)?;
                    values[offset] = coerce_to_column(self, column, value)?;
                }
            }
        }
        if generated {
            for (column, value) in table.columns.iter().zip(values.iter()) {
                let null = if let Some(generation) = self
                    .generated_column(column.id)
                    .filter(|generation| !generation.stored && generation.not_null)
                {
                    query::generated_expression_value(table, generation, column, values)?
                        == Value::Null
                } else {
                    column.name != HIDDEN_ROWID && column.not_null && *value == Value::Null
                };
                if null {
                    let detail = logical_types::failing_row_detail_with(table, values, |id| {
                        self.generated_column(id).is_some_and(|generation| !generation.stored)
                    });
                    logical_types::not_null_violation(table, column, detail);
                    user_types::sql_error_names(self, table, Some(&column.name), None);
                    return Err(SqlError::Null);
                }
            }
        }
        Ok(())
    }

    pub(super) fn check_generated_explicit(&self, column: &ColumnDef, update: bool) -> R<()> {
        if self.generated_column(column.id).is_some() {
            return Err(explicit(column, update));
        }
        Ok(())
    }

    pub(crate) fn has_generated_columns(&self, table: &TableDef) -> bool {
        table
            .columns
            .iter()
            .any(|column| self.generated_column(column.id).is_some())
    }

    pub(crate) fn primary_has_generated(&self, table: &TableDef) -> bool {
        table
            .primary_key
            .iter()
            .any(|i| self.generated_column(table.columns[*i].id).is_some())
    }

    pub(super) fn column_written_by(
        &self,
        table: &TableDef,
        column: &ColumnDef,
        writes: impl Fn(&str) -> bool,
    ) -> bool {
        writes(&column.name)
            || self.generated_column(column.id).is_some_and(|generation| {
                generation.stored
                    && table.columns.iter().any(|dependency| {
                        generation.references.contains(&dependency.id) && writes(&dependency.name)
                    })
            })
    }
}

pub(super) fn check_virtual_primary(state: &Relational, column: &ColumnDef) -> R<()> {
    if state
        .generated_column(column.id)
        .is_some_and(|generation| !generation.stored)
    {
        user_types::view_replace_error(
            "primary keys on virtual generated columns are not supported".into(),
        );
        return Err(SqlError::Unsupported);
    }
    Ok(())
}

pub(super) fn check_index_columns(state: &Relational, columns: &[u64]) -> R<()> {
    if columns.iter().any(|id| {
        state
            .generated_column(*id)
            .is_some_and(|generation| !generation.stored)
    }) {
        user_types::view_replace_error(
            "indexes on virtual generated columns are not supported".into(),
        );
        return Err(SqlError::Unsupported);
    }
    Ok(())
}
