//! Bind SQL relation references without changing their user-visible names.
use super::*;

pub fn relation_key(schema: &str, name: &str) -> String {
    if schema == "public" {
        name.into()
    } else {
        format!("{schema}\0{name}")
    }
}

pub(crate) fn split_key(key: &str) -> (&str, &str) {
    key.split_once('\0').unwrap_or(("public", key))
}

pub(super) fn validate_key(key: &str) -> R<()> {
    let (schema, name) = split_key(key);
    ident(name)?;
    if key.contains('\0') {
        ident(schema)?;
        if schema == "public" {
            return Err(SqlError::Type);
        }
    }
    Ok(())
}

impl Relational {
    pub(crate) fn table_key(&self, table: &TableDef) -> String {
        relation_key(self.table_schema(table.id), &table.name)
    }

    pub(crate) fn resolve_relation_key(&self, name: &str, path: &[String]) -> String {
        if name.contains('\0') {
            let (schema, name) = split_key(name);
            return relation_key(schema, name);
        }
        path.iter()
            .map(|schema| relation_key(schema, name))
            .find(|key| self.relation_name_taken(key))
            // A missing relation must not accidentally resolve to public.
            .unwrap_or_else(|| format!("\0\0{name}"))
    }
}

impl Prepared {
    pub(crate) fn bind_schemas(&self, state: &Relational) -> R<Self> {
        let mut bound = self.clone();
        if self.relations.is_empty() {
            return Ok(bound);
        }
        bound.statement = self
            .unbound_statement
            .as_deref()
            .unwrap_or(&self.statement)
            .clone();
        bound.unbound_statement = Some(Box::new(bound.statement.clone()));
        let create = |name: &str| -> R<String> {
            let schema = if name.contains('\0') {
                split_key(name).0.to_owned()
            } else {
                self.creation_schema(state)?
            };
            Ok(relation_key(&schema, split_key(name).1))
        };
        let read = |name: &mut String| {
            *name = state.resolve_relation_key(name, &self.search_path);
        };
        let created = match &bound.statement {
            Statement::Create { name, .. } => Some(create(name)?),
            _ => None,
        };
        let definitions = |items: &mut [constraints::Definition]| {
            for item in items {
                if let constraints::Definition::ForeignKey { parent, .. } = item {
                    if let Some(own) = &created {
                        let candidates = if parent.contains('\0') {
                            vec![relation_key(split_key(parent).0, split_key(parent).1)]
                        } else {
                            self.search_path
                                .iter()
                                .map(|s| relation_key(s, parent))
                                .collect()
                        };
                        if let Some(key) = candidates
                            .into_iter()
                            .find(|key| key == own || state.relation_name_taken(key))
                        {
                            *parent = key;
                            continue;
                        }
                    }
                    read(parent);
                }
            }
        };
        match &mut bound.statement {
            Statement::Create {
                name, constraints, ..
            } => {
                *name = create(name)?;
                definitions(constraints);
            }
            Statement::DropTable { name, .. }
            | Statement::Insert { name, .. }
            | Statement::Select { name, .. } => read(name),
            Statement::Update { name, search, .. } | Statement::Delete { name, search, .. } => {
                read(name);
                if let Some(query) = search {
                    query.bind_schema_names(&read);
                }
            }
            Statement::UpdateExpressions {
                name, search, sets, ..
            } => {
                read(name);
                if let Some(query) = search {
                    query.bind_schema_names(&read);
                }
                for (_, value) in sets {
                    if let assign::Value::Scalar(query) = value {
                        query.bind_schema_names(&read);
                    }
                }
            }
            Statement::Query(query) => query.bind_schema_names(&read),
            Statement::InsertExpressions(insert) => insert.bind_schema_names(&read),
            Statement::InsertMulti(insert) => {
                read(&mut insert.table);
                if let Some(query) = &mut insert.source {
                    query.bind_schema_names(&read);
                }
            }
            Statement::AlterTable(alter) => {
                read(&mut alter.table);
                for action in &mut alter.actions {
                    if let alter_table::Action::Constraint(definition) = action {
                        definitions(std::slice::from_mut(definition));
                    }
                }
            }
            Statement::CreateIndex(index) => read(&mut index.table),
            Statement::Sequence(ddl) => ddl.bind_schema_names(&read, &create)?,
            _ => {}
        }
        if let Some(returning) = bound.returning_mut() {
            returning.bind_schema_names(&read);
        }
        Ok(bound)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(state: &mut Relational, sql: &str) {
        let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
        state.execute(&prepared, &[], None).unwrap();
    }

    #[test]
    fn encoding_and_catalog_schema_must_agree() {
        assert_eq!(relation_key("public", "t"), "t");
        assert_eq!(relation_key("a", "t"), "a\0t");
        let mut state = Relational::default();
        run(&mut state, "CREATE SCHEMA a");
        run(&mut state, "CREATE SCHEMA b");
        run(&mut state, "CREATE TABLE a.t(id int PRIMARY KEY)");
        state.validate().unwrap();
        let table = state.tables.remove("a\0t").unwrap();
        state.tables.insert("b\0t".into(), table.clone());
        assert!(state.validate().is_err());
        state.tables.remove("b\0t");
        state.tables.insert("t".into(), table);
        assert!(state.validate().is_err());
    }

    #[test]
    fn public_only_serialization_is_the_legacy_catalog() {
        let mut state = Relational::default();
        run(&mut state, "CREATE TABLE t(id int PRIMARY KEY)");
        assert!(state.ext.is_empty());
        assert_eq!(state.tables.keys().collect::<Vec<_>>(), vec!["t"]);
        let before = bincode::serialize(&state).unwrap();
        let reopened: Relational = bincode::deserialize(&before).unwrap();
        reopened.validate().unwrap();
        assert_eq!(before, bincode::serialize(&reopened).unwrap());
    }
}
