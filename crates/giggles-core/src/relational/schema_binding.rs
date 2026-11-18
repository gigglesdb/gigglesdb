//! Bind SQL relation references without changing their user-visible names.
use super::*;
use std::sync::Arc;

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
    // R1 used globally unique bare keys, with schema membership stored only in
    // table_schemas. D2 changed keys without a wire-version bump. Normalize only
    // that exact old spelling; mismatched qualified keys still fail validation.
    pub(super) fn normalize_r1_relation_names(&mut self) -> Result<(), &'static str> {
        let mut tables = BTreeMap::new();
        for (key, table) in std::mem::take(&mut self.tables) {
            let key = if key == table.name {
                relation_key(self.table_schema(table.id), &table.name)
            } else {
                key
            };
            if tables.insert(key, table).is_some() {
                return Err("duplicate normalized relation key");
            }
        }
        self.tables = tables;
        let mut renamed = BTreeMap::new();
        for sequence in self.ext.sequences.values_mut() {
            if !sequence.name.contains('\0')
                && let Some((table, _)) = sequence.owned_by
                && let Some(schema) = self.ext.table_schemas.get(&table)
            {
                let key = relation_key(schema, &sequence.name);
                renamed.insert(sequence.name.clone(), key.clone());
                sequence.name = key;
            }
        }
        defaults::normalize_r1_sequence_defaults(&mut self.ext, &renamed)
            .map_err(|_| "invalid R1 sequence default")?;
        Ok(())
    }

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

    pub(crate) fn resolve_index_key(&self, name: &str, path: &[String]) -> String {
        if name.contains('\0') {
            let (schema, name) = split_key(name);
            return relation_key(schema, name);
        }
        path.iter()
            .map(|schema| relation_key(schema, name))
            .find(|key| self.relation_name_taken(key)
                || sql_index::primary_table(self, key).is_some())
            .unwrap_or_else(|| self.resolve_relation_key(name, path))
    }

    pub(crate) fn index_table_key(&self, key: &str) -> Option<String> {
        self.secondary_descriptors_engine()
            .iter()
            .find(|d| d.name == key)
            .and_then(|d| self.tables.values().find(|t| t.id == d.table))
            .or_else(|| sql_index::primary_table(self, key))
            .map(|t| self.table_key(t))
    }

}

impl Prepared {
    pub(crate) fn bind_schemas(&self, state: &Relational) -> R<Self> {
        if self.relations.is_empty()
            && !matches!(&self.statement, Statement::Plan(_))
            && !matches!(&self.statement, Statement::Query(q) if q.has_domain_expression())
        {
            return Ok(self.clone());
        }
        let unbound = self
            .unbound_statement
            .as_ref()
            .map(Arc::clone)
            .unwrap_or_else(|| Arc::new(self.statement.clone()));
        let mut bound = Self {
            domain: self.domain,
            epoch: self.epoch,
            transaction: self.transaction,
            statement: (*unbound).clone(),
            unbound_statement: Some(unbound),
            params: self.params,
            parameter_types: self.parameter_types.clone(),
            relations: self.relations.clone(),
            search_path: self.search_path.clone(),
            schema_user: self.schema_user.clone(),
            warnings: self.warnings.clone(),
        };
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
            Statement::View(ddl) => {
                if let Some(query) = &mut ddl.query {
                    ddl.names[0] = create(&ddl.names[0])?;
                    query.bind_schema_names(&read, state, &self.search_path);
                } else { ddl.names.iter_mut().for_each(read); }
            }
            Statement::Comment(comment) => read(&mut comment.table),
            Statement::SecurityLabel(label) => read(&mut label.table),
            Statement::Truncate { names, .. } | Statement::DropTable { names, .. } => names.iter_mut().for_each(read),
            Statement::Create {
                name, constraints, as_query, ..
            } => {
                *name = create(name)?;
                if let Some((q,_))=as_query {q.bind_schema_names(&read,state,&self.search_path);}
                definitions(constraints);
            }
            Statement::Insert { name, .. }
            | Statement::Select { name, .. } => read(name),
            Statement::Update { name, search, .. } | Statement::Delete { name, search, .. } => {
                read(name);
                if let Some(query) = search {
                    query.bind_schema_names(&read, state, &self.search_path);
                }
            }
            Statement::UpdateExpressions {
                name, search, sets, ..
            } => {
                read(name);
                if let Some(query) = search {
                    query.bind_schema_names(&read, state, &self.search_path);
                }
                for (_, value) in sets {
                    if let assign::Value::Scalar(query) = value {
                        query.bind_schema_names(&read, state, &self.search_path);
                    }
                }
            }
            Statement::Query(query) => query.bind_schema_names(&read, state, &self.search_path),
            Statement::Plan(plan) => plan.bind_schema_names(&read, state, &self.search_path)?,
            Statement::InsertExpressions(insert) => {
                insert.bind_schema_names(&read, state, &self.search_path)
            }
            Statement::InsertMulti(insert) => {
                read(&mut insert.table);
                if let Some(query) = &mut insert.source {
                    query.bind_schema_names(&read, state, &self.search_path);
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
            Statement::DropIndex(index) => {
                index.name = state.resolve_index_key(&index.name, &self.search_path);
                index.table = state.index_table_key(&index.name);
            }
            Statement::Sequence(ddl) => ddl.bind_schema_names(&read, &create)?,
            _ => {}
        }
        if let Some(returning) = bound.returning_mut() {
            returning.bind_schema_names(&read, state, &self.search_path);
        }
        if let Statement::Query(query) = &bound.statement
            && let Some(plan) = query::plan::Plan::correlated_in(query, state)
        {
            bound.statement = Statement::Plan(plan);
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
    fn rebinding_shares_original_statement_across_search_paths() {
        let mut state = Relational::default();
        run(&mut state, "CREATE SCHEMA a");
        run(&mut state, "CREATE SCHEMA b");
        run(&mut state, "CREATE TABLE a.t(id int PRIMARY KEY)");
        run(&mut state, "CREATE TABLE b.t(id int PRIMARY KEY)");
        let mut prepared = Prepared::parse("SELECT id FROM t", 1, state.epoch).unwrap();
        prepared.search_path = vec!["a".into()];
        let mut first = prepared.bind_schemas(&state).unwrap();
        assert_eq!(first.storage_table(), Some("a\0t"));
        first.search_path = vec!["b".into()];
        let second = first.bind_schemas(&state).unwrap();
        assert_eq!(second.storage_table(), Some("b\0t"));
        assert!(Arc::ptr_eq(
            first.unbound_statement.as_ref().unwrap(),
            second.unbound_statement.as_ref().unwrap(),
        ));
        assert_eq!(prepared.storage_table(), Some("t"));
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
