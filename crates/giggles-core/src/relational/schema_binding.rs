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
        for (key, table) in std::mem::take(std::sync::Arc::make_mut(&mut self.tables)) {
            let key = if key == table.name {
                relation_key(self.table_schema(table.id), &table.name)
            } else {
                key
            };
            if tables.insert(key, table).is_some() {
                return Err("duplicate normalized relation key");
            }
        }
        self.tables = Arc::new(tables);
        let mut renamed = BTreeMap::new();
        let ext = std::sync::Arc::make_mut(&mut self.ext);
        for sequence in ext.sequences.values_mut() {
            if !sequence.name.contains('\0')
                && let Some((table, _)) = sequence.owned_by
                && let Some(schema) = ext.table_schemas.get(&table)
            {
                let key = relation_key(schema, &sequence.name);
                renamed.insert(sequence.name.clone(), key.clone());
                sequence.name = key;
            }
        }
        defaults::normalize_r1_sequence_defaults(std::sync::Arc::make_mut(&mut self.ext), &renamed)
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
            .map(|d| d.table)
            .or_else(|| self.gin_index(key).map(|(_, index)| index.table))
            .and_then(|id| self.tables.values().find(|t| t.id == id))
            .or_else(|| sql_index::primary_table(self, key))
            .map(|t| self.table_key(t))
    }

}

impl Prepared {
    pub(crate) fn bind_schemas(&self, state: &Relational) -> R<Self> {
        let _date_order = super::clock::LiteralDateOrder::enter(self.literal_date_order);
        if self.relations.is_empty()
            && !matches!(self.statement.get(), Statement::Plan(_))
            && !matches!(self.statement.get(), Statement::Query(q) if q.has_domain_expression() || !state.ext.routines.is_empty() || self.unbound_statement.is_some())
        {
            return Ok(self.clone());
        }
        let unbound = self
            .unbound_statement
            .as_ref()
            .map(Arc::clone)
            .unwrap_or_else(|| Arc::new(self.statement.get().clone()));
        let mut bound = Self {
            domain: self.domain,
            epoch: self.epoch,
            transaction: self.transaction,
            engine_generation: self.engine_generation,
            literal_date_order: self.literal_date_order,
            statement: prepared_syntax::Syntax::new((*unbound).clone()),
            native_delete: self.native_delete.clone(),
            native_insert_many: self.native_insert_many,
            unbound_statement: Some(unbound),
            params: self.params,
            parameter_types: self.parameter_types.clone(),
            declared_enum_parameters: self.declared_enum_parameters.clone(),
            relations: self.relations.clone(),
            search_path: self.search_path.clone(),
            schema_user: self.schema_user.clone(),
            extension_owner: self.extension_owner.clone(),
            skip_routine_body_check: self.skip_routine_body_check,
            warnings: self.warnings.clone(),
            authorization_has_views: false,
            // A caller must witness the catalog version of this binding anew.
            authorization_binding: self.authorization_binding.map(AuthorizationBinding::without_version),
            privilege_inquiry: self.privilege_inquiry,
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
        let created = match bound.statement.get() {
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
        match bound.statement.get_mut() {
            Statement::View(ddl) => {
                if let Some(query) = &mut ddl.query {
                    ddl.names[0] = create(&ddl.names[0])?;
                    query.bind_schema_names(&read, state, &self.search_path);
                } else { ddl.names.iter_mut().for_each(read); }
            }
            Statement::Comment(comment) => read(&mut comment.table),
            Statement::Trigger(ddl) => ddl.bind_table(read),
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
                insert.bind_schema_names(&read, state, &self.search_path)?;
            }
            Statement::AlterTable(alter) => {
                read(&mut alter.table);
                for action in &mut alter.actions {
                    match action {
                        alter_table::Action::Constraint(definition) => definitions(std::slice::from_mut(definition)),
                        alter_table::Action::Add(_, _, _, _, _, _, _, _, constraints) => definitions(&mut constraints.definitions),
                        _ => {}
                    }
                }
            }
            Statement::CreateIndex(index) => read(&mut index.table),
            Statement::DropIndex(index) => {
                index.name = state.resolve_index_key(&index.name, &self.search_path);
                if let Some(rename) = &index.rename {
                    if state.gin_index(&index.name).is_none() {
                        return Err(SqlError::Syntax);
                    }
                    ident::ident(rename)?;
                }
                index.table = state.index_table_key(&index.name);
            }
            Statement::Sequence(ddl) => ddl.bind_schema_names(&read, &create)?,
            _ => {}
        }
        if let Some(returning) = bound.returning_mut() {
            returning.bind_schema_names(&read, state, &self.search_path);
        }
        if let Statement::Query(query) = bound.statement.get()
            && let Some(plan) = query::plan::Plan::correlated_in(query, state)
        {
            *bound.statement.get_mut() = Statement::Plan(plan);
        }
        let tables = bound.authorization_tables_ref()?;
        let has_views = tables.iter().any(|name| state.view_names.contains_key(name));
        let privilege_inquiry = tables.iter().any(|name|
            state.view_names.get(name).is_some_and(|(_,p)| p.as_ref().is_ok_and(|p| p.has_privilege_inquiry())));
        bound.authorization_has_views = has_views;
        bound.privilege_inquiry |= privilege_inquiry;
        Ok(bound)
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum AuthorizationBinding {
    Sql(Option<u64>),
    ImplicitBlock(bool),
}
impl AuthorizationBinding {
    fn without_version(self) -> Self {
        match self {
            Self::Sql(_) => Self::Sql(None),
            control => control,
        }
    }
}

/// Exact identity of an eligible, successfully bound SQL authorization plan.
pub struct AuthorizationPlanKey {
    // Hold the allocation identity without retaining a closed statement's
    // potentially large AST. The allocation cannot be reused while Weak lives.
    source: std::sync::Weak<Statement>,
    domain: u128,
    epoch: u64,
    transaction: Option<u128>,
    literal_date_order: giggles_pg_types::temporal::DateOrder,
    params: usize,
    parameter_types: Vec<Option<DataType>>,
    relations: Vec<schemas::Reference>,
    search_path: Vec<String>,
    schema_user: String,
    has_views: bool,
    privilege_inquiry: bool,
    budgets: Option<(usize, usize, bool)>,
}
impl AuthorizationPlanKey {
    pub fn matches(&self, prepared: &Prepared, version: u64) -> bool {
        prepared.authorization_source(version).is_some_and(|source|
            std::ptr::eq(self.source.as_ptr(), Arc::as_ptr(source)))
            && self.domain == prepared.domain
            && self.epoch == prepared.epoch
            && self.transaction == prepared.transaction
            && self.literal_date_order == prepared.literal_date_order
            && self.params == prepared.params
            && self.parameter_types == prepared.parameter_types
            && self.relations == prepared.relations
            && self.search_path == prepared.search_path
            && self.schema_user == prepared.schema_user
            && self.has_views == prepared.authorization_has_views
            && self.privilege_inquiry == prepared.privilege_inquiry
            && self.budgets == prepared.authorization_budgets()
    }
}
impl Prepared {
    pub(crate) fn enable_sql_authorization_binding(&mut self) {
        // The other statement variants (including DDL and TRUNCATE) always
        // take the original path. Constraint references occur only in DDL.
        if matches!(self.statement.get(), Statement::Select { .. } | Statement::CompositeSelect(_)
            | Statement::Query(_) | Statement::Plan(_) | Statement::Insert { .. }
            | Statement::InsertMulti(_) | Statement::InsertExpressions(_)
            | Statement::Update { .. } | Statement::UpdateExpressions { .. }
            | Statement::Delete { .. })
        {
            self.authorization_binding = Some(AuthorizationBinding::Sql(None));
        }
    }

    pub(crate) fn mark_implicit_authorization_block(&mut self, commit: bool) {
        self.authorization_binding = Some(AuthorizationBinding::ImplicitBlock(commit));
    }

    pub fn implicit_authorization_block(&self) -> Option<bool> {
        match self.authorization_binding {
            Some(AuthorizationBinding::ImplicitBlock(commit)) => Some(commit),
            _ => None,
        }
    }

    pub fn has_authorization_binding(&self) -> bool {
        matches!(self.authorization_binding, Some(AuthorizationBinding::Sql(Some(_)) | AuthorizationBinding::ImplicitBlock(_)))
    }

    pub(crate) fn has_sql_authorization_binding(&self) -> bool {
        matches!(self.authorization_binding, Some(AuthorizationBinding::Sql(_)))
    }

    pub(crate) fn clear_authorization_binding_version(&mut self) {
        self.authorization_binding = self.authorization_binding.map(AuthorizationBinding::without_version);
    }

    pub(crate) fn set_authorization_binding_version(&mut self, version: u64) {
        // A table-free statement may have taken bind_schemas' no-op path.
        self.unbound_statement.get_or_insert_with(|| Arc::new(self.statement.get().clone()));
        self.authorization_binding = Some(AuthorizationBinding::Sql(Some(version)));
    }

    fn authorization_source(&self, version: u64) -> Option<&Arc<Statement>> {
        match self.authorization_binding {
            Some(AuthorizationBinding::Sql(Some(bound))) if bound == version => self.unbound_statement.as_ref(),
            _ => None,
        }
    }

    pub fn authorization_may_parse_defaults(&self) -> bool {
        matches!(self.statement.get(), Statement::Plan(_) | Statement::Insert { .. }
            | Statement::InsertMulti(_) | Statement::InsertExpressions(_)
            | Statement::UpdateExpressions { .. })
    }

    fn authorization_budgets(&self) -> Option<(usize, usize, bool)> {
        // A write or a nested mutation can parse a column's default.
        self.authorization_may_parse_defaults()
            .then(|| (max_statement_tokens(), max_value_bytes(), wide_values()))
    }

    pub fn authorization_plan_key(&self, version: u64) -> Option<AuthorizationPlanKey> {
        let source = self.authorization_source(version)?;
        if self.has_privilege_inquiry() {
            return None;
        }
        Some(AuthorizationPlanKey {
            source: Arc::downgrade(source),
            domain: self.domain,
            epoch: self.epoch,
            transaction: self.transaction,
            literal_date_order: self.literal_date_order,
            params: self.params,
            parameter_types: self.parameter_types.clone(),
            relations: self.relations.clone(),
            search_path: self.search_path.clone(),
            schema_user: self.schema_user.clone(),
            has_views: self.authorization_has_views,
            privilege_inquiry: self.privilege_inquiry,
            budgets: self.authorization_budgets(),
        })
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
    fn authorization_dependencies_follow_the_bound_syntax() {
        let mut state = Relational::default();
        for sql in [
            "CREATE SCHEMA a",
            "CREATE SCHEMA b",
            "CREATE TABLE a.t(id int PRIMARY KEY)",
            "CREATE TABLE b.t(id int PRIMARY KEY)",
        ] {
            run(&mut state, sql);
        }
        let mut prepared = Prepared::parse("SELECT id FROM t", 1, state.epoch).unwrap();
        prepared.search_path = vec!["a".into()];
        AUTHORIZATION_DEPENDENCY_WALKS.set(0);
        let mut first = prepared.bind_schemas(&state).unwrap();
        assert_eq!(first.authorization_tables().unwrap(), ["a\0t"]);
        assert_eq!(first.authorization_tables_ref().unwrap(), ["a\0t"]);
        assert_eq!(AUTHORIZATION_DEPENDENCY_WALKS.get(), 1);

        first.search_path = vec!["b".into()];
        let mut second = first.bind_schemas(&state).unwrap();
        assert_eq!(second.authorization_tables().unwrap(), ["b\0t"]);
        assert_eq!(AUTHORIZATION_DEPENDENCY_WALKS.get(), 2);

        *second.statement.get_mut() = first.statement.get().clone();
        assert_eq!(second.authorization_tables().unwrap(), ["a\0t"]);
        assert_eq!(AUTHORIZATION_DEPENDENCY_WALKS.get(), 3);
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
        let table = std::sync::Arc::make_mut(&mut state.tables).remove("a\0t").unwrap();
        std::sync::Arc::make_mut(&mut state.tables).insert("b\0t".into(), table.clone());
        assert!(state.validate().is_err());
        std::sync::Arc::make_mut(&mut state.tables).remove("b\0t");
        std::sync::Arc::make_mut(&mut state.tables).insert("t".into(), table);
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
    #[test]
    fn drop_authority_targets_precede_catalog_binding() {
        assert_eq!(Prepared::drop_table_targets("DROP VIEW information_schema.tables").unwrap(), vec!["information_schema\0tables"]);
        assert!(Prepared::drop_table_targets("DROP INDEX pg_catalog.pg_class_oid_index").unwrap().is_empty());
        assert_eq!(Prepared::drop_index_target("DROP INDEX IF EXISTS pg_catalog.pg_class_oid_index").unwrap(), Some("pg_catalog\0pg_class_oid_index".into()));
        assert!(Prepared::drop_index_target("DROP INDEX pg_catalog.pg_class_oid_index extra").is_err());
        assert_eq!(Prepared::parse("DROP INDEX pg_catalog.pg_class_oid_index", 0, 0).unwrap_err(), SqlError::Reserved);
    }

}
