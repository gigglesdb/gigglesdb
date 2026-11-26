//! Bind SQL relation references without changing their user-visible names.
use super::*;
use std::{borrow::Cow, sync::{Arc, Mutex}};

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

pub(crate) fn missing_relation(key: &str) -> SqlError {
    let name = key.strip_prefix("\0\0").unwrap_or(key).replace('\0', ".");
    user_types::view_replace_error(format!(r#"relation "{name}" does not exist"#));
    SqlError::UnknownTable
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
        self.resolve_relation_key_ref(name, path).into_owned()
    }

    fn resolve_relation_key_ref<'a>(&self, name: &'a str, path: &[String]) -> Cow<'a, str> {
        if name.contains('\0') {
            let (schema, unqualified) = split_key(name);
            return Cow::Borrowed(if schema == "public" { unqualified } else { name });
        }
        for schema in path {
            let key = if schema == "public" { Cow::Borrowed(name) }
                else { Cow::Owned(relation_key(schema, name)) };
            if self.relation_name_taken(&key) {
                return key;
            }
        }
        // A missing relation must not accidentally resolve to public.
        Cow::Owned(format!("\0\0{name}"))
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

/// Session-local reuse of successful schema rewrites. Keep just the last
/// binding so named statements cannot collectively retain old catalog images.
#[derive(Default)]
pub(crate) struct SchemaBindingMemo(Mutex<Option<SchemaBindingEntry>>);

struct SchemaBindingEntry {
    source: Arc<Statement>,
    catalog: Arc<Relational>,
    relations: Vec<schemas::Reference>,
    search_path: Vec<String>,
    schema_user: String,
    date_order: giggles_pg_types::temporal::DateOrder,
    syntax: prepared_syntax::Syntax,
}

/// Witness for a catalog obtained by the current metadata or execution read.
#[derive(Clone, Copy)]
pub(crate) struct SchemaBinding<'a> {
    memo: &'a SchemaBindingMemo,
    catalog: &'a Arc<Relational>,
}

impl SchemaBindingMemo {
    pub(crate) fn at<'a>(&'a self, catalog: &'a Arc<Relational>) -> SchemaBinding<'a> {
        SchemaBinding { memo: self, catalog }
    }
}

impl SchemaBinding<'_> {
    pub(crate) fn bind(self, prepared: &Prepared) -> R<Prepared> {
        let state = self.catalog;
        if !prepared.needs_schema_binding(state) {
            return Ok(prepared.clone());
        }
        let source = prepared.schema_binding_source();
        // Shared session APIs can bind concurrently. Bypass the memo if
        // another caller is using this session's entry.
        let Ok(mut last) = self.memo.0.try_lock() else {
            return prepared.bind_schemas(state);
        };
        if let Some(entry) = last.as_ref()
            && Arc::ptr_eq(source, &entry.source)
            && Arc::ptr_eq(state, &entry.catalog)
            && prepared.relations == entry.relations
            && prepared.search_path == entry.search_path
            && prepared.schema_user == entry.schema_user
            && prepared.literal_date_order == entry.date_order
        {
            return Ok(prepared.with_schema_binding(source.clone(), entry.syntax.clone()));
        }
        *last = None;
        let projection_reads = (!state.view_names.is_empty()).then(query::views::projection_reads);
        let bound = prepared.bind_schema_names(state, source.clone())?;
        // View predicates can be reparsed under the ambient lexer budgets.
        // Unsupported projections can publish diagnostics even on a successful bind.
        if !bound.authorization_has_views
            && projection_reads.is_none_or(|before| before == query::views::projection_reads())
            && !state.view_names.values().any(|(_, view)| *view == Err(SqlError::Unsupported))
        {
            *last = Some(SchemaBindingEntry {
                source: source.clone(),
                catalog: state.clone(),
                relations: prepared.relations.clone(),
                search_path: prepared.search_path.clone(),
                schema_user: prepared.schema_user.clone(),
                date_order: prepared.literal_date_order,
                syntax: bound.statement.clone(),
            });
        }
        Ok(bound)
    }

    /// A statement fork may hydrate rows, replace index entries or change
    /// allocator ceilings. Page preparation can also change the binding
    /// catalog itself, so check every catalog input before reusing its Arc.
    pub(super) fn bind_fork(self, prepared: &Prepared, fork: &Relational) -> R<Prepared> {
        let state = self.catalog;
        if std::ptr::eq(state.as_ref(), fork)
            || Arc::ptr_eq(&state.tables, &fork.tables)
            && Arc::ptr_eq(&state.ext, &fork.ext)
            && state.view_names == fork.view_names
            && state.secondary.descriptors().iter().map(|index| (&index.name, index.table, index.id))
                .eq(fork.secondary.descriptors().iter().map(|index| (&index.name, index.table, index.id)))
        {
            self.bind(prepared)
        } else {
            prepared.bind_schemas(fork)
        }
    }
}

impl Prepared {
    fn needs_schema_binding(&self, state: &Relational) -> bool {
        !self.relations.is_empty()
            || matches!(self.statement.get(), Statement::Plan(_))
            || matches!(self.statement.get(), Statement::Query(q) if q.has_domain_expression() || !state.ext.routines.is_empty() || self.unbound_statement.is_some())
    }

    fn schema_binding_source(&self) -> &Arc<Statement> {
        self.unbound_statement.as_ref().unwrap_or_else(|| self.statement.shared())
    }

    pub(crate) fn bind_schemas(&self, state: &Relational) -> R<Self> {
        if !self.needs_schema_binding(state) {
            return Ok(self.clone());
        }
        self.bind_schema_names(state, self.schema_binding_source().clone())
    }

    pub(crate) fn bind_schemas_with(
        &self,
        state: &Relational,
        binding: Option<SchemaBinding<'_>>,
    ) -> R<Self> {
        match binding {
            Some(binding) => binding.bind_fork(self, state),
            None => self.bind_schemas(state),
        }
    }

    fn with_schema_binding(&self, source: Arc<Statement>, syntax: prepared_syntax::Syntax) -> Self {
        Self {
            domain: self.domain,
            epoch: self.epoch,
            transaction: self.transaction,
            engine_generation: self.engine_generation,
            literal_date_order: self.literal_date_order,
            statement: syntax,
            native_delete: self.native_delete.clone(),
            native_insert_many: self.native_insert_many,
            unbound_statement: Some(source),
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
        }
    }

    fn bind_schema_names(&self, state: &Relational, unbound: Arc<Statement>) -> R<Self> {
        let _date_order = super::clock::LiteralDateOrder::enter(self.literal_date_order);
        let syntax = prepared_syntax::Syntax::new((*unbound).clone());
        let mut bound = self.with_schema_binding(unbound, syntax);
        let create = |name: &str| -> R<String> {
            let schema = if name.contains('\0') {
                split_key(name).0.to_owned()
            } else {
                self.creation_schema(state)?
            };
            Ok(relation_key(&schema, split_key(name).1))
        };
        let read = |name: &mut String| {
            match state.resolve_relation_key_ref(name, &self.search_path) {
                Cow::Borrowed(key) if key.len() == name.len()
                    || (name.starts_with("public\0") && !state.relation_name_taken(key)) => {}
                key => *name = key.into_owned(),
            }
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
                if let Some(create) = as_query {
                    create.source.bind_schema_names(&read, state, &self.search_path)?;
                }
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
                index.table = state.index_table_key(&index.name);
                if let Some(rename) = &index.rename {
                    let target = relation_key(split_key(&index.name).0, rename);
                    let has_index_storage = state.secondary_descriptors_engine().iter()
                        .any(|descriptor| descriptor.name == index.name && state.has_index_storage(descriptor));
                    if index.table.is_none() || state.gin_index(&index.name).is_none()
                        && !has_index_storage && !sql_index::name_taken(state, &target)
                    {
                        return Err(SqlError::Syntax);
                    }
                    ident::ident(rename)?;
                }
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
        self.unbound_statement.get_or_insert_with(|| self.statement.share());
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

        let unchanged = second.clone();
        *second.statement.get_mut() = first.statement.get().clone();
        assert_eq!(second.authorization_tables().unwrap(), ["a\0t"]);
        assert_eq!(AUTHORIZATION_DEPENDENCY_WALKS.get(), 3);
        assert_eq!(unchanged.storage_table(), Some("b\0t"));
    }

    fn memo_fixture() -> Arc<Relational> {
        let mut state = Relational::default();
        for sql in [
            "CREATE SCHEMA a",
            "CREATE SCHEMA b",
            "CREATE TABLE a.t(id int PRIMARY KEY)",
            "CREATE TABLE b.t(id int PRIMARY KEY)",
        ] {
            run(&mut state, sql);
        }
        Arc::new(state)
    }

    #[test]
    fn schema_memo_keys_the_original_statement() {
        let state = memo_fixture();
        let memo = SchemaBindingMemo::default();
        let binding = memo.at(&state);
        let first = Prepared::parse("SELECT id FROM a.t WHERE id = 1", 1, state.epoch).unwrap();
        let second = Prepared::parse("SELECT id FROM a.t WHERE id = 2", 1, state.epoch).unwrap();
        let first = binding.bind(&first).unwrap();
        let second = binding.bind(&second).unwrap();
        assert!(!Arc::ptr_eq(first.statement.shared(), second.statement.shared()));
    }

    #[test]
    fn schema_memo_keys_the_search_path_and_creation_references() {
        let state = memo_fixture();
        let memo = SchemaBindingMemo::default();
        let binding = memo.at(&state);
        let mut read = Prepared::parse("SELECT id FROM t", 1, state.epoch).unwrap();
        read.set_schema_context(vec!["a".into()], "postgres".into());
        let bound = binding.bind(&read).unwrap();
        assert_eq!(bound.storage_table(), Some("a\0t"));

        read.set_schema_context(vec!["b".into()], "postgres".into());
        assert_eq!(binding.bind(&read).unwrap().storage_table(), Some("b\0t"));

        let mut create = Prepared::parse("CREATE TABLE x(id int)", 1, state.epoch).unwrap();
        let first = binding.bind(&create).unwrap();
        create.relations.iter_mut().find(|reference| reference.create).unwrap().schema = Some("a".into());
        let second = binding.bind(&create).unwrap();
        let Statement::Create { name: first, .. } = first.statement.get() else { unreachable!() };
        let Statement::Create { name: second, .. } = second.statement.get() else { unreachable!() };
        assert_eq!(first, "x");
        assert_eq!(second, "a\0x");
    }

    #[test]
    fn schema_memo_keys_date_order_and_schema_user() {
        use giggles_pg_types::temporal::DateOrder;

        let state = memo_fixture();
        let memo = SchemaBindingMemo::default();
        let binding = memo.at(&state);
        let mut prepared = Prepared::parse(
            "SELECT '01/02/2024'::date FROM a.t", 1, state.epoch,
        ).unwrap();
        prepared.literal_date_order = DateOrder::Mdy;
        let original = binding.bind(&prepared).unwrap();
        let same = binding.bind(&prepared).unwrap();
        assert!(Arc::ptr_eq(original.statement.shared(), same.statement.shared()));

        prepared.literal_date_order = DateOrder::Dmy;
        let date_changed = binding.bind(&prepared).unwrap();
        assert!(!Arc::ptr_eq(same.statement.shared(), date_changed.statement.shared()));
        assert_eq!(date_changed.literal_date_order, DateOrder::Dmy);

        // Different roles can expand to the same path when both lack a schema.
        prepared.set_schema_context(prepared.search_path.clone(), "other".into());
        let user_changed = binding.bind(&prepared).unwrap();
        assert!(!Arc::ptr_eq(date_changed.statement.shared(), user_changed.statement.shared()));
    }

    #[test]
    fn schema_memo_preserves_current_prepared_fields_and_clears_the_version() {
        let state = memo_fixture();
        let memo = SchemaBindingMemo::default();
        let binding = memo.at(&state);
        let mut prepared = Prepared::parse("SELECT id FROM a.t WHERE id = $1", 1, state.epoch).unwrap();
        let first = binding.bind(&prepared).unwrap();

        prepared.domain = 2;
        prepared.epoch = 42;
        prepared.transaction = Some(5);
        prepared.parameter_types = vec![Some(DataType::Integer)];
        prepared.privilege_inquiry = true;
        prepared.enable_sql_authorization_binding();
        prepared.set_authorization_binding_version(17);
        let rebound = binding.bind(&prepared).unwrap();

        assert!(Arc::ptr_eq(first.statement.shared(), rebound.statement.shared()));
        assert_eq!((rebound.domain, rebound.epoch, rebound.transaction), (2, 42, Some(5)));
        assert_eq!(rebound.parameter_types, [Some(DataType::Integer)]);
        assert!(rebound.has_privilege_inquiry());
        assert!(matches!(rebound.authorization_binding, Some(AuthorizationBinding::Sql(None))));
    }

    #[test]
    fn schema_memo_rechecks_view_privilege_inquiry_after_token_budget_change() {
        use crate::storage::{SqlViewColumn, SqlViewDef, SqlViewDependency};

        let mut state = Relational::default();
        run(&mut state, "CREATE TABLE t(id int PRIMARY KEY)");
        let table = &state.tables["t"];
        let column = table.logical_columns().next().unwrap();
        let view = SqlViewDef {
            schema: 0,
            name: "v".into(),
            select: "SELECT id FROM t WHERE has_table_privilege('t', 'SELECT')".into(),
            columns: vec![SqlViewColumn {
                id: 101,
                name: "id".into(),
                storage_type: DataType::Integer,
                pg_type_oid: 23,
                typmod: -1,
                source: Some((table.id, column.id)),
            }],
            dependencies: vec![SqlViewDependency { relation: table.id, columns: vec![column.id] }],
        };
        state.catalog_views(&BTreeMap::from([(100, view)])).unwrap();
        let state = Arc::new(state);
        let prepared = Prepared::parse("SELECT id FROM v", 1, state.epoch).unwrap();
        let memo = SchemaBindingMemo::default();
        let binding = memo.at(&state);

        let low = {
            let _budget = StatementBudget::enter(5, true);
            binding.bind(&prepared).unwrap()
        };
        assert!(low.authorization_has_views());
        assert!(!low.has_privilege_inquiry());

        let _budget = StatementBudget::enter(100, true);
        let fresh = prepared.bind_schemas(&state).unwrap();
        let rebound = binding.bind(&prepared).unwrap();
        assert!(fresh.has_privilege_inquiry());
        assert_eq!(rebound.has_privilege_inquiry(), fresh.has_privilege_inquiry());
        assert_eq!(rebound.authorization_has_views(), fresh.authorization_has_views());
    }

    #[test]
    fn schema_memo_republishes_unsupported_view_projection_diagnostic() {
        let mut state = Relational::default();
        state.view_names.insert("v".into(), (100, Err(SqlError::Unsupported)));
        let state = Arc::new(state);
        // The searched DELETE binds a synthetic query over v, even though
        // it requires no read privilege on v's columns.
        let prepared = Prepared::parse("DELETE FROM v WHERE true", 1, state.epoch).unwrap();
        let memo = SchemaBindingMemo::default();
        let binding = memo.at(&state);

        for _ in 0..2 {
            let bound = binding.bind(&prepared).unwrap();
            assert!(!bound.authorization_has_views());
            assert_eq!(
                user_types::take_sql_error_message().as_deref(),
                Some("view projection bindings unavailable; recreate the view"),
            );
        }
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
