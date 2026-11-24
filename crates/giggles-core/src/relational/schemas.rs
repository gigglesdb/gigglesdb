//! Relations share a namespace within each schema, matching PostgreSQL.
//! Server policy: superusers create schemas; schema owners/superusers may drop them.
//! Table creation permits superusers, schema owners, or explicit schema CREATE grants.
//! Public retains its existing grant model across both storage engines.
use super::*;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Schema {
    pub(crate) id: u64,
    #[serde(deserialize_with = "super::catalog_ext::name")]
    pub(crate) owner: String,
}
#[derive(Debug, Clone)]
pub(super) struct Ddl {
    pub create: bool,
    pub rename: Option<String>,
    pub name: String,
    pub owner: Option<String>,
    pub optional: bool,
    pub cascade: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Reference {
    pub name: String,
    pub schema: Option<String>,
    pub create: bool,
}
impl Reference {
    pub fn read(name: String, schema: Option<String>) -> Self {
        Self {
            name,
            schema,
            create: false,
        }
    }
}
pub(super) fn claims(tokens: &[Token]) -> bool {
    matches!(tokens.first(), Some(Token::Word(w)) if w == "create" || w == "drop" || w == "alter")
        && tokens.get(1) == Some(&Token::Word("schema".into()))
}
pub(super) fn parse(p: &mut Parser) -> R<Ddl> {
    let alter = p.word("alter");
    let create = p.word("create");
    if !create && !alter {
        p.need("drop")?;
    }
    p.need("schema")?;
    let optional = !alter && p.word("if");
    if optional {
        if create {
            p.need("not")?;
        }
        p.need("exists")?;
    }
    let name = p.name()?;
    let rename = if alter { p.need("rename")?; p.need("to")?; Some(p.name()?) } else { None };
    let owner = if create && p.word("authorization") {
        Some(alter_table::raw_name(p)?)
    } else {
        None
    };
    let cascade = !create && !alter && p.word("cascade");
    if !create && !alter && !cascade {
        p.word("restrict");
    }
    p.sym(';');
    if p.at != p.tokens.len() {
        return Err(SqlError::Syntax);
    }
    Ok(Ddl {
        create,
        rename,
        name,
        owner,
        optional,
        cascade,
    })
}
impl Relational {
    // Explicit DDL schemas use 3F000; unresolved search-path sentinels are not schemas.
    pub(crate) fn require_relation_schema(&self, name: &str) -> R<()> {
        if let Some((schema, _)) = name.split_once('\0')
            && !schema.is_empty() && schema != "public" && !self.ext.schemas.contains_key(schema)
        {
            return Err(SqlError::InvalidSchema);
        }
        Ok(())
    }
    pub(crate) fn table_schema(&self, id: u64) -> &str {
        self.ext
            .table_schemas
            .get(&id)
            .map_or("public", String::as_str)
    }
    pub(super) fn validate_schemas(
        &self,
        ext: &CatalogExt,
        next_id: u64,
        excluded: Option<u64>,
    ) -> R<()> {
        let mut ids = BTreeSet::new();
        for (name, schema) in &ext.schemas {
            ident(name)?;
            if schema.owner.is_empty()
                || schema.owner.len() > 63
                || name == "public"
                || schema.id == 0
                || schema.id >= next_id
                || !ids.insert(schema.id)
                || self
                    .tables
                    .values()
                    .any(|t| t.id == schema.id || t.columns.iter().any(|c| c.id == schema.id))
            {
                return Err(SqlError::Type);
            }
        }
        for (key, table) in self.tables.iter() {
            if Some(table.id) != excluded {
                ident(&table.name)?;
                schema_binding::validate_key(key)?;
                let schema = ext
                    .table_schemas
                    .get(&table.id)
                    .map_or("public", String::as_str);
                if *key != relation_key(schema, &table.name) {
                    return Err(SqlError::Type);
                }
            }
        }
        for (table, schema) in &ext.table_schemas {
            if Some(*table) == excluded
                || !self.tables.values().any(|t| t.id == *table)
                || !ext.schemas.contains_key(schema)
            {
                return Err(SqlError::Type);
            }
        }
        Ok(())
    }
    pub(super) fn schema_ddl(&mut self, ddl: &Ddl, user: &str) -> R<SqlResult> {
        let exists = ddl.name == "public" || self.ext.schemas.contains_key(&ddl.name);
        if let Some(new) = &ddl.rename {
            if !exists { return Err(SqlError::InvalidSchema); }
            if new == "public" || self.ext.schemas.contains_key(new) { return Err(SqlError::DuplicateObject); }
            if ddl.name == "public" { return Err(SqlError::Unsupported); }
            // Relation/type schema relocation has separate stored bindings. Keep
            // this rename lane limited to schemas holding extension routines.
            if self.tables.values().any(|t| self.table_schema(t.id) == ddl.name)
                || self.ext.types.values().any(|t| t.schema() == ddl.name)
                || self.ext.collations.values().any(|c| c.schema == ddl.name)
                || self.ext.sequences.values().any(|s| split_key(&s.name).0 == ddl.name)
            { return Err(SqlError::Unsupported); }
            for column in std::sync::Arc::make_mut(&mut self.ext).columns.values_mut() {
                if let Some(sql) = &mut column.default {
                    *sql = rename_default_schema(sql, &ddl.name, new)?;
                }
            }
            let schema = std::sync::Arc::make_mut(&mut self.ext).schemas.remove(&ddl.name).unwrap();
            std::sync::Arc::make_mut(&mut self.ext).schemas.insert(new.clone(), schema);
            for extension in std::sync::Arc::make_mut(&mut self.ext).extensions.values_mut() {
                if extension.schema == ddl.name { extension.schema = new.clone(); }
            }
        } else if ddl.create {
            if exists {
                return if ddl.optional {
                    Ok(Default::default())
                } else {
                    Err(SqlError::DuplicateObject)
                };
            }
            let id = allocate(&mut self.next_id)?;
            std::sync::Arc::make_mut(&mut self.ext).schemas.insert(
                ddl.name.clone(),
                Schema {
                    id,
                    owner: ddl.owner.clone().unwrap_or_else(|| user.into()),
                },
            );
        } else {
            if !exists {
                return if ddl.optional {
                    Ok(Default::default())
                } else {
                    Err(SqlError::InvalidSchema)
                };
            }
            let tables: Vec<_> = self
                .tables
                .iter()
                .filter(|(_, t)| self.table_schema(t.id) == ddl.name)
                .map(|(key, _)| key.clone())
                .collect();
            let types: BTreeSet<_> = self
                .ext
                .types
                .iter()
                .filter(|(_, ty)| ty.schema() == ddl.name)
                .map(|(id, _)| *id)
                .collect();
            let schema_id = self.ext.schemas.get(&ddl.name).map_or(0, |s| s.id);
            let routine_ids: BTreeSet<_> = self.ext.routines.iter()
                .filter_map(|(&id, r)| (r.schema == schema_id).then_some(id)).collect();
            if !routine_ids.is_empty() && (!ddl.cascade || ddl.name == "public") {
                return Err(SqlError::DependentObjects);
            }
            let extensions: Vec<_> = self.ext.extensions.iter()
                .filter(|(_, e)| e.schema == ddl.name).map(|(n, _)| n.clone()).collect();
            let defaults = if extensions.iter().any(|n| n == "uuid-ossp") {
                extensions::extension_defaults(self, &[ddl.name.clone(), "public".into()])?
            } else { Vec::new() };
            if !extensions.is_empty() && !ddl.cascade {
                user_types::view_replace_error(format!("cannot drop schema {} because other objects depend on it", ddl.name));
                let mut details: Vec<_> = extensions.iter().map(|n| format!("extension {n} depends on schema {}", ddl.name)).collect();
                details.extend(defaults.iter().map(|(_, d, f)| format!("{d} depends on function {}.{f}()", ddl.name)));
                user_types::sql_error_detail(details.join("\n"));
                user_types::sql_error_hint("Use DROP ... CASCADE to drop the dependent objects too.");
                return Err(SqlError::DependentObjects);
            }
            if (!tables.is_empty() || !types.is_empty() || self.ext.collations.values().any(|c| c.schema == ddl.name)) && !ddl.cascade {
                if let Some(c) = self.ext.collations.values().find(|c| c.schema == ddl.name) {
                    user_types::view_replace_error(format!("cannot drop schema {} because other objects depend on it",ddl.name));
                    user_types::sql_error_detail(format!("collation {}.{} depends on schema {}",c.schema,c.name,c.schema));
                }
                return Err(SqlError::DependentObjects);
            }
            if !routine_ids.is_empty() {
                let ext = Arc::make_mut(&mut self.ext);
                ext.routines.retain(|id, _| !routine_ids.contains(id));
                ext.triggers.retain(|_, triggers| {
                    triggers.retain(|_, t| !routine_ids.contains(&t.routine));
                    !triggers.is_empty()
                });
            }
            if ddl.name == "public" {
                return Err(SqlError::Unsupported);
            }
            // Cross-schema columns depending on a type need column-drop planning,
            // which DROP TYPE CASCADE also deliberately does not implement yet.
            if self
                .tables
                .values()
                .filter(|t| self.table_schema(t.id) != ddl.name)
                .any(|t| {
                    t.columns.iter().any(|c| {
                        self.ext
                            .columns
                            .get(&c.id)
                            .and_then(|e| e.type_id)
                            .is_some_and(|id| types.contains(&id))
                    })
                })
            {
                return Err(SqlError::Unsupported);
            }
            let mut cascades: Vec<_> = extensions.iter().map(|n| format!("drop cascades to extension {n}")).collect();
            for (id, description, _) in defaults {
                std::sync::Arc::make_mut(&mut self.ext).columns.get_mut(&id).unwrap().default = None;
                cascades.push(format!("drop cascades to {description}"));
            }
            if cascades.len() == 1 {
                user_types::sql_notice(cascades.remove(0), "00000");
            } else if !cascades.is_empty() {
                user_types::sql_notice_detail(format!("drop cascades to {} other objects", cascades.len()), cascades.join("\n"), "00000");
            }
            for name in extensions { std::sync::Arc::make_mut(&mut self.ext).extensions.remove(&name); }
            for table in tables {
                self.drop_table_with_actions(&table, true)?;
            }
            std::sync::Arc::make_mut(&mut self.ext)
                .sequences.retain(|_, sequence| split_key(&sequence.name).0 != ddl.name);
            let ext = Arc::make_mut(&mut self.ext);
            ext.types.retain(|id, _| !types.contains(id));
            ext.added_enum_labels.retain(|id, _| !types.contains(id));
            std::sync::Arc::make_mut(&mut self.ext).collations.retain(|_,c| c.schema != ddl.name);
            std::sync::Arc::make_mut(&mut self.ext).schemas.remove(&ddl.name);
        }
        Self::normalize_ext(std::sync::Arc::make_mut(&mut self.ext));
        self.epoch = self.epoch.checked_add(1).ok_or(SqlError::Overflow)?;
        Ok(SqlResult {
            affected: 1,
            ..Default::default()
        })
    }
}
impl Prepared {
    pub fn schema_ddl(&self) -> Option<(&str, Option<&str>, bool)> {
        if let Statement::Schema(ddl) = self.statement.get() {
            Some((&ddl.name, ddl.owner.as_deref(), ddl.create))
        } else {
            None
        }
    }
    pub fn target_schema(&self) -> Option<&str> {
        self.relations
            .iter()
            .find(|r| r.create)
            .and_then(|r| r.schema.as_deref())
    }
    pub(crate) fn creation_schema(&self, state: &Relational) -> R<String> {
        let schema = self
            .target_schema()
            .or_else(|| {
                self.search_path
                    .iter()
                    .find(|s| s.as_str() == "public" || state.ext.schemas.contains_key(*s))
                    .map(String::as_str)
            })
            .ok_or(SqlError::InvalidSchema)?;
        if schema != "public" && !state.ext.schemas.contains_key(schema) {
            return Err(SqlError::InvalidSchema);
        }
        Ok(schema.into())
    }
    pub(crate) fn schema_context_differs(&self, path: &[String], user: &str) -> bool {
        self.search_path != path || self.schema_user != user
    }
    pub(crate) fn set_schema_context(&mut self, path: Vec<String>, user: String) {
        if self.schema_context_differs(&path, &user) {
            self.clear_authorization_binding_version();
        }
        self.search_path = path;
        self.schema_user = user;
    }
    pub(crate) fn validate_schemas(&self, state: &Relational) -> R<()> {
        // Trigger DDL builds relation and IF EXISTS diagnostics during execution.
        if matches!(self.statement.get(), Statement::Trigger(_)) { return Ok(()); }
        // DROP SEQUENCE validates schemas in name order during execution.
        if matches!(self.statement.get(), Statement::Sequence(defaults::SequenceDdl::Drop { .. })) {
            return Ok(());
        }
        // Typed index operations can carry their target without parser references.
        if let Statement::DropIndex(index) = self.statement.get() && !index.optional {
            state.require_relation_schema(&index.name)?;
        }
        for r in &self.relations {
            if r.create {
                self.creation_schema(state)?;
                continue;
            }
            let key = r.schema.as_ref().map_or_else(|| r.name.clone(), |s| relation_key(s, &r.name));
            if state.require_relation_schema(&key).is_err() {
                // A view's SELECT still resolves relations with DML semantics.
                let error = match self.statement.get() {
                    Statement::DropTable { optional: true, .. } => continue,
                    Statement::DropIndex(index) if index.optional => continue,
                    Statement::View(ddl) if ddl.query.is_none() && ddl.optional => continue,
                    Statement::View(ddl) if ddl.query.is_some() => SqlError::UnknownTable,
                    Statement::Create { .. } | Statement::AlterTable(_)
                    | Statement::CreateIndex(_) | Statement::DropIndex(_)
                    | Statement::DropTable { .. } | Statement::Truncate { .. }
                    | Statement::Comment(_) | Statement::SecurityLabel(_) | Statement::View(_) => SqlError::InvalidSchema,
                    _ => SqlError::UnknownTable,
                };
                return Err(error);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct SchemaMetadata {
    pub epoch: u64,
    pub schemas: BTreeMap<String, (u64, String)>,
    pub tables: BTreeMap<u64, String>,
}
impl Relational {
    pub(crate) fn schema_metadata(&self) -> SchemaMetadata {
        SchemaMetadata {
            epoch: self.epoch,
            schemas: self
                .ext
                .schemas
                .iter()
                .map(|(n, s)| (n.clone(), (s.id, s.owner.clone())))
                .collect(),
            tables: self.ext.table_schemas.clone(),
        }
    }
}
pub fn schema_search_path(path: &str, user: &str) -> Vec<String> {
    lex(path)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|t| match t {
            Token::Word(s) | Token::QuotedIdent(s) | Token::String(s) => {
                Some(if s == "$user" { user.into() } else { s })
            }
            _ => None,
        })
        .collect()
}

// Defaults have no column/alias qualifiers. Rewrite only identifier qualifiers,
// preserving string literals that merely mention a schema or function.
fn rename_default_schema(sql: &str, old: &str, new: &str) -> R<String> {
    let mut tokens = lex(sql)?;
    let mut changed = false;
    for i in 0..tokens.len().saturating_sub(1) {
        if matches!(&tokens[i], Token::Word(n) | Token::QuotedIdent(n) if n == old)
            && tokens[i + 1] == Token::Symbol('.')
        {
            tokens[i] = Token::QuotedIdent(new.into());
            changed = true;
        }
    }
    if !changed { return Ok(sql.into()); }
    tokens.iter().map(|t| Ok(match t {
        Token::Word(n) | Token::Number(n) | Token::Op(n) => n.clone(),
        Token::QuotedIdent(n) => query::constraint_quote(n),
        Token::String(n) => format!("'{}'", n.replace('\'', "''")),
        Token::Symbol(c) => c.to_string(),
        Token::Param(_) => return Err(SqlError::Unsupported),
    })).collect::<R<Vec<_>>>().map(|parts| parts.join(" "))
}
