//! Relations share a namespace within each schema, matching PostgreSQL.
//! Server policy: superusers create schemas; schema owners/superusers may drop them.
//! Table creation permits superusers, schema owners, or explicit schema CREATE grants.
//! Public retains its existing grant model. V2 explicitly refuses schema DDL.
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
    matches!(tokens.first(), Some(Token::Word(w)) if w == "create" || w == "drop")
        && tokens.get(1) == Some(&Token::Word("schema".into()))
}
pub(super) fn parse(p: &mut Parser) -> R<Ddl> {
    let create = p.word("create");
    if !create {
        p.need("drop")?;
    }
    p.need("schema")?;
    let optional = p.word("if");
    if optional {
        if create {
            p.need("not")?;
        }
        p.need("exists")?;
    }
    let name = p.name()?;
    let owner = if create && p.word("authorization") {
        Some(alter_table::raw_name(p)?)
    } else {
        None
    };
    let cascade = !create && p.word("cascade");
    if !create && !cascade {
        p.word("restrict");
    }
    p.sym(';');
    if p.at != p.tokens.len() {
        return Err(SqlError::Syntax);
    }
    Ok(Ddl {
        create,
        name,
        owner,
        optional,
        cascade,
    })
}
impl Relational {
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
        if ext.schemas.len() > MAX_TABLES || ext.table_schemas.len() > MAX_TABLES {
            return Err(SqlError::Limit);
        }
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
        for (key, table) in &self.tables {
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
        if ddl.create {
            if exists {
                return if ddl.optional {
                    Ok(Default::default())
                } else {
                    Err(SqlError::DuplicateObject)
                };
            }
            if self.ext.schemas.len() >= MAX_TABLES {
                return Err(SqlError::Limit);
            }
            let id = allocate(&mut self.next_id)?;
            self.ext.schemas.insert(
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
            if (!tables.is_empty() || !types.is_empty()) && !ddl.cascade {
                return Err(SqlError::DependentObjects);
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
            for table in tables {
                self.drop_table_with_actions(&table, true)?;
            }
            self.ext
                .sequences
                .retain(|_, sequence| split_key(&sequence.name).0 != ddl.name);
            self.ext.types.retain(|id, _| !types.contains(id));
            self.ext.schemas.remove(&ddl.name);
        }
        Self::normalize_ext(&mut self.ext);
        self.epoch = self.epoch.checked_add(1).ok_or(SqlError::Overflow)?;
        Ok(SqlResult {
            affected: 1,
            ..Default::default()
        })
    }
}
impl Prepared {
    pub fn schema_ddl(&self) -> Option<(&str, Option<&str>, bool)> {
        if let Statement::Schema(ddl) = &self.statement {
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
        self.search_path = path;
        self.schema_user = user;
    }
    pub(crate) fn validate_schemas(&self, state: &Relational) -> R<()> {
        for r in &self.relations {
            if r.create {
                self.creation_schema(state)?;
                continue;
            }
            if r.schema
                .as_deref()
                .is_some_and(|schema| schema != "public" && !state.ext.schemas.contains_key(schema))
            {
                return Err(SqlError::UnknownTable);
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
