//! Projection-view DDL; persisted definitions use the kind-17 v2 DTO.
use super::*;
use crate::storage::{CatalogParts, SqlViewColumn, SqlViewDef, SqlViewDependency};

#[derive(Debug, Clone)]
pub(super) struct Ddl {
    pub names: Vec<String>,
    pub query: Option<query::Query>,
    columns: Vec<String>,
    pub(super) optional: bool,
    cascade: bool,
    replace: bool,
}
impl Ddl {
    pub(super) fn typed(name: String, query: Option<query::Query>, cascade: bool) -> Self {
        Self { names: vec![name], query, columns: vec![], optional: false, cascade, replace: false }
    }
}
pub(super) fn claims(tokens: &[Token]) -> bool {
    matches!(tokens.first(), Some(Token::Word(w)) if w == "create" || w == "drop")
        && (matches!(tokens.get(1), Some(Token::Word(w)) if w == "view")
            || matches!(&tokens[1..], [Token::Word(a), Token::Word(b), Token::Word(c), ..] if a == "or" && b == "replace" && c == "view"))
}
pub(super) fn parse(p: &mut Parser) -> R<Ddl> {
    let create = p.word("create");
    if !create { p.need("drop")?; }
    let replace = create && p.word("or");
    if replace { p.need("replace")?; }
    p.need("view")?;
    let optional = !create && p.word("if");
    if optional { p.need("exists")?; }
    let mut names = vec![if create { p.create_table_name()? } else { p.table_name()? }];
    let mut columns = Vec::new();
    let query = if create {
        if p.sym('(') {
            columns.push(p.column_name()?);
            while p.sym(',') { columns.push(p.column_name()?); }
            p.punct(')')?;
        }
        p.need("as")?;
        p.tokens.drain(..p.at); p.at = 0;
        Some(query::Query::parse(p)?)
    } else {
        while p.sym(',') { names.push(p.table_name()?); }
        None
    };
    let cascade = !create && p.word("cascade");
    if !create && !cascade { p.word("restrict"); }
    p.sym(';');
    if p.at != p.tokens.len() { return Err(SqlError::Syntax); }
    Ok(Ddl { names, query, columns, optional, cascade, replace })
}
// Runtime projections retain stable base-column IDs, including through aliases.
// Retire their bindings between ALTER actions so a later RESTRICT sees CASCADE's
// earlier removals. Persisted definitions/comments are removed at publication.
pub(super) fn drop_column_views(state: &mut Relational, column: u64, cascade: bool) -> R<()> {
    let mut dropped = state.view_names.iter().filter(|(_, (_, p))|
        p.as_ref().is_ok_and(|p| p.columns.iter().any(|(c, _)| c.id == column) || p.dependencies.contains(&column)))
        .map(|(name, _)| name.clone()).collect::<BTreeSet<_>>();
    loop {
        let before = dropped.len();
        for (name, (_, projection)) in &state.view_names {
            if projection.as_ref().is_ok_and(|p| p.reads.iter().any(|n| dropped.contains(n))) {
                dropped.insert(name.clone());
            }
        }
        if before == dropped.len() { break; }
    }
    if !cascade && !dropped.is_empty() { return Err(SqlError::DependentObjects); }
    state.view_names.retain(|name, _| !dropped.contains(name));
    Ok(())
}
fn oid(ty: &DataType) -> u32 {
    match ty {
        DataType::Boolean => 16, DataType::SmallInt => 21, DataType::Integer => 23,
        DataType::BigInt => 20, DataType::Text => 25, DataType::Bytes => 17,
        DataType::Numeric => 1700, DataType::Real => 700, DataType::DoublePrecision => 701,
        DataType::Date => 1082, DataType::Time => 1083, DataType::TimeTz => 1266,
        DataType::Timestamp => 1114, DataType::TimestampTz => 1184, DataType::Interval => 1186,
        DataType::Varchar => 1043, DataType::Bpchar => 1042, DataType::Uuid => 2950,
        DataType::Jsonb => 3802, DataType::IntArray => 1007, DataType::BigIntArray => 1016,
        DataType::TextArray => 1009, DataType::UuidArray => 2951,
        DataType::RegType => 2206,
    }
}
fn quote(s: &str) -> String { format!("\"{}\"", s.replace('"', "\"\"")) }
impl Prepared {
    pub fn is_view_replacement(&self) -> bool { matches!(&self.statement, Statement::View(ddl) if ddl.replace) }
    pub(crate) fn clear_view_diagnostics(&self) {
        if matches!(self.statement, Statement::View(_) | Statement::Comment(_) | Statement::SecurityLabel(_)) { user_types::clear_sql_error_message(); }
    }
    pub(crate) fn view_drop_locks(&self, state: &Relational, parts: &CatalogParts) -> BTreeSet<u64> {
        let Statement::View(ddl) = &self.statement else { return BTreeSet::new(); };
        if ddl.query.is_some() { return BTreeSet::new(); }
        let mut ids = ddl.names.iter().filter_map(|n| state.view_names.get(n).map(|v| v.0)).collect::<BTreeSet<_>>();
        if ddl.cascade { loop {
            let before = ids.len();
            for (id, v) in &parts.sql_views {
                if v.dependencies.iter().any(|d| ids.contains(&d.relation)) { ids.insert(*id); }
            }
            if before == ids.len() { break; }
        } }
        ids
    }
    pub(crate) fn check_view_drop(&self, state: &Relational) -> R<()> {
        if self.command() == "DROP TABLE" && self.drop_tables().iter().any(|n| state.view_names.contains_key(n)) {
            return Err(SqlError::WrongObjectType);
        }
        Ok(())
    }
    pub(crate) fn apply_view(&self, state: &Relational, parts: &mut CatalogParts, mut next: u64) -> R<()> {
        let cascade = match &self.statement {
            Statement::DropTable { cascade, .. } => Some(*cascade),
            Statement::AlterTable(alter) => Some(alter.actions.iter().any(|a|
                matches!(a, alter_table::Action::Drop(_, _, true)))),
            _ => None,
        };
        if let Some(cascade) = cascade {
            loop {
                let alive = parts.tables.values().map(|t| t.id).chain(parts.sql_views.keys().copied()).collect::<BTreeSet<_>>();
                let dropped = parts.sql_views.iter().filter(|(_, v)| v.dependencies.iter().any(|d|
                    !alive.contains(&d.relation) || parts.tables.values().find(|t| t.id == d.relation)
                        .is_some_and(|t| d.columns.iter().any(|id| !t.columns.iter().any(|c| c.id == *id))))).map(|(id, _)| *id).collect::<BTreeSet<_>>();
                if dropped.is_empty() { break; }
                if !cascade { return Err(SqlError::DependentObjects); }
                parts.sql_views.retain(|id, _| !dropped.contains(id));
                parts.comments.retain(|(id, _), _| !dropped.contains(id));
            }
        }
        let Statement::View(ddl) = &self.statement else { return Ok(()); };
        self.validate_schemas(state)?;
        if let Some(query) = &ddl.query {
            let name = &ddl.names[0];
            let existing = state.view_names.get(name).map(|v| v.0);
            if state.relation_name_taken(name) && (!ddl.replace || existing.is_none()) {
                return Err(if ddl.replace { SqlError::WrongObjectType } else { SqlError::Duplicate });
            }
            let source = query.view_source()?;
            if existing.is_some() && state.view_names.get(source).is_some_and(|(_, projection)|
                projection.as_ref().is_ok_and(|p| p.reads.contains(name))) { return Err(SqlError::Unsupported); }
            let mut binding = state.clone();
            if let Some(id) = state.view_names.get(source).map(|(id, _)| id) {
                let view = &parts.sql_views[id];
                std::sync::Arc::make_mut(&mut binding.tables).insert(source.clone(), TableDef { id: *id, name: split_key(source).1.into(),
                    columns: view.columns.iter().map(|c| ColumnDef { id: c.id, name: c.name.clone(), data_type: c.storage_type.clone(), not_null: false }).collect(), primary_key: vec![] });
            }
            let mut outputs = query.view_projection(&binding)?;
            if ddl.columns.len() > outputs.len() { return Err(SqlError::Syntax); }
            for (column, name) in outputs.iter_mut().zip(&ddl.columns) { column.name = name.clone(); }
            let mut seen = BTreeSet::new();
            if outputs.iter().any(|c| !seen.insert(&c.name)) { return Err(SqlError::DuplicateColumn); }
            let table = binding.tables.get(source).ok_or(SqlError::UnknownTable)?;
            let referenced = query.view_dependencies()?;
            let dependencies = vec![SqlViewDependency { relation: table.id,
                columns: table.logical_columns().filter(|c| outputs.iter().any(|o| o.id == c.id) || referenced.contains(&c.name)).map(|c| c.id).collect() }];
            let mut select = format!("SELECT {} FROM {}.{}", outputs.iter().map(|c| {
                let original = table.logical_columns().find(|s| s.id == c.id).unwrap();
                format!("{} AS {}", quote(&original.name), quote(&c.name))
            }).collect::<Vec<_>>().join(", "), quote(split_key(source).0), quote(split_key(source).1));
            if let Some(predicate) = query.view_where_sql()? { select.push_str(&format!(" WHERE {predicate}")); }
            let id = match existing { Some(id) => id, None => allocate(&mut next)? };
            let previous = existing.and_then(|id| parts.sql_views.get(&id));
            let columns = outputs.iter().enumerate().map(|(position, c)| {
                let inherited = parts.sql_views.get(&table.id).and_then(|v| v.columns.iter().find(|s| s.id == c.id));
                Ok(SqlViewColumn { id: match previous.and_then(|v| v.columns.get(position)) { Some(c) => c.id, None => allocate(&mut next)? }, name: c.name.clone(), storage_type: c.data_type.clone(),
                    pg_type_oid: match inherited { Some(c) => c.pg_type_oid, None => state.ext.columns.get(&c.id).and_then(|c| c.type_id).map(type_oid).transpose()?.unwrap_or_else(|| oid(&c.data_type)) },
                    typmod: inherited.map_or_else(|| state.column_typmod(c.id), |c| c.typmod),
                    source: Some((table.id, c.id)) })
            }).collect::<R<Vec<_>>>()?;
            if let Some(previous) = previous {
                let message = if columns.len() < previous.columns.len() { Some("cannot drop columns from view".into()) } else {
                    previous.columns.iter().zip(&columns).find_map(|(old, new)| {
                        if old.name != new.name { Some(format!("cannot change name of view column \"{}\" to \"{}\"", old.name, new.name)) }
                        else if old.pg_type_oid != new.pg_type_oid || old.typmod != new.typmod || old.storage_type != new.storage_type {
                            Some(format!("cannot change data type of view column \"{}\"", old.name))
                        } else { None }
                    })
                };
                if let Some(message) = message { user_types::view_replace_error(message); return Err(SqlError::InvalidTableDefinition); }
            }
            let (schema, name) = split_key(name);
            let schema = if schema == "public" { 0 } else { state.ext.schemas.get(schema).ok_or(SqlError::InvalidSchema)?.id };
            parts.sql_views.insert(id, SqlViewDef { schema, name: name.into(), columns, select, dependencies });
        } else {
            let mut dropped = BTreeSet::new();
            for name in &ddl.names {
                if state.relation_name_taken(name) && !state.view_names.contains_key(name) { return Err(SqlError::WrongObjectType); }
                match state.view_names.get(name).map(|(id, _)| id) {
                    Some(id) => { dropped.insert(*id); }
                    None if ddl.optional => {
                        let (schema, object) = split_key(name);
                        if !schema.is_empty() && schema != "public" && !state.ext.schemas.contains_key(schema) {
                            user_types::missing_object_notice("schema", schema);
                        } else { user_types::view_missing_notice(object); }
                    },
                    None => {
                        user_types::view_replace_error(format!("view \"{}\" does not exist", split_key(name).1.trim_start_matches('\0')));
                        return Err(SqlError::UnknownTable);
                    },
                }
            }
            loop {
                let before = dropped.len();
                for (id, view) in &parts.sql_views {
                    if !dropped.contains(id) && view.dependencies.iter().any(|d| dropped.contains(&d.relation)) {
                        if !ddl.cascade { return Err(SqlError::DependentObjects); }
                        dropped.insert(*id);
                    }
                }
                if before == dropped.len() { break; }
            }
            parts.sql_views.retain(|id, _| !dropped.contains(id));
            parts.comments.retain(|(id, _), _| !dropped.contains(id));
        }
        Ok(())
    }
}
