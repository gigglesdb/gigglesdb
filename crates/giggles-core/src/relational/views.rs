//! Projection-view DDL; persisted definitions use the existing v2 DTO.
use super::*;
use crate::storage::{CatalogParts, SqlViewColumn, SqlViewDef, SqlViewDependency};

#[derive(Debug, Clone)]
pub(super) struct Ddl {
    pub names: Vec<String>,
    pub query: Option<query::Query>,
    columns: Vec<String>,
    optional: bool,
    cascade: bool,
}
pub(super) fn claims(tokens: &[Token]) -> bool {
    matches!(tokens.first(), Some(Token::Word(w)) if w == "create" || w == "drop")
        && matches!(tokens.get(1), Some(Token::Word(w)) if w == "view")
}
pub(super) fn parse(p: &mut Parser) -> R<Ddl> {
    let create = p.word("create");
    if !create { p.need("drop")?; }
    p.need("view")?;
    let optional = !create && p.word("if");
    if optional { p.need("exists")?; }
    let mut names = vec![if create { p.create_table_name()? } else { p.table_name()? }];
    let mut columns = Vec::new();
    let query = if create {
        if p.sym('(') {
            columns.push(p.name()?);
            while p.sym(',') { columns.push(p.name()?); }
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
    Ok(Ddl { names, query, columns, optional, cascade })
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
        DataType::TextArray => 1009,
    }
}
fn quote(s: &str) -> String { format!("\"{}\"", s.replace('"', "\"\"")) }
impl Prepared {
    pub(crate) fn check_view_drop(&self, state: &Relational) -> R<()> {
        if self.command() == "DROP TABLE" && self.drop_tables().iter().any(|n| state.view_names.contains_key(n)) {
            return Err(SqlError::WrongObjectType);
        }
        Ok(())
    }
    pub(crate) fn apply_view(&self, state: &Relational, parts: &mut CatalogParts, mut next: u64) -> R<()> {
        if let Statement::DropTable { cascade, .. } = &self.statement {
            loop {
                let alive = parts.tables.values().map(|t| t.id).chain(parts.sql_views.keys().copied()).collect::<BTreeSet<_>>();
                let dropped = parts.sql_views.iter().filter(|(_, v)| v.dependencies.iter().any(|d| !alive.contains(&d.relation))).map(|(id, _)| *id).collect::<BTreeSet<_>>();
                if dropped.is_empty() { break; }
                if !cascade { return Err(SqlError::DependentObjects); }
                parts.sql_views.retain(|id, _| !dropped.contains(id));
                parts.comments.retain(|(id, _), _| !dropped.contains(id));
            }
        }
        let Statement::View(ddl) = &self.statement else { return Ok(()); };
        if let Some(query) = &ddl.query {
            let name = &ddl.names[0];
            if state.relation_name_taken(name) { return Err(SqlError::Duplicate); }
            let source = query.read_tables();
            let [source] = source.as_slice() else { return Err(SqlError::Unsupported); };
            let mut binding = state.clone();
            if let Some(id) = state.view_names.get(source) {
                let view = &parts.sql_views[id];
                binding.tables.insert(source.clone(), TableDef { id: *id, name: split_key(source).1.into(),
                    columns: view.columns.iter().map(|c| ColumnDef { id: c.id, name: c.name.clone(), data_type: c.storage_type.clone(), not_null: false }).collect(), primary_key: vec![] });
            }
            let mut outputs = query.view_projection(&binding)?;
            if ddl.columns.len() > outputs.len() { return Err(SqlError::Syntax); }
            for (column, name) in outputs.iter_mut().zip(&ddl.columns) { column.name = name.clone(); }
            let mut seen = BTreeSet::new();
            if outputs.iter().any(|c| !seen.insert(&c.name)) { return Err(SqlError::DuplicateColumn); }
            let table = binding.tables.get(source).ok_or(SqlError::UnknownTable)?;
            let dependencies = vec![SqlViewDependency { relation: table.id,
                columns: table.logical_columns().filter(|c| outputs.iter().any(|o| o.id == c.id)).map(|c| c.id).collect() }];
            let select = format!("SELECT {} FROM {}.{}", outputs.iter().map(|c| {
                let original = table.logical_columns().find(|s| s.id == c.id).unwrap();
                format!("{} AS {}", quote(&original.name), quote(&c.name))
            }).collect::<Vec<_>>().join(", "), quote(split_key(source).0), quote(split_key(source).1));
            let id = allocate(&mut next)?;
            let columns = outputs.iter().map(|c| {
                let inherited = parts.sql_views.get(&table.id).and_then(|v| v.columns.iter().find(|s| s.id == c.id));
                Ok(SqlViewColumn { id: allocate(&mut next)?, name: c.name.clone(), storage_type: c.data_type.clone(),
                    pg_type_oid: match inherited { Some(c) => c.pg_type_oid, None => state.ext.columns.get(&c.id).and_then(|c| c.type_id).map(type_oid).transpose()?.unwrap_or_else(|| oid(&c.data_type)) },
                    typmod: inherited.map_or_else(|| state.column_typmod(c.id), |c| c.typmod) })
            }).collect::<R<Vec<_>>>()?;
            let (schema, name) = split_key(name);
            let schema = if schema == "public" { 0 } else { state.ext.schemas.get(schema).ok_or(SqlError::InvalidSchema)?.id };
            parts.sql_views.insert(id, SqlViewDef { schema, name: name.into(), columns, select, dependencies });
        } else {
            let mut dropped = BTreeSet::new();
            for name in &ddl.names {
                if state.relation_name_taken(name) && !state.view_names.contains_key(name) { return Err(SqlError::WrongObjectType); }
                match state.view_names.get(name) {
                    Some(id) => { dropped.insert(*id); }
                    None if ddl.optional => user_types::view_missing_notice(split_key(name).1.trim_start_matches('\0')),
                    None => return Err(SqlError::UnknownTable),
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
