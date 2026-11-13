//! Runtime projection bindings; scans keep the base table's physical layout.
use super::*;
use crate::storage::SqlViewDef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Projection {
    pub base: String,
    pub columns: Vec<(ColumnDef, usize)>,
    pub reads: Vec<String>,
}
impl Relational {
    pub(crate) fn catalog_views(&mut self, definitions: &BTreeMap<u64, SqlViewDef>) -> R<()> {
        for (id, view) in definitions {
            let schema = if view.schema == 0 { "public" } else {
                self.ext.schemas.iter().find(|(_, s)| s.id == view.schema).ok_or(SqlError::InvalidSchema)?.0
            };
            self.view_names.insert(relation_key(schema, &view.name), (*id, Err(SqlError::UnknownTable)));
        }
        let mut cache = BTreeMap::new();
        let projections = self.view_names.iter().map(|(name, (id, _))|
            (name.clone(), expand(self, definitions, *id, &mut BTreeSet::new(), &mut cache))).collect::<Vec<_>>();
        for (name, projection) in projections { self.view_names.get_mut(&name).unwrap().1 = projection; }
        Ok(())
    }
}
fn expand(state: &Relational, definitions: &BTreeMap<u64, SqlViewDef>, id: u64, visited: &mut BTreeSet<u64>, cache: &mut BTreeMap<(u64, usize), Projection>) -> R<Projection> {
    let key = (id, visited.len());
    if visited.len() >= 32 || visited.contains(&id) { return Err(SqlError::Unsupported); }
    if let Some(projection) = cache.get(&key) { return Ok(projection.clone()); }
    visited.insert(id);
    let view = definitions.get(&id).ok_or(SqlError::UnknownTable)?;
    let mut result = Projection { base: String::new(), columns: vec![], reads: vec![] };
    for output in &view.columns {
        let (relation, column) = output.source.ok_or(SqlError::Unsupported)?;
        let (base, offset, reads, source_id) = if let Some((name, table)) = state.tables.iter().find(|(_, t)| t.id == relation) {
            (name.clone(), table.columns.iter().position(|c| c.id == column).ok_or(SqlError::UnknownColumn)?, vec![name.clone()], column)
        } else {
            let inner = expand(state, definitions, relation, visited, cache)?;
            let index = definitions[&relation].columns.iter().position(|c| c.id == column).ok_or(SqlError::UnknownColumn)?;
            let (source, offset) = &inner.columns[index];
            (inner.base, *offset, inner.reads, source.id)
        };
        if !result.base.is_empty() && result.base != base { return Err(SqlError::Unsupported); }
        result.base = base;
        result.reads.extend(reads);
        result.columns.push((ColumnDef { id: source_id, name: output.name.clone(), data_type: output.storage_type.clone(), not_null: false }, offset));
    }
    result.reads.extend(state.view_names.iter().filter(|(_, v)| v.0 == id).map(|(name, _)| name.clone()));
    result.reads.sort(); result.reads.dedup();
    visited.remove(&id);
    cache.insert(key, result.clone());
    Ok(result)
}
pub(super) fn projection<'a>(state: &'a Relational, name: &str) -> R<Option<&'a Projection>> {
    if state.tables.contains_key(name) { return Ok(None); }
    state.view_names.get(name).map(|(_, p)| p.as_ref().map_err(|e| {
        if *e == SqlError::Unsupported { user_types::view_recreate_error(); }
        e.clone()
    })).transpose()
}
pub(super) fn table<'a>(state: &'a Relational, name: &str) -> R<&'a TableDef> {
    let name = projection(state, name)?.map_or(name, |p| p.base.as_str());
    state.tables.get(name).ok_or(SqlError::UnknownTable)
}
impl Query {
    pub(in crate::relational) fn view_source(&self) -> R<&String> {
        if self.sources.len() != 1 { return Err(SqlError::Unsupported); }
        Ok(&self.sources[0].name)
    }
    pub(super) fn bind_views(&mut self, state: &Relational) {
        let reads = self.sources.iter().filter_map(|s| projection(state, &s.name).ok().flatten())
            .flat_map(|p| p.reads.clone()).collect::<Vec<_>>();
        if reads.is_empty() { return; }
        let mut items = Vec::new();
        for (item, alias) in &self.items {
            if matches!(item, Item::Star | Item::QualifiedStar { .. }) && let Ok(sources) = self.star_sources(item) {
                for index in sources {
                    let source = &self.sources[index];
                    let columns: Vec<String> = match projection(state, &source.name) {
                        Ok(Some(p)) => p.columns.iter().map(|(c, _)| c.name.clone()).collect(),
                        _ => state.tables.get(&source.name).map(|t| t.logical_columns().map(|c| c.name.clone()).collect()).unwrap_or_default(),
                    };
                    for name in columns { items.push((Item::Scalar(Scalar::Column(ColumnRef { qualifier: Some(source.alias.clone()), name })), None)); }
                }
            } else { items.push((item.clone(), alias.clone())); }
        }
        self.items = items;
        if !matches!(self.filter, Some(Filter::Clauses(_))) {
            self.filter = Some(Filter::Clauses(Box::new(QueryClauses { filter: self.filter.take(), ..Default::default() })));
        }
        if let Some(Filter::Clauses(c)) = &mut self.filter { c.view_reads = reads; }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn old_missing_and_recursive_bindings_fail_closed() {
        let mut state = Relational::default();
        let p = Prepared::parse("CREATE TABLE t(a int)", 1, 0).unwrap();
        state.execute(&p, &[], None).unwrap();
        let t = &state.tables["t"];
        let mut view = SqlViewDef { schema: 0, name: "v".into(), select: String::new(), dependencies: vec![],
            columns: vec![crate::storage::SqlViewColumn { id: 1000, name: "x".into(), storage_type: DataType::Integer,
                pg_type_oid: 23, typmod: -1, source: Some((t.id, t.logical_columns().next().unwrap().id)) }] };
        let definitions = |v| BTreeMap::from([(100, v)]);
        let original_binding = view.columns[0].source.unwrap();
        state.catalog_views(&definitions(view.clone())).unwrap();
        assert_eq!(projection(&state,"v").unwrap().unwrap().columns[0].0.name,"x");
        view.columns[0].source = None;
        state.catalog_views(&definitions(view.clone())).unwrap();
        assert_eq!(projection(&state,"v").unwrap_err(),SqlError::Unsupported);
        assert!(user_types::take_sql_error_message().unwrap().contains("recreate"));
        view.columns[0].source = Some(original_binding);
        state.tables.get_mut("t").unwrap().columns.iter_mut().find(|c| c.id == original_binding.1).unwrap().id = 2000;
        state.catalog_views(&definitions(view.clone())).unwrap();
        assert_eq!(projection(&state,"v").unwrap_err(),SqlError::UnknownColumn);
        view.columns[0].source = Some((100,1000));
        state.catalog_views(&definitions(view.clone())).unwrap();
        assert_eq!(projection(&state,"v").unwrap_err(),SqlError::Unsupported);
        let mut nested = BTreeMap::new();
        for id in (100..134).rev() {
            let mut v = view.clone(); v.name = format!("v{id}");
            v.columns[0].source = Some(if id == 133 { (original_binding.0,2000) } else { (id+1,1000) });
            nested.insert(id,v);
        }
        state.view_names.clear(); state.catalog_views(&nested).unwrap();
        assert_eq!(projection(&state,"v100").unwrap_err(),SqlError::Unsupported);
        assert!(projection(&state,"v102").unwrap().is_some());
        user_types::take_sql_error_message();
    }
}
