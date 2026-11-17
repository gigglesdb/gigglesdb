//! Runtime projection bindings; scans keep the base table's physical layout.
use super::*;
use crate::storage::SqlViewDef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Projection {
    pub base: String,
    pub columns: Vec<(ColumnDef, usize)>,
    pub reads: Vec<String>,
    filters: Vec<ViewPredicate>,
    pub dependencies: Vec<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct ViewPredicate {
    select: String,
    columns: Vec<(String, usize)>,
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
    let mut result = Projection { base: String::new(), columns: vec![], reads: vec![], filters: vec![], dependencies: vec![] };
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
    // Retain each layer's predicate with its original names mapped to physical offsets.
    if !view.select.is_empty() {
        let query = stored_query(&view.select)?;
        query.view_source()?;
        let relation = view.dependencies.first().ok_or(SqlError::UnknownTable)?.relation;
        let mapping = if definitions.contains_key(&relation) {
            let inner = expand(state, definitions, relation, visited, cache)?;
            result.filters = inner.filters;
            result.dependencies.extend(inner.dependencies);
            inner.columns.iter().map(|(c, offset)| (c.name.clone(), *offset)).collect()
        } else {
            let table = state.tables.values().find(|t| t.id == relation).ok_or(SqlError::UnknownTable)?;
            table.columns.iter().enumerate().map(|(i,c)| (c.name.clone(),i)).collect()
        };
        if query.where_filter().is_some() { result.filters.push(ViewPredicate { select: view.select.clone(), columns: mapping }); }
    }
    result.dependencies.extend(view.dependencies.iter().flat_map(|d| d.columns.iter().copied()));
    result.reads.extend(state.view_names.iter().filter(|(_, v)| v.0 == id).map(|(name, _)| name.clone()));
    result.reads.sort(); result.reads.dedup();
    visited.remove(&id);
    cache.insert(key, result.clone());
    Ok(result)
}
pub(in crate::relational) fn projection<'a>(state: &'a Relational, name: &str) -> R<Option<&'a Projection>> {
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


fn stored_query(sql: &str) -> R<Query> {
    let mut parser = Parser { tokens: lex(sql)?, at: 0, params: BTreeSet::new(), relations: vec![] };
    Query::parse(&mut parser)
}
pub(super) fn bind_filters(query: &Query, state: &Relational, tables: &[&TableDef], joins: &mut Vec<BoundJoin>, filter: &mut Option<BoundFilter>) -> R<()> {
    for (source, syntax) in query.sources.iter().enumerate() {
        let Some(projection) = projection(state, &syntax.name)? else { continue; };
        for binding in &projection.filters {
            let stored = stored_query(&binding.select)?;
            let Some(predicate) = stored.where_filter() else { continue; };
            let resolve = |c: &ColumnRef, _: usize| {
                let column = binding.columns.iter().find(|(name,_)| name == &c.name).ok_or(SqlError::UnknownColumn)?.1;
                Ok(BoundColumn { source, column })
            };
            let mut params = vec![];
            predicate.infer(&mut params, &resolve, tables)?;
            let bound = predicate.bind(&params, &resolve, tables)?;
            if source > 0 {
                // Filter right-side view rows before the join's NULL extension.
                let mut join = joins.remove(source - 1);
                join.on = BoundFilter::Logic(Box::new(join.on), true, Box::new(bound));
                joins.insert(source - 1, join);
            } else {
                *filter = Some(match filter.take() {
                    Some(previous) => BoundFilter::Logic(Box::new(previous), true, Box::new(bound)),
                    None => bound,
                });
            }
        }
    }
    Ok(())
}
impl Query {
    pub(in crate::relational) fn view_dependencies(&self) -> R<BTreeSet<String>> {
        let mut names = BTreeSet::new();
        self.walk_scalars(&mut |s| { if let Scalar::Column(c) = s { names.insert(c.name.clone()); } Ok(()) })?;
        Ok(names)
    }
    pub(in crate::relational) fn view_where_sql(&self) -> R<Option<String>> {
        self.where_filter().map(view_filter_sql).transpose()
    }
}
fn view_scalar_sql(s: &Scalar) -> R<String> {
    Ok(match s {
        Scalar::Column(c) => format!("\"{}\"", c.name.replace('"', "\"\"")),
        Scalar::Literal(Value::Text(s)) => format!("'{}'", s.replace('\'', "''")),
        Scalar::Literal(Value::Int(n)) => n.to_string(),
        Scalar::Literal(Value::Bool(b)) => b.to_string(),
        Scalar::Literal(Value::Null) => "NULL".into(),
        _ => return Err(SqlError::Unsupported),
    })
}
fn view_filter_sql(f: &Filter) -> R<String> {
    Ok(match f {
        Filter::Scalar(s) => view_scalar_sql(s)?,
        Filter::Compare(a, op, b) => format!("({} {op} {})", view_scalar_sql(a)?, view_scalar_sql(b)?),
        Filter::Null(a, negated) => format!("({} IS {}NULL)", view_scalar_sql(a)?, if *negated { "NOT " } else { "" }),
        Filter::Not(a) => format!("(NOT {})", view_filter_sql(a)?),
        Filter::Logic(a, and, b) => format!("({} {} {})", view_filter_sql(a)?, if *and { "AND" } else { "OR" }, view_filter_sql(b)?),
        _ => return Err(SqlError::Unsupported),
    })
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
