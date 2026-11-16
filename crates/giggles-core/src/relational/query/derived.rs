//! Bounded materialization preserves the inner SELECT's pagination boundary.
use super::*;
use std::borrow::Cow;

impl Query {
    pub(super) fn derived(&self) -> Option<&Query> { self.clauses()?.derived.as_deref() }
}
pub(super) fn parse(p: &mut Parser, items: &mut Vec<(Item, Option<String>)>) -> R<(Source, Box<Query>)> {
    p.sym('(');
    let child = Filter::parse_child(p)?;
    if child.sources.len() != 1 || !child.joins.is_empty() || child.derived().is_some()
        || child.has_in_subquery() || child.distinct() || !child.group().is_empty()
        || child.having().is_some() || child.explain
        || !child.items.iter().all(|(i, _)| matches!(i, Item::Scalar(Scalar::Column(_)) | Item::Star))
        || child.items.len() > 1 && child.items.iter().any(|(i, _)| matches!(i, Item::Star)) {
        return Err(SqlError::Unsupported);
    }
    p.word("as");
    let alias = p.name()?;
    let mut expanded = Vec::new();
    for (item, name) in items.drain(..) {
        match item {
            Item::Star | Item::QualifiedStar { schema: None, .. } => {
                if let Item::QualifiedStar { relation, .. } = &item
                    && relation != &alias { return Err(SqlError::UnknownTable); }
                for (item, name) in &child.items {
                    expanded.push(match item {
                        Item::Scalar(Scalar::Column(c)) => (Item::Scalar(Scalar::Column(ColumnRef {
                            qualifier: Some(alias.clone()), name: name.clone().unwrap_or_else(|| c.name.clone()),
                        })), None),
                        _ => (Item::Star, None),
                    });
                }
            }
            _ => expanded.push((item, name)),
        }
    }
    *items = expanded;
    Ok((Source { name: child.sources[0].name.clone(), alias }, child))
}
pub(super) fn column(query: &Query, c: &ColumnRef) -> R<Option<ColumnRef>> {
    let Some(child) = query.derived() else { return Ok(None); };
    if c.qualifier.as_ref().is_some_and(|q| q != &query.sources[0].alias) {
        return Err(SqlError::UnknownTable);
    }
    if matches!(child.items.as_slice(), [(Item::Star, _)]) { return Ok(None); }
    let matches = child.items.iter().filter_map(|(item, alias)| match item {
        Item::Scalar(Scalar::Column(inner)) if alias.as_ref().unwrap_or(&inner.name) == &c.name => Some(inner),
        _ => None,
    }).collect::<Vec<_>>();
    match matches.as_slice() {
        [inner] => Ok(Some(ColumnRef { qualifier: Some(query.sources[0].alias.clone()), name: inner.name.clone() })),
        [] => Err(SqlError::UnknownColumn),
        _ => Err(SqlError::AmbiguousColumn),
    }
}
/// Materialised derived rows; a v2 child's rows spill past work_mem (tuplestore)
/// and the outer query keeps v2 execution (`streams`).
pub(super) struct Rows<'a> { rows: spill::Store<SqlRow>, streams: bool, _scope: RetentionScope<'a> }
impl scan::RowSource for Rows<'_> {
    fn streams_reductions(&self) -> bool { self.streams }
    fn scan<'s>(&'s self, _: &TableDef, _: &scan::KeyRange, _: scan::Direction) -> R<scan::SourceRows<'s>> {
        Ok(Box::new(self.rows.iter()?.map(|row| Ok(scan::SourceRow {
            key: Cow::Borrowed(&[]), row: row?,
        }))))
    }
    fn get<'s>(&'s self, _: &TableDef, _: &[u8]) -> R<Option<scan::SourceRow<'s>>> { Err(SqlError::Unsupported) }
}
impl BoundQuery<'_> {
    pub(super) fn materialize_derived<'e>(&self, state: &Relational, source: &dyn scan::RowSource,
        params: &[Value], declared: &[Option<DataType>], execution: &'e Execution<'_>) -> R<Option<Rows<'e>>> {
        let Some(child) = self.query.derived() else { return Ok(None); };
        let bound = self.subqueries.get(child as *const Query as usize, SubplanMode::Scalar)?;
        let (result, captured) = execution.capture_subquery(
            || bound.execute_context(state, source, params, declared, execution, true, false))?;
        let table = self.tables[0];
        let offsets = bound.columns.iter().map(|c| table.columns.iter().position(|t| t.id == c.id)
            .ok_or(SqlError::Unsupported)).collect::<R<Vec<_>>>()?;
        let scope = RetentionScope::new(execution);
        let mut rows = spill::Store::new();
        for row in subquery_rows(&result, captured.as_ref())? {
            let row = row?;
            if execution.cancelled() { return Err(SqlError::Aborted); }
            let mut values = vec![Value::Null; table.columns.len()];
            for (value, offset) in row.values.iter().zip(&offsets) { values[*offset] = value.clone(); }
            let bytes = 64 + values.iter().map(value_bytes).sum::<usize>();
            if captured.is_none() { scope.borrow().charge(bytes)?; }
            rows.push(SqlRow { values, version: row.version }, bytes)?;
        }
        rows.finish()?;
        Ok(Some(Rows { rows, streams: captured.is_some(), _scope: scope }))
    }
}
