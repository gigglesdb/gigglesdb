//! Lexical bindings use extra source slots, never statement parameter slots.
use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OuterSlot {
    pub(super) depth: usize,
    pub(super) source: usize,
    pub(super) column: usize,
    pub(super) data_type: DataType,
    /// Source index used by bound expressions; C4 supplies its tuple values.
    pub(super) slot: usize,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn resolve(
    state: &Relational,
    query: &Query,
    outer: &[&[Source]],
    tables: &[&TableDef],
    column: &ColumnRef,
    scope: usize,
    slots: &RefCell<Vec<OuterSlot>>,
) -> R<BoundColumn> {
    if let Some(Filter::Clauses(c)) = &query.filter
        && let Some(namespace) = &c.join_namespace
    {
        return namespace.resolve(column).map(|column| BoundColumn { source: 0, column });
    }
    let mut offset = 0;
    for (depth, sources) in std::iter::once(query.sources.as_slice())
        .chain(outer.iter().copied())
        .enumerate()
    {
        let visible = if depth == 0 {
            scope.min(sources.len())
        } else {
            sources.len()
        };
        let mut found = None;
        let mut qualified = false;
        for (source, syntax) in sources.iter().take(visible).enumerate() {
            if column.qualifier.as_ref().is_some_and(|q| q != &syntax.alias) {
                continue;
            }
            qualified |= column.qualifier.is_some();
            let table = tables[offset + source];
            let projection = views::projection(state, &syntax.name)?;
            let index = if let Some(p) = projection {
                p.columns.iter().find(|(c, _)| c.name == column.name).map(|(_, n)| *n)
            } else if column.qualifier.is_some()
                && let Some(index) = column.name.strip_prefix("\0star_column_").and_then(|n| n.parse::<usize>().ok())
            {
                (index < table.columns.len() && table.hidden_rowid() != Some(index)).then_some(index)
            } else {
                let mut matches = table.logical_offsets().into_iter()
                    .filter(|n| table.columns[*n].name == column.name);
                let index = matches.next();
                if matches.next().is_some() {
                    return Err(SqlError::AmbiguousColumn);
                }
                index
            };
            if let Some(index) = index {
                if found.is_some() {
                    return Err(if outer.is_empty() {
                        SqlError::Duplicate
                    } else {
                        SqlError::AmbiguousColumn
                    });
                }
                found = Some(BoundColumn {
                    source: offset + source,
                    column: index,
                });
            }
        }
        if found.is_none() && column.name == "ctid" {
            for (source, syntax) in sources.iter().take(visible).enumerate() {
                if column.qualifier.as_ref().is_some_and(|q| q != &syntax.alias) {
                    continue;
                }
                if views::projection(state, &syntax.name)?.is_some() { continue; }
                let column = tables[offset + source].hidden_rowid().ok_or(SqlError::Unsupported)?;
                if found.is_some() {
                    return Err(if outer.is_empty() {
                        SqlError::Duplicate
                    } else {
                        SqlError::AmbiguousColumn
                    });
                }
                found = Some(BoundColumn {
                    source: offset + source,
                    column,
                });
            }
        }
        if let Some(bound) = found {
            if depth > 0 {
                let slot = OuterSlot {
                    depth,
                    source: bound.source - offset,
                    column: bound.column,
                    data_type: tables[bound.source].columns[bound.column].data_type.clone(),
                    slot: bound.source,
                };
                let mut slots = slots.borrow_mut();
                if !slots.contains(&slot) {
                    slots.push(slot);
                }
            }
            return Ok(bound);
        }
        // An inner alias hides the entire outer alias, even absent columns.
        if qualified {
            return Err(SqlError::UnknownColumn);
        }
        offset += sources.len();
    }
    Err(if column.qualifier.is_some() {
        SqlError::UnknownTable
    } else {
        SqlError::UnknownColumn
    })
}
