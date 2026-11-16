//! Syntax and shared expression presentation only; the caller supplies plan facts.
use super::*;

#[derive(Clone)]
pub struct ExplainQuery {
    pub sql: String,
    pub read_sql: String,
    pub modification: Option<&'static str>,
    pub table: Option<String>,
    pub filter: Option<String>,
    pub filter_columns: Vec<String>,
    pub sort: Vec<String>,
    pub limit: bool,
}

/// Recognize only the COSTS OFF text profile. Other options retain their old route.
fn statement(sql: &str) -> R<Option<Box<a::Statement>>> {
    if !sql
        .trim_start()
        .get(..7)
        .is_some_and(|s| s.eq_ignore_ascii_case("explain"))
    {
        return Ok(None);
    }
    if sql.len() > 65_536 {
        return Err(Error::Limit);
    }
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|_| Error::Syntax)?;
    let [
        a::Statement::Explain {
            analyze: false,
            verbose: false,
            query_plan: false,
            statement,
            format: None,
            options: Some(options),
            ..
        },
    ] = statements.as_slice()
    else {
        return Ok(None);
    };
    if options.len() != 1 || !options[0].to_string().eq_ignore_ascii_case("COSTS OFF") {
        return Ok(None);
    }
    Ok(Some(statement.clone()))
}

/// Extract the statement for caller authorization before expression deparsing.
pub fn explain_statement(sql: &str) -> R<Option<String>> {
    Ok(statement(sql)?.map(|s| s.to_string()))
}

/// Deparse the supported shapes after the caller has authorized the statement.
pub fn explain_query(sql: &str) -> R<Option<ExplainQuery>> {
    let Some(statement) = statement(sql)? else {
        return Ok(None);
    };
    let mutation = match statement.as_ref() {
        a::Statement::Update { table, selection, from: None, .. } => Some(("Update", table, selection)),
        a::Statement::Delete(delete) if delete.using.is_none() => {
            let (a::FromTable::WithFromKeyword(tables) | a::FromTable::WithoutKeyword(tables)) = &delete.from;
            let [table] = tables.as_slice() else { return Err(Error::Unsupported); };
            Some(("Delete", table, &delete.selection))
        },
        a::Statement::Insert(insert) if insert.on.is_none() && insert.source.as_ref().is_some_and(|q|
            matches!(q.body.as_ref(), a::SetExpr::Values(values) if values.rows.len() == 1)) => {
            return Ok(Some(ExplainQuery { sql: statement.to_string(), read_sql: String::new(),
                modification: Some("Insert"), table: Some(insert.table_name.to_string()),
                filter: None, filter_columns: vec![], sort: vec![], limit: false }));
        },
        _ => None,
    };
    if let Some((command, from, selection)) = mutation {
        if !from.joins.is_empty()
            || !matches!(from.relation, a::TableFactor::Table { alias: None, .. })
        {
            return Err(Error::Unsupported);
        }
        // Searched mutations bind this same single-source target query and use
        // Bound::base_bounds for their scan, as SELECT's native EXPLAIN does.
        let predicate = selection
            .as_ref()
            .map(|e| format!(" WHERE {e}"))
            .unwrap_or_default();
        let mut shape = explain_query(&format!(
            "EXPLAIN (COSTS OFF) SELECT * FROM {from}{predicate}"
        ))?
        .ok_or(Error::Unsupported)?;
        shape.sql = statement.to_string();
        shape.modification = Some(command);
        return Ok(Some(shape));
    }
    let a::Statement::Query(query) = statement.as_ref() else {
        return Err(Error::Unsupported);
    };
    let a::SetExpr::Select(select) = query.body.as_ref() else {
        return Err(Error::Unsupported);
    };
    let table = match select.from.as_slice() {
        [] => None,
        [from] if from.joins.is_empty() => match &from.relation {
            a::TableFactor::Table {
                name,
                alias: None,
                args: None,
                ..
            } => Some(name.to_string()),
            _ => return Err(Error::Unsupported),
        },
        _ => return Err(Error::Unsupported),
    };
    fn constant(e: &A) -> bool {
        match e {
            A::Value(_) => true,
            A::Nested(e) | A::UnaryOp { expr: e, .. } => constant(e),
            A::BinaryOp { left, right, .. } => constant(left) && constant(right),
            _ => false,
        }
    }
    if table.is_none()
        && select.projection.iter().any(|item| match item {
            a::SelectItem::UnnamedExpr(e) | a::SelectItem::ExprWithAlias { expr: e, .. } => {
                !constant(e)
            }
            _ => true,
        })
    {
        return Err(Error::Unsupported);
    }
    let sort = query
        .order_by
        .as_ref()
        .map(|order| {
            order
                .exprs
                .iter()
                .map(|e| {
                    let mut key = viewdef::expr(&e.expr, false, 0)?;
                    if e.asc == Some(false) {
                        key.push_str(" DESC");
                    }
                    if let Some(first) = e.nulls_first {
                        key.push_str(if first { " NULLS FIRST" } else { " NULLS LAST" });
                    }
                    Ok(key)
                })
                .collect::<R<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();
    let mut filter_columns = vec![];
    if let Some(filter) = &select.selection {
        let _ = sqlparser::ast::visit_expressions(filter, |e| {
            match e {
                A::Identifier(i) => filter_columns.push(i.value.clone()),
                A::CompoundIdentifier(ids) => {
                    if let Some(i) = ids.last() {
                        filter_columns.push(i.value.clone());
                    }
                }
                _ => {}
            }
            ControlFlow::<()>::Continue(())
        });
    }
    Ok(Some(ExplainQuery {
        sql: statement.to_string(),
        read_sql: statement.to_string(),
        modification: None,
        table,
        filter: select
            .selection
            .as_ref()
            .map(|e| viewdef::expr(e, false, 0))
            .transpose()?,
        filter_columns,
        sort,
        limit: query.limit.is_some() || query.offset.is_some(),
    }))
}
