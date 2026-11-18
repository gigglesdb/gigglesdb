//! PostgreSQL text presentation of the existing native EXPLAIN strategy.
use super::*;
use giggles_pg_catalog::ExplainQuery;

pub(super) fn description(query: &ExplainQuery) -> Description {
    Description {
        type_oids: if query.json { vec![114] } else { vec![] },
        typmods: vec![-1],
        parameters: vec![],
        columns: vec![giggles_core::relational::ColumnDef {
            id: 0,
            name: "QUERY PLAN".into(),
            data_type: DataType::Text,
            not_null: true,
        }],
    }
}

pub(super) fn prepare(sql: &str, session: &mut Session) -> R<Option<ExplainQuery>> {
    // Preserve authority-before-classification for non-SQL sessions.
    if !session.role.is_sql() {
        return Ok(None);
    }
    let Some(inner) = giggles_pg_catalog::explain_statement(sql).map_err(catalog_wire::error)?
    else {
        return Ok(None);
    };
    let p = core(session.prepare(&inner))?;
    core(session.describe(&p))?;
    if p.parameter_count() != 0 {
        return Err("0A000");
    }
    let query = giggles_pg_catalog::explain_query(sql)
        .map_err(catalog_wire::error)?
        .ok_or("0A000")?;
    // Native EXPLAIN does not expose the cursor's primary-key-order shortcut.
    // Do not infer a Top-N sort for an ordered limit from that incomplete fact.
    if query.limit && !query.sort.is_empty() {
        return Err("0A000");
    }
    if !query.sort.is_empty() {
        let (_, tables, _) = core(session.catalog_snapshot())?;
        if tables
            .iter()
            .find(|t| Some(t.name.as_str()) == p.table())
            .is_some_and(|t| {
                t.primary_key.iter().any(|i| {
                    query.sort[0].split_whitespace().next()
                        == Some(
                            giggles_pg_catalog::quote_explain_identifier(&t.columns[*i].name)
                                .as_str(),
                        )
                })
            })
        {
            return Err("0A000");
        }
    }
    Ok(Some(query))
}

pub(super) fn execute(query: &ExplainQuery, session: &mut Session) -> R<SqlResult> {
    // Recheck authority and bind against the current schema on every execution.
    let p = core(session.prepare(&query.sql))?;
    core(session.describe(&p))?;
    if query.verbose && (query.table.is_some() || query.output.is_empty()) { return Err("0A000"); }
    let mut lines = if query.modification == Some("Insert") {
        vec!["Result".into()]
    } else if let Some(table) = &query.table {
        let sql = format!("EXPLAIN {}", query.read_sql);
        let (table_key, facts) = if query.modification.is_some() {
            core(session.explain_mutation_facts(&p, &sql))?
        } else {
            let read = core(session.prepare(&sql))?;
            (read.table().ok_or("0A000")?.to_owned(), core(session.explain_facts(&read))?)
        };
        let Some(Value::Text(facts)) = facts else {
            return Err("0A000");
        };
        // Grouping uses an ordered BTreeMap, not PostgreSQL's HashAggregate.
        if facts.starts_with("BoundedGroupAggregate")
            || facts.contains("NestedLoop")
            || facts.contains("InSubquery")
            || facts.starts_with("EmptyInput")
        {
            return Err("0A000");
        }
        let point = facts.starts_with("PrimaryKeyLookup;");
        if !point && !facts.starts_with("PrimaryKeyRangeScan;") {
            return Err("0A000");
        }
        if !point {
            let (_, tables, indexes) = core(session.catalog_snapshot())?;
            let definition = tables.iter().find(|t| t.name == table_key).ok_or("42P01")?;
            // Native facts do not split range conditions from residual filters,
            // or expose secondary access paths. Avoid inventing a full scan.
            if definition
                .primary_key
                .iter()
                .any(|i| query.filter_columns.contains(&definition.columns[*i].name))
                || indexes.iter().any(|i| i.table == definition.id)
            {
                return Err("0A000");
            }
        }
        let mut lines = vec![if point {
            let (_, tables, _) = core(session.catalog_snapshot())?;
            let definition = tables.iter().find(|t| t.name == table_key).ok_or("42P01")?;
            let metadata = session
                .sql
                .catalog_metadata()
                .map_err(|e| crate::service::sqlstate(&e))?;
            let default_name = format!(
                "{}_pkey",
                table.rsplit('.').next().unwrap_or(table).trim_matches('"')
            );
            let name = metadata
                .presentation
                .primary_names
                .get(&definition.id)
                .unwrap_or(&default_name);
            format!(
                "Index Scan using {} on {table}",
                giggles_pg_catalog::quote_explain_identifier(name)
            )
        } else {
            format!("Seq Scan on {table}")
        }];
        if let Some(filter) = &query.filter {
            // A point lookup can also have a residual predicate. Until those are
            // separated by the native facts, support only a single key equality.
            if point && (filter.contains(" AND ") || filter.contains(" OR ")) {
                return Err("0A000");
            }
            lines.push(format!(
                "  {}: {filter}",
                if point { "Index Cond" } else { "Filter" }
            ));
        }
        if facts.contains("; GlobalAggregate") {
            lines = parent("Aggregate", &[], lines);
        }
        if facts.contains("; StableSort") || facts.contains("; TopNSort") {
            lines = parent(
                "Sort",
                &[format!("Sort Key: {}", query.sort.join(", "))],
                lines,
            );
        }
        lines
    } else {
        if query.filter.is_some() || !query.sort.is_empty() {
            return Err("0A000");
        }
        vec!["Result".into()]
    };
    if query.limit {
        lines = parent("Limit", &[], lines);
    }
    if let Some(command) = query.modification {
        lines = parent(
            &format!("{command} on {}", query.table.as_ref().ok_or("0A000")?),
            &[],
            lines,
        );
    }
    if query.json {
        // Restrict JSON to the Result shape until its remaining node attributes
        // can be supplied from execution facts rather than guessed.
        if lines != ["Result"] || query.analyze || query.verbose { return Err("0A000"); }
        lines = vec!["[\n  {\n    \"Plan\": {\n      \"Node Type\": \"Result\",\n      \"Parallel Aware\": false,\n      \"Async Capable\": false,\n      \"Disabled\": false\n    }\n  }\n]".into()];
    } else {
        if query.analyze {
            // These profiles have one access node (and optionally a mutation).
            // Complex operators need per-operator counters before admission.
            if query.limit || lines.iter().any(|l| l.contains("Aggregate") || l.trim_start().starts_with("Sort")) { return Err("0A000"); }
            let result = core(session.execute_prepared_unreleased(&p, &[]))?;
            let result = core(session.release_page(result))?.into_inner();
            let count = if query.modification.is_some() { result.affected as usize } else { result.rows.len() };
            for (i, line) in lines.iter_mut().enumerate() {
                if i == 0 || line.contains("->") {
                    let rows = if i == 0 && query.modification.is_some() { result.rows.len() } else { count };
                    line.push_str(&format!(" (actual rows={rows}.00 loops=1)"));
                }
            }
            if lines.iter().any(|l| l.contains("Index Scan")) {
                let indent = if query.modification.is_some() { "        " } else { "  " };
                lines.push(format!("{indent}Index Searches: 1"));
            }
        }
        if query.verbose {
            if query.table.is_some() || query.output.is_empty() { return Err("0A000"); }
            lines.push(format!("  Output: {}", query.output.join(", ")));
        }
    }
    Ok(SqlResult {
        columns: vec!["QUERY PLAN".into()],
        rows: lines
            .into_iter()
            .map(|line| giggles_core::relational::SqlRow {
                values: vec![Value::Text(line)],
                version: 0,
            })
            .collect(),
        affected: 0,
    })
}

fn parent(node: &str, detail: &[String], children: Vec<String>) -> Vec<String> {
    let mut lines = vec![node.into()];
    lines.extend(detail.iter().map(|d| format!("  {d}")));
    lines.extend(children.into_iter().enumerate().map(|(i, line)| {
        if i == 0 {
            format!("  ->  {line}")
        } else {
            format!("      {line}")
        }
    }));
    lines
}
