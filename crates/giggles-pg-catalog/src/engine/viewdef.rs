//! PostgreSQL view deparsing from the existing stored SELECT text.
use super::*;
fn ident(i: &a::Ident) -> String { catalog::quote_identifier(&i.value) }
fn logical_terms(e: &A, op: &Op, pretty: bool, precedence: u8, terms: &mut Vec<String>) -> R<()> {
    match e {
        A::Nested(e) => logical_terms(e, op, pretty, precedence, terms)?,
        A::BinaryOp { left, op: child_op, right } if child_op == op => {
            logical_terms(left, op, pretty, precedence, terms)?;
            logical_terms(right, op, pretty, precedence, terms)?;
        }
        _ => terms.push(expr(e, pretty, precedence)?),
    }
    Ok(())
}
pub(super) fn expr(e: &A, pretty: bool, parent: u8) -> R<String> {
    let (s, precedence, operator) = match e {
        A::Nested(e) => return expr(e, pretty, parent),
        A::Identifier(i) => (ident(i), 9, false),
        A::CompoundIdentifier(ids) => (ids.iter().map(ident).collect::<Vec<_>>().join("."), 9, false),
        A::Value(a::Value::SingleQuotedString(s)) => (format!("'{}'::text", s.replace('\'', "''")), 9, false),
        A::Value(v) => (v.to_string().to_ascii_lowercase(), 9, false),
        A::BinaryOp { left, op, right } => {
            let precedence = match op { Op::Or => 1, Op::And => 2, _ => 4 };
            if matches!(op, Op::And | Op::Or) {
                let mut terms = vec![];
                logical_terms(left, op, pretty, precedence, &mut terms)?;
                logical_terms(right, op, pretty, precedence, &mut terms)?;
                let value = terms.join(&format!(" {op} "));
                return Ok(if !pretty || precedence < parent { format!("({value})") } else { value });
            }
            let op = if *op == Op::NotEq { "<>".into() } else { op.to_string() };
            (format!("{} {op} {}", expr(left, pretty, precedence)?, expr(right, pretty, precedence + u8::from(precedence == 4))?), precedence, true)
        }
        A::UnaryOp { op: a::UnaryOperator::Not, expr: e } => (format!("NOT {}", expr(e, pretty, 3)?), 3, true),
        A::IsNull(e) => (format!("{} IS NULL", expr(e, pretty, 4)?), 4, true),
        A::IsNotNull(e) => (format!("{} IS NOT NULL", expr(e, pretty, 4)?), 4, true),
        _ => return Err(Error::Unsupported),
    };
    Ok(if operator && (!pretty || precedence < parent) { format!("({s})") } else { s })
}
pub(super) fn deparse(sql: &str, snapshot: &CatalogSnapshot, pretty: bool, wrap: Option<i64>) -> R<String> {
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|_| Error::InvalidSnapshot)?;
    let [a::Statement::Query(query)] = statements.as_slice() else { return Err(Error::InvalidSnapshot); };
    let a::SetExpr::Select(select) = query.body.as_ref() else { return Err(Error::Unsupported); };
    let [from] = select.from.as_slice() else { return Err(Error::Unsupported); };
    let a::TableFactor::Table { name, .. } = &from.relation else { return Err(Error::Unsupported); };
    let parts = object(name);
    let (schema, relation) = match parts.as_slice() { [name] => ("public", name.as_str()), [schema,name] => (schema.as_str(), name.as_str()), _ => return Err(Error::Unsupported) };
    let visible = snapshot.search_path.iter().find(|schema| snapshot.tables.iter().any(|t| t.name == relation && snapshot.table_schema(t.id).0 == **schema));
    let relation = if visible.is_some_and(|s| s == schema) { catalog::quote_identifier(relation) }
        else { format!("{}.{}", catalog::quote_identifier(schema), catalog::quote_identifier(relation)) };
    let mut targets = vec![];
    for item in &select.projection {
        targets.push(match item {
            a::SelectItem::UnnamedExpr(e) => expr(e, pretty, 0)?,
            a::SelectItem::ExprWithAlias { expr: e, alias } => {
                let same = match e { A::Identifier(i) => i.value == alias.value, A::CompoundIdentifier(ids) => ids.last().is_some_and(|i| i.value == alias.value), _ => false };
                let value = expr(e, pretty, 0)?;
                if same { value } else { format!("{value} AS {}", ident(alias)) }
            }
            _ => return Err(Error::Unsupported),
        });
    }
    let inline = format!(" SELECT {}", targets.join(", "));
    let mut sql = if wrap.is_some_and(|w| w > 0 && inline.len() <= w as usize) { inline }
        else { format!(" SELECT {}", targets.join(",\n    ")) };
    sql.push_str(&format!("\n   FROM {relation}"));
    if let Some(predicate) = &select.selection { sql.push_str(&format!("\n  WHERE {}", expr(predicate, pretty, 0)?)); }
    sql.push(';');
    Ok(sql)
}
