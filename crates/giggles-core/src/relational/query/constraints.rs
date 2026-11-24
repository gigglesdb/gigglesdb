//! CHECK binding, deterministic deparse and evaluation through the query scalar layer.
use super::*;
pub(super) mod enum_text;
mod enum_rename;
pub(in crate::relational) use enum_rename::rename_enum_check;

pub(in crate::relational) fn constraint_types_compatible(
    child: &DataType,
    parent: &DataType,
) -> bool {
    functions::comparable(child, parent) && functions::join_key_supported(child, parent)
}

pub(in crate::relational) fn constraint_key_supported(
    source: &DataType,
    target: &DataType,
) -> bool {
    functions::join_key_supported(source, target)
}

pub(in crate::relational) fn constraint_key(
    value: &Value,
    source: &DataType,
    target: &DataType,
) -> R<Option<Value>> {
    functions::join_key(value, source, target)
}

fn quote(name: &str) -> String {
    let safe = name
        .as_bytes()
        .first()
        .is_some_and(|b| b.is_ascii_lowercase() || *b == b'_')
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && !functions::keyword_requires_quotes(name);
    if safe {
        name.to_owned()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}
fn without_case(s: &Scalar) -> R<()> {
    match s {
        Scalar::Case(_) => return Err(SqlError::Unsupported),
        Scalar::Predicate(f) => f.visit_scalars(&mut without_case)?,
        Scalar::Cast(s, _) | Scalar::Domain(s, ..) | Scalar::Unary(_, s) => without_case(s)?,
        Scalar::Binary(_, a, b) => {
            without_case(a)?;
            without_case(b)?;
        }
        Scalar::Call(_, args) => {
            for arg in args {
                without_case(arg)?;
            }
        }
        Scalar::SubstringOrder(args) => {
            for arg in args {
                without_case(arg)?;
            }
        }
        _ => (),
    }
    Ok(())
}
pub(super) fn scalar(s: &Scalar) -> R<String> {
    render_scalar(s, false)
}

fn render_scalar(s: &Scalar, stored: bool) -> R<String> {
    let scalar = |s| render_scalar(s, stored);
    let deparse = |f| render_filter(f, stored);
    Ok(match s {
        Scalar::Predicate(f) => deparse(f)?,
        Scalar::Case(case) => {
            // PG indents nested CASE differently; keep this formatter at one level.
            let otherwise = case.otherwise.as_ref().ok_or(SqlError::Unsupported)?;
            without_case(otherwise)?;
            let mut result = "\nCASE\n".to_owned();
            for (condition, value) in &case.arms {
                condition.visit_scalars(&mut without_case)?;
                without_case(value)?;
                result.push_str(&format!(
                    "    WHEN {} THEN {}\n",
                    deparse(condition)?,
                    scalar(value)?
                ));
            }
            result.push_str(&format!("    ELSE {}\nEND", scalar(otherwise)?));
            result
        }
        Scalar::Column(c) if c.qualifier.is_none() => quote(&c.name),
        Scalar::Literal(Value::Int(n)) if i32::try_from(*n).is_ok() => {
            if *n < 0 {
                format!("'{n}'::integer")
            } else {
                n.to_string()
            }
        }
        Scalar::Literal(Value::Bool(b)) => b.to_string(),
        Scalar::Literal(Value::Null) => return Err(SqlError::Unsupported),
        Scalar::Literal(Value::Text(s)) => format!("'{}'::text", s.replace('\'', "''")),
        Scalar::Unary(op, a) => match (op, a.as_ref()) {
            (functions::UnaryOp::Neg, Scalar::Literal(Value::Int(n)))
                if (1..=i32::MAX as i64).contains(n) =>
            {
                format!("'-{n}'::integer")
            }
            (functions::UnaryOp::Neg, Scalar::Literal(_)) => return Err(SqlError::Unsupported),
            (functions::UnaryOp::Neg | functions::UnaryOp::Plus, Scalar::Column(_) | Scalar::Literal(Value::Int(_))) => format!(
                "({} {})",
                if matches!(op, functions::UnaryOp::Neg) {
                    "-"
                } else {
                    "+"
                },
                scalar(a)?
            ),
            _ => return Err(SqlError::Unsupported),
        },
        Scalar::Binary(op, a, b) => {
            let op = match op {
                functions::BinaryOp::Add => "+",
                functions::BinaryOp::Sub => "-",
                functions::BinaryOp::Mul => "*",
                functions::BinaryOp::Div => "/",
                functions::BinaryOp::Mod => "%",
                functions::BinaryOp::Concat => "||",
                _ => return Err(SqlError::Unsupported),
            };
            format!("({} {op} {})", scalar(a)?, scalar(b)?)
        }
        Scalar::Call(name, args) if name == enum_text::MEMBERSHIP => enum_text::deparse_membership(args)?,
        _ if enum_text::varchar_cast(s).is_some() => {
            let (column, length) = enum_text::varchar_cast(s).ok_or(SqlError::InternalError)?;
            let column = quote(&column.name);
            match length {
                Some(length) => format!("({column})::character varying({length})"),
                None => format!("({column})::character varying"),
            }
        }
        Scalar::Call(name, args) if name == "nullif" => {
            let [left, right] = args.as_slice() else {
                return Err(SqlError::Unsupported);
            };
            format!("NULLIF({}, {})", scalar(left)?, scalar(right)?)
        }
        // These calls require PostgreSQL-specific deparsing before they can be stored.
        Scalar::Call(name, _) if matches!(name.as_str(), "width_bucket" | "normalize" | "is_normalized") => {
            return Err(SqlError::Unsupported);
        }
        Scalar::Call(name, args) if !name.contains('.') && !name.contains('\0') => format!(
            "{}({})",
            quote(name),
            args.iter().map(scalar).collect::<R<Vec<_>>>()?.join(", ")
        ),
        Scalar::Cast(a, ty) => {
            if stored {
                // The stored form already includes the casts emitted by this deparser.
                match (a.as_ref(), ty) {
                    (Scalar::Literal(Value::Null), DataType::Boolean) => return Ok("NULL::boolean".into()),
                    (Scalar::Literal(Value::Text(text)), DataType::Text | DataType::Integer) => {
                        let ty = if *ty == DataType::Text { "text" } else { "integer" };
                        return Ok(format!("'{}'::{ty}", text.replace('\'', "''")));
                    }
                    _ => {}
                }
            }
            // Literal casts can fold or change constant type during PG analysis.
            // Keep the admitted deparse subset independent of that machinery.
            if !matches!(a.as_ref(), Scalar::Column(_))
                && !(*ty == DataType::Text && enum_text::varchar_cast(a).is_some())
            {
                return Err(SqlError::Unsupported);
            }
            let ty = match ty {
                DataType::SmallInt => "smallint",
                DataType::Integer => "integer",
                DataType::BigInt => "bigint",
                DataType::Text => "text",
                DataType::Varchar => "varchar",
                DataType::Bpchar => "bpchar",
                DataType::Boolean => "boolean",
                DataType::Numeric => "numeric",
                DataType::Real => "real",
                DataType::DoublePrecision => "double precision",
                _ => return Err(SqlError::Unsupported),
            };
            format!("({})::{ty}", scalar(a)?)
        }
        _ => return Err(SqlError::Unsupported),
    })
}
fn comparison_operand(value: &Scalar, enum_text_comparison: bool, stored: bool) -> R<String> {
    if enum_text_comparison {
        if let Some(text) = enum_text::text_literal(value) {
            return scalar(&Scalar::Literal(Value::Text(text.into())));
        }
        if enum_text::varchar_cast(value).is_some() {
            return Ok(format!("({})::text", scalar(value)?));
        }
    }
    render_scalar(value, stored)
}

fn deparse(f: &Filter) -> R<String> {
    render_filter(f, false)
}

fn render_filter(f: &Filter, stored: bool) -> R<String> {
    let scalar = |s| render_scalar(s, stored);
    let deparse = |f| render_filter(f, stored);
    Ok(match f {
        Filter::InSubquery { .. } => return Err(SqlError::Unsupported),
        Filter::Is(f, Some(target), negated) => {
            let inner = match f.as_ref() {
                Filter::Scalar(s) => scalar(s)?,
                _ => deparse(f)?,
            };
            format!(
                "({inner} IS {}{})",
                if *negated { "NOT " } else { "" },
                if *target { "TRUE" } else { "FALSE" }
            )
        }
        Filter::Is(_, None, _) => return Err(SqlError::Unsupported),
        Filter::Scalar(Scalar::Literal(Value::Null)) => "NULL::boolean".into(),
        Filter::Scalar(s) => scalar(s)?,
        Filter::Compare(a, op, b) => {
            let enum_text_comparison = [a, b].into_iter().any(|value| {
                enum_text::text_operand(value)
                    || enum_text::varchar_cast(value).is_some()
            });
            format!(
                "({} {} {})",
                comparison_operand(a, enum_text_comparison, stored)?,
                if op == "!=" { "<>" } else { op },
                comparison_operand(b, enum_text_comparison, stored)?,
            )
        }
        Filter::Null(s, not) => {
            format!("({} IS {}NULL)", scalar(s)?, if *not { "NOT " } else { "" })
        }
        Filter::Not(f) => format!("(NOT {})", deparse(f)?),
        Filter::Logic(a, and, b) => {
            let left = deparse(a)?;
            let left = if matches!(a.as_ref(),Filter::Logic(_,left_and,_) if left_and == and) {
                left.strip_prefix('(')
                    .and_then(|s| s.strip_suffix(')'))
                    .ok_or(SqlError::Unsupported)?
            } else {
                &left
            };
            format!(
                "({left} {} {})",
                if *and { "AND" } else { "OR" },
                deparse(b)?
            )
        }
        _ => return Err(SqlError::Unsupported),
    })
}
fn expression(tokens: Vec<Token>) -> R<Filter> {
    parse_expression(tokens).map(|(filter, _)| filter)
}

fn parse_expression(tokens: Vec<Token>) -> R<(Filter, Vec<schemas::Reference>)> {
    if tokens.iter().any(|token|matches!(token,Token::Word(word) if word == "select" || word == "distinct"))
        || tokens.iter().enumerate().any(|(at, token)| {
            matches!(token, Token::Word(word) if word == "case")
                && !matches!(tokens.get(at + 1), Some(Token::Word(word)) if word == "when")
        })
        || tokens.windows(2).any(|tokens|matches!((&tokens[0],&tokens[1]),(Token::Word(name) | Token::QuotedIdent(name),Token::Symbol('(')) if Aggregate::from_name(name).is_some()))
    {
        return Err(SqlError::Unsupported);
    }
    let mut p = Parser { privilege_seen: false, privilege_casts: false,
        tokens,
        at: 0,
        params: BTreeSet::new(),
        relations: Vec::new(),
    };
    let filter = Filter::parse(&mut p, 0, 0)?;
    if p.at != p.tokens.len() || !p.params.is_empty() {
        return Err(SqlError::Unsupported);
    }
    Ok((filter, p.relations))
}
pub(in crate::relational) fn validate_trigger_condition(tokens: &[Token]) -> R<()> {
    expression(tokens.to_vec()).map(drop)
}
pub(in crate::relational) fn constraint_deparse(tokens: &[Token]) -> R<String> {
    deparse(&expression(tokens.to_vec())?)
}
pub(in crate::relational) fn constraint_table_deparse(
    state: &Relational,
    table: &TableDef,
    tokens: &[Token],
) -> R<(String, BTreeSet<String>)> {
    let _enums = enum_text::Scope::enter(state, table, false);
    let schema = state.table_schema(table.id);
    let (mut filter, relations) = parse_expression(tokens.to_vec())?;
    if relations.iter().any(|reference| reference.schema.as_deref() != Some(schema)) {
        return Err(SqlError::UnknownTable);
    }
    let mut columns = BTreeSet::new();
    plan::subquery::map_filter(&mut filter, &mut |scalar| {
        if let Scalar::Column(column) = scalar {
            if column.qualifier.take().is_some_and(|name| name != table.name) {
                return Err(SqlError::UnknownTable);
            }
            columns.insert(column.name.clone());
        }
        Ok(())
    })?;
    Ok((deparse(&filter)?, columns))
}

pub(in crate::relational) fn check_enum_text_retype(
    state: &Relational,
    table: &TableDef,
    column: &ColumnDef,
) -> R<()> {
    let enum_column = match (state.column_type(column.id), &column.data_type) {
        (Some(TypeDef::Enum { .. }), DataType::Text) => true,
        (None, DataType::Text | DataType::Varchar) => false,
        _ => return Ok(()),
    };
    let _scope = enum_text::Scope::stored(state, table, false);
    for constraint in state.table_constraints(table.id) {
        let catalog_ext::ConstraintKind::Check { expr, .. } = &constraint.kind else {
            continue;
        };
        let mut found = false;
        let mut inspect_cast = |scalar: &Scalar| {
            found |= matches!(scalar, Scalar::Cast(inner, DataType::Text | DataType::Varchar)
                if matches!(inner.as_ref(), Scalar::Column(reference) if reference.name == column.name));
            Ok(())
        };
        stored_filter(expr)?.walk_scalars(&mut |scalar| {
            if enum_column {
                inspect_cast(scalar)?;
            } else if let Scalar::Call(name, args) = scalar
                && name == enum_text::MEMBERSHIP
                && let Some(operand) = args.first()
            {
                // The canonical membership expression survives a retype to text or varchar.
                walk_scalar(operand, &mut inspect_cast)?;
            }
            Ok(())
        })?;
        if found {
            user_types::view_replace_error(format!(
                "cannot change type of column \"{}\" because of enum-text check constraint \"{}\"",
                column.name, constraint.name,
            ));
            return Err(SqlError::Unsupported);
        }
    }
    Ok(())
}

pub(in crate::relational) fn constraint_references(
    state: &Relational,
    table: &TableDef,
    expr: &str,
    name: &str,
) -> R<bool> {
    let _scope = enum_text::Scope::stored(state, table, false);
    let mut referenced = false;
    stored_filter(expr)?.walk_scalars(&mut |scalar| {
        referenced |= matches!(scalar, Scalar::Column(column) if column.name == name);
        Ok(())
    })?;
    Ok(referenced)
}

pub(in crate::relational) fn constraint_rename(
    state: &Relational,
    table: &TableDef,
    expr: &str,
    old: &str,
    new: &str,
) -> R<Option<String>> {
    let mut scope = enum_text::Scope::stored(state, table, false);
    let mut filter = stored_filter(expr)?;
    let mut changed = false;
    plan::subquery::map_filter(&mut filter, &mut |scalar| {
        if let Scalar::Column(column) = scalar
            && column.name == old
        {
            column.name = new.into();
            changed = true;
        }
        Ok(())
    })?;
    if !changed {
        return Ok(None);
    }
    scope.rename_column(old, new);
    Ok(Some(render_stored_filter(expr, &filter)?))
}

fn render_stored_filter(expr: &str, filter: &Filter) -> R<String> {
    let result = render_filter(filter, true)?;
    let suffix = if expr.ends_with(" NOT VALID") { " NOT VALID" } else { "" };
    Ok(if expr.starts_with("CHECK (") {
        format!("CHECK ({result}){suffix}")
    } else {
        format!("{result}{suffix}")
    })
}

fn stored_filter(expr: &str) -> R<Filter> {
    let expr = expr.strip_suffix(" NOT VALID").unwrap_or(expr);
    let expr = expr
        .strip_prefix("CHECK (")
        .and_then(|e| e.strip_suffix(')'))
        .unwrap_or(expr);
    let mut tokens = lex(expr)?;
    for at in 0..tokens.len() {
        if matches!(tokens.get(at), Some(Token::QuotedIdent(name)) if name == "coalesce")
            && tokens.get(at + 1) == Some(&Token::Symbol('('))
            && at.checked_sub(1).and_then(|at| tokens.get(at)) != Some(&Token::Symbol('.'))
        {
            // The CHECK deparser quotes this keyword construct as a call name.
            // Restore only its stored spelling; original input uses expression().
            tokens[at] = Token::Word("coalesce".into());
        }
    }
    let mut p = Parser { privilege_seen: false, privilege_casts: false,
        tokens,
        at: 0,
        params: BTreeSet::new(),
        relations: Vec::new(),
    };
    let filter = Filter::parse(&mut p, 0, 0)?;
    if p.at != p.tokens.len() || !p.params.is_empty() {
        return Err(SqlError::Unsupported);
    }
    Ok(filter)
}

fn query(table: &TableDef, key: &str, expr: &str) -> R<Query> {
    Ok(table_query(table, key, stored_filter(expr)?))
}

pub(super) fn table_query(table: &TableDef, key: &str, filter: Filter) -> Query {
    Query {
        explain: false,
        sources: vec![Source {
            name: key.into(),
            alias: table.name.clone(),
        }],
        joins: vec![],
        items: vec![(Item::Star, None)],
        predicates: vec![],
        filter: Some(filter),
        order: vec![],
        limit: None,
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ExpressionContext {
    Check,
    Predicate,
    GeneratedColumn,
}

pub(super) fn immutable_for(filter: &BoundFilter, context: ExpressionContext) -> bool {
    let scalar = |value: &BoundScalar| value.immutable_for(context);
    match filter {
        BoundFilter::InSubquery { .. } | BoundFilter::Exists { .. } => false,
        BoundFilter::Scalar(s) | BoundFilter::Null(s, _) => scalar(s),
        BoundFilter::Compare(a, _, b) => scalar(a) && scalar(b),
        BoundFilter::Like {
            value,
            pattern,
            escape,
            ..
        } => {
            scalar(value)
                && scalar(pattern)
                && escape
                    .as_deref()
                    .is_none_or(scalar)
        }
        BoundFilter::Not(a) | BoundFilter::Is(a, _, _) => immutable_for(a, context),
        BoundFilter::Logic(a, _, b) => immutable_for(a, context) && immutable_for(b, context),
    }
}
fn cast_subset(filter: &Filter, table: &TableDef) -> R<()> {
    fn scalar_casts(value: &Scalar, table: &TableDef) -> R<()> {
        match value {
            Scalar::Predicate(filter) => cast_subset(filter, table)?,
            Scalar::Case(case) => {
                for (condition, result) in &case.arms {
                    cast_subset(condition, table)?;
                    scalar_casts(result, table)?;
                }
                if let Some(otherwise) = &case.otherwise {
                    scalar_casts(otherwise, table)?;
                }
            }
            Scalar::Cast(value, target) => {
                match value.as_ref() {
                    Scalar::Column(column) => {
                        if table.columns[col(table, &column.name)?].data_type == *target
                            && !(*target == DataType::Text && enum_text::is_character_column(column))
                        {
                            return Err(SqlError::Unsupported);
                        }
                    }
                    inner if *target == DataType::Text && enum_text::varchar_cast(inner).is_some() => (),
                    // Canonical PG spelling emitted for a boolean-context NULL.
                    Scalar::Literal(Value::Null) if *target == DataType::Boolean => (),
                    // Canonical PG spelling emitted for text literals.
                    Scalar::Literal(Value::Text(_))
                        if matches!(
                            target,
                            DataType::Text | DataType::Varchar | DataType::Bpchar
                        ) => {}
                    // Canonical PG spelling emitted for a folded negative integer.
                    Scalar::Literal(Value::Text(value))
                        if *target == DataType::Integer
                            && value.parse::<i32>().is_ok_and(|value| value < 0) => {}
                    _ => return Err(SqlError::Unsupported),
                }
            }
            Scalar::Unary(_, value) => scalar_casts(value, table)?,
            Scalar::Binary(_, left, right) => {
                scalar_casts(left, table)?;
                scalar_casts(right, table)?;
            }
            Scalar::Call(_, values) => {
                for value in values {
                    scalar_casts(value, table)?;
                }
            }
            _ => (),
        }
        Ok(())
    }
    match filter {
        Filter::InSubquery { .. } => return Err(SqlError::Unsupported),
        Filter::Is(value, _, _) => cast_subset(value, table)?,
        Filter::Scalar(value) | Filter::Null(value, _) => scalar_casts(value, table)?,
        Filter::Compare(left, _, right) => {
            scalar_casts(left, table)?;
            scalar_casts(right, table)?;
        }
        Filter::Not(value) => cast_subset(value, table)?,
        Filter::Logic(left, _, right) => {
            cast_subset(left, table)?;
            cast_subset(right, table)?;
        }
        _ => return Err(SqlError::Unsupported),
    }
    Ok(())
}
pub(in crate::relational) fn constraint_validate(
    state: &Relational,
    table: &TableDef,
    expr: &str,
) -> R<()> {
    let _enums = enum_text::Scope::enter(state, table, true);
    let q = query(
        table,
        &relation_key(state.table_schema(table.id), &table.name),
        expr,
    )?;
    cast_subset(q.filter.as_ref().ok_or(SqlError::Unsupported)?, table)?;
    let bound = q.bind(state, 0, &[])?;
    if !bound.filter.as_ref().is_some_and(|filter| immutable_for(filter, ExpressionContext::Check)) {
        return Err(SqlError::Unsupported);
    }
    Ok(())
}
pub(in crate::relational) fn constraint_eval(
    state: &Relational,
    table: &TableDef,
    expr: &str,
    row: &SqlRow,
) -> R<Option<bool>> {
    let _scope = enum_text::Scope::stored(state, table, true);
    let q = query(
        table,
        &relation_key(state.table_schema(table.id), &table.name),
        expr,
    )?;
    let bound = q.bind(state, 0, &[])?;
    bound.filter.as_ref().ok_or(SqlError::Unsupported)?.eval(
        &Tuple {
            source_versions: vec![],
            values: vec![row.values.clone()],
            version: row.version,
        },
        &[],
        &mut 0,
    )
}
pub(in crate::relational) fn constraint_check(
    state: &Relational,
    table: &TableDef,
    expr: &str,
    row: &SqlRow,
) -> R<()> {
    if constraint_eval(state, table, expr, row)? == Some(false) {
        Err(SqlError::CheckViolation)
    } else {
        Ok(())
    }
}
/// Identifier spelling shared by CHECK deparse and task predicate canonical text.
pub(in crate::relational) fn constraint_quote(name: &str) -> String {
    quote(name)
}
/// Strict task-predicate parse: the CHECK expression grammar (no SELECT, aggregates,
/// parameters or simple CASE) with unqualified column references only.
fn predicate_expression(tokens: &[Token]) -> R<Filter> {
    let filter = expression(tokens.to_vec())?;
    let mut qualified = false;
    filter.walk_scalars(&mut |s| {
        qualified |= matches!(s, Scalar::Column(c) if c.qualifier.is_some());
        Ok(())
    })?;
    if qualified {
        return Err(SqlError::Unsupported);
    }
    Ok(filter)
}
/// Parsed shape of a task predicate, used to prove that canonical text means the same.
pub(in crate::relational) fn constraint_shape(tokens: &[Token]) -> R<String> {
    Ok(format!("{:?}", predicate_expression(tokens)?))
}
/// Parses and binds a task predicate against `table` alone, once (GD-0061 R3-305):
/// the parsed filter and its bound form for repeated evaluation. Callers validate
/// first (`task_predicate::canonical`).
pub(in crate::relational) fn constraint_bind(
    state: &Relational,
    table: &TableDef,
    tokens: &[Token],
) -> R<(Filter, BoundFilter)> {
    let filter = predicate_expression(tokens)?;
    let q = table_query(
        table,
        &relation_key(state.table_schema(table.id), &table.name),
        filter.clone(),
    );
    let mut bound = q.bind(state, 0, &[])?;
    let bound = bound.filter.take().ok_or(SqlError::Unsupported)?;
    Ok((filter, bound))
}
/// Validates a task predicate against `table` alone (bound, deterministic) and lists
/// the table columns it reads.
pub(in crate::relational) fn constraint_columns(
    state: &Relational,
    table: &TableDef,
    tokens: &[Token],
) -> R<BTreeSet<String>> {
    let filter = predicate_expression(tokens)?;
    let mut read = BTreeSet::new();
    filter.walk_scalars(&mut |s| {
        if let Scalar::Column(c) = s {
            read.insert(c.name.clone());
        }
        Ok(())
    })?;
    let q = table_query(
        table,
        &relation_key(state.table_schema(table.id), &table.name),
        filter,
    );
    let bound = q.bind(state, 0, &[])?;
    if !bound.filter.as_ref().is_some_and(|filter| immutable_for(filter, ExpressionContext::Predicate)) {
        return Err(SqlError::Unsupported);
    }
    read.iter()
        .map(|name| Ok(table.columns[col(table, name)?].name.clone()))
        .collect()
}

#[cfg(test)]
mod pg186_deparse_tests {
    use super::*;

    #[test]
    fn frozen_pg186_check_definitions() {
        // Literal pg_get_constraintdef results from e1_constraints/deparse.txt.
        for (sql, expected) in [
            (
                "n IS NULL OR abs(n)>0",
                "CHECK (((n IS NULL) OR (abs(n) > 0)))",
            ),
            ("lower(s)<>'x'", "CHECK ((lower(s) <> 'x'::text))"),
            ("n BETWEEN 1 AND 4", "CHECK (((n >= 1) AND (n <= 4)))"),
            ("NOT (n=2)", "CHECK ((NOT (n = 2)))"),
            ("n::bigint>0", "CHECK (((n)::bigint > 0))"),
            ("length(s)>0", "CHECK ((length(s) > 0))"),
            ("z/0>0", "CHECK (((z / 0) > 0))"),
            ("n>-1", "CHECK ((n > '-1'::integer))"),
            ("n>-0", "CHECK ((n > 0))"),
            ("NULL", "CHECK (NULL::boolean)"),
            ("n>+1", "CHECK ((n > (+ 1)))"),
            ("-n>1", "CHECK (((- n) > 1))"),
            ("+n>1", "CHECK (((+ n) > 1))"),
            (
                "n>1 AND n<5 AND n<>3",
                "CHECK (((n > 1) AND (n < 5) AND (n <> 3)))",
            ),
            (
                "n>1 OR n<5 OR n<>3",
                "CHECK (((n > 1) OR (n < 5) OR (n <> 3)))",
            ),
            (
                "n>1 AND (n<5 AND n<>3)",
                "CHECK (((n > 1) AND ((n < 5) AND (n <> 3))))",
            ),
            (
                "(n>1 OR n<5) AND n<>3",
                "CHECK ((((n > 1) OR (n < 5)) AND (n <> 3)))",
            ),
            (
                "n>1 OR (n<5 AND n<>3)",
                "CHECK (((n > 1) OR ((n < 5) AND (n <> 3))))",
            ),
        ] {
            assert_eq!(
                format!(
                    "CHECK ({})",
                    constraint_deparse(&lex(sql).unwrap()).unwrap()
                ),
                expected,
                "{sql}"
            );
        }
    }

    #[test]
    fn noncanonical_expression_shapes_are_explicitly_bounded() {
        for sql in [
            "n>1::bigint",
            "s='x'::text",
            "s=1::text",
            "n='1'::int",
            "n>2147483648",
            "n=NULL",
            "length(NULL)>0",
            "n>-(1+2)",
            "-(-n)>1",
        ] {
            assert_eq!(
                constraint_deparse(&lex(sql).unwrap()),
                Err(SqlError::Unsupported),
                "{sql}"
            );
        }
    }

    #[test]
    fn pg186_boolean_expression_check_definitions() {
        for (sql, expected) in [
            (
                "CASE WHEN i > 0 THEN true ELSE false END",
                "CHECK (\nCASE\n    WHEN (i > 0) THEN true\n    ELSE false\nEND)",
            ),
            (
                "CASE WHEN b THEN i ELSE 0 END >= 0",
                "CHECK ((\nCASE\n    WHEN b THEN i\n    ELSE 0\nEND >= 0))",
            ),
            ("i > 0 IS TRUE", "CHECK (((i > 0) IS TRUE))"),
            ("(i > 0) IS NOT FALSE", "CHECK (((i > 0) IS NOT FALSE))"),
            (
                "coalesce(i > 0,true)",
                "CHECK (\"coalesce\"((i > 0), true))",
            ),
            (
                "CASE WHEN i > 0 THEN true WHEN b THEN false ELSE true END",
                "CHECK (\nCASE\n    WHEN (i > 0) THEN true\n    WHEN b THEN false\n    ELSE true\nEND)",
            ),
        ] {
            assert_eq!(
                format!(
                    "CHECK ({})",
                    constraint_deparse(&lex(sql).unwrap()).unwrap()
                ),
                expected,
                "{sql}"
            );
        }
    }

    #[test]
    fn canonical_check_coalesce_reparses_without_accepting_original_quoted_calls() {
        let table = TableDef {
            id: 0,
            name: "checks".into(),
            columns: vec![],
            primary_key: vec![],
        };
        let canonical = constraint_deparse(&lex("coalesce(i > 0, false)").unwrap()).unwrap();
        let stored = query(&table, &table.name, &canonical).unwrap();
        assert_eq!(deparse(stored.filter.as_ref().unwrap()).unwrap(), canonical);

        let original = expression(lex("\"coalesce\"(true, false)").unwrap()).unwrap();
        assert!(matches!(
            original.bind(&[], &|_, _| Err(SqlError::UnknownColumn), &[]),
            Err(SqlError::UndefinedFunction)
        ));
        assert_eq!(
            constraint_deparse(&lex("\"coalesce\"(true, false)").unwrap()),
            Err(SqlError::Unsupported)
        );
    }

    #[test]
    fn noncanonical_boolean_expression_shapes_are_explicitly_bounded() {
        for sql in [
            "CASE WHEN b THEN true END",
            "CASE WHEN CASE WHEN b THEN true ELSE false END THEN true ELSE false END",
            "CASE WHEN b THEN CASE WHEN b THEN true ELSE false END ELSE false END",
            "CASE WHEN b THEN true ELSE CASE WHEN b THEN true ELSE false END END",
            "CASE WHEN b THEN coalesce(CASE WHEN b THEN true ELSE false END, true) ELSE false END",
            "CASE i WHEN 1 THEN true ELSE false END",
            "i IS DISTINCT FROM 0",
            "i IS NOT DISTINCT FROM 0",
            "(i > 0) IS UNKNOWN",
            "(i > 0) IS NOT UNKNOWN",
            "(i > 0) IS NULL",
            "(i > 0) IS NOT NULL",
        ] {
            assert_eq!(
                constraint_deparse(&lex(sql).unwrap()),
                Err(SqlError::Unsupported),
                "{sql}"
            );
        }
    }
}
