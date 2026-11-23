//! Single-table predicates for CREATE INDEX and conflict target inference.
use super::*;
use crate::relational::partial_index::{MAX_PREDICATE_BYTES, Predicate};

pub(crate) struct BoundPredicate(BoundFilter);

impl BoundPredicate {
    pub(crate) fn matches(&self, values: &[Value]) -> R<bool> {
        Ok(self.0.eval(
            &Tuple {
                source_versions: vec![],
                values: vec![values.to_vec()],
                version: 0,
            },
            &[],
            &mut 0,
        )? == Some(true))
    }
}

fn failure(error: SqlError, message: String) -> SqlError {
    user_types::view_replace_error(message);
    error
}

thread_local! {
    static BOUNDED_PARSE: Cell<bool> = const { Cell::new(false) };
}

pub(super) fn bounded_parse() -> bool {
    BOUNDED_PARSE.get()
}

struct ParseScope(bool);

impl Drop for ParseScope {
    fn drop(&mut self) {
        BOUNDED_PARSE.set(self.0);
    }
}

fn parse(tokens: &[Token]) -> R<Filter> {
    // Persisted predicates must also fit the startup parser's token budget.
    let _budget = StatementBudget::enter(max_statement_tokens().min(4096), true);
    if tokens.len() > max_statement_tokens() {
        return Err(SqlError::Limit);
    }
    if tokens
        .iter()
        .any(|t| matches!(t, Token::Word(w) if w == "select"))
    {
        return Err(failure(
            SqlError::Unsupported,
            "cannot use subquery in index predicate".into(),
        ));
    }
    if tokens.windows(2).any(|t| {
        matches!(t, [Token::Word(n) | Token::QuotedIdent(n), Token::Symbol('(')]
        if Aggregate::from_name(n).is_some())
    }) {
        return Err(failure(
            SqlError::GroupingError,
            "aggregate functions are not allowed in index predicates".into(),
        ));
    }
    let mut p = Parser {
        privilege_seen: false,
        privilege_casts: false,
        tokens: tokens.to_vec(),
        at: 0,
        params: BTreeSet::new(),
        relations: vec![],
    };
    let _scope = ParseScope(BOUNDED_PARSE.replace(true));
    let filter = Filter::parse(&mut p, 0, 0)?;
    // CHECK rebinds its deparsed expression inside parentheses. Reserve that depth.
    filter.admit_shape(1, &mut 0)?;
    if p.at != tokens.len() {
        return Err(SqlError::Syntax);
    }
    if let Some(param) = p.params.first() {
        return Err(failure(
            SqlError::UndefinedParameter,
            format!("there is no parameter ${param}"),
        ));
    }
    Ok(filter)
}

fn bind_filter(
    state: &Relational,
    table: &TableDef,
    filter: Filter,
    require_immutable: bool,
) -> R<BoundPredicate> {
    let _enums = functions::enum_order::Scope::enter(state);
    let _collations = functions::collation::Scope::enter(state);
    let resolve = |column: &ColumnRef, _| {
        Ok(BoundColumn {
            source: 0,
            column: col(table, &column.name)?,
        })
    };
    let filter = match filter {
        Filter::Scalar(Scalar::Literal(Value::Text(text))) => {
            let value =
                functions::cast_value(Value::Text(text.clone()), &DataType::Boolean, &mut 0)
                    .map_err(|error| {
                        failure(
                            error,
                            format!("invalid input syntax for type boolean: \"{text}\""),
                        )
                    })?;
            Filter::Scalar(Scalar::Literal(value))
        }
        filter => filter,
    };
    let bound = filter.bind(&[], &resolve, &[table]).map_err(|error| {
        if error == SqlError::Type
            && let Filter::Scalar(scalar) = &filter
        {
            let ty = match scalar {
                Scalar::Column(c) => col(table, &c.name)
                    .ok()
                    .map(|i| table.columns[i].data_type.clone()),
                Scalar::Literal(Value::Int(_)) => Some(DataType::Integer),
                Scalar::Literal(Value::Numeric(_)) => Some(DataType::Numeric),
                _ => None,
            };
            if let Some(ty) = ty
                && ty != DataType::Boolean
            {
                return failure(
                    error,
                    format!(
                        "argument of WHERE must be type boolean, not type {}",
                        super::super::defaults::type_name(&ty)
                    ),
                );
            }
        }
        error
    })?;
    if require_immutable && !constraints::immutable(&bound) {
        return Err(failure(
            SqlError::InvalidIndexPredicate,
            "functions in index predicate must be marked IMMUTABLE".into(),
        ));
    }
    Ok(BoundPredicate(bound))
}

pub(in crate::relational) fn create(
    state: &Relational,
    table: &TableDef,
    tokens: &[Token],
    require_immutable: bool,
) -> R<Predicate> {
    let mut filter = parse(tokens)?;
    let mut names = BTreeMap::new();
    plan::subquery::map_filter(&mut filter, &mut |scalar| {
        if let Scalar::Column(c) = scalar {
            if let Some(q) = c.qualifier.take()
                && q != table.name
                && q != relation_key(state.table_schema(table.id), &table.name)
            {
                return Err(failure(
                    SqlError::UnknownTable,
                    format!("missing FROM-clause entry for table \"{q}\""),
                ));
            }
            let i = col(table, &c.name)
                .map_err(|e| failure(e, format!("column \"{}\" does not exist", c.name)))?;
            names.insert(c.name.clone(), table.columns[i].id);
        }
        Ok(())
    })?;
    bind_filter(state, table, filter, require_immutable)?;
    let sql = task_predicate::render(tokens)?;
    if sql.len() > MAX_PREDICATE_BYTES {
        return Err(SqlError::Limit);
    }
    let predicate = Predicate {
        sql,
        columns: names.into_values().collect(),
    };
    bind_filter(state, table, resolve(table, &predicate)?, require_immutable)?;
    Ok(predicate)
}

fn resolve(table: &TableDef, predicate: &Predicate) -> R<Filter> {
    if predicate.sql.is_empty() || predicate.sql.len() > MAX_PREDICATE_BYTES {
        return Err(SqlError::Type);
    }
    let mut filter = parse(&lex(&predicate.sql)?)?;
    let mut names = BTreeSet::new();
    filter.walk_scalars(&mut |scalar| {
        if let Scalar::Column(c) = scalar {
            names.insert(c.name.clone());
        }
        Ok(())
    })?;
    if names.len() != predicate.columns.len() {
        return Err(SqlError::Type);
    }
    let bindings = names
        .into_iter()
        .zip(&predicate.columns)
        .collect::<BTreeMap<_, _>>();
    plan::subquery::map_filter(&mut filter, &mut |scalar| {
        if let Scalar::Column(c) = scalar {
            let id = bindings[&c.name];
            let column = table
                .logical_columns()
                .find(|column| column.id == *id)
                .ok_or(SqlError::UnknownColumn)?;
            c.qualifier = None;
            c.name = column.name.clone();
        }
        Ok(())
    })?;
    Ok(filter)
}

pub(in crate::relational) fn bind(
    state: &Relational,
    table: &TableDef,
    predicate: &Predicate,
) -> R<BoundPredicate> {
    bind_filter(state, table, resolve(table, predicate)?, true)
}

fn conjuncts(filter: &Filter, out: &mut BTreeSet<String>) {
    if let Filter::Logic(a, true, b) = filter {
        conjuncts(a, out);
        conjuncts(b, out);
    } else {
        out.insert(format!("{filter:?}"));
    }
}

pub(in crate::relational) fn implies(
    actual: &Predicate,
    required: &Predicate,
    _state: &Relational,
    table: &TableDef,
) -> R<bool> {
    let mut actual_set = BTreeSet::new();
    conjuncts(&resolve(table, actual)?, &mut actual_set);
    let mut required_set = BTreeSet::new();
    conjuncts(&resolve(table, required)?, &mut required_set);
    Ok(required_set.is_subset(&actual_set))
}

fn cast(state: &Relational, table: &TableDef, against: &Scalar) -> String {
    if let Scalar::Column(c) = against
        && let Ok(i) = col(table, &c.name)
    {
        let column = &table.columns[i];
        if let Some(id) = state.ext.columns.get(&column.id).and_then(|c| c.type_id)
            && let Some(ty) = state.ext.types.get(&id)
        {
            return format!(
                "::{}",
                constraints::constraint_quote(split_key(ty.name()).1)
            );
        }
        return format!("::{}", super::super::defaults::type_name(&column.data_type));
    }
    "::text".into()
}

fn timestamp_literal(text: &str) -> R<String> {
    use giggles_pg_types::temporal::DateOrder;
    let read = || functions::coerce_clocked(Value::Text(text.into()), &DataType::Timestamp, None);
    let value = read()?;
    // Predicates store SQL text. A later read or a restart can bind that
    // text with a different DateStyle, so only admit a stable timestamp.
    for order in [DateOrder::Mdy, DateOrder::Dmy, DateOrder::Ymd] {
        let _order = clock::LiteralDateOrder::enter(order);
        if !read().is_ok_and(|candidate| candidate == value) {
            return Err(SqlError::Unsupported);
        }
    }
    Ok(format!(
        "'{value}'::timestamp without time zone",
        value = functions::output_text(&value)?.ok_or(SqlError::Type)?
    ))
}

fn operand(state: &Relational, table: &TableDef, value: &Scalar, against: &Scalar) -> R<String> {
    if let Scalar::Cast(input, DataType::Timestamp) = value
        && let Scalar::Literal(Value::Text(text)) = input.as_ref()
    {
        return timestamp_literal(text);
    }
    if let Scalar::Literal(Value::Text(text)) = value {
        let cast = cast(state, table, against);
        if cast == "::timestamp without time zone" {
            return timestamp_literal(text);
        }
        let cast = if cast == "::varchar" { "::text" } else { &cast };
        return Ok(format!("'{}'{cast}", text.replace('\'', "''")));
    }
    let rendered = constraints::scalar(value)?;
    if matches!(value, Scalar::Column(_)) && cast(state, table, value) == "::varchar" {
        Ok(format!("({rendered})::text"))
    } else {
        Ok(rendered)
    }
}

fn in_list<'a>(filter: &'a Filter, name: &str, values: &mut Vec<&'a Scalar>) -> bool {
    match filter {
        Filter::Logic(a, false, b) => in_list(a, name, values) && in_list(b, name, values),
        Filter::Compare(Scalar::Column(c), op, value @ Scalar::Literal(_))
            if c.name == name && op == "=" =>
        {
            values.push(value);
            true
        }
        _ => false,
    }
}

fn first_column(filter: &Filter) -> Option<&ColumnRef> {
    match filter {
        Filter::Logic(a, false, _) => first_column(a),
        Filter::Compare(Scalar::Column(c), _, _) => Some(c),
        _ => None,
    }
}

fn render(state: &Relational, table: &TableDef, filter: &Filter, has_in: bool) -> R<String> {
    if has_in
        && let Filter::Logic(_, false, _) = filter
        && let Some(c) = first_column(filter)
    {
        let mut values = vec![];
        if in_list(filter, &c.name, &mut values) {
            let column = Scalar::Column(c.clone());
            let values = values
                .into_iter()
                .map(|v| operand(state, table, v, &column))
                .collect::<R<Vec<_>>>()?
                .join(", ");
            return Ok(format!(
                "({} = ANY (ARRAY[{values}]))",
                operand(state, table, &column, &column)?
            ));
        }
    }
    Ok(match filter {
        Filter::Scalar(Scalar::Literal(Value::Null)) => "NULL::boolean".into(),
        Filter::Scalar(Scalar::Literal(Value::Text(text))) => {
            constraints::scalar(&Scalar::Literal(functions::cast_value(
                Value::Text(text.clone()),
                &DataType::Boolean,
                &mut 0,
            )?))?
        }
        Filter::Scalar(s) => constraints::scalar(s)?,
        Filter::Compare(a, op, b) => format!(
            "({} {} {})",
            operand(state, table, a, b)?,
            if op == "!=" { "<>" } else { op },
            operand(state, table, b, a)?
        ),
        Filter::Null(s, not) => format!(
            "({} IS {}NULL)",
            constraints::scalar(s)?,
            if *not { "NOT " } else { "" }
        ),
        Filter::Not(f) => format!("(NOT {})", render(state, table, f, has_in)?),
        Filter::Is(f, value, not) => format!(
            "({} IS {}{})",
            render(state, table, f, has_in)?,
            if *not { "NOT " } else { "" },
            value.map_or("UNKNOWN", |b| if b { "TRUE" } else { "FALSE" })
        ),
        Filter::Logic(a, and, b) => format!(
            "({} {} {})",
            render(state, table, a, has_in)?,
            if *and { "AND" } else { "OR" },
            render(state, table, b, has_in)?
        ),
        Filter::Like {
            value,
            pattern,
            escape,
            insensitive,
            negated,
        } => {
            let op = match (*insensitive, *negated) {
                (false, false) => "~~",
                (false, true) => "!~~",
                (true, false) => "~~*",
                (true, true) => "!~~*",
            };
            let mut rhs = operand(state, table, pattern, value)?;
            if let Some(escape) = escape {
                rhs = format!(
                    "like_escape({rhs}, {})",
                    operand(state, table, escape, value)?
                );
            }
            format!("({} {op} {rhs})", operand(state, table, value, pattern)?)
        }
        _ => return Err(SqlError::Unsupported),
    })
}

pub(in crate::relational) fn deparse(
    state: &Relational,
    table: &TableDef,
    predicate: &Predicate,
) -> R<String> {
    let has_in = lex(&predicate.sql)?
        .iter()
        .any(|t| matches!(t, Token::Word(w) if w == "in"));
    render(state, table, &resolve(table, predicate)?, has_in)
}
