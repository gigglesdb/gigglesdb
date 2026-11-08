//! FILTER syntax is query-owned, like DISTINCT. Private calls keep all inputs
//! visible to the ordinary scalar dependency walkers; none reach scalar binding.
use super::*;
const PREFIX: &str = "\0aggregate-filter:";
pub(super) const CALL: &str = "\0aggregate-filter:apply";
pub(super) const COUNT_STAR: &str = "\0aggregate-filter:count-star";
const PLACEHOLDER: &str = "\0aggregate-filter:placeholder";
fn node(name: &str, args: Vec<Scalar>) -> Scalar {
    Scalar::Call(format!("{PREFIX}{name}"), args)
}
// Bare string literals have UNKNOWN type until their Boolean context chooses
// a type. Explicit casts and operands of comparisons retain their own types.
fn coerce_boolean_literals(filter: &mut Filter) {
    match filter {
        Filter::Scalar(s) => {
            if matches!(s, Scalar::Literal(Value::Text(_))) {
                let literal = std::mem::replace(s, Scalar::Literal(Value::Null));
                *s = Scalar::Cast(Box::new(literal), DataType::Boolean);
            } else {
                coerce_scalar_conditions(s);
            }
        }
        Filter::Null(s, _) => coerce_scalar_conditions(s),
        Filter::Compare(a, _, b) => {
            coerce_scalar_conditions(a);
            coerce_scalar_conditions(b);
        }
        Filter::Like {
            value,
            pattern,
            escape,
            ..
        } => {
            coerce_scalar_conditions(value);
            coerce_scalar_conditions(pattern);
            if let Some(s) = escape {
                coerce_scalar_conditions(s);
            }
        }
        Filter::Not(f) | Filter::Is(f, _, _) => coerce_boolean_literals(f),
        Filter::Logic(a, _, b) => {
            coerce_boolean_literals(a);
            coerce_boolean_literals(b);
        }
        Filter::Clauses(_) | Filter::InSubquery { .. } => {
            unreachable!("FILTER cannot contain query clauses or subqueries")
        }
    }
}
fn coerce_scalar_conditions(s: &mut Scalar) {
    match s {
        Scalar::Predicate(f) => coerce_boolean_literals(f),
        Scalar::Case(c) => {
            for (f, s) in &mut c.arms {
                coerce_boolean_literals(f);
                coerce_scalar_conditions(s);
            }
            if let Some(s) = &mut c.otherwise {
                coerce_scalar_conditions(s);
            }
        }
        Scalar::Call(_, args) => {
            for s in args {
                coerce_scalar_conditions(s);
            }
        }
        Scalar::SubstringOrder(args) => {
            for s in args {
                coerce_scalar_conditions(s);
            }
        }
        Scalar::Cast(s, _) | Scalar::Unary(_, s) => coerce_scalar_conditions(s),
        Scalar::Binary(_, a, b) => {
            coerce_scalar_conditions(a);
            coerce_scalar_conditions(b);
        }
        Scalar::Literal(_) | Scalar::Param(_) | Scalar::Column(_) => {}
    }
}
fn encode(f: Filter) -> Scalar {
    match f {
        Filter::Scalar(s) => node("scalar", vec![s]),
        Filter::Compare(a, op, b) => node(&format!("compare:{op}"), vec![a, b]),
        Filter::Null(s, not) => node(if not { "not-null" } else { "null" }, vec![s]),
        Filter::Is(f, target, not) => node(
            "is",
            vec![
                encode(*f),
                Scalar::Literal(target.map_or(Value::Null, Value::Bool)),
                Scalar::Literal(Value::Bool(not)),
            ],
        ),
        Filter::Not(f) => node("not", vec![encode(*f)]),
        Filter::Logic(a, and, b) => {
            node(if and { "and" } else { "or" }, vec![encode(*a), encode(*b)])
        }
        Filter::Like {
            value,
            pattern,
            escape,
            insensitive,
            negated,
        } => {
            let mut args = vec![value, pattern];
            args.extend(escape);
            node(
                &format!("like:{}:{}", u8::from(insensitive), u8::from(negated)),
                args,
            )
        }
        Filter::Clauses(_) | Filter::InSubquery { .. } => {
            unreachable!("FILTER cannot contain query clauses or subqueries")
        }
    }
}
pub(super) fn decode(s: &Scalar) -> R<Filter> {
    let Scalar::Call(name, args) = s else {
        return Err(SqlError::Syntax);
    };
    let name = name.strip_prefix(PREFIX).ok_or(SqlError::Syntax)?;
    Ok(match (name, args.as_slice()) {
        ("scalar", [s]) => Filter::Scalar(s.clone()),
        ("null", [s]) => Filter::Null(s.clone(), false),
        ("not-null", [s]) => Filter::Null(s.clone(), true),
        (
            "is",
            [
                s,
                Scalar::Literal(target),
                Scalar::Literal(Value::Bool(not)),
            ],
        ) => {
            let target = match target {
                Value::Bool(value) => Some(*value),
                Value::Null => None,
                _ => return Err(SqlError::Syntax),
            };
            Filter::Is(Box::new(decode(s)?), target, *not)
        }
        ("not", [s]) => Filter::Not(Box::new(decode(s)?)),
        ("and" | "or", [a, b]) => {
            Filter::Logic(Box::new(decode(a)?), name == "and", Box::new(decode(b)?))
        }
        (n, [a, b]) if n.starts_with("compare:") => {
            Filter::Compare(a.clone(), n[8..].into(), b.clone())
        }
        (n, args) if n.starts_with("like:") && (2..=3).contains(&args.len()) => Filter::Like {
            value: args[0].clone(),
            pattern: args[1].clone(),
            escape: args.get(2).cloned(),
            insensitive: n.as_bytes()[5] == b'1',
            negated: n.as_bytes()[7] == b'1',
        },
        _ => return Err(SqlError::Syntax),
    })
}
fn expand(s: &mut Scalar, replacements: &[Scalar], depth: usize, work: &mut usize) -> R<()> {
    if depth > 32 || *work >= MAX_WORK {
        return Err(SqlError::Limit);
    }
    *work += 1;
    match s {
        Scalar::Call(n, args)
            if n == PLACEHOLDER || n.strip_prefix("public.\0") == Some(PLACEHOLDER) =>
        {
            let [Scalar::Literal(Value::Int(index))] = args.as_slice() else {
                return Err(SqlError::Syntax);
            };
            *s = replacements
                .get(*index as usize)
                .ok_or(SqlError::Syntax)?
                .clone();
            expand(s, replacements, depth + 1, work)?;
        }
        Scalar::Call(_, args) => {
            for a in args {
                expand(a, replacements, depth + 1, work)?
            }
        }
        Scalar::SubstringOrder(args) => {
            for a in args {
                expand(a, replacements, depth + 1, work)?
            }
        }
        Scalar::Cast(a, _) | Scalar::Unary(_, a) => expand(a, replacements, depth + 1, work)?,
        Scalar::Binary(_, a, b) => {
            expand(a, replacements, depth + 1, work)?;
            expand(b, replacements, depth + 1, work)?;
        }
        Scalar::Predicate(f) => expand_filter_inner(f, replacements, depth + 1, work)?,
        Scalar::Case(c) => {
            for (f, s) in &mut c.arms {
                expand_filter_inner(f, replacements, depth + 1, work)?;
                expand(s, replacements, depth + 1, work)?;
            }
            if let Some(s) = &mut c.otherwise {
                expand(s, replacements, depth + 1, work)?;
            }
        }
        Scalar::Literal(_) | Scalar::Param(_) | Scalar::Column(_) => {}
    }
    Ok(())
}
pub(super) fn expand_scalar(s: &mut Scalar, replacements: &[Scalar]) -> R<()> {
    expand(s, replacements, 0, &mut 0)
}
pub(super) fn expand_filter(f: &mut Filter, replacements: &[Scalar]) -> R<()> {
    expand_filter_inner(f, replacements, 0, &mut 0)
}
fn expand_filter_inner(
    f: &mut Filter,
    replacements: &[Scalar],
    depth: usize,
    work: &mut usize,
) -> R<()> {
    if depth > 32 || *work >= MAX_WORK {
        return Err(SqlError::Limit);
    }
    *work += 1;
    match f {
        Filter::Scalar(s) | Filter::Null(s, _) | Filter::InSubquery { value: s, .. } => {
            expand(s, replacements, depth + 1, work)?
        }
        Filter::Compare(a, _, b) => {
            expand(a, replacements, depth + 1, work)?;
            expand(b, replacements, depth + 1, work)?;
        }
        Filter::Like {
            value,
            pattern,
            escape,
            ..
        } => {
            expand(value, replacements, depth + 1, work)?;
            expand(pattern, replacements, depth + 1, work)?;
            if let Some(e) = escape {
                expand(e, replacements, depth + 1, work)?
            }
        }
        Filter::Not(f) | Filter::Is(f, _, _) => {
            expand_filter_inner(f, replacements, depth + 1, work)?
        }
        Filter::Logic(a, _, b) => {
            expand_filter_inner(a, replacements, depth + 1, work)?;
            expand_filter_inner(b, replacements, depth + 1, work)?;
        }
        Filter::Clauses(_) => return Err(SqlError::Syntax),
    }
    Ok(())
}
fn innermost_postfix(tokens: &[Token], start: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut best = None;
    let mut skip_until = start;
    for at in start..tokens.len() {
        if at < skip_until {
            continue;
        }
        if is_subquery_open(tokens, at) {
            skip_until = subquery_end(tokens, at)
                .unwrap_or(tokens.len())
                .saturating_add(1);
            continue;
        }
        match tokens[at] {
            Token::Symbol('(') => depth += 1,
            Token::Symbol(')') => depth = depth.saturating_sub(1),
            _ => {}
        }
        if at > start
            && tokens[at - 1] == Token::Symbol(')')
            && matches!(&tokens[at], Token::Word(w) if w == "filter")
            && tokens.get(at + 1) == Some(&Token::Symbol('('))
            && best.is_none_or(|(best_depth, _)| depth >= best_depth)
        {
            best = Some((depth, at));
        }
    }
    best.map(|(_, at)| at)
}
pub(super) fn kind(s: &Scalar) -> Option<Aggregate> {
    match s {
        Scalar::Call(name, args) if name == CALL => match args.first() {
            Some(Scalar::Call(name, _)) if name == COUNT_STAR => Some(Aggregate::Count),
            Some(Scalar::Call(name, _)) => Aggregate::from_name(name),
            _ => None,
        },
        _ => None,
    }
}
/// Validate the query-owned construct without applying grouping rules to its
/// input columns. Both preflight and lowering use these same routine checks.
pub(super) fn parts(
    args: &[Scalar],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
) -> R<(Aggregate, Option<Scalar>, Filter)> {
    let [call, encoded_filter] = args else {
        return Err(SqlError::Syntax);
    };
    let filter = decode(encoded_filter)?;
    if filter_contains_aggregate(&filter) {
        return Err(SqlError::GroupingError);
    }
    let Scalar::Call(name, inputs) = call else {
        return Err(SqlError::WrongObjectType);
    };
    if name == COUNT_STAR {
        return Ok((Aggregate::Count, None, filter));
    }
    let Some(a) = Aggregate::from_name(name) else {
        if name.starts_with("schema.\0") {
            return Err(SqlError::InvalidSchema);
        }
        if scalar_has_undefined_routine(call) {
            return Err(SqlError::UndefinedFunction);
        }
        call.bind(resolve, tables, &[])?;
        return Err(SqlError::WrongObjectType);
    };
    if inputs.len() != 1 {
        return Err(SqlError::UndefinedFunction);
    }
    reject_nested_aggregate(&inputs[0])?;
    Ok((a, Some(inputs[0].clone()), filter))
}
// Transform argument expressions before FILTER. Nested aggregates contribute
// their bound result types so surrounding scalar signatures can be resolved,
// while nesting against the decorated outer aggregate remains a later check.
fn validate_argument_inputs(
    s: &Scalar,
    parameters: &[Option<DataType>],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
) -> R<()> {
    let mut scratch = parameters.to_vec();
    let mut transformed = s.clone();
    match replace_argument_aggregates(&mut transformed, &mut scratch, resolve, tables)
        .and_then(|()| transformed.infer(None, &mut scratch, resolve, tables))
        .and_then(|()| transformed.bind(resolve, tables, &scratch).map(|_| ()))
    {
        Err(SqlError::Parameters | SqlError::IndeterminateDatatype) => Ok(()),
        result => result,
    }
}
fn replace_argument_aggregates(
    s: &mut Scalar,
    parameters: &mut [Option<DataType>],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
) -> R<()> {
    if matches!(s, Scalar::Call(name, _) if name == CALL || Aggregate::from_name(name).is_some()) {
        let query = Query {
            explain: false,
            sources: vec![],
            joins: vec![],
            items: vec![(Item::Scalar(s.clone()), None)],
            predicates: vec![],
            filter: None,
            order: vec![],
            limit: None,
        };
        let mut isolated = parameters.to_vec();
        for (index, parameter) in isolated.iter_mut().enumerate() {
            if parameter.is_none() && !uses_parameter(s, index + 1) {
                // The isolated query must not diagnose unrelated parameters.
                *parameter = Some(DataType::Text);
            }
        }
        let bound = grouping::bind(&query, tables, isolated, resolve, Vec::new())?;
        let ty = bound
            .columns
            .first()
            .ok_or(SqlError::Syntax)?
            .data_type
            .clone();
        for (index, parameter) in parameters.iter_mut().enumerate() {
            if uses_parameter(s, index + 1) {
                *parameter = Some(bound.parameters[index].clone());
            }
        }
        *s = Scalar::Cast(Box::new(Scalar::Literal(Value::Null)), ty);
        return Ok(());
    }
    match s {
        Scalar::Call(_, args) => {
            for arg in args {
                replace_argument_aggregates(arg, parameters, resolve, tables)?;
            }
        }
        Scalar::SubstringOrder(args) => {
            for arg in args {
                replace_argument_aggregates(arg, parameters, resolve, tables)?;
            }
        }
        Scalar::Cast(arg, _) | Scalar::Unary(_, arg) => {
            replace_argument_aggregates(arg, parameters, resolve, tables)?
        }
        Scalar::Binary(_, a, b) => {
            replace_argument_aggregates(a, parameters, resolve, tables)?;
            replace_argument_aggregates(b, parameters, resolve, tables)?;
        }
        Scalar::Predicate(f) => replace_filter_aggregates(f, parameters, resolve, tables)?,
        Scalar::Case(c) => {
            for (f, s) in &mut c.arms {
                replace_filter_aggregates(f, parameters, resolve, tables)?;
                replace_argument_aggregates(s, parameters, resolve, tables)?;
            }
            if let Some(s) = &mut c.otherwise {
                replace_argument_aggregates(s, parameters, resolve, tables)?;
            }
        }
        Scalar::Literal(_) | Scalar::Param(_) | Scalar::Column(_) => {}
    }
    Ok(())
}
fn replace_filter_aggregates(
    f: &mut Filter,
    parameters: &mut [Option<DataType>],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
) -> R<()> {
    match f {
        Filter::Scalar(s) | Filter::Null(s, _) | Filter::InSubquery { value: s, .. } => {
            replace_argument_aggregates(s, parameters, resolve, tables)?
        }
        Filter::Compare(a, _, b) => {
            replace_argument_aggregates(a, parameters, resolve, tables)?;
            replace_argument_aggregates(b, parameters, resolve, tables)?;
        }
        Filter::Like {
            value,
            pattern,
            escape,
            ..
        } => {
            replace_argument_aggregates(value, parameters, resolve, tables)?;
            replace_argument_aggregates(pattern, parameters, resolve, tables)?;
            if let Some(s) = escape {
                replace_argument_aggregates(s, parameters, resolve, tables)?;
            }
        }
        Filter::Not(f) | Filter::Is(f, _, _) => {
            replace_filter_aggregates(f, parameters, resolve, tables)?
        }
        Filter::Logic(a, _, b) => {
            replace_filter_aggregates(a, parameters, resolve, tables)?;
            replace_filter_aggregates(b, parameters, resolve, tables)?;
        }
        Filter::Clauses(_) => return Err(SqlError::Unsupported),
    }
    Ok(())
}
fn uses_parameter(s: &Scalar, parameter: usize) -> bool {
    match s {
        Scalar::Param(n) => *n == parameter,
        Scalar::Call(_, args) => args.iter().any(|s| uses_parameter(s, parameter)),
        Scalar::SubstringOrder(args) => args.iter().any(|s| uses_parameter(s, parameter)),
        Scalar::Cast(s, _) | Scalar::Unary(_, s) => uses_parameter(s, parameter),
        Scalar::Binary(_, a, b) => uses_parameter(a, parameter) || uses_parameter(b, parameter),
        Scalar::Predicate(f) => filter_uses_parameter(f, parameter),
        Scalar::Case(c) => {
            c.arms
                .iter()
                .any(|(f, s)| filter_uses_parameter(f, parameter) || uses_parameter(s, parameter))
                || c.otherwise
                    .as_ref()
                    .is_some_and(|s| uses_parameter(s, parameter))
        }
        Scalar::Literal(_) | Scalar::Column(_) => false,
    }
}
fn filter_uses_parameter(filter: &Filter, parameter: usize) -> bool {
    let mut found = false;
    let _ = filter.visit_scalars(&mut |s| {
        found |= uses_parameter(s, parameter);
        Ok(())
    });
    found
}
// This runs during authorized binding, after normalization has bounded the
// tree. Validate only literal Boolean conversions; no expressions execute.
fn validate_boolean_literal_casts(s: &Scalar) -> R<()> {
    match s {
        Scalar::Cast(arg, ty) => {
            if *ty == DataType::Boolean
                && let Scalar::Literal(Value::Text(value)) = arg.as_ref()
            {
                match functions::cast_value(Value::Text(value.clone()), &DataType::Boolean, &mut 0)
                {
                    Err(SqlError::Type) => return Err(SqlError::InvalidTextRepresentation),
                    result => {
                        result?;
                    }
                }
            }
            validate_boolean_literal_casts(arg)?;
        }
        Scalar::Call(_, args) => {
            for arg in args {
                validate_boolean_literal_casts(arg)?;
            }
        }
        Scalar::SubstringOrder(args) => {
            for arg in args {
                validate_boolean_literal_casts(arg)?;
            }
        }
        Scalar::Unary(_, arg) => validate_boolean_literal_casts(arg)?,
        Scalar::Binary(_, a, b) => {
            validate_boolean_literal_casts(a)?;
            validate_boolean_literal_casts(b)?;
        }
        Scalar::Predicate(f) => f.visit_scalars(&mut validate_boolean_literal_casts)?,
        Scalar::Case(c) => {
            for (f, s) in &c.arms {
                f.visit_scalars(&mut validate_boolean_literal_casts)?;
                validate_boolean_literal_casts(s)?;
            }
            if let Some(s) = &c.otherwise {
                validate_boolean_literal_casts(s)?;
            }
        }
        Scalar::Literal(_) | Scalar::Param(_) | Scalar::Column(_) => {}
    }
    Ok(())
}
/// Input-side errors belong before grouping checks on output expressions.
/// Scratch inference is discarded; the normal aggregate pass still owns shared
/// parameter inference and ambiguous COUNT parameter diagnostics.
pub(super) fn validate_scalar(
    s: &Scalar,
    parameters: &[Option<DataType>],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
) -> R<()> {
    match s {
        Scalar::Call(name, args) if name == CALL => {
            let [call, encoded_filter] = args.as_slice() else {
                return Err(SqlError::Syntax);
            };
            if let Scalar::Call(_, inputs) = call {
                for input in inputs {
                    validate_argument_inputs(input, parameters, resolve, tables)?;
                }
            }
            let condition = decode(encoded_filter)?;
            if filter_contains_aggregate(&condition) {
                return Err(SqlError::GroupingError);
            }
            condition.visit_scalars(&mut validate_boolean_literal_casts)?;
            // PostgreSQL transforms FILTER before resolving the decorated call.
            // Discard its hints here: aggregate inputs still own the shared
            // parameter inference order in the validation pass below.
            let mut condition_parameters = parameters.to_vec();
            condition.infer(&mut condition_parameters, resolve, tables)?;
            match condition.bind(&condition_parameters, resolve, tables) {
                Err(SqlError::Parameters | SqlError::IndeterminateDatatype) => {}
                result => {
                    result?;
                }
            }
            let (a, arg, filter) = parts(args, resolve, tables)?;
            let mut scratch = parameters.to_vec();
            if let Some(arg) = &arg {
                let expected = if matches!(a.base(), Aggregate::Min | Aggregate::Max)
                    && arg.aggregate_unknown(&scratch)
                {
                    Some(&DataType::Text)
                } else {
                    None
                };
                arg.infer(expected, &mut scratch, resolve, tables)?;
            }
            // Unknown MIN/MAX inputs resolve to text before FILTER is
            // transformed. A second unresolved or differently typed reference
            // to that same parameter is an ambiguous parameter, rather than a
            // Boolean or comparison type failure against the chosen text type.
            if matches!(a.base(), Aggregate::Min | Aggregate::Max)
                && let Some(Scalar::Param(n)) = &arg
                && parameters.get(n - 1).is_none_or(Option::is_none)
                && filter_uses_parameter(&filter, *n)
                && condition_parameters.get(n - 1) != scratch.get(n - 1)
            {
                return Err(SqlError::AmbiguousParameter);
            }
            filter.infer(&mut scratch, resolve, tables)?;
            match filter.bind(&scratch, resolve, tables) {
                // Other aggregate inputs can constrain these parameters later.
                Err(SqlError::Parameters | SqlError::IndeterminateDatatype) => {}
                result => {
                    result?;
                }
            }
        }
        Scalar::Call(_, args) => {
            for arg in args {
                validate_scalar(arg, parameters, resolve, tables)?;
            }
        }
        Scalar::SubstringOrder(args) => {
            for arg in args {
                validate_scalar(arg, parameters, resolve, tables)?;
            }
        }
        Scalar::Unary(_, arg) | Scalar::Cast(arg, _) => {
            validate_scalar(arg, parameters, resolve, tables)?
        }
        Scalar::Binary(_, a, b) => {
            validate_scalar(a, parameters, resolve, tables)?;
            validate_scalar(b, parameters, resolve, tables)?;
        }
        Scalar::Predicate(f) => validate_filter(f, parameters, resolve, tables)?,
        Scalar::Case(c) => {
            for (f, s) in &c.arms {
                validate_filter(f, parameters, resolve, tables)?;
                validate_scalar(s, parameters, resolve, tables)?;
            }
            if let Some(s) = &c.otherwise {
                validate_scalar(s, parameters, resolve, tables)?;
            }
        }
        Scalar::Literal(_) | Scalar::Param(_) | Scalar::Column(_) => {}
    }
    Ok(())
}
pub(super) fn validate_filter(
    filter: &Filter,
    parameters: &[Option<DataType>],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
) -> R<()> {
    match filter {
        Filter::Scalar(s) | Filter::Null(s, _) | Filter::InSubquery { value: s, .. } => {
            validate_scalar(s, parameters, resolve, tables)?
        }
        Filter::Compare(a, _, b) => {
            validate_scalar(a, parameters, resolve, tables)?;
            validate_scalar(b, parameters, resolve, tables)?;
        }
        Filter::Like {
            value,
            pattern,
            escape,
            ..
        } => {
            validate_scalar(value, parameters, resolve, tables)?;
            validate_scalar(pattern, parameters, resolve, tables)?;
            if let Some(s) = escape {
                validate_scalar(s, parameters, resolve, tables)?;
            }
        }
        Filter::Not(f) | Filter::Is(f, _, _) => validate_filter(f, parameters, resolve, tables)?,
        Filter::Logic(a, _, b) => {
            validate_filter(a, parameters, resolve, tables)?;
            validate_filter(b, parameters, resolve, tables)?;
        }
        Filter::Clauses(_) => return Err(SqlError::Unsupported),
    }
    Ok(())
}
/// Extract innermost postfix constructs first. Parsing their conditions uses the
/// existing bounded Boolean grammar, then substitutes ordinary private calls.
pub(super) fn normalize(p: &mut Parser) -> R<Vec<Scalar>> {
    let start = p.at;
    let mut replacements = vec![];
    while let Some(at) = innermost_postfix(&p.tokens, start) {
        let mut depth = 1;
        let mut open = at - 1;
        while open > 0 && depth > 0 {
            open -= 1;
            match p.tokens[open] {
                Token::Symbol(')') => depth += 1,
                Token::Symbol('(') => depth -= 1,
                _ => {}
            }
        }
        if depth != 0 || open == 0 {
            return Err(SqlError::Syntax);
        }
        let mut call_start = open - 1;
        if !matches!(p.tokens[call_start], Token::Word(_) | Token::QuotedIdent(_)) {
            return Err(SqlError::Syntax);
        }
        while call_start >= 2 && p.tokens[call_start - 1] == Token::Symbol('.') {
            call_start -= 2
        }
        // COUNT(*) has the same transition as COUNT(1), including empty input.
        let count_star = at == open + 3
            && p.tokens[open + 1] == Token::Symbol('*')
            && matches!(&p.tokens[open-1],Token::Word(n)|Token::QuotedIdent(n) if n=="count");
        if count_star {
            p.tokens[open + 1] = Token::Number("1".into());
        }
        p.at = call_start;
        let mut call = Scalar::parse_aggregate_argument(p, 0)?;
        if count_star && matches!(&call,Scalar::Call(n,_) if n=="count") {
            call = Scalar::Call(COUNT_STAR.into(), vec![]);
        }
        if p.at != at {
            return Err(SqlError::Syntax);
        }
        p.need("filter")?;
        p.punct('(')?;
        p.need("where")?;
        // Boolean grammar uses scalar parsing, whose ordinary COUNT(*) is
        // intentionally unsupported. Normalize only inside this predicate;
        // grouping then rejects the nested aggregate with SQLSTATE 42803.
        let condition_start = p.at;
        let mut condition_end = condition_start;
        let mut nesting = 0usize;
        while condition_end < p.tokens.len() {
            match p.tokens[condition_end] {
                Token::Symbol('(') => nesting += 1,
                Token::Symbol(')') if nesting == 0 => break,
                Token::Symbol(')') => nesting -= 1,
                _ => {}
            }
            condition_end += 1;
        }
        for n in condition_start..condition_end.saturating_sub(3) {
            if matches!(&p.tokens[n],Token::Word(w)|Token::QuotedIdent(w) if w=="count")
                && p.tokens[n + 1] == Token::Symbol('(')
                && p.tokens[n + 2] == Token::Symbol('*')
                && p.tokens[n + 3] == Token::Symbol(')')
            {
                p.tokens[n + 2] = Token::Number("1".into());
            }
        }
        let mut filter = Filter::parse(p, 0, 0)?;
        filter.admit_subquery_positions(false)?;
        coerce_boolean_literals(&mut filter);
        p.punct(')')?;
        let end = p.at;
        expand_scalar(&mut call, &replacements)?;
        expand_filter(&mut filter, &replacements)?;
        let mut encoded = Scalar::Call(CALL.into(), vec![call, encode(filter)]);
        // Admit expanded depth and nodes before storing or cloning the tree.
        expand_scalar(&mut encoded, &[])?;
        let index = replacements.len();
        if index >= MAX_COLUMNS {
            return Err(SqlError::Limit);
        }
        replacements.push(encoded);
        p.tokens.splice(
            call_start..end,
            [
                Token::Word(PLACEHOLDER.into()),
                Token::Symbol('('),
                Token::Number(index.to_string()),
                Token::Symbol(')'),
            ],
        );
    }
    p.at = start;
    Ok(replacements)
}
