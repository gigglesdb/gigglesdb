//! CHECK binding, deterministic deparse and evaluation through the query scalar layer.
use super::*;

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
        Scalar::Cast(s, _) | Scalar::Unary(_, s) => without_case(s)?,
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
fn scalar(s: &Scalar) -> R<String> {
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
            (_, Scalar::Column(_) | Scalar::Literal(Value::Int(_))) => format!(
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
            };
            format!("({} {op} {})", scalar(a)?, scalar(b)?)
        }
        Scalar::Call(name, args) if !name.contains('.') && !name.contains('\0') => format!(
            "{}({})",
            quote(name),
            args.iter().map(scalar).collect::<R<Vec<_>>>()?.join(", ")
        ),
        Scalar::Cast(a, ty) => {
            // Literal casts can fold or change constant type during PG analysis.
            // Keep the admitted deparse subset independent of that machinery.
            if !matches!(a.as_ref(), Scalar::Column(_)) {
                return Err(SqlError::Unsupported);
            }
            let ty = match ty {
                DataType::SmallInt => "smallint",
                DataType::Integer => "integer",
                DataType::BigInt => "bigint",
                DataType::Text => "text",
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
fn deparse(f: &Filter) -> R<String> {
    Ok(match f {
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
        Filter::Compare(a, op, b) => format!(
            "({} {} {})",
            scalar(a)?,
            if op == "!=" { "<>" } else { op },
            scalar(b)?
        ),
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
    if tokens.iter().any(|token|matches!(token,Token::Word(word) if word == "select" || word == "distinct"))
        || tokens.iter().enumerate().any(|(at, token)| {
            matches!(token, Token::Word(word) if word == "case")
                && !matches!(tokens.get(at + 1), Some(Token::Word(word)) if word == "when")
        })
        || tokens.windows(2).any(|tokens|matches!((&tokens[0],&tokens[1]),(Token::Word(name) | Token::QuotedIdent(name),Token::Symbol('(')) if Aggregate::from_name(name).is_some()))
    {
        return Err(SqlError::Unsupported);
    }
    let mut p = Parser {
        tokens,
        at: 0,
        params: BTreeSet::new(),
    };
    let filter = Filter::parse(&mut p, 0, 0)?;
    if p.at != p.tokens.len() || !p.params.is_empty() {
        return Err(SqlError::Unsupported);
    }
    Ok(filter)
}
pub(in crate::relational) fn constraint_deparse(tokens: &[Token]) -> R<String> {
    deparse(&expression(tokens.to_vec())?)
}
fn query(table: &TableDef, expr: &str) -> R<Query> {
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
    let mut p = Parser {
        tokens,
        at: 0,
        params: BTreeSet::new(),
    };
    let filter = Filter::parse(&mut p, 0, 0)?;
    if p.at != p.tokens.len() || !p.params.is_empty() {
        return Err(SqlError::Unsupported);
    }
    Ok(Query {
        explain: false,
        sources: vec![Source {
            name: table.name.clone(),
            alias: table.name.clone(),
        }],
        joins: vec![],
        items: vec![(Item::Star, None)],
        predicates: vec![],
        filter: Some(filter),
        order: vec![],
        limit: None,
    })
}
pub(super) fn immutable(filter: &BoundFilter) -> bool {
    match filter {
        BoundFilter::Scalar(s) | BoundFilter::Null(s, _) => s.constraint_immutable(),
        BoundFilter::Compare(a, _, b) => a.constraint_immutable() && b.constraint_immutable(),
        BoundFilter::Like {
            value,
            pattern,
            escape,
            ..
        } => {
            value.constraint_immutable()
                && pattern.constraint_immutable()
                && escape
                    .as_ref()
                    .is_none_or(BoundScalar::constraint_immutable)
        }
        BoundFilter::Not(a) | BoundFilter::Is(a, _, _) => immutable(a),
        BoundFilter::Logic(a, _, b) => immutable(a) && immutable(b),
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
                        if table.columns[col(table, &column.name)?].data_type == *target {
                            return Err(SqlError::Unsupported);
                        }
                    }
                    // Canonical PG spelling emitted for a boolean-context NULL.
                    Scalar::Literal(Value::Null) if *target == DataType::Boolean => (),
                    // Canonical PG spelling emitted for text literals.
                    Scalar::Literal(Value::Text(_)) if *target == DataType::Text => (),
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
    let q = query(table, expr)?;
    cast_subset(q.filter.as_ref().ok_or(SqlError::Unsupported)?, table)?;
    let bound = q.bind(state, 0, &[])?;
    if !bound.filter.as_ref().is_some_and(immutable) {
        return Err(SqlError::Unsupported);
    }
    Ok(())
}
pub(in crate::relational) fn constraint_check(
    state: &Relational,
    table: &TableDef,
    expr: &str,
    row: &SqlRow,
) -> R<()> {
    let q = query(table, expr)?;
    let bound = q.bind(state, 0, &[])?;
    let result = bound.filter.as_ref().ok_or(SqlError::Unsupported)?.eval(
        &Tuple {
            values: vec![row.values.clone()],
            version: row.version,
        },
        &[],
        &mut 0,
    )?;
    if result == Some(false) {
        Err(SqlError::CheckViolation)
    } else {
        Ok(())
    }
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
        let stored = query(&table, &canonical).unwrap();
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
