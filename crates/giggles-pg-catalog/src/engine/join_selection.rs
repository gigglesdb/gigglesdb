use super::*;

/// Retain the established preserved-input and narrow inner-join placement,
/// then move pure single-base conjuncts to their earliest safe input.
pub(super) fn place(query: &mut Query, parameters: &[PgType]) {
    place_source(
        &mut query.source,
        &mut query.filter,
        query.source_width,
        parameters,
    );
    place_inputs(&mut query.source, query.source_width, parameters);
    push_terms(
        &mut query.source,
        &mut query.filter,
        query.source_width,
        parameters,
    );
    for expression in query
        .projection
        .iter_mut()
        .chain(std::iter::once(&mut query.filter))
        .chain(query.group.iter_mut())
        .chain(query.order.iter_mut().map(|o| &mut o.e))
        .chain(query.expand.iter_mut().flatten())
    {
        nested_queries(expression, parameters);
    }
}

// Optimize each query within its own scope. Derived outputs remain barriers:
// predicates above them never cross projection, window, grouping, or LIMIT.
fn place_inputs(source: &mut Source, width: usize, parameters: &[PgType]) {
    match source {
        Source::Derived(query) | Source::Cte(query) => {
            place(std::sync::Arc::make_mut(query), parameters)
        }
        Source::Union(l, r, _) => {
            place(l, parameters);
            place(r, parameters);
        }
        Source::FlatUnion(arms) => {
            for query in arms {
                place(query, parameters);
            }
        }
        Source::Filtered(input, filter) => {
            place_inputs(input, width, parameters);
            push_terms(input, filter, width, parameters);
            nested_queries(filter, parameters);
        }
        Source::Join(l, r, on, left, rwidth, lateral) => {
            let left_width = width - *rwidth;
            place_inputs(l, left_width, parameters);
            place_inputs(r, *rwidth, parameters);
            nested_queries(on, parameters);
            if *lateral {
                return;
            }
            let mut terms = vec![];
            conjuncts(std::mem::replace(on, truth()), &mut terms);
            let mut retained = vec![];
            for mut term in terms {
                let span = column_span(&term);
                let pushed = if !left_eligible(&term, width, parameters) {
                    false
                } else if span.is_some_and(|(_, max)| max < left_width) && !*left {
                    push_one(l, &term, left_width)
                        || push_join_key(l, &term, left_width, parameters)
                } else if span.is_some_and(|(min, max)| min >= left_width && max < width) {
                    rebase(&mut term, left_width);
                    if push_one(r, &term, *rwidth) || push_join_key(r, &term, *rwidth, parameters) {
                        true
                    } else {
                        shift(&mut term, left_width);
                        false
                    }
                } else {
                    false
                };
                if !pushed {
                    retained.push(term);
                }
            }
            *on = conjunction(retained);
        }
        Source::Series(l, r) => {
            nested_queries(l, parameters);
            nested_queries(r, parameters);
        }
        Source::Scalar(e) | Source::Unnest(e, _) => nested_queries(e, parameters),
        _ => {}
    }
}

fn push_terms(source: &mut Source, filter: &mut E, width: usize, parameters: &[PgType]) {
    // A bare catalog already reaches its query filter before any join. Keep
    // that placement to avoid charging/materializing a redundant filter stage.
    if matches!(source, Source::Catalog(_)) {
        return;
    }
    let mut terms = vec![];
    conjuncts(std::mem::replace(filter, truth()), &mut terms);
    *filter = conjunction(
        terms
            .into_iter()
            .filter(|term| {
                !left_eligible(term, width, parameters) || !push_one(source, term, width)
            })
            .collect(),
    );
}

// A predicate must reference exactly one base relation. In particular, walking
// the right edge of a LEFT JOIN is forbidden for a filter above that join.
fn push_one(source: &mut Source, term: &E, width: usize) -> bool {
    let Some((min, max)) = column_span(term) else {
        return false;
    };
    if max >= width {
        return false;
    }
    match source {
        Source::Catalog(_) => {
            let input = std::mem::replace(source, Source::Empty);
            *source = Source::Filtered(Box::new(input), term.clone());
            true
        }
        Source::Filtered(input, _) => push_one(input, term, width),
        Source::Join(l, r, _, left, rwidth, false) => {
            let left_width = width - *rwidth;
            if max < left_width {
                push_one(l, term, left_width)
            } else if min >= left_width && !*left {
                let mut local = term.clone();
                rebase(&mut local, left_width);
                push_one(r, &local, *rwidth)
            } else {
                false
            }
        }
        _ => false,
    }
}

// A comma join's WHERE may mention several earlier inputs. Put a mandatory,
// total equality on their lowest inner join, rather than materializing their
// Cartesian product before the final input's ON can reject it. LEFT, derived,
// filtered and lateral inputs remain barriers, as do fallible predicates.
fn push_join_key(source: &mut Source, term: &E, width: usize, parameters: &[PgType]) -> bool {
    if !eligible(term, width, parameters) {
        return false;
    }
    let Some((min, max)) = column_span(term) else {
        return false;
    };
    let Source::Join(l, r, on, false, rwidth, false) = source else {
        return false;
    };
    if max >= width || !left_eligible(on, width, parameters) {
        return false;
    }
    let left_width = width - *rwidth;
    if max < left_width {
        push_join_key(l, term, left_width, parameters)
    } else if min >= left_width {
        let mut local = term.clone();
        rebase(&mut local, left_width);
        push_join_key(r, &local, *rwidth, parameters)
    } else {
        *on = binary(std::mem::replace(on, truth()), Op::And, term.clone());
        true
    }
}

fn column_span(expression: &E) -> Option<(usize, usize)> {
    let mut expression = expression.clone();
    let mut span = None;
    walk(&mut expression, &mut |e| {
        if let K::Column(i) = e.kind {
            span = Some(span.map_or((i, i), |(min, max): (usize, usize)| {
                (min.min(i), max.max(i))
            }));
        }
    });
    span
}

fn rebase(expression: &mut E, offset: usize) {
    walk(expression, &mut |e| {
        if let K::Column(i) = &mut e.kind {
            *i -= offset;
        }
    });
}

fn shift(expression: &mut E, offset: usize) {
    walk(expression, &mut |e| {
        if let K::Column(i) = &mut e.kind {
            *i += offset;
        }
    });
}

/// Pick a mandatory, direct cross-input equality for candidate lookup. Keep
/// expressions with parameters or fallible/unsupported operations on the
/// original nested-loop path, preserving their error placement.
pub(super) fn equality_key(on: &E, width: usize, rwidth: usize) -> Option<(usize, usize)> {
    if !left_eligible(on, width, &[]) {
        return None;
    }
    fn representation(ty: PgType) -> Option<u8> {
        match ty {
            PgType::Int2 | PgType::Int4 | PgType::Int8 | PgType::Oid => Some(0),
            PgType::Text | PgType::Name | PgType::Char => Some(1),
            PgType::Bool => Some(2),
            _ => None,
        }
    }
    fn find(e: &E, left_width: usize, width: usize) -> Option<(usize, usize)> {
        match &e.kind {
            K::Binary(l, Op::And, r) => {
                find(l, left_width, width).or_else(|| find(r, left_width, width))
            }
            K::Binary(l, Op::Eq, r) => {
                let (K::Column(a), K::Column(b)) = (&l.kind, &r.kind) else {
                    return None;
                };
                if representation(l.ty).is_none() || representation(l.ty) != representation(r.ty) {
                    return None;
                }
                if *a < left_width && *b >= left_width && *b < width {
                    Some((*a, *b - left_width))
                } else if *b < left_width && *a >= left_width && *a < width {
                    Some((*b, *a - left_width))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
    find(on, width - rwidth, width)
}

fn nested_queries(expression: &mut E, parameters: &[PgType]) {
    walk(expression, &mut |e| match &mut e.kind {
        K::Subquery(q) | K::Exists(q, _) | K::ArrayQuery(q) | K::InQuery(_, q, _) => {
            place(q, parameters)
        }
        _ => {}
    });
}

fn walk(expression: &mut E, visit: &mut dyn FnMut(&mut E)) {
    visit(expression);
    match &mut expression.kind {
        K::Binary(l, _, r) | K::Index(l, r) | K::Any(l, _, r) => {
            walk(l, visit);
            walk(r, visit);
        }
        K::Not(e)
        | K::Neg(e)
        | K::Is(e, _, _)
        | K::Cast(e, _)
        | K::IntegerOid(e, _)
        | K::Regclass(e)
        | K::Regproc(e)
        | K::Regoper(e)
        | K::RegisteredDisplay(e)
        | K::Member(e, _)
        | K::Expand(e)
        | K::Unnest(e)
        | K::InQuery(e, _, _) => walk(e, visit),
        K::Call(_, args) | K::Array(args) => {
            for e in args {
                walk(e, visit);
            }
        }
        K::In(e, args, _) => {
            walk(e, visit);
            for e in args {
                walk(e, visit);
            }
        }
        K::Case(operand, arms, otherwise) => {
            if let Some(e) = operand {
                walk(e, visit);
            }
            for (condition, result) in arms {
                walk(condition, visit);
                walk(result, visit);
            }
            walk(otherwise, visit);
        }
        K::ArrayAgg(e, order) => {
            walk(e, visit);
            for o in order {
                walk(&mut o.e, visit);
            }
        }
        K::RowNumber(partition, order) => {
            for e in partition {
                walk(e, visit);
            }
            for o in order {
                walk(&mut o.e, visit);
            }
        }
        _ => {}
    }
}

fn conjuncts(e: E, out: &mut Vec<E>) {
    if let K::Binary(l, Op::And, r) = e.kind {
        conjuncts(*l, out);
        conjuncts(*r, out);
    } else {
        out.push(e);
    }
}

fn conjunction(expressions: Vec<E>) -> E {
    expressions
        .into_iter()
        .reduce(|l, r| binary(l, Op::And, r))
        .unwrap_or_else(truth)
}

fn place_source(source: &mut Source, filter: &mut E, width: usize, parameters: &[PgType]) {
    match source {
        Source::Join(l, _, _, true, rwidth, false) => {
            let left_width = width - *rwidth;
            let mut terms = vec![];
            conjuncts(std::mem::replace(filter, truth()), &mut terms);
            let (pushed, retained): (Vec<_>, Vec<_>) = terms
                .into_iter()
                .partition(|e| left_eligible(e, left_width, parameters));
            *filter = conjunction(retained);
            if !pushed.is_empty() {
                let mut left_filter = conjunction(pushed);
                place_source(l, &mut left_filter, left_width, parameters);
                if left_filter != truth() {
                    let input = std::mem::replace(l.as_mut(), Source::Empty);
                    **l = Source::Filtered(Box::new(input), left_filter);
                }
            }
        }
        Source::Join(_, _, on, false, _, false)
            if *on == truth() && eligible(filter, width, parameters) =>
        {
            *on = std::mem::replace(filter, truth());
        }
        _ => {}
    }
}

fn left_eligible(e: &E, width: usize, parameters: &[PgType]) -> bool {
    match &e.kind {
        K::Literal(_) => true,
        K::Column(i) => *i < width,
        K::Param(i) => parameters.get(*i) == Some(&e.ty),
        // Execute binds this exact input type before reading any relation. A
        // redundant explicit parameter cast cannot introduce a scan-time error.
        K::Cast(inner, PgType::Oid) if matches!(inner.kind, K::Param(_)) => {
            let K::Param(i) = inner.kind else {
                unreachable!()
            };
            parameters.get(i) == Some(&PgType::Oid)
                && (inner.ty == PgType::Oid || textual(inner.ty))
        }
        K::Binary(
            l,
            Op::And
            | Op::Or
            | Op::Eq
            | Op::NotEq
            | Op::Lt
            | Op::LtEq
            | Op::Gt
            | Op::GtEq
            | Op::PGRegexMatch
            | Op::PGRegexNotMatch
            | Op::PGRegexIMatch
            | Op::PGRegexNotIMatch,
            r,
        ) => left_eligible(l, width, parameters) && left_eligible(r, width, parameters),
        K::Not(e) | K::Is(e, _, _) => left_eligible(e, width, parameters),
        K::In(e, values, _) => {
            left_eligible(e, width, parameters)
                && values.iter().all(|e| left_eligible(e, width, parameters))
        }
        K::Call(name, args)
            if matches!(
                name.as_str(),
                "lower"
                    | "upper"
                    | "length"
                    | "char_length"
                    | "coalesce"
                    | "nullif"
                    | "pg_table_is_visible"
                    | "pg_type_is_visible"
                    | "pg_function_is_visible"
                    | "pg_get_userbyid"
                    | "format_type"
                    | "like"
                    | "ilike"
            ) =>
        {
            args.iter().all(|e| left_eligible(e, width, parameters))
        }
        // In particular, volatile/session calls, subqueries, and fallible
        // arithmetic/casts retain their original evaluation placement.
        _ => false,
    }
}

fn primitive(ty: PgType) -> bool {
    matches!(
        ty,
        PgType::Bool
            | PgType::Int2
            | PgType::Int4
            | PgType::Int8
            | PgType::Oid
            | PgType::Text
            | PgType::Name
            | PgType::Char
    )
}

fn operand(e: &E, width: usize, parameters: &[PgType]) -> bool {
    primitive(e.ty)
        && match &e.kind {
            K::Literal(Value::Null | Value::Bool(_) | Value::Int(_) | Value::Text(_)) => true,
            K::Column(i) => *i < width,
            // A later use can promote the global parameter type. Keep the old
            // placement if this leaf would still cast to a different type.
            K::Param(i) => parameters.get(*i) == Some(&e.ty),
            _ => false,
        }
}

fn eligible(e: &E, width: usize, parameters: &[PgType]) -> bool {
    match &e.kind {
        K::Binary(l, Op::And, r) => {
            eligible(l, width, parameters) && eligible(r, width, parameters)
        }
        K::Binary(l, Op::Eq, r) => operand(l, width, parameters) && operand(r, width, parameters),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(sql: &str) -> Query {
        plan(sql).unwrap().unwrap().query
    }

    fn filtered(source: &Source) -> usize {
        match source {
            Source::Filtered(input, _) => 1 + filtered(input),
            Source::Join(l, r, ..) => filtered(l) + filtered(r),
            _ => 0,
        }
    }

    #[test]
    fn null_extended_where_keeps_original_placement() {
        let query = query("SELECT t.oid FROM pg_type t LEFT JOIN pg_type e ON true WHERE e.oid=23");
        assert_eq!(filtered(&query.source), 0);
        assert_ne!(query.filter, truth());
    }

    #[test]
    fn left_on_filters_only_the_nullable_input() {
        let query =
            query("SELECT t.oid FROM pg_type t LEFT JOIN pg_type e ON t.oid=23 AND e.oid=23");
        let Source::Join(l, r, on, true, _, _) = &query.source else {
            panic!("expected LEFT JOIN");
        };
        assert_eq!(filtered(l), 0);
        assert_eq!(filtered(r), 1);
        assert_ne!(*on, truth());
        assert!(column_span(on).is_some_and(|(_, max)| max < query.source_width - 1));
    }

    #[test]
    fn inner_on_filters_both_inputs_and_retains_relation_comparison() {
        let query = query(
            "SELECT t.oid FROM pg_type t JOIN pg_type e ON t.oid=e.oid AND t.oid=23 AND e.typname LIKE 'int%'",
        );
        let Source::Join(l, r, on, false, _, _) = &query.source else {
            panic!("expected INNER JOIN");
        };
        assert_eq!(filtered(l), 1);
        assert_eq!(filtered(r), 1);
        let Some((min, max)) = column_span(on) else {
            panic!("expected relation comparison");
        };
        assert!(min < query.source_width / 2 && max >= query.source_width / 2);
    }

    #[test]
    fn multi_relation_non_equality_stays_above_join() {
        let query = query("SELECT t.oid FROM pg_type t CROSS JOIN pg_type e WHERE t.oid<e.oid");
        assert_eq!(filtered(&query.source), 0);
        assert_ne!(query.filter, truth());
    }

    #[test]
    fn correlated_columns_are_not_local_base_columns() {
        let mut source = Source::Catalog("pg_type".into());
        let mut filter = binary(
            column(1, PgType::Oid),
            Op::Eq,
            literal(Value::Int(23), PgType::Oid),
        );
        let original = filter.clone();
        push_terms(&mut source, &mut filter, 1, &[]);
        assert_eq!(filter, original);
        assert_eq!(filtered(&source), 0);
    }

    #[test]
    fn candidate_index_requires_mandatory_pure_equality() {
        let equality = binary(column(0, PgType::Oid), Op::Eq, column(1, PgType::Int4));
        assert_eq!(equality_key(&equality, 2, 1), Some((0, 0)));
        let optional = binary(equality.clone(), Op::Or, truth());
        assert_eq!(equality_key(&optional, 2, 1), None);
        let unsafe_term = binary(
            column(0, PgType::Oid),
            Op::Eq,
            E {
                kind: K::Param(0),
                ty: PgType::Oid,
            },
        );
        assert_eq!(
            equality_key(&binary(equality, Op::And, unsafe_term), 2, 1),
            None
        );
        let coercing = binary(column(0, PgType::Text), Op::Eq, column(1, PgType::Oid));
        assert_eq!(equality_key(&coercing, 2, 1), None);
    }

    #[test]
    fn derived_window_is_a_barrier_but_its_own_filter_is_placed() {
        let query = query(
            "SELECT d.oid FROM (SELECT t.oid,row_number() OVER (ORDER BY t.oid) AS rn FROM pg_type t JOIN pg_namespace n ON t.typnamespace=n.oid WHERE t.typname LIKE 'int%') d WHERE d.rn=1",
        );
        let Source::Derived(inner) = &query.source else {
            panic!("expected derived window");
        };
        assert_ne!(query.filter, truth());
        assert!(
            inner
                .projection
                .iter()
                .any(|e| matches!(e.kind, K::RowNumber(..)))
        );
        assert_eq!(filtered(&inner.source), 1);
        assert_eq!(inner.filter, truth());
    }
}
