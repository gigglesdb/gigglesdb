//! Overflow-only, flat UNION ALL adapter. Never build a recursive union AST.
use super::*;

fn keyword(token: &Token, keyword: Keyword) -> bool {
    matches!(token, Token::Word(w) if w.keyword == keyword && w.quote_style.is_none())
}

fn split(tokens: Vec<Token>) -> R<Vec<Vec<Token>>> {
    let tokens: Vec<_> = tokens
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_) | Token::EOF))
        .collect();
    let mut arms = vec![];
    let mut arm = vec![];
    let mut delimiters = vec![];
    let mut constructors = 0;
    let mut not_run = 0usize;
    let mut iter = tokens.into_iter().peekable();
    while let Some(token) = iter.next() {
        if delimiters.is_empty() {
            if keyword(&token, Keyword::UNION) {
                if !iter.next().is_some_and(|t| keyword(&t, Keyword::ALL)) {
                    return Err(Error::Unsupported);
                }
                if arm.is_empty() {
                    return Err(Error::Syntax);
                }
                arms.push(std::mem::take(&mut arm));
                if arms.len() >= 16 {
                    return Err(Error::Limit);
                }
                constructors = 0;
                not_run = 0;
                continue;
            }
            if arm.is_empty() && !keyword(&token, Keyword::SELECT) {
                return Err(Error::Unsupported);
            }
            if [
                Keyword::WITH,
                Keyword::ORDER,
                Keyword::LIMIT,
                Keyword::OFFSET,
                Keyword::FETCH,
                Keyword::FOR,
                Keyword::INTERSECT,
                Keyword::EXCEPT,
            ]
            .into_iter()
            .any(|k| keyword(&token, k))
            {
                return Err(Error::Unsupported);
            }
        }
        match token {
            Token::LParen => delimiters.push(false),
            Token::LBracket => delimiters.push(true),
            Token::RParen if delimiters.pop() != Some(false) => return Err(Error::Syntax),
            Token::RBracket if delimiters.pop() != Some(true) => return Err(Error::Syntax),
            Token::SemiColon => return Err(Error::Mutation),
            _ => {}
        }
        // sqlparser's recursion64 guard can exhaust a debug test stack on
        // forty consecutive NOT prefixes before reaching that guard. Bound
        // this token-level prefix run before entering its recursive parser.
        // This conservative extra guard applies only to the overflow adapter.
        if keyword(&token, Keyword::NOT) {
            not_run += 1;
            if not_run > 16 {
                return Err(Error::Limit);
            }
        } else {
            not_run = 0;
        }
        constructors += usize::from(constructor(&token));
        if constructors > 256 {
            return Err(Error::Limit);
        }
        arm.push(token);
    }
    if !delimiters.is_empty() || arm.is_empty() {
        return Err(Error::Syntax);
    }
    arms.push(arm);
    if arms.len() < 2 {
        return Err(Error::Limit);
    }
    Ok(arms)
}

pub(super) fn plan(tokens: Vec<Token>, gateway: bool) -> R<Option<Plan>> {
    let arms = split(tokens)?;
    let mut scan = Scan::new();
    let mut projections = ProjectionParameters(false);
    // The flat wrapper consumes one query and one source, like an operator.
    let mut compiler = Compiler {
        parameters: 0,
        qcount: 1,
        sources: 1,
        ctes: BTreeMap::new(),
    };
    let mut compiled = vec![];
    let mut failure = None;
    for tokens in arms {
        let statements = Parser::new(&PostgreSqlDialect {})
            .with_recursion_limit(64)
            .with_tokens(tokens)
            .parse_statements()
            .map_err(|_| Error::Syntax)?;
        if statements.len() != 1 {
            return Err(Error::Mutation);
        }
        // CTE aliases from another arm must never mask an application relation.
        scan.ctes.clear();
        if let ControlFlow::Break(e) = statements.visit(&mut scan) {
            return Err(e);
        }
        let a::Statement::Query(q) = &statements[0] else {
            return Err(Error::Mutation);
        };
        if !matches!(q.body.as_ref(), a::SetExpr::Select(_)) || q.with.is_some() {
            return Err(Error::Unsupported);
        }
        let _ = q.visit(&mut projections);
        // Classification of every arm precedes any successful plan/None return.
        // Keep the first compiler failure until combined routing is established.
        match compiler.query(q, &vec![]) {
            Ok(q) => compiled.push(q),
            Err(e) => {
                failure.get_or_insert(e);
            }
        }
        // This bounded AST is dropped before parsing the next arm.
    }
    if !scan.catalog {
        return Ok(None);
    }
    if scan.mutation {
        return Err(Error::Mutation);
    }
    if scan.user {
        return Err(Error::MixedRelations);
    }
    if gateway && scan.core_scalar && !scan.explicit_catalog {
        return Ok(None);
    }
    if let Some(e) = failure {
        return Err(e);
    }
    let fields = compiled[0].fields.clone();
    if compiled.iter().any(|q| {
        q.fields.len() != fields.len()
            || q.fields
                .iter()
                .zip(&fields)
                .any(|(a, b)| a.data_type != b.data_type)
    }) {
        return Err(Error::Type);
    }
    let query = Query {
        projection: fields
            .iter()
            .enumerate()
            .map(|(i, f)| column(i, f.data_type))
            .collect(),
        source_width: fields.len(),
        fields,
        source: Source::FlatUnion(compiled),
        filter: truth(),
        order: vec![],
        limit: MAX_ROWS,
        distinct: false,
        expand: None,
        group: vec![],
        aggregate: false,
    };
    finish_plan(query, compiler.parameters, projections.0)
}

pub(super) fn validate_wrapper(q: &Query) -> R<()> {
    if q.filter != truth()
        || !q.order.is_empty()
        || !q.group.is_empty()
        || q.expand.is_some()
        || q.distinct
        || q.aggregate
        || q.limit != MAX_ROWS
        || q.source_width != q.fields.len()
        || q.projection.len() != q.fields.len()
        || q.projection
            .iter()
            .zip(&q.fields)
            .enumerate()
            .any(|(i, (e, f))| *e != column(i, f.data_type))
    {
        return Err(Error::Unsupported);
    }
    Ok(())
}

pub(super) fn execute(rt: &mut Runtime<'_>, arms: &[Query], outer: &[Value]) -> R<Vec<Vec<Value>>> {
    let mut out = vec![];
    let mut bytes = 0usize;
    for arm in arms {
        let incoming = rt.query(arm, outer)?;
        let incoming_bytes = incoming
            .iter()
            .flatten()
            .try_fold(0usize, |n, v| n.checked_add(v.bytes()).ok_or(Error::Limit))?;
        // Account only retained result values, not operator internals or RSS.
        if incoming.len() > MAX_ROWS
            || incoming_bytes > MAX_BYTES
            || bytes.checked_add(incoming_bytes).ok_or(Error::Limit)? > 2 * MAX_BYTES
        {
            return Err(Error::Limit);
        }
        for row in incoming {
            rt.charge(1)?;
            admit(&out, &row, &mut bytes)?;
            out.push(row);
        }
    }
    Ok(out)
}
