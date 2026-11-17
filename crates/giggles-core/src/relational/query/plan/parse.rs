use super::*;

pub(super) fn parser(tokens: &[Token]) -> Parser {
    Parser {
        tokens: tokens.to_vec(),
        at: 0,
        params: BTreeSet::new(),
        relations: vec![],
    }
}
pub(super) fn word(t: &Token, w: &str) -> bool {
    matches!(t, Token::Word(s) if s == w)
}
fn top(tokens: &[Token]) -> R<Vec<usize>> {
    let mut depth = 0usize;
    let mut out = vec![];
    for (i, t) in tokens.iter().enumerate() {
        if *t == Token::Symbol(')') {
            depth = depth.checked_sub(1).ok_or(SqlError::Syntax)?;
        }
        if depth == 0 {
            out.push(i);
        }
        if *t == Token::Symbol('(') {
            depth += 1;
            if depth > 32 {
                return Err(SqlError::Limit);
            }
        }
    }
    if depth != 0 {
        return Err(SqlError::Syntax);
    }
    Ok(out)
}
impl Plan {
    pub(in crate::relational) fn claims(tokens: &[Token]) -> bool {
        matches!(tokens.first(), Some(Token::Word(w)) if ["select", "values", "table", "explain", "with"].contains(&w.as_str()))
            || tokens.first() == Some(&Token::Symbol('('))
    }
    pub(in crate::relational) fn parse(p: &mut Parser) -> R<Self> {
        let mut tokens = p.tokens[p.at..].to_vec();
        if tokens.last() == Some(&Token::Symbol(';')) {
            tokens.pop();
        }
        if tokens.len() > MAX_WORK * 8 {
            return Err(SqlError::Limit);
        }
        let plan = expression(&tokens, p, 0, &mut 0)?;
        // Query's normalizer may rewrite tokens; collect original parameter numbers.
        p.params.extend(tokens.iter().filter_map(|t| {
            if let Token::Param(n) = t {
                Some(*n)
            } else {
                None
            }
        }));
        p.at = p.tokens.len();
        Ok(plan)
    }
}
pub(super) fn expression(
    tokens: &[Token],
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
) -> R<Plan> {
    if depth > 32 {
        return Err(SqlError::Limit);
    }
    if tokens.is_empty() {
        return Err(SqlError::Syntax);
    }
    if tokens.first().is_some_and(|t| word(t, "with")) {
        return cte::parse(tokens, owner, depth, next);
    }
    let positions = top(tokens)?;
    let tail = positions
        .iter()
        .copied()
        .find(|i| {
            ["order", "limit", "offset", "fetch", "for"]
                .iter()
                .any(|w| word(&tokens[*i], w))
        })
        .unwrap_or(tokens.len());
    let body = &tokens[..tail];
    let mut split = None;
    for i in positions.iter().copied().filter(|i| *i < tail) {
        if word(&tokens[i], "union") || word(&tokens[i], "except") {
            split = Some(i);
        }
    }
    if split.is_none() {
        split = positions
            .iter()
            .copied()
            .rfind(|i| *i < tail && word(&tokens[*i], "intersect"));
    }
    let mut plan = if let Some(i) = split {
        let op = if word(&tokens[i], "union") {
            SetOp::Union
        } else if word(&tokens[i], "except") {
            SetOp::Except
        } else {
            SetOp::Intersect
        };
        let all = body.get(i + 1).is_some_and(|t| word(t, "all"));
        let modifier = all || body.get(i + 1).is_some_and(|t| word(t, "distinct"));
        Plan::new(Body::Set {
            op,
            all,
            left: Box::new(expression(&body[..i], owner, depth + 1, next)?),
            right: Box::new(expression(
                &body[i + 1 + usize::from(modifier)..],
                owner,
                depth + 1,
                next,
            )?),
        })
    } else if body.first() == Some(&Token::Symbol('(')) {
        if body.last() != Some(&Token::Symbol(')')) {
            return Err(SqlError::Syntax);
        }
        // Keep both pagination boundaries, even for ((SELECT ...) LIMIT ...).
        let child = expression(&body[1..body.len() - 1], owner, depth + 1, next)?;
        if tail == tokens.len() {
            return Ok(child);
        }
        // An identity SELECT is an ordinary materialized-input operator.
        let name = "\0plan_input".to_string();
        let mut p = parser(&[
            Token::Word("select".into()),
            Token::Symbol('*'),
            Token::Word("from".into()),
            Token::QuotedIdent("plan_input".into()),
        ]);
        let mut query = Query::parse(&mut p)?;
        query.sources[0].name = name.clone();
        Plan::new(Body::Select(Box::new(Select {
            query,
            parameters: BTreeSet::new(),
            input: Some(Input {
                query: Box::new(child),
                name,
                aliases: vec![],
            }),
        })))
    } else if body.first().is_some_and(|t| word(t, "values")) {
        let mut p = parser(body);
        p.need("values")?;
        let mut rows = vec![];
        loop {
            if !p.sym('(') {
                return Err(SqlError::Syntax);
            }
            let mut row = vec![];
            loop {
                row.push(Scalar::parse_expr(&mut p, 0, false)?);
                if row.len() > MAX_COLUMNS {
                    return Err(SqlError::Limit);
                }
                if !p.sym(',') {
                    break;
                }
            }
            if !p.sym(')') {
                return Err(SqlError::Syntax);
            }
            if rows
                .first()
                .is_some_and(|r: &Vec<Scalar>| r.len() != row.len())
            {
                return Err(SqlError::Syntax);
            }
            rows.push(row);
            if rows.len() > MAX_WORK {
                return Err(SqlError::Limit);
            }
            if !p.sym(',') {
                break;
            }
        }
        if p.at != p.tokens.len() {
            return Err(SqlError::Syntax);
        }
        Plan::new(Body::Values(rows))
    } else {
        return select(tokens, owner, depth, next);
    };
    let mut p = parser(&tokens[tail..]);
    if p.word("order") {
        p.need("by")?;
        loop {
            let scalar = Scalar::parse_expr(&mut p, 0, false)?;
            let desc = if p.word("using") {
                match p.tokens.get(p.at) {
                    Some(Token::Op(op)) if op == "<" || op == ">" => {
                        let desc = op == ">";
                        p.at += 1;
                        desc
                    }
                    _ => return Err(SqlError::UndefinedFunction),
                }
            } else {
                let desc = p.word("desc");
                if !desc {
                    p.word("asc");
                }
                desc
            };
            let first = if p.word("nulls") {
                if p.word("first") {
                    true
                } else {
                    p.need("last")?;
                    false
                }
            } else {
                desc
            };
            plan.order.push((scalar, desc, first));
            if !p.sym(',') {
                break;
            }
        }
    }
    let mut seen_limit = false;
    let mut seen_offset = false;
    loop {
        if !seen_limit && p.word("limit") {
            seen_limit = true;
            if !p.word("all") {
                plan.limit = Some(Scalar::parse_expr(&mut p, 0, false)?);
            }
        } else if !seen_offset && p.word("offset") {
            seen_offset = true;
            plan.offset = Some(Scalar::parse_expr(&mut p, 0, false)?);
            if !p.word("rows") {
                p.word("row");
            }
        } else {
            break;
        }
    }
    if p.at != p.tokens.len() {
        return Err(SqlError::Unsupported);
    }
    Ok(plan)
}
fn select(tokens: &[Token], owner: &mut Parser, depth: usize, next: &mut usize) -> R<Plan> {
    let mut tokens = tokens.to_vec();
    if tokens.first().is_some_and(|t| word(t, "table")) {
        tokens.splice(
            ..1,
            [
                Token::Word("select".into()),
                Token::Symbol('*'),
                Token::Word("from".into()),
            ],
        );
    }
    if !tokens.first().is_some_and(|t| word(t, "select")) {
        return Err(SqlError::Unsupported);
    }
    if tokens.get(1).is_some_and(|t| word(t, "all")) {
        tokens.remove(1);
    }
    let positions = top(&tokens)?;
    let order = positions
        .iter()
        .copied()
        .find(|i| word(&tokens[*i], "order"));
    if let Some(order) = order {
        let mut i = order + 2;
        while i + 1 < tokens.len() {
            if word(&tokens[i], "using") {
                let desc = match &tokens[i + 1] {
                    Token::Op(op) if op == "<" || op == ">" => op == ">",
                    _ => return Err(SqlError::UndefinedFunction),
                };
                tokens.splice(
                    i..i + 2,
                    [Token::Word(if desc { "desc" } else { "asc" }.into())],
                );
            }
            i += 1;
        }
    }
    let mut input = None;
    if let Some(from) = positions
        .iter()
        .copied()
        .find(|i| word(&tokens[*i], "from"))
        && tokens.get(from + 1) == Some(&Token::Symbol('('))
    {
        let open = from + 1;
        let close = subquery_end(&tokens, open)?;
        let child = expression(&tokens[open + 1..close], owner, depth + 1, next)?;
        let mut p = parser(&tokens[close + 1..]);
        let explicit = p.word("as");
        let alias = if explicit
            || matches!(p.tokens.get(p.at), Some(Token::QuotedIdent(_)))
            || matches!(p.tokens.get(p.at), Some(Token::Word(w)) if !["where", "order", "limit", "offset", "group", "having"].contains(&w.as_str()))
        {
            p.name()?
        } else {
            "unnamed_subquery".into()
        };
        let mut aliases = vec![];
        if p.sym('(') {
            loop {
                aliases.push(p.name()?);
                if !p.sym(',') {
                    break;
                }
            }
            if !p.sym(')') {
                return Err(SqlError::Syntax);
            }
        }
        let name = "\0plan_input".to_string();
        tokens.splice(
            open..close + 1 + p.at,
            [
                Token::QuotedIdent("plan_input".into()),
                Token::Word("as".into()),
                Token::QuotedIdent(alias),
            ],
        );
        input = Some(Input {
            query: Box::new(child),
            name,
            aliases,
        });
    }
    let mut p = parser(&tokens);
    let mut query = Query::parse(&mut p)?;
    if let Some(input) = &input {
        query.sources[0].name = input.name.clone();
    }
    let parameters = p.params.clone();
    owner.params.extend(p.params);
    owner.relations.extend(
        p.relations
            .into_iter()
            .filter(|r| input.is_none() || r.name != "plan_input"),
    );
    Ok(Plan::new(Body::Select(Box::new(Select {
        query,
        parameters,
        input,
    }))))
}
