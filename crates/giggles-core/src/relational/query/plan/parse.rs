use super::*;

pub(super) type ParseResult<T> = Result<T, ParseError>;

pub(super) fn parser(tokens: &[Token]) -> Parser {
    Parser { privilege_seen: false, privilege_casts: false,
        tokens: tokens.to_vec(),
        at: 0,
        params: BTreeSet::new(),
        relations: vec![],
    }
}
pub(super) fn word(t: &Token, w: &str) -> bool {
    matches!(t, Token::Word(s) if s == w)
}
pub(super) fn unqualified(tokens: &[Token], at: usize) -> bool {
    at == 0 || !(tokens[at - 1] == Token::Symbol('.') || word(&tokens[at - 1], "as"))
}
pub(super) fn group_clause(tokens: &[Token], at: usize) -> bool {
    word(&tokens[at], "group")
        && unqualified(tokens, at)
        && (at == 0 || !word(&tokens[at - 1], "within"))
}
pub(super) fn top(tokens: &[Token]) -> R<Vec<usize>> {
    top_positions(tokens, false)
}
fn top_positions(tokens: &[Token], statement_end: bool) -> R<Vec<usize>> {
    let mut depth = 0usize;
    let mut out = vec![];
    for (i, t) in tokens.iter().enumerate() {
        if *t == Token::Symbol(')') {
            depth = depth.checked_sub(1).ok_or_else(|| clause_syntax(Some(t)))?;
        }
        if depth == 0 {
            out.push(i);
        }
        if *t == Token::Symbol('(') {
            depth += 1;
            if depth > subplans::MAX_DEPTH {
                return Err(SqlError::Limit);
            }
        }
    }
    if depth != 0 {
        return Err(if statement_end {
            clause_syntax(tokens.last().filter(|t| **t == Token::Symbol(';')))
        } else {
            SqlError::Syntax
        });
    }
    Ok(out)
}
impl Plan {
    pub(in crate::relational) fn requires_grouping_rewrite(tokens: &[Token]) -> bool {
        let mut depth = 0usize;
        let mut group = false;
        let mut start = false;
        let mut at = 0;
        while at < tokens.len() {
            let token = &tokens[at];
            if *token == Token::Symbol(')') { depth = depth.saturating_sub(1); }
            let qualified = !unqualified(tokens, at);
            if depth == 0 {
                if group_clause(tokens, at) && tokens.get(at + 1).is_some_and(|t| word(t, "by")) {
                    group = true;
                    start = true;
                    at += 2;
                    if tokens.get(at).is_some_and(|t| word(t, "distinct") || word(t, "all")) {
                        return true;
                    }
                    continue;
                }
                if group {
                    if !qualified && ends_group(token) {
                        group = false;
                    } else if *token == Token::Symbol(',') { start = true; }
                    else {
                        if start && (
                            (word(token, "rollup") || word(token, "cube")) && tokens.get(at + 1) == Some(&Token::Symbol('('))
                            || word(token, "grouping") && tokens.get(at + 1).is_some_and(|t| word(t, "sets"))
                            || *token == Token::Symbol('(') && group_sets::parenthesized_list(tokens, at)
                        ) { return true; }
                        start = false;
                    }
                }
            }
            if *token == Token::Symbol('(') { depth += 1; }
            at += 1;
        }
        false
    }
    pub(in crate::relational) fn claims(tokens: &[Token]) -> bool {
        if tokens.first() == Some(&Token::Symbol('(')) {
            // Parenthesized query expressions are owned by the planner.
            return true;
        }
        if mutation::claims(tokens) || merge::claims(tokens) { return true; }
        if !tokens.first().is_some_and(|t| word(t, "select")) {
            return tokens
                .first()
                .is_some_and(|t| ["values", "table", "with"].iter().any(|w| word(t, w)));
        }
        if Self::requires_grouping_rewrite(tokens)
            || tokens.get(1).is_some_and(|t| word(t, "distinct")) && tokens.get(2).is_some_and(|t| word(t, "on"))
        { return true; }
        // Claim detection must still see a planner construct when a later
        // operand exceeds the depth limit. Plan::parse owns validation.
        let mut depth = 0usize;
        let positions: Vec<_> = tokens
            .iter()
            .enumerate()
            .filter_map(|(i, t)| {
                if *t == Token::Symbol(')') {
                    depth = depth.saturating_sub(1);
                }
                let position = (depth == 0).then_some(i);
                if *t == Token::Symbol('(') {
                    depth += 1;
                }
                position
            })
            .collect();
        // A simple derived scan already belongs to Query. Probe just FROM so
        // an unrelated rejected projection cannot make that scan a claim.
        let legacy_derived = positions.iter().copied().find_map(|start| {
            if !word(&tokens[start], "from")
                || start
                    .checked_sub(1)
                    .is_some_and(|p| word(&tokens[p], "distinct"))
                || tokens.get(start + 1) != Some(&Token::Symbol('('))
            {
                return None;
            }
            let end = positions
                .iter()
                .copied()
                .find(|i| {
                    *i > start
                        && (group_clause(tokens, *i)
                            || ["where", "having", "order", "limit", "offset", "fetch", "for"]
                                .iter()
                                .any(|w| word(&tokens[*i], w)))
                })
                .unwrap_or(tokens.len());
            let mut probe = vec![Token::Word("select".into()), Token::Symbol('*')];
            probe.extend_from_slice(&tokens[start..end]);
            user_types::probe_preserving_error_message(|| Query::parse(&mut parser(&probe)))
                .ok()
                .map(|_| start + 1)
        });
        if tokens.iter().enumerate().any(|(i, t)| {
            word(t, "over") || word(t, "window") && unqualified(tokens, i)
        })
            || subquery::quantified(tokens)
            || tokens.windows(2).enumerate().any(|(i, pair)| {
                Some(i) != legacy_derived
                    && pair[0] == Token::Symbol('(')
                    && ["select", "values", "table", "with"]
                        .iter()
                        .any(|w| word(&pair[1], w))
            })
        {
            return true;
        }
        let mut from = false;
        let mut from_item = false;
        let mut order = false;
        for i in positions {
            let t = &tokens[i];
            let qualified = i > 0 && tokens[i - 1] == Token::Symbol('.');
            if i > 1 && !qualified && !word(&tokens[i - 1], "as") && word(t, "fetch")
                && tokens.get(i + 1).is_none_or(|t| word(t, "first") || word(t, "next") || *t == Token::Symbol(';'))
            { return true; }
            if (word(t, "limit") || word(t, "offset"))
                && user_types::probe_preserving_error_message(|| Query::parse(&mut parser(tokens))).is_err()
            {
                return true;
            }
            if ["union", "intersect", "except"].iter().any(|w| word(t, w)) {
                return true;
            }
            if word(t, "from")
                && !i
                    .checked_sub(1)
                    .is_some_and(|p| word(&tokens[p], "distinct"))
            {
                from = true;
                from_item = true;
            } else if group_clause(tokens, i) || [
                "where", "having", "order", "limit", "offset", "fetch", "for",
            ]
            .iter()
            .any(|w| word(t, w))
            {
                from = false;
            }
            if from && !qualified && word(t, "tablesample") { return true; }
            if from && word(t, "join") {
                from_item = true;
            } else if word(t, "on") {
                from_item = false;
            }
            // Ordinary scans and INNER/LEFT ON joins stay with the legacy path.
            // Parentheses here cover FROM functions, alias lists and join trees.
            if from
                && (*t == Token::Symbol(',')
                    || from_item && *t == Token::Symbol('(') && Some(i) != legacy_derived
                    || ["lateral", "natural", "right", "full", "cross", "using"]
                        .iter()
                        .any(|w| word(t, w)))
            {
                return true;
            }
            order |= word(t, "order");
            if order && word(t, "using") {
                return true;
            }
        }
        false
    }
    pub(in crate::relational) fn parse(p: &mut Parser) -> ParseResult<Self> {
        let tokens = p.tokens[p.at..].to_vec();
        let (body, following) = match tokens.split_last() {
            Some((semicolon @ Token::Symbol(';'), body)) => (body, Some(semicolon)),
            _ => (tokens.as_slice(), None),
        };
        if body.len() > max_statement_tokens() {
            return Err(SqlError::Limit.into());
        }
        admit_subquery_depth(&tokens)?;
        // Admit parentheses before entering WITH or any recursive planner frame.
        top_positions(&tokens, true).map_err(ParseError::Definite)?;
        let plan = expression(body, following, p, 0, &mut 0)?;
        // Query's normalizer may rewrite tokens; collect original parameter numbers.
        p.params.extend(body.iter().filter_map(|t| {
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
pub(super) fn child_expression(
    tokens: &[Token],
    following: Option<&Token>,
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
) -> ParseResult<Plan> {
    if depth >= subplans::MAX_DEPTH {
        return Err(SqlError::Limit.into());
    }
    expression(tokens, following, owner, depth + 1, next)
}
pub(super) fn expression(
    tokens: &[Token],
    following: Option<&Token>,
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
) -> ParseResult<Plan> {
    if depth > subplans::MAX_DEPTH {
        return Err(SqlError::Limit.into());
    }
    if tokens.is_empty() {
        return Err(SqlError::Syntax.into());
    }
    super::super::row_locks::reject_plan(tokens).map_err(ParseError::Definite)?;
    if merge::claims(tokens) { return merge::parse(tokens, owner, depth, next); }
    if mutation::claims(tokens) { return mutation::parse(tokens, owner, depth, next); }
    if tokens.first().is_some_and(|t| word(t, "with")) {
        return cte::parse(tokens, following, owner, depth, next);
    }
    query_expression(tokens, following, owner, depth, next)
}

// Keep WITH dispatch out of this larger frame: nested WITH bodies must not
// reserve space for unrelated VALUES, pagination and SELECT parsing locals.
fn query_expression(
    tokens: &[Token],
    following: Option<&Token>,
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
) -> ParseResult<Plan> {
    if merge::action_call(tokens) && !tokens.windows(2).any(|t| word(&t[0], "merge") && word(&t[1], "into")) {
        return Err(ParseError::Definite(SqlError::Syntax));
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
    let plan = if let Some(i) = split {
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
            left: Box::new(child_expression(&body[..i], tokens.get(i), owner, depth, next)?),
            right: Box::new(child_expression(
                &body[i + 1 + usize::from(modifier)..],
                tokens.get(tail).or(following),
                owner,
                depth,
                next,
            )?),
        })
    } else if body.first() == Some(&Token::Symbol('(')) {
        if body.last() != Some(&Token::Symbol(')')) {
            return Err(SqlError::Syntax.into());
        }
        // Keep both pagination boundaries, even for ((SELECT ...) LIMIT ...).
        let child = child_expression(&body[1..body.len() - 1], body.last(), owner, depth, next)?;
        if tail == tokens.len() {
            return Ok(child);
        }
        identity(child)?
    } else if body.first().is_some_and(|t| word(t, "values")) {
        values(body)?
    } else {
        return select(tokens, following, owner, depth, next);
    };
    pagination(plan, &tokens[tail..], following)
}

fn identity(child: Plan) -> ParseResult<Plan> {
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
    Ok(Plan::new(Body::Select(Box::new(Select {
        applies: vec![],
        windows: vec![],
        query,
        parameters: BTreeSet::new(),
        from: None,
        input: Some(Input {
            query: Box::new(child),
            name,
            aliases: vec![],
        }),
    }))))
}

fn values(body: &[Token]) -> ParseResult<Plan> {
    let mut p = parser(body);
    p.need("values")?;
    let mut rows = vec![];
    loop {
        if !p.sym('(') {
            return Err(SqlError::Syntax.into());
        }
        let mut row = vec![];
        loop {
            row.push(Scalar::parse_expr(&mut p, 0, false)?);
            if row.len() > MAX_COLUMNS {
                return Err(SqlError::Limit.into());
            }
            if !p.sym(',') {
                break;
            }
        }
        if !p.sym(')') {
            return Err(SqlError::Syntax.into());
        }
        if rows
            .first()
            .is_some_and(|r: &Vec<Scalar>| r.len() != row.len())
        {
            user_types::view_replace_error("VALUES lists must all be the same length".into());
            return Err(ParseError::Definite(SqlError::Syntax));
        }
        rows.push(row);
        if rows.len() > max_statement_tokens() {
            return Err(SqlError::Limit.into());
        }
        if !p.sym(',') {
            break;
        }
    }
    if p.at != p.tokens.len() {
        return Err(SqlError::Syntax.into());
    }
    Ok(Plan::new(Body::Values(rows)))
}

pub(super) fn pagination(
    mut plan: Plan,
    tokens: &[Token],
    following: Option<&Token>,
) -> ParseResult<Plan> {
    let mut p = parser(tokens);
    if p.word("order") {
        if !p.word("by") {
            return Err(ParseError::Definite(clause_syntax(p.tokens.get(p.at).or(following))));
        }
        loop {
            require_clause_operand(&p, following).map_err(ParseError::Definite)?;
            let scalar = Scalar::parse_expr(&mut p, 0, false)?;
            let desc = if p.word("using") {
                match p.tokens.get(p.at) {
                    Some(Token::Op(op)) if op == "<" || op == ">" => {
                        let desc = op == ">";
                        p.at += 1;
                        desc
                    }
                    None => return Err(ParseError::Definite(clause_syntax(following))),
                    _ => return Err(ParseError::Definite(SqlError::UndefinedFunction)),
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
                require_clause_operand(&p, following).map_err(ParseError::Definite)?;
                plan.limit = Some(Scalar::parse_expr(&mut p, 0, false)?);
            }
        } else if !seen_offset && p.word("offset") {
            seen_offset = true;
            require_clause_operand(&p, following).map_err(ParseError::Definite)?;
            plan.offset = Some(Scalar::parse_expr(&mut p, 0, false)?);
            if !p.word("rows") {
                p.word("row");
            }
        } else if !seen_limit && p.word("fetch") {
            seen_limit = true;
            if !p.word("first") && !p.word("next") {
                return Err(ParseError::Definite(clause_syntax(p.tokens.get(p.at).or(following))));
            }
            require_clause_operand(&p, following).map_err(ParseError::Definite)?;
            plan.limit = Some(if p.tokens.get(p.at).is_some_and(|t| word(t, "row") || word(t, "rows")) {
                Scalar::Literal(Value::Int(1))
            } else { Scalar::parse_expr(&mut p, 0, false)? });
            if !p.word("row") && !p.word("rows") {
                return Err(ParseError::Definite(clause_syntax(p.tokens.get(p.at).or(following))));
            }
            if p.word("with") {
                if !p.word("ties") {
                    return Err(ParseError::Definite(clause_syntax(p.tokens.get(p.at).or(following))));
                }
                plan.decorations.with_ties = true;
            } else if !p.word("only") {
                return Err(ParseError::Definite(clause_syntax(p.tokens.get(p.at).or(following))));
            }
        } else {
            break;
        }
    }
    if p.at != p.tokens.len() {
        return Err(SqlError::Unsupported.into());
    }
    if plan.decorations.with_ties && plan.order.is_empty()
        && !matches!(&plan.body, Body::Select(s) if !s.query.order.is_empty())
    { return Err(ParseError::Definite(SqlError::Syntax)); }
    Ok(plan)
}
fn select_inner(
    tokens: &[Token],
    following: Option<&Token>,
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
) -> ParseResult<Plan> {
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
        return Err(SqlError::Unsupported.into());
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
        while i < tokens.len() {
            if tokens[i - 1] != Token::Symbol('.') && word(&tokens[i], "using") {
                let desc = match tokens.get(i + 1) {
                    Some(Token::Op(op)) if op == "<" || op == ">" => op == ">",
                    None => return Err(ParseError::Definite(clause_syntax(following))),
                    _ => return Err(ParseError::Definite(SqlError::UndefinedFunction)),
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
    // The legacy parser continues to own its established scans and ON joins.
    // The planner's general FROM tree is used only when that parse declines.
    let mut probe = parser(&tokens);
    match Query::parse_with_following(&mut probe, following) {
        Err(SqlError::Unsupported | SqlError::Syntax | SqlError::Reserved) => {
            if let Some(plan) = join::parse_select(&tokens, following, owner, depth, next)? {
                return Ok(plan);
            }
        }
        Err(error) => return Err(error.into()),
        Ok(_) => {}
    }
    // Normalizing ORDER BY USING can shift the token positions.
    if let Some(from) = top(&tokens)?
        .into_iter()
        .find(|i| word(&tokens[*i], "from"))
        && tokens.get(from + 1) == Some(&Token::Symbol('('))
    {
        let open = from + 1;
        let close = subquery_end(&tokens, open)?;
        let child = child_expression(
            &tokens[open + 1..close], tokens.get(close), owner, depth, next,
        )?;
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
                aliases.push(p.column_name()?);
                if !p.sym(',') {
                    break;
                }
            }
            if !p.sym(')') {
                return Err(SqlError::Syntax.into());
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
    select_query(&tokens, following, input, owner)
}

fn select_query(
    tokens: &[Token],
    following: Option<&Token>,
    input: Option<Input>,
    owner: &mut Parser,
) -> ParseResult<Plan> {
    let mut p = parser(tokens);
    let mut query = Query::parse_with_following(&mut p, following)?;
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
        applies: vec![],
        windows: vec![],
        query,
        parameters,
        input,
        from: None,
    }))))
}

pub(super) fn select(
    tokens: &[Token],
    following: Option<&Token>,
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
) -> ParseResult<Plan> {
    if paging::claims(tokens)? { paging::parse(tokens, following, owner, depth, next) }
    else { select_base(tokens, following, owner, depth, next, None, &mut []) }
}

pub(super) fn select_base(
    tokens: &[Token],
    following: Option<&Token>,
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
    grouping_sets: Option<&group_sets::GroupingSets>,
    distinct_on: &mut [Scalar],
) -> ParseResult<Plan> {
    if let Some(plan) = window::parse(
        tokens, following, owner, depth, next, grouping_sets, distinct_on,
    )? {
        return Ok(plan);
    }
    let (rewritten, applies) = subquery::rewrite(tokens, owner, depth, next)?;
    let mut plan = if !applies.is_empty() {
        // Rewriting must not let the FROM planner bypass a definite parser error.
        let mut probe = parser(&rewritten);
        match Query::parse_with_following(&mut probe, following) {
            Ok(_) | Err(SqlError::Unsupported | SqlError::Syntax | SqlError::Reserved) => {}
            Err(error) => return Err(error.into()),
        }
        if let Some(plan) = join::parse_select(&rewritten, following, owner, depth, next)? {
            plan
        } else {
            select_inner(&rewritten, following, owner, depth, next)?
        }
    } else {
        select_inner(&rewritten, following, owner, depth, next)?
    };
    if let Body::Select(s) = &mut plan.body {
        s.applies = applies;
        subquery::internalize(s)?;
    }
    Ok(plan)
}
