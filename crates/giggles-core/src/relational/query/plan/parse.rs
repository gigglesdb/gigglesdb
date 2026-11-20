use super::*;

pub(super) type ParseResult<T> = Result<T, ParseError>;

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
pub(super) fn top(tokens: &[Token]) -> R<Vec<usize>> {
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
            if depth > subplans::MAX_DEPTH {
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
    pub(in crate::relational) fn requires_grouping_sets(tokens: &[Token]) -> bool {
        let mut depth = 0usize;
        let mut group = false;
        let mut start = false;
        let mut at = 0;
        while at < tokens.len() {
            let token = &tokens[at];
            if *token == Token::Symbol(')') { depth = depth.saturating_sub(1); }
            let qualified = at > 0 && tokens[at - 1] == Token::Symbol('.');
            if depth == 0 {
                if !qualified && word(token, "group") && tokens.get(at + 1).is_some_and(|t| word(t, "by")) {
                    group = true;
                    start = true;
                    at += 2;
                    continue;
                }
                if group {
                    if !qualified && ["having", "order", "limit", "offset", "fetch", "for", "union", "intersect", "except"].iter().any(|w| word(token, w)) {
                        group = false;
                    } else if *token == Token::Symbol(',') { start = true; }
                    else {
                        if start && ((word(token, "rollup") || word(token, "cube")) && tokens.get(at + 1) == Some(&Token::Symbol('('))
                            || word(token, "grouping") && tokens.get(at + 1).is_some_and(|t| word(t, "sets"))) { return true; }
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
        if Self::requires_grouping_sets(tokens)
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
                        && [
                            "where", "group", "having", "order", "limit", "offset", "fetch", "for",
                        ]
                        .iter()
                        .any(|w| word(&tokens[*i], w))
                })
                .unwrap_or(tokens.len());
            let mut probe = vec![Token::Word("select".into()), Token::Symbol('*')];
            probe.extend_from_slice(&tokens[start..end]);
            Query::parse(&mut parser(&probe)).ok().map(|_| start + 1)
        });
        if tokens.iter().any(|t| word(t, "over") || word(t, "window"))
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
            if !qualified && word(t, "fetch") && tokens.get(i + 1).is_some_and(|t| word(t, "first") || word(t, "next")) { return true; }
            if (word(t, "limit") || word(t, "offset")) && Query::parse(&mut parser(tokens)).is_err() {
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
            } else if [
                "where", "group", "having", "order", "limit", "offset", "fetch", "for",
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
        let mut tokens = p.tokens[p.at..].to_vec();
        if tokens.last() == Some(&Token::Symbol(';')) {
            tokens.pop();
        }
        if tokens.len() > max_statement_tokens() {
            return Err(SqlError::Limit.into());
        }
        admit_subquery_depth(&tokens)?;
        // Admit parentheses before entering WITH or any recursive planner frame.
        top(&tokens)?;
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
pub(super) fn child_expression(
    tokens: &[Token],
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
) -> ParseResult<Plan> {
    if depth >= subplans::MAX_DEPTH {
        return Err(SqlError::Limit.into());
    }
    expression(tokens, owner, depth + 1, next)
}
pub(super) fn expression(
    tokens: &[Token],
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
        return cte::parse(tokens, owner, depth, next);
    }
    query_expression(tokens, owner, depth, next)
}

// Keep WITH dispatch out of this larger frame: nested WITH bodies must not
// reserve space for unrelated VALUES, pagination and SELECT parsing locals.
fn query_expression(
    tokens: &[Token],
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
            left: Box::new(child_expression(&body[..i], owner, depth, next)?),
            right: Box::new(child_expression(
                &body[i + 1 + usize::from(modifier)..],
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
        let child = child_expression(&body[1..body.len() - 1], owner, depth, next)?;
        if tail == tokens.len() {
            return Ok(child);
        }
        identity(child)?
    } else if body.first().is_some_and(|t| word(t, "values")) {
        values(body)?
    } else {
        return select(tokens, owner, depth, next);
    };
    pagination(plan, &tokens[tail..])
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

pub(super) fn pagination(mut plan: Plan, tokens: &[Token]) -> ParseResult<Plan> {
    let mut p = parser(tokens);
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
                plan.limit = Some(Scalar::parse_expr(&mut p, 0, false)?);
            }
        } else if !seen_offset && p.word("offset") {
            seen_offset = true;
            plan.offset = Some(Scalar::parse_expr(&mut p, 0, false)?);
            if !p.word("rows") {
                p.word("row");
            }
        } else if !seen_limit && p.word("fetch") {
            seen_limit = true;
            if !p.word("first") { p.need("next")?; }
            plan.limit = Some(if p.tokens.get(p.at).is_some_and(|t| word(t, "row") || word(t, "rows")) {
                Scalar::Literal(Value::Int(1))
            } else { Scalar::parse_expr(&mut p, 0, false)? });
            if !p.word("row") { p.need("rows")?; }
            if p.word("with") { p.need("ties")?; plan.decorations.with_ties = true; }
            else { p.need("only")?; }
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
        while i + 1 < tokens.len() {
            if word(&tokens[i], "using") {
                let desc = match &tokens[i + 1] {
                    Token::Op(op) if op == "<" || op == ">" => op == ">",
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
    match Query::parse(&mut probe) {
        Err(SqlError::Unsupported | SqlError::Syntax | SqlError::Reserved) => {
            if let Some(plan) = join::parse_select(&tokens, owner, depth, next)? {
                return Ok(plan);
            }
        }
        Err(error) => return Err(error.into()),
        Ok(_) => {}
    }
    if let Some(from) = positions
        .iter()
        .copied()
        .find(|i| word(&tokens[*i], "from"))
        && tokens.get(from + 1) == Some(&Token::Symbol('('))
    {
        let open = from + 1;
        let close = subquery_end(&tokens, open)?;
        let child = child_expression(&tokens[open + 1..close], owner, depth, next)?;
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
    select_query(&tokens, input, owner)
}

fn select_query(tokens: &[Token], input: Option<Input>, owner: &mut Parser) -> ParseResult<Plan> {
    let mut p = parser(tokens);
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
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
) -> ParseResult<Plan> {
    if paging::claims(tokens)? { paging::parse(tokens, owner, depth, next) }
    else { select_base(tokens, owner, depth, next) }
}

pub(super) fn select_base(
    tokens: &[Token], owner: &mut Parser, depth: usize, next: &mut usize,
) -> ParseResult<Plan> {
    if let Some(plan) = window::parse(tokens, owner, depth, next)? {
        return Ok(plan);
    }
    let (rewritten, applies) = subquery::rewrite(tokens, owner, depth, next)?;
    let mut plan = if !applies.is_empty() {
        // Rewriting must not let the FROM planner bypass a definite parser error.
        let mut probe = parser(&rewritten);
        match Query::parse(&mut probe) {
            Ok(_) | Err(SqlError::Unsupported | SqlError::Syntax | SqlError::Reserved) => {}
            Err(error) => return Err(error.into()),
        }
        if let Some(plan) = join::parse_select(&rewritten, owner, depth, next)? {
            plan
        } else {
            select_inner(&rewritten, owner, depth, next)?
        }
    } else {
        select_inner(&rewritten, owner, depth, next)?
    };
    if let Body::Select(s) = &mut plan.body {
        s.applies = applies;
        subquery::internalize(s)?;
    }
    Ok(plan)
}
