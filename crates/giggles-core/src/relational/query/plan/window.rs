//! Window calls are lowered to a materialized SELECT input and synthetic columns.
//! Ordinary expressions, grouping, coercion and aggregate transitions stay shared.
use super::*;
use exec::Relation;
use parse::{ParseResult, parser, top, word};

const INPUT: &str = "\0plan_window_input";
#[derive(Debug, Clone)]
pub(super) struct Call {
    name: String,
    output: String,
    args: Vec<Scalar>,
    filter: Option<Scalar>,
    spec: Spec,
}
#[derive(Debug, Clone, Default)]
struct Spec {
    partition: Vec<Scalar>,
    order: Vec<(Scalar, bool, bool)>,
    frame: Option<Frame>,
}
#[derive(Debug, Clone)]
struct Frame {
    mode: Mode,
    start: Edge,
    end: Edge,
    exclude: Exclude,
}
#[derive(Debug, Clone, Copy)]
enum Mode {
    Rows,
    Range,
    Groups,
}
#[derive(Debug, Clone)]
enum Edge {
    Unbounded(bool),
    Current,
    Offset(Scalar, bool),
}
#[derive(Debug, Clone, Copy)]
enum Exclude {
    None,
    Current,
    Group,
    Ties,
}

impl Call {
    pub(super) fn visit_scalars(&self, f: &mut impl FnMut(&Scalar)) {
        for s in self
            .args
            .iter()
            .chain(&self.filter)
            .chain(&self.spec.partition)
            .chain(self.spec.order.iter().map(|(s, _, _)| s))
        {
            f(s);
        }
        if let Some(frame) = &self.spec.frame {
            for edge in [&frame.start, &frame.end] {
                if let Edge::Offset(s, _) = edge {
                    f(s);
                }
            }
        }
    }
    pub(super) fn map_scalars(&mut self, f: &mut impl FnMut(&mut Scalar) -> R<()>) -> R<()> {
        for s in self
            .args
            .iter_mut()
            .chain(&mut self.filter)
            .chain(&mut self.spec.partition)
            .chain(self.spec.order.iter_mut().map(|(s, _, _)| s))
        {
            subquery::map_scalar(s, f)?;
        }
        if let Some(frame) = &mut self.spec.frame {
            for edge in [&mut frame.start, &mut frame.end] {
                if let Edge::Offset(s, _) = edge {
                    subquery::map_scalar(s, f)?;
                }
            }
        }
        Ok(())
    }
}

fn scalar(tokens: &[Token]) -> R<Scalar> {
    let mut tokens = tokens.to_vec();
    tokens.insert(0, Token::Word("select".into()));
    let q = Query::parse(&mut parser(&tokens))?;
    if !q.sources.is_empty()
        || !q.order.is_empty()
        || q.limit.is_some()
        || q.offset().is_some()
        || q.distinct()
        || !q.group().is_empty()
        || q.where_filter().is_some()
        || q.having().is_some()
    {
        return Err(SqlError::Unsupported);
    }
    match q.items.as_slice() {
        [(Item::Scalar(s), None)] => Ok(s.clone()),
        [(Item::Aggregate(a, arg), None)] => Ok(Scalar::Call(
            a.call_name(),
            vec![arg.clone().unwrap_or(Scalar::Literal(Value::Int(1)))],
        )),
        _ => Err(SqlError::Unsupported),
    }
}
fn column(name: &str) -> Scalar {
    Scalar::Column(ColumnRef {
        qualifier: None,
        name: name.into(),
    })
}
fn name(t: &Token) -> Option<&str> {
    match t {
        Token::Word(s) | Token::QuotedIdent(s) => Some(s),
        _ => None,
    }
}
fn close(tokens: &[Token], start: usize) -> R<usize> {
    subquery_end(tokens, start)
}
fn open(tokens: &[Token], end: usize) -> R<usize> {
    let mut depth = 0usize;
    for i in (0..=end).rev() {
        match tokens[i] {
            Token::Symbol(')') => depth += 1,
            Token::Symbol('(') => {
                depth = depth.checked_sub(1).ok_or(SqlError::Syntax)?;
                if depth == 0 {
                    return Ok(i);
                }
            }
            _ => {}
        }
        if depth > subplans::MAX_DEPTH {
            return Err(SqlError::Limit);
        }
    }
    Err(SqlError::Syntax)
}
fn edge(p: &mut Parser) -> R<Edge> {
    if p.word("unbounded") {
        let following = p.word("following");
        if !following {
            p.need("preceding")?;
        }
        Ok(Edge::Unbounded(following))
    } else if p.word("current") {
        p.need("row")?;
        Ok(Edge::Current)
    } else {
        let s = Scalar::parse_expr(p, 0, false)?;
        let following = p.word("following");
        if !following {
            p.need("preceding")?;
        }
        Ok(Edge::Offset(s, following))
    }
}
fn spec(tokens: &[Token], named: &[(String, Spec)]) -> ParseResult<Spec> {
    let mut p = parser(tokens);
    let inherited = p
        .tokens
        .first()
        .and_then(name)
        .filter(|n| !["partition", "order", "rows", "range", "groups"].contains(n))
        .map(str::to_owned);
    let mut s = if let Some(n) = &inherited {
        let s = named
            .iter()
            .find(|(name, _)| name == n)
            .ok_or(SqlError::UndefinedObject)?
            .1
            .clone();
        if s.frame.is_some() {
            return Err(SqlError::WindowingError.into());
        }
        p.at += 1;
        s
    } else {
        Spec::default()
    };
    if p.word("partition") {
        if inherited.is_some() {
            return Err(SqlError::WindowingError.into());
        }
        p.need("by")?;
        loop {
            s.partition.push(Scalar::parse_expr(&mut p, 0, false)?);
            if !p.sym(',') {
                break;
            }
        }
    }
    if p.word("order") {
        if !s.order.is_empty() {
            return Err(SqlError::WindowingError.into());
        }
        p.need("by")?;
        loop {
            let v = Scalar::parse_expr(&mut p, 0, false)?;
            let desc = p.word("desc");
            if !desc {
                p.word("asc");
            }
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
            s.order.push((v, desc, first));
            if !p.sym(',') {
                break;
            }
        }
    }
    let mode = if p.word("rows") {
        Some(Mode::Rows)
    } else if p.word("range") {
        Some(Mode::Range)
    } else if p.word("groups") {
        Some(Mode::Groups)
    } else {
        None
    };
    if let Some(mode) = mode {
        let between = p.word("between");
        let start = edge(&mut p)?;
        let end = if between {
            p.need("and")?;
            edge(&mut p)?
        } else {
            Edge::Current
        };
        if matches!(start, Edge::Unbounded(true))
            || matches!(end, Edge::Unbounded(false))
            || matches!(
                (&start, &end),
                (Edge::Current, Edge::Offset(_, false))
                    | (
                        Edge::Offset(_, true),
                        Edge::Current | Edge::Offset(_, false)
                    )
            )
            || matches!(mode, Mode::Groups) && s.order.is_empty()
            || matches!(mode, Mode::Range)
                && [&start, &end].iter().any(|e| matches!(e, Edge::Offset(..)))
                && s.order.len() != 1
        {
            return Err(SqlError::WindowingError.into());
        }
        let exclude = if p.word("exclude") {
            if p.word("current") {
                p.need("row")?;
                Exclude::Current
            } else if p.word("group") {
                Exclude::Group
            } else if p.word("ties") {
                Exclude::Ties
            } else {
                p.need("no")?;
                p.need("others")?;
                Exclude::None
            }
        } else {
            Exclude::None
        };
        s.frame = Some(Frame {
            mode,
            start,
            end,
            exclude,
        });
    }
    if p.at != p.tokens.len() {
        return Err(ParseError::Decline);
    }
    Ok(s)
}

pub(super) fn parse(
    tokens: &[Token],
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
) -> ParseResult<Option<Plan>> {
    let positions = top(tokens)?;
    let window = positions
        .iter()
        .copied()
        .find(|i| word(&tokens[*i], "window"));
    let mut named = vec![];
    let mut rewritten = tokens.to_vec();
    if let Some(start) = window {
        let end = positions
            .iter()
            .copied()
            .find(|i| {
                *i > start
                    && ["order", "limit", "offset", "fetch", "for"]
                        .iter()
                        .any(|w| word(&tokens[*i], w))
            })
            .unwrap_or(tokens.len());
        let mut p = parser(&tokens[start + 1..end]);
        loop {
            let n = p.name()?;
            if named.iter().any(|(old, _)| *old == n) {
                return Err(SqlError::WindowingError.into());
            }
            p.need("as")?;
            if !p.sym('(') {
                return Err(ParseError::Decline);
            }
            let end = close(&p.tokens, p.at - 1)?;
            let s = spec(&p.tokens[p.at..end], &named)?;
            p.at = end + 1;
            named.push((n, s));
            if !p.sym(',') {
                break;
            }
        }
        if p.at != p.tokens.len() {
            return Err(ParseError::Decline);
        }
        rewritten.drain(start..end);
    }
    let mut calls = vec![];
    let mut i = 0;
    while i < rewritten.len() {
        if is_subquery_open(&rewritten, i) {
            i = close(&rewritten, i)? + 1;
            continue;
        }
        if !word(&rewritten[i], "over") {
            i += 1;
            continue;
        }
        // A window in an argument/specification is illegal, including nested scalar wrappers.
        let mut end_call = i.checked_sub(1).ok_or(SqlError::Syntax)?;
        let mut filter = None;
        if rewritten.get(end_call) == Some(&Token::Symbol(')')) {
            let begin = open(&rewritten, end_call)?;
            if begin > 0 && word(&rewritten[begin - 1], "filter") {
                let mut p = parser(&rewritten[begin + 1..end_call]);
                p.need("where")?;
                let predicate = Filter::parse(&mut p, 0, 0)?;
                if p.at != p.tokens.len() {
                    return Err(ParseError::Decline);
                }
                filter = Some(Scalar::Predicate(Box::new(predicate)));
                end_call = begin.checked_sub(2).ok_or(SqlError::Syntax)?;
            }
        }
        if rewritten.get(end_call) != Some(&Token::Symbol(')')) {
            return Err(ParseError::Decline);
        }
        let begin = open(&rewritten, end_call)?;
        let mut start = begin.checked_sub(1).ok_or(SqlError::Syntax)?;
        let n = name(&rewritten[start]).ok_or(SqlError::Syntax)?.to_owned();
        if start >= 2 && rewritten[start - 1] == Token::Symbol('.') {
            if name(&rewritten[start - 2]) != Some("pg_catalog") {
                return Err(ParseError::Decline);
            }
            start -= 2;
        }
        if rewritten[begin + 1..end_call]
            .iter()
            .any(|t| word(t, "over"))
        {
            return Err(SqlError::WindowingError.into());
        }
        let clause = top(&rewritten)?
            .into_iter()
            .filter(|p| *p < start)
            .rev()
            .find_map(|p| {
                name(&rewritten[p])
                    .filter(|n| ["select", "where", "group", "having", "order", "from"].contains(n))
            });
        if matches!(clause, Some("where" | "group" | "having" | "from")) {
            return Err(SqlError::WindowingError.into());
        }
        let args_tokens = &rewritten[begin + 1..end_call];
        if n == "count" && args_tokens.is_empty() {
            return Err(SqlError::WrongObjectType.into());
        }
        if Aggregate::from_name(&n).is_some()
            && (args_tokens.first().is_some_and(|t| word(t, "distinct"))
                || top(args_tokens)?
                    .iter()
                    .any(|i| word(&args_tokens[*i], "order")))
        {
            return Err(ParseError::Definite(SqlError::Unsupported));
        }
        let mut args = vec![];
        if !args_tokens.is_empty() && args_tokens != [Token::Symbol('*')] {
            let mut start = 0;
            for end in top(args_tokens)?
                .into_iter()
                .filter(|p| args_tokens[*p] == Token::Symbol(','))
                .chain(std::iter::once(args_tokens.len()))
            {
                args.push(scalar(&args_tokens[start..end])?);
                start = end + 1;
            }
        } else if args_tokens == [Token::Symbol('*')] && n != "count" {
            return Err(ParseError::Decline);
        }
        if !matches!(
            n.as_str(),
            "sum"
                | "avg"
                | "count"
                | "min"
                | "max"
                | "row_number"
                | "rank"
                | "dense_rank"
                | "percent_rank"
                | "cume_dist"
                | "ntile"
                | "first_value"
                | "last_value"
                | "nth_value"
                | "lag"
                | "lead"
        ) {
            return Err(ParseError::Decline);
        }
        let valid = match n.as_str() {
            "count" => args.len() <= 1,
            "row_number" | "rank" | "dense_rank" | "percent_rank" | "cume_dist" => args.is_empty(),
            "nth_value" => args.len() == 2,
            "lag" | "lead" => (1..=3).contains(&args.len()),
            _ => args.len() == 1,
        };
        if !valid {
            return Err(ParseError::Decline);
        }
        if filter.is_some() && Aggregate::from_name(&n).is_none() {
            return Err(ParseError::Decline);
        }
        let (s, end) = if rewritten.get(i + 1) == Some(&Token::Symbol('(')) {
            let end = close(&rewritten, i + 1)?;
            if rewritten[i + 2..end].iter().any(|t| word(t, "over")) {
                return Err(SqlError::WindowingError.into());
            }
            (spec(&rewritten[i + 2..end], &named)?, end + 1)
        } else {
            let n = rewritten
                .get(i + 1)
                .and_then(name)
                .ok_or(SqlError::Syntax)?;
            (
                named
                    .iter()
                    .find(|(old, _)| old == n)
                    .ok_or(SqlError::UndefinedObject)?
                    .1
                    .clone(),
                i + 2,
            )
        };
        let output = loop {
            let candidate = format!("plan_window_{}", *next);
            *next += 1;
            if !tokens.iter().any(|t| name(t) == Some(candidate.as_str())) {
                break candidate;
            }
        };
        calls.push(Call {
            name: n,
            output: output.clone(),
            args,
            filter,
            spec: s,
        });
        rewritten.splice(start..end, [Token::QuotedIdent(output)]);
        i = start + 1;
    }
    if calls.is_empty() && window.is_none() {
        return Ok(None);
    }
    if depth >= subplans::MAX_DEPTH {
        return Err(SqlError::Limit.into());
    }
    if calls.is_empty() {
        return parse::select(&rewritten, owner, depth + 1, next).map(Some);
    }
    let tail = top(&rewritten)?
        .into_iter()
        .find(|i| word(&rewritten[*i], "limit") || word(&rewritten[*i], "offset"))
        .unwrap_or(rewritten.len());
    let page = rewritten[tail..].to_vec();
    rewritten.truncate(tail);
    let mut base = parse::select(&rewritten, owner, depth + 1, next)?;
    let Body::Select(s) = &mut base.body else {
        return Err(ParseError::Decline);
    };
    // Shared identifier admission forbids NUL. Internalize placeholders only
    // after the ordinary parser has accepted their collision-free spellings.
    let names = calls
        .iter()
        .map(|call| (call.output.clone(), format!("\0{}", call.output)))
        .collect::<Vec<_>>();
    let mut rename = |v: &mut Scalar| -> R<()> {
        if let Scalar::Column(c) = v
            && let Some((_, name)) = names.iter().find(|(old, _)| *old == c.name)
        {
            c.name = name.clone();
        }
        Ok(())
    };
    subquery::map_query(&mut s.query, &mut rename)?;
    for call in &mut calls {
        call.map_scalars(&mut rename)?;
        call.output.insert(0, '\0');
    }
    let original = s.query.items.clone();
    if let Some(Filter::Clauses(c)) = &mut s.query.filter {
        for g in &mut c.group {
            let selected = match g {
                Scalar::Literal(Value::Int(n)) if *n > 0 => original.get(*n as usize - 1),
                _ => None,
            };
            if let Some((Item::Scalar(v), _)) = selected {
                if contains_window(v, &calls)? {
                    return Err(SqlError::WindowingError.into());
                }
                *g = v.clone();
            }
        }
    }
    let mut q = s.query.clone();
    let mut items = vec![];
    for (item, alias) in &mut q.items {
        match item {
            Item::Scalar(v) => {
                if alias.is_none() {
                    *alias = Some(output_name(v, &calls));
                }
                lower(v, &mut items, &calls, 0)?;
            }
            Item::Aggregate(a, arg) => {
                let v = Scalar::Call(
                    a.call_name(),
                    vec![arg.clone().unwrap_or(Scalar::Literal(Value::Int(1)))],
                );
                let mut v = v;
                lower(&mut v, &mut items, &calls, 0)?;
                if alias.is_none() {
                    *alias = Some(a.base().name().into());
                }
                *item = Item::Scalar(v);
            }
            Item::Star | Item::QualifiedStar { .. } => items.push((item.clone(), None)),
        }
    }
    let outputs = q.items.clone();
    for (v, _, _) in &mut q.order {
        if matches!(v, Scalar::Literal(Value::Int(_))) {
            continue;
        }
        if let Scalar::Column(c) = v
            && c.qualifier.is_none()
            && outputs
                .iter()
                .any(|(_, alias)| alias.as_ref() == Some(&c.name))
        {
            continue;
        }
        lower(v, &mut items, &calls, 0)?;
    }
    for call in &mut calls {
        for v in call
            .args
            .iter_mut()
            .chain(&mut call.filter)
            .chain(&mut call.spec.partition)
            .chain(call.spec.order.iter_mut().map(|(s, _, _)| s))
        {
            if contains_window(v, &[])? {
                return Err(SqlError::WindowingError.into());
            }
            lower(v, &mut items, &[], 0)?;
        }
    }
    // The input retains WHERE/GROUP/HAVING; final projection retains DISTINCT and pagination.
    // GROUP BY retains the shared binder's input-column-before-output-alias rule.
    for (item, alias) in &original {
        if let Some(alias) = alias
            && s.query.group().iter().any(
                |g| matches!(g, Scalar::Column(c) if c.qualifier.is_none() && c.name == *alias),
            )
        {
            items.push((item.clone(), Some(alias.clone())));
        }
    }
    s.query.items = items;
    s.query.order.clear();
    s.query.limit = None;
    if let Some(Filter::Clauses(c)) = &mut s.query.filter {
        c.distinct = false;
        c.offset = None;
    }
    if s.query.items.is_empty() {
        s.query.items.push((
            Item::Scalar(Scalar::Literal(Value::Int(1))),
            Some("\0window_dummy".into()),
        ));
    }
    s.parameters = s.query.used_parameters();
    let mut parameters = q.used_parameters();
    for call in &calls {
        call.clone().map_scalars(&mut |v| {
            if let Scalar::Param(n) = v {
                parameters.insert(*n);
            }
            Ok(())
        })?;
    }
    q.sources = vec![Source {
        name: INPUT.into(),
        alias: "plan_window_input".into(),
    }];
    q.joins.clear();
    q.predicates.clear();
    let mut clauses = QueryClauses {
        distinct: q.distinct(),
        offset: q.offset().cloned(),
        ..Default::default()
    };
    if let Some(c) = q.clauses() {
        clauses.page_casts = c.page_casts.clone();
    }
    q.filter = Some(Filter::Clauses(Box::new(clauses)));
    let plan = Plan::new(Body::Select(Box::new(Select {
        query: q,
        parameters,
        applies: vec![],
        windows: calls,
        from: None,
        input: Some(Input {
            query: Box::new(base),
            name: INPUT.into(),
            aliases: vec![],
        }),
    })));
    Ok(Some(parse::pagination(plan, &page)?))
}
fn contains_window(s: &Scalar, calls: &[Call]) -> R<bool> {
    let mut yes = false;
    subquery::map_scalar(&mut s.clone(), &mut |v| {
        if let Scalar::Column(c) = v {
            yes |= calls.iter().any(|w| w.output == c.name) || c.name.starts_with("\0plan_window_");
        }
        Ok(())
    })?;
    Ok(yes)
}
fn output_name(mut s: &Scalar, calls: &[Call]) -> String {
    while let Scalar::Cast(inner, _) | Scalar::Domain(inner, ..) = s {
        s = inner;
    }
    match s {
        Scalar::Column(c) => calls
            .iter()
            .find(|w| w.output == c.name)
            .map_or_else(|| c.name.clone(), |w| w.name.clone()),
        Scalar::Call(n, _) => n.clone(),
        _ => "?column?".into(),
    }
}
fn hoist(v: &mut Scalar, items: &mut Vec<(Item, Option<String>)>) {
    let name = format!("\0window_arg_{}", items.len());
    items.push((Item::Scalar(v.clone()), Some(name.clone())));
    *v = column(&name);
}
fn lower(
    v: &mut Scalar,
    items: &mut Vec<(Item, Option<String>)>,
    calls: &[Call],
    depth: usize,
) -> R<()> {
    if depth >= subplans::MAX_DEPTH {
        return Err(SqlError::Limit);
    }
    let dependency = match v {
        Scalar::Column(c) => !calls.iter().any(|call| call.output == c.name),
        Scalar::Call(n, _) if Aggregate::from_name(n).is_some() || n == aggregate_filter::CALL => {
            if contains_window(v, calls)? {
                return Err(SqlError::GroupingError);
            }
            true
        }
        _ => false,
    };
    if dependency {
        hoist(v, items);
        return Ok(());
    }
    match v {
        Scalar::Column(_) => {}
        Scalar::Unary(_, v) | Scalar::Cast(v, _) | Scalar::Domain(v, ..) => {
            lower(v, items, calls, depth + 1)?
        }
        Scalar::Binary(_, a, b) => {
            lower(a, items, calls, depth + 1)?;
            lower(b, items, calls, depth + 1)?;
        }
        Scalar::Call(n, args) => {
            if Aggregate::from_name(n).is_some() {
                return Err(SqlError::GroupingError);
            }
            for v in args {
                lower(v, items, calls, depth + 1)?;
            }
        }
        Scalar::SubstringOrder(args) => {
            for v in args {
                lower(v, items, calls, depth + 1)?;
            }
        }
        Scalar::Predicate(f) => lower_filter(f, items, calls, depth + 1)?,
        Scalar::Case(c) => {
            for (f, v) in &mut c.arms {
                lower_filter(f, items, calls, depth + 1)?;
                lower(v, items, calls, depth + 1)?;
            }
            if let Some(v) = &mut c.otherwise {
                lower(v, items, calls, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}
fn lower_filter(
    f: &mut Filter,
    items: &mut Vec<(Item, Option<String>)>,
    calls: &[Call],
    depth: usize,
) -> R<()> {
    if depth >= subplans::MAX_DEPTH {
        return Err(SqlError::Limit);
    }
    match f {
        Filter::Scalar(v) | Filter::Null(v, _) => lower(v, items, calls, depth + 1)?,
        Filter::Compare(a, _, b) => {
            lower(a, items, calls, depth + 1)?;
            lower(b, items, calls, depth + 1)?;
        }
        Filter::Not(f) | Filter::Is(f, ..) => lower_filter(f, items, calls, depth + 1)?,
        Filter::Logic(a, _, b) => {
            lower_filter(a, items, calls, depth + 1)?;
            lower_filter(b, items, calls, depth + 1)?;
        }
        Filter::Like {
            value,
            pattern,
            escape,
            ..
        } => {
            lower(value, items, calls, depth + 1)?;
            lower(pattern, items, calls, depth + 1)?;
            if let Some(v) = escape {
                lower(v, items, calls, depth + 1)?;
            }
        }
        _ => return Err(SqlError::Unsupported),
    }
    Ok(())
}

fn bind_expression(
    s: &Scalar,
    table: &TableDef,
    parameters: &mut [Option<DataType>],
    clock: Option<&crate::relational::clock::ExecutionClock>,
) -> R<BoundScalar> {
    let resolve = |c: &ColumnRef, _| bind::output_column(c, table);
    s.infer(None, parameters, &resolve, &[table])?;
    let mut b = s.bind(&resolve, &[table], parameters)?;
    b.attach_clock(clock);
    Ok(b)
}
struct BoundCall {
    args: Vec<BoundScalar>,
    filter: Option<BoundScalar>,
    partition: Vec<BoundScalar>,
    order: Vec<(BoundScalar, bool, bool)>,
    aggregate: Option<BoundItem>,
    column: ColumnDef,
    collation: Option<BoundScalar>,
}
fn bind_call(
    call: &Call,
    columns: &[ColumnDef],
    collations: &[Option<BoundScalar>],
    parameters: &mut [Option<DataType>],
    clock: Option<&crate::relational::clock::ExecutionClock>,
) -> R<BoundCall> {
    let mut columns = columns.to_vec();
    for (i, c) in columns.iter_mut().enumerate() {
        c.id = i as u64 + 1;
    }
    let _collations = functions::collation::ColumnScope::enter(&columns, collations)?;
    let table = TableDef {
        id: 0,
        name: INPUT.into(),
        columns,
        primary_key: vec![],
    };
    for (i, s) in call.args.iter().enumerate() {
        let expected = if (call.name == "ntile" && i == 0)
            || (matches!(call.name.as_str(), "lag" | "lead" | "nth_value") && i == 1)
        {
            Some(&DataType::Integer)
        } else {
            None
        };
        s.infer(
            expected,
            parameters,
            &|c, _| bind::output_column(c, &table),
            &[&table],
        )?;
    }
    if matches!(call.name.as_str(), "lag" | "lead") && call.args.len() == 3 {
        let candidates = [0, 2]
            .into_iter()
            .filter(|i| !call.args[*i].aggregate_unknown(parameters))
            .map(|i| {
                call.args[i]
                    .bind(
                        &|c, _| bind::output_column(c, &table),
                        &[&table],
                        parameters,
                    )
                    .map(|b| b.ty)
            })
            .collect::<R<Vec<_>>>()?;
        let ty = bind::common(candidates.into_iter())?;
        for i in [0, 2] {
            call.args[i].infer(
                Some(&ty),
                parameters,
                &|c, _| bind::output_column(c, &table),
                &[&table],
            )?;
        }
    }
    let (partition, order, mut args, filter) = {
        let mut bind_scalar = |s: &Scalar| bind_expression(s, &table, parameters, clock);
        let partition = call
            .spec
            .partition
            .iter()
            .map(&mut bind_scalar)
            .collect::<R<Vec<_>>>()?;
        let order = call
            .spec
            .order
            .iter()
            .map(|(s, d, f)| Ok((bind_scalar(s)?, *d, *f)))
            .collect::<R<Vec<_>>>()?;
        let args = call
            .args
            .iter()
            .map(&mut bind_scalar)
            .collect::<R<Vec<_>>>()?;
        let filter = call.filter.as_ref().map(&mut bind_scalar).transpose()?;
        let mut args = args;
        for (i, s) in call.args.iter().enumerate() {
            if (call.name == "ntile" && i == 0)
                || (matches!(call.name.as_str(), "lag" | "lead" | "nth_value") && i == 1)
            {
                args[i] = bind_scalar(&Scalar::Cast(Box::new(s.clone()), DataType::Integer))?;
            }
        }
        (partition, order, args, filter)
    };
    for scalar in partition.iter().chain(order.iter().map(|(s, _, _)| s)) {
        scalar.require_collation()?;
    }
    if let Some(frame) = &call.spec.frame {
        let range = matches!(frame.mode, Mode::Range);
        for edge in [&frame.start, &frame.end] {
            if let Edge::Offset(s, _) = edge {
                let ty = offset_type(
                    s,
                    if range { Mode::Range } else { frame.mode },
                    &order,
                    parameters,
                )?;
                let mut b = Scalar::Cast(Box::new(s.clone()), ty).bind(
                    &|_, _| Err(SqlError::UnknownColumn),
                    &[],
                    parameters,
                )?;
                b.attach_clock(clock);
                b.plan_constant(&mut 0)?;
            }
        }
    }
    let (ty, aggregate) = if let Some(a) = Aggregate::from_name(&call.name) {
        let mut q = Query::parse(&mut parser(&lex("SELECT 1 FROM window_input")?))?;
        q.sources[0].name = INPUT.into();
        q.items = vec![(Item::Aggregate(a, call.args.first().cloned()), None)];
        let mut state = Relational::default();
        state.tables.insert(INPUT.into(), table.clone());
        let mut bound = q
            .bind_projection_seeded(&state, parameters.to_vec())?
            .with_clock(clock);
        (
            bound.columns[0].data_type.clone(),
            Some(bound.items.remove(0)),
        )
    } else if matches!(call.name.as_str(), "row_number" | "rank" | "dense_rank") {
        (DataType::BigInt, None)
    } else if call.name == "ntile" {
        (DataType::Integer, None)
    } else if matches!(call.name.as_str(), "percent_rank" | "cume_dist") {
        (DataType::DoublePrecision, None)
    } else {
        let ty = if matches!(call.name.as_str(), "lag" | "lead") && args.len() == 3 {
            bind::common(
                [0, 2]
                    .into_iter()
                    .filter(|i| !call.args[*i].aggregate_unknown(parameters))
                    .map(|i| args[i].ty.clone()),
            )?
        } else {
            args[0].ty.clone()
        };
        for i in [0, 2] {
            if let Some(s) = call.args.get(i) {
                args[i] = bind_expression(
                    &Scalar::Cast(Box::new(s.clone()), ty.clone()),
                    &table,
                    parameters,
                    clock,
                )?;
            }
        }
        (ty, None)
    };
    let collation = if matches!(ty, DataType::Text | DataType::Varchar | DataType::Bpchar) {
        // Value windows inherit their value arguments (including lag/lead's default).
        let values = if matches!(call.name.as_str(), "lag" | "lead") && call.args.len() == 3 {
            vec![call.args[0].clone(), call.args[2].clone()]
        } else {
            call.args.first().cloned().into_iter().collect()
        };
        Some(bind_expression(
            &Scalar::Call("coalesce".into(), values),
            &table,
            parameters,
            clock,
        )?)
    } else {
        None
    };
    Ok(BoundCall {
        collation,
        args,
        filter,
        partition,
        order,
        aggregate,
        column: ColumnDef {
            id: 0,
            name: call.output.clone(),
            data_type: ty,
            not_null: matches!(
                call.name.as_str(),
                "count" | "row_number" | "rank" | "dense_rank" | "percent_rank" | "cume_dist"
            ),
        },
    })
}

pub(super) fn columns(
    calls: &[Call],
    meta: &mut bind::Meta,
    parameters: &mut [Option<DataType>],
) -> R<()> {
    let columns = meta.columns.clone();
    let bound_calls = calls
        .iter()
        .map(|call| bind_call(call, &columns, &meta.collations, parameters, None))
        .collect::<R<Vec<_>>>()?;
    meta.collations.resize_with(columns.len(), || None);
    for bound in bound_calls {
        meta.columns.push(bound.column);
        meta.collations.push(bound.collation);
        meta.typmods.push(-1);
    }
    Ok(())
}
pub(super) fn prepare(select: &Select, state: &Relational) -> R<Query> {
    if select.windows.is_empty() {
        return Ok(select.query.clone());
    }
    let columns = &state.tables[&select.input.as_ref().unwrap().name].columns;
    let query = projection(&select.query, columns);
    let visible = columns
        .iter()
        .enumerate()
        .map(|(i, c)| (c.name.clone(), i))
        .collect::<Vec<_>>();
    let namespace = join::Namespace {
        visible: visible.clone(),
        hidden: vec![],
        qualified: vec![(query.sources[0].alias.clone(), visible)],
    };
    join::prepare(
        &query,
        &join::Shape {
            collations: vec![],
            columns: columns.clone(),
            typmods: vec![-1; columns.len()],
            namespace,
        },
    )
}
pub(super) fn projection(query: &Query, columns: &[ColumnDef]) -> Query {
    let mut q = query.clone();
    if q.items
        .iter()
        .any(|(i, _)| matches!(i, Item::Star | Item::QualifiedStar { .. }))
    {
        q.items = q
            .items
            .into_iter()
            .flat_map(|(i, a)| {
                if matches!(i, Item::Star | Item::QualifiedStar { .. }) {
                    columns
                        .iter()
                        .filter(|c| !c.name.starts_with('\0'))
                        .map(|c| (Item::Scalar(column(&c.name)), Some(c.name.clone())))
                        .collect()
                } else {
                    vec![(i, a)]
                }
            })
            .collect();
    }
    q
}
fn compare_keys(a: &[Value], b: &[Value], order: &[(BoundScalar, bool, bool)]) -> Ordering {
    a.iter()
        .zip(b)
        .zip(order)
        .map(
            |((a, b), (scalar, desc, first))| match (a == &Value::Null, b == &Value::Null) {
                (true, true) => Ordering::Equal,
                (true, false) => {
                    if *first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (false, true) => {
                    if *first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                _ => {
                    let c = scalar.compare_values(a, b);
                    if *desc { c.reverse() } else { c }
                }
            },
        )
        .find(|c| *c != Ordering::Equal)
        .unwrap_or(Ordering::Equal)
}
fn eval(b: &BoundScalar, row: &SqlRow, params: &[Value]) -> R<Value> {
    b.eval(
        &Tuple {
            values: vec![row.values.clone()],
            version: row.version,
        },
        params,
        &mut 0,
    )
}
fn offset_type(
    s: &Scalar,
    mode: Mode,
    order: &[(BoundScalar, bool, bool)],
    parameters: &mut [Option<DataType>],
) -> R<DataType> {
    let range = matches!(mode, Mode::Range);
    let expected = if range {
        match order[0].0.ty {
            DataType::SmallInt | DataType::Integer | DataType::BigInt | DataType::Numeric => {
                order[0].0.ty.clone()
            }
            DataType::Real | DataType::DoublePrecision => DataType::DoublePrecision,
            DataType::Date
            | DataType::Time
            | DataType::TimeTz
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Interval => DataType::Interval,
            _ => return Err(SqlError::Unsupported),
        }
    } else {
        DataType::BigInt
    };
    s.infer(
        Some(&expected),
        parameters,
        &|_, _| Err(SqlError::UnknownColumn),
        &[],
    )?;
    if range && !s.aggregate_unknown(parameters) {
        let actual = s
            .bind(&|_, _| Err(SqlError::UnknownColumn), &[], parameters)?
            .ty;
        let numeric = integer_type(&actual)
            || matches!(
                actual,
                DataType::Numeric | DataType::Real | DataType::DoublePrecision
            );
        let supported = match order[0].0.ty {
            DataType::SmallInt | DataType::Integer | DataType::BigInt => integer_type(&actual),
            DataType::Numeric => integer_type(&actual) || actual == DataType::Numeric,
            DataType::Real | DataType::DoublePrecision => numeric,
            _ => actual == DataType::Interval,
        };
        if !supported {
            return Err(SqlError::Unsupported);
        }
        if matches!(order[0].0.ty, DataType::SmallInt | DataType::Integer) {
            return Ok(actual);
        }
    }
    Ok(expected)
}
fn offset(
    s: &Scalar,
    mode: Mode,
    order: &[(BoundScalar, bool, bool)],
    params: &[Value],
    parameters: &[Option<DataType>],
    clock: Option<&crate::relational::clock::ExecutionClock>,
) -> R<Value> {
    let ty = offset_type(s, mode, order, &mut parameters.to_vec())?;
    let mut b = Scalar::Cast(Box::new(s.clone()), ty).bind(
        &|_, _| Err(SqlError::UnknownColumn),
        &[],
        parameters,
    )?;
    b.attach_clock(clock);
    let v = b.eval(
        &Tuple {
            values: vec![],
            version: 0,
        },
        params,
        &mut 0,
    )?;
    let negative = match &v {
        Value::Null => return Err(SqlError::NullValueNotAllowed),
        Value::Int(n) => *n < 0,
        Value::Numeric(n) => {
            n < &Numeric::parse("0").unwrap() || n == &Numeric::parse("NaN").unwrap()
        }
        Value::Float4(n) => n.value() < 0.0 || n.value().is_nan(),
        Value::Float8(n) => n.value() < 0.0 || n.value().is_nan(),
        Value::Interval(n) => n < &Interval::new(0, 0, 0),
        _ => return Err(SqlError::Type),
    };
    if negative {
        return Err(SqlError::InvalidFrameOffset);
    }
    Ok(v)
}
#[derive(Clone)]
struct Keyed {
    index: usize,
    partition: Vec<Value>,
    order: Vec<Value>,
}
fn is_nan(v: &Value) -> bool {
    match v {
        Value::Numeric(n) => *n == Numeric::parse("NaN").unwrap(),
        Value::Float4(n) => n.value().is_nan(),
        Value::Float8(n) => n.value().is_nan(),
        _ => false,
    }
}
#[expect(
    clippy::too_many_arguments,
    reason = "window frame and execution context"
)]
fn frame_edge(
    edge: &Edge,
    offset: Option<&Value>,
    mode: Mode,
    start: bool,
    pos: usize,
    keys: &[Keyed],
    groups: &[usize],
    group: usize,
    order: &[(BoundScalar, bool, bool)],
    clock: Option<&crate::relational::clock::ExecutionClock>,
    execution: &Execution<'_>,
) -> R<usize> {
    let len = keys.len();
    match edge {
        Edge::Unbounded(following) => Ok(if *following { len } else { 0 }),
        Edge::Current => Ok(if matches!(mode, Mode::Rows) {
            pos + usize::from(!start)
        } else if start {
            groups[group]
        } else {
            groups[group + 1]
        }),
        Edge::Offset(_, following) => {
            let v = offset.ok_or(SqlError::Type)?;
            if !matches!(mode, Mode::Range) {
                let Value::Int(n) = v else {
                    return Err(SqlError::Type);
                };
                let current = if matches!(mode, Mode::Groups) {
                    group
                } else {
                    pos
                };
                let target = current as i128
                    + if *following {
                        i128::from(*n)
                    } else {
                        -i128::from(*n)
                    }
                    + i128::from(!start);
                if matches!(mode, Mode::Rows) {
                    return Ok(target.clamp(0, len as i128) as usize);
                }
                return Ok(groups[target.clamp(0, (groups.len() - 1) as i128) as usize]);
            }
            let current = &keys[pos].order[0];
            if *current == Value::Null {
                return Ok(if start {
                    groups[group]
                } else {
                    groups[group + 1]
                });
            }
            let numeric = matches!(current, Value::Int(_) | Value::Numeric(_));
            let ty = if numeric {
                DataType::Numeric
            } else {
                order[0].0.ty.clone()
            };
            let off_ty = if numeric {
                DataType::Numeric
            } else if matches!(v, Value::Interval(_)) {
                DataType::Interval
            } else {
                ty.clone()
            };
            let op = if *following ^ order[0].1 {
                functions::BinaryOp::Add
            } else {
                functions::BinaryOp::Sub
            };
            let expr = Scalar::Binary(
                op,
                Box::new(Scalar::Cast(
                    Box::new(Scalar::Literal(current.clone())),
                    ty.clone(),
                )),
                Box::new(Scalar::Cast(Box::new(Scalar::Literal(v.clone())), off_ty)),
            );
            let mut bound = expr.bind(&|_, _| Err(SqlError::UnknownColumn), &[], &[])?;
            bound.attach_clock(clock);
            // in_range permits an infinite floating boundary when finite
            // addition overflows; ordinary SQL arithmetic instead raises 22003.
            let add = matches!(op, functions::BinaryOp::Add);
            let mut target = match (current, v) {
                (Value::Float4(a), Value::Float4(b)) => Value::Float4(Float4::from_bits(
                    if add {
                        a.value() + b.value()
                    } else {
                        a.value() - b.value()
                    }
                    .to_bits(),
                )),
                (Value::Float4(a), Value::Float8(b)) => Value::Float8(Float8::from_bits(
                    if add {
                        f64::from(a.value()) + b.value()
                    } else {
                        f64::from(a.value()) - b.value()
                    }
                    .to_bits(),
                )),
                (Value::Float8(a), Value::Float8(b)) => Value::Float8(Float8::from_bits(
                    if add {
                        a.value() + b.value()
                    } else {
                        a.value() - b.value()
                    }
                    .to_bits(),
                )),
                _ => bound.eval(
                    &Tuple {
                        values: vec![],
                        version: 0,
                    },
                    &[],
                    &mut 0,
                )?,
            };
            let target_ty = match &target {
                Value::Float8(_) => DataType::DoublePrecision,
                Value::Float4(_) => DataType::Real,
                _ => bound.ty,
            };
            // Infinite offsets cover all finite and infinite keys in their
            // direction. Infinity minus Infinity must not turn a bound into NaN.
            if is_nan(&target) && !is_nan(current) {
                target = functions::coerce_clocked(
                    Value::Text(
                        if matches!(op, functions::BinaryOp::Add) {
                            "Infinity"
                        } else {
                            "-Infinity"
                        }
                        .into(),
                    ),
                    &target_ty,
                    clock,
                )?;
            }
            for (i, k) in keys.iter().enumerate() {
                execution.comparison()?;
                let candidate = &k.order[0];
                let comparison = if *candidate == Value::Null {
                    if order[0].2 {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                } else {
                    let candidate =
                        functions::coerce_clocked(candidate.clone(), &target_ty, clock)?;
                    let c = cmp(&candidate, &target);
                    if order[0].1 { c.reverse() } else { c }
                };
                if if start {
                    comparison != Ordering::Less
                } else {
                    comparison == Ordering::Greater
                } {
                    return Ok(i);
                }
            }
            Ok(len)
        }
    }
}

pub(super) fn run(
    calls: &[Call],
    rows: &mut Relation<'_>,
    collations: &[Option<BoundScalar>],
    params: &[Value],
    parameters: &[Option<DataType>],
    clock: Option<&crate::relational::clock::ExecutionClock>,
    execution: &Execution<'_>,
) -> R<()> {
    if calls.is_empty() {
        return Ok(());
    }
    let input_columns = rows.columns.clone();
    for call in calls {
        let bound = bind_call(
            call,
            &input_columns,
            collations,
            &mut parameters.to_vec(),
            clock,
        )?;
        let frame = call.spec.frame.clone().unwrap_or(Frame {
            mode: Mode::Range,
            start: Edge::Unbounded(false),
            end: if call.spec.order.is_empty() {
                Edge::Unbounded(true)
            } else {
                Edge::Current
            },
            exclude: Exclude::None,
        });
        let mut offsets = vec![];
        for e in [&frame.start, &frame.end] {
            offsets.push(
                if let Edge::Offset(s, _) = e
                    && !matches!(
                        call.name.as_str(),
                        "row_number"
                            | "rank"
                            | "dense_rank"
                            | "percent_rank"
                            | "cume_dist"
                            | "ntile"
                            | "lag"
                            | "lead"
                    )
                {
                    Some(offset(
                        s,
                        frame.mode,
                        &bound.order,
                        params,
                        parameters,
                        clock,
                    )?)
                } else {
                    None
                },
            );
        }
        let scratch = RetentionScope::new(execution);
        let mut keys = Vec::with_capacity(rows.rows.len());
        for (index, row) in rows.rows.iter().enumerate() {
            if execution.cancelled() {
                return Err(SqlError::Aborted);
            }
            let partition = bound
                .partition
                .iter()
                .map(|b| eval(b, row, params))
                .collect::<R<Vec<_>>>()?;
            let order = bound
                .order
                .iter()
                .map(|(b, _, _)| eval(b, row, params))
                .collect::<R<Vec<_>>>()?;
            scratch.borrow().charge(
                std::mem::size_of::<Keyed>()
                    + (partition.len() + order.len()) * std::mem::size_of::<Value>()
                    + partition
                        .iter()
                        .chain(&order)
                        .map(value_bytes)
                        .sum::<usize>(),
            )?;
            keys.push(Keyed {
                index,
                partition,
                order,
            });
        }
        setops::sort(&mut keys, execution, |a, b| {
            a.partition
                .iter()
                .zip(&b.partition)
                .zip(&bound.partition)
                .map(|((a, b), scalar)| scalar.compare_values(a, b))
                .find(|c| *c != Ordering::Equal)
                .unwrap_or_else(|| compare_keys(&a.order, &b.order, &bound.order))
                .then(a.index.cmp(&b.index))
        })?;
        let mut begin = 0;
        let mut results = vec![Value::Null; rows.rows.len()];
        scratch
            .borrow()
            .charge(results.len() * std::mem::size_of::<Value>())?;
        while begin < keys.len() {
            let mut end = begin + 1;
            while end < keys.len()
                && keys[end]
                    .partition
                    .iter()
                    .zip(&keys[begin].partition)
                    .zip(&bound.partition)
                    .all(|((a, b), scalar)| scalar.compare_values(a, b) == Ordering::Equal)
            {
                execution.comparison()?;
                end += 1;
            }
            let partition = &keys[begin..end];
            let group_scope = RetentionScope::new(execution);
            group_scope
                .borrow()
                .charge((partition.len() + 1) * std::mem::size_of::<usize>())?;
            let mut groups = vec![0];
            for i in 1..partition.len() {
                execution.comparison()?;
                if compare_keys(&partition[i - 1].order, &partition[i].order, &bound.order)
                    != Ordering::Equal
                {
                    groups.push(i);
                }
            }
            groups.push(partition.len());
            let mut group = 0;
            let tiles = if call.name == "ntile" {
                Some(eval(
                    &bound.args[0],
                    &rows.rows[partition[0].index],
                    params,
                )?)
            } else {
                None
            };
            for (pos, key) in partition.iter().enumerate() {
                execution.comparison()?;
                if pos == groups[group + 1] {
                    group += 1;
                }
                let row = &rows.rows[key.index];
                let value = match call.name.as_str() {
                    "row_number" => Value::Int(pos as i64 + 1),
                    "rank" => Value::Int(groups[group] as i64 + 1),
                    "dense_rank" => Value::Int(group as i64 + 1),
                    "percent_rank" => Value::Float8(Float8::from_bits(
                        if partition.len() > 1 {
                            groups[group] as f64 / (partition.len() - 1) as f64
                        } else {
                            0.0
                        }
                        .to_bits(),
                    )),
                    "cume_dist" => Value::Float8(Float8::from_bits(
                        (groups[group + 1] as f64 / partition.len() as f64).to_bits(),
                    )),
                    "ntile" => match tiles.as_ref().unwrap() {
                        Value::Null => Value::Null,
                        Value::Int(n) if *n <= 0 => return Err(SqlError::InvalidNtileArgument),
                        Value::Int(n) => {
                            let n = *n as usize;
                            let len = partition.len();
                            let small = len / n;
                            let extra = len % n;
                            let big = (small + 1) * extra;
                            Value::Int(if pos < big {
                                pos / (small + 1) + 1
                            } else {
                                extra + (pos - big) / small + 1
                            } as i64)
                        }
                        _ => return Err(SqlError::Type),
                    },
                    "lag" | "lead" => {
                        let distance = if bound.args.len() > 1 {
                            eval(&bound.args[1], row, params)?
                        } else {
                            Value::Int(1)
                        };
                        match distance {
                            Value::Null => Value::Null,
                            Value::Int(n) => {
                                let target = pos as i128
                                    + if call.name == "lead" {
                                        i128::from(n)
                                    } else {
                                        -i128::from(n)
                                    };
                                if target >= 0 && target < partition.len() as i128 {
                                    eval(
                                        &bound.args[0],
                                        &rows.rows[partition[target as usize].index],
                                        params,
                                    )?
                                } else if bound.args.len() == 3 {
                                    eval(&bound.args[2], row, params)?
                                } else {
                                    Value::Null
                                }
                            }
                            _ => return Err(SqlError::Type),
                        }
                    }
                    _ => {
                        let first = frame_edge(
                            &frame.start,
                            offsets[0].as_ref(),
                            frame.mode,
                            true,
                            pos,
                            partition,
                            &groups,
                            group,
                            &bound.order,
                            clock,
                            execution,
                        )?;
                        let last = frame_edge(
                            &frame.end,
                            offsets[1].as_ref(),
                            frame.mode,
                            false,
                            pos,
                            partition,
                            &groups,
                            group,
                            &bound.order,
                            clock,
                            execution,
                        )?;
                        let included = |i: &usize| match frame.exclude {
                            Exclude::None => true,
                            Exclude::Current => *i != pos,
                            Exclude::Group => *i < groups[group] || *i >= groups[group + 1],
                            Exclude::Ties => {
                                *i == pos || *i < groups[group] || *i >= groups[group + 1]
                            }
                        };
                        if let Some(item) = &bound.aggregate {
                            let agg_scope = RetentionScope::new(execution);
                            let mut retention = agg_scope.borrow();
                            let mut aux =
                                aggregate_auxiliary(std::slice::from_ref(item), &mut retention)?;
                            aux[0].local_transition = true;
                            let mut value = vec![if call.name == "count" {
                                Value::Int(0)
                            } else {
                                Value::Null
                            }];
                            for i in (first..last).filter(included) {
                                execution.comparison()?;
                                let row = &rows.rows[partition[i].index];
                                if let Some(f) = &bound.filter
                                    && eval(f, row, params)? != Value::Bool(true)
                                {
                                    continue;
                                }
                                let before = aggregate_retained(&value);
                                update_aggregates(
                                    std::slice::from_ref(item),
                                    std::slice::from_ref(&bound.column),
                                    &mut value,
                                    &mut aux,
                                    &mut retention,
                                    &Tuple {
                                        values: vec![row.values.clone()],
                                        version: row.version,
                                    },
                                    params,
                                    &mut 0,
                                )?;
                                let after = aggregate_retained(&value);
                                if after > before {
                                    retention.charge(after - before)?;
                                } else {
                                    retention.release(before - after);
                                }
                            }
                            finish_aggregates(
                                std::slice::from_ref(item),
                                &mut value,
                                &aux,
                                &mut 0,
                            )?;
                            value.remove(0)
                        } else {
                            let nth = if call.name == "nth_value" {
                                eval(&bound.args[1], row, params)?
                            } else {
                                Value::Int(1)
                            };
                            let selected = match nth {
                                Value::Null => None,
                                Value::Int(n) if n <= 0 => {
                                    return Err(SqlError::InvalidNthValueArgument);
                                }
                                Value::Int(n) => {
                                    let mut selected = None;
                                    let mut count = 0i64;
                                    for i in first..last {
                                        execution.comparison()?;
                                        if !included(&i) {
                                            continue;
                                        }
                                        count += 1;
                                        if call.name == "last_value" || count == n {
                                            selected = Some(i);
                                            if call.name != "last_value" {
                                                break;
                                            }
                                        }
                                    }
                                    selected
                                }
                                _ => return Err(SqlError::Type),
                            };
                            selected
                                .map(|i| {
                                    eval(&bound.args[0], &rows.rows[partition[i].index], params)
                                })
                                .transpose()?
                                .unwrap_or(Value::Null)
                        }
                    }
                };
                rows.scope
                    .borrow()
                    .charge(std::mem::size_of::<Value>() + value_bytes(&value))?;
                results[key.index] = value;
            }
            begin = end;
        }
        for (row, value) in rows.rows.iter_mut().zip(results) {
            row.values.push(value);
        }
        rows.columns.push(bound.column);
        rows.typmods.push(-1);
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use Value::{Int as I, Null as N};
    fn execute(sql: &str) -> R<SqlResult> {
        let mut s = Relational::default();
        let p = Prepared::parse(sql, 1, 0)?;
        s.execute(&p, &[], None)
    }
    fn values(sql: &str) -> Vec<Vec<Value>> {
        execute(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect()
    }
    const INPUT_SQL: &str = "(VALUES (1,10),(2,20),(3,20),(4,NULL)) t(id,x)";
    #[test]
    fn all_frame_modes_and_exclusions() {
        for mode in ["ROWS", "RANGE", "GROUPS"] {
            for (exclude, expected) in [
                ("NO OTHERS", [50, 50, 50, 50]),
                ("CURRENT ROW", [40, 30, 30, 50]),
                ("GROUP", [40, 10, 10, 50]),
                ("TIES", [50, 30, 30, 50]),
            ] {
                let sql = format!(
                    "SELECT sum(x) OVER (ORDER BY x {mode} BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE {exclude}) FROM {INPUT_SQL} ORDER BY id"
                );
                assert_eq!(
                    values(&sql),
                    expected.into_iter().map(|n| vec![I(n)]).collect::<Vec<_>>(),
                    "{sql}"
                );
            }
        }
    }
    #[test]
    fn peers_null_keys_and_default_frames() {
        assert_eq!(
            values(&format!(
                "SELECT count(*) OVER (ORDER BY x), rank() OVER (ORDER BY x),dense_rank() OVER (ORDER BY x) FROM {INPUT_SQL} ORDER BY id"
            )),
            vec![
                vec![I(1), I(1), I(1)],
                vec![I(3), I(2), I(2)],
                vec![I(3), I(2), I(2)],
                vec![I(4), I(4), I(3)]
            ]
        );
        assert_eq!(
            values(&format!(
                "SELECT row_number() OVER (ORDER BY x DESC),row_number() OVER (ORDER BY x DESC NULLS LAST) FROM {INPUT_SQL} ORDER BY id"
            )),
            vec![
                vec![I(4), I(3)],
                vec![I(2), I(1)],
                vec![I(3), I(2)],
                vec![I(1), I(4)]
            ]
        );
        assert_eq!(
            values(&format!(
                "SELECT count(*) OVER (PARTITION BY x) FROM {INPUT_SQL} ORDER BY id"
            )),
            vec![vec![I(1)], vec![I(2)], vec![I(2)], vec![I(1)]]
        );
        assert!(
            values(&format!(
                "SELECT count(*) OVER (),row_number() OVER () FROM {INPUT_SQL} WHERE false"
            ))
            .is_empty()
        );
    }
    #[test]
    fn bounded_frames_both_directions() {
        for (mode, expected) in [
            ("ROWS", [10, 30, 40, 20]),
            ("GROUPS", [10, 50, 50, 40]),
            ("RANGE", [10, 50, 50, 0]),
        ] {
            let amount = if mode == "RANGE" { 10 } else { 1 };
            let sql = format!(
                "SELECT count(x) OVER (ORDER BY x {mode} BETWEEN {amount} PRECEDING AND CURRENT ROW),sum(x) OVER (ORDER BY x {mode} BETWEEN {amount} PRECEDING AND CURRENT ROW) FROM {INPUT_SQL} ORDER BY id"
            );
            let rows = values(&sql);
            assert_eq!(
                rows.into_iter().map(|r| r[1].clone()).collect::<Vec<_>>(),
                expected
                    .into_iter()
                    .map(|n| if n == 0 { N } else { I(n) })
                    .collect::<Vec<_>>(),
                "{sql}"
            );
        }
        assert_eq!(
            values(&format!(
                "SELECT count(*) OVER (ORDER BY x RANGE BETWEEN CURRENT ROW AND 10 FOLLOWING),count(*) OVER (ORDER BY x DESC RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) FROM {INPUT_SQL} ORDER BY id"
            )),
            vec![
                vec![I(3), I(3)],
                vec![I(2), I(2)],
                vec![I(2), I(2)],
                vec![I(1), I(1)]
            ]
        );
        assert_eq!(
            values(&format!(
                "SELECT count(*) OVER (ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 1 FOLLOWING),count(*) OVER (ORDER BY id ROWS BETWEEN 9 PRECEDING AND 8 PRECEDING) FROM {INPUT_SQL} ORDER BY id"
            )),
            vec![vec![I(0), I(0)]; 4]
        );
    }
    #[test]
    fn value_functions_and_ntile() {
        assert_eq!(
            values(&format!(
                "SELECT lag(x,1,-1) OVER (ORDER BY id),lead(x,-1,-2) OVER (ORDER BY id),first_value(x) OVER (ORDER BY id),last_value(x) OVER (ORDER BY id),nth_value(x,2) OVER (ORDER BY id),ntile(3) OVER (ORDER BY id),ntile(9) OVER (ORDER BY id) FROM {INPUT_SQL} ORDER BY id"
            )),
            vec![
                vec![I(-1), I(-2), I(10), I(10), N, I(1), I(1)],
                vec![I(10), I(10), I(10), I(20), I(20), I(1), I(2)],
                vec![I(20), I(20), I(10), I(20), I(20), I(2), I(3)],
                vec![I(20), I(20), I(10), N, I(20), I(3), I(4)]
            ]
        );
        assert_eq!(
            values(&format!(
                "SELECT lag(x,NULL,0) OVER (),nth_value(x,NULL) OVER (),ntile(NULL) OVER () FROM {INPUT_SQL}"
            )),
            vec![vec![N, N, N]; 4]
        );
    }
    #[test]
    fn named_windows_scalar_wrappers_and_order() {
        assert_eq!(
            values(&format!(
                "SELECT id,sum(x) OVER (w ROWS UNBOUNDED PRECEDING)*2 AS doubled FROM {INPUT_SQL} WINDOW w AS (ORDER BY id) ORDER BY row_number() OVER (ORDER BY id DESC)"
            )),
            vec![
                vec![I(4), I(100)],
                vec![I(3), I(100)],
                vec![I(2), I(60)],
                vec![I(1), I(20)]
            ]
        );
        let r = execute(&format!(
            "SELECT round(avg(x) OVER (),2),count(x) OVER (),sum(x) OVER () FROM {INPUT_SQL}"
        ))
        .unwrap();
        assert_eq!(r.columns, vec!["round", "count", "sum"]);
        let p=Prepared::parse(&format!("SELECT avg(x) OVER (),count(x) OVER (),sum(x) OVER (),row_number() OVER () FROM {INPUT_SQL}"),1,0).unwrap();
        let d = p.describe(&Relational::default()).unwrap();
        assert_eq!(
            d.columns
                .iter()
                .map(|c| c.data_type.clone())
                .collect::<Vec<_>>(),
            vec![
                DataType::Numeric,
                DataType::BigInt,
                DataType::BigInt,
                DataType::BigInt
            ]
        );
    }
    #[test]
    fn result_types_and_distribution_functions() {
        let sql = "SELECT row_number() OVER (),rank() OVER (),dense_rank() OVER (),ntile(2) OVER (),percent_rank() OVER (),cume_dist() OVER (),count(x) OVER (),sum(x) OVER (),avg(x) OVER (),lag(x) OVER (),lead(x) OVER (),first_value(x) OVER (),last_value(x) OVER (),nth_value(x,1) OVER () FROM (VALUES(1)) t(x)";
        let p = Prepared::parse(sql, 1, 0).unwrap();
        let d = p.describe(&Relational::default()).unwrap();
        assert_eq!(
            d.columns
                .iter()
                .map(|c| c.data_type.clone())
                .collect::<Vec<_>>(),
            vec![
                DataType::BigInt,
                DataType::BigInt,
                DataType::BigInt,
                DataType::Integer,
                DataType::DoublePrecision,
                DataType::DoublePrecision,
                DataType::BigInt,
                DataType::BigInt,
                DataType::Numeric,
                DataType::Integer,
                DataType::Integer,
                DataType::Integer,
                DataType::Integer,
                DataType::Integer,
            ]
        );
        let f = |n: f64| Value::Float8(Float8::from_bits(n.to_bits()));
        assert_eq!(
            values(
                "SELECT percent_rank() OVER (ORDER BY x),cume_dist() OVER (ORDER BY x) FROM (VALUES(1),(1),(2),(NULL)) t(x) ORDER BY x"
            ),
            vec![
                vec![f(0.0), f(0.5)],
                vec![f(0.0), f(0.5)],
                vec![f(2.0 / 3.0), f(0.75)],
                vec![f(1.0), f(1.0)],
            ]
        );
        assert_eq!(
            values(
                "SELECT percent_rank() OVER (ROWS -1 PRECEDING),cume_dist() OVER (ROWS -1 PRECEDING)"
            ),
            vec![vec![f(0.0), f(1.0)]]
        );
        assert_eq!(
            values("SELECT percent_rank() OVER (),cume_dist() OVER () FROM (VALUES(1),(2)) t(x)"),
            vec![vec![f(0.0), f(1.0)]; 2]
        );
        assert!(values("SELECT percent_rank() OVER (),cume_dist() OVER () WHERE false").is_empty());
    }
    #[test]
    fn grouped_filter_having_distinct_and_pagination() {
        assert_eq!(
            values(
                "SELECT x,count(*),sum(count(*)) OVER () FROM (VALUES(1),(1),(2),(3)) t(x) WHERE x<3 GROUP BY x HAVING count(*)>0 ORDER BY x"
            ),
            vec![
                vec![I(1), I(2), Value::Numeric(Numeric::parse("3").unwrap())],
                vec![I(2), I(1), Value::Numeric(Numeric::parse("3").unwrap())]
            ]
        );
        assert_eq!(
            values(&format!(
                "SELECT DISTINCT sum(x) FILTER (WHERE x>=20) OVER (),count(*) FILTER (WHERE x IS NULL) OVER () FROM {INPUT_SQL} LIMIT 1"
            )),
            vec![vec![I(40), I(1)]]
        );
        assert_eq!(
            values(&format!(
                "SELECT row_number() OVER (ORDER BY id) FROM {INPUT_SQL} ORDER BY id OFFSET 1 LIMIT 2"
            )),
            vec![vec![I(2)], vec![I(3)]]
        );
    }
    #[test]
    fn cte_set_operands_and_dependencies() {
        assert_eq!(
            values(
                "WITH q AS (SELECT row_number() OVER () AS n) SELECT n FROM q UNION ALL SELECT count(*) OVER () ORDER BY n"
            ),
            vec![vec![I(1)], vec![I(1)]]
        );
        let p = Prepared::parse(
            "SELECT sum(x) OVER (PARTITION BY dept ORDER BY id) FROM source_table",
            1,
            0,
        )
        .unwrap();
        assert_eq!(p.read_tables(), vec!["source_table"]);
        assert_eq!(p.authorization_tables().unwrap(), vec!["source_table"]);
    }
    #[test]
    fn deliberate_pg18_errors() {
        for (sql, error, state) in [
            (
                "SELECT 1 WHERE row_number() OVER ()>0",
                SqlError::WindowingError,
                "42P20",
            ),
            (
                "SELECT row_number() OVER () GROUP BY row_number() OVER ()",
                SqlError::WindowingError,
                "42P20",
            ),
            (
                "SELECT sum(row_number() OVER ()) OVER ()",
                SqlError::WindowingError,
                "42P20",
            ),
            (
                "SELECT sum(1) OVER absent",
                SqlError::UndefinedObject,
                "42704",
            ),
            (
                "SELECT sum(1) OVER (ROWS -1 PRECEDING)",
                SqlError::InvalidFrameOffset,
                "22013",
            ),
            (
                "SELECT sum(1) OVER (ORDER BY 1 RANGE -1 PRECEDING)",
                SqlError::InvalidFrameOffset,
                "22013",
            ),
            (
                "SELECT sum(1) OVER (ROWS UNBOUNDED FOLLOWING)",
                SqlError::WindowingError,
                "42P20",
            ),
            (
                "SELECT sum(1) OVER (ORDER BY 1,2 RANGE 1 PRECEDING)",
                SqlError::WindowingError,
                "42P20",
            ),
            (
                "SELECT ntile(0) OVER ()",
                SqlError::InvalidNtileArgument,
                "22014",
            ),
            (
                "SELECT nth_value(1,0) OVER ()",
                SqlError::InvalidNthValueArgument,
                "22016",
            ),
        ] {
            assert_eq!(execute(sql).unwrap_err(), error, "{sql}");
            assert_eq!(crate::sqlstate(&crate::CoreError::Sql(error)), state);
        }
    }
    #[test]
    fn temporal_range_and_group_aliases() {
        for (input, offset) in [
            (
                "(VALUES(DATE '2024-01-01'),(DATE '2024-01-02'),(DATE '2024-01-04')) t(x)",
                "1 day",
            ),
            (
                "(VALUES(TIME '01:00'),(TIME '02:00'),(TIME '04:00')) t(x)",
                "1 hour",
            ),
            (
                "(VALUES(TIMESTAMP '2024-01-01'),(TIMESTAMP '2024-01-02'),(TIMESTAMP '2024-01-04')) t(x)",
                "1 day",
            ),
        ] {
            assert_eq!(
                values(&format!(
                    "SELECT count(*) OVER (ORDER BY x RANGE BETWEEN INTERVAL '{offset}' PRECEDING AND CURRENT ROW) FROM {input} ORDER BY x"
                )),
                vec![vec![I(1)], vec![I(2)], vec![I(1)]]
            );
        }
        for group in ["1", "x", "y"] {
            assert_eq!(
                values(&format!(
                    "SELECT x AS y,sum(count(*)) OVER () FROM (VALUES(1),(1),(2)) t(x) GROUP BY {group} ORDER BY y"
                )),
                vec![
                    vec![I(1), Value::Numeric(Numeric::parse("3").unwrap())],
                    vec![I(2), Value::Numeric(Numeric::parse("3").unwrap())]
                ]
            );
        }
        assert_eq!(
            values("SELECT lag(x,1,'-1') OVER () FROM (VALUES(1)) t(x)"),
            vec![vec![I(-1)]]
        );
        assert_eq!(
            values("SELECT * ,row_number() OVER (ORDER BY id) FROM (VALUES(1,2)) t(id,x)"),
            vec![vec![I(1), I(2), I(1)]]
        );
    }
    #[test]
    fn prepared_parameter_types() {
        let mut s = Relational::default();
        let p = Prepared::parse(
            "SELECT ntile($1) OVER (),lag(1,$2,$3) OVER (),count(*) OVER (ROWS $4 PRECEDING)",
            1,
            0,
        )
        .unwrap();
        let d = p.describe(&s).unwrap();
        assert_eq!(
            d.parameters,
            vec![
                DataType::Integer,
                DataType::Integer,
                DataType::Integer,
                DataType::BigInt
            ]
        );
        assert_eq!(
            s.execute(&p, &[I(2), I(1), I(-1), I(1)], None)
                .unwrap()
                .rows[0]
                .values,
            vec![I(1), I(-1), I(1)]
        );
    }
    #[test]
    fn cancellation_and_work_and_retention_budgets() {
        for kind in 0..3 {
            let mut cancel = || kind == 0;
            let execution = Execution::new(&mut cancel, &[]);
            let mut rows = Relation::new(vec![], vec![], &execution);
            rows.push(SqlRow {
                values: vec![],
                version: 0,
            })
            .unwrap();
            if kind == 1 {
                execution.visits.set(MAX_JOIN_VISITS);
            }
            if kind == 2 {
                let used = execution.retention.borrow().used();
                *execution.retention.borrow_mut() = scan::Retention::new(used);
                execution.retention.borrow_mut().charge(used).unwrap();
            }
            let call = Call {
                name: "row_number".into(),
                output: "n".into(),
                args: vec![],
                filter: None,
                spec: Spec::default(),
            };
            assert_eq!(
                run(&[call], &mut rows, &[], &[], &[], None, &execution).unwrap_err(),
                if kind == 0 {
                    SqlError::Aborted
                } else {
                    SqlError::Limit
                }
            );
        }
    }
    #[test]
    fn other_confirmed_pg_errors() {
        for (sql, error) in [
            ("SELECT sum(row_number() OVER ())", SqlError::GroupingError),
            (
                "SELECT sum(1) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING)",
                SqlError::WindowingError,
            ),
            (
                "SELECT sum(1) OVER (ROWS BETWEEN CURRENT ROW AND 1 PRECEDING)",
                SqlError::WindowingError,
            ),
            (
                "SELECT sum(1) OVER (GROUPS CURRENT ROW)",
                SqlError::WindowingError,
            ),
            (
                "SELECT sum(1) OVER (ROWS NULL PRECEDING)",
                SqlError::NullValueNotAllowed,
            ),
            (
                "SELECT sum(1) OVER () HAVING row_number() OVER ()>0",
                SqlError::WindowingError,
            ),
            ("SELECT 1 WINDOW w AS (),w AS ()", SqlError::WindowingError),
            (
                "SELECT sum(1) OVER (w PARTITION BY 1) WINDOW w AS ()",
                SqlError::WindowingError,
            ),
            (
                "SELECT sum(1) OVER (w ORDER BY 2) WINDOW w AS (ORDER BY 1)",
                SqlError::WindowingError,
            ),
            (
                "SELECT sum(1) OVER (w) WINDOW w AS (ROWS CURRENT ROW)",
                SqlError::WindowingError,
            ),
        ] {
            assert_eq!(execute(sql).unwrap_err(), error, "{sql}");
        }
    }
    #[test]
    fn lazy_arguments_and_ignored_frames() {
        assert_eq!(
            values("SELECT first_value(10/x) OVER (ORDER BY id) FROM (VALUES(1,1),(2,0)) t(id,x)"),
            vec![vec![I(10)], vec![I(10)]]
        );
        assert_eq!(
            values("SELECT lag(x,0,1/(x-x)) OVER () FROM (VALUES(1),(2)) t(x)"),
            vec![vec![I(1)], vec![I(2)]]
        );
        assert_eq!(
            values(
                "SELECT CASE WHEN x=0 THEN row_number() OVER () ELSE 10/x END FROM (VALUES(0),(2)) t(x)"
            ),
            vec![vec![I(1)], vec![I(5)]]
        );
        assert_eq!(
            values("SELECT row_number() OVER (ROWS -1 PRECEDING)"),
            vec![vec![I(1)]]
        );
    }
    #[test]
    fn range_special_numeric_values() {
        for ty in ["numeric", "float8"] {
            assert_eq!(
                execute(&format!(
                    "SELECT sum(1) OVER (ORDER BY 1::{ty} RANGE 'NaN'::{ty} PRECEDING)"
                ))
                .unwrap_err(),
                SqlError::InvalidFrameOffset
            );
            assert_eq!(
                values(&format!(
                    "SELECT count(*) OVER (ORDER BY x RANGE 'Infinity'::{ty} PRECEDING) FROM (VALUES(1::{ty}),('Infinity'::{ty}),('NaN'::{ty})) t(x) ORDER BY x"
                )),
                vec![vec![I(1)], vec![I(2)], vec![I(1)]]
            );
        }
    }
    #[test]
    fn range_float_boundary_overflow() {
        assert_eq!(
            values(
                "SELECT count(*) OVER (ORDER BY x RANGE BETWEEN CURRENT ROW AND 1e308::float8 FOLLOWING) FROM (VALUES(1e308::float8),('Infinity'::float8)) t(x) ORDER BY x"
            ),
            vec![vec![I(2)], vec![I(1)]]
        );
    }
    #[test]
    fn range_offset_support_types() {
        assert_eq!(
            values("SELECT count(*) OVER (ORDER BY 1 RANGE 9223372036854775807 PRECEDING)"),
            vec![vec![I(1)]]
        );
        assert_eq!(
            values("SELECT count(*) OVER (ORDER BY 1::smallint RANGE 40000 PRECEDING)"),
            vec![vec![I(1)]]
        );
        assert_eq!(
            execute("SELECT count(*) OVER (ORDER BY 1 RANGE 0.5 PRECEDING)").unwrap_err(),
            SqlError::Unsupported
        );
        assert_eq!(
            values("SELECT count(*) OVER (ORDER BY 1::real RANGE 0.5::float8 PRECEDING)"),
            vec![vec![I(1)]]
        );
    }
    #[test]
    fn unsupported_aggregate_window_modifiers() {
        for (sql, error) in [
            ("SELECT count() OVER ()", SqlError::WrongObjectType),
            (
                "SELECT count(DISTINCT x) OVER () FROM (VALUES(1)) t(x)",
                SqlError::Unsupported,
            ),
            (
                "SELECT sum(x ORDER BY x) OVER () FROM (VALUES(1)) t(x)",
                SqlError::Unsupported,
            ),
        ] {
            assert_eq!(execute(sql).unwrap_err(), error);
        }
    }
}
