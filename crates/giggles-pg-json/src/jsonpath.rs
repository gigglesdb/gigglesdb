//! Qualified standalone JSONPath navigation. See JSONPATH.md for the exact boundary.
use super::*;
use std::borrow::Cow;
mod predicates;
use predicates::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Strict,
    Lax,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathError {
    Syntax,
    Unsupported,
    Structural,
    Object,
    Array,
    Subscript,
    Variable,
    VariablesShape,
    Predicate,
    Limit,
}
pub type PathResult<T> = std::result::Result<T, PathError>;
#[derive(Clone, Copy, Debug)]
pub struct PathLimits {
    pub input_bytes: usize,
    pub steps: usize,
    pub sequence: usize,
    pub work: usize,
    pub output_bytes: usize,
}
impl Default for PathLimits {
    fn default() -> Self {
        Self {
            input_bytes: 65536,
            steps: 128,
            sequence: 10000,
            work: 100000,
            output_bytes: 1048576,
        }
    }
}
#[derive(Clone, Debug)]
enum Step {
    Member(String),
    Type,
    Size,
    Members,
    Selectors(Vec<Selector>),
    Elements,
    Filter(Box<Predicate>),
    Descend(u32, u32),
}
#[derive(Clone, Debug)]
enum Bound {
    Index(i64),
    Last,
    LastOffset(bool, u64),
}
#[derive(Clone, Debug)]
enum Selector {
    Index(Bound),
    Range(Bound, Bound),
}
impl Selector {
    fn parse(s: &str) -> PathResult<Self> {
        if let Some((a, z)) = s.split_once(" to ") {
            Ok(Self::Range(Bound::parse(a)?, Bound::parse(z)?))
        } else {
            Ok(Self::Index(Bound::parse(s)?))
        }
    }
}
impl Bound {
    fn parse(s: &str) -> PathResult<Self> {
        let s = s.trim();
        if let Some(rest) = s.strip_prefix("last") {
            let rest = rest.trim();
            if rest.is_empty() {
                return Ok(Self::Last);
            }
            let subtract = match rest.as_bytes().first() {
                Some(b'+') => false,
                Some(b'-') => true,
                _ => return Err(PathError::Unsupported),
            };
            let magnitude = rest[1..].trim();
            if magnitude.is_empty() || !magnitude.bytes().all(|b| b.is_ascii_digit()) {
                return Err(PathError::Unsupported);
            }
            Ok(Self::LastOffset(
                subtract,
                magnitude.parse().map_err(|_| PathError::Unsupported)?,
            ))
        } else {
            s.parse()
                .map(Self::Index)
                .map_err(|_| PathError::Unsupported)
        }
    }
    fn value(&self, len: usize) -> PathResult<i128> {
        let last = (len as i128).checked_sub(1).ok_or(PathError::Subscript)?;
        let value = match self {
            Self::Last => last,
            Self::Index(i) => *i as i128,
            Self::LastOffset(subtract, magnitude) => if *subtract {
                last.checked_sub(*magnitude as i128)
            } else {
                last.checked_add(*magnitude as i128)
            }
            .ok_or(PathError::Subscript)?,
        };
        // PG getArrayIndex rejects int32 conversion overflow independently of
        // strict/lax structural suppression, before any bounds clipping.
        i32::try_from(value)
            .map(i128::from)
            .map_err(|_| PathError::Subscript)
    }
}
#[derive(Clone, Debug)]
pub struct JsonPath {
    mode: Mode,
    variable: Option<String>,
    current: bool,
    literal: Option<Node>,
    predicate: Option<Box<Predicate>>,
    steps: Vec<Step>,
}
impl JsonPath {
    pub fn parse(text: &str, limits: PathLimits) -> PathResult<Self> {
        if text.len() > limits.input_bytes {
            return Err(PathError::Limit);
        }
        let mut s = text.trim();
        let mode = if let Some(t) = s.strip_prefix("strict ") {
            s = t.trim_start();
            Mode::Strict
        } else {
            if let Some(t) = s.strip_prefix("lax ") {
                s = t.trim_start();
            }
            Mode::Lax
        };
        if let Some(predicate) = parse_predicate(s, limits, 0)? {
            return Ok(Self {
                mode,
                variable: None,
                current: false,
                literal: None,
                steps: Vec::new(),
                predicate: Some(Box::new(predicate)),
            });
        }
        if matches!(s.as_bytes().first(), Some(b'"' | b'-' | b'0'..=b'9'))
            || matches!(s, "true" | "false" | "null")
        {
            let value = Jsonb::parse(s, Limits::default()).map_err(|e| {
                if e == Error::Limit {
                    PathError::Limit
                } else {
                    PathError::Unsupported
                }
            })?;
            return Ok(Self {
                mode,
                variable: None,
                current: false,
                literal: Some(value.node),
                steps: Vec::new(),
                predicate: None,
            });
        }
        let current = s.starts_with('@');
        s = s
            .strip_prefix(if current { '@' } else { '$' })
            .ok_or(PathError::Unsupported)?;
        let ident = |c: char| c.is_ascii_alphanumeric() || c == '_';
        let n = s.find(|c: char| !ident(c)).unwrap_or(s.len());
        if s.as_bytes().first().is_some_and(u8::is_ascii_digit) {
            return Err(PathError::Unsupported);
        }
        if current && n > 0 {
            return Err(PathError::Syntax);
        }
        let variable = if n > 0 {
            let v = s[..n].to_owned();
            s = &s[n..];
            Some(v)
        } else {
            None
        };
        let mut steps = Vec::new();
        loop {
            s = s.trim_start();
            if s.is_empty() {
                break;
            }
            if steps.len() >= limits.steps.min(128) {
                return Err(PathError::Limit);
            }
            if let Some(t) = s.strip_prefix('?') {
                let t = t.trim_start();
                if !t.starts_with('(') {
                    return Err(PathError::Syntax);
                }
                let end = closing(t)?;
                let pred = parse_predicate(&t[1..end], limits, 0)?.ok_or(PathError::Syntax)?;
                steps.push(Step::Filter(Box::new(pred)));
                s = &t[end + 1..];
            } else if let Some(t) = s.strip_prefix('.') {
                s = t;
                if let Some(t) = s.strip_prefix("**") {
                    let mut t = t.trim_start();
                    let mut bounds = (0, u32::MAX);
                    if let Some(body) = t.strip_prefix('{') {
                        let end = body.find('}').ok_or(PathError::Syntax)?;
                        let parse = |v: &str| {
                            if v.trim() == "last" {
                                Ok(u32::MAX)
                            } else {
                                v.trim().parse::<u32>().map_err(|_| PathError::Unsupported)
                            }
                        };
                        bounds = if let Some((a, b)) = body[..end].split_once(" to ") {
                            (parse(a)?, parse(b)?)
                        } else {
                            let n = parse(&body[..end])?;
                            (n, n)
                        };
                        t = &body[end + 1..];
                    }
                    steps.push(Step::Descend(bounds.0, bounds.1));
                    s = t;
                } else if let Some(t) = s.strip_prefix('*') {
                    steps.push(Step::Members);
                    s = t;
                } else if s.starts_with('"') {
                    let mut parser = Parser {
                        s,
                        p: 0,
                        count: 0,
                        l: Limits::default(),
                        decode: true,
                        b: true,
                    };
                    let key = parser.string().map_err(|_| PathError::Syntax)?;
                    s = &s[parser.p..];
                    steps.push(Step::Member(key));
                } else {
                    let n = s.find(|c: char| !ident(c)).unwrap_or(s.len());
                    if n == 0 {
                        return Err(PathError::Unsupported);
                    }
                    if s.as_bytes().first().is_some_and(u8::is_ascii_digit) {
                        return Err(PathError::Unsupported);
                    }
                    let name = &s[..n];
                    s = &s[n..];
                    if let Some(rest) = s.strip_prefix("()") {
                        steps.push(match name {
                            "type" => Step::Type,
                            "size" => Step::Size,
                            _ => return Err(PathError::Unsupported),
                        });
                        s = rest;
                    } else {
                        steps.push(Step::Member(name.to_owned()));
                    }
                }
            } else if let Some(t) = s.strip_prefix('[') {
                let end = t.find(']').ok_or(PathError::Syntax)?;
                let sub = t[..end].trim();
                if sub == "*" {
                    steps.push(Step::Elements);
                } else {
                    let mut selectors = Vec::new();
                    for item in sub.split(',') {
                        if item.trim().is_empty() {
                            return Err(PathError::Syntax);
                        }
                        if selectors.len() >= limits.steps.min(128)
                            || selectors.len() >= limits.sequence
                            || selectors.len() >= limits.work
                        {
                            return Err(PathError::Limit);
                        }
                        selectors.push(Selector::parse(item)?);
                    }
                    steps.push(Step::Selectors(selectors));
                }
                s = &t[end + 1..];
            } else {
                return Err(PathError::Unsupported);
            }
        }
        Ok(Self {
            mode,
            variable,
            current,
            literal: None,
            predicate: None,
            steps,
        })
    }
    pub fn mode(&self) -> Mode {
        self.mode
    }
}
struct Budget {
    ignore_structural: bool,
    throw_errors: bool,
    stop_after_first: bool,
    depth: usize,
    work: usize,
    limit: PathLimits,
}
impl Budget {
    fn push_value<'a>(&mut self, out: &mut Vec<Cow<'a, Node>>, n: Cow<'a, Node>) -> PathResult<()> {
        self.visit()?;
        if out.len() >= self.limit.sequence {
            return Err(PathError::Limit);
        }
        out.push(n);
        Ok(())
    }
    // Owned navigation values are exclusively the bounded scalars emitted by
    // type()/size(). Input subtrees always remain borrowed during navigation.
    fn copy_scalar(&mut self, n: &Node) -> PathResult<Node> {
        self.visit()?;
        let bytes = match &n.kind {
            Kind::String(v)
                if matches!(
                    v.as_str(),
                    "null" | "boolean" | "number" | "string" | "array" | "object"
                ) =>
            {
                v.len() + 2
            }
            Kind::Number(v) if v.len() <= 20 && v.bytes().all(|c| c.is_ascii_digit()) => v.len(),
            _ => return Err(PathError::Limit),
        };
        if bytes > self.limit.output_bytes {
            return Err(PathError::Limit);
        }
        Ok(n.clone())
    }
    fn visit(&mut self) -> PathResult<()> {
        self.work = self.work.checked_add(1).ok_or(PathError::Limit)?;
        if self.work > self.limit.work {
            Err(PathError::Limit)
        } else {
            Ok(())
        }
    }
    fn push<'a>(&mut self, out: &mut Vec<&'a Node>, n: &'a Node) -> PathResult<()> {
        self.visit()?;
        if out.len() >= self.limit.sequence {
            return Err(PathError::Limit);
        }
        out.push(n);
        Ok(())
    }
}
fn apply<'a>(
    node: &'a Node,
    step: &Step,
    mode: Mode,
    unwrap: bool,
    out: &mut Vec<&'a Node>,
    budget: &mut Budget,
) -> PathResult<()> {
    budget.visit()?;
    if unwrap
        && mode == Mode::Lax
        && matches!(step, Step::Member(_) | Step::Members)
        && let Kind::Array(a) = &node.kind
    {
        for n in a {
            apply(n, step, mode, false, out, budget)?;
        }
        return Ok(());
    }
    if let Step::Descend(first, last) = step {
        fn descend<'a>(
            n: &'a Node,
            depth: u32,
            first: u32,
            last: u32,
            out: &mut Vec<&'a Node>,
            b: &mut Budget,
        ) -> PathResult<()> {
            b.visit()?;
            if depth > 128 {
                return Err(PathError::Limit);
            }
            let container = matches!(n.kind, Kind::Object(_) | Kind::Array(_));
            if depth >= first && depth <= last
                || depth > 0 && first == u32::MAX && last == u32::MAX && !container
            {
                b.push(out, n)?;
            }
            if depth < last {
                match &n.kind {
                    Kind::Array(a) => {
                        for n in a {
                            descend(n, depth + 1, first, last, out, b)?
                        }
                    }
                    Kind::Object(a) => {
                        for (_, n) in a {
                            descend(n, depth + 1, first, last, out, b)?
                        }
                    }
                    _ => {}
                }
            }
            Ok(())
        }
        return descend(node, 0, *first, *last, out, budget);
    }
    match (step, &node.kind) {
        (Step::Member(k), Kind::Object(_)) => {
            if let Some(n) = node.field(k) {
                budget.push(out, n)?;
            } else if mode == Mode::Strict && !budget.ignore_structural {
                return Err(PathError::Structural);
            }
        }
        (Step::Members, Kind::Object(a)) => {
            for (_, n) in a {
                budget.push(out, n)?;
            }
        }
        (Step::Elements, Kind::Array(a)) => {
            for n in a {
                budget.push(out, n)?;
            }
        }
        (Step::Elements, _) if mode == Mode::Lax => budget.push(out, node)?,
        _ => {
            if mode == Mode::Strict && !budget.ignore_structural {
                return Err(match step {
                    Step::Member(_) => PathError::Structural,
                    Step::Members => PathError::Object,
                    _ => PathError::Array,
                });
            }
        }
    }
    Ok(())
}
fn apply_selector<'a>(
    node: &'a Node,
    selector: &Selector,
    mode: Mode,
    out: &mut Vec<&'a Node>,
    budget: &mut Budget,
) -> PathResult<()> {
    budget.visit()?;
    let values = match &node.kind {
        Kind::Array(a) => a.as_slice(),
        _ if mode == Mode::Lax => std::slice::from_ref(node),
        _ if budget.ignore_structural => return Ok(()),
        _ => return Err(PathError::Array),
    };
    let (from, to) = match selector {
        Selector::Index(index) => {
            let index = index.value(values.len())?;
            (index, index)
        }
        Selector::Range(a, z) => (a.value(values.len())?, z.value(values.len())?),
    };
    if mode == Mode::Strict
        && !budget.ignore_structural
        && (from < 0 || to < from || to >= values.len() as i128)
    {
        return Err(PathError::Subscript);
    }
    let start = from.max(0);
    let end = to.min(values.len() as i128 - 1);
    if start <= end {
        for i in start..=end {
            budget.push(out, &values[i as usize])?;
        }
    }
    Ok(())
}
fn evaluate<'a>(
    target: &'a Jsonb,
    path: &'a JsonPath,
    vars: &'a Jsonb,
    l: PathLimits,
    partial: bool,
    exists: bool,
    silent: bool,
) -> PathResult<Vec<Cow<'a, Node>>> {
    if !matches!(vars.node.kind, Kind::Object(_)) {
        return Err(PathError::VariablesShape);
    }
    if path.steps.len() > l.steps.min(128) {
        return Err(PathError::Limit);
    }
    let mut b = Budget {
        ignore_structural: false,
        throw_errors: !silent,
        stop_after_first: exists && path.mode == Mode::Lax,
        depth: 0,
        work: 0,
        limit: l,
    };
    let mut seq = Vec::new();
    if let Some(pred) = &path.predicate {
        let value = pred.eval(&target.node, &target.node, vars, path.mode, &mut b)?;
        b.push_value(&mut seq, Cow::Borrowed(bool_node(value)))?;
        return Ok(seq);
    }
    let root = match &path.variable {
        Some(k) => vars.node.field(k).ok_or(PathError::Variable)?,
        None => path.literal.as_ref().unwrap_or(&target.node),
    };
    if let Err(e) = walk(
        Cow::Borrowed(root),
        &path.steps,
        path.mode,
        &mut seq,
        &mut b,
        &target.node,
        vars,
    ) && (!partial
        || !matches!(
            e,
            PathError::Structural | PathError::Object | PathError::Array | PathError::Subscript
        ))
    {
        return Err(e);
    }
    fn charge(n: &Node, b: &mut Budget, depth: usize) -> PathResult<()> {
        b.visit()?;
        if depth > 128 {
            return Err(PathError::Limit);
        }
        match &n.kind {
            Kind::Array(a) => {
                for n in a {
                    charge(n, b, depth + 1)?;
                }
            }
            Kind::Object(a) => {
                for (_, n) in a {
                    charge(n, b, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    for n in &seq {
        charge(n, &mut b, 0)?;
    }
    Ok(seq)
}
fn walk<'a>(
    n: Cow<'a, Node>,
    steps: &'a [Step],
    mode: Mode,
    out: &mut Vec<Cow<'a, Node>>,
    b: &mut Budget,
    root: &'a Node,
    vars: &'a Jsonb,
) -> PathResult<()> {
    if b.depth >= 128 {
        return Err(PathError::Limit);
    }
    b.depth += 1;
    let result = walk_inner(n, steps, mode, out, b, root, vars);
    b.depth -= 1;
    result
}
fn walk_inner<'a>(
    n: Cow<'a, Node>,
    steps: &'a [Step],
    mode: Mode,
    out: &mut Vec<Cow<'a, Node>>,
    b: &mut Budget,
    root: &'a Node,
    vars: &'a Jsonb,
) -> PathResult<()> {
    b.visit()?;
    let Some((step, rest)) = steps.split_first() else {
        return b.push_value(out, n);
    };
    if let Step::Selectors(selectors) = step {
        if selectors.len() > b.limit.steps.min(128)
            || selectors.len() > b.limit.sequence
            || selectors.len() > b.limit.work
        {
            return Err(PathError::Limit);
        }
        for selector in selectors {
            b.visit()?;
            match &n {
                Cow::Borrowed(node) => {
                    let mut selected = Vec::new();
                    apply_selector(node, selector, mode, &mut selected, b)?;
                    for value in selected {
                        walk(Cow::Borrowed(value), rest, mode, out, b, root, vars)?;
                        if b.stop_after_first && !out.is_empty() {
                            return Ok(());
                        }
                    }
                }
                Cow::Owned(node) => {
                    let mut selected = Vec::new();
                    apply_selector(node, selector, mode, &mut selected, b)?;
                    for value in selected {
                        if rest.is_empty() && out.len() >= b.limit.sequence {
                            return Err(PathError::Limit);
                        }
                        let scalar = b.copy_scalar(value)?;
                        walk(Cow::Owned(scalar), rest, mode, out, b, root, vars)?;
                        if b.stop_after_first && !out.is_empty() {
                            return Ok(());
                        }
                    }
                }
            }
        }
        return Ok(());
    }
    if matches!(step, Step::Type | Step::Size) {
        b.visit()?;
        if b.limit.sequence == 0 || rest.is_empty() && out.len() >= b.limit.sequence {
            return Err(PathError::Limit);
        }
        let kind = match step {
            Step::Type => {
                let name = match &n.kind {
                    Kind::Null => "null",
                    Kind::Bool(_) => "boolean",
                    Kind::Number(_) => "number",
                    Kind::String(_) => "string",
                    Kind::Array(_) => "array",
                    Kind::Object(_) => "object",
                };
                if name.len() + 2 > b.limit.output_bytes {
                    return Err(PathError::Limit);
                }
                Kind::String(name.to_owned())
            }
            Step::Size => {
                let size = match &n.kind {
                    Kind::Array(a) => a.len(),
                    _ if mode == Mode::Lax => 1,
                    _ if b.ignore_structural => return Ok(()),
                    _ => return Err(PathError::Array),
                };
                let bytes = if size == 0 {
                    1
                } else {
                    size.ilog10() as usize + 1
                };
                if bytes > b.limit.output_bytes {
                    return Err(PathError::Limit);
                }
                Kind::Number(size.to_string())
            }
            _ => unreachable!(),
        };
        return walk(
            Cow::Owned(Node { kind, span: 0..0 }),
            rest,
            mode,
            out,
            b,
            root,
            vars,
        );
    }
    if let Step::Filter(pred) = step {
        let mut candidate = |candidate: Cow<'a, Node>| -> PathResult<bool> {
            b.visit()?;
            if pred.eval(&candidate, root, vars, mode, b)? == Some(true) {
                walk(candidate, rest, mode, out, b, root, vars)?;
            }
            Ok(b.stop_after_first && !out.is_empty())
        };
        match n {
            Cow::Borrowed(node) if mode == Mode::Lax && matches!(node.kind, Kind::Array(_)) => {
                let Kind::Array(a) = &node.kind else {
                    unreachable!()
                };
                for n in a {
                    if candidate(Cow::Borrowed(n))? {
                        break;
                    }
                }
            }
            n => {
                candidate(n)?;
            }
        }
        return Ok(());
    }
    // PG jpiAny replaces the root continuation status with child traversal
    // status for containers. Only no-throw subscript errors can reach this
    // point in our grammar; resource and variable failures always propagate.
    let replace_root_subscript = !b.throw_errors
        && matches!(step, Step::Descend(0, _))
        && matches!(n.kind, Kind::Array(_) | Kind::Object(_));
    let mut next = Vec::new();
    match n {
        Cow::Borrowed(n) => {
            let mut borrowed = Vec::new();
            apply(n, step, mode, true, &mut borrowed, b)?;
            next.extend(borrowed.into_iter().map(Cow::Borrowed));
        }
        Cow::Owned(n) => {
            let mut selected = Vec::new();
            apply(&n, step, mode, true, &mut selected, b)?;
            for value in selected {
                if rest.is_empty() && out.len() >= b.limit.sequence {
                    return Err(PathError::Limit);
                }
                let scalar = b.copy_scalar(value)?;
                b.push_value(&mut next, Cow::Owned(scalar))?;
            }
        }
    }
    let saved_ignore = b.ignore_structural;
    if matches!(step, Step::Descend(..)) {
        b.ignore_structural = true;
    }
    let result = (|| {
        for (index, n) in next.into_iter().enumerate() {
            let result = walk(n, rest, mode, out, b, root, vars);
            if !(index == 0 && replace_root_subscript && result == Err(PathError::Subscript)) {
                result?;
            }
            if b.stop_after_first && !out.is_empty() {
                break;
            }
        }
        Ok(())
    })();
    b.ignore_structural = saved_ignore;
    result
}

fn suppressed<T>(r: PathResult<T>, silent: bool) -> PathResult<Option<T>> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(
            PathError::Structural
            | PathError::Object
            | PathError::Array
            | PathError::Subscript
            | PathError::Predicate,
        ) if silent => Ok(None),
        Err(e) => Err(e),
    }
}
/// SQL NULL arguments return SQL NULL. JSON null remains a JSON value.
pub fn jsonb_path_query(
    target: Option<&Jsonb>,
    path: Option<&JsonPath>,
    vars: Option<&Jsonb>,
    silent: Option<bool>,
    l: PathLimits,
) -> PathResult<Option<Vec<Jsonb>>> {
    let (Some(t), Some(p), Some(v), Some(s)) = (target, path, vars, silent) else {
        return Ok(None);
    };
    let seq = suppressed(evaluate(t, p, v, l, s, false, s), s)?.unwrap_or_default();
    // Render every result under a cumulative byte cap before cloning any result tree.
    let mut bytes = 0usize;
    let mut nodes = 0usize;
    fn admit(n: &Node, depth: usize, count: &mut usize, l: Limits) -> PathResult<()> {
        if depth > l.depth.min(128) || *count >= l.nodes {
            return Err(PathError::Limit);
        }
        *count += 1;
        match &n.kind {
            Kind::Array(a) => {
                for n in a {
                    admit(n, depth + 1, count, l)?;
                }
            }
            Kind::Object(a) => {
                for (_, n) in a {
                    admit(n, depth + 1, count, l)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    for n in &seq {
        admit(n, 0, &mut nodes, t.limits)?;
        let mut text = String::new();
        let mut limits = t.limits;
        limits.output_bytes = limits
            .output_bytes
            .min(l.output_bytes.saturating_sub(bytes));
        render(n, &mut text, limits).map_err(|_| PathError::Limit)?;
        bytes = bytes.checked_add(text.len()).ok_or(PathError::Limit)?;
    }
    Ok(Some(
        seq.into_iter()
            .map(|n| Jsonb {
                node: n.into_owned(),
                limits: t.limits,
            })
            .collect(),
    ))
}
pub fn jsonb_path_query_first(
    t: Option<&Jsonb>,
    p: Option<&JsonPath>,
    v: Option<&Jsonb>,
    s: Option<bool>,
    l: PathLimits,
) -> PathResult<Option<Jsonb>> {
    Ok(jsonb_path_query(t, p, v, s, l)?.and_then(|a| a.into_iter().next()))
}
pub fn jsonb_path_query_array(
    t: Option<&Jsonb>,
    p: Option<&JsonPath>,
    v: Option<&Jsonb>,
    s: Option<bool>,
    l: PathLimits,
) -> PathResult<Option<Jsonb>> {
    let (Some(t), Some(p), Some(v), Some(s)) = (t, p, v, s) else {
        return Ok(None);
    };
    let seq = suppressed(evaluate(t, p, v, l, s, false, s), s)?.unwrap_or_default();
    let mut limits = t.limits;
    limits.output_bytes = limits.output_bytes.min(l.output_bytes);
    let mut text = String::new();
    push(&mut text, "[", limits).map_err(|_| PathError::Limit)?;
    for (i, n) in seq.iter().enumerate() {
        if i > 0 {
            push(&mut text, ", ", limits).map_err(|_| PathError::Limit)?;
        }
        render(n, &mut text, limits).map_err(|_| PathError::Limit)?;
    }
    push(&mut text, "]", limits).map_err(|_| PathError::Limit)?;
    Jsonb::parse(&text, limits)
        .map(Some)
        .map_err(|_| PathError::Limit)
}

pub fn jsonb_path_exists(
    t: Option<&Jsonb>,
    p: Option<&JsonPath>,
    v: Option<&Jsonb>,
    s: Option<bool>,
    l: PathLimits,
) -> PathResult<Option<bool>> {
    let (Some(t), Some(p), Some(v), Some(s)) = (t, p, v, s) else {
        return Ok(None);
    };
    Ok(suppressed(evaluate(t, p, v, l, false, true, s), s)?.map(|a| !a.is_empty()))
}
pub fn jsonb_path_match(
    t: Option<&Jsonb>,
    p: Option<&JsonPath>,
    v: Option<&Jsonb>,
    s: Option<bool>,
    l: PathLimits,
) -> PathResult<Option<bool>> {
    let (Some(t), Some(p), Some(v), Some(s)) = (t, p, v, s) else {
        return Ok(None);
    };
    let r = evaluate(t, p, v, l, s, false, s).and_then(|a| match a.as_slice() {
        [n] => match n.kind {
            Kind::Bool(b) => Ok(Some(b)),
            Kind::Null => Ok(None),
            _ => Err(PathError::Predicate),
        },
        _ => Err(PathError::Predicate),
    });
    Ok(suppressed(r, s)?.flatten())
}

impl PathError {
    pub fn sqlstate(self) -> Option<&'static str> {
        Some(match self {
            Self::Syntax => "42601",
            Self::Structural => "2203A",
            Self::Object => "2203C",
            Self::Array => "22039",
            Self::Subscript => "22033",
            Self::Variable => "42704",
            Self::VariablesShape => "22023",
            Self::Predicate => "22038",
            Self::Unsupported | Self::Limit => return None,
        })
    }
}
#[cfg(test)]
mod continuation_context_tests {
    use super::*;
    #[test]
    fn nested_context_restores_after_success_and_errors() {
        let limits = PathLimits::default();
        let target = Jsonb::parse(r#"{"a":[1,2]}"#, Limits::default()).unwrap();
        let vars = Jsonb::parse("{}", Limits::default()).unwrap();
        for (text, work, expected) in [
            ("strict $.**.**.a", limits.work, Ok(())),
            (
                "strict $.**.** ? (@ == $missing)",
                limits.work,
                Err(PathError::Variable),
            ),
            ("strict $.**.**.a", 10, Err(PathError::Limit)),
        ] {
            let path = JsonPath::parse(text, limits).unwrap();
            let mut budget = Budget {
                ignore_structural: false,
                throw_errors: true,
                stop_after_first: false,
                depth: 0,
                work: 0,
                limit: PathLimits { work, ..limits },
            };
            let mut out = Vec::new();
            assert_eq!(
                walk(
                    Cow::Borrowed(&target.node),
                    &path.steps,
                    path.mode,
                    &mut out,
                    &mut budget,
                    &target.node,
                    &vars
                ),
                expected
            );
            assert!(!budget.ignore_structural);
            assert_eq!(budget.depth, 0);
        }
    }
}

#[cfg(test)]
mod method_ownership_tests {
    use super::*;
    #[test]
    fn borrowed_subtrees_and_small_owned_scalars() {
        let l = PathLimits::default();
        let target = Jsonb::parse(r#"{"a":[{"b":[1,2,3]}]}"#, Limits::default()).unwrap();
        let vars = Jsonb::parse("{}", Limits::default()).unwrap();
        let path = JsonPath::parse("$.a", l).unwrap();
        let result = evaluate(&target, &path, &vars, l, false, false, false).unwrap();
        assert!(
            matches!(&result[0], Cow::Borrowed(n) if std::ptr::eq(*n, target.node.field("a").unwrap()))
        );
        let path = JsonPath::parse("$.a.size()[0].type()", l).unwrap();
        let result = evaluate(&target, &path, &vars, l, false, false, false).unwrap();
        assert!(
            matches!(&result[0], Cow::Owned(Node {kind: Kind::String(s), ..}) if s == "number")
        );
        let path = JsonPath::parse("$.**.size()[0]", l).unwrap();
        let result = evaluate(&target, &path, &vars, l, false, false, false).unwrap();
        assert!(
            result.iter().all(
                |n| matches!(n, Cow::Owned(Node {kind: Kind::Number(s), ..}) if s.len() <= 20)
            )
        );
        // The only scalar-copy helper rejects subtrees, before cloning them.
        let mut b = Budget {
            ignore_structural: false,
            throw_errors: true,
            stop_after_first: false,
            depth: 0,
            work: 0,
            limit: l,
        };
        assert!(matches!(b.copy_scalar(&target.node), Err(PathError::Limit)));
        assert_eq!(target.text().unwrap(), r#"{"a": [{"b": [1, 2, 3]}]}"#);
    }
}

#[cfg(test)]
mod selector_context_tests {
    use super::*;
    #[test]
    fn predicate_status_context_restores_on_success_variable_and_limit() {
        let l = PathLimits::default();
        let t = Jsonb::parse("{}", Limits::default()).unwrap();
        let vars = Jsonb::parse("{}", Limits::default()).unwrap();
        for throw_errors in [false, true] {
            for stop_after_first in [false, true] {
                for (text, work, expected) in [
                    ("exists($.**[2147483648])", l.work, Ok(Some(false))),
                    ("exists($missing[0,last])", l.work, Err(PathError::Variable)),
                    ("exists($.**[2147483648])", 1, Err(PathError::Limit)),
                ] {
                    let path = JsonPath::parse(text, l).unwrap();
                    let mut b = Budget {
                        ignore_structural: false,
                        throw_errors,
                        stop_after_first,
                        depth: 0,
                        work: 0,
                        limit: PathLimits { work, ..l },
                    };
                    assert_eq!(
                        path.predicate
                            .as_ref()
                            .unwrap()
                            .eval(&t.node, &t.node, &vars, path.mode, &mut b),
                        expected
                    );
                    assert_eq!(b.throw_errors, throw_errors);
                    assert_eq!(b.stop_after_first, stop_after_first);
                    assert!(!b.ignore_structural);
                    assert_eq!(b.depth, 0);
                }
            }
        }
    }
}
