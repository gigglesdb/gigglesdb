use super::*;
#[derive(Clone, Debug)]
pub(super) enum Predicate {
    Compare(JsonPath, String, JsonPath),
    Exists(JsonPath),
    Not(Box<Self>),
    Unknown(Box<Self>),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}
static TRUE: Node = Node {
    span: 0..0,
    kind: Kind::Bool(true),
};
static FALSE: Node = Node {
    span: 0..0,
    kind: Kind::Bool(false),
};
static UNKNOWN: Node = Node {
    span: 0..0,
    kind: Kind::Null,
};
pub(super) fn bool_node(v: Option<bool>) -> &'static Node {
    match v {
        Some(true) => &TRUE,
        Some(false) => &FALSE,
        None => &UNKNOWN,
    }
}
// Scan only outside strings and delimiters. Reject malformed nesting before recursion.
fn operators(s: &str) -> PathResult<Vec<usize>> {
    let mut stack = Vec::new();
    let mut quoted = false;
    let mut escape = false;
    let mut out = Vec::new();
    for (i, c) in s.char_indices() {
        if quoted {
            if escape {
                escape = false;
            } else if c == '\\' {
                escape = true;
            } else if c == '"' {
                quoted = false;
            }
            continue;
        }
        if c == '"' {
            quoted = true;
            continue;
        }
        match c {
            '(' | '[' => {
                stack.push(c);
                if stack.len() > 64 {
                    return Err(PathError::Limit);
                }
            }
            ')' | ']' => {
                if stack.pop() != Some(if c == ')' { '(' } else { '[' }) {
                    return Err(PathError::Syntax);
                }
            }
            _ if stack.is_empty() => out.push(i),
            _ => {}
        }
    }
    if quoted || !stack.is_empty() {
        return Err(PathError::Syntax);
    }
    Ok(out)
}
pub(super) fn closing(s: &str) -> PathResult<usize> {
    let mut depth = 0usize;
    let mut quote = false;
    let mut escape = false;
    for (i, c) in s.char_indices() {
        if quote {
            if escape {
                escape = false
            } else if c == '\\' {
                escape = true
            } else if c == '"' {
                quote = false
            }
            continue;
        }
        match c {
            '"' => quote = true,
            '(' => {
                depth += 1;
                if depth > 64 {
                    return Err(PathError::Limit);
                }
            }
            ')' => {
                depth = depth.checked_sub(1).ok_or(PathError::Syntax)?;
                if depth == 0 {
                    return Ok(i);
                }
            }
            _ => {}
        }
    }
    Err(PathError::Syntax)
}
pub(super) fn parse_predicate(
    s: &str,
    l: PathLimits,
    depth: usize,
) -> PathResult<Option<Predicate>> {
    if depth >= l.steps.min(64) {
        return Err(PathError::Limit);
    }
    let s = s.trim();
    let positions = operators(s)?;
    let operand = |t: &str| {
        if t.trim().is_empty() {
            return Err(PathError::Syntax);
        }
        let p = JsonPath::parse(t, l)?;
        if p.predicate.is_some() {
            return Err(PathError::Unsupported);
        }
        Ok(p)
    };
    let sub = |t: &str| parse_predicate(t, l, depth + 1)?.ok_or(PathError::Syntax);
    for (token, and) in [("||", false), ("&&", true)] {
        if let Some(i) = positions
            .iter()
            .copied()
            .find(|&i| s[i..].starts_with(token))
        {
            let a = Box::new(sub(&s[..i])?);
            let b = Box::new(sub(&s[i + 2..])?);
            return Ok(Some(if and {
                Predicate::And(a, b)
            } else {
                Predicate::Or(a, b)
            }));
        }
    }
    if s.starts_with('(') {
        let end = closing(s)?;
        if end == s.len() - 1 {
            return Ok(Some(sub(&s[1..end])?));
        }
        if s[end + 1..].trim() == "is unknown" {
            return Ok(Some(Predicate::Unknown(Box::new(sub(&s[1..end])?))));
        }
    }
    if let Some(t) = s.strip_prefix('!') {
        let t = t.trim();
        if !t.starts_with('(') && !t.starts_with("exists") {
            return Err(PathError::Syntax);
        }
        return Ok(Some(Predicate::Not(Box::new(sub(t)?))));
    }
    if let Some(t) = s.strip_prefix("exists") {
        let t = t.trim();
        if t.starts_with('(') && closing(t)? == t.len() - 1 {
            return Ok(Some(Predicate::Exists(operand(&t[1..t.len() - 1])?)));
        }
    }
    for i in positions {
        for op in ["==", "!=", "<>", "<=", ">=", "<", ">"] {
            if s[i..].starts_with(op) {
                return Ok(Some(Predicate::Compare(
                    operand(s[..i].trim())?,
                    op.into(),
                    operand(s[i + op.len()..].trim())?,
                )));
            }
        }
    }
    Ok(None)
}
fn structural(e: PathError) -> bool {
    matches!(
        e,
        PathError::Structural | PathError::Object | PathError::Array | PathError::Subscript
    )
}
fn sequence<'a>(
    p: &'a JsonPath,
    current: &'a Node,
    root: &'a Node,
    vars: &'a Jsonb,
    mode: Mode,
    b: &mut Budget,
    unwrap: bool,
) -> PathResult<Vec<Cow<'a, Node>>> {
    if let Some(pred) = &p.predicate {
        let value = pred.eval(current, root, vars, mode, b)?;
        let mut out = Vec::new();
        b.push_value(&mut out, Cow::Borrowed(bool_node(value)))?;
        return Ok(out);
    }
    let n = if let Some(n) = &p.literal {
        n
    } else if p.current {
        current
    } else if let Some(v) = &p.variable {
        vars.node.field(v).ok_or(PathError::Variable)?
    } else {
        root
    };
    let mut seq = Vec::new();
    let saved_stop = b.stop_after_first;
    let saved_throw = b.throw_errors;
    b.throw_errors = false;
    b.stop_after_first = !unwrap && mode == Mode::Lax;
    let result = walk(Cow::Borrowed(n), &p.steps, mode, &mut seq, b, root, vars);
    b.stop_after_first = saved_stop;
    b.throw_errors = saved_throw;
    result?;
    if !unwrap || mode == Mode::Strict {
        return Ok(seq);
    }
    let mut out = Vec::new();
    for n in seq {
        b.visit()?;
        match n {
            Cow::Borrowed(Node {
                kind: Kind::Array(a),
                ..
            }) => {
                for n in a {
                    b.push_value(&mut out, Cow::Borrowed(n))?;
                }
            }
            n => b.push_value(&mut out, n)?,
        }
    }
    Ok(out)
}
impl Predicate {
    pub(super) fn eval(
        &self,
        current: &Node,
        root: &Node,
        vars: &Jsonb,
        mode: Mode,
        b: &mut Budget,
    ) -> PathResult<Option<bool>> {
        if b.depth >= 128 {
            return Err(PathError::Limit);
        }
        b.depth += 1;
        let result = self.eval_inner(current, root, vars, mode, b);
        b.depth -= 1;
        result
    }
    fn eval_inner(
        &self,
        current: &Node,
        root: &Node,
        vars: &Jsonb,
        mode: Mode,
        b: &mut Budget,
    ) -> PathResult<Option<bool>> {
        b.visit()?;
        match self {
            Self::Not(p) => Ok(p.eval(current, root, vars, mode, b)?.map(|v| !v)),
            Self::Unknown(p) => Ok(Some(p.eval(current, root, vars, mode, b)?.is_none())),
            Self::And(a, c) | Self::Or(a, c) => {
                let and = matches!(self, Self::And(..));
                let a = a.eval(current, root, vars, mode, b)?;
                if a == Some(!and) {
                    return Ok(a);
                }
                let c = c.eval(current, root, vars, mode, b)?;
                Ok(if c == Some(and) { a } else { c })
            }
            Self::Exists(p) => match sequence(p, current, root, vars, mode, b, false) {
                Ok(v) => Ok(Some(!v.is_empty())),
                Err(e) if structural(e) => Ok(None),
                Err(e) => Err(e),
            },
            Self::Compare(a, op, c) => {
                let a = match sequence(a, current, root, vars, mode, b, true) {
                    Ok(v) => v,
                    Err(e) if structural(e) => return Ok(None),
                    Err(e) => return Err(e),
                };
                let c = match sequence(c, current, root, vars, mode, b, true) {
                    Ok(v) => v,
                    Err(e) if structural(e) => return Ok(None),
                    Err(e) => return Err(e),
                };
                let mut unknown = false;
                let mut found = false;
                for a in a {
                    for c in &c {
                        b.visit()?;
                        let cmp = match (&a.kind, &c.kind) {
                            (Kind::Null, Kind::Null) => Some(std::cmp::Ordering::Equal),
                            (Kind::Null, _) | (_, Kind::Null) => {
                                if op == "!=" || op == "<>" {
                                    Some(std::cmp::Ordering::Less)
                                } else {
                                    continue;
                                }
                            }
                            (Kind::Bool(a), Kind::Bool(c)) => Some(a.cmp(c)),
                            (Kind::String(a), Kind::String(c)) => {
                                Some(a.as_bytes().cmp(c.as_bytes()))
                            }
                            (Kind::Number(a), Kind::Number(c)) => Some(
                                Numeric::parse(a)
                                    .map_err(|_| PathError::Limit)?
                                    .cmp(&Numeric::parse(c).map_err(|_| PathError::Limit)?),
                            ),
                            _ => None,
                        };
                        let Some(cmp) = cmp else {
                            if mode == Mode::Strict {
                                return Ok(None);
                            }
                            unknown = true;
                            continue;
                        };
                        let yes = match op.as_str() {
                            "==" => cmp.is_eq(),
                            "!=" | "<>" => !cmp.is_eq(),
                            "<" => cmp.is_lt(),
                            ">" => cmp.is_gt(),
                            "<=" => !cmp.is_gt(),
                            ">=" => !cmp.is_lt(),
                            _ => unreachable!(),
                        };
                        if yes {
                            if mode == Mode::Lax {
                                return Ok(Some(true));
                            }
                            found = true;
                        }
                    }
                }
                Ok(if found {
                    Some(true)
                } else if unknown {
                    None
                } else {
                    Some(false)
                })
            }
        }
    }
}
