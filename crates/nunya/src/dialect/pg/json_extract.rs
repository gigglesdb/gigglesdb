//! Direct SELECT JSON extraction, retaining the original JSON root type.
use super::*;
use crate::json::extract::{self, Extraction, Step};

pub(super) fn refusal() -> Refusal {
    Refusal::new(NyCode::MaskedExpression, "JSON extraction violates policy")
}

pub(super) struct Extract {
    source: ColumnRef,
    derived: bool,
    treatment: Extraction,
    base: ColumnType,
    span: Span,
    op: usize,
    text: bool,
    path: Vec<Step>,
    root: String,
    guards: Vec<String>,
}

impl Extract {
    pub(super) fn unguarded_plain(&self) -> bool { self.is_plain() && self.guards.is_empty() }

    pub(super) fn is_plain(&self) -> bool {
        self.treatment == Extraction::Plain
    }

    pub(super) fn plain_expression(&self) -> expr::Expr {
        let mut e = expr::Expr::plain(ValueSource::Computed(Label::Plain));
        e.inputs = vec![(self.source.clone(), Label::Plain)];
        e.ty = Some(if self.text {
            ColumnType::Text
        } else {
            self.base.clone()
        });
        e
    }

    pub(super) fn predicate(self, sql: &str) -> Result<(expr::Expr, InputRewrite)> {
        use crate::policy::{JsonLeafType, JsonPathTreatment, JsonTokenType};
        if self.derived {
            return Err(refusal());
        }
        if self.is_plain() {
            let e = self.plain_expression();
            let span = self.span.clone();
            let original = sql.get(span.start..span.end).ok_or_else(refusal)?;
            return Ok((e, InputRewrite::ReplaceSpan { span, text: format!("(CASE WHEN {} THEN {original} END)", self.guards.join(" AND ")) }));
        }
        let Extraction::Filter(ref filter) = self.treatment else {
            return Err(refusal());
        };
        let [rule] = filter.rules.as_slice() else {
            return Err(refusal());
        };
        if !rule.path.is_empty() {
            return Err(refusal());
        }
        let (kind, domain) = match &rule.treatment {
            JsonPathTreatment::Show { ty } => (
                match ty {
                    JsonLeafType::String => "string",
                    JsonLeafType::Number => "number",
                    JsonLeafType::Boolean => "boolean",
                },
                None,
            ),
            JsonPathTreatment::Blind {
                domain,
                ty: JsonTokenType::Text,
            } if self.text => ("string", Some(domain.clone())),
            _ => return Err(refusal()),
        };
        let mut e = self.plain_expression();
        if let Some(domain) = domain {
            e.label = Label::Blind(domain.clone());
            e.value = ValueSource::JsonLeaf(domain);
            e.inputs = vec![(self.source.clone(), e.label.clone())];
            e.ty = Some(ColumnType::Text);
            e.bare = true;
        }
        let span = self.span.clone();
        let original = sql.get(span.start..span.end).ok_or_else(refusal)?;
        let json = if self.text {
            format!(
                "{}{}{}",
                sql.get(span.start..self.op).ok_or_else(refusal)?,
                if sql.get(self.op..).is_some_and(|s| s.starts_with("->>")) {
                    "->"
                } else {
                    "#>"
                },
                sql.get(self.op + 3..span.end).ok_or_else(refusal)?
            )
        } else {
            original.to_owned()
        };
        let function = if self.base == ColumnType::Jsonb {
            "jsonb_typeof"
        } else {
            "json_typeof"
        };
        let mut checks = Vec::new();
        let mut prefix = self.root;
        for step in &self.path {
            let (shape, operand) = match step {
                Step::Key(key) => ("object", format!("'{}'", key.replace("'", "''"))),
                Step::Index(index) => ("array", index.to_string()),
            };
            checks.push(format!("pg_catalog.{function}({prefix}) = '{shape}'"));
            prefix = format!("({prefix} -> {operand})");
        }
        checks.push(format!("pg_catalog.{function}({json}) = '{kind}'"));
        Ok((
            e,
            InputRewrite::ReplaceSpan {
                span,
                text: format!("(CASE WHEN {} THEN {original} END)", checks.join(" AND ")),
            },
        ))
    }

    pub(super) fn grouping(
        mut self,
        state: &State<'_>,
        target: Option<&p::ResTarget>,
    ) -> Result<(expr::Expr, InputRewrite)> {
        if self.derived || state.depth != 1 || state.view_body || state.view_depth != 0 {
            return Err(refusal());
        }
        // A plain extraction (an unlisted path under a show-mode filter) is shown
        // data; its container guards are kept by the predicate rewrite below.
        if !self.is_plain() {
            let filter = state.policy.json_filter(&self.source).ok_or_else(refusal)?;
            extract::require_show_leaf(&self.path, filter, &self.treatment)?;
        }
        if let Some(target) = target {
            self.target_extent(&state.sql, target.location)?;
        }
        let (e, mut rewrite) = self.predicate(&state.sql)?;
        if target.is_some_and(|t| t.name.is_empty())
            && let InputRewrite::ReplaceSpan { text, .. } = &mut rewrite
        {
            text.push_str(" AS \"?column?\"");
        }
        Ok((e, rewrite))
    }

    fn target_extent(&mut self, sql: &str, start: i32) -> Result<()> {
        self.span.start = usize::try_from(start).map_err(|_| refusal())?;
        self.span.end = expression_end(sql, self.span.start, self.span.end)?;
        Ok(())
    }

    pub(super) fn projection(
        mut self,
        sql: &str,
        alias: bool,
        start: i32,
    ) -> Result<(expr::Expr, Option<OutputRewrite>, Option<InputRewrite>)> {
        self.target_extent(sql, start)?;
        let mut e = self.plain_expression();
        let action = match self.treatment {
            Extraction::Plain => {
                let replacement = if self.guards.is_empty() { None } else {
                    let original = sql.get(self.span.start..self.span.end).ok_or_else(refusal)?;
                    Some(InputRewrite::ReplaceSpan { span: self.span, text: format!("(CASE WHEN {} THEN {original} END){}", self.guards.join(" AND "), if alias { " AS \"?column?\"" } else { "" }) })
                };
                return Ok((e, None, replacement));
            },
            Extraction::Masked => OutputRewrite::Mask { placeholder: None },
            Extraction::Filter(filter) => OutputRewrite::JsonExtract {
                filter,
                base: self.base,
                text: self.text,
                keep_empty: matches!(self.path.last(), Some(Step::Index(_))),
            },
        };
        if self.derived {
            return Err(refusal());
        }
        e.inputs = vec![(self.source, Label::Masked)];
        e.label = Label::Masked;
        e.value = ValueSource::Computed(Label::Masked);
        let mut replacement = if self.text && matches!(action, OutputRewrite::JsonExtract { .. }) {
            let before = sql.get(self.span.start..self.op).ok_or_else(refusal)?;
            let op = if sql.get(self.op..).is_some_and(|s| s.starts_with("->>")) {
                "->"
            } else {
                "#>"
            };
            let after = sql.get(self.op + 3..self.span.end).ok_or_else(refusal)?;
            Some(InputRewrite::ReplaceSpan {
                span: self.span.clone(),
                text: format!(
                    "(({before}{op}{after})::text){}",
                    if alias { " AS \"?column?\"" } else { "" }
                ),
            })
        } else {
            None
        };
        if !self.guards.is_empty() {
            let value = match replacement.take() {
                Some(InputRewrite::ReplaceSpan { text, .. }) => text.strip_suffix(" AS \"?column?\"").unwrap_or(&text).to_owned(),
                _ => sql.get(self.span.start..self.span.end).ok_or_else(refusal)?.to_owned(),
            };
            replacement = Some(InputRewrite::ReplaceSpan { span: self.span, text: format!("(CASE WHEN {} THEN {value} END){}", self.guards.join(" AND "), if alias { " AS \"?column?\"" } else { "" }) });
        }
        Ok((e, Some(action), replacement))
    }
}

// Parentheses are absent from PostgreSQL's AST. Include their original
// boundaries when replacing a target, so an inserted alias stays outside them.
fn expression_end(sql: &str, start: usize, end: usize) -> Result<usize> {
    let bytes = sql.as_bytes();
    let mut i = start;
    let mut depth = 0usize;
    while i < end {
        match bytes.get(i) {
            Some(b'\'' | b'"') => {
                let quote = bytes[i];
                i += 1;
                loop {
                    let b = *bytes.get(i).ok_or_else(refusal)?;
                    i += 1;
                    if b == quote {
                        if bytes.get(i) == Some(&quote) {
                            i += 1;
                        } else {
                            break;
                        }
                    }
                }
            }
            Some(b'(') => {
                depth += 1;
                i += 1;
            }
            Some(b')') => {
                depth = depth.checked_sub(1).ok_or_else(refusal)?;
                i += 1;
            }
            Some(b'-') if bytes.get(i + 1) == Some(&b'-') => return Err(refusal()),
            Some(b'/') if bytes.get(i + 1) == Some(&b'*') => return Err(refusal()),
            Some(_) => i += 1,
            None => return Err(refusal()),
        }
    }
    while depth > 0 {
        while bytes.get(i).is_some_and(u8::is_ascii_whitespace) {
            i += 1;
        }
        if bytes.get(i) != Some(&b')') {
            return Err(refusal());
        }
        i += 1;
        depth -= 1;
    }
    Ok(i)
}

struct Chain<'a> {
    base: &'a p::ColumnRef,
    operands: Vec<(&'a p::AExpr, String)>,
}

fn chain(mut n: &p::Node) -> Result<Option<Chain<'_>>> {
    let mut operands = Vec::new();
    let base = loop {
        match node(n)? {
            NodeEnum::ColumnRef(c) => break c,
            NodeEnum::AExpr(a) if a.kind == p::AExprKind::AexprOp as i32 => {
                let names = strings(&a.name)?;
                let op = match names.as_slice() {
                    [op] if matches!(op.as_str(), "->" | "->>" | "#>" | "#>>") => op.clone(),
                    _ => return Ok(None),
                };
                operands.push((a.as_ref(), op));
                n = a.lexpr.as_deref().ok_or_else(refusal)?;
            }
            _ => return Ok(None),
        }
    };
    operands.reverse();
    if operands
        .iter()
        .take(operands.len().saturating_sub(1))
        .any(|(_, op)| op.ends_with(">>"))
    {
        return Err(refusal());
    }
    Ok(Some(Chain { base, operands }))
}

// Scan an AConst using its lexical spelling rather than trusting the AST's
// already-decoded string: E strings, dollar strings and implicit concatenation
// must never enter the extraction normalizer.
fn operand<'a>(sql: &'a str, n: &p::Node) -> Result<(&'a str, usize)> {
    let c = match node(n)? {
        NodeEnum::AConst(c) if !c.isnull => c,
        _ => return Err(refusal()),
    };
    let start = usize::try_from(c.location).map_err(|_| refusal())?;
    let tail = sql.get(start..).ok_or_else(refusal)?;
    if tail.starts_with('\'') {
        let bytes = tail.as_bytes();
        let mut i = 1;
        loop {
            match bytes.get(i) {
                Some(b'\'') if bytes.get(i + 1) == Some(&b'\'') => i += 2,
                Some(b'\'') => {
                    i += 1;
                    let rest = tail.get(i..).ok_or_else(refusal)?;
                    let trimmed = rest.trim_start();
                    if trimmed.starts_with('\'')
                        || trimmed.starts_with("/*")
                        || trimmed.starts_with("--")
                    {
                        return Err(refusal());
                    }
                    return Ok((&tail[..i], start + i));
                }
                Some(_) => i += 1,
                None => return Err(refusal()),
            }
        }
    }
    match &c.val {
        Some(p::a_const::Val::Ival(_)) | Some(p::a_const::Val::Fval(_)) => {
            let (span, _) = lexer::literal_extent(sql, start).ok_or_else(refusal)?;
            Ok((sql.get(span.start..span.end).ok_or_else(refusal)?, span.end))
        }
        _ => Err(refusal()),
    }
}

impl State<'_> {
    pub(super) fn json_extraction(
        &self,
        n: &p::Node,
        scope: &scope::Scope,
    ) -> Result<Option<Extract>> {
        let Some(chain) = chain(n)? else {
            return Ok(None);
        };
        if chain.operands.is_empty() {
            return Ok(None);
        }
        let output = self.resolve(&chain.base.fields, scope)?;
        let Some(source) = output.source else {
            return Ok(None);
        };
        let Some(filter) = self.policy.json_filter(&source) else {
            return Ok(None);
        };
        let base = match output.ty {
            Some(ty @ (ColumnType::Json | ColumnType::Jsonb)) => ty,
            _ => return Err(refusal()),
        };
        let mut path: Vec<Step> = vec![];
        let mut end = 0;
        let mut op_location = 0;
        let mut text = false;
        let mut guarded_depths = Vec::new();
        let mut root_end = None;
        for (a, op) in chain.operands {
            if self.snapshot.user_routines.contains(&op) {
                return Err(refusal());
            }
            let (raw, rhs_end) = operand(&self.sql, a.rexpr.as_deref().ok_or_else(refusal)?)?;
            let previous = path.len();
            extract::normalize(&mut path, &op, raw, filter)?;
            if op.starts_with('#') {
                for depth in previous..path.len() {
                    let numeric = match &path[depth] { Step::Index(_) => true, Step::Key(k) => {
                        if extract::pg_leading_whitespace(k) { return Err(refusal()); }
                        k.parse::<i32>().is_ok()
                    } };
                    if numeric && filter.rules.iter().any(|r| r.path.len() > depth && path[..depth].iter().zip(&r.path).all(|(s, p)| match (s,p) { (Step::Key(a), crate::policy::PathSegment::Key(b)) => a == b, (Step::Index(_), crate::policy::PathSegment::AnyElement) => true, _ => false })) {
                        guarded_depths.push(depth);
                    }
                }
            }
            root_end.get_or_insert(usize::try_from(a.location).map_err(|_| refusal())?);
            end = rhs_end;
            text = op.ends_with(">>");
            op_location = usize::try_from(a.location).map_err(|_| refusal())?;
        }
        let root_end = root_end.ok_or_else(refusal)?;
        let root_start = usize::try_from(chain.base.location).map_err(|_| refusal())?;
        let mut start = root_start;
        while expression_end(&self.sql, start, end).is_err() {
            let before = self.sql.get(..start).ok_or_else(refusal)?.trim_end();
            start = before.len().checked_sub(1).ok_or_else(refusal)?;
            if self.sql.as_bytes().get(start) != Some(&b'(') {
                return Err(refusal());
            }
        }
        let root = self.sql.get(root_start..root_end).ok_or_else(refusal)?.trim_end_matches(|c: char| c.is_ascii_whitespace() || c == ')').to_owned();
        let function = if base == ColumnType::Jsonb { "jsonb_typeof" } else { "json_typeof" };
        let mut prefix = root.clone();
        let mut guards = Vec::new();
        for (depth, step) in path.iter().enumerate() {
            let (shape, operand) = match step { Step::Key(k) => ("object", format!("'{}'", k.replace("'", "''"))), Step::Index(i) => ("array", i.to_string()) };
            if guarded_depths.contains(&depth) { guards.push(format!("pg_catalog.{function}({prefix}) = '{shape}'")); }
            prefix = format!("({prefix} -> {operand})");
        }
        Ok(Some(Extract {
            source,
            derived: output.derived,
            treatment: extract::classify(&path, filter)?,
            base,
            span: Span { start, end },
            op: op_location,
            text,
            path,
            root: self
                .sql
                .get(root_start..root_end)
                .ok_or_else(refusal)?
                .trim_end_matches(|c: char| c.is_ascii_whitespace() || c == ')')
                .to_owned(),
            guards,
        }))
    }
}
