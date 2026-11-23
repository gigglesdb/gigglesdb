//! SELECT targets, relational query layers and clause contexts.
use super::*;

enum ClauseReference {
    Ordinal(usize),
    Name(String),
}

impl ClauseReference {
    fn matches(&self, ordinal: usize, name: &str) -> bool {
        match self {
            Self::Ordinal(index) => *index == ordinal,
            Self::Name(alias) => alias == name,
        }
    }
}

fn sort_expression(n: &p::Node) -> Result<&p::Node> {
    match node(n)? {
        NodeEnum::SortBy(sort) if sort.use_op.is_empty() => {
            sort.node.as_deref().ok_or_else(refuse)
        }
        _ => Err(refuse()),
    }
}

fn clause_reference(
    n: &p::Node,
    scope: &scope::Scope,
    context: &Context,
) -> Result<Option<ClauseReference>> {
    Ok(match node(n)? {
        NodeEnum::AConst(c) => match &c.val {
            Some(p::a_const::Val::Ival(i)) => Some(ClauseReference::Ordinal(
                usize::try_from(i.ival).map_err(|_| refuse())?
                    .checked_sub(1).ok_or_else(refuse)?,
            )),
            _ => None,
        },
        NodeEnum::ColumnRef(c) if c.fields.len() == 1 => {
            let name = strings(&c.fields)?.pop().ok_or_else(refuse)?;
            // GROUP BY resolves an input column ahead of an output alias.
            if *context == Context::GroupBy
                && scope.iter().any(|b| b.unqualified && b.output.name == name)
            {
                None
            } else {
                Some(ClauseReference::Name(name))
            }
        }
        _ => None,
    })
}

impl State<'_> {
    /// Lower one SELECT level with scoped CTE visibility and depth bounds.
    pub(super) fn select(&mut self, s: &p::SelectStmt) -> Result<Vec<scope::Output>> {
        if self.depth >= 64 {
            return Err(refuse());
        }
        self.depth += 1;
        let ctes = self.ctes.clone();
        let result = self.select_body(s);
        self.ctes = ctes;
        self.depth -= 1;
        result
    }
    fn select_body(&mut self, s: &p::SelectStmt) -> Result<Vec<scope::Output>> {
        if s.into_clause.is_some() {
            return Err(Refusal::new(
                NyCode::StatementNotAllowed,
                "SELECT INTO is not allowed",
            ));
        }
        if !s.window_clause.is_empty() {
            return Err(refuse());
        }
        if let Some(with) = &s.with_clause {
            if with.recursive {
                return Err(refuse());
            }
            let mut seen = BTreeSet::new();
            for n in &with.ctes {
                let c = match node(n)? {
                    NodeEnum::CommonTableExpr(c) => c,
                    _ => return Err(refuse()),
                };
                if c.cterecursive
                    || c.search_clause.is_some()
                    || c.cycle_clause.is_some()
                    || !seen.insert(c.ctename.clone())
                {
                    return Err(refuse());
                }
                let query = c.ctequery.as_deref().ok_or_else(refuse)?;
                let select = match node(query)? {
                    NodeEnum::SelectStmt(s) => s,
                    _ => {
                        return Err(Refusal::new(
                            NyCode::StatementNotAllowed,
                            "data-modifying CTE",
                        ));
                    }
                };
                let mut outputs = self.select(select)?;
                scope::rename(&mut outputs, &c.aliascolnames)?;
                self.ctes.insert(c.ctename.clone(), outputs);
            }
        }
        let (scope, outputs) = match p::SetOperation::try_from(s.op).map_err(|_| refuse())? {
            p::SetOperation::SetopNone => {
                if s.larg.is_some() || s.rarg.is_some() {
                    return Err(refuse());
                }
                let mut scope = vec![];
                for n in &s.from_clause {
                    scope.extend(self.bind_from_item(n)?);
                }
                let outputs = if s.values_lists.is_empty() {
                    let previous = self.extract_allowed;
                    self.extract_allowed = self.depth == 1 && !self.view_body && self.view_depth == 0
                        && s.distinct_clause.is_empty() && s.group_clause.is_empty()
                        && s.with_clause.is_none();
                    let result = self.targets(s, &scope);
                    self.extract_allowed = previous;
                    result?
                } else {
                    if !s.target_list.is_empty() || !s.from_clause.is_empty() {
                        return Err(refuse());
                    }
                    let mut columns: Vec<scope::Output> = vec![];
                    for (row_index, row) in s.values_lists.iter().enumerate() {
                        let list = match node(row)? {
                            NodeEnum::List(l) => l,
                            _ => return Err(refuse()),
                        };
                        if row_index > 0 && list.items.len() != columns.len() {
                            return Err(refuse());
                        }
                        for (index, n) in list.items.iter().enumerate() {
                            let mut e = self.root(
                                n,
                                &scope,
                                Context::Values,
                                UseKind::Function {
                                    name: "values".into(),
                                },
                            )?;
                            if e.label != Label::Plain {
                                return Err(refuse());
                            }
                            if row_index == 0 {
                                e.source = None;
                                e.ty = None;
                                columns.push(scope::Output::from_expr(
                                    format!("column{}", index + 1),
                                    e,
                                ));
                            }
                        }
                    }
                    columns
                };
                (scope, outputs)
            }
            p::SetOperation::SetopUnion
            | p::SetOperation::SetopIntersect
            | p::SetOperation::SetopExcept => {
                if !s.target_list.is_empty()
                    || !s.from_clause.is_empty()
                    || !s.values_lists.is_empty()
                    || s.where_clause.is_some()
                    || s.having_clause.is_some()
                    || !s.group_clause.is_empty()
                    || !s.distinct_clause.is_empty()
                    || !s.locking_clause.is_empty()
                {
                    return Err(refuse());
                }
                let left = self.select(s.larg.as_deref().ok_or_else(refuse)?)?;
                let right = self.select(s.rarg.as_deref().ok_or_else(refuse)?)?;
                if left.len() != right.len() {
                    return Err(refuse());
                }
                let mut outputs = vec![];
                for (l, r) in left.into_iter().zip(right) {
                    let mut output = l.clone();
                    output.label = match (&l.label, &r.label) {
                        (Label::Blind(a), Label::Blind(b))
                            if a == b && l.source.is_some() && r.source.is_some() =>
                        {
                            l.label.clone()
                        }
                        (Label::Blind(_), _) | (_, Label::Blind(_)) => Label::Tainted,
                        _ => Label::join(l.label.clone(), r.label.clone()),
                    };
                    output.source = if matches!(output.label, Label::Blind(_))
                        || (output.label == Label::Plain && l.source == r.source)
                    {
                        l.source
                    } else {
                        None
                    };
                    output.inputs = l.inputs.into_iter().chain(r.inputs).collect();
                    if output.ty != r.ty {
                        output.ty = None;
                    }
                    outputs.push(output);
                }
                let scope = outputs
                    .iter()
                    .enumerate()
                    .map(|(ordinal, output)| scope::Bound {
                        schema: String::new(),
                        table: String::new(),
                        output: output.clone(),
                        unqualified: true,
                        qualified: false,
                        ordinal,
                    })
                    .collect();
                (scope, outputs)
            }
            _ => return Err(refuse()),
        };
        if let Some(n) = &s.where_clause {
            self.root(n, &scope, Context::Where, UseKind::Comparison)?;
        }
        if let Some(n) = &s.having_clause {
            self.root(n, &scope, Context::Having, UseKind::Comparison)?;
        }
        for n in &s.group_clause {
            self.clause_item(n, &scope, &outputs, Context::GroupBy, UseKind::GroupBy)?;
        }
        for n in &s.sort_clause {
            self.clause_item(
                sort_expression(n)?,
                &scope,
                &outputs,
                Context::OrderBy,
                UseKind::Order,
            )?;
        }
        for n in &s.distinct_clause {
            if n.node.is_some() {
                self.clause_item(n, &scope, &outputs, Context::OrderBy, UseKind::Order)?;
            } else {
                for output in &outputs {
                    self.record(&expr::Expr::column(output.clone()), UseKind::GroupBy, &Context::GroupBy);
                }
            }
        }
        for n in [&s.limit_count, &s.limit_offset].into_iter().flatten() {
            if self
                .root(n, &scope, Context::Other, UseKind::Comparison)?
                .label
                != Label::Plain
            {
                return Err(refuse());
            }
        }
        for n in &s.locking_clause {
            if !matches!(node(n)?, NodeEnum::LockingClause(_)) {
                return Err(refuse());
            }
        }
        Ok(outputs)
    }
    fn targets(&mut self, select: &p::SelectStmt, scope: &scope::Scope) -> Result<Vec<scope::Output>> {
        let grouped = !select.group_clause.is_empty() || !select.distinct_clause.is_empty();
        let mut order_references = Vec::new();
        for sort in &select.sort_clause {
            if let Some(reference) = clause_reference(sort_expression(sort)?, scope, &Context::OrderBy)? {
                order_references.push(reference);
            }
        }
        let mut outputs = vec![];
        for n in &select.target_list {
            let target = match node(n)? {
                NodeEnum::ResTarget(t) if t.indirection.is_empty() => t,
                _ => return Err(refuse()),
            };
            let n = target.val.as_deref().ok_or_else(refuse)?;
            if let NodeEnum::ColumnRef(c) = node(n)?
                && c.fields
                    .last()
                    .is_some_and(|n| matches!(n.node, Some(NodeEnum::AStar(_))))
            {
                if !target.name.is_empty() || c.fields.len() > 3 {
                    return Err(refuse());
                }
                let qualifier = strings(&c.fields[..c.fields.len() - 1])?;
                let expanded = self.expand_star(&qualifier, scope)?;
                for output in expanded {
                    let e = expr::Expr::column(output.clone());
                    self.record(&e, UseKind::Output, &Context::SelectList);
                    outputs.push(output);
                }
                continue;
            }
            if let Some(extraction) = self.json_extraction(n, scope)? {
                let name = if target.name.is_empty() { "?column?" } else { &target.name };
                if !extraction.unguarded_plain()
                    && (grouped || order_references.iter().any(|r| r.matches(outputs.len(), name)))
                {
                    let (e, rewrite) = extraction.grouping(self, Some(target))?;
                    self.replacements.push(rewrite);
                    self.record(&e, UseKind::Output, &Context::SelectList);
                    outputs.push(scope::Output::from_expr(name.into(), e));
                    continue;
                }
                let (e, action, replacement) =
                    extraction.projection(&self.sql, target.name.is_empty(), target.location)?;
                if let Some(action) = action {
                    if !self.extract_allowed { return Err(json_extract::refusal()); }
                    self.extracts.push((outputs.len(), action));
                }
                if let Some(input) = replacement {
                    if !self.extract_allowed { return Err(json_extract::refusal()); }
                    self.replacements.push(input);
                }
                self.record(&e, UseKind::Output, &Context::SelectList);
                outputs.push(scope::Output::from_expr(if target.name.is_empty() { "?column?".into() } else { target.name.clone() }, e));
                continue;
            }
            let e = self.root(n, scope, Context::SelectList, UseKind::Output)?;
            let default_name = if let NodeEnum::ColumnRef(c) = node(n)? {
                strings(&c.fields)?
                    .last()
                    .cloned()
                    .unwrap_or_else(|| "?column?".into())
            } else {
                "?column?".into()
            };
            outputs.push(scope::Output::from_expr(
                if target.name.is_empty() {
                    default_name
                } else {
                    target.name.clone()
                },
                e,
            ));
        }
        Ok(outputs)
    }
    /// Recognize only the exact top-level explicit-tokenization shape.
    pub(super) fn tokenize(&mut self, s: &p::SelectStmt) -> Result<bool> {
        if s.target_list.len() != 1 {
            return Ok(false);
        }
        let target = match node(&s.target_list[0])? {
            NodeEnum::ResTarget(t) => t,
            _ => return Err(refuse()),
        };
        let f = match node(target.val.as_deref().ok_or_else(refuse)?)? {
            NodeEnum::FuncCall(f) => f,
            _ => return Ok(false),
        };
        let names = strings(&f.funcname)?;
        if names.last().is_none_or(|n| n != "nunya_tokenize") {
            return Ok(false);
        }
        if !matches!(names.as_slice(), [n] if n == "nunya_tokenize")
            && !matches!(names.as_slice(), [s,n] if s == "nunya" && n == "nunya_tokenize")
        {
            return Err(refuse());
        }
        if s.group_distinct
            || s.with_clause.is_some()
            || !s.window_clause.is_empty()
            || !s.values_lists.is_empty()
            || s.larg.is_some()
            || s.rarg.is_some()
            || s.op != p::SetOperation::SetopNone as i32
            || s.into_clause.is_some()
            || !s.from_clause.is_empty()
            || s.where_clause.is_some()
            || !s.group_clause.is_empty()
            || s.having_clause.is_some()
            || !s.sort_clause.is_empty()
            || !s.distinct_clause.is_empty()
            || s.limit_count.is_some()
            || s.limit_offset.is_some()
            || !s.locking_clause.is_empty()
            || !target.indirection.is_empty()
            || f.args.len() != 2
            || !f.agg_order.is_empty()
            || f.agg_filter.is_some()
            || f.over.is_some()
            || f.agg_within_group
            || f.agg_star
            || f.agg_distinct
            || f.func_variadic
        {
            return Err(refuse());
        }
        let domain = match node(&f.args[0])? {
            NodeEnum::AConst(c) => match &c.val {
                Some(p::a_const::Val::Sval(v)) if !c.isnull => v.sval.clone(),
                _ => return Err(refuse()),
            },
            _ => return Err(refuse()),
        };
        // Validate literal spelling even for the domain, independent of parser decoding.
        self.expression(&f.args[0], &vec![], &Context::Other)?;
        let supported = matches!(node(&f.args[1])?, NodeEnum::ParamRef(_))
            || matches!(node(&f.args[1])?, NodeEnum::AConst(c) if matches!(c.val, Some(p::a_const::Val::Sval(_))) && !c.isnull);
        if !supported {
            return Err(refuse());
        }
        let arg = self.expression(&f.args[1], &vec![], &Context::Other)?;
        self.plain.extend(arg.params.iter().copied());
        self.statement.kind = StatementKind::Tokenize {
            domain: crate::policy::DomainName(domain),
            arg: arg.value,
        };
        Ok(true)
    }
    fn clause_item(
        &mut self,
        n: &p::Node,
        scope: &scope::Scope,
        outputs: &[scope::Output],
        context: Context,
        kind: UseKind,
    ) -> Result<()> {
        let output = match clause_reference(n, scope, &context)? {
            Some(ClauseReference::Ordinal(index)) => Some(outputs.get(index).ok_or_else(refuse)?),
            Some(ClauseReference::Name(name)) => {
                let mut matches = outputs.iter().filter(|o| o.name == name);
                let output = matches.next();
                if matches.next().is_some() {
                    return Err(refuse());
                }
                output
            }
            None => None,
        };
        if let Some(o) = output {
            self.record(&expr::Expr::column(o.clone()), kind, &context);
        } else {
            self.root(n, scope, context, kind)?;
        }
        Ok(())
    }
}
