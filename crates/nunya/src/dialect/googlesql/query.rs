use super::*;
impl State<'_> {
    /// Lower a correlated expression subquery.
    pub(super) fn nested(&mut self, q: &a::Query, scope: &Scope) -> Result<Vec<Output>> {
        self.outers.push(scope.clone());
        let r = self.query(q);
        self.outers.pop();
        r
    }
    fn query(&mut self, q: &a::Query) -> Result<Vec<Output>> {
        if self.depth >= 64 {
            return Err(refuse());
        }
        self.depth += 1;
        let ctes = self.ctes.clone();
        let r = self.query_body(q);
        self.ctes = ctes;
        self.depth -= 1;
        r
    }
    fn query_body(&mut self, q: &a::Query) -> Result<Vec<Output>> {
        if !q.limit_by.is_empty()
            || q.fetch.is_some()
            || !q.locks.is_empty()
            || q.for_clause.is_some()
            || q.settings.is_some()
            || q.format_clause.is_some()
        {
            return Err(refuse());
        }
        if let Some(w) = &q.with {
            if w.recursive {
                return Err(refuse());
            }
            let mut seen = BTreeSet::new();
            for c in &w.cte_tables {
                if c.from.is_some()
                    || c.materialized.is_some()
                    || !seen.insert(c.alias.name.value.to_lowercase())
                {
                    return Err(refuse());
                }
                let os = self.query(&c.query)?;
                let bs = Self::bind(os, String::new(), String::new(), Some(&c.alias))?;
                self.ctes.insert(
                    c.alias.name.value.to_lowercase(),
                    bs.into_iter().map(|b| b.output).collect(),
                );
            }
        }
        let (scope, outputs) = self.set(&q.body)?;
        if let Some(order) = &q.order_by {
            if order.interpolate.is_some() {
                return Err(refuse());
            }
            for o in &order.exprs {
                if o.with_fill.is_some() {
                    return Err(refuse());
                }
                self.clause(&o.expr, &scope, &outputs, Context::OrderBy, UseKind::Order)?
            }
        }
        for n in q.limit.iter().chain(q.offset.iter().map(|o| &o.value)) {
            if self
                .root(n, &scope, Context::Other, UseKind::Comparison)?
                .label
                != Label::Plain
            {
                return Err(refuse());
            }
        }
        Ok(outputs)
    }
    fn set(&mut self, set: &a::SetExpr) -> Result<(Scope, Vec<Output>)> {
        match set {
            a::SetExpr::Select(s) => self.select(s),
            a::SetExpr::Query(q) => {
                let os = self.query(q)?;
                Ok((
                    Self::bind(os.clone(), String::new(), String::new(), None)?,
                    os,
                ))
            }
            a::SetExpr::SetOperation {
                left,
                right,
                set_quantifier,
                ..
            } => {
                if !matches!(
                    set_quantifier,
                    a::SetQuantifier::All | a::SetQuantifier::Distinct | a::SetQuantifier::None
                ) {
                    return Err(refuse());
                }
                let (_, l) = self.set(left)?;
                let (_, r) = self.set(right)?;
                if l.len() != r.len() {
                    return Err(refuse());
                }
                let mut os = vec![];
                for (mut l, r) in l.into_iter().zip(r) {
                    let label = match (&l.ir.label, &r.ir.label) {
                        (Label::Blind(a), Label::Blind(b))
                            if a == b && l.ir.source.is_some() && r.ir.source.is_some() =>
                        {
                            l.ir.label.clone()
                        }
                        (Label::Blind(_), _) | (_, Label::Blind(_)) => Label::Tainted,
                        _ => Label::join(l.ir.label.clone(), r.ir.label.clone()),
                    };
                    if !matches!(label, Label::Blind(_))
                        && !(label == Label::Plain && l.ir.source == r.ir.source)
                    {
                        l.ir.source = None
                    }
                    l.ir.label = label;
                    if l.ir.ty != r.ir.ty {
                        l.ir.ty = None
                    }
                    l.inputs.extend(r.inputs);
                    os.push(l)
                }
                Ok((
                    Self::bind(os.clone(), String::new(), String::new(), None)?,
                    os,
                ))
            }
            _ => Err(refuse()),
        }
    }
    fn clause(
        &mut self,
        e: &a::Expr,
        scope: &Scope,
        os: &[Output],
        c: Context,
        k: UseKind,
    ) -> Result<()> {
        let output = match e {
            a::Expr::Value(a::Value::Number(n, _)) => Some(
                os.get(
                    n.parse::<usize>()
                        .map_err(|_| refuse())?
                        .checked_sub(1)
                        .ok_or_else(refuse)?,
                )
                .ok_or_else(refuse)?
                .clone(),
            ),
            a::Expr::Identifier(i)
                if !(c == Context::GroupBy
                    && scope.iter().any(|b| {
                        b.visible && b.output.ir.name.to_lowercase() == i.value.to_lowercase()
                    })) =>
            {
                let mut matches = os
                    .iter()
                    .filter(|o| o.ir.name.to_lowercase() == i.value.to_lowercase());
                let o = matches.next().cloned();
                if matches.next().is_some() {
                    return Err(refuse());
                }
                o
            }
            _ => None,
        };
        if let Some(o) = output {
            self.record(&expr::Expr::column(o), k, &c)
        } else {
            self.root(e, scope, c, k)?;
        }
        Ok(())
    }
    fn select(&mut self, s: &a::Select) -> Result<(Scope, Vec<Output>)> {
        if s.top.is_some()
            || s.into.is_some()
            || !s.lateral_views.is_empty()
            || s.prewhere.is_some()
            || !s.cluster_by.is_empty()
            || !s.distribute_by.is_empty()
            || !s.sort_by.is_empty()
            || !s.named_window.is_empty()
            || s.qualify.is_some()
            || s.value_table_mode.is_some()
            || s.connect_by.is_some()
        {
            return Err(refuse());
        }
        if !matches!(s.distinct, None | Some(a::Distinct::Distinct)) {
            return Err(refuse());
        }
        let mut scope = vec![];
        for t in &s.from {
            self.outers.push(scope.clone());
            let result = self.from(t);
            self.outers.pop();
            scope.extend(result?)
        }
        let mut os = vec![];
        for item in &s.projection {
            match item {
                a::SelectItem::UnnamedExpr(e) | a::SelectItem::ExprWithAlias { expr: e, .. } => {
                    let e = self.root(e, &scope, Context::SelectList, UseKind::Output)?;
                    let name = match item {
                        a::SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                        a::SelectItem::UnnamedExpr(a::Expr::Identifier(i)) => i.value.clone(),
                        a::SelectItem::UnnamedExpr(a::Expr::CompoundIdentifier(ids)) => {
                            ids.last().ok_or_else(refuse)?.value.clone()
                        }
                        _ => "?column?".into(),
                    };
                    os.push(e.output(name))
                }
                a::SelectItem::Wildcard(opts) | a::SelectItem::QualifiedWildcard(_, opts) => {
                    if *opts != a::WildcardAdditionalOptions::default() {
                        return Err(refuse());
                    }
                    let qualifiers = match item {
                        a::SelectItem::QualifiedWildcard(n, _) => {
                            n.0.iter()
                                .map(|i| i.value.to_lowercase())
                                .collect::<Vec<_>>()
                        }
                        _ => vec![],
                    };
                    let mut expanded = vec![];
                    for b in &scope {
                        let yes = match qualifiers.as_slice() {
                            [] => b.visible,
                            [t] => b.qualified && b.table.to_lowercase() == *t,
                            [s, t] => {
                                b.qualified
                                    && b.schema.to_lowercase() == *s
                                    && b.table.to_lowercase() == *t
                            }
                            _ => return Err(refuse()),
                        };
                        if yes {
                            expanded.push(b.clone())
                        }
                    }
                    if expanded.is_empty() {
                        return Err(refuse());
                    }
                    if !qualifiers.is_empty() {
                        if expanded.iter().filter(|b| b.ordinal == 0).count() > 1 {
                            return Err(refuse());
                        }
                        expanded.sort_by_key(|b| b.ordinal);
                    }
                    for b in expanded {
                        let o = b.output;
                        self.record(
                            &expr::Expr::column(o.clone()),
                            UseKind::Output,
                            &Context::SelectList,
                        );
                        os.push(o)
                    }
                }
            }
        }
        if let Some(e) = &s.selection {
            self.root(e, &scope, Context::Where, UseKind::Comparison)?;
        }
        if let Some(e) = &s.having {
            self.root(e, &scope, Context::Having, UseKind::Comparison)?;
        }
        match &s.group_by {
            a::GroupByExpr::Expressions(es, mods) if mods.is_empty() => {
                for e in es {
                    self.clause(e, &scope, &os, Context::GroupBy, UseKind::GroupBy)?
                }
            }
            _ => return Err(refuse()),
        }
        if s.distinct.is_some() {
            for o in &os {
                self.record(
                    &expr::Expr::column(o.clone()),
                    UseKind::GroupBy,
                    &Context::GroupBy,
                )
            }
        }
        Ok((scope, os))
    }
    fn factor(&mut self, t: &a::TableFactor) -> Result<Scope> {
        match t {
            a::TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
                version,
                with_ordinality,
                partitions,
                json_path,
            } => {
                if args.is_some()
                    || !with_hints.is_empty()
                    || version.is_some()
                    || *with_ordinality
                    || !partitions.is_empty()
                    || json_path.is_some()
                {
                    return Err(refuse());
                }
                if name.0.len() == 1
                    && let Some(os) = self.ctes.get(&name.0[0].value.to_lowercase())
                {
                    return Self::bind(
                        os.clone(),
                        String::new(),
                        name.0[0].value.clone(),
                        alias.as_ref(),
                    );
                }
                let r = self.relation(name)?;
                let os = match r.kind {
                    RelationKind::Table => self.table_outputs(&r),
                    RelationKind::View => {
                        if self.view_depth >= 8 {
                            return Err(refuse());
                        }
                        let parsed = Parser::parse_sql(
                            &BigQueryDialect {},
                            r.view_definition.as_deref().ok_or_else(refuse)?,
                        )
                        .map_err(|_| refuse())?;
                        let q = match parsed.as_slice() {
                            [a::Statement::Query(q)] => q,
                            _ => return Err(refuse()),
                        };
                        self.view_depth += 1;
                        let ctes = std::mem::take(&mut self.ctes);
                        let outers = std::mem::take(&mut self.outers);
                        let result = self.query(q);
                        self.ctes = ctes;
                        self.outers = outers;
                        self.view_depth -= 1;
                        let mut os = result?;
                        if os.len() != r.columns.len() {
                            return Err(refuse());
                        }
                        for (o, c) in os.iter_mut().zip(&r.columns) {
                            o.ir.name = c.name.clone();
                            o.ir.ty = Some(c.ty.clone())
                        }
                        os
                    }
                    _ => return Err(refuse()),
                };
                Self::bind(os, r.schema, r.name, alias.as_ref())
            }
            a::TableFactor::Derived {
                lateral: false,
                subquery,
                alias,
            } => {
                let os = self.query(subquery)?;
                Self::bind(os, String::new(), String::new(), alias.as_ref())
            }
            a::TableFactor::NestedJoin {
                table_with_joins,
                alias: None,
            } => self.from(table_with_joins),
            a::TableFactor::UNNEST {
                alias,
                array_exprs,
                with_offset: false,
                with_offset_alias: None,
                with_ordinality: false,
            } => {
                let mut os = vec![];
                for e in array_exprs {
                    let e = self.root(
                        e,
                        &vec![],
                        Context::Other,
                        UseKind::Function {
                            name: "unnest".into(),
                        },
                    )?;
                    if e.label != Label::Plain {
                        return Err(refuse());
                    }
                    os.push(
                        e.output(
                            alias
                                .as_ref()
                                .map_or_else(|| "unnest".into(), |a| a.name.value.clone()),
                        ),
                    )
                }
                Self::bind(os, String::new(), String::new(), alias.as_ref())
            }
            _ => Err(refuse()),
        }
    }
    fn from(&mut self, t: &a::TableWithJoins) -> Result<Scope> {
        let mut scope = self.factor(&t.relation)?;
        for j in &t.joins {
            if j.global {
                return Err(refuse());
            }
            let mut right = self.factor_visible(&j.relation, &scope)?;
            let constraint = match &j.join_operator {
                a::JoinOperator::Inner(c)
                | a::JoinOperator::LeftOuter(c)
                | a::JoinOperator::RightOuter(c)
                | a::JoinOperator::FullOuter(c) => Some(c),
                a::JoinOperator::CrossJoin => None,
                _ => return Err(refuse()),
            };
            let names = match constraint {
                Some(a::JoinConstraint::Using(ids)) => {
                    ids.iter().map(|i| i.value.clone()).collect::<Vec<_>>()
                }
                Some(a::JoinConstraint::Natural) => scope
                    .iter()
                    .filter(|l| {
                        l.visible
                            && right.iter().any(|r| {
                                r.visible
                                    && r.output.ir.name.to_lowercase()
                                        == l.output.ir.name.to_lowercase()
                            })
                    })
                    .map(|b| b.output.ir.name.clone())
                    .collect(),
                Some(a::JoinConstraint::On(_) | a::JoinConstraint::None) | None => vec![],
            };
            let mut merged = vec![];
            let mut seen = BTreeSet::new();
            for name in names {
                if !seen.insert(name.to_lowercase()) {
                    return Err(refuse());
                }
                let ident = a::Ident::new(&name);
                let l = self.resolve(std::slice::from_ref(&ident), &scope)?;
                let r = self.resolve(std::slice::from_ref(&ident), &right)?;
                let le = expr::Expr::column(l.clone());
                let re = expr::Expr::column(r.clone());
                self.record(
                    &le,
                    UseKind::Equality {
                        other: re.value.clone(),
                    },
                    &Context::JoinOn,
                );
                self.record(
                    &re,
                    UseKind::Equality {
                        other: le.value.clone(),
                    },
                    &Context::JoinOn,
                );
                let full = matches!(j.join_operator, a::JoinOperator::FullOuter(_));
                let mut o = if matches!(j.join_operator, a::JoinOperator::RightOuter(_)) {
                    r.clone()
                } else {
                    l.clone()
                };
                if l.ir.label != r.ir.label
                    || (l.ir.ty != r.ir.ty && l.ir.label != Label::Plain)
                    || (full && l.ir.label != Label::Plain)
                {
                    o.ir.label = Label::Tainted;
                    o.ir.source = None
                }
                if full || l.ir.ty != r.ir.ty {
                    o.ir.source = None
                }
                if l.ir.ty != r.ir.ty {
                    o.ir.ty = None
                }
                o.inputs = l.inputs.into_iter().chain(r.inputs).collect();
                merged.push(Bound {
                    schema: String::new(),
                    table: String::new(),
                    output: o,
                    visible: true,
                    qualified: false,
                    ordinal: 0,
                });
                for b in scope.iter_mut().chain(right.iter_mut()) {
                    if b.visible && b.output.ir.name.to_lowercase() == name.to_lowercase() {
                        b.visible = false
                    }
                }
            }
            scope.extend(right);
            merged.extend(scope.iter().filter(|b| b.visible).cloned());
            merged.extend(scope.into_iter().filter(|b| !b.visible));
            scope = merged;
            if let Some(a::JoinConstraint::On(e)) = constraint {
                self.root(e, &scope, Context::JoinOn, UseKind::Comparison)?;
            }
        }
        Ok(scope)
    }
    fn factor_visible(&mut self, t: &a::TableFactor, scope: &Scope) -> Result<Scope> {
        if matches!(t, a::TableFactor::UNNEST { .. }) {
            self.outers.push(scope.clone());
            let result = self.factor(t);
            self.outers.pop();
            result
        } else {
            self.factor(t)
        }
    }
    fn write_scope(&self, t: &a::TableFactor) -> Result<Scope> {
        match t {
            a::TableFactor::Table {
                name,
                alias,
                args: None,
                with_hints,
                version: None,
                with_ordinality: false,
                partitions,
                json_path: None,
            } if with_hints.is_empty() && partitions.is_empty() => {
                let r = self.relation(name)?;
                if r.kind != RelationKind::Table {
                    return Err(refuse());
                }
                Self::bind(self.table_outputs(&r), r.schema, r.name, alias.as_ref())
            }
            _ => Err(refuse()),
        }
    }
    fn write(&mut self, o: Output, n: &a::Expr, scope: &Scope, c: Context) -> Result<()> {
        let e = self.expression(n, scope, &c)?;
        if !matches!(o.ir.label, Label::Blind(_)) || !matches!(e.value, ValueSource::Param(_)) {
            self.plain.extend(e.params)
        }
        self.stmt.writes.push(WriteTarget {
            column: o.ir.source.ok_or_else(refuse)?,
            label: o.ir.label,
            value: e.value,
        });
        Ok(())
    }
    /// Recognize only the exact top-level explicit-tokenization shape.
    fn tokenize(&mut self, q: &a::Query) -> Result<bool> {
        let s = match q.body.as_ref() {
            a::SetExpr::Select(s) => s,
            _ => return Ok(false),
        };
        let e = match s.projection.as_slice() {
            [a::SelectItem::UnnamedExpr(e) | a::SelectItem::ExprWithAlias { expr: e, .. }] => e,
            _ => return Ok(false),
        };
        let f = match e {
            a::Expr::Function(f) => f,
            _ => return Ok(false),
        };
        if !f
            .name
            .0
            .iter()
            .any(|n| n.value.eq_ignore_ascii_case("nunya_tokenize"))
        {
            return Ok(false);
        }
        if f.name.0.len() != 1
            || self.sql_has_backslash
            || q.with.is_some()
            || q.order_by.is_some()
            || q.limit.is_some()
            || q.offset.is_some()
            || !q.limit_by.is_empty()
            || q.fetch.is_some()
            || !q.locks.is_empty()
            || q.for_clause.is_some()
            || q.settings.is_some()
            || q.format_clause.is_some()
            || s.top.is_some()
            || s.into.is_some()
            || !s.from.is_empty()
            || s.selection.is_some()
            || s.having.is_some()
            || s.distinct.is_some()
            || !s.lateral_views.is_empty()
            || s.prewhere.is_some()
            || !s.cluster_by.is_empty()
            || !s.distribute_by.is_empty()
            || !s.sort_by.is_empty()
            || !s.named_window.is_empty()
            || s.qualify.is_some()
            || s.value_table_mode.is_some()
            || s.connect_by.is_some()
            || f.parameters != a::FunctionArguments::None
            || f.filter.is_some()
            || f.null_treatment.is_some()
            || f.over.is_some()
            || !f.within_group.is_empty()
            || f.uses_odbc_syntax
        {
            return Err(refuse());
        }
        match &s.group_by {
            a::GroupByExpr::Expressions(es, mods) if es.is_empty() && mods.is_empty() => {}
            _ => return Err(refuse()),
        }
        let args = match &f.args {
            a::FunctionArguments::List(l)
                if l.clauses.is_empty() && l.duplicate_treatment.is_none() =>
            {
                &l.args
            }
            _ => return Err(refuse()),
        };
        let (domain, value) = match args.as_slice() {
            [
                a::FunctionArg::Unnamed(a::FunctionArgExpr::Expr(domain)),
                a::FunctionArg::Unnamed(a::FunctionArgExpr::Expr(value)),
            ] => (domain, value),
            _ => return Err(refuse()),
        };
        let domain = match domain {
            a::Expr::Value(a::Value::SingleQuotedString(s) | a::Value::DoubleQuotedString(s)) => {
                crate::policy::DomainName(s.clone())
            }
            _ => return Err(refuse()),
        };
        let arg = match value {
            a::Expr::Value(a::Value::Placeholder(_)) => {
                let e = self.expression(value, &vec![], &Context::Other)?;
                self.plain.extend(e.params);
                e.value
            }
            a::Expr::Value(a::Value::SingleQuotedString(s) | a::Value::DoubleQuotedString(s)) => {
                ValueSource::Literal {
                    span: self.stmt.source_span.clone(),
                    text: s.clone(),
                }
            }
            _ => return Err(refuse()),
        };
        self.stmt.kind = StatementKind::Tokenize { domain, arg };
        Ok(true)
    }
    /// Dispatch explicitly supported statement families.
    pub(super) fn statement(&mut self, s: &a::Statement) -> Result<()> {
        match s {
            a::Statement::Query(q) => {
                if self.tokenize(q)? {
                    return Ok(());
                }
                self.stmt.outputs = self.query(q)?.into_iter().map(|o| o.ir).collect()
            }
            a::Statement::Insert(i) => {
                self.stmt.kind = StatementKind::Insert;
                if i.or.is_some()
                    || i.ignore
                    || i.overwrite
                    || i.partitioned.is_some()
                    || !i.after_columns.is_empty()
                    || i.table
                    || i.on.is_some()
                    || i.returning.is_some()
                    || i.replace_into
                    || i.priority.is_some()
                    || i.insert_alias.is_some()
                    || i.table_alias.is_some()
                {
                    return Err(refuse());
                }
                let r = self.relation(&i.table_name)?;
                if r.kind != RelationKind::Table {
                    return Err(refuse());
                }
                let scope = Self::bind(self.table_outputs(&r), r.schema, r.name, None)?;
                let columns = if i.columns.is_empty() {
                    scope.iter().map(|b| b.output.clone()).collect()
                } else {
                    let mut seen = BTreeSet::new();
                    let mut cols = vec![];
                    for c in &i.columns {
                        if !seen.insert(c.value.to_lowercase()) {
                            return Err(refuse());
                        }
                        cols.push(self.resolve(std::slice::from_ref(c), &scope)?)
                    }
                    cols
                };
                let q = i.source.as_ref().ok_or_else(refuse)?;
                if let a::SetExpr::Values(v) = q.body.as_ref() {
                    if q.with.is_some()
                        || q.order_by.is_some()
                        || q.limit.is_some()
                        || q.offset.is_some()
                        || !q.limit_by.is_empty()
                        || q.fetch.is_some()
                        || !q.locks.is_empty()
                        || q.for_clause.is_some()
                        || q.settings.is_some()
                        || q.format_clause.is_some()
                    {
                        return Err(refuse());
                    }
                    for row in &v.rows {
                        if row.len() != columns.len() {
                            return Err(refuse());
                        }
                        for (o, e) in columns.iter().zip(row) {
                            self.write(o.clone(), e, &vec![], Context::Values)?
                        }
                    }
                } else {
                    let start = self.stmt.uses.len();
                    let os = self.query(q)?;
                    let tail = self.stmt.uses.split_off(start);
                    self.stmt
                        .uses
                        .extend(tail.into_iter().filter(|u| u.kind != UseKind::Output));
                    if os.len() != columns.len() {
                        return Err(refuse());
                    }
                    for (o, v) in columns.into_iter().zip(os) {
                        let e = expr::Expr::column(v);
                        self.stmt.writes.push(WriteTarget {
                            column: o.ir.source.ok_or_else(refuse)?,
                            label: o.ir.label,
                            value: e.value,
                        })
                    }
                }
            }
            a::Statement::Update {
                table,
                assignments,
                from: None,
                selection,
                returning: None,
                or: None,
            } => {
                self.stmt.kind = StatementKind::Update;
                if !table.joins.is_empty() {
                    return Err(refuse());
                }
                let scope = self.write_scope(&table.relation)?;
                let mut seen = BTreeSet::new();
                for ass in assignments {
                    let ids = match &ass.target {
                        a::AssignmentTarget::ColumnName(n) => &n.0,
                        _ => return Err(refuse()),
                    };
                    let o = self.resolve(ids, &scope)?;
                    if !seen.insert(o.ir.source.clone()) {
                        return Err(refuse());
                    }
                    self.write(o, &ass.value, &scope, Context::SetClause)?
                }
                if let Some(e) = selection {
                    self.root(e, &scope, Context::Where, UseKind::Comparison)?;
                }
            }
            a::Statement::Update { .. } => return Err(refuse()),
            a::Statement::Delete(d) => {
                self.stmt.kind = StatementKind::Delete;
                if !d.tables.is_empty()
                    || d.using.is_some()
                    || d.returning.is_some()
                    || !d.order_by.is_empty()
                    || d.limit.is_some()
                {
                    return Err(refuse());
                }
                let ts = match &d.from {
                    a::FromTable::WithFromKeyword(t) | a::FromTable::WithoutKeyword(t) => t,
                };
                if ts.len() != 1 || !ts[0].joins.is_empty() {
                    return Err(refuse());
                }
                let scope = self.write_scope(&ts[0].relation)?;
                if let Some(e) = &d.selection {
                    self.root(e, &scope, Context::Where, UseKind::Comparison)?;
                }
            }
            _ => self.stmt.kind = StatementKind::Refused(NyCode::StatementNotAllowed),
        }
        Ok(())
    }
}
