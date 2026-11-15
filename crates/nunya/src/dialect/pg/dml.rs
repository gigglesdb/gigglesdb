//! Fail-closed mutation targets, assignments and returned columns.
use super::*;
impl State<'_> {
    /// Lower the supported PostgreSQL mutations.
    pub(super) fn lower_dml(&mut self, n: &NodeEnum) -> Result<()> {
        match n {
            NodeEnum::InsertStmt(s) => self.insert(s),
            NodeEnum::UpdateStmt(s) => {
                if s.with_clause.is_some() {
                    return Err(refuse());
                }
                self.statement.kind = StatementKind::Update;
                let target = self.write_scope(s.relation.as_ref().ok_or_else(refuse)?)?;
                let mut scope = target.clone();
                for n in &s.from_clause {
                    scope.extend(self.bind_from_item(n)?);
                }
                self.assignments(&s.target_list, &target, &scope)?;
                self.predicate(s.where_clause.as_deref(), &scope)?;
                self.returning(s.returning_clause.as_ref(), &scope)
            }
            NodeEnum::DeleteStmt(s) => {
                if s.with_clause.is_some() {
                    return Err(refuse());
                }
                self.statement.kind = StatementKind::Delete;
                let target = self.write_scope(s.relation.as_ref().ok_or_else(refuse)?)?;
                let mut scope = target.clone();
                for n in &s.using_clause {
                    scope.extend(self.bind_from_item(n)?);
                }
                self.predicate(s.where_clause.as_deref(), &scope)?;
                self.returning(s.returning_clause.as_ref(), &scope)
            }
            _ => Err(refuse()),
        }
    }
    fn write_scope(&mut self, r: &p::RangeVar) -> Result<scope::Scope> {
        if !r.catalogname.is_empty() {
            return Err(refuse());
        }
        let relation = self
            .snapshot
            .resolve_relation(
                (!r.schemaname.is_empty()).then_some(r.schemaname.as_str()),
                &r.relname,
            )
            .ok_or_else(refuse)?;
        if relation.kind != RelationKind::Table {
            return Err(refuse());
        }
        // Qualifying the physical relation prevents a CTE from shadowing the write target.
        let mut r = r.clone();
        r.schemaname = relation.schema.clone();
        self.bind_from_item(&p::Node {
            node: Some(NodeEnum::RangeVar(r)),
        })
    }
    fn target_column(target: &scope::Scope, name: &str) -> Result<scope::Output> {
        target
            .iter()
            .find(|b| b.output.name == name)
            .map(|b| b.output.clone())
            .ok_or_else(refuse)
    }
    fn write_value(
        &mut self,
        column: scope::Output,
        n: &p::Node,
        scope: &scope::Scope,
        context: Context,
    ) -> Result<()> {
        let value = if matches!(node(n)?, NodeEnum::SetToDefault(_)) {
            if matches!(column.label, Label::Blind(_)) {
                return Err(refuse());
            }
            ValueSource::Null
        } else if let NodeEnum::AConst(c) = node(n)?
            && !c.isnull
            && let Some(p::a_const::Val::Boolval(b)) = &c.val
        {
            let start = usize::try_from(c.location).map_err(|_| refuse())?;
            let text = if b.boolval { "true" } else { "false" };
            let end = start.checked_add(text.len()).ok_or_else(refuse)?;
            if !self
                .sql
                .get(start..end)
                .is_some_and(|s| s.eq_ignore_ascii_case(text))
            {
                return Err(refuse());
            }
            ValueSource::Literal {
                span: Span { start, end },
                text: text.into(),
            }
        } else {
            let e = self.expression(n, scope, &context)?;
            if !(matches!(column.label, Label::Blind(_))
                && matches!(e.value, ValueSource::Param(_)))
            {
                self.plain.extend(e.params.iter().copied());
            }
            e.value
        };
        self.statement.writes.push(WriteTarget {
            column: column.source.ok_or_else(refuse)?,
            label: column.label,
            value,
        });
        Ok(())
    }
    fn assignments(
        &mut self,
        targets: &[p::Node],
        target: &scope::Scope,
        scope: &scope::Scope,
    ) -> Result<()> {
        let mut seen = BTreeSet::new();
        for n in targets {
            let t = match node(n)? {
                NodeEnum::ResTarget(t) if t.indirection.is_empty() => t,
                _ => return Err(refuse()),
            };
            if !seen.insert(&t.name) {
                return Err(refuse());
            }
            let column = Self::target_column(target, &t.name)?;
            let mut value = t.val.as_deref().ok_or_else(refuse)?;
            if let NodeEnum::MultiAssignRef(m) = node(value)? {
                let row = match node(m.source.as_deref().ok_or_else(refuse)?)? {
                    NodeEnum::RowExpr(r) => r,
                    _ => return Err(refuse()),
                };
                if usize::try_from(m.ncolumns).map_err(|_| refuse())? != row.args.len() {
                    return Err(refuse());
                }
                value = row
                    .args
                    .get(
                        usize::try_from(m.colno)
                            .map_err(|_| refuse())?
                            .checked_sub(1)
                            .ok_or_else(refuse)?,
                    )
                    .ok_or_else(refuse)?;
            }
            self.write_value(column, value, scope, Context::SetClause)?;
        }
        Ok(())
    }
    fn predicate(&mut self, n: Option<&p::Node>, scope: &scope::Scope) -> Result<()> {
        if let Some(n) = n {
            self.root(n, scope, Context::Where, UseKind::Comparison)?;
        }
        Ok(())
    }
    fn insert(&mut self, s: &p::InsertStmt) -> Result<()> {
        if s.r#override != p::OverridingKind::OverridingNotSet as i32 {
            return Err(refuse());
        }
        self.statement.kind = StatementKind::Insert;
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
                let query = match node(c.ctequery.as_deref().ok_or_else(refuse)?)? {
                    NodeEnum::SelectStmt(s) => s,
                    _ => {
                        return Err(Refusal::new(
                            NyCode::StatementNotAllowed,
                            "data-modifying CTE",
                        ));
                    }
                };
                let start = self.statement.uses.len();
                let mut outputs = self.select(query)?;
                let tail = self.statement.uses.split_off(start);
                self.statement
                    .uses
                    .extend(tail.into_iter().filter(|u| u.kind != UseKind::Output));
                scope::rename(&mut outputs, &c.aliascolnames)?;
                self.ctes.insert(c.ctename.clone(), outputs);
            }
        }
        let target = self.write_scope(s.relation.as_ref().ok_or_else(refuse)?)?;
        let columns = if s.cols.is_empty() {
            target.iter().map(|b| b.output.clone()).collect::<Vec<_>>()
        } else {
            let mut seen = BTreeSet::new();
            let mut columns = vec![];
            for n in &s.cols {
                let t = match node(n)? {
                    NodeEnum::ResTarget(t) if t.indirection.is_empty() && t.val.is_none() => t,
                    _ => return Err(refuse()),
                };
                if !seen.insert(&t.name) {
                    return Err(refuse());
                }
                columns.push(Self::target_column(&target, &t.name)?);
            }
            columns
        };
        if let Some(n) = &s.select_stmt {
            let query = match node(n)? {
                NodeEnum::SelectStmt(s) => s,
                _ => return Err(refuse()),
            };
            if !query.values_lists.is_empty() {
                // A raw VALUES source has no SELECT clauses; refuse any unhandled decoration.
                let mut bare = query.clone();
                bare.values_lists.clear();
                bare.op = 0;
                let expected = p::SelectStmt {
                    limit_option: query.limit_option,
                    ..Default::default()
                };
                if *bare != expected {
                    return Err(refuse());
                }
                for row in &query.values_lists {
                    let row = match node(row)? {
                        NodeEnum::List(l) => l,
                        _ => return Err(refuse()),
                    };
                    if row.items.len() != columns.len() {
                        return Err(refuse());
                    }
                    for (column, value) in columns.iter().zip(&row.items) {
                        self.write_value(column.clone(), value, &target, Context::Values)?;
                    }
                }
            } else {
                let start = self.statement.uses.len();
                let outputs = self.select(query)?;
                let tail = self.statement.uses.split_off(start);
                self.statement
                    .uses
                    .extend(tail.into_iter().filter(|u| u.kind != UseKind::Output));
                if outputs.len() != columns.len() {
                    return Err(refuse());
                }
                for (column, output) in columns.iter().zip(outputs) {
                    self.statement.writes.push(WriteTarget {
                        column: column.source.clone().ok_or_else(refuse)?,
                        label: column.label.clone(),
                        value: output.source.map_or(
                            ValueSource::Computed(output.label.clone()),
                            |column| ValueSource::Column {
                                column,
                                label: output.label,
                            },
                        ),
                    });
                }
            }
        }
        if let Some(conflict) = &s.on_conflict_clause {
            if let Some(infer) = &conflict.infer {
                for n in &infer.index_elems {
                    let index = match node(n)? {
                        NodeEnum::IndexElem(i)
                            if i.collation.is_empty()
                                && i.opclass.is_empty()
                                && i.opclassopts.is_empty() =>
                        {
                            i
                        }
                        _ => return Err(refuse()),
                    };
                    let name = if let Some(n) = &index.expr {
                        if !index.name.is_empty() {
                            return Err(refuse());
                        }
                        match node(n)? {
                            NodeEnum::ColumnRef(c) if c.fields.len() == 1 => {
                                strings(&c.fields)?.pop().ok_or_else(refuse)?
                            }
                            _ => return Err(refuse()),
                        }
                    } else {
                        index.name.clone()
                    };
                    Self::target_column(&target, &name)?;
                }
                self.predicate(infer.where_clause.as_deref(), &target)?;
            }
            match p::OnConflictAction::try_from(conflict.action).map_err(|_| refuse())? {
                p::OnConflictAction::OnconflictNothing
                    if conflict.target_list.is_empty() && conflict.where_clause.is_none() => {}
                p::OnConflictAction::OnconflictUpdate => {
                    let mut scope = target.clone();
                    let mut excluded = target.clone();
                    for b in &mut excluded {
                        b.schema.clear();
                        b.table = "excluded".into();
                        b.unqualified = false;
                    }
                    scope.extend(excluded);
                    self.assignments(&conflict.target_list, &target, &scope)?;
                    self.predicate(conflict.where_clause.as_deref(), &scope)?;
                }
                _ => return Err(refuse()),
            }
        }
        self.returning(s.returning_clause.as_ref(), &target)
    }
    fn returning(
        &mut self,
        returning: Option<&p::ReturningClause>,
        scope: &scope::Scope,
    ) -> Result<()> {
        let Some(returning) = returning else {
            return Ok(());
        };
        if !returning.options.is_empty() {
            return Err(refuse());
        }
        for n in &returning.exprs {
            let t = match node(n)? {
                NodeEnum::ResTarget(t) if t.indirection.is_empty() => t,
                _ => return Err(refuse()),
            };
            let n = t.val.as_deref().ok_or_else(refuse)?;
            if let NodeEnum::ColumnRef(c) = node(n)? {
                let fields = &c.fields;
                let star = fields
                    .last()
                    .is_some_and(|n| matches!(n.node, Some(NodeEnum::AStar(_))));
                let names = strings(if star {
                    &fields[..fields.len() - 1]
                } else {
                    fields
                })?;
                if names.first().is_some_and(|n| n == "old" || n == "new") {
                    return Err(refuse());
                }
                if star {
                    if !t.name.is_empty() {
                        return Err(refuse());
                    }
                    let outputs = self.expand_star(&names, scope)?;
                    for output in outputs {
                        self.record(
                            &expr::Expr::column(output.clone()),
                            UseKind::Output,
                            &Context::Returning,
                        );
                        self.statement.outputs.push(output.into_ir());
                    }
                    continue;
                }
            }
            let e = self.root(n, scope, Context::Returning, UseKind::Output)?;
            let name = if !t.name.is_empty() {
                t.name.clone()
            } else if let NodeEnum::ColumnRef(c) = node(n)? {
                strings(&c.fields)?.last().cloned().ok_or_else(refuse)?
            } else {
                "?column?".into()
            };
            self.statement
                .outputs
                .push(scope::Output::from_expr(name, e).into_ir());
        }
        Ok(())
    }
}
