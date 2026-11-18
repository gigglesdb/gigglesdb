//! Scoped bindings with distinct qualified and coalesced join visibility.
use super::*;
#[derive(Clone)]
/// An internal output retaining sensitive provenance beyond its public source.
pub(super) struct Output {
    /// The exposed output column name.
    pub name: String,
    /// The result information-flow label.
    pub label: Label,
    /// The unchanged single-column pass-through source, when present.
    pub source: Option<ColumnRef>,
    /// The known database column type.
    pub ty: Option<ColumnType>,
    /// All sensitive underlying column inputs.
    pub inputs: Vec<(ColumnRef, Label)>,
    /// Whether this value has crossed a derived relational boundary.
    pub derived: bool,
}
impl Output {
    /// Discard internal provenance after all query uses have been recorded.
    pub(super) fn into_ir(self) -> OutputColumn {
        OutputColumn {
            name: self.name,
            label: self.label,
            source: self.source,
            ty: self.ty,
        }
    }
    /// Create output metadata while retaining expression provenance.
    pub(super) fn from_expr(name: String, e: expr::Expr) -> Self {
        Self {
            name,
            label: e.label,
            source: e.source,
            ty: e.ty,
            inputs: e.inputs,
            derived: true,
        }
    }
}
#[derive(Clone)]
/// A column binding with independent qualified and unqualified visibility.
pub(super) struct Bound {
    /// The qualifier schema, empty for aliases and derived bindings.
    pub schema: String,
    /// The exposed relation or alias qualifier.
    pub table: String,
    /// The output metadata and retained provenance.
    pub output: Output,
    /// Whether bare names and unqualified stars expose this binding.
    pub unqualified: bool,
    /// Whether relation-qualified references expose this binding.
    pub qualified: bool,
    /// The original relation-column order for qualified stars.
    pub ordinal: usize,
}
/// The ordered column bindings visible at one query level.
pub(super) type Scope = Vec<Bound>;
fn local(names: &[String], scope: &Scope) -> Result<(bool, Option<Output>)> {
    let qualifier = |b: &&Bound| match names {
        [_] => b.unqualified,
        [t, _] => b.qualified && b.table == *t,
        [s, t, _] => b.qualified && b.schema == *s && b.table == *t,
        _ => false,
    };
    let qualified_exists = names.len() > 1 && scope.iter().any(|b| qualifier(&b));
    let name = names.last().ok_or_else(refuse)?;
    let mut matches = scope
        .iter()
        .filter(qualifier)
        .filter(|b| b.output.name == *name);
    let output = matches.next().map(|b| b.output.clone());
    if matches.next().is_some() {
        return Err(refuse());
    }
    Ok((qualified_exists || output.is_some(), output))
}
/// Apply a PostgreSQL column-alias prefix in output order.
pub(super) fn rename(outputs: &mut [Output], aliases: &[p::Node]) -> Result<()> {
    let aliases = strings(aliases)?;
    if aliases.len() > outputs.len() {
        return Err(refuse());
    }
    for (output, name) in outputs.iter_mut().zip(aliases) {
        output.name = name;
    }
    Ok(())
}
fn bind(
    mut outputs: Vec<Output>,
    schema: String,
    name: String,
    alias: Option<&p::Alias>,
) -> Result<Scope> {
    if outputs.is_empty() {
        return Err(refuse());
    }
    if let Some(a) = alias {
        rename(&mut outputs, &a.colnames)?;
    }
    Ok(outputs
        .into_iter()
        .enumerate()
        .map(|(ordinal, output)| Bound {
            schema: if alias.is_some() {
                String::new()
            } else {
                schema.clone()
            },
            table: alias.map_or_else(|| name.clone(), |a| a.aliasname.clone()),
            output,
            unqualified: true,
            qualified: true,
            ordinal,
        })
        .collect())
}
impl State<'_> {
    /// Expand current or qualified outer stars without crossing shadowing.
    pub(super) fn expand_star(&self, qualifier: &[String], scope: &Scope) -> Result<Vec<Output>> {
        for candidate in std::iter::once(scope).chain(self.outers.iter().rev()) {
            let mut matches: Vec<_> = candidate
                .iter()
                .filter(|b| match qualifier {
                    [] => b.unqualified,
                    [t] => b.qualified && b.table == *t,
                    [s, t] => b.qualified && b.schema == *s && b.table == *t,
                    _ => false,
                })
                .collect();
            if !matches.is_empty() {
                if !qualifier.is_empty() {
                    let mut ordinals = BTreeSet::new();
                    if matches.iter().any(|b| !ordinals.insert(b.ordinal)) {
                        return Err(refuse());
                    }
                    matches.sort_by_key(|b| b.ordinal);
                }
                return Ok(matches.into_iter().map(|b| b.output.clone()).collect());
            }
            if qualifier.is_empty() {
                break;
            }
        }
        Err(refuse())
    }
    /// Resolve a column from the nearest binding scope without ambiguity.
    pub(super) fn resolve(&self, fields: &[p::Node], scope: &Scope) -> Result<Output> {
        let names = strings(fields)?;
        if names.is_empty() || names.len() > 3 {
            return Err(refuse());
        }
        for candidate in std::iter::once(scope).chain(self.outers.iter().rev()) {
            let (shadowed, output) = local(&names, candidate)?;
            if shadowed {
                return output.ok_or_else(refuse);
            }
        }
        Err(refuse())
    }
    /// Lower a SELECT subquery with an optional true outer query scope.
    pub(super) fn nested(&mut self, n: &p::Node, outer: Option<&Scope>) -> Result<Vec<Output>> {
        let s = match node(n)? {
            NodeEnum::SelectStmt(s) => s,
            _ => return Err(refuse()),
        };
        if let Some(outer) = outer {
            self.outers.push(outer.clone());
        }
        let result = self.select(s).map(|mut outputs| {
            for output in &mut outputs { output.derived = true; }
            outputs
        });
        if outer.is_some() {
            self.outers.pop();
        }
        result
    }
    /// Bind a fully understood FROM item and record internal uses.
    pub(super) fn bind_from_item(&mut self, n: &p::Node) -> Result<Scope> {
        match node(n)? {
            NodeEnum::RangeVar(r) => {
                if !r.catalogname.is_empty() {
                    return Err(refuse());
                }
                if r.schemaname.is_empty()
                    && let Some(outputs) = self.ctes.get(&r.relname)
                {
                    return bind(
                        outputs.clone(),
                        String::new(),
                        r.relname.clone(),
                        r.alias.as_ref(),
                    );
                }
                let relation = self
                    .snapshot
                    .resolve_relation(
                        (!r.schemaname.is_empty()).then_some(r.schemaname.as_str()),
                        &r.relname,
                    )
                    .ok_or_else(refuse)?
                    .clone();
                let outputs = match relation.kind {
                    RelationKind::Table => relation
                        .columns
                        .iter()
                        .map(|c| {
                            let column = ColumnRef {
                                schema: relation.schema.clone(),
                                table: relation.name.clone(),
                                column: c.name.clone(),
                            };
                            let label = self.label(&column);
                            let inputs = if label == Label::Plain {
                                vec![]
                            } else {
                                vec![(column.clone(), label.clone())]
                            };
                            Output {
                                name: c.name.clone(),
                                label,
                                source: Some(column),
                                ty: Some(c.ty.clone()),
                                inputs,
                                derived: false,
                            }
                        })
                        .collect(),
                    RelationKind::View | RelationKind::MaterializedView => {
                        if self.view_depth >= 8 {
                            return Err(refuse());
                        }
                        let definition = relation.view_definition.as_ref().ok_or_else(refuse)?;
                        let parsed = pg_query::parse(definition, 0).map_err(|_| refuse())?;
                        if parsed.protobuf.stmts.len() != 1 {
                            return Err(refuse());
                        }
                        let raw = &parsed.protobuf.stmts[0];
                        let s = match node(raw.stmt.as_deref().ok_or_else(refuse)?)? {
                            NodeEnum::SelectStmt(s) => s,
                            _ => return Err(refuse()),
                        };
                        let old_sql = std::mem::replace(&mut self.sql, definition.clone());
                        let old_ctes = std::mem::take(&mut self.ctes);
                        let old_outers = std::mem::take(&mut self.outers);
                        let old_body = self.view_body;
                        self.view_body = true;
                        self.view_depth += 1;
                        let result = self.select(s);
                        self.sql = old_sql;
                        self.ctes = old_ctes;
                        self.outers = old_outers;
                        self.view_body = old_body;
                        self.view_depth -= 1;
                        let mut outputs = result.map_err(|e| if e.code == NyCode::MaskedExpression {
                            json_extract::refusal()
                        } else { refuse() })?;
                        if outputs.len() != relation.columns.len() {
                            return Err(refuse());
                        }
                        for (output, column) in outputs.iter_mut().zip(&relation.columns) {
                            output.derived = true;
                            output.name = column.name.clone();
                            output.ty = Some(column.ty.clone());
                        }
                        outputs
                    }
                    RelationKind::Foreign | RelationKind::Other => return Err(refuse()),
                };
                bind(outputs, relation.schema, relation.name, r.alias.as_ref())
            }
            NodeEnum::RangeSubselect(r) => {
                if r.lateral {
                    return Err(refuse());
                }
                let outputs = self.nested(r.subquery.as_deref().ok_or_else(refuse)?, None)?;
                bind(outputs, String::new(), String::new(), r.alias.as_ref())
            }
            NodeEnum::JoinExpr(j) => {
                if j.join_using_alias.is_some() || j.alias.is_some() {
                    return Err(refuse());
                }
                let mut left = self.bind_from_item(j.larg.as_deref().ok_or_else(refuse)?)?;
                let mut right = self.bind_from_item(j.rarg.as_deref().ok_or_else(refuse)?)?;
                let names = if j.is_natural {
                    left.iter()
                        .filter(|b| {
                            b.unqualified
                                && right
                                    .iter()
                                    .any(|r| r.unqualified && r.output.name == b.output.name)
                        })
                        .map(|b| b.output.name.clone())
                        .collect::<Vec<_>>()
                } else {
                    strings(&j.using_clause)?
                };
                let mut merged = vec![];
                let mut seen = BTreeSet::new();
                for name in names {
                    if !seen.insert(name.clone()) {
                        return Err(refuse());
                    }
                    let names = [name.clone()];
                    let l = local(&names, &left)?.1.ok_or_else(refuse)?;
                    let r = local(&names, &right)?.1.ok_or_else(refuse)?;
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
                    let jointype = p::JoinType::try_from(j.jointype).map_err(|_| refuse())?;
                    let mut output = match jointype {
                        p::JoinType::JoinRight => r.clone(),
                        p::JoinType::JoinInner | p::JoinType::JoinLeft => l.clone(),
                        p::JoinType::JoinFull => l.clone(),
                        _ => return Err(refuse()),
                    };
                    if l.label != r.label
                        || (l.ty != r.ty && l.label != Label::Plain)
                        || (jointype == p::JoinType::JoinFull && l.label != Label::Plain)
                    {
                        output.label = Label::Tainted;
                        output.source = None;
                    }
                    if jointype == p::JoinType::JoinFull || l.ty != r.ty {
                        output.source = None;
                    }
                    if l.ty != r.ty {
                        output.ty = None;
                    }
                    output.inputs = l.inputs.into_iter().chain(r.inputs).collect();
                    merged.push(Bound {
                        schema: String::new(),
                        table: String::new(),
                        output,
                        unqualified: true,
                        qualified: false,
                        ordinal: merged.len(),
                    });
                    for b in left.iter_mut().chain(&mut right) {
                        if b.unqualified && b.output.name == name {
                            b.unqualified = false;
                        }
                    }
                }
                // Visible outputs precede hidden qualified originals; qualified stars use ordinal order.
                let mut original = left;
                original.extend(right);
                merged.extend(original.iter().filter(|b| b.unqualified).cloned());
                merged.extend(original.into_iter().filter(|b| !b.unqualified));
                if let Some(on) = &j.quals {
                    self.root(on, &merged, Context::JoinOn, UseKind::Comparison)?;
                }
                Ok(merged)
            }
            _ => Err(refuse()),
        }
    }
}
