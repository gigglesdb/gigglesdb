//! Expression provenance and occurrence-sensitive parameter classification.
use super::*;
fn allowed_operator(op: &str, snapshot: &Snapshot) -> bool {
    if snapshot.user_routines.iter().any(|routine| routine == op) {
        return false;
    }
    matches!(
        op,
        "=" | "<>"
            | "!="
            | "<"
            | ">"
            | "<="
            | ">="
            | "+"
            | "-"
            | "*"
            | "/"
            | "%"
            | "^"
            | "||"
            | "~~"
            | "~~*"
            | "!~~"
            | "!~~*"
            | "~"
            | "~*"
            | "!~"
            | "!~*"
    )
}
/// A lowered expression with pass-through and sensitive-input provenance.
pub(super) struct Expr {
    /// The result information-flow label.
    pub label: Label,
    /// The direct counterpart value source.
    pub value: ValueSource,
    /// The unchanged single-column pass-through source, when present.
    pub source: Option<ColumnRef>,
    /// The known database column type.
    pub ty: Option<ColumnType>,
    /// All sensitive underlying column inputs.
    pub inputs: Vec<(ColumnRef, Label)>,
    /// Parameter occurrences not consumed as direct blind counterparts.
    pub params: Vec<u32>,
    /// Whether this expression is syntactically a bare column reference.
    pub bare: bool,
}
impl Expr {
    /// Construct a plain expression without sensitive inputs.
    pub(super) fn plain(value: ValueSource) -> Self {
        Self {
            label: Label::Plain,
            value,
            source: None,
            ty: None,
            inputs: vec![],
            params: vec![],
            bare: false,
        }
    }
    /// Construct a bare reference to an internal output.
    pub(super) fn column(o: scope::Output) -> Self {
        let value = o.source.as_ref().map_or_else(
            || ValueSource::Computed(o.label.clone()),
            |c| ValueSource::Column {
                column: c.clone(),
                label: o.label.clone(),
            },
        );
        Self {
            label: o.label,
            value,
            source: o.source,
            ty: o.ty,
            inputs: o.inputs,
            params: vec![],
            bare: true,
        }
    }
}
impl State<'_> {
    /// Lower an expression, retaining provenance and unmatched parameters.
    pub(super) fn expression(
        &mut self,
        n: &p::Node,
        scope: &scope::Scope,
        context: &Context,
    ) -> Result<Expr> {
        if let Some(extraction) = self.json_extraction(n, scope)? {
            if !matches!(self.statement.kind, StatementKind::Select) {
                return Err(json_extract::refusal());
            }
            if extraction.unguarded_plain() {
                let e = extraction.plain_expression();
                self.record(&e, UseKind::Other { what: "JSON extraction dependency" }, context);
                return Ok(e);
            }
            if !matches!(context, Context::Where | Context::JoinOn) || self.view_body {
                return Err(json_extract::refusal());
            }
            let (e, rewrite) = extraction.predicate(&self.sql)?;
            self.replacements.push(rewrite);
            if e.label == Label::Plain {
                self.record(&e, UseKind::Other { what: "JSON extraction dependency" }, context);
            }
            return Ok(e);
        }
        match node(n)? {
            NodeEnum::ColumnRef(c) => Ok(Expr::column(self.resolve(&c.fields, scope)?)),
            NodeEnum::ParamRef(p) => {
                if self.view_body {
                    return Err(refuse());
                }
                let index = u32::try_from(p.number).map_err(|_| refuse())?;
                if index == 0 {
                    return Err(refuse());
                }
                self.statement.param_count = self.statement.param_count.max(index);
                let mut e = Expr::plain(ValueSource::Param(index));
                e.params.push(index);
                Ok(e)
            }
            NodeEnum::AConst(c) => {
                if c.isnull {
                    return Ok(Expr::plain(if self.view_body {
                        ValueSource::Computed(Label::Plain)
                    } else {
                        ValueSource::Null
                    }));
                }
                if matches!(c.val, Some(p::a_const::Val::Boolval(_))) {
                    return Ok(Expr::plain(ValueSource::Computed(Label::Plain)));
                }
                let (span, text) = lexer::literal_extent(
                    &self.sql,
                    usize::try_from(c.location).map_err(|_| refuse())?,
                )
                .ok_or_else(refuse)?;
                Ok(Expr::plain(if self.view_body {
                    ValueSource::Computed(Label::Plain)
                } else {
                    ValueSource::Literal { span, text }
                }))
            }
            NodeEnum::TypeCast(c) => {
                let t = c.type_name.as_ref().ok_or_else(refuse)?;
                // Unknown cast types can run client-defined conversion functions.
                let names = strings(&t.names)?;
                let name = match names.as_slice() {
                    [n] => n.as_str(),
                    [s, n] if s == "pg_catalog" => n.as_str(),
                    _ => return Err(refuse()),
                };
                if !matches!(
                    name,
                    "text"
                        | "varchar"
                        | "bpchar"
                        | "int2"
                        | "int4"
                        | "int8"
                        | "numeric"
                        | "float4"
                        | "float8"
                        | "bool"
                        | "uuid"
                        | "date"
                        | "time"
                        | "timetz"
                        | "timestamp"
                        | "timestamptz"
                        | "interval"
                        | "bytea"
                        | "json"
                        | "jsonb"
                ) || t.setof
                    || t.pct_type
                {
                    return Err(refuse());
                }
                for modifier in &t.typmods {
                    if self
                        .root(modifier, scope, context.clone(), UseKind::Cast)?
                        .label
                        != Label::Plain
                    {
                        return Err(refuse());
                    }
                }
                for bound in &t.array_bounds {
                    match node(bound)? {
                        NodeEnum::Integer(_) => {}
                        NodeEnum::AConst(_) => {
                            self.root(bound, scope, context.clone(), UseKind::Cast)?;
                        }
                        _ => return Err(refuse()),
                    }
                }
                let mut e =
                    self.expression(c.arg.as_deref().ok_or_else(refuse)?, scope, context)?;
                if e.label != Label::Plain {
                    self.record(&e, UseKind::Cast, context);
                    e.label = Label::Tainted;
                    e.value = ValueSource::Computed(Label::Tainted);
                }
                if e.label == Label::Plain
                    && !matches!(
                        e.value,
                        ValueSource::Param(_) | ValueSource::Literal { .. } | ValueSource::Null
                    )
                {
                    e.value = ValueSource::Computed(Label::Plain);
                }
                e.bare = false;
                e.source = None;
                e.ty = None;
                Ok(e)
            }
            NodeEnum::AExpr(a) => {
                let names = strings(&a.name)?;
                let kind = p::AExprKind::try_from(a.kind).map_err(|_| refuse())?;
                let op = match names.as_slice() {
                    [n] => {
                        if matches!(n.as_str(), "@>" | "<@" | "?" | "?|" | "?&" | "@?" | "@@" | "->" | "->>" | "#>" | "#>>") {
                            return Err(json_extract::refusal());
                        }
                        let expected = match n.as_str() {
                            "BETWEEN" => Some(p::AExprKind::AexprBetween),
                            "NOT BETWEEN" => Some(p::AExprKind::AexprNotBetween),
                            "BETWEEN SYMMETRIC" => Some(p::AExprKind::AexprBetweenSym),
                            "NOT BETWEEN SYMMETRIC" => Some(p::AExprKind::AexprNotBetweenSym),
                            _ => None,
                        };
                        if expected
                            .map_or_else(|| !allowed_operator(n, self.snapshot), |k| k != kind)
                        {
                            return Err(refuse());
                        }
                        let comparisons: &[&str] = if matches!(
                            kind,
                            p::AExprKind::AexprBetween | p::AExprKind::AexprBetweenSym
                        ) {
                            &[">=", "<="]
                        } else if matches!(
                            kind,
                            p::AExprKind::AexprNotBetween | p::AExprKind::AexprNotBetweenSym
                        ) {
                            &["<", ">"]
                        } else {
                            &[]
                        };
                        if comparisons
                            .iter()
                            .any(|op| !allowed_operator(op, self.snapshot))
                        {
                            return Err(refuse());
                        }
                        n.clone()
                    }
                    _ => return Err(refuse()),
                };
                let mut args = Vec::new();
                if let Some(l) = &a.lexpr {
                    args.push(self.expression(l, scope, context)?);
                }
                if let Some(r) = &a.rexpr {
                    if let NodeEnum::List(list) = node(r)? {
                        for n in &list.items {
                            args.push(self.expression(n, scope, context)?);
                        }
                    } else {
                        args.push(
                            if matches!(
                                kind,
                                p::AExprKind::AexprLike
                                    | p::AExprKind::AexprIlike
                                    | p::AExprKind::AexprSimilar
                            ) {
                                self.pattern_operand(r, scope, context, a.location)?
                            } else {
                                self.expression(r, scope, context)?
                            },
                        );
                    }
                }
                if args.is_empty() {
                    return Err(refuse());
                }
                if kind == p::AExprKind::AexprNullif {
                    for e in &args {
                        self.record(
                            e,
                            UseKind::Function {
                                name: "nullif".into(),
                            },
                            context,
                        );
                    }
                    return Ok(combine(args, false));
                }
                if kind == p::AExprKind::AexprOp && op == "=" && args.len() == 2 {
                    for i in 0..2 {
                        if let ValueSource::JsonLeaf(domain) = &args[i].value {
                            let other = &args[1 - i].value;
                            let compatible = match other {
                                ValueSource::Param(_) | ValueSource::Literal { .. } => true,
                                ValueSource::JsonLeaf(other) | ValueSource::Column { label: Label::Blind(other), .. } => domain == other,
                                _ => false,
                            };
                            let counterpart = if i == 0 { a.rexpr.as_deref() } else { a.lexpr.as_deref() }.ok_or_else(json_extract::refusal)?;
                            if !compatible || matches!(node(counterpart)?, NodeEnum::TypeCast(_)) {
                                return Err(json_extract::refusal());
                            }
                        }
                    }
                    for i in 0..2 {
                        if args[i].bare && args[i].label != Label::Plain {
                            self.record(
                                &args[i],
                                UseKind::Equality {
                                    other: args[1 - i].value.clone(),
                                },
                                context,
                            );
                            if matches!(args[i].label, Label::Blind(_))
                                && matches!(args[1 - i].value, ValueSource::Param(_))
                            {
                                args[1 - i].params.clear();
                            }
                        } else if args[i].label != Label::Plain {
                            self.record(&args[i], UseKind::Comparison, context);
                        }
                    }
                    return Ok(combine(args, true));
                }
                if kind == p::AExprKind::AexprIn
                    && op == "="
                    && matches!(args[0].label, Label::Blind(_))
                    && args[0].bare
                    && !matches!(args[0].value, ValueSource::JsonLeaf(_))
                {
                    for i in 1..args.len() {
                        if matches!(
                            args[i].value,
                            ValueSource::Param(_) | ValueSource::Literal { .. }
                        ) {
                            self.record(
                                &args[0],
                                UseKind::Equality {
                                    other: args[i].value.clone(),
                                },
                                context,
                            );
                            args[i].params.clear();
                        } else {
                            self.record(&args[0], UseKind::Comparison, context);
                        }
                        self.record(&args[i], UseKind::Comparison, context);
                    }
                    return Ok(combine(args, true));
                }
                let comparison = matches!(
                    kind,
                    p::AExprKind::AexprOpAny
                        | p::AExprKind::AexprOpAll
                        | p::AExprKind::AexprDistinct
                        | p::AExprKind::AexprNotDistinct
                        | p::AExprKind::AexprIn
                        | p::AExprKind::AexprBetween
                        | p::AExprKind::AexprNotBetween
                        | p::AExprKind::AexprBetweenSym
                        | p::AExprKind::AexprNotBetweenSym
                ) || matches!(op.as_str(), "<" | ">" | "<=" | ">=" | "<>" | "!=");
                let pattern = matches!(
                    kind,
                    p::AExprKind::AexprLike | p::AExprKind::AexprIlike | p::AExprKind::AexprSimilar
                ) || matches!(
                    op.as_str(),
                    "~~" | "~~*" | "!~~" | "!~~*" | "~" | "~*" | "!~" | "!~*"
                );
                if !comparison && !pattern && kind != p::AExprKind::AexprOp {
                    return Err(refuse());
                }
                for e in &args {
                    self.record(
                        e,
                        if pattern {
                            UseKind::Pattern
                        } else if comparison {
                            UseKind::Comparison
                        } else {
                            UseKind::Function { name: op.clone() }
                        },
                        context,
                    );
                }
                Ok(combine(args, comparison || pattern))
            }
            NodeEnum::BoolExpr(b) => {
                let mut args = vec![];
                for n in &b.args {
                    let e = match node(n)? {
                        NodeEnum::SubLink(s)
                            if b.boolop == p::BoolExprType::NotExpr as i32
                                && s.sub_link_type == p::SubLinkType::AnySublink as i32 =>
                        {
                            self.sublink(s, scope, context, false)?
                        }
                        _ => self.expression(n, scope, context)?,
                    };
                    self.record(
                        &e,
                        UseKind::Function {
                            name: "boolean".into(),
                        },
                        context,
                    );
                    args.push(e);
                }
                Ok(combine(args, true))
            }
            NodeEnum::NullTest(t) => {
                let e = self.expression(t.arg.as_deref().ok_or_else(refuse)?, scope, context)?;
                self.record(
                    &e,
                    if e.bare {
                        UseKind::IsNull
                    } else {
                        UseKind::Function {
                            name: "is null".into(),
                        }
                    },
                    context,
                );
                Ok(combine(vec![e], true))
            }
            NodeEnum::BooleanTest(t) => {
                let e = self.expression(t.arg.as_deref().ok_or_else(refuse)?, scope, context)?;
                self.record(
                    &e,
                    UseKind::Function {
                        name: "boolean test".into(),
                    },
                    context,
                );
                Ok(combine(vec![e], true))
            }
            NodeEnum::FuncCall(f) => {
                if !f.agg_order.is_empty()
                    || f.agg_filter.is_some()
                    || f.over.is_some()
                    || f.agg_within_group
                    || f.func_variadic
                {
                    return Err(refuse());
                }
                let names = strings(&f.funcname)?;
                let name = match names.as_slice() {
                    [n] => n.to_lowercase(),
                    [s, n] if s == "pg_catalog" => n.to_lowercase(),
                    _ => return Err(refuse()),
                };
                if name.starts_with("jsonb_path_") || name.starts_with("json_path_")
                    || name.starts_with("json_extract_path") || name.starts_with("jsonb_extract_path")
                    || name.starts_with("json_each") || name.starts_with("jsonb_each") {
                    return Err(json_extract::refusal());
                }
                let aggregate = functions::aggregate(&name, self.snapshot).ok_or_else(refuse)?;
                if f.agg_star {
                    return if name == "count" && f.args.is_empty() && !f.agg_distinct {
                        Ok(Expr::plain(ValueSource::Computed(Label::Plain)))
                    } else {
                        Err(refuse())
                    };
                }
                if f.agg_distinct && !aggregate {
                    return Err(refuse());
                }
                let mut args = vec![];
                for n in &f.args {
                    args.push(self.expression(n, scope, context)?);
                }
                if name == "count" && args.iter().any(|e| e.label != Label::Plain && !e.bare) {
                    return Err(refuse());
                }
                let count = name == "count" && args.len() == 1;
                for e in &args {
                    self.record(
                        e,
                        if count && e.bare {
                            if f.agg_distinct {
                                UseKind::CountDistinct
                            } else {
                                UseKind::Count
                            }
                        } else if aggregate {
                            UseKind::Aggregate { name: name.clone() }
                        } else {
                            UseKind::Function { name: name.clone() }
                        },
                        context,
                    );
                }
                Ok(combine(args, count))
            }
            NodeEnum::CaseExpr(c) => {
                let mut args = vec![];
                if let Some(arg) = &c.arg {
                    args.push(self.expression(arg, scope, context)?);
                }
                for n in &c.args {
                    let when = match node(n)? {
                        NodeEnum::CaseWhen(w) => w,
                        _ => return Err(refuse()),
                    };
                    args.push(self.expression(
                        when.expr.as_deref().ok_or_else(refuse)?,
                        scope,
                        context,
                    )?);
                    args.push(self.expression(
                        when.result.as_deref().ok_or_else(refuse)?,
                        scope,
                        context,
                    )?);
                }
                if let Some(n) = &c.defresult {
                    args.push(self.expression(n, scope, context)?);
                }
                for e in &args {
                    self.record(
                        e,
                        UseKind::Function {
                            name: "case".into(),
                        },
                        context,
                    );
                }
                Ok(combine(args, false))
            }
            NodeEnum::CoalesceExpr(c) => {
                self.call_inputs(&c.args, scope, context, "coalesce", false)
            }
            NodeEnum::MinMaxExpr(c) => {
                self.call_inputs(&c.args, scope, context, "greatest/least", true)
            }
            NodeEnum::RowExpr(r) => self.call_inputs(&r.args, scope, context, "row", false),
            NodeEnum::SqlvalueFunction(v) => {
                match p::SqlValueFunctionOp::try_from(v.op).map_err(|_| refuse())? {
                    p::SqlValueFunctionOp::SvfopCurrentDate
                    | p::SqlValueFunctionOp::SvfopCurrentTime
                    | p::SqlValueFunctionOp::SvfopCurrentTimeN
                    | p::SqlValueFunctionOp::SvfopCurrentTimestamp
                    | p::SqlValueFunctionOp::SvfopCurrentTimestampN
                    | p::SqlValueFunctionOp::SvfopLocaltime
                    | p::SqlValueFunctionOp::SvfopLocaltimeN
                    | p::SqlValueFunctionOp::SvfopLocaltimestamp
                    | p::SqlValueFunctionOp::SvfopLocaltimestampN => {
                        Ok(Expr::plain(ValueSource::Computed(Label::Plain)))
                    }
                    _ => Err(refuse()),
                }
            }
            NodeEnum::AArrayExpr(a) => {
                let mut args = vec![];
                for n in &a.elements {
                    let e = self.expression(n, scope, context)?;
                    if e.label != Label::Plain {
                        return Err(refuse());
                    }
                    args.push(e);
                }
                Ok(combine(args, false))
            }
            NodeEnum::SubLink(s) => self.sublink(s, scope, context, true),
            _ => Err(refuse()),
        }
    }
    fn sublink(
        &mut self,
        s: &p::SubLink,
        scope: &scope::Scope,
        context: &Context,
        positive: bool,
    ) -> Result<Expr> {
        let kind = p::SubLinkType::try_from(s.sub_link_type).map_err(|_| refuse())?;
        let mut test = s
            .testexpr
            .as_deref()
            .map(|n| self.expression(n, scope, context))
            .transpose()?;
        let outputs = self.nested(s.subselect.as_deref().ok_or_else(refuse)?, Some(scope))?;
        match kind {
            p::SubLinkType::ExistsSublink if test.is_none() && s.oper_name.is_empty() => {
                Ok(Expr::plain(ValueSource::Computed(Label::Plain)))
            }
            p::SubLinkType::ExprSublink
                if test.is_none() && s.oper_name.is_empty() && outputs.len() == 1 =>
            {
                let mut e = Expr::column(outputs.into_iter().next().ok_or_else(refuse)?);
                e.bare = false;
                e.value = ValueSource::Computed(e.label.clone());
                Ok(e)
            }
            p::SubLinkType::AnySublink if outputs.len() == 1 => {
                let left = test.take().ok_or_else(refuse)?;
                let right = Expr::column(outputs.into_iter().next().ok_or_else(refuse)?);
                let names = strings(&s.oper_name)?;
                if !matches!(names.as_slice(), [] if allowed_operator("=", self.snapshot))
                    && !matches!(names.as_slice(), [op] if allowed_operator(op, self.snapshot))
                {
                    return Err(refuse());
                }
                let equality = names.is_empty() || matches!(names.as_slice(),[op] if op=="=");
                let compatible = positive
                    && left.bare
                    && equality
                    && left.source.is_some()
                    && right.source.is_some()
                    && matches!((&left.label,&right.label),(Label::Blind(a),Label::Blind(b)) if a==b);
                if compatible {
                    self.record(
                        &left,
                        UseKind::Equality {
                            other: right.value.clone(),
                        },
                        context,
                    );
                    self.record(
                        &right,
                        UseKind::Equality {
                            other: left.value.clone(),
                        },
                        context,
                    );
                } else {
                    self.record(&left, UseKind::Comparison, context);
                    self.record(&right, UseKind::Comparison, context);
                }
                Ok(combine(vec![left, right], true))
            }
            p::SubLinkType::ArraySublink
                if test.is_none() && s.oper_name.is_empty() && outputs.len() == 1 =>
            {
                let e = Expr::column(outputs.into_iter().next().ok_or_else(refuse)?);
                self.record(
                    &e,
                    UseKind::Function {
                        name: "array".into(),
                    },
                    context,
                );
                Ok(combine(vec![e], false))
            }
            _ => Err(refuse()),
        }
    }
    fn pattern_operand(
        &mut self,
        n: &p::Node,
        scope: &scope::Scope,
        context: &Context,
        operator_location: i32,
    ) -> Result<Expr> {
        if let NodeEnum::FuncCall(f) = node(n)? {
            let names = strings(&f.funcname)?;
            if matches!(names.as_slice(),[schema,name] if schema=="pg_catalog" && matches!(name.as_str(),"like_escape" | "similar_to_escape"))
            {
                // PG18 gram.y places synthesized escape helpers at the operator token.
                // An explicit function call has its own later source location.
                if f.location != operator_location
                    || f.funcformat != p::CoercionForm::CoerceExplicitCall as i32
                    || !f.agg_order.is_empty()
                    || f.agg_filter.is_some()
                    || f.over.is_some()
                    || f.agg_within_group
                    || f.agg_star
                    || f.agg_distinct
                    || f.func_variadic
                    || f.args.is_empty()
                    || f.args.len() > 2
                    || (names[1] == "like_escape" && f.args.len() != 2)
                {
                    return Err(refuse());
                }
                let mut args = vec![];
                for n in &f.args {
                    let e = self.expression(n, scope, context)?;
                    self.record(&e, UseKind::Pattern, context);
                    args.push(e);
                }
                return Ok(combine(args, false));
            }
        }
        self.expression(n, scope, context)
    }
    fn call_inputs(
        &mut self,
        ns: &[p::Node],
        scope: &scope::Scope,
        context: &Context,
        name: &str,
        order: bool,
    ) -> Result<Expr> {
        let mut args = vec![];
        for n in ns {
            let e = self.expression(n, scope, context)?;
            self.record(
                &e,
                if order {
                    UseKind::Order
                } else {
                    UseKind::Function { name: name.into() }
                },
                context,
            );
            args.push(e);
        }
        Ok(combine(args, false))
    }
}
fn combine(args: Vec<Expr>, plain_result: bool) -> Expr {
    let non_plain = args.iter().any(|e| e.label != Label::Plain);
    let label = if !plain_result && non_plain {
        Label::Tainted
    } else {
        Label::Plain
    };
    let mut e = Expr::plain(ValueSource::Computed(label.clone()));
    e.label = label;
    for a in args {
        e.params.extend(a.params);
        if !plain_result {
            e.inputs.extend(a.inputs);
        }
    }
    e
}
