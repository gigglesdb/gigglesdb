use super::*;
/// Expression provenance and unmatched parameter occurrences.
pub(super) struct Expr {
    /// Result information-flow label.
    pub label: Label,
    /// Direct value used for equality and writes.
    pub value: ValueSource,
    /// Unchanged column source when present.
    pub source: Option<ColumnRef>,
    /// Known database column type.
    pub ty: Option<ColumnType>,
    /// Sensitive underlying column inputs.
    pub inputs: Vec<(ColumnRef, Label)>,
    /// Unmatched client parameter occurrences.
    pub params: Vec<u32>,
    /// Whether this is a direct column reference.
    pub bare: bool,
}
impl Expr {
    pub(super) fn plain() -> Self {
        Self {
            label: Label::Plain,
            value: ValueSource::Computed(Label::Plain),
            source: None,
            ty: None,
            inputs: vec![],
            params: vec![],
            bare: false,
        }
    }
    /// Build a direct reference to a scoped output.
    pub(super) fn column(o: Output) -> Self {
        Self {
            value: o.ir.source.as_ref().map_or_else(
                || ValueSource::Computed(o.ir.label.clone()),
                |c| ValueSource::Column {
                    column: c.clone(),
                    label: o.ir.label.clone(),
                },
            ),
            label: o.ir.label,
            source: o.ir.source,
            ty: o.ir.ty,
            inputs: o.inputs,
            params: vec![],
            bare: true,
        }
    }
    /// Preserve expression provenance in a relational output.
    pub(super) fn output(self, name: String) -> Output {
        Output {
            base: false,
            ir: OutputColumn {
                name,
                label: self.label,
                source: self.source,
                ty: self.ty,
            },
            inputs: self.inputs,
        }
    }
}
fn combine(args: Vec<Expr>, plain: bool) -> Expr {
    let mut e = Expr::plain();
    if !plain && args.iter().any(|e| e.label != Label::Plain) {
        e.label = Label::Tainted;
        e.value = ValueSource::Computed(Label::Tainted)
    }
    for a in args {
        e.params.extend(a.params);
        if !plain {
            e.inputs.extend(a.inputs)
        }
    }
    e
}
fn date_part(part: &str) -> bool {
    matches!(
        part.to_uppercase().as_str(),
        "YEAR"
            | "ISOYEAR"
            | "QUARTER"
            | "MONTH"
            | "WEEK"
            | "ISOWEEK"
            | "DAY"
            | "DAYOFWEEK"
            | "DAYOFYEAR"
            | "HOUR"
            | "MINUTE"
            | "SECOND"
            | "MILLISECOND"
            | "MICROSECOND"
            | "DATE"
            | "TIME"
            | "DATETIME"
    )
}
fn known_type(ty: &a::DataType) -> bool {
    match ty {
        a::DataType::Array(a::ArrayElemTypeDef::AngleBracket(inner)) => known_type(inner),
        a::DataType::Struct(fields, _) => fields.iter().all(|f| known_type(&f.field_type)),
        _ => matches!(
            ty.to_string().to_uppercase().as_str(),
            "INT64"
                | "FLOAT64"
                | "NUMERIC"
                | "BOOL"
                | "STRING"
                | "BYTES"
                | "DATE"
                | "TIMESTAMP"
                | "JSON"
        ),
    }
}
fn aggregate(name: &str) -> Option<bool> {
    match name {
        "count" | "sum" | "avg" | "min" | "max" | "logical_and" | "logical_or" | "any_value"
        | "array_agg" | "string_agg" => Some(true),
        "lower" | "upper" | "length" | "char_length" | "byte_length" | "substr" | "concat"
        | "trim" | "ltrim" | "rtrim" | "lpad" | "rpad" | "replace" | "starts_with"
        | "ends_with" | "strpos" | "abs" | "ceil" | "floor" | "round" | "mod" | "coalesce"
        | "ifnull" | "nullif" | "if" | "greatest" | "least" | "current_date"
        | "current_timestamp" | "date" | "timestamp" | "extract" | "date_trunc"
        | "timestamp_trunc" | "format_date" | "format_timestamp" | "generate_uuid" => Some(false),
        _ => None,
    }
}
impl State<'_> {
    fn call(
        &mut self,
        args: Vec<Expr>,
        name: &str,
        kind: UseKind,
        context: &Context,
        plain: bool,
    ) -> Result<Expr> {
        if self
            .snapshot
            .user_routines
            .iter()
            .any(|n| n.to_lowercase() == name.to_lowercase())
        {
            return Err(refuse());
        }
        for e in &args {
            self.record(e, kind.clone(), context)
        }
        Ok(combine(args, plain))
    }
    /// Lower only explicitly supported expression forms.
    pub(super) fn expression(&mut self, n: &a::Expr, scope: &Scope, c: &Context) -> Result<Expr> {
        use a::Expr as X;
        if let Some(extract) = self.json_extraction(n, scope)? {
            if !matches!(c, Context::Where | Context::JoinOn) { return Err(refuse()); }
            return self.json_predicate(extract);
        }
        match n {
            X::Identifier(i) => Ok(Expr::column(self.resolve(std::slice::from_ref(i), scope)?)),
            X::CompoundIdentifier(ids) => {
                if ids.len() == 2 {
                    for candidate in std::iter::once(scope).chain(self.outers.iter().rev()) {
                        let qualifier = candidate.iter().any(|b| {
                            b.qualified && b.table.to_lowercase() == ids[0].value.to_lowercase()
                        });
                        let mut prefixes = candidate.iter().filter(|b| {
                            b.visible
                                && b.output.ir.name.to_lowercase() == ids[0].value.to_lowercase()
                        });
                        if let Some(prefix) = prefixes.next() {
                            if qualifier
                                || prefixes.next().is_some()
                                || prefix.output.ir.label != Label::Plain
                            {
                                return Err(refuse());
                            }
                            return Ok(combine(vec![Expr::column(prefix.output.clone())], false));
                        }
                        if qualifier {
                            return Ok(Expr::column(self.resolve(ids, candidate)?));
                        }
                    }
                    return Err(refuse());
                }
                Ok(Expr::column(self.resolve(ids, scope)?))
            }
            X::Value(a::Value::Placeholder(p)) => {
                if self.view_depth > 0 {
                    return Err(refuse());
                }
                let name = p.strip_prefix('@').ok_or_else(refuse)?;
                let i = u32::try_from(
                    self.names
                        .iter()
                        .position(|n| n == name)
                        .ok_or_else(refuse)?,
                )
                .map_err(|_| refuse())?
                .checked_add(1)
                .ok_or_else(refuse)?;
                let mut e = Expr::plain();
                e.value = ValueSource::Param(i);
                e.params.push(i);
                Ok(e)
            }
            X::Value(
                a::Value::Number(..)
                | a::Value::SingleQuotedString(_)
                | a::Value::DoubleQuotedString(_)
                | a::Value::TripleSingleQuotedString(_)
                | a::Value::TripleDoubleQuotedString(_)
                | a::Value::SingleQuotedByteStringLiteral(_)
                | a::Value::DoubleQuotedByteStringLiteral(_)
                | a::Value::TripleSingleQuotedByteStringLiteral(_)
                | a::Value::TripleDoubleQuotedByteStringLiteral(_)
                | a::Value::SingleQuotedRawStringLiteral(_)
                | a::Value::DoubleQuotedRawStringLiteral(_)
                | a::Value::TripleSingleQuotedRawStringLiteral(_)
                | a::Value::TripleDoubleQuotedRawStringLiteral(_)
                | a::Value::Boolean(_)
                | a::Value::Null,
            ) => Ok(Expr::plain()),
            X::TypedString { data_type, .. }
                if matches!(
                    data_type.to_string().to_uppercase().as_str(),
                    "DATE" | "TIMESTAMP"
                ) =>
            {
                Ok(Expr::plain())
            }
            X::Nested(e) => self.expression(e, scope, c),
            X::BinaryOp { left, op, right } => {
                use a::BinaryOperator as B;
                let (kind, plain) = match op {
                    B::Eq => (UseKind::Comparison, true),
                    B::NotEq | B::Lt | B::Gt | B::LtEq | B::GtEq => (UseKind::Comparison, true),
                    B::And | B::Or => (
                        UseKind::Function {
                            name: "boolean".into(),
                        },
                        true,
                    ),
                    B::Plus | B::Minus | B::Multiply | B::Divide | B::Modulo | B::StringConcat => (
                        UseKind::Function {
                            name: op.to_string(),
                        },
                        false,
                    ),
                    _ => return Err(refuse()),
                };
                if self
                    .snapshot
                    .user_routines
                    .contains(&op.to_string().to_lowercase())
                {
                    return Err(refuse());
                }
                let mut es = vec![
                    self.expression(left, scope, c)?,
                    self.expression(right, scope, c)?,
                ];
                if *op == B::Eq && es.iter().any(|e| matches!(e.value, ValueSource::JsonLeaf(_))) {
                    for (i, node) in [left.as_ref(), right.as_ref()].iter().enumerate() {
                        if let X::Value(a::Value::SingleQuotedString(value)) = node {
                            let mut counterpart = [left.as_ref(), right.as_ref()][1-i];
                            while let X::Nested(inner) = counterpart { counterpart = inner; }
                            let extraction = self.json_extraction(counterpart, scope)?.ok_or_else(refuse)?;
                            let boundary = if i == 0 { extraction.span.start } else { extraction.span.end };
                            es[i].value = ValueSource::Literal { span: json_extract::literal_span(self.sql, value, boundary, i == 0)?, text: value.clone() };
                        }
                    }
                    for i in 0..2 {
                        if let ValueSource::JsonLeaf(domain) = &es[i].value {
                            let compatible = match &es[1-i].value {
                                ValueSource::Param(_) | ValueSource::Literal { .. } => true,
                                ValueSource::JsonLeaf(other) | ValueSource::Column { label: Label::Blind(other), .. } => domain == other,
                                _ => false,
                            };
                            if !compatible || matches!([left.as_ref(), right.as_ref()][1-i], X::Cast { .. }) { return Err(refuse()); }
                        }
                    }
                }
                for i in 0..2 {
                    if *op == B::Eq && es[i].bare && es[i].label != Label::Plain {
                        self.record(
                            &es[i],
                            UseKind::Equality {
                                other: es[1 - i].value.clone(),
                            },
                            c,
                        );
                        if matches!(es[i].label, Label::Blind(_))
                            && matches!(es[1 - i].value, ValueSource::Param(_))
                        {
                            es[1 - i].params.clear()
                        }
                    } else {
                        self.record(&es[i], kind.clone(), c)
                    }
                }
                Ok(combine(es, plain))
            }
            X::UnaryOp { op, expr } => {
                let plain = match op {
                    a::UnaryOperator::Not => true,
                    a::UnaryOperator::Plus | a::UnaryOperator::Minus => false,
                    _ => return Err(refuse()),
                };
                let e = self.expression(expr, scope, c)?;
                self.call(
                    vec![e],
                    "boolean",
                    UseKind::Function {
                        name: op.to_string(),
                    },
                    c,
                    plain,
                )
            }
            X::Like {
                any: false,
                expr,
                pattern,
                escape_char,
                ..
            } => {
                if escape_char.is_some() {
                    return Err(refuse());
                }
                let es = vec![
                    self.expression(expr, scope, c)?,
                    self.expression(pattern, scope, c)?,
                ];
                self.call(es, "like", UseKind::Pattern, c, true)
            }
            X::IsNull(e) | X::IsNotNull(e) => {
                let e = self.expression(e, scope, c)?;
                let kind = if e.bare {
                    UseKind::IsNull
                } else {
                    UseKind::Function {
                        name: "is null".into(),
                    }
                };
                self.call(vec![e], "is null", kind, c, true)
            }
            X::Between {
                expr, low, high, ..
            } => {
                let es = vec![
                    self.expression(expr, scope, c)?,
                    self.expression(low, scope, c)?,
                    self.expression(high, scope, c)?,
                ];
                self.call(es, "between", UseKind::Comparison, c, true)
            }
            X::InList {
                expr,
                list,
                negated,
            } => {
                let mut es = vec![self.expression(expr, scope, c)?];
                for v in list {
                    es.push(self.expression(v, scope, c)?)
                }
                for i in 1..es.len() {
                    if !negated
                        && es[0].bare
                        && !matches!(es[0].value, ValueSource::JsonLeaf(_))
                        && matches!(es[0].label, Label::Blind(_))
                        && matches!(es[i].value, ValueSource::Param(_))
                    {
                        self.record(
                            &es[0],
                            UseKind::Equality {
                                other: es[i].value.clone(),
                            },
                            c,
                        );
                        es[i].params.clear()
                    } else {
                        self.record(&es[0], UseKind::Comparison, c)
                    }
                    self.record(&es[i], UseKind::Comparison, c)
                }
                Ok(combine(es, true))
            }
            X::Cast {
                kind,
                expr,
                data_type,
                format,
            } => {
                if format.is_some() || !matches!(kind, a::CastKind::Cast | a::CastKind::SafeCast) {
                    return Err(refuse());
                }
                let ty = data_type.to_string().to_uppercase();
                if !matches!(
                    ty.as_str(),
                    "STRING"
                        | "INT64"
                        | "FLOAT64"
                        | "NUMERIC"
                        | "BOOL"
                        | "BYTES"
                        | "DATE"
                        | "TIMESTAMP"
                        | "JSON"
                ) {
                    return Err(refuse());
                }
                let mut e = self.expression(expr, scope, c)?;
                if e.label != Label::Plain {
                    self.record(&e, UseKind::Cast, c);
                    e.label = Label::Tainted;
                    e.value = ValueSource::Computed(Label::Tainted)
                }
                e.source = None;
                e.ty = None;
                e.bare = false;
                Ok(e)
            }
            X::Function(f) => {
                // Explicit tokenization is only valid as the entire top-level statement.
                if f.name
                    .0
                    .iter()
                    .any(|n| n.value.eq_ignore_ascii_case("nunya_tokenize"))
                {
                    return Err(refuse());
                }
                if f.name.0.len() != 1
                    || f.uses_odbc_syntax
                    || f.parameters != a::FunctionArguments::None
                    || f.filter.is_some()
                    || f.null_treatment.is_some()
                    || f.over.is_some()
                    || !f.within_group.is_empty()
                {
                    return Err(refuse());
                }
                let name = f.name.0[0].value.to_lowercase();
                let agg = aggregate(&name).ok_or_else(refuse)?;
                let mut es = vec![];
                let mut distinct = false;
                match &f.args {
                    a::FunctionArguments::None
                        if matches!(name.as_str(), "current_date" | "current_timestamp") => {}
                    a::FunctionArguments::List(l) => {
                        if !l.clauses.is_empty() {
                            return Err(refuse());
                        }
                        distinct = l.duplicate_treatment == Some(a::DuplicateTreatment::Distinct);
                        if distinct && !agg {
                            return Err(refuse());
                        }
                        for (index, arg) in l.args.iter().enumerate() {
                            match arg {
                                a::FunctionArg::Unnamed(a::FunctionArgExpr::Expr(e)) => {
                                    if index == 1
                                        && matches!(name.as_str(), "date_trunc" | "timestamp_trunc")
                                    {
                                        match e {
                                            X::Identifier(i) if date_part(&i.value) => {
                                                es.push(Expr::plain())
                                            }
                                            _ => return Err(refuse()),
                                        }
                                    } else {
                                        es.push(self.expression(e, scope, c)?)
                                    }
                                }
                                a::FunctionArg::Unnamed(a::FunctionArgExpr::Wildcard)
                                    if name == "count" && l.args.len() == 1 && !distinct => {}
                                _ => return Err(refuse()),
                            }
                        }
                    }
                    _ => return Err(refuse()),
                }
                let count = name == "count";
                if count && es.iter().any(|e| e.label != Label::Plain && !e.bare) {
                    return Err(refuse());
                }
                let kind = if count {
                    if distinct {
                        UseKind::CountDistinct
                    } else {
                        UseKind::Count
                    }
                } else if agg {
                    UseKind::Aggregate { name: name.clone() }
                } else {
                    UseKind::Function { name: name.clone() }
                };
                self.call(es, &name, kind, c, count)
            }
            X::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                let mut es = vec![];
                for e in operand
                    .iter()
                    .map(|e| e.as_ref())
                    .chain(conditions)
                    .chain(results)
                    .chain(else_result.iter().map(|e| e.as_ref()))
                {
                    es.push(self.expression(e, scope, c)?)
                }
                self.call(
                    es,
                    "case",
                    UseKind::Function {
                        name: "case".into(),
                    },
                    c,
                    false,
                )
            }
            X::Array(a) => {
                let mut es = vec![];
                for x in &a.elem {
                    let e = self.expression(x, scope, c)?;
                    if e.label != Label::Plain {
                        return Err(refuse());
                    }
                    es.push(e)
                }
                Ok(combine(es, false))
            }
            X::Struct { values, fields } => {
                if fields.iter().any(|f| !known_type(&f.field_type)) {
                    return Err(refuse());
                }
                let mut es = vec![];
                for x in values {
                    let e = self.expression(x, scope, c)?;
                    if e.label != Label::Plain {
                        return Err(refuse());
                    }
                    es.push(e)
                }
                Ok(combine(es, false))
            }
            X::Named { expr, .. } | X::CompositeAccess { expr, .. } => {
                let e = self.expression(expr, scope, c)?;
                if e.label != Label::Plain {
                    return Err(refuse());
                }
                Ok(combine(vec![e], false))
            }
            X::MapAccess { column, keys } => {
                let mut es = vec![self.expression(column, scope, c)?];
                for key in keys {
                    es.push(self.expression(&key.key, scope, c)?)
                }
                if es.iter().any(|e| e.label != Label::Plain) {
                    return Err(refuse());
                }
                Ok(combine(es, false))
            }
            X::Subscript { expr, subscript } => {
                let e = self.expression(expr, scope, c)?;
                let index = match subscript.as_ref() {
                    a::Subscript::Index { index } => self.expression(index, scope, c)?,
                    _ => return Err(refuse()),
                };
                if e.label != Label::Plain || index.label != Label::Plain {
                    return Err(refuse());
                }
                Ok(combine(vec![e, index], false))
            }
            X::Trim {
                expr,
                trim_where,
                trim_what,
                trim_characters,
            } => {
                if trim_where.is_some() {
                    return Err(refuse());
                }
                let mut es = vec![self.expression(expr, scope, c)?];
                for x in trim_what
                    .iter()
                    .map(|x| x.as_ref())
                    .chain(trim_characters.iter().flatten())
                {
                    es.push(self.expression(x, scope, c)?)
                }
                self.call(
                    es,
                    "trim",
                    UseKind::Function {
                        name: "trim".into(),
                    },
                    c,
                    false,
                )
            }
            X::Ceil { expr, field } | X::Floor { expr, field } => {
                let name = if matches!(n, X::Ceil { .. }) {
                    "ceil"
                } else {
                    "floor"
                };
                let mut es = vec![self.expression(expr, scope, c)?];
                match field {
                    a::CeilFloorKind::DateTimeField(a::DateTimeField::NoDateTime) => {}
                    a::CeilFloorKind::Scale(v) => {
                        es.push(self.expression(&X::Value(v.clone()), scope, c)?)
                    }
                    _ => return Err(refuse()),
                }
                self.call(es, name, UseKind::Function { name: name.into() }, c, false)
            }
            X::Extract { expr, field, .. } => {
                if !date_part(&field.to_string()) {
                    return Err(refuse());
                }
                let e = self.expression(expr, scope, c)?;
                self.call(
                    vec![e],
                    "extract",
                    UseKind::Function {
                        name: "extract".into(),
                    },
                    c,
                    false,
                )
            }
            X::Exists { subquery, .. } => {
                self.nested(subquery, scope)?;
                Ok(Expr::plain())
            }
            X::Subquery(q) => {
                let os = self.nested(q, scope)?;
                if os.len() != 1 {
                    return Err(refuse());
                }
                let mut e = Expr::column(os.into_iter().next().ok_or_else(refuse)?);
                e.bare = false;
                e.source = None;
                e.value = ValueSource::Computed(e.label.clone());
                Ok(e)
            }
            X::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let l = self.expression(expr, scope, c)?;
                let os = self.nested(subquery, scope)?;
                if os.len() != 1 {
                    return Err(refuse());
                }
                let r = Expr::column(os.into_iter().next().ok_or_else(refuse)?);
                if !negated
                    && l.bare
                    && l.source.is_some()
                    && r.source.is_some()
                    && matches!((&l.label,&r.label),(Label::Blind(a),Label::Blind(b))if a==b)
                {
                    self.record(
                        &l,
                        UseKind::Equality {
                            other: r.value.clone(),
                        },
                        c,
                    );
                    self.record(
                        &r,
                        UseKind::Equality {
                            other: l.value.clone(),
                        },
                        c,
                    )
                } else {
                    self.record(&l, UseKind::Comparison, c);
                    self.record(&r, UseKind::Comparison, c)
                }
                Ok(combine(vec![l, r], true))
            }
            _ => Err(refuse()),
        }
    }
}
