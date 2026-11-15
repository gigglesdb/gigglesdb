//! Checked typed native conversions. No parsing, evaluation, or authority is added here.
use super::{codec, error::ApiError, proto as p};
use giggles_core::{
    operations as o,
    relational::{ColumnDef, DataType},
};

#[cfg(test)]
#[path = "native_codec_tests.rs"]
mod tests;
type R<T> = Result<T, ApiError>;
pub(crate) const MAX_EXPR_DEPTH: usize = 24;
fn need<T>(v: Option<T>) -> R<T> {
    v.ok_or(ApiError::InvalidArgument)
}
fn cap(n: usize, max: usize) -> R<()> {
    if n > max {
        Err(ApiError::InvalidArgument)
    } else {
        Ok(())
    }
}
fn list<T, U>(v: Vec<T>, f: impl FnMut(T) -> R<U>) -> R<Vec<U>> {
    v.into_iter().map(f).collect()
}

/// Inspect bounded vectors and trees before encoded_len or recursive conversion.
#[derive(Default)]
struct Shape {
    nodes: usize,
}
impl Shape {
    fn expr<'a>(&mut self, root: &'a p::NativeExpr) -> R<()> {
        let mut pending = vec![(root, 0usize)];
        while let Some((e, depth)) = pending.pop() {
            self.nodes += 1;
            cap(self.nodes, 4096)?;
            cap(depth, MAX_EXPR_DEPTH)?;
            use p::native_expr::Expression as E;
            let mut push = |e: &'a p::NativeExpr| {
                pending.push((e, depth + 1));
            };
            match e.expression.as_ref().ok_or(ApiError::InvalidArgument)? {
                E::Compare(v) => {
                    push(need(v.left.as_deref())?);
                    push(need(v.right.as_deref())?);
                }
                E::Arithmetic(v) => {
                    push(need(v.left.as_deref())?);
                    push(need(v.right.as_deref())?);
                }
                E::AndExpr(v) | E::OrExpr(v) => {
                    push(need(v.left.as_deref())?);
                    push(need(v.right.as_deref())?);
                }
                E::IsNull(v) => push(need(v.expression.as_deref())?),
                E::NotExpr(v) | E::Negate(v) => push(v),
                E::Call(v) => {
                    cap(v.arguments.len(), 64)?;
                    for e in &v.arguments {
                        push(e);
                    }
                }
                E::CaseExpr(v) => {
                    cap(v.arms.len(), 64)?;
                    if let Some(e) = v.operand.as_deref() {
                        push(e);
                    }
                    if let Some(e) = v.else_expr.as_deref() {
                        push(e);
                    }
                    for a in &v.arms {
                        push(need(a.when.as_ref())?);
                        push(need(a.then.as_ref())?);
                    }
                }
                _ => {}
            }
            cap(self.nodes + pending.len(), 4096)?;
        }
        Ok(())
    }
    fn optional(&mut self, e: &Option<p::NativeExpr>) -> R<()> {
        if let Some(e) = e {
            self.expr(e)?;
        }
        Ok(())
    }
    fn assignments(&mut self, v: &[p::NativeAssignment]) -> R<()> {
        cap(v.len(), 64)?;
        for a in v {
            self.expr(need(a.expression.as_ref())?)?;
        }
        Ok(())
    }
    fn projections(&mut self, v: &[p::NativeProjection]) -> R<()> {
        cap(v.len(), 64)?;
        for a in v {
            self.expr(need(a.expression.as_ref())?)?;
        }
        Ok(())
    }
    fn request(&mut self, r: &p::NativeRequest, depth: usize) -> R<()> {
        cap(depth, 2)?;
        use p::native_request::Request as Q;
        match need(r.request.as_ref())? {
            Q::Read(v) => {
                cap(v.order.len(), 64)?;
                cap(v.projection.len(), 64)?;
                self.optional(&v.filter)?;
            }
            Q::Insert(v) => cap(v.values.len(), 64)?,
            Q::Update(v) => {
                cap(v.values.len(), 64)?;
                self.expr(need(v.filter.as_ref())?)?;
                self.optional(&v.guard)?;
                intent_shape(need(v.intent.as_ref())?)?;
            }
            Q::Delete(v) => {
                self.expr(need(v.filter.as_ref())?)?;
                self.optional(&v.guard)?;
            }
            Q::UpdateExpressions(v) => {
                self.assignments(&v.values)?;
                self.expr(need(v.filter.as_ref())?)?;
                self.optional(&v.guard)?;
                intent_shape(need(v.intent.as_ref())?)?;
            }
            Q::ReadExpressions(v) => {
                cap(v.order.len(), 64)?;
                self.projections(&v.projection)?;
                self.optional(&v.filter)?;
            }
            Q::InsertExpressions(v) => self.assignments(&v.values)?,
            Q::InsertMany(v) => {
                cap(v.columns.len(), 64)?;
                cap(v.rows.len(), 1024)?;
                for row in &v.rows {
                    cap(row.values.len(), 64)?;
                    if row.values.len() != v.columns.len() {
                        return Err(ApiError::InvalidArgument);
                    }
                    for e in &row.values {
                        self.expr(e)?;
                    }
                }
            }
            Q::Returning(v) => {
                let r = need(v.request.as_deref())?;
                if !matches!(
                    r.request,
                    Some(
                        Q::Insert(_)
                            | Q::InsertExpressions(_)
                            | Q::InsertMany(_)
                            | Q::Update(_)
                            | Q::UpdateExpressions(_)
                            | Q::Delete(_)
                            | Q::OnConflict(_)
                    )
                ) {
                    return Err(ApiError::InvalidArgument);
                }
                self.projections(&v.projection)?;
                self.request(r, depth + 1)?;
            }
            Q::OnConflict(v) => {
                let r = need(v.request.as_deref())?;
                if !matches!(
                    r.request,
                    Some(Q::Insert(_) | Q::InsertExpressions(_) | Q::InsertMany(_))
                ) {
                    return Err(ApiError::InvalidArgument);
                }
                cap(v.target.len(), 64)?;
                if let Some(p::native_conflict_action::Action::Update(u)) =
                    need(v.action.as_ref())?.action.as_ref()
                {
                    self.optional(&u.guard)?;
                    self.assignments(&u.values)?;
                }
                self.request(r, depth + 1)?;
            }
        }
        Ok(())
    }
    fn transaction(&mut self, t: &p::NativeTransaction) -> R<()> {
        cap(t.steps.len() + usize::from(t.emit.is_some()), 64)?;
        for s in &t.steps {
            match need(s.step.as_ref())? {
                p::native_transaction_step::Step::Execute(r) => self.request(r, 0)?,
                p::native_transaction_step::Step::LetBinding(v) => {
                    self.request(need(v.request.as_ref())?, 0)?
                }
                p::native_transaction_step::Step::Require(e) => self.expr(e)?,
            }
        }
        if let Some(emit) = &t.emit {
            cap(emit.values.len(), 64)?;
            for e in &emit.values {
                if let Some(p::native_emit_expr::Value::Scalar(e)) = &e.value {
                    self.expr(e)?;
                }
            }
        }
        Ok(())
    }
}
fn intent_shape(i: &p::NativeIntent) -> R<()> {
    if let Some(p::native_intent::Intent::Claim(c)) = &i.intent {
        cap(c.order.len(), 64)?;
    }
    Ok(())
}
pub(crate) fn validate(input: &p::ExecuteRequest) -> R<()> {
    match input.request.as_ref() {
        Some(p::execute_request::Request::TypedRead(v)) => read_shape(v)?,
        Some(p::execute_request::Request::Operation(v)) => {
            Shape::default().request(need(v.request.as_ref())?, 0)?;
            check_bindings(&v.bindings)?;
        }
        Some(p::execute_request::Request::Transaction(v)) => {
            Shape::default().transaction(need(v.transaction.as_ref())?)?;
            check_bindings(&v.bindings)?;
        }
        _ => {}
    }
    Ok(())
}

fn read_shape(v: &p::NativeTypedRead) -> R<()> {
    cap(v.projection.len(), 64)?;
    cap(v.group_by.len(), 64)?;
    cap(v.order_by.len(), 64)?;
    let mut pending = Vec::new();
    for e in v.projection.iter().filter_map(|p| p.expression.as_ref())
        .chain(v.group_by.iter()).chain(v.order_by.iter().filter_map(|o| o.expression.as_ref()))
        .chain(v.filter.iter()).chain(v.having.iter())
        .chain(v.join.iter().filter_map(|j| j.on.as_ref())) {
        pending.push((e, 0));
    }
    let mut nodes = 0;
    while let Some((e, depth)) = pending.pop() {
        nodes += 1;
        cap(nodes + pending.len(), 4096)?;
        cap(depth, MAX_EXPR_DEPTH)?;
        use p::native_read_expr::Expression as E;
        let mut push = |e| pending.push((e, depth + 1));
        match need(e.expression.as_ref())? {
            E::Compare(v) => { push(need(v.left.as_deref())?); push(need(v.right.as_deref())?); }
            E::AndExpr(v) | E::OrExpr(v) => { push(need(v.left.as_deref())?); push(need(v.right.as_deref())?); }
            E::IsNull(v) => push(need(v.expression.as_deref())?),
            E::NotExpr(v) => push(v),
            _ => {}
        }
    }
    Ok(())
}
fn read_expr(v: p::NativeReadExpr) -> R<giggles_core::relational::ReadExpr> {
    use giggles_core::relational::{ReadAggregate as A, ReadExpr as E};
    use p::native_read_expr::Expression as P;
    Ok(match need(v.expression)? {
        P::Literal(v) => E::Literal(codec::decode_value(v)?),
        P::Column(v) => match v.source {
            Some(source) => E::QualifiedColumn { source, column: need(v.name)? },
            None => E::Column(need(v.name)?),
        },
        P::Aggregate(v) => {
            let function = need(v.function)?;
            if function == "count_star" {
                if v.column.is_some() { return Err(ApiError::InvalidArgument); }
                E::Aggregate(A::CountStar)
            } else {
                let column = need(v.column)?;
                let name = need(column.name)?;
                let aggregate = match function.as_str() {
                    "count" => A::Count(name), "sum" => A::Sum(name), "min" => A::Min(name),
                    "max" => A::Max(name), "avg" => A::Avg(name),
                    _ => return Err(ApiError::InvalidArgument),
                };
                E::Aggregate(match column.source {
                    Some(source) => A::Qualified { source, aggregate: Box::new(aggregate) },
                    None => aggregate,
                })
            }
        }
        _ => return Err(ApiError::InvalidArgument),
    })
}
fn read_predicate(v: p::NativeReadExpr) -> R<giggles_core::relational::ReadPredicate> {
    use giggles_core::relational::{ReadCompare as C, ReadPredicate as P};
    use p::native_read_expr::Expression as E;
    let boxed = |v: Option<Box<p::NativeReadExpr>>| read_predicate(*need(v)?).map(Box::new);
    Ok(match need(v.expression)? {
        E::Compare(v) => P::Compare(read_expr(*need(v.left)?)?, match v.comparison {
            1 => C::Eq, 2 => C::Ne, 3 => C::Lt, 4 => C::Le, 5 => C::Gt, 6 => C::Ge,
            _ => return Err(ApiError::InvalidArgument),
        }, read_expr(*need(v.right)?)?),
        E::IsNull(v) => P::IsNull(read_expr(*need(v.expression)?)?, need(v.negated)?),
        E::NotExpr(v) => P::Not(Box::new(read_predicate(*v)?)),
        E::AndExpr(v) => P::And(boxed(v.left)?, boxed(v.right)?),
        E::OrExpr(v) => P::Or(boxed(v.left)?, boxed(v.right)?),
        _ => return Err(ApiError::InvalidArgument),
    })
}
pub(crate) fn typed_read(v: p::NativeTypedRead) -> R<codec::Invocation> {
    use giggles_core::relational as r;
    let projection = list(v.projection, |p| Ok(r::ReadProjection {
        expression: read_expr(need(p.expression)?)?, alias: p.alias,
    }))?;
    if projection.is_empty() { return Err(ApiError::InvalidArgument); }
    let filter = v.filter.map(read_predicate).transpose()?;
    let group_by = list(v.group_by, read_expr)?;
    let mut clauses = r::ReadClauses {
        having: v.having.map(read_predicate).transpose()?, limit: v.limit,
        order_by: list(v.order_by, |o| Ok(r::ReadOrder {
            expression: read_expr(need(o.expression)?)?, descending: need(o.descending)?,
            nulls_first: o.nulls_first,
        }))?, ..r::ReadClauses::default()
    };
    Ok(if let Some(j) = v.join {
        codec::Invocation::Join(Box::new(r::TwoSourceRead {
            left: r::ReadSource { table: need(v.table)?, alias: need(j.left_alias)? },
            right: r::ReadSource { table: need(j.table)?, alias: need(j.right_alias)? },
            kind: if need(j.left)? { r::ReadJoinKind::Left } else { r::ReadJoinKind::Inner },
            on: read_predicate(need(j.on)?)?, group_by, projection, filter,
        }.with_clauses(clauses)))
    } else {
        clauses.group_by = list(group_by, |e| match e {
            r::ReadExpr::Column(c) => Ok(c), _ => Err(ApiError::InvalidArgument),
        })?;
        codec::Invocation::Read(Box::new(r::SingleSourceRead { table: need(v.table)?, projection, filter }
            .with_clauses(clauses)))
    })
}
fn check_bindings(values: &[p::NativeBinding]) -> R<()> {
    cap(values.len(), o::MAX_BINDINGS)?;
    let mut names = std::collections::BTreeSet::new();
    let mut bytes = 0usize;
    for b in values {
        if !names.insert(need(b.name.as_deref())?) {
            return Err(ApiError::InvalidArgument);
        }
        bytes = bytes
            .checked_add(match need(need(b.value.as_ref())?.value.as_ref())? {
                p::value::Value::TextValue(v) => v.len(),
                p::value::Value::BytesValue(v) => v.len(),
                _ => 9,
            })
            .ok_or(ApiError::InvalidArgument)?;
        cap(bytes, o::MAX_BINDING_BYTES)?;
    }
    Ok(())
}
pub(crate) fn bindings(values: Vec<p::NativeBinding>) -> R<o::Bindings> {
    check_bindings(&values)?;
    values
        .into_iter()
        .map(|b| Ok((need(b.name)?, codec::decode_value(need(b.value)?)?)))
        .collect()
}
fn value(v: p::NativeValueExpr) -> R<o::ValueExpr> {
    Ok(match need(v.value)? {
        p::native_value_expr::Value::Literal(v) => o::ValueExpr::Literal(codec::decode_value(v)?),
        p::native_value_expr::Value::Parameter(v) => o::ValueExpr::Parameter(v),
    })
}
fn sort(v: p::NativeSort) -> R<o::Sort> {
    Ok(o::Sort {
        field: need(v.field)?,
        descending: need(v.descending)?,
    })
}
fn assignment(v: p::NativeAssignment) -> R<(String, o::Expr)> {
    Ok((need(v.column)?, expr(need(v.expression)?)?))
}
fn value_assignment(v: p::NativeValueAssignment) -> R<(String, o::ValueExpr)> {
    Ok((need(v.column)?, value(need(v.value)?)?))
}
fn projection(v: p::NativeProjection) -> R<o::Projection> {
    Ok(o::Projection {
        expression: expr(need(v.expression)?)?,
        alias: v.alias,
    })
}
fn intent(v: p::NativeIntent) -> R<o::Intent> {
    Ok(match need(v.intent)? {
        p::native_intent::Intent::One(_) => o::Intent::One,
        p::native_intent::Intent::Many(v) => o::Intent::Many {
            cap: value(need(v.cap)?)?,
        },
        p::native_intent::Intent::Claim(v) => o::Intent::Claim {
            order: list(v.order, sort)?,
        },
    })
}
fn conflict(v: p::NativeConflictAction) -> R<o::ConflictAction> {
    Ok(match need(v.action)? {
        p::native_conflict_action::Action::Nothing(_) => o::ConflictAction::Nothing,
        p::native_conflict_action::Action::Update(v) => o::ConflictAction::Update {
            guard: v.guard.map(expr).transpose()?,
            values: list(v.values, assignment)?,
        },
    })
}
pub(crate) fn request(v: p::NativeRequest) -> R<o::Request> {
    use p::native_request::Request as Q;
    Ok(match need(v.request)? {
        Q::Read(v) => o::Request::Read {
            table: need(v.table)?,
            filter: v.filter.map(expr).transpose()?,
            order: list(v.order, sort)?,
            limit: v.limit.map(value).transpose()?,
            projection: v.projection,
        },
        Q::Insert(v) => o::Request::Insert {
            table: need(v.table)?,
            values: list(v.values, value_assignment)?,
        },
        Q::Update(v) => o::Request::Update {
            table: need(v.table)?,
            filter: expr(need(v.filter)?)?,
            guard: v.guard.map(expr).transpose()?,
            values: list(v.values, value_assignment)?,
            intent: intent(need(v.intent)?)?,
        },
        Q::Delete(v) => o::Request::Delete {
            table: need(v.table)?,
            filter: expr(need(v.filter)?)?,
            guard: v.guard.map(expr).transpose()?,
            cap: v.cap.map(value).transpose()?,
        },
        Q::UpdateExpressions(v) => o::Request::UpdateExpressions {
            table: need(v.table)?,
            filter: expr(need(v.filter)?)?,
            guard: v.guard.map(expr).transpose()?,
            values: list(v.values, assignment)?,
            intent: intent(need(v.intent)?)?,
        },
        Q::ReadExpressions(v) => o::Request::ReadExpressions {
            table: need(v.table)?,
            filter: v.filter.map(expr).transpose()?,
            order: list(v.order, sort)?,
            limit: v.limit.map(value).transpose()?,
            projection: list(v.projection, projection)?,
        },
        Q::Returning(v) => o::Request::Returning {
            request: Box::new(request(*need(v.request)?)?),
            projection: list(v.projection, projection)?,
        },
        Q::InsertExpressions(v) => o::Request::InsertExpressions {
            table: need(v.table)?,
            values: list(v.values, assignment)?,
        },
        Q::InsertMany(v) => o::Request::InsertMany {
            table: need(v.table)?,
            columns: v.columns,
            rows: list(v.rows, |r| list(r.values, expr))?,
            cap: value(need(v.cap)?)?,
        },
        Q::OnConflict(v) => o::Request::OnConflict {
            request: Box::new(request(*need(v.request)?)?),
            target: v.target,
            action: conflict(need(v.action)?)?,
        },
    })
}
fn boxed(v: Option<Box<p::NativeExpr>>) -> R<Box<o::Expr>> {
    Ok(Box::new(expr(*need(v)?)?))
}
fn local(v: p::NativeLocalRef) -> R<o::LocalRef> {
    Ok(match need(v.reference)? {
        p::native_local_ref::Reference::Field(v) => o::LocalRef::Field {
            local: need(v.local)?,
            field: need(v.field)?,
        },
        p::native_local_ref::Reference::Status(local) => o::LocalRef::Status { local },
        p::native_local_ref::Reference::Changed(local) => o::LocalRef::Changed { local },
        p::native_local_ref::Reference::Count(local) => o::LocalRef::Count { local },
    })
}
fn expr(v: p::NativeExpr) -> R<o::Expr> {
    use p::native_expr::Expression as E;
    Ok(match need(v.expression)? {
        E::Value(v) => o::Expr::Value(value(v)?),
        E::Field(v) => o::Expr::Field(v),
        E::Compare(v) => {
            o::Expr::Compare(boxed(v.left)?, comparison(v.comparison)?, boxed(v.right)?)
        }
        E::IsNull(v) => o::Expr::IsNull(boxed(v.expression)?, need(v.negated)?),
        E::NotExpr(v) => o::Expr::Not(Box::new(expr(*v)?)),
        E::AndExpr(v) => o::Expr::And(boxed(v.left)?, boxed(v.right)?),
        E::OrExpr(v) => o::Expr::Or(boxed(v.left)?, boxed(v.right)?),
        E::Negate(v) => o::Expr::Negate(Box::new(expr(*v)?)),
        E::Arithmetic(v) => {
            o::Expr::Arithmetic(boxed(v.left)?, arithmetic(v.operator)?, boxed(v.right)?)
        }
        E::Local(v) => o::Expr::Local(local(v)?),
        E::Call(v) => o::Expr::Call {
            function: function(v.function)?,
            arguments: list(v.arguments, expr)?,
        },
        E::CaseExpr(v) => o::Expr::Case {
            operand: v.operand.map(|e| expr(*e).map(Box::new)).transpose()?,
            arms: list(v.arms, |a| Ok((expr(need(a.when)?)?, expr(need(a.then)?)?)))?,
            else_expr: v.else_expr.map(|e| expr(*e).map(Box::new)).transpose()?,
        },
        E::ConflictField(v) => o::Expr::ConflictField {
            source: source(v.source)?,
            field: need(v.field)?,
        },
    })
}
fn comparison(v: i32) -> R<o::Compare> {
    Ok(match v {
        1 => o::Compare::Eq,
        2 => o::Compare::Ne,
        3 => o::Compare::Lt,
        4 => o::Compare::Le,
        5 => o::Compare::Gt,
        6 => o::Compare::Ge,
        _ => return Err(ApiError::InvalidArgument),
    })
}
fn arithmetic(v: i32) -> R<o::Arithmetic> {
    Ok(match v {
        1 => o::Arithmetic::Add,
        2 => o::Arithmetic::Subtract,
        3 => o::Arithmetic::Multiply,
        4 => o::Arithmetic::Divide,
        5 => o::Arithmetic::Remainder,
        _ => return Err(ApiError::InvalidArgument),
    })
}
fn function(v: i32) -> R<o::ScalarFunction> {
    Ok(match v {
        1 => o::ScalarFunction::Coalesce,
        2 => o::ScalarFunction::Abs,
        3 => o::ScalarFunction::Length,
        _ => return Err(ApiError::InvalidArgument),
    })
}
fn source(v: i32) -> R<o::ConflictSource> {
    Ok(match v {
        1 => o::ConflictSource::Existing,
        2 => o::ConflictSource::Incoming,
        _ => return Err(ApiError::InvalidArgument),
    })
}

pub(crate) fn transaction(v: p::NativeTransaction) -> R<o::Transaction> {
    Ok(o::Transaction {
        read_only: need(v.read_only)?,
        steps: list(v.steps, |s| {
            Ok(match need(s.step)? {
                p::native_transaction_step::Step::Execute(v) => {
                    o::TransactionStep::Execute(request(v)?)
                }
                p::native_transaction_step::Step::LetBinding(v) => o::TransactionStep::Let {
                    name: need(v.name)?,
                    request: request(need(v.request)?)?,
                },
                p::native_transaction_step::Step::Require(v) => {
                    o::TransactionStep::Require(expr(v)?)
                }
            })
        })?,
        emit: v
            .emit
            .map(|v| {
                list(v.values, |e| {
                    Ok(match need(e.value)? {
                        p::native_emit_expr::Value::Scalar(v) => o::EmitExpr::Scalar(expr(v)?),
                        p::native_emit_expr::Value::Row(v) => o::EmitExpr::Row(v),
                        p::native_emit_expr::Value::Rows(v) => o::EmitExpr::Rows(v),
                    })
                })
            })
            .transpose()?,
    })
}
fn number(v: usize) -> R<u64> {
    u64::try_from(v).map_err(|_| ApiError::Internal)
}
fn row(v: o::SqlRow) -> p::SqlRow {
    p::SqlRow {
        values: v.values.into_iter().map(codec::encode_value).collect(),
        version: v.version,
    }
}
fn conflicts(v: Option<Vec<o::ConflictTupleOutcome>>) -> R<Option<p::NativeConflictResults>> {
    v.map(|items| {
        list(items, |v| {
            Ok(p::NativeConflictTupleOutcome {
                input_index: number(v.input_index)?,
                status: match v.status {
                    o::ConflictStatus::Success => 1,
                    o::ConflictStatus::Skipped => 2,
                },
                changed: v.changed,
                returned_row: v.returned_row.map(number).transpose()?,
            })
        })
        .map(|items| p::NativeConflictResults { items })
    })
    .transpose()
}
pub(crate) fn encode_outcome(v: o::Outcome) -> R<p::NativeOutcome> {
    Ok(p::NativeOutcome {
        columns: v.columns,
        rows: v.rows.into_iter().map(row).collect(),
        matched: number(v.matched)?,
        affected: number(v.affected)?,
        changed: number(v.changed)?,
        complete: v.complete,
        conflict_results: conflicts(v.conflict_results)?,
    })
}
fn column(v: ColumnDef) -> p::NativeColumn {
    p::NativeColumn {
        id: v.id,
        name: v.name,
        data_type: match v.data_type {
            DataType::Boolean => 1,
            DataType::SmallInt => 2,
            DataType::Integer => 3,
            DataType::BigInt => 4,
            DataType::Uuid
            | DataType::Jsonb
            | DataType::IntArray
            | DataType::BigIntArray
            | DataType::TextArray
            | DataType::Text
            | DataType::Varchar
            | DataType::Bpchar => 5,
            DataType::Bytes => 6,
            DataType::Numeric => 7,
            DataType::Real => 8,
            DataType::DoublePrecision => 9,
            DataType::Date => 10,
            DataType::Time => 11,
            DataType::TimeTz => 12,
            DataType::Timestamp => 13,
            DataType::TimestampTz => 14,
            DataType::Interval => 15,
        },
        not_null: v.not_null,
    }
}
pub(crate) fn encode_transaction_outcome(
    v: o::TransactionOutcome,
) -> R<p::NativeTransactionOutcome> {
    Ok(p::NativeTransactionOutcome {
        summaries: list(v.summaries, |v| {
            Ok(p::NativeStepSummary {
                item_index: number(v.item_index)?,
                matched: number(v.matched)?,
                affected: number(v.affected)?,
                changed: number(v.changed)?,
                complete: v.complete,
                conflict_results: conflicts(v.conflict_results)?,
            })
        })?,
        emitted: v
            .emitted
            .into_iter()
            .map(|v| p::NativeEmittedValue {
                value: Some(match v {
                    o::EmittedValue::Scalar(v) => {
                        p::native_emitted_value::Value::Scalar(codec::encode_value(v))
                    }
                    o::EmittedValue::Row { columns, row: r } => {
                        p::native_emitted_value::Value::Row(p::NativeEmittedRow {
                            columns: columns.into_iter().map(column).collect(),
                            row: Some(row(r)),
                        })
                    }
                    o::EmittedValue::Rows {
                        columns,
                        rows,
                        complete,
                    } => p::native_emitted_value::Value::Rows(p::NativeEmittedRows {
                        columns: columns.into_iter().map(column).collect(),
                        rows: rows.into_iter().map(row).collect(),
                        complete,
                    }),
                }),
            })
            .collect(),
    })
}
pub(crate) fn encode_error(v: o::Error) -> p::NativeError {
    let syntax_offset = if let o::Error::Syntax { offset } = &v {
        u64::try_from(*offset).ok()
    } else {
        None
    };
    let sqlstate = v.sqlstate().map(str::to_owned);
    let kind = match v {
        o::Error::Syntax { .. } => 1,
        o::Error::Unsupported => 2,
        o::Error::Limit => 3,
        o::Error::Bind(_) => 4,
        o::Error::Type => 5,
        o::Error::NotFound => 6,
        o::Error::AmbiguousMatch => 7,
        o::Error::ConditionFailed => 8,
        o::Error::TooManyMatches => 9,
        o::Error::ClaimNotConsumed => 10,
        o::Error::Conflict => 11,
        o::Error::Core(_) => 12,
        o::Error::Unknown => 13,
        o::Error::Overflow => 14,
        o::Error::DivisionByZero => 15,
        o::Error::RequirementFailed => 16,
        o::Error::DuplicateTarget => 17,
        o::Error::PlanRequired => 18,
        o::Error::Duplicate => 19,
        o::Error::Sql { .. } => 20,
    };
    p::NativeError {
        kind,
        syntax_offset,
        sqlstate,
    }
}
