//! PostgreSQL 18 OVERLAY text/bytea semantics, UTF8 code points.
//! Independent implementation informed by REL_18_0 varlena.c text_overlay/bytea_overlay.
use super::*;
#[cfg(test)]
thread_local! { static OVERLAY_BIND_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

fn selected(types: &[Option<DataType>]) -> R<DataType> {
    if !(3..=4).contains(&types.len()) {
        return Err(SqlError::UndefinedFunction);
    }
    let mut family = None;
    for ty in types[..2].iter().flatten() {
        if *ty == DataType::Bpchar {
            return Err(SqlError::Unsupported);
        }
        let ty = if *ty == DataType::Varchar {
            &DataType::Text
        } else {
            ty
        };
        if !matches!(ty, DataType::Text | DataType::Bytes)
            || family.as_ref().is_some_and(|old| old != ty)
        {
            return Err(SqlError::UndefinedFunction);
        }
        family = Some(ty.clone());
    }
    if types[2..]
        .iter()
        .flatten()
        .any(|ty| !matches!(ty, DataType::SmallInt | DataType::Integer))
    {
        return Err(SqlError::UndefinedFunction);
    }
    Ok(family.unwrap_or(DataType::Text))
}

impl Scalar {
    fn overlay_types(
        args: &[Self],
        resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
        tables: &[&TableDef],
        parameters: &[Option<DataType>],
    ) -> R<Vec<Option<DataType>>> {
        args.iter()
            .map(|a| {
                if a.aggregate_unknown(parameters) {
                    Ok(None)
                } else {
                    Ok(Some(a.bind(resolve, tables, parameters)?.ty))
                }
            })
            .collect()
    }
    pub(super) fn infer_overlay(
        args: &[Self],
        parameters: &mut [Option<DataType>],
        resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
        tables: &[&TableDef],
    ) -> R<()> {
        for arg in args {
            arg.infer(None, parameters, resolve, tables)?;
        }
        let types = Self::overlay_types(args, resolve, tables, parameters)?;
        let source = selected(&types)?;
        for (i, arg) in args.iter().enumerate() {
            if types[i].is_none() {
                arg.infer(
                    Some(if i < 2 { &source } else { &DataType::Integer }),
                    parameters,
                    resolve,
                    tables,
                )?;
            }
        }
        Ok(())
    }
    pub(super) fn bind_overlay(
        args: &[Self],
        resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
        tables: &[&TableDef],
        parameters: &[Option<DataType>],
    ) -> R<BoundScalar> {
        #[cfg(test)]
        OVERLAY_BIND_VISITS.with(|visits| visits.set(visits.get() + 1));
        // Bind each child once: re-binding here makes nested calls exponential.
        let bound = args
            .iter()
            .map(|arg| arg.bind(resolve, tables, parameters))
            .collect::<R<Vec<_>>>()?;
        let types: Vec<_> = args
            .iter()
            .zip(&bound)
            .map(|(arg, bound)| (!arg.aggregate_unknown(parameters)).then(|| bound.ty.clone()))
            .collect();
        let source = selected(&types)?;
        let mut normalized = Vec::with_capacity(args.len());
        for (i, mut bound) in bound.into_iter().enumerate() {
            let ty = if i < 2 {
                source.clone()
            } else {
                DataType::Integer
            };
            if types[i].is_none() {
                let Node::Literal(value) = bound.node else {
                    return Err(SqlError::Parameters);
                };
                bound = BoundScalar {
                    ty: ty.clone(),
                    node: Node::Literal(cast_value(value, &ty, &mut 0)?),
                };
            } else if bound.ty != ty {
                bound = BoundScalar {
                    ty,
                    node: Node::Cast(Box::new(bound)),
                };
            }
            normalized.push(bound);
        }
        Ok(BoundScalar {
            ty: source,
            node: Node::Call("overlay".into(), normalized),
        })
    }
}

pub(super) fn eval(v: &[Value], budget: &mut usize) -> R<Value> {
    if !(3..=4).contains(&v.len()) {
        return Err(SqlError::UndefinedFunction);
    }
    if v.contains(&Value::Null) {
        return Ok(Value::Null);
    }
    let num = |i| match v[i] {
        Value::Int(n) => i32::try_from(n).map_err(|_| SqlError::Overflow),
        _ => Err(SqlError::Type),
    };
    let start = num(2)?;
    // Production evaluation already charges/clones bounded child Values. The kernel
    // additionally bounds borrowed input scans without allocating output or changing budget.
    for value in &v[..2] {
        let size = match value {
            Value::Text(s) => s.len(),
            Value::Bytes(b) => b.len(),
            _ => return Err(SqlError::Type),
        };
        if size > MAX_ROW_BYTES {
            return Err(SqlError::Limit);
        }
    }
    let replacement_len = match (&v[0], &v[1]) {
        (Value::Text(_), Value::Text(s)) => s.chars().count(),
        (Value::Bytes(_), Value::Bytes(b)) => b.len(),
        _ => return Err(SqlError::Type),
    };
    let count = if v.len() == 4 {
        num(3)?
    } else {
        i32::try_from(replacement_len).map_err(|_| SqlError::Overflow)?
    };
    if start <= 0 {
        return Err(SqlError::SubstringError);
    }
    let end = start.checked_add(count).ok_or(SqlError::Overflow)?;
    let prefix = (start - 1) as usize;
    let suffix = (end.max(1) - 1) as usize;
    match (&v[0], &v[1]) {
        (Value::Text(source), Value::Text(replacement)) => {
            let offset = |n| {
                source
                    .char_indices()
                    .nth(n)
                    .map_or(source.len(), |(i, _)| i)
            };
            let (prefix, suffix) = (offset(prefix), offset(suffix));
            let size = prefix
                .checked_add(replacement.len())
                .and_then(|n| n.checked_add(source.len() - suffix))
                .ok_or(SqlError::Limit)?;
            reserve(budget, size)?;
            let mut out = String::with_capacity(size);
            out.push_str(&source[..prefix]);
            out.push_str(replacement);
            out.push_str(&source[suffix..]);
            Ok(Value::Text(out))
        }
        (Value::Bytes(source), Value::Bytes(replacement)) => {
            let (prefix, suffix) = (prefix.min(source.len()), suffix.min(source.len()));
            let size = prefix
                .checked_add(replacement.len())
                .and_then(|n| n.checked_add(source.len() - suffix))
                .ok_or(SqlError::Limit)?;
            reserve(budget, size)?;
            let mut out = Vec::with_capacity(size);
            out.extend_from_slice(&source[..prefix]);
            out.extend_from_slice(replacement);
            out.extend_from_slice(&source[suffix..]);
            Ok(Value::Bytes(out))
        }
        _ => Err(SqlError::Type),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn values(source: Value, replacement: Value, start: i64, count: i64) -> [Value; 4] {
        [source, replacement, Value::Int(start), Value::Int(count)]
    }
    #[test]
    fn overlay_exact_output_aggregate_and_rejection_budgets() {
        for binary in [false, true] {
            let make = |n| {
                if binary {
                    Value::Bytes(vec![b'a'; n])
                } else {
                    Value::Text("a".repeat(n))
                }
            };
            let input = values(make(MAX_ROW_BYTES), make(0), 1, 0);
            let mut budget = MAX_RESULT_BYTES - MAX_ROW_BYTES;
            assert_eq!(eval(&input, &mut budget).unwrap(), make(MAX_ROW_BYTES));
            assert_eq!(budget, MAX_RESULT_BYTES);
            let mut budget = MAX_RESULT_BYTES - MAX_ROW_BYTES + 1;
            let before = budget;
            assert_eq!(eval(&input, &mut budget), Err(SqlError::Limit));
            assert_eq!(budget, before);
            let doubled = values(make(MAX_ROW_BYTES), make(0), 2, -1);
            let mut budget = 17;
            let before = budget;
            assert_eq!(eval(&doubled, &mut budget), Err(SqlError::Limit));
            assert_eq!(budget, before);
            let oversized = values(make(MAX_ROW_BYTES + 1), make(0), 1, 1);
            assert_eq!(eval(&oversized, &mut budget), Err(SqlError::Limit));
            assert_eq!(budget, before);
            let oversized = values(make(0), make(MAX_ROW_BYTES + 1), 1, 0);
            assert_eq!(eval(&oversized, &mut budget), Err(SqlError::Limit));
            assert_eq!(budget, before);
            let overflow = values(make(1), make(0), i32::MAX as i64, 1);
            assert_eq!(eval(&overflow, &mut budget), Err(SqlError::Overflow));
            assert_eq!(budget, before);
            let badstart = values(make(1), make(0), 0, 0);
            assert_eq!(eval(&badstart, &mut budget), Err(SqlError::SubstringError));
            assert_eq!(budget, before);
            let huge = values(make(1), make(1), i32::MAX as i64, 0);
            assert_eq!(eval(&huge, &mut budget).unwrap(), make(2));
            assert_eq!(budget, before + 2);
        }
    }
    #[test]
    fn overlay_unicode_bytes_negative_count_and_strict_null() {
        let mut budget = 0;
        assert_eq!(
            eval(
                &values(
                    Value::Text("aé🙂éZ".into()),
                    Value::Text("界🙂".into()),
                    2,
                    2
                ),
                &mut budget
            )
            .unwrap(),
            Value::Text("a界🙂éZ".into())
        );
        assert_eq!(
            eval(
                &values(Value::Text("abc".into()), Value::Text("X".into()), 2, -1),
                &mut budget
            )
            .unwrap(),
            Value::Text("aXabc".into())
        );
        assert_eq!(
            eval(
                &values(Value::Bytes(vec![0, 255, 0]), Value::Bytes(vec![128]), 2, 1),
                &mut budget
            )
            .unwrap(),
            Value::Bytes(vec![0, 128, 0])
        );
        let before = budget;
        assert_eq!(
            eval(&[Value::Null, Value::Null, Value::Int(0)], &mut budget).unwrap(),
            Value::Null
        );
        assert_eq!(budget, before);
    }
}

#[cfg(test)]
mod bind_work_tests {
    use super::*;
    #[test]
    fn overlay_bind_depth_eight_has_single_visit_per_call() {
        let mut expression = Scalar::Literal(Value::Text("abc".into()));
        for _ in 0..8 {
            expression = Scalar::Call(
                "overlay".into(),
                vec![
                    expression,
                    Scalar::Literal(Value::Text("X".into())),
                    Scalar::Literal(Value::Int(2)),
                ],
            );
        }
        OVERLAY_BIND_VISITS.with(|n| n.set(0));
        let bound = expression
            .bind(&|_, _| Err(SqlError::UnknownColumn), &[], &[])
            .unwrap();
        assert_eq!(bound.ty, DataType::Text);
        let visits = OVERLAY_BIND_VISITS.with(|n| n.get());
        eprintln!("overlay bind depth=8 visits={visits}");
        assert_eq!(visits, 8);
    }
}

#[cfg(test)]
mod bind_max_depth_tests {
    use super::*;
    #[test]
    fn overlay_bind_depth_thirty_two_is_linear() {
        let mut expression = Scalar::Literal(Value::Text("abc".into()));
        for _ in 0..32 {
            expression = Scalar::Call(
                "overlay".into(),
                vec![
                    expression,
                    Scalar::Literal(Value::Text("X".into())),
                    Scalar::Literal(Value::Int(2)),
                ],
            );
        }
        OVERLAY_BIND_VISITS.with(|n| n.set(0));
        expression
            .bind(&|_, _| Err(SqlError::UnknownColumn), &[], &[])
            .unwrap();
        assert_eq!(OVERLAY_BIND_VISITS.with(|n| n.get()), 32);
    }
}
