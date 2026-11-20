//! PostgreSQL 18 POSITION over supported text and bytea types.
//! Independent implementation; no upstream source copied.
use super::*;

fn family(types: &[Option<&DataType>]) -> R<DataType> {
    if types.len() != 2 {
        return Err(SqlError::UndefinedFunction);
    }
    let mut selected = None;
    for ty in types.iter().flatten() {
        if **ty == DataType::Bpchar {
            return Err(SqlError::Unsupported);
        }
        let ty = if **ty == DataType::Varchar {
            &&DataType::Text
        } else {
            ty
        };
        if !matches!(ty, DataType::Text | DataType::Bytes)
            || selected.as_ref().is_some_and(|old| old != *ty)
        {
            return Err(SqlError::UndefinedFunction);
        }
        selected = Some((*ty).clone());
    }
    Ok(selected.unwrap_or(DataType::Text))
}

impl Scalar {
    pub(super) fn bind_position(
        args: &[Self],
        resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
        tables: &[&TableDef],
        parameters: &[Option<DataType>],
    ) -> R<BoundScalar> {
        // Bind every child exactly once, derive types from those values, then
        // consume them. Nested calls must not be rebound for signature lookup.
        let bound = args
            .iter()
            .map(|a| a.bind(resolve, tables, parameters))
            .collect::<R<Vec<_>>>()?;
        let types = args
            .iter()
            .zip(&bound)
            .map(|(a, b)| (!a.aggregate_unknown(parameters)).then_some(&b.ty))
            .collect::<Vec<_>>();
        let ty = family(&types)?;
        let args = args
            .iter()
            .zip(bound)
            .map(|(source, bound)| {
                if source.aggregate_unknown(parameters) {
                    let Node::Literal(value) = bound.node else {
                        return Err(SqlError::Parameters);
                    };
                    Ok(BoundScalar {
                        ty: ty.clone(),
                        node: Node::Literal(cast_value(value, &ty, &mut 0)?),
                    })
                } else {
                    Ok(bound)
                }
            })
            .collect::<R<Vec<_>>>()?;
        Ok(BoundScalar {
            ty: DataType::Integer,
            node: Node::Call("position".into(), args),
        })
    }

    pub(super) fn infer_position(
        args: &[Self],
        parameters: &mut [Option<DataType>],
        resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
        tables: &[&TableDef],
    ) -> R<()> {
        // Child order is PostgreSQL's transformed source,needle order.
        for arg in args {
            arg.infer(None, parameters, resolve, tables)?;
        }
        let types = args
            .iter()
            .map(|a| {
                if a.aggregate_unknown(parameters) {
                    Ok(None)
                } else {
                    Ok(Some(a.bind(resolve, tables, parameters)?.ty))
                }
            })
            .collect::<R<Vec<_>>>()?;
        let ty = family(&types.iter().map(Option::as_ref).collect::<Vec<_>>())?;
        for (arg, known) in args.iter().zip(types) {
            if known.is_none() {
                arg.infer(Some(&ty), parameters, resolve, tables)?;
            }
        }
        Ok(())
    }
}

pub(super) fn eval(name: &str, values: &[Value], budget: &mut usize) -> Option<R<Value>> {
    (name == "position").then(|| evaluate(values, budget))
}

fn evaluate(values: &[Value], budget: &mut usize) -> R<Value> {
    if values.len() != 2 {
        return Err(SqlError::UndefinedFunction);
    }
    if values.contains(&Value::Null) {
        return Ok(Value::Null);
    }
    let index = match (&values[0], &values[1]) {
        (Value::Text(source), Value::Text(needle)) => {
            if source.len() > crate::relational::max_value_bytes() || needle.len() > crate::relational::max_value_bytes() {
                return Err(SqlError::Limit);
            }
            source.find(needle).map(|i| source[..i].chars().count())
        }
        (Value::Bytes(source), Value::Bytes(needle)) => {
            if source.len() > crate::relational::max_value_bytes() || needle.len() > crate::relational::max_value_bytes() {
                return Err(SqlError::Limit);
            }
            byte_index(source, needle)
        }
        _ => return Err(SqlError::UndefinedFunction),
    };
    let result = match index {
        Some(index) => index
            .checked_add(1)
            .and_then(|i| i32::try_from(i).ok())
            .ok_or(SqlError::Overflow)?,
        None => 0,
    };
    reserve(budget, 8)?;
    Ok(Value::Int(i64::from(result)))
}

// Critical factorization for the constant-space two-way substring search.
// `left` denotes one past the maximal suffix's starting index; using unsigned
// indices avoids the conventional -1 sentinel and its conversions.
fn maximal_suffix(needle: &[u8], reverse: bool) -> (usize, usize) {
    let (mut left, mut right, mut offset, mut period) = (0, 1, 0, 1);
    while right + offset < needle.len() {
        let (a, b) = (needle[left + offset], needle[right + offset]);
        count_comparison();
        if a == b {
            if offset + 1 == period {
                right += period;
                offset = 0;
            } else {
                offset += 1;
            }
        } else if (a > b) ^ reverse {
            right += offset + 1;
            offset = 0;
            period = right - left;
        } else {
            left = right;
            right += 1;
            offset = 0;
            period = 1;
        }
    }
    (left, period)
}

fn byte_index(source: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    if needle.len() > source.len() {
        return None;
    }
    let a = maximal_suffix(needle, false);
    let b = maximal_suffix(needle, true);
    let (cut, mut period) = if a.0 > b.0 { a } else { b };
    let periodic =
        period <= needle.len() - cut && equal_bytes(&needle[..cut], &needle[period..period + cut]);
    if !periodic {
        period = cut.max(needle.len() - cut) + 1;
    }
    let (mut start, mut memory) = (0, 0);
    while start <= source.len() - needle.len() {
        let mut i = cut.max(memory);
        while i < needle.len() && equal_byte(needle[i], source[start + i]) {
            i += 1;
        }
        if i < needle.len() {
            start += i - cut + 1;
            memory = 0;
            continue;
        }
        i = cut;
        while i > memory && equal_byte(needle[i - 1], source[start + i - 1]) {
            i -= 1;
        }
        if i <= memory {
            return Some(start);
        }
        start += period;
        memory = if periodic { needle.len() - period } else { 0 };
    }
    None
}

#[cfg(test)]
thread_local! { static COMPARISONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
#[inline]
fn count_comparison() {
    #[cfg(test)]
    COMPARISONS.with(|n| n.set(n.get() + 1));
}
#[inline]
fn equal_byte(a: u8, b: u8) -> bool {
    count_comparison();
    a == b
}
fn equal_bytes(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(a, b)| equal_byte(*a, *b))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn reference(source: &[u8], needle: &[u8]) -> Option<usize> {
        if needle.is_empty() {
            Some(0)
        } else {
            source.windows(needle.len()).position(|w| w == needle)
        }
    }
    fn search(source: &[u8], needle: &[u8]) {
        COMPARISONS.with(|n| n.set(0));
        assert_eq!(
            byte_index(source, needle),
            reference(source, needle),
            "source={source:?}, needle={needle:?}"
        );
        let comparisons = COMPARISONS.with(|n| n.get());
        assert!(
            comparisons <= 12 * (source.len() + needle.len()) + 32,
            "comparisons={comparisons}, source={}, needle={}",
            source.len(),
            needle.len()
        );
    }
    #[test]
    fn position_two_way_exhaustive_tiny_alphabet() {
        let strings: Vec<Vec<u8>> = (0..=9)
            .flat_map(|n| {
                (0..1usize << n).map(move |bits| {
                    (0..n)
                        .map(|i| if bits >> i & 1 == 0 { 0 } else { 255 })
                        .collect()
                })
            })
            .collect();
        for source in &strings {
            for needle in &strings {
                search(source, needle);
            }
        }
        // Enumerate a ternary alphabet to exercise both lexical factorizations.
        let strings: Vec<Vec<u8>> = (0..=5)
            .flat_map(|n| {
                (0..3usize.pow(n)).map(move |mut v| {
                    (0..n)
                        .map(|_| {
                            let b = [0, 128, 255][v % 3];
                            v /= 3;
                            b
                        })
                        .collect()
                })
            })
            .collect();
        for source in &strings {
            for needle in &strings {
                search(source, needle);
            }
        }
    }
    #[test]
    fn position_two_way_adversarial_linear_comparisons() {
        for m in [1, 2, 3, 31, 1024, 32768, 65535] {
            for mode in 0..6 {
                let mut source = vec![b'a'; 65536];
                let mut needle = vec![b'a'; m];
                match mode {
                    0 => needle[m - 1] = b'b',
                    1 => needle[0] = b'b',
                    2 => {
                        for (i, b) in source.iter_mut().enumerate() {
                            *b = if i % 2 == 0 { b'a' } else { b'b' };
                        }
                        for (i, b) in needle.iter_mut().enumerate() {
                            *b = if i % 2 == 0 { b'a' } else { b'b' };
                        }
                        needle[m - 1] = b'c';
                    }
                    3 => {
                        for (i, b) in source.iter_mut().enumerate() {
                            *b = (i % 251) as u8;
                        }
                        for (i, b) in needle.iter_mut().enumerate() {
                            *b = (i % 251) as u8;
                        }
                    }
                    4 => source[65535] = b'b',
                    _ => {
                        source.reverse();
                        needle.reverse();
                    }
                }
                search(&source, &needle);
            }
        }
    }
    #[test]
    fn position_kernel_exact_budget_null_type_and_input_limits() {
        for (values, expected) in [
            (
                vec![Value::Text("aé😀z".into()), Value::Text("😀".into())],
                Value::Int(3),
            ),
            (
                vec![Value::Bytes(vec![1, 0, 255, 2]), Value::Bytes(vec![0, 255])],
                Value::Int(2),
            ),
            (
                vec![Value::Bytes(vec![]), Value::Bytes(vec![])],
                Value::Int(1),
            ),
            (
                vec![Value::Text("abc".into()), Value::Text("x".into())],
                Value::Int(0),
            ),
        ] {
            let mut budget = MAX_RESULT_BYTES - 8;
            assert_eq!(evaluate(&values, &mut budget).unwrap(), expected);
            assert_eq!(budget, MAX_RESULT_BYTES);
            let mut budget = MAX_RESULT_BYTES - 7;
            assert_eq!(evaluate(&values, &mut budget), Err(SqlError::Limit));
            assert_eq!(budget, MAX_RESULT_BYTES - 7);
        }
        let mut budget = 0;
        assert_eq!(
            evaluate(&[Value::Null, Value::Bytes(vec![])], &mut budget).unwrap(),
            Value::Null
        );
        assert_eq!(budget, 0);
        assert_eq!(
            evaluate(&[Value::Bool(false), Value::Text("".into())], &mut budget),
            Err(SqlError::UndefinedFunction)
        );
        assert_eq!(budget, 0);
        assert_eq!(
            evaluate(
                &[
                    Value::Bytes(vec![0; MAX_ROW_BYTES + 1]),
                    Value::Bytes(vec![])
                ],
                &mut budget
            ),
            Err(SqlError::Limit)
        );
        assert_eq!(budget, 0);
        let mut budget = usize::MAX;
        assert_eq!(
            evaluate(
                &[Value::Text("".into()), Value::Text("".into())],
                &mut budget
            ),
            Err(SqlError::Limit)
        );
        assert_eq!(budget, usize::MAX);
    }
    #[test]
    fn position_bind_nested_child_is_visited_once() {
        let mut scalar = Scalar::Column(ColumnRef {
            qualifier: None,
            name: "txt".into(),
        });
        for _ in 0..12 {
            scalar = Scalar::Call(
                "position".into(),
                vec![
                    Scalar::Cast(Box::new(scalar), DataType::Text),
                    Scalar::Literal(Value::Text("x".into())),
                ],
            );
        }
        let mut state = Relational::default();
        let create = Prepared::parse(
            "CREATE TABLE position_visit(id BIGINT PRIMARY KEY,txt TEXT)",
            1,
            state.epoch,
        )
        .unwrap();
        state
            .execute_with_cancel(&create, &[], None, || false)
            .unwrap();
        let table = &state.tables["position_visit"];
        let visits = std::cell::Cell::new(0usize);
        let resolver = |_: &ColumnRef, _: usize| {
            visits.set(visits.get() + 1);
            Ok(BoundColumn {
                source: 0,
                column: 1,
            })
        };
        assert!(scalar.bind(&resolver, &[table], &[]).is_ok());
        assert_eq!(visits.get(), 1);
    }
}
