//! FOR-before-FROM numeric overload resolution.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Family {
    TextNumeric,
    BytesNumeric,
    TextRegex,
}

fn family(types: [Option<&DataType>; 3]) -> R<Family> {
    if types.iter().flatten().any(|ty| **ty == DataType::Bpchar) {
        return Err(SqlError::Unsupported);
    }
    let source_text = types[0].is_none_or(|t| matches!(t, DataType::Text | DataType::Varchar));
    let source_bytes = types[0].is_none_or(|t| *t == DataType::Bytes);
    let text = types[1..]
        .iter()
        .all(|t| t.is_none_or(|t| matches!(t, DataType::Text | DataType::Varchar)));
    let numeric = types[1..]
        .iter()
        .all(|t| t.is_none_or(|t| matches!(t, DataType::SmallInt | DataType::Integer)));
    if source_text && text {
        Ok(Family::TextRegex)
    } else if source_text && numeric {
        Ok(Family::TextNumeric)
    } else if source_bytes && numeric {
        Ok(Family::BytesNumeric)
    } else {
        Err(SqlError::UndefinedFunction)
    }
}

impl Scalar {
    fn substring_order_types(
        args: &[Box<Self>; 3],
        resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
        tables: &[&TableDef],
        parameters: &[Option<DataType>],
    ) -> R<[Option<DataType>; 3]> {
        let mut types = [None, None, None];
        for (i, arg) in args.iter().enumerate() {
            if !arg.aggregate_unknown(parameters) {
                types[i] = Some(arg.bind(resolve, tables, parameters)?.ty);
            }
        }
        Ok(types)
    }

    pub(super) fn infer_substring_order(
        args: &[Box<Self>; 3],
        parameters: &mut [Option<DataType>],
        resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
        tables: &[&TableDef],
    ) -> R<()> {
        // PostgreSQL transforms both syntactic orders before child analysis.
        for arg in args {
            arg.infer(None, parameters, resolve, tables)?;
        }
        let types = Self::substring_order_types(args, resolve, tables, parameters)?;
        let selected = family(types.each_ref().map(Option::as_ref))?;
        let expected = match selected {
            Family::TextNumeric => [DataType::Text, DataType::Integer, DataType::Integer],
            Family::BytesNumeric => [DataType::Bytes, DataType::Integer, DataType::Integer],
            Family::TextRegex => [DataType::Text, DataType::Text, DataType::Text],
        };
        for (i, arg) in args.iter().enumerate() {
            // Preserve already selected parameter widths; widen at the call boundary.
            if types[i].is_none() {
                arg.infer(Some(&expected[i]), parameters, resolve, tables)?;
            }
        }
        Ok(())
    }

    pub(super) fn bind_substring_order(
        args: &[Box<Self>; 3],
        resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
        tables: &[&TableDef],
        parameters: &[Option<DataType>],
    ) -> R<BoundScalar> {
        let bound = [
            args[0].bind(resolve, tables, parameters)?,
            args[1].bind(resolve, tables, parameters)?,
            args[2].bind(resolve, tables, parameters)?,
        ];
        let types = std::array::from_fn(|i| {
            (!args[i].aggregate_unknown(parameters)).then_some(&bound[i].ty)
        });
        let source = match family(types)? {
            Family::TextNumeric => DataType::Text,
            Family::BytesNumeric => DataType::Bytes,
            Family::TextRegex => return Err(SqlError::Unsupported),
        };
        let result = source.clone();
        let expected = [source, DataType::Integer, DataType::Integer];
        let mut normalized = Vec::with_capacity(3);
        for ((i, arg), ty) in bound.into_iter().enumerate().zip(expected) {
            let arg = if args[i].aggregate_unknown(parameters) {
                let Node::Literal(value) = arg.node else {
                    return Err(SqlError::Parameters);
                };
                BoundScalar {
                    ty: ty.clone(),
                    node: Node::Literal(cast_value(value, &ty, &mut 0)?),
                }
            } else if arg.ty != ty {
                BoundScalar {
                    ty,
                    node: Node::Cast(Box::new(arg)),
                }
            } else {
                arg
            };
            normalized.push(arg);
        }
        Ok(BoundScalar {
            ty: result,
            node: Node::Call("substring".into(), normalized),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> Relational {
        let mut state = Relational::default();
        let create = Prepared::parse(
            "CREATE TABLE substring_ins(id BIGINT PRIMARY KEY,txt TEXT)",
            1,
            state.epoch,
        )
        .unwrap();
        state
            .execute_with_cancel(&create, &[], None, || false)
            .unwrap();
        let table = state.tables["substring_ins"].clone();
        state
            .create_secondary_index_engine(
                &table,
                state.epoch,
                "substring_txt",
                &[table.columns[1].id],
                || false,
            )
            .unwrap();
        state
    }

    #[test]
    fn substring_order_insert_visits_literals_and_shape_in_all_three_positions() {
        let state = state();
        let huge = format!("'{}'", "x".repeat(65_537));
        let leaf = format!("concat({})", vec!["'1'"; 256].join(","));
        let wide = format!("concat({})", vec![leaf.as_str(); 20].join(","));
        for value in [huge, wide] {
            for position in 0..3 {
                let mut children = ["'abc'".to_string(), "1".into(), "1".into()];
                children[position] = value.clone();
                let query = format!(
                    "INSERT INTO substring_ins VALUES(1,SUBSTRING({} FOR {} FROM {}))",
                    children[0], children[2], children[1]
                );
                assert!(
                    matches!(
                        Prepared::parse(&query, 1, state.epoch),
                        Err(SqlError::Limit)
                    ),
                    "position={position}"
                );
            }
        }
    }

    #[test]
    fn substring_order_insert_cancellation_and_duplicate_preserve_complete_state() {
        let base = state();
        let query = "INSERT INTO substring_ins VALUES(1,SUBSTRING('abcdef' FOR 2 FROM 3))";
        let p = Prepared::parse(query, 1, base.epoch).unwrap();
        let mut success = base.clone();
        let mut calls = 0;
        assert_eq!(
            success
                .execute_with_cancel(&p, &[], None, || {
                    calls += 1;
                    false
                })
                .unwrap()
                .affected,
            1
        );
        for stop in 1..=calls {
            let mut candidate = base.clone();
            let mut seen = 0;
            assert_eq!(
                candidate.execute_with_cancel(&p, &[], None, || {
                    seen += 1;
                    seen == stop
                }),
                Err(SqlError::Aborted),
                "callback={stop}"
            );
            assert_eq!(candidate, base, "complete state rollback callback={stop}");
        }
        let before = success.clone();
        assert_eq!(
            success.execute_with_cancel(&p, &[], None, || false),
            Err(SqlError::Duplicate)
        );
        assert_eq!(success, before);
        success.secondary_validate_engine(|| false).unwrap();
    }
}
