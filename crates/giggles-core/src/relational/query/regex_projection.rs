//! Expansion of regular expression set-returning SELECT items.
use super::*;

type SetValues = Vec<Option<Vec<Value>>>;

impl BoundQuery<'_> {
    pub(super) fn execute_regex_projection(
        &self,
        input: impl Iterator<Item = R<Tuple>>,
        params: &[Value],
        page: scan::Page,
        execution: &Execution<'_>,
        mut output: Output,
        mut result: SqlResult,
    ) -> R<SqlResult> {
        let order_slots = self.set_order.as_ref().expect("set-returning projection");
        let rows = input.flat_map(
            |row| match row.and_then(|row| self.expand_sets(row, params)) {
                Ok((row, sets, len)) => {
                    let expanded = (0..len).map(move |index| {
                        execution.comparison()?;
                        let mut work = 0;
                        let mut bytes = 0;
                        let values = self
                            .items
                            .iter()
                            .zip(&sets)
                            .map(|(item, set)| {
                                let value = match (item, set) {
                                    (_, Some(set)) => {
                                        set.get(index).cloned().unwrap_or(Value::Null)
                                    }
                                    (BoundItem::Scalar(expr), None) => {
                                        expr.eval(&row, params, &mut work)?
                                    }
                                    (BoundItem::Column(column), None) => row.get(*column).clone(),
                                    (BoundItem::Ctid(column), None) => match row.get(*column) {
                                        Value::Int(id) => {
                                            Value::Text(super::super::ctid::format(*id)?)
                                        }
                                        Value::Null => Value::Null,
                                        _ => return Err(SqlError::Type),
                                    },
                                    (BoundItem::Aggregate(..), None) => {
                                        return Err(SqlError::GroupingError);
                                    }
                                };
                                charge_projected(&mut bytes, &value)?;
                                Ok(value)
                            })
                            .collect::<R<Vec<_>>>()?;
                        let keys = self
                            .order
                            .iter()
                            .zip(order_slots)
                            .map(|((expr, _, _), slot)| match slot {
                                Some(slot) if self.columns[*slot].data_type == expr.ty => {
                                    Ok(values[*slot].clone())
                                }
                                _ => expr.eval(&row, params, &mut work),
                            })
                            .collect::<R<Vec<_>>>()?;
                        Ok((
                            SqlRow {
                                values,
                                version: row.version,
                            },
                            keys,
                        ))
                    });
                    Box::new(expanded) as Box<dyn Iterator<Item = R<_>>>
                }
                Err(error) => Box::new(std::iter::once(Err(error))),
            },
        );

        let mut retention = RetentionScope::new(execution);
        let rows: Box<dyn Iterator<Item = R<_>>> = if self.query.distinct() {
            Box::new(spill::unique_external(
                rows,
                keyed_row_bytes,
                |a: &(SqlRow, Vec<Value>), b: &(SqlRow, Vec<Value>)| {
                    grouping::cmp_output(&a.0.values, &b.0.values, &self.items)
                },
                &mut retention,
            )?)
        } else {
            Box::new(rows)
        };
        let rows: Box<dyn Iterator<Item = R<_>>> = if self.order.is_empty() {
            rows
        } else {
            let cmp = |a: &(SqlRow, Vec<Value>), b: &(SqlRow, Vec<Value>)| {
                order_values(&a.1, &b.1, &self.order)
            };
            if execution.unbounded.get() && page.keep().is_none_or(|keep| keep > 10_000) {
                Box::new(spill::sort_external(
                    rows,
                    keyed_row_bytes,
                    cmp,
                    &mut retention,
                )?)
            } else {
                Box::new(
                    scan::sort_bounded(rows, page.keep(), keyed_row_bytes, cmp, &mut retention)?
                        .into_iter()
                        .map(Ok),
                )
            }
        };
        // Check projected errors before skipping OFFSET rows.
        for row in take_set_rows(rows, page) {
            output.push(row?.0, &mut result)?;
        }
        output.finish(&mut result, execution)?;
        Ok(result)
    }

    fn expand_sets(&self, row: Tuple, params: &[Value]) -> R<(Tuple, SetValues, usize)> {
        let sets = self
            .items
            .iter()
            .map(|item| match item {
                BoundItem::Scalar(expr) => expr.regex_set(&row, params),
                _ => Ok(None),
            })
            .collect::<R<Vec<_>>>()?;
        let len = sets.iter().flatten().map(Vec::len).max().unwrap_or(0);
        Ok((row, sets, len))
    }
}

fn take_set_rows<T>(
    mut input: impl Iterator<Item = R<T>>,
    mut page: scan::Page,
) -> impl Iterator<Item = R<T>> {
    std::iter::from_fn(move || {
        while page.limit != Some(0) {
            let row = input.next()?;
            if row.is_err() {
                return Some(row);
            }
            if page.offset == 0 {
                if let Some(n) = &mut page.limit {
                    *n -= 1;
                }
                return Some(row);
            }
            page.offset -= 1;
        }
        None
    })
}
