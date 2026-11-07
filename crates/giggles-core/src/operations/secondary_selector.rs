//! Conservative native selector shared by execution and Explain.
use super::*;
use crate::relational::secondary_index_access::{Endpoint, Range, Request as IndexRequest};

#[cfg(test)]
thread_local! {
    static SUCCESSES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(super) struct Choice<'a> {
    equal: Vec<(u64, Value)>,
    range: Option<Range<'a>>,
    projection: Vec<u64>,
}
fn terms<'a>(b: &'a Bound, out: &mut Vec<(usize, Compare, &'a Value)>) -> Option<()> {
    match b {
        Bound::And(a, b) => {
            terms(a, out)?;
            terms(b, out)
        }
        Bound::Compare(a, op, b) => {
            let (Bound::Field(i), Bound::Value(v)) = (a.as_ref(), b.as_ref()) else {
                return None;
            };
            if matches!(v, Value::Null) || *op == Compare::Ne {
                return None;
            }
            out.push((*i, *op, v));
            Some(())
        }
        _ => None,
    }
}
pub(super) fn choose<'a>(table: &TableDef, filter: &'a Bound) -> Option<Choice<'a>> {
    if table.hidden_rowid().is_some() {
        return None;
    }
    let mut ts = Vec::new();
    terms(filter, &mut ts)?;
    let mut equal = Vec::new();
    let mut range: Option<Range<'a>> = None;
    for (i, op, value) in ts {
        let c = table.columns.get(i)?;
        if !matches!(
            c.data_type,
            DataType::Boolean
                | DataType::SmallInt
                | DataType::Integer
                | DataType::BigInt
                | DataType::Text
                | DataType::Bytes
        ) {
            return None;
        }
        if op == Compare::Eq {
            if range.is_some() || equal.iter().any(|(id, _)| *id == c.id) {
                return None;
            }
            equal.push((c.id, value.clone()));
        } else {
            if equal.iter().any(|(id, _)| *id == c.id) {
                return None;
            }
            let r = range.get_or_insert(Range {
                column: c.id,
                lower: None,
                upper: None,
            });
            if r.column != c.id {
                return None;
            }
            let endpoint = Endpoint {
                value,
                inclusive: matches!(op, Compare::Le | Compare::Ge),
            };
            let slot = if matches!(op, Compare::Gt | Compare::Ge) {
                &mut r.lower
            } else {
                &mut r.upper
            };
            if slot.is_some() {
                return None;
            }
            *slot = Some(endpoint);
        }
    }
    if equal.is_empty() && range.is_none() {
        return None;
    }
    Some(Choice {
        equal,
        range,
        projection: table.columns.iter().map(|c| c.id).collect(),
    })
}
impl Choice<'_> {
    pub(super) fn request(&self) -> IndexRequest<'_> {
        IndexRequest {
            equal: &self.equal,
            range: self.range,
            projection: &self.projection,
            descending: false,
            residuals: &[],
            max_rows: MAX_ROWS,
            max_bytes: crate::relational::MAX_RESULT_BYTES,
        }
    }
    pub(super) fn descriptor(
        &self,
        table: &TableDef,
        ds: &[crate::relational::secondary_index::Descriptor],
    ) -> Option<u64> {
        ds.iter()
            .filter(|d| {
                d.table == table.id
                    && self.equal.len() <= d.columns.len()
                    && self
                        .equal
                        .iter()
                        .zip(&d.columns)
                        .all(|((id, _), c)| id == c)
                    && self
                        .range
                        .is_none_or(|r| d.columns.get(self.equal.len()) == Some(&r.column))
            })
            .map(|d| d.id)
            .min()
    }
}

pub(super) fn read(
    s: &mut impl Context,
    table: &TableDef,
    filter: &Bound,
    fallback: Vec<SqlRow>,
) -> Result<Vec<SqlRow>> {
    if table.hidden_rowid().is_some() {
        return Err(Error::Unsupported);
    }
    let Some(choice) = choose(table, filter) else {
        return Ok(fallback);
    };
    let Ok(session) = s.session() else {
        return Ok(fallback);
    };
    match session.secondary_read_native_engine(table, &choice.request()) {
        Ok(result) => {
            #[cfg(test)]
            SUCCESSES.with(|n| n.set(n.get() + 1));
            let mut rows = result.rows;
            // Native's stable order ties start in complete primary-key order,
            // independently of the physical index order (including NULL order).
            let order: Vec<_> = table.primary_key.iter().map(|p| (*p, false)).collect();
            sort(&mut rows, &order);
            Ok(rows)
        }
        // This optional access path can reserve more projection bytes/work than
        // the already admitted complete input (notably Numeric payloads). Its
        // private quota must not narrow existing native read semantics. Residual
        // evaluation and output admission still run on the complete fallback.
        Err(CoreError::Sql(SqlError::Unsupported | SqlError::Limit)) => Ok(fallback),
        Err(CoreError::Limit) => Err(Error::Limit),
        Err(e) => Err(core(e)),
    }
}

#[cfg(test)]
mod tests;
