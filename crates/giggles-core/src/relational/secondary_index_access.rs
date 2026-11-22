//! Immediate, private engine access. No planner, SQL semantics, durable handle,
//! partition authority, persistence or public CREATE capability is implied.
//! Equality/NULL and ranges use canonical key order (NULL first, binary strings).
//! Callers needing SQL three-valued predicates must not lower them blindly here.
use super::secondary_index::{self as si, Descriptor};
use super::*;

#[derive(Clone, Copy)]
pub(crate) struct Endpoint<'a> {
    pub value: &'a Value,
    pub inclusive: bool,
}
#[derive(Clone, Copy)]
pub(crate) struct Range<'a> {
    pub column: u64,
    pub lower: Option<Endpoint<'a>>,
    pub upper: Option<Endpoint<'a>>,
}
/// Stable IDs, in descriptor order. All residual predicates are deliberately
/// unsupported. `descending` rejects rather than silently sorting/reversing.
/// The sole order offered is ascending full index tuple, then complete PK.
pub(crate) struct Request<'a> {
    pub equal: &'a [(u64, Value)],
    pub range: Option<Range<'a>>,
    pub projection: &'a [u64],
    pub descending: bool,
    pub residuals: &'a [()],
    pub max_rows: usize,
    pub max_bytes: usize,
}
fn cap(n: usize, max: usize) -> R<()> {
    if n > max {
        Err(SqlError::Limit)
    } else {
        Ok(())
    }
}
fn add(a: usize, b: usize) -> R<usize> {
    a.checked_add(b).ok_or(SqlError::Limit)
}
fn charge(work: &mut usize, n: usize) -> R<()> {
    *work = add(*work, n)?;
    cap(*work, si::MAX_WORK)
}
fn tick(cancel: &mut impl FnMut() -> bool) -> R<()> {
    if cancel() {
        Err(SqlError::Aborted)
    } else {
        Ok(())
    }
}
fn supported(c: &ColumnDef) -> R<()> {
    match c.data_type {
        DataType::Boolean
        | DataType::SmallInt
        | DataType::Integer
        | DataType::BigInt
        | DataType::Text
        | DataType::Varchar
        | DataType::Bytes => Ok(()),
        _ => Err(SqlError::Unsupported),
    }
}
fn column(t: &TableDef, id: u64) -> R<usize> {
    t.columns
        .iter()
        .position(|c| c.id == id)
        .ok_or(SqlError::UnknownColumn)
}
// Length-only admission precedes comparisons, scans, encoding and cloning.
fn raw(v: &Value) -> R<usize> {
    match v {
        Value::Null | Value::Bool(_) | Value::Int(_) => Ok(9),
        Value::Text(s) => Ok(s.len()),
        Value::Bytes(b) => Ok(b.len()),
        _ => Err(SqlError::Unsupported),
    }
}
fn admit(req: &Request<'_>) -> R<usize> {
    cap(req.equal.len(), MAX_COLUMNS)?;
    cap(req.projection.len(), MAX_COLUMNS)?;
    cap(req.residuals.len(), MAX_COLUMNS)?;
    cap(req.max_rows, MAX_ROWS)?;
    cap(req.max_bytes, MAX_RESULT_BYTES)?;
    let mut bytes = 0;
    for v in req.equal.iter().map(|(_, v)| v).chain(
        req.range
            .iter()
            .flat_map(|r| [r.lower, r.upper])
            .flatten()
            .map(|e| e.value),
    ) {
        bytes = add(bytes, raw(v)?)?;
        // Reserve one byte for string successor before any owned endpoint copy.
        cap(bytes, crate::relational::max_value_bytes() - 1)?;
    }
    if req.descending || !req.residuals.is_empty() {
        return Err(SqlError::Unsupported);
    }
    if req.equal.is_empty() && req.range.is_none() {
        return Err(SqlError::Unsupported);
    }
    Ok(bytes)
}
fn minimum(c: &ColumnDef, nullable: bool) -> R<Value> {
    supported(c)?;
    if nullable && !c.not_null {
        return Ok(Value::Null);
    }
    Ok(match c.data_type {
        DataType::Boolean => Value::Bool(false),
        DataType::SmallInt => Value::Int(i16::MIN.into()),
        DataType::Integer => Value::Int(i32::MIN.into()),
        DataType::BigInt => Value::Int(i64::MIN),
        DataType::Text | DataType::Varchar => Value::Text(String::new()),
        DataType::Bytes => Value::Bytes(Vec::new()),
        _ => return Err(SqlError::Unsupported),
    })
}
/// Immediate successor in this column's finite/binary scalar domain. A string's
/// successor is itself followed by NUL, not a byte increment in its encoding.
fn successor(v: &Value, c: &ColumnDef) -> R<Option<Value>> {
    Ok(match v {
        Value::Null => Some(minimum(c, false)?),
        Value::Bool(false) => Some(Value::Bool(true)),
        Value::Bool(true) => None,
        Value::Int(n) => {
            let max = match c.data_type {
                DataType::SmallInt => i16::MAX.into(),
                DataType::Integer => i32::MAX.into(),
                DataType::BigInt => i64::MAX,
                _ => return Err(SqlError::Type),
            };
            if *n == max {
                None
            } else {
                Some(Value::Int(n.checked_add(1).ok_or(SqlError::Type)?))
            }
        }
        Value::Text(s) => {
            let mut s = s.clone();
            s.push('\0');
            Some(Value::Text(s))
        }
        Value::Bytes(b) => {
            let mut b = b.clone();
            b.push(0);
            Some(Value::Bytes(b))
        }
        _ => return Err(SqlError::Unsupported),
    })
}
// Carry through exhausted columns. None denotes +infinity, not an empty key.
fn advance(values: &mut Vec<Value>, cols: &[&ColumnDef]) -> R<bool> {
    while let Some(v) = values.pop() {
        if let Some(next) = successor(&v, cols[values.len()])? {
            values.push(next);
            return Ok(true);
        }
    }
    Ok(false)
}
/// Complete canonical tuple AND complete canonical PK, encoded separately so
/// valid combined arity >64 is not mistaken for an oversized individual tuple.
fn complete(mut values: Vec<Value>, cols: &[&ColumnDef], t: &TableDef) -> R<Vec<u8>> {
    for c in &cols[values.len()..] {
        values.push(minimum(c, true)?);
    }
    let pk: Vec<_> = t
        .primary_key
        .iter()
        .map(|p| minimum(&t.columns[*p], false))
        .collect::<R<_>>()?;
    let mut out = encode_key(&values)?;
    let pk = encode_key(&pk)?;
    cap(add(out.len(), pk.len())?, si::MAX_KEY)?;
    out.extend(pk);
    Ok(out)
}
fn matches(d: &Descriptor, req: &Request<'_>) -> bool {
    req.equal.len() <= d.columns.len()
        && req.equal.iter().zip(&d.columns).all(|((id, _), c)| id == c)
        && req
            .range
            .is_none_or(|r| d.columns.get(req.equal.len()) == Some(&r.column))
}
// Non-key typed payloads remain legal. Numeric's validated representation has a
// fixed public MAX_TEXT bound; reserve it without allocating a decimal string.
// This conservative reservation may reject otherwise-small numeric projections.
fn output_size(v: &Value) -> usize {
    match v {
        Value::Numeric(_) => giggles_pg_types::MAX_TEXT,
        Value::Float4(_) => 4,
        Value::Float8(_) => 8,
        Value::Text(s) => s.len(),
        Value::Bytes(b) => b.len(),
        _ => 9,
    }
}
impl Relational {
    /// Executes immediately on this immutable snapshot. No prepared-handle or
    /// authority grant: higher session/partition/protected fences remain duties
    /// of future integrations. No LIMIT truncation: any quota failure is Err.
    #[allow(dead_code)] // Private engineering seam; frontend release remains gated.
    pub(crate) fn secondary_read_engine(
        &self,
        table: &TableDef,
        epoch: u64,
        req: &Request<'_>,
        mut cancel: impl FnMut() -> bool,
    ) -> R<SqlResult> {
        let request_bytes = admit(req)?;
        let mut work = 0usize;
        // Reserve bounded request/type/schema scans and all endpoint copies,
        // successor construction and canonical encoding before any such copy.
        charge(
            &mut work,
            add(
                request_bytes.checked_mul(64).ok_or(SqlError::Limit)?,
                MAX_COLUMNS * MAX_COLUMNS * 64
                    + si::MAX_CANDIDATES * 64,
            )?,
        )?;
        charge(
            &mut work,
            self.secondary.descriptors().len().checked_mul(si::MAX_ARITY * 64).ok_or(SqlError::Limit)?,
        )?;
        let (sidecar, base) = self.secondary_access_engine(table, epoch, &mut cancel)?;
        // Deterministic selection among only descriptors with the exact supported
        // prefix shape. No PK ordering, non-prefix or expression inference.
        let d = sidecar
            .descriptors()
            .iter()
            .filter(|d| d.table == table.id && matches(d, req))
            .min_by_key(|d| d.id)
            .ok_or(SqlError::Unsupported)?;
        let cols = d
            .columns
            .iter()
            .map(|id| Ok(&table.columns[column(table, *id)?]))
            .collect::<R<Vec<_>>>()?;
        for c in &cols {
            supported(c)?;
        }
        for (i, (_, v)) in req.equal.iter().enumerate() {
            check(v, cols[i])?;
        }
        if let Some(r) = req.range {
            if r.lower.is_none() && r.upper.is_none() {
                return Err(SqlError::Unsupported);
            }
            for e in [r.lower, r.upper].into_iter().flatten() {
                check(e.value, cols[req.equal.len()])?;
            }
        }
        let projection = req
            .projection
            .iter()
            .map(|id| column(table, *id))
            .collect::<R<Vec<_>>>()?;
        tick(&mut cancel)?;
        let equal: Vec<_> = req
            .equal
            .iter()
            .map(|(_, v)| {
                tick(&mut cancel)?;
                Ok(v.clone())
            })
            .collect::<R<_>>()?;
        tick(&mut cancel)?;
        let mut lower = equal.clone();
        let mut exhausted = false;
        if let Some(e) = req.range.and_then(|r| r.lower) {
            tick(&mut cancel)?;
            lower.push(e.value.clone());
            if !e.inclusive && !advance(&mut lower, &cols)? {
                exhausted = true;
            }
        }
        tick(&mut cancel)?;
        let lower = complete(lower, &cols, table)?;
        tick(&mut cancel)?;
        let mut upper = equal;
        let upper = if let Some(e) = req.range.and_then(|r| r.upper) {
            tick(&mut cancel)?;
            upper.push(e.value.clone());
            if e.inclusive && !advance(&mut upper, &cols)? {
                None
            } else {
                Some(complete(upper, &cols, table)?)
            }
        } else if advance(&mut upper, &cols)? {
            Some(complete(upper, &cols, table)?)
        } else {
            None
        };
        let empty = exhausted || upper.as_ref().is_some_and(|u| lower >= *u);
        // Reserve worst-case lookup/tree comparisons and key visits BEFORE the
        // borrowed lookup allocates its vector. Complete snapshot is <=1024 rows.
        // Access budget is separate from the accessor's complete-validation budget.
        for (pk, row) in base {
            tick(&mut cancel)?;
            let mut n = pk.len();
            for c in &d.columns {
                let v = &row.values[column(table, *c)?];
                n = add(n, add(raw(v)?.checked_mul(2).ok_or(SqlError::Limit)?, 3)?)?;
            }
            n = add(n, add(lower.len(), upper.as_ref().map_or(0, Vec::len))?)?;
            charge(&mut work, n.checked_mul(64).ok_or(SqlError::Limit)?)?;
        }
        let found = if empty {
            Vec::new()
        } else {
            sidecar
                .lookup(d.id, Some(&lower), upper.as_deref(), &mut cancel)
                .map_err(|e| match e {
                    si::Error::Cancelled => SqlError::Aborted,
                    si::Error::Limit => SqlError::Limit,
                    _ => SqlError::Type,
                })?
        };
        cap(found.len(), req.max_rows)?;
        let mut bytes = 0usize;
        for p in &projection {
            bytes = add(bytes, table.columns[*p].name.len())?;
        }
        cap(bytes, req.max_bytes)?;
        // Full aggregate output admission before any projection or name clones.
        for (_, pk) in &found {
            tick(&mut cancel)?;
            let row = base.get(*pk).ok_or(SqlError::Type)?;
            for p in &projection {
                let n = output_size(&row.values[*p]);
                bytes = add(bytes, n)?;
                cap(bytes, req.max_bytes)?;
                charge(&mut work, add(n, 1)?)?;
            }
        }
        tick(&mut cancel)?;
        let mut result = SqlResult {
            columns: projection
                .iter()
                .map(|p| table.columns[*p].name.clone())
                .collect(),
            ..SqlResult::default()
        };
        for (_, pk) in found {
            tick(&mut cancel)?;
            let row = base.get(pk).ok_or(SqlError::Type)?;
            let mut values = Vec::with_capacity(projection.len());
            for p in &projection {
                tick(&mut cancel)?;
                values.push(row.values[*p].clone());
            }
            result.rows.push(SqlRow {
                values,
                version: row.version,
            });
        }
        tick(&mut cancel)?;
        Ok(result)
    }
}

#[cfg(test)]
#[path = "secondary_index_access_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "secondary_index_qualification_tests.rs"]
mod qualification_tests;
