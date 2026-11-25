//! Immediate, private engine access. No planner, SQL semantics, durable handle,
//! partition authority, persistence or public CREATE capability is implied.
//! Equality/NULL and ranges use canonical key order (NULL first, binary strings).
//! Callers needing SQL three-valued predicates must not lower them blindly here.
use super::secondary_index::{self as si, Descriptor};
use super::secondary_index_adapter::{encode_tuple, tuple_len};
use super::secondary_index_keys::successor;
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
        _ if v.is_temporal() => Ok(value_bytes(v)),
        _ => Err(SqlError::Unsupported),
    }
}
fn admit(req: &Request<'_>) -> R<usize> {
    cap(req.equal.len(), MAX_COLUMNS)?;
    cap(req.projection.len(), MAX_TARGET_COLUMNS)?;
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
        // Keep the access path within its original input budget.
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
fn matches(d: &Descriptor, req: &Request<'_>) -> bool {
    d.predicate.is_none() && req.equal.len() <= d.columns.len()
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
        _ if v.is_temporal() => value_bytes(v),
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
                add(
                    table.columns.len().max(MAX_COLUMNS)
                        .checked_mul(req.projection.len().max(MAX_COLUMNS) * 64)
                        .ok_or(SqlError::Limit)?,
                    si::MAX_CANDIDATES * 64,
                )?,
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
        if cols.iter().any(|c| si::Kind::for_type(&c.data_type).is_none()) {
            return Err(SqlError::Unsupported);
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
        let bound = |endpoint: Option<Endpoint<'_>>| {
            let values = || req.equal.iter().map(|(_, v)| v)
                .chain(endpoint.map(|e| e.value));
            tuple_len(values())?;
            encode_tuple(values())
        };
        let low_endpoint = req.range.and_then(|r| r.lower);
        let high_endpoint = req.range.and_then(|r| r.upper);
        let low = bound(low_endpoint)?;
        let high = bound(high_endpoint)?;
        let lower = if low_endpoint.is_some_and(|e| !e.inclusive) {
            successor(low)
        } else {
            Some(low)
        };
        let upper = if high_endpoint.is_none_or(|e| e.inclusive) {
            successor(high)
        } else {
            Some(high)
        };
        let empty = lower.as_ref().is_none_or(|lower| {
            upper.as_ref().is_some_and(|upper| lower >= upper)
        });
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
            n = add(n, add(lower.as_ref().map_or(0, Vec::len), upper.as_ref().map_or(0, Vec::len))?)?;
            charge(&mut work, n.checked_mul(64).ok_or(SqlError::Limit)?)?;
        }
        let found = if empty {
            Vec::new()
        } else {
            sidecar
                .lookup(d.id, lower.as_deref().filter(|key| !key.is_empty()), upper.as_deref(), &mut cancel)
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

/// Optional SQL path for typed bounds on a leading temporal column. Unknown
/// literals and coercions fall back to SQL's normal per-row evaluation.
pub(super) fn sql_temporal_candidates<'a>(
    state: &'a Relational,
    table: &TableDef,
    predicates: &[(usize, &str, Value)],
) -> R<Option<scan::SourceRows<'a>>> {
    let ds = state.secondary_descriptors_engine();
    let Some((descriptor, offset)) = ds.iter().filter(|d| d.table == table.id && d.predicate.is_none())
        .filter_map(|d| {
            let offset = table.columns.iter().position(|c| Some(&c.id) == d.columns.first())?;
            let c = &table.columns[offset];
            (c.data_type.is_temporal() && state.ext.columns.get(&c.id).is_none_or(|c| c.type_id.is_none())
                && predicates.iter().any(|(i, _, value)| *i == offset && value.is_temporal()
                    && check(value, c).is_ok())).then_some((d, offset))
        })
        .min_by_key(|(d, _)| d.id)
    else {
        return Ok(None);
    };
    let mut lower: Option<Vec<u8>> = None;
    let mut upper: Option<Vec<u8>> = None;
    for (i, op, value) in predicates {
        if *i != offset || !value.is_temporal() || check(value, &table.columns[offset]).is_err() {
            continue;
        }
        let key = secondary_index_keys::component(value)?;
        let (lo, hi) = match *op {
            "=" => (Some(key.clone()), successor(key)),
            ">" => (successor(key), None),
            ">=" => (Some(key), None),
            "<" => (None, Some(key)),
            "<=" => (None, successor(key)),
            _ => continue,
        };
        if let Some(lo) = lo {
            lower = Some(lower.take().map_or(lo.clone(), |old| old.max(lo)));
        }
        if let Some(hi) = hi {
            upper = Some(upper.take().map_or(hi.clone(), |old| old.min(hi)));
        }
    }
    if lower.as_ref().zip(upper.as_ref()).is_some_and(|(lo, hi)| lo >= hi) {
        return Ok(Some(Box::new(std::iter::empty())));
    }
    let mut cancel = || crate::cancellation::check().is_err();
    let (sidecar, base) = match state.secondary_access_engine(table, state.epoch, &mut cancel) {
        Ok(ready) => ready,
        Err(SqlError::Limit | SqlError::Unsupported) => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut hits = sidecar.lookup(descriptor.id, lower.as_deref(), upper.as_deref(), &mut cancel)
        .map_err(|e| match e {
            si::Error::Cancelled => SqlError::Aborted,
            si::Error::Limit => SqlError::Limit,
            _ => SqlError::Type,
        })?;
    hits.sort_unstable_by_key(|(_, pk)| *pk);
    Ok(Some(Box::new(hits.into_iter().map(|(_, key)| {
        let row = base.get(key).ok_or(SqlError::Type)?;
        Ok(scan::SourceRow {
            key: std::borrow::Cow::Borrowed(key),
            row: std::borrow::Cow::Borrowed(row),
        })
    }))))
}

#[cfg(test)]
#[path = "secondary_index_access_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "secondary_index_qualification_tests.rs"]
mod qualification_tests;
