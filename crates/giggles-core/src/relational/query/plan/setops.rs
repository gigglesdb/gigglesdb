use super::*;
use exec::Relation;

pub(super) fn sort<T>(
    rows: &mut [T],
    execution: &Execution<'_>,
    mut compare: impl FnMut(&T, &T) -> Ordering,
) -> R<()> {
    let mut error = None;
    // No sort scratch allocation outside the retention budget.
    rows.sort_unstable_by(|a, b| {
        if error.is_none()
            && let Err(e) = execution.comparison()
        {
            error = Some(e);
        }
        compare(a, b)
    });
    error.map_or(Ok(()), Err)
}
pub(super) fn apply(
    op: SetOp,
    all: bool,
    mut left: Relation<'_>,
    mut right: Relation<'_>,
    out: &mut Relation<'_>,
    execution: &Execution<'_>,
    collations: &[Option<BoundScalar>],
) -> R<()> {
    let row_cmp = |a: &SqlRow, b: &SqlRow| {
        a.values
            .iter()
            .zip(&b.values)
            .enumerate()
            .map(|(i, (a, b))| {
                collations
                    .get(i)
                    .and_then(Option::as_ref)
                    .map_or_else(|| cmp(a, b), |s| s.compare_values(a, b))
            })
            .find(|o| *o != Ordering::Equal)
            .unwrap_or(Ordering::Equal)
    };
    if matches!(op, SetOp::Union) && all {
        for row in left.rows.into_iter().chain(right.rows) {
            if execution.cancelled() {
                return Err(SqlError::Aborted);
            }
            out.push(row)?;
        }
        return Ok(());
    }
    sort(&mut left.rows, execution, row_cmp)?;
    sort(&mut right.rows, execution, row_cmp)?;
    let mut a = left.rows.into_iter().peekable();
    let mut b = right.rows.into_iter().peekable();
    if matches!(op, SetOp::Union) {
        // Keep the current merge input on equal keys, as Merge Append does.
        // The first row in each collation-equal run is its representative.
        let mut prefer_left = true;
        while a.peek().is_some() || b.peek().is_some() {
            execution.comparison()?;
            let take_left = match (a.peek(), b.peek()) {
                (Some(a),Some(b)) => match row_cmp(a,b) { Ordering::Less=>true, Ordering::Greater=>false, Ordering::Equal=>prefer_left },
                (Some(_),None)=>true,
                _=>false,
            };
            prefer_left=take_left;
            let row = if take_left {a.next().unwrap()} else {b.next().unwrap()};
            if out.rows.last().is_none_or(|previous| row_cmp(previous,&row)!=Ordering::Equal) {out.push(SqlRow {values:row.values,version:0})?;}
        }
        return Ok(());
    }
    // Sorted runs provide SQL NULL-equal multiset semantics without a second hash table.
    while a.peek().is_some() || b.peek().is_some() {
        if execution.cancelled() {
            return Err(SqlError::Aborted);
        }
        let take_left = match (a.peek(), b.peek()) {
            (Some(a), Some(b)) => row_cmp(a, b) != Ordering::Greater,
            (Some(_), None) => true,
            _ => false,
        };
        let row = if take_left {
            a.next().unwrap()
        } else {
            b.next().unwrap()
        };
        let mut na = usize::from(take_left);
        let mut nb = usize::from(!take_left);
        while a
            .peek()
            .is_some_and(|a| row_cmp(a, &row) == Ordering::Equal)
        {
            execution.comparison()?;
            a.next();
            na += 1;
        }
        while b
            .peek()
            .is_some_and(|b| row_cmp(b, &row) == Ordering::Equal)
        {
            execution.comparison()?;
            b.next();
            nb += 1;
        }
        let n = match op {
            SetOp::Union => 1,
            SetOp::Intersect => {
                if all {
                    na.min(nb)
                } else {
                    usize::from(na > 0 && nb > 0)
                }
            }
            SetOp::Except => {
                if all {
                    na.saturating_sub(nb)
                } else {
                    usize::from(na > 0 && nb == 0)
                }
            }
        };
        for _ in 0..n {
            out.push(SqlRow {
                values: row.values.clone(),
                version: 0,
            })?;
        }
    }
    Ok(())
}
