use super::*;
use exec::Relation;

fn row_cmp(a: &SqlRow, b: &SqlRow) -> Ordering {
    a.values
        .iter()
        .zip(&b.values)
        .map(|(a, b)| cmp(a, b))
        .find(|c| *c != Ordering::Equal)
        .unwrap_or(Ordering::Equal)
}
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
) -> R<()> {
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
