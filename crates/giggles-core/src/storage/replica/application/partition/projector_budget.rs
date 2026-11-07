//! Prepaid restricted-projector requests and independent logical work quotas.
//! No source-budget refund/reset and no operator/whole-apply continuity claim.
use super::{Bound, ColumnIdentity, CoreError, ProjectedRow, ProjectionRequest, Value};
use std::mem::{align_of, size_of};
const PHASES: [usize; 4] = [4992, 50233, 432743, 416157];
const FAILURE: usize = 256;
const TOTAL: usize = 904381;
fn add(a: usize, b: usize) -> Result<usize, CoreError> {
    a.checked_add(b).ok_or(CoreError::Limit)
}
fn mul(a: usize, b: usize) -> Result<usize, CoreError> {
    a.checked_mul(b).ok_or(CoreError::Limit)
}
fn charge(v: &mut usize, n: usize, cap: usize) -> Result<(), CoreError> {
    let next = add(*v, n)?;
    if next > cap {
        return Err(CoreError::Limit);
    }
    *v = next;
    Ok(())
}
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub(super) struct Work {
    pub(super) serde: usize,
    pub(super) controls: usize,
    pub(super) bytes: usize,
    pub(super) comparisons: usize,
    pub(super) moves: usize,
    pub(super) values: usize,
    pub(super) rows: usize,
}
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub(super) struct Costs {
    pub(super) allocations: usize,
    pub(super) phases: [usize; 4],
    pub(super) work: Work,
    pub(super) actual_controls: usize,
    pub(super) actual_rows: usize,
    pub(super) helpers: usize,
}
pub(in crate::storage::replica) struct ProjectorLimits {
    allocations: usize,
    phases: [usize; 4],
    work: Work,
}
impl Default for ProjectorLimits {
    fn default() -> Self {
        Self {
            allocations: TOTAL,
            phases: PHASES,
            work: work_bound().expect("fixed checked projector work constants"),
        }
    }
}
pub(in crate::storage::replica) struct ProjectorBudget {
    limits: ProjectorLimits,
    used: Costs,
}
impl ProjectorBudget {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Projector budget construction awaits the separately released ancestor consumer."
        )
    )]
    pub(in crate::storage::replica) fn new(limits: ProjectorLimits) -> Self {
        Self {
            limits,
            used: Costs::default(),
        }
    }
    // The reviewed total already contains preflight's bounded controls and
    // Value inspections. Spend that slice BEFORE request inspection, including
    // rejected requests; reserve() spends only the remaining slice.
    pub(super) fn begin(&mut self) -> Result<(), CoreError> {
        charge(&mut self.used.work.controls, 192, self.limits.work.controls)?;
        charge(&mut self.used.work.values, 16, self.limits.work.values)?;
        charge(&mut self.used.actual_controls, 1, self.used.work.controls)
    }
    pub(super) fn reserve(&mut self) -> Result<(), CoreError> {
        layouts()?;
        assert_formulas()?;
        charge(&mut self.used.allocations, TOTAL, self.limits.allocations)?;
        for (i, n) in PHASES.into_iter().enumerate() {
            charge(&mut self.used.phases[i], n, self.limits.phases[i])?;
        }
        let mut w = work_bound()?;
        w.controls = w.controls.checked_sub(192).ok_or(CoreError::Unsupported)?;
        w.values = w.values.checked_sub(16).ok_or(CoreError::Unsupported)?;
        macro_rules! c{($($f:ident),*)=>{$(charge(&mut self.used.work.$f,w.$f,self.limits.work.$f)?;)*}}
        c!(serde, controls, bytes, comparisons, moves, values, rows);
        Ok(())
    }
    pub(super) fn tick(&mut self) -> Result<(), CoreError> {
        charge(&mut self.used.actual_controls, 1, self.used.work.controls)
    }
    pub(super) fn row(&mut self) -> Result<(), CoreError> {
        charge(&mut self.used.actual_rows, 1, self.used.work.rows)
    }
    pub(super) fn helper(&mut self) -> Result<(), CoreError> {
        self.tick()?;
        self.used.helpers = add(self.used.helpers, 1)?;
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn costs(&self) -> Costs {
        self.used
    }
    #[cfg(test)]
    pub(super) fn limited(component: usize, cap: usize) -> Self {
        let mut l = ProjectorLimits::default();
        match component {
            0 => l.allocations = cap,
            1..=4 => l.phases[component - 1] = cap,
            5 => l.work.serde = cap,
            6 => l.work.controls = cap,
            7 => l.work.bytes = cap,
            8 => l.work.comparisons = cap,
            9 => l.work.moves = cap,
            10 => l.work.values = cap,
            _ => l.work.rows = cap,
        };
        Self::new(l)
    }
}
fn layouts() -> Result<(), CoreError> {
    if !cfg!(all(
        target_os = "windows",
        target_arch = "x86_64",
        target_env = "msvc"
    )) || size_of::<ColumnIdentity>() != 40
        || align_of::<ColumnIdentity>() != 8
        || size_of::<ProjectedRow>() != 56
        || align_of::<ProjectedRow>() != 8
        || size_of::<Value>() != 40
    {
        return Err(CoreError::Unsupported);
    }
    Ok(())
}
fn growth(n: usize) -> Result<usize, CoreError> {
    if n == 0 { Ok(0) } else { mul(4, n.max(8)) }
}
fn tree(n: usize, k: usize) -> Result<(usize, usize), CoreError> {
    let h = 1 + (usize::BITS - 1 - (n + 1).leading_zeros()) as usize;
    Ok((
        mul(mul(mul(n, 11)?, h)?, add(1, k)?)?,
        add(mul(mul(mul(n, 2)?, 208)?, h)?, mul(n, 208)?)?,
    ))
}
fn assert_formulas() -> Result<(), CoreError> {
    let identity = 63 + 504 + size_of::<ColumnIdentity>() * 8 + 8 * 8;
    let a0 = 208 * (2 * 8 + 8);
    let a1 = 49 + 49 + 3 * 208 * (8 + 2 * 8 + 2) + 2 * growth(4120)? + identity;
    let a2 = size_of::<ProjectedRow>() * 64
        + 64 * (4120 + 40 * 8 + 8 * 256)
        + identity
        + 2 * (40 * 8 + 8 * 256 + 4120);
    let a3 = 416157;
    if [a0, a1, a2, a3] != PHASES || PHASES.into_iter().try_fold(FAILURE, add)? != TOTAL {
        return Err(CoreError::Unsupported);
    }
    Ok(())
}
/// Exact source-bound equations from the reviewed contract. Components remain
/// separate; these logical units do not purport to count CPU instructions.
fn work_bound() -> Result<Work, CoreError> {
    let (c, p, r, b, k, n) = (8, 8, 64, 2, 4120, 567);
    let q = b + 1;
    let mut comparisons = 0;
    let mut moves = 0;
    for (count, key) in [(c, 1), (c, 63), (p, 1)] {
        let (a, z) = tree(count, key)?;
        comparisons = add(comparisons, a)?;
        moves = add(moves, z)?;
    }
    for (count, key) in [(p, 1), (c + 1, 1), (c, 63)] {
        let (a, z) = tree(count, key)?;
        comparisons = add(comparisons, mul(q, a)?)?;
        moves = add(moves, mul(q, z)?)?;
    }
    moves = add(
        moves,
        208 * q + 40 * c + 8 * p + 56 * r + 40 * c * r + 40 * c + 8 * p + 40 * b * p,
    )?;
    comparisons = add(
        comparisons,
        b * (k + 1) + (k + 1) + 4 * r * (k + 1) + (n + c + 1 + p + 3 * c) + 32 + 2 * 49,
    )?;
    let read_base = n + b * (p * 256 + k) + 64;
    let read_rows = r * (k + c * 256);
    let read_content = n + b * (p * 256 + k) + 32 + read_rows;
    let bytes = 403505
        + q * (9 * (1 + 256) + n)
        + 98
        + b * (p * 256 + k + growth(k)?)
        + 2 * read_base
        + read_rows
        + n
        + read_rows
        + (n + b * (p * 256 + k))
        + 2 * read_content
        + 2 * 416157;
    let evm = 7;
    let evt = 5 + 5 * c + p;
    let evb = 4 + p * (2 + 256) + k;
    let evd = 49 + evt + 2 * evb + evm;
    let evr = 4 + k + c * (2 + 256);
    let evbase = 49 + evd + evm;
    let evcontent = 16 + evd + evm + r * evr;
    Ok(Work {
        serde: 2 * evbase + r * evr + 4 * evm + 2 * evcontent,
        controls: 64 + 4 * c + 2 * p + 5 * b * p + q * (4 * c + 2 * p + 8) + 6 * r + 2 * c * r,
        bytes,
        comparisons,
        moves,
        values: 7 * b * p + 2 * c * r,
        rows: 3 * r,
    })
}
/// Borrowed fixed-int size prediction; invoked ONLY after primitive preflight.
/// It never formats, serializes, clones or parses an input wire schema.
pub(super) fn value_wire(v: &Value) -> Result<usize, CoreError> {
    match v {
        Value::Null => Ok(4),
        Value::Bool(_) => Ok(5),
        Value::Int(_) => Ok(12),
        Value::Text(s) => add(12, s.len()),
        Value::Bytes(v) => add(12, v.len()),
        _ => Err(CoreError::Unsupported),
    }
}
fn bound_wire(b: &Bound) -> Result<usize, CoreError> {
    match b {
        Bound::Key(key) => key
            .values
            .iter()
            .try_fold(add(20, key.encoded.len())?, |n, v| add(n, value_wire(v)?)),
        _ => Ok(4),
    }
}
pub(super) struct WirePlan {
    pub(super) base: usize,
    pub(super) digest_max: usize,
}
impl WirePlan {
    pub(super) fn new(request: &ProjectionRequest) -> Result<Self, CoreError> {
        let d = &request.descriptor;
        let t = &d.table;
        let m = 33 + 16 * usize::from(d.metadata.origin.is_some());
        let a = t.columns.iter().try_fold(0, |n, c| add(n, c.name.len()))?;
        let wt = add(
            add(32, t.name.len())?,
            add(
                mul(21, t.columns.len())?,
                add(a, mul(8, t.primary_key.len())?)?,
            )?,
        )?;
        let wd = add(
            164,
            add(
                wt,
                add(bound_wire(&d.lower)?, add(bound_wire(&d.upper)?, m)?)?,
            )?,
        )?;
        let base = add(wd, add(m, 96)?)?;
        let digest_max = add(base, mul(request.limits.rows, 24 + 4120 + 8 * (12 + 256))?)?
            .checked_sub(32)
            .ok_or(CoreError::Limit)?;
        if base > 13757 || digest_max > 416157 {
            return Err(CoreError::Limit);
        }
        Ok(Self { base, digest_max })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_layout_and_phase_equations() {
        layouts().unwrap();
        assert_formulas().unwrap();
        assert_eq!(work_bound().unwrap().serde, 1238222);
        assert_eq!(PHASES.into_iter().sum::<usize>() + FAILURE, TOTAL);
    }
}
