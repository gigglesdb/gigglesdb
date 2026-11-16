//! Policy decision of a protected commit (GD-0061 design section 6, D-F step 12).
//!
//! `evaluate` is pure: it reads the definition, the table, the mutation head,
//! the collected effects and the membership charge, and returns one decision.
//! It writes nothing; the caller stages records (commit) or a durable denial.
//!
//! Order (D-F "Step 12 order"):
//! 1. structural refusals (other table, identity change): `Denied`;
//! 2. Apply only: effects differ from the stored preview: `ApprovalStale`;
//! 3. policy on every effect, in physical-key order, else `Denied`: visible
//!    rows on `old`, `allowed_write.old_row` on `old`, `allowed_write.new_row`
//!    on `new`, every changed column in `allowed_write.columns`;
//! 4. caps (distinct rows, then transitions): `BudgetExceeded`;
//! 5. approval: above the threshold, Direct is `ApprovalRequired` and Apply
//!    needs a matching approval.
//!
//! Undo (ruling R3) skips steps 3-5. A predicate that no longer binds or fails
//! to evaluate denies (fail closed).
use super::authority::Charge;
use super::definition::{PredicateText, TaskDef};
use super::effects::{Effects, RowEffect};
use super::error::{Budget, DenyRule};
use super::records::MutationHead;
use crate::relational::task_predicate::RowPredicate;
use crate::relational::{Relational, SqlRow, TableDef};

/// What kind of protected request is being decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RequestKind {
    Direct,
    Apply,
    Undo,
}

/// The caller's view of the approval for this request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ApprovalState {
    /// Direct writes and undo.
    NotApplicable,
    /// Apply with no usable approval record.
    Missing,
    /// Apply whose re-run effects equal the preview and whose approval is
    /// open and has the preview's digest.
    Matching,
    /// Apply whose re-run effects differ from the stored preview, or whose
    /// preview header no longer holds.
    Stale,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Decision {
    Commit(Charge),
    Denied(DenyRule),
    BudgetExceeded(Budget),
    ApprovalRequired,
    ApprovalStale,
}

impl Decision {
    /// Committed, Denied and BudgetExceeded consume the operation ID.
    pub(crate) fn is_recorded(&self) -> bool {
        matches!(
            self,
            Decision::Commit(_) | Decision::Denied(_) | Decision::BudgetExceeded(_)
        )
    }
}

/// Decides one protected request (D-F step 12). `state` is the catalog image
/// at the statement snapshot; it binds the definition's predicates.
#[allow(clippy::too_many_arguments)]
pub(crate) fn evaluate(
    state: &Relational,
    def: &TaskDef,
    table: &TableDef,
    head: &MutationHead,
    effects: &Effects,
    charge: &Charge,
    kind: RequestKind,
    approval: ApprovalState,
) -> Decision {
    // 1. Structural refusals.
    for effect in effects.iter() {
        if effect.table != def.table || table.id != def.table {
            return Decision::Denied(DenyRule::OtherTable);
        }
        if effect.old.is_none() || effect.new.is_none() {
            return Decision::Denied(DenyRule::PrimaryKeyChange);
        }
    }
    // 2. Stale preview.
    if kind == RequestKind::Apply && approval == ApprovalState::Stale {
        return Decision::ApprovalStale;
    }
    if kind == RequestKind::Undo {
        return Decision::Commit(*charge);
    }
    // 3. Policy on every effect.
    if let Err(rule) = policy(state, def, table, effects) {
        return Decision::Denied(rule);
    }
    // 4. Caps.
    let caps = def.mutation_caps;
    if head
        .distinct_rows
        .saturating_add(u64::from(charge.distinct_new))
        > caps.distinct_rows
    {
        return Decision::BudgetExceeded(Budget::DistinctRows);
    }
    if head
        .transitions
        .saturating_add(u64::from(charge.transitions))
        > caps.transitions
    {
        return Decision::BudgetExceeded(Budget::Transitions);
    }
    // 5. Approval.
    let above = u64::try_from(effects.len()).unwrap_or(u64::MAX) > def.approval_threshold;
    if above && !(kind == RequestKind::Apply && approval == ApprovalState::Matching) {
        return Decision::ApprovalRequired;
    }
    Decision::Commit(*charge)
}

fn policy(
    state: &Relational,
    def: &TaskDef,
    table: &TableDef,
    effects: &Effects,
) -> Result<(), DenyRule> {
    if effects.is_empty() {
        return Ok(());
    }
    let Some(write) = def.allowed_write.as_ref() else {
        return Err(DenyRule::ColumnNotWritable);
    };
    let visible = compile(state, &def.visible_rows, table)?;
    let old_row = compile(state, &write.old_row, table)?;
    let new_row = compile(state, &write.new_row, table)?;
    for effect in effects.iter() {
        let (old, new) = rows(effect)?;
        check(&visible, old, DenyRule::OutsideVisibleRows)?;
        check(&old_row, old, DenyRule::OutsideAllowedWrite)?;
        check(&new_row, new, DenyRule::OutsideAllowedWrite)?;
        let width = old.values.len().max(new.values.len());
        for at in 0..width {
            if old.values.get(at) == new.values.get(at) {
                continue;
            }
            let listed = table
                .columns
                .get(at)
                .is_some_and(|column| write.columns.contains(&column.name));
            if !listed {
                return Err(DenyRule::ColumnNotWritable);
            }
        }
    }
    Ok(())
}

fn compile<'a>(
    state: &'a Relational,
    text: &'a PredicateText,
    table: &'a TableDef,
) -> Result<RowPredicate<'a>, DenyRule> {
    RowPredicate::compile(state, text, table).map_err(|_| DenyRule::DefinitionChanged)
}

fn rows(effect: &RowEffect) -> Result<(&SqlRow, &SqlRow), DenyRule> {
    match (&effect.old, &effect.new) {
        (Some(old), Some(new)) => Ok((old, new)),
        _ => Err(DenyRule::PrimaryKeyChange),
    }
}

fn check(predicate: &RowPredicate<'_>, row: &SqlRow, rule: DenyRule) -> Result<(), DenyRule> {
    match predicate.matches_row(row) {
        Ok(true) => Ok(()),
        Ok(false) | Err(_) => Err(rule),
    }
}

#[cfg(test)]
#[path = "decide_tests.rs"]
mod tests;
