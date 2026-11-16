//! Per-row history planner for undo (GD-0061 R3-801, design D8).
//!
//! [`classify`] is the pure part of `plan`: it walks a task's change-record
//! rows and sorts every touched row into settled, superseded, conflict or a
//! restore candidate. The dry run through the Phase 6 seam (R3-802) turns
//! candidates into restores or `Constraint` conflicts.
use super::definition::TableId;
use super::preview::PreviewId;
use super::records::{ChangeRow, ROW_BEFORE_BLANKED};
use crate::relational::Value;
use std::collections::BTreeMap;

/// Undo scope: the whole task, or one operation (by its client operation ID bytes).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UndoScope {
    Task,
    Operation(Vec<u8>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum UndoMode {
    Complete = 1,
    Partial = 2,
}

/// Stored as the class-01 row's `aux`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ConflictReason {
    ChangedSince = 1,
    RowMissing = 2,
    Constraint = 3,
    BeforeValuesExpired = 4,
    SchemaChanged = 5,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UndoSummary {
    pub id: PreviewId,
    pub digest: [u8; 32],
    pub restorable: u32,
    pub conflicts: u32,
    pub superseded: u32,
    pub settled: u32,
    /// Wall clock, milliseconds.
    pub expires_at: u64,
}

/// Change-record kinds (`24` header `kind`, design-0): 01 change (a direct
/// write or an applied preview), 02 undo.
pub(crate) const KIND_CHANGE: u8 = 1;
pub(crate) const KIND_UNDO: u8 = 2;

/// One `25` row with the `seq` from its key and the `kind` from its `24` header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HistoryWrite {
    pub(crate) seq: u64,
    pub(crate) kind: u8,
    pub(crate) row: ChangeRow,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Candidate {
    pub(crate) table: TableId,
    pub(crate) pk: Vec<u8>,
    /// The current version; the restore runs with this `expected_version`.
    pub(crate) expect: u64,
    /// The before-values to restore.
    pub(crate) restore: Vec<Value>,
    /// Column offsets where `restore` differs from the current values.
    pub(crate) sets: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Conflict {
    pub(crate) table: TableId,
    pub(crate) pk: Vec<u8>,
    /// The current version, or 0 when the row is missing.
    pub(crate) version_read: u64,
    pub(crate) reason: ConflictReason,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Superseded {
    pub(crate) table: TableId,
    pub(crate) pk: Vec<u8>,
    /// `version_written` of the superseded write.
    pub(crate) version_read: u64,
    pub(crate) seq: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Plan {
    /// Ascending `(table, pk)`.
    pub(crate) candidates: Vec<Candidate>,
    pub(crate) conflicts: Vec<Conflict>,
    pub(crate) superseded: Vec<Superseded>,
    pub(crate) settled: u32,
}

/// Reads a row's current `(version, values)` by `(table, pk)`.
pub(crate) type CurrentRow<'a> = dyn Fn(TableId, &[u8]) -> Option<(u64, Vec<Value>)> + 'a;
/// Decodes a write's before-values against the current table definition.
pub(crate) type DecodeBefore<'a> = dyn Fn(&ChangeRow) -> Option<Vec<Value>> + 'a;

/// The D8 `plan` up to and including the candidates. `writes` are the task's
/// `25` rows in any order (they are ordered by `seq` here, stably, so rows of
/// one record keep their `row_no` order). `scope_seq` is `Some(seq of the
/// operation's receipt)` for operation scope. `current` returns the row's
/// current `(version, values)`; `decode` returns the before-values resized to
/// the current table definition, or `None` when they no longer fit it.
/// Versions are compared for equality only.
pub(crate) fn classify(
    writes: &[HistoryWrite],
    scope_seq: Option<u64>,
    current: &CurrentRow<'_>,
    decode: &DecodeBefore<'_>,
) -> Plan {
    let mut ordered: Vec<&HistoryWrite> = writes
        .iter()
        .filter(|w| scope_seq.is_none_or(|s| w.seq == s))
        .collect();
    ordered.sort_by_key(|w| w.seq);
    let mut by_row: BTreeMap<(TableId, Vec<u8>), Vec<&HistoryWrite>> = BTreeMap::new();
    for w in ordered {
        by_row.entry((w.row.table, w.row.pk.clone())).or_default().push(w);
    }
    let mut plan = Plan::default();
    for ((table, pk), mut history) in by_row {
        if scope_seq.is_none()
            && let Some(j) = history.iter().rposition(|h| h.kind == KIND_UNDO)
        {
            plan.settled += (j + 1) as u32;
            history.drain(..=j);
            if history.is_empty() {
                continue;
            }
        }
        let n = history.len();
        let mut i = n - 1;
        while i > 0 && history[i].row.version_before == history[i - 1].row.version_written {
            i -= 1;
        }
        for h in &history[..i] {
            plan.superseded.push(Superseded {
                table,
                pk: pk.clone(),
                version_read: h.row.version_written,
                seq: h.seq,
            });
        }
        let first = &history[i].row;
        let last = &history[n - 1].row;
        let conflict = |reason, version_read| Conflict { table, pk: pk.clone(), version_read, reason };
        let Some((version, values)) = current(table, &pk) else {
            plan.conflicts.push(conflict(ConflictReason::RowMissing, 0));
            continue;
        };
        if version != last.version_written {
            plan.conflicts.push(conflict(ConflictReason::ChangedSince, version));
            continue;
        }
        if first.flags & ROW_BEFORE_BLANKED != 0 || first.before.is_empty() {
            plan.conflicts.push(conflict(ConflictReason::BeforeValuesExpired, version));
            continue;
        }
        let Some(restore) = decode(first) else {
            plan.conflicts.push(conflict(ConflictReason::SchemaChanged, version));
            continue;
        };
        if restore.len() != values.len() {
            plan.conflicts.push(conflict(ConflictReason::SchemaChanged, version));
            continue;
        }
        if restore == values {
            plan.settled += 1;
            continue;
        }
        let sets = (0..restore.len()).filter(|&c| restore[c] != values[c]).collect();
        plan.candidates.push(Candidate { table, pk, expect: version, restore, sets });
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: u64 = 9;

    /// A write of row `pk` by record `seq` from version `vb` to `vw`, whose
    /// before-value is the single integer `before` (`None` = blanked).
    fn w(seq: u64, kind: u8, pk: u8, vb: u64, vw: u64, before: Option<i64>) -> HistoryWrite {
        let (flags, bytes) = match before {
            Some(v) => (0, v.to_be_bytes().to_vec()),
            None => (ROW_BEFORE_BLANKED, Vec::new()),
        };
        HistoryWrite {
            seq,
            kind,
            row: ChangeRow {
                flags,
                table: T,
                pk: vec![pk],
                version_before: vb,
                version_written: vw,
                before: bytes,
                ..ChangeRow::default()
            },
        }
    }
    fn decode_int(row: &ChangeRow) -> Option<Vec<Value>> {
        let bytes: [u8; 8] = row.before.as_slice().try_into().ok()?;
        Some(vec![Value::Int(i64::from_be_bytes(bytes))])
    }
    fn decode_never(_: &ChangeRow) -> Option<Vec<Value>> {
        None
    }
    fn run(writes: &[HistoryWrite], scope: Option<u64>, rows: &[(u8, u64, i64)]) -> Plan {
        let rows: BTreeMap<Vec<u8>, (u64, Vec<Value>)> =
            rows.iter().map(|&(pk, v, x)| (vec![pk], (v, vec![Value::Int(x)]))).collect();
        classify(writes, scope, &|t, pk| if t == T { rows.get(pk).cloned() } else { None }, &decode_int)
    }

    #[test]
    fn single_write_restorable() {
        let plan = run(&[w(1, KIND_CHANGE, 1, 5, 6, Some(100))], None, &[(1, 6, 200)]);
        assert_eq!(
            plan.candidates,
            vec![Candidate { table: T, pk: vec![1], expect: 6, restore: vec![Value::Int(100)], sets: vec![0] }]
        );
        assert!(plan.conflicts.is_empty() && plan.superseded.is_empty());
        assert_eq!(plan.settled, 0);
    }

    #[test]
    fn linked_writes_restore_to_before_first() {
        let writes = [
            w(1, KIND_CHANGE, 1, 5, 6, Some(100)),
            w(2, KIND_CHANGE, 1, 6, 7, Some(200)),
            w(3, KIND_CHANGE, 1, 7, 8, Some(300)),
        ];
        let plan = run(&writes, None, &[(1, 8, 400)]);
        assert_eq!(plan.candidates.len(), 1);
        assert_eq!(plan.candidates[0].restore, vec![Value::Int(100)]);
        assert_eq!(plan.candidates[0].expect, 8);
        assert!(plan.superseded.is_empty());
    }

    #[test]
    fn application_write_between_task_writes_keeps_application_change() {
        // A -> B by the task (v5 -> v6), B -> C by the application (v6 -> v7),
        // C -> D by the task (v7 -> v8). Restore C; the A -> B write is superseded.
        let (a, c, d) = (1, 3, 4);
        let writes = [w(1, KIND_CHANGE, 1, 5, 6, Some(a)), w(2, KIND_CHANGE, 1, 7, 8, Some(c))];
        let plan = run(&writes, None, &[(1, 8, d)]);
        assert_eq!(plan.candidates.len(), 1);
        assert_eq!(plan.candidates[0].restore, vec![Value::Int(c)]);
        assert_eq!(
            plan.superseded,
            vec![Superseded { table: T, pk: vec![1], version_read: 6, seq: 1 }]
        );
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn changed_since_is_conflict() {
        let plan = run(&[w(1, KIND_CHANGE, 1, 5, 6, Some(100))], None, &[(1, 7, 200)]);
        assert_eq!(
            plan.conflicts,
            vec![Conflict { table: T, pk: vec![1], version_read: 7, reason: ConflictReason::ChangedSince }]
        );
        assert!(plan.candidates.is_empty());
    }

    #[test]
    fn missing_row_is_conflict() {
        let plan = run(&[w(1, KIND_CHANGE, 1, 5, 6, Some(100))], None, &[]);
        assert_eq!(
            plan.conflicts,
            vec![Conflict { table: T, pk: vec![1], version_read: 0, reason: ConflictReason::RowMissing }]
        );
    }

    #[test]
    fn expired_before_values_is_conflict() {
        let writes = [w(1, KIND_CHANGE, 1, 5, 6, None), w(2, KIND_CHANGE, 1, 6, 7, Some(150))];
        let plan = run(&writes, None, &[(1, 7, 200)]);
        assert_eq!(plan.conflicts.len(), 1);
        assert_eq!(plan.conflicts[0].reason, ConflictReason::BeforeValuesExpired);
        assert!(plan.candidates.is_empty());
        // A schema that no longer fits is SchemaChanged.
        let writes = [w(1, KIND_CHANGE, 1, 5, 6, Some(100))];
        let plan = classify(&writes, None, &|_, _| Some((6, vec![Value::Int(1)])), &decode_never);
        assert_eq!(plan.conflicts[0].reason, ConflictReason::SchemaChanged);
    }

    #[test]
    fn undo_record_settles_earlier_writes_in_task_scope() {
        let writes = [
            w(1, KIND_CHANGE, 1, 5, 6, Some(100)),
            w(2, KIND_UNDO, 1, 6, 7, Some(200)),
            w(1, KIND_CHANGE, 2, 1, 2, Some(10)),
            w(2, KIND_UNDO, 2, 2, 3, Some(20)),
            w(3, KIND_CHANGE, 2, 3, 4, Some(10)),
        ];
        let plan = run(&writes, None, &[(1, 7, 100), (2, 4, 30)]);
        // Row 1: both writes settled by the undo; nothing left.
        // Row 2: two settled, then the seq-3 write is restorable.
        assert_eq!(plan.settled, 4);
        assert_eq!(plan.candidates.len(), 1);
        assert_eq!(plan.candidates[0].pk, vec![2]);
        assert_eq!(plan.candidates[0].restore, vec![Value::Int(10)]);
        assert!(plan.conflicts.is_empty() && plan.superseded.is_empty());
    }

    #[test]
    fn operation_scope_ignores_other_records() {
        let writes = [
            w(1, KIND_CHANGE, 1, 5, 6, Some(100)),
            w(2, KIND_UNDO, 1, 6, 7, Some(200)),
            w(3, KIND_CHANGE, 2, 1, 2, Some(10)),
        ];
        // Undo of the undo: only seq 2 is seen, and the undo reset does not apply.
        let plan = run(&writes, Some(2), &[(1, 7, 100), (2, 2, 11)]);
        assert_eq!(plan.settled, 0);
        assert_eq!(plan.candidates.len(), 1);
        assert_eq!(plan.candidates[0].pk, vec![1]);
        assert_eq!(plan.candidates[0].restore, vec![Value::Int(200)]);
        assert!(plan.conflicts.is_empty() && plan.superseded.is_empty());
    }

    #[test]
    fn write_back_to_original_values_is_settled() {
        let plan = run(&[w(1, KIND_CHANGE, 1, 5, 6, Some(100))], None, &[(1, 6, 100)]);
        assert_eq!(plan.settled, 1);
        assert!(plan.candidates.is_empty() && plan.conflicts.is_empty());
    }
}
