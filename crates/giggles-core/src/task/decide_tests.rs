//! R3-614: the step-12 policy decision.
use super::*;
use crate::relational::{task_predicate::canonical, Prepared, Value};
use crate::task::definition::{
    AllowedWrite, DisclosureCaps, LoadLimits, MutationCaps, TaskName, WallClockMs,
};
use crate::task::records::TaskObjectId;
use std::collections::{BTreeMap, BTreeSet};

struct Fixture {
    state: Relational,
    table: TableDef,
    def: TaskDef,
}

fn fixture() -> Fixture {
    let mut state = Relational::default();
    for sql in [
        "CREATE TABLE shipments(id INT PRIMARY KEY, carrier_id INT, status TEXT, note TEXT)",
        "CREATE TABLE carriers(id INT PRIMARY KEY, name TEXT)",
    ] {
        let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
        state.execute(&prepared, &[], None).unwrap();
    }
    let table = state.tables["shipments"].clone();
    let text = |sql: &str| PredicateText(canonical(&state, &table, sql).unwrap());
    let def = TaskDef {
        id: TaskObjectId(41),
        name: TaskName::new("inc-436").unwrap(),
        def_version: 1,
        issuer: "alice".into(),
        executor: "agent".into(),
        approver: Some("bob".into()),
        table: table.id,
        visible_rows: text("carrier_id = 7"),
        columns: BTreeMap::new(),
        untrusted_allowed: BTreeSet::new(),
        allowed_write: Some(AllowedWrite {
            columns: BTreeSet::from(["status".to_owned()]),
            old_row: text("status = 'pending'"),
            new_row: text("status <> 'lost'"),
        }),
        mutation_caps: MutationCaps {
            distinct_rows: 10,
            transitions: 20,
        },
        disclosure_caps: DisclosureCaps::default(),
        limits: LoadLimits::default(),
        approval_threshold: 5,
        expires_at: WallClockMs(i64::MAX),
        revoked: false,
        grants: BTreeMap::new(),
    };
    Fixture { state, table, def }
}

fn row(id: i64, carrier: Option<i64>, status: &str, note: &str, version: u64) -> SqlRow {
    SqlRow {
        values: vec![
            Value::Int(id),
            carrier.map_or(Value::Null, Value::Int),
            Value::Text(status.into()),
            Value::Text(note.into()),
        ],
        version,
    }
}

fn key(table: u64, id: i64) -> Vec<u8> {
    let mut k = table.to_be_bytes().to_vec();
    k.extend(id.to_be_bytes());
    k
}

/// An ordinary allowed change of row `id`: pending -> held.
fn held(e: &mut Effects, table: u64, id: i64) {
    let old = row(id, Some(7), "pending", "n", 10 + id as u64);
    let new = row(id, Some(7), "held", "n", 100 + id as u64);
    e.record(table, &key(table, id), Some(&old), Some(&new))
        .unwrap();
}

fn effects(f: &Fixture, n: i64) -> Effects {
    let mut e = Effects::default();
    for id in 1..=n {
        held(&mut e, f.table.id, id);
    }
    e
}

fn charge_of(e: &Effects) -> Charge {
    let n = e.len() as u32;
    Charge {
        distinct_new: n,
        transitions: n,
        noops: 0,
    }
}

fn head() -> MutationHead {
    MutationHead {
        next_seq: 1,
        next_preview: 1,
        ..MutationHead::default()
    }
}

fn decide(f: &Fixture, e: &Effects, kind: RequestKind, approval: ApprovalState) -> Decision {
    evaluate(
        &f.state,
        &f.def,
        &f.table,
        &head(),
        e,
        &charge_of(e),
        kind,
        approval,
    )
}

fn direct(f: &Fixture, e: &Effects) -> Decision {
    decide(f, e, RequestKind::Direct, ApprovalState::NotApplicable)
}

fn one(f: &Fixture, old: SqlRow, new: SqlRow) -> Effects {
    let mut e = Effects::default();
    e.record(f.table.id, &key(f.table.id, 1), Some(&old), Some(&new))
        .unwrap();
    e
}

#[test]
fn row_outside_visible_predicate_denies_whole_statement() {
    let f = fixture();
    let mut e = effects(&f, 3);
    let old = row(9, Some(8), "pending", "n", 19);
    let new = row(9, Some(8), "held", "n", 109);
    e.record(f.table.id, &key(f.table.id, 9), Some(&old), Some(&new))
        .unwrap();
    assert_eq!(
        direct(&f, &e),
        Decision::Denied(DenyRule::OutsideVisibleRows)
    );
    // NULL is false: a row with no carrier is outside the visible rows.
    let e = one(
        &f,
        row(1, None, "pending", "n", 1),
        row(1, None, "held", "n", 2),
    );
    assert_eq!(
        direct(&f, &e),
        Decision::Denied(DenyRule::OutsideVisibleRows)
    );
    // The three visible rows alone commit.
    assert_eq!(
        direct(&f, &effects(&f, 3)),
        Decision::Commit(charge_of(&effects(&f, 3)))
    );
}

#[test]
fn old_row_predicate_enforced() {
    let f = fixture();
    let e = one(
        &f,
        row(1, Some(7), "shipped", "n", 1),
        row(1, Some(7), "held", "n", 2),
    );
    assert_eq!(
        direct(&f, &e),
        Decision::Denied(DenyRule::OutsideAllowedWrite)
    );
}

#[test]
fn new_row_predicate_enforced() {
    let f = fixture();
    let e = one(
        &f,
        row(1, Some(7), "pending", "n", 1),
        row(1, Some(7), "lost", "n", 2),
    );
    assert_eq!(
        direct(&f, &e),
        Decision::Denied(DenyRule::OutsideAllowedWrite)
    );
}

#[test]
fn unlisted_column_change_denied() {
    let f = fixture();
    let e = one(
        &f,
        row(1, Some(7), "pending", "a", 1),
        row(1, Some(7), "held", "b", 2),
    );
    assert_eq!(
        direct(&f, &e),
        Decision::Denied(DenyRule::ColumnNotWritable)
    );
    // A read-only task may not write at all.
    let mut ro = fixture();
    ro.def.allowed_write = None;
    assert_eq!(
        direct(&ro, &effects(&ro, 1)),
        Decision::Denied(DenyRule::ColumnNotWritable)
    );
    // A no-op on a listed-only row is allowed (no column changed).
    let e = one(
        &f,
        row(1, Some(7), "pending", "a", 1),
        row(1, Some(7), "pending", "a", 2),
    );
    assert!(matches!(direct(&f, &e), Decision::Commit(_)));
}

#[test]
fn other_table_effect_denied() {
    let f = fixture();
    let other = f.state.tables["carriers"].id;
    let mut e = effects(&f, 1);
    let r = SqlRow {
        values: vec![Value::Int(1), Value::Text("x".into())],
        version: 1,
    };
    let s = SqlRow {
        values: vec![Value::Int(1), Value::Text("y".into())],
        version: 2,
    };
    e.record(other, &key(other, 1), Some(&r), Some(&s)).unwrap();
    assert_eq!(direct(&f, &e), Decision::Denied(DenyRule::OtherTable));
}

#[test]
fn identity_change_denied() {
    let f = fixture();
    let r = row(1, Some(7), "pending", "n", 1);
    for (old, new) in [(Some(&r), None), (None, Some(&r))] {
        let mut e = effects(&f, 2);
        e.record(f.table.id, &key(f.table.id, 5), old, new).unwrap();
        assert_eq!(direct(&f, &e), Decision::Denied(DenyRule::PrimaryKeyChange));
        // Structural refusals come before even a stale preview.
        let d = decide(&f, &e, RequestKind::Apply, ApprovalState::Stale);
        assert_eq!(d, Decision::Denied(DenyRule::PrimaryKeyChange));
    }
}

#[test]
fn distinct_cap_exact_boundary() {
    let f = fixture();
    let e = effects(&f, 2);
    let c = charge_of(&e);
    let at = MutationHead {
        distinct_rows: 8,
        ..head()
    };
    let d = evaluate(
        &f.state,
        &f.def,
        &f.table,
        &at,
        &e,
        &c,
        RequestKind::Direct,
        ApprovalState::NotApplicable,
    );
    assert_eq!(d, Decision::Commit(c));
    let over = MutationHead {
        distinct_rows: 9,
        ..head()
    };
    let d = evaluate(
        &f.state,
        &f.def,
        &f.table,
        &over,
        &e,
        &c,
        RequestKind::Direct,
        ApprovalState::NotApplicable,
    );
    assert_eq!(d, Decision::BudgetExceeded(Budget::DistinctRows));
    // Rows changed before charge no distinct row.
    let again = Charge {
        distinct_new: 0,
        transitions: 2,
        noops: 0,
    };
    let d = evaluate(
        &f.state,
        &f.def,
        &f.table,
        &over,
        &e,
        &again,
        RequestKind::Direct,
        ApprovalState::NotApplicable,
    );
    assert_eq!(d, Decision::Commit(again));
}

#[test]
fn transition_cap_exact_boundary() {
    let f = fixture();
    let e = effects(&f, 2);
    let c = Charge {
        distinct_new: 0,
        transitions: 2,
        noops: 0,
    };
    let at = MutationHead {
        transitions: 18,
        ..head()
    };
    let d = evaluate(
        &f.state,
        &f.def,
        &f.table,
        &at,
        &e,
        &c,
        RequestKind::Direct,
        ApprovalState::NotApplicable,
    );
    assert_eq!(d, Decision::Commit(c));
    let over = MutationHead {
        transitions: 19,
        ..head()
    };
    let d = evaluate(
        &f.state,
        &f.def,
        &f.table,
        &over,
        &e,
        &c,
        RequestKind::Direct,
        ApprovalState::NotApplicable,
    );
    assert_eq!(d, Decision::BudgetExceeded(Budget::Transitions));
}

#[test]
fn budget_checked_before_approval() {
    let mut f = fixture();
    f.def.mutation_caps.distinct_rows = 3;
    let e = effects(&f, 6); // above the threshold (5) and the cap (3)
    assert_eq!(
        direct(&f, &e),
        Decision::BudgetExceeded(Budget::DistinctRows)
    );
    for approval in [ApprovalState::Missing, ApprovalState::Matching] {
        let d = decide(&f, &e, RequestKind::Apply, approval);
        assert_eq!(d, Decision::BudgetExceeded(Budget::DistinctRows));
        assert!(d.is_recorded());
    }
    // Policy comes before caps, too.
    let e = one(
        &f,
        row(1, Some(8), "pending", "n", 1),
        row(1, Some(8), "held", "n", 2),
    );
    assert_eq!(
        direct(&f, &e),
        Decision::Denied(DenyRule::OutsideVisibleRows)
    );
}

#[test]
fn direct_above_threshold_needs_approval() {
    let f = fixture();
    assert!(matches!(direct(&f, &effects(&f, 5)), Decision::Commit(_)));
    let e = effects(&f, 6);
    let d = direct(&f, &e);
    assert_eq!(d, Decision::ApprovalRequired);
    assert!(!d.is_recorded());
    // Apply above the threshold needs a matching approval.
    assert_eq!(
        decide(&f, &e, RequestKind::Apply, ApprovalState::Missing),
        Decision::ApprovalRequired
    );
    assert_eq!(
        decide(&f, &e, RequestKind::Apply, ApprovalState::Matching),
        Decision::Commit(charge_of(&e))
    );
    // A stale preview is stale at any size, before policy.
    let small = effects(&f, 1);
    assert_eq!(
        decide(&f, &small, RequestKind::Apply, ApprovalState::Stale),
        Decision::ApprovalStale
    );
    let outside = one(
        &f,
        row(1, Some(8), "pending", "n", 1),
        row(1, Some(8), "held", "n", 2),
    );
    assert_eq!(
        decide(&f, &outside, RequestKind::Apply, ApprovalState::Stale),
        Decision::ApprovalStale
    );
    // Apply at or under the threshold needs no approval; approval never widens.
    assert!(matches!(
        decide(&f, &small, RequestKind::Apply, ApprovalState::Missing),
        Decision::Commit(_)
    ));
    assert_eq!(
        decide(&f, &outside, RequestKind::Apply, ApprovalState::Matching),
        Decision::Denied(DenyRule::OutsideVisibleRows)
    );
}

#[test]
fn threshold_zero_proposes_every_write() {
    let mut f = fixture();
    f.def.approval_threshold = 0;
    assert_eq!(direct(&f, &effects(&f, 1)), Decision::ApprovalRequired);
    // A no-op row is still a row of the Effects.
    let e = one(
        &f,
        row(1, Some(7), "pending", "n", 1),
        row(1, Some(7), "pending", "n", 2),
    );
    assert_eq!(direct(&f, &e), Decision::ApprovalRequired);
}

#[test]
fn zero_rows_commits() {
    let mut f = fixture();
    f.def.approval_threshold = 0;
    f.def.allowed_write = None;
    let e = Effects::default();
    let d = direct(&f, &e);
    assert_eq!(d, Decision::Commit(Charge::default()));
    assert!(d.is_recorded());
}

#[test]
fn undo_skips_policy_caps_and_threshold() {
    let mut f = fixture();
    f.def.approval_threshold = 0;
    f.def.mutation_caps = MutationCaps {
        distinct_rows: 0,
        transitions: 0,
    };
    let e = one(
        &f,
        row(1, Some(8), "held", "x", 1),
        row(1, Some(8), "pending", "y", 2),
    );
    let d = decide(&f, &e, RequestKind::Undo, ApprovalState::NotApplicable);
    assert_eq!(d, Decision::Commit(charge_of(&e)));
    let mut e = Effects::default();
    e.record(
        f.table.id,
        &key(f.table.id, 1),
        None,
        Some(&row(1, Some(7), "p", "n", 1)),
    )
    .unwrap();
    let d = decide(&f, &e, RequestKind::Undo, ApprovalState::NotApplicable);
    assert_eq!(d, Decision::Denied(DenyRule::PrimaryKeyChange));
}

#[test]
fn row_predicate_null_is_false() {
    let f = fixture();
    let p = RowPredicate::bind(&f.state, &f.table, &f.def.visible_rows.0).unwrap();
    let values = |carrier: Value| vec![Value::Int(1), carrier, Value::Null, Value::Null];
    assert!(p.accepts(&values(Value::Int(7))).unwrap());
    assert!(!p.accepts(&values(Value::Int(8))).unwrap());
    assert!(!p.accepts(&values(Value::Null)).unwrap());
    let bad = PredicateText("no_such_column = 1".into());
    assert!(RowPredicate::bind(&f.state, &f.table, &bad.0).is_err());
}
