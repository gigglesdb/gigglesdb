//! R3-616: durable denials, the error table and the internal retry (D-F),
//! one test per row of spec section 7's table plus the denial mechanics.
//!
//! Same task as `commit_tests`: requeue merchant 42's failed `shipments`
//! (`deliveries` is reserved), 25 distinct rows / 25 transitions.
use super::*;
use crate::task::records::{NS_CHANGE_HEADER, NS_RECEIPT};
use std::cell::Cell;

/// Also touches merchant 42's delivered row 5, which `allowed_write.old_row`
/// (`status = 'failed'`) refuses.
const TOO_WIDE: &str = "UPDATE shipments SET status = 'queued' WHERE merchant_id = 42";

/// A loader that serves the catalog definition with `edit` applied.
fn edited(
    edit: impl Fn(&mut TaskDef),
) -> impl FnMut(&V2Db, TaskObjectId, DefAt<'_>) -> Result<Option<Arc<TaskDef>>, CoreError> {
    move |db, task, at| {
        Ok(catalog_def(db, task, at)?.map(|def| {
            let mut def = (*def).clone();
            edit(&mut def);
            Arc::new(def)
        }))
    }
}

/// A loader that commits an unrelated grant or revoke (a Catalog write the protected
/// statement's tracked catalog range covers) during the first `conflicts`
/// statement-snapshot reads, and counts those reads.
fn conflicting<'f>(
    f: &'f Fixture,
    conflicts: usize,
    reads: &'f Cell<usize>,
) -> impl FnMut(&V2Db, TaskObjectId, DefAt<'_>) -> Result<Option<Arc<TaskDef>>, CoreError> + 'f {
    move |db, task, at| {
        if matches!(at, DefAt::Snapshot(_)) {
            reads.set(reads.get() + 1);
            if reads.get() <= conflicts {
                // Grant, then revoke, then grant: each one a real change.
                let roles = f.admin().roles();
                let privilege = [TablePrivilege::Insert];
                if reads.get() % 2 == 1 {
                    roles.grant_table("shipments", "incident_controller", &privilege)
                } else {
                    roles.revoke_table("shipments", "incident_controller", &privilege)
                }
                .unwrap();
            }
        }
        catalog_def(db, task, at)
    }
}

fn commit_text_loading(
    f: &Fixture,
    id: &str,
    text: &str,
    load: &mut LoadDef<'_>,
) -> Result<Receipt, TaskError> {
    let mut session = f.admin().sql_session();
    let request = direct(&mut session, text);
    commit_with(
        &mut session,
        Actor::Executor(&f.ctx),
        &op(id),
        request,
        load,
    )
}

fn untouched(f: &Fixture) {
    assert_eq!(
        f.statuses(),
        [
            "failed",
            "failed",
            "failed",
            "failed",
            "delivered",
            "failed"
        ]
    );
}

/// Only the outcome and the head: no rows, membership, change record or receipt.
fn only_outcome_and_head(f: &Fixture) {
    let db = f.db();
    for ns in [NS_MEMBERSHIP, NS_RECEIPT, NS_CHANGE_HEADER, NS_CHANGE_ROW] {
        assert!(scan(&db, ns, f.def.id).is_empty(), "namespace {ns:#x}");
    }
}

#[test]
fn denied_is_recorded_and_replayed() {
    let f = fixture();
    let error = commit_text(&f, "requeue-1", TOO_WIDE).unwrap_err();
    assert_eq!(error, TaskError::Denied(DenyRule::OutsideAllowedWrite));
    assert_eq!(error.sqlstate(), "42501");
    untouched(&f);
    let stored = outcome(&f, "requeue-1").unwrap();
    assert_eq!(
        stored.payload,
        OutcomePayload::Denied {
            task: f.def.id,
            rule: DenyRule::OutsideAllowedWrite.code(),
            denial_no: 1
        }
    );
    assert_eq!(
        stored.identity,
        ledger::task_identity(f.def.id, &op("requeue-1"))
    );
    assert_eq!(
        (stored.decided_lower_ms, stored.decided_upper_ms),
        (NOW, NOW)
    );
    assert_eq!(stored.retain_until_ms, EXPIRES + OUTCOME_RETENTION_MS);
    let after = head(&f).unwrap();
    assert_eq!(
        (
            after.denials,
            after.next_seq,
            after.distinct_rows,
            after.transitions
        ),
        (1, 1, 0, 0)
    );
    assert_eq!(
        (after.def_version, after.last_decided_ms),
        (f.def.def_version, NOW)
    );
    // Row 5 is now within the allowed write: a re-run would commit. The
    // resend replays the stored denial instead and charges nothing.
    sql(
        f.admin(),
        "UPDATE shipments SET status = 'failed' WHERE id = 5",
    );
    clock(&f.db(), NOW + 60_000);
    assert_eq!(commit_text(&f, "requeue-1", TOO_WIDE).unwrap_err(), error);
    assert_eq!(head(&f).unwrap(), after);
    assert_eq!(f.statuses(), ["failed"; 6]);
    assert_eq!(outcome(&f, "requeue-1").unwrap(), stored);
    // It survives a reopen.
    let mut f = f;
    f.reopen();
    assert_eq!(commit_text(&f, "requeue-1", TOO_WIDE).unwrap_err(), error);
    assert_eq!(head(&f).unwrap(), after);
}

#[test]
fn denied_id_with_other_request_conflicts() {
    let f = fixture();
    commit_text(&f, "requeue-1", TOO_WIDE).unwrap_err();
    let error = commit(&f, "requeue-1").unwrap_err();
    assert_eq!(error, TaskError::OperationConflict);
    assert_eq!(error.sqlstate(), "42T03");
    untouched(&f);
    assert_eq!(head(&f).unwrap().denials, 1);
    // The narrower request under a new ID commits as seq 1.
    let receipt = commit(&f, "requeue-2").unwrap();
    assert_eq!(
        (receipt.seq, receipt.rows_affected, receipt.distinct_charged),
        (1, 4, 4)
    );
    let after = head(&f).unwrap();
    assert_eq!(
        (after.denials, after.next_seq, after.distinct_rows),
        (1, 2, 4)
    );
}

#[test]
fn budget_exceeded_is_recorded() {
    let f = fixture();
    // 30 more failed rows for merchant 42: 34 > the 25-row cap. Caps come
    // before approval, so this is a durable 54T01, not 42T01.
    let values: Vec<String> = (10..40)
        .map(|id| format!("({id}, 42, 'failed', 0)"))
        .collect();
    sql(
        f.admin(),
        &format!("INSERT INTO shipments VALUES {}", values.join(", ")),
    );
    let error = commit(&f, "requeue-1").unwrap_err();
    assert_eq!(error, TaskError::BudgetExceeded(Budget::DistinctRows));
    assert_eq!(error.sqlstate(), "54T01");
    assert_eq!(
        outcome(&f, "requeue-1").unwrap().payload,
        OutcomePayload::BudgetExceeded {
            task: f.def.id,
            budget: Budget::DistinctRows.code(),
            denial_no: 1
        }
    );
    let after = head(&f).unwrap();
    assert_eq!(
        (after.denials, after.next_seq, after.distinct_rows),
        (1, 1, 0)
    );
    only_outcome_and_head(&f);
    let failed = sql(
        f.admin(),
        "SELECT count(*) FROM shipments WHERE status = 'failed'",
    );
    assert_eq!(failed, [[Value::Int(35)]]);
    // Replayed, even after the extra rows are gone.
    sql(f.admin(), "DELETE FROM shipments WHERE id >= 10");
    assert_eq!(commit(&f, "requeue-1").unwrap_err(), error);
    assert_eq!(head(&f).unwrap(), after);
    // A second denial under another ID counts as denial 2.
    commit_text(&f, "requeue-2", TOO_WIDE).unwrap_err();
    assert_eq!(head(&f).unwrap().denials, 2);
    assert!(matches!(
        outcome(&f, "requeue-2").unwrap().payload,
        OutcomePayload::Denied { denial_no: 2, .. }
    ));
}

#[test]
fn approval_required_leaves_id_free() {
    let f = fixture();
    let mut load = edited(|def| def.approval_threshold = 2);
    let error = commit_loading(&f, "requeue-1", &mut load).unwrap_err();
    assert_eq!(error, TaskError::ApprovalRequired);
    assert_eq!(error.sqlstate(), "42T01");
    nothing_recorded(&f, "requeue-1");
    // Under the real threshold (25) the same ID and request commit.
    let receipt = commit(&f, "requeue-1").unwrap();
    assert_eq!((receipt.seq, receipt.rows_affected), (1, 4));
}

#[test]
fn retry_leaves_id_free() {
    let f = fixture();
    let reads = Cell::new(0);
    let mut load = conflicting(&f, ATTEMPTS, &reads);
    assert_eq!(
        commit_loading(&f, "requeue-1", &mut load).unwrap_err(),
        TaskError::Retry
    );
    nothing_recorded(&f, "requeue-1");
    // The client's retry of the same ID runs fresh and commits.
    let receipt = commit(&f, "requeue-1").unwrap();
    assert_eq!((receipt.seq, receipt.rows_affected), (1, 4));
    assert_eq!(head(&f).unwrap().denials, 0);
}

#[test]
fn expired_and_revoked_leave_id_free() {
    let f = fixture();
    clock(&f.db(), EXPIRES);
    assert_eq!(commit(&f, "requeue-1").unwrap_err(), TaskError::Expired);
    nothing_recorded(&f, "requeue-1");
    clock(&f.db(), EXPIRES - 1);
    assert_eq!(commit(&f, "requeue-1").unwrap().seq, 1);
    let after = head(&f).unwrap();

    // Revoked while the hold was awaited (step 6): nothing recorded either.
    sql(
        f.admin(),
        "UPDATE shipments SET status = 'failed' WHERE id = 1",
    );
    let mut latest = 0;
    let mut load = |db: &V2Db, task: TaskObjectId, at: DefAt<'_>| {
        if matches!(at, DefAt::Latest) {
            latest += 1;
            if latest == 2 {
                f.admin().tasks().revoke(&f.issuer, "inc-436").unwrap();
            }
        }
        catalog_def(db, task, at)
    };
    let error = commit_loading(&f, "requeue-2", &mut load).unwrap_err();
    assert_eq!(error, TaskError::Revoked);
    assert_eq!(outcome(&f, "requeue-2"), None);
    assert_eq!(head(&f).unwrap(), after);
    assert_eq!(f.statuses()[0], "failed");
    assert!(kernel(&f.db(), &receipt_key(f.def.id, 2)).is_none());
}

#[test]
fn denial_writes_no_rows_membership_or_receipt() {
    let f = fixture();
    commit_text(&f, "requeue-1", TOO_WIDE).unwrap_err();
    untouched(&f);
    only_outcome_and_head(&f);
    // The denied attempt staged membership charges for rows 1-4; they were
    // rolled back with the statement, so the next commit charges all four.
    let receipt = commit(&f, "requeue-2").unwrap();
    assert_eq!(
        (
            receipt.seq,
            receipt.distinct_charged,
            receipt.transitions_charged
        ),
        (1, 4, 4)
    );
    let db = f.db();
    let members = scan(&db, NS_MEMBERSHIP, f.def.id);
    assert_eq!(members.len(), 4);
    for (_, value) in &members {
        let member = Membership::decode(value).unwrap();
        assert_eq!(
            member,
            Membership {
                first_seq: 1,
                last_seq: 1,
                transitions: 1
            }
        );
    }
    assert_eq!(scan(&db, NS_RECEIPT, f.def.id).len(), 1);
}

#[test]
fn denial_commit_ignores_statement_read_set() {
    let f = fixture();
    // A Catalog write after the statement snapshot fails a committing
    // attempt's validation (see `three_conflicts_surface_40001`); a denial
    // restarts its snapshot first, so it commits on the first attempt.
    let reads = Cell::new(0);
    let mut load = conflicting(&f, 1, &reads);
    let error = commit_text_loading(&f, "requeue-1", TOO_WIDE, &mut load).unwrap_err();
    assert_eq!(error, TaskError::Denied(DenyRule::OutsideAllowedWrite));
    assert_eq!(reads.get(), 1);
    assert!(matches!(
        outcome(&f, "requeue-1").unwrap().payload,
        OutcomePayload::Denied { denial_no: 1, .. }
    ));
    assert_eq!(head(&f).unwrap().denials, 1);
    untouched(&f);
}

#[test]
fn conflict_once_then_success_is_invisible_to_caller() {
    let f = fixture();
    let reads = Cell::new(0);
    let mut load = conflicting(&f, 1, &reads);
    let receipt = commit_loading(&f, "requeue-1", &mut load).unwrap();
    assert_eq!(reads.get(), 2);
    assert_eq!(
        (receipt.seq, receipt.rows_affected, receipt.distinct_charged),
        (1, 4, 4)
    );
    assert_eq!(
        f.statuses(),
        [
            "queued",
            "queued",
            "queued",
            "queued",
            "delivered",
            "failed"
        ]
    );
    let after = head(&f).unwrap();
    assert_eq!(
        (after.next_seq, after.distinct_rows, after.denials),
        (2, 4, 0)
    );
    assert_eq!(scan(&f.db(), NS_RECEIPT, f.def.id).len(), 1);
    assert_eq!(commit(&f, "requeue-1").unwrap(), receipt);
}

#[test]
fn three_conflicts_surface_40001() {
    let f = fixture();
    let reads = Cell::new(0);
    let mut load = conflicting(&f, usize::MAX, &reads);
    let error = commit_loading(&f, "requeue-1", &mut load).unwrap_err();
    assert_eq!(error, TaskError::Retry);
    assert_eq!(error.sqlstate(), "40001");
    assert_eq!(reads.get(), ATTEMPTS);
    nothing_recorded(&f, "requeue-1");
}

#[test]
fn deadlock_is_retried() {
    let f = fixture();
    // The engine's 40P01, injected at the first attempt's step 9.
    let reads = Cell::new(0);
    let mut load = |db: &V2Db, task: TaskObjectId, at: DefAt<'_>| {
        if matches!(at, DefAt::Snapshot(_)) {
            reads.set(reads.get() + 1);
            if reads.get() == 1 {
                return Err(CoreError::Deadlock);
            }
        }
        catalog_def(db, task, at)
    };
    let receipt = commit_loading(&f, "requeue-1", &mut load).unwrap();
    assert_eq!(reads.get(), 2);
    assert_eq!((receipt.seq, receipt.rows_affected), (1, 4));
    // Every attempt deadlocking surfaces 40001.
    let f = fixture();
    let mut load = |db: &V2Db, task: TaskObjectId, at: DefAt<'_>| {
        if matches!(at, DefAt::Snapshot(_)) {
            return Err(CoreError::Deadlock);
        }
        catalog_def(db, task, at)
    };
    assert_eq!(
        commit_loading(&f, "requeue-1", &mut load).unwrap_err(),
        TaskError::Retry
    );
    nothing_recorded(&f, "requeue-1");
}

#[test]
fn lock_timeout_surfaces_55p03_unrecorded() {
    let f = fixture();
    let db = f.db();
    // Another holder of the task's authority key (an apply or a concurrent
    // direct write).
    let other = db.begin(Isolation::Serializable);
    authority::hold(&other, f.def.id, None, &|| false).unwrap();
    let mut load = edited(|def| def.limits.lock_wait_ms = 400);
    let started = Instant::now();
    let error = commit_loading(&f, "requeue-1", &mut load).unwrap_err();
    assert_eq!(
        error,
        TaskError::Core(Box::new(CoreError::LockNotAvailable))
    );
    assert_eq!(error.sqlstate(), "55P03");
    // Not retried: one 400 ms wait, not three.
    assert!(started.elapsed() < Duration::from_millis(1150));
    nothing_recorded(&f, "requeue-1");
    drop(other);
    assert_eq!(commit(&f, "requeue-1").unwrap().seq, 1);
}
