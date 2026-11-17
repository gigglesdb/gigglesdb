//! R3-617: outcome lookup (D-G). `outcome` is the executor's
//! `SHOW OPERATION '<id>'`; `inspect_outcome` the inspector's.
//!
//! Same task as `commit_tests`: requeue merchant 42's failed `shipments`
//! (`deliveries` is reserved), 25 distinct rows / 25 transitions.
use super::commit_tests::{
    clock, commit, commit_text, fixture, head, kernel, op, Fixture, EXPIRES, NOW,
};
use super::*;
use crate::storage::engine::kv::Direction;
use crate::task::definition::{
    AllowedWrite, DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText,
};
use std::ops::Bound;

/// Also touches merchant 42's delivered row 5, which `allowed_write.old_row`
/// (`status = 'failed'`) refuses.
const TOO_WIDE: &str = "UPDATE shipments SET status = 'queued' WHERE merchant_id = 42";

fn lookup(f: &Fixture, id: &str) -> Result<Option<Outcome>, TaskError> {
    outcome(&f.admin().sql_session(), &f.ctx, &op(id))
}

fn inspect(f: &Fixture, task: TaskObjectId, id: &str) -> Result<Option<Outcome>, TaskError> {
    inspect_outcome(&f.admin().sql_session(), task, &op(id))
}

/// Every committed Kernel entry, in key order.
fn kernel_dump(db: &V2Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut txn = db.begin(Isolation::ReadCommitted);
    txn.scan(
        Keyspace::Kernel,
        Bound::Unbounded,
        Bound::Unbounded,
        Direction::Forward,
        None,
    )
    .unwrap()
    .map(|entry| entry.unwrap())
    .collect()
}

#[test]
fn unknown_id_is_none() {
    let f = fixture();
    assert_eq!(lookup(&f, "requeue-1"), Ok(None));
    assert_eq!(inspect(&f, f.def.id, "requeue-1"), Ok(None));
    // A refusal that is not recorded leaves the ID unknown too.
    f.admin()
        .roles()
        .revoke_table("shipments", "repair_agent", &[TablePrivilege::Update])
        .unwrap();
    assert_eq!(
        commit(&f, "requeue-1").unwrap_err(),
        TaskError::Denied(DenyRule::RolePrivilege)
    );
    assert_eq!(lookup(&f, "requeue-1"), Ok(None));
    // Another ID than the committed one is still unknown.
    f.admin()
        .roles()
        .grant_table("shipments", "repair_agent", &[TablePrivilege::Update])
        .unwrap();
    commit(&f, "requeue-1").unwrap();
    assert_eq!(lookup(&f, "requeue-2"), Ok(None));
}

#[test]
fn committed_returns_minimal_receipt() {
    let f = fixture();
    let receipt = commit(&f, "requeue-1").unwrap();
    let Some(Outcome::Committed(found)) = lookup(&f, "requeue-1").unwrap() else {
        panic!("not committed");
    };
    assert!(found.is_minimal());
    assert_eq!(found, receipt);
    assert_eq!(found.operation_id, b"requeue-1");
    assert_eq!((found.seq, found.rows_affected, found.distinct_charged), (1, 4, 4));
    assert_eq!(found.decided_ms, NOW);
    // The executor's view hides roles and the change record.
    assert_eq!(found.issuer(), None);
    assert_eq!(found.executor(), None);
    assert!(found.change().is_none());
}

#[test]
fn denied_returns_denial() {
    let f = fixture();
    let error = commit_text(&f, "requeue-1", TOO_WIDE).unwrap_err();
    assert_eq!(error, TaskError::Denied(DenyRule::OutsideAllowedWrite));
    assert_eq!(
        lookup(&f, "requeue-1"),
        Ok(Some(Outcome::Denied(DenyRule::OutsideAllowedWrite)))
    );
    assert_eq!(
        inspect(&f, f.def.id, "requeue-1"),
        Ok(Some(Outcome::Denied(DenyRule::OutsideAllowedWrite)))
    );
    // A budget denial: the cap leaves room for fewer rows than the update
    // touches.
    let mut load = |db: &V2Db, task: TaskObjectId, at: DefAt<'_>| {
        Ok(catalog_def(db, task, at)?.map(|def| {
            let mut def = (*def).clone();
            def.mutation_caps.distinct_rows = 3;
            Arc::new(def)
        }))
    };
    let mut session = f.admin().sql_session();
    let request = super::commit_tests::direct(&mut session, super::commit_tests::REQUEUE);
    let error =
        commit_with(&mut session, Actor::Executor(&f.ctx), &op("requeue-2"), request, &mut load)
            .unwrap_err();
    assert_eq!(error, TaskError::BudgetExceeded(Budget::DistinctRows));
    assert_eq!(
        lookup(&f, "requeue-2"),
        Ok(Some(Outcome::BudgetExceeded(Budget::DistinctRows)))
    );
}

#[test]
fn works_after_expiry() {
    let f = fixture();
    let receipt = commit(&f, "requeue-1").unwrap();
    commit_text(&f, "too-wide", TOO_WIDE).unwrap_err();
    clock(&f.db(), EXPIRES + 60_000);
    // New writes are refused, and the refusal is not recorded.
    assert_eq!(commit(&f, "requeue-2").unwrap_err(), TaskError::Expired);
    assert_eq!(lookup(&f, "requeue-2"), Ok(None));
    // Earlier outcomes are still readable.
    assert_eq!(
        lookup(&f, "requeue-1"),
        Ok(Some(Outcome::Committed(receipt)))
    );
    assert_eq!(
        lookup(&f, "too-wide"),
        Ok(Some(Outcome::Denied(DenyRule::OutsideAllowedWrite)))
    );
}

#[test]
fn refused_for_executor_after_revocation() {
    let f = fixture();
    commit(&f, "requeue-1").unwrap();
    f.admin().tasks().revoke(&f.issuer, "inc-436").unwrap();
    for id in ["requeue-1", "unknown"] {
        let error = lookup(&f, id).unwrap_err();
        assert_eq!(error, TaskError::Revoked);
        assert_eq!(error.sqlstate(), "28T01");
    }
    // A session bound to another definition version or executor is refused
    // as well (R3 has no ALTER TASK, so either means the binding is stale).
    let f = fixture();
    commit(&f, "requeue-1").unwrap();
    let mut stale = f.ctx.clone();
    stale.bound_def_version += 1;
    assert_eq!(
        outcome(&f.admin().sql_session(), &stale, &op("requeue-1")),
        Err(TaskError::Revoked)
    );
    let mut other = f.ctx.clone();
    other.executor = "incident_controller".into();
    assert_eq!(
        outcome(&f.admin().sql_session(), &other, &op("requeue-1")),
        Err(TaskError::Revoked)
    );
}

#[test]
fn inspector_reads_after_revocation() {
    let f = fixture();
    let receipt = commit(&f, "requeue-1").unwrap();
    commit_text(&f, "too-wide", TOO_WIDE).unwrap_err();
    f.admin().tasks().revoke(&f.issuer, "inc-436").unwrap();
    clock(&f.db(), EXPIRES + 60_000);
    let Some(Outcome::Committed(full)) = inspect(&f, f.def.id, "requeue-1").unwrap() else {
        panic!("not committed");
    };
    // The full view: the same receipt plus roles, versions and the change record.
    assert!(!full.is_minimal());
    assert_eq!(full.minimal(), receipt);
    assert_eq!(full.issuer(), Some("incident_controller"));
    assert_eq!(full.executor(), Some("repair_agent"));
    assert_eq!(full.approver(), None);
    let record = full.record().unwrap();
    assert_eq!((record.def_version, record.distinct_after), (f.def.def_version, 4));
    let change = full.change().unwrap();
    assert_eq!((change.header.row_count, change.rows.len()), (4, 4));
    assert!(receipt::verify_digest(f.def.id, 1, &change.header, &change.rows));
    assert_eq!(change.header.effects_digest, receipt.effects_digest);
    assert_eq!(
        inspect(&f, f.def.id, "too-wide"),
        Ok(Some(Outcome::Denied(DenyRule::OutsideAllowedWrite)))
    );
    assert_eq!(inspect(&f, f.def.id, "unknown"), Ok(None));
}

#[test]
fn other_task_cannot_read() {
    let f = fixture();
    commit(&f, "requeue-1").unwrap();
    // A second task of the same executor on the same table.
    let new = NewTask {
        name: "inc-437".into(),
        executor: "repair_agent".into(),
        approver: None,
        schema: None,
        table: "shipments".into(),
        visible_rows: PredicateText("merchant_id = 7".into()),
        readable: ["id", "merchant_id", "status"]
            .iter()
            .map(|c| c.to_string())
            .collect(),
        masked: Vec::new(),
        untrusted_text: Vec::new(),
        allowed_write: Some(AllowedWrite {
            columns: ["status".to_string()].into_iter().collect(),
            old_row: PredicateText("status = 'failed'".into()),
            new_row: PredicateText("status = 'queued'".into()),
        }),
        mutation_caps: MutationCaps { distinct_rows: 5, transitions: 5 },
        disclosure_caps: DisclosureCaps {
            rows_read: 100,
            rows_returned: 100,
            bytes_returned: 65_536,
        },
        limits: LoadLimits::default(),
        approval_threshold: 5,
        expires: "2026-11-05 17:30:00+00".into(),
    };
    let now = crate::task::definition::WallClockMs(NOW as i64);
    let other = f.admin().tasks().create(&f.issuer, new, now).unwrap();
    assert_ne!(other.id, f.def.id);
    let ctx = TaskContext {
        task: other.id,
        name: other.name.clone(),
        executor: other.executor.clone(),
        bound_def_version: other.def_version,
        credential_fingerprint: [7; 32],
    };
    // The same operation ID under the other task is a different identity.
    assert_eq!(outcome(&f.admin().sql_session(), &ctx, &op("requeue-1")), Ok(None));
    assert_eq!(inspect(&f, other.id, "requeue-1"), Ok(None));
    // The owner still reads it.
    assert!(matches!(lookup(&f, "requeue-1"), Ok(Some(Outcome::Committed(_)))));
}

#[test]
fn lookup_writes_nothing() {
    let f = fixture();
    commit(&f, "requeue-1").unwrap();
    commit_text(&f, "too-wide", TOO_WIDE).unwrap_err();
    let db = f.db();
    let visible = db.visible_ts().unwrap();
    let dump = kernel_dump(&db);
    let head_before = head(&f).unwrap();
    for id in ["requeue-1", "too-wide", "unknown"] {
        lookup(&f, id).unwrap();
        inspect(&f, f.def.id, id).unwrap();
    }
    // After expiry and revocation too (the revoke is a Catalog write only).
    clock(&db, EXPIRES + 1);
    lookup(&f, "requeue-1").unwrap();
    assert_eq!(kernel_dump(&db), dump);
    assert_eq!(db.visible_ts().unwrap(), visible);
    assert_eq!(head(&f).unwrap(), head_before);
    // No outcome appeared for the unknown ID.
    assert!(
        kernel(&db, &ledger::task_outcome_key(&db, f.def.id, &op("unknown"))).is_none()
    );
    // A lookup never takes the hold: it answers while another transaction holds it.
    let holder = db.begin(Isolation::Serializable);
    authority::hold(&holder, f.def.id, None, &|| false).unwrap();
    assert!(matches!(lookup(&f, "requeue-1"), Ok(Some(Outcome::Committed(_)))));
    assert!(matches!(inspect(&f, f.def.id, "requeue-1"), Ok(Some(Outcome::Committed(_)))));
    drop(holder);
}
