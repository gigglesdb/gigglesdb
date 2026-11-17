//! R3-618: the Apply attachment point of `protected_commit`, driven by a fake
//! [`ApplySource`] (the stored preview and approval arrive with Phase 7).
//!
//! Same task as `commit_tests`: requeue merchant 42's failed `shipments`
//! (`deliveries` is reserved), 25 distinct rows / 25 transitions, approval
//! above 25 rows. The fake source's preview holds merchant 42's four failed
//! rows; a different matching set is stale.
use super::commit_tests::{fixture, head, kernel, op, sql, Fixture, NOW, REQUEUE};
use super::*;
use crate::task::records::{approval_key, ApprovalRecord};
use std::cell::{Cell, RefCell};

const PREVIEW: PreviewId = PreviewId(1);
const DIGEST: [u8; 32] = [0x5a; 32];

/// A stored preview of `REQUEUE` that matched `rows` rows, approved or not.
struct FakeSource<'f> {
    task: TaskObjectId,
    rows: usize,
    approved: bool,
    /// The seq of every `stage_consumed` call, one per attempt that staged.
    consumed: &'f RefCell<Vec<u64>>,
    /// `approval_state` calls.
    checks: &'f Cell<usize>,
}

impl ApplySource for FakeSource<'_> {
    fn statement(&self, session: &mut SqlSession) -> Result<(Prepared, Vec<Value>), TaskError> {
        Ok((session.prepare(REQUEUE)?, Vec::new()))
    }

    fn approval_state(&self, _txn: &mut Transaction, effects: &Effects) -> Result<ApprovalState, CoreError> {
        self.checks.set(self.checks.get() + 1);
        Ok(if effects.len() != self.rows {
            ApprovalState::Stale
        } else if self.approved {
            ApprovalState::Matching
        } else {
            ApprovalState::Missing
        })
    }

    fn stage_consumed(&self, txn: &mut Transaction, seq: u64) -> Result<(), CoreError> {
        self.consumed.borrow_mut().push(seq);
        let record = ApprovalRecord {
            state: 3,
            approver: "incident_controller".into(),
            approver_oid: 0,
            digest: DIGEST,
            approved_ms: NOW,
            consumed_seq: seq,
        };
        txn.put(Keyspace::Kernel, approval_key(self.task, PREVIEW.0), record.encode())?;
        Ok(())
    }
}

struct Probe {
    consumed: RefCell<Vec<u64>>,
    checks: Cell<usize>,
}

fn probe() -> Probe {
    Probe { consumed: RefCell::new(Vec::new()), checks: Cell::new(0) }
}

fn apply_request(digest: [u8; 32]) -> ProtectedRequest {
    ProtectedRequest::Apply { preview: PREVIEW, digest }
}

/// Apply `PREVIEW` under `id` through a fake source, with `load` serving the
/// definition.
fn apply_loading(
    f: &Fixture,
    id: &str,
    digest: [u8; 32],
    source: (usize, bool),
    probe: &Probe,
    load: &mut LoadDef<'_>,
) -> Result<Receipt, TaskError> {
    let (rows, approved) = source;
    let mut resolved = 0;
    let mut sources = |_: &V2Db, ctx: &TaskContext, preview: PreviewId, _: &[u8; 32]| {
        resolved += 1;
        assert_eq!(preview, PREVIEW);
        Ok(Box::new(FakeSource {
            task: ctx.task,
            rows,
            approved,
            consumed: &probe.consumed,
            checks: &probe.checks,
        }) as Box<dyn ApplySource + '_>)
    };
    let mut session = f.admin().sql_session();
    let result = commit_sourced(
        &mut session,
        Actor::Executor(&f.ctx),
        &op(id),
        apply_request(digest),
        load,
        &mut sources,
    );
    assert!(resolved <= 1, "the source is resolved once per call");
    result
}

fn apply(f: &Fixture, id: &str, source: (usize, bool), probe: &Probe) -> Result<Receipt, TaskError> {
    apply_loading(f, id, DIGEST, source, probe, &mut catalog_def)
}

fn approval(f: &Fixture) -> Option<ApprovalRecord> {
    kernel(&f.db(), &approval_key(f.def.id, PREVIEW.0)).map(|v| ApprovalRecord::decode(&v).unwrap())
}

fn stored_outcome_of(f: &Fixture, id: &str) -> Option<OutcomeRecord> {
    let db = f.db();
    ledger::lookup(&db, &ledger::task_outcome_key(&db, f.def.id, &op(id))).unwrap()
}

/// Nothing recorded for `id` and the rows are unchanged.
fn unrecorded(f: &Fixture, id: &str, probe: &Probe) {
    assert_eq!(head(f), None);
    assert_eq!(stored_outcome_of(f, id), None);
    assert_eq!(approval(f), None);
    assert!(probe.consumed.borrow().is_empty());
    assert_eq!(f.statuses(), ["failed", "failed", "failed", "failed", "delivered", "failed"]);
}

#[test]
fn stale_source_gives_approval_stale_unrecorded() {
    let f = fixture();
    // A fifth failed row for merchant 42 appeared after the preview.
    sql(f.admin(), "INSERT INTO shipments VALUES (7, 42, 'failed', 0)");
    let p = probe();
    let error = apply(&f, "apply-1", (4, true), &p).unwrap_err();
    assert_eq!(error, TaskError::ApprovalStale);
    assert_eq!(error.sqlstate(), TaskError::ApprovalStale.sqlstate());
    assert_eq!(p.checks.get(), 1);
    assert_eq!(head(&f), None);
    assert_eq!(stored_outcome_of(&f, "apply-1"), None);
    assert_eq!(approval(&f), None);
    assert!(p.consumed.borrow().is_empty());
    assert_eq!(
        f.statuses(),
        ["failed", "failed", "failed", "failed", "delivered", "failed", "failed"]
    );
    // The ID stays free: once the extra row is gone the same ID applies.
    sql(f.admin(), "DELETE FROM shipments WHERE id = 7");
    let receipt = apply(&f, "apply-1", (4, true), &p).unwrap();
    assert_eq!(receipt.rows_affected, 4);
}

#[test]
fn matching_source_commits_and_stages_consumption_in_same_batch() {
    let f = fixture();
    let p = probe();
    // The first attempt's commit fails validation (a grant commits after its
    // statement snapshot); the retry stages the consumption again.
    let grants = Cell::new(0);
    let mut load = |db: &V2Db, task: TaskObjectId, at: DefAt<'_>| {
        if matches!(at, DefAt::Snapshot(_)) && grants.get() == 0 {
            grants.set(1);
            f.admin()
                .roles()
                .grant_table("shipments", "incident_controller", &[TablePrivilege::Insert])
                .unwrap();
        }
        catalog_def(db, task, at)
    };
    let receipt = apply_loading(&f, "apply-1", DIGEST, (4, true), &p, &mut load).unwrap();
    assert_eq!((receipt.seq, receipt.rows_affected, receipt.distinct_charged), (1, 4, 4));
    // Staged by both attempts, committed once, with the receipt's seq.
    assert_eq!(*p.consumed.borrow(), [1, 1]);
    assert_eq!(p.checks.get(), 2);
    let consumed = approval(&f).unwrap();
    assert_eq!((consumed.state, consumed.consumed_seq), (3, receipt.seq));
    // Everything else of the commit is there with it.
    assert_eq!(f.statuses(), ["queued", "queued", "queued", "queued", "delivered", "failed"]);
    let stored = ReceiptRecord::decode(&kernel(&f.db(), &receipt_key(f.def.id, 1)).unwrap()).unwrap();
    assert_eq!((stored.request_kind, stored.preview), (ledger::KIND_APPLY, PREVIEW.0));
    assert_eq!(
        stored_outcome_of(&f, "apply-1").unwrap().payload,
        OutcomePayload::Committed { task: f.def.id, seq: 1 }
    );
    assert_eq!(head(&f).unwrap().next_seq, 2);
    // A refusal after the source was asked stages nothing: the second apply
    // re-runs on rows that no longer match (zero effects), which is stale.
    let q = probe();
    assert_eq!(apply(&f, "apply-2", (4, true), &q).unwrap_err(), TaskError::ApprovalStale);
    assert!(q.consumed.borrow().is_empty());
    assert_eq!(approval(&f).unwrap().consumed_seq, 1);
}

#[test]
fn missing_approval_above_threshold() {
    let f = fixture();
    let threshold = |limit: u64| {
        move |db: &V2Db, task: TaskObjectId, at: DefAt<'_>| {
            Ok(catalog_def(db, task, at)?.map(|def| {
                let mut def = (*def).clone();
                def.approval_threshold = limit;
                Arc::new(def)
            }))
        }
    };
    // Four rows above a threshold of 2 and no approval: refused, not recorded.
    let p = probe();
    let error = apply_loading(&f, "apply-1", DIGEST, (4, false), &p, &mut threshold(2)).unwrap_err();
    assert_eq!(error, TaskError::ApprovalRequired);
    assert_eq!(p.checks.get(), 1);
    unrecorded(&f, "apply-1", &p);
    // With a matching approval the same ID commits and consumes it.
    let receipt = apply_loading(&f, "apply-1", DIGEST, (4, true), &p, &mut threshold(2)).unwrap();
    assert_eq!(receipt.rows_affected, 4);
    assert_eq!(*p.consumed.borrow(), [1]);
    assert_eq!(approval(&f).unwrap().consumed_seq, 1);
    // At or under the threshold no approval is needed. Every Apply commit
    // calls `stage_consumed`; the source decides what it consumes (D7.6
    // step 6: the preview header always, the approval only if one was used).
    let g = fixture();
    let q = probe();
    let receipt = apply(&g, "apply-1", (4, false), &q).unwrap();
    assert_eq!(receipt.rows_affected, 4);
    assert_eq!(q.checks.get(), 1);
    assert_eq!(*q.consumed.borrow(), [1]);
}

#[test]
fn apply_fingerprint_differs_from_direct() {
    let f = fixture();
    let direct_receipt = super::commit_tests::commit(&f, "requeue-1").unwrap();
    sql(f.admin(), "UPDATE shipments SET status = 'failed' WHERE merchant_id = 42 AND id <= 4");
    // The same rows applied under the direct write's ID are another request.
    let p = probe();
    assert_eq!(apply(&f, "requeue-1", (4, true), &p).unwrap_err(), TaskError::OperationConflict);
    assert_eq!(p.checks.get(), 0);
    assert!(p.consumed.borrow().is_empty());
    // Under a new ID the apply commits with its own fingerprint.
    let receipt = apply(&f, "apply-1", (4, true), &p).unwrap();
    assert_ne!(receipt.request_fp, direct_receipt.request_fp);
    let body = ledger::RequestBody::Apply { preview: PREVIEW.0, digest: DIGEST };
    assert_eq!(receipt.request_fp, ledger::request_fingerprint(f.def.id, &body));
    let direct_body = ledger::RequestBody::Direct { text: REQUEUE.as_bytes(), params: Vec::new() };
    assert_eq!(direct_receipt.request_fp, ledger::request_fingerprint(f.def.id, &direct_body));
    // A resend replays without asking the source again.
    let q = probe();
    assert_eq!(apply(&f, "apply-1", (4, true), &q).unwrap(), receipt);
    assert_eq!(q.checks.get(), 0);
    assert!(q.consumed.borrow().is_empty());
    // Another digest under the same ID conflicts.
    let other = apply_loading(&f, "apply-1", [0xa5; 32], (4, true), &q, &mut catalog_def);
    assert_eq!(other.unwrap_err(), TaskError::OperationConflict);
    // A direct write under the apply's ID conflicts too.
    let error = super::commit_tests::commit(&f, "apply-1").unwrap_err();
    assert_eq!(error, TaskError::OperationConflict);
}
