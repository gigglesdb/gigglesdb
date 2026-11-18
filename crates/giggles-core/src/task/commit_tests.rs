//! R3-615: `protected_commit`, the Direct success path.
//!
//! Task "requeue failed for merchant 42, 25 rows" on `shipments` (`deliveries`
//! is reserved): visible rows `merchant_id = 42`, write `status` from
//! 'failed' to 'queued', 25 distinct rows / 25 transitions, approval above 25.
use super::*;
use crate::storage::engine::commit::ManualClock;
use crate::storage::engine::kv::Direction;
use crate::storage::roles::NewRole;
use crate::task::definition::{
    AllowedWrite, DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText, TaskName,
};
use crate::task::records::{
    membership_key, mutation_head_key, task_prefix, ChangeHeader, ChangeRow, Membership,
    NS_CHANGE_ROW, NS_MEMBERSHIP,
};
use crate::{Admin, Engine, Limits};
use std::ops::Bound;
use std::path::PathBuf;

#[path = "commit_denial_tests.rs"]
mod denial;

/// 2026-11-05 17:00:00 UTC; the task expires at 17:30.
pub(super) const NOW: u64 = 1_793_898_000_000;
pub(super) const EXPIRES: u64 = NOW + 1_800_000;
pub(super) const REQUEUE: &str =
    "UPDATE shipments SET status = 'queued' WHERE merchant_id = 42 AND status = 'failed'";

pub(super) struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn directory() -> Directory {
    let mut id = [0; 16];
    getrandom::fill(&mut id).unwrap();
    Directory(std::env::temp_dir().join(format!("task-commit-{:032x}", u128::from_be_bytes(id))))
}

pub(super) fn sql(admin: &Admin, statement: &str) -> Vec<Vec<Value>> {
    admin
        .sql_session()
        .execute(statement, &[], None)
        .unwrap_or_else(|e| panic!("{statement}: {e:?}"))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

pub(super) struct Fixture {
    pub(super) dir: Directory,
    pub(super) admin: Option<Admin>,
    pub(super) issuer: crate::roles::CatalogRole,
    pub(super) ctx: TaskContext,
    pub(super) def: TaskDef,
}

impl Fixture {
    pub(super) fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }
    pub(super) fn db(&self) -> Arc<V2Db> {
        self.admin().sql_session().v2_db().unwrap()
    }
    pub(super) fn statuses(&self) -> Vec<String> {
        sql(self.admin(), "SELECT status FROM shipments ORDER BY id")
            .into_iter()
            .map(|row| match &row[0] {
                Value::Text(text) => text.clone(),
                other => panic!("{other:?}"),
            })
            .collect()
    }
    pub(super) fn reopen(&mut self) {
        drop(self.admin.take());
        self.admin = Some(Admin::open(&self.dir.0, Limits::default()).unwrap());
        clock(&self.db(), NOW);
    }
}

pub(super) fn clock(db: &V2Db, ms: u64) {
    db.set_wall_clock(Arc::new(ManualClock::new(ms)));
}

pub(super) fn fixture() -> Fixture {
    fixture_key(false)
}

pub(super) fn fixture_key(prefix: bool) -> Fixture {
    let dir = directory();
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    sql(
        &admin,
        if prefix {
            "CREATE TABLE shipments (id bigint, merchant_id bigint, status text, attempt_count int, PRIMARY KEY (merchant_id, id))"
        } else {
            "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text, attempt_count int)"
        },
    );
    sql(
        &admin,
        "INSERT INTO shipments VALUES (1, 42, 'failed', 1), (2, 42, 'failed', 2), \
         (3, 42, 'failed', 1), (4, 42, 'failed', 3), (5, 42, 'delivered', 0), (6, 7, 'failed', 1)",
    );
    let roles = admin.roles();
    for (name, task_only) in [("incident_controller", false), ("repair_agent", true)] {
        roles
            .create(NewRole { name: name.into(), login: true, ..NewRole::default() })
            .unwrap();
        if task_only {
            roles.set_task_only(name, true).unwrap();
        }
    }
    for role in ["incident_controller", "repair_agent"] {
        roles
            .grant_table("shipments", role, &[TablePrivilege::Select, TablePrivilege::Update])
            .unwrap();
    }
    let issuer = roles.get("incident_controller").unwrap().unwrap();
    let new = NewTask {
        name: "inc-436".into(),
        executor: "repair_agent".into(),
        approver: None,
        schema: None,
        table: "shipments".into(),
        visible_rows: PredicateText("merchant_id = 42".into()),
        readable: ["id", "merchant_id", "status", "attempt_count"]
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
        mutation_caps: MutationCaps { distinct_rows: 25, transitions: 25 },
        disclosure_caps: DisclosureCaps {
            rows_read: 1000,
            rows_returned: 1000,
            bytes_returned: 2_097_152,
        },
        limits: LoadLimits::default(),
        approval_threshold: 25,
        expires: "2026-11-05 17:30:00+00".into(),
    };
    let now = crate::task::definition::WallClockMs(NOW as i64);
    let def = admin.tasks().create(&issuer, new, now).unwrap();
    assert_eq!(def.expires_at.0, EXPIRES as i64);
    let ctx = TaskContext {
        task: def.id,
        name: def.name.clone(),
        executor: def.executor.clone(),
        bound_def_version: def.def_version,
        credential_fingerprint: [7; 32],
    };
    let f = Fixture { dir, admin: Some(admin), issuer, ctx, def };
    clock(&f.db(), NOW);
    f
}

pub(super) fn op(id: &str) -> OperationId {
    OperationId::new(id.as_bytes()).unwrap()
}

pub(super) fn direct(session: &mut SqlSession, text: &str) -> ProtectedRequest {
    ProtectedRequest::Direct {
        statement: session.prepare(text).unwrap(),
        params: Vec::new(),
        text: Arc::from(text),
    }
}

pub(super) fn commit_text(f: &Fixture, id: &str, text: &str) -> Result<Receipt, TaskError> {
    let mut session = f.admin().sql_session();
    let request = direct(&mut session, text);
    protected_commit(&mut session, Actor::Executor(&f.ctx), &op(id), request)
}

pub(super) fn commit(f: &Fixture, id: &str) -> Result<Receipt, TaskError> {
    commit_text(f, id, REQUEUE)
}

fn commit_loading(f: &Fixture, id: &str, load: &mut LoadDef<'_>) -> Result<Receipt, TaskError> {
    let mut session = f.admin().sql_session();
    let request = direct(&mut session, REQUEUE);
    commit_with(&mut session, Actor::Executor(&f.ctx), &op(id), request, load)
}

pub(super) fn kernel(db: &V2Db, key: &[u8]) -> Option<Vec<u8>> {
    db.begin(Isolation::ReadCommitted)
        .kernel_latest_committed(key)
        .unwrap()
}

pub(super) fn scan(db: &V2Db, ns: u8, task: TaskObjectId) -> Vec<(Vec<u8>, Vec<u8>)> {
    let (start, end) = task_prefix(ns, task);
    let mut txn = db.begin(Isolation::ReadCommitted);
    txn.scan(
        Keyspace::Kernel,
        Bound::Included(&start),
        Bound::Excluded(&end),
        Direction::Forward,
        None,
    )
    .unwrap()
    .map(|entry| entry.unwrap())
    .collect()
}

pub(super) fn head(f: &Fixture) -> Option<MutationHead> {
    kernel(&f.db(), &mutation_head_key(f.def.id)).map(|v| MutationHead::decode(&v).unwrap())
}

fn outcome(f: &Fixture, id: &str) -> Option<OutcomeRecord> {
    let db = f.db();
    ledger::lookup(&db, &ledger::task_outcome_key(&db, f.def.id, &op(id))).unwrap()
}

/// Nothing at all was recorded for the task, and the rows are unchanged.
fn nothing_recorded(f: &Fixture, id: &str) {
    assert_eq!(head(f), None);
    assert_eq!(outcome(f, id), None);
    assert!(scan(&f.db(), NS_MEMBERSHIP, f.def.id).is_empty());
    assert_eq!(f.statuses(), ["failed", "failed", "failed", "failed", "delivered", "failed"]);
}

fn pk(f: &Fixture, id: i64) -> Vec<u8> {
    // Physical keys are `table BE ‖ pk`; read them from the change record.
    let rows = scan(&f.db(), NS_CHANGE_ROW, f.def.id);
    let row = ChangeRow::decode(&rows[usize::try_from(id - 1).unwrap()].1).unwrap();
    row.pk
}

#[test]
fn four_row_update_commits_rows_head_membership_change_receipt_outcome() {
    let f = fixture();
    let receipt = commit(&f, "requeue-1").unwrap();
    assert!(receipt.is_minimal());
    assert_eq!(receipt.operation_id, b"requeue-1");
    assert_eq!(receipt.decision, DECISION_COMMITTED);
    assert_eq!(receipt.seq, 1);
    assert_eq!(receipt.rows_affected, 4);
    assert_eq!(
        (receipt.distinct_charged, receipt.transitions_charged, receipt.noop_rows),
        (4, 4, 0)
    );
    assert_eq!(receipt.decided_ms, NOW);
    // Rows: merchant 42's failed rows only.
    assert_eq!(f.statuses(), ["queued", "queued", "queued", "queued", "delivered", "failed"]);
    // Head.
    let head = head(&f).unwrap();
    assert_eq!(
        (head.def_version, head.distinct_rows, head.transitions, head.next_seq),
        (f.def.def_version, 4, 4, 2)
    );
    assert_eq!((head.next_preview, head.denials, head.last_decided_ms), (1, 0, NOW));
    // Membership: one entry per changed row.
    let db = f.db();
    let members = scan(&db, NS_MEMBERSHIP, f.def.id);
    assert_eq!(members.len(), 4);
    for id in 1..=4 {
        let key = membership_key(f.def.id, f.def.table, &pk(&f, id));
        let member = Membership::decode(&kernel(&db, &key).unwrap()).unwrap();
        assert_eq!(member, Membership { first_seq: 1, last_seq: 1, transitions: 1 });
    }
    // Change record.
    let header = ChangeHeader::decode(&kernel(&db, &change_header_key(f.def.id, 1)).unwrap()).unwrap();
    let key = ledger::task_outcome_key(&db, f.def.id, &op("requeue-1"));
    assert_eq!((header.kind, header.table, header.row_count), (1, f.def.table, 4));
    assert_eq!(&header.outcome_key[..], &key[1..]);
    assert_eq!(header.effects_digest, receipt.effects_digest);
    assert_eq!(scan(&db, NS_CHANGE_ROW, f.def.id).len(), 4);
    // Receipt record: the full view behind the minimal one.
    let stored = ReceiptRecord::decode(&kernel(&db, &receipt_key(f.def.id, 1)).unwrap()).unwrap();
    assert_eq!(Receipt::from_record(&stored, None).minimal(), receipt);
    assert_eq!(stored.request_kind, ledger::KIND_DIRECT);
    assert_eq!((stored.distinct_after, stored.transitions_after), (4, 4));
    assert_eq!(
        (stored.issuer.as_str(), stored.executor.as_str(), stored.approver.as_deref()),
        ("incident_controller", "repair_agent", None)
    );
    assert_eq!(stored.preview, 0);
    // Outcome.
    let outcome = outcome(&f, "requeue-1").unwrap();
    assert_eq!(outcome.payload, OutcomePayload::Committed { task: f.def.id, seq: 1 });
    assert_eq!(outcome.request_fp, receipt.request_fp);
    assert_eq!(outcome.identity, ledger::task_identity(f.def.id, &op("requeue-1")));
    assert_eq!((outcome.decided_lower_ms, outcome.decided_upper_ms), (NOW, NOW));
    assert_eq!(outcome.retain_until_ms, EXPIRES + OUTCOME_RETENTION_MS);
}

#[test]
fn resend_same_request_returns_same_receipt_and_charges_nothing() {
    let f = fixture();
    let first = commit(&f, "requeue-1").unwrap();
    let head_after = head(&f).unwrap();
    // Put one row back: a re-run would change and charge it again.
    sql(f.admin(), "UPDATE shipments SET status = 'failed' WHERE id = 1");
    clock(&f.db(), NOW + 60_000);
    let again = commit(&f, "requeue-1").unwrap();
    assert_eq!(again, first);
    assert_eq!(head(&f).unwrap(), head_after);
    assert_eq!(f.statuses(), ["failed", "queued", "queued", "queued", "delivered", "failed"]);
    assert!(kernel(&f.db(), &receipt_key(f.def.id, 2)).is_none());
}

#[test]
fn different_request_same_id_conflicts() {
    let f = fixture();
    commit(&f, "requeue-1").unwrap();
    sql(f.admin(), "UPDATE shipments SET status = 'failed' WHERE id = 1");
    let other = "UPDATE shipments SET status = 'queued' WHERE merchant_id = 42 AND id = 1";
    let error = commit_text(&f, "requeue-1", other).unwrap_err();
    assert_eq!(error, TaskError::OperationConflict);
    assert_eq!(error.sqlstate(), "42T03");
    assert_eq!(head(&f).unwrap().next_seq, 2);
    assert_eq!(f.statuses()[0], "failed");
    // The same text under a new ID is a new operation.
    let receipt = commit_text(&f, "requeue-2", other).unwrap();
    assert_eq!((receipt.seq, receipt.rows_affected, receipt.distinct_charged), (2, 1, 0));
    assert_eq!(receipt.transitions_charged, 1);
}

#[test]
fn revoked_before_and_during_run() {
    // Before: step 3 refuses.
    let f = fixture();
    f.admin().tasks().revoke(&f.issuer, "inc-436").unwrap();
    let error = commit(&f, "requeue-1").unwrap_err();
    assert_eq!(error, TaskError::Revoked);
    assert_eq!(error.sqlstate(), "28T01");
    nothing_recorded(&f, "requeue-1");

    // While waiting for the hold: step 6 re-reads the latest definition.
    let f = fixture();
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
    assert_eq!(commit_loading(&f, "requeue-1", &mut load).unwrap_err(), TaskError::Revoked);
    nothing_recorded(&f, "requeue-1");

    // After the statement snapshot: the tracked catalog read fails the
    // commit's validation, and the internal retry (R3-616) sees the revoke.
    let f = fixture();
    let mut revoked = false;
    let mut load = |db: &V2Db, task: TaskObjectId, at: DefAt<'_>| {
        if matches!(at, DefAt::Snapshot(_)) && !revoked {
            revoked = true;
            f.admin().tasks().revoke(&f.issuer, "inc-436").unwrap();
        }
        catalog_def(db, task, at)
    };
    let error = commit_loading(&f, "requeue-1", &mut load).unwrap_err();
    assert_eq!(error, TaskError::Revoked);
    nothing_recorded(&f, "requeue-1");
    assert_eq!(commit(&f, "requeue-1").unwrap_err(), TaskError::Revoked);
}

#[test]
fn expired_by_clock() {
    let f = fixture();
    clock(&f.db(), EXPIRES);
    let error = commit(&f, "requeue-1").unwrap_err();
    assert_eq!(error, TaskError::Expired);
    assert_eq!(error.sqlstate(), "28T02");
    nothing_recorded(&f, "requeue-1");
    // One millisecond earlier the same request commits.
    clock(&f.db(), EXPIRES - 1);
    assert_eq!(commit(&f, "requeue-1").unwrap().decided_ms, EXPIRES - 1);
}

#[test]
fn privilege_revoked_before_snapshot_is_refused() {
    let f = fixture();
    f.admin()
        .roles()
        .revoke_table("shipments", "repair_agent", &[TablePrivilege::Update])
        .unwrap();
    let error = commit(&f, "requeue-1").unwrap_err();
    assert_eq!(error, TaskError::Denied(DenyRule::RolePrivilege));
    assert_eq!(error.sqlstate(), "42501");
    nothing_recorded(&f, "requeue-1");
    // Granted again, the operation ID is still free.
    f.admin()
        .roles()
        .grant_table("shipments", "repair_agent", &[TablePrivilege::Update])
        .unwrap();
    assert_eq!(commit(&f, "requeue-1").unwrap().rows_affected, 4);
}

#[test]
fn effects_digest_matches_stored_rows() {
    let f = fixture();
    let receipt = commit(&f, "requeue-1").unwrap();
    let db = f.db();
    let header = ChangeHeader::decode(&kernel(&db, &change_header_key(f.def.id, 1)).unwrap()).unwrap();
    let rows: Vec<ChangeRow> = scan(&db, NS_CHANGE_ROW, f.def.id)
        .iter()
        .map(|(_, value)| ChangeRow::decode(value).unwrap())
        .collect();
    assert!(receipt::verify_digest(f.def.id, 1, &header, &rows));
    assert_eq!(
        receipt::effects_digest(f.def.id, 1, f.def.table, &rows),
        receipt.effects_digest
    );
    // Each row's after-hash is the hash of the row as now stored, and its
    // before-values are the row as it was.
    let now = sql(f.admin(), "SELECT * FROM shipments WHERE id <= 4 ORDER BY id");
    for (row, values) in rows.iter().zip(&now) {
        let after = crate::storage::task_row_payload(values).unwrap();
        assert_eq!(row.after_hash, receipt::row_hash(&header.salt, &after));
        let mut before = values.clone();
        before[2] = Value::Text("failed".into());
        assert_eq!(row.before, crate::storage::task_row_payload(&before).unwrap());
        assert_eq!(row.flags, 0);
        assert!(row.version_written > row.version_before);
    }
    // A tampered row no longer verifies.
    let mut tampered = rows.clone();
    tampered[0].after_hash[0] ^= 1;
    assert!(!receipt::verify_digest(f.def.id, 1, &header, &tampered));
}

fn dummy_ctx() -> TaskContext {
    TaskContext {
        task: TaskObjectId(1),
        name: TaskName::new("inc-436").unwrap(),
        executor: "repair_agent".into(),
        bound_def_version: 1,
        credential_fingerprint: [0; 32],
    }
}

#[test]
fn replicated_database_refused_0a000() {
    use crate::storage::engine::kv::{Durability, KvBackend, WriteBatch};
    let backend = Arc::new(crate::storage::engine::kv_mem::MemKv::new());
    crate::storage::engine::meta::write_initial_meta(backend.as_ref(), [3; 16], crate::storage::engine::meta::REPLICATED).unwrap();
    let mut batch = WriteBatch::default();
    batch.put(Keyspace::Meta, b"raft_applied".to_vec(), vec![0; 16]);
    backend.write(batch, Durability::Sync).unwrap();
    let admin = Admin::attach_v2_backend(
        backend,
        Limits::default(),
        crate::storage::v2::DEFAULT_WRITE_SET_BYTES,
    )
    .unwrap();
    sql(&admin, "CREATE TABLE shipments (id bigint primary key, status text)");
    sql(&admin, "INSERT INTO shipments VALUES (1, 'failed')");
    let mut session = admin.sql_session();
    let request = direct(&mut session, "UPDATE shipments SET status = 'queued'");
    let ctx = dummy_ctx();
    let error = protected_commit(&mut session, Actor::Executor(&ctx), &op("r"), request).unwrap_err();
    assert_eq!(error, TaskError::Core(Box::new(CoreError::Unsupported)));
    assert_eq!(error.sqlstate(), "0A000");
    assert_eq!(sql(&admin, "SELECT status FROM shipments"), [[Value::Text("failed".into())]]);
}

#[test]
fn legacy_engine_refused_0a000() {
    let dir = directory();
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::Legacy).unwrap();
    sql(&admin, "CREATE TABLE shipments (id bigint primary key, status text)");
    sql(&admin, "INSERT INTO shipments VALUES (1, 'failed')");
    let mut session = admin.sql_session();
    assert!(session.v2_db().is_none());
    let request = direct(&mut session, "UPDATE shipments SET status = 'queued'");
    let ctx = dummy_ctx();
    let error = protected_commit(&mut session, Actor::Executor(&ctx), &op("r"), request).unwrap_err();
    assert_eq!(error.sqlstate(), "0A000");
    assert_eq!(sql(&admin, "SELECT status FROM shipments"), [[Value::Text("failed".into())]]);
}

#[test]
fn reopen_preserves_everything() {
    let mut f = fixture();
    let receipt = commit(&f, "requeue-1").unwrap();
    let db = f.db();
    let before: Vec<_> = [NS_MEMBERSHIP, NS_CHANGE_ROW]
        .into_iter()
        .map(|ns| scan(&db, ns, f.def.id))
        .collect();
    let head_before = head(&f).unwrap();
    let outcome_before = outcome(&f, "requeue-1").unwrap();
    drop(db);
    f.reopen();
    assert_eq!(head(&f).unwrap(), head_before);
    assert_eq!(outcome(&f, "requeue-1").unwrap(), outcome_before);
    let db = f.db();
    for (ns, rows) in [NS_MEMBERSHIP, NS_CHANGE_ROW].into_iter().zip(&before) {
        assert_eq!(&scan(&db, ns, f.def.id), rows);
    }
    assert_eq!(f.statuses(), ["queued", "queued", "queued", "queued", "delivered", "failed"]);
    // The stored outcome answers the resend; the next operation takes seq 2.
    assert_eq!(commit(&f, "requeue-1").unwrap(), receipt);
    sql(f.admin(), "UPDATE shipments SET status = 'failed' WHERE id = 6");
    sql(f.admin(), "UPDATE shipments SET merchant_id = 42 WHERE id = 6");
    let next = commit(&f, "requeue-2").unwrap();
    assert_eq!((next.seq, next.rows_affected, next.distinct_charged), (2, 1, 1));
}

/// A caller's row bound (the native intent) on the real affected count.
fn commit_bounded(f: &Fixture, id: &str, at_most: usize) -> Result<Receipt, TaskError> {
    let mut session = f.admin().sql_session();
    let request = direct(&mut session, REQUEUE);
    let check = |rows: usize| {
        if rows > at_most {
            Err(TaskError::Sql(SqlError::CardinalityViolation))
        } else {
            Ok(())
        }
    };
    protected_commit_checked(&mut session, Actor::Executor(&f.ctx), &op(id), request, &check)
}

#[test]
fn row_check_refusal_records_nothing_and_leaves_the_id_free() {
    let f = fixture();
    // Four rows match; a bound of one (and of three) refuses before staging.
    for bound in [1, 3] {
        assert_eq!(
            commit_bounded(&f, "requeue-1", bound),
            Err(TaskError::Sql(SqlError::CardinalityViolation))
        );
        nothing_recorded(&f, "requeue-1");
    }
    // The ID stayed free: the same request under a bound of four commits.
    let receipt = commit_bounded(&f, "requeue-1", 4).unwrap();
    assert_eq!(receipt.rows_affected, 4);
    assert_eq!(f.statuses(), ["queued", "queued", "queued", "queued", "delivered", "failed"]);
    // A replay returns the stored receipt; the check does not run again.
    assert_eq!(commit_bounded(&f, "requeue-1", 0).unwrap(), receipt);
}
