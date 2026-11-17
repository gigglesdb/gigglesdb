//! R3-622: the retention sweeper (D5), driven by a manual wall clock.
//!
//! Task "requeue failed for merchant 42" on `shipments` (`deliveries` is
//! reserved), as in the protected-commit tests: one Direct commit changes
//! four rows and writes a `24` header, four `25` rows, a `23` receipt, a
//! `22` outcome, the `20` head and four `21` memberships.
use super::*;
use crate::storage::engine::codec;
use crate::storage::engine::commit::ManualClock;
use crate::storage::roles::{NewRole, TablePrivilege};
use crate::task::commit::{protected_commit, Actor, ProtectedRequest};
use crate::task::context::TaskContext;
use crate::task::definition::{
    AllowedWrite, DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText, TaskDef,
    WallClockMs,
};
use crate::task::ledger::{self, OperationId};
use crate::task::receipt::{self, Receipt};
use crate::task::records::{
    change_header_key, debit_key, outcome_key, read_log_key, receipt_key, LEDGER_HEADER_KEY,
    NS_AUTHORITY, NS_MAX, NS_MEMBERSHIP, NS_MIN, NS_RECEIPT, OUTCOME_PAYLOAD_BLANKED,
};
use crate::{Admin, Engine, Limits};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

const DAY: u64 = 24 * 60 * 60 * 1000;
/// 2026-11-05 17:00:00 UTC; the task expires at 17:30.
const NOW: u64 = 1_793_898_000_000;
const EXPIRES: u64 = NOW + 1_800_000;
const REQUEUE: &str =
    "UPDATE shipments SET status = 'queued' WHERE merchant_id = 42 AND status = 'failed'";

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn directory() -> Directory {
    let mut id = [0; 16];
    getrandom::fill(&mut id).unwrap();
    Directory(std::env::temp_dir().join(format!("task-retention-{:032x}", u128::from_be_bytes(id))))
}

fn sql(admin: &Admin, statement: &str) {
    admin
        .sql_session()
        .execute(statement, &[], None)
        .unwrap_or_else(|e| panic!("{statement}: {e:?}"));
}

struct Fixture {
    // Field order: the admin closes the database before the directory goes.
    admin: Admin,
    issuer: crate::roles::CatalogRole,
    ctx: TaskContext,
    def: TaskDef,
    _dir: Directory,
}

impl Fixture {
    fn db(&self) -> Arc<V2Db> {
        self.admin.sql_session().v2_db().unwrap()
    }
    fn commit(&self, id: &str) -> Receipt {
        let mut session = self.admin.sql_session();
        let request = ProtectedRequest::Direct {
            statement: session.prepare(REQUEUE).unwrap(),
            params: Vec::new(),
            text: Arc::from(REQUEUE),
        };
        protected_commit(&mut session, Actor::Executor(&self.ctx), &op(id), request).unwrap()
    }
    fn revoke(&self) {
        self.admin.tasks().revoke(&self.issuer, "inc-436").unwrap();
    }
    fn outcome_key(&self, id: &str) -> Vec<u8> {
        ledger::task_outcome_key(&self.db(), self.def.id, &op(id))
    }
}

fn op(id: &str) -> OperationId {
    OperationId::new(id.as_bytes()).unwrap()
}

fn fixture() -> Fixture {
    let dir = directory();
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    sql(
        &admin,
        "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text, \
         attempt_count int)",
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
        roles
            .grant_table("shipments", name, &[TablePrivilege::Select, TablePrivilege::Update])
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
    let def = admin.tasks().create(&issuer, new, WallClockMs(NOW as i64)).unwrap();
    assert_eq!(def.expires_at.0, EXPIRES as i64);
    let ctx = TaskContext {
        task: def.id,
        name: def.name.clone(),
        executor: def.executor.clone(),
        bound_def_version: def.def_version,
        credential_fingerprint: [7; 32],
    };
    let f = Fixture { admin, issuer, ctx, def, _dir: dir };
    clock(&f.db(), NOW);
    f
}

fn clock(db: &V2Db, ms: u64) {
    db.set_wall_clock(Arc::new(ManualClock::new(ms)));
}

/// One sweep at wall time `ms`.
fn sweep_at(db: &V2Db, ms: u64, config: &RetentionConfig) -> RetentionStats {
    clock(db, ms);
    sweep(db, config).unwrap()
}

fn kernel(db: &V2Db, key: &[u8]) -> Option<Vec<u8>> {
    db.begin(Isolation::ReadCommitted).kernel_latest_committed(key).unwrap()
}

/// Every committed entry of Kernel `[from, to)`.
fn range(db: &V2Db, from: u8, to: u8) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut txn = db.begin(Isolation::ReadCommitted);
    txn.scan(
        Keyspace::Kernel,
        Bound::Included(&[from][..]),
        Bound::Excluded(&[to][..]),
        Direction::Forward,
        None,
    )
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

fn namespace(db: &V2Db, ns: u8) -> Vec<(Vec<u8>, Vec<u8>)> {
    range(db, ns, ns + 1)
}

/// Writes raw protected records in one transaction.
fn put(db: &V2Db, records: Vec<(Vec<u8>, Vec<u8>)>) {
    let mut txn = db.begin(Isolation::ReadCommitted);
    for (key, value) in records {
        txn.put(Keyspace::Kernel, key, value).unwrap();
    }
    txn.require_features(PROTECTED_RECORDS);
    txn.commit().unwrap();
}

fn read_log(task: TaskObjectId, read_seq: u64, wall_ms: u64, pages: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = vec![(
        read_log_key(task, read_seq),
        ReadLogRecord { wall_ms, pages, outcome: 1, ..ReadLogRecord::default() }.encode(),
    )];
    for page in 0..pages {
        out.push((
            debit_key(task, read_seq, page),
            DebitRecord { wall_ms, rows_read: 1, ..DebitRecord::default() }.encode(),
        ));
    }
    out
}

fn outcome(identity: &[u8], retain_until_ms: u64, payload: OutcomePayload) -> OutcomeRecord {
    let flags = match &payload {
        OutcomePayload::Once(bytes) if bytes.is_empty() => OUTCOME_PAYLOAD_BLANKED,
        _ => 0,
    };
    OutcomeRecord {
        flags,
        identity: identity.to_vec(),
        request_fp: [1; 32],
        accepted_fp: [1; 32],
        decided_lower_ms: NOW,
        decided_upper_ms: NOW,
        retain_until_ms,
        payload,
    }
}

fn change(db: &V2Db, task: TaskObjectId, seq: u64) -> (ChangeHeader, Vec<ChangeRow>) {
    let header = ChangeHeader::decode(&kernel(db, &change_header_key(task, seq)).unwrap()).unwrap();
    let (start, end) = task_prefix(NS_CHANGE_ROW, task);
    let mut txn = db.begin(Isolation::ReadCommitted);
    let rows = txn
        .scan(Keyspace::Kernel, Bound::Included(&start), Bound::Excluded(&end), Direction::Forward, None)
        .unwrap()
        .map(|entry| ChangeRow::decode(&entry.unwrap().1).unwrap())
        .collect();
    (header, rows)
}

#[test]
fn read_log_deleted_after_90_days_with_debits() {
    let dir = directory();
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    let db = admin.sql_session().v2_db().unwrap();
    let (a, b) = (TaskObjectId(5), TaskObjectId(6));
    let mut records = read_log(a, 1, NOW, 2);
    records.extend(read_log(a, 2, NOW + 2 * DAY, 1));
    // Older than its predecessor: kept until the young entry before it goes.
    records.extend(read_log(a, 3, NOW, 1));
    records.extend(read_log(b, 1, NOW, 1));
    put(&db, records);
    let config = RetentionConfig::default();

    // Exactly 90 days old is still kept.
    let stats = sweep_at(&db, NOW + 90 * DAY, &config);
    assert_eq!(stats, RetentionStats::default());
    assert_eq!(namespace(&db, NS_READ_LOG).len(), 4);
    assert_eq!(namespace(&db, NS_DEBIT).len(), 5);

    let stats = sweep_at(&db, NOW + 90 * DAY + 1, &config);
    assert_eq!((stats.read_logs_deleted, stats.debits_deleted), (2, 3));
    let keys = |ns| namespace(&db, ns).into_iter().map(|(k, _)| k).collect::<Vec<_>>();
    assert_eq!(keys(NS_READ_LOG), [read_log_key(a, 2), read_log_key(a, 3)]);
    assert_eq!(keys(NS_DEBIT), [debit_key(a, 2, 0), debit_key(a, 3, 0)]);

    let stats = sweep_at(&db, NOW + 92 * DAY + 1, &config);
    assert_eq!((stats.read_logs_deleted, stats.debits_deleted), (2, 2));
    assert!(namespace(&db, NS_READ_LOG).is_empty());
    assert!(namespace(&db, NS_DEBIT).is_empty());
    drop(db);
    drop(admin);
}

#[test]
fn before_values_blanked_after_30_days_digest_still_verifies() {
    let f = fixture();
    let receipt = f.commit("requeue-1");
    let db = f.db();
    let task = f.def.id;
    let (header, rows) = change(&db, task, 1);
    assert_eq!(rows.len(), 4);
    assert!(rows.iter().all(|row| !row.before.is_empty() && row.flags == 0));
    let stored_receipt = kernel(&db, &receipt_key(task, 1)).unwrap();
    let config = RetentionConfig::default();

    assert_eq!(sweep_at(&db, NOW + 30 * DAY, &config), RetentionStats::default());
    assert_eq!(change(&db, task, 1), (header.clone(), rows.clone()));

    let stats = sweep_at(&db, NOW + 30 * DAY + 1, &config);
    assert_eq!((stats.headers_blanked, stats.rows_blanked, stats.outcomes_deleted), (1, 4, 0));
    let (blanked, blanked_rows) = change(&db, task, 1);
    assert_eq!(blanked.flags, CHANGE_BEFORE_BLANKED);
    assert_eq!(ChangeHeader { flags: 0, ..blanked.clone() }, header);
    for (now, was) in blanked_rows.iter().zip(&rows) {
        assert_eq!(now.flags, ROW_BEFORE_BLANKED);
        assert!(now.before.is_empty());
        assert_eq!(ChangeRow { flags: 0, before: was.before.clone(), ..now.clone() }, *was);
    }
    // The digest is checked through the hashes that stay in the rows.
    assert!(receipt::verify_digest(task, 1, &blanked, &blanked_rows));
    assert_eq!(receipt::effects_digest(task, 1, f.def.table, &blanked_rows), receipt.effects_digest);
    assert_eq!(blanked.effects_digest, receipt.effects_digest);
    let mut tampered = blanked_rows.clone();
    tampered[1].before_hash[0] ^= 1;
    assert!(!receipt::verify_digest(task, 1, &blanked, &tampered));
    // The receipt and the outcome are untouched.
    assert_eq!(kernel(&db, &receipt_key(task, 1)).unwrap(), stored_receipt);
    assert!(kernel(&db, &f.outcome_key("requeue-1")).is_some());
}

#[test]
fn outcome_kept_while_task_can_commit() {
    let f = fixture();
    f.commit("requeue-1");
    let db = f.db();
    let key = f.outcome_key("requeue-1");
    let stored = OutcomeRecord::decode(&kernel(&db, &key).unwrap()).unwrap();
    let retain = stored.retain_until_ms;
    assert_eq!(retain, EXPIRES + 30 * DAY);

    // Before retain_until_ms nothing goes, whatever the task's state.
    let stats = sweep_at(&db, retain - 1, &RetentionConfig::default());
    assert_eq!(stats.outcomes_deleted, 0);
    assert!(kernel(&db, &key).is_some());

    // Past retain_until_ms, a live task whose outcome window is still open
    // keeps the outcome.
    let longer = RetentionConfig { outcome_ms: 60 * DAY, ..RetentionConfig::default() };
    let stats = sweep_at(&db, retain, &longer);
    assert_eq!(stats.outcomes_deleted, 0);
    assert_eq!(OutcomeRecord::decode(&kernel(&db, &key).unwrap()).unwrap(), stored);

    // Revoked: the task can never commit again, so the outcome goes.
    f.revoke();
    let stats = sweep_at(&db, retain, &longer);
    assert_eq!(stats.outcomes_deleted, 1);
    assert_eq!(kernel(&db, &key), None);
}

#[test]
fn outcome_deleted_after_expiry_plus_30_days() {
    let f = fixture();
    f.commit("requeue-1");
    let db = f.db();
    let task = f.def.id;
    let retain = EXPIRES + 30 * DAY;
    let domain = db.meta.domain;
    let denied = outcome(b"\x02denied", retain, OutcomePayload::Denied { task, rule: 1, denial_no: 1 });
    let budget = outcome(
        b"\x02budget",
        retain,
        OutcomePayload::BudgetExceeded { task, budget: 1, denial_no: 2 },
    );
    // A task that no longer exists: deleted as soon as retain_until_ms passes.
    let absent = outcome(
        b"\x02absent",
        NOW,
        OutcomePayload::Committed { task: TaskObjectId(u64::MAX - 9), seq: 1 },
    );
    put(
        &db,
        [&denied, &budget, &absent]
            .iter()
            .map(|record| (outcome_key(domain, &record.identity), record.encode()))
            .collect(),
    );
    assert_eq!(namespace(&db, NS_OUTCOME).len(), 4);

    let stats = sweep_at(&db, NOW + 1, &RetentionConfig::default());
    assert_eq!(stats.outcomes_deleted, 1);
    assert_eq!(kernel(&db, &outcome_key(domain, &absent.identity)), None);

    let stats = sweep_at(&db, retain - 1, &RetentionConfig::default());
    assert_eq!(stats.outcomes_deleted, 0);
    assert_eq!(namespace(&db, NS_OUTCOME).len(), 3);

    let stats = sweep_at(&db, retain, &RetentionConfig::default());
    assert_eq!(stats.outcomes_deleted, 3);
    assert!(namespace(&db, NS_OUTCOME).is_empty());
    assert_eq!(ledger::lookup(&db, &f.outcome_key("requeue-1")).unwrap(), None);
    // The receipt stays: the sweeper never touches `23`.
    assert!(kernel(&db, &receipt_key(task, 1)).is_some());
}

/// A native `once` insert into `shipments`, published through the real
/// once engine as a kind-1 `22` record with a GDOU receipt (R3-625).
fn once_insert(
    engine: &crate::operations::Engine,
    domain: u128,
    key: &str,
    id: i64,
    retention_seconds: u64,
) {
    use crate::operations::once::{
        OnceBody, OnceIdentity, OnceRequest, OnceTime, ONCE_CONTRACT_VERSION,
    };
    use crate::operations::{Bindings, Request, ValueExpr};
    use crate::relational::Value;
    let request = OnceRequest {
        contract_version: ONCE_CONTRACT_VERSION,
        identity: OnceIdentity {
            database_domain: domain,
            tenant: 42,
            namespace: "ops".into(),
            key: key.as_bytes().to_vec(),
        },
        retention_seconds,
        body: OnceBody::Operation(Request::Insert {
            table: "shipments".into(),
            values: vec![
                ("id".into(), ValueExpr::Literal(Value::Int(id))),
                ("merchant_id".into(), ValueExpr::Literal(Value::Int(9))),
                ("status".into(), ValueExpr::Literal(Value::Text("delivered".into()))),
                ("attempt_count".into(), ValueExpr::Literal(Value::Int(0))),
            ],
        }),
    };
    let clock = || {
        Ok(OnceTime {
            lower_unix_seconds: 100,
            upper_unix_seconds: 110,
            valid_until: std::time::Instant::now() + Duration::from_secs(10),
        })
    };
    assert!(!engine.execute_once(&request, &Bindings::new(), &clock).unwrap().replayed);
}

#[test]
fn once_records_untouched() {
    let f = fixture();
    f.commit("requeue-1");
    let db = f.db();
    let domain = u128::from_be_bytes(db.meta.domain);
    // Two native once outcomes in the shared `22` ledger: one live GDOU
    // receipt, and one whose payload compaction has blanked.
    let engine = crate::operations::Engine::new(f.admin.clone());
    once_insert(&engine, domain, "live", 100, 600);
    once_insert(&engine, domain, "old", 101, 1);
    let compact_clock = || {
        Ok(crate::operations::once::OnceTime {
            lower_unix_seconds: 200,
            upper_unix_seconds: 210,
            valid_until: std::time::Instant::now() + Duration::from_secs(10),
        })
    };
    assert_eq!(engine.compact_outcomes(&compact_clock), Ok(1));
    let mut once_records: Vec<_> = namespace(&db, NS_OUTCOME)
        .into_iter()
        .filter(|(_, value)| OutcomeRecord::decode(value).unwrap().payload.kind() == 1)
        .collect();
    once_records.sort();
    let decoded: Vec<_> =
        once_records.iter().map(|(_, value)| OutcomeRecord::decode(value).unwrap()).collect();
    assert_eq!(decoded.len(), 2);
    assert!(decoded.iter().any(|record| record.flags == 0
        && matches!(&record.payload, OutcomePayload::Once(bytes) if bytes.starts_with(b"GDOU"))));
    assert!(decoded.iter().any(|record| record.flags == OUTCOME_PAYLOAD_BLANKED
        && record.payload == OutcomePayload::Once(Vec::new())));
    let ledger_header = kernel(&db, &LEDGER_HEADER_KEY).unwrap();
    f.revoke();
    let kept = |db: &V2Db| {
        [NS_AUTHORITY, NS_MEMBERSHIP, NS_RECEIPT]
            .into_iter()
            .map(|ns| namespace(db, ns))
            .collect::<Vec<_>>()
    };
    let before = kept(&db);
    assert!(before.iter().all(|entries| !entries.is_empty()));

    let stats = sweep_at(&db, NOW + 3650 * DAY, &RetentionConfig::default());
    assert_eq!((stats.outcomes_deleted, stats.headers_blanked), (1, 1));
    assert_eq!(kept(&db), before);
    let mut outcomes = namespace(&db, NS_OUTCOME);
    outcomes.sort();
    assert_eq!(outcomes, once_records);
    assert_eq!(kernel(&db, &LEDGER_HEADER_KEY).unwrap(), ledger_header);
    // The change header stays; only its before-values went.
    assert_eq!(namespace(&db, NS_CHANGE_HEADER).len(), 1);
}

#[test]
fn sweep_is_idempotent_and_batched() {
    let f = fixture();
    f.commit("requeue-1");
    let db = f.db();
    let task = f.def.id;
    put(&db, (1..=7).flat_map(|seq| read_log(task, seq, NOW, 1)).collect());
    f.revoke();
    let config = RetentionConfig { batch: 3, ..RetentionConfig::default() };

    let stats = sweep_at(&db, NOW + 3650 * DAY, &config);
    assert_eq!(
        (
            stats.read_logs_deleted,
            stats.debits_deleted,
            stats.rows_blanked,
            stats.headers_blanked,
            stats.outcomes_deleted
        ),
        (7, 7, 4, 1, 1)
    );
    assert_eq!(stats.largest_transaction, 3);
    // 20 writes in transactions of at most 3.
    assert!(stats.transactions >= 7, "{stats:?}");
    let after = range(&db, NS_MIN, NS_MAX + 1);

    let again = sweep_at(&db, NOW + 3650 * DAY, &config);
    assert_eq!(again, RetentionStats::default());
    assert_eq!(range(&db, NS_MIN, NS_MAX + 1), after);
    // A larger batch finds nothing either.
    assert_eq!(sweep_at(&db, NOW + 3650 * DAY, &RetentionConfig::default()), RetentionStats::default());
}

/// Raw stored versions of one Kernel user key.
fn versions(db: &V2Db, key: &[u8]) -> usize {
    db.kv
        .range(Keyspace::Kernel, Bound::Unbounded, Bound::Unbounded, Direction::Forward)
        .unwrap()
        .filter(|pair| codec::split_versioned(&pair.as_ref().unwrap().0).unwrap().0 == key)
        .count()
}

#[test]
fn gc_then_removes_old_versions_physically() {
    let f = fixture();
    f.commit("requeue-1");
    let db = f.db();
    let task = f.def.id;
    put(&db, read_log(task, 1, NOW, 2));
    f.revoke();
    let row_keys: Vec<Vec<u8>> = namespace(&db, NS_CHANGE_ROW).into_iter().map(|(k, _)| k).collect();
    let gone = [
        read_log_key(task, 1),
        debit_key(task, 1, 0),
        debit_key(task, 1, 1),
        f.outcome_key("requeue-1"),
    ];

    // Through the settings the maintenance thread uses.
    f.admin.set_task_retention(RetentionConfig { batch: 2, ..RetentionConfig::default() }).unwrap();
    clock(&db, NOW + 3650 * DAY);
    let stats = db.retention_step().unwrap();
    assert_eq!((stats.read_logs_deleted, stats.rows_blanked, stats.outcomes_deleted), (1, 4, 1));
    assert_eq!(stats.largest_transaction, 2);
    // Logically gone, physically still there: the old value plus a tombstone
    // or a blanked version.
    for key in gone.iter().chain(&row_keys) {
        assert_eq!(versions(&db, key), 2);
    }

    // With zero GC retention the watermark is `visible` truncated to whole
    // milliseconds; a later commit in a fresh millisecond moves it past the sweep.
    std::thread::sleep(Duration::from_millis(2));
    put(&db, read_log(TaskObjectId(1), 1, u64::MAX, 0));
    f.admin.set_gc_retention(Duration::ZERO).unwrap();
    f.admin.maintenance_step().unwrap();

    for key in &gone {
        assert_eq!(versions(&db, key), 0, "deleted record collected");
    }
    for key in &row_keys {
        assert_eq!(versions(&db, key), 1, "only the blanked row remains");
        let row = ChangeRow::decode(&kernel(&db, key).unwrap()).unwrap();
        assert!(row.before.is_empty());
    }
}
