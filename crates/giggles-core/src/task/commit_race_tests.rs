//! R3-623: `protected_commit` under real concurrency: ten sessions racing for
//! the last slot or one operation ID, an application writer between the
//! statement's scan and the commit, a grant/revoke storm, DDL, two tasks on
//! one table, and counters against the change records after 200 mixed
//! operations.
//!
//! Same task as `commit_tests`: requeue merchant 42's failed `shipments`
//! (`deliveries` is reserved), 25 distinct rows / 25 transitions. Threads
//! meet at `Barrier`s; no sleep is an oracle. Where a test needs another
//! thread to be blocked, it waits for the lock manager to report a waiter.
use super::commit_tests::{
    commit, direct, fixture, head as task_head, kernel, op, scan, sql, Fixture, REQUEUE,
};
use super::*;
use crate::storage::engine::hlc::Ts;
use crate::storage::engine::kv::Direction;
use crate::storage::engine::mvcc;
use crate::task::definition::{
    AllowedWrite, DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText,
};
use crate::task::records::{
    mutation_head_key, ChangeHeader, Membership, NS_CHANGE_HEADER, NS_CHANGE_ROW, NS_MEMBERSHIP,
    NS_RECEIPT,
};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Barrier, Mutex};
use std::thread;

const SESSIONS: usize = 10;

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

fn commit_as(
    f: &Fixture,
    ctx: &TaskContext,
    id: &str,
    text: &str,
    load: &mut LoadDef<'_>,
) -> Result<Receipt, TaskError> {
    let mut session = f.admin().sql_session();
    let request = direct(&mut session, text);
    commit_with(&mut session, Actor::Executor(ctx), &op(id), request, load)
}

fn stored(f: &Fixture, id: &str) -> Option<OutcomeRecord> {
    let db = f.db();
    ledger::lookup(&db, &ledger::task_outcome_key(&db, f.def.id, &op(id))).unwrap()
}

fn insert_failed(f: &Fixture, ids: std::ops::Range<i64>) {
    let values: Vec<String> = ids.map(|id| format!("({id}, 42, 'failed', 0)")).collect();
    sql(f.admin(), &format!("INSERT INTO shipments VALUES {}", values.join(", ")));
}

fn count(f: &Fixture, filter: &str) -> i64 {
    match &sql(f.admin(), &format!("SELECT count(*) FROM shipments WHERE {filter}"))[0][0] {
        Value::Int(n) => *n,
        other => panic!("{other:?}"),
    }
}

fn receipts(f: &Fixture) -> Vec<ReceiptRecord> {
    scan(&f.db(), NS_RECEIPT, f.def.id)
        .iter()
        .map(|(_, value)| ReceiptRecord::decode(value).unwrap())
        .collect()
}

/// Spins (yielding) until `ready`, failing after ten seconds.
fn until(what: &str, ready: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::yield_now();
    }
}

#[test]
fn ten_sessions_last_slot_exactly_one_commits_nine_budget_exceeded() {
    let f = fixture();
    // 20 more failed rows: the first operation uses 24 of the 25 slots.
    insert_failed(&f, 10..30);
    let first = commit(&f, "fill").unwrap();
    assert_eq!((first.rows_affected, first.distinct_charged), (24, 24));
    // Ten new rows, one per session; each would take the last slot.
    insert_failed(&f, 100..100 + SESSIONS as i64);
    let gate = Barrier::new(SESSIONS);
    let results: Vec<(usize, Result<Receipt, TaskError>)> = thread::scope(|s| {
        let handles: Vec<_> = (0..SESSIONS)
            .map(|n| {
                let (f, gate) = (&f, &gate);
                s.spawn(move || {
                    let text = format!(
                        "UPDATE shipments SET status = 'queued' \
                         WHERE merchant_id = 42 AND status = 'failed' AND id = {}",
                        100 + n
                    );
                    let mut session = f.admin().sql_session();
                    let request = direct(&mut session, &text);
                    let id = op(&format!("last-{n}"));
                    gate.wait();
                    (n, protected_commit(&mut session, Actor::Executor(&f.ctx), &id, request))
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let winners: Vec<_> = results.iter().filter(|(_, r)| r.is_ok()).collect();
    assert_eq!(winners.len(), 1, "{results:?}");
    let (winner, receipt) = (winners[0].0, winners[0].1.as_ref().unwrap());
    assert_eq!((receipt.seq, receipt.rows_affected, receipt.distinct_charged), (2, 1, 1));
    for (n, result) in &results {
        if *n == winner {
            continue;
        }
        let error = result.as_ref().unwrap_err();
        assert_eq!(*error, TaskError::BudgetExceeded(Budget::DistinctRows), "session {n}");
        assert_eq!(error.sqlstate(), "54T01");
        // Durable: the stored denial, replayed on a resend.
        let record = stored(&f, &format!("last-{n}")).unwrap();
        assert!(matches!(
            record.payload,
            OutcomePayload::BudgetExceeded { task, budget, .. }
                if task == f.def.id && budget == Budget::DistinctRows.code()
        ));
        let text = format!(
            "UPDATE shipments SET status = 'queued' \
             WHERE merchant_id = 42 AND status = 'failed' AND id = {}",
            100 + n
        );
        let mut session = f.admin().sql_session();
        let request = direct(&mut session, &text);
        let again =
            protected_commit(&mut session, Actor::Executor(&f.ctx), &op(&format!("last-{n}")), request);
        assert_eq!(again.unwrap_err(), *error);
    }
    // Denial numbers 1..=9, each once.
    let mut numbers: Vec<u64> = results
        .iter()
        .filter(|(n, _)| *n != winner)
        .map(|(n, _)| match stored(&f, &format!("last-{n}")).unwrap().payload {
            OutcomePayload::BudgetExceeded { denial_no, .. } => denial_no,
            other => panic!("{other:?}"),
        })
        .collect();
    numbers.sort_unstable();
    assert_eq!(numbers, (1..=9).collect::<Vec<_>>());
    let head = task_head(&f).unwrap();
    assert_eq!(
        (head.distinct_rows, head.transitions, head.next_seq, head.denials),
        (25, 25, 3, 9)
    );
    assert_eq!(scan(&f.db(), NS_MEMBERSHIP, f.def.id).len(), 25);
    assert_eq!(receipts(&f).len(), 2);
    // Only the winner's row moved.
    assert_eq!(count(&f, "id >= 100 AND status = 'queued'"), 1);
    assert_eq!(
        count(&f, &format!("id = {} AND status = 'queued'", 100 + winner)),
        1
    );
}

#[test]
fn ten_sessions_same_op_id_one_execution_ten_equal_receipts() {
    let f = fixture();
    let gate = Barrier::new(SESSIONS);
    let results: Vec<Result<Receipt, TaskError>> = thread::scope(|s| {
        let handles: Vec<_> = (0..SESSIONS)
            .map(|_| {
                let (f, gate) = (&f, &gate);
                s.spawn(move || {
                    let mut session = f.admin().sql_session();
                    let request = direct(&mut session, REQUEUE);
                    gate.wait();
                    protected_commit(&mut session, Actor::Executor(&f.ctx), &op("same"), request)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let first = results[0].as_ref().unwrap().clone();
    for result in &results {
        assert_eq!(result.as_ref().unwrap(), &first);
    }
    assert_eq!((first.seq, first.rows_affected, first.distinct_charged), (1, 4, 4));
    // One execution: one receipt, one change record, the head moved once.
    let head = task_head(&f).unwrap();
    assert_eq!(
        (head.next_seq, head.distinct_rows, head.transitions, head.denials),
        (2, 4, 4, 0)
    );
    assert_eq!(receipts(&f).len(), 1);
    assert_eq!(scan(&f.db(), NS_CHANGE_HEADER, f.def.id).len(), 1);
    assert_eq!(scan(&f.db(), NS_CHANGE_ROW, f.def.id).len(), 4);
    assert_eq!(
        stored(&f, "same").unwrap().payload,
        OutcomePayload::Committed { task: f.def.id, seq: 1 }
    );
    assert_eq!(f.statuses(), ["queued", "queued", "queued", "queued", "delivered", "failed"]);
}

/// Runs REQUEUE as "app-race"; at its first statement-snapshot recheck (after
/// the scan, before the commit) another thread runs `app` and commits.
/// Returns the result and the number of statement-snapshot reads.
fn race_app_writer(f: &Fixture, app: &str) -> (Result<Receipt, TaskError>, usize) {
    let (go, done) = (Barrier::new(2), Barrier::new(2));
    let reads = AtomicUsize::new(0);
    thread::scope(|s| {
        let writer = s.spawn(|| {
            go.wait();
            sql(f.admin(), app);
            done.wait();
        });
        let mut load = |db: &V2Db, task: TaskObjectId, at: DefAt<'_>| {
            if matches!(at, DefAt::Snapshot(_)) && reads.fetch_add(1, Ordering::SeqCst) == 0 {
                go.wait();
                done.wait();
            }
            catalog_def(db, task, at)
        };
        let result = commit_as(f, &f.ctx, "app-race", REQUEUE, &mut load);
        writer.join().unwrap();
        (result, reads.load(Ordering::SeqCst))
    })
}

#[test]
fn app_insert_between_scan_and_commit_is_retried_then_denied_or_committed_exactly() {
    // Committed exactly: the retry's statement sees and changes the new row.
    let f = fixture();
    let (result, reads) =
        race_app_writer(&f, "INSERT INTO shipments VALUES (7, 42, 'failed', 0)");
    let receipt = result.unwrap();
    assert_eq!(reads, 2, "the phantom must fail the first attempt");
    assert_eq!((receipt.seq, receipt.rows_affected, receipt.distinct_charged), (1, 5, 5));
    assert_eq!(
        f.statuses(),
        ["queued", "queued", "queued", "queued", "delivered", "failed", "queued"]
    );
    let head = task_head(&f).unwrap();
    assert_eq!((head.next_seq, head.distinct_rows, head.transitions), (2, 5, 5));
    assert_eq!(scan(&f.db(), NS_CHANGE_ROW, f.def.id).len(), 5);
    assert_eq!(scan(&f.db(), NS_MEMBERSHIP, f.def.id).len(), 5);

    // Denied: 22 new rows make the retry's 26 rows exceed the 25-row cap.
    let f = fixture();
    let values: Vec<String> = (10..32).map(|id| format!("({id}, 42, 'failed', 0)")).collect();
    let app = format!("INSERT INTO shipments VALUES {}", values.join(", "));
    let (result, reads) = race_app_writer(&f, &app);
    assert_eq!(reads, 2, "the phantom must fail the first attempt");
    assert_eq!(result.unwrap_err(), TaskError::BudgetExceeded(Budget::DistinctRows));
    assert!(matches!(
        stored(&f, "app-race").unwrap().payload,
        OutcomePayload::BudgetExceeded { denial_no: 1, .. }
    ));
    let head = task_head(&f).unwrap();
    assert_eq!((head.next_seq, head.distinct_rows, head.denials), (1, 0, 1));
    assert_eq!(count(&f, "status = 'queued'"), 0);
    assert!(scan(&f.db(), NS_MEMBERSHIP, f.def.id).is_empty());
    assert!(receipts(&f).is_empty());
}

/// The catalog key of `repair_agent`'s grant on `shipments` (role image
/// prefix `tgrant/`, encoded under key byte 11).
fn grant_key(db: &V2Db) -> Vec<u8> {
    let mut txn = db.begin(Isolation::ReadCommitted);
    let keys: Vec<Vec<u8>> = txn
        .scan(
            Keyspace::Catalog,
            Bound::Included(&[11u8][..]),
            Bound::Excluded(&[12u8][..]),
            Direction::Forward,
            None,
        )
        .unwrap()
        .map(|entry| entry.unwrap().0)
        .filter(|key| {
            let has = |needle: &[u8]| key.windows(needle.len()).any(|w| w == needle);
            has(b"shipments") && has(b"repair_agent")
        })
        .collect();
    assert_eq!(keys.len(), 1, "{keys:?}");
    keys.into_iter().next().unwrap()
}

fn latest_ts(db: &V2Db, ks: Keyspace, key: &[u8]) -> Ts {
    mvcc::latest_version(db.kv.as_ref(), ks, key).unwrap().unwrap().0
}

#[test]
fn grant_revoke_storm_never_commits_without_privilege() {
    // Deterministic part: a revoke between the statement snapshot and the
    // commit fails the commit's validation; the retry sees it and refuses.
    let f = fixture();
    let mut revoked = false;
    let mut load = |db: &V2Db, task: TaskObjectId, at: DefAt<'_>| {
        if matches!(at, DefAt::Snapshot(_)) && !revoked {
            revoked = true;
            f.admin()
                .roles()
                .revoke_table("shipments", "repair_agent", &[TablePrivilege::Update])
                .unwrap();
        }
        catalog_def(db, task, at)
    };
    let error = commit_as(&f, &f.ctx, "storm", REQUEUE, &mut load).unwrap_err();
    assert_eq!(error, TaskError::Denied(DenyRule::RolePrivilege));
    assert_eq!(stored(&f, "storm"), None);
    assert_eq!(task_head(&f), None);
    assert_eq!(count(&f, "status = 'queued'"), 0);

    // The storm: each round, one toggle of UPDATE races one protected commit.
    // Every commit's serialization point must see the privilege: held at its
    // statement snapshot S, and no grant change in (S, C], C its commit.
    const ROUNDS: usize = 40;
    let f = fixture();
    let db = f.db();
    let key = grant_key(&db);
    // (version ts, UPDATE held after it); only the storm writes this key.
    let timeline = Mutex::new(vec![(latest_ts(&db, Keyspace::Catalog, &key), true)]);
    let gate = Barrier::new(2);
    let caps = |def: &mut TaskDef| {
        def.mutation_caps = MutationCaps { distinct_rows: 1_000, transitions: 1_000 };
    };
    let results: Vec<(Result<Receipt, TaskError>, Option<Ts>)> = thread::scope(|s| {
        s.spawn(|| {
            let roles = f.admin().roles();
            for round in 0..ROUNDS {
                gate.wait();
                let held = round % 2 == 1;
                if held {
                    roles.grant_table("shipments", "repair_agent", &[TablePrivilege::Update])
                } else {
                    roles.revoke_table("shipments", "repair_agent", &[TablePrivilege::Update])
                }
                .unwrap();
                let ts = latest_ts(&db, Keyspace::Catalog, &key);
                timeline.lock().unwrap().push((ts, held));
                gate.wait();
            }
        });
        let mut load = edited(caps);
        (0..ROUNDS)
            .map(|round| {
                sql(f.admin(), "UPDATE shipments SET status = 'failed' WHERE id <= 4");
                gate.wait();
                let result = commit_as(&f, &f.ctx, &format!("storm-{round}"), REQUEUE, &mut load);
                // Single committer: the head's newest version is this commit.
                let committed = result
                    .is_ok()
                    .then(|| latest_ts(&db, Keyspace::Kernel, &mutation_head_key(f.def.id)));
                gate.wait();
                (result, committed)
            })
            .collect()
    });
    let timeline = timeline.into_inner().unwrap();
    assert_eq!(timeline.len(), ROUNDS + 1);
    let held_at = |ts: u64| {
        timeline
            .iter()
            .filter(|(at, _)| at.0 <= ts)
            .max_by_key(|(at, _)| *at)
            .unwrap()
            .1
    };
    let records: BTreeMap<u64, ReceiptRecord> =
        receipts(&f).into_iter().map(|r| (r.seq, r)).collect();
    let mut committed = 0;
    for (round, (result, commit_ts)) in results.iter().enumerate() {
        match result {
            Ok(receipt) => {
                committed += 1;
                let record = &records[&receipt.seq];
                let (snapshot, commit_ts) = (record.catalog_version, commit_ts.unwrap().0);
                assert!(snapshot < commit_ts, "round {round}");
                assert!(held_at(snapshot), "round {round}: committed without UPDATE at its snapshot");
                assert!(
                    !timeline
                        .iter()
                        .any(|(at, _)| at.0 > snapshot && at.0 <= commit_ts),
                    "round {round}: the grant changed between snapshot and commit"
                );
                assert_eq!(receipt.rows_affected, 4, "round {round}");
            }
            Err(error) => {
                assert!(
                    matches!(error.sqlstate(), "42501" | "40001"),
                    "round {round}: {error:?}"
                );
                assert_eq!(stored(&f, &format!("storm-{round}")), None, "round {round}");
            }
        }
    }
    eprintln!("R3-623 storm: {committed} of {ROUNDS} rounds committed");
    assert_eq!(records.len(), committed);
    let head = task_head(&f).unwrap_or(MutationHead { next_seq: 1, ..MutationHead::default() });
    assert_eq!(head.next_seq, committed as u64 + 1);
    assert_eq!(head.denials, 0);
}

#[test]
fn ddl_during_commit_waits() {
    let f = fixture();
    let (go, ddl_done) = (Barrier::new(2), AtomicBool::new(false));
    let (reads, waited) = (AtomicUsize::new(0), AtomicBool::new(false));
    let (result, ddl) = thread::scope(|s| {
        let ddl = s.spawn(|| {
            go.wait();
            let result = f
                .admin()
                .sql_session()
                .execute("ALTER TABLE shipments ADD COLUMN note text", &[], None)
                .map(|_| ());
            ddl_done.store(true, Ordering::SeqCst);
            result
        });
        let mut load = |db: &V2Db, task: TaskObjectId, at: DefAt<'_>| {
            if matches!(at, DefAt::Snapshot(_)) && reads.fetch_add(1, Ordering::SeqCst) == 0 {
                // The statement's locks are held: the DDL must queue behind them.
                go.wait();
                let probe = db.begin(Isolation::ReadCommitted);
                until("the DDL to wait or finish", || {
                    probe.sql_waiting_count() > 0 || ddl_done.load(Ordering::SeqCst)
                });
                waited.store(!ddl_done.load(Ordering::SeqCst), Ordering::SeqCst);
            }
            catalog_def(db, task, at)
        };
        let result = commit_as(&f, &f.ctx, "ddl", REQUEUE, &mut load);
        (result, ddl.join().unwrap())
    });
    assert!(waited.load(Ordering::SeqCst), "the DDL finished while the commit held its locks");
    let receipt = result.unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 1, "the commit must not be retried");
    assert_eq!((receipt.seq, receipt.rows_affected), (1, 4));
    ddl.unwrap();
    // The DDL ran after the commit: its column exists, the rows are requeued.
    assert_eq!(sql(f.admin(), "SELECT note FROM shipments WHERE id = 1"), [[Value::Null]]);
    assert_eq!(f.statuses(), ["queued", "queued", "queued", "queued", "delivered", "failed"]);
}

/// A second task on `shipments` for merchant 7, same executor.
fn merchant_seven(f: &Fixture) -> (TaskDef, TaskContext) {
    let new = NewTask {
        name: "inc-437".into(),
        executor: "repair_agent".into(),
        approver: None,
        schema: None,
        table: "shipments".into(),
        visible_rows: PredicateText("merchant_id = 7".into()),
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
    let now = crate::task::definition::WallClockMs(super::commit_tests::NOW as i64);
    let def = f.admin().tasks().create(&f.issuer, new, now).unwrap();
    let ctx = TaskContext {
        task: def.id,
        name: def.name.clone(),
        executor: def.executor.clone(),
        bound_def_version: def.def_version,
        credential_fingerprint: [8; 32],
    };
    (def, ctx)
}

#[test]
fn two_tasks_same_table_do_not_block_each_other_on_authority() {
    let f = fixture();
    let (seven, seven_ctx) = merchant_seven(&f);
    assert_ne!(seven.id, f.def.id);
    let (held, release) = (Barrier::new(2), Barrier::new(2));
    let reads = AtomicUsize::new(0);
    let (first, second) = thread::scope(|s| {
        // Task inc-436 stops mid-commit, holding its authority and its
        // statement's locks, until task inc-437 has committed.
        let first = s.spawn(|| {
            let mut load = |db: &V2Db, task: TaskObjectId, at: DefAt<'_>| {
                if matches!(at, DefAt::Snapshot(_)) && reads.fetch_add(1, Ordering::SeqCst) == 0 {
                    held.wait();
                    release.wait();
                }
                catalog_def(db, task, at)
            };
            commit_as(&f, &f.ctx, "a-1", REQUEUE, &mut load)
        });
        held.wait();
        // A short lock wait: blocking on inc-436 surfaces 55P03, not a hang.
        let mut load = edited(|def| def.limits.lock_wait_ms = 5_000);
        let started = Instant::now();
        let second = commit_as(
            &f,
            &seven_ctx,
            "b-1",
            "UPDATE shipments SET status = 'queued' WHERE merchant_id = 7 AND status = 'failed'",
            &mut load,
        );
        let elapsed = started.elapsed();
        release.wait();
        (first.join().unwrap(), (second, elapsed))
    });
    let (second, elapsed) = second;
    let second = second.unwrap();
    assert!(elapsed < Duration::from_secs(5), "inc-437 waited {elapsed:?}");
    assert_eq!((second.seq, second.rows_affected), (1, 1));
    let first = first.unwrap();
    assert_eq!((first.seq, first.rows_affected), (1, 4));
    assert_eq!(f.statuses(), ["queued", "queued", "queued", "queued", "delivered", "queued"]);
    // Each task's head counts only its own rows.
    assert_eq!(task_head(&f).unwrap().distinct_rows, 4);
    let other = kernel(&f.db(), &mutation_head_key(seven.id)).unwrap();
    let other = MutationHead::decode(&other).unwrap();
    assert_eq!((other.distinct_rows, other.next_seq), (1, 2));
}

#[test]
fn counters_equal_change_record_totals_after_200_mixed_ops() {
    const THREADS: usize = 4;
    const OPS: usize = 50;
    let f = fixture();
    // Each thread owns ids base..base+5 (failed) and base+5 (delivered).
    for t in 0..THREADS as i64 {
        let base = 1_000 + t * 10;
        insert_failed(&f, base..base + 5);
        sql(
            f.admin(),
            &format!("INSERT INTO shipments VALUES ({}, 42, 'delivered', 0)", base + 5),
        );
    }
    let caps = |def: &mut TaskDef| {
        def.mutation_caps = MutationCaps { distinct_rows: 1_000, transitions: 150 };
    };
    let gate = Barrier::new(THREADS);
    // (op id, result) per call, in each thread's order.
    let calls: Vec<Vec<(String, Result<Receipt, TaskError>)>> = thread::scope(|s| {
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let (f, gate) = (&f, &gate);
                s.spawn(move || {
                    let base = 1_000 + t as i64 * 10;
                    let requeue = format!(
                        "UPDATE shipments SET status = 'queued' WHERE merchant_id = 42 \
                         AND status = 'failed' AND id >= {base} AND id < {}",
                        base + 5
                    );
                    let too_wide = format!(
                        "UPDATE shipments SET status = 'queued' WHERE merchant_id = 42 \
                         AND id >= {base} AND id <= {}",
                        base + 5
                    );
                    let none = format!(
                        "UPDATE shipments SET status = 'queued' WHERE merchant_id = 42 \
                         AND status = 'failed' AND id = {}",
                        base + 9
                    );
                    let mut load = edited(caps);
                    let mut history: Vec<(String, String)> = Vec::new();
                    let mut out = Vec::new();
                    gate.wait();
                    for i in 0..OPS {
                        let fresh = format!("t{t}-op{i}");
                        let (id, text) = match i % 6 {
                            0 | 3 => {
                                let reset = format!(
                                    "UPDATE shipments SET status = 'failed' \
                                     WHERE id >= {base} AND id < {} AND id % 2 = {}",
                                    base + 5,
                                    i % 2
                                );
                                // Another thread's scan may conflict with this write.
                                let _ = f.admin().sql_session().execute(&reset, &[], None);
                                (fresh, requeue.clone())
                            }
                            1 => (fresh, too_wide.clone()),
                            2 => (fresh, none.clone()),
                            // A resend of an earlier call.
                            4 => history[i / 2 % history.len()].clone(),
                            // An earlier ID with another request.
                            _ => {
                                let (id, text) = history[i / 3 % history.len()].clone();
                                let other = if text == none { requeue.clone() } else { none.clone() };
                                (id, other)
                            }
                        };
                        let result = commit_as(f, &f.ctx, &id, &text, &mut load);
                        if !history.iter().any(|(known, _)| *known == id) {
                            history.push((id.clone(), text));
                        }
                        out.push((id, result));
                    }
                    out
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let calls: Vec<_> = calls.into_iter().flatten().collect();
    assert_eq!(calls.len(), THREADS * OPS);
    // Every result is one of the documented outcomes.
    let mut kinds: BTreeMap<&str, usize> = BTreeMap::new();
    for (id, result) in &calls {
        let kind = match result {
            Ok(_) => "committed",
            Err(TaskError::Denied(DenyRule::OutsideAllowedWrite)) => "denied",
            Err(TaskError::BudgetExceeded(Budget::Transitions)) => "budget",
            Err(TaskError::OperationConflict) => "conflict",
            Err(TaskError::Retry) => "retry",
            Err(other) => panic!("{id}: {other:?}"),
        };
        *kinds.entry(kind).or_default() += 1;
    }
    eprintln!("R3-623 mixed ops: {kinds:?}");
    for kind in ["committed", "denied", "conflict"] {
        assert!(kinds.contains_key(kind), "no {kind} call: {kinds:?}");
    }
    let db = f.db();
    let head = task_head(&f).unwrap();
    let records = receipts(&f);
    let headers: Vec<ChangeHeader> = scan(&db, NS_CHANGE_HEADER, f.def.id)
        .iter()
        .map(|(_, value)| ChangeHeader::decode(value).unwrap())
        .collect();
    let members: Vec<Membership> = scan(&db, NS_MEMBERSHIP, f.def.id)
        .iter()
        .map(|(_, value)| Membership::decode(value).unwrap())
        .collect();
    let change_rows = scan(&db, NS_CHANGE_ROW, f.def.id).len() as u64;
    // Sequence numbers: one receipt and one change record per commit.
    assert_eq!(records.len() as u64, head.next_seq - 1, "{kinds:?}");
    assert_eq!(headers.len(), records.len());
    assert_eq!(
        records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        (1..head.next_seq).collect::<Vec<_>>()
    );
    // Rows: the change records hold every affected row.
    let affected: u64 = records.iter().map(|r| u64::from(r.rows_affected)).sum();
    let header_rows: u64 = headers.iter().map(|h| u64::from(h.row_count)).sum();
    assert_eq!((affected, header_rows), (change_rows, change_rows));
    // Transitions: head = receipts = membership, within the cap.
    let charged: u64 = records.iter().map(|r| u64::from(r.transitions_charged)).sum();
    let member_transitions: u64 = members.iter().map(|m| u64::from(m.transitions)).sum();
    assert_eq!((head.transitions, member_transitions), (charged, charged));
    assert!(head.transitions <= 150);
    let noops: u64 = records.iter().map(|r| u64::from(r.noop_rows)).sum();
    assert_eq!(charged + noops, change_rows);
    // Distinct rows: head = receipts = membership entries.
    let distinct: u64 = records.iter().map(|r| u64::from(r.distinct_charged)).sum();
    assert_eq!((head.distinct_rows, members.len() as u64), (distinct, distinct));
    // The last receipt's after-counters are the head's.
    let last = records.last().unwrap();
    assert_eq!((last.distinct_after, last.transitions_after), (head.distinct_rows, head.transitions));
    // Outcomes: one per decided ID; denials numbered 1..=head.denials.
    let ids: std::collections::BTreeSet<&String> = calls.iter().map(|(id, _)| id).collect();
    let mut denial_numbers = Vec::new();
    let mut committed_seqs = Vec::new();
    for id in ids {
        match stored(&f, id).map(|record| record.payload) {
            Some(OutcomePayload::Committed { seq, .. }) => committed_seqs.push(seq),
            Some(
                OutcomePayload::Denied { denial_no, .. }
                | OutcomePayload::BudgetExceeded { denial_no, .. },
            ) => denial_numbers.push(denial_no),
            None => {}
            Some(other) => panic!("{id}: {other:?}"),
        }
    }
    committed_seqs.sort_unstable();
    denial_numbers.sort_unstable();
    assert_eq!(committed_seqs, (1..head.next_seq).collect::<Vec<_>>());
    assert_eq!(denial_numbers, (1..=head.denials).collect::<Vec<_>>());
    // Callers saw the same: a committed or denied call's resend agrees.
    for (id, result) in &calls {
        match (result, stored(&f, id).map(|record| record.payload)) {
            (Ok(receipt), Some(OutcomePayload::Committed { seq, .. })) => {
                assert_eq!(receipt.seq, seq, "{id}");
            }
            (Err(TaskError::Denied(_)), Some(OutcomePayload::Denied { .. }))
            | (Err(TaskError::BudgetExceeded(_)), Some(OutcomePayload::BudgetExceeded { .. }))
            | (Err(TaskError::OperationConflict), Some(_))
            | (Err(TaskError::Retry), _) => {}
            (result, stored) => panic!("{id}: {result:?} but stored {stored:?}"),
        }
    }
}

#[test]
fn key_prefix_only_conflicts_with_in_scope_app_insert() {
    for (merchant, attempts, affected) in [(7, 1, 4), (42, 2, 5)] {
        let f = super::commit_tests::fixture_key(true);
        let (result, reads) = race_app_writer(&f,
            &format!("INSERT INTO shipments VALUES (7, {merchant}, 'failed', 0)"));
        assert_eq!(result.unwrap().rows_affected, affected);
        // protected_commit retries 40001 once: two snapshot rechecks prove
        // the first attempt failed; an outside-prefix write needs no retry.
        assert_eq!(reads, attempts, "merchant {merchant}");
    }
}

#[test]
fn key_prefix_in_scope_writes_exhaust_retries_with_40001() {
    let f = super::commit_tests::fixture_key(true);
    let mut reads = 0;
    let mut load = |db: &V2Db, task: TaskObjectId, at: DefAt<'_>| {
        if matches!(at, DefAt::Snapshot(_)) {
            reads += 1;
            // Each independent application transaction commits after the
            // protected attempt's scan, inserting a phantom in merchant 42.
            sql(f.admin(), &format!("INSERT INTO shipments VALUES ({}, 42, 'failed', 0)", 100 + reads));
        }
        catalog_def(db, task, at)
    };
    let error = commit_as(&f, &f.ctx, "prefix-conflict", REQUEUE, &mut load).unwrap_err();
    assert_eq!(error.sqlstate(), "40001");
    assert_eq!(reads, 3);
    assert_eq!(count(&f, "status = 'queued'"), 0);
}
