mod common;
use common::*;
use giggles_core::{
    relational::{SqlError, Value as V},
    *,
};
use giggles_ops::*;
#[test]
fn relational_values_schema_reopen() {
    let (_, c, a) = fixture();
    let mut s = a.sql_session();
    s.execute("CREATE TABLE typed (id BIGINT PRIMARY KEY, active BOOLEAN NOT NULL, title TEXT, raw BYTEA)",&[],None).unwrap();
    s.execute(
        "INSERT INTO typed VALUES($1,$2,$3,$4)",
        &[
            V::Int(i64::MIN),
            V::Bool(true),
            V::Text("sensitive-snow ☃\0".into()),
            V::Bytes(vec![0, 255, 42]),
        ],
        None,
    )
    .unwrap();
    s.execute("INSERT INTO typed VALUES(2,false,NULL,NULL)", &[], None)
        .unwrap();
    let prepared = s.prepare("SELECT * FROM typed LIMIT 10").unwrap();
    let expected = s.execute_prepared(&prepared, &[], None).unwrap();
    let schema = a.catalog().unwrap();
    c.backup(&a, &id()).unwrap();
    let target = fresh("sql-target").join("restored");
    let report = c.restore(&id(), &target).unwrap();
    assert!(report.reopened);
    let r = Admin::open(&target, Limits::default()).unwrap();
    assert_eq!(r.catalog().unwrap(), schema);
    assert_eq!(
        r.sql_session()
            .execute("SELECT * FROM typed LIMIT 10", &[], None)
            .unwrap(),
        expected
    );
    assert_eq!(
        r.sql_session().execute_prepared(&prepared, &[], None),
        Err(CoreError::Sql(SqlError::StaleSchema))
    );
}
#[test]
fn task_receipts_dedupe_and_approvals_revoked() {
    let (_, c, a) = fixture();
    let (e, approval, decision) = protected(&a);
    let original = a.snapshot().unwrap();
    c.backup(&a, &id()).unwrap();
    let target = fresh("authority-target").join("restored");
    let report = c.restore(&id(), &target).unwrap();
    assert_eq!(report.operations, 1);
    let r = Admin::open(&target, Limits::default()).unwrap();
    let after = r.snapshot().unwrap();
    assert_ne!(after.domain, original.domain);
    assert_eq!(after.operations, original.operations);
    assert_eq!(after.rows, original.rows);
    assert!(after.approvals[&approval].revoked);
    assert!(after.tasks[&TaskId(1)].revoked);
    let re = r.executor(TaskId(1), Principal(2)).unwrap();
    assert_eq!(
        re.status(OperationId(1)).unwrap(),
        OperationStatus::Decided(decision.clone())
    );
    assert_eq!(
        re.execute(req(2), Time::Certified(11), Some(approval))
            .unwrap(),
        Err(Error::Revoked)
    );
    // Dedupe/history survives as forensic outcome; retry does not reissue authority.
    let before_replay = r.snapshot().unwrap();
    assert_eq!(
        re.execute(req(1), Time::Certified(11), Some(approval))
            .unwrap(),
        decision
    );
    let replayed = r.snapshot().unwrap();
    assert_eq!(replayed.rows, after.rows);
    assert_eq!(replayed.operations, before_replay.operations);
    assert_eq!(
        replayed.tasks[&TaskId(1)].transitions,
        after.tasks[&TaskId(1)].transitions
    );
    assert_eq!(
        e.status(OperationId(1)).unwrap(),
        OperationStatus::Decided(decision)
    );
    assert_eq!(r.authority_metadata().unwrap().3, Some(original.domain));
}
#[test]
fn nonexistent_only_and_live_locked_refusal() {
    let (_, c, a) = fixture();
    c.backup(&a, &id()).unwrap();
    let existing = fresh("existing");
    std::fs::write(existing.join("marker"), b"retain").unwrap();
    assert!(matches!(c.restore(&id(), &existing), Err(OpsError::Exists)));
    assert_eq!(std::fs::read(existing.join("marker")).unwrap(), b"retain");
    let live = fresh("live").join("db");
    let db = Admin::create(&live, Limits::default()).unwrap();
    let before = db.snapshot().unwrap();
    assert!(matches!(c.restore(&id(), &live), Err(OpsError::Exists)));
    assert_eq!(db.snapshot().unwrap(), before);
    let target = fresh("once").join("db");
    c.restore(&id(), &target).unwrap();
    let report = std::fs::read(target.join("ops-restore-report.json")).unwrap();
    assert!(matches!(c.restore(&id(), &target), Err(OpsError::Exists)));
    assert_eq!(
        std::fs::read(target.join("ops-restore-report.json")).unwrap(),
        report
    );
}
#[test]
fn restore_fault_retains_evidence() {
    for point in [
        FaultPoint::BeforeRestore,
        FaultPoint::InstallCreated,
        FaultPoint::InstallStaged,
        FaultPoint::InstallCommitted,
        FaultPoint::AfterInstall,
    ] {
        let (_, c, a) = fixture();
        c.backup(&a, &id()).unwrap();
        let target = fresh("fault-target").join("db");
        assert!(
            c.restore_with_fault(
                &id(),
                &target,
                Fault {
                    point,
                    action: FaultAction::Error
                }
            )
            .is_err()
        );
        assert_eq!(target.exists(), point != FaultPoint::BeforeRestore);
        assert!(!target.join("ops-restore-report.json").exists());
        if point != FaultPoint::BeforeRestore {
            assert!(matches!(c.restore(&id(), &target), Err(OpsError::Exists)));
        }
        assert_eq!(
            Admin::open(&target, Limits::default()).is_ok(),
            matches!(
                point,
                FaultPoint::InstallCommitted | FaultPoint::AfterInstall
            )
        );
        c.verify(&id()).unwrap();
    }
}
#[test]
fn report_contains_no_sensitive_values() {
    let (_, c, a) = fixture();
    let _protected = protected(&a);
    let mut s = a.sql_session();
    s.execute(
        "CREATE TABLE secret (id BIGINT PRIMARY KEY, value TEXT)",
        &[],
        None,
    )
    .unwrap();
    s.execute(
        "INSERT INTO secret VALUES(1,'private-secret-123')",
        &[],
        None,
    )
    .unwrap();
    c.backup(&a, &id()).unwrap();
    let target = fresh("report").join("db");
    let report = c.restore(&id(), &target).unwrap();
    let text = std::fs::read_to_string(target.join("ops-restore-report.json")).unwrap();
    assert!(!text.contains("private-secret"));
    assert!(!text.contains("tenant"));
    let saved: RestoreReport = serde_json::from_str(&text).unwrap();
    assert_eq!(saved.restored_domain, report.restored_domain);
    assert!(saved.authorities_revoked);
}

#[test]
fn concurrent_restore_one_winner_no_overwrite() {
    let (_, c, a) = fixture();
    let _protected = protected(&a);
    c.backup(&a, &id()).unwrap();
    let target = fresh("race-restore").join("db");
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
    let workers: Vec<_> = (0..4)
        .map(|_| {
            let c = c.clone();
            let target = target.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                b.wait();
                c.restore(&id(), target).is_ok()
            })
        })
        .collect();
    assert_eq!(
        workers
            .into_iter()
            .map(|w| usize::from(w.join().unwrap()))
            .sum::<usize>(),
        1
    );
    let r = Admin::open(&target, Limits::default()).unwrap();
    assert_eq!(r.snapshot().unwrap().operations.len(), 1);
    assert!(target.join("ops-restore-report.json").is_file());
}

#[test]
fn generated_snapshot_restore_state_machine() {
    let (_, c, a) = fixture();
    let mut s = a.sql_session();
    s.execute(
        "CREATE TABLE machine (id BIGINT PRIMARY KEY, n BIGINT)",
        &[],
        None,
    )
    .unwrap();
    s.execute("INSERT INTO machine VALUES(1,0)", &[], None)
        .unwrap();
    let mut expected = 0i64;
    let mut seed = 0x12345678u64;
    for step in 0..24 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        if seed & 1 == 0 {
            s.execute("BEGIN", &[], None).unwrap();
            s.execute("UPDATE machine SET n=999 WHERE id=1", &[], None)
                .unwrap();
            s.execute("ROLLBACK", &[], None).unwrap();
        } else {
            expected = (seed >> 32) as i32 as i64;
            s.execute(
                "UPDATE machine SET n=$1 WHERE id=1",
                &[V::Int(expected)],
                None,
            )
            .unwrap();
        }
        let backup = BackupId::new(format!("cut-{step}")).unwrap();
        c.backup(&a, &backup).unwrap();
        let target = fresh("machine-restore").join("db");
        c.restore(&backup, &target).unwrap();
        let r = Admin::open(target, Limits::default()).unwrap();
        assert_eq!(
            r.sql_session()
                .execute("SELECT n FROM machine WHERE id=1", &[], None)
                .unwrap()
                .rows[0]
                .values,
            vec![V::Int(expected)]
        );
    }
}
