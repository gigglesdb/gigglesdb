mod common;
use common::*;
use giggles_core::*;

fn cap(read_rows: u64, disclosed_rows: u64, encoded_bytes: u64) -> TaskResources {
    TaskResources {
        read_rows,
        disclosed_rows,
        encoded_bytes,
    }
}
fn used(a: &Admin) -> TaskResources {
    a.task_resources(TaskId(1)).unwrap().unwrap().used
}
fn budget<T: std::fmt::Debug>(r: Result<T, CoreError>) {
    assert_eq!(r.unwrap_err(), CoreError::Policy(Error::Budget));
}

#[test]
fn independent_u128_oracle_covers_zero_exact_one_over_and_u64_overflow() {
    // The oracle uses independent wider arithmetic, never production add/fits.
    for limit in [0, 1, 2, 17, u64::MAX - 1, u64::MAX] {
        for bytes in [0, 1, 2, 17, u64::MAX - 1, u64::MAX] {
            let (_, a, e) = fixture(0);
            a.provision_task_resources(TaskId(1), cap(0, 2, limit))
                .unwrap();
            let mut total = 0u128;
            let mut rows = 0u128;
            for debit in [bytes, bytes, 0, 1] {
                let expected = total + u128::from(debit) <= u128::from(limit) && rows < 2;
                let actual = e.reserve_disclosure(debit, 10, 10);
                assert_eq!(
                    actual.is_ok(),
                    expected,
                    "limit={limit} debit={debit} total={total}"
                );
                if expected {
                    total += u128::from(debit);
                    rows += 1;
                }
                assert_eq!(used(&a), cap(0, rows as u64, total as u64));
            }
        }
    }
}

#[test]
fn unprovisioned_and_zero_read_fail_closed_without_changing_model() {
    let (_, a, e) = fixture(1);
    let original = a.snapshot().unwrap();
    budget(e.preview_current(&request(1, &[0]), 10, 10));
    budget(e.reserve_disclosure(0, 10, 10));
    assert_eq!(a.snapshot().unwrap(), original);
    a.provision_task_resources(TaskId(1), cap(0, 0, 0)).unwrap();
    budget(e.execute_current(request(1, &[0]), 10, 10, None));
    budget(e.status_current(OperationId(1), 10, 10));
    budget(e.reserve_disclosure(0, 10, 10));
    assert_eq!(used(&a), cap(0, 0, 0));
    // Original trusted local repair interfaces remain the P0-compatible model.
    e.execute(request(1, &[0]), Time::Certified(10), None)
        .unwrap()
        .unwrap();
}

#[test]
fn read_work_preview_rejection_dedupe_status_and_scope_are_cumulative() {
    let (_, a, e) = fixture(3);
    a.provision_task_resources(TaskId(1), cap(1000, 1000, 1000))
        .unwrap();
    e.preview_current(&predicate(1), 10, 10).unwrap();
    assert_eq!(used(&a).read_rows, 10); // two scans, selected-row visits, plus one
    let committed = e
        .execute_current(request(1, &[0, 1]), 10, 10, None)
        .unwrap()
        .unwrap();
    assert_eq!(used(&a).read_rows, 12);
    assert_eq!(
        e.execute_current(request(1, &[1, 0]), 10, 10, None)
            .unwrap()
            .unwrap(),
        committed
    );
    assert_eq!(used(&a).read_rows, 15); // cached effects + one
    assert_eq!(
        e.execute_current(request(1, &[2]), 10, 10, None).unwrap(),
        Err(Error::IdConflict)
    );
    assert_eq!(used(&a).read_rows, 18);
    e.status_current(OperationId(1), 10, 10).unwrap();
    e.status_current(OperationId(999), 10, 10).unwrap();
    assert_eq!(used(&a).read_rows, 22);
    let mut bad = request(2, &[2]);
    bad.selection = Selection::Keys(vec![Key {
        tenant: TenantId(43),
        row: RowId(2),
    }]);
    assert_eq!(
        e.execute_current(bad, 10, 10, None).unwrap(),
        Err(Error::Scope)
    );
    assert_eq!(used(&a).read_rows, 23);
    assert_eq!(a.snapshot().unwrap().tasks[&TaskId(1)].transitions, 2);
    assert_eq!(used(&a).disclosed_rows, 0);
}

#[test]
fn exact_read_budget_empty_results_and_one_over_are_atomic() {
    for (ids, limit) in [(vec![], 1), (vec![0], 1), (vec![0, 1], 2)] {
        let (_, a, e) = fixture(2);
        a.provision_task_resources(TaskId(1), cap(limit, 100, 1000))
            .unwrap();
        e.preview_current(&request(1, &ids), 10, 10).unwrap();
        assert_eq!(used(&a).read_rows, limit);
        let before = a.snapshot().unwrap();
        budget(e.preview_current(&request(2, &ids), 10, 10));
        assert_eq!(a.snapshot().unwrap(), before);
        assert_eq!(used(&a).read_rows, limit);
    }
    let (_, a, e) = fixture(1);
    a.provision_task_resources(TaskId(1), cap(0, 100, 1000))
        .unwrap();
    budget(e.preview_current(&request(1, &[]), 10, 10));
    assert_eq!(used(&a), TaskResources::default());
}

#[test]
fn selected_key_limit_and_usize_max_admission_never_wrap_or_spend() {
    let path = fresh("accounting-usize-bound");
    let a = Admin::create(
        path,
        Limits {
            admitted: usize::MAX,
            selected_keys: 1,
        },
    )
    .unwrap();
    let e = a.grant(grant()).unwrap();
    a.provision_task_resources(TaskId(1), cap(u64::MAX, u64::MAX, u64::MAX))
        .unwrap();
    assert_eq!(
        e.preview_current(&request(1, &[0, 1]), 10, 10),
        Err(CoreError::Limit)
    );
    assert_eq!(used(&a), TaskResources::default());
    // Full-width limits are stored/round-trip without usize casts.
    e.reserve_disclosure(u64::MAX, 10, 10).unwrap();
    budget(e.reserve_disclosure(1, 10, 10));
    assert_eq!(used(&a).encoded_bytes, u64::MAX);
}

#[test]
fn failed_approval_spends_reads_but_not_mutation_and_can_spend_disclosure() {
    let (_, a, e) = fixture(6);
    a.provision_task_resources(TaskId(1), cap(12, 1, 10))
        .unwrap();
    assert_eq!(
        e.execute_current(request(1, &[0, 1, 2, 3, 4, 5]), 10, 10, None)
            .unwrap(),
        Err(Error::ApprovalRequired)
    );
    assert_eq!(used(&a).read_rows, 6);
    assert_eq!(a.snapshot().unwrap().tasks[&TaskId(1)].transitions, 0);
    e.reserve_disclosure(10, 10, 10).unwrap();
    budget(e.reserve_disclosure(0, 10, 10));
    assert_eq!(used(&a), cap(6, 1, 10));
}

#[test]
fn committed_mutation_survives_withheld_disclosure_and_retry_never_refunds() {
    let (_, a, e) = fixture(1);
    a.provision_task_resources(TaskId(1), cap(20, 1, 3))
        .unwrap();
    let original = e
        .execute_current(request(1, &[0]), 10, 10, None)
        .unwrap()
        .unwrap();
    budget(e.reserve_disclosure(4, 10, 10));
    assert_eq!(
        e.status(OperationId(1)).unwrap(),
        OperationStatus::Decided(Ok(original.clone()))
    );
    e.reserve_disclosure(3, 10, 10).unwrap();
    assert_eq!(
        e.execute_current(request(1, &[0]), 10, 10, None)
            .unwrap()
            .unwrap(),
        original
    );
    budget(e.reserve_disclosure(3, 10, 10));
    assert_eq!(used(&a), cap(3, 1, 3));
    assert_eq!(a.snapshot().unwrap().tasks[&TaskId(1)].transitions, 1);
}

#[test]
fn concurrent_last_budget_has_one_winner_and_is_task_scoped() {
    let (_, a, e) = fixture(0);
    a.provision_task_resources(TaskId(1), cap(0, 1, 7)).unwrap();
    let mut g = grant();
    g.task = TaskId(2);
    let other = a.grant(g).unwrap();
    a.provision_task_resources(TaskId(2), cap(0, 1, 7)).unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(16));
    let jobs: Vec<_> = (0..16)
        .map(|_| {
            let e = e.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                b.wait();
                e.reserve_disclosure(7, 10, 10).is_ok()
            })
        })
        .collect();
    assert_eq!(
        jobs.into_iter()
            .filter(|j| j.thread().id() != std::thread::current().id())
            .map(|j| usize::from(j.join().unwrap()))
            .sum::<usize>(),
        1
    );
    other.reserve_disclosure(7, 10, 10).unwrap();
    assert_eq!(used(&a), cap(0, 1, 7));
}

#[test]
fn limits_are_immutable_and_restart_export_restore_preserve_identity_and_counters() {
    let (path, a, e) = fixture(1);
    let limits = cap(100, 3, 9);
    a.provision_task_resources(TaskId(1), limits).unwrap();
    e.reserve_disclosure(3, 10, 10).unwrap();
    assert_eq!(
        a.provision_task_resources(TaskId(1), cap(u64::MAX, u64::MAX, u64::MAX)),
        Err(CoreError::Policy(Error::TaskExists))
    );
    let file = fresh("accounting-export");
    a.export(&file).unwrap();
    let original = a.authority_metadata().unwrap().0;
    drop(e);
    drop(a);
    let a = Admin::open(path, Limits::default()).unwrap();
    assert_eq!(used(&a), cap(0, 1, 3));
    let restored = Admin::restore(&file, fresh("accounting-restored"), Limits::default()).unwrap();
    assert_ne!(original, restored.authority_metadata().unwrap().0);
    assert_eq!(
        restored.task_resources(TaskId(1)).unwrap(),
        a.task_resources(TaskId(1)).unwrap()
    );
    let e = restored.executor(TaskId(1), Principal(2)).unwrap();
    assert_eq!(
        e.reserve_disclosure(3, 10, 10),
        Err(CoreError::Policy(Error::Revoked))
    );
    assert_eq!(used(&restored), cap(0, 1, 3));
}

#[test]
fn revoke_expiry_uncertain_interval_and_time_regression_never_reserve() {
    for mode in 0..5 {
        let (_, a, e) = fixture(0);
        a.provision_task_resources(TaskId(1), cap(100, 100, 100))
            .unwrap();
        let (lo, hi) = match mode {
            0 => {
                a.revoke(TaskId(1)).unwrap();
                (10, 10)
            }
            1 => (999, 1000),
            2 => (0, 1),
            3 => (11, 10),
            _ => {
                a.certify_server_time(20, 20).unwrap();
                (10, 10)
            }
        };
        assert!(e.reserve_disclosure(1, lo, hi).is_err());
        assert_eq!(used(&a), TaskResources::default());
    }
}

#[test]
fn cancellation_before_reservation_is_empty_but_after_decision_cannot_refund() {
    for point in [
        DecisionPoint::BeforeDecision,
        DecisionPoint::Decided,
        DecisionPoint::Committed,
    ] {
        let (_, a, e) = fixture(0);
        a.provision_task_resources(TaskId(1), cap(0, 1, 7)).unwrap();
        let c = Cancellation::default();
        let cancel = c.clone();
        a.set_decision_hook(Some(std::sync::Arc::new(move |p| {
            if p == point {
                cancel.cancel();
            }
        })));
        let r = c.scope(|| e.reserve_disclosure(7, 10, 10));
        a.set_decision_hook(None);
        assert_eq!(r.is_ok(), point != DecisionPoint::BeforeDecision);
        assert_eq!(used(&a).disclosed_rows, u64::from(r.is_ok()));
    }
}

#[test]
fn ambiguous_reservation_persistence_fences_and_never_returns_success() {
    for point in [CrashPoint::ErrorBeforeCommit, CrashPoint::ErrorAfterCommit] {
        let (path, a, e) = fixture(0);
        a.provision_task_resources(TaskId(1), cap(0, 1, 7)).unwrap();
        a.set_crash_point(point).unwrap();
        assert!(matches!(
            e.reserve_disclosure(7, 10, 10),
            Err(CoreError::Storage(_))
        ));
        assert_eq!(a.task_resources(TaskId(1)), Err(CoreError::Fenced));
        drop(e);
        drop(a);
        let a = Admin::open(path, Limits::default()).unwrap();
        assert_eq!(
            used(&a).disclosed_rows,
            u64::from(point == CrashPoint::ErrorAfterCommit)
        );
    }
}

#[test]
fn accounting_subprocess_worker() {
    let Ok(path) = std::env::var("GD_ACCOUNTING_CHILD") else {
        return;
    };
    let a = Admin::open(&path, Limits::default()).unwrap();
    let e = a.executor(TaskId(1), Principal(2)).unwrap();
    let mode = std::env::var("GD_ACCOUNTING_POINT").unwrap();
    a.set_crash_point(match mode.as_str() {
        "before" => CrashPoint::BeforeCommit,
        "after" => CrashPoint::AfterCommit,
        "ack" => CrashPoint::BeforeAck,
        _ => CrashPoint::None,
    })
    .unwrap();
    e.reserve_disclosure(7, 10, 10).unwrap();
    std::fs::write(
        std::path::Path::new(&path).join("protected-output.txt"),
        b"released",
    )
    .unwrap();
}

#[test]
fn real_process_loss_around_reservation_never_publishes_before_durable_commit() {
    for mode in ["before", "after", "ack", "success"] {
        let (path, a, e) = fixture(0);
        a.provision_task_resources(TaskId(1), cap(0, 1, 7)).unwrap();
        drop(e);
        drop(a);
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "accounting_subprocess_worker", "--nocapture"])
            .env("GD_ACCOUNTING_CHILD", &path)
            .env("GD_ACCOUNTING_POINT", mode)
            .output()
            .unwrap();
        std::fs::write(path.join("child.stdout.txt"), &out.stdout).unwrap();
        std::fs::write(path.join("child.stderr.txt"), &out.stderr).unwrap();
        std::fs::write(
            path.join("child.status.txt"),
            format!("{mode}: {}", out.status),
        )
        .unwrap();
        assert_eq!(
            out.status.code(),
            Some(if mode == "success" { 0 } else { 86 })
        );
        let a = Admin::open(&path, Limits::default()).unwrap();
        assert_eq!(used(&a).disclosed_rows, u64::from(mode != "before"));
        assert_eq!(
            path.join("protected-output.txt").exists(),
            mode == "success"
        );
    }
}
