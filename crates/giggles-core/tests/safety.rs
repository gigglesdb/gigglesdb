mod common;
use common::*;
use giggles_core::*;
use std::sync::{Arc, Barrier};
fn run(e: &Executor, r: Request) -> Decision {
    e.execute(r, Time::Certified(10), None).unwrap()
}
#[test]
fn durable_reopen_and_stable_retry() {
    let (p, a, e) = fixture(3);
    let r = request(1, &[0, 1]);
    let result = run(&e, r.clone());
    assert!(result.is_ok());
    let before = a.snapshot().unwrap();
    drop(e);
    drop(a);
    let a = Admin::open(p, Limits::default()).unwrap();
    assert_eq!(a.snapshot().unwrap(), before);
    let e = a.executor(TaskId(1), Principal(2)).unwrap();
    assert_eq!(e.execute(r, Time::Uncertain, None).unwrap(), result);
    assert_eq!(run(&e, request(1, &[2])), Err(Error::IdConflict));
    assert_eq!(
        e.status(OperationId(1)).unwrap(),
        OperationStatus::Decided(result)
    );
    assert_eq!(before.tasks[&TaskId(1)].transitions, 2);
}
#[test]
fn ten_thread_last_slot_race() {
    let p = fresh("race");
    let a = Admin::create(p, Limits::default()).unwrap();
    for n in 0..10 {
        a.put(key(n), row()).unwrap();
    }
    let mut g = grant();
    g.policy.distinct_limit = 1;
    g.policy.transition_limit = 1;
    let e = a.grant(g).unwrap();
    let barrier = Arc::new(Barrier::new(10));
    let handles: Vec<_> = (0..10)
        .map(|n| {
            let e = e.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                b.wait();
                run(&e, request(n, &[n]))
            })
        })
        .collect();
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results.iter().filter(|r| **r == Err(Error::Budget)).count(),
        9
    );
    let s = a.snapshot().unwrap();
    assert_eq!(s.tasks[&TaskId(1)].transitions, 1);
    assert_eq!(s.operations.len(), 10);
}
#[test]
fn concurrent_identical_retry_only_charges_once() {
    let (_, a, e) = fixture(1);
    let barrier = Arc::new(Barrier::new(10));
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let e = e.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                b.wait();
                run(&e, request(7, &[0]))
            })
        })
        .collect();
    let out: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert!(out.iter().all(|d| d == &out[0]));
    let s = a.snapshot().unwrap();
    assert_eq!(s.operations.len(), 1);
    assert_eq!(s.tasks[&TaskId(1)].transitions, 1);
}
#[test]
fn failed_multirow_is_atomic_and_bound() {
    let (_, a, e) = fixture(2);
    let mut v = row();
    v.failed_at = 99;
    a.put(key(1), v).unwrap();
    let rows = a.snapshot().unwrap().rows;
    assert_eq!(run(&e, request(1, &[0, 1])), Err(Error::Scope));
    assert_eq!(a.snapshot().unwrap().rows, rows);
    a.put(key(1), row()).unwrap();
    assert_eq!(run(&e, request(1, &[0, 1])), Err(Error::Scope));
    assert!(run(&e, request(2, &[0, 1])).is_ok());
}
#[test]
fn denial_binds_missing_approval() {
    let (_, a, e) = fixture(6);
    let r = predicate(1);
    assert_eq!(run(&e, r.clone()), Err(Error::ApprovalRequired));
    let p = e.preview(&r, Time::Certified(10)).unwrap();
    let id = a.approve(Principal(3), p, Time::Certified(10)).unwrap();
    assert_eq!(
        e.execute(r, Time::Certified(10), Some(id)).unwrap(),
        Err(Error::ApprovalRequired)
    );
    assert!(!a.snapshot().unwrap().approvals[&id].consumed);
}
#[test]
fn exact_approval_consumption_and_retry() {
    let (_, a, e) = fixture(6);
    let r = predicate(1);
    let p = e.preview(&r, Time::Certified(10)).unwrap();
    let id = a.approve(Principal(3), p, Time::Certified(10)).unwrap();
    let d = e.execute(r.clone(), Time::Certified(10), Some(id)).unwrap();
    assert!(d.is_ok());
    assert!(a.snapshot().unwrap().approvals[&id].consumed);
    assert_eq!(e.execute(r, Time::Uncertain, None).unwrap(), d);
}
#[test]
fn predicate_phantom_rejects() {
    let (_, a, e) = fixture(2);
    let r = predicate(1);
    let p = e.preview(&r, Time::Certified(10)).unwrap();
    let id = a.approve(Principal(3), p, Time::Certified(10)).unwrap();
    a.put(key(9), row()).unwrap();
    assert_eq!(
        e.execute(r, Time::Certified(10), Some(id)).unwrap(),
        Err(Error::StalePreview)
    );
    assert_eq!(a.snapshot().unwrap().tasks[&TaskId(1)].transitions, 0);
}
#[test]
fn selected_aba_invalidates_but_unrelated_write_does_not() {
    for selected in [true, false] {
        let (_, a, e) = fixture(2);
        let r = request(1, &[0]);
        let p = e.preview(&r, Time::Certified(10)).unwrap();
        let id = a.approve(Principal(3), p, Time::Certified(10)).unwrap();
        a.put(key(if selected { 0 } else { 1 }), row()).unwrap();
        let d = e.execute(r, Time::Certified(10), Some(id)).unwrap();
        if selected {
            assert_eq!(d, Err(Error::StalePreview));
        } else {
            assert!(d.is_ok());
        }
    }
}
#[test]
fn schema_revocation_expiry_fences() {
    for mode in 0..4 {
        let (_, a, e) = fixture(1);
        let r = request(1, &[0]);
        let p = e.preview(&r, Time::Certified(10)).unwrap();
        let id = a.approve(Principal(3), p, Time::Certified(10)).unwrap();
        let time = if mode == 3 {
            Time::Certified(1000)
        } else {
            Time::Certified(10)
        };
        match mode {
            0 => a.bump_schema().unwrap(),
            1 => a.revoke(TaskId(1)).unwrap(),
            2 => a.revoke_approval(id).unwrap(),
            _ => {}
        }
        let d = e.execute(r, time, Some(id)).unwrap();
        assert_eq!(
            d,
            Err([
                Error::StalePreview,
                Error::Revoked,
                Error::InvalidApproval,
                Error::Expired
            ][mode])
        );
        assert_eq!(a.snapshot().unwrap().tasks[&TaskId(1)].transitions, 0);
    }
}
#[test]
fn approval_sequence_and_time_survive_reopen() {
    let (path, a, e) = fixture(1);
    let r = request(1, &[0]);
    let p = e.preview(&r, Time::Certified(50)).unwrap();
    let first = a.approve(Principal(3), p, Time::Certified(50)).unwrap();
    drop(e);
    drop(a);
    let a = Admin::open(path, Limits::default()).unwrap();
    let e = a.executor(TaskId(1), Principal(2)).unwrap();
    assert_eq!(
        e.preview(&r, Time::Certified(49)),
        Err(CoreError::Policy(Error::TimeRegression))
    );
    let p = e.preview(&r, Time::Certified(50)).unwrap();
    let second = a.approve(Principal(3), p, Time::Certified(50)).unwrap();
    assert_eq!(second.0, first.0 + 1);
    assert_eq!(a.authority_metadata().unwrap().1, 50);
}
#[test]
fn repeated_transitions_and_distinct_cumulative() {
    let p = fresh("cumulative");
    let a = Admin::create(p, Limits::default()).unwrap();
    a.put(key(0), row()).unwrap();
    a.put(key(1), row()).unwrap();
    let mut g = grant();
    g.policy.distinct_limit = 1;
    g.policy.transition_limit = 2;
    let e = a.grant(g).unwrap();
    assert!(run(&e, request(1, &[0])).is_ok());
    a.put(key(0), row()).unwrap();
    assert!(run(&e, request(2, &[0])).is_ok());
    a.put(key(0), row()).unwrap();
    assert_eq!(run(&e, request(3, &[0])), Err(Error::Budget));
    assert_eq!(run(&e, request(4, &[1])), Err(Error::Budget));
    let t = &a.snapshot().unwrap().tasks[&TaskId(1)];
    assert_eq!(t.transitions, 2);
    assert_eq!(t.touched.len(), 1);
}
#[test]
fn io_error_fences_and_reopen_resolves_both_sides() {
    for (point, committed) in [
        (CrashPoint::ErrorBeforeCommit, false),
        (CrashPoint::ErrorAfterCommit, true),
    ] {
        let (path, a, e) = fixture(1);
        a.set_crash_point(point).unwrap();
        assert!(matches!(
            e.execute(request(1, &[0]), Time::Certified(10), None),
            Err(CoreError::Storage(_))
        ));
        assert_eq!(e.status(OperationId(1)), Err(CoreError::Fenced));
        assert_eq!(a.put(key(2), row()), Err(CoreError::Fenced));
        drop(e);
        drop(a);
        let a = Admin::open(path, Limits::default()).unwrap();
        let e = a.executor(TaskId(1), Principal(2)).unwrap();
        let status = e.status(OperationId(1)).unwrap();
        assert_eq!(matches!(status, OperationStatus::Decided(Ok(_))), committed);
        assert_eq!(
            a.snapshot().unwrap().rows[&key(0)].status,
            if committed {
                Status::Queued
            } else {
                Status::Failed
            }
        );
    }
}
#[test]
fn reject_existing_missing_corrupt_and_second_open() {
    let (path, a, _e) = fixture(0);
    assert!(Admin::create(&path, Limits::default()).is_err());
    assert!(Admin::open(&path, Limits::default()).is_err());
    assert!(Admin::open(fresh("absent"), Limits::default()).is_err());
    let bad = fresh("bad");
    std::fs::create_dir(&bad).unwrap();
    std::fs::write(bad.join("data.redb"), b"corrupt").unwrap();
    assert!(Admin::open(&bad, Limits::default()).is_err());
    assert_eq!(std::fs::read(bad.join("data.redb")).unwrap(), b"corrupt");
    drop(a);
}
#[test]
fn trusted_rebinding_and_immutable_tasks() {
    let (_, a, _e) = fixture(0);
    assert!(matches!(
        a.executor(TaskId(1), Principal(9)),
        Err(CoreError::Policy(Error::Authentication))
    ));
    a.revoke(TaskId(1)).unwrap();
    assert!(matches!(
        a.grant(grant()),
        Err(CoreError::Policy(Error::TaskExists))
    ));
}
#[test]
fn bounded_input_and_invalid_grant() {
    let (_, a, e) = fixture(0);
    let ids: Vec<_> = (0..1025).collect();
    assert_eq!(
        e.execute(request(1, &ids), Time::Certified(10), None),
        Err(CoreError::Limit)
    );
    assert_eq!(e.status(OperationId(1)).unwrap(), OperationStatus::Absent);
    let mut g = grant();
    g.task = TaskId(2);
    g.executor = g.issuer;
    assert!(matches!(
        a.grant(g),
        Err(CoreError::Policy(Error::InvalidGrant))
    ));
}
#[test]
fn altered_payload_race_binds_one_payload() {
    let (_, a, e) = fixture(10);
    let barrier = Arc::new(Barrier::new(10));
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let e = e.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                b.wait();
                run(&e, request(1, &[i]))
            })
        })
        .collect();
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|d| d.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|d| **d == Err(Error::IdConflict))
            .count(),
        9
    );
    assert_eq!(a.snapshot().unwrap().operations.len(), 1);
}
#[test]
fn predicate_limit_does_not_bind_but_existing_retry_stays_stable() {
    let p = fresh("bounded-predicate");
    let a = Admin::create(
        &p,
        Limits {
            admitted: 4,
            selected_keys: 1,
        },
    )
    .unwrap();
    a.put(key(0), row()).unwrap();
    let e = a.grant(grant()).unwrap();
    let r = predicate(1);
    let d = e.execute(r.clone(), Time::Certified(10), None).unwrap();
    assert!(d.is_ok());
    a.put(key(1), row()).unwrap();
    a.put(key(2), row()).unwrap();
    assert_eq!(e.execute(r, Time::Certified(10), None).unwrap(), d);
    assert_eq!(
        e.execute(predicate(2), Time::Certified(10), None),
        Err(CoreError::Limit)
    );
    assert_eq!(e.status(OperationId(2)).unwrap(), OperationStatus::Absent);
}
#[test]
fn process_lock_follows_hardlink_alias() {
    let (p, _a, _e) = fixture(0);
    let alias = fresh("hardlink-alias");
    std::fs::create_dir(&alias).unwrap();
    std::fs::hard_link(p.join("data.redb"), alias.join("data.redb")).unwrap();
    assert!(Admin::open(alias, Limits::default()).is_err());
}
#[test]
fn corrupt_engine_header_is_rejected_without_replacement() {
    use std::io::{Seek, Write};
    let (p, a, e) = fixture(1);
    drop(e);
    drop(a);
    let dest = fresh("corrupt-engine");
    std::fs::create_dir(&dest).unwrap();
    std::fs::copy(p.join("data.redb"), dest.join("data.redb")).unwrap();
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .open(dest.join("data.redb"))
        .unwrap();
    f.seek(std::io::SeekFrom::Start(0)).unwrap();
    f.write_all(&[0xff; 512]).unwrap();
    f.sync_all().unwrap();
    drop(f);
    assert!(Admin::open(&dest, Limits::default()).is_err());
    assert_eq!(
        &std::fs::read(dest.join("data.redb")).unwrap()[..512],
        &[0xff; 512]
    );
    assert!(Admin::open(p, Limits::default()).is_ok());
}
#[test]
fn unsupported_columns_duplicates_missing_and_bad_predicates_are_atomic() {
    let (_, a, e) = fixture(2);
    let rows = a.snapshot().unwrap().rows;
    let mut bad = request(1, &[0]);
    bad.column = Column::AttemptCount;
    assert_eq!(run(&e, bad), Err(Error::Scope));
    assert_eq!(run(&e, request(2, &[0, 0])), Err(Error::DuplicateKey));
    assert_eq!(run(&e, request(3, &[0, 99])), Err(Error::MissingRow));
    let mut bad = predicate(4);
    bad.selection = Selection::Predicate {
        tenant: TenantId(42),
        status: Status::Failed,
        from: 107,
        until: 100,
    };
    assert_eq!(run(&e, bad), Err(Error::InvalidPredicate));
    assert_eq!(a.snapshot().unwrap().rows, rows);
    assert_eq!(a.snapshot().unwrap().tasks[&TaskId(1)].transitions, 0);
}
#[test]
fn noops_charge_zero_but_still_check_scope_and_versions() {
    let (_, a, e) = fixture(1);
    let r = request(1, &[0]);
    assert!(run(&e, r).is_ok());
    let d = run(&e, request(2, &[0])).unwrap();
    assert_eq!(d.charged_transitions, 0);
    assert_eq!(d.lifetime_transitions, 1);
    let mut outside = row();
    outside.status = Status::Queued;
    outside.failed_at = 99;
    a.put(key(0), outside).unwrap();
    assert_eq!(run(&e, request(3, &[0])), Err(Error::Scope));
}
#[test]
fn unknown_wrong_operation_and_forged_previews_never_authorize() {
    let (_, a, e) = fixture(1);
    let r = request(1, &[0]);
    let p = e.preview(&r, Time::Certified(10)).unwrap();
    let mut forged = p.clone();
    forged.dependencies.get_mut(&key(0)).unwrap().version = 44;
    assert_eq!(
        a.approve(Principal(3), forged, Time::Certified(10)),
        Err(CoreError::Policy(Error::StalePreview))
    );
    assert_eq!(
        a.approve(Principal(2), p.clone(), Time::Certified(10)),
        Err(CoreError::Policy(Error::InvalidApproval))
    );
    let id = a.approve(Principal(3), p, Time::Certified(10)).unwrap();
    assert_eq!(
        e.execute(request(2, &[0]), Time::Certified(10), Some(id))
            .unwrap(),
        Err(Error::InvalidApproval)
    );
    assert_eq!(
        e.execute(request(3, &[0]), Time::Certified(10), Some(ApprovalId(99)))
            .unwrap(),
        Err(Error::InvalidApproval)
    );
    assert_eq!(a.snapshot().unwrap().tasks[&TaskId(1)].transitions, 0);
}
#[test]
fn status_retry_survive_expiry_and_revocation_while_new_work_denies() {
    let (_, a, e) = fixture(2);
    let r = request(1, &[0]);
    let d = run(&e, r.clone());
    a.revoke(TaskId(1)).unwrap();
    assert_eq!(
        e.status(OperationId(1)).unwrap(),
        OperationStatus::Decided(d.clone())
    );
    assert_eq!(e.execute(r, Time::Certified(1001), None).unwrap(), d);
    assert_eq!(run(&e, request(2, &[1])), Err(Error::Revoked));
}
#[test]
fn uncertain_and_regressing_time_are_durable_terminal_bindings() {
    let (path, a, e) = fixture(1);
    assert_eq!(
        e.execute(request(1, &[0]), Time::Uncertain, None).unwrap(),
        Err(Error::UncertainTime)
    );
    e.preview(&request(2, &[0]), Time::Certified(50)).unwrap();
    assert_eq!(run(&e, request(2, &[0])), Err(Error::TimeRegression));
    drop(e);
    drop(a);
    let a = Admin::open(path, Limits::default()).unwrap();
    let e = a.executor(TaskId(1), Principal(2)).unwrap();
    assert_eq!(
        e.execute(request(1, &[0]), Time::Certified(50), None)
            .unwrap(),
        Err(Error::UncertainTime)
    );
    assert_eq!(
        e.execute(request(2, &[0]), Time::Certified(50), None)
            .unwrap(),
        Err(Error::TimeRegression)
    );
    assert_eq!(a.authority_metadata().unwrap().1, 50);
}
