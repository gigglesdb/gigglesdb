mod common;
use common::*;
use giggles_core::*;
use std::{
    sync::{Arc, Mutex, mpsc},
    time::{Duration, Instant},
};

const WAIT: Duration = Duration::from_secs(10);
fn pause(a: &Admin, at: DecisionPoint) -> (mpsc::Receiver<Cancellation>, mpsc::Sender<()>) {
    let (tx, rx) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let wait = Mutex::new(wait);
    a.set_decision_hook(Some(Arc::new(move |point| {
        if point == at {
            tx.send(Cancellation::current().expect("scoped request"))
                .unwrap();
            wait.lock().unwrap().recv_timeout(WAIT).unwrap();
        }
    })));
    (rx, release)
}
fn app() -> Admin {
    let a = Admin::create(fresh("cancel-sql"), Limits::default()).unwrap();
    a.sql_session()
        .execute("CREATE TABLE app (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    a
}
fn count(a: &Admin) -> usize {
    a.sql_session()
        .execute("SELECT id FROM app LIMIT 10", &[], None)
        .unwrap()
        .rows
        .len()
}

#[test]
fn sql_autocommit_before_decision_cancel_and_after_decision_too_late() {
    for point in [
        DecisionPoint::BeforeDecision,
        DecisionPoint::Decided,
        DecisionPoint::Committed,
    ] {
        let a = app();
        let (entered, release) = pause(&a, point);
        let worker = a.clone();
        let job = std::thread::spawn(move || {
            Cancellation::default().scope(|| {
                worker
                    .sql_session()
                    .execute("INSERT INTO app VALUES (1)", &[], None)
            })
        });
        let context = entered.recv_timeout(WAIT).unwrap();
        let wins = point == DecisionPoint::BeforeDecision;
        assert_eq!(context.cancel(), wins);
        release.send(()).unwrap();
        let result = job.join().unwrap();
        a.set_decision_hook(None);
        if wins {
            assert_eq!(result, Err(CoreError::Cancelled));
        } else {
            result.unwrap();
        }
        assert_eq!(count(&a), usize::from(!wins));
        // Cancellation must not fence storage or leak onto the reused thread/session.
        a.sql_session()
            .execute("INSERT INTO app VALUES (2)", &[], None)
            .unwrap();
    }
}

#[test]
fn explicit_commit_cancel_poison_rollback_and_success_after_decision() {
    for point in [
        DecisionPoint::BeforeDecision,
        DecisionPoint::Decided,
        DecisionPoint::Committed,
    ] {
        let a = app();
        let mut s = a.sql_session();
        s.execute("BEGIN", &[], None).unwrap();
        s.execute("INSERT INTO app VALUES (1)", &[], None).unwrap();
        let (entered, release) = pause(&a, point);
        let job = std::thread::spawn(move || {
            let result = Cancellation::default().scope(|| s.execute("COMMIT", &[], None));
            (s, result)
        });
        let c = entered.recv_timeout(WAIT).unwrap();
        let wins = point == DecisionPoint::BeforeDecision;
        assert_eq!(c.cancel(), wins);
        release.send(()).unwrap();
        let (mut s, result) = job.join().unwrap();
        a.set_decision_hook(None);
        if wins {
            assert_eq!(result, Err(CoreError::Cancelled));
            assert!(s.is_aborted());
            assert_eq!(
                s.execute("COMMIT", &[], None),
                Err(CoreError::Sql(relational::SqlError::Aborted))
            );
            s.execute("ROLLBACK", &[], None).unwrap();
        } else {
            result.unwrap();
            assert!(!s.in_transaction());
        }
        assert_eq!(count(&a), usize::from(!wins));
        s.execute("BEGIN", &[], None).unwrap();
        s.execute("ROLLBACK", &[], None).unwrap();
    }
}

#[test]
fn cancelled_staging_aborts_without_publishing() {
    let a = app();
    let mut s = a.sql_session();
    s.execute("BEGIN", &[], None).unwrap();
    let c = Cancellation::default();
    assert!(c.cancel());
    assert_eq!(
        c.scope(|| s.execute("INSERT INTO app VALUES (1)", &[], None)),
        Err(CoreError::Cancelled)
    );
    assert!(s.is_aborted());
    s.execute("ROLLBACK", &[], None).unwrap();
    assert_eq!(count(&a), 0);
}

#[test]
fn cancelled_core_lock_wait_returns_while_holder_still_owns_gate() {
    let a = app();
    let (entered, release) = pause(&a, DecisionPoint::BeforeDecision);
    let holder = a.clone();
    let job = std::thread::spawn(move || {
        Cancellation::default().scope(|| {
            holder
                .sql_session()
                .execute("INSERT INTO app VALUES (1)", &[], None)
        })
    });
    let holder_context = entered.recv_timeout(WAIT).unwrap();
    let waiter = a.clone();
    let c = Cancellation::default();
    let wc = c.clone();
    let (started, start) = mpsc::channel();
    let (done, result) = mpsc::channel();
    let wait_job = std::thread::spawn(move || {
        wc.scope(|| {
            started.send(()).unwrap();
            done.send(
                waiter
                    .sql_session()
                    .execute("INSERT INTO app VALUES (2)", &[], None),
            )
            .unwrap();
        })
    });
    start.recv_timeout(WAIT).unwrap();
    assert!(c.cancel());
    assert_eq!(
        result.recv_timeout(WAIT).unwrap(),
        Err(CoreError::Cancelled)
    );
    wait_job.join().unwrap();
    assert!(holder_context.cancel());
    release.send(()).unwrap();
    assert_eq!(job.join().unwrap(), Err(CoreError::Cancelled));
    a.set_decision_hook(None);
    assert_eq!(count(&a), 0);
}







#[test]
fn expired_deadline_before_admission_and_scope_restored_after_panic() {
    let a = app();
    let c = Cancellation::new(Some(Instant::now() - Duration::from_secs(1)));
    assert_eq!(
        c.scope(|| a
            .sql_session()
            .execute("INSERT INTO app VALUES (1)", &[], None)),
        Err(CoreError::Cancelled)
    );
    assert!(std::panic::catch_unwind(|| c.scope(|| panic!("scope restoration test"))).is_err());
    assert!(Cancellation::current().is_none());
    a.sql_session()
        .execute("INSERT INTO app VALUES (2)", &[], None)
        .unwrap();
    assert_eq!(count(&a), 1);
}
