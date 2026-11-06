// Shared retained-certificate fixtures and baseline helpers, without duplicating tests.
// This file is included as a module by network.rs.
use super::*;
use giggles_core::{Cancellation, DecisionPoint};
use std::{
    sync::{Mutex, mpsc},
    time::{Duration, Instant},
};

const WAIT: Duration = Duration::from_secs(8);
// Leave scheduling room before each mandatory hook; expiry is still exercised
// by holding the real request at that hook for its original wire budget.
const WIRE_DEADLINE_MS: u64 = 2_000;
fn pause(
    a: &Admin,
    at: DecisionPoint,
) -> (
    tokio::sync::oneshot::Receiver<Cancellation>,
    mpsc::Sender<()>,
) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = Mutex::new(Some(tx));
    let (release, wait) = mpsc::channel();
    let wait = Mutex::new(wait);
    a.set_decision_hook(Some(Arc::new(move |point| {
        if point == at
            && let Some(tx) = tx.lock().unwrap().take()
        {
            let _ = tx.send(Cancellation::current().expect("request context"));
            wait.lock().unwrap().recv_timeout(WAIT).unwrap();
        }
    })));
    (rx, release)
}
async fn entered(rx: tokio::sync::oneshot::Receiver<Cancellation>) -> Cancellation {
    tokio::time::timeout(WAIT, rx).await.unwrap().unwrap()
}
async fn cancelled(c: &Cancellation) {
    tokio::time::timeout(WAIT, async {
        while c.check().is_ok() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}
async fn raw_key(f: &Fixture) -> (tokio_rustls::client::TlsStream<TcpStream>, (u32, u32)) {
    let mut s = raw_tls(f, f.app.clone(), true).await;
    let mut b = 196608u32.to_be_bytes().to_vec();
    b.extend(b"user\0giggles\0database\0giggles\0\0");
    s.write_u32((b.len() + 4) as u32).await.unwrap();
    s.write_all(&b).await.unwrap();
    let mut key = None;
    loop {
        let (tag, b) = pg_read(&mut s).await;
        if tag == b'K' {
            assert_eq!(b.len(), 8);
            key = Some((
                u32::from_be_bytes(b[..4].try_into().unwrap()),
                u32::from_be_bytes(b[4..].try_into().unwrap()),
            ));
        }
        if tag == b'Z' {
            return (s, key.unwrap());
        }
    }
}
async fn raw_cancel(f: &Fixture, config: Arc<ClientConfig>, key: (u32, u32)) {
    let mut s = raw_tls(f, config, true).await;
    s.write_u32(16).await.unwrap();
    s.write_u32(80877102).await.unwrap();
    s.write_u32(key.0).await.unwrap();
    s.write_u32(key.1).await.unwrap();
    s.flush().await.unwrap();
    // CancelRequest has no response; EOF proves the server handled this connection.
    assert!(
        tokio::time::timeout(WAIT, s.read_u8())
            .await
            .unwrap()
            .is_err()
    );
}
async fn query_done(s: &mut tokio_rustls::client::TlsStream<TcpStream>) -> Vec<(u8, Vec<u8>)> {
    let mut out = vec![];
    loop {
        let frame = pg_read(s).await;
        let end = frame.0 == b'Z';
        out.push(frame);
        if end {
            return out;
        }
    }
}
fn has_error(frames: &[(u8, Vec<u8>)], code: &str) -> bool {
    frames
        .iter()
        .any(|(tag, b)| *tag == b'E' && b.windows(code.len()).any(|v| v == code.as_bytes()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pinned_driver_cancel_autocommit_and_commit_at_each_decision_boundary() {
    for explicit in [false, true] {
        for point in [
            DecisionPoint::BeforeDecision,
            DecisionPoint::Decided,
            DecisionPoint::Committed,
        ] {
            let f = Fixture::new().await;
            let (pg, connection) = f.pg(f.app.clone()).await;
            if explicit {
                pg.batch_execute("BEGIN").await.unwrap();
                pg.execute("INSERT INTO app (id) VALUES (1)", &[])
                    .await
                    .unwrap();
            }
            let token = pg.cancel_token();
            let (rx, release) = pause(&f.admin, point);
            let job = tokio::spawn(async move {
                let result = if explicit {
                    pg.batch_execute("COMMIT").await
                } else {
                    pg.execute("INSERT INTO app (id) VALUES (1)", &[])
                        .await
                        .map(|_| ())
                };
                (pg, result)
            });
            let context = entered(rx).await;
            token
                .cancel_query(tokio_postgres_rustls::MakeRustlsConnect::new(
                    (*f.app).clone(),
                ))
                .await
                .unwrap();
            let wins = point == DecisionPoint::BeforeDecision;
            if wins {
                cancelled(&context).await;
            } else {
                assert!(!context.cancel());
            }
            release.send(()).unwrap();
            let (pg, result) = tokio::time::timeout(WAIT, job).await.unwrap().unwrap();
            f.admin.set_decision_hook(None);
            if wins {
                assert_eq!(result.unwrap_err().code().unwrap().code(), "57014");
                if explicit {
                    pg.batch_execute("COMMIT").await.unwrap();
                    pg.batch_execute("ROLLBACK").await.unwrap();
                }
            } else {
                result.unwrap();
            }
            assert_eq!(
                pg.query("SELECT id FROM app WHERE id=1", &[])
                    .await
                    .unwrap()
                    .len(),
                usize::from(!wins)
            );
            // Subsequent query gets a new context even on the same worker/session.
            pg.execute("INSERT INTO app (id) VALUES (2)", &[])
                .await
                .unwrap();
            drop(pg);
            connection.await.unwrap();
            f.stop().await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authenticated_wrong_stale_cross_role_and_idle_keys_cannot_cancel_next_request() {
    let f = Fixture::new().await;
    let (mut stale, old) = raw_key(&f).await;
    pg_send(&mut stale, b'X', &[]).await;
    drop(stale);
    let (mut target, key) = raw_key(&f).await;
    assert_ne!(key.0, old.0);
    raw_cancel(&f, f.app.clone(), key).await; // idle, must not queue cancellation
    let (rx, release) = pause(&f.admin, DecisionPoint::BeforeDecision);
    pg_send(&mut target, b'Q', b"INSERT INTO app (id) VALUES (1)\0").await;
    let c = entered(rx).await;
    for (config, k) in [
        (f.app.clone(), (key.0, key.1 ^ 1)),
        (f.app.clone(), (key.0 ^ 1, key.1)),
        (f.other.clone(), key),
        (f.task.clone(), key),
        (f.unbound.clone(), key),
        (f.app.clone(), old),
    ] {
        raw_cancel(&f, config, k).await;
        assert!(c.check().is_ok());
    }
    raw_cancel(&f, f.app.clone(), key).await;
    assert_eq!(c.check(), Err(giggles_core::CoreError::Cancelled));
    release.send(()).unwrap();
    assert!(has_error(&query_done(&mut target).await, "57014"));
    f.admin.set_decision_hook(None);
    pg_send(&mut target, b'Q', b"INSERT INTO app (id) VALUES (2)\0").await;
    assert!(
        !query_done(&mut target)
            .await
            .iter()
            .any(|(t, _)| *t == b'E')
    );
    drop(target);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_native_deadline_predecision_and_postdecision_preserve_outcomes() {
    for point in [
        DecisionPoint::BeforeDecision,
        DecisionPoint::Decided,
        DecisionPoint::Committed,
    ] {
        let f = Fixture::new().await;
        let mut client = f.native(f.app.clone()).await;
        let (rx, release) = pause(&f.admin, point);
        let mut job = tokio::spawn(async move {
            let r = client
                .call(&Request::Deadline {
                    budget_ms: WIRE_DEADLINE_MS,
                    request: Box::new(sql("INSERT INTO app (id) VALUES (1)", vec![])),
                })
                .await
                .unwrap();
            (client, r)
        });
        let c = tokio::select! {
            c = entered(rx) => c,
            completed = &mut job => {
                let (_, response) = completed.unwrap();
                panic!("native deadline completed before required {point:?} hook: {response:?}");
            }
        };
        // Every boundary is mandatory: an early expiry is a diagnostic failure.
        // Hold the real storage decision until the original wire deadline expires.
        if point == DecisionPoint::BeforeDecision {
            cancelled(&c).await;
        } else {
            let held = Instant::now();
            let budget = Duration::from_millis(WIRE_DEADLINE_MS);
            tokio::time::sleep(budget).await;
            // Receipt preceded hook entry, so this independently proves the
            // original wire deadline elapsed even after decision won the race.
            assert!(held.elapsed() >= budget);
            assert!(c.check().is_ok());
            assert!(!c.cancel());
        }
        release.send(()).unwrap();
        let (mut client, result) = job.await.unwrap();
        f.admin.set_decision_hook(None);
        assert_eq!(
            result.code,
            if point == DecisionPoint::BeforeDecision {
                "57014"
            } else {
                "ok"
            }
        );
        let r = client
            .call(&sql("SELECT id FROM app WHERE id=1", vec![]))
            .await
            .unwrap();
        assert_eq!(
            r.sql.unwrap().rows.len(),
            usize::from(point != DecisionPoint::BeforeDecision)
        );
        assert_eq!(
            client
                .call(&sql("INSERT INTO app (id) VALUES (2)", vec![]))
                .await
                .unwrap()
                .code,
            "ok"
        );
        drop(client);
        f.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn socket_disconnect_does_not_cancel_an_admitted_write() {
    let f = Fixture::new().await;
    let (mut target, _) = raw_key(&f).await;
    let (rx, release) = pause(&f.admin, DecisionPoint::BeforeDecision);
    pg_send(&mut target, b'Q', b"INSERT INTO app (id) VALUES (1)\0").await;
    let c = entered(rx).await;
    drop(target);
    assert!(c.check().is_ok());
    release.send(()).unwrap();
    f.admin.set_decision_hook(None);
    let (pg, connection) = f.pg(f.app.clone()).await;
    assert_eq!(
        pg.query("SELECT id FROM app WHERE id=1", &[])
            .await
            .unwrap()
            .len(),
        1
    );
    drop(pg);
    connection.await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_cancel_returns_from_core_lock_wait_before_holder_releases() {
    let f = Fixture::new().await;
    let (mut holder, _) = raw_key(&f).await;
    let (mut waiter, key) = raw_key(&f).await;
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let held_tx = Mutex::new(Some(held_tx));
    let (waiting_tx, waiting_rx) = tokio::sync::oneshot::channel();
    let waiting_tx = Mutex::new(Some(waiting_tx));
    let (release, wait) = mpsc::channel();
    let wait = Mutex::new(wait);
    f.admin.set_decision_hook(Some(Arc::new(move |point| {
        if point == DecisionPoint::BeforeDecision
            && let Some(tx) = held_tx.lock().unwrap().take()
        {
            tx.send(()).unwrap();
            wait.lock().unwrap().recv_timeout(WAIT).unwrap();
        }
        if point == DecisionPoint::WaitingLock
            && let Some(tx) = waiting_tx.lock().unwrap().take()
        {
            tx.send(()).unwrap();
        }
    })));
    pg_send(&mut holder, b'Q', b"INSERT INTO app (id) VALUES (1)\0").await;
    tokio::time::timeout(WAIT, held_rx).await.unwrap().unwrap();
    pg_send(&mut waiter, b'Q', b"INSERT INTO app (id) VALUES (2)\0").await;
    tokio::time::timeout(WAIT, waiting_rx)
        .await
        .unwrap()
        .unwrap();
    raw_cancel(&f, f.app.clone(), key).await;
    assert!(has_error(&query_done(&mut waiter).await, "57014"));
    release.send(()).unwrap();
    assert!(!has_error(&query_done(&mut holder).await, "57014"));
    f.admin.set_decision_hook(None);
    pg_send(&mut waiter, b'Q', b"INSERT INTO app (id) VALUES (2)\0").await;
    assert!(
        !query_done(&mut waiter)
            .await
            .iter()
            .any(|(t, _)| *t == b'E')
    );
    drop(holder);
    drop(waiter);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_commit_and_pipelined_rollback_obey_one_session_order() {
    for point in [DecisionPoint::BeforeDecision, DecisionPoint::Decided] {
        let f = Fixture::new().await;
        let (mut target, key) = raw_key(&f).await;
        for q in [b"BEGIN\0".as_slice(), b"INSERT INTO app (id) VALUES (1)\0"] {
            pg_send(&mut target, b'Q', q).await;
            assert!(
                !query_done(&mut target)
                    .await
                    .iter()
                    .any(|(t, _)| *t == b'E')
            );
        }
        let (rx, release) = pause(&f.admin, point);
        pg_send(&mut target, b'Q', b"COMMIT\0").await;
        let c = entered(rx).await;
        pg_send(&mut target, b'Q', b"ROLLBACK\0").await;
        raw_cancel(&f, f.app.clone(), key).await;
        let wins = point == DecisionPoint::BeforeDecision;
        assert_eq!(c.check().is_err(), wins);
        release.send(()).unwrap();
        let committed = query_done(&mut target).await;
        let rolled = query_done(&mut target).await;
        f.admin.set_decision_hook(None);
        if wins {
            assert!(has_error(&committed, "57014"));
            assert!(!rolled.iter().any(|(t, _)| *t == b'E'));
        } else {
            assert!(!committed.iter().any(|(t, _)| *t == b'E'));
            assert!(!rolled.iter().any(|(t, _)| *t == b'E'));
        }
        assert!(rolled.iter().any(|(t, b)| *t == b'C' && b == b"ROLLBACK\0"));
        assert_eq!(rolled.last(), Some(&(b'Z', vec![b'I'])));
        assert_eq!(
            f.admin
                .sql_session()
                .execute("SELECT id FROM app WHERE id=1", &[], None)
                .unwrap()
                .rows
                .len(),
            usize::from(!wins)
        );
        drop(target);
        f.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_native_task_deadline_preserves_operation_approval_budget_and_status() {
    let f = Fixture::new().await;
    let request = repair(9901, 0..6);
    let executor = f.admin.executor(TaskId(1), Principal(2)).unwrap();
    let time = now();
    let p = executor.preview_current(&request, time, time).unwrap();
    let approval = f
        .admin
        .approve(Principal(3), p, Time::Certified(time))
        .unwrap();
    let before = f.admin.snapshot().unwrap();
    let mut native = f.native(f.task.clone()).await;
    let (rx, release) = pause(&f.admin, DecisionPoint::BeforeDecision);
    let r = request.clone();
    let mut job = tokio::spawn(async move {
        let result = native
            .call(&Request::Deadline {
                budget_ms: WIRE_DEADLINE_MS,
                request: Box::new(Request::Execute {
                    request: r,
                    approval: Some(approval),
                }),
            })
            .await
            .unwrap();
        (native, result)
    });
    let c = tokio::select! {
        c = entered(rx) => c,
        completed = &mut job => {
            let (_, response) = completed.unwrap();
            panic!("native task deadline completed before required BeforeDecision hook: {response:?}");
        }
    };
    cancelled(&c).await;
    release.send(()).unwrap();
    let (mut native, response) = job.await.unwrap();
    f.admin.set_decision_hook(None);
    assert_eq!(response.code, "57014");
    assert_eq!(response.operation, None); // Unreserved cancellation envelope carries no task outcome.
    assert_eq!(f.admin.snapshot().unwrap(), before);
    assert_eq!(
        native
            .call(&Request::Status {
                operation: request.operation
            })
            .await
            .unwrap()
            .code,
        "absent"
    );
    let committed = native
        .call(&Request::Execute {
            request: request.clone(),
            approval: Some(approval),
        })
        .await
        .unwrap();
    assert_eq!(committed.code, "committed");
    assert_eq!(committed.count, Some(6));
    assert_eq!(
        native
            .call(&Request::Execute {
                request,
                approval: Some(approval)
            })
            .await
            .unwrap(),
        committed
    );
    drop(native);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_cancel_and_lost_task_ack_reconcile_same_operation_after_restart() {
    let f = Fixture::new().await;
    let path = f.path.clone();
    let config = f.task.clone();
    let server_config = f.server_config.clone();
    let bindings: Vec<Binding> =
        serde_json::from_slice(&std::fs::read(path.join("bindings.json")).unwrap()).unwrap();
    let request = Request::RepairSql {
        operation: OperationId(9991),
        sql: "UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND id = 0".into(),
        approval: None,
    };
    let (pg, connection) = f.pg(f.task.clone()).await;
    let token = pg.cancel_token();
    let (rx, release) = pause(&f.admin, DecisionPoint::Committed);
    let execution = serde_json::to_string(&request).unwrap();
    let job = tokio::spawn(async move { pg.simple_query(&format!("GIGGLES {execution}")).await });
    let c = entered(rx).await;
    token
        .cancel_query(tokio_postgres_rustls::MakeRustlsConnect::new(
            (*f.task).clone(),
        ))
        .await
        .unwrap();
    assert!(!c.cancel());
    // Deliberately discard the ACK by closing both driver tasks while commit is paused.
    job.abort();
    connection.abort();
    let _ = job.await;
    let _ = connection.await;
    release.send(()).unwrap();
    let mut native = f.native(f.task.clone()).await;
    let status = native
        .call(&Request::Status {
            operation: OperationId(9991),
        })
        .await
        .unwrap();
    assert_eq!(status.code, "committed");
    assert_eq!(status.count, Some(1));
    f.admin.set_decision_hook(None);
    drop(native);
    f.stop().await;
    let admin = Admin::open(path.join("db.redb"), Limits::default()).unwrap();
    let service = Service::new(admin.clone(), bindings, Clock::new(0, 3600).unwrap()).unwrap();
    let running = serve("127.0.0.1:0".parse().unwrap(), service, server_config, 8)
        .await
        .unwrap();
    let mut native = Client::connect(running.address, "localhost", config)
        .await
        .unwrap();
    assert_eq!(
        native
            .call(&Request::Status {
                operation: OperationId(9991)
            })
            .await
            .unwrap(),
        status
    );
    assert_eq!(native.call(&request).await.unwrap(), status);
    assert_eq!(admin.snapshot().unwrap().tasks[&TaskId(1)].transitions, 1);
    drop(native);
    running.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_driver_task_repair_cancel_before_decision_has_absent_status() {
    let f = Fixture::new().await;
    let (pg, connection) = f.pg(f.task.clone()).await;
    let token = pg.cancel_token();
    let request = Request::RepairSql {
        operation: OperationId(9992),
        sql: "UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND id = 0".into(),
        approval: None,
    };
    let (rx, release) = pause(&f.admin, DecisionPoint::BeforeDecision);
    let job = tokio::spawn(async move {
        let result = task_wire(&pg, &request).await;
        (pg, result)
    });
    let c = entered(rx).await;
    token
        .cancel_query(tokio_postgres_rustls::MakeRustlsConnect::new(
            (*f.task).clone(),
        ))
        .await
        .unwrap();
    cancelled(&c).await;
    release.send(()).unwrap();
    let (pg, result) = job.await.unwrap();
    f.admin.set_decision_hook(None);
    assert_eq!(result.code, "57014");
    assert_eq!(result.operation, None); // Unreserved cancellation envelope carries no task outcome.
    assert_eq!(
        task_wire(
            &pg,
            &Request::Status {
                operation: OperationId(9992)
            }
        )
        .await
        .code,
        "absent"
    );
    assert_eq!(f.admin.snapshot().unwrap().tasks[&TaskId(1)].transitions, 0);
    drop(pg);
    connection.await.unwrap();
    f.stop().await;
}
