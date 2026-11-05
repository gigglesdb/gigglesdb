use super::*;
use giggles_core::{OperationStatus, TaskResources};

#[tokio::test]
async fn accounting_same_bound_portal_each_execute_reserves_and_never_caches_output() {
    let f = Fixture::with_task_resources(resources(1, 100000)).await;
    let mut socket = super::prepared_network::raw_task(&f).await;
    pg_send(&mut socket, b'P', &parse("st", "GIGGLES STATUS ($1)")).await;
    pg_send(
        &mut socket,
        b'B',
        &bind("portal", "st", &[Some("\\x000000000000004d")], true),
    )
    .await;
    pg_send(&mut socket, b'H', &[]).await;
    assert_eq!(pg_read(&mut socket).await.0, b'1');
    assert_eq!(pg_read(&mut socket).await.0, b'2');
    let mut history = Vec::new();
    for _ in 0..3 {
        let frames = super::prepared_network::raw_execute(&mut socket, "portal", 1).await;
        history.push(super::prepared_network::row_response(&frames));
    }
    assert_eq!(history[0].code, "absent");
    check_withheld(&history[1]);
    check_withheld(&history[2]);
    assert_eq!(
        f.admin
            .task_resources(TaskId(1))
            .unwrap()
            .unwrap()
            .used
            .disclosed_rows,
        1
    );
    assert_eq!(
        f.admin
            .task_resources(TaskId(1))
            .unwrap()
            .unwrap()
            .used
            .read_rows,
        3
    );
    std::fs::write(
        f.path.join("same-portal-accounting-history.json"),
        serde_json::to_vec_pretty(&history).unwrap(),
    )
    .unwrap();
    drop(socket);
    f.stop().await;
}

fn resources(rows: u64, bytes: u64) -> TaskResources {
    TaskResources {
        read_rows: 10000,
        disclosed_rows: rows,
        encoded_bytes: bytes,
    }
}
fn bind_task(f: &Fixture, id: u64, limits: TaskResources) -> Arc<Service> {
    f.admin.grant(grant(id, 42, 25, 5)).unwrap();
    f.admin
        .provision_task_resources(TaskId(id), limits)
        .unwrap();
    super::prepared_network::task_service(f, id, 0)
}
fn absent(op: u64) -> Request {
    Request::Status {
        operation: OperationId(op),
    }
}
fn check_withheld(r: &Response) {
    assert_eq!(r, &Response::code("42501"));
}
async fn pg_response(c: &tokio_postgres::Client, request: &Request) -> Response {
    let rows = c
        .simple_query(&format!(
            "GIGGLES {}",
            serde_json::to_string(request).unwrap()
        ))
        .await
        .unwrap();
    rows.iter()
        .find_map(|r| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = r {
                Some(serde_json::from_str(r.get(0).unwrap()).unwrap())
            } else {
                None
            }
        })
        .unwrap()
}
async fn servers(f: &Fixture, service: Arc<Service>) -> (Running, Running) {
    let n = serve(
        "127.0.0.1:0".parse().unwrap(),
        service.clone(),
        f.server_config.clone(),
        32,
    )
    .await
    .unwrap();
    let p = serve_pg(
        "127.0.0.1:0".parse().unwrap(),
        service,
        f.server_config.clone(),
        32,
    )
    .await
    .unwrap();
    (n, p)
}
async fn prepared_status(
    c: &tokio_postgres::Client,
    s: &tokio_postgres::Statement,
    id: u64,
) -> Response {
    let id = id.to_be_bytes().to_vec();
    let row = c.query_one(s, &[&id]).await.unwrap();
    serde_json::from_str(&row.get::<_, String>(0)).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accounting_mixed_native_simple_prepared_last_allowance_race() {
    let f = Fixture::new().await;
    let service = bind_task(&f, 3, resources(1, 100000));
    let (n, p) = servers(&f, service).await;
    let barrier = Arc::new(tokio::sync::Barrier::new(12));
    let mut jobs = Vec::new();
    for i in 0..12 {
        let config = f.task.clone();
        let b = barrier.clone();
        let na = n.address;
        let pa = p.address;
        jobs.push(tokio::spawn(async move {
            if i % 3 == 0 {
                let mut c = Client::connect(na, "localhost", config).await.unwrap();
                b.wait().await;
                c.call(&absent(i)).await.unwrap()
            } else {
                let (c, conn) = super::prepared_network::connect_at(pa, config).await;
                let response = if i % 3 == 1 {
                    b.wait().await;
                    pg_response(&c, &absent(i)).await
                } else {
                    let s = c.prepare("GIGGLES STATUS ($1)").await.unwrap();
                    b.wait().await;
                    prepared_status(&c, &s, i).await
                };
                drop(c);
                conn.await.unwrap();
                response
            }
        }));
    }
    let mut history = Vec::new();
    for j in jobs {
        history.push(j.await.unwrap());
    }
    assert_eq!(history.iter().filter(|r| r.code == "absent").count(), 1);
    for r in history.iter().filter(|r| r.code != "absent") {
        check_withheld(r);
    }
    let a = f.admin.task_resources(TaskId(3)).unwrap().unwrap();
    assert_eq!(a.used.disclosed_rows, 1);
    assert_eq!(a.used.read_rows, 12);
    let winner = history.iter().find(|r| r.code == "absent").unwrap();
    assert_eq!(
        a.used.encoded_bytes,
        serde_json::to_vec(winner).unwrap().len() as u64 + 1024
    );
    std::fs::write(
        f.path.join("accounting-mixed-history.json"),
        serde_json::to_vec_pretty(&history).unwrap(),
    )
    .unwrap();
    n.shutdown().await.unwrap();
    p.shutdown().await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn accounting_bytes_exact_one_over_and_zero_preserve_committed_original() {
    for delta in [-1i64, 0, 1] {
        let f = Fixture::new().await;
        // First derive the canonical same-domain response with another task; its
        // digest depends only on effects, not transport/task identity.
        let expected = Response {
            operation: Some(OperationId(77)),
            ..Response::code("absent")
        };
        let exact = serde_json::to_vec(&expected).unwrap().len() as u64 + 1024;
        let service = bind_task(&f, 3, resources(1, (exact as i64 + delta) as u64));
        let (n, p) = servers(&f, service).await;
        let mut c = Client::connect(n.address, "localhost", f.task.clone())
            .await
            .unwrap();
        let r = c.call(&absent(77)).await.unwrap();
        if delta < 0 {
            check_withheld(&r);
        } else {
            assert_eq!(r, expected);
        }
        let used = f.admin.task_resources(TaskId(3)).unwrap().unwrap().used;
        assert_eq!(used.disclosed_rows, u64::from(delta >= 0));
        assert_eq!(used.encoded_bytes, if delta >= 0 { exact } else { 0 });
        check_withheld(&c.call(&absent(77)).await.unwrap());
        drop(c);
        n.shutdown().await.unwrap();
        p.shutdown().await.unwrap();
        f.stop().await;
    }
}

#[tokio::test]
async fn accounting_no_egress_mutation_commits_status_retry_and_empty_preview_withheld() {
    let f = Fixture::new().await;
    let service = bind_task(&f, 3, resources(0, 0));
    let (n, p) = servers(&f, service).await;
    let mut c = Client::connect(n.address, "localhost", f.task.clone())
        .await
        .unwrap();
    let r = repair(77, 0..1);
    check_withheld(
        &c.call(&Request::Execute {
            request: r.clone(),
            approval: None,
        })
        .await
        .unwrap(),
    );
    check_withheld(
        &c.call(&Request::Execute {
            request: r.clone(),
            approval: None,
        })
        .await
        .unwrap(),
    );
    check_withheld(&c.call(&absent(77)).await.unwrap());
    check_withheld(
        &c.call(&Request::Preview {
            request: repair(78, 0..0),
        })
        .await
        .unwrap(),
    );
    check_withheld(&c.call(&Request::Ping).await.unwrap());
    let e = f.admin.executor(TaskId(3), Principal(2)).unwrap();
    assert!(matches!(
        e.status(OperationId(77)).unwrap(),
        OperationStatus::Decided(Ok(_))
    ));
    assert_eq!(f.admin.snapshot().unwrap().tasks[&TaskId(3)].transitions, 1);
    assert_eq!(
        f.admin
            .task_resources(TaskId(3))
            .unwrap()
            .unwrap()
            .used
            .disclosed_rows,
        0
    );
    drop(c);
    n.shutdown().await.unwrap();
    p.shutdown().await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn accounting_reconnect_and_service_restart_cannot_reset_allowance() {
    let f = Fixture::new().await;
    let service = bind_task(&f, 3, resources(1, 100000));
    let (n, p) = servers(&f, service).await;
    let (c, conn) = super::prepared_network::connect_at(p.address, f.task.clone()).await;
    let s = c.prepare("GIGGLES STATUS ($1)").await.unwrap();
    assert_eq!(prepared_status(&c, &s, 88).await.code, "absent");
    check_withheld(&prepared_status(&c, &s, 88).await);
    drop(s);
    drop(c);
    conn.await.unwrap();
    n.shutdown().await.unwrap();
    p.shutdown().await.unwrap();
    let (n, p) = servers(&f, super::prepared_network::task_service(&f, 3, 0)).await;
    let mut c = Client::connect(n.address, "localhost", f.task.clone())
        .await
        .unwrap();
    check_withheld(&c.call(&absent(88)).await.unwrap());
    drop(c);
    let (c, conn) = super::prepared_network::connect_at(p.address, f.task.clone()).await;
    check_withheld(&pg_response(&c, &absent(88)).await);
    drop(c);
    conn.await.unwrap();
    n.shutdown().await.unwrap();
    p.shutdown().await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accounting_socket_loss_after_reservation_consumes_without_refund() {
    let f = Fixture::new().await;
    let before = f.admin.task_resources(TaskId(1)).unwrap().unwrap().used;
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = std::sync::Mutex::new(Some(tx));
    let (release, wait) = std::sync::mpsc::channel();
    let wait = std::sync::Mutex::new(wait);
    f.admin.set_decision_hook(Some(Arc::new(move |p| {
        if p == giggles_core::DecisionPoint::Committed
            && let Some(tx) = tx.lock().unwrap().take()
        {
            tx.send(()).unwrap();
            wait.lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(8))
                .unwrap();
        }
    })));
    let mut socket = raw_tls(&f, f.task.clone(), false).await;
    let request = serde_json::to_vec(&absent(777)).unwrap();
    socket.write_u32(request.len() as u32).await.unwrap();
    socket.write_all(&request).await.unwrap();
    socket.flush().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(8), rx)
        .await
        .unwrap()
        .unwrap();
    // The reservation is committed, but the service has not returned bytes to IO.
    drop(socket);
    release.send(()).unwrap();
    let mut c = f.native(f.task.clone()).await;
    let next = c.call(&absent(777)).await.unwrap();
    assert_eq!(next.code, "absent");
    f.admin.set_decision_hook(None);
    let after = f.admin.task_resources(TaskId(1)).unwrap().unwrap().used;
    let encoded = serde_json::to_vec(&next).unwrap().len() as u64 + 1024;
    assert_eq!(after.disclosed_rows - before.disclosed_rows, 2);
    assert_eq!(after.encoded_bytes - before.encoded_bytes, encoded * 2);
    assert_eq!(after.read_rows - before.read_rows, 2);
    std::fs::write(
        f.path.join("accounting-socket-loss.json"),
        serde_json::to_vec_pretty(&(before, after)).unwrap(),
    )
    .unwrap();
    drop(c);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accounting_native_and_simple_expiry_after_commit_withhold_original_outcome() {
    for native in [true, false] {
        let f = Fixture::new().await;
        let mut g = grant(3, 42, 1, 1);
        g.policy.expires = now() + 2;
        let expiry = g.policy.expires;
        f.admin.grant(g).unwrap();
        f.admin
            .provision_task_resources(TaskId(3), resources(1, 10000))
            .unwrap();
        let (n, p) = servers(&f, super::prepared_network::task_service(&f, 3, 0)).await;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let tx = std::sync::Mutex::new(Some(tx));
        let (release, wait) = std::sync::mpsc::channel();
        let wait = std::sync::Mutex::new(wait);
        f.admin.set_decision_hook(Some(Arc::new(move |point| {
            if point == giggles_core::DecisionPoint::Committed
                && let Some(tx) = tx.lock().unwrap().take()
            {
                tx.send(()).unwrap();
                wait.lock()
                    .unwrap()
                    .recv_timeout(std::time::Duration::from_secs(8))
                    .unwrap();
            }
        })));
        let config = f.task.clone();
        let na = n.address;
        let pa = p.address;
        let job = tokio::spawn(async move {
            let request = Request::Execute {
                request: repair(123, 0..1),
                approval: None,
            };
            if native {
                let mut c = Client::connect(na, "localhost", config).await.unwrap();
                c.call(&request).await.unwrap()
            } else {
                let (c, conn) = super::prepared_network::connect_at(pa, config).await;
                let r = pg_response(&c, &request).await;
                drop(c);
                conn.await.unwrap();
                r
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(8), rx)
            .await
            .unwrap()
            .unwrap();
        while now() < expiry {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        release.send(()).unwrap();
        let response = job.await.unwrap();
        f.admin.set_decision_hook(None);
        check_withheld(&response);
        let account = f.admin.task_resources(TaskId(3)).unwrap().unwrap();
        assert_eq!(account.used.disclosed_rows, 0);
        assert_eq!(account.used.encoded_bytes, 0);
        assert_eq!(account.used.read_rows, 1);
        assert!(matches!(
            f.admin
                .executor(TaskId(3), Principal(2))
                .unwrap()
                .status(OperationId(123))
                .unwrap(),
            OperationStatus::Decided(Ok(_))
        ));
        assert_eq!(f.admin.snapshot().unwrap().tasks[&TaskId(3)].transitions, 1);
        n.shutdown().await.unwrap();
        p.shutdown().await.unwrap();
        f.stop().await;
    }
}
