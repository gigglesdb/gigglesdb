//! GD-0075 R5-103: per-role statement admission over pgwire. Role `a` at its
//! statement limit (blocked on an advisory lock) plus a full queue must not
//! move role `b`'s latency: p99 of b's statements stays under 1 s (without per-role admission it would wait until role a releases its lock) on a
//! loaded debug build, and a's statement beyond concurrent+queue fails 53000.
#![cfg(feature = "engine-rocksdb")]

use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Service,
    admission::RoleStatementLimits,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_postgres::{Client, NoTls};

const B_P99_BOUND: Duration = Duration::from_millis(1_000);

async fn client(address: SocketAddr, user: &str) -> Client {
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(address.port())
        .user(user)
        .dbname("postgres")
        .connect(NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    client
}

/// SQLSTATE and message of a failed statement.
fn refusal(
    result: Result<Vec<tokio_postgres::SimpleQueryMessage>, tokio_postgres::Error>,
) -> (String, String) {
    let error = result.unwrap_err();
    let db = error.as_db_error().expect("server error");
    (db.code().code().to_owned(), db.message().to_owned())
}

async fn server(concurrent: usize, queue: usize) -> (std::path::PathBuf, giggles_server::transport::Running) {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let root = std::path::PathBuf::from(std::env::var_os("TEMP").expect("task TEMP"));
    let path = root.join(format!(
        "role-admission-{}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    for (name, superuser) in [("owner", true), ("a", true), ("b", false)] {
        admin
            .roles()
            .create(NewRole {
                name: name.into(),
                login: true,
                superuser,
                ..NewRole::default()
            })
            .unwrap();
    }
    let service = Service::new(admin.clone(), vec![], Clock::new(0, 3600).unwrap()).unwrap();
    service
        .set_role_statement_limits(RoleStatementLimits {
            concurrent,
            queue,
            wait_ms: 2_000,
        })
        .unwrap();
    let config = PostgresqlConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: TlsMode::Disabled,
        certificate: None,
        private_key: None,
        client_ca: None,
        certificates: BTreeMap::new(),
        databases: vec!["postgres".into()],
        loopback_trust: true,
        connections: 64,
    };
    let pg = serve_postgresql(
        config.listen,
        service.clone(),
        Arc::new(config.build().unwrap()),
    )
    .await
    .unwrap();
    (path, pg)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn role_at_its_limit_does_not_move_another_roles_latency() {
    let (path, pg) = server(4, 1).await;
    let address = pg.address;
    let owner = client(address, "owner").await;
    owner
        .simple_query("SELECT pg_advisory_lock(7)")
        .await
        .unwrap();

    // Four statements hold the role cap. Another 36 lock waiters would
    // exhaust the shared pool without role admission.
    let mut waiting_sessions = Vec::new();
    for _ in 0..40 {
        let a = client(address, "a").await;
        a.simple_query("BEGIN").await.unwrap();
        waiting_sessions.push(a);
    }
    let mut blocked = Vec::new();
    for a in waiting_sessions.drain(..4) {
        blocked.push(tokio::spawn(async move {
            a.simple_query("SELECT pg_advisory_xact_lock(7); COMMIT")
                .await
                .map(|_| ())
        }));
    }
    // Probe until a is saturated: a probe that runs means a slot was free; once
    // all four are held the probe queues and fails after wait_ms with 53000.
    let probe = client(address, "a").await;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        assert!(Instant::now() < deadline, "role a never reached its limit");
        let started = Instant::now();
        match probe.simple_query("SELECT 1").await {
            Ok(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            Err(error) => {
                let (code, message) = refusal(Err(error));
                assert_eq!(code, "53000", "{message}");
                assert!(
                    message.contains("role \"a\"") && message.contains("waited 2000 ms"),
                    "{message}"
                );
                assert!(started.elapsed() >= Duration::from_millis(1_900));
                break;
            }
        }
    }

    // Of 36 extra lock waiters, one queues and 35 are refused immediately.
    let mut extra = Vec::new();
    for a in waiting_sessions {
        extra.push(tokio::spawn(async move {
            let started = Instant::now();
            let result = a.simple_query("SELECT pg_advisory_xact_lock(7); COMMIT").await;
            (refusal(result), started.elapsed(), a)
        }));
    }
    // Meanwhile role b runs short statements on two sessions.
    let mut sessions = Vec::new();
    for _ in 0..2 {
        let b = client(address, "b").await;
        sessions.push(tokio::spawn(async move {
            let mut latencies = Vec::new();
            for _ in 0..50 {
                let started = Instant::now();
                b.simple_query("SELECT 1").await.unwrap();
                latencies.push(started.elapsed());
            }
            latencies
        }));
    }
    let mut latencies = Vec::new();
    for session in sessions {
        latencies.extend(session.await.unwrap());
    }
    latencies.sort();
    let p99 = latencies[latencies.len() * 99 / 100 - 1];
    assert!(
        p99 < B_P99_BOUND,
        "role b p99 {p99:?} with role a at its limit; all {latencies:?}"
    );

    let mut outcomes = Vec::new();
    for task in extra {
        outcomes.push(task.await.unwrap());
    }
    outcomes.sort_by_key(|(_, elapsed, _)| *elapsed);
    assert_eq!(outcomes.len(), 36);
    for ((code, message), elapsed, _) in &outcomes[..35] {
        assert_eq!(code, "53000");
        assert!(message.contains("queue limit 1"), "{message}");
        assert!(*elapsed < Duration::from_millis(1_500), "{elapsed:?}");
    }
    let ((wait_code, waited), wait_elapsed, a) = &outcomes[35];
    assert_eq!(wait_code, "53000");
    assert!(waited.contains("waited 2000 ms"), "{waited}");
    assert!(*wait_elapsed >= Duration::from_millis(1_900));

    // Releasing the lock finishes a's blocked statements and frees its slots.
    owner
        .simple_query("SELECT pg_advisory_unlock(7)")
        .await
        .unwrap();
    for task in blocked {
        tokio::time::timeout(Duration::from_secs(30), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    a.simple_query("ROLLBACK").await.unwrap();
    for _ in 0..3 {
        a.simple_query("SELECT 1").await.unwrap();
        probe.simple_query("SELECT 1").await.unwrap();
    }
    drop(pg);
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transaction_owner_can_commit_while_role_slots_wait_on_its_row_lock() {
    let (path, pg) = server(2, 1).await;
    let address = pg.address;
    let owner = client(address, "a").await;
    owner.simple_query("CREATE TABLE admission_lock (id INT PRIMARY KEY, v INT); INSERT INTO admission_lock VALUES (1, 0)").await.unwrap();
    owner.simple_query("BEGIN; UPDATE admission_lock SET v = 1 WHERE id = 1").await.unwrap();
    let failed = client(address, "a").await;
    failed.simple_query("BEGIN").await.unwrap();
    assert!(failed.simple_query("SELECT * FROM admission_missing").await.is_err());
    let idle = client(address, "a").await;
    idle.simple_query("BEGIN; SAVEPOINT s").await.unwrap();
    let mut waiters = Vec::new();
    for _ in 0..2 {
        let a = client(address, "a").await;
        waiters.push(tokio::spawn(async move {
            a.simple_query("UPDATE admission_lock SET v = v + 1 WHERE id = 1").await
        }));
    }
    let probe = client(address, "a").await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(Instant::now() < deadline, "waiters never took both role slots");
        if let Err(error) = probe.simple_query("SELECT 1").await {
            assert_eq!(refusal(Err(error)).0, "53000");
            break;
        }
        tokio::task::yield_now().await;
    }
    for sql in ["SAVEPOINT t", "RELEASE SAVEPOINT t", "ROLLBACK TO SAVEPOINT s", "ABORT"] {
        tokio::time::timeout(Duration::from_secs(1), idle.simple_query(sql))
            .await.expect("transaction recovery control was gated").unwrap();
    }
    for extended in [false, true] {
        let error = if extended {
            failed.query("SELECT 1", &[]).await.unwrap_err()
        } else {
            failed.simple_query("SELECT 1").await.unwrap_err()
        };
        assert_eq!(error.as_db_error().unwrap().code().code(), "25P02");
    }
    // Extended-query rollback must also bypass a saturated role.
    tokio::time::timeout(Duration::from_secs(1), failed.execute("ROLLBACK", &[]))
        .await.expect("extended ROLLBACK was gated").unwrap();
    tokio::time::timeout(Duration::from_secs(1), owner.simple_query("COMMIT; SELECT 1"))
        .await.expect("COMMIT was gated behind its own row-lock waiters").unwrap();
    for waiter in waiters {
        tokio::time::timeout(Duration::from_secs(10), waiter).await.unwrap().unwrap().unwrap();
    }
    let rows = owner.simple_query("SELECT v FROM admission_lock WHERE id = 1").await.unwrap();
    assert!(rows.iter().any(|row| matches!(row, tokio_postgres::SimpleQueryMessage::Row(row) if row.get(0) == Some("3"))));
    drop(pg);
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_holds_role_slot_until_done_or_fail() {
    let (path, pg) = server(1, 0).await;
    let a = client(pg.address, "a").await;
    let probe = client(pg.address, "a").await;
    a.simple_query("CREATE TABLE admission_copy (id INT)").await.unwrap();
    let copy = a.copy_in::<_, bytes::Bytes>("COPY admission_copy FROM STDIN").await.unwrap();
    assert_eq!(refusal(probe.simple_query("SELECT 1").await).0, "53000");
    tokio::pin!(copy);
    assert_eq!(copy.as_mut().finish().await.unwrap(), 0);
    probe.simple_query("SELECT 1").await.unwrap();

    let failed = a.copy_in::<_, bytes::Bytes>("COPY admission_copy FROM STDIN").await.unwrap();
    assert_eq!(refusal(probe.simple_query("SELECT 1").await).0, "53000");
    drop(failed); // tokio-postgres sends CopyFail, followed by Sync.
    // Probe before sending another statement on the COPY connection.
    let deadline = Instant::now() + Duration::from_secs(5);
    while probe.simple_query("SELECT 1").await.is_err() {
        assert!(Instant::now() < deadline, "COPY cleanup retained its permit");
        tokio::task::yield_now().await;
    }
    a.simple_query("SELECT 1").await.unwrap();
    drop(pg);
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_and_aborted_transactions_do_not_hold_role_slots() {
    let (path, pg) = server(2, 0).await;
    let a1 = client(pg.address, "a").await;
    let a2 = client(pg.address, "a").await;
    let probe = client(pg.address, "a").await;
    a1.simple_query("BEGIN; SELECT 1").await.unwrap();
    a2.simple_query("BEGIN; SELECT 1").await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), probe.simple_query("SELECT 1"))
        .await.expect("idle transactions consumed role slots").unwrap();
    assert!(a1.simple_query("SELECT * FROM admission_missing").await.is_err());
    probe.simple_query("SELECT 1").await.unwrap();
    a1.simple_query("ROLLBACK").await.unwrap();
    a2.simple_query("ROLLBACK").await.unwrap();
    drop(pg);
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_inside_transaction_releases_slot_after_done_or_fail() {
    let (path, pg) = server(1, 0).await;
    let a = client(pg.address, "a").await;
    let probe = client(pg.address, "a").await;
    a.simple_query("CREATE TABLE admission_tx_copy (id INT); BEGIN").await.unwrap();
    let copy = a.copy_in::<_, bytes::Bytes>("COPY admission_tx_copy FROM STDIN").await.unwrap();
    assert_eq!(refusal(probe.simple_query("SELECT 1").await).0, "53000");
    tokio::pin!(copy);
    assert_eq!(copy.as_mut().finish().await.unwrap(), 0);
    probe.simple_query("SELECT 1").await.unwrap();
    let failed = a.copy_in::<_, bytes::Bytes>("COPY admission_tx_copy FROM STDIN").await.unwrap();
    assert_eq!(refusal(probe.simple_query("SELECT 1").await).0, "53000");
    drop(failed);
    // Probe before another statement on the COPY connection could mask a leak.
    let deadline = Instant::now() + Duration::from_secs(5);
    while probe.simple_query("SELECT 1").await.is_err() {
        assert!(Instant::now() < deadline, "CopyFail retained its permit");
        tokio::task::yield_now().await;
    }
    a.simple_query("END").await.unwrap();
    probe.simple_query("SELECT 1").await.unwrap();
    // An error in an implicit extended-query transaction releases on Sync.
    assert!(a.query("SELECT 1 / 0", &[]).await.is_err());
    // Probe before sending another statement on the COPY connection.
    let deadline = Instant::now() + Duration::from_secs(5);
    while probe.simple_query("SELECT 1").await.is_err() {
        assert!(Instant::now() < deadline, "COPY cleanup retained its permit");
        tokio::task::yield_now().await;
    }
    a.simple_query("SELECT 1").await.unwrap();
    drop(pg);
    let _ = std::fs::remove_dir_all(&path);
}
