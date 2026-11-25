use giggles_core::{Admin, Engine, Limits, roles::{NewRole, SchemaPrivilege}};
use giggles_server::{Service, Running, auth::{PostgresqlConfig, TlsMode, serve_postgresql}, clock::Clock};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

type Fixture = (
    std::path::PathBuf, Admin, Arc<Service>, Running, tokio_postgres::Client,
    tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
);

async fn fixture() -> Fixture {
    start_fixture(false, None, 8).await
}

async fn start_fixture(
    superuser: bool,
    limits: Option<giggles_server::admission::RoleStatementLimits>,
    connections: usize,
) -> Fixture {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!("durable-{}-{}-{}", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)));
    let engine = if cfg!(feature = "engine-rocksdb") { Engine::V2 } else { Engine::Legacy };
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    admin.roles().create(NewRole { name: "owner".into(), login: true, superuser, ..Default::default() }).unwrap();
    admin.roles().grant_schema("public", "owner", &[SchemaPrivilege::Create, SchemaPrivilege::Usage]).unwrap();
    let service = Service::new(admin.clone(), vec![], Clock::new(0, 3600).unwrap()).unwrap();
    if let Some(limits) = limits {
        service.set_role_statement_limits(limits).unwrap();
    }
    let config = PostgresqlConfig {
        listen: "127.0.0.1:0".parse().unwrap(), tls: TlsMode::Disabled, certificate: None,
        private_key: None, client_ca: None, certificates: BTreeMap::new(),
        databases: vec!["postgres".into()], loopback_trust: true, connections,
    };
    let running = serve_postgresql(config.listen, service.clone(), Arc::new(config.build().unwrap())).await.unwrap();
    let (client, connection) = tokio_postgres::Config::new().hostaddr(running.address.ip()).port(running.address.port())
        .user("owner").dbname("postgres").connect(tokio_postgres::NoTls).await.unwrap();
    (path, admin, service, running, client, tokio::spawn(connection))
}
#[cfg(feature = "engine-rocksdb")]
struct ReleaseOnDrop(Arc<giggles_core::CommitSyncHook>);
#[cfg(feature = "engine-rocksdb")]
impl Drop for ReleaseOnDrop { fn drop(&mut self) { self.0.release(false); } }
fn idle(service: &Service) {
    assert_eq!(service.pgwire_blocking_live(), 0);
    assert_eq!(service.blocking_permits_held(), 0);
    for (available, capacity, queued) in service.role_pool_counts() {
        assert_eq!(available, capacity);
        assert_eq!(queued, 0);
    }
    let metrics = service.durability_metrics();
    assert_eq!(metrics["durable_completions_live"], 0);
    for metric in ["statements_awaiting_durability", "durable_async_total"] {
        for path in ["local", "cluster", "nextval"] { assert_eq!(metrics[metric][path], 0); }
    }
}
#[tokio::test]
async fn mixed_statements_release_all_counts() {
    let (path, admin, service, running, client, connection) = fixture().await;
    client.batch_execute("CREATE TABLE drain_rows(id int PRIMARY KEY)").await.unwrap();
    for n in 0..30 {
        client.batch_execute(&format!("INSERT INTO drain_rows VALUES ({n})")).await.unwrap();
        client.simple_query("SELECT * FROM drain_rows").await.unwrap();
        assert!(client.batch_execute("SELECT missing FROM drain_rows").await.is_err());
    }
    // The existing gate catches this panic and closes the connection; the
    // live-task guard must still retire after the worker returns.
    service.set_statement_gate(Arc::new(|| panic!("injected pgwire gate panic"))).unwrap_or_else(|_| panic!("gate already installed"));
    assert!(client.simple_query("SELECT 1").await.is_err());
    drop(client);
    let _ = connection.await.unwrap();
    running.shutdown().await.unwrap();
    assert!(service.pgwire_blocking_tasks() > 30);
    idle(&service);
    drop(service); drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
#[tokio::test]
async fn tracker_tokens_survive_unwind_and_shutdown_waits() {
    let (path, admin, service, running, client, connection) = fixture().await;
    let tracker = service.durable_tracker();
    let token = tracker.register();
    let context = giggles_core::Cancellation::default();
    context.set_durable_tracker(tracker.clone());
    assert_eq!(context.durable_tracker().unwrap().live(), 1);
    let panic_tracker = tracker.clone();
    assert!(tokio::task::spawn_blocking(move || { let _ticket = panic_tracker.register(); panic!("test token unwind"); }).await.is_err());
    assert_eq!(tracker.live(), 1);
    drop(client); connection.await.unwrap().unwrap();
    let draining = service.clone();
    let shutdown = tokio::spawn(async move {
        draining.stop_submissions();
        running.shutdown().await?;
        draining.drain().await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while service.drain_entries() == 0 { tokio::task::yield_now().await; }
    }).await.expect("drain entered");
    assert!(!shutdown.is_finished());
    // Exceed the ordinary worker-drain deadline while a real durable token
    // is held: terminal durability must not time out and tear down storage.
    tokio::time::sleep(Duration::from_secs(11)).await;
    assert!(!shutdown.is_finished());
    drop(token);
    shutdown.await.unwrap().unwrap();
    idle(&service);
    drop(context); drop(service); drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
#[cfg(feature = "engine-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_waits_for_detached_in_sync_commit() {
    shutdown_with_parked_sync_commit(Duration::from_millis(30), true).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_waits_for_in_sync_commit_when_listener_closes_socket() {
    shutdown_with_parked_sync_commit(Duration::from_millis(30), false).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listener_shutdown_waits_past_worker_deadline_for_sync_commit() {
    shutdown_with_parked_sync_commit(
        giggles_server::IO_TIMEOUT + Duration::from_secs(1),
        false,
    ).await;
}

#[cfg(feature = "engine-rocksdb")]
async fn shutdown_with_parked_sync_commit(listener_wait: Duration, disconnect_client: bool) {
    let (path, admin, service, running, client, mut connection) =
        start_fixture(false, None, 1).await;
    client.batch_execute("CREATE TABLE drain_rows(id int PRIMARY KEY)").await.unwrap();
    let hook = giggles_core::CommitSyncHook::new(); let _release = ReleaseOnDrop(hook.clone());
    admin.set_commit_sync_hook(Some(hook.clone()));
    let query = tokio::spawn(async move { client.batch_execute("INSERT INTO drain_rows VALUES (1)").await });
    let entered = { let hook = hook.clone(); tokio::task::spawn_blocking(move || hook.wait_entered(Duration::from_secs(5))).await.unwrap() };
    assert!(entered);
    assert_eq!(service.durability_metrics()["durable_completions_live"], 1);

    if disconnect_client {
        connection.abort();
        assert!((&mut connection).await.unwrap_err().is_cancelled());
        // A new SSLRequest gets a reply only after the old socket task
        // returns the sole listener permit. The TLS-disabled listener says N.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let ssl_request = [8u32.to_be_bytes(), 80877103u32.to_be_bytes()].concat();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(mut probe) = tokio::net::TcpStream::connect(running.address).await
                    && probe.write_all(&ssl_request).await.is_ok()
                    && matches!(probe.read_u8().await, Ok(b'N'))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("server socket task must end before shutdown");
    }
    service.stop_submissions();
    let mut shutdown = tokio::spawn(running.shutdown());
    if !disconnect_client {
        let _ = tokio::time::timeout(Duration::from_secs(5), &mut connection).await.unwrap().unwrap();
    }
    assert!(query.await.unwrap().is_err());
    tokio::time::timeout(listener_wait, &mut shutdown)
        .await
        .expect_err("listener shutdown returned with a parked sync commit");
    assert_eq!(service.drain_entries(), 0);
    assert_eq!(service.pgwire_blocking_live(), 1);
    let error = tokio::time::timeout(Duration::from_secs(1),
        service.drain_with_timeout(Duration::from_millis(30))).await
        .expect("durability drain must not hang").unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    assert_eq!(service.durability_metrics()["durable_completions_live"], 1);
    hook.release(false);
    shutdown.await.unwrap().unwrap();
    if !disconnect_client {
        service.drain().await.unwrap();
    }
    idle(&service);
    admin.set_commit_sync_hook(None);
    drop(service);
    drop(admin);
    let reopened = Admin::open(&path, Limits::default()).unwrap();
    // The in-Sync group succeeded despite disconnect and the shutdown fence.
    let result = reopened.sql_session().execute("SELECT * FROM drain_rows", &[], None).unwrap();
    assert_eq!(result.rows.len(), 1);
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

// A listener stop is also used between replication roles. It must not fence
// the Admin that can survive that transport lifecycle.
#[tokio::test]
async fn listener_shutdown_does_not_fence_shared_admin() {
    let (path, admin, service, running, client, connection) = fixture().await;
    running.shutdown().await.unwrap();
    drop(client);
    let _ = connection.await;
    let mut session = admin.sql_session();
    session.execute("CREATE TABLE after_listener_stop(id int PRIMARY KEY)", &[], None).unwrap();
    session.execute("INSERT INTO after_listener_stop VALUES (1)", &[], None).unwrap();
    service.drain().await.unwrap();
    idle(&service);
    drop(session); drop(service); drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[cfg(feature = "engine-rocksdb")]
mod mechanism_a {
    use super::*;
    use giggles_server::admission::RoleStatementLimits;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    type Wire = tokio::net::TcpStream;
    const EXECUTE_UNNAMED: &[u8] = b"\0\0\0\0\0";
    const BIND_COMMIT: &[u8] = b"\0c\0\0\0\0\0\0\0";
    const BIND_INSERT: &[u8] = b"\0i\0\0\0\0\0\0\0";
    const BLOCKING_CAPACITY: usize = 32;
    const ROW_LOCK: &str = "UPDATE admission_hold SET v = 1 WHERE id = 7";

    // A row-lock waiter retains its role and blocking permits.
    async fn hold_row_lock(client: &tokio_postgres::Client) {
        client.batch_execute(
            "CREATE TABLE admission_hold(id int PRIMARY KEY, v int); INSERT INTO admission_hold VALUES (7, 0)",
        ).await.unwrap();
        client.batch_execute(&format!("BEGIN; {ROW_LOCK}")).await.unwrap();
    }

    async fn fixture() -> Fixture {
        start_fixture(true, Some(RoleStatementLimits { concurrent: 1, queue: 8, wait_ms: 5000 }), 8).await
    }

    async fn connect(address: std::net::SocketAddr) -> Wire {
        connect_as(address, "owner").await
    }

    async fn connect_as(address: std::net::SocketAddr, role: &str) -> Wire {
        let mut wire = Wire::connect(address).await.unwrap();
        startup(&mut wire, role, "postgres").await;
        wire
    }

    async fn connect_cancellable(address: std::net::SocketAddr) -> (Wire, Vec<u8>) {
        let mut wire = Wire::connect(address).await.unwrap();
        let key = startup(&mut wire, "owner", "postgres").await.into_iter()
            .find(|(tag, _)| *tag == b'K').unwrap().1;
        (wire, key)
    }

    async fn cancel(address: std::net::SocketAddr, key: &[u8]) {
        let mut wire = Wire::connect(address).await.unwrap();
        let mut request = 16u32.to_be_bytes().to_vec();
        request.extend(80877102u32.to_be_bytes());
        request.extend_from_slice(key);
        wire.write_all(&request).await.unwrap();
        // EOF confirms the listener has handled the cancellation request.
        assert_eq!(tokio::time::timeout(Duration::from_secs(3), wire.read(&mut [0]))
            .await.unwrap().unwrap(), 0);
    }
    async fn startup(wire: &mut (impl AsyncReadExt + AsyncWriteExt + Unpin), role: &str, database: &str) -> Vec<(u8, Vec<u8>)> {
        let mut body = 196608u32.to_be_bytes().to_vec();
        body.extend_from_slice(format!("user\0{role}\0database\0{database}\0\0").as_bytes());
        wire.write_all(&((body.len() + 4) as u32).to_be_bytes()).await.unwrap();
        wire.write_all(&body).await.unwrap();
        until_ready(wire).await
    }
    fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut bytes = vec![tag];
        bytes.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        bytes.extend_from_slice(body); bytes
    }
    async fn query(wire: &mut (impl AsyncWriteExt + Unpin), sql: &str) {
        let mut body = sql.as_bytes().to_vec(); body.push(0);
        wire.write_all(&frame(b'Q', &body)).await.unwrap();
    }
    async fn message(wire: &mut (impl AsyncReadExt + Unpin)) -> (u8, Vec<u8>) {
        tokio::time::timeout(Duration::from_secs(10), async {
            let tag = wire.read_u8().await.unwrap();
            let n = wire.read_u32().await.unwrap();
            let mut body = vec![0; n as usize - 4]; wire.read_exact(&mut body).await.unwrap();
            (tag, body)
        }).await.expect("wire response timeout")
    }
    async fn until_ready(wire: &mut (impl AsyncReadExt + Unpin)) -> Vec<(u8, Vec<u8>)> {
        let mut messages = Vec::new();
        loop { let m = message(wire).await; let done = m.0 == b'Z'; messages.push(m); if done { return messages; } }
    }
    fn extended_execute(sql: &str) -> Vec<u8> {
        let mut bytes = frame(b'P', format!("\0{sql}\0\0\0").as_bytes());
        bytes.extend(frame(b'B', &[0; 8]));
        bytes.extend(frame(b'E', EXECUTE_UNNAMED));
        bytes
    }
    async fn entered(hook: &Arc<giggles_core::CommitSyncHook>) {
        let hook = hook.clone();
        assert!(tokio::task::spawn_blocking(move || hook.wait_entered(Duration::from_secs(5))).await.unwrap());
    }
    async fn wait_until(mut condition: impl FnMut() -> bool, description: &str) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await.expect(description);
    }

    async fn released(service: &Service) {
        wait_until(
            || service.durability_metrics()["statements_awaiting_durability"]["local"] == 1,
            "durability release",
        ).await;
    }

    async fn prepare_commit(wire: &mut Wire, insert: bool) {
        query(wire, "BEGIN; INSERT INTO pipeline_tail VALUES (1)").await;
        assert_eq!(until_ready(wire).await.last().unwrap().1, b"T");
        let mut bytes = frame(b'P', b"c\0COMMIT\0\0\0");
        if insert {
            bytes.extend(frame(b'P', b"i\0INSERT INTO pipeline_tail VALUES (2)\0\0\0"));
        }
        bytes.extend(frame(b'B', BIND_COMMIT));
        bytes.extend(frame(b'H', b""));
        wire.write_all(&bytes).await.unwrap();
        assert_eq!(message(wire).await.0, b'1');
        if insert {
            assert_eq!(message(wire).await.0, b'1');
        }
        assert_eq!(message(wire).await.0, b'2');
    }

    async fn saturate_blocking_pool(service: &Service, running: &Running, occupied: usize) -> Vec<Wire> {
        let mut waiters = Vec::new();
        for n in 0..BLOCKING_CAPACITY - occupied {
            let role = if n == 0 { "other" } else { "owner" };
            let mut waiter = connect_as(running.address, role).await;
            query(&mut waiter, ROW_LOCK).await;
            waiters.push(waiter);
        }
        wait_until(|| service.blocking_permits_held() == BLOCKING_CAPACITY, "blocking pool saturated").await;
        waiters
    }

    async fn saturated_fixture() -> Fixture {
        let (path, admin, service, running, client, connection) = start_fixture(
            true,
            Some(RoleStatementLimits { concurrent: BLOCKING_CAPACITY - 1, queue: 8, wait_ms: 5000 }),
            BLOCKING_CAPACITY + 4,
        ).await;
        admin.roles().create(NewRole { name: "other".into(), login: true, superuser: true, ..Default::default() }).unwrap();
        client.batch_execute("CREATE TABLE pipeline_tail(id int PRIMARY KEY)").await.unwrap();
        hold_row_lock(&client).await;
        (path, admin, service, running, client, connection)
    }

    async fn hold_role(service: &Service, running: &Running) -> (Wire, Vec<u8>) {
        let (mut holder, key) = connect_cancellable(running.address).await;
        query(&mut holder, ROW_LOCK).await;
        wait_until(|| service.role_pool_counts().contains(&(0, 1, 0)), "role permit occupied").await;
        (holder, key)
    }

    async fn release_role(running: &Running, (holder, key): (Wire, Vec<u8>)) {
        // EOF alone does not cancel an executing row-lock statement.
        cancel(running.address, &key).await;
        drop(holder);
    }

    fn assert_error(reply: (u8, Vec<u8>), sqlstate: &[u8]) {
        assert_eq!(reply.0, b'E');
        assert!(reply.1.split(|byte| *byte == 0).any(|field| field.strip_prefix(b"C") == Some(sqlstate)), "{reply:?}");
    }
    async fn queued(service: &Service) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !service.role_pool_counts().iter().any(|(_, _, queued)| *queued == 1) {
                tokio::task::yield_now().await;
            }
        }).await.expect("role waiter queued");
    }
    async fn remove_fixture(path: &std::path::Path) {
        // Windows can briefly report a sharing violation during engine teardown.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            match std::fs::remove_dir_all(path) {
                Ok(()) => return,
                Err(error) if cfg!(windows) && error.raw_os_error() == Some(32)
                    && std::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => panic!("remove {} after engine shutdown: {error}", path.display()),
            }
        }
    }

    async fn close_fixture(admin: &Admin, service: &Service, running: Running,
        client: tokio_postgres::Client, connection: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>) {
        admin.set_commit_sync_hook(None);
        drop(client); connection.await.unwrap().unwrap();
        running.shutdown().await.unwrap(); service.drain().await.unwrap(); idle(service);
        assert_eq!(admin.sql_operations_live(), 0);
    }

    async fn finish(path: std::path::PathBuf, admin: Admin, service: Arc<Service>, running: Running,
        client: tokio_postgres::Client, connection: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>) {
        close_fixture(&admin, &service, running, client, connection).await;
        drop(service); drop(admin); remove_fixture(&path).await;
    }

    async fn assert_only_first_row_after_reopen(path: &std::path::Path, sql: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let reopened = loop {
            match Admin::open(path, Limits::default()) {
                Ok(admin) => break admin,
                Err(giggles_core::CoreError::Storage(message))
                    if message == "database already open by another writer"
                        && std::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => panic!("reopen {} after engine shutdown: {error}", path.display()),
            }
        };
        let rows = reopened.sql_session().execute(sql, &[], None).unwrap().rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values, [giggles_core::relational::Value::Int(1)]);
        drop(reopened);
        remove_fixture(path).await;
    }

    async fn dml_with_a_parked_commit(extended: bool) {
        let (path, admin, service, running, client, connection) = start_fixture(
            true,
            Some(RoleStatementLimits { concurrent: BLOCKING_CAPACITY - 1, queue: 8, wait_ms: 5000 }),
            BLOCKING_CAPACITY + 4,
        ).await;
        admin.roles().create(NewRole { name: "other".into(), login: true, superuser: true, ..Default::default() }).unwrap();
        client.batch_execute("CREATE TABLE core_admission(id int PRIMARY KEY, n int); \
            INSERT INTO core_admission VALUES (1, 0)").await.unwrap();
        let mut parked = connect(running.address).await;
        let hook = giggles_core::CommitSyncHook::new();
        let _release = ReleaseOnDrop(hook.clone());
        admin.set_commit_sync_hook(Some(hook.clone()));
        let sql = "UPDATE core_admission SET n = 1 WHERE id = 1";
        if extended {
            let mut bytes = extended_execute(sql);
            bytes.extend(frame(b'S', b""));
            parked.write_all(&bytes).await.unwrap();
        } else {
            query(&mut parked, sql).await;
        }
        entered(&hook).await;
        released(&service).await;

        let mut waiters = tokio::task::JoinSet::new();
        for index in 0..=BLOCKING_CAPACITY {
            let role = if index < 2 { "other" } else { "owner" };
            let mut wire = connect_as(running.address, role).await;
            query(&mut wire, "UPDATE core_admission SET n = 2 WHERE id = 1").await;
            waiters.spawn(async move { until_ready(&mut wire).await });
        }
        tokio::select! {
            result = waiters.join_next() => {
                let replies = result.unwrap().unwrap();
                panic!("DML replied while its row was locked: {:?}", String::from_utf8_lossy(&replies[0].1));
            }
            () = wait_until(|| admin.waiting_lock_count() == BLOCKING_CAPACITY, "all admitted DML waiting on the row lock") => {}
        }
        assert_eq!(admin.sql_operations_live(), BLOCKING_CAPACITY);
        assert_eq!(service.blocking_permits_held(), BLOCKING_CAPACITY);

        admin.set_commit_sync_hook(None);
        hook.release(false);
        let expected = if extended { b"12CZ".as_slice() } else { b"CZ".as_slice() };
        let replies = until_ready(&mut parked).await;
        assert_eq!(replies.iter().map(|reply| reply.0).collect::<Vec<_>>(), expected);
        assert_eq!(replies[replies.len() - 2].1, b"UPDATE 1\0");
        while let Some(result) = waiters.join_next().await {
            assert_eq!(result.unwrap(), [(b'C', b"UPDATE 1\0".to_vec()), (b'Z', b"I".to_vec())]);
        }
        drop(parked);
        finish(path, admin, service, running, client, connection).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn autocommit_park_leaves_core_admission_for_dml() {
        dml_with_a_parked_commit(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn implicit_sync_park_leaves_core_admission_for_dml() {
        dml_with_a_parked_commit(true).await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn autocommit_releases_both_permits_and_never_sends_early_success() {
        for fail in [false, true] {
            let (path, admin, service, running, client, connection) = fixture().await;
            client.batch_execute("CREATE TABLE tail_rows(id int PRIMARY KEY)").await.unwrap();
            let mut a = connect(running.address).await;
            let hook = giggles_core::CommitSyncHook::new(); let _release = ReleaseOnDrop(hook.clone()); admin.set_commit_sync_hook(Some(hook.clone()));
            query(&mut a, "INSERT INTO tail_rows VALUES (1)").await;
            entered(&hook).await; released(&service).await;
            assert_eq!(service.blocking_permits_held(), 0);
            assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 1);
            tokio::time::timeout(Duration::from_secs(3), client.simple_query("SELECT 1")).await.unwrap().unwrap();
            // B completed a wire round trip while A's Sync remains stalled.
            // Observe A without introducing a timing-based negative window.
            let mut byte = [0];
            assert!(matches!(a.try_read(&mut byte), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock));
            hook.release(fail);
            let responses = until_ready(&mut a).await;
            assert_eq!(responses.iter().any(|m| m.0 == b'E'), fail);
            assert_eq!(responses.iter().any(|m| m.0 == b'C'), !fail);
            admin.set_commit_sync_hook(None);
            if !fail { assert_eq!(client.query("SELECT * FROM tail_rows", &[]).await.unwrap().len(), 1); }
            drop(a);
            if fail {
                admin.set_commit_sync_hook(None); drop(client); connection.await.unwrap().unwrap();
                running.shutdown().await.unwrap(); service.drain().await.unwrap(); idle(&service);
                drop(service); drop(admin);
                let reopened = Admin::open(&path, Limits::default()).unwrap();
                assert!(reopened.sql_session().execute("SELECT * FROM tail_rows", &[], None).unwrap().rows.is_empty());
                drop(reopened); remove_fixture(&path).await;
            } else { finish(path, admin, service, running, client, connection).await; }
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ddl_sequence_install_and_reclaim_keep_admission() {
        for sql in ["CREATE TABLE ddl_tail(id int)", "CREATE SEQUENCE tail_seq", "DROP TABLE victim"] {
            let (path, admin, service, running, client, connection) = fixture().await;
            client.batch_execute("CREATE TABLE victim(id int)").await.unwrap();
            let mut a = connect(running.address).await;
            let hook = giggles_core::CommitSyncHook::new(); let _release = ReleaseOnDrop(hook.clone()); admin.set_commit_sync_hook(Some(hook.clone()));
            query(&mut a, sql).await; entered(&hook).await;
            assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 0);
            { let b = client.simple_query("SELECT 1"); tokio::pin!(b);
            tokio::select! {
                result = &mut b => panic!("queued query completed early: {result:?}"),
                () = queued(&service) => {}
            }
            hook.release(false);
            assert!(!until_ready(&mut a).await.iter().any(|m| m.0 == b'E'));
            assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 0);
            b.await.unwrap(); } drop(a);
            finish(path, admin, service, running, client, connection).await;
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn serial_settlement_remains_eligible() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute("CREATE TABLE serial_tail(id serial PRIMARY KEY)").await.unwrap();
        // Warm the counter reservation before installing the group-Sync hook.
        client.batch_execute("INSERT INTO serial_tail DEFAULT VALUES").await.unwrap();
        let mut a = connect(running.address).await;
        let before = service.durability_metrics()["durable_releases_total"]["local"].as_u64().unwrap();
        let hook = giggles_core::CommitSyncHook::new(); let _release = ReleaseOnDrop(hook.clone()); admin.set_commit_sync_hook(Some(hook.clone()));
        query(&mut a, "INSERT INTO serial_tail DEFAULT VALUES RETURNING id").await;
        entered(&hook).await; released(&service).await;
        client.simple_query("SELECT 1").await.unwrap();
        hook.release(false);
        let responses = until_ready(&mut a).await;
        assert!(responses.iter().any(|(tag, body)| *tag == b'D' && body.ends_with(b"2")));
        assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], before + 1);
        drop(a); finish(path, admin, service, running, client, connection).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn serial_reservation_settles_before_release_and_late_cancel_keeps_success() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute("CREATE TABLE serial_tail(id serial PRIMARY KEY); \
            ALTER SEQUENCE serial_tail_id_seq MAXVALUE 34").await.unwrap();
        client.batch_execute("INSERT INTO serial_tail DEFAULT VALUES").await.unwrap();
        // The warm insert reserves through 33. One refill covers 34, the only
        // value in this statement that needs the new reservation to be durable.
        let values = vec!["(DEFAULT)"; 33].join(",");
        let (mut wire, cancel_key) = connect_cancellable(running.address).await;
        let before = service.durability_metrics()["durable_releases_total"]["local"].clone();
        let reservation = giggles_core::CommitSyncHook::new();
        let _reservation_release = ReleaseOnDrop(reservation.clone());
        let commit = giggles_core::CommitSyncHook::new();
        let _commit_release = ReleaseOnDrop(commit.clone());
        admin.set_commit_sync_hook(Some(reservation.clone()));
        query(&mut wire, &format!("INSERT INTO serial_tail VALUES {values} RETURNING id")).await;
        entered(&reservation).await;
        {
            let waiting = client.simple_query("SELECT 1");
            tokio::pin!(waiting);
            tokio::select! {
                result = &mut waiting => panic!("role admission opened before reservation settlement: {result:?}"),
                () = queued(&service) => {}
            }
            assert_eq!(service.blocking_permits_held(), 1);
            assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], before);

            admin.set_commit_sync_hook(Some(commit.clone()));
            reservation.release(false);
            entered(&commit).await;
            released(&service).await;
            tokio::time::timeout(Duration::from_secs(3), waiting).await.unwrap().unwrap();
        }
        assert_eq!(service.blocking_permits_held(), 0);

        cancel(running.address, &cancel_key).await;
        commit.release(false);
        let replies = until_ready(&mut wire).await;
        assert!(!replies.iter().any(|reply| reply.0 == b'E'));
        assert_eq!(replies.iter().filter(|reply| reply.0 == b'D').count(), 33);
        assert_eq!(replies[replies.len() - 2], (b'C', b"INSERT 0 33\0".to_vec()));
        assert_eq!(client.query("SELECT id FROM serial_tail", &[]).await.unwrap().len(), 34);
        drop(wire);
        finish(path, admin, service, running, client, connection).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn returning_reply_spilled_serial_settles_before_late_cancel() {
        let _serial = crate::api_result_stream::SERIAL.lock().await;
        let (path, admin, service, running, client, connection) = fixture().await;
        client
            .batch_execute("CREATE TABLE serial_tail(id serial PRIMARY KEY)")
            .await
            .unwrap();
        client
            .batch_execute("ALTER SEQUENCE serial_tail_id_seq MAXVALUE 34")
            .await
            .unwrap();
        client
            .batch_execute("INSERT INTO serial_tail DEFAULT VALUES")
            .await
            .unwrap();
        let values = vec!["(DEFAULT)"; 33].join(",");
        let (mut wire, cancel_key) = connect_cancellable(running.address).await;
        let reservation = giggles_core::CommitSyncHook::new();
        let _reservation_release = ReleaseOnDrop(reservation.clone());
        let commit = giggles_core::CommitSyncHook::new();
        let _commit_release = ReleaseOnDrop(commit.clone());
        admin.set_commit_sync_hook(Some(reservation.clone()));
        query(
            &mut wire,
            &format!("INSERT INTO serial_tail VALUES {values} RETURNING id, repeat('r', 40000)"),
        )
        .await;
        entered(&reservation).await;
        admin.set_commit_sync_hook(Some(commit.clone()));
        reservation.release(false);
        entered(&commit).await;

        cancel(running.address, &cancel_key).await;
        commit.release(false);
        let replies = until_ready(&mut wire).await;
        let written = client
            .query("SELECT id FROM serial_tail", &[])
            .await
            .unwrap()
            .len();
        let error = replies
            .iter()
            .find(|(tag, _)| *tag == b'E')
            .map(|(_, body)| String::from_utf8_lossy(body).into_owned());
        let rows = replies.iter().filter(|(tag, _)| *tag == b'D').count();
        drop(wire);
        finish(path, admin, service, running, client, connection).await;
        assert_eq!(written, 34);
        assert!(error.is_none(), "write committed with {error:?}");
        assert_eq!(rows, 33);
        assert_eq!(
            replies[replies.len() - 2],
            (b'C', b"INSERT 0 33\0".to_vec())
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn returning_reply_w4_implicit_batch_settles_serial_before_late_cancel() {
        serial_batch_settles_before_late_cancel(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn returning_reply_w5_explicit_commit_settles_serial_before_late_cancel() {
        serial_batch_settles_before_late_cancel(true).await;
    }

    async fn serial_batch_settles_before_late_cancel(explicit: bool) {
        let (path, admin, service, running, client, connection) = fixture().await;
        client
            .batch_execute("CREATE TABLE serial_tail(id serial PRIMARY KEY)")
            .await
            .unwrap();
        client
            .batch_execute("ALTER SEQUENCE serial_tail_id_seq MAXVALUE 34")
            .await
            .unwrap();
        client
            .batch_execute("INSERT INTO serial_tail DEFAULT VALUES")
            .await
            .unwrap();
        let (mut wire, cancel_key) = connect_cancellable(running.address).await;
        let reservation = giggles_core::CommitSyncHook::new();
        let _reservation_release = ReleaseOnDrop(reservation.clone());
        let commit = giggles_core::CommitSyncHook::new();
        let _commit_release = ReleaseOnDrop(commit.clone());
        let values = vec!["(DEFAULT)"; if explicit { 33 } else { 32 }].join(",");
        let (sql, completion) = if explicit {
            (
                format!("BEGIN; INSERT INTO serial_tail VALUES {values}; COMMIT"),
                b"COMMIT\0".as_slice(),
            )
        } else {
            (
                format!(
                    "INSERT INTO serial_tail DEFAULT VALUES RETURNING id;
                     INSERT INTO serial_tail VALUES {values}"
                ),
                b"INSERT 0 32\0".as_slice(),
            )
        };
        admin.set_commit_sync_hook(Some(reservation.clone()));
        query(&mut wire, &sql).await;
        entered(&reservation).await;
        admin.set_commit_sync_hook(Some(commit.clone()));
        reservation.release(false);
        entered(&commit).await;

        cancel(running.address, &cancel_key).await;
        commit.release(false);
        let replies = until_ready(&mut wire).await;
        let written = client
            .query("SELECT id FROM serial_tail", &[])
            .await
            .unwrap()
            .len();
        drop(wire);
        finish(path, admin, service, running, client, connection).await;
        assert_eq!(written, 34);
        assert_eq!(replies.last().unwrap().1, b"I");
        assert_eq!(
            replies[replies.len() - 2],
            (b'C', completion.to_vec()),
            "write committed with {replies:?}"
        );
    }

    async fn cancel_while_reservation_is_pending(extended: bool) {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute("CREATE TABLE cancel_serial(id serial PRIMARY KEY); \
            ALTER SEQUENCE cancel_serial_id_seq MAXVALUE 34").await.unwrap();
        // This batch retains admission. The first insert reserves through 33.
        client.batch_execute("BEGIN; INSERT INTO cancel_serial DEFAULT VALUES; COMMIT").await.unwrap();
        let values = vec!["(DEFAULT)"; 33].join(",");
        let sql = format!("INSERT INTO cancel_serial VALUES {values}");
        let (mut wire, cancel_key) = connect_cancellable(running.address).await;
        let reservation = giggles_core::CommitSyncHook::new();
        let _release = ReleaseOnDrop(reservation.clone());
        admin.set_commit_sync_hook(Some(reservation.clone()));
        if extended {
            let mut bytes = extended_execute(&sql);
            bytes.extend(frame(b'H', b""));
            wire.write_all(&bytes).await.unwrap();
        } else {
            query(&mut wire, &sql).await;
        }
        entered(&reservation).await;
        wait_until(|| service.durable_tracker().live() == 1, "statement waiting on reservation acknowledgment").await;
        if !extended {
            assert_eq!(admin.sql_operations_live(), 1);
        }
        assert_eq!(service.blocking_permits_held(), 1);
        assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 0);
        cancel(running.address, &cancel_key).await;
        if extended {
            // Execute must fail before returning CommandComplete, with Sync
            // still unsent and the reservation still held.
            assert_eq!(message(&mut wire).await.0, b'1');
            assert_eq!(message(&mut wire).await.0, b'2');
            assert_error(message(&mut wire).await, b"57014");
            wire.write_all(&frame(b'S', b"")).await.unwrap();
            assert_eq!(message(&mut wire).await, (b'Z', b"I".to_vec()));
        } else {
            wait_until(|| service.durable_tracker().live() == 0, "cancellation stopped reservation wait").await;
        }
        admin.set_commit_sync_hook(None);
        reservation.release(false);

        if !extended {
            let replies = until_ready(&mut wire).await;
            assert_eq!(replies.len(), 2);
            assert_error(replies[0].clone(), b"57014");
            assert_eq!(replies[1], (b'Z', b"I".to_vec()));
        }
        assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 0);
        drop(wire);
        close_fixture(&admin, &service, running, client, connection).await;
        drop(service);
        drop(admin);
        assert_only_first_row_after_reopen(&path, "SELECT id FROM cancel_serial").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn simple_query_cancel_during_reservation_ack_rolls_back() {
        cancel_while_reservation_is_pending(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn implicit_sync_cancel_during_reservation_ack_rolls_back() {
        cancel_while_reservation_is_pending(true).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_from_notice_waits_for_reservation_ack() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute(
            "CREATE SEQUENCE copy_notice_seq;
             CREATE TABLE copy_notice(id bigint PRIMARY KEY DEFAULT nextval('copy_notice_seq'), query tsquery);
             SELECT setval('copy_notice_seq', 2)",
        ).await.unwrap();
        let mut wire = connect(running.address).await;
        query(&mut wire, "COPY copy_notice(id, query) FROM STDIN WITH (FORMAT csv, DEFAULT 'use_default')").await;
        assert_eq!(message(&mut wire).await.0, b'G');

        let reservation = giggles_core::CommitSyncHook::new();
        let _release = ReleaseOnDrop(reservation.clone());
        admin.set_commit_sync_hook(Some(reservation.clone()));
        let mut data: String = (100..299).map(|id| format!("{id},cat\n")).collect();
        data.push_str("use_default,\"   \"\n");
        wire.write_all(&frame(b'd', data.as_bytes())).await.unwrap();
        entered(&reservation).await;
        let mut byte = [0];
        tokio::select! {
            biased;
            early = wire.peek(&mut byte) => panic!("COPY replied before settling its notice: {early:?}"),
            () = wait_until(|| service.durable_tracker().live() == 1, "COPY notice settlement") => {}
        }
        admin.set_commit_sync_hook(None);
        reservation.release(false);
        let (tag, notice) = message(&mut wire).await;
        assert_eq!(tag, b'N');
        assert!(String::from_utf8_lossy(&notice).contains("lexemes: \"   \""));
        wire.write_all(&frame(b'c', b"")).await.unwrap();
        let replies = until_ready(&mut wire).await;
        assert_eq!(replies, [(b'C', b"COPY 200\0".to_vec()), (b'Z', b"I".to_vec())]);
        drop(wire);
        finish(path, admin, service, running, client, connection).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_from_cancelled_reservation_scrubs_default_error() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute(
            "CREATE SEQUENCE copy_error_seq;
             CREATE TABLE copy_error(payload bigint, id uuid DEFAULT (nextval('copy_error_seq')::text)::uuid);
             SELECT setval('copy_error_seq', 54320)",
        ).await.unwrap();
        let (mut wire, cancel_key) = connect_cancellable(running.address).await;
        query(&mut wire, "COPY copy_error(payload) FROM STDIN").await;
        assert_eq!(message(&mut wire).await.0, b'G');
        let reservation = giggles_core::CommitSyncHook::new();
        let _release = ReleaseOnDrop(reservation.clone());
        admin.set_commit_sync_hook(Some(reservation.clone()));
        wire.write_all(&frame(b'd', "1\n".repeat(200).as_bytes())).await.unwrap();
        entered(&reservation).await;
        cancel(running.address, &cancel_key).await;
        let response = message(&mut wire).await;
        admin.set_commit_sync_hook(None);
        reservation.release(false);
        assert_error(response.clone(), b"57014");
        assert!(!String::from_utf8_lossy(&response.1).contains("54321"), "{response:?}");
        assert_eq!(message(&mut wire).await, (b'Z', b"I".to_vec()));
        drop(wire);
        finish(path, admin, service, running, client, connection).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn binary_copy_buffers_a_page_before_settling_at_done() {
        binary_copy_with_ack_held(None).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn binary_copy_fail_returns_with_ack_held() {
        binary_copy_with_ack_held(Some((b'f', b"client stopped COPY\0", b"57014"))).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn binary_copy_decoder_error_returns_with_ack_held() {
        binary_copy_with_ack_held(Some((b'd', b"extra data", b"22P04"))).await;
    }

    async fn binary_copy_with_ack_held(failure: Option<(u8, &[u8], &[u8])>) {
        tokio::time::timeout(Duration::from_secs(30), async {
            binary_copy_page(failure).await;
        }).await.expect("binary COPY fixture timed out");
    }

    async fn binary_copy_page(failure: Option<(u8, &[u8], &[u8])>) {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute(
            "CREATE SEQUENCE binary_seq;
             CREATE TABLE binary_page(id bigint PRIMARY KEY DEFAULT nextval('binary_seq'), payload int);
             SELECT setval('binary_seq', 2)",
        ).await.unwrap();
        let mut wire = connect(running.address).await;
        query(&mut wire, "SET statement_timeout = 0").await;
        assert_eq!(until_ready(&mut wire).await.last().unwrap().1, b"I");
        query(&mut wire, "COPY binary_page(payload) FROM STDIN (FORMAT binary)").await;
        assert_eq!(message(&mut wire).await.0, b'G');

        let reservation = giggles_core::CommitSyncHook::new();
        let _release = ReleaseOnDrop(reservation.clone());
        admin.set_commit_sync_hook(Some(reservation.clone()));
        let mut data = b"PGCOPY\n\xff\r\n\0".to_vec();
        data.extend([0; 8]); // Flags and header extension length.
        data.extend(1i16.to_be_bytes());
        data.extend(4i32.to_be_bytes());
        data.extend(42i32.to_be_bytes());
        data.extend((-1i16).to_be_bytes());
        let tasks_before = service.pgwire_blocking_tasks();
        wire.write_all(&frame(b'd', &data)).await.unwrap();
        entered(&reservation).await;
        wait_until(
            || service.pgwire_blocking_tasks() > tasks_before && service.blocking_permits_held() == 0,
            "binary COPY page waited for its reservation",
        ).await;

        if let Some((tag, body, code)) = failure {
            let replies = tokio::time::timeout(Duration::from_secs(2), async {
                wire.write_all(&frame(tag, body)).await.unwrap();
                until_ready(&mut wire).await
            }).await;
            admin.set_commit_sync_hook(None);
            reservation.release(false);
            let replies = replies.expect("binary COPY rejection waited for the held reservation");
            assert_eq!(replies.len(), 2);
            assert_error(replies[0].clone(), code);
            assert_eq!(replies[1], (b'Z', b"I".to_vec()));
            assert_eq!(client.query_one("SELECT count(*) FROM binary_page", &[]).await.unwrap().get::<_, i64>(0), 0);
            drop(wire);
            finish(path, admin, service, running, client, connection).await;
            return;
        }

        wire.write_all(&frame(b'c', b"")).await.unwrap();
        let mut byte = [0];
        tokio::select! {
            biased;
            early = wire.peek(&mut byte) => panic!("CopyDone replied while its reservation was held: {early:?}"),
            () = wait_until(|| service.durable_tracker().live() == 1, "CopyDone settlement") => {}
        }
        admin.set_commit_sync_hook(None);
        reservation.release(false);
        assert_eq!(until_ready(&mut wire).await, [(b'C', b"COPY 1\0".to_vec()), (b'Z', b"I".to_vec())]);
        let row = client.query_one("SELECT id, payload FROM binary_page", &[]).await.unwrap();
        assert_eq!((row.get::<_, i64>(0), row.get::<_, i32>(1)), (3, 42));
        drop(wire);
        finish(path, admin, service, running, client, connection).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn malformed_sync_recovers_timed_out_execute_with_held_ack() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute(
            "CREATE SEQUENCE recover_seq;
             CREATE TABLE recover_insert(id bigint PRIMARY KEY DEFAULT nextval('recover_seq'));
             SELECT setval('recover_seq', 2)",
        ).await.unwrap();
        let mut wire = connect(running.address).await;
        query(&mut wire, "SET statement_timeout = '2s'").await;
        assert_eq!(until_ready(&mut wire).await.last().unwrap().1, b"I");
        let reservation = giggles_core::CommitSyncHook::new();
        let _release = ReleaseOnDrop(reservation.clone());
        admin.set_commit_sync_hook(Some(reservation.clone()));
        wire.write_all(&extended_execute("INSERT INTO recover_insert DEFAULT VALUES")).await.unwrap();
        entered(&reservation).await;
        assert_eq!(message(&mut wire).await.0, b'1');
        assert_eq!(message(&mut wire).await.0, b'2');
        assert_error(message(&mut wire).await, b"57014");

        wire.write_all(&frame(b'S', b"extra")).await.unwrap();
        assert_error(message(&mut wire).await, b"08P01");
        assert_eq!(message(&mut wire).await, (b'Z', b"I".to_vec()));
        assert_eq!(admin.sql_transactions_live(), 0);
        admin.set_commit_sync_hook(None);
        reservation.release(false);
        drop(wire);
        finish(path, admin, service, running, client, connection).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rollback_with_no_statement_timeout_returns_with_held_ack() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute(
            "CREATE SEQUENCE rollback_seq;
             CREATE TABLE rollback_insert(id bigint PRIMARY KEY DEFAULT nextval('rollback_seq'));
             SELECT setval('rollback_seq', 2)",
        ).await.unwrap();
        let (mut wire, cancel_key) = connect_cancellable(running.address).await;
        query(&mut wire, "SET statement_timeout = 0").await;
        assert_eq!(until_ready(&mut wire).await.last().unwrap().1, b"I");
        query(&mut wire, "BEGIN").await;
        assert_eq!(until_ready(&mut wire).await.last().unwrap().1, b"T");

        let reservation = giggles_core::CommitSyncHook::new();
        let _release = ReleaseOnDrop(reservation.clone());
        admin.set_commit_sync_hook(Some(reservation.clone()));
        wire.write_all(&extended_execute("INSERT INTO rollback_insert DEFAULT VALUES")).await.unwrap();
        entered(&reservation).await;
        wait_until(|| service.durable_tracker().live() == 1, "Execute reservation wait").await;
        cancel(running.address, &cancel_key).await;
        assert_eq!(message(&mut wire).await.0, b'1');
        assert_eq!(message(&mut wire).await.0, b'2');
        assert_error(message(&mut wire).await, b"57014");
        wire.write_all(&frame(b'S', b"")).await.unwrap();
        assert_eq!(message(&mut wire).await, (b'Z', b"E".to_vec()));

        query(&mut wire, "ROLLBACK").await;
        assert_eq!(until_ready(&mut wire).await, [(b'C', b"ROLLBACK\0".to_vec()), (b'Z', b"I".to_vec())]);
        assert_eq!(admin.sql_transactions_live(), 0);
        admin.set_commit_sync_hook(None);
        reservation.release(false);
        drop(wire);
        finish(path, admin, service, running, client, connection).await;
    }

    #[derive(Clone, Copy)]
    enum Rejection {
        Batch,
        Admission,
        SimpleQuery,
        ExtendedParse,
        MalformedComment,
        UnknownColumn,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rejected_simple_batch_returns_ready_with_ack_held() {
        rejection_with_ack_held(Rejection::Batch).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn simple_batch_admission_refusal_returns_ready_with_ack_held() {
        rejection_with_ack_held(Rejection::Admission).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rejected_simple_prepare_returns_ready_with_ack_held() {
        rejection_with_ack_held(Rejection::SimpleQuery).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rejected_extended_parse_returns_ready_with_ack_held() {
        rejection_with_ack_held(Rejection::ExtendedParse).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rejected_parse_precheck_returns_ready_with_ack_held() {
        rejection_with_ack_held(Rejection::MalformedComment).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rejected_parse_description_returns_ready_with_ack_held() {
        rejection_with_ack_held(Rejection::UnknownColumn).await;
    }

    async fn rejection_with_ack_held(rejection: Rejection) {
        tokio::time::timeout(Duration::from_secs(30), reject_with_ack_held(rejection))
            .await.expect("statement rejection fixture timed out");
    }

    async fn reject_with_ack_held(rejection: Rejection) {
        let (path, admin, service, running, client, connection) = fixture().await;
        tokio::time::timeout(Duration::from_secs(10), client.batch_execute(
            "CREATE SEQUENCE rejected_seq;
             CREATE TABLE rejected_insert(id bigint PRIMARY KEY DEFAULT nextval('rejected_seq'));
             CREATE TABLE admission_hold(id int PRIMARY KEY, v int);
             INSERT INTO admission_hold VALUES (7, 0);
             SELECT setval('rejected_seq', 2)",
        )).await.unwrap().unwrap();
        let (mut wire, cancel_key) = connect_cancellable(running.address).await;
        query(&mut wire, "SET statement_timeout = 0").await;
        assert_eq!(until_ready(&mut wire).await.last().unwrap().1, b"I");
        query(&mut wire, "BEGIN").await;
        assert_eq!(until_ready(&mut wire).await.last().unwrap().1, b"T");

        let reservation = giggles_core::CommitSyncHook::new();
        let _release = ReleaseOnDrop(reservation.clone());
        admin.set_commit_sync_hook(Some(reservation.clone()));
        wire.write_all(&extended_execute("INSERT INTO rejected_insert DEFAULT VALUES")).await.unwrap();
        entered(&reservation).await;
        wait_until(|| service.durable_tracker().live() == 1, "Execute reservation wait").await;
        cancel(running.address, &cancel_key).await;
        assert_eq!(message(&mut wire).await.0, b'1');
        assert_eq!(message(&mut wire).await.0, b'2');
        assert_error(message(&mut wire).await, b"57014");
        wire.write_all(&frame(b'S', b"")).await.unwrap();
        assert_eq!(message(&mut wire).await, (b'Z', b"E".to_vec()));

        let holder = if matches!(rejection, Rejection::Admission) {
            service.set_role_statement_limits(RoleStatementLimits {
                concurrent: 1, queue: 0, wait_ms: 10,
            }).unwrap();
            // The commit hook is held, so take the row lock without committing.
            tokio::time::timeout(Duration::from_secs(5), client.batch_execute(&format!("BEGIN; {ROW_LOCK}")))
                .await.unwrap().unwrap();
            Some(hold_role(&service, &running).await)
        } else {
            None
        };
        let (sql, extended, idle, code) = match rejection {
            Rejection::Batch => ("BEGIN; SELECT 1", false, false, b"25P02"),
            Rejection::Admission => ("ROLLBACK; BEGIN", false, false, b"53000"),
            Rejection::SimpleQuery => ("SELECT 1 FROM rejected_insert", false, false, b"25P02"),
            Rejection::ExtendedParse => ("SELECT 1 FROM rejected_insert", true, false, b"25P02"),
            Rejection::MalformedComment => ("COMMENT ON TABLE", true, true, b"42601"),
            Rejection::UnknownColumn => ("SELECT nonexistent_column FROM rejected_insert", true, true, b"42703"),
        };
        let send = async |wire: &mut Wire, sql: &str| {
            if extended {
                let mut parse = frame(b'P', format!("\0{sql}\0\0\0").as_bytes());
                parse.extend(frame(b'S', b""));
                wire.write_all(&parse).await.unwrap();
            } else {
                query(wire, sql).await;
            }
            until_ready(wire).await
        };
        let replies = tokio::time::timeout(Duration::from_secs(2), async {
            if idle {
                query(&mut wire, "ROLLBACK").await;
                assert_eq!(until_ready(&mut wire).await, [(b'C', b"ROLLBACK\0".to_vec()), (b'Z', b"I".to_vec())]);
            }
            if matches!(rejection, Rejection::SimpleQuery | Rejection::ExtendedParse) {
                // The bare SELECT uses the catalog route. Check it, then use an
                // application table so the second statement reaches core prepare.
                let replies = send(&mut wire, "SELECT 1").await;
                assert_eq!(replies.len(), 2);
                assert_error(replies[0].clone(), b"25P02");
                assert_eq!(replies[1], (b'Z', b"E".to_vec()));
            }
            send(&mut wire, sql).await
        }).await;
        admin.set_commit_sync_hook(None);
        reservation.release(false);
        drop(wire);
        if let Some(holder) = holder {
            release_role(&running, holder).await;
        }
        tokio::time::timeout(Duration::from_secs(10),
            finish(path, admin, service, running, client, connection),
        ).await.unwrap();
        let replies = replies.expect("a rejected statement waited for the held reservation");
        if matches!(rejection, Rejection::Admission) {
            assert_eq!(replies.len(), 3);
            assert_eq!(replies[0], (b'C', b"ROLLBACK\0".to_vec()));
            assert_error(replies[1].clone(), b"53000");
            assert_eq!(replies[2], (b'Z', b"I".to_vec()));
        } else {
            assert_eq!(replies.len(), 2);
            assert_error(replies[0].clone(), code);
            assert_eq!(replies[1], (b'Z', vec![if idle { b'I' } else { b'E' }]));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn terminate_releases_session_after_execute_timeout_with_held_ack() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute(
            "CREATE SEQUENCE terminate_seq;
             CREATE TABLE terminate_insert(id bigint PRIMARY KEY DEFAULT nextval('terminate_seq'));
             SELECT setval('terminate_seq', 2)",
        ).await.unwrap();
        let mut wire = connect(running.address).await;
        query(&mut wire, "SET statement_timeout = '2s'").await;
        assert_eq!(until_ready(&mut wire).await.last().unwrap().1, b"I");
        let reservation = giggles_core::CommitSyncHook::new();
        let _release = ReleaseOnDrop(reservation.clone());
        admin.set_commit_sync_hook(Some(reservation.clone()));

        let mut bytes = extended_execute("INSERT INTO terminate_insert DEFAULT VALUES RETURNING id");
        bytes.extend(frame(b'S', b""));
        wire.write_all(&bytes).await.unwrap();
        entered(&reservation).await;
        assert_eq!(message(&mut wire).await.0, b'1');
        assert_eq!(message(&mut wire).await.0, b'2');
        assert_error(message(&mut wire).await, b"57014");
        let ready = message(&mut wire).await;
        assert_eq!(ready, (b'Z', b"I".to_vec()));

        wire.write_all(&frame(b'X', b"")).await.unwrap();
        let eof = tokio::time::timeout(Duration::from_secs(2), wire.read(&mut [0])).await;
        admin.set_commit_sync_hook(None);
        reservation.release(false);
        assert_eq!(eof.expect("Terminate did not close while the reservation was held").unwrap(), 0);
        assert!(client.query("SELECT id FROM terminate_insert", &[]).await.unwrap().is_empty());
        let after_release: i64 = client.query_one("SELECT nextval('terminate_seq')", &[]).await.unwrap().get(0);
        assert!(after_release > 3);
        drop(wire);
        close_fixture(&admin, &service, running, client, connection).await;
        drop(service);
        drop(admin);
        let reopened = Admin::open(&path, Limits::default()).unwrap();
        let clock = giggles_core::relational::StatementClock {
            unavailable: None, transaction_us: 100, statement_us: 100, zone: None, wall: None,
        };
        let rows = reopened.sql_session()
            .execute_at("SELECT nextval('terminate_seq')", &[], None, &clock).unwrap().rows;
        assert!(matches!(rows[0].values[0], giggles_core::relational::Value::Int(value) if value > after_release));
        drop(reopened);
        remove_fixture(&path).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn implicit_sync_cancel_after_release_keeps_success_and_durable_row() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute("CREATE TABLE cancel_sync(id int PRIMARY KEY)").await.unwrap();
        let (mut wire, cancel_key) = connect_cancellable(running.address).await;
        let hook = giggles_core::CommitSyncHook::new();
        let _release = ReleaseOnDrop(hook.clone());
        admin.set_commit_sync_hook(Some(hook.clone()));
        let mut bytes = extended_execute("INSERT INTO cancel_sync VALUES (1)");
        bytes.extend(frame(b'S', b""));
        wire.write_all(&bytes).await.unwrap();
        entered(&hook).await;
        released(&service).await;
        cancel(running.address, &cancel_key).await;
        hook.release(false);

        let replies = until_ready(&mut wire).await;
        assert_eq!(replies, [(b'1', vec![]), (b'2', vec![]),
            (b'C', b"INSERT 0 1\0".to_vec()), (b'Z', b"I".to_vec())]);
        drop(wire);
        close_fixture(&admin, &service, running, client, connection).await;
        drop(service);
        drop(admin);
        assert_only_first_row_after_reopen(&path, "SELECT id FROM cancel_sync").await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn extended_sync_releases_and_pipeline_order_is_preserved() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute("CREATE TABLE extended_tail(id int PRIMARY KEY)").await.unwrap();
        hold_row_lock(&client).await;
        let mut a = connect(running.address).await;
        let hook = giggles_core::CommitSyncHook::new(); let _release = ReleaseOnDrop(hook.clone()); admin.set_commit_sync_hook(Some(hook.clone()));
        let mut parse = b"\0INSERT INTO extended_tail VALUES (1)\0".to_vec(); parse.extend_from_slice(&0u16.to_be_bytes());
        let mut bytes = frame(b'P', &parse);
        bytes.extend(frame(b'B', b"\0\0\0\0\0\0\0\0"));
        bytes.extend(frame(b'E', b"\0\0\0\0\0")); bytes.extend(frame(b'S', b""));
        bytes.extend(frame(b'Q', b"SELECT 42\0"));
        a.write_all(&bytes).await.unwrap(); entered(&hook).await; released(&service).await;
        let holder = hold_role(&service, &running).await;
        hook.release(false);
        let first = until_ready(&mut a).await;
        assert_eq!(first.iter().map(|m| m.0).collect::<Vec<_>>(), b"12CZ");
        release_role(&running, holder).await;
        let second = until_ready(&mut a).await;
        assert_eq!(second.iter().map(|m| m.0).collect::<Vec<_>>(), b"TDCZ");
        drop(a); finish(path, admin, service, running, client, connection).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pipelined_extended_cycles_reply_before_next_role_admission() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute("CREATE TABLE pipeline_tail(id int PRIMARY KEY)").await.unwrap();
        hold_row_lock(&client).await;
        let mut wire = connect(running.address).await;
        let hook = giggles_core::CommitSyncHook::new();
        let _release = ReleaseOnDrop(hook.clone());
        admin.set_commit_sync_hook(Some(hook.clone()));
        let mut bytes = extended_execute("INSERT INTO pipeline_tail VALUES (1)");
        bytes.extend(frame(b'S', b""));
        bytes.extend(extended_execute("INSERT INTO pipeline_tail VALUES (2)"));
        bytes.extend(frame(b'S', b""));
        wire.write_all(&bytes).await.unwrap();
        entered(&hook).await;
        released(&service).await;
        let holder = hold_role(&service, &running).await;
        hook.release(false);

        let first = until_ready(&mut wire).await;
        assert_eq!(first.iter().map(|reply| reply.0).collect::<Vec<_>>(), b"12CZ");
        assert_eq!(first[2].1, b"INSERT 0 1\0");
        assert_eq!(first[3].1, b"I");
        queued(&service).await;
        assert_only_first_pipeline_row(&admin);
        release_role(&running, holder).await;

        let second = until_ready(&mut wire).await;
        assert_eq!(second.iter().map(|reply| reply.0).collect::<Vec<_>>(), b"12CZ");
        assert_eq!(second[2].1, b"INSERT 0 1\0");
        assert_eq!(second[3].1, b"I");
        assert_eq!(client.query("SELECT id FROM pipeline_tail", &[]).await.unwrap().len(), 2);
        drop(wire);
        finish(path, admin, service, running, client, connection).await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn released_commit_does_not_readmit_behind_running_statement() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute("CREATE TABLE no_readmit(id int PRIMARY KEY)").await.unwrap();
        service.set_role_statement_limits(giggles_server::admission::RoleStatementLimits { concurrent: 1, queue: 0, wait_ms: 10 }).unwrap();
        let mut a = connect(running.address).await;
        hold_row_lock(&client).await;
        let hook = giggles_core::CommitSyncHook::new(); let _release = ReleaseOnDrop(hook.clone()); admin.set_commit_sync_hook(Some(hook.clone()));
        query(&mut a, "INSERT INTO no_readmit VALUES (1)").await; entered(&hook).await; released(&service).await;
        let b = hold_role(&service, &running).await;
        hook.release(false);
        let responses = until_ready(&mut a).await;
        assert!(responses.iter().any(|m| m.0 == b'C'));
        assert!(!responses.iter().any(|m| m.0 == b'E'));
        drop(a); release_role(&running, b).await; finish(path, admin, service, running, client, connection).await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn simple_batches_keep_admission_through_commit() {
        for (setup, sql, expected) in [
            (Some("BEGIN; INSERT INTO pipeline_tail VALUES (1)"), "COMMIT; SELECT 1", b"CTDCZ"),
            (None, "INSERT INTO pipeline_tail VALUES (1); SELECT 1", b"CTDCZ"),
        ] {
            let (path, admin, service, running, client, connection) = saturated_fixture().await;
            let mut a = connect(running.address).await;
            if let Some(setup) = setup {
                query(&mut a, setup).await;
                assert_eq!(until_ready(&mut a).await.last().unwrap().1, b"T");
            }
            let hook = giggles_core::CommitSyncHook::new();
            let _release = ReleaseOnDrop(hook.clone());
            admin.set_commit_sync_hook(Some(hook.clone()));
            query(&mut a, sql).await;
            entered(&hook).await;
            assert_eq!(service.blocking_permits_held(), 1);
            let waiters = saturate_blocking_pool(&service, &running, 1).await;

            hook.release(false);
            let replies = until_ready(&mut a).await;
            assert_eq!(replies.iter().map(|m| m.0).collect::<Vec<_>>(), expected);
            assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 0);
            assert_only_first_pipeline_row(&admin);

            drop(a);
            drop(waiters);
            finish(path, admin, service, running, client, connection).await;
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancel_after_release_keeps_success_and_durable_row() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute("CREATE TABLE cancel_tail(id int PRIMARY KEY)").await.unwrap();
        let (a, driver) = tokio_postgres::Config::new().hostaddr(running.address.ip()).port(running.address.port())
            .user("owner").dbname("postgres").connect(tokio_postgres::NoTls).await.unwrap();
        let driver = tokio::spawn(driver); let cancel = a.cancel_token();
        let hook = giggles_core::CommitSyncHook::new(); let _release = ReleaseOnDrop(hook.clone()); admin.set_commit_sync_hook(Some(hook.clone()));
        let job = tokio::spawn(async move { a.batch_execute("INSERT INTO cancel_tail VALUES (1)").await });
        entered(&hook).await; released(&service).await;
        cancel.cancel_query(tokio_postgres::NoTls).await.unwrap();
        assert!(!job.is_finished()); hook.release(false);
        job.await.unwrap().unwrap();
        admin.set_commit_sync_hook(None);
        let rows = client.query("SELECT * FROM cancel_tail", &[]).await.unwrap();
        assert_eq!(rows.len(), 1);
        driver.await.unwrap().unwrap();
        finish(path, admin, service, running, client, connection).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn explicit_commit_in_simple_query_keeps_admission() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute("CREATE TABLE pipeline_tail(id int PRIMARY KEY)").await.unwrap();
        let mut a = connect(running.address).await;
        query(&mut a, "BEGIN; INSERT INTO pipeline_tail VALUES (1)").await;
        assert_eq!(until_ready(&mut a).await.last().unwrap().1, b"T");
        let hook = giggles_core::CommitSyncHook::new();
        let _release = ReleaseOnDrop(hook.clone());
        admin.set_commit_sync_hook(Some(hook.clone()));
        query(&mut a, "COMMIT").await;
        entered(&hook).await;
        assert_eq!(service.blocking_permits_held(), 1);
        hook.release(false);

        assert_eq!(until_ready(&mut a).await, [(b'C', b"COMMIT\0".to_vec()), (b'Z', b"I".to_vec())]);
        assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 0);
        assert_only_first_pipeline_row(&admin);
        drop(a);
        finish(path, admin, service, running, client, connection).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn explicit_execute_flushes_under_saturation_without_releasing() {
        let (path, admin, service, running, client, connection) = saturated_fixture().await;
        let mut a = connect(running.address).await;
        prepare_commit(&mut a, false).await;
        let hook = giggles_core::CommitSyncHook::new();
        let _release = ReleaseOnDrop(hook.clone());
        admin.set_commit_sync_hook(Some(hook.clone()));
        let mut bytes = frame(b'E', EXECUTE_UNNAMED);
        bytes.extend(frame(b'H', b""));
        a.write_all(&bytes).await.unwrap();
        entered(&hook).await;
        assert_eq!(service.blocking_permits_held(), 1);
        let waiters = saturate_blocking_pool(&service, &running, 1).await;

        hook.release(false);
        assert_eq!(message(&mut a).await, (b'C', b"COMMIT\0".to_vec()));
        assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 0);
        a.write_all(&frame(b'S', b"")).await.unwrap();
        assert_eq!(until_ready(&mut a).await, [(b'Z', b"I".to_vec())]);
        assert_only_first_pipeline_row(&admin);

        drop(waiters);
        drop(a);
        finish(path, admin, service, running, client, connection).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn buffered_sync_commits_after_disconnect_during_execute() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute("CREATE TABLE pipeline_tail(id int PRIMARY KEY)").await.unwrap();
        let mut a = connect(running.address).await;
        let mut prepare = frame(b'P', b"i\0INSERT INTO pipeline_tail VALUES (1)\0\0\0");
        prepare.extend(frame(b'B', BIND_INSERT));
        prepare.extend(frame(b'H', b""));
        a.write_all(&prepare).await.unwrap();
        assert_eq!(message(&mut a).await.0, b'1');
        assert_eq!(message(&mut a).await.0, b'2');

        let entered = Arc::new(tokio::sync::Notify::new());
        let gate_entered = entered.clone();
        let (allow, released) = std::sync::mpsc::channel();
        let released = std::sync::Mutex::new(released);
        service.set_statement_gate(Arc::new(move || {
            gate_entered.notify_one();
            released.lock().unwrap().recv_timeout(Duration::from_secs(5))
                .map_err(|error| error.to_string())
        })).unwrap_or_else(|_| panic!("gate already installed"));
        let mut bytes = frame(b'E', EXECUTE_UNNAMED);
        bytes.extend(frame(b'S', b""));
        a.write_all(&bytes).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), entered.notified()).await.unwrap();

        a.shutdown().await.unwrap();
        let eof = tokio::time::timeout(Duration::from_secs(5), a.read_u8()).await.unwrap().unwrap_err();
        assert_eq!(eof.kind(), std::io::ErrorKind::UnexpectedEof);
        allow.send(()).unwrap();

        wait_until(|| service.pgwire_blocking_live() == 0, "detached batch retired").await;
        assert_only_first_pipeline_row(&admin);
        assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 1);
        idle(&service);
        drop(a);
        finish(path, admin, service, running, client, connection).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn released_commit_holds_same_row_lock_until_durable() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute("CREATE TABLE locked_tail(id int PRIMARY KEY, n int); INSERT INTO locked_tail VALUES (1, 0)").await.unwrap();
        let mut a = connect(running.address).await;
        let mut b = connect(running.address).await;
        let hook = giggles_core::CommitSyncHook::new();
        let _release = ReleaseOnDrop(hook.clone());
        admin.set_commit_sync_hook(Some(hook.clone()));
        query(&mut a, "UPDATE locked_tail SET n = 1 WHERE id = 1").await;
        entered(&hook).await;
        released(&service).await;

        query(&mut b, "UPDATE locked_tail SET n = 2 WHERE id = 1").await;
        wait_until(|| admin.waiting_lock_count() == 1, "second update waiting on row lock").await;
        admin.set_commit_sync_hook(None);
        hook.release(false);
        assert_eq!(message(&mut a).await, (b'C', b"UPDATE 1\0".to_vec()));
        assert_eq!(message(&mut b).await, (b'C', b"UPDATE 1\0".to_vec()));
        let row = client.query_one("SELECT n FROM locked_tail", &[]).await.unwrap();
        assert_eq!(row.get::<_, i32>(0), 2);
        drop(a);
        drop(b);
        finish(path, admin, service, running, client, connection).await;
    }

    fn assert_only_first_pipeline_row(admin: &Admin) {
        let result = admin.sql_session().execute("SELECT id FROM pipeline_tail", &[], None).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values, [giggles_core::relational::Value::Int(1)]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disconnect_cancels_a_queued_pipeline_readmission() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute("CREATE TABLE pipeline_tail(id int PRIMARY KEY)").await.unwrap();
        hold_row_lock(&client).await;
        let mut a = connect(running.address).await;
        prepare_commit(&mut a, true).await;
        let hook = giggles_core::CommitSyncHook::new();
        let _release = ReleaseOnDrop(hook.clone());
        admin.set_commit_sync_hook(Some(hook.clone()));
        let mut bytes = frame(b'E', EXECUTE_UNNAMED);
        bytes.extend(frame(b'B', BIND_INSERT));
        bytes.extend(frame(b'E', EXECUTE_UNNAMED));
        bytes.extend(frame(b'S', b""));
        a.write_all(&bytes).await.unwrap();
        entered(&hook).await;
        assert_eq!(service.blocking_permits_held(), 1);
        let holder = hold_role(&service, &running).await;

        hook.release(false);
        queued(&service).await;
        assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 0);
        a.write_all(&frame(b'X', b"")).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !service.role_pool_counts().contains(&(0, 1, 0))
                || service.pgwire_blocking_live() != 1
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await.expect("disconnected role waiter retired while the permit was still occupied");
        assert_only_first_pipeline_row(&admin);

        release_role(&running, holder).await;
        drop(a);
        finish(path, admin, service, running, client, connection).await;
    }


    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn extended_copy_retains_admission_through_sync_commit() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute("CREATE TABLE copy_tail(id int PRIMARY KEY)").await.unwrap();
        let mut a = connect(running.address).await;
        let mut bytes = frame(b'P', b"i\0COPY copy_tail FROM STDIN\0\0\0");
        bytes.extend(frame(b'B', BIND_INSERT));
        bytes.extend(frame(b'E', EXECUTE_UNNAMED));
        a.write_all(&bytes).await.unwrap();
        for tag in b"12G" {
            assert_eq!(message(&mut a).await.0, *tag);
        }
        let hook = giggles_core::CommitSyncHook::new();
        let _release = ReleaseOnDrop(hook.clone());
        admin.set_commit_sync_hook(Some(hook.clone()));
        let mut bytes = frame(b'd', b"1\n");
        bytes.extend(frame(b'c', b""));
        bytes.extend(frame(b'S', b""));
        a.write_all(&bytes).await.unwrap();
        entered(&hook).await;
        assert_eq!(service.blocking_permits_held(), 1);
        assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 0);
        hook.release(false);
        let replies = until_ready(&mut a).await;
        assert_eq!(replies.iter().map(|reply| reply.0).collect::<Vec<_>>(), b"CZ");

        admin.set_commit_sync_hook(None);
        // The next transaction on the same connection is release-eligible.
        query(&mut a, "INSERT INTO copy_tail VALUES (2)").await;
        assert!(!until_ready(&mut a).await.iter().any(|reply| reply.0 == b'E'));
        assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 1);
        drop(a);
        finish(path, admin, service, running, client, connection).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn simple_batch_retains_admission_after_draining_returning_spill() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute("CREATE TABLE spill_tail(id int PRIMARY KEY)").await.unwrap();
        let mut a = connect(running.address).await;
        let hook = giggles_core::CommitSyncHook::new();
        let _release = ReleaseOnDrop(hook.clone());
        admin.set_commit_sync_hook(Some(hook.clone()));
        let ids = (1..=256).map(|id| format!("({id})")).collect::<Vec<_>>().join(",");
        query(&mut a, &format!("BEGIN; INSERT INTO spill_tail VALUES {ids} RETURNING repeat('x', 8192); COMMIT")).await;
        entered(&hook).await;
        assert_eq!(service.blocking_permits_held(), 1);
        assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 0);
        hook.release(false);
        let replies = until_ready(&mut a).await;
        assert!(!replies.iter().any(|reply| reply.0 == b'E'));
        assert_eq!(replies.iter().filter(|reply| reply.0 == b'D').count(), 256);
        assert_eq!(replies[replies.len() - 2], (b'C', b"COMMIT\0".to_vec()));

        admin.set_commit_sync_hook(None);
        query(&mut a, "INSERT INTO spill_tail VALUES (257)").await;
        assert!(!until_ready(&mut a).await.iter().any(|reply| reply.0 == b'E'));
        assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 1);
        drop(a);
        finish(path, admin, service, running, client, connection).await;
    }

    fn returning_spill_insert() -> String {
        let ids = (1..=256).map(|id| format!("({id})")).collect::<Vec<_>>().join(",");
        format!("INSERT INTO spill_tail VALUES {ids} RETURNING repeat('x', 8192)")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_autocommit_retains_admission_for_returning_spill() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute("CREATE TABLE spill_tail(id int PRIMARY KEY)").await.unwrap();
        let mut wire = connect(running.address).await;
        let hook = giggles_core::CommitSyncHook::new();
        let _release = ReleaseOnDrop(hook.clone());
        admin.set_commit_sync_hook(Some(hook.clone()));
        query(&mut wire, &returning_spill_insert()).await;
        entered(&hook).await;
        assert_eq!(service.blocking_permits_held(), 1);
        assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 0);
        hook.release(false);

        let replies = until_ready(&mut wire).await;
        assert_eq!(replies.iter().filter(|reply| reply.0 == b'D').count(), 256);
        assert_eq!(replies[replies.len() - 2], (b'C', b"INSERT 0 256\0".to_vec()));
        assert!(!replies.iter().any(|reply| reply.0 == b'E'));
        assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 0);
        drop(wire);
        finish(path, admin, service, running, client, connection).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn extended_sync_retains_admission_after_closing_returning_spill() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute("CREATE TABLE spill_tail(id int PRIMARY KEY)").await.unwrap();
        let mut wire = connect(running.address).await;
        let mut bytes = extended_execute(&returning_spill_insert());
        bytes.extend(frame(b'H', b""));
        wire.write_all(&bytes).await.unwrap();
        for tag in b"12" {
            assert_eq!(message(&mut wire).await.0, *tag);
        }
        for _ in 0..256 {
            assert_eq!(message(&mut wire).await.0, b'D');
        }
        assert_eq!(message(&mut wire).await, (b'C', b"INSERT 0 256\0".to_vec()));

        let hook = giggles_core::CommitSyncHook::new();
        let _release = ReleaseOnDrop(hook.clone());
        admin.set_commit_sync_hook(Some(hook.clone()));
        // Remove the portal's spill handle so only the transaction flag can guard Sync.
        let mut bytes = frame(b'C', b"P\0");
        bytes.extend(frame(b'S', b""));
        wire.write_all(&bytes).await.unwrap();
        entered(&hook).await;
        assert_eq!(service.blocking_permits_held(), 1);
        assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 0);
        hook.release(false);

        assert_eq!(until_ready(&mut wire).await, [(b'3', vec![]), (b'Z', b"I".to_vec())]);
        assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 0);
        drop(wire);
        finish(path, admin, service, running, client, connection).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn application_role_retains_admission_in_both_protocols() {
        use giggles_server::{Binding, Role};

        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute("CREATE TABLE app_tail(id int PRIMARY KEY)").await.unwrap();
        let fingerprint = "12".repeat(32);
        service.reload_bindings(vec![Binding {
            fingerprint: fingerprint.clone(),
            role: Role::Application {
                domain: admin.authority_metadata().unwrap().0.to_string(),
                tables: ["app_tail".into()].into(),
            },
        }]).unwrap();
        let session = service.session(&fingerprint).unwrap();
        let (mut wire, mut server) = tokio::io::duplex(4096);
        let worker = tokio::spawn(async move { giggles_server::pgwire::run(&mut server, session).await });
        startup(&mut wire, "giggles", "giggles").await;
        for (sql, extended, expected) in [
            ("INSERT INTO app_tail VALUES (1)", false, b"CZ".as_slice()),
            ("INSERT INTO app_tail VALUES (2)", true, b"12CZ".as_slice()),
        ] {
            let hook = giggles_core::CommitSyncHook::new();
            let _release = ReleaseOnDrop(hook.clone());
            admin.set_commit_sync_hook(Some(hook.clone()));
            if extended {
                let mut bytes = extended_execute(sql);
                bytes.extend(frame(b'S', b""));
                wire.write_all(&bytes).await.unwrap();
            } else {
                query(&mut wire, sql).await;
            }
            entered(&hook).await;
            assert_eq!(service.blocking_permits_held(), 1);
            assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 0);
            hook.release(false);
            let replies = until_ready(&mut wire).await;
            assert_eq!(replies.iter().map(|reply| reply.0).collect::<Vec<_>>(), expected);
            assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 0);
        }
        assert_eq!(client.query("SELECT id FROM app_tail", &[]).await.unwrap().len(), 2);
        wire.write_all(&frame(b'X', b"")).await.unwrap();
        worker.await.unwrap().unwrap();
        finish(path, admin, service, running, client, connection).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replica_simple_query_releases_at_implicit_commit() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute("CREATE TABLE replica_parent(id int PRIMARY KEY); \
            CREATE TABLE replica_child(id int PRIMARY KEY REFERENCES replica_parent(id))").await.unwrap();
        let mut wire = connect(running.address).await;
        query(&mut wire, "INSERT INTO replica_child VALUES (1)").await;
        let mut replies = until_ready(&mut wire).await;
        assert_error(replies.remove(0), b"23503");
        query(&mut wire, "SET session_replication_role = replica").await;
        assert!(!until_ready(&mut wire).await.iter().any(|reply| reply.0 == b'E'));

        let hook = giggles_core::CommitSyncHook::new();
        let _release = ReleaseOnDrop(hook.clone());
        admin.set_commit_sync_hook(Some(hook.clone()));
        query(&mut wire, "INSERT INTO replica_child VALUES (1)").await;
        entered(&hook).await;
        released(&service).await;
        assert_eq!(service.blocking_permits_held(), 0);
        assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], 1);
        hook.release(false);

        assert_eq!(until_ready(&mut wire).await, [(b'C', b"INSERT 0 1\0".to_vec()), (b'Z', b"I".to_vec())]);
        assert_eq!(client.query("SELECT id FROM replica_child", &[]).await.unwrap().len(), 1);
        drop(wire);
        finish(path, admin, service, running, client, connection).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn leadership_change_while_waiting_for_initial_role_admission_refuses_query() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let (path, admin, service, running, client, connection) = fixture().await;
        hold_row_lock(&client).await;
        let holder = hold_role(&service, &running).await;
        let leader = Arc::new(AtomicBool::new(true));
        let calls = Arc::new(AtomicUsize::new(0));
        service.set_statement_gate(Arc::new({
            let leader = leader.clone();
            let calls = calls.clone();
            move || {
                calls.fetch_add(1, Ordering::Relaxed);
                if leader.load(Ordering::Acquire) { Ok(()) } else { Err("leadership lost".into()) }
            }
        })).unwrap_or_else(|_| panic!("gate already installed"));

        let mut simple = connect(running.address).await;
        query(&mut simple, "SELECT 1").await;
        queued(&service).await;
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        leader.store(false, Ordering::Release);
        release_role(&running, holder).await;

        assert_error(message(&mut simple).await, b"57P01");
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        drop(simple);
        finish(path, admin, service, running, client, connection).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_and_buffered_serializable_cursor_keep_admission() {
        for copy in [true, false] {
            let (path, admin, service, running, client, connection) = fixture().await;
            client.batch_execute("CREATE TABLE held_tail(id int PRIMARY KEY)").await.unwrap();
            let mut a = connect(running.address).await;
            let sql = if copy {
                let input = path.join("input.txt");
                std::fs::write(&input, "1\n").unwrap();
                format!("COPY held_tail FROM '{}'", input.to_string_lossy().replace('\\', "/").replace('\'', "''"))
            } else {
                query(&mut a, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE").await;
                assert!(!until_ready(&mut a).await.iter().any(|m| m.0 == b'E'));
                "SELECT count(*) FROM held_tail".to_owned()
            };
            let before = service.durability_metrics()["durable_releases_total"]["local"].clone();
            let hook = giggles_core::CommitSyncHook::new(); let _release = ReleaseOnDrop(hook.clone());
            admin.set_commit_sync_hook(Some(hook.clone()));
            query(&mut a, &sql).await;
            let waiting = hook.clone();
            if !tokio::task::spawn_blocking(move || waiting.wait_entered(Duration::from_secs(5))).await.unwrap() {
                panic!("no Sync for {sql}: {:?}", until_ready(&mut a).await);
            }
            assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], before);
            assert_eq!(service.blocking_permits_held(), 1);
            {
                let b = client.simple_query("SELECT 1"); tokio::pin!(b);
                tokio::select! {
                    result = &mut b => panic!("query completed before held operation: {result:?}"),
                    () = queued(&service) => {}
                }
                hook.release(false);
                assert!(!until_ready(&mut a).await.iter().any(|m| m.0 == b'E'));
                b.await.unwrap();
            }
            drop(a); finish(path, admin, service, running, client, connection).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn update_and_delete_autocommit_release() {
        for sql in ["UPDATE mutations SET id = 2 WHERE id = 1", "DELETE FROM mutations WHERE id = 1"] {
            let (path, admin, service, running, client, connection) = fixture().await;
            client.batch_execute("CREATE TABLE mutations(id int PRIMARY KEY); INSERT INTO mutations VALUES (1)").await.unwrap();
            let mut a = connect(running.address).await;
            let before = service.durability_metrics()["durable_releases_total"]["local"].as_u64().unwrap();
            let hook = giggles_core::CommitSyncHook::new(); let _release = ReleaseOnDrop(hook.clone());
            admin.set_commit_sync_hook(Some(hook.clone()));
            query(&mut a, sql).await; entered(&hook).await; released(&service).await;
            assert_eq!(service.durability_metrics()["durable_releases_total"]["local"], before + 1);
            client.simple_query("SELECT 1").await.unwrap(); hook.release(false);
            let replies = until_ready(&mut a).await;
            assert!(replies.iter().any(|m| m.0 == b'C'));
            assert!(!replies.iter().any(|m| m.0 == b'E'));
            drop(a); finish(path, admin, service, running, client, connection).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn extended_sync_failure_has_error_before_ready_and_no_row() {
        let (path, admin, service, running, client, connection) = fixture().await;
        client.batch_execute("CREATE TABLE failed_tail(id int PRIMARY KEY)").await.unwrap();
        let mut a = connect(running.address).await;
        let hook = giggles_core::CommitSyncHook::new(); let _release = ReleaseOnDrop(hook.clone());
        admin.set_commit_sync_hook(Some(hook.clone()));
        let mut bytes = frame(b'P', b"\0INSERT INTO failed_tail VALUES (1)\0\0\0");
        bytes.extend(frame(b'B', b"\0\0\0\0\0\0\0\0"));
        bytes.extend(frame(b'E', b"\0\0\0\0\0")); bytes.extend(frame(b'S', b""));
        a.write_all(&bytes).await.unwrap(); entered(&hook).await; released(&service).await;
        hook.release(true);
        let responses = until_ready(&mut a).await;
        // Execute's CommandComplete predates Sync and is existing protocol
        // behavior. Sync adds only ErrorResponse then ReadyForQuery on failure.
        assert_eq!(responses.iter().map(|m| m.0).collect::<Vec<_>>(), b"12CEZ");
        drop(a); admin.set_commit_sync_hook(None); drop(client); connection.await.unwrap().unwrap();
        running.shutdown().await.unwrap(); service.drain().await.unwrap(); idle(&service);
        drop(service); drop(admin);
        let reopened = Admin::open(&path, Limits::default()).unwrap();
        assert!(reopened.sql_session().execute("SELECT * FROM failed_tail", &[], None).unwrap().rows.is_empty());
        drop(reopened); remove_fixture(&path).await;
    }

}
