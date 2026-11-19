use giggles_core::{Admin, Engine, Limits, roles::{NewRole, SchemaPrivilege}};
use giggles_server::{Service, Running, auth::{PostgresqlConfig, TlsMode, serve_postgresql}, clock::Clock};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

async fn fixture() -> (std::path::PathBuf, Admin, Arc<Service>, Running, tokio_postgres::Client, tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>) {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!("durable-{}-{}-{}", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)));
    let engine = if cfg!(feature = "engine-rocksdb") { Engine::V2 } else { Engine::Legacy };
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    admin.roles().create(NewRole { name: "owner".into(), login: true, ..Default::default() }).unwrap();
    admin.roles().grant_schema("public", "owner", &[SchemaPrivilege::Create, SchemaPrivilege::Usage]).unwrap();
    let service = Service::new(admin.clone(), vec![], Clock::new(0, 3600).unwrap()).unwrap();
    let config = PostgresqlConfig {
        listen: "127.0.0.1:0".parse().unwrap(), tls: TlsMode::Disabled, certificate: None,
        private_key: None, client_ca: None, certificates: BTreeMap::new(),
        databases: vec!["postgres".into()], loopback_trust: true, connections: 8,
    };
    let running = serve_postgresql(config.listen, service.clone(), Arc::new(config.build().unwrap())).await.unwrap();
    let (client, connection) = tokio_postgres::Config::new().hostaddr(running.address.ip()).port(running.address.port())
        .user("owner").dbname("postgres").connect(tokio_postgres::NoTls).await.unwrap();
    (path, admin, service, running, client, tokio::spawn(connection))
}
fn idle(service: &Service) {
    assert_eq!(service.pgwire_blocking_live(), 0);
    assert_eq!(service.blocking_permits_held(), 0);
    for (available, capacity, queued) in service.role_pool_counts() {
        assert_eq!(available, capacity);
        assert_eq!(queued, 0);
    }
    let metrics = service.durability_metrics();
    assert_eq!(metrics["durable_completions_live"], 0);
    for metric in ["statements_awaiting_durability", "durable_releases_total", "durable_async_total"] {
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
    let (path, admin, service, running, client, connection) = fixture().await;
    client.batch_execute("CREATE TABLE drain_rows(id int PRIMARY KEY)").await.unwrap();
    let hook = giggles_core::CommitSyncHook::new();
    struct ReleaseOnDrop(Arc<giggles_core::CommitSyncHook>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) { self.0.release(false); }
    }
    let _release = ReleaseOnDrop(hook.clone());
    admin.set_commit_sync_hook(Some(hook.clone()));
    let query = tokio::spawn(async move { client.batch_execute("INSERT INTO drain_rows VALUES (1)").await });
    let entered = { let hook = hook.clone(); tokio::task::spawn_blocking(move || hook.wait_entered(Duration::from_secs(5))).await.unwrap() };
    assert!(entered);
    assert_eq!(service.durability_metrics()["durable_completions_live"], 1);
    connection.abort();
    let _ = connection.await;
    let _ = query.await;
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
    assert_eq!(service.pgwire_blocking_live(), 1);
    let error = tokio::time::timeout(Duration::from_secs(1),
        service.drain_with_timeout(Duration::from_millis(30))).await
        .expect("durability drain must not hang").unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    assert_eq!(service.durability_metrics()["durable_completions_live"], 1);
    hook.release(false);
    shutdown.await.unwrap().unwrap();
    idle(&service);
    admin.set_commit_sync_hook(None);
    // The in-Sync group succeeded despite disconnect and the shutdown fence.
    let mut session = admin.sql_session();
    let result = session.execute("SELECT * FROM drain_rows", &[], None).unwrap();
    assert_eq!(result.rows.len(), 1);
    drop(session); drop(service); drop(admin);
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
