#![cfg(feature = "engine-rocksdb")]
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{Running, Service, auth::{PostgresqlConfig, TlsMode, serve_postgresql}, clock::Clock};
use std::{collections::BTreeMap, sync::Arc};
use tokio_postgres::{Client, Config, NoTls};
struct Fixture { service: Arc<Service>, pg: Running, path: std::path::PathBuf }
impl Fixture {
    async fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("l1c-wire-{}-{}-{}", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)));
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        for (name, superuser) in [("owner", true), ("reader", false)] {
            admin.roles().create(NewRole { name: name.into(), superuser, login: true, ..Default::default() }).unwrap();
        }
        let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
        let config = PostgresqlConfig { listen: "127.0.0.1:0".parse().unwrap(), tls: TlsMode::Disabled,
            certificate: None, private_key: None, client_ca: None, certificates: BTreeMap::new(),
            databases: vec!["postgres".into()], loopback_trust: true, connections: 8 };
        let listener = Arc::new(config.build().unwrap());
        let pg = serve_postgresql(config.listen, service.clone(), listener).await.unwrap();
        Self { service, pg, path }
    }
    async fn client(&self, name: &str) -> Client {
        let (client, connection) = Config::new().hostaddr(self.pg.address.ip()).port(self.pg.address.port())
            .user(name).dbname("postgres").connect(NoTls).await.unwrap();
        tokio::spawn(async move { let _ = connection.await; }); client
    }
    async fn stop(self) { self.pg.shutdown().await.unwrap(); drop(self.service); std::fs::remove_dir_all(self.path).unwrap(); }
}
#[tokio::test]
async fn readonly_privileges_and_wait_diagnostics() {
    let f = Fixture::new().await; let a = f.client("owner").await; let b = f.client("owner").await;
    a.simple_query("CREATE TABLE lk(id int PRIMARY KEY,v int); INSERT INTO lk VALUES(1,0)").await.unwrap();
    for mode in ["KEY SHARE", "SHARE", "NO KEY UPDATE", "UPDATE"] {
        a.simple_query("BEGIN READ ONLY").await.unwrap();
        a.simple_query(&format!("SELECT 1 FOR {mode}")).await.unwrap();
        let e = a.simple_query(&format!("SELECT id FROM lk FOR {mode}")).await.unwrap_err();
        let e = e.as_db_error().unwrap(); assert_eq!(e.code().code(), "25006");
        assert_eq!(e.message(), format!("cannot execute SELECT FOR {mode} in a read-only transaction"));
        a.simple_query("ROLLBACK").await.unwrap();
    }
    a.simple_query("BEGIN; SELECT id FROM lk FOR UPDATE").await.unwrap();
    let e = b.simple_query("SELECT id FROM lk FOR UPDATE NOWAIT").await.unwrap_err();
    let e = e.as_db_error().unwrap(); assert_eq!(e.code().code(), "55P03");
    assert_eq!(e.message(), "could not obtain lock on row in relation \"lk\"");
    b.simple_query("SET lock_timeout='40ms'").await.unwrap();
    let e = b.query("SELECT id FROM lk FOR SHARE", &[]).await.unwrap_err();
    let e = e.as_db_error().unwrap(); assert_eq!(e.code().code(), "55P03");
    assert_eq!(e.message(), "canceling statement due to lock timeout");
    a.simple_query("ROLLBACK").await.unwrap();
    let reader = f.client("reader").await;
    a.simple_query("GRANT SELECT ON lk TO reader").await.unwrap();
    for mode in ["KEY SHARE", "SHARE", "NO KEY UPDATE", "UPDATE"] {
        let e = reader.simple_query(&format!("SELECT id FROM lk FOR {mode}")).await.unwrap_err();
        assert_eq!(e.code().unwrap().code(), "42501");
        assert_eq!(e.as_db_error().unwrap().message(), "permission denied for table lk");
    }
    a.simple_query("GRANT UPDATE ON lk TO reader").await.unwrap();
    reader.simple_query("SELECT id FROM lk FOR KEY SHARE").await.unwrap();
    a.simple_query("REVOKE SELECT ON lk FROM reader").await.unwrap();
    assert_eq!(reader.simple_query("SELECT id FROM lk FOR UPDATE").await.unwrap_err().code().unwrap().code(), "42501");
    drop(reader); drop(a); drop(b); f.stop().await;
}

#[tokio::test]
async fn nested_marks_require_update_and_reject_readonly() {
    let f = Fixture::new().await; let owner = f.client("owner").await;
    owner.simple_query("CREATE TABLE lk(id int PRIMARY KEY); INSERT INTO lk VALUES(1)").await.unwrap();
    owner.simple_query("GRANT SELECT ON lk TO reader").await.unwrap();
    let reader = f.client("reader").await;
    for sql in [
        "SELECT 1 WHERE EXISTS(SELECT 1 FROM lk FOR UPDATE NOWAIT)",
        "SELECT 1 WHERE 1 IN (SELECT id FROM lk FOR UPDATE NOWAIT)",
        "SELECT (SELECT id FROM lk WHERE id=1 FOR UPDATE NOWAIT)",
    ] {
        assert_eq!(reader.simple_query(sql).await.unwrap_err().code().unwrap().code(), "42501");
        owner.simple_query("BEGIN READ ONLY").await.unwrap();
        assert_eq!(owner.simple_query(sql).await.unwrap_err().code().unwrap().code(), "25006");
        owner.simple_query("ROLLBACK").await.unwrap();
    }
    drop(reader); drop(owner); f.stop().await;
}

#[tokio::test]
async fn locking_cursors_fail_at_declare_in_both_protocols() {
    let f = Fixture::new().await;
    let c = f.client("owner").await;
    c.simple_query("CREATE TABLE lk(id int PRIMARY KEY); INSERT INTO lk VALUES(1)").await.unwrap();
    for simple in [true, false] {
        for (sql, message, detail) in [
            ("DECLARE c CURSOR WITH HOLD FOR SELECT id FROM lk FOR UPDATE",
             "DECLARE CURSOR WITH HOLD ... FOR UPDATE is not supported",
             Some("Holdable cursors must be READ ONLY.")),
            ("DECLARE c CURSOR FOR SELECT id FROM lk FOR SHARE",
             "unsupported SQL feature or parameter type in this server profile", None),
            ("DECLARE c CURSOR FOR SELECT * FROM (SELECT id FROM lk FOR UPDATE) AS q",
             "unsupported SQL feature or parameter type in this server profile", None),
            ("DECLARE c CURSOR FOR SELECT 1 FOR UPDATE",
             "unsupported SQL feature or parameter type in this server profile", None),
            ("DECLARE c SCROLL CURSOR FOR SELECT id FROM lk FOR UPDATE",
             "unsupported SQL feature or parameter type in this server profile", None),
        ] {
            c.simple_query("BEGIN").await.unwrap();
            let e = if simple {
                c.simple_query(sql).await.unwrap_err()
            } else {
                // Parse/Describe must reject DECLARE before any Execute or FETCH.
                c.prepare(sql).await.unwrap_err()
            };
            let e = e.as_db_error().unwrap();
            assert_eq!(e.code().code(), "0A000", "{sql} (simple={simple})");
            assert_eq!(e.message(), message, "{sql} (simple={simple})");
            if let Some(detail) = detail { assert_eq!(e.detail(), Some(detail)); }
            c.simple_query("ROLLBACK").await.unwrap();
        }
        c.simple_query("BEGIN").await.unwrap();
        let sql = "DECLARE c CURSOR FOR SELECT id FROM lk";
        if simple { c.simple_query(sql).await.unwrap(); }
        else { c.execute(sql, &[]).await.unwrap(); }
        let row = c.query_one("FETCH NEXT FROM c", &[]).await.unwrap();
        assert_eq!(row.get::<_, i32>(0), 1);
        c.simple_query("CLOSE c; COMMIT").await.unwrap();
    }
    drop(c);
    f.stop().await;
}
