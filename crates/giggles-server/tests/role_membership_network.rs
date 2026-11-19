//! R3-109 (GD-0061): CREATE ROLE / ALTER ROLE ... TASKONLY over a real pgwire client.
#![cfg(feature = "engine-rocksdb")]
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, scram::Verifier, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Client, Config, NoTls, config::SslMode};

static SERIAL: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    pg: Running,
    admin: Admin,
    _service: Arc<Service>,
}
impl Fixture {
    async fn new(engine: Engine) -> Self {
        let root = std::path::PathBuf::from(
            std::env::var_os("TEMP").expect("set task TEMP under J:/gigglesdb-work"),
        );
        let path = root.join(format!(
            "role-membership-network-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create_engine(path.join("db.redb"), Limits::default(), engine).unwrap();
        for (name, superuser, createrole) in [
            ("owner", true, false),
            ("manager", false, true),
            ("plain", false, false),
        ] {
            admin
                .roles()
                .create(NewRole {
                    inherit: None,
                    name: name.into(),
                    login: true,
                    superuser,
                    createrole,
                    password: Some(Verifier::generate(&format!("{name}-pw"))),
                })
                .unwrap();
        }
        let service = Service::new(admin.clone(), vec![], Clock::new(0, 3600).unwrap()).unwrap();
        let config = PostgresqlConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            tls: TlsMode::Disabled,
            certificate: None,
            private_key: None,
            client_ca: None,
            certificates: BTreeMap::new(),
            databases: vec!["postgres".into()],
            loopback_trust: false,
            connections: 16,
        };
        let listener = config.build().unwrap();
        let pg = serve_postgresql(config.listen, service.clone(), Arc::new(listener))
            .await
            .unwrap();
        Self {
            pg,
            admin,
            _service: service,
        }
    }
    async fn connect(&self, user: &str) -> Client {
        self.connect_notices(user).await.0
    }
    async fn connect_notices(&self, user: &str) -> (Client, tokio::sync::mpsc::UnboundedReceiver<tokio_postgres::error::DbError>) {
        let mut config = Config::new();
        config
            .host("localhost")
            .hostaddr(self.pg.address.ip())
            .port(self.pg.address.port())
            .user(user)
            .password(format!("{user}-pw"))
            .dbname("postgres")
            .ssl_mode(SslMode::Disable);
        let (client, mut connection) = config.connect(NoTls).await.unwrap();
        let (send, notices) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(message) = std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                if let Ok(tokio_postgres::AsyncMessage::Notice(notice)) = message { let _ = send.send(notice); }
            }
        });
        (client, notices)
    }
}


async fn run(client: &Client, sql: &str) -> Result<(), tokio_postgres::Error> {
    for statement in sql.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        client.simple_query(statement).await?;
    }
    Ok(())
}
async fn code(client: &Client, sql: &str, expected: &str) {
    let error = client.simple_query(sql).await.expect_err(sql);
    assert_eq!(error.code().unwrap().code(), expected, "{sql}: {error}");
}
#[tokio::test]
async fn membership_and_effective_identity() {
    let f = Fixture::new(Engine::V2).await;
    let owner = f.connect("owner").await;
    run(&owner, "CREATE ROLE grp; CREATE ROLE usr IN ROLE grp; CREATE TABLE t(id integer); INSERT INTO t VALUES (1); GRANT SELECT ON t TO grp; SET ROLE usr").await.unwrap();
    let row = owner.query_one("SELECT current_user, session_user", &[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "usr");
    assert_eq!(row.get::<_, String>(1), "owner");
    assert_eq!(owner.query("SELECT * FROM t", &[]).await.unwrap().len(), 1);
    let row = owner.query_one("SELECT pg_has_role(current_user, 'grp', 'MEMBER'), pg_has_role(current_user, 'grp', 'USAGE'), pg_has_role(current_user, 'grp', 'SET')", &[]).await.unwrap();
    assert!(row.get::<_, bool>(0)); assert!(row.get::<_, bool>(1)); assert!(row.get::<_, bool>(2));
    code(&owner, "SELECT pg_has_role(current_user, 'no_such_role', 'MEMBER')", "42704").await;
    code(&owner, "SELECT pg_has_role(current_user, 'grp', 'bogus')", "22023").await;
    code(&owner, "CREATE ROLE denied", "42501").await;
    run(&owner, "RESET ROLE; REVOKE grp FROM usr; SET ROLE usr").await.unwrap();
    code(&owner, "SELECT * FROM t", "42501").await;
    run(&owner, "RESET ROLE; DROP TABLE t; DROP ROLE usr, grp").await.unwrap();
    f.pg.shutdown().await.unwrap();
}
#[tokio::test]
async fn role_transactions_and_task_only() {
    let f = Fixture::new(Engine::V2).await;
    let owner = f.connect("owner").await;
    run(&owner, "CREATE ROLE a; CREATE ROLE b; CREATE ROLE task TASKONLY; SET ROLE a; BEGIN; SET ROLE b; ROLLBACK").await.unwrap();
    assert_eq!(owner.query_one("SHOW role", &[]).await.unwrap().get::<_, String>(0), "a");
    run(&owner, "BEGIN; SET LOCAL ROLE b; COMMIT").await.unwrap();
    assert_eq!(owner.query_one("SHOW role", &[]).await.unwrap().get::<_, String>(0), "a");
    run(&owner, "RESET ROLE").await.unwrap();
    code(&owner, "SET ROLE task", "42501").await;
    code(&owner, "SET SESSION AUTHORIZATION task", "42501").await;
    code(&owner, "SET ROLE unknown_role", "22023").await;
    run(&owner, "SET SESSION AUTHORIZATION a").await.unwrap();
    let row = owner.query_one("SELECT current_user, session_user", &[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "a"); assert_eq!(row.get::<_, String>(1), "a");
    run(&owner, "RESET SESSION AUTHORIZATION").await.unwrap();
    let plain = f.connect("plain").await;
    code(&plain, "SET ROLE a", "42501").await;
    code(&plain, "SET SESSION AUTHORIZATION a", "42501").await;
    assert!(f.admin.roles().get("task").unwrap().unwrap().task_only);
    f.pg.shutdown().await.unwrap();
}

#[tokio::test]
async fn copy_privileges_follow_inherit_and_effective_superuser() {
    let f = Fixture::new(Engine::V2).await;
    let owner = f.connect("owner").await;
    run(&owner, "CREATE ROLE reader; CREATE ROLE child IN ROLE reader; CREATE ROLE nochild NOINHERIT IN ROLE reader; CREATE TABLE c(id integer); GRANT INSERT ON c TO PUBLIC; GRANT pg_read_server_files TO reader").await.unwrap();
    owner.execute("SET ROLE child", &[]).await.unwrap();
    code(&owner, "COPY c FROM 'J:/gigglesdb-work/pgparity/roles/does-not-exist-b1'", "58P01").await;
    owner.execute("SET ROLE nochild", &[]).await.unwrap();
    code(&owner, "COPY c FROM 'J:/gigglesdb-work/pgparity/roles/does-not-exist-b1'", "42501").await;
    run(&owner, "RESET ROLE; GRANT CREATE ON SCHEMA public TO reader; SET ROLE child; CREATE TABLE owned(id integer); RESET ROLE").await.unwrap();
    assert_eq!(f.admin.roles().table_owner("owned").unwrap().as_deref(), Some("child"));
    f.pg.shutdown().await.unwrap();
}

#[tokio::test]
async fn schema_usage_and_inherited_create() {
    let f = Fixture::new(Engine::V2).await;
    let owner = f.connect("owner").await;
    run(&owner, "CREATE ROLE parent; CREATE ROLE child IN ROLE parent; CREATE SCHEMA custom; CREATE TABLE custom.t(id integer); INSERT INTO custom.t VALUES (1); GRANT SELECT ON custom.t TO PUBLIC; SET ROLE child").await.unwrap();
    code(&owner, "SELECT * FROM custom.t", "42501").await;
    run(&owner, "RESET ROLE; GRANT USAGE ON SCHEMA custom TO PUBLIC; GRANT CREATE ON SCHEMA custom TO parent; SET ROLE child").await.unwrap();
    assert_eq!(owner.query("SELECT * FROM custom.t", &[]).await.unwrap().len(), 1);
    owner.execute("CREATE VIEW custom.v AS SELECT id FROM custom.t", &[]).await.unwrap();
    f.pg.shutdown().await.unwrap();
}

#[tokio::test]
async fn ordinary_session_authorization_self_and_defaults() {
    let f = Fixture::new(Engine::V2).await;
    let plain = f.connect("plain").await;
    for sql in ["RESET SESSION AUTHORIZATION", "SET SESSION AUTHORIZATION DEFAULT", "SET SESSION AUTHORIZATION plain"] {
        plain.simple_query(sql).await.unwrap();
        plain.execute(sql, &[]).await.unwrap();
    }
    run(&plain, "BEGIN; SET SESSION AUTHORIZATION plain; ROLLBACK").await.unwrap();
    let row = plain.query_one("SELECT current_user, session_user", &[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "plain");
    assert_eq!(row.get::<_, String>(1), "plain");
    for error in [
        plain.simple_query("SET SESSION AUTHORIZATION owner").await.unwrap_err(),
        plain.execute("SET SESSION AUTHORIZATION owner", &[]).await.unwrap_err(),
    ] {
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "42501");
        assert_eq!(error.message(), "permission denied to set session authorization \"owner\"");
    }
    f.pg.shutdown().await.unwrap();
}
#[tokio::test]
async fn restore_rechecks_set_permission_and_oid() {
    let f = Fixture::new(Engine::V2).await;
    let owner = f.connect("owner").await;
    let (plain, mut notices) = f.connect_notices("plain").await;
    for boundary in ["ROLLBACK", "ROLLBACK TO SAVEPOINT s", "COMMIT"] {
        run(&owner, "CREATE ROLE restore_target; CREATE ROLE local_target; GRANT restore_target TO plain").await.unwrap();
        run(&plain, "SET ROLE restore_target; BEGIN; SAVEPOINT s").await.unwrap();
        if boundary == "COMMIT" { run(&plain, "SET LOCAL ROLE plain").await.unwrap(); }
        else { run(&plain, "RESET ROLE").await.unwrap(); }
        run(&owner, "REVOKE SET OPTION FOR restore_target FROM plain").await.unwrap();
        plain.simple_query(boundary).await.unwrap();
        assert_eq!(plain.query_one("SHOW role", &[]).await.unwrap().get::<_, String>(0), "none", "{boundary}");
        assert!(std::iter::from_fn(|| notices.try_recv().ok()).any(|notice|
            notice.code().code() == "01000" && notice.message() == "role \"restore_target\" can no longer be set; role reset"));
        run(&plain, "ROLLBACK").await.unwrap();
        run(&owner, "GRANT restore_target TO plain WITH SET TRUE").await.unwrap();
        run(&plain, "SET ROLE restore_target; BEGIN; SAVEPOINT s").await.unwrap();
        if boundary == "COMMIT" { run(&plain, "SET LOCAL ROLE plain").await.unwrap(); }
        else { run(&plain, "RESET ROLE").await.unwrap(); }
        run(&owner, "DROP ROLE restore_target; CREATE ROLE restore_target SUPERUSER; GRANT restore_target TO plain").await.unwrap();
        plain.batch_execute(boundary).await.unwrap();
        assert_eq!(plain.query_one("SHOW role", &[]).await.unwrap().get::<_, String>(0), "none", "replacement {boundary}");
        assert!(std::iter::from_fn(|| notices.try_recv().ok()).any(|notice|
            notice.code().code() == "01000" && notice.message() == "role \"restore_target\" can no longer be set; role reset"));
        run(&plain, "ROLLBACK").await.unwrap();
        run(&owner, "DROP ROLE restore_target, local_target").await.unwrap();
    }
    f.pg.shutdown().await.unwrap();
}

#[tokio::test]
async fn session_authorization_restore_checks_oid() {
    let f = Fixture::new(Engine::V2).await;
    let owner = f.connect("owner").await;
    let (session, mut notices) = f.connect_notices("owner").await;
    run(&owner, "CREATE ROLE saved_auth").await.unwrap();
    run(&session, "SET SESSION AUTHORIZATION saved_auth; BEGIN; RESET SESSION AUTHORIZATION").await.unwrap();
    run(&owner, "DROP ROLE saved_auth; CREATE ROLE saved_auth SUPERUSER").await.unwrap();
    session.batch_execute("ROLLBACK").await.unwrap();
    let row = session.query_one("SELECT current_user, session_user", &[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "owner");
    assert_eq!(row.get::<_, String>(1), "owner");
    assert!(std::iter::from_fn(|| notices.try_recv().ok()).any(|notice| notice.code().code() == "01000"
        && notice.message() == "role \"saved_auth\" can no longer be set; role reset"));
    run(&owner, "DROP ROLE saved_auth").await.unwrap();
    f.pg.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_promoted_grantor_preserves_named_identity() {
    let f = Fixture::new(Engine::V2).await;
    let owner = f.connect("owner").await;
    run(&owner, "CREATE ROLE c2g; CREATE ROLE c2q; CREATE ROLE c2u; GRANT c2g TO c2q WITH ADMIN TRUE; SET ROLE c2q; GRANT c2g TO c2u WITH ADMIN TRUE; RESET ROLE; GRANT c2g TO c2u; ALTER ROLE c2q SUPERUSER; GRANT c2g TO c2u WITH ADMIN FALSE, SET FALSE GRANTED BY c2q").await.unwrap();
    let edges = f.admin.roles().edges().unwrap();
    let edge = edges.iter().find(|e| e.role == "c2g" && e.member == "c2u" && e.grantor == "c2q").unwrap();
    assert!(!edge.admin && !edge.set);
    assert_eq!(edges.iter().filter(|e| e.role == "c2g" && e.member == "c2u").count(), 2);
    run(&owner, "REVOKE c2g FROM c2u GRANTED BY c2q").await.unwrap();
    let edges = f.admin.roles().edges().unwrap();
    let remaining: Vec<_> = edges.iter().filter(|e| e.role == "c2g" && e.member == "c2u").collect();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].grantor, giggles_core::roles::BOOTSTRAP_GRANTOR);
    run(&owner, "GRANT c2g TO c2u WITH SET FALSE GRANTED BY c2q").await.unwrap();
    assert!(f.admin.roles().edges().unwrap().iter().any(|e| e.role == "c2g" && e.member == "c2u" && e.grantor == "c2q" && !e.set));
    f.pg.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_revoke_missing_named_edge_preserves_bootstrap() {
    let f = Fixture::new(Engine::V2).await;
    let owner = f.connect("owner").await;
    run(&owner, "CREATE ROLE c3g; CREATE ROLE c3q; CREATE ROLE c3u; GRANT c3g TO c3q WITH ADMIN TRUE; SET ROLE c3q; GRANT c3g TO c3u WITH ADMIN TRUE; RESET ROLE; GRANT c3g TO c3u WITH ADMIN TRUE; ALTER ROLE c3q SUPERUSER").await.unwrap();
    run(&owner, "REVOKE ADMIN OPTION FOR c3g FROM c3u GRANTED BY c3q").await.unwrap();
    let edges = f.admin.roles().edges().unwrap();
    assert!(edges.iter().any(|e| e.role == "c3g" && e.member == "c3u" && e.grantor == giggles_core::roles::BOOTSTRAP_GRANTOR && e.admin));
    assert!(edges.iter().any(|e| e.role == "c3g" && e.member == "c3u" && e.grantor == "c3q" && !e.admin));
    run(&owner, "REVOKE c3g FROM c3u GRANTED BY c3q").await.unwrap();
    drop(owner);
    let (owner, mut notices) = f.connect_notices("owner").await;
    let before = f.admin.roles().edges().unwrap();
    for sql in ["REVOKE c3g FROM c3u GRANTED BY c3q", "REVOKE ADMIN OPTION FOR c3g FROM c3u GRANTED BY c3q", "REVOKE SET OPTION FOR c3g FROM c3u GRANTED BY c3q", "REVOKE INHERIT OPTION FOR c3g FROM c3u GRANTED BY c3q"] {
        let messages = owner.simple_query(sql).await.unwrap();
        assert!(messages.iter().any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_))));
        let notice = notices.recv().await.unwrap();
        assert_eq!(notice.code().code(), "01000");
        assert_eq!(notice.message(), "role \"c3u\" has not been granted membership in role \"c3g\" by role \"c3q\"");
        assert_eq!(f.admin.roles().edges().unwrap(), before);
    }
    // Explicit bootstrap uses the logical bootstrap edge, like PG's postgres.
    run(&owner, "REVOKE ADMIN OPTION FOR c3g FROM c3u GRANTED BY owner").await.unwrap();
    assert!(f.admin.roles().edges().unwrap().iter().any(|e| e.role == "c3g" && e.member == "c3u" && e.grantor == giggles_core::roles::BOOTSTRAP_GRANTOR && !e.admin));
    run(&owner, "REVOKE c3g FROM c3u GRANTED BY owner; REVOKE c3g FROM c3u GRANTED BY owner").await.unwrap();
    assert_eq!(notices.recv().await.unwrap().message(), "role \"c3u\" has not been granted membership in role \"c3g\" by role \"owner\"");
    assert!(!f.admin.roles().is_member("c3u", "c3g").unwrap());
    f.pg.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_superuser_grant_requires_admin_except_bootstrap() {
    let f = Fixture::new(Engine::V2).await;
    let (owner, mut notices) = f.connect_notices("owner").await;
    run(&owner, "CREATE ROLE c3g; CREATE ROLE c3q SUPERUSER; CREATE ROLE c3u").await.unwrap();
    let before = f.admin.roles().edges().unwrap();
    for sql in ["GRANT c3g TO c3u GRANTED BY c3q", "SET ROLE c3q"] {
        if sql.starts_with("SET") { run(&owner, sql).await.unwrap(); }
        else {
            let error = owner.simple_query(sql).await.unwrap_err();
            let error = error.as_db_error().unwrap();
            assert_eq!(error.code().code(), "42501");
            assert_eq!(error.message(), "permission denied to grant privileges as role \"c3q\"");
            assert_eq!(error.detail(), Some("The grantor must have the ADMIN option on role \"c3g\"."));
        }
    }
    code(&owner, "GRANT c3g TO c3u GRANTED BY CURRENT_USER", "42501").await;
    assert_eq!(f.admin.roles().edges().unwrap(), before);
    // Unlike GRANT, REVOKE by a named superuser needs no ADMIN.
    run(&owner, "REVOKE c3g FROM c3u GRANTED BY c3q").await.unwrap();
    assert_eq!(notices.recv().await.unwrap().code().code(), "01000");
    run(&owner, "GRANT c3g TO c3u; RESET ROLE").await.unwrap();
    assert!(f.admin.roles().edges().unwrap().iter().any(|e| e.role == "c3g" && e.member == "c3u" && e.grantor == giggles_core::roles::BOOTSTRAP_GRANTOR));
    run(&owner, "REVOKE c3g FROM c3u; GRANT c3g TO c3u GRANTED BY owner").await.unwrap();
    let edges = f.admin.roles().edges().unwrap();
    let grants: Vec<_> = edges.iter().filter(|e| e.role == "c3g" && e.member == "c3u").collect();
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0].grantor, giggles_core::roles::BOOTSTRAP_GRANTOR);
    f.pg.shutdown().await.unwrap();
}

#[tokio::test]
async fn executor_refusal_precedes_superuser_switch_and_membership_write() {
    let f = Fixture::new(Engine::V2).await;
    let owner = f.connect("owner").await;
    run(&owner, "CREATE ROLE executor TASKONLY").await.unwrap();
    let before = f.admin.roles().edges().unwrap();
    for name in ["owner", "manager", "plain"] {
        let client = f.connect(name).await;
        for sql in ["SET ROLE executor", "SET SESSION AUTHORIZATION executor",
            "GRANT executor TO owner", "GRANT executor TO plain WITH ADMIN OPTION"] {
            let expected = if sql.starts_with("GRANT") && name == "owner" { "22023" } else { "42501" };
            code(&client, sql, expected).await;
            assert_eq!(client.query_one("SELECT current_user", &[]).await.unwrap().get::<_, String>(0), name);
            assert_eq!(f.admin.roles().edges().unwrap(), before);
        }
    }
    f.pg.shutdown().await.unwrap();
}
