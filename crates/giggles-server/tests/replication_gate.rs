use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Request, Running, Service, StatementGate,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage};

struct Fixture {
    service: Arc<Service>,
    leader: Arc<AtomicBool>,
    pg: Running,
    path: std::path::PathBuf,
}
impl Fixture {
    async fn new() -> Self {
        static NEXT_FIXTURE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "replication-gate-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "owner".into(),
                superuser: true,
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
        let leader = Arc::new(AtomicBool::new(true));
        let state = leader.clone();
        let gate: StatementGate = Arc::new(move || {
            if state.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err("not leader".into())
            }
        });
        assert!(service.set_statement_gate(gate.clone()).is_ok());
        // Installation is one-shot; a caller cannot replace the leadership check.
        assert!(service.set_statement_gate(gate).is_err());
        let config = PostgresqlConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            tls: TlsMode::Disabled,
            certificate: None,
            private_key: None,
            client_ca: None,
            certificates: BTreeMap::new(),
            databases: vec!["postgres".into()],
            loopback_trust: true,
            connections: 8,
        };
        let pg = serve_postgresql(
            config.listen,
            service.clone(),
            Arc::new(config.build().unwrap()),
        )
        .await
        .unwrap();
        Self {
            service,
            leader,
            pg,
            path,
        }
    }
    async fn client(&self) -> Client {
        let (client, connection) = Config::new()
            .host("127.0.0.1")
            .port(self.pg.address.port())
            .user("owner")
            .dbname("postgres")
            .connect(NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }
    async fn stop(self) {
        self.pg.shutdown().await.unwrap();
        drop(self.service);
        std::fs::remove_dir_all(self.path).unwrap();
    }
}
fn assert_fatal(error: tokio_postgres::Error) {
    let error = error
        .as_db_error()
        .unwrap_or_else(|| panic!("server ErrorResponse: {error:?}"));
    assert_eq!(error.code().code(), "57P01");
    assert_eq!(error.severity(), "FATAL");
    assert_eq!(
        error.parsed_severity(),
        Some(tokio_postgres::error::Severity::Fatal)
    );
    assert_eq!(
        error.message(),
        "terminating connection: this node is not the replication leader"
    );
}

#[tokio::test]
async fn simple_query_stops_after_leadership_loss() {
    let f = Fixture::new().await;
    let client = f.client().await;
    let rows = client.simple_query("SELECT 1").await.unwrap();
    assert!(rows.iter().any(|message| matches!(message,
        SimpleQueryMessage::Row(row) if row.get(0) == Some("1"))));
    f.leader.store(false, Ordering::SeqCst);
    // Err carries no result rows, even though the SQL itself succeeds on a leader.
    assert_fatal(client.simple_query("SELECT 1").await.unwrap_err());
    assert!(client.simple_query("SELECT 1").await.is_err());
    drop(client);
    f.stop().await;
}

#[tokio::test]
async fn extended_query_rejects_a_fresh_connection_on_a_follower() {
    let f = Fixture::new().await;
    f.leader.store(false, Ordering::SeqCst);
    let client = f.client().await;
    assert_fatal(client.query("SELECT $1::int4", &[&1i32]).await.unwrap_err());
    assert!(client.simple_query("SELECT 1").await.is_err());
    drop(client);
    f.stop().await;
}

#[tokio::test]
async fn native_request_checks_the_gate() {
    let f = Fixture::new().await;
    let mut session = f.service.role_session("owner").unwrap();
    assert_eq!(
        session.handle_received(Request::Ping, Instant::now()).code,
        "ok"
    );
    f.leader.store(false, Ordering::SeqCst);
    let response = session.handle_received(Request::Ping, Instant::now());
    assert_eq!(response.code, "57P01");
    assert!(response.sql.is_none());
    drop(session);
    f.stop().await;
}
