//! R3-203 (GD-0061 section 7): task-only roles are refused on every ordinary login path.
#![cfg(feature = "engine-rocksdb")]
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Binding, Role, Running, Service,
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

fn admin() -> Admin {
    let root = std::path::PathBuf::from(
        std::env::var_os("TEMP").expect("set task TEMP under J:/gigglesdb-work"),
    );
    let path = root.join(format!(
        "task-only-roles-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        SERIAL.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).unwrap();
    let admin = Admin::create_engine(path.join("db.redb"), Limits::default(), Engine::V2).unwrap();
    for (name, superuser) in [
        ("owner", true),
        ("repair_agent", false),
        ("oncall_lead", false),
        ("incident_controller", false),
    ] {
        admin
            .roles()
            .create(NewRole {
                name: name.into(),
                login: true,
                superuser,
                password: Some(Verifier::generate(&format!("{name}-pw"))),
                ..NewRole::default()
            })
            .unwrap();
    }
    admin.roles().set_task_only("repair_agent", true).unwrap();
    admin
}

async fn serve(service: Arc<Service>, loopback_trust: bool) -> Running {
    let config = PostgresqlConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: TlsMode::Disabled,
        certificate: None,
        private_key: None,
        client_ca: None,
        certificates: BTreeMap::new(),
        databases: vec!["postgres".into()],
        loopback_trust,
        connections: 16,
    };
    let listener = config.build().unwrap();
    serve_postgresql(config.listen, service, Arc::new(listener))
        .await
        .unwrap()
}

fn config(pg: &Running, user: &str, password: &str) -> Config {
    let mut config = Config::new();
    config
        .host("localhost")
        .hostaddr(pg.address.ip())
        .port(pg.address.port())
        .user(user)
        .password(password)
        .dbname("postgres")
        .ssl_mode(SslMode::Disable);
    config
}
async fn connect(pg: &Running, user: &str) -> Client {
    let (client, connection) = config(pg, user, &format!("{user}-pw"))
        .connect(NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}
async fn refused(pg: &Running, user: &str, password: &str) {
    let e = match config(pg, user, password).connect(NoTls).await {
        Ok(_) => panic!("{user}: unexpected login"),
        Err(e) => e,
    };
    assert_eq!(e.code().expect("SQLSTATE").code(), "28P01", "{e}");
}
async fn error(client: &Client, sql: &str, expected: &str) {
    let e = client.simple_query(sql).await.unwrap_err();
    assert_eq!(e.code().expect("SQLSTATE").code(), expected, "{sql}: {e}");
}

#[tokio::test]
async fn scram_login_refused_like_bad_password() {
    let admin = admin();
    let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
    let pg = serve(service, false).await;
    refused(&pg, "repair_agent", "repair_agent-pw").await;
    refused(&pg, "repair_agent", "wrong").await;
    connect(&pg, "oncall_lead").await;
}

#[tokio::test]
async fn certificate_map_and_loopback_trust_refused() {
    let admin = admin();
    let service = Service::new(admin.clone(), vec![], Clock::new(0, 3600).unwrap()).unwrap();
    // Loopback trust skips the password; role_session still refuses task-only roles.
    let pg = serve(service.clone(), true).await;
    refused(&pg, "repair_agent", "").await;
    connect(&pg, "oncall_lead").await;
    // The certificate map ends in the same role_session lookup.
    assert!(service.role_session("repair_agent").is_err());
    assert!(service.role_session("oncall_lead").is_ok());
}

#[tokio::test]
async fn ordinary_binding_to_task_only_role_rejected_at_load_and_reload() {
    let admin = admin();
    let binding = |name: &str| Binding {
        fingerprint: "a".repeat(64),
        role: Role::Database { name: name.into() },
    };
    assert!(
        Service::new(
            admin.clone(),
            vec![binding("repair_agent")],
            Clock::new(0, 3600).unwrap()
        )
        .is_err()
    );
    let service = Service::new(
        admin.clone(),
        vec![binding("oncall_lead")],
        Clock::new(0, 3600).unwrap(),
    )
    .unwrap();
    assert!(service.session(&"a".repeat(64)).is_ok());
    assert!(service.reload_bindings(vec![binding("repair_agent")]).is_err());
    // Becoming task-only after load fails the binding at connect.
    admin.roles().set_task_only("oncall_lead", true).unwrap();
    assert!(service.session(&"a".repeat(64)).is_err());
}

#[tokio::test]
async fn alter_role_task_only_cuts_live_session() {
    let admin = admin();
    let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
    let pg = serve(service, false).await;
    let owner = connect(&pg, "owner").await;
    let lead = connect(&pg, "oncall_lead").await;
    lead.simple_query("SELECT 1").await.unwrap();
    owner
        .simple_query("ALTER ROLE oncall_lead TASKONLY")
        .await
        .unwrap();
    assert!(lead.simple_query("SELECT 1").await.is_err());
    refused(&pg, "oncall_lead", "oncall_lead-pw").await;
}

#[tokio::test]
async fn set_role_set_session_authorization_and_set_config_role_fail() {
    let admin = admin();
    let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
    let pg = serve(service, false).await;
    let lead = connect(&pg, "oncall_lead").await;
    for sql in [
        "SET ROLE repair_agent",
        "SET SESSION AUTHORIZATION repair_agent",
        "SELECT set_config('role', 'repair_agent', false)",
    ] {
        assert!(lead.simple_query(sql).await.is_err(), "{sql}");
    }
    let row = lead.simple_query("SELECT current_user").await.unwrap();
    let user = row.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_owned),
        _ => None,
    });
    assert_eq!(user.as_deref(), Some("oncall_lead"));
}

#[tokio::test]
async fn startup_options_role_refused() {
    let admin = admin();
    let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
    let pg = serve(service, false).await;
    let mut c = config(&pg, "oncall_lead", "oncall_lead-pw");
    c.options("-c role=repair_agent");
    if let Ok((client, connection)) = c.connect(NoTls).await {
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let rows = client.simple_query("SELECT current_user").await.unwrap();
        let user = rows.iter().find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_owned),
            _ => None,
        });
        assert_eq!(user.as_deref(), Some("oncall_lead"));
    }
}

#[tokio::test]
async fn task_only_role_cannot_become_superuser_owner_or_member() {
    let admin = admin();
    let service = Service::new(admin.clone(), vec![], Clock::new(0, 3600).unwrap()).unwrap();
    let pg = serve(service, false).await;
    let owner = connect(&pg, "owner").await;
    error(&owner, "ALTER ROLE repair_agent SUPERUSER", "42P17").await;
    error(&owner, "ALTER ROLE repair_agent CREATEROLE", "42P17").await;
    owner
        .simple_query("CREATE TABLE incidents (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    // Owning anything is refused by the catalog's D6 check.
    assert!(
        admin
            .roles()
            .set_table_owner("incidents", "repair_agent")
            .is_err()
    );
    error(&owner, "ALTER ROLE owner TASKONLY", "42P17").await;
    let roles = admin.roles().get("repair_agent").unwrap().unwrap();
    assert!(roles.task_only && !roles.superuser && !roles.createrole);
}
