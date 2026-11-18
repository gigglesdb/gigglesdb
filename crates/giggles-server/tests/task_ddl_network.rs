//! R3-110 (GD-0061 design-1 sections 5 and 11): task statements over a real
//! pgwire client. CREATE TASK, SHOW TASK(S), REVOKE TASK and GRANT / REVOKE
//! { UNDO | INSPECT } ON TASK run on the simple and extended protocols of the
//! ordinary PostgreSQL listener, as standalone statements like role DDL. A
//! task-only role cannot log in there, and an agent task session refuses
//! every task statement as RoleStatement.
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::{
    DAY_MS, Msg, agent_wire, error_of, now_ms, ordinary_wire, parity::inc_436_sql, timestamptz,
};
use giggles_core::{
    Admin, Engine, Limits,
    roles::NewRole,
    task::{StatementClass, TaskError},
};
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
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage, config::SslMode, types::Type};

static SERIAL: AtomicU64 = AtomicU64::new(0);

const TASK: &str = "inc-436";
const SCHEMA: &str = "CREATE TABLE shipments (merchant_id bigint, id bigint, status text, \
     failed_at timestamptz, attempt_count int, customer_email text, card_token text, \
     PRIMARY KEY (merchant_id, id))";

struct Fixture {
    pg: Running,
    admin: Admin,
    _service: Arc<Service>,
}

impl Fixture {
    /// A server with `owner` (superuser), `incident_controller` (CREATEROLE,
    /// the issuer), `oncall_lead` and `auditor`. On v2, `owner` creates
    /// `shipments` and grants the issuer SELECT and UPDATE on it.
    async fn new(engine: Engine) -> Self {
        let root = std::path::PathBuf::from(
            std::env::var_os("TEMP").expect("set task TEMP under J:/gigglesdb-work"),
        );
        let path = root.join(format!(
            "task-ddl-network-{}-{}-{}",
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
            ("incident_controller", false, true),
            ("oncall_lead", false, false),
            ("auditor", false, false),
        ] {
            admin
                .roles()
                .create(NewRole {
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
        let fixture = Self {
            pg,
            admin,
            _service: service,
        };
        if engine == Engine::V2 {
            let owner = fixture.connect("owner").await;
            ok(&owner, SCHEMA).await;
            ok(&owner, "GRANT SELECT, UPDATE ON shipments TO incident_controller").await;
        }
        fixture
    }

    async fn try_connect(&self, user: &str) -> Result<Client, tokio_postgres::Error> {
        let mut config = Config::new();
        config
            .host("localhost")
            .hostaddr(self.pg.address.ip())
            .port(self.pg.address.port())
            .user(user)
            .password(format!("{user}-pw"))
            .dbname("postgres")
            .ssl_mode(SslMode::Disable);
        let (client, connection) = config.connect(NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }

    async fn connect(&self, user: &str) -> Client {
        self.try_connect(user)
            .await
            .unwrap_or_else(|e| panic!("connect {user}: {e}"))
    }

    /// The worked example's roles (design-1 section 5): the issuer creates
    /// the task-only executor; the owner grants it the table.
    async fn executor(&self, controller: &Client) {
        ok(
            controller,
            "CREATE ROLE repair_agent LOGIN PASSWORD 'repair_agent-pw' TASKONLY",
        )
        .await;
        let owner = self.connect("owner").await;
        ok(&owner, "GRANT SELECT, UPDATE ON shipments TO repair_agent").await;
    }

    fn task(&self) -> Option<giggles_core::task::definition::TaskDef> {
        self.admin
            .tasks()
            .get(TASK)
            .unwrap()
            .map(|def| (*def).clone())
    }
}

fn expires() -> String {
    timestamptz(now_ms() + DAY_MS)
}

async fn ok(client: &Client, sql: &str) -> Vec<SimpleQueryMessage> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

async fn error(client: &Client, sql: &str, expected: &str) {
    let e = client.simple_query(sql).await.unwrap_err();
    assert_eq!(e.code().expect("SQLSTATE").code(), expected, "{sql}: {e}");
}

/// The rows of a simple-protocol result as (column -> text) maps.
fn simple_rows(messages: &[SimpleQueryMessage]) -> Vec<BTreeMap<String, Option<String>>> {
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(
                row.columns()
                    .iter()
                    .enumerate()
                    .map(|(i, c)| (c.name().to_owned(), row.get(i).map(str::to_owned)))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

fn field<'a>(row: &'a BTreeMap<String, Option<String>>, name: &str) -> Option<&'a str> {
    row.get(name)
        .unwrap_or_else(|| panic!("no column {name}"))
        .as_deref()
}

const SHOW_COLUMNS: [&str; 26] = [
    "task",
    "id",
    "def_version",
    "issuer",
    "executor",
    "approver",
    "table_name",
    "visible_rows",
    "readable",
    "masked",
    "untrusted_text",
    "write_columns",
    "old_row",
    "new_row",
    "max_distinct_rows",
    "max_transitions",
    "max_rows_read",
    "max_rows_returned",
    "max_bytes_returned",
    "limits",
    "approve_above",
    "expires_at",
    "revoked",
    "expired",
    "grants",
    "credentials",
];

#[tokio::test]
async fn worked_example_create_show_revoke_simple_protocol() {
    let f = Fixture::new(Engine::V2).await;
    let controller = f.connect("incident_controller").await;
    f.executor(&controller).await;
    ok(&controller, &inc_436_sql(&expires())).await;
    let def = f.task().expect("task created");
    assert_eq!(def.issuer, "incident_controller");
    assert_eq!(def.executor, "repair_agent");
    assert_eq!(def.approver.as_deref(), Some("oncall_lead"));
    assert!(!def.revoked);

    let shown = simple_rows(&ok(&controller, "SHOW TASK \"inc-436\"").await);
    assert_eq!(shown.len(), 1);
    let row = &shown[0];
    assert_eq!(row.keys().count(), SHOW_COLUMNS.len());
    assert_eq!(field(row, "task"), Some(TASK));
    assert_eq!(field(row, "issuer"), Some("incident_controller"));
    assert_eq!(field(row, "executor"), Some("repair_agent"));
    assert_eq!(field(row, "approver"), Some("oncall_lead"));
    assert_eq!(field(row, "table_name"), Some("shipments"));
    assert_eq!(field(row, "visible_rows"), Some(def.visible_rows.0.as_str()));
    assert_eq!(field(row, "readable"), Some("attempt_count, failed_at, id, status"));
    assert_eq!(field(row, "masked"), Some("customer_email"));
    assert_eq!(field(row, "write_columns"), Some("status"));
    assert_eq!(field(row, "max_distinct_rows"), Some("25"));
    assert_eq!(field(row, "max_rows_read"), Some("1000"));
    assert_eq!(field(row, "max_rows_returned"), Some("1000"));
    assert_eq!(field(row, "max_bytes_returned"), Some("2097152"));
    assert_eq!(field(row, "approve_above"), Some("5"));
    assert_eq!(field(row, "revoked"), Some("f"));
    assert_eq!(field(row, "expired"), Some("f"));
    assert_eq!(field(row, "grants"), Some(""));
    let listed = simple_rows(&ok(&controller, "SHOW TASKS").await);
    assert_eq!(listed.len(), 1);

    ok(&controller, "REVOKE TASK \"inc-436\"").await;
    let def = f.task().unwrap();
    assert!(def.revoked);
    let shown = simple_rows(&ok(&controller, "SHOW TASK \"inc-436\"").await);
    assert_eq!(field(&shown[0], "revoked"), Some("t"));
    assert_eq!(field(&shown[0], "def_version"), Some(def.def_version.to_string().as_str()));
    // Revoking again succeeds without a write.
    ok(&controller, "REVOKE TASK \"inc-436\"").await;
    assert_eq!(f.task().unwrap().def_version, def.def_version);
    // A malformed task statement keeps the parser's SQLSTATE.
    error(&controller, "CREATE TASK \"inc-437\" EXECUTOR", "42601").await;
}

#[tokio::test]
async fn same_over_extended_protocol_with_describe() {
    let f = Fixture::new(Engine::V2).await;
    let controller = f.connect("incident_controller").await;
    f.executor(&controller).await;

    let create = controller.prepare(&inc_436_sql(&expires())).await.unwrap();
    assert!(create.columns().is_empty());
    assert!(create.params().is_empty());
    assert_eq!(controller.execute(&create, &[]).await.unwrap(), 0);
    assert!(f.task().is_some());

    let show = controller.prepare("SHOW TASK \"inc-436\"").await.unwrap();
    let names: Vec<_> = show.columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, SHOW_COLUMNS);
    let types: Vec<_> = show.columns().iter().map(|c| c.type_().clone()).collect();
    assert_eq!(types[0], Type::TEXT);
    assert_eq!(types[1], Type::INT8);
    assert_eq!(types[14], Type::INT8);
    assert_eq!(types[21], Type::TIMESTAMPTZ);
    assert_eq!(types[22], Type::BOOL);
    assert_eq!(types[23], Type::BOOL);
    let rows = controller.query(&show, &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("task"), TASK);
    assert_eq!(rows[0].get::<_, Option<String>>("approver").as_deref(), Some("oncall_lead"));
    assert_eq!(rows[0].get::<_, i64>("max_transitions"), 25);
    assert!(!rows[0].get::<_, bool>("revoked"));
    assert!(!rows[0].get::<_, bool>("expired"));

    // SHOW TASKS over extended, in one-row portal slices.
    let list = controller.prepare("SHOW TASKS").await.unwrap();
    assert_eq!(list.columns().len(), SHOW_COLUMNS.len());
    assert_eq!(controller.query(&list, &[]).await.unwrap().len(), 1);

    controller
        .execute("REVOKE TASK \"inc-436\"", &[])
        .await
        .unwrap();
    assert!(f.task().unwrap().revoked);
    let rows = controller.query(&show, &[]).await.unwrap();
    assert!(rows[0].get::<_, bool>("revoked"));
    // Parse reports a malformed statement.
    let e = controller.prepare("SHOW TASK a b").await.unwrap_err();
    assert_eq!(e.code().unwrap().code(), "42601");
}

#[tokio::test]
async fn revoke_task_not_swallowed_by_role_parser() {
    let f = Fixture::new(Engine::V2).await;
    let controller = f.connect("incident_controller").await;
    f.executor(&controller).await;
    ok(&controller, &inc_436_sql(&expires())).await;
    // The role parser would answer 0A000 or 42601 for these; the task
    // statements answer as tasks do.
    error(&controller, "REVOKE TASK nosuch", "42704").await;
    let lead = f.connect("oncall_lead").await;
    error(&lead, "REVOKE TASK \"inc-436\"", "42501").await;
    assert!(!f.task().unwrap().revoked);
    ok(&controller, "REVOKE TASK \"inc-436\"").await;
    assert!(f.task().unwrap().revoked);
    // `REVOKE task FROM r` names a role called task: the task parser leaves
    // it to the role parser, which answers it as any other role name.
    assert!(giggles_core::task::statements::parse("REVOKE task FROM auditor").is_none());
    let owner = f.connect("owner").await;
    let code = |e: tokio_postgres::Error| e.code().map(|c| c.code().to_owned());
    let as_task = owner.simple_query("REVOKE task FROM auditor").await.map_err(code);
    let as_role = owner.simple_query("REVOKE other FROM auditor").await.map_err(code);
    assert_eq!(as_task.map(|_| ()), as_role.map(|_| ()));
    // GRANT on a table is still the role parser's.
    ok(&owner, "GRANT SELECT ON shipments TO auditor").await;
}

#[tokio::test]
async fn grant_inspect_on_task_then_show() {
    let f = Fixture::new(Engine::V2).await;
    let controller = f.connect("incident_controller").await;
    f.executor(&controller).await;
    ok(&controller, &inc_436_sql(&expires())).await;
    let auditor = f.connect("auditor").await;
    // Existence does not leak.
    error(&auditor, "SHOW TASK \"inc-436\"", "42704").await;
    assert!(simple_rows(&ok(&auditor, "SHOW TASKS").await).is_empty());
    // Only the issuer or a superuser grants.
    error(&auditor, "GRANT INSPECT ON TASK \"inc-436\" TO auditor", "42501").await;
    // Not to a task-only role.
    error(
        &controller,
        "GRANT INSPECT ON TASK \"inc-436\" TO repair_agent",
        "22023",
    )
    .await;

    ok(&controller, "GRANT INSPECT, UNDO ON TASK \"inc-436\" TO auditor").await;
    let shown = simple_rows(&ok(&auditor, "SHOW TASK \"inc-436\"").await);
    assert_eq!(shown.len(), 1);
    assert_eq!(field(&shown[0], "grants"), Some("auditor (undo, inspect)"));
    assert_eq!(simple_rows(&ok(&auditor, "SHOW TASKS").await).len(), 1);
    // The approver sees it by definition.
    let lead = f.connect("oncall_lead").await;
    assert_eq!(simple_rows(&ok(&lead, "SHOW TASKS").await).len(), 1);

    ok(&controller, "REVOKE INSPECT ON TASK \"inc-436\" FROM auditor").await;
    error(&auditor, "SHOW TASK \"inc-436\"", "42704").await;
    let shown = simple_rows(&ok(&controller, "SHOW TASK \"inc-436\"").await);
    assert_eq!(field(&shown[0], "grants"), Some("auditor (undo)"));
    // APPROVE is not grantable in R3.
    error(
        &controller,
        "GRANT APPROVE ON TASK \"inc-436\" TO auditor",
        "0A000",
    )
    .await;
}

#[tokio::test]
async fn create_task_inside_begin_25001() {
    let f = Fixture::new(Engine::V2).await;
    let controller = f.connect("incident_controller").await;
    f.executor(&controller).await;
    ok(&controller, "BEGIN").await;
    error(&controller, &inc_436_sql(&expires()), "25001").await;
    ok(&controller, "ROLLBACK").await;
    assert!(f.task().is_none());
    // The extended protocol answers the same.
    ok(&controller, "BEGIN").await;
    let e = controller
        .execute(inc_436_sql(&expires()).as_str(), &[])
        .await
        .unwrap_err();
    assert_eq!(e.code().unwrap().code(), "25001");
    ok(&controller, "ROLLBACK").await;
    assert!(f.task().is_none());
    ok(&controller, &inc_436_sql(&expires())).await;
    assert!(f.task().is_some());
}

#[tokio::test]
async fn create_task_in_multi_statement_query_25001() {
    let f = Fixture::new(Engine::V2).await;
    let controller = f.connect("incident_controller").await;
    f.executor(&controller).await;
    error(
        &controller,
        &format!("SELECT 1; {}", inc_436_sql(&expires())),
        "25001",
    )
    .await;
    error(
        &controller,
        &format!("{}; SELECT 1", inc_436_sql(&expires())),
        "25001",
    )
    .await;
    error(&controller, "SELECT 1; SHOW TASKS", "25001").await;
    assert!(f.task().is_none());
}

#[tokio::test]
async fn read_only_transaction_default_refuses_create() {
    let f = Fixture::new(Engine::V2).await;
    let controller = f.connect("incident_controller").await;
    f.executor(&controller).await;
    ok(&controller, "SET default_transaction_read_only = on").await;
    error(&controller, &inc_436_sql(&expires()), "25006").await;
    assert!(f.task().is_none());
    ok(&controller, "SET default_transaction_read_only = off").await;
    ok(&controller, &inc_436_sql(&expires())).await;
    ok(&controller, "SET default_transaction_read_only = on").await;
    // SHOW is exempt; the other writes are not.
    assert_eq!(simple_rows(&ok(&controller, "SHOW TASKS").await).len(), 1);
    error(&controller, "REVOKE TASK \"inc-436\"", "25006").await;
    error(&controller, "GRANT INSPECT ON TASK \"inc-436\" TO auditor", "25006").await;
    assert!(!f.task().unwrap().revoked);
}

#[tokio::test]
async fn legacy_server_0a000() {
    let f = Fixture::new(Engine::Legacy).await;
    let controller = f.connect("incident_controller").await;
    error(&controller, &inc_436_sql(&expires()), "0A000").await;
    error(&controller, "SHOW TASKS", "0A000").await;
    error(&controller, "SHOW TASK \"inc-436\"", "0A000").await;
    error(&controller, "REVOKE TASK \"inc-436\"", "0A000").await;
    let e = controller.prepare("SHOW TASKS").await.unwrap();
    let e = controller.query(&e, &[]).await.unwrap_err();
    assert_eq!(e.code().unwrap().code(), "0A000");
}

#[tokio::test]
#[allow(non_snake_case)]
async fn legacy_GIGGLES_STATUS_still_parses() {
    // The legacy prototype grammar (`Plan::Task`) is untouched, and the task
    // statement parser does not claim it.
    assert!(giggles_server::task_statement::TaskStatement::parse("GIGGLES STATUS ( $1 )").is_ok());
    assert!(giggles_core::task::statements::parse("GIGGLES STATUS ( $1 )").is_none());
    // A database role still gets the legacy path's 42501 for it.
    let f = Fixture::new(Engine::V2).await;
    let owner = f.connect("owner").await;
    let e = owner.prepare("GIGGLES STATUS ( $1 )").await.unwrap_err();
    assert_eq!(e.code().unwrap().code(), "42501");
}

#[tokio::test]
async fn task_only_role_and_agent_session_cannot_run_task_statements() {
    // A task-only role cannot log in on an ordinary listener (D6).
    let f = Fixture::new(Engine::V2).await;
    let controller = f.connect("incident_controller").await;
    f.executor(&controller).await;
    let Err(e) = f.try_connect("repair_agent").await else {
        panic!("a task-only role logged in on an ordinary listener");
    };
    assert_eq!(e.code().map(|c| c.code()), Some("28P01"), "{e}");

    // An agent task session refuses every task statement by class, on both
    // protocols, before anything runs.
    let (server, _agent, ordinary, mut wire) = agent_wire().await;
    let refusal = (
        "0A000".to_owned(),
        TaskError::NotAllowedInTaskSession(StatementClass::RoleStatement)
            .message()
            .to_owned(),
    );
    let create = format!(
        "CREATE TASK t2 EXECUTOR agent_exec ON shipments VISIBLE ROWS WHERE (merchant_id = 42) \
         READABLE (id) DISCLOSE AT MOST 10 ROWS READ, 100 BYTES RETURNED EXPIRES '{}'",
        expires()
    );
    for sql in [
        create.as_str(),
        "SHOW TASKS",
        "SHOW TASK \"inc-436\"",
        "REVOKE TASK \"inc-436\"",
        "GRANT INSPECT ON TASK \"inc-436\" TO oncall_lead",
        "SHOW TASK EXPOSURE \"inc-436\"",
        "CREATE TASK broken",
    ] {
        let simple = wire.simple(sql).await;
        assert_eq!(error_of(&simple), Some(refusal.clone()), "{sql}: {simple:?}");
        assert_eq!(simple.last(), Some(&Msg::Ready(b'I')), "{sql}");
        let extended = wire.parse_describe(sql, &[]).await;
        assert_eq!(error_of(&extended), Some(refusal.clone()), "{sql}: {extended:?}");
    }
    assert!(!server.admin.tasks().get("inc-436").unwrap().unwrap().revoked);
    assert!(server.admin.tasks().get("t2").unwrap().is_none());

    // The same service's ordinary listener runs them, with PostgreSQL tags.
    let (_listener, mut lead) = ordinary_wire(&server, &ordinary).await;
    let shown = lead.simple("SHOW TASKS").await;
    assert!(shown.contains(&Msg::Complete("SHOW".into())), "{shown:?}");
    assert!(
        shown
            .iter()
            .any(|m| matches!(m, Msg::Columns(c) if c.len() == SHOW_COLUMNS.len())),
        "{shown:?}"
    );
    let revoked = lead.simple("REVOKE TASK \"inc-436\"").await;
    assert_eq!(error_of(&revoked).map(|e| e.0).as_deref(), Some("42501"));
}
