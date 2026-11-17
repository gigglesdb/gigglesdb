//! R3-113 (GD-0061 design-1 section 9): the native execution entry for task
//! definition. Profile 0.22 native task text runs on the ordinary HTTP API
//! for database roles (`Invocation::TaskDdl` -> `Session::execute_task_ddl`)
//! and in the embedded engine (`Engine::execute_task_text`). Both store the
//! same definition the SQL statement stores over pgwire, and a refusal has
//! the pgwire SQLSTATE.
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::{
    DAY_MS, TestCert, certificate, now_ms,
    parity::{http_call, observe_native},
    scratch, timestamptz,
};
use giggles_core::{
    Admin, Engine, Limits,
    roles::{NewRole, TablePrivilege},
    task::{definition::TaskDef, statements::TaskOutcome},
};
use giggles_server::{
    Binding, Role, Running, Service,
    api::{ApiLimits, ApiRuntime, net::serve_http_bound},
    auth::{PostgresqlConfig, TlsMode, scram::Verifier, serve_postgresql},
    clock::Clock,
    tls,
};
use rustls::pki_types::PrivatePkcs8KeyDer;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use tokio::{net::TcpListener, sync::watch};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage, config::SslMode};

const TASK: &str = "inc-436";
const CONTROLLER: &str = "incident_controller";
const LEAD: &str = "oncall_lead";
const SCHEMA: &str = "CREATE TABLE shipments (merchant_id bigint, id bigint, status text, \
     failed_at timestamptz, attempt_count int, customer_email text, card_token text, \
     PRIMARY KEY (merchant_id, id))";

/// A whole-second expiry a day out, as `YYYY-MM-DD HH:MM:SS+00`.
fn expires() -> String {
    let ms = now_ms() + DAY_MS;
    timestamptz(ms - ms.rem_euclid(1000)).replace(".000+00", "+00")
}

/// The worked example in SQL (design-1 section 5).
fn sql_inc_436(expires: &str) -> String {
    format!(
        "CREATE TASK \"{TASK}\" EXECUTOR repair_agent APPROVER oncall_lead ON shipments \
         VISIBLE ROWS WHERE (merchant_id = 42) \
         READABLE (id, status, failed_at, attempt_count) MASKED (customer_email) \
         ALLOW UPDATE (status) \
           OLD ROW WHERE (status = 'failed' AND failed_at >= '2026-11-05 17:00:00+00' \
                          AND failed_at < '2026-11-05 17:07:00+00') \
           NEW ROW WHERE (status = 'queued') \
           AT MOST 25 DISTINCT ROWS AND 25 TRANSITIONS APPROVE ABOVE 5 \
         DISCLOSE AT MOST 1000 ROWS READ, 2097152 BYTES RETURNED \
         WITH (statement_ms = 2000, rows_scanned = 100000, concurrent_statements = 2) \
         EXPIRES '{expires}'"
    )
}

/// The same definition in native profile 0.22 (design-1 section 9).
fn native_inc_436(expires: &str) -> String {
    format!(
        "create task \"{TASK}\" for role repair_agent approver oncall_lead on shipments \
         visible when merchant_id = 42 \
         readable (id, status, failed_at, attempt_count) masked (customer_email) \
         allow update (status) \
           from old.status = \"failed\" and old.failed_at >= timestamp(\"2026-11-05 17:00:00+00\") \
             and old.failed_at < timestamp(\"2026-11-05 17:07:00+00\") \
           to new.status = \"queued\" \
           at most 25 distinct rows and 25 transitions approve above 5 \
         disclose at most 1000 rows read and 2097152 bytes \
         limits (statement_ms = 2000, rows_scanned = 100000, concurrent_statements = 2) \
         expires timestamp(\"{expires}\");"
    )
}

struct Fixture {
    admin: Admin,
    _service: Arc<Service>,
    ca: TestCert,
    controller: TestCert,
    lead: TestCert,
    app: TestCert,
    postgresql: Running,
    http: Running,
    api: Arc<ApiRuntime>,
}

impl Fixture {
    /// A v2 database with `shipments`, the issuer `incident_controller`
    /// (CREATEROLE), the task-only executor `repair_agent` and `oncall_lead`;
    /// a PostgreSQL password listener; and the ordinary HTTP API, where
    /// `controller` and `lead` are bound to their database roles and `app`
    /// is an application binding for `shipments`.
    async fn start() -> Self {
        let admin =
            Admin::create_engine(scratch("task-ddl-native").join("db"), Limits::default(), Engine::V2)
                .unwrap();
        admin.sql_session().execute(SCHEMA, &[], None).unwrap();
        let roles = admin.roles();
        for (name, createrole, task_only) in [
            (CONTROLLER, true, false),
            (LEAD, false, false),
            ("repair_agent", false, true),
        ] {
            roles
                .create(NewRole {
                    name: name.into(),
                    login: true,
                    createrole,
                    password: Some(Verifier::generate(&format!("{name}-pw"))),
                    ..NewRole::default()
                })
                .unwrap();
            if task_only {
                roles.set_task_only(name, true).unwrap();
            }
            use TablePrivilege::{Select, Update};
            roles.grant_table("shipments", name, &[Select, Update]).unwrap();
        }

        let ca = certificate(None, false);
        let server = certificate(Some(&ca), true);
        let controller = certificate(Some(&ca), false);
        let lead = certificate(Some(&ca), false);
        let app = certificate(Some(&ca), false);
        let domain = admin.authority_metadata().unwrap().0.to_string();
        let service = Service::new(
            admin.clone(),
            vec![
                Binding {
                    fingerprint: controller.fingerprint(),
                    role: Role::Database { name: CONTROLLER.into() },
                },
                Binding {
                    fingerprint: lead.fingerprint(),
                    role: Role::Database { name: LEAD.into() },
                },
                Binding {
                    fingerprint: app.fingerprint(),
                    role: Role::Application {
                        domain,
                        tables: ["public.shipments".to_owned()].into(),
                    },
                },
            ],
            Clock::new(0, 3600).unwrap(),
        )
        .unwrap();
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
        let postgresql = serve_postgresql(config.listen, service.clone(), Arc::new(listener))
            .await
            .unwrap();
        let server_tls = tls::server(
            vec![server.cert.der().clone()],
            PrivatePkcs8KeyDer::from(server.key.serialize_der()).into(),
            vec![ca.cert.der().clone()],
        )
        .unwrap();
        let api = ApiRuntime::new(service.clone(), ApiLimits::default()).unwrap();
        let (_open, gate) = watch::channel(true);
        let http = serve_http_bound(
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
            api.clone(),
            server_tls,
            16,
            gate,
        )
        .await
        .unwrap();
        Self {
            admin,
            _service: service,
            ca,
            controller,
            lead,
            app,
            postgresql,
            http,
            api,
        }
    }

    async fn stop(self) {
        self.postgresql.shutdown().await.unwrap();
        self.http.shutdown().await.unwrap();
        self.api.shutdown().await;
    }

    async fn pg(&self, user: &str) -> Client {
        let (client, connection) = tokio_postgres::Config::new()
            .host("localhost")
            .hostaddr(self.postgresql.address.ip())
            .port(self.postgresql.address.port())
            .user(user)
            .password(format!("{user}-pw"))
            .dbname("postgres")
            .ssl_mode(SslMode::Disable)
            .connect(NoTls)
            .await
            .unwrap_or_else(|e| panic!("connect {user}: {e}"));
        tokio::spawn(connection);
        client
    }

    /// Native `text` (profile 0.22) as `cert`'s binding, in a session opened
    /// and closed around the call: the HTTP reply.
    async fn native(&self, cert: &TestCert, text: &str) -> Value {
        let peer = (&self.ca, cert);
        let address = self.http.address;
        let (status, opened) =
            http_call(address, peer, "POST", "/v1/sessions", None, Some(&json!({}))).await;
        assert_eq!(status, 201, "open: {opened}");
        let token = opened["sessionId"].as_str().unwrap().to_owned();
        let body = json!({"text": {"text": text, "languageVersion": "0.22"}});
        let (status, reply) = http_call(
            address,
            peer,
            "POST",
            "/v1/session/execute",
            Some(&token),
            Some(&body),
        )
        .await;
        assert_eq!(status, 200, "{text}: {reply}");
        http_call(address, peer, "DELETE", "/v1/session", Some(&token), None).await;
        reply
    }

    fn task(&self) -> Option<TaskDef> {
        self.admin
            .tasks()
            .get(TASK)
            .unwrap()
            .map(|def| (*def).clone())
    }
}

/// The SQLSTATE of a native reply: `None` for success. A task error is a
/// native SQL error carrying the SQLSTATE; a policy refusal is the code.
fn sqlstate(reply: &Value) -> Option<String> {
    match reply["code"].as_str().unwrap_or_else(|| panic!("{reply}")) {
        "ok" => None,
        "native_error" => Some(
            reply["nativeError"]["sqlstate"]
                .as_str()
                .unwrap_or_else(|| panic!("{reply}"))
                .to_owned(),
        ),
        code => Some(code.to_owned()),
    }
}

async fn pg_ok(client: &Client, sql: &str) -> Vec<SimpleQueryMessage> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

async fn pg_sqlstate(client: &Client, sql: &str) -> String {
    let e = client.simple_query(sql).await.unwrap_err();
    e.code().expect("SQLSTATE").code().to_owned()
}

type Rows = Vec<BTreeMap<String, Option<String>>>;

/// A simple-protocol result as (column -> text) maps.
fn pg_rows(messages: &[SimpleQueryMessage]) -> Rows {
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

/// A native SHOW result as (column -> text) maps, keeping the columns whose
/// values are null, boolean, integer or text (`expires_at`, a timestamp, is
/// compared through the stored definition instead).
fn native_rows(reply: &Value) -> Rows {
    assert_eq!(sqlstate(reply), None, "{reply}");
    let sql = &reply["sql"];
    let columns: Vec<String> = sql["columns"]
        .as_array()
        .unwrap_or_else(|| panic!("no columns: {reply}"))
        .iter()
        .map(|c| c.as_str().unwrap().to_owned())
        .collect();
    assert_eq!(columns, giggles_core::task::statements::SHOW_TASK_COLUMNS);
    sql["rows"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .map(|row| {
            columns
                .iter()
                .zip(row["values"].as_array().unwrap())
                .filter_map(|(column, value)| {
                    let (kind, inner) = value.as_object().unwrap().iter().next().unwrap();
                    let text = match kind.as_str() {
                        "nullValue" => None,
                        "boolValue" => Some(if inner.as_bool().unwrap() { "t" } else { "f" }.into()),
                        "intValue" | "textValue" => Some(
                            inner.as_str().map_or_else(|| inner.to_string(), str::to_owned),
                        ),
                        _ => return None,
                    };
                    Some((column.clone(), text))
                })
                .collect()
        })
        .collect()
}

/// `pg` restricted to the columns `native` carries.
fn same_rows(native: &Rows, pg: &Rows) {
    assert_eq!(native.len(), pg.len(), "{native:?} / {pg:?}");
    assert!(native.iter().all(|row| row.len() >= 20), "{native:?}");
    for (native, pg) in native.iter().zip(pg) {
        for (column, value) in native {
            assert_eq!(Some(value), pg.get(column), "column {column}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_text_creates_task_visible_over_pgwire_show() {
    let f = Fixture::start().await;
    let created = f.native(&f.controller, &native_inc_436(&expires())).await;
    assert_eq!(sqlstate(&created), None, "{created}");
    assert_eq!(created["sql"], Value::Null, "a tag carries no rows: {created}");
    let def = f.task().expect("created");
    assert_eq!((def.issuer.as_str(), def.def_version), (CONTROLLER, 1));

    let controller = f.pg(CONTROLLER).await;
    let shown = pg_rows(&pg_ok(&controller, "SHOW TASK \"inc-436\"").await);
    assert_eq!(shown.len(), 1);
    assert_eq!(shown[0]["task"].as_deref(), Some(TASK));
    assert_eq!(shown[0]["executor"].as_deref(), Some("repair_agent"));
    assert_eq!(shown[0]["approver"].as_deref(), Some(LEAD));
    assert_eq!(shown[0]["issuer"].as_deref(), Some(CONTROLLER));
    assert_eq!(shown[0]["def_version"].as_deref(), Some("1"));
    // The native SHOW of the same task gives the same row.
    let native = native_rows(&f.native(&f.controller, "show task \"inc-436\"").await);
    same_rows(&native, &shown);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_created_task_shown_by_native_show() {
    let f = Fixture::start().await;
    let controller = f.pg(CONTROLLER).await;
    pg_ok(&controller, &sql_inc_436(&expires())).await;
    let pg = pg_rows(&pg_ok(&controller, "SHOW TASKS").await);
    let native = native_rows(&f.native(&f.controller, "show tasks;").await);
    assert_eq!(native.len(), 1);
    assert_eq!(native[0]["task"].as_deref(), Some(TASK));
    same_rows(&native, &pg);
    // The approver sees it too (INSPECT), over both entries.
    let lead = native_rows(&f.native(&f.lead, "show task \"inc-436\"").await);
    same_rows(&lead, &pg);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_and_sql_definitions_have_identical_record_bytes() {
    let expires = expires();
    let sql = Fixture::start().await;
    let native = Fixture::start().await;
    pg_ok(&sql.pg(CONTROLLER).await, &sql_inc_436(&expires)).await;
    let created = native.native(&native.controller, &native_inc_436(&expires)).await;
    assert_eq!(sqlstate(&created), None, "{created}");

    let show = "SHOW TASK \"inc-436\"";
    let from_sql = pg_rows(&pg_ok(&sql.pg(CONTROLLER).await, show).await);
    let from_native = pg_rows(&pg_ok(&native.pg(CONTROLLER).await, show).await);
    assert_eq!(from_sql.len(), 1);
    // Every SHOW column but the object ID, which each database allocates.
    let without_id = |rows: &Rows| {
        let mut row = rows[0].clone();
        row.remove("id");
        row
    };
    assert_eq!(without_id(&from_native), without_id(&from_sql));
    assert_eq!(from_native[0]["def_version"], from_sql[0]["def_version"]);

    let (mut a, b) = (sql.task().unwrap(), native.task().unwrap());
    a.id = b.id;
    assert_eq!(a, b, "the stored definitions differ");
    sql.stop().await;
    native.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn application_role_denied() {
    let f = Fixture::start().await;
    let reply = f.native(&f.app, &native_inc_436(&expires())).await;
    assert_eq!(sqlstate(&reply).as_deref(), Some("42501"), "{reply}");
    assert!(f.task().is_none(), "an application binding created a task");
    let shown = f.native(&f.app, "show tasks").await;
    assert_eq!(sqlstate(&shown).as_deref(), Some("42501"), "{shown}");
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlstate_parity_for_42501_and_42710() {
    let f = Fixture::start().await;
    let expires = expires();
    let created = f.native(&f.controller, &native_inc_436(&expires)).await;
    assert_eq!(sqlstate(&created), None, "{created}");

    // 42710: the name is taken, over both entries.
    let again = f.native(&f.controller, &native_inc_436(&expires)).await;
    assert_eq!(sqlstate(&again).as_deref(), Some("42710"), "{again}");
    let controller = f.pg(CONTROLLER).await;
    assert_eq!(pg_sqlstate(&controller, &sql_inc_436(&expires)).await, "42710");

    // 42501: the approver may not revoke the task, over both entries.
    let revoke = f.native(&f.lead, "revoke task \"inc-436\";").await;
    assert_eq!(sqlstate(&revoke).as_deref(), Some("42501"), "{revoke}");
    let lead = f.pg(LEAD).await;
    assert_eq!(pg_sqlstate(&lead, "REVOKE TASK \"inc-436\"").await, "42501");
    assert!(!f.task().unwrap().revoked);

    // The native reply's error class is the native SQL one (observed as a
    // native error, not a transport refusal).
    assert_eq!(observe_native(&revoke).transport, None);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn embedded_executor_unknown_actor_42704() {
    let f = Fixture::start().await;
    let engine = giggles_language::Engine::with_version(
        f.admin.clone(),
        giggles_language::LanguageVersion::Embedded022,
    );
    let text = native_inc_436(&expires());
    assert_eq!(
        engine.execute_task_text("nobody", &text).unwrap_err(),
        giggles_language::Error::Sql { sqlstate: "42704".into() }
    );
    assert!(f.task().is_none());
    // A real actor creates it; the SQL form then refuses a second as 42710.
    match engine.execute_task_text(CONTROLLER, &text).unwrap() {
        TaskOutcome::Tag(tag) => assert_eq!(tag, "CREATE TASK"),
        other => panic!("{other:?}"),
    }
    assert_eq!(f.task().unwrap().issuer.as_str(), CONTROLLER);
    assert_eq!(
        engine.execute_task_text(CONTROLLER, &text).unwrap_err(),
        giggles_language::Error::Sql { sqlstate: "42710".into() }
    );
    match engine.execute_task_text(LEAD, "show tasks").unwrap() {
        TaskOutcome::Rows(description, rows) => {
            assert_eq!(description.columns.len(), rows.columns.len());
            assert_eq!(rows.rows.len(), 1);
        }
        other => panic!("{other:?}"),
    }
    // Profiles before 0.22 have no task statements.
    let old = giggles_language::Engine::with_version(
        f.admin.clone(),
        giggles_language::LanguageVersion::Embedded021,
    );
    assert_eq!(
        old.execute_task_text(CONTROLLER, "show tasks").unwrap_err(),
        giggles_language::Error::Unsupported
    );
    f.stop().await;
}
