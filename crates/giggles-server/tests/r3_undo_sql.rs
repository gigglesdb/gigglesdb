//! GD-0061 R3-804 (design-7 D7.1, D8): PREVIEW UNDO, SHOW UNDO and APPROVE
//! UNDO over pgwire with a real driver (tokio-postgres), in the simple and
//! the extended protocol.
//!
//! The executor `agent_exec` writes on the agent listener (`OPERATION '<id>'
//! UPDATE ..`). The undo role `oncall_lead` (the task's approver, SELECT and
//! UPDATE on the table) runs the undo forms on the password PostgreSQL
//! listener. The agent listener refuses them as `Other`, the same role bound
//! to a certificate on the mTLS listener gets 42501, and the executor can
//! never undo.
//!
//! Task `inc-436` on `shipments` (`deliveries` is reserved): merchant 42's
//! rows, `status` from 'failed' to 'queued', approval above 2 rows.
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::{
    AgentServer, DAY_MS, TestCert, binding, certificate, now_ms, scratch, timestamptz,
};
use giggles_core::{
    Admin, Engine, Limits,
    roles::{NewRole, TablePrivilege},
    task::{
        DenyRule, StatementClass, TaskError, UndoScope,
        definition::{
            AllowedWrite, DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText,
            TaskName, WallClockMs,
        },
    },
};
use giggles_server::{
    Binding, Role, Running,
    auth::{PostgresqlConfig, TlsMode, scram::Verifier, serve_postgresql},
};
use std::{collections::BTreeMap, sync::Arc};
use tokio::{net::TcpListener, sync::watch};
use tokio_postgres::{
    Client, NoTls, SimpleQueryMessage,
    config::SslMode,
    types::{ToSql, Type},
};
use tokio_postgres_rustls::MakeRustlsConnect;

const TASK: &str = "\"inc-436\"";
const UNDOER: &str = "oncall_lead";
const UNDOER_PW: &str = "lead-pw";
const EXECUTOR_PW: &str = "exec-pw";
/// The two agent writes: rows 1-2, then rows 3-4.
const W1: &str = "OPERATION 'w1' UPDATE shipments SET status = 'queued' \
                  WHERE merchant_id = 42 AND status = 'failed' AND id <= 2";
const W2: &str = "OPERATION 'w2' UPDATE shipments SET status = 'queued' \
                  WHERE merchant_id = 42 AND status = 'failed' AND id <= 4";
const BEFORE: [&str; 6] = [
    "failed",
    "failed",
    "failed",
    "failed",
    "delivered",
    "failed",
];
const WRITTEN: [&str; 6] = [
    "queued",
    "queued",
    "queued",
    "queued",
    "delivered",
    "failed",
];
/// After a partial undo with row 1 changed by the application since.
const PARTIAL: [&str; 6] = [
    "queued",
    "failed",
    "failed",
    "failed",
    "delivered",
    "failed",
];
const PREVIEW_UNDO_COLUMNS: [&str; 7] = [
    "change_id",
    "digest",
    "restorable",
    "conflicts",
    "superseded",
    "settled",
    "expires_at",
];
const SHOW_UNDO_COLUMNS: [&str; 6] = [
    "class",
    "key",
    "column_name",
    "current_value",
    "restored_value",
    "reason",
];

#[derive(Clone, Copy, Debug)]
enum Protocol {
    Simple,
    Extended,
}
const PROTOCOLS: [Protocol; 2] = [Protocol::Simple, Protocol::Extended];

/// What a statement returned: its rows as text (NULL as `None`) and the
/// CommandComplete count (the row count for a statement with columns).
#[derive(Debug, PartialEq)]
struct Reply {
    rows: Vec<Vec<Option<String>>>,
    count: u64,
}

fn cell(row: &tokio_postgres::Row, i: usize) -> Option<String> {
    let ty = row.columns()[i].type_().clone();
    if ty == Type::INT8 {
        row.get::<_, Option<i64>>(i).map(|v| v.to_string())
    } else if ty == Type::TIMESTAMPTZ {
        row.get::<_, Option<std::time::SystemTime>>(i)
            .map(|v| format!("{v:?}"))
    } else {
        row.get::<_, Option<String>>(i)
    }
}

async fn run(
    client: &Client,
    protocol: Protocol,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Reply, tokio_postgres::Error> {
    match protocol {
        Protocol::Simple => {
            assert!(params.is_empty(), "the simple protocol has no parameters");
            let mut reply = Reply {
                rows: vec![],
                count: 0,
            };
            for message in client.simple_query(sql).await? {
                match message {
                    SimpleQueryMessage::Row(row) => reply.rows.push(
                        (0..row.len())
                            .map(|i| row.get(i).map(str::to_owned))
                            .collect(),
                    ),
                    SimpleQueryMessage::CommandComplete(count) => reply.count = count,
                    _ => {}
                }
            }
            Ok(reply)
        }
        Protocol::Extended => {
            let statement = client.prepare(sql).await?;
            if statement.columns().is_empty() {
                let count = client.execute(&statement, params).await?;
                return Ok(Reply {
                    rows: vec![],
                    count,
                });
            }
            let rows = client.query(&statement, params).await?;
            Ok(Reply {
                count: rows.len() as u64,
                rows: rows
                    .iter()
                    .map(|row| (0..row.len()).map(|i| cell(row, i)).collect())
                    .collect(),
            })
        }
    }
}

#[track_caller]
fn code(result: Result<Reply, tokio_postgres::Error>) -> (String, String) {
    let error = result.expect_err("expected an error");
    let db = error.as_db_error().unwrap_or_else(|| panic!("{error:?}"));
    (db.code().code().to_owned(), db.message().to_owned())
}

fn task_admin() -> Admin {
    let path = scratch("r3-undo-sql");
    let admin = Admin::create_engine(path.join("db.redb"), Limits::default(), Engine::V2).unwrap();
    for statement in [
        "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text, \
         attempt_count int)",
        "INSERT INTO shipments VALUES (1, 42, 'failed', 1), (2, 42, 'failed', 2), \
         (3, 42, 'failed', 1), (4, 42, 'failed', 3), (5, 42, 'delivered', 0), (6, 7, 'failed', 1)",
    ] {
        admin.sql_session().execute(statement, &[], None).unwrap();
    }
    let roles = admin.roles();
    for (name, task_only, password) in [
        ("incident_controller", false, None),
        (UNDOER, false, Some(UNDOER_PW)),
        ("bystander", false, Some("by-pw")),
        ("agent_exec", true, Some(EXECUTOR_PW)),
    ] {
        roles
            .create(NewRole {
                name: name.into(),
                login: true,
                password: password.map(Verifier::generate),
                ..NewRole::default()
            })
            .unwrap();
        if task_only {
            roles.set_task_only(name, true).unwrap();
        }
    }
    let read_write = &[TablePrivilege::Select, TablePrivilege::Update][..];
    for (name, privileges) in [
        ("incident_controller", read_write),
        ("agent_exec", read_write),
        (UNDOER, read_write),
        ("bystander", &[TablePrivilege::Select][..]),
    ] {
        roles.grant_table("shipments", name, privileges).unwrap();
    }
    let issuer = roles.get("incident_controller").unwrap().unwrap();
    admin
        .tasks()
        .create(
            &issuer,
            NewTask {
                name: "inc-436".into(),
                executor: "agent_exec".into(),
                approver: Some(UNDOER.into()),
                schema: None,
                table: "shipments".into(),
                visible_rows: PredicateText("merchant_id = 42".into()),
                readable: ["id", "merchant_id", "status", "attempt_count"]
                    .iter()
                    .map(|c| c.to_string())
                    .collect(),
                masked: Vec::new(),
                untrusted_text: Vec::new(),
                allowed_write: Some(AllowedWrite {
                    columns: ["status".to_string()].into_iter().collect(),
                    old_row: PredicateText("status = 'failed'".into()),
                    new_row: PredicateText("status = 'queued'".into()),
                }),
                mutation_caps: MutationCaps {
                    distinct_rows: 25,
                    transitions: 25,
                },
                disclosure_caps: DisclosureCaps {
                    rows_read: 1000,
                    rows_returned: 1000,
                    bytes_returned: 2_097_152,
                },
                limits: LoadLimits::default(),
                approval_threshold: 2,
                expires: timestamptz(now_ms() + DAY_MS),
            },
            WallClockMs(now_ms()),
        )
        .unwrap();
    admin
}

/// The service with three listeners: agent (mTLS, the executor's
/// certificate), password PostgreSQL, and certificate PostgreSQL (mTLS) where
/// `undoer_cert` is bound to `oncall_lead`.
struct Fixture {
    server: AgentServer,
    agent_cert: TestCert,
    undoer_cert: TestCert,
    pg: Running,
    mtls: Running,
}

impl Fixture {
    async fn new() -> Self {
        let ca = certificate(None, false);
        let agent_cert = certificate(Some(&ca), false);
        let undoer_cert = certificate(Some(&ca), false);
        let server = AgentServer::start(
            task_admin(),
            ca,
            vec![Binding {
                fingerprint: undoer_cert.fingerprint(),
                role: Role::Database {
                    name: UNDOER.into(),
                },
            }],
            vec![binding(&agent_cert, "agent_exec", &["inc-436"])],
        )
        .await;
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
        let pg = serve_postgresql(config.listen, server.service.clone(), Arc::new(listener))
            .await
            .unwrap();
        let (_open, gate) = watch::channel(true);
        let mtls = giggles_server::serve_pg_bound(
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
            server.service.clone(),
            server.tls.clone(),
            8,
            gate,
        )
        .await
        .unwrap();
        Self {
            server,
            agent_cert,
            undoer_cert,
            pg,
            mtls,
        }
    }

    /// The executor on the agent listener (the binding has one task).
    async fn agent(&self) -> Client {
        let address = self.server.agent.address;
        let (client, connection) = tokio_postgres::Config::new()
            .host("localhost")
            .hostaddr(address.ip())
            .port(address.port())
            .user("agent_exec")
            .dbname("giggles")
            .ssl_mode(SslMode::Require)
            .connect(MakeRustlsConnect::new(
                self.agent_cert.client(&self.server.ca),
            ))
            .await
            .expect("agent connects");
        tokio::spawn(connection);
        client
    }

    /// `user` with its password on the PostgreSQL listener.
    async fn try_password(
        &self,
        user: &str,
        password: &str,
    ) -> Result<Client, tokio_postgres::Error> {
        let (client, connection) = tokio_postgres::Config::new()
            .host("localhost")
            .hostaddr(self.pg.address.ip())
            .port(self.pg.address.port())
            .user(user)
            .password(password)
            .dbname("postgres")
            .ssl_mode(SslMode::Disable)
            .connect(NoTls)
            .await?;
        tokio::spawn(connection);
        Ok(client)
    }

    async fn password(&self, user: &str, password: &str) -> Client {
        self.try_password(user, password)
            .await
            .expect("password login")
    }

    /// `oncall_lead` through its certificate binding on the mTLS listener.
    async fn certificate_bound(&self) -> Client {
        let (client, connection) = tokio_postgres::Config::new()
            .host("localhost")
            .hostaddr(self.mtls.address.ip())
            .port(self.mtls.address.port())
            .user("giggles")
            .dbname("giggles")
            .ssl_mode(SslMode::Require)
            .connect(MakeRustlsConnect::new(
                self.undoer_cert.client(&self.server.ca),
            ))
            .await
            .expect("certificate login");
        tokio::spawn(connection);
        client
    }

    fn sql(&self, statement: &str) {
        self.server
            .admin
            .sql_session()
            .execute(statement, &[], None)
            .unwrap();
    }

    fn statuses(&self) -> Vec<String> {
        self.server
            .admin
            .sql_session()
            .execute("SELECT status FROM shipments ORDER BY id", &[], None)
            .unwrap()
            .rows
            .into_iter()
            .map(|row| match &row.values[0] {
                giggles_core::relational::Value::Text(text) => text.clone(),
                other => panic!("{other:?}"),
            })
            .collect()
    }

    /// The executor's two direct writes, rows 1-2 and 3-4.
    async fn agent_writes(&self) {
        let agent = self.agent().await;
        for write in [W1, W2] {
            assert_eq!(
                run(&agent, Protocol::Simple, write, &[])
                    .await
                    .unwrap()
                    .count,
                2,
                "{write}"
            );
        }
        assert_eq!(self.statuses(), WRITTEN);
    }
}

/// A PREVIEW UNDO row: (change_id, digest, [restorable, conflicts,
/// superseded, settled]); expires_at is present.
#[track_caller]
fn undo_row(reply: &Reply) -> (i64, String, [i64; 4]) {
    assert_eq!(reply.rows.len(), 1, "{reply:?}");
    let row = &reply.rows[0];
    assert_eq!(row.len(), 7, "{reply:?}");
    assert!(row[6].is_some(), "expires_at: {reply:?}");
    let digest = row[1].clone().unwrap();
    assert!(digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit()));
    let n = |i: usize| -> i64 { row[i].as_deref().unwrap().parse().unwrap() };
    (n(0), digest, [n(2), n(3), n(4), n(5)])
}

/// One receipt row as `column name -> text`.
#[track_caller]
fn receipt(reply: &Reply, names: &[String]) -> BTreeMap<String, Option<String>> {
    assert_eq!(reply.rows.len(), 1, "{reply:?}");
    assert_eq!(reply.rows[0].len(), names.len(), "{reply:?}");
    names
        .iter()
        .cloned()
        .zip(reply.rows[0].iter().cloned())
        .collect()
}

fn preview_undo(operation: Option<&str>) -> String {
    match operation {
        Some(op) => format!("PREVIEW UNDO OF TASK {TASK} OPERATION '{op}'"),
        None => format!("PREVIEW UNDO OF TASK {TASK}"),
    }
}

fn approve_undo(id: i64, digest: &str, mode: &str) -> String {
    format!("APPROVE UNDO {id} OF TASK {TASK} DIGEST '{digest}' MODE {mode}")
}

#[tokio::test]
async fn undo_preview_show_approve_over_pgwire() {
    for protocol in PROTOCOLS {
        let f = Fixture::new().await;
        f.agent_writes().await;
        // The application changes row 1 after the agent: a conflict.
        f.sql("UPDATE shipments SET attempt_count = 9 WHERE id = 1");
        let undoer = f.password(UNDOER, UNDOER_PW).await;

        // The extended protocol describes each form's columns.
        let described = undoer.prepare(&preview_undo(None)).await.unwrap();
        let names: Vec<_> = described.columns().iter().map(|c| c.name()).collect();
        assert_eq!(names, PREVIEW_UNDO_COLUMNS);

        // Operation scope: w2's rows 3-4 alone.
        let reply = run(&undoer, protocol, &preview_undo(Some("w2")), &[])
            .await
            .unwrap();
        let (_, _, counts) = undo_row(&reply);
        assert_eq!(counts, [2, 0, 0, 0], "{protocol:?}");

        // Task scope: rows 2-4 restorable, row 1 changed since.
        let reply = run(&undoer, protocol, &preview_undo(None), &[])
            .await
            .unwrap();
        let (id, digest, counts) = undo_row(&reply);
        assert_eq!(counts, [3, 1, 0, 0], "{protocol:?}");
        assert_eq!(f.statuses(), WRITTEN, "an undo preview writes no user rows");

        // SHOW UNDO: one line per restored column, one for the conflict.
        let show = format!("SHOW UNDO {id} OF TASK {TASK}");
        let statement = undoer.prepare(&show).await.unwrap();
        let names: Vec<_> = statement.columns().iter().map(|c| c.name()).collect();
        assert_eq!(names, SHOW_UNDO_COLUMNS);
        let lines = run(&undoer, protocol, &show, &[]).await.unwrap().rows;
        assert_eq!(lines.len(), 4, "{protocol:?}: {lines:?}");
        let restores: Vec<_> = lines
            .iter()
            .filter(|l| l[0].as_deref() == Some("restore"))
            .collect();
        assert_eq!(restores.len(), 3, "{lines:?}");
        for (line, id) in restores.iter().zip(2..) {
            assert_eq!(line[1].as_deref(), Some(format!("(id)=({id})").as_str()));
            assert_eq!(line[2].as_deref(), Some("status"));
            assert_eq!(line[3].as_deref(), Some("queued"));
            assert_eq!(line[4].as_deref(), Some("failed"));
            assert_eq!(line[5], None);
        }
        let conflict = lines
            .iter()
            .find(|l| l[0].as_deref() == Some("conflict"))
            .expect("conflict line");
        assert_eq!(
            conflict[1..],
            [
                Some("(id)=(1)".to_owned()),
                None,
                None,
                None,
                Some("changed_since".to_owned())
            ]
        );

        // COMPLETE refuses a preview with conflicts, and writes nothing.
        let complete = approve_undo(id, &digest, "COMPLETE");
        let refused = TaskError::Denied(DenyRule::UndoHasConflicts);
        assert_eq!(
            code(run(&undoer, protocol, &complete, &[]).await),
            (refused.sqlstate().to_owned(), refused.message().to_owned()),
            "{protocol:?}"
        );
        assert_eq!(f.statuses(), WRITTEN);

        // PARTIAL restores the rest and returns the full receipt.
        let partial = approve_undo(id, &digest, "PARTIAL");
        let statement = undoer.prepare(&partial).await.unwrap();
        let names: Vec<String> = statement
            .columns()
            .iter()
            .map(|c| c.name().to_owned())
            .collect();
        assert_eq!(names.len(), 20);
        let reply = run(&undoer, protocol, &partial, &[]).await.unwrap();
        let fields = receipt(&reply, &names);
        let field = |name: &str| fields[name].clone();
        let id_text = id.to_string();
        for (name, expected) in [
            ("operation_id", Some(format!("undo:{id}"))),
            ("decision", Some("undo_partial".into())),
            ("change_id", Some(id_text)),
            ("mode", Some("partial".into())),
            ("restored", Some("3".into())),
            ("conflicts", Some("1".into())),
            ("superseded", Some("0".into())),
            ("row_count", Some("3".into())),
            ("distinct_charged", Some("0".into())),
            ("transitions_charged", Some("0".into())),
            ("distinct_after", Some("4".into())),
            ("transitions_after", Some("4".into())),
            ("issuer", Some("incident_controller".into())),
            ("executor", Some("agent_exec".into())),
            ("undone_by", Some(UNDOER.into())),
        ] {
            assert_eq!(field(name), expected, "{protocol:?}: {name}");
        }
        assert_eq!(field("receipt_seq").as_deref(), Some("3"));
        for name in ["effects_digest", "request_fp"] {
            assert_eq!(field(name).map(|d| d.len()), Some(64), "{name}");
        }
        assert!(field("decided_at").is_some());
        assert_eq!(f.statuses(), PARTIAL);

        // The same request replays the same receipt; another mode conflicts.
        assert_eq!(run(&undoer, protocol, &partial, &[]).await.unwrap(), reply);
        assert_eq!(
            code(run(&undoer, protocol, &complete, &[]).await).0,
            TaskError::OperationConflict.sqlstate()
        );
        assert_eq!(f.statuses(), PARTIAL);

        // SHOW UNDO needs the undo or inspect privilege and SELECT.
        let bystander = f.password("bystander", "by-pw").await;
        assert_eq!(code(run(&bystander, protocol, &show, &[]).await).0, "42501");
        // Unknown id and unknown task: 42704.
        let unknown = format!("SHOW UNDO 99 OF TASK {TASK}");
        assert_eq!(code(run(&undoer, protocol, &unknown, &[]).await).0, "42704");
        let no_task = "PREVIEW UNDO OF TASK \"inc-999\"";
        assert_eq!(code(run(&undoer, protocol, no_task, &[]).await).0, "42704");
        // Inside a transaction block the forms are refused.
        undoer.batch_execute("BEGIN").await.unwrap();
        assert_eq!(code(run(&undoer, protocol, &show, &[]).await).0, "25001");
        undoer.batch_execute("ROLLBACK").await.unwrap();
    }
}

#[tokio::test]
async fn undo_on_agent_listener_is_refused() {
    let f = Fixture::new().await;
    f.agent_writes().await;
    let undoer = f.password(UNDOER, UNDOER_PW).await;
    let reply = run(&undoer, Protocol::Simple, &preview_undo(None), &[])
        .await
        .unwrap();
    let (id, digest, _) = undo_row(&reply);
    let agent = f.agent().await;
    let refused = (
        "0A000".to_owned(),
        TaskError::NotAllowedInTaskSession(StatementClass::Other)
            .message()
            .to_owned(),
    );
    for protocol in PROTOCOLS {
        for sql in [
            preview_undo(None),
            preview_undo(Some("w1")),
            format!("SHOW UNDO {id} OF TASK {TASK}"),
            approve_undo(id, &digest, "COMPLETE"),
            approve_undo(id, &digest, "PARTIAL"),
            // Malformed ones are refused the same way.
            format!("APPROVE UNDO {id} OF TASK {TASK} DIGEST '{digest}'"),
            "PREVIEW UNDO OF TASK".to_owned(),
            "SHOW UNDO OF TASK x".to_owned(),
        ] {
            assert_eq!(
                code(run(&agent, protocol, &sql, &[]).await),
                refused,
                "{protocol:?}: {sql}"
            );
        }
    }
    // Nothing was undone; the undo role still can.
    assert_eq!(f.statuses(), WRITTEN);
    let complete = approve_undo(id, &digest, "COMPLETE");
    run(&undoer, Protocol::Extended, &complete, &[])
        .await
        .unwrap();
    assert_eq!(f.statuses(), BEFORE);
}

#[tokio::test]
async fn executor_role_cannot_undo() {
    let f = Fixture::new().await;
    f.agent_writes().await;
    // On its own listener every undo form is a task-session refusal.
    let agent = f.agent().await;
    for protocol in PROTOCOLS {
        assert_eq!(
            code(run(&agent, protocol, &preview_undo(None), &[]).await).0,
            "0A000"
        );
    }
    // A task-only role never logs in on the PostgreSQL listener, even with a
    // password.
    assert!(f.try_password("agent_exec", EXECUTOR_PW).await.is_err());
    // And the executor holds no undo privilege, even when granted one: the
    // core statement as that role outside a task session is 42501.
    let issuer = f.password(UNDOER, UNDOER_PW).await;
    let _ = issuer
        .batch_execute(&format!("GRANT UNDO ON TASK {TASK} TO agent_exec"))
        .await;
    let task = TaskName::new("inc-436").unwrap();
    let error = giggles_core::task::undo_preview(
        &mut f.server.admin.sql_session(),
        &"agent_exec".to_owned(),
        &task,
        UndoScope::Task,
    )
    .expect_err("the executor cannot undo");
    assert_eq!(error.sqlstate(), "42501", "{error:?}");
    assert_eq!(f.statuses(), WRITTEN);
}

#[tokio::test]
async fn undo_by_certificate_bound_role_is_42501() {
    let f = Fixture::new().await;
    f.agent_writes().await;
    let undoer = f.password(UNDOER, UNDOER_PW).await;
    let (id, digest, _) = undo_row(
        &run(&undoer, Protocol::Simple, &preview_undo(None), &[])
            .await
            .unwrap(),
    );
    // The same role, certificate bound on the mTLS listener: 42501 on every form.
    let bound = f.certificate_bound().await;
    for protocol in PROTOCOLS {
        for sql in [
            preview_undo(None),
            format!("SHOW UNDO {id} OF TASK {TASK}"),
            approve_undo(id, &digest, "COMPLETE"),
        ] {
            assert_eq!(
                code(run(&bound, protocol, &sql, &[]).await).0,
                "42501",
                "{protocol:?}: {sql}"
            );
        }
    }
    assert_eq!(f.statuses(), WRITTEN);
}

#[tokio::test]
async fn mode_keyword_required() {
    let f = Fixture::new().await;
    f.agent_writes().await;
    let undoer = f.password(UNDOER, UNDOER_PW).await;
    let (id, digest, counts) = undo_row(
        &run(&undoer, Protocol::Simple, &preview_undo(None), &[])
            .await
            .unwrap(),
    );
    assert_eq!(counts, [4, 0, 0, 0]);
    for protocol in PROTOCOLS {
        for sql in [
            format!("APPROVE UNDO {id} OF TASK {TASK} DIGEST '{digest}'"),
            format!("APPROVE UNDO {id} OF TASK {TASK} DIGEST '{digest}' COMPLETE"),
            format!("APPROVE UNDO {id} OF TASK {TASK} DIGEST '{digest}' MODE"),
            approve_undo(id, &digest, "FULL"),
            format!("APPROVE UNDO {id} OF TASK {TASK} MODE COMPLETE DIGEST '{digest}'"),
        ] {
            assert_eq!(
                code(run(&undoer, protocol, &sql, &[]).await).0,
                "42601",
                "{protocol:?}: {sql}"
            );
        }
        // A bad digest with a mode is 22023.
        let bad = approve_undo(id, "xyz", "COMPLETE");
        assert_eq!(code(run(&undoer, protocol, &bad, &[]).await).0, "22023");
    }
    assert_eq!(f.statuses(), WRITTEN);
    let reply = run(
        &undoer,
        Protocol::Extended,
        &approve_undo(id, &digest, "complete"),
        &[],
    )
    .await
    .unwrap();
    assert_eq!(reply.rows[0][1].as_deref(), Some("undo_complete"));
    assert_eq!(f.statuses(), BEFORE);
}
