//! GD-0061 R3-707 (design-7 D7.1): PREVIEW, APPLY CHANGE and the approver
//! forms over pgwire with a real driver (tokio-postgres), in the simple and
//! the extended protocol.
//!
//! The executor `agent_exec` previews and applies on the agent listener (with
//! the ruling R2 forms `PREVIEW UPDATE ..` and `OPERATION '<id>' APPLY CHANGE
//! <id> DIGEST '<hex>'`). The approver `oncall_lead` runs SHOW CHANGES, SHOW
//! CHANGE, APPROVE and REVOKE APPROVAL on the password PostgreSQL listener;
//! the same role bound to a certificate on the mTLS listener is refused with
//! 42501, and the agent listener refuses the approver forms as `Other`.
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
        StatementClass, TaskError,
        definition::{
            AllowedWrite, DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText,
            WallClockMs,
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
const REQUEUE: &str =
    "UPDATE shipments SET status = 'queued' WHERE merchant_id = 42 AND status = 'failed'";
const BEFORE: [&str; 6] = ["failed", "failed", "failed", "failed", "delivered", "failed"];
const AFTER: [&str; 6] = ["queued", "queued", "queued", "queued", "delivered", "failed"];
const APPROVER_PW: &str = "lead-pw";

#[derive(Clone, Copy, Debug)]
enum Protocol {
    Simple,
    Extended,
}
const PROTOCOLS: [Protocol; 2] = [Protocol::Simple, Protocol::Extended];

/// What a statement returned: its rows as text (NULL as `None`) and the
/// CommandComplete count (the row count for a statement with columns).
#[derive(Debug)]
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

/// The worked example with an approver: `oncall_lead` (password login, SELECT
/// on the table) approves above 2 rows.
fn task_admin() -> Admin {
    let path = scratch("r3-preview-sql");
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
        ("oncall_lead", false, Some(APPROVER_PW)),
        ("bystander", false, Some("by-pw")),
        ("agent_exec", true, None),
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
    for (name, privileges) in [
        (
            "incident_controller",
            &[TablePrivilege::Select, TablePrivilege::Update][..],
        ),
        (
            "agent_exec",
            &[TablePrivilege::Select, TablePrivilege::Update][..],
        ),
        ("oncall_lead", &[TablePrivilege::Select][..]),
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
                approver: Some("oncall_lead".into()),
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
/// `approver_cert` is bound to `oncall_lead`.
struct Fixture {
    server: AgentServer,
    agent_cert: TestCert,
    approver_cert: TestCert,
    pg: Running,
    mtls: Running,
}

impl Fixture {
    async fn new() -> Self {
        let ca = certificate(None, false);
        let agent_cert = certificate(Some(&ca), false);
        let approver_cert = certificate(Some(&ca), false);
        let server = AgentServer::start(
            task_admin(),
            ca,
            vec![Binding {
                fingerprint: approver_cert.fingerprint(),
                role: Role::Database {
                    name: "oncall_lead".into(),
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
            approver_cert,
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
    async fn password(&self, user: &str, password: &str) -> Client {
        let (client, connection) = tokio_postgres::Config::new()
            .host("localhost")
            .hostaddr(self.pg.address.ip())
            .port(self.pg.address.port())
            .user(user)
            .password(password)
            .dbname("postgres")
            .ssl_mode(SslMode::Disable)
            .connect(NoTls)
            .await
            .expect("password login");
        tokio::spawn(connection);
        client
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
                self.approver_cert.client(&self.server.ca),
            ))
            .await
            .expect("certificate login");
        tokio::spawn(connection);
        client
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
}

/// A preview row: (change_id, digest, row_count); expires_at is present.
#[track_caller]
fn preview_row(reply: &Reply) -> (i64, String, i64) {
    assert_eq!(reply.rows.len(), 1, "{reply:?}");
    let row = &reply.rows[0];
    assert_eq!(row.len(), 4, "{reply:?}");
    assert!(row[3].is_some(), "expires_at: {reply:?}");
    let digest = row[1].clone().unwrap();
    assert!(digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit()));
    (
        row[0].as_deref().unwrap().parse().unwrap(),
        digest,
        row[2].as_deref().unwrap().parse().unwrap(),
    )
}

/// SHOW CHANGES as (change_id, state, approved_by).
async fn changes(client: &Client, protocol: Protocol) -> Vec<(String, String, Option<String>)> {
    let reply = run(client, protocol, &format!("SHOW CHANGES OF TASK {TASK}"), &[])
        .await
        .unwrap();
    reply
        .rows
        .into_iter()
        .map(|row| {
            assert_eq!(row.len(), 8);
            (row[0].clone().unwrap(), row[2].clone().unwrap(), row[7].clone())
        })
        .collect()
}

#[tokio::test]
async fn preview_then_approve_then_apply_over_pgwire() {
    for protocol in PROTOCOLS {
        let f = Fixture::new().await;
        let agent = f.agent().await;
        let approver = f.password("oncall_lead", APPROVER_PW).await;
        let preview = run(&agent, protocol, &format!("PREVIEW {REQUEUE}"), &[])
            .await
            .unwrap();
        let (change, digest, rows) = preview_row(&preview);
        assert_eq!((change, rows), (1, 4), "{protocol:?}");
        assert_eq!(f.statuses(), BEFORE, "a preview writes no user rows");
        let apply = |op: &str| format!("OPERATION '{op}' APPLY CHANGE {change} DIGEST '{digest}'");
        // 4 rows is above the threshold of 2: no approval yet.
        assert_eq!(
            code(run(&agent, protocol, &apply("a1"), &[]).await).0,
            TaskError::ApprovalRequired.sqlstate(),
            "{protocol:?}"
        );
        assert_eq!(f.statuses(), BEFORE);
        assert_eq!(
            changes(&approver, protocol).await,
            [(change.to_string(), "open".to_owned(), None)]
        );
        let approve = format!("APPROVE CHANGE {change} OF TASK {TASK} DIGEST '{digest}'");
        run(&approver, protocol, &approve, &[]).await.unwrap();
        assert_eq!(
            changes(&approver, protocol).await,
            [(
                change.to_string(),
                "approved".to_owned(),
                Some("oncall_lead".to_owned())
            )]
        );
        // Revoke, and the apply needs approval again; approve once more.
        let revoke = format!("REVOKE APPROVAL OF CHANGE {change} OF TASK {TASK}");
        run(&approver, protocol, &revoke, &[]).await.unwrap();
        assert_eq!(
            code(run(&agent, protocol, &apply("a1"), &[]).await).0,
            TaskError::ApprovalRequired.sqlstate()
        );
        run(&approver, protocol, &approve, &[]).await.unwrap();
        // The apply commits as the direct write does: UPDATE 4.
        let applied = run(&agent, protocol, &apply("a2"), &[]).await.unwrap();
        assert_eq!(applied.count, 4, "{protocol:?}");
        assert_eq!(f.statuses(), AFTER);
        // The resend under the same ID replays the receipt.
        assert_eq!(run(&agent, protocol, &apply("a2"), &[]).await.unwrap().count, 4);
        let outcome = run(&agent, protocol, "SHOW OPERATION 'a2'", &[])
            .await
            .unwrap();
        assert_eq!(outcome.rows.len(), 1);
        assert_eq!(outcome.rows[0][1].as_deref(), Some("committed"));
        assert_eq!(outcome.rows[0][2].as_deref(), Some("4"));
        assert_eq!(
            changes(&approver, protocol).await,
            [(
                change.to_string(),
                "applied".to_owned(),
                Some("oncall_lead".to_owned())
            )]
        );
        // A new ID finds the preview applied.
        assert_eq!(
            code(run(&agent, protocol, &apply("a3"), &[]).await).0,
            TaskError::ApprovalStale.sqlstate()
        );
    }
}

#[tokio::test]
async fn approve_on_agent_listener_is_refused() {
    let f = Fixture::new().await;
    let agent = f.agent().await;
    let approver = f.password("oncall_lead", APPROVER_PW).await;
    let preview = run(&agent, Protocol::Simple, &format!("PREVIEW {REQUEUE}"), &[])
        .await
        .unwrap();
    let (change, digest, _) = preview_row(&preview);
    let refused = (
        "0A000".to_owned(),
        TaskError::NotAllowedInTaskSession(StatementClass::Other)
            .message()
            .to_owned(),
    );
    for protocol in PROTOCOLS {
        for sql in [
            format!("APPROVE CHANGE {change} OF TASK {TASK} DIGEST '{digest}'"),
            format!("REVOKE APPROVAL OF CHANGE {change} OF TASK {TASK}"),
            format!("SHOW CHANGES OF TASK {TASK}"),
            format!("SHOW CHANGE {change} OF TASK {TASK}"),
            // Malformed ones are refused the same way.
            format!("APPROVE CHANGE {change} OF TASK {TASK} DIGEST 'xyz'"),
            "SHOW CHANGES OF TASK".to_owned(),
        ] {
            assert_eq!(
                code(run(&agent, protocol, &sql, &[]).await),
                refused,
                "{protocol:?}: {sql}"
            );
        }
    }
    // Nothing was approved.
    assert_eq!(
        changes(&approver, Protocol::Simple).await,
        [(change.to_string(), "open".to_owned(), None)]
    );
}

#[tokio::test]
async fn approve_by_certificate_bound_role_on_mtls_listener_is_42501() {
    let f = Fixture::new().await;
    let agent = f.agent().await;
    let preview = run(&agent, Protocol::Simple, &format!("PREVIEW {REQUEUE}"), &[])
        .await
        .unwrap();
    let (change, digest, _) = preview_row(&preview);
    let approve = format!("APPROVE CHANGE {change} OF TASK {TASK} DIGEST '{digest}'");
    // The named approver, but certificate bound: refused on every form.
    let bound = f.certificate_bound().await;
    for protocol in PROTOCOLS {
        for sql in [
            approve.clone(),
            format!("REVOKE APPROVAL OF CHANGE {change} OF TASK {TASK}"),
            format!("SHOW CHANGES OF TASK {TASK}"),
            format!("SHOW CHANGE {change} OF TASK {TASK}"),
        ] {
            assert_eq!(
                code(run(&bound, protocol, &sql, &[]).await).0,
                "42501",
                "{protocol:?}: {sql}"
            );
        }
    }
    let approver = f.password("oncall_lead", APPROVER_PW).await;
    assert_eq!(
        changes(&approver, Protocol::Simple).await,
        [(change.to_string(), "open".to_owned(), None)]
    );
    // A password role that is not the approver: 42501 as well.
    let bystander = f.password("bystander", "by-pw").await;
    assert_eq!(
        code(run(&bystander, Protocol::Extended, &approve, &[]).await).0,
        "42501"
    );
    // The same role on the PostgreSQL listener approves.
    run(&approver, Protocol::Extended, &approve, &[])
        .await
        .unwrap();
    assert_eq!(changes(&approver, Protocol::Extended).await[0].1, "approved");
}

#[tokio::test]
async fn preview_with_bound_parameters() {
    let f = Fixture::new().await;
    let agent = f.agent().await;
    let approver = f.password("oncall_lead", APPROVER_PW).await;
    let sql = "PREVIEW UPDATE shipments SET status = $1 WHERE merchant_id = $2 AND id <= $3 \
               AND status = 'failed'";
    // Describe reports the wrapped UPDATE's parameters and the preview row.
    let statement = agent.prepare(sql).await.unwrap();
    assert_eq!(statement.params(), [Type::TEXT, Type::INT8, Type::INT8]);
    let names: Vec<_> = statement.columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, ["change_id", "digest", "row_count", "expires_at"]);
    let types: Vec<_> = statement.columns().iter().map(|c| c.type_().clone()).collect();
    assert_eq!(types, [Type::INT8, Type::TEXT, Type::INT8, Type::TIMESTAMPTZ]);
    let reply = run(
        &agent,
        Protocol::Extended,
        sql,
        &[&"queued", &42i64, &3i64],
    )
    .await
    .unwrap();
    let (change, digest, rows) = preview_row(&reply);
    assert_eq!((change, rows), (1, 3));
    // Other parameter values store another preview with its own digest.
    let other = run(
        &agent,
        Protocol::Extended,
        sql,
        &[&"queued", &42i64, &2i64],
    )
    .await
    .unwrap();
    let (other_change, other_digest, other_rows) = preview_row(&other);
    assert_eq!((other_change, other_rows), (2, 2));
    assert_ne!(digest, other_digest);
    // Apply re-runs the stored parameters: rows 1-3 only.
    let approve = format!("APPROVE CHANGE {change} OF TASK {TASK} DIGEST '{digest}'");
    run(&approver, Protocol::Extended, &approve, &[])
        .await
        .unwrap();
    let apply = format!("OPERATION 'p1' APPLY CHANGE {change} DIGEST '{digest}'");
    assert_eq!(
        run(&agent, Protocol::Extended, &apply, &[]).await.unwrap().count,
        3
    );
    assert_eq!(
        f.statuses(),
        ["queued", "queued", "queued", "failed", "delivered", "failed"]
    );
}

#[tokio::test]
async fn show_change_over_pgwire() {
    let f = Fixture::new().await;
    let agent = f.agent().await;
    let approver = f.password("oncall_lead", APPROVER_PW).await;
    let preview = run(&agent, Protocol::Simple, &format!("PREVIEW {REQUEUE}"), &[])
        .await
        .unwrap();
    let (change, _, _) = preview_row(&preview);
    let show = format!("SHOW CHANGE {change} OF TASK {TASK}");
    for protocol in PROTOCOLS {
        let statement = approver.prepare(&show).await.unwrap();
        let names: Vec<_> = statement.columns().iter().map(|c| c.name()).collect();
        assert_eq!(names, ["row_no", "key", "column_name", "old_value", "new_value"]);
        let reply = run(&approver, protocol, &show, &[]).await.unwrap();
        // One line per changed column: only status changes, on rows 1-4.
        assert_eq!(reply.rows.len(), 4, "{protocol:?}: {reply:?}");
        for (i, row) in reply.rows.iter().enumerate() {
            assert_eq!(row[0], Some((i + 1).to_string()), "{protocol:?}");
            assert!(row[1].as_deref().is_some_and(|key| !key.is_empty()));
            assert_eq!(row[2].as_deref(), Some("status"));
            assert_eq!(row[3].as_deref(), Some("failed"));
            assert_eq!(row[4].as_deref(), Some("queued"));
        }
    }
    // Unknown change id: 42704; unknown task: 42704; no privilege: 42501.
    let unknown = format!("SHOW CHANGE 99 OF TASK {TASK}");
    assert_eq!(code(run(&approver, Protocol::Simple, &unknown, &[]).await).0, "42704");
    let no_task = "SHOW CHANGE 1 OF TASK \"inc-999\"";
    assert_eq!(code(run(&approver, Protocol::Extended, no_task, &[]).await).0, "42704");
    let bystander = f.password("bystander", "by-pw").await;
    assert_eq!(code(run(&bystander, Protocol::Simple, &show, &[]).await).0, "42501");
    // Inside a transaction block the forms are refused (they commit on their own).
    approver.batch_execute("BEGIN").await.unwrap();
    assert_eq!(code(run(&approver, Protocol::Simple, &show, &[]).await).0, "25001");
    approver.batch_execute("ROLLBACK").await.unwrap();
}

#[tokio::test]
async fn apply_change_bad_hex_is_22023() {
    let f = Fixture::new().await;
    let agent = f.agent().await;
    let approver = f.password("oncall_lead", APPROVER_PW).await;
    let preview = run(&agent, Protocol::Simple, &format!("PREVIEW {REQUEUE}"), &[])
        .await
        .unwrap();
    let (change, digest, _) = preview_row(&preview);
    let short = &digest[..63];
    let not_hex = format!("{}g", &digest[..63]);
    let long = format!("{digest}0");
    for protocol in PROTOCOLS {
        for bad in [short, not_hex.as_str(), long.as_str(), "", "xyz"] {
            let apply = format!("OPERATION 'h1' APPLY CHANGE {change} DIGEST '{bad}'");
            assert_eq!(
                code(run(&agent, protocol, &apply, &[]).await).0,
                "22023",
                "{protocol:?}: {apply}"
            );
            let approve = format!("APPROVE CHANGE {change} OF TASK {TASK} DIGEST '{bad}'");
            assert_eq!(
                code(run(&approver, protocol, &approve, &[]).await).0,
                "22023",
                "{protocol:?}: {approve}"
            );
        }
        // A digest that is not a string literal is a syntax error.
        let unquoted = format!("OPERATION 'h1' APPLY CHANGE {change} DIGEST {digest}");
        assert_eq!(code(run(&agent, protocol, &unquoted, &[]).await).0, "42601");
    }
    assert_eq!(f.statuses(), BEFORE);
    assert_eq!(
        changes(&approver, Protocol::Simple).await,
        [(change.to_string(), "open".to_owned(), None)]
    );
}
