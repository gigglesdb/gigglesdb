#![cfg(feature = "engine-rocksdb")]
mod r3_support;

use giggles_core::{
    roles::TablePrivilege,
    task::{
        DenyRule, OperationId, PreviewId, ProtectedPoint, StatementClass, TaskError,
        context::TaskDefinitions, definition::TaskName, inspect_outcome,
    },
};
use giggles_server::auth::{PostgresqlConfig, TlsMode, serve_postgresql};
use r3_support::agent_support::{certificate, scratch, timestamptz};
use r3_support::{APP, APP_PW, EXECUTOR, EXECUTOR_PW, R3Fixture, TASK, inc_436_sql};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage, config::SslMode};
use tokio_postgres_rustls::MakeRustlsConnect;

#[derive(Debug)]
struct Reply {
    rows: Vec<Vec<Option<String>>>,
    count: u64,
}
async fn simple(client: &Client, sql: &str) -> Result<Reply, tokio_postgres::Error> {
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
            SimpleQueryMessage::CommandComplete(n) => reply.count = n,
            _ => {}
        }
    }
    Ok(reply)
}
fn failure(result: Result<Reply, tokio_postgres::Error>) -> (String, String) {
    let e = result.expect_err("statement must be refused");
    let e = e.as_db_error().unwrap();
    (e.code().code().into(), e.message().into())
}
fn expected(e: TaskError) -> (String, String) {
    (e.sqlstate().into(), e.message().into())
}
async fn agent(f: &R3Fixture) -> Client {
    // The cert + SQL-issued secret probe also accepted a wrong password:
    // this listener authenticates the known certificate, not the credential.
    // Use the fixture's certificate path deterministically.
    f.agent().await
}
const REQUEUE: &str = "UPDATE shipments SET status = 'queued' WHERE status = 'failed' AND id >= 26";
fn apply(change: i64, digest: &str) -> String {
    format!("OPERATION 'w1' APPLY CHANGE {change} DIGEST '{digest}'")
}
fn approve(change: i64, digest: &str) -> String {
    format!("APPROVE CHANGE {change} OF TASK \"{TASK}\" DIGEST '{digest}'")
}
async fn preview(agent: &Client) -> (i64, String) {
    let reply = simple(agent, &format!("PREVIEW {REQUEUE}")).await.unwrap();
    assert_eq!(reply.rows[0][2].as_deref(), Some("6"));
    (
        reply.rows[0][0].as_ref().unwrap().parse().unwrap(),
        reply.rows[0][1].clone().unwrap(),
    )
}
fn pause_at(
    target: ProtectedPoint,
    max_pauses: usize,
) -> (
    giggles_core::task::ProtectedHook,
    mpsc::UnboundedReceiver<()>,
    std::sync::mpsc::Sender<()>,
    Arc<AtomicUsize>,
) {
    let (paused_tx, paused_rx) = mpsc::unbounded_channel();
    let (go_tx, go_rx) = std::sync::mpsc::channel();
    let go_rx = Mutex::new(go_rx);
    let pauses = AtomicUsize::new(0);
    let hits = Arc::new(AtomicUsize::new(0));
    let hook_hits = hits.clone();
    let hook = Arc::new(move |point| {
        if point == ProtectedPoint::AfterStatement {
            hook_hits.fetch_add(1, Ordering::SeqCst);
        }
        if point == target && pauses.fetch_add(1, Ordering::SeqCst) < max_pauses {
            paused_tx.send(()).unwrap();
            go_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
        }
    });
    (hook, paused_rx, go_tx, hits)
}
async fn changed(controller: &Client) -> Vec<Vec<Option<String>>> {
    simple(controller, "SHOW TASK EXPOSURE \"inc-436\"")
        .await
        .unwrap()
        .rows
        .into_iter()
        .filter(|r| r[0].as_deref() == Some("changed"))
        .collect()
}

// DDL is raced before the table lock, and separately behind an in-flight apply.
async fn race(kind: u8) {
    let f = R3Fixture::start().await;
    f.create_inc_436();
    let agent = agent(&f).await;
    let approver = f.approver().await;
    let controller = f.controller().await;
    let app = f.app().await;
    let task = f
        .admin()
        .sql_session()
        .task_definition(&TaskName::new(TASK).unwrap())
        .unwrap()
        .unwrap()
        .id;
    if kind == 4 {
        assert_eq!(simple(&app, "SELECT id, status FROM shipments WHERE merchant_id = 42 AND id BETWEEN 1 AND 4 ORDER BY id").await.unwrap().rows,
            (1..=4).map(|id| vec![Some(id.to_string()), Some("failed".into())]).collect::<Vec<_>>());
        simple(
            &app,
            "UPDATE shipments SET status = 'pending' WHERE merchant_id = 42 AND id = 4",
        )
        .await
        .unwrap();
    }
    let before = changed(&controller).await;
    assert_eq!(before.len(), 3);
    let approved = if kind < 4 {
        let (id, digest) = preview(&agent).await;
        simple(&approver, &approve(id, &digest)).await.unwrap();
        Some((id, digest))
    } else {
        None
    };
    let approval_before = approved.as_ref().map(|(id, _)| {
        f.admin()
            .task_approval(task, PreviewId(*id as u64))
            .unwrap()
            .unwrap()
    });
    let write = approved.as_ref().map_or_else(
        || "OPERATION 'w1' UPDATE shipments SET status = 'queued' WHERE id BETWEEN 1 AND 4 AND status = 'failed'".into(),
        |(id, digest)| apply(*id, digest),
    );
    let (hook, mut paused, go, hits) = pause_at(
        if kind == 3 {
            ProtectedPoint::AfterHold
        } else {
            ProtectedPoint::AfterStatement
        },
        if kind == 4 { 3 } else { 1 },
    );
    f.admin().set_protected_hook(Some(hook)).unwrap();
    let other = async {
        let mut result = Ok(());
        for _ in 0..if kind == 4 { 3 } else { 1 } {
            if !matches!(
                tokio::time::timeout(Duration::from_secs(20), paused.recv()).await,
                Ok(Some(()))
            ) {
                result = Err("protected write never reached pause");
                break;
            }
            result = tokio::time::timeout(Duration::from_secs(3), async {
                match kind {
                    1 => { simple(&app, "INSERT INTO shipments VALUES (42, 32, 'failed', '2026-11-05 17:06:00+00', 1, 'new@example.com', 'tok_new')").await.unwrap(); }
                    2 => { let roles = f.admin().roles(); tokio::task::spawn_blocking(move || roles.revoke_table("shipments", EXECUTOR, &[TablePrivilege::Update])).await.unwrap().unwrap(); }
                    3 => { let admin = f.admin().clone(); tokio::task::spawn_blocking(move || admin.sql_session().execute("ALTER TABLE shipments ADD COLUMN note text", &[], None)).await.unwrap().unwrap(); }
                    4 => { simple(&app, "UPDATE shipments SET attempt_count = attempt_count + 1 WHERE merchant_id = 42 AND id = 4").await.unwrap(); }
                    _ => unreachable!()
                }
            }).await.map_err(|_| "concurrent writer blocked on paused commit");
            go.send(()).unwrap();
            if result.is_err() {
                break;
            }
        }
        result
    };
    let (written, other) = tokio::join!(simple(&agent, &write), other);
    f.admin().set_protected_hook(None).unwrap();
    other.expect("R3-1102 writer race");
    assert_eq!(
        failure(written),
        match kind {
            1 | 3 => expected(TaskError::ApprovalStale),
            2 => expected(TaskError::Denied(DenyRule::RolePrivilege)),
            4 => expected(TaskError::Retry),
            _ => unreachable!(),
        }
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        match kind {
            1 => 2,
            2 => 2,
            3 => 0,
            4 => 3,
            _ => unreachable!(),
        }
    );
    assert_eq!(f.count("merchant_id = 42 AND status = 'queued'"), 0);
    assert_eq!(
        f.count("merchant_id = 42 AND status = 'failed'"),
        if kind == 1 {
            32
        } else if kind == 4 {
            30
        } else {
            31
        }
    );
    assert_eq!(changed(&controller).await, before);
    assert!(
        inspect_outcome(
            &f.admin().sql_session(),
            task,
            &OperationId::new("w1").unwrap()
        )
        .unwrap()
        .is_none()
    );
    assert!(
        simple(&agent, "SHOW OPERATION 'w1'")
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    if let Some((id, digest)) = approved {
        assert_eq!(
            f.admin().task_approval(task, PreviewId(id as u64)).unwrap(),
            approval_before
        );
        if kind == 1 {
            let fresh = simple(&agent, &format!("PREVIEW {REQUEUE}")).await.unwrap();
            assert_eq!(fresh.rows[0][2].as_deref(), Some("7"));
        } else if kind == 2 {
            f.admin()
                .roles()
                .grant_table("shipments", EXECUTOR, &[TablePrivilege::Update])
                .unwrap();
            assert_eq!(simple(&agent, &apply(id, &digest)).await.unwrap().count, 6);
        } else {
            let (id, digest) = preview(&agent).await;
            simple(&approver, &approve(id, &digest)).await.unwrap();
            let (hook, mut paused, go, hits) = pause_at(ProtectedPoint::AfterStatement, 1);
            f.admin().set_protected_hook(Some(hook)).unwrap();
            let ddl = async {
                tokio::time::timeout(Duration::from_secs(20), paused.recv())
                    .await
                    .unwrap()
                    .unwrap();
                let admin = f.admin().clone();
                let mut alter = tokio::task::spawn_blocking(move || {
                    admin.sql_session().execute(
                        "ALTER TABLE shipments ADD COLUMN note2 text",
                        &[],
                        None,
                    )
                });
                let blocked = tokio::time::timeout(Duration::from_secs(1), &mut alter)
                    .await
                    .is_err();
                go.send(()).unwrap();
                let result = tokio::time::timeout(Duration::from_secs(10), alter)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(
                    blocked,
                    "engine serialises DDL behind the in-flight apply's table lock"
                );
                result.unwrap();
            };
            let write = apply(id, &digest).replace("'w1'", "'w2'");
            let (written, ()) = tokio::join!(
                tokio::time::timeout(Duration::from_secs(10), simple(&agent, &write)),
                ddl
            );
            f.admin().set_protected_hook(None).unwrap();
            assert_eq!(written.unwrap().unwrap().count, 6);
            assert_eq!(hits.load(Ordering::SeqCst), 1);
        }
    } else {
        assert_eq!(
            f.count("merchant_id = 42 AND id BETWEEN 1 AND 3 AND status = 'failed'"),
            3
        );
        assert_eq!(simple(&agent, &write).await.unwrap().count, 3);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_of_matching_row_before_commit_rejects_apply() {
    race(1).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_revoke_of_executor_privilege_rejects_apply() {
    race(2).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_alter_table_rejects_apply() {
    race(3).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_update_of_read_row_rejects_direct_write() {
    race(4).await;
}

async fn ordinary(
    address: std::net::SocketAddr,
    user: &str,
    cert: Option<rustls::ClientConfig>,
) -> Result<Client, tokio_postgres::Error> {
    let mut config = tokio_postgres::Config::new();
    config
        .host("localhost")
        .hostaddr(address.ip())
        .port(address.port())
        .user(user)
        .dbname("postgres");
    if let Some(cert) = cert {
        let (client, connection) = config
            .ssl_mode(SslMode::Require)
            .connect(MakeRustlsConnect::new(cert))
            .await?;
        tokio::spawn(connection);
        Ok(client)
    } else {
        let (client, connection) = config.ssl_mode(SslMode::Disable).connect(NoTls).await?;
        tokio::spawn(connection);
        Ok(client)
    }
}
fn login_refusal(result: Result<Client, tokio_postgres::Error>, code: &str) {
    let error = result.expect_err("executor must not log in on PostgreSQL listener");
    assert_eq!(error.code().unwrap().code(), code, "{error:?}");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn executor_role_refused_on_postgresql_listener_scram_cert_and_trust() {
    let f = R3Fixture::start().await;
    login_refusal(f.try_password(EXECUTOR, EXECUTOR_PW).await, "28P01");
    let control = f.try_password(APP, APP_PW).await.unwrap();
    simple(&control, "SELECT 1").await.unwrap();
    let dir = scratch("r3-exit-listeners");
    let cert_path = dir.join("server.pem");
    let key_path = dir.join("server.key");
    let ca_path = dir.join("ca.pem");
    std::fs::write(&cert_path, f.server_cert.cert.pem()).unwrap();
    std::fs::write(&key_path, f.server_cert.key.serialize_pem()).unwrap();
    std::fs::write(&ca_path, f.ca.cert.pem()).unwrap();
    let app_cert = certificate(Some(&f.ca), false);
    for tls in [true, false] {
        let config = PostgresqlConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            tls: if tls {
                TlsMode::Required
            } else {
                TlsMode::Disabled
            },
            certificate: tls.then(|| cert_path.to_str().unwrap().into()),
            private_key: tls.then(|| key_path.to_str().unwrap().into()),
            client_ca: tls.then(|| ca_path.to_str().unwrap().into()),
            certificates: if tls {
                BTreeMap::from([
                    (f.agent_cert.fingerprint(), EXECUTOR.into()),
                    (app_cert.fingerprint(), APP.into()),
                ])
            } else {
                BTreeMap::new()
            },
            databases: vec!["postgres".into()],
            loopback_trust: !tls,
            connections: 8,
        };
        let listener = serve_postgresql(
            config.listen,
            f.service().clone(),
            Arc::new(config.build().unwrap()),
        )
        .await
        .unwrap();
        let result = ordinary(
            listener.address,
            EXECUTOR,
            tls.then(|| f.agent_cert.client(&f.ca)),
        )
        .await;
        let control = ordinary(listener.address, APP, tls.then(|| app_cert.client(&f.ca))).await;
        listener.shutdown().await.unwrap();
        login_refusal(result, "28P01");
        let control = control.unwrap();
        // Successful startup itself is the positive control.
        drop(control);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_statement_switches_into_executor_role() {
    let f = R3Fixture::start().await;
    f.create_inc_436();
    let controller = f.controller().await;
    for sql in [
        "SET ROLE repair_agent",
        "SET SESSION AUTHORIZATION repair_agent",
        "SET ROLE oncall_lead",
        "SET SESSION AUTHORIZATION oncall_lead",
        "GRANT oncall_lead TO incident_controller",
        "GRANT repair_agent TO incident_controller",
        "GRANT repair_agent TO incident_controller WITH ADMIN OPTION",
    ] {
        let refused = failure(simple(&controller, sql).await);
        // SET ROLE and membership are unsupported for every role, including
        // the ordinary approver control, rather than executor-specific checks.
        assert_eq!(
            refused,
            if sql.starts_with("SET") {
                (
                    "42704".into(),
                    "unrecognized configuration parameter".into(),
                )
            } else {
                (
                    "0A000".into(),
                    "unsupported SQL feature or parameter type in this server profile".into(),
                )
            },
            "{sql}"
        );
        assert_eq!(
            simple(&controller, "SELECT current_user")
                .await
                .unwrap()
                .rows[0][0]
                .as_deref(),
            Some("incident_controller")
        );
    }
    let agent = agent(&f).await;
    assert_eq!(
        failure(simple(&agent, "SET ROLE incident_controller").await),
        expected(TaskError::NotAllowedInTaskSession(StatementClass::Set))
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn executor_cannot_approve_undo_or_create_task() {
    let f = R3Fixture::start().await;
    f.create_inc_436();
    let agent = agent(&f).await;
    let (id, digest) = preview(&agent).await;
    let task = f
        .admin()
        .sql_session()
        .task_definition(&TaskName::new(TASK).unwrap())
        .unwrap()
        .unwrap()
        .id;
    let create = inc_436_sql(&timestamptz(f.expires_ms)).replace("inc-436", "forbidden");
    for (sql, class) in [
        (approve(id, &digest), StatementClass::Other),
        (
            "PREVIEW UNDO OF TASK \"inc-436\"".into(),
            StatementClass::Other,
        ),
        (
            format!(
                "APPROVE UNDO 1 OF TASK \"inc-436\" DIGEST '{}'",
                "0".repeat(64)
            ),
            StatementClass::Other,
        ),
        (create.clone(), StatementClass::RoleStatement),
    ] {
        assert_eq!(
            failure(simple(&agent, &sql).await),
            expected(TaskError::NotAllowedInTaskSession(class)),
            "{sql}"
        );
    }
    assert!(
        f.admin()
            .task_approval(task, PreviewId(id as u64))
            .unwrap()
            .is_none()
    );
    assert!(f.admin().tasks().get("forbidden").unwrap().is_none());
    let controller = f.controller().await;
    let refused = failure(
        simple(
            &controller,
            &create.replace("APPROVER oncall_lead", "APPROVER repair_agent"),
        )
        .await,
    );
    assert_eq!(
        refused,
        (
            "22023".into(),
            "task approver must be an ordinary role other than the executor".into()
        )
    );
    let refused = failure(
        simple(
            &controller,
            "GRANT APPROVE ON TASK \"inc-436\" TO repair_agent",
        )
        .await,
    );
    assert_eq!(
        refused,
        (
            "0A000".into(),
            "unsupported SQL feature or parameter type in this server profile".into()
        )
    );
    assert!(
        f.admin()
            .task_approval(task, PreviewId(id as u64))
            .unwrap()
            .is_none()
    );
}
