//! R3-1104 / D11.2: real process termination and v2 reopen over both listeners.
//! Preview/approval have no injected seam: kill before send, in flight, or after ack.
#![cfg(feature = "engine-rocksdb")]
// Like all.rs/network.rs, trusted test setup may use the plain core exits.
#![allow(clippy::disallowed_methods)]
#[path = "agent_support/mod.rs"]
mod agent_support;

use agent_support::{certificate, no_cert_client, now_ms, scratch, timestamptz};
use giggles_core::{
    Admin, CrashPoint, Engine, Limits,
    roles::{NewRole, TablePrivilege},
    task::{OperationId, Outcome, ProtectedPoint, inspect_outcome},
};
use giggles_server::{
    Service,
    admission::ServiceLimits,
    agent::serve_agent_pg_bound,
    auth::{PostgresqlConfig, TlsMode, scram::Verifier, serve_postgresql},
    clock::Clock,
    tls,
};
use rustls::pki_types::PrivatePkcs8KeyDer;
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{net::TcpListener, sync::watch};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage, config::SslMode};
use tokio_postgres_rustls::MakeRustlsConnect;

const TASK: &str = "\"crash-task\"";
const UPDATE: &str = "UPDATE shipments SET status = 'queued' WHERE id <= 4";
const WRITE: &str = "OPERATION 'target' UPDATE shipments SET status = 'queued' WHERE id = 1";
const PROBE: &str = "OPERATION 'probe' UPDATE shipments SET status = 'queued' WHERE id = 2";
const DENIED: &str = "OPERATION 'target' UPDATE shipments SET status = 'forbidden' WHERE id = 1";
const WAIT: Duration = Duration::from_secs(30);
type Rows = Vec<Vec<Option<String>>>;

// This test is also the subprocess entry point. libtest's banner is ignored by
// the parent's control reader; only the explicit `ok` line acknowledges arming.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_child() {
    let Ok(root) = std::env::var("GIGGLES_R3_CRASH_CHILD") else {
        return;
    };
    let root = PathBuf::from(root);
    let db = root.join("db");
    let admin = if db.exists() {
        Admin::open(&db, Limits::default()).unwrap()
    } else {
        let admin = Admin::create_engine(&db, Limits::default(), Engine::V2).unwrap();
        let roles = admin.roles();
        for (name, task_only) in [
            ("controller", false),
            ("approver", false),
            ("executor", true),
        ] {
            roles
                .create(NewRole {
                    name: name.into(),
                    login: true,
                    password: Some(Verifier::generate("password")),
                    ..NewRole::default()
                })
                .unwrap();
            if task_only {
                roles.set_task_only(name, true).unwrap();
            }
        }
        admin
            .sql_session()
            .execute(
                "CREATE TABLE shipments (id bigint primary key, status text)",
                &[],
                None,
            )
            .unwrap();
        for role in ["controller", "approver", "executor"] {
            roles
                .grant_table(
                    "shipments",
                    role,
                    &[
                        TablePrivilege::Select,
                        TablePrivilege::Update,
                        TablePrivilege::Insert,
                    ],
                )
                .unwrap();
        }
        admin
    };
    let ca = certificate(None, false);
    let cert = certificate(Some(&ca), true);
    std::fs::write(root.join("ca.der"), ca.cert.der()).unwrap();
    let tls = tls::server_optional_client(
        vec![cert.cert.der().clone()],
        PrivatePkcs8KeyDer::from(cert.key.serialize_der()).into(),
        Some(vec![ca.cert.der().clone()]),
    )
    .unwrap();
    let service = Service::with_agent(
        admin.clone(),
        vec![],
        vec![],
        Clock::new(0, 3600).unwrap(),
        ServiceLimits::default(),
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
    let pg = serve_postgresql(config.listen, service.clone(), Arc::new(listener))
        .await
        .unwrap();
    let (_open, start) = watch::channel(true);
    let agent = serve_agent_pg_bound(
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        service,
        tls,
        16,
        vec!["giggles".into()],
        start,
    )
    .await
    .unwrap();
    std::fs::write(
        root.join("addresses.pending"),
        format!("{}\n{}\n", pg.address, agent.address),
    )
    .unwrap();
    std::fs::rename(root.join("addresses.pending"), root.join("addresses")).unwrap();
    // A dedicated blocking thread leaves both async runtime threads available.
    tokio::task::spawn_blocking(move || {
        for line in std::io::stdin().lock().lines() {
            let line = line.unwrap();
            admin.set_crash_point(CrashPoint::None).unwrap();
            admin.set_protected_point(None).unwrap();
            match line.as_str() {
                "disarm" => {}
                "arm crash BeforeCommit" => {
                    admin.set_crash_point(CrashPoint::BeforeCommit).unwrap()
                }
                "arm crash AfterCommit" => admin.set_crash_point(CrashPoint::AfterCommit).unwrap(),
                "arm crash BeforeAck" => admin.set_crash_point(CrashPoint::BeforeAck).unwrap(),
                other => {
                    let point = match other.strip_prefix("arm protected ").unwrap() {
                        "AfterLookup" => ProtectedPoint::AfterLookup,
                        "AfterHold" => ProtectedPoint::AfterHold,
                        "AfterStatement" => ProtectedPoint::AfterStatement,
                        "AfterDecision" => ProtectedPoint::AfterDecision,
                        "AfterDenialRollback" => ProtectedPoint::AfterDenialRollback,
                        "AfterDenialStage" => ProtectedPoint::AfterDenialStage,
                        _ => panic!("unknown control: {other}"),
                    };
                    admin.set_protected_point(Some(point)).unwrap();
                }
            }
            println!("ok");
            std::io::stdout().flush().unwrap();
        }
    })
    .await
    .unwrap();
}

struct TestDir(PathBuf);
impl TestDir {
    fn new() -> Self {
        Self(scratch("r3-crash"))
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Server {
    child: Child,
    replies: std::sync::mpsc::Receiver<String>,
    pg: SocketAddr,
    agent: SocketAddr,
    ca: rustls::ClientConfig,
}
impl Server {
    async fn start(root: &Path) -> Self {
        let ready = root.join("addresses");
        if ready.exists() {
            std::fs::remove_file(&ready).unwrap();
        }
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_child", "--nocapture"])
            .env("GIGGLES_R3_CRASH_CHILD", root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        // Establish the guard before waiting for readiness, including on panic.
        let dummy_ca = certificate(None, false);
        let mut server = Self {
            child,
            replies: std::sync::mpsc::channel().1,
            pg: "127.0.0.1:0".parse().unwrap(),
            agent: "127.0.0.1:0".parse().unwrap(),
            ca: no_cert_client(&dummy_ca),
        };
        let stdout = server.child.stdout.take().unwrap();
        let (send, recv) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                match line {
                    Ok(line) => {
                        if send.send(line).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        server.replies = recv;
        let deadline = Instant::now() + WAIT;
        while !ready.exists() {
            assert!(
                server.child.try_wait().unwrap().is_none(),
                "child died during startup"
            );
            assert!(Instant::now() < deadline, "child startup timeout");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let addresses = std::fs::read_to_string(ready).unwrap();
        let mut lines = addresses.lines();
        server.pg = lines.next().unwrap().parse().unwrap();
        server.agent = lines.next().unwrap().parse().unwrap();
        let der = std::fs::read(root.join("ca.der")).unwrap();
        server.ca = rustls::ClientConfig::builder()
            .with_root_certificates(tls::roots(vec![der.into()]).unwrap())
            .with_no_client_auth();
        server
    }
    fn arm(&mut self, point: &str) {
        writeln!(self.child.stdin.as_mut().unwrap(), "arm {point}").unwrap();
        self.child.stdin.as_mut().unwrap().flush().unwrap();
        let deadline = Instant::now() + WAIT;
        loop {
            let line = self
                .replies
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap();
            if line == "ok" {
                break;
            }
        }
    }
    async fn password(&self, user: &str) -> Client {
        let (client, connection) = tokio_postgres::Config::new()
            .hostaddr(self.pg.ip())
            .port(self.pg.port())
            .user(user)
            .password("password")
            .dbname("postgres")
            .ssl_mode(SslMode::Disable)
            .connect_timeout(WAIT)
            .connect(NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        client
    }
    async fn executor(&self, secret: &str) -> Client {
        let (client, connection) = tokio_postgres::Config::new()
            .host("localhost")
            .hostaddr(self.agent.ip())
            .port(self.agent.port())
            .user("executor")
            .password(secret)
            .dbname("giggles")
            .ssl_mode(SslMode::Require)
            .connect_timeout(WAIT)
            .connect(MakeRustlsConnect::new(self.ca.clone()))
            .await
            .unwrap();
        tokio::spawn(connection);
        client
    }
    async fn crashed(&mut self) {
        let deadline = Instant::now() + WAIT;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert_eq!(status.code(), Some(86), "wrong crash exit: {status}");
                break;
            }
            assert!(Instant::now() < deadline, "crash seam was not reached");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    fn kill(&mut self) {
        // The subprocess starts no descendants: killing and waiting terminates
        // the entire server (all runtime/storage threads) without destructors.
        if self.child.try_wait().unwrap().is_none() {
            self.child.kill().unwrap();
        }
        self.child.wait().unwrap();
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.kill();
    }
}

async fn query(client: &Client, sql: &str) -> Rows {
    tokio::time::timeout(WAIT, client.simple_query(sql))
        .await
        .unwrap()
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}
async fn setup(server: &Server) -> String {
    let controller = server.password("controller").await;
    query(
        &controller,
        "INSERT INTO shipments VALUES (1,'failed'),(2,'failed'),(3,'failed'),(4,'failed')",
    )
    .await;
    query(
        &controller,
        &format!(
            "CREATE TASK {TASK} EXECUTOR executor APPROVER approver ON shipments \
         VISIBLE ROWS WHERE (id > 0) READABLE (id, status) \
         ALLOW UPDATE (status) OLD ROW WHERE (status = 'failed') NEW ROW WHERE (status = 'queued') \
         AT MOST 25 DISTINCT ROWS AND 25 TRANSITIONS APPROVE ABOVE 2 \
         DISCLOSE AT MOST 1000 ROWS READ, 2097152 BYTES RETURNED \
         WITH (statement_ms = 5000, rows_scanned = 10000, concurrent_statements = 2) EXPIRES '{}'",
            timestamptz(now_ms() + 86_400_000)
        ),
    )
    .await;
    let rows = query(&controller, &format!("ISSUE CREDENTIAL FOR TASK {TASK}")).await;
    rows[0][2].clone().unwrap()
}
async fn crash_query(server: &mut Server, client: &Client, seam: &str, sql: &str) {
    server.arm(seam);
    let error = tokio::time::timeout(WAIT, client.simple_query(sql))
        .await
        .unwrap()
        .expect_err("connection must drop");
    assert!(
        error.as_db_error().is_none(),
        "expected disconnect, got {error}"
    );
    server.crashed().await;
}

// Check both TCP-visible state and full inspector receipts. The latter loads
// the header and all change rows as it reconstructs them.
// Physical row version numbers and orphan keys have no public inspection API.
#[allow(clippy::too_many_arguments)] // Keep each matrix case's expected state together.
async fn check_invariants(
    root: &Path,
    server: &mut Server,
    secret: &str,
    queued: &[usize],
    operation: Option<&str>,
    committed: bool,
    denied: bool,
    transitions: u64,
) {
    let controller = server.password("controller").await;
    let rows = query(&controller, "SELECT id,status FROM shipments ORDER BY id").await;
    assert_eq!(rows.len(), 4);
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row[0], Some((i + 1).to_string()));
        assert_eq!(
            row[1].as_deref(),
            Some(if queued.contains(&(i + 1)) { "queued" } else { "failed" })
        );
    }
    query(&controller, &format!("SHOW TASK {TASK}")).await;
    query(&controller, &format!("SHOW TASK READS {TASK}")).await;
    let agent = server.executor(secret).await;
    if let Some(op) = operation {
        let outcome = query(&agent, &format!("SHOW OPERATION '{op}'")).await;
        if committed || denied {
            assert_eq!(outcome.len(), 1, "{outcome:?}");
            assert_eq!(
                outcome[0][1].as_deref(),
                Some(if denied { "denied" } else { "committed" })
            );
        } else {
            assert!(outcome.is_empty(), "uncommitted outcome: {outcome:?}");
        }
    }
    drop(agent);
    drop(controller);
    server.kill();
    let admin = Admin::open(root.join("db"), Limits::default()).unwrap();
    let task = admin.tasks().get("crash-task").unwrap().unwrap().id;
    assert!(admin.task_read_verify(task).unwrap().ok());
    let mut sum = 0;
    // `probe` is a fresh write after reopen: its receipt's counters must
    // continue from the committed receipts, so no head was leaked or lost.
    for op in ["seed", "target", "probe"] {
        let outcome =
            inspect_outcome(&admin.sql_session(), task, &OperationId::new(op).unwrap()).unwrap();
        if let Some(Outcome::Committed(receipt)) = &outcome {
            let (kind, count) = receipt.change_summary().expect("committed header and rows");
            assert_eq!(kind, 1);
            assert_eq!(count, receipt.rows_affected);
            sum += u64::from(receipt.transitions_charged);
            assert_eq!(receipt.counters_after(), Some((sum, sum)));
        }
        if operation == Some(op) {
            assert_eq!(matches!(outcome, Some(Outcome::Committed(_))), committed);
            assert_eq!(matches!(outcome, Some(Outcome::Denied(_))), denied);
            assert_eq!(outcome.is_some(), committed || denied);
        }
    }
    assert_eq!(sum, transitions, "sum of receipt charges");
}

async fn direct_cases(seams: &[(&str, bool)], denial: bool) {
    for &(seam, durable) in seams {
        let dir = TestDir::new();
        let mut server = Server::start(&dir.0).await;
        let secret = setup(&server).await;
        let agent = server.executor(&secret).await;
        crash_query(
            &mut server,
            &agent,
            seam,
            if denial { DENIED } else { WRITE },
        )
        .await;
        drop(agent);
        drop(server);
        let mut server = Server::start(&dir.0).await;
        let agent = server.executor(&secret).await;
        if durable {
            // Reusing the persisted credential and operation ID must replay.
            if denial {
                let err = agent.simple_query(DENIED).await.unwrap_err();
                assert_eq!(err.code().unwrap().code(), "42501");
            } else {
                query(&agent, WRITE).await;
                query(&agent, WRITE).await;
            }
        }
        query(&agent, PROBE).await;
        drop(agent);
        let target = durable && !denial;
        check_invariants(
            &dir.0,
            &mut server,
            &secret,
            if target { &[1, 2] } else { &[2] },
            Some("target"),
            target,
            durable && denial,
            u64::from(target) + 1,
        )
        .await;
    }
}

#[tokio::test]
async fn crash_before_statement() {
    direct_cases(
        &[
            ("protected AfterLookup", false),
            ("protected AfterHold", false),
        ],
        false,
    )
    .await;
}
#[tokio::test]
async fn crash_after_statement() {
    direct_cases(
        &[
            ("protected AfterStatement", false),
            ("protected AfterDecision", false),
        ],
        false,
    )
    .await;
}
#[tokio::test]
async fn crash_before_commit() {
    direct_cases(&[("crash BeforeCommit", false)], false).await;
}
#[tokio::test]
async fn crash_after_commit_before_ack() {
    direct_cases(
        &[("crash AfterCommit", true), ("crash BeforeAck", true)],
        false,
    )
    .await;
}
#[tokio::test]
async fn crash_around_denial() {
    direct_cases(
        &[
            ("protected AfterDenialRollback", false),
            ("protected AfterDenialStage", false),
            ("crash BeforeCommit", false),
            ("crash AfterCommit", true),
        ],
        true,
    )
    .await;
}

async fn preview(agent: &Client) -> (String, String) {
    let rows = query(agent, &format!("PREVIEW {UPDATE}")).await;
    assert_eq!(rows.len(), 1);
    (rows[0][0].clone().unwrap(), rows[0][1].clone().unwrap())
}
fn approve(id: &str, digest: &str) -> String {
    format!("APPROVE CHANGE {id} OF TASK {TASK} DIGEST '{digest}'")
}
fn apply(id: &str, digest: &str) -> String {
    format!("OPERATION 'target' APPLY CHANGE {id} DIGEST '{digest}'")
}
async fn change_state(server: &Server, id: &str) -> String {
    let approver = server.password("approver").await;
    let changes = query(&approver, &format!("SHOW CHANGES OF TASK {TASK}")).await;
    let row = changes
        .iter()
        .find(|r| r[0].as_deref() == Some(id))
        .unwrap();
    row[2].clone().unwrap()
}

#[derive(Clone, Copy, Debug)]
enum KillAt {
    Before,
    InFlight,
    After,
}
// Start polling the query before signalling the killer. Unlike constructing an
// unpolled query future, this actually hands its request to the wire driver.
async fn kill_around(server: &mut Server, client: Client, sql: String, at: KillAt) {
    match at {
        KillAt::Before => server.kill(),
        KillAt::After => {
            query(&client, &sql).await;
            server.kill();
        }
        KillAt::InFlight => {
            let mut request = Box::pin(client.simple_query(&sql));
            tokio::select! {
                biased;
                _ = &mut request => {},
                _ = tokio::task::yield_now() => {},
            }
            server.kill();
            let _ = tokio::time::timeout(WAIT, request).await;
        }
    }
}

async fn unseamed(approval: bool) {
    for at in [KillAt::Before, KillAt::InFlight, KillAt::After] {
        let dir = TestDir::new();
        let mut server = Server::start(&dir.0).await;
        let secret = setup(&server).await;
        let agent = server.executor(&secret).await;
        let id = if approval {
            let (id, digest) = preview(&agent).await;
            let approver = server.password("approver").await;
            kill_around(&mut server, approver, approve(&id, &digest), at).await;
            Some(id)
        } else {
            kill_around(&mut server, agent, format!("PREVIEW {UPDATE}"), at).await;
            None
        };
        drop(server);
        let mut server = Server::start(&dir.0).await;
        if let Some(id) = id {
            let state = change_state(&server, &id).await;
            match at {
                KillAt::Before => assert_eq!(state, "open"),
                KillAt::After => assert_eq!(state, "approved"),
                KillAt::InFlight => assert!(["open", "approved"].contains(&state.as_str())),
            }
        } else {
            let approver = server.password("approver").await;
            let changes = query(&approver, &format!("SHOW CHANGES OF TASK {TASK}")).await;
            match at {
                KillAt::Before => assert!(changes.is_empty()),
                KillAt::After => assert_eq!(changes.len(), 1),
                KillAt::InFlight => assert!(changes.len() <= 1),
            }
            if let Some(row) = changes.first() {
                assert_eq!(row[2].as_deref(), Some("open"));
                let id = row[0].as_deref().unwrap();
                query(&approver, &format!("SHOW CHANGE {id} OF TASK {TASK}")).await;
            }
        }
        check_invariants(
            &dir.0,
            &mut server,
            &secret,
            &[],
            Some("target"),
            false,
            false,
            0,
        )
        .await;
    }
}
#[tokio::test]
async fn crash_around_preview_store() {
    unseamed(false).await;
}
#[tokio::test]
async fn crash_around_approve() {
    unseamed(true).await;
}

#[tokio::test]
async fn crash_around_apply() {
    for (seam, durable) in [("crash BeforeCommit", false), ("crash AfterCommit", true)] {
        let dir = TestDir::new();
        let mut server = Server::start(&dir.0).await;
        let secret = setup(&server).await;
        let agent = server.executor(&secret).await;
        let (id, digest) = preview(&agent).await;
        query(&server.password("approver").await, &approve(&id, &digest)).await;
        crash_query(&mut server, &agent, seam, &apply(&id, &digest)).await;
        drop(agent);
        drop(server);
        let mut server = Server::start(&dir.0).await;
        assert_eq!(
            change_state(&server, &id).await,
            if durable { "applied" } else { "approved" }
        );
        if durable {
            query(&server.executor(&secret).await, &apply(&id, &digest)).await;
        }
        check_invariants(
            &dir.0,
            &mut server,
            &secret,
            if durable { &[1, 2, 3, 4] } else { &[] },
            Some("target"),
            durable,
            false,
            4 * u64::from(durable),
        )
        .await;
        let admin = Admin::open(dir.0.join("db"), Limits::default()).unwrap();
        let task = admin.tasks().get("crash-task").unwrap().unwrap().id;
        let approval = admin
            .task_approval(task, giggles_core::task::PreviewId(id.parse().unwrap()))
            .unwrap()
            .expect("persisted approval");
        assert_eq!(approval.consumed_seq, u64::from(durable));
        assert_eq!(approval.approver, "approver");
    }
}

#[tokio::test]
async fn crash_around_undo() {
    for (seam, durable) in [("crash BeforeCommit", false), ("crash AfterCommit", true)] {
        let dir = TestDir::new();
        let mut server = Server::start(&dir.0).await;
        let secret = setup(&server).await;
        let agent = server.executor(&secret).await;
        query(&agent, &WRITE.replace("'target'", "'seed'")).await;
        let approver = server.password("approver").await;
        let rows = query(&approver, &format!("PREVIEW UNDO OF TASK {TASK}")).await;
        let id = rows[0][0].as_deref().unwrap();
        let digest = rows[0][1].as_deref().unwrap();
        let undo = format!("APPROVE UNDO {id} OF TASK {TASK} DIGEST '{digest}' MODE COMPLETE");
        crash_query(&mut server, &approver, seam, &undo).await;
        drop(approver);
        drop(agent);
        drop(server);
        let mut server = Server::start(&dir.0).await;
        assert_eq!(
            change_state(&server, id).await,
            if durable { "applied" } else { "open" }
        );
        let approver = server.password("approver").await;
        if durable {
            // Replay the same undo: it must not create a second receipt.
            let replay = query(&approver, &undo).await;
            assert_eq!(replay.len(), 1);
            assert_eq!(replay[0][1].as_deref(), Some("undo_complete"));
            assert_eq!(replay[0][2].as_deref(), Some("2"));
            assert_eq!(replay[0][5].as_deref(), Some("1"));
            assert_eq!(replay[0][8].as_deref(), Some("1"));
            for i in [9, 10] {
                assert_eq!(replay[0][i].as_deref(), Some("0"));
            }
            for i in [11, 12] {
                assert_eq!(replay[0][i].as_deref(), Some("1"));
            }
            assert_eq!(query(&approver, &undo).await, replay);
        }
        let rows = query(&approver, &format!("SHOW UNDO {id} OF TASK {TASK}")).await;
        assert_eq!(rows.len(), usize::from(!durable));
        check_invariants(
            &dir.0,
            &mut server,
            &secret,
            if durable { &[] } else { &[1] },
            None,
            false,
            false,
            1,
        )
        .await;
    }
}

#[tokio::test]
async fn page_sent_is_never_uncharged() {
    // Agent pgwire refuses nonzero portal max_rows. Use keyset LIMIT pages,
    // as task_safe_read_network does, and interrupt the second page's debit.
    for seam in ["crash BeforeCommit", "crash AfterCommit", "crash BeforeAck"] {
        let dir = TestDir::new();
        let mut server = Server::start(&dir.0).await;
        let secret = setup(&server).await;
        let agent = server.executor(&secret).await;
        let first = query(
            &agent,
            "SELECT id FROM shipments WHERE id > 0 ORDER BY id LIMIT 2",
        )
        .await;
        assert_eq!(first.len(), 2);
        server.arm(seam);
        let mut received = first.len() as u64;
        // simple_query buffers rows, so query_raw would hide a partial page.
        // Poll its stream directly and count every DataRow delivered to us.
        use std::future::poll_fn;
        let stream = agent
            .simple_query_raw("SELECT id FROM shipments WHERE id > 2 ORDER BY id LIMIT 2")
            .await;
        if let Ok(stream) = stream {
            let mut stream = Box::pin(stream);
            use futures_core::Stream;
            while let Some(message) =
                tokio::time::timeout(WAIT, poll_fn(|cx| stream.as_mut().poll_next(cx)))
                    .await
                    .unwrap()
            {
                match message {
                    Ok(SimpleQueryMessage::Row(_)) => received += 1,
                    Err(_) => break,
                    _ => {}
                }
            }
        }
        server.crashed().await;
        drop(agent);
        drop(server);
        let mut server = Server::start(&dir.0).await;
        check_invariants(&dir.0, &mut server, &secret, &[], None, false, false, 0).await;
        let admin = Admin::open(dir.0.join("db"), Limits::default()).unwrap();
        let task = admin.tasks().get("crash-task").unwrap().unwrap().id;
        let disclosure = admin.task_disclosure(task).unwrap();
        // Reservation chunks are durably charged before release; unused
        // reservations are not refunded when the process dies.
        assert!(
            disclosure.rows_returned >= received,
            "received {received}, charged {disclosure:?}"
        );
        assert_eq!(
            disclosure
                .debits
                .iter()
                .map(|d| d.rows_returned + d.reserved_rows)
                .sum::<u64>(),
            disclosure.rows_returned
        );
    }
}
