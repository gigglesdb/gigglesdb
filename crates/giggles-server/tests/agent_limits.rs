//! R3-403 (GD-0061 design-3 section 7): the agent statement ceiling
//! (`agent.max_concurrent_statements`) and agent admission. Agent requests take
//! an agent slot before a blocking worker, a full admission is 54T01 at once,
//! and applications keep the rest of the pool.
//!
//! Requests are held in flight by a table lock an application transaction
//! holds (an uncommitted ALTER); the tests wait on the server's own counters,
//! never on elapsed time, and release the lock to let the waiters finish.
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::{
    AGENT_STARTUP, DAY_MS, Msg, TestCert, Wire, binding, certificate, connect_raw, error_of,
    now_ms, scratch, timestamptz,
};
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    roles::{NewRole, TablePrivilege},
    task::definition::{
        AllowedWrite, DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText, WallClockMs,
    },
};
use giggles_server::{
    Binding, Role, Running, Service,
    admission::ServiceLimits,
    agent::{AgentConfig, AgentLimits, serve_agent_pg_bound},
    clock::Clock,
    serve_pg_bound, tls,
};
use rustls::pki_types::PrivatePkcs8KeyDer;
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::mpsc, sync::watch, task::JoinHandle};

const APP_SELECT: &str = "SELECT id FROM shipments";
/// Rows 1..=ROWS, merchant 42, all `failed`.
const ROWS: usize = 24;

/// Agent request `i`: a protected update of its own row, admitted for task
/// sessions (R3-626), which waits on the held table lock.
fn agent_update(i: usize) -> String {
    format!(
        "OPERATION 'op-{i}' UPDATE shipments SET status = 'queued'          WHERE id = {i} AND status = 'failed'"
    )
}

fn admin_with(limits: Limits) -> Admin {
    let path = scratch("agent-limits");
    let admin = Admin::create_engine(path.join("db.redb"), limits, Engine::V2).unwrap();
    let mut s = admin.sql_session();
    s.execute(
        "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text)",
        &[],
        None,
    )
    .unwrap();
    for id in 1..=ROWS {
        s.execute(
            &format!("INSERT INTO shipments VALUES ({id}, 42, 'failed')"),
            &[],
            None,
        )
        .unwrap();
    }
    let roles = admin.roles();
    for (name, task_only) in [
        ("incident_controller", false),
        ("oncall_lead", false),
        ("agent_exec", true),
    ] {
        roles
            .create(NewRole {
                name: name.into(),
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        if task_only {
            roles.set_task_only(name, true).unwrap();
        }
    }
    roles
        .grant_table("shipments", "oncall_lead", &[TablePrivilege::Select])
        .unwrap();
    for role in ["incident_controller", "agent_exec"] {
        roles
            .grant_table(
                "shipments",
                role,
                &[TablePrivilege::Select, TablePrivilege::Update],
            )
            .unwrap();
    }
    // Long statement time and per-task concurrency: only the all-agent
    // ceiling may refuse here.
    let issuer = roles.get("incident_controller").unwrap().unwrap();
    admin
        .tasks()
        .create(
            &issuer,
            NewTask {
                name: "inc-436".into(),
                executor: "agent_exec".into(),
                approver: None,
                schema: None,
                table: "shipments".into(),
                visible_rows: PredicateText("merchant_id = 42".into()),
                readable: vec!["id".into(), "status".into()],
                masked: Vec::new(),
                untrusted_text: Vec::new(),
                allowed_write: Some(AllowedWrite {
                    columns: ["status".to_string()].into_iter().collect(),
                    old_row: PredicateText("status = 'failed'".into()),
                    new_row: PredicateText("status = 'queued'".into()),
                }),
                mutation_caps: MutationCaps {
                    distinct_rows: 100,
                    transitions: 100,
                },
                disclosure_caps: DisclosureCaps {
                    rows_read: 10_000,
                    rows_returned: 10_000,
                    bytes_returned: 1 << 20,
                },
                limits: LoadLimits {
                    statement_ms: 120_000,
                    concurrent_statements: 32,
                    ..LoadLimits::default()
                },
                approval_threshold: 100,
                expires: timestamptz(now_ms() + DAY_MS),
            },
            WallClockMs(now_ms()),
        )
        .unwrap();
    admin
}

struct Fixture {
    admin: Admin,
    ca: TestCert,
    agent: TestCert,
    ordinary: TestCert,
    service: Arc<Service>,
    agent_listener: Running,
    app_listener: Running,
}

async fn fixture() -> Fixture {
    let admin = admin_with(Limits::default());
    let ca = certificate(None, false);
    let server = certificate(Some(&ca), true);
    let agent = certificate(Some(&ca), false);
    let ordinary = certificate(Some(&ca), false);
    let service = Service::with_agent(
        admin.clone(),
        vec![Binding {
            fingerprint: ordinary.fingerprint(),
            role: Role::Database {
                name: "oncall_lead".into(),
            },
        }],
        vec![binding(&agent, "agent_exec", &["inc-436"])],
        Clock::new(0, 3600).unwrap(),
        ServiceLimits::default(),
    )
    .unwrap();
    let tls = tls::server(
        vec![server.cert.der().clone()],
        PrivatePkcs8KeyDer::from(server.key.serialize_der()).into(),
        vec![ca.cert.der().clone()],
    )
    .unwrap();
    let (_, start) = watch::channel(true);
    let agent_listener = serve_agent_pg_bound(
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        service.clone(),
        tls.clone(),
        32,
        vec!["giggles".into()],
        start.clone(),
    )
    .await
    .unwrap();
    let app_listener = serve_pg_bound(
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        service.clone(),
        tls,
        32,
        start,
    )
    .await
    .unwrap();
    Fixture {
        admin,
        ca,
        agent,
        ordinary,
        service,
        agent_listener,
        app_listener,
    }
}

impl Fixture {
    async fn agent_wire(&self) -> Wire {
        connect_raw(self.agent_listener.address, &self.ca, &self.agent, AGENT_STARTUP).await
    }

    async fn app_wire(&self) -> Wire {
        connect_raw(
            self.app_listener.address,
            &self.ca,
            &self.ordinary,
            &[("user", "giggles"), ("database", "giggles")],
        )
        .await
    }

    /// An application transaction holding ACCESS EXCLUSIVE on `shipments`.
    fn hold_table_lock(&self) -> SqlSession {
        let mut s = self.admin.sql_session();
        s.execute("BEGIN", &[], None).unwrap();
        s.execute("ALTER TABLE shipments ADD COLUMN extra int", &[], None)
            .unwrap();
        s
    }

    /// (agent slots, blocking permits) held now.
    fn held(&self) -> (usize, usize) {
        (
            self.service.agent_requests_admitted(),
            self.service.blocking_permits_held(),
        )
    }
}

/// Waits until `done` holds; the counters are the oracle, the poll interval is not.
async fn until(what: &str, mut done: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(60), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

/// Sends `sql(i)` on wire `i` in its own task; each answer goes to the channel.
fn send_all(
    wires: Vec<Wire>,
    sql: impl Fn(usize) -> String,
) -> (Vec<JoinHandle<Wire>>, mpsc::UnboundedReceiver<Vec<Msg>>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let handles = wires
        .into_iter()
        .enumerate()
        .map(|(i, mut wire)| {
            let tx = tx.clone();
            let sql = sql(i + 1);
            tokio::spawn(async move {
                let answer = wire.simple(&sql).await;
                tx.send(answer).unwrap();
                wire
            })
        })
        .collect();
    (handles, rx)
}

async fn next(rx: &mut mpsc::UnboundedReceiver<Vec<Msg>>) -> Vec<Msg> {
    tokio::time::timeout(Duration::from_secs(60), rx.recv())
        .await
        .expect("no answer")
        .expect("channel closed")
}

#[track_caller]
fn assert_rows(messages: &[Msg], rows: usize) {
    assert_eq!(error_of(messages), None, "{messages:?}");
    assert_eq!(
        messages.iter().filter(|m| matches!(m, Msg::Row(_))).count(),
        rows,
        "{messages:?}"
    );
    assert_eq!(messages.last(), Some(&Msg::Ready(b'I')), "{messages:?}");
}

/// A protected update that ran: `UPDATE 1`, no error, idle.
#[track_caller]
fn assert_updated(messages: &[Msg]) {
    assert_eq!(error_of(messages), None, "{messages:?}");
    assert!(
        messages.contains(&Msg::Complete("UPDATE 1".into())),
        "{messages:?}"
    );
    assert_eq!(messages.last(), Some(&Msg::Ready(b'I')), "{messages:?}");
}

#[track_caller]
fn assert_all_agents(messages: &[Msg]) {
    assert_eq!(
        error_of(messages).map(|e| e.0).as_deref(),
        Some("54T01"),
        "{messages:?}"
    );
    assert_eq!(messages.last(), Some(&Msg::Ready(b'I')), "{messages:?}");
}

fn agent_config(json: &str) -> Result<AgentConfig, serde_json::Error> {
    serde_json::from_str(json)
}

#[tokio::test]
async fn ceiling_default_is_eight() {
    let config = agent_config(r#"{"postgres":"127.0.0.1:0"}"#).unwrap();
    assert_eq!(config.validate(), Ok(()));
    assert_eq!(config.limits(), AgentLimits::default());
    assert_eq!(
        config.limits(),
        AgentLimits {
            max_concurrent_statements: 8,
            disclosure_chunk_rows: 256,
            disclosure_chunk_bytes: 262_144,
        }
    );
    // A service with no agent settings applied runs with the same ceiling.
    let f = fixture().await;
    assert_eq!(f.service.agent_limits().max_concurrent_statements, 8);
    assert_eq!(f.service.set_agent_limits(config.limits()), Ok(()));
    assert_eq!(f.service.agent_limits(), AgentLimits::default());
    // All three keys are read.
    let config = agent_config(
        r#"{"postgres":"127.0.0.1:0","max_concurrent_statements":3,
            "disclosure_chunk_rows":0,"disclosure_chunk_bytes":4096}"#,
    )
    .unwrap();
    assert_eq!(
        config.limits(),
        AgentLimits {
            max_concurrent_statements: 3,
            disclosure_chunk_rows: 0,
            disclosure_chunk_bytes: 4096,
        }
    );
    assert_eq!(f.service.set_agent_limits(config.limits()), Ok(()));
    assert_eq!(f.service.agent_limits(), config.limits());
}

#[tokio::test]
async fn ceiling_above_half_pool_rejected_at_load() {
    let at = |n: usize| {
        agent_config(&format!(
            r#"{{"postgres":"127.0.0.1:0","max_concurrent_statements":{n}}}"#
        ))
        .unwrap()
    };
    // Half the blocking pool (32) is the largest ceiling; 0 is refused.
    assert_eq!(at(16).validate(), Ok(()));
    assert!(at(17).validate().is_err());
    assert!(at(32).validate().is_err());
    assert!(at(0).validate().is_err());
    // Unknown keys are refused.
    for key in ["max_concurrent_statement", "disclosure_chunk", "ceiling"] {
        assert!(
            agent_config(&format!(r#"{{"postgres":"127.0.0.1:0","{key}":4}}"#)).is_err(),
            "{key}"
        );
    }
    // Half the engine's admitted operations is the other bound: an engine
    // admitting 8 accepts at most 4, and a refused setting keeps the prior one.
    let admin = admin_with(Limits {
        admitted: 8,
        ..Limits::default()
    });
    let service = Service::with_agent(
        admin,
        Vec::new(),
        Vec::new(),
        Clock::new(0, 3600).unwrap(),
        ServiceLimits::default(),
    )
    .unwrap();
    assert_eq!(service.agent_limits().max_concurrent_statements, 4);
    let limits = |n| AgentLimits {
        max_concurrent_statements: n,
        ..AgentLimits::default()
    };
    assert_eq!(service.set_agent_limits(limits(5)), Err(CoreError::Limit));
    assert_eq!(service.set_agent_limits(limits(17)), Err(CoreError::Limit));
    assert_eq!(service.set_agent_limits(limits(0)), Err(CoreError::Limit));
    assert_eq!(service.agent_limits().max_concurrent_statements, 4);
    assert_eq!(service.set_agent_limits(limits(3)), Ok(()));
    assert_eq!(service.agent_limits().max_concurrent_statements, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agent_requests_never_hold_more_than_ceiling_blocking_permits() {
    const CEILING: usize = 4;
    const AGENTS: usize = 10;
    let f = fixture().await;
    f.service
        .set_agent_limits(AgentLimits {
            max_concurrent_statements: CEILING,
            ..AgentLimits::default()
        })
        .unwrap();
    let mut wires = Vec::new();
    for _ in 0..AGENTS {
        wires.push(f.agent_wire().await);
    }
    assert_eq!(f.held(), (0, 0));
    let mut holder = f.hold_table_lock();
    let (handles, mut answers) = send_all(wires, agent_update);
    // While the admitted requests wait on the lock, the rest are refused at
    // once: they neither queue nor take a blocking worker.
    for _ in 0..AGENTS - CEILING {
        assert_all_agents(&next(&mut answers).await);
        let (slots, permits) = f.held();
        assert!(slots <= CEILING, "{slots}");
        assert!(permits <= CEILING, "{permits}");
    }
    until("the admitted agents to hold their workers", || {
        f.held() == (CEILING, CEILING)
    })
    .await;
    assert!(f.admin.task_statement_slots().0 <= CEILING);
    assert!(answers.try_recv().is_err());
    // A further request is refused while the ceiling is held.
    let mut extra = f.agent_wire().await;
    assert_all_agents(&extra.simple(&agent_update(AGENTS + 1)).await);
    assert_eq!(f.held(), (CEILING, CEILING));
    holder.execute("ROLLBACK", &[], None).unwrap();
    for _ in 0..CEILING {
        assert_updated(&next(&mut answers).await);
    }
    for handle in handles {
        handle.await.unwrap();
    }
    until("all slots to return", || f.held() == (0, 0)).await;
    assert_eq!(f.admin.task_statement_slots(), (0, 0));
    // The slots are reusable.
    assert_updated(&extra.simple(&agent_update(AGENTS + 2)).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn application_statements_admitted_while_agents_saturated() {
    const CEILING: usize = 8;
    const APPS: usize = 16;
    let f = fixture().await;
    assert_eq!(f.service.agent_limits().max_concurrent_statements, CEILING);
    let mut agents = Vec::new();
    for _ in 0..CEILING {
        agents.push(f.agent_wire().await);
    }
    let mut extra = f.agent_wire().await;
    let mut apps = Vec::new();
    for _ in 0..APPS {
        apps.push(f.app_wire().await);
    }
    let mut holder = f.hold_table_lock();
    // Saturate the agents: every slot held, each in a worker waiting on the lock.
    let (agent_handles, mut agent_answers) = send_all(agents, agent_update);
    until("the agents to saturate", || f.held() == (CEILING, CEILING)).await;
    assert_all_agents(&extra.simple(&agent_update(CEILING + 1)).await);
    // Sixteen application statements are admitted and in flight together
    // with the saturated agents.
    let (app_handles, mut app_answers) = send_all(apps, |_| APP_SELECT.to_owned());
    until("the application statements to run", || {
        f.held() == (CEILING, CEILING + APPS)
    })
    .await;
    assert!(agent_answers.try_recv().is_err());
    assert!(app_answers.try_recv().is_err());
    holder.execute("ROLLBACK", &[], None).unwrap();
    for _ in 0..APPS {
        let answer = next(&mut app_answers).await;
        assert_ne!(
            error_of(&answer).map(|e| e.0).as_deref(),
            Some("53300"),
            "{answer:?}"
        );
        assert_rows(&answer, ROWS);
    }
    for _ in 0..CEILING {
        assert_updated(&next(&mut agent_answers).await);
    }
    for handle in agent_handles.into_iter().chain(app_handles) {
        handle.await.unwrap();
    }
    until("all slots to return", || f.held() == (0, 0)).await;
    assert_eq!(f.admin.task_statement_slots(), (0, 0));
}

/// R3-502: the exposure report lists each agent credential bound to the
/// task, from the server's bindings, after the fixed listener row; a draft
/// of the same task lists them too, and a task with no binding lists none.
#[test]
fn exposure_lists_bound_credentials() {
    use giggles_core::{
        relational::{SqlResult, Value},
        task::{
            exposure::{LISTENER_DETAIL, draft_exposure, show_exposure},
            statements::{TaskStatement, parse},
        },
    };
    use giggles_server::agent::{AgentBinding, exposure_listener_rows};

    let admin = admin_with(Limits::default());
    let agent = |fp: &str, tasks: &[&str]| AgentBinding {
        fingerprint: fp.repeat(32),
        executor: "agent_exec".into(),
        tasks: tasks.iter().map(|t| t.to_string()).collect(),
    };
    let service = Service::with_agent(
        admin.clone(),
        vec![],
        vec![
            agent("bb", &["inc-436"]),
            agent("aa", &["inc-436", "inc-437"]),
            agent("cc", &["inc-437"]),
        ],
        Clock::new(0, 3600).unwrap(),
        ServiceLimits::default(),
    )
    .unwrap();
    let listener = |result: &SqlResult| -> Vec<(String, Option<String>)> {
        result
            .rows
            .iter()
            .filter(|row| row.values[0] == Value::Text("listener".into()))
            .map(|row| {
                let Value::Text(item) = &row.values[1] else { panic!("{row:?}") };
                let detail = match &row.values[2] {
                    Value::Text(t) => Some(t.clone()),
                    _ => None,
                };
                (item.clone(), detail)
            })
            .collect()
    };
    let issuer = admin.roles().get("incident_controller").unwrap().unwrap();
    let rows = exposure_listener_rows(&service, "inc-436");
    assert_eq!(
        rows,
        vec![
            ("credential".to_string(), "aaaaaaaa as agent_exec".to_string()),
            ("credential".to_string(), "bbbbbbbb as agent_exec".to_string()),
        ]
    );
    let shown = show_exposure(&admin.sql_session(), &issuer, "inc-436", &rows).unwrap();
    assert_eq!(
        listener(&shown),
        vec![
            ("agent".to_string(), Some(LISTENER_DETAIL.to_string())),
            ("credential".to_string(), Some("aaaaaaaa as agent_exec".to_string())),
            ("credential".to_string(), Some("bbbbbbbb as agent_exec".to_string())),
        ]
    );
    // A draft of a task that is bound but not yet created.
    let draft = format!(
        "SHOW TASK EXPOSURE FOR CREATE TASK \"inc-437\" EXECUTOR agent_exec ON shipments \
         VISIBLE ROWS WHERE (merchant_id = 42) READABLE (id, status) \
         DISCLOSE AT MOST 10 ROWS READ, 10 ROWS RETURNED, 4096 BYTES RETURNED EXPIRES '{}'",
        timestamptz(now_ms() + DAY_MS)
    );
    let Some(TaskStatement::ShowExposureDraft(new)) = parse(&draft) else {
        panic!("not a draft: {draft}");
    };
    let rows = exposure_listener_rows(&service, "inc-437");
    let drafted = draft_exposure(
        &admin.sql_session(),
        &issuer,
        &new,
        WallClockMs(now_ms()),
        &rows,
    )
    .unwrap();
    assert_eq!(
        listener(&drafted),
        vec![
            ("agent".to_string(), Some(LISTENER_DETAIL.to_string())),
            ("credential".to_string(), Some("aaaaaaaa as agent_exec".to_string())),
            ("credential".to_string(), Some("cccccccc as agent_exec".to_string())),
        ]
    );
    // No binding, or no valid name: only the fixed row.
    assert!(exposure_listener_rows(&service, "inc-999").is_empty());
    assert!(exposure_listener_rows(&service, "").is_empty());
}
