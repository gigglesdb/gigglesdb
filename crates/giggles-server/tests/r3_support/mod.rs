//! The first-milestone fixture (GD-0061 R3-901, design-7 D9): an in-process
//! v2 server with a PostgreSQL password listener and an agent listener, the
//! worked example's roles and data, task `inc-436` with the spec section 3
//! values, and a background application workload.
//!
//! The table is `shipments` with the D9 columns (`deliveries` is a reserved
//! name in GigglesDB). Merchant 42 has 31 failed rows inside the incident
//! window, merchant 43 has 40, and merchants 100-199 have 2,000 rows that the
//! `delivery_app` workload reads and writes for the whole test.
//!
//! Clocks: `advance_clock` pins the database wall clock
//! (`Admin::set_task_wall_clock`), which task expiry in the protected commit,
//! receipts, previews and retention read. The server's per-call revalidation
//! reads the host clock, which a test cannot move.
#![allow(dead_code)]

#[path = "../agent_support/mod.rs"]
pub mod agent_support;

use agent_support::{TestCert, binding, certificate, now_ms, scratch, timestamptz};
use giggles_core::{
    Admin, Engine, Limits,
    relational::Value,
    roles::{NewRole, TablePrivilege},
    task::{
        definition::WallClockMs,
        statements::{TaskStatement, parse},
    },
};
use giggles_server::{
    Running, Service,
    admission::ServiceLimits,
    agent::serve_agent_pg_bound,
    auth::{PostgresqlConfig, TlsMode, scram::Verifier, serve_postgresql},
    clock::Clock,
    tls,
};
use rustls::{ServerConfig, pki_types::PrivatePkcs8KeyDer};
use std::{
    collections::BTreeMap,
    future::Future,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};
use tokio_postgres::{
    Client, NoTls, Statement,
    config::SslMode,
    error::SqlState,
    types::{ToSql, Type},
};
use tokio_postgres_rustls::MakeRustlsConnect;

pub const TASK: &str = "inc-436";
pub const TABLE: &str = "shipments";
pub const CONTROLLER: &str = "incident_controller";
pub const EXECUTOR: &str = "repair_agent";
pub const APPROVER: &str = "oncall_lead";
pub const APP: &str = "delivery_app";
pub const CONTROLLER_PW: &str = "controller-pw";
pub const EXECUTOR_PW: &str = "agent-pw";
pub const APPROVER_PW: &str = "lead-pw";
pub const APP_PW: &str = "app-pw";
/// The spec's incident window (section 3, allowed write old row).
pub const INCIDENT_START: &str = "2026-11-05 17:00:00+00";
pub const INCIDENT_END: &str = "2026-11-05 17:07:00+00";
/// Unix milliseconds of `INCIDENT_START`.
pub const INCIDENT_START_MS: i64 = 1_793_898_000_000;
pub const MERCHANT_42_ROWS: usize = 31;
pub const MERCHANT_43_ROWS: usize = 40;
pub const APP_MERCHANTS: std::ops::RangeInclusive<i64> = 100..=199;
pub const APP_ROWS_PER_MERCHANT: i64 = 20;
/// Ten seconds of this many connections is the smoke workload.
pub const DEFAULT_CLIENTS: usize = 4;
/// D9: at most 50 attempts per agent step.
pub const MAX_ATTEMPTS: u32 = 50;

const SCHEMA: &str = "CREATE TABLE shipments (merchant_id bigint, id bigint, status text, \
     failed_at timestamptz, attempt_count int, customer_email text, card_token text, \
     PRIMARY KEY (merchant_id, id))";

/// Spec section 3's values as a CREATE TASK statement; `expires` is a
/// timestamptz literal.
pub fn inc_436_sql(expires: &str) -> String {
    format!(
        "CREATE TASK \"{TASK}\" \
           EXECUTOR {EXECUTOR} APPROVER {APPROVER} \
           ON {TABLE} \
           VISIBLE ROWS WHERE (merchant_id = 42) \
           READABLE (id, status, failed_at, attempt_count) \
           MASKED (customer_email) \
           ALLOW UPDATE (status) \
             OLD ROW WHERE (status = 'failed' AND failed_at >= '{INCIDENT_START}' \
                            AND failed_at < '{INCIDENT_END}') \
             NEW ROW WHERE (status = 'queued') \
             AT MOST 25 DISTINCT ROWS AND 25 TRANSITIONS \
             APPROVE ABOVE 5 \
           DISCLOSE AT MOST 1000 ROWS READ, 2097152 BYTES RETURNED \
           WITH (statement_ms = 2000, rows_scanned = 100000, concurrent_statements = 2) \
           EXPIRES '{expires}'"
    )
}

/// The next 17:30Z at least an hour after `now` (spec section 3: "17:30Z").
pub fn next_1730z(now: i64) -> i64 {
    const DAY: i64 = 86_400_000;
    let at = now.div_euclid(DAY) * DAY + 17 * 3_600_000 + 30 * 60_000;
    if at < now + 3_600_000 { at + DAY } else { at }
}

fn failed_at(i: usize) -> String {
    // Every ten seconds from the window start: 31 rows end at 17:05:00.
    timestamptz(INCIDENT_START_MS + i as i64 * 10_000)
}

/// The D9 rows, as multi-row INSERT statements of at most 500 rows.
fn seed_rows() -> Vec<String> {
    let mut rows = Vec::new();
    for i in 0..MERCHANT_42_ROWS {
        let id = i as i64 + 1;
        rows.push(format!(
            "(42, {id}, 'failed', '{}', {}, 'customer{id}@example.com', 'tok_42_{id}')",
            failed_at(i),
            1 + i % 3
        ));
    }
    for i in 0..MERCHANT_43_ROWS {
        let id = i as i64 + 1;
        rows.push(format!(
            "(43, {id}, 'failed', '{}', 1, 'buyer{id}@example.com', 'tok_43_{id}')",
            failed_at(i % MERCHANT_42_ROWS)
        ));
    }
    const STATUSES: [&str; 4] = ["delivered", "pending", "failed", "queued"];
    for merchant in APP_MERCHANTS {
        for id in 1..=APP_ROWS_PER_MERCHANT {
            let status = STATUSES[(merchant + id) as usize % STATUSES.len()];
            let failed = if status == "failed" {
                format!("'{}'", failed_at(id as usize))
            } else {
                "NULL".into()
            };
            rows.push(format!(
                "({merchant}, {id}, '{status}', {failed}, 0, 'm{merchant}c{id}@example.com', \
                 'tok_{merchant}_{id}')"
            ));
        }
    }
    rows.chunks(500)
        .map(|chunk| format!("INSERT INTO {TABLE} VALUES {}", chunk.join(", ")))
        .collect()
}

/// What runs between a start and a restart.
struct Live {
    admin: Admin,
    service: Arc<Service>,
    pg: Running,
    agent: Running,
}

/// The milestone server: a v2 database, a PostgreSQL password listener and an
/// agent listener on one service.
pub struct R3Fixture {
    path: PathBuf,
    live: Option<Live>,
    pub ca: TestCert,
    pub server_cert: TestCert,
    pub agent_cert: TestCert,
    tls: Arc<ServerConfig>,
    /// The PostgreSQL listener's address; restart moves it, and the workload
    /// reconnects to the new one.
    pg_address: Arc<RwLock<SocketAddr>>,
    agent_address: SocketAddr,
    /// The pinned database wall clock, once `advance_clock` ran.
    clock_ms: Option<u64>,
    /// Attempts per agent step, by label (D9 report).
    retries: Mutex<BTreeMap<String, u32>>,
    /// `inc-436`'s expiry, Unix ms.
    pub expires_ms: i64,
}

impl R3Fixture {
    /// The database, roles, schema and rows, then both listeners.
    pub async fn start() -> Self {
        let path = scratch("r3-milestone").join("db");
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut session = admin.sql_session();
        session.execute(SCHEMA, &[], None).unwrap();
        for insert in seed_rows() {
            session.execute(&insert, &[], None).unwrap();
        }
        let roles = admin.roles();
        for (name, task_only, password) in [
            (CONTROLLER, false, CONTROLLER_PW),
            (APPROVER, false, APPROVER_PW),
            (APP, false, APP_PW),
            (EXECUTOR, true, EXECUTOR_PW),
        ] {
            roles
                .create(NewRole {
                    name: name.into(),
                    login: true,
                    password: Some(Verifier::generate(password)),
                    ..NewRole::default()
                })
                .unwrap();
            if task_only {
                roles.set_task_only(name, true).unwrap();
            }
        }
        use TablePrivilege::{Insert, Select, Update};
        for (name, privileges) in [
            (CONTROLLER, &[Select, Update][..]),
            // Spec section 14: SELECT and UPDATE (status); grants are per table.
            (EXECUTOR, &[Select, Update][..]),
            // The undo role writes the restored values.
            (APPROVER, &[Select, Update][..]),
            (APP, &[Select, Insert, Update][..]),
        ] {
            roles.grant_table(TABLE, name, privileges).unwrap();
        }
        let ca = certificate(None, false);
        let server_cert = certificate(Some(&ca), true);
        let agent_cert = certificate(Some(&ca), false);
        let tls = tls::server(
            vec![server_cert.cert.der().clone()],
            PrivatePkcs8KeyDer::from(server_cert.key.serialize_der()).into(),
            vec![ca.cert.der().clone()],
        )
        .unwrap();
        let mut fixture = Self {
            path,
            live: None,
            ca,
            server_cert,
            agent_cert,
            tls,
            pg_address: Arc::new(RwLock::new(SocketAddr::from(([127, 0, 0, 1], 0)))),
            agent_address: SocketAddr::from(([127, 0, 0, 1], 0)),
            clock_ms: None,
            retries: Mutex::new(BTreeMap::new()),
            expires_ms: next_1730z(now_ms()),
        };
        fixture.serve(admin).await;
        fixture
    }

    async fn serve(&mut self, admin: Admin) {
        let service = Service::with_agent(
            admin.clone(),
            Vec::new(),
            vec![binding(&self.agent_cert, EXECUTOR, &[TASK])],
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
            connections: 32,
        };
        let listener = config.build().unwrap();
        let pg = serve_postgresql(config.listen, service.clone(), Arc::new(listener))
            .await
            .unwrap();
        let (_open, start) = watch::channel(true);
        let agent = serve_agent_pg_bound(
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
            service.clone(),
            self.tls.clone(),
            16,
            vec!["giggles".into()],
            start,
        )
        .await
        .unwrap();
        *self.pg_address.write().unwrap() = pg.address;
        self.agent_address = agent.address;
        self.live = Some(Live {
            admin,
            service,
            pg,
            agent,
        });
    }

    fn live(&self) -> &Live {
        self.live.as_ref().expect("the server is running")
    }

    pub fn admin(&self) -> &Admin {
        &self.live().admin
    }

    pub fn service(&self) -> &Arc<Service> {
        &self.live().service
    }

    pub fn pg_address(&self) -> SocketAddr {
        *self.pg_address.read().unwrap()
    }

    pub fn agent_address(&self) -> SocketAddr {
        self.agent_address
    }

    /// `CREATE TASK "inc-436"` with the spec section 3 values, issued by
    /// `incident_controller`.
    pub fn create_inc_436(&self) {
        let sql = inc_436_sql(&timestamptz(self.expires_ms));
        let Some(TaskStatement::Create(new)) = parse(&sql) else {
            panic!("CREATE TASK did not parse: {sql}");
        };
        let issuer = self.admin().roles().get(CONTROLLER).unwrap().unwrap();
        let now = self.clock_ms.map_or_else(now_ms, |ms| ms as i64);
        self.admin()
            .tasks()
            .create(&issuer, *new, WallClockMs(now))
            .unwrap();
    }

    /// The executor `repair_agent` on the agent listener (its certificate is
    /// bound to `inc-436` alone).
    pub async fn agent(&self) -> Client {
        let (client, connection) = tokio_postgres::Config::new()
            .host("localhost")
            .hostaddr(self.agent_address.ip())
            .port(self.agent_address.port())
            .user(EXECUTOR)
            .dbname("giggles")
            .ssl_mode(SslMode::Require)
            .connect(MakeRustlsConnect::new(self.agent_cert.client(&self.ca)))
            .await
            .expect("agent connects");
        tokio::spawn(connection);
        client
    }

    /// `user` with `password` on the PostgreSQL listener.
    pub async fn try_password(
        &self,
        user: &str,
        password: &str,
    ) -> Result<Client, tokio_postgres::Error> {
        connect_password(self.pg_address(), user, password).await
    }

    /// `oncall_lead` on the PostgreSQL listener.
    pub async fn approver(&self) -> Client {
        self.try_password(APPROVER, APPROVER_PW)
            .await
            .expect("approver login")
    }

    /// `incident_controller` on the PostgreSQL listener.
    pub async fn controller(&self) -> Client {
        self.try_password(CONTROLLER, CONTROLLER_PW)
            .await
            .expect("controller login")
    }

    /// `delivery_app` on the PostgreSQL listener.
    pub async fn app(&self) -> Client {
        self.try_password(APP, APP_PW).await.expect("app login")
    }

    /// Runs `f` until it does not fail with 40001, at most 50 times. The
    /// attempt count is recorded under `op` (see `retries`).
    pub async fn retry_40001<T, F, Fut>(
        &self,
        op: &str,
        mut f: F,
    ) -> Result<T, tokio_postgres::Error>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, tokio_postgres::Error>>,
    {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let result = f().await;
            let retry =
                matches!(&result, Err(e) if e.code() == Some(&SqlState::T_R_SERIALIZATION_FAILURE));
            if !retry || attempt >= MAX_ATTEMPTS {
                *self
                    .retries
                    .lock()
                    .unwrap()
                    .entry(op.to_owned())
                    .or_default() += attempt;
                return result;
            }
            tokio::time::sleep(Duration::from_millis(5 * u64::from(attempt.min(20)))).await;
        }
    }

    /// Attempts per labelled agent step so far.
    pub fn retries(&self) -> BTreeMap<String, u32> {
        self.retries.lock().unwrap().clone()
    }

    /// A service stop and reopen (D9 step 18): both listeners stop, every
    /// handle on the database is dropped, the database reopens from disk and
    /// new listeners start. Connections open before are closed; the workload
    /// reconnects to the new PostgreSQL address. A pinned clock is pinned
    /// again.
    pub async fn restart(&mut self) {
        let Live {
            admin,
            service,
            pg,
            agent,
        } = self.live.take().expect("the server is running");
        pg.shutdown().await.unwrap();
        agent.shutdown().await.unwrap();
        drop(service);
        drop(admin);
        // Detached blocking work can still hold a Session (and its Admin)
        // for a moment after the socket tasks stop.
        let deadline = Instant::now() + Duration::from_secs(10);
        let admin = loop {
            match Admin::open(&self.path, Limits::default()) {
                Err(giggles_core::CoreError::Storage(message))
                    if message.contains("already open") && Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                result => break result.expect("reopen the database"),
            }
        };
        if let Some(ms) = self.clock_ms {
            admin.set_task_wall_clock(Some(ms)).unwrap();
        }
        self.serve(admin).await;
    }

    /// Moves the database wall clock `ms` forward from the host clock (first
    /// call) or from its pinned value, and keeps it pinned there.
    pub fn advance_clock(&mut self, ms: u64) {
        let now = self.clock_ms.unwrap_or(now_ms() as u64) + ms;
        self.clock_ms = Some(now);
        self.admin().set_task_wall_clock(Some(now)).unwrap();
    }

    /// Back to the host clock.
    pub fn release_clock(&mut self) {
        self.clock_ms = None;
        self.admin().set_task_wall_clock(None).unwrap();
    }

    /// The database wall clock as the fixture set it.
    pub fn wall_ms(&self) -> i64 {
        self.clock_ms.map_or_else(now_ms, |ms| ms as i64)
    }

    /// Runs `sql` as the administrator and returns its rows.
    pub fn sql(&self, sql: &str) -> Vec<Vec<Value>> {
        self.admin()
            .sql_session()
            .execute(sql, &[], None)
            .unwrap()
            .rows
            .into_iter()
            .map(|row| row.values)
            .collect()
    }

    /// `count(*)` of `shipments` rows matching `filter`.
    pub fn count(&self, filter: &str) -> i64 {
        match &self.sql(&format!("SELECT count(*) FROM {TABLE} WHERE {filter}"))[0][0] {
            Value::Int(n) => *n,
            other => panic!("{other:?}"),
        }
    }

    /// Starts `clients` `delivery_app` connections (D9 workload).
    pub fn workload(&self, clients: usize) -> Workload {
        Workload::start(self.pg_address.clone(), clients)
    }
}

async fn connect_password(
    address: SocketAddr,
    user: &str,
    password: &str,
) -> Result<Client, tokio_postgres::Error> {
    let (client, connection) = tokio_postgres::Config::new()
        .host("localhost")
        .hostaddr(address.ip())
        .port(address.port())
        .user(user)
        .password(password)
        .dbname("postgres")
        .ssl_mode(SslMode::Disable)
        .connect_timeout(Duration::from_secs(5))
        .connect(NoTls)
        .await?;
    tokio::spawn(connection);
    Ok(client)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Op {
    Insert,
    Update,
    Select,
}

/// One client's tallies.
#[derive(Debug, Default)]
struct Tally {
    samples: Vec<(Op, Duration)>,
    serialization: u64,
    deadlocks: u64,
    disconnects: u64,
    connect_failures: u64,
    errors: BTreeMap<String, u64>,
    error_samples: Vec<String>,
}

/// What the workload saw.
#[derive(Debug, Default)]
pub struct WorkloadReport {
    pub clients: usize,
    pub elapsed: Duration,
    /// Completed operations by kind.
    pub ops: BTreeMap<Op, u64>,
    /// Latencies of completed operations, sorted, by kind.
    pub latencies: BTreeMap<Op, Vec<Duration>>,
    /// 40001 and 40P01: expected under contention, not errors.
    pub serialization_failures: u64,
    pub deadlocks: u64,
    /// Connections closed by the server (a restart), and failed reconnects.
    pub disconnects: u64,
    pub connect_failures: u64,
    /// Every other failure, by SQLSTATE ("none" without one).
    pub errors: BTreeMap<String, u64>,
    /// The first few unexpected errors, for the failure message.
    pub error_samples: Vec<String>,
}

impl WorkloadReport {
    pub fn total_ops(&self) -> u64 {
        self.ops.values().sum()
    }
    /// Failures other than 40001 and 40P01.
    pub fn unexpected_errors(&self) -> u64 {
        self.errors.values().sum()
    }
    /// The `q` quantile (0.0..=1.0) of `op`'s latency.
    pub fn quantile(&self, op: Op, q: f64) -> Option<Duration> {
        let samples = self.latencies.get(&op)?;
        if samples.is_empty() {
            return None;
        }
        let at = ((samples.len() - 1) as f64 * q).round() as usize;
        Some(samples[at])
    }
}

/// The D9 application: `clients` `delivery_app` connections running INSERT,
/// UPDATE and SELECT on merchants 100-199 until `stop`.
pub struct Workload {
    stop: Arc<AtomicBool>,
    clients: Vec<JoinHandle<Tally>>,
    started: Instant,
}

impl Workload {
    pub fn start(address: Arc<RwLock<SocketAddr>>, clients: usize) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let handles = (0..clients)
            .map(|k| tokio::spawn(client_loop(k as u64, address.clone(), stop.clone())))
            .collect();
        Self {
            stop,
            clients: handles,
            started: Instant::now(),
        }
    }

    /// Stops every client after its current operation and returns the report.
    pub async fn stop(self) -> WorkloadReport {
        self.stop.store(true, Ordering::SeqCst);
        let mut report = WorkloadReport {
            clients: self.clients.len(),
            ..WorkloadReport::default()
        };
        for handle in self.clients {
            let tally = tokio::time::timeout(Duration::from_secs(30), handle)
                .await
                .expect("a workload client did not stop")
                .expect("a workload client panicked");
            for (op, latency) in tally.samples {
                *report.ops.entry(op).or_default() += 1;
                report.latencies.entry(op).or_default().push(latency);
            }
            report.serialization_failures += tally.serialization;
            report.deadlocks += tally.deadlocks;
            report.disconnects += tally.disconnects;
            report.connect_failures += tally.connect_failures;
            for (code, n) in tally.errors {
                *report.errors.entry(code).or_default() += n;
            }
            for sample in tally.error_samples {
                if report.error_samples.len() < 10 {
                    report.error_samples.push(sample);
                }
            }
        }
        for samples in report.latencies.values_mut() {
            samples.sort();
        }
        report.elapsed = self.started.elapsed();
        report
    }
}

struct Prepared {
    insert: Statement,
    update: Statement,
    select: Statement,
}

async fn prepare(client: &Client) -> Result<Prepared, tokio_postgres::Error> {
    Ok(Prepared {
        insert: client
            .prepare_typed(
                "INSERT INTO shipments (merchant_id, id, status, failed_at, attempt_count, \
                 customer_email, card_token) VALUES ($1, $2, 'pending', NULL, 0, $3, $4)",
                &[Type::INT8, Type::INT8, Type::TEXT, Type::TEXT],
            )
            .await?,
        update: client
            .prepare_typed(
                "UPDATE shipments SET status = $3, attempt_count = attempt_count + 1 \
                 WHERE merchant_id = $1 AND id = $2",
                &[Type::INT8, Type::INT8, Type::TEXT],
            )
            .await?,
        select: client
            .prepare_typed(
                "SELECT id, status, attempt_count FROM shipments WHERE merchant_id = $1 \
                 ORDER BY id",
                &[Type::INT8],
            )
            .await?,
    })
}

/// xorshift64*: deterministic per client, no extra dependency.
fn next(state: &mut u64) -> u64 {
    *state ^= *state >> 12;
    *state ^= *state << 25;
    *state ^= *state >> 27;
    state.wrapping_mul(0x2545_f491_4f6c_dd1d)
}

impl Tally {
    fn failed(&mut self, op: Option<Op>, error: &tokio_postgres::Error, closed: bool) {
        match error.code() {
            Some(code) if *code == SqlState::T_R_SERIALIZATION_FAILURE => self.serialization += 1,
            Some(code) if *code == SqlState::T_R_DEADLOCK_DETECTED => self.deadlocks += 1,
            None if closed => self.disconnects += 1,
            code => {
                let code = code.map_or("none".to_owned(), |c| c.code().to_owned());
                *self.errors.entry(code.clone()).or_default() += 1;
                if self.error_samples.len() < 10 {
                    self.error_samples.push(format!("{op:?} {code}: {error}"));
                }
            }
        }
    }
}

async fn client_loop(k: u64, address: Arc<RwLock<SocketAddr>>, stop: Arc<AtomicBool>) -> Tally {
    const STATUSES: [&str; 3] = ["delivered", "pending", "failed"];
    let mut tally = Tally::default();
    let mut rng = 0x9e37_79b9_7f4a_7c15u64 ^ (k + 1).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    let mut inserted = 0i64;
    let mut connection: Option<(Client, Prepared)> = None;
    while !stop.load(Ordering::SeqCst) {
        if connection.as_ref().is_none_or(|(c, _)| c.is_closed()) {
            let at = *address.read().unwrap();
            let client = match connect_password(at, APP, APP_PW).await {
                Ok(client) => client,
                Err(_) => {
                    tally.connect_failures += 1;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            match prepare(&client).await {
                Ok(prepared) => connection = Some((client, prepared)),
                Err(e) => {
                    tally.failed(None, &e, client.is_closed());
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            }
        }
        let (client, prepared) = connection.as_ref().unwrap();
        let merchant = *APP_MERCHANTS.start() + (next(&mut rng) % 100) as i64;
        let op = match next(&mut rng) % 3 {
            0 => Op::Insert,
            1 => Op::Update,
            _ => Op::Select,
        };
        let started = Instant::now();
        let result = match op {
            Op::Insert => {
                inserted += 1;
                let id = 1_000_000 + k as i64 * 10_000_000 + inserted;
                let email = format!("new{k}_{inserted}@example.com");
                let token = format!("tok_new_{k}_{inserted}");
                let params: [&(dyn ToSql + Sync); 4] = [&merchant, &id, &email, &token];
                client.execute(&prepared.insert, &params).await.map(|_| ())
            }
            Op::Update => {
                let id = 1 + (next(&mut rng) % APP_ROWS_PER_MERCHANT as u64) as i64;
                let status = STATUSES[(next(&mut rng) % 3) as usize];
                let params: [&(dyn ToSql + Sync); 3] = [&merchant, &id, &status];
                client.execute(&prepared.update, &params).await.map(|_| ())
            }
            Op::Select => client
                .query(&prepared.select, &[&merchant])
                .await
                .map(|rows| assert!(rows.len() >= APP_ROWS_PER_MERCHANT as usize)),
        };
        match result {
            Ok(()) => tally.samples.push((op, started.elapsed())),
            Err(e) => {
                let closed = client.is_closed();
                tally.failed(Some(op), &e, closed);
            }
        }
    }
    tally
}

/// The first SQLSTATE of a failed statement.
pub fn sqlstate(error: &tokio_postgres::Error) -> Option<String> {
    error.code().map(|c| c.code().to_owned())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixture_starts_and_workload_runs_10s_without_errors() {
    let mut f = R3Fixture::start().await;
    f.create_inc_436();
    assert_eq!(f.count("merchant_id = 42 AND status = 'failed'"), 31);
    assert_eq!(f.count("merchant_id = 43 AND status = 'failed'"), 40);
    assert_eq!(f.count("merchant_id BETWEEN 100 AND 199"), 2_000);
    let expires = timestamptz(f.expires_ms);
    assert!(expires.contains(" 17:30:00.000+00"), "{expires}");

    // Both listeners answer: the agent sees merchant 42's 31 failed rows,
    // the approver logs in with its password.
    let agent = f.agent().await;
    let rows = f
        .retry_40001("smoke-select", || {
            agent.simple_query("SELECT * FROM shipments WHERE status = 'failed'")
        })
        .await
        .expect("agent select");
    let returned = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(returned, 31);
    assert_eq!(f.retries().get("smoke-select").copied(), Some(1));
    let approver = f.approver().await;
    let n: i64 = approver
        .query_one("SELECT count(*) FROM shipments WHERE merchant_id = 42", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 31);
    // The executor's password never opens the PostgreSQL listener.
    assert!(f.try_password(EXECUTOR, EXECUTOR_PW).await.is_err());

    let workload = f.workload(DEFAULT_CLIENTS);
    tokio::time::sleep(Duration::from_secs(10)).await;
    let report = workload.stop().await;
    assert_eq!(
        report.unexpected_errors(),
        0,
        "{:?} {:?}",
        report.errors,
        report.error_samples
    );
    assert_eq!(
        report.disconnects + report.connect_failures,
        0,
        "{report:?}"
    );
    for op in [Op::Insert, Op::Update, Op::Select] {
        assert!(
            report.ops.get(&op).copied().unwrap_or(0) > 0,
            "{op:?}: {:?}",
            report.ops
        );
        let p50 = report.quantile(op, 0.5).unwrap();
        let p99 = report.quantile(op, 0.99).unwrap();
        assert!(p50 <= p99, "{op:?}");
    }
    let inserts = report.ops[&Op::Insert] as i64;
    assert_eq!(f.count("merchant_id BETWEEN 100 AND 199"), 2_000 + inserts);
    // The task's rows are untouched by the application.
    assert_eq!(f.count("merchant_id = 42 AND status = 'failed'"), 31);

    // Restart: rows and the task survive, both listeners serve again.
    drop(agent);
    drop(approver);
    f.advance_clock(60_000);
    let pinned = f.wall_ms();
    f.restart().await;
    assert_eq!(f.wall_ms(), pinned);
    assert_eq!(f.count("merchant_id BETWEEN 100 AND 199"), 2_000 + inserts);
    let agent = f.agent().await;
    let rows = agent
        .simple_query("SELECT id FROM shipments WHERE status = 'failed'")
        .await
        .expect("agent select after restart");
    assert_eq!(
        rows.iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count(),
        31
    );
    let app = f.app().await;
    let n: i64 = app
        .query_one(
            "SELECT count(*) FROM shipments WHERE merchant_id = 100",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(n >= APP_ROWS_PER_MERCHANT);
    eprintln!(
        "r3 workload: {} ops in {:?} ({:?}), 40001 {}, 40P01 {}, p99 {:?}/{:?}/{:?}",
        report.total_ops(),
        report.elapsed,
        report.ops,
        report.serialization_failures,
        report.deadlocks,
        report.quantile(Op::Insert, 0.99),
        report.quantile(Op::Update, 0.99),
        report.quantile(Op::Select, 0.99),
    );
}
