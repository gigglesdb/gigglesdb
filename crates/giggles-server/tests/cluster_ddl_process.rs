#![cfg(feature = "engine-rocksdb")]
//! DDL across gateways with real server processes (GD-0075 Phase 11, R5-100): a DDL on one
//! node closes the other gateways (their writers drain, nothing writes in the old layout after
//! it), dead or cut-off gateways are evicted and fenced, and every node ends with one schema.
//! LONG: each test starts its own 3-node cluster of server processes (tens of seconds).

use crate::cluster_harness::ClusterProcs;
use std::{
    collections::BTreeSet,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;
use tokio_postgres::{Client, Config, Error, NoTls, SimpleQueryMessage, config::SslMode};

/// One statement must answer within this (a range leader change behind it included).
const STATEMENT_WAIT: Duration = Duration::from_secs(60);
/// How long a node may take to serve again after a fault healed or a node restarted.
const CONVERGE_WAIT: Duration = Duration::from_secs(60);
/// The range engines' lock lease (`EngineSettings::default().lease`, as the server runs).
const LOCK_LEASE: Duration = Duration::from_secs(10);
/// The gateway membership lease (cluster default, `cluster.gateway_lease_ms`).
const GATEWAY_LEASE: Duration = Duration::from_secs(4);
/// Serialization failure and deadlock: the errors a client is expected to retry.
const RETRYABLE: [&str; 2] = ["40001", "40P01"];

fn retryable(e: &Error) -> bool {
    e.code().is_some_and(|c| RETRYABLE.contains(&c.code()))
}

fn describe(e: &Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{} {}", db.code().code(), db.message()),
        None => format!("{e}"),
    }
}

async fn try_batch(c: &Client, sql: &str) -> Result<(), Error> {
    tokio::time::timeout(STATEMENT_WAIT, c.batch_execute(sql))
        .await
        .unwrap_or_else(|_| panic!("{sql}: no answer within {STATEMENT_WAIT:?}"))
}

async fn batch(c: &Client, sql: &str) {
    try_batch(c, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {}", describe(&e)));
}

/// Runs a DDL statement, retrying the retryable errors; returns how many it retried.
async fn ddl(c: &Client, sql: &str) -> Result<usize, String> {
    let started = Instant::now();
    let deadline = started + STATEMENT_WAIT;
    let mut attempts = 0;
    loop {
        attempts += 1;
        match try_batch(c, sql).await {
            Ok(()) => return Ok(attempts - 1),
            Err(e) => {
                if !retryable(&e) || Instant::now() >= deadline {
                    return Err(format!(
                        "{sql}: {} after {attempts} attempts in {:?}",
                        describe(&e),
                        started.elapsed()
                    ));
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

type TextRows = Vec<Vec<Option<String>>>;

/// Column names and rows of a query, as text.
async fn try_text(c: &Client, sql: &str) -> Result<(Vec<String>, TextRows), Error> {
    let messages = tokio::time::timeout(STATEMENT_WAIT, c.simple_query(sql))
        .await
        .unwrap_or_else(|_| panic!("{sql}: no answer within {STATEMENT_WAIT:?}"))?;
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    for message in messages {
        if let SimpleQueryMessage::Row(row) = message {
            columns = row.columns().iter().map(|c| c.name().to_string()).collect();
            rows.push(
                (0..row.len())
                    .map(|i| row.get(i).map(String::from))
                    .collect(),
            );
        }
    }
    Ok((columns, rows))
}

async fn text(c: &Client, sql: &str) -> TextRows {
    try_text(c, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {}", describe(&e)))
        .1
}

async fn count(c: &Client, sql: &str) -> i64 {
    text(c, sql).await[0][0]
        .as_deref()
        .unwrap()
        .parse()
        .unwrap()
}

/// A session to one node that reconnects after the server closed it: a gateway that lost
/// its membership answers with the "range unavailable" class (08007), which ends the session.
struct Session {
    address: SocketAddr,
    client: Option<Client>,
}

impl Session {
    fn new(address: SocketAddr, client: Client) -> Self {
        Self {
            address,
            client: Some(client),
        }
    }

    /// One attempt: (re)connects if needed, then runs `sql`. Err is any failure, as text.
    async fn try_text(&mut self, sql: &str) -> Result<(Vec<String>, TextRows), String> {
        if self.client.as_ref().is_none_or(Client::is_closed) {
            let mut config = Config::new();
            config
                .user("owner")
                .dbname("postgres")
                .ssl_mode(SslMode::Disable)
                .connect_timeout(Duration::from_secs(5))
                .host("127.0.0.1")
                .port(self.address.port());
            let connect = tokio::time::timeout(STATEMENT_WAIT, config.connect(NoTls)).await;
            let (client, connection) = match connect {
                Ok(Ok(pair)) => pair,
                Ok(Err(e)) => return Err(format!("connect: {}", describe(&e))),
                Err(_) => return Err("connect: no answer".into()),
            };
            tokio::spawn(async move {
                let _ = connection.await;
            });
            self.client = Some(client);
        }
        let client = self.client.as_ref().unwrap();
        try_text(client, sql).await.map_err(|e| {
            let closed = if e.is_closed() {
                " (session closed)"
            } else {
                ""
            };
            format!("{}{closed}", describe(&e))
        })
    }

    /// Retries `sql` until it succeeds or `deadline` passes.
    async fn until_ok(&mut self, sql: &str, deadline: Instant) -> (Vec<String>, TextRows) {
        loop {
            match self.try_text(sql).await {
                Ok(answer) => return answer,
                Err(e) => assert!(
                    Instant::now() < deadline,
                    "{}: {sql} still failing: {e}",
                    self.address
                ),
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

const SCHEMA: &str = "SELECT table_name, column_name FROM information_schema.columns \
    WHERE table_schema = 'public' ORDER BY table_name, ordinal_position";

/// Every public table's columns in order, as `table.column`.
async fn schema(c: &Client) -> Vec<String> {
    text(c, SCHEMA)
        .await
        .into_iter()
        .map(|r| format!("{}.{}", r[0].as_deref().unwrap(), r[1].as_deref().unwrap()))
        .collect()
}

fn columns_of(schema: &[String], table: &str) -> Vec<String> {
    schema
        .iter()
        .filter_map(|c| c.strip_prefix(&format!("{table}.")).map(String::from))
        .collect()
}

/// What an INSERT loop saw.
#[derive(Default)]
struct LoopLog {
    /// Start and end of each successful insert, with its id.
    ok: Vec<(Instant, Instant, i64)>,
    /// Ids whose insert failed (it did not commit, or its outcome is unknown).
    failed: Vec<i64>,
    retryable: Vec<String>,
    other: Vec<String>,
}

/// A client inserting into `table (id, node)` until stopped (explicit column list, so it
/// keeps working when columns are added).
struct InsertLoop {
    node: i64,
    log: Arc<Mutex<LoopLog>>,
    stop: Arc<AtomicBool>,
    task: JoinHandle<Client>,
}

impl InsertLoop {
    fn start(client: Client, node: i64, table: &str) -> Self {
        let log = Arc::new(Mutex::new(LoopLog::default()));
        let stop = Arc::new(AtomicBool::new(false));
        let sql = format!("INSERT INTO {table} (id, node) VALUES ($1, $2)");
        let (task_log, task_stop) = (log.clone(), stop.clone());
        let task = tokio::spawn(async move {
            let mut id = node * 1_000_000;
            while !task_stop.load(Ordering::Acquire) {
                id += 1;
                let start = Instant::now();
                let result =
                    tokio::time::timeout(STATEMENT_WAIT, client.execute(&sql, &[&id, &node])).await;
                let mut log = task_log.lock().unwrap();
                match result {
                    Ok(Ok(1)) => log.ok.push((start, Instant::now(), id)),
                    Ok(Ok(n)) => log.other.push(format!("insert {id} affected {n} rows")),
                    Ok(Err(e)) => {
                        log.failed.push(id);
                        if retryable(&e) {
                            log.retryable.push(describe(&e));
                        } else {
                            log.other.push(describe(&e));
                        }
                    }
                    Err(_) => {
                        log.failed.push(id);
                        log.other.push(format!("insert {id}: no answer"));
                    }
                }
            }
            client
        });
        Self {
            node,
            log,
            stop,
            task,
        }
    }

    fn successes_since(&self, after: Instant) -> usize {
        let log = self.log.lock().unwrap();
        log.ok
            .iter()
            .filter(|(start, _, _)| *start >= after)
            .count()
    }

    /// Waits until an insert that started after `after` succeeded.
    async fn wait_resumed(&self, after: Instant, what: &str) {
        let deadline = Instant::now() + CONVERGE_WAIT;
        while self.successes_since(after) == 0 {
            assert!(
                Instant::now() < deadline,
                "node {}: DML did not resume within {CONVERGE_WAIT:?} after {what}: {:?}",
                self.node,
                self.log.lock().unwrap().other
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Longest time without a completed insert while `[start, end]` ran.
    fn stall(&self, start: Instant, end: Instant) -> Duration {
        let log = self.log.lock().unwrap();
        let mut done: Vec<Instant> = log.ok.iter().map(|(_, end, _)| *end).collect();
        done.sort();
        done.windows(2)
            .filter(|w| w[1] >= start && w[0] <= end)
            .map(|w| w[1] - w[0])
            .max()
            .unwrap_or_default()
    }

    async fn stop(self) -> (Client, LoopLog) {
        self.stop.store(true, Ordering::Release);
        let client = self.task.await.unwrap();
        let log = std::mem::take(&mut *self.log.lock().unwrap());
        (client, log)
    }
}

/// Every insert reported done is present; every row present was inserted (or its insert's
/// outcome was unknown to the client).
fn check_ids(rows: &TextRows, logs: &[&LoopLog], keep: &BTreeSet<i64>) {
    let present: BTreeSet<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    for log in logs {
        for (_, _, id) in &log.ok {
            assert!(present.contains(id), "acknowledged insert {id} missing");
        }
    }
    let known: BTreeSet<i64> = logs
        .iter()
        .flat_map(|log| {
            log.ok
                .iter()
                .map(|(_, _, id)| *id)
                .chain(log.failed.iter().copied())
        })
        .chain(keep.iter().copied())
        .collect();
    let unknown: Vec<_> = present.difference(&known).collect();
    assert!(unknown.is_empty(), "rows nobody inserted: {unknown:?}");
}

async fn create_t(c: &Client) {
    batch(
        c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, node BIGINT NOT NULL)",
    )
    .await;
    batch(c, "INSERT INTO t (id, node) VALUES (1, 1), (2, 1), (3, 1)").await;
}

/// A DDL statement on node 1 runs while nodes 2 and 3 insert: their inserts wait (never
/// write in the old layout once the DDL committed), only retryable errors, DML resumes after
/// each DDL and all nodes return the same rows and columns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ddl_on_one_node_while_dml_runs_on_the_others() {
    let started = Instant::now();
    let cluster = ClusterProcs::new(3).await;
    let c1 = cluster.client(0).await;
    create_t(&c1).await;
    let loops = [
        InsertLoop::start(cluster.client(1).await, 2, "t"),
        InsertLoop::start(cluster.client(2).await, 3, "t"),
    ];
    for l in &loops {
        l.wait_resumed(started, "start").await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    for statement in [
        "ALTER TABLE t ADD COLUMN c INT DEFAULT 7",
        "CREATE INDEX t_node ON t (node)",
        "ALTER TABLE t ADD COLUMN d TEXT",
    ] {
        let start = Instant::now();
        let retries = ddl(&c1, statement).await.unwrap();
        let end = Instant::now();
        for l in &loops {
            l.wait_resumed(end, statement).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let stalls: Vec<_> = loops.iter().map(|l| l.stall(start, end)).collect();
        eprintln!(
            "DDL {statement:?}: {:?} ({retries} retries); longest DML stall on nodes 2, 3: {stalls:?}",
            end - start
        );
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let [l2, l3] = loops;
    let (c2, log2) = l2.stop().await;
    let (c3, log3) = l3.stop().await;
    for (node, log) in [(2, &log2), (3, &log3)] {
        assert!(log.other.is_empty(), "node {node}: {:?}", log.other);
        eprintln!(
            "node {node}: {} inserts, retryable errors {:?}",
            log.ok.len(),
            log.retryable
        );
    }

    let query = "SELECT id, node, c, d FROM t ORDER BY id";
    let rows = text(&c1, query).await;
    check_ids(&rows, &[&log2, &log3], &BTreeSet::from([1, 2, 3]));
    let expected_schema: Vec<String> = ["t.id", "t.node", "t.c", "t.d"].map(String::from).into();
    for c in [&c1, &c2, &c3] {
        assert_eq!(count(c, "SELECT count(*) FROM t WHERE c IS NULL").await, 0);
        assert_eq!(count(c, "SELECT count(*) FROM t WHERE c <> 7").await, 0);
        assert_eq!(text(c, query).await, rows);
        assert_eq!(schema(c).await, expected_schema);
        assert_eq!(
            count(c, "SELECT count(*) FROM t WHERE node = 2").await,
            count(&c1, "SELECT count(*) FROM t WHERE node = 2").await
        );
    }
    cluster.assert_proxies_unsaturated();
    eprintln!(
        "ddl_on_one_node_while_dml_runs_on_the_others: {:?}",
        started.elapsed()
    );
}

/// Node 3 holds an open writer, so node 1's ALTER waits to close it; node 3 is killed: node 1
/// evicts it and the ALTER completes within the stated bound; node 3 restarts with the new
/// column and without its uncommitted row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_killed_gateway_does_not_block_ddl() {
    let started = Instant::now();
    let mut cluster = ClusterProcs::new(3).await;
    let c1 = Arc::new(cluster.client(0).await);
    create_t(&c1).await;
    let c2 = cluster.client(1).await;
    let c3 = cluster.client(2).await;
    batch(&c3, "BEGIN").await;
    batch(&c3, "INSERT INTO t (id, node) VALUES (999, 3)").await;

    let alter = {
        let c1 = c1.clone();
        tokio::spawn(async move {
            let result = ddl(&c1, "ALTER TABLE t ADD COLUMN c INT DEFAULT 7").await;
            (result, Instant::now())
        })
    };
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        !alter.is_finished(),
        "the ALTER did not wait for node 3's open writer"
    );
    let killed = Instant::now();
    cluster.kill(2);
    drop(c3);
    // Eviction (about a second of failed close calls, then the gateway lease) and the range
    // lock lease of node 3's row lock, which the ALTER's rewrite may wait for.
    let bound = LOCK_LEASE + GATEWAY_LEASE + Duration::from_secs(10);
    let (result, done) = tokio::time::timeout(bound + STATEMENT_WAIT, alter)
        .await
        .expect("ALTER never finished")
        .unwrap();
    result.unwrap();
    let took = done - killed;
    eprintln!("ALTER completed {took:?} after node 3 was killed (bound {bound:?})");
    assert!(
        took <= bound,
        "ALTER took {took:?} after the kill, above {bound:?}"
    );

    assert_eq!(columns_of(&schema(&c2).await, "t"), ["id", "node", "c"]);
    assert_eq!(count(&c2, "SELECT count(*) FROM t WHERE c = 7").await, 3);

    cluster.restart(2).await;
    let c3 = cluster.client(2).await;
    let query = "SELECT id, node, c FROM t ORDER BY id";
    let rows = text(&c1, query).await;
    assert_eq!(rows.len(), 3, "{rows:?}");
    assert_eq!(text(&c3, query).await, rows, "node 3 after its restart");
    assert_eq!(columns_of(&schema(&c3).await, "t"), ["id", "node", "c"]);
    assert_eq!(count(&c3, "SELECT count(*) FROM t WHERE id = 999").await, 0);
    batch(&c3, "INSERT INTO t (id, node) VALUES (1000, 3)").await;
    assert_eq!(count(&c2, "SELECT c FROM t WHERE id = 1000").await, 7);
    cluster.assert_proxies_unsaturated();
    eprintln!(
        "a_killed_gateway_does_not_block_ddl: {:?}",
        started.elapsed()
    );
}

/// Node 1's ALTER waits at close for node 2's open writer (node 3 is closed already); node 1 is
/// killed: nodes 2 and 3 notice the DDL's gateway is gone, fence, rejoin once its DDL lock
/// lease ends and serve DML again within the stated bound; the schema is all old or all new
/// and every node agrees, node 1 too after its restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killing_the_ddl_gateway_releases_the_others() {
    let started = Instant::now();
    let mut cluster = ClusterProcs::new(3).await;
    let c1 = Arc::new(cluster.client(0).await);
    create_t(&c1).await;
    let c2 = cluster.client(1).await;
    let c3 = Arc::new(cluster.client(2).await);
    // Sessions for after the kill, opened while every gateway serves.
    let mut s2 = Session::new(cluster.pg_addr(1), cluster.client(1).await);
    let mut s3 = Session::new(cluster.pg_addr(2), cluster.client(2).await);
    batch(&c2, "BEGIN").await;
    batch(&c2, "INSERT INTO t (id, node) VALUES (500, 2)").await;

    let alter = {
        let c1 = c1.clone();
        tokio::spawn(async move { ddl(&c1, "ALTER TABLE t ADD COLUMN c INT DEFAULT 7").await })
    };
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        !alter.is_finished(),
        "the ALTER did not wait for node 2's open writer"
    );
    // Node 3 is closed for the ALTER: its writers wait.
    let blocked = {
        let c3 = c3.clone();
        tokio::spawn(async move {
            try_batch(&c3, "INSERT INTO t (id, node) VALUES (600, 3)")
                .await
                .map_err(|e| describe(&e))
        })
    };
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert!(
        !blocked.is_finished(),
        "node 3 wrote while closed for the ALTER"
    );

    let killed = Instant::now();
    cluster.kill(0);
    drop(c1);
    // Watchdog: the DDL's gateway unanswered for the gateway lease -> fence; the dead DDL's
    // range locks (the DDL lock included) expire after the lock lease; then rejoin.
    let bound = LOCK_LEASE + GATEWAY_LEASE + Duration::from_secs(20);
    let deadline = killed + bound + CONVERGE_WAIT;
    // ON CONFLICT: a retry after an attempt with an unknown outcome stays valid.
    s2.until_ok(
        "INSERT INTO t (id, node) VALUES (2001, 2) ON CONFLICT (id) DO NOTHING",
        deadline,
    )
    .await;
    let back2 = killed.elapsed();
    s3.until_ok(
        "INSERT INTO t (id, node) VALUES (3001, 3) ON CONFLICT (id) DO NOTHING",
        deadline,
    )
    .await;
    let back3 = killed.elapsed();
    eprintln!(
        "DML served again after the DDL gateway was killed: node 2 {back2:?}, node 3 {back3:?} (bound {bound:?})"
    );
    assert!(back2 <= bound && back3 <= bound, "above {bound:?}");
    let blocked = tokio::time::timeout(STATEMENT_WAIT, blocked)
        .await
        .expect("node 3's closed writer never answered")
        .unwrap();
    eprintln!("node 3's writer that waited on the closed gateway: {blocked:?}");
    let open_writer = try_batch(&c2, "COMMIT").await.map_err(|e| describe(&e));
    eprintln!("node 2's writer open across the fence, COMMIT: {open_writer:?}");

    let (c2b, c3b) = (s2.client.take().unwrap(), s3.client.take().unwrap());
    let schema2 = schema(&c2b).await;
    assert_eq!(schema(&c3b).await, schema2);
    let t = columns_of(&schema2, "t");
    let new = t == ["id", "node", "c"];
    assert!(new || t == ["id", "node"], "half a schema change: {t:?}");
    eprintln!("schema after the DDL gateway died: {t:?}");
    let query = "SELECT * FROM t ORDER BY id";
    let rows = text(&c2b, query).await;
    assert_eq!(text(&c3b, query).await, rows);
    if new {
        assert_eq!(
            count(&c2b, "SELECT count(*) FROM t WHERE c IS NULL").await,
            0
        );
    }

    cluster.restart(0).await;
    let c1 = cluster.client(0).await;
    assert_eq!(schema(&c1).await, schema2);
    assert_eq!(text(&c1, query).await, text(&c2b, query).await);
    cluster.assert_proxies_unsaturated();
    eprintln!(
        "killing_the_ddl_gateway_releases_the_others: {:?}",
        started.elapsed()
    );
}

/// Nodes 1 and 2 run CREATE TABLE and ALTER TABLE concurrently (their own tables and one
/// shared table) while node 3 inserts: every DDL succeeds or fails retryably, and all three
/// nodes end with the same tables and columns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_ddl_from_two_gateways() {
    const ROUNDS: usize = 4;
    let started = Instant::now();
    let cluster = ClusterProcs::new(3).await;
    let c1 = cluster.client(0).await;
    batch(
        &c1,
        "CREATE TABLE shared (id BIGINT PRIMARY KEY, node BIGINT NOT NULL)",
    )
    .await;
    let dml = InsertLoop::start(cluster.client(2).await, 3, "shared");
    dml.wait_resumed(started, "start").await;

    let run = |client: Client, node: usize| {
        tokio::spawn(async move {
            let mut retries = 0;
            let mut errors = Vec::new();
            for k in 0..ROUNDS {
                for sql in [
                    format!("CREATE TABLE own_{node}_{k} (id BIGINT PRIMARY KEY)"),
                    format!("ALTER TABLE shared ADD COLUMN s_{node}_{k} INT"),
                    format!("ALTER TABLE own_{node}_0 ADD COLUMN x_{k} INT DEFAULT {k}"),
                ] {
                    match ddl(&client, &sql).await {
                        Ok(n) => retries += n,
                        Err(e) => errors.push(e),
                    }
                }
            }
            (client, retries, errors)
        })
    };
    let a = run(c1, 1);
    let b = run(cluster.client(1).await, 2);
    let (c1, retries1, errors1) = a.await.unwrap();
    let (c2, retries2, errors2) = b.await.unwrap();
    let ddl_time = started.elapsed();
    dml.wait_resumed(Instant::now(), "the concurrent DDL").await;
    let (c3, log) = dml.stop().await;
    assert!(
        errors1.is_empty() && errors2.is_empty(),
        "{errors1:?} {errors2:?}"
    );
    assert!(log.other.is_empty(), "node 3 DML: {:?}", log.other);
    eprintln!(
        "concurrent DDL done after {ddl_time:?}; retries: node 1 {retries1}, node 2 {retries2}; \
         node 3 inserts {}, retryable errors {:?}",
        log.ok.len(),
        log.retryable
    );

    let expected = schema(&c1).await;
    assert_eq!(schema(&c2).await, expected);
    assert_eq!(schema(&c3).await, expected);
    let tables: BTreeSet<&str> = expected
        .iter()
        .map(|c| c.split_once('.').unwrap().0)
        .collect();
    assert_eq!(tables.len(), 1 + 2 * ROUNDS, "{tables:?}");
    assert_eq!(columns_of(&expected, "shared").len(), 2 + 2 * ROUNDS);
    for node in [1, 2] {
        assert_eq!(
            columns_of(&expected, &format!("own_{node}_0")).len(),
            1 + ROUNDS
        );
    }
    let rows = text(&c1, "SELECT id FROM shared ORDER BY id").await;
    check_ids(&rows, &[&log], &BTreeSet::new());
    for c in [&c2, &c3] {
        assert_eq!(text(c, "SELECT id FROM shared ORDER BY id").await, rows);
    }
    cluster.assert_proxies_unsaturated();
    eprintln!("concurrent_ddl_from_two_gateways: {:?}", started.elapsed());
}

/// Node 3 is cut off: node 1's ALTER evicts it and completes; node 3 answers only errors while
/// cut off (never rows in the old schema); healed, it rejoins and returns the new schema.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_isolated_node_is_fenced_during_ddl() {
    let started = Instant::now();
    let cluster = ClusterProcs::new(3).await;
    let c1 = cluster.client(0).await;
    create_t(&c1).await;
    let c2 = cluster.client(1).await;
    let c3 = cluster.client(2).await;
    assert_eq!(count(&c3, "SELECT count(*) FROM t").await, 3);

    cluster.isolate(2);
    let start = Instant::now();
    // Failed close calls (2 s each) for about a second, the gateway lease, and range leaders
    // on node 3 failing over behind the ALTER's range calls (a 15 s call timeout each). When
    // node 3 led the range of the gateway liveness locks, every gateway's probe waits out
    // that call timeout, so nodes 1 and 2 fence too and rejoin once a new leader serves: the
    // ALTER is then refused (its session ends) and retried here.
    let bound = Duration::from_secs(15) + LOCK_LEASE + GATEWAY_LEASE + Duration::from_secs(30);
    let mut s1 = Session::new(cluster.pg_addr(0), c1);
    let mut refused = Vec::new();
    loop {
        match s1
            .try_text("ALTER TABLE t ADD COLUMN IF NOT EXISTS c INT DEFAULT 7")
            .await
        {
            Ok(_) => break,
            Err(e) => refused.push(e),
        }
        assert!(
            start.elapsed() <= bound,
            "ALTER not done within {bound:?}: {refused:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let took = start.elapsed();
    eprintln!(
        "ALTER with node 3 cut off: {took:?} (bound {bound:?}); refused attempts {refused:?}"
    );
    let deadline = Instant::now() + CONVERGE_WAIT;
    let mut s2 = Session::new(cluster.pg_addr(1), c2);
    let (_, schema2) = s2.until_ok(SCHEMA, deadline).await;
    assert!(
        schema2.contains(&vec![Some("t".into()), Some("c".into())]),
        "{schema2:?}"
    );

    // The ALTER committed only after node 3's membership lapsed: it serves nothing now (an
    // error, or a refused connection once the server closed the session; never rows).
    let mut s3 = Session::new(cluster.pg_addr(2), c3);
    for sql in [
        "SELECT * FROM t ORDER BY id",
        "INSERT INTO t (id, node) VALUES (700, 3)",
        "SELECT * FROM t ORDER BY id",
    ] {
        match s3.try_text(sql).await {
            Ok((columns, rows)) => panic!("cut-off node 3 answered {sql}: {columns:?} {rows:?}"),
            Err(e) => eprintln!("node 3 while cut off, {sql}: {e}"),
        }
    }

    cluster.heal();
    let healed = Instant::now();
    let query = "SELECT * FROM t ORDER BY id";
    let expected = s1.until_ok(query, healed + CONVERGE_WAIT).await;
    assert_eq!(expected.0, ["id", "node", "c"]);
    // The first answer node 3 gives is already in the new schema.
    let answer = s3.until_ok(query, healed + CONVERGE_WAIT).await;
    assert_eq!(answer, expected, "node 3 after it was healed");
    eprintln!("node 3 served again {:?} after the heal", healed.elapsed());
    let c3 = s3.client.take().unwrap();
    batch(&c3, "INSERT INTO t (id, node) VALUES (701, 3)").await;
    let c1 = s1.client.take().unwrap();
    assert_eq!(count(&c1, "SELECT c FROM t WHERE id = 701").await, 7);
    cluster.assert_proxies_unsaturated();
    eprintln!(
        "an_isolated_node_is_fenced_during_ddl: {:?}",
        started.elapsed()
    );
}
