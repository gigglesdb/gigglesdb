#![cfg(feature = "engine-rocksdb")]
//! GD-0075 R5-77: single-node isolation anomaly matrix on the v2 engine.
//! Each test replays one case from J:/gigglesdb-work/r5/e/oracle/cases at
//! READ COMMITTED, REPEATABLE READ and SERIALIZABLE and asserts what
//! PostgreSQL 18.6 does (J:/gigglesdb-work/r5/e/oracle/matrix.out), except for
//! the divergences commented in place.
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::task::JoinHandle;
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage, config::SslMode};

static SERIAL: AtomicU64 = AtomicU64::new(0);
const LEVELS: [&str; 3] = ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"];
const RC: &str = "READ COMMITTED";
const RR: &str = "REPEATABLE READ";
const SER: &str = "SERIALIZABLE";

struct Fixture {
    service: Arc<Service>,
    pg: Running,
    path: std::path::PathBuf,
}
impl Fixture {
    async fn new(setup: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "isolation-anomalies-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create_engine(path.join("db"), Limits::default(), Engine::V2).unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "owner".into(),
                superuser: true,
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
        let config = PostgresqlConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            tls: TlsMode::Disabled,
            certificate: None,
            private_key: None,
            client_ca: None,
            certificates: BTreeMap::new(),
            databases: vec!["postgres".into()],
            loopback_trust: true,
            connections: 8,
        };
        let listener = Arc::new(config.build().unwrap());
        let pg = serve_postgresql(config.listen, service.clone(), listener)
            .await
            .unwrap();
        let fixture = Self { service, pg, path };
        let client = fixture.client().await;
        client.batch_execute(setup).await.unwrap();
        drop(client);
        fixture
    }
    async fn client(&self) -> Client {
        let (client, connection) = Config::new()
            .host("localhost")
            .hostaddr(self.pg.address.ip())
            .port(self.pg.address.port())
            .user("owner")
            .dbname("postgres")
            .ssl_mode(SslMode::Disable)
            .connect(NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }
    /// Runs the case's final query on a fresh session and stops the server.
    async fn finish(self, query: &str) -> Vec<String> {
        let client = self.client().await;
        let result = ok(&client, query).await;
        drop(client);
        self.pg.shutdown().await.unwrap();
        drop(self.service);
        // Best effort: Windows may still hold engine files briefly after shutdown.
        let _ = std::fs::remove_dir_all(self.path);
        result
    }
}

type Outcome = Result<Vec<String>, String>;

fn code(sql: &str, error: tokio_postgres::Error) -> String {
    match error.code() {
        Some(state) => state.code().to_owned(),
        None => panic!("{sql}: {error:?}"),
    }
}
/// Rows (columns joined with `|`), or the SQLSTATE of the error.
async fn run(client: &Client, sql: &str) -> Outcome {
    let messages = client.simple_query(sql).await.map_err(|e| code(sql, e))?;
    Ok(messages
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).unwrap_or("NULL"))
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
            _ => None,
        })
        .collect())
}
/// Affected-row count of a DML statement, or the SQLSTATE of the error.
async fn dml(client: &Client, sql: &str) -> Result<u64, String> {
    let messages = client.simple_query(sql).await.map_err(|e| code(sql, e))?;
    Ok(messages
        .iter()
        .find_map(|message| match message {
            SimpleQueryMessage::CommandComplete(count) => Some(*count),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{sql}: no command tag")))
}
async fn ok(client: &Client, sql: &str) -> Vec<String> {
    run(client, sql)
        .await
        .unwrap_or_else(|state| panic!("{sql}: SQLSTATE {state}"))
}
fn rows(values: &[&str]) -> Outcome {
    Ok(values.iter().map(|value| (*value).to_owned()).collect())
}
fn sqlstate(state: &str) -> Outcome {
    Err(state.to_owned())
}
async fn begin(client: &Client, level: &str) {
    ok(client, &format!("BEGIN ISOLATION LEVEL {level}")).await;
}

/// Starts a DML statement the oracle shows waiting on a row lock, and checks
/// it is still waiting ~300 ms later.
async fn blocked(client: Client, sql: &'static str) -> JoinHandle<(Client, Result<u64, String>)> {
    let handle = tokio::spawn(async move {
        let result = dml(&client, sql).await;
        (client, result)
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!handle.is_finished(), "{sql} should wait on a lock");
    handle
}

const T_V: &str = "CREATE TABLE t (id int PRIMARY KEY, v int); INSERT INTO t VALUES (1, 0);";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_update() {
    for level in LEVELS {
        let f = Fixture::new(T_V).await;
        let (a, b) = (f.client().await, f.client().await);
        begin(&a, level).await;
        begin(&b, level).await;
        assert_eq!(run(&a, "SELECT v FROM t WHERE id = 1").await, rows(&["0"]), "{level}");
        assert_eq!(run(&b, "SELECT v FROM t WHERE id = 1").await, rows(&["0"]), "{level}");
        assert_eq!(dml(&a, "UPDATE t SET v = v + 1 WHERE id = 1").await, Ok(1), "{level}");
        let waiting = blocked(b, "UPDATE t SET v = v + 1 WHERE id = 1").await;
        assert_eq!(run(&a, "COMMIT").await, rows(&[]), "{level}");
        let (b, result) = waiting.await.unwrap();
        let expected = if level == RC { Ok(1) } else { Err("40001".to_owned()) };
        assert_eq!(result, expected, "{level}");
        assert_eq!(run(&b, "COMMIT").await, rows(&[]), "{level}");
        drop((a, b));
        let fin = if level == RC { "2" } else { "1" };
        assert_eq!(f.finish("SELECT v FROM t").await, vec![fin], "{level}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_skew() {
    for level in LEVELS {
        let f = Fixture::new(
            "CREATE TABLE doctors (id int PRIMARY KEY, oncall boolean); \
             INSERT INTO doctors VALUES (1, true), (2, true);",
        )
        .await;
        let (a, b) = (f.client().await, f.client().await);
        begin(&a, level).await;
        begin(&b, level).await;
        let count = "SELECT count(*) FROM doctors WHERE oncall";
        assert_eq!(run(&a, count).await, rows(&["2"]), "{level}");
        assert_eq!(run(&b, count).await, rows(&["2"]), "{level}");
        let off_1 = "UPDATE doctors SET oncall = false WHERE id = 1";
        let off_2 = "UPDATE doctors SET oncall = false WHERE id = 2";
        assert_eq!(dml(&a, off_1).await, Ok(1), "{level}");
        assert_eq!(dml(&b, off_2).await, Ok(1), "{level}");
        assert_eq!(run(&a, "COMMIT").await, rows(&[]), "{level}");
        let expected = if level == SER { sqlstate("40001") } else { rows(&[]) };
        assert_eq!(run(&b, "COMMIT").await, expected, "{level}");
        drop((a, b));
        let fin = if level == SER { "1" } else { "0" };
        assert_eq!(f.finish(count).await, vec![fin], "{level}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phantom_read() {
    for level in LEVELS {
        let f = Fixture::new("CREATE TABLE t (id int PRIMARY KEY, g int); INSERT INTO t VALUES (1, 1);")
            .await;
        let (a, b) = (f.client().await, f.client().await);
        begin(&a, level).await;
        let count = "SELECT count(*) FROM t WHERE g = 1";
        assert_eq!(run(&a, count).await, rows(&["1"]), "{level}");
        assert_eq!(dml(&b, "INSERT INTO t VALUES (2, 1)").await, Ok(1), "{level}");
        let seen = if level == RC { "2" } else { "1" };
        assert_eq!(run(&a, count).await, rows(&[seen]), "{level}");
        // Divergence from PostgreSQL 18.6 at SERIALIZABLE (optimistic
        // validation): PG commits the read-only A; the engine validates
        // read-only transactions too, and B wrote into A's read range, so A's
        // COMMIT fails with 40001. A wrote nothing, so the final state matches.
        let expected = if level == SER { sqlstate("40001") } else { rows(&[]) };
        assert_eq!(run(&a, "COMMIT").await, expected, "{level}");
        drop((a, b));
        assert_eq!(f.finish("SELECT count(*) FROM t").await, vec!["2"], "{level}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phantom_write_skew() {
    for level in LEVELS {
        let f = Fixture::new(
            "CREATE TABLE t (id int PRIMARY KEY, g int); INSERT INTO t VALUES (1, 1), (2, 2);",
        )
        .await;
        let (a, b) = (f.client().await, f.client().await);
        begin(&a, level).await;
        begin(&b, level).await;
        let g1 = "SELECT count(*) FROM t WHERE g = 1";
        let g2 = "SELECT count(*) FROM t WHERE g = 2";
        assert_eq!(run(&a, g1).await, rows(&["1"]), "{level}");
        assert_eq!(run(&b, g2).await, rows(&["1"]), "{level}");
        assert_eq!(dml(&a, "INSERT INTO t VALUES (3, 2)").await, Ok(1), "{level}");
        assert_eq!(dml(&b, "INSERT INTO t VALUES (4, 1)").await, Ok(1), "{level}");
        assert_eq!(run(&a, "COMMIT").await, rows(&[]), "{level}");
        let expected = if level == SER { sqlstate("40001") } else { rows(&[]) };
        assert_eq!(run(&b, "COMMIT").await, expected, "{level}");
        drop((a, b));
        let fin = if level == SER { "3" } else { "4" };
        assert_eq!(f.finish("SELECT count(*) FROM t").await, vec![fin], "{level}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonrepeatable_read() {
    for level in LEVELS {
        let f = Fixture::new(T_V).await;
        let (a, b) = (f.client().await, f.client().await);
        begin(&a, level).await;
        let read = "SELECT v FROM t WHERE id = 1";
        assert_eq!(run(&a, read).await, rows(&["0"]), "{level}");
        assert_eq!(dml(&b, "UPDATE t SET v = 7 WHERE id = 1").await, Ok(1), "{level}");
        let seen = if level == RC { "7" } else { "0" };
        assert_eq!(run(&a, read).await, rows(&[seen]), "{level}");
        // Divergence at SERIALIZABLE, as in phantom_read: PG commits the
        // read-only A; the engine fails its COMMIT with 40001.
        let expected = if level == SER { sqlstate("40001") } else { rows(&[]) };
        assert_eq!(run(&a, "COMMIT").await, expected, "{level}");
        drop((a, b));
        assert_eq!(f.finish("SELECT v FROM t").await, vec!["7"], "{level}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_at_first_statement() {
    for level in LEVELS {
        let f = Fixture::new(T_V).await;
        let (a, b) = (f.client().await, f.client().await);
        begin(&a, level).await;
        let read = "SELECT v FROM t WHERE id = 1";
        // BEGIN takes no snapshot: A's first statement sees B's commit.
        assert_eq!(dml(&b, "UPDATE t SET v = 9 WHERE id = 1").await, Ok(1), "{level}");
        assert_eq!(run(&a, read).await, rows(&["9"]), "{level}");
        assert_eq!(dml(&b, "UPDATE t SET v = 10 WHERE id = 1").await, Ok(1), "{level}");
        let seen = if level == RC { "10" } else { "9" };
        assert_eq!(run(&a, read).await, rows(&[seen]), "{level}");
        // Divergence at SERIALIZABLE, as in phantom_read: PG commits the
        // read-only A; the engine fails its COMMIT with 40001.
        let expected = if level == SER { sqlstate("40001") } else { rows(&[]) };
        assert_eq!(run(&a, "COMMIT").await, expected, "{level}");
        drop((a, b));
        assert_eq!(f.finish("SELECT v FROM t").await, vec!["10"], "{level}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_only_anomaly() {
    for level in LEVELS {
        let f = Fixture::new(
            "CREATE TABLE acct (id int PRIMARY KEY, bal int); \
             INSERT INTO acct VALUES (1, 0), (2, 0);",
        )
        .await;
        let (a, b, c) = (f.client().await, f.client().await, f.client().await);
        begin(&b, level).await;
        assert_eq!(run(&b, "SELECT sum(bal) FROM acct").await, rows(&["0"]), "{level}");
        begin(&a, level).await;
        let deposit = "UPDATE acct SET bal = bal + 20 WHERE id = 2";
        assert_eq!(dml(&a, deposit).await, Ok(1), "{level}");
        assert_eq!(run(&a, "COMMIT").await, rows(&[]), "{level}");
        ok(&c, &format!("BEGIN ISOLATION LEVEL {level} READ ONLY")).await;
        let all = "SELECT id, bal FROM acct ORDER BY id";
        assert_eq!(run(&c, all).await, rows(&["1|0", "2|20"]), "{level}");
        assert_eq!(run(&c, "COMMIT").await, rows(&[]), "{level}");
        // Divergence from PostgreSQL 18.6 at SERIALIZABLE (optimistic
        // validation): PG fails B's UPDATE with 40001 once C has observed A's
        // commit; we validate B's read of acct at COMMIT, so the UPDATE
        // succeeds and B's COMMIT fails with 40001. The final state matches.
        let withdraw = "UPDATE acct SET bal = bal - 11 WHERE id = 1";
        assert_eq!(dml(&b, withdraw).await, Ok(1), "{level}");
        let expected = if level == SER { sqlstate("40001") } else { rows(&[]) };
        assert_eq!(run(&b, "COMMIT").await, expected, "{level}");
        drop((a, b, c));
        let fin: &[&str] = if level == SER {
            &["1|0", "2|20"]
        } else {
            &["1|-11", "2|20"]
        };
        assert_eq!(f.finish(all).await, fin, "{level}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_conflict() {
    for level in LEVELS {
        let f = Fixture::new(T_V).await;
        let (a, b) = (f.client().await, f.client().await);
        begin(&a, level).await;
        begin(&b, level).await;
        let read = "SELECT v FROM t WHERE id = 1";
        assert_eq!(run(&a, read).await, rows(&["0"]), "{level}");
        assert_eq!(run(&b, read).await, rows(&["0"]), "{level}");
        assert_eq!(dml(&a, "DELETE FROM t WHERE id = 1").await, Ok(1), "{level}");
        let waiting = blocked(b, "UPDATE t SET v = 5 WHERE id = 1").await;
        assert_eq!(run(&a, "COMMIT").await, rows(&[]), "{level}");
        let (b, result) = waiting.await.unwrap();
        if level == RC {
            assert_eq!(result, Ok(0), "{level}");
            assert_eq!(run(&b, read).await, rows(&[]), "{level}");
        } else {
            assert_eq!(result, Err("40001".to_owned()), "{level}");
            assert_eq!(run(&b, read).await, sqlstate("25P02"), "{level}");
        }
        assert_eq!(run(&b, "COMMIT").await, rows(&[]), "{level}");
        drop((a, b));
        assert_eq!(f.finish("SELECT count(*) FROM t").await, vec!["0"], "{level}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_conflict() {
    for level in LEVELS {
        let f = Fixture::new("CREATE TABLE t (id int PRIMARY KEY, v int);").await;
        let (a, b) = (f.client().await, f.client().await);
        begin(&a, level).await;
        begin(&b, level).await;
        assert_eq!(run(&a, "SELECT count(*) FROM t").await, rows(&["0"]), "{level}");
        assert_eq!(run(&b, "SELECT count(*) FROM t").await, rows(&["0"]), "{level}");
        assert_eq!(dml(&a, "INSERT INTO t VALUES (1, 1)").await, Ok(1), "{level}");
        let waiting = blocked(b, "INSERT INTO t VALUES (1, 2)").await;
        assert_eq!(run(&a, "COMMIT").await, rows(&[]), "{level}");
        let (b, result) = waiting.await.unwrap();
        let state = result.expect_err(level);
        match level {
            RC => assert_eq!(state, "23505"),
            // Divergence from PostgreSQL 18.6 at REPEATABLE READ (optimistic
            // validation): PG reports the duplicate key (23505) after the
            // wait; the engine may instead report that the key changed since
            // B's snapshot (40001). Either way B is aborted.
            RR => assert!(matches!(state.as_str(), "23505" | "40001"), "{state}"),
            _ => assert_eq!(state, "40001"),
        }
        assert_eq!(run(&b, "COMMIT").await, rows(&[]), "{level}");
        drop((a, b));
        assert_eq!(f.finish("SELECT v FROM t").await, vec!["1"], "{level}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_skew_through_row_reads() {
    // Rows returned to the client (cursor path), not an aggregate: the reads
    // must still be validated at SERIALIZABLE.
    for level in LEVELS {
        let f = Fixture::new("CREATE TABLE t (id int PRIMARY KEY, v int); INSERT INTO t VALUES (1, 0), (2, 0);")
            .await;
        let (a, b) = (f.client().await, f.client().await);
        begin(&a, level).await;
        begin(&b, level).await;
        assert_eq!(run(&a, "SELECT v FROM t WHERE id = 1").await, rows(&["0"]), "{level}");
        assert_eq!(run(&b, "SELECT v FROM t WHERE id = 2").await, rows(&["0"]), "{level}");
        assert_eq!(dml(&a, "UPDATE t SET v = 1 WHERE id = 2").await, Ok(1), "{level}");
        assert_eq!(dml(&b, "UPDATE t SET v = 1 WHERE id = 1").await, Ok(1), "{level}");
        assert_eq!(run(&a, "COMMIT").await, rows(&[]), "{level}");
        let expected = if level == SER { sqlstate("40001") } else { rows(&[]) };
        assert_eq!(run(&b, "COMMIT").await, expected, "{level}");
        drop((a, b));
        let fin = if level == SER { "1" } else { "2" };
        assert_eq!(f.finish("SELECT sum(v) FROM t").await, vec![fin], "{level}");
    }
}
