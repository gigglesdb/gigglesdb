//! Real GigglesDB apply qualification, including actual socket loss at COMMIT.
use async_trait::async_trait;
use skedaddle::{
    buffer::reader::ReaderCheckpoint,
    event::{AssembledTxn, Event, EventKind, OpaqueValue, Position, Row, SourceIdentity, TxnId},
    gigglesdb::apply::{GigglesdbExecutor, install_progress, plan_for_gigglesdb},
    mapping::{PgType, Value},
    pg::apply::{
        ApplyError, Column, CommitDecision, PlanContext, RangeState, SourcePositions, Table,
        TransactionPlan,
        executor::{self, Connector, Destination, Error, INSTALL_INTENT, Observer, Seam},
        plan_transaction,
    },
};
use std::{
    error::Error as StdError,
    fs,
    net::{Shutdown, TcpListener},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::oneshot,
};
use tokio_postgres::{Client, NoTls, types::Type};

type TestResult<T> = Result<T, Box<dyn StdError + Send + Sync>>;
type Socket = Arc<Mutex<Option<std::net::TcpStream>>>;

struct Server {
    child: Child,
    root: PathBuf,
    port: u16,
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Err(error) = fs::remove_dir_all(&self.root) {
            eprintln!("FAILED to remove {}: {error}", self.root.display());
        }
    }
}

struct Connection {
    port: u16,
    socket: Socket,
    proxy: Option<u16>,
    drained: Option<oneshot::Receiver<()>>,
    connections: Arc<AtomicUsize>,
}
impl Connection {
    fn new(port: u16) -> Self {
        Self {
            port,
            socket: Arc::default(),
            proxy: None,
            drained: None,
            connections: Arc::default(),
        }
    }
}
#[async_trait]
impl Connector for Connection {
    async fn connect(&mut self) -> executor::Result<Client> {
        // Wait only for the real server to finish handling the discarded COMMIT
        // reply, so the recovery read cannot race the still-running COMMIT.
        if self.proxy.is_none()
            && let Some(drained) = self.drained.take()
        {
            drained.await.map_err(|_| Error::Database)?;
        }
        let port = self.proxy.take().unwrap_or(self.port);
        let stream = TcpStream::connect(("127.0.0.1", port))
            .await
            .map_err(|_| Error::Database)?;
        let stream = stream.into_std().map_err(|_| Error::Database)?;
        *self.socket.lock().unwrap() = Some(stream.try_clone().map_err(|_| Error::Database)?);
        let stream = TcpStream::from_std(stream).map_err(|_| Error::Database)?;
        let mut config = tokio_postgres::Config::new();
        config.user("postgres").dbname("postgres");
        let (client, driver) = config.connect_raw(stream, NoTls).await?;
        tokio::spawn(async move {
            let _ = driver.await;
        });
        self.connections.fetch_add(1, Ordering::SeqCst);
        Ok(client)
    }
}

async fn fixture() -> TestResult<Option<(Server, Client)>> {
    let Some(binary) = std::env::var_os("GIGGLES_SERVER_BIN") else {
        eprintln!("SKIPPED: set GIGGLES_SERVER_BIN to run live GigglesDB apply qualification");
        return Ok(None);
    };
    let parent = PathBuf::from(
        std::env::var_os("SKEDADDLE_TEST_ROOT").ok_or("SKEDADDLE_TEST_ROOT required")?,
    );
    let root = parent.join(format!("gd65-n3-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root)?;
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    let child = match Command::new(binary)
        .args(["--dev", "--trust-local", "--data"])
        .arg(root.join("data"))
        .args(["--port", &port.to_string()])
        .env("GIGGLES_OWNER_PASSWORD", uuid::Uuid::new_v4().to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            fs::remove_dir_all(&root)?;
            return Err(error.into());
        }
    };
    let mut server = Server { child, root, port };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut connection = Connection::new(port);
    let mut client = loop {
        if let Some(status) = server.child.try_wait()? {
            return Err(format!("giggles-server exited during startup: {status}").into());
        }
        if let Ok(Ok(client)) =
            tokio::time::timeout(Duration::from_secs(1), connection.connect()).await
        {
            break client;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("giggles-server startup timeout".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    install_progress(&mut client, |intent| async move {
        assert_eq!(intent, INSTALL_INTENT);
        Ok(())
    })
    .await?;
    client.batch_execute("CREATE TABLE n3_a (id integer PRIMARY KEY, v integer NOT NULL); CREATE TABLE n3_b (id integer PRIMARY KEY, v integer NOT NULL); INSERT INTO n3_a VALUES (2,20); INSERT INTO n3_b VALUES (3,30)").await?;
    Ok(Some((server, client)))
}

fn row(id: i32, value: Option<i32>) -> Row {
    let opaque = |value| OpaqueValue {
        type_id: "int4".into(),
        bytes: Some(Value::Int4(value).canonical_bytes().unwrap()),
    };
    let mut row = Row::from([("id".into(), opaque(id))]);
    if let Some(value) = value {
        row.insert("v".into(), opaque(value));
    }
    row
}

fn plan(stream: &str, n: u64) -> TransactionPlan {
    transaction_plan(stream, n, true)
}

fn transaction_plan(stream: &str, n: u64, with_rows: bool) -> TransactionPlan {
    let source = SourceIdentity {
        engine: "gigglesdb".into(),
        system_id: "source".into(),
        lineage: "1".into(),
        db_name: "postgres".into(),
        stream: "source".into(),
    };
    let txn = TxnId(n.to_be_bytes().to_vec());
    let position = Position::new("gigglesdb/source", n.to_be_bytes());
    let mut events: Vec<_> = [
        ("n3_a", EventKind::Insert, 1, Some(10)),
        ("n3_a", EventKind::Update, 2, Some(21)),
        ("n3_b", EventKind::Delete, 3, None),
        ("n3_b", EventKind::Insert, 4, Some(40)),
    ]
    .into_iter()
    .enumerate()
    .map(|(ordinal, (table, kind, id, value))| Event {
        source: source.clone(),
        txn: txn.clone(),
        ordinal_in_txn: ordinal as u64,
        position: position.clone(),
        schema_version: 1,
        kind,
        table: Some(table.into()),
        key: row(id, None),
        before: None,
        after: value.map(|v| row(id, Some(v))),
        unchanged_cols: Default::default(),
    })
    .collect();
    if !with_rows {
        events.clear();
    }
    let txn =
        AssembledTxn::new(source, txn, position, Some(1_700_000_000_000_000), events).unwrap();
    let tables = ["n3_a", "n3_b"].map(|name| Table {
        event_table: name.into(),
        schema: "public".into(),
        name: name.into(),
        schema_version: 1,
        columns: ["id", "v"]
            .map(|name| Column {
                name: name.into(),
                type_id: "int4".into(),
                pg_type: PgType::Int4,
                wire_type: Type::INT4,
            })
            .to_vec(),
        key: vec!["id".into()],
    });
    plan_for_gigglesdb(
        plan_transaction(
            &txn,
            &tables,
            &PlanContext {
                positions: SourcePositions::native_pg("gigglesdb/source"),
                stream: stream.into(),
                mseq: n,
                origin: "unused".into(),
                synthetic_commit_time: None,
            },
            |_, _| Ok(RangeState::Completed),
        )
        .unwrap(),
    )
    .unwrap()
}

async fn rows(client: &Client) -> TestResult<(Vec<(i32, i32)>, Vec<(i32, i32)>)> {
    let mut result = Vec::new();
    for table in ["n3_a", "n3_b"] {
        result.push(
            client
                .query(&format!("SELECT id,v FROM {table} ORDER BY id"), &[])
                .await?
                .iter()
                .map(|r| (r.get(0), r.get(1)))
                .collect(),
        );
    }
    Ok((result.remove(0), result.remove(0)))
}

async fn count(client: &Client, table: &str, stream: &str) -> TestResult<i64> {
    Ok(client
        .query_one(
            &format!("SELECT count(*) FROM _skedaddle.{table} WHERE stream = $1"),
            &[&stream],
        )
        .await?
        .get(0))
}

#[tokio::test]
async fn install_twice_is_journaled_and_idempotent() -> TestResult<()> {
    let Some((_server, mut client)) = fixture().await? else {
        return Ok(());
    };
    let mut journaled = false;
    install_progress(&mut client, |intent| {
        journaled = true;
        async move {
            assert_eq!(intent, INSTALL_INTENT);
            Ok(())
        }
    })
    .await?;
    assert!(journaled);
    assert_eq!(count(&client, "apply_progress", "forward").await?, 0);
    assert_eq!(count(&client, "apply_receipts", "forward").await?, 0);
    Ok(())
}

#[tokio::test]
async fn two_table_transaction_redelivery_and_digest_conflict() -> TestResult<()> {
    let Some((server, client)) = fixture().await? else {
        return Ok(());
    };
    let plan = plan("forward", 1);
    let mut executor = GigglesdbExecutor::new(
        Connection::new(server.port),
        "forward".into(),
        10,
        SourcePositions::native_pg("gigglesdb/source"),
    )?;
    assert_eq!(executor.apply(&plan).await?, CommitDecision::Applied);
    let expected = (vec![(1, 10), (2, 21)], vec![(4, 40)]);
    assert_eq!(rows(&client).await?, expected);
    assert_eq!(count(&client, "apply_progress", "forward").await?, 1);
    assert_eq!(count(&client, "apply_receipts", "forward").await?, 1);
    let progress = client.query_one("SELECT last_mseq::text,last_txn,last_position,last_digest FROM _skedaddle.apply_progress WHERE stream = 'forward'", &[]).await?;
    assert_eq!(progress.get::<_, String>(0), "1");
    assert_eq!(progress.get::<_, Vec<u8>>(1), plan.progress.txn.0);
    assert_eq!(
        progress.get::<_, Vec<u8>>(2),
        plan.progress.position.bytes()
    );
    assert_eq!(progress.get::<_, Vec<u8>>(3), plan.progress.digest);
    assert_eq!(executor.apply(&plan).await?, CommitDecision::Applied);
    assert_eq!(rows(&client).await?, expected);
    assert_eq!(count(&client, "apply_receipts", "forward").await?, 1);
    let mut conflict = plan.clone();
    conflict.progress.digest[0] ^= 1;
    assert!(matches!(
        executor.apply(&conflict).await,
        Err(Error::Apply(ApplyError::DigestMismatch))
    ));
    assert!(matches!(executor.apply(&plan).await, Err(Error::Blocked)));
    assert_eq!(rows(&client).await?, expected);
    executor.close().await?;
    Ok(())
}

#[tokio::test]
async fn row_count_fault_rolls_back_both_tables_and_evidence() -> TestResult<()> {
    let Some((server, client)) = fixture().await? else {
        return Ok(());
    };
    client.batch_execute("DELETE FROM n3_b").await?;
    let before = rows(&client).await?;
    let mut executor = GigglesdbExecutor::new(
        Connection::new(server.port),
        "forward".into(),
        10,
        SourcePositions::native_pg("gigglesdb/source"),
    )?;
    assert!(matches!(
        executor.apply(&plan("forward", 1)).await,
        Err(Error::Apply(ApplyError::MissingRow { .. }))
    ));
    assert_eq!(rows(&client).await?, before);
    assert_eq!(count(&client, "apply_progress", "forward").await?, 0);
    assert_eq!(count(&client, "apply_receipts", "forward").await?, 0);
    executor.close().await?;
    Ok(())
}

#[tokio::test]
async fn receipt_bound_and_checkpoint_retention() -> TestResult<()> {
    let Some((server, client)) = fixture().await? else {
        return Ok(());
    };
    let mut executor = GigglesdbExecutor::new(
        Connection::new(server.port),
        "forward".into(),
        1,
        SourcePositions::native_pg("gigglesdb/source"),
    )?;
    let first = plan("forward", 1);
    executor.apply(&first).await?;
    // Empty source transactions still consume a receipt.
    let next = transaction_plan("forward", 2, false);
    assert!(matches!(
        executor.apply(&next).await,
        Err(Error::ReceiptLimit)
    ));
    assert_eq!(count(&client, "apply_receipts", "forward").await?, 1);
    executor.close().await?;
    let mut executor = GigglesdbExecutor::new(
        Connection::new(server.port),
        "forward".into(),
        1,
        SourcePositions::native_pg("gigglesdb/source"),
    )?;
    executor
        .retain_after_checkpoint(&ReaderCheckpoint {
            next_mseq: 1,
            ..Default::default()
        })
        .await?;
    assert_eq!(count(&client, "apply_receipts", "forward").await?, 1);
    executor
        .retain_after_checkpoint(&ReaderCheckpoint {
            next_mseq: 2,
            ..Default::default()
        })
        .await?;
    assert_eq!(count(&client, "apply_receipts", "forward").await?, 0);
    assert_eq!(executor.apply(&next).await?, CommitDecision::Applied);
    // Historical replay without a retained exact receipt must fail closed.
    assert!(matches!(
        executor.decision(&first.progress).await,
        Err(Error::Apply(ApplyError::MissingDigestEvidence))
    ));
    executor.close().await?;
    Ok(())
}

#[tokio::test]
async fn retained_receipt_proves_historical_replay_and_rejects_digest_conflict() -> TestResult<()> {
    let Some((server, client)) = fixture().await? else {
        return Ok(());
    };
    let mut executor = GigglesdbExecutor::new(
        Connection::new(server.port),
        "forward".into(),
        10,
        SourcePositions::native_pg("gigglesdb/source"),
    )?;
    let first = plan("forward", 1);
    executor.apply(&first).await?;
    executor
        .apply(&transaction_plan("forward", 2, false))
        .await?;
    assert_eq!(executor.apply(&first).await?, CommitDecision::Applied);
    assert_eq!(count(&client, "apply_receipts", "forward").await?, 2);
    let mut conflict = first.progress.clone();
    conflict.digest[0] ^= 1;
    assert!(matches!(
        executor.decision(&conflict).await,
        Err(Error::Apply(ApplyError::DigestMismatch))
    ));
    assert_eq!(
        rows(&client).await?,
        (vec![(1, 10), (2, 21)], vec![(4, 40)])
    );
    executor.close().await?;
    Ok(())
}

struct CutBeforeCommit(Socket);
#[async_trait]
impl Observer for CutBeforeCommit {
    async fn observe(&mut self, seam: Seam, _: &Client) -> executor::Result<()> {
        if seam == Seam::BeforeCommit {
            self.0
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .shutdown(Shutdown::Both)
                .map_err(|_| Error::Database)?;
            return Err(Error::Database);
        }
        Ok(())
    }
}

#[tokio::test]
async fn socket_killed_before_commit_is_not_applied() -> TestResult<()> {
    let Some((server, client)) = fixture().await? else {
        return Ok(());
    };
    let connection = Connection::new(server.port);
    let mut observer = CutBeforeCommit(connection.socket.clone());
    let mut executor = GigglesdbExecutor::new(
        connection,
        "forward".into(),
        10,
        SourcePositions::native_pg("gigglesdb/source"),
    )?;
    let plan = plan("forward", 1);
    assert!(matches!(
        executor.apply_observed(&plan, &mut observer).await,
        Err(Error::Database)
    ));
    drop(executor);
    let mut fresh = GigglesdbExecutor::new(
        Connection::new(server.port),
        "forward".into(),
        10,
        SourcePositions::native_pg("gigglesdb/source"),
    )?;
    assert_eq!(
        fresh.decision(&plan.progress).await?,
        CommitDecision::NotApplied
    );
    assert_eq!(rows(&client).await?, (vec![(2, 20)], vec![(3, 30)]));
    assert_eq!(count(&client, "apply_progress", "forward").await?, 0);
    assert_eq!(count(&client, "apply_receipts", "forward").await?, 0);
    fresh.close().await?;
    Ok(())
}

async fn frame(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let tag = stream.read_u8().await?;
    let length = stream.read_u32().await?;
    if !(4..=16_777_216).contains(&length) {
        return Err(std::io::ErrorKind::InvalidData.into());
    }
    let mut packet = vec![tag];
    packet.extend_from_slice(&length.to_be_bytes());
    packet.resize(length as usize + 1, 0);
    stream.read_exact(&mut packet[5..]).await?;
    Ok((tag, packet))
}

async fn relay_ready(
    upstream: &mut TcpStream,
    mut downstream: Option<&mut TcpStream>,
) -> std::io::Result<()> {
    loop {
        let (tag, packet) = frame(upstream).await?;
        if let Some(downstream) = &mut downstream {
            downstream.write_all(&packet).await?;
        }
        if tag == b'Z' {
            return Ok(());
        }
    }
}

// A real TCP proxy forwards actual startup, Parse/Bind/Execute/Sync, and Query
// frames. On COMMIT it writes the entire packet to the server, then closes the
// client socket *before reading any reply*. No response or database is faked.
async fn lost_reply_proxy(
    port: u16,
) -> TestResult<(
    u16,
    oneshot::Receiver<()>,
    tokio::task::JoinHandle<TestResult<()>>,
)> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let proxy_port = listener.local_addr()?.port();
    let (done, drained) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut downstream, _) = listener.accept().await?;
        let mut upstream = TcpStream::connect(("127.0.0.1", port)).await?;
        let length = downstream.read_u32().await?;
        if !(8..=10_000).contains(&length) {
            return Err("invalid startup frame".into());
        }
        let mut startup = length.to_be_bytes().to_vec();
        startup.resize(length as usize, 0);
        downstream.read_exact(&mut startup[4..]).await?;
        upstream.write_all(&startup).await?;
        relay_ready(&mut upstream, Some(&mut downstream)).await?;
        loop {
            let (tag, packet) = frame(&mut downstream).await?;
            upstream.write_all(&packet).await?;
            if tag == b'Q' && &packet[5..] == b"COMMIT\0" {
                downstream.shutdown().await?;
                drop(downstream);
                relay_ready(&mut upstream, None).await?;
                done.send(()).map_err(|_| "recovery receiver dropped")?;
                return Ok(());
            }
            if tag == b'Q' || tag == b'S' {
                relay_ready(&mut upstream, Some(&mut downstream)).await?;
            }
        }
    });
    Ok((proxy_port, drained, task))
}

#[tokio::test]
async fn lost_commit_reply_recovers_and_fresh_executor_confirms_applied() -> TestResult<()> {
    let Some((server, client)) = fixture().await? else {
        return Ok(());
    };
    let (port, drained, proxy) = lost_reply_proxy(server.port).await?;
    let mut connection = Connection::new(server.port);
    connection.proxy = Some(port);
    connection.drained = Some(drained);
    let connections = connection.connections.clone();
    let mut executor = GigglesdbExecutor::new(
        connection,
        "forward".into(),
        10,
        SourcePositions::native_pg("gigglesdb/source"),
    )?;
    let plan = plan("forward", 1);
    let result = tokio::time::timeout(Duration::from_secs(30), executor.apply(&plan)).await;
    if !matches!(result, Ok(Ok(CommitDecision::Applied))) {
        proxy.abort();
    }
    assert_eq!(result??, CommitDecision::Applied);
    proxy.await??;
    assert_eq!(
        connections.load(Ordering::SeqCst),
        2,
        "lost reply must reconnect"
    );
    executor.close().await?;
    let mut fresh = GigglesdbExecutor::new(
        Connection::new(server.port),
        "forward".into(),
        10,
        SourcePositions::native_pg("gigglesdb/source"),
    )?;
    assert_eq!(
        fresh.decision(&plan.progress).await?,
        CommitDecision::Applied
    );
    assert_eq!(fresh.apply(&plan).await?, CommitDecision::Applied);
    assert_eq!(
        rows(&client).await?,
        (vec![(1, 10), (2, 21)], vec![(4, 40)])
    );
    assert_eq!(count(&client, "apply_receipts", "forward").await?, 1);
    fresh.close().await?;
    Ok(())
}
