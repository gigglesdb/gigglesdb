//! Real dual-engine data-path rollback after simulated activation; no access restoration.
#[path = "../src/mysql/capture/test_support.rs"]
mod startup_support;
use mysql_async::prelude::Queryable;
use skedaddle::pg::fence::Connection as _;
use skedaddle::{
    buffer::{
        backends::ObjectBufferStore,
        reader::ReaderCheckpoint,
        writer::{BufferWriter, WriterConfig},
    },
    coordinator::{
        Materialized, State, Transition,
        journal::Entry,
        rollback::{Record, Step},
    },
    cutover::{Key, Marker},
    engine::Capture as _,
    event::{PositionOrder, Row, SourceIdentity},
    mapping::{Value, mysql::plan_column},
    mysql::{
        MysqlOrder, MysqlPosition,
        capture::MysqlCapture,
        chunk::map_row,
        client::MysqlConfig,
        dest_schema, reverse,
        schema::{TableDescriptor, load_descriptors},
    },
    pg::{
        self,
        apply::{
            self,
            executor::{self, Destination},
        },
        ddl, fence, source,
    },
    rollback::apply as rollback,
};
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const WAIT: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct PgConfig(pg::tls::Config);
impl PgConfig {
    async fn connect(&self) -> Result<tokio_postgres::Client> {
        let (client, driver, _) =
            pg::tls::connect(&self.0, &skedaddle::security::TlsPolicy::Required).await?;
        tokio::spawn(async move {
            driver.await.expect("PG connection driver");
        });
        Ok(client)
    }
}
#[async_trait::async_trait]
impl executor::Connector for PgConfig {
    async fn connect(&mut self) -> executor::Result<tokio_postgres::Client> {
        PgConfig::connect(self)
            .await
            .map_err(|_| executor::Error::Database)
    }
}
struct BoundPg {
    runtime: tokio::runtime::Handle,
    client: tokio_postgres::Client,
}
impl fence::Connection for BoundPg {
    fn query(&mut self, sql: &str, timeout: Duration) -> std::result::Result<fence::Rows, String> {
        self.runtime.block_on(async {
            tokio::time::timeout(timeout, self.client.simple_query(sql))
                .await
                .map_err(|e| e.to_string())?
                .map(|messages| {
                    messages
                        .into_iter()
                        .filter_map(|m| {
                            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                                Some(
                                    (0..r.len())
                                        .map(|i| r.get(i).unwrap_or("").to_owned())
                                        .collect(),
                                )
                            } else {
                                None
                            }
                        })
                        .collect()
                })
                .map_err(|e| e.to_string())
        })
    }
}
// Transport only: frames come unchanged from the real server's pgoutput plugin.
struct Frames(VecDeque<Vec<u8>>);
impl source::Transport for Frames {
    async fn next(&mut self, _: u64) -> source::Result<Vec<u8>> {
        let mut frame = vec![b'w'];
        frame.extend_from_slice(&[0; 24]);
        frame.extend(
            self.0
                .pop_front()
                .expect("real pgoutput exhausted before exact stop"),
        );
        Ok(frame)
    }
}
async fn writer(
    root: &Path,
    name: &str,
    cfg: WriterConfig,
    order: Arc<dyn PositionOrder + Send + Sync>,
) -> Result<(Arc<ObjectBufferStore>, BufferWriter)> {
    let dir = root.join(name);
    std::fs::create_dir_all(dir.join("buffer"))?;
    std::fs::create_dir_all(dir.join("scratch"))?;
    let store = Arc::new(ObjectBufferStore::local(dir.join("buffer"), WAIT)?);
    let writer = BufferWriter::open_with_scratch(
        store.clone(),
        cfg,
        order,
        std::sync::mpsc::channel().0,
        dir.join("scratch"),
    )
    .await?;
    Ok((store, writer))
}
fn forward_table(d: &TableDescriptor) -> apply::Table {
    apply::Table {
        event_table: format!("{}.{}", d.schema, d.table),
        schema: d.schema.clone(),
        name: d.table.clone(),
        schema_version: 1,
        columns: d
            .columns
            .iter()
            .map(|c| {
                let p = plan_column(c).unwrap();
                apply::Column {
                    name: c.name.clone(),
                    type_id: p.type_id,
                    pg_type: p.pg_type,
                    wire_type: tokio_postgres::types::Type::INT8,
                }
            })
            .collect(),
        key: d.primary_key.clone(),
    }
}
async fn mysql_rows(c: &mut mysql_async::Conn, d: &TableDescriptor) -> Result<Vec<Row>> {
    let rows: Vec<mysql_async::Row> = c
        .query(format!(
            "SELECT CAST(id AS BINARY),CAST(value AS BINARY) FROM `{}`.`{}` ORDER BY id",
            d.schema, d.table
        ))
        .await?;
    rows.into_iter()
        .map(|r| Ok(map_row(&d.columns, r)?))
        .collect()
}
async fn pg_rows(c: &tokio_postgres::Client, d: &TableDescriptor) -> Result<Vec<Row>> {
    c.query(
        &format!(
            "SELECT id,value FROM \"{}\".\"{}\" ORDER BY id",
            d.schema, d.table
        ),
        &[],
    )
    .await?
    .into_iter()
    .map(|r| {
        d.columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                Ok((
                    col.name.clone(),
                    skedaddle::event::OpaqueValue {
                        type_id: plan_column(col)?.type_id,
                        bytes: r
                            .get::<_, Option<i64>>(i)
                            .map(|v| Value::Int8(v).canonical_bytes())
                            .transpose()?,
                    },
                ))
            })
            .collect()
    })
    .collect()
}
struct Verify {
    pg: tokio_postgres::Client,
    mysql: mysql_async::Conn,
    tables: Vec<TableDescriptor>,
    calls: usize,
}
impl rollback::Verification for Verify {
    async fn verify(&mut self) -> rollback::Result<()> {
        for d in &self.tables {
            let p = pg_rows(&self.pg, d)
                .await
                .map_err(|_| rollback::Error::Retry)?;
            let m = mysql_rows(&mut self.mysql, d)
                .await
                .map_err(|_| rollback::Error::Retry)?;
            if p != m {
                return Err(rollback::Error::OperatorAction("canonical rowsets differ"));
            }
        }
        self.calls += 1;
        Ok(())
    }
}
// File-backed journal adapter uses the production materializer; no database behavior is replaced.
struct Log {
    state: Materialized,
    saved: (ReaderCheckpoint, bool),
    root: PathBuf,
    saves: usize,
}
impl rollback::Journal for Log {
    fn state(&self) -> Materialized {
        self.state.clone()
    }
    fn publish(&mut self, record: Record) -> rollback::Result<()> {
        let entry = Entry::new(
            &self.state,
            Transition::Rollback {
                record: Box::new(record),
            },
            "owner",
        )
        .map_err(|_| rollback::Error::Retry)?;
        std::fs::write(
            self.root
                .join(format!("entry-{}.json", self.state.applied_seq + 1)),
            serde_json::to_vec(&entry).unwrap(),
        )
        .map_err(|_| rollback::Error::Retry)?;
        self.state
            .apply(&entry)
            .map_err(|_| rollback::Error::OperatorAction("journal rejected"))
    }
    fn progress(&self, _: &str) -> rollback::Result<(ReaderCheckpoint, bool)> {
        Ok(self.saved.clone())
    }
    fn save(&mut self, _: &str, cp: ReaderCheckpoint, reached: bool) -> rollback::Result<()> {
        let data = serde_json::to_vec(&(
            skedaddle::engine::encode_reader_checkpoint(&cp).unwrap(),
            reached,
        ))
        .unwrap();
        let mut file = std::fs::File::create(self.root.join("reader.json"))
            .map_err(|_| rollback::Error::Retry)?;
        std::io::Write::write_all(&mut file, &data).map_err(|_| rollback::Error::Retry)?;
        file.sync_all().map_err(|_| rollback::Error::Retry)?;
        let (marker, reached): (String, bool) =
            serde_json::from_slice(&std::fs::read(self.root.join("reader.json")).unwrap()).unwrap();
        self.saved = (
            skedaddle::engine::decode_reader_checkpoint(&marker).unwrap(),
            reached,
        );
        self.saves += 1;
        Ok(())
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_forward_reverse_exact_stop_receipt_replay_and_echo() -> Result {
    let mut missing = vec![];
    for name in ["SKEDADDLE_MYSQL_CONFIG", "SKEDADDLE_TEST_CONFIG"] {
        if std::env::var_os(name).is_none() {
            missing.push(name);
        }
    }
    if !missing.is_empty() {
        eprintln!(
            "MYSQL SKIPPED: missing {}; use MySQL harness -MysqlWithPg",
            missing.join(", ")
        );
        return Ok(());
    }
    let cfg: MysqlConfig = serde_json::from_slice(&std::fs::read(
        std::env::var_os("SKEDADDLE_MYSQL_CONFIG").unwrap(),
    )?)?;
    let config: serde_json::Value = serde_json::from_slice(&std::fs::read(
        std::env::var_os("SKEDADDLE_TEST_CONFIG").unwrap(),
    )?)?;
    let pgcfg = PgConfig(pg::tls::Config::new(
        config["dsn"]
            .as_str()
            .expect("dual-engine PG dsn")
            .parse()?,
        config["ca_pem"].as_str().expect("PG CA"),
    ));
    let mut pg = pgcfg.connect().await?;
    let version: u32 = pg
        .query_one("SHOW server_version_num", &[])
        .await?
        .get::<_, String>(0)
        .parse()?;
    assert!(version >= 160000);
    let id = format!("m4_{}", std::process::id());
    let mut admin = cfg.connect().await?;
    admin
        .conn
        .query_drop(format!("CREATE DATABASE {id}"))
        .await?;
    let names = vec![(id.clone(), "a".into()), (id.clone(), "b".into())];
    for (_, table) in &names {
        admin
            .conn
            .query_drop(format!(
                "CREATE TABLE {id}.{table}(id BIGINT PRIMARY KEY,value BIGINT) ENGINE=InnoDB"
            ))
            .await?;
    }
    let tables = load_descriptors(&mut admin.conn, &names).await?;
    for sql in dest_schema::build(&tables)?.statements() {
        pg.batch_execute(sql).await?;
    }
    executor::install_opaque_progress(&mut pg, |_| async { Ok(()) }).await?;
    pg.batch_execute(&format!(
        "SELECT pg_replication_origin_create('forward_{id}')"
    ))
    .await?;
    let source = SourceIdentity {
        engine: "mysql".into(),
        system_id: id.clone(),
        lineage: "one".into(),
        db_name: id.clone(),
        stream: id.clone(),
    };
    let status = admin.status().await?;
    let start = if status.gtid_on {
        MysqlPosition::Gtid {
            set: status.executed,
            server_uuid: status.server_uuid.clone(),
            file: status.file,
            offset: status.offset,
        }
    } else {
        MysqlPosition::File {
            server_uuid: status.server_uuid.clone(),
            file_seq: status.file.rsplit('.').next().unwrap().parse()?,
            offset: status.offset,
        }
    };
    let order: Arc<dyn PositionOrder + Send + Sync> = Arc::new(if status.gtid_on {
        MysqlOrder::gtid(&source.system_id)?
    } else {
        MysqlOrder::file(&status.server_uuid)?
    });
    let root = PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").expect("task root")).join(&id);
    std::fs::create_dir_all(&root)?;
    let forward_cfg = WriterConfig {
        prefix: "forward".into(),
        migration_id: id.clone(),
        stream: id.clone(),
        epoch: 1,
        segment_bytes: 1_000_000,
        segment_transactions: 100,
        segment_age: Duration::from_secs(1),
        max_transaction_bytes: 1_000_000,
    };
    let (_, fw) = writer(&root, "forward", forward_cfg, order.clone()).await?;
    let mut capture = MysqlCapture::new(&cfg, source, start, &names, fw, 6495).await?;
    capture = startup_support::install(capture, &cfg, &names, &root.join("startup"))
        .await
        .0;
    let positions = apply::SourcePositions::new(order.clone(), apply::OriginPolicy::Opaque);
    let mut executor = executor::Executor::new(
        pgcfg.clone(),
        id.clone(),
        format!("forward_{id}"),
        100,
        positions.clone(),
    )?;
    let ftables: Vec<_> = tables.iter().map(forward_table).collect();
    for sql in [
        format!("INSERT INTO {id}.a VALUES(1,10),(2,20),(3,NULL)"),
        format!("INSERT INTO {id}.b VALUES(1,100)"),
        format!("UPDATE {id}.a SET value=21 WHERE id=2"),
    ] {
        admin.conn.query_drop(sql).await?;
        let txn = tokio::time::timeout(WAIT, async {
            loop {
                let b = capture.step().await?;
                if let Some(t) = b.transaction {
                    break Ok::<_, skedaddle::coordinator::Error>(t);
                }
            }
        })
        .await??;
        let plan = apply::plan_transaction(
            &txn,
            &ftables,
            &apply::PlanContext {
                positions: positions.clone(),
                stream: id.clone(),
                mseq: 0,
                origin: format!("forward_{id}"),
                synthetic_commit_time: Some(0),
            },
            |_, _| Ok(apply::RangeState::Completed),
        )?;
        assert_eq!(plan.origin_policy, apply::OriginPolicy::Opaque);
        executor.apply(&plan).await?;
    }
    executor.close().await?;
    for d in &tables {
        assert_eq!(
            mysql_rows(&mut admin.conn, d).await?,
            pg_rows(&pg, d).await?
        );
    }
    let reverse = reverse::Config::new(
        &id,
        version,
        tables
            .iter()
            .map(|d| reverse::Table {
                descriptor: d.clone(),
                plans: d.columns.iter().map(|c| plan_column(c).unwrap()).collect(),
            })
            .collect(),
    )?;
    // Only this test owns the workload: NOLOGIN is the simulated application fence.
    pg.batch_execute(&format!("CREATE ROLE {id}_app NOLOGIN"))
        .await?;
    let mut bound = BoundPg {
        runtime: tokio::runtime::Handle::current(),
        client: pgcfg.connect().await?,
    };
    let mut intents = vec![];
    let mut prepared = reverse
        .prepare(&mut bound, &mut pg, &cfg, None, WAIT, |c, intent| {
            let rows = c
                .query(
                    &format!(
                        "SELECT (NOT rolcanlogin)::text FROM pg_roles WHERE rolname='{id}_app'"
                    ),
                    WAIT,
                )
                .map_err(pg::reverse::Error::Journal)?;
            assert_eq!(rows[0][0], "true");
            intents.push(intent.clone());
            std::fs::write(
                root.join("preparation.json"),
                serde_json::to_vec(&intents).unwrap(),
            )
            .map_err(|e| pg::reverse::Error::Journal(e.to_string()))?;
            Ok(())
        })
        .await?;
    let ready = prepared.readiness().clone();
    // Actual WAL activity after slot creation establishes a strict activation boundary.
    pg.batch_execute(&format!("SELECT pg_replication_origin_session_setup('forward_{id}'); BEGIN; UPDATE {id}.a SET value=10 WHERE id=1; SELECT pg_logical_emit_message(true,'activation','simulated'); COMMIT; SELECT pg_replication_origin_session_reset()"))
        .await?;
    let activation = pg::replication::parse_lsn(
        &pg.query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await?
            .get::<_, String>(0),
    )?;
    ready.check_activation(&reverse, activation)?;
    let (store, rw) = writer(&root, "reverse", reverse.writer_config(1), reverse.order()).await?;
    let eligibility = pg::reverse::Eligibility::new(|_| Ok(()));
    let mut rev = reverse.capture(
        &ready,
        activation,
        ready.slot().consistent_point,
        rw,
        ddl::SchemaVersions::new(id.clone(), 0, [])?,
        eligibility.clone(),
    )?;
    for sql in [
        format!("INSERT INTO {id}.a VALUES(4,40)"),
        format!("UPDATE {id}.a SET value=11 WHERE id=1"),
        format!("DELETE FROM {id}.a WHERE id=2"),
        format!(
            "BEGIN; UPDATE {id}.a SET id=30,value=31 WHERE id=3; INSERT INTO {id}.b VALUES(2,200); COMMIT"
        ),
    ] {
        pg.batch_execute(&sql).await?;
    }
    // Fence simulated app, then emit the exact final marker transaction.
    assert!(
        !pg.query_one(
            &format!("SELECT rolcanlogin FROM pg_roles WHERE rolname='{id}_app'"),
            &[]
        )
        .await?
        .get::<_, bool>(0)
    );
    let mut marker = Marker {
        cutover: "rollback_m4".into(),
        epoch: 1,
        lsn: 0,
    };
    let payload = marker.payload();
    let lsn: String = pg
        .query_one(
            "SELECT pg_logical_emit_message(true,$1,$2)::text",
            &[&pg::chunk::MESSAGE_PREFIX, &payload],
        )
        .await?
        .get(0);
    marker.lsn = pg::replication::parse_lsn(&lsn)?;
    rev.stop_at(&marker)?;
    let sql = format!(
        "SELECT data FROM pg_logical_slot_get_binary_changes('{}',NULL,NULL,'proto_version','1','publication_names','{}','messages','true','origin','none')",
        reverse.postgres().slot(),
        reverse.postgres().publication()
    );
    let mut frames = Frames(
        pg.query(&sql, &[])
            .await?
            .into_iter()
            .map(|r| r.get(0))
            .collect(),
    );
    let (_, mut pause) = tokio::sync::watch::channel(false);
    while rev.stopped_at().is_none() {
        rev.step(&mut frames, &mut |_: source::Watermark| Ok(()), &mut pause)
            .await?;
    }
    assert!(eligibility.eligible());
    let stop = rev.stop_boundary()?.expect("durable exact marker");
    let checkpoint = rev.writer().checkpoint()?;
    stop.validate(&checkpoint)?;
    assert_eq!(stop.marker, marker);
    std::fs::write(root.join("stop.json"), serde_json::to_vec(&stop)?)?;
    let key = Key {
        migration: id.clone(),
        cutover: marker.cutover.clone(),
        epoch: 1,
        coordinator: "owner".into(),
    };
    let mut state = Materialized {
        epoch: 1,
        owner: "owner".into(),
        ..Default::default()
    };
    state.lifecycle.state = State::RollbackWindow;
    let mut log = Log {
        state,
        saved: (Default::default(), false),
        root: root.clone(),
        saves: 0,
    };
    use rollback::Journal;
    log.publish(Record::Admit {
        request: marker.cutover.clone(),
        deadline_at_ms: u64::MAX,
    })?;
    for step in [
        Step::PreHook,
        Step::FenceDestination,
        Step::Drain,
        Step::FinalMarker,
    ] {
        log.publish(Record::Intent { step })?;
        if step == Step::FinalMarker {
            log.publish(Record::CaptureStopped {
                boundary: stop.clone(),
            })?;
        }
        log.publish(Record::Checkpoint {
            step,
            final_marker: (step == Step::FinalMarker).then_some(marker.lsn),
        })?;
    }
    let earlier = log.state.clone();
    std::fs::write(
        root.join("earlier-reader.json"),
        skedaddle::engine::encode_reader_checkpoint(&log.saved.0)?,
    )?;
    let mut verify = Verify {
        pg: pgcfg.connect().await?,
        mysql: cfg.connect().await?.conn,
        tables: tables.clone(),
        calls: 0,
    };
    let mut ordinary_writer = cfg.connect().await?;
    let replay = async {
        let apply = prepared.replay(
            &key,
            store.clone(),
            &stop,
            &checkpoint,
            &mut verify,
            &mut log,
        );
        let ordinary = async {
            ordinary_writer
                .conn
                .query_drop(format!("INSERT INTO {id}.b VALUES(9998,9998)"))
                .await?;
            ordinary_writer
                .conn
                .query_drop(format!("DELETE FROM {id}.b WHERE id=9998"))
                .await?;
            Ok::<_, mysql_async::Error>(())
        };
        let (applied, user) = tokio::join!(apply, ordinary);
        assert_eq!(applied?, rollback::Outcome::Applied);
        user?;
        let receipts: u64 = admin
            .conn
            .query_first("SELECT COUNT(*) FROM _skedaddle.apply_receipts")
            .await?
            .unwrap();
        assert_eq!(
            log.saves, 5,
            "four PG DML transactions plus final marker; forward-origin rows must be excluded"
        );
        assert_eq!(receipts, 5);
        let before: Vec<_> = tables.iter().map(|d| d.table.clone()).collect();
        assert_eq!(before, vec!["a", "b"]);
        log.state = earlier;
        log.saved = (
            skedaddle::engine::decode_reader_checkpoint(&std::fs::read_to_string(
                root.join("earlier-reader.json"),
            )?)?,
            false,
        );
        assert_eq!(
            prepared
                .replay(
                    &key,
                    store.clone(),
                    &stop,
                    &checkpoint,
                    &mut verify,
                    &mut log
                )
                .await?,
            rollback::Outcome::Applied
        );
        assert_eq!(
            admin
                .conn
                .query_first::<u64, _>("SELECT COUNT(*) FROM _skedaddle.apply_receipts")
                .await?,
            Some(receipts)
        );
        assert_eq!(
            prepared
                .replay(
                    &key,
                    store.clone(),
                    &stop,
                    &checkpoint,
                    &mut verify,
                    &mut log
                )
                .await?,
            rollback::Outcome::Verified
        );
        assert_eq!(verify.calls, 1);
        assert_eq!(log.state.rollback.as_ref().unwrap().next, Step::Sequences);
        // A later ordinary write proves the running collector passed every reverse receipt.
        admin
            .conn
            .query_drop(format!("INSERT INTO {id}.b VALUES(9999,9999)"))
            .await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let echoes = async {
        let mut ordinary = vec![];
        loop {
            let batch = capture.step().await?;
            assert!(!batch.stop);
            if let Some(t) = batch.transaction {
                assert_eq!(t.events.len(), 1, "reverse transaction escaped echo filter");
                let e = &t.events[0];
                let n = Value::from_canonical_bytes(e.key["id"].bytes.as_ref().unwrap())?;
                assert!(
                    matches!(n, Value::Int8(9998 | 9999)),
                    "reverse echo leaked: {n:?}"
                );
                ordinary.push((n.clone(), e.kind.clone()));
                if n == Value::Int8(9999) {
                    break;
                }
            }
        }
        assert_eq!(
            ordinary,
            vec![
                (Value::Int8(9998), skedaddle::event::EventKind::Insert),
                (Value::Int8(9998), skedaddle::event::EventKind::Delete),
                (Value::Int8(9999), skedaddle::event::EventKind::Insert)
            ]
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    tokio::time::timeout(WAIT, async { tokio::try_join!(replay, echoes) }).await??;
    assert_eq!(log.saves, 10);
    drop(capture);
    drop(rev);
    drop(verify);
    drop(prepared);
    admin
        .conn
        .query_drop(format!("DELETE FROM {id}.b WHERE id=9999"))
        .await?;
    for d in &tables {
        assert_eq!(
            mysql_rows(&mut admin.conn, d).await?,
            pg_rows(&pg, d).await?
        );
    }
    admin.conn.query_drop(format!("DROP DATABASE {id}")).await?;
    admin.conn.disconnect().await?;
    pg.batch_execute(&format!("{} SELECT pg_drop_replication_slot('{}'); DROP PUBLICATION {}; DROP SCHEMA {id} CASCADE; DROP ROLE {id}_app; SELECT pg_replication_origin_drop('forward_{id}');",ddl::TriggerSql::new(&id,version)?.remove,reverse.postgres().slot(),reverse.postgres().publication())).await?;
    Ok(())
}
