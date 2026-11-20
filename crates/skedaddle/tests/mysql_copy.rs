//! D15: real engine/capture/chunk/journal/buffer; replay follows PG apply row semantics.
#[path = "support/root.rs"]
mod test_root;

#[path = "../src/mysql/capture/test_support.rs"]
mod startup_support;
use mysql_async::{Params, prelude::Queryable};
use skedaddle::{
    budget::{Limits, RateLimit, ReadLimits, SystemClock, ThroughputBudget},
    buffer::{
        backends::ObjectBufferStore,
        format,
        reader::{BufferReader, ReaderCheckpoint, ReaderConfig},
        writer::{BufferWriter, WriterConfig},
    },
    coordinator::{
        self, Event, Materialized, State, Transition,
        chunk::ChunkPlan,
        epoch::Coordinator,
        journal::{self, Journal},
        local::Local,
    },
    engine::{self, Applier, ChunkDatabase, CopyConfig, Engine, PgTransactionPlanner},
    event::{EventKind, Position, PositionOrder, Row, SourceIdentity},
    mapping::mysql::plan_column,
    mysql::{
        MysqlOrder, MysqlPosition,
        capture::MysqlCapture,
        chunk::{MysqlChunks, map_row, sample_plan},
        chunk_key::{MysqlKeyColumn, MysqlKeyDirection, MysqlTableKey},
        client::{MysqlClient, MysqlConfig},
        control::install_watermark_table,
        planner::{MysqlPlanner, MysqlPlannerTable},
        schema::{TableDescriptor, load_descriptors},
    },
    pg::{
        apply::{self, RangeState},
        catalog::RangeStatus,
        chunk::{ChunkRange, KeyIdentity, Watermark},
    },
};
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};
use tokio::{
    sync::{Notify, watch},
    time::{sleep, timeout},
};
use tokio_postgres::types::Type;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

// Applying is deliberately deferred to the authenticated on-disk replay below.
// This adapter performs no source/destination database behavior.
struct DeferredApply;
impl Applier for DeferredApply {
    async fn step(&mut self, _: &engine::Runtime) -> coordinator::Result<bool> {
        Ok(false)
    }
}
#[derive(Default)]
struct Schedule {
    low: Notify,
    before: Notify,
    snapshot: Notify,
    after: Notify,
    crash: Notify,
}
// Scheduling around the real adapter; no database behavior is replaced.
struct ScheduledChunks {
    inner: MysqlChunks,
    schedule: Arc<Schedule>,
    crash: bool,
}
impl ChunkDatabase for ScheduledChunks {
    async fn emit(&mut self, marker: Watermark) -> coordinator::Result<()> {
        ChunkDatabase::emit(&mut self.inner, marker).await
    }
    async fn read(
        &mut self,
        plan: &ChunkPlan,
        chunk: &ChunkRange,
    ) -> coordinator::Result<Vec<Row>> {
        if chunk.id.ordinal != 1 {
            return ChunkDatabase::read(&mut self.inner, plan, chunk).await;
        }
        self.schedule.low.notify_one();
        self.schedule.before.notified().await;
        let rows = ChunkDatabase::read(&mut self.inner, plan, chunk).await?;
        self.schedule.snapshot.notify_one();
        self.schedule.after.notified().await;
        if self.crash {
            self.schedule.crash.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(rows)
    }
}
fn key(t: &TableDescriptor) -> MysqlTableKey {
    MysqlTableKey {
        event_table: format!("{}.{}", t.schema, t.table),
        schema: t.schema.clone(),
        table: t.table.clone(),
        columns: t
            .primary_key
            .iter()
            .map(|n| {
                let c = t.columns.iter().find(|c| &c.name == n).unwrap();
                MysqlKeyColumn {
                    name: n.clone(),
                    data_type: c.data_type.clone(),
                    column_type: c.column_type.clone(),
                    character_set: c.character_set.clone(),
                    collation: c.collation.clone(),
                    type_id: plan_column(c).unwrap().type_id,
                    direction: MysqlKeyDirection::Asc,
                }
            })
            .collect(),
    }
}
fn destination(t: &TableDescriptor) -> apply::Table {
    apply::Table {
        event_table: format!("{}.{}", t.schema, t.table),
        schema: "public".into(),
        name: t.table.clone(),
        schema_version: 1,
        columns: t
            .columns
            .iter()
            .map(|c| {
                let p = plan_column(c).unwrap();
                let wire_type = match &p.pg_type {
                    skedaddle::mapping::PgType::Int4 => Type::INT4,
                    skedaddle::mapping::PgType::Varchar { .. } => Type::VARCHAR,
                    _ => panic!("test column type"),
                };
                apply::Column {
                    name: c.name.clone(),
                    type_id: p.type_id,
                    pg_type: p.pg_type,
                    wire_type,
                }
            })
            .collect(),
        key: t.primary_key.clone(),
    }
}
async fn status(client: &mut MysqlClient) -> Result<MysqlPosition> {
    let s = client.status().await?;
    Ok(if s.gtid_on {
        MysqlPosition::Gtid {
            set: s.executed,
            server_uuid: s.server_uuid,
            file: s.file,
            offset: s.offset,
        }
    } else {
        MysqlPosition::File {
            server_uuid: s.server_uuid,
            file_seq: s.file.rsplit('.').next().unwrap().parse()?,
            offset: s.offset,
        }
    })
}
async fn writer(
    root: &Path,
    store: Arc<ObjectBufferStore>,
    order: Arc<MysqlOrder>,
    epoch: u64,
) -> Result<BufferWriter> {
    Ok(BufferWriter::open_with_scratch(
        store,
        WriterConfig {
            prefix: "copy".into(),
            migration_id: "d15".into(),
            stream: "rows".into(),
            epoch,
            segment_bytes: 1_000_000,
            segment_transactions: 100,
            segment_age: Duration::from_secs(1),
            max_transaction_bytes: 1_000_000,
        },
        order,
        std::sync::mpsc::channel().0,
        root.join("scratch"),
    )
    .await?)
}
fn budget() -> ThroughputBudget {
    let rate = RateLimit {
        per_second: 1_000_000,
        burst: 1_000_000,
    };
    let reads = ReadLimits {
        rows: rate,
        bytes: rate,
    };
    ThroughputBudget::new(
        Limits {
            chunk_reads: reads,
            verification_reads: reads,
            applier_transactions: rate,
            connections: Default::default(),
            in_memory_segments: 2,
        },
        Arc::new(SystemClock::default()),
    )
    .unwrap()
}
fn copy(source: &SourceIdentity, order: &MysqlOrder) -> CopyConfig {
    CopyConfig {
        source: source.clone(),
        order_domain: order.domain().into(),
        schema_version: 1,
        read_rows: 1000,
        read_bytes: 1_000_000,
        window_bytes: 1_000_000,
    }
}
async fn workload(cfg: &MysqlConfig, name: &str, s: &Schedule) -> Result {
    let mut c = cfg.connect().await?;
    s.low.notified().await;
    for sql in [
        "INSERT INTO TABLE VALUES(55,'before-insert')",
        "UPDATE TABLE SET body='before-update' WHERE id=60",
        "DELETE FROM TABLE WHERE id=70",
        "UPDATE TABLE SET id=65 WHERE id=20",
    ] {
        c.conn.query_drop(sql.replace("TABLE", name)).await?;
    }
    s.before.notify_one();
    s.snapshot.notified().await;
    for sql in [
        "UPDATE TABLE SET id=150 WHERE id=50",
        "UPDATE TABLE SET id=75 WHERE id=100",
        "DELETE FROM TABLE WHERE id=80",
        "INSERT INTO TABLE VALUES(85,'after-insert')",
        "UPDATE TABLE SET body='after-update' WHERE id=60",
        "UPDATE TABLE SET id=25 WHERE id=65",
    ] {
        c.conn.query_drop(sql.replace("TABLE", name)).await?;
    }
    s.after.notify_one();
    c.conn.disconnect().await?;
    Ok(())
}
async fn finish(
    engine: &Engine,
    capture: &mut MysqlCapture,
    chunks: &mut impl ChunkDatabase,
    cfg: &MysqlConfig,
    name: &str,
    source: &SourceIdentity,
    order: &MysqlOrder,
) -> Result {
    let (pause, paused) = watch::channel(false);
    let runtime = engine.runtime();
    let drive = async {
        while runtime.state().await.lifecycle.state != State::CatchingUp {
            sleep(Duration::from_millis(10)).await;
        }
        let mut c = cfg.connect().await?;
        // These are strictly after every H: update/delete must require existing rows.
        for sql in [
            "UPDATE TABLE SET body='completed-update' WHERE id=10",
            "DELETE FROM TABLE WHERE id=110",
            "INSERT INTO TABLE VALUES(200,'completed-insert')",
        ] {
            c.conn.query_drop(sql.replace("TABLE", name)).await?;
        }
        let target = Position::new(order.domain(), status(&mut c).await?.encode()?);
        loop {
            if let Some((_, payload)) = runtime.state().await.progress.get("buffered") {
                let p = engine::decode_buffered_position(payload, source, order.domain())?;
                if p.compare(&target, order)? != std::cmp::Ordering::Less {
                    break;
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
        pause.send_replace(true);
        c.conn.disconnect().await?;
        Result::Ok(())
    };
    let mut apply = DeferredApply;
    let mut b = budget();
    let (run, drive) = tokio::join!(
        engine.run(
            capture,
            chunks,
            &mut apply,
            copy(source, order),
            &mut b,
            paused
        ),
        drive
    );
    drive?;
    run?;
    Ok(())
}
async fn replay(
    reader: &mut BufferReader,
    planner: &mut MysqlPlanner,
    key: &MysqlTableKey,
    state: &Materialized,
) -> Result<Vec<Row>> {
    let mut rows: BTreeMap<Vec<u8>, Row> = BTreeMap::new();
    let mut chunks = 0;
    let mut copying = 0;
    let mut completed = 0;
    let mut moves = 0;
    while let Some(txn) = reader.next_txn().await? {
        let plan = planner
            .plan(&txn, reader.checkpoint().next_mseq, state)
            .await?;
        assert_eq!(plan.origin_policy, apply::OriginPolicy::Opaque);
        assert!(
            !plan
                .statements
                .iter()
                .any(|s| s.sql.contains("replication_origin_xact_setup"))
        );
        for e in &txn.events {
            if matches!(e.kind, EventKind::Watermark | EventKind::FinalMarker) {
                continue;
            }
            let old = key.key(&e.key)?.0;
            let status = if e.kind == EventKind::ChunkRows {
                chunks += 1;
                RangeState::Copying
            } else {
                planner
                    .classify_keys(
                        &key.event_table,
                        std::slice::from_ref(&e.key),
                        &txn.commit_position,
                        state,
                    )
                    .await?[0]
                    .apply_state()
            };
            if e.kind != EventKind::ChunkRows {
                match status {
                    RangeState::Copying => copying += 1,
                    RangeState::Completed => completed += 1,
                }
            }
            // Same operations/row-count/fallback rules as pg::apply::plan_transaction;
            // full MySQL images mean there are no omitted/TOAST columns to synthesize.
            match e.kind {
                EventKind::Insert | EventKind::ChunkRows => {
                    let row = e.after.clone().ok_or("insert image")?;
                    let id = key.key(&row)?.0;
                    let previous = rows.insert(id, row);
                    if status == RangeState::Completed {
                        assert!(previous.is_none(), "completed insert duplicate");
                    }
                }
                EventKind::Update => {
                    let previous = rows.remove(&old);
                    let updated_existing = previous.is_some();
                    if status == RangeState::Completed {
                        assert!(previous.is_some(), "completed update missing");
                    }
                    let mut row = previous.unwrap_or_else(|| e.key.clone());
                    row.extend(e.after.clone().ok_or("update image")?);
                    let id = key.key(&row)?.0;
                    if id != old {
                        moves += 1;
                    }
                    let collision = rows.insert(id, row);
                    if updated_existing {
                        assert!(collision.is_none(), "update collided with destination key");
                    }
                }
                EventKind::Delete => {
                    let previous = rows.remove(&old);
                    if status == RangeState::Completed {
                        assert!(previous.is_some(), "completed delete missing");
                    }
                }
                _ => return Err("unexpected replay event".into()),
            }
        }
    }
    assert!(
        chunks > 0 && copying > 0 && completed > 0 && moves >= 4,
        "D15 paths chunks={chunks} copying={copying} completed={completed} moves={moves}"
    );
    Ok(rows.into_values().collect())
}

async fn scenario(cfg: &MysqlConfig, crash: bool) -> Result {
    eprintln!("D15 scenario crash={crash}");
    let mut admin = cfg.connect().await?;
    let name = if crash {
        "type_zoo.copy_crash"
    } else {
        "type_zoo.copy_live"
    };
    admin
        .conn
        .query_drop(format!(
            "CREATE TABLE {name}(id INT PRIMARY KEY,body VARCHAR(80) NOT NULL) ENGINE=InnoDB"
        ))
        .await?;
    for n in (10..=120).step_by(10) {
        admin
            .conn
            .exec_drop(
                format!("INSERT INTO {name} VALUES(?,?)"),
                (n, format!("initial-{n}")),
            )
            .await?;
    }
    let selected = vec![("type_zoo".into(), name.split('.').nth(1).unwrap().into())];
    let tables = load_descriptors(&mut admin.conn, &selected).await?;
    let k = key(&tables[0]);
    let plan = sample_plan(&mut admin.conn, &k, 4).await?;
    assert_eq!(plan.ranges.len(), 3);
    let s = admin.status().await?;
    let source = SourceIdentity {
        engine: "mysql".into(),
        system_id: "d15-cluster".into(),
        lineage: "harness".into(),
        db_name: "type_zoo".into(),
        stream: "rows".into(),
    };
    let order = Arc::new(if s.gtid_on {
        MysqlOrder::gtid(&source.system_id)?
    } else {
        MysqlOrder::file(&s.server_uuid)?
    });
    let start = status(&mut admin).await?;
    let root = crate::test_root::scratch_root().join(format!("d15-{}", uuid::Uuid::new_v4()));
    for p in ["buffer", "journal", "scratch"] {
        std::fs::create_dir_all(root.join(p))?;
    }
    let store = Arc::new(ObjectBufferStore::local(
        root.join("buffer"),
        Duration::from_secs(3),
    )?);
    let rc = ReaderConfig {
        prefix: "copy".into(),
        migration_id: "d15".into(),
        stream: "rows".into(),
        limits: format::Limits::default(),
        required_next_mseq: 0,
    };
    let journal = Journal::new(
        Arc::new(journal::local_store(root.join("journal"))?),
        "copy",
        "d15",
        Duration::from_secs(3),
    )?
    .with_chunk_buffer(store.clone(), rc.clone(), order.clone());
    let mut coordinator = Coordinator::observer(
        journal.clone(),
        Local::create(root.join("local"))?,
        "mysql-d15",
    )?;
    coordinator.acquire().await?;
    // The real MySQL control installation is shared by the two scenarios; no PG installation is needed.
    for to in [State::Assessed, State::Copying] {
        coordinator
            .commit(Transition::Lifecycle {
                event: Event::Advance { to },
            })
            .await?;
    }
    let mut engine = Engine::new(coordinator, journal.clone());
    engine.plan(&plan).await?;
    let mut capture = MysqlCapture::new(
        cfg,
        source.clone(),
        start.clone(),
        &selected,
        writer(&root, store.clone(), order.clone(), 1).await?,
        6480,
    )
    .await?;
    let startup_inputs = startup_support::inputs(cfg, &selected).await;
    capture = capture.with_startup_inputs(startup_inputs.clone());
    engine
        .runtime()
        .marker(
            "source-startup",
            capture.startup_record()?.encode()?,
            String::new(),
        )
        .await?;
    let schedule = Arc::new(Schedule::default());
    let mut chunks = ScheduledChunks {
        inner: MysqlChunks::connect(cfg, tables.clone()).await?,
        schedule: schedule.clone(),
        crash,
    };
    if crash {
        let (_pause, paused) = watch::channel(false);
        let mut apply = DeferredApply;
        let mut b = budget();
        {
            let run = engine.run(
                &mut capture,
                &mut chunks,
                &mut apply,
                copy(&source, &order),
                &mut b,
                paused,
            );
            tokio::pin!(run);
            let crash_run = async {
                tokio::select! { result = &mut run => Err(format!("engine ended before crash: {result:?}").into()), _ = schedule.crash.notified() => Result::Ok(()) }
            };
            let (run, writes) = tokio::join!(crash_run, workload(cfg, name, &schedule));
            run?;
            writes?;
        } // Abruptly drop every borrowed engine worker before H, without graceful pause.
        let abandoned = engine.runtime().state().await;
        let id = serde_json::to_string(&plan.ranges[1].id)?;
        assert_eq!(abandoned.chunk_attempts[&id].attempt, 1);
        assert!(abandoned.chunk_attempts[&id].done.is_none());
        drop(capture);
        chunks.inner.disconnect().await?;
        drop(engine);
        let saved = journal.load(true).await?;
        assert!(saved.chunk_attempts[&id].done.is_none());
        let restart = if let Some((_, payload)) = saved.progress.get("buffered") {
            MysqlPosition::decode(
                engine::decode_buffered_position(payload, &source, order.domain())?.bytes(),
            )?
        } else {
            start
        };
        let mut coordinator = Coordinator::observer(
            journal.clone(),
            Local::create(root.join("recovered-local"))?,
            "mysql-d15-restart",
        )?;
        coordinator.acquire().await?;
        engine = Engine::new(coordinator, journal.clone());
        capture = MysqlCapture::new(
            cfg,
            source.clone(),
            restart,
            &selected,
            writer(&root, store.clone(), order.clone(), 2).await?,
            6481,
        )
        .await?;
        capture = capture.with_startup_inputs(startup_inputs);
        let mut retry = MysqlChunks::connect(cfg, tables.clone()).await?;
        finish(
            &engine,
            &mut capture,
            &mut retry,
            cfg,
            name,
            &source,
            &order,
        )
        .await?;
        assert_eq!(
            engine.runtime().state().await.chunk_attempts[&id].attempt,
            2
        );
        retry.disconnect().await?;
    } else {
        let (run, writes) = tokio::join!(
            finish(
                &engine,
                &mut capture,
                &mut chunks,
                cfg,
                name,
                &source,
                &order
            ),
            workload(cfg, name, &schedule)
        );
        run?;
        writes?;
        chunks.inner.disconnect().await?;
    }
    let state = journal.load(true).await?;
    assert_eq!(state.chunk_attempts.len(), 3);
    assert!(state.chunk_attempts.values().all(|a| a.done.is_some()));
    let mut planner = MysqlPlanner::connect(
        cfg,
        vec![MysqlPlannerTable::new(
            plan.clone(),
            destination(&tables[0]),
        )?],
        (*order).clone(),
        "d15".into(),
        "d15-origin".into(),
        2,
    )
    .await?;
    let mut reader = BufferReader::new(store, rc, ReaderCheckpoint::default(), order.clone())?;
    let mut actual = replay(&mut reader, &mut planner, &k, &state).await?;
    let full = ChunkPlan::new(k.clone(), 1000, vec![])?;
    let q = k.read_query_columns(&full.ranges[0], &tables[0].columns)?;
    let mut expected = admin
        .conn
        .exec::<mysql_async::Row, _, _>(q.sql, Params::Positional(q.parameters))
        .await?
        .into_iter()
        .map(|r| map_row(&tables[0].columns, r))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let identity = |r: &Row| k.key(r).unwrap().0;
    actual.sort_by_key(identity);
    expected.sort_by_key(identity);
    assert_eq!(actual, expected, "D15 row-by-row equality crash={crash}");
    eprintln!("D15 row-by-row equality passed crash={crash}");
    // Real source queries qualify batching, absent-row keys, exact boundary status and cancellation.
    let probe = expected.iter().cycle().take(7).cloned().collect::<Vec<_>>();
    let tail = Position::new(order.domain(), status(&mut admin).await?.encode()?);
    assert_eq!(
        planner
            .classify_keys(&k.event_table, &probe, &tail, &state)
            .await?,
        vec![RangeStatus::Done; 7]
    );
    let high = &state.chunk_attempts[&serde_json::to_string(&plan.ranges[0].id)?]
        .done
        .as_ref()
        .unwrap()
        .high;
    assert_eq!(
        planner
            .classify_keys(&k.event_table, &probe[..1], high, &state)
            .await?[0],
        RangeStatus::InWindow
    );
    let txn = skedaddle::event::AssembledTxn::new(
        source.clone(),
        skedaddle::event::TxnId(vec![1]),
        high.clone(),
        None,
        vec![],
    )?;
    drop(planner.plan(&txn, 0, &state));
    assert!(
        planner.plan(&txn, 0, &state).await.is_err(),
        "cancelled planner requires reconstruction"
    );
    planner.disconnect().await?;
    drop(reader);
    drop(capture);
    drop(engine);
    admin.conn.disconnect().await?;
    // The task owner removes the test root after this process exits. Windows
    // may retain local-store background handles briefly after Engine drops.
    Ok(())
}
#[tokio::test]
async fn live_mysql_engine_copy_concurrent_key_moves_and_crash_retry() -> Result {
    let Some(path) = std::env::var_os("SKEDADDLE_MYSQL_CONFIG") else {
        eprintln!(
            "MYSQL SKIPPED: SKEDADDLE_MYSQL_CONFIG absent; use harness mysql84 GTID ON or mysql80 GTID OFF with --test mysql_copy -- --nocapture"
        );
        return Ok(());
    };
    let cfg: MysqlConfig = serde_json::from_slice(&std::fs::read(path)?)?;
    let mut admin = cfg.connect().await?;
    install_watermark_table(&mut admin.conn).await?;
    admin.conn.disconnect().await?;
    for crash in [false, true] {
        timeout(Duration::from_secs(90), Box::pin(scenario(&cfg, crash))).await??;
    }
    Ok(())
}
