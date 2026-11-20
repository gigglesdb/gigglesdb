//! Engine admission restores exact capture schema before ordinary/fenced WAL.
use super::*;
use crate::{
    buffer::{
        backends::ObjectBufferStore,
        reader::{BufferReader, ReaderCheckpoint, ReaderConfig},
        writer::{BufferWriter, WriterCheckpoint, WriterConfig},
    },
    coordinator::{journal, local::Local, readiness::ReadinessFacts},
    event::{EventError, PositionOrder},
    pg::{
        ddl::{self, Column, DdlCommand, DdlRecord, SchemaVersions, TableDescriptor},
        pgoutput::{Relation, RelationColumn},
        source::{self, ColumnMapping},
    },
};
use serde_json::json;
use std::{cmp::Ordering, path::PathBuf};
const INSTALLATION: &str = "d11a";
const STREAM: &str = "[\"test\",\"migration\",\"wal\"]";
const DOMAIN: &str = "ddl-restart";
struct Order;
impl PositionOrder for Order {
    fn domain(&self) -> &str {
        DOMAIN
    }
    fn compare(&self, a: &[u8], b: &[u8]) -> std::result::Result<Ordering, EventError> {
        Ok(a.cmp(b))
    }
}
fn source_id() -> SourceIdentity {
    SourceIdentity {
        engine: "postgres".into(),
        system_id: "1".into(),
        lineage: "1".into(),
        db_name: "db".into(),
        stream: "slot".into(),
    }
}
type Resolver = fn(&Relation<'_>, &RelationColumn<'_>) -> source::Result<ColumnMapping>;
fn resolve(_: &Relation<'_>, _: &RelationColumn<'_>) -> source::Result<ColumnMapping> {
    Err(source::Error::RecoveryRequired(
        "no row types needed by D11a protocol fixture",
    ))
}
fn baseline() -> SchemaVersions {
    SchemaVersions::new(INSTALLATION.into(), 0, [descriptor(1), descriptor(2)]).unwrap()
}
fn descriptor(oid: u32) -> TableDescriptor {
    TableDescriptor {
        oid,
        schema: "public".into(),
        columns: vec![Column {
            attnum: 1,
            name: "id".into(),
            type_name: "integer".into(),
            notnull: false,
            dropped: false,
            identity: "".into(),
            generated: "".into(),
            missing: false,
            missing_value: None,
            default: None,
            attributes: json!({"type_oid":23,"typmod":-1,"collation":0,"acl":null,"storage":"p","compression":"","options":null,"fdwoptions":null,"local":true,"inhcount":0}),
        }],
        relation: json!({"kind":"r","partition":false,"filenode":1,"name":"a","namespace":2200,"owner":10,"acl":null,"options":null,"replica_identity":"d","tablespace":0,"persistence":"p","access_method":2,"row_security":false,"force_row_security":false,"partition_key":null,"partition_bound":null,"constraints":[],"indexes":[],"triggers":[],"policies":[],"parents":[]}),
    }
}
fn internal(version: u64) -> DdlRecord {
    DdlRecord {
        format: 1,
        installation: INSTALLATION.into(),
        origin: Some(INSTALLATION.into()),
        server_major: 18,
        version,
        tag: "REVOKE".into(),
        xid: "1".into(),
        query: "REVOKE INSERT ON public.a FROM app".into(),
        commands: vec![],
    }
}
fn alter() -> DdlRecord {
    let mut after = descriptor(1);
    let mut column = after.columns[0].clone();
    column.attnum = 2;
    column.name = "extra".into();
    after.columns.push(column);
    DdlRecord {
        origin: None,
        tag: "ALTER TABLE".into(),
        query: "ALTER TABLE public.a ADD COLUMN extra integer".into(),
        commands: vec![DdlCommand {
            classid: Some(1259),
            objid: Some(1),
            objsubid: Some(0),
            command_tag: "ALTER TABLE".into(),
            object_type: "table".into(),
            schema_name: Some("public".into()),
            object_identity: Some("public.a".into()),
            in_extension: false,
            columns: Some(after.columns),
            relation: Some(after.relation),
        }],
        ..internal(1)
    }
}
fn wal(message: Vec<u8>) -> Vec<u8> {
    let mut b = vec![b'w'];
    b.extend(999_u64.to_be_bytes());
    b.extend(9999_u64.to_be_bytes());
    b.extend(0_i64.to_be_bytes());
    b.extend(message);
    b
}
fn messages(record: &DdlRecord, lsn: u64) -> VecDeque<Vec<u8>> {
    let mut begin = vec![b'B'];
    begin.extend(lsn.to_be_bytes());
    begin.extend(0_i64.to_be_bytes());
    begin.extend(1_u32.to_be_bytes());
    let content = ddl::encode(record).unwrap();
    let mut logical = vec![b'M', 1];
    logical.extend((lsn - 1).to_be_bytes());
    logical.extend(b"skedaddle\0");
    logical.extend((content.len() as u32).to_be_bytes());
    logical.extend(content);
    let mut commit = vec![b'C', 0];
    commit.extend(lsn.to_be_bytes());
    commit.extend((lsn + 1).to_be_bytes());
    commit.extend(0_i64.to_be_bytes());
    [wal(begin), wal(logical), wal(commit)].into()
}
fn budget() -> ThroughputBudget {
    let rate = crate::budget::RateLimit {
        per_second: 1000000,
        burst: 1000000,
    };
    ThroughputBudget::new(
        crate::budget::Limits {
            chunk_reads: crate::budget::ReadLimits {
                rows: rate,
                bytes: rate,
            },
            verification_reads: crate::budget::ReadLimits {
                rows: rate,
                bytes: rate,
            },
            applier_transactions: rate,
            connections: Default::default(),
            in_memory_segments: 2,
        },
        Arc::new(crate::budget::SystemClock::default()),
    )
    .unwrap()
}
fn ready() -> ReadinessFacts {
    ReadinessFacts {
        all_chunks_done: true,
        capture_healthy: true,
        lineage_matches: true,
        indexes_constraints_valid: true,
        sequences_syncable: true,
        reverse_prerequisites: true,
        fence_risks_acknowledged: true,
        buffer_contract_matches: true,
        ..Default::default()
    }
}
struct NoCopy;
impl ChunkDatabase for NoCopy {
    async fn emit(&mut self, _: Watermark) -> Result<()> {
        panic!("copy marker while fenced/catching up")
    }
    async fn read(&mut self, _: &ChunkPlan, _: &ChunkRange) -> Result<Vec<Row>> {
        panic!("copy read while fenced/catching up")
    }
}

struct Bytes {
    input: mpsc::Receiver<Vec<u8>>,
    waiting: Arc<tokio::sync::Notify>,
}
impl source::Transport for Bytes {
    async fn next(&mut self, _: u64) -> source::Result<Vec<u8>> {
        if self.input.is_empty() {
            self.waiting.notify_one();
        }
        Ok(self
            .input
            .recv()
            .await
            .expect("transport retained during run"))
    }
}
struct NoApply;
impl Applier for NoApply {
    async fn step(&mut self, _: &Runtime) -> Result<bool> {
        Ok(false)
    }
}
struct Fixture {
    root: PathBuf,
    journal: Journal,
    objects: Arc<dyn object_store::ObjectStore>,
    store: Arc<ObjectBufferStore>,
}
impl Fixture {
    fn new() -> Self {
        let root = crate::test_root::scratch_root()
            .join(format!("engine-schema-{}", uuid::Uuid::new_v4()));
        for name in ["journal", "buffer", "scratch"] {
            std::fs::create_dir_all(root.join(name)).unwrap();
        }
        let objects = Arc::new(journal::local_store(root.join("journal")).unwrap());
        let store = Arc::new(
            ObjectBufferStore::local(root.join("buffer"), Duration::from_secs(5)).unwrap(),
        );
        let journal =
            Journal::new(objects.clone(), "test", "migration", Duration::from_secs(5)).unwrap();
        Self {
            root,
            journal,
            objects,
            store,
        }
    }
    async fn owner(&self) -> Engine {
        let mut owner = Coordinator::observer(
            self.journal.clone(),
            Local::create(self.root.join(uuid::Uuid::new_v4().to_string())).unwrap(),
            "schema-owner",
        )
        .unwrap();
        owner.acquire().await.unwrap();
        if owner.state().lifecycle.state == State::Defined {
            for to in [State::Assessed, State::Copying, State::CatchingUp] {
                owner
                    .commit(Transition::Lifecycle {
                        event: Event::Advance { to },
                    })
                    .await
                    .unwrap();
            }
        }
        let engine = Engine::new(owner, self.journal.clone());
        if engine.runtime.snapshot().lifecycle.state == State::Paused {
            engine.resume().await.unwrap();
        }
        engine
    }
    async fn capture(
        &self,
        engine: &Engine,
        checkpoint: WriterCheckpoint,
    ) -> (
        PgCapture<Resolver, Bytes>,
        mpsc::Sender<Vec<u8>>,
        Arc<tokio::sync::Notify>,
    ) {
        let (ack, _) = std::sync::mpsc::channel();
        let writer = BufferWriter::open_from_checkpoint(
            self.store.clone(),
            WriterConfig {
                prefix: "test".into(),
                migration_id: "migration".into(),
                stream: "wal".into(),
                epoch: engine.runtime.snapshot().epoch,
                segment_bytes: 1000000,
                segment_transactions: 100,
                segment_age: Duration::from_secs(1),
                max_transaction_bytes: 1000000,
            },
            checkpoint,
            Arc::new(Order),
            ack,
            self.root.join("scratch"),
        )
        .await
        .unwrap();
        let capture = super::capture(
            source_id(),
            DOMAIN.into(),
            10,
            writer,
            resolve as Resolver,
            INSTALLATION.into(),
            baseline(),
        )
        .unwrap();
        let (tx, input) = mpsc::channel(32);
        let waiting = Arc::new(tokio::sync::Notify::new());
        let (_gate, paused) = watch::channel(false);
        (
            PgCapture::new(
                capture,
                Bytes {
                    input,
                    waiting: waiting.clone(),
                },
                paused,
                self.objects.clone(),
                STREAM.into(),
                Arc::new(Order),
                source_id(),
            ),
            tx,
            waiting,
        )
    }
    async fn run(
        &self,
        engine: &Engine,
        capture: &mut PgCapture<Resolver, Bytes>,
        waiting: &tokio::sync::Notify,
        fenced: bool,
    ) {
        let (pause, paused) = watch::channel(false);
        let mut chunks = NoCopy;
        let mut apply = NoApply;
        let mut budget = budget();
        let config = CopyConfig {
            source: source_id(),
            order_domain: DOMAIN.into(),
            schema_version: 0,
            read_rows: 100,
            read_bytes: 10000,
            window_bytes: 1000000,
        };
        let driver = async {
            waiting.notified().await;
            pause.send_replace(true);
        };
        let operation = async {
            if fenced {
                engine
                    .run_fenced_until(
                        capture,
                        &mut chunks,
                        &mut apply,
                        config,
                        &mut budget,
                        paused,
                        StopAt::UntilPaused,
                    )
                    .await
            } else {
                engine
                    .run_until(
                        capture,
                        &mut chunks,
                        &mut apply,
                        config,
                        &mut budget,
                        paused,
                        StopAt::UntilPaused,
                    )
                    .await
            }
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(20), async {
            tokio::join!(operation, driver)
        })
        .await
        .unwrap();
        result.unwrap();
    }
}
async fn send(tx: &mpsc::Sender<Vec<u8>>, record: DdlRecord, lsn: u64) {
    for b in messages(&record, lsn) {
        tx.send(b).await.unwrap();
    }
}
async fn restart(fenced: bool) {
    let f = Fixture::new();
    let e = f.owner().await;
    let (mut c, tx, waiting) = f.capture(&e, WriterCheckpoint::default()).await;
    send(&tx, internal(1), 20).await;
    send(
        &tx,
        DdlRecord {
            version: 2,
            ..alter()
        },
        30,
    )
    .await;
    f.run(&e, &mut c, &waiting, false).await;
    assert_eq!(
        SchemaVersions::restore(INSTALLATION, c.capture.schema_snapshot())
            .unwrap()
            .last_version(),
        2
    );
    let checkpoint = c.capture.writer().checkpoint().unwrap();
    assert_eq!(checkpoint.next_mseq, 2);
    drop(c);
    drop(e);
    drop(tx);
    let e = f.owner().await;
    assert_eq!(e.runtime.snapshot().epoch, 2);
    if fenced {
        e.runtime
            .commit(Transition::Lifecycle {
                event: Event::Readiness { facts: ready() },
            })
            .await
            .unwrap();
        e.runtime
            .commit(Transition::Lifecycle {
                event: Event::Advance {
                    to: State::SourceFenced,
                },
            })
            .await
            .unwrap();
    }
    let (mut c, tx, waiting) = f.capture(&e, checkpoint).await;
    send(&tx, internal(3), 40).await;
    f.run(&e, &mut c, &waiting, fenced).await;
    let schemas = SchemaVersions::restore(INSTALLATION, c.capture.schema_snapshot()).unwrap();
    assert_eq!(schemas.last_version(), 3);
    assert_eq!(schemas.table(1).unwrap().version, 2);
    assert_eq!(schemas.table(2).unwrap().version, 0);
    assert_eq!(schemas.table(1).unwrap().descriptor.columns.len(), 2);
    assert!(!c.capture.schema_state().unknown_schema_job);
    assert!(c.capture.schema_state().blocked_boundary.is_none());
    assert_eq!(c.capture.durable_lsn(), 41);
    assert_eq!(c.capture.writer().checkpoint().unwrap().next_mseq, 3);
    let mut reader = BufferReader::new(
        f.store,
        ReaderConfig {
            prefix: "test".into(),
            migration_id: "migration".into(),
            stream: "wal".into(),
            limits: Default::default(),
            required_next_mseq: 3,
        },
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    for _ in 0..3 {
        assert!(reader.next_txn().await.unwrap().is_some());
    }
    assert!(reader.next_txn().await.unwrap().is_none());
}
#[tokio::test]
async fn ordinary_owner_restart_restores_schema_before_next_ddl() {
    Box::pin(restart(false)).await;
}
#[tokio::test]
async fn fenced_owner_restart_restores_schema_before_next_ddl() {
    Box::pin(restart(true)).await;
}
#[tokio::test]
async fn prepared_capture_continuation_keeps_inflight_ddl_in_same_epoch() {
    let f = Fixture::new();
    let e = f.owner().await;
    let (mut c, tx, waiting) = f.capture(&e, WriterCheckpoint::default()).await;
    let mut bytes = messages(&internal(1), 20);
    tx.send(bytes.pop_front().unwrap()).await.unwrap();
    tx.send(bytes.pop_front().unwrap()).await.unwrap();
    f.run(&e, &mut c, &waiting, false).await;
    assert!(c.capture.schema_state().unknown_schema_job);
    e.resume().await.unwrap();
    tx.send(bytes.pop_front().unwrap()).await.unwrap();
    f.run(&e, &mut c, &waiting, false).await;
    assert_eq!(
        SchemaVersions::restore(INSTALLATION, c.capture.schema_snapshot())
            .unwrap()
            .last_version(),
        1
    );
    assert_eq!(c.capture.durable_lsn(), 21);
    assert!(!c.capture.schema_state().unknown_schema_job);
}

#[tokio::test]
async fn buffered_feedback_never_passes_durable_marker() {
    let fixture = Fixture::new();
    let engine = fixture.owner().await;
    let runtime = engine.runtime();
    runtime
        .marker(
            "buffered",
            "manifest-durable".into(),
            encode_buffered_position(&source_id(), &Position::new(DOMAIN, 100u64.to_be_bytes()))
                .unwrap(),
        )
        .await
        .unwrap();
    runtime
        .progress_marker(
            "buffered",
            "manifest-durable".into(),
            encode_buffered_position(&source_id(), &Position::new(DOMAIN, 200u64.to_be_bytes()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        super::pg::durable_feedback_lsn(&runtime, 250, 0, &source_id(), DOMAIN).unwrap(),
        100
    );
    runtime.flush_progress().await.unwrap();
    assert_eq!(
        super::pg::durable_feedback_lsn(&runtime, 250, 0, &source_id(), DOMAIN).unwrap(),
        200
    );
    assert_eq!(
        super::pg::durable_feedback_lsn(&runtime, 150, 0, &source_id(), DOMAIN).unwrap(),
        150
    );
}

#[tokio::test]
async fn schema_restore_rejects_obsolete_or_wrong_domain_buffered_progress() {
    let wrong = encode_buffered_position(&source_id(), &Position::new("foreign", [0; 8])).unwrap();
    for payload in [
        "123".to_string(),
        "{}".into(),
        r#"{"version":2}"#.into(),
        wrong,
    ] {
        let fixture = Fixture::new();
        let engine = fixture.owner().await;
        engine
            .runtime()
            .marker("buffered", "manifest-durable".into(), payload)
            .await
            .unwrap();
        let (mut capture, _tx, _) = fixture.capture(&engine, WriterCheckpoint::default()).await;
        assert!(matches!(
            capture
                .prepare_schema(&fixture.journal, &engine.runtime())
                .await,
            Err(coordinator::Error::RecoveryRequired(_))
        ));
    }
}
