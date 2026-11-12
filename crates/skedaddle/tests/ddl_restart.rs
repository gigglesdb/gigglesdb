//! Crash-window qualification against a real filesystem, coordinator journal and
//! buffer. The object-store wrapper only injects cancellation/outage points;
//! every successful operation reaches the real fsync-enabled local backend.
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use object_store::{ObjectStore, ObjectStoreExt, path::Path, *};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use skedaddle::{
    buffer::{
        backends::{BufferStore, ObjectBufferStore},
        format::Limits,
        gc::{AppliedSegment, RetentionGc, RetentionPolicy},
        reader::{BufferReader, ReaderCheckpoint, ReaderConfig},
        writer::{BufferWriter, WriterCheckpoint, WriterConfig},
    },
    coordinator::{
        Transition,
        epoch::Coordinator,
        gc_authority::CoordinatorGcJournal,
        journal::{self, Entry, Journal},
        local::Local,
    },
    engine::{Engine, Runtime},
    event::{AssembledTxn, EventError, Position, PositionOrder, SourceIdentity, TxnId},
    pg::{
        chunk,
        ddl::{
            self, Classification, Column, DdlCommand, DdlRecord, SchemaVersions, TableDescriptor,
        },
        pgoutput::{Relation, RelationColumn},
        source::{self, Capture, ColumnMapping, SchemaJournalBinding, Step, WatermarkSink},
    },
};
use std::{
    cmp::Ordering,
    collections::VecDeque,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering as AtomicOrdering},
        mpsc,
    },
    time::Duration,
};
use tokio::sync::{Mutex, Notify, watch};

const INSTALLATION: &str = "d11a";
const STREAM: &str = "test/migration/wal";
const DOMAIN: &str = "ddl-restart";

// 1/2 cancel before/after object creation; 3/4 cancel before/after
// promotion's journal append; 5/6 fail preparation/promotion journal puts.
#[derive(Debug, Default)]
struct Counts {
    operations: AtomicU64,
    prepares: AtomicU64,
    promotes: AtomicU64,
    creates: AtomicU64,
    current: std::sync::Mutex<Option<String>>,
}
impl Counts {
    fn reset(&self) {
        for counter in [
            &self.operations,
            &self.prepares,
            &self.promotes,
            &self.creates,
        ] {
            counter.store(0, AtomicOrdering::SeqCst);
        }
    }
    fn values(&self) -> [u64; 4] {
        [
            &self.operations,
            &self.prepares,
            &self.promotes,
            &self.creates,
        ]
        .map(|c| c.load(AtomicOrdering::SeqCst))
    }
}
#[derive(Debug)]
struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    mode: Arc<AtomicU8>,
    entered: Arc<Notify>,
    counts: Arc<Counts>,
}
impl std::fmt::Display for FaultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("D11a fault injection over local filesystem")
    }
}
impl FaultStore {
    fn arm(&self, mode: u8) {
        self.mode.store(mode, AtomicOrdering::SeqCst);
    }
    async fn trigger(&self, mode: u8) -> object_store::Result<()> {
        if self
            .mode
            .compare_exchange(mode, 0, AtomicOrdering::SeqCst, AtomicOrdering::SeqCst)
            .is_ok()
        {
            self.entered.notify_one();
            if mode >= 5 {
                return Err(object_store::Error::Generic {
                    store: "D11a injected journal outage",
                    source: std::io::Error::other("outage").into(),
                });
            }
            futures::future::pending::<()>().await;
        }
        Ok(())
    }
}
#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.counts.operations.fetch_add(1, AtomicOrdering::SeqCst);
        let snapshot = location.as_ref().starts_with("capture-schema/");
        let bytes: Vec<u8> = payload.iter().flat_map(|b| b.iter().copied()).collect();
        let head = serde_json::from_slice::<Entry>(&bytes)
            .ok()
            .and_then(|entry| match entry.transition {
                Transition::Progress { stream, marker, .. }
                    if stream.starts_with("capture-schema:") =>
                {
                    serde_json::from_str::<Value>(&marker).ok()
                }
                _ => None,
            });
        let preparing = head.as_ref().is_some_and(|h| !h["prepared"].is_null());
        let promoting = head.as_ref().is_some_and(|h| {
            h["prepared"].is_null() && !h["current"].is_null() && !h["deleting"].is_null()
        });
        if snapshot {
            self.trigger(1).await?;
        }
        if preparing {
            self.trigger(5).await?;
        }
        if promoting {
            self.trigger(3).await?;
            self.trigger(6).await?;
        }
        let result = self.inner.put_opts(location, payload, options).await?;
        if snapshot {
            self.counts.creates.fetch_add(1, AtomicOrdering::SeqCst);
            self.trigger(2).await?;
        }
        if preparing {
            self.counts.prepares.fetch_add(1, AtomicOrdering::SeqCst);
        }
        if let Some(head) = &head
            && let Some(digest) = head["current"]["digest"].as_str()
        {
            let mut current = self.counts.current.lock().unwrap();
            if current.as_deref() != Some(digest) {
                self.counts.promotes.fetch_add(1, AtomicOrdering::SeqCst);
                *current = Some(digest.into());
            }
        }
        if promoting {
            self.trigger(4).await?;
        }
        Ok(result)
    }
    async fn put_multipart_opts(
        &self,
        p: &Path,
        o: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.counts.operations.fetch_add(1, AtomicOrdering::SeqCst);
        self.inner.put_multipart_opts(p, o).await
    }
    async fn get_opts(&self, p: &Path, o: GetOptions) -> object_store::Result<GetResult> {
        self.counts.operations.fetch_add(1, AtomicOrdering::SeqCst);
        self.inner.get_opts(p, o).await
    }
    fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.counts.operations.fetch_add(1, AtomicOrdering::SeqCst);
        self.inner.list(p)
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
        self.counts.operations.fetch_add(1, AtomicOrdering::SeqCst);
        self.inner.list_with_delimiter(p).await
    }
    fn delete_stream(
        &self,
        p: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = self.inner.clone();
        let mode = self.mode.clone();
        let entered = self.entered.clone();
        let counts = self.counts.clone();
        p.then(move |path| {
            let (inner, mode, entered, counts) =
                (inner.clone(), mode.clone(), entered.clone(), counts.clone());
            async move {
                let path = path?;
                counts.operations.fetch_add(1, AtomicOrdering::SeqCst);
                if mode
                    .compare_exchange(7, 0, AtomicOrdering::SeqCst, AtomicOrdering::SeqCst)
                    .is_ok()
                {
                    return Err(object_store::Error::Generic {
                        store: "D11a delete outage",
                        source: std::io::Error::other("outage").into(),
                    });
                }
                inner.delete(&path).await?;
                if mode
                    .compare_exchange(8, 0, AtomicOrdering::SeqCst, AtomicOrdering::SeqCst)
                    .is_ok()
                {
                    entered.notify_one();
                    futures::future::pending::<()>().await;
                }
                Ok(path)
            }
        })
        .boxed()
    }
    async fn copy_opts(&self, a: &Path, b: &Path, o: CopyOptions) -> object_store::Result<()> {
        self.counts.operations.fetch_add(1, AtomicOrdering::SeqCst);
        self.inner.copy_opts(a, b, o).await
    }
    async fn rename_opts(&self, a: &Path, b: &Path, o: RenameOptions) -> object_store::Result<()> {
        self.counts.operations.fetch_add(1, AtomicOrdering::SeqCst);
        self.inner.rename_opts(a, b, o).await
    }
}

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
        slot: "slot".into(),
    }
}
type Resolver = fn(&Relation<'_>, &RelationColumn<'_>) -> source::Result<ColumnMapping>;
fn resolve(_: &Relation<'_>, column: &RelationColumn<'_>) -> source::Result<ColumnMapping> {
    if column.type_oid == 23 {
        Ok(ColumnMapping::identity(skedaddle::mapping::PgType::Int4))
    } else {
        Err(source::Error::RecoveryRequired(
            "unknown protocol fixture type",
        ))
    }
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
            classid: 1259,
            objid: 1,
            objsubid: 0,
            command_tag: "ALTER TABLE".into(),
            object_type: "table".into(),
            schema_name: Some("public".into()),
            object_identity: "public.a".into(),
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
// Protocol bytes only; no SQL, database or object-store behavior is simulated.
struct Bytes(VecDeque<Vec<u8>>);
impl source::Transport for Bytes {
    async fn next(&mut self, _: u64) -> source::Result<Vec<u8>> {
        Ok(self.0.pop_front().expect("unexpected extra WAL read"))
    }
}
#[derive(Default)]
struct Sink(usize);
impl WatermarkSink for Sink {
    fn committed(&mut self, _: &AssembledTxn) -> source::Result<()> {
        self.0 += 1;
        Ok(())
    }
    fn watermark(&mut self, _: source::Watermark) -> source::Result<()> {
        Ok(())
    }
}

struct Fixture {
    root: PathBuf,
    objects: Arc<FaultStore>,
    journal: Journal,
    buffer: Arc<ObjectBufferStore>,
}
impl Fixture {
    fn new() -> Self {
        let parent = PathBuf::from(
            std::env::var_os("SKEDADDLE_TEST_ROOT").expect("set task SKEDADDLE_TEST_ROOT"),
        );
        assert!(parent.starts_with("J:/gigglesdb-work"));
        let root = parent.join(format!("ddl-restart-{}", uuid::Uuid::new_v4()));
        for dir in ["objects", "buffer", "scratch"] {
            std::fs::create_dir_all(root.join(dir)).unwrap();
        }
        let objects = Arc::new(FaultStore {
            inner: Arc::new(journal::local_store(root.join("objects")).unwrap()),
            mode: Arc::new(AtomicU8::new(0)),
            entered: Arc::new(Notify::new()),
            counts: Arc::new(Counts::default()),
        });
        let journal =
            Journal::new(objects.clone(), "test", "migration", Duration::from_secs(2)).unwrap();
        let buffer = Arc::new(
            ObjectBufferStore::local(root.join("buffer"), Duration::from_secs(2)).unwrap(),
        );
        Self {
            root,
            objects,
            journal,
            buffer,
        }
    }
    async fn coordinator(&self) -> Coordinator {
        let mut c = Coordinator::observer(
            self.journal.clone(),
            Local::create(self.root.join(format!("owner-{}", uuid::Uuid::new_v4()))).unwrap(),
            "d11a-owner",
        )
        .unwrap();
        c.acquire().await.unwrap();
        c
    }
    async fn owner(&self) -> Engine {
        Engine::new(self.coordinator().await, self.journal.clone())
    }
    fn binding(&self, runtime: Runtime) -> SchemaJournalBinding {
        SchemaJournalBinding {
            journal: self.journal.clone(),
            runtime,
            objects: self.objects.clone(),
            stream: STREAM.into(),
        }
    }
    fn config(epoch: u64) -> WriterConfig {
        WriterConfig {
            prefix: "test".into(),
            migration_id: "migration".into(),
            stream: "wal".into(),
            epoch,
            segment_bytes: u64::MAX,
            segment_transactions: u64::MAX,
            segment_age: Duration::from_secs(60),
            max_transaction_bytes: 8 * 1024 * 1024,
        }
    }
    async fn capture(&self, epoch: u64, checkpoint: WriterCheckpoint) -> Capture<Resolver> {
        let (tx, _rx) = mpsc::channel();
        let writer = BufferWriter::open_from_checkpoint(
            self.buffer.clone(),
            Self::config(epoch),
            checkpoint,
            Arc::new(Order),
            tx,
            self.root.join("scratch"),
        )
        .await
        .unwrap();
        Capture::new(
            source_id(),
            DOMAIN.into(),
            10,
            writer,
            resolve as Resolver,
            INSTALLATION.into(),
            baseline(),
            chunk::MESSAGE_PREFIX.into(),
        )
        .unwrap()
    }
    async fn fresh(&self) -> (Engine, Capture<Resolver>) {
        let e = self.owner().await;
        let mut c = self
            .capture(e.runtime().state().await.epoch, WriterCheckpoint::default())
            .await;
        c.restore_schema(self.binding(e.runtime())).await.unwrap();
        (e, c)
    }
    async fn head(&self) -> Value {
        let state = self.journal.load(true).await.unwrap();
        serde_json::from_str(&state.progress[&format!("capture-schema:{STREAM}")].0).unwrap()
    }
    fn key(&self, digest: &str) -> Path {
        Path::from(format!(
            "capture-schema/{:x}/{:x}/{digest}.json",
            Sha256::digest(self.journal.namespace().as_bytes()),
            Sha256::digest(STREAM.as_bytes())
        ))
    }
    async fn snapshots(&self) -> usize {
        self.objects
            .list(Some(&Path::from("capture-schema")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len()
    }
}
async fn prefix(
    c: &mut Capture<Resolver>,
    b: &mut Bytes,
    sink: &mut Sink,
    paused: &mut watch::Receiver<bool>,
) {
    for _ in 0..2 {
        assert_eq!(c.step(b, sink, paused).await.unwrap(), Step::Message);
    }
}
async fn capture_record(c: &mut Capture<Resolver>, record: &DdlRecord, lsn: u64) -> Step {
    let mut bytes = Bytes(messages(record, lsn));
    let mut sink = Sink::default();
    let (_tx, mut paused) = watch::channel(false);
    prefix(c, &mut bytes, &mut sink, &mut paused).await;
    c.step(&mut bytes, &mut sink, &mut paused).await.unwrap()
}
fn versions(c: &Capture<Resolver>) -> SchemaVersions {
    SchemaVersions::restore(INSTALLATION, c.schema_snapshot()).unwrap()
}

#[tokio::test]
async fn initial_restore_cancellation_can_retry_the_same_capture() {
    for mode in [1, 2] {
        let f = Fixture::new();
        let e = f.owner().await;
        let mut c = f
            .capture(e.runtime().state().await.epoch, WriterCheckpoint::default())
            .await;
        f.objects.arm(mode);
        tokio::select! {
            _ = f.objects.entered.notified() => (),
            result = c.restore_schema(f.binding(e.runtime())) => panic!("expected cancellation, {result:?}"),
        }
        assert!(c.schema_state().unknown_schema_job);
        c.restore_schema(f.binding(e.runtime())).await.unwrap();
        assert!(!c.schema_state().unknown_schema_job);
        assert_eq!(f.snapshots().await, 1);
        assert!(matches!(
            capture_record(&mut c, &internal(1), 20).await,
            Step::Durable { stop: None, .. }
        ));
    }
}

#[tokio::test]
async fn snapshot_delete_outage_and_cancelled_completion_keep_retryable_intents() {
    for mode in [7, 8] {
        let f = Fixture::new();
        let (e, mut c) = f.fresh().await;
        let mut bytes = Bytes(messages(&alter(), 20));
        let mut sink = Sink::default();
        let (_tx, mut paused) = watch::channel(false);
        prefix(&mut c, &mut bytes, &mut sink, &mut paused).await;
        f.objects.arm(mode);
        if mode == 7 {
            assert!(c.step(&mut bytes, &mut sink, &mut paused).await.is_err());
        } else {
            tokio::select! {
                _ = f.objects.entered.notified() => (),
                result = c.step(&mut bytes, &mut sink, &mut paused) => panic!("expected cancellation, {result:?}"),
            }
        }
        assert!(!f.head().await["deleting"].is_null());
        assert_eq!(c.durable_lsn(), 10);
        assert_eq!(sink.0, 0);
        let checkpoint = c.writer().checkpoint().unwrap();
        drop(c);
        drop(e);
        let e = f.owner().await;
        let mut c = f.capture(e.runtime().state().await.epoch, checkpoint).await;
        c.restore_schema(f.binding(e.runtime())).await.unwrap();
        assert!(f.head().await["deleting"].is_null());
        assert_eq!(f.snapshots().await, 1);
        assert_eq!(versions(&c).table(1).unwrap().version, 1);
        assert!(matches!(
            capture_record(&mut c, &internal(2), 30).await,
            Step::Durable { stop: None, .. }
        ));
    }
}

#[tokio::test]
async fn internal_and_application_ddl_restore_global_and_individual_mapped_versions() {
    for record in [internal(1), alter()] {
        let f = Fixture::new();
        let (e, mut c) = f.fresh().await;
        assert!(matches!(
            capture_record(&mut c, &record, 20).await,
            Step::Durable { stop: None, .. }
        ));
        let saved = c.writer().checkpoint().unwrap();
        drop(c);
        drop(e);
        let e = f.owner().await;
        let mut c = f.capture(e.runtime().state().await.epoch, saved).await;
        c.restore_schema(f.binding(e.runtime())).await.unwrap();
        assert_eq!(c.durable_lsn(), 21);
        assert_eq!(versions(&c).last_version(), 1);
        assert_eq!(
            versions(&c).table(1).unwrap().version,
            u64::from(record.origin.is_none())
        );
        assert_eq!(versions(&c).table(2).unwrap().version, 0);
        assert!(matches!(
            capture_record(&mut c, &internal(2), 30).await,
            Step::Durable { stop: None, .. }
        ));
        assert_eq!(versions(&c).last_version(), 2);
        assert_eq!(
            f.snapshots().await,
            1,
            "promotion deletes superseded objects"
        );
    }
}

#[tokio::test]
async fn cancellation_before_and_after_snapshot_creation_retains_previous_boundary() {
    for mode in [1, 2] {
        let f = Fixture::new();
        let (e, mut c) = f.fresh().await;
        let mut bytes = Bytes(messages(&alter(), 20));
        let mut sink = Sink::default();
        let (_tx, mut paused) = watch::channel(false);
        prefix(&mut c, &mut bytes, &mut sink, &mut paused).await;
        f.objects.arm(mode);
        tokio::select! { _=f.objects.entered.notified()=>(), result=c.step(&mut bytes,&mut sink,&mut paused)=>panic!("expected cancellation, {result:?}") }
        assert_eq!(c.durable_lsn(), 10);
        assert_eq!(c.writer().checkpoint().unwrap().next_mseq, 0);
        assert_eq!(sink.0, 0);
        assert!(!f.head().await["prepared"].is_null());
        assert!(
            f.buffer
                .get(&reader_config().manifest_key(0))
                .await
                .unwrap()
                .is_none()
        );
        drop(c);
        drop(e);
        let e = f.owner().await;
        let mut c = f
            .capture(e.runtime().state().await.epoch, WriterCheckpoint::default())
            .await;
        c.restore_schema(f.binding(e.runtime())).await.unwrap();
        assert_eq!(versions(&c).last_version(), 0);
        assert_eq!(f.snapshots().await, 1);
        assert!(matches!(
            capture_record(&mut c, &alter(), 20).await,
            Step::Durable { stop: None, .. }
        ));
    }
}

#[tokio::test]
async fn cancellation_retries_same_capture_without_reading_or_duplicate_snapshots() {
    for mode in [1, 2, 3, 4] {
        let f = Fixture::new();
        let (e, mut c) = f.fresh().await;
        let mut bytes = Bytes(messages(&alter(), 20));
        let mut sink = Sink::default();
        let (_tx, mut paused) = watch::channel(false);
        prefix(&mut c, &mut bytes, &mut sink, &mut paused).await;
        f.objects.arm(mode);
        tokio::select! { _=f.objects.entered.notified()=>(), result=c.step(&mut bytes,&mut sink,&mut paused)=>panic!("expected cancellation, {result:?}") }
        assert_eq!(c.durable_lsn(), 10);
        assert_eq!(sink.0, 0);
        assert_eq!(
            c.step(&mut bytes, &mut sink, &mut paused).await.unwrap(),
            Step::Durable {
                end_lsn: 21,
                stop: None
            }
        );
        assert_eq!(sink.0, 1);
        assert_eq!(f.snapshots().await, 1);
        assert_eq!(versions(&c).last_version(), 1);
        drop(e);
    }
}

#[tokio::test]
async fn recovered_writer_selects_prepared_successor_after_manifest_before_promotion() {
    for mode in [3, 4] {
        let f = Fixture::new();
        let (e, mut c) = f.fresh().await;
        let mut bytes = Bytes(messages(&alter(), 20));
        let mut sink = Sink::default();
        let (_tx, mut paused) = watch::channel(false);
        prefix(&mut c, &mut bytes, &mut sink, &mut paused).await;
        f.objects.arm(mode);
        tokio::select! { _=f.objects.entered.notified()=>(), result=c.step(&mut bytes,&mut sink,&mut paused)=>panic!("expected cancellation, {result:?}") }
        assert_eq!(c.writer().checkpoint().unwrap().next_mseq, 1);
        assert_eq!(c.durable_lsn(), 10);
        assert_eq!(sink.0, 0);
        drop(c);
        drop(e);
        let e = f.owner().await; // Deliberately recover from the older genesis checkpoint.
        let mut c = f
            .capture(e.runtime().state().await.epoch, WriterCheckpoint::default())
            .await;
        c.restore_schema(f.binding(e.runtime())).await.unwrap();
        assert_eq!(c.durable_lsn(), 21);
        assert_eq!(versions(&c).table(1).unwrap().version, 1);
        assert!(matches!(
            capture_record(&mut c, &internal(2), 30).await,
            Step::Durable { stop: None, .. }
        ));
        assert_eq!(f.snapshots().await, 1);
    }
}

#[tokio::test]
async fn journal_outages_never_ack_or_callback_unpromoted_schema_state() {
    for mode in [5, 6] {
        let f = Fixture::new();
        let (e, mut c) = f.fresh().await;
        let mut bytes = Bytes(messages(&internal(1), 20));
        let mut sink = Sink::default();
        let (_tx, mut paused) = watch::channel(false);
        prefix(&mut c, &mut bytes, &mut sink, &mut paused).await;
        f.objects.arm(mode);
        assert!(c.step(&mut bytes, &mut sink, &mut paused).await.is_err());
        assert_eq!(c.durable_lsn(), 10);
        assert_eq!(sink.0, 0);
        assert_eq!(
            c.writer().checkpoint().unwrap().next_mseq,
            u64::from(mode == 6)
        );
        drop(c);
        drop(e);
        let e = f.owner().await;
        let mut c = f
            .capture(e.runtime().state().await.epoch, WriterCheckpoint::default())
            .await;
        c.restore_schema(f.binding(e.runtime())).await.unwrap();
        assert_eq!(versions(&c).last_version(), u64::from(mode == 6));
        if mode == 5 {
            capture_record(&mut c, &internal(1), 20).await;
        }
        assert!(matches!(
            capture_record(&mut c, &internal(2), 30).await,
            Step::Durable { stop: None, .. }
        ));
    }
}

fn reader_config() -> ReaderConfig {
    ReaderConfig {
        prefix: "test".into(),
        migration_id: "migration".into(),
        stream: "wal".into(),
        limits: Limits::default(),
        required_next_mseq: 0,
    }
}

#[tokio::test]
async fn snapshot_survives_real_buffer_gc_with_pruned_redelivery_history() {
    let f = Fixture::new();
    let (e, mut c) = f.fresh().await;
    capture_record(&mut c, &alter(), 20).await;
    capture_record(&mut c, &internal(2), 30).await;
    let mut writer = c.writer().checkpoint().unwrap();
    let mut reader = BufferReader::new(
        f.buffer.clone(),
        reader_config(),
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    reader.next_txn().await.unwrap().unwrap();
    let first = reader.last_verified().unwrap().clone();
    reader.next_txn().await.unwrap().unwrap();
    let reader_checkpoint = reader.checkpoint().clone();
    drop(reader);
    drop(c);
    drop(e);
    let coordinator = Arc::new(Mutex::new(f.coordinator().await));
    let authority = CoordinatorGcJournal::new(
        coordinator.clone(),
        f.buffer.clone(),
        reader_config(),
        Arc::new(Order),
    )
    .await
    .unwrap();
    authority
        .persist_reader_checkpoint(&reader_checkpoint)
        .await
        .unwrap();
    // Redelivery before the last boundary may legitimately be retired too.
    writer
        .redelivery
        .retain(|r| r.position == Position::new(DOMAIN, 30_u64.to_be_bytes().to_vec()));
    writer.redelivery_from = Some(Position::new(DOMAIN, 30_u64.to_be_bytes().to_vec()));
    authority.persist_writer_checkpoint(&writer).await.unwrap();
    authority
        .persist_applied_through(&Position::new(DOMAIN, 30_u64.to_be_bytes().to_vec()))
        .await
        .unwrap();
    let gc = RetentionGc::new(
        f.buffer.clone(),
        Arc::new(Order),
        RetentionPolicy {
            retained_for_ms: 0,
            orphan_grace_ms: 0,
        },
    );
    assert_eq!(
        gc.collect_applied(&AppliedSegment::new(first.clone(), 0, 0), 1, &authority)
            .await
            .unwrap()
            .deleted_objects,
        2
    );
    assert!(f.buffer.get(&first.manifest_key()).await.unwrap().is_none());
    assert!(
        f.buffer
            .get(&first.entry().segment_key)
            .await
            .unwrap()
            .is_none()
    );
    drop(authority);
    drop(coordinator);
    let e = f.owner().await;
    let mut c = f.capture(e.runtime().state().await.epoch, writer).await;
    c.restore_schema(f.binding(e.runtime())).await.unwrap();
    assert_eq!(versions(&c).last_version(), 2);
    assert_eq!(versions(&c).table(1).unwrap().version, 1);
    assert!(matches!(
        capture_record(&mut c, &internal(3), 40).await,
        Step::Durable { stop: None, .. }
    ));
}

#[tokio::test]
async fn tampered_missing_foreign_and_mismatched_snapshots_fail_closed() {
    for change in ["tamper", "missing", "foreign", "boundary", "unknown"] {
        let f = Fixture::new();
        let (e, mut c) = f.fresh().await;
        capture_record(&mut c, &alter(), 20).await;
        let saved = c.writer().checkpoint().unwrap();
        let mut head = f.head().await;
        let key = f.key(head["current"]["digest"].as_str().unwrap());
        if change == "missing" {
            f.objects.delete(&key).await.unwrap();
        } else {
            let mut data: Value =
                serde_json::from_slice(&f.objects.get(&key).await.unwrap().bytes().await.unwrap())
                    .unwrap();
            match change {
                "tamper" => data["end_lsn"] = json!(999),
                "foreign" => data["identity"]["source"][1] = json!("another-system"),
                "boundary" => data["record"]["position"] = json!(999),
                "unknown" => data["schemas"]["tables"][0]["version"] = json!(99),
                _ => unreachable!(),
            }
            let bytes = serde_json::to_vec(&data).unwrap();
            if change == "tamper" {
                f.objects.put(&key, bytes.into()).await.unwrap();
            } else {
                let digest = format!("{:x}", Sha256::digest(&bytes));
                f.objects.put(&f.key(&digest), bytes.into()).await.unwrap();
                head["current"]["digest"] = json!(digest);
                e.runtime()
                    .marker(
                        &format!("capture-schema:{STREAM}"),
                        head.to_string(),
                        f.journal.namespace().into(),
                    )
                    .await
                    .unwrap();
            }
        }
        drop(c);
        drop(e);
        let e = f.owner().await;
        let mut c = f.capture(e.runtime().state().await.epoch, saved).await;
        assert!(
            c.restore_schema(f.binding(e.runtime())).await.is_err(),
            "{change}"
        );
        assert!(c.schema_state().unknown_schema_job);
        let mut bytes = Bytes(VecDeque::new());
        let (_tx, mut paused) = watch::channel(false);
        assert!(
            c.step(&mut bytes, &mut Sink::default(), &mut paused)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn non_genesis_without_schema_reference_is_rejected() {
    let f = Fixture::new();
    let e = f.owner().await;
    let mut c = f
        .capture(e.runtime().state().await.epoch, WriterCheckpoint::default())
        .await;
    capture_record(&mut c, &internal(1), 20).await;
    let saved = c.writer().checkpoint().unwrap();
    drop(c);
    let mut c = f.capture(e.runtime().state().await.epoch, saved).await;
    assert!(c.restore_schema(f.binding(e.runtime())).await.is_err());
}

#[tokio::test]
async fn sticky_stop_and_unknown_flags_roundtrip_with_contract_validation() {
    let f = Fixture::new();
    let (e, mut c) = f.fresh().await;
    let bad = DdlRecord {
        origin: None,
        tag: "DROP TABLE".into(),
        query: "DROP TABLE public.a".into(),
        ..internal(1)
    };
    assert!(matches!(
        capture_record(&mut c, &bad, 20).await,
        Step::Durable { stop: Some(_), .. }
    ));
    let mut head = f.head().await;
    let mut data: Value = serde_json::from_slice(
        &f.objects
            .get(&f.key(head["current"]["digest"].as_str().unwrap()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    data["unknown"] = json!(true);
    let bytes = serde_json::to_vec(&data).unwrap();
    let digest = format!("{:x}", Sha256::digest(&bytes));
    f.objects.put(&f.key(&digest), bytes.into()).await.unwrap();
    head["current"]["digest"] = json!(digest);
    e.runtime()
        .marker(
            &format!("capture-schema:{STREAM}"),
            head.to_string(),
            f.journal.namespace().into(),
        )
        .await
        .unwrap();
    let saved = c.writer().checkpoint().unwrap();
    drop(c);
    drop(e);
    let e = f.owner().await;
    let mut c = f.capture(e.runtime().state().await.epoch, saved).await;
    c.restore_schema(f.binding(e.runtime())).await.unwrap();
    let stop = c.schema_state().blocked_boundary.unwrap();
    assert!(stop.capture_continues && stop.stop_apply_before_transaction && stop.readiness_blocked);
    assert!(c.schema_state().unknown_schema_job);
    let mut snapshot = serde_json::to_value(c.schema_snapshot()).unwrap();
    snapshot["blocker"]["remediation"] = json!("ignore");
    assert!(
        SchemaVersions::restore(INSTALLATION, serde_json::from_value(snapshot).unwrap()).is_err()
    );
}

#[tokio::test]
async fn oversized_snapshot_fails_before_writer_append_or_ack() {
    let f = Fixture::new();
    let e = f.owner().await;
    let mut c = f
        .capture(e.runtime().state().await.epoch, WriterCheckpoint::default())
        .await;
    let mut huge = descriptor(1);
    huge.relation["acl"] = json!("x".repeat(source::MAX_SCHEMA_SNAPSHOT_BYTES));
    let writer_checkpoint = c.writer().checkpoint().unwrap();
    drop(c);
    let (tx, _rx) = mpsc::channel();
    let writer = BufferWriter::open_from_checkpoint(
        f.buffer.clone(),
        Fixture::config(1),
        writer_checkpoint,
        Arc::new(Order),
        tx,
        f.root.join("scratch"),
    )
    .await
    .unwrap();
    c = Capture::new(
        source_id(),
        DOMAIN.into(),
        10,
        writer,
        resolve as Resolver,
        INSTALLATION.into(),
        SchemaVersions::new(INSTALLATION.into(), 0, [huge]).unwrap(),
        chunk::MESSAGE_PREFIX.into(),
    )
    .unwrap();
    assert!(c.restore_schema(f.binding(e.runtime())).await.is_err());
    assert_eq!(c.writer().checkpoint().unwrap().next_mseq, 0);
    assert_eq!(c.durable_lsn(), 10);
    assert_eq!(f.snapshots().await, 0);
}

#[tokio::test]
async fn chunk_rows_preserve_schema_state_at_the_equal_h_writer_slot() {
    let f = Fixture::new();
    let (e, mut c) = f.fresh().await;
    capture_record(&mut c, &alter(), 20).await;
    let chunk = AssembledTxn::new(
        source_id(),
        TxnId(b"chunk".to_vec()),
        Position::new(DOMAIN, 20_u64.to_be_bytes().to_vec()),
        None,
        vec![],
    )
    .unwrap();
    c.append_chunk_rows(20, chunk).await.unwrap();
    let saved = c.writer().checkpoint().unwrap();
    assert!(saved.last_record.as_ref().unwrap().chunk_rows);
    drop(c);
    drop(e);
    let e = f.owner().await;
    let mut c = f.capture(e.runtime().state().await.epoch, saved).await;
    c.restore_schema(f.binding(e.runtime())).await.unwrap();
    assert_eq!(versions(&c).table(1).unwrap().version, 1);
    assert_eq!(c.durable_lsn(), 21);
    assert!(matches!(
        capture_record(&mut c, &internal(2), 30).await,
        Step::Durable { stop: None, .. }
    ));
}

#[test]
fn schema_snapshot_rejects_duplicate_tables_and_future_versions() {
    let mut schemas = baseline();
    assert!(matches!(
        schemas.classify_transaction(&[alter()]),
        Classification::Supported { .. }
    ));
    let original = serde_json::to_value(schemas.snapshot()).unwrap();
    for field in ["duplicate", "future", "installation", "format"] {
        let mut value = original.clone();
        match field {
            "duplicate" => {
                let first = value["tables"][0].clone();
                value["tables"].as_array_mut().unwrap().push(first);
            }
            "future" => value["tables"][0]["version"] = json!(2),
            "installation" => value["installation"] = json!("foreign"),
            "format" => value["version"] = json!(2),
            _ => unreachable!(),
        }
        assert!(
            SchemaVersions::restore(INSTALLATION, serde_json::from_value(value).unwrap()).is_err()
        );
    }
}

// Protocol fixtures transport bytes only; publication, journal and snapshots use
// real local durable backends throughout these regressions.
async fn dml(c: &mut Capture<Resolver>, lsn: u64) {
    let mut wire = messages(&internal(1), lsn);
    let mut relation = vec![b'R'];
    relation.extend(1_u32.to_be_bytes());
    relation.extend(b"public\0a\0d");
    relation.extend(1_u16.to_be_bytes());
    relation.extend(b"\x01id\0");
    relation.extend(23_u32.to_be_bytes());
    relation.extend((-1_i32).to_be_bytes());
    let mut insert = vec![b'I'];
    insert.extend(1_u32.to_be_bytes());
    insert.push(b'N');
    insert.extend(1_u16.to_be_bytes());
    insert.push(b't');
    insert.extend(1_u32.to_be_bytes());
    insert.push(b'7');
    wire[1] = wal(insert);
    wire.push_front(wal(relation));
    let mut bytes = Bytes(wire);
    let (_tx, mut paused) = watch::channel(false);
    for _ in 0..3 {
        assert_eq!(
            c.step(&mut bytes, &mut Sink::default(), &mut paused)
                .await
                .unwrap(),
            Step::Message
        );
    }
    assert!(matches!(
        c.step(&mut bytes, &mut Sink::default(), &mut paused)
            .await
            .unwrap(),
        Step::Durable { stop: None, .. }
    ));
}

#[tokio::test]
async fn thousand_dml_and_chunk_transactions_do_no_schema_io_and_restore_older_ddl() {
    let f = Fixture::new();
    let (e, mut c) = f.fresh().await;
    let counts = f.objects.counts.clone();
    counts.reset();
    for i in 0..500 {
        dml(&mut c, 20 + 10 * i).await;
    }
    assert_eq!(counts.values(), [0, 0, 0, 0]);
    capture_record(&mut c, &alter(), 5020).await;
    let [operations, prepares, promotes, creates] = counts.values();
    assert!(operations > 0);
    assert_eq!([prepares, promotes, creates], [1, 1, 1]);
    counts.reset();
    for i in 0..500 {
        dml(&mut c, 5030 + 10 * i).await;
    }
    let chunk = AssembledTxn::new(
        source_id(),
        TxnId(b"counted-chunk".to_vec()),
        Position::new(DOMAIN, 10020_u64.to_be_bytes().to_vec()),
        None,
        vec![],
    )
    .unwrap();
    c.append_chunk_rows(10020, chunk).await.unwrap();
    assert_eq!(counts.values(), [0, 0, 0, 0]);
    let saved = c.writer().checkpoint().unwrap();
    drop(c);
    drop(e);
    let e = f.owner().await;
    let mut c = f.capture(e.runtime().state().await.epoch, saved).await;
    c.restore_schema(f.binding(e.runtime())).await.unwrap();
    assert_eq!(versions(&c).table(1).unwrap().version, 1);
    assert_eq!(versions(&c).last_version(), 1);
    assert_eq!(c.durable_lsn(), 10020);
    assert_eq!(f.snapshots().await, 1);
    capture_record(&mut c, &internal(2), 10030).await;
    assert_eq!(versions(&c).last_version(), 2);
}

#[tokio::test]
async fn prepared_ddl_is_recovered_even_below_later_dml_boundary() {
    let f = Fixture::new();
    let (e, mut c) = f.fresh().await;
    dml(&mut c, 20).await;
    let mut bytes = Bytes(messages(&alter(), 30));
    let (_tx, mut paused) = watch::channel(false);
    let mut sink = Sink::default();
    prefix(&mut c, &mut bytes, &mut sink, &mut paused).await;
    f.objects.arm(3);
    tokio::select! {
        _ = f.objects.entered.notified() => (),
        result = c.step(&mut bytes, &mut sink, &mut paused) => panic!("expected cancellation, {result:?}"),
    }
    let saved = c.writer().checkpoint().unwrap();
    drop(c);
    // Deliberately advance the real writer without schema-journal promotion to
    // exercise recovery from a prepared transaction strictly below the boundary.
    let mut c = f.capture(e.runtime().state().await.epoch, saved).await;
    for i in 0..10 {
        dml(&mut c, 40 + 10 * i).await;
    }
    let saved = c.writer().checkpoint().unwrap();
    drop(c);
    drop(e);
    let e = f.owner().await;
    let mut c = f
        .capture(e.runtime().state().await.epoch, saved.clone())
        .await;
    c.restore_schema(f.binding(e.runtime())).await.unwrap();
    assert_eq!(versions(&c).last_version(), 1);
    assert_eq!(c.durable_lsn(), 130);
    assert!(f.head().await["prepared"].is_null());
    drop(c);
    // A second restore authenticates the promoted coverage boundary, even
    // though the snapshot's own DDL transaction was earlier.
    let mut c = f.capture(e.runtime().state().await.epoch, saved).await;
    c.restore_schema(f.binding(e.runtime())).await.unwrap();
    capture_record(&mut c, &internal(2), 140).await;
}

#[tokio::test]
async fn unappended_prepare_after_dml_is_discarded_without_snapshot_object() {
    for mode in [1, 2] {
        let f = Fixture::new();
        let (e, mut c) = f.fresh().await;
        dml(&mut c, 20).await;
        let mut bytes = Bytes(messages(&alter(), 30));
        let mut sink = Sink::default();
        let (_tx, mut paused) = watch::channel(false);
        prefix(&mut c, &mut bytes, &mut sink, &mut paused).await;
        f.objects.arm(mode);
        tokio::select! {
            _ = f.objects.entered.notified() => (),
            result = c.step(&mut bytes, &mut sink, &mut paused) => panic!("expected cancellation, {result:?}"),
        }
        let saved = c.writer().checkpoint().unwrap();
        drop(c);
        drop(e);
        let e = f.owner().await;
        let mut c = f.capture(e.runtime().state().await.epoch, saved).await;
        c.restore_schema(f.binding(e.runtime())).await.unwrap();
        assert_eq!(versions(&c).last_version(), 0);
        assert!(f.head().await["prepared"].is_null());
        assert_eq!(f.snapshots().await, 1);
        capture_record(&mut c, &alter(), 30).await;
    }
}

#[tokio::test]
async fn unchanged_sticky_stop_does_not_prepare_again() {
    let f = Fixture::new();
    let (_e, mut c) = f.fresh().await;
    let bad = DdlRecord {
        origin: None,
        tag: "DROP TABLE".into(),
        query: "DROP TABLE public.a".into(),
        ..internal(1)
    };
    assert!(matches!(
        capture_record(&mut c, &bad, 20).await,
        Step::Durable { stop: Some(_), .. }
    ));
    f.objects.counts.reset();
    assert!(matches!(
        capture_record(&mut c, &internal(2), 30).await,
        Step::Durable { stop: Some(_), .. }
    ));
    assert_eq!(f.objects.counts.values(), [0, 0, 0, 0]);
}

#[tokio::test]
async fn snapshotless_chunk_history_requires_complete_bound_evidence() {
    for change in ["none", "pruned", "foreign"] {
        let f = Fixture::new();
        let e = f.owner().await;
        let (tx, _rx) = mpsc::channel();
        let mut writer = BufferWriter::open_from_checkpoint(
            f.buffer.clone(),
            Fixture::config(1),
            WriterCheckpoint::default(),
            Arc::new(Order),
            tx,
            f.root.join("scratch"),
        )
        .await
        .unwrap();
        for lsn in [20_u64, 30] {
            let mut source = source_id();
            if change == "foreign" {
                source.system_id = "other".into();
            }
            let txn = AssembledTxn::new(
                source,
                TxnId(lsn.to_be_bytes().to_vec()),
                Position::new(DOMAIN, lsn.to_be_bytes().to_vec()),
                None,
                vec![],
            )
            .unwrap();
            writer
                .append_chunk_rows(txn, std::time::Instant::now())
                .await
                .unwrap();
        }
        let mut saved = writer.checkpoint().unwrap();
        drop(writer);
        if change == "pruned" {
            saved
                .redelivery
                .retain(|r| r.position == Position::new(DOMAIN, 30_u64.to_be_bytes().to_vec()));
            saved.redelivery_from = Some(Position::new(DOMAIN, 30_u64.to_be_bytes().to_vec()));
        }
        let mut c = f.capture(1, saved).await;
        let result = c.restore_schema(f.binding(e.runtime())).await;
        if change == "none" {
            result.unwrap();
            assert_eq!(versions(&c).last_version(), 0);
            assert_eq!(f.snapshots().await, 0);
            capture_record(&mut c, &internal(1), 40).await;
            assert_eq!(f.snapshots().await, 1);
        } else {
            assert!(result.is_err(), "{change}");
            assert!(c.schema_state().unknown_schema_job);
        }
    }
}

#[tokio::test]
async fn durable_prepared_snapshot_requires_object_and_retained_transaction_proof() {
    for change in ["missing", "pruned", "mismatch"] {
        let f = Fixture::new();
        let (e, mut c) = f.fresh().await;
        let mut bytes = Bytes(messages(&alter(), 20));
        let mut sink = Sink::default();
        let (_tx, mut paused) = watch::channel(false);
        prefix(&mut c, &mut bytes, &mut sink, &mut paused).await;
        f.objects.arm(3);
        tokio::select! {
            _ = f.objects.entered.notified() => (),
            result = c.step(&mut bytes, &mut sink, &mut paused) => panic!("expected cancellation, {result:?}"),
        }
        let mut saved = c.writer().checkpoint().unwrap();
        drop(c);
        let mut c = f.capture(1, saved).await;
        dml(&mut c, 30).await;
        saved = c.writer().checkpoint().unwrap();
        drop(c);
        match change {
            "missing" => {
                let head = f.head().await;
                f.objects
                    .delete(&f.key(head["prepared"]["digest"].as_str().unwrap()))
                    .await
                    .unwrap();
            }
            "pruned" => {
                saved
                    .redelivery
                    .retain(|r| r.position == Position::new(DOMAIN, 30_u64.to_be_bytes().to_vec()));
                saved.redelivery_from = Some(Position::new(DOMAIN, 30_u64.to_be_bytes().to_vec()));
            }
            "mismatch" => {
                saved
                    .redelivery
                    .iter_mut()
                    .find(|r| r.position == Position::new(DOMAIN, 20_u64.to_be_bytes().to_vec()))
                    .unwrap()
                    .digest = [42; 32];
            }
            _ => unreachable!(),
        }
        let mut c = f.capture(1, saved).await;
        assert!(
            c.restore_schema(f.binding(e.runtime())).await.is_err(),
            "{change}"
        );
        assert!(c.schema_state().unknown_schema_job);
    }
}

#[tokio::test]
async fn cancelled_promotion_can_publish_the_next_schema_change() {
    for mode in [4, 8] {
        let f = Fixture::new();
        let (_e, mut c) = f.fresh().await;
        let mut bytes = Bytes(messages(&alter(), 20));
        let mut sink = Sink::default();
        let (_tx, mut paused) = watch::channel(false);
        prefix(&mut c, &mut bytes, &mut sink, &mut paused).await;
        f.objects.arm(mode);
        tokio::select! {
            _ = f.objects.entered.notified() => (),
            result = c.step(&mut bytes, &mut sink, &mut paused) => panic!("expected cancellation, {result:?}"),
        }
        assert!(matches!(
            c.step(&mut bytes, &mut sink, &mut paused).await.unwrap(),
            Step::Durable { stop: None, .. }
        ));
        capture_record(&mut c, &internal(2), 30).await;
        let head = f.head().await;
        let object = f
            .objects
            .get(&f.key(head["current"]["digest"].as_str().unwrap()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let snapshot: Value = serde_json::from_slice(&object).unwrap();
        assert_eq!(snapshot["schemas"]["last_version"], json!(2));
        assert_eq!(f.snapshots().await, 1);
    }
}

#[tokio::test]
async fn prepared_predecessor_ahead_of_recovered_writer_fails_closed() {
    let f = Fixture::new();
    let (e, mut c) = f.fresh().await;
    dml(&mut c, 20).await;
    let mut bytes = Bytes(messages(&alter(), 30));
    let mut sink = Sink::default();
    let (_tx, mut paused) = watch::channel(false);
    prefix(&mut c, &mut bytes, &mut sink, &mut paused).await;
    f.objects.arm(2);
    tokio::select! {
        _ = f.objects.entered.notified() => (),
        result = c.step(&mut bytes, &mut sink, &mut paused) => panic!("expected cancellation, {result:?}"),
    }
    drop(c);
    // Corrupt the real backend by losing the durable DML manifest. The journal's
    // prepared predecessor still proves that boundary previously existed.
    f.buffer
        .delete(&reader_config().manifest_key(0))
        .await
        .unwrap();
    let mut c = f.capture(1, WriterCheckpoint::default()).await;
    assert!(c.restore_schema(f.binding(e.runtime())).await.is_err());
    assert!(c.schema_state().unknown_schema_job);
}
