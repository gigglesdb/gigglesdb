use super::*;
use crate::{
    buffer::{
        backends::{BufferStore, ObjectBufferStore},
        format,
        reader::{BufferReader, ReaderCheckpoint, ReaderConfig},
        writer::WriterConfig,
    },
    event::{EventError, Position, PositionOrder},
    mapping::{PgType, Value},
    pg::{
        frame::FrameBuffer,
        pgoutput::{Relation, RelationColumn, StandbyStatusUpdate},
    },
};
use std::{
    cmp::Ordering,
    path::PathBuf,
    sync::{Arc, mpsc},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

struct Order;
impl PositionOrder for Order {
    fn domain(&self) -> &str {
        "pg-test"
    }
    fn compare(&self, a: &[u8], b: &[u8]) -> std::result::Result<Ordering, EventError> {
        Ok(a.cmp(b))
    }
}
type Resolver = fn(&Relation<'_>, &RelationColumn<'_>) -> Result<ColumnMapping>;
fn resolve(_: &Relation<'_>, column: &RelationColumn<'_>) -> Result<ColumnMapping> {
    Ok(ColumnMapping::identity(match column.type_oid {
        23 => PgType::Int4,
        25 => PgType::Text {
            encoding: "UTF8".into(),
        },
        _ => return Err(Error::RecoveryRequired("unknown test catalog type")),
    }))
}
fn root() -> PathBuf {
    let parent = std::env::var_os("SKEDADDLE_TEST_ROOT")
        .expect("source tests require SKEDADDLE_TEST_ROOT under the task folder");
    let path = PathBuf::from(parent).join(format!("pg-source-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(path.join("store")).unwrap();
    std::fs::create_dir_all(path.join("scratch")).unwrap();
    path
}
fn writer_config() -> WriterConfig {
    WriterConfig {
        prefix: "test".into(),
        migration_id: "source".into(),
        stream: "wal".into(),
        epoch: 1,
        segment_bytes: u64::MAX,
        segment_transactions: u64::MAX,
        segment_age: Duration::from_secs(60),
        max_transaction_bytes: writer::MAX_TRANSACTION_BYTES,
    }
}
async fn setup() -> (Capture<Resolver>, Arc<ObjectBufferStore>, PathBuf) {
    let root = root();
    let store =
        Arc::new(ObjectBufferStore::local(root.join("store"), Duration::from_secs(5)).unwrap());
    let (tx, _) = mpsc::channel();
    let writer = BufferWriter::open_with_scratch(
        store.clone(),
        writer_config(),
        Arc::new(Order),
        tx,
        root.join("scratch"),
    )
    .await
    .unwrap();
    let source = SourceIdentity {
        engine: "postgres".into(),
        system_id: "1".into(),
        lineage: "1".into(),
        db_name: "db".into(),
        stream: "slot".into(),
    };
    let capture = Capture::new(
        source,
        "pg-test".into(),
        10,
        writer,
        resolve as Resolver,
        "p5e".into(),
        SchemaVersions::new("p5e".into(), 0, []).unwrap(),
        chunk::MESSAGE_PREFIX.into(),
    )
    .unwrap();
    (capture, store, root)
}
async fn read_all(store: Arc<ObjectBufferStore>, count: u64) -> Vec<AssembledTxn> {
    let mut reader = BufferReader::new(
        store,
        ReaderConfig {
            prefix: "test".into(),
            migration_id: "source".into(),
            stream: "wal".into(),
            limits: format::Limits::default(),
            required_next_mseq: count,
        },
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    let mut txns = Vec::new();
    while let Some(txn) = reader.next_txn().await.unwrap() {
        txns.push(txn);
    }
    txns
}

// This peer has no SQL, catalog, slot, authentication or database semantics.
// It transports protocol bytes through Tokio's actual in-process duplex I/O.
struct BytesTransport {
    io: DuplexStream,
    frames: FrameBuffer,
    reads: usize,
}
impl Transport for BytesTransport {
    async fn next(&mut self, durable_lsn: u64) -> Result<Vec<u8>> {
        let feedback = StandbyStatusUpdate {
            write_lsn: durable_lsn,
            flush_lsn: durable_lsn,
            apply_lsn: durable_lsn,
            client_time: 0,
            reply_requested: false,
        }
        .encode_copy_data(durable_lsn)?;
        self.io
            .write_all(&feedback)
            .await
            .map_err(ReplicationError::from)?;
        loop {
            if let Some(frame) = self.frames.next_frame()? {
                assert_eq!(frame.tag, b'd');
                self.reads += 1;
                return Ok(frame.payload);
            }
            let byte = self.io.read_u8().await.map_err(ReplicationError::from)?;
            self.frames.push(&[byte])?;
        }
    }
}
fn transport(messages: Vec<Vec<u8>>) -> (BytesTransport, tokio::task::JoinHandle<Vec<u64>>) {
    let (io, mut peer) = tokio::io::duplex(128);
    let task = tokio::spawn(async move {
        let mut acks = Vec::new();
        for payload in messages {
            let mut ack = [0; 39];
            peer.read_exact(&mut ack).await.unwrap();
            assert_eq!(&ack[..6], &[b'd', 0, 0, 0, 38, b'r']);
            acks.push(u64::from_be_bytes(ack[14..22].try_into().unwrap()));
            let mut frame = vec![b'd'];
            frame.extend(((payload.len() + 4) as u32).to_be_bytes());
            frame.extend(payload);
            for fragment in frame.chunks(3) {
                peer.write_all(fragment).await.unwrap();
            }
        }
        acks
    });
    (
        BytesTransport {
            io,
            frames: FrameBuffer::default(),
            reads: 0,
        },
        task,
    )
}
fn wal(message: Vec<u8>) -> Vec<u8> {
    let mut p = vec![b'w'];
    p.extend(999_u64.to_be_bytes());
    p.extend(9999_u64.to_be_bytes());
    p.extend(0_i64.to_be_bytes());
    p.extend(message);
    p
}
fn keepalive() -> Vec<u8> {
    let mut p = vec![b'k'];
    p.extend(u64::MAX.to_be_bytes());
    p.extend(0_i64.to_be_bytes());
    p.push(1);
    p
}
fn begin(lsn: u64) -> Vec<u8> {
    let mut p = vec![b'B'];
    p.extend(lsn.to_be_bytes());
    p.extend(0_i64.to_be_bytes());
    p.extend(1_u32.to_be_bytes());
    wal(p)
}
fn commit(lsn: u64) -> Vec<u8> {
    let mut p = vec![b'C', 0];
    p.extend(lsn.to_be_bytes());
    p.extend((lsn + 1).to_be_bytes());
    p.extend(0_i64.to_be_bytes());
    wal(p)
}
fn relation(id: u32, name: &str) -> Vec<u8> {
    let mut p = vec![b'R'];
    p.extend(id.to_be_bytes());
    p.extend(b"public\0");
    p.extend(name.as_bytes());
    p.extend([0, b'd']);
    p.extend(2_u16.to_be_bytes());
    for (key, name, oid) in [(1, "id", 23_u32), (0, "value", 25)] {
        p.push(key);
        p.extend(name.as_bytes());
        p.push(0);
        p.extend(oid.to_be_bytes());
        p.extend((-1_i32).to_be_bytes());
    }
    wal(p)
}
fn tuple(p: &mut Vec<u8>, id: &[u8], value: Option<&[u8]>, toast: bool) {
    p.extend(2_u16.to_be_bytes());
    p.push(b't');
    p.extend((id.len() as u32).to_be_bytes());
    p.extend(id);
    if toast {
        p.push(b'u');
    } else if let Some(value) = value {
        p.push(b't');
        p.extend((value.len() as u32).to_be_bytes());
        p.extend(value);
    } else {
        p.push(b'n');
    }
}
fn insert(id: u32) -> Vec<u8> {
    let mut p = vec![b'I'];
    p.extend(id.to_be_bytes());
    p.push(b'N');
    tuple(&mut p, b"7", Some(b"value"), false);
    wal(p)
}
fn logical(transactional: bool, prefix: &str, content: &[u8]) -> Vec<u8> {
    let mut p = vec![b'M', u8::from(transactional)];
    p.extend(15_u64.to_be_bytes());
    p.extend(prefix.as_bytes());
    p.push(0);
    p.extend((content.len() as u32).to_be_bytes());
    p.extend(content);
    wal(p)
}
async fn drive(c: &mut Capture<Resolver>, s: &mut BytesTransport, count: usize) -> Vec<Step> {
    let (_tx, mut paused) = watch::channel(false);
    let mut steps = Vec::new();
    for _ in 0..count {
        steps.push(c.step(s, &mut |_| Ok(()), &mut paused).await.unwrap());
    }
    steps
}

#[tokio::test]
async fn durable_feedback_ignores_wal_and_keepalives() {
    let (mut c, store, _) = setup().await;
    let (mut s, peer) = transport(vec![
        keepalive(),
        relation(1, "a"),
        begin(20),
        insert(1),
        commit(20),
        keepalive(),
    ]);
    let steps = drive(&mut c, &mut s, 6).await;
    assert_eq!(
        steps[4],
        Step::Durable {
            end_lsn: 21,
            stop: None
        }
    );
    assert_eq!(peer.await.unwrap(), vec![10, 10, 10, 10, 10, 21]);
    assert_eq!(c.durable_lsn(), 21);
    let txns = read_all(store, 1).await;
    let value = &txns[0].events[0].after.as_ref().unwrap()["id"];
    assert!(value.type_id.starts_with("skva.v1:"));
    assert_eq!(
        Value::from_canonical_bytes(value.bytes.as_ref().unwrap()).unwrap(),
        Value::Int4(7)
    );
}

#[tokio::test]
async fn backpressure_and_cancellation_leave_transport_unread_and_lose_nothing() {
    let (mut c, store, _) = setup().await;
    let (mut s, peer) = transport(vec![
        relation(1, "a"),
        begin(20),
        insert(1),
        commit(20),
        begin(30),
        insert(1),
        commit(30),
    ]);
    drive(&mut c, &mut s, 4).await;
    let (gate, mut paused) = watch::channel(true);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(30),
            c.step(&mut s, &mut |_| Ok(()), &mut paused)
        )
        .await
        .is_err()
    );
    assert_eq!(s.reads, 4);
    assert_eq!(c.durable_lsn(), 21);
    gate.send(false).unwrap();
    for _ in 0..3 {
        c.step(&mut s, &mut |_| Ok(()), &mut paused).await.unwrap();
    }
    assert_eq!(peer.await.unwrap(), vec![10, 10, 10, 10, 21, 21, 21]);
    assert_eq!(read_all(store, 2).await.len(), 2);
}

#[tokio::test]
async fn ddl_stop_is_durable_and_capture_continues_with_watermark_routing() {
    let (mut c, store, _) = setup().await;
    let record = DdlRecord {
        origin: None,
        format: 1,
        installation: "p5e".into(),
        server_major: 17,
        version: 1,
        tag: "DROP TABLE".into(),
        xid: "1".into(),
        query: "DROP TABLE a".into(),
        commands: Vec::new(),
    };
    let bytes = ddl::encode(&record).unwrap();
    let (mut s, peer) = transport(vec![
        logical(false, chunk::MESSAGE_PREFIX, b"low"),
        begin(20),
        logical(true, "skedaddle", &bytes),
        commit(20),
        begin(30),
        logical(true, chunk::MESSAGE_PREFIX, b"high"),
        commit(30),
    ]);
    let (_gate, mut paused) = watch::channel(false);
    let mut marks = Vec::new();
    let mut outcomes = Vec::new();
    for index in 0..7 {
        outcomes.push(
            c.step(
                &mut s,
                &mut |m| {
                    marks.push(m);
                    Ok(())
                },
                &mut paused,
            )
            .await
            .unwrap(),
        );
        assert_eq!(marks.len(), usize::from(index == 6));
    }
    assert!(matches!(
        &outcomes[3],
        Step::Durable {
            stop: Some(SafeBoundaryStop {
                capture_continues: true,
                ..
            }),
            ..
        }
    ));
    assert_eq!(
        marks.iter().map(|m| m.content.clone()).collect::<Vec<_>>(),
        vec![b"high".to_vec()]
    );
    assert!(marks[0].transactional);
    assert_eq!(marks[0].commit_lsn, 30);
    assert_eq!(peer.await.unwrap(), vec![10, 10, 10, 10, 21, 21, 21]);
    let txns = read_all(store, 2).await;
    assert_eq!(txns[0].events.len(), 2);
    let stop = &txns[0].events[1].after.as_ref().unwrap()["control"];
    assert_eq!(stop.type_id, "skedaddle.stop.v1");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(stop.bytes.as_ref().unwrap()).unwrap()["kind"],
        "SafeBoundaryStop"
    );
    assert_eq!(txns[1].events[0].kind, EventKind::Watermark);
    assert_eq!(marks[0].committed_events, txns[1].events);
}

fn internal_record(version: u64) -> DdlRecord {
    DdlRecord {
        format: 1,
        installation: "p5e".into(),
        origin: Some("p5e".into()),
        server_major: 18,
        version,
        tag: "REVOKE".into(),
        xid: "1".into(),
        query: "REVOKE INSERT ON public.a FROM app".into(),
        commands: vec![],
    }
}

fn truncate(ids: &[u32]) -> Vec<u8> {
    let mut bytes = vec![b'T'];
    bytes.extend((ids.len() as u32).to_be_bytes());
    bytes.push(0);
    for id in ids {
        bytes.extend(id.to_be_bytes());
    }
    wal(bytes)
}

fn apply_tables(txn: &AssembledTxn, version: u64) -> Vec<crate::pg::apply::Table> {
    use crate::pg::apply;
    let event = txn
        .events
        .iter()
        .find(|e| e.kind == EventKind::Insert)
        .unwrap();
    vec![apply::Table {
        event_table: "\"public\".\"a\"".into(),
        schema: "public".into(),
        name: "a".into(),
        schema_version: version,
        key: vec!["id".into()],
        columns: event
            .after
            .as_ref()
            .unwrap()
            .iter()
            .map(|(name, value)| apply::Column {
                name: name.clone(),
                type_id: value.type_id.clone(),
                pg_type: if name == "id" {
                    PgType::Int4
                } else {
                    PgType::Text {
                        encoding: "UTF8".into(),
                    }
                },
                wire_type: if name == "id" {
                    tokio_postgres::types::Type::INT4
                } else {
                    tokio_postgres::types::Type::TEXT
                },
            })
            .collect(),
    }]
}

fn assert_plans_unchanged(txns: &[AssembledTxn], version: u64) {
    use crate::pg::apply;
    let tables = apply_tables(&txns[0], version);
    for (mseq, txn) in txns.iter().enumerate() {
        let plan = apply::plan_transaction_with_ddl(
            txn,
            &tables,
            &apply::PlanContext {
                stream: "wal".into(),
                mseq: mseq as u64,
                origin: "skedaddle_p5e".into(),
                synthetic_commit_time: None,
            },
            "p5e",
            |_, _| Ok(apply::RangeState::Completed),
        )
        .unwrap();
        assert_eq!(plan.progress.digest, txn.digest);
        assert_eq!(plan.progress.txn, txn.txn);
        for event in &txn.events {
            if matches!(event.kind, EventKind::Insert | EventKind::Truncate { .. }) {
                assert_eq!(event.schema_version, version);
            }
        }
        assert!(
            plan.statements
                .iter()
                .any(|s| s.sql.starts_with("TRUNCATE TABLE"))
        );
    }
}

#[tokio::test]
async fn internal_ddl_buffer_roundtrip_keeps_receipt_and_same_and_later_row_versions() {
    let (mut c, store, _) = setup().await;
    let bytes = ddl::encode(&internal_record(1)).unwrap();
    let messages = vec![
        relation(1, "a"),
        begin(20),
        logical(true, ddl::MESSAGE_PREFIX, &bytes),
        insert(1),
        truncate(&[1]),
        commit(20),
        begin(30),
        insert(1),
        truncate(&[1]),
        commit(30),
    ];
    let (mut stream, peer) = transport(messages);
    let mut steps = drive(&mut c, &mut stream, 6).await;
    let captured_first = c.boundary.as_ref().unwrap().clone();
    steps.extend(drive(&mut c, &mut stream, 4).await);
    peer.await.unwrap();
    assert!(
        steps
            .iter()
            .all(|s| !matches!(s, Step::Durable { stop: Some(_), .. }))
    );
    assert_eq!(c.schemas.last_version(), 1);
    let captured = c.boundary.as_ref().unwrap().clone();
    let txns = read_all(store, 2).await;
    assert_eq!(txns[0], captured_first);
    assert_eq!(txns[1], captured); // byte/digest/ordinal identity retained by BufferReader
    assert_eq!(txns[0].events[0].kind, EventKind::DdlBoundary);
    assert_eq!(
        txns[0].events[0].after.as_ref().unwrap()["control"]
            .bytes
            .as_deref(),
        Some(bytes.as_slice())
    );
    assert_plans_unchanged(&txns, 0);
}

fn retained_descriptor() -> ddl::TableDescriptor {
    let attributes = |oid, storage, collation| {
        serde_json::json!({"type_oid":oid,"typmod":-1,"collation":collation,"acl":null,"storage":storage,
        "compression":"","options":null,"fdwoptions":null,"local":true,"inhcount":0})
    };
    let column = |name: &str, attnum, type_name: &str, attributes| ddl::Column {
        attnum,
        name: name.into(),
        type_name: type_name.into(),
        notnull: false,
        dropped: false,
        identity: String::new(),
        generated: String::new(),
        missing: false,
        missing_value: None,
        default: None,
        attributes,
    };
    ddl::TableDescriptor {
        oid: 1,
        schema: "public".into(),
        columns: vec![
            column("id", 1, "integer", attributes(23, "p", 0)),
            column("value", 2, "text", attributes(25, "x", 100)),
        ],
        relation: serde_json::json!({"kind":"r","partition":false,"filenode":1,"name":"a","namespace":2200,
            "owner":10,"acl":null,"options":null,"replica_identity":"d","tablespace":0,
            "persistence":"p","access_method":2,"row_security":false,"force_row_security":false,
            "partition_key":null,"partition_bound":null,"constraints":[],"indexes":[],"triggers":[],"policies":[],"parents":[]}),
    }
}

#[tokio::test]
async fn application_alter_advances_only_its_table_and_recovery_uses_retained_versions() {
    let (mut c, store, _) = setup().await;
    let before = retained_descriptor();
    c.schemas = SchemaVersions::new("p5e".into(), 0, [before.clone()]).unwrap();
    let mut after = before.clone();
    let mut added = after.columns[1].clone();
    added.attnum = 3;
    added.name = "extra".into();
    after.columns.push(added);
    let mut record = internal_record(1);
    record.origin = None;
    record.tag = "ALTER TABLE".into();
    record.query = "ALTER TABLE public.a ADD COLUMN extra text".into();
    record.commands.push(ddl::DdlCommand {
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
    });
    let bytes = ddl::encode(&record).unwrap();
    // Fresh pgoutput relation and tuple include the added column after ALTER.
    let mut new_relation = relation(1, "a");
    // Header size is 25 bytes; locate the old column count after R/id/schema/name/identity.
    let count_offset = 25 + 1 + 4 + b"public\0".len() + b"a\0".len() + 1;
    new_relation[count_offset..count_offset + 2].copy_from_slice(&3_u16.to_be_bytes());
    new_relation.push(0);
    new_relation.extend(b"extra\0");
    new_relation.extend(25_u32.to_be_bytes());
    new_relation.extend((-1_i32).to_be_bytes());
    let mut new_insert = insert(1);
    let tuple_offset = 25 + 1 + 4 + 1;
    new_insert[tuple_offset..tuple_offset + 2].copy_from_slice(&3_u16.to_be_bytes());
    new_insert.push(b'n');
    let admin = ddl::encode(&internal_record(2)).unwrap();
    let messages = vec![
        relation(1, "a"),
        relation(2, "b"),
        begin(20),
        logical(true, ddl::MESSAGE_PREFIX, &bytes),
        new_relation.clone(),
        new_insert.clone(),
        insert(2),
        commit(20),
        begin(30),
        logical(true, ddl::MESSAGE_PREFIX, &admin),
        new_insert.clone(),
        truncate(&[1]),
        commit(30),
    ];
    let count = messages.len();
    let (mut stream, peer) = transport(messages);
    let steps = drive(&mut c, &mut stream, count).await;
    peer.await.unwrap();
    assert!(
        steps
            .iter()
            .all(|s| !matches!(s, Step::Durable { stop: Some(_), .. }))
    );
    assert_eq!(c.schemas.table(1).unwrap().version, 1);
    assert_eq!(c.schemas.last_version(), 2);
    let txns = read_all(store.clone(), 2).await;
    assert_eq!(txns[0].events[1].schema_version, 1);
    assert_eq!(txns[0].events[2].schema_version, 0); // unrelated table
    assert_plans_unchanged(&txns[1..], 1);
    // Recover with global version 2 but retained mapped descriptor version 1.
    let source = txns[0].source.clone();
    let schemas = c.schemas.clone();
    let mut recovered = Capture::new(
        source,
        "pg-test".into(),
        c.durable_lsn,
        c.writer,
        resolve as Resolver,
        "p5e".into(),
        schemas,
        chunk::MESSAGE_PREFIX.into(),
    )
    .unwrap();
    let admin = ddl::encode(&internal_record(3)).unwrap();
    let messages = vec![
        new_relation,
        begin(40),
        logical(true, ddl::MESSAGE_PREFIX, &admin),
        new_insert,
        truncate(&[1]),
        commit(40),
    ];
    let count = messages.len();
    let (mut stream, peer) = transport(messages);
    drive(&mut recovered, &mut stream, count).await;
    peer.await.unwrap();
    assert_eq!(recovered.schemas.last_version(), 3);
    let txns = read_all(store, 3).await;
    assert_plans_unchanged(&txns[1..], 1);
}

#[test]
fn grouped_truncate_keeps_all_version_checks() {
    let mut mapper = RelationMapper::new(resolve as Resolver, 0);
    let decoder = Decoder::default();
    for (id, name) in [(1, "a"), (2, "b")] {
        let bytes = relation(id, name);
        let CopyData::XLogData { data, .. } = decoder.decode_copy_data(&bytes).unwrap() else {
            panic!()
        };
        let Message::Relation(relation) = decoder.decode(data).unwrap() else {
            panic!()
        };
        mapper.relation(&relation).unwrap();
    }
    mapper.set_mapped_versions([(1, 1), (2, 2)]);
    assert!(matches!(
        mapper.map(&Message::Truncate {
            cascade: false,
            restart_identity: false,
            relation_ids: vec![1, 2]
        }),
        Err(Error::RecoveryRequired(
            "grouped truncate has different mapped versions"
        ))
    ));
}

#[tokio::test]
async fn watermark_is_provisional_until_its_manifest_is_durable() {
    let (mut c, _store, root) = setup().await;
    let obstruction = root
        .join("store")
        .join(format::manifest_key("test", "source", "wal", 0));
    std::fs::create_dir_all(&obstruction).unwrap();
    let (mut stream, peer) = transport(vec![
        relation(1, "a"),
        begin(20),
        insert(1),
        logical(true, chunk::MESSAGE_PREFIX, b"marker"),
        commit(20),
    ]);
    let (_gate, mut paused) = watch::channel(false);
    let mut marks = Vec::new();
    for _ in 0..4 {
        c.step(
            &mut stream,
            &mut |m| {
                marks.push(m);
                Ok(())
            },
            &mut paused,
        )
        .await
        .unwrap();
        assert!(marks.is_empty());
    }
    assert!(
        c.step(
            &mut stream,
            &mut |m| {
                marks.push(m);
                Ok(())
            },
            &mut paused
        )
        .await
        .is_err()
    );
    assert!(marks.is_empty());
    assert_eq!(c.durable_lsn(), 10);
    assert!(c.writer().checkpoint().is_err());
    peer.await.unwrap();
}

#[tokio::test]
async fn committed_sink_reports_exact_durable_events_in_transaction_order() {
    #[derive(Default)]
    struct Sink {
        transactions: Vec<AssembledTxn>,
        marks: Vec<Watermark>,
    }
    impl WatermarkSink for Sink {
        fn committed(&mut self, transaction: &AssembledTxn) -> Result<()> {
            self.transactions.push(transaction.clone());
            Ok(())
        }
        fn watermark(&mut self, mark: Watermark) -> Result<()> {
            assert_eq!(
                self.transactions.last().unwrap().events,
                mark.committed_events
            );
            self.marks.push(mark);
            Ok(())
        }
    }
    let (mut c, store, _) = setup().await;
    let (mut stream, peer) = transport(vec![
        relation(1, "a"),
        begin(20),
        insert(1),
        logical(true, chunk::MESSAGE_PREFIX, b"marker"),
        insert(1),
        commit(20),
        begin(30),
        insert(1),
        commit(30),
    ]);
    let (_gate, mut paused) = watch::channel(false);
    let mut sink = Sink::default();
    for index in 0..9 {
        c.step(&mut stream, &mut sink, &mut paused).await.unwrap();
        assert_eq!(
            sink.transactions.len(),
            if index < 5 {
                0
            } else if index < 8 {
                1
            } else {
                2
            }
        );
        assert_eq!(sink.marks.len(), usize::from(index >= 5));
    }
    peer.await.unwrap();
    assert_eq!(sink.transactions, read_all(store, 2).await);
    assert_eq!(sink.marks[0].message_lsn, 15);
    assert_eq!(sink.marks[0].commit_lsn, 20);
    assert_eq!(
        sink.marks[0]
            .committed_events
            .iter()
            .map(|e| e.kind.clone())
            .collect::<Vec<_>>(),
        vec![EventKind::Insert, EventKind::Watermark, EventKind::Insert]
    );
    assert_eq!(c.durable_lsn(), 31);
}

#[tokio::test]
async fn uncommitted_and_rolled_back_markers_are_never_reported() {
    // pgoutput protocol v1 omits rolled-back transactions entirely. An actual
    // rollback therefore has no M/COMMIT bytes. Also test a lost connection
    // after provisional M: even that marker cannot escape before COMMIT.
    for messages in [
        vec![keepalive()],
        vec![begin(20), logical(true, chunk::MESSAGE_PREFIX, b"aborted")],
    ] {
        let (mut c, store, _) = setup().await;
        let count = messages.len();
        let (mut stream, peer) = transport(messages);
        let (_gate, mut paused) = watch::channel(false);
        let mut marks = Vec::new();
        for _ in 0..count {
            c.step(
                &mut stream,
                &mut |m| {
                    marks.push(m);
                    Ok(())
                },
                &mut paused,
            )
            .await
            .unwrap();
        }
        peer.await.unwrap();
        assert!(
            c.step(
                &mut stream,
                &mut |m| {
                    marks.push(m);
                    Ok(())
                },
                &mut paused
            )
            .await
            .is_err()
        );
        assert!(marks.is_empty());
        assert_eq!(c.durable_lsn(), 10);
        assert!(read_all(store, 0).await.is_empty());
    }
}

#[tokio::test]
async fn late_chunk_insertion_is_typed_and_leaves_capture_usable() {
    let (mut c, store, _) = setup().await;
    let (mut stream, peer) = transport(vec![
        begin(20),
        logical(true, chunk::MESSAGE_PREFIX, b"H"),
        commit(20),
        begin(30),
        commit(30),
        keepalive(),
    ]);
    drive(&mut c, &mut stream, 3).await;
    let boundary = c.boundary.as_ref().unwrap();
    let rows = AssembledTxn::new(
        boundary.source.clone(),
        crate::event::TxnId(vec![42]),
        boundary.commit_position.clone(),
        None,
        Vec::new(),
    )
    .unwrap();
    assert!(matches!(
        c.append_chunk_rows(40, rows.clone()).await,
        Err(Error::ChunkBoundaryUnavailable)
    ));
    drive(&mut c, &mut stream, 2).await;
    assert!(matches!(
        c.append_chunk_rows(20, rows).await,
        Err(Error::ChunkBoundaryPassed {
            requested: 20,
            committed: 30
        })
    ));
    assert!(!c.writer().is_blocked());
    drive(&mut c, &mut stream, 1).await;
    peer.await.unwrap();
    assert_eq!(read_all(store, 2).await.len(), 2);
}

#[tokio::test]
async fn configuration_requires_the_exact_chunk_prefix() {
    for prefix in ["", ddl::MESSAGE_PREFIX, "skedaddle.wm.extra"] {
        let (c, _, _) = setup().await;
        assert!(matches!(
            Capture::new(
                SourceIdentity {
                    engine: "postgres".into(),
                    system_id: "1".into(),
                    lineage: "1".into(),
                    db_name: "db".into(),
                    stream: "slot".into(),
                },
                "pg-test".into(),
                10,
                c.writer,
                resolve as Resolver,
                "p5e".into(),
                SchemaVersions::new("p5e".into(), 0, []).unwrap(),
                prefix.into()
            ),
            Err(Error::RecoveryRequired(
                "capture configuration or writer ownership"
            ))
        ));
    }
}

#[tokio::test]
async fn chunk_publication_at_high_precedes_next_commit_and_validates_in_journal() {
    use crate::{
        coordinator::{
            Transition,
            chunk::{ChunkPlan, ChunkRowsRef},
            journal::{Entry, Journal, local_store},
        },
        pg::chunk::{Direction, KeyColumn, QualifiedName, TableKey, WatermarkKind, Window},
    };
    let (mut c, store, root) = setup().await;
    let plan = ChunkPlan::new(
        TableKey {
            table: "\"public\".\"a\"".into(),
            relation: QualifiedName {
                schema: "public".into(),
                name: "a".into(),
            },
            columns: vec![KeyColumn {
                name: "id".into(),
                pg_type: PgType::Int4,
                sql_type: QualifiedName {
                    schema: "pg_catalog".into(),
                    name: "int4".into(),
                },
                collation: None,
                direction: Direction::Asc,
            }],
        },
        100,
        vec![],
    )
    .unwrap();
    let range = plan.ranges[0].clone();
    let snapshot = [7, 8]
        .into_iter()
        .map(|id| {
            Row::from([(
                "id".into(),
                OpaqueValue {
                    type_id: "int4".into(),
                    bytes: Some(Value::Int4(id).canonical_bytes().unwrap()),
                },
            )])
        })
        .collect();
    let window = Window::new(plan.table.clone(), range.clone(), snapshot, 1).unwrap();
    struct Sink {
        window: Window,
        marks: Vec<Watermark>,
        transactions: Vec<AssembledTxn>,
    }
    impl WatermarkSink for Sink {
        fn committed(&mut self, transaction: &AssembledTxn) -> Result<()> {
            for event in &transaction.events {
                self.window.event(event).unwrap();
            }
            self.transactions.push(transaction.clone());
            Ok(())
        }
        fn watermark(&mut self, mark: Watermark) -> Result<()> {
            self.window
                .watermark(&mark.message(), mark.commit_lsn, &Order)
                .unwrap();
            self.marks.push(mark);
            Ok(())
        }
    }
    let marker = |wm| {
        chunk::Watermark {
            wm,
            chunk: range.id.clone(),
            attempt: 1,
        }
        .encode()
        .unwrap()
    };
    let (mut stream, peer) = transport(vec![
        relation(1, "a"),
        begin(20),
        logical(true, chunk::MESSAGE_PREFIX, &marker(WatermarkKind::Low)),
        commit(20),
        begin(25),
        insert(1),
        commit(25),
        begin(30),
        logical(true, chunk::MESSAGE_PREFIX, &marker(WatermarkKind::High)),
        commit(30),
        begin(40),
        insert(1),
        commit(40),
    ]);
    let (_gate, mut paused) = watch::channel(false);
    let mut sink = Sink {
        window,
        marks: Vec::new(),
        transactions: Vec::new(),
    };
    for index in 0..10 {
        c.step(&mut stream, &mut sink, &mut paused).await.unwrap();
        assert_eq!(
            sink.marks.len(),
            if index < 3 {
                0
            } else if index < 9 {
                1
            } else {
                2
            }
        );
    }
    assert_eq!(
        sink.marks.iter().map(|m| m.commit_lsn).collect::<Vec<_>>(),
        vec![20, 30]
    );
    assert!(
        sink.marks
            .iter()
            .all(|m| m.message_lsn == 15 && m.transactional)
    );
    let source = sink.transactions[0].source.clone();
    let format::SegmentRecord::ChunkRows(rows) = sink.window.finish(source, "pg-test", 0).unwrap()
    else {
        panic!("chunk rows")
    };
    assert_eq!(rows.events.len(), 1); // key 7 was changed by the intervening commit
    let reference = c.append_chunk_rows(30, rows.clone()).await.unwrap();
    assert_eq!(reference.mseq, 3);
    assert_eq!(
        c.append_chunk_rows(30, rows.clone()).await.unwrap(),
        reference
    );
    drive(&mut c, &mut stream, 3).await;
    assert!(matches!(
        c.append_chunk_rows(30, rows.clone()).await,
        Err(Error::ChunkBoundaryPassed {
            requested: 30,
            committed: 40
        })
    ));
    peer.await.unwrap();
    let durable = read_all(store.clone(), 5).await;
    assert_eq!(durable[0..3], sink.transactions);
    assert_eq!(durable[3], rows);
    assert_eq!(commit_lsn(&durable[2]).unwrap(), 30);
    assert_eq!(commit_lsn(&durable[4]).unwrap(), 40);
    assert_eq!(sink.marks[1].committed_events, durable[2].events);
    std::fs::create_dir_all(root.join("journal")).unwrap();
    let journal = Journal::new(
        Arc::new(local_store(root.join("journal")).unwrap()),
        "source-test",
        "source",
        Duration::from_secs(5),
    )
    .unwrap()
    .with_chunk_buffer(
        store,
        ReaderConfig {
            prefix: "test".into(),
            migration_id: "source".into(),
            stream: "wal".into(),
            limits: format::Limits::default(),
            required_next_mseq: 0,
        },
        Arc::new(Order),
    );
    let saved_plan = journal.publish_chunk_plan(&plan).await.unwrap();
    for transition in [
        Transition::AcquireOwnership { epoch: 1 },
        Transition::ChunkPlanned { plan: saved_plan },
        Transition::ChunkStarted {
            chunk: range.clone(),
            attempt: 1,
        },
        Transition::ChunkDone {
            chunk: range,
            attempt: 1,
            high: Position::new("pg-test", 30u64.to_be_bytes()),
            chunk_rows: ChunkRowsRef {
                mseq: reference.mseq,
                manifest_digest: reference.manifest_digest,
            },
        },
    ] {
        let entry = Entry::new(
            &journal.load(false).await.unwrap(),
            transition,
            "source-test",
        )
        .unwrap();
        journal.put(&entry).await.unwrap();
    }
    journal.load(false).await.unwrap();
}

#[tokio::test]
async fn toast_is_not_null_and_truncate_remains_one_ordered_event() {
    let (mut c, store, _) = setup().await;
    let mut update = vec![b'U'];
    update.extend(1_u32.to_be_bytes());
    update.push(b'N');
    tuple(&mut update, b"7", None, true);
    let mut truncate = vec![b'T'];
    truncate.extend(2_u32.to_be_bytes());
    truncate.push(3);
    truncate.extend(2_u32.to_be_bytes());
    truncate.extend(1_u32.to_be_bytes());
    let (mut s, peer) = transport(vec![
        relation(1, "a"),
        relation(2, "b"),
        begin(20),
        wal(update),
        wal(truncate),
        commit(20),
    ]);
    drive(&mut c, &mut s, 6).await;
    peer.await.unwrap();
    let txns = read_all(store, 1).await;
    assert_eq!(
        txns[0].events[0].unchanged_cols,
        BTreeSet::from(["value".into()])
    );
    assert!(
        !txns[0].events[0]
            .after
            .as_ref()
            .unwrap()
            .contains_key("value")
    );
    assert_eq!(txns[0].events.len(), 2);
    assert_eq!(
        txns[0].events[1].kind,
        EventKind::Truncate {
            relations: vec!["\"public\".\"b\"".into(), "\"public\".\"a\"".into()],
            cascade: true,
            restart_identity: true
        }
    );
}

#[tokio::test]
async fn manifest_failure_after_segment_put_never_acknowledges_and_replays() {
    let (mut c, store, root) = setup().await;
    // Actual local filesystem obstruction: the segment can sync, but the exact
    // manifest destination is a directory. No object-store behavior is mocked.
    let manifest = format::manifest_key("test", "source", "wal", 0);
    let obstruction = root.join("store").join(&manifest);
    std::fs::create_dir_all(&obstruction).unwrap();
    let (mut s, peer) = transport(vec![relation(1, "a"), begin(20), insert(1), commit(20)]);
    drive(&mut c, &mut s, 3).await;
    let (_gate, mut paused) = watch::channel(false);
    assert!(c.step(&mut s, &mut |_| Ok(()), &mut paused).await.is_err());
    assert_eq!(c.durable_lsn(), 10);
    // The immutable-key conflict fences the writer; it cannot report a new
    // durable checkpoint. Capture still owns the unchanged feedback boundary.
    assert!(c.writer().checkpoint().is_err());
    assert_eq!(c.writer().pending_transactions(), 1);
    assert_eq!(peer.await.unwrap(), vec![10; 4]);
    let pending = &c.pending.as_ref().unwrap().completed.transaction;
    let segment = format::segment_key(
        "test",
        "source",
        "wal",
        &pending.commit_position,
        &pending.commit_position,
        1,
    );
    assert!(store.get(&segment).await.unwrap().is_some());
    drop(c);
    drop(store); // simulated process loss at the actual FS boundary
    std::fs::remove_dir(&obstruction).unwrap();
    let store =
        Arc::new(ObjectBufferStore::local(root.join("store"), Duration::from_secs(5)).unwrap());
    let (tx, _) = mpsc::channel();
    let writer = BufferWriter::open_with_scratch(
        store.clone(),
        writer_config(),
        Arc::new(Order),
        tx,
        root.join("scratch"),
    )
    .await
    .unwrap();
    assert!(writer.checkpoint().unwrap().last_position.is_none());
    let source = SourceIdentity {
        engine: "postgres".into(),
        system_id: "1".into(),
        lineage: "1".into(),
        db_name: "db".into(),
        stream: "slot".into(),
    };
    let mut replay = Capture::new(
        source,
        "pg-test".into(),
        10,
        writer,
        resolve as Resolver,
        "p5e".into(),
        SchemaVersions::new("p5e".into(), 0, []).unwrap(),
        chunk::MESSAGE_PREFIX.into(),
    )
    .unwrap();
    let (mut s, peer) = transport(vec![
        relation(1, "a"),
        begin(20),
        insert(1),
        commit(20),
        keepalive(),
    ]);
    drive(&mut replay, &mut s, 5).await;
    assert_eq!(peer.await.unwrap(), vec![10, 10, 10, 10, 21]);
    assert_eq!(read_all(store, 1).await.len(), 1);
}

#[tokio::test]
async fn unsupported_message_poisoning_prevents_skipping_wal() {
    let (mut c, _, _) = setup().await;
    let (mut s, peer) = transport(vec![logical(false, "unknown", b"data")]);
    let (_gate, mut paused) = watch::channel(false);
    assert!(c.step(&mut s, &mut |_| Ok(()), &mut paused).await.is_err());
    assert!(matches!(
        c.step(&mut s, &mut |_| Ok(()), &mut paused).await,
        Err(Error::RecoveryRequired(_))
    ));
    assert_eq!(c.durable_lsn(), 10);
    peer.await.unwrap();
}

#[test]
fn session_settings_and_lineage_fail_closed() {
    let expected = Lineage {
        system_id: 1,
        timeline: 1,
        database: "db".into(),
        xlogpos: 100,
    };
    let mut settings = SessionSettings {
        date_style: "ISO, MDY".into(),
        interval_style: "postgres".into(),
        time_zone: "UTC".into(),
        extra_float_digits: 3,
        client_encoding: "UTF8".into(),
        standard_conforming_strings: true,
    };
    verify_session(&expected, &expected, &settings).unwrap();
    settings.time_zone = "America/Chicago".into();
    assert!(matches!(
        verify_session(&expected, &expected, &settings),
        Err(Error::RecoveryRequired(_))
    ));
    let mut changed = expected.clone();
    changed.timeline += 1;
    assert!(matches!(
        verify_session(&expected, &changed, &settings),
        Err(Error::Replication(ReplicationError::RecoveryRequired(_)))
    ));
}
