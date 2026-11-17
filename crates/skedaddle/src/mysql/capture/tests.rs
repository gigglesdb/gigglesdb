use super::*;
use crate::{
    buffer::{
        backends::ObjectBufferStore,
        format,
        reader::{BufferReader, ReaderCheckpoint, ReaderConfig},
        writer::WriterConfig,
    },
    mysql::{
        binlog::{BinlogDecoder, MysqlColumn, MysqlTable, MysqlValue as V, QueryKind, RowKind},
        schema::{Collation, ColumnDescriptor},
    },
    pg::chunk::{ChunkId, Watermark, WatermarkKind},
};
use std::{io::Cursor, path::PathBuf};
const UUID: &str = "426f5bf9-e122-11f1-a0ab-0242ac120002";
fn source() -> SourceIdentity {
    SourceIdentity {
        engine: "mysql".into(),
        system_id: "fixture-cluster".into(),
        lineage: "test".into(),
        db_name: "type_zoo".into(),
        stream: "zoo".into(),
    }
}
fn start(gtid: bool) -> MysqlPosition {
    if gtid {
        MysqlPosition::Gtid {
            set: Default::default(),
            server_uuid: UUID.into(),
            file: "mysql-bin.000004".into(),
            offset: 4,
        }
    } else {
        MysqlPosition::File {
            server_uuid: UUID.into(),
            file_seq: 4,
            offset: 4,
        }
    }
}
fn zoo() -> TableDescriptor {
    let specs = [
        ("id", "bigint"),
        ("i8", "tinyint"),
        ("u8", "tinyint unsigned"),
        ("i16", "smallint"),
        ("u16", "smallint unsigned"),
        ("i24", "mediumint"),
        ("u24", "mediumint unsigned"),
        ("i32", "int"),
        ("u32", "int unsigned"),
        ("i64", "bigint"),
        ("u64", "bigint unsigned"),
        ("decv", "decimal(65,30)"),
        ("fv", "float"),
        ("dv", "double"),
        ("bits", "bit(17)"),
        ("d", "date"),
        ("dt", "datetime(6)"),
        ("ts", "timestamp(6)"),
        ("tm", "time(6)"),
        ("yr", "year"),
        ("cv", "char(8)"),
        ("vv", "varchar(128)"),
        ("txt", "text"),
        ("bv", "binary(8)"),
        ("blobv", "blob"),
        ("ev", "enum('zero','one','two')"),
        ("sv", "set('a','b','c')"),
        ("jv", "json"),
        ("nullable_v", "varchar(8)"),
        ("wide_row", "mediumtext"),
    ];
    TableDescriptor {
        schema: "type_zoo".into(),
        table: "zoo".into(),
        primary_key: vec!["id".into()],
        columns: specs
            .iter()
            .enumerate()
            .map(|(i, (n, t))| {
                let data_type = t.split(['(', ' ']).next().unwrap().to_owned();
                let text = matches!(
                    data_type.as_str(),
                    "char" | "varchar" | "text" | "mediumtext" | "enum" | "set"
                );
                ColumnDescriptor {
                    name: (*n).into(),
                    ordinal: i as u64 + 1,
                    data_type,
                    column_type: (*t).into(),
                    character_set: text.then(|| "utf8mb4".into()),
                    collation: text.then(|| Collation {
                        name: "utf8mb4_0900_ai_ci".into(),
                        id: 255,
                    }),
                    nullable: *n != "id",
                    generated: false,
                    column_default: None,
                    default_generated: false,
                    generation_expression: None,
                }
            })
            .collect(),
    }
}
async fn writer(p: &MysqlPosition) -> (BufferWriter, BufferReader, PathBuf) {
    let root = PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").expect("set task test root"))
        .join(format!("mysql-capture-{}", uuid::Uuid::new_v4()));
    for name in ["buffer", "scratch"] {
        std::fs::create_dir_all(root.join(name)).unwrap();
    }
    let store =
        Arc::new(ObjectBufferStore::local(root.join("buffer"), Duration::from_secs(3)).unwrap());
    let order: Arc<dyn PositionOrder + Send + Sync> =
        Arc::new(if matches!(p, MysqlPosition::Gtid { .. }) {
            MysqlOrder::gtid("fixture-cluster").unwrap()
        } else {
            MysqlOrder::file(UUID).unwrap()
        });
    let writer = BufferWriter::open_with_scratch(
        store.clone(),
        WriterConfig {
            prefix: "capture".into(),
            migration_id: "test".into(),
            stream: "zoo".into(),
            epoch: 1,
            segment_bytes: 1_000_000,
            segment_transactions: 100,
            segment_age: Duration::from_secs(1),
            max_transaction_bytes: 2_000_000,
        },
        order.clone(),
        std::sync::mpsc::channel().0,
        root.join("scratch"),
    )
    .await
    .unwrap();
    let reader = BufferReader::new(
        store,
        ReaderConfig {
            prefix: "capture".into(),
            migration_id: "test".into(),
            stream: "zoo".into(),
            limits: format::Limits::default(),
            required_next_mseq: 0,
        },
        ReaderCheckpoint::default(),
        order,
    )
    .unwrap();
    (writer, reader, root)
}
fn fixture(variant: &str) -> Vec<PositionedEvent> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/mysql")
        .join(variant);
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("manifest.json")).unwrap()).unwrap();
    let file = manifest[0]["file"].as_str().unwrap();
    let bytes = std::fs::read(root.join(format!("{file}.binlog"))).unwrap();
    let mut input = Cursor::new(&bytes[4..]);
    let mut decoder = BinlogDecoder::new(16 * 1024 * 1024);
    let mut events = Vec::new();
    while let Some(mut event) = decoder.read_positioned(&mut input, file).unwrap() {
        // M2's historical control fixture is varchar('capture-multi-statement'),
        // not M4c's JSON protocol. Route only that fixture table as unselected;
        // independent tests below exercise the strict real control descriptor.
        fn legacy(event: &mut MysqlEvent) {
            match event {
                MysqlEvent::TableMap(t) if t.schema == b"_skedaddle" => {
                    t.schema = b"fixture_legacy".to_vec()
                }
                MysqlEvent::TransactionPayload(es) => {
                    for e in es {
                        legacy(e);
                    }
                }
                _ => {}
            }
        }
        legacy(&mut event.event);
        events.push(event);
    }
    events
}
fn at(event: MysqlEvent, log_pos: u64) -> PositionedEvent {
    PositionedEvent {
        event,
        file: "mysql-bin.000004".into(),
        log_pos,
    }
}

#[tokio::test]
async fn fixture_transactions_are_mapped_once_and_manifest_durable() {
    for variant in [
        "mysql80-gtid-on",
        "mysql80-gtid-off",
        "mysql84-gtid-on",
        "mysql84-gtid-off",
    ] {
        let events = fixture(variant);
        let p = start(variant.ends_with("-on"));
        let (w, mut reader, root) = writer(&p).await;
        let ddl = events
            .iter()
            .position(|e| {
                matches!(
                    e.event,
                    MysqlEvent::Query {
                        kind: QueryKind::Ddl,
                        ..
                    }
                )
            })
            .unwrap();
        let mut capture = MysqlCapture::from_stream(
            source(),
            p,
            vec![zoo()],
            w,
            futures::stream::iter(events[..=ddl].iter().cloned().map(Ok).collect::<Vec<_>>()),
            1_000_000,
        )
        .unwrap();
        let stopped = capture.step().await.unwrap();
        assert!(!stopped.stop);
        assert!(capture.stop_reason().is_none());
        assert!(stopped.transaction.is_none());
        let p = MysqlPosition::decode(stopped.durable_position.as_ref().unwrap().bytes()).unwrap();
        let mut capture = MysqlCapture::from_stream(
            source(),
            p,
            vec![zoo()],
            capture.writer,
            futures::stream::iter(
                events[ddl + 1..]
                    .iter()
                    .cloned()
                    .map(Ok)
                    .collect::<Vec<_>>(),
            ),
            1_000_000,
        )
        .unwrap();
        let mut previous = None;
        for count in [3, 2] {
            let batch = capture.step().await.unwrap();
            let txn = batch.transaction.as_ref().unwrap();
            assert_eq!(txn.events.len(), count);
            assert!(batch.watermarks.is_empty());
            assert_eq!(batch.durable_position.as_ref(), Some(&txn.commit_position));
            assert_eq!(reader.next_txn().await.unwrap().as_ref(), Some(txn));
            assert!(
                capture
                    .writer_checkpoint()
                    .unwrap()
                    .unwrap()
                    .redelivery
                    .iter()
                    .any(|r| r.digest == txn.digest && r.manifest.is_some())
            );
            assert!(
                capture
                    .buffered_marker(&batch)
                    .unwrap()
                    .contains("mysql-buffered-v1")
            );
            for event in &txn.events {
                for row in std::iter::once(&event.key)
                    .chain(event.before.iter())
                    .chain(event.after.iter())
                {
                    assert!(row.values().all(|v| !v.type_id.starts_with("mysql.v1:")));
                }
            }
            assert_eq!(
                txn.events[0].after.as_ref().unwrap()["id"].bytes,
                Some(crate::mapping::Value::Int8(2).canonical_bytes().unwrap())
            );
            if let Some(previous) = previous {
                assert_eq!(
                    txn.commit_position
                        .compare(&previous, capture.order.as_ref())
                        .unwrap(),
                    std::cmp::Ordering::Greater
                );
            }
            previous = Some(txn.commit_position.clone());
        }
        assert!(reader.next_txn().await.unwrap().is_none());
        assert!(matches!(
            capture.step().await,
            Err(Error::Mysql(MysqlError::Invalid(_)))
        ));
        drop(capture);
        drop(reader);
        std::fs::remove_dir_all(root).unwrap();
    }
}
#[tokio::test]
async fn filtered_fixture_progress_and_heartbeat_never_pass_open_transaction() {
    let events = fixture("mysql84-gtid-on");
    let ddl = events
        .iter()
        .position(|e| {
            matches!(
                e.event,
                MysqlEvent::Query {
                    kind: QueryKind::Ddl,
                    ..
                }
            )
        })
        .unwrap();
    let p = start(true);
    let (w, _, root) = writer(&p).await;
    let mut events = events[ddl + 1..].to_vec();
    events.push(at(MysqlEvent::Heartbeat, 999_999));
    events.push(at(
        MysqlEvent::Gtid {
            commit_time: None,
            sid: UUID.into(),
            sequence: 999,
            tag: None,
        },
        999_999,
    ));
    events.push(at(MysqlEvent::Heartbeat, 999_999));
    let mut capture = MysqlCapture::from_stream(
        source(),
        p,
        vec![],
        w,
        futures::stream::iter(events.into_iter().map(Ok)),
        1_000_000,
    )
    .unwrap();
    capture.last_progress -= Duration::from_secs(2);
    let a = capture.step().await.unwrap();
    assert!(a.transaction.is_none());
    let b = capture.step().await.unwrap();
    assert!(b.transaction.is_none());
    assert_eq!(
        b.durable_position
            .as_ref()
            .unwrap()
            .compare(a.durable_position.as_ref().unwrap(), capture.order.as_ref())
            .unwrap(),
        std::cmp::Ordering::Greater
    );
    let c = capture.step().await.unwrap();
    assert_eq!(b.durable_position, c.durable_position);
    assert_eq!(capture.writer_checkpoint().unwrap().unwrap().next_mseq, 0);
    drop(capture);
    std::fs::remove_dir_all(root).unwrap();
}
fn marker() -> Watermark {
    Watermark {
        wm: WatermarkKind::High,
        chunk: ChunkId {
            table: "type_zoo.zoo".into(),
            plan: "a".repeat(64),
            ordinal: 0,
        },
        attempt: 1,
    }
}

#[test]
fn verification_marker_is_versioned_and_isolated_from_chunks() {
    use crate::mysql::{assembly::ControlRow, value::to_opaque};
    let row = |payload| {
        let d = control::descriptor();
        ControlRow {
            table: "_skedaddle.watermark".into(),
            kind: crate::event::EventKind::Insert,
            before: None,
            after: Some(
                [
                    (
                        "id".into(),
                        to_opaque(&d.columns[0], &V::Unsigned(1)).unwrap(),
                    ),
                    (
                        "marker".into(),
                        to_opaque(&d.columns[1], &V::Json(payload)).unwrap(),
                    ),
                ]
                .into(),
            ),
        }
    };
    let m = control::VerifyMarker::new("attempt-id".into(), 2).unwrap();
    let r = row(serde_json::from_str(&m.encode().unwrap()).unwrap());
    assert_eq!(control::decode_verify_marker(&r).unwrap(), Some(m));
    assert!(control::decode_watermark(&r).is_err());
    assert!(control::decode_verify_marker(&row(serde_json::to_value(marker()).unwrap())).is_err());
    for payload in [
        serde_json::json!({"version":2,"verify":"id","attempt":1}),
        serde_json::json!({"version":1,"verify":"id","attempt":0}),
        serde_json::json!({"version":1,"verify":"id","attempt":1,"extra":true}),
    ] {
        assert!(control::decode_verify_marker(&row(payload)).is_err());
    }
}

#[tokio::test]
async fn verification_marker_capture_uses_exact_commit_and_keeps_marker_event() {
    use crate::verify::mysql::evidence::MysqlEvidenceInput;
    let p = start(false);
    let (w, mut reader, root) = writer(&p).await;
    let m = control::VerifyMarker::new("session".into(), 1).unwrap();
    let events = control_events(serde_json::to_value(&m).unwrap(), RowKind::Write);
    let mut capture = MysqlCapture::from_stream(
        source(),
        p,
        vec![zoo()],
        w,
        futures::stream::iter(events.into_iter().map(Ok)),
        10000,
    )
    .unwrap();
    // No connection is opened: this exercises capture and the actual subscription.
    let config = serde_json::from_value(serde_json::json!({"host":"unused","port":0,"ca":"unused","user":"unused","password":"unused","profile":"unused","gtid":"OFF","container_id":"unused","max_decompressed_bytes":10000})).unwrap();
    let mut input = capture.evidence_input(config, 10000, 10).unwrap();
    let range = crate::verify::RangePlan {
        id: marker().chunk,
        lo: None,
        hi: None,
    };
    input.arm(&range).await.unwrap();
    let batch = capture.step().await.unwrap();
    assert!(batch.watermarks.is_empty());
    let crate::verify::mysql::evidence::EvidenceEvent::Commit { position, changes } =
        input.next().await.unwrap()
    else {
        panic!()
    };
    assert_eq!(Some(position), batch.durable_position);
    assert!(changes.is_empty());
    assert_eq!(reader.next_txn().await.unwrap(), batch.transaction);
    drop(capture);
    drop(reader);
    std::fs::remove_dir_all(root).unwrap();
}
fn control_events(payload: serde_json::Value, kind: RowKind) -> Vec<PositionedEvent> {
    let row = vec![V::Unsigned(1), V::Json(payload)];
    vec![
        at(
            MysqlEvent::AnonymousGtid {
                commit_time: None,
                sid: UUID.into(),
                sequence: 0,
            },
            5,
        ),
        at(
            MysqlEvent::Query {
                schema: vec![],
                sql: b"BEGIN".to_vec(),
                kind: QueryKind::Begin,
            },
            10,
        ),
        at(
            MysqlEvent::TableMap(MysqlTable {
                id: 1,
                schema: b"_skedaddle".to_vec(),
                table: b"watermark".to_vec(),
                columns: (0..2)
                    .map(|_| MysqlColumn {
                        name: vec![],
                        column_type: 0,
                        metadata: vec![],
                        unsigned: None,
                        collation: None,
                        nullable: false,
                        labels: None,
                    })
                    .collect(),
                full_metadata: vec![],
            }),
            20,
        ),
        at(
            MysqlEvent::Rows {
                table_id: 1,
                kind,
                images: vec![(
                    if kind == RowKind::Write {
                        None
                    } else {
                        Some(row.clone())
                    },
                    if kind == RowKind::Delete {
                        None
                    } else {
                        Some(row)
                    },
                )],
            },
            30,
        ),
        at(MysqlEvent::Xid(1), 100),
    ]
}

#[tokio::test]
async fn chunk_publication_is_bound_to_native_high_and_idempotent() {
    let p = start(true);
    let (w, mut reader, root) = writer(&p).await;
    let events = fixture("mysql84-gtid-on");
    let ddl = events
        .iter()
        .position(|e| {
            matches!(
                e.event,
                MysqlEvent::Query {
                    kind: QueryKind::Ddl,
                    ..
                }
            )
        })
        .unwrap();
    let mut c = MysqlCapture::from_stream(
        source(),
        p,
        vec![zoo()],
        w,
        futures::stream::iter(
            events[ddl + 1..]
                .iter()
                .cloned()
                .map(Ok)
                .collect::<Vec<_>>(),
        ),
        1_000_000,
    )
    .unwrap();
    let batch = c.step().await.unwrap();
    let txn = batch.transaction.unwrap();
    assert_eq!(reader.next_txn().await.unwrap(), Some(txn.clone()));
    let id = crate::event::TxnId(b"snapshot-test".to_vec());
    let mut row = txn.events[0].clone();
    row.kind = crate::event::EventKind::ChunkRows;
    row.txn = id.clone();
    let rows =
        AssembledTxn::new(source(), id, txn.commit_position.clone(), None, vec![row]).unwrap();
    let wrong = Position::new(c.order.domain(), start(true).encode().unwrap());
    assert!(c.publish(wrong, rows.clone()).await.is_err());
    let reference = c
        .publish(txn.commit_position.clone(), rows.clone())
        .await
        .unwrap();
    assert_eq!(
        c.publish(txn.commit_position.clone(), rows.clone())
            .await
            .unwrap(),
        reference
    );
    assert_eq!(reader.next_txn().await.unwrap(), Some(rows));
    assert!(reader.next_txn().await.unwrap().is_none());
    assert_eq!(c.step().await.unwrap().transaction.unwrap().events.len(), 2);
    assert!(c.publish(txn.commit_position.clone(), txn).await.is_err());
    drop(c);
    drop(reader);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn control_routes_and_invalid_identities_fail_closed() {
    use crate::mysql::{assembly::ControlRow, value::to_opaque};
    let d = control::descriptor();
    let row = |payload| ControlRow {
        table: "_skedaddle.watermark".into(),
        kind: crate::event::EventKind::Insert,
        before: None,
        after: Some(
            [
                (
                    "id".into(),
                    to_opaque(&d.columns[0], &V::Unsigned(1)).unwrap(),
                ),
                (
                    "marker".into(),
                    to_opaque(&d.columns[1], &V::Json(payload)).unwrap(),
                ),
            ]
            .into(),
        ),
    };
    let good = row(serde_json::to_value(marker()).unwrap());
    assert_eq!(control::decode_watermark(&good).unwrap(), Some(marker()));
    let mut other = good.clone();
    other.table = "type_zoo.zoo".into();
    assert_eq!(control::decode_watermark(&other).unwrap(), None);
    other.table = "_skedaddle.unknown".into();
    assert!(matches!(
        control::decode_watermark(&other),
        Err(MysqlError::Control("unknown control table"))
    ));
    for payload in [
        serde_json::json!(null),
        serde_json::json!({"wm":"high","chunk":{"table":"x","plan":"invalid","ordinal":0},"attempt":1}),
        serde_json::json!({"wm":"high","chunk":{"table":"x","plan":"a".repeat(64),"ordinal":0},"attempt":1,"extra":1}),
    ] {
        assert!(control::decode_watermark(&row(payload)).is_err());
    }
}
#[tokio::test]
async fn watermark_is_committed_with_durable_transaction_and_strict_payload() {
    let p = start(false);
    let (w, mut reader, root) = writer(&p).await;
    let events = control_events(serde_json::to_value(marker()).unwrap(), RowKind::Write);
    let mut capture = MysqlCapture::from_stream(
        source(),
        p,
        vec![],
        w,
        futures::stream::iter(events.into_iter().map(Ok)),
        10000,
    )
    .unwrap();
    let batch = capture.step().await.unwrap();
    assert_eq!(batch.watermarks.len(), 1);
    let CapturedMarker::Chunk {
        marker: wm,
        commit,
        committed_events,
    } = &batch.watermarks[0]
    else {
        panic!("chunk marker")
    };
    assert_eq!(wm, &marker());
    assert_eq!(Some(commit), batch.durable_position.as_ref());
    assert_eq!(
        committed_events,
        &batch.transaction.as_ref().unwrap().events
    );
    assert_eq!(reader.next_txn().await.unwrap(), batch.transaction);
    drop(capture);
    drop(reader);
    std::fs::remove_dir_all(root).unwrap();
    for (payload, kind) in [
        (serde_json::json!({"wm":"other"}), RowKind::Write),
        (serde_json::to_value(marker()).unwrap(), RowKind::Update),
        (serde_json::to_value(marker()).unwrap(), RowKind::Delete),
    ] {
        let p = start(false);
        let (w, _, root) = writer(&p).await;
        let mut c = MysqlCapture::from_stream(
            source(),
            p,
            vec![],
            w,
            futures::stream::iter(control_events(payload, kind).into_iter().map(Ok)),
            10000,
        )
        .unwrap();
        assert!(matches!(
            c.step().await,
            Err(Error::Mysql(MysqlError::Control(_)))
        ));
        assert_eq!(c.writer_checkpoint().unwrap().unwrap().next_mseq, 0);
        drop(c);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn ddl_classification_stops_selected_and_unknown_at_query_boundary() {
    for (sql, schema, mode, stop, unknown) in [
        (
            "ALTER TABLE zoo ADD COLUMN x INT",
            "type_zoo",
            0,
            true,
            false,
        ),
        ("RENAME TABLE other TO zoo", "type_zoo", 0, true, false),
        ("TRUNCATE TABLE type_zoo.zoo", "", 0, true, false),
        ("CREATE INDEX ix ON zoo(id)", "type_zoo", 0, true, false),
        (
            "ALTER TABLE ZOO ADD COLUMN x INT",
            "TYPE_ZOO",
            1,
            true,
            false,
        ),
        (
            "ALTER TABLE ZOO ADD COLUMN x INT",
            "TYPE_ZOO",
            2,
            true,
            false,
        ),
        (
            "ALTER TABLE ZOO ADD COLUMN x INT",
            "TYPE_ZOO",
            0,
            false,
            false,
        ),
        (
            "ALTER TABLE other ADD COLUMN x INT",
            "type_zoo",
            0,
            false,
            false,
        ),
        ("CREATE TABLE _skedaddle.own (id INT)", "", 0, false, false),
        ("UNPARSED zoo", "type_zoo", 0, true, true),
        ("ALTER TABLE zoo ADD COLUMN x INT", "", 0, true, true),
    ] {
        for gtid in [false, true] {
            let p = start(gtid);
            let (w, _, root) = writer(&p).await;
            let first = if gtid {
                MysqlEvent::Gtid {
                    commit_time: None,
                    sid: UUID.into(),
                    sequence: 1,
                    tag: None,
                }
            } else {
                MysqlEvent::AnonymousGtid {
                    commit_time: None,
                    sid: UUID.into(),
                    sequence: 0,
                }
            };
            let events = vec![
                at(first, 10),
                at(
                    MysqlEvent::Query {
                        schema: schema.as_bytes().to_vec(),
                        sql: sql.as_bytes().to_vec(),
                        kind: QueryKind::Ddl,
                    },
                    100,
                ),
            ];
            let mut c = MysqlCapture::from_stream_with_case_mode(
                source(),
                p,
                vec![zoo()],
                w,
                futures::stream::iter(events.into_iter().map(Ok)),
                1_000_000,
                mode,
            )
            .unwrap();
            let batch = c.step().await.unwrap();
            assert_eq!(batch.stop, stop, "{sql}");
            assert!(batch.transaction.is_none());
            let native =
                MysqlPosition::decode(batch.durable_position.as_ref().unwrap().bytes()).unwrap();
            match native {
                MysqlPosition::Gtid { offset, set, .. } => {
                    assert_eq!(offset, 100);
                    assert!(set.to_string().contains(":1"));
                }
                MysqlPosition::File { offset, .. } => assert_eq!(offset, 100),
            }
            match c.stop_reason() {
                Some(CaptureStopReason::Ddl { tables, statement }) => {
                    assert!(!unknown);
                    assert_eq!(tables, &vec![("type_zoo".into(), "zoo".into())]);
                    assert_eq!(statement, sql);
                }
                Some(CaptureStopReason::UnknownDdl { statement, reason }) => {
                    assert!(unknown);
                    assert_eq!(statement, sql);
                    assert!(reason.contains("zoo"));
                }
                None => assert!(!stop),
                other => panic!("unexpected reason {other:?}"),
            }
            let state = c.schema_state().unwrap();
            assert!(!state.unknown_schema_job);
            if stop {
                let Some(SchemaBoundary::Connector(boundary)) = &state.blocked_boundary else {
                    panic!("expected typed MySQL stop: {state:?}");
                };
                assert_eq!(boundary.connector, "mysql");
                assert_eq!(
                    boundary.kind,
                    if unknown {
                        SchemaStopKind::UnknownDdl
                    } else {
                        SchemaStopKind::Ddl
                    }
                );
                assert_eq!(
                    boundary.table.as_deref(),
                    if unknown { None } else { Some("type_zoo.zoo") }
                );
                assert!(boundary.diagnostic.contains(sql));
                assert_eq!(Some(&boundary.position), batch.durable_position.as_ref());
                assert!(
                    matches!(c.step().await, Err(Error::RecoveryRequired(reason)) if reason.contains(sql))
                );
            }
            assert_eq!(
                c.schema_state(),
                Some(state.clone()),
                "stop must survive a later step"
            );
            assert_eq!(state.blocked_boundary.is_some(), stop);
            assert_eq!(c.writer_checkpoint().unwrap().unwrap().next_mseq, 0);
            drop(c);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}

#[tokio::test]
async fn fixture_table_map_drift_stops_before_rows_and_preserves_last_commit() {
    for variant in [
        "mysql80-gtid-on",
        "mysql80-gtid-off",
        "mysql84-gtid-on",
        "mysql84-gtid-off",
    ] {
        let events = fixture(variant);
        let map = events
            .iter()
            .find_map(|e| match &e.event {
                MysqlEvent::TableMap(t) if t.schema == b"type_zoo" => Some(t.clone()),
                _ => None,
            })
            .unwrap();
        assert_eq!(compare_table_map(&zoo(), &map), Ok(()), "{variant}");
        let commits = events
            .iter()
            .enumerate()
            .filter(|(_, e)| matches!(e.event, MysqlEvent::Xid(_)))
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        let first_end = commits[0];
        for change in 0..8 {
            let p = start(variant.ends_with("-on"));
            let (w, mut reader, root) = writer(&p).await;
            let mut changed = map.clone();
            match change {
                0 => {
                    changed.columns.pop();
                }
                1 => changed.columns[0].column_type = 3,
                2 => changed.columns[21].metadata[0] ^= 1,
                3 => changed.columns[21].collation = Some(46),
                4 => changed.columns[2].unsigned = Some(false),
                5 => changed.columns[0].nullable = true,
                6 => changed.columns[0].name = b"renamed".to_vec(),
                7 => changed.columns[25]
                    .labels
                    .as_mut()
                    .unwrap()
                    .push(b"new".to_vec()),
                _ => unreachable!(),
            }
            let mut input = events[..=first_end].to_vec();
            // Reuse a real next transaction's GTID/BEGIN, then substitute its map
            // as a fault real servers cannot emit without an earlier DDL query.
            'prefix: for e in &events[first_end + 1..] {
                match &e.event {
                    MysqlEvent::TableMap(_) => break,
                    MysqlEvent::TransactionPayload(inner) => {
                        for event in inner {
                            if matches!(event, MysqlEvent::TableMap(_)) {
                                break 'prefix;
                            }
                            input.push(PositionedEvent {
                                event: event.clone(),
                                file: e.file.clone(),
                                log_pos: e.log_pos,
                            });
                        }
                    }
                    _ => input.push(e.clone()),
                }
            }
            let bad = MysqlEvent::TableMap(changed);
            input.push(at(
                if change % 2 == 0 {
                    MysqlEvent::TransactionPayload(vec![bad])
                } else {
                    bad
                },
                999_999,
            ));
            input.push(at(MysqlEvent::Heartbeat, 1_000_000));
            let mut c = MysqlCapture::from_stream(
                source(),
                p,
                vec![zoo()],
                w,
                futures::stream::iter(input.into_iter().map(Ok)),
                1_000_000,
            )
            .unwrap();
            // The fixture starts with unselected CREATE TABLE, delivered as progress.
            let progress = c.step().await.unwrap();
            assert!(!progress.stop);
            assert!(progress.transaction.is_none());
            let committed = c.step().await.unwrap();
            assert!(committed.transaction.is_some());
            assert_eq!(reader.next_txn().await.unwrap(), committed.transaction);
            let stopped = c.step().await.unwrap();
            assert!(stopped.stop);
            assert!(stopped.transaction.is_none());
            assert_eq!(stopped.durable_position, committed.durable_position);
            assert!(
                matches!(c.stop_reason(), Some(CaptureStopReason::TableMap { schema, table, .. }) if schema == "type_zoo" && table == "zoo")
            );
            let state = c.schema_state().unwrap();
            let Some(SchemaBoundary::Connector(boundary)) = &state.blocked_boundary else {
                panic!("expected TABLE_MAP boundary: {state:?}");
            };
            assert_eq!(boundary.connector, "mysql");
            assert_eq!(boundary.kind, SchemaStopKind::TableMapChanged);
            assert_eq!(boundary.table.as_deref(), Some("type_zoo.zoo"));
            assert!(!boundary.diagnostic.is_empty());
            assert_eq!(
                Some(&boundary.position),
                committed.durable_position.as_ref()
            );
            assert_eq!(c.writer_checkpoint().unwrap().unwrap().next_mseq, 1);
            assert!(reader.next_txn().await.unwrap().is_none());
            assert!(c.step().await.is_err());
            assert_eq!(c.schema_state(), Some(state));
            drop(c);
            drop(reader);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}

#[tokio::test]
async fn fixture_table_maps_follow_source_case_rules() {
    for mode in [0, 1, 2] {
        let p = start(true);
        let (w, _, root) = writer(&p).await;
        let mut descriptor = zoo();
        descriptor.schema = "TYPE_ZOO".into();
        descriptor.table = "ZOO".into();
        let mut events = fixture("mysql84-gtid-on");
        let end = events
            .iter()
            .position(|e| matches!(e.event, MysqlEvent::Xid(_)))
            .unwrap();
        events.truncate(end + 1);
        events.push(at(MysqlEvent::Heartbeat, 999_999));
        let mut c = MysqlCapture::from_stream_with_case_mode(
            source(),
            p,
            vec![descriptor],
            w,
            futures::stream::iter(events.into_iter().map(Ok)),
            1_000_000,
            mode,
        )
        .unwrap();
        let progress = c.step().await.unwrap();
        assert!(!progress.stop);
        let batch = c.step().await.unwrap();
        assert!(!batch.stop);
        assert_eq!(batch.transaction.is_some(), mode != 0);
        if let Some(txn) = batch.transaction {
            assert!(
                txn.events
                    .iter()
                    .all(|e| e.table.as_deref() == Some("TYPE_ZOO.ZOO"))
            );
        }
        drop(c);
        std::fs::remove_dir_all(root).unwrap();
    }
}

fn apply_control(
    receipt: bool,
    kind: crate::event::EventKind,
) -> crate::mysql::assembly::ControlRow {
    let d = control::apply_descriptors().remove(if receipt { 0 } else { 1 });
    let row: Row = d
        .columns
        .iter()
        .map(|c| {
            let v = match c.name.as_str() {
                "committed_at" => V::Timestamp("1700000000.000000".into()),
                "stream" | "position_hash" | "digest" => V::Binary(vec![1; 32]),
                _ => V::Binary(vec![1]),
            };
            (
                c.name.clone(),
                crate::mysql::value::to_opaque(c, &v).unwrap(),
            )
        })
        .collect();
    crate::mysql::assembly::ControlRow {
        table: format!("{}.{}", d.schema, d.table),
        before: (kind != crate::event::EventKind::Insert).then(|| row.clone()),
        after: (kind != crate::event::EventKind::Delete).then_some(row),
        kind,
    }
}

#[tokio::test]
async fn apply_echo_and_gc_are_progress_only_before_mapping_and_adjacent_user_survives() {
    use crate::event::{Event, EventKind, TxnId};
    for kind in [EventKind::Insert, EventKind::Delete, EventKind::Update] {
        let p = start(false);
        let (w, mut reader, root) = writer(&p).await;
        let mut capture = MysqlCapture::from_stream(
            source(),
            p,
            vec![zoo()],
            w,
            futures::stream::pending(),
            1_000_000,
        )
        .unwrap();
        let position = |n| {
            Position::new(
                capture.order.domain(),
                MysqlPosition::File {
                    server_uuid: UUID.into(),
                    file_seq: 4,
                    offset: n,
                }
                .encode()
                .unwrap(),
            )
        };
        let first = position(100);
        let second = position(200);
        // An unmappable extra table proves receipt inspection precedes mapping.
        let extra = Event {
            source: source(),
            txn: TxnId(vec![1]),
            ordinal_in_txn: 0,
            position: first.clone(),
            schema_version: 1,
            kind: EventKind::Insert,
            table: Some("not_planned.extra".into()),
            key: Row::new(),
            before: None,
            after: Some(Row::new()),
            unchanged_cols: Default::default(),
        };
        let events = if kind == EventKind::Insert {
            vec![extra]
        } else {
            vec![]
        };
        let txn = AssembledTxn::new(source(), TxnId(vec![1]), first.clone(), None, events).unwrap();
        let key = Row::from([(
            "id".into(),
            crate::mysql::value::to_opaque(&zoo().columns[0], &V::Signed(42)).unwrap(),
        )]);
        let user = AssembledTxn::new(
            source(),
            TxnId(vec![2]),
            second.clone(),
            None,
            vec![Event {
                source: source(),
                txn: TxnId(vec![2]),
                position: second.clone(),
                ordinal_in_txn: 0,
                schema_version: 1,
                kind: EventKind::Insert,
                table: Some("type_zoo.zoo".into()),
                key: key.clone(),
                before: None,
                after: Some(key),
                unchanged_cols: Default::default(),
            }],
        )
        .unwrap();
        let mapped_user = capture.mapped(user.clone()).unwrap();
        capture.outputs.push_back(AssemblerOutput::Txn {
            txn,
            control_rows: vec![apply_control(kind != EventKind::Update, kind)],
        });
        capture.outputs.push_back(AssemblerOutput::Txn {
            txn: user.clone(),
            control_rows: vec![],
        });
        let suppressed = capture.step().await.unwrap();
        assert!(suppressed.transaction.is_none() && suppressed.watermarks.is_empty());
        assert_eq!(suppressed.durable_position, Some(first));
        capture.buffered_marker(&suppressed).unwrap();
        assert!(
            capture
                .writer_checkpoint()
                .unwrap()
                .unwrap()
                .redelivery
                .is_empty()
        );
        assert_eq!(
            capture.step().await.unwrap().transaction,
            Some(mapped_user.clone())
        );
        assert_eq!(reader.next_txn().await.unwrap(), Some(mapped_user));
        drop((capture, reader));
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn apply_control_malformed_shapes_fail_closed() {
    use crate::event::EventKind;
    for receipt in [false, true] {
        let good = apply_control(receipt, EventKind::Insert);
        assert_eq!(control::decode_apply(&good).unwrap(), Some(receipt));
        let mut missing = good.clone();
        missing.after.as_mut().unwrap().remove("stream");
        assert!(matches!(
            control::decode_apply(&missing),
            Err(MysqlError::Control(_))
        ));
        let mut bad = good.clone();
        bad.after.as_mut().unwrap().get_mut("stream").unwrap().bytes = Some(vec![1]);
        assert!(matches!(
            control::decode_apply(&bad),
            Err(MysqlError::Control(_))
        ));
        let mut null = good;
        null.after
            .as_mut()
            .unwrap()
            .get_mut("stream")
            .unwrap()
            .bytes = None;
        assert!(matches!(
            control::decode_apply(&null),
            Err(MysqlError::Control(_))
        ));
    }
    assert!(control::decode_apply(&apply_control(true, EventKind::Update)).is_err());
}

#[test]
fn apply_table_map_malformed_shape_is_typed_control_error() {
    let mut a = Assembler::new(
        source(),
        control::apply_descriptors(),
        start(false),
        false,
        1_000_000,
    )
    .unwrap();
    let t = MysqlTable {
        id: 7,
        schema: b"_skedaddle".to_vec(),
        table: b"apply_receipts".to_vec(),
        columns: vec![],
        full_metadata: vec![],
    };
    assert!(matches!(
        a.push(at(MysqlEvent::TableMap(t), 10)),
        Err(MysqlError::Control(_))
    ));
}

fn startup_inputs() -> StartupInputs {
    let d = zoo();
    StartupInputs {
        selected: vec![SelectedTable {
            schema: d.schema,
            table: d.table,
            columns: d.columns.iter().map(|c| c.name.clone()).collect(),
            primary_key: d.primary_key,
        }],
        mapping_versions: vec!["mysql.v1".into(), "skva.v1".into()],
        gtid_mode: GtidMode::OFF,
        version: MysqlVersion::V84,
    }
}
async fn startup_journal(
    root: &std::path::Path,
) -> (crate::coordinator::journal::Journal, crate::engine::Runtime) {
    use crate::coordinator::{self, epoch::Coordinator, journal::Journal, local::Local};
    std::fs::create_dir_all(root.join("journal")).unwrap();
    let journal = Journal::new(
        Arc::new(coordinator::journal::local_store(root.join("journal")).unwrap()),
        "capture",
        "startup",
        Duration::from_secs(3),
    )
    .unwrap();
    let mut coordinator = Coordinator::observer(
        journal.clone(),
        Local::create(root.join("local")).unwrap(),
        "test",
    )
    .unwrap();
    coordinator.acquire().await.unwrap();
    let runtime = crate::engine::Engine::new(coordinator, journal.clone()).runtime();
    (journal, runtime)
}
async fn startup_capture(d: TableDescriptor) -> (MysqlCapture, PathBuf) {
    let (w, _, root) = writer(&start(false)).await;
    // Actual recorded events, instrumented to fail if an admission check polls.
    let stream = futures::stream::iter(fixture("mysql84-gtid-off").into_iter().map(|_| {
        panic!("stream read before startup admission");
        #[allow(unreachable_code)]
        Ok(at(MysqlEvent::Heartbeat, 4))
    }));
    (
        MysqlCapture::from_stream(source(), start(false), vec![d], w, stream, 1_000_000)
            .unwrap()
            .with_startup_inputs(startup_inputs()),
        root,
    )
}
#[tokio::test]
async fn startup_missing_malformed_and_drift_refuse_before_stream_read() {
    let (mut c, root) = startup_capture(zoo()).await;
    let (journal, runtime) = startup_journal(&root).await;
    assert!(matches!(
        c.prepare_schema(&journal, &runtime).await,
        Err(Error::Mysql(MysqlError::CaptureStartup(
            CaptureStartupError::MissingRecord
        )))
    ));
    runtime
        .marker("source-startup", "{}".into(), String::new())
        .await
        .unwrap();
    assert!(matches!(
        c.prepare_schema(&journal, &runtime).await,
        Err(Error::Mysql(MysqlError::CaptureStartup(
            CaptureStartupError::Record(_)
        )))
    ));
    runtime
        .marker(
            "source-startup",
            c.startup_record().unwrap().encode().unwrap(),
            String::new(),
        )
        .await
        .unwrap();
    c.descriptors[0].columns[1].nullable = false;
    assert!(matches!(
        c.prepare_schema(&journal, &runtime).await,
        Err(Error::Mysql(MysqlError::CaptureStartup(
            CaptureStartupError::Drift(StartupDrift::DescriptorChanged { .. })
        )))
    ));
    c.descriptors = vec![zoo()];
    c.startup_inputs.as_mut().unwrap().gtid_mode = GtidMode::ON;
    assert!(matches!(
        c.prepare_schema(&journal, &runtime).await,
        Err(Error::Mysql(MysqlError::CaptureStartup(
            CaptureStartupError::Drift(StartupDrift::GtidModeChanged)
        )))
    ));
    drop(c);
    drop(runtime);
    drop(journal);
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn startup_schema_stop_is_durable_sticky_and_conflicts_are_typed() {
    let (mut c, root) = startup_capture(zoo()).await;
    let (journal, runtime) = startup_journal(&root).await;
    runtime
        .marker(
            "source-startup",
            c.startup_record().unwrap().encode().unwrap(),
            String::new(),
        )
        .await
        .unwrap();
    c.prepare_schema(&journal, &runtime).await.unwrap();
    let reason = CaptureStopReason::Ddl {
        tables: vec![("type_zoo".into(), "zoo".into())],
        statement: "TRUNCATE type_zoo.zoo".into(),
    };
    let batch = c.stop(reason.clone()).await.unwrap();
    assert!(batch.stop);
    assert!(
        journal
            .load(true)
            .await
            .unwrap()
            .progress
            .contains_key("mysql-schema-stop")
    );
    let blocked = c.schema_state().unwrap();
    drop(c);
    let (mut restarted, restart_root) = startup_capture(zoo()).await;
    restarted.prepare_schema(&journal, &runtime).await.unwrap();
    assert_eq!(restarted.schema_state().unwrap(), blocked);
    assert!(restarted.step().await.is_err());
    restarted.persist_stop(&reason).await.unwrap();
    assert!(matches!(
        restarted
            .persist_stop(&CaptureStopReason::UnknownDdl {
                statement: "other".into(),
                reason: "unknown".into()
            })
            .await,
        Err(Error::Mysql(MysqlError::CaptureStartup(
            CaptureStartupError::ConflictingStop
        )))
    ));
    assert_eq!(restarted.schema_state().unwrap(), blocked);
    drop(restarted);
    drop(runtime);
    drop(journal);
    std::fs::remove_dir_all(root).unwrap();
    std::fs::remove_dir_all(restart_root).unwrap();
}
