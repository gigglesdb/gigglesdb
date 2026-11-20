#[path = "support/root.rs"]
mod test_root;

#[path = "../src/mysql/capture/test_support.rs"]
mod startup_support;
use futures::StreamExt;
use mysql_async::prelude::Queryable;
use skedaddle::{
    buffer::{
        backends::ObjectBufferStore,
        format,
        reader::{BufferReader, ReaderCheckpoint, ReaderConfig},
        writer::{BufferWriter, WriterConfig},
    },
    engine::{Capture, Captured, CapturedMarker},
    event::{AssembledTxn, EventKind, Position, PositionOrder, SourceIdentity},
    mapping::{Value, mysql::plan_column},
    mysql::{
        MysqlOrder, MysqlPosition, capture::MysqlCapture, client::MysqlConfig,
        control::install_watermark_table, schema::load_descriptors,
    },
    pg::chunk::{ChunkId, Watermark, WatermarkKind},
};
use std::{sync::Arc, time::Duration};
use tokio::time::{Instant, timeout_at};

const COLUMNS: &str = "i8,u8,i16,u16,i24,u24,i32,u32,i64,u64,decv,fv,dv,bits,d,dt,ts,tm,yr,cv,vv,txt,bv,blobv,ev,sv,jv,nullable_v,wide_row";
fn source() -> SourceIdentity {
    SourceIdentity {
        engine: "mysql".into(),
        system_id: "capture-test-cluster".into(),
        lineage: "harness".into(),
        db_name: "type_zoo".into(),
        stream: "zoo".into(),
    }
}
fn config() -> WriterConfig {
    WriterConfig {
        prefix: "capture".into(),
        migration_id: "live".into(),
        stream: "zoo".into(),
        epoch: 1,
        segment_bytes: 1_000_000,
        segment_transactions: 100,
        segment_age: Duration::from_secs(1),
        max_transaction_bytes: 4_000_000,
    }
}
async fn writer(
    root: &std::path::Path,
    order: Arc<dyn PositionOrder + Send + Sync>,
) -> BufferWriter {
    BufferWriter::open_with_scratch(
        Arc::new(ObjectBufferStore::local(root.join("buffer"), Duration::from_secs(3)).unwrap()),
        config(),
        order,
        std::sync::mpsc::channel().0,
        root.join("scratch"),
    )
    .await
    .unwrap()
}
async fn next_transaction(capture: &mut MysqlCapture) -> Captured {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let batch = timeout_at(deadline, capture.step())
            .await
            .expect("transaction timeout")
            .expect("capture step");
        assert!(!batch.stop);
        if batch.transaction.is_some() {
            return batch;
        }
    }
}
async fn verify(
    batch: &Captured,
    capture: &MysqlCapture,
    reader: &mut BufferReader,
    previous: &mut Option<Position>,
    count: usize,
) -> AssembledTxn {
    let txn = batch.transaction.as_ref().unwrap();
    assert_eq!(txn.events.len(), count);
    assert_eq!(batch.durable_position.as_ref(), Some(&txn.commit_position));
    if let Some(previous) = previous.as_ref() {
        assert_eq!(
            txn.commit_position
                .compare(previous, capture.position_order().as_ref())
                .unwrap(),
            std::cmp::Ordering::Greater
        );
    }
    let cp = capture.writer_checkpoint().unwrap().unwrap();
    assert_eq!(cp.last_position.as_ref(), Some(&txn.commit_position));
    assert!(
        cp.redelivery
            .iter()
            .any(|r| r.txn == txn.txn && r.digest == txn.digest && r.manifest.is_some())
    );
    assert_eq!(reader.next_txn().await.unwrap().as_ref(), Some(txn));
    assert!(
        capture
            .buffered_marker(batch)
            .unwrap()
            .contains("mysql-buffered-v1")
    );
    for event in &txn.events {
        for row in std::iter::once(&event.key)
            .chain(event.before.iter())
            .chain(event.after.iter())
        {
            assert!(row.values().all(|v| v.type_id.starts_with("skva.v1:")));
        }
    }
    *previous = Some(txn.commit_position.clone());
    txn.clone()
}
#[tokio::test]
async fn live_capture_durable_mapping_control_progress_and_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(path) = std::env::var_os("SKEDADDLE_MYSQL_CONFIG") else {
        eprintln!(
            "MYSQL SKIPPED: SKEDADDLE_MYSQL_CONFIG absent; use harness/run.ps1 -Phase mysql80|mysql84 -MysqlGtid ON|OFF -MysqlCargoTest '--test mysql_capture -- --nocapture'"
        );
        return Ok(());
    };
    let cfg: MysqlConfig = serde_json::from_slice(&std::fs::read(path)?)?;
    let root = crate::test_root::scratch_root().join(format!("capture-{}", uuid::Uuid::new_v4()));
    for name in ["buffer", "scratch"] {
        std::fs::create_dir_all(root.join(name))?;
    }
    let mut admin = cfg.connect().await?;
    admin.conn.query_drop("SET NAMES utf8mb4").await?;
    admin.conn.query_drop("SET time_zone='+00:00'").await?;
    admin
        .conn
        .query_drop("CREATE TABLE type_zoo.capture_selected LIKE type_zoo.zoo")
        .await?;
    admin
        .conn
        .query_drop(
            "CREATE TABLE type_zoo.capture_unselected (id BIGINT PRIMARY KEY) ENGINE=InnoDB",
        )
        .await?;
    install_watermark_table(&mut admin.conn).await?;
    admin.conn.query_drop(
        "CREATE TABLE type_zoo.capture_binary (id INT PRIMARY KEY, b16 BINARY(16), b32 BINARY(32)) ENGINE=InnoDB"
    ).await?;
    let status = admin.status().await?;
    assert_eq!(status.gtid_on, cfg.gtid == "ON");
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
        MysqlOrder::gtid(&source().system_id)?
    } else {
        MysqlOrder::file(&status.server_uuid)?
    });
    let tables = vec![
        ("type_zoo".into(), "zoo".into()),
        ("type_zoo".into(), "capture_selected".into()),
        ("type_zoo".into(), "capture_binary".into()),
    ];
    let descriptors = load_descriptors(&mut admin.conn, &tables).await?;
    let mut binary_stream = cfg
        .connect()
        .await?
        .open_stream(&start, 6423, Duration::from_millis(500))
        .await?;
    let mut capture = MysqlCapture::new(
        &cfg,
        source(),
        start,
        &tables,
        writer(&root, order.clone()).await,
        6421,
    )
    .await?;
    let (prepared, _, _, _) =
        startup_support::install(capture, &cfg, &tables, &root.join("journal-first")).await;
    capture = prepared;
    let mut reader = BufferReader::new(
        Arc::new(ObjectBufferStore::local(
            root.join("buffer"),
            Duration::from_secs(3),
        )?),
        ReaderConfig {
            prefix: "capture".into(),
            migration_id: "live".into(),
            stream: "zoo".into(),
            limits: format::Limits::default(),
            required_next_mseq: 0,
        },
        ReaderCheckpoint::default(),
        order,
    )?;
    admin.conn.query_drop("START TRANSACTION").await?;
    for (table, id) in [("zoo", 410), ("zoo", 411), ("capture_selected", 510)] {
        admin
            .conn
            .query_drop(format!(
                "INSERT INTO type_zoo.{table} SELECT {id},{COLUMNS} FROM type_zoo.zoo WHERE id=1"
            ))
            .await?;
    }
    admin
        .conn
        .query_drop("INSERT INTO type_zoo.capture_unselected VALUES(1)")
        .await?;
    admin.conn.query_drop("COMMIT").await?;
    admin
        .conn
        .query_drop("SET SESSION binlog_transaction_compression=ON")
        .await?;
    assert_eq!(
        admin
            .conn
            .query_first::<u8, _>("SELECT @@session.binlog_transaction_compression")
            .await?,
        Some(1)
    );
    admin.conn.query_drop("START TRANSACTION").await?;
    admin
        .conn
        .query_drop(format!(
            "INSERT INTO type_zoo.zoo SELECT 412,{COLUMNS} FROM type_zoo.zoo WHERE id=1"
        ))
        .await?;
    admin
        .conn
        .query_drop(
            "UPDATE type_zoo.zoo SET vv='capture-updated',nullable_v='nonnull' WHERE id=410",
        )
        .await?;
    admin
        .conn
        .query_drop("DELETE FROM type_zoo.zoo WHERE id=411")
        .await?;
    admin.conn.query_drop("COMMIT").await?;
    let mut previous = None;
    let batch = next_transaction(&mut capture).await;
    let first = verify(&batch, &capture, &mut reader, &mut previous, 3).await;
    assert_eq!(
        first
            .events
            .iter()
            .map(|e| e.table.as_deref().unwrap())
            .collect::<Vec<_>>(),
        ["type_zoo.zoo", "type_zoo.zoo", "type_zoo.capture_selected"]
    );
    for (event, id) in first.events.iter().zip([410, 411, 510]) {
        assert_eq!(
            event.key["id"].bytes,
            Some(Value::Int8(id).canonical_bytes()?)
        );
        let row = event.after.as_ref().unwrap();
        for column in &descriptors[0].columns {
            assert_eq!(row[&column.name].type_id, plan_column(column)?.type_id);
        }
        assert_eq!(
            row["u64"].bytes,
            Some(
                Value::Numeric(skedaddle::mapping::Numeric::parse("18446744073709551615")?)
                    .canonical_bytes()?
            )
        );
    }
    let batch = next_transaction(&mut capture).await;
    let second = verify(&batch, &capture, &mut reader, &mut previous, 3).await;
    assert_eq!(
        second.events.iter().map(|e| &e.kind).collect::<Vec<_>>(),
        [&EventKind::Insert, &EventKind::Update, &EventKind::Delete]
    );
    assert_eq!(
        second.events[1].after.as_ref().unwrap()["vv"].bytes,
        Some(
            Value::Varchar(skedaddle::mapping::EncodedBytes {
                encoding: "UTF8".into(),
                bytes: b"capture-updated".to_vec()
            })
            .canonical_bytes()?
        )
    );
    assert!(
        second.events[1].before.as_ref().unwrap()["nullable_v"]
            .bytes
            .is_none()
    );
    // Fixed-width binary images must retain every trailing zero through mapping.
    admin
        .conn
        .query_drop(
            "INSERT INTO type_zoo.capture_binary VALUES
         (1, UNHEX(CONCAT(REPEAT('ab',15),'00')), UNHEX(CONCAT(REPEAT('ab',31),'00'))),
         (2, UNHEX(CONCAT(REPEAT('cd',12),'00000000')), UNHEX(CONCAT(REPEAT('cd',28),'00000000'))),
         (3, UNHEX(REPEAT('00',16)), UNHEX(REPEAT('00',32)))",
        )
        .await?;
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut binary_table = None;
    let mut seen = 0;
    let mut binary_lengths = Vec::new();
    while seen < 3 {
        use skedaddle::mysql::binlog::{MysqlEvent, MysqlValue};
        let event = timeout_at(deadline, binary_stream.next())
            .await
            .unwrap()
            .unwrap()?;
        match event.event {
            MysqlEvent::TableMap(table) if table.table == b"capture_binary" => {
                eprintln!("BINARY TABLE metadata: {:?}", table.columns);
                binary_table = Some(table.id);
            }
            MysqlEvent::Rows {
                table_id, images, ..
            } if Some(table_id) == binary_table => {
                for (_, after) in images {
                    let row = after.unwrap();
                    eprintln!("BINARY raw decoded row: {row:?}");
                    for (i, width) in [(1, 16), (2, 32)] {
                        let MysqlValue::Binary(bytes) = &row[i] else {
                            panic!("binary cell")
                        };
                        binary_lengths.push((bytes.len(), width));
                    }
                    seen += 1;
                }
            }
            _ => {}
        }
    }
    binary_stream.close().await?;
    assert!(
        binary_lengths.iter().all(|(actual, width)| actual == width),
        "decoded binary lengths: {binary_lengths:?}"
    );
    let binary = next_transaction(&mut capture).await;
    let binary = verify(&binary, &capture, &mut reader, &mut previous, 3).await;
    for (event, (byte, zeros)) in binary.events.iter().zip([(0xab, 1), (0xcd, 4), (0, 32)]) {
        let row = event.after.as_ref().unwrap();
        for (column, width) in [("b16", 16), ("b32", 32)] {
            let mut expected = vec![byte; width];
            expected[width - zeros.min(width)..].fill(0);
            assert_eq!(
                row[column].bytes,
                Some(Value::Bytea(expected).canonical_bytes()?)
            );
        }
    }
    admin
        .conn
        .query_drop("INSERT INTO type_zoo.capture_unselected VALUES(2)")
        .await?;
    let deadline = Instant::now() + Duration::from_secs(10);
    let progress = loop {
        let batch = timeout_at(deadline, capture.step()).await??;
        assert!(batch.transaction.is_none());
        if batch.durable_position.as_ref().unwrap().compare(
            previous.as_ref().unwrap(),
            capture.position_order().as_ref(),
        )? == std::cmp::Ordering::Greater
        {
            break batch;
        }
    };
    assert!(reader.next_txn().await?.is_none());
    let wm = Watermark {
        wm: WatermarkKind::High,
        chunk: ChunkId {
            table: "type_zoo.zoo".into(),
            plan: "b".repeat(64),
            ordinal: 1,
        },
        attempt: 1,
    };
    admin.conn.query_drop("START TRANSACTION").await?;
    admin
        .conn
        .query_drop("UPDATE type_zoo.zoo SET vv='marker-update' WHERE id=410")
        .await?;
    admin
        .conn
        .exec_drop(
            "INSERT INTO _skedaddle.watermark(marker) VALUES(?)",
            (String::from_utf8(wm.encode()?)?,),
        )
        .await?;
    admin.conn.query_drop("COMMIT").await?;
    let batch = next_transaction(&mut capture).await;
    let txn = verify(&batch, &capture, &mut reader, &mut previous, 1).await;
    assert_eq!(batch.watermarks.len(), 1);
    let CapturedMarker::Chunk {
        marker,
        commit,
        committed_events,
    } = &batch.watermarks[0]
    else {
        panic!("chunk marker")
    };
    assert_eq!(marker, &wm);
    assert_eq!(commit, &txn.commit_position);
    assert_eq!(committed_events, &txn.events);
    assert_eq!(
        txn.commit_position.compare(
            progress.durable_position.as_ref().unwrap(),
            capture.position_order().as_ref()
        )?,
        std::cmp::Ordering::Greater
    );
    let restart = MysqlPosition::decode(batch.durable_position.as_ref().unwrap().bytes())?;
    let order = capture.position_order();
    drop(capture);
    // One transaction during downtime, one after restart: neither can be lost.
    admin
        .conn
        .query_drop(format!(
            "INSERT INTO type_zoo.zoo SELECT 420,{COLUMNS} FROM type_zoo.zoo WHERE id=1"
        ))
        .await?;
    let mut capture = MysqlCapture::new(
        &cfg,
        source(),
        restart,
        &tables,
        writer(&root, order).await,
        6422,
    )
    .await?;
    let (prepared, _, _, _) =
        startup_support::install(capture, &cfg, &tables, &root.join("journal-restart")).await;
    capture = prepared;
    admin
        .conn
        .query_drop(format!(
            "INSERT INTO type_zoo.zoo SELECT 421,{COLUMNS} FROM type_zoo.zoo WHERE id=1"
        ))
        .await?;
    for id in [420, 421] {
        let batch = next_transaction(&mut capture).await;
        let txn = verify(&batch, &capture, &mut reader, &mut previous, 1).await;
        assert_eq!(
            txn.events[0].key["id"].bytes,
            Some(Value::Int8(id).canonical_bytes()?)
        );
    }
    let idle = timeout_at(Instant::now() + Duration::from_secs(5), capture.step()).await??;
    assert!(idle.transaction.is_none());
    assert!(reader.next_txn().await?.is_none());
    admin
        .conn
        .query_drop("ALTER TABLE type_zoo.capture_unselected ADD COLUMN ddl_stop INT")
        .await?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let batch = timeout_at(deadline, capture.step()).await??;
        assert!(batch.transaction.is_none());
        assert!(!batch.stop);
        if batch.durable_position.as_ref().unwrap().compare(
            previous.as_ref().unwrap(),
            capture.position_order().as_ref(),
        )? == std::cmp::Ordering::Greater
        {
            break;
        }
    }
    admin
        .conn
        .query_drop(format!(
            "INSERT INTO type_zoo.zoo SELECT 422,{COLUMNS} FROM type_zoo.zoo WHERE id=1"
        ))
        .await?;
    let batch = next_transaction(&mut capture).await;
    let txn = verify(&batch, &capture, &mut reader, &mut previous, 1).await;
    assert_eq!(
        txn.events[0].key["id"].bytes,
        Some(Value::Int8(422).canonical_bytes()?)
    );
    drop(capture);
    drop(reader);
    admin.conn.disconnect().await?;
    std::fs::remove_dir_all(root)?;
    Ok(())
}
