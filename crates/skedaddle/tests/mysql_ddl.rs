use mysql_async::prelude::Queryable;
use skedaddle::{
    buffer::{
        backends::ObjectBufferStore,
        writer::{BufferWriter, WriterConfig},
    },
    engine::{Capture, Captured},
    event::{Position, PositionOrder, SourceIdentity},
    mapping::Value,
    mysql::{
        MysqlOrder, MysqlPosition,
        capture::{CaptureStopReason, MysqlCapture},
        client::{MysqlConfig, MysqlStatus},
    },
};
use std::{path::Path, sync::Arc, time::Duration};
use tokio::time::{Instant, timeout_at};

const COLUMNS: &str = "i8,u8,i16,u16,i24,u24,i32,u32,i64,u64,decv,fv,dv,bits,d,dt,ts,tm,yr,cv,vv,txt,bv,blobv,ev,sv,jv,nullable_v,wide_row";
fn source() -> SourceIdentity {
    SourceIdentity {
        engine: "mysql".into(),
        system_id: "ddl-cluster".into(),
        lineage: "harness".into(),
        db_name: "type_zoo".into(),
        stream: "ddl".into(),
    }
}
fn position(s: &MysqlStatus) -> MysqlPosition {
    if s.gtid_on {
        MysqlPosition::Gtid {
            set: s.executed.clone(),
            server_uuid: s.server_uuid.clone(),
            file: s.file.clone(),
            offset: s.offset,
        }
    } else {
        MysqlPosition::File {
            server_uuid: s.server_uuid.clone(),
            file_seq: s.file.rsplit('.').next().unwrap().parse().unwrap(),
            offset: s.offset,
        }
    }
}
async fn capture(
    cfg: &MysqlConfig,
    start: MysqlPosition,
    tables: &[(String, String)],
    root: &Path,
    server_id: u32,
) -> MysqlCapture {
    for name in ["buffer", "scratch"] {
        std::fs::create_dir_all(root.join(name)).unwrap();
    }
    let order: Arc<dyn PositionOrder + Send + Sync> = Arc::new(match &start {
        MysqlPosition::Gtid { .. } => MysqlOrder::gtid(&source().system_id).unwrap(),
        MysqlPosition::File { server_uuid, .. } => MysqlOrder::file(server_uuid).unwrap(),
    });
    let writer = BufferWriter::open_with_scratch(
        Arc::new(ObjectBufferStore::local(root.join("buffer"), Duration::from_secs(3)).unwrap()),
        WriterConfig {
            prefix: "ddl".into(),
            migration_id: "ddl".into(),
            stream: "ddl".into(),
            epoch: 1,
            segment_bytes: 1_000_000,
            segment_transactions: 100,
            segment_age: Duration::from_secs(1),
            max_transaction_bytes: 4_000_000,
        },
        order,
        std::sync::mpsc::channel().0,
        root.join("scratch"),
    )
    .await
    .unwrap();
    MysqlCapture::new(cfg, source(), start, tables, writer, server_id)
        .await
        .unwrap()
}
async fn transaction(c: &mut MysqlCapture) -> Captured {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let batch = timeout_at(deadline, c.step())
            .await
            .expect("selected write timeout")
            .expect("capture step");
        assert!(!batch.stop, "unexpected stop: {:?}", c.stop_reason());
        if batch.transaction.is_some() {
            return batch;
        }
    }
}
fn assert_position(batch: &Captured, expected: &MysqlStatus) {
    let native = MysqlPosition::decode(batch.durable_position.as_ref().unwrap().bytes()).unwrap();
    assert_eq!(native, position(expected));
}

#[tokio::test]
async fn live_ddl_unselected_progress_and_selected_typed_stops()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(path) = std::env::var_os("SKEDADDLE_MYSQL_CONFIG") else {
        eprintln!(
            "MYSQL SKIPPED: SKEDADDLE_MYSQL_CONFIG absent; use harness/run.ps1 -Phase mysql80|mysql84 -MysqlGtid ON|OFF -MysqlCargoTest '--test mysql_ddl -- --nocapture'"
        );
        return Ok(());
    };
    let cfg: MysqlConfig = serde_json::from_slice(&std::fs::read(path)?)?;
    let root = std::path::PathBuf::from(
        std::env::var_os("SKEDADDLE_TEST_ROOT").expect("harness test root"),
    )
    .join(format!("ddl-{}", uuid::Uuid::new_v4()));
    let mut admin = cfg.connect().await?;
    admin.conn.query_drop("SET NAMES utf8mb4").await?;
    admin
        .conn
        .query_drop("CREATE TABLE type_zoo.ddl_other LIKE type_zoo.zoo")
        .await?;
    let start = position(&admin.status().await?);
    let mut c = capture(
        &cfg,
        start,
        &[("type_zoo".into(), "zoo".into())],
        &root.join("unselected"),
        6481,
    )
    .await;
    let statements = [
        "ALTER TABLE type_zoo.ddl_other ADD COLUMN extra INT",
        "RENAME TABLE type_zoo.ddl_other TO type_zoo.ddl_renamed",
        "TRUNCATE TABLE type_zoo.ddl_renamed",
        "CREATE INDEX ddl_ix ON type_zoo.ddl_renamed(i32)",
        "DROP TABLE type_zoo.ddl_renamed",
    ];
    let mut previous: Option<Position> = None;
    for (i, sql) in statements.iter().enumerate() {
        admin.conn.query_drop(*sql).await?;
        let status = admin.status().await?;
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let batch = timeout_at(deadline, c.step()).await??;
            assert!(!batch.stop);
            assert!(batch.transaction.is_none());
            if MysqlPosition::decode(batch.durable_position.as_ref().unwrap().bytes())?
                == position(&status)
            {
                assert_position(&batch, &status);
                break;
            }
        }
        assert!(c.stop_reason().is_none());
        let id = 800 + i as i64;
        admin
            .conn
            .query_drop(format!(
                "INSERT INTO type_zoo.zoo SELECT {id},{COLUMNS} FROM type_zoo.zoo WHERE id=1"
            ))
            .await?;
        let batch = transaction(&mut c).await;
        let txn = batch.transaction.as_ref().unwrap();
        assert_eq!(txn.events.len(), 1);
        assert_eq!(txn.events[0].table.as_deref(), Some("type_zoo.zoo"));
        assert_eq!(
            txn.events[0].key["id"].bytes,
            Some(Value::Int8(id).canonical_bytes()?)
        );
        if let Some(previous) = previous {
            assert_eq!(
                txn.commit_position
                    .compare(&previous, c.position_order().as_ref())?,
                std::cmp::Ordering::Greater
            );
        }
        previous = batch.durable_position;
    }
    drop(c);
    for (i, template) in [
        "ALTER TABLE {table} ADD COLUMN extra INT",
        "RENAME TABLE {table} TO type_zoo.ddl_selected_renamed",
        "TRUNCATE TABLE {table}",
        "CREATE INDEX ddl_ix ON {table}(i32)",
    ]
    .iter()
    .enumerate()
    {
        let table = format!("ddl_selected_{i}");
        let qualified = format!("type_zoo.{table}");
        admin
            .conn
            .query_drop(format!("CREATE TABLE {qualified} LIKE type_zoo.zoo"))
            .await?;
        admin
            .conn
            .query_drop(format!(
                "INSERT INTO {qualified} SELECT * FROM type_zoo.zoo WHERE id=1"
            ))
            .await?;
        let start = position(&admin.status().await?);
        let tables = vec![("type_zoo".into(), table)];
        let mut c = capture(
            &cfg,
            start,
            &tables,
            &root.join(format!("selected-{i}")),
            6482 + i as u32,
        )
        .await;
        let sql = template.replace("{table}", &qualified);
        admin.conn.query_drop(&sql).await?;
        let status = admin.status().await?;
        let deadline = Instant::now() + Duration::from_secs(20);
        let stopped = loop {
            let batch = timeout_at(deadline, c.step()).await??;
            assert!(batch.transaction.is_none());
            if batch.stop {
                break batch;
            }
        };
        assert_position(&stopped, &status);
        match c.stop_reason() {
            Some(CaptureStopReason::Ddl {
                tables: affected,
                statement,
            }) => {
                assert_eq!(affected, &tables);
                assert_eq!(statement, &sql);
            }
            other => panic!("expected selected DDL stop: {other:?}"),
        }
        assert_eq!(c.writer_checkpoint()?.unwrap().next_mseq, 0);
        assert!(c.step().await.is_err());
        drop(c);
    }
    admin.conn.disconnect().await?;
    std::fs::remove_dir_all(root)?;
    Ok(())
}
