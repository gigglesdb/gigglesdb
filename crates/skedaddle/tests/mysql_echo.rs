use mysql_async::prelude::Queryable;
use skedaddle::{
    buffer::{
        backends::ObjectBufferStore,
        writer::{BufferWriter, WriterConfig},
    },
    engine::Capture,
    event::{
        AssembledTxn, Event, EventKind, OpaqueValue, Position, PositionOrder, SourceIdentity, TxnId,
    },
    mapping::{Value, mysql::plan_column},
    mysql::{
        MysqlOrder, MysqlPosition,
        apply::{MysqlApplier, install_apply_tables},
        capture::MysqlCapture,
        client::MysqlConfig,
        schema::load_descriptors,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

fn reverse_source() -> SourceIdentity {
    SourceIdentity {
        engine: "pg".into(),
        system_id: "echo-pg".into(),
        lineage: "one".into(),
        db_name: "db".into(),
        stream: "reverse".into(),
    }
}
fn apply_txn(n: u64, type_id: &str) -> AssembledTxn {
    let source = reverse_source();
    let txn = TxnId(n.to_be_bytes().to_vec());
    let position = Position::new("pg-echo", n.to_be_bytes());
    let row = BTreeMap::from([(
        "id".into(),
        OpaqueValue {
            type_id: type_id.into(),
            bytes: Some(Value::Int8(n as i64).canonical_bytes().unwrap()),
        },
    )]);
    AssembledTxn::new(
        source.clone(),
        txn.clone(),
        position.clone(),
        None,
        vec![Event {
            source,
            txn,
            position,
            ordinal_in_txn: 0,
            schema_version: 1,
            kind: EventKind::Insert,
            table: Some("echo".into()),
            key: row.clone(),
            before: None,
            after: Some(row),
            unchanged_cols: BTreeSet::new(),
        }],
    )
    .unwrap()
}

#[tokio::test]
async fn live_apply_echo_concurrent_user_gc_and_dump_reconnect()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(path) = std::env::var_os("SKEDADDLE_MYSQL_CONFIG") else {
        eprintln!(
            "MYSQL SKIPPED: SKEDADDLE_MYSQL_CONFIG absent; run harness mysql84 GTID ON / mysql80 GTID OFF"
        );
        return Ok(());
    };
    let mut cfg: MysqlConfig = serde_json::from_slice(&std::fs::read(path)?)?;
    cfg.user = "root".into();
    cfg.password = "skedaddle".into();
    let mut admin = cfg.connect().await?;
    admin
        .conn
        .query_drop("CREATE TABLE type_zoo.echo (id BIGINT PRIMARY KEY) ENGINE=InnoDB")
        .await?;
    install_apply_tables(&cfg).await?;
    let tables = vec![("type_zoo".into(), "echo".into())];
    let d = load_descriptors(&mut admin.conn, &tables).await?.remove(0);
    let type_id = plan_column(&d.columns[0])?.type_id;
    let mut applier = MysqlApplier::new(
        cfg.clone(),
        reverse_source(),
        BTreeMap::from([("echo".into(), d)]),
    )?;
    let status = admin.status().await?;
    let source = SourceIdentity {
        engine: "mysql".into(),
        system_id: "echo-cluster".into(),
        lineage: "one".into(),
        db_name: "type_zoo".into(),
        stream: "echo".into(),
    };
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
    let root = std::path::PathBuf::from(
        std::env::var_os("SKEDADDLE_TEST_ROOT").expect("harness test root"),
    )
    .join(format!("echo-{}", uuid::Uuid::new_v4()));
    for name in ["buffer", "scratch"] {
        std::fs::create_dir_all(root.join(name))?;
    }
    let writer = BufferWriter::open_with_scratch(
        Arc::new(ObjectBufferStore::local(
            root.join("buffer"),
            Duration::from_secs(3),
        )?),
        WriterConfig {
            prefix: "echo".into(),
            migration_id: "echo".into(),
            stream: "echo".into(),
            epoch: 1,
            segment_bytes: 1_000_000,
            segment_transactions: 100,
            segment_age: Duration::from_secs(1),
            max_transaction_bytes: 1_000_000,
        },
        order.clone(),
        std::sync::mpsc::channel().0,
        root.join("scratch"),
    )
    .await?;
    let mut capture = MysqlCapture::new(&cfg, source, start, &tables, writer, 6491).await?;
    // Each round's applier and ordinary writer run concurrently with capture.
    // Keep the collector alive across KILL so reconnect uses delivered progress.
    let collect = async {
        let mut users = BTreeSet::new();
        let mut previous = None;
        let mut progress = 0;
        loop {
            let batch = capture.step().await?;
            assert!(!batch.stop);
            assert!(batch.watermarks.is_empty());
            if let Some(p) = &batch.durable_position {
                if let Some(old) = &previous {
                    assert_ne!(p.compare(old, order.as_ref())?, std::cmp::Ordering::Less);
                }
                previous = Some(p.clone());
            }
            capture.buffered_marker(&batch)?;
            if let Some(txn) = batch.transaction {
                assert_eq!(txn.events.len(), 1);
                let v = &txn.events[0].after.as_ref().unwrap()["id"];
                let Value::Int8(n) = Value::from_canonical_bytes(v.bytes.as_ref().unwrap())? else {
                    panic!("expected bigint");
                };
                assert!((1001..=1020).contains(&n) || n == 9999, "echo leaked: {n}");
                assert!(users.insert(n), "duplicate ordinary transaction: {n}");
                if n == 9999 {
                    assert_eq!(users.len(), 21);
                    assert!(progress >= 21);
                    break;
                }
            } else {
                progress += 1;
            }
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    let writes = async {
        for n in 1..=20 {
            let txn = apply_txn(n, &type_id);
            let apply = applier.apply(&txn);
            let user = admin
                .conn
                .query_drop(format!("INSERT INTO type_zoo.echo VALUES({})", 1000 + n));
            let (a, u) = tokio::join!(apply, user);
            a?;
            u?;
            if n == 10 {
                let ids:Vec<u32>=admin.conn.query("SELECT ID FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%'").await?;
                assert_eq!(ids.len(), 1);
                admin
                    .conn
                    .query_drop(format!("KILL CONNECTION {}", ids[0]))
                    .await?;
            }
        }
        // Real receipt-GC and progress-only writes must not surface as empty txns.
        admin
            .conn
            .query_drop("DELETE FROM _skedaddle.apply_receipts")
            .await?;
        admin
            .conn
            .query_drop("UPDATE _skedaddle.apply_progress SET last_position=0x01")
            .await?;
        let status = admin.status().await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        admin
            .conn
            .query_drop("INSERT INTO type_zoo.echo VALUES(9999)")
            .await?;
        Ok::<_, Box<dyn std::error::Error>>(status)
    };
    let (_, end) = tokio::time::timeout(Duration::from_secs(90), async {
        tokio::try_join!(collect, writes)
    })
    .await??;
    // The final user commit is later than the suppressed GC commits.
    let end_position = if end.gtid_on {
        MysqlPosition::Gtid {
            set: end.executed,
            server_uuid: end.server_uuid,
            file: end.file,
            offset: end.offset,
        }
    } else {
        MysqlPosition::File {
            server_uuid: end.server_uuid,
            file_seq: end.file.rsplit('.').next().unwrap().parse()?,
            offset: end.offset,
        }
    };
    let cp = capture.writer_checkpoint()?.unwrap();
    assert_eq!(cp.redelivery.len(), 21);
    let last = cp.last_position.unwrap();
    assert_eq!(
        last.compare(
            &Position::new(order.domain(), end_position.encode()?),
            order.as_ref()
        )?,
        std::cmp::Ordering::Greater
    );
    drop((capture, applier));
    admin.conn.disconnect().await?;
    std::fs::remove_dir_all(root)?;
    Ok(())
}
