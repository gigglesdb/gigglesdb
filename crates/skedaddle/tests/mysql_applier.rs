use mysql_async::prelude::Queryable;
use skedaddle::{
    buffer::{
        backends::ObjectBufferStore,
        format,
        reader::{BufferReader, ReaderCheckpoint, ReaderConfig},
        writer::{BufferWriter, WriterConfig},
    },
    coordinator::{epoch::Coordinator, journal::Journal, local::Local},
    engine::{Applier, Engine, decode_reader_checkpoint},
    event::{
        AssembledTxn, Event, EventError, EventKind, OpaqueValue, Position, PositionOrder, Row,
        SourceIdentity, TxnId,
    },
    mapping::{Value, mysql::plan_column},
    mysql::{
        apply::{ApplyError, MysqlApplier, install_apply_tables, require_binlogging},
        chunk::map_row,
        client::MysqlConfig,
        schema::{TableDescriptor, load_descriptors},
    },
};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant},
};
struct Order;
impl PositionOrder for Order {
    fn domain(&self) -> &str {
        "adapter-test"
    }
    fn compare(&self, a: &[u8], b: &[u8]) -> Result<Ordering, EventError> {
        Ok(a.cmp(b))
    }
}
fn source() -> SourceIdentity {
    SourceIdentity {
        engine: "pg".into(),
        system_id: "test".into(),
        lineage: "one".into(),
        db_name: "db".into(),
        stream: "adapter".into(),
    }
}
fn txn(n: u64, events: Vec<Event>) -> AssembledTxn {
    let mut events = events;
    for (i, e) in events.iter_mut().enumerate() {
        e.source = source();
        e.txn = TxnId(n.to_be_bytes().to_vec());
        e.position = Position::new("adapter-test", n.to_be_bytes());
        e.ordinal_in_txn = i as u64;
    }
    AssembledTxn::new(
        source(),
        TxnId(n.to_be_bytes().to_vec()),
        Position::new("adapter-test", n.to_be_bytes()),
        None,
        events,
    )
    .unwrap()
}
fn event(table: &str, kind: EventKind, row: Row) -> Event {
    Event {
        source: source(),
        txn: TxnId(vec![]),
        ordinal_in_txn: 0,
        position: Position::new("adapter-test", vec![]),
        schema_version: 1,
        kind,
        table: Some(table.into()),
        key: BTreeMap::from([("id".into(), row["id"].clone())]),
        before: None,
        after: Some(row),
        unchanged_cols: BTreeSet::new(),
    }
}
fn id(row: &Row, d: &TableDescriptor, n: i64) -> Row {
    let mut row = row.clone();
    let p = plan_column(&d.columns[0]).unwrap();
    row.insert(
        "id".into(),
        OpaqueValue {
            type_id: p.type_id,
            bytes: Some(Value::Int8(n).canonical_bytes().unwrap()),
        },
    );
    row
}
async fn read(
    c: &mut mysql_async::Conn,
    d: &TableDescriptor,
    n: i64,
) -> Result<Row, Box<dyn std::error::Error>> {
    let projection = d
        .columns
        .iter()
        .map(|c| {
            if matches!(c.data_type.as_str(), "float" | "double") {
                format!("`{}`", c.name)
            } else {
                format!("CAST(`{}` AS BINARY)", c.name)
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    let r: mysql_async::Row = c
        .exec_first(
            format!(
                "SELECT {projection} FROM `{}`.`{}` WHERE id=?",
                d.schema, d.table
            ),
            (n,),
        )
        .await?
        .unwrap();
    Ok(map_row(&d.columns, r)?)
}

#[tokio::test]
async fn restricted_adapter_checkpoint_restart_and_binlog_refusal()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(path) = std::env::var_os("SKEDADDLE_MYSQL_CONFIG") else {
        eprintln!("MYSQL SKIPPED: SKEDADDLE_MYSQL_CONFIG absent; use harness");
        return Ok(());
    };
    let mut config: MysqlConfig = serde_json::from_slice(&std::fs::read(path)?)?;
    config.user = "root".into();
    config.password = "skedaddle".into();
    install_apply_tables(&config).await?;
    let mut admin = config.connect().await?;
    admin
        .conn
        .query_drop("CREATE TABLE type_zoo.adapter_target LIKE type_zoo.zoo")
        .await?;
    let target = load_descriptors(
        &mut admin.conn,
        &[("type_zoo".into(), "adapter_target".into())],
    )
    .await?
    .pop()
    .unwrap();
    let zoo = load_descriptors(&mut admin.conn, &[("type_zoo".into(), "zoo".into())])
        .await?
        .pop()
        .unwrap();
    let row = read(&mut admin.conn, &zoo, 1).await?;
    admin
        .conn
        .query_drop("CREATE USER 'adapter_apply'@'%' IDENTIFIED BY 'adapter-test'")
        .await?;
    for table in [
        "type_zoo.adapter_target",
        "_skedaddle.apply_progress",
        "_skedaddle.apply_receipts",
    ] {
        admin
            .conn
            .query_drop(format!(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON {table} TO 'adapter_apply'@'%'"
            ))
            .await?;
    }
    admin.conn.query_drop("SET SESSION sql_log_bin=0").await?;
    assert!(matches!(
        require_binlogging(&mut admin.conn).await,
        Err(ApplyError::BinlogDisabled)
    ));
    admin.conn.query_drop("SET SESSION sql_log_bin=1").await?;
    config.user = "adapter_apply".into();
    config.password = "adapter-test".into();
    let root = std::path::PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").unwrap())
        .join(format!("mysql-adapter-{}", uuid::Uuid::new_v4()));
    for dir in ["buffer", "journal", "scratch"] {
        std::fs::create_dir_all(root.join(dir))?;
    }
    let store = Arc::new(ObjectBufferStore::local(
        root.join("buffer"),
        Duration::from_secs(3),
    )?);
    let journal = Journal::new(
        Arc::new(skedaddle::coordinator::journal::local_store(
            root.join("journal"),
        )?),
        "adapter",
        "test",
        Duration::from_secs(3),
    )?;
    let mut coordinator = Coordinator::observer(
        journal.clone(),
        Local::create(root.join("local"))?,
        "adapter",
    )?;
    coordinator.acquire().await?;
    let engine = Engine::new(coordinator, journal.clone());
    let runtime = engine.runtime();
    let rc = ReaderConfig {
        prefix: "adapter".into(),
        migration_id: "test".into(),
        stream: "adapter".into(),
        limits: format::Limits::default(),
        required_next_mseq: 0,
    };
    let (tx, _) = std::sync::mpsc::channel();
    let mut writer = BufferWriter::open_with_scratch(
        store.clone(),
        WriterConfig {
            prefix: rc.prefix.clone(),
            migration_id: rc.migration_id.clone(),
            stream: rc.stream.clone(),
            epoch: 1,
            segment_bytes: 1_000_000,
            segment_transactions: 100,
            segment_age: Duration::from_secs(1),
            max_transaction_bytes: 1_000_000,
        },
        Arc::new(Order),
        tx,
        root.join("scratch"),
    )
    .await?;
    for n in 1..=3 {
        writer
            .append(
                txn(
                    n,
                    vec![event(
                        "target",
                        EventKind::Insert,
                        id(&row, &target, 100 + n as i64),
                    )],
                ),
                Instant::now(),
            )
            .await?;
        if n == 2 || n == 3 {
            writer.seal().await?;
        }
    }
    let make = |checkpoint| -> Result<MysqlApplier, Box<dyn std::error::Error>> {
        Ok(MysqlApplier::new(
            config.clone(),
            source(),
            BTreeMap::from([("target".into(), target.clone())]),
        )?
        .with_reader(BufferReader::new(
            store.clone(),
            rc.clone(),
            checkpoint,
            Arc::new(Order),
        )?))
    };
    let count_receipts = |config: MysqlConfig| async move {
        let mut c = config.connect().await.unwrap();
        let n: u64 = c
            .conn
            .query_first("SELECT COUNT(*) FROM _skedaddle.apply_receipts WHERE position_domain='adapter-test'")
            .await
            .unwrap()
            .unwrap();
        c.conn.disconnect().await.unwrap();
        n
    };
    let mut a = make(ReaderCheckpoint::default())?;
    assert!(Applier::step(&mut a, &runtime).await?);
    assert_eq!(count_receipts(config.clone()).await, 1); // unfinished segment retained
    let mid = decode_reader_checkpoint(&journal.load(false).await?.progress["applied"].0)?;
    assert_eq!(mid.next_mseq, 0);
    drop(a);
    let mut a = make(mid)?;
    assert!(Applier::step(&mut a, &runtime).await?); // replay of first INSERT is a no-op
    assert_eq!(count_receipts(config.clone()).await, 1);
    assert!(Applier::step(&mut a, &runtime).await?);
    assert_eq!(count_receipts(config.clone()).await, 0);
    let persisted = journal.load(false).await?;
    let cp = decode_reader_checkpoint(&persisted.progress["applied"].0)?;
    assert_eq!(cp.next_mseq, 1);
    drop(a);
    let mut restarted = make(cp)?;
    assert!(Applier::step(&mut restarted, &runtime).await?);
    assert!(!Applier::step(&mut restarted, &runtime).await?);
    assert_eq!(count_receipts(config.clone()).await, 0);
    for n in 1..=3 {
        assert_eq!(
            read(&mut admin.conn, &target, 100 + n).await?,
            id(&row, &target, 100 + n)
        );
    }
    writer
        .append_chunk_rows(txn(4, vec![]), Instant::now())
        .await?;
    assert!(matches!(
        restarted.buffer_step(&runtime).await,
        Err(ApplyError::DestinationCopyUnsupported)
    ));
    let after_copy = decode_reader_checkpoint(&journal.load(false).await?.progress["applied"].0)?;
    assert_eq!(after_copy.next_mseq, 2);
    admin
        .conn
        .query_drop("DROP USER 'adapter_apply'@'%'")
        .await?;
    admin
        .conn
        .query_drop("DROP TABLE type_zoo.adapter_target")
        .await?;
    admin.conn.disconnect().await?;
    Ok(())
}
