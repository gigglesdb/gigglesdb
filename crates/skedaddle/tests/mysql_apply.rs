use futures::StreamExt;
use mysql_async::prelude::Queryable;
use skedaddle::{
    event::{AssembledTxn, Event, EventKind, OpaqueValue, Position, Row, SourceIdentity, TxnId},
    mapping::{Value, mysql::plan_column},
    mysql::{
        MysqlPosition,
        apply::{ApplyError, ApplyResult, CommitObserver, MysqlApplier, install_apply_tables},
        binlog::MysqlEvent,
        chunk::map_row,
        client::{MysqlConfig, MysqlError},
        schema::{TableDescriptor, load_descriptors},
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

fn source() -> SourceIdentity {
    SourceIdentity {
        engine: "pg".into(),
        system_id: "test".into(),
        lineage: "one".into(),
        db_name: "db".into(),
        stream: "reverse".into(),
    }
}
fn txn(n: u64, events: Vec<Event>) -> AssembledTxn {
    let mut events = events;
    for (i, e) in events.iter_mut().enumerate() {
        e.source = source();
        e.txn = TxnId(n.to_be_bytes().to_vec());
        e.position = Position::new("pg-test", n.to_be_bytes());
        e.ordinal_in_txn = i as u64;
    }
    AssembledTxn::new(
        source(),
        TxnId(n.to_be_bytes().to_vec()),
        Position::new("pg-test", n.to_be_bytes()),
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
        position: Position::new("pg-test", vec![]),
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
struct LoseReply {
    config: MysqlConfig,
    fired: bool,
}
#[async_trait::async_trait]
impl CommitObserver for LoseReply {
    async fn after_commit(&mut self, id: u32) -> Result<(), MysqlError> {
        assert!(!self.fired);
        self.fired = true;
        let mut admin = self.config.connect().await?;
        admin
            .conn
            .query_drop(format!("KILL CONNECTION {id}"))
            .await?;
        admin.conn.disconnect().await?;
        // Suppress the successful real COMMIT acknowledgment, then run exactly
        // the same resolution path as a socket lost before delivering it.
        Err(std::io::Error::from(std::io::ErrorKind::ConnectionReset).into())
    }
}
#[tokio::test]
async fn live_mysql_apply_replay_conflict_lost_reply_and_echo_marker()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(path) = std::env::var_os("SKEDADDLE_MYSQL_CONFIG") else {
        eprintln!(
            "MYSQL SKIPPED: SKEDADDLE_MYSQL_CONFIG absent; use harness mysql84 GTID ON / mysql80 GTID OFF"
        );
        return Ok(());
    };
    let mut config: MysqlConfig = serde_json::from_slice(&std::fs::read(path)?)?;
    // Harness grants migration capture permissions; installation/DDL and KILL
    // are performed by the harness's fixed test-only admin account.
    config.user = "root".into();
    config.password = "skedaddle".into();
    let mut admin = config.connect().await?;
    admin.conn.query_drop("SET time_zone='+00:00'").await?;
    admin.conn.query_drop("SET NAMES utf8mb4").await?;
    admin
        .conn
        .query_drop("CREATE TABLE type_zoo.apply_other LIKE type_zoo.zoo")
        .await?;
    let mut ds = load_descriptors(
        &mut admin.conn,
        &[
            ("type_zoo".into(), "zoo".into()),
            ("type_zoo".into(), "apply_other".into()),
        ],
    )
    .await?;
    let other = ds.pop().unwrap();
    let zoo = ds.pop().unwrap();
    let row = read(&mut admin.conn, &zoo, 1).await?;
    install_apply_tables(&config).await?;
    install_apply_tables(&config).await?;
    admin
        .conn
        .query_drop("CREATE USER 'm10_apply'@'%' IDENTIFIED BY 'apply-test'")
        .await?;
    for table in [
        "type_zoo.zoo",
        "type_zoo.apply_other",
        "_skedaddle.apply_progress",
        "_skedaddle.apply_receipts",
    ] {
        admin
            .conn
            .query_drop(format!(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON {table} TO 'm10_apply'@'%'"
            ))
            .await?;
    }
    let mut restricted = config.clone();
    restricted.user = "m10_apply".into();
    restricted.password = "apply-test".into();
    let mut applier = MysqlApplier::new(
        restricted,
        source(),
        BTreeMap::from([("zoo".into(), zoo.clone()), ("other".into(), other.clone())]),
    )?;
    let status = admin.status().await?;
    let pos = if status.gtid_on {
        MysqlPosition::Gtid {
            set: status.executed,
            server_uuid: status.server_uuid,
            file: status.file,
            offset: status.offset,
        }
    } else {
        MysqlPosition::File {
            server_uuid: status.server_uuid,
            file_seq: status.file.rsplit('.').next().unwrap().parse()?,
            offset: status.offset,
        }
    };
    let mut capture = config
        .connect()
        .await?
        .open_stream(&pos, 6419, Duration::from_millis(500))
        .await?;
    let t = txn(
        1,
        vec![
            event("zoo", EventKind::Insert, id(&row, &zoo, 10)),
            event("zoo", EventKind::Insert, id(&row, &zoo, 11)),
            event("other", EventKind::Insert, id(&row, &other, 20)),
        ],
    );
    assert_eq!(applier.apply(&t).await?, ApplyResult::Applied);
    assert_eq!(applier.apply(&t).await?, ApplyResult::AlreadyApplied);
    assert_eq!(read(&mut admin.conn, &zoo, 10).await?, id(&row, &zoo, 10));
    assert_eq!(read(&mut admin.conn, &zoo, 11).await?, id(&row, &zoo, 11));
    assert_eq!(
        read(&mut admin.conn, &other, 20).await?,
        id(&row, &other, 20)
    );
    let conflict = txn(1, vec![event("zoo", EventKind::Insert, id(&row, &zoo, 12))]);
    assert!(matches!(
        applier.apply(&conflict).await,
        Err(ApplyError::DigestConflict)
    ));
    let mut changed = id(&row, &zoo, 10);
    changed.insert(
        "nullable_v".into(),
        OpaqueValue {
            type_id: plan_column(zoo.columns.iter().find(|c| c.name == "nullable_v").unwrap())?
                .type_id,
            bytes: Some(
                Value::parse_pg_text(
                    &plan_column(zoo.columns.iter().find(|c| c.name == "nullable_v").unwrap())?
                        .pg_type,
                    Some(b"changed"),
                )?
                .canonical_bytes()?,
            ),
        },
    );
    let t = txn(
        2,
        vec![
            event("zoo", EventKind::Update, changed.clone()),
            event("zoo", EventKind::Delete, id(&row, &zoo, 11)),
        ],
    );
    assert_eq!(applier.apply(&t).await?, ApplyResult::Applied);
    assert_eq!(read(&mut admin.conn, &zoo, 10).await?, changed);
    let count: Option<u64> = admin
        .conn
        .query_first("SELECT COUNT(*) FROM type_zoo.zoo WHERE id=11")
        .await?;
    assert_eq!(count, Some(0));
    assert_eq!(
        applier
            .apply(&txn(
                3,
                vec![event("zoo", EventKind::Update, changed.clone())]
            ))
            .await?,
        ApplyResult::Applied
    ); // unchanged update
    let failed = txn(
        4,
        vec![
            event("other", EventKind::Insert, id(&row, &other, 21)),
            event("zoo", EventKind::Delete, id(&row, &zoo, 999)),
        ],
    );
    assert!(matches!(
        applier.apply(&failed).await,
        Err(ApplyError::RowCount { actual: 0, .. })
    ));
    let count: Option<u64> = admin
        .conn
        .query_first("SELECT COUNT(*) FROM type_zoo.apply_other WHERE id=21")
        .await?;
    assert_eq!(count, Some(0));
    let t = txn(
        5,
        vec![event("other", EventKind::Insert, id(&row, &other, 22))],
    );
    let mut loss = LoseReply {
        config: config.clone(),
        fired: false,
    };
    assert_eq!(
        applier.apply_with_observer(&t, &mut loss).await?,
        ApplyResult::ResolvedCommitted
    );
    assert!(loss.fired);
    assert_eq!(applier.apply(&t).await?, ApplyResult::AlreadyApplied);
    assert_eq!(
        read(&mut admin.conn, &other, 22).await?,
        id(&row, &other, 22)
    );
    // Decoder proof: each of the four committed source transactions contains
    // the receipt row marker before XID, including the lost acknowledgment.
    let mut receipt_id = None;
    let mut marker = false;
    let mut marked_commits = 0;
    tokio::time::timeout(Duration::from_secs(15), async {
        while marked_commits < 4 {
            match capture.next().await.unwrap().unwrap().event {
                MysqlEvent::TableMap(t)
                    if t.schema == b"_skedaddle" && t.table == b"apply_receipts" =>
                {
                    receipt_id = Some(t.id)
                }
                MysqlEvent::Rows { table_id, .. } if Some(table_id) == receipt_id => marker = true,
                MysqlEvent::Xid(_) => {
                    if marker {
                        marked_commits += 1;
                    }
                    marker = false;
                }
                MysqlEvent::TransactionPayload(_) => {
                    panic!("unexpected compressed apply transaction")
                }
                _ => {}
            }
        }
    })
    .await?;
    capture.close().await?;
    // Shape mismatch is typed, never silently accepted on reinstall.
    admin
        .conn
        .query_drop("ALTER TABLE _skedaddle.apply_receipts ADD COLUMN unexpected INT")
        .await?;
    let mismatch = install_apply_tables(&config).await;
    // The harness runs later binaries against this same server. Restore the
    // deliberately damaged shape before asserting, even if rejection regresses.
    admin
        .conn
        .query_drop("ALTER TABLE _skedaddle.apply_receipts DROP COLUMN unexpected")
        .await?;
    assert!(matches!(mismatch, Err(ApplyError::TableShape(_))));
    install_apply_tables(&config).await?;
    admin.conn.query_drop("DROP USER 'm10_apply'@'%'").await?;
    admin.conn.disconnect().await?;
    Ok(())
}
