use futures::StreamExt;
use mysql_async::prelude::Queryable;
use skedaddle::mysql::{
    GtidSet, MysqlPosition,
    binlog::{MysqlEvent, MysqlValue as V, QueryKind, RowKind},
    client::{MysqlBinlogStream, MysqlConfig, MysqlError, MysqlVersion},
};
use std::time::Duration;
use tokio::time::{Instant, timeout, timeout_at};

const HEARTBEAT: Duration = Duration::from_millis(500);
const COLUMNS: &str = "i8,u8,i16,u16,i24,u24,i32,u32,i64,u64,decv,fv,dv,bits,d,dt,ts,tm,yr,cv,vv,txt,bv,blobv,ev,sv,jv,nullable_v,wide_row";

fn zoo(row: &[V], id: i64, updated: bool) {
    let text = |value: &str| V::Text {
        bytes: value.as_bytes().to_vec(),
        collation: 255,
    };
    assert_eq!(
        row,
        &[
            V::Signed(id),
            V::Signed(-128),
            V::Unsigned(255),
            V::Signed(-32768),
            V::Unsigned(65535),
            V::Signed(-8388608),
            V::Unsigned(16777215),
            V::Signed(-2147483648),
            V::Unsigned(4294967295),
            V::Signed(i64::MIN),
            V::Unsigned(u64::MAX),
            V::Decimal("12345678901234567890123456789012345.123456789012345678901234567890".into()),
            V::Float(1.25),
            V::Double(-1.23456789012345),
            V::Bit(vec![1, 0, 1]),
            V::DateTime(2024, 2, 29, 0, 0, 0, 0),
            V::DateTime(2024, 2, 29, 23, 59, 59, 123456),
            V::Timestamp("1709251199.654321".into()),
            V::Time {
                negative: true,
                days: 34,
                hours: 22,
                minutes: 59,
                seconds: 58,
                micros: 999999
            },
            V::Year("2024".into()),
            text("é😀"),
            text(if updated {
                "updated 😀"
            } else {
                "utf8mb4 😀"
            }),
            text("text é 😀"),
            V::Binary(vec![0, 1, 2, 3, 4, 5, 6, 7]),
            V::Binary(vec![0, 255, 254, 1, 2]),
            V::Enum {
                ordinal: 3,
                label: Some(b"two".to_vec())
            },
            V::Set {
                bitmap: 5,
                labels: Some(vec![b"a".to_vec(), b"c".to_vec()])
            },
            V::Json(serde_json::json!({"nested":[1,true,null,"😀"]})),
            if updated { text("nonnull") } else { V::Null },
            text(&"wide 😀 ".repeat(2048)),
        ]
    );
}
async fn idle(stream: &mut MysqlBinlogStream) {
    let deadline = Instant::now() + HEARTBEAT * 4;
    loop {
        match timeout_at(deadline, stream.next())
            .await
            .expect("heartbeat within four periods")
            .expect("live stream")
            .expect("decode")
        {
            MysqlEvent::Heartbeat => break,
            MysqlEvent::Rotate { .. }
            | MysqlEvent::FormatDescription { .. }
            | MysqlEvent::PreviousGtids(_) => {}
            event => panic!("unexpected replay or duplicate before heartbeat: {event:?}"),
        }
    }
}

async fn transaction(stream: &mut MysqlBinlogStream, gtid: bool, id: i64, executed: &mut GtidSet) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut kinds = Vec::new();
    let mut table = None;
    loop {
        let event = timeout_at(deadline, stream.next())
            .await
            .expect("transaction timeout")
            .expect("live stream")
            .expect("decode");
        match event {
            MysqlEvent::Gtid { sid, sequence, tag } => {
                assert!(gtid);
                assert!(tag.is_none());
                assert!(sequence > 0);
                executed.add(&format!("{sid}:{sequence}")).unwrap();
                kinds.push("gtid");
            }
            MysqlEvent::AnonymousGtid { sequence, .. } => {
                assert!(!gtid);
                assert_eq!(sequence, 0);
                kinds.push("gtid");
            }
            MysqlEvent::Query {
                kind: QueryKind::Begin,
                ..
            } => kinds.push("begin"),
            MysqlEvent::TableMap(t) => {
                assert_eq!(t.schema, b"type_zoo");
                assert_eq!(t.table, b"zoo");
                assert_eq!(t.columns.len(), 30);
                assert_eq!(t.columns[10].name, b"u64");
                assert_eq!(t.columns[10].unsigned, Some(true));
                table = Some(t.id);
                kinds.push("table");
            }
            MysqlEvent::Rows {
                table_id,
                kind,
                images,
            } => {
                assert_eq!(Some(table_id), table);
                assert_eq!(images.len(), 1);
                let (before, after) = &images[0];
                assert_eq!(
                    (before.is_some(), after.is_some()),
                    match kind {
                        RowKind::Write => (false, true),
                        RowKind::Update => (true, true),
                        RowKind::Delete => (true, false),
                    }
                );
                if let Some(row) = before {
                    zoo(row, id, kind == RowKind::Delete);
                }
                if let Some(row) = after {
                    zoo(row, id, kind == RowKind::Update);
                }
                kinds.push(match kind {
                    RowKind::Write => "write",
                    RowKind::Update => "update",
                    RowKind::Delete => "delete",
                });
            }
            MysqlEvent::Xid(xid) => {
                assert!(xid > 0);
                kinds.push("xid");
                break;
            }
            MysqlEvent::Heartbeat => {}
            event => panic!("unexpected transaction event: {event:?}"),
        }
    }
    assert_eq!(
        kinds,
        [
            "gtid", "begin", "table", "write", "table", "update", "table", "delete", "xid"
        ]
    );
}

#[tokio::test]
async fn live_binlog_tls_status_rows_heartbeat_rotate() -> Result<(), Box<dyn std::error::Error>> {
    let Some(path) = std::env::var_os("SKEDADDLE_MYSQL_CONFIG") else {
        eprintln!(
            "MYSQL SKIPPED: SKEDADDLE_MYSQL_CONFIG absent; use harness/run.ps1 -Phase mysql80|mysql84 -MysqlGtid ON|OFF -MysqlCargoTest '--test mysql_binlog_live -- --nocapture'"
        );
        return Ok(());
    };
    let config: MysqlConfig = serde_json::from_slice(&std::fs::read(path)?)?;
    let mut writer = config.connect().await?; // CA and hostname verified; cipher checked.
    assert_eq!(
        writer.version,
        if config.profile == "mysql80" {
            MysqlVersion::V80
        } else {
            MysqlVersion::V84
        }
    );
    writer.conn.query_drop("SET NAMES utf8mb4").await?;
    writer.conn.query_drop("SET time_zone='+00:00'").await?;
    let status = writer.status().await?;
    uuid::Uuid::parse_str(&status.server_uuid)?;
    assert_eq!(status.gtid_on, config.gtid == "ON");
    assert!(status.purged.is_subset(&status.executed));
    assert!(
        status
            .binary_logs
            .iter()
            .any(|(file, size)| file == &status.file && *size >= status.offset)
    );
    let gtid_position = MysqlPosition::Gtid {
        set: status.executed.clone(),
        server_uuid: status.server_uuid.clone(),
        file: status.file.clone(),
        offset: status.offset,
    };
    let file_position = MysqlPosition::File {
        server_uuid: status.server_uuid.clone(),
        file_seq: status.file.rsplit('.').next().unwrap().parse().unwrap(),
        offset: status.offset,
    };
    if !status.gtid_on {
        assert!(matches!(
            writer.binlog_request(&gtid_position, 6402, HEARTBEAT).await,
            Err(MysqlError::Invalid(
                "GTID auto-position requires gtid_mode=ON"
            ))
        ));
    } else {
        let mut tagged = gtid_position.clone();
        if let MysqlPosition::Gtid { set, .. } = &mut tagged {
            *set = format!("{}:tag:1", status.server_uuid).parse().unwrap();
        }
        assert!(matches!(
            writer.binlog_request(&tagged, 6402, HEARTBEAT).await,
            Err(MysqlError::Invalid("tagged GTID requests unsupported"))
        ));
    }
    for offset in [3, u64::from(u32::MAX) + 1] {
        let mut invalid = file_position.clone();
        if let MysqlPosition::File { offset: value, .. } = &mut invalid {
            *value = offset;
        }
        assert!(matches!(
            writer.binlog_request(&invalid, 6402, HEARTBEAT).await,
            Err(MysqlError::Invalid(
                "file offset outside COM_BINLOG_DUMP range"
            ))
        ));
    }
    let position = if status.gtid_on {
        &gtid_position
    } else {
        &file_position
    };
    let mut stream = config
        .connect()
        .await?
        .open_stream(position, 6402, HEARTBEAT)
        .await?;
    idle(&mut stream).await;
    let mut empty_stream = if status.gtid_on {
        if !status.executed.to_string().is_empty() {
            eprintln!(
                "MYSQL SUBCASE SKIPPED: fresh empty gtid_executed unavailable after harness initialization; testing an explicitly empty requested set at current status instead"
            );
        }
        let mut empty = gtid_position.clone();
        if let MysqlPosition::Gtid { set, .. } = &mut empty {
            *set = GtidSet::default();
        }
        let mut stream = config
            .connect()
            .await?
            .open_stream(&empty, 6403, HEARTBEAT)
            .await?;
        idle(&mut stream).await; // Must not replay the initialized zoo row or old GTIDs.
        Some(stream)
    } else {
        None
    };
    let mut limited_config = config.clone();
    limited_config.max_decompressed_bytes = 1;
    let mut limited = limited_config
        .connect()
        .await?
        .open_stream(position, 6404, HEARTBEAT)
        .await?;
    idle(&mut limited).await;
    for (id, compression) in [(2, "OFF"), (3, "ON")] {
        writer
            .conn
            .query_drop(format!(
                "SET SESSION binlog_transaction_compression={compression}"
            ))
            .await?;
        let enabled: u8 = writer
            .conn
            .query_first("SELECT @@session.binlog_transaction_compression")
            .await?
            .unwrap();
        assert_eq!(enabled, u8::from(compression == "ON"));
        writer.conn.query_drop("START TRANSACTION").await?;
        writer
            .conn
            .query_drop(format!(
                "INSERT INTO type_zoo.zoo SELECT {id},{COLUMNS} FROM type_zoo.zoo WHERE id=1"
            ))
            .await?;
        writer
            .conn
            .query_drop(format!(
                "UPDATE type_zoo.zoo SET vv='updated 😀',nullable_v='nonnull' WHERE id={id}"
            ))
            .await?;
        writer
            .conn
            .query_drop(format!("DELETE FROM type_zoo.zoo WHERE id={id}"))
            .await?;
        writer.conn.query_drop("COMMIT").await?;
    }
    let mut executed = status.executed.clone();
    for id in [2, 3] {
        transaction(&mut stream, status.gtid_on, id, &mut executed).await;
    }
    let mut empty_executed = GtidSet::default();
    if let Some(stream) = &mut empty_stream {
        for id in [2, 3] {
            transaction(stream, true, id, &mut empty_executed).await;
        }
        assert_eq!(empty_executed, executed.difference(&status.executed));
        idle(stream).await;
    }
    assert_eq!(writer.status().await?.executed, executed);
    transaction(
        &mut limited,
        status.gtid_on,
        2,
        &mut status.executed.clone(),
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match timeout_at(deadline, limited.next())
            .await?
            .expect("limit stream")
        {
            Err(MysqlError::PayloadTooLarge { limit: 1 }) => break,
            Ok(
                MysqlEvent::Gtid { .. } | MysqlEvent::AnonymousGtid { .. } | MysqlEvent::Heartbeat,
            ) => {}
            event => panic!("expected compressed payload limit rejection: {event:?}"),
        }
    }
    assert!(limited.next().await.is_none()); // A failed stream cannot yield more rows.
    timeout(Duration::from_secs(5), limited.close()).await??;
    idle(&mut stream).await; // Also catches double expansion of compressed rows.
    writer.conn.query_drop("FLUSH BINARY LOGS").await?;
    let rotated = writer.status().await?;
    assert_ne!(rotated.file, status.file);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let event = timeout_at(deadline, stream.next())
            .await
            .expect("rotate timeout")
            .unwrap()
            .unwrap();
        match event {
            MysqlEvent::Rotate { file, offset } => {
                assert_eq!(file, rotated.file.as_bytes());
                assert_eq!(offset, 4);
                break;
            }
            MysqlEvent::Heartbeat => {}
            event => panic!("unexpected before rotate: {event:?}"),
        }
    }
    timeout(Duration::from_secs(5), stream.close())
        .await?
        .unwrap();
    if let Some(stream) = empty_stream {
        timeout(Duration::from_secs(5), stream.close())
            .await?
            .unwrap();
    }
    writer.conn.disconnect().await?;
    eprintln!(
        "MYSQL LIVE PASSED: {} GTID {}; TLS, status, normal+compressed transactions, heartbeat, rotate",
        config.profile, config.gtid
    );
    Ok(())
}
