use futures::StreamExt;
use mysql_async::{Params, prelude::Queryable};
use skedaddle::{
    coordinator::chunk::ChunkPlan,
    event::{EventKind, Row},
    mapping::{Value, mysql::plan_column},
    mysql::{
        MysqlPosition,
        assembly::ControlRow,
        binlog::{MysqlEvent, RowKind},
        chunk::{MysqlChunkError, MysqlChunks, map_row, sample_plan},
        chunk_key::{MysqlKeyColumn, MysqlKeyDirection, MysqlTableKey},
        client::MysqlConfig,
        control::{self, install_watermark_table},
        schema::{TableDescriptor, load_descriptors},
        value,
    },
    pg::chunk::{Watermark, WatermarkKind},
};
use std::{collections::BTreeSet, time::Duration};
use tokio::time::{Instant, timeout_at};
fn key(table: &TableDescriptor) -> MysqlTableKey {
    MysqlTableKey {
        event_table: format!("{}.{}", table.schema, table.table),
        schema: table.schema.clone(),
        table: table.table.clone(),
        columns: table
            .primary_key
            .iter()
            .map(|name| {
                let c = table.columns.iter().find(|c| &c.name == name).unwrap();
                MysqlKeyColumn {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    column_type: c.column_type.clone(),
                    character_set: c.character_set.clone(),
                    collation: c.collation.clone(),
                    type_id: plan_column(c).unwrap().type_id,
                    direction: MysqlKeyDirection::Asc,
                }
            })
            .collect(),
    }
}
fn row_key(key: &MysqlTableKey, row: &Row) -> Vec<Value> {
    key.columns
        .iter()
        .map(|c| Value::from_canonical_bytes(row[&c.name].bytes.as_deref().unwrap()).unwrap())
        .collect()
}
#[tokio::test]
async fn live_mysql_chunks_source_order_exact_coverage_and_committed_markers()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(path) = std::env::var_os("SKEDADDLE_MYSQL_CONFIG") else {
        eprintln!(
            "MYSQL SKIPPED: SKEDADDLE_MYSQL_CONFIG absent; use harness/run.ps1 -Phase mysql84|mysql80 -MysqlGtid ON|OFF -MysqlCargoTest '--test mysql_chunks -- --nocapture'"
        );
        return Ok(());
    };
    let cfg: MysqlConfig = serde_json::from_slice(&std::fs::read(path)?)?;
    let mut admin = cfg.connect().await?;
    admin.conn.query_drop("SET NAMES utf8mb4").await?;
    admin.conn.query_drop("SET time_zone='+00:00'").await?;
    // Create the historical VARCHAR placeholder ourselves (other test binaries
    // may already have installed the real table on this shared server); detect it.
    admin
        .conn
        .query_drop("CREATE SCHEMA IF NOT EXISTS _skedaddle")
        .await?;
    admin
        .conn
        .query_drop("DROP TABLE IF EXISTS _skedaddle.watermark")
        .await?;
    admin
        .conn
        .query_drop("CREATE TABLE _skedaddle.watermark (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, marker VARCHAR(255) NOT NULL) ENGINE=InnoDB")
        .await?;
    assert!(install_watermark_table(&mut admin.conn).await.is_err());
    admin
        .conn
        .query_drop("DROP TABLE _skedaddle.watermark")
        .await?;
    install_watermark_table(&mut admin.conn).await?;
    install_watermark_table(&mut admin.conn).await?;
    admin.conn.query_drop("CREATE TABLE type_zoo.chunk_composite (txt VARCHAR(12) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,u BIGINT UNSIGNED NOT NULL,payload VARBINARY(8),PRIMARY KEY(txt,u)) ENGINE=InnoDB").await?;
    for (t, u) in [
        ("a", 1u64),
        ("A", 2),
        ("á", 9223372036854775808),
        ("a", u64::MAX),
        ("b", 1),
        ("B", 9223372036854775809),
        ("c", u64::MAX),
    ] {
        admin
            .conn
            .exec_drop(
                "INSERT INTO type_zoo.chunk_composite VALUES(?,?,?)",
                (t, u, vec![0u8, 255]),
            )
            .await?;
    }
    let zoo_columns:Vec<String>=admin.conn.query("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='type_zoo' AND TABLE_NAME='zoo' AND COLUMN_NAME<>'id' ORDER BY ORDINAL_POSITION").await?;
    let projection = zoo_columns
        .iter()
        .map(|c| skedaddle::mysql::chunk::ident(c).unwrap())
        .collect::<Vec<_>>()
        .join(",");
    for id in 710..715 {
        admin
            .conn
            .query_drop(format!(
                "INSERT INTO type_zoo.zoo SELECT {id},{projection} FROM type_zoo.zoo WHERE id=1"
            ))
            .await?;
    }
    let tables = load_descriptors(
        &mut admin.conn,
        &[
            ("type_zoo".into(), "zoo".into()),
            ("type_zoo".into(), "chunk_composite".into()),
        ],
    )
    .await?;
    let mut chunks = MysqlChunks::connect(&cfg, tables.clone()).await?;
    for table in &tables {
        let key = key(table);
        let plan = sample_plan(&mut admin.conn, &key, 2).await?;
        assert!(plan.ranges.len() > 1);
        for pair in plan.samples.windows(2) {
            let q = key.adjacent_query(&pair[0], &pair[1])?;
            assert_eq!(
                admin
                    .conn
                    .exec_first::<u8, _, _>(q.sql, Params::Positional(q.parameters))
                    .await?,
                Some(1)
            );
        }
        let full_plan = ChunkPlan::new(key.clone(), 100, vec![])?;
        let q = key.read_query_columns(&full_plan.ranges[0], &table.columns)?;
        let full = admin
            .conn
            .exec::<mysql_async::Row, _, _>(q.sql, Params::Positional(q.parameters))
            .await?
            .into_iter()
            .map(|r| map_row(&table.columns, r))
            .collect::<Result<Vec<_>, _>>()?;
        let mut actual = Vec::new();
        let mut seen = BTreeSet::new();
        for range in &plan.ranges {
            let rows = chunks.read(&plan, range).await?;
            for row in rows {
                assert!(seen.insert(serde_json::to_vec(&row_key(&key, &row))?));
                actual.push(row);
            }
        }
        assert_eq!(
            actual, full,
            "chunk union must equal full SELECT row by row"
        );
        for pair in actual.windows(2) {
            let q = key.adjacent_query(&row_key(&key, &pair[0]), &row_key(&key, &pair[1]))?;
            assert_eq!(
                admin
                    .conn
                    .exec_first::<u8, _, _>(q.sql, Params::Positional(q.parameters))
                    .await?,
                Some(1)
            );
        }
        assert_eq!(
            admin
                .conn
                .query_first::<u8, _>("SELECT @@autocommit")
                .await?,
            Some(1)
        );
    }
    // Refusal happens before any reader SQL, with its own error variant.
    let pg = skedaddle::pg::chunk::TableKey {
        table: "pg".into(),
        relation: skedaddle::pg::chunk::QualifiedName {
            schema: "public".into(),
            name: "pg".into(),
        },
        columns: vec![skedaddle::pg::chunk::KeyColumn {
            name: "id".into(),
            pg_type: skedaddle::mapping::PgType::Int4,
            sql_type: skedaddle::pg::chunk::QualifiedName {
                schema: "pg_catalog".into(),
                name: "int4".into(),
            },
            collation: None,
            direction: skedaddle::pg::chunk::Direction::Asc,
        }],
    };
    let pg = ChunkPlan::new(pg, 2, vec![])?;
    assert!(matches!(
        chunks.read(&pg, &pg.ranges[0]).await,
        Err(MysqlChunkError::UnsupportedSource)
    ));
    let plan = sample_plan(&mut admin.conn, &key(&tables[0]), 2).await?;
    let status = admin.status().await?;
    let start = if status.gtid_on {
        MysqlPosition::Gtid {
            set: status.executed,
            server_uuid: status.server_uuid.clone(),
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
    let mut stream = cfg
        .connect()
        .await?
        .open_stream(&start, 6473, Duration::from_millis(500))
        .await?;
    let markers = [
        Watermark {
            wm: WatermarkKind::Low,
            chunk: plan.ranges[0].id.clone(),
            attempt: 1,
        },
        Watermark {
            wm: WatermarkKind::High,
            chunk: plan.ranges[0].id.clone(),
            attempt: 1,
        },
    ];
    for marker in &markers {
        chunks.emit(marker.clone()).await?;
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut watermark_id = None;
    let mut decoded = Vec::new();
    let mut commits = 0;
    while commits < 2 {
        let event = timeout_at(deadline, stream.next())
            .await?
            .ok_or("binlog ended")??;
        match event.event {
            MysqlEvent::TableMap(t) if t.schema == b"_skedaddle" && t.table == b"watermark" => {
                watermark_id = Some(t.id)
            }
            MysqlEvent::Rows {
                table_id,
                kind,
                images,
            } if Some(table_id) == watermark_id => {
                assert_eq!(kind, RowKind::Write);
                for (before, after) in images {
                    assert!(before.is_none());
                    let after = after.unwrap();
                    let d = control::descriptor();
                    let row = d
                        .columns
                        .iter()
                        .zip(after)
                        .map(|(c, v)| Ok((c.name.clone(), value::to_opaque(c, &v)?)))
                        .collect::<Result<Row, skedaddle::mysql::client::MysqlError>>()?;
                    decoded.push(
                        control::decode_watermark(&ControlRow {
                            table: "_skedaddle.watermark".into(),
                            kind: EventKind::Insert,
                            before: None,
                            after: Some(row),
                        })?
                        .unwrap(),
                    );
                }
            }
            MysqlEvent::Xid(_) => {
                assert_eq!(decoded.len(), commits + 1);
                commits += 1;
            }
            _ => {}
        }
    }
    assert_eq!(decoded, markers);
    stream.close().await?;
    chunks.disconnect().await?;
    admin.conn.disconnect().await?;
    Ok(())
}
