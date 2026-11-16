use super::*;
use crate::mysql::{
    binlog::{BinlogDecoder, MysqlColumn, MysqlTable, MysqlValue as V},
    schema::{Collation, ColumnDescriptor},
    value::{from_opaque, to_opaque},
};
use std::{cmp::Ordering, io::Cursor, path::PathBuf};
const UUID: &str = "426f5bf9-e122-11f1-a0ab-0242ac120002";
fn column(name: &str, ordinal: u64, column_type: &str) -> ColumnDescriptor {
    let data_type = column_type.split(['(', ' ']).next().unwrap().to_owned();
    let character = matches!(
        data_type.as_str(),
        "char" | "varchar" | "text" | "mediumtext" | "enum" | "set"
    );
    ColumnDescriptor {
        name: name.into(),
        ordinal,
        data_type,
        column_type: column_type.into(),
        character_set: character.then(|| "utf8mb4".into()),
        collation: character.then(|| Collation {
            name: "utf8mb4_0900_ai_ci".into(),
            id: 255,
        }),
        nullable: name != "id",
        generated: false,
    }
}
fn descriptors() -> Vec<TableDescriptor> {
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
    vec![
        TableDescriptor {
            schema: "type_zoo".into(),
            table: "zoo".into(),
            columns: specs
                .iter()
                .enumerate()
                .map(|(i, (n, t))| column(n, i as u64 + 1, t))
                .collect(),
            primary_key: vec!["id".into()],
        },
        TableDescriptor {
            schema: "_skedaddle".into(),
            table: "watermark".into(),
            columns: vec![
                column("id", 1, "bigint"),
                column("marker", 2, "varchar(128)"),
            ],
            primary_key: vec!["id".into()],
        },
    ]
}
fn assembler(gtid: bool, cap: usize) -> Assembler {
    let start = if gtid {
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
    };
    Assembler::new(
        SourceIdentity {
            engine: "mysql".into(),
            system_id: "fixture-cluster".into(),
            lineage: "test".into(),
            db_name: "type_zoo".into(),
            stream: "zoo".into(),
        },
        descriptors(),
        start,
        gtid,
        cap,
    )
    .unwrap()
}
fn at(event: M, end: u64) -> PositionedEvent {
    PositionedEvent {
        event,
        file: "mysql-bin.000004".into(),
        log_pos: end,
    }
}
fn push(a: &mut Assembler, event: M) -> Result<Vec<AssemblerOutput>> {
    a.push(at(event, 100))
}
fn begin(a: &mut Assembler) {
    push(
        a,
        M::AnonymousGtid {
            sid: UUID.into(),
            sequence: 0,
        },
    )
    .unwrap();
    push(
        a,
        M::Query {
            schema: vec![],
            sql: b"BEGIN".to_vec(),
            kind: QueryKind::Begin,
        },
    )
    .unwrap();
}
fn map(n: usize) -> M {
    M::TableMap(MysqlTable {
        id: 1,
        schema: b"type_zoo".to_vec(),
        table: b"zoo".to_vec(),
        columns: (0..n)
            .map(|_| MysqlColumn {
                name: vec![],
                column_type: 0,
                metadata: vec![],
                unsigned: None,
                collation: None,
                nullable: true,
                labels: None,
            })
            .collect(),
        full_metadata: vec![],
    })
}
fn roundtrips(event: &M, maps: &mut BTreeMap<u64, TableDescriptor>, ds: &[TableDescriptor]) {
    match event {
        M::TableMap(t) => {
            if let Some(d) = ds
                .iter()
                .find(|d| d.schema.as_bytes() == t.schema && d.table.as_bytes() == t.table)
            {
                maps.insert(t.id, d.clone());
            }
        }
        M::Rows {
            table_id, images, ..
        } => {
            for (before, after) in images {
                if let Some(d) = maps.get(table_id) {
                    for r in before.iter().chain(after.iter()) {
                        for (c, v) in d.columns.iter().zip(r) {
                            let opaque = to_opaque(c, v).unwrap();
                            assert_eq!(from_opaque(c, &opaque).unwrap(), *v, "{}", c.name);
                        }
                    }
                }
            }
        }
        M::TransactionPayload(events) => {
            for e in events {
                roundtrips(e, maps, ds);
            }
        }
        _ => {}
    }
}
fn replay(variant: &str) {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/mysql")
        .join(variant);
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("manifest.json")).unwrap()).unwrap();
    let filename = manifest[0]["file"].as_str().unwrap();
    let workload = std::fs::read_to_string(root.join("workload.sql")).unwrap();
    assert_eq!(workload.matches("START TRANSACTION").count(), 2);
    let bytes = std::fs::read(root.join(format!("{filename}.binlog"))).unwrap();
    let mut decoder = BinlogDecoder::new(16 * 1024 * 1024);
    let mut input = Cursor::new(&bytes[4..]);
    let mut a = assembler(variant.ends_with("-on"), 1024 * 1024);
    let mut outputs = Vec::new();
    let mut maps = BTreeMap::new();
    let mut payload_end = None;
    while let Some(e) = decoder.read_positioned(&mut input, filename).unwrap() {
        if matches!(&e.event, M::TransactionPayload(_)) {
            payload_end = Some(e.log_pos);
        }
        roundtrips(&e.event, &mut maps, &descriptors());
        outputs.extend(a.push(e).unwrap());
    }
    a.finish().unwrap();
    assert_eq!(outputs.len(), 3);
    let AssemblerOutput::Ddl {
        schema,
        sql,
        position,
    } = &outputs[0]
    else {
        panic!("DDL first")
    };
    // The workload uses a qualified name without selecting a default database;
    // every manifest's QUERY_DDL_EVENT.bin has an empty schema field.
    assert_eq!(schema, b"");
    let ddl = workload
        .lines()
        .find(|line| line.starts_with("CREATE TABLE"))
        .unwrap()
        .trim_end_matches(';');
    assert_eq!(sql, ddl.as_bytes());
    let mut previous = position.encode().unwrap();
    let mut txns = Vec::new();
    for output in &outputs[1..] {
        let AssemblerOutput::Txn { txn, control_rows } = output else {
            panic!("transaction")
        };
        assert_eq!(
            a.order
                .compare(&previous, txn.commit_position.bytes())
                .unwrap(),
            Ordering::Less
        );
        previous = txn.commit_position.bytes().to_vec();
        assert_eq!(
            AssembledTxn::from_canonical_bytes(&txn.canonical_bytes().unwrap()).unwrap(),
            *txn
        );
        let commit = MysqlPosition::decode(txn.commit_position.bytes()).unwrap();
        match commit {
            MysqlPosition::Gtid {
                set, server_uuid, ..
            } => {
                assert_eq!(server_uuid, UUID); // identity comes from the caller
                assert!(
                    set.contains(std::str::from_utf8(&txn.txn.0).unwrap())
                        .unwrap()
                );
            }
            MysqlPosition::File {
                server_uuid,
                file_seq,
                offset,
            } => {
                assert_eq!(server_uuid, UUID);
                assert_eq!(txn.txn.0, format!("{file_seq}:{offset}").into_bytes());
            }
        }
        assert_eq!(control_rows.len(), usize::from(txns.is_empty()));
        if let Some(control) = control_rows.first() {
            assert_eq!(control.table, "_skedaddle.watermark");
            assert_eq!(control.kind, EventKind::Insert);
            assert_eq!(
                control.after.as_ref().unwrap()["id"].bytes.as_deref(),
                Some(&b"99"[..])
            );
        }
        for (i, e) in txn.events.iter().enumerate() {
            assert_eq!(e.ordinal_in_txn, i as u64);
            assert_eq!(e.position, txn.commit_position);
            assert_eq!(e.txn, txn.txn);
            assert_eq!(e.schema_version, 1);
            assert_eq!(e.key["id"].bytes.as_deref(), Some(&b"2"[..]));
        }
        txns.push(txn);
    }
    let first = &txns[0].events;
    let compressed = &txns[1].events;
    assert_eq!(
        first.iter().map(|e| &e.kind).collect::<Vec<_>>(),
        [&EventKind::Insert, &EventKind::Update, &EventKind::Delete]
    );
    assert_eq!(
        compressed.iter().map(|e| &e.kind).collect::<Vec<_>>(),
        [&EventKind::Insert, &EventKind::Delete]
    );
    assert_eq!(first[0].before, None);
    assert_eq!(first[2].after, None);
    assert_eq!(first[0].after, first[1].before);
    assert_eq!(first[1].after, first[2].before);
    let initial = first[0].after.as_ref().unwrap();
    let updated = first[1].after.as_ref().unwrap();
    assert_eq!(initial["nullable_v"].bytes, None);
    assert_eq!(
        updated["nullable_v"].bytes.as_deref(),
        Some(&b"nonnull"[..])
    );
    assert_eq!(initial["wide_row"].bytes.as_ref().unwrap().len(), 20480);
    assert_eq!(initial.len(), 30);
    assert_eq!(
        initial["u64"].bytes.as_deref(),
        Some(&b"18446744073709551615"[..])
    );
    assert_eq!(
        initial["ts"].bytes.as_deref(),
        Some(&b"2024-02-29 23:59:59.654321"[..])
    );
    assert_eq!(compressed[0].after, first[0].after);
    assert_eq!(compressed[1].before, first[0].after);
    let p = MysqlPosition::decode(txns[1].commit_position.bytes()).unwrap();
    assert!(match p {
        MysqlPosition::Gtid { offset, .. } | MysqlPosition::File { offset, .. } =>
            Some(offset) == payload_end,
    });
}
#[test]
fn mysql80_gtid_on() {
    replay("mysql80-gtid-on");
}
#[test]
fn mysql80_gtid_off() {
    replay("mysql80-gtid-off");
}
#[test]
fn mysql84_gtid_on() {
    replay("mysql84-gtid-on");
}
#[test]
fn mysql84_gtid_off() {
    replay("mysql84-gtid-off");
}
#[test]
fn typed_failures_and_poison() {
    let mut a = assembler(false, 1024);
    begin(&mut a);
    assert!(matches!(
        push(
            &mut a,
            M::Rows {
                table_id: 9,
                kind: RowKind::Write,
                images: vec![]
            }
        ),
        Err(MysqlError::Assembly("ROWS without table map"))
    ));
    assert!(matches!(
        push(&mut a, M::Xid(1)),
        Err(MysqlError::Assembly("assembler poisoned"))
    ));
    assert!(a.finish().is_err());
    let mut a = assembler(false, 1024);
    assert!(matches!(
        push(&mut a, map(1)),
        Err(MysqlError::Assembly("table map column count"))
    ));
    let mut a = assembler(false, 1024);
    assert!(matches!(
        push(
            &mut a,
            M::Rows {
                table_id: 1,
                kind: RowKind::Write,
                images: vec![]
            }
        ),
        Err(MysqlError::Assembly("ROWS outside transaction"))
    ));
    let mut a = assembler(false, 1024);
    assert!(matches!(
        push(
            &mut a,
            M::Query {
                schema: vec![],
                sql: b"XA START 'x'".to_vec(),
                kind: QueryKind::Other
            }
        ),
        Err(MysqlError::Assembly("XA unsupported"))
    ));
    let mut a = assembler(true, 1024);
    assert!(matches!(
        push(
            &mut a,
            M::Gtid {
                sid: UUID.into(),
                sequence: 1,
                tag: Some("tag".into())
            }
        ),
        Err(MysqlError::Assembly("tagged GTID"))
    ));
    let mut a = assembler(false, 1024);
    begin(&mut a);
    assert!(matches!(
        a.finish(),
        Err(MysqlError::Assembly(
            "incomplete or failed transaction stream"
        ))
    ));
}
#[test]
fn limits_progress_control_only_and_commit_query() {
    let mut a = assembler(false, 1024);
    begin(&mut a);
    assert!(matches!(
        push(&mut a, M::Xid(1)).unwrap().as_slice(),
        [AssemblerOutput::Progress(MysqlPosition::File {
            file_seq: 4,
            offset: 100,
            ..
        })]
    ));
    a.finish().unwrap();
    let d = descriptors().remove(1);
    let control_map = || {
        let M::TableMap(mut t) = map(2) else {
            unreachable!()
        };
        t.schema = b"_skedaddle".to_vec();
        t.table = b"watermark".to_vec();
        M::TableMap(t)
    };
    let images = || {
        vec![(
            None,
            Some(vec![
                V::Signed(9),
                V::Text {
                    bytes: b"marker".to_vec(),
                    collation: 255,
                },
            ]),
        )]
    };
    let mut a = assembler(false, 1024);
    begin(&mut a);
    push(&mut a, control_map()).unwrap();
    push(
        &mut a,
        M::Rows {
            table_id: 1,
            kind: RowKind::Write,
            images: images(),
        },
    )
    .unwrap();
    let output = push(
        &mut a,
        M::Query {
            schema: vec![],
            sql: b"COMMIT".to_vec(),
            kind: QueryKind::Commit,
        },
    )
    .unwrap();
    let [AssemblerOutput::Txn { txn, control_rows }] = output.as_slice() else {
        panic!("control only")
    };
    assert!(txn.events.is_empty());
    assert_eq!(txn.txn.0, b"4:100");
    assert_eq!(control_rows.len(), 1);
    assert_eq!(
        control_rows[0].after.as_ref().unwrap()["marker"],
        to_opaque(
            &d.columns[1],
            &V::Text {
                bytes: b"marker".to_vec(),
                collation: 255
            }
        )
        .unwrap()
    );
    let mut a = assembler(false, 1);
    begin(&mut a);
    push(&mut a, control_map()).unwrap();
    assert!(matches!(
        push(
            &mut a,
            M::Rows {
                table_id: 1,
                kind: RowKind::Write,
                images: images()
            }
        ),
        Err(MysqlError::TransactionTooLarge { limit: 1 })
    ));
    assert!(a.finish().is_err());
}
#[test]
fn canonical_values_reject_mismatches() {
    let c = column("n", 1, "bigint unsigned");
    assert!(matches!(
        to_opaque(&c, &V::Signed(1)),
        Err(MysqlError::Value(_))
    ));
    let mut opaque = to_opaque(&c, &V::Unsigned(1)).unwrap();
    opaque.bytes = Some(b"01".to_vec());
    assert!(from_opaque(&c, &opaque).is_err());
    opaque.type_id = "mysql.v2:bigint unsigned".into();
    assert!(from_opaque(&c, &opaque).is_err());
    let c = column("t", 1, "datetime(3)");
    assert!(to_opaque(&c, &V::DateTime(2024, 1, 1, 1, 1, 1, 1)).is_err());
    for us in [0, 1000, 999000] {
        let v = V::DateTime(2024, 1, 1, 1, 1, 1, us);
        assert_eq!(from_opaque(&c, &to_opaque(&c, &v).unwrap()).unwrap(), v);
    }
    let c = column("e", 1, "enum('a','b')");
    assert!(
        to_opaque(
            &c,
            &V::Enum {
                ordinal: 1,
                label: Some(b"b".to_vec())
            }
        )
        .is_err()
    );
    let c = column("s", 1, "set('a','b')");
    assert!(
        to_opaque(
            &c,
            &V::Set {
                bitmap: 3,
                labels: Some(vec![b"b".to_vec(), b"a".to_vec()])
            }
        )
        .is_err()
    );
}
