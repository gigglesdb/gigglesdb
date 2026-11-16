use skedaddle::mysql::{
    binlog::{BinlogDecoder, MysqlEvent, MysqlValue as V, QueryKind, RowKind},
    client::{MysqlError, MysqlVersion},
};
use std::{collections::BTreeMap, io::Cursor, path::PathBuf};

fn zoo(row: &[V]) {
    assert_eq!(row.len(), 30);
    assert_eq!(
        &row[1..11],
        &[
            V::Signed(-128),
            V::Unsigned(255),
            V::Signed(-32768),
            V::Unsigned(65535),
            V::Signed(-8388608),
            V::Unsigned(16777215),
            V::Signed(-2147483648),
            V::Unsigned(4294967295),
            V::Signed(i64::MIN),
            V::Unsigned(u64::MAX)
        ]
    );
    assert_eq!(
        row[11],
        V::Decimal("12345678901234567890123456789012345.123456789012345678901234567890".into())
    );
    assert_eq!(row[12], V::Float(1.25));
    assert_eq!(row[13], V::Double(-1.23456789012345));
    assert_eq!(row[14], V::Bit(vec![1, 0, 1]));
    assert_eq!(row[15], V::DateTime(2024, 2, 29, 0, 0, 0, 0));
    assert_eq!(row[16], V::DateTime(2024, 2, 29, 23, 59, 59, 123456));
    assert_eq!(row[17], V::Timestamp("1709251199.654321".into()));
    assert_eq!(
        row[18],
        V::Time {
            negative: true,
            days: 34,
            hours: 22,
            minutes: 59,
            seconds: 58,
            micros: 999999
        }
    );
    assert_eq!(row[19], V::Year("2024".into()));
    for (i, expected) in [
        (20, "é😀"),
        (22, "text é 😀"),
        (29, &"wide 😀 ".repeat(2048)),
    ] {
        let V::Text { bytes, collation } = &row[i] else {
            panic!("expected text at {i}: {:?}", row[i]);
        };
        assert_eq!(bytes, expected.as_bytes());
        assert_eq!(*collation, 255);
    }
    assert_eq!(row[23], V::Binary(vec![0, 1, 2, 3, 4, 5, 6, 7]));
    assert_eq!(row[24], V::Binary(vec![0, 255, 254, 1, 2]));
    assert_eq!(
        row[25],
        V::Enum {
            ordinal: 3,
            label: Some(b"two".to_vec())
        }
    );
    assert_eq!(
        row[26],
        V::Set {
            bitmap: 5,
            labels: Some(vec![b"a".to_vec(), b"c".to_vec()])
        }
    );
    assert_eq!(
        row[27],
        V::Json(serde_json::json!({"nested":[1,true,null,"😀"]}))
    );
    let V::Text { bytes, .. } = &row[21] else {
        panic!("varchar");
    };
    if bytes == "updated 😀".as_bytes() {
        assert!(matches!(&row[28], V::Text { bytes, .. } if bytes == b"nonnull"));
    } else {
        assert_eq!(bytes, "utf8mb4 😀".as_bytes());
        assert_eq!(row[28], V::Null);
    }
}
fn observe(
    event: &MysqlEvent,
    tables: &mut BTreeMap<u64, bool>,
    images: &mut usize,
    kinds: &mut Vec<&'static str>,
) {
    let kind = match event {
        MysqlEvent::TableMap(t) => {
            let is_zoo = t.schema == b"type_zoo" && t.table == b"zoo";
            if is_zoo {
                assert_eq!(t.columns.len(), 30);
                assert_eq!(t.columns[10].name, b"u64");
                assert_eq!(t.columns[10].unsigned, Some(true));
            }
            tables.insert(t.id, is_zoo);
            "table"
        }
        MysqlEvent::Rows {
            table_id,
            kind,
            images: rows,
        } => {
            for (before, after) in rows {
                assert_eq!(
                    (before.is_some(), after.is_some()),
                    match kind {
                        RowKind::Write => (false, true),
                        RowKind::Update => (true, true),
                        RowKind::Delete => (true, false),
                    }
                );
                if tables[table_id] {
                    for row in [before, after].into_iter().flatten() {
                        zoo(row);
                        *images += 1;
                    }
                }
            }
            "rows"
        }
        MysqlEvent::TransactionPayload(events) => {
            for e in events {
                observe(e, tables, images, kinds);
            }
            "payload"
        }
        MysqlEvent::Gtid { sequence, .. } => {
            assert!(*sequence > 0);
            "gtid"
        }
        MysqlEvent::AnonymousGtid { sequence, .. } => {
            assert_eq!(*sequence, 0);
            "anon"
        }
        MysqlEvent::Query {
            kind: QueryKind::Begin,
            ..
        } => "begin",
        MysqlEvent::Query {
            kind: QueryKind::Ddl,
            ..
        } => "ddl",
        MysqlEvent::Xid(_) => "xid",
        MysqlEvent::Rotate { .. } => {
            tables.clear();
            "rotate"
        }
        MysqlEvent::Heartbeat => "heartbeat",
        _ => "other",
    };
    kinds.push(kind);
}
fn decode(variant: &str) {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/mysql")
        .join(variant);
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("manifest.json")).unwrap()).unwrap();
    let mut decoder = BinlogDecoder::new(16 * 1024 * 1024);
    let mut tables = BTreeMap::new();
    let mut images = 0;
    let mut kinds = Vec::new();
    let path = std::fs::read_dir(&root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|s| s == "binlog"))
        .unwrap();
    let bytes = std::fs::read(path).unwrap();
    let mut input = Cursor::new(&bytes[4..]);
    while let Some(event) = decoder.read(&mut input).unwrap() {
        observe(&event, &mut tables, &mut images, &mut kinds);
    }
    assert_eq!(images, 6);
    for k in [
        "begin",
        "ddl",
        "table",
        "rows",
        "xid",
        "payload",
        "rotate",
        if variant.ends_with("-on") {
            "gtid"
        } else {
            "anon"
        },
    ] {
        assert!(kinds.contains(&k), "missing {k}");
    }
    // Decode every individual wire capture in manifest order with its FULL table map.
    let mut single = BinlogDecoder::new(16 * 1024 * 1024);
    for entry in manifest.as_array().unwrap() {
        let kind = entry["type"].as_str().unwrap();
        let name = entry["capture"]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{kind}.bin"));
        let bytes = std::fs::read(root.join(name)).unwrap();
        assert!(single.read(&mut Cursor::new(bytes)).unwrap().is_some());
    }
    let mut limited = BinlogDecoder::new(1);
    limited
        .read(&mut Cursor::new(
            std::fs::read(root.join("FORMAT_DESCRIPTION_EVENT.bin")).unwrap(),
        ))
        .unwrap();
    assert!(matches!(
        limited.read(&mut Cursor::new(
            std::fs::read(root.join("TRANSACTION_PAYLOAD_EVENT.bin")).unwrap()
        )),
        Err(MysqlError::PayloadTooLarge { limit: 1 })
    ));
}
#[test]
fn mysql80_gtid_on() {
    decode("mysql80-gtid-on");
}
#[test]
fn mysql80_gtid_off() {
    decode("mysql80-gtid-off");
}
#[test]
fn mysql84_gtid_on() {
    decode("mysql84-gtid-on");
}
#[test]
fn mysql84_gtid_off() {
    decode("mysql84-gtid-off");
}
#[test]
fn versions() {
    assert_eq!(
        MysqlVersion::parse("8.0.46").unwrap().status_sql(),
        "SHOW MASTER STATUS"
    );
    assert_eq!(
        MysqlVersion::parse("8.4.13").unwrap().status_sql(),
        "SHOW BINARY LOG STATUS"
    );
    assert!(matches!(
        MysqlVersion::parse("9.0.1"),
        Err(MysqlError::UnsupportedVersion(_))
    ));
}

#[test]
fn damaged_real_event_rejects_checksum() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/mysql/mysql80-gtid-on/FORMAT_DESCRIPTION_EVENT.bin");
    let mut bytes = std::fs::read(path).unwrap();
    bytes[0] ^= 1; // Timestamp damage preserves framing and event data.
    assert!(matches!(
        BinlogDecoder::new(16 * 1024 * 1024).read(&mut Cursor::new(bytes)),
        Err(MysqlError::Invalid("binlog checksum mismatch"))
    ));
}
