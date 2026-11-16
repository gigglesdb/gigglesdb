use super::*;
use crate::{
    mapping::{Numeric, Value},
    mysql::{chunk_key::MysqlKeyDirection, schema::Collation},
    pg::chunk::ChunkId,
};
fn column(name: &str, kind: &str, full: &str, cs: Option<&str>) -> MysqlKeyColumn {
    let mut c = MysqlKeyColumn {
        name: name.into(),
        data_type: kind.into(),
        column_type: full.into(),
        character_set: cs.map(Into::into),
        collation: cs.map(|_| Collation {
            name: "utf8mb4_0900_ai_ci".into(),
            id: 255,
        }),
        type_id: String::new(),
        direction: MysqlKeyDirection::Asc,
    };
    c.type_id = mysql::plan_column(&descriptor(&c, 0)).unwrap().type_id;
    c
}
fn table(columns: Vec<MysqlKeyColumn>) -> MysqlTableKey {
    MysqlTableKey {
        event_table: "test".into(),
        schema: "d`b".into(),
        table: "t`b".into(),
        columns,
    }
}
fn range(lo: Option<Key>, hi: Option<Key>) -> ChunkRange {
    ChunkRange {
        id: ChunkId {
            table: "test".into(),
            plan: "a".repeat(64),
            ordinal: 0,
        },
        lo,
        hi,
    }
}
#[test]
fn composite_predicates_quoting_collations_and_stride_share_order() {
    let key = table(vec![
        column("t`x", "varchar", "varchar(12)", Some("utf8mb4")),
        column("u", "bigint", "bigint unsigned", None),
    ]);
    let text = mysql::forward(
        &mappings(&key).unwrap()[0],
        &MysqlValue::Text {
            bytes: b"A".to_vec(),
            collation: 255,
        },
    )
    .unwrap();
    let n = Value::Numeric(Numeric::parse("18446744073709551615").unwrap());
    let q = key
        .read_query_columns(
            &range(Some(vec![text.clone(), n.clone()]), Some(vec![text, n])),
            &key.columns
                .iter()
                .enumerate()
                .map(|(i, c)| descriptor(c, i))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    assert!(q.sql.contains("FROM `d``b`.`t``b` WHERE ((`t``x` COLLATE `utf8mb4_0900_ai_ci`), `u`) >= ((CONVERT(? USING `utf8mb4`) COLLATE `utf8mb4_0900_ai_ci`), CAST(? AS UNSIGNED)) AND"),"{}",q.sql);
    assert!(q.sql.contains(" < ((CONVERT(? USING `utf8mb4`)"));
    assert!(
        q.sql
            .ends_with("ORDER BY (`t``x` COLLATE `utf8mb4_0900_ai_ci`) ASC, `u` ASC")
    );
    assert_eq!(
        q.parameters,
        vec![
            Parameter::Bytes(b"A".to_vec()),
            Parameter::UInt(u64::MAX),
            Parameter::Bytes(b"A".to_vec()),
            Parameter::UInt(u64::MAX)
        ]
    );
    let sample = key.sampling_query(3).unwrap();
    assert!(sample.sql.contains(
        "ROW_NUMBER() OVER (ORDER BY (`t``x` COLLATE `utf8mb4_0900_ai_ci`) ASC, `u` ASC)"
    ));
    assert!(sample.sql.contains("`__rn` > 1 AND (`__rn` - 1) % ? = 0"));
    assert_eq!(sample.parameters, vec![Parameter::UInt(3)]);
    assert!(key.sampling_query(0).is_err());
}
#[test]
fn inverse_parameters_keep_signed_unsigned_decimal_binary_and_temporal_types() {
    for (c, source, expected, sql) in [
        (
            column("i", "bigint", "bigint", None),
            MysqlValue::Signed(-9),
            Parameter::Int(-9),
            "CAST(? AS SIGNED)",
        ),
        (
            column("u", "bigint", "bigint unsigned", None),
            MysqlValue::Unsigned(u64::MAX),
            Parameter::UInt(u64::MAX),
            "CAST(? AS UNSIGNED)",
        ),
        (
            column("d", "decimal", "decimal(20,2)", None),
            MysqlValue::Decimal("12345678901234567.89".into()),
            Parameter::Bytes(b"12345678901234567.89".to_vec()),
            "CAST(? AS DECIMAL(20,2))",
        ),
        (
            column("b", "binary", "binary(3)", None),
            MysqlValue::Binary(vec![0, 255, 0]),
            Parameter::Bytes(vec![0, 255, 0]),
            "CAST(? AS BINARY)",
        ),
        (
            column("b", "varbinary", "varbinary(3)", None),
            MysqlValue::Binary(vec![0, 255]),
            Parameter::Bytes(vec![0, 255]),
            "CAST(? AS BINARY)",
        ),
        (
            column("d", "date", "date", None),
            MysqlValue::DateTime(2025, 1, 2, 0, 0, 0, 0),
            Parameter::Date(2025, 1, 2, 0, 0, 0, 0),
            "CAST(? AS DATE)",
        ),
        (
            column("d", "datetime", "datetime(6)", None),
            MysqlValue::DateTime(2025, 1, 2, 3, 4, 5, 123456),
            Parameter::Date(2025, 1, 2, 3, 4, 5, 123456),
            "CAST(? AS DATETIME(6))",
        ),
        (
            column("d", "timestamp", "timestamp(0)", None),
            MysqlValue::Timestamp("1735787045".into()),
            Parameter::Bytes(b"2025-01-02 03:04:05".to_vec()),
            "CAST(? AS DATETIME(0))",
        ),
    ] {
        let key = table(vec![c]);
        let p = mappings(&key).unwrap();
        let v = mysql::forward(&p[0], &source).unwrap();
        let (s, b) = parameters(&key, &vec![v]).unwrap();
        assert_eq!(s, vec![sql]);
        assert_eq!(b, vec![expected]);
    }
}
#[test]
fn unsupported_and_foreign_mapping_keys_fail_closed() {
    for kind in ["enum", "set", "json", "float", "double"] {
        let mut key = table(vec![column("i", "int", "int", None)]);
        key.columns[0].data_type = kind.into();
        assert!(matches!(
            key.validate(),
            Err(MysqlKeyError::UnsupportedKeyType { .. })
        ));
        assert!(key.sampling_query(2).is_err());
    }
    let mut key = table(vec![column("i", "int", "int", None)]);
    key.columns[0].type_id = "foreign".into();
    assert!(key.sampling_query(2).is_err());
    let key = table(vec![column("i", "int", "int", None)]);
    assert!(parameters(&key, &vec![Value::Null]).is_err());
    assert!(parameters(&key, &vec![]).is_err());
    assert!(key.read_query_columns(&range(None, None), &[]).is_err());
}
#[test]
fn adjacent_comparison_casts_both_sides_on_source() {
    let key = table(vec![column("u", "bigint", "bigint unsigned", None)]);
    let q = key
        .adjacent_query(
            &vec![Value::Numeric(
                Numeric::parse("9223372036854775808").unwrap(),
            )],
            &vec![Value::Numeric(
                Numeric::parse("9223372036854775809").unwrap(),
            )],
        )
        .unwrap();
    assert_eq!(q.sql, "SELECT CAST(? AS UNSIGNED) < CAST(? AS UNSIGNED)");
    assert_eq!(
        q.parameters,
        vec![
            Parameter::UInt(9223372036854775808),
            Parameter::UInt(9223372036854775809)
        ]
    );
}
