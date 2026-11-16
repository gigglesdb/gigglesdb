use super::*;
use crate::{
    mapping::{Numeric, mysql::plan_column},
    mysql::{
        chunk_key::{MysqlKeyColumn, MysqlKeyDirection},
        schema::Collation,
    },
    pg::chunk::ChunkId,
};

const UUID: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

#[test]
fn sampled_boundaries_never_claim_exact_snapshot_equivalence() {
    for mode in ["ON", "OFF"] {
        let boundary = sampled_boundary(
            mode,
            UUID.into(),
            &format!("{UUID}:1-9"),
            "binlog.000012".into(),
            123,
        )
        .unwrap();
        assert_eq!(
            matches!(boundary, SnapshotBoundary::UpperBound(_)),
            mode == "OFF"
        );
        assert_eq!(
            matches!(boundary, SnapshotBoundary::UncertifiedGtidSample(_)),
            mode == "ON"
        );
        let position = boundary.native();
        assert_eq!(
            MysqlPosition::decode(&position.encode().unwrap()).unwrap(),
            *position
        );
        match position {
            MysqlPosition::Gtid { set, offset, .. } => {
                assert_eq!(set.to_string(), format!("{UUID}:1-9"));
                assert_eq!(*offset, 123);
            }
            MysqlPosition::File {
                file_seq, offset, ..
            } => assert_eq!((*file_seq, *offset), (12, 123)),
        }
        assert!(matches!(
            boundary.durable_report(),
            Err(VerifyError::NonPgVerificationReportUnsupported)
        ));
    }
    assert!(sampled_boundary("ON", UUID.into(), "", "binlog.000001".into(), 4).is_ok());
}

#[test]
fn malformed_boundary_and_transitional_mode_fail_closed() {
    for (mode, uuid, gtids, file, offset) in [
        ("ON_PERMISSIVE", UUID, "", "binlog.000001", 4),
        ("OFF_PERMISSIVE", UUID, "", "binlog.000001", 4),
        ("ON", UUID, "garbage", "binlog.000001", 4),
        ("OFF", "bad", "", "binlog.000001", 4),
        ("OFF", UUID, "", "binlog", 4),
        ("OFF", UUID, "", "binlog.000000", 4),
        ("OFF", UUID, "", "binlog.000001", 3),
        ("OFF", UUID, "", "binlog.000001", u64::from(u32::MAX) + 1),
    ] {
        assert!(sampled_boundary(mode, uuid.into(), gtids, file.into(), offset).is_err());
    }
}

fn columns() -> Vec<ColumnDescriptor> {
    [
        ("text ORDER BY x", "varchar", "varchar(12)"),
        ("u", "bigint", "bigint unsigned"),
    ]
    .iter()
    .enumerate()
    .map(|(i, (name, data_type, column_type))| ColumnDescriptor {
        name: (*name).into(),
        ordinal: i as u64 + 1,
        data_type: (*data_type).into(),
        column_type: (*column_type).into(),
        character_set: (i == 0).then(|| "utf8mb4".into()),
        collation: (i == 0).then(|| Collation {
            name: "utf8mb4_0900_ai_ci".into(),
            id: 255,
        }),
        nullable: false,
        generated: false,
        column_default: None,
        default_generated: false,
        generation_expression: None,
    })
    .collect()
}

#[test]
fn keyset_pages_use_shared_bounds_collations_unsigned_and_strict_cursor() {
    let columns = columns();
    let key = MysqlTableKey {
        event_table: "db.table".into(),
        schema: "db".into(),
        table: "table".into(),
        columns: columns
            .iter()
            .map(|c| MysqlKeyColumn {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                column_type: c.column_type.clone(),
                character_set: c.character_set.clone(),
                collation: c.collation.clone(),
                type_id: plan_column(c).unwrap().type_id,
                direction: MysqlKeyDirection::Asc,
            })
            .collect(),
    };
    let text = crate::mapping::mysql::forward(
        &plan_column(&columns[0]).unwrap(),
        &crate::mysql::binlog::MysqlValue::Text {
            bytes: b"A".to_vec(),
            collation: 255,
        },
    )
    .unwrap();
    let values = vec![
        text,
        Value::Numeric(Numeric::parse("18446744073709551615").unwrap()),
    ];
    let range = RangePlan {
        id: ChunkId {
            table: key.event_table.clone(),
            plan: "a".repeat(64),
            ordinal: 0,
        },
        lo: Some(values.clone()),
        hi: Some(values.clone()),
    };
    let query = key
        .page_query_columns(&range, &columns, Some(&values), 2)
        .unwrap();
    assert!(query.sql.contains(" >= "));
    assert!(query.sql.contains(" < "));
    assert!(query.sql.contains(" AND ((`text ORDER BY x` COLLATE `utf8mb4_0900_ai_ci`), `u`) > ((CONVERT(? USING `utf8mb4`) COLLATE `utf8mb4_0900_ai_ci`), CAST(? AS UNSIGNED)) ORDER BY"));
    assert!(query.sql.ends_with(" ASC LIMIT ?"));
    assert_eq!(query.parameters.len(), 7);
    assert_eq!(query.parameters[5], mysql_async::Value::UInt(u64::MAX));
    assert_eq!(query.parameters[6], mysql_async::Value::UInt(2));
    assert!(key.page_query_columns(&range, &columns, None, 0).is_err());
    assert!(
        key.page_query_columns(&range, &columns, None, u64::MAX)
            .is_err()
    );
    assert!(
        key.page_query_columns(&range, &columns, Some(&vec![Value::Null]), 1)
            .is_err()
    );
}
