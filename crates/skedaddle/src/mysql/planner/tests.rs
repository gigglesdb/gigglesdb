use super::*;
use crate::{
    coordinator::chunk::{ChunkAttempt, ChunkCompletion, ChunkPlanRef, ChunkRowsRef},
    event::{EventError, OpaqueValue, PositionOrder},
    mapping::{PgType, mysql::plan_column},
    mysql::{MysqlPosition, schema::ColumnDescriptor},
};
use mysql_common::{constants::ColumnType, packets::Column, row::new_row};
use tokio_postgres::types::Type;
const UUID: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
fn table() -> MysqlPlannerTable {
    let mut key = crate::mysql::chunk_key::tests::table();
    key.columns[0].column_type = "int".into();
    key.columns[0].type_id = plan_column(&ColumnDescriptor {
        name: "id".into(),
        ordinal: 1,
        data_type: "int".into(),
        column_type: "int".into(),
        character_set: None,
        collation: None,
        nullable: false,
        generated: false,
        column_default: None,
        default_generated: false,
        generation_expression: None,
    })
    .unwrap()
    .type_id;
    let destination = apply::Table {
        event_table: "rows".into(),
        schema: "public".into(),
        name: "rows".into(),
        schema_version: 0,
        columns: vec![apply::Column {
            name: "id".into(),
            type_id: key.columns[0].type_id.clone(),
            pg_type: PgType::Int4,
            wire_type: Type::INT4,
        }],
        key: vec!["id".into()],
    };
    MysqlPlannerTable::new(
        ChunkPlan::new(key, 2, vec![vec![Value::Int4(5)]]).unwrap(),
        destination,
    )
    .unwrap()
}
fn position(order: &MysqlOrder, n: u64) -> Position {
    Position::new(
        order.domain(),
        MysqlPosition::File {
            server_uuid: UUID.into(),
            file_seq: 1,
            offset: n,
        }
        .encode()
        .unwrap(),
    )
}
fn state(table: &MysqlPlannerTable) -> Materialized {
    let mut s = Materialized::default();
    s.chunk_plans.insert(
        "rows".into(),
        ChunkPlanRef {
            table: "rows".into(),
            plan: table.plan.ranges[0].id.plan.clone(),
            digest: "a".repeat(64),
            bytes: 1,
            chunks: 2,
        },
    );
    s
}
#[test]
fn durable_done_is_completed_only_strictly_after_native_high() {
    let t = table();
    let order = MysqlOrder::file(UUID).unwrap();
    let mut s = state(&t);
    let r = &t.plan.ranges[0];
    let p = position(&order, 100);
    t.validate(&p, &s, &order).unwrap();
    assert_eq!(t.status(r, &p, &s, &order).unwrap(), RangeStatus::NotYet);
    let id = serde_json::to_string(&r.id).unwrap();
    s.chunk_attempts.insert(
        id.clone(),
        ChunkAttempt {
            chunk: r.clone(),
            attempt: 2,
            done: None,
        },
    );
    assert_eq!(t.status(r, &p, &s, &order).unwrap(), RangeStatus::InWindow);
    s.chunk_attempts.get_mut(&id).unwrap().done = Some(ChunkCompletion {
        high: p.clone(),
        chunk_rows: ChunkRowsRef {
            mseq: 1,
            manifest_digest: [1; 32],
        },
    });
    for n in [99, 100] {
        assert_eq!(
            t.status(r, &position(&order, n), &s, &order).unwrap(),
            RangeStatus::InWindow
        );
    }
    assert_eq!(
        t.status(r, &position(&order, 101), &s, &order).unwrap(),
        RangeStatus::Done
    );
    s.chunk_attempts.get_mut(&id).unwrap().chunk.lo = Some(vec![Value::Int4(1)]);
    assert!(t.status(r, &p, &s, &order).is_err());
}
#[test]
fn wrong_domain_incomparable_gtids_and_foreign_plan_fail_closed() {
    let t = table();
    let order = MysqlOrder::file(UUID).unwrap();
    let mut s = state(&t);
    assert!(matches!(
        t.validate(&Position::new("pg", vec![0; 8]), &s, &order),
        Err(PgPlannerError::Apply(ApplyError::Position(
            EventError::PositionDomainMismatch
        )))
    ));
    s.chunk_plans.get_mut("rows").unwrap().plan = "b".repeat(64);
    assert!(t.validate(&position(&order, 100), &s, &order).is_err());
    let order = MysqlOrder::gtid("source").unwrap();
    let p = |set: &str| {
        Position::new(
            order.domain(),
            MysqlPosition::Gtid {
                set: set.parse().unwrap(),
                server_uuid: UUID.into(),
                file: "binlog.000001".into(),
                offset: 100,
            }
            .encode()
            .unwrap(),
        )
    };
    let mut s = state(&t);
    let r = &t.plan.ranges[0];
    s.chunk_attempts.insert(
        serde_json::to_string(&r.id).unwrap(),
        ChunkAttempt {
            chunk: r.clone(),
            attempt: 1,
            done: Some(ChunkCompletion {
                high: p(&format!("{UUID}:2")),
                chunk_rows: ChunkRowsRef {
                    mseq: 1,
                    manifest_digest: [0; 32],
                },
            }),
        },
    );
    assert!(matches!(
        t.status(r, &p(&format!("{UUID}:1")), &s, &order),
        Err(PgPlannerError::Apply(ApplyError::Position(_)))
    ));
}
fn result(values: &[u64]) -> mysql_async::Row {
    new_row(
        values
            .iter()
            .copied()
            .map(mysql_async::Value::UInt)
            .collect(),
        vec![Column::new(ColumnType::MYSQL_TYPE_LONGLONG); values.len()].into(),
    )
}
#[test]
fn membership_requires_exactly_one_range_and_one_result_per_key() {
    assert_eq!(
        decode_membership(vec![result(&[1, 0, 1]), result(&[0, 1, 0])], 2, 2).unwrap(),
        [0, 1]
    );
    for rows in [
        vec![result(&[0, 1, 1])],
        vec![result(&[0, 0, 0])],
        vec![result(&[1, 1, 0])],
        vec![],
        vec![result(&[0, 1, 0]), result(&[0, 1, 0])],
        vec![result(&[0, 2, 0])],
        vec![result(&[0, 1])],
    ] {
        assert!(decode_membership(rows, 1, 2).is_err());
    }
}
#[test]
fn membership_uses_shared_bounds_and_typed_derived_parameters() {
    let t = table();
    let q = t
        .key
        .membership_query(
            &t.plan.ranges,
            &[vec![Value::Int4(-7)], vec![Value::Int4(9)]],
        )
        .unwrap();
    assert!(q.sql.contains("(`__k0` < CAST(? AS SIGNED))"));
    assert!(q.sql.contains("(`__k0` >= CAST(? AS SIGNED))"));
    assert!(
        q.sql
            .contains("SELECT ? AS `__ordinal`, CAST(? AS SIGNED) AS `__k0` UNION ALL SELECT")
    );
    assert_eq!(
        q.parameters,
        vec![
            mysql_async::Value::Int(5),
            mysql_async::Value::Int(5),
            mysql_async::Value::UInt(0),
            mysql_async::Value::Int(-7),
            mysql_async::Value::UInt(1),
            mysql_async::Value::Int(9)
        ]
    );
    assert!(t.key.membership_query(&t.plan.ranges, &[]).is_err());
    assert!(
        t.key
            .membership_query(&t.plan.ranges, &[vec![Value::Null]])
            .is_err()
    );
    assert!(
        t.key
            .membership_query(&t.plan.ranges, &vec![vec![Value::Int4(1)]; 32768])
            .is_err()
    );
}
#[test]
fn cache_identity_rejects_null_incomplete_and_foreign_mappings() {
    let t = table();
    let mut row: Row = [(
        "id".into(),
        OpaqueValue {
            type_id: t.key.columns[0].type_id.clone(),
            bytes: Some(Value::Int4(4).canonical_bytes().unwrap()),
        },
    )]
    .into();
    assert_eq!(t.key(&row).unwrap().1, [Value::Int4(4)]);
    assert!(t.key(&Row::new()).is_err());
    row.get_mut("id").unwrap().type_id = "foreign".into();
    assert!(t.key(&row).is_err());
    row.get_mut("id").unwrap().type_id = t.key.columns[0].type_id.clone();
    row.get_mut("id").unwrap().bytes = None;
    assert!(t.key(&row).is_err());
}
#[test]
fn destination_and_saved_plan_must_agree() {
    let t = table();
    let mut plan = t.plan.clone();
    plan.ranges[0].hi = None;
    assert!(MysqlPlannerTable::new(plan, t.destination.clone()).is_err());
    let mut destination = t.destination.clone();
    destination.key.clear();
    assert!(MysqlPlannerTable::new(t.plan.clone(), destination).is_err());
}
