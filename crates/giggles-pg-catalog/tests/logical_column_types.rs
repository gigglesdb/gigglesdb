use giggles_pg_catalog::{CatalogSnapshot, ColumnDef, ScalarType, TableDef, Value, execute, plan};

#[test]
fn uuid_and_jsonb_metadata_precede_columns() {
    let mut snapshot = CatalogSnapshot::default();
    let query =
        plan("SELECT oid,typname FROM pg_type WHERE oid IN (2950,2951,3802,3807) ORDER BY oid")
            .unwrap()
            .unwrap();
    // Built-in UUID and JSONB types must be discoverable when Npgsql first connects.
    assert_eq!(
        execute(&query, &snapshot, &[]).unwrap().rows,
        vec![
            vec![Value::Int(2950), Value::Text("uuid".into())],
            vec![Value::Int(2951), Value::Text("_uuid".into())],
            vec![Value::Int(3802), Value::Text("jsonb".into())],
            vec![Value::Int(3807), Value::Text("_jsonb".into())],
        ]
    );
    // PostgreSQL 18.6 built-in rows, checked before any application DDL.
    let metadata = plan("SELECT oid,typname,typtype,typelem,typarray FROM pg_type WHERE oid IN (3802,3807) ORDER BY oid").unwrap().unwrap();
    assert_eq!(
        execute(&metadata, &snapshot, &[]).unwrap().rows,
        vec![
            vec![
                Value::Int(3802),
                Value::Text("jsonb".into()),
                Value::Text("b".into()),
                Value::Int(0),
                Value::Int(3807)
            ],
            vec![
                Value::Int(3807),
                Value::Text("_jsonb".into()),
                Value::Text("b".into()),
                Value::Int(3802),
                Value::Int(0)
            ],
        ]
    );
    snapshot.tables.push(TableDef {
        id: 1,
        name: "logical".into(),
        primary_key: vec![],
        columns: vec![
            ColumnDef {
                id: 2,
                name: "u".into(),
                data_type: ScalarType::Uuid,
                not_null: false,
            },
            ColumnDef {
                id: 3,
                name: "j".into(),
                data_type: ScalarType::Jsonb,
                not_null: false,
            },
        ],
    });
    assert_eq!(
        execute(&query, &snapshot, &[]).unwrap().rows,
        vec![
            vec![Value::Int(2950), Value::Text("uuid".into())],
            vec![Value::Int(2951), Value::Text("_uuid".into())],
            vec![Value::Int(3802), Value::Text("jsonb".into())],
            vec![Value::Int(3807), Value::Text("_jsonb".into())],
        ]
    );
    let query=plan("SELECT column_name,data_type,udt_name FROM information_schema.columns WHERE table_name='logical' ORDER BY ordinal_position").unwrap().unwrap();
    assert_eq!(
        execute(&query, &snapshot, &[]).unwrap().rows,
        vec![
            vec![
                Value::Text("u".into()),
                Value::Text("uuid".into()),
                Value::Text("uuid".into())
            ],
            vec![
                Value::Text("j".into()),
                Value::Text("jsonb".into()),
                Value::Text("jsonb".into())
            ],
        ]
    );
}
