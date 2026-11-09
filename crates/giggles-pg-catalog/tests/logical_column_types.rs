use giggles_pg_catalog::{CatalogSnapshot, ColumnDef, ScalarType, TableDef, Value, execute, plan};

#[test]
fn logical_type_metadata_follows_visible_columns() {
    let mut snapshot = CatalogSnapshot::default();
    let query =
        plan("SELECT oid,typname FROM pg_type WHERE oid IN (2950,2951,3802,3807) ORDER BY oid")
            .unwrap()
            .unwrap();
    assert!(execute(&query, &snapshot, &[]).unwrap().rows.is_empty());
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
