use giggles_pg_catalog::*;
#[test]
fn rails_columns_resolve_quoted_application_regclass() {
    let mut snapshot = CatalogSnapshot::default();
    snapshot.tables.push(TableDef { id: 1, name: "users".into(), columns: vec![ColumnDef { id: 2, name: "id".into(), data_type: ScalarType::Integer, not_null: true }], primary_key: vec![0] });
    let sql = r#"SELECT a.attname, format_type(a.atttypid, a.atttypmod), pg_get_expr(d.adbin, d.adrelid), a.attnotnull, a.atttypid, a.atttypmod, c.collname, col_description(a.attrelid, a.attnum) AS comment, attidentity AS identity, attgenerated as attgenerated FROM pg_attribute a LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum LEFT JOIN pg_type t ON a.atttypid = t.oid LEFT JOIN pg_collation c ON a.attcollation = c.oid AND a.attcollation <> t.typcollation WHERE a.attrelid = '"users"'::regclass AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum"#;
    let p = plan(sql).unwrap().unwrap();
    assert_eq!(execute(&p, &snapshot, &[]).unwrap().rows, vec![vec![Value::Text("id".into()), Value::Text("integer".into()), Value::Null, Value::Bool(true), Value::Int(23), Value::Int(-1), Value::Null, Value::Null, Value::Text("".into()), Value::Text("".into())]]);
    for literal in ["public.users", "public.\"users\"", "\"public\".\"users\""] {
        let p = plan(&sql.replace("\"users\"", literal)).unwrap().unwrap();
        assert_eq!(execute(&p, &snapshot, &[]).unwrap().rows.len(), 1);
    }
}

#[test]
fn rails_primary_key_order_and_array_position_nulls() {
    let mut snapshot = CatalogSnapshot::default();
    snapshot.tables.push(TableDef { id: 1, name: "users".into(), columns: vec![ColumnDef { id: 2, name: "id".into(), data_type: ScalarType::Integer, not_null: true }], primary_key: vec![0] });
    let p = plan(r#"SELECT a.attname, array_position(i.indkey,a.attnum) FROM pg_index i JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) WHERE i.indrelid='"users"'::regclass AND i.indisprimary ORDER BY array_position(i.indkey,a.attnum)"#).unwrap().unwrap();
    assert_eq!(execute(&p,&snapshot,&[]).unwrap().rows, vec![vec![Value::Text("id".into()),Value::Int(0)]]);
    let p = plan("SELECT array_position(ARRAY[3,2,3],3),array_position(ARRAY[1,NULL,1],NULL),array_position(ARRAY[1,2],4) FROM pg_namespace WHERE oid=11").unwrap().unwrap();
    assert_eq!(execute(&p,&snapshot,&[]).unwrap().rows, vec![vec![Value::Int(1),Value::Int(2),Value::Null]]);
}
