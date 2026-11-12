use giggles_pg_catalog::*;

#[test]
fn psycopg_type_discovery_and_regtype_names() {
    let mut snapshot = CatalogSnapshot::default();
    snapshot.user_types.push(UserType {
        schema: "public".into(),
        oid: 65540,
        name: "mytype".into(),
        kind: 'e',
        base: 0,
        typmod: -1,
        not_null: false,
        labels: vec!["a".into()],
    });
    let p = plan("SELECT typname AS name, oid, typarray AS array_oid, oid::regtype::text AS regtype, typdelim AS delimiter FROM pg_type t WHERE t.oid = to_regtype($1) ORDER BY t.oid").unwrap().unwrap();
    assert_eq!(p.parameter_types(), &[PgType::Text]);
    assert!(
        execute(&p, &snapshot, &[Value::Text("hstore".into())])
            .unwrap()
            .rows
            .is_empty()
    );
    for (name, oid, display) in [
        ("int4", 23, "integer"),
        ("integer", 23, "integer"),
        ("public.mytype", 65540, "mytype"),
        ("pg_catalog.text", 25, "text"),
        ("text[]", 1009, "text[]"),
        ("character varying(12)", 1043, "character varying"),
    ] {
        let rows = execute(&p, &snapshot, &[Value::Text(name.into())])
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 1, "{name}");
        assert_eq!(rows[0][1], Value::Int(oid));
        assert_eq!(rows[0][3], Value::Text(display.into()));
    }
    for name in ["bad..syntax", "integer(", "text junk", "int4; SELECT 1"] {
        assert_eq!(
            execute(&p, &snapshot, &[Value::Text(name.into())]).unwrap_err(),
            Error::Syntax,
            "{name}"
        );
    }
    for name in ["public.missing", "pg_catalog.integer", ""] {
        assert!(
            execute(&p, &snapshot, &[Value::Text(name.into())])
                .unwrap()
                .rows
                .is_empty(),
            "{name}"
        );
    }
    let p = plan("SELECT 'integer'::regtype::text, NULL::regtype, to_regtype(NULL), to_regtype('999999')::oid, 1::regtype::text FROM pg_namespace WHERE oid=11").unwrap().unwrap();
    assert_eq!(
        execute(&p, &snapshot, &[]).unwrap().rows,
        vec![vec![
            Value::Text("integer".into()),
            Value::Null,
            Value::Null,
            Value::Int(999999),
            Value::Text("1".into())
        ]]
    );
}

#[test]
fn sqlalchemy_schema_qualified_catalog_columns() {
    let p = plan("SELECT pg_catalog.pg_class.relname FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace WHERE pg_catalog.pg_class.relname = $1::VARCHAR AND pg_catalog.pg_class.relkind = ANY (ARRAY[$2::VARCHAR, $3::VARCHAR, $4::VARCHAR, $5::VARCHAR, $6::VARCHAR]) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != $7::VARCHAR").unwrap().unwrap();
    let params =
        ["alembic_version", "r", "p", "f", "v", "m", "pg_catalog"].map(|s| Value::Text(s.into()));
    assert!(
        execute(&p, &CatalogSnapshot::default(), &params)
            .unwrap()
            .rows
            .is_empty()
    );
    for sql in [
        "SELECT public.pg_class.oid FROM pg_catalog.pg_class",
        "SELECT pg_catalog.pg_class.oid FROM pg_catalog.pg_class AS c",
        "SELECT pg_catalog.c.oid FROM pg_catalog.pg_class AS c",
    ] {
        assert_eq!(plan(sql).unwrap_err(), Error::UnknownColumn);
    }
}
