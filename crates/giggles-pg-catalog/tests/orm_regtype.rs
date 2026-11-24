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
        label_oids: vec![65544],
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

#[test]
fn prisma_schema_exists_and_immutable_presets() {
    let sql = "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = $1), version(), current_setting('server_version_num')::integer as numeric_version";
    let p = plan_gateway(sql).unwrap().unwrap();
    assert_eq!(p.parameter_types(), &[PgType::Name]);
    for (schema, expected) in [("public", true), ("missing", false)] {
        let rows = execute(&p, &CatalogSnapshot::default(), &[Value::Text(schema.into())]).unwrap().rows;
        assert_eq!(rows[0][0], Value::Bool(expected));
        assert_eq!(rows[0][2], Value::Int(180006));
    }
    assert!(plan_gateway("SELECT current_setting('search_path') FROM pg_namespace").is_err());
}

#[test]
fn prisma_pg_tables_matches_authorized_table_inventory() {
    let mut snapshot = CatalogSnapshot::default();
    snapshot.tables.push(TableDef { id: 1, name: "users".into(), columns: vec![ColumnDef { id: 1, name: "id".into(), data_type: ScalarType::Integer, not_null: false }], primary_key: vec![] });
    let p = plan("SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename IN ('users','orders','_prisma_migrations')").unwrap().unwrap();
    assert_eq!(execute(&p, &snapshot, &[]).unwrap().rows, vec![vec![Value::Text("users".into())]]);
    let p = plan("SELECT schemaname, tablename, tableowner, tablespace, hasindexes, hasrules, hastriggers, rowsecurity FROM pg_catalog.pg_tables WHERE tablename IN ('pg_class','pg_tables','users') ORDER BY tablename").unwrap().unwrap();
    assert!(describe(&p).unwrap().iter().take(4).all(|f| f.data_type == PgType::Name));
    let rows = execute(&p, &snapshot, &[]).unwrap().rows;
    assert_eq!(rows.len(), 2);
    assert_eq!(&rows[0][3..], &[Value::Null, Value::Bool(true), Value::Bool(false), Value::Bool(false), Value::Bool(false)]);
    assert_eq!(&rows[1][3..], &[Value::Null, Value::Bool(false), Value::Bool(false), Value::Bool(false), Value::Bool(false)]);
}

#[test]
fn prisma_namespace_any_infers_name_array() {
    let p = plan("SELECT tbl.relname AS table_name, namespace.nspname AS table_namespace FROM pg_class AS tbl INNER JOIN pg_namespace AS namespace ON namespace.oid = tbl.relnamespace WHERE tbl.relkind = 'r' AND namespace.nspname = ANY ($1)").unwrap().unwrap();
    assert_eq!(p.parameter_types(), &[PgType::NameArray]);
    assert!(execute(&p, &CatalogSnapshot::default(), &[Value::Array(vec![Value::Text("public".into())])]).unwrap().rows.is_empty());
}

#[test]
fn regtype_validates_type_modifiers_before_resolving_the_base_oid() {
    let snapshot = CatalogSnapshot::default();
    let query = plan("SELECT ($1::regtype)::oid FROM pg_namespace WHERE oid=11")
        .unwrap()
        .unwrap();
    for (name, oid) in [
        ("integer [4][5]", 1007),
        ("character varying(8)", 1043),
        ("numeric(8,2)", 1700),
        ("numeric(3,-2)", 1700),
        ("pg_catalog.numeric(3,-2)[]", 1231),
        (r#""numeric"(3,-2)"#, 1700),
        ("pg_catalog.varchar(12)", 1043),
        ("float", 701),
        ("float(1)", 700),
        ("float(24)", 700),
        ("float(25)", 701),
        ("float(53)", 701),
        ("float(5)[]", 1021),
        ("pg_catalog.timestamp(7)", 1114),
        ("timestamp(3)", 1114),
        ("pg_catalog.timestamptz(3)", 1184),
        ("pg_catalog.time(7)", 1083),
        ("pg_catalog.timetz(3)", 1266),
        ("interval(7)", 1186),
        ("pg_catalog.interval(32767,7)", 1186),
        ("pg_catalog._timestamp(3)", 1115),
    ] {
        let result = execute(&query, &snapshot, &[Value::Text(name.into())]).unwrap();
        assert_eq!(result.rows, vec![vec![Value::Int(oid)]], "{name}");
    }
    for (name, expected) in [
        ("pg_catalog.float4(1)", "pg_catalog.float4"),
        (r#""float4"(1)"#, "float4"),
        ("pg_catalog.int4(1)", "pg_catalog.int4"),
        ("int4(1)", "int4"),
        (r#""text"(5)"#, "text"),
        ("pg_catalog.text(5)", "pg_catalog.text"),
        ("pg_catalog.bool(1)", "pg_catalog.bool"),
        ("bool(1)", "bool"),
        ("pg_catalog.int4(1,2)", "pg_catalog.int4"),
    ] {
        assert_eq!(
            execute(&query, &snapshot, &[Value::Text(name.into())]).unwrap_err(),
            Error::TypeModifierNotAllowed(expected.into()),
            "{name}"
        );
    }
    for (name, message) in [
        ("numeric(0)", "NUMERIC precision 0 must be between 1 and 1000"),
        ("pg_catalog.numeric(0)", "NUMERIC precision 0 must be between 1 and 1000"),
        ("pg_catalog.varchar(0)", "length for type varchar must be at least 1"),
        (r#""varchar"(0)"#, "length for type varchar must be at least 1"),
        ("numeric(1,-1001)", "NUMERIC scale -1001 must be between -1000 and 1000"),
        ("varchar(0)", "length for type varchar must be at least 1"),
        ("float(0)", "precision for type float must be at least 1 bit"),
        ("float(54)", "precision for type float must be less than 54 bits"),
        ("pg_catalog.timestamp(-1)", "TIMESTAMP(-1) precision must not be negative"),
        ("pg_catalog.timestamptz(-1)", "TIMESTAMP(-1) WITH TIME ZONE precision must not be negative"),
        ("pg_catalog.timetz(-1)", "TIME(-1) WITH TIME ZONE precision must not be negative"),
        ("pg_catalog._timestamp(-1)", "TIMESTAMP(-1) precision must not be negative"),
        ("pg_catalog.interval(7)", "invalid INTERVAL type modifier"),
        ("pg_catalog.interval(32767,-1)", "INTERVAL(-1) precision must not be negative"),
    ] {
        assert_eq!(
            execute(&query, &snapshot, &[Value::Text(name.into())]).unwrap_err(),
            Error::InvalidTypeModifier(message.into()),
            "{name}"
        );
    }
    assert_eq!(
        execute(&query, &snapshot, &[Value::Text("timestamp(-1)".into())]).unwrap_err(),
        Error::TypeNameSyntax { near: "-", input: Some("timestamp(-1)".into()) },
    );
}
