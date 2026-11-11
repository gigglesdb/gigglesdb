use giggles_pg_catalog::*;

const FIXTURES: &[(&str, &str)] = &[
    (
        "sequences",
        include_str!("fixtures/datagrip_refresh/sequences.sql"),
    ),
    ("types", include_str!("fixtures/datagrip_refresh/types.sql")),
    (
        "tables",
        include_str!("fixtures/datagrip_refresh/tables.sql"),
    ),
    (
        "foreign_tables",
        include_str!("fixtures/datagrip_refresh/foreign_tables.sql"),
    ),
    (
        "type_dependencies",
        include_str!("fixtures/datagrip_refresh/type_dependencies.sql"),
    ),
    (
        "routines",
        include_str!("fixtures/datagrip_refresh/routines.sql"),
    ),
    (
        "aggregates",
        include_str!("fixtures/datagrip_refresh/aggregates.sql"),
    ),
    (
        "operators",
        include_str!("fixtures/datagrip_refresh/operators.sql"),
    ),
    (
        "collations",
        include_str!("fixtures/datagrip_refresh/collations.sql"),
    ),
    (
        "operator_classes",
        include_str!("fixtures/datagrip_refresh/operator_classes.sql"),
    ),
    (
        "operator_families",
        include_str!("fixtures/datagrip_refresh/operator_families.sql"),
    ),
    (
        "access_operators",
        include_str!("fixtures/datagrip_refresh/access_operators.sql"),
    ),
    (
        "access_procedures",
        include_str!("fixtures/datagrip_refresh/access_procedures.sql"),
    ),
    (
        "columns",
        include_str!("fixtures/datagrip_refresh/columns.sql"),
    ),
    (
        "indexes",
        include_str!("fixtures/datagrip_refresh/indexes.sql"),
    ),
    (
        "constraints",
        include_str!("fixtures/datagrip_refresh/constraints.sql"),
    ),
    ("rules", include_str!("fixtures/datagrip_refresh/rules.sql")),
    (
        "policies",
        include_str!("fixtures/datagrip_refresh/policies.sql"),
    ),
    (
        "triggers",
        include_str!("fixtures/datagrip_refresh/triggers.sql"),
    ),
    (
        "comments",
        include_str!("fixtures/datagrip_refresh/comments.sql"),
    ),
    (
        "table_acls",
        include_str!("fixtures/datagrip_refresh/table_acls.sql"),
    ),
    ("views", include_str!("fixtures/datagrip_refresh/views.sql")),
    (
        "statistics",
        include_str!("fixtures/datagrip_refresh/statistics.sql"),
    ),
    (
        "routine_dependencies",
        include_str!("fixtures/datagrip_refresh/routine_dependencies.sql"),
    ),
];

fn snapshot(generation: u64) -> CatalogSnapshot {
    CatalogSnapshot {
        generation,
        tables: vec![TableDef {
            id: 7,
            name: "refresh_table".into(),
            columns: vec![ColumnDef {
                id: 1,
                name: "id".into(),
                data_type: ScalarType::BigInt,
                not_null: true,
            }],
            primary_key: vec![0],
        }],
        ..CatalogSnapshot::default()
    }
}

// The capture logs JDBC question marks; PostgreSQL extended-protocol SQL uses
// numbered placeholders. Preserve the fixtures and perform that translation here.
fn jdbc_sql(sql: &str) -> String {
    let mut sql = sql.to_owned();
    let mut parameter = 0;
    while let Some(at) = sql.find("?::") {
        parameter += 1;
        sql.replace_range(at..at + 1, &format!("${parameter}"));
    }
    sql
}

fn parameters(p: &Plan, generation: u64) -> Vec<Value> {
    p.parameter_types()
        .iter()
        .map(|ty| match ty {
            PgType::Oid => Value::Int(2200),
            PgType::Text | PgType::Varchar | PgType::Xid => {
                Value::Text((generation as u32).to_string())
            }
            ty => panic!("unexpected DataGrip parameter: {ty:?}"),
        })
        .collect()
}

#[test]
fn every_captured_incremental_query_plans_and_executes() {
    let snapshot = snapshot(42);
    for (name, sql) in FIXTURES {
        let p = plan(&jdbc_sql(sql))
            .unwrap_or_else(|e| panic!("{name} planning: {e:?}"))
            .unwrap();
        let fields = describe(&p).unwrap();
        let r = execute(&p, &snapshot, &parameters(&p, snapshot.generation))
            .unwrap_or_else(|e| panic!("{name} execution: {e:?}"));
        assert_eq!(r.fields, fields, "{name}");
    }
}

#[test]
fn refresh_keeps_tables_at_same_and_newer_generations() {
    let mut snapshot = snapshot(42);
    let read = plan("select xmin from pg_class where relname='refresh_table'")
        .unwrap()
        .unwrap();
    let previous = execute(&read, &snapshot, &[]).unwrap().rows[0][0].clone();
    assert_eq!(previous, Value::Int(42));
    let p = plan(&jdbc_sql(include_str!(
        "fixtures/datagrip_refresh/tables.sql"
    )))
    .unwrap()
    .unwrap();
    for generation in [42, 43] {
        snapshot.generation = generation;
        let r = execute(&p, &snapshot, &[Value::Int(2200), Value::Text("42".into())]).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][1], Value::Text("refresh_table".into()));
        assert_eq!(r.rows[0][3], Value::Int(generation as i64));
        let ages = plan("select age(xmin), pg_catalog.age($1::varchar::xid) from pg_class where relname='refresh_table'").unwrap().unwrap();
        let r = execute(&ages, &snapshot, &[Value::Text("42".into())]).unwrap();
        assert_eq!(
            r.fields.iter().map(|f| f.data_type).collect::<Vec<_>>(),
            [PgType::Int4; 2]
        );
        assert_eq!(
            r.rows,
            [vec![Value::Int(0), Value::Int((generation - 42) as i64)]]
        );
    }
}

#[test]
fn xid_casts_nulls_wrapping_and_parameter_descriptors() {
    let p = plan("select '4294967295'::xid, $1::xid, $2::varchar::xid, age('4294967295'::xid), age(NULL::xid), greatest(NULL::int, -1, 0)").unwrap().unwrap();
    assert_eq!(p.parameter_types(), [PgType::Xid, PgType::Text]);
    assert_eq!(
        describe(&p)
            .unwrap()
            .iter()
            .map(|f| f.data_type)
            .collect::<Vec<_>>(),
        [
            PgType::Xid,
            PgType::Xid,
            PgType::Xid,
            PgType::Int4,
            PgType::Int4,
            PgType::Int8
        ]
    );
    let r = execute(
        &p,
        &snapshot(3),
        &[Value::Text("42".into()), Value::Text("43".into())],
    )
    .unwrap();
    assert_eq!(
        r.rows,
        [vec![
            Value::Int(u32::MAX.into()),
            Value::Int(42),
            Value::Int(43),
            Value::Int(4),
            Value::Null,
            Value::Int(0)
        ]]
    );
    let p = plan("select age('4'::xid), age('2147483651'::xid), age('0'::xid)")
        .unwrap()
        .unwrap();
    assert_eq!(
        execute(&p, &snapshot(3), &[]).unwrap().rows,
        [vec![
            Value::Int(-1),
            Value::Int(i32::MIN.into()),
            Value::Int(3)
        ]]
    );
    for (text, expected) in [
        ("0", 0),
        ("+42", 42),
        (" 42 ", 42),
        ("-1", u32::MAX),
        ("-2147483648", 1 << 31),
        ("0xffffffff", u32::MAX),
        ("077", 63),
    ] {
        assert_eq!(parse_xid(text), Ok(expected), "{text}");
    }
    for text in [
        "",
        "x",
        "4294967296",
        "-2147483649",
        "1.2",
        "1x",
        "1 2",
        "09",
        "+ 1",
    ] {
        assert_eq!(parse_xid(text), Err(Error::Type), "{text}");
    }
    assert!(plan("select age(1)").is_err());
}

#[test]
fn every_exposed_catalog_xmin_uses_the_same_current_generation() {
    let snapshot = snapshot((1 << 32) + 42);
    let p = plan("select c.relname from pg_class c join pg_attribute a on c.oid=a.attrelid where a.attname='xmin'").unwrap().unwrap();
    let relations = execute(&p, &snapshot, &[]).unwrap();
    assert!(!relations.rows.is_empty());
    for row in relations.rows {
        let Value::Text(name) = &row[0] else {
            panic!("catalog name")
        };
        let p = plan(&format!("select xmin, age(xmin) from pg_catalog.{name}"))
            .unwrap()
            .unwrap();
        let r = execute(&p, &snapshot, &[]).unwrap();
        assert!(
            r.rows
                .iter()
                .all(|row| *row == [Value::Int(42), Value::Int(0)]),
            "{name}"
        );
    }
    let p = plan("select greatest('2',3), greatest(NULL::int,NULL::int)")
        .unwrap()
        .unwrap();
    assert_eq!(
        execute(&p, &snapshot, &[]).unwrap().rows,
        [vec![Value::Int(3), Value::Null]]
    );
}

#[test]
fn standalone_refresh_captures_plan_execute_and_describe() {
    type RefreshCase<'a> = (&'a str, &'a str, &'a [(&'a str, PgType)], usize);
    let cases: &[RefreshCase<'_>] = &[
        (
            "ssl",
            include_str!("fixtures/datagrip_refresh/ssl.sql"),
            &[("ssl", PgType::Bool)],
            1,
        ),
        (
            "locks",
            include_str!("fixtures/datagrip_refresh/locks.sql"),
            &[("transaction_id", PgType::Int8)],
            0,
        ),
        (
            "user_mapping",
            include_str!("fixtures/datagrip_refresh/user_mapping.sql"),
            &[
                ("id", PgType::Oid),
                ("server_id", PgType::Oid),
                ("user", PgType::Name),
                ("options", PgType::TextArray),
            ],
            0,
        ),
        (
            "column_acls",
            include_str!("fixtures/datagrip_refresh/column_acls.sql"),
            &[
                ("object_id", PgType::Oid),
                ("attr_position", PgType::Int2),
                ("acl", PgType::AclItemArray),
            ],
            2,
        ),
    ];
    for ssl in [false, true] {
        let mut snapshot = snapshot(1_800_000_070);
        snapshot.backend_pid = 1234;
        snapshot.backend_ssl = ssl;
        for (name, sql, expected, count) in cases {
            let p = plan_gateway(&jdbc_sql(sql))
                .unwrap_or_else(|e| panic!("{name}: {e:?}"))
                .unwrap();
            let r = execute(&p, &snapshot, &parameters(&p, snapshot.generation))
                .unwrap_or_else(|e| panic!("{name}: {e:?}"));
            assert_eq!(r.fields, describe(&p).unwrap());
            assert_eq!(
                r.fields
                    .iter()
                    .map(|f| (f.name.as_str(), f.data_type))
                    .collect::<Vec<_>>(),
                *expected,
                "{name}"
            );
            assert_eq!(r.rows.len(), *count, "{name}");
            if *name == "ssl" {
                assert_eq!(r.rows, [vec![Value::Bool(ssl)]]);
            }
            if *name == "column_acls" {
                assert!(r.rows.iter().all(|r| r[2] == Value::Null));
            }
        }
        // Exercise xid casts and age even though the real pg_locks is empty.
        let p = plan_gateway("select '1800000000'::xid::varchar::bigint, age('1800000000'::xid), pg_backend_pid() from pg_stat_ssl").unwrap().unwrap();
        assert_eq!(
            execute(&p, &snapshot, &[]).unwrap().rows,
            [vec![
                Value::Int(1_800_000_000),
                Value::Int(70),
                Value::Int(1234)
            ]]
        );
    }
}

#[test]
fn standalone_relations_have_postgresql_18_column_order_and_types() {
    use PgType as T;
    let cases: &[(&str, &[(&str, PgType)])] = &[
        (
            "pg_user_mapping",
            &[
                ("oid", T::Oid),
                ("umuser", T::Oid),
                ("umserver", T::Oid),
                ("umoptions", T::TextArray),
            ],
        ),
        (
            "pg_locks",
            &[
                ("locktype", T::Text),
                ("database", T::Oid),
                ("relation", T::Oid),
                ("page", T::Int4),
                ("tuple", T::Int2),
                ("virtualxid", T::Text),
                ("transactionid", T::Xid),
                ("classid", T::Oid),
                ("objid", T::Oid),
                ("objsubid", T::Int2),
                ("virtualtransaction", T::Text),
                ("pid", T::Int4),
                ("mode", T::Text),
                ("granted", T::Bool),
                ("fastpath", T::Bool),
                ("waitstart", T::TimestampTz),
            ],
        ),
        (
            "pg_stat_ssl",
            &[
                ("pid", T::Int4),
                ("ssl", T::Bool),
                ("version", T::Text),
                ("cipher", T::Text),
                ("bits", T::Int4),
                ("client_dn", T::Text),
                ("client_serial", T::Numeric),
                ("issuer_dn", T::Text),
            ],
        ),
        (
            "pg_attribute",
            &[
                ("attrelid", T::Oid),
                ("attname", T::Name),
                ("atttypid", T::Oid),
                ("attlen", T::Int2),
                ("attnum", T::Int2),
                ("atttypmod", T::Int4),
                ("attndims", T::Int2),
                ("attbyval", T::Bool),
                ("attalign", T::Char),
                ("attstorage", T::Char),
                ("attcompression", T::Char),
                ("attnotnull", T::Bool),
                ("atthasdef", T::Bool),
                ("atthasmissing", T::Bool),
                ("attidentity", T::Char),
                ("attgenerated", T::Char),
                ("attisdropped", T::Bool),
                ("attislocal", T::Bool),
                ("attinhcount", T::Int2),
                ("attcollation", T::Oid),
                ("attstattarget", T::Int2),
                ("attacl", T::AclItemArray),
                ("attoptions", T::TextArray),
                ("attfdwoptions", T::TextArray),
                ("attmissingval", T::AnyArray),
                ("xmin", T::Xid),
            ],
        ),
    ];
    for (name, expected) in cases {
        let p = plan(&format!("select * from pg_catalog.{name}"))
            .unwrap()
            .unwrap();
        let fields = describe(&p).unwrap();
        assert_eq!(
            fields
                .iter()
                .map(|f| (f.name.as_str(), f.data_type))
                .collect::<Vec<_>>(),
            *expected,
            "{name}"
        );
        let r = execute(&p, &snapshot(42), &[]).unwrap();
        assert!(
            r.rows.iter().all(|row| row.len() == expected.len()),
            "{name}"
        );
    }
    assert_eq!(T::TimestampTz.oid(), 1184);
    assert_eq!(T::TimestampTz.type_size(), 8);
}

#[test]
fn reconnect_txid_and_catalog_union_match_the_snapshot() {
    let txid = plan_gateway(include_str!("fixtures/datagrip_refresh/current_txid.sql"))
        .unwrap()
        .unwrap();
    let union = plan_gateway(include_str!("fixtures/datagrip_refresh/catalog_union.sql"))
        .unwrap()
        .unwrap();
    for generation in [42, u64::from(u32::MAX), (1 << 32) + 42] {
        let snapshot = snapshot(generation);
        let xid = Value::Int(i64::from(generation as u32));
        let current = execute(&txid, &snapshot, &[]).unwrap();
        assert_eq!(current.fields, describe(&txid).unwrap());
        assert_eq!(current.fields.len(), 1);
        assert_eq!(current.fields[0].name, "current_txid");
        assert_eq!(current.fields[0].data_type, PgType::Int8);
        assert_eq!(current.rows, [vec![xid.clone()]]);
        let catalogs = execute(&union, &snapshot, &[]).unwrap();
        assert_eq!(catalogs.fields, describe(&union).unwrap());
        assert_eq!(
            catalogs
                .fields
                .iter()
                .map(|f| (f.name.as_str(), f.data_type))
                .collect::<Vec<_>>(),
            [("oid", PgType::Oid), ("xmin", PgType::Xid)]
        );
        assert_eq!(catalogs.rows, [vec![Value::Int(2200), xid.clone()]]);
        let direct = plan_gateway(
            "SELECT txid_current(), pg_catalog.txid_current(), age(txid_current()::varchar::xid)",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            execute(&direct, &snapshot, &[]).unwrap().rows,
            [vec![xid.clone(), xid, Value::Int(0)]]
        );
    }
    assert!(catalog_identifier("txid_current"));
    assert!(plan("SELECT pg_catalog.txid_current(1)").is_err());
}

#[test]
fn catalog_union_leaf_budget_accepts_32_and_rejects_larger_chains() {
    // These constant leaves avoid the independent 32-source budget and stay
    // beneath the token/constructor guards, exercising the query budget itself.
    let leaf = "SELECT pg_catalog.txid_current()";
    let allowed = plan_gateway(&[leaf; 32].join(" UNION ")).unwrap().unwrap();
    assert_eq!(
        execute(&allowed, &snapshot(42), &[]).unwrap().rows,
        [vec![Value::Int(42)]]
    );
    for count in [33, 64] {
        assert!(matches!(
            plan_gateway(&vec![leaf; count].join(" UNION ")),
            Err(Error::Limit)
        ));
    }
    // Removing recursive AST clones must preserve the enclosing UNION's
    // ORDER BY/LIMIT, and WITH bindings must remain visible in both arms.
    let p = plan("WITH n AS (SELECT oid FROM pg_namespace WHERE oid IN (11,2200)) SELECT oid FROM n UNION SELECT oid FROM n ORDER BY oid DESC LIMIT 1")
        .unwrap()
        .unwrap();
    assert_eq!(
        execute(&p, &snapshot(42), &[]).unwrap().rows,
        [vec![Value::Int(2200)]]
    );
}

#[test]
fn bigint_remainder_preserves_sign_nulls_and_zero_errors() {
    let p = plan("SELECT -5::bigint % 4::bigint, 5::bigint % -4::bigint, '-9223372036854775808'::bigint % -1::bigint, NULL::bigint % 0::bigint, pg_catalog.txid_current() % 4294967296::bigint")
        .unwrap()
        .unwrap();
    assert!(
        describe(&p)
            .unwrap()
            .iter()
            .all(|f| f.data_type == PgType::Int8)
    );
    assert_eq!(
        execute(&p, &snapshot(42), &[]).unwrap().rows,
        [vec![
            Value::Int(-1),
            Value::Int(1),
            Value::Int(0),
            Value::Null,
            Value::Int(42)
        ]]
    );
    for op in ["%", "/"] {
        let p = plan(&format!("SELECT pg_catalog.txid_current() {op} 0::bigint"))
            .unwrap()
            .unwrap();
        assert_eq!(execute(&p, &snapshot(42), &[]), Err(Error::DivisionByZero));
    }
}
