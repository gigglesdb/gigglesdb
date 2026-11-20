use giggles_pg_catalog::*;
fn snapshot() -> CatalogSnapshot {
    CatalogSnapshot {
        tables: vec![TableDef {
            id: 7,
            name: "inventory".into(),
            columns: vec![
                ColumnDef {
                    id: 11,
                    name: "tenant".into(),
                    data_type: ScalarType::Integer,
                    not_null: true,
                },
                ColumnDef {
                    id: 15,
                    name: "sku".into(),
                    data_type: ScalarType::Text,
                    not_null: false,
                },
                ColumnDef {
                    id: 19,
                    name: "enabled".into(),
                    data_type: ScalarType::Boolean,
                    not_null: false,
                },
                ColumnDef {
                    id: 23,
                    name: "payload".into(),
                    data_type: ScalarType::Bytes,
                    not_null: false,
                },
                ColumnDef {
                    id: 27,
                    name: "small".into(),
                    data_type: ScalarType::SmallInt,
                    not_null: false,
                },
                ColumnDef {
                    id: 31,
                    name: "large".into(),
                    data_type: ScalarType::BigInt,
                    not_null: false,
                },
            ],
            primary_key: vec![1, 0],
        }],
        ..CatalogSnapshot::default()
    }
}
fn run(sql: &str) -> ResultSet {
    let p = plan(sql)
        .unwrap_or_else(|e| panic!("plan {e:?}: {sql}"))
        .expect("catalog");
    execute(&p, &snapshot(), &[]).unwrap_or_else(|e| panic!("execute {e:?}: {sql}"))
}
fn s(s: &str) -> Value {
    Value::Text(s.into())
}
fn i(i: i64) -> Value {
    Value::Int(i)
}
#[test]
fn exact_jdbc_schemas() {
    let r = run(include_str!("fixtures/jdbc_schemas.sql"));
    assert_eq!(
        r.rows,
        vec![
            vec![s("information_schema"), Value::Null],
            vec![s("pg_catalog"), Value::Null],
            vec![s("public"), Value::Null]
        ]
    );
    assert_eq!(r.fields[0].data_type.oid(), 19)
}
#[test]
fn exact_jdbc_tables() {
    let r = run(include_str!("fixtures/jdbc_tables.sql"));
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        &r.rows[0][..5],
        &[
            Value::Null,
            s("public"),
            s("inventory"),
            s("TABLE"),
            Value::Null
        ]
    );
}
#[test]
fn exact_jdbc_columns() {
    let r = run(include_str!("fixtures/jdbc_columns.sql"));
    assert_eq!(r.rows.len(), 6);
    assert_eq!(
        &r.rows[1][..10],
        &[
            s("public"),
            s("inventory"),
            s("sku"),
            i(25),
            Value::Bool(true),
            i(-1),
            i(-1),
            i(-1),
            i(2),
            Value::Null
        ]
    );
    assert_eq!(
        r.rows.iter().map(|r| r[3].clone()).collect::<Vec<_>>(),
        vec![i(23), i(25), i(16), i(17), i(21), i(20)]
    );
    assert_eq!(r.fields[3].data_type, PgType::Oid);
}
#[test]
fn exact_jdbc_pk() {
    let r = run(include_str!("fixtures/jdbc_pk.sql"));
    assert_eq!(
        r.rows,
        vec![
            vec![
                Value::Null,
                s("public"),
                s("inventory"),
                s("sku"),
                i(1),
                s("inventory_pkey")
            ],
            vec![
                Value::Null,
                s("public"),
                s("inventory"),
                s("tenant"),
                i(2),
                s("inventory_pkey")
            ]
        ]
    );
}
#[test]
fn exact_jdbc_typeinfo() {
    let r = run(include_str!("fixtures/jdbc_typeinfo.sql"));
    for (name, oid) in [
        ("bool", 16),
        ("bytea", 17),
        ("int8", 20),
        ("int2", 21),
        ("int4", 23),
        ("text", 25),
    ] {
        assert!(r.rows.contains(&vec![s(name), i(oid)]));
    }
    for (name, oid) in [("numeric", 1700), ("float4", 700), ("float8", 701)] {
        assert!(r.rows.contains(&vec![s(name), i(oid)]));
    }
}
#[test]
fn exact_jdbc_typecache() {
    let r = run(include_str!("fixtures/jdbc_typecache.sql"));
    assert!(
        r.rows
            .contains(&vec![Value::Bool(true), s("b"), s("_text"), i(1009)])
    );
    assert!(
        r.rows
            .contains(&vec![Value::Bool(false), s("b"), s("int4"), i(23)])
    );
}
#[test]
fn exact_jdbc_typename_parameter() {
    let p = plan(include_str!("fixtures/jdbc_typename.sql"))
        .unwrap()
        .unwrap();
    assert_eq!(p.parameter_count(), 1);
    let r = execute(&p, &snapshot(), &[i(23)]).unwrap();
    assert_eq!(
        r.rows,
        vec![vec![Value::Bool(true), s("pg_catalog"), s("int4")]]
    );
}
#[test]
fn identity() {
    assert_eq!(
        run("SELECT current_database(), current_schema(), current_user, session_user").rows,
        vec![vec![s("giggles"), s("public"), s("giggles"), s("giggles")]]
    );
}
#[test]
fn snapshot_identity_is_read_on_each_execution() {
    let sql = "SELECT current_user, session_user, current_role, user, current_database(), current_catalog";
    let plan = plan_gateway(sql).unwrap().expect("catalog identity");
    let mut snapshot = CatalogSnapshot::default();
    assert_eq!(
        execute(&plan, &snapshot, &[]).unwrap().rows,
        vec![vec![s("giggles"); 6]]
    );
    for (user, database) in [("alice", "appdb"), ("bob", "postgres")] {
        snapshot.user = user.into();
        snapshot.database = database.into();
        assert_eq!(
            execute(&plan, &snapshot, &[]).unwrap().rows,
            vec![vec![
                s(user),
                s(user),
                s(user),
                s(user),
                s(database),
                s(database)
            ]]
        );
    }
    for keyword in ["current_role", "user", "current_catalog"] {
        assert!(
            plan_gateway(&format!("SELECT {keyword}"))
                .unwrap()
                .is_some()
        );
        assert!(plan_gateway(&format!("SELECT \"{keyword}\" FROM pg_database")).is_err());
    }
}
#[test]
fn datagrip_list_schemas() {
    let r = run(
        "select N.oid::bigint as id, N.xmin as state_number, nspname as name, D.description, pg_catalog.pg_get_userbyid(N.nspowner) as owner from pg_catalog.pg_namespace N left join pg_catalog.pg_description D on N.oid = D.objoid order by case when nspname = pg_catalog.current_schema() then -1::bigint else N.oid::bigint end",
    );
    assert_eq!(
        r.rows[0],
        vec![
            i(2200),
            i(0),
            s("public"),
            s("standard public schema"),
            s("pg_database_owner")
        ]
    );
}
#[test]
fn datagrip_list_databases() {
    let r = run(
        "select N.oid::bigint as id, datname as name, D.description, datistemplate as is_template, datallowconn as allow_connections, pg_catalog.pg_get_userbyid(N.datdba) as owner from pg_catalog.pg_database N left join pg_catalog.pg_shdescription D on N.oid = D.objoid order by case when datname = pg_catalog.current_database() then -1::bigint else N.oid::bigint end",
    );
    assert_eq!(
        r.rows,
        vec![vec![
            i(16384),
            s("giggles"),
            Value::Null,
            Value::Bool(false),
            Value::Bool(true),
            s("giggles")
        ]]
    );
}
#[test]
fn snapshot_is_only_visibility_source_and_refreshes() {
    let p = plan("SELECT relname FROM pg_class WHERE relnamespace = 2200 ORDER BY relname")
        .unwrap()
        .unwrap();
    assert_eq!(
        execute(&p, &CatalogSnapshot::default(), &[]).unwrap().rows,
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(
        execute(&p, &snapshot(), &[]).unwrap().rows,
        vec![vec![s("inventory")], vec![s("inventory_pkey")]]
    );
    assert_eq!(
        execute(&p, &CatalogSnapshot::default(), &[]).unwrap().rows,
        Vec::<Vec<Value>>::new()
    );
}
#[test]
fn stable_oid_independent_of_snapshot_order() {
    let mut sn = snapshot();
    let mut other = sn.tables[0].clone();
    other.id = 100;
    other.name = "other".into();
    sn.tables.push(other);
    let p = plan("SELECT oid FROM pg_class WHERE relname='inventory'")
        .unwrap()
        .unwrap();
    let a = execute(&p, &sn, &[]).unwrap();
    sn.tables.reverse();
    assert_eq!(execute(&p, &sn, &[]).unwrap(), a);
    assert_eq!(a.rows, vec![vec![i(65564)]]);
}
#[test]
fn quote_and_comment_semantics() {
    assert!(
        plan("SELECT 'pg_catalog.pg_class' FROM app")
            .unwrap()
            .is_none()
    );
    assert!(plan("SELECT /* pg_class */ x FROM app").unwrap().is_none());
    assert!(
        plan("SELECT relname FROM public.pg_class")
            .unwrap()
            .is_none()
    );
    assert!(plan("SELECT relname FROM \"PG_CLASS\"").unwrap().is_none());
    assert_eq!(run("SELECT c.\"relname\" AS \"Exact\" FROM pg_catalog.\"pg_class\" c WHERE c.relname='inventory'").fields[0].name,"Exact");
}
#[test]
fn rejects_mixed_and_mutating() {
    for sql in [
        "SELECT * FROM pg_class, inventory",
        "SELECT * FROM pg_class c JOIN public.inventory i ON true",
        "SELECT (SELECT count(*) FROM inventory) FROM pg_class",
    ] {
        assert_eq!(plan(sql).unwrap_err(), Error::MixedRelations);
    }
    for sql in [
        "DELETE FROM pg_class",
        "UPDATE pg_class SET relname='x'",
        "SELECT * INTO app FROM pg_class",
        "SELECT * FROM pg_class; SELECT * FROM app",
        "WITH x AS (DELETE FROM pg_class RETURNING *) SELECT * FROM x",
    ] {
        assert!(plan(sql).is_err(), "{sql}");
    }
}
#[test]
fn rejects_unimplemented_even_empty() {
    for sql in [
        "SELECT pg_sleep(1) FROM pg_attrdef",
        "SELECT unknown FROM pg_attrdef",
        "SELECT * FROM pg_catalog.made_up",
        "SELECT * FROM pg_class FOR UPDATE",
        "SELECT count(DISTINCT oid) FROM pg_class",
        "SELECT relname FROM pg_class INTERSECT SELECT relname FROM pg_class",
    ] {
        assert!(plan(sql).is_err(), "{sql}");
    }
}
#[test]
fn invalid_snapshot_cannot_alias_oids_or_columns() {
    let p = plan("SELECT oid FROM pg_class").unwrap().unwrap();
    for mode in 0..5 {
        let mut sn = snapshot();
        match mode {
            0 => sn.tables[0].id = u64::MAX,
            1 => sn.tables.push(sn.tables[0].clone()),
            2 => sn.tables[0].columns[1].id = 11,
            3 => sn.tables[0].primary_key = vec![99],
            _ => sn.tables[0].primary_key = vec![0, 0],
        };
        assert!(execute(&p, &sn, &[]).is_err());
    }
}
#[test]
fn three_valued_predicates_and_like() {
    assert_eq!(run("SELECT null = 1, null OR false, null AND false, null OR true, 'a_b' LIKE 'a\\_b', 'a\nb' LIKE 'a_b' FROM pg_database").rows,vec![vec![Value::Null,Value::Null,Value::Bool(false),Value::Bool(true),Value::Bool(true),Value::Bool(true)]]);
}
#[test]
fn bounds() {
    assert!(plan(&" ".repeat(MAX_SQL + 1)).unwrap().is_none());
    assert!(
        plan(&format!(
            "SELECT {}current_database(){}",
            "(".repeat(100),
            ")".repeat(100)
        ))
        .is_err()
    );
    let mut sn = snapshot();
    sn.tables[0].name = "x".repeat(65537);
    let p = plan("SELECT oid FROM pg_class").unwrap().unwrap();
    assert_eq!(execute(&p, &sn, &[]).unwrap_err(), Error::Limit);
    assert!(run_result("SELECT a.attname FROM pg_attribute a CROSS JOIN pg_attribute b CROSS JOIN pg_attribute c").is_err());
}
fn run_result(sql: &str) -> Result<ResultSet, Error> {
    execute(&plan(sql)?.ok_or(Error::Unsupported)?, &snapshot(), &[])
}
#[test]
fn parameter_errors() {
    let p = plan("SELECT typname FROM pg_type WHERE oid=$1")
        .unwrap()
        .unwrap();
    assert_eq!(
        execute(&p, &snapshot(), &[]).unwrap_err(),
        Error::Parameters
    );
    assert!(execute(&p, &snapshot(), &[s("invalid")]).is_err());
    assert_eq!(
        execute(&p, &snapshot(), &[s("23")]).unwrap().rows,
        vec![vec![s("int4")]]
    );
}
#[test]
fn descriptions_exist_for_empty_results() {
    let p=plan("SELECT a.attname, a.atttypid, a.attnotnull, i.indkey FROM pg_attribute a JOIN pg_index i ON a.attrelid=i.indrelid WHERE false").unwrap().unwrap();
    assert_eq!(
        describe(&p)
            .unwrap()
            .iter()
            .map(|f| f.data_type.oid())
            .collect::<Vec<_>>(),
        vec![19, 26, 16, 22]
    );
    assert!(
        execute(&p, &CatalogSnapshot::default(), &[])
            .unwrap()
            .rows
            .is_empty()
    );
}

#[test]
fn full_typeinfo_cache_lookup_variants() {
    for (sql, params, expected) in [
        (
            include_str!("fixtures/jdbc_oid_simple.sql"),
            vec![s("int4")],
            vec![i(23), s("int4")],
        ),
        (
            include_str!("fixtures/jdbc_oid_array.sql"),
            vec![s("int4"), s("pg_catalog"), Value::Bool(false)],
            vec![i(1007), s("_int4")],
        ),
        (
            include_str!("fixtures/jdbc_oid_qualified.sql"),
            vec![s("text"), Value::Null, Value::Bool(true)],
            vec![i(25), s("text")],
        ),
        (
            include_str!("fixtures/jdbc_array_delimiter.sql"),
            vec![i(1007)],
            vec![s(",")],
        ),
        (
            include_str!("fixtures/jdbc_array_element.sql"),
            vec![i(1007)],
            vec![i(23), Value::Bool(true), s("pg_catalog"), s("int4")],
        ),
    ] {
        let p = plan(sql)
            .unwrap_or_else(|e| panic!("{e:?}: {sql}"))
            .unwrap();
        let r = execute(&p, &snapshot(), &params).unwrap_or_else(|e| panic!("{e:?}: {sql}"));
        assert_eq!(r.rows, vec![expected]);
    }
}
#[test]
fn captured_datagrip_discovery_and_absent_features() {
    assert_eq!(
        run(include_str!("fixtures/datagrip_schemas.sql")).rows[0][2],
        s("public")
    );
    assert_eq!(
        run(include_str!("fixtures/datagrip_databases.sql")).rows[0][1],
        s("giggles")
    );
    assert_eq!(
        run(include_str!("fixtures/datagrip_superuser.sql")).rows,
        vec![vec![Value::Bool(true)]]
    );
    for sql in [
        include_str!("fixtures/datagrip_existent_languages.sql"),
        include_str!("fixtures/datagrip_languages.sql"),
        include_str!("fixtures/datagrip_event_triggers.sql"),
    ] {
        assert!(run(sql).rows.is_empty());
    }
    assert!(matches!(
        execute(&plan(
            "select round(extract(epoch from pg_postmaster_start_time() at time zone 'UTC')) as startup_time"
        ).unwrap().unwrap(), &CatalogSnapshot::default(), &[]),
        Err(Error::Unsupported)
    ));
}
#[test]
fn types_rejected_before_empty_execution() {
    for sql in [
        "SELECT oid AND true FROM pg_attrdef",
        "SELECT oid + true FROM pg_attrdef",
        "SELECT 1 FROM pg_attrdef WHERE oid",
        "SELECT length(oid) FROM pg_attrdef",
        "SELECT true = oid FROM pg_attrdef",
        "SELECT true IN (1,2) FROM pg_attrdef",
        "SELECT case when 1 then 'x' else 'y' end FROM pg_attrdef",
        "SELECT oid FROM pg_attrdef JOIN pg_type ON 1",
        "SELECT 1 FROM pg_attrdef WHERE row_number() over () = 1",
        "SELECT coalesce(information_schema._pg_expandarray(array[1]),null) FROM pg_attrdef",
    ] {
        assert!(plan(sql).is_err(), "{sql}");
    }
}
#[test]
fn privileges_are_read_only_and_visibility_bounded() {
    // PG18: owners and superusers hold every table privilege (GD-0077 E2e).
    assert_eq!(run("SELECT has_table_privilege('inventory','SELECT'), has_table_privilege('inventory','UPDATE'), has_table_privilege('hidden','SELECT'), has_schema_privilege('public','USAGE'), has_schema_privilege('public','CREATE') FROM pg_database").rows,vec![vec![Value::Bool(true),Value::Bool(true),Value::Bool(false),Value::Bool(true),Value::Bool(false)]]);
}
#[test]
fn bounds_parameter_nesting_and_intermediate_arrays() {
    let p = plan("SELECT $1 FROM pg_database").unwrap().unwrap();
    let mut nested = Value::Null;
    for _ in 0..18 {
        nested = Value::Array(vec![nested]);
    }
    assert_eq!(
        execute(&p, &snapshot(), &[nested]).unwrap_err(),
        Error::Limit
    );
    let p = plan("SELECT array[$1,$1,$1] FROM pg_database")
        .unwrap()
        .unwrap();
    assert_eq!(
        execute(&p, &snapshot(), &[s(&"x".repeat(MAX_BYTES / 2))]).unwrap_err(),
        Error::Limit
    );
    let many = (0..40)
        .map(|i| format!("pg_database p{i}"))
        .collect::<Vec<_>>()
        .join(",");
    assert_eq!(
        plan(&format!("SELECT p0.oid FROM {many}")).unwrap_err(),
        Error::Limit
    );
}
#[test]
fn null_empty_and_escaped_patterns() {
    assert_eq!(run("SELECT 1 IN (null,1), 2 IN (null,1), 2 NOT IN (null,1), null IN (1), 'é' LIKE '_', 'a%b' LIKE 'a!%b' ESCAPE '!' FROM pg_database").rows,vec![vec![Value::Bool(true),Value::Null,Value::Null,Value::Null,Value::Bool(true),Value::Bool(true)]]);
    assert!(run("SELECT oid FROM pg_database LIMIT 0").rows.is_empty());
    assert_eq!(
        run("SELECT case when true then 1 else 1/0 end FROM pg_database").rows,
        vec![vec![i(1)]]
    );
}

#[test]
fn primary_index_namespace_collision_and_composite_links() {
    let mut sn = snapshot();
    let mut other = sn.tables[0].clone();
    other.id = 8;
    other.name = "inventory_pkey".into();
    sn.tables.push(other);
    let p=plan("SELECT c.oid, c.relname, c.reltype, t.typrelid FROM pg_class c JOIN pg_type t ON t.oid=c.reltype WHERE c.relnamespace=2200 ORDER BY c.oid").unwrap().unwrap();
    assert_eq!(
        execute(&p, &sn, &[]).unwrap().rows,
        vec![
            vec![i(65564), s("inventory"), i(65567), i(65564)],
            vec![i(65568), s("inventory_pkey"), i(65571), i(65568)]
        ]
    );
    let p = plan("SELECT relname FROM pg_class WHERE relnamespace=2200")
        .unwrap()
        .unwrap();
    let r = execute(&p, &sn, &[]).unwrap();
    let names = r
        .rows
        .iter()
        .map(|r| r[0].clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(names.len(), r.rows.len());
}
#[test]
fn exact_transport_and_parameter_descriptors() {
    let p=plan("SELECT $1::int4, current_schemas(true), (current_schemas(true))[1], $2 FROM pg_type WHERE oid=$3 AND $4").unwrap().unwrap();
    assert_eq!(
        p.parameter_types(),
        &[PgType::Int4, PgType::Text, PgType::Oid, PgType::Bool]
    );
    assert_eq!(
        describe(&p)
            .unwrap()
            .iter()
            .map(|f| f.data_type.oid())
            .collect::<Vec<_>>(),
        vec![23, 1003, 19, 25]
    );
    assert_eq!(PgType::Oid.type_size(), 4);
    assert_eq!(PgType::Name.type_size(), 64);
    assert_eq!(PgType::NameArray.type_size(), -1);
    let p = plan(
        "SELECT i.indkey,c.conkey FROM pg_index i JOIN pg_constraint c ON c.conindid=i.indexrelid",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        describe(&p)
            .unwrap()
            .iter()
            .map(|f| f.data_type.oid())
            .collect::<Vec<_>>(),
        vec![22, 1005]
    );
    assert_eq!(
        execute(&p, &snapshot(), &[]).unwrap().rows,
        vec![vec![
            Value::Array(vec![i(2), i(1)]),
            Value::Array(vec![i(2), i(1)])
        ]]
    );
}
#[test]
fn xmin_reports_current_catalog_generation() {
    assert_eq!(
        run("SELECT xmin FROM pg_namespace").rows,
        vec![vec![Value::Int(0)]; 3]
    );
    assert_eq!(
        run("SELECT xmin FROM pg_namespace").fields[0]
            .data_type
            .oid(),
        28
    );
}
#[test]
fn scalar_and_sort_semantics() {
    assert_eq!(
        run("SELECT coalesce(1,1/0), nullif(1,1), 2147483648 FROM pg_database").rows,
        vec![vec![i(1), Value::Null, i(2147483648)]]
    );
    assert_eq!(
        run("SELECT 2147483648 FROM pg_database").fields[0].data_type,
        PgType::Int8
    );
    assert!(plan("SELECT oid AS same,typname AS same FROM pg_type ORDER BY same").is_err());
    assert!(plan("SELECT try_cast(oid as bigint) FROM pg_type").is_err());
    assert_eq!(
        run("SELECT oid FROM pg_database WHERE null IS NULL AND false IS FALSE").rows,
        vec![vec![i(16384)]]
    );
}
#[test]
fn escaped_array_bytes_and_repeated_projection_admitted_atomically() {
    let value = s(&"\\".repeat(MAX_BYTES / 8));
    let p = plan("SELECT $1,$1,$1,$1 FROM pg_database")
        .unwrap()
        .unwrap();
    assert_eq!(
        execute(&p, &snapshot(), &[value]).unwrap_err(),
        Error::Limit
    );
    let p = plan("SELECT repeat('x',99999999) FROM pg_attrdef");
    assert_eq!(p.unwrap_err(), Error::Unsupported);
}

#[test]
fn arrays_and_formatted_type_names_are_typed() {
    let r = run(
        "SELECT array[1,2147483648],array[true,false],array[null,1],array['a','b'],format_type(23,-1),format_type(16,null),format_type(1007,-1),pg_get_userbyid(10) FROM pg_database",
    );
    assert_eq!(
        r.fields
            .iter()
            .map(|f| f.data_type.oid())
            .collect::<Vec<_>>(),
        vec![1016, 1000, 1007, 1009, 25, 25, 25, 19]
    );
    assert_eq!(
        &r.rows[0][4..],
        &[s("integer"), s("boolean"), s("integer[]"), s("giggles")]
    );
    assert_eq!(PgType::Int4Array.array_element(), Some(PgType::Int4));
    assert_eq!(PgType::Int2Vector.array_element(), None);
}
#[test]
fn invalid_bind_rejected_on_empty_relations() {
    let p = plan("SELECT oid FROM pg_attrdef WHERE oid=$1")
        .unwrap()
        .unwrap();
    assert_eq!(
        execute(&p, &snapshot(), &[s("wrong")]).unwrap_err(),
        Error::Type
    );
    assert_eq!(execute(&p, &snapshot(), &[i(-1)]).unwrap_err(), Error::Type);
    assert!(
        execute(&p, &snapshot(), &[Value::Null])
            .unwrap()
            .rows
            .is_empty()
    );
}
#[test]
fn maximum_oid_boundary_and_no_pk() {
    let mut sn = snapshot();
    sn.tables[0].id = (u32::MAX as u64 - 3 - 65536) / 4;
    sn.tables[0].primary_key.clear();
    let p = plan("SELECT oid,reltype,relhasindex FROM pg_class WHERE relnamespace=2200")
        .unwrap()
        .unwrap();
    assert_eq!(
        execute(&p, &sn, &[]).unwrap().rows,
        vec![vec![
            i(u32::MAX as i64 - 3),
            i(u32::MAX as i64),
            Value::Bool(false)
        ]]
    );
    let p = plan(include_str!("fixtures/jdbc_pk.sql")).unwrap().unwrap();
    assert!(execute(&p, &sn, &[]).unwrap().rows.is_empty());
    sn.tables[0].id += 1;
    assert_eq!(execute(&p, &sn, &[]).unwrap_err(), Error::InvalidSnapshot);
}
#[test]
fn every_attribute_references_an_actual_type_and_index_shape() {
    let r = run(
        "SELECT a.atttypid FROM pg_attribute a LEFT JOIN pg_type t ON a.atttypid=t.oid WHERE t.oid IS NULL",
    );
    assert!(r.rows.is_empty());
    let r = run(
        "SELECT a.attname,a.attnum FROM pg_attribute a JOIN pg_index i ON a.attrelid=i.indexrelid ORDER BY a.attnum",
    );
    assert_eq!(r.rows, vec![vec![s("sku"), i(1)], vec![s("tenant"), i(2)]]);
}
#[test]
fn row_number_work_bound_without_output_overflow() {
    let mut sn = CatalogSnapshot::default();
    for id in 0..128 {
        sn.tables.push(TableDef {
            id,
            name: format!("table_{id}"),
            columns: (0..12)
                .map(|n| ColumnDef {
                    id: n,
                    name: format!("col_{n}"),
                    data_type: ScalarType::Integer,
                    not_null: false,
                })
                .collect(),
            primary_key: vec![],
        });
    }
    let p = plan("SELECT row_number() OVER () FROM pg_attribute")
        .unwrap()
        .unwrap();
    assert_eq!(execute(&p, &sn, &[]).unwrap_err(), Error::Limit);
}
#[test]
fn large_in_and_repeated_sort_aliases_stay_bounded() {
    let list = (0..1000)
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let order = std::iter::repeat_n("answer", 1000)
        .collect::<Vec<_>>()
        .join(",");
    let r = run(&format!(
        "SELECT oid IN ({list}) AS answer FROM pg_database ORDER BY {order}"
    ));
    assert_eq!(r.rows, vec![vec![Value::Bool(false)]]);
}
#[test]
fn malformed_and_nested_sql_never_panics() {
    for seed in 0..180 {
        let sql = match seed % 6 {
            0 => format!(
                "SELECT {}oid{} FROM pg_class",
                "(".repeat(seed),
                ")".repeat(seed)
            ),
            1 => format!("SELECT oid FROM pg_class WHERE oid=$-{seed}"),
            2 => format!("SELECT oid FROM pg_class /* {}", "*".repeat(seed)),
            3 => format!("SELECT '{}' FROM pg_class WHERE ", "'".repeat(seed)),
            4 => format!("SELECT oid FROM pg_class LIMIT {seed}99999999999999999999999999"),
            _ => format!(
                "SELECT oid FROM pg_class WHERE oid IN ({})",
                std::iter::repeat_n("null", seed)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        };
        assert!(std::panic::catch_unwind(|| plan(&sql)).is_ok(), "{seed}");
    }
}

#[test]
fn rejects_srf_and_window_misuse_on_empty_sources() {
    for sql in [
        "SELECT (case when true then information_schema._pg_expandarray(array[1::smallint]) else null end).n FROM pg_attrdef",
        "SELECT oid FROM pg_attrdef ORDER BY information_schema._pg_expandarray(array[1::smallint])",
        "SELECT row_number() OVER (ORDER BY row_number() OVER ()) FROM pg_attrdef",
    ] {
        assert!(plan(sql).is_err(), "{sql}");
    }
}
#[test]
fn function_schema_cannot_spoof_catalog_builtins() {
    for sql in [
        "SELECT evil.current_database() FROM pg_database",
        "SELECT information_schema.current_database() FROM pg_database",
        "SELECT pg_catalog._pg_expandarray(array[1::smallint]) FROM pg_database",
        "SELECT _pg_expandarray(array[1::smallint]) FROM pg_database",
    ] {
        assert!(plan(sql).is_err(), "{sql}");
    }
    assert_eq!(
        run("SELECT pg_catalog.current_database() FROM pg_database").rows,
        vec![vec![s("giggles")]]
    );
}

#[test]
fn postgres_int2vector_lower_bound_differs_from_arrays() {
    let r = run(
        "SELECT i.indkey[0],i.indkey[1],i.indkey[2],array_lower(i.indkey,1),array_upper(i.indkey,1),c.conkey[0],c.conkey[1] FROM pg_index i JOIN pg_constraint c ON c.conindid=i.indexrelid",
    );
    assert_eq!(
        r.rows,
        vec![vec![i(2), i(1), Value::Null, i(0), i(1), Value::Null, i(2)]]
    );
}

#[test]
fn numeric_float_type_rows_are_precise() {
    let r = run(
        "SELECT oid, typname, typlen, typcategory, typispreferred, typarray FROM pg_catalog.pg_type WHERE oid IN (700, 701, 1700) ORDER BY oid",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![
                i(700),
                s("float4"),
                i(4),
                s("N"),
                Value::Bool(false),
                i(1021)
            ],
            vec![
                i(701),
                s("float8"),
                i(8),
                s("N"),
                Value::Bool(true),
                i(1022)
            ],
            vec![
                i(1700),
                s("numeric"),
                i(-1),
                s("N"),
                Value::Bool(false),
                i(1231)
            ],
        ]
    );
}

#[test]
fn temporal_catalog_identity_width_and_category() {
    let result = run(
        "SELECT oid, typname, typlen, typcategory FROM pg_catalog.pg_type WHERE oid IN (1082,1083,1266,1114,1184,1186) ORDER BY oid",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![i(1082), s("date"), i(4), s("D")],
            vec![i(1083), s("time"), i(8), s("D")],
            vec![i(1114), s("timestamp"), i(8), s("D")],
            vec![i(1184), s("timestamptz"), i(8), s("D")],
            vec![i(1186), s("interval"), i(16), s("T")],
            vec![i(1266), s("timetz"), i(12), s("D")],
        ]
    );
}

#[test]
fn mixed_snapshot_input_limit() {
    let sn = snapshot();
    let fields = vec![Field {
        name: "id".into(),
        data_type: PgType::Int4,
    }];
    let schemas = std::collections::BTreeMap::from([("app".into(), fields.clone())]);
    let mixed = plan_gateway_with_relations(
        "SELECT count(t.*) FROM pg_class c LEFT JOIN app t ON true WHERE c.relname = 'absent'",
        &schemas,
    )
    .unwrap()
    .unwrap();
    let mut sn = sn;
    sn.user_relations.insert(
        "app".into(),
        ResultSet {
            fields,
            rows: vec![vec![Value::Int(1)]; MAX_ROWS + 1],
        },
    );
    assert_eq!(execute(&mixed, &sn, &[]), Err(Error::Limit));
    let counts = plan_gateway_with_relations(
        "SELECT count(t.*), count(*), count(t.id) FROM pg_class c LEFT JOIN app t ON true WHERE c.oid = 1259",
        &schemas,
    ).unwrap().unwrap();
    sn.user_relations.get_mut("app").unwrap().rows.clear();
    assert_eq!(
        execute(&counts, &sn, &[]).unwrap().rows,
        vec![vec![i(0), i(1), i(0)]]
    );
    sn.user_relations
        .get_mut("app")
        .unwrap()
        .rows
        .push(vec![Value::Null]);
    assert_eq!(
        execute(&counts, &sn, &[]).unwrap().rows,
        vec![vec![i(1), i(1), i(0)]]
    );
    let empty = plan("SELECT count(*) FROM pg_class WHERE oid = 0")
        .unwrap()
        .unwrap();
    assert_eq!(execute(&empty, &sn, &[]).unwrap().rows, vec![vec![i(0)]]);
}
