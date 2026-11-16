use giggles_pg_catalog::*;

fn run(snapshot: &CatalogSnapshot, sql: &str) -> ResultSet {
    let p = plan(sql).expect(sql).expect("catalog query");
    let result = execute(&p, snapshot, &[]).expect(sql);
    assert_eq!(describe(&p).unwrap(), result.fields);
    result
}
fn role(oid: u32, name: &str, login: bool) -> RoleDef {
    RoleDef {
        oid,
        name: name.into(),
        superuser: false,
        login,
        createrole: true,
        createdb: false,
        inherit: false,
        replication: true,
        bypassrls: false,
        connlimit: 7,
    }
}
fn s(s: &str) -> Value {
    Value::Text(s.into())
}

#[test]
fn build_identity_and_immutable_capabilities_are_pinned() {
    assert_eq!(
        CAPABILITIES,
        "add_column_if_not_exists,role_session_termination,server_encoding,union_common_type"
    );
    let snapshot = CatalogSnapshot::default();
    for sql in [
        "SELECT current_setting('giggles_capabilities') FROM pg_catalog.pg_roles LIMIT 1",
        "SELECT pg_catalog.current_setting('GiGgLeS_CaPaBiLiTiEs') FROM pg_catalog.pg_roles LIMIT 1",
        "SELECT current_setting('giggles_capabilities', true) FROM pg_catalog.pg_roles LIMIT 1",
        "SELECT pg_catalog.current_setting('GiGgLeS_CaPaBiLiTiEs', false) FROM pg_catalog.pg_roles LIMIT 1",
    ] {
        let result = run(&snapshot, sql);
        assert_eq!(result.fields[0].data_type, PgType::Text, "{sql}");
        assert_eq!(result.rows, vec![vec![s(CAPABILITIES)]], "{sql}");
    }
    let expected = format!("PostgreSQL 18.0 (GigglesDB 0.1.0+{BUILD_ID})");
    assert!(
        regex::Regex::new(r"^PostgreSQL 18\.0 \(GigglesDB 0\.1\.0\+([0-9a-f]{12}|unknown)\)$")
            .unwrap()
            .is_match(&expected)
    );
    for sql in [
        "SELECT version()",
        "SELECT pg_catalog.version() FROM pg_catalog.pg_roles LIMIT 1",
    ] {
        let result = run(&snapshot, sql);
        assert_eq!(result.rows, vec![vec![s(&expected)]], "{sql}");
    }
    println!("observed version(): {expected}");
}
fn i(i: i64) -> Value {
    Value::Int(i)
}

#[test]
fn pg18_predefined_roles_and_default_bootstrap_are_exact() {
    let snapshot = CatalogSnapshot::default();
    assert_eq!(snapshot.roles.len(), 1);
    assert_eq!(
        snapshot.roles[0],
        RoleDef {
            oid: 10,
            name: "giggles".into(),
            superuser: true,
            login: true,
            createrole: true,
            createdb: true,
            inherit: true,
            replication: true,
            bypassrls: true,
            connlimit: -1
        }
    );
    assert!(snapshot.table_owners.is_empty());
    assert_eq!(snapshot.database_owner, 10);
    let actual = run(
        &snapshot,
        "SELECT oid,rolname,rolsuper,rolinherit,rolcreaterole,rolcreatedb,rolcanlogin,rolreplication,rolconnlimit,rolpassword,rolvaliduntil,rolbypassrls,rolconfig FROM pg_roles ORDER BY oid",
    );
    // Captured from PostgreSQL 18; bootstrap identity is the only substitution.
    let expected: Vec<Vec<Value>> = include_str!("fixtures/pg18_roles.psv")
        .lines()
        .skip(1)
        .filter(|line| !line.starts_with('('))
        .map(|line| {
            line.split('|')
                .enumerate()
                .map(|(n, cell)| match n {
                    0 | 8 => i(cell.parse().unwrap()),
                    2..=7 | 11 => Value::Bool(cell == "t"),
                    10 | 12 => Value::Null,
                    1 if cell == "postgres" => s("giggles"),
                    _ => s(cell),
                })
                .collect()
        })
        .collect();
    assert_eq!(actual.rows, expected);
    assert_eq!(actual.rows.len(), 17); // bootstrap plus exactly sixteen predefined roles
}

#[test]
fn role_relation_schemas_match_pg18_order_and_types() {
    use PgType::*;
    let role_fields = vec![
        ("rolname", Name),
        ("rolsuper", Bool),
        ("rolinherit", Bool),
        ("rolcreaterole", Bool),
        ("rolcreatedb", Bool),
        ("rolcanlogin", Bool),
        ("rolreplication", Bool),
        ("rolconnlimit", Int4),
        ("rolpassword", Text),
        ("rolvaliduntil", Text),
        ("rolbypassrls", Bool),
        ("rolconfig", TextArray),
        ("oid", Oid),
    ];
    let auth_fields = vec![
        ("oid", Oid),
        ("rolname", Name),
        ("rolsuper", Bool),
        ("rolinherit", Bool),
        ("rolcreaterole", Bool),
        ("rolcreatedb", Bool),
        ("rolcanlogin", Bool),
        ("rolreplication", Bool),
        ("rolbypassrls", Bool),
        ("rolconnlimit", Int4),
        ("rolpassword", Text),
        ("rolvaliduntil", Text),
    ];
    let user_fields = vec![
        ("usename", Name),
        ("usesysid", Oid),
        ("usecreatedb", Bool),
        ("usesuper", Bool),
        ("userepl", Bool),
        ("usebypassrls", Bool),
        ("passwd", Text),
        ("valuntil", Text),
        ("useconfig", TextArray),
    ];
    let member_fields = vec![
        ("oid", Oid),
        ("roleid", Oid),
        ("member", Oid),
        ("grantor", Oid),
        ("admin_option", Bool),
        ("inherit_option", Bool),
        ("set_option", Bool),
    ];
    for (name, expected) in [
        ("pg_roles", role_fields),
        ("pg_authid", auth_fields),
        ("pg_user", user_fields.clone()),
        ("pg_shadow", user_fields),
        ("pg_auth_members", member_fields),
    ] {
        let result = run(
            &CatalogSnapshot::default(),
            &format!("SELECT * FROM {name}"),
        );
        assert_eq!(
            result
                .fields
                .iter()
                .map(|f| (f.name.as_str(), f.data_type))
                .collect::<Vec<_>>(),
            expected,
            "{name}"
        );
        if name == "pg_auth_members" {
            assert!(result.rows.is_empty());
        }
    }
}

#[test]
fn custom_flags_login_filter_and_password_redaction() {
    let mut snapshot = CatalogSnapshot::default();
    snapshot
        .roles
        .extend([role(20000, "alice", true), role(20001, "group_role", false)]);
    assert_eq!(run(&snapshot,"SELECT usename,usesysid,usecreatedb,usesuper,userepl,usebypassrls,passwd,valuntil,useconfig FROM pg_user ORDER BY usesysid").rows,
        vec![vec![s("giggles"),i(10),Value::Bool(true),Value::Bool(true),Value::Bool(true),Value::Bool(true),s("********"),Value::Null,Value::Null],vec![s("alice"),i(20000),Value::Bool(false),Value::Bool(false),Value::Bool(true),Value::Bool(false),s("********"),Value::Null,Value::Null]]);
    assert_eq!(run(&snapshot,"SELECT rolsuper,rolinherit,rolcreaterole,rolcreatedb,rolcanlogin,rolreplication,rolbypassrls,rolconnlimit FROM pg_roles WHERE oid=20000").rows,
        vec![vec![Value::Bool(false),Value::Bool(false),Value::Bool(true),Value::Bool(false),Value::Bool(true),Value::Bool(true),Value::Bool(false),i(7)]]);
    assert_eq!(
        run(
            &snapshot,
            "SELECT usename,usesysid FROM pg_shadow ORDER BY usesysid"
        )
        .rows,
        vec![vec![s("giggles"), i(10)], vec![s("alice"), i(20000)]]
    );
    assert_eq!(run(&snapshot,"SELECT oid,rolname,rolsuper,rolinherit,rolcreaterole,rolcreatedb,rolcanlogin,rolreplication,rolbypassrls,rolconnlimit FROM pg_authid ORDER BY oid").rows, run(&snapshot,"SELECT oid,rolname,rolsuper,rolinherit,rolcreaterole,rolcreatedb,rolcanlogin,rolreplication,rolbypassrls,rolconnlimit FROM pg_roles ORDER BY oid").rows);
    for sql in [
        "SELECT rolpassword,rolvaliduntil FROM pg_authid",
        "SELECT passwd,valuntil FROM pg_shadow",
    ] {
        assert!(
            run(&snapshot, sql)
                .rows
                .iter()
                .all(|r| r == &vec![Value::Null, Value::Null])
        );
    }
    assert_eq!(run(&snapshot,"SELECT pg_get_userbyid(10),pg_get_userbyid(6171),pg_get_userbyid(20000),pg_get_userbyid(123456),pg_get_userbyid(NULL::oid)").rows,
        vec![vec![s("giggles"),s("pg_database_owner"),s("alice"),s("unknown (OID=123456)"),Value::Null]]);
}

fn owned_snapshot() -> CatalogSnapshot {
    CatalogSnapshot {
        tables: vec![TableDef {
            id: 7,
            name: "owned".into(),
            columns: vec![ColumnDef {
                id: 1,
                name: "id".into(),
                data_type: ScalarType::Integer,
                not_null: true,
            }],
            primary_key: vec![0],
        }],
        ..CatalogSnapshot::default()
    }
}
#[test]
fn owners_follow_snapshot_with_oid10_fallback() {
    let mut snapshot = owned_snapshot();
    snapshot.roles.push(role(20000, "alice", true));
    snapshot.indexes = vec![
        IndexDef {
            id: 8,
            table: 7,
            name: "owned_secondary".into(),
            columns: vec![1],
            unique: false,
        },
        IndexDef {
            id: 9,
            table: 7,
            name: "owned_unique".into(),
            columns: vec![1],
            unique: true,
        },
    ];
    snapshot.database = "actual_db".into();
    snapshot.user = "session_identity_is_not_owner".into();
    for owner in [10, 20000] {
        snapshot.database_owner = owner;
        if owner == 20000 {
            snapshot.table_owners.insert(7, owner);
        }
        assert_eq!(
            run(
                &snapshot,
                "SELECT relname,relowner FROM pg_class WHERE relnamespace=2200 ORDER BY relname"
            )
            .rows,
            vec![
                vec![s("owned"), i(owner.into())],
                vec![s("owned_pkey"), i(owner.into())],
                vec![s("owned_secondary"), i(owner.into())],
                vec![s("owned_unique"), i(owner.into())]
            ]
        );
        assert_eq!(
            run(
                &snapshot,
                "SELECT typowner FROM pg_type WHERE typname='owned'"
            )
            .rows,
            vec![vec![i(owner.into())]]
        );
        assert_eq!(
            run(&snapshot, "SELECT datname,datdba FROM pg_database").rows,
            vec![vec![s("actual_db"), i(owner.into())]]
        );
    }
    assert_eq!(
        run(
            &snapshot,
            "SELECT nspname,nspowner FROM pg_namespace ORDER BY nspname"
        )
        .rows,
        vec![
            vec![s("information_schema"), i(10)],
            vec![s("pg_catalog"), i(10)],
            vec![s("public"), i(6171)]
        ]
    );
}

#[test]
fn invalid_role_and_owner_snapshots_fail_closed() {
    let base = owned_snapshot();
    let mut cases = Vec::new();
    let mut x = base.clone();
    x.roles.push(role(10, "duplicate_oid", true));
    cases.push(("duplicate oid", x));
    let mut x = base.clone();
    x.roles.push(role(20000, "giggles", true));
    cases.push(("duplicate name", x));
    let mut x = base.clone();
    x.roles.push(role(20000, "", true));
    cases.push(("empty name", x));
    let mut x = base.clone();
    x.roles.clear();
    cases.push(("missing oid10", x));
    let mut x = base.clone();
    x.roles.push(role(6171, "reserved_oid", true));
    cases.push(("predefined oid", x));
    let mut x = base.clone();
    x.roles.push(role(20000, "pg_reserved", true));
    cases.push(("reserved name", x));
    let mut x = base.clone();
    x.table_owners.insert(8, 10);
    cases.push(("unknown table", x));
    let mut x = base.clone();
    x.table_owners.insert(7, 123456);
    cases.push(("unknown table owner", x));
    let mut x = base.clone();
    x.database_owner = 123456;
    cases.push(("unknown database owner", x));
    let p = plan("SELECT oid FROM pg_roles").unwrap().unwrap();
    for (label, snapshot) in cases {
        assert_eq!(
            execute(&p, &snapshot, &[]),
            Err(Error::InvalidSnapshot),
            "{label}"
        );
    }
}

#[test]
fn reflected_role_relation_oids_are_unique_and_builtin_owners_are_bootstrap() {
    let snapshot = CatalogSnapshot::default();
    let rows = run(&snapshot, "SELECT oid FROM pg_class").rows;
    let unique: std::collections::BTreeSet<_> = rows
        .iter()
        .map(|row| match row[0] {
            Value::Int(oid) => oid,
            _ => panic!("oid"),
        })
        .collect();
    assert_eq!(unique.len(), rows.len());
    for (name, oid) in [
        ("pg_roles", 12000),
        ("pg_authid", 1260),
        ("pg_user", 12014),
        ("pg_shadow", 12005),
        ("pg_auth_members", 1261),
    ] {
        assert_eq!(
            run(
                &snapshot,
                &format!("SELECT oid,relowner FROM pg_class WHERE relname='{name}'")
            )
            .rows,
            vec![vec![i(oid), i(10)]]
        );
        assert_eq!(
            run(
                &snapshot,
                &format!("SELECT typowner FROM pg_type WHERE typname='{name}'")
            )
            .rows,
            vec![vec![i(10)]]
        );
    }
    assert_eq!(
        run(
            &snapshot,
            "SELECT typowner FROM pg_type WHERE typname='int4'"
        )
        .rows,
        vec![vec![i(10)]]
    );
}

#[test]
fn role_count_and_name_bytes_share_catalog_limits() {
    let p = plan("SELECT oid FROM pg_roles").unwrap().unwrap();
    let mut snapshot = CatalogSnapshot::default();
    snapshot
        .roles
        .extend((0..MAX_ROWS - 16).map(|n| role(20000 + n as u32, &format!("r{n}"), false)));
    assert_eq!(execute(&p, &snapshot, &[]), Err(Error::Limit));
    let mut snapshot = CatalogSnapshot::default();
    snapshot.roles[0].name = "x".repeat(65537);
    assert_eq!(execute(&p, &snapshot, &[]), Err(Error::Limit));
}

#[test]
fn server_encoding_is_an_immutable_utf8_setting_in_catalog_queries() {
    let snapshot = CatalogSnapshot::default();
    for sql in [
        "SELECT current_setting('server_encoding') FROM pg_catalog.pg_roles LIMIT 1",
        "SELECT pg_catalog.current_setting('server_encoding') FROM pg_catalog.pg_roles LIMIT 1",
        "SELECT current_setting('SeRvEr_EnCoDiNg') FROM pg_catalog.pg_roles LIMIT 1",
        "SELECT pg_catalog.current_setting('SeRvEr_EnCoDiNg') FROM pg_catalog.pg_roles LIMIT 1",
    ] {
        let result = run(&snapshot, sql);
        assert_eq!(result.fields.len(), 1, "{sql}");
        assert_eq!(result.fields[0].data_type, PgType::Text, "{sql}");
        assert_eq!(result.rows, vec![vec![s("UTF8")]], "{sql}");
    }
}
