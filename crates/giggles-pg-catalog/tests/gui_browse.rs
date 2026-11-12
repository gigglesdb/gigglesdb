use giggles_pg_catalog::*;

fn run(sql: &str, snapshot: &CatalogSnapshot) -> ResultSet {
    execute(&plan(sql).unwrap().expect("catalog"), snapshot, &[]).unwrap()
}

#[test]
fn django_descriptions_and_search_path_visibility() {
    let mut snapshot = CatalogSnapshot::default();
    snapshot.schemas.insert("gui_sales".into(), (70000, 10));
    snapshot.schemas.insert("gui_aux".into(), (70001, 10));
    for (id, schema) in [(10, "gui_sales"), (20, "gui_aux")] {
        snapshot.tables.push(TableDef {
            id,
            name: "parent".into(),
            primary_key: vec![0],
            columns: vec![ColumnDef {
                id,
                name: "id".into(),
                data_type: ScalarType::Integer,
                not_null: true,
            }],
        });
        snapshot.table_schemas.insert(id, schema.into());
    }
    let sql = "SELECT n.nspname,pg_catalog.pg_table_is_visible(c.oid),obj_description(c.oid),obj_description(c.oid,'pg_class'),col_description(c.oid,1),shobj_description(c.oid,'pg_class') FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.relname='parent' ORDER BY n.nspname";
    for (path, expected) in [
        (vec!["gui_sales", "gui_aux"], [false, true]),
        (vec!["gui_aux", "gui_sales"], [true, false]),
        (vec!["public"], [false, false]),
    ] {
        snapshot.search_path = path.into_iter().map(str::to_owned).collect();
        let rows = run(sql, &snapshot).rows;
        assert_eq!(rows.len(), 2);
        for (row, visible) in rows.iter().zip(expected) {
            assert_eq!(row[1], Value::Bool(visible));
            assert_eq!(&row[2..], vec![Value::Null; 4]);
        }
    }
    snapshot.search_path = vec!["gui_sales".into()];
    assert_eq!(run("SELECT c.relname,pg_get_constraintdef(con.oid) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_depend d ON d.classid=c.tableoid AND d.objid=c.oid AND d.refclassid='pg_constraint'::regclass AND d.deptype='i' JOIN pg_constraint con ON con.tableoid=d.refclassid AND con.oid=d.refobjid WHERE pg_table_is_visible(c.oid)", &snapshot).rows,
        vec![vec![Value::Text("parent_pkey".into()), Value::Text("PRIMARY KEY (id)".into())]]);
    assert_eq!(run("SELECT c.relname, CASE WHEN c.relispartition THEN 'p' WHEN c.relkind IN ('m','v') THEN 'v' ELSE 't' END, obj_description(c.oid, 'pg_class') FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind IN ('f','m','p','r','v') AND n.nspname NOT IN ('pg_catalog','pg_toast') AND pg_catalog.pg_table_is_visible(c.oid)", &snapshot).rows,
        vec![vec![Value::Text("parent".into()), Value::Text("t".into()), Value::Null]]);
    assert_eq!(run("SELECT pg_table_is_visible(0::oid),pg_table_is_visible(NULL::oid),obj_description(0::oid),obj_description(NULL::oid,'pg_class'),col_description(0::oid,1),shobj_description(0::oid,'pg_database')", &snapshot).rows,
        vec![vec![Value::Null; 6]]);
    assert_eq!(
        run(
            "SELECT pg_table_is_visible('pg_class'::regclass)",
            &snapshot
        )
        .rows,
        vec![vec![Value::Bool(true)]]
    );
    snapshot.database = "postgres".into();
    assert_eq!(
        run(
            "SELECT shobj_description(oid,'pg_database') FROM pg_database",
            &snapshot
        )
        .rows,
        vec![vec![Value::Text(
            "default administrative connection database".into()
        )]]
    );
}

#[test]
fn catalog_target_srfs_zip_and_preserve_element_types() {
    let snapshot = CatalogSnapshot::default();
    let result = run(
        "SELECT unnest(ARRAY[2::int2,3::int2]),unnest(ARRAY[7,8,9]),unnest(ARRAY['a']) FROM pg_namespace WHERE oid=11",
        &snapshot,
    );
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| f.data_type)
            .collect::<Vec<_>>(),
        vec![PgType::Int2, PgType::Int4, PgType::Text]
    );
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(2), Value::Int(7), Value::Text("a".into())],
            vec![Value::Int(3), Value::Int(8), Value::Null],
            vec![Value::Null, Value::Int(9), Value::Null]
        ]
    );
    assert_eq!(run("SELECT unnest(ARRAY(SELECT generate_series(1,3))),unnest(ARRAY[4,5]) FROM pg_namespace WHERE oid=11", &snapshot).rows,
        vec![vec![Value::Int(1),Value::Int(4)],vec![Value::Int(2),Value::Int(5)],vec![Value::Int(3),Value::Null]]);
    assert!(
        run(
            "SELECT unnest(NULL::int4[]) FROM pg_namespace WHERE oid=11",
            &snapshot
        )
        .rows
        .is_empty()
    );
    assert_eq!(run("SELECT (information_schema._pg_expandarray(ARRAY[NULL::int2])).n,unnest(ARRAY[4,5]) FROM pg_namespace WHERE oid=11", &snapshot).rows,
        vec![vec![Value::Int(1),Value::Int(4)],vec![Value::Null,Value::Int(5)]]);
}

#[test]
fn gui_substring_numeric_and_regex_forms() {
    let result = run(
        "SELECT substring('abc',2),substring('abc',0,2),substring('abc',-2,4),substring('éclair',2,3),substring('fillfactor=70' FROM 'fillfactor=([0-9]*)'),substring('abc' FROM 'z'),substring(NULL::text,1) FROM pg_namespace WHERE oid=11",
        &CatalogSnapshot::default(),
    );
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Text("bc".into()),
            Value::Text("a".into()),
            Value::Text("a".into()),
            Value::Text("cla".into()),
            Value::Text("70".into()),
            Value::Null,
            Value::Null
        ]]
    );
}

#[test]
fn qualified_wildcard_excludes_system_columns_and_preserves_duplicates() {
    let snapshot = CatalogSnapshot::default();
    let result = run(
        "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass ORDER BY nspname",
        &snapshot,
    );
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>(),
        ["oid", "oid", "nspname", "nspowner", "nspacl", "description"]
    );
    assert!(!result.rows.is_empty());
    assert!(
        result
            .rows
            .iter()
            .all(|r| r[0] == r[1] && r[5] == Value::Null)
    );
    assert_eq!(
        plan("SELECT missing.* FROM pg_namespace n").unwrap_err(),
        Error::UnknownColumn
    );
}

#[test]
fn pgadmin_connection_catalog_queries() {
    let mut snapshot = CatalogSnapshot {
        database: "postgres".into(),
        backend_pid: 123,
        ..Default::default()
    };
    let result = run(
        "SELECT db.oid, db.datname, has_database_privilege(db.oid,'CREATE'), db.dattablespace FROM pg_catalog.pg_database db WHERE db.datname=current_database()",
        &snapshot,
    );
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Int(16384),
            Value::Text(snapshot.database.clone()),
            Value::Bool(true),
            Value::Int(1663)
        ]]
    );
    assert_eq!(run("SELECT gss_authenticated, encrypted FROM pg_catalog.pg_stat_gssapi WHERE pid=pg_backend_pid()", &snapshot).rows, vec![vec![Value::Bool(false), Value::Bool(false)]]);
    assert_eq!(run("SELECT CASE WHEN (SELECT count(extname) FROM pg_catalog.pg_extension WHERE extname='bdr') > 0 THEN 'pgd' WHEN (SELECT count(*) FROM pg_replication_slots) > 0 THEN 'log' ELSE NULL END AS type", &snapshot).rows, vec![vec![Value::Null]]);
    assert_eq!(run("SELECT CASE WHEN usesuper AND pg_is_in_recovery() THEN pg_is_wal_replay_paused() ELSE FALSE END FROM pg_user WHERE usename=current_user", &snapshot).rows, vec![vec![Value::Bool(false)]]);
    assert_eq!(run("SELECT t.spcname, pg_tablespace_location(t.oid), d.description FROM pg_database b JOIN pg_tablespace t ON t.oid=b.dattablespace JOIN pg_shdescription d ON d.objoid=b.oid AND d.classoid='pg_database'::regclass", &snapshot).rows, vec![vec![Value::Text("pg_default".into()), Value::Text(String::new()), Value::Text("default administrative connection database".into())]]);
    snapshot.roles[0].superuser = false;
    snapshot.database_owner = 999;
    let mut owner = snapshot.roles[0].clone();
    owner.oid = 999;
    owner.name = "owner".into();
    snapshot.roles.push(owner);
    assert_eq!(run("SELECT has_database_privilege(16384::oid,'CREATE'), has_database_privilege(16384::oid,'CONNECT'), has_database_privilege(0::oid,'CREATE')", &snapshot).rows, vec![vec![Value::Bool(false), Value::Bool(true), Value::Null]]);
}

#[test]
fn pgadmin_role_recursion_stops_at_absent_memberships() {
    let result = run(
        "SELECT roles.rolname, 'pg_signal_backend'=ANY(ARRAY(WITH RECURSIVE cte AS (SELECT pg_roles.oid,pg_roles.rolname FROM pg_roles WHERE pg_roles.oid=roles.oid UNION ALL SELECT m.roleid,pgr.rolname FROM cte cte_1 JOIN pg_auth_members m ON m.member=cte_1.oid JOIN pg_roles pgr ON pgr.oid=m.roleid) SELECT rolname FROM cte)) FROM pg_roles roles WHERE rolname=current_user",
        &CatalogSnapshot::default(),
    );
    assert_eq!(
        result.rows,
        vec![vec![Value::Text("giggles".into()), Value::Bool(false)]]
    );
    assert!(plan("WITH RECURSIVE cte AS (SELECT oid FROM pg_roles UNION ALL SELECT oid FROM cte) SELECT * FROM cte").is_err());
}

#[test]
fn gui_acl_records_and_heap_system_attributes() {
    let mut snapshot = CatalogSnapshot::default();
    snapshot.tables.push(TableDef {
        id: 10,
        name: "child".into(),
        primary_key: vec![0],
        columns: vec![ColumnDef {
            id: 10,
            name: "id".into(),
            data_type: ScalarType::Integer,
            not_null: true,
        }],
    });
    for column in ["relacl", "attacl"] {
        let relation = if column == "relacl" {
            "pg_class"
        } else {
            "pg_attribute"
        };
        let result = run(
            &format!(
                "SELECT (d).grantor,(d).grantee,(d).privilege_type,(d).is_grantable FROM (SELECT aclexplode({column}) d FROM {relation}) a"
            ),
            &snapshot,
        );
        assert!(result.rows.is_empty());
        assert_eq!(
            result
                .fields
                .iter()
                .map(|f| f.data_type)
                .collect::<Vec<_>>(),
            vec![PgType::Oid, PgType::Oid, PgType::Text, PgType::Bool]
        );
    }
    snapshot.roles[0].name = "a/=\"b".into();
    let result = run(
        "SELECT (d).grantor,(d).grantee,(d).privilege_type,(d).is_grantable FROM (SELECT aclexplode(acldefault('r',10)) d FROM pg_namespace WHERE oid=11) a",
        &snapshot,
    );
    assert_eq!(
        result.rows,
        [
            "INSERT",
            "SELECT",
            "UPDATE",
            "DELETE",
            "TRUNCATE",
            "REFERENCES",
            "TRIGGER",
            "MAINTAIN"
        ]
        .into_iter()
        .map(|p| vec![
            Value::Int(10),
            Value::Int(10),
            Value::Text(p.into()),
            Value::Bool(false)
        ])
        .collect::<Vec<_>>()
    );
    let result = run(
        "SELECT attname,attnum,atttypid,attlen,attnotnull,attbyval,attalign,attstorage,attislocal,attstattarget FROM pg_attribute WHERE attrelid=65576 AND attnum<0 ORDER BY attnum",
        &snapshot,
    );
    let expected = [
        (-6, "tableoid", 26, 4, true, "i"),
        (-5, "cmax", 29, 4, true, "i"),
        (-4, "xmax", 28, 4, true, "i"),
        (-3, "cmin", 29, 4, true, "i"),
        (-2, "xmin", 28, 4, true, "i"),
        (-1, "ctid", 27, 6, false, "s"),
    ];
    assert_eq!(
        result.rows,
        expected
            .into_iter()
            .map(|(n, name, ty, len, byval, align)| vec![
                Value::Text(name.into()),
                Value::Int(n),
                Value::Int(ty),
                Value::Int(len),
                Value::Bool(true),
                Value::Bool(byval),
                Value::Text(align.into()),
                Value::Text("p".into()),
                Value::Bool(true),
                Value::Null
            ])
            .collect::<Vec<_>>()
    );
}

#[test]
fn catalog_index_definition_uses_identity_and_search_path() {
    let mut snapshot = CatalogSnapshot::default();
    for (id, schema, column) in [(10, "gui_sales", "amount"), (20, "gui_aux", "id")] {
        snapshot
            .schemas
            .insert(schema.into(), (70000 + id as u32, 10));
        snapshot.table_schemas.insert(id, schema.into());
        snapshot.tables.push(TableDef {
            id,
            name: "child".into(),
            primary_key: vec![],
            columns: vec![ColumnDef {
                id,
                name: column.into(),
                data_type: ScalarType::Integer,
                not_null: false,
            }],
        });
        snapshot.indexes.push(IndexDef {
            id: id + 100,
            table: id,
            name: "child_idx".into(),
            columns: vec![id],
            unique: false,
        });
    }
    let sql = "SELECT pg_get_indexdef(c.oid,0,true),pg_get_indexdef(c.oid,0,false),pg_get_indexdef(c.oid,1,true) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname='child_idx' ORDER BY n.nspname";
    for path in ["public", "gui_sales", "gui_aux"] {
        snapshot.search_path = vec![path.into()];
        let result = run(sql, &snapshot);
        for (row, (schema, column)) in result
            .rows
            .iter()
            .zip([("gui_aux", "id"), ("gui_sales", "amount")])
        {
            let qualified =
                format!("CREATE INDEX child_idx ON {schema}.child USING btree ({column})");
            let pretty = if path == schema {
                format!("CREATE INDEX child_idx ON child USING btree ({column})")
            } else {
                qualified.clone()
            };
            assert_eq!(
                row,
                &vec![
                    Value::Text(pretty),
                    Value::Text(qualified),
                    Value::Text(column.into())
                ]
            );
        }
    }
}
