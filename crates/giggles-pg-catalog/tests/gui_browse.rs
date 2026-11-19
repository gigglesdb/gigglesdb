use giggles_pg_catalog::*;

#[test]
fn jdbc_trim_bitwise_and_pgadmin_text_array_casts() {
    let snapshot = CatalogSnapshot::default();
    let result = run("SELECT trim(both 'xy' from 'xyabcxy'),trim(leading 'é' from 'ééabé'),trim(trailing 'xy' from 'xyabxy'),trim(' abc '),trim(both '' from ' abc '),trim(NULL::text),5::smallint & 3::smallint,-1::bigint & 3::integer,NULL::smallint & 1::smallint,concat(NULL::text,'ab',NULL::text,'cd'),concat(NULL::text,NULL::text),(CASE WHEN true THEN ARRAY['ASC','NULLS LAST'] ELSE ARRAY['DESC','NULLS FIRST'] END)::text[] FROM pg_namespace WHERE oid=11", &snapshot);
    assert_eq!(result.fields[6].data_type, PgType::Int2);
    assert_eq!(result.fields[7].data_type, PgType::Int8);
    assert_eq!(result.fields[11].data_type, PgType::TextArray);
    assert_eq!(result.rows, vec![vec![Value::Text("abc".into()),Value::Text("abé".into()),Value::Text("xyab".into()),Value::Text("abc".into()),Value::Text(" abc ".into()),Value::Null,Value::Int(1),Value::Int(3),Value::Null,Value::Text("abcd".into()),Value::Text(String::new()),Value::Array(vec![Value::Text("ASC".into()),Value::Text("NULLS LAST".into())])]]);
    assert!(plan("SELECT true & false FROM pg_namespace").is_err());
    let expensive = plan(&format!("SELECT trim(both '{}' from '{}') FROM pg_namespace WHERE oid=11", "x".repeat(1500), "x".repeat(1500))).unwrap().unwrap();
    assert!(matches!(execute(&expensive, &snapshot, &[]), Err(Error::Limit)));
}

#[test]
fn pg18_catalog_star_order_matches_reflected_attributes() {
    let snapshot = CatalogSnapshot::default();
    for (name, names) in [
        ("pg_class", "oid,relname,relnamespace,reltype,reloftype,relowner,relam,relfilenode,reltablespace,relpages,reltuples,relallvisible,relallfrozen,reltoastrelid,relhasindex,relisshared,relpersistence,relkind,relnatts,relchecks,relhasrules,relhastriggers,relhassubclass,relrowsecurity,relforcerowsecurity,relispopulated,relreplident,relispartition,relrewrite,relfrozenxid,relminmxid,relacl,reloptions,relpartbound"),
        ("pg_constraint", "oid,conname,connamespace,contype,condeferrable,condeferred,conenforced,convalidated,conrelid,contypid,conindid,conparentid,confrelid,confupdtype,confdeltype,confmatchtype,conislocal,coninhcount,connoinherit,conperiod,conkey,confkey,conpfeqop,conppeqop,conffeqop,confdelsetcols,conexclop,conbin"),
        ("pg_index", "indexrelid,indrelid,indnatts,indnkeyatts,indisunique,indnullsnotdistinct,indisprimary,indisexclusion,indimmediate,indisclustered,indisvalid,indcheckxmin,indisready,indislive,indisreplident,indkey,indcollation,indclass,indoption,indexprs,indpred"),
    ] {
        let result = run(&format!("SELECT c.* FROM {name} c"), &snapshot);
        assert_eq!(result.fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>().join(","), names);
        let reflected = run(&format!("SELECT a.attname,a.atttypid FROM pg_attribute a WHERE a.attrelid='{name}'::regclass AND a.attnum>0 ORDER BY a.attnum"), &snapshot);
        assert_eq!(reflected.rows, result.fields.iter().map(|f| vec![Value::Text(f.name.clone()),Value::Int(i64::from(f.data_type.oid()))]).collect::<Vec<_>>());
    }
    assert_eq!(run("SELECT relkind,relfilenode,relrewrite,relisshared,relhassubclass,relpartbound FROM pg_class WHERE relname='pg_class'", &snapshot).rows,
        vec![vec![Value::Text("r".into()),Value::Int(0),Value::Int(0),Value::Bool(false),Value::Bool(false),Value::Null]]);
}

#[test]
fn pgadmin_nested_joins_and_projected_distinct_on() {
    let snapshot = CatalogSnapshot::default();
    assert_eq!(run("SELECT n.nspname,c.relname FROM pg_namespace n LEFT JOIN (pg_class c JOIN pg_namespace ns ON ns.oid=c.relnamespace AND c.relname='pg_class') ON c.relnamespace=n.oid WHERE n.oid=11", &snapshot).rows,
        vec![vec![Value::Text("pg_catalog".into()),Value::Text("pg_class".into())]]);
    assert_eq!(run("SELECT n.nspname,c.relname FROM pg_namespace n LEFT JOIN (pg_class c JOIN pg_namespace ns ON ns.oid=c.relnamespace AND c.relname='absent') ON c.relnamespace=n.oid WHERE n.oid=11", &snapshot).rows,
        vec![vec![Value::Text("pg_catalog".into()),Value::Null]]);
    assert_eq!(run("SELECT DISTINCT ON(n.oid) n.oid,c.relname FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid WHERE n.oid=11 AND c.relname IN ('pg_class','pg_namespace') ORDER BY n.oid,c.relname DESC LIMIT 1", &snapshot).rows,
        vec![vec![Value::Int(11),Value::Text("pg_namespace".into())]]);
    assert_eq!(run("SELECT DISTINCT ON(k) k,v FROM (SELECT 1 AS k,'a' AS v FROM pg_namespace WHERE oid=11 UNION ALL SELECT 1,'b' UNION ALL SELECT 2,'c') s ORDER BY k,v DESC", &snapshot).rows,
        vec![vec![Value::Int(1),Value::Text("b".into())],vec![Value::Int(2),Value::Text("c".into())]]);
    assert_eq!(run("SELECT EXISTS(SELECT DISTINCT ON(n.oid) n.oid FROM pg_namespace n)", &snapshot).rows,
        vec![vec![Value::Bool(true)]]);
    assert_eq!(plan("SELECT DISTINCT ON(oid) oid,nspname FROM pg_namespace ORDER BY nspname").unwrap_err(),Error::Unsupported);
    assert_eq!(plan("SELECT DISTINCT ON(2) 2,oid FROM pg_namespace").unwrap_err(),Error::Unsupported);
    assert_eq!(plan("SELECT DISTINCT ON((oid)) nspname AS oid,oid AS nspname FROM pg_namespace").unwrap_err(),Error::Unsupported);
}

#[test]
fn jdbc_imported_keys_joins_stay_within_execution_budget() {
    let mut s = CatalogSnapshot::default();
    s.schemas.insert("gui_sales".into(), (70000, 10));
    for (id, name, columns) in [(10, "parent", vec!["id"]), (20, "child", vec!["id", "parent_id"])] {
        s.tables.push(TableDef { id, name: name.into(), primary_key: vec![0], columns: columns.iter().enumerate().map(|(i,n)| ColumnDef {
            id: id + i as u64, name: (*n).into(), data_type: ScalarType::Integer, not_null: i == 0,
        }).collect() });
        s.table_schemas.insert(id, "gui_sales".into());
    }
    s.constraints.push(ConstraintDef { id: 31, table: 20, name: "child_parent_id_fkey".into(), kind: 'f', columns: vec![21],
        foreign_table: 10, foreign_columns: vec![10], foreign_key_target: Some(ForeignKeyTarget::PrimaryKey), update: 'a', delete: 'a', validated: true, ..Default::default() });
    let sql = "SELECT pkn.nspname,pkc.relname,pka.attname,fkn.nspname,fkc.relname,fka.attname,pos.n,con.conname,pkic.relname FROM pg_namespace pkn,pg_class pkc,pg_attribute pka,pg_namespace fkn,pg_class fkc,pg_attribute fka,pg_constraint con,generate_series(1,32) pos(n),pg_class pkic WHERE pkn.oid=pkc.relnamespace AND pkc.oid=pka.attrelid AND pka.attnum=con.confkey[pos.n] AND con.confrelid=pkc.oid AND fkn.oid=fkc.relnamespace AND fkc.oid=fka.attrelid AND fka.attnum=con.conkey[pos.n] AND con.conrelid=fkc.oid AND con.contype='f' AND (pkic.relkind='i' OR pkic.relkind='I') AND pkic.oid=con.conindid AND fkn.nspname='gui_sales' AND fkc.relname='child' ORDER BY pkn.nspname,pkc.relname,con.conname,pos.n";
    assert_eq!(run(sql, &s).rows, vec![vec![Value::Text("gui_sales".into()),Value::Text("parent".into()),Value::Text("id".into()),Value::Text("gui_sales".into()),Value::Text("child".into()),Value::Text("parent_id".into()),Value::Int(1),Value::Text("child_parent_id_fkey".into()),Value::Text("parent_pkey".into())]]);
}

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
            .all(|r| r[0] == r[1] && r[5] == if r[0] == Value::Int(2200) { Value::Text("standard public schema".into()) } else { Value::Null })
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

#[test]
fn pg18_gui_description_casts_and_physical_statistics() {
    let s = CatalogSnapshot::default();
    assert_eq!(run("SELECT objoid,classoid,objsubid,description FROM pg_description WHERE objoid=2200", &s).rows,
        vec![vec![Value::Int(2200),Value::Int(2615),Value::Int(0),Value::Text("standard public schema".into())]]);
    let casts = run("SELECT oid,castsource,casttarget,castfunc,castcontext,castmethod FROM pg_cast ORDER BY oid", &s);
    let mut expected = vec![(10010,23,20,481,"i","f"),(10011,23,21,314,"a","f"),(10012,23,700,318,"i","f"),(10013,23,701,316,"i","f"),(10014,23,1700,1740,"i","f"),(10034,23,16,2557,"e","f"),(10039,23,26,0,"i","b"),(10046,23,24,0,"i","b"),(10125,25,1042,0,"i","b"),(10126,25,1043,0,"i","b"),(10137,25,18,944,"a","f"),(10140,25,19,407,"i","f"),(10144,23,17,6368,"e","f"),(10150,23,18,78,"e","f")];
    expected.extend([(10032,23,790,3811,"a","f"),(10055,23,2202,0,"i","b"),(10062,23,2203,0,"i","b"),(10071,23,2204,0,"i","b"),(10078,23,2205,0,"i","b"),(10085,23,4191,0,"i","b"),(10092,23,2206,0,"i","b"),(10099,23,3734,0,"i","b"),(10106,23,3769,0,"i","b"),(10109,25,2205,1079,"i","f"),(10115,23,4096,0,"i","b"),(10122,23,4089,0,"i","b")]);
    expected.sort_by_key(|row| row.0);
    assert_eq!(casts.rows, expected.into_iter().map(|(o,s,t,f,c,m)| vec![Value::Int(o),Value::Int(s),Value::Int(t),Value::Int(f),Value::Text(c.into()),Value::Text(m.into())]).collect::<Vec<_>>());
    assert_eq!(casts.fields.iter().map(|f| f.data_type).collect::<Vec<_>>(), vec![PgType::Oid,PgType::Oid,PgType::Oid,PgType::Oid,PgType::Char,PgType::Char]);
    assert_eq!(run("SELECT DISTINCT castsource FROM pg_cast WHERE castcontext IN ('i','a') ORDER BY castsource", &s).rows, vec![vec![Value::Int(23)],vec![Value::Int(25)]]);
    for function in ["pg_relation_size","pg_total_relation_size","pg_table_size","pg_indexes_size","pg_stat_get_numscans"] {
        let result = run(&format!("SELECT {function}(c.oid),{function}(0::oid),{function}(NULL::oid) FROM pg_class c WHERE c.relname='pg_class'"), &s);
        assert!(result.fields.iter().all(|f| f.data_type == PgType::Int8));
        assert_eq!(result.rows, vec![vec![Value::Int(0),if function == "pg_stat_get_numscans" { Value::Int(0) } else { Value::Null },Value::Null]]);
    }
    assert_eq!(run("SELECT pg_relation_size(oid,'fsm') FROM pg_class WHERE relname='pg_class'", &s).rows, vec![vec![Value::Int(0)]]);
    assert!(execute(&plan("SELECT pg_relation_size(oid,'invalid') FROM pg_class").unwrap().unwrap(), &s, &[]).is_err());
}

#[test]
fn cte_group_aliases_use_input_precedence_and_reject_ambiguity() {
    let snapshot = CatalogSnapshot::default();
    assert_eq!(run("WITH inh AS (SELECT attname AS name,concat(n.nspname,'.',p.relname) AS inheritedfrom FROM pg_attribute a JOIN pg_inherits i ON i.inhparent=a.attrelid JOIN pg_class p ON p.oid=i.inhparent JOIN pg_namespace n ON n.oid=p.relnamespace GROUP BY attname,inheritedfrom ORDER BY inheritedfrom) SELECT * FROM inh", &snapshot).rows, Vec::<Vec<Value>>::new());
    assert_eq!(run("SELECT nspname AS oid,count(*) FROM pg_namespace WHERE oid=11 GROUP BY oid,nspname", &snapshot).rows, vec![vec![Value::Text("pg_catalog".into()),Value::Int(1)]]);
    assert_eq!(plan("SELECT oid AS x,nspname AS x FROM pg_namespace GROUP BY x").unwrap_err(), Error::AmbiguousColumn);
    assert_eq!(plan("SELECT count(*) AS x FROM pg_namespace GROUP BY x").unwrap_err(), Error::Unsupported);
    assert_eq!(run("SELECT ('2.5'::real)::bigint,('3.5'::real)::bigint,('-2.5'::real)::bigint FROM pg_namespace WHERE oid=11", &snapshot).rows, vec![vec![Value::Int(2),Value::Int(4),Value::Int(-2)]]);
}

#[test]
fn pg18_constraint_descriptor_types_and_duplicate_oid() {
    let result = run("SELECT c.oid,c.* FROM pg_constraint c", &CatalogSnapshot::default());
    assert_eq!(result.fields.iter().map(|f| f.data_type.oid()).collect::<Vec<_>>(),
        vec![26,26,19,26,18,16,16,16,16,26,26,26,26,26,18,18,18,16,21,16,16,1005,1005,1028,1028,1028,1005,1028,194]);
}

#[test]
fn pgadmin_edit_type_inventory_matches_pg18() {
    let snapshot = CatalogSnapshot::default();
    for (source, names) in [(23,"bigint,double precision,information_schema.cardinal_number,money,numeric,oid,real,regclass,regcollation,regconfig,regdictionary,regnamespace,regoper,regoperator,regproc,regprocedure,regrole,regtype,smallint"),(25,"\"char\",character,character varying,name,regclass")] {
        let rows = run(&format!("SELECT format_type(t.oid,NULL) AS name FROM pg_type t JOIN pg_cast c ON c.casttarget=t.oid WHERE c.castsource={source} AND c.castcontext IN ('i','a') UNION SELECT format_type(oid,NULL) FROM pg_type WHERE typbasetype={source} ORDER BY name"), &snapshot).rows;
        assert_eq!(rows, names.split(',').map(|name| vec![Value::Text(name.into())]).collect::<Vec<_>>());
    }
    let result = run("SELECT 4096*1024 FROM pg_namespace WHERE oid=11", &snapshot);
    assert_eq!(result.fields[0].data_type, PgType::Int4);
    assert_eq!(result.rows, vec![vec![Value::Int(4194304)]]);
}

#[test]
fn pgadmin_role_recursion_follows_membership_chains() {
    let mut snapshot = CatalogSnapshot::default();
    for (oid, role, member) in [(1, 3374, 3373), (2, 3375, 3373), (3, 3377, 3373), (4, 4200, 3375)] {
        snapshot.role_memberships.push(vec![Value::Int(oid), Value::Int(role), Value::Int(member), Value::Int(10), Value::Bool(false), Value::Bool(true), Value::Bool(true)]);
    }
    let sql = "WITH RECURSIVE cte AS (SELECT oid,rolname FROM pg_roles WHERE rolname='pg_monitor' UNION ALL SELECT m.roleid,pgr.rolname FROM cte JOIN pg_auth_members m ON m.member=cte.oid JOIN pg_roles pgr ON pgr.oid=m.roleid) SELECT rolname FROM cte ORDER BY rolname";
    let result = run(sql, &snapshot);
    assert_eq!(result.rows, ["pg_monitor", "pg_read_all_settings", "pg_read_all_stats", "pg_signal_backend", "pg_stat_scan_tables"].map(|name| vec![Value::Text(name.into())]));
}

#[test]
fn recursive_membership_cycles_deduplicate_or_hit_existing_budget() {
    let mut snapshot = CatalogSnapshot::default();
    for (oid, role, member) in [(1, 3375, 3373), (2, 3373, 3375)] {
        snapshot.role_memberships.push(vec![Value::Int(oid), Value::Int(role), Value::Int(member), Value::Int(10), Value::Bool(false), Value::Bool(true), Value::Bool(true)]);
    }
    let sql = "WITH RECURSIVE cte AS (SELECT oid,rolname FROM pg_roles WHERE rolname='pg_monitor' UNION SELECT m.roleid,pgr.rolname FROM cte JOIN pg_auth_members m ON m.member=cte.oid JOIN pg_roles pgr ON pgr.oid=m.roleid) SELECT rolname FROM cte ORDER BY rolname";
    assert_eq!(run(sql, &snapshot).rows, ["pg_monitor", "pg_read_all_stats"].map(|name| vec![Value::Text(name.into())]));
    let plan = plan(&sql.replace(" UNION ", " UNION ALL ")).unwrap().unwrap();
    assert_eq!(execute(&plan, &snapshot, &[]), Err(Error::Limit));
}

#[test]
fn unrelated_self_recursion_with_membership_catalog_stays_unsupported() {
    for recursive in [
        "SELECT cte.oid FROM cte CROSS JOIN pg_auth_members",
        "SELECT cte.oid FROM cte JOIN pg_auth_members m ON m.member=cte.oid",
        "SELECT m.roleid FROM cte CROSS JOIN pg_auth_members m",
        "SELECT m.member FROM cte JOIN pg_auth_members m ON m.member=cte.oid",
    ] {
        let sql = format!("WITH RECURSIVE cte AS (SELECT oid FROM pg_roles WHERE rolname='pg_monitor' UNION ALL {recursive}) SELECT * FROM cte LIMIT 1");
        assert_eq!(plan(&sql).unwrap_err(), Error::Unsupported, "{sql}");
    }
    let snapshot = CatalogSnapshot::default();
    for recursive in [
        "SELECT m.roleid FROM cte JOIN pg_auth_members m ON m.member=cte.oid",
        "SELECT m.member FROM pg_auth_members m JOIN cte ON cte.oid=m.roleid",
    ] {
        let sql = format!("WITH RECURSIVE cte AS (SELECT oid FROM pg_roles WHERE rolname='pg_monitor' UNION {recursive}) SELECT * FROM cte");
        assert_eq!(run(&sql, &snapshot).rows, vec![vec![Value::Int(3373)]]);
    }
}
